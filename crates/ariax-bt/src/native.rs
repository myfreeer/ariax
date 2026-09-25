use crate::{
    BridgeBudget, BridgeLease, BridgeLimits, BtError, BtIdentity, FileMapping, MappingOptions,
    MetadataLimits, OwnedBlob, ProtectedRoot, TorrentMetadata, map_files, parse_info, parse_magnet,
    parse_torrent,
};
use ariax_bt_libtorrent_sys as sys;
use ariax_runtime::{
    BoundedQueue, ByteBudget, BytePermit, CloseReason, HandleBudgets, HandlePermit,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU16, Ordering},
    mpsc,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct BtAdapterConfig {
    pub listen: String,
    pub max_torrents: u32,
    pub peers: u32,
    pub files: u32,
    pub disk_threads: u32,
    pub metadata: MetadataLimits,
    pub bridge: BridgeLimits,
    pub allow_private: bool,
    pub dht: bool,
    pub pex: bool,
    pub encryption: u8,
}

impl Default for BtAdapterConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:0".into(),
            max_torrents: 32,
            peers: 256,
            files: 64,
            disk_threads: 1,
            metadata: MetadataLimits::default(),
            bridge: BridgeLimits {
                blob_bytes: 128 * 1024 * 1024,
                ..BridgeLimits::default()
            },
            allow_private: false,
            dht: true,
            pex: true,
            encryption: 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BtResources {
    pub resident: ByteBudget,
    pub handles: HandleBudgets,
    pub threads: ByteBudget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BtTaskSettings {
    pub download_limit: u32,
    pub upload_limit: u32,
    pub peers: u32,
    pub dht: bool,
    pub pex: bool,
    pub metadata_only: bool,
    pub save_metadata: bool,
    pub seed_ratio_milli: u32,
    pub seed_seconds: Option<u64>,
}

impl Default for BtTaskSettings {
    fn default() -> Self {
        Self {
            download_limit: 0,
            upload_limit: 0,
            peers: 64,
            dht: true,
            pex: true,
            metadata_only: false,
            save_metadata: false,
            seed_ratio_milli: 1000,
            seed_seconds: None,
        }
    }
}

#[derive(Debug)]
pub struct BtAdmission {
    pub gid: u64,
    pub root: ProtectedRoot,
    pub torrent: Option<Arc<OwnedBlob>>,
    pub magnet: Option<String>,
    pub resume: Option<Arc<OwnedBlob>>,
    pub mapping: MappingOptions,
    pub settings: BtTaskSettings,
    pub expected_identity: Option<BtIdentity>,
    pub expected_mapping: Option<Vec<FileMapping>>,
    pub allow_existing: bool,
}

#[derive(Debug)]
pub enum BtCommand {
    Add(Box<BtAdmission>),
    ReadMetadata {
        gid: u64,
    },
    /// Submitted only after the session owner commits this exact mapping.
    Approve {
        gid: u64,
        mapping: Vec<FileMapping>,
    },
    Resume {
        gid: u64,
    },
    Remove {
        gid: u64,
    },
    /// Caller has durably recorded DirtyCheckpoint; still waits for owned work.
    RemoveDirty {
        gid: u64,
    },
    Checkpoint {
        gid: u64,
        request: u64,
        limit: usize,
        timeout: Duration,
    },
    SetTask {
        gid: u64,
        version: u64,
        settings: BtTaskSettings,
    },
    SetRates {
        download: Option<u64>,
        upload: u32,
    },
    ConnectPeer {
        gid: u64,
        address: std::net::SocketAddr,
    },
    Peers {
        gid: u64,
        limit: u32,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BtPeer {
    pub address: String,
    pub port: u16,
    pub peer_id: String,
    pub download_rate: u32,
    pub upload_rate: u32,
    pub seeder: bool,
    pub am_choking: bool,
    pub peer_choking: bool,
}

#[derive(Debug)]
pub enum BtReply {
    Applied {
        version: u64,
    },
    Metadata {
        info: Arc<OwnedBlob>,
        metadata: TorrentMetadata,
        mapping: Vec<FileMapping>,
    },
    Checkpoint {
        request: u64,
        data: Arc<OwnedBlob>,
    },
    Peers(Vec<BtPeer>),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BtSnapshot {
    pub gid: u64,
    pub metadata: bool,
    pub held: bool,
    pub paused: bool,
    pub checking: bool,
    pub finished: bool,
    pub seeding: bool,
    pub error: u32,
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub download_rate: u32,
    pub upload_rate: u32,
    pub peers: u32,
    pub seeds: u32,
    pub file_progress: Vec<u64>,
    #[serde(skip)]
    _memory: BytePermit,
}

#[derive(Debug)]
pub struct BtEvent {
    pub gid: u64,
    pub terminal: bool,
    pub metadata_ready: bool,
}

struct Delivery {
    value: Result<BtReply, BtError>,
    _lease: Arc<BridgeLease>,
}
struct Work {
    command: BtCommand,
    reply: mpsc::SyncSender<Delivery>,
    lease: Arc<BridgeLease>,
}
struct QueuedEvent {
    event: BtEvent,
    _memory: BytePermit,
    _reliable: Option<BridgeLease>,
}

pub struct BtPending {
    receiver: Option<mpsc::Receiver<Delivery>>,
}
impl BtPending {
    pub fn try_take(&mut self) -> Result<Option<BtReply>, BtError> {
        let receiver = self.receiver.as_ref().ok_or(BtError::StaleCompletion)?;
        match receiver.try_recv() {
            Ok(delivery) => {
                self.receiver = None;
                delivery.value.map(Some)
            }
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                self.receiver = None;
                Err(BtError::Closed)
            }
        }
    }

    pub fn wait(mut self, timeout: Duration) -> Result<BtReply, BtError> {
        self.receiver
            .take()
            .ok_or(BtError::StaleCompletion)?
            .recv_timeout(timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => BtError::CheckpointTimeout,
                mpsc::RecvTimeoutError::Disconnected => BtError::Closed,
            })?
            .value
    }
}

struct Shared {
    commands: BoundedQueue<Work>,
    events: BoundedQueue<QueuedEvent>,
    terminal: BoundedQueue<QueuedEvent>,
    budget: BridgeBudget,
    resident: ByteBudget,
    snapshots: RwLock<BTreeMap<u64, Arc<BtSnapshot>>>,
    stopping: AtomicBool,
    stopped: AtomicBool,
    reconcile: AtomicBool,
    port: AtomicU16,
}

#[derive(Clone)]
pub struct BtHandle {
    shared: Arc<Shared>,
    thread: thread::Thread,
}
impl BtHandle {
    pub fn blob(&self, bytes: Vec<u8>) -> Result<Arc<OwnedBlob>, BtError> {
        self.shared.budget.blob(bytes).map(Arc::new)
    }

    pub fn submit(&self, command: BtCommand) -> Result<BtPending, BtError> {
        self.try_submit_owned(command).map_err(|(_, error)| error)
    }

    /// Returns unaccepted work with its blob ownership when capacity is unavailable.
    pub fn try_submit_owned(&self, command: BtCommand) -> Result<BtPending, (BtCommand, BtError)> {
        if self.shared.stopping.load(Ordering::Acquire) {
            return Err((command, BtError::Closed));
        }
        let bytes = match &command {
            BtCommand::Add(value) => 2048usize
                .saturating_add(value.magnet.as_ref().map_or(0, String::capacity))
                .saturating_add(value.root.path().as_os_str().len()),
            BtCommand::Approve { mapping, .. } => mapping.iter().fold(512usize, |bytes, file| {
                bytes.saturating_add(file.path.capacity() + 128)
            }),
            _ => 512,
        };
        let output = match &command {
            BtCommand::Peers { limit, .. } => {
                (*limit as usize).saturating_mul(256).saturating_add(512)
            }
            _ => 4096,
        };
        let lease = match self.shared.budget.reserve(bytes, output) {
            Ok(lease) => Arc::new(lease),
            Err(error) => return Err((command, error)),
        };
        let (reply, receiver) = mpsc::sync_channel(1);
        let work = Work {
            command,
            reply,
            lease,
        };
        if let Err(rejection) = self.shared.commands.try_send(work, bytes) {
            use ariax_runtime::QueueSendError;
            let (work, error) = match rejection {
                QueueSendError::Full(work) | QueueSendError::ItemTooLarge(work) => {
                    (work, BtError::Overloaded)
                }
                QueueSendError::Closed { value, .. } => (value, BtError::Closed),
            };
            return Err((work.command, error));
        }
        self.thread.unpark();
        Ok(BtPending {
            receiver: Some(receiver),
        })
    }

    pub fn snapshot(&self, gid: u64) -> Option<Arc<BtSnapshot>> {
        self.shared
            .snapshots
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&gid)
            .cloned()
    }
    pub fn event(&self) -> Option<BtEvent> {
        self.shared
            .terminal
            .try_recv()
            .or_else(|| self.shared.events.try_recv())
            .map(|value| value.event)
    }
    pub fn needs_reconciliation(&self) -> bool {
        self.shared.reconcile.swap(false, Ordering::AcqRel)
    }
    pub fn pending_commands(&self) -> usize {
        self.shared.budget.pending()
    }
    pub fn is_stopped(&self) -> bool {
        self.shared.stopped.load(Ordering::Acquire)
    }
    pub fn listen_port(&self) -> u16 {
        self.shared.port.load(Ordering::Acquire)
    }
}

pub struct BtAdapter {
    handle: BtHandle,
    worker: Option<JoinHandle<()>>,
    port: u16,
}
impl BtAdapter {
    pub fn start(config: BtAdapterConfig, resources: BtResources) -> Result<Self, BtError> {
        config.metadata.validate()?;
        if config.max_torrents == 0
            || config.max_torrents > 4096
            || config.peers < 2
            || config.peers > 16384
            || config.files < 2
            || config.files > 16384
            || config.disk_threads == 0
            || config.disk_threads > 16
        {
            return Err(BtError::Overloaded);
        }
        let budget = BridgeBudget::new(config.bridge, resources.resident.clone())?;
        let reservation = NativeReservation::acquire(&config, &resources)?;
        let shared = Arc::new(Shared {
            commands: BoundedQueue::new(config.bridge.commands, config.bridge.command_bytes),
            events: BoundedQueue::new(config.bridge.events, config.bridge.event_bytes),
            terminal: BoundedQueue::new(config.bridge.completions, config.bridge.completion_bytes),
            budget,
            resident: resources.resident,
            snapshots: RwLock::new(BTreeMap::new()),
            stopping: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            reconcile: AtomicBool::new(false),
            port: AtomicU16::new(0),
        });
        let (ready, initialized) = mpsc::sync_channel(1);
        let worker_shared = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("ariax-bt".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                struct Done(Arc<Shared>);
                impl Drop for Done {
                    fn drop(&mut self) {
                        self.0.stopped.store(true, Ordering::Release);
                    }
                }
                let _done = Done(Arc::clone(&worker_shared));
                match Worker::new(config, worker_shared, reservation) {
                    Ok(mut worker) => {
                        let _ = ready.send(Ok(worker.session.listen_port()));
                        worker.run();
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                }
            })
            .map_err(|_| BtError::Native)?;
        let handle = BtHandle {
            shared,
            thread: worker.thread().clone(),
        };
        match initialized.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(port)) => Ok(Self {
                handle,
                worker: Some(worker),
                port,
            }),
            result => {
                handle.shared.stopping.store(true, Ordering::Release);
                handle.thread.unpark();
                Err(result.ok().and_then(Result::err).unwrap_or(BtError::Native))
            }
        }
    }

    pub fn handle(&self) -> BtHandle {
        self.handle.clone()
    }
    pub fn initial_listen_port(&self) -> u16 {
        self.port
    }
    pub fn request_stop(&self) {
        self.handle.shared.stopping.store(true, Ordering::Release);
        self.handle.thread.unpark();
    }
    pub fn poll_stopped(&mut self) -> bool {
        if self.worker.as_ref().is_some_and(JoinHandle::is_finished) {
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
            return true;
        }
        self.worker.is_none()
    }
}
impl Drop for BtAdapter {
    fn drop(&mut self) {
        self.request_stop();
    }
}

struct NativeReservation {
    _memory: BytePermit,
    _threads: BytePermit,
    _handles: Vec<HandlePermit>,
}
impl NativeReservation {
    fn acquire(config: &BtAdapterConfig, resources: &BtResources) -> Result<Self, BtError> {
        let threads = config.disk_threads as usize + 3; // adapter, session and resolver
        let memory = 32 * 1024 * 1024
            + config.peers as usize * 128 * 1024
            + config.files as usize * 4096
            + threads * 2 * 1024 * 1024;
        let memory = resources
            .resident
            .try_acquire(memory)
            .map_err(|_| BtError::Overloaded)?;
        let threads = resources
            .threads
            .try_acquire(threads)
            .map_err(|_| BtError::Overloaded)?;
        let sockets = config.peers as usize + config.max_torrents as usize * 8 + 16;
        let mut handles = Vec::with_capacity(sockets + config.files as usize);
        for _ in 0..sockets {
            handles.push(
                resources
                    .handles
                    .try_acquire_socket()
                    .map_err(|_| BtError::Overloaded)?,
            );
        }
        for _ in 0..config.files {
            handles.push(
                resources
                    .handles
                    .try_acquire_file()
                    .map_err(|_| BtError::Overloaded)?,
            );
        }
        Ok(Self {
            _memory: memory,
            _threads: threads,
            _handles: handles,
        })
    }
}

struct Entry {
    admission: BtAdmission,
    identity: BtIdentity,
    metadata: Option<TorrentMetadata>,
    mapping: Option<Vec<FileMapping>>,
    metadata_blob: Option<Arc<OwnedBlob>>,
    magnet_memory: Option<(BytePermit, BytePermit)>,
    version: u64,
    notified: (bool, bool, u32),
    checkpointed: bool,
}

struct PendingCheckpoint {
    gid: u64,
    request: u64,
    deadline: Instant,
    limit: usize,
    permits: (BytePermit, BytePermit),
    reply: Option<mpsc::SyncSender<Delivery>>,
    lease: Arc<BridgeLease>,
}

struct Worker {
    // Field order is a lifetime invariant: join native callbacks before dropping
    // pending reservations, entries or the native resource share, including unwind.
    session: sys::NativeSessionOwner,
    pending: Vec<PendingCheckpoint>,
    entries: BTreeMap<u64, Entry>,
    _reservation: NativeReservation,
    config: BtAdapterConfig,
    shared: Arc<Shared>,
}

impl Worker {
    fn new(
        config: BtAdapterConfig,
        shared: Arc<Shared>,
        reservation: NativeReservation,
    ) -> Result<Self, BtError> {
        let options = sys::NativeOptions {
            listen: config.listen.clone(),
            max_torrents: config.max_torrents,
            connections: config.peers,
            files: config.files,
            disk_threads: config.disk_threads,
            metadata_bytes: config.metadata.bytes as u32,
            max_files: config.metadata.files as u32,
            max_pieces: config.metadata.pieces as u32,
            decode_depth: config.metadata.depth as u32,
            decode_tokens: config.metadata.tokens as u32,
            alert_items: 4096,
            download_limit: 0,
            upload_limit: 0,
            dht: config.dht,
            pex: config.pex,
            allow_private: config.allow_private,
            encryption: config.encryption,
        };
        let session = sys::new_session(&options).map_err(|_| BtError::Native)?;
        Ok(Self {
            session,
            pending: Vec::new(),
            entries: BTreeMap::new(),
            _reservation: reservation,
            config,
            shared,
        })
    }

    fn run(&mut self) {
        let mut next_status = Instant::now();
        while !self.shared.stopping.load(Ordering::Acquire) {
            for _ in 0..16 {
                let Some(work) = self.shared.commands.try_recv() else {
                    break;
                };
                self.execute(work);
            }
            self.poll_checkpoints();
            if Instant::now() >= next_status {
                self.publish();
                next_status = Instant::now() + Duration::from_millis(25);
            }
            if self.shared.commands.is_empty() {
                thread::park_timeout(Duration::from_millis(5));
            }
        }
        for work in self.shared.commands.close(CloseReason::Shutdown) {
            Self::deliver(work.reply, work.lease, Err(BtError::Closed));
        }
        for pending in &mut self.pending {
            if let Some(reply) = pending.reply.take() {
                Self::deliver(reply, Arc::clone(&pending.lease), Err(BtError::Closed));
            }
        }
        // NativeSession is the first field: its destructor drains callbacks
        // before pending checkpoints, metadata and native resource shares drop.
        self.shared
            .snapshots
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.shared.events.close(CloseReason::Shutdown);
        self.shared.terminal.close(CloseReason::Shutdown);
    }

    fn deliver(
        reply: mpsc::SyncSender<Delivery>,
        lease: Arc<BridgeLease>,
        value: Result<BtReply, BtError>,
    ) {
        let _ = reply.send(Delivery {
            value,
            _lease: lease,
        });
    }

    fn execute(&mut self, work: Work) {
        let Work {
            command,
            reply,
            lease,
        } = work;
        if let BtCommand::Checkpoint {
            gid,
            request,
            limit,
            timeout,
        } = command
        {
            let result = (|| {
                if request == 0
                    || limit == 0
                    || limit > 64 * 1024 * 1024
                    || timeout.is_zero()
                    || timeout > Duration::from_secs(300)
                    || self.pending.iter().any(|value| value.gid == gid)
                {
                    return Err(BtError::CheckpointFailed);
                }
                self.entry(gid)?;
                let permits = self.shared.budget.reserve_blob_output(limit)?;
                self.session
                    .pin_mut()
                    .checkpoint(gid, request, limit as u32)
                    .map_err(|_| BtError::CheckpointFailed)?;
                Ok(permits)
            })();
            match result {
                Ok(permits) => self.pending.push(PendingCheckpoint {
                    gid,
                    request,
                    deadline: Instant::now() + timeout,
                    limit,
                    permits,
                    reply: Some(reply),
                    lease,
                }),
                Err(error) => Self::deliver(reply, lease, Err(error)),
            }
            return;
        }
        let result = self.apply(command);
        Self::deliver(reply, lease, result);
    }

    fn entry(&self, gid: u64) -> Result<&Entry, BtError> {
        self.entries.get(&gid).ok_or(BtError::StaleCompletion)
    }

    fn validate_settings(&self, settings: &BtTaskSettings) -> Result<(), BtError> {
        if settings.peers < 2
            || settings.peers > self.config.peers
            || settings.download_limit > i32::MAX as u32
            || settings.upload_limit > i32::MAX as u32
        {
            return Err(BtError::UnsupportedOption);
        }
        if settings.dht && !self.config.dht || settings.pex && !self.config.pex {
            return Err(BtError::RequiresRestart);
        }
        Ok(())
    }

    fn apply(&mut self, command: BtCommand) -> Result<BtReply, BtError> {
        match command {
            BtCommand::Add(admission) => self.add(*admission)?,
            BtCommand::ReadMetadata { gid } => return self.read_metadata(gid),
            BtCommand::Approve { gid, mapping } => {
                let entry = self.entry(gid)?;
                if entry.mapping.as_ref() != Some(&mapping)
                    || entry.admission.settings.metadata_only
                {
                    return Err(BtError::IdentityMismatch);
                }
                entry
                    .admission
                    .root
                    .validate_mapping(&mapping, entry.admission.allow_existing)?;
                let paths = mapping
                    .iter()
                    .map(|file| file.path.clone())
                    .collect::<Vec<_>>();
                let priorities = mapping
                    .iter()
                    .map(|file| if file.selected && !file.padding { 4 } else { 0 })
                    .collect::<Vec<_>>();
                self.session
                    .pin_mut()
                    .approve(gid, &paths, &priorities)
                    .map_err(|_| BtError::Native)?;
            }
            BtCommand::Resume { gid } => {
                let entry = self.entry(gid)?;
                if self.pending.iter().any(|value| value.gid == gid) {
                    return Err(BtError::Overloaded);
                }
                if let Some(mapping) = &entry.mapping {
                    entry.admission.root.validate_mapping(mapping, true)?;
                } else {
                    entry.admission.root.revalidate()?;
                }
                self.session
                    .pin_mut()
                    .resume(gid)
                    .map_err(|_| BtError::Native)?;
                self.entries
                    .get_mut(&gid)
                    .expect("validated entry")
                    .checkpointed = false;
            }
            BtCommand::Remove { gid } | BtCommand::RemoveDirty { gid } => {
                let dirty = matches!(command, BtCommand::RemoveDirty { .. });
                let entry = self.entry(gid)?;
                if self.pending.iter().any(|value| value.gid == gid) {
                    return Err(BtError::Overloaded);
                }
                let status = self.session.status(gid).map_err(|_| BtError::Native)?;
                if !status.held && (!status.paused || !entry.checkpointed && !dirty) {
                    return Err(BtError::CheckpointFailed);
                }
                self.session
                    .pin_mut()
                    .remove(gid)
                    .map_err(|_| BtError::Native)?;
                self.entries.remove(&gid);
                self.shared
                    .snapshots
                    .write()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&gid);
            }
            BtCommand::SetTask {
                gid,
                version,
                settings,
            } => {
                self.validate_settings(&settings)?;
                let entry = self.entry(gid)?;
                if version <= entry.version {
                    return Err(BtError::StaleCompletion);
                }
                if settings.metadata_only != entry.admission.settings.metadata_only
                    || settings.save_metadata != entry.admission.settings.save_metadata
                {
                    return Err(BtError::RequiresRestart);
                }
                self.session
                    .pin_mut()
                    .set_task_options(
                        gid,
                        settings.download_limit,
                        settings.upload_limit,
                        settings.peers,
                        settings.dht,
                        settings.pex,
                    )
                    .map_err(|_| BtError::Native)?;
                let entry = self.entries.get_mut(&gid).expect("validated entry");
                entry.version = version;
                entry.admission.settings = settings;
                return Ok(BtReply::Applied { version });
            }
            BtCommand::SetRates { download, upload } => {
                self.session
                    .pin_mut()
                    .set_rates(
                        download.unwrap_or(0).min(i32::MAX as u64) as u32,
                        upload,
                        download == Some(0),
                    )
                    .map_err(|_| BtError::UnsupportedOption)?;
            }
            BtCommand::ConnectPeer { gid, address } => {
                self.entry(gid)?;
                self.session
                    .pin_mut()
                    .connect_peer(gid, &address.ip().to_string(), address.port())
                    .map_err(|_| BtError::Destination)?;
            }
            BtCommand::Peers { gid, limit } => {
                self.entry(gid)?;
                let peers = self
                    .session
                    .peers(gid, limit)
                    .map_err(|_| BtError::Overloaded)?
                    .into_iter()
                    .map(|peer| BtPeer {
                        address: peer.address,
                        port: peer.port,
                        peer_id: peer.peer_id,
                        download_rate: peer.download_rate,
                        upload_rate: peer.upload_rate,
                        seeder: peer.seeder,
                        am_choking: peer.am_choking,
                        peer_choking: peer.peer_choking,
                    })
                    .collect();
                return Ok(BtReply::Peers(peers));
            }
            BtCommand::Checkpoint { .. } => unreachable!("handled with tracked ownership"),
        }
        Ok(BtReply::Applied { version: 0 })
    }

    fn add(&mut self, admission: BtAdmission) -> Result<(), BtError> {
        if admission.gid == 0
            || self.entries.contains_key(&admission.gid)
            || self.entries.len() >= self.config.max_torrents as usize
        {
            return Err(BtError::Overloaded);
        }
        self.validate_settings(&admission.settings)?;
        admission.root.revalidate()?;
        let (identity, metadata, mapping) = match (&admission.torrent, &admission.magnet) {
            (Some(blob), None) => {
                let metadata = parse_torrent(blob.bytes(), self.config.metadata)?;
                let mapping = map_files(&metadata, &admission.mapping)?;
                admission
                    .root
                    .validate_mapping(&mapping, admission.allow_existing)?;
                (metadata.identity.clone(), Some(metadata), Some(mapping))
            }
            (None, Some(uri)) => (parse_magnet(uri)?.identity, None, None),
            _ => return Err(BtError::InvalidMetadata),
        };
        if self
            .entries
            .values()
            .any(|entry| entry.identity.overlaps(&identity))
            || admission
                .expected_identity
                .as_ref()
                .is_some_and(|expected| !expected.matches(&identity))
            || mapping.as_ref().is_some_and(|mapping| {
                admission
                    .expected_mapping
                    .as_ref()
                    .is_some_and(|expected| expected != mapping)
            })
        {
            return Err(BtError::IdentityMismatch);
        }
        if let Some(resume) = &admission.resume {
            crate::validate_resume(resume.bytes(), &identity)?;
        }
        // Reserved before a magnet can receive any metadata, and retained by the
        // entry even when the caller abandons its admission completion.
        let metadata_capacity = admission
            .torrent
            .as_ref()
            .map_or(self.config.metadata.bytes, |blob| blob.bytes().len());
        let magnet_memory = Some(self.shared.budget.reserve_blob_output(metadata_capacity)?);
        let root = admission.root.path().to_str().ok_or(BtError::UnsafePath)?;
        self.session
            .pin_mut()
            .add(
                admission.gid,
                admission.torrent.as_ref().map_or(&[], |blob| blob.bytes()),
                admission.magnet.as_deref().unwrap_or(""),
                root,
                admission.resume.as_ref().map_or(&[], |blob| blob.bytes()),
            )
            .map_err(|_| BtError::Native)?;
        let settings = &admission.settings;
        if self
            .session
            .pin_mut()
            .set_task_options(
                admission.gid,
                settings.download_limit,
                settings.upload_limit,
                settings.peers,
                settings.dht,
                settings.pex,
            )
            .is_err()
        {
            let _ = self.session.pin_mut().remove(admission.gid);
            return Err(BtError::Native);
        }
        self.entries.insert(
            admission.gid,
            Entry {
                admission,
                identity,
                metadata,
                mapping,
                metadata_blob: None,
                magnet_memory,
                version: 0,
                notified: (false, false, 0),
                checkpointed: false,
            },
        );
        Ok(())
    }

    fn read_metadata(&mut self, gid: u64) -> Result<BtReply, BtError> {
        if self.entry(gid)?.metadata_blob.is_none() {
            let native = self
                .session
                .metadata(gid)
                .map_err(|_| BtError::InvalidMetadata)?;
            let mut metadata = parse_info(&native.info, self.config.metadata)?;
            let hex = |value: &[u8]| {
                value
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            };
            let identity = BtIdentity {
                v1: (!native.v1.is_empty()).then(|| hex(&native.v1)),
                v2: (!native.v2.is_empty()).then(|| hex(&native.v2)),
            };
            let entry = self.entry(gid)?;
            if metadata.identity != identity
                || !entry.identity.matches(&identity)
                || self
                    .entries
                    .iter()
                    .any(|(other, entry)| *other != gid && entry.identity.overlaps(&identity))
                || entry
                    .admission
                    .expected_identity
                    .as_ref()
                    .is_some_and(|expected| !expected.matches(&identity))
                || metadata.files.len() != native.file_sizes.len()
                || native.file_sizes.len() != native.padding.len()
                || native.file_sizes.len() != native.symlinks.len()
                || metadata.piece_length != u64::from(native.piece_length)
                || metadata.pieces != u64::from(native.pieces)
                || metadata.files.iter().enumerate().any(|(index, file)| {
                    file.length != native.file_sizes[index]
                        || file.padding != (native.padding[index] != 0)
                        || native.symlinks[index] != 0
                })
            {
                return Err(BtError::IdentityMismatch);
            }
            if let Some(original) = &entry.metadata {
                metadata.trackers = original.trackers.clone();
                metadata.web_seeds = original.web_seeds.clone();
            } else if let Some(magnet) = &entry.admission.magnet {
                let magnet = parse_magnet(magnet)?;
                metadata.trackers = magnet.trackers;
                metadata.web_seeds = magnet.web_seeds;
            }
            let mapping = map_files(&metadata, &entry.admission.mapping)?;
            if entry
                .admission
                .expected_mapping
                .as_ref()
                .is_some_and(|expected| expected != &mapping)
            {
                return Err(BtError::IdentityMismatch);
            }
            entry
                .admission
                .root
                .validate_mapping(&mapping, entry.admission.allow_existing)?;
            let entry = self.entries.get_mut(&gid).expect("validated entry");
            let permits = entry
                .magnet_memory
                .take()
                .expect("metadata reserved before native admission");
            entry.metadata_blob = Some(Arc::new(BridgeBudget::finish_blob(native.info, permits)));
            entry.identity = identity;
            entry.metadata = Some(metadata);
            entry.mapping = Some(mapping);
        }
        let entry = self.entry(gid)?;
        Ok(BtReply::Metadata {
            info: Arc::clone(entry.metadata_blob.as_ref().expect("validated metadata")),
            metadata: entry.metadata.as_ref().expect("validated metadata").clone(),
            mapping: entry.mapping.as_ref().expect("validated mapping").clone(),
        })
    }

    fn poll_checkpoints(&mut self) {
        let mut index = 0;
        while index < self.pending.len() {
            let pending = &mut self.pending[index];
            let result = self
                .session
                .pin_mut()
                .poll_checkpoint(pending.gid, pending.request);
            match result {
                Err(_) => {
                    if let Some(reply) = pending.reply.take() {
                        Self::deliver(
                            reply,
                            Arc::clone(&pending.lease),
                            Err(BtError::CheckpointFailed),
                        );
                    }
                    // An exceptional poll cannot prove that native ownership
                    // ended. Retain its credit until a successful poll or drop.
                    self.shared.reconcile.store(true, Ordering::Release);
                    index += 1;
                }
                Ok(result) if result.state == 0 => {
                    if Instant::now() >= pending.deadline
                        && let Some(reply) = pending.reply.take()
                    {
                        // This delivery does not release the accepted-work
                        // credit or native output reservation on timeout.
                        Self::deliver(
                            reply,
                            Arc::clone(&pending.lease),
                            Err(BtError::CheckpointTimeout),
                        );
                    }
                    index += 1;
                }
                result => {
                    let pending = self.pending.swap_remove(index);
                    let value = match result {
                        Ok(result)
                            if result.state == 1
                                && result.data.len() <= pending.limit
                                && result.request == pending.request =>
                        {
                            if let Some(entry) = self.entries.get_mut(&pending.gid) {
                                entry.checkpointed = true;
                            }
                            Ok(BtReply::Checkpoint {
                                request: pending.request,
                                data: Arc::new(BridgeBudget::finish_blob(
                                    result.data,
                                    pending.permits,
                                )),
                            })
                        }
                        _ => Err(BtError::CheckpointFailed),
                    };
                    if let Some(reply) = pending.reply {
                        Self::deliver(reply, pending.lease, value);
                    }
                }
            }
        }
    }

    fn publish(&mut self) {
        self.shared
            .port
            .store(self.session.listen_port(), Ordering::Release);
        if self
            .session
            .pin_mut()
            .drain_alerts()
            .map_or(true, |lost| lost != 0)
        {
            self.shared.reconcile.store(true, Ordering::Release);
        }
        for (&gid, entry) in &mut self.entries {
            let bytes = self
                .config
                .metadata
                .files
                .max(8)
                .saturating_mul(8)
                .saturating_add(512);
            let Ok(mut memory) = self.shared.resident.try_acquire(bytes) else {
                self.shared.reconcile.store(true, Ordering::Release);
                continue;
            };
            let Ok(status) = self.session.status(gid) else {
                self.shared.reconcile.store(true, Ordering::Release);
                continue;
            };
            let notified = (status.metadata, status.finished, status.error);
            let event = (notified != entry.notified).then_some(BtEvent {
                gid,
                terminal: status.finished || status.error != 0,
                metadata_ready: status.metadata && !entry.notified.0,
            });
            entry.notified = notified;
            memory
                .shrink_to(512 + status.file_progress.capacity() * 8)
                .expect("bounded native snapshot");
            let snapshot = Arc::new(BtSnapshot {
                gid,
                metadata: status.metadata,
                held: status.held,
                paused: status.paused,
                checking: status.checking,
                finished: status.finished,
                seeding: status.seeding,
                error: status.error,
                total_bytes: status.total_bytes,
                done_bytes: status.done_bytes,
                downloaded: status.downloaded,
                uploaded: status.uploaded,
                download_rate: status.download_rate,
                upload_rate: status.upload_rate,
                peers: status.peers,
                seeds: status.seeds,
                file_progress: status.file_progress,
                _memory: memory,
            });
            self.shared
                .snapshots
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .insert(gid, snapshot);
            if let Some(event) = event {
                Self::event(&self.shared, event);
            }
        }
    }

    fn event(shared: &Shared, event: BtEvent) {
        let result = (|| {
            let memory = shared.resident.try_acquire(128).map_err(|_| ())?;
            let reliable = if event.terminal {
                Some(shared.budget.reserve(128, 128).map_err(|_| ())?)
            } else {
                None
            };
            let queue = if event.terminal {
                &shared.terminal
            } else {
                &shared.events
            };
            queue
                .try_send(
                    QueuedEvent {
                        event,
                        _memory: memory,
                        _reliable: reliable,
                    },
                    128,
                )
                .map_err(|_| ())
        })();
        if result.is_err() {
            shared.reconcile.store(true, Ordering::Release);
        }
    }
}
