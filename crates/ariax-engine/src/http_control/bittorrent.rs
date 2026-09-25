//! BitTorrent participates in the existing scheduler and persistence owner.
//! Native commands and filesystem preparation never run on the mutable owner.

use super::*;
use ariax_bt::{
    BtAdapter, BtAdapterConfig, BtAdmission, BtCommand, BtError, BtHandle, BtPeer, BtPending,
    BtReply, BtResources, BtSnapshot, BtTaskSettings, FileMapping, MappingOptions, MetadataLimits,
    OwnedBlob, ProtectedRoot, TorrentMetadata, info_section, map_files, parse_magnet,
    parse_torrent, torrent_from_info, with_web_seeds,
};
use ariax_runtime::{ByteBudget, BytePermit};
use ariax_storage::{
    SessionBtBinding, SessionBtCheckpoint, SessionBtFile, SessionBtTaskRecord, SessionCompletion,
    SessionOwnerError,
};
use base64ct::Encoding;
use std::collections::BTreeSet;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::task::Poll;

const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
const PEER_INTERVAL: Duration = Duration::from_secs(1);

pub(super) fn task_option(name: &str) -> bool {
    matches!(
        name,
        "max-download-limit"
            | "max-upload-limit"
            | "bt-max-peers"
            | "enable-dht"
            | "enable-peer-exchange"
            | "bt-metadata-only"
            | "bt-save-metadata"
            | "seed-ratio"
            | "seed-time"
            | "bt-resume-data-limit"
            | "bt-resume-timeout"
            | "select-file"
            | "out"
            | "index-out"
            | "bt-tracker"
            | "bt-exclude-tracker"
    )
}

fn invalid(message: &'static str) -> HttpControlError {
    HttpControlError::InvalidParams(message)
}
fn bt_error(error: BtError) -> HttpControlError {
    HttpControlError::Persistence(error.to_string())
}
fn public_error(error: BtError) -> PublicError {
    let kind = match error {
        BtError::CheckpointFailed | BtError::CheckpointTimeout => {
            ariax_core::ErrorKind::DirtyCheckpoint
        }
        BtError::Overloaded => ariax_core::ErrorKind::ResourceLimit,
        BtError::UnsafePath | BtError::Symlink | BtError::Collision | BtError::UnprotectedRoot => {
            ariax_core::ErrorKind::InvalidPath
        }
        _ => ariax_core::ErrorKind::Network,
    };
    PublicError::new(kind, error.to_string(), RetryClass::Never)
}

fn spawn<T: Send + 'static>(
    pool: &ariax_runtime::CpuPool,
    operation: impl FnOnce() -> Result<T, HttpControlError> + Send + 'static,
) -> Result<Receiver<Result<T, HttpControlError>>, HttpControlError> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let reservation = pool
        .reserve(64 * 1024)
        .map_err(|_| HttpControlError::Busy)?;
    drop(reservation.spawn(move || {
        let _ = sender.send(operation());
    }));
    Ok(receiver)
}

fn receive<T>(
    receiver: &Receiver<Result<T, HttpControlError>>,
) -> Result<Option<T>, HttpControlError> {
    match receiver.try_recv() {
        Ok(result) => result.map(Some),
        Err(TryRecvError::Empty) => Ok(None),
        Err(TryRecvError::Disconnected) => Err(bt_error(BtError::Closed)),
    }
}

#[derive(Clone)]
pub(super) struct Options {
    pub(super) settings: BtTaskSettings,
    pub(super) mapping: MappingOptions,
    pub(super) persisted: SanitizedOptionMap,
    pub(super) resume_limit: usize,
    pub(super) timeout: Duration,
    paused: bool,
    directory: Option<String>,
}

fn scalar(value: &Value) -> Result<String, HttpControlError> {
    option_input_text(value)
}

fn decimal(value: &str, scale: f64, maximum: u64) -> Result<u64, HttpControlError> {
    let number: f64 = value
        .parse()
        .map_err(|_| invalid("invalid BitTorrent numeric option"))?;
    let scaled = number * scale;
    if !scaled.is_finite() || scaled < 0.0 || scaled > maximum as f64 {
        return Err(invalid("BitTorrent numeric option is out of range"));
    }
    Ok(scaled.round() as u64)
}

fn selection(value: &str) -> Result<BTreeSet<u32>, HttpControlError> {
    let mut selected = BTreeSet::new();
    for item in value.split(',') {
        let (first, last) = item.split_once('-').unwrap_or((item, item));
        let first: u32 = first
            .parse()
            .map_err(|_| invalid("invalid BitTorrent file selection"))?;
        let last: u32 = last
            .parse()
            .map_err(|_| invalid("invalid BitTorrent file selection"))?;
        if first == 0 || last < first || last > 10_000 {
            return Err(invalid("BitTorrent file selection is out of range"));
        }
        selected.extend(first..=last);
    }
    if selected.is_empty() {
        return Err(invalid("BitTorrent file selection is empty"));
    }
    Ok(selected)
}

impl Options {
    fn retained_bytes(&self) -> usize {
        self.persisted
            .entries()
            .fold(1024usize, |bytes, (name, value)| {
                bytes
                    .saturating_add(name.len())
                    .saturating_add(value.len())
                    .saturating_add(512)
            })
            .saturating_mul(3)
    }

    pub(super) fn parse(value: &Value) -> Result<Self, HttpControlError> {
        let values = value
            .as_object()
            .ok_or_else(|| invalid("BitTorrent options must be an object"))?;
        let registry = builtin_registry();
        let mut values = values.clone();
        for definition in registry
            .definitions()
            .iter()
            .filter(|definition| task_option(definition.name))
        {
            if let Some(default) = definition.default {
                values
                    .entry(definition.name.to_owned())
                    .or_insert_with(|| json!(default));
            }
        }
        let mut result = Self {
            settings: BtTaskSettings::default(),
            mapping: MappingOptions::default(),
            persisted: SanitizedOptionMap::new([]).expect("empty options"),
            resume_limit: 16 * 1024 * 1024,
            timeout: Duration::from_secs(30),
            paused: false,
            directory: None,
        };
        let mut persisted = BTreeMap::new();
        for (name, input) in &values {
            let text = scalar(input)?;
            if name == "pause" {
                result.paused = match text.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(invalid("pause must be boolean")),
                };
                continue;
            }
            if name == "dir" {
                if text.is_empty() {
                    return Err(invalid("empty BitTorrent output directory"));
                }
                result.directory = Some(text);
                continue;
            }
            let definition = registry
                .find(name)
                .ok_or_else(|| invalid("unsupported BitTorrent option"))?;
            let parsed = parse_option_value(definition, &text, None)
                .map_err(|_| invalid("invalid BitTorrent option"))?;
            let text = canonical_option_value(&parsed)?;
            let number = || {
                text.parse::<u64>()
                    .map_err(|_| invalid("invalid BitTorrent integer option"))
            };
            match name.as_str() {
                "max-download-limit" => {
                    result.settings.download_limit = u32::try_from(number()?)
                        .ok()
                        .filter(|value| *value <= i32::MAX as u32)
                        .ok_or_else(|| invalid("BitTorrent download limit is too large"))?
                }
                "max-upload-limit" => result.settings.upload_limit = number()? as u32,
                "bt-max-peers" => result.settings.peers = number()? as u32,
                "enable-dht" => result.settings.dht = text == "true",
                "enable-peer-exchange" => result.settings.pex = text == "true",
                "bt-metadata-only" => result.settings.metadata_only = text == "true",
                "bt-save-metadata" => result.settings.save_metadata = text == "true",
                "seed-ratio" => {
                    result.settings.seed_ratio_milli =
                        decimal(&text, 1000.0, u32::MAX as u64)? as u32
                }
                "seed-time" => {
                    result.settings.seed_seconds = Some(decimal(&text, 60.0, 31_536_000)?)
                }
                "bt-resume-data-limit" => result.resume_limit = number()? as usize,
                "bt-resume-timeout" => result.timeout = Duration::from_secs(number()?),
                "select-file" => result.mapping.selected = Some(selection(&text)?),
                "out" => {
                    SafePathBuilder::from_user_path(&text, PathPlatform::Windows)
                        .map_err(|_| invalid("unsafe BitTorrent output path"))?;
                    result.mapping.output = Some(text.clone());
                }
                "index-out" => {
                    for item in text.split('\n') {
                        let (index, path) = item
                            .split_once('=')
                            .ok_or_else(|| invalid("index-out requires INDEX=PATH"))?;
                        let index: u32 = index
                            .parse()
                            .map_err(|_| invalid("invalid index-out index"))?;
                        if index == 0
                            || index > 10_000
                            || result
                                .mapping
                                .index_out
                                .insert(index, path.to_owned())
                                .is_some()
                        {
                            return Err(invalid("invalid index-out index"));
                        }
                        SafePathBuilder::from_user_path(path, PathPlatform::Windows)
                            .map_err(|_| invalid("unsafe index-out path"))?;
                    }
                }
                "bt-tracker" | "bt-exclude-tracker" => {
                    if text.len() > 65536 || text.split(',').count() > 64 {
                        return Err(invalid("tracker option exceeds its bound"));
                    }
                    for tracker in text.split(',') {
                        if name == "bt-exclude-tracker" && tracker == "*" {
                            continue;
                        }
                        ariax_bt::validate_tracker(tracker).map_err(bt_error)?;
                    }
                }
                _ => return Err(invalid("unsupported BitTorrent option")),
            }
            persisted.insert(name.clone(), text);
        }
        result.persisted = SanitizedOptionMap::new(persisted)
            .map_err(|_| invalid("unsafe BitTorrent persistence options"))?;
        Ok(result)
    }
}

pub(super) struct ResumeData {
    pub(super) bytes: Arc<[u8]>,
    _memory: BytePermit,
}

fn retained_resume(
    bytes: Arc<[u8]>,
    resident: &ByteBudget,
) -> Result<Option<Arc<ResumeData>>, HttpControlError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let memory = resident
        .try_acquire(bytes.len().saturating_add(128))
        .map_err(|_| HttpControlError::Busy)?;
    Ok(Some(Arc::new(ResumeData {
        bytes,
        _memory: memory,
    })))
}

pub(super) struct Spec {
    pub(super) resume_data: Option<Arc<ResumeData>>,
    pub(super) task_id: TaskId,
    pub(super) record: Arc<SessionBtTaskRecord>,
    pub(super) root: ProtectedRoot,
    pub(super) options: Options,
    pub(super) metadata: Option<TorrentMetadata>,
    _memory: BytePermit,
}

impl Spec {
    fn files(&self) -> Vec<FileMapping> {
        self.record
            .binding
            .files
            .iter()
            .map(|file| FileMapping {
                index: file.index,
                path: file.path.clone(),
                length: file.length,
                offset: file.offset,
                selected: file.selected,
                padding: file.padding,
            })
            .collect()
    }
    fn total_length(&self) -> u64 {
        self.record
            .binding
            .files
            .iter()
            .filter(|file| file.selected && !file.padding)
            .map(|file| file.length)
            .sum()
    }
    pub(super) fn retained_bytes(&self) -> usize {
        self.record
            .binding
            .owned_bytes()
            .saturating_mul(3)
            .saturating_add(self.options.retained_bytes())
            .saturating_add(128 * 1024)
    }
}

pub(super) struct QueryTask {
    pub(super) resume_data: Option<Arc<ResumeData>>,
    pub(super) spec: Arc<Spec>,
    pub(super) snapshot: Option<Arc<BtSnapshot>>,
    pub(super) peers: Arc<Vec<BtPeer>>,
    pub(super) dirty: bool,
    pub(super) checkpoint_failed: bool,
    pub(super) downloaded: u64,
    pub(super) uploaded: u64,
    pub(super) seed_millis: u64,
}

pub(super) struct PreparedAdmission {
    pub(super) spec: Arc<Spec>,
    adapter: Option<BtAdapter>,
    position: Option<usize>,
}
pub(super) struct PendingAdmission {
    preparation: Receiver<Result<PreparedAdmission, HttpControlError>>,
    ready: Option<PreparedAdmission>,
    reply: Option<oneshot::Sender<Result<Value, HttpControlError>>>,
    work: ControlWorkReservation,
    _request: crate::rpc_budget::RpcRequestLease,
    validation: Option<MappingValidation>,
}

/// A checked catalog remains current across progress-only publications.
struct MappingValidation {
    http: Arc<crate::HttpTaskCatalog>,
    revision: Arc<()>,
    result: Receiver<Result<(), HttpControlError>>,
}

fn validate_current_mapping(
    pending: &mut Option<MappingValidation>,
    spec: &Arc<Spec>,
    cpu: &ariax_runtime::CpuPool,
    http: &SharedHttpTaskCatalog,
    catalog: &Arc<BTreeMap<Gid, Arc<QueryTask>>>,
    revision: &Arc<()>,
) -> Result<bool, HttpControlError> {
    if let Some(validation) = pending {
        if receive(&validation.result)?.is_none() {
            return Ok(false);
        }
        let current = Arc::ptr_eq(&validation.http, &http.snapshot())
            && Arc::ptr_eq(&validation.revision, revision);
        *pending = None;
        if current {
            return Ok(true);
        }
    }
    let http = http.snapshot();
    let checked_http = http.clone();
    let catalog = catalog.clone();
    let spec = spec.clone();
    let result = spawn(cpu, move || collision(&spec, &checked_http, &catalog))?;
    *pending = Some(MappingValidation {
        http,
        revision: revision.clone(),
        result,
    });
    Ok(false)
}

pub(super) struct BtControl {
    mapping_revision: Arc<()>,
    upload_limit: u32,
    upload_applied: Option<u32>,
    bandwidth: ariax_bt::BandwidthAllocation,
    rate_updates: VecDeque<ariax_bt::BandwidthUpdate>,
    rate_pending: Option<BtPending>,
    rate_applied: bool,
    pub(super) config: BtAdapterConfig,
    pub(super) resources: Option<BtResources>,
    recovered_share: Option<BytePermit>,
    adapter: Option<BtAdapter>,
    initializing: Option<Receiver<Result<BtAdapter, HttpControlError>>>,
    pub(super) catalog: Arc<BTreeMap<Gid, Arc<QueryTask>>>,
    tasks: BTreeMap<TaskId, Task>,
    pub(super) admission: Option<PendingAdmission>,
    cursor: Option<TaskId>,
}

impl Default for BtControl {
    fn default() -> Self {
        Self {
            mapping_revision: Arc::new(()),
            upload_limit: 0,
            upload_applied: None,
            bandwidth: ariax_bt::BandwidthAllocation {
                bt: None,
                transfer: None,
            },
            rate_updates: VecDeque::new(),
            rate_pending: None,
            rate_applied: false,
            config: BtAdapterConfig::default(),
            resources: None,
            recovered_share: None,
            adapter: None,
            initializing: None,
            catalog: Arc::new(BTreeMap::new()),
            tasks: BTreeMap::new(),
            admission: None,
            cursor: None,
        }
    }
}

impl BtControl {
    pub(super) fn poll_rates(
        &mut self,
        rate: Option<&RateArbiter>,
    ) -> Result<bool, HttpControlError> {
        use ariax_bt::{BandwidthGroup, BandwidthUpdate};
        if (!self.rate_applied || self.upload_applied != Some(self.upload_limit))
            && self.handle().is_some()
            && self.rate_updates.is_empty()
        {
            self.rate_updates.push_back(BandwidthUpdate {
                group: BandwidthGroup::Bt,
                limit: self.bandwidth.bt,
            });
        }
        let Some(update) = self.rate_updates.front().copied() else {
            return Ok(false);
        };
        match update.group {
            BandwidthGroup::Transfer => {
                if let Some(rate) = rate {
                    rate.set_global_allocation(update.limit);
                }
                self.bandwidth.transfer = update.limit;
                self.rate_updates.pop_front();
            }
            BandwidthGroup::Bt => {
                if let Some(mut pending) = self.rate_pending.take() {
                    match pending.try_take().map_err(bt_error)? {
                        Some(BtReply::Applied { version: 0 }) => {
                            self.bandwidth.bt = update.limit;
                            self.rate_applied = true;
                            self.upload_applied = Some(self.upload_limit);
                            self.rate_updates.pop_front();
                        }
                        None => {
                            self.rate_pending = Some(pending);
                            return Ok(false);
                        }
                        _ => return Err(bt_error(BtError::StaleCompletion)),
                    }
                } else if let Some(handle) = self.handle() {
                    match handle.submit(BtCommand::SetRates {
                        download: update.limit,
                        upload: self.upload_limit,
                    }) {
                        Ok(pending) => self.rate_pending = Some(pending),
                        Err(BtError::Overloaded) => return Ok(false),
                        Err(error) => return Err(bt_error(error)),
                    }
                } else {
                    self.bandwidth.bt = update.limit;
                    self.rate_updates.pop_front();
                }
            }
        }
        Ok(true)
    }

    pub(super) fn require_bandwidth(
        &mut self,
        total: u64,
        upload: u32,
        bt: bool,
        transfer: bool,
        rate: Option<&RateArbiter>,
    ) -> Result<bool, HttpControlError> {
        self.poll_rates(rate)?;
        if !self.rate_updates.is_empty() {
            return Ok(false);
        }
        self.upload_limit = upload;
        let desired = ariax_bt::split_bandwidth((total != 0).then_some(total), bt, transfer);
        if self.bandwidth != desired {
            self.rate_updates = ariax_bt::bandwidth_updates(self.bandwidth, desired).into();
        }
        self.poll_rates(rate)?;
        Ok(self.rate_updates.is_empty()
            && (self.handle().is_none()
                || self.rate_applied && self.upload_applied == Some(upload)))
    }

    pub(super) fn begin_shutdown(&mut self) {
        for task in self.tasks.values_mut() {
            task.shutdown = true;
        }
    }

    pub(super) fn poll_stopped(&mut self) -> bool {
        if self.admission.is_some()
            || self.initializing.is_some()
            || self.rate_pending.is_some()
            || !self.rate_updates.is_empty()
            || self.tasks.values().any(Task::owns_work)
        {
            return false;
        }
        if let Some(adapter) = &mut self.adapter {
            adapter.request_stop();
            adapter.poll_stopped()
        } else {
            true
        }
    }
    pub(super) fn clean(&self) -> bool {
        self.tasks.values().all(|task| !task.checkpoint_failed)
    }
    pub(super) fn attach_resources(
        &mut self,
        resources: BtResources,
    ) -> Result<(), HttpControlError> {
        if self.adapter.is_some() || self.initializing.is_some() || self.admission.is_some() {
            return Err(HttpControlError::Busy);
        }
        if !self.tasks.is_empty() {
            self.recovered_share = Some(
                resources
                    .resident
                    .try_acquire(self.retained_bytes())
                    .map_err(|_| HttpControlError::Busy)?,
            );
        }
        self.resources = Some(resources);
        Ok(())
    }

    pub(super) fn terminal_step(
        &self,
        task_id: TaskId,
        generation: Generation,
        status: Aria2Status,
        error: Option<&PublicError>,
        transition: ariax_storage::SessionQueueTransition,
    ) -> Result<PersistencePlanStep, HttpControlError> {
        let task = self.tasks.get(&task_id).ok_or(HttpControlError::NotFound)?;
        if task.generation != generation || task.native_added {
            return Err(bt_error(BtError::StaleCompletion));
        }
        let result = SessionStoppedResultRecord {
            gid: task.spec.record.gid,
            status: match status {
                Aria2Status::Complete => SessionTerminalStatus::Complete,
                Aria2Status::Error => SessionTerminalStatus::Error,
                Aria2Status::Removed => SessionTerminalStatus::Removed,
                _ => return Err(bt_error(BtError::StaleCompletion)),
            },
            error_kind: error.map(PublicError::kind),
            safe_message: error.map_or_else(String::new, |error| error.safe_message().to_owned()),
            total_length: (status == Aria2Status::Complete).then(|| task.spec.total_length()),
            layout_hash: (status == Aria2Status::Complete)
                .then(|| task.spec.record.binding.layout_hash()),
            completed_ms: now_unix_ms(),
        };
        Ok(PersistencePlanStep::PersistBtTerminal {
            result,
            transition,
            generation: generation.get(),
            request: task.request,
        })
    }
    pub(super) fn contains(&self, task: TaskId) -> bool {
        self.tasks.contains_key(&task)
    }
    pub(super) fn handle(&self) -> Option<BtHandle> {
        self.adapter.as_ref().map(BtAdapter::handle)
    }
    pub(super) fn retained_bytes(&self) -> usize {
        self.catalog
            .values()
            .map(|task| task.spec.retained_bytes())
            .sum()
    }
    fn publish(&mut self, id: TaskId) {
        let task = &self.tasks[&id];
        if self
            .catalog
            .get(&task.spec.record.gid)
            .is_none_or(|previous| !Arc::ptr_eq(&previous.spec.record, &task.spec.record))
        {
            self.mapping_revision = Arc::new(());
        }
        Arc::make_mut(&mut self.catalog).insert(
            task.spec.record.gid,
            Arc::new(QueryTask {
                resume_data: task.resume_data.clone(),
                spec: task.spec.clone(),
                snapshot: task.snapshot.clone(),
                peers: task.peers.clone(),
                dirty: task.dirty,
                checkpoint_failed: task.checkpoint_failed,
                downloaded: task.downloaded(),
                uploaded: task.uploaded(),
                seed_millis: task.seed_millis(),
            }),
        );
    }
    pub(super) fn install(&mut self, spec: Arc<Spec>) {
        let id = spec.task_id;
        self.tasks.insert(id, Task::new(spec));
        self.publish(id);
    }
    pub(super) fn remove(&mut self, task: TaskId) {
        if let Some(task) = self.tasks.remove(&task) {
            self.mapping_revision = Arc::new(());
            Arc::make_mut(&mut self.catalog).remove(&task.spec.record.gid);
        }
    }

    pub(super) fn poll(
        &mut self,
        runtime: &crate::RuntimeEffectHandle,
        session: &SessionHandle,
        cpu: &ariax_runtime::CpuPool,
        http: &SharedHttpTaskCatalog,
    ) -> Result<bool, HttpControlError> {
        if self.adapter.is_none() && !self.tasks.is_empty() {
            if let Some(receiver) = &self.initializing {
                if let Some(adapter) = receive(receiver)? {
                    self.adapter = Some(adapter);
                    self.rate_applied = false;
                    self.initializing = None;
                }
            } else if self.admission.is_none() {
                let config = self.config.clone();
                let resources = self
                    .resources
                    .clone()
                    .ok_or(HttpControlError::InvalidConfig)?;
                self.initializing = Some(spawn(cpu, move || {
                    BtAdapter::start(config, resources).map_err(bt_error)
                })?);
            }
            return Ok(true);
        }
        let Some(handle) = self.handle() else {
            return Ok(false);
        };
        if !self.rate_applied || !self.rate_updates.is_empty() {
            return Ok(false);
        }

        if let Some(request) =
            runtime.take_allocation_matching(|task| self.tasks.contains_key(&task))
        {
            let task = self
                .tasks
                .get_mut(&request.task_id())
                .expect("registered BT task");
            if !matches!(task.phase, Phase::Idle | Phase::Finished) {
                return Err(bt_error(BtError::StaleCompletion));
            }
            task.generation = request.generation();
            task.request = 0;
            task.dirty = true;
            task.allocation = Some(request);
            task.phase = Phase::LoadResume;
            return Ok(true);
        }
        if let Some(request) =
            runtime.take_cancellation_matching(|task| self.tasks.contains_key(&task))
        {
            let task = self
                .tasks
                .get_mut(&request.task_id())
                .expect("registered BT task");
            task.cancellation = Some(request);
            return Ok(true);
        }
        if handle.needs_reconciliation() {
            for task in self.tasks.values_mut() {
                task.checkpoint_at = Instant::now();
            }
        }
        let _ = handle.event(); // Notifications only wake reconciliation; snapshots are authoritative.
        let Some(id) = self
            .tasks
            .range((
                self.cursor
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(|(id, _)| *id)
            .or_else(|| self.tasks.keys().next().copied())
        else {
            return Ok(false);
        };
        self.cursor = Some(id);
        let resources = self
            .resources
            .as_ref()
            .ok_or(HttpControlError::InvalidConfig)?;
        let changed = self.tasks.get_mut(&id).expect("BT task").poll(
            &handle,
            runtime,
            session,
            cpu,
            http,
            &self.catalog,
            &self.mapping_revision,
            &resources.resident,
        )?;
        if changed {
            self.publish(id);
        }
        Ok(changed)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Idle,
    LoadResume,
    PrepareAdd,
    Add,
    Metadata,
    PrepareBinding,
    Bind,
    SaveMetadata,
    Approve,
    Dirty,
    Resume,
    Running,
    Checkpoint,
    PrepareCheckpoint,
    PersistCheckpoint,
    Remove,
    Finished,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Boundary {
    Periodic,
    Cancel,
    Complete,
    Failure,
    Shutdown,
}

struct Task {
    mapping_validation: Option<MappingValidation>,
    resume_data: Option<Arc<ResumeData>>,
    option_change: Option<OptionChange>,
    settings_version: u64,
    shutdown: bool,
    spec: Arc<Spec>,
    generation: Generation,
    phase: Phase,
    boundary: Boundary,
    allocation: Option<crate::AllocationRequest>,
    active: Option<crate::ActiveTransferRequest>,
    seeding: Option<crate::runtime_effects::SeedingRequest>,
    cancellation: Option<crate::CancellationRequest>,
    event: Option<crate::RuntimeEventSubmission>,
    native: Option<BtPending>,
    native_offered: Option<BtCommand>,
    store: Option<SessionCompletion>,
    offered: Option<SessionCommand>,
    preparation: Option<Receiver<Result<Box<BtAdmission>, HttpControlError>>>,
    binding: Option<Receiver<Result<Arc<Spec>, HttpControlError>>>,
    prepared_spec: Option<Arc<Spec>>,
    save: Option<Receiver<Result<(), HttpControlError>>>,
    add: Option<Box<BtAdmission>>,
    resume: Option<ariax_storage::SessionBtResumeRecord>,
    checkpoint: Option<Arc<OwnedBlob>>,
    checkpoint_preparation:
        Option<Receiver<Result<(Arc<SessionBtCheckpoint>, BytePermit), HttpControlError>>>,
    prepared_checkpoint: Option<(Arc<SessionBtCheckpoint>, BytePermit)>,
    snapshot: Option<Arc<BtSnapshot>>,
    peers: Arc<Vec<BtPeer>>,
    request: u64,
    dirty: bool,
    checkpoint_failed: bool,
    native_added: bool,
    checkpoint_at: Instant,
    peers_at: Instant,
    seeded_since: Option<Instant>,
    seed_base: u64,
    download_base: u64,
    upload_base: u64,
    native_download_start: u64,
    native_upload_start: u64,
    failure: Option<PublicError>,
}

impl Task {
    fn owns_work(&self) -> bool {
        self.native_added
            || self.native.is_some()
            || self.native_offered.is_some()
            || self.store.is_some()
            || self.offered.is_some()
            || self.event.is_some()
            || self.option_change.is_some()
            || self.preparation.is_some()
            || self.binding.is_some()
            || self.mapping_validation.is_some()
            || self.save.is_some()
            || self.checkpoint_preparation.is_some()
            || !matches!(self.phase, Phase::Idle | Phase::Finished)
    }

    fn new(spec: Arc<Spec>) -> Self {
        let now = Instant::now();
        Self {
            mapping_validation: None,
            resume_data: spec.resume_data.clone(),
            shutdown: false,
            option_change: None,
            settings_version: 0,
            generation: Generation::new(spec.record.generation),
            seed_base: spec.record.seed_millis,
            download_base: spec.record.downloaded,
            upload_base: spec.record.uploaded,
            spec,
            phase: Phase::Idle,
            boundary: Boundary::Periodic,
            allocation: None,
            active: None,
            seeding: None,
            cancellation: None,
            event: None,
            native: None,
            native_offered: None,
            store: None,
            offered: None,
            preparation: None,
            binding: None,
            prepared_spec: None,
            save: None,
            add: None,
            resume: None,
            checkpoint: None,
            snapshot: None,
            peers: Arc::new(Vec::new()),
            request: 0,
            checkpoint_preparation: None,
            prepared_checkpoint: None,
            native_download_start: 0,
            native_upload_start: 0,
            dirty: true,
            checkpoint_failed: false,
            native_added: false,
            checkpoint_at: now + CHECKPOINT_INTERVAL,
            peers_at: now,
            seeded_since: None,
            failure: None,
        }
    }
    fn downloaded(&self) -> u64 {
        self.download_base
            .saturating_add(self.snapshot.as_ref().map_or(0, |s| {
                s.downloaded.saturating_sub(self.native_download_start)
            }))
    }
    fn uploaded(&self) -> u64 {
        self.upload_base.saturating_add(
            self.snapshot
                .as_ref()
                .map_or(0, |s| s.uploaded.saturating_sub(self.native_upload_start)),
        )
    }
    fn seed_millis(&self) -> u64 {
        self.seed_base
            .saturating_add(self.seeded_since.map_or(0, |at| {
                at.elapsed().as_millis().min(u64::MAX as u128) as u64
            }))
    }
    fn begin_boundary(&mut self, boundary: Boundary) -> Result<(), HttpControlError> {
        self.boundary = boundary;
        self.request = self
            .request
            .checked_add(1)
            .ok_or_else(|| bt_error(BtError::StaleCompletion))?;
        self.phase = if self.native_added {
            Phase::Checkpoint
        } else {
            Phase::PrepareCheckpoint
        };
        Ok(())
    }
    fn store(
        &mut self,
        session: &SessionHandle,
        command: impl FnOnce() -> SessionCommand,
    ) -> Poll<Result<SessionCommandResult, HttpControlError>> {
        if let Some(completion) = self.store.take() {
            return match completion.try_wait() {
                Ok(None) => {
                    self.store = Some(completion);
                    Poll::Pending
                }
                Ok(Some(result)) => Poll::Ready(Ok(result)),
                Err(error) => Poll::Ready(Err(HttpControlError::Persistence(error.to_string()))),
            };
        }
        let command = self.offered.take().unwrap_or_else(command);
        match session.try_submit_owned(command) {
            Ok(completion) => self.store = Some(completion),
            Err(rejection) => {
                let (command, error) = rejection.into_boxed_parts();
                if matches!(error, SessionOwnerError::QueueFull) {
                    self.offered = Some(*command);
                } else {
                    return Poll::Ready(Err(HttpControlError::Persistence(error.to_string())));
                }
            }
        }
        Poll::Pending
    }
    fn native(
        &mut self,
        handle: &BtHandle,
        command: impl FnOnce() -> BtCommand,
    ) -> Poll<Result<BtReply, BtError>> {
        if let Some(mut pending) = self.native.take() {
            return match pending.try_take() {
                Ok(None) => {
                    self.native = Some(pending);
                    Poll::Pending
                }
                other => Poll::Ready(other.and_then(|reply| reply.ok_or(BtError::Closed))),
            };
        }
        let command = self.native_offered.take().unwrap_or_else(command);
        match handle.try_submit_owned(command) {
            Ok(pending) => {
                self.native = Some(pending);
                Poll::Pending
            }
            Err((command, BtError::Overloaded)) => {
                self.native_offered = Some(command);
                Poll::Pending
            }
            Err((_, error)) => Poll::Ready(Err(error)),
        }
    }
}

impl Task {
    #[allow(clippy::too_many_arguments)]
    fn poll(
        &mut self,
        handle: &BtHandle,
        runtime: &crate::RuntimeEffectHandle,
        session: &SessionHandle,
        cpu: &ariax_runtime::CpuPool,
        http: &SharedHttpTaskCatalog,
        catalog: &Arc<BTreeMap<Gid, Arc<QueryTask>>>,
        mapping_revision: &Arc<()>,
        resident: &ByteBudget,
    ) -> Result<bool, HttpControlError> {
        let gid = self.spec.record.gid;
        if let Some(event) = self.event.take() {
            if let Err(rejection) = runtime.try_submit_event(event) {
                self.event = Some(rejection.into_submission());
                return Ok(false);
            }
            return Ok(true);
        }
        if self
            .option_change
            .as_ref()
            .is_some_and(|change| change.started)
            || self.option_change.is_some()
                && self.native.is_none()
                && self.native_offered.is_none()
                && self.store.is_none()
                && matches!(self.phase, Phase::Idle | Phase::Finished | Phase::Running)
        {
            return self.poll_options(handle, session);
        }
        if let Some(snapshot) = handle.snapshot(gid.get()) {
            self.snapshot = Some(snapshot);
        }
        // Every accepted operation is driven to completion before the cancellation
        // boundary. Dropping a public caller never cancels the accepted native work.
        if (self.cancellation.is_some() || self.shutdown)
            && self.native.is_none()
            && self.native_offered.is_none()
            && self.store.is_none()
            && self.offered.is_none()
            && self.mapping_validation.is_none()
            && matches!(
                self.phase,
                Phase::Idle
                    | Phase::Finished
                    | Phase::Metadata
                    | Phase::Running
                    | Phase::Approve
                    | Phase::Dirty
                    | Phase::Resume
            )
        {
            if !self.native_added {
                self.allocation = None;
                self.active = None;
                self.seeding = None;
                self.event = self
                    .cancellation
                    .take()
                    .map(crate::CancellationRequest::drained);
                self.phase = Phase::Idle;
                return Ok(true);
            }
            self.begin_boundary(if self.shutdown {
                Boundary::Shutdown
            } else {
                Boundary::Cancel
            })?;
        }
        match self.phase {
            Phase::Idle | Phase::Finished => return Ok(false),
            Phase::LoadResume => {
                let limit = self.spec.options.resume_limit;
                if let Poll::Ready(result) =
                    self.store(session, || SessionCommand::ReadBtResume { gid, limit })
                {
                    let SessionCommandResult::BtResume(resume) = result? else {
                        return Err(bt_error(BtError::StaleCompletion));
                    };
                    self.resume_data = retained_resume(resume.resume_blob.clone(), resident)?;
                    self.resume = Some(resume);
                    self.snapshot = None;
                    self.native_download_start = 0;
                    self.native_upload_start = 0;
                    self.phase = Phase::PrepareAdd;
                }
            }
            Phase::PrepareAdd => {
                if let Some(receiver) = &self.preparation {
                    if let Some(add) = receive(receiver)? {
                        self.add = Some(add);
                        self.preparation = None;
                        self.phase = Phase::Add;
                    }
                } else {
                    let spec = self.spec.clone();
                    let handle = handle.clone();
                    let resume = self.resume.take();
                    self.preparation = Some(spawn(cpu, move || {
                        spec.root.revalidate().map_err(bt_error)?;
                        if spec.root.identity().as_ref() != spec.record.binding.root_identity {
                            return Err(bt_error(BtError::IdentityMismatch));
                        }
                        let binding = &spec.record.binding;
                        let mapping = spec.files();
                        spec.root
                            .validate_mapping(&mapping, true)
                            .map_err(bt_error)?;
                        // A dirty snapshot is a progress hint only. Force native piece
                        // verification by omitting its cached completion bitfield.
                        let resume = resume
                            .filter(|resume| !resume.resume_blob.is_empty())
                            .map(|resume| handle.blob(resume.resume_blob.to_vec()))
                            .transpose()
                            .map_err(bt_error)?;
                        let torrent = (!binding.metainfo.is_empty())
                            .then(|| handle.blob(binding.metainfo.clone()))
                            .transpose()
                            .map_err(bt_error)?;
                        Ok(Box::new(BtAdmission {
                            gid: spec.record.gid.get(),
                            root: spec.root.clone(),
                            magnet: torrent.is_none().then(|| binding.magnet.clone()).flatten(),
                            torrent,
                            resume,
                            mapping: spec.options.mapping.clone(),
                            settings: spec.options.settings.clone(),
                            expected_identity: Some(binding.identity.clone()),
                            expected_mapping: (!mapping.is_empty()).then_some(mapping),
                            allow_existing: true,
                        }))
                    })?);
                }
            }
            Phase::Add => {
                let add = self.add.take();
                let result = self.native(handle, || {
                    BtCommand::Add(add.expect("prepared BT admission"))
                });
                match result {
                    Poll::Ready(Ok(_)) => {
                        self.native_added = true;
                        self.phase = Phase::Metadata;
                    }
                    Poll::Ready(Err(error)) => {
                        self.failure = Some(public_error(error));
                        self.begin_boundary(Boundary::Failure)?;
                    }
                    Poll::Pending => {}
                }
            }
            Phase::Metadata => {
                if !self
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.metadata)
                {
                    return Ok(false);
                }
                if let Poll::Ready(result) =
                    self.native(handle, || BtCommand::ReadMetadata { gid: gid.get() })
                {
                    match result {
                        Ok(BtReply::Metadata {
                            info,
                            metadata,
                            mapping,
                        }) => {
                            if let Some(snapshot) = &self.snapshot {
                                self.native_download_start = snapshot.downloaded;
                                self.native_upload_start = snapshot.uploaded;
                            }
                            let previous = self.spec.clone();
                            let resident = resident.clone();
                            let http = http.snapshot();
                            let catalog = catalog.clone();
                            self.binding = Some(spawn(cpu, move || {
                                let memory = resident
                                    .try_acquire(
                                        previous
                                            .retained_bytes()
                                            .saturating_add(info.bytes().len().saturating_mul(4))
                                            .saturating_add(mapping.len().saturating_mul(8192)),
                                    )
                                    .map_err(|_| HttpControlError::Busy)?;
                                let mut record = (*previous.record).clone();
                                record.binding.identity = metadata.identity.clone();
                                record.binding.info = info.bytes().to_vec();
                                if record.binding.metainfo.is_empty() {
                                    record.binding.metainfo =
                                        torrent_from_info(info.bytes(), MetadataLimits::default())
                                            .map_err(bt_error)?;
                                }
                                record.binding.files = files(&mapping);
                                record
                                    .binding
                                    .validate()
                                    .map_err(|_| invalid("invalid persisted BitTorrent binding"))?;
                                previous
                                    .root
                                    .validate_mapping(&mapping, true)
                                    .map_err(bt_error)?;
                                let spec = Arc::new(Spec {
                                    resume_data: previous.resume_data.clone(),
                                    task_id: previous.task_id,
                                    record: Arc::new(record),
                                    root: previous.root.clone(),
                                    options: previous.options.clone(),
                                    metadata: Some(metadata),
                                    _memory: memory,
                                });
                                collision(&spec, &http, &catalog)?;
                                Ok(spec)
                            })?);
                            self.phase = Phase::PrepareBinding;
                        }
                        Ok(_) => return Err(bt_error(BtError::StaleCompletion)),
                        Err(error) => {
                            self.failure = Some(public_error(error));
                            self.begin_boundary(Boundary::Failure)?;
                        }
                    }
                }
            }
            Phase::PrepareBinding => {
                let receiver = self.binding.as_ref().expect("metadata preparation");
                match receive(receiver) {
                    Ok(Some(spec)) => {
                        self.prepared_spec = Some(spec);
                        self.binding = None;
                        self.phase = Phase::Bind;
                    }
                    Ok(None) => return Ok(false),
                    Err(_) => {
                        self.binding = None;
                        self.failure = Some(public_error(BtError::InvalidMetadata));
                        self.begin_boundary(Boundary::Failure)?;
                    }
                }
            }
            Phase::Bind => {
                let spec = self
                    .prepared_spec
                    .as_ref()
                    .expect("prepared metadata")
                    .clone();
                let generation = self.generation.get();
                if let Poll::Ready(result) =
                    self.store(session, || SessionCommand::BindBtMetadata {
                        gid,
                        generation,
                        binding: Arc::new(spec.record.binding.clone()),
                    })
                {
                    expect_unit(result?)?;
                    self.spec = self.prepared_spec.take().expect("bound metadata");
                    self.phase = Phase::SaveMetadata;
                }
            }
            Phase::SaveMetadata => {
                if !self.spec.options.settings.save_metadata {
                    self.phase = Phase::Approve;
                } else if let Some(receiver) = &self.save {
                    if receive(receiver)?.is_some() {
                        self.save = None;
                        self.phase = Phase::Approve;
                    }
                } else {
                    let spec = self.spec.clone();
                    self.save = Some(spawn(cpu, move || {
                        spec.root.revalidate().map_err(bt_error)?;
                        let identity = &spec.record.binding.identity;
                        let hash = identity
                            .v1
                            .as_ref()
                            .or(identity.v2.as_ref())
                            .ok_or_else(|| bt_error(BtError::IdentityMismatch))?;
                        let destination = ariax_storage::SessionExportDestination::new(
                            &spec.root.path().join(format!("{hash}.torrent")),
                        )
                        .map_err(|_| bt_error(BtError::UnsafePath))?;
                        destination
                            .publish(&spec.record.binding.metainfo)
                            .map_err(|_| bt_error(BtError::Native))
                    })?);
                }
            }
            Phase::Approve => {
                if self.native.is_none() && self.native_offered.is_none() {
                    match validate_current_mapping(
                        &mut self.mapping_validation,
                        &self.spec,
                        cpu,
                        http,
                        catalog,
                        mapping_revision,
                    ) {
                        Ok(false) => return Ok(false),
                        Ok(true) => {
                            if self.cancellation.is_some() || self.shutdown {
                                self.begin_boundary(if self.shutdown {
                                    Boundary::Shutdown
                                } else {
                                    Boundary::Cancel
                                })?;
                                return Ok(true);
                            }
                        }
                        Err(_) => {
                            self.mapping_validation = None;
                            self.failure = Some(public_error(BtError::Collision));
                            self.begin_boundary(Boundary::Failure)?;
                            return Ok(true);
                        }
                    }
                }
                if self.spec.options.settings.metadata_only {
                    let (event, active) = self.allocation.take().expect("BT allocation").activate();
                    self.event = Some(event);
                    self.active = Some(active);
                    self.phase = Phase::Running;
                } else {
                    let mapping = self.spec.files();
                    if let Poll::Ready(result) = self.native(handle, || BtCommand::Approve {
                        gid: gid.get(),
                        mapping,
                    }) {
                        match result {
                            Ok(_) => self.phase = Phase::Dirty,
                            Err(error) => {
                                self.failure = Some(public_error(error));
                                self.begin_boundary(Boundary::Failure)?;
                            }
                        }
                    }
                }
            }
            Phase::Dirty => {
                let generation = self.generation.get();
                if let Poll::Ready(result) =
                    self.store(session, || SessionCommand::MarkBtDirty { gid, generation })
                {
                    expect_unit(result?)?;
                    self.dirty = true;
                    self.phase = Phase::Resume;
                }
            }
            Phase::Resume => {
                if let Poll::Ready(result) =
                    self.native(handle, || BtCommand::Resume { gid: gid.get() })
                {
                    match result {
                        Ok(_) => {
                            if let Some(allocation) = self.allocation.take() {
                                let (event, active) = allocation.activate();
                                self.event = Some(event);
                                self.active = Some(active);
                            }
                            self.phase = Phase::Running;
                            self.checkpoint_at = Instant::now() + CHECKPOINT_INTERVAL;
                        }
                        Err(error) => {
                            self.failure = Some(public_error(error));
                            self.begin_boundary(Boundary::Failure)?;
                        }
                    }
                }
            }
            Phase::Running => {
                if self.native.is_some() || self.native_offered.is_some() {
                    let limit = self.spec.options.settings.peers.min(1000);
                    match self.native(handle, || BtCommand::Peers {
                        gid: gid.get(),
                        limit,
                    }) {
                        Poll::Pending => return Ok(false),
                        Poll::Ready(Ok(BtReply::Peers(peers))) => self.peers = Arc::new(peers),
                        Poll::Ready(Ok(_)) => return Err(bt_error(BtError::StaleCompletion)),
                        Poll::Ready(Err(_)) => {}
                    }
                    self.peers_at = Instant::now() + PEER_INTERVAL;
                    return Ok(true);
                }
                if let Some(snapshot) = &self.snapshot
                    && snapshot.error != 0
                {
                    self.failure = Some(public_error(BtError::Native));
                    self.begin_boundary(Boundary::Failure)?;
                } else if self.spec.options.settings.metadata_only
                    || self.snapshot.as_ref().is_some_and(|s| s.finished)
                {
                    if let Some(active) = self.active.take() {
                        let (event, seeding) = active.start_seeding();
                        self.event = Some(event);
                        self.seeding = Some(seeding);
                        self.seeded_since = Some(Instant::now());
                    } else if self.spec.options.settings.metadata_only || self.seed_goal_met() {
                        self.begin_boundary(Boundary::Complete)?;
                    }
                }
                if self.phase == Phase::Running && Instant::now() >= self.checkpoint_at {
                    self.begin_boundary(Boundary::Periodic)?;
                }
                if self.phase == Phase::Running && Instant::now() >= self.peers_at {
                    let limit = self.spec.options.settings.peers.min(1000);
                    if let Poll::Ready(result) = self.native(handle, || BtCommand::Peers {
                        gid: gid.get(),
                        limit,
                    }) {
                        match result {
                            Ok(BtReply::Peers(peers)) => self.peers = Arc::new(peers),
                            Ok(_) => return Err(bt_error(BtError::StaleCompletion)),
                            Err(_) => {}
                        }
                        self.peers_at = Instant::now() + PEER_INTERVAL;
                    }
                }
            }
            Phase::Checkpoint => {
                let request = self.request;
                let limit = self.spec.options.resume_limit;
                let timeout = self.spec.options.timeout;
                if let Poll::Ready(result) = self.native(handle, || BtCommand::Checkpoint {
                    gid: gid.get(),
                    request,
                    limit,
                    timeout,
                }) {
                    match result {
                        Ok(BtReply::Checkpoint {
                            request: completed,
                            data,
                        }) if completed == request => {
                            self.checkpoint = Some(data);
                            self.checkpoint_failed = false;
                        }
                        Err(error) => {
                            self.checkpoint_failed = true;
                            if self.boundary == Boundary::Complete
                                || self.boundary == Boundary::Periodic
                            {
                                self.failure = Some(public_error(error));
                                self.boundary = Boundary::Failure;
                            }
                        }
                        _ => return Err(bt_error(BtError::StaleCompletion)),
                    }
                    self.phase = Phase::PrepareCheckpoint;
                }
            }
            Phase::PrepareCheckpoint => {
                if let Some(receiver) = &self.checkpoint_preparation {
                    if let Some(prepared) = receive(receiver)? {
                        self.prepared_checkpoint = Some(prepared);
                        self.checkpoint_preparation = None;
                        self.phase = Phase::PersistCheckpoint;
                    }
                } else {
                    let blob = self.checkpoint.clone();
                    let resident = resident.clone();
                    let generation = self.generation.get();
                    let request = self.request;
                    let downloaded = self.downloaded();
                    let uploaded = self.uploaded();
                    let seed_millis = self.seed_millis();
                    self.checkpoint_preparation = Some(spawn(cpu, move || {
                        let memory = resident
                            .try_acquire(
                                blob.as_ref()
                                    .map_or(1024, |blob| blob.bytes().len().saturating_add(1024)),
                            )
                            .map_err(|_| HttpControlError::Busy)?;
                        Ok((
                            Arc::new(SessionBtCheckpoint {
                                gid,
                                generation,
                                request,
                                resume_blob: blob.map(|blob| Arc::from(blob.bytes())),
                                downloaded,
                                uploaded,
                                seed_millis,
                                saved_ms: now_unix_ms(),
                            }),
                            memory,
                        ))
                    })?);
                }
            }
            Phase::PersistCheckpoint => {
                let checkpoint = self
                    .prepared_checkpoint
                    .as_ref()
                    .expect("prepared checkpoint")
                    .0
                    .clone();
                if let Poll::Ready(result) =
                    self.store(session, || SessionCommand::CheckpointBt(checkpoint))
                {
                    expect_unit(result?)?;
                    self.dirty = self.checkpoint.is_none();
                    self.checkpoint = None;
                    if let Some((checkpoint, memory)) = self.prepared_checkpoint.take()
                        && let Some(bytes) = &checkpoint.resume_blob
                    {
                        self.resume_data = Some(Arc::new(ResumeData {
                            bytes: bytes.clone(),
                            _memory: memory,
                        }));
                    }
                    self.phase = if self.boundary == Boundary::Periodic
                        && self.cancellation.is_none()
                        && !self.shutdown
                    {
                        Phase::Dirty
                    } else {
                        Phase::Remove
                    };
                }
            }
            Phase::Remove => {
                let dirty = self.dirty;
                let result = if self.native_added {
                    self.native(handle, || {
                        if dirty {
                            BtCommand::RemoveDirty { gid: gid.get() }
                        } else {
                            BtCommand::Remove { gid: gid.get() }
                        }
                    })
                } else {
                    Poll::Ready(Ok(BtReply::Applied { version: 0 }))
                };
                match result {
                    Poll::Ready(Ok(_)) => {
                        self.native_added = false;
                        self.phase = Phase::Finished;
                        self.download_base = self.downloaded();
                        self.upload_base = self.uploaded();
                        self.native_download_start =
                            self.snapshot.as_ref().map_or(0, |s| s.downloaded);
                        self.native_upload_start = self.snapshot.as_ref().map_or(0, |s| s.uploaded);
                        self.seed_base = self.seed_millis();
                        self.seeded_since = None;
                        if let Some(cancellation) = self.cancellation.take() {
                            self.active = None;
                            self.allocation = None;
                            self.seeding = None;
                            self.event = Some(cancellation.drained());
                        } else if let Some(error) = self.failure.take() {
                            self.event = if let Some(seeding) = self.seeding.take() {
                                Some(seeding.failed(error))
                            } else if let Some(active) = self.active.take() {
                                Some(active.failed(error))
                            } else {
                                self.allocation
                                    .take()
                                    .map(|allocation| allocation.failed(error))
                            };
                        } else if self.boundary == Boundary::Complete {
                            self.event = self
                                .seeding
                                .take()
                                .map(crate::runtime_effects::SeedingRequest::complete);
                        }
                    }
                    Poll::Ready(Err(BtError::Overloaded)) | Poll::Pending => return Ok(false),
                    Poll::Ready(Err(error)) => return Err(bt_error(error)),
                }
            }
        }
        Ok(true)
    }

    fn seed_goal_met(&self) -> bool {
        let settings = &self.spec.options.settings;
        // aria2 ends seeding when either configured goal is reached. A zero
        // ratio/time requests no seeding; an absent time leaves ratio in control.
        let denominator = self.spec.total_length().max(self.downloaded()).max(1);
        settings.seed_ratio_milli == 0
            || u128::from(self.uploaded()) * 1000
                >= u128::from(denominator) * u128::from(settings.seed_ratio_milli)
            || settings
                .seed_seconds
                .is_some_and(|seconds| self.seed_millis() >= seconds.saturating_mul(1000))
    }
}

struct OptionChange {
    preparation: Receiver<Result<Arc<Spec>, HttpControlError>>,
    prepared: Option<Arc<Spec>>,
    reply: Option<oneshot::Sender<Result<Value, HttpControlError>>>,
    started: bool,
    applied: bool,
    native: Option<BtPending>,
    store: Option<SessionCompletion>,
    _request: crate::rpc_budget::RpcRequestLease,
}

impl Task {
    fn poll_options(
        &mut self,
        handle: &BtHandle,
        session: &SessionHandle,
    ) -> Result<bool, HttpControlError> {
        let mut change = self.option_change.take().expect("BT option operation");
        change.started = true;
        if change.prepared.is_none() {
            match receive(&change.preparation) {
                Ok(prepared) => change.prepared = prepared,
                Err(error) => {
                    let _ = change
                        .reply
                        .take()
                        .expect("BT option reply")
                        .send(Err(error));
                    return Ok(true);
                }
            }
        }
        let Some(prepared) = change.prepared.as_ref() else {
            self.option_change = Some(change);
            return Ok(false);
        };
        if !change.applied {
            if self.native_added {
                if let Some(mut pending) = change.native.take() {
                    match pending.try_take() {
                        Ok(None) => change.native = Some(pending),
                        Ok(Some(BtReply::Applied { version }))
                            if version == self.settings_version + 1 =>
                        {
                            change.applied = true;
                            self.settings_version = version;
                        }
                        Ok(_) => return Err(bt_error(BtError::StaleCompletion)),
                        Err(error) => {
                            let _ = change
                                .reply
                                .take()
                                .expect("BT option reply")
                                .send(Err(bt_error(error)));
                            return Ok(true);
                        }
                    }
                } else {
                    match handle.submit(BtCommand::SetTask {
                        gid: prepared.record.gid.get(),
                        version: self.settings_version + 1,
                        settings: prepared.options.settings.clone(),
                    }) {
                        Ok(pending) => change.native = Some(pending),
                        Err(BtError::Overloaded) => {}
                        Err(error) => {
                            let _ = change
                                .reply
                                .take()
                                .expect("BT option reply")
                                .send(Err(bt_error(error)));
                            return Ok(true);
                        }
                    }
                }
            } else {
                change.applied = true;
            }
        } else if let Some(completion) = change.store.take() {
            match completion
                .try_wait()
                .map_err(|error| HttpControlError::Persistence(error.to_string()))?
            {
                None => change.store = Some(completion),
                Some(result) => {
                    expect_unit(result)?;
                    self.spec = change.prepared.take().expect("acknowledged BT options");
                    let _ = change
                        .reply
                        .take()
                        .expect("BT option reply")
                        .send(Ok(json!("OK")));
                    return Ok(true);
                }
            }
        } else {
            match session.try_submit(SessionCommand::ReplaceTaskOptions {
                gid: prepared.record.gid,
                scope: OptionsSnapshotScope::CurrentGeneration,
                options: prepared.options.persisted.clone(),
            }) {
                Ok(completion) => change.store = Some(completion),
                Err(SessionOwnerError::QueueFull) => {}
                Err(error) => return Err(HttpControlError::Persistence(error.to_string())),
            }
        }
        self.option_change = Some(change);
        Ok(true)
    }
}

fn expect_unit(result: SessionCommandResult) -> Result<(), HttpControlError> {
    if matches!(result, SessionCommandResult::Unit) {
        Ok(())
    } else {
        Err(bt_error(BtError::StaleCompletion))
    }
}

fn files(mapping: &[FileMapping]) -> Vec<SessionBtFile> {
    mapping
        .iter()
        .map(|file| SessionBtFile {
            index: file.index,
            path: file.path.clone(),
            length: file.length,
            offset: file.offset,
            selected: file.selected,
            padding: file.padding,
        })
        .collect()
}

fn charge(
    record: &SessionBtTaskRecord,
    options: &Options,
    resident: &ByteBudget,
) -> Result<BytePermit, HttpControlError> {
    resident
        .try_acquire(
            record
                .binding
                .owned_bytes()
                .saturating_mul(3)
                .saturating_add(options.retained_bytes())
                .saturating_add(128 * 1024),
        )
        .map_err(|_| HttpControlError::Busy)
}

fn output_key(root: &Path, relative: &str) -> String {
    root.join(relative)
        .to_string_lossy()
        .replace('\\', "/")
        .to_lowercase()
}

fn paths_overlap(first: &str, second: &str) -> bool {
    first == second
        || first
            .strip_prefix(second)
            .is_some_and(|tail| tail.starts_with('/'))
        || second
            .strip_prefix(first)
            .is_some_and(|tail| tail.starts_with('/'))
}

pub(super) fn collision_transfer<'a>(
    transfer: &HttpTaskSpec,
    bt: impl Iterator<Item = &'a Spec>,
) -> Result<(), HttpControlError> {
    let root =
        std::fs::canonicalize(transfer.output_root()).map_err(|_| bt_error(BtError::UnsafePath))?;
    let path = output_key(&root, &transfer.output().canonical_string());
    for other in bt {
        if other
            .record
            .binding
            .files
            .iter()
            .filter(|file| !file.padding)
            .any(|file| paths_overlap(&path, &output_key(other.root.path(), &file.path)))
        {
            return Err(invalid(
                "transfer output overlaps an existing BitTorrent task",
            ));
        }
    }
    Ok(())
}

pub(super) fn collision(
    spec: &Spec,
    http: &crate::HttpTaskCatalog,
    bt: &BTreeMap<Gid, Arc<QueryTask>>,
) -> Result<(), HttpControlError> {
    let paths: Vec<_> = spec
        .record
        .binding
        .files
        .iter()
        .filter(|file| !file.padding)
        .map(|file| output_key(spec.root.path(), &file.path))
        .collect();
    for transfer in http.entries() {
        collision_transfer(transfer, std::iter::once(spec))?;
    }
    for other in bt
        .values()
        .filter(|other| other.spec.task_id != spec.task_id)
    {
        if other
            .spec
            .record
            .binding
            .identity
            .overlaps(&spec.record.binding.identity)
        {
            return Err(invalid("BitTorrent identity is already admitted"));
        }
        if other
            .spec
            .record
            .binding
            .files
            .iter()
            .filter(|file| !file.padding)
            .any(|file| {
                paths.iter().any(|path| {
                    paths_overlap(path, &output_key(other.spec.root.path(), &file.path))
                })
            })
        {
            return Err(invalid("BitTorrent output overlaps an existing task"));
        }
    }
    Ok(())
}

impl HttpControlPlane {
    pub(super) fn require_bt_bandwidth(
        &mut self,
        scheduler: &RequestScheduler,
        total: u64,
        upload: u32,
    ) -> Result<bool, HttpControlError> {
        let mut bt = false;
        let mut transfer = false;
        for gid in scheduler.queue_snapshot(QueueClass::Active) {
            if self.bt.catalog.contains_key(&gid) {
                bt = true;
            } else {
                transfer = true;
            }
        }
        self.bt.require_bandwidth(
            total,
            upload,
            bt,
            transfer,
            self.global_download_rate.as_ref(),
        )
    }

    pub(super) fn begin_bt_option_change(
        &mut self,
        gid: Gid,
        patch: &Value,
        request: crate::rpc_budget::RpcRequestLease,
    ) -> Result<ControlReply, HttpControlError> {
        if !self.engine_idle()
            || self.admission_fenced()
            || self.bt.admission.is_some()
            || self.pending_mutation.is_some()
        {
            return Err(HttpControlError::Busy);
        }
        let task_id = self
            .engine
            .scheduler()
            .task(gid)
            .ok_or(HttpControlError::NotFound)?
            .task_id;
        let task = self
            .bt
            .tasks
            .get_mut(&task_id)
            .ok_or(HttpControlError::NotFound)?;
        if task.option_change.is_some() || task.shutdown {
            return Err(HttpControlError::Busy);
        }
        let patch = patch
            .as_object()
            .ok_or_else(|| invalid("BitTorrent option patch must be an object"))?;
        let mut rejected = Vec::new();
        for name in patch.keys() {
            if !matches!(
                name.as_str(),
                "max-download-limit"
                    | "max-upload-limit"
                    | "bt-max-peers"
                    | "enable-dht"
                    | "enable-peer-exchange"
                    | "seed-ratio"
                    | "seed-time"
            ) {
                rejected.push(OptionPatchRejection {
                    name: name.clone(),
                    reason: if builtin_registry().find(name).is_some() {
                        OptionPatchRejectReason::RequiresExplicitBtRestart
                    } else {
                        OptionPatchRejectReason::Unsupported
                    },
                });
            }
        }
        if !rejected.is_empty() {
            return Err(HttpControlError::OptionPatchRejected(rejected));
        }
        let previous = task.spec.clone();
        let mut options = crate::rpc_result::to_value(
            &crate::rpc_result::OptionMap(&previous.options.persisted),
            crate::rpc_result::RESULT_VALUE_BYTES,
        )?;
        options
            .as_object_mut()
            .expect("options object")
            .extend(patch.clone());
        let resources = self
            .bt
            .resources
            .clone()
            .ok_or(HttpControlError::InvalidConfig)?;
        let peers = self.bt.config.peers;
        let dht = self.bt.config.dht;
        let pex = self.bt.config.pex;
        let held = request.clone();
        let preparation = spawn(&self.cpu_pool, move || {
            let _held = held;
            let options = Options::parse(&options)?;
            if options.settings.peers > peers
                || options.settings.dht && !dht
                || options.settings.pex && !pex
            {
                return Err(bt_error(BtError::RequiresRestart));
            }
            let memory = charge(&previous.record, &options, &resources.resident)?;
            Ok(Arc::new(Spec {
                resume_data: previous.resume_data.clone(),
                task_id: previous.task_id,
                record: previous.record.clone(),
                root: previous.root.clone(),
                options,
                metadata: previous.metadata.clone(),
                _memory: memory,
            }))
        })?;
        let (reply, receiver) = oneshot::channel();
        task.option_change = Some(OptionChange {
            preparation,
            prepared: None,
            reply: Some(reply),
            started: false,
            applied: false,
            native: None,
            store: None,
            _request: request,
        });
        Ok(ControlReply::Deferred(receiver))
    }

    pub(super) fn drain_bt(&mut self, deadline: Instant) -> bool {
        self.bt.begin_shutdown();
        while !self.bt.poll_stopped() {
            if Instant::now() >= deadline || self.poll_once().is_err() {
                return false;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        self.bt.clean()
    }

    pub(super) async fn drain_bt_async(&mut self, deadline: Instant) -> bool {
        self.bt.begin_shutdown();
        while !self.bt.poll_stopped() {
            if Instant::now() >= deadline || self.poll_once().is_err() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        self.bt.clean()
    }
    /// Startup configuration only. Destination policy cannot be relaxed by RPC.
    pub fn configure_bittorrent(
        &mut self,
        config: BtAdapterConfig,
    ) -> Result<(), HttpControlError> {
        if self.managed_runtime.is_some()
            || self.bt.adapter.is_some()
            || self.bt.initializing.is_some()
            || self.bt.admission.is_some()
        {
            return Err(HttpControlError::Busy);
        }
        config.metadata.validate().map_err(bt_error)?;
        self.bt.config = config;
        self.ensure_bt_resources()
    }

    pub fn bittorrent_handle(&self) -> Option<BtHandle> {
        self.bt.handle()
    }

    pub(super) fn ensure_bt_resources(&mut self) -> Result<(), HttpControlError> {
        if self.bt.resources.is_none() {
            if let Some(resources) = &self.process_resources {
                self.bt.resources = Some(resources.bt_resources());
            } else {
                static FALLBACK: std::sync::OnceLock<BtResources> = std::sync::OnceLock::new();
                let resources = FALLBACK.get_or_init(|| {
                    let resources = crate::HttpProcessResources::for_profile(
                        ariax_runtime::RuntimeProfile::Auto,
                    )
                    .expect("default process resource limits");
                    let mut resources = resources.bt_resources();
                    resources.resident = crate::RpcBudgets::process_default().resident_budget();
                    resources
                });
                self.bt.resources = Some(resources.clone());
            }
        }
        Ok(())
    }

    pub(super) fn begin_bt_admission(
        &mut self,
        params: Value,
        torrent: bool,
        request: crate::rpc_budget::RpcRequestLease,
    ) -> Result<ControlReply, HttpControlError> {
        if self.pending_admission.is_some()
            || self.bt.admission.is_some()
            || self.bt.initializing.is_some()
            || self.pending_configuration.is_some()
            || !self.engine_idle()
            || self.pending_mutation.is_some()
            || self.engine.snapshot_reader().load().len() >= self.config.task_capacity.get()
        {
            return Err(HttpControlError::Busy);
        }
        self.ensure_bt_resources()?;
        let work = self.reserve_scheduler_work(Some(&request), 1)?;
        let config = self.bt.config.clone();
        let configuration = self.configuration_snapshot();
        let resources = self.bt.resources.clone().expect("BT resources");
        let initialize = self.bt.adapter.is_none();
        let root = self.config.output_root.clone();
        let task_id = TaskId::new(self.next_task_id).ok_or(HttpControlError::InvalidConfig)?;
        let session_id = self.session_id;
        let http = self.tasks.snapshot();
        let bt = self.bt.catalog.clone();
        let policy = self.engine.persisted_option_policy();
        let retained = request.clone();
        let work_copy = work.clone();
        let preparation = spawn(&self.cpu_pool, move || {
            let _work = work_copy;
            let mut args = params
                .as_array()
                .cloned()
                .ok_or_else(|| invalid("invalid BitTorrent admission arguments"))?;
            let options_index = if torrent { 2 } else { 1 };
            let explicit = args
                .get(options_index)
                .cloned()
                .unwrap_or_else(|| json!({}));
            let options = configuration.merged_bt_options(explicit, false, &config)?;
            while args.len() <= options_index {
                args.push(if torrent && args.len() == 1 {
                    json!([])
                } else {
                    json!({})
                });
            }
            args[options_index] = options;
            let mut prepared = prepare_admission(
                Value::Array(args),
                torrent,
                task_id,
                session_id,
                &root,
                &resources.resident,
                &retained,
                false,
            )?;
            if prepared.spec.options.settings.peers > config.peers
                || prepared.spec.options.settings.dht && !config.dht
                || prepared.spec.options.settings.pex && !config.pex
            {
                return Err(invalid(
                    "task peer limit exceeds the BitTorrent process share",
                ));
            }
            if !prepared
                .spec
                .options
                .persisted
                .entries()
                .all(|(name, _)| policy.permits(name))
            {
                return Err(invalid("BitTorrent options cannot be persisted"));
            }
            collision(&prepared.spec, &http, &bt)?;
            if initialize {
                prepared.adapter = Some(BtAdapter::start(config, resources).map_err(bt_error)?);
            }
            Ok(prepared)
        })?;
        let (reply, receiver) = oneshot::channel();
        self.bt.admission = Some(PendingAdmission {
            validation: None,
            preparation,
            ready: None,
            reply: Some(reply),
            work,
            _request: request,
        });
        Ok(ControlReply::Deferred(receiver))
    }

    pub(super) fn poll_bt_admission(&mut self) -> Result<bool, HttpControlError> {
        let Some(mut pending) = self.bt.admission.take() else {
            return Ok(false);
        };
        let result = (|| {
            if pending.ready.is_none() {
                pending.ready = receive(&pending.preparation)?;
            }
            if pending.ready.is_none()
                || !self.engine_idle()
                || self.pending_mutation.is_some()
                || self.pending_admission.is_some()
            {
                return Ok(false);
            }
            if !validate_current_mapping(
                &mut pending.validation,
                &pending.ready.as_ref().expect("prepared BT admission").spec,
                &self.cpu_pool,
                &self.tasks,
                &self.bt.catalog,
                &self.bt.mapping_revision,
            )? {
                return Ok(false);
            }
            let mut ready = pending.ready.take().expect("prepared BT admission");
            let queue = if ready.spec.options.paused {
                QueueClass::Paused
            } else {
                QueueClass::Waiting
            };
            let position = ready
                .position
                .unwrap_or(usize::MAX)
                .min(self.engine.scheduler().queue_snapshot(queue).len());
            // The record has not been published and is uniquely owned here.
            let spec = Arc::get_mut(&mut ready.spec).expect("unpublished spec");
            Arc::get_mut(&mut spec.record)
                .expect("unpublished record")
                .queue_position = position as u32;
            let gid = ready.spec.record.gid;
            let task_id = ready.spec.task_id;
            let command = SchedulerCommand::AddValidatedTaskAt {
                task_id,
                gid,
                desired_paused: ready.spec.options.paused,
                conditions: TaskConditions::default(),
                position,
            };
            let mut simulation = self.engine.scheduler().clone();
            let outcome = simulation
                .execute_command_at(command.clone(), MonotonicInstant::now())
                .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
            let effect = outcome
                .effects
                .into_iter()
                .find(|effect| matches!(effect, TransitionEffect::PersistTask { .. }))
                .ok_or(HttpControlError::InvalidConfig)?;
            let plan = PersistenceEffectPlan::new(
                effect,
                vec![PersistencePlanStep::CreateBtTask {
                    task: ready.spec.record.clone(),
                    options: ready.spec.options.persisted.clone(),
                }],
            )
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
            self.engine.runtime_handle().register_bt_task(task_id);
            self.prepare_and_begin(plan, command)?;
            if let Some(adapter) = ready.adapter {
                self.bt.adapter = Some(adapter);
                self.bt.rate_applied = false;
            }
            self.bt.install(ready.spec);
            self.next_task_id = self
                .next_task_id
                .checked_add(1)
                .ok_or(HttpControlError::InvalidConfig)?;
            self.pending_mutation = Some(PendingMutation {
                publication: MutationPublication::Admission {
                    gid,
                    readmission_started: false,
                },
                reply: pending.reply.take().expect("BT admission reply"),
            });
            self.retain_pending_work(Some(pending.work.clone()))?;
            self.turn.mark_progress();
            Ok(true)
        })();
        match result {
            Ok(true) => Ok(true),
            Ok(false) => {
                self.bt.admission = Some(pending);
                Ok(false)
            }
            Err(error) => {
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(error));
                }
                self.turn.mark_progress();
                Ok(false)
            }
        }
    }

    pub(super) fn restore_bt_catalog(&mut self) -> Result<(), HttpControlError> {
        let records = match self
            .session
            .execute(SessionCommand::ReadBtTasks)
            .map_err(|error| HttpControlError::Persistence(error.to_string()))?
        {
            SessionCommandResult::BtTasks(records) => records,
            _ => return Err(bt_error(BtError::StaleCompletion)),
        };
        if records.is_empty() {
            return Ok(());
        }
        self.ensure_bt_resources()?;
        for record in records {
            let task_id = self
                .engine
                .scheduler()
                .task(record.gid)
                .ok_or(HttpControlError::NotFound)?
                .task_id;
            let options = match self
                .session
                .execute(SessionCommand::ReadTaskOptions {
                    gid: record.gid,
                    scope: OptionsSnapshotScope::CurrentGeneration,
                })
                .map_err(|error| HttpControlError::Persistence(error.to_string()))?
            {
                SessionCommandResult::TaskOptions(options) => options,
                _ => return Err(bt_error(BtError::StaleCompletion)),
            };
            let mut options = Options::parse(&crate::rpc_result::to_value(
                &crate::rpc_result::OptionMap(&options),
                crate::rpc_result::RESULT_VALUE_BYTES,
            )?)?;
            options.paused = record.desired_paused;
            let root = ariax_storage::platform_path_to_current(&record.root_display)
                .map_err(|_| bt_error(BtError::UnsafePath))?;
            let root = ProtectedRoot::open(root).map_err(bt_error)?;
            if root.identity().as_ref() != record.binding.root_identity {
                return Err(bt_error(BtError::IdentityMismatch));
            }
            let metadata = (!record.binding.metainfo.is_empty())
                .then(|| parse_torrent(&record.binding.metainfo, MetadataLimits::default()))
                .transpose()
                .map_err(bt_error)?;
            let memory = charge(
                &record,
                &options,
                &self.bt.resources.as_ref().expect("BT resources").resident,
            )?;
            let resume = match self
                .session
                .execute(SessionCommand::ReadBtResume {
                    gid: record.gid,
                    limit: options.resume_limit,
                })
                .map_err(|error| HttpControlError::Persistence(error.to_string()))?
            {
                SessionCommandResult::BtResume(resume) => resume,
                _ => return Err(bt_error(BtError::StaleCompletion)),
            };
            let dirty = resume.dirty;
            let resume_data = retained_resume(
                resume.resume_blob,
                &self.bt.resources.as_ref().expect("BT resources").resident,
            )?;
            let spec = Arc::new(Spec {
                resume_data,
                task_id,
                record: Arc::new(record),
                root,
                options,
                metadata,
                _memory: memory,
            });
            collision(&spec, &self.tasks.snapshot(), &self.bt.catalog)?;
            spec.root
                .validate_mapping(&spec.files(), true)
                .map_err(bt_error)?;
            self.engine.runtime_handle().register_bt_task(task_id);
            self.bt.install(spec);
            self.bt
                .tasks
                .get_mut(&task_id)
                .expect("restored BT task")
                .dirty = dirty;
            self.bt.publish(task_id);
            self.next_task_id = self.next_task_id.max(
                task_id
                    .get()
                    .checked_add(1)
                    .ok_or(HttpControlError::InvalidConfig)?,
            );
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_admission(
    params: Value,
    torrent: bool,
    task_id: TaskId,
    session_id: SessionId,
    allowed_root: &Path,
    resident: &ByteBudget,
    request: &crate::rpc_budget::RpcRequestLease,
    allow_existing: bool,
) -> Result<PreparedAdmission, HttpControlError> {
    let args = params
        .as_array()
        .filter(|args| {
            if torrent {
                (1..=4).contains(&args.len())
            } else {
                (1..=3).contains(&args.len())
            }
        })
        .ok_or_else(|| invalid("invalid BitTorrent admission arguments"))?;
    let options_index = if torrent { 2 } else { 1 };
    let options = Options::parse(args.get(options_index).unwrap_or(&json!({})))?;
    let position = args
        .get(options_index + 1)
        .map(|value| parse_i64(value, "position"))
        .transpose()?;
    if position.is_some_and(|position| position < -1) {
        return Err(invalid(
            "BitTorrent queue position must be -1 or nonnegative",
        ));
    }
    let position = position
        .filter(|position| *position >= 0)
        .map(|position| {
            usize::try_from(position).map_err(|_| invalid("BitTorrent queue position is too large"))
        })
        .transpose()?;
    let limits = MetadataLimits::default();
    let option = |name| {
        options
            .persisted
            .entries()
            .find_map(|(key, value)| (key == name).then_some(value))
    };
    let trackers = option("bt-tracker")
        .map(|value| value.split(',').map(str::to_owned).collect::<Vec<_>>())
        .unwrap_or_default();
    let excluded = option("bt-exclude-tracker")
        .map(|value| value.split(',').map(str::to_owned).collect::<Vec<_>>())
        .unwrap_or_default();
    let (metainfo, magnet, metadata, identity) = if torrent {
        let encoded = args[0]
            .as_str()
            .ok_or_else(|| invalid("torrent must be base64 text"))?;
        if encoded.len() > limits.bytes.saturating_mul(4).div_ceil(3) {
            return Err(invalid("torrent exceeds the metadata byte limit"));
        }
        request
            .reserve(encoded.len().saturating_mul(8).saturating_add(64 * 1024))
            .map_err(|_| HttpControlError::Busy)?;
        let mut bytes =
            base64ct::Base64::decode_vec(encoded).map_err(|_| invalid("invalid torrent base64"))?;
        if let Some(seeds) = args.get(1) {
            let seeds = seeds
                .as_array()
                .filter(|seeds| seeds.len() <= 64)
                .ok_or_else(|| invalid("web seeds must be a bounded URI array"))?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("web seed must be text"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !seeds.is_empty() {
                bytes = with_web_seeds(&bytes, &seeds, limits).map_err(bt_error)?;
            }
        }
        if !trackers.is_empty() || !excluded.is_empty() {
            bytes =
                ariax_bt::with_trackers(&bytes, &trackers, &excluded, limits).map_err(bt_error)?;
        }
        let metadata = parse_torrent(&bytes, limits).map_err(bt_error)?;
        let identity = metadata.identity.clone();
        (bytes, None, Some(metadata), identity)
    } else {
        let uris = args[0]
            .as_array()
            .filter(|uris| uris.len() == 1)
            .ok_or_else(|| invalid("magnet admission requires exactly one URI"))?;
        let uri = uris[0]
            .as_str()
            .ok_or_else(|| invalid("magnet must be text"))?;
        let uri = ariax_bt::magnet_with_trackers(uri, &trackers, &excluded).map_err(bt_error)?;
        let identity = parse_magnet(&uri).map_err(bt_error)?.identity;
        (Vec::new(), Some(uri), None, identity)
    };
    let approved_root =
        std::fs::canonicalize(allowed_root).map_err(|_| bt_error(BtError::UnsafePath))?;
    let root = options
        .directory
        .as_ref()
        .map_or_else(|| approved_root.clone(), |dir| approved_root.join(dir));
    let root = std::fs::canonicalize(root).map_err(|_| bt_error(BtError::UnsafePath))?;
    if !root.starts_with(&approved_root) {
        return Err(bt_error(BtError::UnsafePath));
    }
    let root = ProtectedRoot::open(root).map_err(bt_error)?;
    let mapping = metadata
        .as_ref()
        .map(|metadata| map_files(metadata, &options.mapping))
        .transpose()
        .map_err(bt_error)?
        .unwrap_or_default();
    root.validate_mapping(&mapping, allow_existing)
        .map_err(bt_error)?;
    let info = if metainfo.is_empty() {
        Vec::new()
    } else {
        info_section(&metainfo, limits).map_err(bt_error)?.to_vec()
    };
    let mut hash = Sha256::new();
    hash.update(b"ariax/bt-gid/v3\0");
    hash.update(session_id.as_bytes());
    hash.update(task_id.get().to_le_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let gid = Gid::new(u64::from_be_bytes(digest[..8].try_into().expect("GID digest")).max(1))
        .expect("nonzero GID");
    let now = now_unix_ms();
    let record = SessionBtTaskRecord {
        gid,
        session_id,
        queue_state: if options.paused {
            SessionQueueState::Paused
        } else {
            SessionQueueState::Waiting
        },
        queue_position: 0,
        desired_paused: options.paused,
        root_display: PlatformPath::from_current(root.path())
            .map_err(|_| bt_error(BtError::UnsafePath))?,
        generation: Generation::INITIAL.get(),
        binding: SessionBtBinding {
            identity,
            root_identity: root.identity().into_vec(),
            metainfo,
            info,
            magnet,
            files: files(&mapping),
        },
        downloaded: 0,
        uploaded: 0,
        seed_millis: 0,
        created_ms: now,
        updated_ms: now,
    };
    record
        .binding
        .validate()
        .map_err(|_| invalid("invalid BitTorrent task binding"))?;
    let memory = charge(&record, &options, resident)?;
    Ok(PreparedAdmission {
        spec: Arc::new(Spec {
            resume_data: None,
            task_id,
            record: Arc::new(record),
            root,
            options,
            metadata,
            _memory: memory,
        }),
        adapter: None,
        position,
    })
}

/// Import validates the same admission contract and binds the receiver's root.
#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_import(
    imported: crate::session_file::ImportedBt,
    options: Value,
    task_id: TaskId,
    session_id: SessionId,
    root: &Path,
    resident: &ByteBudget,
    request: &crate::rpc_budget::RpcRequestLease,
) -> Result<Arc<Spec>, HttpControlError> {
    let torrent = !imported.binding.metainfo.is_empty();
    let params = if torrent {
        json!([
            base64ct::Base64::encode_string(&imported.binding.metainfo),
            [],
            options
        ])
    } else {
        json!([
            [imported
                .binding
                .magnet
                .as_deref()
                .ok_or_else(|| invalid("missing imported magnet"))?],
            options
        ])
    };
    let mut prepared = prepare_admission(
        params, torrent, task_id, session_id, root, resident, request, true,
    )?;
    let spec = Arc::get_mut(&mut prepared.spec).expect("unpublished imported BT task");
    if spec.record.binding.identity != imported.binding.identity
        || spec.record.binding.files != imported.binding.files
    {
        return Err(bt_error(BtError::IdentityMismatch));
    }
    Arc::get_mut(&mut spec.record)
        .expect("unpublished imported record")
        .binding
        .magnet = imported.binding.magnet;
    spec.resume_data = retained_resume(imported.resume_data.into(), resident)?;
    Ok(prepared.spec)
}

pub(super) fn query_for_import(spec: Arc<Spec>) -> Arc<QueryTask> {
    Arc::new(QueryTask {
        resume_data: spec.resume_data.clone(),
        spec,
        snapshot: None,
        peers: Arc::new(Vec::new()),
        dirty: true,
        checkpoint_failed: false,
        downloaded: 0,
        uploaded: 0,
        seed_millis: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_validation_ignores_progress_and_rechecks_changed_catalogs() {
        let directory = super::super::tests::TestDirectory::new();
        let mut plane = directory.control_plane();
        plane.ensure_bt_resources().unwrap();
        let request = plane.direct_client.try_request(0).unwrap();
        let spec = prepare_admission(
            json!([base64ct::Base64::encode_string(include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent")), [], {"pause":true}]),
            true, TaskId::new(1).unwrap(), plane.session_id, &directory.output,
            &plane.bt.resources.as_ref().unwrap().resident, &request, false,
        ).unwrap().spec;
        plane.bt.install(spec.clone());
        let mut validation = None;
        let revision = plane.bt.mapping_revision.clone();
        plane.bt.publish(spec.task_id);
        assert!(Arc::ptr_eq(&revision, &plane.bt.mapping_revision));
        let deadline = Instant::now() + Duration::from_secs(10);
        while !validate_current_mapping(
            &mut validation,
            &spec,
            &plane.cpu_pool,
            &plane.tasks,
            &plane.bt.catalog,
            &plane.bt.mapping_revision,
        )
        .unwrap()
        {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !validate_current_mapping(
                &mut validation,
                &spec,
                &plane.cpu_pool,
                &plane.tasks,
                &plane.bt.catalog,
                &plane.bt.mapping_revision
            )
            .unwrap()
        );
        let transfer = HttpTaskSpec::new(
            TaskId::new(2).unwrap(),
            Gid::new(2).unwrap(),
            vec!["https://example.test/file".into()],
            directory.output.clone(),
            SafePathBuilder::from_user_path(
                &spec.record.binding.files[0].path,
                PathPlatform::current(),
            )
            .unwrap(),
            HttpTaskOptions::default(),
            false,
        )
        .unwrap();
        plane.tasks.insert(transfer).unwrap();
        loop {
            match validate_current_mapping(
                &mut validation,
                &spec,
                &plane.cpu_pool,
                &plane.tasks,
                &plane.bt.catalog,
                &plane.bt.mapping_revision,
            ) {
                Err(HttpControlError::InvalidParams(_)) => break,
                Ok(false) => {}
                other => panic!("stale mapping check accepted a conflicting output: {other:?}"),
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(std::fs::read_dir(&directory.output).unwrap().count(), 0);
        plane.tasks.remove(TaskId::new(2).unwrap());
        plane.bt.remove(spec.task_id);
        drop((validation, spec, request));
        plane.shutdown().unwrap();
    }
}
