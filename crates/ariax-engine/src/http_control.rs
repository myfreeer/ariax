//! The bounded Phase-3 HTTP control plane.
//!
//! The control plane owns the only mutable bridge between RPC requests,
//! scheduler effects, persisted task metadata, and the worker catalog.  RPC
//! transports never mutate the scheduler directly.

use crate::http_first_slice::append_initial_admission_with_options;
use crate::{
    HttpContentChecksum, HttpRetryAfterPolicy, HttpRetryBackoff, HttpRetryPolicy, HttpRetryProfile,
    HttpRetryStatusSet, HttpRetryTriggerSet, HttpRpcBackend, HttpRpcBackendError,
    HttpTaskCatalogError, HttpTaskOptions, HttpTaskSpec, HttpTaskSpecError, HttpTaskWorker,
    HttpTransferStatsSnapshot, HttpWorkerSupervisor, HttpWorkerSupervisorConfig,
    HttpWorkerSupervisorShutdown, MAX_HTTP_ENDGAME_MAX_DUPLICATES, PersistenceEffectPlan,
    PersistencePlanStep, ProcessDrainOutcome, RpcEvent, RpcEventBroker, RpcEventClass,
    RpcEventError, RpcEventKey, RpcEventLimits, RpcEventSubscriber, SharedHttpTaskCatalog,
    SharedHttpTransferStats, derive_http_journal_id, http_journal_directory,
};
use ariax_config::{
    CompatStatus, FlatConfigLimits, OptionValue, RuntimeUpdate, Scope, SecurityClass,
    UnknownOptionMode, builtin_registry, parse_flat_config, parse_option_value,
};
use ariax_core::{
    Aria2Status, Generation, Gid, MonotonicInstant, OptionPatchId, PendingBarrier, PublicError,
    QueueClass, QueueOrder, RequestScheduler, RetryClass, SchedulerCommand, TaskConditions,
    TaskEvent, TaskEventEnvelope, TaskId, TaskSnapshot, TransitionEffect, ValidatedOptionPatchKind,
};
use ariax_runtime::{RateArbiter, RateLimit, RateScope};
use ariax_storage::{
    ControlJournalAppender, GenerationStartReason, JournalPayload, OptionsSnapshotScope,
    PathPlatform, PlatformPath, SafePathBuilder, SanitizedOptionMap, SessionCommand,
    SessionCommandResult, SessionHandle, SessionId, SessionQueueOrder, SessionQueueState,
    SessionSlowSlotState, SessionStoppedResultRecord, SessionTaskRecord, SessionTerminalStatus,
    TaskPauseReason, TaskRemoveReason,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, oneshot, watch};

const CONTROL_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_PROGRESS_POLL: Duration = Duration::from_micros(50);
const MAX_HTTP_RETRY_ATTEMPTS: u32 = 1024;
const MAX_HTTP_RETRY_WAIT_SECS: u64 = 600;
const MAX_HTTP_RETRY_ELAPSED_SECS: u64 = 7200;
const MAX_RPC_LIST_ITEMS: usize = 1000;

/// Bounded process configuration needed by public HTTP task admission.
#[derive(Clone, Debug)]
pub struct HttpControlPlaneConfig {
    pub output_root: PathBuf,
    pub journal_root: PathBuf,
    pub task_capacity: NonZeroUsize,
    pub supervisor: HttpWorkerSupervisorConfig,
}

impl HttpControlPlaneConfig {
    pub fn validate(&self) -> Result<(), HttpControlError> {
        if !self.output_root.is_absolute()
            || self.output_root.as_os_str().is_empty()
            || !self.journal_root.is_absolute()
            || self.journal_root.as_os_str().is_empty()
        {
            return Err(HttpControlError::InvalidConfig);
        }
        self.supervisor
            .validate()
            .map_err(|_| HttpControlError::InvalidConfig)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum HttpControlError {
    InvalidConfig,
    InvalidParams(&'static str),
    TaskSpec(HttpTaskSpecError),
    Catalog(HttpTaskCatalogError),
    Persistence(String),
    Scheduler(String),
    Journal(String),
    NotFound,
    Unsupported(&'static str),
    Busy,
    SlowConsumer,
}

impl fmt::Display for HttpControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid HTTP control configuration"),
            Self::InvalidParams(message) => formatter.write_str(message),
            Self::TaskSpec(error) => error.fmt(formatter),
            Self::Catalog(error) => write!(formatter, "HTTP task catalog error: {error:?}"),
            Self::Persistence(error) => write!(formatter, "persistence failed: {error}"),
            Self::Scheduler(error) => write!(formatter, "scheduler rejected command: {error}"),
            Self::Journal(error) => write!(formatter, "journal failed: {error}"),
            Self::NotFound => formatter.write_str("task was not found"),
            Self::Unsupported(message) => formatter.write_str(message),
            Self::Busy => formatter.write_str("control plane is busy"),
            Self::SlowConsumer => formatter.write_str("RPC event subscriber is too slow"),
        }
    }
}

impl Error for HttpControlError {}

struct PendingOptionSnapshot {
    options: SanitizedOptionMap,
    previous_generation: Generation,
}

struct PendingSourceReplacement {
    replacement: HttpTaskSpec,
    response: Value,
    reply: oneshot::Sender<Result<Value, HttpControlError>>,
    committing: bool,
}

/// Shared mutable control plane used by both transports.
pub struct HttpControlPlane {
    engine: crate::BootstrappedEngine,
    config: HttpControlPlaneConfig,
    tasks: SharedHttpTaskCatalog,
    stats: SharedHttpTransferStats,
    supervisor: Option<HttpWorkerSupervisor>,
    session: SessionHandle,
    session_id: SessionId,
    journal_sequences: BTreeMap<Gid, u64>,
    next_task_id: u64,
    global_options: BTreeMap<String, String>,
    pending_option_snapshots: BTreeMap<OptionPatchId, PendingOptionSnapshot>,
    pending_restart_patches: BTreeMap<Gid, OptionPatchId>,
    pending_source_replacements: BTreeMap<Gid, PendingSourceReplacement>,
    next_option_patch_id: u64,
    shutdown_requested: bool,
    force_shutdown_requested: bool,
    events: RpcEventBroker,
    subscriptions: BTreeMap<u64, RpcEventSubscriber>,
    observed_statuses: BTreeMap<Gid, Aria2Status>,
    global_download_rate: Option<RateArbiter>,
}

impl fmt::Debug for HttpControlPlane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpControlPlane")
            .field("task_count", &self.tasks.len())
            .field("supervisor_attached", &self.supervisor.is_some())
            .finish_non_exhaustive()
    }
}

impl HttpControlPlane {
    pub fn new(
        engine: crate::BootstrappedEngine,
        config: HttpControlPlaneConfig,
    ) -> Result<Self, HttpControlError> {
        config.validate()?;
        let tasks = SharedHttpTaskCatalog::new(config.task_capacity);
        let stats = SharedHttpTransferStats::new(config.task_capacity);
        let next_task_id = engine
            .snapshot_reader()
            .load()
            .tasks()
            .values()
            .map(|task| task.task_id.get())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        let mut plane = Self {
            session: engine.session_handle(),
            session_id: engine.session_id(),
            engine,
            config,
            tasks,
            stats,
            supervisor: None,
            journal_sequences: BTreeMap::new(),
            next_task_id,
            global_options: default_global_options()?,
            pending_option_snapshots: BTreeMap::new(),
            pending_restart_patches: BTreeMap::new(),
            pending_source_replacements: BTreeMap::new(),
            next_option_patch_id: now_unix_ms().max(1),
            shutdown_requested: false,
            force_shutdown_requested: false,
            events: RpcEventBroker::new(),
            subscriptions: BTreeMap::new(),
            observed_statuses: BTreeMap::new(),
            global_download_rate: None,
        };
        plane.restore_catalog()?;
        plane.reset_observed_statuses();
        Ok(plane)
    }

    /// Attaches the real HTTP worker supervisor.  The catalog is shared with
    /// the supervisor, but remains private until persistence succeeds.
    pub fn attach_worker(
        &mut self,
        worker: Arc<dyn HttpTaskWorker>,
    ) -> Result<(), HttpControlError> {
        if self.supervisor.is_some() {
            return Err(HttpControlError::InvalidConfig);
        }
        let supervisor = HttpWorkerSupervisor::new(
            self.engine.runtime_handle(),
            self.tasks.clone(),
            worker,
            self.config.supervisor,
        )
        .map_err(|_| HttpControlError::InvalidConfig)?;
        self.supervisor = Some(supervisor);
        Ok(())
    }

    /// Attaches the process-owned download arbiter so global live option and
    /// reload changes affect already-running HTTP workers.
    pub fn attach_global_download_rate(
        &mut self,
        rate: RateArbiter,
    ) -> Result<(), HttpControlError> {
        if let Some(limit) = self.global_options.get("max-overall-download-limit") {
            let bytes = limit
                .parse::<u64>()
                .map_err(|_| HttpControlError::InvalidConfig)?;
            rate.set_global_limit(RateLimit::per_second(bytes))
                .map_err(|_| HttpControlError::InvalidConfig)?;
        }
        self.global_download_rate = Some(rate);
        Ok(())
    }

    pub fn shutdown(self) -> Result<crate::ProcessShutdownReport, crate::ProcessShutdownError> {
        let Self {
            engine,
            mut supervisor,
            ..
        } = self;
        let mut shutdown = engine.begin_shutdown()?;
        let drain = if let Some(supervisor) = supervisor.as_mut() {
            let active_workers = supervisor.active_workers();
            supervisor.cancel_all();
            if active_workers == 0 {
                ProcessDrainOutcome::Drained
            } else {
                ProcessDrainOutcome::Failed
            }
        } else {
            ProcessDrainOutcome::Drained
        };
        shutdown.complete_drain(drain)?;
        shutdown.finish()
    }

    /// Drains live HTTP workers before closing journals and the session owner.
    pub async fn shutdown_async(
        self,
    ) -> Result<crate::ProcessShutdownReport, crate::ProcessShutdownError> {
        let Self {
            engine, supervisor, ..
        } = self;
        let mut shutdown = engine.begin_shutdown()?;
        let drain_timeout = shutdown.drain_timeout();
        let drain = match supervisor {
            Some(supervisor) => match supervisor.shutdown_with_timeout(drain_timeout).await {
                HttpWorkerSupervisorShutdown::Drained => ProcessDrainOutcome::Drained,
                HttpWorkerSupervisorShutdown::TimedOut { .. } => ProcessDrainOutcome::TimedOut,
            },
            None => ProcessDrainOutcome::Drained,
        };
        shutdown.complete_drain(drain)?;
        shutdown.finish()
    }

    fn restore_catalog(&mut self) -> Result<(), HttpControlError> {
        let recovered = self.engine.recovered_tasks().to_vec();
        let records = match self.session.execute(SessionCommand::ReadTasks) {
            Ok(SessionCommandResult::Tasks(records)) => records
                .into_iter()
                .map(|record| (record.gid, record))
                .collect::<BTreeMap<_, _>>(),
            _ => BTreeMap::new(),
        };
        for recovered in recovered {
            for snapshot in [
                recovered.journal.current_options(),
                recovered.journal.pending_options(),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(patch_id) = snapshot.patch_id() {
                    self.next_option_patch_id = self.next_option_patch_id.max(
                        patch_id
                            .get()
                            .checked_add(1)
                            .ok_or(HttpControlError::InvalidConfig)?,
                    );
                }
            }
            let Some(record) = records.get(&recovered.gid) else {
                continue;
            };
            let Ok(output_root) = ariax_storage::platform_path_to_current(&record.root_display)
            else {
                continue;
            };
            let sources = match self
                .session
                .execute(SessionCommand::ReadTaskSources { gid: recovered.gid })
            {
                Ok(SessionCommandResult::TaskSources(sources)) => sources,
                _ => continue,
            };
            let uris = sources
                .into_iter()
                .filter_map(|source| source.persistence_safe_uri)
                .collect::<Vec<_>>();
            if uris.is_empty() {
                continue;
            }
            let mut persisted_options =
                match self.session.execute(SessionCommand::ReadTaskOptions {
                    gid: recovered.gid,
                    scope: OptionsSnapshotScope::CurrentGeneration,
                }) {
                    Ok(SessionCommandResult::TaskOptions(options)) => options,
                    _ => continue,
                };
            if let Some(staged) = recovered.journal.pending_options()
                && let Some(patch_id) = staged.patch_id()
            {
                self.replace_option_mirror(
                    recovered.gid,
                    OptionsSnapshotScope::NextAdmission,
                    staged.options().clone(),
                )?;
                persisted_options = staged.options().clone();
                self.pending_option_snapshots.insert(
                    patch_id,
                    PendingOptionSnapshot {
                        options: persisted_options.clone(),
                        previous_generation: recovered.journal.generation(),
                    },
                );
                self.pending_restart_patches.insert(recovered.gid, patch_id);
            } else if let Some(current) = recovered.journal.current_options()
                && current.patch_id().is_some()
            {
                let staged = match self.session.execute(SessionCommand::ReadTaskOptions {
                    gid: recovered.gid,
                    scope: OptionsSnapshotScope::NextAdmission,
                }) {
                    Ok(SessionCommandResult::TaskOptions(options)) => options,
                    _ => {
                        return Err(HttpControlError::Persistence(
                            "cannot read staged option mirror".to_owned(),
                        ));
                    }
                };
                if staged.entries().len() != 0 {
                    if &staged != current.options() {
                        return Err(HttpControlError::Persistence(
                            "promoted option mirror does not match the journal".to_owned(),
                        ));
                    }
                    match self.session.execute(SessionCommand::PromoteTaskOptions {
                        gid: recovered.gid,
                        options: staged.clone(),
                    }) {
                        Ok(SessionCommandResult::Unit) => persisted_options = staged,
                        _ => {
                            return Err(HttpControlError::Persistence(
                                "cannot promote recovered option mirror".to_owned(),
                            ));
                        }
                    }
                }
            }
            let options = match HttpTaskOptions::from_sanitized(&persisted_options) {
                Ok(options) => options,
                Err(_) if !self.pending_restart_patches.contains_key(&recovered.gid) => continue,
                Err(_) => {
                    return Err(HttpControlError::Persistence(
                        "recovered option patch is invalid".to_owned(),
                    ));
                }
            };
            let output = HttpTaskSpec::persisted_output(&persisted_options).or_else(|_| {
                recovered
                    .journal
                    .layout()
                    .and_then(|layout| layout.layout().files().first())
                    .map(|file| file.safe_path().clone())
                    .ok_or(HttpTaskSpecError::InvalidOptions)
            });
            let Ok(output) = output else {
                continue;
            };
            let spec = match HttpTaskSpec::new(
                recovered.task_id,
                recovered.gid,
                uris,
                output_root,
                output,
                options,
                false,
            ) {
                Ok(spec) => spec,
                Err(_) => continue,
            };
            if self.tasks.insert(spec).is_ok() {
                self.journal_sequences
                    .insert(recovered.gid, recovered.journal.last_sequence());
                let _ = self.stats.get_or_create(recovered.task_id);
            }
        }
        Ok(())
    }

    fn replace_option_mirror(
        &self,
        gid: Gid,
        scope: OptionsSnapshotScope,
        options: SanitizedOptionMap,
    ) -> Result<(), HttpControlError> {
        match self.session.execute(SessionCommand::ReplaceTaskOptions {
            gid,
            scope,
            options,
        }) {
            Ok(SessionCommandResult::Unit) => Ok(()),
            Ok(_) => Err(HttpControlError::Persistence(
                "unexpected option replacement result".to_owned(),
            )),
            Err(error) => Err(HttpControlError::Persistence(error.to_string())),
        }
    }

    fn retire_promoted_option_patches(&mut self) {
        self.pending_restart_patches.retain(|gid, patch_id| {
            let promoted = self.engine.scheduler().task(*gid).is_none_or(|task| {
                self.pending_option_snapshots
                    .get(patch_id)
                    .is_some_and(|pending| {
                        task.generation > pending.previous_generation
                            && !matches!(
                                task.pending_barrier,
                                Some(PendingBarrier::GenerationPersistence { .. })
                            )
                    })
            });
            if promoted {
                self.pending_option_snapshots.remove(patch_id);
            }
            !promoted
        });
    }

    #[must_use]
    pub fn task_catalog(&self) -> SharedHttpTaskCatalog {
        self.tasks.clone()
    }

    #[must_use]
    pub fn stats_catalog(&self) -> SharedHttpTransferStats {
        self.stats.clone()
    }

    #[must_use]
    pub fn session_handle(&self) -> SessionHandle {
        self.session.clone()
    }

    #[must_use]
    pub fn event_broker(&self) -> RpcEventBroker {
        self.events.clone()
    }

    #[must_use]
    pub const fn shutdown_requested(&self) -> bool {
        self.shutdown_requested
    }

    #[must_use]
    pub const fn force_shutdown_requested(&self) -> bool {
        self.force_shutdown_requested
    }

    /// Performs one bounded engine/supervisor progress turn.
    pub fn poll_once(&mut self) -> Result<(), HttpControlError> {
        let now = MonotonicInstant::now();
        if let Some(supervisor) = self.supervisor.as_mut() {
            supervisor
                .poll_once(now)
                .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        }
        if self.engine.is_idle()
            && let Some(event) = self.engine.runtime_handle().poll_event_at(now)
        {
            self.prepare_runtime_event(&event, now)?;
            self.engine
                .handle_event_at(&event, now)
                .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
            self.drive_engine()?;
        }
        if self.engine.is_idle() {
            self.complete_source_replacements()?;
            self.try_admit_one(now)?;
        }
        self.drive_engine()?;
        self.publish_task_state_events();
        Ok(())
    }

    pub fn call(&mut self, method: &str, params: Value) -> Result<Value, HttpControlError> {
        let result = match method {
            "aria2.addUri" | "addUri" => self.add_uri(params),
            "aria2.tellStatus" | "tellStatus" => self.tell_status(params),
            "aria2.tellActive" | "tellActive" => self.tell_active(params),
            "aria2.tellWaiting" | "tellWaiting" => self.tell_waiting(params),
            "aria2.tellStopped" | "tellStopped" => self.tell_stopped(params),
            "aria2.pause" | "pause" => self.pause(params, false),
            "aria2.forcePause" | "forcePause" => self.pause(params, true),
            "aria2.pauseAll" | "pauseAll" => self.pause_all(params, false),
            "aria2.forcePauseAll" | "forcePauseAll" => self.pause_all(params, true),
            "aria2.unpause" | "unpause" => self.unpause(params),
            "aria2.unpauseAll" | "unpauseAll" => self.unpause_all(params),
            "aria2.remove" | "remove" => self.remove(params, false),
            "aria2.forceRemove" | "forceRemove" => self.remove(params, true),
            "aria2.removeDownloadResult" | "removeDownloadResult" => {
                self.remove_download_result(params)
            }
            "aria2.purgeDownloadResult" | "purgeDownloadResult" => {
                self.purge_download_result(params)
            }
            "aria2.changePosition" | "changePosition" => self.change_position(params),
            "aria2.getUris" | "getUris" => self.get_uris(params),
            "aria2.getFiles" | "getFiles" => self.get_files(params),
            "aria2.getServers" | "getServers" => self.get_servers(params),
            "aria2.getOption" | "getOption" => self.get_option(params),
            "aria2.changeOption" | "changeOption" => self.change_option(params),
            "aria2.changeUri" | "changeUri" | "ariax.replaceSources" => {
                self.source_call_sync(method, params)
            }
            "aria2.getGlobalOption" | "getGlobalOption" => self.get_global_option(params),
            "aria2.changeGlobalOption" | "changeGlobalOption" => self.change_global_option(params),
            "aria2.getVersion" | "getVersion" => self.get_version(params),
            "aria2.getSessionInfo" | "getSessionInfo" => self.get_session_info(params),
            "aria2.getGlobalStat" | "getGlobalStat" => self.global_stat(params),
            "aria2.shutdown" | "shutdown" => self.request_shutdown(params, false),
            "aria2.forceShutdown" | "forceShutdown" => self.request_shutdown(params, true),
            "ariax.subscribe" => self.subscribe_events(params),
            "ariax.unsubscribe" => self.unsubscribe_events(params),
            "ariax.pollEvents" => self.poll_events(params),
            "ariax.checkConfig" => self.check_config(params),
            "ariax.reloadConfig" => self.reload_config(params),
            "ariax.dumpConfig" => self.dump_config(params),
            "ariax.exportSession" => self.export_session(params),
            "ariax.importSession" => self.import_session(params),
            _ => Err(HttpControlError::Unsupported("method not found")),
        };
        if let Ok(value) = &result {
            self.publish_control_event(method, value);
            self.publish_task_state_events();
        }
        result
    }

    fn subscribe_events(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() <= 2).ok_or(
            HttpControlError::InvalidParams("subscribe accepts optional event and byte limits"),
        )?;
        let events = values
            .first()
            .map(|value| parse_bounded_usize(value, "event capacity"))
            .transpose()?
            .unwrap_or(crate::DEFAULT_RPC_EVENT_CAPACITY);
        let bytes = values
            .get(1)
            .map(|value| parse_bounded_usize(value, "event byte capacity"))
            .transpose()?
            .unwrap_or(crate::DEFAULT_RPC_EVENT_BYTE_CAPACITY);
        let limits = RpcEventLimits {
            events: NonZeroUsize::new(events).ok_or(HttpControlError::InvalidParams(
                "event capacity must be nonzero",
            ))?,
            bytes: NonZeroUsize::new(bytes).ok_or(HttpControlError::InvalidParams(
                "event byte capacity must be nonzero",
            ))?,
        };
        let subscriber = self.events.subscribe(limits).map_err(event_backend_error)?;
        let id = subscriber.id();
        self.subscriptions.insert(id, subscriber);
        Ok(
            json!({"subscriptionId": id.to_string(), "snapshotRevision": self.engine.snapshot_reader().load().revision()}),
        )
    }

    fn unsubscribe_events(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("unsubscribe requires a subscription id"),
        )?;
        let id = parse_subscription_id(&values[0])?;
        if self.subscriptions.remove(&id).is_none() {
            return Err(HttpControlError::NotFound);
        }
        Ok(Value::String("OK".to_owned()))
    }

    fn poll_events(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params
            .as_array()
            .filter(|values| (1..=2).contains(&values.len()))
            .ok_or(HttpControlError::InvalidParams(
                "pollEvents requires subscription id and optional count",
            ))?;
        let id = parse_subscription_id(&values[0])?;
        let count = values
            .get(1)
            .map(|value| parse_bounded_usize(value, "event count"))
            .transpose()?
            .unwrap_or(64)
            .min(256);
        let subscriber = self
            .subscriptions
            .get_mut(&id)
            .ok_or(HttpControlError::NotFound)?;
        let mut events = Vec::new();
        for _ in 0..count {
            match subscriber.try_next().map_err(event_backend_error)? {
                Some(delivery) => events.push(delivery.into_value()),
                None => break,
            }
        }
        Ok(Value::Array(events))
    }

    fn publish_control_event(&self, method: &str, value: &Value) {
        let gid = value
            .as_str()
            .or_else(|| value.get("gid").and_then(Value::as_str))
            .and_then(|value| value.parse().ok());
        let event = match method {
            "aria2.tellStatus" | "tellStatus" => RpcEvent::notification(
                "ariax.onStatus",
                value.clone(),
                RpcEventClass::Coalesced,
                Some(RpcEventKey::new(gid, "ariax.onStatus")),
            ),
            "aria2.shutdown" | "shutdown" | "aria2.forceShutdown" | "forceShutdown" => {
                RpcEvent::notification(
                    "ariax.onShutdown",
                    json!({"force": self.force_shutdown_requested}),
                    RpcEventClass::Reliable,
                    None,
                )
            }
            _ => return,
        };
        if let Ok(event) = event {
            self.events.publish(event);
        }
    }

    fn reset_observed_statuses(&mut self) {
        let root = self.engine.snapshot_reader().load();
        self.observed_statuses = root
            .tasks()
            .values()
            .filter_map(|task| {
                task.snapshot
                    .wire_status()
                    .ok()
                    .map(|status| (task.snapshot.gid, status))
            })
            .collect();
    }

    fn publish_task_state_events(&mut self) {
        let current = {
            let root = self.engine.snapshot_reader().load();
            root.tasks()
                .values()
                .filter_map(|task| {
                    task.snapshot
                        .wire_status()
                        .ok()
                        .map(|status| (task.snapshot.gid, status))
                })
                .collect::<BTreeMap<_, _>>()
        };
        for (&gid, &status) in &current {
            let previous = self.observed_statuses.get(&gid).copied();
            if previous == Some(status) {
                continue;
            }
            let method = match status {
                Aria2Status::Active => Some("aria2.onDownloadStart"),
                Aria2Status::Paused => previous
                    .is_some_and(|previous| previous != Aria2Status::Paused)
                    .then_some("aria2.onDownloadPause"),
                Aria2Status::Complete => Some("aria2.onDownloadComplete"),
                Aria2Status::Error => Some("aria2.onDownloadError"),
                Aria2Status::Removed => Some("aria2.onDownloadStop"),
                Aria2Status::Waiting => None,
            };
            if let Some(method) = method
                && let Ok(event) = aria2_task_event(method, Some(gid))
            {
                self.events.publish(event);
            }
            if let Ok(event) = RpcEvent::notification(
                "ariax.onStatus",
                json!({"gid": gid.to_string(), "status": status.as_str()}),
                RpcEventClass::Coalesced,
                Some(RpcEventKey::new(Some(gid), "ariax.onStatus")),
            ) {
                self.events.publish(event);
            }
        }
        self.observed_statuses = current;
    }

    fn add_uri(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let array = params.as_array().ok_or(HttpControlError::InvalidParams(
            "addUri params must be an array",
        ))?;
        if !(1..=2).contains(&array.len()) {
            return Err(HttpControlError::InvalidParams(
                "addUri accepts a URI array and optional options object",
            ));
        }
        let uris =
            array
                .first()
                .and_then(Value::as_array)
                .ok_or(HttpControlError::InvalidParams(
                    "addUri requires a URI array",
                ))?;
        let uris = uris
            .iter()
            .map(|uri| {
                uri.as_str()
                    .map(str::to_owned)
                    .ok_or(HttpControlError::InvalidParams("URI must be a string"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if uris.is_empty() {
            return Err(HttpControlError::InvalidParams(
                "addUri requires at least one URI",
            ));
        }
        let options = array.get(1).cloned().unwrap_or_else(|| json!({}));
        let (spec, record, plan, appender) = self.build_admission(uris, options)?;
        let task_id = spec.task();
        let gid = spec.gid();

        // Keep the catalog private to this serialized control call while the
        // journal and SQLite admission are committed in order.
        let inserted = self.tasks.insert(spec).map_err(HttpControlError::Catalog)?;
        if let Err(error) = self.install_journal(gid, appender) {
            self.tasks.remove(task_id);
            return Err(error);
        }
        if let Err(error) = self.prepare_and_execute(
            plan,
            SchedulerCommand::AddValidatedTask {
                task_id,
                gid,
                desired_paused: record.desired_paused,
                conditions: TaskConditions::default(),
            },
        ) {
            self.tasks.remove(task_id);
            let _ = self.session.execute(SessionCommand::CloseJournal { gid });
            return Err(error);
        }
        self.journal_sequences.insert(gid, 2);
        self.try_admit_one(MonotonicInstant::now())?;
        self.drive_engine()?;
        Ok(Value::String(inserted.gid().to_string()))
    }

    fn pause(&mut self, params: Value, force: bool) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        self.execute_control_command(SchedulerCommand::Pause { gid, force })?;
        Ok(Value::String(gid.to_string()))
    }

    fn unpause(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        self.execute_control_command(SchedulerCommand::Resume { gid })?;
        self.try_admit_one(MonotonicInstant::now())?;
        self.drive_engine()?;
        Ok(Value::String(gid.to_string()))
    }

    fn remove(&mut self, params: Value, force: bool) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        self.execute_control_command(SchedulerCommand::Remove { gid, force })?;
        Ok(Value::String(gid.to_string()))
    }

    fn tell_status(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let (gid, keys) = self.resolve_gid_and_keys(&params)?;
        let root = self.engine.snapshot_reader().load();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        self.applied_status(task, keys.as_deref())
    }

    fn tell_active(&self, params: Value) -> Result<Value, HttpControlError> {
        let keys = parse_optional_keys_only(&params)?;
        self.list_statuses(
            &[QueueClass::Active],
            0,
            MAX_RPC_LIST_ITEMS,
            keys.as_deref(),
        )
    }

    fn tell_waiting(&self, params: Value) -> Result<Value, HttpControlError> {
        let (offset, count, keys) = parse_list_params(&params)?;
        self.list_statuses(
            &[QueueClass::Waiting, QueueClass::Demoted, QueueClass::Paused],
            offset,
            count,
            keys.as_deref(),
        )
    }

    fn tell_stopped(&self, params: Value) -> Result<Value, HttpControlError> {
        let (offset, count, keys) = parse_list_params(&params)?;
        self.list_statuses(&[QueueClass::Stopped], offset, count, keys.as_deref())
    }

    fn list_statuses(
        &self,
        classes: &[QueueClass],
        offset: i64,
        count: usize,
        keys: Option<&[String]>,
    ) -> Result<Value, HttpControlError> {
        let root = self.engine.snapshot_reader().load();
        let gids = classes
            .iter()
            .flat_map(|class| root.queue(*class).iter().copied())
            .collect::<Vec<_>>();
        let start = normalized_offset(offset, gids.len());
        let end = start.saturating_add(count).min(gids.len());
        let mut values = Vec::with_capacity(end.saturating_sub(start));
        for gid in &gids[start..end] {
            let task = root.task(*gid).ok_or_else(|| {
                HttpControlError::Scheduler("queue index references a missing task".to_owned())
            })?;
            values.push(self.applied_status(task, keys)?);
        }
        Ok(Value::Array(values))
    }

    fn applied_status(
        &self,
        task: &ariax_runtime::AppliedTaskSnapshot,
        keys: Option<&[String]>,
    ) -> Result<Value, HttpControlError> {
        let snapshot = &task.snapshot;
        let status = snapshot
            .wire_status()
            .map_err(|_| HttpControlError::Scheduler("invalid public snapshot".to_owned()))?;
        let stats = self
            .stats
            .get(task.task_id)
            .map(|stats| stats.snapshot())
            .unwrap_or_default();
        Ok(project_status(status_value(snapshot, status, stats), keys))
    }

    fn pause_all(&mut self, params: Value, force: bool) -> Result<Value, HttpControlError> {
        require_no_params(&params, "pauseAll")?;
        let root = self.engine.snapshot_reader().load();
        let gids = [QueueClass::Active, QueueClass::Waiting, QueueClass::Demoted]
            .into_iter()
            .flat_map(|class| root.queue(class).iter().copied())
            .collect::<Vec<_>>();
        drop(root);
        for gid in gids {
            self.execute_control_command(SchedulerCommand::Pause { gid, force })?;
        }
        Ok(Value::String("OK".to_owned()))
    }

    fn unpause_all(&mut self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "unpauseAll")?;
        let root = self.engine.snapshot_reader().load();
        let gids = root.queue(QueueClass::Paused).to_vec();
        drop(root);
        for gid in gids {
            self.execute_control_command(SchedulerCommand::Resume { gid })?;
        }
        self.try_admit_one(MonotonicInstant::now())?;
        self.drive_engine()?;
        Ok(Value::String("OK".to_owned()))
    }

    fn remove_download_result(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        let root = self.engine.snapshot_reader().load();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?.task_id;
        drop(root);
        self.execute_control_command(SchedulerCommand::RemoveStoppedResult { gid })?;
        self.tasks.remove(task);
        self.stats.remove(task);
        Ok(Value::String("OK".to_owned()))
    }

    fn purge_download_result(&mut self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "purgeDownloadResult")?;
        let root = self.engine.snapshot_reader().load();
        let gids = root.queue(QueueClass::Stopped).to_vec();
        drop(root);
        for gid in gids {
            self.remove_download_result(json!([gid.to_string()]))?;
        }
        Ok(Value::String("OK".to_owned()))
    }

    fn change_position(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 3).ok_or(
            HttpControlError::InvalidParams("changePosition requires GID, position, and mode"),
        )?;
        let gid = self.resolve_gid_text(
            values[0]
                .as_str()
                .ok_or(HttpControlError::InvalidParams("GID must be a string"))?,
        )?;
        let requested = parse_i64(&values[1], "position")?;
        let mode = values[2].as_str().ok_or(HttpControlError::InvalidParams(
            "position mode must be a string",
        ))?;
        let root = self.engine.snapshot_reader().load();
        let (order, current) = queue_order_and_position(&root, gid)?;
        let last = i64::try_from(order.len().saturating_sub(1)).unwrap_or(i64::MAX);
        let target = match mode {
            "POS_SET" => requested,
            "POS_CUR" => i64::try_from(current)
                .unwrap_or(i64::MAX)
                .saturating_add(requested),
            "POS_END" => last.saturating_add(requested),
            _ => return Err(HttpControlError::InvalidParams("invalid position mode")),
        }
        .clamp(0, last);
        drop(root);
        let target = usize::try_from(target)
            .map_err(|_| HttpControlError::InvalidParams("position is out of range"))?;
        self.execute_control_command(SchedulerCommand::ChangePosition {
            gid,
            position: target,
        })?;
        Ok(Value::from(target))
    }

    fn get_uris(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        let spec = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        Ok(Value::Array(
            spec.sources()
                .iter()
                .map(|source| json!({"uri": source.uri(), "status": "used"}))
                .collect(),
        ))
    }

    fn get_files(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        let root = self.engine.snapshot_reader().load();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        let spec = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        let stats = self
            .stats
            .get(task.task_id)
            .map(|stats| stats.snapshot())
            .unwrap_or_default();
        let completed = task.snapshot.completed_length.max(stats.durable_bytes);
        let total = task
            .snapshot
            .total_length
            .unwrap_or(stats.total_length)
            .max(completed);
        let path = spec.output_root().join(spec.output().canonical_string());
        Ok(json!([{
            "index": "1",
            "path": path.to_string_lossy(),
            "length": total.to_string(),
            "completedLength": completed.to_string(),
            "selected": "true",
            "uris": spec.sources().iter().map(|source| json!({"uri": source.uri(), "status":"used"})).collect::<Vec<_>>(),
        }]))
    }

    fn get_servers(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        let spec = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        Ok(Value::Array(
            spec.sources()
                .iter()
                .map(|source| {
                    json!({
                        "index": (usize::try_from(source.id().get()).unwrap_or(usize::MAX) + 1).to_string(),
                        "servers": [{"uri": source.uri(), "currentUri": source.uri(), "downloadSpeed":"0"}],
                    })
                })
                .collect(),
        ))
    }

    fn get_option(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        let spec = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        let options = spec
            .persistence_options()
            .map_err(HttpControlError::TaskSpec)?;
        Ok(string_map_value(options.entries()))
    }

    fn change_option(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 2).ok_or(
            HttpControlError::InvalidParams("changeOption requires GID and option object"),
        )?;
        let gid = self.resolve_gid_text(
            values[0]
                .as_str()
                .ok_or(HttpControlError::InvalidParams("GID must be a string"))?,
        )?;
        let patch = parse_registry_options(&values[1], Scope::RpcChange)?;
        if self.pending_restart_patches.contains_key(&gid)
            || self.pending_source_replacements.contains_key(&gid)
        {
            return Err(HttpControlError::Busy);
        }
        if patch.is_empty() {
            return Ok(Value::String("OK".to_owned()));
        }
        let current = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        let root = self.engine.snapshot_reader().load();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        let previous_generation = task.snapshot.generation;
        let status = task
            .snapshot
            .wire_status()
            .map_err(|_| HttpControlError::Scheduler("invalid public snapshot".to_owned()))?;
        if matches!(
            status,
            Aria2Status::Complete | Aria2Status::Error | Aria2Status::Removed
        ) {
            return Err(HttpControlError::InvalidParams(
                "terminal download options cannot be changed",
            ));
        }
        if status == Aria2Status::Active
            && patch
                .values()
                .any(|entry| entry.runtime_update == RuntimeUpdate::WaitingOnly)
        {
            return Err(HttpControlError::InvalidParams(
                "one or more options may only change while waiting or paused",
            ));
        }
        drop(root);

        let mut merged = current
            .persistence_options()
            .map_err(HttpControlError::TaskSpec)?
            .entries()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect::<BTreeMap<_, _>>();
        for (name, entry) in &patch {
            merged.insert(name.clone(), entry.canonical.clone());
        }
        let options = SanitizedOptionMap::new(merged)
            .map_err(|_| HttpControlError::InvalidParams("option patch exceeds bounds"))?;
        if !self.engine.permits_persisted_options(&options) {
            return Err(HttpControlError::InvalidParams(
                "option patch violates persistence policy",
            ));
        }
        let http_options = HttpTaskOptions::from_sanitized(&options).map_err(|_| {
            HttpControlError::InvalidParams("option is not supported by HTTP tasks")
        })?;
        let output =
            HttpTaskSpec::persisted_output(&options).map_err(HttpControlError::TaskSpec)?;
        let replacement = HttpTaskSpec::new(
            current.task(),
            current.gid(),
            current
                .sources()
                .iter()
                .map(|source| source.uri().to_owned()),
            current.output_root().clone(),
            output,
            http_options,
            current
                .sources()
                .iter()
                .any(crate::HttpSourceSpec::needs_credentials),
        )
        .map_err(HttpControlError::TaskSpec)?;

        let patch_id =
            OptionPatchId::new(self.next_option_patch_id).ok_or(HttpControlError::InvalidConfig)?;
        self.next_option_patch_id = self
            .next_option_patch_id
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        let live_only = patch
            .values()
            .all(|entry| entry.runtime_update == RuntimeUpdate::Live);
        let kind = if status == Aria2Status::Active && !live_only {
            ValidatedOptionPatchKind::ActiveRestart
        } else {
            ValidatedOptionPatchKind::InPlace
        };
        let live_rate = if status == Aria2Status::Active && patch.contains_key("max-download-limit")
        {
            match &self.global_download_rate {
                Some(rate) => Some(
                    rate.prepare_scoped_limit(
                        RateScope::Task(current.task().get()),
                        RateLimit::per_second(replacement.options().max_download_limit),
                    )
                    .map_err(|_| HttpControlError::InvalidConfig)?
                    .ok_or(HttpControlError::Busy)?,
                ),
                None => {
                    return Err(HttpControlError::Unsupported(
                        "live rate control requires the process rate arbiter",
                    ));
                }
            }
        } else {
            None
        };
        let command = SchedulerCommand::ApplyOptionPatch {
            gid,
            patch_id,
            kind,
            satisfies_credentials: None,
        };
        let mut simulation = self.engine.scheduler().clone();
        let outcome = simulation
            .execute_command_at(command.clone(), MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        if kind == ValidatedOptionPatchKind::ActiveRestart {
            self.pending_option_snapshots.insert(
                patch_id,
                PendingOptionSnapshot {
                    options: options.clone(),
                    previous_generation,
                },
            );
            self.pending_restart_patches.insert(gid, patch_id);
        }
        if let Err(error) = self.prepare_outcome_plans(&mut simulation, outcome.effects, None) {
            self.pending_option_snapshots.remove(&patch_id);
            self.pending_restart_patches.remove(&gid);
            return Err(error);
        }
        if kind == ValidatedOptionPatchKind::InPlace {
            self.replace_option_mirror(gid, OptionsSnapshotScope::CurrentGeneration, options)?;
        }
        if let Err(error) = self
            .engine
            .execute_command_at(command, MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))
        {
            self.pending_option_snapshots.remove(&patch_id);
            self.pending_restart_patches.remove(&gid);
            return Err(error);
        }
        self.drive_engine()?;
        if kind == ValidatedOptionPatchKind::ActiveRestart
            && self.engine.scheduler().task(gid).is_some_and(|task| {
                task.pending_option_patch.is_none()
                    && task.pending_barrier.is_none()
                    && task.generation == previous_generation
            })
        {
            self.pending_option_snapshots.remove(&patch_id);
            self.pending_restart_patches.remove(&gid);
            return Err(HttpControlError::Persistence(
                "option patch was not persisted".to_owned(),
            ));
        }
        if let Some(update) = live_rate {
            update.apply();
        }
        self.tasks
            .replace(replacement)
            .map_err(HttpControlError::Catalog)?;
        Ok(Value::String("OK".to_owned()))
    }

    fn change_uri_request(
        &self,
        params: Value,
    ) -> Result<(Gid, Vec<String>, Value), HttpControlError> {
        let values = params.as_array().filter(|values| (4..=5).contains(&values.len())).ok_or(
            HttpControlError::InvalidParams(
                "changeUri requires GID, file index, deleted URIs, added URIs, and optional position",
            ),
        )?;
        let gid = self.resolve_gid_text(
            values[0]
                .as_str()
                .ok_or(HttpControlError::InvalidParams("GID must be a string"))?,
        )?;
        if parse_i64(&values[1], "file index")? != 1 {
            return Err(HttpControlError::InvalidParams(
                "HTTP downloads have exactly one file with index 1",
            ));
        }
        let deleted = parse_uri_array(&values[2])?;
        let added = parse_uri_array(&values[3])?;
        let position = values
            .get(4)
            .map(|value| parse_i64(value, "position"))
            .transpose()?
            .unwrap_or(i64::MAX);
        let spec = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        let mut uris = spec
            .sources()
            .iter()
            .map(|source| source.uri().to_owned())
            .collect::<Vec<_>>();
        let before = uris.len();
        uris.retain(|uri| !deleted.contains(uri));
        let deleted_count = before.saturating_sub(uris.len());
        let insertion = if position < 0 {
            0
        } else {
            usize::try_from(position)
                .unwrap_or(usize::MAX)
                .min(uris.len())
        };
        let mut added_count = 0_usize;
        for uri in added.into_iter().rev() {
            if !uris.contains(&uri) {
                uris.insert(insertion, uri);
                added_count += 1;
            }
        }
        Ok((gid, uris, json!([deleted_count, added_count])))
    }

    fn replace_sources_request(
        &self,
        params: Value,
    ) -> Result<(Gid, Vec<String>, Value), HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 2).ok_or(
            HttpControlError::InvalidParams("replaceSources requires GID and URI array"),
        )?;
        let gid = self.resolve_gid_text(
            values[0]
                .as_str()
                .ok_or(HttpControlError::InvalidParams("GID must be a string"))?,
        )?;
        let uris = parse_uri_array(&values[1])?;
        Ok((gid, uris, Value::String(gid.to_string())))
    }

    fn prepare_source_call(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(HttpTaskSpec, Value), HttpControlError> {
        let (gid, uris, response) = if method == "ariax.replaceSources" {
            self.replace_sources_request(params)?
        } else {
            self.change_uri_request(params)?
        };
        Ok((self.prepare_source_replacement(gid, uris)?, response))
    }

    fn prepare_source_replacement(
        &self,
        gid: Gid,
        uris: Vec<String>,
    ) -> Result<HttpTaskSpec, HttpControlError> {
        if self.pending_restart_patches.contains_key(&gid)
            || self.pending_source_replacements.contains_key(&gid)
        {
            return Err(HttpControlError::Busy);
        }
        let scheduler_task = self
            .engine
            .scheduler()
            .task(gid)
            .ok_or(HttpControlError::NotFound)?;
        if scheduler_task.pending_barrier.is_some() || scheduler_task.pending_source_replacement {
            return Err(HttpControlError::Busy);
        }
        let current = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        let root = self.engine.snapshot_reader().load();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        let status = task
            .snapshot
            .wire_status()
            .map_err(|_| HttpControlError::Scheduler("invalid public snapshot".to_owned()))?;
        if matches!(
            status,
            Aria2Status::Complete | Aria2Status::Error | Aria2Status::Removed
        ) {
            return Err(HttpControlError::InvalidParams(
                "terminal download sources cannot be replaced",
            ));
        }
        drop(root);
        HttpTaskSpec::new(
            current.task(),
            current.gid(),
            uris,
            current.output_root().clone(),
            current.output().clone(),
            current.options().clone(),
            current
                .sources()
                .iter()
                .any(crate::HttpSourceSpec::needs_credentials),
        )
        .map_err(HttpControlError::TaskSpec)
    }

    fn source_call_sync(&mut self, method: &str, params: Value) -> Result<Value, HttpControlError> {
        let (replacement, response) = self.prepare_source_call(method, params)?;
        if self
            .engine
            .scheduler()
            .task(replacement.gid())
            .is_some_and(|task| task.slot.owns_slot())
        {
            return Err(HttpControlError::Busy);
        }
        let mut reply = self.begin_source_plan(replacement, response)?;
        self.complete_source_replacements()?;
        reply.try_recv().map_err(|_| HttpControlError::Busy)?
    }

    fn begin_source_call(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<oneshot::Receiver<Result<Value, HttpControlError>>, HttpControlError> {
        let (replacement, response) = self.prepare_source_call(method, params)?;
        self.begin_source_plan(replacement, response)
    }

    fn begin_source_plan(
        &mut self,
        replacement: HttpTaskSpec,
        response: Value,
    ) -> Result<oneshot::Receiver<Result<Value, HttpControlError>>, HttpControlError> {
        let gid = replacement.gid();
        let (reply, receiver) = oneshot::channel();
        self.execute_control_command(SchedulerCommand::BeginSourceReplacement { gid })?;
        self.pending_source_replacements.insert(
            gid,
            PendingSourceReplacement {
                replacement,
                response,
                reply,
                committing: false,
            },
        );
        Ok(receiver)
    }

    fn complete_source_replacements(&mut self) -> Result<(), HttpControlError> {
        let ready = self
            .pending_source_replacements
            .keys()
            .copied()
            .filter(|gid| {
                self.engine
                    .scheduler()
                    .task(*gid)
                    .is_none_or(|task| task.pending_barrier.is_none() && !task.slot.owns_slot())
            })
            .collect::<Vec<_>>();
        for gid in ready {
            let terminal = self.engine.scheduler().task(gid).is_none_or(|task| {
                matches!(
                    task.state,
                    ariax_core::TaskState::Removed
                        | ariax_core::TaskState::Error
                        | ariax_core::TaskState::Complete
                        | ariax_core::TaskState::StoppedResult
                )
            });
            if terminal {
                let pending = self
                    .pending_source_replacements
                    .remove(&gid)
                    .expect("pending source replacement");
                let _ = pending.reply.send(Err(HttpControlError::InvalidParams(
                    "source replacement was cancelled before commit",
                )));
                continue;
            }
            self.pending_source_replacements
                .get_mut(&gid)
                .expect("pending source replacement")
                .committing = true;
            let result =
                self.execute_control_command(SchedulerCommand::CommitSourceReplacement { gid });
            let pending = self
                .pending_source_replacements
                .remove(&gid)
                .expect("pending source replacement");
            match result {
                Ok(()) => {
                    self.tasks
                        .replace(pending.replacement)
                        .map_err(HttpControlError::Catalog)?;
                    let _ = pending.reply.send(Ok(pending.response));
                }
                Err(error) => {
                    let diagnostic = error.to_string();
                    let _ = pending.reply.send(Err(error));
                    return Err(HttpControlError::Persistence(diagnostic));
                }
            }
        }
        Ok(())
    }

    /// Drives a deferred control call without holding the owner while awaiting a worker.
    pub async fn call_shared(
        plane: &Arc<Mutex<Self>>,
        method: &str,
        params: Value,
    ) -> Result<Value, HttpControlError> {
        let mut reply = {
            let mut owner = plane.lock().await;
            owner.poll_once()?;
            if !matches!(
                method,
                "aria2.changeUri" | "changeUri" | "ariax.replaceSources"
            ) {
                return owner.call(method, params);
            }
            owner.begin_source_call(method, params)?
        };
        loop {
            tokio::select! {
                result = &mut reply => return result.map_err(|_| HttpControlError::Persistence("source replacement owner stopped".to_owned()))?,
                () = tokio::time::sleep(Duration::from_millis(1)) => {
                    if let Err(error) = plane.lock().await.poll_once() {
                        return reply.try_recv().unwrap_or(Err(error));
                    }
                }
            }
        }
    }

    fn get_global_option(&self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "getGlobalOption")?;
        Ok(string_map_value(
            self.global_options
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        ))
    }

    fn change_global_option(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("changeGlobalOption requires one option object"),
        )?;
        let patch = parse_registry_options(&values[0], Scope::RpcGlobal)?;
        if patch.keys().any(|name| !is_executable_global_option(name)) {
            return Err(HttpControlError::InvalidParams(
                "global option is not executable in this checkpoint",
            ));
        }
        for (name, entry) in patch {
            if name == "max-overall-download-limit" {
                self.apply_global_download_limit(&entry.canonical)?;
            }
            self.global_options.insert(name, entry.canonical);
        }
        Ok(Value::String("OK".to_owned()))
    }

    fn get_version(&self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "getVersion")?;
        Ok(json!({
            "version": env!("CARGO_PKG_VERSION"),
            "enabledFeatures": ["HTTP", "HTTPS", "JSON-RPC", "Session", "Async DNS"],
        }))
    }

    fn get_session_info(&self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "getSessionInfo")?;
        Ok(json!({"sessionId": self.session_id.to_string()}))
    }

    fn request_shutdown(&mut self, params: Value, force: bool) -> Result<Value, HttpControlError> {
        require_no_params(&params, if force { "forceShutdown" } else { "shutdown" })?;
        self.shutdown_requested = true;
        self.force_shutdown_requested |= force;
        Ok(Value::String("OK".to_owned()))
    }

    fn check_config(&self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("checkConfig requires configuration text"),
        )?;
        let text = values[0].as_str().ok_or(HttpControlError::InvalidParams(
            "configuration must be text",
        ))?;
        let parsed = parse_flat_config(
            builtin_registry(),
            text,
            UnknownOptionMode::Strict,
            FlatConfigLimits::default(),
            None,
        )
        .map_err(|_| HttpControlError::InvalidParams("configuration is invalid"))?;
        Ok(json!({
            "valid": true,
            "options": parsed.entries().count(),
            "warnings": parsed.warnings().len(),
        }))
    }

    fn reload_config(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("reloadConfig requires configuration text"),
        )?;
        let text = values[0].as_str().ok_or(HttpControlError::InvalidParams(
            "configuration must be text",
        ))?;
        let parsed = parse_flat_config(
            builtin_registry(),
            text,
            UnknownOptionMode::Strict,
            FlatConfigLimits::default(),
            None,
        )
        .map_err(|_| HttpControlError::InvalidParams("configuration is invalid"))?;
        let mut next = self.global_options.clone();
        for (name, entry) in parsed.entries() {
            if !is_executable_global_option(name)
                || !entry.definition.scopes.contains(Scope::Global)
                || matches!(
                    entry.definition.runtime_update,
                    RuntimeUpdate::None
                        | RuntimeUpdate::StartupOnly
                        | RuntimeUpdate::UnsafeCompatOnly
                        | RuntimeUpdate::BtLive
                        | RuntimeUpdate::BtRestartRequired
                )
            {
                return Err(HttpControlError::InvalidParams(
                    "configuration contains a non-reloadable option",
                ));
            }
            next.insert(name.to_owned(), canonical_option_value(&entry.value)?);
        }
        if let Some(limit) = next.get("max-overall-download-limit") {
            self.apply_global_download_limit(limit)?;
        }
        self.global_options = next;
        Ok(json!({"reloaded": true, "options": parsed.entries().count()}))
    }

    fn apply_global_download_limit(&self, canonical: &str) -> Result<(), HttpControlError> {
        let bytes = canonical
            .parse::<u64>()
            .map_err(|_| HttpControlError::InvalidConfig)?;
        if let Some(rate) = &self.global_download_rate {
            rate.set_global_limit(RateLimit::per_second(bytes))
                .map_err(|_| HttpControlError::InvalidConfig)?;
        }
        Ok(())
    }

    fn dump_config(&self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() <= 1).ok_or(
            HttpControlError::InvalidParams("dumpConfig accepts an optional mode"),
        )?;
        let mode = values
            .first()
            .and_then(Value::as_str)
            .unwrap_or("effective");
        if !matches!(mode, "defaults" | "effective") {
            return Err(HttpControlError::InvalidParams(
                "dumpConfig mode must be defaults or effective",
            ));
        }
        if mode == "effective" {
            return Ok(string_map_value(
                self.global_options
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str())),
            ));
        }
        let mut defaults = BTreeMap::new();
        for definition in builtin_registry().definitions() {
            if definition.compat == CompatStatus::Unsupported
                || definition.security != SecurityClass::Normal
            {
                continue;
            }
            if let Some(default) = definition.default {
                let value = parse_option_value(definition, default, None)
                    .map_err(|_| HttpControlError::InvalidConfig)?;
                defaults.insert(definition.name.to_owned(), canonical_option_value(&value)?);
            }
        }
        Ok(string_map_value(
            defaults
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        ))
    }

    fn export_session(&self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "exportSession")?;
        let root = self.engine.snapshot_reader().load();
        if root.len() > MAX_RPC_LIST_ITEMS {
            return Err(HttpControlError::Busy);
        }
        let mut tasks = Vec::with_capacity(root.len());
        for applied in root.tasks().values() {
            let spec = self
                .tasks
                .get(applied.task_id)
                .ok_or(HttpControlError::NotFound)?;
            let options = spec
                .persistence_options()
                .map_err(HttpControlError::TaskSpec)?;
            tasks.push(json!({
                "gid": applied.snapshot.gid.to_string(),
                "uris": spec.sources().iter().map(|source| source.uri()).collect::<Vec<_>>(),
                "options": string_map_value(options.entries()),
                "state": applied.snapshot.state.code(),
            }));
        }
        Ok(json!({"sessionId": self.session_id.to_string(), "tasks": tasks}))
    }

    fn import_session(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("importSession requires an export object"),
        )?;
        let tasks = values[0].get("tasks").and_then(Value::as_array).ok_or(
            HttpControlError::InvalidParams("session export has no tasks"),
        )?;
        if tasks.len() > MAX_RPC_LIST_ITEMS {
            return Err(HttpControlError::InvalidParams("too many session tasks"));
        }
        let mut gids = Vec::with_capacity(tasks.len());
        for task in tasks {
            let uris = task
                .get("uris")
                .ok_or(HttpControlError::InvalidParams("session task has no URIs"))?;
            let mut options = task.get("options").cloned().unwrap_or_else(|| json!({}));
            if let Some(object) = options.as_object_mut() {
                object.insert("pause".to_owned(), Value::Bool(true));
            }
            gids.push(self.add_uri(json!([uris, options]))?);
        }
        Ok(Value::Array(gids))
    }

    fn resolve_gid_param(&self, params: &Value) -> Result<Gid, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("exactly one hexadecimal GID is required"),
        )?;
        let value = values[0].as_str().ok_or(HttpControlError::InvalidParams(
            "a hexadecimal GID is required",
        ))?;
        self.resolve_gid_text(value)
    }

    fn resolve_gid_and_keys(
        &self,
        params: &Value,
    ) -> Result<(Gid, Option<Vec<String>>), HttpControlError> {
        let values = params
            .as_array()
            .filter(|values| (1..=2).contains(&values.len()))
            .ok_or(HttpControlError::InvalidParams(
                "tellStatus requires GID and optional key array",
            ))?;
        let value = values[0].as_str().ok_or(HttpControlError::InvalidParams(
            "a hexadecimal GID is required",
        ))?;
        let keys = values.get(1).map(parse_keys).transpose()?;
        Ok((self.resolve_gid_text(value)?, keys))
    }

    fn resolve_gid_text(&self, value: &str) -> Result<Gid, HttpControlError> {
        if value.is_empty()
            || value.len() > 16
            || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(HttpControlError::InvalidParams("invalid GID"));
        }
        let value = value.to_ascii_lowercase();
        let root = self.engine.snapshot_reader().load();
        let mut matches = root
            .tasks()
            .keys()
            .copied()
            .filter(|gid| gid.to_string().starts_with(&value));
        let gid = matches.next().ok_or(HttpControlError::NotFound)?;
        if matches.next().is_some() {
            return Err(HttpControlError::InvalidParams("GID prefix is ambiguous"));
        }
        Ok(gid)
    }

    fn global_stat(&mut self, params: Value) -> Result<Value, HttpControlError> {
        if !params.as_array().is_some_and(Vec::is_empty) {
            return Err(HttpControlError::InvalidParams(
                "getGlobalStat takes no params",
            ));
        }
        let root = self.engine.snapshot_reader().load();
        let mut active = 0_u64;
        let mut waiting = 0_u64;
        let mut stopped = 0_u64;
        let mut download_speed = 0_u64;
        let mut completed = 0_u64;
        for applied in root.tasks().values() {
            match applied.snapshot.wire_status().ok() {
                Some(Aria2Status::Active) => active += 1,
                Some(Aria2Status::Waiting | Aria2Status::Paused) => waiting += 1,
                Some(Aria2Status::Complete | Aria2Status::Error | Aria2Status::Removed) => {
                    stopped += 1
                }
                None => {}
            }
            let stats = self
                .stats
                .get(applied.task_id)
                .map(|stats| stats.snapshot())
                .unwrap_or_default();
            download_speed = download_speed.saturating_add(stats.current_speed);
            completed = completed
                .saturating_add(applied.snapshot.completed_length.max(stats.durable_bytes));
        }
        Ok(json!({
            "downloadSpeed": download_speed.to_string(),
            "uploadSpeed": "0",
            "numActive": active.to_string(),
            "numWaiting": waiting.to_string(),
            "numStopped": stopped.to_string(),
            "completedLength": completed.to_string(),
        }))
    }

    fn execute_control_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<(), HttpControlError> {
        self.prepare_and_execute_command(command)
    }

    fn prepare_and_execute_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<(), HttpControlError> {
        let mut simulation = self.engine.scheduler().clone();
        let outcome = simulation
            .execute_command_at(command.clone(), MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        self.prepare_outcome_plans(&mut simulation, outcome.effects, None)?;
        self.engine
            .execute_command_at(command, MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
        self.drive_engine()
    }

    fn prepare_and_execute(
        &mut self,
        plan: PersistenceEffectPlan,
        command: SchedulerCommand,
    ) -> Result<(), HttpControlError> {
        self.engine
            .prepare_persistence(plan)
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
        self.engine
            .execute_command_at(command, MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
        self.drive_engine()
    }

    fn drive_engine(&mut self) -> Result<(), HttpControlError> {
        let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
        loop {
            match self.engine.poll_at(MonotonicInstant::now()) {
                ariax_runtime::SchedulerDriverPoll::Idle
                | ariax_runtime::SchedulerDriverPoll::Completed { .. } => {
                    if self.engine.is_idle() {
                        self.retire_promoted_option_patches();
                        return Ok(());
                    }
                }
                ariax_runtime::SchedulerDriverPoll::Progressed
                | ariax_runtime::SchedulerDriverPoll::WaitingForCompletion { .. }
                | ariax_runtime::SchedulerDriverPoll::Backpressured { .. } => {}
                ariax_runtime::SchedulerDriverPoll::Faulted(error) => {
                    return Err(HttpControlError::Scheduler(format!("{error:?}")));
                }
            }
            if Instant::now() >= deadline {
                return Err(HttpControlError::Busy);
            }
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
    }

    fn try_admit_one(&mut self, now: MonotonicInstant) -> Result<(), HttpControlError> {
        if self.supervisor.is_none() {
            return Ok(());
        }
        let mut simulation = self.engine.scheduler().clone();
        let outcome = match simulation.admit_next_at(now) {
            Ok(outcome) => outcome,
            Err(_) => return Ok(()),
        };
        if let Some(rate) = &self.global_download_rate {
            for effect in &outcome.effects {
                if let TransitionEffect::PersistGenerationStarted { task_id, .. } = effect {
                    let spec = self.tasks.get(*task_id).ok_or(HttpControlError::NotFound)?;
                    rate.set_scoped_limit(
                        RateScope::Task(task_id.get()),
                        RateLimit::per_second(spec.options().max_download_limit),
                    )
                    .map_err(|_| HttpControlError::Busy)?;
                }
            }
        }
        self.prepare_outcome_plans(
            &mut simulation,
            outcome.effects,
            Some(GenerationStartReason::RetryReadmission),
        )?;
        self.engine
            .admit_next_at(now)
            .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
        Ok(())
    }

    fn prepare_outcome_plans(
        &mut self,
        simulation: &mut RequestScheduler,
        effects: Vec<TransitionEffect>,
        generation_reason: Option<GenerationStartReason>,
    ) -> Result<(), HttpControlError> {
        let mut queue = VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            if effect.kind().persistence_effect() {
                self.engine
                    .prepare_persistence(self.plan_for_effect(&effect, generation_reason)?)
                    .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
                let generation = simulation
                    .task(effect.gid())
                    .map_or(Generation::INITIAL, |task| task.generation);
                if let Some(ack) = persistence_ack(&effect, generation) {
                    let outcome = simulation
                        .handle_event_at(&ack, MonotonicInstant::now())
                        .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
                    queue.extend(outcome.effects);
                }
            } else if matches!(effect, TransitionEffect::ApplyOptionPatch { .. }) {
                let plan = crate::OptionApplicationPlan::new(
                    effect.clone(),
                    crate::OptionApplicationOutcome::Applied,
                )
                .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
                self.engine
                    .prepare_runtime(crate::RuntimeEffectPreparation::OptionApplication(plan))
                    .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
            }
        }
        Ok(())
    }

    fn prepare_runtime_event(
        &mut self,
        event: &TaskEventEnvelope,
        at: MonotonicInstant,
    ) -> Result<(), HttpControlError> {
        let mut simulation = self.engine.scheduler().clone();
        let outcome = simulation
            .handle_event_at(event, at)
            .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        let generation_reason = match event.event() {
            TaskEvent::RetryReady { .. } => Some(GenerationStartReason::RetryReadmission),
            TaskEvent::ActiveRepresentationRestart { .. } => {
                Some(GenerationStartReason::RepresentationRestart)
            }
            _ => None,
        };
        self.prepare_outcome_plans(&mut simulation, outcome.effects, generation_reason)
    }

    fn plan_for_effect(
        &self,
        effect: &TransitionEffect,
        generation_reason: Option<GenerationStartReason>,
    ) -> Result<PersistenceEffectPlan, HttpControlError> {
        match effect {
            TransitionEffect::PersistTask { .. } => Err(HttpControlError::Unsupported(
                "task admission must use metadata plan",
            )),
            TransitionEffect::PersistGenerationStarted {
                task_id,
                gid,
                generation,
            } => {
                if let Some(patch_id) = self.pending_restart_patches.get(gid).copied() {
                    let pending =
                        self.pending_option_snapshots
                            .get(&patch_id)
                            .ok_or_else(|| {
                                HttpControlError::Persistence(
                                    "option restart snapshot is unavailable".to_owned(),
                                )
                            })?;
                    let previous_generation = Generation::new(generation.get().saturating_sub(1));
                    if previous_generation != pending.previous_generation {
                        return Err(HttpControlError::Persistence(
                            "option patch generation does not match admission".to_owned(),
                        ));
                    }
                    let options = &pending.options;
                    return PersistenceEffectPlan::new(
                        effect.clone(),
                        vec![
                            PersistencePlanStep::AppendAndFlushJournal {
                                gid: *gid,
                                generation: *generation,
                                payload: JournalPayload::GenerationStarted {
                                    previous_generation,
                                    reason: GenerationStartReason::OptionPatch,
                                    next_snapshot_hash: options.snapshot_hash(),
                                    patch_id: Some(patch_id),
                                },
                            },
                            PersistencePlanStep::PromoteTaskOptions {
                                gid: *gid,
                                options: options.clone(),
                            },
                        ],
                    )
                    .map_err(|error| HttpControlError::Persistence(format!("{error:?}")));
                }
                let steps = if *generation == Generation::INITIAL {
                    vec![PersistencePlanStep::FlushJournal {
                        gid: *gid,
                        through_sequence: self.journal_sequences.get(gid).copied().unwrap_or(2),
                    }]
                } else {
                    let options = self
                        .tasks
                        .get(*task_id)
                        .ok_or(HttpControlError::NotFound)?
                        .persistence_options()
                        .map_err(HttpControlError::TaskSpec)?;
                    let previous_generation = Generation::new(generation.get().saturating_sub(1));
                    let snapshot_hash = options.snapshot_hash();
                    let recovered = self.engine.recovered_tasks().iter().find(|task| {
                        task.gid == *gid && task.journal.generation() == previous_generation
                    });
                    let recovered_representation_restart = recovered
                        .filter(|task| task.journal.paused() == Some(TaskPauseReason::Restarting));
                    let reason = if recovered_representation_restart.is_some() {
                        GenerationStartReason::RepresentationRestart
                    } else {
                        generation_reason.unwrap_or(GenerationStartReason::RecoveryRepair)
                    };
                    let mut steps = Vec::with_capacity(3);
                    if reason == GenerationStartReason::RepresentationRestart
                        && recovered_representation_restart.is_none()
                    {
                        steps.push(PersistencePlanStep::AppendAndFlushJournal {
                            gid: *gid,
                            generation: previous_generation,
                            payload: JournalPayload::TaskPaused {
                                reason: TaskPauseReason::Restarting,
                            },
                        });
                    }
                    let staged_snapshot = recovered_representation_restart
                        .and_then(|task| task.journal.pending_options());
                    if let Some(staged) = staged_snapshot {
                        if staged.patch_id().is_some()
                            || staged.snapshot_hash() != snapshot_hash
                            || staged.options() != &options
                        {
                            return Err(HttpControlError::Persistence(
                                "recovered representation restart snapshot does not match the task"
                                    .to_owned(),
                            ));
                        }
                    } else {
                        steps.push(PersistencePlanStep::AppendAndFlushJournal {
                            gid: *gid,
                            generation: previous_generation,
                            payload: JournalPayload::OptionsSnapshot {
                                scope: OptionsSnapshotScope::NextAdmission,
                                patch_id: None,
                                snapshot_hash,
                                options: options.clone(),
                            },
                        });
                    }
                    steps.push(PersistencePlanStep::AppendAndFlushJournal {
                        gid: *gid,
                        generation: *generation,
                        payload: JournalPayload::GenerationStarted {
                            previous_generation,
                            reason,
                            next_snapshot_hash: snapshot_hash,
                            patch_id: None,
                        },
                    });
                    steps
                };
                PersistenceEffectPlan::new(effect.clone(), steps)
                    .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
            TransitionEffect::StageOptionPatch { gid, patch_id, .. } => {
                let options = self
                    .pending_option_snapshots
                    .get(patch_id)
                    .ok_or_else(|| {
                        HttpControlError::Persistence(
                            "option patch snapshot is unavailable".to_owned(),
                        )
                    })?
                    .options
                    .clone();
                let generation = self
                    .engine
                    .snapshot_reader()
                    .load()
                    .task(*gid)
                    .ok_or(HttpControlError::NotFound)?
                    .snapshot
                    .generation;
                PersistenceEffectPlan::new(
                    effect.clone(),
                    vec![
                        PersistencePlanStep::AppendAndFlushJournal {
                            gid: *gid,
                            generation,
                            payload: JournalPayload::OptionsSnapshot {
                                scope: OptionsSnapshotScope::NextAdmission,
                                patch_id: Some(*patch_id),
                                snapshot_hash: options.snapshot_hash(),
                                options: options.clone(),
                            },
                        },
                        PersistencePlanStep::ReplaceTaskOptions {
                            gid: *gid,
                            scope: OptionsSnapshotScope::NextAdmission,
                            options,
                        },
                    ],
                )
                .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
            TransitionEffect::PersistQueueTransition {
                task_id: _,
                gid,
                from: Some(from),
                to: Some(to),
                desired_paused,
                slow_demotion_count,
                slow_slot,
                orders,
            } => {
                let transition = ariax_storage::SessionQueueTransition {
                    gid: *gid,
                    expected_state: session_queue(*from),
                    target_state: session_queue(*to),
                    desired_paused: *desired_paused,
                    slow_demotion_count: *slow_demotion_count,
                    slow_slot: slow_slot.map(|slot| SessionSlowSlotState {
                        original_position: u32::try_from(slot.original_position)
                            .unwrap_or(u32::MAX),
                        retry: Some(ariax_storage::SessionSlowRetryDecision {
                            scheduled_at_ms: slot.decision.scheduled_at_ms,
                            delay_ms: slot.decision.delay_ms,
                        }),
                    }),
                    final_orders: orders
                        .iter()
                        .map(|order| SessionQueueOrder {
                            state: session_queue(order.class),
                            gids: order.order.clone(),
                        })
                        .collect(),
                    updated_ms: now_unix_ms(),
                };
                let step = match self
                    .pending_source_replacements
                    .get(gid)
                    .filter(|pending| pending.committing)
                {
                    Some(pending) => PersistencePlanStep::ReplaceTaskSourcesAndQueue {
                        transition,
                        sources: pending.replacement.persistence_sources(),
                    },
                    None => PersistencePlanStep::TransitionTaskQueue(transition),
                };
                PersistenceEffectPlan::new(effect.clone(), vec![step])
                    .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
            TransitionEffect::PersistTerminal {
                task_id,
                gid,
                generation,
                status,
                error,
                from,
                to,
                desired_paused,
                slow_demotion_count,
                slow_slot,
                orders,
            } => self.terminal_plan(
                effect,
                *task_id,
                *gid,
                *generation,
                *status,
                error.as_ref(),
                *from,
                *to,
                *desired_paused,
                *slow_demotion_count,
                *slow_slot,
                orders,
            ),
            TransitionEffect::DeleteStoppedTaskMetadata {
                gid,
                remaining_order,
                ..
            } => PersistenceEffectPlan::new(
                effect.clone(),
                vec![PersistencePlanStep::DeleteStoppedTaskMetadata {
                    gid: *gid,
                    remaining_order: remaining_order.clone(),
                    updated_ms: now_unix_ms(),
                }],
            )
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}"))),
            _ => Err(HttpControlError::Unsupported(
                "HTTP control effect is not implemented",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn terminal_plan(
        &self,
        effect: &TransitionEffect,
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        status: Aria2Status,
        error: Option<&PublicError>,
        from: QueueClass,
        to: QueueClass,
        desired_paused: bool,
        slow_demotion_count: u32,
        slow_slot: Option<ariax_core::SlowSlotPersistence>,
        orders: &[QueueOrder],
    ) -> Result<PersistenceEffectPlan, HttpControlError> {
        let transition = ariax_storage::SessionQueueTransition {
            gid,
            expected_state: session_queue(from),
            target_state: session_queue(to),
            desired_paused,
            slow_demotion_count,
            slow_slot: slow_slot.map(|slot| SessionSlowSlotState {
                original_position: u32::try_from(slot.original_position).unwrap_or(u32::MAX),
                retry: Some(ariax_storage::SessionSlowRetryDecision {
                    scheduled_at_ms: slot.decision.scheduled_at_ms,
                    delay_ms: slot.decision.delay_ms,
                }),
            }),
            final_orders: orders
                .iter()
                .map(|order| SessionQueueOrder {
                    state: session_queue(order.class),
                    gids: order.order.clone(),
                })
                .collect(),
            updated_ms: now_unix_ms(),
        };
        if status == Aria2Status::Complete && error.is_none() {
            let evidence = self
                .stats
                .completion(task_id)
                .ok_or(HttpControlError::Unsupported(
                    "successful terminal evidence is not available yet",
                ))?;
            let result = SessionStoppedResultRecord {
                gid,
                status: SessionTerminalStatus::Complete,
                error_kind: None,
                safe_message: String::new(),
                total_length: Some(evidence.total_length),
                layout_hash: Some(evidence.layout_hash),
                completed_ms: evidence.completed_at_unix_ms,
            };
            return PersistenceEffectPlan::new(
                effect.clone(),
                vec![
                    PersistencePlanStep::FlushJournal {
                        gid,
                        through_sequence: evidence.terminal_sequence,
                    },
                    PersistencePlanStep::PersistStoppedResult { result, transition },
                ],
            )
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")));
        }
        let (payload, result) = match (status, error) {
            (Aria2Status::Error, Some(error)) => (
                JournalPayload::TaskError {
                    error_class: error.kind(),
                    retriable: !matches!(
                        error.retry_class(),
                        RetryClass::Never | RetryClass::UserAction
                    ),
                    diagnostic_id: error.diagnostic_id().unwrap_or(0),
                },
                SessionStoppedResultRecord {
                    gid,
                    status: SessionTerminalStatus::Error,
                    error_kind: Some(error.kind()),
                    safe_message: error.safe_message().to_owned(),
                    total_length: None,
                    layout_hash: None,
                    completed_ms: now_unix_ms(),
                },
            ),
            (Aria2Status::Removed, None) => (
                JournalPayload::TaskRemoved {
                    reason: TaskRemoveReason::User,
                },
                SessionStoppedResultRecord {
                    gid,
                    status: SessionTerminalStatus::Removed,
                    error_kind: None,
                    safe_message: String::new(),
                    total_length: None,
                    layout_hash: None,
                    completed_ms: now_unix_ms(),
                },
            ),
            _ => {
                return Err(HttpControlError::Unsupported(
                    "terminal result does not match scheduler status",
                ));
            }
        };
        PersistenceEffectPlan::new(
            effect.clone(),
            vec![
                PersistencePlanStep::AppendAndFlushJournal {
                    gid,
                    generation,
                    payload,
                },
                PersistencePlanStep::PersistStoppedResult { result, transition },
            ],
        )
        .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
    }

    fn build_admission(
        &mut self,
        uris: Vec<String>,
        options: Value,
    ) -> Result<
        (
            HttpTaskSpec,
            SessionTaskRecord,
            PersistenceEffectPlan,
            ControlJournalAppender,
        ),
        HttpControlError,
    > {
        let task_id = TaskId::new(self.next_task_id).ok_or(HttpControlError::InvalidConfig)?;
        self.next_task_id = self
            .next_task_id
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        let gid = derive_http_gid(self.session_id, task_id);
        let (http_options, output_root, output, paused) =
            parse_add_options(&options, &self.config.output_root, &uris)?;
        let spec = HttpTaskSpec::new(
            task_id,
            gid,
            uris,
            output_root.clone(),
            output,
            http_options,
            false,
        )
        .map_err(HttpControlError::TaskSpec)?;
        let sanitized = spec
            .persistence_options()
            .map_err(HttpControlError::TaskSpec)?;
        if !self.engine.permits_persisted_options(&sanitized)
            || !HttpTaskOptions::from_sanitized(&sanitized)
                .is_ok_and(|recovered| &recovered == spec.options())
        {
            return Err(HttpControlError::InvalidParams(
                "task options cannot be recovered under the persistence policy",
            ));
        }
        let journal_id = derive_http_journal_id(task_id, gid);
        let journal_directory = http_journal_directory(&self.config.journal_root, gid);
        let mut appender = ControlJournalAppender::create(
            &journal_directory,
            gid,
            journal_id,
            Generation::INITIAL,
            now_unix_ms(),
        )
        .map_err(|error| HttpControlError::Journal(error.to_string()))?;
        append_initial_admission_with_options(
            &mut appender,
            Generation::INITIAL,
            sanitized.clone(),
        )
        .map_err(|error| HttpControlError::Journal(error.to_string()))?;
        let queue = if paused {
            SessionQueueState::Paused
        } else {
            SessionQueueState::Waiting
        };
        let queue_class = if paused {
            QueueClass::Paused
        } else {
            QueueClass::Waiting
        };
        let position = self.engine.scheduler().queue_snapshot(queue_class).len();
        let record = SessionTaskRecord {
            gid,
            session_id: self.session_id,
            queue_state: queue,
            queue_position: u32::try_from(position).map_err(|_| HttpControlError::InvalidConfig)?,
            desired_paused: paused,
            slow_demotion_count: 0,
            slow_slot: None,
            primary_journal_id: journal_id,
            primary_journal_path: PlatformPath::from_current(&journal_directory)
                .map_err(|error| HttpControlError::Journal(error.to_string()))?,
            replica_journal_path: None,
            replica_sequence: None,
            root_display: PlatformPath::from_current(&output_root).map_err(|_error| {
                HttpControlError::InvalidParams("output root is not representable")
            })?,
            cached_layout_hash: None,
            cached_root_binding_hash: None,
            cached_snapshot_hash: sanitized.snapshot_hash(),
            no_space: None,
            created_ms: now_unix_ms(),
            updated_ms: now_unix_ms(),
        };
        let effect = TransitionEffect::PersistTask {
            task_id,
            gid,
            queue: queue_class,
            position,
            desired_paused: paused,
            slow_demotion_count: 0,
            conditions: TaskConditions::default(),
        };
        let plan = PersistenceEffectPlan::new(
            effect,
            vec![PersistencePlanStep::CreateTaskWithMetadata {
                task: record.clone(),
                sources: spec.persistence_sources(),
                options: sanitized,
            }],
        )
        .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
        Ok((spec, record, plan, appender))
    }

    fn install_journal(
        &self,
        gid: Gid,
        appender: ControlJournalAppender,
    ) -> Result<(), HttpControlError> {
        match self
            .session
            .execute(SessionCommand::InstallJournalAppender { gid, appender })
        {
            Ok(SessionCommandResult::Unit) => Ok(()),
            Ok(_) => Err(HttpControlError::Persistence(
                "unexpected journal install result".to_owned(),
            )),
            Err(error) => Err(HttpControlError::Persistence(error.to_string())),
        }
    }
}

/// Tokio-facing backend handle. Calls are serialized through one bounded
/// async mutex, while the scheduler remains the sole mutable engine owner.
#[derive(Clone)]
pub struct HttpControlBackend {
    plane: Arc<Mutex<HttpControlPlane>>,
    shutdown: watch::Sender<bool>,
    events: RpcEventBroker,
}

impl HttpControlBackend {
    #[must_use]
    pub fn new(plane: HttpControlPlane) -> Self {
        let (shutdown, _) = watch::channel(false);
        let events = plane.event_broker();
        Self {
            plane: Arc::new(Mutex::new(plane)),
            shutdown,
            events,
        }
    }

    #[must_use]
    pub fn plane(&self) -> Arc<Mutex<HttpControlPlane>> {
        self.plane.clone()
    }

    #[must_use]
    pub fn into_plane(self) -> Arc<Mutex<HttpControlPlane>> {
        self.plane
    }

    #[must_use]
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    /// Recovers the sole control-plane owner once all transport and progress
    /// handles have been drained.
    pub fn try_into_control_plane(self) -> Result<HttpControlPlane, Self> {
        match Arc::try_unwrap(self.plane) {
            Ok(plane) => Ok(plane.into_inner()),
            Err(plane) => Err(Self {
                plane,
                shutdown: self.shutdown,
                events: self.events,
            }),
        }
    }
}

impl HttpRpcBackend for HttpControlBackend {
    fn call(&self, method: &str, params: Value) -> crate::RpcFuture {
        let plane = self.plane.clone();
        let shutdown = self.shutdown.clone();
        let method = method.to_owned();
        Box::pin(async move {
            let result = HttpControlPlane::call_shared(&plane, &method, params)
                .await
                .map_err(control_backend_error);
            if plane.lock().await.shutdown_requested() {
                let _ = shutdown.send(true);
            }
            result
        })
    }
}

impl crate::RpcWebSocketBackend for HttpControlBackend {
    fn event_broker(&self) -> RpcEventBroker {
        self.events.clone()
    }
}

fn control_backend_error(error: HttpControlError) -> HttpRpcBackendError {
    let code = match error {
        HttpControlError::InvalidParams(_) | HttpControlError::TaskSpec(_) => -32602,
        HttpControlError::Unsupported(_) => -32601,
        HttpControlError::NotFound => -32004,
        HttpControlError::Busy => -32005,
        HttpControlError::SlowConsumer => -32007,
        _ => -32000,
    };
    HttpRpcBackendError::new(code, error.to_string())
}

fn require_no_params(params: &Value, method: &'static str) -> Result<(), HttpControlError> {
    if params.as_array().is_some_and(Vec::is_empty) {
        Ok(())
    } else {
        Err(HttpControlError::InvalidParams(match method {
            "pauseAll" => "pauseAll takes no params",
            "unpauseAll" => "unpauseAll takes no params",
            "purgeDownloadResult" => "purgeDownloadResult takes no params",
            "getGlobalOption" => "getGlobalOption takes no params",
            "getVersion" => "getVersion takes no params",
            "getSessionInfo" => "getSessionInfo takes no params",
            "getGlobalStat" => "getGlobalStat takes no params",
            "saveSession" => "saveSession takes no params",
            "shutdown" => "shutdown takes no params",
            "forceShutdown" => "forceShutdown takes no params",
            "exportSession" => "exportSession takes no params",
            _ => "method takes no params",
        }))
    }
}

fn parse_bounded_usize(value: &Value, name: &'static str) -> Result<usize, HttpControlError> {
    let value = value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(HttpControlError::InvalidParams(match name {
            "event capacity" => "event capacity must be a bounded integer",
            "event byte capacity" => "event byte capacity must be a bounded integer",
            "event count" => "event count must be a bounded integer",
            _ => "value must be a bounded integer",
        }))?;
    Ok(value)
}

fn parse_subscription_id(value: &Value) -> Result<u64, HttpControlError> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .filter(|value| *value != 0)
        .ok_or(HttpControlError::InvalidParams(
            "subscription id must be a nonzero integer",
        ))
}

fn event_backend_error(error: RpcEventError) -> HttpControlError {
    match error {
        RpcEventError::Disconnected(crate::RpcEventDisconnect::SlowConsumer) => {
            HttpControlError::SlowConsumer
        }
        RpcEventError::Disconnected(crate::RpcEventDisconnect::Unsubscribed) => {
            HttpControlError::NotFound
        }
        RpcEventError::TooManySubscribers => HttpControlError::Busy,
        _ => HttpControlError::InvalidParams("invalid event subscription"),
    }
}

fn aria2_task_event(method: &'static str, gid: Option<Gid>) -> Result<RpcEvent, RpcEventError> {
    let gid = gid.ok_or(RpcEventError::Serialization)?;
    RpcEvent::notification(
        method,
        json!([{"gid": gid.to_string()}]),
        RpcEventClass::Reliable,
        None,
    )
}

fn parse_optional_keys_only(params: &Value) -> Result<Option<Vec<String>>, HttpControlError> {
    let values = params.as_array().filter(|values| values.len() <= 1).ok_or(
        HttpControlError::InvalidParams("tellActive accepts an optional key array"),
    )?;
    values.first().map(parse_keys).transpose()
}

fn parse_list_params(
    params: &Value,
) -> Result<(i64, usize, Option<Vec<String>>), HttpControlError> {
    let values = params
        .as_array()
        .filter(|values| (2..=3).contains(&values.len()))
        .ok_or(HttpControlError::InvalidParams(
            "list query requires offset, count, and optional keys",
        ))?;
    let offset = parse_i64(&values[0], "offset")?;
    let count = parse_i64(&values[1], "count")?;
    if count < 0
        || usize::try_from(count)
            .ok()
            .is_none_or(|count| count > MAX_RPC_LIST_ITEMS)
    {
        return Err(HttpControlError::InvalidParams(
            "list count must be between 0 and 1000",
        ));
    }
    let keys = values.get(2).map(parse_keys).transpose()?;
    Ok((
        offset,
        usize::try_from(count).expect("validated nonnegative bounded count"),
        keys,
    ))
}

fn parse_keys(value: &Value) -> Result<Vec<String>, HttpControlError> {
    let values = value
        .as_array()
        .ok_or(HttpControlError::InvalidParams("keys must be an array"))?;
    if values.len() > 128 {
        return Err(HttpControlError::InvalidParams("too many status keys"));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|key| key.len() <= 128)
                .map(str::to_owned)
                .ok_or(HttpControlError::InvalidParams(
                    "status key must be a bounded string",
                ))
        })
        .collect()
}

fn parse_uri_array(value: &Value) -> Result<Vec<String>, HttpControlError> {
    let values = value
        .as_array()
        .ok_or(HttpControlError::InvalidParams("URIs must be an array"))?;
    if values.len() > crate::MAX_HTTP_TASK_SOURCES {
        return Err(HttpControlError::InvalidParams("too many URIs"));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or(HttpControlError::InvalidParams("URI must be a string"))
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedRegistryOption {
    canonical: String,
    runtime_update: RuntimeUpdate,
}

fn default_global_options() -> Result<BTreeMap<String, String>, HttpControlError> {
    let mut options = BTreeMap::new();
    for definition in builtin_registry().definitions() {
        if !is_executable_global_option(definition.name)
            || !definition.scopes.contains(Scope::Global)
            || definition.compat == CompatStatus::Unsupported
            || definition.security != SecurityClass::Normal
        {
            continue;
        }
        let Some(default) = definition.default else {
            continue;
        };
        let value = parse_option_value(definition, default, None)
            .map_err(|_| HttpControlError::InvalidConfig)?;
        options.insert(definition.name.to_owned(), canonical_option_value(&value)?);
    }
    Ok(options)
}

fn is_executable_global_option(name: &str) -> bool {
    name == "max-overall-download-limit"
}

fn parse_registry_options(
    value: &Value,
    scope: Scope,
) -> Result<BTreeMap<String, ParsedRegistryOption>, HttpControlError> {
    let object = value
        .as_object()
        .ok_or(HttpControlError::InvalidParams("options must be an object"))?;
    if object.len() > ariax_storage::MAX_OPTION_MAP_ENTRIES {
        return Err(HttpControlError::InvalidParams("too many options"));
    }
    let registry = builtin_registry();
    let mut parsed = BTreeMap::new();
    for (name, value) in object {
        let definition = registry
            .find(name)
            .ok_or(HttpControlError::InvalidParams("unknown option"))?;
        if !definition.scopes.contains(scope) {
            return Err(HttpControlError::InvalidParams(
                "option is not allowed on this RPC surface",
            ));
        }
        if definition.security != SecurityClass::Normal {
            return Err(HttpControlError::InvalidParams(
                "option requires a local administrative surface",
            ));
        }
        if matches!(
            definition.runtime_update,
            RuntimeUpdate::None
                | RuntimeUpdate::StartupOnly
                | RuntimeUpdate::UnsafeCompatOnly
                | RuntimeUpdate::BtLive
                | RuntimeUpdate::BtRestartRequired
        ) {
            return Err(HttpControlError::InvalidParams(
                "option cannot be changed at runtime",
            ));
        }
        let input = option_input_text(value)?;
        let value = parse_option_value(definition, &input, None)
            .map_err(|_| HttpControlError::InvalidParams("invalid option value"))?;
        let canonical = canonical_option_value(&value)?;
        parsed.insert(
            name.clone(),
            ParsedRegistryOption {
                canonical,
                runtime_update: definition.runtime_update,
            },
        );
    }
    Ok(parsed)
}

fn option_input_text(value: &Value) -> Result<String, HttpControlError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(HttpControlError::InvalidParams(
            "option values must be strings, booleans, or numbers",
        )),
    }
}

fn canonical_option_value(value: &OptionValue) -> Result<String, HttpControlError> {
    Ok(match value {
        OptionValue::Bool(value) => value.to_string(),
        OptionValue::Integer(value) => value.to_string(),
        OptionValue::SizeBytes(value) => value.to_string(),
        OptionValue::DurationSeconds(value) => value.to_string(),
        OptionValue::Enum(value) | OptionValue::String(value) => value.clone(),
        OptionValue::Path(value) => value.to_string_lossy().into_owned(),
        OptionValue::HeaderList(values) => values.join(","),
        OptionValue::StatusCodeSet(values) => values
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(","),
        OptionValue::Secret(_) => {
            return Err(HttpControlError::InvalidParams(
                "secret option cannot be persisted by this RPC surface",
            ));
        }
    })
}

fn parse_i64(value: &Value, name: &'static str) -> Result<i64, HttpControlError> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or(HttpControlError::InvalidParams(match name {
            "offset" => "offset must be an integer",
            "count" => "count must be an integer",
            "position" => "position must be an integer",
            _ => "value must be an integer",
        }))
}

fn normalized_offset(offset: i64, length: usize) -> usize {
    if offset >= 0 {
        usize::try_from(offset).unwrap_or(usize::MAX).min(length)
    } else {
        length.saturating_sub(usize::try_from(offset.unsigned_abs()).unwrap_or(usize::MAX))
    }
}

fn queue_order_and_position(
    root: &ariax_runtime::StatusSnapshotRoot,
    gid: Gid,
) -> Result<(&[Gid], usize), HttpControlError> {
    for class in [
        QueueClass::Waiting,
        QueueClass::Demoted,
        QueueClass::Paused,
        QueueClass::Active,
        QueueClass::Stopped,
    ] {
        let order = root.queue(class);
        if let Some(position) = order.iter().position(|candidate| *candidate == gid) {
            return Ok((order, position));
        }
    }
    Err(HttpControlError::NotFound)
}

fn project_status(value: Value, keys: Option<&[String]>) -> Value {
    let Some(keys) = keys else {
        return value;
    };
    let Some(object) = value.as_object() else {
        return value;
    };
    Value::Object(
        keys.iter()
            .filter_map(|key| object.get(key).cloned().map(|value| (key.clone(), value)))
            .collect(),
    )
}

fn string_map_value<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(name, value)| (name.to_owned(), Value::String(value.to_owned())))
            .collect(),
    )
}

fn status_value(
    snapshot: &TaskSnapshot,
    status: Aria2Status,
    stats: HttpTransferStatsSnapshot,
) -> Value {
    let completed = snapshot.completed_length.max(stats.durable_bytes);
    let total = snapshot
        .total_length
        .or((stats.total_length != 0).then_some(stats.total_length));
    let mut value = json!({
        "gid": snapshot.gid.to_string(),
        "status": status.as_str(),
        "totalLength": total.unwrap_or(0).to_string(),
        "completedLength": completed.to_string(),
        "downloadSpeed": stats.current_speed.to_string(),
        "uploadSpeed": "0",
        "connections": stats.active_connections.to_string(),
        "errorCode": snapshot.error.as_ref().map_or_else(|| "0".to_owned(), |error| error.kind().number().to_string()),
        "errorMessage": snapshot.error.as_ref().map_or_else(String::new, |error| error.safe_message().to_owned()),
    });
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "verifiedLength".to_owned(),
            Value::String(stats.durable_bytes.to_string()),
        );
        object.insert(
            "verifiedSpeed".to_owned(),
            Value::String(stats.durable_speed.to_string()),
        );
        object.insert(
            "retryCount".to_owned(),
            Value::String(stats.retry_count.to_string()),
        );
        object.insert(
            "discardedLength".to_owned(),
            Value::String(stats.discarded_bytes.to_string()),
        );
        object.insert(
            "discardBudgetConsumed".to_owned(),
            Value::String(stats.discard_budget_consumed.to_string()),
        );
        object.insert(
            "discardBudgetRemaining".to_owned(),
            Value::String(stats.discard_budget_remaining.to_string()),
        );
        object.insert(
            "receivedPayloadLength".to_owned(),
            Value::String(stats.raw_body_bytes.to_string()),
        );
        object.insert(
            "acceptedLength".to_owned(),
            Value::String(stats.accepted_bytes.to_string()),
        );
        object.insert(
            "wireSpeed".to_owned(),
            Value::String(stats.wire_speed.to_string()),
        );
        object.insert(
            "usefulSpeed".to_owned(),
            Value::String(stats.useful_speed.to_string()),
        );
        object.insert(
            "smoothedSpeed".to_owned(),
            Value::String(stats.smoothed_speed.to_string()),
        );
        object.insert(
            "sampleAge".to_owned(),
            Value::String(stats.sample_age.as_millis().to_string()),
        );
        object.insert(
            "connectionCondition".to_owned(),
            Value::String(stats.connection_condition.code().to_owned()),
        );
        object.insert(
            "conditionReason".to_owned(),
            Value::String(
                stats
                    .condition_reason
                    .map_or("", |reason| reason.code())
                    .to_owned(),
            ),
        );
        object.insert(
            "rateDebt".to_owned(),
            Value::String(stats.rate_debt_bytes.to_string()),
        );
        if let Some(diagnostic) = stats.retry_diagnostic {
            object.insert(
                "retryDiagnostic".to_owned(),
                retry_diagnostic_value(diagnostic),
            );
        }
    }
    value
}

fn retry_diagnostic_value(diagnostic: crate::HttpRetryDiagnosticSnapshot) -> Value {
    json!({
        "trigger": diagnostic.cause.code(),
        "httpStatus": diagnostic.cause.http_status().unwrap_or(0).to_string(),
        "recoveredErrorClass": diagnostic.cause.recovered_error_class().map_or("", ariax_core::ErrorKind::code),
        "uriId": diagnostic.source.get().to_string(),
        "pieceId": diagnostic.piece.get().to_string(),
        "attempt": diagnostic.total_attempt.to_string(),
        "remainingAttempts": diagnostic.total_remaining.to_string(),
        "mirrorAttempt": diagnostic.source_attempt.to_string(),
        "mirrorRemainingAttempts": diagnostic.source_remaining.to_string(),
        "scheduledAt": diagnostic.scheduled_at_unix_ms.to_string(),
        "retryDelay": diagnostic.delay_ms.to_string(),
        "retryAt": diagnostic.retry_at_unix_ms.to_string(),
        "retryAfterStatus": diagnostic.delay.map_or("", crate::HttpRetryDelayDiagnostic::code),
        "stopReason": diagnostic.stop_reason.map_or("", crate::HttpRetryStopReason::code),
        "nextAction": diagnostic.next_action.code(),
        "leaseDisposition": diagnostic.lease_disposition.code(),
        "previousLease": diagnostic.prior_lease.map_or(0, ariax_core::LeaseId::get).to_string(),
        "nextLease": diagnostic.next_lease.map_or(0, ariax_core::LeaseId::get).to_string(),
        "recovered": matches!(diagnostic.cause, crate::HttpRetryDiagnosticCause::Recovered(_)),
    })
}

fn parse_add_options(
    options: &Value,
    default_root: &Path,
    uris: &[String],
) -> Result<
    (
        HttpTaskOptions,
        PathBuf,
        ariax_storage::SafeRelativePath,
        bool,
    ),
    HttpControlError,
> {
    let object = options.as_object().ok_or(HttpControlError::InvalidParams(
        "addUri options must be an object",
    ))?;
    let mut parsed = HttpTaskOptions::default();
    let mut root = default_root.to_path_buf();
    let mut out = None;
    let mut paused = false;
    for (name, value) in object {
        if is_retry_option(name) {
            continue;
        }
        match name.as_str() {
            "dir" => {
                let requested = PathBuf::from(
                    value
                        .as_str()
                        .ok_or(HttpControlError::InvalidParams("dir must be a string"))?,
                );
                if requested != default_root {
                    return Err(HttpControlError::InvalidParams(
                        "dir must equal the configured output root",
                    ));
                }
                root = requested;
            }
            "out" => {
                out = Some(
                    value
                        .as_str()
                        .ok_or(HttpControlError::InvalidParams("out must be a string"))?
                        .to_owned(),
                )
            }
            "pause" => {
                paused = match value {
                    Value::Bool(value) => *value,
                    Value::String(value) if value == "true" => true,
                    Value::String(value) if value == "false" => false,
                    _ => {
                        return Err(HttpControlError::InvalidParams(
                            "pause must be true or false",
                        ));
                    }
                }
            }
            "split" => parsed.split = parse_nonzero(value, "split", 1024)?,
            "max-connection-per-server" => {
                parsed.max_connections_per_server = parse_nonzero(value, name, 1024)?
            }
            "min-split-size" => parsed.min_split_size = parse_size(value)?,
            "piece-length" => parsed.piece_length = parse_size(value)?,
            "connect-timeout" => {
                parsed.connect_timeout = Duration::from_secs(parse_timeout(value)?)
            }
            "timeout" => parsed.response_body_timeout = Duration::from_secs(parse_timeout(value)?),
            "max-download-limit" => parsed.max_download_limit = parse_size(value)?,
            "lowest-speed-limit" => parsed.lowest_speed_limit = parse_size(value)?,
            "endgame-max-duplicates" => {
                parsed.endgame_max_duplicates =
                    usize::try_from(parse_u64(value, name)?).map_err(|_| {
                        HttpControlError::InvalidParams("endgame duplicate cap is too large")
                    })?;
                if parsed.endgame_max_duplicates > MAX_HTTP_ENDGAME_MAX_DUPLICATES {
                    return Err(HttpControlError::InvalidParams(
                        "endgame duplicate cap is too large",
                    ));
                }
            }
            "checksum" => {
                parsed.checksum = Some(
                    HttpContentChecksum::parse(
                        value
                            .as_str()
                            .ok_or(HttpControlError::InvalidParams("checksum must be a string"))?,
                    )
                    .map_err(|_| HttpControlError::InvalidParams("invalid checksum"))?,
                );
            }
            "verify-mirror-identity" => {
                parsed.mirror_identity = match value.as_str() {
                    Some("strict") => crate::HttpMirrorIdentityPolicy::RequireSharedDigest,
                    Some("off") | None => crate::HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
                    _ => {
                        return Err(HttpControlError::InvalidParams(
                            "invalid mirror identity policy",
                        ));
                    }
                }
            }
            _ => return Err(HttpControlError::InvalidParams("unsupported addUri option")),
        }
    }
    if object.keys().any(|name| is_retry_option(name)) {
        parsed.retry = Some(parse_retry_options(object)?);
    }
    if !root.is_absolute() {
        return Err(HttpControlError::InvalidParams("dir must be absolute"));
    }
    let output = out.unwrap_or_else(|| default_output_name(uris));
    let output = SafePathBuilder::from_user_path(&output, PathPlatform::current())
        .map_err(|_| HttpControlError::InvalidParams("out is not a safe relative path"))?;
    Ok((parsed, root, output, paused))
}

fn is_retry_option(name: &str) -> bool {
    matches!(
        name,
        "max-tries"
            | "retry-wait"
            | "retry-profile"
            | "retry-on"
            | "retry-on-http-status"
            | "retry-on-http-status-add"
            | "retry-on-http-status-remove"
            | "retry-after"
            | "retry-after-max"
            | "retry-after-min"
            | "retry-backoff"
            | "retry-max-wait"
            | "retry-max-attempts"
            | "retry-max-attempts-per-mirror"
            | "retry-max-elapsed"
            | "stale-validator-policy"
    )
}

fn parse_retry_options(
    object: &serde_json::Map<String, Value>,
) -> Result<HttpRetryPolicy, HttpControlError> {
    let profile = match object.get("retry-profile") {
        Some(value) => HttpRetryProfile::parse(retry_text(value, "retry-profile")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry profile"))?,
        None => HttpRetryProfile::Conservative,
    };
    let mut policy = HttpRetryPolicy::from_profile(profile);

    if let Some(value) = object.get("retry-on") {
        policy.retry_on = HttpRetryTriggerSet::parse(retry_text(value, "retry-on")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry trigger set"))?;
    }
    if let Some(value) = object.get("retry-on-http-status") {
        policy.retryable_statuses =
            HttpRetryStatusSet::parse(retry_text(value, "retry-on-http-status")?)
                .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?;
    }
    if let Some(value) = object.get("retry-on-http-status-add") {
        for code in HttpRetryStatusSet::parse(retry_text(value, "retry-on-http-status-add")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?
            .iter()
        {
            policy
                .retryable_statuses
                .insert(code)
                .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?;
        }
    }
    if let Some(value) = object.get("retry-on-http-status-remove") {
        for code in HttpRetryStatusSet::parse(retry_text(value, "retry-on-http-status-remove")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?
            .iter()
        {
            policy.retryable_statuses.remove(code);
        }
    }

    let max_tries = object
        .get("max-tries")
        .map(|value| parse_retry_attempt_cap(value, "max-tries"))
        .transpose()?;
    let max_attempts = object
        .get("retry-max-attempts")
        .map(|value| parse_retry_attempt_cap(value, "retry-max-attempts"))
        .transpose()?;
    if let Some(value) = stricter_retry_cap(max_tries, max_attempts) {
        policy.max_attempts = value;
    }
    if let Some(value) = object.get("retry-max-attempts-per-mirror") {
        policy.max_attempts_per_mirror =
            parse_retry_attempt_cap(value, "retry-max-attempts-per-mirror")?;
    }
    if let Some(value) = object.get("retry-wait") {
        policy.base_wait =
            parse_retry_duration(value, "retry-wait", MAX_HTTP_RETRY_WAIT_SECS, true)?;
    }
    if let Some(value) = object.get("retry-max-wait") {
        policy.max_wait =
            parse_retry_duration(value, "retry-max-wait", MAX_HTTP_RETRY_WAIT_SECS, false)?;
    }
    if let Some(value) = object.get("retry-max-elapsed") {
        policy.max_elapsed = parse_retry_duration(
            value,
            "retry-max-elapsed",
            MAX_HTTP_RETRY_ELAPSED_SECS,
            false,
        )?;
    }
    if let Some(value) = object.get("retry-after-min") {
        policy.retry_after_min =
            parse_retry_duration(value, "retry-after-min", MAX_HTTP_RETRY_WAIT_SECS, true)?;
    }
    if let Some(value) = object.get("retry-after-max") {
        policy.retry_after_max =
            parse_retry_duration(value, "retry-after-max", MAX_HTTP_RETRY_WAIT_SECS, true)?;
    }
    if let Some(value) = object.get("retry-after") {
        policy.respect_retry_after = matches!(
            HttpRetryAfterPolicy::parse(retry_text(value, "retry-after")?)
                .map_err(|_| HttpControlError::InvalidParams("invalid retry-after policy"))?,
            HttpRetryAfterPolicy::Respect
        );
    }
    if let Some(value) = object.get("retry-backoff") {
        policy.backoff = HttpRetryBackoff::parse(retry_text(value, "retry-backoff")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry backoff"))?;
    }
    if let Some(value) = object.get("stale-validator-policy") {
        policy.stale_validator_policy =
            crate::HttpStaleValidatorPolicy::parse(retry_text(value, "stale-validator-policy")?)
                .map_err(|_| HttpControlError::InvalidParams("invalid stale validator policy"))?;
    }
    policy
        .validate()
        .map_err(|_| HttpControlError::InvalidParams("invalid retry policy"))?;
    Ok(policy)
}

fn retry_text<'a>(value: &'a Value, name: &str) -> Result<&'a str, HttpControlError> {
    value
        .as_str()
        .ok_or(HttpControlError::InvalidParams(match name {
            "retry-profile" => "retry-profile must be a string",
            "retry-on" => "retry-on must be a string",
            "retry-on-http-status" | "retry-on-http-status-add" | "retry-on-http-status-remove" => {
                "retry status set must be a string"
            }
            "retry-after" => "retry-after must be a string",
            "retry-backoff" => "retry-backoff must be a string",
            "stale-validator-policy" => "stale-validator-policy must be a string",
            _ => "retry option must be a string",
        }))
}

fn parse_retry_attempt_cap(value: &Value, name: &str) -> Result<NonZeroU32, HttpControlError> {
    let value = parse_u64(value, name)?;
    let value = u32::try_from(value)
        .map_err(|_| HttpControlError::InvalidParams("retry attempt cap is too large"))?;
    if value > MAX_HTTP_RETRY_ATTEMPTS {
        return Err(HttpControlError::InvalidParams(
            "retry attempt cap is too large",
        ));
    }
    NonZeroU32::new(value).ok_or(HttpControlError::InvalidParams(
        "retry attempt cap must be nonzero",
    ))
}

fn stricter_retry_cap(
    max_tries: Option<NonZeroU32>,
    max_attempts: Option<NonZeroU32>,
) -> Option<NonZeroU32> {
    match (max_tries, max_attempts) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn parse_retry_duration(
    value: &Value,
    name: &str,
    maximum: u64,
    allow_zero: bool,
) -> Result<Duration, HttpControlError> {
    let seconds = parse_u64(value, name)?;
    if seconds > maximum || (!allow_zero && seconds == 0) {
        return Err(HttpControlError::InvalidParams(
            "retry duration is out of range",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_nonzero(
    value: &Value,
    name: &str,
    maximum: usize,
) -> Result<NonZeroUsize, HttpControlError> {
    let number = parse_u64(value, name)?;
    let number = usize::try_from(number)
        .map_err(|_| HttpControlError::InvalidParams("option is too large"))?;
    if number > maximum {
        return Err(HttpControlError::InvalidParams("option is too large"));
    }
    NonZeroUsize::new(number).ok_or(HttpControlError::InvalidParams("option must be nonzero"))
}

fn parse_u64(value: &Value, _name: &str) -> Result<u64, HttpControlError> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or(HttpControlError::InvalidParams("numeric option is invalid"))
}

fn parse_size(value: &Value) -> Result<u64, HttpControlError> {
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    let text = value
        .as_str()
        .ok_or(HttpControlError::InvalidParams("size option is invalid"))?
        .trim();
    let (digits, multiplier) = match text.as_bytes().last().copied() {
        Some(b'K' | b'k') => (&text[..text.len() - 1], 1024_u64),
        Some(b'M' | b'm') => (&text[..text.len() - 1], 1024_u64.pow(2)),
        Some(b'G' | b'g') => (&text[..text.len() - 1], 1024_u64.pow(3)),
        Some(b'T' | b't') => (&text[..text.len() - 1], 1024_u64.pow(4)),
        _ => (text, 1),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(multiplier))
        .ok_or(HttpControlError::InvalidParams("size option is invalid"))
}

fn parse_timeout(value: &Value) -> Result<u64, HttpControlError> {
    let seconds = parse_u64(value, "timeout")?;
    if !(1..=crate::MAX_HTTP_TIMEOUT_SECS).contains(&seconds) {
        return Err(HttpControlError::InvalidParams(
            "timeout must be between 1 and 600 seconds",
        ));
    }
    Ok(seconds)
}

fn default_output_name(uris: &[String]) -> String {
    uris.first()
        .and_then(|uri| uri.rsplit('/').find(|part| !part.is_empty()))
        .filter(|part| !part.contains('?') && !part.contains('#'))
        .unwrap_or("download")
        .to_owned()
}

fn derive_http_gid(session: SessionId, task: TaskId) -> Gid {
    let mut digest = Sha256::new();
    digest.update(b"ariax/http-gid/v1\0");
    digest.update(session.as_bytes());
    digest.update(task.get().to_le_bytes());
    let bytes: [u8; 32] = digest.finalize().into();
    let mut value_bytes = [0_u8; 8];
    value_bytes.copy_from_slice(&bytes[..8]);
    let mut value = u64::from_le_bytes(value_bytes) | (1_u64 << 63);
    if value == 0 {
        value = 1;
    }
    Gid::new(value).expect("derived HTTP GID is nonzero")
}

fn session_queue(class: QueueClass) -> SessionQueueState {
    match class {
        QueueClass::Waiting => SessionQueueState::Waiting,
        QueueClass::Demoted => SessionQueueState::Demoted,
        QueueClass::Paused => SessionQueueState::Paused,
        QueueClass::Active => SessionQueueState::Active,
        QueueClass::Stopped => SessionQueueState::Stopped,
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn persistence_ack(
    effect: &TransitionEffect,
    current_generation: Generation,
) -> Option<TaskEventEnvelope> {
    let task_id = effect.task_id();
    let gid = effect.gid();
    let generation = effect.generation().unwrap_or(current_generation);
    let event = match effect {
        TransitionEffect::PersistGenerationStarted { .. } => {
            TaskEvent::GenerationPersisted { gid, generation }
        }
        TransitionEffect::StageOptionPatch { patch_id, .. } => TaskEvent::OptionPatchPersisted {
            gid,
            generation,
            patch_id: *patch_id,
        },
        TransitionEffect::PersistTerminal { status, .. } => TaskEvent::TerminalPersisted {
            gid,
            generation,
            status: *status,
        },
        TransitionEffect::DeleteStoppedTaskMetadata { deletion_id, .. } => {
            TaskEvent::StoppedResultDeleted {
                gid,
                generation,
                deletion_id: *deletion_id,
            }
        }
        _ => return None,
    };
    Some(event.for_task(task_id))
}

trait PersistenceKindExt {
    fn persistence_effect(self) -> bool;
}

impl PersistenceKindExt for ariax_core::TransitionEffectKind {
    fn persistence_effect(self) -> bool {
        matches!(
            self,
            Self::PersistTask
                | Self::PersistQueueTransition
                | Self::StageOptionPatch
                | Self::PersistGenerationStarted
                | Self::PersistConditions
                | Self::PersistHostKeyChallenge
                | Self::PersistHostKeyPinAndClearChallenge
                | Self::PersistHostKeyChallengeRejected
                | Self::PersistTerminal
                | Self::DeleteStoppedTaskMetadata
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HTTP_CONNECTION_RESERVATION_BYTES, HttpDestinationPolicy, HttpDirectTransportConfig,
        HttpMultiRangeWorker, HttpMultiRangeWorkerConfig, HttpPolicyClient, HttpPolicyClientConfig,
        HttpResolver, HttpResolverConfig, HttpTransportBudgets, ProcessBootstrapConfig,
        RuntimeEffectConfig, StartupRecoveryConfig, StorageEngineConfig, bootstrap_process,
    };
    use ariax_core::{ErrorKind, LeaseId, PieceId, SchedulerConfig, UriId};
    use ariax_runtime::ShutdownStep;
    use ariax_storage::{
        JournalDirectoryCapability, JournalStateLimits, ReplayLimits, ReplayStop,
        SanitizedOptionMap, SessionOwnerConfig, SessionStore, SessionStoreConfig,
        SessionTerminalStatus,
    };
    use std::fs;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory {
        root: PathBuf,
        control: PathBuf,
        output: PathBuf,
        journals: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "ariax-http-control-{}-{}",
                std::process::id(),
                TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            create_private_directory(&root);
            let control = root.join("control");
            let output = root.join("output");
            let journals = control.join("http-journals");
            create_private_directory(&control);
            create_private_directory(&output);
            create_private_directory(&journals);
            Self {
                root,
                control,
                output,
                journals,
            }
        }

        fn process_config(&self) -> ProcessBootstrapConfig {
            let capacity = NonZeroUsize::new(16).expect("capacity");
            ProcessBootstrapConfig {
                session_owner: SessionOwnerConfig::new(self.root.join("session.db")),
                control_directory: self.control.clone(),
                allowed_output_roots: vec![self.output.clone()],
                replay_limits: ReplayLimits::default(),
                journal_state_limits: JournalStateLimits::default(),
                recovery: StartupRecoveryConfig {
                    scheduler: SchedulerConfig::new(capacity, capacity, false)
                        .expect("scheduler config"),
                    now_wall_unix_ms: 1_000,
                    now_monotonic: MonotonicInstant::now(),
                    max_retry_wait_ms: NonZeroU64::new(60_000).expect("retry wait"),
                    max_slow_wait_ms: NonZeroU64::new(60_000).expect("slow wait"),
                    max_no_space_wait_ms: NonZeroU64::new(60_000).expect("no-space wait"),
                    max_retry_elapsed_ms: 60_000,
                },
                runtime: RuntimeEffectConfig {
                    request_capacity: capacity,
                    event_capacity: capacity,
                    timer_capacity: capacity,
                    option_plan_capacity: capacity,
                },
                persistence_plan_capacity: capacity,
                shutdown_step_timeout_ms: crate::DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS,
                updated_ms: 1_000,
                recovery_created_at_unix_ms: 1_000,
            }
        }

        fn control_plane(&self) -> HttpControlPlane {
            self.control_plane_with_supervisor(HttpWorkerSupervisorConfig::default())
        }

        fn control_plane_with_supervisor(
            &self,
            supervisor: HttpWorkerSupervisorConfig,
        ) -> HttpControlPlane {
            self.control_plane_with_policy(supervisor, ariax_config::persisted_option_is_safe)
        }

        fn control_plane_with_policy(
            &self,
            supervisor: HttpWorkerSupervisorConfig,
            policy: impl ariax_storage::PersistedOptionPolicy + Clone + Send + Sync + 'static,
        ) -> HttpControlPlane {
            let engine =
                bootstrap_process(self.process_config(), policy).expect("bootstrap process");
            HttpControlPlane::new(
                engine,
                HttpControlPlaneConfig {
                    output_root: self.output.clone(),
                    journal_root: self.journals.clone(),
                    task_capacity: NonZeroUsize::new(16).expect("task capacity"),
                    supervisor,
                },
            )
            .expect("control plane")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn retry_diagnostic_rpc_shape_is_bounded_and_credential_free() {
        let diagnostic = crate::HttpRetryDiagnosticSnapshot {
            cause: crate::HttpRetryDiagnosticCause::Live(crate::HttpRetryCause::HttpStatus(503)),
            source: UriId::new(3),
            piece: PieceId::new(9),
            prior_lease: LeaseId::new(11),
            next_lease: LeaseId::new(12),
            total_attempt: 2,
            total_remaining: 3,
            source_attempt: 1,
            source_remaining: 2,
            scheduled_at_unix_ms: 1_000,
            delay_ms: 5_000,
            retry_at_unix_ms: 6_000,
            delay: Some(crate::HttpRetryDelayDiagnostic::Live(
                crate::HttpRetryDelaySource::RetryAfterClamped,
            )),
            stop_reason: None,
            next_action: crate::HttpRetryNextAction::DifferentSource,
            lease_disposition: crate::HttpRetryLeaseDisposition::Aborted,
        };

        let value = retry_diagnostic_value(diagnostic);
        assert_eq!(value["trigger"], "http-status");
        assert_eq!(value["httpStatus"], "503");
        assert_eq!(value["recoveredErrorClass"], "");
        assert_eq!(value["uriId"], "3");
        assert_eq!(value["pieceId"], "9");
        assert_eq!(value["attempt"], "2");
        assert_eq!(value["remainingAttempts"], "3");
        assert_eq!(value["mirrorAttempt"], "1");
        assert_eq!(value["mirrorRemainingAttempts"], "2");
        assert_eq!(value["retryAfterStatus"], "retry-after-clamped");
        assert_eq!(value["nextAction"], "different-source");
        assert_eq!(value["leaseDisposition"], "aborted");
        assert_eq!(value["previousLease"], "11");
        assert_eq!(value["nextLease"], "12");
        assert_eq!(value["recovered"], false);
        assert!(!value.to_string().contains("secret"));

        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let root = plane.engine.snapshot_reader().load();
        let applied = root.task(gid).expect("paused task snapshot");
        let status = applied.snapshot.wire_status().expect("public task status");
        let rendered = status_value(
            &applied.snapshot,
            status,
            HttpTransferStatsSnapshot {
                retry_diagnostic: Some(diagnostic),
                ..HttpTransferStatsSnapshot::default()
            },
        );
        assert_eq!(rendered["retryDiagnostic"], value);
        drop(root);
        assert!(plane.shutdown().expect("shutdown control plane").is_clean());

        let recovered = retry_diagnostic_value(crate::HttpRetryDiagnosticSnapshot {
            cause: crate::HttpRetryDiagnosticCause::Recovered(ErrorKind::Network),
            prior_lease: None,
            next_lease: None,
            delay: Some(crate::HttpRetryDelayDiagnostic::Recovered(
                ariax_storage::RetryReason::Backoff,
            )),
            next_action: crate::HttpRetryNextAction::RetryRange,
            lease_disposition: crate::HttpRetryLeaseDisposition::UnknownRecovered,
            ..diagnostic
        });
        assert_eq!(recovered["trigger"], "recovered");
        assert_eq!(recovered["recoveredErrorClass"], "Network");
        assert_eq!(recovered["retryAfterStatus"], "backoff");
        assert_eq!(recovered["previousLease"], "0");
        assert_eq!(recovered["nextLease"], "0");
        assert_eq!(recovered["recovered"], true);
    }

    #[cfg(unix)]
    fn create_private_directory(path: &Path) {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path).expect("create private test directory");
    }

    #[cfg(windows)]
    fn create_private_directory(path: &Path) {
        ariax_windows_security::create_private_directory(path)
            .expect("create private test directory");
    }

    fn add_paused(plane: &mut HttpControlPlane) -> Gid {
        let result = plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/file.bin"], {"pause": true}]),
            )
            .expect("add paused HTTP task");
        result
            .as_str()
            .expect("GID result")
            .parse()
            .expect("valid GID")
    }

    #[test]
    fn query_surface_uses_bounded_indexes_prefixes_and_projection() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let prefix = &gid.to_string()[..8];

        let status = plane
            .call(
                "aria2.tellStatus",
                json!([prefix, ["gid", "status", "missing"]]),
            )
            .expect("prefix status");
        assert_eq!(status, json!({"gid": gid.to_string(), "status": "paused"}));

        let waiting = plane
            .call("aria2.tellWaiting", json!([0, 1000, ["gid"]]))
            .expect("waiting list");
        assert_eq!(waiting, json!([{"gid": gid.to_string()}]));
        assert!(matches!(
            plane.call("aria2.tellWaiting", json!([0, 1001])),
            Err(HttpControlError::InvalidParams(
                "list count must be between 0 and 1000"
            ))
        ));

        let uris = plane
            .call("aria2.getUris", json!([prefix]))
            .expect("URI view");
        assert_eq!(uris[0]["uri"], "http://example.test/file.bin");
        let options = plane
            .call("aria2.getOption", json!([prefix]))
            .expect("option view");
        assert_eq!(options["out"], "file.bin");
        assert_eq!(
            plane
                .call("aria2.getSessionInfo", json!([]))
                .expect("session info")["sessionId"]
                .as_str()
                .expect("session id")
                .len(),
            32
        );

        assert!(plane.shutdown().expect("shutdown control plane").is_clean());
    }

    #[test]
    fn pause_resume_bulk_position_and_shutdown_controls_are_shared() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let first = add_paused(&mut plane);
        let second = add_paused(&mut plane);

        assert_eq!(
            plane
                .call(
                    "aria2.changePosition",
                    json!([second.to_string(), 0, "POS_SET"])
                )
                .expect("move task"),
            json!(0)
        );
        let waiting = plane
            .call("aria2.tellWaiting", json!([0, 2, ["gid"]]))
            .expect("ordered waiting list");
        assert_eq!(waiting[0]["gid"], second.to_string());
        assert_eq!(waiting[1]["gid"], first.to_string());

        assert_eq!(
            plane
                .call("aria2.unpauseAll", json!([]))
                .expect("resume all"),
            "OK"
        );
        assert_eq!(
            plane
                .call("aria2.forcePauseAll", json!([]))
                .expect("pause all"),
            "OK"
        );
        assert_eq!(
            plane
                .call("aria2.forceShutdown", json!([]))
                .expect("request shutdown"),
            "OK"
        );
        assert!(plane.shutdown_requested());
        assert!(plane.force_shutdown_requested());

        assert!(plane.shutdown().expect("shutdown control plane").is_clean());
    }

    #[test]
    fn source_replacement_validates_then_persists_before_catalog_publication() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);

        assert_eq!(
            plane
                .call(
                    "aria2.changeUri",
                    json!([
                        gid.to_string(),
                        1,
                        ["http://example.test/file.bin"],
                        ["https://mirror.test/new.bin"],
                        0
                    ]),
                )
                .expect("replace source"),
            json!([1, 1])
        );
        assert_eq!(
            plane
                .call("aria2.getUris", json!([gid.to_string()]))
                .expect("updated source view")[0]["uri"],
            "https://mirror.test/new.bin"
        );
        match plane
            .session
            .execute(SessionCommand::ReadTaskSources { gid })
            .expect("read persisted sources")
        {
            SessionCommandResult::TaskSources(sources) => assert_eq!(
                sources[0].persistence_safe_uri.as_deref(),
                Some("https://mirror.test/new.bin")
            ),
            result => panic!("unexpected source result: {result:?}"),
        }
        assert!(matches!(
            plane.call(
                "ariax.replaceSources",
                json!([gid.to_string(), ["ftp://example.test/file"]]),
            ),
            Err(HttpControlError::TaskSpec(
                HttpTaskSpecError::UnsupportedScheme
            ))
        ));
        assert_eq!(
            plane
                .call("aria2.getUris", json!([gid.to_string()]))
                .expect("rejected source retained old view")[0]["uri"],
            "https://mirror.test/new.bin"
        );

        assert!(plane.shutdown().expect("shutdown control plane").is_clean());
    }

    #[test]
    fn typed_option_changes_are_atomic_persisted_and_scope_checked() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);

        assert_eq!(
            plane
                .call(
                    "aria2.changeOption",
                    json!([gid.to_string(), {"max-download-limit": "1M", "timeout": 30}]),
                )
                .expect("change task options"),
            "OK"
        );
        let options = plane
            .call("aria2.getOption", json!([gid.to_string()]))
            .expect("updated task options");
        assert_eq!(options["max-download-limit"], "1048576");
        assert_eq!(options["timeout"], "30");
        match plane
            .session
            .execute(SessionCommand::ReadTaskOptions {
                gid,
                scope: OptionsSnapshotScope::CurrentGeneration,
            })
            .expect("read task options")
        {
            SessionCommandResult::TaskOptions(options) => {
                assert_eq!(
                    options
                        .entries()
                        .find_map(|(name, value)| (name == "max-download-limit").then_some(value)),
                    Some("1048576")
                );
            }
            result => panic!("unexpected option result: {result:?}"),
        }

        assert!(matches!(
            plane.call(
                "aria2.changeOption",
                json!([gid.to_string(), {"rpc-secret": "secret"}]),
            ),
            Err(HttpControlError::InvalidParams(
                "option is not allowed on this RPC surface"
            )) | Err(HttpControlError::InvalidParams(
                "option requires a local administrative surface"
            ))
        ));
        assert_eq!(
            plane
                .call(
                    "aria2.changeGlobalOption",
                    json!([{"max-overall-download-limit": "2M"}]),
                )
                .expect("change global option"),
            "OK"
        );
        assert_eq!(
            plane
                .call("aria2.getGlobalOption", json!([]))
                .expect("global options")["max-overall-download-limit"],
            "2097152"
        );
        assert!(matches!(
            plane.call("aria2.changeGlobalOption", json!([{"timeout": 30}])),
            Err(HttpControlError::InvalidParams(
                "global option is not executable in this checkpoint"
            ))
        ));

        assert!(plane.shutdown().expect("shutdown control plane").is_clean());
    }

    #[test]
    fn event_subscriptions_are_bounded_pollable_and_explicitly_removed() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let subscription = plane
            .call("ariax.subscribe", json!([4, 4096]))
            .expect("subscribe");
        let id = subscription["subscriptionId"]
            .as_str()
            .expect("subscription id")
            .to_owned();
        assert_eq!(subscription["snapshotRevision"], 0);

        let gid = add_paused(&mut plane);
        assert_eq!(
            plane
                .call("aria2.unpause", json!([gid.to_string()]))
                .expect("resume task"),
            gid.to_string()
        );
        assert_eq!(
            plane
                .call("aria2.remove", json!([gid.to_string()]))
                .expect("remove task"),
            gid.to_string()
        );
        let events = plane
            .call("ariax.pollEvents", json!([id, 4]))
            .expect("poll events");
        let events = events.as_array().expect("events");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["method"], "ariax.onStatus");
        assert_eq!(events[0]["params"]["status"], "removed");
        assert_eq!(events[1]["method"], "aria2.onDownloadStop");
        assert_eq!(events[1]["params"][0]["gid"], gid.to_string());

        assert_eq!(
            plane
                .call(
                    "ariax.unsubscribe",
                    json!([subscription["subscriptionId"].clone()]),
                )
                .expect("unsubscribe"),
            "OK"
        );
        assert!(matches!(
            plane.call("ariax.pollEvents", json!([id])),
            Err(HttpControlError::NotFound)
        ));

        assert!(plane.shutdown().expect("shutdown control plane").is_clean());
    }

    #[test]
    fn config_reload_is_atomic_and_session_exports_are_bounded_and_importable() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let original = add_paused(&mut plane);

        assert_eq!(
            plane
                .call(
                    "ariax.checkConfig",
                    json!(["max-overall-download-limit=1M\ntimeout=30\n"]),
                )
                .expect("check config")["valid"],
            true
        );
        assert_eq!(
            plane
                .call(
                    "ariax.reloadConfig",
                    json!(["max-overall-download-limit=2M\n"]),
                )
                .expect("reload config")["reloaded"],
            true
        );
        assert_eq!(
            plane
                .call("ariax.dumpConfig", json!(["effective"]))
                .expect("dump effective config")["max-overall-download-limit"],
            "2097152"
        );
        assert!(matches!(
            plane.call("ariax.reloadConfig", json!(["session-store=memory\n"])),
            Err(HttpControlError::InvalidParams(
                "configuration contains a non-reloadable option"
            ))
        ));
        assert_eq!(
            plane
                .call("ariax.dumpConfig", json!(["effective"]))
                .expect("failed reload retained config")["max-overall-download-limit"],
            "2097152"
        );

        let export = plane
            .call("ariax.exportSession", json!([]))
            .expect("export session");
        assert_eq!(export["tasks"].as_array().expect("tasks").len(), 1);
        assert_eq!(export["tasks"][0]["gid"], original.to_string());
        let imported = plane
            .call("ariax.importSession", json!([export]))
            .expect("import session");
        assert_eq!(imported.as_array().expect("imported gids").len(), 1);
        assert_ne!(imported[0], original.to_string());
        assert_eq!(
            plane
                .call("aria2.tellWaiting", json!([0, 10, ["gid"]]))
                .expect("waiting after import")
                .as_array()
                .expect("waiting")
                .len(),
            2
        );

        assert!(plane.shutdown().expect("shutdown control plane").is_clean());
    }

    fn replay_journal_payloads(
        directory: &TestDirectory,
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
    ) -> Vec<JournalPayload> {
        let journal_directory = http_journal_directory(&directory.journals, gid);
        let capability = JournalDirectoryCapability::open_trusted(&journal_directory)
            .expect("journal directory capability");
        let paths = ControlJournalAppender::discover_segment_paths(
            &capability,
            ReplayLimits::default().max_segments,
        )
        .expect("journal segment paths");
        let (mut appender, replay) = ControlJournalAppender::open_recovered(
            &journal_directory,
            &paths,
            gid,
            derive_http_journal_id(task_id, gid),
            ReplayLimits::default(),
            generation,
            now_unix_ms(),
        )
        .expect("reopen journal");
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        let payloads = replay
            .records
            .iter()
            .map(|record| record.decode_payload().expect("decode payload"))
            .collect();
        appender.close_flushed().expect("close replayed journal");
        payloads
    }

    fn append_restarting_prefix(
        directory: &TestDirectory,
        task_id: TaskId,
        gid: Gid,
        options: Option<SanitizedOptionMap>,
    ) -> Option<ariax_storage::JournalHash> {
        let snapshot_hash = options.as_ref().map(SanitizedOptionMap::snapshot_hash);
        let journal_directory = http_journal_directory(&directory.journals, gid);
        let capability = JournalDirectoryCapability::open_trusted(&journal_directory)
            .expect("journal directory capability");
        let paths = ControlJournalAppender::discover_segment_paths(
            &capability,
            ReplayLimits::default().max_segments,
        )
        .expect("journal paths");
        let (mut appender, replay) = ControlJournalAppender::open_recovered(
            &journal_directory,
            &paths,
            gid,
            derive_http_journal_id(task_id, gid),
            ReplayLimits::default(),
            Generation::INITIAL,
            now_unix_ms(),
        )
        .expect("open journal for staged restart");
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        let marker = appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::TaskPaused {
                    reason: TaskPauseReason::Restarting,
                },
            )
            .expect("append restarting marker");
        appender.flush(marker.sequence()).expect("flush marker");
        if let (Some(snapshot_hash), Some(options)) = (snapshot_hash, options) {
            let staged = appender
                .append_payload(
                    Generation::INITIAL,
                    &JournalPayload::OptionsSnapshot {
                        scope: OptionsSnapshotScope::NextAdmission,
                        patch_id: None,
                        snapshot_hash,
                        options,
                    },
                )
                .expect("append staged snapshot");
            appender.flush(staged.sequence()).expect("flush snapshot");
        }
        appender.close_flushed().expect("close staged journal");
        snapshot_hash
    }

    async fn serve_control_file(data: Arc<[u8]>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).await.expect("request");
                    request.push(byte[0]);
                }
                let text = String::from_utf8(request).expect("ASCII request");
                let range = text
                    .lines()
                    .find_map(|line| {
                        let value = line
                            .strip_prefix("Range: bytes=")
                            .or_else(|| line.strip_prefix("range: bytes="))?;
                        let (start, end) = value.split_once('-')?;
                        Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                    })
                    .expect("range");
                let body = &data[range.0..=range.1];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nETag: \"control-v1\"\r\nConnection: close\r\n\r\n",
                    body.len(),
                    range.0,
                    range.1,
                    data.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
                stream.write_all(body).await.expect("body");
            }
        });
        (format!("http://{address}/file.bin"), task)
    }

    async fn serve_control_restart_file(data: Arc<[u8]>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            for index in 0..4 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).await.expect("request");
                    request.push(byte[0]);
                }
                let text = String::from_utf8(request).expect("ASCII request");
                let (start, end) = text
                    .lines()
                    .find_map(|line| {
                        let value = line
                            .strip_prefix("Range: bytes=")
                            .or_else(|| line.strip_prefix("range: bytes="))?;
                        let (start, end) = value.split_once('-')?;
                        Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                    })
                    .expect("range");
                let body = &data[start..=end];
                let etag = if index == 0 { "\"v1\"" } else { "\"v2\"" };
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nAccept-Ranges: bytes\r\nETag: {etag}\r\nConnection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
                if index != 1 {
                    stream.write_all(body).await.expect("body");
                }
            }
        });
        (format!("http://{address}/restart.bin"), task)
    }

    fn attach_loopback_worker(plane: &mut HttpControlPlane, directory: &TestDirectory) {
        let resolver = HttpResolver::new(HttpResolverConfig::default()).expect("resolver");
        let client = HttpPolicyClient::new(
            resolver,
            HttpPolicyClientConfig {
                destination: HttpDestinationPolicy {
                    allow_loopback: true,
                    ..HttpDestinationPolicy::default()
                },
                direct: HttpDirectTransportConfig {
                    connect_timeout: Duration::from_secs(5),
                    handshake_timeout: Duration::from_secs(5),
                    max_connections_per_origin: 2,
                    max_idle_connections_per_origin: 0,
                    budgets: HttpTransportBudgets::new(2, 2 * HTTP_CONNECTION_RESERVATION_BYTES)
                        .expect("transport budgets"),
                    ..HttpDirectTransportConfig::default()
                },
                ..HttpPolicyClientConfig::default()
            },
        );
        let worker = HttpMultiRangeWorker::new(
            client,
            HttpMultiRangeWorkerConfig {
                journal_root: directory.journals.clone(),
                storage: StorageEngineConfig::default(),
                ..HttpMultiRangeWorkerConfig::default()
            },
            plane.stats_catalog(),
        )
        .expect("worker")
        .with_session_owner(plane.session_handle());
        plane
            .attach_worker(Arc::new(worker))
            .expect("attach worker");
    }

    struct UncooperativeShutdownWorker {
        started: Arc<Notify>,
    }

    struct DelayedCancellationWorker {
        started: Arc<Notify>,
        drain: Arc<tokio::sync::Semaphore>,
    }

    impl HttpTaskWorker for DelayedCancellationWorker {
        fn start(
            &self,
            _task: Arc<HttpTaskSpec>,
            _generation: Generation,
            cancellation: crate::HttpCancellation,
        ) -> crate::HttpWorkerFuture {
            let started = self.started.clone();
            let drain = self.drain.clone();
            Box::pin(async move {
                started.notify_one();
                cancellation.cancelled().await;
                drain.acquire().await.expect("drain gate").forget();
                Err(PublicError::new(
                    ErrorKind::Cancelled,
                    "cancelled",
                    RetryClass::Never,
                ))
            })
        }
    }

    fn option_mirror(
        plane: &HttpControlPlane,
        gid: Gid,
        scope: OptionsSnapshotScope,
    ) -> SanitizedOptionMap {
        match plane
            .session
            .execute(SessionCommand::ReadTaskOptions { gid, scope })
            .expect("read option mirror")
        {
            SessionCommandResult::TaskOptions(options) => options,
            other => panic!("unexpected options response: {other:?}"),
        }
    }

    async fn poll_until(
        plane: &mut HttpControlPlane,
        predicate: impl Fn(&HttpControlPlane) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !predicate(plane) {
            plane.poll_once().expect("drive control plane");
            assert!(
                Instant::now() < deadline,
                "control progress timed out: {plane:?}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn append_and_flush(
        plane: &HttpControlPlane,
        gid: Gid,
        generation: Generation,
        payload: JournalPayload,
    ) {
        let through_sequence = match plane
            .session
            .execute(SessionCommand::AppendJournal {
                gid,
                generation,
                payload,
            })
            .expect("append prefix")
        {
            SessionCommandResult::JournalAppended(evidence) => evidence.sequence(),
            other => panic!("unexpected append response: {other:?}"),
        };
        plane
            .session
            .execute(SessionCommand::FlushJournal {
                gid,
                through_sequence,
            })
            .expect("flush prefix");
    }

    #[tokio::test]
    async fn disconnected_source_caller_does_not_abandon_the_accepted_operation() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let started = Arc::new(Notify::new());
        let drain = Arc::new(tokio::sync::Semaphore::new(0));
        plane
            .attach_worker(Arc::new(DelayedCancellationWorker {
                started: started.clone(),
                drain: drain.clone(),
            }))
            .expect("worker");
        let gid: Gid = plane
            .call("aria2.addUri", json!([["http://example.test/file.bin"]]))
            .expect("add")
            .as_str()
            .expect("gid")
            .parse()
            .expect("gid");
        plane.poll_once().expect("start worker");
        started.notified().await;
        poll_until(&mut plane, |plane| {
            plane
                .engine
                .scheduler()
                .task(gid)
                .expect("task")
                .pending_barrier
                .is_none()
        })
        .await;
        let shared = Arc::new(Mutex::new(plane));
        let caller = shared.clone();
        let request = tokio::spawn(async move {
            HttpControlPlane::call_shared(
                &caller,
                "ariax.replaceSources",
                json!([gid.to_string(), ["http://new.test/file.bin"]]),
            )
            .await
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !shared
            .lock()
            .await
            .pending_source_replacements
            .contains_key(&gid)
        {
            assert!(!request.is_finished());
            assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        request.abort();
        assert!(
            request
                .await
                .expect_err("caller disconnected")
                .is_cancelled()
        );
        let mut plane = Arc::try_unwrap(shared)
            .expect("owner remains available")
            .into_inner();
        drain.add_permits(1);
        poll_until(&mut plane, |plane| {
            plane.pending_source_replacements.is_empty()
        })
        .await;
        assert_eq!(
            plane
                .tasks
                .get_gid(gid)
                .expect("committed catalog")
                .sources()[0]
                .uri(),
            "http://new.test/file.bin"
        );
        drain.add_permits(1);
        plane.shutdown_async().await.expect("shutdown");
    }

    #[tokio::test]
    async fn interrupted_source_quiescence_recovers_old_sources_and_running_intent() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane_with_supervisor(HttpWorkerSupervisorConfig {
            shutdown_timeout: Duration::from_millis(10),
            ..HttpWorkerSupervisorConfig::default()
        });
        let started = Arc::new(Notify::new());
        plane
            .attach_worker(Arc::new(DelayedCancellationWorker {
                started: started.clone(),
                drain: Arc::new(tokio::sync::Semaphore::new(0)),
            }))
            .expect("worker");
        let gid: Gid = plane
            .call("aria2.addUri", json!([["http://example.test/file.bin"]]))
            .expect("add")
            .as_str()
            .expect("gid")
            .parse()
            .expect("gid");
        plane.poll_once().expect("start worker");
        started.notified().await;
        poll_until(&mut plane, |plane| {
            plane
                .engine
                .scheduler()
                .task(gid)
                .expect("task")
                .pending_barrier
                .is_none()
        })
        .await;
        let reply = plane
            .begin_source_call(
                "ariax.replaceSources",
                json!([gid.to_string(), ["http://new.test/file.bin"]]),
            )
            .expect("begin quiescence");
        assert!(
            !plane
                .shutdown_async()
                .await
                .expect("bounded interrupted shutdown")
                .is_clean()
        );
        assert!(reply.await.is_err());
        let recovered = directory.control_plane();
        assert_eq!(
            recovered.tasks.get_gid(gid).expect("old sources").sources()[0].uri(),
            "http://example.test/file.bin"
        );
        let task = recovered
            .engine
            .scheduler()
            .task(gid)
            .expect("running intent");
        assert!(!task.desired_paused);
        assert_eq!(task.state, ariax_core::TaskState::Waiting);
        assert!(!task.pending_source_replacement);
        recovered.shutdown().expect("recovered shutdown");
    }

    #[tokio::test]
    async fn source_replacement_waits_for_drain_without_blocking_queries_or_user_controls() {
        for method in ["ariax.replaceSources", "aria2.changeUri"] {
            for user_control in [None, Some("aria2.pause"), Some("aria2.remove")] {
                let directory = TestDirectory::new();
                let mut plane = directory.control_plane();
                let started = Arc::new(Notify::new());
                let drain = Arc::new(tokio::sync::Semaphore::new(0));
                plane
                    .attach_worker(Arc::new(DelayedCancellationWorker {
                        started: started.clone(),
                        drain: drain.clone(),
                    }))
                    .expect("worker");
                let gid: Gid = plane
                    .call("aria2.addUri", json!([["http://example.test/file.bin"]]))
                    .expect("add")
                    .as_str()
                    .expect("gid")
                    .parse()
                    .expect("gid");
                plane.poll_once().expect("start worker");
                started.notified().await;
                poll_until(&mut plane, |plane| {
                    plane
                        .engine
                        .scheduler()
                        .task(gid)
                        .expect("task")
                        .pending_barrier
                        .is_none()
                })
                .await;
                let shared = Arc::new(Mutex::new(plane));
                let caller = shared.clone();
                let params = if method == "aria2.changeUri" {
                    json!([
                        gid.to_string(),
                        1,
                        ["http://example.test/file.bin"],
                        ["http://new.test/file.bin"]
                    ])
                } else {
                    json!([gid.to_string(), ["http://new.test/file.bin"]])
                };
                let request = tokio::spawn(async move {
                    HttpControlPlane::call_shared(&caller, method, params).await
                });
                let deadline = Instant::now() + Duration::from_secs(2);
                while !shared
                    .lock()
                    .await
                    .pending_source_replacements
                    .contains_key(&gid)
                {
                    assert!(
                        !request.is_finished(),
                        "source request rejected before quiescence"
                    );
                    assert!(Instant::now() < deadline, "source request did not start");
                    tokio::task::yield_now().await;
                }
                assert!(!request.is_finished());
                let status = tokio::time::timeout(
                    Duration::from_millis(500),
                    HttpControlPlane::call_shared(
                        &shared,
                        "aria2.tellStatus",
                        json!([gid.to_string()]),
                    ),
                )
                .await
                .expect("query remains responsive")
                .expect("status");
                assert_eq!(status["status"], "waiting");
                {
                    let owner = shared.lock().await;
                    assert!(
                        !owner
                            .engine
                            .scheduler()
                            .task(gid)
                            .expect("task")
                            .desired_paused
                    );
                    assert_eq!(
                        owner.tasks.get_gid(gid).expect("old catalog").sources()[0].uri(),
                        "http://example.test/file.bin"
                    );
                    assert!(
                        matches!(owner.session.execute(SessionCommand::ReadTaskSources { gid }).expect("sources"), SessionCommandResult::TaskSources(sources) if sources[0].persistence_safe_uri.as_deref() == Some("http://example.test/file.bin"))
                    );
                }
                assert!(matches!(
                    HttpControlPlane::call_shared(
                        &shared,
                        "aria2.changeOption",
                        json!([gid.to_string(), {"split": 3}])
                    )
                    .await,
                    Err(HttpControlError::Busy)
                ));
                if let Some(command) = user_control {
                    HttpControlPlane::call_shared(&shared, command, json!([gid.to_string()]))
                        .await
                        .expect("user control wins");
                }
                drain.add_permits(1);
                let result = tokio::time::timeout(Duration::from_secs(3), request)
                    .await
                    .expect("source request finishes")
                    .expect("request task");
                if user_control == Some("aria2.remove") {
                    assert!(matches!(
                        result,
                        Err(HttpControlError::InvalidParams(
                            "source replacement was cancelled before commit"
                        ))
                    ));
                } else {
                    assert_eq!(
                        result.expect("confirmed commit"),
                        if method == "aria2.changeUri" {
                            json!([1, 1])
                        } else {
                            json!(gid.to_string())
                        }
                    );
                }
                let owner = Arc::try_unwrap(shared).expect("sole owner").into_inner();
                let expected_uri = if user_control == Some("aria2.remove") {
                    "http://example.test/file.bin"
                } else {
                    "http://new.test/file.bin"
                };
                assert_eq!(
                    owner.tasks.get_gid(gid).expect("catalog").sources()[0].uri(),
                    expected_uri
                );
                assert!(owner.pending_source_replacements.is_empty());
                assert_eq!(
                    owner
                        .engine
                        .scheduler()
                        .task(gid)
                        .expect("task")
                        .desired_paused,
                    user_control == Some("aria2.pause")
                );
                drain.add_permits(1);
                owner.shutdown_async().await.expect("shutdown");
                let recovered = directory.control_plane();
                assert_eq!(
                    recovered
                        .tasks
                        .get_gid(gid)
                        .expect("recovered source set")
                        .sources()[0]
                        .uri(),
                    expected_uri
                );
                assert_eq!(
                    recovered
                        .engine
                        .scheduler()
                        .task(gid)
                        .expect("recovered task")
                        .desired_paused,
                    user_control == Some("aria2.pause")
                );
                recovered.shutdown().expect("recovered shutdown");
            }
        }
    }

    #[tokio::test]
    async fn live_only_rate_patch_changes_credit_without_restarting_and_recovers() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let rate = RateArbiter::new(
            ariax_runtime::RateDirection::Download,
            ariax_runtime::RateArbiterConfig::default(),
        )
        .expect("rate arbiter");
        plane
            .attach_global_download_rate(rate.clone())
            .expect("attach rate");
        let started = Arc::new(Notify::new());
        let drain = Arc::new(tokio::sync::Semaphore::new(1));
        plane
            .attach_worker(Arc::new(DelayedCancellationWorker {
                started: started.clone(),
                drain,
            }))
            .expect("worker");
        let gid: Gid = plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/file.bin"], {"max-download-limit": 4}]),
            )
            .expect("add")
            .as_str()
            .expect("gid")
            .parse()
            .expect("gid");
        plane.poll_once().expect("start worker");
        started.notified().await;
        poll_until(&mut plane, |plane| {
            plane
                .engine
                .scheduler()
                .task(gid)
                .expect("task")
                .pending_barrier
                .is_none()
        })
        .await;
        let path = ariax_runtime::RatePath {
            host: 1,
            task: 1,
            stream: 1,
        };
        let requested = NonZeroUsize::new(4).expect("quantum");
        let permit = rate
            .try_acquire(path, requested)
            .expect("old rate")
            .expect("old permit");
        assert_eq!(permit.reserved_bytes(), 4);
        drop(permit);
        let before = plane
            .call("aria2.getOption", json!([gid.to_string()]))
            .expect("options");
        assert!(
            plane
                .call(
                    "aria2.changeOption",
                    json!([gid.to_string(), {"max-download-limit": 1, "split": 0}])
                )
                .is_err()
        );
        assert_eq!(
            plane
                .call("aria2.getOption", json!([gid.to_string()]))
                .expect("options after rejection"),
            before
        );
        let permit = rate
            .try_acquire(path, requested)
            .expect("unchanged rate")
            .expect("permit");
        assert_eq!(permit.reserved_bytes(), 4);
        drop(permit);
        assert_eq!(
            plane
                .call(
                    "aria2.changeOption",
                    json!([gid.to_string(), {"max-download-limit": 1}])
                )
                .expect("live patch"),
            "OK"
        );
        let permit = rate
            .try_acquire(path, requested)
            .expect("new rate")
            .expect("permit");
        assert_eq!(permit.reserved_bytes(), 1);
        drop(permit);
        assert!(plane.pending_restart_patches.is_empty());
        assert_eq!(
            plane.engine.scheduler().task(gid).expect("task").generation,
            Generation::INITIAL
        );
        assert_eq!(
            plane
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .expect("status")["status"],
            "active"
        );
        let expected = plane
            .tasks
            .get_gid(gid)
            .expect("task")
            .persistence_options()
            .expect("options");
        plane.shutdown_async().await.expect("shutdown");
        let recovered = directory.control_plane();
        assert_eq!(
            recovered
                .tasks
                .get_gid(gid)
                .expect("recovered task")
                .persistence_options()
                .expect("options"),
            expected
        );
        recovered.shutdown().expect("shutdown recovery");
        let payloads = replay_journal_payloads(
            &directory,
            TaskId::new(1).expect("task"),
            gid,
            Generation::INITIAL,
        );
        assert!(!payloads.iter().any(|payload| matches!(
            payload,
            JournalPayload::GenerationStarted { .. }
                | JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::NextAdmission,
                    ..
                }
        )));
    }

    #[tokio::test]
    async fn option_patch_recovery_repairs_each_durable_prefix_without_reappending_staging() {
        for boundary in 0..5 {
            let directory = TestDirectory::new();
            let mut plane = directory.control_plane();
            let gid = add_paused(&mut plane);
            let before = option_mirror(&plane, gid, OptionsSnapshotScope::CurrentGeneration);
            let mut entries = before
                .entries()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect::<BTreeMap<_, _>>();
            entries.insert("split".to_owned(), "3".to_owned());
            let next = SanitizedOptionMap::new(entries).expect("next options");
            let patch_id = OptionPatchId::new(u64::MAX - 100).expect("recovered high patch id");
            if boundary >= 1 {
                append_and_flush(
                    &plane,
                    gid,
                    Generation::INITIAL,
                    JournalPayload::OptionsSnapshot {
                        scope: OptionsSnapshotScope::NextAdmission,
                        patch_id: Some(patch_id),
                        snapshot_hash: next.snapshot_hash(),
                        options: next.clone(),
                    },
                );
            }
            if boundary >= 2 {
                plane
                    .replace_option_mirror(gid, OptionsSnapshotScope::NextAdmission, next.clone())
                    .expect("stage mirror");
            }
            if boundary >= 3 {
                append_and_flush(
                    &plane,
                    gid,
                    Generation::new(1),
                    JournalPayload::GenerationStarted {
                        previous_generation: Generation::INITIAL,
                        reason: GenerationStartReason::OptionPatch,
                        next_snapshot_hash: next.snapshot_hash(),
                        patch_id: Some(patch_id),
                    },
                );
            }
            if boundary >= 4 {
                plane
                    .session
                    .execute(SessionCommand::PromoteTaskOptions {
                        gid,
                        options: next.clone(),
                    })
                    .expect("promote mirror");
            }
            plane.shutdown().expect("close prefix");

            let mut recovered = directory.control_plane();
            let expected = if boundary == 0 { &before } else { &next };
            assert_eq!(
                &recovered
                    .tasks
                    .get_gid(gid)
                    .expect("recovered task")
                    .persistence_options()
                    .expect("options"),
                expected
            );
            let task = recovered
                .engine
                .scheduler()
                .task(gid)
                .expect("recovered scheduler task");
            assert!(task.desired_paused);
            assert_eq!(
                recovered
                    .call("aria2.tellStatus", json!([gid.to_string()]))
                    .expect("status")["status"],
                "paused"
            );
            if boundary > 0 {
                assert!(recovered.next_option_patch_id > patch_id.get());
            }
            if (1..3).contains(&boundary) {
                assert_eq!(
                    option_mirror(&recovered, gid, OptionsSnapshotScope::CurrentGeneration),
                    before
                );
                assert_eq!(
                    option_mirror(&recovered, gid, OptionsSnapshotScope::NextAdmission),
                    next
                );
                let drain = Arc::new(tokio::sync::Semaphore::new(1));
                recovered
                    .attach_worker(Arc::new(DelayedCancellationWorker {
                        started: Arc::new(Notify::new()),
                        drain,
                    }))
                    .expect("worker");
                recovered
                    .call("aria2.unpause", json!([gid.to_string()]))
                    .expect("admit staged patch");
                assert!(recovered.pending_restart_patches.is_empty());
                assert_eq!(
                    option_mirror(&recovered, gid, OptionsSnapshotScope::CurrentGeneration),
                    next
                );
                assert_eq!(
                    option_mirror(&recovered, gid, OptionsSnapshotScope::NextAdmission)
                        .entries()
                        .len(),
                    0
                );
            } else {
                assert_eq!(
                    &option_mirror(&recovered, gid, OptionsSnapshotScope::CurrentGeneration),
                    expected
                );
                assert_eq!(
                    option_mirror(&recovered, gid, OptionsSnapshotScope::NextAdmission)
                        .entries()
                        .len(),
                    0
                );
            }
            recovered.shutdown_async().await.expect("shutdown recovery");
            let payloads = replay_journal_payloads(
                &directory,
                TaskId::new(1).expect("task id"),
                gid,
                Generation::new(u64::from(boundary != 0)),
            );
            assert_eq!(
                payloads
                    .iter()
                    .filter(|payload| matches!(
                        payload,
                        JournalPayload::OptionsSnapshot {
                            patch_id: Some(_),
                            ..
                        }
                    ))
                    .count(),
                usize::from(boundary != 0)
            );
            assert_eq!(
                payloads
                    .iter()
                    .filter(|payload| matches!(
                        payload,
                        JournalPayload::GenerationStarted {
                            patch_id: Some(_),
                            ..
                        }
                    ))
                    .count(),
                usize::from(boundary != 0)
            );
        }
    }

    #[tokio::test]
    async fn active_option_patch_survives_delayed_drain_and_promotes_exactly_once() {
        for patch in [
            json!({"split": 3}),
            json!({"out": "renamed.bin"}),
            json!({"split": 3, "max-download-limit": "1M"}),
        ] {
            let directory = TestDirectory::new();
            let mut plane = directory.control_plane();
            plane
                .attach_global_download_rate(
                    RateArbiter::new(
                        ariax_runtime::RateDirection::Download,
                        ariax_runtime::RateArbiterConfig::default(),
                    )
                    .expect("rate arbiter"),
                )
                .expect("attach rate");
            let started = Arc::new(Notify::new());
            let drain = Arc::new(tokio::sync::Semaphore::new(0));
            plane
                .attach_worker(Arc::new(DelayedCancellationWorker {
                    started: started.clone(),
                    drain: drain.clone(),
                }))
                .expect("worker");
            let gid: Gid = plane
                .call("aria2.addUri", json!([["http://example.test/file.bin"]]))
                .expect("add")
                .as_str()
                .expect("gid")
                .parse()
                .expect("gid");
            plane.poll_once().expect("start worker");
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .expect("worker started");
            let old = option_mirror(&plane, gid, OptionsSnapshotScope::CurrentGeneration);
            assert_eq!(
                plane
                    .call("aria2.changeOption", json!([gid.to_string(), patch]))
                    .expect("accept patch"),
                "OK"
            );
            let patch_id = plane.pending_restart_patches[&gid];
            let expected = plane
                .tasks
                .get_gid(gid)
                .expect("task")
                .persistence_options()
                .expect("options");
            assert_ne!(old, expected);
            assert_eq!(
                option_mirror(&plane, gid, OptionsSnapshotScope::CurrentGeneration),
                old
            );
            assert_eq!(
                option_mirror(&plane, gid, OptionsSnapshotScope::NextAdmission),
                expected
            );
            assert_eq!(
                plane
                    .call("aria2.tellStatus", json!([gid.to_string()]))
                    .expect("waiting status")["status"],
                "waiting"
            );
            assert!(matches!(
                plane.call("aria2.changeOption", json!([gid.to_string(), {"split": 7}])),
                Err(HttpControlError::Busy)
            ));
            assert!(matches!(
                plane.call(
                    "ariax.replaceSources",
                    json!([gid.to_string(), ["http://other.test/file.bin"]])
                ),
                Err(HttpControlError::Busy)
            ));
            for _ in 0..3 {
                plane.poll_once().expect("delayed drain");
                tokio::task::yield_now().await;
            }
            assert_eq!(
                plane.engine.scheduler().task(gid).expect("task").generation,
                Generation::INITIAL
            );
            assert_eq!(plane.pending_restart_patches[&gid], patch_id);
            drain.add_permits(1);
            poll_until(&mut plane, |plane| {
                !plane.pending_restart_patches.contains_key(&gid)
            })
            .await;
            assert_eq!(
                plane
                    .engine
                    .scheduler()
                    .task(gid)
                    .expect("promoted task")
                    .generation,
                Generation::new(1)
            );
            assert_eq!(
                option_mirror(&plane, gid, OptionsSnapshotScope::CurrentGeneration),
                expected
            );
            assert_eq!(
                option_mirror(&plane, gid, OptionsSnapshotScope::NextAdmission)
                    .entries()
                    .len(),
                0
            );
            drain.add_permits(1);
            plane
                .shutdown_async()
                .await
                .expect("shutdown promoted task");
            let recovered = directory.control_plane();
            assert_eq!(
                recovered
                    .tasks
                    .get_gid(gid)
                    .expect("recovered task")
                    .persistence_options()
                    .expect("recovered options"),
                expected
            );
            assert!(recovered.next_option_patch_id > patch_id.get());
            recovered.shutdown().expect("shutdown recovered task");
            let payloads = replay_journal_payloads(
                &directory,
                TaskId::new(1).expect("task id"),
                gid,
                Generation::new(1),
            );
            assert_eq!(payloads.iter().filter(|payload| matches!(payload, JournalPayload::OptionsSnapshot { patch_id: Some(id), .. } if *id == patch_id)).count(), 1);
            assert_eq!(payloads.iter().filter(|payload| matches!(payload, JournalPayload::GenerationStarted { patch_id: Some(id), .. } if *id == patch_id)).count(), 1);
        }
    }

    #[tokio::test]
    async fn consecutive_option_patches_use_current_generation_acknowledgements() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let started = Arc::new(Notify::new());
        let drain = Arc::new(tokio::sync::Semaphore::new(0));
        plane
            .attach_worker(Arc::new(DelayedCancellationWorker {
                started: started.clone(),
                drain: drain.clone(),
            }))
            .expect("worker");
        let gid: Gid = plane
            .call("aria2.addUri", json!([["http://example.test/file.bin"]]))
            .expect("add")
            .as_str()
            .expect("gid")
            .parse()
            .expect("gid");
        let mut patches = Vec::new();
        for generation in 0..2 {
            plane.poll_once().expect("start worker");
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .expect("worker started");
            poll_until(&mut plane, |plane| {
                plane
                    .engine
                    .scheduler()
                    .task(gid)
                    .expect("task")
                    .pending_barrier
                    .is_none()
            })
            .await;
            assert_eq!(
                plane
                    .call(
                        "aria2.changeOption",
                        json!([gid.to_string(), {"split": generation + 3}])
                    )
                    .expect("patch"),
                "OK"
            );
            patches.push(plane.pending_restart_patches[&gid]);
            drain.add_permits(1);
            poll_until(&mut plane, |plane| {
                !plane.pending_restart_patches.contains_key(&gid)
            })
            .await;
            assert_eq!(
                plane.engine.scheduler().task(gid).expect("task").generation,
                Generation::new(generation + 1)
            );
        }
        assert!(patches[1] > patches[0]);
        drain.add_permits(1);
        plane.shutdown_async().await.expect("shutdown");
        let recovered = directory.control_plane();
        assert_eq!(
            recovered
                .tasks
                .get_gid(gid)
                .expect("recovered")
                .options()
                .split
                .get(),
            4
        );
        assert!(recovered.next_option_patch_id > patches[1].get());
        recovered.shutdown().expect("recovered shutdown");
    }

    #[tokio::test]
    async fn rejected_patch_staging_retains_the_old_catalog_and_mirrors() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let started = Arc::new(Notify::new());
        let drain = Arc::new(tokio::sync::Semaphore::new(1));
        plane
            .attach_worker(Arc::new(DelayedCancellationWorker {
                started: started.clone(),
                drain,
            }))
            .expect("worker");
        let gid: Gid = plane
            .call("aria2.addUri", json!([["http://example.test/file.bin"]]))
            .expect("add")
            .as_str()
            .expect("gid")
            .parse()
            .expect("gid");
        plane.poll_once().expect("start worker");
        started.notified().await;
        poll_until(&mut plane, |plane| {
            plane
                .engine
                .scheduler()
                .task(gid)
                .expect("task")
                .pending_barrier
                .is_none()
        })
        .await;
        let previous = plane
            .tasks
            .get_gid(gid)
            .expect("task")
            .persistence_options()
            .expect("options");
        plane
            .session
            .execute(SessionCommand::CloseJournal { gid })
            .expect("inject missing journal");
        assert!(matches!(
            plane.call("aria2.changeOption", json!([gid.to_string(), {"split": 3}])),
            Err(HttpControlError::Persistence(_))
        ));
        assert_eq!(
            plane
                .tasks
                .get_gid(gid)
                .expect("task")
                .persistence_options()
                .expect("options"),
            previous
        );
        assert_eq!(
            option_mirror(&plane, gid, OptionsSnapshotScope::CurrentGeneration),
            previous
        );
        assert_eq!(
            option_mirror(&plane, gid, OptionsSnapshotScope::NextAdmission)
                .entries()
                .len(),
            0
        );
        assert!(plane.pending_restart_patches.is_empty());
        plane
            .call("aria2.getGlobalStat", json!([]))
            .expect("control remains usable");
        plane.shutdown_async().await.expect("shutdown");
    }

    impl HttpTaskWorker for UncooperativeShutdownWorker {
        fn start(
            &self,
            _task: Arc<HttpTaskSpec>,
            _generation: Generation,
            _cancellation: crate::HttpCancellation,
        ) -> crate::HttpWorkerFuture {
            let started = Arc::clone(&self.started);
            Box::pin(async move {
                started.notify_one();
                std::future::pending::<Result<crate::HttpWorkerSuccess, PublicError>>().await
            })
        }
    }

    #[test]
    fn paused_add_persists_task_sources_and_options_before_publication() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);

        let status = plane
            .call("aria2.tellStatus", json!([gid.to_string()]))
            .expect("tell status");
        assert_eq!(status["status"], "paused");
        assert_eq!(status["completedLength"], "0");
        assert_eq!(status["downloadSpeed"], "0");
        assert_eq!(status["verifiedLength"], "0");
        assert_eq!(status["retryCount"], "0");
        assert_eq!(status["discardBudgetConsumed"], "0");
        assert_eq!(status["discardBudgetRemaining"], "0");

        let global = plane
            .call("aria2.getGlobalStat", json!([]))
            .expect("global stat");
        assert_eq!(global["numWaiting"], "1");
        assert_eq!(global["numActive"], "0");

        let session = plane.session_handle();
        let tasks = match session
            .execute(SessionCommand::ReadTasks)
            .expect("read tasks")
        {
            SessionCommandResult::Tasks(tasks) => tasks,
            result => panic!("unexpected task result: {result:?}"),
        };
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].gid, gid);
        assert_eq!(tasks[0].queue_state, SessionQueueState::Paused);
        let sources = match session
            .execute(SessionCommand::ReadTaskSources { gid })
            .expect("read sources")
        {
            SessionCommandResult::TaskSources(sources) => sources,
            result => panic!("unexpected source result: {result:?}"),
        };
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].persistence_safe_uri.as_deref(),
            Some("http://example.test/file.bin")
        );
        let options = match session
            .execute(SessionCommand::ReadTaskOptions {
                gid,
                scope: OptionsSnapshotScope::CurrentGeneration,
            })
            .expect("read options")
        {
            SessionCommandResult::TaskOptions(options) => options,
            result => panic!("unexpected option result: {result:?}"),
        };
        assert!(
            options
                .entries()
                .any(|(name, value)| name == "out" && value == "file.bin")
        );
        assert!(
            options
                .entries()
                .any(|(name, value)| name == "connect-timeout" && value == "60")
        );
        drop(session);
        assert_eq!(plane.shutdown().expect("shutdown").journals_closed, 1);
    }

    #[test]
    fn remove_persists_terminal_result_and_recovered_catalog_keeps_sources() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        assert_eq!(plane.shutdown().expect("first shutdown").journals_closed, 1);

        let mut recovered = directory.control_plane();
        let task = recovered
            .task_catalog()
            .get(TaskId::new(1).expect("task id"))
            .expect("recovered HTTP task");
        assert_eq!(task.gid(), gid);
        assert_eq!(task.sources()[0].uri(), "http://example.test/file.bin");
        assert_eq!(task.output().canonical_string(), "file.bin");

        let removed = recovered
            .call("aria2.remove", json!([gid.to_string()]))
            .expect("remove recovered task");
        assert_eq!(removed, gid.to_string());
        let status = recovered
            .call("aria2.tellStatus", json!([gid.to_string()]))
            .expect("removed status");
        assert_eq!(status["status"], "removed");

        let session = recovered.session_handle();
        let stopped = match session
            .execute(SessionCommand::ReadStoppedResults)
            .expect("read stopped results")
        {
            SessionCommandResult::StoppedResults(results) => results,
            result => panic!("unexpected stopped-result response: {result:?}"),
        };
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].gid, gid);
        assert_eq!(stopped[0].status, SessionTerminalStatus::Removed);
        drop(session);
        assert_eq!(recovered.shutdown().expect("shutdown").journals_closed, 1);
    }

    #[test]
    fn recovered_restarting_marker_stages_the_snapshot_before_promotion() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let task_id = TaskId::new(1).expect("task id");
        let options = plane
            .task_catalog()
            .get(task_id)
            .expect("HTTP task")
            .persistence_options()
            .expect("persistence options");
        let snapshot_hash = options.snapshot_hash();
        assert_eq!(plane.shutdown().expect("first shutdown").journals_closed, 1);
        assert_eq!(
            append_restarting_prefix(&directory, task_id, gid, None),
            None
        );

        let recovered = directory.control_plane();
        let next = Generation::new(1);
        let plan = recovered
            .plan_for_effect(
                &TransitionEffect::PersistGenerationStarted {
                    task_id,
                    gid,
                    generation: next,
                },
                None,
            )
            .expect("recover marker-only representation restart");
        assert_eq!(
            plan.steps(),
            [
                PersistencePlanStep::AppendAndFlushJournal {
                    gid,
                    generation: Generation::INITIAL,
                    payload: JournalPayload::OptionsSnapshot {
                        scope: OptionsSnapshotScope::NextAdmission,
                        patch_id: None,
                        snapshot_hash,
                        options,
                    },
                },
                PersistencePlanStep::AppendAndFlushJournal {
                    gid,
                    generation: next,
                    payload: JournalPayload::GenerationStarted {
                        previous_generation: Generation::INITIAL,
                        reason: GenerationStartReason::RepresentationRestart,
                        next_snapshot_hash: snapshot_hash,
                        patch_id: None,
                    },
                },
            ]
        );
        assert_eq!(recovered.shutdown().expect("shutdown").journals_closed, 1);
    }

    #[test]
    fn recovered_restarting_prefix_promotes_the_exact_staged_snapshot() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let task_id = TaskId::new(1).expect("task id");
        let options = plane
            .task_catalog()
            .get(task_id)
            .expect("HTTP task")
            .persistence_options()
            .expect("persistence options");
        assert_eq!(plane.shutdown().expect("first shutdown").journals_closed, 1);
        let snapshot_hash = append_restarting_prefix(&directory, task_id, gid, Some(options))
            .expect("staged snapshot hash");

        let recovered = directory.control_plane();
        let journal = recovered
            .engine
            .recovered_tasks()
            .iter()
            .find(|task| task.gid == gid)
            .expect("recovered journal");
        assert_eq!(journal.journal.generation(), Generation::INITIAL);
        assert_eq!(journal.journal.paused(), Some(TaskPauseReason::Restarting));
        assert_eq!(
            journal
                .journal
                .pending_options()
                .expect("pending snapshot")
                .snapshot_hash(),
            snapshot_hash
        );

        let next = Generation::new(1);
        let plan = recovered
            .plan_for_effect(
                &TransitionEffect::PersistGenerationStarted {
                    task_id,
                    gid,
                    generation: next,
                },
                None,
            )
            .expect("recover representation restart plan");
        assert_eq!(
            plan.steps(),
            [PersistencePlanStep::AppendAndFlushJournal {
                gid,
                generation: next,
                payload: JournalPayload::GenerationStarted {
                    previous_generation: Generation::INITIAL,
                    reason: GenerationStartReason::RepresentationRestart,
                    next_snapshot_hash: snapshot_hash,
                    patch_id: None,
                },
            }]
        );
        assert_eq!(recovered.shutdown().expect("shutdown").journals_closed, 1);
    }

    #[test]
    fn recovered_restarting_prefix_rejects_a_different_staged_snapshot() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let task_id = TaskId::new(1).expect("task id");
        assert_eq!(plane.shutdown().expect("first shutdown").journals_closed, 1);
        append_restarting_prefix(
            &directory,
            task_id,
            gid,
            Some(
                SanitizedOptionMap::new([("out".to_owned(), "different.bin".to_owned())])
                    .expect("different staged options"),
            ),
        );

        let recovered = directory.control_plane();
        assert!(matches!(
            recovered.plan_for_effect(
                &TransitionEffect::PersistGenerationStarted {
                    task_id,
                    gid,
                    generation: Generation::new(1),
                },
                None,
            ),
            Err(HttpControlError::Persistence(message))
                if message == "recovered representation restart snapshot does not match the task"
        ));
        assert_eq!(recovered.shutdown().expect("shutdown").journals_closed, 1);
    }

    #[test]
    fn rejected_output_paths_and_options_leave_no_task_metadata() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let outside = directory.root.join("outside");
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"dir": outside}]),
            ),
            Err(HttpControlError::InvalidParams(
                "dir must equal the configured output root"
            ))
        ));
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"out": "../escape"}]),
            ),
            Err(HttpControlError::InvalidParams(
                "out is not a safe relative path"
            ))
        ));
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"unknown": true}]),
            ),
            Err(HttpControlError::InvalidParams("unsupported addUri option"))
        ));
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"piece-length": "3M"}]),
            ),
            Err(HttpControlError::TaskSpec(
                HttpTaskSpecError::InvalidOptions
            ))
        ));
        assert!(matches!(
            plane.call("aria2.addUri", json!([])),
            Err(HttpControlError::InvalidParams(
                "addUri accepts a URI array and optional options object"
            ))
        ));
        assert!(matches!(
            plane.call("aria2.addUri", json!([["http://example.test/file"], {}, 0]),),
            Err(HttpControlError::InvalidParams(
                "addUri accepts a URI array and optional options object"
            ))
        ));
        assert!(matches!(
            plane.call("aria2.pause", json!(["0000000000000001", false])),
            Err(HttpControlError::InvalidParams(
                "exactly one hexadecimal GID is required"
            ))
        ));
        let tasks = match plane
            .session_handle()
            .execute(SessionCommand::ReadTasks)
            .expect("read tasks")
        {
            SessionCommandResult::Tasks(tasks) => tasks,
            result => panic!("unexpected task response: {result:?}"),
        };
        assert!(tasks.is_empty());
        assert!(
            fs::read_dir(&directory.journals)
                .expect("read journals")
                .next()
                .is_none()
        );
        assert_eq!(plane.shutdown().expect("shutdown").journals_closed, 0);
    }

    #[test]
    fn add_option_parser_accepts_aria2_sizes_and_rejects_bounds() {
        let directory = TestDirectory::new();
        let uris = vec!["http://example.test/file".to_owned()];
        let (options, root, output, paused) = parse_add_options(
            &json!({
                "piece-length": "1M",
                "min-split-size": "1M",
                "split": "4",
                "max-connection-per-server": 2,
                "connect-timeout": "1",
                "timeout": 600,
                "max-download-limit": "64K",
                "retry-profile": "custom",
                "retry-on": "timeout,lowest-speed",
                "retry-on-http-status": "418,429",
                "retry-on-http-status-add": "500-501",
                "retry-on-http-status-remove": "429",
                "max-tries": 5,
                "retry-max-attempts": 4,
                "retry-max-attempts-per-mirror": 2,
                "retry-wait": 0,
                "retry-backoff": "fixed",
                "retry-after": "ignore",
                "retry-after-min": 0,
                "retry-after-max": 60,
                "retry-max-wait": 60,
                "retry-max-elapsed": 600,
                "stale-validator-policy": "revalidate",
                "endgame-max-duplicates": 8,
                "checksum": "sha-256=abababababababababababababababababababababababababababababababab",
                "pause": "true",
            }),
            &directory.output,
            &uris,
        )
        .expect("valid bounded options");
        assert_eq!(options.piece_length, 1024 * 1024);
        assert_eq!(options.min_split_size, 1024 * 1024);
        assert_eq!(options.split.get(), 4);
        assert_eq!(options.max_connections_per_server.get(), 2);
        assert_eq!(options.connect_timeout, Duration::from_secs(1));
        assert_eq!(options.response_body_timeout, Duration::from_secs(600));
        assert_eq!(options.max_download_limit, 64 * 1024);
        assert_eq!(options.endgame_max_duplicates, 8);
        assert_eq!(
            options.checksum,
            Some(HttpContentChecksum::sha256([0xab; 32]))
        );
        let retry = options.retry.expect("resolved retry policy");
        assert_eq!(retry.profile, HttpRetryProfile::Custom);
        assert_eq!(retry.max_attempts.get(), 4);
        assert_eq!(retry.max_attempts_per_mirror.get(), 2);
        assert_eq!(retry.retry_on.canonical(), "timeout,lowest-speed");
        assert_eq!(retry.retryable_statuses.canonical(), "418,500,501");
        assert_eq!(retry.backoff, HttpRetryBackoff::Fixed);
        assert!(!retry.respect_retry_after);
        assert_eq!(
            retry.stale_validator_policy,
            crate::HttpStaleValidatorPolicy::Revalidate
        );
        assert_eq!(root, directory.output);
        assert_eq!(output.canonical_string(), "file");
        assert!(paused);

        let (disabled, _, _, _) = parse_add_options(
            &json!({"endgame-max-duplicates": 0}),
            &directory.output,
            &uris,
        )
        .expect("zero disables endgame");
        assert_eq!(disabled.endgame_max_duplicates, 0);

        for invalid in [
            json!({"split": 1025}),
            json!({"max-connection-per-server": 0}),
            json!({"connect-timeout": 0}),
            json!({"timeout": 601}),
            json!({"retry-profile": "custom"}),
            json!({"retry-max-attempts": 0}),
            json!({"retry-max-wait": 0}),
            json!({"retry-on-http-status": "99"}),
            json!({"endgame-max-duplicates": 9}),
            json!({"endgame-max-duplicates": -1}),
            json!({"stale-validator-policy": "unsafe"}),
            json!({"stale-validator-policy": 7}),
            json!({"piece-length": "18446744073709551615T"}),
            json!({"checksum": "sha-512=abcd"}),
            json!({"checksum": 7}),
        ] {
            assert!(matches!(
                parse_add_options(&invalid, &directory.output, &uris),
                Err(HttpControlError::InvalidParams(_))
            ));
        }
    }

    #[test]
    fn retry_admission_recovers_canonical_options_with_production_policy() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let mut expected = Vec::new();
        for mut options in [
            json!({"retry-profile":"aria2"}),
            json!({"retry-profile":"conservative", "max-tries":4}),
            json!({"retry-profile":"aggressive"}),
            json!({
                "retry-profile":"custom", "retry-on":"timeout,lowest-speed",
                "retry-on-http-status":"418,429", "retry-on-http-status-add":"500-501",
                "retry-on-http-status-remove":"429", "max-tries":5, "retry-max-attempts":4,
                "retry-max-attempts-per-mirror":2, "retry-wait":0, "retry-backoff":"fixed",
                "retry-after":"ignore", "retry-after-min":0, "retry-after-max":60,
                "retry-max-wait":60, "retry-max-elapsed":600, "stale-validator-policy":"revalidate"
            }),
            json!({"retry-profile":"aria2", "retry-on-http-status-remove":"504"}),
        ] {
            options["pause"] = json!(true);
            let gid: Gid = plane
                .call(
                    "aria2.addUri",
                    json!([["http://example.test/retry.bin"], options]),
                )
                .expect("production admission")
                .as_str()
                .expect("GID")
                .parse()
                .expect("valid GID");
            let spec = plane.tasks.get_gid(gid).expect("task");
            let persisted = spec.persistence_options().expect("canonical options");
            for (name, value) in persisted.entries() {
                let definition = builtin_registry().find(name).expect("registered option");
                assert!(ariax_config::persisted_option_is_safe(name));
                parse_option_value(definition, value, None).expect("canonical registry value");
            }
            assert!(
                persisted
                    .entries()
                    .all(|(name, _)| !name.ends_with("-add") && !name.ends_with("-remove"))
            );
            expected.push((
                gid,
                spec.options().clone(),
                plane
                    .call("aria2.getOption", json!([gid.to_string()]))
                    .expect("options"),
            ));
        }
        plane.shutdown().expect("close first process");
        let mut recovered = directory.control_plane();
        for (gid, options, canonical) in expected {
            assert_eq!(
                recovered
                    .tasks
                    .get_gid(gid)
                    .expect("recovered task")
                    .options(),
                &options
            );
            assert_eq!(
                recovered
                    .call("aria2.getOption", json!([gid.to_string()]))
                    .expect("recovered options"),
                canonical
            );
        }
        add_paused(&mut recovered);
        recovered
            .call("aria2.getGlobalStat", json!([]))
            .expect("subsequent query");
        recovered.shutdown().expect("shutdown");
    }

    #[test]
    fn rejected_admission_has_no_artifacts_and_does_not_fault_the_scheduler() {
        let directory = TestDirectory::new();
        let mut plane = directory
            .control_plane_with_policy(HttpWorkerSupervisorConfig::default(), |name: &str| {
                ariax_config::persisted_option_is_safe(name) && name != "retry-profile"
            });
        for options in [
            json!({"max-tries":4}),
            json!({"retry-max-attempts":0}),
            json!({"rpc-secret":"secret-canary"}),
            json!({"unknown-option":"value"}),
            json!({"retry-on":"invalid"}),
        ] {
            assert!(matches!(
                plane.call(
                    "aria2.addUri",
                    json!([["http://example.test/retry.bin"], options])
                ),
                Err(HttpControlError::InvalidParams(_))
            ));
            assert_eq!(plane.tasks.len(), 0);
            assert!(
                fs::read_dir(&directory.journals)
                    .expect("journal directory")
                    .next()
                    .is_none()
            );
            assert!(
                matches!(plane.session.execute(SessionCommand::ReadTasks).expect("session remains usable"), SessionCommandResult::Tasks(tasks) if tasks.is_empty())
            );
            plane
                .call("aria2.getGlobalStat", json!([]))
                .expect("query after rejection");
        }
        add_paused(&mut plane);
        assert_eq!(plane.tasks.len(), 1);
        plane.shutdown().expect("shutdown after valid admission");
    }

    #[tokio::test]
    async fn live_shutdown_timeout_aborts_worker_and_persists_dirty_checkpoint() {
        let directory = TestDirectory::new();
        let started = Arc::new(Notify::new());
        let supervisor = HttpWorkerSupervisorConfig {
            shutdown_timeout: Duration::from_millis(10),
            ..HttpWorkerSupervisorConfig::default()
        };
        let mut plane = directory.control_plane_with_supervisor(supervisor);
        plane
            .attach_worker(Arc::new(UncooperativeShutdownWorker {
                started: Arc::clone(&started),
            }))
            .expect("attach uncooperative worker");
        plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/hung.bin"], {"pause": false}]),
            )
            .expect("add live task");
        plane.poll_once().expect("start live worker");
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("worker started");

        let report = plane.shutdown_async().await.expect("bounded shutdown");
        assert!(!report.is_clean());
        assert!(report.shutdown().step_timed_out(ShutdownStep::DrainDiskCpu));
        assert_eq!(report.journals_flushed, 1);
        assert_eq!(report.journals_closed, 1);

        let store = SessionStore::open(
            directory.root.join("session.db"),
            SessionStoreConfig::default(),
        )
        .expect("reopen dirty session");
        assert!(
            !store
                .session()
                .expect("session")
                .expect("record")
                .clean_shutdown
        );
    }

    #[test]
    fn synchronous_shutdown_with_an_idle_supervisor_remains_clean() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        attach_loopback_worker(&mut plane, &directory);

        let report = plane.shutdown().expect("shutdown idle supervisor");
        assert!(report.is_clean());
        assert_eq!(report.journals_flushed, 0);
        assert_eq!(report.journals_closed, 0);
    }

    #[tokio::test]
    async fn synchronous_shutdown_with_an_active_worker_persists_dirty_checkpoint() {
        let directory = TestDirectory::new();
        let started = Arc::new(Notify::new());
        let mut plane = directory.control_plane();
        plane
            .attach_worker(Arc::new(UncooperativeShutdownWorker {
                started: Arc::clone(&started),
            }))
            .expect("attach uncooperative worker");
        plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/sync-hung.bin"], {"pause": false}]),
            )
            .expect("add live task");
        plane.poll_once().expect("start live worker");
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("worker started");

        let report = plane.shutdown().expect("synchronous shutdown");
        assert!(!report.is_clean());
        assert!(report.shutdown().step_failed(ShutdownStep::DrainDiskCpu));
        assert!(!report.shutdown().step_timed_out(ShutdownStep::DrainDiskCpu));
        let store = SessionStore::open(
            directory.root.join("session.db"),
            SessionStoreConfig::default(),
        )
        .expect("reopen dirty session");
        assert!(
            !store
                .session()
                .expect("session")
                .expect("record")
                .clean_shutdown
        );
    }

    #[tokio::test]
    async fn live_supervisor_completes_http_task_and_persists_terminal_evidence() {
        let directory = TestDirectory::new();
        let data: Arc<[u8]> = vec![0x5a; 1024 * 1024].into();
        let (uri, server) = serve_control_file(Arc::clone(&data)).await;
        let mut plane = directory.control_plane();
        attach_loopback_worker(&mut plane, &directory);
        let checksum =
            HttpContentChecksum::sha256(Sha256::digest(data.as_ref()).into()).canonical();
        let gid = plane
            .call(
                "aria2.addUri",
                json!([[uri], {"pause": false, "checksum": checksum}]),
            )
            .expect("add URI")
            .as_str()
            .expect("GID")
            .parse::<Gid>()
            .expect("valid GID");

        let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
        loop {
            plane.poll_once().expect("control progress");
            let status = plane
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .expect("status");
            if status["status"] == "complete" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "live worker did not reach complete status"
            );
            tokio::task::yield_now().await;
        }
        server.await.expect("server");
        assert_eq!(
            fs::read(directory.output.join("file.bin")).expect("output"),
            data.as_ref()
        );
        let stopped = match plane
            .session_handle()
            .execute(SessionCommand::ReadStoppedResults)
            .expect("stopped results")
        {
            SessionCommandResult::StoppedResults(results) => results,
            result => panic!("unexpected stopped response: {result:?}"),
        };
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].gid, gid);
        assert_eq!(stopped[0].status, SessionTerminalStatus::Complete);
        assert_eq!(stopped[0].total_length, Some(data.len() as u64));
        let report = plane.shutdown_async().await.expect("shutdown");
        assert!(report.is_clean());
        assert_eq!(report.journals_closed, 1);
    }

    #[tokio::test]
    async fn live_stale_validator_restart_persists_and_completes_the_next_generation() {
        let directory = TestDirectory::new();
        let data: Arc<[u8]> = vec![0x73; 1024 * 1024].into();
        let (uri, server) = serve_control_restart_file(Arc::clone(&data)).await;
        let mut plane = directory.control_plane();
        attach_loopback_worker(&mut plane, &directory);
        let gid = plane
            .call(
                "aria2.addUri",
                json!([[uri], {
                    "pause": false,
                    "split": 1,
                    "retry-max-attempts": 2,
                    "retry-max-attempts-per-mirror": 2,
                    "stale-validator-policy": "restart-if-safe"
                }]),
            )
            .expect("add restarting URI")
            .as_str()
            .expect("GID")
            .parse::<Gid>()
            .expect("valid GID");

        let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
        loop {
            plane.poll_once().expect("restart control progress");
            let status = plane
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .expect("status");
            if status["status"] == "complete" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "restarted worker did not reach complete status"
            );
            tokio::task::yield_now().await;
        }
        server.await.expect("server");
        assert_eq!(
            fs::read(directory.output.join("restart.bin")).expect("output"),
            data.as_ref()
        );
        let stopped = match plane
            .session_handle()
            .execute(SessionCommand::ReadStoppedResults)
            .expect("stopped results")
        {
            SessionCommandResult::StoppedResults(results) => results,
            result => panic!("unexpected stopped response: {result:?}"),
        };
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].gid, gid);
        assert_eq!(stopped[0].status, SessionTerminalStatus::Complete);
        assert_eq!(stopped[0].total_length, Some(data.len() as u64));
        let report = plane.shutdown_async().await.expect("shutdown");
        assert!(report.is_clean());
        assert_eq!(report.journals_closed, 1);
        let payloads = replay_journal_payloads(
            &directory,
            TaskId::new(1).expect("task id"),
            gid,
            Generation::new(1),
        );
        assert!(payloads.windows(3).any(|window| matches!(
            window,
            [
                JournalPayload::TaskPaused {
                    reason: TaskPauseReason::Restarting,
                },
                JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::NextAdmission,
                    patch_id: None,
                    ..
                },
                JournalPayload::GenerationStarted {
                    previous_generation: Generation::INITIAL,
                    reason: GenerationStartReason::RepresentationRestart,
                    patch_id: None,
                    ..
                },
            ]
        )));
    }
}
