//! The bounded Phase-3 HTTP control plane.
//!
//! The control plane owns the only mutable bridge between RPC requests,
//! scheduler effects, persisted task metadata, and the worker catalog.  RPC
//! transports never mutate the scheduler directly.

mod admission;
#[cfg(feature = "bt")]
mod bittorrent;
mod configuration;
mod control_io;
mod control_ops;
pub(crate) mod control_runtime;
mod metadata_follow;
#[cfg(feature = "metalink")]
mod metalink_admission;
pub(crate) mod query;
mod scheduling;

pub(crate) fn validate_bittorrent_options(value: &Value) -> Result<(), HttpControlError> {
    #[cfg(feature = "bt")]
    {
        bittorrent::Options::parse(value).map(|_| ())
    }
    #[cfg(not(feature = "bt"))]
    {
        let _ = value;
        Err(HttpControlError::Unsupported(
            "BitTorrent feature unavailable",
        ))
    }
}

pub use control_runtime::ControlRuntimeMetrics;

use crate::http_first_slice::append_initial_admission_with_options;
use crate::rpc_result::{
    DisplayValue, OptionMap, PersistedSources, PersistedUris, RESULT_VALUE_BYTES, ResultList,
    SessionOptions, SourceServers, SourceUris,
};
use crate::{
    HttpRetryAfterPolicy, HttpRetryBackoff, HttpRetryPolicy, HttpRetryProfile, HttpRetryStatusSet,
    HttpRetryTriggerSet, HttpRpcBackend, HttpRpcBackendError, HttpTaskCatalogError,
    HttpTaskOptions, HttpTaskSpec, HttpTaskSpecError, HttpTaskWorker, HttpTransferStatsSnapshot,
    HttpWorkerSupervisor, HttpWorkerSupervisorConfig, HttpWorkerSupervisorShutdown,
    MAX_HTTP_ENDGAME_MAX_DUPLICATES, PersistenceEffectPlan, PersistencePlanStep,
    ProcessDrainOutcome, RpcEvent, RpcEventBroker, RpcEventClass, RpcEventError, RpcEventKey,
    RpcEventLimits, RpcEventSubscriber, SharedHttpTaskCatalog, SharedHttpTransferStats,
    derive_http_journal_id, http_journal_directory,
};
use ariax_config::{
    CompatStatus, FlatConfigLimits, OptionValue, RuntimeUpdate, Scope, SecurityClass,
    UnknownOptionMode, builtin_registry, parse_flat_config, parse_option_value,
};
use ariax_core::{
    Aria2Status, Generation, Gid, MonotonicInstant, OptionPatchId, OptionPatchRejectReason,
    PendingBarrier, PublicError, QueueClass, QueueOrder, RequestScheduler, RetryClass,
    SchedulerCommand, TaskConditions, TaskEvent, TaskEventEnvelope, TaskId, TaskSnapshot,
    TransitionEffect, ValidatedOptionPatchKind,
};
use ariax_runtime::{RateArbiter, RateLimit, RateScope};
use ariax_storage::{
    ControlJournalAppender, GenerationStartReason, JournalPayload, OptionsSnapshotScope,
    PathPlatform, PlatformPath, SafePathBuilder, SanitizedOptionMap, SessionCommand,
    SessionCommandResult, SessionHandle, SessionId, SessionQueueOrder, SessionQueueState,
    SessionSlowSlotState, SessionStoppedResultRecord, SessionTaskMetadata, SessionTaskRecord,
    SessionTerminalStatus, TaskPauseReason, TaskRemoveReason,
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

/// Local-only destination for explicit, periodic, and shutdown session exports.
#[derive(Clone, Debug)]
pub struct SessionExportConfig {
    pub path: PathBuf,
    pub format: crate::SessionFormat,
    pub interval: Option<Duration>,
}

struct ConfiguredSessionExport {
    destination: ariax_storage::SessionExportDestination,
    format: crate::SessionFormat,
    interval: Option<Duration>,
    next_save: Option<Instant>,
}

struct PendingSessionExport {
    completion: oneshot::Receiver<Result<(), HttpControlError>>,
    reply: Option<oneshot::Sender<Result<Value, HttpControlError>>>,
    _thread: std::thread::JoinHandle<()>,
    _request: crate::rpc_budget::RpcRequestLease,
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

#[derive(Clone, Debug)]
pub enum HttpControlError {
    InvalidConfig,
    InvalidParams(&'static str),
    OptionPatchRejected(Vec<OptionPatchRejection>),
    TaskSpec(HttpTaskSpecError),
    Catalog(HttpTaskCatalogError),
    Persistence(String),
    Scheduler(String),
    Journal(String),
    NotFound,
    Unsupported(&'static str),
    Busy,
    SlowConsumer,
    ResponseTooLarge,
}

/// One value-free rejection in an atomic runtime option patch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionPatchRejection {
    pub name: String,
    pub reason: OptionPatchRejectReason,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlDiagnostics {
    pub profile: Option<String>,
    pub event_backend: Option<String>,
    pub disk_backend: Option<String>,
    pub buffer_budget_bytes: Option<usize>,
    pub task_count: usize,
    pub active_workers: usize,
    pub config_generation: u64,
    pub event_subscribers: usize,
    pub rpc_items: usize,
    pub rpc_item_limit: usize,
    pub rpc_bytes: usize,
    pub rpc_byte_limit: usize,
    pub resident_bytes: usize,
    pub resident_limit: usize,
}

impl fmt::Display for HttpControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid HTTP control configuration"),
            Self::InvalidParams(message) => formatter.write_str(message),
            Self::OptionPatchRejected(_) => formatter.write_str("OptionPatchRejected"),
            Self::TaskSpec(error) => error.fmt(formatter),
            Self::Catalog(error) => write!(formatter, "HTTP task catalog error: {error:?}"),
            Self::Persistence(error) => write!(formatter, "persistence failed: {error}"),
            Self::Scheduler(error) => write!(formatter, "scheduler rejected command: {error}"),
            Self::Journal(error) => write!(formatter, "journal failed: {error}"),
            Self::NotFound => formatter.write_str("task was not found"),
            Self::Unsupported(message) => formatter.write_str(message),
            Self::Busy => formatter.write_str("control plane is busy"),
            Self::SlowConsumer => formatter.write_str("RPC event subscriber is too slow"),
            Self::ResponseTooLarge => {
                formatter.write_str("RPC response exceeds its materialization budget")
            }
        }
    }
}

impl Error for HttpControlError {}

impl From<crate::rpc_result::ResultTooLarge> for HttpControlError {
    fn from(_: crate::rpc_result::ResultTooLarge) -> Self {
        Self::ResponseTooLarge
    }
}

struct PendingOptionSnapshot {
    options: SanitizedOptionMap,
    previous_generation: Generation,
    request: Option<crate::rpc_budget::RpcRequestLease>,
}

enum ControlReply {
    Ready(Value),
    Deferred(oneshot::Receiver<Result<Value, HttpControlError>>),
}

enum MutationPublication {
    Control {
        response: Value,
        remove_task: Option<TaskId>,
        readmit: bool,
        replacement: Option<Box<HttpTaskSpec>>,
    },
    Import {
        remaining: VecDeque<ImportMember>,
        result: Value,
        parent_spec: Option<Box<HttpTaskSpec>>,
    },
    Admission {
        gid: Gid,
        readmission_started: bool,
    },
    Options {
        replacement: Box<HttpTaskSpec>,
        patch_id: OptionPatchId,
        previous_generation: Generation,
        kind: ValidatedOptionPatchKind,
        live_rate: Option<ariax_runtime::PreparedRateLimit>,
    },
}

struct ImportMember {
    command: SchedulerCommand,
    plan: PersistenceEffectPlan,
}

struct PendingMutation {
    publication: MutationPublication,
    reply: oneshot::Sender<Result<Value, HttpControlError>>,
}

struct PendingSourceReplacement {
    replacement: HttpTaskSpec,
    response: Value,
    reply: oneshot::Sender<Result<Value, HttpControlError>>,
    committing: bool,
    _request: Option<crate::rpc_budget::RpcRequestLease>,
}

#[derive(Clone)]
struct ControlWorkReservation {
    _input: crate::rpc_budget::RpcRequestLease,
    _copies: crate::rpc_budget::RpcRequestLease,
}

struct OwnerTurn {
    remaining: usize,
    used: usize,
    deadline: Instant,
    progressed: bool,
}

impl OwnerTurn {
    fn new() -> Self {
        Self {
            remaining: 32,
            used: 0,
            deadline: Instant::now() + Duration::from_millis(1),
            progressed: false,
        }
    }

    fn take_step(&mut self) -> bool {
        if self.remaining == 0 || Instant::now() >= self.deadline {
            return false;
        }
        self.remaining -= 1;
        self.used += 1;
        true
    }

    fn stop(&mut self) {
        self.remaining = 0;
    }

    fn mark_progress(&mut self) {
        self.progressed = true;
    }
}

/// Shared mutable control plane used by both transports.
pub struct HttpControlPlane {
    #[cfg(feature = "bt")]
    bt: bittorrent::BtControl,
    engine: crate::BootstrappedEngine,
    config: HttpControlPlaneConfig,
    tasks: SharedHttpTaskCatalog,
    stats: SharedHttpTransferStats,
    supervisor: Option<HttpWorkerSupervisor>,
    metadata_follow: Option<crate::MetadataFollowQueue>,
    pending_follow: Option<metadata_follow::PendingFollow>,
    session: SessionHandle,
    session_id: SessionId,
    journal_sequences: BTreeMap<Gid, u64>,
    next_task_id: u64,
    global_options: Arc<BTreeMap<String, String>>,
    flat_options: Arc<BTreeMap<String, String>>,
    rpc_template: Arc<BTreeMap<String, String>>,
    url_rules: Arc<ariax_config::UrlRules>,
    config_generation: u64,
    config_charge: Option<crate::rpc_budget::RpcByteCharge>,
    pending_option_snapshots: BTreeMap<OptionPatchId, PendingOptionSnapshot>,
    pending_restart_patches: BTreeMap<Gid, OptionPatchId>,
    pending_source_replacements: BTreeMap<Gid, PendingSourceReplacement>,
    next_option_patch_id: u64,
    shutdown_requested: bool,
    force_shutdown_requested: bool,
    events: RpcEventBroker,
    subscriptions: BTreeMap<u64, RpcEventSubscriber>,
    observed_statuses: Arc<ariax_runtime::StatusSnapshotRoot>,
    global_download_rate: Option<RateArbiter>,
    rpc_budgets: crate::RpcBudgets,
    process_resources: Option<crate::HttpProcessResources>,
    scheduling: crate::HttpSchedulingPolicy,
    slow_observations: Arc<BTreeMap<Gid, crate::slow_slots::SlowObservation>>,
    next_slow_sample: Option<MonotonicInstant>,
    owner_client: crate::RpcClientBudget,
    direct_client: crate::RpcClientBudget,
    pending_work: Option<ControlWorkReservation>,
    pending_mutation: Option<PendingMutation>,
    pending_input: Option<control_io::PendingInput>,
    pending_admission: Option<admission::PendingAdmission>,
    pending_configuration: Option<configuration::PendingConfiguration>,
    #[cfg(test)]
    admission_gate: Option<Arc<admission::PreparationGate>>,
    pending_bulk: Option<control_ops::PendingBulkControl>,
    bulk_first: bool,
    control_order: control_ops::ControlOrdering,
    dispatch_sequence: Option<u64>,
    dispatch_bulk: Option<control_ops::PreparedBulk>,
    session_export: Option<ConfiguredSessionExport>,
    pending_session_export: Option<PendingSessionExport>,
    completed_session_exports: u64,
    last_session_export_failed: bool,
    queries: query::ControlQueryReader,
    query_publication_charge: Arc<crate::rpc_budget::RpcByteCharge>,
    turn: OwnerTurn,
    managed_runtime: Option<Arc<control_runtime::SharedRuntime>>,
    cpu_pool: ariax_runtime::CpuPool,
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
    fn bt_admission_pending(&self) -> bool {
        #[cfg(feature = "bt")]
        {
            self.bt.admission.is_some()
        }
        #[cfg(not(feature = "bt"))]
        {
            false
        }
    }
    fn is_bt_task(&self, task: TaskId) -> bool {
        #[cfg(feature = "bt")]
        {
            self.bt.contains(task)
        }
        #[cfg(not(feature = "bt"))]
        {
            let _ = task;
            false
        }
    }

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
        let rpc_budgets = crate::RpcBudgets::process_default();
        let owner_client = rpc_budgets.client().map_err(|_| HttpControlError::Busy)?;
        let direct_client = rpc_budgets.client().map_err(|_| HttpControlError::Busy)?;
        let query_publication_charge = Arc::new(
            owner_client
                .charge(query::publication_bytes(config.task_capacity.get()))
                .map_err(|_| HttpControlError::Busy)?,
        );
        let observed_statuses = engine.snapshot_reader().load();
        let cpu_pool = ariax_runtime::CpuPool::new(ariax_runtime::CpuPoolConfig {
            workers: 1,
            jobs: 32,
            bytes: 32 * 1024 * 1024,
            resident: ariax_runtime::ByteBudget::new(32 * 1024 * 1024),
            shared_disk: false,
        })
        .map_err(|_| HttpControlError::InvalidConfig)?;
        let mut plane = Self {
            #[cfg(feature = "bt")]
            bt: bittorrent::BtControl::default(),
            cpu_pool,
            session: engine.session_handle(),
            session_id: engine.session_id(),
            engine,
            config,
            tasks,
            stats,
            supervisor: None,
            metadata_follow: None,
            pending_follow: None,
            journal_sequences: BTreeMap::new(),
            next_task_id,
            global_options: Arc::new(default_global_options()?),
            flat_options: Arc::new(BTreeMap::new()),
            rpc_template: Arc::new(BTreeMap::new()),
            url_rules: Arc::new(ariax_config::UrlRules::default()),
            config_generation: 0,
            config_charge: None,
            pending_option_snapshots: BTreeMap::new(),
            pending_restart_patches: BTreeMap::new(),
            pending_source_replacements: BTreeMap::new(),
            next_option_patch_id: now_unix_ms().max(1),
            shutdown_requested: false,
            force_shutdown_requested: false,
            events: RpcEventBroker::new(),
            subscriptions: BTreeMap::new(),
            observed_statuses,
            global_download_rate: None,
            rpc_budgets,
            process_resources: None,
            scheduling: crate::HttpSchedulingPolicy::default(),
            slow_observations: Arc::new(BTreeMap::new()),
            next_slow_sample: None,
            owner_client,
            direct_client,
            pending_work: None,
            pending_mutation: None,
            pending_input: None,
            pending_admission: None,
            pending_configuration: None,
            #[cfg(test)]
            admission_gate: None,
            pending_bulk: None,
            bulk_first: false,
            control_order: control_ops::ControlOrdering::default(),
            dispatch_sequence: None,
            dispatch_bulk: None,
            session_export: None,
            pending_session_export: None,
            completed_session_exports: 0,
            last_session_export_failed: false,
            queries: query::ControlQueryReader::new(),
            query_publication_charge,
            turn: OwnerTurn::new(),
            managed_runtime: None,
        };
        plane.restore_catalog()?;
        #[cfg(feature = "bt")]
        plane.restore_bt_catalog()?;
        plane.reset_observed_statuses();
        Ok(plane)
    }

    pub fn attach_rpc_budgets(
        &mut self,
        budgets: crate::RpcBudgets,
    ) -> Result<(), HttpControlError> {
        if self.managed_runtime.is_some()
            || self.events.subscriber_count() != 0
            || self.pending_session_export.is_some()
            || self.config_charge.is_some()
            || !self.pending_source_replacements.is_empty()
            || self
                .pending_option_snapshots
                .values()
                .any(|pending| pending.request.is_some())
            || self.pending_work.is_some()
            || self.pending_admission.is_some()
            || self.pending_configuration.is_some()
        {
            return Err(HttpControlError::Busy);
        }
        let owner_client = budgets.client().map_err(|_| HttpControlError::Busy)?;
        let direct_client = budgets.client().map_err(|_| HttpControlError::Busy)?;
        let query_publication_charge = Arc::new(
            owner_client
                .charge(query::publication_bytes(self.config.task_capacity.get()))
                .map_err(|_| HttpControlError::Busy)?,
        );
        self.query_publication_charge = query_publication_charge;
        self.events = RpcEventBroker::with_budgets(budgets.clone());
        self.rpc_budgets = budgets;
        self.process_resources = None;
        self.owner_client = owner_client;
        self.direct_client = direct_client;
        Ok(())
    }

    pub fn attach_process_resources(
        &mut self,
        resources: crate::HttpProcessResources,
    ) -> Result<(), HttpControlError> {
        self.tasks
            .set_metadata_budget(resources.metadata_budget())
            .map_err(|_| HttpControlError::Busy)?;
        self.attach_rpc_budgets(resources.rpc_budgets())?;
        self.cpu_pool = resources.cpu_pool();
        self.scheduling = resources.scheduling_policy();
        #[cfg(feature = "bt")]
        {
            self.bt.attach_resources(resources.bt_resources())?;
        }
        self.process_resources = Some(resources);
        Ok(())
    }

    pub fn scheduling_policy(&self) -> crate::HttpSchedulingPolicy {
        self.scheduling.clone()
    }

    pub fn diagnostics(&self) -> ControlDiagnostics {
        let budget = self.rpc_budgets.snapshot();
        let profile = self
            .process_resources
            .as_ref()
            .map(crate::HttpProcessResources::profile);
        ControlDiagnostics {
            profile: profile.map(|profile| profile.baseline().code().to_owned()),
            event_backend: profile.map(|_| {
                if cfg!(windows) {
                    "tokio-iocp"
                } else if cfg!(target_os = "linux") {
                    "tokio-epoll"
                } else {
                    "tokio"
                }
                .to_owned()
            }),
            disk_backend: profile.map(|_| "blocking-positioned".to_owned()),
            buffer_budget_bytes: profile.map(|profile| profile.limits().buffer_budget_bytes),
            task_count: self.engine.snapshot_reader().load().len(),
            active_workers: self
                .supervisor
                .as_ref()
                .map_or(0, HttpWorkerSupervisor::active_workers),
            config_generation: self.config_generation,
            event_subscribers: self.events.subscriber_count(),
            rpc_items: budget.items,
            rpc_item_limit: budget.item_limit,
            rpc_bytes: budget.bytes,
            rpc_byte_limit: budget.byte_limit,
            resident_bytes: budget.resident_bytes,
            resident_limit: budget.resident_limit,
        }
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
        self.metadata_follow = worker.metadata_follow_queue();
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

    pub fn shutdown(mut self) -> Result<crate::ProcessShutdownReport, crate::ProcessShutdownError> {
        self.shutdown_requested = true;
        if let Some(queue) = &self.metadata_follow {
            queue.close();
        }
        let deadline = Instant::now() + self.config.supervisor.shutdown_timeout;
        let continuations_drained = loop {
            if self.pending_admission.is_some()
                || self.bt_admission_pending()
                || self.pending_configuration.is_some()
                || self.pending_follow.is_some()
            {
                if Instant::now() >= deadline || self.poll_once().is_err() {
                    break false;
                }
                std::thread::park_timeout(CONTROL_PROGRESS_POLL);
                continue;
            }
            if self.drive_engine_until(deadline).is_err() {
                break false;
            }
            if self.pending_bulk.is_some() {
                self.turn = OwnerTurn::new();
                if Instant::now() >= deadline || self.poll_bulk_control().is_err() {
                    break false;
                }
                continue;
            }
            if self.pending_source_replacements.is_empty() {
                break true;
            }
            if self.complete_source_replacements().is_err() || self.engine_idle() {
                break false;
            }
        };
        #[cfg(feature = "bt")]
        let bt_drained = continuations_drained && self.drain_bt(deadline);
        #[cfg(not(feature = "bt"))]
        let bt_drained = true;
        let exports_drained = continuations_drained && self.drain_session_export(deadline);
        let Self {
            engine,
            mut supervisor,
            pending_work: _pending_work,
            pending_mutation: _pending_mutation,
            pending_input: _pending_input,
            pending_admission: _pending_admission,
            pending_follow: _pending_follow,
            pending_configuration: _pending_configuration,
            pending_bulk: _pending_bulk,
            pending_source_replacements: _pending_sources,
            pending_option_snapshots: _pending_options,
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
        shutdown.complete_drain(if continuations_drained && exports_drained && bt_drained {
            drain
        } else {
            ProcessDrainOutcome::Failed
        })?;
        shutdown.finish()
    }

    /// Drains live HTTP workers before closing journals and the session owner.
    pub async fn shutdown_async(
        mut self,
    ) -> Result<crate::ProcessShutdownReport, crate::ProcessShutdownError> {
        self.shutdown_requested = true;
        if let Some(queue) = &self.metadata_follow {
            queue.close();
        }
        let started = Instant::now();
        let deadline = started + self.config.supervisor.shutdown_timeout;
        let continuations_drained = loop {
            if self.engine_idle()
                && self.pending_source_replacements.is_empty()
                && self.pending_bulk.is_none()
                && self.pending_admission.is_none()
                && !self.bt_admission_pending()
                && self.pending_follow.is_none()
                && self.pending_configuration.is_none()
                && self.pending_mutation.is_none()
            {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            match self.poll_once() {
                Ok(()) | Err(HttpControlError::Busy) => {}
                Err(_) => break false,
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        #[cfg(feature = "bt")]
        let bt_drained = continuations_drained && self.drain_bt_async(deadline).await;
        #[cfg(not(feature = "bt"))]
        let bt_drained = true;
        let exports_drained =
            continuations_drained && self.drain_session_export_async(deadline).await;
        let Self {
            engine,
            supervisor,
            pending_work: _pending_work,
            pending_mutation: _pending_mutation,
            pending_input: _pending_input,
            pending_admission: _pending_admission,
            pending_follow: _pending_follow,
            pending_configuration: _pending_configuration,
            pending_bulk: _pending_bulk,
            pending_source_replacements: _pending_sources,
            pending_option_snapshots: _pending_options,
            ..
        } = self;
        let mut shutdown = engine.begin_shutdown()?;
        let drain_timeout = shutdown.drain_timeout().saturating_sub(started.elapsed());
        let drain = match supervisor {
            Some(supervisor) => match supervisor.shutdown_with_timeout(drain_timeout).await {
                HttpWorkerSupervisorShutdown::Drained => ProcessDrainOutcome::Drained,
                HttpWorkerSupervisorShutdown::TimedOut { .. } => ProcessDrainOutcome::TimedOut,
            },
            None => ProcessDrainOutcome::Drained,
        };
        shutdown.complete_drain(if continuations_drained && exports_drained && bt_drained {
            drain
        } else {
            ProcessDrainOutcome::Failed
        })?;
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
            if sources.is_empty() {
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
                        request: None,
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
            if recovered
                .journal
                .host_key_state()
                .is_some_and(|state| state.decision == ariax_storage::HostKeyDecision::Approved)
                && recovered
                    .journal
                    .current_options()
                    .is_none_or(|current| current.options() != &persisted_options)
            {
                return Err(HttpControlError::Persistence(
                    "approved host-key option mirror does not match the journal".to_owned(),
                ));
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
            let mut spec = match HttpTaskSpec::from_persisted_sources(
                recovered.task_id,
                recovered.gid,
                sources,
                output_root,
                output,
                options,
            ) {
                Ok(spec) => spec,
                Err(_) => continue,
            };
            if let Some(manifest) = recovered.journal.verification_manifest() {
                let index = persisted_options.entries().find_map(|(name, value)| {
                    (name == "metalink-file-index")
                        .then(|| value.parse().ok())
                        .flatten()
                });
                spec = spec
                    .with_verification(manifest.clone(), index)
                    .map_err(HttpControlError::TaskSpec)?;
            } else if spec.options().transfer.verification_fingerprint.is_some() {
                return Err(HttpControlError::Persistence(
                    "required verification manifest is incomplete".to_owned(),
                ));
            }
            if self.tasks.insert(spec).is_ok() {
                self.journal_sequences
                    .insert(recovered.gid, recovered.journal.last_sequence());
                let _ = self.stats.get_or_create(recovered.task_id);
            }
        }
        Ok(())
    }

    /// Startup repair only; live option changes use a nonblocking session prelude.
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

    fn finish_pending_mutation(&mut self) {
        let Some(pending) = self.pending_mutation.take() else {
            return;
        };
        let result = match pending.publication {
            MutationPublication::Control {
                response,
                remove_task,
                readmit,
                replacement,
            } => {
                if let Some(replacement) = replacement
                    && let Err(error) = self.tasks.replace(*replacement)
                {
                    self.engine.fail_control_publication();
                    let _ = pending.reply.send(Err(HttpControlError::Catalog(error)));
                    return;
                }
                if let Some(task) = remove_task {
                    self.tasks.remove(task);
                    self.stats.remove(task);
                    #[cfg(feature = "bt")]
                    {
                        self.bt.remove(task);
                        self.engine.runtime_handle().unregister_bt_task(task);
                    }
                }
                if readmit {
                    match self.try_admit_one(MonotonicInstant::now()) {
                        Ok(()) | Err(HttpControlError::Busy) => {}
                        Err(error) => {
                            let _ = pending.reply.send(Err(error));
                            return;
                        }
                    }
                    if !self.engine_idle() {
                        self.pending_mutation = Some(PendingMutation {
                            publication: MutationPublication::Control {
                                response,
                                remove_task: None,
                                readmit: false,
                                replacement: None,
                            },
                            reply: pending.reply,
                        });
                        return;
                    }
                }
                Ok(response)
            }

            MutationPublication::Import {
                mut remaining,
                result,
                parent_spec,
            } => {
                if let Some(next) = remaining.pop_front() {
                    if let Err(error) = self.prepare_and_begin(next.plan, next.command) {
                        self.engine.fail_control_publication();
                        let _ = pending.reply.send(Err(error));
                        return;
                    }
                    self.pending_mutation = Some(PendingMutation {
                        publication: MutationPublication::Import {
                            remaining,
                            result,
                            parent_spec,
                        },
                        reply: pending.reply,
                    });
                    return;
                }
                if let Some(spec) = parent_spec
                    && let Err(error) = self.tasks.replace(*spec)
                {
                    self.engine.fail_control_publication();
                    let _ = pending.reply.send(Err(HttpControlError::Catalog(error)));
                    return;
                }
                Ok(result)
            }
            MutationPublication::Admission {
                gid,
                readmission_started,
            } => {
                self.journal_sequences.entry(gid).or_insert(2);
                if !readmission_started {
                    // Metadata admission is already durable. A temporary
                    // rate or scheduler backpressure condition only delays
                    // ordinary readmission; it must not turn the accepted
                    // add into a failed RPC or lose its catalog entry.
                    match self.try_admit_one(MonotonicInstant::now()) {
                        Ok(()) | Err(HttpControlError::Busy) => {}
                        Err(error) => {
                            let _ = pending.reply.send(Err(error));
                            return;
                        }
                    }
                    if !self.engine_idle() {
                        self.pending_mutation = Some(PendingMutation {
                            publication: MutationPublication::Admission {
                                gid,
                                readmission_started: true,
                            },
                            reply: pending.reply,
                        });
                        return;
                    }
                }
                Ok(Value::String(gid.to_string()))
            }
            MutationPublication::Options {
                replacement,
                patch_id,
                previous_generation,
                kind,
                live_rate,
            } => {
                let gid = replacement.gid();
                if kind == ValidatedOptionPatchKind::ActiveRestart
                    && self.engine.scheduler().task(gid).is_some_and(|task| {
                        task.pending_option_patch.is_none()
                            && task.pending_barrier.is_none()
                            && task.generation == previous_generation
                    })
                {
                    self.pending_option_snapshots.remove(&patch_id);
                    self.pending_restart_patches.remove(&gid);
                    Err(HttpControlError::Persistence(
                        "option patch was not persisted".to_owned(),
                    ))
                } else {
                    self.tasks
                        .replace(*replacement)
                        .map_err(HttpControlError::Catalog)
                        .map(|_| {
                            if let Some(update) = live_rate {
                                update.apply();
                            }
                            Value::String("OK".to_owned())
                        })
                }
            }
        };
        if result.is_ok() {
            self.publish_task_state_events();
        }
        let _ = pending.reply.send(result);
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
        self.turn = OwnerTurn::new();
        // A large scheduler step may use the remaining cooperative deadline.
        // Alternate first access so neither continuation nor maintenance work
        // can be permanently placed behind that step on every owner turn.
        self.bulk_first = !self.bulk_first;
        let result = if self.bulk_first {
            self.poll_bulk_control()
                .and_then(|()| self.poll_once_inner())
        } else {
            self.poll_once_inner()
                .and_then(|()| self.poll_bulk_control())
        };
        self.publish_query();
        result
    }

    fn poll_once_inner(&mut self) -> Result<(), HttpControlError> {
        if !self.turn.take_step() {
            return Ok(());
        }
        self.poll_session_export();
        if !self.engine_idle() {
            self.poll_engine_turn()?;
            if !self.engine_idle() {
                return Ok(());
            }
        }
        if !self.turn.take_step() {
            return Ok(());
        }
        self.poll_configuration();
        #[cfg(feature = "bt")]
        if self.poll_bt_admission()? {
            return Ok(());
        }
        self.poll_metadata_follow()?;
        if !self.turn.take_step()
            || self.poll_admission()?
            || self.admission_fenced()
            || !self.engine_idle()
        {
            return Ok(());
        }
        let work = self.reserve_scheduler_work(None, 0)?;
        let result = self.poll_once_reserved();
        self.retain_pending_work(Some(work))?;
        match result {
            Err(HttpControlError::Busy) => Ok(()),
            result => result,
        }
    }

    fn poll_once_reserved(&mut self) -> Result<(), HttpControlError> {
        let now = MonotonicInstant::now();
        if !self.turn.take_step() {
            return Ok(());
        }
        #[cfg(feature = "bt")]
        if self.bt.poll_rates(self.global_download_rate.as_ref())? {
            self.turn.mark_progress();
        }
        #[cfg(feature = "bt")]
        if self.bt.poll(
            &self.engine.runtime_handle(),
            &self.session,
            &self.cpu_pool,
            &self.tasks,
        )? {
            self.turn.mark_progress();
        }
        if let Some(supervisor) = self.supervisor.as_mut() {
            supervisor
                .poll_once(now)
                .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        }
        if self.engine_idle()
            && self.turn.take_step()
            && let Some(event) = self.engine.runtime_handle().poll_event_at(now)
        {
            self.prepare_and_begin_event(event, now)?;
            self.poll_engine_turn()?;
        }
        if self.engine_idle() && self.turn.take_step() {
            self.complete_source_replacements()?;
            if self.engine_idle() && self.turn.take_step() {
                self.poll_slow_slots_at(now)?;
            }
            if self.engine_idle() && self.turn.take_step() {
                self.try_admit_one(now)?;
            }
        }
        self.poll_engine_turn()?;
        self.publish_task_state_events();
        Ok(())
    }

    pub fn call(&mut self, method: &str, params: Value) -> Result<Value, HttpControlError> {
        self.call_admitted(method, params, None)
    }

    fn call_admitted(
        &mut self,
        method: &str,
        params: Value,
        request: Option<crate::rpc_budget::RpcRequestLease>,
    ) -> Result<Value, HttpControlError> {
        if changes_scheduler_tasks(method) {
            while !self.engine_idle() {
                match self.drive_engine() {
                    Ok(()) | Err(HttpControlError::Busy) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        match self.begin_call_admitted(method, params, request)? {
            ControlReply::Ready(value) => Ok(value),
            ControlReply::Deferred(reply) => self.wait_for_mutation(reply),
        }
    }

    fn begin_call_admitted(
        &mut self,
        method: &str,
        params: Value,
        request: Option<crate::rpc_budget::RpcRequestLease>,
    ) -> Result<ControlReply, HttpControlError> {
        self.begin_call_authorized(method, params, request, false)
    }

    fn begin_call_authorized(
        &mut self,
        method: &str,
        params: Value,
        request: Option<crate::rpc_budget::RpcRequestLease>,
        local_admin: bool,
    ) -> Result<ControlReply, HttpControlError> {
        let request = self.reserve_command_memory(method, &params, request.as_ref())?;
        let torrent = matches!(method, "aria2.addTorrent" | "addTorrent");
        let magnet = matches!(method, "aria2.addUri" | "addUri")
            && params.get(0).and_then(Value::as_array).is_some_and(|uris| {
                uris.iter()
                    .any(|uri| uri.as_str().is_some_and(|uri| uri.starts_with("magnet:")))
            });
        if torrent || magnet {
            #[cfg(feature = "bt")]
            {
                return self.begin_bt_admission(
                    params,
                    torrent,
                    request.expect("command reservation"),
                );
            }
            #[cfg(not(feature = "bt"))]
            {
                return Err(HttpControlError::Unsupported(
                    "BitTorrent feature unavailable",
                ));
            }
        }
        if matches!(method, "aria2.saveSession" | "saveSession") {
            require_no_params(&params, "saveSession")?;
            return self.begin_session_export(request.expect("command reservation"), true);
        }
        if matches!(
            method,
            "aria2.changeGlobalOption" | "changeGlobalOption" | "ariax.reloadConfig"
        ) {
            return self.begin_configuration(method, params, request.expect("command reservation"));
        }
        if matches!(
            method,
            "ariax.importSession" | "aria2.addUri" | "addUri" | "aria2.addMetalink" | "addMetalink"
        ) {
            return self.begin_admission(
                params,
                request.expect("command reservation"),
                if method == "ariax.importSession" {
                    admission::AdmissionKind::Session
                } else if matches!(method, "aria2.addMetalink" | "addMetalink") {
                    admission::AdmissionKind::Metalink
                } else {
                    admission::AdmissionKind::Uri
                },
                local_admin,
            );
        }
        if self.admission_fenced() && !query::is_query(method) {
            return Err(HttpControlError::Busy);
        }
        let changes_tasks = changes_scheduler_tasks(method);
        let work = changes_tasks
            .then(|| {
                let new_tasks = usize::from(matches!(method, "aria2.addUri" | "addUri"));
                self.reserve_scheduler_work(request.as_ref(), new_tasks)
            })
            .transpose()?;
        if control_ops::is_bulk_control(method) {
            let result =
                self.begin_bulk_control(method, params, request.expect("command reservation"));
            self.retain_pending_work(work)?;
            return result;
        }
        if control_ops::is_task_control(method) {
            let result = self.begin_task_control(method, params);
            self.retain_pending_work(work)?;
            return result;
        }
        if matches!(method, "aria2.changeOption" | "changeOption") {
            let result = self.begin_change_option(params, request.clone());
            self.retain_pending_work(work)?;
            return result;
        }
        let result = match method {
            "aria2.tellStatus" | "tellStatus" => self.tell_status(params),
            "aria2.tellActive" | "tellActive" => self.tell_active(params),
            "aria2.tellWaiting" | "tellWaiting" => self.tell_waiting(params),
            "aria2.tellStopped" | "tellStopped" => self.tell_stopped(params),
            "aria2.getUris" | "getUris" => self.get_uris(params),
            "aria2.getFiles" | "getFiles" => self.get_files(params),
            "aria2.getServers" | "getServers" => self.get_servers(params),
            "aria2.getPeers" | "getPeers" => self.capture_query().get_peers(params),
            "aria2.getOption" | "getOption" => self.get_option(params),
            "aria2.changeUri" | "changeUri" | "ariax.replaceSources" => {
                self.source_call_sync(method, params)
            }
            "aria2.getGlobalOption" | "getGlobalOption" => self.get_global_option(params),
            "aria2.getVersion" | "getVersion" => self.get_version(params),
            "aria2.getSessionInfo" | "getSessionInfo" => self.get_session_info(params),
            "aria2.getGlobalStat" | "getGlobalStat" => self.global_stat(params),
            "aria2.shutdown" | "shutdown" => self.request_shutdown(params, false),
            "aria2.forceShutdown" | "forceShutdown" => self.request_shutdown(params, true),
            "ariax.subscribe" => self.subscribe_events(params),
            "ariax.unsubscribe" => self.unsubscribe_events(params),
            "ariax.pollEvents" => self.poll_events(params),
            "ariax.checkConfig" => self.check_config(params),
            "ariax.dumpConfig" => self.dump_config(params),
            "ariax.getDiagnostics" => {
                require_no_params(&params, "getDiagnostics")?;
                crate::rpc_result::to_value(&self.diagnostics(), RESULT_VALUE_BYTES)
                    .map_err(Into::into)
            }
            "ariax.exportSession" => self.export_session(params),
            _ => Err(HttpControlError::Unsupported("method not found")),
        };
        if let Ok(value) = &result {
            self.publish_query();
            self.publish_control_event(method, value);
            if changes_tasks {
                self.publish_task_state_events();
            }
        }
        self.retain_pending_work(work)?;
        result.map(ControlReply::Ready)
    }

    fn wait_for_mutation(
        &mut self,
        mut reply: oneshot::Receiver<Result<Value, HttpControlError>>,
    ) -> Result<Value, HttpControlError> {
        loop {
            self.poll_session_export();
            match reply.try_recv() {
                Ok(result) => return result,
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(HttpControlError::Persistence(
                        "mutation owner stopped".to_owned(),
                    ));
                }
            }
            match self.poll_once() {
                Ok(()) | Err(HttpControlError::Busy) => {}
                Err(error) => return Err(error),
            }
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
    }

    fn subscribe_events(&mut self, params: Value) -> Result<Value, HttpControlError> {
        self.subscribe_events_with_client(params, None)
    }

    fn subscribe_events_with_client(
        &mut self,
        params: Value,
        client: Option<crate::RpcClientBudget>,
    ) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() <= 3).ok_or(
            HttpControlError::InvalidParams("subscribe accepts event and byte limits and a filter"),
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
        let filter = values
            .get(2)
            .map(crate::RpcEventFilter::from_rpc)
            .transpose()
            .map_err(event_backend_error)?
            .unwrap_or_default();
        let subscriber = match client {
            Some(client) => self
                .events
                .subscribe_filtered_with_client(limits, filter, client),
            None => self.events.subscribe_filtered(limits, filter),
        }
        .map_err(event_backend_error)?;
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
        let mut events = ResultList::new();
        for _ in 0..count {
            match subscriber.try_next_bounded(events.remaining()) {
                Ok(Some(delivery)) => events.push_scratch(delivery.into_value())?,
                Ok(None) => break,
                Err(RpcEventError::EventTooLarge) if !events.is_empty() => break,
                Err(error) => return Err(event_backend_error(error)),
            }
        }
        Ok(events.finish())
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
        self.observed_statuses = self.engine.snapshot_reader().load();
    }

    fn publish_task_state_events(&mut self) {
        self.publish_query();
        let current = self.engine.snapshot_reader().load();
        if Arc::ptr_eq(&current, &self.observed_statuses) {
            return;
        }
        let changes: Box<dyn Iterator<Item = Gid> + '_> =
            if current.revision() == self.observed_statuses.revision().saturating_add(1) {
                Box::new(current.changed_tasks().iter().copied())
            } else {
                // Startup and explicit low-level driver adapters can skip revisions.
                Box::new(current.tasks().keys().copied())
            };
        for gid in changes {
            let Some(status) = current
                .task(gid)
                .and_then(|task| task.snapshot.wire_status().ok())
            else {
                continue;
            };
            let previous = self
                .observed_statuses
                .task(gid)
                .and_then(|task| task.snapshot.wire_status().ok());
            let seeding = current
                .task(gid)
                .is_some_and(|task| task.snapshot.state == ariax_core::TaskState::Seeding);
            let was_seeding = self
                .observed_statuses
                .task(gid)
                .is_some_and(|task| task.snapshot.state == ariax_core::TaskState::Seeding);
            if seeding != was_seeding {
                if seeding
                    && let Ok(event) = aria2_task_event("aria2.onBtDownloadComplete", Some(gid))
                {
                    self.events.publish(event);
                }
                if let Ok(event) = RpcEvent::notification(
                    "ariax.onSeeding",
                    json!({"gid": gid.to_string(), "seeding": seeding}),
                    RpcEventClass::Reliable,
                    None,
                ) {
                    self.events.publish(event);
                }
            }
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

    fn tell_status(&mut self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().tell_status(params)
    }

    fn tell_active(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().tell_active(params)
    }

    fn tell_waiting(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().tell_waiting(params)
    }

    fn tell_stopped(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().tell_stopped(params)
    }

    fn get_uris(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().get_uris(params)
    }

    fn get_files(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().get_files(params)
    }

    fn get_servers(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().get_servers(params)
    }

    fn get_option(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().get_option(params)
    }

    fn begin_change_option(
        &mut self,
        params: Value,
        request: Option<crate::rpc_budget::RpcRequestLease>,
    ) -> Result<ControlReply, HttpControlError> {
        let values = params
            .as_array()
            .filter(|values| (2..=3).contains(&values.len()))
            .ok_or(HttpControlError::InvalidParams(
                "changeOption requires GID, options, and optional restart settings",
            ))?;
        let restart = match values.get(2) {
            None => false,
            Some(Value::Object(settings))
                if settings.len() == 1
                    && settings.get("restart").and_then(Value::as_bool).is_some() =>
            {
                settings["restart"].as_bool().expect("validated flag")
            }
            _ => return Err(HttpControlError::InvalidParams("invalid restart settings")),
        };
        let gid = self.resolve_gid_text(
            values[0]
                .as_str()
                .ok_or(HttpControlError::InvalidParams("GID must be a string"))?,
        )?;
        #[cfg(feature = "bt")]
        if self.bt.catalog.contains_key(&gid) {
            return self.begin_bt_option_change(
                gid,
                &values[1],
                request.expect("option request reservation"),
            );
        }
        let mut patch = parse_registry_options(&values[1], Scope::RpcChange)?;
        if self.pending_restart_patches.contains_key(&gid)
            || self.pending_source_replacements.contains_key(&gid)
        {
            return Err(HttpControlError::Busy);
        }
        if patch.is_empty() {
            return Ok(ControlReply::Ready(Value::String("OK".to_owned())));
        }
        let current = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        if let Some(directory) = patch.remove("dir") {
            if Path::new(&directory.canonical) != current.output_root() {
                return Err(rejected_option_names(
                    ["dir"],
                    OptionPatchRejectReason::InvalidValue,
                ));
            }
            if patch.is_empty() {
                return Ok(ControlReply::Ready(Value::String("OK".to_owned())));
            }
        }
        let root = self.engine.snapshot_reader().load();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        let previous_generation = task.snapshot.generation;
        let status = task
            .snapshot
            .wire_status()
            .map_err(|_| HttpControlError::Scheduler("invalid public snapshot".to_owned()))?;
        // Allocating projects as aria2 "waiting", but its worker already owns
        // this generation's options and rate bucket.
        let active_generation = status == Aria2Status::Active
            || task.snapshot.state == ariax_core::TaskState::Allocating;
        if matches!(
            status,
            Aria2Status::Complete | Aria2Status::Error | Aria2Status::Removed
        ) {
            return Err(rejected_option_names(
                patch.keys(),
                OptionPatchRejectReason::NotRuntimeMutable,
            ));
        }
        if active_generation {
            let rejected: Vec<_> = patch
                .iter()
                .filter_map(|(name, entry)| {
                    let reason = match entry.runtime_update {
                        RuntimeUpdate::NewGeneration if !restart => {
                            OptionPatchRejectReason::RequiresNewGeneration
                        }
                        RuntimeUpdate::WaitingOnly => OptionPatchRejectReason::NotRuntimeMutable,
                        _ => return None,
                    };
                    Some(OptionPatchRejection {
                        name: name.clone(),
                        reason,
                    })
                })
                .collect();
            if !rejected.is_empty() {
                return Err(HttpControlError::OptionPatchRejected(rejected));
            }
        }
        drop(root);

        let mut merged = current
            .persistence_options()
            .map_err(HttpControlError::TaskSpec)?
            .entries()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect::<BTreeMap<_, _>>();
        merged.retain(|name, _| {
            !configuration::discard_inherited_retry(name, |key| patch.contains_key(key))
        });
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
            rejected_option_names(patch.keys(), OptionPatchRejectReason::InvalidValue)
        })?;
        let output = HttpTaskSpec::persisted_output(&options).map_err(|_| {
            rejected_option_names(patch.keys(), OptionPatchRejectReason::InvalidValue)
        })?;
        let replacement = current.with_options(output, http_options).map_err(|_| {
            rejected_option_names(patch.keys(), OptionPatchRejectReason::InvalidValue)
        })?;
        let replacement = self
            .tasks
            .snapshot()
            .reserve_spec(replacement)
            .map_err(|_| HttpControlError::Busy)?;
        let options = replacement
            .persistence_options()
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
        let kind = if active_generation && !live_only {
            ValidatedOptionPatchKind::ActiveRestart
        } else {
            ValidatedOptionPatchKind::InPlace
        };
        let live_rate = if active_generation && patch.contains_key("max-download-limit") {
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
                    request,
                },
            );
            self.pending_restart_patches.insert(gid, patch_id);
        }
        let mut writes = match self.prepare_outcome_plans(&mut simulation, outcome.effects, None) {
            Ok(writes) => writes,
            Err(error) => {
                self.pending_option_snapshots.remove(&patch_id);
                self.pending_restart_patches.remove(&gid);
                return Err(error);
            }
        };
        if kind == ValidatedOptionPatchKind::InPlace {
            writes.unit(SessionCommand::ReplaceTaskOptions {
                gid,
                scope: OptionsSnapshotScope::CurrentGeneration,
                options,
            });
        }
        if let Err(error) = self.begin_prepared_input(
            control_io::PreparedInput::Command(command, MonotonicInstant::now()),
            writes,
        ) {
            self.pending_option_snapshots.remove(&patch_id);
            self.pending_restart_patches.remove(&gid);
            return Err(error);
        }
        let (reply, receiver) = oneshot::channel();
        self.pending_mutation = Some(PendingMutation {
            publication: MutationPublication::Options {
                replacement: Box::new(replacement),
                patch_id,
                previous_generation,
                kind,
                live_rate,
            },
            reply,
        });
        Ok(ControlReply::Deferred(receiver))
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
            .filter_map(|source| source.uri().map(str::to_owned))
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
        let replacement = HttpTaskSpec::new(
            current.task(),
            current.gid(),
            uris,
            current.output_root().clone(),
            current.output().clone(),
            current.options().clone(),
            false,
        )
        .map_err(HttpControlError::TaskSpec)?;
        match current.verification() {
            Some(manifest) => replacement
                .with_verification(manifest.clone(), current.metalink_index())
                .map_err(HttpControlError::TaskSpec),
            None => Ok(replacement),
        }
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
        let mut reply = self.begin_source_plan(replacement, response, None)?;
        loop {
            match reply.try_recv() {
                Ok(result) => return result,
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(HttpControlError::Persistence(
                        "source replacement owner stopped".to_owned(),
                    ));
                }
            }
            match self.drive_engine() {
                Ok(()) => self.complete_source_replacements()?,
                Err(HttpControlError::Busy) => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn begin_source_call(
        &mut self,
        method: &str,
        params: Value,
        request: Option<crate::rpc_budget::RpcRequestLease>,
    ) -> Result<oneshot::Receiver<Result<Value, HttpControlError>>, HttpControlError> {
        let (replacement, response) = self.prepare_source_call(method, params)?;
        self.begin_source_plan(replacement, response, request)
    }

    fn begin_source_plan(
        &mut self,
        replacement: HttpTaskSpec,
        response: Value,
        request: Option<crate::rpc_budget::RpcRequestLease>,
    ) -> Result<oneshot::Receiver<Result<Value, HttpControlError>>, HttpControlError> {
        let replacement = self
            .tasks
            .snapshot()
            .reserve_spec(replacement)
            .map_err(|_| HttpControlError::Busy)?;
        let gid = replacement.gid();
        let (reply, receiver) = oneshot::channel();
        self.pending_source_replacements.insert(
            gid,
            PendingSourceReplacement {
                replacement,
                response,
                reply,
                committing: false,
                _request: request,
            },
        );
        if let Err(error) =
            self.prepare_and_begin_command(SchedulerCommand::BeginSourceReplacement { gid })
        {
            self.pending_source_replacements.remove(&gid);
            return Err(error);
        }
        Ok(receiver)
    }

    fn complete_source_replacements(&mut self) -> Result<(), HttpControlError> {
        if !self.engine_idle() {
            return Ok(());
        }
        let ready = self
            .pending_source_replacements
            .keys()
            .copied()
            .find(|gid| {
                self.engine
                    .scheduler()
                    .task(*gid)
                    .is_none_or(|task| task.pending_barrier.is_none() && !task.slot.owns_slot())
            });
        if let Some(gid) = ready {
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
                return Ok(());
            }
            self.pending_source_replacements
                .get_mut(&gid)
                .expect("pending source replacement")
                .committing = true;
            let satisfies_credentials = self
                .engine
                .scheduler()
                .credential_requirement_key(gid)
                .filter(|key| {
                    matches!(
                        key.kind,
                        ariax_core::CredentialKind::SourceUri
                            | ariax_core::CredentialKind::HttpAuthentication
                    )
                });
            if let Err(error) =
                self.prepare_and_begin_command(SchedulerCommand::CommitSourceReplacement {
                    gid,
                    satisfies_credentials,
                })
            {
                self.pending_source_replacements
                    .get_mut(&gid)
                    .expect("pending sources")
                    .committing = false;
                return Err(error);
            }
            return Ok(());
        }
        Ok(())
    }

    fn publish_committed_sources(&mut self) -> Result<(), HttpControlError> {
        let committed = self
            .pending_source_replacements
            .iter()
            .find_map(|(gid, pending)| pending.committing.then_some(*gid));
        if let Some(gid) = committed {
            let pending = self
                .pending_source_replacements
                .remove(&gid)
                .expect("committed sources");
            let result = self
                .tasks
                .replace(pending.replacement)
                .map(|_| pending.response)
                .map_err(HttpControlError::Catalog);
            let failed = result.is_err();
            if !failed {
                self.publish_query();
            }
            let _ = pending.reply.send(result);
            if failed {
                return Err(HttpControlError::Persistence(
                    "committed source catalog publication failed".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Compatibility adapter that attaches the same managed runtime as native/RPC handles.
    /// Reuse a `HttpControlBackend` for repeated calls; this adapter briefly locks
    /// the owner to acquire that handle before projecting or admitting the call.
    pub async fn call_shared(
        plane: &Arc<Mutex<Self>>,
        method: &str,
        params: Value,
    ) -> Result<Value, HttpControlError> {
        Self::call_shared_with_context(plane, method, params, crate::RpcClientContext::default())
            .await
    }

    pub(crate) async fn call_shared_with_context(
        plane: &Arc<Mutex<Self>>,
        method: &str,
        params: Value,
        context: crate::RpcClientContext,
    ) -> Result<Value, HttpControlError> {
        let backend = HttpControlBackend::from_shared(plane.clone()).await;
        backend.start_control_runtime()?;
        backend.control.call(method, params, context).await
    }

    fn reserve_command_memory(
        &self,
        method: &str,
        params: &Value,
        request: Option<&crate::rpc_budget::RpcRequestLease>,
    ) -> Result<Option<crate::rpc_budget::RpcRequestLease>, HttpControlError> {
        let mut bytes = crate::rpc_json::command_value_bytes(params).saturating_add(64 * 1024);
        bytes = bytes.saturating_add(self.configuration_command_bytes(method, params)?);
        if matches!(method, "aria2.changeUri" | "changeUri")
            && let Some(gid) = params
                .as_array()
                .and_then(|params| params.first())
                .and_then(Value::as_str)
        {
            let gid = self.resolve_gid_text(gid)?;
            if let Some(spec) = self.tasks.get_gid(gid) {
                for source in spec.sources() {
                    bytes = bytes
                        .saturating_add(source.uri().map_or(0, str::len).saturating_mul(12))
                        .saturating_add(1024);
                }
            }
        }
        if bytes > crate::MAX_RPC_CLIENT_REQUEST_BYTES {
            return Err(HttpControlError::Busy);
        }
        let request = match request {
            Some(request) => request.clone(),
            None => self
                .direct_client
                .try_request(0)
                .map_err(|_| HttpControlError::Busy)?,
        };
        request
            .reserve_command(bytes)
            .map(Some)
            .map_err(|_| HttpControlError::Busy)
    }

    fn reserve_scheduler_work(
        &self,
        request: Option<&crate::rpc_budget::RpcRequestLease>,
        new_tasks: usize,
    ) -> Result<ControlWorkReservation, HttpControlError> {
        if !self.engine_idle() {
            return Err(HttpControlError::Busy);
        }
        let scheduler = self.engine.scheduler();
        let bytes = scheduler
            .estimated_clone_bytes()
            .saturating_add(self.engine.snapshot_reader().load().estimated_draft_bytes())
            .saturating_add(scheduler.len().saturating_mul(512))
            .saturating_add(new_tasks.saturating_mul(8192))
            .saturating_add(128 * 1024);
        let input = match request {
            Some(request) => request.clone(),
            None => self
                .owner_client
                .try_request(0)
                .map_err(|_| HttpControlError::Busy)?,
        };
        let copies = input
            .reserve_command(bytes)
            .map_err(|_| HttpControlError::Busy)?;
        Ok(ControlWorkReservation {
            _input: input,
            _copies: copies,
        })
    }

    fn retain_pending_work(
        &mut self,
        work: Option<ControlWorkReservation>,
    ) -> Result<(), HttpControlError> {
        if self.engine_idle() {
            if let Err(error) = self.engine.discard_prepared_control() {
                self.pending_work = work;
                return Err(HttpControlError::Scheduler(format!("{error:?}")));
            }
        } else if let Some(work) = work {
            debug_assert!(self.pending_work.is_none());
            self.pending_work = Some(work);
        }
        Ok(())
    }

    fn get_global_option(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().get_global_option(params)
    }

    fn get_version(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().get_version(params)
    }

    fn get_session_info(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().get_session_info(params)
    }

    fn request_shutdown(&mut self, params: Value, force: bool) -> Result<Value, HttpControlError> {
        require_no_params(&params, if force { "forceShutdown" } else { "shutdown" })?;
        self.shutdown_requested = true;
        self.force_shutdown_requested |= force;
        Ok(Value::String("OK".to_owned()))
    }

    fn export_session(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().export_session(params)
    }

    pub fn configure_session_export(
        &mut self,
        config: SessionExportConfig,
    ) -> Result<(), HttpControlError> {
        if self.pending_session_export.is_some()
            || config.interval.is_some_and(|interval| {
                interval.is_zero() || interval > Duration::from_secs(86_400)
            })
        {
            return Err(HttpControlError::InvalidConfig);
        }
        let destination =
            ariax_storage::SessionExportDestination::new(&config.path).map_err(|_| {
                HttpControlError::InvalidParams("invalid local session export destination")
            })?;
        let canonical_parent = std::fs::canonicalize(
            config
                .path
                .parent()
                .ok_or(HttpControlError::InvalidConfig)?,
        )
        .map_err(|_| HttpControlError::InvalidConfig)?;
        let canonical_path = std::fs::canonicalize(&config.path).unwrap_or_else(|_| {
            canonical_parent.join(config.path.file_name().expect("validated export filename"))
        });
        let control = std::fs::canonicalize(self.engine.control_directory())
            .map_err(|_| HttpControlError::InvalidConfig)?;
        let database = std::fs::canonicalize(self.engine.session_database_path())
            .map_err(|_| HttpControlError::InvalidConfig)?;
        let managed_database = database.parent() == Some(canonical_parent.as_path())
            && ["", "-wal", "-shm", ".ariax-owner-lock"]
                .iter()
                .any(|suffix| {
                    let mut name = database
                        .file_name()
                        .expect("database filename")
                        .to_os_string();
                    name.push(suffix);
                    canonical_path.file_name() == Some(name.as_os_str())
                });
        if canonical_path.starts_with(control) || managed_database {
            return Err(HttpControlError::InvalidParams(
                "session export overlaps managed persistence",
            ));
        }
        self.session_export = Some(ConfiguredSessionExport {
            destination,
            format: config.format,
            interval: config.interval,
            next_save: config.interval.map(|interval| Instant::now() + interval),
        });
        Ok(())
    }

    pub fn import_session_file(
        &mut self,
        path: &std::path::Path,
        format: crate::SessionFormat,
    ) -> Result<Value, HttpControlError> {
        let request = self
            .direct_client
            .try_request(0)
            .map_err(|_| HttpControlError::Busy)?;
        request
            .reserve(crate::MAX_SESSION_DOCUMENT_BYTES + 1)
            .map_err(|_| HttpControlError::Busy)?;
        let document = ariax_storage::read_session_document(path).map_err(|_| {
            HttpControlError::InvalidParams("cannot read bounded local session input")
        })?;
        self.call_admitted(
            "ariax.importSession",
            json!([document, format.as_str()]),
            Some(request),
        )
    }

    fn begin_session_export(
        &mut self,
        request: crate::rpc_budget::RpcRequestLease,
        reply_requested: bool,
    ) -> Result<ControlReply, HttpControlError> {
        if self.pending_session_export.is_some()
            || !self.engine_idle()
            || self.pending_mutation.is_some()
            || !self.pending_source_replacements.is_empty()
        {
            return Err(HttpControlError::Busy);
        }
        let configured = self
            .session_export
            .as_ref()
            .ok_or(HttpControlError::Unsupported(
                "save-session is not configured",
            ))?;
        let destination = configured.destination.clone();
        let format = configured.format;
        let work = request
            .reserve_command(crate::MAX_SESSION_DOCUMENT_BYTES)
            .map_err(|_| HttpControlError::Busy)?;
        let workspace = request
            .client()
            .charge(crate::rpc_budget::RPC_RESULT_WORKSPACE_BYTES)
            .map_err(|_| HttpControlError::Busy)?;
        let root = self.capture_query();
        let retention = request
            .client()
            .charge(root.retained_bytes())
            .map_err(|_| HttpControlError::Busy)?;
        let projection = self.queries.reserve_projection()?;
        let (send, completion) = oneshot::channel();
        let retained = work.clone();
        let thread = std::thread::Builder::new()
            .name("ariax-session-export".to_owned())
            .spawn(move || {
                let _work = retained;
                let result = (|| {
                    let document = root.export_session(json!([]))?;
                    let bytes = crate::session_file::render(&document, format)?;
                    drop(document);
                    drop(workspace);
                    drop(projection);
                    drop(root);
                    drop(retention);
                    destination.publish(&bytes).map_err(|_| {
                        HttpControlError::Persistence(
                            "session export publication failed".to_owned(),
                        )
                    })
                })();
                let _ = send.send(result);
            })
            .map_err(|_| HttpControlError::Busy)?;
        let (reply, receiver) = oneshot::channel();
        self.pending_session_export = Some(PendingSessionExport {
            completion,
            reply: reply_requested.then_some(reply),
            _thread: thread,
            _request: work,
        });
        if let Some(configured) = self.session_export.as_mut() {
            configured.next_save = configured
                .interval
                .map(|interval| Instant::now() + interval);
        }
        Ok(if reply_requested {
            ControlReply::Deferred(receiver)
        } else {
            ControlReply::Ready(Value::Null)
        })
    }

    fn poll_session_export(&mut self) {
        if let Some(mut pending) = self.pending_session_export.take() {
            match pending.completion.try_recv() {
                Err(oneshot::error::TryRecvError::Empty) => {
                    self.pending_session_export = Some(pending)
                }
                completion => {
                    self.turn.mark_progress();
                    let result = completion.unwrap_or_else(|_| {
                        Err(HttpControlError::Persistence(
                            "session export writer stopped".to_owned(),
                        ))
                    });
                    self.last_session_export_failed = result.is_err();
                    if result.is_ok() {
                        self.completed_session_exports =
                            self.completed_session_exports.saturating_add(1);
                    }
                    if let Some(reply) = pending.reply.take() {
                        let _ = reply.send(result.map(|()| Value::String("OK".to_owned())));
                    }
                }
            }
        }
        if !self.shutdown_requested
            && self.pending_session_export.is_none()
            && self.engine_idle()
            && self.pending_mutation.is_none()
            && self.pending_source_replacements.is_empty()
            && self
                .session_export
                .as_ref()
                .and_then(|config| config.next_save)
                .is_some_and(|at| at <= Instant::now())
        {
            let result = self
                .rpc_budgets
                .client()
                .and_then(|client| client.try_request(0))
                .map_err(|_| HttpControlError::Busy)
                .and_then(|request| self.begin_session_export(request, false));
            if result.is_err() {
                self.last_session_export_failed = true;
                if let Some(config) = self.session_export.as_mut() {
                    config.next_save = config.interval.map(|interval| Instant::now() + interval);
                }
            }
        }
    }

    fn drain_session_export(&mut self, deadline: Instant) -> bool {
        let mut final_started = self.session_export.is_none();
        loop {
            self.poll_session_export();
            if self.pending_session_export.is_none() {
                if final_started {
                    return !self.last_session_export_failed;
                }
                let result = self
                    .rpc_budgets
                    .client()
                    .and_then(|client| client.try_request(0))
                    .map_err(|_| HttpControlError::Busy)
                    .and_then(|request| self.begin_session_export(request, false));
                if result.is_err() {
                    return false;
                }
                final_started = true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
    }

    async fn drain_session_export_async(&mut self, deadline: Instant) -> bool {
        let mut final_started = self.session_export.is_none();
        loop {
            self.poll_session_export();
            if self.pending_session_export.is_none() {
                if final_started {
                    return !self.last_session_export_failed;
                }
                let result = self
                    .rpc_budgets
                    .client()
                    .and_then(|client| client.try_request(0))
                    .map_err(|_| HttpControlError::Busy)
                    .and_then(|request| self.begin_session_export(request, false));
                if result.is_err() {
                    return false;
                }
                final_started = true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
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

    fn resolve_gid_text(&self, value: &str) -> Result<Gid, HttpControlError> {
        query::resolve_gid(&self.engine.snapshot_reader().load(), value)
    }

    fn global_stat(&mut self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().global_stat(params)
    }

    fn prepare_and_begin_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<(), HttpControlError> {
        let mut simulation = self.engine.scheduler().clone();
        let outcome = simulation
            .execute_command_at(command.clone(), MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        let writes = self.prepare_outcome_plans(&mut simulation, outcome.effects, None)?;
        self.begin_prepared_input(
            control_io::PreparedInput::Command(command, MonotonicInstant::now()),
            writes,
        )
    }

    fn prepare_and_begin(
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
        Ok(())
    }

    fn drive_engine(&mut self) -> Result<(), HttpControlError> {
        self.drive_engine_until(Instant::now() + CONTROL_PROGRESS_TIMEOUT)
    }

    fn drive_engine_until(&mut self, deadline: Instant) -> Result<(), HttpControlError> {
        loop {
            let progress = self.poll_engine_step()?;
            if self.engine_idle() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(HttpControlError::Busy);
            }
            if !progress {
                std::thread::park_timeout(CONTROL_PROGRESS_POLL);
            }
        }
    }

    fn poll_engine_turn(&mut self) -> Result<(), HttpControlError> {
        while self.turn.take_step() {
            if !self.poll_engine_step()? {
                break;
            }
        }
        Ok(())
    }

    fn poll_engine_step(&mut self) -> Result<bool, HttpControlError> {
        if let Err(error) = self.poll_prepared_input() {
            if let Some(pending) = self.pending_mutation.take() {
                let _ = pending.reply.send(Err(error.clone()));
            }
            return Err(error);
        }
        if self.pending_input.is_some() {
            return Ok(false);
        }
        let poll = self.engine.poll_at(MonotonicInstant::now());
        let completed = matches!(poll, ariax_runtime::SchedulerDriverPoll::Completed { .. });
        match poll {
            ariax_runtime::SchedulerDriverPoll::Idle
            | ariax_runtime::SchedulerDriverPoll::Completed { .. } => {
                if self.engine_idle() {
                    self.engine
                        .discard_prepared_control()
                        .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
                    if completed || self.pending_mutation.is_some() {
                        self.turn.mark_progress();
                        self.finish_pending_mutation();
                        // One completed command/member ends this owner turn even
                        // when its continuation has started the next effect chain.
                        self.turn.stop();
                        self.publish_task_state_events();
                    }
                    if self.engine_idle() {
                        self.publish_committed_sources()?;
                        self.pending_work = None;
                        self.retire_promoted_option_patches();
                        return Ok(false);
                    }
                }
                Ok(false)
            }
            ariax_runtime::SchedulerDriverPoll::Progressed => {
                self.turn.mark_progress();
                Ok(true)
            }
            ariax_runtime::SchedulerDriverPoll::WaitingForCompletion { .. }
            | ariax_runtime::SchedulerDriverPoll::Backpressured { .. } => Ok(false),
            ariax_runtime::SchedulerDriverPoll::Faulted(error) => {
                if let Some(pending) = self.pending_mutation.take() {
                    let _ = pending.reply.send(Err(HttpControlError::Persistence(
                        "mutation owner failed; recovery is required".to_owned(),
                    )));
                }
                Err(HttpControlError::Scheduler(format!("{error:?}")))
            }
        }
    }

    fn try_admit_one(&mut self, now: MonotonicInstant) -> Result<(), HttpControlError> {
        if self.shutdown_requested || self.pending_configuration.is_some() {
            return Ok(());
        }
        let mut simulation = self.engine.scheduler().clone();
        #[cfg(feature = "bt")]
        let total = self
            .global_options
            .get("max-overall-download-limit")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        #[cfg(feature = "bt")]
        let upload = self
            .global_options
            .get("max-overall-upload-limit")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let outcome = match simulation.admit_next_at(now) {
            Ok(outcome) => outcome,
            Err(_) => {
                #[cfg(feature = "bt")]
                self.require_bt_bandwidth(&simulation, total, upload)?;
                return Ok(());
            }
        };
        if self.supervisor.is_none() && outcome.effects.iter().any(|effect| matches!(effect, TransitionEffect::PersistGenerationStarted { task_id, .. } if !self.is_bt_task(*task_id))) {
            #[cfg(feature = "bt")]
            self.require_bt_bandwidth(&self.engine.scheduler().clone(), total, upload)?;
            return Ok(());
        }
        #[cfg(feature = "bt")]
        if !self.require_bt_bandwidth(&simulation, total, upload)? {
            return Ok(());
        }
        if let Some(rate) = &self.global_download_rate {
            for effect in &outcome.effects {
                if let TransitionEffect::PersistGenerationStarted { task_id, .. } = effect {
                    if self.is_bt_task(*task_id) {
                        continue;
                    }
                    let spec = self.tasks.get(*task_id).ok_or(HttpControlError::NotFound)?;
                    rate.set_scoped_limit(
                        RateScope::Task(task_id.get()),
                        RateLimit::per_second(spec.options().max_download_limit),
                    )
                    .map_err(|_| HttpControlError::Busy)?;
                }
            }
        }
        let writes = self.prepare_outcome_plans(
            &mut simulation,
            outcome.effects,
            Some(GenerationStartReason::RetryReadmission),
        )?;
        self.begin_prepared_input(control_io::PreparedInput::Admit(now), writes)
    }

    fn prepare_outcome_plans(
        &mut self,
        simulation: &mut RequestScheduler,
        effects: Vec<TransitionEffect>,
        generation_reason: Option<GenerationStartReason>,
    ) -> Result<control_io::SessionWrites, HttpControlError> {
        let mut writes = control_io::SessionWrites::default();
        let mut queue = VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            let host_state = match &effect {
                TransitionEffect::PersistHostKeyChallenge { challenge, .. } => {
                    Some((challenge.clone(), ariax_storage::HostKeyDecision::Pending))
                }
                TransitionEffect::PersistHostKeyPinAndClearChallenge { gid, .. }
                | TransitionEffect::PersistHostKeyChallengeRejected { gid, .. } => {
                    let challenge = self
                        .engine
                        .scheduler()
                        .presented_host_key(*gid)
                        .cloned()
                        .ok_or(HttpControlError::NotFound)?;
                    let decision = if matches!(
                        effect,
                        TransitionEffect::PersistHostKeyPinAndClearChallenge { .. }
                    ) {
                        ariax_storage::HostKeyDecision::Approved
                    } else {
                        ariax_storage::HostKeyDecision::Rejected
                    };
                    Some((challenge, decision))
                }
                _ => None,
            };
            if let Some((challenge, decision)) = host_state {
                let generation = simulation
                    .task(effect.gid())
                    .map_or(Generation::INITIAL, |task| task.generation);
                writes.journal(
                    effect.gid(),
                    generation,
                    JournalPayload::HostKeyState {
                        state: ariax_storage::JournalHostKeyState {
                            challenge,
                            decision,
                            created_ms: now_unix_ms(),
                        },
                    },
                );
            }
            if let TransitionEffect::PublishSnapshot { snapshot, .. } = &effect
                && let Some(task) = simulation.task(snapshot.gid)
                && self.engine.scheduler().task(snapshot.gid).is_some()
                && !self.is_bt_task(task.task_id)
                && task.pending_barrier.is_none()
                && matches!(
                    task.state,
                    ariax_core::TaskState::Paused
                        | ariax_core::TaskState::PausedSlow
                        | ariax_core::TaskState::PausedHostKey
                )
            {
                // TaskPaused requires an empty lease set. The supervisor's
                // drained event supplies that authority; never write it while
                // cancellation still owns the generation's worker.
                let reason = if task.state == ariax_core::TaskState::PausedSlow {
                    TaskPauseReason::SlowSlot
                } else if task.state == ariax_core::TaskState::PausedHostKey {
                    TaskPauseReason::HostKeyApproval
                } else {
                    TaskPauseReason::User
                };
                writes.journal(
                    task.gid,
                    task.generation,
                    JournalPayload::TaskPaused { reason },
                );
            }
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
                    .prepare_runtime(crate::RuntimeEffectPreparation::OptionApplication(
                        Box::new(plan),
                    ))
                    .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
            }
        }
        Ok(writes)
    }

    fn prepare_and_begin_event(
        &mut self,
        event: TaskEventEnvelope,
        at: MonotonicInstant,
    ) -> Result<(), HttpControlError> {
        let mut simulation = self.engine.scheduler().clone();
        let outcome = simulation
            .handle_event_at(&event, at)
            .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        let generation_reason = match event.event() {
            TaskEvent::RetryReady { .. } => Some(GenerationStartReason::RetryReadmission),
            TaskEvent::ActiveRepresentationRestart { .. } => {
                Some(GenerationStartReason::RepresentationRestart)
            }
            _ => None,
        };
        let writes =
            self.prepare_outcome_plans(&mut simulation, outcome.effects, generation_reason)?;
        self.begin_prepared_input(control_io::PreparedInput::Event(event, at), writes)
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
                #[cfg(feature = "bt")]
                if self.bt.contains(*task_id) {
                    return PersistenceEffectPlan::new(
                        effect.clone(),
                        vec![PersistencePlanStep::BeginBtGeneration {
                            gid: *gid,
                            expected: generation.get().saturating_sub(1),
                            generation: generation.get(),
                        }],
                    )
                    .map_err(|error| HttpControlError::Persistence(format!("{error:?}")));
                }
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
            TransitionEffect::PersistHostKeyChallenge { gid, challenge, .. } => {
                let summary = challenge.summary();
                PersistenceEffectPlan::new(
                    effect.clone(),
                    vec![PersistencePlanStep::PutHostKeyChallenge(
                        ariax_storage::SessionHostKeyChallengeRecord {
                            gid: *gid,
                            challenge_id: summary.id,
                            canonical_host: summary.canonical_host.clone(),
                            port: summary.port,
                            algorithm: summary.algorithm.clone(),
                            presented_public_key: challenge.presented_public_key().to_vec(),
                            fingerprint_sha256: summary.fingerprint_sha256,
                            created_ms: now_unix_ms(),
                        },
                    )],
                )
                .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
            TransitionEffect::PersistHostKeyPinAndClearChallenge {
                gid,
                challenge,
                fingerprint_sha256,
                presented_public_key,
                ..
            } => {
                let spec = self.pinned_host_key_spec(*gid, *fingerprint_sha256)?;
                PersistenceEffectPlan::new(
                    effect.clone(),
                    vec![PersistencePlanStep::ResolveHostKeyChallenge(
                        ariax_storage::SessionHostKeyResolution {
                            gid: *gid,
                            challenge_id: *challenge,
                            fingerprint_sha256: *fingerprint_sha256,
                            presented_public_key: presented_public_key.clone(),
                            scope: OptionsSnapshotScope::CurrentGeneration,
                            pinned_options: spec
                                .persistence_options()
                                .map_err(HttpControlError::TaskSpec)?,
                        },
                    )],
                )
                .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
            TransitionEffect::PersistHostKeyChallengeRejected { gid, challenge, .. } => {
                PersistenceEffectPlan::new(
                    effect.clone(),
                    vec![PersistencePlanStep::RejectHostKeyChallenge {
                        gid: *gid,
                        challenge_id: *challenge,
                    }],
                )
                .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
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
        #[cfg(feature = "bt")]
        if self.bt.contains(task_id) {
            let step = self
                .bt
                .terminal_step(task_id, generation, status, error, transition)?;
            return PersistenceEffectPlan::new(effect.clone(), vec![step])
                .map_err(|error| HttpControlError::Persistence(format!("{error:?}")));
        }
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
}

/// Tokio-facing handle for shared bounded command admission and immutable queries.
#[derive(Clone)]
pub struct HttpControlBackend {
    control: control_runtime::ControlRuntime,
    plane: Arc<Mutex<HttpControlPlane>>,
    events: RpcEventBroker,
    rpc_budgets: crate::RpcBudgets,
}

impl HttpControlBackend {
    fn attach_runtime(
        plane: &Arc<Mutex<HttpControlPlane>>,
        owner: &mut HttpControlPlane,
    ) -> control_runtime::ControlRuntime {
        if let Some(shared) = owner.managed_runtime.clone() {
            return control_runtime::ControlRuntime { shared };
        }
        let runtime = control_runtime::ControlRuntime::new(plane, owner);
        owner.managed_runtime = Some(runtime.shared.clone());
        runtime
    }

    pub(crate) async fn from_shared(plane: Arc<Mutex<HttpControlPlane>>) -> Self {
        let mut owner = plane.lock().await;
        let events = owner.events.clone();
        let rpc_budgets = owner.rpc_budgets.clone();
        let control = Self::attach_runtime(&plane, &mut owner);
        drop(owner);
        Self {
            control,
            plane,
            events,
            rpc_budgets,
        }
    }

    #[must_use]
    pub fn new(plane: HttpControlPlane) -> Self {
        let events = plane.event_broker();
        let rpc_budgets = plane.rpc_budgets.clone();
        let plane = Arc::new(Mutex::new(plane));
        let control =
            Self::attach_runtime(&plane, &mut plane.try_lock().expect("new control owner"));
        Self {
            control,
            plane,
            events,
            rpc_budgets,
        }
    }

    pub(crate) fn control_runtime(&self) -> control_runtime::ControlRuntime {
        self.control.clone()
    }

    pub fn start_control_runtime(&self) -> Result<(), HttpControlError> {
        self.control.start()
    }

    pub fn control_failure_receiver(&self) -> watch::Receiver<Option<String>> {
        self.control.failure_receiver()
    }

    #[must_use]
    pub fn control_runtime_metrics(&self) -> ControlRuntimeMetrics {
        self.control.metrics()
    }

    pub async fn drain_control_runtime(&self) -> Result<(), HttpControlError> {
        self.control.drain().await
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
        self.control.shutdown_receiver()
    }

    /// Recover the owner after draining transports and `drain_control_runtime`.
    pub fn try_into_control_plane(self) -> Result<HttpControlPlane, Self> {
        match Arc::try_unwrap(self.plane) {
            Ok(plane) => Ok(plane.into_inner()),
            Err(plane) => Err(Self {
                plane,
                control: self.control,
                events: self.events,
                rpc_budgets: self.rpc_budgets,
            }),
        }
    }
}

impl HttpRpcBackend for HttpControlBackend {
    fn call(&self, method: &str, params: Value) -> crate::RpcFuture {
        self.call_with_context(method, params, crate::RpcClientContext::default())
    }

    fn rpc_budgets(&self) -> crate::RpcBudgets {
        self.rpc_budgets.clone()
    }

    fn call_with_context(
        &self,
        method: &str,
        params: Value,
        context: crate::RpcClientContext,
    ) -> crate::RpcFuture {
        let control = self.control.clone();
        let plane = self.plane.clone();
        let method = method.to_owned();
        Box::pin(async move {
            let _plane = plane;
            control
                .call(&method, params, context)
                .await
                .map_err(control_backend_error)
        })
    }
}

impl crate::RpcWebSocketBackend for HttpControlBackend {
    fn event_broker(&self) -> RpcEventBroker {
        self.events.clone()
    }
}

fn changes_scheduler_tasks(method: &str) -> bool {
    matches!(
        method.strip_prefix("aria2.").unwrap_or(method),
        "addUri"
            | "addTorrent"
            | "addMetalink"
            | "pause"
            | "forcePause"
            | "pauseAll"
            | "forcePauseAll"
            | "unpause"
            | "unpauseAll"
            | "remove"
            | "forceRemove"
            | "removeDownloadResult"
            | "purgeDownloadResult"
            | "changePosition"
            | "changeOption"
            | "changeUri"
            | "ariax.replaceSources"
            | "ariax.importSession"
            | "ariax.approveHostKey"
    )
}

#[cfg(test)]
#[path = "http_control/phase5_tests.rs"]
mod phase5_tests;

#[cfg(all(test, feature = "bt"))]
#[path = "http_control/phase6_tests.rs"]
mod phase6_tests;

fn control_backend_error(error: HttpControlError) -> HttpRpcBackendError {
    if let HttpControlError::OptionPatchRejected(rejected) = &error {
        return HttpRpcBackendError::new(-32602, "OptionPatchRejected").with_data(json!({
            "code": "OptionPatchRejected",
            "rejected": rejected.iter().map(|entry| json!({"name": entry.name, "reason": entry.reason.code()})).collect::<Vec<_>>()
        }));
    }
    let code = match error {
        HttpControlError::InvalidParams(_) | HttpControlError::TaskSpec(_) => -32602,
        HttpControlError::Unsupported(_) => -32601,
        HttpControlError::NotFound => -32004,
        HttpControlError::Busy => -32005,
        HttpControlError::SlowConsumer => -32007,
        HttpControlError::ResponseTooLarge => -32006,
        _ => -32000,
    };
    HttpRpcBackendError::new(code, error.to_string())
}

fn require_no_params(params: &Value, method: &str) -> Result<(), HttpControlError> {
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
        RpcEventError::TooManySubscribers | RpcEventError::BudgetExhausted => {
            HttpControlError::Busy
        }
        RpcEventError::EventTooLarge => HttpControlError::ResponseTooLarge,
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
    let options = HttpTaskOptions {
        retry: Some(HttpRetryPolicy::default()),
        ..HttpTaskOptions::default()
    }
    .sanitized()
    .map_err(HttpControlError::TaskSpec)?;
    let mut result = options
        .entries()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect::<BTreeMap<_, _>>();
    result.insert("max-overall-download-limit".to_owned(), "0".to_owned());
    #[cfg(feature = "bt")]
    for definition in builtin_registry()
        .definitions()
        .iter()
        .filter(|definition| {
            definition.owner == "bt"
                && definition.runtime_update != RuntimeUpdate::StartupOnly
                && definition.name != "follow-torrent"
        })
    {
        if let Some(default) = definition.default {
            result.insert(definition.name.to_owned(), default.to_owned());
        }
    }
    for option in builtin_registry()
        .definitions()
        .iter()
        .filter(|option| is_scheduling_option(option.name))
    {
        result.insert(
            option.name.to_owned(),
            option.default.expect("scheduling default").to_owned(),
        );
    }
    Ok(result)
}

fn is_executable_download_option(name: &str) -> bool {
    matches!(
        name,
        "dir"
            | "out"
            | "split"
            | "max-connection-per-server"
            | "min-split-size"
            | "piece-length"
            | "connect-timeout"
            | "timeout"
            | "max-download-limit"
            | "lowest-speed-limit"
            | "endgame-max-duplicates"
            | "checksum"
            | "verify-mirror-identity"
    ) || is_retry_option(name)
        || crate::TransferOptions::handles(name)
}

fn is_executable_global_option(name: &str) -> bool {
    name == "max-overall-download-limit"
        || is_executable_bt_option(name)
        || is_executable_download_option(name)
        || is_scheduling_option(name)
}

fn is_executable_bt_option(name: &str) -> bool {
    #[cfg(feature = "bt")]
    {
        bittorrent::task_option(name) || name == "max-overall-upload-limit"
    }
    #[cfg(not(feature = "bt"))]
    {
        let _ = name;
        false
    }
}

fn is_scheduling_option(name: &str) -> bool {
    matches!(
        name,
        "slow-slot-policy"
            | "slow-slot-speed-limit"
            | "slow-slot-grace-period"
            | "slow-slot-min-active-time"
            | "slow-slot-max-demotions"
            | "slow-slot-readmit-after"
            | "slow-slot-readmit-policy"
            | "retry-wait-consumes-slot"
    )
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
    let mut rejected = Vec::new();
    for (name, value) in object {
        let result = (|| {
            let definition = registry
                .find(name)
                .ok_or(OptionPatchRejectReason::Unsupported)?;
            if definition.compat == CompatStatus::UnsafeCompat {
                return Err(OptionPatchRejectReason::UnsafeCompatRequired);
            }
            if !definition.scopes.contains(scope)
                || definition.security != SecurityClass::Normal
                || matches!(definition.runtime_update, RuntimeUpdate::StartupOnly)
            {
                return Err(OptionPatchRejectReason::NotRuntimeMutable);
            }
            let bt_global = scope == Scope::RpcGlobal && is_executable_bt_option(name);
            if definition.compat == CompatStatus::Unsupported
                || definition.compat == CompatStatus::FeatureGated && !bt_global
                || !is_executable_global_option(name)
                || matches!(
                    definition.runtime_update,
                    RuntimeUpdate::None | RuntimeUpdate::UnsafeCompatOnly
                )
                || matches!(
                    definition.runtime_update,
                    RuntimeUpdate::BtLive | RuntimeUpdate::BtRestartRequired
                ) && !bt_global
            {
                return Err(OptionPatchRejectReason::Unsupported);
            }
            let input =
                option_input_text(value).map_err(|_| OptionPatchRejectReason::InvalidValue)?;
            let value = parse_option_value(definition, &input, None)
                .map_err(|_| OptionPatchRejectReason::InvalidValue)?;
            let canonical = canonical_option_value(&value)
                .map_err(|_| OptionPatchRejectReason::InvalidValue)?;
            if name == "sftp-check-host-key" && canonical != "true" {
                return Err(OptionPatchRejectReason::NotRuntimeMutable);
            }
            Ok(ParsedRegistryOption {
                canonical,
                runtime_update: definition.runtime_update,
            })
        })();
        match result {
            Ok(entry) => {
                parsed.insert(name.clone(), entry);
            }
            Err(reason) => rejected.push(OptionPatchRejection {
                name: safe_option_name(name),
                reason,
            }),
        }
    }
    if rejected.is_empty() {
        Ok(parsed)
    } else {
        Err(HttpControlError::OptionPatchRejected(rejected))
    }
}

fn safe_option_name(name: &str) -> String {
    if name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        name.to_owned()
    } else {
        "unknown-option".to_owned()
    }
}

fn rejected_option_names<I, S>(names: I, reason: OptionPatchRejectReason) -> HttpControlError
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    HttpControlError::OptionPatchRejected(
        names
            .into_iter()
            .map(|name| OptionPatchRejection {
                name: safe_option_name(name.as_ref()),
                reason,
            })
            .collect(),
    )
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

fn string_map_value<'a>(
    entries: impl Clone + Iterator<Item = (&'a str, &'a str)>,
) -> Result<Value, crate::rpc_result::ResultTooLarge> {
    crate::rpc_result::to_value(&crate::rpc_result::StringMap(entries), RESULT_VALUE_BYTES)
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
        if let Some(diagnostic) = stats.ssh_connection {
            object.insert("sshConnection".to_owned(), json!(diagnostic));
        }
        if let Some(challenge) = &snapshot.host_key_challenge {
            use base64ct::Encoding;
            object.insert("hostKeyChallenge".to_owned(), json!({
                "id": crate::transfer_task::hex_bytes(challenge.id.as_bytes()),
                "host": challenge.canonical_host, "port": challenge.port,
                "algorithm": challenge.algorithm,
                "fingerprintSha256": format!("SHA256:{}", base64ct::Base64Unpadded::encode_string(challenge.fingerprint_sha256.as_bytes())),
            }));
        }
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
    parse_add_options_authorized(options, default_root, uris, false)
}

pub(crate) fn parse_add_options_authorized(
    options: &Value,
    default_root: &Path,
    uris: &[String],
    local_admin: bool,
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
    if object.len() > ariax_storage::MAX_OPTION_MAP_ENTRIES {
        return Err(HttpControlError::InvalidParams("too many options"));
    }
    let registry = builtin_registry();
    for (name, value) in object {
        if name == "pause" {
            continue;
        }
        let definition = registry
            .find(name)
            .ok_or(HttpControlError::InvalidParams("unsupported addUri option"))?;
        if !definition.scopes.contains(Scope::PerDownload)
            || !(matches!(
                definition.security,
                SecurityClass::Normal | SecurityClass::Sensitive
            ) || (definition.security == SecurityClass::LocalAdmin && local_admin))
            || !is_executable_download_option(name)
        {
            return Err(HttpControlError::InvalidParams("unsupported addUri option"));
        }
        parse_option_value(definition, &option_input_text(value)?, None)
            .map_err(|_| HttpControlError::InvalidParams("invalid option value"))?;
    }
    let mut parsed = HttpTaskOptions::default();
    let mut root = default_root.to_path_buf();
    let mut out = None;
    let mut paused = false;
    for (name, value) in object {
        if is_retry_option(name) {
            continue;
        }
        match name.as_str() {
            "ftp-user" | "ftp-passwd" => {}
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
                parsed
                    .set_content_checksum(
                        value
                            .as_str()
                            .ok_or(HttpControlError::InvalidParams("checksum must be a string"))?,
                    )
                    .map_err(|_| HttpControlError::InvalidParams("invalid checksum"))?;
            }
            "verify-mirror-identity" => {
                parsed.mirror_identity = match value.as_str() {
                    Some("strict") => crate::HttpMirrorIdentityPolicy::RequireSharedDigest,
                    Some("off") => crate::HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
                    _ => {
                        return Err(HttpControlError::InvalidParams(
                            "invalid mirror identity policy",
                        ));
                    }
                }
            }
            _ if crate::TransferOptions::handles(name) => {
                let definition = registry.find(name).expect("validated definition");
                let input = option_input_text(value)?;
                let value = parse_option_value(definition, &input, None)
                    .map_err(|_| HttpControlError::InvalidParams("invalid option value"))?;
                let canonical = if definition.security == SecurityClass::Sensitive {
                    input
                } else {
                    canonical_option_value(&value)?
                };
                parsed
                    .transfer
                    .set(name, &canonical)
                    .map_err(HttpControlError::TaskSpec)?;
                if !parsed.transfer.sftp_check_host_key && !local_admin {
                    return Err(HttpControlError::InvalidParams(
                        "host key bypass requires local administrator authority",
                    ));
                }
            }
            _ => return Err(HttpControlError::InvalidParams("unsupported addUri option")),
        }
    }
    if object.keys().any(|name| is_retry_option(name)) {
        parsed.retry = Some(parse_retry_options(object)?);
    }
    if object.contains_key("ftp-user") || object.contains_key("ftp-passwd") {
        let user = object.get("ftp-user").and_then(Value::as_str).ok_or(
            HttpControlError::InvalidParams("ftp-user is required with ftp-passwd"),
        )?;
        let password = object
            .get("ftp-passwd")
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or(HttpControlError::InvalidParams("ftp-passwd must be text"))
            })
            .transpose()?;
        parsed.transfer.credentials = Some(
            crate::TransferCredentials::new(user.to_owned(), password)
                .map_err(HttpControlError::TaskSpec)?,
        );
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
        policy.retryable_statuses = parse_retry_status_option(value, "retry-on-http-status")?;
    }
    if let Some(value) = object.get("retry-on-http-status-add") {
        for code in parse_retry_status_option(value, "retry-on-http-status-add")?.iter() {
            policy
                .retryable_statuses
                .insert(code)
                .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?;
        }
    }
    if let Some(value) = object.get("retry-on-http-status-remove") {
        for code in parse_retry_status_option(value, "retry-on-http-status-remove")?.iter() {
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

fn parse_retry_status_option(
    value: &Value,
    name: &str,
) -> Result<HttpRetryStatusSet, HttpControlError> {
    let text = retry_text(value, name)?;
    if text.trim().is_empty() {
        return Ok(HttpRetryStatusSet::default());
    }
    HttpRetryStatusSet::parse(text)
        .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))
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
        TransitionEffect::PersistHostKeyPinAndClearChallenge { resolution_id, .. } => {
            TaskEvent::HostKeyResolutionPersisted {
                gid,
                generation,
                resolution_id: *resolution_id,
            }
        }
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
    use ariax_core::{ErrorKind, LeaseId, PieceId, SchedulerConfig, TaskState, UriId};
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

    pub(super) struct TestDirectory {
        pub(super) root: PathBuf,
        control: PathBuf,
        pub(super) output: PathBuf,
        journals: PathBuf,
    }

    impl TestDirectory {
        #[cfg(feature = "metalink")]
        pub(super) fn at(root: PathBuf) -> Self {
            Self {
                control: root.join("control"),
                output: root.join("output"),
                journals: root.join("control/http-journals"),
                root,
            }
        }
        pub(super) fn new() -> Self {
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

        pub(super) fn control_plane(&self) -> HttpControlPlane {
            self.control_plane_with_supervisor(HttpWorkerSupervisorConfig::default())
        }

        fn control_plane_with_capacity(&self, capacity: usize) -> HttpControlPlane {
            let capacity = NonZeroUsize::new(capacity).expect("capacity");
            let mut config = self.process_config();
            config.recovery.scheduler.max_tasks = capacity;
            let engine = bootstrap_process(config, ariax_config::persisted_option_is_safe)
                .expect("bootstrap");
            HttpControlPlane::new(
                engine,
                HttpControlPlaneConfig {
                    output_root: self.output.clone(),
                    journal_root: self.journals.clone(),
                    task_capacity: capacity,
                    supervisor: HttpWorkerSupervisorConfig::default(),
                },
            )
            .expect("control plane")
        }

        fn control_plane_with_active_limit(&self, limit: usize) -> HttpControlPlane {
            let mut config = self.process_config();
            config.recovery.scheduler.max_active_tasks = NonZeroUsize::new(limit).unwrap();
            let engine = bootstrap_process(config, ariax_config::persisted_option_is_safe)
                .expect("bootstrap process");
            HttpControlPlane::new(
                engine,
                HttpControlPlaneConfig {
                    output_root: self.output.clone(),
                    journal_root: self.journals.clone(),
                    task_capacity: NonZeroUsize::new(16).unwrap(),
                    supervisor: HttpWorkerSupervisorConfig::default(),
                },
            )
            .expect("control plane")
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

    pub(super) fn add_paused(plane: &mut HttpControlPlane) -> Gid {
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

    #[tokio::test]
    async fn filesystem_preparation_preserves_queries_urgent_progress_and_import_fencing() {
        struct Release(Arc<admission::PreparationGate>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let existing = add_paused(&mut owner);
        let gate = Arc::new(admission::PreparationGate::default());
        let release = Release(gate.clone());
        owner.admission_gate = Some(gate.clone());
        let queries = owner.query_reader();
        let query_slots = queries.occupy_execution();
        let client = owner.rpc_budgets.client().expect("import client");
        let backend = HttpControlBackend::new(owner);
        let plane = backend.plane();
        let import = tokio::spawn(
            backend.call_with_context(
                "ariax.importSession",
                json!([import_document(2)]),
                crate::RpcClientContext::default()
                    .with_request(client.try_request(0).expect("import request")),
            ),
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        while !gate.entered.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "filesystem preparation has its own slot"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(query_slots);
        assert_eq!(
            backend
                .call("aria2.tellWaiting", json!([0, 1000]))
                .await
                .expect("query during preparation")
                .as_array()
                .expect("list")
                .len(),
            1
        );
        assert_eq!(
            backend
                .call("aria2.unpause", json!([existing.to_string()]))
                .await
                .expect("urgent progress"),
            existing.to_string()
        );
        {
            let mut owner = plane.lock().await;
            assert!(matches!(
                owner.begin_call_admitted(
                    "aria2.addUri",
                    json!([["http://example.test/another"], {"pause":true}]),
                    None
                ),
                Err(HttpControlError::Busy)
            ));
            assert!(!owner.admission_fenced());
        }
        import.abort();
        let _ = import.await;
        assert_eq!(client.outstanding_requests(), 1);
        drop(release);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_fence = false;
        loop {
            let mut owner = plane.lock().await;
            if owner.admission_fenced() {
                saw_fence = true;
                assert!(matches!(
                    owner.begin_task_control("aria2.pause", json!([existing.to_string()])),
                    Err(HttpControlError::Busy)
                ));
                assert!(
                    queries
                        .current()
                        .expect("query root")
                        .get_version(json!([]))
                        .is_ok()
                );
            }
            if owner.engine.snapshot_reader().load().len() == 3
                && owner.pending_admission.is_none()
                && owner.pending_mutation.is_none()
            {
                break;
            }
            drop(owner);
            assert!(Instant::now() < deadline, "disconnected import completes");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(saw_fence);
        backend
            .drain_control_runtime()
            .await
            .expect("runtime drain");
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.request_bytes(), 0);
        drop(backend);
        let owner = Arc::try_unwrap(plane).expect("sole owner").into_inner();
        let root = owner.engine.snapshot_reader().load();
        assert_eq!(root.queue(QueueClass::Waiting), &[existing]);
        assert_eq!(root.queue(QueueClass::Paused).len(), 2);
        assert!(
            matches!(owner.session.execute(SessionCommand::ReadQueueOrder { state: SessionQueueState::Paused }).expect("durable order"), SessionCommandResult::QueueOrder(gids) if gids == root.queue(QueueClass::Paused))
        );
        owner.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn delayed_sqlite_option_write_keeps_queries_and_owner_turns_available() {
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let gid = add_paused(&mut owner);
        let backend = HttpControlBackend::new(owner);
        let plane = backend.plane();
        let lock =
            rusqlite::Connection::open(directory.root.join("session.db")).expect("test writer");
        lock.execute_batch("BEGIN IMMEDIATE")
            .expect("hold SQLite writer");
        let mutation =
            tokio::spawn(backend.call("aria2.changeOption", json!([gid.to_string(), {"split":3}])));
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if plane.lock().await.pending_input.is_some() {
                break;
            }
            assert!(Instant::now() < deadline, "option prelude submitted");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let options = tokio::time::timeout(
            Duration::from_millis(500),
            backend.call("aria2.getOption", json!([gid.to_string()])),
        )
        .await
        .expect("query does not wait for SQLite")
        .expect("old options");
        assert_eq!(options["split"], crate::DEFAULT_HTTP_SPLIT.to_string());
        assert!(!mutation.is_finished());
        assert!(
            tokio::time::timeout(Duration::from_millis(500), plane.lock())
                .await
                .is_ok()
        );
        lock.execute_batch("ROLLBACK")
            .expect("release SQLite writer");
        assert_eq!(
            mutation.await.expect("caller").expect("durable mutation"),
            "OK"
        );
        assert_eq!(
            backend
                .call("aria2.getOption", json!([gid.to_string()]))
                .await
                .expect("new options")["split"],
            "3"
        );
        backend.drain_control_runtime().await.expect("drain");
        drop(backend);
        Arc::try_unwrap(plane)
            .expect("sole owner")
            .into_inner()
            .shutdown()
            .expect("shutdown");
    }

    #[test]
    fn unavailable_session_rejects_option_prelude_without_publishing_new_metadata() {
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let gid = add_paused(&mut owner);
        let ControlReply::Deferred(mut reply) = owner
            .begin_call_admitted(
                "aria2.changeOption",
                json!([gid.to_string(), {"split":3}]),
                None,
            )
            .expect("prepare option")
        else {
            panic!("deferred option");
        };
        owner.session.shutdown().expect("stop storage owner");
        assert!(owner.poll_once().is_err());
        assert!(reply.try_recv().expect("failed reply").is_err());
        assert_eq!(
            owner
                .capture_query()
                .get_option(json!([gid.to_string()]))
                .expect("old catalog")["split"],
            crate::DEFAULT_HTTP_SPLIT.to_string()
        );
        assert!(matches!(
            owner.engine.poll_at(MonotonicInstant::now()),
            ariax_runtime::SchedulerDriverPoll::Faulted(_)
        ));
    }

    #[tokio::test]
    async fn published_queries_complete_while_the_control_owner_is_locked() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let backend = HttpControlBackend::new(plane);
        let shared = backend.plane();
        let guard = shared.lock().await;
        for (method, params) in [
            ("aria2.tellStatus", json!([gid.to_string()])),
            ("aria2.tellWaiting", json!([0, 1000])),
            ("aria2.tellActive", json!([])),
            ("aria2.tellStopped", json!([0, 1000])),
            ("aria2.getUris", json!([gid.to_string()])),
            ("aria2.getFiles", json!([gid.to_string()])),
            ("aria2.getServers", json!([gid.to_string()])),
            ("aria2.getOption", json!([gid.to_string()])),
            ("aria2.getGlobalOption", json!([])),
            ("aria2.getGlobalStat", json!([])),
            ("aria2.getSessionInfo", json!([])),
            ("aria2.getVersion", json!([])),
            ("ariax.checkConfig", json!(["timeout=30\n"])),
            (
                "ariax.dumpConfig",
                json!(["task-effective", "json", gid.to_string()]),
            ),
            ("ariax.exportSession", json!([])),
            ("ariax.getDiagnostics", json!([])),
        ] {
            tokio::time::timeout(Duration::from_secs(2), backend.call(method, params))
                .await
                .expect("query must not await owner unlock")
                .unwrap_or_else(|error| panic!("{method}: {error:?}"));
        }
        let invalid = tokio::time::timeout(
            Duration::from_secs(2),
            backend.call("aria2.tellStatus", json!(["not-a-gid"])),
        )
        .await
        .expect("rejection must not await owner unlock");
        assert!(invalid.is_err());
        drop(guard);
        drop(shared);
        backend
            .try_into_control_plane()
            .unwrap_or_else(|_| panic!("sole owner"))
            .shutdown()
            .expect("shutdown");
    }

    #[test]
    fn frozen_query_keeps_metadata_across_replacement_and_removal() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/file?token=query-secret-canary"], {"pause":true}]),
            )
            .expect("add")
            .as_str()
            .expect("gid")
            .to_owned();
        let reader = plane.query_reader();
        let frozen = reader.current().expect("published root");
        let before_options = frozen.get_option(json!([gid])).expect("old options");
        assert_eq!(
            frozen.get_uris(json!([gid])).expect("live URI")[0]["uri"],
            "http://example.test/file?token=query-secret-canary"
        );
        assert!(
            !frozen
                .export_session(json!([]))
                .expect("safe export")
                .to_string()
                .contains("query-secret-canary")
        );
        plane
            .call(
                "ariax.replaceSources",
                json!([gid, ["http://example.test/replacement"]]),
            )
            .expect("replace source");
        plane
            .call("aria2.changeOption", json!([gid, {"timeout":45}]))
            .expect("change options");
        let newer = reader.current().expect("replacement root");
        assert_eq!(
            newer.get_uris(json!([gid])).expect("new URI")[0]["uri"],
            "http://example.test/replacement"
        );
        assert_eq!(
            newer.get_option(json!([gid])).expect("new options")["timeout"],
            "45"
        );
        assert_eq!(
            frozen.get_option(json!([gid])).expect("frozen options"),
            before_options
        );
        plane.call("aria2.remove", json!([gid])).expect("remove");
        plane
            .call("aria2.removeDownloadResult", json!([gid]))
            .expect("forget result");
        assert!(matches!(
            reader.current().expect("current").get_files(json!([gid])),
            Err(HttpControlError::NotFound)
        ));
        assert_eq!(
            frozen.get_files(json!([gid])).expect("frozen file")[0]["uris"][0]["uri"],
            "http://example.test/file?token=query-secret-canary"
        );
        assert_eq!(
            frozen.tell_status(json!([gid])).expect("frozen status")["status"],
            "paused"
        );
        drop(newer);
        drop(frozen);
        drop(reader);
        plane.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn query_execution_and_failure_refund_admitted_credit() {
        let directory = TestDirectory::new();
        let plane = directory.control_plane();
        let reader = plane.query_reader();
        let client = plane.rpc_budgets.client().expect("client");
        let baseline = client.bytes();
        let held = reader.occupy_execution();
        let request = client.try_request(0).expect("request");
        let context = crate::RpcClientContext::default().with_request(request);
        assert!(matches!(
            reader.call("aria2.getVersion", json!([]), context).await,
            Err(HttpControlError::Busy)
        ));
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.bytes(), baseline);
        drop(held);
        for params in [
            json!(["invalid"]),
            json!(["0000000000000000"]),
            json!(["0"]),
        ] {
            let request = client.try_request(0).expect("request");
            let context = crate::RpcClientContext::default().with_request(request);
            assert!(
                reader
                    .call("aria2.tellStatus", params, context)
                    .await
                    .is_err()
            );
            assert_eq!(client.outstanding_requests(), 0);
            assert_eq!(client.bytes(), baseline);
        }
        reader
            .call(
                "aria2.getVersion",
                json!([]),
                crate::RpcClientContext::default(),
            )
            .await
            .expect("usable after rejection");
        drop(reader);
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn later_pause_and_remove_supersede_unfinished_resume_all() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let first = add_paused(&mut plane);
        let removed = add_paused(&mut plane);
        let paused = add_paused(&mut plane);
        let ControlReply::Deferred(mut reply) = plane
            .begin_call_admitted("aria2.unpauseAll", json!([]), None)
            .expect("bulk accepted")
        else {
            panic!("bulk continuation");
        };
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(plane.pending_bulk.is_some());
        plane
            .call("aria2.pause", json!([paused.to_string()]))
            .expect("later pause");
        plane
            .call("aria2.remove", json!([removed.to_string()]))
            .expect("later remove");
        let deadline = Instant::now() + Duration::from_secs(2);
        while plane.pending_bulk.is_some() {
            assert!(Instant::now() < deadline, "bulk progress");
            plane.poll_once().expect("progress");
            std::thread::yield_now();
        }
        assert_eq!(
            reply.try_recv().expect("reply").expect("bulk success"),
            "OK"
        );
        assert_eq!(
            plane
                .tell_status(json!([first.to_string()]))
                .expect("resumed")["status"],
            "waiting"
        );
        assert_eq!(
            plane
                .tell_status(json!([paused.to_string()]))
                .expect("later pause retained")["status"],
            "paused"
        );
        assert_eq!(
            plane
                .tell_status(json!([removed.to_string()]))
                .expect("later remove retained")["status"],
            "removed"
        );
        assert_eq!(plane.control_order.retained_identities(), 0);
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn accepted_bulk_survives_disconnect_and_refunds_its_credit() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gids = (0..4).map(|_| add_paused(&mut plane)).collect::<Vec<_>>();
        let client = plane.rpc_budgets.client().expect("client");
        let baseline = client.bytes();
        let request = client.try_request(0).expect("request");
        let ControlReply::Deferred(reply) = plane
            .begin_call_admitted("aria2.unpauseAll", json!([]), Some(request))
            .expect("accept bulk")
        else {
            panic!("bulk continuation");
        };
        drop(reply);
        assert_eq!(client.outstanding_requests(), 1);
        let deadline = Instant::now() + Duration::from_secs(2);
        while plane.pending_bulk.is_some() {
            assert!(Instant::now() < deadline, "accepted bulk must finish");
            plane.poll_once().expect("progress");
            std::thread::yield_now();
        }
        for gid in gids {
            assert_eq!(
                plane.tell_status(json!([gid.to_string()])).expect("status")["status"],
                "waiting"
            );
        }
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.bytes(), baseline);
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn rejected_bulk_does_not_replace_the_accepted_continuation() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let ControlReply::Deferred(reply) = plane
            .begin_call_admitted("aria2.unpauseAll", json!([]), None)
            .expect("first bulk")
        else {
            panic!("bulk continuation");
        };
        assert!(matches!(
            plane.begin_call_admitted("aria2.pauseAll", json!([]), None),
            Err(HttpControlError::Busy)
        ));
        assert!(matches!(
            plane.begin_call_admitted("aria2.pauseAll", json!([true]), None),
            Err(HttpControlError::InvalidParams(_))
        ));
        plane.wait_for_mutation(reply).expect("first bulk finishes");
        assert_eq!(
            plane.tell_status(json!([gid.to_string()])).expect("status")["status"],
            "waiting"
        );
        plane.shutdown().expect("shutdown");
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
                json!([gid.to_string(), ["gopher://example.test/file"]]),
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
            Err(HttpControlError::OptionPatchRejected(_))
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
        plane
            .call("aria2.changeGlobalOption", json!([{"timeout": 45}]))
            .expect("future download template");
        let next = add_paused(&mut plane);
        assert_eq!(
            plane
                .tasks
                .get_gid(next)
                .expect("new task")
                .options()
                .response_body_timeout,
            Duration::from_secs(45)
        );
        assert_eq!(
            plane
                .tasks
                .get_gid(gid)
                .expect("existing task")
                .options()
                .response_body_timeout,
            Duration::from_secs(30)
        );
        assert!(
            plane
                .call(
                    "aria2.changeGlobalOption",
                    json!([{"timeout": 20, "allow-overwrite": true}])
                )
                .is_err()
        );
        assert_eq!(plane.global_options["timeout"], "45");

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

    #[test]
    fn config_checks_validate_merged_policy_and_dumps_track_derived_sources() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        for method in ["ariax.checkConfig", "ariax.reloadConfig"] {
            assert!(
                plane
                    .call(method, json!(["retry-wait=30\nretry-max-wait=5\n"]))
                    .is_err()
            );
            assert_eq!(plane.config_generation, 0);
        }
        plane
            .call(
                "ariax.checkConfig",
                json!(["session-store=memory\nretry-profile=aggressive\n"]),
            )
            .expect("startup check without publishing");
        assert_eq!(plane.config_generation, 0);
        plane
            .call("ariax.reloadConfig", json!(["retry-profile=aggressive\n"]))
            .expect("profile");
        plane
            .call(
                "aria2.changeGlobalOption",
                json!([{"retry-max-attempts": 9, "retry-on-http-status-add":"404"}]),
            )
            .expect("partial override");
        let dump = plane
            .call("ariax.dumpConfig", json!(["effective", "json"]))
            .expect("sources");
        assert_eq!(dump["sources"]["retry-wait"], "config");
        assert_eq!(dump["sources"]["retry-max-attempts"], "rpc");
        assert_eq!(dump["sources"]["max-tries"], "rpc");
        assert_eq!(dump["sources"]["retry-on-http-status"], "rpc");
        assert_eq!(dump["options"]["max-tries"], "9");
        plane
            .call(
                "aria2.changeGlobalOption",
                json!([{"retry-max-wait": 5, "retry-after-max":5}]),
            )
            .expect("valid upper wait");
        let before = plane.global_options.clone();
        for method in ["ariax.checkConfig", "ariax.reloadConfig"] {
            assert!(plane.call(method, json!(["retry-after-min=10\n"])).is_err());
            assert_eq!(plane.global_options, before);
        }
        assert!(plane.shutdown().expect("shutdown").is_clean());
    }

    #[test]
    fn runtime_patch_rejections_are_grouped_value_free_and_atomic() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let before = plane
            .call("aria2.getOption", json!([gid.to_string()]))
            .expect("before");
        let error = plane
            .call(
                "aria2.changeOption",
                json!([gid.to_string(), {
                    "split":0, "event-backend":"auto", "allow-overwrite":true,
                    "on-download-complete":"secret-command-canary", "max-download-limit":"1M"
                }]),
            )
            .expect_err("atomic rejection");
        let error = control_backend_error(error);
        let data = error.data.expect("grouped reasons");
        assert_eq!(
            data,
            json!({"code":"OptionPatchRejected", "rejected":[
                {"name":"allow-overwrite","reason":"unsupported"},
                {"name":"event-backend","reason":"not_runtime_mutable"},
                {"name":"on-download-complete","reason":"unsafe_compat_required"},
                {"name":"split","reason":"invalid_value"}
            ]})
        );
        assert!(!data.to_string().contains("canary"));
        assert_eq!(
            plane
                .call("aria2.getOption", json!([gid.to_string()]))
                .expect("after"),
            before
        );
        let error = plane
            .call(
                "aria2.changeOption",
                json!([gid.to_string(), {"retry-wait":30, "retry-max-wait":5}]),
            )
            .expect_err("cross-field validation");
        assert!(matches!(error, HttpControlError::OptionPatchRejected(_)));
        plane
            .call(
                "aria2.changeOption",
                json!([gid.to_string(), {"retry-profile":"aggressive", "max-download-limit":"1M"}]),
            )
            .expect("valid complete patch");
        assert_eq!(
            plane
                .call("aria2.getOption", json!([gid.to_string()]))
                .expect("changed")["max-download-limit"],
            "1048576"
        );
        assert!(plane.shutdown().expect("shutdown").is_clean());
    }

    #[test]
    fn configured_session_saves_are_bounded_periodic_and_complete_after_disconnect() {
        for format in [crate::SessionFormat::Json, crate::SessionFormat::Aria2] {
            let directory = TestDirectory::new();
            let mut plane = directory.control_plane();
            assert!(matches!(
                plane.call("aria2.saveSession", json!([])),
                Err(HttpControlError::Unsupported(_))
            ));
            let gid = add_paused(&mut plane);
            let path = directory.root.join("export.txt");
            plane
                .configure_session_export(SessionExportConfig {
                    path: path.clone(),
                    format,
                    interval: Some(Duration::from_secs(60)),
                })
                .expect("configure export");
            assert!(
                plane
                    .call("aria2.saveSession", json!(["remote-path"]))
                    .is_err()
            );
            let client = plane.rpc_budgets.client().expect("client");
            let request = client.try_request(0).expect("request");
            let ControlReply::Deferred(reply) = plane
                .begin_call_admitted("aria2.saveSession", json!([]), Some(request))
                .expect("begin save")
            else {
                panic!("save must be deferred");
            };
            assert_eq!(client.outstanding_requests(), 1);
            assert!(client.request_bytes() >= crate::MAX_SESSION_DOCUMENT_BYTES);
            assert!(matches!(
                plane.call("aria2.saveSession", json!([])),
                Err(HttpControlError::Busy)
            ));
            drop(reply);
            let deadline = Instant::now() + Duration::from_secs(5);
            while plane.pending_session_export.is_some() {
                plane.poll_session_export();
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(CONTROL_PROGRESS_POLL);
            }
            assert_eq!(client.outstanding_requests(), 0);
            assert_eq!(client.request_bytes(), 0);
            assert_eq!(plane.completed_session_exports, 1);
            assert!(
                fs::read_to_string(&path)
                    .expect("export")
                    .contains(&gid.to_string())
            );
            plane.session_export.as_mut().expect("configured").next_save = Some(Instant::now());
            plane.poll_session_export();
            while plane.pending_session_export.is_some() {
                plane.poll_session_export();
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(CONTROL_PROGRESS_POLL);
            }
            assert_eq!(plane.completed_session_exports, 2);
            let last = add_paused(&mut plane);
            assert!(plane.shutdown().expect("shutdown save").is_clean());
            assert!(
                fs::read_to_string(&path)
                    .expect("final export")
                    .contains(&last.to_string())
            );
            let mut recovered = directory.control_plane();
            let imported = recovered
                .import_session_file(&path, format)
                .expect("local import");
            assert_eq!(imported.as_array().expect("GIDs").len(), 2);
            assert_eq!(recovered.tasks.len(), 4);
            assert!(recovered.shutdown().expect("shutdown import").is_clean());
        }
    }

    #[test]
    fn versioned_reload_and_url_rules_preserve_precedence_and_reject_atomic_mixed_changes() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let original = add_paused(&mut plane);
        let rules = "[[rule]]\nmatch.host='example.test'\noptions.split=3\noptions.timeout=20\n";
        let result = plane.call("ariax.reloadConfig", json!(["split=2\ntimeout=40\nmax-overall-download-limit=1M\n", {"urlRules":rules, "expectedGeneration":0}])).expect("versioned reload");
        assert_eq!(result["configGeneration"], 1);
        assert_eq!(
            plane
                .tasks
                .get_gid(original)
                .expect("unchanged old task")
                .options()
                .split
                .get(),
            5
        );
        let ruled = add_paused(&mut plane);
        assert_eq!(
            plane
                .tasks
                .get_gid(ruled)
                .expect("ruled task")
                .options()
                .split
                .get(),
            3
        );
        assert_eq!(
            plane
                .tasks
                .get_gid(ruled)
                .expect("rule timeout")
                .options()
                .response_body_timeout
                .as_secs(),
            20
        );
        plane
            .call("aria2.changeGlobalOption", json!([{"split":7}]))
            .expect("RPC template override");
        let template = add_paused(&mut plane);
        assert_eq!(
            plane
                .tasks
                .get_gid(template)
                .expect("RPC template")
                .options()
                .split
                .get(),
            7
        );
        let explicit: Gid = plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"pause":true, "split":9}]),
            )
            .expect("explicit override")
            .as_str()
            .expect("gid")
            .parse()
            .expect("gid");
        assert_eq!(
            plane
                .tasks
                .get_gid(explicit)
                .expect("per-download override")
                .options()
                .split
                .get(),
            9
        );
        let before = plane.global_options.clone();
        let generation = plane.config_generation;
        for params in [
            json!(["timeout=10\n", {"expectedGeneration":0}]),
            json!(["max-overall-download-limit=2M\n", {"urlRules":"[[rule]]\nmatch.host='example.test'\noptions.rpc-secret='secret-canary'\n"}]),
            json!(["max-overall-download-limit=2M\nretry-wait=30\nretry-max-wait=5\n"]),
            json!(["max-overall-download-limit=2M\nrpc-passwd=secret-canary\n"]),
        ] {
            assert!(plane.call("ariax.reloadConfig", params).is_err());
            assert_eq!(plane.global_options, before);
            assert_eq!(plane.config_generation, generation);
        }
        let flat = plane
            .call("ariax.dumpConfig", json!(["effective", "flat"]))
            .expect("flat");
        assert!(flat.as_str().expect("text").contains("split=7\n"));
        let json = plane
            .call("ariax.dumpConfig", json!(["effective", "json"]))
            .expect("JSON");
        assert_eq!(json["options"]["split"], "7");
        assert_eq!(json["sources"]["split"], "rpc");
        let toml = plane
            .call("ariax.dumpConfig", json!(["effective", "toml"]))
            .expect("TOML");
        assert!(toml.as_str().expect("text").contains("[sources]"));
        assert!(!format!("{flat} {json} {toml}").contains("secret-canary"));
        let rule_text = plane
            .call("ariax.dumpConfig", json!(["url-rules", "toml"]))
            .expect("TOML rules");
        assert_eq!(
            ariax_config::UrlRules::parse(rule_text.as_str().expect("TOML"))
                .expect("rule dump roundtrip")
                .apply("http://example.test/file")
                .expect("apply")["split"],
            "3"
        );
        let task = plane
            .call(
                "ariax.dumpConfig",
                json!(["task-effective", "json", original.to_string()]),
            )
            .expect("task dump");
        assert_eq!(task["options"]["split"], "5");
        assert!(plane.shutdown().expect("shutdown").is_clean());
        let recovered = directory.control_plane();
        assert_eq!(
            recovered
                .tasks
                .get_gid(ruled)
                .expect("persisted rules")
                .options()
                .split
                .get(),
            3
        );
        assert_eq!(
            recovered
                .tasks
                .get_gid(explicit)
                .expect("persisted override")
                .options()
                .split
                .get(),
            9
        );
        recovered.shutdown().expect("recovery shutdown");
    }

    #[tokio::test]
    async fn session_export_failure_is_reported_and_prevents_clean_async_shutdown() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        add_paused(&mut plane);
        let config = |path| SessionExportConfig {
            path,
            format: crate::SessionFormat::Json,
            interval: None,
        };
        for path in [
            directory.root.join("session.db"),
            directory.root.join("session.db-wal"),
            directory.control.join("export.json"),
        ] {
            assert!(plane.configure_session_export(config(path)).is_err());
        }
        let path = directory.root.join("export.json");
        plane
            .configure_session_export(config(path.clone()))
            .expect("configure");
        fs::create_dir(&path).expect("raced directory");
        assert!(matches!(
            plane.call("aria2.saveSession", json!([])),
            Err(HttpControlError::Persistence(_))
        ));
        let report = plane.shutdown_async().await.expect("shutdown report");
        assert!(!report.is_clean());
        assert!(path.is_dir());
    }

    #[test]
    fn accepted_add_and_option_calls_finish_after_expired_progress_and_disconnected_reply() {
        for disconnected in [false, true] {
            let directory = TestDirectory::new();
            let mut plane = directory.control_plane();
            let client = plane.rpc_budgets.client().expect("client");
            let request = client.try_request(128).expect("request");
            let ControlReply::Deferred(mut reply) = plane
                .begin_call_admitted(
                    "aria2.addUri",
                    json!([["http://example.test/file.bin"], {"pause": true}]),
                    Some(request),
                )
                .expect("admitted add")
            else {
                panic!("add must retain continuation");
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            while plane.pending_admission.is_some() {
                assert!(Instant::now() < deadline, "admission preparation");
                plane.poll_admission().expect("stage admission");
                std::thread::park_timeout(CONTROL_PROGRESS_POLL);
            }
            assert!(matches!(
                plane.drive_engine_until(Instant::now()),
                Err(HttpControlError::Busy)
            ));
            assert!(plane.pending_mutation.is_some());
            assert_eq!(plane.tasks.len(), 1);
            assert!(plane.engine.snapshot_reader().load().is_empty());
            assert_eq!(client.outstanding_requests(), 1);
            if disconnected {
                reply.close();
            }
            plane.drive_engine().expect("finish admission");
            let gid = *plane
                .engine
                .snapshot_reader()
                .load()
                .tasks()
                .keys()
                .next()
                .expect("published gid");
            if !disconnected {
                assert_eq!(
                    reply.try_recv().expect("reply").expect("add"),
                    gid.to_string()
                );
            }
            assert!(plane.pending_mutation.is_none());
            assert_eq!(client.request_bytes(), 0);
            let request = client.try_request(128).expect("option request");
            let ControlReply::Deferred(mut reply) = plane
                .begin_call_admitted(
                    "aria2.changeOption",
                    json!([gid.to_string(), {"split": 3}]),
                    Some(request),
                )
                .expect("accepted option patch")
            else {
                panic!("option continuation");
            };
            assert_eq!(
                plane
                    .tasks
                    .get_gid(gid)
                    .expect("old catalog")
                    .options()
                    .split
                    .get(),
                crate::DEFAULT_HTTP_SPLIT
            );
            assert!(matches!(
                plane.drive_engine_until(Instant::now()),
                Err(HttpControlError::Busy)
            ));
            if disconnected {
                reply.close();
            }
            plane.drive_engine().expect("publish accepted patch");
            if !disconnected {
                assert_eq!(reply.try_recv().expect("reply").expect("patch"), "OK");
            }
            assert_eq!(
                plane
                    .tasks
                    .get_gid(gid)
                    .expect("new catalog")
                    .options()
                    .split
                    .get(),
                3
            );
            assert_eq!(client.outstanding_requests(), 0);
            assert_eq!(client.request_bytes(), 0);
            plane.shutdown().expect("shutdown");
            let mut recovered = directory.control_plane();
            assert_eq!(
                recovered
                    .call("aria2.getOption", json!([gid.to_string()]))
                    .expect("recovered options")["split"],
                "3"
            );
            recovered.shutdown().expect("recovery shutdown");
        }
    }

    #[tokio::test]
    async fn dropping_async_add_reply_keeps_admission_until_shutdown_drain() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gate = Arc::new(admission::PreparationGate::default());
        plane.admission_gate = Some(gate.clone());
        let client = plane.rpc_budgets.client().expect("client");
        let request = client.try_request(128).expect("request");
        let shared = Arc::new(Mutex::new(plane));
        let mut call = Box::pin(HttpControlPlane::call_shared_with_context(
            &shared,
            "aria2.addUri",
            json!([["http://example.test/file.bin"], {"pause": true}]),
            crate::RpcClientContext::default().with_request(request),
        ));
        assert!(futures_util::poll!(&mut call).is_pending());
        let deadline = Instant::now() + Duration::from_secs(2);
        while shared.lock().await.pending_admission.is_none() {
            assert!(Instant::now() < deadline, "managed admission starts");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(call);
        assert_eq!(client.outstanding_requests(), 1);
        gate.release();
        let backend = HttpControlBackend::from_shared(shared.clone()).await;
        backend
            .drain_control_runtime()
            .await
            .expect("drain admitted work");
        drop(backend);
        let owner = Arc::try_unwrap(shared).expect("sole owner").into_inner();
        let report = owner
            .shutdown_async()
            .await
            .expect("shutdown drains accepted add");
        assert!(report.is_clean());
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.request_bytes(), 0);
        let recovered = directory.control_plane();
        assert_eq!(recovered.tasks.len(), 1);
        assert_eq!(recovered.engine.snapshot_reader().load().len(), 1);
        recovered.shutdown().expect("recovery shutdown");
    }

    #[test]
    fn source_commit_remains_pending_until_publication_and_never_dispatches_twice() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let client = plane.rpc_budgets.client().expect("client");
        let request = client.try_request(128).expect("request");
        let params = json!([gid.to_string(), ["http://new.test/file.bin"]]);
        let command = plane
            .reserve_command_memory("ariax.replaceSources", &params, Some(&request))
            .expect("input");
        let work = plane
            .reserve_scheduler_work(command.as_ref(), 0)
            .expect("work");
        let mut reply = plane
            .begin_source_call("ariax.replaceSources", params, command)
            .expect("source begin");
        plane.retain_pending_work(Some(work)).expect("retain begin");
        drop(request);
        plane.drive_engine().expect("finish source begin");
        let work = plane.reserve_scheduler_work(None, 0).expect("commit work");
        plane.complete_source_replacements().expect("begin commit");
        plane
            .retain_pending_work(Some(work))
            .expect("retain commit");
        assert!(plane.pending_source_replacements[&gid].committing);
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            plane.drive_engine_until(Instant::now()),
            Err(HttpControlError::Busy)
        ));
        assert_eq!(
            plane.tasks.get_gid(gid).expect("old catalog").sources()[0]
                .uri()
                .expect("available source"),
            "http://example.test/file.bin"
        );
        assert_eq!(client.outstanding_requests(), 1);
        plane
            .complete_source_replacements()
            .expect("pending commit is not dispatched twice");
        reply.close();
        plane
            .drive_engine()
            .expect("finish commit after disconnect");
        assert_eq!(
            plane.tasks.get_gid(gid).expect("new catalog").sources()[0]
                .uri()
                .expect("available source"),
            "http://new.test/file.bin"
        );
        assert!(plane.pending_source_replacements.is_empty());
        assert_eq!(client.request_bytes(), 0);
        plane.shutdown().expect("shutdown");
        let recovered = directory.control_plane();
        assert_eq!(
            recovered
                .tasks
                .get_gid(gid)
                .expect("recovered sources")
                .sources()[0]
                .uri()
                .expect("available source"),
            "http://new.test/file.bin"
        );
        recovered.shutdown().expect("recovery shutdown");
    }

    #[test]
    fn signed_sources_export_without_secrets_and_recover_as_manageable_placeholders() {
        for paused in [false, true] {
            let directory = TestDirectory::new();
            let mut plane = directory.control_plane();
            let value = plane
                .call(
                    "aria2.addUri",
                    json!([
                        ["http://example.test/file.bin?token=secret-canary"], {"pause": paused}
                    ]),
                )
                .expect("signed source remains usable in memory");
            let gid: Gid = value.as_str().expect("gid").parse().expect("gid");
            let live = plane.tasks.get_gid(gid).expect("live task");
            assert!(
                live.sources()[0]
                    .uri()
                    .expect("live source")
                    .contains("secret-canary")
            );
            let export = plane
                .call("ariax.exportSession", json!([]))
                .expect("sanitized export");
            assert!(!export.to_string().contains("secret-canary"));
            assert_eq!(export["tasks"][0]["uris"], json!([]));
            assert_eq!(export["tasks"][0]["sources"][0]["uri"], Value::Null);
            assert_eq!(export["tasks"][0]["sources"][0]["needsCredentials"], true);
            assert!(!format!("{live:?}").contains("secret-canary"));
            plane.shutdown().expect("first shutdown");

            let mut recovered = directory.control_plane();
            let spec = recovered
                .tasks
                .get_gid(gid)
                .expect("placeholder task remains cataloged");
            assert_eq!(spec.sources()[0].uri(), None);
            let view = recovered
                .engine
                .scheduler()
                .task(gid)
                .expect("blocked task");
            assert!(view.conditions.needs_credentials);
            assert_eq!(view.desired_paused, paused);
            recovered
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .expect("status");
            recovered
                .call("aria2.getOption", json!([gid.to_string()]))
                .expect("options");
            assert!(
                recovered
                    .call(
                        "ariax.replaceSources",
                        json!([
                            gid.to_string(),
                            ["http://user:secret-canary@example.test/file"]
                        ])
                    )
                    .is_err()
            );
            assert!(
                recovered
                    .engine
                    .scheduler()
                    .task(gid)
                    .expect("still blocked")
                    .conditions
                    .needs_credentials
            );
            recovered
                .call(
                    "ariax.replaceSources",
                    json!([gid.to_string(), ["http://safe.test/file.bin"]]),
                )
                .expect("replace unavailable source");
            let view = recovered
                .engine
                .scheduler()
                .task(gid)
                .expect("unblocked task");
            assert!(!view.conditions.needs_credentials);
            assert_eq!(view.desired_paused, paused);
            recovered.shutdown().expect("second shutdown");
            let restarted = directory.control_plane();
            let view = restarted
                .engine
                .scheduler()
                .task(gid)
                .expect("durable source commit");
            assert!(!view.conditions.needs_credentials);
            assert_eq!(view.desired_paused, paused);
            restarted.shutdown().expect("final shutdown");

            let mut paths = vec![directory.root.clone()];
            while let Some(path) = paths.pop() {
                if path.is_dir() {
                    paths.extend(
                        fs::read_dir(path)
                            .expect("artifact directory")
                            .map(|entry| entry.expect("artifact").path()),
                    );
                } else {
                    let bytes = fs::read(path).expect("artifact bytes");
                    assert!(
                        !bytes
                            .windows(b"secret-canary".len())
                            .any(|window| window == b"secret-canary")
                    );
                }
            }
        }
    }

    fn import_document(count: usize) -> Value {
        json!({"formatVersion":3,"tasks": (0..count).map(|index| json!({
            "kind": "transfer",
            "uris": [format!("http://example.test/import-{index}.bin")],
            "options": {"split": "2"}
        })).collect::<Vec<_>>()})
    }

    #[test]
    fn thousand_task_bulk_progress_is_bounded_and_later_controls_win() {
        #[cfg(unix)]
        if ariax_runtime::native_process_handle_limit().is_some_and(|limit| limit < 4096) {
            let status = std::process::Command::new("bash").args([
                "-c", "ulimit -n 20000 && exec \"$1\" --exact http_control::tests::thousand_task_bulk_progress_is_bounded_and_later_controls_win --nocapture",
                "ariax-thousand-task-test",
            ]).arg(std::env::current_exe().expect("test executable")).status().expect("isolated handle limit");
            assert!(
                status.success(),
                "1,000-task test under sufficient native handle capacity"
            );
            return;
        }
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane_with_capacity(1000);
        let mut gids = Vec::with_capacity(1000);
        // Leave request budget for the growing scheduler and snapshot drafts
        // alongside the explicit v3 task-kind envelope. This test exercises
        // bulk control over 1,000 tasks, independent of import batch size.
        for batch in 0..20 {
            gids.extend(
                plane
                    .call("ariax.importSession", json!([import_document(50)]))
                    .unwrap_or_else(|error| panic!(
                        "bounded import batch {batch}: {error:?}; tasks={}, scheduler_bytes={}, draft_bytes={}, request_bytes={}, client_bytes={}, process={:?}",
                        plane.engine.scheduler().len(),
                        plane.engine.scheduler().estimated_clone_bytes(),
                        plane.engine.snapshot_reader().load().estimated_draft_bytes(),
                        plane.direct_client.request_bytes(),
                        plane.direct_client.bytes(),
                        plane.rpc_budgets.snapshot(),
                    ))
                    .as_array()
                    .expect("gids")
                    .iter()
                    .map(|gid| gid.as_str().expect("gid").parse::<Gid>().expect("gid")),
            );
        }
        let ControlReply::Deferred(mut bulk) = plane
            .begin_call_admitted("aria2.unpauseAll", json!([]), None)
            .expect("bulk admission")
        else {
            panic!("bulk continuation");
        };
        assert!(matches!(
            plane.begin_call_admitted("aria2.pauseAll", json!([]), None),
            Err(HttpControlError::Busy)
        ));
        // This debug-build fixture proves bounded turns and durable ordering.
        // Detect a stuck continuation independently of cumulative native I/O
        // and timer costs; optimized native benchmarks enforce latency limits.
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut progress_deadline = Instant::now() + Duration::from_secs(30);
        let mut overridden = false;
        let mut query_count = 0;
        let mut turns = 0;
        loop {
            let before = plane
                .engine
                .snapshot_reader()
                .load()
                .queue(QueueClass::Waiting)
                .len();
            plane.poll_once().expect("bounded progress");
            let after = plane
                .engine
                .snapshot_reader()
                .load()
                .queue(QueueClass::Waiting)
                .len();
            assert!(after <= before + 1, "one bulk target per turn");
            assert!(plane.turn.used <= 32, "one shared owner step budget");
            if after > before {
                progress_deadline = Instant::now() + Duration::from_secs(30);
            }
            if !overridden && after != 0 {
                plane
                    .call("aria2.pause", json!([gids[999].to_string()]))
                    .expect("later pause");
                plane
                    .call("aria2.remove", json!([gids[998].to_string()]))
                    .expect("later remove");
                overridden = true;
            }
            if turns % 64 == 0 {
                let query = plane.capture_query();
                let statuses = query
                    .tell_waiting(json!([0, 1000, ["gid", "status"]]))
                    .expect("substantial projection between turns");
                assert_eq!(
                    statuses.as_array().expect("statuses").len(),
                    if overridden { 999 } else { 1000 }
                );
                query_count += 1;
            }
            match bulk.try_recv() {
                Ok(result) => {
                    assert_eq!(result.expect("bulk completion"), "OK");
                    break;
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(error) => panic!("lost bulk continuation: {error}"),
            }
            assert!(
                Instant::now() < deadline && Instant::now() < progress_deadline,
                "bulk progress deadline: waiting={after}, turns={turns}, queries={query_count}, steps={}, progressed={}, idle={}, mutation={}, input={}, work={}, scheduler_bytes={}, draft_bytes={}, request_bytes={}, owner_bytes={}",
                plane.turn.used,
                plane.turn.progressed,
                plane.engine_idle(),
                plane.pending_mutation.is_some(),
                plane.pending_input.is_some(),
                plane.pending_work.is_some(),
                plane.engine.scheduler().estimated_clone_bytes(),
                plane
                    .engine
                    .snapshot_reader()
                    .load()
                    .estimated_draft_bytes(),
                plane.direct_client.request_bytes(),
                plane.owner_client.request_bytes(),
            );
            turns += 1;
            // Match the managed owner: productive turns yield without a timer.
            // Windows may round even a 50-us park up to a full scheduler tick.
            if plane.turn.progressed {
                std::thread::yield_now();
            } else {
                std::thread::park_timeout(CONTROL_PROGRESS_POLL);
            }
        }
        assert!(query_count > 10);
        let root = plane.engine.snapshot_reader().load();
        assert_eq!(root.queue(QueueClass::Waiting).len(), 998);
        assert_eq!(root.queue(QueueClass::Paused), &[gids[999]]);
        assert_eq!(root.queue(QueueClass::Stopped), &[gids[998]]);
        assert!(
            matches!(plane.session.execute(SessionCommand::ReadQueueOrder { state: SessionQueueState::Waiting }).expect("durable order"), SessionCommandResult::QueueOrder(gids) if gids == root.queue(QueueClass::Waiting))
        );
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn session_import_validates_every_member_before_journals_or_publication() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        for options in [
            json!({"out": "../escape"}),
            json!({"split": "0"}),
            json!({"pause": "invalid"}),
            json!({"verify-mirror-identity": true}),
            json!({"rpc-secret": "secret-canary"}),
        ] {
            let mut document = import_document(2);
            document["tasks"][1]["options"] = options;
            assert!(
                plane
                    .call("ariax.importSession", json!([document]))
                    .is_err()
            );
            assert!(plane.tasks.is_empty());
            assert!(plane.engine.snapshot_reader().load().is_empty());
            assert!(
                fs::read_dir(&directory.journals)
                    .expect("journal directory")
                    .next()
                    .is_none()
            );
            assert!(
                matches!(plane.session.execute(SessionCommand::ReadTasks).expect("tasks"), SessionCommandResult::Tasks(tasks) if tasks.is_empty())
            );
        }
        assert!(
            plane
                .call("ariax.importSession", json!([import_document(17)]))
                .is_err()
        );
        assert!(plane.tasks.is_empty());
        let result = plane
            .call("ariax.importSession", json!([import_document(3)]))
            .expect("valid batch after rejection");
        assert_eq!(result.as_array().expect("gids").len(), 3);
        for gid in result.as_array().expect("gids") {
            assert_eq!(
                plane
                    .call("aria2.tellStatus", json!([gid]))
                    .expect("imported status")["status"],
                "paused"
            );
        }
        plane.shutdown().expect("shutdown");
        let recovered = directory.control_plane();
        assert_eq!(recovered.tasks.len(), 3);
        recovered.shutdown().expect("recovered shutdown");
    }

    #[test]
    fn disconnected_import_retains_credit_and_shutdown_completes_every_member() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let client = plane.rpc_budgets.client().expect("client");
        let request = client.try_request(128).expect("request");
        let ControlReply::Deferred(reply) = plane
            .begin_call_admitted(
                "ariax.importSession",
                json!([import_document(3)]),
                Some(request.clone()),
            )
            .expect("begin import")
        else {
            panic!("deferred import");
        };
        assert!(plane.engine.snapshot_reader().load().is_empty());
        drop(reply);
        drop(request);
        assert_eq!(client.outstanding_requests(), 1);
        assert!(client.request_bytes() > 0);
        assert!(plane.shutdown().expect("drained import").is_clean());
        assert_eq!(client.request_bytes(), 0);
        assert_eq!(client.outstanding_requests(), 0);
        let recovered = directory.control_plane();
        assert_eq!(recovered.tasks.len(), 3);
        assert_eq!(recovered.engine.snapshot_reader().load().len(), 3);
        recovered.shutdown().expect("shutdown");
    }

    #[test]
    fn interrupted_import_recovers_all_committed_metadata_and_skips_orphan_destinations() {
        for committed in [false, true] {
            let directory = TestDirectory::new();
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "http_control::tests::interrupted_import_child",
                        "--nocapture",
                    ])
                    .env("ARIAX_IMPORT_CONTROL_ROOT", &directory.root)
                    .env(
                        "ARIAX_IMPORT_CONTROL_COMMITTED",
                        if committed { "true" } else { "false" },
                    )
                    .status()
                    .expect("crash child");
            assert_eq!(status.code(), Some(77));
            let mut recovered = directory.control_plane();
            assert_eq!(recovered.tasks.len(), if committed { 3 } else { 0 });
            let gid = add_paused(&mut recovered);
            assert!(
                recovered
                    .tasks
                    .get_gid(gid)
                    .expect("subsequent add")
                    .task()
                    .get()
                    > 3
            );
            recovered.shutdown().expect("shutdown");
        }
    }

    #[test]
    fn interrupted_import_child() {
        let Some(root) = std::env::var_os("ARIAX_IMPORT_CONTROL_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let directory = TestDirectory {
            control: root.join("control"),
            output: root.join("output"),
            journals: root.join("control/http-journals"),
            root,
        };
        let committed =
            std::env::var("ARIAX_IMPORT_CONTROL_COMMITTED").expect("crash point") == "true";
        let mut plane = directory.control_plane();
        let request = plane.direct_client.try_request(128).expect("request");
        let _reply = plane
            .begin_call_admitted(
                "ariax.importSession",
                json!([import_document(3)]),
                Some(request),
            )
            .expect("begin import");
        let deadline = Instant::now() + Duration::from_secs(5);
        while plane.pending_admission.is_some() {
            assert!(Instant::now() < deadline, "prepare import crash point");
            plane.poll_admission().expect("stage import");
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
        if committed {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !plane.engine.is_idle() {
                assert!(!matches!(
                    plane.engine.poll_at(MonotonicInstant::now()),
                    ariax_runtime::SchedulerDriverPoll::Faulted(_)
                ));
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_micros(50));
            }
            assert_eq!(plane.engine.snapshot_reader().load().len(), 1);
        }
        assert!(
            matches!(plane.session.execute(SessionCommand::ReadTasks).expect("atomic metadata"), SessionCommandResult::Tasks(tasks) if tasks.len() == if committed { 3 } else { 0 })
        );
        std::process::exit(77);
    }

    #[test]
    fn aria2_import_honors_pause_and_json_import_preserves_credential_placeholders() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let result = plane.call("ariax.importSession", json!([
            "http://example.test/one.bin\n  pause=false\n  split=2\nhttp://example.test/two.bin?token=secret-canary\n  pause=true\n", "aria2"
        ])).expect("aria2 import");
        assert_eq!(
            plane
                .call("aria2.tellStatus", json!([result[0]]))
                .expect("waiting status")["status"],
            "waiting"
        );
        assert_eq!(
            plane
                .call("aria2.tellStatus", json!([result[1]]))
                .expect("paused status")["status"],
            "paused"
        );
        let export = plane
            .call("ariax.exportSession", json!([]))
            .expect("sanitized export");
        assert!(!export.to_string().contains("secret-canary"));
        let imported = plane
            .call("ariax.importSession", json!([export]))
            .expect("JSON reimport");
        let credential_count = imported
            .as_array()
            .expect("gids")
            .iter()
            .filter(|gid| {
                let gid: Gid = gid.as_str().expect("gid").parse().expect("gid");
                plane
                    .engine
                    .scheduler()
                    .task(gid)
                    .expect("task")
                    .conditions
                    .needs_credentials
            })
            .count();
        assert_eq!(credential_count, 1);
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn scheduler_scratch_rejects_before_journal_creation_and_refunds_after_success() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let client = plane.rpc_budgets.client().expect("client");
        let held = client.try_request(0).expect("retained command");
        held.reserve(crate::MAX_RPC_CLIENT_REQUEST_BYTES - 160 * 1024)
            .expect("occupy request bytes");
        let request = client.try_request(0).expect("next request");
        let baseline = client.request_bytes();
        assert!(matches!(
            plane.call_admitted(
                "aria2.addUri",
                json!([["http://example.test/rejected.bin"], {"pause": true}]),
                Some(request.clone()),
            ),
            Err(HttpControlError::Busy)
        ));
        assert!(plane.tasks.is_empty());
        assert!(
            fs::read_dir(&directory.journals)
                .expect("journals")
                .next()
                .is_none()
        );
        assert_eq!(client.request_bytes(), baseline);
        assert_eq!(
            plane
                .call_admitted("aria2.tellActive", json!([]), Some(request.clone()))
                .expect("query without scheduler scratch"),
            json!([])
        );
        drop(held);
        plane
            .call_admitted(
                "aria2.addUri",
                json!([["http://example.test/accepted.bin"], {"pause": true}]),
                Some(request.clone()),
            )
            .expect("admit after credit release");
        drop(request);
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.request_bytes(), 0);
        assert!(plane.pending_work.is_none());
        plane.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn pending_scheduler_chain_retains_credit_and_queries_use_published_state() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let client = plane.rpc_budgets.client().expect("client");
        let request = client.try_request(1).expect("request");
        let input = plane
            .reserve_command_memory("aria2.unpause", &json!([gid.to_string()]), Some(&request))
            .expect("typed input");
        let work = plane
            .reserve_scheduler_work(input.as_ref(), 0)
            .expect("scheduler scratch");
        let command = SchedulerCommand::Resume { gid };
        let now = MonotonicInstant::now();
        let mut simulation = plane.engine.scheduler().clone();
        let outcome = simulation
            .execute_command_at(command.clone(), now)
            .expect("preview");
        plane
            .prepare_outcome_plans(&mut simulation, outcome.effects, None)
            .expect("plans");
        plane
            .engine
            .execute_command_at(command, now)
            .expect("pending driver input");
        drop(simulation);
        plane
            .retain_pending_work(Some(work))
            .expect("retain pending work");
        drop(input);
        drop(request);
        assert!(matches!(
            plane.drive_engine_until(Instant::now()),
            Err(HttpControlError::Busy)
        ));
        assert!(plane.pending_work.is_some());
        assert_eq!(client.outstanding_requests(), 1);
        assert!(client.request_bytes() > 128 * 1024);

        let backend = HttpControlBackend::new(plane);
        let plane = backend.plane();
        let status = backend
            .call("aria2.tellStatus", json!([gid.to_string()]))
            .await
            .expect("query during pending persistence");
        assert_eq!(status["status"], "paused");
        let mut owner = plane.lock().await;
        assert!(owner.pending_work.is_some());
        owner.drive_engine().expect("finish pending chain");
        assert!(owner.pending_work.is_none());
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.request_bytes(), 0);
        assert_eq!(
            owner
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .expect("new root")["status"],
            "waiting"
        );
        drop(owner);
        drop(backend);
        let owner = Arc::try_unwrap(plane).expect("sole owner").into_inner();
        owner.shutdown().expect("shutdown");
    }

    #[test]
    fn owner_budget_rejection_preserves_events_and_direct_requests_do_not_block_progress() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let task = plane.tasks.get_gid(gid).expect("task").task();
        let runtime = plane.engine.runtime_handle();
        runtime.enqueue_allocation_for_test(task, gid, Generation::INITIAL);
        let event = runtime.take_allocation().expect("allocation").succeeded();
        let expected = event.event().clone();
        assert!(runtime.try_submit_event(event).is_ok());
        let held = plane.owner_client.try_request(0).expect("owner pressure");
        held.reserve(crate::MAX_RPC_CLIENT_REQUEST_BYTES - 64 * 1024)
            .expect("occupy owner bytes");
        assert!(matches!(plane.poll_once(), Err(HttpControlError::Busy)));
        assert_eq!(
            runtime.poll_event_at(MonotonicInstant::now()),
            Some(expected)
        );
        drop(held);
        let direct = (0..crate::MAX_RPC_CLIENT_REQUESTS)
            .map(|_| plane.direct_client.try_request(0).expect("direct request"))
            .collect::<Vec<_>>();
        plane.poll_once().expect("independent owner progress");
        drop(direct);
        assert_eq!(plane.owner_client.outstanding_requests(), 0);
        assert_eq!(plane.owner_client.request_bytes(), 0);
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn borrowed_source_results_reject_oversize_before_building_json_and_queries_remain_usable() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        let original = plane.tasks.get_gid(gid).expect("task");
        for count in [2, 512] {
            let spec = HttpTaskSpec::new(
                original.task(),
                gid,
                (0..count)
                    .map(|index| format!("http://example.test/file/{index}/{}", "x".repeat(6000))),
                original.output_root().clone(),
                original.output().clone(),
                original.options().clone(),
                false,
            )
            .expect("bounded source catalog");
            plane.tasks.replace(spec).expect("test source catalog");
            for method in [
                "aria2.getUris",
                "aria2.getFiles",
                "aria2.getServers",
                "ariax.exportSession",
            ] {
                let params = if method == "ariax.exportSession" {
                    json!([])
                } else {
                    json!([gid.to_string()])
                };
                let result = plane.call(method, params);
                if count == 2 {
                    let value = result.expect("small borrowed result");
                    assert!(crate::rpc_json::owned_value_bytes(&value) <= RESULT_VALUE_BYTES);
                    if method == "aria2.getFiles" {
                        assert_eq!(value[0]["index"], "1");
                        assert_eq!(value[0]["uris"].as_array().expect("uris").len(), 2);
                    }
                } else {
                    assert!(
                        matches!(result, Err(HttpControlError::ResponseTooLarge)),
                        "{method}: {result:?}"
                    );
                }
            }
            assert_eq!(
                plane
                    .call("aria2.tellStatus", json!([gid.to_string(), ["gid"]]))
                    .expect("subsequent status"),
                json!({"gid": gid.to_string()})
            );
        }
        plane
            .tasks
            .replace((*original).clone())
            .expect("restore persisted test catalog");
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn typed_command_forecast_rejects_before_admission_and_accounts_existing_change_uri_sources() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let client = plane.rpc_budgets.client().expect("client");
        let lease = client.try_request(1).expect("request");
        let large_uri = format!("http://example.test/file?{}", "x".repeat(700_000));
        assert!(matches!(
            plane.call_admitted("aria2.addUri", json!([[large_uri]]), Some(lease.clone())),
            Err(HttpControlError::Busy)
        ));
        assert!(plane.tasks.is_empty());
        assert!(
            fs::read_dir(&directory.journals)
                .expect("journal directory")
                .next()
                .is_none()
        );
        let gid = add_paused(&mut plane);
        let original = plane.tasks.get_gid(gid).expect("task");
        let spec = HttpTaskSpec::new(
            original.task(),
            gid,
            (0..128).map(|index| {
                format!(
                    "http://example.test/file?index={index}&{}",
                    "x".repeat(6000)
                )
            }),
            original.output_root().clone(),
            original.output().clone(),
            original.options().clone(),
            false,
        )
        .expect("large existing source set");
        plane.tasks.replace(spec).expect("catalog");
        assert!(matches!(
            plane.call_admitted(
                "aria2.changeUri",
                json!([gid.to_string(), 1, [], ["http://other.test/file"]]),
                Some(lease.clone())
            ),
            Err(HttpControlError::Busy)
        ));
        assert!(plane.pending_source_replacements.is_empty());
        plane
            .call_admitted(
                "aria2.changeOption",
                json!([gid.to_string(), {"split": 3}]),
                Some(lease.clone()),
            )
            .expect("option-only changes share sources");
        assert_eq!(
            plane
                .tasks
                .get_gid(gid)
                .expect("unchanged sources")
                .sources()
                .len(),
            128
        );
        assert!(client.request_bytes() < 256 * 1024);
        drop(lease);
        assert_eq!(client.request_bytes(), 0);
        plane.shutdown().expect("shutdown");
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

    pub(super) fn attach_loopback_worker(plane: &mut HttpControlPlane, directory: &TestDirectory) {
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
                scheduling: plane.scheduling_policy(),
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
        let client = plane.rpc_budgets.client().expect("RPC client");
        let lease = client.try_request(512).expect("command lease");
        let context = crate::RpcClientContext::default().with_request(lease);
        let shared = Arc::new(Mutex::new(plane));
        let caller = shared.clone();
        let request = tokio::spawn(async move {
            HttpControlPlane::call_shared_with_context(
                &caller,
                "ariax.replaceSources",
                json!([gid.to_string(), ["http://new.test/file.bin"]]),
                context,
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
        assert_eq!(client.outstanding_requests(), 1);
        assert!(client.request_bytes() > 0);
        drain.add_permits(1);
        let backend = HttpControlBackend::from_shared(shared.clone()).await;
        backend
            .drain_control_runtime()
            .await
            .expect("finish disconnected source mutation");
        drop(backend);
        let plane = Arc::try_unwrap(shared)
            .expect("owner remains available")
            .into_inner();
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.request_bytes(), 0);
        assert_eq!(
            plane
                .tasks
                .get_gid(gid)
                .expect("committed catalog")
                .sources()[0]
                .uri()
                .expect("available source"),
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
            plane.engine.is_idle()
                && plane
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
                None,
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
            recovered.tasks.get_gid(gid).expect("old sources").sources()[0]
                .uri()
                .expect("available source"),
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
                loop {
                    let owner = shared.lock().await;
                    let quiescing =
                        owner.pending_source_replacements.contains_key(&gid)
                            && owner.engine.snapshot_reader().load().task(gid).is_some_and(
                                |task| task.snapshot.wire_status() == Ok(Aria2Status::Waiting),
                            );
                    drop(owner);
                    if quiescing {
                        break;
                    }
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
                        owner.tasks.get_gid(gid).expect("old catalog").sources()[0]
                            .uri()
                            .expect("available source"),
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
                let backend = HttpControlBackend::from_shared(shared.clone()).await;
                backend
                    .drain_control_runtime()
                    .await
                    .expect("drain source control runtime");
                drop(backend);
                let owner = Arc::try_unwrap(shared).expect("sole owner").into_inner();
                let expected_uri = if user_control == Some("aria2.remove") {
                    "http://example.test/file.bin"
                } else {
                    "http://new.test/file.bin"
                };
                assert_eq!(
                    owner.tasks.get_gid(gid).expect("catalog").sources()[0]
                        .uri()
                        .expect("available source"),
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
                        .uri()
                        .expect("available source"),
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
        rate_patch_changes_credit_without_restarting_and_recovers(false).await;
    }

    #[tokio::test]
    async fn allocating_rate_patch_updates_worker_credit_before_allocation_acknowledgement() {
        rate_patch_changes_credit_without_restarting_and_recovers(true).await;
    }

    async fn rate_patch_changes_credit_without_restarting_and_recovers(
        before_allocation_acknowledgement: bool,
    ) {
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
        plane
            .supervisor
            .as_mut()
            .expect("supervisor")
            .poll_once(MonotonicInstant::now())
            .expect("start worker without consuming its allocation acknowledgement");
        started.notified().await;
        if before_allocation_acknowledgement {
            assert_eq!(
                plane.engine.scheduler().task(gid).expect("task").state,
                ariax_core::TaskState::Allocating
            );
            assert_eq!(
                plane
                    .engine
                    .snapshot_reader()
                    .load()
                    .task(gid)
                    .expect("published task")
                    .snapshot
                    .wire_status(),
                Ok(Aria2Status::Waiting)
            );
        } else {
            poll_until(&mut plane, |plane| {
                let task = plane.engine.scheduler().task(gid).expect("task");
                task.state == ariax_core::TaskState::Active && task.pending_barrier.is_none()
            })
            .await;
        }
        let task_id = plane.tasks.get_gid(gid).expect("task").task();
        let path = ariax_runtime::RatePath {
            host: 1,
            task: task_id.get(),
            stream: 1,
        };
        let requested = NonZeroUsize::new(8).expect("quantum");
        let permit = rate
            .try_acquire(path, requested)
            .expect("old rate")
            .expect("old permit");
        assert_eq!(permit.reserved_bytes(), 4);
        drop(permit);
        let before = plane
            .call("aria2.getOption", json!([gid.to_string()]))
            .expect("options");
        for patch in [
            json!({"piece-length": "1M"}),
            json!({"out": "renamed.bin"}),
            json!({"allow-overwrite": true}),
        ] {
            assert!(matches!(
                plane.call("aria2.changeOption", json!([gid.to_string(), patch])),
                Err(HttpControlError::OptionPatchRejected(_))
            ));
        }
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
        poll_until(&mut plane, |plane| {
            plane.engine.scheduler().task(gid).expect("task").state == ariax_core::TaskState::Active
        })
        .await;
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
        let payloads = replay_journal_payloads(&directory, task_id, gid, Generation::INITIAL);
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
            recovered
                .attach_rpc_budgets(crate::RpcBudgets::process_default())
                .expect("recovered patches have no live client lease");
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
        option_patch_survives_delayed_drain_and_promotes_exactly_once(false).await;
    }

    #[tokio::test]
    async fn allocating_option_patch_survives_delayed_drain_and_promotes_exactly_once() {
        option_patch_survives_delayed_drain_and_promotes_exactly_once(true).await;
    }

    async fn option_patch_survives_delayed_drain_and_promotes_exactly_once(
        before_allocation_acknowledgement: bool,
    ) {
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
            plane
                .supervisor
                .as_mut()
                .expect("supervisor")
                .poll_once(MonotonicInstant::now())
                .expect("start worker without consuming its allocation acknowledgement");
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .expect("worker started");
            if before_allocation_acknowledgement {
                assert_eq!(
                    plane.engine.scheduler().task(gid).expect("task").state,
                    ariax_core::TaskState::Allocating
                );
            } else {
                poll_until(&mut plane, |plane| {
                    let task = plane.engine.scheduler().task(gid).expect("task");
                    task.state == ariax_core::TaskState::Active && task.pending_barrier.is_none()
                })
                .await;
            }
            let old = option_mirror(&plane, gid, OptionsSnapshotScope::CurrentGeneration);
            let client = plane.rpc_budgets.client().expect("RPC client");
            let request = client.try_request(512).expect("option command request");
            assert_eq!(
                plane
                    .call_admitted(
                        "aria2.changeOption",
                        json!([gid.to_string(), patch, {"restart":true}]),
                        Some(request)
                    )
                    .expect("accept patch"),
                "OK"
            );
            assert_eq!(client.outstanding_requests(), 1);
            assert!(matches!(
                plane.attach_rpc_budgets(crate::RpcBudgets::process_default()),
                Err(HttpControlError::Busy)
            ));
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
            assert_eq!(client.outstanding_requests(), 0);
            assert_eq!(client.request_bytes(), 0);
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
        assert_eq!(
            task.sources()[0].uri().expect("available source"),
            "http://example.test/file.bin"
        );
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
            Some(crate::HttpContentChecksum::sha256([0xab; 32]))
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

    async fn serve_slot_file(
        retry: bool,
    ) -> (
        String,
        Arc<std::sync::Mutex<Vec<Instant>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = attempts.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let recorded = recorded.clone();
                connections.spawn(async move {
                    let mut request = Vec::new();
                    let mut byte = [0];
                    while !request.ends_with(b"\r\n\r\n") {
                        if stream.read_exact(&mut byte).await.is_err() { return; }
                        request.push(byte[0]);
                        assert!(request.len() < 8192);
                    }
                    let text = String::from_utf8(request).unwrap();
                    let (start, end) = text.lines().find_map(|line| {
                        let value = line.strip_prefix("range: bytes=").or_else(|| line.strip_prefix("Range: bytes="))?;
                        let (start, end) = value.split_once('-')?;
                        Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                    }).unwrap();
                    let slow = text.starts_with("GET /slow.bin ") && end > start;
                    let attempt = if slow {
                        let mut attempts = recorded.lock().unwrap();
                        attempts.push(Instant::now());
                        attempts.len()
                    } else { 0 };
                    if slow && retry && attempt == 1 {
                        let _ = stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nRetry-After: 1\r\nConnection: close\r\n\r\n").await;
                        return;
                    }
                    let length = end - start + 1;
                    let head = format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {length}\r\nContent-Range: bytes {start}-{end}/1048576\r\nETag: \"slots-v1\"\r\nConnection: close\r\n\r\n");
                    if stream.write_all(head.as_bytes()).await.is_err() { return; }
                    if slow && !retry {
                        let _ = stream.read(&mut byte).await;
                    } else {
                        let _ = stream.write_all(&vec![0x63; length]).await;
                    }
                });
                while connections.try_join_next().is_some() {}
            }
        });
        (uri, attempts, server)
    }

    async fn progress_until(
        plane: &mut HttpControlPlane,
        ready: impl Fn(&HttpControlPlane) -> bool,
    ) {
        let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
        loop {
            plane.poll_once().expect("control progress");
            if plane.engine.is_idle() && ready(plane) {
                return;
            }
            assert!(Instant::now() < deadline, "control progress deadline");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn add_slot_task(plane: &mut HttpControlPlane, uri: &str, file: &str) -> Gid {
        plane
            .call(
                "aria2.addUri",
                json!([[format!("{uri}/{file}")], {
                    "split":1, "endgame-max-duplicates":0,
                    "retry-wait":1, "retry-max-wait":1, "retry-after-min":1,
                    "retry-after-max":1, "retry-backoff":"fixed"
                }]),
            )
            .expect("add slot task")
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }

    #[tokio::test]
    async fn every_advertised_method_executes_or_reports_its_disabled_protocol() {
        use crate::{
            HttpRpcBackend, RpcAuthPolicy, RpcClientContext, RpcCompatibility, RpcDispatcher,
        };
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        plane
            .configure_session_export(SessionExportConfig {
                path: directory.root.join("catalog-session.json"),
                format: crate::SessionFormat::Json,
                interval: None,
            })
            .unwrap();
        let gid = add_paused(&mut plane).to_string();
        let second = add_paused(&mut plane).to_string();
        let context = RpcClientContext::with_events(plane.event_broker(), false).unwrap();
        let backend = Arc::new(HttpControlBackend::new(plane));
        let dispatcher = RpcDispatcher::new(backend.clone(), RpcAuthPolicy::default())
            .with_compatibility(RpcCompatibility::Strict);
        let mut covered = std::collections::BTreeSet::new();
        for method in crate::RPC_METHODS {
            let error = dispatcher
                .call_with_context(method, json!([{"invalidArgument":true}]), context.clone())
                .await
                .expect_err("bad arguments must reject");
            if matches!(*method, "aria2.addTorrent" | "aria2.getPeers")
                || (*method == "aria2.addMetalink" && !cfg!(feature = "metalink"))
            {
                assert_eq!(error.data.unwrap()["code"], "ProtocolFeatureUnavailable");
                covered.insert(*method);
            } else {
                assert_ne!(
                    error.code, -32601,
                    "advertised method has no handler: {method}"
                );
            }
        }
        macro_rules! call {
            ($method:expr, $params:expr) => {{
                covered.insert($method);
                dispatcher
                    .call_with_context($method, $params, context.clone())
                    .await
                    .unwrap_or_else(|error| panic!("{}: {error}", $method))
            }};
        }
        #[cfg(feature = "metalink")]
        {
            use base64ct::Encoding;
            let xml=b"<metalink xmlns='urn:ietf:params:xml:ns:metalink'><file name='catalog-metalink'><size>0</size><url>http://example.test/empty</url></file></metalink>";
            call!(
                "aria2.addMetalink",
                json!([base64ct::Base64::encode_string(xml),{"pause":true}])
            );
        }
        assert!(
            dispatcher
                .call_with_context(
                    "ariax.approveHostKey",
                    json!([gid, "00".repeat(16), "00".repeat(32)]),
                    context.clone()
                )
                .await
                .is_err()
        );
        covered.insert("ariax.approveHostKey");
        for method in [
            "system.listMethods",
            "system.listNotifications",
            "aria2.tellActive",
            "aria2.getGlobalOption",
            "aria2.getVersion",
            "aria2.getSessionInfo",
            "aria2.getGlobalStat",
            "ariax.getDiagnostics",
        ] {
            call!(method, json!([]));
        }
        for method in [
            "aria2.tellStatus",
            "aria2.getUris",
            "aria2.getFiles",
            "aria2.getServers",
            "aria2.getOption",
        ] {
            call!(method, json!([gid]));
        }
        call!("aria2.tellWaiting", json!([0, 10]));
        call!("aria2.tellStopped", json!([0, 10]));
        call!(
            "system.multicall",
            json!([[{"methodName":"aria2.getVersion","params":[]}]])
        );
        call!("aria2.changeOption", json!([gid,{"split":2}]));
        call!("aria2.changeGlobalOption", json!([{"timeout":30}]));
        call!("ariax.checkConfig", json!(["split=3\n"]));
        call!("ariax.reloadConfig", json!(["split=3\n"]));
        call!("ariax.dumpConfig", json!([]));
        call!(
            "aria2.changeUri",
            json!([gid, 1, [], ["http://example.test/replacement.bin"]])
        );
        call!(
            "ariax.replaceSources",
            json!([gid, ["http://example.test/replacement.bin"]])
        );
        call!("aria2.changePosition", json!([gid, 0, "POS_SET"]));
        call!("aria2.pause", json!([gid]));
        call!("aria2.forcePause", json!([gid]));
        call!("aria2.unpause", json!([gid]));
        call!("aria2.pauseAll", json!([]));
        call!("aria2.unpauseAll", json!([]));
        call!("aria2.forcePauseAll", json!([]));
        let subscription = call!(
            "ariax.subscribe",
            json!([16,65536,{"methods":["ariax.onStatus"]}])
        );
        call!(
            "ariax.pollEvents",
            json!([subscription["subscriptionId"], 16])
        );
        call!("ariax.unsubscribe", json!([subscription["subscriptionId"]]));
        call!("ariax.setEventFilter", json!([{"gids":[gid]}]));
        let mut exported = call!("ariax.exportSession", json!([]));
        // Importing into this same root must choose fresh safe output names.
        for (index, task) in exported["tasks"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
        {
            task["options"]["out"] = json!(format!("catalog-import-{index}"));
        }
        call!("ariax.importSession", json!([exported]));
        call!("aria2.saveSession", json!([]));
        call!("aria2.remove", json!([gid]));
        call!("aria2.removeDownloadResult", json!([gid]));
        call!("aria2.forceRemove", json!([second]));
        call!("aria2.purgeDownloadResult", json!([]));
        call!(
            "aria2.addUri",
            json!([["http://example.test/catalog.bin"],{"pause":true}])
        );
        call!("aria2.shutdown", json!([]));
        call!("aria2.forceShutdown", json!([]));
        assert_eq!(covered, crate::RPC_METHODS.iter().copied().collect());
        drop(dispatcher);
        drop(context);
        Arc::try_unwrap(backend)
            .ok()
            .expect("sole backend")
            .try_into_control_plane()
            .ok()
            .expect("sole plane")
            .shutdown_async()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn slow_remote_workers_free_slots_and_user_controls_override_cooldown() {
        for policy in ["off", "demote", "pause"] {
            let directory = TestDirectory::new();
            let (uri, _, server) = serve_slot_file(false).await;
            let mut plane = directory.control_plane_with_active_limit(1);
            plane
                .call(
                    "aria2.changeGlobalOption",
                    json!([{
                        "slow-slot-policy":policy, "slow-slot-grace-period":1,
                        "slow-slot-min-active-time":0, "slow-slot-readmit-after":60,
                        "slow-slot-max-demotions":1
                    }]),
                )
                .expect("configure policy");
            attach_loopback_worker(&mut plane, &directory);
            let slow = add_slot_task(&mut plane, &uri, "slow.bin");
            progress_until(&mut plane, |plane| {
                let task = plane.engine.scheduler().task(slow).unwrap();
                plane.stats.get(task.task_id).is_some_and(|stats| {
                    let stats = stats.snapshot();
                    stats.network_phase && stats.active_connections == 1 && !stats.local_pressure
                })
            })
            .await;
            let generation = plane.engine.scheduler().task(slow).unwrap().generation;
            let waiter = add_slot_task(&mut plane, &uri, "waiting.bin");
            // Advance only the pure policy clock; the HTTP worker and persistence
            // execute normally. This exercises a sustained interval without a long sleep.
            Arc::make_mut(&mut plane.slow_observations).clear();
            plane.next_slow_sample = None;
            let start = MonotonicInstant::now();
            for step in 0..=10 {
                plane
                    .poll_slow_slots_at(
                        start
                            .checked_add(Duration::from_millis(step * 100))
                            .unwrap(),
                    )
                    .expect("policy sample");
            }
            if policy == "off" {
                assert!(plane.slow_observations.is_empty());
                assert_eq!(
                    plane.engine.scheduler().task(slow).unwrap().state,
                    TaskState::Active
                );
                assert_eq!(
                    plane.engine.scheduler().task(waiter).unwrap().state,
                    TaskState::Waiting
                );
            } else {
                progress_until(&mut plane, |plane| {
                    let slow = plane.engine.scheduler().task(slow).unwrap();
                    slow.pending_barrier.is_none()
                        && plane.engine.scheduler().task(waiter).unwrap().state
                            == TaskState::StoppedResult
                })
                .await;
                let status = plane
                    .call("aria2.tellStatus", json!([slow.to_string()]))
                    .unwrap();
                assert_eq!(
                    status["slotState"],
                    if policy == "demote" {
                        "waitingSlow"
                    } else {
                        "pausedSlow"
                    }
                );
                assert_eq!(status["slotReason"], "remoteSlow");
                assert_eq!(
                    fs::read(directory.output.join("waiting.bin")).unwrap(),
                    vec![0x63; 1024 * 1024]
                );
                assert!(plane.shutdown_async().await.unwrap().is_clean());
                plane = directory.control_plane_with_active_limit(1);
                let restored = plane.engine.scheduler().task(slow).unwrap();
                assert_eq!(
                    restored.state,
                    if policy == "demote" {
                        TaskState::WaitingSlow
                    } else {
                        TaskState::PausedSlow
                    }
                );
                assert_eq!(restored.slow_demotion_count, u32::from(policy == "demote"));
                attach_loopback_worker(&mut plane, &directory);
                plane
                    .call("aria2.unpause", json!([slow.to_string()]))
                    .expect("user overrides cooldown");
                progress_until(&mut plane, |plane| {
                    plane.engine.scheduler().task(slow).unwrap().state == TaskState::Active
                })
                .await;
                let resumed = plane.engine.scheduler().task(slow).unwrap();
                assert!(resumed.generation > generation);
                assert_eq!(resumed.slow_demotion_count, 0);
            }
            assert!(plane.shutdown_async().await.unwrap().is_clean());
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn retry_wait_slot_policy_uses_real_worker_deadlines() {
        for policy in ["true", "false", "auto"] {
            let directory = TestDirectory::new();
            let (uri, attempts, server) = serve_slot_file(true).await;
            let mut plane = directory.control_plane_with_active_limit(1);
            plane
                .call(
                    "aria2.changeGlobalOption",
                    json!([{"retry-wait-consumes-slot":policy}]),
                )
                .unwrap();
            attach_loopback_worker(&mut plane, &directory);
            let slow = add_slot_task(&mut plane, &uri, "slow.bin");
            let waiter = add_slot_task(&mut plane, &uri, "waiting.bin");
            progress_until(&mut plane, |plane| {
                let task = plane.engine.scheduler().task(slow).unwrap();
                if policy == "true" {
                    plane
                        .stats
                        .get(task.task_id)
                        .is_some_and(|stats| stats.snapshot().retry_wait_until.is_some())
                } else {
                    task.state == TaskState::RetryWait && !task.slot.owns_slot()
                }
            })
            .await;
            let status = plane
                .call("aria2.tellStatus", json!([slow.to_string()]))
                .unwrap();
            assert_eq!(status["slotState"], "retryWait");
            assert_eq!(status["retryWaitConsumesSlot"], policy == "true");
            assert_eq!(attempts.lock().unwrap().len(), 1);
            if policy == "true" {
                assert_eq!(
                    plane.engine.scheduler().task(waiter).unwrap().state,
                    TaskState::Waiting
                );
            } else {
                progress_until(&mut plane, |plane| {
                    plane.engine.scheduler().task(waiter).unwrap().state == TaskState::StoppedResult
                })
                .await;
            }
            if policy == "false" {
                assert!(plane.shutdown_async().await.unwrap().is_clean());
                plane = directory.control_plane_with_active_limit(1);
                // Recovery retains the persisted deadline even if process policy resets.
                attach_loopback_worker(&mut plane, &directory);
                assert_eq!(attempts.lock().unwrap().len(), 1);
            }
            progress_until(&mut plane, |plane| {
                [slow, waiter].into_iter().all(|gid| {
                    plane.engine.scheduler().task(gid).unwrap().state == TaskState::StoppedResult
                })
            })
            .await;
            let status = plane
                .call("aria2.tellStatus", json!([slow.to_string()]))
                .unwrap();
            assert_eq!(status["status"], "complete");
            let times = attempts.lock().unwrap().clone();
            assert_eq!(times.len(), 2);
            assert!(
                times[1].duration_since(times[0]) >= Duration::from_millis(990),
                "{policy}: slot release shortened the durable retry wait to {:?}: {status}",
                times[1].duration_since(times[0])
            );
            assert_eq!(
                fs::read(directory.output.join("slow.bin")).unwrap(),
                vec![0x63; 1024 * 1024]
            );
            assert!(plane.shutdown_async().await.unwrap().is_clean());
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn live_supervisor_completes_http_task_and_persists_terminal_evidence() {
        let directory = TestDirectory::new();
        let data: Arc<[u8]> = vec![0x5a; 1024 * 1024].into();
        let (uri, server) = serve_control_file(Arc::clone(&data)).await;
        let mut plane = directory.control_plane();
        attach_loopback_worker(&mut plane, &directory);
        let checksum =
            crate::HttpContentChecksum::sha256(Sha256::digest(data.as_ref()).into()).canonical();
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
    async fn explicit_layout_changes_download_new_placement_and_geometry_without_clobbering() {
        for case in ["placement", "geometry", "occupied"] {
            let directory = TestDirectory::new();
            let data: Arc<[u8]> = vec![0x37; 2 * 1024 * 1024].into();
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let uri = format!(
                "http://{}/original.bin",
                listener.local_addr().expect("address")
            );
            let served = Arc::new(AtomicU64::new(0));
            let server_data = data.clone();
            let server = tokio::spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    let (mut stream, _) = listener.accept().await.expect("accept");
                    let data = server_data.clone();
                    let served = served.clone();
                    connections.spawn(async move {
                        let mut request = Vec::new();
                        let mut byte = [0];
                        while !request.ends_with(b"\r\n\r\n") {
                            if stream.read_exact(&mut byte).await.is_err() { return; }
                            request.push(byte[0]);
                            assert!(request.len() < 8192);
                        }
                        let text = String::from_utf8(request).expect("request");
                        let (start, end) = text.lines().find_map(|line| {
                            let value = line.strip_prefix("range: bytes=").or_else(|| line.strip_prefix("Range: bytes="))?;
                            let (start, end) = value.split_once('-')?;
                            Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                        }).expect("range");
                        let body = &data[start..=end];
                        let head = format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"layout-v1\"\r\nConnection: close\r\n\r\n", body.len(), data.len());
                        if stream.write_all(head.as_bytes()).await.is_err() { return; }
                        if body.len() > 1 && served.fetch_add(1, Ordering::SeqCst) == 0 {
                            stream.write_all(&body[..1024 * 1024]).await.expect("first durable piece");
                            let _ = stream.read(&mut byte).await;
                        } else {
                            let _ = stream.write_all(body).await;
                        }
                    });
                    while connections.try_join_next().is_some() {}
                }
            });
            let mut plane = directory.control_plane();
            attach_loopback_worker(&mut plane, &directory);
            let gid: Gid = plane
                .call(
                    "aria2.addUri",
                    json!([[uri], {"split":1,"endgame-max-duplicates":0}]),
                )
                .expect("add")
                .as_str()
                .expect("gid")
                .parse()
                .expect("gid");
            let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
            loop {
                plane.poll_once().expect("progress first generation");
                let status = plane
                    .call("aria2.tellStatus", json!([gid.to_string()]))
                    .expect("status");
                if status["verifiedLength"]
                    .as_str()
                    .expect("length")
                    .parse::<u64>()
                    .expect("integer")
                    >= 1024 * 1024
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "first piece never became durable: {status}"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let patch = if case == "geometry" {
                json!({"piece-length":"2M"})
            } else {
                json!({"out":"replacement.bin"})
            };
            let before = plane
                .call("aria2.getOption", json!([gid.to_string()]))
                .expect("old options");
            assert!(
                plane
                    .call("aria2.changeOption", json!([gid.to_string(), patch]))
                    .is_err()
            );
            assert_eq!(
                plane
                    .call("aria2.getOption", json!([gid.to_string()]))
                    .expect("unchanged"),
                before
            );
            if case == "occupied" {
                fs::write(
                    directory.output.join("replacement.bin"),
                    b"existing-user-file",
                )
                .expect("existing destination");
            }
            plane
                .call(
                    "aria2.changeOption",
                    json!([gid.to_string(), patch, {"restart":true}]),
                )
                .expect("authorized generation");
            let expected_status = if case == "occupied" {
                "error"
            } else {
                "complete"
            };
            loop {
                plane.poll_once().expect("progress replacement generation");
                let status = plane
                    .call("aria2.tellStatus", json!([gid.to_string()]))
                    .expect("status");
                if status["status"] == expected_status {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "replacement did not reach {expected_status}: {status}"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if case == "occupied" {
                assert_eq!(
                    fs::read(directory.output.join("replacement.bin")).expect("destination"),
                    b"existing-user-file"
                );
            } else {
                let path = if case == "geometry" {
                    "original.bin"
                } else {
                    "replacement.bin"
                };
                assert_eq!(
                    fs::read(directory.output.join(path)).expect("download"),
                    data.as_ref()
                );
            }
            if case != "geometry" {
                let original = fs::read(directory.output.join("original.bin"))
                    .expect("preserved previous file");
                assert_eq!(&original[..1024 * 1024], &data[..1024 * 1024]);
            }
            assert!(plane.shutdown_async().await.expect("shutdown").is_clean());
            server.abort();
            let _ = server.await;
            let mut recovered = directory.control_plane();
            assert_eq!(
                recovered
                    .call("aria2.tellStatus", json!([gid.to_string()]))
                    .expect("recovered status")["status"],
                expected_status
            );
            recovered.shutdown().expect("recovered shutdown");
        }
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
