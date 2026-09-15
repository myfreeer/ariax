use crate::{
    DerivedStartupError, NativeFilesystemBackend, NativeFilesystemError, NativeFilesystemPolicy,
    NativeStartupError, PersistenceCatalogError, PersistenceEffectCatalog, PersistenceEffectPlan,
    PersistenceSchedulerEffectSink, PersistenceSchedulerPreparation,
    PersistenceSchedulerPrepareError, RecoveredEngineTask, RuntimeEffectConfig,
    RuntimeEffectConfigError, RuntimeEffectHandle, RuntimeEffectPreparation,
    RuntimeEffectPrepareError, RuntimeSchedulerEffectSink, StartupRecoveryConfig,
    StartupSessionRepairExecutor, StartupSessionRepairFinishError, StartupSessionRepairPoll,
    complete_native_startup, reconcile_startup_derived,
};
use ariax_core::{
    MAX_PERSISTED_MILLISECONDS, MonotonicInstant, RequestScheduler, SchedulerCommand,
    TaskEventEnvelope, TaskId,
};
use ariax_runtime::{
    SchedulerDriver, SchedulerDriverFault, SchedulerDriverInputError, SchedulerDriverPoll,
    SchedulerDriverPrepareError, ShutdownCoordinator, ShutdownCoordinatorError, ShutdownProfile,
    ShutdownProgress, ShutdownReport, ShutdownStep, ShutdownStepResult, ShutdownTicket,
    StatusSnapshotReader,
};
use ariax_storage::{
    JournalStateLimits, PersistedOptionPolicy, ReplayLimits, RootDirectoryCapability,
    SessionCommand, SessionCommandResult, SessionCompletion, SessionHandle, SessionId,
    SessionOwner, SessionOwnerConfig, SessionOwnerError, SessionOwnerShutdown, SessionRecord,
    SessionStore, SessionStoreConfig,
};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS: u64 = 5_000;

pub type ProcessSchedulerSink = PersistenceSchedulerEffectSink<RuntimeSchedulerEffectSink>;
pub type ProcessSchedulerDriver = SchedulerDriver<ProcessSchedulerSink>;
pub type ProcessSchedulerPreparation = PersistenceSchedulerPreparation<RuntimeEffectPreparation>;
pub type ProcessSchedulerPrepareError = PersistenceSchedulerPrepareError<RuntimeEffectPrepareError>;

/// Complete immutable bootstrap policy for one process invocation.
pub struct ProcessBootstrapConfig {
    pub session_owner: SessionOwnerConfig,
    pub control_directory: PathBuf,
    pub allowed_output_roots: Vec<PathBuf>,
    pub replay_limits: ReplayLimits,
    pub journal_state_limits: JournalStateLimits,
    pub recovery: StartupRecoveryConfig,
    pub runtime: RuntimeEffectConfig,
    pub persistence_plan_capacity: NonZeroUsize,
    pub shutdown_step_timeout_ms: u64,
    pub updated_ms: u64,
    pub recovery_created_at_unix_ms: u64,
}

/// A publication-safe process engine. Construction is possible only after
/// SQLite repair, native recovery, appender installation, and the complete
/// scheduler restore effect chain have succeeded.
pub struct BootstrappedEngine {
    session: SessionHandle,
    session_id: SessionId,
    session_record: SessionRecord,
    session_database_path: PathBuf,
    session_store_config: SessionStoreConfig,
    driver: ProcessSchedulerDriver,
    runtime: RuntimeEffectHandle,
    shutdown: Option<ShutdownCoordinator>,
    control_directory: PathBuf,
    replay_limits: ReplayLimits,
    journal_state_limits: JournalStateLimits,
    roots: BTreeMap<TaskId, Option<RootDirectoryCapability>>,
    tasks: Vec<RecoveredEngineTask>,
    installed_journals: BTreeSet<ariax_core::Gid>,
    retirement_failures: Vec<ariax_core::Gid>,
    option_policy: Arc<dyn PersistedOptionPolicy + Send + Sync>,
}

impl BootstrappedEngine {
    pub(crate) fn persisted_option_policy(&self) -> Arc<dyn PersistedOptionPolicy + Send + Sync> {
        self.option_policy.clone()
    }

    /// Reuses the owner's exact policy before a caller creates admission artifacts.
    #[must_use]
    pub fn permits_persisted_options(&self, options: &ariax_storage::SanitizedOptionMap) -> bool {
        options.entries().len() <= ariax_storage::SESSION_MAX_OPTIONS_PER_TASK
            && options
                .entries()
                .all(|(name, _)| self.option_policy.permits(name))
    }

    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    #[must_use]
    pub fn scheduler(&self) -> &RequestScheduler {
        self.driver.scheduler()
    }

    pub(crate) fn configure_queue_policies(
        &mut self,
        retry_wait_holds_slot: bool,
        slow_readmission_policy: ariax_core::SlowReadmissionPolicy,
    ) -> Result<(), SchedulerDriverInputError> {
        self.driver
            .configure_queue_policies(retry_wait_holds_slot, slow_readmission_policy)
    }

    pub(crate) fn fail_control_publication(&mut self) {
        self.driver.fail_control_publication();
    }

    #[must_use]
    pub fn snapshot_reader(&self) -> StatusSnapshotReader {
        self.driver.snapshot_reader()
    }

    #[must_use]
    pub fn runtime_handle(&self) -> RuntimeEffectHandle {
        self.runtime.clone()
    }

    #[must_use]
    pub fn session_handle(&self) -> SessionHandle {
        self.session.clone()
    }

    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    #[must_use]
    pub const fn session_record(&self) -> &SessionRecord {
        &self.session_record
    }

    #[must_use]
    pub fn control_directory(&self) -> &std::path::Path {
        &self.control_directory
    }

    pub(crate) fn session_database_path(&self) -> &std::path::Path {
        &self.session_database_path
    }

    #[must_use]
    pub const fn replay_limits(&self) -> ReplayLimits {
        self.replay_limits
    }

    #[must_use]
    pub const fn journal_state_limits(&self) -> JournalStateLimits {
        self.journal_state_limits
    }

    #[must_use]
    pub fn root(&self, task_id: TaskId) -> Option<&RootDirectoryCapability> {
        self.roots.get(&task_id).and_then(Option::as_ref)
    }

    #[must_use]
    pub fn recovered_tasks(&self) -> &[RecoveredEngineTask] {
        &self.tasks
    }

    #[must_use]
    pub const fn installed_journals(&self) -> &BTreeSet<ariax_core::Gid> {
        &self.installed_journals
    }

    #[must_use]
    pub fn retirement_failures(&self) -> &[ariax_core::Gid] {
        &self.retirement_failures
    }

    pub fn prepare_persistence(
        &mut self,
        plan: PersistenceEffectPlan,
    ) -> Result<(), SchedulerDriverPrepareError<ProcessSchedulerPrepareError>> {
        self.driver
            .prepare_sink(ProcessSchedulerPreparation::Persistence(Box::new(plan)))
    }

    pub fn prepare_runtime(
        &mut self,
        preparation: RuntimeEffectPreparation,
    ) -> Result<(), SchedulerDriverPrepareError<ProcessSchedulerPrepareError>> {
        self.driver
            .prepare_sink(ProcessSchedulerPreparation::Delegate(preparation))
    }

    pub(crate) fn discard_prepared_control(
        &mut self,
    ) -> Result<(), SchedulerDriverPrepareError<ProcessSchedulerPrepareError>> {
        self.driver
            .prepare_sink(ProcessSchedulerPreparation::DiscardPrepared(
                RuntimeEffectPreparation::DiscardOptionApplications,
            ))
    }

    pub fn execute_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<(), SchedulerDriverInputError> {
        self.driver.execute_command(command)
    }

    pub fn execute_command_at(
        &mut self,
        command: SchedulerCommand,
        at: MonotonicInstant,
    ) -> Result<(), SchedulerDriverInputError> {
        self.driver.execute_command_at(command, at)
    }

    pub fn admit_next_at(&mut self, at: MonotonicInstant) -> Result<(), SchedulerDriverInputError> {
        self.driver.admit_next_at(at)
    }

    pub fn handle_event_at(
        &mut self,
        event: &TaskEventEnvelope,
        at: MonotonicInstant,
    ) -> Result<(), SchedulerDriverInputError> {
        self.driver.handle_event_at(event, at)
    }

    /// Moves one queued worker result or due timer through the scheduler input
    /// boundary. A returned `false` means there was no ready runtime event.
    pub fn handle_next_runtime_event_at(
        &mut self,
        at: MonotonicInstant,
    ) -> Result<bool, SchedulerDriverInputError> {
        if !self.driver.is_idle() {
            return Err(SchedulerDriverInputError::Busy);
        }
        let Some(event) = self.runtime.poll_event_at(at) else {
            return Ok(false);
        };
        self.driver.handle_event_at(&event, at)?;
        Ok(true)
    }

    pub fn poll(&mut self) -> SchedulerDriverPoll {
        self.driver.poll()
    }

    pub fn poll_at(&mut self, at: MonotonicInstant) -> SchedulerDriverPoll {
        self.driver.poll_at(at)
    }

    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.driver.is_idle()
    }

    /// Starts the fixed minimal shutdown sequence and completes the process-
    /// owned admission barrier before any external lane is drained.
    pub(crate) fn begin_shutdown(mut self) -> Result<ProcessShutdown, ProcessShutdownError> {
        let mut coordinator = self
            .shutdown
            .take()
            .ok_or(ProcessShutdownError::CoordinatorAlreadyTaken)?;
        let stop = coordinator
            .begin(MonotonicInstant::now())
            .map_err(ProcessShutdownError::Coordinator)?;
        if stop.step() != ShutdownStep::StopAdmission {
            return Err(ProcessShutdownError::UnexpectedStep {
                expected: ShutdownStep::StopAdmission,
                actual: Some(stop.step()),
            });
        }
        self.runtime.close();
        let progress = coordinator
            .complete(stop, ShutdownStepResult::Succeeded, MonotonicInstant::now())
            .map_err(ProcessShutdownError::Coordinator)?;
        let ticket = next_shutdown_ticket(progress, ShutdownStep::DrainDiskCpu)?;
        Ok(ProcessShutdown {
            engine: self,
            coordinator,
            ticket,
            journals_flushed: 0,
            journals_closed: 0,
            journal_failure: None,
            session_failure: None,
        })
    }

    /// Runs the real minimal shutdown path for a process without an external
    /// HTTP worker lane.
    pub fn shutdown(self) -> Result<ProcessShutdownReport, ProcessShutdownError> {
        let mut shutdown = self.begin_shutdown()?;
        shutdown.complete_drain(ProcessDrainOutcome::Drained)?;
        shutdown.finish()
    }
}

#[derive(Debug)]
pub struct ProcessShutdownReport {
    pub journals_closed: usize,
    pub journals_flushed: usize,
    shutdown: ShutdownReport,
    journal_failure: Option<&'static str>,
    session_failure: Option<&'static str>,
}

impl ProcessShutdownReport {
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.shutdown.is_clean()
    }

    #[must_use]
    pub const fn shutdown(&self) -> ShutdownReport {
        self.shutdown
    }

    #[must_use]
    pub const fn journal_failure(&self) -> Option<&'static str> {
        self.journal_failure
    }

    #[must_use]
    pub const fn session_failure(&self) -> Option<&'static str> {
        self.session_failure
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessDrainOutcome {
    Drained,
    Failed,
    TimedOut,
}

pub(crate) struct ProcessShutdown {
    engine: BootstrappedEngine,
    coordinator: ShutdownCoordinator,
    ticket: ShutdownTicket,
    journals_flushed: usize,
    journals_closed: usize,
    journal_failure: Option<&'static str>,
    session_failure: Option<&'static str>,
}

impl ProcessShutdown {
    pub(crate) fn drain_timeout(&self) -> Duration {
        self.ticket
            .deadline()
            .duration_since(MonotonicInstant::now())
            .max(Duration::from_millis(1))
    }

    pub(crate) fn complete_drain(
        &mut self,
        outcome: ProcessDrainOutcome,
    ) -> Result<(), ProcessShutdownError> {
        if self.ticket.step() != ShutdownStep::DrainDiskCpu {
            return Err(ProcessShutdownError::UnexpectedStep {
                expected: ShutdownStep::DrainDiskCpu,
                actual: Some(self.ticket.step()),
            });
        }
        let progress = match outcome {
            ProcessDrainOutcome::Drained => self.coordinator.complete(
                self.ticket,
                ShutdownStepResult::Succeeded,
                MonotonicInstant::now(),
            ),
            ProcessDrainOutcome::Failed => self.coordinator.complete(
                self.ticket,
                ShutdownStepResult::Failed,
                MonotonicInstant::now(),
            ),
            ProcessDrainOutcome::TimedOut => self.coordinator.poll(self.ticket.deadline()),
        }
        .map_err(ProcessShutdownError::Coordinator)?;
        self.ticket = next_shutdown_ticket(progress, ShutdownStep::FlushJournal)?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<ProcessShutdownReport, ProcessShutdownError> {
        self.flush_and_close_journals()?;
        self.persist_session_and_stop_owner()
    }

    fn flush_and_close_journals(&mut self) -> Result<(), ProcessShutdownError> {
        if self.ticket.step() != ShutdownStep::FlushJournal {
            return Err(ProcessShutdownError::UnexpectedStep {
                expected: ShutdownStep::FlushJournal,
                actual: Some(self.ticket.step()),
            });
        }
        let mut succeeded = true;
        let mut timed_out = false;
        match submit_session_command(
            &self.engine.session,
            SessionCommand::FlushAllJournals,
            self.ticket.deadline(),
        ) {
            SessionCommandWait::Completed(SessionCommandResult::JournalsFlushed(count)) => {
                self.journals_flushed = count;
            }
            SessionCommandWait::Completed(result) => {
                succeeded = false;
                self.journal_failure = Some(session_result_code(&result));
            }
            SessionCommandWait::Failed(error) => {
                succeeded = false;
                self.journal_failure = Some(error.code());
            }
            SessionCommandWait::TimedOut => {
                succeeded = false;
                timed_out = true;
                self.journal_failure = Some("timed_out");
            }
        }
        if succeeded {
            match submit_session_command(
                &self.engine.session,
                SessionCommand::CloseAllFlushedJournals,
                self.ticket.deadline(),
            ) {
                SessionCommandWait::Completed(SessionCommandResult::JournalsClosed(count)) => {
                    self.journals_closed = count;
                }
                SessionCommandWait::Completed(result) => {
                    succeeded = false;
                    self.journal_failure = Some(session_result_code(&result));
                }
                SessionCommandWait::Failed(error) => {
                    succeeded = false;
                    self.journal_failure = Some(error.code());
                }
                SessionCommandWait::TimedOut => {
                    succeeded = false;
                    timed_out = true;
                    self.journal_failure = Some("timed_out");
                }
            }
        }
        let progress = if timed_out {
            self.coordinator.poll(self.ticket.deadline())
        } else {
            self.coordinator.complete(
                self.ticket,
                if succeeded {
                    ShutdownStepResult::Succeeded
                } else {
                    ShutdownStepResult::Failed
                },
                MonotonicInstant::now(),
            )
        }
        .map_err(ProcessShutdownError::Coordinator)?;
        self.ticket = next_shutdown_ticket(progress, ShutdownStep::PersistSession)?;
        Ok(())
    }

    fn persist_session_and_stop_owner(
        mut self,
    ) -> Result<ProcessShutdownReport, ProcessShutdownError> {
        if self.ticket.step() != ShutdownStep::PersistSession {
            return Err(ProcessShutdownError::UnexpectedStep {
                expected: ShutdownStep::PersistSession,
                actual: Some(self.ticket.step()),
            });
        }
        let attempted_clean = !self.ticket.checkpoint_dirty();
        self.engine.session_record.updated_ms = current_unix_ms()
            .max(self.engine.session_record.created_ms)
            .max(self.engine.session_record.updated_ms);
        self.engine.session_record.clean_shutdown = attempted_clean;

        let owner_timeout = self
            .ticket
            .deadline()
            .duration_since(MonotonicInstant::now())
            .max(Duration::from_millis(1));
        let owner_shutdown = match self.engine.session.shutdown_with_timeout(owner_timeout) {
            Ok(SessionOwnerShutdown::Joined) => Ok(()),
            Ok(SessionOwnerShutdown::DetachedUncertain { timeout }) => {
                Err(SessionOwnerError::ShutdownTimedOut { timeout })
            }
            Err(error) => Err(error),
        };
        if let Err(error) = owner_shutdown {
            self.session_failure = Some(error.code());
            let timed_out = matches!(error, SessionOwnerError::ShutdownTimedOut { .. });
            let progress = if timed_out {
                self.coordinator.poll(self.ticket.deadline())
            } else {
                self.coordinator.complete(
                    self.ticket,
                    ShutdownStepResult::Failed,
                    MonotonicInstant::now(),
                )
            }
            .map_err(ProcessShutdownError::Coordinator)?;
            let report = completed_shutdown_report(progress)?;
            return Err(ProcessShutdownError::Owner {
                report: Box::new(ProcessShutdownReport {
                    journals_closed: self.journals_closed,
                    journals_flushed: self.journals_flushed,
                    shutdown: report,
                    journal_failure: self.journal_failure,
                    session_failure: self.session_failure,
                }),
                error: Box::new(error),
            });
        }

        let mut store = match SessionStore::open(
            self.engine.session_database_path.clone(),
            self.engine.session_store_config,
        ) {
            Ok(store) => Some(store),
            Err(error) => {
                self.session_failure = Some(error.code());
                None
            }
        };
        let succeeded = if let Some(store) = store.as_mut() {
            match store.put_session(&self.engine.session_record) {
                Ok(()) => true,
                Err(error) => {
                    self.session_failure = Some(error.code());
                    false
                }
            }
        } else {
            false
        };
        let progress = self
            .coordinator
            .complete(
                self.ticket,
                if succeeded {
                    ShutdownStepResult::Succeeded
                } else {
                    ShutdownStepResult::Failed
                },
                MonotonicInstant::now(),
            )
            .map_err(ProcessShutdownError::Coordinator)?;
        let report = completed_shutdown_report(progress)?;
        if attempted_clean && !report.is_clean() {
            self.engine.session_record.clean_shutdown = false;
            if let Some(store) = store.as_mut() {
                if let Err(error) = store.put_session(&self.engine.session_record) {
                    self.session_failure = Some(error.code());
                }
            } else {
                self.session_failure.get_or_insert("store");
            }
        }
        Ok(ProcessShutdownReport {
            journals_closed: self.journals_closed,
            journals_flushed: self.journals_flushed,
            shutdown: report,
            journal_failure: self.journal_failure,
            session_failure: self.session_failure,
        })
    }
}

#[derive(Debug)]
pub enum ProcessShutdownError {
    Coordinator(ShutdownCoordinatorError),
    CoordinatorAlreadyTaken,
    UnexpectedStep {
        expected: ShutdownStep,
        actual: Option<ShutdownStep>,
    },
    Owner {
        report: Box<ProcessShutdownReport>,
        error: Box<SessionOwnerError>,
    },
}

impl ProcessShutdownError {
    #[must_use]
    pub fn report(&self) -> Option<&ProcessShutdownReport> {
        match self {
            Self::Owner { report, .. } => Some(report),
            Self::Coordinator(_) | Self::CoordinatorAlreadyTaken | Self::UnexpectedStep { .. } => {
                None
            }
        }
    }
}

impl fmt::Display for ProcessShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordinator(error) => write!(formatter, "shutdown coordinator failed: {error}"),
            Self::CoordinatorAlreadyTaken => {
                formatter.write_str("shutdown coordinator authority is unavailable")
            }
            Self::UnexpectedStep { expected, actual } => write!(
                formatter,
                "shutdown expected {} but reached {}",
                expected.code(),
                actual.map_or("complete", ShutdownStep::code)
            ),
            Self::Owner { error, .. } => {
                write!(formatter, "session owner shutdown failed: {error}")
            }
        }
    }
}

impl Error for ProcessShutdownError {}

#[derive(Debug)]
pub enum ProcessBootstrapFailure {
    Owner(SessionOwnerError),
    UnexpectedSessionResult(&'static str),
    Filesystem(NativeFilesystemError),
    Reconciliation(DerivedStartupError),
    Repairs(StartupSessionRepairFinishError),
    Native(NativeStartupError<NativeFilesystemError>),
    RuntimeConfig(RuntimeEffectConfigError),
    ShutdownCoordinator(ShutdownCoordinatorError),
    PersistenceCatalog(PersistenceCatalogError),
    RuntimeRequestCapacity { required: usize, configured: usize },
    RuntimeTimerCapacity { required: usize, configured: usize },
    DriverInput(SchedulerDriverInputError),
    DriverFault(SchedulerDriverFault),
    RestoreBackpressured,
    RestoreDidNotComplete,
    StartupProbeAuthorityRemaining(usize),
}

impl fmt::Display for ProcessBootstrapFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(error) => write!(formatter, "session owner startup failed: {error}"),
            Self::UnexpectedSessionResult(result) => {
                write!(
                    formatter,
                    "session initialization returned unexpected {result}"
                )
            }
            Self::Filesystem(error) => {
                write!(formatter, "native filesystem startup failed: {error}")
            }
            Self::Reconciliation(error) => {
                write!(formatter, "startup reconciliation failed: {error}")
            }
            Self::Repairs(error) => write!(formatter, "startup SQLite repair failed: {error}"),
            Self::Native(error) => write!(formatter, "native startup failed: {error}"),
            Self::RuntimeConfig(error) => error.fmt(formatter),
            Self::ShutdownCoordinator(error) => {
                write!(
                    formatter,
                    "shutdown coordinator configuration failed: {error}"
                )
            }
            Self::PersistenceCatalog(error) => {
                write!(
                    formatter,
                    "persistence catalog configuration failed: {error:?}"
                )
            }
            Self::RuntimeRequestCapacity {
                required,
                configured,
            } => write!(
                formatter,
                "runtime request capacity {configured} cannot retain {required} restored probes"
            ),
            Self::RuntimeTimerCapacity {
                required,
                configured,
            } => write!(
                formatter,
                "runtime timer capacity {configured} cannot retain {required} restored timers"
            ),
            Self::DriverInput(error) => {
                write!(formatter, "scheduler restore input failed: {error:?}")
            }
            Self::DriverFault(error) => write!(formatter, "scheduler restore faulted: {error:?}"),
            Self::RestoreBackpressured => {
                formatter.write_str("scheduler restore unexpectedly backpressured")
            }
            Self::RestoreDidNotComplete => {
                formatter.write_str("scheduler restore exceeded its bounded progress budget")
            }
            Self::StartupProbeAuthorityRemaining(count) => write!(
                formatter,
                "{count} startup no-space probe targets were not consumed"
            ),
        }
    }
}

impl Error for ProcessBootstrapFailure {}

/// Startup failure plus any failure from the mandatory best-effort owner
/// shutdown that follows it.
#[derive(Debug)]
pub struct ProcessBootstrapError {
    failure: Box<ProcessBootstrapFailure>,
    shutdown: Option<Box<SessionOwnerError>>,
}

impl ProcessBootstrapError {
    #[must_use]
    pub const fn failure(&self) -> &ProcessBootstrapFailure {
        &self.failure
    }

    #[must_use]
    pub fn shutdown_error(&self) -> Option<&SessionOwnerError> {
        self.shutdown.as_deref()
    }
}

impl fmt::Display for ProcessBootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.failure.fmt(formatter)?;
        if let Some(shutdown) = &self.shutdown {
            write!(formatter, "; owner cleanup also failed: {shutdown}")?;
        }
        Ok(())
    }
}

impl Error for ProcessBootstrapError {}

/// Runs the only publication-producing startup path for a recovered process.
pub fn bootstrap_process<P>(
    config: ProcessBootstrapConfig,
    option_policy: P,
) -> Result<BootstrappedEngine, ProcessBootstrapError>
where
    P: PersistedOptionPolicy + Clone + Send + Sync + 'static,
{
    let (session, mut snapshot) =
        SessionOwner::spawn(config.session_owner.clone(), option_policy.clone()).map_err(
            |failure| ProcessBootstrapError {
                failure: Box::new(ProcessBootstrapFailure::Owner(failure)),
                shutdown: None,
            },
        )?;
    let record = match snapshot.session.clone() {
        Some(mut record) => {
            record.updated_ms = record
                .updated_ms
                .max(record.created_ms)
                .max(config.updated_ms);
            record.clean_shutdown = false;
            record
        }
        None => SessionRecord {
            session_id: derive_process_session_id(&config),
            created_ms: config.recovery_created_at_unix_ms,
            updated_ms: config.updated_ms,
            clean_shutdown: false,
        },
    };
    if snapshot.session.as_ref() != Some(&record) {
        match session.execute(SessionCommand::PutSession(record.clone())) {
            Ok(SessionCommandResult::Unit) => snapshot.session = Some(record),
            Ok(result) => {
                let shutdown = session.shutdown().err().map(Box::new);
                return Err(ProcessBootstrapError {
                    failure: Box::new(ProcessBootstrapFailure::UnexpectedSessionResult(
                        session_result_code(&result),
                    )),
                    shutdown,
                });
            }
            Err(error) => {
                let shutdown = session.shutdown().err().map(Box::new);
                return Err(ProcessBootstrapError {
                    failure: Box::new(ProcessBootstrapFailure::Owner(error)),
                    shutdown,
                });
            }
        }
    }
    let result = bootstrap_after_owner(&config, option_policy, session.clone(), snapshot);
    match result {
        Ok(engine) => Ok(engine),
        Err(failure) => {
            let shutdown = session.shutdown().err().map(Box::new);
            Err(ProcessBootstrapError { failure, shutdown })
        }
    }
}

fn bootstrap_after_owner<P>(
    config: &ProcessBootstrapConfig,
    option_policy: P,
    session: SessionHandle,
    snapshot: ariax_storage::SessionStartupSnapshot,
) -> Result<BootstrappedEngine, Box<ProcessBootstrapFailure>>
where
    P: PersistedOptionPolicy + Clone + Send + Sync + 'static,
{
    let admission_option_policy = Arc::new(option_policy.clone());
    let session_record = snapshot
        .session
        .as_ref()
        .expect("bootstrap installs a session before reconciliation")
        .clone();
    let session_id = session_record.session_id;
    let native_policy = NativeFilesystemPolicy::new(
        &config.control_directory,
        config.allowed_output_roots.iter().cloned(),
        config.replay_limits,
        config.journal_state_limits,
        option_policy,
    )
    .map_err(|error| Box::new(ProcessBootstrapFailure::Filesystem(error)))?;
    let backend = NativeFilesystemBackend::new(native_policy);
    let journals = backend
        .recover_snapshot_journals(&snapshot)
        .map_err(|error| Box::new(ProcessBootstrapFailure::Filesystem(error)))?;
    let reconciliation = reconcile_startup_derived(snapshot, journals, config.recovery)
        .map_err(|error| Box::new(ProcessBootstrapFailure::Reconciliation(error)))?;

    let mut repairs = StartupSessionRepairExecutor::new(
        session.clone(),
        reconciliation,
        config.recovery.scheduler,
    );
    loop {
        match repairs.poll() {
            StartupSessionRepairPoll::Complete | StartupSessionRepairPoll::Faulted => break,
            StartupSessionRepairPoll::Progressed
            | StartupSessionRepairPoll::Backpressured
            | StartupSessionRepairPoll::WaitingForCompletion => std::thread::yield_now(),
        }
    }
    let reconciliation = repairs
        .finish_reconciliation()
        .map_err(|error| Box::new(ProcessBootstrapFailure::Repairs(error)))?;

    let native = complete_native_startup(
        session.clone(),
        backend,
        reconciliation,
        config.recovery.scheduler,
        config.updated_ms,
        config.recovery_created_at_unix_ms,
    )
    .map_err(|error| Box::new(ProcessBootstrapFailure::Native(error)))?;

    let startup_probe_count = native.startup.no_space_probe_targets.remaining();
    if startup_probe_count > config.runtime.request_capacity.get() {
        return Err(Box::new(ProcessBootstrapFailure::RuntimeRequestCapacity {
            required: startup_probe_count,
            configured: config.runtime.request_capacity.get(),
        }));
    }
    let timer_count = native
        .startup
        .tasks
        .iter()
        .filter_map(|task| native.startup.scheduler.task(task.gid))
        .map(|task| {
            usize::from(task.retry_timer.is_some()) + usize::from(task.slow_readmission.is_some())
        })
        .sum::<usize>();
    if timer_count > config.runtime.timer_capacity.get() {
        return Err(Box::new(ProcessBootstrapFailure::RuntimeTimerCapacity {
            required: timer_count,
            configured: config.runtime.timer_capacity.get(),
        }));
    }

    let crate::EngineStartup {
        scheduler,
        restore_plan,
        tasks,
        no_space_probe_targets,
        authority_repairs,
        appender_recoveries,
        journal_install_recoveries,
        queue_session_repairs,
        terminal_session_repairs,
        host_key_resolutions,
        host_key_challenge_repairs,
    } = native.startup;
    debug_assert!(host_key_resolutions.is_empty());
    debug_assert!(host_key_challenge_repairs.is_empty());
    debug_assert!(authority_repairs.is_empty());
    debug_assert!(appender_recoveries.is_empty());
    debug_assert!(journal_install_recoveries.is_empty());
    debug_assert!(queue_session_repairs.is_empty());
    debug_assert!(terminal_session_repairs.is_empty());

    let restore_effects = restore_plan.remaining();
    let (runtime_sink, runtime) =
        RuntimeSchedulerEffectSink::new(config.runtime, no_space_probe_targets)
            .map_err(|error| Box::new(ProcessBootstrapFailure::RuntimeConfig(error)))?;
    let persistence = PersistenceEffectCatalog::new(config.persistence_plan_capacity)
        .map_err(|error| Box::new(ProcessBootstrapFailure::PersistenceCatalog(error)))?;
    let sink = PersistenceSchedulerEffectSink::new(session.clone(), runtime_sink, persistence);
    let mut driver = SchedulerDriver::new(scheduler, sink);
    driver
        .begin_restore(restore_plan)
        .map_err(|error| Box::new(ProcessBootstrapFailure::DriverInput(error)))?;
    let poll_budget = restore_effects
        .saturating_mul(4)
        .saturating_add(tasks.len().saturating_mul(2))
        .saturating_add(32);
    let mut completed = false;
    for _ in 0..poll_budget {
        match driver.poll_at(config.recovery.now_monotonic) {
            SchedulerDriverPoll::Completed { .. } => {
                completed = true;
                break;
            }
            SchedulerDriverPoll::Faulted(fault) => {
                return Err(Box::new(ProcessBootstrapFailure::DriverFault(fault)));
            }
            SchedulerDriverPoll::Backpressured { .. } => {
                return Err(Box::new(ProcessBootstrapFailure::RestoreBackpressured));
            }
            SchedulerDriverPoll::Idle => break,
            SchedulerDriverPoll::Progressed | SchedulerDriverPoll::WaitingForCompletion { .. } => {}
        }
    }
    if !completed || !driver.is_idle() {
        return Err(Box::new(ProcessBootstrapFailure::RestoreDidNotComplete));
    }
    let remaining_targets = driver.sink().delegate().remaining_startup_probe_targets();
    if remaining_targets != 0 {
        return Err(Box::new(
            ProcessBootstrapFailure::StartupProbeAuthorityRemaining(remaining_targets),
        ));
    }
    let shutdown =
        ShutdownCoordinator::new(ShutdownProfile::Minimal, config.shutdown_step_timeout_ms)
            .map_err(|error| Box::new(ProcessBootstrapFailure::ShutdownCoordinator(error)))?;
    Ok(BootstrappedEngine {
        session,
        session_id,
        session_record,
        session_database_path: config.session_owner.database_path.clone(),
        session_store_config: config.session_owner.store,
        driver,
        runtime,
        shutdown: Some(shutdown),
        control_directory: config.control_directory.clone(),
        replay_limits: config.replay_limits,
        journal_state_limits: config.journal_state_limits,
        roots: native.roots,
        tasks,
        installed_journals: native.installed_journals,
        retirement_failures: native.retirement_failures,
        option_policy: admission_option_policy,
    })
}

fn derive_process_session_id(config: &ProcessBootstrapConfig) -> SessionId {
    const DOMAIN: &[u8] = b"ariax/process-session/v1\0";
    let mut digest = Sha256::new();
    digest.update(DOMAIN);
    digest.update(
        config
            .session_owner
            .database_path
            .to_string_lossy()
            .as_bytes(),
    );
    digest.update(config.recovery_created_at_unix_ms.to_le_bytes());
    digest.update(u64::from(std::process::id()).to_le_bytes());
    let digest: [u8; 32] = digest.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    SessionId::new(bytes)
}

fn next_shutdown_ticket(
    progress: ShutdownProgress,
    expected: ShutdownStep,
) -> Result<ShutdownTicket, ProcessShutdownError> {
    match progress {
        ShutdownProgress::Advanced { next, .. } if next.step() == expected => Ok(next),
        ShutdownProgress::Advanced { next, .. } | ShutdownProgress::Waiting(next) => {
            Err(ProcessShutdownError::UnexpectedStep {
                expected,
                actual: Some(next.step()),
            })
        }
        ShutdownProgress::Idle | ShutdownProgress::Complete(_) => {
            Err(ProcessShutdownError::UnexpectedStep {
                expected,
                actual: None,
            })
        }
    }
}

fn completed_shutdown_report(
    progress: ShutdownProgress,
) -> Result<ShutdownReport, ProcessShutdownError> {
    match progress {
        ShutdownProgress::Complete(report) => Ok(report),
        ShutdownProgress::Advanced { next, .. } | ShutdownProgress::Waiting(next) => {
            Err(ProcessShutdownError::UnexpectedStep {
                expected: ShutdownStep::PersistSession,
                actual: Some(next.step()),
            })
        }
        ShutdownProgress::Idle => Err(ProcessShutdownError::UnexpectedStep {
            expected: ShutdownStep::PersistSession,
            actual: None,
        }),
    }
}

enum SessionCommandWait {
    Completed(SessionCommandResult),
    Failed(SessionOwnerError),
    TimedOut,
}

fn wait_for_session_command(
    completion: SessionCompletion,
    deadline: MonotonicInstant,
) -> SessionCommandWait {
    loop {
        match completion.try_wait() {
            Ok(Some(result)) => return SessionCommandWait::Completed(result),
            Ok(None) => {}
            Err(error) => return SessionCommandWait::Failed(error),
        }
        let now = MonotonicInstant::now();
        if now >= deadline {
            return SessionCommandWait::TimedOut;
        }
        std::thread::park_timeout(deadline.duration_since(now).min(Duration::from_millis(1)));
    }
}

fn submit_session_command(
    session: &SessionHandle,
    command: SessionCommand,
    deadline: MonotonicInstant,
) -> SessionCommandWait {
    match session.try_submit(command) {
        Ok(completion) => wait_for_session_command(completion, deadline),
        Err(error) => SessionCommandWait::Failed(error),
    }
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
        .min(MAX_PERSISTED_MILLISECONDS)
}

fn session_result_code(result: &SessionCommandResult) -> &'static str {
    match result {
        SessionCommandResult::Unit => "unit",
        SessionCommandResult::Tasks(_) => "tasks",
        SessionCommandResult::StoppedResults(_) => "stopped_results",
        SessionCommandResult::TaskSources(_) => "task_sources",
        SessionCommandResult::TaskOptions(_) => "task_options",
        SessionCommandResult::HostKeyChallenge(_) => "host_key_challenge",
        SessionCommandResult::HostKeyChallenges(_) => "host_key_challenges",
        SessionCommandResult::QueueOrder(_) => "queue_order",
        SessionCommandResult::JournalAppended(_) => "journal_appended",
        SessionCommandResult::JournalFlushed(_) => "journal_flushed",
        SessionCommandResult::JournalsFlushed(_) => "journals_flushed",
        SessionCommandResult::JournalsClosed(_) => "journals_closed",
        SessionCommandResult::JournalSnapshot(_) => "journal_snapshot",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS, ProcessBootstrapConfig, ProcessDrainOutcome,
        RuntimeEffectConfig, bootstrap_process,
    };
    use crate::StartupRecoveryConfig;
    use ariax_core::{MonotonicInstant, SchedulerConfig};
    use ariax_runtime::{ShutdownCoordinatorError, ShutdownStep};
    use ariax_storage::{
        JournalStateLimits, ReplayLimits, SessionOwner, SessionOwnerConfig, SessionStore,
        SessionStoreConfig,
    };
    use std::fs;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "ariax-process-bootstrap-{}-{}",
                std::process::id(),
                TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder.create(&path).expect("create private test root");
            }
            #[cfg(windows)]
            ariax_windows_security::create_private_directory(&path)
                .expect("create private test root");
            Self(path)
        }

        fn private_subdirectory(&self, name: &str) -> PathBuf {
            let path = self.0.join(name);
            if path.is_dir() {
                return path;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder.create(&path).expect("create private subdirectory");
            }
            #[cfg(windows)]
            ariax_windows_security::create_private_directory(&path)
                .expect("create private subdirectory");
            path
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn allow_all_options(_name: &str) -> bool {
        true
    }

    fn config(directory: &TestDirectory) -> ProcessBootstrapConfig {
        let capacity = NonZeroUsize::new(8).expect("capacity");
        ProcessBootstrapConfig {
            session_owner: SessionOwnerConfig::new(directory.0.join("session.db")),
            control_directory: directory.private_subdirectory("control"),
            allowed_output_roots: Vec::new(),
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
            shutdown_step_timeout_ms: DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS,
            updated_ms: 1_000,
            recovery_created_at_unix_ms: 1_000,
        }
    }

    #[test]
    fn empty_process_bootstrap_publishes_only_after_all_startup_stages() {
        let directory = TestDirectory::new();
        let engine = bootstrap_process(config(&directory), allow_all_options)
            .expect("bootstrap empty process");
        assert_eq!(engine.task_count(), 0);
        assert!(engine.is_idle());
        assert!(engine.snapshot_reader().load().is_empty());
        assert!(!engine.session_record().clean_shutdown);
        let report = engine.shutdown().expect("shutdown process");
        assert!(report.is_clean());
        assert_eq!(report.journals_flushed, 0);
        assert_eq!(report.journals_closed, 0);

        let store = SessionStore::open(
            directory.0.join("session.db"),
            SessionStoreConfig::default(),
        )
        .expect("reopen clean session");
        assert!(
            store
                .session()
                .expect("session")
                .expect("record")
                .clean_shutdown
        );
        drop(store);

        let restarted = bootstrap_process(config(&directory), allow_all_options)
            .expect("restart clean process");
        assert!(!restarted.session_record().clean_shutdown);
        assert!(
            restarted
                .shutdown()
                .expect("shutdown restarted process")
                .is_clean()
        );
    }

    #[test]
    fn failed_and_timed_out_drain_persist_a_dirty_checkpoint() {
        for (label, outcome) in [
            ("failed", ProcessDrainOutcome::Failed),
            ("timed-out", ProcessDrainOutcome::TimedOut),
        ] {
            let directory = TestDirectory::new();
            let engine = bootstrap_process(config(&directory), allow_all_options)
                .expect("bootstrap process");
            let mut shutdown = engine.begin_shutdown().expect("begin shutdown");
            shutdown
                .complete_drain(outcome)
                .expect("record drain result");
            let report = shutdown.finish().expect("finish dirty shutdown");
            assert!(!report.is_clean(), "{label}");
            assert!(
                report.shutdown().step_failed(ShutdownStep::DrainDiskCpu),
                "{label}"
            );
            assert_eq!(
                report.shutdown().step_timed_out(ShutdownStep::DrainDiskCpu),
                outcome == ProcessDrainOutcome::TimedOut,
                "{label}"
            );
            assert_eq!(report.journal_failure(), None, "{label}");
            assert_eq!(report.session_failure(), None, "{label}");

            let store = SessionStore::open(
                directory.0.join("session.db"),
                SessionStoreConfig::default(),
            )
            .expect("reopen dirty session");
            assert!(
                !store
                    .session()
                    .expect("session")
                    .expect("record")
                    .clean_shutdown,
                "{label}"
            );
        }
    }

    #[test]
    fn failed_bootstrap_closes_the_session_owner_before_returning() {
        let directory = TestDirectory::new();
        let mut bootstrap = config(&directory);
        let owner_config = bootstrap.session_owner.clone();
        bootstrap.control_directory = directory.0.join("missing-control");
        let error = bootstrap_process(bootstrap, allow_all_options)
            .err()
            .expect("missing control directory must fail");
        assert!(error.shutdown_error().is_none());

        let (owner, _) = SessionOwner::spawn(owner_config, allow_all_options)
            .expect("failed bootstrap released the owner lock");
        owner.shutdown().expect("shutdown replacement owner");
    }

    #[test]
    fn invalid_shutdown_timeout_fails_before_process_publication() {
        let directory = TestDirectory::new();
        let mut bootstrap = config(&directory);
        let owner_config = bootstrap.session_owner.clone();
        bootstrap.shutdown_step_timeout_ms = 0;
        let error = bootstrap_process(bootstrap, allow_all_options)
            .err()
            .expect("zero shutdown timeout must fail");
        assert!(matches!(
            error.failure(),
            super::ProcessBootstrapFailure::ShutdownCoordinator(
                ShutdownCoordinatorError::InvalidStepTimeout
            )
        ));
        assert!(error.shutdown_error().is_none());

        let (owner, _) = SessionOwner::spawn(owner_config, allow_all_options)
            .expect("failed shutdown configuration released owner lock");
        owner.shutdown().expect("shutdown replacement owner");
    }
}
