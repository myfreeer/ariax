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
use ariax_core::{MonotonicInstant, RequestScheduler, SchedulerCommand, TaskEventEnvelope, TaskId};
use ariax_runtime::{
    SchedulerDriver, SchedulerDriverFault, SchedulerDriverInputError, SchedulerDriverPoll,
    SchedulerDriverPrepareError, StatusSnapshotReader,
};
use ariax_storage::{
    JournalStateLimits, PersistedOptionPolicy, ReplayLimits, RootDirectoryCapability,
    SessionCommand, SessionCommandResult, SessionHandle, SessionId, SessionOwner,
    SessionOwnerConfig, SessionOwnerError, SessionRecord,
};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;

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
    pub updated_ms: u64,
    pub recovery_created_at_unix_ms: u64,
}

/// A publication-safe process engine. Construction is possible only after
/// SQLite repair, native recovery, appender installation, and the complete
/// scheduler restore effect chain have succeeded.
pub struct BootstrappedEngine {
    session: SessionHandle,
    session_id: SessionId,
    driver: ProcessSchedulerDriver,
    runtime: RuntimeEffectHandle,
    control_directory: PathBuf,
    replay_limits: ReplayLimits,
    journal_state_limits: JournalStateLimits,
    roots: BTreeMap<TaskId, Option<RootDirectoryCapability>>,
    tasks: Vec<RecoveredEngineTask>,
    installed_journals: BTreeSet<ariax_core::Gid>,
    retirement_failures: Vec<ariax_core::Gid>,
}

impl BootstrappedEngine {
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    #[must_use]
    pub fn scheduler(&self) -> &RequestScheduler {
        self.driver.scheduler()
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
    pub fn control_directory(&self) -> &std::path::Path {
        &self.control_directory
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

    /// Stops runtime admission, closes every flushed journal on the owner
    /// thread, and then performs the owner's bounded join.
    pub fn shutdown(self) -> Result<ProcessShutdownReport, ProcessShutdownError> {
        self.runtime.close();
        let close = self
            .session
            .execute(SessionCommand::CloseAllFlushedJournals);
        let shutdown = self.session.shutdown();
        match (close, shutdown) {
            (Ok(SessionCommandResult::JournalsClosed(count)), Ok(())) => {
                Ok(ProcessShutdownReport {
                    journals_closed: count,
                })
            }
            (Ok(result), Ok(())) => Err(ProcessShutdownError::UnexpectedCloseResult(
                session_result_code(&result),
            )),
            (Err(close), Ok(())) => Err(ProcessShutdownError::Close(Box::new(close))),
            (Ok(SessionCommandResult::JournalsClosed(_)), Err(shutdown)) => {
                Err(ProcessShutdownError::Owner(Box::new(shutdown)))
            }
            (Ok(result), Err(shutdown)) => Err(ProcessShutdownError::UnexpectedAndOwner {
                result: session_result_code(&result),
                shutdown: Box::new(shutdown),
            }),
            (Err(close), Err(shutdown)) => Err(ProcessShutdownError::CloseAndOwner {
                close: Box::new(close),
                shutdown: Box::new(shutdown),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessShutdownReport {
    pub journals_closed: usize,
}

#[derive(Debug)]
pub enum ProcessShutdownError {
    Close(Box<SessionOwnerError>),
    Owner(Box<SessionOwnerError>),
    UnexpectedCloseResult(&'static str),
    UnexpectedAndOwner {
        result: &'static str,
        shutdown: Box<SessionOwnerError>,
    },
    CloseAndOwner {
        close: Box<SessionOwnerError>,
        shutdown: Box<SessionOwnerError>,
    },
}

impl fmt::Display for ProcessShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Close(error) => write!(formatter, "failed to close process journals: {error}"),
            Self::Owner(error) => write!(formatter, "session owner shutdown failed: {error}"),
            Self::UnexpectedCloseResult(result) => {
                write!(formatter, "journal close returned unexpected {result}")
            }
            Self::UnexpectedAndOwner { result, shutdown } => write!(
                formatter,
                "journal close returned unexpected {result}; owner shutdown failed: {shutdown}"
            ),
            Self::CloseAndOwner { close, shutdown } => write!(
                formatter,
                "journal close failed: {close}; owner shutdown failed: {shutdown}"
            ),
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
    if snapshot.session.is_none() {
        let record = SessionRecord {
            session_id: derive_process_session_id(&config),
            created_ms: config.recovery_created_at_unix_ms,
            updated_ms: config.updated_ms,
            clean_shutdown: false,
        };
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
    P: PersistedOptionPolicy + Send + Sync + 'static,
{
    let session_id = snapshot
        .session
        .as_ref()
        .expect("bootstrap installs a session before reconciliation")
        .session_id;
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
    } = native.startup;
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
    Ok(BootstrappedEngine {
        session,
        session_id,
        driver,
        runtime,
        control_directory: config.control_directory.clone(),
        replay_limits: config.replay_limits,
        journal_state_limits: config.journal_state_limits,
        roots: native.roots,
        tasks,
        installed_journals: native.installed_journals,
        retirement_failures: native.retirement_failures,
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
        SessionCommandResult::JournalsClosed(_) => "journals_closed",
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessBootstrapConfig, RuntimeEffectConfig, bootstrap_process};
    use crate::StartupRecoveryConfig;
    use ariax_core::{MonotonicInstant, SchedulerConfig};
    use ariax_storage::{JournalStateLimits, ReplayLimits, SessionOwner, SessionOwnerConfig};
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
        assert_eq!(
            engine.shutdown().expect("shutdown process").journals_closed,
            0
        );
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
}
