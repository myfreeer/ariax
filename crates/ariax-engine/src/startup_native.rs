use crate::{
    DeferredAppenderRecovery, DeferredJournalInstallRecovery, EngineStartup, StartupReconciliation,
    StartupRecoveryError, restore_reconciliation,
};
use ariax_core::{Gid, SchedulerConfig, TaskId};
use ariax_storage::RootBinding;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

/// Result of validating a persisted journal-install intent.
///
/// An `Installing` intent may select either the retained old set or the fully
/// validated checkpoint set, and therefore returns the appender that belongs
/// to the selected authoritative set. An `Installed` intent is already
/// authoritative in SQLite; native work only validates/cleans its residue and
/// the normal appender request is opened afterward.
#[derive(Debug, Eq, PartialEq)]
pub enum NativeInstallOutcome<A> {
    RecoveredAppender(A),
    ValidatedInstalled,
}

/// Narrow native boundary for startup recovery.
///
/// The engine owns ordering and publication. Implementations own descriptor-
/// safe root acquisition, namespace exclusion, candidate validation, old-set
/// retirement, and recovered appender opening. A mock implementation is valid
/// for portable tests; real io_uring/Windows/macOS adapters implement the same
/// contract in native CI.
pub trait NativeStartupBackend {
    type RootCapability;
    type Appender;
    type Error: Error + Send + Sync + 'static;

    fn acquire_root(
        &mut self,
        gid: Gid,
        binding: Option<&RootBinding>,
    ) -> Result<Self::RootCapability, Self::Error>;

    fn recover_journal_install(
        &mut self,
        request: &DeferredJournalInstallRecovery,
        root: &Self::RootCapability,
    ) -> Result<NativeInstallOutcome<Self::Appender>, Self::Error>;

    fn open_recovered_appender(
        &mut self,
        request: &DeferredAppenderRecovery,
        root: &Self::RootCapability,
    ) -> Result<Self::Appender, Self::Error>;
}

/// A restored scheduler together with the native capabilities that must stay
/// alive for its recovered tasks.
pub struct NativeEngineStartup<R, A> {
    pub startup: EngineStartup,
    pub roots: BTreeMap<TaskId, R>,
    pub appenders: BTreeMap<Gid, A>,
}

pub type NativeStartupResult<B> = Result<
    NativeEngineStartup<
        <B as NativeStartupBackend>::RootCapability,
        <B as NativeStartupBackend>::Appender,
    >,
    NativeStartupError<<B as NativeStartupBackend>::Error>,
>;

impl<R, A> NativeEngineStartup<R, A> {
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.startup.tasks.len()
    }
}

/// Why native startup could not produce a publication-safe restored driver.
#[derive(Debug)]
pub enum NativeStartupError<E> {
    RepairsPending,
    TaskLimitReached,
    MissingTask(Gid),
    DuplicateRoot(Gid),
    DuplicateAppender(Gid),
    Backend {
        gid: Gid,
        operation: &'static str,
        source: E,
    },
    Restore(StartupRecoveryError),
}

impl<E: fmt::Display> fmt::Display for NativeStartupError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RepairsPending => formatter.write_str("SQLite startup repairs remain pending"),
            Self::TaskLimitReached => {
                formatter.write_str("native startup task limit is below the recovered batch")
            }
            Self::MissingTask(gid) => {
                write!(formatter, "native startup references unknown task {gid}")
            }
            Self::DuplicateRoot(gid) => write!(
                formatter,
                "native startup acquired duplicate root for {gid}"
            ),
            Self::DuplicateAppender(gid) => write!(
                formatter,
                "native startup opened duplicate appender for {gid}"
            ),
            Self::Backend {
                gid,
                operation,
                source,
            } => {
                write!(
                    formatter,
                    "native startup {operation} failed for {gid}: {source}"
                )
            }
            Self::Restore(error) => write!(
                formatter,
                "scheduler restore failed after native recovery: {error}"
            ),
        }
    }
}

impl<E: Error + 'static> Error for NativeStartupError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Backend { source, .. } => Some(source),
            Self::RepairsPending
            | Self::TaskLimitReached
            | Self::MissingTask(_)
            | Self::DuplicateRoot(_)
            | Self::DuplicateAppender(_) => None,
            Self::Restore(error) => Some(error),
        }
    }
}

/// Executes the mandatory native recovery stage and publishes only after all
/// roots, install candidates, appenders, and the scheduler restore succeed.
pub fn complete_native_startup<B>(
    mut reconciliation: StartupReconciliation,
    scheduler_config: SchedulerConfig,
    backend: &mut B,
) -> NativeStartupResult<B>
where
    B: NativeStartupBackend,
{
    if !reconciliation.queue_session_repairs.is_empty()
        || !reconciliation.terminal_session_repairs.is_empty()
        || !reconciliation.authority_repairs.is_empty()
    {
        return Err(NativeStartupError::RepairsPending);
    }

    let task_count = reconciliation.tasks.len();
    let mut roots = BTreeMap::new();
    if task_count > scheduler_config.max_tasks.get() {
        return Err(NativeStartupError::TaskLimitReached);
    }
    for task in &reconciliation.tasks {
        let binding = task
            .journal
            .layout()
            .map(|layout| layout.layout().root_binding());
        let root = backend.acquire_root(task.gid, binding).map_err(|source| {
            NativeStartupError::Backend {
                gid: task.gid,
                operation: "acquire_root",
                source,
            }
        })?;
        if roots.insert(task.task_id, root).is_some() {
            return Err(NativeStartupError::DuplicateRoot(task.gid));
        }
    }

    let mut appenders = BTreeMap::new();
    for request in &reconciliation.journal_install_recoveries {
        let root = roots
            .get(&request.task_id)
            .ok_or(NativeStartupError::MissingTask(request.intent.gid))?;
        match backend
            .recover_journal_install(request, root)
            .map_err(|source| NativeStartupError::Backend {
                gid: request.intent.gid,
                operation: "recover_journal_install",
                source,
            })? {
            NativeInstallOutcome::RecoveredAppender(appender) => {
                if appenders.insert(request.intent.gid, appender).is_some() {
                    return Err(NativeStartupError::DuplicateAppender(request.intent.gid));
                }
            }
            NativeInstallOutcome::ValidatedInstalled => {}
        }
    }

    for request in &reconciliation.appender_recoveries {
        let root = roots
            .get(&request.task_id)
            .ok_or(NativeStartupError::MissingTask(request.gid))?;
        let appender = backend
            .open_recovered_appender(request, root)
            .map_err(|source| NativeStartupError::Backend {
                gid: request.gid,
                operation: "open_recovered_appender",
                source,
            })?;
        if appenders.insert(request.gid, appender).is_some() {
            return Err(NativeStartupError::DuplicateAppender(request.gid));
        }
    }

    reconciliation.appender_recoveries.clear();
    reconciliation.journal_install_recoveries.clear();
    let startup = restore_reconciliation(reconciliation, scheduler_config)
        .map_err(NativeStartupError::Restore)?;
    Ok(NativeEngineStartup {
        startup,
        roots,
        appenders,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        NativeInstallOutcome, NativeStartupBackend, NativeStartupError, complete_native_startup,
    };
    use crate::{
        DeferredAppenderRecovery, DeferredJournalInstallRecovery, NoSpaceProbeTargetCatalog,
        RecoveredEngineTask, SessionAuthorityRepair, StartupReconciliation,
    };
    use ariax_core::{
        Generation, Gid, QueueClass, QueueOrder, RecoveredSchedulerTask, SchedulerConfig,
        SchedulerRestoreBatch, TaskConditions, TaskId, TaskState,
    };
    use ariax_storage::{
        CheckpointId, DurabilityMode, JournalHash, JournalId, JournalInstallIntent,
        JournalInstallPhase, JournalPayload, JournalRecord, JournalStateLimits, JournalStateStop,
        PathPlatform, PlatformPath, RootBinding, SessionJournalCache, recover_journal_state,
    };
    use std::error::Error;
    use std::fmt;
    use std::num::NonZeroUsize;

    #[derive(Debug)]
    struct BackendError;

    impl fmt::Display for BackendError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("backend error")
        }
    }

    impl Error for BackendError {}

    struct Backend;

    impl NativeStartupBackend for Backend {
        type RootCapability = ();
        type Appender = ();
        type Error = BackendError;

        fn acquire_root(
            &mut self,
            _gid: Gid,
            _binding: Option<&RootBinding>,
        ) -> Result<Self::RootCapability, Self::Error> {
            Ok(())
        }

        fn recover_journal_install(
            &mut self,
            _request: &crate::DeferredJournalInstallRecovery,
            _root: &Self::RootCapability,
        ) -> Result<NativeInstallOutcome<Self::Appender>, Self::Error> {
            Ok(NativeInstallOutcome::ValidatedInstalled)
        }

        fn open_recovered_appender(
            &mut self,
            _request: &crate::DeferredAppenderRecovery,
            _root: &Self::RootCapability,
        ) -> Result<Self::Appender, Self::Error> {
            Ok(())
        }
    }

    struct RecordingBackend {
        calls: Vec<&'static str>,
    }

    impl NativeStartupBackend for RecordingBackend {
        type RootCapability = Gid;
        type Appender = &'static str;
        type Error = BackendError;

        fn acquire_root(
            &mut self,
            gid: Gid,
            _binding: Option<&RootBinding>,
        ) -> Result<Self::RootCapability, Self::Error> {
            self.calls.push(if gid == gid_value(1) {
                "root_one"
            } else {
                "root_two"
            });
            Ok(gid)
        }

        fn recover_journal_install(
            &mut self,
            request: &DeferredJournalInstallRecovery,
            root: &Self::RootCapability,
        ) -> Result<NativeInstallOutcome<Self::Appender>, Self::Error> {
            assert_eq!(request.intent.gid, *root);
            self.calls.push("install_one");
            Ok(NativeInstallOutcome::RecoveredAppender("installed"))
        }

        fn open_recovered_appender(
            &mut self,
            request: &DeferredAppenderRecovery,
            root: &Self::RootCapability,
        ) -> Result<Self::Appender, Self::Error> {
            assert_eq!(request.gid, *root);
            self.calls.push("appender_two");
            Ok("ordinary")
        }
    }

    fn gid_value(value: u64) -> Gid {
        Gid::new(value).expect("gid")
    }

    fn task_id(value: u64) -> TaskId {
        TaskId::new(value).expect("task id")
    }

    fn journal(value: u8) -> JournalId {
        JournalId::new([value; 16]).expect("journal")
    }

    fn path(value: &str) -> PlatformPath {
        PlatformPath::from_native_bytes(PathPlatform::Unix, value.as_bytes()).expect("path")
    }

    fn recovered_engine_task(value: u64) -> RecoveredEngineTask {
        let task_id = task_id(value);
        let payload = JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 1,
        };
        let record = JournalRecord {
            record_type: payload.record_type(),
            generation: Generation::INITIAL,
            sequence: 1,
            payload: payload.encode().expect("payload"),
        };
        let replay = recover_journal_state(
            &[record],
            task_id,
            &|_name: &str| true,
            JournalStateLimits::default(),
        );
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        RecoveredEngineTask {
            task_id,
            gid: gid_value(value),
            journal_id: journal(value as u8),
            journal: replay.state.expect("state"),
            recovered_retry_budget_elapsed_ms: None,
        }
    }

    fn recovered_scheduler_task(value: u64) -> RecoveredSchedulerTask {
        RecoveredSchedulerTask {
            task_id: task_id(value),
            gid: gid_value(value),
            state: TaskState::Waiting,
            generation: Generation::INITIAL,
            generation_started: true,
            desired_paused: false,
            conditions: TaskConditions::default(),
            slow_demotion_count: 0,
            slow_slot: None,
            retry_at: None,
            host_key_challenge: None,
            error: None,
            stopped_status: None,
        }
    }

    fn config() -> SchedulerConfig {
        SchedulerConfig::new(
            NonZeroUsize::new(4).expect("tasks"),
            NonZeroUsize::new(1).expect("active"),
            false,
        )
        .expect("config")
    }

    fn empty_reconciliation() -> StartupReconciliation {
        StartupReconciliation {
            scheduler_batch: SchedulerRestoreBatch::new(
                Vec::new(),
                [
                    QueueClass::Waiting,
                    QueueClass::Demoted,
                    QueueClass::Paused,
                    QueueClass::Active,
                    QueueClass::Stopped,
                ]
                .into_iter()
                .map(|class| QueueOrder {
                    class,
                    order: Vec::new(),
                })
                .collect(),
            ),
            tasks: Vec::new(),
            authority_repairs: Vec::new(),
            appender_recoveries: Vec::new(),
            journal_install_recoveries: Vec::new(),
            no_space_probe_targets: NoSpaceProbeTargetCatalog::new(Vec::new()),
            queue_session_repairs: Vec::new(),
            terminal_session_repairs: Vec::new(),
        }
    }

    fn native_reconciliation() -> StartupReconciliation {
        let first_gid = gid_value(1);
        let second_gid = gid_value(2);
        let first_journal = journal(1);
        let second_journal = journal(2);
        StartupReconciliation {
            scheduler_batch: SchedulerRestoreBatch::new(
                vec![recovered_scheduler_task(1), recovered_scheduler_task(2)],
                [
                    QueueClass::Waiting,
                    QueueClass::Demoted,
                    QueueClass::Paused,
                    QueueClass::Active,
                    QueueClass::Stopped,
                ]
                .into_iter()
                .map(|class| QueueOrder {
                    class,
                    order: if class == QueueClass::Waiting {
                        vec![first_gid, second_gid]
                    } else {
                        Vec::new()
                    },
                })
                .collect(),
            ),
            tasks: vec![recovered_engine_task(1), recovered_engine_task(2)],
            authority_repairs: Vec::new(),
            appender_recoveries: vec![DeferredAppenderRecovery {
                task_id: task_id(2),
                gid: second_gid,
                journal_id: second_journal,
                primary_path: path("/journal/two"),
                replica: None,
                expected_last_sequence: 1,
            }],
            journal_install_recoveries: vec![DeferredJournalInstallRecovery {
                task_id: task_id(1),
                intent: JournalInstallIntent {
                    gid: first_gid,
                    checkpoint_id: CheckpointId::new([3; 16]).expect("checkpoint"),
                    old_journal_id: first_journal,
                    old_path: path("/journal/one-old"),
                    new_journal_id: journal(3),
                    new_path: path("/journal/one-new"),
                    source_last_sequence: 1,
                    phase: JournalInstallPhase::Installing,
                    created_ms: 1,
                },
                authoritative_journal_id: first_journal,
                authoritative_last_sequence: 1,
                authoritative_checkpoint: None,
            }],
            no_space_probe_targets: NoSpaceProbeTargetCatalog::new(Vec::new()),
            queue_session_repairs: Vec::new(),
            terminal_session_repairs: Vec::new(),
        }
    }

    #[test]
    fn empty_native_stage_publishes_restored_scheduler() {
        let startup = complete_native_startup(empty_reconciliation(), config(), &mut Backend)
            .expect("native startup");
        assert_eq!(startup.task_count(), 0);
        assert!(startup.roots.is_empty());
        assert!(startup.appenders.is_empty());
        assert!(startup.startup.appender_recoveries.is_empty());
        assert!(startup.startup.journal_install_recoveries.is_empty());
    }

    #[test]
    fn native_stage_resolves_installs_before_appenders_and_clears_requests() {
        let mut backend = RecordingBackend { calls: Vec::new() };
        let startup = complete_native_startup(native_reconciliation(), config(), &mut backend)
            .expect("native startup");
        assert_eq!(
            backend.calls,
            ["root_one", "root_two", "install_one", "appender_two"]
        );
        assert_eq!(startup.task_count(), 2);
        assert_eq!(startup.roots.len(), 2);
        assert_eq!(startup.appenders.len(), 2);
        assert!(startup.startup.appender_recoveries.is_empty());
        assert!(startup.startup.journal_install_recoveries.is_empty());
    }

    #[test]
    fn pending_sql_repairs_block_native_publication() {
        let mut reconciliation = empty_reconciliation();
        reconciliation
            .authority_repairs
            .push(SessionAuthorityRepair {
                gid: Gid::new(1).expect("gid"),
                expected_journal_id: JournalId::new([1; 16]).expect("journal"),
                cache: Some(SessionJournalCache {
                    layout_hash: None,
                    root_binding_hash: None,
                    snapshot_hash: JournalHash::new([1; 32]).expect("hash"),
                }),
                root_display: Some(
                    PlatformPath::from_native_bytes(PathPlatform::Unix, b"/root").expect("path"),
                ),
                updated_ms: 1,
            });
        assert!(matches!(
            complete_native_startup(reconciliation, config(), &mut Backend),
            Err(NativeStartupError::RepairsPending)
        ));
    }

    #[test]
    fn unknown_appender_task_is_rejected_before_backend_call() {
        let mut reconciliation = empty_reconciliation();
        reconciliation
            .appender_recoveries
            .push(crate::DeferredAppenderRecovery {
                task_id: ariax_core::TaskId::new(1).expect("task"),
                gid: Gid::new(1).expect("gid"),
                journal_id: JournalId::new([1; 16]).expect("journal"),
                primary_path: PlatformPath::from_native_bytes(PathPlatform::Unix, b"/journal")
                    .expect("path"),
                replica: None,
                expected_last_sequence: 0,
            });
        assert!(matches!(
            complete_native_startup(reconciliation, config(), &mut Backend),
            Err(NativeStartupError::MissingTask(_))
        ));
    }
}
