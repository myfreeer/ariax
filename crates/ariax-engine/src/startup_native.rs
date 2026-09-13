use crate::{
    DeferredAppenderRecovery, DeferredJournalInstallRecovery, EngineStartup, RecoveredEngineTask,
    StartupReconciliation, StartupRecoveryError, restore_reconciliation,
};
use ariax_core::{Gid, SchedulerConfig, TaskId};
use ariax_storage::{
    JournalInstallPhase, PreparedJournalSet, SessionCommand, SessionCommandResult,
    SessionCompletion, SessionHandle, SessionOwnerError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

/// Filesystem resolution for one persisted journal-install intent.
#[derive(Debug)]
pub enum NativeInstallOutcome {
    /// The candidate was rejected and cleaned where ownership was provable;
    /// the retained old set must be installed after tokenized abort.
    RejectedCandidate(PreparedJournalSet),
    /// The candidate is valid and becomes authoritative after tokenized
    /// completion, retirement, and clear.
    AcceptedCandidate(PreparedJournalSet),
    /// SQLite already names the new set; retirement and tokenized clear remain.
    ValidatedInstalled(PreparedJournalSet),
}

/// Narrow descriptor-safe native boundary for startup recovery.
pub trait NativeStartupBackend {
    type RootCapability;
    type Error: Error + Send + Sync + 'static;

    fn acquire_root(
        &mut self,
        task: &RecoveredEngineTask,
    ) -> Result<Self::RootCapability, Self::Error>;

    fn recover_journal_install(
        &mut self,
        request: &DeferredJournalInstallRecovery,
        root: &Self::RootCapability,
    ) -> Result<NativeInstallOutcome, Self::Error>;

    /// Attempts identity-proven old-set retirement. Failure is diagnostic and
    /// does not reverse an already durable pointer switch.
    fn retire_journal_install(
        &mut self,
        request: &DeferredJournalInstallRecovery,
        root: &Self::RootCapability,
    ) -> Result<(), Self::Error>;

    fn prepare_recovered_journal(
        &mut self,
        request: &DeferredAppenderRecovery,
        root: &Self::RootCapability,
    ) -> Result<PreparedJournalSet, Self::Error>;
}

/// Restored scheduler plus capabilities that remain authoritative for tasks.
pub struct NativeEngineStartup<R> {
    pub startup: EngineStartup,
    pub roots: BTreeMap<TaskId, R>,
    pub installed_journals: BTreeSet<Gid>,
    pub retirement_failures: Vec<Gid>,
}

impl<R> NativeEngineStartup<R> {
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.startup.tasks.len()
    }
}

pub type NativeStartupResult<B> = Result<
    NativeEngineStartup<<B as NativeStartupBackend>::RootCapability>,
    NativeStartupError<<B as NativeStartupBackend>::Error>,
>;

/// One bounded unit of native startup work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeStartupPoll {
    Progressed,
    Backpressured,
    WaitingForCompletion,
    Complete,
    Faulted,
}

/// Why native startup could not produce a publication-safe restored driver.
#[derive(Debug)]
pub enum NativeStartupError<E> {
    NotComplete,
    RepairsPending,
    TaskLimitReached,
    MissingTask(Gid),
    DuplicateRoot(Gid),
    DuplicateJournal(Gid),
    Backend {
        gid: Gid,
        operation: &'static str,
        source: E,
    },
    Owner(SessionOwnerError),
    UnexpectedOwnerResult(&'static str),
    InternalInvariant,
    Restore(StartupRecoveryError),
}

impl<E: fmt::Display> fmt::Display for NativeStartupError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotComplete => formatter.write_str("native startup is not complete"),
            Self::RepairsPending => formatter.write_str("SQLite startup repairs remain pending"),
            Self::TaskLimitReached => {
                formatter.write_str("native startup task limit is below the recovered batch")
            }
            Self::MissingTask(gid) => {
                write!(formatter, "native startup references unknown task {gid}")
            }
            Self::DuplicateRoot(gid) => {
                write!(
                    formatter,
                    "native startup acquired duplicate root for {gid}"
                )
            }
            Self::DuplicateJournal(gid) => {
                write!(
                    formatter,
                    "native startup prepared duplicate journal for {gid}"
                )
            }
            Self::Backend {
                gid,
                operation,
                source,
            } => write!(
                formatter,
                "native startup {operation} failed for {gid}: {source}"
            ),
            Self::Owner(error) => write!(formatter, "native startup owner command failed: {error}"),
            Self::UnexpectedOwnerResult(result) => {
                write!(
                    formatter,
                    "native startup owner returned unexpected {result}"
                )
            }
            Self::InternalInvariant => {
                formatter.write_str("native startup internal invariant failed")
            }
            Self::Restore(error) => {
                write!(
                    formatter,
                    "scheduler restore failed after native recovery: {error}"
                )
            }
        }
    }
}

impl<E: Error + 'static> Error for NativeStartupError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Backend { source, .. } => Some(source),
            Self::Owner(error) => Some(error),
            Self::Restore(error) => Some(error),
            Self::NotComplete
            | Self::RepairsPending
            | Self::TaskLimitReached
            | Self::MissingTask(_)
            | Self::DuplicateRoot(_)
            | Self::DuplicateJournal(_)
            | Self::UnexpectedOwnerResult(_)
            | Self::InternalInvariant => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativePhase {
    Roots,
    Installs,
    Appenders,
    OwnerInstall,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerAction {
    AbortInstall(Gid),
    CompleteInstall(Gid),
    ClearInstall(Gid),
    InstallJournal(Gid),
}

struct OfferedCommand {
    command: SessionCommand,
    action: OwnerAction,
}

struct PendingCompletion {
    completion: SessionCompletion,
    action: OwnerAction,
}

/// Applies native filesystem recovery and owner-thread journal installation
/// without waiting synchronously on the bounded owner queue.
pub struct NativeStartupExecutor<B>
where
    B: NativeStartupBackend,
{
    session: SessionHandle,
    backend: B,
    reconciliation: Option<StartupReconciliation>,
    scheduler_config: SchedulerConfig,
    updated_ms: u64,
    recovery_created_at_unix_ms: u64,
    phase: NativePhase,
    index: usize,
    roots: BTreeMap<TaskId, B::RootCapability>,
    prepared: BTreeMap<Gid, PreparedJournalSet>,
    pending_prepared: Option<(Gid, PreparedJournalSet)>,
    pending_install: Option<DeferredJournalInstallRecovery>,
    offered: Option<OfferedCommand>,
    completion: Option<PendingCompletion>,
    installed_journals: BTreeSet<Gid>,
    retirement_failures: Vec<Gid>,
    fault: Option<NativeStartupError<B::Error>>,
}

impl<B> NativeStartupExecutor<B>
where
    B: NativeStartupBackend,
{
    #[must_use]
    pub fn new(
        session: SessionHandle,
        backend: B,
        reconciliation: StartupReconciliation,
        scheduler_config: SchedulerConfig,
        updated_ms: u64,
        recovery_created_at_unix_ms: u64,
    ) -> Self {
        let initial_fault = if !reconciliation.queue_session_repairs.is_empty()
            || !reconciliation.terminal_session_repairs.is_empty()
            || !reconciliation.authority_repairs.is_empty()
        {
            Some(NativeStartupError::RepairsPending)
        } else if reconciliation.tasks.len() > scheduler_config.max_tasks.get() {
            Some(NativeStartupError::TaskLimitReached)
        } else {
            None
        };
        Self {
            session,
            backend,
            reconciliation: Some(reconciliation),
            scheduler_config,
            updated_ms,
            recovery_created_at_unix_ms,
            phase: NativePhase::Roots,
            index: 0,
            roots: BTreeMap::new(),
            prepared: BTreeMap::new(),
            pending_prepared: None,
            pending_install: None,
            offered: None,
            completion: None,
            installed_journals: BTreeSet::new(),
            retirement_failures: Vec::new(),
            fault: initial_fault,
        }
    }

    #[must_use]
    pub const fn fault(&self) -> Option<&NativeStartupError<B::Error>> {
        self.fault.as_ref()
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.fault.is_none()
            && self.phase == NativePhase::Complete
            && self.offered.is_none()
            && self.completion.is_none()
            && self.pending_prepared.is_none()
            && self.pending_install.is_none()
    }

    pub fn poll(&mut self) -> NativeStartupPoll {
        if self.fault.is_some() {
            return NativeStartupPoll::Faulted;
        }
        if let Some(pending) = self.completion.take() {
            return match pending.completion.try_wait() {
                Ok(None) => {
                    self.completion = Some(pending);
                    NativeStartupPoll::WaitingForCompletion
                }
                Ok(Some(SessionCommandResult::Unit)) => {
                    match self.complete_owner_action(pending.action) {
                        Ok(()) => NativeStartupPoll::Progressed,
                        Err(error) => self.fail(error),
                    }
                }
                Ok(Some(result)) => self.fail(NativeStartupError::UnexpectedOwnerResult(
                    command_result_code(&result),
                )),
                Err(error) => self.fail(NativeStartupError::Owner(error)),
            };
        }
        if let Some(offered) = self.offered.take() {
            return match self.session.try_submit_owned(offered.command) {
                Ok(completion) => {
                    self.completion = Some(PendingCompletion {
                        completion,
                        action: offered.action,
                    });
                    NativeStartupPoll::WaitingForCompletion
                }
                Err(rejection) => {
                    let (command, error) = rejection.into_parts();
                    if matches!(error, SessionOwnerError::QueueFull) {
                        self.offered = Some(OfferedCommand {
                            command,
                            action: offered.action,
                        });
                        NativeStartupPoll::Backpressured
                    } else {
                        self.fail(NativeStartupError::Owner(error))
                    }
                }
            };
        }
        match self.phase {
            NativePhase::Roots => self.poll_roots(),
            NativePhase::Installs => self.poll_installs(),
            NativePhase::Appenders => self.poll_appenders(),
            NativePhase::OwnerInstall => self.poll_owner_install(),
            NativePhase::Complete => NativeStartupPoll::Complete,
        }
    }

    pub fn finish(mut self) -> NativeStartupResult<B> {
        if let Some(error) = self.fault.take() {
            return Err(error);
        }
        if !self.is_complete() {
            return Err(NativeStartupError::NotComplete);
        }
        let mut reconciliation = self
            .reconciliation
            .take()
            .ok_or(NativeStartupError::InternalInvariant)?;
        reconciliation.appender_recoveries.clear();
        reconciliation.journal_install_recoveries.clear();
        let startup = restore_reconciliation(reconciliation, self.scheduler_config)
            .map_err(NativeStartupError::Restore)?;
        Ok(NativeEngineStartup {
            startup,
            roots: self.roots,
            installed_journals: self.installed_journals,
            retirement_failures: self.retirement_failures,
        })
    }

    fn poll_roots(&mut self) -> NativeStartupPoll {
        let Some(reconciliation) = self.reconciliation.as_ref() else {
            return self.fail(NativeStartupError::InternalInvariant);
        };
        let Some(task) = reconciliation.tasks.get(self.index) else {
            self.phase = NativePhase::Installs;
            self.index = 0;
            return NativeStartupPoll::Progressed;
        };
        let gid = task.gid;
        let task_id = task.task_id;
        let root = match self.backend.acquire_root(task) {
            Ok(root) => root,
            Err(source) => {
                return self.fail(NativeStartupError::Backend {
                    gid,
                    operation: "acquire_root",
                    source,
                });
            }
        };
        if self.roots.insert(task_id, root).is_some() {
            return self.fail(NativeStartupError::DuplicateRoot(gid));
        }
        self.index += 1;
        NativeStartupPoll::Progressed
    }

    fn poll_installs(&mut self) -> NativeStartupPoll {
        let request = match self
            .reconciliation
            .as_ref()
            .and_then(|value| value.journal_install_recoveries.get(self.index))
            .cloned()
        {
            Some(request) => request,
            None => {
                self.phase = NativePhase::Appenders;
                self.index = 0;
                return NativeStartupPoll::Progressed;
            }
        };
        let gid = request.intent.gid;
        let Some(root) = self.roots.get(&request.task_id) else {
            return self.fail(NativeStartupError::MissingTask(gid));
        };
        let outcome = match self.backend.recover_journal_install(&request, root) {
            Ok(outcome) => outcome,
            Err(source) => {
                return self.fail(NativeStartupError::Backend {
                    gid,
                    operation: "recover_journal_install",
                    source,
                });
            }
        };
        self.pending_install = Some(request.clone());
        match outcome {
            NativeInstallOutcome::RejectedCandidate(prepared) => {
                if request.intent.phase != JournalInstallPhase::Installing {
                    return self.fail(NativeStartupError::InternalInvariant);
                }
                self.pending_prepared = Some((gid, prepared));
                self.offered = Some(OfferedCommand {
                    command: SessionCommand::AbortJournalInstall {
                        token: request.intent.token(),
                    },
                    action: OwnerAction::AbortInstall(gid),
                });
            }
            NativeInstallOutcome::AcceptedCandidate(prepared) => {
                if request.intent.phase != JournalInstallPhase::Installing {
                    return self.fail(NativeStartupError::InternalInvariant);
                }
                self.pending_prepared = Some((gid, prepared));
                self.offered = Some(OfferedCommand {
                    command: SessionCommand::CompleteJournalInstall {
                        token: request.intent.token(),
                        updated_ms: self.updated_ms,
                    },
                    action: OwnerAction::CompleteInstall(gid),
                });
            }
            NativeInstallOutcome::ValidatedInstalled(prepared) => {
                if request.intent.phase != JournalInstallPhase::Installed {
                    return self.fail(NativeStartupError::InternalInvariant);
                }
                self.pending_prepared = Some((gid, prepared));
                if let Err(error) = self.try_retire_current_install(gid) {
                    return self.fail(error);
                }
                self.offered = Some(OfferedCommand {
                    command: SessionCommand::ClearInstalledJournal {
                        token: request.intent.token(),
                    },
                    action: OwnerAction::ClearInstall(gid),
                });
            }
        }
        NativeStartupPoll::Progressed
    }

    fn poll_appenders(&mut self) -> NativeStartupPoll {
        let request = match self
            .reconciliation
            .as_ref()
            .and_then(|value| value.appender_recoveries.get(self.index))
            .cloned()
        {
            Some(request) => request,
            None => {
                self.phase = NativePhase::OwnerInstall;
                self.index = 0;
                return NativeStartupPoll::Progressed;
            }
        };
        let Some(root) = self.roots.get(&request.task_id) else {
            return self.fail(NativeStartupError::MissingTask(request.gid));
        };
        let prepared = match self.backend.prepare_recovered_journal(&request, root) {
            Ok(prepared) => prepared,
            Err(source) => {
                return self.fail(NativeStartupError::Backend {
                    gid: request.gid,
                    operation: "prepare_recovered_journal",
                    source,
                });
            }
        };
        if self.prepared.insert(request.gid, prepared).is_some() {
            return self.fail(NativeStartupError::DuplicateJournal(request.gid));
        }
        self.index += 1;
        NativeStartupPoll::Progressed
    }

    fn poll_owner_install(&mut self) -> NativeStartupPoll {
        let Some((&gid, _)) = self.prepared.first_key_value() else {
            self.phase = NativePhase::Complete;
            return NativeStartupPoll::Progressed;
        };
        let prepared = self
            .prepared
            .remove(&gid)
            .expect("first prepared key remains present");
        let generation = match self.task(gid) {
            Some(task) => task.journal.generation(),
            None => return self.fail(NativeStartupError::MissingTask(gid)),
        };
        self.offered = Some(OfferedCommand {
            command: SessionCommand::InstallPreparedJournal {
                gid,
                prepared,
                recovery_starting_generation: generation,
                recovery_created_at_unix_ms: self.recovery_created_at_unix_ms,
            },
            action: OwnerAction::InstallJournal(gid),
        });
        NativeStartupPoll::Progressed
    }

    fn complete_owner_action(
        &mut self,
        action: OwnerAction,
    ) -> Result<(), NativeStartupError<B::Error>> {
        match action {
            OwnerAction::AbortInstall(gid) => {
                self.finish_pending_install(gid)?;
            }
            OwnerAction::CompleteInstall(gid) => {
                self.try_retire_current_install(gid)?;
                let request = self
                    .pending_install
                    .as_ref()
                    .ok_or(NativeStartupError::InternalInvariant)?;
                self.offered = Some(OfferedCommand {
                    command: SessionCommand::ClearInstalledJournal {
                        token: request.intent.token(),
                    },
                    action: OwnerAction::ClearInstall(gid),
                });
            }
            OwnerAction::ClearInstall(gid) => {
                self.finish_pending_install(gid)?;
            }
            OwnerAction::InstallJournal(gid) => {
                if !self.installed_journals.insert(gid) {
                    return Err(NativeStartupError::DuplicateJournal(gid));
                }
            }
        }
        Ok(())
    }

    fn finish_pending_install(&mut self, gid: Gid) -> Result<(), NativeStartupError<B::Error>> {
        let (prepared_gid, prepared) = self
            .pending_prepared
            .take()
            .ok_or(NativeStartupError::InternalInvariant)?;
        if prepared_gid != gid || self.prepared.insert(gid, prepared).is_some() {
            return Err(NativeStartupError::DuplicateJournal(gid));
        }
        let request = self
            .pending_install
            .take()
            .ok_or(NativeStartupError::InternalInvariant)?;
        if request.intent.gid != gid {
            return Err(NativeStartupError::InternalInvariant);
        }
        self.index += 1;
        Ok(())
    }

    fn try_retire_current_install(&mut self, gid: Gid) -> Result<(), NativeStartupError<B::Error>> {
        let Some(request) = self.pending_install.as_ref() else {
            return Err(NativeStartupError::InternalInvariant);
        };
        let Some(root) = self.roots.get(&request.task_id) else {
            return Err(NativeStartupError::MissingTask(gid));
        };
        if self.backend.retire_journal_install(request, root).is_err() {
            self.retirement_failures.push(gid);
        }
        Ok(())
    }

    fn task(&self, gid: Gid) -> Option<&RecoveredEngineTask> {
        self.reconciliation
            .as_ref()?
            .tasks
            .iter()
            .find(|task| task.gid == gid)
    }

    fn fail(&mut self, error: NativeStartupError<B::Error>) -> NativeStartupPoll {
        self.fault = Some(error);
        NativeStartupPoll::Faulted
    }
}

/// Convenience driver for startup callers that are not themselves poll loops.
pub fn complete_native_startup<B>(
    session: SessionHandle,
    backend: B,
    reconciliation: StartupReconciliation,
    scheduler_config: SchedulerConfig,
    updated_ms: u64,
    recovery_created_at_unix_ms: u64,
) -> NativeStartupResult<B>
where
    B: NativeStartupBackend,
{
    let mut executor = NativeStartupExecutor::new(
        session,
        backend,
        reconciliation,
        scheduler_config,
        updated_ms,
        recovery_created_at_unix_ms,
    );
    loop {
        match executor.poll() {
            NativeStartupPoll::Complete | NativeStartupPoll::Faulted => break,
            NativeStartupPoll::Progressed
            | NativeStartupPoll::Backpressured
            | NativeStartupPoll::WaitingForCompletion => std::thread::yield_now(),
        }
    }
    executor.finish()
}

fn command_result_code(result: &SessionCommandResult) -> &'static str {
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
        NativeStartupBackend, NativeStartupError, NativeStartupExecutor, NativeStartupPoll,
    };
    use crate::{
        DeferredAppenderRecovery, DeferredJournalInstallRecovery, NoSpaceProbeTargetCatalog,
        RecoveredEngineTask, SessionAuthorityRepair, StartupReconciliation,
    };
    use ariax_core::{
        ALL_QUEUE_CLASSES, Generation, Gid, QueueClass, QueueOrder, RecoveredSchedulerTask,
        SchedulerConfig, SchedulerRestoreBatch, TaskConditions, TaskId, TaskState,
    };
    use ariax_storage::{
        ControlJournalAppender, DurabilityMode, JournalHash, JournalId, JournalPayload,
        JournalRecord, JournalStateLimits, JournalStateStop, PlatformPath, PreparedJournalSet,
        ReplayLimits, SessionCommand, SessionCommandResult, SessionOwner, SessionOwnerConfig,
        journal_segment_path, recover_journal_state,
    };
    use std::collections::BTreeSet;
    use std::error::Error;
    use std::fmt;
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "ariax-engine-native-{label}-{}-{}",
                std::process::id(),
                TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            #[cfg(unix)]
            {
                fs::create_dir(&path).expect("create test directory");
                use std::os::unix::fs::PermissionsExt as _;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    .expect("private test directory");
            }
            #[cfg(windows)]
            ariax_windows_security::create_private_directory(&path)
                .expect("create private test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Debug)]
    struct BackendError;

    impl fmt::Display for BackendError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("backend error")
        }
    }

    impl Error for BackendError {}

    struct Backend {
        prepared: Option<PreparedJournalSet>,
        calls: Vec<&'static str>,
        fail_prepare: bool,
    }

    impl NativeStartupBackend for Backend {
        type RootCapability = Gid;
        type Error = BackendError;

        fn acquire_root(
            &mut self,
            task: &RecoveredEngineTask,
        ) -> Result<Self::RootCapability, Self::Error> {
            self.calls.push("root");
            Ok(task.gid)
        }

        fn recover_journal_install(
            &mut self,
            _request: &DeferredJournalInstallRecovery,
            _root: &Self::RootCapability,
        ) -> Result<super::NativeInstallOutcome, Self::Error> {
            Err(BackendError)
        }

        fn retire_journal_install(
            &mut self,
            _request: &DeferredJournalInstallRecovery,
            _root: &Self::RootCapability,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn prepare_recovered_journal(
            &mut self,
            request: &DeferredAppenderRecovery,
            root: &Self::RootCapability,
        ) -> Result<PreparedJournalSet, Self::Error> {
            assert_eq!(request.gid, *root);
            self.calls.push("prepare");
            if self.fail_prepare {
                return Err(BackendError);
            }
            self.prepared.take().ok_or(BackendError)
        }
    }

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("gid")
    }

    fn task_id(value: u64) -> TaskId {
        TaskId::new(value).expect("task id")
    }

    fn journal(value: u8) -> JournalId {
        JournalId::new([value; 16]).expect("journal")
    }

    fn path(value: &std::path::Path) -> PlatformPath {
        PlatformPath::from_current(value).expect("platform path")
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
            gid: gid(value),
            journal_id: journal(value as u8),
            journal: replay.state.expect("state"),
            recovered_retry_budget_elapsed_ms: None,
        }
    }

    fn recovered_scheduler_task(value: u64) -> RecoveredSchedulerTask {
        RecoveredSchedulerTask {
            task_id: task_id(value),
            gid: gid(value),
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

    fn accept_all_options(_name: &str) -> bool {
        true
    }

    fn empty_reconciliation() -> StartupReconciliation {
        StartupReconciliation {
            scheduler_batch: SchedulerRestoreBatch::new(
                Vec::new(),
                ALL_QUEUE_CLASSES
                    .iter()
                    .copied()
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

    fn prepared_journal(directory: &TestDirectory) -> PreparedJournalSet {
        let journal_directory = directory.0.join("journal");
        let mut appender = ControlJournalAppender::create(
            &journal_directory,
            gid(1),
            journal(1),
            Generation::INITIAL,
            1,
        )
        .expect("create journal");
        let appended = appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::TaskCreated {
                    durability: DurabilityMode::Balanced,
                    creator_version: 1,
                },
            )
            .expect("append");
        appender.flush(appended.sequence()).expect("flush");
        appender.close_flushed().expect("close");
        drop(appender);
        ControlJournalAppender::prepare_recovered(
            &journal_directory,
            &[journal_segment_path(&journal_directory, 0)],
            gid(1),
            journal(1),
            ReplayLimits::default(),
        )
        .expect("prepare")
    }

    fn ordinary_reconciliation(directory: &TestDirectory) -> StartupReconciliation {
        let mut reconciliation = empty_reconciliation();
        reconciliation.scheduler_batch = SchedulerRestoreBatch::new(
            vec![recovered_scheduler_task(1)],
            ALL_QUEUE_CLASSES
                .iter()
                .copied()
                .map(|class| QueueOrder {
                    class,
                    order: if class == QueueClass::Waiting {
                        vec![gid(1)]
                    } else {
                        Vec::new()
                    },
                })
                .collect(),
        );
        reconciliation.tasks.push(recovered_engine_task(1));
        reconciliation
            .appender_recoveries
            .push(DeferredAppenderRecovery {
                task_id: task_id(1),
                gid: gid(1),
                journal_id: journal(1),
                primary_path: path(&directory.0.join("journal")),
                replica: None,
                expected_last_sequence: 1,
            });
        reconciliation
    }

    fn drive<B: NativeStartupBackend>(executor: &mut NativeStartupExecutor<B>) {
        for _ in 0..1_000 {
            match executor.poll() {
                NativeStartupPoll::Complete | NativeStartupPoll::Faulted => return,
                NativeStartupPoll::Progressed => std::thread::yield_now(),
                NativeStartupPoll::Backpressured | NativeStartupPoll::WaitingForCompletion => {
                    std::thread::park_timeout(std::time::Duration::from_millis(1));
                }
            }
        }
        panic!("native executor did not finish within its bounded poll budget");
    }

    #[test]
    fn prepared_journal_is_installed_on_owner_before_publication() {
        let directory = TestDirectory::new("owner-install");
        let (session, _) = SessionOwner::spawn(
            SessionOwnerConfig::new(directory.0.join("session.db")),
            accept_all_options,
        )
        .expect("session owner");
        let backend = Backend {
            prepared: Some(prepared_journal(&directory)),
            calls: Vec::new(),
            fail_prepare: false,
        };
        let mut executor = NativeStartupExecutor::new(
            session.clone(),
            backend,
            ordinary_reconciliation(&directory),
            config(),
            10,
            10,
        );
        drive(&mut executor);
        let startup = executor.finish().expect("native startup");
        assert_eq!(startup.task_count(), 1);
        assert_eq!(startup.installed_journals, BTreeSet::from([gid(1)]));
        assert!(startup.startup.appender_recoveries.is_empty());
        assert!(matches!(
            session
                .execute(SessionCommand::AppendJournal {
                    gid: gid(1),
                    generation: Generation::INITIAL,
                    payload: JournalPayload::TaskPaused {
                        reason: ariax_storage::TaskPauseReason::User,
                    },
                })
                .expect("owner append"),
            SessionCommandResult::JournalAppended(_)
        ));
        session.shutdown().expect("shutdown owner");
    }

    #[test]
    fn backend_failure_never_publishes_scheduler() {
        let directory = TestDirectory::new("backend-failure");
        let (session, _) = SessionOwner::spawn(
            SessionOwnerConfig::new(directory.0.join("session.db")),
            accept_all_options,
        )
        .expect("session owner");
        let backend = Backend {
            prepared: None,
            calls: Vec::new(),
            fail_prepare: true,
        };
        let mut executor = NativeStartupExecutor::new(
            session.clone(),
            backend,
            ordinary_reconciliation(&directory),
            config(),
            10,
            10,
        );
        drive(&mut executor);
        assert!(matches!(
            executor.fault(),
            Some(NativeStartupError::Backend { .. })
        ));
        assert!(matches!(
            executor.finish(),
            Err(NativeStartupError::Backend { .. })
        ));
        session.shutdown().expect("shutdown owner");
    }

    #[test]
    fn pending_sql_repairs_fault_before_native_work() {
        let directory = TestDirectory::new("repairs");
        let (session, _) = SessionOwner::spawn(
            SessionOwnerConfig::new(directory.0.join("session.db")),
            accept_all_options,
        )
        .expect("session owner");
        let mut reconciliation = empty_reconciliation();
        reconciliation
            .authority_repairs
            .push(SessionAuthorityRepair {
                gid: gid(1),
                expected_journal_id: journal(1),
                cache: Some(ariax_storage::SessionJournalCache {
                    layout_hash: None,
                    root_binding_hash: None,
                    snapshot_hash: JournalHash::new([1; 32]).expect("hash"),
                }),
                root_display: Some(path(std::path::Path::new("/root"))),
                updated_ms: 1,
            });
        let mut executor = NativeStartupExecutor::new(
            session.clone(),
            Backend {
                prepared: None,
                calls: Vec::new(),
                fail_prepare: false,
            },
            reconciliation,
            config(),
            1,
            1,
        );
        assert_eq!(executor.poll(), NativeStartupPoll::Faulted);
        assert!(matches!(
            executor.finish(),
            Err(NativeStartupError::RepairsPending)
        ));
        session.shutdown().expect("shutdown owner");
    }
}
