use crate::{StartupReconciliation, StartupRecoveryError, restore_reconciliation};
use ariax_core::SchedulerConfig;
use ariax_storage::{
    SessionCommand, SessionCommandResult, SessionCompletion, SessionHandle, SessionOwnerError,
};
use std::error::Error;
use std::fmt;

/// One bounded unit of pre-publication SQLite startup progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupSessionRepairPoll {
    Progressed,
    Backpressured,
    WaitingForCompletion,
    Complete,
    Faulted,
}

/// A session repair failed after reconciliation but before scheduler creation.
#[derive(Debug)]
pub enum StartupSessionRepairError {
    Owner(SessionOwnerError),
    UnexpectedResult(&'static str),
    InternalInvariant,
}

impl fmt::Display for StartupSessionRepairError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(error) => write!(formatter, "startup session repair failed: {error}"),
            Self::UnexpectedResult(result) => {
                write!(
                    formatter,
                    "startup session repair returned unexpected {result}"
                )
            }
            Self::InternalInvariant => {
                formatter.write_str("startup session repair internal invariant failed")
            }
        }
    }
}

impl Error for StartupSessionRepairError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Owner(error) => Some(error),
            Self::UnexpectedResult(_) | Self::InternalInvariant => None,
        }
    }
}

/// Why a repair executor cannot yet yield its restored scheduler.
#[derive(Debug)]
pub enum StartupSessionRepairFinishError {
    NotComplete,
    Faulted(StartupSessionRepairError),
    Restore(StartupRecoveryError),
}

impl fmt::Display for StartupSessionRepairFinishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotComplete => formatter.write_str("startup session repairs are not complete"),
            Self::Faulted(error) => error.fmt(formatter),
            Self::Restore(error) => {
                write!(formatter, "scheduler restore failed after repairs: {error}")
            }
        }
    }
}

impl Error for StartupSessionRepairFinishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NotComplete => None,
            Self::Faulted(error) => Some(error),
            Self::Restore(error) => Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RepairPhase {
    Queue,
    Terminal,
    Authority,
    Complete,
}

#[cfg(test)]
struct TestRepairCompletion {
    pending_polls: std::cell::Cell<usize>,
    result: std::cell::RefCell<Option<Result<SessionCommandResult, SessionOwnerError>>>,
}

#[cfg(test)]
impl TestRepairCompletion {
    fn try_wait(&self) -> Result<Option<SessionCommandResult>, SessionOwnerError> {
        let pending = self.pending_polls.get();
        if pending != 0 {
            self.pending_polls.set(pending - 1);
            return Ok(None);
        }
        self.result
            .borrow_mut()
            .take()
            .unwrap_or(Err(SessionOwnerError::Unavailable))
            .map(Some)
    }
}

#[cfg(test)]
struct TestSessionEndpoint {
    reject_once: bool,
    accepted: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
    attempted: std::rc::Rc<std::cell::RefCell<Vec<String>>>,
    results: std::collections::VecDeque<Result<SessionCommandResult, SessionOwnerError>>,
}

#[cfg(test)]
impl TestSessionEndpoint {
    fn try_submit(
        &mut self,
        command: SessionCommand,
    ) -> Result<RepairCompletion, RejectedRepairCommand> {
        self.attempted.borrow_mut().push(format!("{command:?}"));
        if self.reject_once {
            self.reject_once = false;
            return Err(RejectedRepairCommand {
                command: Box::new(command),
                error: SessionOwnerError::QueueFull,
            });
        }
        self.accepted.borrow_mut().push(test_command_code(&command));
        Ok(RepairCompletion::Test(TestRepairCompletion {
            pending_polls: std::cell::Cell::new(1),
            result: std::cell::RefCell::new(Some(
                self.results
                    .pop_front()
                    .unwrap_or(Ok(SessionCommandResult::Unit)),
            )),
        }))
    }
}

#[cfg(test)]
fn test_command_code(command: &SessionCommand) -> &'static str {
    match command {
        SessionCommand::TransitionTaskQueue(_) => "queue",
        SessionCommand::PersistStoppedResult { .. } => "terminal",
        SessionCommand::ReconcileJournalAuthority { .. } => "authority",
        _ => "unexpected",
    }
}

enum SessionEndpoint {
    Owner(SessionHandle),
    #[cfg(test)]
    Test(TestSessionEndpoint),
}

impl SessionEndpoint {
    fn try_submit(
        &mut self,
        command: SessionCommand,
    ) -> Result<RepairCompletion, RejectedRepairCommand> {
        match self {
            Self::Owner(handle) => handle
                .try_submit_owned(command)
                .map(RepairCompletion::Owner)
                .map_err(|rejection| {
                    let (command, error) = rejection.into_parts();
                    RejectedRepairCommand {
                        command: Box::new(command),
                        error,
                    }
                }),
            #[cfg(test)]
            Self::Test(endpoint) => endpoint.try_submit(command),
        }
    }
}

struct RejectedRepairCommand {
    command: Box<SessionCommand>,
    error: SessionOwnerError,
}

enum RepairCompletion {
    Owner(SessionCompletion),
    #[cfg(test)]
    Test(TestRepairCompletion),
}

impl RepairCompletion {
    fn try_wait(&mut self) -> Result<Option<SessionCommandResult>, SessionOwnerError> {
        match self {
            Self::Owner(completion) => completion.try_wait(),
            #[cfg(test)]
            Self::Test(completion) => completion.try_wait(),
        }
    }
}

/// Applies all reconciler-authorized SQLite repairs through the dedicated
/// owner before constructing a scheduler. It never waits synchronously and
/// retains an unaccepted command exactly across queue backpressure.
pub struct StartupSessionRepairExecutor {
    endpoint: SessionEndpoint,
    reconciliation: Option<StartupReconciliation>,
    scheduler_config: SchedulerConfig,
    phase: RepairPhase,
    index: usize,
    offered: Option<SessionCommand>,
    completion: Option<RepairCompletion>,
    fault: Option<StartupSessionRepairError>,
}

impl StartupSessionRepairExecutor {
    #[must_use]
    pub fn new(
        session: SessionHandle,
        reconciliation: StartupReconciliation,
        scheduler_config: SchedulerConfig,
    ) -> Self {
        Self::from_endpoint(
            SessionEndpoint::Owner(session),
            reconciliation,
            scheduler_config,
        )
    }

    fn from_endpoint(
        endpoint: SessionEndpoint,
        reconciliation: StartupReconciliation,
        scheduler_config: SchedulerConfig,
    ) -> Self {
        Self {
            endpoint,
            reconciliation: Some(reconciliation),
            scheduler_config,
            phase: RepairPhase::Queue,
            index: 0,
            offered: None,
            completion: None,
            fault: None,
        }
    }

    #[must_use]
    pub const fn fault(&self) -> Option<&StartupSessionRepairError> {
        self.fault.as_ref()
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.fault.is_none()
            && self.phase == RepairPhase::Complete
            && self.offered.is_none()
            && self.completion.is_none()
    }

    pub fn poll(&mut self) -> StartupSessionRepairPoll {
        if self.fault.is_some() {
            return StartupSessionRepairPoll::Faulted;
        }
        if let Some(completion) = self.completion.as_mut() {
            return match completion.try_wait() {
                Ok(None) => StartupSessionRepairPoll::WaitingForCompletion,
                Ok(Some(SessionCommandResult::Unit)) => {
                    self.completion = None;
                    if self.advance_completed().is_err() {
                        self.fault = Some(StartupSessionRepairError::InternalInvariant);
                        StartupSessionRepairPoll::Faulted
                    } else {
                        StartupSessionRepairPoll::Progressed
                    }
                }
                Ok(Some(result)) => self.fail(StartupSessionRepairError::UnexpectedResult(
                    command_result_code(&result),
                )),
                Err(error) => self.fail(StartupSessionRepairError::Owner(error)),
            };
        }
        if let Some(command) = self.offered.take() {
            return match self.endpoint.try_submit(command) {
                Ok(completion) => {
                    self.completion = Some(completion);
                    StartupSessionRepairPoll::WaitingForCompletion
                }
                Err(RejectedRepairCommand { command, error }) => {
                    if matches!(error, SessionOwnerError::QueueFull) {
                        self.offered = Some(*command);
                        StartupSessionRepairPoll::Backpressured
                    } else {
                        self.fail(StartupSessionRepairError::Owner(error))
                    }
                }
            };
        }
        if self.phase == RepairPhase::Complete {
            return StartupSessionRepairPoll::Complete;
        }
        match self.next_command() {
            Ok(Some(command)) => {
                self.offered = Some(command);
                StartupSessionRepairPoll::Progressed
            }
            Ok(None) => StartupSessionRepairPoll::Progressed,
            Err(error) => self.fail(error),
        }
    }

    /// Returns the reconciliation after every ordered SQLite repair has been
    /// acknowledged. The repair vectors are cleared before handoff so a later
    /// native-startup stage cannot accidentally replay an already-applied
    /// transaction.
    pub fn finish_reconciliation(
        mut self,
    ) -> Result<StartupReconciliation, StartupSessionRepairFinishError> {
        if let Some(error) = self.fault.take() {
            return Err(StartupSessionRepairFinishError::Faulted(error));
        }
        if !self.is_complete() {
            return Err(StartupSessionRepairFinishError::NotComplete);
        }
        let mut reconciliation = self
            .reconciliation
            .take()
            .ok_or(StartupSessionRepairFinishError::NotComplete)?;
        reconciliation.queue_session_repairs.clear();
        reconciliation.terminal_session_repairs.clear();
        reconciliation.authority_repairs.clear();
        Ok(reconciliation)
    }

    pub fn finish(self) -> Result<crate::EngineStartup, StartupSessionRepairFinishError> {
        let scheduler_config = self.scheduler_config;
        let reconciliation = self.finish_reconciliation()?;
        restore_reconciliation(reconciliation, scheduler_config)
            .map_err(StartupSessionRepairFinishError::Restore)
    }

    fn next_command(&mut self) -> Result<Option<SessionCommand>, StartupSessionRepairError> {
        let reconciliation = self
            .reconciliation
            .as_ref()
            .ok_or(StartupSessionRepairError::InternalInvariant)?;
        let command = match self.phase {
            RepairPhase::Queue => reconciliation
                .queue_session_repairs
                .get(self.index)
                .cloned()
                .map(SessionCommand::TransitionTaskQueue),
            RepairPhase::Terminal => reconciliation
                .terminal_session_repairs
                .get(self.index)
                .cloned()
                .map(|repair| SessionCommand::PersistStoppedResult {
                    result: repair.result,
                    transition: repair.transition,
                }),
            RepairPhase::Authority => reconciliation
                .authority_repairs
                .get(self.index)
                .cloned()
                .map(|repair| SessionCommand::ReconcileJournalAuthority {
                    gid: repair.gid,
                    expected_journal_id: repair.expected_journal_id,
                    cache: repair.cache,
                    root_display: repair.root_display,
                    updated_ms: repair.updated_ms,
                }),
            RepairPhase::Complete => None,
        };
        if command.is_some() {
            return Ok(command);
        }
        self.phase = match self.phase {
            RepairPhase::Queue => RepairPhase::Terminal,
            RepairPhase::Terminal => RepairPhase::Authority,
            RepairPhase::Authority => RepairPhase::Complete,
            RepairPhase::Complete => return Ok(None),
        };
        self.index = 0;
        Ok(None)
    }

    fn advance_completed(&mut self) -> Result<(), ()> {
        if self.phase == RepairPhase::Complete {
            return Err(());
        }
        self.index = self.index.checked_add(1).ok_or(())?;
        Ok(())
    }

    fn fail(&mut self, error: StartupSessionRepairError) -> StartupSessionRepairPoll {
        self.offered = None;
        self.completion = None;
        self.fault = Some(error);
        StartupSessionRepairPoll::Faulted
    }
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
        SessionEndpoint, StartupSessionRepairError, StartupSessionRepairExecutor,
        StartupSessionRepairFinishError, StartupSessionRepairPoll, TestSessionEndpoint,
    };
    use crate::{NoSpaceProbeTargetCatalog, SessionAuthorityRepair, StartupReconciliation};
    use ariax_core::{Gid, QueueClass, QueueOrder, SchedulerConfig, SchedulerRestoreBatch};
    use ariax_storage::{
        JournalHash, JournalId, PathPlatform, PlatformPath, SessionCommandResult,
        SessionJournalCache, SessionOwnerError, SessionQueueOrder, SessionQueueState,
        SessionQueueTransition, SessionStoppedResultRecord, SessionTerminalStatus,
    };
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::rc::Rc;

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("nonzero gid")
    }

    fn journal(value: u8) -> JournalId {
        JournalId::new([value; 16]).expect("journal id")
    }

    fn path(value: &str) -> PlatformPath {
        PlatformPath::from_native_bytes(PathPlatform::Unix, value.as_bytes()).expect("path")
    }

    fn scheduler_config() -> SchedulerConfig {
        SchedulerConfig::new(
            NonZeroUsize::new(8).expect("tasks"),
            NonZeroUsize::new(2).expect("active"),
            false,
        )
        .expect("scheduler config")
    }

    fn reconciliation() -> StartupReconciliation {
        let queue_transition = SessionQueueTransition {
            gid: gid(1),
            expected_state: SessionQueueState::Waiting,
            target_state: SessionQueueState::Paused,
            desired_paused: true,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: Vec::new(),
                },
                SessionQueueOrder {
                    state: SessionQueueState::Paused,
                    gids: vec![gid(1)],
                },
            ],
            updated_ms: 10,
        };
        let terminal_transition = SessionQueueTransition {
            gid: gid(2),
            expected_state: SessionQueueState::Waiting,
            target_state: SessionQueueState::Stopped,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: Vec::new(),
                },
                SessionQueueOrder {
                    state: SessionQueueState::Stopped,
                    gids: vec![gid(2)],
                },
            ],
            updated_ms: 20,
        };
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
            authority_repairs: vec![SessionAuthorityRepair {
                gid: gid(3),
                expected_journal_id: journal(3),
                cache: Some(SessionJournalCache {
                    layout_hash: None,
                    root_binding_hash: None,
                    snapshot_hash: JournalHash::new([3; 32]).expect("hash"),
                }),
                root_display: Some(path("/root/three")),
                updated_ms: 30,
            }],
            appender_recoveries: Vec::new(),
            journal_install_recoveries: Vec::new(),
            no_space_probe_targets: NoSpaceProbeTargetCatalog::new(Vec::new()),
            queue_session_repairs: vec![queue_transition],
            terminal_session_repairs: vec![crate::DeferredTerminalSessionRepair {
                result: SessionStoppedResultRecord {
                    gid: gid(2),
                    status: SessionTerminalStatus::Removed,
                    error_kind: None,
                    safe_message: String::new(),
                    total_length: None,
                    layout_hash: None,
                    completed_ms: 20,
                },
                transition: terminal_transition,
            }],
        }
    }

    fn executor(endpoint: TestSessionEndpoint) -> StartupSessionRepairExecutor {
        StartupSessionRepairExecutor::from_endpoint(
            SessionEndpoint::Test(endpoint),
            reconciliation(),
            scheduler_config(),
        )
    }

    fn drive(executor: &mut StartupSessionRepairExecutor) {
        for _ in 0..32 {
            match executor.poll() {
                StartupSessionRepairPoll::Complete => return,
                StartupSessionRepairPoll::Faulted => panic!("executor faulted"),
                StartupSessionRepairPoll::Progressed
                | StartupSessionRepairPoll::Backpressured
                | StartupSessionRepairPoll::WaitingForCompletion => {}
            }
        }
        panic!("executor did not finish");
    }

    #[test]
    fn repairs_are_ordered_and_queue_full_retries_the_exact_command() {
        let accepted = Rc::new(RefCell::new(Vec::new()));
        let attempted = Rc::new(RefCell::new(Vec::new()));
        let mut executor = executor(TestSessionEndpoint {
            reject_once: true,
            accepted: Rc::clone(&accepted),
            attempted: Rc::clone(&attempted),
            results: VecDeque::new(),
        });
        assert_eq!(executor.poll(), StartupSessionRepairPoll::Progressed);
        assert_eq!(executor.poll(), StartupSessionRepairPoll::Backpressured);
        drive(&mut executor);
        assert_eq!(&*accepted.borrow(), &["queue", "terminal", "authority"]);
        let attempted = attempted.borrow();
        assert_eq!(attempted[0], attempted[1]);
        assert!(executor.is_complete());
        let startup = executor.finish().expect("restored after repairs");
        assert!(startup.scheduler.is_empty());
        assert!(startup.queue_session_repairs.is_empty());
        assert!(startup.terminal_session_repairs.is_empty());
        assert!(startup.authority_repairs.is_empty());
    }

    #[test]
    fn accepted_failure_and_unexpected_result_fault_before_restore() {
        let accepted = Rc::new(RefCell::new(Vec::new()));
        let mut failed = executor(TestSessionEndpoint {
            reject_once: false,
            accepted: Rc::clone(&accepted),
            attempted: Rc::new(RefCell::new(Vec::new())),
            results: VecDeque::from([Err(SessionOwnerError::Unavailable)]),
        });
        assert_eq!(failed.poll(), StartupSessionRepairPoll::Progressed);
        assert_eq!(
            failed.poll(),
            StartupSessionRepairPoll::WaitingForCompletion
        );
        assert_eq!(
            failed.poll(),
            StartupSessionRepairPoll::WaitingForCompletion
        );
        assert_eq!(failed.poll(), StartupSessionRepairPoll::Faulted);
        assert!(matches!(
            failed.finish(),
            Err(StartupSessionRepairFinishError::Faulted(
                StartupSessionRepairError::Owner(SessionOwnerError::Unavailable)
            ))
        ));

        let mut unexpected = executor(TestSessionEndpoint {
            reject_once: false,
            accepted,
            attempted: Rc::new(RefCell::new(Vec::new())),
            results: VecDeque::from([Ok(SessionCommandResult::QueueOrder(Vec::new()))]),
        });
        assert_eq!(unexpected.poll(), StartupSessionRepairPoll::Progressed);
        assert_eq!(
            unexpected.poll(),
            StartupSessionRepairPoll::WaitingForCompletion
        );
        assert_eq!(
            unexpected.poll(),
            StartupSessionRepairPoll::WaitingForCompletion
        );
        assert_eq!(unexpected.poll(), StartupSessionRepairPoll::Faulted);
        assert!(matches!(
            unexpected.fault(),
            Some(StartupSessionRepairError::UnexpectedResult("queue_order"))
        ));
    }

    #[test]
    fn finish_rejects_incomplete_execution() {
        let executor = executor(TestSessionEndpoint {
            reject_once: false,
            accepted: Rc::new(RefCell::new(Vec::new())),
            attempted: Rc::new(RefCell::new(Vec::new())),
            results: VecDeque::new(),
        });
        assert!(matches!(
            executor.finish(),
            Err(StartupSessionRepairFinishError::NotComplete)
        ));
    }

    #[test]
    fn finish_reconciliation_clears_applied_repairs_for_native_handoff() {
        let mut executor = executor(TestSessionEndpoint {
            reject_once: false,
            accepted: Rc::new(RefCell::new(Vec::new())),
            attempted: Rc::new(RefCell::new(Vec::new())),
            results: VecDeque::new(),
        });
        drive(&mut executor);
        let reconciliation = executor
            .finish_reconciliation()
            .expect("post-repair handoff");
        assert!(reconciliation.queue_session_repairs.is_empty());
        assert!(reconciliation.terminal_session_repairs.is_empty());
        assert!(reconciliation.authority_repairs.is_empty());
    }
}
