use ariax_core::{MAX_PERSISTED_MILLISECONDS, MonotonicInstant};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT_SHUTDOWN_AUTHORITY_ID: AtomicU64 = AtomicU64::new(1);

const MINIMAL_STEPS: &[ShutdownStep] = &[
    ShutdownStep::StopAdmission,
    ShutdownStep::DrainDiskCpu,
    ShutdownStep::FlushJournal,
    ShutdownStep::PersistSession,
];
const BITTORRENT_STEPS: &[ShutdownStep] = &[
    ShutdownStep::StopAdmission,
    ShutdownStep::QuiesceBitTorrent,
    ShutdownStep::DrainDiskCpu,
    ShutdownStep::CheckpointBitTorrent,
    ShutdownStep::FlushJournal,
    ShutdownStep::PersistSession,
    ShutdownStep::StopBitTorrent,
];

/// Fixed shutdown topology selected at startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownProfile {
    Minimal,
    BitTorrent,
}

impl ShutdownProfile {
    const fn steps(self) -> &'static [ShutdownStep] {
        match self {
            Self::Minimal => MINIMAL_STEPS,
            Self::BitTorrent => BITTORRENT_STEPS,
        }
    }
}

/// One fixed graceful-shutdown barrier in execution order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ShutdownStep {
    StopAdmission,
    QuiesceBitTorrent,
    DrainDiskCpu,
    CheckpointBitTorrent,
    FlushJournal,
    PersistSession,
    StopBitTorrent,
}

impl ShutdownStep {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::StopAdmission => "stop_admission",
            Self::QuiesceBitTorrent => "quiesce_bittorrent",
            Self::DrainDiskCpu => "drain_disk_cpu",
            Self::CheckpointBitTorrent => "checkpoint_bittorrent",
            Self::FlushJournal => "flush_journal",
            Self::PersistSession => "persist_session",
            Self::StopBitTorrent => "stop_bittorrent",
        }
    }

    const fn bit(self) -> u8 {
        1 << match self {
            Self::StopAdmission => 0,
            Self::QuiesceBitTorrent => 1,
            Self::DrainDiskCpu => 2,
            Self::CheckpointBitTorrent => 3,
            Self::FlushJournal => 4,
            Self::PersistSession => 5,
            Self::StopBitTorrent => 6,
        }
    }
}

/// Correlation identity and timeout for the sole current shutdown step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownTicket {
    authority_id: NonZeroU64,
    id: NonZeroU64,
    step: ShutdownStep,
    deadline: MonotonicInstant,
    checkpoint_dirty: bool,
}

impl ShutdownTicket {
    #[must_use]
    pub const fn id(self) -> NonZeroU64 {
        self.id
    }

    #[must_use]
    pub const fn step(self) -> ShutdownStep {
        self.step
    }

    #[must_use]
    pub const fn deadline(self) -> MonotonicInstant {
        self.deadline
    }

    /// Whether session persistence must record an incomplete checkpoint.
    #[must_use]
    pub const fn checkpoint_dirty(self) -> bool {
        self.checkpoint_dirty
    }
}

/// Adapter result for one exact shutdown ticket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownStepResult {
    Succeeded,
    Failed,
}

/// Failure classification retained in the bounded final report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownFailureKind {
    Failed,
    TimedOut,
}

/// First observed shutdown failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownFailure {
    pub step: ShutdownStep,
    pub kind: ShutdownFailureKind,
}

/// Bounded terminal shutdown evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    failed_steps: u8,
    timed_out_steps: u8,
    first_failure: Option<ShutdownFailure>,
}

impl ShutdownReport {
    #[must_use]
    pub const fn is_clean(self) -> bool {
        self.failed_steps == 0
    }

    #[must_use]
    pub const fn first_failure(self) -> Option<ShutdownFailure> {
        self.first_failure
    }

    #[must_use]
    pub const fn step_failed(self, step: ShutdownStep) -> bool {
        self.failed_steps & step.bit() != 0
    }

    #[must_use]
    pub const fn step_timed_out(self, step: ShutdownStep) -> bool {
        self.timed_out_steps & step.bit() != 0
    }
}

/// Observable result of one bounded coordinator poll or completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownProgress {
    Idle,
    Waiting(ShutdownTicket),
    Advanced {
        completed: ShutdownStep,
        next: ShutdownTicket,
    },
    Complete(ShutdownReport),
}

/// Invalid construction or correlation input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownCoordinatorError {
    InvalidStepTimeout,
    AuthorityIdExhausted,
    AlreadyStarted,
    NotRunning,
    StaleTicket {
        expected: ShutdownTicket,
        actual: ShutdownTicket,
    },
    TicketIdExhausted,
    DeadlineOutOfRange,
}

impl fmt::Display for ShutdownCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidStepTimeout => "shutdown step timeout is invalid",
            Self::AuthorityIdExhausted => "shutdown coordinator authority id exhausted",
            Self::AlreadyStarted => "shutdown has already started",
            Self::NotRunning => "shutdown is not running",
            Self::StaleTicket { .. } => "shutdown completion ticket is stale or out of order",
            Self::TicketIdExhausted => "shutdown ticket id exhausted",
            Self::DeadlineOutOfRange => "shutdown step deadline is out of range",
        })
    }
}

impl Error for ShutdownCoordinatorError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoordinatorState {
    Idle,
    Running {
        index: usize,
        ticket: ShutdownTicket,
    },
    Complete(ShutdownReport),
}

/// Poll-driven, one-step-in-flight graceful-shutdown batch executor.
///
/// The coordinator is deliberately move-only so one current ticket cannot be
/// accepted by two forked shutdown authorities.
///
/// ```compile_fail
/// use ariax_runtime::{ShutdownCoordinator, ShutdownProfile};
///
/// let coordinator =
///     ShutdownCoordinator::new(ShutdownProfile::Minimal, 1_000).expect("coordinator");
/// let _duplicate_authority = coordinator.clone();
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct ShutdownCoordinator {
    authority_id: NonZeroU64,
    profile: ShutdownProfile,
    step_timeout_ms: NonZeroU64,
    state: CoordinatorState,
    next_ticket_id: u64,
    report: ShutdownReport,
}

impl ShutdownCoordinator {
    pub fn new(
        profile: ShutdownProfile,
        step_timeout_ms: u64,
    ) -> Result<Self, ShutdownCoordinatorError> {
        if step_timeout_ms == 0 || step_timeout_ms > MAX_PERSISTED_MILLISECONDS {
            return Err(ShutdownCoordinatorError::InvalidStepTimeout);
        }
        let authority_id = allocate_shutdown_authority_id()
            .ok_or(ShutdownCoordinatorError::AuthorityIdExhausted)?;
        Ok(Self {
            authority_id,
            profile,
            step_timeout_ms: NonZeroU64::new(step_timeout_ms)
                .expect("validated shutdown timeout is nonzero"),
            state: CoordinatorState::Idle,
            next_ticket_id: 1,
            report: ShutdownReport {
                failed_steps: 0,
                timed_out_steps: 0,
                first_failure: None,
            },
        })
    }

    pub fn begin(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<ShutdownTicket, ShutdownCoordinatorError> {
        if self.state != CoordinatorState::Idle {
            return Err(ShutdownCoordinatorError::AlreadyStarted);
        }
        let ticket = self.make_ticket(self.profile.steps()[0], now)?;
        self.state = CoordinatorState::Running { index: 0, ticket };
        Ok(ticket)
    }

    #[must_use]
    pub const fn report(&self) -> Option<ShutdownReport> {
        match self.state {
            CoordinatorState::Complete(report) => Some(report),
            CoordinatorState::Idle | CoordinatorState::Running { .. } => None,
        }
    }

    #[must_use]
    pub const fn current_ticket(&self) -> Option<ShutdownTicket> {
        match self.state {
            CoordinatorState::Running { ticket, .. } => Some(ticket),
            CoordinatorState::Idle | CoordinatorState::Complete(_) => None,
        }
    }

    /// Advances a timed-out step without requiring an ordinary queue message.
    pub fn poll(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<ShutdownProgress, ShutdownCoordinatorError> {
        match self.state {
            CoordinatorState::Idle => Ok(ShutdownProgress::Idle),
            CoordinatorState::Complete(report) => Ok(ShutdownProgress::Complete(report)),
            CoordinatorState::Running { ticket, .. } if now < ticket.deadline => {
                Ok(ShutdownProgress::Waiting(ticket))
            }
            CoordinatorState::Running { index, ticket } => {
                self.record_failure(ticket.step, ShutdownFailureKind::TimedOut);
                self.advance(index, ticket.step, now)
            }
        }
    }

    pub fn complete(
        &mut self,
        ticket: ShutdownTicket,
        result: ShutdownStepResult,
        now: MonotonicInstant,
    ) -> Result<ShutdownProgress, ShutdownCoordinatorError> {
        let (index, expected) = match self.state {
            CoordinatorState::Running { index, ticket } => (index, ticket),
            CoordinatorState::Idle | CoordinatorState::Complete(_) => {
                return Err(ShutdownCoordinatorError::NotRunning);
            }
        };
        if ticket != expected {
            return Err(ShutdownCoordinatorError::StaleTicket {
                expected,
                actual: ticket,
            });
        }
        if now >= expected.deadline {
            self.record_failure(expected.step, ShutdownFailureKind::TimedOut);
        } else if result == ShutdownStepResult::Failed {
            self.record_failure(expected.step, ShutdownFailureKind::Failed);
        }
        self.advance(index, expected.step, now)
    }

    fn advance(
        &mut self,
        index: usize,
        completed: ShutdownStep,
        now: MonotonicInstant,
    ) -> Result<ShutdownProgress, ShutdownCoordinatorError> {
        let next_index = index + 1;
        let steps = self.profile.steps();
        if next_index == steps.len() {
            self.state = CoordinatorState::Complete(self.report);
            return Ok(ShutdownProgress::Complete(self.report));
        }
        let next = self.make_ticket(steps[next_index], now)?;
        self.state = CoordinatorState::Running {
            index: next_index,
            ticket: next,
        };
        Ok(ShutdownProgress::Advanced { completed, next })
    }

    fn make_ticket(
        &mut self,
        step: ShutdownStep,
        now: MonotonicInstant,
    ) -> Result<ShutdownTicket, ShutdownCoordinatorError> {
        let deadline = now
            .checked_add(Duration::from_millis(self.step_timeout_ms.get()))
            .ok_or(ShutdownCoordinatorError::DeadlineOutOfRange)?;
        let id = NonZeroU64::new(self.next_ticket_id)
            .ok_or(ShutdownCoordinatorError::TicketIdExhausted)?;
        self.next_ticket_id = self.next_ticket_id.checked_add(1).unwrap_or(0);
        Ok(ShutdownTicket {
            authority_id: self.authority_id,
            id,
            step,
            deadline,
            checkpoint_dirty: !self.report.is_clean(),
        })
    }

    fn record_failure(&mut self, step: ShutdownStep, kind: ShutdownFailureKind) {
        self.report.failed_steps |= step.bit();
        if kind == ShutdownFailureKind::TimedOut {
            self.report.timed_out_steps |= step.bit();
        }
        if self.report.first_failure.is_none() {
            self.report.first_failure = Some(ShutdownFailure { step, kind });
        }
    }
}

fn allocate_shutdown_authority_id() -> Option<NonZeroU64> {
    let mut current = NEXT_SHUTDOWN_AUTHORITY_ID.load(Ordering::Relaxed);
    loop {
        let authority_id = NonZeroU64::new(current)?;
        let next = current.checked_add(1).unwrap_or(0);
        match NEXT_SHUTDOWN_AUTHORITY_ID.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Some(authority_id),
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ShutdownCoordinator, ShutdownCoordinatorError, ShutdownFailure, ShutdownFailureKind,
        ShutdownProfile, ShutdownProgress, ShutdownStep, ShutdownStepResult,
    };
    use ariax_core::{MAX_PERSISTED_MILLISECONDS, MonotonicInstant};
    use std::time::Duration;

    fn later(now: MonotonicInstant, milliseconds: u64) -> MonotonicInstant {
        now.checked_add(Duration::from_millis(milliseconds))
            .expect("test deadline")
    }

    #[test]
    fn minimal_shutdown_runs_exact_order_and_finishes_clean() {
        let now = MonotonicInstant::now();
        let mut coordinator =
            ShutdownCoordinator::new(ShutdownProfile::Minimal, 1_000).expect("coordinator");
        let mut ticket = coordinator.begin(now).expect("start shutdown");
        let expected = [
            ShutdownStep::StopAdmission,
            ShutdownStep::DrainDiskCpu,
            ShutdownStep::FlushJournal,
            ShutdownStep::PersistSession,
        ];

        for (index, step) in expected.into_iter().enumerate() {
            assert_eq!(ticket.step(), step);
            let progress = coordinator
                .complete(
                    ticket,
                    ShutdownStepResult::Succeeded,
                    later(now, index as u64 + 1),
                )
                .expect("ordered completion");
            match progress {
                ShutdownProgress::Advanced { next, .. } => ticket = next,
                ShutdownProgress::Complete(report) => {
                    assert_eq!(index, expected.len() - 1);
                    assert!(report.is_clean());
                }
                other => panic!("unexpected progress: {other:?}"),
            }
        }
    }

    #[test]
    fn earlier_failure_marks_session_checkpoint_dirty_and_retains_first_failure() {
        let now = MonotonicInstant::now();
        let mut coordinator =
            ShutdownCoordinator::new(ShutdownProfile::BitTorrent, 1_000).expect("coordinator");
        let mut ticket = coordinator.begin(now).expect("start shutdown");
        loop {
            let result = if ticket.step() == ShutdownStep::CheckpointBitTorrent {
                ShutdownStepResult::Failed
            } else {
                ShutdownStepResult::Succeeded
            };
            let progress = coordinator
                .complete(ticket, result, later(now, 1))
                .expect("ordered completion");
            match progress {
                ShutdownProgress::Advanced { next, .. } => {
                    if next.step() == ShutdownStep::PersistSession {
                        assert!(next.checkpoint_dirty());
                    }
                    ticket = next;
                }
                ShutdownProgress::Complete(report) => {
                    assert!(!report.is_clean());
                    assert!(report.step_failed(ShutdownStep::CheckpointBitTorrent));
                    assert_eq!(
                        report.first_failure(),
                        Some(ShutdownFailure {
                            step: ShutdownStep::CheckpointBitTorrent,
                            kind: ShutdownFailureKind::Failed,
                        })
                    );
                    break;
                }
                other => panic!("unexpected progress: {other:?}"),
            }
        }
    }

    #[test]
    fn timeout_advances_out_of_band_and_rejects_late_completion() {
        let now = MonotonicInstant::now();
        let mut coordinator =
            ShutdownCoordinator::new(ShutdownProfile::Minimal, 10).expect("coordinator");
        let expired = coordinator.begin(now).expect("start shutdown");
        let progress = coordinator.poll(later(now, 10)).expect("timeout poll");
        let next = match progress {
            ShutdownProgress::Advanced { next, .. } => next,
            other => panic!("unexpected progress: {other:?}"),
        };
        assert_eq!(next.step(), ShutdownStep::DrainDiskCpu);
        assert!(next.checkpoint_dirty());
        assert_eq!(
            coordinator.complete(expired, ShutdownStepResult::Succeeded, later(now, 11)),
            Err(ShutdownCoordinatorError::StaleTicket {
                expected: next,
                actual: expired,
            })
        );
        let report = coordinator.report();
        assert_eq!(report, None);
    }

    #[test]
    fn construction_and_duplicate_begin_are_rejected() {
        assert_eq!(
            ShutdownCoordinator::new(ShutdownProfile::Minimal, 0),
            Err(ShutdownCoordinatorError::InvalidStepTimeout)
        );
        assert_eq!(
            ShutdownCoordinator::new(ShutdownProfile::Minimal, MAX_PERSISTED_MILLISECONDS + 1,),
            Err(ShutdownCoordinatorError::InvalidStepTimeout)
        );

        let now = MonotonicInstant::now();
        let mut coordinator =
            ShutdownCoordinator::new(ShutdownProfile::Minimal, 1).expect("coordinator");
        coordinator.begin(now).expect("first begin");
        assert_eq!(
            coordinator.begin(now),
            Err(ShutdownCoordinatorError::AlreadyStarted)
        );
    }

    #[test]
    fn tickets_are_bound_to_the_exact_coordinator_authority() {
        let now = MonotonicInstant::now();
        let mut first =
            ShutdownCoordinator::new(ShutdownProfile::Minimal, 1_000).expect("first coordinator");
        let mut second =
            ShutdownCoordinator::new(ShutdownProfile::Minimal, 1_000).expect("second coordinator");
        let foreign = first.begin(now).expect("first ticket");
        let expected = second.begin(now).expect("second ticket");

        assert_eq!(foreign.id(), expected.id());
        assert_eq!(foreign.step(), expected.step());
        assert_eq!(foreign.deadline(), expected.deadline());
        assert_ne!(foreign, expected);
        assert_eq!(
            second.complete(foreign, ShutdownStepResult::Succeeded, later(now, 1),),
            Err(ShutdownCoordinatorError::StaleTicket {
                expected,
                actual: foreign,
            })
        );
        assert_eq!(second.current_ticket(), Some(expected));
        assert!(matches!(
            second
                .complete(expected, ShutdownStepResult::Succeeded, later(now, 1),)
                .expect("exact authority completion"),
            ShutdownProgress::Advanced {
                completed: ShutdownStep::StopAdmission,
                ..
            }
        ));
    }
}
