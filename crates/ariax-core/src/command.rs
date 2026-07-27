use crate::{
    Aria2Status, Generation, Gid, HostKeyChallenge, HostKeyChallengeId, HostKeyFingerprint,
    MonotonicInstant, NoSpaceCondition, NoSpaceProbeId, OptionPatchId, PublicError, RetryTimerId,
    SlowReadmissionId, StateTransition, TaskConditions, TaskId, TaskSnapshot, TaskState,
};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

/// Maximum number of ordered side effects emitted by one scheduler operation.
pub const MAX_SCHEDULER_EFFECTS: usize = 8;

/// Fixed bounds and policies owned by one scheduler instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerConfig {
    pub max_tasks: NonZeroUsize,
    pub max_active_tasks: NonZeroUsize,
    pub retry_wait_holds_slot: bool,
}

impl SchedulerConfig {
    /// Creates a scheduler configuration whose active cap fits inside its task cap.
    pub fn new(
        max_tasks: NonZeroUsize,
        max_active_tasks: NonZeroUsize,
        retry_wait_holds_slot: bool,
    ) -> Result<Self, SchedulerConfigError> {
        if max_active_tasks.get() > max_tasks.get() {
            return Err(SchedulerConfigError::ActiveLimitExceedsTaskLimit);
        }
        Ok(Self {
            max_tasks,
            max_active_tasks,
            retry_wait_holds_slot,
        })
    }
}

/// Why scheduler bounds could not be constructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerConfigError {
    ActiveLimitExceedsTaskLimit,
}

impl fmt::Display for SchedulerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("active task limit exceeds total task limit")
    }
}

impl Error for SchedulerConfigError {}

/// Explicit scheduler membership. It is never inferred from wire status.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum QueueClass {
    Waiting,
    Demoted,
    Paused,
    Active,
    Stopped,
}

pub const ALL_QUEUE_CLASSES: &[QueueClass] = &[
    QueueClass::Waiting,
    QueueClass::Demoted,
    QueueClass::Paused,
    QueueClass::Active,
    QueueClass::Stopped,
];

impl QueueClass {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Demoted => "demoted",
            Self::Paused => "paused",
            Self::Active => "active",
            Self::Stopped => "stopped",
        }
    }
}

/// One task's ownership of the global active-task budget.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SlotOwnership {
    None,
    Reserved,
    Active,
    RetryRetained,
}

pub const ALL_SLOT_OWNERSHIP: &[SlotOwnership] = &[
    SlotOwnership::None,
    SlotOwnership::Reserved,
    SlotOwnership::Active,
    SlotOwnership::RetryRetained,
];

impl SlotOwnership {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Reserved => "reserved",
            Self::Active => "active",
            Self::RetryRetained => "retry_retained",
        }
    }

    #[must_use]
    pub const fn owns_slot(self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Destination applied after an in-flight generation acknowledges cancellation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DrainTarget {
    Paused,
    PausedSlow,
    Waiting,
    WaitingSlow,
    PausedRestarting,
    Error,
    Removed,
}

pub const ALL_DRAIN_TARGETS: &[DrainTarget] = &[
    DrainTarget::Paused,
    DrainTarget::PausedSlow,
    DrainTarget::Waiting,
    DrainTarget::WaitingSlow,
    DrainTarget::PausedRestarting,
    DrainTarget::Error,
    DrainTarget::Removed,
];

impl DrainTarget {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::PausedSlow => "paused_slow",
            Self::Waiting => "waiting",
            Self::WaitingSlow => "waiting_slow",
            Self::PausedRestarting => "paused_restarting",
            Self::Error => "error",
            Self::Removed => "removed",
        }
    }

    #[must_use]
    pub const fn state(self) -> TaskState {
        match self {
            Self::Paused => TaskState::Paused,
            Self::PausedSlow => TaskState::PausedSlow,
            Self::Waiting => TaskState::Waiting,
            Self::WaitingSlow => TaskState::WaitingSlow,
            Self::PausedRestarting => TaskState::PausedRestarting,
            Self::Error => TaskState::Error,
            Self::Removed => TaskState::Removed,
        }
    }
}

/// An asynchronous durability or cancellation acknowledgement still required.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingBarrier {
    GenerationPersistence {
        generation: Generation,
    },
    CancellationDrain {
        generation: Generation,
        target: DrainTarget,
        force: bool,
    },
    TerminalPersistence {
        generation: Generation,
        status: Aria2Status,
    },
}

impl PendingBarrier {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::GenerationPersistence { .. } => "generation_persistence",
            Self::CancellationDrain { .. } => "cancellation_drain",
            Self::TerminalPersistence { .. } => "terminal_persistence",
        }
    }
}

/// Commands accepted from CLI, RPC, or library adapters in the first slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulerCommand {
    AddValidatedTask {
        task_id: TaskId,
        gid: Gid,
        desired_paused: bool,
        conditions: TaskConditions,
    },
    Pause {
        gid: Gid,
        force: bool,
    },
    Resume {
        gid: Gid,
    },
    ApproveHostKey {
        gid: Gid,
        challenge: HostKeyChallengeId,
        fingerprint_sha256: HostKeyFingerprint,
    },
    SatisfyCredentials {
        gid: Gid,
    },
    ApplyOptionPatch {
        gid: Gid,
        patch_id: OptionPatchId,
    },
    Remove {
        gid: Gid,
        force: bool,
    },
    RemoveStoppedResult {
        gid: Gid,
    },
    ChangePosition {
        gid: Gid,
        position: usize,
    },
    OrderlyShutdown,
}

/// Closed scheduler-command vocabulary, independent of command payloads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SchedulerCommandKind {
    AddValidatedTask,
    Pause,
    Resume,
    ApproveHostKey,
    SatisfyCredentials,
    ApplyOptionPatch,
    Remove,
    RemoveStoppedResult,
    ChangePosition,
    OrderlyShutdown,
}

/// Whether a command is represented by the per-task state matrix.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SchedulerCommandHandling {
    StateMatrix,
    QueueOperation,
}

pub const ALL_SCHEDULER_COMMAND_HANDLINGS: &[SchedulerCommandHandling] = &[
    SchedulerCommandHandling::StateMatrix,
    SchedulerCommandHandling::QueueOperation,
];

impl SchedulerCommandHandling {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::StateMatrix => "state_matrix",
            Self::QueueOperation => "queue_operation",
        }
    }
}

pub const ALL_SCHEDULER_COMMAND_KINDS: &[SchedulerCommandKind] = &[
    SchedulerCommandKind::AddValidatedTask,
    SchedulerCommandKind::Pause,
    SchedulerCommandKind::Resume,
    SchedulerCommandKind::ApproveHostKey,
    SchedulerCommandKind::SatisfyCredentials,
    SchedulerCommandKind::ApplyOptionPatch,
    SchedulerCommandKind::Remove,
    SchedulerCommandKind::RemoveStoppedResult,
    SchedulerCommandKind::ChangePosition,
    SchedulerCommandKind::OrderlyShutdown,
];

impl SchedulerCommandKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::AddValidatedTask => "add_validated_task",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::ApproveHostKey => "approve_host_key",
            Self::SatisfyCredentials => "satisfy_credentials",
            Self::ApplyOptionPatch => "apply_option_patch",
            Self::Remove => "remove",
            Self::RemoveStoppedResult => "remove_stopped_result",
            Self::ChangePosition => "change_position",
            Self::OrderlyShutdown => "orderly_shutdown",
        }
    }

    /// Declares whether this command changes one task state or only queue order.
    #[must_use]
    pub const fn handling(self) -> SchedulerCommandHandling {
        match self {
            Self::ChangePosition => SchedulerCommandHandling::QueueOperation,
            Self::AddValidatedTask
            | Self::Pause
            | Self::Resume
            | Self::ApproveHostKey
            | Self::SatisfyCredentials
            | Self::ApplyOptionPatch
            | Self::Remove
            | Self::RemoveStoppedResult
            | Self::OrderlyShutdown => SchedulerCommandHandling::StateMatrix,
        }
    }
}

impl SchedulerCommand {
    #[must_use]
    pub const fn kind(&self) -> SchedulerCommandKind {
        match self {
            Self::AddValidatedTask { .. } => SchedulerCommandKind::AddValidatedTask,
            Self::Pause { .. } => SchedulerCommandKind::Pause,
            Self::Resume { .. } => SchedulerCommandKind::Resume,
            Self::ApproveHostKey { .. } => SchedulerCommandKind::ApproveHostKey,
            Self::SatisfyCredentials { .. } => SchedulerCommandKind::SatisfyCredentials,
            Self::ApplyOptionPatch { .. } => SchedulerCommandKind::ApplyOptionPatch,
            Self::Remove { .. } => SchedulerCommandKind::Remove,
            Self::RemoveStoppedResult { .. } => SchedulerCommandKind::RemoveStoppedResult,
            Self::ChangePosition { .. } => SchedulerCommandKind::ChangePosition,
            Self::OrderlyShutdown => SchedulerCommandKind::OrderlyShutdown,
        }
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.kind().code()
    }
}

/// Validated asynchronous results delivered back to the scheduler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskEvent {
    GenerationPersisted {
        gid: Gid,
        generation: Generation,
    },
    AllocationSucceeded {
        gid: Gid,
        generation: Generation,
    },
    AllocationRetryable {
        gid: Gid,
        generation: Generation,
        retry_at: MonotonicInstant,
    },
    AllocationHostKeyChallenge {
        gid: Gid,
        generation: Generation,
        challenge: HostKeyChallenge,
    },
    AllocationFailed {
        gid: Gid,
        generation: Generation,
        error: PublicError,
    },
    RetryReady {
        gid: Gid,
        generation: Generation,
        retry_timer_id: RetryTimerId,
    },
    ActiveRetryIdle {
        gid: Gid,
        generation: Generation,
        retry_at: MonotonicInstant,
    },
    DataComplete {
        gid: Gid,
        generation: Generation,
        seed: bool,
    },
    NoSpace {
        gid: Gid,
        generation: Generation,
        condition: NoSpaceCondition,
    },
    TerminalFailure {
        gid: Gid,
        generation: Generation,
        error: PublicError,
    },
    SlowDemoted {
        gid: Gid,
        generation: Generation,
        readmit_at: MonotonicInstant,
    },
    SlowPaused {
        gid: Gid,
        generation: Generation,
    },
    SlowReadmit {
        gid: Gid,
        generation: Generation,
        readmission_id: SlowReadmissionId,
    },
    VerificationSucceeded {
        gid: Gid,
        generation: Generation,
    },
    VerificationRecoverable {
        gid: Gid,
        generation: Generation,
    },
    VerificationFailed {
        gid: Gid,
        generation: Generation,
        error: PublicError,
    },
    SeedingComplete {
        gid: Gid,
        generation: Generation,
    },
    SeedingFailed {
        gid: Gid,
        generation: Generation,
        error: PublicError,
    },
    CancellationDrained {
        gid: Gid,
        generation: Generation,
    },
    NoSpaceProbeCompleted {
        gid: Gid,
        generation: Generation,
        probe_id: NoSpaceProbeId,
        origin: NoSpaceProbeOrigin,
        ready: bool,
        next_retry_at: Option<MonotonicInstant>,
    },
    TerminalPersisted {
        gid: Gid,
        generation: Generation,
        status: Aria2Status,
    },
}

/// Why a no-space readiness probe was issued.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum NoSpaceProbeOrigin {
    ExplicitResume,
    AutomaticRetry,
}

pub const ALL_NO_SPACE_PROBE_ORIGINS: &[NoSpaceProbeOrigin] = &[
    NoSpaceProbeOrigin::ExplicitResume,
    NoSpaceProbeOrigin::AutomaticRetry,
];

impl NoSpaceProbeOrigin {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ExplicitResume => "explicit_resume",
            Self::AutomaticRetry => "automatic_retry",
        }
    }
}

/// Typed identity carried by asynchronous scheduler events that may outlive
/// the state which scheduled them.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TaskEventToken {
    RetryTimer(RetryTimerId),
    SlowReadmission(SlowReadmissionId),
    NoSpaceProbe(NoSpaceProbeId),
}

/// Closed asynchronous-event vocabulary, independent of event payloads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TaskEventKind {
    GenerationPersisted,
    AllocationSucceeded,
    AllocationRetryable,
    AllocationHostKeyChallenge,
    AllocationFailed,
    RetryReady,
    ActiveRetryIdle,
    DataComplete,
    BitTorrentPayloadComplete,
    NoSpace,
    TerminalFailure,
    SlowDemoted,
    SlowPaused,
    SlowReadmit,
    VerificationSucceeded,
    VerificationRecoverable,
    VerificationFailed,
    SeedingComplete,
    SeedingFailed,
    CancellationDrained,
    NoSpaceProbeCompleted,
    TerminalPersisted,
}

pub const ALL_TASK_EVENT_KINDS: &[TaskEventKind] = &[
    TaskEventKind::GenerationPersisted,
    TaskEventKind::AllocationSucceeded,
    TaskEventKind::AllocationRetryable,
    TaskEventKind::AllocationHostKeyChallenge,
    TaskEventKind::AllocationFailed,
    TaskEventKind::RetryReady,
    TaskEventKind::ActiveRetryIdle,
    TaskEventKind::DataComplete,
    TaskEventKind::BitTorrentPayloadComplete,
    TaskEventKind::NoSpace,
    TaskEventKind::TerminalFailure,
    TaskEventKind::SlowDemoted,
    TaskEventKind::SlowPaused,
    TaskEventKind::SlowReadmit,
    TaskEventKind::VerificationSucceeded,
    TaskEventKind::VerificationRecoverable,
    TaskEventKind::VerificationFailed,
    TaskEventKind::SeedingComplete,
    TaskEventKind::SeedingFailed,
    TaskEventKind::CancellationDrained,
    TaskEventKind::NoSpaceProbeCompleted,
    TaskEventKind::TerminalPersisted,
];

impl TaskEventKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::GenerationPersisted => "generation_persisted",
            Self::AllocationSucceeded => "allocation_succeeded",
            Self::AllocationRetryable => "allocation_retryable",
            Self::AllocationHostKeyChallenge => "allocation_host_key_challenge",
            Self::AllocationFailed => "allocation_failed",
            Self::RetryReady => "retry_ready",
            Self::ActiveRetryIdle => "active_retry_idle",
            Self::DataComplete => "data_complete",
            Self::BitTorrentPayloadComplete => "bt_payload_complete",
            Self::NoSpace => "no_space",
            Self::TerminalFailure => "terminal_failure",
            Self::SlowDemoted => "slow_demoted",
            Self::SlowPaused => "slow_paused",
            Self::SlowReadmit => "slow_readmit",
            Self::VerificationSucceeded => "verification_succeeded",
            Self::VerificationRecoverable => "verification_recoverable",
            Self::VerificationFailed => "verification_failed",
            Self::SeedingComplete => "seeding_complete",
            Self::SeedingFailed => "seeding_failed",
            Self::CancellationDrained => "cancellation_drained",
            Self::NoSpaceProbeCompleted => "no_space_probe_completed",
            Self::TerminalPersisted => "terminal_persisted",
        }
    }
}

impl TaskEvent {
    #[must_use]
    pub const fn gid(&self) -> Gid {
        match self {
            Self::GenerationPersisted { gid, .. }
            | Self::AllocationSucceeded { gid, .. }
            | Self::AllocationRetryable { gid, .. }
            | Self::AllocationHostKeyChallenge { gid, .. }
            | Self::AllocationFailed { gid, .. }
            | Self::RetryReady { gid, .. }
            | Self::ActiveRetryIdle { gid, .. }
            | Self::DataComplete { gid, .. }
            | Self::NoSpace { gid, .. }
            | Self::TerminalFailure { gid, .. }
            | Self::SlowDemoted { gid, .. }
            | Self::SlowPaused { gid, .. }
            | Self::SlowReadmit { gid, .. }
            | Self::VerificationSucceeded { gid, .. }
            | Self::VerificationRecoverable { gid, .. }
            | Self::VerificationFailed { gid, .. }
            | Self::SeedingComplete { gid, .. }
            | Self::SeedingFailed { gid, .. }
            | Self::CancellationDrained { gid, .. }
            | Self::NoSpaceProbeCompleted { gid, .. }
            | Self::TerminalPersisted { gid, .. } => *gid,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        match self {
            Self::GenerationPersisted { generation, .. }
            | Self::AllocationSucceeded { generation, .. }
            | Self::AllocationRetryable { generation, .. }
            | Self::AllocationHostKeyChallenge { generation, .. }
            | Self::AllocationFailed { generation, .. }
            | Self::RetryReady { generation, .. }
            | Self::ActiveRetryIdle { generation, .. }
            | Self::DataComplete { generation, .. }
            | Self::NoSpace { generation, .. }
            | Self::TerminalFailure { generation, .. }
            | Self::SlowDemoted { generation, .. }
            | Self::SlowPaused { generation, .. }
            | Self::SlowReadmit { generation, .. }
            | Self::VerificationSucceeded { generation, .. }
            | Self::VerificationRecoverable { generation, .. }
            | Self::VerificationFailed { generation, .. }
            | Self::SeedingComplete { generation, .. }
            | Self::SeedingFailed { generation, .. }
            | Self::CancellationDrained { generation, .. }
            | Self::NoSpaceProbeCompleted { generation, .. }
            | Self::TerminalPersisted { generation, .. } => *generation,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> TaskEventKind {
        match self {
            Self::GenerationPersisted { .. } => TaskEventKind::GenerationPersisted,
            Self::AllocationSucceeded { .. } => TaskEventKind::AllocationSucceeded,
            Self::AllocationRetryable { .. } => TaskEventKind::AllocationRetryable,
            Self::AllocationHostKeyChallenge { .. } => TaskEventKind::AllocationHostKeyChallenge,
            Self::AllocationFailed { .. } => TaskEventKind::AllocationFailed,
            Self::RetryReady { .. } => TaskEventKind::RetryReady,
            Self::ActiveRetryIdle { .. } => TaskEventKind::ActiveRetryIdle,
            Self::DataComplete { seed: false, .. } => TaskEventKind::DataComplete,
            Self::DataComplete { seed: true, .. } => TaskEventKind::BitTorrentPayloadComplete,
            Self::NoSpace { .. } => TaskEventKind::NoSpace,
            Self::TerminalFailure { .. } => TaskEventKind::TerminalFailure,
            Self::SlowDemoted { .. } => TaskEventKind::SlowDemoted,
            Self::SlowPaused { .. } => TaskEventKind::SlowPaused,
            Self::SlowReadmit { .. } => TaskEventKind::SlowReadmit,
            Self::VerificationSucceeded { .. } => TaskEventKind::VerificationSucceeded,
            Self::VerificationRecoverable { .. } => TaskEventKind::VerificationRecoverable,
            Self::VerificationFailed { .. } => TaskEventKind::VerificationFailed,
            Self::SeedingComplete { .. } => TaskEventKind::SeedingComplete,
            Self::SeedingFailed { .. } => TaskEventKind::SeedingFailed,
            Self::CancellationDrained { .. } => TaskEventKind::CancellationDrained,
            Self::NoSpaceProbeCompleted { .. } => TaskEventKind::NoSpaceProbeCompleted,
            Self::TerminalPersisted { .. } => TaskEventKind::TerminalPersisted,
        }
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.kind().code()
    }

    /// Returns the scheduler-issued identity used to reject stale or duplicate
    /// timer, readmission, and probe completions.
    #[must_use]
    pub const fn token(&self) -> Option<TaskEventToken> {
        match self {
            Self::RetryReady { retry_timer_id, .. } => {
                Some(TaskEventToken::RetryTimer(*retry_timer_id))
            }
            Self::SlowReadmit { readmission_id, .. } => {
                Some(TaskEventToken::SlowReadmission(*readmission_id))
            }
            Self::NoSpaceProbeCompleted { probe_id, .. } => {
                Some(TaskEventToken::NoSpaceProbe(*probe_id))
            }
            _ => None,
        }
    }
}

/// Ordered work handed to persistence, worker, timer, and snapshot adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionEffect {
    PersistTask {
        gid: Gid,
    },
    PersistDesiredPaused {
        gid: Gid,
        paused: bool,
    },
    PersistQueueOrder {
        class: QueueClass,
        order: Vec<Gid>,
    },
    PersistGenerationStarted {
        gid: Gid,
        generation: Generation,
    },
    StartAllocation {
        gid: Gid,
        generation: Generation,
    },
    CancelGeneration {
        gid: Gid,
        generation: Generation,
        force: bool,
    },
    ReleaseSlot {
        gid: Gid,
        ownership: SlotOwnership,
    },
    ScheduleRetry {
        gid: Gid,
        generation: Generation,
        retry_timer_id: RetryTimerId,
        at: MonotonicInstant,
    },
    CancelRetry {
        gid: Gid,
        generation: Generation,
        retry_timer_id: RetryTimerId,
    },
    ScheduleSlowReadmission {
        gid: Gid,
        generation: Generation,
        readmission_id: SlowReadmissionId,
        at: MonotonicInstant,
    },
    CancelSlowReadmission {
        gid: Gid,
        generation: Generation,
        readmission_id: SlowReadmissionId,
    },
    ProbeNoSpace {
        gid: Gid,
        generation: Generation,
        probe_id: NoSpaceProbeId,
        origin: NoSpaceProbeOrigin,
    },
    PersistConditions {
        gid: Gid,
    },
    PersistHostKeyChallenge {
        gid: Gid,
        challenge: HostKeyChallenge,
    },
    ClearHostKeyChallenge {
        gid: Gid,
    },
    PersistTerminal {
        gid: Gid,
        generation: Generation,
        status: Aria2Status,
        error: Option<PublicError>,
    },
    PublishSnapshot(TaskSnapshot),
}

/// Result of one accepted command or event.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchedulerOutcome {
    pub transition: Option<StateTransition>,
    pub effects: Vec<TransitionEffect>,
}

impl SchedulerOutcome {
    #[must_use]
    pub const fn is_noop(&self) -> bool {
        self.transition.is_none() && self.effects.is_empty()
    }
}

/// Deterministic scheduler failures. Rejected work has no side effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerError {
    TaskLimitReached,
    GidCollision,
    TaskIdCollision,
    TaskNotFound,
    ActiveLimitReached,
    NoEligibleTask,
    InvalidPosition {
        position: usize,
        queue_len: usize,
    },
    StaleGeneration {
        expected: Generation,
        actual: Generation,
    },
    Conflict {
        state: TaskState,
        operation: &'static str,
    },
    PendingBarrier {
        barrier: PendingBarrier,
        operation: &'static str,
    },
    GenerationExhausted,
    HostKeyApprovalRequired,
    StaleChallenge,
    InvalidTerminalAcknowledgement,
    EffectLimitExceeded,
    InternalInvariant,
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskLimitReached => formatter.write_str("scheduler task limit reached"),
            Self::GidCollision => formatter.write_str("GID already exists"),
            Self::TaskIdCollision => formatter.write_str("task id already exists"),
            Self::TaskNotFound => formatter.write_str("task does not exist"),
            Self::ActiveLimitReached => formatter.write_str("active task limit reached"),
            Self::NoEligibleTask => {
                formatter.write_str("no waiting task is eligible for admission")
            }
            Self::InvalidPosition {
                position,
                queue_len,
            } => write!(
                formatter,
                "queue position {position} is outside queue length {queue_len}"
            ),
            Self::StaleGeneration { expected, actual } => write!(
                formatter,
                "stale generation {} does not match current generation {}",
                actual.get(),
                expected.get()
            ),
            Self::Conflict { state, operation } => {
                write!(
                    formatter,
                    "{operation} conflicts with state {}",
                    state.code()
                )
            }
            Self::PendingBarrier { barrier, operation } => write!(
                formatter,
                "{operation} conflicts with pending {} barrier",
                barrier.code()
            ),
            Self::GenerationExhausted => formatter.write_str("task generation exhausted"),
            Self::HostKeyApprovalRequired => {
                formatter.write_str("generic resume cannot approve a host key")
            }
            Self::StaleChallenge => {
                formatter.write_str("host-key challenge is stale or mismatched")
            }
            Self::InvalidTerminalAcknowledgement => {
                formatter.write_str("terminal persistence acknowledgement does not match")
            }
            Self::EffectLimitExceeded => formatter.write_str("scheduler effect limit exceeded"),
            Self::InternalInvariant => formatter.write_str("scheduler invariant failed"),
        }
    }
}

impl Error for SchedulerError {}

#[cfg(test)]
mod tests {
    use super::{
        ALL_DRAIN_TARGETS, ALL_NO_SPACE_PROBE_ORIGINS, ALL_QUEUE_CLASSES,
        ALL_SCHEDULER_COMMAND_HANDLINGS, ALL_SCHEDULER_COMMAND_KINDS, ALL_SLOT_OWNERSHIP,
        ALL_TASK_EVENT_KINDS, DrainTarget, NoSpaceProbeOrigin, SchedulerCommandHandling,
        SchedulerCommandKind, SchedulerConfig, SchedulerConfigError, SlotOwnership, TaskEvent,
        TaskEventKind, TaskEventToken, TransitionEffect,
    };
    use crate::{Aria2Status, Generation, Gid, NoSpaceProbeId, RetryTimerId, SlowReadmissionId};
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;

    #[test]
    fn command_event_queue_and_slot_vocabularies_are_closed_and_unique() {
        let commands: BTreeSet<_> = ALL_SCHEDULER_COMMAND_KINDS
            .iter()
            .map(|kind| kind.code())
            .collect();
        let events: BTreeSet<_> = ALL_TASK_EVENT_KINDS
            .iter()
            .map(|kind| kind.code())
            .collect();
        let queues: BTreeSet<_> = ALL_QUEUE_CLASSES.iter().map(|class| class.code()).collect();
        let slots: BTreeSet<_> = ALL_SLOT_OWNERSHIP
            .iter()
            .map(|ownership| ownership.code())
            .collect();
        let probe_origins: BTreeSet<_> = ALL_NO_SPACE_PROBE_ORIGINS
            .iter()
            .map(|origin| origin.code())
            .collect();
        let command_handlings: BTreeSet<_> = ALL_SCHEDULER_COMMAND_HANDLINGS
            .iter()
            .map(|handling| handling.code())
            .collect();
        let drain_targets: BTreeSet<_> = ALL_DRAIN_TARGETS
            .iter()
            .map(|target| target.code())
            .collect();

        assert_eq!(commands.len(), ALL_SCHEDULER_COMMAND_KINDS.len());
        assert_eq!(events.len(), ALL_TASK_EVENT_KINDS.len());
        assert_eq!(queues.len(), ALL_QUEUE_CLASSES.len());
        assert_eq!(slots.len(), ALL_SLOT_OWNERSHIP.len());
        assert_eq!(probe_origins.len(), ALL_NO_SPACE_PROBE_ORIGINS.len());
        assert_eq!(
            command_handlings.len(),
            ALL_SCHEDULER_COMMAND_HANDLINGS.len()
        );
        assert_eq!(drain_targets.len(), ALL_DRAIN_TARGETS.len());
        assert_eq!(ALL_SCHEDULER_COMMAND_KINDS.len(), 10);
        assert_eq!(ALL_TASK_EVENT_KINDS.len(), 22);
        assert_eq!(ALL_NO_SPACE_PROBE_ORIGINS.len(), 2);
        assert_eq!(ALL_SCHEDULER_COMMAND_HANDLINGS.len(), 2);
        assert_eq!(ALL_DRAIN_TARGETS.len(), 7);
        assert!(commands.contains(SchedulerCommandKind::Pause.code()));
        assert!(events.contains(TaskEventKind::TerminalPersisted.code()));
        assert_eq!(
            SchedulerCommandKind::ChangePosition.handling(),
            SchedulerCommandHandling::QueueOperation
        );
        assert_eq!(
            SchedulerCommandKind::Resume.handling(),
            SchedulerCommandHandling::StateMatrix
        );
    }

    #[test]
    fn long_lived_events_expose_typed_identity_tokens() {
        let gid = Gid::new(1).expect("gid");
        let generation = Generation::new(1);
        let retry_timer_id = RetryTimerId::new(2).expect("retry timer");
        let readmission_id = SlowReadmissionId::new(3).expect("readmission");
        let probe_id = NoSpaceProbeId::new(4).expect("probe");

        assert_eq!(
            TaskEvent::RetryReady {
                gid,
                generation,
                retry_timer_id,
            }
            .token(),
            Some(TaskEventToken::RetryTimer(retry_timer_id))
        );
        assert_eq!(
            TaskEvent::SlowReadmit {
                gid,
                generation,
                readmission_id,
            }
            .token(),
            Some(TaskEventToken::SlowReadmission(readmission_id))
        );
        assert_eq!(
            TaskEvent::NoSpaceProbeCompleted {
                gid,
                generation,
                probe_id,
                origin: NoSpaceProbeOrigin::ExplicitResume,
                ready: false,
                next_retry_at: None,
            }
            .token(),
            Some(TaskEventToken::NoSpaceProbe(probe_id))
        );
        assert_eq!(
            TaskEvent::GenerationPersisted { gid, generation }.token(),
            None
        );
    }

    #[test]
    fn data_completion_event_kind_distinguishes_verification_from_seeding() {
        let gid = Gid::new(1).expect("gid");
        let generation = Generation::new(1);
        let ordinary = TaskEvent::DataComplete {
            gid,
            generation,
            seed: false,
        };
        let bit_torrent = TaskEvent::DataComplete {
            gid,
            generation,
            seed: true,
        };

        assert_eq!(ordinary.kind(), TaskEventKind::DataComplete);
        assert_eq!(ordinary.code(), "data_complete");
        assert_eq!(bit_torrent.kind(), TaskEventKind::BitTorrentPayloadComplete);
        assert_eq!(bit_torrent.code(), "bt_payload_complete");
        assert_eq!(ordinary.gid(), gid);
        assert_eq!(ordinary.generation(), generation);
    }

    #[test]
    fn scheduler_bounds_reject_an_active_cap_above_the_task_cap() {
        let tasks = NonZeroUsize::new(2).expect("task cap");
        let active = NonZeroUsize::new(3).expect("active cap");
        assert_eq!(
            SchedulerConfig::new(tasks, active, false),
            Err(SchedulerConfigError::ActiveLimitExceedsTaskLimit)
        );
    }

    #[test]
    fn slot_ownership_is_explicit() {
        assert!(!SlotOwnership::None.owns_slot());
        for ownership in [
            SlotOwnership::Reserved,
            SlotOwnership::Active,
            SlotOwnership::RetryRetained,
        ] {
            assert!(ownership.owns_slot(), "{ownership:?}");
        }
    }

    #[test]
    fn cancellation_targets_and_terminal_persistence_keep_correlation() {
        for target in ALL_DRAIN_TARGETS {
            assert_eq!(target.code(), target.state().code());
        }
        assert!(ALL_DRAIN_TARGETS.contains(&DrainTarget::PausedSlow));

        let gid = Gid::new(1).expect("gid");
        let generation = Generation::new(7);
        let effect = TransitionEffect::PersistTerminal {
            gid,
            generation,
            status: Aria2Status::Complete,
            error: None,
        };
        assert!(matches!(
            effect,
            TransitionEffect::PersistTerminal {
                gid: actual_gid,
                generation: actual_generation,
                status: Aria2Status::Complete,
                error: None,
            } if actual_gid == gid && actual_generation == generation
        ));
    }
}
