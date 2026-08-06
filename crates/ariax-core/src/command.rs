use crate::{
    Aria2Status, CredentialRequirementKey, Generation, Gid, HostKeyChallengeId, HostKeyFingerprint,
    HostKeyResolutionId, MonotonicInstant, NoSpaceCondition, NoSpaceProbeId, OptionPatchId,
    PresentedHostKeyChallenge, PublicError, RetryTimerId, SlowReadmissionId, StateTransition,
    StoppedResultDeletionId, TaskConditions, TaskDeletion, TaskId, TaskSnapshot, TaskState,
};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

/// Maximum number of ordered side effects emitted by one scheduler operation.
pub const MAX_SCHEDULER_EFFECTS: usize = 8;
/// Maximum task count representable by the normative SQLite session store.
pub const MAX_SCHEDULER_TASKS: usize = 100_000;
/// Largest non-negative millisecond timestamp representable by SQLite INTEGER.
pub const MAX_PERSISTED_MILLISECONDS: u64 = i64::MAX as u64;

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
        if max_tasks.get() > MAX_SCHEDULER_TASKS {
            return Err(SchedulerConfigError::TaskLimitExceedsPersistenceLimit);
        }
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
    TaskLimitExceedsPersistenceLimit,
    ActiveLimitExceedsTaskLimit,
}

impl fmt::Display for SchedulerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TaskLimitExceedsPersistenceLimit => {
                "task limit exceeds the persisted session-store bound"
            }
            Self::ActiveLimitExceedsTaskLimit => "active task limit exceeds total task limit",
        })
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

/// One immutable queue order included in an atomic persisted queue transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueOrder {
    pub class: QueueClass,
    pub order: Vec<Gid>,
}

/// Bounded slow-slot metadata persisted atomically with demoted queue state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlowReadmissionDecision {
    pub readmit_at: MonotonicInstant,
    pub scheduled_at_ms: u64,
    pub delay_ms: u64,
}

/// Bounded slow-slot metadata persisted atomically with demoted queue state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlowSlotPersistence {
    pub original_position: usize,
    pub demotion_count: u32,
    pub decision: SlowReadmissionDecision,
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
    OptionPatchPersistence {
        generation: Generation,
        patch_id: OptionPatchId,
    },
    OptionPatchApplication {
        generation: Generation,
        patch_id: OptionPatchId,
    },
    HostKeyResolution {
        generation: Generation,
        resolution_id: HostKeyResolutionId,
        challenge: HostKeyChallengeId,
    },
    StoppedResultDeletion {
        generation: Generation,
        deletion_id: StoppedResultDeletionId,
    },
}

impl PendingBarrier {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::GenerationPersistence { .. } => "generation_persistence",
            Self::CancellationDrain { .. } => "cancellation_drain",
            Self::TerminalPersistence { .. } => "terminal_persistence",
            Self::OptionPatchPersistence { .. } => "option_patch_persistence",
            Self::OptionPatchApplication { .. } => "option_patch_application",
            Self::HostKeyResolution { .. } => "host_key_resolution",
            Self::StoppedResultDeletion { .. } => "stopped_result_deletion",
        }
    }
}

/// Scheduler-relevant behavior of an option patch that has already passed
/// registry and value validation in `ariax-config`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ValidatedOptionPatchKind {
    /// Apply without restarting a live worker generation.
    InPlace,
    /// Stage now, cancel the current generation, and apply after quiescence.
    ActiveRestart,
    /// Apply a host-key pin that names the currently displayed challenge.
    MatchingHostKey {
        challenge: HostKeyChallengeId,
        fingerprint_sha256: HostKeyFingerprint,
    },
}

impl ValidatedOptionPatchKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InPlace => "in_place",
            Self::ActiveRestart => "active_restart",
            Self::MatchingHostKey { .. } => "matching_host_key",
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
    ApplyOptionPatch {
        gid: Gid,
        patch_id: OptionPatchId,
        kind: ValidatedOptionPatchKind,
        satisfies_credentials: Option<CredentialRequirementKey>,
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
    BatchOperation,
}

pub const ALL_SCHEDULER_COMMAND_HANDLINGS: &[SchedulerCommandHandling] = &[
    SchedulerCommandHandling::StateMatrix,
    SchedulerCommandHandling::QueueOperation,
    SchedulerCommandHandling::BatchOperation,
];

impl SchedulerCommandHandling {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::StateMatrix => "state_matrix",
            Self::QueueOperation => "queue_operation",
            Self::BatchOperation => "batch_operation",
        }
    }
}

pub const ALL_SCHEDULER_COMMAND_KINDS: &[SchedulerCommandKind] = &[
    SchedulerCommandKind::AddValidatedTask,
    SchedulerCommandKind::Pause,
    SchedulerCommandKind::Resume,
    SchedulerCommandKind::ApproveHostKey,
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
            Self::OrderlyShutdown => SchedulerCommandHandling::BatchOperation,
            Self::AddValidatedTask
            | Self::Pause
            | Self::Resume
            | Self::ApproveHostKey
            | Self::ApplyOptionPatch
            | Self::Remove
            | Self::RemoveStoppedResult => SchedulerCommandHandling::StateMatrix,
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
    OptionPatchPersisted {
        gid: Gid,
        generation: Generation,
        patch_id: OptionPatchId,
    },
    OptionPatchPersistenceFailed {
        gid: Gid,
        generation: Generation,
        patch_id: OptionPatchId,
    },
    OptionPatchApplied {
        gid: Gid,
        generation: Generation,
        patch_id: OptionPatchId,
    },
    OptionPatchApplicationFailed {
        gid: Gid,
        generation: Generation,
        patch_id: OptionPatchId,
        error: PublicError,
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
        challenge: PresentedHostKeyChallenge,
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
        decision: SlowReadmissionDecision,
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
    HostKeyResolutionPersisted {
        gid: Gid,
        generation: Generation,
        resolution_id: HostKeyResolutionId,
    },
    HostKeyResolutionFailed {
        gid: Gid,
        generation: Generation,
        resolution_id: HostKeyResolutionId,
    },
    StoppedResultDeleted {
        gid: Gid,
        generation: Generation,
        deletion_id: StoppedResultDeletionId,
    },
    StoppedResultDeletionFailed {
        gid: Gid,
        generation: Generation,
        deletion_id: StoppedResultDeletionId,
    },
}

/// One asynchronous task event correlated to the immutable in-process task
/// instance that issued the underlying work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskEventEnvelope {
    task_id: TaskId,
    event: TaskEvent,
}

impl TaskEventEnvelope {
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    #[must_use]
    pub const fn event(&self) -> &TaskEvent {
        &self.event
    }

    #[must_use]
    pub fn into_event(self) -> TaskEvent {
        self.event
    }
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
    OptionPatch(OptionPatchId),
    HostKeyResolution(HostKeyResolutionId),
    StoppedResultDeletion(StoppedResultDeletionId),
}

/// Closed asynchronous-event vocabulary, independent of event payloads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TaskEventKind {
    GenerationPersisted,
    OptionPatchPersisted,
    OptionPatchPersistenceFailed,
    OptionPatchApplied,
    OptionPatchApplicationFailed,
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
    HostKeyResolutionPersisted,
    HostKeyResolutionFailed,
    StoppedResultDeleted,
    StoppedResultDeletionFailed,
}

pub const ALL_TASK_EVENT_KINDS: &[TaskEventKind] = &[
    TaskEventKind::GenerationPersisted,
    TaskEventKind::OptionPatchPersisted,
    TaskEventKind::OptionPatchPersistenceFailed,
    TaskEventKind::OptionPatchApplied,
    TaskEventKind::OptionPatchApplicationFailed,
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
    TaskEventKind::HostKeyResolutionPersisted,
    TaskEventKind::HostKeyResolutionFailed,
    TaskEventKind::StoppedResultDeleted,
    TaskEventKind::StoppedResultDeletionFailed,
];

impl TaskEventKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::GenerationPersisted => "generation_persisted",
            Self::OptionPatchPersisted => "option_patch_persisted",
            Self::OptionPatchPersistenceFailed => "option_patch_persistence_failed",
            Self::OptionPatchApplied => "option_patch_applied",
            Self::OptionPatchApplicationFailed => "option_patch_application_failed",
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
            Self::HostKeyResolutionPersisted => "host_key_resolution_persisted",
            Self::HostKeyResolutionFailed => "host_key_resolution_failed",
            Self::StoppedResultDeleted => "stopped_result_deleted",
            Self::StoppedResultDeletionFailed => "stopped_result_deletion_failed",
        }
    }
}

impl TaskEvent {
    /// Binds this event to the immutable task instance that issued its work.
    #[must_use]
    pub fn for_task(self, task_id: TaskId) -> TaskEventEnvelope {
        TaskEventEnvelope {
            task_id,
            event: self,
        }
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        match self {
            Self::GenerationPersisted { gid, .. }
            | Self::OptionPatchPersisted { gid, .. }
            | Self::OptionPatchPersistenceFailed { gid, .. }
            | Self::OptionPatchApplied { gid, .. }
            | Self::OptionPatchApplicationFailed { gid, .. }
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
            | Self::TerminalPersisted { gid, .. }
            | Self::HostKeyResolutionPersisted { gid, .. }
            | Self::HostKeyResolutionFailed { gid, .. }
            | Self::StoppedResultDeleted { gid, .. }
            | Self::StoppedResultDeletionFailed { gid, .. } => *gid,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        match self {
            Self::GenerationPersisted { generation, .. }
            | Self::OptionPatchPersisted { generation, .. }
            | Self::OptionPatchPersistenceFailed { generation, .. }
            | Self::OptionPatchApplied { generation, .. }
            | Self::OptionPatchApplicationFailed { generation, .. }
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
            | Self::TerminalPersisted { generation, .. }
            | Self::HostKeyResolutionPersisted { generation, .. }
            | Self::HostKeyResolutionFailed { generation, .. }
            | Self::StoppedResultDeleted { generation, .. }
            | Self::StoppedResultDeletionFailed { generation, .. } => *generation,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> TaskEventKind {
        match self {
            Self::GenerationPersisted { .. } => TaskEventKind::GenerationPersisted,
            Self::OptionPatchPersisted { .. } => TaskEventKind::OptionPatchPersisted,
            Self::OptionPatchPersistenceFailed { .. } => {
                TaskEventKind::OptionPatchPersistenceFailed
            }
            Self::OptionPatchApplied { .. } => TaskEventKind::OptionPatchApplied,
            Self::OptionPatchApplicationFailed { .. } => {
                TaskEventKind::OptionPatchApplicationFailed
            }
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
            Self::HostKeyResolutionPersisted { .. } => TaskEventKind::HostKeyResolutionPersisted,
            Self::HostKeyResolutionFailed { .. } => TaskEventKind::HostKeyResolutionFailed,
            Self::StoppedResultDeleted { .. } => TaskEventKind::StoppedResultDeleted,
            Self::StoppedResultDeletionFailed { .. } => TaskEventKind::StoppedResultDeletionFailed,
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
            Self::OptionPatchPersisted { patch_id, .. }
            | Self::OptionPatchPersistenceFailed { patch_id, .. }
            | Self::OptionPatchApplied { patch_id, .. }
            | Self::OptionPatchApplicationFailed { patch_id, .. } => {
                Some(TaskEventToken::OptionPatch(*patch_id))
            }
            Self::HostKeyResolutionPersisted { resolution_id, .. }
            | Self::HostKeyResolutionFailed { resolution_id, .. } => {
                Some(TaskEventToken::HostKeyResolution(*resolution_id))
            }
            Self::StoppedResultDeleted { deletion_id, .. } => {
                Some(TaskEventToken::StoppedResultDeletion(*deletion_id))
            }
            Self::StoppedResultDeletionFailed { deletion_id, .. } => {
                Some(TaskEventToken::StoppedResultDeletion(*deletion_id))
            }
            _ => None,
        }
    }
}

/// Closed effect vocabulary, independent of effect payloads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TransitionEffectKind {
    PersistTask,
    PersistQueueTransition,
    StageOptionPatch,
    ApplyOptionPatch,
    PersistGenerationStarted,
    StartAllocation,
    CancelGeneration,
    ReleaseSlot,
    ScheduleRetry,
    CancelRetry,
    ScheduleSlowReadmission,
    CancelSlowReadmission,
    ProbeNoSpace,
    PersistConditions,
    PersistHostKeyChallenge,
    PersistHostKeyPinAndClearChallenge,
    PersistHostKeyChallengeRejected,
    PersistTerminal,
    DeleteStoppedTaskMetadata,
    PublishSnapshot,
}

pub const ALL_TRANSITION_EFFECT_KINDS: &[TransitionEffectKind] = &[
    TransitionEffectKind::PersistTask,
    TransitionEffectKind::PersistQueueTransition,
    TransitionEffectKind::StageOptionPatch,
    TransitionEffectKind::ApplyOptionPatch,
    TransitionEffectKind::PersistGenerationStarted,
    TransitionEffectKind::StartAllocation,
    TransitionEffectKind::CancelGeneration,
    TransitionEffectKind::ReleaseSlot,
    TransitionEffectKind::ScheduleRetry,
    TransitionEffectKind::CancelRetry,
    TransitionEffectKind::ScheduleSlowReadmission,
    TransitionEffectKind::CancelSlowReadmission,
    TransitionEffectKind::ProbeNoSpace,
    TransitionEffectKind::PersistConditions,
    TransitionEffectKind::PersistHostKeyChallenge,
    TransitionEffectKind::PersistHostKeyPinAndClearChallenge,
    TransitionEffectKind::PersistHostKeyChallengeRejected,
    TransitionEffectKind::PersistTerminal,
    TransitionEffectKind::DeleteStoppedTaskMetadata,
    TransitionEffectKind::PublishSnapshot,
];

impl TransitionEffectKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::PersistTask => "persist_task",
            Self::PersistQueueTransition => "persist_queue_transition",
            Self::StageOptionPatch => "stage_option_patch",
            Self::ApplyOptionPatch => "apply_option_patch",
            Self::PersistGenerationStarted => "persist_generation_started",
            Self::StartAllocation => "start_allocation",
            Self::CancelGeneration => "cancel_generation",
            Self::ReleaseSlot => "release_slot",
            Self::ScheduleRetry => "schedule_retry",
            Self::CancelRetry => "cancel_retry",
            Self::ScheduleSlowReadmission => "schedule_slow_readmission",
            Self::CancelSlowReadmission => "cancel_slow_readmission",
            Self::ProbeNoSpace => "probe_no_space",
            Self::PersistConditions => "persist_conditions",
            Self::PersistHostKeyChallenge => "persist_host_key_challenge",
            Self::PersistHostKeyPinAndClearChallenge => "persist_host_key_pin_and_clear_challenge",
            Self::PersistHostKeyChallengeRejected => "persist_host_key_challenge_rejected",
            Self::PersistTerminal => "persist_terminal",
            Self::DeleteStoppedTaskMetadata => "delete_stopped_task_metadata",
            Self::PublishSnapshot => "publish_snapshot",
        }
    }
}

/// Stable scheduler-owned identity of one effect before dispatch assigns its
/// process-local sequence number.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransitionEffectIdentity {
    pub task_id: TaskId,
    pub gid: Gid,
    pub kind: TransitionEffectKind,
    pub generation: Option<Generation>,
    pub token: Option<TaskEventToken>,
}

/// Ordered work handed to persistence, worker, timer, and snapshot adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionEffect {
    PersistTask {
        task_id: TaskId,
        gid: Gid,
        queue: QueueClass,
        position: usize,
        desired_paused: bool,
        slow_demotion_count: u32,
        conditions: TaskConditions,
    },
    PersistQueueTransition {
        task_id: TaskId,
        gid: Gid,
        from: Option<QueueClass>,
        to: Option<QueueClass>,
        desired_paused: bool,
        slow_demotion_count: u32,
        slow_slot: Option<SlowSlotPersistence>,
        orders: Vec<QueueOrder>,
    },
    StageOptionPatch {
        task_id: TaskId,
        gid: Gid,
        patch_id: OptionPatchId,
        satisfies_credentials: Option<CredentialRequirementKey>,
    },
    ApplyOptionPatch {
        task_id: TaskId,
        gid: Gid,
        patch_id: OptionPatchId,
        satisfies_credentials: Option<CredentialRequirementKey>,
    },
    PersistGenerationStarted {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
    },
    StartAllocation {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
    },
    CancelGeneration {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        force: bool,
    },
    ReleaseSlot {
        task_id: TaskId,
        gid: Gid,
        ownership: SlotOwnership,
    },
    ScheduleRetry {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        retry_timer_id: RetryTimerId,
        at: MonotonicInstant,
    },
    CancelRetry {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        retry_timer_id: RetryTimerId,
    },
    ScheduleSlowReadmission {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        readmission_id: SlowReadmissionId,
        at: MonotonicInstant,
    },
    CancelSlowReadmission {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        readmission_id: SlowReadmissionId,
    },
    ProbeNoSpace {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        probe_id: NoSpaceProbeId,
        origin: NoSpaceProbeOrigin,
        at: MonotonicInstant,
    },
    PersistConditions {
        task_id: TaskId,
        gid: Gid,
        conditions: TaskConditions,
    },
    PersistHostKeyChallenge {
        task_id: TaskId,
        gid: Gid,
        challenge: PresentedHostKeyChallenge,
    },
    PersistHostKeyPinAndClearChallenge {
        task_id: TaskId,
        gid: Gid,
        resolution_id: HostKeyResolutionId,
        challenge: HostKeyChallengeId,
        fingerprint_sha256: HostKeyFingerprint,
        presented_public_key: Vec<u8>,
        option_patch: Option<OptionPatchId>,
    },
    PersistHostKeyChallengeRejected {
        task_id: TaskId,
        gid: Gid,
        challenge: HostKeyChallengeId,
    },
    PersistTerminal {
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        status: Aria2Status,
        error: Option<PublicError>,
        from: QueueClass,
        to: QueueClass,
        desired_paused: bool,
        slow_demotion_count: u32,
        slow_slot: Option<SlowSlotPersistence>,
        orders: Vec<QueueOrder>,
    },
    DeleteStoppedTaskMetadata {
        task_id: TaskId,
        gid: Gid,
        deletion_id: StoppedResultDeletionId,
        remaining_order: Vec<Gid>,
    },
    PublishSnapshot {
        task_id: TaskId,
        snapshot: TaskSnapshot,
    },
}

impl TransitionEffect {
    #[must_use]
    pub const fn kind(&self) -> TransitionEffectKind {
        match self {
            Self::PersistTask { .. } => TransitionEffectKind::PersistTask,
            Self::PersistQueueTransition { .. } => TransitionEffectKind::PersistQueueTransition,
            Self::StageOptionPatch { .. } => TransitionEffectKind::StageOptionPatch,
            Self::ApplyOptionPatch { .. } => TransitionEffectKind::ApplyOptionPatch,
            Self::PersistGenerationStarted { .. } => TransitionEffectKind::PersistGenerationStarted,
            Self::StartAllocation { .. } => TransitionEffectKind::StartAllocation,
            Self::CancelGeneration { .. } => TransitionEffectKind::CancelGeneration,
            Self::ReleaseSlot { .. } => TransitionEffectKind::ReleaseSlot,
            Self::ScheduleRetry { .. } => TransitionEffectKind::ScheduleRetry,
            Self::CancelRetry { .. } => TransitionEffectKind::CancelRetry,
            Self::ScheduleSlowReadmission { .. } => TransitionEffectKind::ScheduleSlowReadmission,
            Self::CancelSlowReadmission { .. } => TransitionEffectKind::CancelSlowReadmission,
            Self::ProbeNoSpace { .. } => TransitionEffectKind::ProbeNoSpace,
            Self::PersistConditions { .. } => TransitionEffectKind::PersistConditions,
            Self::PersistHostKeyChallenge { .. } => TransitionEffectKind::PersistHostKeyChallenge,
            Self::PersistHostKeyPinAndClearChallenge { .. } => {
                TransitionEffectKind::PersistHostKeyPinAndClearChallenge
            }
            Self::PersistHostKeyChallengeRejected { .. } => {
                TransitionEffectKind::PersistHostKeyChallengeRejected
            }
            Self::PersistTerminal { .. } => TransitionEffectKind::PersistTerminal,
            Self::DeleteStoppedTaskMetadata { .. } => {
                TransitionEffectKind::DeleteStoppedTaskMetadata
            }
            Self::PublishSnapshot { .. } => TransitionEffectKind::PublishSnapshot,
        }
    }

    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        match self {
            Self::PersistTask { task_id, .. }
            | Self::PersistQueueTransition { task_id, .. }
            | Self::StageOptionPatch { task_id, .. }
            | Self::ApplyOptionPatch { task_id, .. }
            | Self::PersistGenerationStarted { task_id, .. }
            | Self::StartAllocation { task_id, .. }
            | Self::CancelGeneration { task_id, .. }
            | Self::ReleaseSlot { task_id, .. }
            | Self::ScheduleRetry { task_id, .. }
            | Self::CancelRetry { task_id, .. }
            | Self::ScheduleSlowReadmission { task_id, .. }
            | Self::CancelSlowReadmission { task_id, .. }
            | Self::ProbeNoSpace { task_id, .. }
            | Self::PersistConditions { task_id, .. }
            | Self::PersistHostKeyChallenge { task_id, .. }
            | Self::PersistHostKeyPinAndClearChallenge { task_id, .. }
            | Self::PersistHostKeyChallengeRejected { task_id, .. }
            | Self::PersistTerminal { task_id, .. }
            | Self::DeleteStoppedTaskMetadata { task_id, .. }
            | Self::PublishSnapshot { task_id, .. } => *task_id,
        }
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        match self {
            Self::PersistTask { gid, .. }
            | Self::PersistQueueTransition { gid, .. }
            | Self::StageOptionPatch { gid, .. }
            | Self::ApplyOptionPatch { gid, .. }
            | Self::PersistGenerationStarted { gid, .. }
            | Self::StartAllocation { gid, .. }
            | Self::CancelGeneration { gid, .. }
            | Self::ReleaseSlot { gid, .. }
            | Self::ScheduleRetry { gid, .. }
            | Self::CancelRetry { gid, .. }
            | Self::ScheduleSlowReadmission { gid, .. }
            | Self::CancelSlowReadmission { gid, .. }
            | Self::ProbeNoSpace { gid, .. }
            | Self::PersistConditions { gid, .. }
            | Self::PersistHostKeyChallenge { gid, .. }
            | Self::PersistHostKeyPinAndClearChallenge { gid, .. }
            | Self::PersistHostKeyChallengeRejected { gid, .. }
            | Self::PersistTerminal { gid, .. }
            | Self::DeleteStoppedTaskMetadata { gid, .. } => *gid,
            Self::PublishSnapshot { snapshot, .. } => snapshot.gid,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> Option<Generation> {
        match self {
            Self::PersistGenerationStarted { generation, .. }
            | Self::StartAllocation { generation, .. }
            | Self::CancelGeneration { generation, .. }
            | Self::ScheduleRetry { generation, .. }
            | Self::CancelRetry { generation, .. }
            | Self::ScheduleSlowReadmission { generation, .. }
            | Self::CancelSlowReadmission { generation, .. }
            | Self::ProbeNoSpace { generation, .. }
            | Self::PersistTerminal { generation, .. } => Some(*generation),
            Self::PublishSnapshot { snapshot, .. } => Some(snapshot.generation),
            Self::PersistTask { .. }
            | Self::PersistQueueTransition { .. }
            | Self::StageOptionPatch { .. }
            | Self::ApplyOptionPatch { .. }
            | Self::ReleaseSlot { .. }
            | Self::PersistConditions { .. }
            | Self::PersistHostKeyChallenge { .. }
            | Self::PersistHostKeyPinAndClearChallenge { .. }
            | Self::PersistHostKeyChallengeRejected { .. }
            | Self::DeleteStoppedTaskMetadata { .. } => None,
        }
    }

    #[must_use]
    pub const fn token(&self) -> Option<TaskEventToken> {
        match self {
            Self::StageOptionPatch { patch_id, .. } | Self::ApplyOptionPatch { patch_id, .. } => {
                Some(TaskEventToken::OptionPatch(*patch_id))
            }
            Self::ScheduleRetry { retry_timer_id, .. }
            | Self::CancelRetry { retry_timer_id, .. } => {
                Some(TaskEventToken::RetryTimer(*retry_timer_id))
            }
            Self::ScheduleSlowReadmission { readmission_id, .. }
            | Self::CancelSlowReadmission { readmission_id, .. } => {
                Some(TaskEventToken::SlowReadmission(*readmission_id))
            }
            Self::ProbeNoSpace { probe_id, .. } => Some(TaskEventToken::NoSpaceProbe(*probe_id)),
            Self::PersistHostKeyPinAndClearChallenge { resolution_id, .. } => {
                Some(TaskEventToken::HostKeyResolution(*resolution_id))
            }
            Self::DeleteStoppedTaskMetadata { deletion_id, .. } => {
                Some(TaskEventToken::StoppedResultDeletion(*deletion_id))
            }
            Self::PersistTask { .. }
            | Self::PersistQueueTransition { .. }
            | Self::PersistGenerationStarted { .. }
            | Self::StartAllocation { .. }
            | Self::CancelGeneration { .. }
            | Self::ReleaseSlot { .. }
            | Self::PersistConditions { .. }
            | Self::PersistHostKeyChallenge { .. }
            | Self::PersistHostKeyChallengeRejected { .. }
            | Self::PersistTerminal { .. }
            | Self::PublishSnapshot { .. } => None,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> TransitionEffectIdentity {
        TransitionEffectIdentity {
            task_id: self.task_id(),
            gid: self.gid(),
            kind: self.kind(),
            generation: self.generation(),
            token: self.token(),
        }
    }
}

/// Result of one accepted command or event.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchedulerOutcome {
    pub transition: Option<StateTransition>,
    pub deletion: Option<TaskDeletion>,
    pub effects: Vec<TransitionEffect>,
}

impl SchedulerOutcome {
    /// Constructs an outcome only when its ordered effect list fits the hard
    /// per-operation scheduler bound.
    pub fn checked(
        transition: Option<StateTransition>,
        deletion: Option<TaskDeletion>,
        effects: Vec<TransitionEffect>,
    ) -> Result<Self, SchedulerError> {
        if effects.len() > MAX_SCHEDULER_EFFECTS {
            return Err(SchedulerError::EffectLimitExceeded);
        }
        Ok(Self {
            transition,
            deletion,
            effects,
        })
    }

    #[must_use]
    pub const fn is_noop(&self) -> bool {
        self.transition.is_none() && self.deletion.is_none() && self.effects.is_empty()
    }
}

/// Deterministic scheduler failures. Rejected work has no side effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerError {
    TaskLimitReached,
    GidCollision,
    TaskIdCollision,
    OptionPatchIdCollision,
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
    StaleCredentialRequirement,
    InvalidTaskConditions,
    InvalidSlowReadmissionDecision,
    InvalidTerminalAcknowledgement,
    ShutdownBatchRequired,
    EffectLimitExceeded,
    InternalInvariant,
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskLimitReached => formatter.write_str("scheduler task limit reached"),
            Self::GidCollision => formatter.write_str("GID already exists"),
            Self::TaskIdCollision => formatter.write_str("task id already exists"),
            Self::OptionPatchIdCollision => {
                formatter.write_str("option patch id was already used by this task")
            }
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
            Self::StaleCredentialRequirement => {
                formatter.write_str("credential requirement is stale or mismatched")
            }
            Self::InvalidTaskConditions => {
                formatter.write_str("task conditions exceed scheduler persistence bounds")
            }
            Self::InvalidSlowReadmissionDecision => {
                formatter.write_str("slow readmission decision is not persistence-safe")
            }
            Self::InvalidTerminalAcknowledgement => {
                formatter.write_str("terminal persistence acknowledgement does not match")
            }
            Self::ShutdownBatchRequired => {
                formatter.write_str("orderly shutdown requires the bounded batch shutdown executor")
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
        ALL_TASK_EVENT_KINDS, ALL_TRANSITION_EFFECT_KINDS, DrainTarget, NoSpaceProbeOrigin,
        QueueClass, QueueOrder, SchedulerCommandHandling, SchedulerCommandKind, SchedulerConfig,
        SchedulerConfigError, SlotOwnership, TaskEvent, TaskEventKind, TaskEventToken,
        TransitionEffect, TransitionEffectKind,
    };
    use crate::{
        Aria2Status, Generation, Gid, NoSpaceProbeId, RetryTimerId, SlowReadmissionId, TaskId,
    };
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
        let effects: BTreeSet<_> = ALL_TRANSITION_EFFECT_KINDS
            .iter()
            .map(|kind| kind.code())
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
        assert_eq!(effects.len(), ALL_TRANSITION_EFFECT_KINDS.len());
        assert_eq!(ALL_SCHEDULER_COMMAND_KINDS.len(), 9);
        assert_eq!(ALL_TASK_EVENT_KINDS.len(), 30);
        assert_eq!(ALL_NO_SPACE_PROBE_ORIGINS.len(), 2);
        assert_eq!(ALL_SCHEDULER_COMMAND_HANDLINGS.len(), 3);
        assert_eq!(ALL_DRAIN_TARGETS.len(), 7);
        assert_eq!(ALL_TRANSITION_EFFECT_KINDS.len(), 20);
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
            task_id: TaskId::new(1).expect("task id"),
            gid,
            generation,
            status: Aria2Status::Complete,
            error: None,
            from: QueueClass::Active,
            to: QueueClass::Stopped,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            orders: vec![QueueOrder {
                class: QueueClass::Stopped,
                order: vec![gid],
            }],
        };
        assert!(matches!(
            effect,
            TransitionEffect::PersistTerminal {
                task_id: _,
                gid: actual_gid,
                generation: actual_generation,
                status: Aria2Status::Complete,
                error: None,
                from: QueueClass::Active,
                to: QueueClass::Stopped,
                desired_paused: false,
                ..
            } if actual_gid == gid && actual_generation == generation
        ));
        assert_eq!(effect.kind(), TransitionEffectKind::PersistTerminal);
        assert_eq!(effect.task_id(), TaskId::new(1).expect("task id"));
        assert_eq!(effect.gid(), gid);
        assert_eq!(effect.generation(), Some(generation));
        assert_eq!(effect.token(), None);
        assert_eq!(
            effect.identity().kind,
            TransitionEffectKind::PersistTerminal
        );
    }
}
