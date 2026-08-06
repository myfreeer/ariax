#![forbid(unsafe_code)]

//! Core identifiers and contracts shared by ariax components.

mod command;
mod error;
mod ids;
mod persisted_delay;
mod scheduler;
mod snapshot;
mod state;
mod transition;

pub use command::{
    ALL_DRAIN_TARGETS, ALL_NO_SPACE_PROBE_ORIGINS, ALL_QUEUE_CLASSES,
    ALL_SCHEDULER_COMMAND_HANDLINGS, ALL_SCHEDULER_COMMAND_KINDS, ALL_SLOT_OWNERSHIP,
    ALL_TASK_EVENT_KINDS, ALL_TRANSITION_EFFECT_KINDS, DrainTarget, MAX_PERSISTED_MILLISECONDS,
    MAX_SCHEDULER_EFFECTS, MAX_SCHEDULER_TASKS, NoSpaceProbeOrigin, PendingBarrier, QueueClass,
    QueueOrder, SchedulerCommand, SchedulerCommandHandling, SchedulerCommandKind, SchedulerConfig,
    SchedulerConfigError, SchedulerError, SchedulerOutcome, SlotOwnership, SlowReadmissionDecision,
    SlowSlotPersistence, TaskEvent, TaskEventEnvelope, TaskEventKind, TaskEventToken,
    TransitionEffect, TransitionEffectIdentity, TransitionEffectKind, ValidatedOptionPatchKind,
};

pub use error::{
    ALL_ERROR_KINDS, ALL_OPTION_PATCH_REJECT_REASONS, ErrorKind, MAX_PUBLIC_ERROR_MESSAGE_BYTES,
    OptionPatchRejectReason, PublicError, RetryClass,
};
pub use ids::{
    BufferId, FileId, Generation, Gid, GidLookupError, GidPrefix, HostKeyChallengeId,
    HostKeyFingerprint, HostKeyResolutionId, LeaseId, NoSpaceProbeId, OptionPatchId,
    OverlapGroupId, ParseGidError, ParseGidPrefixError, PieceId, RetryTimerId, SlowReadmissionId,
    StoppedResultDeletionId, TaskId, TransferAttemptId, UriId, resolve_gid_prefix,
};
pub use persisted_delay::{
    PersistedDelayDecision, PersistedDelayError, RecoveredDelayDecision, RecoveredWallClock,
};
pub use scheduler::{
    PendingOptionPatchMode, PendingUserControl, RecoveredSchedulerTask, RequestScheduler,
    SchedulerRestoreBatch, SchedulerRestoreError, SchedulerRestorePlan, SchedulerTaskView,
};
pub use snapshot::{
    HostKeyChallenge, MAX_HOST_KEY_ALGORITHM_BYTES, MAX_HOST_KEY_CANONICAL_HOST_BYTES,
    MAX_PRESENTED_HOST_KEY_BYTES, PresentedHostKeyChallenge, PresentedHostKeyChallengeError,
    TaskSnapshot,
};
pub use state::{
    ALL_ARIA2_STATUSES, ALL_TASK_STATES, Aria2Status, CredentialKind, CredentialRequirement,
    CredentialRequirementKey, MAX_CONDITION_DESCRIPTION_BYTES, MAX_REDACTED_PATH_BYTES,
    MonotonicInstant, NoSpaceCondition, PlannedSpanState, TaskConditions, TaskConditionsError,
    TaskConditionsSnapshot, TaskState, WireProjection, WireProjectionError,
};
pub use transition::{
    ALL_EVENT_DISPOSITIONS, ALL_SCHEDULER_ACTIONS, ALL_STATE_REASONS,
    ALL_TRANSITION_CONTRACT_KINDS, ALL_TRANSITION_REJECTIONS, EventDisposition, SchedulerAction,
    SchedulerActionSource, SchedulerActionSourceError, StateReason, StateTransition, TaskDeletion,
    TransitionContract, TransitionContractKind, TransitionRejection, transition_contract,
};

/// The engine and command-line product name.
pub const ENGINE_NAME: &str = "ariax";
