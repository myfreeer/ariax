#![forbid(unsafe_code)]

//! Core identifiers and contracts shared by ariax components.

mod command;
mod error;
mod ids;
mod snapshot;
mod state;
mod transition;

pub use command::{
    ALL_DRAIN_TARGETS, ALL_NO_SPACE_PROBE_ORIGINS, ALL_QUEUE_CLASSES,
    ALL_SCHEDULER_COMMAND_HANDLINGS, ALL_SCHEDULER_COMMAND_KINDS, ALL_SLOT_OWNERSHIP,
    ALL_TASK_EVENT_KINDS, DrainTarget, MAX_SCHEDULER_EFFECTS, NoSpaceProbeOrigin, PendingBarrier,
    QueueClass, SchedulerCommand, SchedulerCommandHandling, SchedulerCommandKind, SchedulerConfig,
    SchedulerConfigError, SchedulerError, SchedulerOutcome, SlotOwnership, TaskEvent,
    TaskEventKind, TaskEventToken, TransitionEffect,
};

pub use error::{
    ALL_ERROR_KINDS, ALL_OPTION_PATCH_REJECT_REASONS, ErrorKind, OptionPatchRejectReason,
    PublicError, RetryClass,
};
pub use ids::{
    BufferId, FileId, Generation, Gid, GidLookupError, GidPrefix, HostKeyChallengeId,
    HostKeyFingerprint, LeaseId, NoSpaceProbeId, OptionPatchId, OverlapGroupId, ParseGidError,
    ParseGidPrefixError, PieceId, RetryTimerId, SlowReadmissionId, TaskId, TransferAttemptId,
    UriId, resolve_gid_prefix,
};
pub use snapshot::{HostKeyChallenge, TaskSnapshot};
pub use state::{
    ALL_ARIA2_STATUSES, ALL_TASK_STATES, Aria2Status, CredentialKind, CredentialRequirement,
    MonotonicInstant, NoSpaceCondition, PlannedSpanState, TaskConditions, TaskConditionsSnapshot,
    TaskState, WireProjection, WireProjectionError,
};
pub use transition::{
    ALL_EVENT_DISPOSITIONS, ALL_SCHEDULER_ACTIONS, ALL_STATE_REASONS,
    ALL_TRANSITION_CONTRACT_KINDS, ALL_TRANSITION_REJECTIONS, EventDisposition, SchedulerAction,
    SchedulerActionSource, SchedulerActionSourceError, StateReason, StateTransition,
    TransitionContract, TransitionContractKind, TransitionRejection, transition_contract,
};

/// The engine and command-line product name.
pub const ENGINE_NAME: &str = "ariax";
