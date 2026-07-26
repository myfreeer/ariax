#![forbid(unsafe_code)]

//! Core identifiers and contracts shared by ariax components.

mod error;
mod ids;
mod snapshot;
mod state;

pub use error::{
    ALL_ERROR_KINDS, ALL_OPTION_PATCH_REJECT_REASONS, ErrorKind, OptionPatchRejectReason,
    PublicError, RetryClass,
};
pub use ids::{
    BufferId, FileId, Generation, Gid, GidLookupError, GidPrefix, HostKeyChallengeId,
    HostKeyFingerprint, LeaseId, OptionPatchId, OverlapGroupId, ParseGidError, ParseGidPrefixError,
    PieceId, TaskId, TransferAttemptId, UriId, resolve_gid_prefix,
};
pub use snapshot::{HostKeyChallenge, TaskSnapshot};
pub use state::{
    ALL_ARIA2_STATUSES, ALL_TASK_STATES, Aria2Status, CredentialKind, CredentialRequirement,
    MonotonicInstant, NoSpaceCondition, PlannedSpanState, TaskConditions, TaskConditionsSnapshot,
    TaskState, WireProjection, WireProjectionError,
};

/// The engine and command-line product name.
pub const ENGINE_NAME: &str = "ariax";
