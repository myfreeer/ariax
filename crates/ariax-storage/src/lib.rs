#![forbid(unsafe_code)]

//! Portable storage contracts shared by safe-open and disk backends.

mod journal;
mod journal_tags;
mod layout;
mod path;
mod root_binding;

pub use journal::{
    ALL_HEADER_DECODE_ERRORS, ALL_RECORD_STOP_REASONS, ALL_RECORD_TYPES, ALL_REPLAY_RESOURCES,
    COMMIT_MAGIC, EncodedSegment, HEADER_MAGIC, HeaderDecodeError, JOURNAL_ENDIANNESS_ASSERTION,
    JOURNAL_FORMAT_VERSION, JournalEncodeError, JournalId, JournalRecord, JournalReplay,
    MAX_RECORD_PAYLOAD, RECORD_MAGIC, RECORD_OVERHEAD, RECORD_PREFIX_LEN, RecordStopReason,
    RecordType, ReplayLimits, ReplayResource, ReplayStop, SEGMENT_HASH_DOMAIN, SEGMENT_HEADER_LEN,
    SegmentEncoder, SegmentHash, SegmentHeader, replay_ordered_segments,
};
pub use journal_tags::{
    ALL_DATA_BARRIER_KINDS, ALL_DURABILITY_MODES, ALL_GENERATION_START_REASONS,
    ALL_LEASE_ABORT_REASONS, ALL_OPTIONS_SNAPSHOT_SCOPES, ALL_RETRY_REASONS, ALL_RETRY_SCOPES,
    ALL_TASK_PAUSE_REASONS, ALL_TASK_REMOVE_REASONS, DataBarrierKind, DurabilityMode,
    GenerationStartReason, LeaseAbortReason, OptionsSnapshotScope, RetryReason, RetryScope,
    TaskPauseReason, TaskRemoveReason, UnknownJournalTag,
};
pub use layout::{
    ALL_LAYOUT_ERRORS, ALL_MAP_SPAN_ERRORS, FileEntry, FileLayout, FileSpan, GlobalOffsetMapper,
    GlobalSpan, LAYOUT_HASH_DOMAIN, LayoutError, LayoutHash, MAX_LAYOUT_BYTES, MAX_LAYOUT_ENTRIES,
    MapSpanError,
};
pub use path::{
    ALL_PATH_VALIDATION_ERRORS, MAX_SAFE_RELATIVE_BYTES, PathPlatform, PathValidationError,
    SafePathBuilder, SafeRelativePath,
};
pub use root_binding::{
    ALL_ROOT_BINDING_ERROR_CLASSES, FileIdentity, MAX_IDENTITY_BYTES, MAX_PLATFORM_PATH_BYTES,
    PlatformPath, ROOT_BINDING_HASH_DOMAIN, RootBinding, RootBindingError, RootBindingHash,
    RootIdentity,
};
