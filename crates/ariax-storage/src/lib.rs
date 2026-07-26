#![forbid(unsafe_code)]

//! Portable storage contracts shared by safe-open and disk backends.

mod journal;
mod journal_appender;
mod journal_payload;
mod journal_state;
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
pub use journal_appender::{
    ALL_JOURNAL_APPENDER_ERROR_CODES, ALL_JOURNAL_APPENDER_FAULTS, ALL_JOURNAL_IO_OPERATIONS,
    ALL_JOURNAL_TAIL_MISMATCHES, Appended, ControlJournalAppender, Flushed,
    JOURNAL_SEGMENT_FILE_PREFIX, JOURNAL_SEGMENT_FILE_SUFFIX, JOURNAL_TEMP_FILE_SUFFIX,
    JournalAppenderError, JournalAppenderFault, JournalIoOperation, JournalRotation,
    JournalTailMismatch, journal_segment_file_name, journal_segment_path,
};
pub use journal_payload::{
    ALL_JOURNAL_DIGEST_ALGORITHMS, ALL_PAYLOAD_CODEC_ERROR_CLASSES, AppendPayloadError,
    CheckpointId, DurableEvidenceRun, JournalDigest, JournalDigestAlgorithm,
    JournalFileLayoutEntry, JournalHash, JournalPayload, JournalRelativePath,
    MAX_DIGEST_ALGORITHM_BYTES, MAX_DIGEST_VALUE_BYTES, MAX_OPTION_KEY_BYTES, MAX_OPTION_MAP_BYTES,
    MAX_OPTION_MAP_ENTRIES, MAX_OPTION_VALUE_BYTES, MAX_PIECE_STATE_BITMAP_BYTES,
    MAX_PIECE_STATE_COVERED_PIECES, OPTIONS_SNAPSHOT_HASH_DOMAIN, PAYLOAD_CODEC_RECORD_TYPES,
    PayloadCodecError, PersistedId, PersistedSpan, SanitizedOptionMap,
};
pub use journal_state::{
    ALL_JOURNAL_STATE_ERROR_CODES, CHECKPOINT_STATE_HASH_DOMAIN, CONTRIBUTORS_HASH_DOMAIN,
    DurablePieceOrigin, JournalContributor, JournalStateError, JournalStateLimits,
    JournalStateReplay, JournalStateResource, JournalStateStop, PersistedOptionPolicy,
    REBIND_VALIDATOR_SET_HASH_DOMAIN, RecoveredCheckpoint, RecoveredCleanShutdown,
    RecoveredDurablePiece, RecoveredFinalization, RecoveredJournalState, RecoveredLayout,
    RecoveredOptionSnapshot, RecoveredRetryState, RecoveredTerminal, VALIDATOR_SET_HASH_DOMAIN,
    calculate_checkpoint_state_hash, calculate_contributors_hash,
    calculate_rebind_validator_set_fingerprint, calculate_validator_set_fingerprint,
    recover_journal_state,
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
