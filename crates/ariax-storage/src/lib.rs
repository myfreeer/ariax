#![forbid(unsafe_code)]

//! Portable storage contracts shared by safe-open and disk backends.

mod journal;
mod metalink_expansion;
pub use metalink_expansion::{METALINK_EXPANSION_OPTION, MetalinkExpansion, MetalinkParent};
mod journal_appender;
mod journal_payload;
mod journal_state;
mod journal_tags;
mod layout;
mod native_capability;
mod native_identity;
mod path;
mod root_binding;
mod session_export;
mod session_owner;
mod session_store;
mod verification;

pub use verification::{
    MAX_VERIFICATION_CHUNKS, MAX_VERIFICATION_MANIFEST_BYTES, ProtocolValidator,
    VERIFICATION_MANIFEST_DOMAIN, VERIFICATION_MANIFEST_PART_BYTES, VerificationManifest,
    VerificationManifestError,
};

pub use session_export::{
    SESSION_EXPORT_MAX_BYTES, SessionExportDestination, read_session_document,
};

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
    JournalTailMismatch, PreparedJournalSet, journal_segment_file_name, journal_segment_path,
};
pub use journal_payload::{
    ALL_JOURNAL_DIGEST_ALGORITHMS, ALL_PAYLOAD_CODEC_ERROR_CLASSES, AppendPayloadError,
    CheckpointId, DurableEvidenceRun, HTTP_RANGE_IDENTITY_HASH_DOMAIN,
    HTTP_STRONG_VALIDATOR_HASH_DOMAIN, HostKeyDecision, JournalDigest, JournalDigestAlgorithm,
    JournalFileLayoutEntry, JournalHash, JournalHostKeyState, JournalPayload, JournalRelativePath,
    MAX_DIGEST_ALGORITHM_BYTES, MAX_DIGEST_VALUE_BYTES, MAX_HTTP_STRONG_ETAG_BYTES,
    MAX_OPTION_KEY_BYTES, MAX_OPTION_MAP_BYTES, MAX_OPTION_MAP_ENTRIES, MAX_OPTION_VALUE_BYTES,
    MAX_PIECE_STATE_BITMAP_BYTES, MAX_PIECE_STATE_COVERED_PIECES, OPTIONS_SNAPSHOT_HASH_DOMAIN,
    PAYLOAD_CODEC_RECORD_TYPES, PayloadCodecError, PersistedId, PersistedSpan, SanitizedOptionMap,
    calculate_http_range_identity_fingerprint, calculate_http_strong_validator_fingerprint,
};
pub use journal_state::{
    ALL_JOURNAL_STATE_ERROR_CODES, CHECKPOINT_STATE_HASH_DOMAIN, CONTRIBUTORS_HASH_DOMAIN,
    DurablePieceOrigin, JournalContributor, JournalStateError, JournalStateLimits,
    JournalStateReplay, JournalStateResource, JournalStateStop, PersistedOptionPolicy,
    REBIND_VALIDATOR_SET_HASH_DOMAIN, RecoveredCheckpoint, RecoveredCleanShutdown,
    RecoveredDurablePiece, RecoveredFinalization, RecoveredHttpRangeIdentity,
    RecoveredHttpStrongValidator, RecoveredJournalState, RecoveredLayout, RecoveredOptionSnapshot,
    RecoveredRetryState, RecoveredTerminal, VALIDATOR_SET_HASH_DOMAIN,
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
pub use native_capability::{
    JournalDirectoryCapability, MAX_NATIVE_ALLOWED_ROOTS, NativeCapabilityError, NativeObjectKind,
    RootDirectoryCapability, RootFileCapability, platform_path_to_current,
};
pub use native_identity::{
    ALL_NATIVE_IDENTITY_ERROR_CODES, NATIVE_IDENTITY_UNIX_BYTES, NATIVE_IDENTITY_VERSION,
    NATIVE_IDENTITY_WINDOWS_BYTES, NativeIdentityError, NativeIdentityV1,
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
pub use session_owner::{
    ALL_SESSION_OWNER_ERROR_CODES, ALL_SESSION_PERSISTENCE_ERROR_CODES,
    SESSION_OWNER_DEFAULT_CAPACITY, SESSION_OWNER_DEFAULT_SHUTDOWN_TIMEOUT,
    SESSION_OWNER_DEFAULT_STARTUP_TIMEOUT, SESSION_OWNER_MAX_CAPACITY, SESSION_OWNER_MAX_WAIT,
    SessionCommand, SessionCommandResult, SessionCompletion, SessionHandle, SessionOwner,
    SessionOwnerConfig, SessionOwnerError, SessionOwnerShutdown, SessionOwnerWait,
    SessionPersistenceError, SessionStartupSnapshot, SessionSubmitError,
};
pub use session_store::{
    ALL_SESSION_IO_OPERATIONS, ALL_SESSION_SQLITE_LIMITS, ALL_SESSION_STORE_ERROR_CODES,
    JournalInstallIntent, JournalInstallPhase, JournalInstallToken, SESSION_BUNDLED_SQLITE_FLAGS,
    SESSION_BUSY_TIMEOUT_MS, SESSION_DEFAULT_CACHE_KIB, SESSION_HOST_KEY_PIN_OPTION,
    SESSION_IMPORT_MAX_BYTES, SESSION_INSTALL_READ_BUDGET_BYTES, SESSION_MAX_ALGORITHM_BYTES,
    SESSION_MAX_BT_RESUME_BYTES, SESSION_MAX_CACHE_KIB, SESSION_MAX_HOST_KEY_BYTES,
    SESSION_MAX_IMPORT_TASKS, SESSION_MAX_OPTIONS_PER_TASK, SESSION_MAX_SAFE_MESSAGE_BYTES,
    SESSION_MAX_SAFE_URI_BYTES, SESSION_MAX_SOURCES_PER_TASK, SESSION_MAX_TASKS,
    SESSION_MIN_CACHE_KIB, SESSION_MMAP_SIZE_BYTES, SESSION_OWNER_LOCK_SUFFIX,
    SESSION_PAGE_SIZE_BYTES, SESSION_RUSQLITE_FEATURES, SESSION_RUSQLITE_VERSION,
    SESSION_SCHEMA_OBJECTS, SESSION_SCHEMA_VERSION, SESSION_SOURCE_READ_BUDGET_BYTES,
    SESSION_TASK_READ_BUDGET_BYTES, SESSION_WAL_AUTO_CHECKPOINT_PAGES, SessionBtBinding,
    SessionBtCheckpoint, SessionBtFile, SessionBtResumeRecord, SessionBtTaskRecord,
    SessionCacheReconciliation, SessionHostKeyChallengeRecord, SessionHostKeyResolution, SessionId,
    SessionIoOperation, SessionJournalCache, SessionJournalMode, SessionNoSpaceCondition,
    SessionQueueOrder, SessionQueueState, SessionQueueTransition, SessionRecord,
    SessionSchemaObject, SessionSchemaObjectKind, SessionSlowRetryDecision, SessionSlowSlotState,
    SessionSqliteLimit, SessionStoppedResultRecord, SessionStore, SessionStoreConfig,
    SessionStoreError, SessionStoreSettings, SessionTaskMetadata, SessionTaskRecord,
    SessionTaskSourceRecord, SessionTaskSourceSet, SessionTerminalStatus,
    session_host_key_pin_value, uri_is_safe_to_persist,
};
