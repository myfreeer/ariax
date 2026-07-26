use crate::{
    CheckpointId, JournalHash, JournalId, MAX_PLATFORM_PATH_BYTES, OptionsSnapshotScope,
    PathPlatform, PersistedOptionPolicy, PlatformPath, SanitizedOptionMap,
};
use ariax_core::Gid;
use rusqlite::limits::Limit;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

pub const SESSION_SCHEMA_VERSION: u32 = 1;
pub const SESSION_RUSQLITE_VERSION: &str = "0.40.1";
pub const SESSION_RUSQLITE_FEATURES: [&str; 4] = ["bundled", "backup", "cache", "limits"];
pub const SESSION_BUNDLED_SQLITE_FLAGS: &str = "-DSQLITE_MAX_LIKE_PATTERN_LENGTH=65536";
pub const SESSION_PAGE_SIZE_BYTES: i64 = 4096;
pub const SESSION_BUSY_TIMEOUT_MS: u64 = 5000;
pub const SESSION_WAL_AUTO_CHECKPOINT_PAGES: i64 = 1000;
pub const SESSION_MMAP_SIZE_BYTES: i64 = 0;
pub const SESSION_DEFAULT_CACHE_KIB: u32 = 8192;
pub const SESSION_MIN_CACHE_KIB: u32 = 1024;
pub const SESSION_MAX_CACHE_KIB: u32 = 256 * 1024;
pub const SESSION_MAX_BT_RESUME_BYTES: usize = 64 * 1024 * 1024;
pub const SESSION_MAX_SAFE_MESSAGE_BYTES: usize = 4096;
pub const SESSION_MAX_SAFE_URI_BYTES: usize = 64 * 1024;
pub const SESSION_MAX_HOST_KEY_BYTES: usize = 1024 * 1024;
pub const SESSION_MAX_ALGORITHM_BYTES: usize = 128;

const PLATFORM_PATH_ENCODING_OVERHEAD: usize = 5;
const MAX_ENCODED_PLATFORM_PATH_BYTES: usize =
    MAX_PLATFORM_PATH_BYTES + PLATFORM_PATH_ENCODING_OVERHEAD;

const SESSION_TABLE_SQL: &str = r#"CREATE TABLE session (
    session_id BLOB PRIMARY KEY NOT NULL CHECK(typeof(session_id) = 'blob' AND length(session_id) = 16),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms),
    clean_shutdown INTEGER NOT NULL CHECK(clean_shutdown IN (0, 1))
) STRICT"#;

const TASK_TABLE_SQL: &str = r#"CREATE TABLE task (
    gid TEXT PRIMARY KEY NOT NULL CHECK(length(gid) = 16 AND gid NOT GLOB '*[^0-9a-f]*'),
    session_id BLOB NOT NULL CHECK(typeof(session_id) = 'blob' AND length(session_id) = 16),
    queue_state INTEGER NOT NULL CHECK(queue_state IN (1, 2, 3, 4)),
    queue_position INTEGER NOT NULL CHECK(queue_position >= 0),
    desired_paused INTEGER NOT NULL CHECK(desired_paused IN (0, 1)),
    primary_journal_id BLOB NOT NULL CHECK(typeof(primary_journal_id) = 'blob' AND length(primary_journal_id) = 16),
    primary_journal_path BLOB NOT NULL CHECK(typeof(primary_journal_path) = 'blob' AND length(primary_journal_path) BETWEEN 6 AND 65541),
    replica_journal_path BLOB CHECK(replica_journal_path IS NULL OR (typeof(replica_journal_path) = 'blob' AND length(replica_journal_path) BETWEEN 6 AND 65541)),
    replica_sequence BLOB CHECK(replica_sequence IS NULL OR (typeof(replica_sequence) = 'blob' AND length(replica_sequence) = 8)),
    root_display BLOB NOT NULL CHECK(typeof(root_display) = 'blob' AND length(root_display) BETWEEN 6 AND 65541),
    cached_layout_hash BLOB CHECK(cached_layout_hash IS NULL OR (typeof(cached_layout_hash) = 'blob' AND length(cached_layout_hash) = 32)),
    cached_root_binding_hash BLOB CHECK(cached_root_binding_hash IS NULL OR (typeof(cached_root_binding_hash) = 'blob' AND length(cached_root_binding_hash) = 32)),
    cached_snapshot_hash BLOB NOT NULL CHECK(typeof(cached_snapshot_hash) = 'blob' AND length(cached_snapshot_hash) = 32),
    no_space_target BLOB CHECK(no_space_target IS NULL OR (typeof(no_space_target) = 'blob' AND length(no_space_target) BETWEEN 6 AND 65541)),
    no_space_scheduled_at_ms INTEGER CHECK(no_space_scheduled_at_ms IS NULL OR no_space_scheduled_at_ms >= 0),
    no_space_delay_ms BLOB CHECK(no_space_delay_ms IS NULL OR (typeof(no_space_delay_ms) = 'blob' AND length(no_space_delay_ms) = 8)),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms),
    CHECK((replica_journal_path IS NULL) = (replica_sequence IS NULL)),
    CHECK((no_space_target IS NULL) = (no_space_scheduled_at_ms IS NULL) AND (no_space_target IS NULL) = (no_space_delay_ms IS NULL)),
    FOREIGN KEY(session_id) REFERENCES session(session_id) ON UPDATE RESTRICT ON DELETE CASCADE
) STRICT"#;

const TASK_OPTION_TABLE_SQL: &str = r#"CREATE TABLE task_option (
    gid TEXT NOT NULL,
    scope INTEGER NOT NULL CHECK(scope IN (1, 2)),
    key TEXT NOT NULL CHECK(length(CAST(key AS BLOB)) BETWEEN 1 AND 256),
    canonical_value BLOB NOT NULL CHECK(typeof(canonical_value) = 'blob' AND length(canonical_value) <= 65536),
    PRIMARY KEY(gid, scope, key),
    FOREIGN KEY(gid) REFERENCES task(gid) ON UPDATE CASCADE ON DELETE CASCADE
) STRICT"#;

const TASK_SOURCE_TABLE_SQL: &str = r#"CREATE TABLE task_source (
    gid TEXT NOT NULL,
    uri_id INTEGER NOT NULL CHECK(uri_id BETWEEN 0 AND 4294967295),
    persistence_safe_uri TEXT CHECK(persistence_safe_uri IS NULL OR length(CAST(persistence_safe_uri AS BLOB)) <= 65536),
    redacted_fingerprint BLOB NOT NULL CHECK(typeof(redacted_fingerprint) = 'blob' AND length(redacted_fingerprint) = 32),
    needs_credentials INTEGER NOT NULL CHECK(needs_credentials IN (0, 1)),
    priority INTEGER NOT NULL,
    PRIMARY KEY(gid, uri_id),
    FOREIGN KEY(gid) REFERENCES task(gid) ON UPDATE CASCADE ON DELETE CASCADE
) STRICT"#;

const HOST_KEY_CHALLENGE_TABLE_SQL: &str = r#"CREATE TABLE host_key_challenge (
    gid TEXT PRIMARY KEY NOT NULL,
    challenge_id BLOB NOT NULL CHECK(typeof(challenge_id) = 'blob' AND length(challenge_id) = 16),
    canonical_host TEXT NOT NULL CHECK(length(CAST(canonical_host AS BLOB)) BETWEEN 1 AND 253),
    port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
    algorithm TEXT NOT NULL CHECK(length(CAST(algorithm AS BLOB)) BETWEEN 1 AND 128),
    presented_public_key BLOB NOT NULL CHECK(typeof(presented_public_key) = 'blob' AND length(presented_public_key) BETWEEN 1 AND 1048576),
    fingerprint_sha256 BLOB NOT NULL CHECK(typeof(fingerprint_sha256) = 'blob' AND length(fingerprint_sha256) = 32),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    FOREIGN KEY(gid) REFERENCES task(gid) ON UPDATE CASCADE ON DELETE CASCADE
) STRICT"#;

const STOPPED_RESULT_TABLE_SQL: &str = r#"CREATE TABLE stopped_result (
    gid TEXT PRIMARY KEY NOT NULL CHECK(length(gid) = 16 AND gid NOT GLOB '*[^0-9a-f]*'),
    terminal_status INTEGER NOT NULL CHECK(terminal_status IN (1, 2, 3)),
    error_code INTEGER NOT NULL CHECK(error_code BETWEEN 0 AND 29),
    safe_message TEXT NOT NULL CHECK(length(CAST(safe_message AS BLOB)) <= 4096),
    total_length BLOB CHECK(total_length IS NULL OR (typeof(total_length) = 'blob' AND length(total_length) = 8)),
    layout_hash BLOB CHECK(layout_hash IS NULL OR (typeof(layout_hash) = 'blob' AND length(layout_hash) = 32)),
    completed_ms INTEGER NOT NULL CHECK(completed_ms >= 0)
) STRICT"#;

const JOURNAL_INSTALL_TABLE_SQL: &str = r#"CREATE TABLE journal_install (
    gid TEXT PRIMARY KEY NOT NULL,
    checkpoint_id BLOB NOT NULL CHECK(typeof(checkpoint_id) = 'blob' AND length(checkpoint_id) = 16),
    old_journal_id BLOB NOT NULL CHECK(typeof(old_journal_id) = 'blob' AND length(old_journal_id) = 16),
    old_path BLOB NOT NULL CHECK(typeof(old_path) = 'blob' AND length(old_path) BETWEEN 6 AND 65541),
    new_journal_id BLOB NOT NULL CHECK(typeof(new_journal_id) = 'blob' AND length(new_journal_id) = 16),
    new_path BLOB NOT NULL CHECK(typeof(new_path) = 'blob' AND length(new_path) BETWEEN 6 AND 65541),
    source_last_sequence BLOB NOT NULL CHECK(typeof(source_last_sequence) = 'blob' AND length(source_last_sequence) = 8),
    phase INTEGER NOT NULL CHECK(phase IN (1, 2)),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    FOREIGN KEY(gid) REFERENCES task(gid) ON UPDATE CASCADE ON DELETE CASCADE
) STRICT"#;

const BT_RESUME_TABLE_SQL: &str = r#"CREATE TABLE bt_resume (
    gid TEXT PRIMARY KEY NOT NULL,
    resume_blob BLOB NOT NULL CHECK(typeof(resume_blob) = 'blob' AND length(resume_blob) <= 67108864),
    dirty INTEGER NOT NULL CHECK(dirty IN (0, 1)),
    saved_ms INTEGER NOT NULL CHECK(saved_ms >= 0),
    FOREIGN KEY(gid) REFERENCES task(gid) ON UPDATE CASCADE ON DELETE CASCADE
) STRICT"#;

const TASK_QUEUE_INDEX_SQL: &str =
    "CREATE INDEX task_queue_index ON task(queue_state, queue_position, gid)";
const TASK_SESSION_INDEX_SQL: &str = "CREATE INDEX task_session_index ON task(session_id, gid)";
const TASK_SOURCE_PRIORITY_INDEX_SQL: &str =
    "CREATE INDEX task_source_priority_index ON task_source(gid, priority, uri_id)";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SessionSchemaObjectKind {
    Table,
    Index,
}

impl SessionSchemaObjectKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Index => "index",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionSchemaObject {
    pub kind: SessionSchemaObjectKind,
    pub name: &'static str,
    pub sql: &'static str,
}

pub const SESSION_SCHEMA_OBJECTS: &[SessionSchemaObject] = &[
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "session",
        sql: SESSION_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "task",
        sql: TASK_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "task_option",
        sql: TASK_OPTION_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "task_source",
        sql: TASK_SOURCE_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "host_key_challenge",
        sql: HOST_KEY_CHALLENGE_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "stopped_result",
        sql: STOPPED_RESULT_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "journal_install",
        sql: JOURNAL_INSTALL_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Table,
        name: "bt_resume",
        sql: BT_RESUME_TABLE_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Index,
        name: "task_queue_index",
        sql: TASK_QUEUE_INDEX_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Index,
        name: "task_session_index",
        sql: TASK_SESSION_INDEX_SQL,
    },
    SessionSchemaObject {
        kind: SessionSchemaObjectKind::Index,
        name: "task_source_priority_index",
        sql: TASK_SOURCE_PRIORITY_INDEX_SQL,
    },
];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SessionSqliteLimit {
    Length,
    SqlLength,
    Column,
    ExpressionDepth,
    CompoundSelect,
    FunctionArgument,
    Attached,
    LikePatternLength,
    VariableNumber,
    TriggerDepth,
    WorkerThreads,
}

impl SessionSqliteLimit {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Length => "length",
            Self::SqlLength => "sql_length",
            Self::Column => "column",
            Self::ExpressionDepth => "expression_depth",
            Self::CompoundSelect => "compound_select",
            Self::FunctionArgument => "function_argument",
            Self::Attached => "attached",
            Self::LikePatternLength => "like_pattern_length",
            Self::VariableNumber => "variable_number",
            Self::TriggerDepth => "trigger_depth",
            Self::WorkerThreads => "worker_threads",
        }
    }

    #[must_use]
    pub const fn value(self) -> i32 {
        match self {
            Self::Length => 80 * 1024 * 1024,
            Self::SqlLength => 1024 * 1024,
            Self::Column => 64,
            Self::ExpressionDepth => 100,
            Self::CompoundSelect => 16,
            Self::FunctionArgument => 32,
            Self::Attached => 0,
            Self::LikePatternLength => 64 * 1024,
            Self::VariableNumber => 256,
            Self::TriggerDepth => 16,
            Self::WorkerThreads => 0,
        }
    }

    const fn rusqlite(self) -> Limit {
        match self {
            Self::Length => Limit::SQLITE_LIMIT_LENGTH,
            Self::SqlLength => Limit::SQLITE_LIMIT_SQL_LENGTH,
            Self::Column => Limit::SQLITE_LIMIT_COLUMN,
            Self::ExpressionDepth => Limit::SQLITE_LIMIT_EXPR_DEPTH,
            Self::CompoundSelect => Limit::SQLITE_LIMIT_COMPOUND_SELECT,
            Self::FunctionArgument => Limit::SQLITE_LIMIT_FUNCTION_ARG,
            Self::Attached => Limit::SQLITE_LIMIT_ATTACHED,
            Self::LikePatternLength => Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
            Self::VariableNumber => Limit::SQLITE_LIMIT_VARIABLE_NUMBER,
            Self::TriggerDepth => Limit::SQLITE_LIMIT_TRIGGER_DEPTH,
            Self::WorkerThreads => Limit::SQLITE_LIMIT_WORKER_THREADS,
        }
    }
}

pub const ALL_SESSION_SQLITE_LIMITS: [SessionSqliteLimit; 11] = [
    SessionSqliteLimit::Length,
    SessionSqliteLimit::SqlLength,
    SessionSqliteLimit::Column,
    SessionSqliteLimit::ExpressionDepth,
    SessionSqliteLimit::CompoundSelect,
    SessionSqliteLimit::FunctionArgument,
    SessionSqliteLimit::Attached,
    SessionSqliteLimit::LikePatternLength,
    SessionSqliteLimit::VariableNumber,
    SessionSqliteLimit::TriggerDepth,
    SessionSqliteLimit::WorkerThreads,
];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(i64)]
pub enum SessionQueueState {
    Waiting = 1,
    Active = 2,
    Paused = 3,
    Stopped = 4,
}

impl SessionQueueState {
    pub const ALL: [Self; 4] = [Self::Waiting, Self::Active, Self::Paused, Self::Stopped];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }
}

impl TryFrom<i64> for SessionQueueState {
    type Error = SessionStoreError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Waiting),
            2 => Ok(Self::Active),
            3 => Ok(Self::Paused),
            4 => Ok(Self::Stopped),
            _ => Err(SessionStoreError::InvalidPersistedValue("queue_state")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(i64)]
pub enum JournalInstallPhase {
    Installing = 1,
    Installed = 2,
}

impl JournalInstallPhase {
    pub const ALL: [Self; 2] = [Self::Installing, Self::Installed];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Installing => "installing",
            Self::Installed => "installed",
        }
    }
}

impl TryFrom<i64> for JournalInstallPhase {
    type Error = SessionStoreError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Installing),
            2 => Ok(Self::Installed),
            _ => Err(SessionStoreError::InvalidPersistedValue(
                "journal_install.phase",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionJournalMode {
    Wal,
    Delete,
}

impl SessionJournalMode {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionId([u8; 16]);

impl SessionId {
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionStoreConfig {
    pub cache_kib: u32,
    pub busy_timeout_ms: u64,
    pub prefer_wal: bool,
}

impl Default for SessionStoreConfig {
    fn default() -> Self {
        Self {
            cache_kib: SESSION_DEFAULT_CACHE_KIB,
            busy_timeout_ms: SESSION_BUSY_TIMEOUT_MS,
            prefer_wal: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRecord {
    pub session_id: SessionId,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub clean_shutdown: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionNoSpaceCondition {
    pub target: PlatformPath,
    pub scheduled_at_ms: u64,
    pub delay_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTaskRecord {
    pub gid: Gid,
    pub session_id: SessionId,
    pub queue_state: SessionQueueState,
    pub queue_position: u32,
    pub desired_paused: bool,
    pub primary_journal_id: JournalId,
    pub primary_journal_path: PlatformPath,
    pub replica_journal_path: Option<PlatformPath>,
    pub replica_sequence: Option<u64>,
    pub root_display: PlatformPath,
    pub cached_layout_hash: Option<JournalHash>,
    pub cached_root_binding_hash: Option<JournalHash>,
    pub cached_snapshot_hash: JournalHash,
    pub no_space: Option<SessionNoSpaceCondition>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionJournalCache {
    pub layout_hash: Option<JournalHash>,
    pub root_binding_hash: Option<JournalHash>,
    pub snapshot_hash: JournalHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionCacheReconciliation {
    Unchanged,
    Updated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalInstallIntent {
    pub gid: Gid,
    pub checkpoint_id: CheckpointId,
    pub old_journal_id: JournalId,
    pub old_path: PlatformPath,
    pub new_journal_id: JournalId,
    pub new_path: PlatformPath,
    pub source_last_sequence: u64,
    pub phase: JournalInstallPhase,
    pub created_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStoreSettings {
    pub journal_mode: SessionJournalMode,
    pub page_size: i64,
    pub synchronous: i64,
    pub foreign_keys: bool,
    pub cache_size: i64,
    pub mmap_size: i64,
    pub wal_auto_checkpoint: i64,
    pub limits: BTreeMap<&'static str, i32>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SessionIoOperation {
    InspectPath,
    CreateDirectory,
    CreateDatabase,
    TightenPermissions,
    CreateBackup,
    RemoveFailedBackup,
}

impl SessionIoOperation {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InspectPath => "inspect_path",
            Self::CreateDirectory => "create_directory",
            Self::CreateDatabase => "create_database",
            Self::TightenPermissions => "tighten_permissions",
            Self::CreateBackup => "create_backup",
            Self::RemoveFailedBackup => "remove_failed_backup",
        }
    }
}

pub const ALL_SESSION_IO_OPERATIONS: [SessionIoOperation; 6] = [
    SessionIoOperation::InspectPath,
    SessionIoOperation::CreateDirectory,
    SessionIoOperation::CreateDatabase,
    SessionIoOperation::TightenPermissions,
    SessionIoOperation::CreateBackup,
    SessionIoOperation::RemoveFailedBackup,
];

#[derive(Debug)]
pub enum SessionStoreError {
    Sqlite(rusqlite::Error),
    Io {
        operation: SessionIoOperation,
        kind: io::ErrorKind,
    },
    InvalidConfig(&'static str),
    NewerSchema {
        found: u32,
        supported: u32,
    },
    UnversionedDatabase,
    SchemaMismatch(&'static str),
    IntegrityCheckFailed,
    LimitRejected {
        limit: SessionSqliteLimit,
        requested: i32,
        actual: i32,
    },
    InvalidPersistedValue(&'static str),
    InvalidRecord(&'static str),
    SessionConflict,
    QueueInvariant,
    NotFound,
    ForbiddenPersistedOption,
    JournalPointerMismatch,
    JournalInstallConflict,
    BackupPathExists,
    WalCheckpointBusy,
}

impl SessionStoreError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "sqlite",
            Self::Io { .. } => "io",
            Self::InvalidConfig(_) => "invalid_config",
            Self::NewerSchema { .. } => "newer_schema",
            Self::UnversionedDatabase => "unversioned_database",
            Self::SchemaMismatch(_) => "schema_mismatch",
            Self::IntegrityCheckFailed => "integrity_check_failed",
            Self::LimitRejected { .. } => "limit_rejected",
            Self::InvalidPersistedValue(_) => "invalid_persisted_value",
            Self::InvalidRecord(_) => "invalid_record",
            Self::SessionConflict => "session_conflict",
            Self::QueueInvariant => "queue_invariant",
            Self::NotFound => "not_found",
            Self::ForbiddenPersistedOption => "forbidden_persisted_option",
            Self::JournalPointerMismatch => "journal_pointer_mismatch",
            Self::JournalInstallConflict => "journal_install_conflict",
            Self::BackupPathExists => "backup_path_exists",
            Self::WalCheckpointBusy => "wal_checkpoint_busy",
        }
    }
}

pub const ALL_SESSION_STORE_ERROR_CODES: [&str; 18] = [
    "sqlite",
    "io",
    "invalid_config",
    "newer_schema",
    "unversioned_database",
    "schema_mismatch",
    "integrity_check_failed",
    "limit_rejected",
    "invalid_persisted_value",
    "invalid_record",
    "session_conflict",
    "queue_invariant",
    "not_found",
    "forbidden_persisted_option",
    "journal_pointer_mismatch",
    "journal_install_conflict",
    "backup_path_exists",
    "wal_checkpoint_busy",
];

impl fmt::Display for SessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => error.fmt(formatter),
            Self::Io { operation, kind } => {
                write!(formatter, "session {} failed: {kind}", operation.code())
            }
            Self::InvalidConfig(field) => write!(formatter, "invalid session config: {field}"),
            Self::NewerSchema { found, supported } => write!(
                formatter,
                "session schema {found} is newer than supported schema {supported}"
            ),
            Self::UnversionedDatabase => {
                formatter.write_str("unversioned database contains schema objects")
            }
            Self::SchemaMismatch(object) => write!(formatter, "session schema mismatch: {object}"),
            Self::IntegrityCheckFailed => formatter.write_str("SQLite integrity check failed"),
            Self::LimitRejected {
                limit,
                requested,
                actual,
            } => write!(
                formatter,
                "SQLite limit {} requested {requested}, applied {actual}",
                limit.code()
            ),
            Self::InvalidPersistedValue(field) => {
                write!(formatter, "invalid persisted session value: {field}")
            }
            Self::InvalidRecord(field) => write!(formatter, "invalid session record: {field}"),
            Self::SessionConflict => formatter.write_str("a different session already exists"),
            Self::QueueInvariant => formatter.write_str("queue positions are not dense and unique"),
            Self::NotFound => formatter.write_str("session record was not found"),
            Self::ForbiddenPersistedOption => {
                formatter.write_str("option policy forbids persistence")
            }
            Self::JournalPointerMismatch => {
                formatter.write_str("journal install old pointer does not match the task")
            }
            Self::JournalInstallConflict => {
                formatter.write_str("journal install intent conflicts with persisted state")
            }
            Self::BackupPathExists => formatter.write_str("backup path already exists"),
            Self::WalCheckpointBusy => formatter.write_str("WAL truncate checkpoint remained busy"),
        }
    }
}

impl Error for SessionStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for SessionStoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

pub struct SessionStore {
    path: PathBuf,
    connection: Connection,
    journal_mode: SessionJournalMode,
    config: SessionStoreConfig,
}

impl SessionStore {
    pub fn open(
        path: impl AsRef<Path>,
        config: SessionStoreConfig,
    ) -> Result<Self, SessionStoreError> {
        validate_config(config)?;
        let path = path.as_ref().to_path_buf();
        let existed = prepare_database_path(&path)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut connection = Connection::open_with_flags(&path, flags)?;
        apply_limits(&connection)?;
        connection.busy_timeout(Duration::from_millis(config.busy_timeout_ms))?;
        let version = read_user_version(&connection)?;
        if version > SESSION_SCHEMA_VERSION {
            return Err(SessionStoreError::NewerSchema {
                found: version,
                supported: SESSION_SCHEMA_VERSION,
            });
        }
        if version == 0 && count_schema_objects(&connection)? != 0 {
            return Err(SessionStoreError::UnversionedDatabase);
        }
        if existed {
            tighten_database_permissions(&path)?;
        }
        if version == SESSION_SCHEMA_VERSION {
            validate_integrity(&connection)?;
        } else {
            connection.pragma_update(None, "page_size", SESSION_PAGE_SIZE_BYTES)?;
        }
        let journal_mode = configure_pragmas(&connection, config)?;
        if version == 0 {
            create_schema(&mut connection)?;
        }
        validate_schema(&connection)?;
        validate_integrity(&connection)?;
        tighten_database_permissions(&path)?;
        Ok(Self {
            path,
            connection,
            journal_mode,
            config,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub const fn journal_mode(&self) -> SessionJournalMode {
        self.journal_mode
    }

    #[must_use]
    pub const fn config(&self) -> SessionStoreConfig {
        self.config
    }

    pub fn settings(&self) -> Result<SessionStoreSettings, SessionStoreError> {
        let mut limits = BTreeMap::new();
        for limit in ALL_SESSION_SQLITE_LIMITS {
            limits.insert(limit.code(), self.connection.limit(limit.rusqlite())?);
        }
        Ok(SessionStoreSettings {
            journal_mode: self.journal_mode,
            page_size: pragma_i64(&self.connection, "page_size")?,
            synchronous: pragma_i64(&self.connection, "synchronous")?,
            foreign_keys: pragma_i64(&self.connection, "foreign_keys")? == 1,
            cache_size: pragma_i64(&self.connection, "cache_size")?,
            mmap_size: pragma_i64(&self.connection, "mmap_size")?,
            wal_auto_checkpoint: pragma_i64(&self.connection, "wal_autocheckpoint")?,
            limits,
        })
    }

    pub fn put_session(&mut self, record: &SessionRecord) -> Result<(), SessionStoreError> {
        validate_time_order(record.created_ms, record.updated_ms)?;
        let existing = self
            .connection
            .prepare("SELECT session_id FROM session ORDER BY session_id")?
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if existing.len() > 1
            || existing
                .first()
                .is_some_and(|value| value.as_slice() != record.session_id.as_bytes())
        {
            return Err(SessionStoreError::SessionConflict);
        }
        self.connection.execute(
            "INSERT INTO session(session_id, created_ms, updated_ms, clean_shutdown) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(session_id) DO UPDATE SET created_ms = excluded.created_ms, updated_ms = excluded.updated_ms, clean_shutdown = excluded.clean_shutdown",
            params![
                record.session_id.as_bytes().as_slice(),
                time_to_i64(record.created_ms, "session.created_ms")?,
                time_to_i64(record.updated_ms, "session.updated_ms")?,
                bool_to_i64(record.clean_shutdown),
            ],
        )?;
        Ok(())
    }

    pub fn session(&self) -> Result<Option<SessionRecord>, SessionStoreError> {
        let rows = self
            .connection
            .prepare(
                "SELECT session_id, created_ms, updated_ms, clean_shutdown FROM session ORDER BY session_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        match rows.as_slice() {
            [] => Ok(None),
            [(id, created, updated, clean)] => Ok(Some(SessionRecord {
                session_id: decode_session_id(id)?,
                created_ms: nonnegative_i64(*created, "session.created_ms")?,
                updated_ms: nonnegative_i64(*updated, "session.updated_ms")?,
                clean_shutdown: decode_bool(*clean, "session.clean_shutdown")?,
            })),
            _ => Err(SessionStoreError::InvalidPersistedValue(
                "multiple_session_rows",
            )),
        }
    }

    pub fn put_task(&mut self, task: &SessionTaskRecord) -> Result<(), SessionStoreError> {
        validate_task(task)?;
        let primary_path = encode_platform_path(&task.primary_journal_path)?;
        let replica_path = task
            .replica_journal_path
            .as_ref()
            .map(encode_platform_path)
            .transpose()?;
        let replica_sequence = task.replica_sequence.map(encode_u64);
        let root_display = encode_platform_path(&task.root_display)?;
        let no_space_target = task
            .no_space
            .as_ref()
            .map(|value| encode_platform_path(&value.target))
            .transpose()?;
        let no_space_scheduled = task
            .no_space
            .as_ref()
            .map(|value| time_to_i64(value.scheduled_at_ms, "task.no_space_scheduled_at_ms"))
            .transpose()?;
        let no_space_delay = task
            .no_space
            .as_ref()
            .map(|value| encode_u64(value.delay_ms));
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO task(gid, session_id, queue_state, queue_position, desired_paused, primary_journal_id, primary_journal_path, replica_journal_path, replica_sequence, root_display, cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash, no_space_target, no_space_scheduled_at_ms, no_space_delay_ms, created_ms, updated_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18) ON CONFLICT(gid) DO UPDATE SET session_id = excluded.session_id, queue_state = excluded.queue_state, queue_position = excluded.queue_position, desired_paused = excluded.desired_paused, primary_journal_id = excluded.primary_journal_id, primary_journal_path = excluded.primary_journal_path, replica_journal_path = excluded.replica_journal_path, replica_sequence = excluded.replica_sequence, root_display = excluded.root_display, cached_layout_hash = excluded.cached_layout_hash, cached_root_binding_hash = excluded.cached_root_binding_hash, cached_snapshot_hash = excluded.cached_snapshot_hash, no_space_target = excluded.no_space_target, no_space_scheduled_at_ms = excluded.no_space_scheduled_at_ms, no_space_delay_ms = excluded.no_space_delay_ms, created_ms = excluded.created_ms, updated_ms = excluded.updated_ms",
            params![
                task.gid.to_string(),
                task.session_id.as_bytes().as_slice(),
                task.queue_state as i64,
                i64::from(task.queue_position),
                bool_to_i64(task.desired_paused),
                task.primary_journal_id.as_bytes().as_slice(),
                primary_path,
                replica_path,
                replica_sequence,
                root_display,
                task.cached_layout_hash.map(|value| value.as_bytes().to_vec()),
                task.cached_root_binding_hash.map(|value| value.as_bytes().to_vec()),
                task.cached_snapshot_hash.as_bytes().as_slice(),
                no_space_target,
                no_space_scheduled,
                no_space_delay,
                time_to_i64(task.created_ms, "task.created_ms")?,
                time_to_i64(task.updated_ms, "task.updated_ms")?,
            ],
        )?;
        validate_dense_queues(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn tasks(&self) -> Result<Vec<SessionTaskRecord>, SessionStoreError> {
        let mut statement = self.connection.prepare(
            "SELECT gid, session_id, queue_state, queue_position, desired_paused, primary_journal_id, primary_journal_path, replica_journal_path, replica_sequence, root_display, cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash, no_space_target, no_space_scheduled_at_ms, no_space_delay_ms, created_ms, updated_ms FROM task ORDER BY queue_state, queue_position, gid",
        )?;
        let raw = statement
            .query_map([], RawTaskRow::from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        raw.into_iter().map(RawTaskRow::decode).collect()
    }

    pub fn reorder_queue(
        &mut self,
        state: SessionQueueState,
        ordered_gids: &[Gid],
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        let requested = ordered_gids.iter().copied().collect::<HashSet<_>>();
        if requested.len() != ordered_gids.len() {
            return Err(SessionStoreError::QueueInvariant);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .prepare("SELECT gid FROM task WHERE queue_state = ?1 ORDER BY gid")?
            .query_map([state as i64], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|value| decode_gid(&value))
            .collect::<Result<HashSet<_>, _>>()?;
        if existing != requested {
            return Err(SessionStoreError::QueueInvariant);
        }
        {
            let mut statement = transaction.prepare(
                "UPDATE task SET queue_position = ?1, updated_ms = ?2 WHERE gid = ?3 AND queue_state = ?4",
            )?;
            for (position, gid) in ordered_gids.iter().copied().enumerate() {
                let position =
                    i64::try_from(position).map_err(|_| SessionStoreError::QueueInvariant)?;
                let changed = statement.execute(params![
                    position,
                    time_to_i64(updated_ms, "task.updated_ms")?,
                    gid.to_string(),
                    state as i64,
                ])?;
                if changed != 1 {
                    return Err(SessionStoreError::QueueInvariant);
                }
            }
        }
        validate_dense_queues(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn replace_task_options(
        &mut self,
        gid: Gid,
        scope: OptionsSnapshotScope,
        options: &SanitizedOptionMap,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        if options.entries().any(|(key, _)| !policy.permits(key)) {
            return Err(SessionStoreError::ForbiddenPersistedOption);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM task_option WHERE gid = ?1 AND scope = ?2",
            params![gid.to_string(), scope.number()],
        )?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO task_option(gid, scope, key, canonical_value) VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (key, value) in options.entries() {
                statement.execute(params![
                    gid.to_string(),
                    scope.number(),
                    key,
                    value.as_bytes(),
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn task_options(
        &self,
        gid: Gid,
        scope: OptionsSnapshotScope,
    ) -> Result<SanitizedOptionMap, SessionStoreError> {
        let entries = self
            .connection
            .prepare(
                "SELECT key, canonical_value FROM task_option WHERE gid = ?1 AND scope = ?2 ORDER BY key",
            )?
            .query_map(params![gid.to_string(), scope.number()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(entries.len())
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_option.allocation"))?;
        for (key, value) in entries {
            let value = String::from_utf8(value)
                .map_err(|_| SessionStoreError::InvalidPersistedValue("task_option.value"))?;
            decoded.push((key, value));
        }
        SanitizedOptionMap::new(decoded)
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_option"))
    }

    pub fn reconcile_journal_cache(
        &mut self,
        gid: Gid,
        cache: SessionJournalCache,
        updated_ms: u64,
    ) -> Result<SessionCacheReconciliation, SessionStoreError> {
        if cache.layout_hash.is_some() != cache.root_binding_hash.is_some() {
            return Err(SessionStoreError::InvalidRecord(
                "layout_and_root_hash_presence",
            ));
        }
        let existing = self
            .connection
            .query_row(
                "SELECT cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash FROM task WHERE gid = ?1",
                [gid.to_string()],
                |row| {
                    Ok((
                        row.get::<_, Option<Vec<u8>>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        let existing_cache = SessionJournalCache {
            layout_hash: existing
                .0
                .as_deref()
                .map(|value| decode_hash(value, "task.cached_layout_hash"))
                .transpose()?,
            root_binding_hash: existing
                .1
                .as_deref()
                .map(|value| decode_hash(value, "task.cached_root_binding_hash"))
                .transpose()?,
            snapshot_hash: decode_hash(&existing.2, "task.cached_snapshot_hash")?,
        };
        if existing_cache == cache {
            return Ok(SessionCacheReconciliation::Unchanged);
        }
        let changed = self.connection.execute(
            "UPDATE task SET cached_layout_hash = ?1, cached_root_binding_hash = ?2, cached_snapshot_hash = ?3, updated_ms = ?4 WHERE gid = ?5",
            params![
                cache.layout_hash.map(|value| value.as_bytes().to_vec()),
                cache
                    .root_binding_hash
                    .map(|value| value.as_bytes().to_vec()),
                cache.snapshot_hash.as_bytes().as_slice(),
                time_to_i64(updated_ms, "task.updated_ms")?,
                gid.to_string(),
            ],
        )?;
        if changed != 1 {
            return Err(SessionStoreError::NotFound);
        }
        Ok(SessionCacheReconciliation::Updated)
    }

    pub fn begin_journal_install(
        &mut self,
        intent: &JournalInstallIntent,
    ) -> Result<(), SessionStoreError> {
        validate_install_intent(intent)?;
        let old_path = encode_platform_path(&intent.old_path)?;
        let new_path = encode_platform_path(&intent.new_path)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT primary_journal_id, primary_journal_path FROM task WHERE gid = ?1",
                [intent.gid.to_string()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        if current.0 != intent.old_journal_id.as_bytes() || current.1 != old_path {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
        let existing: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM journal_install WHERE gid = ?1",
            [intent.gid.to_string()],
            |row| row.get(0),
        )?;
        if existing != 0 {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        transaction.execute(
            "INSERT INTO journal_install(gid, checkpoint_id, old_journal_id, old_path, new_journal_id, new_path, source_last_sequence, phase, created_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                intent.gid.to_string(),
                intent.checkpoint_id.as_bytes().as_slice(),
                intent.old_journal_id.as_bytes().as_slice(),
                old_path,
                intent.new_journal_id.as_bytes().as_slice(),
                new_path,
                encode_u64(intent.source_last_sequence),
                intent.phase as i64,
                time_to_i64(intent.created_ms, "journal_install.created_ms")?,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_journal_install(
        &mut self,
        gid: Gid,
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let install = transaction
            .query_row(
                "SELECT old_journal_id, old_path, new_journal_id, new_path, phase FROM journal_install WHERE gid = ?1",
                [gid.to_string()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        if JournalInstallPhase::try_from(install.4)? != JournalInstallPhase::Installing {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        let current = transaction
            .query_row(
                "SELECT primary_journal_id, primary_journal_path FROM task WHERE gid = ?1",
                [gid.to_string()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        if current.0 != install.0 || current.1 != install.1 {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
        let changed = transaction.execute(
            "UPDATE task SET primary_journal_id = ?1, primary_journal_path = ?2, updated_ms = ?3 WHERE gid = ?4",
            params![
                install.2,
                install.3,
                time_to_i64(updated_ms, "task.updated_ms")?,
                gid.to_string(),
            ],
        )?;
        if changed != 1 {
            return Err(SessionStoreError::NotFound);
        }
        transaction.execute(
            "UPDATE journal_install SET phase = ?1 WHERE gid = ?2",
            params![JournalInstallPhase::Installed as i64, gid.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn clear_installed_journal(&mut self, gid: Gid) -> Result<(), SessionStoreError> {
        let changed = self.connection.execute(
            "DELETE FROM journal_install WHERE gid = ?1 AND phase = ?2",
            params![gid.to_string(), JournalInstallPhase::Installed as i64],
        )?;
        if changed == 1 {
            Ok(())
        } else {
            Err(SessionStoreError::JournalInstallConflict)
        }
    }

    pub fn journal_installs(&self) -> Result<Vec<JournalInstallIntent>, SessionStoreError> {
        let raw = self
            .connection
            .prepare(
                "SELECT gid, checkpoint_id, old_journal_id, old_path, new_journal_id, new_path, source_last_sequence, phase, created_ms FROM journal_install ORDER BY gid",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        raw.into_iter()
            .map(
                |(
                    gid,
                    checkpoint,
                    old_id,
                    old_path,
                    new_id,
                    new_path,
                    sequence,
                    phase,
                    created,
                )| {
                    Ok(JournalInstallIntent {
                        gid: decode_gid(&gid)?,
                        checkpoint_id: decode_checkpoint_id(&checkpoint)?,
                        old_journal_id: decode_journal_id(
                            &old_id,
                            "journal_install.old_journal_id",
                        )?,
                        old_path: decode_platform_path(&old_path, "journal_install.old_path")?,
                        new_journal_id: decode_journal_id(
                            &new_id,
                            "journal_install.new_journal_id",
                        )?,
                        new_path: decode_platform_path(&new_path, "journal_install.new_path")?,
                        source_last_sequence: decode_u64(
                            &sequence,
                            "journal_install.source_last_sequence",
                        )?,
                        phase: JournalInstallPhase::try_from(phase)?,
                        created_ms: nonnegative_i64(created, "journal_install.created_ms")?,
                    })
                },
            )
            .collect()
    }

    pub fn integrity_check(&self) -> Result<(), SessionStoreError> {
        validate_integrity(&self.connection)
    }

    pub fn checkpoint_wal_truncate(&self) -> Result<(), SessionStoreError> {
        if self.journal_mode != SessionJournalMode::Wal {
            return Ok(());
        }
        let (busy, _log_pages, _checkpointed_pages): (i64, i64, i64) =
            self.connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
        if busy == 0 {
            Ok(())
        } else {
            Err(SessionStoreError::WalCheckpointBusy)
        }
    }

    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<(), SessionStoreError> {
        let destination = destination.as_ref();
        if destination
            .try_exists()
            .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?
        {
            return Err(SessionStoreError::BackupPathExists);
        }
        create_secure_file(destination, SessionIoOperation::CreateBackup)?;
        if let Err(error) = self.connection.backup(rusqlite::MAIN_DB, destination, None) {
            if let Err(remove_error) = fs::remove_file(destination) {
                return Err(session_io_error(
                    SessionIoOperation::RemoveFailedBackup,
                    remove_error,
                ));
            }
            return Err(SessionStoreError::Sqlite(error));
        }
        tighten_database_permissions(destination)?;
        let backup = Connection::open_with_flags(destination, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        apply_limits(&backup)?;
        validate_integrity(&backup)?;
        validate_schema(&backup)?;
        Ok(())
    }
}

struct RawTaskRow {
    gid: String,
    session_id: Vec<u8>,
    queue_state: i64,
    queue_position: i64,
    desired_paused: i64,
    primary_journal_id: Vec<u8>,
    primary_journal_path: Vec<u8>,
    replica_journal_path: Option<Vec<u8>>,
    replica_sequence: Option<Vec<u8>>,
    root_display: Vec<u8>,
    cached_layout_hash: Option<Vec<u8>>,
    cached_root_binding_hash: Option<Vec<u8>>,
    cached_snapshot_hash: Vec<u8>,
    no_space_target: Option<Vec<u8>>,
    no_space_scheduled_at_ms: Option<i64>,
    no_space_delay_ms: Option<Vec<u8>>,
    created_ms: i64,
    updated_ms: i64,
}

impl RawTaskRow {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            gid: row.get(0)?,
            session_id: row.get(1)?,
            queue_state: row.get(2)?,
            queue_position: row.get(3)?,
            desired_paused: row.get(4)?,
            primary_journal_id: row.get(5)?,
            primary_journal_path: row.get(6)?,
            replica_journal_path: row.get(7)?,
            replica_sequence: row.get(8)?,
            root_display: row.get(9)?,
            cached_layout_hash: row.get(10)?,
            cached_root_binding_hash: row.get(11)?,
            cached_snapshot_hash: row.get(12)?,
            no_space_target: row.get(13)?,
            no_space_scheduled_at_ms: row.get(14)?,
            no_space_delay_ms: row.get(15)?,
            created_ms: row.get(16)?,
            updated_ms: row.get(17)?,
        })
    }

    fn decode(self) -> Result<SessionTaskRecord, SessionStoreError> {
        let replica_journal_path = self
            .replica_journal_path
            .as_deref()
            .map(|value| decode_platform_path(value, "task.replica_journal_path"))
            .transpose()?;
        let replica_sequence = self
            .replica_sequence
            .as_deref()
            .map(|value| decode_u64(value, "task.replica_sequence"))
            .transpose()?;
        if replica_journal_path.is_some() != replica_sequence.is_some() {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task.replica_pair",
            ));
        }
        let no_space = match (
            self.no_space_target,
            self.no_space_scheduled_at_ms,
            self.no_space_delay_ms,
        ) {
            (None, None, None) => None,
            (Some(target), Some(scheduled), Some(delay)) => Some(SessionNoSpaceCondition {
                target: decode_platform_path(&target, "task.no_space_target")?,
                scheduled_at_ms: nonnegative_i64(scheduled, "task.no_space_scheduled_at_ms")?,
                delay_ms: decode_u64(&delay, "task.no_space_delay_ms")?,
            }),
            _ => {
                return Err(SessionStoreError::InvalidPersistedValue(
                    "task.no_space_tuple",
                ));
            }
        };
        let queue_position = u32::try_from(self.queue_position)
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task.queue_position"))?;
        Ok(SessionTaskRecord {
            gid: decode_gid(&self.gid)?,
            session_id: decode_session_id(&self.session_id)?,
            queue_state: SessionQueueState::try_from(self.queue_state)?,
            queue_position,
            desired_paused: decode_bool(self.desired_paused, "task.desired_paused")?,
            primary_journal_id: decode_journal_id(
                &self.primary_journal_id,
                "task.primary_journal_id",
            )?,
            primary_journal_path: decode_platform_path(
                &self.primary_journal_path,
                "task.primary_journal_path",
            )?,
            replica_journal_path,
            replica_sequence,
            root_display: decode_platform_path(&self.root_display, "task.root_display")?,
            cached_layout_hash: self
                .cached_layout_hash
                .as_deref()
                .map(|value| decode_hash(value, "task.cached_layout_hash"))
                .transpose()?,
            cached_root_binding_hash: self
                .cached_root_binding_hash
                .as_deref()
                .map(|value| decode_hash(value, "task.cached_root_binding_hash"))
                .transpose()?,
            cached_snapshot_hash: decode_hash(
                &self.cached_snapshot_hash,
                "task.cached_snapshot_hash",
            )?,
            no_space,
            created_ms: nonnegative_i64(self.created_ms, "task.created_ms")?,
            updated_ms: nonnegative_i64(self.updated_ms, "task.updated_ms")?,
        })
    }
}

fn validate_config(config: SessionStoreConfig) -> Result<(), SessionStoreError> {
    if !(SESSION_MIN_CACHE_KIB..=SESSION_MAX_CACHE_KIB).contains(&config.cache_kib) {
        return Err(SessionStoreError::InvalidConfig("cache_kib"));
    }
    if config.busy_timeout_ms == 0 || config.busy_timeout_ms > 60_000 {
        return Err(SessionStoreError::InvalidConfig("busy_timeout_ms"));
    }
    Ok(())
}

fn prepare_database_path(path: &Path) -> Result<bool, SessionStoreError> {
    let existed = path
        .try_exists()
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    if existed {
        return Ok(true);
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .map_err(|error| session_io_error(SessionIoOperation::CreateDirectory, error))?;
    }
    create_secure_file(path, SessionIoOperation::CreateDatabase)?;
    Ok(false)
}

fn create_secure_file(path: &Path, operation: SessionIoOperation) -> Result<(), SessionStoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map(|_| ())
        .map_err(|error| session_io_error(operation, error))
}

#[cfg(unix)]
fn tighten_database_permissions(path: &Path) -> Result<(), SessionStoreError> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))?
        .permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))
}

#[cfg(not(unix))]
fn tighten_database_permissions(_path: &Path) -> Result<(), SessionStoreError> {
    Ok(())
}

fn configure_pragmas(
    connection: &Connection,
    config: SessionStoreConfig,
) -> Result<SessionJournalMode, SessionStoreError> {
    connection.pragma_update(None, "foreign_keys", true)?;
    let journal_mode = if config.prefer_wal {
        match connection
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
        {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => SessionJournalMode::Wal,
            Ok(_) | Err(_) => {
                let mode =
                    connection.pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                        row.get::<_, String>(0)
                    })?;
                if !mode.eq_ignore_ascii_case("delete") {
                    return Err(SessionStoreError::InvalidPersistedValue("journal_mode"));
                }
                SessionJournalMode::Delete
            }
        }
    } else {
        let mode = connection.pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
            row.get::<_, String>(0)
        })?;
        if !mode.eq_ignore_ascii_case("delete") {
            return Err(SessionStoreError::InvalidPersistedValue("journal_mode"));
        }
        SessionJournalMode::Delete
    };
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "cache_size", -i64::from(config.cache_kib))?;
    connection.pragma_update(None, "mmap_size", SESSION_MMAP_SIZE_BYTES)?;
    connection.pragma_update(
        None,
        "wal_autocheckpoint",
        SESSION_WAL_AUTO_CHECKPOINT_PAGES,
    )?;
    if pragma_i64(connection, "foreign_keys")? != 1
        || pragma_i64(connection, "synchronous")? != 2
        || pragma_i64(connection, "cache_size")? != -i64::from(config.cache_kib)
        || pragma_i64(connection, "mmap_size")? != SESSION_MMAP_SIZE_BYTES
        || pragma_i64(connection, "wal_autocheckpoint")? != SESSION_WAL_AUTO_CHECKPOINT_PAGES
    {
        return Err(SessionStoreError::InvalidPersistedValue("required_pragma"));
    }
    Ok(journal_mode)
}

fn apply_limits(connection: &Connection) -> Result<(), SessionStoreError> {
    for limit in ALL_SESSION_SQLITE_LIMITS {
        let requested = limit.value();
        connection.set_limit(limit.rusqlite(), requested)?;
        let actual = connection.limit(limit.rusqlite())?;
        if actual != requested {
            return Err(SessionStoreError::LimitRejected {
                limit,
                requested,
                actual,
            });
        }
    }
    Ok(())
}

fn create_schema(connection: &mut Connection) -> Result<(), SessionStoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for object in SESSION_SCHEMA_OBJECTS {
        transaction.execute(object.sql, [])?;
    }
    transaction.pragma_update(None, "user_version", SESSION_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn validate_schema(connection: &Connection) -> Result<(), SessionStoreError> {
    if read_user_version(connection)? != SESSION_SCHEMA_VERSION {
        return Err(SessionStoreError::SchemaMismatch("user_version"));
    }
    let mut found = HashSet::new();
    let mut statement = connection.prepare(
        "SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    for row in rows {
        let (kind, name, sql) = row?;
        let expected = SESSION_SCHEMA_OBJECTS
            .iter()
            .find(|object| object.kind.code() == kind && object.name == name)
            .ok_or(SessionStoreError::SchemaMismatch("unexpected_object"))?;
        let sql = sql.ok_or(SessionStoreError::SchemaMismatch(expected.name))?;
        if normalize_sql(&sql) != normalize_sql(expected.sql) {
            return Err(SessionStoreError::SchemaMismatch(expected.name));
        }
        found.insert((kind, name));
    }
    if found.len() != SESSION_SCHEMA_OBJECTS.len() {
        return Err(SessionStoreError::SchemaMismatch("missing_object"));
    }
    Ok(())
}

fn validate_integrity(connection: &Connection) -> Result<(), SessionStoreError> {
    let results = connection
        .prepare("PRAGMA integrity_check")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if results.as_slice() == ["ok"] {
        Ok(())
    } else {
        Err(SessionStoreError::IntegrityCheckFailed)
    }
}

fn normalize_sql(sql: &str) -> String {
    sql.trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn count_schema_objects(connection: &Connection) -> Result<u32, SessionStoreError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    u32::try_from(count).map_err(|_| SessionStoreError::InvalidPersistedValue("schema_count"))
}

fn read_user_version(connection: &Connection) -> Result<u32, SessionStoreError> {
    let version = pragma_i64(connection, "user_version")?;
    u32::try_from(version).map_err(|_| SessionStoreError::InvalidPersistedValue("user_version"))
}

fn pragma_i64(connection: &Connection, name: &str) -> Result<i64, SessionStoreError> {
    connection
        .pragma_query_value(None, name, |row| row.get(0))
        .map_err(SessionStoreError::Sqlite)
}

fn validate_task(task: &SessionTaskRecord) -> Result<(), SessionStoreError> {
    validate_time_order(task.created_ms, task.updated_ms)?;
    if task.replica_journal_path.is_some() != task.replica_sequence.is_some() {
        return Err(SessionStoreError::InvalidRecord("replica_pair"));
    }
    if task.cached_layout_hash.is_some() != task.cached_root_binding_hash.is_some() {
        return Err(SessionStoreError::InvalidRecord(
            "layout_and_root_hash_presence",
        ));
    }
    if task
        .no_space
        .as_ref()
        .is_some_and(|value| value.delay_ms == 0)
    {
        return Err(SessionStoreError::InvalidRecord("no_space.delay_ms"));
    }
    Ok(())
}

fn validate_install_intent(intent: &JournalInstallIntent) -> Result<(), SessionStoreError> {
    if intent.phase != JournalInstallPhase::Installing {
        return Err(SessionStoreError::InvalidRecord("journal_install.phase"));
    }
    if intent.source_last_sequence == 0 {
        return Err(SessionStoreError::InvalidRecord(
            "journal_install.source_last_sequence",
        ));
    }
    if intent.old_journal_id == intent.new_journal_id {
        return Err(SessionStoreError::InvalidRecord(
            "journal_install.journal_id",
        ));
    }
    Ok(())
}

fn validate_dense_queues(connection: &Connection) -> Result<(), SessionStoreError> {
    let rows = connection
        .prepare("SELECT queue_state, queue_position FROM task ORDER BY queue_state, queue_position, gid")?
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut next = BTreeMap::<i64, i64>::new();
    for (state, position) in rows {
        SessionQueueState::try_from(state)?;
        let expected = next.entry(state).or_insert(0);
        if position != *expected {
            return Err(SessionStoreError::QueueInvariant);
        }
        *expected += 1;
    }
    Ok(())
}

fn validate_time_order(created_ms: u64, updated_ms: u64) -> Result<(), SessionStoreError> {
    if updated_ms < created_ms {
        Err(SessionStoreError::InvalidRecord("updated_before_created"))
    } else {
        Ok(())
    }
}

fn encode_platform_path(path: &PlatformPath) -> Result<Vec<u8>, SessionStoreError> {
    let length = u32::try_from(path.bytes().len())
        .map_err(|_| SessionStoreError::InvalidRecord("platform_path.length"))?;
    let total = PLATFORM_PATH_ENCODING_OVERHEAD
        .checked_add(path.bytes().len())
        .ok_or(SessionStoreError::InvalidRecord("platform_path.length"))?;
    if total > MAX_ENCODED_PLATFORM_PATH_BYTES {
        return Err(SessionStoreError::InvalidRecord("platform_path.length"));
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(total)
        .map_err(|_| SessionStoreError::InvalidRecord("platform_path.allocation"))?;
    output.push(path.platform() as u8);
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(path.bytes());
    Ok(output)
}

fn decode_platform_path(
    input: &[u8],
    field: &'static str,
) -> Result<PlatformPath, SessionStoreError> {
    if input.len() < PLATFORM_PATH_ENCODING_OVERHEAD
        || input.len() > MAX_ENCODED_PLATFORM_PATH_BYTES
    {
        return Err(SessionStoreError::InvalidPersistedValue(field));
    }
    let platform = match input[0] {
        1 => PathPlatform::Unix,
        2 => PathPlatform::Windows,
        _ => return Err(SessionStoreError::InvalidPersistedValue(field)),
    };
    let declared = u32::from_le_bytes([input[1], input[2], input[3], input[4]]) as usize;
    if declared == 0 || declared.checked_add(PLATFORM_PATH_ENCODING_OVERHEAD) != Some(input.len()) {
        return Err(SessionStoreError::InvalidPersistedValue(field));
    }
    PlatformPath::from_native_bytes(platform, &input[PLATFORM_PATH_ENCODING_OVERHEAD..])
        .map_err(|_| SessionStoreError::InvalidPersistedValue(field))
}

fn encode_u64(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn decode_u64(input: &[u8], field: &'static str) -> Result<u64, SessionStoreError> {
    let bytes: [u8; 8] = input
        .try_into()
        .map_err(|_| SessionStoreError::InvalidPersistedValue(field))?;
    Ok(u64::from_le_bytes(bytes))
}

fn decode_session_id(input: &[u8]) -> Result<SessionId, SessionStoreError> {
    let bytes: [u8; 16] = input
        .try_into()
        .map_err(|_| SessionStoreError::InvalidPersistedValue("session_id"))?;
    Ok(SessionId::new(bytes))
}

fn decode_journal_id(input: &[u8], field: &'static str) -> Result<JournalId, SessionStoreError> {
    let bytes: [u8; 16] = input
        .try_into()
        .map_err(|_| SessionStoreError::InvalidPersistedValue(field))?;
    JournalId::new(bytes).ok_or(SessionStoreError::InvalidPersistedValue(field))
}

fn decode_checkpoint_id(input: &[u8]) -> Result<CheckpointId, SessionStoreError> {
    let bytes: [u8; 16] = input
        .try_into()
        .map_err(|_| SessionStoreError::InvalidPersistedValue("checkpoint_id"))?;
    CheckpointId::new(bytes).ok_or(SessionStoreError::InvalidPersistedValue("checkpoint_id"))
}

fn decode_hash(input: &[u8], field: &'static str) -> Result<JournalHash, SessionStoreError> {
    let bytes: [u8; 32] = input
        .try_into()
        .map_err(|_| SessionStoreError::InvalidPersistedValue(field))?;
    JournalHash::new(bytes).ok_or(SessionStoreError::InvalidPersistedValue(field))
}

fn decode_gid(input: &str) -> Result<Gid, SessionStoreError> {
    let gid = Gid::from_str(input).map_err(|_| SessionStoreError::InvalidPersistedValue("gid"))?;
    if gid.to_string() == input {
        Ok(gid)
    } else {
        Err(SessionStoreError::InvalidPersistedValue("gid"))
    }
}

fn bool_to_i64(value: bool) -> i64 {
    i64::from(u8::from(value))
}

fn decode_bool(value: i64, field: &'static str) -> Result<bool, SessionStoreError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(SessionStoreError::InvalidPersistedValue(field)),
    }
}

fn time_to_i64(value: u64, field: &'static str) -> Result<i64, SessionStoreError> {
    i64::try_from(value).map_err(|_| SessionStoreError::InvalidRecord(field))
}

fn nonnegative_i64(value: i64, field: &'static str) -> Result<u64, SessionStoreError> {
    u64::try_from(value).map_err(|_| SessionStoreError::InvalidPersistedValue(field))
}

fn session_io_error(operation: SessionIoOperation, error: io::Error) -> SessionStoreError {
    SessionStoreError::Io {
        operation,
        kind: error.kind(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_SESSION_IO_OPERATIONS, ALL_SESSION_SQLITE_LIMITS, ALL_SESSION_STORE_ERROR_CODES,
        JournalInstallIntent, JournalInstallPhase, SESSION_SCHEMA_OBJECTS, SESSION_SCHEMA_VERSION,
        SessionCacheReconciliation, SessionId, SessionJournalCache, SessionJournalMode,
        SessionNoSpaceCondition, SessionQueueState, SessionRecord, SessionStore,
        SessionStoreConfig, SessionStoreError, SessionTaskRecord,
    };
    use crate::{
        CheckpointId, JournalHash, JournalId, OptionsSnapshotScope, PathPlatform, PlatformPath,
        SanitizedOptionMap,
    };
    use ariax_config::{SecurityClass, builtin_registry};
    use ariax_core::Gid;
    use rusqlite::Connection;
    use std::collections::HashSet;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("ariax-session-store-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create test directory");
            Self { path }
        }

        fn database(&self) -> PathBuf {
            self.path.join("session.db")
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("gid")
    }

    fn hash(value: u8) -> JournalHash {
        JournalHash::new([value; 32]).expect("hash")
    }

    fn journal(value: u8) -> JournalId {
        JournalId::new([value; 16]).expect("journal id")
    }

    fn path(value: &[u8]) -> PlatformPath {
        PlatformPath::from_native_bytes(PathPlatform::Unix, value).expect("path")
    }

    fn session_record() -> SessionRecord {
        SessionRecord {
            session_id: SessionId::new([1; 16]),
            created_ms: 100,
            updated_ms: 100,
            clean_shutdown: false,
        }
    }

    fn task_record(gid: Gid, position: u32) -> SessionTaskRecord {
        SessionTaskRecord {
            gid,
            session_id: SessionId::new([1; 16]),
            queue_state: SessionQueueState::Waiting,
            queue_position: position,
            desired_paused: false,
            primary_journal_id: journal(2),
            primary_journal_path: path(format!("/journal/{gid}").as_bytes()),
            replica_journal_path: Some(path(format!("/replica/{gid}").as_bytes())),
            replica_sequence: Some(u64::MAX),
            root_display: path(format!("/output/{gid}").as_bytes()),
            cached_layout_hash: Some(hash(3)),
            cached_root_binding_hash: Some(hash(4)),
            cached_snapshot_hash: hash(5),
            no_space: Some(SessionNoSpaceCondition {
                target: path(b"/output"),
                scheduled_at_ms: 200,
                delay_ms: u64::MAX,
            }),
            created_ms: 100,
            updated_ms: 200,
        }
    }

    fn open_store(directory: &TestDirectory) -> SessionStore {
        let mut store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("open store");
        store.put_session(&session_record()).expect("put session");
        store
    }

    #[test]
    fn creates_exact_strict_schema_pragmas_and_limits() {
        let directory = TestDirectory::new();
        let store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("open store");
        let settings = store.settings().expect("settings");
        assert!(matches!(
            settings.journal_mode,
            SessionJournalMode::Wal | SessionJournalMode::Delete
        ));
        assert_eq!(settings.page_size, 4096);
        assert_eq!(settings.synchronous, 2);
        assert!(settings.foreign_keys);
        assert_eq!(settings.cache_size, -8192);
        assert_eq!(settings.mmap_size, 0);
        assert_eq!(settings.wal_auto_checkpoint, 1000);
        for limit in ALL_SESSION_SQLITE_LIMITS {
            assert_eq!(settings.limits[limit.code()], limit.value());
        }
        let connection = Connection::open(directory.database()).expect("inspect schema");
        let version: u32 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("version");
        assert_eq!(version, SESSION_SCHEMA_VERSION);
        for object in SESSION_SCHEMA_OBJECTS {
            let strict: Option<i64> = connection
                .query_row(
                    "SELECT strict FROM pragma_table_list WHERE name = ?1",
                    [object.name],
                    |row| row.get(0),
                )
                .ok();
            if object.kind.code() == "table" {
                assert_eq!(strict, Some(1), "{}", object.name);
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(directory.database())
                .expect("database metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn rejects_newer_schema_without_rewriting_database_bytes() {
        let directory = TestDirectory::new();
        let connection = Connection::open(directory.database()).expect("create newer");
        connection
            .execute_batch("CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES ('keep'); PRAGMA user_version=2;")
            .expect("seed newer");
        drop(connection);
        let before = fs::read(directory.database()).expect("read before");
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::NewerSchema {
                found: 2,
                supported: 1
            })
        ));
        assert_eq!(fs::read(directory.database()).expect("read after"), before);
    }

    #[test]
    fn rejects_unversioned_nonempty_database() {
        let directory = TestDirectory::new();
        Connection::open(directory.database())
            .expect("open")
            .execute("CREATE TABLE legacy(value INTEGER)", [])
            .expect("create legacy");
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::UnversionedDatabase)
        ));
    }

    #[test]
    fn session_and_task_round_trip_preserves_u64_blobs_and_paths() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let task = task_record(gid(1), 0);
        store.put_task(&task).expect("put task");
        assert_eq!(store.session().expect("session"), Some(session_record()));
        assert_eq!(store.tasks().expect("tasks"), vec![task]);
    }

    #[test]
    fn queue_density_failure_rolls_back_the_task_write() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("first");
        assert!(matches!(
            store.put_task(&task_record(gid(2), 2)),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert_eq!(store.tasks().expect("tasks").len(), 1);
    }

    #[test]
    fn queue_reorder_updates_all_members_in_one_dense_transaction() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("first");
        store.put_task(&task_record(gid(2), 1)).expect("second");
        store
            .reorder_queue(SessionQueueState::Waiting, &[gid(2), gid(1)], 300)
            .expect("reorder");
        let tasks = store.tasks().expect("tasks");
        assert_eq!(
            tasks.iter().map(|task| task.gid).collect::<Vec<_>>(),
            vec![gid(2), gid(1)]
        );
        assert!(matches!(
            store.reorder_queue(SessionQueueState::Waiting, &[gid(1)], 400),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert_eq!(
            store
                .tasks()
                .expect("unchanged tasks")
                .iter()
                .map(|task| task.gid)
                .collect::<Vec<_>>(),
            vec![gid(2), gid(1)]
        );
    }

    #[test]
    fn option_replacement_is_atomic_and_registry_policy_gated() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        let registry = builtin_registry();
        let policy = |name: &str| {
            registry
                .find(name)
                .is_some_and(|definition| definition.security == SecurityClass::Normal)
        };
        let safe = SanitizedOptionMap::new([("piece-length".to_owned(), "1M".to_owned())])
            .expect("safe options");
        store
            .replace_task_options(
                gid(1),
                OptionsSnapshotScope::CurrentGeneration,
                &safe,
                &policy,
            )
            .expect("store safe");
        let secret =
            SanitizedOptionMap::new([("rpc-secret".to_owned(), "seeded-secret".to_owned())])
                .expect("secret-shaped map");
        assert!(matches!(
            store.replace_task_options(
                gid(1),
                OptionsSnapshotScope::CurrentGeneration,
                &secret,
                &policy,
            ),
            Err(SessionStoreError::ForbiddenPersistedOption)
        ));
        assert_eq!(
            store
                .task_options(gid(1), OptionsSnapshotScope::CurrentGeneration)
                .expect("load options"),
            safe
        );
        assert!(
            !fs::read(directory.database())
                .expect("read database")
                .windows(b"seeded-secret".len())
                .any(|window| window == b"seeded-secret")
        );
    }

    #[test]
    fn journal_cache_reconciliation_changes_only_journal_owned_mirrors() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let task = task_record(gid(1), 0);
        store.put_task(&task).expect("task");
        let cache = SessionJournalCache {
            layout_hash: Some(hash(7)),
            root_binding_hash: Some(hash(8)),
            snapshot_hash: hash(9),
        };
        assert_eq!(
            store
                .reconcile_journal_cache(gid(1), cache, 300)
                .expect("reconcile"),
            SessionCacheReconciliation::Updated
        );
        let recovered = store.tasks().expect("tasks").pop().expect("task");
        assert_eq!(recovered.queue_state, task.queue_state);
        assert_eq!(recovered.queue_position, task.queue_position);
        assert_eq!(recovered.desired_paused, task.desired_paused);
        assert_eq!(recovered.cached_layout_hash, cache.layout_hash);
        assert_eq!(recovered.cached_root_binding_hash, cache.root_binding_hash);
        assert_eq!(recovered.cached_snapshot_hash, cache.snapshot_hash);
        assert_eq!(
            store
                .reconcile_journal_cache(gid(1), cache, 400)
                .expect("second reconcile"),
            SessionCacheReconciliation::Unchanged
        );
    }

    #[test]
    fn journal_install_pointer_switch_is_transactional_and_recoverable() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let task = task_record(gid(1), 0);
        store.put_task(&task).expect("task");
        let intent = JournalInstallIntent {
            gid: gid(1),
            checkpoint_id: CheckpointId::new([11; 16]).expect("checkpoint"),
            old_journal_id: task.primary_journal_id,
            old_path: task.primary_journal_path.clone(),
            new_journal_id: journal(12),
            new_path: path(b"/journal/new"),
            source_last_sequence: u64::MAX,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        store.begin_journal_install(&intent).expect("begin install");
        assert_eq!(
            store.journal_installs().expect("intents"),
            vec![intent.clone()]
        );
        assert_eq!(
            store.tasks().expect("tasks")[0].primary_journal_id,
            intent.old_journal_id
        );
        store
            .complete_journal_install(gid(1), 400)
            .expect("complete install");
        let installed = store.journal_installs().expect("installed");
        assert_eq!(installed[0].phase, JournalInstallPhase::Installed);
        let updated = store.tasks().expect("tasks").pop().expect("task");
        assert_eq!(updated.primary_journal_id, intent.new_journal_id);
        assert_eq!(updated.primary_journal_path, intent.new_path);
        store
            .clear_installed_journal(gid(1))
            .expect("retirement complete");
        assert!(store.journal_installs().expect("cleared").is_empty());
    }

    #[test]
    fn journal_install_completion_rechecks_the_old_authoritative_pointer() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let mut task = task_record(gid(1), 0);
        store.put_task(&task).expect("task");
        let intent = JournalInstallIntent {
            gid: task.gid,
            checkpoint_id: CheckpointId::new([21; 16]).expect("checkpoint"),
            old_journal_id: task.primary_journal_id,
            old_path: task.primary_journal_path.clone(),
            new_journal_id: journal(22),
            new_path: path(b"/journal/candidate"),
            source_last_sequence: 9,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        store.begin_journal_install(&intent).expect("begin install");
        task.primary_journal_id = journal(23);
        task.primary_journal_path = path(b"/journal/other-authority");
        task.updated_ms = 350;
        store.put_task(&task).expect("change pointer");
        assert!(matches!(
            store.complete_journal_install(gid(1), 400),
            Err(SessionStoreError::JournalPointerMismatch)
        ));
        assert_eq!(
            store.tasks().expect("tasks")[0].primary_journal_id,
            task.primary_journal_id
        );
        assert_eq!(
            store.journal_installs().expect("intent")[0].phase,
            JournalInstallPhase::Installing
        );
    }

    #[test]
    fn hot_backup_is_complete_refuses_overwrite_and_passes_integrity() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        let backup = directory.path().join("session.backup.db");
        store.backup_to(&backup).expect("backup");
        assert!(matches!(
            store.backup_to(&backup),
            Err(SessionStoreError::BackupPathExists)
        ));
        let backup_store = SessionStore::open(
            &backup,
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("open backup");
        assert_eq!(backup_store.tasks().expect("backup tasks").len(), 1);
    }

    #[test]
    fn vocabularies_and_schema_objects_are_closed_and_unique() {
        assert_eq!(
            ALL_SESSION_STORE_ERROR_CODES
                .into_iter()
                .collect::<HashSet<_>>()
                .len(),
            ALL_SESSION_STORE_ERROR_CODES.len()
        );
        assert_eq!(
            ALL_SESSION_IO_OPERATIONS
                .into_iter()
                .map(super::SessionIoOperation::code)
                .collect::<HashSet<_>>()
                .len(),
            ALL_SESSION_IO_OPERATIONS.len()
        );
        assert_eq!(
            ALL_SESSION_SQLITE_LIMITS
                .into_iter()
                .map(super::SessionSqliteLimit::code)
                .collect::<HashSet<_>>()
                .len(),
            ALL_SESSION_SQLITE_LIMITS.len()
        );
        assert_eq!(
            SESSION_SCHEMA_OBJECTS
                .iter()
                .map(|object| (object.kind.code(), object.name))
                .collect::<HashSet<_>>()
                .len(),
            SESSION_SCHEMA_OBJECTS.len()
        );
        assert_eq!(
            ariax_core::ALL_ERROR_KINDS
                .last()
                .expect("closed error vocabulary")
                .number(),
            29
        );
    }
}
