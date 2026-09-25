use crate::{
    CheckpointId, JournalDirectoryCapability, JournalHash, JournalId, MAX_OPTION_MAP_BYTES,
    MAX_OPTION_MAP_ENTRIES, MAX_PLATFORM_PATH_BYTES, NativeCapabilityError, OptionsSnapshotScope,
    PathPlatform, PersistedOptionPolicy, PlatformPath, SanitizedOptionMap,
};
use ariax_core::{ErrorKind, Gid, HostKeyChallengeId, HostKeyFingerprint};
use fs2::FileExt as _;
use rusqlite::limits::Limit;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[path = "session_bt.rs"]
mod bt;
pub use bt::{
    SessionBtBinding, SessionBtCheckpoint, SessionBtFile, SessionBtResumeRecord,
    SessionBtTaskRecord,
};

pub const SESSION_SCHEMA_VERSION: u32 = 3;
pub const SESSION_RUSQLITE_VERSION: &str = "0.40.2";
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
pub const SESSION_MAX_HOST_KEY_BYTES: usize = 16 * 1024;
pub const SESSION_MAX_ALGORITHM_BYTES: usize = 64;
pub const SESSION_MAX_TASKS: usize = 100_000;
pub const SESSION_MAX_IMPORT_TASKS: usize = 1_000;
pub const SESSION_IMPORT_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const SESSION_MAX_OPTIONS_PER_TASK: usize = MAX_OPTION_MAP_ENTRIES;
pub const SESSION_MAX_SOURCES_PER_TASK: usize = 4_096;
pub const SESSION_SOURCE_READ_BUDGET_BYTES: usize = 4 * 1024 * 1024;
pub const SESSION_HOST_KEY_PIN_OPTION: &str = "sftp-host-key-sha256";
pub const SESSION_TASK_READ_BUDGET_BYTES: usize = 64 * 1024 * 1024;
pub const SESSION_INSTALL_READ_BUDGET_BYTES: usize = 16 * 1024 * 1024;
pub const SESSION_OWNER_LOCK_SUFFIX: &str = ".ariax-owner-lock";

/// One member of an atomic current-format session import.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionAdmissionMetadata {
    Transfer(SessionTaskMetadata),
    BitTorrent {
        task: SessionBtTaskRecord,
        options: SanitizedOptionMap,
        resume: std::sync::Arc<[u8]>,
    },
}

impl SessionAdmissionMetadata {
    pub fn gid(&self) -> Gid {
        match self {
            Self::Transfer(entry) => entry.task.gid,
            Self::BitTorrent { task, .. } => task.gid,
        }
    }
    pub fn owned_bytes(&self) -> usize {
        match self {
            Self::Transfer(entry) => entry.owned_bytes(),
            Self::BitTorrent {
                task,
                options,
                resume,
            } => task
                .binding
                .owned_bytes()
                .saturating_add(resume.len())
                .saturating_add(task.root_display.bytes().len())
                .saturating_add(
                    options
                        .entries()
                        .map(|(name, value)| {
                            name.len().saturating_add(value.len()).saturating_add(128)
                        })
                        .sum::<usize>(),
                )
                .saturating_add(1024),
        }
    }
}

/// Complete, secret-free metadata for one atomic import member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTaskMetadata {
    pub task: SessionTaskRecord,
    pub sources: Vec<SessionTaskSourceRecord>,
    pub options: SanitizedOptionMap,
}

impl SessionTaskMetadata {
    #[must_use]
    pub fn owned_bytes(&self) -> usize {
        let paths = self
            .task
            .primary_journal_path
            .bytes()
            .len()
            .saturating_add(
                self.task
                    .replica_journal_path
                    .as_ref()
                    .map_or(0, |path| path.bytes().len()),
            )
            .saturating_add(self.task.root_display.bytes().len())
            .saturating_add(
                self.task
                    .no_space
                    .as_ref()
                    .map_or(0, |condition| condition.target.bytes().len()),
            );
        let options = self
            .options
            .canonical_bytes()
            .saturating_add(self.options.entries().len().saturating_mul(128));
        self.sources.iter().fold(
            std::mem::size_of::<Self>()
                .saturating_add(paths)
                .saturating_add(options)
                .saturating_add(
                    self.sources
                        .capacity()
                        .saturating_mul(std::mem::size_of::<SessionTaskSourceRecord>()),
                ),
            |total, source| {
                total.saturating_add(
                    source
                        .persistence_safe_uri
                        .as_ref()
                        .map_or(0, String::capacity),
                )
            },
        )
    }
}

const PLATFORM_PATH_ENCODING_OVERHEAD: usize = 5;
const MAX_ENCODED_PLATFORM_PATH_BYTES: usize =
    MAX_PLATFORM_PATH_BYTES + PLATFORM_PATH_ENCODING_OVERHEAD;
static BACKUP_TEMP_ID: AtomicU64 = AtomicU64::new(1);
const MAX_BACKUP_PUBLICATION_CANDIDATES: usize = 64;
const BACKUP_TEMP_MARKER: &str = ".ariax-backup-";
const BACKUP_TEMP_SUFFIX: &str = ".tmp";

#[cfg(test)]
thread_local! {
    static BACKUP_FAIL_NEXT_UNLINK: Cell<bool> = const { Cell::new(false) };
    static IMPORT_CRASH_POINT: Cell<Option<usize>> = const { Cell::new(None) };
}

const SESSION_TABLE_SQL: &str = r#"CREATE TABLE session (
    session_id BLOB PRIMARY KEY NOT NULL CHECK(typeof(session_id) = 'blob' AND length(session_id) = 16),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms),
    clean_shutdown INTEGER NOT NULL CHECK(clean_shutdown IN (0, 1))
) STRICT"#;

const TASK_TABLE_SQL: &str = r#"CREATE TABLE task (
    gid TEXT PRIMARY KEY NOT NULL CHECK(length(gid) = 16 AND gid NOT GLOB '*[^0-9a-f]*'),
    session_id BLOB NOT NULL CHECK(typeof(session_id) = 'blob' AND length(session_id) = 16),
    task_kind INTEGER NOT NULL DEFAULT 1 CHECK(task_kind IN (1, 2)),
    queue_state INTEGER NOT NULL CHECK(queue_state IN (1, 2, 3, 4, 5)),
    queue_position INTEGER NOT NULL CHECK(queue_position BETWEEN 0 AND 4294967295),
    desired_paused INTEGER NOT NULL CHECK(desired_paused IN (0, 1)),
    slow_demotion_count INTEGER NOT NULL CHECK(slow_demotion_count BETWEEN 0 AND 4294967295),
    slow_original_position INTEGER CHECK(slow_original_position IS NULL OR slow_original_position BETWEEN 0 AND 99999),
    slow_retry_scheduled_at_ms INTEGER CHECK(slow_retry_scheduled_at_ms IS NULL OR slow_retry_scheduled_at_ms >= 0),
    slow_retry_delay_ms BLOB CHECK(slow_retry_delay_ms IS NULL OR (typeof(slow_retry_delay_ms) = 'blob' AND length(slow_retry_delay_ms) = 8 AND slow_retry_delay_ms != X'0000000000000000')),
    primary_journal_id BLOB CHECK(primary_journal_id IS NULL OR (typeof(primary_journal_id) = 'blob' AND length(primary_journal_id) = 16)),
    primary_journal_path BLOB CHECK(primary_journal_path IS NULL OR (typeof(primary_journal_path) = 'blob' AND length(primary_journal_path) BETWEEN 6 AND 65541)),
    replica_journal_path BLOB CHECK(replica_journal_path IS NULL OR (typeof(replica_journal_path) = 'blob' AND length(replica_journal_path) BETWEEN 6 AND 65541)),
    replica_sequence BLOB CHECK(replica_sequence IS NULL OR (typeof(replica_sequence) = 'blob' AND length(replica_sequence) = 8)),
    root_display BLOB NOT NULL CHECK(typeof(root_display) = 'blob' AND length(root_display) BETWEEN 6 AND 65541),
    cached_layout_hash BLOB CHECK(cached_layout_hash IS NULL OR (typeof(cached_layout_hash) = 'blob' AND length(cached_layout_hash) = 32)),
    cached_root_binding_hash BLOB CHECK(cached_root_binding_hash IS NULL OR (typeof(cached_root_binding_hash) = 'blob' AND length(cached_root_binding_hash) = 32)),
    cached_snapshot_hash BLOB CHECK(cached_snapshot_hash IS NULL OR (typeof(cached_snapshot_hash) = 'blob' AND length(cached_snapshot_hash) = 32)),
    no_space_target BLOB CHECK(no_space_target IS NULL OR (typeof(no_space_target) = 'blob' AND length(no_space_target) BETWEEN 6 AND 65541)),
    no_space_scheduled_at_ms INTEGER CHECK(no_space_scheduled_at_ms IS NULL OR no_space_scheduled_at_ms >= 0),
    no_space_delay_ms BLOB CHECK(no_space_delay_ms IS NULL OR (typeof(no_space_delay_ms) = 'blob' AND length(no_space_delay_ms) = 8)),
    created_ms INTEGER NOT NULL CHECK(created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK(updated_ms >= created_ms),
    CHECK((task_kind = 1 AND primary_journal_id IS NOT NULL AND primary_journal_path IS NOT NULL AND cached_snapshot_hash IS NOT NULL)
       OR (task_kind = 2 AND primary_journal_id IS NULL AND primary_journal_path IS NULL AND replica_journal_path IS NULL
           AND replica_sequence IS NULL AND cached_snapshot_hash IS NULL AND cached_layout_hash IS NULL AND cached_root_binding_hash IS NULL
           AND no_space_target IS NULL AND slow_demotion_count = 0 AND queue_state != 5)),
    CHECK((replica_journal_path IS NULL) = (replica_sequence IS NULL)),
    CHECK((no_space_target IS NULL) = (no_space_scheduled_at_ms IS NULL) AND (no_space_target IS NULL) = (no_space_delay_ms IS NULL)),
    CHECK((slow_retry_scheduled_at_ms IS NULL) = (slow_retry_delay_ms IS NULL)),
    CHECK((queue_state = 5) = (slow_original_position IS NOT NULL)),
    CHECK(queue_state != 5 OR slow_demotion_count > 0),
    CHECK(queue_state != 5 OR desired_paused = 0),
    CHECK(slow_retry_scheduled_at_ms IS NULL OR slow_original_position IS NOT NULL),
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
    algorithm TEXT NOT NULL CHECK(length(CAST(algorithm AS BLOB)) BETWEEN 1 AND 64),
    presented_public_key BLOB NOT NULL CHECK(typeof(presented_public_key) = 'blob' AND length(presented_public_key) BETWEEN 1 AND 16384),
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
    request BLOB NOT NULL CHECK(typeof(request) = 'blob' AND length(request) = 8),
    generation BLOB NOT NULL CHECK(typeof(generation) = 'blob' AND length(generation) = 8),
    saved_ms INTEGER NOT NULL CHECK(saved_ms >= 0),
    FOREIGN KEY(gid) REFERENCES bt_metadata(gid) ON UPDATE CASCADE ON DELETE CASCADE
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
        name: "bt_metadata",
        sql: bt::BT_METADATA_TABLE_SQL,
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
    Demoted = 5,
}

impl SessionQueueState {
    pub const ALL: [Self; 5] = [
        Self::Waiting,
        Self::Active,
        Self::Paused,
        Self::Stopped,
        Self::Demoted,
    ];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Demoted => "demoted",
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
            5 => Ok(Self::Demoted),
            _ => Err(SessionStoreError::InvalidPersistedValue("queue_state")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(i64)]
pub enum SessionTerminalStatus {
    Error = 1,
    Complete = 2,
    Removed = 3,
}

impl SessionTerminalStatus {
    pub const ALL: [Self; 3] = [Self::Error, Self::Complete, Self::Removed];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Complete => "complete",
            Self::Removed => "removed",
        }
    }
}

impl TryFrom<i64> for SessionTerminalStatus {
    type Error = SessionStoreError;

    fn try_from(value: i64) -> Result<Self, SessionStoreError> {
        match value {
            1 => Ok(Self::Error),
            2 => Ok(Self::Complete),
            3 => Ok(Self::Removed),
            _ => Err(SessionStoreError::InvalidPersistedValue(
                "stopped_result.terminal_status",
            )),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionSlowRetryDecision {
    pub scheduled_at_ms: u64,
    pub delay_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionSlowSlotState {
    pub original_position: u32,
    pub retry: Option<SessionSlowRetryDecision>,
}

/// One complete scheduler-supplied final queue order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionQueueOrder {
    pub state: SessionQueueState,
    pub gids: Vec<Gid>,
}

/// One exact queue mutation whose supplied orders are verified before commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionQueueTransition {
    pub gid: Gid,
    pub expected_state: SessionQueueState,
    pub target_state: SessionQueueState,
    pub desired_paused: bool,
    pub slow_demotion_count: u32,
    pub slow_slot: Option<SessionSlowSlotState>,
    pub final_orders: Vec<SessionQueueOrder>,
    pub updated_ms: u64,
}

/// One persistence-safe or redacted source row owned by a task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTaskSourceRecord {
    pub uri_id: u32,
    pub persistence_safe_uri: Option<String>,
    pub redacted_fingerprint: [u8; 32],
    pub needs_credentials: bool,
    pub priority: i64,
}

/// One exact bounded source set materialized for a startup task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTaskSourceSet {
    pub gid: Gid,
    pub sources: Vec<SessionTaskSourceRecord>,
}

/// One exact, bounded SFTP host-key challenge retained for restart recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionHostKeyChallengeRecord {
    pub gid: Gid,
    pub challenge_id: HostKeyChallengeId,
    pub canonical_host: String,
    pub port: u16,
    pub algorithm: String,
    pub presented_public_key: Vec<u8>,
    pub fingerprint_sha256: HostKeyFingerprint,
    pub created_ms: u64,
}

/// One challenge-bound, atomic option-snapshot replacement and challenge clear.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionHostKeyResolution {
    pub gid: Gid,
    pub challenge_id: HostKeyChallengeId,
    pub fingerprint_sha256: HostKeyFingerprint,
    pub presented_public_key: Vec<u8>,
    pub scope: OptionsSnapshotScope,
    pub pinned_options: SanitizedOptionMap,
}

/// Canonical lowercase hexadecimal value stored for an approved SHA-256 pin.
#[must_use]
pub fn session_host_key_pin_value(fingerprint: HostKeyFingerprint) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in fingerprint.as_bytes() {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStoppedResultRecord {
    pub gid: Gid,
    pub status: SessionTerminalStatus,
    pub error_kind: Option<ErrorKind>,
    pub safe_message: String,
    pub total_length: Option<u64>,
    pub layout_hash: Option<JournalHash>,
    pub completed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTaskRecord {
    pub gid: Gid,
    pub session_id: SessionId,
    pub queue_state: SessionQueueState,
    pub queue_position: u32,
    pub desired_paused: bool,
    pub slow_demotion_count: u32,
    pub slow_slot: Option<SessionSlowSlotState>,
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

/// Identity token that prevents stale install completion or retirement commands.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JournalInstallToken {
    pub gid: Gid,
    pub checkpoint_id: CheckpointId,
    pub new_journal_id: JournalId,
}

impl JournalInstallIntent {
    #[must_use]
    pub const fn token(&self) -> JournalInstallToken {
        JournalInstallToken {
            gid: self.gid,
            checkpoint_id: self.checkpoint_id,
            new_journal_id: self.new_journal_id,
        }
    }
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
    AcquireOwnerLock,
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
            Self::AcquireOwnerLock => "acquire_owner_lock",
            Self::TightenPermissions => "tighten_permissions",
            Self::CreateBackup => "create_backup",
            Self::RemoveFailedBackup => "remove_failed_backup",
        }
    }
}

pub const ALL_SESSION_IO_OPERATIONS: [SessionIoOperation; 7] = [
    SessionIoOperation::InspectPath,
    SessionIoOperation::CreateDirectory,
    SessionIoOperation::CreateDatabase,
    SessionIoOperation::AcquireOwnerLock,
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
    UnsupportedSchema {
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
    QueueTransitionRequired,
    OwnerLockBusy,
    NotFound,
    HostKeyChallengeMismatch,
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
            Self::UnsupportedSchema { .. } => "unsupported_schema",
            Self::UnversionedDatabase => "unversioned_database",
            Self::SchemaMismatch(_) => "schema_mismatch",
            Self::IntegrityCheckFailed => "integrity_check_failed",
            Self::LimitRejected { .. } => "limit_rejected",
            Self::InvalidPersistedValue(_) => "invalid_persisted_value",
            Self::InvalidRecord(_) => "invalid_record",
            Self::SessionConflict => "session_conflict",
            Self::QueueInvariant => "queue_invariant",
            Self::QueueTransitionRequired => "queue_transition_required",
            Self::OwnerLockBusy => "owner_lock_busy",
            Self::NotFound => "not_found",
            Self::HostKeyChallengeMismatch => "host_key_challenge_mismatch",
            Self::ForbiddenPersistedOption => "forbidden_persisted_option",
            Self::JournalPointerMismatch => "journal_pointer_mismatch",
            Self::JournalInstallConflict => "journal_install_conflict",
            Self::BackupPathExists => "backup_path_exists",
            Self::WalCheckpointBusy => "wal_checkpoint_busy",
        }
    }
}

pub const ALL_SESSION_STORE_ERROR_CODES: [&str; 21] = [
    "sqlite",
    "io",
    "invalid_config",
    "unsupported_schema",
    "unversioned_database",
    "schema_mismatch",
    "integrity_check_failed",
    "limit_rejected",
    "invalid_persisted_value",
    "invalid_record",
    "session_conflict",
    "queue_invariant",
    "queue_transition_required",
    "owner_lock_busy",
    "not_found",
    "host_key_challenge_mismatch",
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
            Self::UnsupportedSchema { found, supported } => write!(
                formatter,
                "unsupported session format {found}; a fresh schema {supported} store is required"
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
            Self::QueueTransitionRequired => {
                formatter.write_str("task queue changes require an atomic queue transition")
            }
            Self::OwnerLockBusy => formatter.write_str("another process owns the session database"),
            Self::NotFound => formatter.write_str("session record was not found"),
            Self::HostKeyChallengeMismatch => {
                formatter.write_str("host-key challenge identity or key material does not match")
            }
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

struct SessionOwnerLock {
    file: File,
}

impl Drop for SessionOwnerLock {
    fn drop(&mut self) {
        // `flock` locks follow duplicated open-file descriptions on Unix. A
        // concurrently spawned child can briefly inherit such a descriptor,
        // so closing only this handle can retain the lock past store teardown.
        // Explicit unlock also gives Windows a deterministic release point.
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

pub struct SessionStore {
    path: PathBuf,
    connection: Connection,
    journal_mode: SessionJournalMode,
    config: SessionStoreConfig,
    _owner_lock: SessionOwnerLock,
}

impl SessionStore {
    pub fn open(
        path: impl AsRef<Path>,
        config: SessionStoreConfig,
    ) -> Result<Self, SessionStoreError> {
        validate_config(config)?;
        let path = path.as_ref().to_path_buf();
        validate_persistence_file_name(&path)?;
        prepare_private_directory(required_private_parent(&path)?)?;
        let path = canonicalize_database_path(path)?;
        let existed_before_lock = validate_database_artifacts(&path)?;
        if existed_before_lock {
            let version = inspect_persisted_user_version(&path)?;
            if version != 0 && version != SESSION_SCHEMA_VERSION {
                return Err(SessionStoreError::UnsupportedSchema {
                    found: version,
                    supported: SESSION_SCHEMA_VERSION,
                });
            }
        }
        let owner_lock = acquire_session_owner_lock(&path)?;
        let existed = prepare_database_path(&path)?;
        if existed {
            let version = inspect_persisted_user_version(&path)?;
            if version != 0 && version != SESSION_SCHEMA_VERSION {
                return Err(SessionStoreError::UnsupportedSchema {
                    found: version,
                    supported: SESSION_SCHEMA_VERSION,
                });
            }
            tighten_sqlite_artifact_permissions(&path)?;
        }
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut connection = Connection::open_with_flags(&path, flags)?;
        apply_limits(&connection)?;
        connection.busy_timeout(Duration::from_millis(config.busy_timeout_ms))?;
        let version = read_user_version(&connection)?;
        if version != 0 && version != SESSION_SCHEMA_VERSION {
            return Err(SessionStoreError::UnsupportedSchema {
                found: version,
                supported: SESSION_SCHEMA_VERSION,
            });
        }
        if version == 0 && count_schema_objects(&connection)? != 0 {
            return Err(SessionStoreError::UnversionedDatabase);
        }
        match version {
            0 => connection.pragma_update(None, "page_size", SESSION_PAGE_SIZE_BYTES)?,
            SESSION_SCHEMA_VERSION => {
                validate_schema(&connection)?;
                validate_integrity(&connection)?;
                validate_persisted_semantics(&connection)?;
            }
            _ => return Err(SessionStoreError::SchemaMismatch("user_version")),
        }
        let journal_mode = configure_pragmas(&connection, config)?;
        match version {
            0 => create_schema(&mut connection)?,
            SESSION_SCHEMA_VERSION => {}
            _ => return Err(SessionStoreError::SchemaMismatch("user_version")),
        }
        validate_schema(&connection)?;
        validate_integrity(&connection)?;
        validate_persisted_semantics(&connection)?;
        tighten_sqlite_artifact_permissions(&path)?;
        Ok(Self {
            path,
            connection,
            journal_mode,
            config,
            _owner_lock: owner_lock,
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .prepare("SELECT session_id FROM session ORDER BY session_id LIMIT 2")?
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if existing.len() > 1
            || existing
                .first()
                .is_some_and(|value| value.as_slice() != record.session_id.as_bytes())
        {
            return Err(SessionStoreError::SessionConflict);
        }
        transaction.execute(
            "INSERT INTO session(session_id, created_ms, updated_ms, clean_shutdown) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(session_id) DO UPDATE SET created_ms = excluded.created_ms, updated_ms = excluded.updated_ms, clean_shutdown = excluded.clean_shutdown",
            params![
                record.session_id.as_bytes().as_slice(),
                time_to_i64(record.created_ms, "session.created_ms")?,
                time_to_i64(record.updated_ms, "session.updated_ms")?,
                bool_to_i64(record.clean_shutdown),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn session(&self) -> Result<Option<SessionRecord>, SessionStoreError> {
        let rows = self
            .connection
            .prepare(
                "SELECT session_id, created_ms, updated_ms, clean_shutdown FROM session ORDER BY session_id LIMIT 2",
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
        if task.queue_state == SessionQueueState::Stopped {
            return Err(SessionStoreError::QueueTransitionRequired);
        }
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
        let slow_original_position = task
            .slow_slot
            .as_ref()
            .map(|value| i64::from(value.original_position));
        let slow_demotion_count = i64::from(task.slow_demotion_count);
        let slow_retry_scheduled = task
            .slow_slot
            .as_ref()
            .and_then(|value| value.retry.as_ref())
            .map(|value| time_to_i64(value.scheduled_at_ms, "task.slow_retry_scheduled_at_ms"))
            .transpose()?;
        let slow_retry_delay = task
            .slow_slot
            .as_ref()
            .and_then(|value| value.retry.as_ref())
            .map(|value| encode_u64(value.delay_ms));
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT queue_state, queue_position, slow_original_position, slow_demotion_count, slow_retry_scheduled_at_ms, slow_retry_delay_ms, primary_journal_id, primary_journal_path FROM task WHERE gid = ?1",
                [task.gid.to_string()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<Vec<u8>>>(5)?,
                        row.get::<_, Vec<u8>>(6)?,
                        row.get::<_, Vec<u8>>(7)?,
                    ))
                },
            )
            .optional()?;
        if let Some((
            queue_state,
            queue_position,
            existing_slow_original,
            existing_slow_count,
            existing_slow_scheduled,
            existing_slow_delay,
            journal_id,
            journal_path,
        )) = existing
        {
            if queue_state != task.queue_state as i64
                || queue_position != i64::from(task.queue_position)
                || existing_slow_original != slow_original_position
                || existing_slow_count != slow_demotion_count
                || existing_slow_scheduled != slow_retry_scheduled
                || existing_slow_delay != slow_retry_delay
            {
                return Err(SessionStoreError::QueueTransitionRequired);
            }
            if journal_id != task.primary_journal_id.as_bytes() || journal_path != primary_path {
                return Err(SessionStoreError::JournalPointerMismatch);
            }
        }
        transaction.execute(
            "INSERT INTO task(gid, session_id, queue_state, queue_position, desired_paused, slow_original_position, slow_demotion_count, slow_retry_scheduled_at_ms, slow_retry_delay_ms, primary_journal_id, primary_journal_path, replica_journal_path, replica_sequence, root_display, cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash, no_space_target, no_space_scheduled_at_ms, no_space_delay_ms, created_ms, updated_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22) ON CONFLICT(gid) DO UPDATE SET session_id = excluded.session_id, desired_paused = excluded.desired_paused, replica_journal_path = excluded.replica_journal_path, replica_sequence = excluded.replica_sequence, root_display = excluded.root_display, cached_layout_hash = excluded.cached_layout_hash, cached_root_binding_hash = excluded.cached_root_binding_hash, cached_snapshot_hash = excluded.cached_snapshot_hash, no_space_target = excluded.no_space_target, no_space_scheduled_at_ms = excluded.no_space_scheduled_at_ms, no_space_delay_ms = excluded.no_space_delay_ms, created_ms = excluded.created_ms, updated_ms = excluded.updated_ms",
            params![
                task.gid.to_string(),
                task.session_id.as_bytes().as_slice(),
                task.queue_state as i64,
                i64::from(task.queue_position),
                bool_to_i64(task.desired_paused),
                slow_original_position,
                slow_demotion_count,
                slow_retry_scheduled,
                slow_retry_delay,
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
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically creates one task and its complete non-secret protocol metadata.
    ///
    /// Existing task rows are rejected rather than updated so a GID collision
    /// cannot replace another task's sources or option snapshot.
    pub fn create_task_with_metadata(
        &mut self,
        task: &SessionTaskRecord,
        sources: &[SessionTaskSourceRecord],
        options: &SanitizedOptionMap,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        validate_admission_metadata(task, sources, options, policy)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_admission_metadata(&transaction, task, sources, options)?;
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    /// Commits both protocols together before any imported task is published.
    pub fn create_session_batch(
        &mut self,
        members: &[SessionAdmissionMetadata],
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        if members.is_empty() || members.len() > SESSION_MAX_IMPORT_TASKS {
            return Err(SessionStoreError::InvalidRecord("import.task_count"));
        }
        let mut gids = HashSet::new();
        let mut bytes = 0usize;
        for member in members {
            bytes = bytes.saturating_add(member.owned_bytes());
            if bytes > SESSION_IMPORT_MAX_BYTES || !gids.insert(member.gid()) {
                return Err(SessionStoreError::InvalidRecord("import.batch"));
            }
            match member {
                SessionAdmissionMetadata::Transfer(entry) => validate_admission_metadata(
                    &entry.task,
                    &entry.sources,
                    &entry.options,
                    policy,
                )?,
                SessionAdmissionMetadata::BitTorrent {
                    task,
                    options,
                    resume,
                } => {
                    bt::validate_admission(task, options, policy)?;
                    if !resume.is_empty() {
                        ariax_bt_metadata::validate_resume(resume, &task.binding.identity)
                            .map_err(|_| SessionStoreError::InvalidRecord("bt.resume"))?;
                    }
                }
            }
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: i64 =
            transaction.query_row("SELECT COUNT(*) FROM task", [], |row| row.get(0))?;
        if bounded_count(existing, SESSION_MAX_TASKS, "task.count")?.saturating_add(members.len())
            > SESSION_MAX_TASKS
        {
            return Err(SessionStoreError::InvalidRecord("import.task_limit"));
        }
        for (index, member) in members.iter().enumerate() {
            match member {
                SessionAdmissionMetadata::Transfer(entry) => insert_admission_metadata(
                    &transaction,
                    &entry.task,
                    &entry.sources,
                    &entry.options,
                )?,
                SessionAdmissionMetadata::BitTorrent {
                    task,
                    options,
                    resume,
                } => {
                    bt::insert_admission(&transaction, task, options)?;
                    transaction.execute(
                        "UPDATE bt_resume SET resume_blob=?2 WHERE gid=?1",
                        params![task.gid.to_string(), resume.as_ref()],
                    )?;
                }
            }
            #[cfg(test)]
            import_crash_checkpoint(index);
            #[cfg(not(test))]
            let _ = index;
        }
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        #[cfg(test)]
        import_crash_checkpoint(usize::MAX);
        Ok(())
    }

    /// Installs a fully validated import atomically, including source and option rows.
    pub fn create_task_batch(
        &mut self,
        tasks: &[SessionTaskMetadata],
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        self.create_task_batch_following(tasks, None, policy)
    }

    pub fn create_task_batch_following(
        &mut self,
        tasks: &[SessionTaskMetadata],
        parent: Option<crate::MetalinkParent>,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        if tasks.is_empty() || tasks.len() > SESSION_MAX_IMPORT_TASKS {
            return Err(SessionStoreError::InvalidRecord("import.task_count"));
        }
        let mut bytes = 0_usize;
        let mut gids = HashSet::new();
        for entry in tasks {
            bytes = bytes.saturating_add(entry.owned_bytes());
            if bytes > SESSION_IMPORT_MAX_BYTES {
                return Err(SessionStoreError::InvalidRecord("import.byte_budget"));
            }
            if !gids.insert(entry.task.gid) {
                return Err(SessionStoreError::InvalidRecord("import.duplicate_gid"));
            }
            validate_admission_metadata(&entry.task, &entry.sources, &entry.options, policy)?;
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: i64 =
            transaction.query_row("SELECT COUNT(*) FROM task", [], |row| row.get(0))?;
        if bounded_count(existing, SESSION_MAX_TASKS, "task.count")?.saturating_add(tasks.len())
            > SESSION_MAX_TASKS
        {
            return Err(SessionStoreError::InvalidRecord("import.task_limit"));
        }
        if let Some(parent) = parent {
            let options = read_task_options(
                &transaction,
                parent.gid,
                OptionsSnapshotScope::CurrentGeneration,
                policy,
            )?;
            let queue: i64 = transaction.query_row(
                "SELECT queue_state FROM task WHERE gid = ?1",
                [parent.gid.to_string()],
                |row| row.get(0),
            )?;
            if queue != SessionQueueState::Active as i64
                || options.snapshot_hash() != parent.snapshot_hash
                || options
                    .entries()
                    .any(|(name, _)| name == crate::METALINK_EXPANSION_OPTION)
            {
                return Err(SessionStoreError::InvalidRecord("metalink.parent_changed"));
            }
            let expansion = crate::MetalinkExpansion {
                parent,
                children: tasks.iter().map(|entry| entry.task.gid).collect(),
            };
            if !expansion.validate() {
                return Err(SessionStoreError::InvalidRecord("metalink.expansion"));
            }
            let options = expansion
                .with_options(&options)
                .map_err(|_| SessionStoreError::InvalidRecord("metalink.expansion"))?;
            validate_options_for_persistence(&options, policy)?;
            replace_task_options_in_transaction(
                &transaction,
                parent.gid,
                OptionsSnapshotScope::CurrentGeneration,
                &options,
            )?;
        }
        for (index, entry) in tasks.iter().enumerate() {
            insert_admission_metadata(&transaction, &entry.task, &entry.sources, &entry.options)?;
            #[cfg(test)]
            import_crash_checkpoint(index);
            #[cfg(not(test))]
            let _ = index;
        }
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        #[cfg(test)]
        import_crash_checkpoint(usize::MAX);
        Ok(())
    }

    /// A later scheduler member may acknowledge only the exact committed import row.
    pub fn confirm_task_metadata(
        &self,
        expected: &SessionTaskMetadata,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        validate_admission_metadata(&expected.task, &expected.sources, &expected.options, policy)?;
        let raw = self.connection.query_row(
            "SELECT gid, session_id, queue_state, queue_position, desired_paused, slow_original_position, slow_demotion_count, slow_retry_scheduled_at_ms, slow_retry_delay_ms, primary_journal_id, primary_journal_path, replica_journal_path, replica_sequence, root_display, cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash, no_space_target, no_space_scheduled_at_ms, no_space_delay_ms, created_ms, updated_ms FROM task WHERE gid = ?1",
            [expected.task.gid.to_string()], RawTaskRow::from_row,
        ).optional()?.ok_or(SessionStoreError::NotFound)?;
        if raw.decode()? != expected.task
            || self.task_sources(expected.task.gid)? != expected.sources
            || self.task_options(
                expected.task.gid,
                OptionsSnapshotScope::CurrentGeneration,
                policy,
            )? != expected.options
        {
            return Err(SessionStoreError::InvalidRecord("import.metadata_mismatch"));
        }
        Ok(())
    }

    pub fn tasks(&self) -> Result<Vec<SessionTaskRecord>, SessionStoreError> {
        validate_dense_queues(&self.connection)?;
        validate_stopped_result_pairing(&self.connection)?;
        read_task_records(&self.connection)
    }

    pub fn stopped_results(&self) -> Result<Vec<SessionStoppedResultRecord>, SessionStoreError> {
        validate_dense_queues(&self.connection)?;
        validate_stopped_result_pairing(&self.connection)?;
        read_stopped_results(&self.connection)
    }

    pub fn queue_order(&self, state: SessionQueueState) -> Result<Vec<Gid>, SessionStoreError> {
        read_queue_order(&self.connection, state)
    }

    pub fn set_task_no_space_condition(
        &mut self,
        gid: Gid,
        condition: Option<&SessionNoSpaceCondition>,
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        let (target, scheduled_at_ms, delay_ms) = match condition {
            Some(condition) => {
                if condition.delay_ms == 0 {
                    return Err(SessionStoreError::InvalidRecord("no_space.delay_ms"));
                }
                (
                    Some(encode_platform_path(&condition.target)?),
                    Some(time_to_i64(
                        condition.scheduled_at_ms,
                        "task.no_space_scheduled_at_ms",
                    )?),
                    Some(encode_u64(condition.delay_ms)),
                )
            }
            None => (None, None, None),
        };
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let queue_state = transaction
            .query_row(
                "SELECT queue_state FROM task WHERE gid = ?1",
                [gid.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        let queue_state = SessionQueueState::try_from(queue_state)?;
        if condition.is_some() && queue_state == SessionQueueState::Stopped {
            return Err(SessionStoreError::InvalidRecord("no_space.queue_state"));
        }
        if transaction.execute(
            "UPDATE task SET no_space_target = ?1, no_space_scheduled_at_ms = ?2, no_space_delay_ms = ?3, updated_ms = ?4 WHERE gid = ?5",
            params![
                target,
                scheduled_at_ms,
                delay_ms,
                time_to_i64(updated_ms, "task.updated_ms")?,
                gid.to_string(),
            ],
        )? != 1
        {
            return Err(SessionStoreError::NotFound);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn reorder_queue(
        &mut self,
        state: SessionQueueState,
        ordered_gids: &[Gid],
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        if ordered_gids.len() > SESSION_MAX_TASKS {
            return Err(SessionStoreError::QueueInvariant);
        }
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
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one atomic queue transition must bind membership, order, pause intent, slow metadata, and timestamp"
    )]
    pub fn transition_task_queue(
        &mut self,
        gid: Gid,
        expected_state: SessionQueueState,
        target_state: SessionQueueState,
        target_position: u32,
        desired_paused: bool,
        slow_demotion_count: u32,
        slow_slot: Option<&SessionSlowSlotState>,
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        if expected_state == SessionQueueState::Stopped
            || target_state == SessionQueueState::Stopped
        {
            return Err(SessionStoreError::QueueTransitionRequired);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transition_task_queue_in_transaction(
            &transaction,
            gid,
            expected_state,
            target_state,
            target_position,
            desired_paused,
            slow_demotion_count,
            slow_slot,
            updated_ms,
        )?;
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn transition_task_queue_exact(
        &mut self,
        transition: &SessionQueueTransition,
    ) -> Result<(), SessionStoreError> {
        if transition.expected_state == SessionQueueState::Stopped
            || transition.target_state == SessionQueueState::Stopped
        {
            return Err(SessionStoreError::QueueTransitionRequired);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        apply_exact_queue_transition_in_transaction(&transaction, transition)?;
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn persist_stopped_result(
        &mut self,
        result: &SessionStoppedResultRecord,
        expected_state: SessionQueueState,
        target_position: u32,
        desired_paused: bool,
        slow_demotion_count: u32,
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        if expected_state == SessionQueueState::Stopped {
            return Err(SessionStoreError::QueueTransitionRequired);
        }
        validate_stopped_result(result)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM host_key_challenge WHERE gid = ?1",
            [result.gid.to_string()],
        )?;
        transition_task_queue_in_transaction(
            &transaction,
            result.gid,
            expected_state,
            SessionQueueState::Stopped,
            target_position,
            desired_paused,
            slow_demotion_count,
            None,
            updated_ms,
        )?;
        let error_code = result
            .error_kind
            .map_or(0_i64, |kind| i64::from(kind.number()));
        let total_length = result.total_length.map(encode_u64);
        let layout_hash = result.layout_hash.map(|value| value.as_bytes().to_vec());
        transaction.execute(
            "INSERT INTO stopped_result(gid, terminal_status, error_code, safe_message, total_length, layout_hash, completed_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                result.gid.to_string(),
                result.status as i64,
                error_code,
                result.safe_message,
                total_length,
                layout_hash,
                time_to_i64(result.completed_ms, "stopped_result.completed_ms")?,
            ],
        )?;
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn persist_stopped_result_exact(
        &mut self,
        result: &SessionStoppedResultRecord,
        transition: &SessionQueueTransition,
    ) -> Result<(), SessionStoreError> {
        if result.gid != transition.gid
            || transition.expected_state == SessionQueueState::Stopped
            || transition.target_state != SessionQueueState::Stopped
            || transition.slow_slot.is_some()
        {
            return Err(SessionStoreError::QueueTransitionRequired);
        }
        validate_stopped_result(result)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM host_key_challenge WHERE gid = ?1",
            [result.gid.to_string()],
        )?;
        apply_exact_queue_transition_in_transaction(&transaction, transition)?;
        let error_code = result
            .error_kind
            .map_or(0_i64, |kind| i64::from(kind.number()));
        transaction.execute(
            "INSERT INTO stopped_result(gid, terminal_status, error_code, safe_message, total_length, layout_hash, completed_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                result.gid.to_string(),
                result.status as i64,
                error_code,
                result.safe_message,
                result.total_length.map(encode_u64),
                result.layout_hash.map(|value| value.as_bytes().to_vec()),
                time_to_i64(result.completed_ms, "stopped_result.completed_ms")?,
            ],
        )?;
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn delete_stopped_task_metadata(
        &mut self,
        gid: Gid,
        remaining_order: &[Gid],
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        if remaining_order.len() >= SESSION_MAX_TASKS
            || remaining_order.contains(&gid)
            || remaining_order
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != remaining_order.len()
        {
            return Err(SessionStoreError::QueueInvariant);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_order = read_queue_order(&transaction, SessionQueueState::Stopped)?;
        let Some(position) = current_order.iter().position(|candidate| *candidate == gid) else {
            return Err(SessionStoreError::NotFound);
        };
        let mut expected_remaining = current_order;
        expected_remaining.remove(position);
        if expected_remaining != remaining_order {
            return Err(SessionStoreError::QueueInvariant);
        }
        if transaction.execute(
            "DELETE FROM stopped_result WHERE gid = ?1",
            [gid.to_string()],
        )? != 1
        {
            return Err(SessionStoreError::NotFound);
        }
        if transaction.execute(
            "DELETE FROM task WHERE gid = ?1 AND queue_state = ?2",
            params![gid.to_string(), SessionQueueState::Stopped as i64],
        )? != 1
        {
            return Err(SessionStoreError::QueueInvariant);
        }
        let updated_ms = time_to_i64(updated_ms, "task.updated_ms")?;
        {
            let mut statement = transaction.prepare(
                "UPDATE task SET queue_position = ?1, updated_ms = ?2 WHERE gid = ?3 AND queue_state = ?4",
            )?;
            for (position, remaining_gid) in remaining_order.iter().copied().enumerate() {
                let position =
                    i64::try_from(position).map_err(|_| SessionStoreError::QueueInvariant)?;
                if statement.execute(params![
                    position,
                    updated_ms,
                    remaining_gid.to_string(),
                    SessionQueueState::Stopped as i64,
                ])? != 1
                {
                    return Err(SessionStoreError::QueueInvariant);
                }
            }
        }
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
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
        validate_options_for_persistence(options, policy)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        replace_task_options_in_transaction(&transaction, gid, scope, options)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn promote_task_options(
        &mut self,
        gid: Gid,
        options: &SanitizedOptionMap,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        validate_options_for_persistence(options, policy)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !task_exists(&transaction, gid)? {
            return Err(SessionStoreError::NotFound);
        }
        let staged = read_task_options(
            &transaction,
            gid,
            OptionsSnapshotScope::NextAdmission,
            policy,
        )?;
        if staged.entries().len() == 0 || &staged != options {
            return Err(SessionStoreError::InvalidRecord(
                "task_option.promotion_mismatch",
            ));
        }
        replace_task_options_in_transaction(
            &transaction,
            gid,
            OptionsSnapshotScope::CurrentGeneration,
            options,
        )?;
        transaction.execute(
            "DELETE FROM task_option WHERE gid = ?1 AND scope = ?2",
            params![
                gid.to_string(),
                OptionsSnapshotScope::NextAdmission.number()
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn replace_task_sources(
        &mut self,
        gid: Gid,
        sources: &[SessionTaskSourceRecord],
    ) -> Result<(), SessionStoreError> {
        validate_task_sources_for_write(sources)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        replace_task_sources_in_transaction(&transaction, gid, sources)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn replace_task_sources_and_queue(
        &mut self,
        transition: &SessionQueueTransition,
        sources: &[SessionTaskSourceRecord],
    ) -> Result<(), SessionStoreError> {
        validate_task_sources_for_write(sources)?;
        if sources.is_empty()
            || transition.expected_state == SessionQueueState::Stopped
            || transition.target_state == SessionQueueState::Stopped
        {
            return Err(SessionStoreError::QueueTransitionRequired);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        apply_exact_queue_transition_in_transaction(&transaction, transition)?;
        replace_task_sources_in_transaction(&transaction, transition.gid, sources)?;
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn task_sources(
        &self,
        gid: Gid,
    ) -> Result<Vec<SessionTaskSourceRecord>, SessionStoreError> {
        if !task_exists(&self.connection, gid)? {
            return Err(SessionStoreError::NotFound);
        }
        read_task_sources(&self.connection, gid)
    }

    pub fn task_source_sets(&self) -> Result<Vec<SessionTaskSourceSet>, SessionStoreError> {
        read_task_source_sets_with_budget(&self.connection, SESSION_TASK_READ_BUDGET_BYTES)
    }

    pub fn put_host_key_challenge(
        &mut self,
        challenge: &SessionHostKeyChallengeRecord,
    ) -> Result<(), SessionStoreError> {
        validate_host_key_challenge_record(challenge)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_paused_task(&transaction, challenge.gid)?;
        transaction.execute(
            "INSERT INTO host_key_challenge(gid, challenge_id, canonical_host, port, algorithm, presented_public_key, fingerprint_sha256, created_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(gid) DO UPDATE SET challenge_id = excluded.challenge_id, canonical_host = excluded.canonical_host, port = excluded.port, algorithm = excluded.algorithm, presented_public_key = excluded.presented_public_key, fingerprint_sha256 = excluded.fingerprint_sha256, created_ms = excluded.created_ms",
            params![
                challenge.gid.to_string(),
                challenge.challenge_id.as_bytes().as_slice(),
                challenge.canonical_host,
                i64::from(challenge.port),
                challenge.algorithm,
                challenge.presented_public_key,
                challenge.fingerprint_sha256.as_bytes().as_slice(),
                time_to_i64(challenge.created_ms, "host_key_challenge.created_ms")?,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn host_key_challenge(
        &self,
        gid: Gid,
    ) -> Result<Option<SessionHostKeyChallengeRecord>, SessionStoreError> {
        read_host_key_challenge(&self.connection, gid)
    }

    pub fn host_key_challenges(
        &self,
    ) -> Result<Vec<SessionHostKeyChallengeRecord>, SessionStoreError> {
        read_host_key_challenge_records(&self.connection, SESSION_TASK_READ_BUDGET_BYTES)
    }

    pub fn reject_host_key_challenge(
        &mut self,
        gid: Gid,
        challenge_id: HostKeyChallengeId,
    ) -> Result<(), SessionStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            read_host_key_challenge(&transaction, gid)?.ok_or(SessionStoreError::NotFound)?;
        if current.challenge_id != challenge_id {
            return Err(SessionStoreError::HostKeyChallengeMismatch);
        }
        if transaction.execute(
            "DELETE FROM host_key_challenge WHERE gid = ?1 AND challenge_id = ?2",
            params![gid.to_string(), challenge_id.as_bytes().as_slice()],
        )? != 1
        {
            return Err(SessionStoreError::HostKeyChallengeMismatch);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn resolve_host_key_challenge(
        &mut self,
        resolution: &SessionHostKeyResolution,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        validate_options_for_persistence(&resolution.pinned_options, policy)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_paused_task(&transaction, resolution.gid)?;
        let current = read_host_key_challenge(&transaction, resolution.gid)?
            .ok_or(SessionStoreError::NotFound)?;
        if current.challenge_id != resolution.challenge_id
            || current.fingerprint_sha256 != resolution.fingerprint_sha256
            || current.presented_public_key != resolution.presented_public_key
            || HostKeyFingerprint::for_presented_key(&resolution.presented_public_key)
                != resolution.fingerprint_sha256
        {
            return Err(SessionStoreError::HostKeyChallengeMismatch);
        }
        let expected_pin = session_host_key_pin_value(resolution.fingerprint_sha256);
        if !resolution
            .pinned_options
            .entries()
            .any(|(key, value)| key == SESSION_HOST_KEY_PIN_OPTION && value == expected_pin)
        {
            return Err(SessionStoreError::InvalidRecord("host_key_pin.option"));
        }
        replace_task_options_in_transaction(
            &transaction,
            resolution.gid,
            resolution.scope,
            &resolution.pinned_options,
        )?;
        if transaction.execute(
            "DELETE FROM host_key_challenge WHERE gid = ?1 AND challenge_id = ?2 AND fingerprint_sha256 = ?3",
            params![
                resolution.gid.to_string(),
                resolution.challenge_id.as_bytes().as_slice(),
                resolution.fingerprint_sha256.as_bytes().as_slice(),
            ],
        )? != 1
        {
            return Err(SessionStoreError::HostKeyChallengeMismatch);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn task_options(
        &self,
        gid: Gid,
        scope: OptionsSnapshotScope,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<SanitizedOptionMap, SessionStoreError> {
        read_task_options(&self.connection, gid, scope, policy)
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

    pub fn reconcile_journal_authority(
        &mut self,
        gid: Gid,
        expected_journal_id: JournalId,
        cache: Option<SessionJournalCache>,
        root_display: Option<&PlatformPath>,
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        if cache.is_none() && root_display.is_none() {
            return Err(SessionStoreError::InvalidRecord("journal_authority.empty"));
        }
        if cache
            .is_some_and(|cache| cache.layout_hash.is_some() != cache.root_binding_hash.is_some())
        {
            return Err(SessionStoreError::InvalidRecord(
                "layout_and_root_hash_presence",
            ));
        }
        let encoded_root = root_display.map(encode_platform_path).transpose()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT primary_journal_id, root_display, cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash FROM task WHERE gid = ?1",
                [gid.to_string()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        if decode_journal_id(&existing.0, "task.primary_journal_id")? != expected_journal_id {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
        let _existing_root = decode_platform_path(&existing.1, "task.root_display")?;
        let existing_cache = SessionJournalCache {
            layout_hash: existing
                .2
                .as_deref()
                .map(|value| decode_hash(value, "task.cached_layout_hash"))
                .transpose()?,
            root_binding_hash: existing
                .3
                .as_deref()
                .map(|value| decode_hash(value, "task.cached_root_binding_hash"))
                .transpose()?,
            snapshot_hash: decode_hash(&existing.4, "task.cached_snapshot_hash")?,
        };
        let desired_cache = cache.unwrap_or(existing_cache);
        let changed = transaction.execute(
            "UPDATE task SET root_display = ?1, cached_layout_hash = ?2, cached_root_binding_hash = ?3, cached_snapshot_hash = ?4, updated_ms = ?5 WHERE gid = ?6 AND primary_journal_id = ?7",
            params![
                encoded_root.unwrap_or_else(|| existing.1.clone()),
                desired_cache.layout_hash.map(|value| value.as_bytes().to_vec()),
                desired_cache
                    .root_binding_hash
                    .map(|value| value.as_bytes().to_vec()),
                desired_cache.snapshot_hash.as_bytes().as_slice(),
                time_to_i64(updated_ms, "task.updated_ms")?,
                gid.to_string(),
                expected_journal_id.as_bytes().as_slice(),
            ],
        )?;
        if changed != 1 {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn begin_journal_install(
        &mut self,
        intent: &JournalInstallIntent,
    ) -> Result<JournalInstallToken, SessionStoreError> {
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
        Ok(intent.token())
    }

    pub fn complete_journal_install(
        &mut self,
        token: JournalInstallToken,
        updated_ms: u64,
    ) -> Result<(), SessionStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let install = read_journal_install_for_gid(&transaction, token.gid)?;
        if install.phase != JournalInstallPhase::Installing {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        validate_install_intent(&install)?;
        if install.token() != token {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        let current = transaction
            .query_row(
                "SELECT primary_journal_id, primary_journal_path FROM task WHERE gid = ?1",
                [token.gid.to_string()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        if decode_journal_id(&current.0, "task.primary_journal_id")? != install.old_journal_id
            || decode_platform_path(&current.1, "task.primary_journal_path")? != install.old_path
        {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
        let changed = transaction.execute(
            "UPDATE task SET primary_journal_id = ?1, primary_journal_path = ?2, updated_ms = ?3 WHERE gid = ?4",
            params![
                install.new_journal_id.as_bytes().as_slice(),
                encode_platform_path(&install.new_path)?,
                time_to_i64(updated_ms, "task.updated_ms")?,
                token.gid.to_string(),
            ],
        )?;
        if changed != 1 {
            return Err(SessionStoreError::NotFound);
        }
        let phase_changed = transaction.execute(
            "UPDATE journal_install SET phase = ?1 WHERE gid = ?2 AND checkpoint_id = ?3 AND new_journal_id = ?4",
            params![
                JournalInstallPhase::Installed as i64,
                token.gid.to_string(),
                token.checkpoint_id.as_bytes().as_slice(),
                token.new_journal_id.as_bytes().as_slice(),
            ],
        )?;
        if phase_changed != 1 {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        transaction.commit()?;
        Ok(())
    }

    /// Aborts one exact installing intent while preserving the retained old
    /// journal pointer as authority.
    pub fn abort_journal_install(
        &mut self,
        token: JournalInstallToken,
    ) -> Result<(), SessionStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let install = read_journal_install_for_gid(&transaction, token.gid)?;
        if install.phase != JournalInstallPhase::Installing || install.token() != token {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        validate_install_intent(&install)?;
        let current = transaction
            .query_row(
                "SELECT primary_journal_id, primary_journal_path FROM task WHERE gid = ?1",
                [token.gid.to_string()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        if decode_journal_id(&current.0, "task.primary_journal_id")? != install.old_journal_id
            || decode_platform_path(&current.1, "task.primary_journal_path")? != install.old_path
        {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
        let changed = transaction.execute(
            "DELETE FROM journal_install WHERE gid = ?1 AND checkpoint_id = ?2 AND new_journal_id = ?3 AND phase = ?4",
            params![
                token.gid.to_string(),
                token.checkpoint_id.as_bytes().as_slice(),
                token.new_journal_id.as_bytes().as_slice(),
                JournalInstallPhase::Installing as i64,
            ],
        )?;
        if changed != 1 {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn clear_installed_journal(
        &mut self,
        token: JournalInstallToken,
    ) -> Result<(), SessionStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let install = read_journal_install_for_gid(&transaction, token.gid)?;
        if install.phase != JournalInstallPhase::Installed || install.token() != token {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        validate_install_values(&install)?;
        let current = transaction
            .query_row(
                "SELECT primary_journal_id, primary_journal_path FROM task WHERE gid = ?1",
                [token.gid.to_string()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        if decode_journal_id(&current.0, "task.primary_journal_id")? != install.new_journal_id
            || decode_platform_path(&current.1, "task.primary_journal_path")? != install.new_path
        {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
        let changed = transaction.execute(
            "DELETE FROM journal_install WHERE gid = ?1 AND checkpoint_id = ?2 AND new_journal_id = ?3 AND phase = ?4",
            params![
                token.gid.to_string(),
                token.checkpoint_id.as_bytes().as_slice(),
                token.new_journal_id.as_bytes().as_slice(),
                JournalInstallPhase::Installed as i64,
            ],
        )?;
        if changed != 1 {
            return Err(SessionStoreError::JournalInstallConflict);
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn journal_installs(&self) -> Result<Vec<JournalInstallIntent>, SessionStoreError> {
        read_journal_installs(&self.connection)
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
        backup_connection_to(
            &self.connection,
            destination.as_ref(),
            SessionBackupSchema::Current,
        )
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the shared transaction primitive binds queue membership, pause intent, slow metadata, and timestamp"
)]
fn transition_task_queue_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    gid: Gid,
    expected_state: SessionQueueState,
    target_state: SessionQueueState,
    target_position: u32,
    desired_paused: bool,
    slow_demotion_count: u32,
    slow_slot: Option<&SessionSlowSlotState>,
    updated_ms: u64,
) -> Result<(), SessionStoreError> {
    validate_slow_slot_state(target_state, desired_paused, slow_demotion_count, slow_slot)?;
    let slow_original_position = slow_slot.map(|value| i64::from(value.original_position));
    let slow_demotion_count = i64::from(slow_demotion_count);
    let slow_retry_scheduled = slow_slot
        .and_then(|value| value.retry.as_ref())
        .map(|value| time_to_i64(value.scheduled_at_ms, "task.slow_retry_scheduled_at_ms"))
        .transpose()?;
    let slow_retry_delay = slow_slot
        .and_then(|value| value.retry.as_ref())
        .map(|value| encode_u64(value.delay_ms));
    let (current_state, current_position): (i64, i64) = transaction
        .query_row(
            "SELECT queue_state, queue_position FROM task WHERE gid = ?1",
            [gid.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(SessionStoreError::NotFound)?;
    if SessionQueueState::try_from(current_state)? != expected_state {
        return Err(SessionStoreError::QueueTransitionRequired);
    }
    reject_retained_host_key_departure(transaction, gid, expected_state, target_state)?;
    let target_len: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM task WHERE queue_state = ?1",
        [target_state as i64],
        |row| row.get(0),
    )?;
    let target_position = i64::from(target_position);
    let maximum = if expected_state == target_state {
        target_len.saturating_sub(1)
    } else {
        target_len
    };
    if target_position < 0 || target_position > maximum {
        return Err(SessionStoreError::QueueInvariant);
    }
    let updated_ms = time_to_i64(updated_ms, "task.updated_ms")?;
    if expected_state == target_state {
        if target_position < current_position {
            transaction.execute(
                "UPDATE task SET queue_position = queue_position + 1, updated_ms = ?1 WHERE queue_state = ?2 AND queue_position >= ?3 AND queue_position < ?4",
                params![updated_ms, target_state as i64, target_position, current_position],
            )?;
        } else if target_position > current_position {
            transaction.execute(
                "UPDATE task SET queue_position = queue_position - 1, updated_ms = ?1 WHERE queue_state = ?2 AND queue_position > ?3 AND queue_position <= ?4",
                params![updated_ms, target_state as i64, current_position, target_position],
            )?;
        }
    } else {
        transaction.execute(
            "UPDATE task SET queue_position = queue_position - 1, updated_ms = ?1 WHERE queue_state = ?2 AND queue_position > ?3",
            params![updated_ms, expected_state as i64, current_position],
        )?;
        transaction.execute(
            "UPDATE task SET queue_position = queue_position + 1, updated_ms = ?1 WHERE queue_state = ?2 AND queue_position >= ?3",
            params![updated_ms, target_state as i64, target_position],
        )?;
    }
    if transaction.execute(
        "UPDATE task SET queue_state = ?1, queue_position = ?2, desired_paused = ?3, slow_original_position = ?4, slow_demotion_count = ?5, slow_retry_scheduled_at_ms = ?6, slow_retry_delay_ms = ?7, updated_ms = ?8 WHERE gid = ?9 AND queue_state = ?10 AND queue_position = ?11",
        params![
            target_state as i64,
            target_position,
            bool_to_i64(desired_paused),
            slow_original_position,
            slow_demotion_count,
            slow_retry_scheduled,
            slow_retry_delay,
            updated_ms,
            gid.to_string(),
            expected_state as i64,
            current_position,
        ],
    )? != 1
    {
        return Err(SessionStoreError::QueueInvariant);
    }
    Ok(())
}

fn apply_exact_queue_transition_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    transition: &SessionQueueTransition,
) -> Result<(), SessionStoreError> {
    validate_slow_slot_state(
        transition.target_state,
        transition.desired_paused,
        transition.slow_demotion_count,
        transition.slow_slot.as_ref(),
    )?;
    let expected_states = [transition.expected_state, transition.target_state]
        .into_iter()
        .collect::<HashSet<_>>();
    let supplied_states = transition
        .final_orders
        .iter()
        .map(|order| order.state)
        .collect::<HashSet<_>>();
    if transition.final_orders.is_empty()
        || supplied_states.len() != transition.final_orders.len()
        || supplied_states != expected_states
    {
        return Err(SessionStoreError::QueueInvariant);
    }

    let current_state = transaction
        .query_row(
            "SELECT queue_state FROM task WHERE gid = ?1",
            [transition.gid.to_string()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or(SessionStoreError::NotFound)?;
    if SessionQueueState::try_from(current_state)? != transition.expected_state {
        return Err(SessionStoreError::QueueTransitionRequired);
    }
    reject_retained_host_key_departure(
        transaction,
        transition.gid,
        transition.expected_state,
        transition.target_state,
    )?;

    let mut supplied_membership = HashSet::new();
    for order in &transition.final_orders {
        if order.gids.len() > SESSION_MAX_TASKS
            || order.gids.iter().copied().collect::<HashSet<_>>().len() != order.gids.len()
            || !order
                .gids
                .iter()
                .copied()
                .all(|gid| supplied_membership.insert(gid))
        {
            return Err(SessionStoreError::QueueInvariant);
        }
        let mut expected_membership = read_queue_order(transaction, order.state)?
            .into_iter()
            .collect::<HashSet<_>>();
        if transition.expected_state != transition.target_state {
            if order.state == transition.expected_state
                && !expected_membership.remove(&transition.gid)
            {
                return Err(SessionStoreError::QueueInvariant);
            }
            if order.state == transition.target_state && !expected_membership.insert(transition.gid)
            {
                return Err(SessionStoreError::QueueInvariant);
            }
        }
        if expected_membership != order.gids.iter().copied().collect::<HashSet<_>>() {
            return Err(SessionStoreError::QueueInvariant);
        }
    }
    let target_order = transition
        .final_orders
        .iter()
        .find(|order| order.state == transition.target_state)
        .ok_or(SessionStoreError::QueueInvariant)?;
    if !target_order.gids.contains(&transition.gid) {
        return Err(SessionStoreError::QueueInvariant);
    }
    if transition.expected_state != transition.target_state
        && transition
            .final_orders
            .iter()
            .find(|order| order.state == transition.expected_state)
            .is_some_and(|order| order.gids.contains(&transition.gid))
    {
        return Err(SessionStoreError::QueueInvariant);
    }

    let slow_original_position = transition
        .slow_slot
        .as_ref()
        .map(|value| i64::from(value.original_position));
    let slow_retry_scheduled = transition
        .slow_slot
        .as_ref()
        .and_then(|value| value.retry.as_ref())
        .map(|value| time_to_i64(value.scheduled_at_ms, "task.slow_retry_scheduled_at_ms"))
        .transpose()?;
    let slow_retry_delay = transition
        .slow_slot
        .as_ref()
        .and_then(|value| value.retry.as_ref())
        .map(|value| encode_u64(value.delay_ms));
    let updated_ms = time_to_i64(transition.updated_ms, "task.updated_ms")?;
    let temporary_base =
        i64::try_from(SESSION_MAX_TASKS + 1).map_err(|_| SessionStoreError::QueueInvariant)?;
    if transaction.execute(
        "UPDATE task SET queue_state = ?1, queue_position = ?2, desired_paused = ?3, slow_original_position = ?4, slow_demotion_count = ?5, slow_retry_scheduled_at_ms = ?6, slow_retry_delay_ms = ?7, updated_ms = ?8 WHERE gid = ?9 AND queue_state = ?10",
        params![
            transition.target_state as i64,
            temporary_base,
            bool_to_i64(transition.desired_paused),
            slow_original_position,
            i64::from(transition.slow_demotion_count),
            slow_retry_scheduled,
            slow_retry_delay,
            updated_ms,
            transition.gid.to_string(),
            transition.expected_state as i64,
        ],
    )? != 1
    {
        return Err(SessionStoreError::QueueTransitionRequired);
    }

    for (order_index, order) in transition.final_orders.iter().enumerate() {
        let order_index =
            i64::try_from(order_index).map_err(|_| SessionStoreError::QueueInvariant)?;
        let order_base = temporary_base
            .checked_add(
                order_index
                    .checked_mul(
                        i64::try_from(SESSION_MAX_TASKS + 1)
                            .map_err(|_| SessionStoreError::QueueInvariant)?,
                    )
                    .ok_or(SessionStoreError::QueueInvariant)?,
            )
            .ok_or(SessionStoreError::QueueInvariant)?;
        let mut temporary = transaction.prepare(
            "UPDATE task SET queue_position = ?1, updated_ms = ?2 WHERE gid = ?3 AND queue_state = ?4",
        )?;
        for (position, gid) in order.gids.iter().copied().enumerate() {
            let temporary_position = order_base
                .checked_add(
                    i64::try_from(position).map_err(|_| SessionStoreError::QueueInvariant)?,
                )
                .ok_or(SessionStoreError::QueueInvariant)?;
            if temporary.execute(params![
                temporary_position,
                updated_ms,
                gid.to_string(),
                order.state as i64,
            ])? != 1
            {
                return Err(SessionStoreError::QueueInvariant);
            }
        }
    }
    for order in &transition.final_orders {
        let mut final_position = transaction
            .prepare("UPDATE task SET queue_position = ?1 WHERE gid = ?2 AND queue_state = ?3")?;
        for (position, gid) in order.gids.iter().copied().enumerate() {
            if final_position.execute(params![
                i64::try_from(position).map_err(|_| SessionStoreError::QueueInvariant)?,
                gid.to_string(),
                order.state as i64,
            ])? != 1
            {
                return Err(SessionStoreError::QueueInvariant);
            }
        }
        if read_queue_order(transaction, order.state)? != order.gids {
            return Err(SessionStoreError::QueueInvariant);
        }
    }
    Ok(())
}

fn read_queue_order(
    connection: &Connection,
    state: SessionQueueState,
) -> Result<Vec<Gid>, SessionStoreError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task WHERE queue_state = ?1",
        [state as i64],
        |row| row.get(0),
    )?;
    let count = bounded_count(count, SESSION_MAX_TASKS, "task.queue_count")?;
    let mut statement = connection
        .prepare("SELECT gid FROM task WHERE queue_state = ?1 ORDER BY queue_position, gid")?;
    let mut rows = statement.query([state as i64])?;
    let mut order = Vec::new();
    order
        .try_reserve_exact(count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("task.queue_allocation"))?;
    while let Some(row) = rows.next()? {
        order.push(decode_gid(&row.get::<_, String>(0)?)?);
        if order.len() > count {
            return Err(SessionStoreError::InvalidPersistedValue("task.queue_count"));
        }
    }
    if order.len() != count {
        return Err(SessionStoreError::InvalidPersistedValue("task.queue_count"));
    }
    Ok(order)
}

#[cfg(test)]
fn import_crash_checkpoint(point: usize) {
    if IMPORT_CRASH_POINT.with(|value| value.get() == Some(point)) {
        std::process::exit(77);
    }
}

fn validate_admission_metadata(
    task: &SessionTaskRecord,
    sources: &[SessionTaskSourceRecord],
    options: &SanitizedOptionMap,
    policy: &impl PersistedOptionPolicy,
) -> Result<(), SessionStoreError> {
    if task.queue_state == SessionQueueState::Stopped {
        return Err(SessionStoreError::QueueTransitionRequired);
    }
    if sources.is_empty() {
        return Err(SessionStoreError::InvalidRecord("task_source.empty"));
    }
    validate_task(task)?;
    validate_task_sources_for_write(sources)?;
    validate_options_for_persistence(options, policy)?;
    if task.cached_snapshot_hash != options.snapshot_hash() {
        return Err(SessionStoreError::InvalidRecord(
            "task.option_snapshot_hash",
        ));
    }
    Ok(())
}

fn insert_admission_metadata(
    transaction: &rusqlite::Transaction<'_>,
    task: &SessionTaskRecord,
    sources: &[SessionTaskSourceRecord],
    options: &SanitizedOptionMap,
) -> Result<(), SessionStoreError> {
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
    let slow_original_position = task
        .slow_slot
        .as_ref()
        .map(|value| i64::from(value.original_position));
    let slow_demotion_count = i64::from(task.slow_demotion_count);
    let slow_retry_scheduled = task
        .slow_slot
        .as_ref()
        .and_then(|value| value.retry.as_ref())
        .map(|value| time_to_i64(value.scheduled_at_ms, "task.slow_retry_scheduled_at_ms"))
        .transpose()?;
    let slow_retry_delay = task
        .slow_slot
        .as_ref()
        .and_then(|value| value.retry.as_ref())
        .map(|value| encode_u64(value.delay_ms));

    if task_exists(transaction, task.gid)? {
        return Err(SessionStoreError::InvalidRecord("task.gid_exists"));
    }
    let queue_len: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM task WHERE queue_state = ?1",
        [task.queue_state as i64],
        |row| row.get(0),
    )?;
    if i64::from(task.queue_position) > queue_len {
        return Err(SessionStoreError::InvalidRecord("task.queue_position"));
    }
    transaction.execute("UPDATE task SET queue_position = queue_position + 1 WHERE queue_state = ?1 AND queue_position >= ?2", params![task.queue_state as i64, i64::from(task.queue_position)])?;
    transaction.execute(
        "INSERT INTO task(gid, session_id, queue_state, queue_position, desired_paused, slow_original_position, slow_demotion_count, slow_retry_scheduled_at_ms, slow_retry_delay_ms, primary_journal_id, primary_journal_path, replica_journal_path, replica_sequence, root_display, cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash, no_space_target, no_space_scheduled_at_ms, no_space_delay_ms, created_ms, updated_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
        params![
            task.gid.to_string(),
            task.session_id.as_bytes().as_slice(),
            task.queue_state as i64,
            i64::from(task.queue_position),
            bool_to_i64(task.desired_paused),
            slow_original_position,
            slow_demotion_count,
            slow_retry_scheduled,
            slow_retry_delay,
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
    {
        let mut statement = transaction.prepare(
            "INSERT INTO task_source(gid, uri_id, persistence_safe_uri, redacted_fingerprint, needs_credentials, priority) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        for source in sources {
            statement.execute(params![
                task.gid.to_string(),
                i64::from(source.uri_id),
                source.persistence_safe_uri,
                source.redacted_fingerprint.as_slice(),
                bool_to_i64(source.needs_credentials),
                source.priority,
            ])?;
        }
    }
    replace_task_options_in_transaction(
        transaction,
        task.gid,
        OptionsSnapshotScope::CurrentGeneration,
        options,
    )?;
    Ok(())
}

fn task_exists(connection: &Connection, gid: Gid) -> Result<bool, SessionStoreError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task WHERE gid = ?1",
        [gid.to_string()],
        |row| row.get(0),
    )?;
    match count {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(SessionStoreError::InvalidPersistedValue("task.gid")),
    }
}

fn validate_options_for_persistence(
    options: &SanitizedOptionMap,
    policy: &impl PersistedOptionPolicy,
) -> Result<(), SessionStoreError> {
    if options.entries().len() > SESSION_MAX_OPTIONS_PER_TASK {
        return Err(SessionStoreError::InvalidRecord("task_option.count"));
    }
    if options.entries().any(|(key, _)| !policy.permits(key)) {
        return Err(SessionStoreError::ForbiddenPersistedOption);
    }
    Ok(())
}

fn read_task_options(
    connection: &Connection,
    gid: Gid,
    scope: OptionsSnapshotScope,
    policy: &impl PersistedOptionPolicy,
) -> Result<SanitizedOptionMap, SessionStoreError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task_option WHERE gid = ?1 AND scope = ?2",
        params![gid.to_string(), scope.number()],
        |row| row.get(0),
    )?;
    let count = bounded_count(count, SESSION_MAX_OPTIONS_PER_TASK, "task_option.count")?;
    let mut statement = connection.prepare(
        "SELECT key, canonical_value FROM task_option WHERE gid = ?1 AND scope = ?2 ORDER BY key",
    )?;
    let mut rows = statement.query(params![gid.to_string(), scope.number()])?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("task_option.allocation"))?;
    let mut canonical_bytes = 4_usize;
    while let Some(row) = rows.next()? {
        let key = row.get::<_, String>(0)?;
        let value = row.get::<_, Vec<u8>>(1)?;
        if !policy.permits(&key) {
            return Err(SessionStoreError::ForbiddenPersistedOption);
        }
        canonical_bytes = canonical_bytes
            .checked_add(8)
            .and_then(|total| total.checked_add(key.len()))
            .and_then(|total| total.checked_add(value.len()))
            .ok_or(SessionStoreError::InvalidPersistedValue(
                "task_option.bytes",
            ))?;
        if canonical_bytes > MAX_OPTION_MAP_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_option.bytes",
            ));
        }
        let value = String::from_utf8(value)
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_option.value"))?;
        decoded.push((key, value));
        if decoded.len() > count {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_option.count",
            ));
        }
    }
    if decoded.len() != count {
        return Err(SessionStoreError::InvalidPersistedValue(
            "task_option.count",
        ));
    }
    SanitizedOptionMap::new(decoded)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("task_option"))
}

fn replace_task_options_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    gid: Gid,
    scope: OptionsSnapshotScope,
    options: &SanitizedOptionMap,
) -> Result<(), SessionStoreError> {
    if !task_exists(transaction, gid)? {
        return Err(SessionStoreError::NotFound);
    }
    transaction.execute(
        "DELETE FROM task_option WHERE gid = ?1 AND scope = ?2",
        params![gid.to_string(), scope.number()],
    )?;
    let mut statement = transaction.prepare(
        "INSERT INTO task_option(gid, scope, key, canonical_value) VALUES (?1, ?2, ?3, ?4)",
    )?;
    for (key, value) in options.entries() {
        statement.execute(params![
            gid.to_string(),
            scope.number(),
            key,
            value.as_bytes()
        ])?;
    }
    Ok(())
}

fn replace_task_sources_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    gid: Gid,
    sources: &[SessionTaskSourceRecord],
) -> Result<(), SessionStoreError> {
    if !task_exists(transaction, gid)? {
        return Err(SessionStoreError::NotFound);
    }
    transaction.execute("DELETE FROM task_source WHERE gid = ?1", [gid.to_string()])?;
    let mut statement = transaction.prepare(
        "INSERT INTO task_source(gid, uri_id, persistence_safe_uri, redacted_fingerprint, needs_credentials, priority) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    for source in sources {
        statement.execute(params![
            gid.to_string(),
            i64::from(source.uri_id),
            source.persistence_safe_uri,
            source.redacted_fingerprint.as_slice(),
            bool_to_i64(source.needs_credentials),
            source.priority
        ])?;
    }
    Ok(())
}

/// Query fields and userinfo have no non-secret registry classification yet.
/// This is an allocation-free secrecy check, not a protocol URI validator.
#[must_use]
pub fn uri_is_safe_to_persist(uri: &str) -> bool {
    if uri.is_empty() || uri.contains(['?', '#']) || uri.bytes().any(|byte| byte.is_ascii_control())
    {
        return false;
    }
    !uri.split_once("://").is_some_and(|(_, rest)| {
        rest.split('/')
            .next()
            .is_some_and(|authority| authority.contains('@'))
    })
}

fn validate_task_sources_for_write(
    sources: &[SessionTaskSourceRecord],
) -> Result<(), SessionStoreError> {
    if sources.len() > SESSION_MAX_SOURCES_PER_TASK
        || sources
            .iter()
            .map(|source| source.uri_id)
            .collect::<HashSet<_>>()
            .len()
            != sources.len()
    {
        return Err(SessionStoreError::InvalidRecord("task_source.count"));
    }
    let mut bytes = 0_usize;
    for source in sources {
        if source.persistence_safe_uri.is_none() && !source.needs_credentials {
            return Err(SessionStoreError::InvalidRecord("task_source.credentials"));
        }
        if source.persistence_safe_uri.as_ref().is_some_and(|uri| {
            uri.len() > SESSION_MAX_SAFE_URI_BYTES || !uri_is_safe_to_persist(uri)
        }) {
            return Err(SessionStoreError::InvalidRecord("task_source.uri"));
        }
        let row_bytes =
            task_source_owned_bytes(source.persistence_safe_uri.as_ref().map_or(0, String::len))
                .ok_or(SessionStoreError::InvalidRecord("task_source.bytes"))?;
        bytes = bytes
            .checked_add(row_bytes)
            .ok_or(SessionStoreError::InvalidRecord("task_source.bytes"))?;
        if bytes > SESSION_SOURCE_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidRecord("task_source.bytes"));
        }
    }
    Ok(())
}

fn read_task_sources(
    connection: &Connection,
    gid: Gid,
) -> Result<Vec<SessionTaskSourceRecord>, SessionStoreError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task_source WHERE gid = ?1",
        [gid.to_string()],
        |row| row.get(0),
    )?;
    let count = bounded_count(count, SESSION_MAX_SOURCES_PER_TASK, "task_source.count")?;
    let mut statement = connection.prepare(
        "SELECT uri_id, CAST(persistence_safe_uri AS BLOB), redacted_fingerprint, needs_credentials, priority FROM task_source WHERE gid = ?1 ORDER BY priority, uri_id",
    )?;
    let mut rows = statement.query([gid.to_string()])?;
    let mut sources = Vec::new();
    sources
        .try_reserve_exact(count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.allocation"))?;
    while let Some(row) = rows.next()? {
        let uri_id = u32::try_from(row.get::<_, i64>(0)?)
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.uri_id"))?;
        let persistence_safe_uri = row
            .get::<_, Option<Vec<u8>>>(1)?
            .map(String::from_utf8)
            .transpose()
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.uri"))?;
        let fingerprint = row.get::<_, Vec<u8>>(2)?;
        let redacted_fingerprint: [u8; 32] = fingerprint
            .try_into()
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.fingerprint"))?;
        sources.push(SessionTaskSourceRecord {
            uri_id,
            persistence_safe_uri,
            redacted_fingerprint,
            needs_credentials: decode_bool(row.get::<_, i64>(3)?, "task_source.needs_credentials")?,
            priority: row.get(4)?,
        });
        if sources.len() > count {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.count",
            ));
        }
    }
    if sources.len() != count {
        return Err(SessionStoreError::InvalidPersistedValue(
            "task_source.count",
        ));
    }
    validate_task_sources_for_write(&sources).map_err(|error| match error {
        SessionStoreError::InvalidRecord("task_source.count") => {
            SessionStoreError::InvalidPersistedValue("task_source.count")
        }
        SessionStoreError::InvalidRecord("task_source.uri") => {
            SessionStoreError::InvalidPersistedValue("task_source.uri")
        }
        SessionStoreError::InvalidRecord("task_source.bytes") => {
            SessionStoreError::InvalidPersistedValue("task_source.bytes")
        }
        SessionStoreError::InvalidRecord("task_source.credentials") => {
            SessionStoreError::InvalidPersistedValue("task_source.credentials")
        }
        other => other,
    })?;
    Ok(sources)
}

fn read_task_source_sets_with_budget(
    connection: &Connection,
    budget: usize,
) -> Result<Vec<SessionTaskSourceSet>, SessionStoreError> {
    let task_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM task WHERE task_kind = 1", [], |row| {
            row.get(0)
        })?;
    let task_count = bounded_count(task_count, SESSION_MAX_TASKS, "task.count")?;
    let mut sets = Vec::new();
    sets.try_reserve_exact(task_count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.allocation"))?;
    let mut statement = connection.prepare(
        "SELECT task.gid, source.uri_id, CAST(source.persistence_safe_uri AS BLOB), source.redacted_fingerprint, source.needs_credentials, source.priority
         FROM task LEFT JOIN task_source AS source ON source.gid = task.gid
         WHERE task.task_kind = 1
         ORDER BY task.gid, source.priority, source.uri_id",
    )?;
    let mut rows = statement.query([])?;
    let mut used_bytes = 0_usize;
    let mut task_bytes = 0_usize;
    let mut current_gid = None;
    while let Some(row) = rows.next()? {
        let gid = decode_gid(&row.get::<_, String>(0)?)?;
        if current_gid != Some(gid) {
            current_gid = Some(gid);
            task_bytes = 0;
            used_bytes = used_bytes
                .checked_add(std::mem::size_of::<SessionTaskSourceSet>())
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "task_source.read_budget",
                ))?;
            if used_bytes > budget {
                return Err(SessionStoreError::InvalidPersistedValue(
                    "task_source.read_budget",
                ));
            }
            sets.push(SessionTaskSourceSet {
                gid,
                sources: Vec::new(),
            });
            if sets.len() > task_count {
                return Err(SessionStoreError::InvalidPersistedValue("task.count"));
            }
        }

        let Some(raw_uri_id) = row.get::<_, Option<i64>>(1)? else {
            if row.get::<_, Option<Vec<u8>>>(2)?.is_some()
                || row.get::<_, Option<Vec<u8>>>(3)?.is_some()
                || row.get::<_, Option<i64>>(4)?.is_some()
                || row.get::<_, Option<i64>>(5)?.is_some()
            {
                return Err(SessionStoreError::InvalidPersistedValue(
                    "task_source.null_row",
                ));
            }
            continue;
        };
        let persistence_safe_uri = row
            .get::<_, Option<Vec<u8>>>(2)?
            .map(String::from_utf8)
            .transpose()
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.uri"))?;
        let fingerprint =
            row.get::<_, Option<Vec<u8>>>(3)?
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "task_source.fingerprint",
                ))?;
        let source = SessionTaskSourceRecord {
            uri_id: u32::try_from(raw_uri_id)
                .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.uri_id"))?,
            persistence_safe_uri,
            redacted_fingerprint: fingerprint
                .try_into()
                .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.fingerprint"))?,
            needs_credentials: decode_bool(
                row.get::<_, Option<i64>>(4)?
                    .ok_or(SessionStoreError::InvalidPersistedValue(
                        "task_source.needs_credentials",
                    ))?,
                "task_source.needs_credentials",
            )?,
            priority: row.get::<_, Option<i64>>(5)?.ok_or(
                SessionStoreError::InvalidPersistedValue("task_source.priority"),
            )?,
        };
        validate_task_sources_for_write(std::slice::from_ref(&source)).map_err(
            |error| match error {
                SessionStoreError::InvalidRecord("task_source.uri") => {
                    SessionStoreError::InvalidPersistedValue("task_source.uri")
                }
                SessionStoreError::InvalidRecord("task_source.bytes") => {
                    SessionStoreError::InvalidPersistedValue("task_source.bytes")
                }
                SessionStoreError::InvalidRecord("task_source.credentials") => {
                    SessionStoreError::InvalidPersistedValue("task_source.credentials")
                }
                other => other,
            },
        )?;
        let row_bytes =
            task_source_owned_bytes(source.persistence_safe_uri.as_ref().map_or(0, String::len))
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "task_source.read_budget",
                ))?;
        task_bytes =
            task_bytes
                .checked_add(row_bytes)
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "task_source.bytes",
                ))?;
        if task_bytes > SESSION_SOURCE_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.bytes",
            ));
        }
        used_bytes =
            used_bytes
                .checked_add(row_bytes)
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "task_source.read_budget",
                ))?;
        if used_bytes > budget {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.read_budget",
            ));
        }
        let sources = &mut sets
            .last_mut()
            .expect("a joined source row always follows its task")
            .sources;
        if sources.len() == SESSION_MAX_SOURCES_PER_TASK {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.count",
            ));
        }
        sources.push(source);
    }
    if sets.len() != task_count {
        return Err(SessionStoreError::InvalidPersistedValue("task.count"));
    }
    Ok(sets)
}

fn validate_task_sources(connection: &Connection) -> Result<(), SessionStoreError> {
    let mut statement = connection.prepare(
        "SELECT gid, uri_id, CAST(persistence_safe_uri AS BLOB), redacted_fingerprint, needs_credentials, priority FROM task_source ORDER BY gid, uri_id",
    )?;
    let mut rows = statement.query([])?;
    let mut current_gid = None;
    let mut current_count = 0_usize;
    let mut current_bytes = 0_usize;
    let mut total_bytes = 0_usize;
    while let Some(row) = rows.next()? {
        let gid = row.get::<_, String>(0)?;
        decode_gid(&gid)?;
        if current_gid.as_deref() == Some(gid.as_str()) {
            current_count =
                current_count
                    .checked_add(1)
                    .ok_or(SessionStoreError::InvalidPersistedValue(
                        "task_source.count",
                    ))?;
        } else {
            current_gid = Some(gid.clone());
            current_count = 1;
            current_bytes = 0;
        }
        if current_count > SESSION_MAX_SOURCES_PER_TASK {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.count",
            ));
        }
        u32::try_from(row.get::<_, i64>(1)?)
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task_source.uri_id"))?;
        let uri = row.get::<_, Option<Vec<u8>>>(2)?;
        if uri.as_ref().is_some_and(|uri| {
            uri.len() > SESSION_MAX_SAFE_URI_BYTES
                || !std::str::from_utf8(uri).is_ok_and(uri_is_safe_to_persist)
        }) {
            return Err(SessionStoreError::InvalidPersistedValue("task_source.uri"));
        }
        let fingerprint = row.get::<_, Vec<u8>>(3)?;
        if fingerprint.len() != 32 {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.fingerprint",
            ));
        }
        let needs_credentials =
            decode_bool(row.get::<_, i64>(4)?, "task_source.needs_credentials")?;
        if uri.is_none() && !needs_credentials {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.credentials",
            ));
        }
        let _priority = row.get::<_, i64>(5)?;
        let row_bytes = task_source_owned_bytes(uri.as_ref().map_or(0, Vec::len)).ok_or(
            SessionStoreError::InvalidPersistedValue("task_source.bytes"),
        )?;
        current_bytes = current_bytes.checked_add(row_bytes).ok_or(
            SessionStoreError::InvalidPersistedValue("task_source.bytes"),
        )?;
        if current_bytes > SESSION_SOURCE_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.bytes",
            ));
        }
        total_bytes = total_bytes
            .checked_add(row_bytes)
            .and_then(|value| value.checked_add(gid.len()))
            .ok_or(SessionStoreError::InvalidPersistedValue(
                "task_source.read_budget",
            ))?;
        if total_bytes > SESSION_TASK_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue(
                "task_source.read_budget",
            ));
        }
    }
    Ok(())
}

fn task_source_owned_bytes(uri_bytes: usize) -> Option<usize> {
    std::mem::size_of::<SessionTaskSourceRecord>()
        .checked_add(uri_bytes)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<[u8; 32]>()))
}

fn validate_host_key_challenge_record(
    challenge: &SessionHostKeyChallengeRecord,
) -> Result<(), SessionStoreError> {
    if challenge.canonical_host.is_empty()
        || challenge.canonical_host.len() > 253
        || challenge.port == 0
        || challenge.algorithm.is_empty()
        || challenge.algorithm.len() > SESSION_MAX_ALGORITHM_BYTES
        || challenge.presented_public_key.is_empty()
        || challenge.presented_public_key.len() > SESSION_MAX_HOST_KEY_BYTES
    {
        return Err(SessionStoreError::InvalidRecord(
            "host_key_challenge.bounds",
        ));
    }
    if HostKeyFingerprint::for_presented_key(&challenge.presented_public_key)
        != challenge.fingerprint_sha256
    {
        return Err(SessionStoreError::InvalidRecord(
            "host_key_challenge.fingerprint_sha256",
        ));
    }
    time_to_i64(challenge.created_ms, "host_key_challenge.created_ms")?;
    Ok(())
}

fn require_paused_task(connection: &Connection, gid: Gid) -> Result<(), SessionStoreError> {
    let state = connection
        .query_row(
            "SELECT queue_state FROM task WHERE gid = ?1",
            [gid.to_string()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or(SessionStoreError::NotFound)?;
    if SessionQueueState::try_from(state)? == SessionQueueState::Paused {
        Ok(())
    } else {
        Err(SessionStoreError::InvalidRecord(
            "host_key_challenge.queue_state",
        ))
    }
}

fn reject_retained_host_key_departure(
    connection: &Connection,
    gid: Gid,
    expected_state: SessionQueueState,
    target_state: SessionQueueState,
) -> Result<(), SessionStoreError> {
    if expected_state != SessionQueueState::Paused || target_state == SessionQueueState::Paused {
        return Ok(());
    }
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM host_key_challenge WHERE gid = ?1",
        [gid.to_string()],
        |row| row.get(0),
    )?;
    match count {
        0 => Ok(()),
        1 => Err(SessionStoreError::InvalidRecord(
            "host_key_challenge.queue_state",
        )),
        _ => Err(SessionStoreError::InvalidPersistedValue(
            "host_key_challenge.count",
        )),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the strict row decoder validates the complete persisted host-key challenge tuple"
)]
fn decode_host_key_challenge_row(
    gid: &str,
    challenge_id: Vec<u8>,
    canonical_host: Vec<u8>,
    port: i64,
    algorithm: Vec<u8>,
    presented_public_key: Vec<u8>,
    fingerprint_sha256: Vec<u8>,
    created_ms: i64,
    queue_state: i64,
) -> Result<SessionHostKeyChallengeRecord, SessionStoreError> {
    if SessionQueueState::try_from(queue_state)? != SessionQueueState::Paused {
        return Err(SessionStoreError::InvalidPersistedValue(
            "host_key_challenge.queue_state",
        ));
    }
    let record = SessionHostKeyChallengeRecord {
        gid: decode_gid(gid)?,
        challenge_id: HostKeyChallengeId::new(challenge_id.try_into().map_err(|_| {
            SessionStoreError::InvalidPersistedValue("host_key_challenge.challenge_id")
        })?),
        canonical_host: String::from_utf8(canonical_host).map_err(|_| {
            SessionStoreError::InvalidPersistedValue("host_key_challenge.canonical_host")
        })?,
        port: u16::try_from(port)
            .map_err(|_| SessionStoreError::InvalidPersistedValue("host_key_challenge.port"))?,
        algorithm: String::from_utf8(algorithm).map_err(|_| {
            SessionStoreError::InvalidPersistedValue("host_key_challenge.algorithm")
        })?,
        presented_public_key,
        fingerprint_sha256: HostKeyFingerprint::new(fingerprint_sha256.try_into().map_err(
            |_| SessionStoreError::InvalidPersistedValue("host_key_challenge.fingerprint_sha256"),
        )?),
        created_ms: nonnegative_i64(created_ms, "host_key_challenge.created_ms")?,
    };
    validate_host_key_challenge_record(&record).map_err(|error| match error {
        SessionStoreError::InvalidRecord("host_key_challenge.bounds") => {
            SessionStoreError::InvalidPersistedValue("host_key_challenge.bounds")
        }
        SessionStoreError::InvalidRecord("host_key_challenge.fingerprint_sha256") => {
            SessionStoreError::InvalidPersistedValue("host_key_challenge.fingerprint_sha256")
        }
        other => other,
    })?;
    Ok(record)
}

fn read_host_key_challenge(
    connection: &Connection,
    gid: Gid,
) -> Result<Option<SessionHostKeyChallengeRecord>, SessionStoreError> {
    let row = connection
        .query_row(
            "SELECT challenge.gid, challenge.challenge_id, CAST(challenge.canonical_host AS BLOB), challenge.port, CAST(challenge.algorithm AS BLOB), challenge.presented_public_key, challenge.fingerprint_sha256, challenge.created_ms, task.queue_state FROM host_key_challenge AS challenge JOIN task ON task.gid = challenge.gid WHERE challenge.gid = ?1",
            [gid.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()?;
    row.map(|row| {
        decode_host_key_challenge_row(
            &row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8,
        )
    })
    .transpose()
}

fn read_host_key_challenge_records(
    connection: &Connection,
    read_budget_bytes: usize,
) -> Result<Vec<SessionHostKeyChallengeRecord>, SessionStoreError> {
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM host_key_challenge", [], |row| {
            row.get(0)
        })?;
    let count = bounded_count(count, SESSION_MAX_TASKS, "host_key_challenge.count")?;
    let mut statement = connection.prepare(
        "SELECT challenge.gid, challenge.challenge_id, CAST(challenge.canonical_host AS BLOB), challenge.port, CAST(challenge.algorithm AS BLOB), challenge.presented_public_key, challenge.fingerprint_sha256, challenge.created_ms, task.queue_state FROM host_key_challenge AS challenge JOIN task ON task.gid = challenge.gid ORDER BY challenge.gid",
    )?;
    let mut rows = statement.query([])?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("host_key_challenge.allocation"))?;
    let mut bytes = 0_usize;
    while let Some(row) = rows.next()? {
        let gid = row.get::<_, String>(0)?;
        let challenge_id = row.get::<_, Vec<u8>>(1)?;
        let canonical_host = row.get::<_, Vec<u8>>(2)?;
        let port = row.get::<_, i64>(3)?;
        let algorithm = row.get::<_, Vec<u8>>(4)?;
        let presented_public_key = row.get::<_, Vec<u8>>(5)?;
        let fingerprint_sha256 = row.get::<_, Vec<u8>>(6)?;
        let created_ms = row.get::<_, i64>(7)?;
        let queue_state = row.get::<_, i64>(8)?;
        bytes = [
            std::mem::size_of::<SessionHostKeyChallengeRecord>(),
            gid.len(),
            challenge_id.len(),
            canonical_host.len(),
            algorithm.len(),
            presented_public_key.len(),
            fingerprint_sha256.len(),
        ]
        .into_iter()
        .try_fold(bytes, |total, value| total.checked_add(value))
        .ok_or(SessionStoreError::InvalidPersistedValue(
            "host_key_challenge.read_budget",
        ))?;
        if bytes > read_budget_bytes {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.read_budget",
            ));
        }
        records.push(decode_host_key_challenge_row(
            &gid,
            challenge_id,
            canonical_host,
            port,
            algorithm,
            presented_public_key,
            fingerprint_sha256,
            created_ms,
            queue_state,
        )?);
    }
    if records.len() != count {
        return Err(SessionStoreError::InvalidPersistedValue(
            "host_key_challenge.count",
        ));
    }
    Ok(records)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionBackupSchema {
    Current,
}

#[derive(Debug)]
struct BackupPublicationCandidate {
    name: OsString,
    path: PathBuf,
}

fn discover_backup_publication_candidates(
    parent: &JournalDirectoryCapability,
    destination: &Path,
) -> Result<Vec<BackupPublicationCandidate>, SessionStoreError> {
    let destination_name = destination
        .file_name()
        .ok_or(SessionStoreError::InvalidConfig("backup.destination"))?;
    let entries = parent
        .entries()
        .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?;
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(entries.len().min(MAX_BACKUP_PUBLICATION_CANDIDATES))
        .map_err(|_| SessionStoreError::InvalidPersistedValue("backup.publication_candidates"))?;
    for name in entries {
        if !is_backup_publication_candidate_name(destination_name, &name) {
            continue;
        }
        if candidates.len() == MAX_BACKUP_PUBLICATION_CANDIDATES {
            return Err(SessionStoreError::InvalidPersistedValue(
                "backup.publication_candidates",
            ));
        }
        candidates.push(BackupPublicationCandidate {
            path: parent.display().join(&name),
            name,
        });
    }
    Ok(candidates)
}

#[cfg(unix)]
fn is_backup_publication_candidate_name(destination: &OsStr, candidate: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt as _;

    let Some(suffix) = candidate.as_bytes().strip_prefix(destination.as_bytes()) else {
        return false;
    };
    let Some(suffix) = suffix.strip_prefix(BACKUP_TEMP_MARKER.as_bytes()) else {
        return false;
    };
    let Some(token) = suffix.strip_suffix(BACKUP_TEMP_SUFFIX.as_bytes()) else {
        return false;
    };
    valid_backup_publication_token_bytes(token)
}

#[cfg(unix)]
fn valid_backup_publication_token_bytes(token: &[u8]) -> bool {
    let mut parts = token.split(|byte| *byte == b'-');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(process), Some(identifier), None)
            if !process.is_empty()
                && !identifier.is_empty()
                && process.iter().all(u8::is_ascii_digit)
                && identifier.iter().all(u8::is_ascii_digit)
    )
}

#[cfg(windows)]
fn is_backup_publication_candidate_name(destination: &OsStr, candidate: &OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt as _;

    let destination = destination.encode_wide().collect::<Vec<_>>();
    let candidate = candidate.encode_wide().collect::<Vec<_>>();
    let marker = BACKUP_TEMP_MARKER.encode_utf16().collect::<Vec<_>>();
    let suffix = BACKUP_TEMP_SUFFIX.encode_utf16().collect::<Vec<_>>();
    let Some(token) = candidate
        .strip_prefix(destination.as_slice())
        .and_then(|rest| rest.strip_prefix(marker.as_slice()))
        .and_then(|rest| rest.strip_suffix(suffix.as_slice()))
    else {
        return false;
    };
    let mut parts = token.split(|unit| *unit == u16::from(b'-'));
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(process), Some(identifier), None)
            if !process.is_empty()
                && !identifier.is_empty()
                && process
                    .iter()
                    .all(|unit| (u16::from(b'0')..=u16::from(b'9')).contains(unit))
                && identifier
                    .iter()
                    .all(|unit| (u16::from(b'0')..=u16::from(b'9')).contains(unit))
    )
}

#[cfg(not(any(unix, windows)))]
fn is_backup_publication_candidate_name(destination: &OsStr, candidate: &OsStr) -> bool {
    let destination = destination.to_string_lossy();
    let candidate = candidate.to_string_lossy();
    let Some(token) = candidate
        .strip_prefix(destination.as_ref())
        .and_then(|rest| rest.strip_prefix(BACKUP_TEMP_MARKER))
        .and_then(|rest| rest.strip_suffix(BACKUP_TEMP_SUFFIX))
    else {
        return false;
    };
    let mut parts = token.split('-');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(process), Some(identifier), None)
            if !process.is_empty()
                && !identifier.is_empty()
                && process.bytes().all(|byte| byte.is_ascii_digit())
                && identifier.bytes().all(|byte| byte.is_ascii_digit())
    )
}

fn validate_backup_database(
    path: &Path,
    schema: SessionBackupSchema,
) -> Result<(), SessionStoreError> {
    let backup = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    apply_limits(&backup)?;
    let journal_mode: String =
        backup.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("delete") {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.journal_mode",
        ));
    }
    validate_integrity(&backup)?;
    match schema {
        SessionBackupSchema::Current => {
            validate_schema(&backup)?;
            validate_persisted_semantics(&backup)?;
        }
    }
    Ok(())
}

fn validate_recovered_backup_database(
    path: &Path,
    schema: SessionBackupSchema,
) -> Result<(), SessionStoreError> {
    if validate_existing_sqlite_sidecars(path)? {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.publication_candidate",
        ));
    }
    let result = validate_backup_database(path, schema);
    let cleanup = remove_owned_sqlite_sidecars(path);
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

fn reconcile_backup_publication(
    parent: &JournalDirectoryCapability,
    destination: &Path,
    schema: SessionBackupSchema,
) -> Result<(), SessionStoreError> {
    let candidates = discover_backup_publication_candidates(parent, destination)?;
    if candidates.is_empty() {
        return Ok(());
    }
    let destination_name = destination
        .file_name()
        .ok_or(SessionStoreError::InvalidConfig("backup.destination"))?;
    let destination_exists = path_entry_exists(destination)?;

    if destination_exists {
        for candidate in &candidates {
            if !parent
                .same_regular_file(&candidate.name, destination_name)
                .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
            {
                return Err(SessionStoreError::InvalidPersistedValue(
                    "backup.publication_candidate",
                ));
            }
        }
        let expected_links = u64::try_from(candidates.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or(SessionStoreError::InvalidPersistedValue(
                "backup.publication_candidate",
            ))?;
        if parent
            .regular_file_link_count(destination_name)
            .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
            != expected_links
        {
            return Err(SessionStoreError::InvalidPersistedValue(
                "backup.publication_candidate",
            ));
        }
        verify_private_backup_publication_permissions(destination)?;
        validate_recovered_backup_database(destination, schema)?;
        remove_backup_publication_candidates(parent, destination_name, &candidates)?;
        parent
            .sync()
            .map_err(|error| session_capability_error(SessionIoOperation::CreateBackup, error))?;
        validate_regular_artifact(destination)?;
        verify_private_file_permissions(destination)?;
        return Ok(());
    }

    if candidates.len() != 1 {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.publication_candidate",
        ));
    }
    let candidate = &candidates[0];
    validate_regular_artifact(&candidate.path)?;
    verify_private_file_permissions(&candidate.path)?;
    if parent
        .regular_file_link_count(&candidate.name)
        .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
        != 1
    {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.publication_candidate",
        ));
    }
    validate_recovered_backup_database(&candidate.path, schema)?;
    match parent.link_no_replace(&candidate.name, destination_name) {
        Ok(()) => {
            backup_test_crash("after_link");
            parent.sync().map_err(|error| {
                session_capability_error(SessionIoOperation::CreateBackup, error)
            })?;
            backup_test_crash("after_link_sync");
        }
        Err(error) if native_error_kind(&error) == io::ErrorKind::AlreadyExists => {
            if !parent
                .same_regular_file(&candidate.name, destination_name)
                .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
            {
                return Err(SessionStoreError::BackupPathExists);
            }
        }
        Err(error) => {
            return Err(session_capability_error(
                SessionIoOperation::CreateBackup,
                error,
            ));
        }
    }
    if !parent
        .same_regular_file(&candidate.name, destination_name)
        .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
        || parent
            .regular_file_link_count(destination_name)
            .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
            != 2
    {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.publication_candidate",
        ));
    }
    remove_backup_publication_candidates(parent, destination_name, &candidates)?;
    backup_test_crash("after_unlink");
    parent
        .sync()
        .map_err(|error| session_capability_error(SessionIoOperation::CreateBackup, error))?;
    validate_regular_artifact(destination)?;
    verify_private_file_permissions(destination)?;
    Ok(())
}

fn remove_backup_publication_candidates(
    parent: &JournalDirectoryCapability,
    destination_name: &OsStr,
    candidates: &[BackupPublicationCandidate],
) -> Result<(), SessionStoreError> {
    for candidate in candidates {
        if !parent
            .same_regular_file(&candidate.name, destination_name)
            .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
        {
            return Err(SessionStoreError::InvalidPersistedValue(
                "backup.publication_candidate",
            ));
        }
    }
    let expected_links = u64::try_from(candidates.len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .ok_or(SessionStoreError::InvalidPersistedValue(
            "backup.publication_candidate",
        ))?;
    if parent
        .regular_file_link_count(destination_name)
        .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?
        != expected_links
    {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.publication_candidate",
        ));
    }
    for candidate in candidates {
        #[cfg(test)]
        if BACKUP_FAIL_NEXT_UNLINK.with(|fault| fault.replace(false)) {
            return Err(session_io_error(
                SessionIoOperation::RemoveFailedBackup,
                io::Error::other("injected backup temporary unlink failure"),
            ));
        }
        parent.remove_file(&candidate.name).map_err(|error| {
            session_capability_error(SessionIoOperation::RemoveFailedBackup, error)
        })?;
    }
    Ok(())
}

fn backup_connection_to(
    connection: &Connection,
    destination: &Path,
    schema: SessionBackupSchema,
) -> Result<(), SessionStoreError> {
    let destination = destination.to_path_buf();
    validate_persistence_file_name(&destination)?;
    if backup_name_has_reserved_sqlite_suffix(&destination) {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.reserved_sqlite_companion",
        ));
    }
    prepare_private_directory(required_private_parent(&destination)?)?;
    let destination = canonicalize_persistence_parent(destination)?;
    let parent =
        JournalDirectoryCapability::open_trusted(required_private_parent(&destination)?)
            .map_err(|error| session_capability_error(SessionIoOperation::InspectPath, error))?;
    reconcile_backup_publication(&parent, &destination, schema)?;
    if path_entry_exists(&destination)? {
        return Err(SessionStoreError::BackupPathExists);
    }
    if validate_existing_sqlite_sidecars(&destination)? {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.orphan_sqlite_sidecar",
        ));
    }
    let temporary = backup_temporary_path(&destination);
    let temporary_name = temporary
        .file_name()
        .ok_or(SessionStoreError::InvalidConfig("backup.temporary"))?
        .to_os_string();
    let destination_name = destination
        .file_name()
        .ok_or(SessionStoreError::InvalidConfig("backup.destination"))?;
    if validate_existing_sqlite_sidecars(&temporary)? {
        return Err(SessionStoreError::InvalidPersistedValue(
            "backup.orphan_sqlite_sidecar",
        ));
    }
    create_secure_file(&temporary, SessionIoOperation::CreateBackup)?;
    let mut installed = false;
    let result = (|| {
        tighten_database_permissions(&temporary)?;
        connection.backup(rusqlite::MAIN_DB, &temporary, None)?;
        tighten_database_permissions(&temporary)?;
        validate_backup_database(&temporary, schema)?;
        remove_owned_sqlite_sidecars(&temporary)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&temporary)
            .and_then(|file| file.sync_all())
            .map_err(|error| session_io_error(SessionIoOperation::CreateBackup, error))?;
        parent
            .link_no_replace(&temporary_name, destination_name)
            .map_err(|error| {
                if native_error_kind(&error) == io::ErrorKind::AlreadyExists {
                    SessionStoreError::BackupPathExists
                } else {
                    session_capability_error(SessionIoOperation::CreateBackup, error)
                }
            })?;
        installed = true;
        backup_test_crash("after_link");
        parent
            .sync()
            .map_err(|error| session_capability_error(SessionIoOperation::CreateBackup, error))?;
        backup_test_crash("after_link_sync");
        Ok(())
    })();
    let sidecar_cleanup = remove_owned_sqlite_sidecars(&temporary);
    let publication_candidate = BackupPublicationCandidate {
        name: temporary_name.clone(),
        path: temporary.clone(),
    };
    let temporary_cleanup = if installed {
        remove_backup_publication_candidates(
            &parent,
            destination_name,
            std::slice::from_ref(&publication_candidate),
        )
    } else {
        remove_backup_temporary(&parent, &temporary_name)
    };
    if let Some(cleanup_error) = sidecar_cleanup.err().or_else(|| temporary_cleanup.err()) {
        if installed {
            // Never risk deleting a raced destination replacement. A failed
            // temporary-link cleanup leaves two names for one inode, so the
            // operation cannot report success under the unique-link contract.
            return match result {
                Ok(()) => Err(cleanup_error),
                Err(error) => Err(error),
            };
        }
        return result.and(Err(cleanup_error));
    }
    if installed {
        backup_test_crash("after_unlink");
        parent
            .sync()
            .map_err(|error| session_capability_error(SessionIoOperation::CreateBackup, error))?;
    }
    result
}

fn remove_backup_temporary(
    parent: &JournalDirectoryCapability,
    temporary_name: &OsStr,
) -> Result<(), SessionStoreError> {
    #[cfg(test)]
    if BACKUP_FAIL_NEXT_UNLINK.with(|fault| fault.replace(false)) {
        return Err(session_io_error(
            SessionIoOperation::RemoveFailedBackup,
            io::Error::other("injected backup temporary unlink failure"),
        ));
    }
    parent
        .remove_file(temporary_name)
        .map_err(|error| session_capability_error(SessionIoOperation::RemoveFailedBackup, error))
}

fn remove_owned_sqlite_sidecars(database: &Path) -> Result<(), SessionStoreError> {
    let mut first_error = None;
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sqlite_sidecar_path(database, suffix);
        if let Err(error) = fs::remove_file(sidecar)
            && error.kind() != io::ErrorKind::NotFound
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    match first_error {
        Some(error) => Err(session_io_error(
            SessionIoOperation::RemoveFailedBackup,
            error,
        )),
        None => Ok(()),
    }
}

fn backup_temporary_path(destination: &Path) -> PathBuf {
    let identifier = BACKUP_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    let mut name = destination.as_os_str().to_os_string();
    name.push(format!(
        ".ariax-backup-{}-{identifier}.tmp",
        std::process::id()
    ));
    PathBuf::from(name)
}

struct RawTaskRow {
    gid: String,
    session_id: Vec<u8>,
    queue_state: i64,
    queue_position: i64,
    desired_paused: i64,
    slow_original_position: Option<i64>,
    slow_demotion_count: i64,
    slow_retry_scheduled_at_ms: Option<i64>,
    slow_retry_delay_ms: Option<Vec<u8>>,
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
            slow_original_position: row.get(5)?,
            slow_demotion_count: row.get(6)?,
            slow_retry_scheduled_at_ms: row.get(7)?,
            slow_retry_delay_ms: row.get(8)?,
            primary_journal_id: row.get(9)?,
            primary_journal_path: row.get(10)?,
            replica_journal_path: row.get(11)?,
            replica_sequence: row.get(12)?,
            root_display: row.get(13)?,
            cached_layout_hash: row.get(14)?,
            cached_root_binding_hash: row.get(15)?,
            cached_snapshot_hash: row.get(16)?,
            no_space_target: row.get(17)?,
            no_space_scheduled_at_ms: row.get(18)?,
            no_space_delay_ms: row.get(19)?,
            created_ms: row.get(20)?,
            updated_ms: row.get(21)?,
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
        let slow_demotion_count = u32::try_from(self.slow_demotion_count)
            .map_err(|_| SessionStoreError::InvalidPersistedValue("task.slow_demotion_count"))?;
        let slow_slot = match (
            self.slow_original_position,
            self.slow_retry_scheduled_at_ms,
            self.slow_retry_delay_ms,
        ) {
            (None, None, None) => None,
            (Some(original), scheduled, delay) => {
                let retry = match (scheduled, delay) {
                    (None, None) => None,
                    (Some(scheduled_at_ms), Some(delay_ms)) => Some(SessionSlowRetryDecision {
                        scheduled_at_ms: nonnegative_i64(
                            scheduled_at_ms,
                            "task.slow_retry_scheduled_at_ms",
                        )?,
                        delay_ms: decode_u64(&delay_ms, "task.slow_retry_delay_ms")?,
                    }),
                    _ => {
                        return Err(SessionStoreError::InvalidPersistedValue(
                            "task.slow_retry_tuple",
                        ));
                    }
                };
                Some(SessionSlowSlotState {
                    original_position: u32::try_from(original).map_err(|_| {
                        SessionStoreError::InvalidPersistedValue("task.slow_original_position")
                    })?,
                    retry,
                })
            }
            _ => {
                return Err(SessionStoreError::InvalidPersistedValue(
                    "task.slow_slot_tuple",
                ));
            }
        };
        Ok(SessionTaskRecord {
            gid: decode_gid(&self.gid)?,
            session_id: decode_session_id(&self.session_id)?,
            queue_state: SessionQueueState::try_from(self.queue_state)?,
            queue_position,
            desired_paused: decode_bool(self.desired_paused, "task.desired_paused")?,
            slow_demotion_count,
            slow_slot,
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

    fn estimated_read_bytes(&self) -> Result<usize, SessionStoreError> {
        let variable = [
            self.gid.len(),
            self.session_id.len(),
            self.primary_journal_id.len(),
            self.primary_journal_path.len(),
            self.replica_journal_path.as_ref().map_or(0, Vec::len),
            self.replica_sequence.as_ref().map_or(0, Vec::len),
            self.root_display.len(),
            self.cached_layout_hash.as_ref().map_or(0, Vec::len),
            self.cached_root_binding_hash.as_ref().map_or(0, Vec::len),
            self.cached_snapshot_hash.len(),
            self.slow_retry_delay_ms.as_ref().map_or(0, Vec::len),
            self.no_space_target.as_ref().map_or(0, Vec::len),
            self.no_space_delay_ms.as_ref().map_or(0, Vec::len),
        ];
        variable.into_iter().try_fold(
            std::mem::size_of::<SessionTaskRecord>() + std::mem::size_of::<Self>(),
            |total, value| {
                total
                    .checked_add(value)
                    .ok_or(SessionStoreError::InvalidPersistedValue("task.read_budget"))
            },
        )
    }
}

fn read_task_records(connection: &Connection) -> Result<Vec<SessionTaskRecord>, SessionStoreError> {
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM task WHERE task_kind = 1", [], |row| {
            row.get(0)
        })?;
    let count = bounded_count(count, SESSION_MAX_TASKS, "task.count")?;
    let mut statement = connection.prepare(
        "SELECT gid, session_id, queue_state, queue_position, desired_paused, slow_original_position, slow_demotion_count, slow_retry_scheduled_at_ms, slow_retry_delay_ms, primary_journal_id, primary_journal_path, replica_journal_path, replica_sequence, root_display, cached_layout_hash, cached_root_binding_hash, cached_snapshot_hash, no_space_target, no_space_scheduled_at_ms, no_space_delay_ms, created_ms, updated_ms FROM task WHERE task_kind = 1 ORDER BY queue_state, queue_position, gid",
    )?;
    let mut rows = statement.query([])?;
    let mut tasks = Vec::new();
    tasks
        .try_reserve_exact(count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("task.allocation"))?;
    let mut read_bytes = 0_usize;
    while let Some(row) = rows.next()? {
        let raw = RawTaskRow::from_row(row)?;
        read_bytes = read_bytes
            .checked_add(raw.estimated_read_bytes()?)
            .ok_or(SessionStoreError::InvalidPersistedValue("task.read_budget"))?;
        if read_bytes > SESSION_TASK_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue("task.read_budget"));
        }
        let task = raw.decode()?;
        validate_task(&task)?;
        tasks.push(task);
        if tasks.len() > count {
            return Err(SessionStoreError::InvalidPersistedValue("task.count"));
        }
    }
    if tasks.len() != count {
        return Err(SessionStoreError::InvalidPersistedValue("task.count"));
    }
    Ok(tasks)
}

fn read_stopped_results(
    connection: &Connection,
) -> Result<Vec<SessionStoppedResultRecord>, SessionStoreError> {
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM stopped_result", [], |row| row.get(0))?;
    let count = bounded_count(count, SESSION_MAX_TASKS, "stopped_result.count")?;
    let mut statement = connection.prepare(
        "SELECT result.gid, result.terminal_status, result.error_code, result.safe_message, result.total_length, result.layout_hash, result.completed_ms
         FROM stopped_result AS result
         JOIN task ON task.gid = result.gid
         WHERE task.queue_state = ?1
         ORDER BY task.queue_position, task.gid",
    )?;
    let mut rows = statement.query([SessionQueueState::Stopped as i64])?;
    let mut results = Vec::new();
    results
        .try_reserve_exact(count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("stopped_result.allocation"))?;
    let mut read_bytes = 0_usize;
    while let Some(row) = rows.next()? {
        let gid = row.get::<_, String>(0)?;
        let terminal_status = row.get::<_, i64>(1)?;
        let error_code = row.get::<_, i64>(2)?;
        let safe_message = row.get::<_, String>(3)?;
        let total_length = row.get::<_, Option<Vec<u8>>>(4)?;
        let layout_hash = row.get::<_, Option<Vec<u8>>>(5)?;
        let completed_ms = row.get::<_, i64>(6)?;
        let row_bytes = [
            std::mem::size_of::<SessionStoppedResultRecord>(),
            gid.len(),
            safe_message.len(),
            total_length.as_ref().map_or(0, Vec::len),
            layout_hash.as_ref().map_or(0, Vec::len),
        ]
        .into_iter()
        .try_fold(0_usize, |total, value| total.checked_add(value))
        .ok_or(SessionStoreError::InvalidPersistedValue(
            "stopped_result.read_budget",
        ))?;
        read_bytes =
            read_bytes
                .checked_add(row_bytes)
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "stopped_result.read_budget",
                ))?;
        if read_bytes > SESSION_TASK_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue(
                "stopped_result.read_budget",
            ));
        }
        let error_kind = match error_code {
            0 => None,
            value => Some(
                ErrorKind::try_from(u8::try_from(value).map_err(|_| {
                    SessionStoreError::InvalidPersistedValue("stopped_result.error_code")
                })?)
                .map_err(|()| {
                    SessionStoreError::InvalidPersistedValue("stopped_result.error_code")
                })?,
            ),
        };
        let result = SessionStoppedResultRecord {
            gid: decode_gid(&gid)?,
            status: SessionTerminalStatus::try_from(terminal_status)?,
            error_kind,
            safe_message,
            total_length: total_length
                .as_deref()
                .map(|value| decode_u64(value, "stopped_result.total_length"))
                .transpose()?,
            layout_hash: layout_hash
                .as_deref()
                .map(|value| decode_hash(value, "stopped_result.layout_hash"))
                .transpose()?,
            completed_ms: nonnegative_i64(completed_ms, "stopped_result.completed_ms")?,
        };
        validate_stopped_result(&result)?;
        results.push(result);
        if results.len() > count {
            return Err(SessionStoreError::InvalidPersistedValue(
                "stopped_result.count",
            ));
        }
    }
    if results.len() != count {
        return Err(SessionStoreError::InvalidPersistedValue(
            "stopped_result.count",
        ));
    }
    Ok(results)
}

fn read_journal_install_for_gid(
    connection: &Connection,
    gid: Gid,
) -> Result<JournalInstallIntent, SessionStoreError> {
    let raw = connection
        .query_row(
            "SELECT checkpoint_id, old_journal_id, old_path, new_journal_id, new_path, source_last_sequence, phase, created_ms FROM journal_install WHERE gid = ?1",
            [gid.to_string()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()?
        .ok_or(SessionStoreError::NotFound)?;
    let intent = JournalInstallIntent {
        gid,
        checkpoint_id: decode_checkpoint_id(&raw.0)?,
        old_journal_id: decode_journal_id(&raw.1, "journal_install.old_journal_id")?,
        old_path: decode_platform_path(&raw.2, "journal_install.old_path")?,
        new_journal_id: decode_journal_id(&raw.3, "journal_install.new_journal_id")?,
        new_path: decode_platform_path(&raw.4, "journal_install.new_path")?,
        source_last_sequence: decode_u64(&raw.5, "journal_install.source_last_sequence")?,
        phase: JournalInstallPhase::try_from(raw.6)?,
        created_ms: nonnegative_i64(raw.7, "journal_install.created_ms")?,
    };
    validate_install_values(&intent)?;
    Ok(intent)
}

fn read_journal_installs(
    connection: &Connection,
) -> Result<Vec<JournalInstallIntent>, SessionStoreError> {
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM journal_install", [], |row| row.get(0))?;
    let count = bounded_count(count, SESSION_MAX_TASKS, "journal_install.count")?;
    let mut statement = connection.prepare(
        "SELECT gid, checkpoint_id, old_journal_id, old_path, new_journal_id, new_path, source_last_sequence, phase, created_ms FROM journal_install ORDER BY gid",
    )?;
    let mut rows = statement.query([])?;
    let mut intents = Vec::new();
    intents
        .try_reserve_exact(count)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("journal_install.allocation"))?;
    let mut read_bytes = 0_usize;
    while let Some(row) = rows.next()? {
        let gid = row.get::<_, String>(0)?;
        let checkpoint = row.get::<_, Vec<u8>>(1)?;
        let old_id = row.get::<_, Vec<u8>>(2)?;
        let old_path = row.get::<_, Vec<u8>>(3)?;
        let new_id = row.get::<_, Vec<u8>>(4)?;
        let new_path = row.get::<_, Vec<u8>>(5)?;
        let sequence = row.get::<_, Vec<u8>>(6)?;
        let row_bytes = [
            gid.len(),
            checkpoint.len(),
            old_id.len(),
            old_path.len(),
            new_id.len(),
            new_path.len(),
            sequence.len(),
            std::mem::size_of::<JournalInstallIntent>(),
        ]
        .into_iter()
        .try_fold(0_usize, |total, value| total.checked_add(value))
        .ok_or(SessionStoreError::InvalidPersistedValue(
            "journal_install.read_budget",
        ))?;
        read_bytes =
            read_bytes
                .checked_add(row_bytes)
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "journal_install.read_budget",
                ))?;
        if read_bytes > SESSION_INSTALL_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue(
                "journal_install.read_budget",
            ));
        }
        let intent = JournalInstallIntent {
            gid: decode_gid(&gid)?,
            checkpoint_id: decode_checkpoint_id(&checkpoint)?,
            old_journal_id: decode_journal_id(&old_id, "journal_install.old_journal_id")?,
            old_path: decode_platform_path(&old_path, "journal_install.old_path")?,
            new_journal_id: decode_journal_id(&new_id, "journal_install.new_journal_id")?,
            new_path: decode_platform_path(&new_path, "journal_install.new_path")?,
            source_last_sequence: decode_u64(&sequence, "journal_install.source_last_sequence")?,
            phase: JournalInstallPhase::try_from(row.get::<_, i64>(7)?)?,
            created_ms: nonnegative_i64(row.get::<_, i64>(8)?, "journal_install.created_ms")?,
        };
        validate_install_values(&intent)?;
        intents.push(intent);
    }
    if intents.len() != count {
        return Err(SessionStoreError::InvalidPersistedValue(
            "journal_install.count",
        ));
    }
    Ok(intents)
}

fn validate_config(config: SessionStoreConfig) -> Result<(), SessionStoreError> {
    if !(SESSION_MIN_CACHE_KIB..=SESSION_MAX_CACHE_KIB).contains(&config.cache_kib) {
        return Err(SessionStoreError::InvalidConfig("cache_kib"));
    }
    if config.busy_timeout_ms != SESSION_BUSY_TIMEOUT_MS {
        return Err(SessionStoreError::InvalidConfig("busy_timeout_ms"));
    }
    Ok(())
}

fn prepare_database_path(path: &Path) -> Result<bool, SessionStoreError> {
    prepare_private_directory(required_private_parent(path)?)?;
    let existed = validate_database_artifacts(path)?;
    if existed {
        return Ok(true);
    }
    create_secure_file(path, SessionIoOperation::CreateDatabase)?;
    tighten_database_permissions(path)?;
    Ok(false)
}

fn validate_database_artifacts(path: &Path) -> Result<bool, SessionStoreError> {
    let existed = path_entry_exists(path)?;
    let main_is_empty = if existed {
        validate_regular_artifact(path)?.len() == 0
    } else {
        true
    };
    let has_sidecars = validate_existing_sqlite_sidecars(path)?;
    if main_is_empty && has_sidecars {
        return Err(SessionStoreError::InvalidPersistedValue(
            "orphan_sqlite_sidecar",
        ));
    }
    Ok(existed)
}

fn session_owner_lock_path(database_path: &Path) -> PathBuf {
    let mut value = database_path.as_os_str().to_os_string();
    value.push(SESSION_OWNER_LOCK_SUFFIX);
    PathBuf::from(value)
}

fn acquire_session_owner_lock(database_path: &Path) -> Result<SessionOwnerLock, SessionStoreError> {
    let lock_path = session_owner_lock_path(database_path);
    if !path_entry_exists(&lock_path)? {
        match create_secure_file(&lock_path, SessionIoOperation::AcquireOwnerLock) {
            Ok(())
            | Err(SessionStoreError::Io {
                operation: SessionIoOperation::AcquireOwnerLock,
                kind: io::ErrorKind::AlreadyExists,
            }) => {}
            Err(error) => return Err(error),
        }
    }
    validate_regular_artifact(&lock_path)?;
    tighten_database_permissions(&lock_path)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| session_io_error(SessionIoOperation::AcquireOwnerLock, error))?;
    match lock.try_lock_exclusive() {
        Ok(()) => Ok(SessionOwnerLock { file: lock }),
        Err(error) if is_lock_contended(&error) => Err(SessionStoreError::OwnerLockBusy),
        Err(error) => Err(session_io_error(
            SessionIoOperation::AcquireOwnerLock,
            error,
        )),
    }
}

fn is_lock_contended(error: &io::Error) -> bool {
    let expected = fs2::lock_contended_error();
    error.kind() == io::ErrorKind::WouldBlock
        || matches!(
            (error.raw_os_error(), expected.raw_os_error()),
            (Some(actual), Some(expected)) if actual == expected
        )
}

#[cfg(not(windows))]
fn validate_persistence_file_name(_path: &Path) -> Result<(), SessionStoreError> {
    Ok(())
}

#[cfg(windows)]
fn validate_persistence_file_name(path: &Path) -> Result<(), SessionStoreError> {
    use std::os::windows::ffi::OsStrExt as _;

    let name = path
        .file_name()
        .ok_or_else(|| invalid_persistence_file_name("persistence path has no final filename"))?;
    let units = name.encode_wide().collect::<Vec<_>>();
    if units.is_empty()
        || units.iter().any(|unit| *unit == u16::from(b':'))
        || units
            .last()
            .is_some_and(|unit| matches!(*unit, 0x20 | 0x2e))
        || is_windows_reserved_file_name(&units)
    {
        return Err(invalid_persistence_file_name(
            "persistence filename has an unsafe Win32 alias form",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn invalid_persistence_file_name(message: &'static str) -> SessionStoreError {
    session_io_error(
        SessionIoOperation::InspectPath,
        io::Error::new(io::ErrorKind::InvalidInput, message),
    )
}

#[cfg(windows)]
fn is_windows_reserved_file_name(units: &[u16]) -> bool {
    let stem = units
        .split(|unit| *unit == u16::from(b'.'))
        .next()
        .unwrap_or_default();
    windows_ascii_name_eq(stem, b"CON")
        || windows_ascii_name_eq(stem, b"PRN")
        || windows_ascii_name_eq(stem, b"AUX")
        || windows_ascii_name_eq(stem, b"NUL")
        || (stem.len() == 4
            && (windows_ascii_name_eq(&stem[..3], b"COM")
                || windows_ascii_name_eq(&stem[..3], b"LPT"))
            && matches!(stem[3], 0x31..=0x39 | 0x00b2 | 0x00b3 | 0x00b9))
}

#[cfg(windows)]
fn windows_ascii_name_eq(units: &[u16], expected_uppercase: &[u8]) -> bool {
    units.len() == expected_uppercase.len()
        && units
            .iter()
            .zip(expected_uppercase)
            .all(|(actual, expected)| {
                let actual = if (u16::from(b'a')..=u16::from(b'z')).contains(actual) {
                    *actual - u16::from(b'a' - b'A')
                } else {
                    *actual
                };
                actual == u16::from(*expected)
            })
}

#[cfg(not(windows))]
fn canonicalize_database_path(path: PathBuf) -> Result<PathBuf, SessionStoreError> {
    Ok(path)
}

#[cfg(windows)]
fn canonicalize_database_path(path: PathBuf) -> Result<PathBuf, SessionStoreError> {
    let path = canonicalize_persistence_parent(path)?;
    if !path_entry_exists(&path)? {
        return Ok(path);
    }
    validate_regular_artifact(&path)?;
    fs::canonicalize(path).map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))
}

#[cfg(not(windows))]
fn canonicalize_persistence_parent(path: PathBuf) -> Result<PathBuf, SessionStoreError> {
    let file_name = path
        .file_name()
        .ok_or(SessionStoreError::InvalidConfig("persistence_path"))?
        .to_os_string();
    let parent = fs::canonicalize(required_private_parent(&path)?)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    Ok(parent.join(file_name))
}

#[cfg(windows)]
fn canonicalize_persistence_parent(path: PathBuf) -> Result<PathBuf, SessionStoreError> {
    let file_name = path
        .file_name()
        .ok_or_else(|| invalid_persistence_file_name("persistence path has no final filename"))?
        .to_os_string();
    let parent = fs::canonicalize(required_private_parent(&path)?)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    Ok(parent.join(file_name))
}

fn required_private_parent(path: &Path) -> Result<&Path, SessionStoreError> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            session_io_error(
                SessionIoOperation::InspectPath,
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "persistence path must include an explicit parent directory",
                ),
            )
        })
}

fn path_entry_exists(path: &Path) -> Result<bool, SessionStoreError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(session_io_error(SessionIoOperation::InspectPath, error)),
    }
}

fn validate_existing_sqlite_sidecars(path: &Path) -> Result<bool, SessionStoreError> {
    let mut found = false;
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sqlite_sidecar_path(path, suffix);
        if path_entry_exists(&sidecar)? {
            validate_regular_artifact(&sidecar)?;
            found = true;
        }
    }
    Ok(found)
}

fn validate_regular_artifact(path: &Path) -> Result<fs::Metadata, SessionStoreError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    if metadata_is_link_like(&metadata) || !metadata.is_file() {
        return Err(session_io_error(
            SessionIoOperation::InspectPath,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "persistence artifact is not a uniquely linked regular file",
            ),
        ));
    }
    validate_unique_file_identity(path, &metadata)?;
    Ok(metadata)
}

#[cfg(unix)]
fn validate_unique_file_identity(
    _path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), SessionStoreError> {
    use std::os::unix::fs::MetadataExt as _;

    if metadata.nlink() == 1 {
        Ok(())
    } else {
        Err(session_io_error(
            SessionIoOperation::InspectPath,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "persistence artifact is not a uniquely linked regular file",
            ),
        ))
    }
}

#[cfg(windows)]
fn validate_unique_file_identity(
    path: &Path,
    _metadata: &fs::Metadata,
) -> Result<(), SessionStoreError> {
    ariax_windows_security::verify_single_link_regular_file(path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))
}

fn prepare_private_directory(path: &Path) -> Result<(), SessionStoreError> {
    validate_directory_path_components(path)?;
    let mut missing = Vec::new();
    let mut cursor = path;
    while !path_entry_exists(cursor)? {
        missing.push(cursor.to_path_buf());
        cursor = cursor.parent().ok_or_else(|| {
            session_io_error(
                SessionIoOperation::CreateDirectory,
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "directory has no existing ancestor",
                ),
            )
        })?;
    }
    let ancestor = fs::symlink_metadata(cursor)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    if ancestor.file_type().is_symlink() || !ancestor.is_dir() {
        return Err(session_io_error(
            SessionIoOperation::InspectPath,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "persistence directory ancestor is not a real directory",
            ),
        ));
    }
    if missing.is_empty() {
        return verify_private_directory(path);
    }
    for directory in missing.into_iter().rev() {
        create_private_directory(&directory)?;
        tighten_directory_permissions(&directory)?;
    }
    Ok(())
}

fn validate_directory_path_components(path: &Path) -> Result<(), SessionStoreError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        if component == Component::ParentDir {
            return Err(session_io_error(
                SessionIoOperation::InspectPath,
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "persistence paths cannot contain parent traversal",
                ),
            ));
        }
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_)) {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata_is_link_like(&metadata) || !metadata.is_dir() {
                    return Err(session_io_error(
                        SessionIoOperation::InspectPath,
                        io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "persistence path contains a linked or non-directory component",
                        ),
                    ));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(session_io_error(SessionIoOperation::InspectPath, error));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn metadata_is_link_like(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn metadata_is_link_like(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_type().is_symlink()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> Result<(), SessionStoreError> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|error| session_io_error(SessionIoOperation::CreateDirectory, error))
}

#[cfg(windows)]
fn create_private_directory(path: &Path) -> Result<(), SessionStoreError> {
    ariax_windows_security::create_private_directory(path)
        .map_err(|error| session_io_error(SessionIoOperation::CreateDirectory, error))
}

#[cfg(unix)]
fn create_secure_file(path: &Path, operation: SessionIoOperation) -> Result<(), SessionStoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
    options
        .open(path)
        .map(|_| ())
        .map_err(|error| session_io_error(operation, error))
}

#[cfg(windows)]
fn create_secure_file(path: &Path, operation: SessionIoOperation) -> Result<(), SessionStoreError> {
    ariax_windows_security::create_private_file(path)
        .map(drop)
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
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))?;
    verify_private_file_permissions(path)
}

#[cfg(unix)]
fn tighten_directory_permissions(path: &Path) -> Result<(), SessionStoreError> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))?
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))?;
    verify_private_directory(path)
}

#[cfg(unix)]
fn verify_private_file_permissions(path: &Path) -> Result<(), SessionStoreError> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    if metadata_is_link_like(&metadata)
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(session_io_error(
            SessionIoOperation::TightenPermissions,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "persistence file is not a private regular file",
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_private_directory(path: &Path) -> Result<(), SessionStoreError> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    if !metadata.is_dir() || metadata_is_link_like(&metadata) {
        return Err(session_io_error(
            SessionIoOperation::InspectPath,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "persistence directory is not a real directory",
            ),
        ));
    }
    if metadata.permissions().mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(session_io_error(
            SessionIoOperation::TightenPermissions,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "existing persistence directory is accessible by group or other users",
            ),
        ))
    }
}

#[cfg(unix)]
fn verify_private_backup_publication_permissions(path: &Path) -> Result<(), SessionStoreError> {
    verify_private_file_permissions(path)
}

#[cfg(windows)]
fn tighten_database_permissions(path: &Path) -> Result<(), SessionStoreError> {
    ariax_windows_security::apply_private_file_acl(path)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))?;
    verify_private_file_permissions(path)
}

#[cfg(windows)]
fn tighten_directory_permissions(path: &Path) -> Result<(), SessionStoreError> {
    verify_private_directory(path)
}

#[cfg(windows)]
fn verify_private_directory(path: &Path) -> Result<(), SessionStoreError> {
    ariax_windows_security::verify_private_directory(path)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))
}

#[cfg(windows)]
fn verify_private_file_permissions(path: &Path) -> Result<(), SessionStoreError> {
    ariax_windows_security::verify_private_file(path)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))
}

#[cfg(windows)]
fn verify_private_backup_publication_permissions(path: &Path) -> Result<(), SessionStoreError> {
    ariax_windows_security::verify_private_file_allow_alias(path)
        .map_err(|error| session_io_error(SessionIoOperation::TightenPermissions, error))
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn backup_name_has_reserved_sqlite_suffix(path: &Path) -> bool {
    let Some(name) = path.file_name() else {
        return false;
    };
    let name = name.to_string_lossy();
    ["-wal", "-shm", "-journal"].iter().any(|suffix| {
        name.as_bytes()
            .get(name.len().saturating_sub(suffix.len())..)
            .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix.as_bytes()))
    })
}

fn tighten_sqlite_artifact_permissions(path: &Path) -> Result<(), SessionStoreError> {
    tighten_database_permissions(path)?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sqlite_sidecar_path(path, suffix);
        if path_entry_exists(&sidecar)? {
            validate_regular_artifact(&sidecar)?;
            tighten_database_permissions(&sidecar)?;
        }
    }
    Ok(())
}

fn inspect_persisted_user_version(path: &Path) -> Result<u32, SessionStoreError> {
    const DATABASE_HEADER_BYTES: usize = 100;

    let metadata = fs::metadata(path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    if metadata.len() == 0 {
        return Ok(0);
    }
    let rollback = inspect_rollback_journal(path)?;
    if matches!(
        rollback,
        RollbackJournalPreflight::Hot {
            database_pages: 0,
            ..
        }
    ) {
        return Ok(0);
    }
    let raw_header = match rollback {
        RollbackJournalPreflight::Hot {
            page_one: Some(header),
            database_pages: _,
        } => Some(header),
        RollbackJournalPreflight::Cold
        | RollbackJournalPreflight::Hot {
            page_one: None,
            database_pages: _,
        } => {
            if metadata.len() < DATABASE_HEADER_BYTES as u64 {
                None
            } else {
                let mut database = File::open(path)
                    .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
                let mut header = [0_u8; DATABASE_HEADER_BYTES];
                database
                    .read_exact(&mut header)
                    .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
                Some(header)
            }
        }
    };
    let valid_header = raw_header.filter(|header| decode_database_header(header).is_ok());
    let recovered_header = inspect_wal_database_header(path, valid_header)?;
    let header = recovered_header
        .or(raw_header)
        .ok_or(SessionStoreError::InvalidPersistedValue("database.header"))?;
    let (_, version) = decode_database_header(&header)?;
    Ok(version)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RollbackJournalPreflight {
    Cold,
    Hot {
        page_one: Option<[u8; 100]>,
        database_pages: u64,
    },
}

fn inspect_rollback_journal(
    database_path: &Path,
) -> Result<RollbackJournalPreflight, SessionStoreError> {
    const JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
    const JOURNAL_HEADER_BYTES: u64 = 28;
    const MAX_SECTOR_SIZE: usize = 65_536;

    let journal_path = sqlite_sidecar_path(database_path, "-journal");
    if !path_entry_exists(&journal_path)? {
        return Ok(RollbackJournalPreflight::Cold);
    }
    validate_regular_artifact(&journal_path)?;
    let journal_length = fs::metadata(&journal_path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?
        .len();
    if journal_length < JOURNAL_HEADER_BYTES {
        return Ok(RollbackJournalPreflight::Cold);
    }
    let mut journal = File::open(&journal_path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    let mut first_header = [0_u8; JOURNAL_HEADER_BYTES as usize];
    journal
        .read_exact(&mut first_header)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    if first_header[..JOURNAL_MAGIC.len()] != JOURNAL_MAGIC {
        return Ok(RollbackJournalPreflight::Cold);
    }
    let sector_size = usize::try_from(decode_be_u32(&first_header[20..24]))
        .map_err(|_| SessionStoreError::InvalidPersistedValue("rollback_journal.sector_size"))?;
    let encoded_page_size = decode_be_u32(&first_header[24..28]);
    if encoded_page_size == 0 {
        return Err(SessionStoreError::InvalidPersistedValue(
            "rollback_journal.legacy_page_size",
        ));
    }
    let page_size = usize::try_from(encoded_page_size)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("rollback_journal.page_size"))?;
    if !(32..=MAX_SECTOR_SIZE).contains(&sector_size)
        || !sector_size.is_power_of_two()
        || !(512..=65_536).contains(&page_size)
        || !page_size.is_power_of_two()
    {
        return Ok(RollbackJournalPreflight::Cold);
    }
    let sector_size = u64::try_from(sector_size)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("rollback_journal.sector_size"))?;
    if journal_length < sector_size {
        return Ok(RollbackJournalPreflight::Cold);
    }
    let page_bytes = u64::try_from(page_size)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("rollback_journal.page_size"))?;
    let record_bytes =
        page_bytes
            .checked_add(8)
            .ok_or(SessionStoreError::InvalidPersistedValue(
                "rollback_journal.record_size",
            ))?;
    let pending_lock_page = (0x4000_0000_u64 / page_bytes) + 1;
    reject_super_journal_trailer(&mut journal, journal_length, &JOURNAL_MAGIC)?;
    let mut page = vec![0_u8; page_size];
    let mut page_one = None;
    let mut header_offset = 0_u64;
    let mut initial_database_pages = None;
    loop {
        if header_offset
            .checked_add(sector_size)
            .is_none_or(|end| end > journal_length)
        {
            break;
        }
        journal
            .seek(SeekFrom::Start(header_offset))
            .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
        let mut header = [0_u8; JOURNAL_HEADER_BYTES as usize];
        journal
            .read_exact(&mut header)
            .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
        if header[..JOURNAL_MAGIC.len()] != JOURNAL_MAGIC {
            break;
        }
        let record_count = decode_be_u32(&header[8..12]);
        let checksum_seed = decode_be_u32(&header[12..16]);
        let database_pages = u64::from(decode_be_u32(&header[16..20]));
        let original_database_pages = *initial_database_pages.get_or_insert(database_pages);
        let mut record_offset = header_offset.checked_add(sector_size).ok_or(
            SessionStoreError::InvalidPersistedValue("rollback_journal.offset"),
        )?;
        let available_records = journal_length.saturating_sub(record_offset) / record_bytes;
        let records = if record_count == u32::MAX {
            available_records
        } else {
            u64::from(record_count)
        };
        journal
            .seek(SeekFrom::Start(record_offset))
            .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
        for _ in 0..records {
            if record_offset
                .checked_add(record_bytes)
                .is_none_or(|end| end > journal_length)
            {
                return Ok(RollbackJournalPreflight::Hot {
                    page_one,
                    database_pages: original_database_pages,
                });
            }
            let mut page_number = [0_u8; 4];
            journal
                .read_exact(&mut page_number)
                .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
            journal
                .read_exact(&mut page)
                .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
            let mut checksum = [0_u8; 4];
            journal
                .read_exact(&mut checksum)
                .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
            let page_number = u64::from(decode_be_u32(&page_number));
            if page_number == 0 || page_number == pending_lock_page {
                return Ok(RollbackJournalPreflight::Hot {
                    page_one,
                    database_pages: original_database_pages,
                });
            }
            if page_number > original_database_pages {
                record_offset = record_offset.checked_add(record_bytes).ok_or(
                    SessionStoreError::InvalidPersistedValue("rollback_journal.offset"),
                )?;
                continue;
            }
            if rollback_journal_checksum(&page, checksum_seed) != decode_be_u32(&checksum) {
                return Ok(RollbackJournalPreflight::Hot {
                    page_one,
                    database_pages: original_database_pages,
                });
            }
            if page_number == 1 {
                let mut header = [0_u8; 100];
                header.copy_from_slice(&page[..100]);
                page_one = Some(header);
            }
            record_offset = record_offset.checked_add(record_bytes).ok_or(
                SessionStoreError::InvalidPersistedValue("rollback_journal.offset"),
            )?;
        }
        if record_count == u32::MAX {
            break;
        }
        header_offset = round_up_to_sector(record_offset, sector_size)?;
    }
    Ok(RollbackJournalPreflight::Hot {
        page_one,
        database_pages: initial_database_pages.unwrap_or(0),
    })
}

fn reject_super_journal_trailer(
    journal: &mut File,
    journal_length: u64,
    journal_magic: &[u8; 8],
) -> Result<(), SessionStoreError> {
    const TRAILER_BYTES: u64 = 16;
    if journal_length < TRAILER_BYTES {
        return Ok(());
    }
    journal
        .seek(SeekFrom::Start(journal_length - TRAILER_BYTES))
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    let mut trailer = [0_u8; TRAILER_BYTES as usize];
    journal
        .read_exact(&mut trailer)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    let name_length = u64::from(decode_be_u32(&trailer[..4]));
    if trailer[8..] != *journal_magic || name_length == 0 {
        return Ok(());
    }
    let Some(name_offset) = journal_length
        .checked_sub(TRAILER_BYTES)
        .and_then(|offset| offset.checked_sub(name_length))
    else {
        return Ok(());
    };
    let name_length = usize::try_from(name_length)
        .map_err(|_| SessionStoreError::InvalidPersistedValue("rollback_journal.super_journal"))?;
    if name_length > MAX_PLATFORM_PATH_BYTES {
        return Err(SessionStoreError::InvalidPersistedValue(
            "rollback_journal.super_journal",
        ));
    }
    journal
        .seek(SeekFrom::Start(name_offset))
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    let mut name = vec![0_u8; name_length];
    journal
        .read_exact(&mut name)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    let expected_checksum = decode_be_u32(&trailer[4..8]);
    let unsigned_checksum = name.iter().fold(0_u32, |checksum, byte| {
        checksum.wrapping_add(u32::from(*byte))
    });
    let signed_checksum = name.iter().fold(0_u32, |checksum, byte| {
        checksum.wrapping_add((*byte as i8 as i32) as u32)
    });
    if expected_checksum == unsigned_checksum || expected_checksum == signed_checksum {
        return Err(SessionStoreError::InvalidPersistedValue(
            "rollback_journal.super_journal",
        ));
    }
    Ok(())
}

fn rollback_journal_checksum(page: &[u8], seed: u32) -> u32 {
    let mut checksum = seed;
    let mut index = page.len().saturating_sub(200);
    while index > 0 {
        checksum = checksum.wrapping_add(u32::from(page[index]));
        index = index.saturating_sub(200);
    }
    checksum
}

fn round_up_to_sector(offset: u64, sector_size: u64) -> Result<u64, SessionStoreError> {
    if offset == 0 {
        return Ok(0);
    }
    offset
        .checked_sub(1)
        .and_then(|value| value.checked_div(sector_size))
        .and_then(|value| value.checked_add(1))
        .and_then(|value| value.checked_mul(sector_size))
        .ok_or(SessionStoreError::InvalidPersistedValue(
            "rollback_journal.offset",
        ))
}

fn decode_database_header(header: &[u8; 100]) -> Result<(usize, u32), SessionStoreError> {
    const DATABASE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
    if &header[..DATABASE_MAGIC.len()] != DATABASE_MAGIC {
        return Err(SessionStoreError::InvalidPersistedValue("database.header"));
    }
    Ok((
        decode_database_page_size(&header[16..18])?,
        decode_be_u32(&header[60..64]),
    ))
}

fn inspect_wal_database_header(
    database_path: &Path,
    main_header: Option<[u8; 100]>,
) -> Result<Option<[u8; 100]>, SessionStoreError> {
    const WAL_HEADER_BYTES: u64 = 32;
    const WAL_FRAME_HEADER_BYTES: u64 = 24;
    const WAL_MAGIC_LITTLE_CHECKSUM: u32 = 0x377f_0682;
    const WAL_MAGIC_BIG_CHECKSUM: u32 = 0x377f_0683;
    const WAL_FORMAT_VERSION: u32 = 3_007_000;

    let wal_path = sqlite_sidecar_path(database_path, "-wal");
    if !path_entry_exists(&wal_path)? {
        return Ok(main_header);
    }
    validate_regular_artifact(&wal_path)?;
    let wal_length = fs::metadata(&wal_path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?
        .len();
    if wal_length < WAL_HEADER_BYTES {
        return Ok(main_header);
    }
    let mut wal = File::open(&wal_path)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    let mut header = [0_u8; WAL_HEADER_BYTES as usize];
    wal.read_exact(&mut header)
        .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
    let checksum_big_endian = match decode_be_u32(&header[0..4]) {
        WAL_MAGIC_LITTLE_CHECKSUM => false,
        WAL_MAGIC_BIG_CHECKSUM => true,
        _ => return Ok(main_header),
    };
    let mut checksum = (0_u32, 0_u32);
    wal_checksum(&header[..24], checksum_big_endian, &mut checksum);
    if checksum.0 != decode_be_u32(&header[24..28]) || checksum.1 != decode_be_u32(&header[28..32])
    {
        return Ok(main_header);
    }
    if decode_be_u32(&header[4..8]) != WAL_FORMAT_VERSION {
        return Err(SessionStoreError::InvalidPersistedValue(
            "wal.format_version",
        ));
    }
    let wal_page_size = usize::try_from(decode_be_u32(&header[8..12]))
        .map_err(|_| SessionStoreError::InvalidPersistedValue("wal.page_size"))?;
    if !(512..=65_536).contains(&wal_page_size) || !wal_page_size.is_power_of_two() {
        return Err(SessionStoreError::InvalidPersistedValue("wal.page_size"));
    }
    if main_header
        .as_ref()
        .map(decode_database_header)
        .transpose()?
        .is_some_and(|(database_page_size, _)| wal_page_size != database_page_size)
    {
        return Err(SessionStoreError::InvalidPersistedValue("wal.page_size"));
    }
    let salt_1 = [header[16], header[17], header[18], header[19]];
    let salt_2 = [header[20], header[21], header[22], header[23]];
    let frame_bytes = WAL_FRAME_HEADER_BYTES
        .checked_add(
            u64::try_from(wal_page_size)
                .map_err(|_| SessionStoreError::InvalidPersistedValue("wal.page_size"))?,
        )
        .ok_or(SessionStoreError::InvalidPersistedValue("wal.frame_size"))?;
    let mut frame_header = [0_u8; WAL_FRAME_HEADER_BYTES as usize];
    let mut page = vec![0_u8; wal_page_size];
    let mut offset = WAL_HEADER_BYTES;
    let mut committed_header = main_header;
    let mut pending_header = main_header;
    while offset
        .checked_add(frame_bytes)
        .is_some_and(|end| end <= wal_length)
    {
        wal.read_exact(&mut frame_header)
            .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
        wal.read_exact(&mut page)
            .map_err(|error| session_io_error(SessionIoOperation::InspectPath, error))?;
        if frame_header[8..12] != salt_1 || frame_header[12..16] != salt_2 {
            break;
        }
        if decode_be_u32(&frame_header[0..4]) == 0 {
            break;
        }
        wal_checksum(&frame_header[..8], checksum_big_endian, &mut checksum);
        wal_checksum(&page, checksum_big_endian, &mut checksum);
        if checksum.0 != decode_be_u32(&frame_header[16..20])
            || checksum.1 != decode_be_u32(&frame_header[20..24])
        {
            break;
        }
        if decode_be_u32(&frame_header[0..4]) == 1 {
            let mut header = [0_u8; 100];
            header.copy_from_slice(&page[..100]);
            pending_header = Some(header);
        }
        if decode_be_u32(&frame_header[4..8]) != 0 {
            committed_header = pending_header;
        }
        offset += frame_bytes;
    }
    Ok(committed_header)
}

fn decode_database_page_size(bytes: &[u8]) -> Result<usize, SessionStoreError> {
    let encoded = u16::from_be_bytes([bytes[0], bytes[1]]);
    let page_size = if encoded == 1 {
        65_536
    } else {
        usize::from(encoded)
    };
    if (512..=65_536).contains(&page_size) && page_size.is_power_of_two() {
        Ok(page_size)
    } else {
        Err(SessionStoreError::InvalidPersistedValue(
            "database.page_size",
        ))
    }
}

fn decode_be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn wal_checksum(bytes: &[u8], big_endian: bool, checksum: &mut (u32, u32)) {
    debug_assert_eq!(bytes.len() % 8, 0);
    for words in bytes.chunks_exact(8) {
        let first = if big_endian {
            decode_be_u32(&words[..4])
        } else {
            u32::from_le_bytes([words[0], words[1], words[2], words[3]])
        };
        let second = if big_endian {
            decode_be_u32(&words[4..8])
        } else {
            u32::from_le_bytes([words[4], words[5], words[6], words[7]])
        };
        checksum.0 = checksum.0.wrapping_add(first).wrapping_add(checksum.1);
        checksum.1 = checksum.1.wrapping_add(second).wrapping_add(checksum.0);
    }
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
            Ok(mode)
                if mode.eq_ignore_ascii_case("wal") && probe_journal_write(connection).is_ok() =>
            {
                SessionJournalMode::Wal
            }
            Ok(_) | Err(_) => {
                let _ = connection.execute_batch("ROLLBACK");
                let mode =
                    connection.pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                        row.get::<_, String>(0)
                    })?;
                if !mode.eq_ignore_ascii_case("delete") {
                    return Err(SessionStoreError::InvalidPersistedValue("journal_mode"));
                }
                probe_journal_write(connection)?;
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
        probe_journal_write(connection)?;
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

fn probe_journal_write(connection: &Connection) -> Result<(), rusqlite::Error> {
    let user_version: u32 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    connection.execute_batch("BEGIN IMMEDIATE")?;
    let write = connection.pragma_update(None, "user_version", user_version);
    let rollback = connection.execute_batch("ROLLBACK");
    write?;
    rollback
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
    validate_schema_version(connection, SESSION_SCHEMA_VERSION, SESSION_SCHEMA_OBJECTS)
}

fn validate_schema_version(
    connection: &Connection,
    version: u32,
    objects: &[SessionSchemaObject],
) -> Result<(), SessionStoreError> {
    if read_user_version(connection)? != version {
        return Err(SessionStoreError::SchemaMismatch("user_version"));
    }
    let mut found = HashSet::new();
    let mut statement = connection.prepare(
        "SELECT type, name, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name",
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
        let expected = objects
            .iter()
            .find(|object| object.kind.code() == kind && object.name == name)
            .ok_or(SessionStoreError::SchemaMismatch("unexpected_object"))?;
        let sql = sql.ok_or(SessionStoreError::SchemaMismatch(expected.name))?;
        if normalize_sql(&sql) != normalize_sql(expected.sql) {
            return Err(SessionStoreError::SchemaMismatch(expected.name));
        }
        found.insert((kind, name));
    }
    if found.len() != objects.len() {
        return Err(SessionStoreError::SchemaMismatch("missing_object"));
    }
    Ok(())
}

fn validate_integrity(connection: &Connection) -> Result<(), SessionStoreError> {
    let mut statement = connection.prepare("PRAGMA integrity_check")?;
    let mut rows = statement.query([])?;
    let first = rows
        .next()?
        .ok_or(SessionStoreError::IntegrityCheckFailed)?
        .get::<_, String>(0)?;
    if first != "ok" || rows.next()?.is_some() {
        return Err(SessionStoreError::IntegrityCheckFailed);
    }
    Ok(())
}

fn validate_persisted_semantics(connection: &Connection) -> Result<(), SessionStoreError> {
    validate_dense_queues(connection)?;
    let session_rows: i64 =
        connection.query_row("SELECT COUNT(*) FROM session", [], |row| row.get(0))?;
    if !(0..=1).contains(&session_rows) {
        return Err(SessionStoreError::InvalidPersistedValue(
            "multiple_session_rows",
        ));
    }
    let foreign_key_failures: i64 =
        connection.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_failures != 0 {
        return Err(SessionStoreError::InvalidPersistedValue(
            "foreign_key_check",
        ));
    }
    read_task_records(connection)?;
    bt::validate_rows(connection)?;
    validate_task_sources(connection)?;
    validate_stopped_result_pairing(connection)?;
    read_stopped_results(connection)?;
    validate_host_key_challenges(connection)?;
    for intent in read_journal_installs(connection)? {
        let current = connection
            .query_row(
                "SELECT primary_journal_id, primary_journal_path FROM task WHERE gid = ?1",
                [intent.gid.to_string()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
            .ok_or(SessionStoreError::InvalidPersistedValue(
                "journal_install.task",
            ))?;
        let current_id = decode_journal_id(&current.0, "task.primary_journal_id")?;
        let current_path = decode_platform_path(&current.1, "task.primary_journal_path")?;
        let expected = match intent.phase {
            JournalInstallPhase::Installing => (intent.old_journal_id, &intent.old_path),
            JournalInstallPhase::Installed => (intent.new_journal_id, &intent.new_path),
        };
        if current_id != expected.0 || current_path != *expected.1 {
            return Err(SessionStoreError::JournalPointerMismatch);
        }
    }
    Ok(())
}

fn validate_host_key_challenges(connection: &Connection) -> Result<(), SessionStoreError> {
    validate_host_key_challenges_with_budget(connection, SESSION_TASK_READ_BUDGET_BYTES)
}

fn validate_host_key_challenges_with_budget(
    connection: &Connection,
    read_budget_bytes: usize,
) -> Result<(), SessionStoreError> {
    let count: i64 =
        connection.query_row("SELECT COUNT(*) FROM host_key_challenge", [], |row| {
            row.get(0)
        })?;
    let count = bounded_count(count, SESSION_MAX_TASKS, "host_key_challenge.count")?;
    let mut statement = connection.prepare(
        "SELECT CAST(challenge.canonical_host AS BLOB), challenge.port,
                CAST(challenge.algorithm AS BLOB), challenge.presented_public_key,
                challenge.fingerprint_sha256, task.queue_state
         FROM host_key_challenge AS challenge
         JOIN task ON task.gid = challenge.gid
         ORDER BY challenge.gid",
    )?;
    let mut rows = statement.query([])?;
    let mut rows_read = 0_usize;
    let mut bytes_read = 0_usize;
    while let Some(row) = rows.next()? {
        let canonical_host = row.get::<_, Vec<u8>>(0)?;
        let port = row.get::<_, i64>(1)?;
        let algorithm = row.get::<_, Vec<u8>>(2)?;
        let presented_public_key = row.get::<_, Vec<u8>>(3)?;
        let fingerprint_sha256 = row.get::<_, Vec<u8>>(4)?;
        let queue_state = SessionQueueState::try_from(row.get::<_, i64>(5)?)?;
        rows_read = rows_read
            .checked_add(1)
            .ok_or(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.count",
            ))?;
        let row_bytes = std::mem::size_of::<SessionHostKeyChallengeRecord>()
            .checked_add(canonical_host.len())
            .and_then(|size| size.checked_add(algorithm.len()))
            .and_then(|size| size.checked_add(presented_public_key.len()))
            .and_then(|size| size.checked_add(fingerprint_sha256.len()))
            .ok_or(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.read_budget",
            ))?;
        bytes_read =
            bytes_read
                .checked_add(row_bytes)
                .ok_or(SessionStoreError::InvalidPersistedValue(
                    "host_key_challenge.read_budget",
                ))?;
        if bytes_read > read_budget_bytes {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.read_budget",
            ));
        }

        if canonical_host.is_empty()
            || canonical_host.len() > 253
            || std::str::from_utf8(&canonical_host).is_err()
        {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.canonical_host",
            ));
        }
        if !(1..=i64::from(u16::MAX)).contains(&port) {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.port",
            ));
        }
        if algorithm.len() > SESSION_MAX_ALGORITHM_BYTES
            || presented_public_key.len() > SESSION_MAX_HOST_KEY_BYTES
        {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge_bounds",
            ));
        }
        if algorithm.is_empty() || std::str::from_utf8(&algorithm).is_err() {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.algorithm",
            ));
        }
        if presented_public_key.is_empty() {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.presented_public_key",
            ));
        }
        let fingerprint_sha256: [u8; 32] = fingerprint_sha256.try_into().map_err(|_| {
            SessionStoreError::InvalidPersistedValue("host_key_challenge.fingerprint_sha256")
        })?;
        if HostKeyFingerprint::new(fingerprint_sha256)
            != HostKeyFingerprint::for_presented_key(&presented_public_key)
        {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.fingerprint_sha256",
            ));
        }
        if queue_state != SessionQueueState::Paused {
            return Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.queue_state",
            ));
        }
    }
    if rows_read != count {
        return Err(SessionStoreError::InvalidPersistedValue(
            "host_key_challenge.count",
        ));
    }
    Ok(())
}

fn normalize_sql(sql: &str) -> String {
    sql.trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn count_schema_objects(connection: &Connection) -> Result<u32, SessionStoreError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
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
    validate_slow_slot_state(
        task.queue_state,
        task.desired_paused,
        task.slow_demotion_count,
        task.slow_slot.as_ref(),
    )?;
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

fn validate_stopped_result(result: &SessionStoppedResultRecord) -> Result<(), SessionStoreError> {
    time_to_i64(result.completed_ms, "stopped_result.completed_ms")?;
    if result.safe_message.len() > SESSION_MAX_SAFE_MESSAGE_BYTES {
        return Err(SessionStoreError::InvalidRecord(
            "stopped_result.safe_message",
        ));
    }
    match result.status {
        SessionTerminalStatus::Complete
            if result.error_kind.is_none()
                && result.safe_message.is_empty()
                && result.total_length.is_some()
                && result.layout_hash.is_some() =>
        {
            Ok(())
        }
        SessionTerminalStatus::Complete => Err(SessionStoreError::InvalidRecord(
            "stopped_result.complete_payload",
        )),
        SessionTerminalStatus::Error if result.error_kind.is_none() => Err(
            SessionStoreError::InvalidRecord("stopped_result.error_code"),
        ),
        SessionTerminalStatus::Error
            if result.total_length.is_some() || result.layout_hash.is_some() =>
        {
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.error_payload",
            ))
        }
        SessionTerminalStatus::Error => Ok(()),
        SessionTerminalStatus::Removed
            if result.error_kind.is_none()
                && result.safe_message.is_empty()
                && result.total_length.is_none()
                && result.layout_hash.is_none() =>
        {
            Ok(())
        }
        SessionTerminalStatus::Removed => Err(SessionStoreError::InvalidRecord(
            "stopped_result.non_error_payload",
        )),
    }
}

fn validate_stopped_result_pairing(connection: &Connection) -> Result<(), SessionStoreError> {
    let orphaned_results: i64 = connection.query_row(
        "SELECT COUNT(*)
         FROM stopped_result AS result
         LEFT JOIN task ON task.gid = result.gid
         WHERE task.gid IS NULL OR task.queue_state != ?1",
        [SessionQueueState::Stopped as i64],
        |row| row.get(0),
    )?;
    let missing_results: i64 = connection.query_row(
        "SELECT COUNT(*)
         FROM task
         LEFT JOIN stopped_result AS result ON result.gid = task.gid
         WHERE task.queue_state = ?1 AND result.gid IS NULL",
        [SessionQueueState::Stopped as i64],
        |row| row.get(0),
    )?;
    if orphaned_results == 0 && missing_results == 0 {
        Ok(())
    } else {
        Err(SessionStoreError::InvalidPersistedValue(
            "stopped_result.task_pair",
        ))
    }
}

fn validate_slow_slot_state(
    queue_state: SessionQueueState,
    desired_paused: bool,
    slow_demotion_count: u32,
    slow_slot: Option<&SessionSlowSlotState>,
) -> Result<(), SessionStoreError> {
    if (queue_state == SessionQueueState::Demoted) != slow_slot.is_some() {
        return Err(SessionStoreError::InvalidRecord("slow_slot.queue_state"));
    }
    if queue_state == SessionQueueState::Demoted && desired_paused {
        return Err(SessionStoreError::InvalidRecord("slow_slot.desired_paused"));
    }
    if slow_slot.is_some_and(|value| value.original_position as usize >= SESSION_MAX_TASKS) {
        return Err(SessionStoreError::InvalidRecord(
            "slow_slot.original_position",
        ));
    }
    if queue_state == SessionQueueState::Demoted && slow_demotion_count == 0 {
        return Err(SessionStoreError::InvalidRecord("slow_slot.demotion_count"));
    }
    if let Some(retry) = slow_slot.and_then(|value| value.retry.as_ref()) {
        time_to_i64(retry.scheduled_at_ms, "task.slow_retry_scheduled_at_ms")?;
        if retry.delay_ms == 0 {
            return Err(SessionStoreError::InvalidRecord("slow_slot.retry.delay_ms"));
        }
    }
    Ok(())
}

fn validate_install_intent(intent: &JournalInstallIntent) -> Result<(), SessionStoreError> {
    if intent.phase != JournalInstallPhase::Installing {
        return Err(SessionStoreError::InvalidRecord("journal_install.phase"));
    }
    validate_install_values(intent)
}

fn validate_install_values(intent: &JournalInstallIntent) -> Result<(), SessionStoreError> {
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
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM task", [], |row| row.get(0))?;
    let maximum = bounded_count(count, SESSION_MAX_TASKS, "task.count")?;
    let mut statement = connection.prepare(
        "SELECT queue_state, queue_position FROM task ORDER BY queue_state, queue_position, gid",
    )?;
    let mut rows = statement.query([])?;
    let mut next = BTreeMap::<i64, i64>::new();
    let mut seen = 0_usize;
    while let Some(row) = rows.next()? {
        let state = row.get::<_, i64>(0)?;
        let position = row.get::<_, i64>(1)?;
        SessionQueueState::try_from(state)?;
        let expected = next.entry(state).or_insert(0);
        if position != *expected {
            return Err(SessionStoreError::QueueInvariant);
        }
        *expected += 1;
        seen += 1;
    }
    if seen == maximum {
        Ok(())
    } else {
        Err(SessionStoreError::InvalidPersistedValue("task.count"))
    }
}

fn validate_time_order(created_ms: u64, updated_ms: u64) -> Result<(), SessionStoreError> {
    if updated_ms < created_ms {
        Err(SessionStoreError::InvalidRecord("updated_before_created"))
    } else {
        Ok(())
    }
}

fn bounded_count(
    count: i64,
    maximum: usize,
    field: &'static str,
) -> Result<usize, SessionStoreError> {
    let count =
        usize::try_from(count).map_err(|_| SessionStoreError::InvalidPersistedValue(field))?;
    if count > maximum {
        Err(SessionStoreError::InvalidPersistedValue(field))
    } else {
        Ok(count)
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

fn native_error_kind(error: &NativeCapabilityError) -> io::ErrorKind {
    match error {
        NativeCapabilityError::Io(error) => error.kind(),
        NativeCapabilityError::UnsupportedPlatform | NativeCapabilityError::SafeOpenUnavailable => {
            io::ErrorKind::Unsupported
        }
        NativeCapabilityError::InvalidAbsolutePath
        | NativeCapabilityError::UnsafePathComponent
        | NativeCapabilityError::PlatformPathMismatch
        | NativeCapabilityError::TooManyAllowedRoots => io::ErrorKind::InvalidInput,
        NativeCapabilityError::OutsideAllowedRoot
        | NativeCapabilityError::ObjectKindMismatch { .. }
        | NativeCapabilityError::HardLinkAlias
        | NativeCapabilityError::Identity(_)
        | NativeCapabilityError::IdentityMismatch => io::ErrorKind::PermissionDenied,
    }
}

fn session_capability_error(
    operation: SessionIoOperation,
    error: NativeCapabilityError,
) -> SessionStoreError {
    session_io_error(
        operation,
        io::Error::new(native_error_kind(&error), error.to_string()),
    )
}

#[cfg(test)]
fn backup_test_crash(phase: &str) {
    let Some(configured) = std::env::var_os("ARIAX_BACKUP_CRASH_PHASE") else {
        return;
    };
    if configured == phase {
        let code = match phase {
            "after_link" => 121,
            "after_link_sync" => 122,
            "after_unlink" => 123,
            _ => 124,
        };
        std::process::exit(code);
    }
}

#[cfg(not(test))]
fn backup_test_crash(_phase: &str) {}

#[cfg(test)]
mod tests {
    use super::{
        ALL_SESSION_IO_OPERATIONS, ALL_SESSION_SQLITE_LIMITS, ALL_SESSION_STORE_ERROR_CODES,
        JournalInstallIntent, JournalInstallPhase, JournalInstallToken, SESSION_SCHEMA_OBJECTS,
        SESSION_SCHEMA_VERSION, SessionCacheReconciliation, SessionHostKeyChallengeRecord,
        SessionHostKeyResolution, SessionId, SessionJournalCache, SessionJournalMode,
        SessionNoSpaceCondition, SessionQueueOrder, SessionQueueState, SessionQueueTransition,
        SessionRecord, SessionSlowRetryDecision, SessionSlowSlotState, SessionStoppedResultRecord,
        SessionStore, SessionStoreConfig, SessionStoreError, SessionTaskRecord,
        SessionTaskSourceRecord, SessionTerminalStatus,
    };
    use super::{
        IMPORT_CRASH_POINT, SESSION_IMPORT_MAX_BYTES, SESSION_MAX_IMPORT_TASKS, SessionTaskMetadata,
    };
    use crate::{
        CheckpointId, JournalHash, JournalId, OptionsSnapshotScope, PathPlatform, PlatformPath,
        SanitizedOptionMap,
    };
    use ariax_config::{SecurityClass, builtin_registry};
    use ariax_core::{ErrorKind, Gid, HostKeyChallengeId, HostKeyFingerprint};
    use rusqlite::Connection;
    use std::collections::{BTreeMap, HashSet};
    use std::ffi::OsString;
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::process::Command;
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
            #[cfg(unix)]
            {
                fs::create_dir_all(&path).expect("create test directory");
                super::tighten_directory_permissions(&path).expect("secure test directory");
            }
            #[cfg(windows)]
            super::create_private_directory(&path).expect("create private test directory");
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

    #[derive(Debug, Eq, PartialEq)]
    struct DirectorySnapshot {
        entries: Vec<OsString>,
        contents: BTreeMap<OsString, Vec<u8>>,
        #[cfg(unix)]
        modes: BTreeMap<OsString, u32>,
    }

    fn directory_entry_names(path: &Path) -> Vec<OsString> {
        let mut entries = fs::read_dir(path)
            .expect("read snapshot directory")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect::<Vec<_>>();
        entries.sort();
        entries
    }

    fn snapshot_directory(path: &Path) -> DirectorySnapshot {
        let entries = directory_entry_names(path);
        let mut contents = BTreeMap::new();
        #[cfg(unix)]
        let mut modes = BTreeMap::new();
        for name in &entries {
            let artifact = path.join(name);
            let metadata = fs::symlink_metadata(&artifact).expect("artifact metadata");
            if metadata.is_file() {
                contents.insert(name.clone(), fs::read(&artifact).expect("artifact bytes"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                modes.insert(name.clone(), metadata.permissions().mode() & 0o7777);
            }
        }
        DirectorySnapshot {
            entries,
            contents,
            #[cfg(unix)]
            modes,
        }
    }

    fn synthetic_wal(
        database: &Path,
        checksum_big_endian: bool,
        frames: &[(u32, bool)],
    ) -> Vec<u8> {
        let frames = frames
            .iter()
            .map(|(version, committed)| (1_u32, *version, *committed))
            .collect::<Vec<_>>();
        synthetic_wal_frames(database, checksum_big_endian, &frames)
    }

    fn synthetic_wal_frames(
        database: &Path,
        checksum_big_endian: bool,
        frames: &[(u32, u32, bool)],
    ) -> Vec<u8> {
        let database_bytes = fs::read(database).expect("database bytes");
        let page_size =
            super::decode_database_page_size(&database_bytes[16..18]).expect("database page size");
        let page_count = u32::try_from(database_bytes.len() / page_size).expect("page count");
        let page = &database_bytes[..page_size];
        let mut output = vec![0_u8; 32];
        output[0..4].copy_from_slice(
            &(if checksum_big_endian {
                0x377f_0683_u32
            } else {
                0x377f_0682_u32
            })
            .to_be_bytes(),
        );
        output[4..8].copy_from_slice(&3_007_000_u32.to_be_bytes());
        output[8..12].copy_from_slice(
            &u32::try_from(page_size)
                .expect("page size u32")
                .to_be_bytes(),
        );
        output[16..20].copy_from_slice(&0x1122_3344_u32.to_be_bytes());
        output[20..24].copy_from_slice(&0x5566_7788_u32.to_be_bytes());
        let mut checksum = (0_u32, 0_u32);
        super::wal_checksum(&output[..24], checksum_big_endian, &mut checksum);
        output[24..28].copy_from_slice(&checksum.0.to_be_bytes());
        output[28..32].copy_from_slice(&checksum.1.to_be_bytes());
        for (page_number, version, committed) in frames {
            let mut frame_header = [0_u8; 24];
            frame_header[0..4].copy_from_slice(&page_number.to_be_bytes());
            frame_header[4..8]
                .copy_from_slice(&(if *committed { page_count } else { 0 }).to_be_bytes());
            frame_header[8..12].copy_from_slice(&output[16..20]);
            frame_header[12..16].copy_from_slice(&output[20..24]);
            let mut frame_page = page.to_vec();
            frame_page[60..64].copy_from_slice(&version.to_be_bytes());
            super::wal_checksum(&frame_header[..8], checksum_big_endian, &mut checksum);
            super::wal_checksum(&frame_page, checksum_big_endian, &mut checksum);
            frame_header[16..20].copy_from_slice(&checksum.0.to_be_bytes());
            frame_header[20..24].copy_from_slice(&checksum.1.to_be_bytes());
            output.extend_from_slice(&frame_header);
            output.extend_from_slice(&frame_page);
        }
        output
    }

    fn write_wal(database: &Path, bytes: &[u8]) {
        let wal = super::sqlite_sidecar_path(database, "-wal");
        let mut file = fs::File::create(wal).expect("create synthetic WAL");
        file.write_all(bytes).expect("write synthetic WAL");
        file.sync_all().expect("sync synthetic WAL");
    }

    fn recompute_wal_header_checksum(wal: &mut [u8], checksum_big_endian: bool) {
        let mut checksum = (0_u32, 0_u32);
        super::wal_checksum(&wal[..24], checksum_big_endian, &mut checksum);
        wal[24..28].copy_from_slice(&checksum.0.to_be_bytes());
        wal[28..32].copy_from_slice(&checksum.1.to_be_bytes());
    }

    fn set_wal_format_version(wal: &mut [u8], version: u32, checksum_big_endian: bool) {
        wal[4..8].copy_from_slice(&version.to_be_bytes());
        recompute_wal_header_checksum(wal, checksum_big_endian);
    }

    fn synthetic_rollback_journal(
        database: &Path,
        original_database_pages: u32,
        records: &[(u32, u32, bool)],
    ) -> Vec<u8> {
        const JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
        const SECTOR_SIZE: usize = 512;
        const CHECKSUM_SEED: u32 = 0x1234_5678;

        let database_bytes = fs::read(database).expect("database bytes");
        let page_size =
            super::decode_database_page_size(&database_bytes[16..18]).expect("database page size");
        let mut output = vec![0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(&JOURNAL_MAGIC);
        output[8..12].copy_from_slice(
            &u32::try_from(records.len())
                .expect("record count")
                .to_be_bytes(),
        );
        output[12..16].copy_from_slice(&CHECKSUM_SEED.to_be_bytes());
        output[16..20].copy_from_slice(&original_database_pages.to_be_bytes());
        output[20..24].copy_from_slice(&(SECTOR_SIZE as u32).to_be_bytes());
        output[24..28].copy_from_slice(
            &u32::try_from(page_size)
                .expect("page size u32")
                .to_be_bytes(),
        );
        for (page_number, version, valid_checksum) in records {
            let mut page = database_bytes[..page_size].to_vec();
            page[60..64].copy_from_slice(&version.to_be_bytes());
            output.extend_from_slice(&page_number.to_be_bytes());
            output.extend_from_slice(&page);
            let mut checksum = super::rollback_journal_checksum(&page, CHECKSUM_SEED);
            if !valid_checksum {
                checksum ^= 1;
            }
            output.extend_from_slice(&checksum.to_be_bytes());
        }
        output
    }

    fn append_super_journal_trailer(
        journal: &mut Vec<u8>,
        marker: u32,
        name: &[u8],
        valid_checksum: bool,
    ) {
        const JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
        journal.extend_from_slice(&marker.to_be_bytes());
        journal.extend_from_slice(name);
        journal.extend_from_slice(
            &u32::try_from(name.len())
                .expect("super-journal name length")
                .to_be_bytes(),
        );
        let mut checksum = name
            .iter()
            .fold(0_u32, |value, byte| value.wrapping_add(u32::from(*byte)));
        if !valid_checksum {
            checksum ^= 1;
        }
        journal.extend_from_slice(&checksum.to_be_bytes());
        journal.extend_from_slice(&JOURNAL_MAGIC);
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
            slow_demotion_count: 0,
            slow_slot: None,
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

    fn stopped_result_record(
        gid: Gid,
        status: SessionTerminalStatus,
    ) -> SessionStoppedResultRecord {
        let (error_kind, safe_message, total_length, layout_hash) = match status {
            SessionTerminalStatus::Complete => (None, String::new(), Some(u64::MAX), Some(hash(9))),
            SessionTerminalStatus::Error => (
                Some(ErrorKind::Network),
                "network failure".to_owned(),
                None,
                None,
            ),
            SessionTerminalStatus::Removed => (None, String::new(), None, None),
        };
        SessionStoppedResultRecord {
            gid,
            status,
            error_kind,
            safe_message,
            total_length,
            layout_hash,
            completed_ms: 300,
        }
    }

    fn open_store(directory: &TestDirectory) -> SessionStore {
        let mut store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("open store");
        store.put_session(&session_record()).expect("put session");
        store
    }

    fn seed_raw_host_key_challenge(
        directory: &TestDirectory,
        canonical_host: &[u8],
        algorithm: &[u8],
        fingerprint_sha256: &[u8],
    ) {
        let connection = Connection::open(directory.database()).expect("open host-key database");
        connection
            .execute(
                "UPDATE task SET queue_state = ?1 WHERE gid = ?2",
                rusqlite::params![SessionQueueState::Paused as i64, gid(1).to_string()],
            )
            .expect("pause host-key task");
        connection
            .execute(
                "INSERT INTO host_key_challenge(
                     gid, challenge_id, canonical_host, port, algorithm,
                     presented_public_key, fingerprint_sha256, created_ms
                 ) VALUES (?1, ?2, CAST(?3 AS TEXT), ?4, CAST(?5 AS TEXT), ?6, ?7, ?8)",
                rusqlite::params![
                    gid(1).to_string(),
                    [1_u8; 16].as_slice(),
                    canonical_host,
                    22_i64,
                    algorithm,
                    [7_u8; 32].as_slice(),
                    fingerprint_sha256,
                    200_i64,
                ],
            )
            .expect("insert raw host-key challenge");
    }

    fn seed_owner_lock(database: &Path) {
        drop(super::acquire_session_owner_lock(database).expect("seed owner lock"));
    }

    fn copy_fixture_file(source: &Path, destination: &Path) {
        let expected_bytes = fs::metadata(source).expect("source fixture metadata").len();
        let mut source_file = fs::File::open(source).expect("open source fixture");
        let mut destination_file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .expect("create destination fixture");
        let mut copied_bytes = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let bytes_read = source_file.read(&mut buffer).expect("read fixture bytes");
            if bytes_read == 0 {
                break;
            }
            destination_file
                .write_all(&buffer[..bytes_read])
                .expect("write fixture bytes");
            copied_bytes += u64::try_from(bytes_read).expect("fixture byte count");
        }
        assert_eq!(copied_bytes, expected_bytes);
        destination_file
            .sync_all()
            .expect("sync destination fixture");
        drop(destination_file);
        super::tighten_database_permissions(destination).expect("secure destination fixture");
    }

    fn copy_hot_rollback_fixture(source: &Path, destination: &Path) {
        copy_fixture_file(source, destination);
        copy_fixture_file(
            &super::sqlite_sidecar_path(source, "-journal"),
            &super::sqlite_sidecar_path(destination, "-journal"),
        );
    }

    #[derive(Clone, Copy, Debug)]
    enum MainHeaderCorruption {
        Magic,
        PageSize,
        UserVersion,
    }

    fn corrupt_main_header(database: &Path, corruption: MainHeaderCorruption) {
        let (offset, bytes): (u64, &[u8]) = match corruption {
            MainHeaderCorruption::Magic => (0, &[0_u8; 16]),
            MainHeaderCorruption::PageSize => (16, &[0_u8; 2]),
            MainHeaderCorruption::UserVersion => (60, &[0_u8, 0, 0, 2]),
        };
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(database)
            .expect("open main database for corruption");
        file.seek(SeekFrom::Start(offset))
            .expect("seek main header");
        file.write_all(bytes).expect("corrupt main header");
        file.sync_all().expect("sync main corruption");
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
    fn unpaired_stopped_tasks_are_rejected_in_v3() {
        let current = TestDirectory::new();
        let mut store = open_store(&current);
        store
            .put_task(&task_record(gid(1), 0))
            .expect("current task");
        store
            .connection
            .execute(
                "UPDATE task SET queue_state = ?1 WHERE gid = ?2",
                rusqlite::params![SessionQueueState::Stopped as i64, gid(1).to_string()],
            )
            .expect("seed unpaired current stopped task");
        assert!(matches!(
            store.tasks(),
            Err(SessionStoreError::InvalidPersistedValue(
                "stopped_result.task_pair"
            ))
        ));
    }

    #[test]
    fn current_schema_rejects_inconsistent_host_key_challenges_before_reopen() {
        let key = [7_u8; 32];
        let valid_fingerprint = HostKeyFingerprint::for_presented_key(&key);
        for (canonical_host, algorithm, fingerprint, expected_field) in [
            (
                b"example.test".as_slice(),
                b"ssh-ed25519".as_slice(),
                [9_u8; 32].as_slice(),
                "host_key_challenge.fingerprint_sha256",
            ),
            (
                [0xff_u8].as_slice(),
                b"ssh-ed25519".as_slice(),
                valid_fingerprint.as_bytes().as_slice(),
                "host_key_challenge.canonical_host",
            ),
            (
                b"example.test".as_slice(),
                [0xff_u8].as_slice(),
                valid_fingerprint.as_bytes().as_slice(),
                "host_key_challenge.algorithm",
            ),
        ] {
            let directory = TestDirectory::new();
            let mut store = open_store(&directory);
            store.put_task(&task_record(gid(1), 0)).expect("task");
            drop(store);
            seed_raw_host_key_challenge(&directory, canonical_host, algorithm, fingerprint);
            super::tighten_sqlite_artifact_permissions(&directory.database())
                .expect("normalize fixture permissions");
            let before = snapshot_directory(directory.path());
            let result = SessionStore::open(directory.database(), SessionStoreConfig::default());
            assert!(matches!(
                result,
                Err(SessionStoreError::InvalidPersistedValue(field)) if field == expected_field
            ));
            assert_eq!(snapshot_directory(directory.path()), before);
        }
    }

    #[test]
    fn host_key_challenges_require_paused_tasks_in_v3() {
        let key = [7_u8; 32];
        let fingerprint = HostKeyFingerprint::for_presented_key(&key);
        let current = TestDirectory::new();
        let mut store = open_store(&current);
        store
            .put_task(&task_record(gid(1), 0))
            .expect("current task");
        drop(store);
        seed_raw_host_key_challenge(
            &current,
            b"example.test",
            b"ssh-ed25519",
            fingerprint.as_bytes(),
        );
        let connection =
            Connection::open(current.database()).expect("open current host-key database");
        connection
            .execute(
                "UPDATE task SET queue_state = ?1 WHERE gid = ?2",
                rusqlite::params![SessionQueueState::Waiting as i64, gid(1).to_string()],
            )
            .expect("make current host-key task non-paused");
        drop(connection);

        assert!(matches!(
            SessionStore::open(current.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.queue_state"
            ))
        ));
        let connection =
            Connection::open(current.database()).expect("inspect retained current database");
        assert_eq!(
            super::read_user_version(&connection).expect("retained current version"),
            SESSION_SCHEMA_VERSION
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM host_key_challenge", [], |row| row
                    .get::<_, i64>(0))
                .expect("retained current challenge"),
            1
        );
    }

    #[test]
    fn host_key_read_budgets_charge_owned_records_at_the_boundary() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        drop(store);
        let key = [7_u8; 32];
        let fingerprint = HostKeyFingerprint::for_presented_key(&key);
        seed_raw_host_key_challenge(
            &directory,
            b"example.test",
            b"ssh-ed25519",
            fingerprint.as_bytes(),
        );
        let connection = Connection::open(directory.database()).expect("open host-key database");
        let semantic_row_bytes = std::mem::size_of::<SessionHostKeyChallengeRecord>()
            + b"example.test".len()
            + b"ssh-ed25519".len()
            + key.len()
            + fingerprint.as_bytes().len();

        assert!(matches!(
            super::validate_host_key_challenges_with_budget(&connection, semantic_row_bytes - 1),
            Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.read_budget"
            ))
        ));
        super::validate_host_key_challenges_with_budget(&connection, semantic_row_bytes)
            .expect("accept challenge at exact read budget");

        let materialized_row_bytes = semantic_row_bytes
            + gid(1).to_string().len()
            + HostKeyChallengeId::new([1; 16]).as_bytes().len();
        assert!(matches!(
            super::read_host_key_challenge_records(&connection, materialized_row_bytes - 1),
            Err(SessionStoreError::InvalidPersistedValue(
                "host_key_challenge.read_budget"
            ))
        ));
        assert_eq!(
            super::read_host_key_challenge_records(&connection, materialized_row_bytes)
                .expect("materialize challenge at exact read budget")
                .len(),
            1
        );
    }

    #[test]
    fn owner_lock_excludes_other_processes_until_drop() {
        let directory = TestDirectory::new();
        let store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("open owning store");
        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--ignored",
                "--exact",
                "session_store::tests::owner_lock_child",
                "--nocapture",
            ])
            .env("ARIAX_OWNER_LOCK_CHILD", directory.database())
            .status()
            .expect("spawn owner-lock child");
        assert!(status.success());
        drop(store);
        SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("lock released with owner");
    }

    #[test]
    fn owner_lock_drop_unlocks_before_duplicated_handles_close() {
        let directory = TestDirectory::new();
        let owner =
            super::acquire_session_owner_lock(&directory.database()).expect("acquire owner lock");
        let inherited = owner.file.try_clone().expect("duplicate lock handle");

        drop(owner);

        drop(
            super::acquire_session_owner_lock(&directory.database())
                .expect("explicit unlock permits immediate reacquisition"),
        );
        drop(inherited);
    }

    #[test]
    #[ignore = "spawned by owner_lock_excludes_other_processes_until_drop"]
    fn owner_lock_child() {
        let Some(database) = std::env::var_os("ARIAX_OWNER_LOCK_CHILD") else {
            return;
        };
        assert!(matches!(
            SessionStore::open(PathBuf::from(database), SessionStoreConfig::default()),
            Err(SessionStoreError::OwnerLockBusy)
        ));
    }

    #[test]
    fn session_identity_check_and_write_are_one_transaction() {
        let directory = TestDirectory::new();
        let mut store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("open store");
        let first = session_record();
        store.put_session(&first).expect("first session");
        let second = SessionRecord {
            session_id: SessionId::new([2; 16]),
            created_ms: 100,
            updated_ms: 100,
            clean_shutdown: false,
        };
        assert!(matches!(
            store.put_session(&second),
            Err(SessionStoreError::SessionConflict)
        ));
        assert_eq!(store.session().expect("session"), Some(first));
    }

    #[test]
    fn creates_private_directory_tree_and_sqlite_artifacts() {
        let directory = TestDirectory::new();
        let persistence = directory.path().join("nested").join("private");
        let database = persistence.join("session.db");
        let mut store = SessionStore::open(&database, SessionStoreConfig::default())
            .expect("open nested store");
        store.put_session(&session_record()).expect("write session");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [directory.path().join("nested"), persistence.clone()] {
                assert_eq!(
                    fs::metadata(path)
                        .expect("directory metadata")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700
                );
            }
            for path in [
                database.clone(),
                super::sqlite_sidecar_path(&database, "-wal"),
                super::sqlite_sidecar_path(&database, "-shm"),
            ] {
                if path.exists() {
                    assert_eq!(
                        fs::metadata(path)
                            .expect("file metadata")
                            .permissions()
                            .mode()
                            & 0o777,
                        0o600
                    );
                }
            }
        }
        #[cfg(windows)]
        {
            super::verify_private_directory(&persistence).expect("private directory ACL");
            super::verify_private_file_permissions(&database).expect("private database ACL");
            for suffix in ["-wal", "-shm"] {
                let sidecar = super::sqlite_sidecar_path(&database, suffix);
                if sidecar.exists() {
                    super::verify_private_file_permissions(&sidecar).expect("private sidecar ACL");
                }
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn rejects_unsafe_windows_database_and_backup_names_without_mutation() {
        let invalid_names = [
            "session.db:metadata",
            "session.db.",
            "session.db ",
            "NUL.db",
            "con",
            "COM1.sqlite",
            "LPT¹.db",
        ];
        for name in invalid_names {
            let directory = TestDirectory::new();
            let before = directory_entry_names(directory.path());
            assert!(matches!(
                SessionStore::open(directory.path().join(name), SessionStoreConfig::default()),
                Err(SessionStoreError::Io {
                    operation: super::SessionIoOperation::InspectPath,
                    kind: std::io::ErrorKind::InvalidInput,
                })
            ));
            assert_eq!(directory_entry_names(directory.path()), before, "{name}");

            let mut store = open_store(&directory);
            store
                .put_task(&task_record(gid(1), 0))
                .expect("seed backup source");
            let before = directory_entry_names(directory.path());
            assert!(matches!(
                store.backup_to(directory.path().join(name)),
                Err(SessionStoreError::Io {
                    operation: super::SessionIoOperation::InspectPath,
                    kind: std::io::ErrorKind::InvalidInput,
                })
            ));
            assert_eq!(directory_entry_names(directory.path()), before, "{name}");
        }

        for name in [
            "資料.db",
            ".session.db",
            "my session.v1.db",
            "com10.db",
            "report.con.db",
        ] {
            let directory = TestDirectory::new();
            SessionStore::open(directory.path().join(name), SessionStoreConfig::default())
                .expect("safe Windows persistence filename");
        }
    }

    #[cfg(windows)]
    #[test]
    fn canonicalizes_existing_windows_aliases_before_owner_locking() {
        let directory = TestDirectory::new();
        let database = directory.path().join("CanonicalSession.db");
        let store = SessionStore::open(&database, SessionStoreConfig::default())
            .expect("open canonical database");
        let canonical = fs::canonicalize(&database).expect("canonical database path");
        assert_eq!(store.path(), canonical);

        let alias = directory.path().join("canonicalsession.db");
        assert!(matches!(
            SessionStore::open(&alias, SessionStoreConfig::default()),
            Err(SessionStoreError::OwnerLockBusy)
        ));
        drop(store);

        let reopened = SessionStore::open(alias, SessionStoreConfig::default())
            .expect("reopen through alternate spelling");
        assert_eq!(reopened.path(), canonical);
    }

    #[test]
    fn rejects_parentless_database_and_backup_paths() {
        assert!(matches!(
            SessionStore::open("session.db", SessionStoreConfig::default()),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::InspectPath,
                kind: std::io::ErrorKind::InvalidInput,
            })
        ));

        let directory = TestDirectory::new();
        let store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("open store");
        assert!(matches!(
            store.backup_to("session.backup.db"),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::InspectPath,
                kind: std::io::ErrorKind::InvalidInput,
            })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_broad_existing_parent_without_changing_its_mode() {
        use std::os::unix::fs::PermissionsExt;

        let directory = TestDirectory::new();
        let broad = directory.path().join("shared");
        fs::create_dir(&broad).expect("create broad directory");
        fs::set_permissions(&broad, fs::Permissions::from_mode(0o777))
            .expect("make directory broad");
        assert!(matches!(
            SessionStore::open(broad.join("session.db"), SessionStoreConfig::default()),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::TightenPermissions,
                kind: std::io::ErrorKind::PermissionDenied,
            })
        ));
        assert_eq!(
            fs::metadata(&broad)
                .expect("broad metadata")
                .permissions()
                .mode()
                & 0o777,
            0o777
        );
        assert!(!broad.join("session.db").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_database_and_sidecar_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let target = directory.path().join("target.db");
        fs::write(&target, b"not a database").expect("target file");
        let database = directory.database();
        symlink(&target, &database).expect("database symlink");
        assert!(matches!(
            SessionStore::open(&database, SessionStoreConfig::default()),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::InspectPath,
                kind: std::io::ErrorKind::PermissionDenied,
            })
        ));
        fs::remove_file(&database).expect("remove database symlink");

        let store = SessionStore::open(&database, SessionStoreConfig::default())
            .expect("create real database");
        drop(store);
        let wal = super::sqlite_sidecar_path(&database, "-wal");
        symlink(directory.path().join("missing-wal-target"), &wal).expect("dangling WAL symlink");
        assert!(matches!(
            SessionStore::open(&database, SessionStoreConfig::default()),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::InspectPath,
                kind: std::io::ErrorKind::PermissionDenied,
            })
        ));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn rejects_hard_linked_database_aliases_before_owner_locking() {
        let source = TestDirectory::new();
        let store = SessionStore::open(source.database(), SessionStoreConfig::default())
            .expect("create source store");
        drop(store);

        let alias = TestDirectory::new();
        #[cfg(unix)]
        fs::hard_link(source.database(), alias.database()).expect("create database hard link");
        #[cfg(windows)]
        {
            fs::write(
                alias.database(),
                fs::read(source.database()).expect("read source database"),
            )
            .expect("create inherited-ACL database");
            assert!(
                ariax_windows_security::verify_private_file(&alias.database()).is_err(),
                "test database must begin with an inherited ACL"
            );
            fs::hard_link(alias.database(), alias.path().join("session-hard-link.db"))
                .expect("create database hard link");
        }
        let before = snapshot_directory(alias.path());
        assert!(matches!(
            SessionStore::open(alias.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::InspectPath,
                kind: std::io::ErrorKind::PermissionDenied,
            })
        ));
        assert_eq!(snapshot_directory(alias.path()), before);
        assert!(!super::session_owner_lock_path(&alias.database()).exists());

        #[cfg(unix)]
        fs::remove_file(alias.database()).expect("remove database alias");
        #[cfg(windows)]
        {
            fs::remove_file(alias.path().join("session-hard-link.db"))
                .expect("remove database alias");
            assert!(
                ariax_windows_security::verify_private_file(&alias.database()).is_err(),
                "rejected database ACL must remain unchanged"
            );
        }
        SessionStore::open(source.database(), SessionStoreConfig::default())
            .expect("source reopens after alias removal");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_in_intermediate_path_components() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let target = directory.path().join("target");
        let private = target.join("private");
        fs::create_dir(&target).expect("create target");
        super::tighten_directory_permissions(&target).expect("secure target");
        fs::create_dir(&private).expect("create private target");
        super::tighten_directory_permissions(&private).expect("secure private target");
        let link = directory.path().join("link");
        symlink(&target, &link).expect("create intermediate symlink");
        let database = link.join("private").join("session.db");
        assert!(matches!(
            SessionStore::open(&database, SessionStoreConfig::default()),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::InspectPath,
                kind: std::io::ErrorKind::PermissionDenied,
            })
        ));
        assert!(!private.join("session.db").exists());
    }

    #[test]
    fn rejects_orphan_sidecars_before_creating_a_database() {
        let directory = TestDirectory::new();
        let database = directory.database();
        let wal = super::sqlite_sidecar_path(&database, "-wal");
        fs::write(&wal, b"orphan WAL").expect("write orphan WAL");
        let before = snapshot_directory(directory.path());
        assert!(matches!(
            SessionStore::open(&database, SessionStoreConfig::default()),
            Err(SessionStoreError::InvalidPersistedValue(
                "orphan_sqlite_sidecar"
            ))
        ));
        assert!(!database.exists());
        assert_eq!(snapshot_directory(directory.path()), before);
    }

    #[test]
    fn rejects_sidecars_beside_an_empty_database_before_mutation() {
        for suffix in ["-wal", "-shm", "-journal"] {
            let directory = TestDirectory::new();
            let database = directory.database();
            fs::write(&database, []).expect("create empty database");
            let sidecar = super::sqlite_sidecar_path(&database, suffix);
            fs::write(&sidecar, b"untrusted sidecar").expect("write sidecar");
            let before = snapshot_directory(directory.path());

            assert!(matches!(
                SessionStore::open(&database, SessionStoreConfig::default()),
                Err(SessionStoreError::InvalidPersistedValue(
                    "orphan_sqlite_sidecar"
                ))
            ));
            assert_eq!(snapshot_directory(directory.path()), before);
            assert!(!super::session_owner_lock_path(&database).exists());
        }
    }

    #[test]
    fn rejects_unsupported_schema_without_rewriting_database_bytes() {
        let directory = TestDirectory::new();
        let connection = Connection::open(directory.database()).expect("create newer");
        connection
            .execute_batch("CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES ('keep'); PRAGMA user_version=4;")
            .expect("seed newer");
        drop(connection);
        let before = fs::read(directory.database()).expect("read before");
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::UnsupportedSchema {
                found: 4,
                supported: SESSION_SCHEMA_VERSION
            })
        ));
        assert_eq!(fs::read(directory.database()).expect("read after"), before);
    }

    #[test]
    fn rejects_unsupported_schema_committed_in_wal_without_touching_artifacts() {
        let directory = TestDirectory::new();
        let connection = Connection::open(directory.database()).expect("create WAL database");
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE sentinel(value TEXT); PRAGMA user_version=4; INSERT INTO sentinel VALUES ('wal');",
            )
            .expect("seed newer WAL schema");
        let wal = super::sqlite_sidecar_path(&directory.database(), "-wal");
        assert!(wal.exists());
        assert_eq!(
            super::inspect_persisted_user_version(&directory.database()).expect("preflight"),
            4
        );
        let before = snapshot_directory(directory.path());
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::UnsupportedSchema {
                found: 4,
                supported: SESSION_SCHEMA_VERSION,
            })
        ));
        assert_eq!(snapshot_directory(directory.path()), before);
        drop(connection);
    }

    #[test]
    fn wal_version_preflight_handles_commits_endianness_and_corruption() {
        let directory = TestDirectory::new();
        let database = directory.database();
        let connection = Connection::open(&database).expect("create main database");
        connection
            .execute_batch("PRAGMA journal_mode=DELETE; PRAGMA user_version=1;")
            .expect("seed main version");
        drop(connection);

        for checksum_big_endian in [false, true] {
            let uncommitted = synthetic_wal(&database, checksum_big_endian, &[(2, false)]);
            write_wal(&database, &uncommitted);
            assert_eq!(
                super::inspect_persisted_user_version(&database).expect("uncommitted preflight"),
                1
            );

            let committed = synthetic_wal(&database, checksum_big_endian, &[(2, true), (3, true)]);
            write_wal(&database, &committed);
            assert_eq!(
                super::inspect_persisted_user_version(&database).expect("committed preflight"),
                3
            );

            let deferred_commit = synthetic_wal_frames(
                &database,
                checksum_big_endian,
                &[(1, 2, false), (2, 0, true)],
            );
            write_wal(&database, &deferred_commit);
            assert_eq!(
                super::inspect_persisted_user_version(&database)
                    .expect("non-page-one commit preflight"),
                2
            );

            let page_zero_commit = synthetic_wal_frames(
                &database,
                checksum_big_endian,
                &[(1, 2, false), (0, 0, true)],
            );
            write_wal(&database, &page_zero_commit);
            assert_eq!(
                super::inspect_persisted_user_version(&database)
                    .expect("page-zero frame preflight"),
                1
            );

            let mut torn = committed.clone();
            torn.extend_from_slice(&[0xaa; 17]);
            write_wal(&database, &torn);
            assert_eq!(
                super::inspect_persisted_user_version(&database).expect("torn preflight"),
                3
            );

            let first_only = synthetic_wal(&database, checksum_big_endian, &[(2, true)]);
            let mut bad_second = committed.clone();
            let second_page_offset = first_only.len() + 24;
            bad_second[second_page_offset + 70] ^= 0x80;
            write_wal(&database, &bad_second);
            assert_eq!(
                super::inspect_persisted_user_version(&database).expect("bad checksum preflight"),
                2
            );

            let mut bad_salt = committed.clone();
            bad_salt[first_only.len() + 8] ^= 1;
            write_wal(&database, &bad_salt);
            assert_eq!(
                super::inspect_persisted_user_version(&database).expect("bad salt preflight"),
                2
            );

            let mut bad_format = committed.clone();
            bad_format[4..8].copy_from_slice(&0_u32.to_be_bytes());
            write_wal(&database, &bad_format);
            assert_eq!(
                super::inspect_persisted_user_version(&database).expect("bad format preflight"),
                1
            );

            let mut unsupported_format = committed.clone();
            set_wal_format_version(&mut unsupported_format, 3_007_001, checksum_big_endian);
            write_wal(&database, &unsupported_format);
            assert!(matches!(
                super::inspect_persisted_user_version(&database),
                Err(SessionStoreError::InvalidPersistedValue(
                    "wal.format_version"
                ))
            ));

            let mut bad_page_size = committed.clone();
            bad_page_size[8..12].copy_from_slice(&512_u32.to_be_bytes());
            recompute_wal_header_checksum(&mut bad_page_size, checksum_big_endian);
            write_wal(&database, &bad_page_size);
            assert!(matches!(
                super::inspect_persisted_user_version(&database),
                Err(SessionStoreError::InvalidPersistedValue("wal.page_size"))
            ));
        }

        let database_bytes = fs::read(&database).expect("database bytes");
        let page_size =
            super::decode_database_page_size(&database_bytes[16..18]).expect("page size");
        let database_pages =
            u32::try_from(database_bytes.len() / page_size).expect("database pages");
        let journal = synthetic_rollback_journal(
            &database,
            database_pages,
            &[(1, SESSION_SCHEMA_VERSION, true)],
        );
        fs::write(super::sqlite_sidecar_path(&database, "-journal"), journal)
            .expect("write hot rollback fixture");
        write_wal(&database, &synthetic_wal(&database, false, &[(2, true)]));
        assert_eq!(
            super::inspect_persisted_user_version(&database)
                .expect("rollback followed by WAL preflight"),
            2
        );
    }

    #[test]
    fn rejects_checksum_valid_unsupported_wal_format_without_mutation() {
        let directory = TestDirectory::new();
        let database = directory.database();
        let connection = Connection::open(&database).expect("create main database");
        connection
            .execute_batch("PRAGMA journal_mode=DELETE; PRAGMA user_version=1;")
            .expect("seed main version");
        drop(connection);

        let mut wal = synthetic_wal(&database, false, &[(2, true)]);
        set_wal_format_version(&mut wal, 3_007_001, false);
        write_wal(&database, &wal);
        let before = snapshot_directory(directory.path());

        assert!(matches!(
            SessionStore::open(&database, SessionStoreConfig::default()),
            Err(SessionStoreError::InvalidPersistedValue(
                "wal.format_version"
            ))
        ));
        assert_eq!(snapshot_directory(directory.path()), before);
    }

    #[test]
    fn committed_wal_page_one_recovers_a_corrupt_main_header() {
        let source = TestDirectory::new();
        let mut store = SessionStore::open(source.database(), SessionStoreConfig::default())
            .expect("create source store");
        store.put_session(&session_record()).expect("session");
        store.put_task(&task_record(gid(1), 0)).expect("task");
        drop(store);

        let connection = Connection::open(source.database()).expect("open source database");
        connection
            .execute_batch(
                &format!("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA user_version=0; PRAGMA wal_checkpoint(TRUNCATE); PRAGMA user_version={SESSION_SCHEMA_VERSION};"),
            )
            .expect("create committed page-one WAL");
        let source_wal = super::sqlite_sidecar_path(&source.database(), "-wal");
        assert!(
            fs::metadata(&source_wal)
                .expect("source WAL metadata")
                .len()
                > 32
        );

        for corruption in [
            MainHeaderCorruption::Magic,
            MainHeaderCorruption::PageSize,
            MainHeaderCorruption::UserVersion,
        ] {
            let direct_directory = TestDirectory::new();
            let direct_database = direct_directory.database();
            copy_fixture_file(&source.database(), &direct_database);
            copy_fixture_file(
                &source_wal,
                &super::sqlite_sidecar_path(&direct_database, "-wal"),
            );
            corrupt_main_header(&direct_database, corruption);
            let direct = Connection::open(&direct_database).expect("SQLite WAL recovery");
            let direct_version: u32 = direct
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .expect("direct recovered version");
            assert_eq!(direct_version, SESSION_SCHEMA_VERSION, "{corruption:?}");
            drop(direct);

            let store_directory = TestDirectory::new();
            let store_database = store_directory.database();
            copy_fixture_file(&source.database(), &store_database);
            copy_fixture_file(
                &source_wal,
                &super::sqlite_sidecar_path(&store_database, "-wal"),
            );
            corrupt_main_header(&store_database, corruption);
            let recovered = SessionStore::open(&store_database, SessionStoreConfig::default())
                .expect("SessionStore WAL recovery");
            assert_eq!(recovered.tasks().expect("recovered tasks").len(), 1);
        }
        drop(connection);
    }

    #[test]
    fn wal_preflight_mutation_corpus_never_panics_or_authorizes_partial_frames() {
        let directory = TestDirectory::new();
        let database = directory.database();
        let connection = Connection::open(&database).expect("create mutation database");
        connection
            .execute_batch("PRAGMA journal_mode=DELETE; PRAGMA user_version=1;")
            .expect("seed main version");
        drop(connection);
        let valid = synthetic_wal(&database, false, &[(2, true)]);

        for length in (0..valid.len()).step_by(17) {
            write_wal(&database, &valid[..length]);
            let result = super::inspect_persisted_user_version(&database);
            assert!(
                result.is_ok()
                    || matches!(&result, Err(SessionStoreError::InvalidPersistedValue(_)))
            );
            assert!(
                !matches!(result, Ok(2)),
                "partial WAL length {length} was authorized"
            );
        }
        for index in (0..valid.len()).step_by(17) {
            let mut mutated = valid.clone();
            mutated[index] ^= 0x5a;
            write_wal(&database, &mutated);
            let result = super::inspect_persisted_user_version(&database);
            assert!(
                result.is_ok()
                    || matches!(result, Err(SessionStoreError::InvalidPersistedValue(_)))
            );
        }
    }

    #[test]
    fn wal_preflight_streams_valid_wal_above_checkpoint_trigger() {
        let directory = TestDirectory::new();
        let database = directory.database();
        let connection = Connection::open(&database).expect("create large-WAL database");
        connection
            .execute_batch(
                "PRAGMA journal_mode=DELETE; PRAGMA page_size=65536; VACUUM; PRAGMA user_version=1;",
            )
            .expect("seed large-page database");
        drop(connection);
        let frames = vec![(2_u32, true); 1025];
        let wal = synthetic_wal(&database, false, &frames);
        assert!(wal.len() > 64 * 1024 * 1024);
        write_wal(&database, &wal);
        assert_eq!(
            super::inspect_persisted_user_version(&database).expect("large WAL preflight"),
            2
        );
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
    fn rejects_persisted_queue_corruption_during_preflight() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        drop(store);
        let connection = Connection::open(directory.database()).expect("open raw database");
        connection
            .execute("UPDATE task SET queue_position = 2", [])
            .expect("corrupt queue");
        drop(connection);
        super::tighten_sqlite_artifact_permissions(&directory.database())
            .expect("normalize fixture permissions");
        let before = snapshot_directory(directory.path());
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert_eq!(snapshot_directory(directory.path()), before);
    }

    #[test]
    fn rejects_foreign_key_corruption_during_preflight() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        drop(store);
        let connection = Connection::open(directory.database()).expect("open raw database");
        connection
            .execute_batch("PRAGMA foreign_keys=OFF")
            .expect("disable foreign keys");
        connection
            .execute(
                "UPDATE task SET session_id = ?1 WHERE gid = ?2",
                rusqlite::params![[99_u8; 16].as_slice(), gid(1).to_string()],
            )
            .expect("corrupt foreign key");
        drop(connection);
        super::tighten_sqlite_artifact_permissions(&directory.database())
            .expect("normalize fixture permissions");
        let before = snapshot_directory(directory.path());
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::InvalidPersistedValue(
                "foreign_key_check"
            ))
        ));
        assert_eq!(snapshot_directory(directory.path()), before);
    }

    #[test]
    fn busy_timeout_is_an_exact_contract() {
        let directory = TestDirectory::new();
        assert!(matches!(
            SessionStore::open(
                directory.database(),
                SessionStoreConfig {
                    busy_timeout_ms: super::SESSION_BUSY_TIMEOUT_MS - 1,
                    ..SessionStoreConfig::default()
                },
            ),
            Err(SessionStoreError::InvalidConfig("busy_timeout_ms"))
        ));
        assert!(!directory.database().exists());
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
    fn demoted_task_round_trip_preserves_bounded_slow_slot_state() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let mut task = task_record(gid(1), 0);
        task.queue_state = SessionQueueState::Demoted;
        task.slow_demotion_count = u32::MAX;
        task.slow_slot = Some(SessionSlowSlotState {
            original_position: (super::SESSION_MAX_TASKS - 1) as u32,
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: i64::MAX as u64,
                delay_ms: u64::MAX,
            }),
        });
        store.put_task(&task).expect("put demoted task");
        assert_eq!(store.tasks().expect("demoted tasks"), vec![task]);
    }

    #[test]
    fn slow_slot_state_rejects_inconsistent_queue_and_retry_values() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let slow_slot = SessionSlowSlotState {
            original_position: 0,
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: 200,
                delay_ms: 1,
            }),
        };

        let mut missing = task_record(gid(1), 0);
        missing.queue_state = SessionQueueState::Demoted;
        missing.slow_demotion_count = 1;
        assert!(matches!(
            store.put_task(&missing),
            Err(SessionStoreError::InvalidRecord("slow_slot.queue_state"))
        ));

        let mut unexpected = task_record(gid(2), 0);
        unexpected.slow_demotion_count = 1;
        unexpected.slow_slot = Some(slow_slot);
        assert!(matches!(
            store.put_task(&unexpected),
            Err(SessionStoreError::InvalidRecord("slow_slot.queue_state"))
        ));

        let mut zero_delay = task_record(gid(3), 0);
        zero_delay.queue_state = SessionQueueState::Demoted;
        zero_delay.slow_demotion_count = 1;
        zero_delay.slow_slot = Some(SessionSlowSlotState {
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: 200,
                delay_ms: 0,
            }),
            ..slow_slot
        });
        assert!(matches!(
            store.put_task(&zero_delay),
            Err(SessionStoreError::InvalidRecord("slow_slot.retry.delay_ms"))
        ));

        let mut position_outside_store = task_record(gid(4), 0);
        position_outside_store.queue_state = SessionQueueState::Demoted;
        position_outside_store.slow_demotion_count = 1;
        position_outside_store.slow_slot = Some(SessionSlowSlotState {
            original_position: super::SESSION_MAX_TASKS as u32,
            ..slow_slot
        });
        assert!(matches!(
            store.put_task(&position_outside_store),
            Err(SessionStoreError::InvalidRecord(
                "slow_slot.original_position"
            ))
        ));

        let mut zero_demotions = task_record(gid(5), 0);
        zero_demotions.queue_state = SessionQueueState::Demoted;
        zero_demotions.slow_slot = Some(slow_slot);
        assert!(matches!(
            store.put_task(&zero_demotions),
            Err(SessionStoreError::InvalidRecord("slow_slot.demotion_count"))
        ));

        let mut paused_demotion = task_record(gid(6), 0);
        paused_demotion.queue_state = SessionQueueState::Demoted;
        paused_demotion.desired_paused = true;
        paused_demotion.slow_demotion_count = 1;
        paused_demotion.slow_slot = Some(slow_slot);
        assert!(matches!(
            store.put_task(&paused_demotion),
            Err(SessionStoreError::InvalidRecord("slow_slot.desired_paused"))
        ));

        let mut unrepresentable_time = task_record(gid(7), 0);
        unrepresentable_time.queue_state = SessionQueueState::Demoted;
        unrepresentable_time.slow_demotion_count = 1;
        unrepresentable_time.slow_slot = Some(SessionSlowSlotState {
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: u64::MAX,
                delay_ms: 1,
            }),
            ..slow_slot
        });
        assert!(matches!(
            store.put_task(&unrepresentable_time),
            Err(SessionStoreError::InvalidRecord(
                "task.slow_retry_scheduled_at_ms"
            ))
        ));
        assert!(store.tasks().expect("no invalid tasks").is_empty());
    }

    #[test]
    fn strict_v2_schema_rejects_invalid_slow_slot_tuples() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store
            .put_task(&task_record(gid(1), 0))
            .expect("waiting task");
        assert!(
            store
                .connection
                .execute(
                    "UPDATE task SET queue_state = 5 WHERE gid = ?1",
                    [gid(1).to_string()],
                )
                .is_err()
        );
        let slow_slot = SessionSlowSlotState {
            original_position: 0,
            retry: None,
        };
        store
            .transition_task_queue(
                gid(1),
                SessionQueueState::Waiting,
                SessionQueueState::Demoted,
                0,
                false,
                1,
                Some(&slow_slot),
                300,
            )
            .expect("valid demotion");
        for sql in [
            "UPDATE task SET slow_original_position = 100000 WHERE gid = ?1",
            "UPDATE task SET slow_demotion_count = 0 WHERE gid = ?1",
            "UPDATE task SET desired_paused = 1 WHERE gid = ?1",
            "UPDATE task SET slow_retry_scheduled_at_ms = 301, slow_retry_delay_ms = X'0000000000000000' WHERE gid = ?1",
        ] {
            assert!(
                store.connection.execute(sql, [gid(1).to_string()]).is_err(),
                "{sql}"
            );
        }
        let task = store
            .tasks()
            .expect("valid task after rejected SQL")
            .remove(0);
        assert_eq!(task.slow_slot, Some(slow_slot));
        assert_eq!(task.slow_demotion_count, 1);
        assert!(!task.desired_paused);
    }

    #[test]
    fn demotion_transition_atomically_updates_queue_pause_and_slow_metadata() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        for (position, value) in [1_u64, 2, 3].into_iter().enumerate() {
            store
                .put_task(&task_record(gid(value), position as u32))
                .expect("insert waiting task");
        }
        let slow_slot = SessionSlowSlotState {
            original_position: 1,
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: 300,
                delay_ms: 60_000,
            }),
        };

        store
            .transition_task_queue(
                gid(2),
                SessionQueueState::Waiting,
                SessionQueueState::Demoted,
                0,
                false,
                2,
                Some(&slow_slot),
                300,
            )
            .expect("demote task");
        let demoted = store
            .tasks()
            .expect("demoted queues")
            .into_iter()
            .find(|task| task.gid == gid(2))
            .expect("demoted task");
        assert_eq!(demoted.queue_state, SessionQueueState::Demoted);
        assert_eq!(demoted.queue_position, 0);
        assert!(!demoted.desired_paused);
        assert_eq!(demoted.slow_demotion_count, 2);
        assert_eq!(demoted.slow_slot, Some(slow_slot));

        let mut bypass = demoted.clone();
        bypass.slow_demotion_count = 3;
        assert!(matches!(
            store.put_task(&bypass),
            Err(SessionStoreError::QueueTransitionRequired)
        ));

        let before_invalid = store.tasks().expect("before invalid transition");
        assert!(matches!(
            store.transition_task_queue(
                gid(2),
                SessionQueueState::Demoted,
                SessionQueueState::Waiting,
                3,
                true,
                2,
                None,
                400,
            ),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert_eq!(
            store.tasks().expect("invalid transition rollback"),
            before_invalid
        );

        store
            .transition_task_queue(
                gid(2),
                SessionQueueState::Demoted,
                SessionQueueState::Waiting,
                1,
                false,
                2,
                None,
                500,
            )
            .expect("readmit demoted task");
        let readmitted = store
            .tasks()
            .expect("readmitted queues")
            .into_iter()
            .find(|task| task.gid == gid(2))
            .expect("readmitted task");
        assert_eq!(readmitted.queue_state, SessionQueueState::Waiting);
        assert!(!readmitted.desired_paused);
        assert_eq!(readmitted.slow_demotion_count, 2);
        assert_eq!(readmitted.slow_slot, None);

        store
            .transition_task_queue(
                gid(2),
                SessionQueueState::Waiting,
                SessionQueueState::Demoted,
                0,
                false,
                3,
                Some(&slow_slot),
                600,
            )
            .expect("demote task again");
        store
            .transition_task_queue(
                gid(2),
                SessionQueueState::Demoted,
                SessionQueueState::Paused,
                0,
                true,
                3,
                None,
                700,
            )
            .expect("pause demoted task");
        let paused = store
            .tasks()
            .expect("paused queues")
            .into_iter()
            .find(|task| task.gid == gid(2))
            .expect("paused task");
        assert_eq!(paused.queue_state, SessionQueueState::Paused);
        assert!(paused.desired_paused);
        assert_eq!(paused.slow_demotion_count, 3);
        assert_eq!(paused.slow_slot, None);
    }

    #[test]
    fn stopped_results_retain_order_and_delete_metadata_atomically() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        for (position, value) in [1_u64, 2, 3].into_iter().enumerate() {
            store
                .put_task(&task_record(gid(value), position as u32))
                .expect("insert waiting task");
        }
        let first = stopped_result_record(gid(1), SessionTerminalStatus::Complete);
        store
            .persist_stopped_result(&first, SessionQueueState::Waiting, 0, false, 0, 400)
            .expect("retain first stopped result");
        let second = stopped_result_record(gid(2), SessionTerminalStatus::Error);
        store
            .persist_stopped_result(&second, SessionQueueState::Waiting, 0, false, 2, 500)
            .expect("retain second result at front");
        assert_eq!(
            store.stopped_results().expect("ordered results"),
            vec![second, first]
        );
        let stopped = store
            .tasks()
            .expect("paired stopped tasks")
            .into_iter()
            .filter(|task| task.queue_state == SessionQueueState::Stopped)
            .map(|task| (task.gid, task.queue_position, task.slow_demotion_count))
            .collect::<Vec<_>>();
        assert_eq!(stopped, vec![(gid(2), 0, 2), (gid(1), 1, 0)]);

        store
            .connection
            .execute(
                "INSERT INTO task_source(gid, uri_id, persistence_safe_uri, redacted_fingerprint, needs_credentials, priority) VALUES (?1, 0, NULL, ?2, 1, 0)",
                rusqlite::params![gid(2).to_string(), [7_u8; 32].as_slice()],
            )
            .expect("insert retained task metadata");
        let before_tasks = store.tasks().expect("before invalid deletion");
        let before_results = store
            .stopped_results()
            .expect("before invalid deletion results");
        assert!(matches!(
            store.delete_stopped_task_metadata(gid(2), &[gid(1), gid(3)], 600),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert_eq!(store.tasks().expect("rolled back tasks"), before_tasks);
        assert_eq!(
            store.stopped_results().expect("rolled back results"),
            before_results
        );
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM task_source WHERE gid = ?1",
                    [gid(2).to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .expect("retained child count"),
            1
        );

        store
            .delete_stopped_task_metadata(gid(2), &[gid(1)], 700)
            .expect("delete paired stopped metadata");
        assert_eq!(
            store
                .stopped_results()
                .expect("compacted stopped results")
                .into_iter()
                .map(|result| result.gid)
                .collect::<Vec<_>>(),
            vec![gid(1)]
        );
        assert_eq!(
            store
                .tasks()
                .expect("tasks after deletion")
                .into_iter()
                .filter(|task| task.queue_state == SessionQueueState::Stopped)
                .map(|task| (task.gid, task.queue_position))
                .collect::<Vec<_>>(),
            vec![(gid(1), 0)]
        );
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM task_source WHERE gid = ?1",
                    [gid(2).to_string()],
                    |row| row.get::<_, i64>(0),
                )
                .expect("deleted child count"),
            0
        );
    }

    #[test]
    fn stopped_result_payloads_and_generic_stopped_transitions_are_rejected() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        let before = store.tasks().expect("initial tasks");

        let mut complete_with_error =
            stopped_result_record(gid(1), SessionTerminalStatus::Complete);
        complete_with_error.error_kind = Some(ErrorKind::Disk);
        assert!(matches!(
            store.persist_stopped_result(
                &complete_with_error,
                SessionQueueState::Waiting,
                0,
                false,
                0,
                300,
            ),
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.complete_payload"
            ))
        ));
        let mut complete_without_layout =
            stopped_result_record(gid(1), SessionTerminalStatus::Complete);
        complete_without_layout.layout_hash = None;
        assert!(matches!(
            store.persist_stopped_result(
                &complete_without_layout,
                SessionQueueState::Waiting,
                0,
                false,
                0,
                300,
            ),
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.complete_payload"
            ))
        ));
        let mut error_without_kind = stopped_result_record(gid(1), SessionTerminalStatus::Error);
        error_without_kind.error_kind = None;
        assert!(matches!(
            store.persist_stopped_result(
                &error_without_kind,
                SessionQueueState::Waiting,
                0,
                false,
                0,
                300,
            ),
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.error_code"
            ))
        ));
        let mut error_with_completion = stopped_result_record(gid(1), SessionTerminalStatus::Error);
        error_with_completion.total_length = Some(1);
        assert!(matches!(
            store.persist_stopped_result(
                &error_with_completion,
                SessionQueueState::Waiting,
                0,
                false,
                0,
                300,
            ),
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.error_payload"
            ))
        ));
        let mut removed_with_completion =
            stopped_result_record(gid(1), SessionTerminalStatus::Removed);
        removed_with_completion.layout_hash = Some(hash(7));
        assert!(matches!(
            store.persist_stopped_result(
                &removed_with_completion,
                SessionQueueState::Waiting,
                0,
                false,
                0,
                300,
            ),
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.non_error_payload"
            ))
        ));
        assert!(matches!(
            store.persist_stopped_result(
                &stopped_result_record(gid(1), SessionTerminalStatus::Complete),
                SessionQueueState::Paused,
                0,
                false,
                0,
                300,
            ),
            Err(SessionStoreError::QueueTransitionRequired)
        ));
        assert!(matches!(
            store.persist_stopped_result(
                &stopped_result_record(gid(1), SessionTerminalStatus::Complete),
                SessionQueueState::Waiting,
                1,
                false,
                0,
                300,
            ),
            Err(SessionStoreError::QueueInvariant)
        ));
        let mut stopped_task = task_record(gid(2), 0);
        stopped_task.queue_state = SessionQueueState::Stopped;
        assert!(matches!(
            store.put_task(&stopped_task),
            Err(SessionStoreError::QueueTransitionRequired)
        ));
        assert!(matches!(
            store.transition_task_queue(
                gid(1),
                SessionQueueState::Waiting,
                SessionQueueState::Stopped,
                0,
                false,
                0,
                None,
                300,
            ),
            Err(SessionStoreError::QueueTransitionRequired)
        ));
        assert_eq!(store.tasks().expect("all rejections roll back"), before);

        store
            .persist_stopped_result(
                &stopped_result_record(gid(1), SessionTerminalStatus::Removed),
                SessionQueueState::Waiting,
                0,
                false,
                0,
                400,
            )
            .expect("retain result through dedicated API");
        assert!(matches!(
            store.transition_task_queue(
                gid(1),
                SessionQueueState::Stopped,
                SessionQueueState::Waiting,
                0,
                false,
                0,
                None,
                500,
            ),
            Err(SessionStoreError::QueueTransitionRequired)
        ));
        store
            .connection
            .execute(
                "UPDATE stopped_result SET total_length = ?1 WHERE gid = ?2",
                rusqlite::params![super::encode_u64(1), gid(1).to_string()],
            )
            .expect("simulate noncanonical removed result");
        assert!(matches!(
            store.stopped_results(),
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.non_error_payload"
            ))
        ));
        drop(store);
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::InvalidRecord(
                "stopped_result.non_error_payload"
            ))
        ));
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
    fn queue_transition_moves_between_dense_queues_and_rolls_back_invalid_positions() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        for (position, value) in [1_u64, 2, 3].into_iter().enumerate() {
            store
                .put_task(&task_record(gid(value), position as u32))
                .expect("insert waiting task");
        }
        store
            .transition_task_queue(
                gid(2),
                SessionQueueState::Waiting,
                SessionQueueState::Paused,
                0,
                true,
                0,
                None,
                300,
            )
            .expect("move task to paused queue");
        let tasks = store.tasks().expect("transitioned tasks");
        let waiting = tasks
            .iter()
            .filter(|task| task.queue_state == SessionQueueState::Waiting)
            .map(|task| (task.gid, task.queue_position))
            .collect::<Vec<_>>();
        let paused = tasks
            .iter()
            .filter(|task| task.queue_state == SessionQueueState::Paused)
            .map(|task| (task.gid, task.queue_position, task.desired_paused))
            .collect::<Vec<_>>();
        assert_eq!(waiting, vec![(gid(1), 0), (gid(3), 1)]);
        assert_eq!(paused, vec![(gid(2), 0, true)]);

        let before = tasks;
        assert!(matches!(
            store.transition_task_queue(
                gid(1),
                SessionQueueState::Waiting,
                SessionQueueState::Active,
                1,
                false,
                0,
                None,
                400,
            ),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert_eq!(store.tasks().expect("rolled back tasks"), before);

        let mut bypass = store.tasks().expect("tasks")[0].clone();
        bypass.queue_state = SessionQueueState::Stopped;
        assert!(matches!(
            store.put_task(&bypass),
            Err(SessionStoreError::QueueTransitionRequired)
        ));
    }

    #[test]
    fn queue_transition_reorders_within_one_queue_and_supports_noop_updates() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        for (position, value) in [1_u64, 2, 3].into_iter().enumerate() {
            store
                .put_task(&task_record(gid(value), position as u32))
                .expect("insert waiting task");
        }

        store
            .transition_task_queue(
                gid(3),
                SessionQueueState::Waiting,
                SessionQueueState::Waiting,
                0,
                true,
                0,
                None,
                300,
            )
            .expect("move upward");
        assert_eq!(
            store
                .tasks()
                .expect("upward tasks")
                .into_iter()
                .map(|task| (task.gid, task.queue_position))
                .collect::<Vec<_>>(),
            vec![(gid(3), 0), (gid(1), 1), (gid(2), 2)]
        );

        store
            .transition_task_queue(
                gid(3),
                SessionQueueState::Waiting,
                SessionQueueState::Waiting,
                2,
                false,
                0,
                None,
                400,
            )
            .expect("move downward");
        assert_eq!(
            store
                .tasks()
                .expect("downward tasks")
                .into_iter()
                .map(|task| (task.gid, task.queue_position))
                .collect::<Vec<_>>(),
            vec![(gid(1), 0), (gid(2), 1), (gid(3), 2)]
        );

        store
            .transition_task_queue(
                gid(2),
                SessionQueueState::Waiting,
                SessionQueueState::Waiting,
                1,
                true,
                0,
                None,
                500,
            )
            .expect("same-position update");
        let task = store
            .tasks()
            .expect("no-op tasks")
            .into_iter()
            .find(|task| task.gid == gid(2))
            .expect("updated task");
        assert_eq!(task.queue_position, 1);
        assert!(task.desired_paused);
        assert_eq!(task.updated_ms, 500);
    }

    #[test]
    fn queue_transition_inserts_into_nonempty_targets_and_rejects_stale_state() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        for (position, value) in [1_u64, 2, 3].into_iter().enumerate() {
            store
                .put_task(&task_record(gid(value), position as u32))
                .expect("insert waiting task");
        }
        for (position, value) in [4_u64, 5].into_iter().enumerate() {
            let mut task = task_record(gid(value), position as u32);
            task.queue_state = SessionQueueState::Paused;
            task.desired_paused = true;
            store.put_task(&task).expect("insert paused task");
        }

        store
            .transition_task_queue(
                gid(2),
                SessionQueueState::Waiting,
                SessionQueueState::Paused,
                1,
                true,
                0,
                None,
                300,
            )
            .expect("insert in middle");
        let before_stale = store.tasks().expect("middle tasks");
        assert!(matches!(
            store.transition_task_queue(
                gid(2),
                SessionQueueState::Waiting,
                SessionQueueState::Active,
                0,
                false,
                0,
                None,
                400,
            ),
            Err(SessionStoreError::QueueTransitionRequired)
        ));
        assert_eq!(store.tasks().expect("stale rollback"), before_stale);

        store
            .transition_task_queue(
                gid(1),
                SessionQueueState::Waiting,
                SessionQueueState::Paused,
                0,
                true,
                0,
                None,
                500,
            )
            .expect("insert at beginning");
        store
            .transition_task_queue(
                gid(3),
                SessionQueueState::Waiting,
                SessionQueueState::Paused,
                4,
                true,
                0,
                None,
                600,
            )
            .expect("insert at end");

        let tasks = store.tasks().expect("final queues");
        let waiting = tasks
            .iter()
            .filter(|task| task.queue_state == SessionQueueState::Waiting)
            .map(|task| (task.gid, task.queue_position))
            .collect::<Vec<_>>();
        let paused = tasks
            .iter()
            .filter(|task| task.queue_state == SessionQueueState::Paused)
            .map(|task| (task.gid, task.queue_position))
            .collect::<Vec<_>>();
        assert!(waiting.is_empty());
        assert_eq!(
            paused,
            vec![
                (gid(1), 0),
                (gid(4), 1),
                (gid(2), 2),
                (gid(5), 3),
                (gid(3), 4),
            ]
        );
    }

    #[test]
    fn exact_queue_transition_applies_only_complete_scheduler_orders() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        for (position, value) in [1_u64, 2, 3].into_iter().enumerate() {
            store
                .put_task(&task_record(gid(value), position as u32))
                .expect("waiting task");
        }
        for (position, value) in [4_u64, 5].into_iter().enumerate() {
            let mut task = task_record(gid(value), position as u32);
            task.queue_state = SessionQueueState::Paused;
            task.desired_paused = true;
            store.put_task(&task).expect("paused task");
        }
        let transition = SessionQueueTransition {
            gid: gid(2),
            expected_state: SessionQueueState::Waiting,
            target_state: SessionQueueState::Paused,
            desired_paused: true,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: vec![gid(3), gid(1)],
                },
                SessionQueueOrder {
                    state: SessionQueueState::Paused,
                    gids: vec![gid(4), gid(2), gid(5)],
                },
            ],
            updated_ms: 300,
        };
        store
            .transition_task_queue_exact(&transition)
            .expect("exact transition");
        assert_eq!(
            store
                .queue_order(SessionQueueState::Waiting)
                .expect("waiting order"),
            vec![gid(3), gid(1)]
        );
        assert_eq!(
            store
                .queue_order(SessionQueueState::Paused)
                .expect("paused order"),
            vec![gid(4), gid(2), gid(5)]
        );

        let before = store.tasks().expect("before rejected exact transition");
        let mut incomplete = transition.clone();
        incomplete.gid = gid(3);
        incomplete.final_orders = vec![SessionQueueOrder {
            state: SessionQueueState::Waiting,
            gids: vec![gid(1)],
        }];
        assert!(matches!(
            store.transition_task_queue_exact(&incomplete),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert_eq!(store.tasks().expect("rejected transition rollback"), before);

        let mut duplicate = SessionQueueTransition {
            gid: gid(3),
            expected_state: SessionQueueState::Waiting,
            target_state: SessionQueueState::Paused,
            desired_paused: true,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: vec![gid(1)],
                },
                SessionQueueOrder {
                    state: SessionQueueState::Paused,
                    gids: vec![gid(3), gid(2), gid(3), gid(4), gid(5)],
                },
            ],
            updated_ms: 400,
        };
        assert!(matches!(
            store.transition_task_queue_exact(&duplicate),
            Err(SessionStoreError::QueueInvariant)
        ));
        duplicate.final_orders[1].gids = vec![gid(3), gid(2), gid(4), gid(5)];
        store
            .transition_task_queue_exact(&duplicate)
            .expect("complete second transition");
    }

    #[test]
    fn no_space_updates_are_atomic_and_queue_gated() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let mut waiting = task_record(gid(1), 0);
        waiting.no_space = None;
        store.put_task(&waiting).expect("waiting task");
        let condition = SessionNoSpaceCondition {
            target: path(b"/redacted/volume"),
            scheduled_at_ms: 350,
            delay_ms: 5_000,
        };
        store
            .set_task_no_space_condition(gid(1), Some(&condition), 350)
            .expect("set condition");
        assert_eq!(
            store.tasks().expect("condition task")[0].no_space,
            Some(condition.clone())
        );
        let invalid = SessionNoSpaceCondition {
            delay_ms: 0,
            ..condition.clone()
        };
        assert!(matches!(
            store.set_task_no_space_condition(gid(1), Some(&invalid), 360),
            Err(SessionStoreError::InvalidRecord("no_space.delay_ms"))
        ));
        assert_eq!(
            store.tasks().expect("invalid update rollback")[0].no_space,
            Some(condition.clone())
        );
        store
            .set_task_no_space_condition(gid(1), None, 370)
            .expect("clear condition");
        assert_eq!(store.tasks().expect("cleared task")[0].no_space, None);

        let mut active = task_record(gid(2), 0);
        active.queue_state = SessionQueueState::Active;
        active.no_space = None;
        store.put_task(&active).expect("active task");
        assert!(matches!(
            store.set_task_no_space_condition(gid(2), Some(&invalid), 380),
            Err(SessionStoreError::InvalidRecord("no_space.delay_ms"))
        ));
        let valid = SessionNoSpaceCondition {
            delay_ms: 1,
            ..invalid
        };
        store
            .set_task_no_space_condition(gid(2), Some(&valid), 380)
            .expect("persist condition before active cancellation drains");

        let mut demoted = task_record(gid(3), 0);
        demoted.queue_state = SessionQueueState::Demoted;
        demoted.slow_demotion_count = 1;
        demoted.slow_slot = Some(SessionSlowSlotState {
            original_position: 0,
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: 300,
                delay_ms: 1_000,
            }),
        });
        store.put_task(&demoted).expect("demoted task");
        store
            .set_task_no_space_condition(gid(3), Some(&valid), 390)
            .expect("refresh condition while slow-demoted");

        let mut stopped = task_record(gid(4), 1);
        stopped.no_space = Some(valid.clone());
        store.put_task(&stopped).expect("future stopped task");
        let terminal = stopped_result_record(gid(4), SessionTerminalStatus::Complete);
        store
            .persist_stopped_result(&terminal, SessionQueueState::Waiting, 0, false, 0, 400)
            .expect("terminal transition");
        assert!(matches!(
            store.set_task_no_space_condition(gid(4), Some(&valid), 410),
            Err(SessionStoreError::InvalidRecord("no_space.queue_state"))
        ));
        assert_eq!(
            store
                .tasks()
                .expect("rejected stopped update")
                .into_iter()
                .find(|task| task.gid == gid(4))
                .expect("stopped task")
                .no_space,
            Some(valid)
        );
        store
            .set_task_no_space_condition(gid(4), None, 420)
            .expect("clear stale stopped condition");
    }

    #[test]
    fn task_sources_replace_atomically_and_read_in_priority_order() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        let sources = vec![
            SessionTaskSourceRecord {
                uri_id: 8,
                persistence_safe_uri: Some("https://mirror.example/file".to_owned()),
                redacted_fingerprint: [8; 32],
                needs_credentials: false,
                priority: 20,
            },
            SessionTaskSourceRecord {
                uri_id: 3,
                persistence_safe_uri: None,
                redacted_fingerprint: [3; 32],
                needs_credentials: true,
                priority: 10,
            },
        ];
        store
            .replace_task_sources(gid(1), &sources)
            .expect("replace sources");
        assert_eq!(
            store.task_sources(gid(1)).expect("read sources"),
            vec![sources[1].clone(), sources[0].clone()]
        );

        let before = store.task_sources(gid(1)).expect("before duplicate");
        assert!(matches!(
            store.replace_task_sources(gid(1), &[sources[0].clone(), sources[0].clone()]),
            Err(SessionStoreError::InvalidRecord("task_source.count"))
        ));
        assert_eq!(
            store.task_sources(gid(1)).expect("duplicate rollback"),
            before
        );
        assert!(matches!(
            store.replace_task_sources(gid(9), &sources),
            Err(SessionStoreError::NotFound)
        ));
        let oversized = SessionTaskSourceRecord {
            persistence_safe_uri: Some("x".repeat(super::SESSION_MAX_SAFE_URI_BYTES + 1)),
            ..sources[0].clone()
        };
        assert!(matches!(
            store.replace_task_sources(gid(1), &[oversized]),
            Err(SessionStoreError::InvalidRecord("task_source.uri"))
        ));
        let invalid_placeholder = SessionTaskSourceRecord {
            persistence_safe_uri: None,
            needs_credentials: false,
            ..sources[0].clone()
        };
        assert!(matches!(
            store.replace_task_sources(gid(1), &[invalid_placeholder]),
            Err(SessionStoreError::InvalidRecord("task_source.credentials"))
        ));

        store.put_task(&task_record(gid(2), 1)).expect("empty task");
        assert_eq!(
            store.task_source_sets().expect("startup source sets"),
            vec![
                super::SessionTaskSourceSet {
                    gid: gid(1),
                    sources: vec![sources[1].clone(), sources[0].clone()],
                },
                super::SessionTaskSourceSet {
                    gid: gid(2),
                    sources: Vec::new(),
                },
            ]
        );
    }

    #[test]
    fn source_uri_secrecy_is_checked_before_writes_and_during_recovery() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        let safe = SessionTaskSourceRecord {
            uri_id: 0,
            persistence_safe_uri: Some("https://example.test/file@version".to_owned()),
            redacted_fingerprint: [1; 32],
            needs_credentials: false,
            priority: 0,
        };
        store
            .replace_task_sources(gid(1), std::slice::from_ref(&safe))
            .expect("safe source");
        for uri in [
            "https://user:secret-canary@example.test/file",
            "https://example.test/file?token=secret-canary",
            "https://example.test/file?",
            "https://example.test/file#secret-canary",
            "https://example.test/file\nsecret-canary",
            "",
        ] {
            let source = SessionTaskSourceRecord {
                persistence_safe_uri: Some(uri.to_owned()),
                ..safe.clone()
            };
            let error = store
                .replace_task_sources(gid(1), &[source])
                .expect_err("unsafe source");
            assert!(!format!("{error:?} {error}").contains("secret-canary"));
            assert_eq!(
                store.task_sources(gid(1)).expect("unchanged sources"),
                vec![safe.clone()]
            );
        }
        store
            .connection
            .execute(
                "UPDATE task_source SET persistence_safe_uri = ?1 WHERE gid = ?2",
                rusqlite::params![
                    "https://example.test/file?token=secret-canary",
                    gid(1).to_string()
                ],
            )
            .expect("inject legacy unsafe metadata");
        assert!(matches!(
            store.task_sources(gid(1)),
            Err(SessionStoreError::InvalidPersistedValue("task_source.uri"))
        ));
        assert!(matches!(
            store.task_source_sets(),
            Err(SessionStoreError::InvalidPersistedValue("task_source.uri"))
        ));
        drop(store);
        let error = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .err()
            .expect("unsafe recovery must fail closed");
        assert!(!format!("{error:?} {error}").contains("secret-canary"));
    }

    #[test]
    fn task_creation_persists_sources_and_options_atomically_without_replacement() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let mut task = task_record(gid(1), 0);
        let sources = vec![
            SessionTaskSourceRecord {
                uri_id: 2,
                persistence_safe_uri: Some("https://second.example/file".to_owned()),
                redacted_fingerprint: [2; 32],
                needs_credentials: false,
                priority: 20,
            },
            SessionTaskSourceRecord {
                uri_id: 1,
                persistence_safe_uri: Some("https://first.example/file".to_owned()),
                redacted_fingerprint: [1; 32],
                needs_credentials: false,
                priority: 10,
            },
        ];
        let options = SanitizedOptionMap::new([
            ("piece-length".to_owned(), "1M".to_owned()),
            ("split".to_owned(), "5".to_owned()),
        ])
        .expect("options");
        let permit_all = |_: &str| true;

        task.cached_snapshot_hash = options.snapshot_hash();

        store
            .create_task_with_metadata(&task, &sources, &options, &permit_all)
            .expect("atomic task creation");
        assert_eq!(store.tasks().expect("tasks"), vec![task.clone()]);
        assert_eq!(
            store.task_sources(task.gid).expect("sources"),
            vec![sources[1].clone(), sources[0].clone()]
        );
        assert_eq!(
            store
                .task_options(
                    task.gid,
                    OptionsSnapshotScope::CurrentGeneration,
                    &permit_all,
                )
                .expect("options"),
            options
        );

        let replacement_sources = vec![SessionTaskSourceRecord {
            uri_id: 9,
            persistence_safe_uri: Some("https://replacement.example/file".to_owned()),
            redacted_fingerprint: [9; 32],
            needs_credentials: false,
            priority: 0,
        }];
        let replacement_options =
            SanitizedOptionMap::new([("split".to_owned(), "1".to_owned())]).expect("options");
        let replacement_task = SessionTaskRecord {
            cached_snapshot_hash: replacement_options.snapshot_hash(),
            ..task.clone()
        };
        assert!(matches!(
            store.create_task_with_metadata(
                &replacement_task,
                &replacement_sources,
                &replacement_options,
                &permit_all,
            ),
            Err(SessionStoreError::InvalidRecord("task.gid_exists"))
        ));
        assert_eq!(
            store.task_sources(task.gid).expect("unchanged sources"),
            vec![sources[1].clone(), sources[0].clone()]
        );

        let rejected = task_record(gid(2), 1);
        let deny_split = |name: &str| name != "split";
        assert!(matches!(
            store.create_task_with_metadata(&rejected, &sources, &options, &deny_split),
            Err(SessionStoreError::ForbiddenPersistedOption)
        ));
        assert_eq!(store.tasks().expect("rollback tasks"), vec![task]);
        assert!(matches!(
            store.task_sources(rejected.gid),
            Err(SessionStoreError::NotFound)
        ));
    }

    fn import_metadata(id: u64, position: u32) -> SessionTaskMetadata {
        let options =
            SanitizedOptionMap::new([("split".to_owned(), "3".to_owned())]).expect("options");
        SessionTaskMetadata {
            task: SessionTaskRecord {
                cached_snapshot_hash: options.snapshot_hash(),
                ..task_record(gid(id), position)
            },
            sources: vec![SessionTaskSourceRecord {
                uri_id: 0,
                persistence_safe_uri: Some(format!("https://example.test/{id}")),
                redacted_fingerprint: [1; 32],
                needs_credentials: false,
                priority: 0,
            }],
            options,
        }
    }

    fn mixed_import_metadata() -> Vec<SessionAdmissionMetadata> {
        let transfer = import_metadata(1, 0);
        let metainfo =
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent").to_vec();
        let metadata = ariax_bt_metadata::parse_torrent(
            &metainfo,
            ariax_bt_metadata::MetadataLimits::default(),
        )
        .unwrap();
        let task = crate::SessionBtTaskRecord {
            gid: gid(2),
            session_id: transfer.task.session_id,
            queue_state: transfer.task.queue_state,
            queue_position: 1,
            desired_paused: transfer.task.desired_paused,
            root_display: transfer.task.root_display.clone(),
            generation: 0,
            downloaded: 0,
            uploaded: 0,
            seed_millis: 0,
            created_ms: transfer.task.created_ms,
            updated_ms: transfer.task.updated_ms,
            binding: crate::SessionBtBinding {
                identity: metadata.identity,
                root_identity: vec![1; 16],
                metainfo,
                info: Vec::new(),
                magnet: None,
                files: metadata
                    .files
                    .into_iter()
                    .map(|file| crate::SessionBtFile {
                        index: file.index,
                        path: file.components.join("/"),
                        length: file.length,
                        offset: file.offset,
                        selected: !file.padding,
                        padding: file.padding,
                    })
                    .collect(),
            },
        };
        vec![
            SessionAdmissionMetadata::Transfer(transfer),
            SessionAdmissionMetadata::BitTorrent {
                task,
                options: SanitizedOptionMap::new([]).unwrap(),
                resume: Arc::from([]),
            },
        ]
    }

    #[test]
    fn mixed_session_batch_rolls_back_both_protocols_and_confirms_exact_binding() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let batch = mixed_import_metadata();
        assert!(store.create_session_batch(&[], &|_: &str| true).is_err());
        let mut invalid = batch.clone();
        let SessionAdmissionMetadata::BitTorrent { task, .. } = &mut invalid[1] else {
            panic!("BT member")
        };
        task.binding.files[0].path = "../outside".into();
        assert!(
            store
                .create_session_batch(&invalid, &|_: &str| true)
                .is_err()
        );
        assert!(store.tasks().unwrap().is_empty());
        assert!(store.bt_tasks().unwrap().is_empty());
        store.connection.execute_batch("CREATE TEMP TRIGGER fail_bt_import BEFORE INSERT ON bt_metadata BEGIN SELECT RAISE(ABORT, 'injected mixed import failure'); END;").unwrap();
        assert!(store.create_session_batch(&batch, &|_: &str| true).is_err());
        assert!(store.tasks().unwrap().is_empty());
        assert!(store.bt_tasks().unwrap().is_empty());
        store
            .connection
            .execute_batch("DROP TRIGGER fail_bt_import")
            .unwrap();
        store.create_session_batch(&batch, &|_: &str| true).unwrap();
        let SessionAdmissionMetadata::BitTorrent { task, options, .. } = &batch[1] else {
            panic!("BT member")
        };
        store
            .confirm_bt_task(task, options, &|_: &str| true)
            .unwrap();
        let mut changed = task.clone();
        changed.binding.files[0].selected = false;
        assert!(
            store
                .confirm_bt_task(&changed, options, &|_: &str| true)
                .is_err()
        );
        assert!(store.bt_resume(task.gid, 1024).unwrap().dirty);
        drop(store);
        let reopened =
            SessionStore::open(directory.database(), SessionStoreConfig::default()).unwrap();
        assert_eq!(reopened.tasks().unwrap().len(), 1);
        assert_eq!(reopened.bt_tasks().unwrap(), [task.clone()]);
    }

    #[test]
    fn mixed_import_crashes_recover_none_or_both_protocols() {
        for committed in [false, true] {
            let directory = TestDirectory::new();
            drop(open_store(&directory));
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "session_store::tests::import_batch_crash_child",
                    "--nocapture",
                ])
                .env("ARIAX_IMPORT_CRASH_DATABASE", directory.database())
                .env("ARIAX_IMPORT_CRASH_MIXED", "true")
                .env(
                    "ARIAX_IMPORT_CRASH_COMMITTED",
                    if committed { "true" } else { "false" },
                )
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(77));
            let store =
                SessionStore::open(directory.database(), SessionStoreConfig::default()).unwrap();
            assert_eq!(store.tasks().unwrap().len(), usize::from(committed));
            assert_eq!(store.bt_tasks().unwrap().len(), usize::from(committed));
        }
    }

    #[test]
    fn import_batch_is_atomic_and_member_confirmation_requires_exact_metadata() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let batch = vec![import_metadata(1, 0), import_metadata(2, 1)];
        let policy = |_: &str| true;
        assert!(store.create_task_batch(&[], &policy).is_err());
        assert!(
            store
                .create_task_batch(
                    &vec![batch[0].clone(); SESSION_MAX_IMPORT_TASKS + 1],
                    &policy
                )
                .is_err()
        );
        assert!(
            store
                .create_task_batch(&[batch[0].clone(), batch[0].clone()], &policy)
                .is_err()
        );
        assert!(store.create_task_batch(&batch, &|_: &str| false).is_err());
        let mut wrong_hash = batch.clone();
        wrong_hash[1].task.cached_snapshot_hash = hash(8);
        assert!(store.create_task_batch(&wrong_hash, &policy).is_err());
        let mut queue_gap = batch.clone();
        queue_gap[1].task.queue_position = 3;
        assert!(store.create_task_batch(&queue_gap, &policy).is_err());
        let mut oversized = batch[0].clone();
        oversized
            .sources
            .reserve(SESSION_IMPORT_MAX_BYTES / std::mem::size_of::<SessionTaskSourceRecord>() + 1);
        assert!(matches!(
            store.create_task_batch(&[oversized], &policy),
            Err(SessionStoreError::InvalidRecord("import.byte_budget"))
        ));
        assert!(store.tasks().expect("no invalid prefix").is_empty());
        store.connection.execute_batch("CREATE TEMP TRIGGER fail_second_import BEFORE INSERT ON task_source WHEN NEW.gid = '0000000000000002' BEGIN SELECT RAISE(ABORT, 'injected import failure'); END;").expect("fault trigger");
        assert!(store.create_task_batch(&batch, &policy).is_err());
        assert!(store.tasks().expect("rolled back task rows").is_empty());
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM task_source", [], |row| row
                    .get::<_, i64>(0))
                .expect("source rows"),
            0
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM task_option", [], |row| row
                    .get::<_, i64>(0))
                .expect("option rows"),
            0
        );
        store
            .connection
            .execute_batch("DROP TRIGGER fail_second_import")
            .expect("clear fault");
        store
            .create_task_batch(&batch, &policy)
            .expect("atomic import");
        for entry in &batch {
            store
                .confirm_task_metadata(entry, &policy)
                .expect("exact committed member");
        }
        let mut altered = batch[1].clone();
        altered.sources[0].priority = 5;
        assert!(store.confirm_task_metadata(&altered, &policy).is_err());
        assert!(
            store
                .create_task_batch(&[import_metadata(3, 2), batch[1].clone()], &policy)
                .is_err()
        );
        assert_eq!(
            store
                .tasks()
                .expect("collision did not publish prefix")
                .len(),
            2
        );
        drop(store);
        let reopened = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("reopen import");
        for entry in &batch {
            reopened
                .confirm_task_metadata(entry, &policy)
                .expect("recovered member");
        }
    }

    #[test]
    fn import_batch_process_exit_recovers_none_or_the_whole_document() {
        for committed in [false, true] {
            let directory = TestDirectory::new();
            drop(open_store(&directory));
            let status = Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "session_store::tests::import_batch_crash_child",
                    "--nocapture",
                ])
                .env("ARIAX_IMPORT_CRASH_DATABASE", directory.database())
                .env(
                    "ARIAX_IMPORT_CRASH_COMMITTED",
                    if committed { "true" } else { "false" },
                )
                .status()
                .expect("import crash child");
            assert_eq!(status.code(), Some(77));
            let store = SessionStore::open(directory.database(), SessionStoreConfig::default())
                .expect("recover crashed import");
            assert_eq!(
                store.tasks().expect("recovered tasks").len(),
                if committed { 2 } else { 0 }
            );
            if committed {
                for entry in [import_metadata(1, 0), import_metadata(2, 1)] {
                    store
                        .confirm_task_metadata(&entry, &|_: &str| true)
                        .expect("whole metadata recovery");
                }
            }
        }
    }

    #[test]
    fn import_batch_crash_child() {
        let Some(database) = std::env::var_os("ARIAX_IMPORT_CRASH_DATABASE") else {
            return;
        };
        let committed =
            std::env::var("ARIAX_IMPORT_CRASH_COMMITTED").expect("crash point") == "true";
        let mut store = SessionStore::open(PathBuf::from(database), SessionStoreConfig::default())
            .expect("child store");
        IMPORT_CRASH_POINT.with(|point| point.set(Some(if committed { usize::MAX } else { 0 })));
        if std::env::var("ARIAX_IMPORT_CRASH_MIXED").as_deref() == Ok("true") {
            store
                .create_session_batch(&mixed_import_metadata(), &|_: &str| true)
                .unwrap();
            panic!("mixed import crash checkpoint was not reached");
        }
        store
            .create_task_batch(
                &[import_metadata(1, 0), import_metadata(2, 1)],
                &|_: &str| true,
            )
            .expect("crash injection");
        panic!("import crash checkpoint was not reached");
    }

    #[test]
    fn task_source_per_task_byte_budget_is_exact_and_revalidated_on_open() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");

        let empty_row_bytes = super::task_source_owned_bytes(0).expect("empty row size");
        let full_row_bytes = super::task_source_owned_bytes(super::SESSION_MAX_SAFE_URI_BYTES)
            .expect("full row size");
        let full_rows =
            (super::SESSION_SOURCE_READ_BUDGET_BYTES - empty_row_bytes) / full_row_bytes;
        let final_uri_bytes =
            super::SESSION_SOURCE_READ_BUDGET_BYTES - full_rows * full_row_bytes - empty_row_bytes;
        assert!(final_uri_bytes <= super::SESSION_MAX_SAFE_URI_BYTES);

        let mut sources = Vec::with_capacity(full_rows + 2);
        for index in 0..full_rows {
            sources.push(SessionTaskSourceRecord {
                uri_id: u32::try_from(index).expect("bounded source id"),
                persistence_safe_uri: Some("x".repeat(super::SESSION_MAX_SAFE_URI_BYTES)),
                redacted_fingerprint: [7; 32],
                needs_credentials: false,
                priority: i64::try_from(index).expect("bounded priority"),
            });
        }
        sources.push(SessionTaskSourceRecord {
            uri_id: u32::try_from(full_rows).expect("final source id"),
            persistence_safe_uri: Some("y".repeat(final_uri_bytes)),
            redacted_fingerprint: [8; 32],
            needs_credentials: true,
            priority: i64::try_from(full_rows).expect("final priority"),
        });
        store
            .replace_task_sources(gid(1), &sources)
            .expect("persist sources at exact byte budget");
        assert_eq!(
            store
                .task_sources(gid(1))
                .expect("read sources at exact byte budget")
                .len(),
            sources.len()
        );

        let extra_uri_id = u32::try_from(sources.len()).expect("extra source id");
        sources.push(SessionTaskSourceRecord {
            uri_id: extra_uri_id,
            persistence_safe_uri: None,
            redacted_fingerprint: [9; 32],
            needs_credentials: true,
            priority: i64::from(extra_uri_id),
        });
        assert!(matches!(
            store.replace_task_sources(gid(1), &sources),
            Err(SessionStoreError::InvalidRecord("task_source.bytes"))
        ));
        assert_eq!(
            store
                .task_sources(gid(1))
                .expect("rejected replacement retained exact-budget sources")
                .len(),
            sources.len() - 1
        );

        store
            .connection
            .execute(
                "INSERT INTO task_source(gid, uri_id, persistence_safe_uri, redacted_fingerprint, needs_credentials, priority) VALUES (?1, ?2, NULL, ?3, 1, ?4)",
                rusqlite::params![
                    gid(1).to_string(),
                    i64::from(extra_uri_id),
                    [9_u8; 32].as_slice(),
                    i64::from(extra_uri_id),
                ],
            )
            .expect("simulate persisted per-task byte overflow");
        assert!(matches!(
            super::validate_task_sources(&store.connection),
            Err(SessionStoreError::InvalidPersistedValue(
                "task_source.bytes"
            ))
        ));
        assert!(matches!(
            store.task_sources(gid(1)),
            Err(SessionStoreError::InvalidPersistedValue(
                "task_source.bytes"
            ))
        ));
        drop(store);
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::InvalidPersistedValue(
                "task_source.bytes"
            ))
        ));
    }

    #[test]
    fn startup_task_source_global_budget_charges_empty_sets_and_owned_rows_exactly() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store
            .put_task(&task_record(gid(1), 0))
            .expect("source task");
        store.put_task(&task_record(gid(2), 1)).expect("empty task");
        let source = SessionTaskSourceRecord {
            uri_id: 7,
            persistence_safe_uri: Some("https://mirror.example/file".to_owned()),
            redacted_fingerprint: [7; 32],
            needs_credentials: false,
            priority: 0,
        };
        store
            .replace_task_sources(gid(1), std::slice::from_ref(&source))
            .expect("persist source");

        let exact_budget = 2 * std::mem::size_of::<super::SessionTaskSourceSet>()
            + super::task_source_owned_bytes(
                source.persistence_safe_uri.as_ref().map_or(0, String::len),
            )
            .expect("row bytes");
        assert_eq!(
            super::read_task_source_sets_with_budget(&store.connection, exact_budget)
                .expect("exact global budget")
                .len(),
            2
        );
        assert!(matches!(
            super::read_task_source_sets_with_budget(&store.connection, exact_budget - 1),
            Err(SessionStoreError::InvalidPersistedValue(
                "task_source.read_budget"
            ))
        ));
    }

    #[test]
    fn host_key_resolution_is_exact_challenge_bound_and_atomic() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let mut task = task_record(gid(1), 0);
        task.queue_state = SessionQueueState::Paused;
        task.desired_paused = true;
        task.no_space = None;
        store.put_task(&task).expect("paused task");
        let key = vec![7_u8; 64];
        let challenge = SessionHostKeyChallengeRecord {
            gid: gid(1),
            challenge_id: HostKeyChallengeId::new([1; 16]),
            canonical_host: "sftp.example".to_owned(),
            port: 22,
            algorithm: "ssh-ed25519".to_owned(),
            fingerprint_sha256: HostKeyFingerprint::for_presented_key(&key),
            presented_public_key: key.clone(),
            created_ms: 300,
        };
        store
            .put_host_key_challenge(&challenge)
            .expect("persist challenge");
        assert_eq!(
            store.host_key_challenge(gid(1)).expect("challenge"),
            Some(challenge.clone())
        );
        assert!(matches!(
            store.reject_host_key_challenge(gid(1), HostKeyChallengeId::new([2; 16])),
            Err(SessionStoreError::HostKeyChallengeMismatch)
        ));

        let pin_value = super::session_host_key_pin_value(challenge.fingerprint_sha256);
        let options = SanitizedOptionMap::new([
            (super::SESSION_HOST_KEY_PIN_OPTION.to_owned(), pin_value),
            ("piece-length".to_owned(), "1M".to_owned()),
        ])
        .expect("pinned options");
        let mut stale = SessionHostKeyResolution {
            gid: gid(1),
            challenge_id: challenge.challenge_id,
            fingerprint_sha256: HostKeyFingerprint::new([9; 32]),
            presented_public_key: key.clone(),
            scope: OptionsSnapshotScope::NextAdmission,
            pinned_options: options.clone(),
        };
        assert!(matches!(
            store.resolve_host_key_challenge(&stale, &|_: &str| true),
            Err(SessionStoreError::HostKeyChallengeMismatch)
        ));
        assert_eq!(
            store
                .host_key_challenge(gid(1))
                .expect("retained challenge"),
            Some(challenge.clone())
        );
        stale.fingerprint_sha256 = challenge.fingerprint_sha256;
        store
            .resolve_host_key_challenge(&stale, &|_: &str| true)
            .expect("resolve exact challenge");
        assert_eq!(store.host_key_challenge(gid(1)).expect("cleared"), None);
        assert_eq!(
            store
                .task_options(gid(1), OptionsSnapshotScope::NextAdmission, &|_: &str| true,)
                .expect("pinned snapshot"),
            options
        );
        assert!(matches!(
            store.resolve_host_key_challenge(&stale, &|_: &str| true),
            Err(SessionStoreError::NotFound)
        ));
    }

    #[test]
    fn retained_host_keys_block_nonterminal_departure_and_clear_with_terminal_state() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        for (position, value) in [1_u64, 2].into_iter().enumerate() {
            let mut task = task_record(gid(value), position as u32);
            task.queue_state = SessionQueueState::Paused;
            task.desired_paused = false;
            task.no_space = None;
            store.put_task(&task).expect("paused host-key task");
            let key = vec![value as u8; 32];
            store
                .put_host_key_challenge(&SessionHostKeyChallengeRecord {
                    gid: task.gid,
                    challenge_id: HostKeyChallengeId::new([value as u8; 16]),
                    canonical_host: format!("sftp-{value}.example"),
                    port: 22,
                    algorithm: "ssh-ed25519".to_owned(),
                    fingerprint_sha256: HostKeyFingerprint::for_presented_key(&key),
                    presented_public_key: key,
                    created_ms: 300,
                })
                .expect("host-key challenge");
        }
        store
            .put_task(&task_record(gid(3), 0))
            .expect("waiting task");

        assert!(matches!(
            store.transition_task_queue(
                gid(1),
                SessionQueueState::Paused,
                SessionQueueState::Waiting,
                1,
                false,
                0,
                None,
                400,
            ),
            Err(SessionStoreError::InvalidRecord(
                "host_key_challenge.queue_state"
            ))
        ));
        let departure = SessionQueueTransition {
            gid: gid(1),
            expected_state: SessionQueueState::Paused,
            target_state: SessionQueueState::Waiting,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Paused,
                    gids: vec![gid(2)],
                },
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: vec![gid(3), gid(1)],
                },
            ],
            updated_ms: 400,
        };
        assert!(matches!(
            store.transition_task_queue_exact(&departure),
            Err(SessionStoreError::InvalidRecord(
                "host_key_challenge.queue_state"
            ))
        ));
        assert_eq!(
            store
                .queue_order(SessionQueueState::Paused)
                .expect("unchanged paused queue"),
            vec![gid(1), gid(2)]
        );
        assert!(
            store
                .host_key_challenge(gid(1))
                .expect("retained first challenge")
                .is_some()
        );

        let first_result = stopped_result_record(gid(1), SessionTerminalStatus::Removed);
        store
            .persist_stopped_result(&first_result, SessionQueueState::Paused, 0, false, 0, 500)
            .expect("terminalize first challenge");
        assert_eq!(
            store
                .host_key_challenge(gid(1))
                .expect("cleared first challenge"),
            None
        );

        let second_result = stopped_result_record(gid(2), SessionTerminalStatus::Removed);
        let mut exact_terminal = SessionQueueTransition {
            gid: gid(2),
            expected_state: SessionQueueState::Paused,
            target_state: SessionQueueState::Stopped,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Paused,
                    gids: Vec::new(),
                },
                SessionQueueOrder {
                    state: SessionQueueState::Stopped,
                    gids: vec![gid(1)],
                },
            ],
            updated_ms: 600,
        };
        assert!(matches!(
            store.persist_stopped_result_exact(&second_result, &exact_terminal),
            Err(SessionStoreError::QueueInvariant)
        ));
        assert!(
            store
                .host_key_challenge(gid(2))
                .expect("rolled-back second challenge")
                .is_some()
        );
        exact_terminal.final_orders[1].gids.push(gid(2));
        store
            .persist_stopped_result_exact(&second_result, &exact_terminal)
            .expect("terminalize second challenge exactly");
        assert_eq!(
            store
                .host_key_challenge(gid(2))
                .expect("cleared second challenge"),
            None
        );
        assert_eq!(
            store.stopped_results().expect("paired terminal results"),
            vec![first_result, second_result]
        );
    }

    #[test]
    fn exact_stopped_result_transition_verifies_both_final_orders() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("first");
        store.put_task(&task_record(gid(2), 1)).expect("second");
        let transition = SessionQueueTransition {
            gid: gid(1),
            expected_state: SessionQueueState::Waiting,
            target_state: SessionQueueState::Stopped,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: vec![gid(2)],
                },
                SessionQueueOrder {
                    state: SessionQueueState::Stopped,
                    gids: vec![gid(1)],
                },
            ],
            updated_ms: 400,
        };
        let result = stopped_result_record(gid(1), SessionTerminalStatus::Complete);
        store
            .persist_stopped_result_exact(&result, &transition)
            .expect("exact stopped transition");
        assert_eq!(store.stopped_results().expect("result"), vec![result]);
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
                .task_options(gid(1), OptionsSnapshotScope::CurrentGeneration, &policy)
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
    fn source_replacement_and_queue_transition_commit_or_rollback_together() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        let old = SessionTaskSourceRecord {
            uri_id: 0,
            persistence_safe_uri: Some("https://old.example/file".to_owned()),
            redacted_fingerprint: [1; 32],
            needs_credentials: false,
            priority: 0,
        };
        let next = SessionTaskSourceRecord {
            persistence_safe_uri: Some("https://new.example/file".to_owned()),
            redacted_fingerprint: [2; 32],
            ..old.clone()
        };
        store
            .replace_task_sources(gid(1), std::slice::from_ref(&old))
            .expect("old sources");
        let transition = SessionQueueTransition {
            gid: gid(1),
            expected_state: SessionQueueState::Waiting,
            target_state: SessionQueueState::Paused,
            desired_paused: true,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: Vec::new(),
                },
                SessionQueueOrder {
                    state: SessionQueueState::Paused,
                    gids: vec![gid(1)],
                },
            ],
            updated_ms: 300,
        };
        let mut wrong = transition.clone();
        wrong.final_orders[1].gids.push(gid(2));
        assert!(
            store
                .replace_task_sources_and_queue(&wrong, std::slice::from_ref(&next))
                .is_err()
        );
        assert_eq!(
            store.task_sources(gid(1)).expect("old after wrong queue"),
            vec![old.clone()]
        );
        store.connection.execute_batch("CREATE TEMP TRIGGER reject_source_insert BEFORE INSERT ON task_source BEGIN SELECT RAISE(ABORT, 'injected source write failure'); END;").expect("inject write failure");
        assert!(
            store
                .replace_task_sources_and_queue(&transition, std::slice::from_ref(&next))
                .is_err()
        );
        assert_eq!(
            store
                .task_sources(gid(1))
                .expect("old sources after rollback"),
            vec![old]
        );
        let task = store.tasks().expect("tasks").remove(0);
        assert_eq!(task.queue_state, SessionQueueState::Waiting);
        assert!(!task.desired_paused);
        store
            .connection
            .execute_batch("DROP TRIGGER reject_source_insert;")
            .expect("remove fault");
        store
            .replace_task_sources_and_queue(&transition, std::slice::from_ref(&next))
            .expect("commit both");
        drop(store);
        let store = open_store(&directory);
        assert_eq!(
            store.task_sources(gid(1)).expect("durable sources"),
            vec![next]
        );
        let task = store.tasks().expect("tasks").remove(0);
        assert_eq!(task.queue_state, SessionQueueState::Paused);
        assert!(task.desired_paused);
    }

    #[test]
    fn option_promotion_requires_exact_staging_and_rolls_back_both_scopes() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        let policy = ariax_config::persisted_option_is_safe;
        let current =
            SanitizedOptionMap::new([("split".to_owned(), "2".to_owned())]).expect("current");
        let next = SanitizedOptionMap::new([("split".to_owned(), "4".to_owned())]).expect("next");
        for (scope, options) in [
            (OptionsSnapshotScope::CurrentGeneration, &current),
            (OptionsSnapshotScope::NextAdmission, &next),
        ] {
            store
                .replace_task_options(gid(1), scope, options, &policy)
                .expect("seed options");
        }
        assert!(matches!(
            store.promote_task_options(gid(1), &current, &policy),
            Err(SessionStoreError::InvalidRecord(
                "task_option.promotion_mismatch"
            ))
        ));
        assert!(matches!(
            store.promote_task_options(gid(2), &next, &policy),
            Err(SessionStoreError::NotFound)
        ));
        let secret = SanitizedOptionMap::new([("rpc-secret".to_owned(), "canary".to_owned())])
            .expect("secret map");
        assert!(matches!(
            store.promote_task_options(gid(1), &secret, &policy),
            Err(SessionStoreError::ForbiddenPersistedOption)
        ));

        store.connection.execute_batch(&format!(
            "CREATE TEMP TRIGGER reject_option_promotion BEFORE DELETE ON task_option WHEN OLD.scope = {} BEGIN SELECT RAISE(ABORT, 'injected promotion failure'); END;",
            OptionsSnapshotScope::NextAdmission.number(),
        )).expect("inject delete failure");
        assert!(matches!(
            store.promote_task_options(gid(1), &next, &policy),
            Err(SessionStoreError::Sqlite(_))
        ));
        assert_eq!(
            store
                .task_options(gid(1), OptionsSnapshotScope::CurrentGeneration, &policy)
                .expect("current after rollback"),
            current
        );
        assert_eq!(
            store
                .task_options(gid(1), OptionsSnapshotScope::NextAdmission, &policy)
                .expect("staged after rollback"),
            next
        );
        store
            .connection
            .execute_batch("DROP TRIGGER reject_option_promotion;")
            .expect("remove fault");
        store
            .promote_task_options(gid(1), &next, &policy)
            .expect("promote exact snapshot");
        assert!(matches!(
            store.promote_task_options(gid(1), &next, &policy),
            Err(SessionStoreError::InvalidRecord(
                "task_option.promotion_mismatch"
            ))
        ));
        drop(store);
        let store = open_store(&directory);
        assert_eq!(
            store
                .task_options(gid(1), OptionsSnapshotScope::CurrentGeneration, &policy)
                .expect("durable promoted options"),
            next
        );
        assert_eq!(
            store
                .task_options(gid(1), OptionsSnapshotScope::NextAdmission, &policy)
                .expect("durable cleared staging")
                .entries()
                .len(),
            0
        );
    }

    #[test]
    fn option_reads_reapply_policy_to_tampered_rows() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("task");
        store
            .connection
            .execute(
                "INSERT INTO task_option(gid, scope, key, canonical_value) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    gid(1).to_string(),
                    OptionsSnapshotScope::CurrentGeneration.number(),
                    "rpc-secret",
                    b"tampered-secret".as_slice(),
                ],
            )
            .expect("tamper option row");
        let registry = builtin_registry();
        let policy = |name: &str| {
            registry
                .find(name)
                .is_some_and(|definition| definition.security == SecurityClass::Normal)
        };
        assert!(matches!(
            store.task_options(gid(1), OptionsSnapshotScope::CurrentGeneration, &policy),
            Err(SessionStoreError::ForbiddenPersistedOption)
        ));
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

        let authoritative_cache = SessionJournalCache {
            layout_hash: Some(hash(10)),
            root_binding_hash: Some(hash(11)),
            snapshot_hash: hash(12),
        };
        let authoritative_root = path(b"/authoritative/root");
        store
            .reconcile_journal_authority(
                gid(1),
                task.primary_journal_id,
                Some(authoritative_cache),
                Some(&authoritative_root),
                500,
            )
            .expect("atomic authority repair");
        let repaired = store.tasks().expect("repaired tasks").pop().expect("task");
        assert_eq!(repaired.queue_state, task.queue_state);
        assert_eq!(repaired.queue_position, task.queue_position);
        assert_eq!(repaired.primary_journal_id, task.primary_journal_id);
        assert_eq!(repaired.primary_journal_path, task.primary_journal_path);
        assert_eq!(repaired.root_display, authoritative_root);
        assert_eq!(repaired.cached_layout_hash, authoritative_cache.layout_hash);
        assert_eq!(
            repaired.cached_root_binding_hash,
            authoritative_cache.root_binding_hash
        );
        assert_eq!(
            repaired.cached_snapshot_hash,
            authoritative_cache.snapshot_hash
        );

        let before_rejection = repaired;
        assert!(matches!(
            store.reconcile_journal_authority(
                gid(1),
                journal(99),
                Some(cache),
                Some(&path(b"/wrong/root")),
                600,
            ),
            Err(SessionStoreError::JournalPointerMismatch)
        ));
        assert_eq!(
            store.tasks().expect("rejected repair").pop().expect("task"),
            before_rejection
        );
        assert!(matches!(
            store.reconcile_journal_authority(gid(1), task.primary_journal_id, None, None, 700,),
            Err(SessionStoreError::InvalidRecord("journal_authority.empty"))
        ));
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
        let token = store.begin_journal_install(&intent).expect("begin install");
        assert_eq!(
            store.journal_installs().expect("intents"),
            vec![intent.clone()]
        );
        assert_eq!(
            store.tasks().expect("tasks")[0].primary_journal_id,
            intent.old_journal_id
        );
        store
            .complete_journal_install(token, 400)
            .expect("complete install");
        let installed = store.journal_installs().expect("installed");
        assert_eq!(installed[0].phase, JournalInstallPhase::Installed);
        let updated = store.tasks().expect("tasks").pop().expect("task");
        assert_eq!(updated.primary_journal_id, intent.new_journal_id);
        assert_eq!(updated.primary_journal_path, intent.new_path);
        store
            .clear_installed_journal(token)
            .expect("retirement complete");
        assert!(store.journal_installs().expect("cleared").is_empty());
    }

    #[test]
    fn journal_install_abort_rechecks_token_and_retained_pointer() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let task = task_record(gid(1), 0);
        store.put_task(&task).expect("task");
        let intent = JournalInstallIntent {
            gid: task.gid,
            checkpoint_id: CheckpointId::new([13; 16]).expect("checkpoint"),
            old_journal_id: task.primary_journal_id,
            old_path: task.primary_journal_path.clone(),
            new_journal_id: journal(14),
            new_path: path(b"/journal/rejected"),
            source_last_sequence: 9,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        let token = store.begin_journal_install(&intent).expect("begin install");
        let stale = JournalInstallToken {
            new_journal_id: journal(15),
            ..token
        };
        assert!(matches!(
            store.abort_journal_install(stale),
            Err(SessionStoreError::JournalInstallConflict)
        ));
        store
            .abort_journal_install(token)
            .expect("abort exact install");
        assert!(store.journal_installs().expect("cleared").is_empty());
        let current = store.tasks().expect("tasks").remove(0);
        assert_eq!(current.primary_journal_id, task.primary_journal_id);
        assert_eq!(current.primary_journal_path, task.primary_journal_path);
    }

    #[test]
    fn journal_install_abort_rejects_pointer_change() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let task = task_record(gid(1), 0);
        store.put_task(&task).expect("task");
        let intent = JournalInstallIntent {
            gid: task.gid,
            checkpoint_id: CheckpointId::new([16; 16]).expect("checkpoint"),
            old_journal_id: task.primary_journal_id,
            old_path: task.primary_journal_path.clone(),
            new_journal_id: journal(17),
            new_path: path(b"/journal/rejected"),
            source_last_sequence: 9,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        let token = store.begin_journal_install(&intent).expect("begin install");
        store
            .connection
            .execute(
                "UPDATE task SET primary_journal_id = ?1 WHERE gid = ?2",
                rusqlite::params![journal(18).as_bytes().as_slice(), task.gid.to_string()],
            )
            .expect("simulate pointer corruption");
        assert!(matches!(
            store.abort_journal_install(token),
            Err(SessionStoreError::JournalPointerMismatch)
        ));
        assert_eq!(store.journal_installs().expect("intent").len(), 1);
    }

    #[test]
    fn journal_install_tokens_survive_reopen_and_reject_stale_commands() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let first_task = task_record(gid(1), 0);
        store.put_task(&first_task).expect("task");
        let first = JournalInstallIntent {
            gid: first_task.gid,
            checkpoint_id: CheckpointId::new([31; 16]).expect("checkpoint"),
            old_journal_id: first_task.primary_journal_id,
            old_path: first_task.primary_journal_path.clone(),
            new_journal_id: journal(32),
            new_path: path(b"/journal/first"),
            source_last_sequence: 10,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        let first_token = store.begin_journal_install(&first).expect("begin first");
        drop(store);

        let mut store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("recover installing intent");
        store
            .complete_journal_install(first_token, 400)
            .expect("complete recovered intent");
        drop(store);

        let mut store = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("recover installed intent");
        store
            .clear_installed_journal(first_token)
            .expect("retire recovered intent");
        let current = store.tasks().expect("current task").remove(0);
        let second = JournalInstallIntent {
            gid: current.gid,
            checkpoint_id: CheckpointId::new([33; 16]).expect("checkpoint"),
            old_journal_id: current.primary_journal_id,
            old_path: current.primary_journal_path,
            new_journal_id: journal(34),
            new_path: path(b"/journal/second"),
            source_last_sequence: 20,
            phase: JournalInstallPhase::Installing,
            created_ms: 500,
        };
        let second_token = store.begin_journal_install(&second).expect("begin second");
        assert!(matches!(
            store.complete_journal_install(first_token, 600),
            Err(SessionStoreError::JournalInstallConflict)
        ));
        assert!(matches!(
            store.clear_installed_journal(first_token),
            Err(SessionStoreError::JournalInstallConflict)
        ));
        store
            .complete_journal_install(second_token, 600)
            .expect("complete current intent");
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
        let token = store.begin_journal_install(&intent).expect("begin install");
        task.primary_journal_id = journal(23);
        task.primary_journal_path = path(b"/journal/other-authority");
        task.updated_ms = 350;
        assert!(matches!(
            store.put_task(&task),
            Err(SessionStoreError::JournalPointerMismatch)
        ));
        store
            .connection
            .execute(
                "UPDATE task SET primary_journal_id = ?1, primary_journal_path = ?2, updated_ms = ?3 WHERE gid = ?4",
                rusqlite::params![
                    task.primary_journal_id.as_bytes().as_slice(),
                    super::encode_platform_path(&task.primary_journal_path).expect("encode path"),
                    350_i64,
                    task.gid.to_string(),
                ],
            )
            .expect("simulate external pointer corruption");
        assert!(matches!(
            store.complete_journal_install(token, 400),
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
    fn journal_install_retirement_rechecks_the_new_authoritative_pointer() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        let task = task_record(gid(1), 0);
        store.put_task(&task).expect("task");
        let intent = JournalInstallIntent {
            gid: task.gid,
            checkpoint_id: CheckpointId::new([41; 16]).expect("checkpoint"),
            old_journal_id: task.primary_journal_id,
            old_path: task.primary_journal_path.clone(),
            new_journal_id: journal(42),
            new_path: path(b"/journal/installed"),
            source_last_sequence: 11,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        let token = store.begin_journal_install(&intent).expect("begin install");
        store
            .complete_journal_install(token, 400)
            .expect("complete install");
        store
            .connection
            .execute(
                "UPDATE task SET primary_journal_id = ?1, primary_journal_path = ?2, updated_ms = ?3 WHERE gid = ?4",
                rusqlite::params![
                    task.primary_journal_id.as_bytes().as_slice(),
                    super::encode_platform_path(&task.primary_journal_path).expect("encode path"),
                    450_i64,
                    task.gid.to_string(),
                ],
            )
            .expect("simulate external pointer corruption");
        assert!(matches!(
            store.clear_installed_journal(token),
            Err(SessionStoreError::JournalPointerMismatch)
        ));
        assert_eq!(store.journal_installs().expect("intent").len(), 1);
    }

    #[test]
    fn journal_install_pointer_relations_are_validated_on_reopen() {
        let installing_directory = TestDirectory::new();
        let mut installing_store = open_store(&installing_directory);
        let installing_task = task_record(gid(1), 0);
        installing_store
            .put_task(&installing_task)
            .expect("installing task");
        let installing = JournalInstallIntent {
            gid: installing_task.gid,
            checkpoint_id: CheckpointId::new([51; 16]).expect("checkpoint"),
            old_journal_id: installing_task.primary_journal_id,
            old_path: installing_task.primary_journal_path.clone(),
            new_journal_id: journal(52),
            new_path: path(b"/journal/installing-new"),
            source_last_sequence: 12,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        installing_store
            .begin_journal_install(&installing)
            .expect("begin installing intent");
        installing_store
            .connection
            .execute(
                "UPDATE task SET primary_journal_id = ?1, primary_journal_path = ?2 WHERE gid = ?3",
                rusqlite::params![
                    installing.new_journal_id.as_bytes().as_slice(),
                    super::encode_platform_path(&installing.new_path).expect("encode new path"),
                    installing.gid.to_string(),
                ],
            )
            .expect("corrupt installing pointer");
        drop(installing_store);
        assert!(matches!(
            SessionStore::open(
                installing_directory.database(),
                SessionStoreConfig::default()
            ),
            Err(SessionStoreError::JournalPointerMismatch)
        ));

        let installed_directory = TestDirectory::new();
        let mut installed_store = open_store(&installed_directory);
        let installed_task = task_record(gid(2), 0);
        installed_store
            .put_task(&installed_task)
            .expect("installed task");
        let installed = JournalInstallIntent {
            gid: installed_task.gid,
            checkpoint_id: CheckpointId::new([53; 16]).expect("checkpoint"),
            old_journal_id: installed_task.primary_journal_id,
            old_path: installed_task.primary_journal_path.clone(),
            new_journal_id: journal(54),
            new_path: path(b"/journal/installed-new"),
            source_last_sequence: 13,
            phase: JournalInstallPhase::Installing,
            created_ms: 300,
        };
        let installed_token = installed_store
            .begin_journal_install(&installed)
            .expect("begin installed intent");
        installed_store
            .complete_journal_install(installed_token, 400)
            .expect("complete installed intent");
        installed_store
            .connection
            .execute(
                "UPDATE task SET primary_journal_id = ?1, primary_journal_path = ?2 WHERE gid = ?3",
                rusqlite::params![
                    installed.old_journal_id.as_bytes().as_slice(),
                    super::encode_platform_path(&installed.old_path).expect("encode old path"),
                    installed.gid.to_string(),
                ],
            )
            .expect("corrupt installed pointer");
        drop(installed_store);
        assert!(matches!(
            SessionStore::open(
                installed_directory.database(),
                SessionStoreConfig::default()
            ),
            Err(SessionStoreError::JournalPointerMismatch)
        ));
    }

    #[test]
    fn hot_backup_is_complete_refuses_overwrite_and_passes_integrity() {
        for prefer_wal in [true, false] {
            let directory = TestDirectory::new();
            let mut store = SessionStore::open(
                directory.database(),
                SessionStoreConfig {
                    prefer_wal,
                    ..SessionStoreConfig::default()
                },
            )
            .expect("open source store");
            store.put_session(&session_record()).expect("put session");
            store.put_task(&task_record(gid(1), 0)).expect("task");
            assert_eq!(
                store.journal_mode(),
                if prefer_wal {
                    SessionJournalMode::Wal
                } else {
                    SessionJournalMode::Delete
                }
            );
            let backup = directory.path().join("session.backup.db");
            store.backup_to(&backup).expect("backup");
            assert!(matches!(
                store.backup_to(&backup),
                Err(SessionStoreError::BackupPathExists)
            ));
            assert!(
                directory_entry_names(directory.path())
                    .iter()
                    .all(|name| !name.to_string_lossy().contains(".ariax-backup-"))
            );
            for suffix in ["-wal", "-shm", "-journal"] {
                assert!(!super::sqlite_sidecar_path(&backup, suffix).exists());
            }
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
    }

    #[test]
    fn hot_backup_unlink_failure_is_verified_and_reconciled_on_retry() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_session(&session_record()).expect("put session");
        store.put_task(&task_record(gid(1), 0)).expect("put task");
        let destination = directory.path().join("unlink-failure.backup.db");

        super::BACKUP_FAIL_NEXT_UNLINK.with(|fault| fault.set(true));
        assert!(matches!(
            store.backup_to(&destination),
            Err(SessionStoreError::Io {
                operation: super::SessionIoOperation::RemoveFailedBackup,
                ..
            })
        ));
        let capability = crate::JournalDirectoryCapability::open_trusted(directory.path())
            .expect("open backup directory capability");
        let candidates = super::discover_backup_publication_candidates(&capability, &destination)
            .expect("discover failed-unlink candidate");
        assert_eq!(candidates.len(), 1);
        assert!(destination.exists());

        assert!(matches!(
            store.backup_to(&destination),
            Err(SessionStoreError::BackupPathExists)
        ));
        assert!(
            super::discover_backup_publication_candidates(&capability, &destination)
                .expect("discover reconciled candidates")
                .is_empty()
        );
        let backup = SessionStore::open(
            &destination,
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("open reconciled backup");
        assert_eq!(backup.tasks().expect("backup tasks").len(), 1);
    }

    #[test]
    fn hot_backup_temp_only_residue_is_published_without_rebuilding() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_session(&session_record()).expect("put session");
        store.put_task(&task_record(gid(1), 0)).expect("put task");
        let destination = directory.path().join("temp-only.backup.db");

        super::BACKUP_FAIL_NEXT_UNLINK.with(|fault| fault.set(true));
        assert!(store.backup_to(&destination).is_err());
        fs::remove_file(&destination).expect("remove published name to model pre-link crash");
        let capability = crate::JournalDirectoryCapability::open_trusted(directory.path())
            .expect("open backup directory capability");
        assert_eq!(
            super::discover_backup_publication_candidates(&capability, &destination)
                .expect("discover temp-only candidate")
                .len(),
            1
        );

        assert!(matches!(
            store.backup_to(&destination),
            Err(SessionStoreError::BackupPathExists)
        ));
        assert!(destination.exists());
        assert!(
            super::discover_backup_publication_candidates(&capability, &destination)
                .expect("discover after temp-only recovery")
                .is_empty()
        );
        let backup = SessionStore::open(
            &destination,
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("open recovered temp-only backup");
        assert_eq!(backup.tasks().expect("backup tasks").len(), 1);
    }

    #[test]
    fn hot_backup_recovery_preserves_a_raced_destination_replacement() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_session(&session_record()).expect("put session");
        let destination = directory.path().join("raced.backup.db");

        super::BACKUP_FAIL_NEXT_UNLINK.with(|fault| fault.set(true));
        assert!(store.backup_to(&destination).is_err());
        let capability = crate::JournalDirectoryCapability::open_trusted(directory.path())
            .expect("open backup directory capability");
        let candidates = super::discover_backup_publication_candidates(&capability, &destination)
            .expect("discover publication candidate");
        assert_eq!(candidates.len(), 1);
        let candidate_before = fs::read(&candidates[0].path).expect("read candidate before race");
        fs::remove_file(&destination).expect("remove published destination");
        fs::write(&destination, b"raced replacement").expect("install raced replacement");

        assert!(matches!(
            store.backup_to(&destination),
            Err(SessionStoreError::InvalidPersistedValue(
                "backup.publication_candidate"
            ))
        ));
        assert_eq!(
            fs::read(&destination).expect("preserved raced destination"),
            b"raced replacement"
        );
        assert_eq!(
            fs::read(&candidates[0].path).expect("preserved publication candidate"),
            candidate_before
        );
    }

    #[test]
    fn hot_backup_recovery_rejects_unaccounted_hard_links_without_cleanup() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_session(&session_record()).expect("put session");
        let destination = directory.path().join("extra-link.backup.db");

        super::BACKUP_FAIL_NEXT_UNLINK.with(|fault| fault.set(true));
        assert!(store.backup_to(&destination).is_err());
        let capability = crate::JournalDirectoryCapability::open_trusted(directory.path())
            .expect("open backup directory capability");
        let candidates = super::discover_backup_publication_candidates(&capability, &destination)
            .expect("discover publication candidate");
        assert_eq!(candidates.len(), 1);
        let unexpected = directory.path().join("unexpected-backup-alias.db");
        fs::hard_link(&destination, &unexpected).expect("add unaccounted hard link");

        assert!(matches!(
            store.backup_to(&destination),
            Err(SessionStoreError::InvalidPersistedValue(
                "backup.publication_candidate"
            ))
        ));
        assert!(destination.exists());
        assert!(candidates[0].path.exists());
        assert!(unexpected.exists());
    }

    #[test]
    fn hot_backup_recovery_preserves_invalid_same_file_residue() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_session(&session_record()).expect("put session");
        let destination = directory.path().join("invalid-residue.backup.db");

        super::BACKUP_FAIL_NEXT_UNLINK.with(|fault| fault.set(true));
        assert!(store.backup_to(&destination).is_err());
        let capability = crate::JournalDirectoryCapability::open_trusted(directory.path())
            .expect("open backup directory capability");
        let candidates = super::discover_backup_publication_candidates(&capability, &destination)
            .expect("discover publication candidate");
        assert_eq!(candidates.len(), 1);
        fs::write(&destination, b"not a SQLite database").expect("corrupt published backup");
        let invalid = fs::read(&destination).expect("read invalid residue");

        assert!(store.backup_to(&destination).is_err());
        assert_eq!(
            fs::read(&destination).expect("preserved invalid destination"),
            invalid
        );
        assert_eq!(
            fs::read(&candidates[0].path).expect("preserved invalid candidate"),
            invalid
        );
    }

    #[test]
    fn hot_backup_publication_crash_matrix_recovers_a_usable_destination() {
        for (phase, exit_code) in [
            ("after_link", 121),
            ("after_link_sync", 122),
            ("after_unlink", 123),
        ] {
            let directory = TestDirectory::new();
            let mut store = open_store(&directory);
            store.put_session(&session_record()).expect("put session");
            store.put_task(&task_record(gid(1), 0)).expect("put task");
            drop(store);
            let destination = directory.path().join(format!("{phase}.backup.db"));
            let status = Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--ignored",
                    "--exact",
                    "session_store::tests::hot_backup_publication_child",
                    "--nocapture",
                ])
                .env("ARIAX_BACKUP_CRASH_DATABASE", directory.database())
                .env("ARIAX_BACKUP_CRASH_DESTINATION", &destination)
                .env("ARIAX_BACKUP_CRASH_PHASE", phase)
                .status()
                .expect("spawn backup crash child");
            assert_eq!(status.code(), Some(exit_code), "{phase}");

            let source = open_store(&directory);
            assert!(matches!(
                source.backup_to(&destination),
                Err(SessionStoreError::BackupPathExists)
            ));
            let capability = crate::JournalDirectoryCapability::open_trusted(directory.path())
                .expect("open backup directory capability");
            assert!(
                super::discover_backup_publication_candidates(&capability, &destination)
                    .expect("discover post-crash candidates")
                    .is_empty(),
                "{phase}"
            );
            let recovered = SessionStore::open(
                &destination,
                SessionStoreConfig {
                    prefer_wal: false,
                    ..SessionStoreConfig::default()
                },
            )
            .expect("open crash-recovered backup");
            assert_eq!(recovered.tasks().expect("backup tasks").len(), 1, "{phase}");
        }
    }

    #[test]
    #[ignore = "spawned by hot_backup_publication_crash_matrix_recovers_a_usable_destination"]
    fn hot_backup_publication_child() {
        let Some(database) = std::env::var_os("ARIAX_BACKUP_CRASH_DATABASE") else {
            return;
        };
        let destination =
            std::env::var_os("ARIAX_BACKUP_CRASH_DESTINATION").expect("backup crash destination");
        let store = SessionStore::open(PathBuf::from(database), SessionStoreConfig::default())
            .expect("child open source store");
        let result = store.backup_to(PathBuf::from(destination));
        panic!("backup crash hook failed to exit: {result:?}");
    }

    #[test]
    fn backup_validation_failure_removes_temporary_database_and_sidecars() {
        let directory = TestDirectory::new();
        let store = open_store(&directory);
        store
            .connection
            .execute_batch("CREATE TABLE unexpected(value TEXT)")
            .expect("alter schema");
        let destination = directory.path().join("invalid-schema.backup.db");
        let before = directory_entry_names(directory.path());
        assert!(matches!(
            super::backup_connection_to(
                &store.connection,
                &destination,
                super::SessionBackupSchema::Current,
            ),
            Err(SessionStoreError::SchemaMismatch(_))
        ));
        assert!(!destination.exists());
        assert_eq!(directory_entry_names(directory.path()), before);
    }

    #[test]
    fn backup_rejects_orphan_destination_sidecars_without_mutation() {
        let directory = TestDirectory::new();
        let store = open_store(&directory);
        for suffix in ["-wal", "-shm", "-journal"] {
            let destination = directory.path().join(format!("orphan-{}.db", &suffix[1..]));
            fs::write(
                super::sqlite_sidecar_path(&destination, suffix),
                b"orphan sidecar",
            )
            .expect("write orphan backup sidecar");
            let sidecar = super::sqlite_sidecar_path(&destination, suffix);
            let before_entries = directory_entry_names(directory.path());
            let before_sidecar = fs::read(&sidecar).expect("read orphan backup sidecar");
            assert!(matches!(
                store.backup_to(&destination),
                Err(SessionStoreError::InvalidPersistedValue(
                    "backup.orphan_sqlite_sidecar"
                ))
            ));
            assert!(!destination.exists());
            assert_eq!(directory_entry_names(directory.path()), before_entries);
            assert_eq!(
                fs::read(&sidecar).expect("read unchanged backup sidecar"),
                before_sidecar
            );
        }
    }

    #[test]
    fn backup_rejects_reserved_sqlite_companion_suffixes_in_wal_and_delete_modes() {
        for prefer_wal in [true, false] {
            let directory = TestDirectory::new();
            let store = SessionStore::open(
                directory.database(),
                SessionStoreConfig {
                    prefer_wal,
                    ..SessionStoreConfig::default()
                },
            )
            .expect("open store");
            assert_eq!(
                store.journal_mode(),
                if prefer_wal {
                    SessionJournalMode::Wal
                } else {
                    SessionJournalMode::Delete
                }
            );

            for suffix in ["-wal", "-shm", "-journal"] {
                let mut destinations =
                    vec![super::sqlite_sidecar_path(&directory.database(), suffix)];
                destinations.push(
                    directory
                        .path()
                        .join(format!("SESSION.DB{}", suffix.to_ascii_uppercase())),
                );

                for destination in destinations {
                    let before_entries = directory_entry_names(directory.path());
                    assert!(matches!(
                        store.backup_to(&destination),
                        Err(SessionStoreError::InvalidPersistedValue(
                            "backup.reserved_sqlite_companion"
                        ))
                    ));
                    assert_eq!(directory_entry_names(directory.path()), before_entries);
                }
            }
        }
    }

    #[test]
    fn wal_checkpoint_truncates_and_delete_mode_is_a_noop() {
        let wal_directory = TestDirectory::new();
        let mut wal_store = open_store(&wal_directory);
        assert_eq!(wal_store.journal_mode(), SessionJournalMode::Wal);
        wal_store
            .put_task(&task_record(gid(1), 0))
            .expect("write WAL task");
        wal_store
            .checkpoint_wal_truncate()
            .expect("truncate WAL checkpoint");
        let wal = super::sqlite_sidecar_path(&wal_directory.database(), "-wal");
        if wal.exists() {
            assert_eq!(fs::metadata(wal).expect("WAL metadata").len(), 0);
        }

        let delete_directory = TestDirectory::new();
        let mut delete_store = SessionStore::open(
            delete_directory.database(),
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("open DELETE store");
        assert_eq!(delete_store.journal_mode(), SessionJournalMode::Delete);
        delete_store
            .put_session(&session_record())
            .expect("write DELETE session");
        delete_store
            .checkpoint_wal_truncate()
            .expect("DELETE checkpoint no-op");
        assert!(!super::sqlite_sidecar_path(&delete_directory.database(), "-wal").exists());
    }

    #[test]
    fn wal_checkpoint_reports_busy_until_read_snapshot_is_released() {
        let directory = TestDirectory::new();
        let mut store = open_store(&directory);
        store.put_task(&task_record(gid(1), 0)).expect("first task");

        let reader = Connection::open(directory.database()).expect("open reader");
        reader.execute_batch("BEGIN").expect("begin read snapshot");
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM task", [], |row| row.get(0))
            .expect("establish read snapshot");
        assert_eq!(count, 1);

        store
            .put_task(&task_record(gid(2), 1))
            .expect("second task");
        assert!(matches!(
            store.checkpoint_wal_truncate(),
            Err(SessionStoreError::WalCheckpointBusy)
        ));
        reader.execute_batch("ROLLBACK").expect("release snapshot");
        store
            .checkpoint_wal_truncate()
            .expect("checkpoint after reader release");
    }

    #[test]
    fn unsupported_journal_modes_fail_closed() {
        let connection = Connection::open_in_memory().expect("memory database");
        assert!(matches!(
            super::configure_pragmas(&connection, SessionStoreConfig::default()),
            Err(SessionStoreError::InvalidPersistedValue("journal_mode"))
        ));
    }

    #[test]
    fn rollback_preflight_uses_last_valid_page_one_and_skips_out_of_range_records() {
        let directory = TestDirectory::new();
        let store = SessionStore::open(
            directory.database(),
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("create DELETE store");
        drop(store);
        let database_bytes = fs::read(directory.database()).expect("database bytes");
        let page_size =
            super::decode_database_page_size(&database_bytes[16..18]).expect("page size");
        let database_pages =
            u32::try_from(database_bytes.len() / page_size).expect("database pages");
        let journal_path = super::sqlite_sidecar_path(&directory.database(), "-journal");

        let duplicate = synthetic_rollback_journal(
            &directory.database(),
            database_pages,
            &[(1, 2, true), (1, 1, true)],
        );
        fs::write(&journal_path, duplicate).expect("write duplicate page-one journal");
        let super::RollbackJournalPreflight::Hot {
            page_one: Some(header),
            ..
        } = super::inspect_rollback_journal(&directory.database()).expect("duplicate preflight")
        else {
            panic!("expected recovered page one");
        };
        assert_eq!(super::decode_database_header(&header).expect("header").1, 1);

        let duplicate = synthetic_rollback_journal(
            &directory.database(),
            database_pages,
            &[(1, 1, true), (1, 2, true)],
        );
        fs::write(&journal_path, duplicate).expect("write reversed duplicate journal");
        let super::RollbackJournalPreflight::Hot {
            page_one: Some(header),
            ..
        } = super::inspect_rollback_journal(&directory.database())
            .expect("reversed duplicate preflight")
        else {
            panic!("expected last recovered page one");
        };
        assert_eq!(super::decode_database_header(&header).expect("header").1, 2);

        let out_of_range = synthetic_rollback_journal(
            &directory.database(),
            database_pages,
            &[(database_pages + 1, 1, false), (1, 2, true)],
        );
        fs::write(&journal_path, out_of_range).expect("write out-of-range journal");
        let super::RollbackJournalPreflight::Hot {
            page_one: Some(header),
            ..
        } = super::inspect_rollback_journal(&directory.database()).expect("range preflight")
        else {
            panic!("expected page one after ignored out-of-range record");
        };
        assert_eq!(super::decode_database_header(&header).expect("header").1, 2);
    }

    #[test]
    fn rollback_preflight_handles_empty_legacy_and_super_journal_cases() {
        let directory = TestDirectory::new();
        let store = SessionStore::open(
            directory.database(),
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("create DELETE store");
        drop(store);
        let database_bytes = fs::read(directory.database()).expect("database bytes");
        let page_size =
            super::decode_database_page_size(&database_bytes[16..18]).expect("page size");
        let database_pages =
            u32::try_from(database_bytes.len() / page_size).expect("database pages");
        let journal_path = super::sqlite_sidecar_path(&directory.database(), "-journal");

        let empty = synthetic_rollback_journal(&directory.database(), 0, &[]);
        fs::write(&journal_path, empty).expect("write empty-origin journal");
        fs::write(directory.database(), &database_bytes[..32])
            .expect("truncate main database header");
        assert_eq!(
            super::inspect_persisted_user_version(&directory.database())
                .expect("empty-origin preflight"),
            0
        );
        fs::write(directory.database(), &database_bytes).expect("restore main database");

        let mut legacy = synthetic_rollback_journal(&directory.database(), database_pages, &[]);
        legacy[24..28].fill(0);
        fs::write(&journal_path, legacy).expect("write legacy journal");
        let before_legacy = snapshot_directory(directory.path());
        assert!(matches!(
            super::inspect_rollback_journal(&directory.database()),
            Err(SessionStoreError::InvalidPersistedValue(
                "rollback_journal.legacy_page_size"
            ))
        ));
        assert_eq!(snapshot_directory(directory.path()), before_legacy);

        let mut super_journal =
            synthetic_rollback_journal(&directory.database(), database_pages, &[(1, 1, true)]);
        append_super_journal_trailer(
            &mut super_journal,
            0xfeed_beef,
            b"-mjabcdef9ab\0ignored",
            true,
        );
        fs::write(&journal_path, &super_journal).expect("write super-journal trailer");
        assert!(matches!(
            super::inspect_rollback_journal(&directory.database()),
            Err(SessionStoreError::InvalidPersistedValue(
                "rollback_journal.super_journal"
            ))
        ));

        let mut checksum_collision =
            synthetic_rollback_journal(&directory.database(), database_pages, &[(1, 1, true)]);
        append_super_journal_trailer(
            &mut checksum_collision,
            0xfeed_beef,
            b"ordinary-page-tail",
            false,
        );
        fs::write(&journal_path, checksum_collision).expect("write false trailer");
        assert_eq!(
            super::inspect_persisted_user_version(&directory.database())
                .expect("false trailer preflight"),
            1
        );
    }

    #[test]
    fn hot_rollback_journal_is_recovered_before_v3_validation() {
        let directory = TestDirectory::new();
        let mut store = SessionStore::open(
            directory.database(),
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("create DELETE store");
        store.put_session(&session_record()).expect("session");
        store.put_task(&task_record(gid(1), 0)).expect("task");
        drop(store);

        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--ignored",
                "--exact",
                "session_store::tests::hot_rollback_journal_child",
                "--nocapture",
            ])
            .env("ARIAX_HOT_JOURNAL_CHILD", directory.database())
            .status()
            .expect("spawn hot-journal child");
        assert_eq!(status.code(), Some(91));
        let journal = super::sqlite_sidecar_path(&directory.database(), "-journal");
        assert!(fs::metadata(&journal).expect("hot journal metadata").len() > 0);

        let store = SessionStore::open(
            directory.database(),
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("recover hot rollback journal");
        assert_eq!(store.tasks().expect("recovered tasks").len(), 1);
        assert!(
            store
                .task_options(
                    gid(1),
                    OptionsSnapshotScope::CurrentGeneration,
                    &|_: &str| true,
                )
                .expect("rolled back options")
                .entries()
                .next()
                .is_none()
        );
    }

    #[test]
    fn hot_rollback_page_one_recovers_corrupt_main_headers() {
        let source = TestDirectory::new();
        let mut store = SessionStore::open(
            source.database(),
            SessionStoreConfig {
                prefer_wal: false,
                ..SessionStoreConfig::default()
            },
        )
        .expect("create DELETE source store");
        store.put_session(&session_record()).expect("session");
        store.put_task(&task_record(gid(1), 0)).expect("task");
        drop(store);

        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--ignored",
                "--exact",
                "session_store::tests::hot_rollback_page_one_child",
                "--nocapture",
            ])
            .env("ARIAX_HOT_PAGE_ONE_CHILD", source.database())
            .status()
            .expect("spawn page-one hot-journal child");
        assert_eq!(status.code(), Some(92));
        let journal = super::sqlite_sidecar_path(&source.database(), "-journal");
        assert!(fs::metadata(&journal).expect("hot journal metadata").len() > 0);
        assert!(matches!(
            super::inspect_rollback_journal(&source.database()).expect("rollback preflight"),
            super::RollbackJournalPreflight::Hot {
                page_one: Some(_),
                ..
            }
        ));

        for corruption in [
            MainHeaderCorruption::Magic,
            MainHeaderCorruption::PageSize,
            MainHeaderCorruption::UserVersion,
        ] {
            let direct_directory = TestDirectory::new();
            let direct_database = direct_directory.database();
            copy_hot_rollback_fixture(&source.database(), &direct_database);
            corrupt_main_header(&direct_database, corruption);
            let direct = Connection::open(&direct_database).expect("SQLite direct recovery");
            let version: u32 = direct
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .expect("direct recovered version");
            let crash_table: i64 = direct
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'crash_fill'",
                    [],
                    |row| row.get(0),
                )
                .expect("direct recovered schema");
            assert_eq!(version, SESSION_SCHEMA_VERSION, "{corruption:?}");
            assert_eq!(crash_table, 0, "{corruption:?}");
            drop(direct);

            let store_directory = TestDirectory::new();
            let store_database = store_directory.database();
            copy_hot_rollback_fixture(&source.database(), &store_database);
            corrupt_main_header(&store_database, corruption);
            let recovered = SessionStore::open(
                &store_database,
                SessionStoreConfig {
                    prefer_wal: false,
                    ..SessionStoreConfig::default()
                },
            )
            .expect("SessionStore hot rollback recovery");
            assert_eq!(recovered.tasks().expect("recovered tasks").len(), 1);
            assert!(!super::sqlite_sidecar_path(&store_database, "-journal").exists());
        }
    }

    #[test]
    #[ignore = "spawned by hot_rollback_journal_is_recovered_before_v3_validation"]
    fn hot_rollback_journal_child() {
        let Some(database) = std::env::var_os("ARIAX_HOT_JOURNAL_CHILD") else {
            return;
        };
        let connection = Connection::open(PathBuf::from(database)).expect("child open database");
        connection
            .execute_batch(
                "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=5; PRAGMA cache_spill=ON; BEGIN IMMEDIATE;",
            )
            .expect("child begin transaction");
        let value = vec![b'x'; 1024];
        for index in 0..2048_u32 {
            connection
                .execute(
                    "INSERT INTO task_option(gid, scope, key, canonical_value) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        gid(1).to_string(),
                        OptionsSnapshotScope::CurrentGeneration.number(),
                        format!("child-{index:04}"),
                        value.as_slice(),
                    ],
                )
                .expect("child insert option");
        }
        std::process::exit(91);
    }

    #[test]
    #[ignore = "spawned by hot_rollback_page_one_recovers_corrupt_main_headers"]
    fn hot_rollback_page_one_child() {
        let Some(database) = std::env::var_os("ARIAX_HOT_PAGE_ONE_CHILD") else {
            return;
        };
        let connection = Connection::open(PathBuf::from(database)).expect("child open database");
        connection
            .execute_batch(
                "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=1; PRAGMA cache_spill=ON; BEGIN IMMEDIATE; PRAGMA user_version=2; CREATE TABLE crash_fill(value BLOB); INSERT INTO crash_fill VALUES(zeroblob(8388608));",
            )
            .expect("child create page-one hot journal");
        std::process::exit(92);
    }

    #[test]
    fn bounded_count_rejects_negative_and_excessive_rows() {
        assert!(matches!(
            super::bounded_count(-1, 10, "test.count"),
            Err(SessionStoreError::InvalidPersistedValue("test.count"))
        ));
        assert!(matches!(
            super::bounded_count(11, 10, "test.count"),
            Err(SessionStoreError::InvalidPersistedValue("test.count"))
        ));
        assert_eq!(
            super::bounded_count(10, 10, "test.count").expect("bounded count"),
            10
        );
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
            SessionQueueState::ALL.map(|state| state as i64),
            [1, 2, 3, 4, 5]
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
