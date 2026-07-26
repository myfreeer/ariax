use crate::{
    DataBarrierKind, DurabilityMode, GenerationStartReason, JournalEncodeError, JournalRecord,
    LeaseAbortReason, MAX_IDENTITY_BYTES, MAX_LAYOUT_ENTRIES, MAX_PLATFORM_PATH_BYTES,
    MAX_SAFE_RELATIVE_BYTES, OptionsSnapshotScope, PathPlatform, PlatformPath, RecordType,
    RetryReason, RetryScope, SafePathBuilder, SegmentEncoder, TaskPauseReason, TaskRemoveReason,
    UnknownJournalTag,
};
use ariax_core::{
    ErrorKind, FileId, Generation, LeaseId, OptionPatchId, PieceId, TransferAttemptId,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::str;

pub const MAX_DIGEST_ALGORITHM_BYTES: usize = 32;
pub const MAX_DIGEST_VALUE_BYTES: usize = 64;
pub const MAX_OPTION_MAP_ENTRIES: usize = 4096;
pub const MAX_OPTION_KEY_BYTES: usize = 256;
pub const MAX_OPTION_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_OPTION_MAP_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_PIECE_STATE_COVERED_PIECES: usize = 131_072;
pub const MAX_PIECE_STATE_BITMAP_BYTES: usize = MAX_PIECE_STATE_COVERED_PIECES.div_ceil(8);
pub const OPTIONS_SNAPSHOT_HASH_DOMAIN: &str = "ariax/options-snapshot/v1\0";

/// Every record payload with a complete version-1 typed codec.
pub const PAYLOAD_CODEC_RECORD_TYPES: [RecordType; 24] = crate::ALL_RECORD_TYPES;

/// A nonzero ID used by payload fields without a more specific core newtype.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PersistedId(u64);

impl PersistedId {
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A nonzero checkpoint-set identifier.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointId([u8; 16]);

impl CheckpointId {
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Option<Self> {
        if all_zero_16(&bytes) {
            None
        } else {
            Some(Self(bytes))
        }
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for CheckpointId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CheckpointId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// One nonzero SHA-256 value carried by a journal payload.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JournalHash([u8; 32]);

impl JournalHash {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Option<Self> {
        if all_zero_32(&bytes) {
            None
        } else {
            Some(Self(bytes))
        }
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for JournalHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JournalHash(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for JournalHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// One canonical nonempty payload span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedSpan {
    offset: u64,
    len: u64,
}

impl PersistedSpan {
    pub fn new(offset: u64, len: u64) -> Result<Self, PayloadCodecError> {
        if len == 0 {
            return Err(PayloadCodecError::ZeroSpanLength);
        }
        offset
            .checked_add(len)
            .ok_or(PayloadCodecError::SpanOverflow)?;
        Ok(Self { offset, len })
    }

    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn len(self) -> u64 {
        self.len
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        false
    }
}

/// Digest names accepted by version 1.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JournalDigestAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Sha512,
}

impl JournalDigestAlgorithm {
    pub const ALL: [Self; 4] = [Self::Md5, Self::Sha1, Self::Sha256, Self::Sha512];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Md5 => "md5",
            Self::Sha1 => "sha-1",
            Self::Sha256 => "sha-256",
            Self::Sha512 => "sha-512",
        }
    }

    #[must_use]
    pub const fn value_len(self) -> usize {
        match self {
            Self::Md5 => 16,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }
}

impl TryFrom<&str> for JournalDigestAlgorithm {
    type Error = PayloadCodecError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "md5" => Ok(Self::Md5),
            "sha-1" => Ok(Self::Sha1),
            "sha-256" => Ok(Self::Sha256),
            "sha-512" => Ok(Self::Sha512),
            _ => Err(PayloadCodecError::InvalidDigestAlgorithm),
        }
    }
}

pub const ALL_JOURNAL_DIGEST_ALGORITHMS: [JournalDigestAlgorithm; 4] = JournalDigestAlgorithm::ALL;

/// A canonical digest with an exact algorithm-specific byte length.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalDigest {
    algorithm: JournalDigestAlgorithm,
    value: Box<[u8]>,
}

impl JournalDigest {
    pub fn new(
        algorithm: JournalDigestAlgorithm,
        value: impl Into<Box<[u8]>>,
    ) -> Result<Self, PayloadCodecError> {
        let value = value.into();
        if value.len() != algorithm.value_len() {
            return Err(PayloadCodecError::InvalidDigestLength);
        }
        Ok(Self { algorithm, value })
    }

    #[must_use]
    pub const fn algorithm(&self) -> JournalDigestAlgorithm {
        self.algorithm
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// A sorted, duplicate-free, bounded map of already-sanitized option strings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SanitizedOptionMap {
    entries: BTreeMap<Box<str>, Box<str>>,
    canonical_bytes: usize,
}

impl SanitizedOptionMap {
    pub fn new(
        entries: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, PayloadCodecError> {
        let mut canonical_bytes = 4_usize;
        let mut output = BTreeMap::new();
        for (key, value) in entries {
            validate_option_key(&key)?;
            if value.len() > MAX_OPTION_VALUE_BYTES {
                return Err(PayloadCodecError::OptionValueTooLong);
            }
            canonical_bytes = canonical_bytes
                .checked_add(8)
                .and_then(|total| total.checked_add(key.len()))
                .and_then(|total| total.checked_add(value.len()))
                .ok_or(PayloadCodecError::OptionMapTooLarge)?;
            if canonical_bytes > MAX_OPTION_MAP_BYTES {
                return Err(PayloadCodecError::OptionMapTooLarge);
            }
            if output.insert(key.into(), value.into()).is_some() {
                return Err(PayloadCodecError::DuplicateOptionKey);
            }
            if output.len() > MAX_OPTION_MAP_ENTRIES {
                return Err(PayloadCodecError::TooManyOptions);
            }
        }
        Ok(Self {
            entries: output,
            canonical_bytes,
        })
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_ref(), value.as_ref()))
    }

    #[must_use]
    pub const fn canonical_bytes(&self) -> usize {
        self.canonical_bytes
    }

    /// Hashes the canonical option map independently of its journal scope or patch ID.
    #[must_use]
    pub fn snapshot_hash(&self) -> JournalHash {
        let mut digest = Sha256::new();
        digest.update(OPTIONS_SNAPSHOT_HASH_DOMAIN.as_bytes());
        digest.update((self.entries.len() as u32).to_le_bytes());
        for (key, value) in self.entries() {
            digest.update((key.len() as u32).to_le_bytes());
            digest.update(key.as_bytes());
            digest.update((value.len() as u32).to_le_bytes());
            digest.update(value.as_bytes());
        }
        JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero")
    }
}

/// A canonical UTF-8 safe relative path as persisted in layout/finalization records.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JournalRelativePath(Box<str>);

impl JournalRelativePath {
    pub fn new(value: impl Into<Box<str>>) -> Result<Self, PayloadCodecError> {
        let value = value.into();
        if value.len() > MAX_SAFE_RELATIVE_BYTES {
            return Err(PayloadCodecError::RelativePathTooLong);
        }
        let safe = SafePathBuilder::from_user_path(&value, PathPlatform::Unix)
            .map_err(|_| PayloadCodecError::InvalidRelativePath)?;
        if safe.canonical_string() != value.as_ref() {
            return Err(PayloadCodecError::InvalidRelativePath);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One structurally canonical persisted file-layout entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalFileLayoutEntry {
    file_id: FileId,
    global_start: u64,
    global_end: u64,
    length: u64,
    selected: bool,
    safe_relative_path: JournalRelativePath,
    file_identity: Box<[u8]>,
}

impl JournalFileLayoutEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_id: FileId,
        global_start: u64,
        global_end: u64,
        length: u64,
        selected: bool,
        safe_relative_path: JournalRelativePath,
        file_identity: impl Into<Box<[u8]>>,
    ) -> Result<Self, PayloadCodecError> {
        if global_start.checked_add(length) != Some(global_end) {
            return Err(PayloadCodecError::InvalidLayoutEntry);
        }
        let file_identity = file_identity.into();
        if file_identity.len() > MAX_IDENTITY_BYTES || selected != !file_identity.is_empty() {
            return Err(PayloadCodecError::InvalidFileIdentity);
        }
        Ok(Self {
            file_id,
            global_start,
            global_end,
            length,
            selected,
            safe_relative_path,
            file_identity,
        })
    }

    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.file_id
    }

    #[must_use]
    pub const fn global_start(&self) -> u64 {
        self.global_start
    }

    #[must_use]
    pub const fn global_end(&self) -> u64 {
        self.global_end
    }

    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }

    #[must_use]
    pub const fn selected(&self) -> bool {
        self.selected
    }

    #[must_use]
    pub const fn safe_relative_path(&self) -> &JournalRelativePath {
        &self.safe_relative_path
    }

    #[must_use]
    pub fn file_identity(&self) -> &[u8] {
        &self.file_identity
    }
}

/// Canonical evidence shared by a consecutive run of durable checkpoint pieces.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableEvidenceRun {
    first_piece_delta: u32,
    piece_count: u32,
    validator_set_fingerprint: JournalHash,
    digest_algorithm: Option<JournalDigestAlgorithm>,
    digest_value_len: u16,
    digest_values: Box<[u8]>,
}

impl DurableEvidenceRun {
    pub fn new(
        first_piece_delta: u32,
        piece_count: u32,
        validator_set_fingerprint: JournalHash,
        digest_algorithm: Option<JournalDigestAlgorithm>,
        digest_values: impl Into<Box<[u8]>>,
    ) -> Result<Self, PayloadCodecError> {
        if piece_count == 0 {
            return Err(PayloadCodecError::InvalidEvidenceRun);
        }
        let digest_values = digest_values.into();
        let digest_value_len = match digest_algorithm {
            Some(algorithm) => {
                let expected = usize::try_from(piece_count)
                    .ok()
                    .and_then(|count| count.checked_mul(algorithm.value_len()))
                    .ok_or(PayloadCodecError::InvalidEvidenceRun)?;
                if digest_values.len() != expected {
                    return Err(PayloadCodecError::InvalidEvidenceRun);
                }
                algorithm.value_len() as u16
            }
            None if digest_values.is_empty() => 0,
            None => return Err(PayloadCodecError::InvalidEvidenceRun),
        };
        Ok(Self {
            first_piece_delta,
            piece_count,
            validator_set_fingerprint,
            digest_algorithm,
            digest_value_len,
            digest_values,
        })
    }

    #[must_use]
    pub const fn first_piece_delta(&self) -> u32 {
        self.first_piece_delta
    }

    #[must_use]
    pub const fn piece_count(&self) -> u32 {
        self.piece_count
    }

    #[must_use]
    pub const fn validator_set_fingerprint(&self) -> JournalHash {
        self.validator_set_fingerprint
    }

    #[must_use]
    pub const fn digest_algorithm(&self) -> Option<JournalDigestAlgorithm> {
        self.digest_algorithm
    }

    #[must_use]
    pub const fn digest_value_len(&self) -> u16 {
        self.digest_value_len
    }

    #[must_use]
    pub fn digest_values(&self) -> &[u8] {
        &self.digest_values
    }
}

/// Typed version-1 payloads whose canonical scalar codecs are implemented.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalPayload {
    TaskCreated {
        durability: DurabilityMode,
        creator_version: u16,
    },
    OptionsSnapshot {
        scope: OptionsSnapshotScope,
        patch_id: Option<OptionPatchId>,
        snapshot_hash: JournalHash,
        options: SanitizedOptionMap,
    },
    LayoutCommitted {
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
        root_display: PlatformPath,
        root_identity: Box<[u8]>,
        total_length: Option<u64>,
        piece_length: u64,
        total_file_count: u32,
        chunk_count: u32,
        inline_files: Box<[JournalFileLayoutEntry]>,
    },
    GenerationStarted {
        previous_generation: Generation,
        reason: GenerationStartReason,
        next_snapshot_hash: JournalHash,
        patch_id: Option<OptionPatchId>,
    },
    LeaseStarted {
        transfer_attempt_id: TransferAttemptId,
        lease_id: LeaseId,
        span: PersistedSpan,
        validator_fingerprint: JournalHash,
    },
    PieceStarted {
        lease_id: LeaseId,
        piece_id: PieceId,
        piece_span: PersistedSpan,
    },
    PieceWritten {
        lease_id: LeaseId,
        piece_id: PieceId,
        written_span: PersistedSpan,
    },
    LeaseCommitted {
        lease_id: LeaseId,
        span: PersistedSpan,
        validator_fingerprint: JournalHash,
        response_digest: Option<JournalDigest>,
    },
    LeaseAborted {
        lease_id: LeaseId,
        reason: LeaseAbortReason,
    },
    PieceVerified {
        piece_id: PieceId,
        piece_span: PersistedSpan,
        contributors_hash: JournalHash,
        digest: JournalDigest,
    },
    PieceFailed {
        lease_id: Option<LeaseId>,
        piece_id: PieceId,
        piece_span: PersistedSpan,
        error_class: ErrorKind,
        attempt: u32,
    },
    PieceDurable {
        piece_id: PieceId,
        piece_span: PersistedSpan,
        contributors_hash: JournalHash,
        validator_set_fingerprint: JournalHash,
        digest: Option<JournalDigest>,
        data_barrier: DataBarrierKind,
    },
    RetryState {
        scope: RetryScope,
        scope_id: PersistedId,
        attempt: u32,
        elapsed_before_wait_ms: u64,
        scheduled_at_unix_ms: u64,
        delay_ms: u64,
        error_class: ErrorKind,
        retry_reason: RetryReason,
    },
    TaskPaused {
        reason: TaskPauseReason,
    },
    TaskComplete {
        layout_hash: JournalHash,
        final_length: u64,
        final_digest: Option<JournalDigest>,
        completed_at_unix_ms: u64,
    },
    TaskError {
        error_class: ErrorKind,
        retriable: bool,
        diagnostic_id: u64,
    },
    TaskRemoved {
        reason: TaskRemoveReason,
    },
    CleanShutdown {
        checkpoint_sequence: u64,
        shutdown_at_unix_ms: u64,
    },
    CheckpointStart {
        checkpoint_id: CheckpointId,
        source_last_sequence: u64,
        source_segment_hash: JournalHash,
        state_record_count: u32,
        created_at_unix_ms: u64,
    },
    CheckpointEnd {
        checkpoint_id: CheckpointId,
        state_record_count: u32,
        state_hash: JournalHash,
    },
    LayoutChunk {
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
        chunk_index: u32,
        chunk_count: u32,
        files: Box<[JournalFileLayoutEntry]>,
    },
    FinalizeIntent {
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
        file_id: FileId,
        temp_relative_path: JournalRelativePath,
        final_relative_path: JournalRelativePath,
        final_length: u64,
        file_identity: Box<[u8]>,
    },
    FinalizeDone {
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
        file_id: FileId,
        final_relative_path: JournalRelativePath,
    },
    PieceStateChunk {
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
        chunk_index: u32,
        chunk_count: u32,
        first_piece_id: PieceId,
        covered_piece_count: u32,
        durable_bitmap: Box<[u8]>,
        evidence_runs: Box<[DurableEvidenceRun]>,
    },
}

impl JournalPayload {
    #[must_use]
    pub const fn record_type(&self) -> RecordType {
        match self {
            Self::TaskCreated { .. } => RecordType::TaskCreated,
            Self::OptionsSnapshot { .. } => RecordType::OptionsSnapshot,
            Self::LayoutCommitted { .. } => RecordType::LayoutCommitted,
            Self::GenerationStarted { .. } => RecordType::GenerationStarted,
            Self::LeaseStarted { .. } => RecordType::LeaseStarted,
            Self::PieceStarted { .. } => RecordType::PieceStarted,
            Self::PieceWritten { .. } => RecordType::PieceWritten,
            Self::LeaseCommitted { .. } => RecordType::LeaseCommitted,
            Self::LeaseAborted { .. } => RecordType::LeaseAborted,
            Self::PieceVerified { .. } => RecordType::PieceVerified,
            Self::PieceFailed { .. } => RecordType::PieceFailed,
            Self::PieceDurable { .. } => RecordType::PieceDurable,
            Self::RetryState { .. } => RecordType::RetryState,
            Self::TaskPaused { .. } => RecordType::TaskPaused,
            Self::TaskComplete { .. } => RecordType::TaskComplete,
            Self::TaskError { .. } => RecordType::TaskError,
            Self::TaskRemoved { .. } => RecordType::TaskRemoved,
            Self::CleanShutdown { .. } => RecordType::CleanShutdown,
            Self::CheckpointStart { .. } => RecordType::CheckpointStart,
            Self::CheckpointEnd { .. } => RecordType::CheckpointEnd,
            Self::LayoutChunk { .. } => RecordType::LayoutChunk,
            Self::FinalizeIntent { .. } => RecordType::FinalizeIntent,
            Self::FinalizeDone { .. } => RecordType::FinalizeDone,
            Self::PieceStateChunk { .. } => RecordType::PieceStateChunk,
        }
    }

    pub fn encode(&self) -> Result<Box<[u8]>, PayloadCodecError> {
        validate_payload(self)?;
        let mut encoder = PayloadEncoder::new()?;
        match self {
            Self::TaskCreated {
                durability,
                creator_version,
            } => {
                encoder.u8(durability.number())?;
                encoder.u16(*creator_version)?;
            }
            Self::OptionsSnapshot {
                scope,
                patch_id,
                snapshot_hash,
                options,
            } => {
                encoder.u8(scope.number())?;
                encoder.optional_id(patch_id.map(OptionPatchId::get))?;
                encoder.hash(*snapshot_hash)?;
                encoder.option_map(options)?;
            }
            Self::LayoutCommitted {
                layout_hash,
                root_binding_hash,
                root_display,
                root_identity,
                total_length,
                piece_length,
                total_file_count,
                chunk_count,
                inline_files,
            } => {
                encoder.hash(*layout_hash)?;
                encoder.hash(*root_binding_hash)?;
                encoder.platform_path(root_display)?;
                encoder.bytes(root_identity)?;
                encoder.optional_u64(*total_length)?;
                encoder.u64(*piece_length)?;
                encoder.u32(*total_file_count)?;
                encoder.u32(*chunk_count)?;
                encoder.u32(
                    u32::try_from(inline_files.len())
                        .map_err(|_| PayloadCodecError::TooManyLayoutEntries)?,
                )?;
                encoder.file_entries(inline_files)?;
            }
            Self::GenerationStarted {
                previous_generation,
                reason,
                next_snapshot_hash,
                patch_id,
            } => {
                encoder.u64(previous_generation.get())?;
                encoder.u8(reason.number())?;
                encoder.hash(*next_snapshot_hash)?;
                encoder.optional_id(patch_id.map(OptionPatchId::get))?;
            }
            Self::LeaseStarted {
                transfer_attempt_id,
                lease_id,
                span,
                validator_fingerprint,
            } => {
                encoder.u64(transfer_attempt_id.get())?;
                encoder.u64(lease_id.get())?;
                encoder.span(*span)?;
                encoder.hash(*validator_fingerprint)?;
            }
            Self::PieceStarted {
                lease_id,
                piece_id,
                piece_span,
            } => {
                encoder.u64(lease_id.get())?;
                encoder.u64(piece_id.get())?;
                encoder.span(*piece_span)?;
            }
            Self::PieceWritten {
                lease_id,
                piece_id,
                written_span,
            } => {
                encoder.u64(lease_id.get())?;
                encoder.u64(piece_id.get())?;
                encoder.span(*written_span)?;
            }
            Self::LeaseCommitted {
                lease_id,
                span,
                validator_fingerprint,
                response_digest,
            } => {
                encoder.u64(lease_id.get())?;
                encoder.span(*span)?;
                encoder.hash(*validator_fingerprint)?;
                encoder.optional_digest(response_digest.as_ref())?;
            }
            Self::LeaseAborted { lease_id, reason } => {
                encoder.u64(lease_id.get())?;
                encoder.u8(reason.number())?;
            }
            Self::PieceVerified {
                piece_id,
                piece_span,
                contributors_hash,
                digest,
            } => {
                encoder.u64(piece_id.get())?;
                encoder.span(*piece_span)?;
                encoder.hash(*contributors_hash)?;
                encoder.digest(digest)?;
            }
            Self::PieceFailed {
                lease_id,
                piece_id,
                piece_span,
                error_class,
                attempt,
            } => {
                encoder.optional_id(lease_id.map(LeaseId::get))?;
                encoder.u64(piece_id.get())?;
                encoder.span(*piece_span)?;
                encoder.u8(error_class.number())?;
                encoder.u32(*attempt)?;
            }
            Self::PieceDurable {
                piece_id,
                piece_span,
                contributors_hash,
                validator_set_fingerprint,
                digest,
                data_barrier,
            } => {
                encoder.u64(piece_id.get())?;
                encoder.span(*piece_span)?;
                encoder.hash(*contributors_hash)?;
                encoder.hash(*validator_set_fingerprint)?;
                encoder.optional_digest(digest.as_ref())?;
                encoder.u8(data_barrier.number())?;
            }
            Self::RetryState {
                scope,
                scope_id,
                attempt,
                elapsed_before_wait_ms,
                scheduled_at_unix_ms,
                delay_ms,
                error_class,
                retry_reason,
            } => {
                encoder.u8(scope.number())?;
                encoder.u64(scope_id.get())?;
                encoder.u32(*attempt)?;
                encoder.u64(*elapsed_before_wait_ms)?;
                encoder.u64(*scheduled_at_unix_ms)?;
                encoder.u64(*delay_ms)?;
                encoder.u8(error_class.number())?;
                encoder.u8(retry_reason.number())?;
            }
            Self::TaskPaused { reason } => encoder.u8(reason.number())?,
            Self::TaskComplete {
                layout_hash,
                final_length,
                final_digest,
                completed_at_unix_ms,
            } => {
                encoder.hash(*layout_hash)?;
                encoder.u64(*final_length)?;
                encoder.optional_digest(final_digest.as_ref())?;
                encoder.u64(*completed_at_unix_ms)?;
            }
            Self::TaskError {
                error_class,
                retriable,
                diagnostic_id,
            } => {
                encoder.u8(error_class.number())?;
                encoder.boolean(*retriable)?;
                encoder.u64(*diagnostic_id)?;
            }
            Self::TaskRemoved { reason } => encoder.u8(reason.number())?,
            Self::CleanShutdown {
                checkpoint_sequence,
                shutdown_at_unix_ms,
            } => {
                encoder.u64(*checkpoint_sequence)?;
                encoder.u64(*shutdown_at_unix_ms)?;
            }
            Self::CheckpointStart {
                checkpoint_id,
                source_last_sequence,
                source_segment_hash,
                state_record_count,
                created_at_unix_ms,
            } => {
                encoder.fixed(checkpoint_id.as_bytes())?;
                encoder.u64(*source_last_sequence)?;
                encoder.hash(*source_segment_hash)?;
                encoder.u32(*state_record_count)?;
                encoder.u64(*created_at_unix_ms)?;
            }
            Self::CheckpointEnd {
                checkpoint_id,
                state_record_count,
                state_hash,
            } => {
                encoder.fixed(checkpoint_id.as_bytes())?;
                encoder.u32(*state_record_count)?;
                encoder.hash(*state_hash)?;
            }
            Self::LayoutChunk {
                layout_hash,
                root_binding_hash,
                chunk_index,
                chunk_count,
                files,
            } => {
                encoder.hash(*layout_hash)?;
                encoder.hash(*root_binding_hash)?;
                encoder.u32(*chunk_index)?;
                encoder.u32(*chunk_count)?;
                encoder.u32(
                    u32::try_from(files.len())
                        .map_err(|_| PayloadCodecError::TooManyLayoutEntries)?,
                )?;
                encoder.file_entries(files)?;
            }
            Self::FinalizeIntent {
                layout_hash,
                root_binding_hash,
                file_id,
                temp_relative_path,
                final_relative_path,
                final_length,
                file_identity,
            } => {
                encoder.hash(*layout_hash)?;
                encoder.hash(*root_binding_hash)?;
                encoder.u64(u64::from(file_id.get()))?;
                encoder.bytes(temp_relative_path.as_str().as_bytes())?;
                encoder.bytes(final_relative_path.as_str().as_bytes())?;
                encoder.u64(*final_length)?;
                encoder.bytes(file_identity)?;
            }
            Self::FinalizeDone {
                layout_hash,
                root_binding_hash,
                file_id,
                final_relative_path,
            } => {
                encoder.hash(*layout_hash)?;
                encoder.hash(*root_binding_hash)?;
                encoder.u64(u64::from(file_id.get()))?;
                encoder.bytes(final_relative_path.as_str().as_bytes())?;
            }
            Self::PieceStateChunk {
                layout_hash,
                root_binding_hash,
                chunk_index,
                chunk_count,
                first_piece_id,
                covered_piece_count,
                durable_bitmap,
                evidence_runs,
            } => {
                encoder.hash(*layout_hash)?;
                encoder.hash(*root_binding_hash)?;
                encoder.u32(*chunk_index)?;
                encoder.u32(*chunk_count)?;
                encoder.u64(first_piece_id.get())?;
                encoder.u32(*covered_piece_count)?;
                encoder.bytes(durable_bitmap)?;
                encoder.u32(
                    u32::try_from(evidence_runs.len())
                        .map_err(|_| PayloadCodecError::TooManyEvidenceRuns)?,
                )?;
                encoder.evidence_runs(evidence_runs)?;
            }
        }
        encoder.finish()
    }

    pub fn append_to(
        &self,
        encoder: &mut SegmentEncoder,
        generation: Generation,
    ) -> Result<u64, AppendPayloadError> {
        let payload = self.encode().map_err(AppendPayloadError::Codec)?;
        encoder
            .append(self.record_type(), generation, &payload)
            .map_err(AppendPayloadError::Journal)
    }

    pub fn decode(record_type: RecordType, input: &[u8]) -> Result<Self, PayloadCodecError> {
        if input.len() > crate::MAX_RECORD_PAYLOAD {
            return Err(PayloadCodecError::PayloadTooLarge);
        }
        let mut decoder = PayloadDecoder::new(input);
        let payload = match record_type {
            RecordType::TaskCreated => Self::TaskCreated {
                durability: decoder.tag()?,
                creator_version: decoder.u16()?,
            },
            RecordType::OptionsSnapshot => Self::OptionsSnapshot {
                scope: decoder.tag()?,
                patch_id: decoder
                    .optional_id()?
                    .map(|value| OptionPatchId::new(value).expect("decoded nonzero ID")),
                snapshot_hash: decoder.hash()?,
                options: decoder.option_map()?,
            },
            RecordType::LayoutCommitted => {
                let layout_hash = decoder.hash()?;
                let root_binding_hash = decoder.hash()?;
                let root_display = decoder.platform_path()?;
                let root_identity = decoder.bytes(MAX_IDENTITY_BYTES)?;
                let total_length = decoder.optional_u64()?;
                let piece_length = decoder.u64()?;
                let total_file_count = decoder.u32()?;
                let chunk_count = decoder.u32()?;
                let inline_file_count = decoder.u32()?;
                let inline_files = decoder.file_entries(inline_file_count)?;
                Self::LayoutCommitted {
                    layout_hash,
                    root_binding_hash,
                    root_display,
                    root_identity,
                    total_length,
                    piece_length,
                    total_file_count,
                    chunk_count,
                    inline_files,
                }
            }
            RecordType::GenerationStarted => Self::GenerationStarted {
                previous_generation: Generation::new(decoder.u64()?),
                reason: decoder.tag()?,
                next_snapshot_hash: decoder.hash()?,
                patch_id: decoder
                    .optional_id()?
                    .map(|value| OptionPatchId::new(value).expect("decoded nonzero ID")),
            },
            RecordType::LeaseStarted => Self::LeaseStarted {
                transfer_attempt_id: TransferAttemptId::new(decoder.required_id()?)
                    .expect("decoded nonzero ID"),
                lease_id: LeaseId::new(decoder.required_id()?).expect("decoded nonzero ID"),
                span: decoder.span()?,
                validator_fingerprint: decoder.hash()?,
            },
            RecordType::PieceStarted => Self::PieceStarted {
                lease_id: LeaseId::new(decoder.required_id()?).expect("decoded nonzero ID"),
                piece_id: PieceId::new(decoder.u64()?),
                piece_span: decoder.span()?,
            },
            RecordType::PieceWritten => Self::PieceWritten {
                lease_id: LeaseId::new(decoder.required_id()?).expect("decoded nonzero ID"),
                piece_id: PieceId::new(decoder.u64()?),
                written_span: decoder.span()?,
            },
            RecordType::LeaseCommitted => Self::LeaseCommitted {
                lease_id: LeaseId::new(decoder.required_id()?).expect("decoded nonzero ID"),
                span: decoder.span()?,
                validator_fingerprint: decoder.hash()?,
                response_digest: decoder.optional_digest()?,
            },
            RecordType::LeaseAborted => Self::LeaseAborted {
                lease_id: LeaseId::new(decoder.required_id()?).expect("decoded nonzero ID"),
                reason: decoder.tag()?,
            },
            RecordType::PieceVerified => Self::PieceVerified {
                piece_id: PieceId::new(decoder.u64()?),
                piece_span: decoder.span()?,
                contributors_hash: decoder.hash()?,
                digest: decoder.digest()?,
            },
            RecordType::PieceFailed => Self::PieceFailed {
                lease_id: decoder
                    .optional_id()?
                    .map(|value| LeaseId::new(value).expect("decoded nonzero ID")),
                piece_id: PieceId::new(decoder.u64()?),
                piece_span: decoder.span()?,
                error_class: decoder.error_kind()?,
                attempt: decoder.u32()?,
            },
            RecordType::PieceDurable => Self::PieceDurable {
                piece_id: PieceId::new(decoder.u64()?),
                piece_span: decoder.span()?,
                contributors_hash: decoder.hash()?,
                validator_set_fingerprint: decoder.hash()?,
                digest: decoder.optional_digest()?,
                data_barrier: decoder.tag()?,
            },
            RecordType::RetryState => Self::RetryState {
                scope: decoder.tag()?,
                scope_id: PersistedId::new(decoder.required_id()?).expect("decoded nonzero ID"),
                attempt: decoder.u32()?,
                elapsed_before_wait_ms: decoder.u64()?,
                scheduled_at_unix_ms: decoder.u64()?,
                delay_ms: decoder.u64()?,
                error_class: decoder.error_kind()?,
                retry_reason: decoder.tag()?,
            },
            RecordType::TaskPaused => Self::TaskPaused {
                reason: decoder.tag()?,
            },
            RecordType::TaskComplete => Self::TaskComplete {
                layout_hash: decoder.hash()?,
                final_length: decoder.u64()?,
                final_digest: decoder.optional_digest()?,
                completed_at_unix_ms: decoder.u64()?,
            },
            RecordType::TaskError => Self::TaskError {
                error_class: decoder.error_kind()?,
                retriable: decoder.boolean()?,
                diagnostic_id: decoder.u64()?,
            },
            RecordType::TaskRemoved => Self::TaskRemoved {
                reason: decoder.tag()?,
            },
            RecordType::CleanShutdown => Self::CleanShutdown {
                checkpoint_sequence: decoder.u64()?,
                shutdown_at_unix_ms: decoder.u64()?,
            },
            RecordType::CheckpointStart => Self::CheckpointStart {
                checkpoint_id: decoder.checkpoint_id()?,
                source_last_sequence: decoder.u64()?,
                source_segment_hash: decoder.hash()?,
                state_record_count: decoder.u32()?,
                created_at_unix_ms: decoder.u64()?,
            },
            RecordType::CheckpointEnd => Self::CheckpointEnd {
                checkpoint_id: decoder.checkpoint_id()?,
                state_record_count: decoder.u32()?,
                state_hash: decoder.hash()?,
            },
            RecordType::LayoutChunk => {
                let layout_hash = decoder.hash()?;
                let root_binding_hash = decoder.hash()?;
                let chunk_index = decoder.u32()?;
                let chunk_count = decoder.u32()?;
                let file_count = decoder.u32()?;
                let files = decoder.file_entries(file_count)?;
                Self::LayoutChunk {
                    layout_hash,
                    root_binding_hash,
                    chunk_index,
                    chunk_count,
                    files,
                }
            }
            RecordType::FinalizeIntent => Self::FinalizeIntent {
                layout_hash: decoder.hash()?,
                root_binding_hash: decoder.hash()?,
                file_id: decoder.file_id()?,
                temp_relative_path: decoder.relative_path()?,
                final_relative_path: decoder.relative_path()?,
                final_length: decoder.u64()?,
                file_identity: decoder.bytes(MAX_IDENTITY_BYTES)?,
            },
            RecordType::FinalizeDone => Self::FinalizeDone {
                layout_hash: decoder.hash()?,
                root_binding_hash: decoder.hash()?,
                file_id: decoder.file_id()?,
                final_relative_path: decoder.relative_path()?,
            },
            RecordType::PieceStateChunk => {
                let layout_hash = decoder.hash()?;
                let root_binding_hash = decoder.hash()?;
                let chunk_index = decoder.u32()?;
                let chunk_count = decoder.u32()?;
                let first_piece_id = PieceId::new(decoder.u64()?);
                let covered_piece_count = decoder.u32()?;
                let durable_bitmap = decoder.bytes(MAX_PIECE_STATE_BITMAP_BYTES)?;
                let evidence_run_count = decoder.u32()?;
                let evidence_runs = decoder.evidence_runs(evidence_run_count)?;
                Self::PieceStateChunk {
                    layout_hash,
                    root_binding_hash,
                    chunk_index,
                    chunk_count,
                    first_piece_id,
                    covered_piece_count,
                    durable_bitmap,
                    evidence_runs,
                }
            }
        };
        decoder.finish()?;
        validate_payload(&payload)?;
        Ok(payload)
    }
}

impl JournalRecord {
    pub fn decode_payload(&self) -> Result<JournalPayload, PayloadCodecError> {
        JournalPayload::decode(self.record_type, &self.payload)
    }
}

fn validate_payload(payload: &JournalPayload) -> Result<(), PayloadCodecError> {
    match payload {
        JournalPayload::TaskCreated {
            creator_version: 0, ..
        } => Err(PayloadCodecError::ZeroCreatorVersion),
        JournalPayload::PieceFailed { attempt: 0, .. }
        | JournalPayload::RetryState { attempt: 0, .. } => Err(PayloadCodecError::ZeroAttempt),
        JournalPayload::CheckpointStart {
            source_last_sequence: 0,
            ..
        } => Err(PayloadCodecError::ZeroSourceSequence),
        JournalPayload::CheckpointStart {
            state_record_count: 0,
            ..
        }
        | JournalPayload::CheckpointEnd {
            state_record_count: 0,
            ..
        } => Err(PayloadCodecError::ZeroStateRecordCount),
        JournalPayload::LayoutCommitted {
            root_display,
            root_identity,
            total_length,
            piece_length,
            total_file_count,
            chunk_count,
            inline_files,
            ..
        } => validate_layout_committed(
            root_display,
            root_identity,
            *total_length,
            *piece_length,
            *total_file_count,
            *chunk_count,
            inline_files,
        ),
        JournalPayload::LayoutChunk {
            chunk_index,
            chunk_count,
            files,
            ..
        } => validate_layout_chunk(*chunk_index, *chunk_count, files),
        JournalPayload::FinalizeIntent {
            temp_relative_path,
            final_relative_path,
            file_identity,
            ..
        } => {
            if temp_relative_path == final_relative_path
                || file_identity.is_empty()
                || file_identity.len() > MAX_IDENTITY_BYTES
            {
                Err(PayloadCodecError::InvalidFinalization)
            } else {
                Ok(())
            }
        }
        JournalPayload::PieceStateChunk {
            chunk_index,
            chunk_count,
            first_piece_id,
            covered_piece_count,
            durable_bitmap,
            evidence_runs,
            ..
        } => validate_piece_state_chunk(
            *chunk_index,
            *chunk_count,
            *first_piece_id,
            *covered_piece_count,
            durable_bitmap,
            evidence_runs,
        ),
        _ => Ok(()),
    }
}

fn validate_option_key(key: &str) -> Result<(), PayloadCodecError> {
    if key.is_empty()
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !key
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !key.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
    {
        return Err(PayloadCodecError::InvalidOptionKey);
    }
    if key.len() > MAX_OPTION_KEY_BYTES {
        return Err(PayloadCodecError::OptionKeyTooLong);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_layout_committed(
    root_display: &PlatformPath,
    root_identity: &[u8],
    total_length: Option<u64>,
    piece_length: u64,
    total_file_count: u32,
    chunk_count: u32,
    inline_files: &[JournalFileLayoutEntry],
) -> Result<(), PayloadCodecError> {
    if root_identity.is_empty() || root_identity.len() > MAX_IDENTITY_BYTES {
        return Err(PayloadCodecError::InvalidRootIdentity);
    }
    if piece_length == 0 {
        return Err(PayloadCodecError::ZeroPieceLength);
    }
    let total_file_count =
        usize::try_from(total_file_count).map_err(|_| PayloadCodecError::InvalidFileCount)?;
    if total_file_count == 0 || total_file_count > MAX_LAYOUT_ENTRIES {
        return Err(PayloadCodecError::InvalidFileCount);
    }
    if chunk_count == 0
        || inline_files.is_empty()
        || inline_files.len() > total_file_count
        || (chunk_count == 1 && inline_files.len() != total_file_count)
        || (chunk_count > 1 && inline_files.len() == total_file_count)
    {
        return Err(PayloadCodecError::InvalidChunk);
    }
    validate_layout_entries(inline_files, true)?;
    for entry in inline_files {
        let safe = SafePathBuilder::from_user_path(
            entry.safe_relative_path().as_str(),
            root_display.platform(),
        )
        .map_err(|_| PayloadCodecError::InvalidRelativePath)?;
        if safe.canonical_string() != entry.safe_relative_path().as_str() {
            return Err(PayloadCodecError::InvalidRelativePath);
        }
    }
    if chunk_count == 1
        && total_length.is_some_and(|length| {
            inline_files
                .last()
                .is_none_or(|entry| entry.global_end() != length)
        })
    {
        return Err(PayloadCodecError::InvalidFileCount);
    }
    Ok(())
}

fn validate_layout_chunk(
    chunk_index: u32,
    chunk_count: u32,
    files: &[JournalFileLayoutEntry],
) -> Result<(), PayloadCodecError> {
    if chunk_count <= 1 || chunk_index == 0 || chunk_index >= chunk_count || files.is_empty() {
        return Err(PayloadCodecError::InvalidChunk);
    }
    validate_layout_entries(files, false)?;
    if files[0].file_id().get() == 0 {
        return Err(PayloadCodecError::NonCanonicalLayoutOrder);
    }
    Ok(())
}

fn validate_layout_entries(
    entries: &[JournalFileLayoutEntry],
    require_origin: bool,
) -> Result<(), PayloadCodecError> {
    if entries.len() > MAX_LAYOUT_ENTRIES {
        return Err(PayloadCodecError::TooManyLayoutEntries);
    }
    if require_origin
        && entries
            .first()
            .is_none_or(|entry| entry.file_id().get() != 0 || entry.global_start() != 0)
    {
        return Err(PayloadCodecError::NonCanonicalLayoutOrder);
    }
    for pair in entries.windows(2) {
        if pair[0].file_id().get().checked_add(1) != Some(pair[1].file_id().get())
            || pair[0].global_end() != pair[1].global_start()
        {
            return Err(PayloadCodecError::NonCanonicalLayoutOrder);
        }
    }
    Ok(())
}

fn validate_piece_state_chunk(
    chunk_index: u32,
    chunk_count: u32,
    first_piece_id: PieceId,
    covered_piece_count: u32,
    durable_bitmap: &[u8],
    evidence_runs: &[DurableEvidenceRun],
) -> Result<(), PayloadCodecError> {
    if chunk_count == 0 || chunk_index >= chunk_count {
        return Err(PayloadCodecError::InvalidChunk);
    }
    let covered =
        usize::try_from(covered_piece_count).map_err(|_| PayloadCodecError::InvalidBitmap)?;
    if covered == 0 || covered > MAX_PIECE_STATE_COVERED_PIECES {
        return Err(PayloadCodecError::InvalidBitmap);
    }
    if first_piece_id
        .get()
        .checked_add(u64::from(covered_piece_count - 1))
        .is_none()
    {
        return Err(PayloadCodecError::InvalidBitmap);
    }
    let expected_bytes = covered.div_ceil(8);
    if durable_bitmap.len() != expected_bytes || !bitmap_get(durable_bitmap, 0) {
        return Err(PayloadCodecError::InvalidBitmap);
    }
    if !covered.is_multiple_of(8) {
        let used_bits = covered % 8;
        let forbidden = !((1_u8 << used_bits) - 1);
        if durable_bitmap[expected_bytes - 1] & forbidden != 0 {
            return Err(PayloadCodecError::InvalidBitmap);
        }
    }
    if evidence_runs.len() > covered {
        return Err(PayloadCodecError::TooManyEvidenceRuns);
    }
    let mut accounted = vec![0_u8; expected_bytes];
    let mut previous_end = 0_usize;
    let mut previous: Option<&DurableEvidenceRun> = None;
    for run in evidence_runs {
        let start = usize::try_from(run.first_piece_delta())
            .map_err(|_| PayloadCodecError::InvalidEvidenceRun)?;
        let count = usize::try_from(run.piece_count())
            .map_err(|_| PayloadCodecError::InvalidEvidenceRun)?;
        let end = start
            .checked_add(count)
            .ok_or(PayloadCodecError::InvalidEvidenceRun)?;
        if start < previous_end || end > covered {
            return Err(PayloadCodecError::InvalidEvidenceRun);
        }
        if start == previous_end
            && previous.is_some_and(|previous| same_evidence_class(previous, run))
        {
            return Err(PayloadCodecError::InvalidEvidenceRun);
        }
        for piece in start..end {
            if !bitmap_get(durable_bitmap, piece) || bitmap_get(&accounted, piece) {
                return Err(PayloadCodecError::InvalidEvidenceRun);
            }
            bitmap_set(&mut accounted, piece);
        }
        previous_end = end;
        previous = Some(run);
    }
    if accounted != durable_bitmap {
        return Err(PayloadCodecError::InvalidEvidenceRun);
    }
    Ok(())
}

fn same_evidence_class(left: &DurableEvidenceRun, right: &DurableEvidenceRun) -> bool {
    left.validator_set_fingerprint() == right.validator_set_fingerprint()
        && left.digest_algorithm() == right.digest_algorithm()
        && left.digest_value_len() == right.digest_value_len()
}

fn bitmap_get(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1 << (index % 8)) != 0
}

fn bitmap_set(bitmap: &mut [u8], index: usize) {
    bitmap[index / 8] |= 1 << (index % 8);
}

/// Why a typed journal payload was not canonical version-1 data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadCodecError {
    PayloadTooLarge,
    Truncated,
    TrailingBytes,
    InvalidPresence(u8),
    InvalidBoolean(u8),
    ZeroId,
    ZeroCheckpointId,
    ZeroHash,
    ZeroSpanLength,
    SpanOverflow,
    InvalidDigestAlgorithm,
    InvalidDigestLength,
    InvalidUtf8,
    InvalidTag(UnknownJournalTag),
    InvalidErrorKind(u8),
    InvalidOptionKey,
    OptionKeyTooLong,
    OptionValueTooLong,
    TooManyOptions,
    DuplicateOptionKey,
    NonCanonicalOptionOrder,
    OptionMapTooLarge,
    FieldTooLong,
    InvalidPlatformTag(u8),
    InvalidPlatformPath,
    InvalidRootIdentity,
    RelativePathTooLong,
    InvalidRelativePath,
    InvalidFileId,
    TooManyLayoutEntries,
    InvalidLayoutEntry,
    InvalidFileIdentity,
    NonCanonicalLayoutOrder,
    ZeroPieceLength,
    InvalidFileCount,
    InvalidChunk,
    InvalidFinalization,
    InvalidBitmap,
    TooManyEvidenceRuns,
    InvalidEvidenceRun,
    ZeroCreatorVersion,
    ZeroAttempt,
    ZeroSourceSequence,
    ZeroStateRecordCount,
    AllocationFailed,
}

impl PayloadCodecError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::PayloadTooLarge => "payload_too_large",
            Self::Truncated => "truncated",
            Self::TrailingBytes => "trailing_bytes",
            Self::InvalidPresence(_) => "invalid_presence",
            Self::InvalidBoolean(_) => "invalid_boolean",
            Self::ZeroId => "zero_id",
            Self::ZeroCheckpointId => "zero_checkpoint_id",
            Self::ZeroHash => "zero_hash",
            Self::ZeroSpanLength => "zero_span_length",
            Self::SpanOverflow => "span_overflow",
            Self::InvalidDigestAlgorithm => "invalid_digest_algorithm",
            Self::InvalidDigestLength => "invalid_digest_length",
            Self::InvalidUtf8 => "invalid_utf8",
            Self::InvalidTag(_) => "invalid_tag",
            Self::InvalidErrorKind(_) => "invalid_error_kind",
            Self::InvalidOptionKey => "invalid_option_key",
            Self::OptionKeyTooLong => "option_key_too_long",
            Self::OptionValueTooLong => "option_value_too_long",
            Self::TooManyOptions => "too_many_options",
            Self::DuplicateOptionKey => "duplicate_option_key",
            Self::NonCanonicalOptionOrder => "noncanonical_option_order",
            Self::OptionMapTooLarge => "option_map_too_large",
            Self::FieldTooLong => "field_too_long",
            Self::InvalidPlatformTag(_) => "invalid_platform_tag",
            Self::InvalidPlatformPath => "invalid_platform_path",
            Self::InvalidRootIdentity => "invalid_root_identity",
            Self::RelativePathTooLong => "relative_path_too_long",
            Self::InvalidRelativePath => "invalid_relative_path",
            Self::InvalidFileId => "invalid_file_id",
            Self::TooManyLayoutEntries => "too_many_layout_entries",
            Self::InvalidLayoutEntry => "invalid_layout_entry",
            Self::InvalidFileIdentity => "invalid_file_identity",
            Self::NonCanonicalLayoutOrder => "noncanonical_layout_order",
            Self::ZeroPieceLength => "zero_piece_length",
            Self::InvalidFileCount => "invalid_file_count",
            Self::InvalidChunk => "invalid_chunk",
            Self::InvalidFinalization => "invalid_finalization",
            Self::InvalidBitmap => "invalid_bitmap",
            Self::TooManyEvidenceRuns => "too_many_evidence_runs",
            Self::InvalidEvidenceRun => "invalid_evidence_run",
            Self::ZeroCreatorVersion => "zero_creator_version",
            Self::ZeroAttempt => "zero_attempt",
            Self::ZeroSourceSequence => "zero_source_sequence",
            Self::ZeroStateRecordCount => "zero_state_record_count",
            Self::AllocationFailed => "allocation_failed",
        }
    }
}

pub const ALL_PAYLOAD_CODEC_ERROR_CLASSES: [PayloadCodecError; 45] = [
    PayloadCodecError::PayloadTooLarge,
    PayloadCodecError::Truncated,
    PayloadCodecError::TrailingBytes,
    PayloadCodecError::InvalidPresence(2),
    PayloadCodecError::InvalidBoolean(2),
    PayloadCodecError::ZeroId,
    PayloadCodecError::ZeroCheckpointId,
    PayloadCodecError::ZeroHash,
    PayloadCodecError::ZeroSpanLength,
    PayloadCodecError::SpanOverflow,
    PayloadCodecError::InvalidDigestAlgorithm,
    PayloadCodecError::InvalidDigestLength,
    PayloadCodecError::InvalidUtf8,
    PayloadCodecError::InvalidTag(UnknownJournalTag::new_for_contract()),
    PayloadCodecError::InvalidErrorKind(0),
    PayloadCodecError::InvalidOptionKey,
    PayloadCodecError::OptionKeyTooLong,
    PayloadCodecError::OptionValueTooLong,
    PayloadCodecError::TooManyOptions,
    PayloadCodecError::DuplicateOptionKey,
    PayloadCodecError::NonCanonicalOptionOrder,
    PayloadCodecError::OptionMapTooLarge,
    PayloadCodecError::FieldTooLong,
    PayloadCodecError::InvalidPlatformTag(0),
    PayloadCodecError::InvalidPlatformPath,
    PayloadCodecError::InvalidRootIdentity,
    PayloadCodecError::RelativePathTooLong,
    PayloadCodecError::InvalidRelativePath,
    PayloadCodecError::InvalidFileId,
    PayloadCodecError::TooManyLayoutEntries,
    PayloadCodecError::InvalidLayoutEntry,
    PayloadCodecError::InvalidFileIdentity,
    PayloadCodecError::NonCanonicalLayoutOrder,
    PayloadCodecError::ZeroPieceLength,
    PayloadCodecError::InvalidFileCount,
    PayloadCodecError::InvalidChunk,
    PayloadCodecError::InvalidFinalization,
    PayloadCodecError::InvalidBitmap,
    PayloadCodecError::TooManyEvidenceRuns,
    PayloadCodecError::InvalidEvidenceRun,
    PayloadCodecError::ZeroCreatorVersion,
    PayloadCodecError::ZeroAttempt,
    PayloadCodecError::ZeroSourceSequence,
    PayloadCodecError::ZeroStateRecordCount,
    PayloadCodecError::AllocationFailed,
];

impl fmt::Display for PayloadCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPresence(value) => write!(formatter, "invalid presence tag {value}"),
            Self::InvalidBoolean(value) => write!(formatter, "invalid boolean tag {value}"),
            Self::InvalidTag(error) => error.fmt(formatter),
            Self::InvalidErrorKind(value) => write!(formatter, "invalid error kind {value}"),
            Self::InvalidPlatformTag(value) => write!(formatter, "invalid platform tag {value}"),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for PayloadCodecError {}

impl From<UnknownJournalTag> for PayloadCodecError {
    fn from(error: UnknownJournalTag) -> Self {
        Self::InvalidTag(error)
    }
}

/// Why a typed payload could not be encoded and appended atomically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendPayloadError {
    Codec(PayloadCodecError),
    Journal(JournalEncodeError),
}

impl fmt::Display for AppendPayloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => error.fmt(formatter),
            Self::Journal(error) => error.fmt(formatter),
        }
    }
}

impl Error for AppendPayloadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Journal(error) => Some(error),
        }
    }
}

struct PayloadEncoder {
    output: Vec<u8>,
}

impl PayloadEncoder {
    fn new() -> Result<Self, PayloadCodecError> {
        let mut output = Vec::new();
        output
            .try_reserve(128)
            .map_err(|_| PayloadCodecError::AllocationFailed)?;
        Ok(Self { output })
    }

    fn finish(self) -> Result<Box<[u8]>, PayloadCodecError> {
        if self.output.len() > crate::MAX_RECORD_PAYLOAD {
            return Err(PayloadCodecError::PayloadTooLarge);
        }
        Ok(self.output.into_boxed_slice())
    }

    fn fixed(&mut self, value: &[u8]) -> Result<(), PayloadCodecError> {
        self.output
            .try_reserve(value.len())
            .map_err(|_| PayloadCodecError::AllocationFailed)?;
        self.output.extend_from_slice(value);
        Ok(())
    }

    fn u8(&mut self, value: u8) -> Result<(), PayloadCodecError> {
        self.fixed(&[value])
    }

    fn boolean(&mut self, value: bool) -> Result<(), PayloadCodecError> {
        self.u8(u8::from(value))
    }

    fn u16(&mut self, value: u16) -> Result<(), PayloadCodecError> {
        self.fixed(&value.to_le_bytes())
    }

    fn u32(&mut self, value: u32) -> Result<(), PayloadCodecError> {
        self.fixed(&value.to_le_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), PayloadCodecError> {
        self.fixed(&value.to_le_bytes())
    }

    fn hash(&mut self, value: JournalHash) -> Result<(), PayloadCodecError> {
        self.fixed(value.as_bytes())
    }

    fn span(&mut self, value: PersistedSpan) -> Result<(), PayloadCodecError> {
        self.u64(value.offset())?;
        self.u64(value.len())
    }

    fn optional_id(&mut self, value: Option<u64>) -> Result<(), PayloadCodecError> {
        match value {
            Some(value) => {
                if value == 0 {
                    return Err(PayloadCodecError::ZeroId);
                }
                self.u8(1)?;
                self.u64(value)
            }
            None => self.u8(0),
        }
    }

    fn optional_u64(&mut self, value: Option<u64>) -> Result<(), PayloadCodecError> {
        match value {
            Some(value) => {
                self.u8(1)?;
                self.u64(value)
            }
            None => self.u8(0),
        }
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), PayloadCodecError> {
        let length = u32::try_from(value.len()).map_err(|_| PayloadCodecError::PayloadTooLarge)?;
        self.u32(length)?;
        self.fixed(value)
    }

    fn digest(&mut self, value: &JournalDigest) -> Result<(), PayloadCodecError> {
        self.bytes(value.algorithm().code().as_bytes())?;
        self.bytes(value.value())
    }

    fn optional_digest(&mut self, value: Option<&JournalDigest>) -> Result<(), PayloadCodecError> {
        match value {
            Some(value) => {
                self.u8(1)?;
                self.digest(value)
            }
            None => self.u8(0),
        }
    }

    fn option_map(&mut self, value: &SanitizedOptionMap) -> Result<(), PayloadCodecError> {
        self.u32(
            u32::try_from(value.entries.len()).map_err(|_| PayloadCodecError::TooManyOptions)?,
        )?;
        for (key, value) in value.entries() {
            self.bytes(key.as_bytes())?;
            self.bytes(value.as_bytes())?;
        }
        Ok(())
    }

    fn platform_path(&mut self, value: &PlatformPath) -> Result<(), PayloadCodecError> {
        self.u8(value.platform() as u8)?;
        self.bytes(value.bytes())
    }

    fn file_entries(&mut self, values: &[JournalFileLayoutEntry]) -> Result<(), PayloadCodecError> {
        for value in values {
            self.u64(u64::from(value.file_id().get()))?;
            self.u64(value.global_start())?;
            self.u64(value.global_end())?;
            self.u64(value.length())?;
            self.boolean(value.selected())?;
            self.bytes(value.safe_relative_path().as_str().as_bytes())?;
            self.bytes(value.file_identity())?;
        }
        Ok(())
    }

    fn evidence_runs(&mut self, values: &[DurableEvidenceRun]) -> Result<(), PayloadCodecError> {
        for value in values {
            self.u32(value.first_piece_delta())?;
            self.u32(value.piece_count())?;
            self.hash(value.validator_set_fingerprint())?;
            match value.digest_algorithm() {
                Some(algorithm) => self.bytes(algorithm.code().as_bytes())?,
                None => self.bytes(&[])?,
            }
            self.u16(value.digest_value_len())?;
            self.bytes(value.digest_values())?;
        }
        Ok(())
    }
}

struct PayloadDecoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> PayloadDecoder<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn finish(self) -> Result<(), PayloadCodecError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(PayloadCodecError::TrailingBytes)
        }
    }

    fn remaining(&self) -> usize {
        self.input.len() - self.offset
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PayloadCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(PayloadCodecError::Truncated)?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(PayloadCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, PayloadCodecError> {
        Ok(self.take(1)?[0])
    }

    fn boolean(&mut self) -> Result<bool, PayloadCodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(PayloadCodecError::InvalidBoolean(value)),
        }
    }

    fn u16(&mut self) -> Result<u16, PayloadCodecError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("fixed length"),
        ))
    }

    fn u32(&mut self) -> Result<u32, PayloadCodecError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed length"),
        ))
    }

    fn u64(&mut self) -> Result<u64, PayloadCodecError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("fixed length"),
        ))
    }

    fn required_id(&mut self) -> Result<u64, PayloadCodecError> {
        let value = self.u64()?;
        if value == 0 {
            Err(PayloadCodecError::ZeroId)
        } else {
            Ok(value)
        }
    }

    fn optional_id(&mut self) -> Result<Option<u64>, PayloadCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.required_id().map(Some),
            value => Err(PayloadCodecError::InvalidPresence(value)),
        }
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, PayloadCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.u64().map(Some),
            value => Err(PayloadCodecError::InvalidPresence(value)),
        }
    }

    fn hash(&mut self) -> Result<JournalHash, PayloadCodecError> {
        let bytes = self.take(32)?.try_into().expect("fixed length");
        JournalHash::new(bytes).ok_or(PayloadCodecError::ZeroHash)
    }

    fn checkpoint_id(&mut self) -> Result<CheckpointId, PayloadCodecError> {
        let bytes = self.take(16)?.try_into().expect("fixed length");
        CheckpointId::new(bytes).ok_or(PayloadCodecError::ZeroCheckpointId)
    }

    fn span(&mut self) -> Result<PersistedSpan, PayloadCodecError> {
        PersistedSpan::new(self.u64()?, self.u64()?)
    }

    fn bytes(&mut self, maximum: usize) -> Result<Box<[u8]>, PayloadCodecError> {
        self.bytes_with_error(maximum, PayloadCodecError::FieldTooLong)
    }

    fn bytes_with_error(
        &mut self,
        maximum: usize,
        too_long: PayloadCodecError,
    ) -> Result<Box<[u8]>, PayloadCodecError> {
        let length = self.u32()? as usize;
        if length > maximum {
            return Err(too_long);
        }
        let input = self.take(length)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(length)
            .map_err(|_| PayloadCodecError::AllocationFailed)?;
        output.extend_from_slice(input);
        Ok(output.into_boxed_slice())
    }

    fn digest(&mut self) -> Result<JournalDigest, PayloadCodecError> {
        let algorithm = self.bytes_with_error(
            MAX_DIGEST_ALGORITHM_BYTES,
            PayloadCodecError::InvalidDigestAlgorithm,
        )?;
        let algorithm = str::from_utf8(&algorithm).map_err(|_| PayloadCodecError::InvalidUtf8)?;
        let algorithm = JournalDigestAlgorithm::try_from(algorithm)?;
        let value = self.bytes_with_error(
            MAX_DIGEST_VALUE_BYTES,
            PayloadCodecError::InvalidDigestLength,
        )?;
        JournalDigest::new(algorithm, value)
    }

    fn optional_digest(&mut self) -> Result<Option<JournalDigest>, PayloadCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.digest().map(Some),
            value => Err(PayloadCodecError::InvalidPresence(value)),
        }
    }

    fn error_kind(&mut self) -> Result<ErrorKind, PayloadCodecError> {
        let value = self.u8()?;
        ErrorKind::try_from(value).map_err(|()| PayloadCodecError::InvalidErrorKind(value))
    }

    fn option_map(&mut self) -> Result<SanitizedOptionMap, PayloadCodecError> {
        let start = self.offset;
        let count = usize::try_from(self.u32()?).map_err(|_| PayloadCodecError::TooManyOptions)?;
        if count > MAX_OPTION_MAP_ENTRIES {
            return Err(PayloadCodecError::TooManyOptions);
        }
        if count
            .checked_mul(8)
            .is_none_or(|minimum| minimum > self.remaining())
        {
            return Err(PayloadCodecError::Truncated);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| PayloadCodecError::AllocationFailed)?;
        let mut previous: Option<Box<str>> = None;
        for _ in 0..count {
            let key =
                self.bytes_with_error(MAX_OPTION_KEY_BYTES, PayloadCodecError::OptionKeyTooLong)?;
            let key = str::from_utf8(&key).map_err(|_| PayloadCodecError::InvalidUtf8)?;
            validate_option_key(key)?;
            if previous.as_deref().is_some_and(|value| value >= key) {
                return Err(if previous.as_deref() == Some(key) {
                    PayloadCodecError::DuplicateOptionKey
                } else {
                    PayloadCodecError::NonCanonicalOptionOrder
                });
            }
            let value = self.bytes_with_error(
                MAX_OPTION_VALUE_BYTES,
                PayloadCodecError::OptionValueTooLong,
            )?;
            let value = str::from_utf8(&value).map_err(|_| PayloadCodecError::InvalidUtf8)?;
            let key: Box<str> = key.into();
            previous = Some(key.clone());
            entries.push((key.into_string(), value.to_owned()));
            if self.offset - start > MAX_OPTION_MAP_BYTES {
                return Err(PayloadCodecError::OptionMapTooLarge);
            }
        }
        SanitizedOptionMap::new(entries)
    }

    fn platform_path(&mut self) -> Result<PlatformPath, PayloadCodecError> {
        let platform = match self.u8()? {
            1 => PathPlatform::Unix,
            2 => PathPlatform::Windows,
            value => return Err(PayloadCodecError::InvalidPlatformTag(value)),
        };
        let bytes = self.bytes_with_error(
            MAX_PLATFORM_PATH_BYTES,
            PayloadCodecError::InvalidPlatformPath,
        )?;
        PlatformPath::from_native_bytes(platform, &bytes)
            .map_err(|_| PayloadCodecError::InvalidPlatformPath)
    }

    fn relative_path(&mut self) -> Result<JournalRelativePath, PayloadCodecError> {
        let bytes = self.bytes_with_error(
            MAX_SAFE_RELATIVE_BYTES,
            PayloadCodecError::RelativePathTooLong,
        )?;
        let value = str::from_utf8(&bytes).map_err(|_| PayloadCodecError::InvalidUtf8)?;
        JournalRelativePath::new(value.to_owned())
    }

    fn file_id(&mut self) -> Result<FileId, PayloadCodecError> {
        let value = self.u64()?;
        let value = u32::try_from(value).map_err(|_| PayloadCodecError::InvalidFileId)?;
        Ok(FileId::new(value))
    }

    fn file_entries(
        &mut self,
        count: u32,
    ) -> Result<Box<[JournalFileLayoutEntry]>, PayloadCodecError> {
        let count = usize::try_from(count).map_err(|_| PayloadCodecError::TooManyLayoutEntries)?;
        if count > MAX_LAYOUT_ENTRIES {
            return Err(PayloadCodecError::TooManyLayoutEntries);
        }
        if count
            .checked_mul(42)
            .is_none_or(|minimum| minimum > self.remaining())
        {
            return Err(PayloadCodecError::Truncated);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| PayloadCodecError::AllocationFailed)?;
        for _ in 0..count {
            let file_id = self.file_id()?;
            let global_start = self.u64()?;
            let global_end = self.u64()?;
            let length = self.u64()?;
            let selected = self.boolean()?;
            let safe_relative_path = self.relative_path()?;
            let file_identity =
                self.bytes_with_error(MAX_IDENTITY_BYTES, PayloadCodecError::InvalidFileIdentity)?;
            entries.push(JournalFileLayoutEntry::new(
                file_id,
                global_start,
                global_end,
                length,
                selected,
                safe_relative_path,
                file_identity,
            )?);
        }
        Ok(entries.into_boxed_slice())
    }

    fn evidence_runs(
        &mut self,
        count: u32,
    ) -> Result<Box<[DurableEvidenceRun]>, PayloadCodecError> {
        let count = usize::try_from(count).map_err(|_| PayloadCodecError::TooManyEvidenceRuns)?;
        if count > MAX_PIECE_STATE_COVERED_PIECES {
            return Err(PayloadCodecError::TooManyEvidenceRuns);
        }
        if count
            .checked_mul(50)
            .is_none_or(|minimum| minimum > self.remaining())
        {
            return Err(PayloadCodecError::Truncated);
        }
        let mut runs = Vec::new();
        runs.try_reserve_exact(count)
            .map_err(|_| PayloadCodecError::AllocationFailed)?;
        for _ in 0..count {
            let first_piece_delta = self.u32()?;
            let piece_count = self.u32()?;
            let validator_set_fingerprint = self.hash()?;
            let algorithm = self.bytes_with_error(
                MAX_DIGEST_ALGORITHM_BYTES,
                PayloadCodecError::InvalidDigestAlgorithm,
            )?;
            let digest_algorithm = if algorithm.is_empty() {
                None
            } else {
                let algorithm =
                    str::from_utf8(&algorithm).map_err(|_| PayloadCodecError::InvalidUtf8)?;
                Some(JournalDigestAlgorithm::try_from(algorithm)?)
            };
            let encoded_digest_value_len = self.u16()?;
            let digest_values = self.bytes_with_error(
                crate::MAX_RECORD_PAYLOAD,
                PayloadCodecError::InvalidEvidenceRun,
            )?;
            let run = DurableEvidenceRun::new(
                first_piece_delta,
                piece_count,
                validator_set_fingerprint,
                digest_algorithm,
                digest_values,
            )?;
            if run.digest_value_len() != encoded_digest_value_len {
                return Err(PayloadCodecError::InvalidEvidenceRun);
            }
            runs.push(run);
        }
        Ok(runs.into_boxed_slice())
    }

    fn tag<T>(&mut self) -> Result<T, PayloadCodecError>
    where
        T: TryFrom<u8, Error = UnknownJournalTag>,
    {
        T::try_from(self.u8()?).map_err(Into::into)
    }
}

const fn all_zero_16(bytes: &[u8; 16]) -> bool {
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != 0 {
            return false;
        }
        index += 1;
    }
    true
}

const fn all_zero_32(bytes: &[u8; 32]) -> bool {
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != 0 {
            return false;
        }
        index += 1;
    }
    true
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_PAYLOAD_CODEC_ERROR_CLASSES, AppendPayloadError, CheckpointId, DurableEvidenceRun,
        JournalDigest, JournalDigestAlgorithm, JournalFileLayoutEntry, JournalHash, JournalPayload,
        JournalRelativePath, MAX_PIECE_STATE_COVERED_PIECES, PAYLOAD_CODEC_RECORD_TYPES,
        PayloadCodecError, PayloadDecoder, PayloadEncoder, PersistedId, PersistedSpan,
        SanitizedOptionMap,
    };
    use crate::{
        DataBarrierKind, DurabilityMode, GenerationStartReason, LeaseAbortReason,
        OptionsSnapshotScope, PathPlatform, PlatformPath, RecordType, ReplayLimits, ReplayStop,
        RetryReason, RetryScope, SegmentEncoder, TaskPauseReason, TaskRemoveReason,
        replay_ordered_segments,
    };
    use ariax_core::{
        ErrorKind, FileId, Generation, LeaseId, OptionPatchId, PieceId, TransferAttemptId,
    };
    use std::collections::BTreeSet;

    fn hash(value: u8) -> JournalHash {
        JournalHash::new([value; 32]).expect("hash")
    }

    fn checkpoint_id() -> CheckpointId {
        CheckpointId::new([9; 16]).expect("checkpoint")
    }

    fn span() -> PersistedSpan {
        PersistedSpan::new(1024, 4096).expect("span")
    }

    fn digest() -> JournalDigest {
        JournalDigest::new(JournalDigestAlgorithm::Sha256, [7; 32].to_vec()).expect("digest")
    }

    fn option_map() -> SanitizedOptionMap {
        SanitizedOptionMap::new([
            ("piece-length".to_owned(), "1M".to_owned()),
            ("continue".to_owned(), "true".to_owned()),
        ])
        .expect("options")
    }

    fn relative_path(value: &str) -> JournalRelativePath {
        JournalRelativePath::new(value.to_owned()).expect("relative path")
    }

    fn file_entry(id: u32, start: u64, end: u64, path: &str) -> JournalFileLayoutEntry {
        JournalFileLayoutEntry::new(
            FileId::new(id),
            start,
            end,
            end - start,
            true,
            relative_path(path),
            format!("file-{id}").into_bytes(),
        )
        .expect("file entry")
    }

    fn sample(record_type: RecordType) -> JournalPayload {
        samples()
            .into_iter()
            .find(|payload| payload.record_type() == record_type)
            .expect("sample")
    }

    fn raw_options(entries: &[(&str, &str)]) -> Box<[u8]> {
        let mut encoder = PayloadEncoder::new().expect("encoder");
        encoder
            .u8(OptionsSnapshotScope::CurrentGeneration.number())
            .expect("scope");
        encoder.optional_id(None).expect("patch");
        encoder.hash(hash(8)).expect("hash");
        encoder.u32(entries.len() as u32).expect("count");
        for (key, value) in entries {
            encoder.bytes(key.as_bytes()).expect("key");
            encoder.bytes(value.as_bytes()).expect("value");
        }
        encoder.finish().expect("payload")
    }

    fn samples() -> Vec<JournalPayload> {
        vec![
            JournalPayload::TaskCreated {
                durability: DurabilityMode::Balanced,
                creator_version: 1,
            },
            JournalPayload::OptionsSnapshot {
                scope: OptionsSnapshotScope::CurrentGeneration,
                patch_id: None,
                snapshot_hash: hash(8),
                options: option_map(),
            },
            JournalPayload::LayoutCommitted {
                layout_hash: hash(9),
                root_binding_hash: hash(10),
                root_display: PlatformPath::from_native_bytes(PathPlatform::Unix, b"/srv")
                    .expect("root"),
                root_identity: b"root-id".to_vec().into_boxed_slice(),
                total_length: Some(2048),
                piece_length: 1024,
                total_file_count: 2,
                chunk_count: 2,
                inline_files: vec![file_entry(0, 0, 1024, "first.bin")].into_boxed_slice(),
            },
            JournalPayload::GenerationStarted {
                previous_generation: Generation::new(2),
                reason: GenerationStartReason::OptionPatch,
                next_snapshot_hash: hash(1),
                patch_id: Some(OptionPatchId::new(4).expect("patch")),
            },
            JournalPayload::LeaseStarted {
                transfer_attempt_id: TransferAttemptId::new(5).expect("attempt"),
                lease_id: LeaseId::new(6).expect("lease"),
                span: span(),
                validator_fingerprint: hash(2),
            },
            JournalPayload::PieceStarted {
                lease_id: LeaseId::new(6).expect("lease"),
                piece_id: PieceId::new(0),
                piece_span: span(),
            },
            JournalPayload::PieceWritten {
                lease_id: LeaseId::new(6).expect("lease"),
                piece_id: PieceId::new(0),
                written_span: span(),
            },
            JournalPayload::LeaseCommitted {
                lease_id: LeaseId::new(6).expect("lease"),
                span: span(),
                validator_fingerprint: hash(2),
                response_digest: Some(digest()),
            },
            JournalPayload::LeaseAborted {
                lease_id: LeaseId::new(6).expect("lease"),
                reason: LeaseAbortReason::Retry,
            },
            JournalPayload::PieceVerified {
                piece_id: PieceId::new(0),
                piece_span: span(),
                contributors_hash: hash(3),
                digest: digest(),
            },
            JournalPayload::PieceFailed {
                lease_id: None,
                piece_id: PieceId::new(0),
                piece_span: span(),
                error_class: ErrorKind::ChecksumMismatch,
                attempt: 2,
            },
            JournalPayload::PieceDurable {
                piece_id: PieceId::new(0),
                piece_span: span(),
                contributors_hash: hash(3),
                validator_set_fingerprint: hash(4),
                digest: None,
                data_barrier: DataBarrierKind::BalancedGroup,
            },
            JournalPayload::RetryState {
                scope: RetryScope::Span,
                scope_id: PersistedId::new(8).expect("scope"),
                attempt: 3,
                elapsed_before_wait_ms: 100,
                scheduled_at_unix_ms: 200,
                delay_ms: 300,
                error_class: ErrorKind::Timeout,
                retry_reason: RetryReason::Backoff,
            },
            JournalPayload::TaskPaused {
                reason: TaskPauseReason::User,
            },
            JournalPayload::TaskComplete {
                layout_hash: hash(5),
                final_length: 8192,
                final_digest: Some(digest()),
                completed_at_unix_ms: 400,
            },
            JournalPayload::TaskError {
                error_class: ErrorKind::Disk,
                retriable: false,
                diagnostic_id: 99,
            },
            JournalPayload::TaskRemoved {
                reason: TaskRemoveReason::User,
            },
            JournalPayload::CleanShutdown {
                checkpoint_sequence: 77,
                shutdown_at_unix_ms: 500,
            },
            JournalPayload::CheckpointStart {
                checkpoint_id: checkpoint_id(),
                source_last_sequence: 77,
                source_segment_hash: hash(6),
                state_record_count: 8,
                created_at_unix_ms: 600,
            },
            JournalPayload::CheckpointEnd {
                checkpoint_id: checkpoint_id(),
                state_record_count: 8,
                state_hash: hash(7),
            },
            JournalPayload::LayoutChunk {
                layout_hash: hash(9),
                root_binding_hash: hash(10),
                chunk_index: 1,
                chunk_count: 2,
                files: vec![file_entry(1, 1024, 2048, "second.bin")].into_boxed_slice(),
            },
            JournalPayload::FinalizeIntent {
                layout_hash: hash(9),
                root_binding_hash: hash(10),
                file_id: FileId::new(0),
                temp_relative_path: relative_path("first.bin.ariax.tmp"),
                final_relative_path: relative_path("first.bin"),
                final_length: 1024,
                file_identity: b"file-0".to_vec().into_boxed_slice(),
            },
            JournalPayload::FinalizeDone {
                layout_hash: hash(9),
                root_binding_hash: hash(10),
                file_id: FileId::new(0),
                final_relative_path: relative_path("first.bin"),
            },
            JournalPayload::PieceStateChunk {
                layout_hash: hash(9),
                root_binding_hash: hash(10),
                chunk_index: 0,
                chunk_count: 1,
                first_piece_id: PieceId::new(20),
                covered_piece_count: 5,
                durable_bitmap: vec![0b0001_0011].into_boxed_slice(),
                evidence_runs: vec![
                    DurableEvidenceRun::new(0, 2, hash(11), None, Vec::new()).expect("evidence"),
                    DurableEvidenceRun::new(
                        4,
                        1,
                        hash(12),
                        Some(JournalDigestAlgorithm::Sha256),
                        vec![13; 32],
                    )
                    .expect("digest evidence"),
                ]
                .into_boxed_slice(),
            },
        ]
    }

    #[test]
    fn every_supported_payload_round_trips_without_trailing_bytes() {
        let samples = samples();
        assert_eq!(samples.len(), PAYLOAD_CODEC_RECORD_TYPES.len());
        for (payload, record_type) in samples.iter().zip(PAYLOAD_CODEC_RECORD_TYPES) {
            assert_eq!(payload.record_type(), record_type);
            let encoded = payload.encode().expect("encode");
            assert_eq!(
                JournalPayload::decode(record_type, &encoded),
                Ok(payload.clone())
            );

            let mut trailing = encoded.to_vec();
            trailing.push(0);
            assert_eq!(
                JournalPayload::decode(record_type, &trailing),
                Err(PayloadCodecError::TrailingBytes)
            );
        }
    }

    #[test]
    fn option_snapshot_hash_is_order_independent_and_value_sensitive() {
        let first = SanitizedOptionMap::new([
            ("piece-length".to_owned(), "1M".to_owned()),
            ("continue".to_owned(), "true".to_owned()),
        ])
        .expect("options");
        let reordered = SanitizedOptionMap::new([
            ("continue".to_owned(), "true".to_owned()),
            ("piece-length".to_owned(), "1M".to_owned()),
        ])
        .expect("options");
        let changed = SanitizedOptionMap::new([
            ("continue".to_owned(), "false".to_owned()),
            ("piece-length".to_owned(), "1M".to_owned()),
        ])
        .expect("options");
        assert_eq!(first.snapshot_hash(), reordered.snapshot_hash());
        assert_ne!(first.snapshot_hash(), changed.snapshot_hash());
    }

    #[test]
    fn scalar_layout_is_exact_little_endian() {
        let task = JournalPayload::TaskCreated {
            durability: DurabilityMode::Strict,
            creator_version: 0x1234,
        };
        assert_eq!(task.encode().expect("task").as_ref(), &[3, 0x34, 0x12]);

        let lease = JournalPayload::LeaseStarted {
            transfer_attempt_id: TransferAttemptId::new(1).expect("attempt"),
            lease_id: LeaseId::new(2).expect("lease"),
            span: PersistedSpan::new(3, 4).expect("span"),
            validator_fingerprint: hash(5),
        };
        let encoded = lease.encode().expect("lease");
        assert_eq!(&encoded[0..8], &1_u64.to_le_bytes());
        assert_eq!(&encoded[8..16], &2_u64.to_le_bytes());
        assert_eq!(&encoded[16..24], &3_u64.to_le_bytes());
        assert_eq!(&encoded[24..32], &4_u64.to_le_bytes());
        assert_eq!(&encoded[32..], &[5; 32]);
    }

    #[test]
    fn decoders_reject_truncation_unknown_tags_and_invalid_scalars() {
        let encoded = sample(RecordType::LeaseCommitted)
            .encode()
            .expect("digest payload");
        for cut in 0..encoded.len() {
            assert!(JournalPayload::decode(RecordType::LeaseCommitted, &encoded[..cut]).is_err());
        }

        assert_eq!(
            JournalPayload::decode(RecordType::TaskCreated, &[0, 1, 0]),
            Err(PayloadCodecError::InvalidTag(
                DurabilityMode::try_from(0).expect_err("zero")
            ))
        );
        assert_eq!(
            JournalPayload::decode(RecordType::TaskCreated, &[1, 0, 0]),
            Err(PayloadCodecError::ZeroCreatorVersion)
        );
        assert_eq!(
            JournalPayload::decode(RecordType::TaskError, &[1, 2, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(PayloadCodecError::InvalidBoolean(2))
        );
        assert_eq!(
            JournalPayload::decode(RecordType::TaskError, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(PayloadCodecError::InvalidErrorKind(0))
        );
    }

    #[test]
    fn ids_spans_hashes_and_digests_fail_closed() {
        assert_eq!(
            PersistedSpan::new(0, 0),
            Err(PayloadCodecError::ZeroSpanLength)
        );
        assert_eq!(
            PersistedSpan::new(u64::MAX, 1),
            Err(PayloadCodecError::SpanOverflow)
        );
        assert!(PersistedId::new(0).is_none());
        assert!(CheckpointId::new([0; 16]).is_none());
        assert!(JournalHash::new([0; 32]).is_none());
        assert_eq!(
            JournalDigest::new(JournalDigestAlgorithm::Sha256, vec![0; 31]),
            Err(PayloadCodecError::InvalidDigestLength)
        );

        let mut invalid_digest = sample(RecordType::LeaseCommitted)
            .encode()
            .expect("digest payload")
            .to_vec();
        let algorithm_offset = 8 + 16 + 32 + 1;
        invalid_digest[algorithm_offset..algorithm_offset + 4]
            .copy_from_slice(&3_u32.to_le_bytes());
        invalid_digest[algorithm_offset + 4..algorithm_offset + 7].copy_from_slice(b"bad");
        assert_eq!(
            JournalPayload::decode(RecordType::LeaseCommitted, &invalid_digest),
            Err(PayloadCodecError::InvalidDigestAlgorithm)
        );
    }

    #[test]
    fn bounded_collection_constructors_reject_noncanonical_values() {
        assert_eq!(
            SanitizedOptionMap::new([
                ("continue".to_owned(), "true".to_owned()),
                ("continue".to_owned(), "false".to_owned()),
            ]),
            Err(PayloadCodecError::DuplicateOptionKey)
        );
        assert_eq!(
            SanitizedOptionMap::new([("Bad-Key".to_owned(), "true".to_owned())]),
            Err(PayloadCodecError::InvalidOptionKey)
        );
        assert_eq!(
            JournalRelativePath::new("dir//file".to_owned()),
            Err(PayloadCodecError::InvalidRelativePath)
        );
        assert_eq!(
            JournalFileLayoutEntry::new(
                FileId::new(0),
                0,
                1,
                1,
                false,
                relative_path("file"),
                b"identity".to_vec(),
            ),
            Err(PayloadCodecError::InvalidFileIdentity)
        );

        let invalid_order = JournalPayload::LayoutCommitted {
            layout_hash: hash(1),
            root_binding_hash: hash(2),
            root_display: PlatformPath::from_native_bytes(PathPlatform::Unix, b"/srv")
                .expect("root"),
            root_identity: b"root".to_vec().into_boxed_slice(),
            total_length: Some(1),
            piece_length: 1,
            total_file_count: 1,
            chunk_count: 1,
            inline_files: vec![file_entry(1, 0, 1, "file")].into_boxed_slice(),
        };
        assert_eq!(
            invalid_order.encode(),
            Err(PayloadCodecError::NonCanonicalLayoutOrder)
        );
    }

    #[test]
    fn piece_state_chunks_require_exact_bitmap_evidence_coverage() {
        let missing_evidence = JournalPayload::PieceStateChunk {
            layout_hash: hash(1),
            root_binding_hash: hash(2),
            chunk_index: 0,
            chunk_count: 1,
            first_piece_id: PieceId::new(0),
            covered_piece_count: 1,
            durable_bitmap: vec![1].into_boxed_slice(),
            evidence_runs: Vec::new().into_boxed_slice(),
        };
        assert_eq!(
            missing_evidence.encode(),
            Err(PayloadCodecError::InvalidEvidenceRun)
        );

        let trailing_bits = JournalPayload::PieceStateChunk {
            layout_hash: hash(1),
            root_binding_hash: hash(2),
            chunk_index: 0,
            chunk_count: 1,
            first_piece_id: PieceId::new(0),
            covered_piece_count: 1,
            durable_bitmap: vec![0x81].into_boxed_slice(),
            evidence_runs: vec![
                DurableEvidenceRun::new(0, 1, hash(3), None, Vec::new()).expect("run"),
            ]
            .into_boxed_slice(),
        };
        assert_eq!(
            trailing_bits.encode(),
            Err(PayloadCodecError::InvalidBitmap)
        );

        let split_run = JournalPayload::PieceStateChunk {
            layout_hash: hash(1),
            root_binding_hash: hash(2),
            chunk_index: 0,
            chunk_count: 1,
            first_piece_id: PieceId::new(0),
            covered_piece_count: 2,
            durable_bitmap: vec![0b11].into_boxed_slice(),
            evidence_runs: vec![
                DurableEvidenceRun::new(0, 1, hash(3), None, Vec::new()).expect("first"),
                DurableEvidenceRun::new(1, 1, hash(3), None, Vec::new()).expect("second"),
            ]
            .into_boxed_slice(),
        };
        assert_eq!(
            split_run.encode(),
            Err(PayloadCodecError::InvalidEvidenceRun)
        );
    }

    #[test]
    fn collection_decoders_reject_order_and_count_bombs_before_allocation() {
        assert_eq!(
            JournalPayload::decode(
                RecordType::OptionsSnapshot,
                &raw_options(&[("z-option", "1"), ("a-option", "2")]),
            ),
            Err(PayloadCodecError::NonCanonicalOptionOrder)
        );
        assert_eq!(
            JournalPayload::decode(
                RecordType::OptionsSnapshot,
                &raw_options(&[("a-option", "1"), ("a-option", "2")]),
            ),
            Err(PayloadCodecError::DuplicateOptionKey)
        );

        let mut option_count = raw_options(&[("a-option", "1")]).to_vec();
        option_count[34..38].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            JournalPayload::decode(RecordType::OptionsSnapshot, &option_count),
            Err(PayloadCodecError::TooManyOptions)
        );

        let mut layout = sample(RecordType::LayoutCommitted)
            .encode()
            .expect("layout")
            .to_vec();
        layout[64] = 0;
        assert_eq!(
            JournalPayload::decode(RecordType::LayoutCommitted, &layout),
            Err(PayloadCodecError::InvalidPlatformTag(0))
        );

        let mut piece_state = sample(RecordType::PieceStateChunk)
            .encode()
            .expect("piece state")
            .to_vec();
        let evidence_count_offset = {
            let mut decoder = PayloadDecoder::new(&piece_state);
            decoder.hash().expect("layout hash");
            decoder.hash().expect("binding hash");
            decoder.u32().expect("chunk index");
            decoder.u32().expect("chunk count");
            decoder.u64().expect("first piece");
            decoder.u32().expect("covered");
            decoder
                .bytes(super::MAX_PIECE_STATE_BITMAP_BYTES)
                .expect("bitmap");
            decoder.offset
        };
        piece_state[evidence_count_offset..evidence_count_offset + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            JournalPayload::decode(RecordType::PieceStateChunk, &piece_state),
            Err(PayloadCodecError::TooManyEvidenceRuns)
        );
    }

    #[test]
    fn maximum_piece_state_bitmap_round_trips_with_bounded_metadata() {
        let covered = MAX_PIECE_STATE_COVERED_PIECES as u32;
        let payload = JournalPayload::PieceStateChunk {
            layout_hash: hash(1),
            root_binding_hash: hash(2),
            chunk_index: 0,
            chunk_count: 1,
            first_piece_id: PieceId::new(0),
            covered_piece_count: covered,
            durable_bitmap: vec![0xff; MAX_PIECE_STATE_COVERED_PIECES / 8].into_boxed_slice(),
            evidence_runs: vec![
                DurableEvidenceRun::new(0, covered, hash(3), None, Vec::new()).expect("run"),
            ]
            .into_boxed_slice(),
        };
        let encoded = payload.encode().expect("encode maximum bitmap");
        assert!(encoded.len() < crate::MAX_RECORD_PAYLOAD);
        assert_eq!(
            JournalPayload::decode(RecordType::PieceStateChunk, &encoded),
            Ok(payload)
        );
    }

    #[test]
    fn payload_rejection_codes_are_unique_and_complete() {
        let codes: BTreeSet<_> = ALL_PAYLOAD_CODEC_ERROR_CLASSES
            .iter()
            .map(|error| error.code())
            .collect();
        assert_eq!(codes.len(), ALL_PAYLOAD_CODEC_ERROR_CLASSES.len());
        assert!(codes.contains("invalid_evidence_run"));
        assert!(codes.contains("option_map_too_large"));
    }

    #[test]
    fn typed_append_preserves_record_type_and_failed_codec_does_not_consume_sequence() {
        let mut encoder = SegmentEncoder::first(
            ariax_core::Gid::new(1).expect("gid"),
            crate::JournalId::new([1; 16]).expect("journal"),
            Generation::INITIAL,
            0,
        );
        let invalid = JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 0,
        };
        assert_eq!(
            invalid.append_to(&mut encoder, Generation::INITIAL),
            Err(AppendPayloadError::Codec(
                PayloadCodecError::ZeroCreatorVersion
            ))
        );

        let valid = JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 1,
        };
        assert_eq!(
            valid
                .append_to(&mut encoder, Generation::INITIAL)
                .expect("append"),
            1
        );
        let segment = encoder.finish();
        let replay = replay_ordered_segments(&[segment.bytes()], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.records[0].decode_payload(), Ok(valid));
    }
}
