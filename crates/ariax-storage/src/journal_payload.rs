use crate::{
    DataBarrierKind, DurabilityMode, GenerationStartReason, JournalEncodeError, JournalRecord,
    LeaseAbortReason, RecordType, RetryReason, RetryScope, SegmentEncoder, TaskPauseReason,
    TaskRemoveReason, UnknownJournalTag,
};
use ariax_core::{ErrorKind, Generation, LeaseId, OptionPatchId, PieceId, TransferAttemptId};
use std::error::Error;
use std::fmt;
use std::str;

pub const MAX_DIGEST_ALGORITHM_BYTES: usize = 32;
pub const MAX_DIGEST_VALUE_BYTES: usize = 64;

/// Record payloads with complete version-1 scalar codecs in this checkpoint.
pub const PAYLOAD_CODEC_RECORD_TYPES: [RecordType; 18] = [
    RecordType::TaskCreated,
    RecordType::GenerationStarted,
    RecordType::LeaseStarted,
    RecordType::PieceStarted,
    RecordType::PieceWritten,
    RecordType::LeaseCommitted,
    RecordType::LeaseAborted,
    RecordType::PieceVerified,
    RecordType::PieceFailed,
    RecordType::PieceDurable,
    RecordType::RetryState,
    RecordType::TaskPaused,
    RecordType::TaskComplete,
    RecordType::TaskError,
    RecordType::TaskRemoved,
    RecordType::CleanShutdown,
    RecordType::CheckpointStart,
    RecordType::CheckpointEnd,
];

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

/// Typed version-1 payloads whose canonical scalar codecs are implemented.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalPayload {
    TaskCreated {
        durability: DurabilityMode,
        creator_version: u16,
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
}

impl JournalPayload {
    #[must_use]
    pub const fn record_type(&self) -> RecordType {
        match self {
            Self::TaskCreated { .. } => RecordType::TaskCreated,
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
        if !PAYLOAD_CODEC_RECORD_TYPES.contains(&record_type) {
            return Err(PayloadCodecError::UnsupportedRecordType(record_type));
        }
        if input.len() > crate::MAX_RECORD_PAYLOAD {
            return Err(PayloadCodecError::PayloadTooLarge);
        }
        let mut decoder = PayloadDecoder::new(input);
        let payload = match record_type {
            RecordType::TaskCreated => Self::TaskCreated {
                durability: decoder.tag()?,
                creator_version: decoder.u16()?,
            },
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
            RecordType::OptionsSnapshot
            | RecordType::LayoutCommitted
            | RecordType::LayoutChunk
            | RecordType::FinalizeIntent
            | RecordType::FinalizeDone
            | RecordType::PieceStateChunk => {
                return Err(PayloadCodecError::UnsupportedRecordType(record_type));
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
        _ => Ok(()),
    }
}

/// Why a typed journal payload was not canonical version-1 data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadCodecError {
    UnsupportedRecordType(RecordType),
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
            Self::UnsupportedRecordType(_) => "unsupported_record_type",
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
            Self::ZeroCreatorVersion => "zero_creator_version",
            Self::ZeroAttempt => "zero_attempt",
            Self::ZeroSourceSequence => "zero_source_sequence",
            Self::ZeroStateRecordCount => "zero_state_record_count",
            Self::AllocationFailed => "allocation_failed",
        }
    }
}

pub const ALL_PAYLOAD_CODEC_ERROR_CLASSES: [PayloadCodecError; 21] = [
    PayloadCodecError::UnsupportedRecordType(RecordType::OptionsSnapshot),
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
    PayloadCodecError::ZeroCreatorVersion,
    PayloadCodecError::ZeroAttempt,
    PayloadCodecError::ZeroSourceSequence,
    PayloadCodecError::ZeroStateRecordCount,
    PayloadCodecError::AllocationFailed,
];

impl fmt::Display for PayloadCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedRecordType(record_type) => {
                write!(
                    formatter,
                    "payload codec does not yet support {}",
                    record_type.code()
                )
            }
            Self::InvalidPresence(value) => write!(formatter, "invalid presence tag {value}"),
            Self::InvalidBoolean(value) => write!(formatter, "invalid boolean tag {value}"),
            Self::InvalidTag(error) => error.fmt(formatter),
            Self::InvalidErrorKind(value) => write!(formatter, "invalid error kind {value}"),
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
        let length = self.u32()? as usize;
        if length > maximum {
            return Err(PayloadCodecError::InvalidDigestLength);
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
        let algorithm = self.bytes(MAX_DIGEST_ALGORITHM_BYTES)?;
        let algorithm = str::from_utf8(&algorithm).map_err(|_| PayloadCodecError::InvalidUtf8)?;
        let algorithm = JournalDigestAlgorithm::try_from(algorithm)?;
        let value = self.bytes(MAX_DIGEST_VALUE_BYTES)?;
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
        AppendPayloadError, CheckpointId, JournalDigest, JournalDigestAlgorithm, JournalHash,
        JournalPayload, PAYLOAD_CODEC_RECORD_TYPES, PayloadCodecError, PersistedId, PersistedSpan,
    };
    use crate::{
        DataBarrierKind, DurabilityMode, GenerationStartReason, LeaseAbortReason, RecordType,
        ReplayLimits, ReplayStop, RetryReason, RetryScope, SegmentEncoder, TaskPauseReason,
        TaskRemoveReason, replay_ordered_segments,
    };
    use ariax_core::{ErrorKind, Generation, LeaseId, OptionPatchId, PieceId, TransferAttemptId};

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

    fn samples() -> Vec<JournalPayload> {
        vec![
            JournalPayload::TaskCreated {
                durability: DurabilityMode::Balanced,
                creator_version: 1,
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
        let encoded = samples()[5].encode().expect("digest payload");
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

        let mut invalid_digest = samples()[5].encode().expect("digest payload").to_vec();
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
    fn collection_payloads_remain_explicitly_unsupported() {
        for record_type in [
            RecordType::OptionsSnapshot,
            RecordType::LayoutCommitted,
            RecordType::LayoutChunk,
            RecordType::FinalizeIntent,
            RecordType::FinalizeDone,
            RecordType::PieceStateChunk,
        ] {
            assert_eq!(
                JournalPayload::decode(record_type, &[]),
                Err(PayloadCodecError::UnsupportedRecordType(record_type))
            );
        }
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
