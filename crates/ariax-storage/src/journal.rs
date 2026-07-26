use ariax_core::{Generation, Gid};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;

pub const HEADER_MAGIC: [u8; 4] = *b"ARXJ";
pub const RECORD_MAGIC: [u8; 4] = *b"ARXR";
pub const COMMIT_MAGIC: [u8; 4] = *b"CMIT";
pub const JOURNAL_FORMAT_VERSION: u16 = 1;
pub const JOURNAL_ENDIANNESS_ASSERTION: u8 = 1;
pub const SEGMENT_HEADER_LEN: usize = 104;
pub const RECORD_PREFIX_LEN: usize = 28;
pub const RECORD_OVERHEAD: usize = 36;
pub const MAX_RECORD_PAYLOAD: usize = 16 * 1024 * 1024;
pub const SEGMENT_HASH_DOMAIN: &str = "ariax/segment/v1\0";

const HEADER_CRC_OFFSET: usize = SEGMENT_HEADER_LEN - 4;

/// A nonzero identifier shared by all segments in one installed journal set.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JournalId([u8; 16]);

impl JournalId {
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

impl fmt::Debug for JournalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JournalId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// SHA-256 over the domain-separated valid bytes of an immutable segment.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SegmentHash([u8; 32]);

impl SegmentHash {
    pub const ZERO: Self = Self([0; 32]);

    #[must_use]
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for SegmentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SegmentHash(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for SegmentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

macro_rules! record_types {
    ($(($variant:ident, $number:literal, $code:literal)),+ $(,)?) => {
        /// Version-1 journal record type numbers.
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr(u16)]
        pub enum RecordType {
            $($variant = $number),+
        }

        impl RecordType {
            #[must_use]
            pub const fn code(self) -> &'static str {
                match self {
                    $(Self::$variant => $code),+
                }
            }

            pub const ALL: [Self; record_types!(@count $(($variant, $number, $code)),+)] = [
                $(Self::$variant),+
            ];
        }

        impl TryFrom<u16> for RecordType {
            type Error = ();

            fn try_from(value: u16) -> Result<Self, Self::Error> {
                match value {
                    $($number => Ok(Self::$variant)),+,
                    _ => Err(()),
                }
            }
        }
    };
    (@count $(($variant:ident, $number:literal, $code:literal)),+) => {
        <[()]>::len(&[$(record_types!(@unit $variant)),+])
    };
    (@unit $variant:ident) => { () };
}

record_types!(
    (TaskCreated, 1, "task_created"),
    (OptionsSnapshot, 2, "options_snapshot"),
    (LayoutCommitted, 3, "layout_committed"),
    (GenerationStarted, 4, "generation_started"),
    (LeaseStarted, 5, "lease_started"),
    (PieceStarted, 6, "piece_started"),
    (PieceWritten, 7, "piece_written"),
    (LeaseCommitted, 8, "lease_committed"),
    (LeaseAborted, 9, "lease_aborted"),
    (PieceVerified, 10, "piece_verified"),
    (PieceFailed, 11, "piece_failed"),
    (PieceDurable, 12, "piece_durable"),
    (RetryState, 13, "retry_state"),
    (TaskPaused, 14, "task_paused"),
    (TaskComplete, 15, "task_complete"),
    (TaskError, 16, "task_error"),
    (TaskRemoved, 17, "task_removed"),
    (CleanShutdown, 18, "clean_shutdown"),
    (CheckpointStart, 19, "checkpoint_start"),
    (CheckpointEnd, 20, "checkpoint_end"),
    (LayoutChunk, 21, "layout_chunk"),
    (FinalizeIntent, 22, "finalize_intent"),
    (FinalizeDone, 23, "finalize_done"),
    (PieceStateChunk, 24, "piece_state_chunk"),
);

pub const ALL_RECORD_TYPES: [RecordType; 24] = RecordType::ALL;

/// Exact version-1 segment header fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    task_gid: Gid,
    journal_id: JournalId,
    segment_index: u32,
    first_sequence: u64,
    starting_generation: Generation,
    previous_segment_last_sequence: u64,
    previous_segment_hash: SegmentHash,
    created_at_unix_ms: u64,
}

impl SegmentHeader {
    #[must_use]
    pub const fn first(
        task_gid: Gid,
        journal_id: JournalId,
        starting_generation: Generation,
        created_at_unix_ms: u64,
    ) -> Self {
        Self {
            task_gid,
            journal_id,
            segment_index: 0,
            first_sequence: 1,
            starting_generation,
            previous_segment_last_sequence: 0,
            previous_segment_hash: SegmentHash::ZERO,
            created_at_unix_ms,
        }
    }

    #[must_use]
    pub const fn task_gid(self) -> Gid {
        self.task_gid
    }

    #[must_use]
    pub const fn journal_id(self) -> JournalId {
        self.journal_id
    }

    #[must_use]
    pub const fn segment_index(self) -> u32 {
        self.segment_index
    }

    #[must_use]
    pub const fn first_sequence(self) -> u64 {
        self.first_sequence
    }

    #[must_use]
    pub const fn starting_generation(self) -> Generation {
        self.starting_generation
    }

    #[must_use]
    pub const fn previous_segment_last_sequence(self) -> u64 {
        self.previous_segment_last_sequence
    }

    #[must_use]
    pub const fn previous_segment_hash(self) -> SegmentHash {
        self.previous_segment_hash
    }

    #[must_use]
    pub const fn created_at_unix_ms(self) -> u64 {
        self.created_at_unix_ms
    }

    #[must_use]
    pub fn encode(self) -> [u8; SEGMENT_HEADER_LEN] {
        let mut output = [0_u8; SEGMENT_HEADER_LEN];
        output[0..4].copy_from_slice(&HEADER_MAGIC);
        output[4..6].copy_from_slice(&JOURNAL_FORMAT_VERSION.to_le_bytes());
        output[6] = JOURNAL_ENDIANNESS_ASSERTION;
        output[7] = 0;
        output[8..16].copy_from_slice(&self.task_gid.get().to_le_bytes());
        output[16..32].copy_from_slice(self.journal_id.as_bytes());
        output[32..36].copy_from_slice(&self.segment_index.to_le_bytes());
        output[36..44].copy_from_slice(&self.first_sequence.to_le_bytes());
        output[44..52].copy_from_slice(&self.starting_generation.get().to_le_bytes());
        output[52..60].copy_from_slice(&self.previous_segment_last_sequence.to_le_bytes());
        output[60..92].copy_from_slice(self.previous_segment_hash.as_bytes());
        output[92..100].copy_from_slice(&self.created_at_unix_ms.to_le_bytes());
        let crc = crc32c::crc32c(&output[..HEADER_CRC_OFFSET]);
        output[HEADER_CRC_OFFSET..].copy_from_slice(&crc.to_le_bytes());
        output
    }

    pub fn decode(input: &[u8]) -> Result<Self, HeaderDecodeError> {
        if input.len() < SEGMENT_HEADER_LEN {
            return Err(HeaderDecodeError::Truncated);
        }
        if input[..4] != HEADER_MAGIC {
            return Err(HeaderDecodeError::InvalidMagic);
        }
        let expected_crc = read_u32(input, HEADER_CRC_OFFSET);
        if crc32c::crc32c(&input[..HEADER_CRC_OFFSET]) != expected_crc {
            return Err(HeaderDecodeError::BadCrc);
        }
        if read_u16(input, 4) != JOURNAL_FORMAT_VERSION {
            return Err(HeaderDecodeError::UnsupportedVersion);
        }
        if input[6] != JOURNAL_ENDIANNESS_ASSERTION {
            return Err(HeaderDecodeError::InvalidEndianness);
        }
        if input[7] != 0 {
            return Err(HeaderDecodeError::UnknownFlags);
        }
        let task_gid = Gid::new(read_u64(input, 8)).ok_or(HeaderDecodeError::ZeroGid)?;
        let journal_id =
            JournalId::new(read_array_16(input, 16)).ok_or(HeaderDecodeError::ZeroJournalId)?;
        let segment_index = read_u32(input, 32);
        let first_sequence = read_u64(input, 36);
        let starting_generation = Generation::new(read_u64(input, 44));
        let previous_segment_last_sequence = read_u64(input, 52);
        let previous_segment_hash = SegmentHash(read_array_32(input, 60));
        let created_at_unix_ms = read_u64(input, 92);

        if segment_index == 0 {
            if first_sequence != 1 {
                return Err(HeaderDecodeError::InvalidFirstSequence);
            }
            if previous_segment_last_sequence != 0 || previous_segment_hash != SegmentHash::ZERO {
                return Err(HeaderDecodeError::UnexpectedPreviousLink);
            }
        } else {
            if previous_segment_last_sequence == 0 || previous_segment_hash == SegmentHash::ZERO {
                return Err(HeaderDecodeError::MissingPreviousLink);
            }
            if previous_segment_last_sequence.checked_add(1) != Some(first_sequence) {
                return Err(HeaderDecodeError::InvalidFirstSequence);
            }
        }
        Ok(Self {
            task_gid,
            journal_id,
            segment_index,
            first_sequence,
            starting_generation,
            previous_segment_last_sequence,
            previous_segment_hash,
            created_at_unix_ms,
        })
    }

    pub(crate) fn successor(
        self,
        previous_segment_last_sequence: u64,
        previous_segment_hash: SegmentHash,
        starting_generation: Generation,
        created_at_unix_ms: u64,
    ) -> Result<Self, JournalEncodeError> {
        if previous_segment_last_sequence < self.first_sequence {
            return Err(JournalEncodeError::CannotRotateEmptySegment);
        }
        let segment_index = self
            .segment_index
            .checked_add(1)
            .ok_or(JournalEncodeError::SegmentIndexExhausted)?;
        let first_sequence = previous_segment_last_sequence
            .checked_add(1)
            .ok_or(JournalEncodeError::SequenceExhausted)?;
        Ok(Self {
            task_gid: self.task_gid,
            journal_id: self.journal_id,
            segment_index,
            first_sequence,
            starting_generation,
            previous_segment_last_sequence,
            previous_segment_hash,
            created_at_unix_ms,
        })
    }
}

/// Why a segment header was not a canonical version-1 header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderDecodeError {
    Truncated,
    InvalidMagic,
    BadCrc,
    UnsupportedVersion,
    InvalidEndianness,
    UnknownFlags,
    ZeroGid,
    ZeroJournalId,
    InvalidFirstSequence,
    UnexpectedPreviousLink,
    MissingPreviousLink,
}

impl HeaderDecodeError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Truncated => "truncated",
            Self::InvalidMagic => "invalid_magic",
            Self::BadCrc => "bad_crc",
            Self::UnsupportedVersion => "unsupported_version",
            Self::InvalidEndianness => "invalid_endianness",
            Self::UnknownFlags => "unknown_flags",
            Self::ZeroGid => "zero_gid",
            Self::ZeroJournalId => "zero_journal_id",
            Self::InvalidFirstSequence => "invalid_first_sequence",
            Self::UnexpectedPreviousLink => "unexpected_previous_link",
            Self::MissingPreviousLink => "missing_previous_link",
        }
    }
}

impl fmt::Display for HeaderDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HeaderDecodeError {}

pub const ALL_HEADER_DECODE_ERRORS: [HeaderDecodeError; 11] = [
    HeaderDecodeError::Truncated,
    HeaderDecodeError::InvalidMagic,
    HeaderDecodeError::BadCrc,
    HeaderDecodeError::UnsupportedVersion,
    HeaderDecodeError::InvalidEndianness,
    HeaderDecodeError::UnknownFlags,
    HeaderDecodeError::ZeroGid,
    HeaderDecodeError::ZeroJournalId,
    HeaderDecodeError::InvalidFirstSequence,
    HeaderDecodeError::UnexpectedPreviousLink,
    HeaderDecodeError::MissingPreviousLink,
];

/// One committed record decoded from the valid journal prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalRecord {
    pub record_type: RecordType,
    pub generation: Generation,
    pub sequence: u64,
    pub payload: Box<[u8]>,
}

/// A serialized immutable segment and the linkage evidence for rotation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedSegment {
    header: SegmentHeader,
    bytes: Vec<u8>,
    last_sequence: u64,
    hash: SegmentHash,
}

impl EncodedSegment {
    #[must_use]
    pub const fn header(&self) -> SegmentHeader {
        self.header
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    #[must_use]
    pub const fn hash(&self) -> SegmentHash {
        self.hash
    }

    pub fn rotate(
        &self,
        starting_generation: Generation,
        created_at_unix_ms: u64,
    ) -> Result<SegmentEncoder, JournalEncodeError> {
        let header = self.header.successor(
            self.last_sequence,
            self.hash,
            starting_generation,
            created_at_unix_ms,
        )?;
        Ok(SegmentEncoder::from_header(header))
    }
}

/// Serialized single-owner sequence assignment for one segment.
#[derive(Debug)]
pub struct SegmentEncoder {
    header: SegmentHeader,
    bytes: Vec<u8>,
    next_sequence: u64,
}

impl SegmentEncoder {
    #[must_use]
    pub fn first(
        task_gid: Gid,
        journal_id: JournalId,
        starting_generation: Generation,
        created_at_unix_ms: u64,
    ) -> Self {
        Self::from_header(SegmentHeader::first(
            task_gid,
            journal_id,
            starting_generation,
            created_at_unix_ms,
        ))
    }

    fn from_header(header: SegmentHeader) -> Self {
        let bytes = header.encode().to_vec();
        Self {
            header,
            bytes,
            next_sequence: header.first_sequence,
        }
    }

    pub fn append(
        &mut self,
        record_type: RecordType,
        generation: Generation,
        payload: &[u8],
    ) -> Result<u64, JournalEncodeError> {
        if generation < self.header.starting_generation {
            return Err(JournalEncodeError::GenerationBeforeSegment);
        }
        if payload.len() > MAX_RECORD_PAYLOAD {
            return Err(JournalEncodeError::PayloadTooLarge {
                length: payload.len(),
            });
        }
        let sequence = self.next_sequence;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(JournalEncodeError::SequenceExhausted)?;
        let record = encode_record(record_type, generation, sequence, payload)?;
        self.bytes
            .try_reserve(record.len())
            .map_err(|_| JournalEncodeError::AllocationFailed)?;
        self.bytes.extend_from_slice(&record);
        self.next_sequence = next_sequence;
        Ok(sequence)
    }

    #[must_use]
    pub fn finish(self) -> EncodedSegment {
        let last_sequence = self.next_sequence - 1;
        let hash = hash_segment(&self.bytes);
        EncodedSegment {
            header: self.header,
            bytes: self.bytes,
            last_sequence,
            hash,
        }
    }
}

/// Why a canonical record or rotation could not be encoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalEncodeError {
    GenerationBeforeSegment,
    PayloadTooLarge { length: usize },
    AllocationFailed,
    SequenceExhausted,
    SegmentIndexExhausted,
    CannotRotateEmptySegment,
}

impl fmt::Display for JournalEncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationBeforeSegment => {
                formatter.write_str("record generation precedes segment generation")
            }
            Self::PayloadTooLarge { length } => {
                write!(formatter, "journal payload {length} exceeds 16 MiB")
            }
            Self::AllocationFailed => formatter.write_str("journal record allocation failed"),
            Self::SequenceExhausted => formatter.write_str("journal sequence exhausted"),
            Self::SegmentIndexExhausted => formatter.write_str("journal segment index exhausted"),
            Self::CannotRotateEmptySegment => formatter.write_str("cannot rotate an empty segment"),
        }
    }
}

impl Error for JournalEncodeError {}

pub(crate) fn encode_record(
    record_type: RecordType,
    generation: Generation,
    sequence: u64,
    payload: &[u8],
) -> Result<Vec<u8>, JournalEncodeError> {
    let record_len =
        u32::try_from(payload.len()).map_err(|_| JournalEncodeError::PayloadTooLarge {
            length: payload.len(),
        })?;
    let total = RECORD_PREFIX_LEN
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(8))
        .ok_or(JournalEncodeError::AllocationFailed)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(total)
        .map_err(|_| JournalEncodeError::AllocationFailed)?;
    output.extend_from_slice(&RECORD_MAGIC);
    output.extend_from_slice(&record_len.to_le_bytes());
    output.extend_from_slice(&(record_type as u16).to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes());
    output.extend_from_slice(&generation.get().to_le_bytes());
    output.extend_from_slice(&sequence.to_le_bytes());
    output.extend_from_slice(payload);
    let crc = crc32c::crc32c(&output);
    output.extend_from_slice(&crc.to_le_bytes());
    output.extend_from_slice(&COMMIT_MAGIC);
    Ok(output)
}

/// Per-record reason replay stopped at the first invalid or torn record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordStopReason {
    TruncatedFraming,
    InvalidMagic,
    PayloadTooLarge,
    UnknownRecordType,
    UnknownFlags,
    SequenceGap { expected: u64, actual: u64 },
    GenerationBeforeSegment,
    LengthOverflow,
    TruncatedRecord,
    BadCrc,
    MissingCommit,
}

impl RecordStopReason {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::TruncatedFraming => "truncated_framing",
            Self::InvalidMagic => "invalid_magic",
            Self::PayloadTooLarge => "payload_too_large",
            Self::UnknownRecordType => "unknown_record_type",
            Self::UnknownFlags => "unknown_flags",
            Self::SequenceGap { .. } => "sequence_gap",
            Self::GenerationBeforeSegment => "generation_before_segment",
            Self::LengthOverflow => "length_overflow",
            Self::TruncatedRecord => "truncated_record",
            Self::BadCrc => "bad_crc",
            Self::MissingCommit => "missing_commit",
        }
    }
}

pub const ALL_RECORD_STOP_REASONS: [RecordStopReason; 11] = [
    RecordStopReason::TruncatedFraming,
    RecordStopReason::InvalidMagic,
    RecordStopReason::PayloadTooLarge,
    RecordStopReason::UnknownRecordType,
    RecordStopReason::UnknownFlags,
    RecordStopReason::SequenceGap {
        expected: 1,
        actual: 2,
    },
    RecordStopReason::GenerationBeforeSegment,
    RecordStopReason::LengthOverflow,
    RecordStopReason::TruncatedRecord,
    RecordStopReason::BadCrc,
    RecordStopReason::MissingCommit,
];

/// Replay resource caps checked before proportional collection allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayLimits {
    pub max_segments: usize,
    pub max_records: usize,
    pub max_payload_bytes: usize,
}

impl Default for ReplayLimits {
    fn default() -> Self {
        Self {
            max_segments: 1024,
            max_records: 262_144,
            max_payload_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Which replay budget was exhausted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayResource {
    Segments,
    Records,
    PayloadBytes,
}

impl ReplayResource {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Segments => "segments",
            Self::Records => "records",
            Self::PayloadBytes => "payload_bytes",
        }
    }
}

pub const ALL_REPLAY_RESOURCES: [ReplayResource; 3] = [
    ReplayResource::Segments,
    ReplayResource::Records,
    ReplayResource::PayloadBytes,
];

/// Why the global valid prefix ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayStop {
    CleanEnd,
    NoSegments,
    Header {
        input_index: usize,
        error: HeaderDecodeError,
    },
    SegmentIndex {
        expected: u32,
        actual: u32,
    },
    TaskMismatch,
    JournalMismatch,
    FirstSequenceMismatch {
        expected: u64,
        actual: u64,
    },
    PreviousSequenceMismatch,
    PreviousHashMismatch,
    Record {
        segment_index: u32,
        offset: usize,
        reason: RecordStopReason,
    },
    ResourceLimit(ReplayResource),
}

/// Collected records and exact valid-prefix information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalReplay {
    pub records: Vec<JournalRecord>,
    pub last_sequence: u64,
    pub valid_segment_prefixes: Vec<usize>,
    pub payload_bytes: usize,
    pub stop: ReplayStop,
}

/// Replays segments in canonical index order and stops at the first invalid global byte.
#[must_use]
pub fn replay_ordered_segments(segments: &[&[u8]], limits: ReplayLimits) -> JournalReplay {
    let mut replay = JournalReplay {
        records: Vec::new(),
        last_sequence: 0,
        valid_segment_prefixes: Vec::new(),
        payload_bytes: 0,
        stop: ReplayStop::NoSegments,
    };
    if segments.is_empty() {
        return replay;
    }
    let mut expected_gid = None;
    let mut expected_journal = None;
    let mut expected_index = 0_u32;
    let mut expected_sequence = 1_u64;
    let mut previous_hash = SegmentHash::ZERO;

    for (input_index, bytes) in segments.iter().copied().enumerate() {
        if input_index >= limits.max_segments {
            replay.stop = ReplayStop::ResourceLimit(ReplayResource::Segments);
            return replay;
        }
        let header = match SegmentHeader::decode(bytes) {
            Ok(header) => header,
            Err(error) => {
                replay.stop = ReplayStop::Header { input_index, error };
                return replay;
            }
        };
        if header.segment_index != expected_index {
            replay.stop = ReplayStop::SegmentIndex {
                expected: expected_index,
                actual: header.segment_index,
            };
            return replay;
        }
        if expected_gid.is_some_and(|gid| gid != header.task_gid) {
            replay.stop = ReplayStop::TaskMismatch;
            return replay;
        }
        if expected_journal.is_some_and(|journal| journal != header.journal_id) {
            replay.stop = ReplayStop::JournalMismatch;
            return replay;
        }
        if header.first_sequence != expected_sequence {
            replay.stop = ReplayStop::FirstSequenceMismatch {
                expected: expected_sequence,
                actual: header.first_sequence,
            };
            return replay;
        }
        if expected_index > 0 {
            if header.previous_segment_last_sequence != replay.last_sequence {
                replay.stop = ReplayStop::PreviousSequenceMismatch;
                return replay;
            }
            if header.previous_segment_hash != previous_hash {
                replay.stop = ReplayStop::PreviousHashMismatch;
                return replay;
            }
        }
        expected_gid.get_or_insert(header.task_gid);
        expected_journal.get_or_insert(header.journal_id);

        let mut offset = SEGMENT_HEADER_LEN;
        while offset < bytes.len() {
            if replay.records.len() >= limits.max_records {
                replay.valid_segment_prefixes.push(offset);
                replay.stop = ReplayStop::ResourceLimit(ReplayResource::Records);
                return replay;
            }
            match decode_record(
                &bytes[offset..],
                expected_sequence,
                header.starting_generation,
            ) {
                Ok(decoded) => {
                    if replay
                        .payload_bytes
                        .checked_add(decoded.payload.len())
                        .is_none_or(|total| total > limits.max_payload_bytes)
                    {
                        replay.valid_segment_prefixes.push(offset);
                        replay.stop = ReplayStop::ResourceLimit(ReplayResource::PayloadBytes);
                        return replay;
                    }
                    let record = JournalRecord {
                        record_type: decoded.record_type,
                        generation: decoded.generation,
                        sequence: decoded.sequence,
                        payload: decoded.payload.to_vec().into_boxed_slice(),
                    };
                    replay.payload_bytes += decoded.payload.len();
                    replay.last_sequence = decoded.sequence;
                    expected_sequence = match expected_sequence.checked_add(1) {
                        Some(sequence) => sequence,
                        None => {
                            replay
                                .valid_segment_prefixes
                                .push(offset + decoded.consumed);
                            replay.stop = ReplayStop::ResourceLimit(ReplayResource::Records);
                            return replay;
                        }
                    };
                    replay.records.push(record);
                    offset += decoded.consumed;
                }
                Err(reason) => {
                    replay.valid_segment_prefixes.push(offset);
                    replay.stop = ReplayStop::Record {
                        segment_index: header.segment_index,
                        offset,
                        reason,
                    };
                    return replay;
                }
            }
        }
        replay.valid_segment_prefixes.push(offset);
        previous_hash = hash_segment(bytes);
        expected_index = match expected_index.checked_add(1) {
            Some(index) => index,
            None => {
                replay.stop = ReplayStop::ResourceLimit(ReplayResource::Segments);
                return replay;
            }
        };
    }
    replay.stop = ReplayStop::CleanEnd;
    replay
}

struct DecodedRecord<'a> {
    record_type: RecordType,
    generation: Generation,
    sequence: u64,
    payload: &'a [u8],
    consumed: usize,
}

fn decode_record(
    input: &[u8],
    expected_sequence: u64,
    starting_generation: Generation,
) -> Result<DecodedRecord<'_>, RecordStopReason> {
    if input.len() < RECORD_PREFIX_LEN {
        return Err(RecordStopReason::TruncatedFraming);
    }
    if input[..4] != RECORD_MAGIC {
        return Err(RecordStopReason::InvalidMagic);
    }
    let payload_len = read_u32(input, 4) as usize;
    if payload_len > MAX_RECORD_PAYLOAD {
        return Err(RecordStopReason::PayloadTooLarge);
    }
    let record_type = RecordType::try_from(read_u16(input, 8))
        .map_err(|_| RecordStopReason::UnknownRecordType)?;
    if read_u16(input, 10) != 0 {
        return Err(RecordStopReason::UnknownFlags);
    }
    let generation = Generation::new(read_u64(input, 12));
    if generation < starting_generation {
        return Err(RecordStopReason::GenerationBeforeSegment);
    }
    let sequence = read_u64(input, 20);
    if sequence != expected_sequence {
        return Err(RecordStopReason::SequenceGap {
            expected: expected_sequence,
            actual: sequence,
        });
    }
    let crc_offset = RECORD_PREFIX_LEN
        .checked_add(payload_len)
        .ok_or(RecordStopReason::LengthOverflow)?;
    let commit_offset = crc_offset
        .checked_add(4)
        .ok_or(RecordStopReason::LengthOverflow)?;
    let total = commit_offset
        .checked_add(4)
        .ok_or(RecordStopReason::LengthOverflow)?;
    if input.len() < commit_offset {
        return Err(RecordStopReason::TruncatedRecord);
    }
    if input.len() < total {
        return Err(RecordStopReason::MissingCommit);
    }
    if crc32c::crc32c(&input[..crc_offset]) != read_u32(input, crc_offset) {
        return Err(RecordStopReason::BadCrc);
    }
    if input[commit_offset..total] != COMMIT_MAGIC {
        return Err(RecordStopReason::MissingCommit);
    }
    Ok(DecodedRecord {
        record_type,
        generation,
        sequence,
        payload: &input[RECORD_PREFIX_LEN..crc_offset],
        consumed: total,
    })
}

pub(crate) fn validate_record(
    input: &[u8],
    expected_sequence: u64,
    starting_generation: Generation,
) -> Result<usize, RecordStopReason> {
    decode_record(input, expected_sequence, starting_generation).map(|record| record.consumed)
}

pub(crate) fn hash_segment(bytes: &[u8]) -> SegmentHash {
    let mut digest = Sha256::new();
    digest.update(SEGMENT_HASH_DOMAIN.as_bytes());
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
    SegmentHash(digest.finalize().into())
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

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([input[offset], input[offset + 1]])
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
    ])
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
        input[offset + 4],
        input[offset + 5],
        input[offset + 6],
        input[offset + 7],
    ])
}

fn read_array_16(input: &[u8], offset: usize) -> [u8; 16] {
    input[offset..offset + 16]
        .try_into()
        .expect("validated fixed header length")
}

fn read_array_32(input: &[u8], offset: usize) -> [u8; 32] {
    input[offset..offset + 32]
        .try_into()
        .expect("validated fixed header length")
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
        COMMIT_MAGIC, HEADER_CRC_OFFSET, HeaderDecodeError, JournalEncodeError, JournalId,
        MAX_RECORD_PAYLOAD, RECORD_PREFIX_LEN, RecordStopReason, RecordType, ReplayLimits,
        ReplayResource, ReplayStop, SEGMENT_HEADER_LEN, SegmentEncoder, SegmentHeader,
        replay_ordered_segments,
    };
    use ariax_core::{Generation, Gid};

    fn gid() -> Gid {
        Gid::new(0x1234).expect("gid")
    }

    fn journal_id() -> JournalId {
        JournalId::new([7; 16]).expect("journal id")
    }

    fn first_segment() -> super::EncodedSegment {
        let mut encoder = SegmentEncoder::first(gid(), journal_id(), Generation::INITIAL, 1000);
        encoder
            .append(RecordType::TaskCreated, Generation::INITIAL, b"first")
            .expect("first");
        encoder
            .append(RecordType::OptionsSnapshot, Generation::INITIAL, b"second")
            .expect("second");
        encoder.finish()
    }

    fn rewrite_header_crc(encoded: &mut [u8]) {
        let crc = crc32c::crc32c(&encoded[..HEADER_CRC_OFFSET]);
        encoded[HEADER_CRC_OFFSET..SEGMENT_HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
    }

    fn replace_header(segment: &super::EncodedSegment, header: SegmentHeader) -> Vec<u8> {
        let mut bytes = segment.bytes().to_vec();
        bytes[..SEGMENT_HEADER_LEN].copy_from_slice(&header.encode());
        bytes
    }

    #[test]
    fn header_is_exact_length_crc_covered_and_round_trips() {
        let header = SegmentHeader::first(gid(), journal_id(), Generation::new(3), 44);
        let encoded = header.encode();
        assert_eq!(encoded.len(), SEGMENT_HEADER_LEN);
        assert_eq!(SegmentHeader::decode(&encoded), Ok(header));

        let mut corrupt = encoded;
        corrupt[44] ^= 1;
        assert_eq!(
            SegmentHeader::decode(&corrupt),
            Err(HeaderDecodeError::BadCrc)
        );
        let crc = crc32c::crc32c(&corrupt[..HEADER_CRC_OFFSET]);
        corrupt[HEADER_CRC_OFFSET..].copy_from_slice(&crc.to_le_bytes());
        assert_ne!(SegmentHeader::decode(&corrupt), Ok(header));
    }

    #[test]
    fn headers_reject_every_noncanonical_v1_field_class() {
        let encoded = SegmentHeader::first(gid(), journal_id(), Generation::INITIAL, 44).encode();
        assert_eq!(
            SegmentHeader::decode(&encoded[..SEGMENT_HEADER_LEN - 1]),
            Err(HeaderDecodeError::Truncated)
        );

        let cases = [
            (0, 1, HeaderDecodeError::InvalidMagic),
            (4, 1, HeaderDecodeError::UnsupportedVersion),
            (6, 1, HeaderDecodeError::InvalidEndianness),
            (7, 1, HeaderDecodeError::UnknownFlags),
            (36, 3, HeaderDecodeError::InvalidFirstSequence),
            (52, 1, HeaderDecodeError::UnexpectedPreviousLink),
        ];
        for (offset, xor, expected) in cases {
            let mut candidate = encoded;
            candidate[offset] ^= xor;
            rewrite_header_crc(&mut candidate);
            assert_eq!(SegmentHeader::decode(&candidate), Err(expected));
        }

        let mut zero_gid = encoded;
        zero_gid[8..16].fill(0);
        rewrite_header_crc(&mut zero_gid);
        assert_eq!(
            SegmentHeader::decode(&zero_gid),
            Err(HeaderDecodeError::ZeroGid)
        );

        let mut zero_journal = encoded;
        zero_journal[16..32].fill(0);
        rewrite_header_crc(&mut zero_journal);
        assert_eq!(
            SegmentHeader::decode(&zero_journal),
            Err(HeaderDecodeError::ZeroJournalId)
        );

        let mut missing_link = encoded;
        missing_link[32..36].copy_from_slice(&1_u32.to_le_bytes());
        missing_link[36..44].copy_from_slice(&2_u64.to_le_bytes());
        rewrite_header_crc(&mut missing_link);
        assert_eq!(
            SegmentHeader::decode(&missing_link),
            Err(HeaderDecodeError::MissingPreviousLink)
        );
    }

    #[test]
    fn records_round_trip_with_global_sequence_and_commit_markers() {
        let segment = first_segment();
        assert_eq!(segment.last_sequence(), 2);
        assert_eq!(&segment.bytes()[segment.bytes().len() - 4..], &COMMIT_MAGIC);
        let replay = replay_ordered_segments(&[segment.bytes()], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 2);
        assert_eq!(replay.records.len(), 2);
        assert_eq!(replay.records[0].payload.as_ref(), b"first");
        assert_eq!(replay.records[1].record_type, RecordType::OptionsSnapshot);
    }

    #[test]
    fn oversized_append_does_not_consume_a_sequence() {
        let mut encoder = SegmentEncoder::first(gid(), journal_id(), Generation::INITIAL, 0);
        let payload = vec![0; MAX_RECORD_PAYLOAD + 1];
        assert_eq!(
            encoder.append(RecordType::TaskCreated, Generation::INITIAL, &payload),
            Err(JournalEncodeError::PayloadTooLarge {
                length: MAX_RECORD_PAYLOAD + 1
            })
        );
        assert_eq!(
            encoder
                .append(RecordType::TaskCreated, Generation::INITIAL, b"ok")
                .expect("next append"),
            1
        );

        let mut generation = SegmentEncoder::first(gid(), journal_id(), Generation::new(2), 0);
        assert_eq!(
            generation.append(RecordType::TaskCreated, Generation::new(1), b"old"),
            Err(JournalEncodeError::GenerationBeforeSegment)
        );
        assert_eq!(
            generation
                .append(RecordType::TaskCreated, Generation::new(2), b"current")
                .expect("current generation"),
            1
        );
    }

    #[test]
    fn torn_active_tail_preserves_only_the_committed_prefix() {
        let segment = first_segment();
        let second_record_offset = SEGMENT_HEADER_LEN + RECORD_PREFIX_LEN + 5 + 8;
        for (cut, expected) in [
            (second_record_offset + 1, RecordStopReason::TruncatedFraming),
            (segment.bytes().len() - 5, RecordStopReason::TruncatedRecord),
            (segment.bytes().len() - 4, RecordStopReason::MissingCommit),
            (segment.bytes().len() - 1, RecordStopReason::MissingCommit),
        ] {
            let replay =
                replay_ordered_segments(&[&segment.bytes()[..cut]], ReplayLimits::default());
            assert_eq!(replay.records.len(), 1, "cut {cut}");
            assert!(matches!(
                replay.stop,
                ReplayStop::Record { reason, .. } if reason == expected
            ));
        }

        let boundary = replay_ordered_segments(
            &[&segment.bytes()[..second_record_offset]],
            ReplayLimits::default(),
        );
        assert_eq!(boundary.records.len(), 1);
        assert_eq!(boundary.stop, ReplayStop::CleanEnd);
    }

    #[test]
    fn framing_crc_commit_and_sequence_corruption_stop_replay() {
        let segment = first_segment();
        let record_offset = SEGMENT_HEADER_LEN;
        let cases = [
            (record_offset, RecordStopReason::InvalidMagic),
            (record_offset + 10, RecordStopReason::UnknownFlags),
            (record_offset + 8, RecordStopReason::UnknownRecordType),
            (
                record_offset + 20,
                RecordStopReason::SequenceGap {
                    expected: 1,
                    actual: 0,
                },
            ),
            (record_offset + RECORD_PREFIX_LEN, RecordStopReason::BadCrc),
            (
                record_offset + RECORD_PREFIX_LEN + 5 + 4,
                RecordStopReason::MissingCommit,
            ),
        ];
        for (offset, expected) in cases {
            let mut bytes = segment.bytes().to_vec();
            bytes[offset] ^= 1;
            let replay = replay_ordered_segments(&[&bytes], ReplayLimits::default());
            assert_eq!(replay.records.len(), 0);
            assert!(matches!(
                replay.stop,
                ReplayStop::Record { reason, .. } if reason == expected
            ));
        }
    }

    #[test]
    fn oversized_length_is_rejected_before_payload_collection() {
        let segment = first_segment();
        let mut bytes = segment.bytes().to_vec();
        bytes[SEGMENT_HEADER_LEN + 4..SEGMENT_HEADER_LEN + 8]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        let replay = replay_ordered_segments(
            &[&bytes],
            ReplayLimits {
                max_payload_bytes: 0,
                ..ReplayLimits::default()
            },
        );
        assert!(matches!(
            replay.stop,
            ReplayStop::Record {
                reason: RecordStopReason::PayloadTooLarge,
                ..
            }
        ));
        assert_eq!(replay.payload_bytes, 0);
    }

    #[test]
    fn replay_rejects_a_record_generation_before_its_segment() {
        let mut encoder = SegmentEncoder::first(gid(), journal_id(), Generation::new(2), 1000);
        encoder
            .append(RecordType::TaskCreated, Generation::new(2), b"current")
            .expect("record");
        let segment = encoder.finish();
        let mut bytes = segment.bytes().to_vec();
        bytes[SEGMENT_HEADER_LEN + 12..SEGMENT_HEADER_LEN + 20]
            .copy_from_slice(&1_u64.to_le_bytes());

        assert!(matches!(
            replay_ordered_segments(&[&bytes], ReplayLimits::default()).stop,
            ReplayStop::Record {
                reason: RecordStopReason::GenerationBeforeSegment,
                ..
            }
        ));
    }

    #[test]
    fn rotation_links_immutable_segments_and_detects_missing_or_corrupt_links() {
        let first = first_segment();
        let mut next = first.rotate(Generation::new(1), 2000).expect("rotate");
        next.append(RecordType::GenerationStarted, Generation::new(1), b"next")
            .expect("record");
        let second = next.finish();
        let replay =
            replay_ordered_segments(&[first.bytes(), second.bytes()], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 3);

        let missing = replay_ordered_segments(&[second.bytes()], ReplayLimits::default());
        assert!(matches!(missing.stop, ReplayStop::SegmentIndex { .. }));

        let mut corrupt_first = first.bytes().to_vec();
        corrupt_first[SEGMENT_HEADER_LEN + RECORD_PREFIX_LEN] ^= 1;
        let corrupt =
            replay_ordered_segments(&[&corrupt_first, second.bytes()], ReplayLimits::default());
        assert!(matches!(corrupt.stop, ReplayStop::Record { .. }));
        assert_eq!(corrupt.records.len(), 0);

        let mut wrong_hash_header = second.header();
        wrong_hash_header.previous_segment_hash = super::SegmentHash::ZERO;
        let wrong_hash = replace_header(&second, wrong_hash_header);
        assert!(matches!(
            replay_ordered_segments(&[first.bytes(), &wrong_hash], ReplayLimits::default()).stop,
            ReplayStop::Header {
                error: HeaderDecodeError::MissingPreviousLink,
                ..
            }
        ));

        let mut different_hash_header = second.header();
        different_hash_header.previous_segment_hash = super::SegmentHash([9; 32]);
        let different_hash = replace_header(&second, different_hash_header);
        assert_eq!(
            replay_ordered_segments(&[first.bytes(), &different_hash], ReplayLimits::default())
                .stop,
            ReplayStop::PreviousHashMismatch
        );

        let mut task_header = second.header();
        task_header.task_gid = Gid::new(0x5678).expect("other gid");
        let task_mismatch = replace_header(&second, task_header);
        assert_eq!(
            replay_ordered_segments(&[first.bytes(), &task_mismatch], ReplayLimits::default()).stop,
            ReplayStop::TaskMismatch
        );

        let mut journal_header = second.header();
        journal_header.journal_id = JournalId::new([8; 16]).expect("other journal");
        let journal_mismatch = replace_header(&second, journal_header);
        assert_eq!(
            replay_ordered_segments(&[first.bytes(), &journal_mismatch], ReplayLimits::default())
                .stop,
            ReplayStop::JournalMismatch
        );
    }

    #[test]
    fn replay_limits_stop_before_collecting_excess_state() {
        let segment = first_segment();
        let records = replay_ordered_segments(
            &[segment.bytes()],
            ReplayLimits {
                max_records: 1,
                ..ReplayLimits::default()
            },
        );
        assert_eq!(
            records.stop,
            ReplayStop::ResourceLimit(ReplayResource::Records)
        );
        assert_eq!(records.records.len(), 1);

        let payload = replay_ordered_segments(
            &[segment.bytes()],
            ReplayLimits {
                max_payload_bytes: 4,
                ..ReplayLimits::default()
            },
        );
        assert_eq!(
            payload.stop,
            ReplayStop::ResourceLimit(ReplayResource::PayloadBytes)
        );
        assert!(payload.records.is_empty());

        let segments = replay_ordered_segments(
            &[segment.bytes()],
            ReplayLimits {
                max_segments: 0,
                ..ReplayLimits::default()
            },
        );
        assert_eq!(
            segments.stop,
            ReplayStop::ResourceLimit(ReplayResource::Segments)
        );

        assert_eq!(
            replay_ordered_segments(&[], ReplayLimits::default()).stop,
            ReplayStop::NoSegments
        );
    }

    #[test]
    fn all_record_numbers_are_closed_and_contiguous() {
        for (index, record_type) in super::ALL_RECORD_TYPES.iter().copied().enumerate() {
            assert_eq!(record_type as u16, index as u16 + 1);
            assert_eq!(RecordType::try_from(index as u16 + 1), Ok(record_type));
        }
        assert!(RecordType::try_from(0).is_err());
        assert!(RecordType::try_from(25).is_err());
        assert!(JournalId::new([0; 16]).is_none());
    }
}
