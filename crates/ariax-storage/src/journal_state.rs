use crate::{
    CheckpointId, DataBarrierKind, DurabilityMode, FileEntry, FileIdentity, FileLayout,
    GenerationStartReason, JournalDigest, JournalFileLayoutEntry, JournalHash, JournalPayload,
    JournalRecord, LayoutError, MAX_LAYOUT_ENTRIES, OptionsSnapshotScope, PathValidationError,
    PayloadCodecError, PersistedId, PersistedSpan, PlatformPath, RecordType, RetryReason,
    RetryScope, RootBinding, RootBindingError, RootIdentity, SafePathBuilder, SanitizedOptionMap,
    TaskPauseReason, TaskRemoveReason, calculate_http_range_identity_fingerprint,
};
use ariax_core::{
    ErrorKind, FileId, Generation, LeaseId, OptionPatchId, PieceId, TaskId, TransferAttemptId,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

pub const CHECKPOINT_STATE_HASH_DOMAIN: &str = "ariax/checkpoint-state/v1\0";
pub const CONTRIBUTORS_HASH_DOMAIN: &str = "ariax/contributors/v1\0";
pub const VALIDATOR_SET_HASH_DOMAIN: &str = "ariax/validator-set/v1\0";
pub const REBIND_VALIDATOR_SET_HASH_DOMAIN: &str = "ariax/rebind-validator-set/v1\0";

/// Caller-owned policy for deciding which already-sanitized option names may be persisted.
pub trait PersistedOptionPolicy {
    fn permits(&self, name: &str) -> bool;
}

impl<F> PersistedOptionPolicy for F
where
    F: for<'name> Fn(&'name str) -> bool,
{
    fn permits(&self, name: &str) -> bool {
        self(name)
    }
}

/// Semantic-state caps applied before inserting attacker-controlled record collections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalStateLimits {
    pub max_records: usize,
    pub max_leases: usize,
    pub max_durable_pieces: usize,
    pub max_retry_states: usize,
    pub max_finalizations: usize,
}

impl Default for JournalStateLimits {
    fn default() -> Self {
        Self {
            max_records: 262_144,
            max_leases: 65_536,
            max_durable_pieces: 262_144,
            max_retry_states: 65_536,
            max_finalizations: MAX_LAYOUT_ENTRIES,
        }
    }
}

/// One canonical current or staged generation option snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredOptionSnapshot {
    patch_id: Option<OptionPatchId>,
    snapshot_hash: JournalHash,
    options: SanitizedOptionMap,
}

/// One canonical committed lease tuple used to prove a piece's contributors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalContributor {
    lease_id: LeaseId,
    span: PersistedSpan,
    validator_fingerprint: JournalHash,
}

impl JournalContributor {
    #[must_use]
    pub const fn new(
        lease_id: LeaseId,
        span: PersistedSpan,
        validator_fingerprint: JournalHash,
    ) -> Self {
        Self {
            lease_id,
            span,
            validator_fingerprint,
        }
    }

    #[must_use]
    pub const fn lease_id(self) -> LeaseId {
        self.lease_id
    }

    #[must_use]
    pub const fn span(self) -> PersistedSpan {
        self.span
    }

    #[must_use]
    pub const fn validator_fingerprint(self) -> JournalHash {
        self.validator_fingerprint
    }
}

impl RecoveredOptionSnapshot {
    #[must_use]
    pub const fn patch_id(&self) -> Option<OptionPatchId> {
        self.patch_id
    }

    #[must_use]
    pub const fn snapshot_hash(&self) -> JournalHash {
        self.snapshot_hash
    }

    #[must_use]
    pub const fn options(&self) -> &SanitizedOptionMap {
        &self.options
    }
}

/// A fully reassembled layout whose persisted hashes were recomputed successfully.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredLayout {
    layout: FileLayout,
    layout_hash: JournalHash,
    root_binding_hash: JournalHash,
}

impl RecoveredLayout {
    #[must_use]
    pub const fn layout(&self) -> &FileLayout {
        &self.layout
    }

    #[must_use]
    pub const fn layout_hash(&self) -> JournalHash {
        self.layout_hash
    }

    #[must_use]
    pub const fn root_binding_hash(&self) -> JournalHash {
        self.root_binding_hash
    }

    fn has_same_identity_evidence(&self, other: &Self) -> bool {
        if self.layout_hash != other.layout_hash
            || self.layout.root_binding().path().platform()
                != other.layout.root_binding().path().platform()
            || self.layout.root_binding().root_identity()
                != other.layout.root_binding().root_identity()
            || self.layout.files().len() != other.layout.files().len()
        {
            return false;
        }
        self.layout
            .files()
            .iter()
            .zip(other.layout.files())
            .all(|(left, right)| {
                left.id() == right.id()
                    && left.identity().map(FileIdentity::bytes)
                        == right.identity().map(FileIdentity::bytes)
            })
    }
}

/// How one durable piece entered the trusted recovered state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurablePieceOrigin {
    Live {
        contributors_hash: JournalHash,
        data_barrier: DataBarrierKind,
    },
    Checkpoint,
}

/// Durable evidence retained across restart for one exact layout piece.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredDurablePiece {
    piece_id: PieceId,
    piece_span: PersistedSpan,
    validator_set_fingerprint: JournalHash,
    digest: Option<JournalDigest>,
    origin: DurablePieceOrigin,
}

impl RecoveredDurablePiece {
    #[must_use]
    pub const fn piece_id(&self) -> PieceId {
        self.piece_id
    }

    #[must_use]
    pub const fn piece_span(&self) -> PersistedSpan {
        self.piece_span
    }

    #[must_use]
    pub const fn validator_set_fingerprint(&self) -> JournalHash {
        self.validator_set_fingerprint
    }

    #[must_use]
    pub const fn digest(&self) -> Option<&JournalDigest> {
        self.digest.as_ref()
    }

    #[must_use]
    pub const fn origin(&self) -> &DurablePieceOrigin {
        &self.origin
    }
}

/// Latest persisted retry decision for one semantic retry owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredRetryState {
    pub scope: RetryScope,
    pub scope_id: PersistedId,
    pub attempt: u32,
    pub elapsed_before_wait_ms: u64,
    pub scheduled_at_unix_ms: u64,
    pub delay_ms: u64,
    pub error_class: ErrorKind,
    pub retry_reason: RetryReason,
}

/// Bounded raw strong ETag material retained for one exact HTTP resource and
/// representation length. This is the only journal value authorized to become
/// an `If-Range` header after semantic replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredHttpStrongValidator {
    resource_fingerprint: JournalHash,
    validator_fingerprint: JournalHash,
    total_length: u64,
    etag: Box<[u8]>,
}

impl RecoveredHttpStrongValidator {
    #[must_use]
    pub const fn resource_fingerprint(&self) -> JournalHash {
        self.resource_fingerprint
    }

    #[must_use]
    pub const fn validator_fingerprint(&self) -> JournalHash {
        self.validator_fingerprint
    }

    #[must_use]
    pub const fn total_length(&self) -> u64 {
        self.total_length
    }

    #[must_use]
    pub fn etag(&self) -> &[u8] {
        &self.etag
    }
}

/// Bounded digest-only HTTP identity retained across restart.  Unlike a
/// strong validator it never becomes an `If-Range` value; it only authorizes
/// exact-range body revalidation before recovered durable pieces are released.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredHttpRangeIdentity {
    identity_fingerprint: JournalHash,
    total_length: u64,
    representation_digest: JournalDigest,
}

impl RecoveredHttpRangeIdentity {
    #[must_use]
    pub const fn identity_fingerprint(&self) -> JournalHash {
        self.identity_fingerprint
    }

    #[must_use]
    pub const fn total_length(&self) -> u64 {
        self.total_length
    }

    #[must_use]
    pub const fn representation_digest(&self) -> &JournalDigest {
        &self.representation_digest
    }
}

/// One final rename and whether its matching `FinalizeDone` was observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredFinalization {
    pub layout_hash: JournalHash,
    pub root_binding_hash: JournalHash,
    pub file_id: FileId,
    pub temp_relative_path: crate::JournalRelativePath,
    pub final_relative_path: crate::JournalRelativePath,
    pub final_length: u64,
    pub file_identity: Box<[u8]>,
    pub done: bool,
}

/// A task-local terminal safety veto reconstructed from the journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveredTerminal {
    Complete {
        layout_hash: JournalHash,
        final_length: u64,
        final_digest: Option<JournalDigest>,
        completed_at_unix_ms: u64,
    },
    Error {
        error_class: ErrorKind,
        retriable: bool,
        diagnostic_id: u64,
    },
    Removed {
        reason: TaskRemoveReason,
    },
}

/// A complete compact-checkpoint envelope that passed count and state-hash validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub source_last_sequence: u64,
    pub source_segment_hash: JournalHash,
    pub state_record_count: u32,
    pub state_hash: JournalHash,
    pub end_sequence: u64,
}

/// A clean-shutdown marker is meaningful only when it remains at the valid semantic tail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveredCleanShutdown {
    pub checkpoint_sequence: u64,
    pub shutdown_at_unix_ms: u64,
    pub record_sequence: u64,
}

/// Trusted task state reconstructed from the semantic valid prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredJournalState {
    task: TaskId,
    durability: DurabilityMode,
    creator_version: u16,
    generation: Generation,
    current_options: Option<RecoveredOptionSnapshot>,
    pending_options: Option<RecoveredOptionSnapshot>,
    layout: Option<RecoveredLayout>,
    http_strong_validator: Option<RecoveredHttpStrongValidator>,
    http_range_identity: Option<RecoveredHttpRangeIdentity>,
    durable_pieces: BTreeMap<PieceId, RecoveredDurablePiece>,
    retry_states: BTreeMap<(u8, u64), RecoveredRetryState>,
    paused: Option<TaskPauseReason>,
    finalizations: BTreeMap<FileId, RecoveredFinalization>,
    terminal: Option<RecoveredTerminal>,
    checkpoint: Option<RecoveredCheckpoint>,
    rebind_source_root_binding_hash: Option<JournalHash>,
    clean_shutdown: Option<RecoveredCleanShutdown>,
    last_sequence: u64,
}

impl RecoveredJournalState {
    #[must_use]
    pub const fn task(&self) -> TaskId {
        self.task
    }

    #[must_use]
    pub const fn durability(&self) -> DurabilityMode {
        self.durability
    }

    #[must_use]
    pub const fn creator_version(&self) -> u16 {
        self.creator_version
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn current_options(&self) -> Option<&RecoveredOptionSnapshot> {
        self.current_options.as_ref()
    }

    #[must_use]
    pub const fn pending_options(&self) -> Option<&RecoveredOptionSnapshot> {
        self.pending_options.as_ref()
    }

    #[must_use]
    pub const fn layout(&self) -> Option<&RecoveredLayout> {
        self.layout.as_ref()
    }

    #[must_use]
    pub const fn http_strong_validator(&self) -> Option<&RecoveredHttpStrongValidator> {
        self.http_strong_validator.as_ref()
    }

    #[must_use]
    pub const fn http_range_identity(&self) -> Option<&RecoveredHttpRangeIdentity> {
        self.http_range_identity.as_ref()
    }

    #[must_use]
    pub const fn durable_pieces(&self) -> &BTreeMap<PieceId, RecoveredDurablePiece> {
        &self.durable_pieces
    }

    #[must_use]
    pub const fn retry_states(&self) -> &BTreeMap<(u8, u64), RecoveredRetryState> {
        &self.retry_states
    }

    #[must_use]
    pub const fn paused(&self) -> Option<TaskPauseReason> {
        self.paused
    }

    #[must_use]
    pub const fn finalizations(&self) -> &BTreeMap<FileId, RecoveredFinalization> {
        &self.finalizations
    }

    #[must_use]
    pub const fn terminal(&self) -> Option<&RecoveredTerminal> {
        self.terminal.as_ref()
    }

    #[must_use]
    pub const fn checkpoint(&self) -> Option<&RecoveredCheckpoint> {
        self.checkpoint.as_ref()
    }

    #[must_use]
    pub const fn rebind_source_root_binding_hash(&self) -> Option<JournalHash> {
        self.rebind_source_root_binding_hash
    }

    #[must_use]
    pub const fn clean_shutdown(&self) -> Option<RecoveredCleanShutdown> {
        self.clean_shutdown
    }

    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }
}

/// Which semantic-state collection hit its configured cap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalStateResource {
    Records,
    Leases,
    DurablePieces,
    RetryStates,
    Finalizations,
}

impl JournalStateResource {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Leases => "leases",
            Self::DurablePieces => "durable_pieces",
            Self::RetryStates => "retry_states",
            Self::Finalizations => "finalizations",
        }
    }
}

/// Why one structurally valid record was not legal in the semantic v1 history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalStateError {
    ResourceLimit(JournalStateResource),
    AllocationFailed,
    SequenceMismatch {
        expected: u64,
        actual: u64,
    },
    Payload(PayloadCodecError),
    CheckpointNotFirst,
    CheckpointIncomplete,
    CheckpointCountMismatch,
    CheckpointIdMismatch,
    CheckpointHashMismatch,
    CheckpointRecordForbidden,
    NonCanonicalCheckpointOrder,
    TaskCreatedMissing,
    DuplicateTaskCreated,
    InvalidInitialGeneration,
    RecordAfterTerminal,
    GenerationMismatch {
        expected: Generation,
        actual: Generation,
    },
    GenerationAdvanceMismatch,
    CurrentSnapshotMissing,
    DuplicateCurrentSnapshot,
    SnapshotHashMismatch,
    ForbiddenPersistedOption {
        name: Box<str>,
    },
    InvalidStagedSnapshotReplacement,
    StagedSnapshotMissing,
    StagedSnapshotMismatch,
    PatchReasonMismatch,
    GenerationNotDrained,
    LayoutMissing,
    DuplicateLayout,
    UnexpectedLayoutChunk,
    IncompleteLayoutSequence,
    LayoutChunkMismatch,
    LayoutCountMismatch,
    Layout(LayoutError),
    LayoutPath(PathValidationError),
    RootBinding(RootBindingError),
    LayoutHashMismatch,
    RootBindingHashMismatch,
    InvalidHttpStrongValidator,
    InvalidHttpRangeIdentity,
    SpanOutsideLayout,
    PieceSpanMismatch,
    DuplicateLease,
    UnknownLease,
    LeaseMismatch,
    PieceLeaseMismatch,
    NonCanonicalContributors,
    VerificationMismatch,
    DurabilityBarrierMismatch,
    DuplicateDurablePiece,
    PieceStateOutsideCheckpoint,
    DuplicatePieceState,
    IncompletePieceStateSequence,
    PieceStateChunkMismatch,
    FinalizationMismatch,
    DuplicateTerminal,
    InvalidCleanShutdown,
}

impl JournalStateError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ResourceLimit(_) => "resource_limit",
            Self::AllocationFailed => "allocation_failed",
            Self::SequenceMismatch { .. } => "sequence_mismatch",
            Self::Payload(_) => "payload_codec",
            Self::CheckpointNotFirst => "checkpoint_not_first",
            Self::CheckpointIncomplete => "checkpoint_incomplete",
            Self::CheckpointCountMismatch => "checkpoint_count_mismatch",
            Self::CheckpointIdMismatch => "checkpoint_id_mismatch",
            Self::CheckpointHashMismatch => "checkpoint_hash_mismatch",
            Self::CheckpointRecordForbidden => "checkpoint_record_forbidden",
            Self::NonCanonicalCheckpointOrder => "noncanonical_checkpoint_order",
            Self::TaskCreatedMissing => "task_created_missing",
            Self::DuplicateTaskCreated => "duplicate_task_created",
            Self::InvalidInitialGeneration => "invalid_initial_generation",
            Self::RecordAfterTerminal => "record_after_terminal",
            Self::GenerationMismatch { .. } => "generation_mismatch",
            Self::GenerationAdvanceMismatch => "generation_advance_mismatch",
            Self::CurrentSnapshotMissing => "current_snapshot_missing",
            Self::DuplicateCurrentSnapshot => "duplicate_current_snapshot",
            Self::SnapshotHashMismatch => "snapshot_hash_mismatch",
            Self::ForbiddenPersistedOption { .. } => "forbidden_persisted_option",
            Self::InvalidStagedSnapshotReplacement => "invalid_staged_snapshot_replacement",
            Self::StagedSnapshotMissing => "staged_snapshot_missing",
            Self::StagedSnapshotMismatch => "staged_snapshot_mismatch",
            Self::PatchReasonMismatch => "patch_reason_mismatch",
            Self::GenerationNotDrained => "generation_not_drained",
            Self::LayoutMissing => "layout_missing",
            Self::DuplicateLayout => "duplicate_layout",
            Self::UnexpectedLayoutChunk => "unexpected_layout_chunk",
            Self::IncompleteLayoutSequence => "incomplete_layout_sequence",
            Self::LayoutChunkMismatch => "layout_chunk_mismatch",
            Self::LayoutCountMismatch => "layout_count_mismatch",
            Self::Layout(_) => "invalid_layout",
            Self::LayoutPath(_) => "invalid_layout_path",
            Self::RootBinding(_) => "invalid_root_binding",
            Self::LayoutHashMismatch => "layout_hash_mismatch",
            Self::RootBindingHashMismatch => "root_binding_hash_mismatch",
            Self::InvalidHttpStrongValidator => "invalid_http_strong_validator",
            Self::InvalidHttpRangeIdentity => "invalid_http_range_identity",
            Self::SpanOutsideLayout => "span_outside_layout",
            Self::PieceSpanMismatch => "piece_span_mismatch",
            Self::DuplicateLease => "duplicate_lease",
            Self::UnknownLease => "unknown_lease",
            Self::LeaseMismatch => "lease_mismatch",
            Self::PieceLeaseMismatch => "piece_lease_mismatch",
            Self::NonCanonicalContributors => "noncanonical_contributors",
            Self::VerificationMismatch => "verification_mismatch",
            Self::DurabilityBarrierMismatch => "durability_barrier_mismatch",
            Self::DuplicateDurablePiece => "duplicate_durable_piece",
            Self::PieceStateOutsideCheckpoint => "piece_state_outside_checkpoint",
            Self::DuplicatePieceState => "duplicate_piece_state",
            Self::IncompletePieceStateSequence => "incomplete_piece_state_sequence",
            Self::PieceStateChunkMismatch => "piece_state_chunk_mismatch",
            Self::FinalizationMismatch => "finalization_mismatch",
            Self::DuplicateTerminal => "duplicate_terminal",
            Self::InvalidCleanShutdown => "invalid_clean_shutdown",
        }
    }
}

pub const ALL_JOURNAL_STATE_ERROR_CODES: &[&str] = &[
    "resource_limit",
    "allocation_failed",
    "sequence_mismatch",
    "payload_codec",
    "checkpoint_not_first",
    "checkpoint_incomplete",
    "checkpoint_count_mismatch",
    "checkpoint_id_mismatch",
    "checkpoint_hash_mismatch",
    "checkpoint_record_forbidden",
    "noncanonical_checkpoint_order",
    "task_created_missing",
    "duplicate_task_created",
    "invalid_initial_generation",
    "record_after_terminal",
    "generation_mismatch",
    "generation_advance_mismatch",
    "current_snapshot_missing",
    "duplicate_current_snapshot",
    "snapshot_hash_mismatch",
    "forbidden_persisted_option",
    "invalid_staged_snapshot_replacement",
    "staged_snapshot_missing",
    "staged_snapshot_mismatch",
    "patch_reason_mismatch",
    "generation_not_drained",
    "layout_missing",
    "duplicate_layout",
    "unexpected_layout_chunk",
    "incomplete_layout_sequence",
    "layout_chunk_mismatch",
    "layout_count_mismatch",
    "invalid_layout",
    "invalid_layout_path",
    "invalid_root_binding",
    "layout_hash_mismatch",
    "root_binding_hash_mismatch",
    "invalid_http_strong_validator",
    "invalid_http_range_identity",
    "span_outside_layout",
    "piece_span_mismatch",
    "duplicate_lease",
    "unknown_lease",
    "lease_mismatch",
    "piece_lease_mismatch",
    "noncanonical_contributors",
    "verification_mismatch",
    "durability_barrier_mismatch",
    "duplicate_durable_piece",
    "piece_state_outside_checkpoint",
    "duplicate_piece_state",
    "incomplete_piece_state_sequence",
    "piece_state_chunk_mismatch",
    "finalization_mismatch",
    "duplicate_terminal",
    "invalid_clean_shutdown",
];

impl fmt::Display for JournalStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceLimit(resource) => {
                write!(
                    formatter,
                    "journal state {} limit exceeded",
                    resource.code()
                )
            }
            Self::SequenceMismatch { expected, actual } => {
                write!(
                    formatter,
                    "expected semantic sequence {expected}, found {actual}"
                )
            }
            Self::Payload(error) => error.fmt(formatter),
            Self::ForbiddenPersistedOption { name } => {
                write!(
                    formatter,
                    "option {name} is forbidden in a journal snapshot"
                )
            }
            Self::GenerationMismatch { expected, actual } => write!(
                formatter,
                "expected generation {}, found {}",
                expected.get(),
                actual.get()
            ),
            Self::Layout(error) => error.fmt(formatter),
            Self::LayoutPath(error) => error.fmt(formatter),
            Self::RootBinding(error) => error.fmt(formatter),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for JournalStateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Payload(error) => Some(error),
            Self::Layout(error) => Some(error),
            Self::LayoutPath(error) => Some(error),
            Self::RootBinding(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PayloadCodecError> for JournalStateError {
    fn from(error: PayloadCodecError) -> Self {
        Self::Payload(error)
    }
}

/// Why semantic replay stopped and whether a trusted prefix remains usable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalStateStop {
    NoRecords,
    CleanEnd,
    IncompleteLayout {
        expected_chunk: u32,
        chunk_count: u32,
    },
    InvalidRecord {
        record_index: usize,
        sequence: u64,
        error: JournalStateError,
    },
}

/// Semantic valid-prefix result layered on top of framing replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalStateReplay {
    pub state: Option<RecoveredJournalState>,
    pub accepted_records: usize,
    pub last_sequence: u64,
    pub stop: JournalStateStop,
}

/// Hashes canonical checkpoint state records without their mutable sequence/CRC framing.
pub fn calculate_checkpoint_state_hash(
    records: &[JournalRecord],
) -> Result<JournalHash, JournalStateError> {
    let count = u32::try_from(records.len())
        .map_err(|_| JournalStateError::ResourceLimit(JournalStateResource::Records))?;
    let mut digest = Sha256::new();
    digest.update(CHECKPOINT_STATE_HASH_DOMAIN.as_bytes());
    digest.update(count.to_le_bytes());
    for record in records {
        let payload_len = u32::try_from(record.payload.len())
            .map_err(|_| JournalStateError::Payload(PayloadCodecError::PayloadTooLarge))?;
        digest.update((record.record_type as u16).to_le_bytes());
        digest.update(record.generation.get().to_le_bytes());
        digest.update(payload_len.to_le_bytes());
        digest.update(&record.payload);
    }
    Ok(JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero"))
}

/// Hashes a strictly lease-ID-ordered contributor list exactly as `PieceVerified` records use it.
pub fn calculate_contributors_hash(
    contributors: &[JournalContributor],
) -> Result<JournalHash, JournalStateError> {
    if contributors
        .windows(2)
        .any(|pair| pair[0].lease_id().get() >= pair[1].lease_id().get())
    {
        return Err(JournalStateError::NonCanonicalContributors);
    }
    let count = u32::try_from(contributors.len())
        .map_err(|_| JournalStateError::ResourceLimit(JournalStateResource::Leases))?;
    let mut digest = Sha256::new();
    digest.update(CONTRIBUTORS_HASH_DOMAIN.as_bytes());
    digest.update(count.to_le_bytes());
    for contributor in contributors {
        digest.update(contributor.lease_id().get().to_le_bytes());
        digest.update(contributor.span().offset().to_le_bytes());
        digest.update(contributor.span().len().to_le_bytes());
        digest.update(contributor.validator_fingerprint().as_bytes());
    }
    Ok(JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero"))
}

/// Hashes the canonical distinct validator set retained with a durable piece.
pub fn calculate_validator_set_fingerprint(
    contributors: &[JournalContributor],
) -> Result<JournalHash, JournalStateError> {
    let validators = contributors
        .iter()
        .map(|contributor| contributor.validator_fingerprint())
        .collect::<BTreeSet<_>>();
    let mut digest = Sha256::new();
    digest.update(VALIDATOR_SET_HASH_DOMAIN.as_bytes());
    let count = u32::try_from(validators.len())
        .map_err(|_| JournalStateError::ResourceLimit(JournalStateResource::Leases))?;
    digest.update(count.to_le_bytes());
    for validator in validators {
        digest.update(validator.as_bytes());
    }
    Ok(JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero"))
}

/// Hashes the special validator evidence for a digest-proven different-identity rebind.
#[must_use]
pub fn calculate_rebind_validator_set_fingerprint(
    previous_root_binding_hash: JournalHash,
    new_root_binding_hash: JournalHash,
    digest_value: &JournalDigest,
) -> JournalHash {
    let mut digest = Sha256::new();
    digest.update(REBIND_VALIDATOR_SET_HASH_DOMAIN.as_bytes());
    digest.update(previous_root_binding_hash.as_bytes());
    digest.update(new_root_binding_hash.as_bytes());
    let algorithm = digest_value.algorithm().code().as_bytes();
    digest.update((algorithm.len() as u32).to_le_bytes());
    digest.update(algorithm);
    digest.update((digest_value.value().len() as u32).to_le_bytes());
    digest.update(digest_value.value());
    JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero")
}

/// Reconstructs trusted state, stopping before the first semantically invalid live record.
///
/// A compact checkpoint candidate is stricter: any invalid envelope, state record, chunk,
/// or state hash rejects the candidate as a whole and returns no state.
#[must_use]
pub fn recover_journal_state<P>(
    records: &[JournalRecord],
    task: TaskId,
    option_policy: &P,
    limits: JournalStateLimits,
) -> JournalStateReplay
where
    P: PersistedOptionPolicy + ?Sized,
{
    if records.is_empty() {
        return JournalStateReplay {
            state: None,
            accepted_records: 0,
            last_sequence: 0,
            stop: JournalStateStop::NoRecords,
        };
    }
    if records[0].record_type == RecordType::CheckpointStart {
        recover_checkpoint(records, task, option_policy, limits)
    } else {
        recover_live(records, task, option_policy, limits)
    }
}

fn recover_live<P>(
    records: &[JournalRecord],
    task: TaskId,
    option_policy: &P,
    limits: JournalStateLimits,
) -> JournalStateReplay
where
    P: PersistedOptionPolicy + ?Sized,
{
    let mut machine = SemanticMachine::new(task, option_policy, limits, false, None);
    match process_records(&mut machine, records, 0, 1) {
        Ok(()) => finish_replay(machine, records.len()),
        Err((index, error)) => {
            let accepted = index;
            let last_sequence = if accepted == 0 {
                0
            } else {
                records[accepted - 1].sequence
            };
            invalid_replay(
                machine.state,
                accepted,
                last_sequence,
                index,
                records[index].sequence,
                error,
            )
        }
    }
}

fn recover_checkpoint<P>(
    records: &[JournalRecord],
    task: TaskId,
    option_policy: &P,
    limits: JournalStateLimits,
) -> JournalStateReplay
where
    P: PersistedOptionPolicy + ?Sized,
{
    if records[0].sequence != 1 {
        return checkpoint_invalid(
            0,
            records[0].sequence,
            JournalStateError::SequenceMismatch {
                expected: 1,
                actual: records[0].sequence,
            },
        );
    }
    let start = match records[0].decode_payload() {
        Ok(JournalPayload::CheckpointStart {
            checkpoint_id,
            source_last_sequence,
            source_segment_hash,
            state_record_count,
            ..
        }) => (
            checkpoint_id,
            source_last_sequence,
            source_segment_hash,
            state_record_count,
        ),
        Ok(_) => unreachable!("record type and payload variant are coupled"),
        Err(error) => return checkpoint_invalid(0, records[0].sequence, error.into()),
    };
    let state_count = match usize::try_from(start.3) {
        Ok(count) => count,
        Err(_) => {
            return checkpoint_invalid(
                0,
                records[0].sequence,
                JournalStateError::CheckpointCountMismatch,
            );
        }
    };
    let end_index = match 1_usize.checked_add(state_count) {
        Some(index) if index < records.len() => index,
        _ => {
            return checkpoint_invalid(
                records.len() - 1,
                records.last().expect("nonempty").sequence,
                JournalStateError::CheckpointIncomplete,
            );
        }
    };
    if end_index >= limits.max_records {
        let index = limits.max_records.min(records.len() - 1);
        return checkpoint_invalid(
            index,
            records[index].sequence,
            JournalStateError::ResourceLimit(JournalStateResource::Records),
        );
    }
    for (index, record) in records.iter().enumerate().take(end_index + 1).skip(1) {
        let expected = index as u64 + 1;
        if record.sequence != expected {
            return checkpoint_invalid(
                index,
                record.sequence,
                JournalStateError::SequenceMismatch {
                    expected,
                    actual: record.sequence,
                },
            );
        }
    }
    let state_records = &records[1..end_index];
    let calculated_hash = match calculate_checkpoint_state_hash(state_records) {
        Ok(hash) => hash,
        Err(error) => return checkpoint_invalid(0, records[0].sequence, error),
    };
    let end = match records[end_index].decode_payload() {
        Ok(JournalPayload::CheckpointEnd {
            checkpoint_id,
            state_record_count,
            state_hash,
        }) => (checkpoint_id, state_record_count, state_hash),
        Ok(_) => {
            return checkpoint_invalid(
                end_index,
                records[end_index].sequence,
                JournalStateError::CheckpointIncomplete,
            );
        }
        Err(error) => {
            return checkpoint_invalid(end_index, records[end_index].sequence, error.into());
        }
    };
    if records[end_index].record_type != RecordType::CheckpointEnd {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::CheckpointIncomplete,
        );
    }
    if records[end_index].generation != records[0].generation {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::GenerationMismatch {
                expected: records[0].generation,
                actual: records[end_index].generation,
            },
        );
    }
    if end.0 != start.0 {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::CheckpointIdMismatch,
        );
    }
    if end.1 != start.3 {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::CheckpointCountMismatch,
        );
    }
    if end.2 != calculated_hash {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::CheckpointHashMismatch,
        );
    }

    let mut machine = SemanticMachine::new(
        task,
        option_policy,
        limits,
        true,
        Some(records[0].generation),
    );
    if let Err((relative, error)) = process_records(&mut machine, state_records, 1, 2) {
        let index = relative + 1;
        return checkpoint_invalid(index, records[index].sequence, error);
    }
    if machine.state.is_none() {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::TaskCreatedMissing,
        );
    }
    if machine.pending_layout.is_some() || machine.pending_piece_state.is_some() {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::CheckpointIncomplete,
        );
    }
    if machine
        .state
        .as_ref()
        .is_none_or(|state| state.current_options.is_none())
    {
        return checkpoint_invalid(
            end_index,
            records[end_index].sequence,
            JournalStateError::CurrentSnapshotMissing,
        );
    }
    let state = machine.state.as_mut().expect("checked state");
    state.checkpoint = Some(RecoveredCheckpoint {
        checkpoint_id: start.0,
        source_last_sequence: start.1,
        source_segment_hash: start.2,
        state_record_count: start.3,
        state_hash: end.2,
        end_sequence: records[end_index].sequence,
    });
    state.last_sequence = records[end_index].sequence;
    machine.checkpoint_state = false;
    machine.expected_checkpoint_generation = None;

    let tail_start = end_index + 1;
    if tail_start < records.len() {
        let expected_sequence = records[end_index].sequence.saturating_add(1);
        if let Err((relative, error)) = process_records(
            &mut machine,
            &records[tail_start..],
            tail_start,
            expected_sequence,
        ) {
            let index = tail_start + relative;
            let last_sequence = records[index - 1].sequence;
            return invalid_replay(
                machine.state,
                index,
                last_sequence,
                index,
                records[index].sequence,
                error,
            );
        }
    }
    finish_replay(machine, records.len())
}

fn process_records<P>(
    machine: &mut SemanticMachine<'_, P>,
    records: &[JournalRecord],
    absolute_start: usize,
    mut expected_sequence: u64,
) -> Result<(), (usize, JournalStateError)>
where
    P: PersistedOptionPolicy + ?Sized,
{
    for (relative, record) in records.iter().enumerate() {
        if record.sequence != expected_sequence {
            return Err((
                relative,
                JournalStateError::SequenceMismatch {
                    expected: expected_sequence,
                    actual: record.sequence,
                },
            ));
        }
        if absolute_start + relative >= machine.limits.max_records {
            return Err((
                relative,
                JournalStateError::ResourceLimit(JournalStateResource::Records),
            ));
        }
        let payload = record
            .decode_payload()
            .map_err(|error| (relative, error.into()))?;
        machine
            .apply(record, payload)
            .map_err(|error| (relative, error))?;
        if let Some(state) = machine.state.as_mut() {
            state.last_sequence = record.sequence;
        }
        expected_sequence = expected_sequence.saturating_add(1);
    }
    Ok(())
}

fn finish_replay<P>(machine: SemanticMachine<'_, P>, accepted: usize) -> JournalStateReplay
where
    P: PersistedOptionPolicy + ?Sized,
{
    let last_sequence = machine
        .state
        .as_ref()
        .map_or(0, RecoveredJournalState::last_sequence);
    let stop = if let Some(pending) = machine.pending_layout {
        JournalStateStop::IncompleteLayout {
            expected_chunk: pending.next_chunk,
            chunk_count: pending.chunk_count,
        }
    } else {
        JournalStateStop::CleanEnd
    };
    JournalStateReplay {
        state: machine.state,
        accepted_records: accepted,
        last_sequence,
        stop,
    }
}

fn invalid_replay(
    state: Option<RecoveredJournalState>,
    accepted_records: usize,
    last_sequence: u64,
    record_index: usize,
    sequence: u64,
    error: JournalStateError,
) -> JournalStateReplay {
    JournalStateReplay {
        state,
        accepted_records,
        last_sequence,
        stop: JournalStateStop::InvalidRecord {
            record_index,
            sequence,
            error,
        },
    }
}

fn checkpoint_invalid(
    record_index: usize,
    sequence: u64,
    error: JournalStateError,
) -> JournalStateReplay {
    invalid_replay(None, 0, 0, record_index, sequence, error)
}

#[derive(Clone, Debug)]
struct PendingLayout {
    generation: Generation,
    layout_hash: JournalHash,
    root_binding_hash: JournalHash,
    root_display: PlatformPath,
    root_identity: Box<[u8]>,
    total_length: Option<u64>,
    piece_length: u64,
    total_file_count: usize,
    chunk_count: u32,
    next_chunk: u32,
    files: Vec<JournalFileLayoutEntry>,
    previous_layout: Option<RecoveredLayout>,
    previous_durable_pieces: BTreeMap<PieceId, RecoveredDurablePiece>,
}

#[derive(Clone, Debug)]
struct PendingPieceState {
    layout_hash: JournalHash,
    root_binding_hash: JournalHash,
    chunk_count: u32,
    next_chunk: u32,
    previous_covered_end: u64,
}

#[derive(Clone, Copy, Debug)]
struct LeaseState {
    span: PersistedSpan,
    validator_fingerprint: JournalHash,
}

#[derive(Clone, Debug)]
struct VerifiedPiece {
    span: PersistedSpan,
    contributors_hash: JournalHash,
    validator_set_fingerprint: JournalHash,
    digest: JournalDigest,
}

struct SemanticMachine<'a, P: ?Sized> {
    task: TaskId,
    option_policy: &'a P,
    limits: JournalStateLimits,
    checkpoint_state: bool,
    expected_checkpoint_generation: Option<Generation>,
    checkpoint_rank: u8,
    checkpoint_retry_key: Option<(u8, u64)>,
    state: Option<RecoveredJournalState>,
    pending_layout: Option<PendingLayout>,
    pending_piece_state: Option<PendingPieceState>,
    seen_piece_state: bool,
    layout_record_generation: Option<Generation>,
    active_leases: BTreeMap<LeaseId, LeaseState>,
    committed_leases: BTreeMap<LeaseId, LeaseState>,
    seen_leases: BTreeSet<LeaseId>,
    piece_leases: BTreeSet<(LeaseId, PieceId)>,
    verified_pieces: BTreeMap<PieceId, VerifiedPiece>,
    network_started: bool,
}

impl<'a, P> SemanticMachine<'a, P>
where
    P: PersistedOptionPolicy + ?Sized,
{
    fn new(
        task: TaskId,
        option_policy: &'a P,
        limits: JournalStateLimits,
        checkpoint_state: bool,
        expected_checkpoint_generation: Option<Generation>,
    ) -> Self {
        Self {
            task,
            option_policy,
            limits,
            checkpoint_state,
            expected_checkpoint_generation,
            checkpoint_rank: 0,
            checkpoint_retry_key: None,
            state: None,
            pending_layout: None,
            pending_piece_state: None,
            seen_piece_state: false,
            layout_record_generation: None,
            active_leases: BTreeMap::new(),
            committed_leases: BTreeMap::new(),
            seen_leases: BTreeSet::new(),
            piece_leases: BTreeSet::new(),
            verified_pieces: BTreeMap::new(),
            network_started: false,
        }
    }

    fn apply(
        &mut self,
        record: &JournalRecord,
        payload: JournalPayload,
    ) -> Result<(), JournalStateError> {
        if self.pending_layout.is_some() && !matches!(payload, JournalPayload::LayoutChunk { .. }) {
            return Err(JournalStateError::IncompleteLayoutSequence);
        }
        if self.pending_piece_state.is_some()
            && !matches!(payload, JournalPayload::PieceStateChunk { .. })
        {
            return Err(JournalStateError::IncompletePieceStateSequence);
        }
        if self.checkpoint_state {
            self.validate_checkpoint_order(&payload)?;
        } else if matches!(payload, JournalPayload::CheckpointStart { .. }) {
            return Err(JournalStateError::CheckpointNotFirst);
        }
        if self
            .state
            .as_ref()
            .is_some_and(|state| state.terminal.is_some())
            && !matches!(payload, JournalPayload::CleanShutdown { .. })
        {
            return Err(JournalStateError::RecordAfterTerminal);
        }
        let clears_clean_shutdown = !matches!(payload, JournalPayload::CleanShutdown { .. });
        let result = match payload {
            JournalPayload::TaskCreated {
                durability,
                creator_version,
            } => self.apply_task_created(record, durability, creator_version),
            JournalPayload::OptionsSnapshot {
                scope,
                patch_id,
                snapshot_hash,
                options,
            } => self.apply_options(record, scope, patch_id, snapshot_hash, options),
            JournalPayload::LayoutCommitted {
                layout_hash,
                root_binding_hash,
                root_display,
                root_identity,
                total_length,
                piece_length,
                total_file_count,
                chunk_count,
                inline_files,
            } => self.apply_layout_committed(
                record,
                PendingLayoutInput {
                    layout_hash,
                    root_binding_hash,
                    root_display,
                    root_identity,
                    total_length,
                    piece_length,
                    total_file_count,
                    chunk_count,
                    inline_files,
                },
            ),
            JournalPayload::LayoutChunk {
                layout_hash,
                root_binding_hash,
                chunk_index,
                chunk_count,
                files,
            } => self.apply_layout_chunk(
                record,
                layout_hash,
                root_binding_hash,
                chunk_index,
                chunk_count,
                files,
            ),
            JournalPayload::GenerationStarted {
                previous_generation,
                reason,
                next_snapshot_hash,
                patch_id,
            } => self.apply_generation_started(
                record,
                previous_generation,
                reason,
                next_snapshot_hash,
                patch_id,
            ),
            JournalPayload::LeaseStarted {
                transfer_attempt_id,
                lease_id,
                span,
                validator_fingerprint,
            } => self.apply_lease_started(
                record,
                transfer_attempt_id,
                lease_id,
                span,
                validator_fingerprint,
            ),
            JournalPayload::PieceStarted {
                lease_id,
                piece_id,
                piece_span,
            } => self.apply_piece_started(record, lease_id, piece_id, piece_span),
            JournalPayload::PieceWritten {
                lease_id,
                piece_id,
                written_span,
            } => self.apply_piece_written(record, lease_id, piece_id, written_span),
            JournalPayload::LeaseCommitted {
                lease_id,
                span,
                validator_fingerprint,
                ..
            } => self.apply_lease_committed(record, lease_id, span, validator_fingerprint),
            JournalPayload::LeaseAborted { lease_id, .. } => {
                self.apply_lease_aborted(record, lease_id)
            }
            JournalPayload::PieceVerified {
                piece_id,
                piece_span,
                contributors_hash,
                digest,
            } => self.apply_piece_verified(record, piece_id, piece_span, contributors_hash, digest),
            JournalPayload::PieceFailed {
                lease_id,
                piece_id,
                piece_span,
                ..
            } => self.apply_piece_failed(record, lease_id, piece_id, piece_span),
            JournalPayload::PieceDurable {
                piece_id,
                piece_span,
                contributors_hash,
                validator_set_fingerprint,
                digest,
                data_barrier,
            } => self.apply_piece_durable(
                record,
                piece_id,
                piece_span,
                contributors_hash,
                validator_set_fingerprint,
                digest,
                data_barrier,
            ),
            JournalPayload::RetryState {
                scope,
                scope_id,
                attempt,
                elapsed_before_wait_ms,
                scheduled_at_unix_ms,
                delay_ms,
                error_class,
                retry_reason,
            } => self.apply_retry(
                record,
                RecoveredRetryState {
                    scope,
                    scope_id,
                    attempt,
                    elapsed_before_wait_ms,
                    scheduled_at_unix_ms,
                    delay_ms,
                    error_class,
                    retry_reason,
                },
            ),
            JournalPayload::TaskPaused { reason } => self.apply_paused(record, reason),
            JournalPayload::TaskComplete {
                layout_hash,
                final_length,
                final_digest,
                completed_at_unix_ms,
            } => self.apply_complete(
                record,
                layout_hash,
                final_length,
                final_digest,
                completed_at_unix_ms,
            ),
            JournalPayload::TaskError {
                error_class,
                retriable,
                diagnostic_id,
            } => self.apply_terminal(
                record,
                RecoveredTerminal::Error {
                    error_class,
                    retriable,
                    diagnostic_id,
                },
            ),
            JournalPayload::TaskRemoved { reason } => {
                self.apply_terminal(record, RecoveredTerminal::Removed { reason })
            }
            JournalPayload::CleanShutdown {
                checkpoint_sequence,
                shutdown_at_unix_ms,
            } => self.apply_clean_shutdown(record, checkpoint_sequence, shutdown_at_unix_ms),
            JournalPayload::FinalizeIntent {
                layout_hash,
                root_binding_hash,
                file_id,
                temp_relative_path,
                final_relative_path,
                final_length,
                file_identity,
            } => self.apply_finalize_intent(
                record,
                RecoveredFinalization {
                    layout_hash,
                    root_binding_hash,
                    file_id,
                    temp_relative_path,
                    final_relative_path,
                    final_length,
                    file_identity,
                    done: false,
                },
            ),
            JournalPayload::FinalizeDone {
                layout_hash,
                root_binding_hash,
                file_id,
                final_relative_path,
            } => self.apply_finalize_done(
                record,
                layout_hash,
                root_binding_hash,
                file_id,
                final_relative_path,
            ),
            JournalPayload::PieceStateChunk {
                layout_hash,
                root_binding_hash,
                chunk_index,
                chunk_count,
                first_piece_id,
                covered_piece_count,
                durable_bitmap,
                evidence_runs,
            } => self.apply_piece_state_chunk(
                record,
                PieceStateInput {
                    layout_hash,
                    root_binding_hash,
                    chunk_index,
                    chunk_count,
                    first_piece_id,
                    covered_piece_count,
                    durable_bitmap,
                    evidence_runs,
                },
            ),
            JournalPayload::HttpStrongValidator {
                resource_fingerprint,
                validator_fingerprint,
                total_length,
                etag,
            } => self.apply_http_strong_validator(
                record,
                RecoveredHttpStrongValidator {
                    resource_fingerprint,
                    validator_fingerprint,
                    total_length,
                    etag,
                },
            ),
            JournalPayload::HttpRangeIdentity {
                identity_fingerprint,
                total_length,
                representation_digest,
            } => self.apply_http_range_identity(
                record,
                RecoveredHttpRangeIdentity {
                    identity_fingerprint,
                    total_length,
                    representation_digest,
                },
            ),
            JournalPayload::CheckpointStart { .. } | JournalPayload::CheckpointEnd { .. } => {
                Err(JournalStateError::CheckpointRecordForbidden)
            }
        };
        if result.is_ok()
            && clears_clean_shutdown
            && let Some(state) = self.state.as_mut()
        {
            state.clean_shutdown = None;
        }
        result
    }

    fn validate_checkpoint_order(
        &mut self,
        payload: &JournalPayload,
    ) -> Result<(), JournalStateError> {
        let rank = match payload {
            JournalPayload::TaskCreated { .. } => 1,
            JournalPayload::OptionsSnapshot {
                scope: OptionsSnapshotScope::CurrentGeneration,
                ..
            } => 2,
            JournalPayload::LayoutCommitted { .. } | JournalPayload::LayoutChunk { .. } => 3,
            JournalPayload::HttpStrongValidator { .. }
            | JournalPayload::HttpRangeIdentity { .. } => 4,
            JournalPayload::RetryState {
                scope, scope_id, ..
            } => {
                let key = (scope.number(), scope_id.get());
                if self
                    .checkpoint_retry_key
                    .is_some_and(|previous| previous >= key)
                {
                    return Err(JournalStateError::NonCanonicalCheckpointOrder);
                }
                self.checkpoint_retry_key = Some(key);
                5
            }
            JournalPayload::PieceStateChunk { .. } => 6,
            JournalPayload::TaskPaused { .. } => 7,
            JournalPayload::FinalizeIntent { .. } | JournalPayload::FinalizeDone { .. } => 8,
            JournalPayload::TaskComplete { .. }
            | JournalPayload::TaskError { .. }
            | JournalPayload::TaskRemoved { .. } => 9,
            _ => return Err(JournalStateError::CheckpointRecordForbidden),
        };
        if rank < self.checkpoint_rank
            || (rank == 1 && self.checkpoint_rank != 0)
            || (rank == 2 && self.checkpoint_rank == 2)
        {
            return Err(JournalStateError::NonCanonicalCheckpointOrder);
        }
        self.checkpoint_rank = rank;
        Ok(())
    }

    fn apply_task_created(
        &mut self,
        record: &JournalRecord,
        durability: DurabilityMode,
        creator_version: u16,
    ) -> Result<(), JournalStateError> {
        if self.state.is_some() {
            return Err(JournalStateError::DuplicateTaskCreated);
        }
        if let Some(expected) = self.expected_checkpoint_generation {
            if record.generation != expected {
                return Err(JournalStateError::GenerationMismatch {
                    expected,
                    actual: record.generation,
                });
            }
        } else if record.generation != Generation::INITIAL {
            return Err(JournalStateError::InvalidInitialGeneration);
        }
        self.state = Some(RecoveredJournalState {
            task: self.task,
            durability,
            creator_version,
            generation: record.generation,
            current_options: None,
            pending_options: None,
            layout: None,
            http_strong_validator: None,
            http_range_identity: None,
            durable_pieces: BTreeMap::new(),
            retry_states: BTreeMap::new(),
            paused: None,
            finalizations: BTreeMap::new(),
            terminal: None,
            checkpoint: None,
            rebind_source_root_binding_hash: None,
            clean_shutdown: None,
            last_sequence: record.sequence,
        });
        Ok(())
    }

    fn apply_options(
        &mut self,
        record: &JournalRecord,
        scope: OptionsSnapshotScope,
        patch_id: Option<OptionPatchId>,
        snapshot_hash: JournalHash,
        options: SanitizedOptionMap,
    ) -> Result<(), JournalStateError> {
        self.require_current_generation(record)?;
        if options.snapshot_hash() != snapshot_hash {
            return Err(JournalStateError::SnapshotHashMismatch);
        }
        for (name, _) in options.entries() {
            if !self.option_policy.permits(name) {
                return Err(JournalStateError::ForbiddenPersistedOption { name: name.into() });
            }
        }
        let snapshot = RecoveredOptionSnapshot {
            patch_id,
            snapshot_hash,
            options,
        };
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        match scope {
            OptionsSnapshotScope::CurrentGeneration => {
                if state.current_options.is_some() {
                    return Err(JournalStateError::DuplicateCurrentSnapshot);
                }
                if !self.checkpoint_state && state.generation != Generation::INITIAL {
                    return Err(JournalStateError::DuplicateCurrentSnapshot);
                }
                state.current_options = Some(snapshot);
            }
            OptionsSnapshotScope::NextAdmission => {
                if self.checkpoint_state {
                    return Err(JournalStateError::CheckpointRecordForbidden);
                }
                if state.current_options.is_none() {
                    return Err(JournalStateError::CurrentSnapshotMissing);
                }
                if let Some(previous) = state.pending_options.as_ref()
                    && (patch_id.is_none() || patch_id == previous.patch_id)
                {
                    return Err(JournalStateError::InvalidStagedSnapshotReplacement);
                }
                state.pending_options = Some(snapshot);
            }
        }
        Ok(())
    }

    fn apply_generation_started(
        &mut self,
        record: &JournalRecord,
        previous_generation: Generation,
        reason: GenerationStartReason,
        next_snapshot_hash: JournalHash,
        patch_id: Option<OptionPatchId>,
    ) -> Result<(), JournalStateError> {
        if self.checkpoint_state {
            return Err(JournalStateError::CheckpointRecordForbidden);
        }
        if !self.active_leases.is_empty() {
            return Err(JournalStateError::GenerationNotDrained);
        }
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if previous_generation != state.generation
            || state.generation.checked_next() != Some(record.generation)
        {
            return Err(JournalStateError::GenerationAdvanceMismatch);
        }
        if (reason == GenerationStartReason::OptionPatch) != patch_id.is_some() {
            return Err(JournalStateError::PatchReasonMismatch);
        }
        let pending = state
            .pending_options
            .take()
            .ok_or(JournalStateError::StagedSnapshotMissing)?;
        if pending.snapshot_hash != next_snapshot_hash || pending.patch_id != patch_id {
            state.pending_options = Some(pending);
            return Err(JournalStateError::StagedSnapshotMismatch);
        }
        state.generation = record.generation;
        state.current_options = Some(pending);
        state.retry_states.clear();
        state.paused = None;
        state.http_strong_validator = None;
        state.http_range_identity = None;
        if reason == GenerationStartReason::RepresentationRestart {
            // The old layout remains only as descriptor-bound authority for
            // reopening the task-owned file. No byte from the prior
            // representation may remain publishable in the new generation.
            state.durable_pieces.clear();
            state.finalizations.clear();
        }
        state.rebind_source_root_binding_hash = None;
        state.clean_shutdown = None;
        self.committed_leases.clear();
        self.seen_leases.clear();
        self.piece_leases.clear();
        self.verified_pieces.clear();
        self.network_started = false;
        self.layout_record_generation = None;
        Ok(())
    }

    fn apply_layout_committed(
        &mut self,
        record: &JournalRecord,
        input: PendingLayoutInput,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if !self.active_leases.is_empty() || self.network_started {
            return Err(JournalStateError::GenerationNotDrained);
        }
        if self.layout_record_generation == Some(record.generation) {
            return Err(JournalStateError::DuplicateLayout);
        }
        let total_file_count = usize::try_from(input.total_file_count)
            .map_err(|_| JournalStateError::LayoutCountMismatch)?;
        let mut pending = PendingLayout {
            generation: record.generation,
            layout_hash: input.layout_hash,
            root_binding_hash: input.root_binding_hash,
            root_display: input.root_display,
            root_identity: input.root_identity,
            total_length: input.total_length,
            piece_length: input.piece_length,
            total_file_count,
            chunk_count: input.chunk_count,
            next_chunk: 1,
            files: input.inline_files.into_vec(),
            previous_layout: None,
            previous_durable_pieces: BTreeMap::new(),
        };
        if pending.chunk_count == 1 {
            let recovered = assemble_layout(self.task, pending)?;
            let state = self
                .state
                .as_mut()
                .ok_or(JournalStateError::TaskCreatedMissing)?;
            let previous_layout = state.layout.take();
            let previous_durable_pieces = std::mem::take(&mut state.durable_pieces);
            self.install_layout(recovered.layout, previous_layout, previous_durable_pieces)
        } else {
            let state = self
                .state
                .as_mut()
                .ok_or(JournalStateError::TaskCreatedMissing)?;
            pending.previous_layout = state.layout.take();
            pending.previous_durable_pieces = std::mem::take(&mut state.durable_pieces);
            state.finalizations.clear();
            self.pending_layout = Some(pending);
            Ok(())
        }
    }

    fn apply_layout_chunk(
        &mut self,
        record: &JournalRecord,
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
        chunk_index: u32,
        chunk_count: u32,
        files: Box<[JournalFileLayoutEntry]>,
    ) -> Result<(), JournalStateError> {
        self.require_current_generation(record)?;
        let mut pending = self
            .pending_layout
            .take()
            .ok_or(JournalStateError::UnexpectedLayoutChunk)?;
        if pending.generation != record.generation
            || pending.layout_hash != layout_hash
            || pending.root_binding_hash != root_binding_hash
            || pending.chunk_count != chunk_count
            || pending.next_chunk != chunk_index
        {
            return Err(JournalStateError::LayoutChunkMismatch);
        }
        if pending
            .files
            .len()
            .checked_add(files.len())
            .is_none_or(|count| count > pending.total_file_count || count > MAX_LAYOUT_ENTRIES)
        {
            return Err(JournalStateError::LayoutCountMismatch);
        }
        if pending.files.last().is_none_or(|previous| {
            files.first().is_none_or(|next| {
                previous.file_id().get().checked_add(1) != Some(next.file_id().get())
                    || previous.global_end() != next.global_start()
            })
        }) {
            return Err(JournalStateError::LayoutChunkMismatch);
        }
        pending
            .files
            .try_reserve(files.len())
            .map_err(|_| JournalStateError::AllocationFailed)?;
        pending.files.extend(files);
        pending.next_chunk += 1;
        if pending.next_chunk == pending.chunk_count {
            if pending.files.len() != pending.total_file_count {
                return Err(JournalStateError::LayoutCountMismatch);
            }
            self.finish_layout(pending)
        } else {
            self.pending_layout = Some(pending);
            Ok(())
        }
    }

    fn finish_layout(&mut self, pending: PendingLayout) -> Result<(), JournalStateError> {
        if pending.files.len() != pending.total_file_count {
            return Err(JournalStateError::LayoutCountMismatch);
        }
        let recovered = assemble_layout(self.task, pending)?;
        self.install_layout(
            recovered.layout,
            recovered.previous_layout,
            recovered.previous_durable_pieces,
        )
    }

    fn install_layout(
        &mut self,
        layout: RecoveredLayout,
        previous_layout: Option<RecoveredLayout>,
        previous_durable_pieces: BTreeMap<PieceId, RecoveredDurablePiece>,
    ) -> Result<(), JournalStateError> {
        let retain_durable = previous_layout
            .as_ref()
            .is_some_and(|previous| previous.has_same_identity_evidence(&layout));
        let rebind_source_root_binding_hash = previous_layout
            .as_ref()
            .filter(|_| !retain_durable)
            .map(RecoveredLayout::root_binding_hash);
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        state.durable_pieces = if retain_durable {
            previous_durable_pieces
        } else {
            BTreeMap::new()
        };
        state.finalizations.clear();
        state.http_strong_validator = None;
        state.http_range_identity = None;
        state.rebind_source_root_binding_hash = rebind_source_root_binding_hash;
        state.layout = Some(layout);
        self.layout_record_generation = Some(state.generation);
        Ok(())
    }

    fn apply_http_strong_validator(
        &mut self,
        record: &JournalRecord,
        validator: RecoveredHttpStrongValidator,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if self.network_started {
            return Err(JournalStateError::InvalidHttpStrongValidator);
        }
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        let total_length = state
            .layout
            .as_ref()
            .and_then(|layout| layout.layout().total_length())
            .ok_or(JournalStateError::InvalidHttpStrongValidator)?;
        if state.http_strong_validator.is_some()
            || state.http_range_identity.is_some()
            || total_length != validator.total_length
        {
            return Err(JournalStateError::InvalidHttpStrongValidator);
        }
        state.http_strong_validator = Some(validator);
        Ok(())
    }

    fn apply_http_range_identity(
        &mut self,
        record: &JournalRecord,
        identity: RecoveredHttpRangeIdentity,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if self.network_started {
            return Err(JournalStateError::InvalidHttpRangeIdentity);
        }
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        let total_length = state
            .layout
            .as_ref()
            .and_then(|layout| layout.layout().total_length())
            .ok_or(JournalStateError::InvalidHttpRangeIdentity)?;
        if state.http_range_identity.is_some()
            || state.http_strong_validator.is_some()
            || total_length != identity.total_length
        {
            return Err(JournalStateError::InvalidHttpRangeIdentity);
        }
        let calculated = calculate_http_range_identity_fingerprint(
            &identity.representation_digest,
            identity.total_length,
        )
        .map_err(|_| JournalStateError::InvalidHttpRangeIdentity)?;
        if calculated != identity.identity_fingerprint {
            return Err(JournalStateError::InvalidHttpRangeIdentity);
        }
        state.http_range_identity = Some(identity);
        Ok(())
    }

    fn apply_lease_started(
        &mut self,
        record: &JournalRecord,
        _transfer_attempt_id: TransferAttemptId,
        lease_id: LeaseId,
        span: PersistedSpan,
        validator_fingerprint: JournalHash,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        self.require_span_in_layout(span)?;
        if self
            .state
            .as_ref()
            .and_then(|state| state.http_strong_validator.as_ref())
            .is_some_and(|validator| validator.validator_fingerprint != validator_fingerprint)
        {
            return Err(JournalStateError::InvalidHttpStrongValidator);
        }
        if self
            .state
            .as_ref()
            .and_then(|state| state.http_range_identity.as_ref())
            .is_some_and(|identity| identity.identity_fingerprint != validator_fingerprint)
        {
            return Err(JournalStateError::InvalidHttpRangeIdentity);
        }
        if self.seen_leases.contains(&lease_id) {
            return Err(JournalStateError::DuplicateLease);
        }
        if self.seen_leases.len() >= self.limits.max_leases {
            return Err(JournalStateError::ResourceLimit(
                JournalStateResource::Leases,
            ));
        }
        self.seen_leases.insert(lease_id);
        self.active_leases.insert(
            lease_id,
            LeaseState {
                span,
                validator_fingerprint,
            },
        );
        self.network_started = true;
        Ok(())
    }

    fn apply_piece_started(
        &mut self,
        record: &JournalRecord,
        lease_id: LeaseId,
        piece_id: PieceId,
        piece_span: PersistedSpan,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        let expected = self.expected_piece_span(piece_id)?;
        if expected != piece_span {
            return Err(JournalStateError::PieceSpanMismatch);
        }
        let lease = self
            .active_leases
            .get(&lease_id)
            .ok_or(JournalStateError::UnknownLease)?;
        if !spans_intersect(lease.span, piece_span)
            || !self.piece_leases.insert((lease_id, piece_id))
        {
            return Err(JournalStateError::PieceLeaseMismatch);
        }
        Ok(())
    }

    fn apply_piece_written(
        &mut self,
        record: &JournalRecord,
        lease_id: LeaseId,
        piece_id: PieceId,
        written_span: PersistedSpan,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        let lease = self
            .active_leases
            .get(&lease_id)
            .ok_or(JournalStateError::UnknownLease)?;
        let piece_span = self.expected_piece_span(piece_id)?;
        if !self.piece_leases.contains(&(lease_id, piece_id))
            || !span_contains(lease.span, written_span)
            || !span_contains(piece_span, written_span)
        {
            return Err(JournalStateError::PieceLeaseMismatch);
        }
        Ok(())
    }

    fn apply_lease_committed(
        &mut self,
        record: &JournalRecord,
        lease_id: LeaseId,
        span: PersistedSpan,
        validator_fingerprint: JournalHash,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        let lease = self
            .active_leases
            .remove(&lease_id)
            .ok_or(JournalStateError::UnknownLease)?;
        if lease.span != span || lease.validator_fingerprint != validator_fingerprint {
            return Err(JournalStateError::LeaseMismatch);
        }
        self.committed_leases.insert(lease_id, lease);
        Ok(())
    }

    fn apply_lease_aborted(
        &mut self,
        record: &JournalRecord,
        lease_id: LeaseId,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if self.active_leases.remove(&lease_id).is_none() {
            return Err(JournalStateError::UnknownLease);
        }
        self.piece_leases.retain(|(lease, _)| *lease != lease_id);
        Ok(())
    }

    fn apply_piece_verified(
        &mut self,
        record: &JournalRecord,
        piece_id: PieceId,
        piece_span: PersistedSpan,
        contributors_hash: JournalHash,
        digest: JournalDigest,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if self.expected_piece_span(piece_id)? != piece_span {
            return Err(JournalStateError::VerificationMismatch);
        }
        let contributors = self.contributors_for_piece(piece_id, piece_span)?;
        if calculate_contributors_hash(&contributors)? != contributors_hash {
            return Err(JournalStateError::VerificationMismatch);
        }
        let validator_set_fingerprint = calculate_validator_set_fingerprint(&contributors)?;
        self.verified_pieces.insert(
            piece_id,
            VerifiedPiece {
                span: piece_span,
                contributors_hash,
                validator_set_fingerprint,
                digest,
            },
        );
        Ok(())
    }

    fn apply_piece_failed(
        &mut self,
        record: &JournalRecord,
        lease_id: Option<LeaseId>,
        piece_id: PieceId,
        piece_span: PersistedSpan,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if self.expected_piece_span(piece_id)? != piece_span
            || lease_id.is_some_and(|lease| !self.seen_leases.contains(&lease))
        {
            return Err(JournalStateError::VerificationMismatch);
        }
        self.verified_pieces.remove(&piece_id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_piece_durable(
        &mut self,
        record: &JournalRecord,
        piece_id: PieceId,
        piece_span: PersistedSpan,
        contributors_hash: JournalHash,
        validator_set_fingerprint: JournalHash,
        digest: Option<JournalDigest>,
        data_barrier: DataBarrierKind,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if self.expected_piece_span(piece_id)? != piece_span {
            return Err(JournalStateError::PieceSpanMismatch);
        }
        let durability = self
            .state
            .as_ref()
            .ok_or(JournalStateError::TaskCreatedMissing)?
            .durability;
        match data_barrier {
            DataBarrierKind::RecoveryReadback => {
                let state = self
                    .state
                    .as_ref()
                    .ok_or(JournalStateError::TaskCreatedMissing)?;
                let Some(digest_value) = digest.as_ref() else {
                    return Err(JournalStateError::DurabilityBarrierMismatch);
                };
                let Some(previous_root_binding_hash) = state.rebind_source_root_binding_hash else {
                    return Err(JournalStateError::DurabilityBarrierMismatch);
                };
                let new_root_binding_hash = state
                    .layout
                    .as_ref()
                    .ok_or(JournalStateError::LayoutMissing)?
                    .root_binding_hash;
                if self.network_started
                    || contributors_hash != calculate_contributors_hash(&[])?
                    || validator_set_fingerprint
                        != calculate_rebind_validator_set_fingerprint(
                            previous_root_binding_hash,
                            new_root_binding_hash,
                            digest_value,
                        )
                {
                    return Err(JournalStateError::DurabilityBarrierMismatch);
                }
            }
            DataBarrierKind::BalancedGroup if durability == DurabilityMode::Balanced => {}
            DataBarrierKind::StrictPiece if durability == DurabilityMode::Strict => {}
            DataBarrierKind::FastFinalization if durability == DurabilityMode::Fast => {}
            _ => return Err(JournalStateError::DurabilityBarrierMismatch),
        }
        if data_barrier != DataBarrierKind::RecoveryReadback {
            let verified = self
                .verified_pieces
                .get(&piece_id)
                .ok_or(JournalStateError::VerificationMismatch)?;
            if verified.span != piece_span
                || verified.contributors_hash != contributors_hash
                || verified.validator_set_fingerprint != validator_set_fingerprint
                || digest
                    .as_ref()
                    .is_some_and(|value| value != &verified.digest)
            {
                return Err(JournalStateError::VerificationMismatch);
            }
        }
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if state.durable_pieces.contains_key(&piece_id) {
            return Err(JournalStateError::DuplicateDurablePiece);
        }
        if state.durable_pieces.len() >= self.limits.max_durable_pieces {
            return Err(JournalStateError::ResourceLimit(
                JournalStateResource::DurablePieces,
            ));
        }
        state.durable_pieces.insert(
            piece_id,
            RecoveredDurablePiece {
                piece_id,
                piece_span,
                validator_set_fingerprint,
                digest,
                origin: DurablePieceOrigin::Live {
                    contributors_hash,
                    data_barrier,
                },
            },
        );
        Ok(())
    }

    fn contributors_for_piece(
        &self,
        piece_id: PieceId,
        piece_span: PersistedSpan,
    ) -> Result<Vec<JournalContributor>, JournalStateError> {
        let mut contributors = Vec::new();
        contributors
            .try_reserve(self.committed_leases.len())
            .map_err(|_| JournalStateError::AllocationFailed)?;
        for (lease_id, lease) in &self.committed_leases {
            if self.piece_leases.contains(&(*lease_id, piece_id))
                && spans_intersect(lease.span, piece_span)
            {
                contributors.push(JournalContributor::new(
                    *lease_id,
                    lease.span,
                    lease.validator_fingerprint,
                ));
            }
        }
        if contributors.is_empty() {
            return Err(JournalStateError::VerificationMismatch);
        }
        Ok(contributors)
    }

    fn apply_retry(
        &mut self,
        record: &JournalRecord,
        retry: RecoveredRetryState,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        let key = (retry.scope.number(), retry.scope_id.get());
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if !state.retry_states.contains_key(&key)
            && state.retry_states.len() >= self.limits.max_retry_states
        {
            return Err(JournalStateError::ResourceLimit(
                JournalStateResource::RetryStates,
            ));
        }
        state.retry_states.insert(key, retry);
        Ok(())
    }

    fn apply_paused(
        &mut self,
        record: &JournalRecord,
        reason: TaskPauseReason,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if !self.active_leases.is_empty() {
            return Err(JournalStateError::GenerationNotDrained);
        }
        self.state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?
            .paused = Some(reason);
        Ok(())
    }

    fn apply_complete(
        &mut self,
        record: &JournalRecord,
        layout_hash: JournalHash,
        final_length: u64,
        final_digest: Option<JournalDigest>,
        completed_at_unix_ms: u64,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        if !self.active_leases.is_empty()
            || self
                .state
                .as_ref()
                .is_some_and(|state| state.finalizations.values().any(|value| !value.done))
        {
            return Err(JournalStateError::FinalizationMismatch);
        }
        let layout = self.require_layout()?;
        if layout.layout_hash != layout_hash
            || layout
                .layout
                .total_length()
                .is_some_and(|length| length != final_length)
        {
            return Err(JournalStateError::FinalizationMismatch);
        }
        self.apply_terminal(
            record,
            RecoveredTerminal::Complete {
                layout_hash,
                final_length,
                final_digest,
                completed_at_unix_ms,
            },
        )
    }

    fn apply_terminal(
        &mut self,
        record: &JournalRecord,
        terminal: RecoveredTerminal,
    ) -> Result<(), JournalStateError> {
        self.require_current_generation(record)?;
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if state.terminal.is_some() {
            return Err(JournalStateError::DuplicateTerminal);
        }
        state.terminal = Some(terminal);
        Ok(())
    }

    fn apply_clean_shutdown(
        &mut self,
        record: &JournalRecord,
        checkpoint_sequence: u64,
        shutdown_at_unix_ms: u64,
    ) -> Result<(), JournalStateError> {
        if self.checkpoint_state {
            return Err(JournalStateError::CheckpointRecordForbidden);
        }
        self.require_current_generation(record)?;
        if checkpoint_sequence >= record.sequence {
            return Err(JournalStateError::InvalidCleanShutdown);
        }
        self.state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?
            .clean_shutdown = Some(RecoveredCleanShutdown {
            checkpoint_sequence,
            shutdown_at_unix_ms,
            record_sequence: record.sequence,
        });
        Ok(())
    }

    fn apply_finalize_intent(
        &mut self,
        record: &JournalRecord,
        finalization: RecoveredFinalization,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        let layout = self.require_layout()?;
        if layout.layout_hash != finalization.layout_hash
            || layout.root_binding_hash != finalization.root_binding_hash
        {
            return Err(JournalStateError::FinalizationMismatch);
        }
        let entry = layout
            .layout
            .files()
            .get(finalization.file_id.get() as usize)
            .ok_or(JournalStateError::FinalizationMismatch)?;
        if !entry.selected()
            || entry.length() != finalization.final_length
            || entry.safe_path().canonical_string() != finalization.final_relative_path.as_str()
            || entry.identity().map(FileIdentity::bytes) != Some(&finalization.file_identity)
        {
            return Err(JournalStateError::FinalizationMismatch);
        }
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if state.finalizations.values().any(|value| !value.done)
            || state.finalizations.contains_key(&finalization.file_id)
            || state
                .finalizations
                .last_key_value()
                .is_some_and(|(file, _)| *file >= finalization.file_id)
        {
            return Err(JournalStateError::FinalizationMismatch);
        }
        if state.finalizations.len() >= self.limits.max_finalizations {
            return Err(JournalStateError::ResourceLimit(
                JournalStateResource::Finalizations,
            ));
        }
        state
            .finalizations
            .insert(finalization.file_id, finalization);
        Ok(())
    }

    fn apply_finalize_done(
        &mut self,
        record: &JournalRecord,
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
        file_id: FileId,
        final_relative_path: crate::JournalRelativePath,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        let finalization = state
            .finalizations
            .get_mut(&file_id)
            .ok_or(JournalStateError::FinalizationMismatch)?;
        if finalization.done
            || finalization.layout_hash != layout_hash
            || finalization.root_binding_hash != root_binding_hash
            || finalization.final_relative_path != final_relative_path
        {
            return Err(JournalStateError::FinalizationMismatch);
        }
        finalization.done = true;
        Ok(())
    }

    fn apply_piece_state_chunk(
        &mut self,
        record: &JournalRecord,
        input: PieceStateInput,
    ) -> Result<(), JournalStateError> {
        if !self.checkpoint_state {
            return Err(JournalStateError::PieceStateOutsideCheckpoint);
        }
        self.require_ready_nonterminal(record)?;
        let (expected_layout_hash, expected_root_binding_hash) = {
            let layout = self.require_layout()?;
            (layout.layout_hash, layout.root_binding_hash)
        };
        if expected_layout_hash != input.layout_hash
            || expected_root_binding_hash != input.root_binding_hash
        {
            return Err(JournalStateError::PieceStateChunkMismatch);
        }
        if input.chunk_index == 0 {
            if self.seen_piece_state || self.pending_piece_state.is_some() {
                return Err(JournalStateError::DuplicatePieceState);
            }
            self.seen_piece_state = true;
            self.state
                .as_mut()
                .ok_or(JournalStateError::TaskCreatedMissing)?
                .durable_pieces
                .clear();
        } else {
            let pending = self
                .pending_piece_state
                .as_ref()
                .ok_or(JournalStateError::PieceStateChunkMismatch)?;
            if pending.layout_hash != input.layout_hash
                || pending.root_binding_hash != input.root_binding_hash
                || pending.chunk_count != input.chunk_count
                || pending.next_chunk != input.chunk_index
                || input.first_piece_id.get() < pending.previous_covered_end
            {
                return Err(JournalStateError::PieceStateChunkMismatch);
            }
        }

        let durable_count = input
            .durable_bitmap
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum::<usize>();
        let current_durable = self
            .state
            .as_ref()
            .ok_or(JournalStateError::TaskCreatedMissing)?
            .durable_pieces
            .len();
        if current_durable
            .checked_add(durable_count)
            .is_none_or(|count| count > self.limits.max_durable_pieces)
        {
            return Err(JournalStateError::ResourceLimit(
                JournalStateResource::DurablePieces,
            ));
        }
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(durable_count)
            .map_err(|_| JournalStateError::AllocationFailed)?;
        for run in &input.evidence_runs {
            let run_start = u64::from(run.first_piece_delta());
            let digest_len = usize::from(run.digest_value_len());
            for index in 0..run.piece_count() {
                let delta = run_start + u64::from(index);
                let piece_value = input
                    .first_piece_id
                    .get()
                    .checked_add(delta)
                    .ok_or(JournalStateError::PieceStateChunkMismatch)?;
                let piece_id = PieceId::new(piece_value);
                let piece_span = expected_piece_span(&self.require_layout()?.layout, piece_id)?;
                let digest = match run.digest_algorithm() {
                    Some(algorithm) => {
                        let start = usize::try_from(index)
                            .ok()
                            .and_then(|value| value.checked_mul(digest_len))
                            .ok_or(JournalStateError::PieceStateChunkMismatch)?;
                        let end = start
                            .checked_add(digest_len)
                            .ok_or(JournalStateError::PieceStateChunkMismatch)?;
                        Some(
                            JournalDigest::new(
                                algorithm,
                                run.digest_values()
                                    .get(start..end)
                                    .ok_or(JournalStateError::PieceStateChunkMismatch)?
                                    .to_vec(),
                            )
                            .map_err(JournalStateError::Payload)?,
                        )
                    }
                    None => None,
                };
                decoded.push(RecoveredDurablePiece {
                    piece_id,
                    piece_span,
                    validator_set_fingerprint: run.validator_set_fingerprint(),
                    digest,
                    origin: DurablePieceOrigin::Checkpoint,
                });
            }
        }
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        for piece in decoded {
            if state.durable_pieces.insert(piece.piece_id, piece).is_some() {
                return Err(JournalStateError::DuplicateDurablePiece);
            }
        }
        let previous_covered_end = input
            .first_piece_id
            .get()
            .checked_add(u64::from(input.covered_piece_count))
            .ok_or(JournalStateError::PieceStateChunkMismatch)?;
        if input.chunk_index + 1 == input.chunk_count {
            self.pending_piece_state = None;
        } else {
            self.pending_piece_state = Some(PendingPieceState {
                layout_hash: input.layout_hash,
                root_binding_hash: input.root_binding_hash,
                chunk_count: input.chunk_count,
                next_chunk: input.chunk_index + 1,
                previous_covered_end,
            });
        }
        Ok(())
    }

    fn require_current_generation(&self, record: &JournalRecord) -> Result<(), JournalStateError> {
        let state = self
            .state
            .as_ref()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if record.generation != state.generation {
            return Err(JournalStateError::GenerationMismatch {
                expected: state.generation,
                actual: record.generation,
            });
        }
        Ok(())
    }

    fn require_ready_nonterminal(&self, record: &JournalRecord) -> Result<(), JournalStateError> {
        self.require_current_generation(record)?;
        let state = self
            .state
            .as_ref()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if state.current_options.is_none() {
            return Err(JournalStateError::CurrentSnapshotMissing);
        }
        if state.terminal.is_some() {
            return Err(JournalStateError::RecordAfterTerminal);
        }
        Ok(())
    }

    fn require_layout(&self) -> Result<&RecoveredLayout, JournalStateError> {
        self.state
            .as_ref()
            .and_then(|state| state.layout.as_ref())
            .ok_or(JournalStateError::LayoutMissing)
    }

    fn require_span_in_layout(&self, span: PersistedSpan) -> Result<(), JournalStateError> {
        let total = self
            .require_layout()?
            .layout
            .total_length()
            .ok_or(JournalStateError::SpanOutsideLayout)?;
        if span
            .offset()
            .checked_add(span.len())
            .is_none_or(|end| end > total)
        {
            return Err(JournalStateError::SpanOutsideLayout);
        }
        Ok(())
    }

    fn expected_piece_span(&self, piece_id: PieceId) -> Result<PersistedSpan, JournalStateError> {
        expected_piece_span(&self.require_layout()?.layout, piece_id)
    }
}

struct PendingLayoutInput {
    layout_hash: JournalHash,
    root_binding_hash: JournalHash,
    root_display: PlatformPath,
    root_identity: Box<[u8]>,
    total_length: Option<u64>,
    piece_length: u64,
    total_file_count: u32,
    chunk_count: u32,
    inline_files: Box<[JournalFileLayoutEntry]>,
}

struct PieceStateInput {
    layout_hash: JournalHash,
    root_binding_hash: JournalHash,
    chunk_index: u32,
    chunk_count: u32,
    first_piece_id: PieceId,
    covered_piece_count: u32,
    durable_bitmap: Box<[u8]>,
    evidence_runs: Box<[crate::DurableEvidenceRun]>,
}

struct AssembledLayout {
    layout: RecoveredLayout,
    previous_layout: Option<RecoveredLayout>,
    previous_durable_pieces: BTreeMap<PieceId, RecoveredDurablePiece>,
}

fn assemble_layout(
    task: TaskId,
    pending: PendingLayout,
) -> Result<AssembledLayout, JournalStateError> {
    let root_identity =
        RootIdentity::new(pending.root_identity).map_err(JournalStateError::RootBinding)?;
    let mut identities = Vec::new();
    identities
        .try_reserve(pending.files.len())
        .map_err(|_| JournalStateError::AllocationFailed)?;
    let platform = pending.root_display.platform();
    let mut files = Vec::new();
    files
        .try_reserve_exact(pending.files.len())
        .map_err(|_| JournalStateError::AllocationFailed)?;
    for entry in pending.files {
        let safe_path =
            SafePathBuilder::from_user_path(entry.safe_relative_path().as_str(), platform)
                .map_err(JournalStateError::LayoutPath)?;
        let identity = if entry.selected() {
            let identity = FileIdentity::new(entry.file_identity().to_vec())
                .map_err(JournalStateError::RootBinding)?;
            identities.push((entry.file_id(), identity.clone()));
            Some(identity)
        } else {
            None
        };
        files.push(FileEntry::new(
            entry.file_id(),
            safe_path,
            identity,
            entry.length(),
            entry.global_start(),
            entry.global_end(),
            entry.selected(),
        ));
    }
    let root_binding = RootBinding::new(pending.root_display, root_identity, identities)
        .map_err(JournalStateError::RootBinding)?;
    let layout = FileLayout::new(
        task,
        pending.generation,
        root_binding,
        files,
        pending.total_length,
        pending.piece_length,
    )
    .map_err(JournalStateError::Layout)?;
    if layout.layout_hash().as_bytes() != pending.layout_hash.as_bytes() {
        return Err(JournalStateError::LayoutHashMismatch);
    }
    if layout.root_binding().hash().as_bytes() != pending.root_binding_hash.as_bytes() {
        return Err(JournalStateError::RootBindingHashMismatch);
    }
    Ok(AssembledLayout {
        layout: RecoveredLayout {
            layout,
            layout_hash: pending.layout_hash,
            root_binding_hash: pending.root_binding_hash,
        },
        previous_layout: pending.previous_layout,
        previous_durable_pieces: pending.previous_durable_pieces,
    })
}

fn expected_piece_span(
    layout: &FileLayout,
    piece_id: PieceId,
) -> Result<PersistedSpan, JournalStateError> {
    let total = layout
        .total_length()
        .ok_or(JournalStateError::SpanOutsideLayout)?;
    let start = piece_id
        .get()
        .checked_mul(layout.piece_length())
        .ok_or(JournalStateError::PieceSpanMismatch)?;
    if start >= total {
        return Err(JournalStateError::PieceSpanMismatch);
    }
    let len = layout.piece_length().min(total - start);
    PersistedSpan::new(start, len).map_err(JournalStateError::Payload)
}

fn span_contains(outer: PersistedSpan, inner: PersistedSpan) -> bool {
    let outer_end = outer.offset() + outer.len();
    let inner_end = inner.offset() + inner.len();
    inner.offset() >= outer.offset() && inner_end <= outer_end
}

fn spans_intersect(left: PersistedSpan, right: PersistedSpan) -> bool {
    left.offset() < right.offset() + right.len() && right.offset() < left.offset() + left.len()
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_JOURNAL_STATE_ERROR_CODES, DurablePieceOrigin, JournalContributor, JournalStateError,
        JournalStateLimits, JournalStateStop, calculate_checkpoint_state_hash,
        calculate_contributors_hash, calculate_rebind_validator_set_fingerprint,
        calculate_validator_set_fingerprint, recover_journal_state,
    };
    use crate::{
        CheckpointId, DataBarrierKind, DurabilityMode, DurableEvidenceRun, FileEntry, FileIdentity,
        FileLayout, GenerationStartReason, JournalDigest, JournalDigestAlgorithm,
        JournalFileLayoutEntry, JournalHash, JournalPayload, JournalRecord, JournalRelativePath,
        OptionsSnapshotScope, PathPlatform, PayloadCodecError, PersistedSpan, PlatformPath,
        RootBinding, RootIdentity, SafePathBuilder, SanitizedOptionMap, TaskPauseReason,
        calculate_http_range_identity_fingerprint, calculate_http_strong_validator_fingerprint,
    };
    use ariax_config::{SecurityClass, builtin_registry};
    use ariax_core::{
        FileId, Generation, LeaseId, OptionPatchId, PieceId, TaskId, TransferAttemptId,
    };
    use std::collections::BTreeSet;

    const TASK_NUMBER: u64 = 7;

    struct LayoutFixture {
        committed: JournalPayload,
        continuation: Option<JournalPayload>,
        layout_hash: JournalHash,
        root_binding_hash: JournalHash,
    }

    fn task() -> TaskId {
        TaskId::new(TASK_NUMBER).expect("task")
    }

    fn allow_all(_: &str) -> bool {
        true
    }

    fn hash(value: u8) -> JournalHash {
        JournalHash::new([value; 32]).expect("hash")
    }

    fn span(offset: u64, len: u64) -> PersistedSpan {
        PersistedSpan::new(offset, len).expect("span")
    }

    fn digest(value: u8) -> JournalDigest {
        JournalDigest::new(JournalDigestAlgorithm::Sha256, vec![value; 32]).expect("digest")
    }

    fn options(entries: &[(&str, &str)]) -> SanitizedOptionMap {
        SanitizedOptionMap::new(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
        )
        .expect("options")
    }

    fn current_options(entries: &[(&str, &str)]) -> JournalPayload {
        let options = options(entries);
        JournalPayload::OptionsSnapshot {
            scope: OptionsSnapshotScope::CurrentGeneration,
            patch_id: None,
            snapshot_hash: options.snapshot_hash(),
            options,
        }
    }

    fn record(sequence: u64, generation: u64, payload: JournalPayload) -> JournalRecord {
        JournalRecord {
            record_type: payload.record_type(),
            generation: Generation::new(generation),
            sequence,
            payload: payload.encode().expect("encode payload"),
        }
    }

    fn task_created() -> JournalPayload {
        JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 1,
        }
    }

    fn layout_fixture(generation: Generation, chunked: bool) -> LayoutFixture {
        layout_fixture_with_identity(generation, chunked, b"root-id", b"file-0")
    }

    fn layout_fixture_with_identity(
        generation: Generation,
        chunked: bool,
        root_identity_bytes: &[u8],
        file_zero_identity_bytes: &[u8],
    ) -> LayoutFixture {
        let root_display =
            PlatformPath::from_native_bytes(PathPlatform::Unix, b"/srv/downloads").expect("path");
        let root_identity = RootIdentity::new(root_identity_bytes.to_vec()).expect("root identity");
        let file_zero_identity =
            FileIdentity::new(file_zero_identity_bytes.to_vec()).expect("identity");
        let file_one_identity = FileIdentity::new(b"file-1".to_vec()).expect("identity");
        let root_binding = RootBinding::new(
            root_display.clone(),
            root_identity.clone(),
            if chunked {
                vec![
                    (FileId::new(0), file_zero_identity.clone()),
                    (FileId::new(1), file_one_identity.clone()),
                ]
            } else {
                vec![(FileId::new(0), file_zero_identity.clone())]
            },
        )
        .expect("binding");
        let first_path =
            SafePathBuilder::from_user_path("first.bin", PathPlatform::Unix).expect("safe path");
        let mut files = vec![FileEntry::new(
            FileId::new(0),
            first_path,
            Some(file_zero_identity.clone()),
            2048,
            0,
            2048,
            true,
        )];
        if chunked {
            files.push(FileEntry::new(
                FileId::new(1),
                SafePathBuilder::from_user_path("second.bin", PathPlatform::Unix)
                    .expect("safe path"),
                Some(file_one_identity.clone()),
                2048,
                2048,
                4096,
                true,
            ));
        }
        let total_length = if chunked { 4096 } else { 2048 };
        let layout = FileLayout::new(
            task(),
            generation,
            root_binding,
            files,
            Some(total_length),
            1024,
        )
        .expect("layout");
        let layout_hash = JournalHash::new(*layout.layout_hash().as_bytes()).expect("layout hash");
        let root_binding_hash =
            JournalHash::new(*layout.root_binding().hash().as_bytes()).expect("binding hash");
        let first = JournalFileLayoutEntry::new(
            FileId::new(0),
            0,
            2048,
            2048,
            true,
            JournalRelativePath::new("first.bin").expect("path"),
            file_zero_identity.bytes().to_vec(),
        )
        .expect("entry");
        let committed = JournalPayload::LayoutCommitted {
            layout_hash,
            root_binding_hash,
            root_display,
            root_identity: root_identity.bytes().to_vec().into_boxed_slice(),
            total_length: Some(total_length),
            piece_length: 1024,
            total_file_count: if chunked { 2 } else { 1 },
            chunk_count: if chunked { 2 } else { 1 },
            inline_files: vec![first].into_boxed_slice(),
        };
        let continuation = chunked.then(|| JournalPayload::LayoutChunk {
            layout_hash,
            root_binding_hash,
            chunk_index: 1,
            chunk_count: 2,
            files: vec![
                JournalFileLayoutEntry::new(
                    FileId::new(1),
                    2048,
                    4096,
                    2048,
                    true,
                    JournalRelativePath::new("second.bin").expect("path"),
                    file_one_identity.bytes().to_vec(),
                )
                .expect("entry"),
            ]
            .into_boxed_slice(),
        });
        LayoutFixture {
            committed,
            continuation,
            layout_hash,
            root_binding_hash,
        }
    }

    fn base_records(layout: LayoutFixture) -> Vec<JournalRecord> {
        vec![
            record(1, 0, task_created()),
            record(2, 0, current_options(&[("piece-length", "1M")])),
            record(3, 0, layout.committed),
        ]
    }

    #[test]
    fn live_recovery_promotes_only_verified_durable_pieces() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let mut records = base_records(fixture);
        let lease = LeaseId::new(11).expect("lease");
        let validator = hash(42);
        let contributor = JournalContributor::new(lease, span(0, 1024), validator);
        let contributors = calculate_contributors_hash(&[contributor]).expect("contributors");
        let validator_set =
            calculate_validator_set_fingerprint(&[contributor]).expect("validator set");
        let piece_digest = digest(41);
        records.extend([
            record(
                4,
                0,
                JournalPayload::LeaseStarted {
                    transfer_attempt_id: TransferAttemptId::new(10).expect("attempt"),
                    lease_id: lease,
                    span: span(0, 1024),
                    validator_fingerprint: validator,
                },
            ),
            record(
                5,
                0,
                JournalPayload::PieceStarted {
                    lease_id: lease,
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                },
            ),
            record(
                6,
                0,
                JournalPayload::PieceWritten {
                    lease_id: lease,
                    piece_id: PieceId::new(0),
                    written_span: span(0, 1024),
                },
            ),
            record(
                7,
                0,
                JournalPayload::LeaseCommitted {
                    lease_id: lease,
                    span: span(0, 1024),
                    validator_fingerprint: validator,
                    response_digest: None,
                },
            ),
            record(
                8,
                0,
                JournalPayload::PieceVerified {
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                    contributors_hash: contributors,
                    digest: piece_digest.clone(),
                },
            ),
            record(
                9,
                0,
                JournalPayload::PieceDurable {
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                    contributors_hash: contributors,
                    validator_set_fingerprint: validator_set,
                    digest: Some(piece_digest),
                    data_barrier: DataBarrierKind::BalancedGroup,
                },
            ),
            record(
                10,
                0,
                JournalPayload::LeaseStarted {
                    transfer_attempt_id: TransferAttemptId::new(12).expect("attempt"),
                    lease_id: LeaseId::new(13).expect("lease"),
                    span: span(1024, 1024),
                    validator_fingerprint: hash(44),
                },
            ),
        ]);

        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        assert_eq!(replay.accepted_records, 10);
        let state = replay.state.expect("trusted state");
        assert_eq!(state.durable_pieces().len(), 1);
        assert!(matches!(
            state
                .durable_pieces()
                .get(&PieceId::new(0))
                .expect("piece")
                .origin(),
            DurablePieceOrigin::Live {
                data_barrier: DataBarrierKind::BalancedGroup,
                ..
            }
        ));
        assert!(!state.durable_pieces().contains_key(&PieceId::new(1)));
    }

    #[test]
    fn crash_with_pending_overlap_candidates_replays_no_durable_piece() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let mut records = base_records(fixture);
        let validator = hash(91);
        let original = LeaseId::new(21).expect("original lease");
        let duplicate = LeaseId::new(22).expect("duplicate lease");
        records.extend([
            record(
                4,
                0,
                JournalPayload::LeaseStarted {
                    transfer_attempt_id: TransferAttemptId::new(20).expect("attempt"),
                    lease_id: original,
                    span: span(0, 1024),
                    validator_fingerprint: validator,
                },
            ),
            record(
                5,
                0,
                JournalPayload::PieceStarted {
                    lease_id: original,
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                },
            ),
            record(
                6,
                0,
                JournalPayload::PieceWritten {
                    lease_id: original,
                    piece_id: PieceId::new(0),
                    written_span: span(0, 1024),
                },
            ),
            record(
                7,
                0,
                JournalPayload::LeaseStarted {
                    transfer_attempt_id: TransferAttemptId::new(21).expect("attempt"),
                    lease_id: duplicate,
                    span: span(0, 1024),
                    validator_fingerprint: validator,
                },
            ),
            record(
                8,
                0,
                JournalPayload::PieceStarted {
                    lease_id: duplicate,
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                },
            ),
            record(
                9,
                0,
                JournalPayload::PieceWritten {
                    lease_id: duplicate,
                    piece_id: PieceId::new(0),
                    written_span: span(0, 1024),
                },
            ),
        ]);

        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        let state = replay.state.expect("safe crash prefix");
        assert!(
            state.durable_pieces().is_empty(),
            "unsettled overlap members cannot publish a durable piece after restart"
        );
    }

    #[test]
    fn strong_http_validator_replays_before_network_and_binds_lease_fingerprints() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let validator_fingerprint = calculate_http_strong_validator_fingerprint(b"\"v1\"", 2048)
            .expect("validator fingerprint");
        let validator = JournalPayload::HttpStrongValidator {
            resource_fingerprint: hash(70),
            validator_fingerprint,
            total_length: 2048,
            etag: b"\"v1\"".to_vec().into_boxed_slice(),
        };
        let mut records = base_records(fixture);
        records.push(record(4, 0, validator.clone()));
        records.push(record(
            5,
            0,
            JournalPayload::LeaseStarted {
                transfer_attempt_id: TransferAttemptId::new(10).expect("attempt"),
                lease_id: LeaseId::new(11).expect("lease"),
                span: span(0, 1024),
                validator_fingerprint,
            },
        ));
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        let state = replay.state.expect("state");
        let recovered = state.http_strong_validator().expect("validator");
        assert_eq!(recovered.resource_fingerprint(), hash(70));
        assert_eq!(recovered.validator_fingerprint(), validator_fingerprint);
        assert_eq!(recovered.total_length(), 2048);
        assert_eq!(recovered.etag(), b"\"v1\"");

        let mut mismatched = records.clone();
        mismatched[4] = record(
            5,
            0,
            JournalPayload::LeaseStarted {
                transfer_attempt_id: TransferAttemptId::new(10).expect("attempt"),
                lease_id: LeaseId::new(11).expect("lease"),
                span: span(0, 1024),
                validator_fingerprint: hash(71),
            },
        );
        let replay = recover_journal_state(&mismatched, task(), &allow_all, Default::default());
        assert_eq!(replay.accepted_records, 4);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::InvalidHttpStrongValidator,
                ..
            }
        ));

        let mut duplicate = records;
        duplicate.push(record(6, 0, validator));
        let replay = recover_journal_state(&duplicate, task(), &allow_all, Default::default());
        assert_eq!(replay.accepted_records, 5);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::InvalidHttpStrongValidator,
                ..
            }
        ));
    }

    #[test]
    fn digest_only_range_identity_replays_before_network_and_rejects_mismatch() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let representation_digest = digest(73);
        let identity_fingerprint =
            calculate_http_range_identity_fingerprint(&representation_digest, 2048)
                .expect("identity fingerprint");
        let identity = JournalPayload::HttpRangeIdentity {
            identity_fingerprint,
            total_length: 2048,
            representation_digest: representation_digest.clone(),
        };
        let mut records = base_records(fixture);
        records.push(record(4, 0, identity.clone()));
        records.push(record(
            5,
            0,
            JournalPayload::LeaseStarted {
                transfer_attempt_id: TransferAttemptId::new(10).expect("attempt"),
                lease_id: LeaseId::new(11).expect("lease"),
                span: span(0, 1024),
                validator_fingerprint: identity_fingerprint,
            },
        ));
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        let state = replay.state.expect("state");
        let recovered = state.http_range_identity().expect("range identity");
        assert_eq!(recovered.identity_fingerprint(), identity_fingerprint);
        assert_eq!(recovered.total_length(), 2048);
        assert_eq!(recovered.representation_digest(), &representation_digest);

        let mut mismatched = base_records(layout_fixture(Generation::INITIAL, false));
        let valid_identity = JournalPayload::HttpRangeIdentity {
            identity_fingerprint,
            total_length: 2048,
            representation_digest,
        };
        let mut invalid_payload = valid_identity.encode().expect("encode identity").into_vec();
        invalid_payload[..32].copy_from_slice(hash(74).as_bytes());
        mismatched.push(JournalRecord {
            record_type: valid_identity.record_type(),
            generation: Generation::INITIAL,
            sequence: 4,
            payload: invalid_payload.into_boxed_slice(),
        });
        let replay = recover_journal_state(&mismatched, task(), &allow_all, Default::default());
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::Payload(PayloadCodecError::InvalidHttpRangeIdentity),
                ..
            }
        ));

        let mut duplicate = records;
        duplicate.push(record(6, 0, identity));
        let replay = recover_journal_state(&duplicate, task(), &allow_all, Default::default());
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::InvalidHttpRangeIdentity,
                ..
            }
        ));
    }

    #[test]
    fn generation_advance_requires_the_exact_staged_snapshot() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let mut records = base_records(fixture);
        let staged = options(&[("piece-length", "2M")]);
        let staged_hash = staged.snapshot_hash();
        let patch = OptionPatchId::new(9).expect("patch");
        records.push(record(
            4,
            0,
            JournalPayload::OptionsSnapshot {
                scope: OptionsSnapshotScope::NextAdmission,
                patch_id: Some(patch),
                snapshot_hash: staged_hash,
                options: staged,
            },
        ));
        records.push(record(
            5,
            1,
            JournalPayload::GenerationStarted {
                previous_generation: Generation::INITIAL,
                reason: GenerationStartReason::OptionPatch,
                next_snapshot_hash: staged_hash,
                patch_id: Some(patch),
            },
        ));
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        let state = replay.state.expect("state");
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        assert_eq!(state.generation(), Generation::new(1));
        assert_eq!(
            state.current_options().expect("current").snapshot_hash(),
            staged_hash
        );
        assert!(state.pending_options().is_none());

        let mut mismatched = records;
        mismatched[4] = record(
            5,
            1,
            JournalPayload::GenerationStarted {
                previous_generation: Generation::INITIAL,
                reason: GenerationStartReason::OptionPatch,
                next_snapshot_hash: hash(99),
                patch_id: Some(patch),
            },
        );
        let replay = recover_journal_state(&mismatched, task(), &allow_all, Default::default());
        assert_eq!(replay.accepted_records, 4);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::StagedSnapshotMismatch,
                ..
            }
        ));
        let state = replay.state.expect("valid prefix");
        assert_eq!(state.generation(), Generation::INITIAL);
        assert!(state.pending_options().is_some());
    }

    #[test]
    fn different_identity_rebind_requires_digest_bound_lease_free_evidence() {
        let old =
            layout_fixture_with_identity(Generation::INITIAL, false, b"old-root", b"old-file");
        let old_root_binding_hash = old.root_binding_hash;
        let new = layout_fixture_with_identity(Generation::new(1), false, b"new-root", b"new-file");
        let new_root_binding_hash = new.root_binding_hash;
        assert_eq!(old.layout_hash, new.layout_hash);
        assert_ne!(old_root_binding_hash, new_root_binding_hash);

        let current = options(&[("piece-length", "1M")]);
        let current_hash = current.snapshot_hash();
        let staged = options(&[("piece-length", "1M")]);
        let staged_hash = staged.snapshot_hash();
        let piece_digest = digest(70);
        let empty_contributors = calculate_contributors_hash(&[]).expect("empty contributors");
        let rebind_validator = calculate_rebind_validator_set_fingerprint(
            old_root_binding_hash,
            new_root_binding_hash,
            &piece_digest,
        );
        let mut records = vec![
            record(1, 0, task_created()),
            record(
                2,
                0,
                JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::CurrentGeneration,
                    patch_id: None,
                    snapshot_hash: current_hash,
                    options: current,
                },
            ),
            record(3, 0, old.committed),
            record(
                4,
                0,
                JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::NextAdmission,
                    patch_id: None,
                    snapshot_hash: staged_hash,
                    options: staged,
                },
            ),
            record(
                5,
                1,
                JournalPayload::GenerationStarted {
                    previous_generation: Generation::INITIAL,
                    reason: GenerationStartReason::RootRebind,
                    next_snapshot_hash: staged_hash,
                    patch_id: None,
                },
            ),
            record(6, 1, new.committed),
            record(
                7,
                1,
                JournalPayload::PieceDurable {
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                    contributors_hash: empty_contributors,
                    validator_set_fingerprint: rebind_validator,
                    digest: Some(piece_digest.clone()),
                    data_barrier: DataBarrierKind::RecoveryReadback,
                },
            ),
        ];
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        let state = replay.state.expect("rebound state");
        assert_eq!(
            state.rebind_source_root_binding_hash(),
            Some(old_root_binding_hash)
        );
        assert_eq!(state.durable_pieces().len(), 1);

        records[6] = record(
            7,
            1,
            JournalPayload::PieceDurable {
                piece_id: PieceId::new(0),
                piece_span: span(0, 1024),
                contributors_hash: hash(99),
                validator_set_fingerprint: rebind_validator,
                digest: Some(piece_digest),
                data_barrier: DataBarrierKind::RecoveryReadback,
            },
        );
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.accepted_records, 6);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::DurabilityBarrierMismatch,
                ..
            }
        ));
    }

    #[test]
    fn registry_policy_rejects_sensitive_unsafe_and_unknown_snapshot_keys() {
        let registry = builtin_registry();
        let policy = |name: &str| {
            registry
                .find(name)
                .is_some_and(|definition| definition.security == SecurityClass::Normal)
        };
        for forbidden in ["rpc-secret", "on-download-complete", "unknown-option"] {
            let records = vec![
                record(1, 0, task_created()),
                record(2, 0, current_options(&[(forbidden, "seeded-secret")])),
            ];
            let replay = recover_journal_state(&records, task(), &policy, Default::default());
            assert_eq!(replay.accepted_records, 1, "{forbidden}");
            assert!(matches!(
                replay.stop,
                JournalStateStop::InvalidRecord {
                    error: JournalStateError::ForbiddenPersistedOption { .. },
                    ..
                }
            ));
        }

        let records = vec![
            record(1, 0, task_created()),
            record(2, 0, current_options(&[("piece-length", "1M")])),
        ];
        assert_eq!(
            recover_journal_state(&records, task(), &policy, Default::default()).stop,
            JournalStateStop::CleanEnd
        );
    }

    #[test]
    fn layout_chunks_are_immediate_exact_and_recomputed() {
        let fixture = layout_fixture(Generation::INITIAL, true);
        let continuation = fixture.continuation.clone().expect("continuation");
        let mut records = base_records(fixture);
        records.push(record(4, 0, continuation));
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        let state = replay.state.expect("state");
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        assert_eq!(state.layout().expect("layout").layout().files().len(), 2);

        let fixture = layout_fixture(Generation::INITIAL, true);
        let incomplete = base_records(fixture);
        let replay = recover_journal_state(&incomplete, task(), &allow_all, Default::default());
        assert_eq!(
            replay.stop,
            JournalStateStop::IncompleteLayout {
                expected_chunk: 1,
                chunk_count: 2,
            }
        );
        assert!(replay.state.expect("prefix").layout().is_none());

        let fixture = layout_fixture(Generation::INITIAL, true);
        let mut records = base_records(fixture);
        records.push(record(
            4,
            0,
            JournalPayload::TaskPaused {
                reason: TaskPauseReason::User,
            },
        ));
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::IncompleteLayoutSequence,
                ..
            }
        ));
    }

    #[test]
    fn checkpoint_hash_and_piece_state_are_all_or_nothing() {
        let fixture = layout_fixture(Generation::new(2), false);
        let layout_hash = fixture.layout_hash;
        let root_binding_hash = fixture.root_binding_hash;
        let state_payloads = vec![
            task_created(),
            current_options(&[("piece-length", "1M")]),
            fixture.committed,
            JournalPayload::HttpStrongValidator {
                resource_fingerprint: hash(57),
                validator_fingerprint: calculate_http_strong_validator_fingerprint(
                    b"\"checkpoint-v1\"",
                    2048,
                )
                .expect("validator fingerprint"),
                total_length: 2048,
                etag: b"\"checkpoint-v1\"".to_vec().into_boxed_slice(),
            },
            JournalPayload::PieceStateChunk {
                layout_hash,
                root_binding_hash,
                chunk_index: 0,
                chunk_count: 1,
                first_piece_id: PieceId::new(0),
                covered_piece_count: 2,
                durable_bitmap: vec![0b11].into_boxed_slice(),
                evidence_runs: vec![
                    DurableEvidenceRun::new(0, 2, hash(55), None, Vec::new()).expect("evidence"),
                ]
                .into_boxed_slice(),
            },
            JournalPayload::TaskPaused {
                reason: TaskPauseReason::RecoveryHold,
            },
        ];
        let state_records = state_payloads
            .into_iter()
            .enumerate()
            .map(|(index, payload)| record(index as u64 + 2, 2, payload))
            .collect::<Vec<_>>();
        let state_record_count = state_records.len() as u32;
        let state_hash = calculate_checkpoint_state_hash(&state_records).expect("state hash");
        let checkpoint_id = CheckpointId::new([7; 16]).expect("checkpoint");
        let mut records = Vec::new();
        records.push(record(
            1,
            2,
            JournalPayload::CheckpointStart {
                checkpoint_id,
                source_last_sequence: 91,
                source_segment_hash: hash(56),
                state_record_count,
                created_at_unix_ms: 100,
            },
        ));
        records.extend(state_records);
        records.push(record(
            records.len() as u64 + 1,
            2,
            JournalPayload::CheckpointEnd {
                checkpoint_id,
                state_record_count,
                state_hash,
            },
        ));

        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        let state = replay.state.expect("checkpoint state");
        assert_eq!(state.generation(), Generation::new(2));
        assert_eq!(state.durable_pieces().len(), 2);
        assert!(
            state
                .durable_pieces()
                .values()
                .all(|piece| piece.origin() == &DurablePieceOrigin::Checkpoint)
        );
        assert_eq!(state.paused(), Some(TaskPauseReason::RecoveryHold));
        let validator = state.http_strong_validator().expect("checkpoint validator");
        assert_eq!(validator.etag(), b"\"checkpoint-v1\"");
        assert_eq!(validator.total_length(), 2048);
        assert_eq!(
            state.checkpoint().expect("checkpoint").state_hash,
            state_hash
        );

        let mut bad_hash = records.clone();
        let end_sequence = bad_hash.last().expect("end").sequence;
        bad_hash.pop();
        bad_hash.push(record(
            end_sequence,
            2,
            JournalPayload::CheckpointEnd {
                checkpoint_id,
                state_record_count,
                state_hash: hash(99),
            },
        ));
        let replay = recover_journal_state(&bad_hash, task(), &allow_all, Default::default());
        assert!(replay.state.is_none());
        assert_eq!(replay.accepted_records, 0);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::CheckpointHashMismatch,
                ..
            }
        ));

        let mut missing_end = records;
        missing_end.pop();
        let replay = recover_journal_state(&missing_end, task(), &allow_all, Default::default());
        assert!(replay.state.is_none());
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::CheckpointIncomplete,
                ..
            }
        ));
    }

    #[test]
    fn piece_state_is_forbidden_outside_a_checkpoint() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let layout_hash = fixture.layout_hash;
        let root_binding_hash = fixture.root_binding_hash;
        let mut records = base_records(fixture);
        records.push(record(
            4,
            0,
            JournalPayload::PieceStateChunk {
                layout_hash,
                root_binding_hash,
                chunk_index: 0,
                chunk_count: 1,
                first_piece_id: PieceId::new(0),
                covered_piece_count: 1,
                durable_bitmap: vec![1].into_boxed_slice(),
                evidence_runs: vec![
                    DurableEvidenceRun::new(0, 1, hash(60), None, Vec::new()).expect("evidence"),
                ]
                .into_boxed_slice(),
            },
        ));
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.accepted_records, 3);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::PieceStateOutsideCheckpoint,
                ..
            }
        ));
    }

    #[test]
    fn finalization_pairs_exact_paths_identity_and_layout() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let layout_hash = fixture.layout_hash;
        let root_binding_hash = fixture.root_binding_hash;
        let mut records = base_records(fixture);
        records.extend([
            record(
                4,
                0,
                JournalPayload::FinalizeIntent {
                    layout_hash,
                    root_binding_hash,
                    file_id: FileId::new(0),
                    temp_relative_path: JournalRelativePath::new("first.bin.ariax.tmp")
                        .expect("temp"),
                    final_relative_path: JournalRelativePath::new("first.bin").expect("final"),
                    final_length: 2048,
                    file_identity: b"file-0".to_vec().into_boxed_slice(),
                },
            ),
            record(
                5,
                0,
                JournalPayload::FinalizeDone {
                    layout_hash,
                    root_binding_hash,
                    file_id: FileId::new(0),
                    final_relative_path: JournalRelativePath::new("first.bin").expect("final"),
                },
            ),
            record(
                6,
                0,
                JournalPayload::TaskComplete {
                    layout_hash,
                    final_length: 2048,
                    final_digest: None,
                    completed_at_unix_ms: 200,
                },
            ),
        ]);
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.stop, JournalStateStop::CleanEnd);
        let state = replay.state.expect("state");
        assert!(state.finalizations()[&FileId::new(0)].done);
        assert!(state.terminal().is_some());
    }

    #[test]
    fn invalid_tail_does_not_mutate_the_last_trusted_clean_shutdown() {
        let fixture = layout_fixture(Generation::INITIAL, false);
        let mut records = base_records(fixture);
        records.extend([
            record(
                4,
                0,
                JournalPayload::CleanShutdown {
                    checkpoint_sequence: 3,
                    shutdown_at_unix_ms: 500,
                },
            ),
            record(
                5,
                1,
                JournalPayload::TaskPaused {
                    reason: TaskPauseReason::User,
                },
            ),
        ]);
        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(replay.accepted_records, 4);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::GenerationMismatch { .. },
                ..
            }
        ));
        assert_eq!(
            replay
                .state
                .expect("trusted prefix")
                .clean_shutdown()
                .expect("clean marker")
                .record_sequence,
            4
        );
    }

    #[test]
    fn incomplete_replacement_layout_exposes_neither_old_layout_nor_old_progress() {
        let old = layout_fixture(Generation::INITIAL, false);
        let old_layout_hash = old.layout_hash;
        let old_root_binding_hash = old.root_binding_hash;
        let lease = LeaseId::new(80).expect("lease");
        let validator = hash(81);
        let contributor = JournalContributor::new(lease, span(0, 1024), validator);
        let contributors = calculate_contributors_hash(&[contributor]).expect("contributors");
        let validator_set =
            calculate_validator_set_fingerprint(&[contributor]).expect("validator set");
        let piece_digest = digest(82);
        let staged = options(&[("piece-length", "1M")]);
        let staged_hash = staged.snapshot_hash();
        let replacement = layout_fixture(Generation::new(1), true);
        assert_ne!(old_layout_hash, replacement.layout_hash);
        assert_ne!(old_root_binding_hash, replacement.root_binding_hash);
        let records = vec![
            record(1, 0, task_created()),
            record(2, 0, current_options(&[("piece-length", "1M")])),
            record(3, 0, old.committed),
            record(
                4,
                0,
                JournalPayload::LeaseStarted {
                    transfer_attempt_id: TransferAttemptId::new(79).expect("attempt"),
                    lease_id: lease,
                    span: span(0, 1024),
                    validator_fingerprint: validator,
                },
            ),
            record(
                5,
                0,
                JournalPayload::PieceStarted {
                    lease_id: lease,
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                },
            ),
            record(
                6,
                0,
                JournalPayload::LeaseCommitted {
                    lease_id: lease,
                    span: span(0, 1024),
                    validator_fingerprint: validator,
                    response_digest: None,
                },
            ),
            record(
                7,
                0,
                JournalPayload::PieceVerified {
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                    contributors_hash: contributors,
                    digest: piece_digest.clone(),
                },
            ),
            record(
                8,
                0,
                JournalPayload::PieceDurable {
                    piece_id: PieceId::new(0),
                    piece_span: span(0, 1024),
                    contributors_hash: contributors,
                    validator_set_fingerprint: validator_set,
                    digest: Some(piece_digest),
                    data_barrier: DataBarrierKind::BalancedGroup,
                },
            ),
            record(
                9,
                0,
                JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::NextAdmission,
                    patch_id: None,
                    snapshot_hash: staged_hash,
                    options: staged,
                },
            ),
            record(
                10,
                1,
                JournalPayload::GenerationStarted {
                    previous_generation: Generation::INITIAL,
                    reason: GenerationStartReason::RepresentationRestart,
                    next_snapshot_hash: staged_hash,
                    patch_id: None,
                },
            ),
            record(11, 1, replacement.committed),
        ];
        let restarted =
            recover_journal_state(&records[..10], task(), &allow_all, Default::default());
        assert_eq!(restarted.stop, JournalStateStop::CleanEnd);
        let restarted = restarted.state.expect("restarted generation state");
        assert_eq!(restarted.generation(), Generation::new(1));
        assert_eq!(
            restarted
                .layout()
                .expect("old layout remains reopen authority")
                .layout()
                .generation(),
            Generation::INITIAL
        );
        assert!(
            restarted.durable_pieces().is_empty(),
            "representation restart must invalidate old publishable progress before layout replacement"
        );

        let replay = recover_journal_state(&records, task(), &allow_all, Default::default());
        assert_eq!(
            replay.stop,
            JournalStateStop::IncompleteLayout {
                expected_chunk: 1,
                chunk_count: 2,
            }
        );
        let state = replay.state.expect("safe incomplete admission");
        assert!(state.layout().is_none());
        assert!(state.durable_pieces().is_empty());
    }

    #[test]
    fn checkpoint_state_hash_ignores_sequence_but_binds_generation_and_payload() {
        let first = record(2, 3, task_created());
        let mut renumbered = first.clone();
        renumbered.sequence = 99;
        assert_eq!(
            calculate_checkpoint_state_hash(std::slice::from_ref(&first)),
            calculate_checkpoint_state_hash(&[renumbered])
        );

        let mut other_generation = first.clone();
        other_generation.generation = Generation::new(4);
        assert_ne!(
            calculate_checkpoint_state_hash(std::slice::from_ref(&first)),
            calculate_checkpoint_state_hash(&[other_generation])
        );

        let other_payload = record(
            2,
            3,
            JournalPayload::TaskCreated {
                durability: DurabilityMode::Strict,
                creator_version: 1,
            },
        );
        assert_ne!(
            calculate_checkpoint_state_hash(&[first]),
            calculate_checkpoint_state_hash(&[other_payload])
        );
    }

    #[test]
    fn state_caps_and_error_codes_are_closed() {
        let codes = ALL_JOURNAL_STATE_ERROR_CODES
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(codes.len(), ALL_JOURNAL_STATE_ERROR_CODES.len());

        let records = vec![
            record(1, 0, task_created()),
            record(2, 0, current_options(&[("piece-length", "1M")])),
        ];
        let replay = recover_journal_state(
            &records,
            task(),
            &allow_all,
            JournalStateLimits {
                max_records: 1,
                ..Default::default()
            },
        );
        assert_eq!(replay.accepted_records, 1);
        assert!(matches!(
            replay.stop,
            JournalStateStop::InvalidRecord {
                error: JournalStateError::ResourceLimit(_),
                ..
            }
        ));
    }
}
