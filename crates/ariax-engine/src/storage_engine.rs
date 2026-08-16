use ariax_core::{
    ErrorKind, FileId, Generation, LeaseId, OverlapGroupId, PieceId, TaskId, TransferAttemptId,
};
use ariax_runtime::{
    BlockingBackendEpoch, BlockingDiskCancelHandle, BlockingDiskError, BlockingDiskLane,
    BlockingDiskLaneConfig, BlockingDiskLaneStartError, BlockingDiskOperation,
    BlockingDiskOperationId, BlockingDiskSubmission, BlockingDiskSubmitErrorKind,
    BlockingFileHandle, BlockingFileRegistry, BlockingFileRegistryError, BufferLease, BufferPool,
    BufferPoolConfig, BufferState, BufferTransitionError, OwnerTag, PoolError,
};
use ariax_storage::{
    ControlJournalAppender, DataBarrierKind, FileLayout, GlobalOffsetMapper, GlobalSpan,
    JournalAppenderError, JournalContributor, JournalDigest, JournalDigestAlgorithm, JournalHash,
    JournalPayload, JournalStateError, LeaseAbortReason, MapSpanError, NativeCapabilityError,
    PersistedId, PersistedSpan, RetryReason, RetryScope, RootFileCapability,
    calculate_contributors_hash, calculate_validator_set_fingerprint,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::time::Duration;

/// Bounded resources for one first-slice storage engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageEngineConfig {
    pub disk_workers: usize,
    pub disk_queue_capacity: usize,
    pub disk_completion_capacity: usize,
    pub max_in_flight_bytes: usize,
    pub buffer_pool_bytes: usize,
    pub shutdown_timeout: Duration,
}

impl Default for StorageEngineConfig {
    fn default() -> Self {
        Self {
            disk_workers: 1,
            disk_queue_capacity: 8,
            disk_completion_capacity: 8,
            max_in_flight_bytes: 2 * 1024 * 1024,
            buffer_pool_bytes: 4 * 1024 * 1024,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// Frozen provisional ownership for one exact storage piece.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseWritePlan {
    pub task: TaskId,
    pub generation: Generation,
    pub transfer_attempt: TransferAttemptId,
    pub lease: LeaseId,
    pub span: GlobalSpan,
    pub validator: JournalHash,
    pub overlap_group: Option<OverlapGroupId>,
}

/// One immutable transfer buffer submitted for positional placement.
pub struct WriteBlock {
    pub task: TaskId,
    pub generation: Generation,
    pub lease: LeaseId,
    pub global_offset: u64,
    pub expected_len: usize,
    pub buffer: BufferLease,
    pub piece: PieceId,
}

/// Exact evidence required before one provisional lease can commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseCommit {
    pub task: TaskId,
    pub generation: Generation,
    pub lease: LeaseId,
    pub received_len: u64,
    pub validator: JournalHash,
    pub response_digest: Option<JournalDigest>,
}

/// A fully selected, flushed retry wait decision. It is intentionally separate
/// from lease writes so span retries release storage ownership before waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryStateWrite {
    pub scope: RetryScope,
    pub scope_id: PersistedId,
    pub attempt: u32,
    pub elapsed_before_wait_ms: u64,
    pub scheduled_at_unix_ms: u64,
    pub delay_ms: u64,
    pub error_class: ErrorKind,
    pub retry_reason: RetryReason,
}

/// Storage-visible acknowledgement for the executable first slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteAck {
    ProvisionalAccepted {
        lease: LeaseId,
        span: GlobalSpan,
    },
    LeaseCommitPending {
        lease: LeaseId,
        group: OverlapGroupId,
    },
    LeaseCommitted {
        lease: LeaseId,
        span: GlobalSpan,
    },
    LeaseAborted {
        lease: LeaseId,
    },
    SpanRolledBack {
        group: OverlapGroupId,
        span: GlobalSpan,
    },
    PieceDurable {
        piece: PieceId,
        sequence: u64,
    },
}

/// Stable rejection classes at the protocol/storage boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteReject {
    TaskMismatch,
    GenerationMismatch,
    UnknownLease,
    DuplicateLease,
    LeaseMismatch,
    NonPieceAlignedLease,
    OverlapPolicy,
    NonContiguousWrite,
    PieceMismatch,
    BufferLengthMismatch,
    BufferState,
    Mapping,
    DiskAdmission,
    DiskCompletion,
    Journal,
    NativeFile,
    IdentifierExhausted,
    Shutdown,
}

impl WriteReject {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::TaskMismatch => "task_mismatch",
            Self::GenerationMismatch => "generation_mismatch",
            Self::UnknownLease => "unknown_lease",
            Self::DuplicateLease => "duplicate_lease",
            Self::LeaseMismatch => "lease_mismatch",
            Self::NonPieceAlignedLease => "non_piece_aligned_lease",
            Self::OverlapPolicy => "overlap_policy",
            Self::NonContiguousWrite => "noncontiguous_write",
            Self::PieceMismatch => "piece_mismatch",
            Self::BufferLengthMismatch => "buffer_length_mismatch",
            Self::BufferState => "buffer_state",
            Self::Mapping => "mapping",
            Self::DiskAdmission => "disk_admission",
            Self::DiskCompletion => "disk_completion",
            Self::Journal => "journal",
            Self::NativeFile => "native_file",
            Self::IdentifierExhausted => "identifier_exhausted",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Failure from the concrete descriptor-backed storage transaction.
#[derive(Debug)]
pub struct StorageEngineError {
    reject: WriteReject,
    detail: StorageEngineErrorDetail,
}

#[derive(Debug)]
enum StorageEngineErrorDetail {
    None,
    Journal(JournalAppenderError),
    Native(NativeCapabilityError),
    DiskStart(BlockingDiskLaneStartError),
    DiskRegistry(BlockingFileRegistryError),
    Disk(BlockingDiskError),
    DiskAdmission(BlockingDiskSubmitErrorKind),
    Buffer(PoolError),
    BufferTransition(BufferTransitionError),
    Map(MapSpanError),
    JournalState(JournalStateError),
}

impl StorageEngineError {
    #[must_use]
    pub const fn reject(&self) -> WriteReject {
        self.reject
    }

    fn bare(reject: WriteReject) -> Self {
        Self {
            reject,
            detail: StorageEngineErrorDetail::None,
        }
    }

    fn with(reject: WriteReject, detail: StorageEngineErrorDetail) -> Self {
        Self { reject, detail }
    }

    pub(crate) fn from_buffer_transition(error: BufferTransitionError) -> Self {
        Self::with(
            WriteReject::BufferState,
            StorageEngineErrorDetail::BufferTransition(error),
        )
    }
}

impl fmt::Display for StorageEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "storage rejected {}", self.reject.code())?;
        match &self.detail {
            StorageEngineErrorDetail::None => Ok(()),
            StorageEngineErrorDetail::Journal(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::Native(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::DiskStart(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::DiskRegistry(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::Disk(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::DiskAdmission(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::Buffer(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::BufferTransition(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::Map(error) => write!(formatter, ": {error}"),
            StorageEngineErrorDetail::JournalState(error) => write!(formatter, ": {error}"),
        }
    }
}

impl Error for StorageEngineError {}

struct StorageFile {
    capability: RootFileCapability,
    handle: BlockingFileHandle,
}

struct ActiveLease {
    plan: LeaseWritePlan,
    piece: PieceId,
    next_offset: u64,
    written_len: u64,
    digest: Sha256,
}

#[derive(Debug)]
struct OverlapGroup {
    piece: PieceId,
    span: GlobalSpan,
    members: BTreeSet<LeaseId>,
    candidate: Option<LeaseCommit>,
    frozen: bool,
}

/// First concrete `StorageEngine`: descriptor-only output authority, bounded
/// pooled buffers, bounded positional writes, and strict per-piece durability.
pub struct StorageEngine {
    task: TaskId,
    generation: Generation,
    layout: FileLayout,
    mapper: GlobalOffsetMapper,
    pool: BufferPool,
    registry: BlockingFileRegistry,
    lane: Option<BlockingDiskLane>,
    files: BTreeMap<FileId, StorageFile>,
    active: BTreeMap<LeaseId, ActiveLease>,
    overlap_groups: BTreeMap<OverlapGroupId, OverlapGroup>,
    seen_leases: BTreeSet<LeaseId>,
    next_operation_id: u64,
    journal: ControlJournalAppender,
    shutdown_timeout: Duration,
}

impl StorageEngine {
    pub fn open_layout(
        layout: FileLayout,
        opened_files: impl IntoIterator<Item = (FileId, RootFileCapability)>,
        journal: ControlJournalAppender,
        config: StorageEngineConfig,
    ) -> Result<Self, StorageEngineError> {
        let mapper = GlobalOffsetMapper::new(&layout).map_err(|error| {
            StorageEngineError::with(WriteReject::Mapping, StorageEngineErrorDetail::Map(error))
        })?;
        let pool = BufferPool::new(BufferPoolConfig::new(
            config.buffer_pool_bytes,
            config.buffer_pool_bytes,
        ))
        .map_err(|error| {
            StorageEngineError::with(
                WriteReject::BufferState,
                StorageEngineErrorDetail::Buffer(error),
            )
        })?;
        let epoch = BlockingBackendEpoch::new(1)
            .expect("the first process-local blocking backend epoch is nonzero");
        let registry = BlockingFileRegistry::new(epoch);
        let mut files = BTreeMap::new();
        for (id, capability) in opened_files {
            let entry = layout
                .files()
                .iter()
                .find(|entry| entry.id() == id && entry.selected())
                .ok_or_else(|| StorageEngineError::bare(WriteReject::Mapping))?;
            let expected = entry
                .identity()
                .ok_or_else(|| StorageEngineError::bare(WriteReject::NativeFile))?;
            if capability.identity().encode().as_ref() != expected.bytes() {
                return Err(StorageEngineError::bare(WriteReject::NativeFile));
            }
            let registered = capability.try_clone_file().map_err(|error| {
                StorageEngineError::with(
                    WriteReject::NativeFile,
                    StorageEngineErrorDetail::Native(error),
                )
            })?;
            let handle = registry
                .register(registered, entry.length())
                .map_err(|error| {
                    StorageEngineError::with(
                        WriteReject::NativeFile,
                        StorageEngineErrorDetail::DiskRegistry(error),
                    )
                })?;
            if files
                .insert(id, StorageFile { capability, handle })
                .is_some()
            {
                return Err(StorageEngineError::bare(WriteReject::NativeFile));
            }
        }
        let selected_count = layout
            .files()
            .iter()
            .filter(|entry| entry.selected())
            .count();
        if files.len() != selected_count {
            return Err(StorageEngineError::bare(WriteReject::NativeFile));
        }
        let lane = BlockingDiskLane::new(
            BlockingDiskLaneConfig {
                worker_count: config.disk_workers,
                queue_capacity: config.disk_queue_capacity,
                completion_capacity: config.disk_completion_capacity,
                max_accepted_bytes: config.max_in_flight_bytes,
            },
            epoch,
            registry.clone(),
        )
        .map_err(|error| {
            StorageEngineError::with(
                WriteReject::DiskAdmission,
                StorageEngineErrorDetail::DiskStart(error),
            )
        })?;
        Ok(Self {
            task: layout.task(),
            generation: layout.generation(),
            layout,
            mapper,
            pool,
            registry,
            lane: Some(lane),
            files,
            active: BTreeMap::new(),
            overlap_groups: BTreeMap::new(),
            seen_leases: BTreeSet::new(),
            next_operation_id: 1,
            journal,
            shutdown_timeout: config.shutdown_timeout,
        })
    }

    #[must_use]
    pub const fn layout(&self) -> &FileLayout {
        &self.layout
    }

    #[must_use]
    pub const fn buffer_pool(&self) -> &BufferPool {
        &self.pool
    }

    pub fn reserve_network_buffer(
        &self,
        minimum_capacity: usize,
    ) -> Result<BufferLease, StorageEngineError> {
        let mut lease = self
            .pool
            .try_reserve(
                minimum_capacity,
                OwnerTag::Network,
                Some(self.task),
                Some(self.generation),
            )
            .map_err(|error| {
                StorageEngineError::with(
                    WriteReject::BufferState,
                    StorageEngineErrorDetail::Buffer(error),
                )
            })?;
        lease
            .transition(BufferState::NetworkFill, OwnerTag::Network)
            .map_err(|error| {
                StorageEngineError::with(
                    WriteReject::BufferState,
                    StorageEngineErrorDetail::BufferTransition(error),
                )
            })?;
        Ok(lease)
    }

    /// Returns a filled network buffer that became obsolete before positional
    /// submission, for example after an endgame candidate froze its overlap
    /// group. No storage visibility or journal fact is produced.
    pub fn discard_network_buffer(&self, lease: BufferLease) -> Result<(), StorageEngineError> {
        self.release_buffer(lease)
    }

    pub fn begin_lease(&mut self, plan: LeaseWritePlan) -> Result<WriteAck, StorageEngineError> {
        self.validate_identity(plan.task, plan.generation)?;
        if self.seen_leases.contains(&plan.lease) || self.active.contains_key(&plan.lease) {
            return Err(StorageEngineError::bare(WriteReject::DuplicateLease));
        }
        let piece = PieceId::new(plan.span.offset / self.layout.piece_length());
        let expected = self.piece_span(piece)?;
        if expected.offset() != plan.span.offset
            || usize::try_from(expected.len()).ok() != Some(plan.span.len)
        {
            return Err(StorageEngineError::bare(WriteReject::NonPieceAlignedLease));
        }
        let overlapping = self
            .active
            .iter()
            .filter(|(_, active)| active.piece == piece)
            .map(|(&lease, _)| lease)
            .collect::<Vec<_>>();
        match plan.overlap_group {
            None => {
                if !overlapping.is_empty() {
                    return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
                }
            }
            Some(group) => {
                let overlap = self
                    .overlap_groups
                    .get(&group)
                    .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
                if overlap.frozen
                    || overlap.piece != piece
                    || overlap.span != plan.span
                    || !overlap.members.contains(&plan.lease)
                    || overlapping.len() != 1
                {
                    return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
                }
                let peer = overlapping[0];
                let active = self
                    .active
                    .get(&peer)
                    .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
                if active.plan.overlap_group != Some(group)
                    || active.plan.span != plan.span
                    || active.plan.validator != plan.validator
                {
                    return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
                }
            }
        }
        let persisted = expected;
        self.journal
            .append_payload(
                self.generation,
                &JournalPayload::LeaseStarted {
                    transfer_attempt_id: plan.transfer_attempt,
                    lease_id: plan.lease,
                    span: persisted,
                    validator_fingerprint: plan.validator,
                },
            )
            .map_err(journal_error)?;
        self.journal
            .append_payload(
                self.generation,
                &JournalPayload::PieceStarted {
                    lease_id: plan.lease,
                    piece_id: piece,
                    piece_span: persisted,
                },
            )
            .map_err(journal_error)?;
        self.seen_leases.insert(plan.lease);
        self.active.insert(
            plan.lease,
            ActiveLease {
                next_offset: plan.span.offset,
                plan,
                piece,
                written_len: 0,
                digest: Sha256::new(),
            },
        );
        Ok(WriteAck::ProvisionalAccepted {
            lease: plan.lease,
            span: plan.span,
        })
    }

    /// Reserves the logical two-member overlap group before the duplicate's
    /// response head is accepted. This closes the race where the original
    /// reaches `CommitLease` while the duplicate is still in flight.
    #[allow(clippy::too_many_arguments)]
    pub fn register_overlap_group(
        &mut self,
        task: TaskId,
        generation: Generation,
        group: OverlapGroupId,
        original: LeaseId,
        duplicate: LeaseId,
        span: GlobalSpan,
        validator: JournalHash,
    ) -> Result<(), StorageEngineError> {
        self.validate_identity(task, generation)?;
        if original == duplicate || self.overlap_groups.contains_key(&group) {
            return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
        }
        let active = self
            .active
            .get_mut(&original)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        if active.plan.overlap_group.is_some()
            || active.plan.span != span
            || active.plan.validator != validator
        {
            return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
        }
        if self.seen_leases.contains(&duplicate) {
            return Err(StorageEngineError::bare(WriteReject::DuplicateLease));
        }
        active.plan.overlap_group = Some(group);
        self.overlap_groups.insert(
            group,
            OverlapGroup {
                piece: active.piece,
                span,
                members: BTreeSet::from([original, duplicate]),
                candidate: None,
                frozen: false,
            },
        );
        Ok(())
    }

    /// Settles a duplicate that was cancelled before its response head could
    /// begin a storage lease. It has no journal abort because no lease-start
    /// fact exists yet; a pending candidate can therefore commit cleanly.
    pub fn settle_unopened_overlap_member(
        &mut self,
        task: TaskId,
        generation: Generation,
        group_id: OverlapGroupId,
        lease: LeaseId,
    ) -> Result<Vec<WriteAck>, StorageEngineError> {
        self.validate_identity(task, generation)?;
        if self.active.contains_key(&lease) {
            return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
        }
        let mut group = self
            .overlap_groups
            .remove(&group_id)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
        if !group.members.remove(&lease) || group.members.len() != 1 {
            return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
        }
        let remaining = group
            .members
            .iter()
            .copied()
            .next()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
        if let Some(candidate) = group.candidate {
            if candidate.lease != remaining {
                return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
            }
            return self.commit_active(candidate);
        }
        self.active
            .get_mut(&remaining)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?
            .plan
            .overlap_group = None;
        Ok(Vec::new())
    }

    pub async fn write_block(&mut self, block: WriteBlock) -> Result<WriteAck, StorageEngineError> {
        if let Err(error) = self.validate_block(&block) {
            self.release_buffer(block.buffer)?;
            return Err(error);
        }
        let mapped = self
            .mapper
            .map(GlobalSpan {
                offset: block.global_offset,
                len: block.expected_len,
            })
            .map_err(|error| {
                StorageEngineError::with(WriteReject::Mapping, StorageEngineErrorDetail::Map(error))
            });
        let mapped = match mapped {
            Ok(mapped) => mapped,
            Err(error) => {
                self.release_buffer(block.buffer)?;
                return Err(error);
            }
        };
        let file = self
            .files
            .get(&mapped.file)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::NativeFile));
        let file = match file {
            Ok(file) => file,
            Err(error) => {
                self.release_buffer(block.buffer)?;
                return Err(error);
            }
        };
        let operation_id = BlockingDiskOperationId::new(self.next_operation_id)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::IdentifierExhausted))?;
        self.next_operation_id = self
            .next_operation_id
            .checked_add(1)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::IdentifierExhausted))?;
        let (_cancel, registration) = BlockingDiskCancelHandle::pair();
        let operation = BlockingDiskOperation::WriteAt {
            handle: file.handle,
            offset: mapped.file_offset,
            expected_len: block.expected_len,
        };
        let submission = BlockingDiskSubmission::new(
            operation_id,
            self.registry.backend_epoch(),
            operation,
            registration,
            block.buffer,
        );
        let lane = self
            .lane
            .as_ref()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Shutdown))?;
        if let Err(error) = lane.try_submit(submission) {
            let (reason, submission) = error.into_parts();
            self.release_buffer(submission.into_lease())?;
            return Err(StorageEngineError::with(
                WriteReject::DiskAdmission,
                StorageEngineErrorDetail::DiskAdmission(reason),
            ));
        }
        let outcome = loop {
            if let Some(outcome) = lane.try_recv() {
                if outcome.operation_id() == operation_id {
                    break outcome;
                }
                debug_assert!(
                    false,
                    "the sequential first slice has one disk operation in flight"
                );
            }
            tokio::task::yield_now().await;
        };
        let (_, _, _, result, lease) = outcome.into_parts();
        if let Err(error) = result {
            self.release_buffer(lease)?;
            return Err(StorageEngineError::with(
                WriteReject::DiskCompletion,
                StorageEngineErrorDetail::Disk(error),
            ));
        }
        {
            let bytes = match lease.bytes() {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.release_buffer(lease)?;
                    return Err(StorageEngineError::with(
                        WriteReject::BufferState,
                        StorageEngineErrorDetail::BufferTransition(error),
                    ));
                }
            };
            let active = self
                .active
                .get_mut(&block.lease)
                .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
            active.digest.update(bytes);
            active.next_offset = active
                .next_offset
                .checked_add(u64::try_from(block.expected_len).expect("buffer length fits u64"))
                .ok_or_else(|| StorageEngineError::bare(WriteReject::NonContiguousWrite))?;
            active.written_len +=
                u64::try_from(block.expected_len).expect("buffer length fits u64");
        }
        self.release_buffer(lease)?;
        Ok(WriteAck::ProvisionalAccepted {
            lease: block.lease,
            span: GlobalSpan {
                offset: block.global_offset,
                len: block.expected_len,
            },
        })
    }

    pub fn commit_lease(
        &mut self,
        commit: LeaseCommit,
    ) -> Result<Vec<WriteAck>, StorageEngineError> {
        self.validate_identity(commit.task, commit.generation)?;
        let active = self
            .active
            .get(&commit.lease)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        self.validate_commit(active, &commit)?;
        if let Some(group_id) = active.plan.overlap_group {
            let group = self
                .overlap_groups
                .get_mut(&group_id)
                .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
            if group.frozen
                || group.candidate.is_some()
                || group.members.len() != 2
                || !group.members.contains(&commit.lease)
                || group.piece != active.piece
                || group.span != active.plan.span
            {
                return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
            }
            group.frozen = true;
            group.candidate = Some(commit.clone());
            return Ok(vec![WriteAck::LeaseCommitPending {
                lease: commit.lease,
                group: group_id,
            }]);
        }
        self.commit_active(commit)
    }

    fn validate_commit(
        &self,
        active: &ActiveLease,
        commit: &LeaseCommit,
    ) -> Result<(), StorageEngineError> {
        let expected_len = u64::try_from(active.plan.span.len)
            .map_err(|_| StorageEngineError::bare(WriteReject::LeaseMismatch))?;
        if active.plan.validator != commit.validator
            || active.written_len != expected_len
            || commit.received_len != expected_len
        {
            return Err(StorageEngineError::bare(WriteReject::LeaseMismatch));
        }
        Ok(())
    }

    fn commit_active(&mut self, commit: LeaseCommit) -> Result<Vec<WriteAck>, StorageEngineError> {
        let active = self
            .active
            .get(&commit.lease)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        self.validate_commit(active, &commit)?;
        let active = self
            .active
            .remove(&commit.lease)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        let span = self.piece_span(active.piece)?;
        let digest = JournalDigest::new(
            JournalDigestAlgorithm::Sha256,
            active.digest.finalize().to_vec(),
        )
        .expect("SHA-256 output has canonical length");
        self.journal
            .append_payload(
                self.generation,
                &JournalPayload::PieceWritten {
                    lease_id: commit.lease,
                    piece_id: active.piece,
                    written_span: span,
                },
            )
            .map_err(journal_error)?;
        self.journal
            .append_payload(
                self.generation,
                &JournalPayload::LeaseCommitted {
                    lease_id: commit.lease,
                    span,
                    validator_fingerprint: commit.validator,
                    response_digest: commit.response_digest,
                },
            )
            .map_err(journal_error)?;
        let contributor = JournalContributor::new(commit.lease, span, commit.validator);
        let contributors_hash = calculate_contributors_hash(&[contributor]).map_err(|error| {
            StorageEngineError::with(
                WriteReject::Journal,
                StorageEngineErrorDetail::JournalState(error),
            )
        })?;
        let validator_set_fingerprint = calculate_validator_set_fingerprint(&[contributor])
            .map_err(|error| {
                StorageEngineError::with(
                    WriteReject::Journal,
                    StorageEngineErrorDetail::JournalState(error),
                )
            })?;
        self.journal
            .append_payload(
                self.generation,
                &JournalPayload::PieceVerified {
                    piece_id: active.piece,
                    piece_span: span,
                    contributors_hash,
                    digest: digest.clone(),
                },
            )
            .map_err(journal_error)?;
        self.sync_piece_files(span)?;
        let durable = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::PieceDurable {
                    piece_id: active.piece,
                    piece_span: span,
                    contributors_hash,
                    validator_set_fingerprint,
                    digest: Some(digest),
                    data_barrier: DataBarrierKind::StrictPiece,
                },
            )
            .map_err(journal_error)?;
        let flushed = self
            .journal
            .flush(durable.sequence())
            .map_err(journal_error)?;
        Ok(vec![
            WriteAck::LeaseCommitted {
                lease: commit.lease,
                span: active.plan.span,
            },
            WriteAck::PieceDurable {
                piece: active.piece,
                sequence: flushed.through_sequence(),
            },
        ])
    }

    pub fn abort_lease(
        &mut self,
        task: TaskId,
        generation: Generation,
        lease: LeaseId,
        reason: LeaseAbortReason,
    ) -> Result<Vec<WriteAck>, StorageEngineError> {
        self.validate_identity(task, generation)?;
        let active = self
            .active
            .get(&lease)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        let Some(group_id) = active.plan.overlap_group else {
            self.active.remove(&lease);
            let sequence = self.append_lease_abort(lease, reason)?;
            self.journal.flush(sequence).map_err(journal_error)?;
            return Ok(vec![WriteAck::LeaseAborted { lease }]);
        };
        let group = self
            .overlap_groups
            .get(&group_id)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
        if !group.members.contains(&lease) {
            return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
        }
        let candidate = group.candidate.clone();
        let wrote = active.written_len != 0;
        if candidate
            .as_ref()
            .is_some_and(|candidate| candidate.lease == lease)
            || wrote
        {
            return self.rollback_overlap(group_id, reason);
        }
        self.active.remove(&lease);
        let abort_sequence = self.append_lease_abort(lease, reason)?;
        let mut group = self
            .overlap_groups
            .remove(&group_id)
            .expect("validated overlap group exists");
        group.members.remove(&lease);
        let remaining = group
            .members
            .iter()
            .copied()
            .next()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
        if let Some(candidate) = candidate {
            if candidate.lease != remaining {
                return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
            }
            let mut acknowledgements = vec![WriteAck::LeaseAborted { lease }];
            acknowledgements.extend(self.commit_active(candidate)?);
            return Ok(acknowledgements);
        }
        if let Some(remaining) = self.active.get_mut(&remaining) {
            remaining.plan.overlap_group = None;
        }
        self.journal.flush(abort_sequence).map_err(journal_error)?;
        Ok(vec![WriteAck::LeaseAborted { lease }])
    }

    fn rollback_overlap(
        &mut self,
        group_id: OverlapGroupId,
        reason: LeaseAbortReason,
    ) -> Result<Vec<WriteAck>, StorageEngineError> {
        let group = self
            .overlap_groups
            .remove(&group_id)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
        let mut acknowledgements = Vec::with_capacity(group.members.len() + 1);
        let mut last_sequence = None;
        for lease in group.members {
            if self.active.remove(&lease).is_some() {
                last_sequence = Some(self.append_lease_abort(lease, reason)?);
                acknowledgements.push(WriteAck::LeaseAborted { lease });
            }
        }
        let sequence =
            last_sequence.ok_or_else(|| StorageEngineError::bare(WriteReject::OverlapPolicy))?;
        self.journal.flush(sequence).map_err(journal_error)?;
        acknowledgements.push(WriteAck::SpanRolledBack {
            group: group_id,
            span: group.span,
        });
        Ok(acknowledgements)
    }

    fn append_lease_abort(
        &mut self,
        lease: LeaseId,
        reason: LeaseAbortReason,
    ) -> Result<u64, StorageEngineError> {
        self.journal
            .append_payload(
                self.generation,
                &JournalPayload::LeaseAborted {
                    lease_id: lease,
                    reason,
                },
            )
            .map(|appended| appended.sequence())
            .map_err(journal_error)
    }

    pub fn record_retry_state(&mut self, retry: RetryStateWrite) -> Result<(), StorageEngineError> {
        if retry.attempt == 0 || retry.delay_ms == 0 {
            return Err(StorageEngineError::bare(WriteReject::Journal));
        }
        let appended = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::RetryState {
                    scope: retry.scope,
                    scope_id: retry.scope_id,
                    attempt: retry.attempt,
                    elapsed_before_wait_ms: retry.elapsed_before_wait_ms,
                    scheduled_at_unix_ms: retry.scheduled_at_unix_ms,
                    delay_ms: retry.delay_ms,
                    error_class: retry.error_class,
                    retry_reason: retry.retry_reason,
                },
            )
            .map_err(journal_error)?;
        self.journal
            .flush(appended.sequence())
            .map_err(journal_error)?;
        Ok(())
    }

    pub fn complete(
        &mut self,
        final_digest: Option<JournalDigest>,
        completed_at_unix_ms: u64,
    ) -> Result<u64, StorageEngineError> {
        if !self.active.is_empty() || !self.overlap_groups.is_empty() {
            return Err(StorageEngineError::bare(WriteReject::LeaseMismatch));
        }
        let final_length = self
            .layout
            .total_length()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Mapping))?;
        let layout_hash = JournalHash::new(*self.layout.layout_hash().as_bytes())
            .expect("layout SHA-256 is nonzero");
        let appended = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::TaskComplete {
                    layout_hash,
                    final_length,
                    final_digest,
                    completed_at_unix_ms,
                },
            )
            .map_err(journal_error)?;
        let flushed = self
            .journal
            .flush(appended.sequence())
            .map_err(journal_error)?;
        Ok(flushed.through_sequence())
    }

    pub fn close(mut self) -> Result<(), StorageEngineError> {
        self.shutdown_lane()?;
        self.journal.close_flushed().map_err(journal_error)
    }

    /// Quiesces storage and returns the flushed journal to its serialized
    /// session owner. The caller must install it before emitting a scheduler
    /// event that can require further journal-backed persistence.
    pub fn into_flushed_journal(mut self) -> Result<ControlJournalAppender, StorageEngineError> {
        self.shutdown_lane()?;
        self.journal.close_flushed().map_err(journal_error)?;
        Ok(self.journal)
    }

    fn validate_identity(
        &self,
        task: TaskId,
        generation: Generation,
    ) -> Result<(), StorageEngineError> {
        if task != self.task {
            return Err(StorageEngineError::bare(WriteReject::TaskMismatch));
        }
        if generation != self.generation {
            return Err(StorageEngineError::bare(WriteReject::GenerationMismatch));
        }
        Ok(())
    }

    fn validate_block(&self, block: &WriteBlock) -> Result<(), StorageEngineError> {
        self.validate_identity(block.task, block.generation)?;
        let active = self
            .active
            .get(&block.lease)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        if active.plan.overlap_group.is_some_and(|group| {
            self.overlap_groups
                .get(&group)
                .is_none_or(|state| state.frozen)
        }) {
            return Err(StorageEngineError::bare(WriteReject::OverlapPolicy));
        }
        if active.piece != block.piece {
            return Err(StorageEngineError::bare(WriteReject::PieceMismatch));
        }
        if active.next_offset != block.global_offset {
            return Err(StorageEngineError::bare(WriteReject::NonContiguousWrite));
        }
        if block.buffer.len() != block.expected_len {
            return Err(StorageEngineError::bare(WriteReject::BufferLengthMismatch));
        }
        if block.buffer.state() != BufferState::Filled {
            return Err(StorageEngineError::bare(WriteReject::BufferState));
        }
        let block_len = u64::try_from(block.expected_len)
            .map_err(|_| StorageEngineError::bare(WriteReject::LeaseMismatch))?;
        let end = block
            .global_offset
            .checked_add(block_len)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::LeaseMismatch))?;
        let lease_end = active
            .plan
            .span
            .offset
            .checked_add(
                u64::try_from(active.plan.span.len)
                    .map_err(|_| StorageEngineError::bare(WriteReject::LeaseMismatch))?,
            )
            .ok_or_else(|| StorageEngineError::bare(WriteReject::LeaseMismatch))?;
        if end > lease_end {
            return Err(StorageEngineError::bare(WriteReject::LeaseMismatch));
        }
        Ok(())
    }

    fn piece_span(&self, piece: PieceId) -> Result<PersistedSpan, StorageEngineError> {
        let total = self
            .layout
            .total_length()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Mapping))?;
        let offset = piece
            .get()
            .checked_mul(self.layout.piece_length())
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Mapping))?;
        if offset >= total {
            return Err(StorageEngineError::bare(WriteReject::Mapping));
        }
        PersistedSpan::new(offset, self.layout.piece_length().min(total - offset)).map_err(
            |error| {
                StorageEngineError::with(
                    WriteReject::Mapping,
                    StorageEngineErrorDetail::JournalState(JournalStateError::Payload(error)),
                )
            },
        )
    }

    fn sync_piece_files(&self, span: PersistedSpan) -> Result<(), StorageEngineError> {
        let len = usize::try_from(span.len())
            .map_err(|_| StorageEngineError::bare(WriteReject::Mapping))?;
        let mapped = self
            .mapper
            .map(GlobalSpan {
                offset: span.offset(),
                len,
            })
            .map_err(|error| {
                StorageEngineError::with(WriteReject::Mapping, StorageEngineErrorDetail::Map(error))
            })?;
        self.files
            .get(&mapped.file)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::NativeFile))?
            .capability
            .sync_all()
            .map_err(|error| {
                StorageEngineError::with(
                    WriteReject::NativeFile,
                    StorageEngineErrorDetail::Native(error),
                )
            })
    }

    fn release_buffer(&self, mut lease: BufferLease) -> Result<(), StorageEngineError> {
        if lease.state() != BufferState::Releasable {
            lease
                .transition(BufferState::Releasable, OwnerTag::Storage)
                .map_err(|error| {
                    StorageEngineError::with(
                        WriteReject::BufferState,
                        StorageEngineErrorDetail::BufferTransition(error),
                    )
                })?;
        }
        self.pool.release(lease).map_err(|error| {
            let (error, _lease) = error.into_parts();
            StorageEngineError::with(
                WriteReject::BufferState,
                StorageEngineErrorDetail::Buffer(error),
            )
        })
    }

    fn shutdown_lane(&mut self) -> Result<(), StorageEngineError> {
        let Some(lane) = self.lane.take() else {
            return Ok(());
        };
        let shutdown = lane.shutdown(self.shutdown_timeout);
        if shutdown.detached_workers() != 0
            || shutdown.in_flight_at_timeout() != 0
            || !shutdown.completion_closed()
        {
            return Err(StorageEngineError::bare(WriteReject::Shutdown));
        }
        for outcome in shutdown.into_outcomes() {
            let (_, _, _, result, lease) = outcome.into_parts();
            self.release_buffer(lease)?;
            result.map_err(|error| {
                StorageEngineError::with(
                    WriteReject::DiskCompletion,
                    StorageEngineErrorDetail::Disk(error),
                )
            })?;
        }
        for file in self.files.values() {
            self.registry.unregister(file.handle).map_err(|error| {
                StorageEngineError::with(
                    WriteReject::NativeFile,
                    StorageEngineErrorDetail::DiskRegistry(error),
                )
            })?;
        }
        Ok(())
    }
}

fn journal_error(error: JournalAppenderError) -> StorageEngineError {
    StorageEngineError::with(
        WriteReject::Journal,
        StorageEngineErrorDetail::Journal(error),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_first_slice::{
        append_initial_admission, append_layout, build_single_file_layout,
    };
    use ariax_core::Gid;
    use ariax_storage::{JournalId, PathPlatform, RootDirectoryCapability, SafePathBuilder};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "ariax-storage-engine-{}-{ordinal}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _removed = fs::remove_dir_all(&self.0);
        }
    }

    fn lease_plan(
        lease: LeaseId,
        attempt: u64,
        span: GlobalSpan,
        validator: JournalHash,
        overlap_group: Option<OverlapGroupId>,
    ) -> LeaseWritePlan {
        LeaseWritePlan {
            task: TaskId::new(1).expect("task"),
            generation: Generation::INITIAL,
            transfer_attempt: TransferAttemptId::new(attempt).expect("attempt"),
            lease,
            span,
            validator,
            overlap_group,
        }
    }

    async fn write_piece(engine: &mut StorageEngine, lease: LeaseId, bytes: &[u8]) {
        let mut buffer = engine
            .reserve_network_buffer(bytes.len())
            .expect("reserve network buffer");
        buffer.writable().expect("writable buffer")[..bytes.len()].copy_from_slice(bytes);
        buffer
            .mark_filled(bytes.len(), OwnerTag::Storage)
            .expect("filled buffer");
        engine
            .write_block(WriteBlock {
                task: TaskId::new(1).expect("task"),
                generation: Generation::INITIAL,
                lease,
                global_offset: 0,
                expected_len: bytes.len(),
                buffer,
                piece: PieceId::new(0),
            })
            .await
            .expect("piece write");
    }

    #[tokio::test]
    async fn aborting_pending_candidate_rolls_back_before_clean_loser_can_commit_it() {
        let directory = TestDirectory::new();
        let output_root = directory.0.join("output");
        let journal_root = directory.0.join("journal");
        fs::create_dir_all(&output_root).expect("output root");
        let root = RootDirectoryCapability::open_trusted(&output_root).expect("root capability");
        let output =
            SafePathBuilder::from_user_path("output.bin", PathPlatform::current()).expect("path");
        let output_file = root.create_new_file(&output).expect("output file");
        output_file.set_len(4).expect("preallocate output");
        let layout = build_single_file_layout(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            &root,
            &output,
            &output_file,
            4,
            4,
        )
        .expect("layout");
        let mut journal = ControlJournalAppender::create(
            &journal_root,
            Gid::new(1).expect("gid"),
            JournalId::new([1; 16]).expect("journal id"),
            Generation::INITIAL,
            1,
        )
        .expect("journal");
        append_initial_admission(&mut journal, Generation::INITIAL).expect("admission");
        append_layout(&mut journal, &layout).expect("layout journal");
        let mut engine = StorageEngine::open_layout(
            layout,
            [(FileId::new(0), output_file)],
            journal,
            StorageEngineConfig::default(),
        )
        .expect("storage engine");

        let original = LeaseId::new(1).expect("original");
        let candidate = LeaseId::new(2).expect("candidate");
        let replacement = LeaseId::new(3).expect("replacement");
        let group = OverlapGroupId::new(1).expect("group");
        let span = GlobalSpan { offset: 0, len: 4 };
        let validator = JournalHash::new([7; 32]).expect("validator");
        engine
            .begin_lease(lease_plan(original, 1, span, validator, None))
            .expect("original lease");
        engine
            .register_overlap_group(
                TaskId::new(1).expect("task"),
                Generation::INITIAL,
                group,
                original,
                candidate,
                span,
                validator,
            )
            .expect("overlap group");
        engine
            .begin_lease(lease_plan(candidate, 2, span, validator, Some(group)))
            .expect("candidate lease");
        write_piece(&mut engine, candidate, &[1, 2, 3, 4]).await;
        assert_eq!(
            engine
                .commit_lease(LeaseCommit {
                    task: TaskId::new(1).expect("task"),
                    generation: Generation::INITIAL,
                    lease: candidate,
                    received_len: 4,
                    validator,
                    response_digest: None,
                })
                .expect("pending commit"),
            vec![WriteAck::LeaseCommitPending {
                lease: candidate,
                group,
            }]
        );

        let rollback = engine
            .abort_lease(
                TaskId::new(1).expect("task"),
                Generation::INITIAL,
                candidate,
                LeaseAbortReason::Cancelled,
            )
            .expect("candidate rollback");
        assert!(rollback.contains(&WriteAck::LeaseAborted { lease: original }));
        assert!(rollback.contains(&WriteAck::LeaseAborted { lease: candidate }));
        assert!(rollback.contains(&WriteAck::SpanRolledBack { group, span }));
        assert!(
            !rollback
                .iter()
                .any(|ack| matches!(ack, WriteAck::PieceDurable { .. }))
        );

        engine
            .begin_lease(lease_plan(replacement, 3, span, validator, None))
            .expect("replacement lease");
        write_piece(&mut engine, replacement, &[9, 9, 9, 9]).await;
        let committed = engine
            .commit_lease(LeaseCommit {
                task: TaskId::new(1).expect("task"),
                generation: Generation::INITIAL,
                lease: replacement,
                received_len: 4,
                validator,
                response_digest: None,
            })
            .expect("replacement commit");
        assert!(
            committed
                .iter()
                .any(|ack| matches!(ack, WriteAck::PieceDurable { .. }))
        );
        engine.close().expect("close engine");
        assert_eq!(
            fs::read(output_root.join("output.bin")).expect("output bytes"),
            [9, 9, 9, 9]
        );
    }
}
