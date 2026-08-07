use ariax_core::{FileId, Generation, LeaseId, PieceId, TaskId, TransferAttemptId};
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
    PersistedSpan, RootFileCapability, calculate_contributors_hash,
    calculate_validator_set_fingerprint,
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

/// Storage-visible acknowledgement for the executable first slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteAck {
    ProvisionalAccepted { lease: LeaseId, span: GlobalSpan },
    LeaseCommitted { lease: LeaseId, span: GlobalSpan },
    LeaseAborted { lease: LeaseId },
    PieceDurable { piece: PieceId, sequence: u64 },
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
    ) -> Result<[WriteAck; 2], StorageEngineError> {
        self.validate_identity(commit.task, commit.generation)?;
        let active = self
            .active
            .remove(&commit.lease)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        let expected_len = u64::try_from(active.plan.span.len)
            .map_err(|_| StorageEngineError::bare(WriteReject::LeaseMismatch))?;
        if active.plan.validator != commit.validator
            || active.written_len != expected_len
            || commit.received_len != expected_len
        {
            self.active.insert(commit.lease, active);
            return Err(StorageEngineError::bare(WriteReject::LeaseMismatch));
        }
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
        Ok([
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
    ) -> Result<WriteAck, StorageEngineError> {
        self.validate_identity(task, generation)?;
        if self.active.remove(&lease).is_none() {
            return Err(StorageEngineError::bare(WriteReject::UnknownLease));
        }
        let appended = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::LeaseAborted {
                    lease_id: lease,
                    reason,
                },
            )
            .map_err(journal_error)?;
        self.journal
            .flush(appended.sequence())
            .map_err(journal_error)?;
        Ok(WriteAck::LeaseAborted { lease })
    }

    pub fn complete(
        &mut self,
        final_digest: Option<JournalDigest>,
        completed_at_unix_ms: u64,
    ) -> Result<u64, StorageEngineError> {
        if !self.active.is_empty() {
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
