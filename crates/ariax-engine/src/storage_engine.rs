use ariax_core::{
    ErrorKind, FileId, Generation, LeaseId, OverlapGroupId, PieceId, TaskId, TransferAttemptId,
};
use ariax_runtime::{
    BlockingBackendEpoch, BlockingDiskCancelHandle, BlockingDiskError, BlockingDiskLane,
    BlockingDiskLaneConfig, BlockingDiskLaneStartError, BlockingDiskOperation,
    BlockingDiskOperationId, BlockingDiskSubmission, BlockingDiskSubmitErrorKind,
    BlockingFileHandle, BlockingFileRegistry, BlockingFileRegistryError, BufferLease, BufferPool,
    BufferPoolConfig, BufferState, BufferTransitionError, ByteBudget, HandleBudgetError,
    HandleBudgets, HandlePermit, OwnerTag, PoolError,
};
use ariax_storage::{
    ControlJournalAppender, DataBarrierKind, FileLayout, GlobalOffsetMapper, GlobalSpan,
    JournalAppenderError, JournalContributor, JournalDigest, JournalDigestAlgorithm, JournalHash,
    JournalPayload, JournalStateError, LeaseAbortReason, MapSpanError, NativeCapabilityError,
    PersistedId, PersistedSpan, RetryReason, RetryScope, RootFileCapability,
    calculate_contributors_hash, calculate_http_range_identity_fingerprint,
    calculate_validator_set_fingerprint,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::time::Duration;

#[cfg(test)]
use ariax_runtime::{BlockingDiskExecutor, BlockingDiskIoError, BlockingDiskIoErrorKind};
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use tokio::sync::Notify;

/// Bounded resources for one first-slice storage engine.
#[derive(Clone, Debug)]
pub struct StorageEngineConfig {
    pub disk_workers: usize,
    pub disk_queue_capacity: usize,
    pub disk_completion_capacity: usize,
    pub max_in_flight_bytes: usize,
    pub buffer_pool_bytes: usize,
    /// Global resident-byte domain shared with process HTTP ingress and
    /// connection overhead when constructed from a runtime profile.
    pub resident_budget: ByteBudget,
    /// Optional process-owned file-handle domains. One selected output file
    /// consumes two permits: its capability descriptor and the descriptor
    /// registered with the blocking disk lane.
    pub handle_budgets: Option<HandleBudgets>,
    pub shutdown_timeout: Duration,
    #[cfg(test)]
    pub disk_fault: Option<StorageEngineDiskFault>,
    #[cfg(test)]
    pub crash_point: Option<StorageEngineCrashPoint>,
    #[cfg(test)]
    pub piece_durable_notifier: Option<std::sync::mpsc::Sender<PieceId>>,
    #[cfg(test)]
    pub(crate) write_completion_gate: Option<StorageEngineWriteCompletionGate>,
}

/// Deterministic disk boundary faults used by the HTTP/storage integration
/// tests.  These never exist in non-test builds and cannot alter production
/// backend selection.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageEngineDiskFault {
    OutOfSpace,
    PermissionDenied,
    ShortWrite,
}

/// Exact crash barriers used only by child-process recovery tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageEngineCrashPoint {
    LeaseCommitted,
    DataSyncBeforePieceDurable,
    PieceDurableBeforeJournalSync,
}

/// Test-only two-party gate placed after one successful disk completion and
/// before storage publishes the provisional write acknowledgement. This makes
/// cancellation races deterministic without changing production scheduling.
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct StorageEngineWriteCompletionGate {
    reached: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    release: Arc<Notify>,
}

#[cfg(test)]
impl StorageEngineWriteCompletionGate {
    pub(crate) fn new() -> Self {
        Self {
            reached: Arc::new(AtomicBool::new(false)),
            released: Arc::new(AtomicBool::new(false)),
            release: Arc::new(Notify::new()),
        }
    }

    async fn pause_storage(&self) {
        self.reached.store(true, Ordering::Release);
        loop {
            let released = self.release.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            released.await;
        }
    }

    pub(crate) fn wait_until_reached(&self) {
        while !self.reached.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
    }

    pub(crate) fn release_storage(&self) {
        self.released.store(true, Ordering::Release);
        self.release.notify_waiters();
    }
}

impl Default for StorageEngineConfig {
    fn default() -> Self {
        let buffer_pool_bytes = 4 * 1024 * 1024;
        Self {
            disk_workers: 1,
            disk_queue_capacity: 8,
            disk_completion_capacity: 8,
            max_in_flight_bytes: 2 * 1024 * 1024,
            buffer_pool_bytes,
            resident_budget: ByteBudget::new(buffer_pool_bytes),
            handle_budgets: None,
            shutdown_timeout: Duration::from_secs(5),
            #[cfg(test)]
            disk_fault: None,
            #[cfg(test)]
            crash_point: None,
            #[cfg(test)]
            piece_durable_notifier: None,
            #[cfg(test)]
            write_completion_gate: None,
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
    Handle(HandleBudgetError),
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
            StorageEngineErrorDetail::Handle(error) => write!(formatter, ": {error}"),
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
    _capability_permit: Option<HandlePermit>,
    _registered_permit: Option<HandlePermit>,
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
    #[cfg(test)]
    crash_point: Option<StorageEngineCrashPoint>,
    #[cfg(test)]
    piece_durable_notifier: Option<std::sync::mpsc::Sender<PieceId>>,
    #[cfg(test)]
    write_completion_gate: Option<StorageEngineWriteCompletionGate>,
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
        let mut pool_config =
            BufferPoolConfig::new(config.buffer_pool_bytes, config.buffer_pool_bytes);
        pool_config.resident_budget = config.resident_budget.clone();
        let pool = BufferPool::new(pool_config).map_err(|error| {
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
            // Reserve both descriptors before cloning/opening the second one;
            // this keeps the native handle boundary closed even on failure.
            let (capability_permit, registered_permit) =
                if let Some(budgets) = config.handle_budgets.as_ref() {
                    let capability_permit = budgets.try_acquire_file().map_err(|error| {
                        StorageEngineError::with(
                            WriteReject::NativeFile,
                            StorageEngineErrorDetail::Handle(error),
                        )
                    })?;
                    let registered_permit = budgets.try_acquire_file().map_err(|error| {
                        StorageEngineError::with(
                            WriteReject::NativeFile,
                            StorageEngineErrorDetail::Handle(error),
                        )
                    })?;
                    (Some(capability_permit), Some(registered_permit))
                } else {
                    (None, None)
                };
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
                .insert(
                    id,
                    StorageFile {
                        capability,
                        handle,
                        _capability_permit: capability_permit,
                        _registered_permit: registered_permit,
                    },
                )
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
        let lane_result = {
            let lane_config = BlockingDiskLaneConfig {
                worker_count: config.disk_workers,
                queue_capacity: config.disk_queue_capacity,
                completion_capacity: config.disk_completion_capacity,
                max_accepted_bytes: config.max_in_flight_bytes,
            };
            #[cfg(test)]
            {
                match config.disk_fault {
                    Some(fault) => {
                        BlockingDiskLane::new(lane_config, epoch, FaultInjectingExecutor { fault })
                    }
                    None => BlockingDiskLane::new(lane_config, epoch, registry.clone()),
                }
            }
            #[cfg(not(test))]
            {
                BlockingDiskLane::new(lane_config, epoch, registry.clone())
            }
        };
        let lane = lane_result.map_err(|error| {
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
            #[cfg(test)]
            crash_point: config.crash_point,
            #[cfg(test)]
            piece_durable_notifier: config.piece_durable_notifier,
            #[cfg(test)]
            write_completion_gate: config.write_completion_gate,
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
        #[cfg(test)]
        if let Some(gate) = &self.write_completion_gate {
            gate.pause_storage().await;
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
        #[cfg(test)]
        self.crash_at(StorageEngineCrashPoint::LeaseCommitted);
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
        #[cfg(test)]
        self.crash_at(StorageEngineCrashPoint::DataSyncBeforePieceDurable);
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
        #[cfg(test)]
        self.crash_at(StorageEngineCrashPoint::PieceDurableBeforeJournalSync);
        let flushed = self
            .journal
            .flush(durable.sequence())
            .map_err(journal_error)?;
        #[cfg(test)]
        if let Some(notifier) = &self.piece_durable_notifier {
            let _notified = notifier.send(active.piece);
        }
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

    /// Persists the settled digest-only mirror identity before any range lease
    /// can be admitted.  The record is flushed independently so a process
    /// crash cannot leave durable bytes without the identity that authorizes
    /// their restart validation.
    pub fn record_http_range_identity(
        &mut self,
        total_length: u64,
        representation_digest: JournalDigest,
    ) -> Result<(), StorageEngineError> {
        let identity_fingerprint =
            calculate_http_range_identity_fingerprint(&representation_digest, total_length)
                .map_err(|_| StorageEngineError::bare(WriteReject::Journal))?;
        let appended = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::HttpRangeIdentity {
                    identity_fingerprint,
                    total_length,
                    representation_digest,
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

    #[cfg(test)]
    fn crash_at(&self, point: StorageEngineCrashPoint) {
        if self.crash_point == Some(point) {
            wait_for_forced_kill_if_requested();
            std::process::exit(match point {
                StorageEngineCrashPoint::LeaseCommitted => 97,
                StorageEngineCrashPoint::DataSyncBeforePieceDurable => 98,
                StorageEngineCrashPoint::PieceDurableBeforeJournalSync => 99,
            });
        }
    }
}

#[cfg(test)]
fn wait_for_forced_kill_if_requested() {
    let Some(ready) = std::env::var_os("ARIAX_STORAGE_CRASH_READY") else {
        return;
    };
    std::fs::write(ready, b"ready").expect("publish storage kill-ready marker");
    loop {
        std::thread::park_timeout(Duration::from_secs(1));
    }
}

#[cfg(test)]
#[derive(Clone)]
struct FaultInjectingExecutor {
    fault: StorageEngineDiskFault,
}

#[cfg(test)]
impl BlockingDiskExecutor for FaultInjectingExecutor {
    fn write_at(
        &self,
        _handle: BlockingFileHandle,
        _offset: u64,
        bytes: &[u8],
    ) -> Result<usize, BlockingDiskIoError> {
        match self.fault {
            StorageEngineDiskFault::OutOfSpace => Err(BlockingDiskIoError {
                kind: BlockingDiskIoErrorKind::OutOfSpace,
                raw_os_error: None,
            }),
            StorageEngineDiskFault::PermissionDenied => Err(BlockingDiskIoError {
                kind: BlockingDiskIoErrorKind::PermissionDenied,
                raw_os_error: None,
            }),
            StorageEngineDiskFault::ShortWrite => Ok(bytes.len().saturating_sub(1)),
        }
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
    use crate::{KnownLengthHttpRecoveryRequest, recover_known_length_http};
    use ariax_core::Gid;
    use ariax_storage::{
        JournalId, JournalStateStop, PathPlatform, RECORD_OVERHEAD, ReplayLimits,
        RootDirectoryCapability, SafePathBuilder, journal_segment_path, replay_ordered_segments,
    };
    use std::fs::{self, OpenOptions};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Instant;

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

    fn crash_journal_id() -> JournalId {
        JournalId::new([44; 16]).expect("crash journal id")
    }

    fn crash_recovery_request(directory: &Path) -> KnownLengthHttpRecoveryRequest {
        KnownLengthHttpRecoveryRequest {
            task: TaskId::new(1).expect("task"),
            gid: Gid::new(1).expect("gid"),
            journal_id: crash_journal_id(),
            generation: Generation::INITIAL,
            journal_directory: directory.join("journal"),
            output_root: directory.join("output"),
            replay_limits: Default::default(),
            state_limits: Default::default(),
        }
    }

    fn crash_engine(
        directory: &Path,
        crash_point: Option<StorageEngineCrashPoint>,
    ) -> StorageEngine {
        let output_root = directory.join("output");
        let journal_root = directory.join("journal");
        fs::create_dir_all(&output_root).expect("crash output root");
        let root =
            RootDirectoryCapability::open_trusted(&output_root).expect("crash output capability");
        let output = SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
            .expect("crash output path");
        let output_file = root.create_new_file(&output).expect("crash output file");
        output_file.set_len(4).expect("preallocate crash output");
        let layout = build_single_file_layout(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            &root,
            &output,
            &output_file,
            4,
            4,
        )
        .expect("crash layout");
        let mut journal = ControlJournalAppender::create(
            &journal_root,
            Gid::new(1).expect("gid"),
            crash_journal_id(),
            Generation::INITIAL,
            1,
        )
        .expect("crash journal");
        append_initial_admission(&mut journal, Generation::INITIAL).expect("crash admission");
        append_layout(&mut journal, &layout).expect("crash layout journal");
        StorageEngine::open_layout(
            layout,
            [(FileId::new(0), output_file)],
            journal,
            StorageEngineConfig {
                crash_point,
                ..StorageEngineConfig::default()
            },
        )
        .expect("crash storage engine")
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

    #[tokio::test]
    async fn injected_disk_faults_leave_no_durable_piece_and_replay_cleanly() {
        for (ordinal, fault) in [
            StorageEngineDiskFault::OutOfSpace,
            StorageEngineDiskFault::PermissionDenied,
            StorageEngineDiskFault::ShortWrite,
        ]
        .into_iter()
        .enumerate()
        {
            let directory = TestDirectory::new();
            let output_root = directory.0.join("output");
            let journal_root = directory.0.join("journal");
            fs::create_dir_all(&output_root).expect("output root");
            let root =
                RootDirectoryCapability::open_trusted(&output_root).expect("root capability");
            let output = SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
                .expect("path");
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
            let journal_id = JournalId::new([u8::try_from(ordinal + 3).expect("journal id"); 16])
                .expect("journal id");
            let mut journal = ControlJournalAppender::create(
                &journal_root,
                Gid::new(1).expect("gid"),
                journal_id,
                Generation::INITIAL,
                1,
            )
            .expect("journal");
            append_initial_admission(&mut journal, Generation::INITIAL).expect("admission");
            append_layout(&mut journal, &layout).expect("layout journal");
            let config = StorageEngineConfig {
                disk_fault: Some(fault),
                ..StorageEngineConfig::default()
            };
            let mut engine = StorageEngine::open_layout(
                layout,
                [(FileId::new(0), output_file)],
                journal,
                config,
            )
            .expect("storage engine");
            let lease = LeaseId::new(1).expect("lease");
            let validator = JournalHash::new([7; 32]).expect("validator");
            engine
                .begin_lease(lease_plan(
                    lease,
                    1,
                    GlobalSpan { offset: 0, len: 4 },
                    validator,
                    None,
                ))
                .expect("lease");
            let mut buffer = engine.reserve_network_buffer(4).expect("buffer");
            buffer.writable().expect("writable")[..4].copy_from_slice(&[1, 2, 3, 4]);
            buffer.mark_filled(4, OwnerTag::Storage).expect("filled");
            let error = engine
                .write_block(WriteBlock {
                    task: TaskId::new(1).expect("task"),
                    generation: Generation::INITIAL,
                    lease,
                    global_offset: 0,
                    expected_len: 4,
                    buffer,
                    piece: PieceId::new(0),
                })
                .await
                .expect_err("injected disk fault");
            assert_eq!(error.reject(), WriteReject::DiskCompletion);
            match (fault, &error.detail) {
                (
                    StorageEngineDiskFault::OutOfSpace,
                    StorageEngineErrorDetail::Disk(BlockingDiskError::Backend(error)),
                ) => assert_eq!(error.kind, BlockingDiskIoErrorKind::OutOfSpace),
                (
                    StorageEngineDiskFault::PermissionDenied,
                    StorageEngineErrorDetail::Disk(BlockingDiskError::Backend(error)),
                ) => assert_eq!(error.kind, BlockingDiskIoErrorKind::PermissionDenied),
                (
                    StorageEngineDiskFault::ShortWrite,
                    StorageEngineErrorDetail::Disk(BlockingDiskError::ShortWrite {
                        expected: 4,
                        actual: 3,
                    }),
                ) => {}
                _ => panic!("unexpected injected disk result: {error:?}"),
            }
            engine
                .abort_lease(
                    TaskId::new(1).expect("task"),
                    Generation::INITIAL,
                    lease,
                    LeaseAbortReason::Retry,
                )
                .expect("abort failed write");
            drop(
                engine
                    .into_flushed_journal()
                    .expect("close flushed journal"),
            );
            assert_eq!(
                fs::read(output_root.join("output.bin")).expect("output bytes"),
                [0, 0, 0, 0]
            );
            let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
                task: TaskId::new(1).expect("task"),
                gid: Gid::new(1).expect("gid"),
                journal_id,
                generation: Generation::INITIAL,
                journal_directory: journal_root,
                output_root,
                replay_limits: Default::default(),
                state_limits: Default::default(),
            })
            .expect("recover faulted journal");
            assert_eq!(recovered.durable_prefix, 0);
            assert!(recovered.replay.state.is_some());
        }
    }

    #[test]
    fn forced_process_crashes_preserve_only_the_piece_durable_prefix() {
        for (phase, exit_code, expected_prefix) in [
            ("after_write", 96, 0),
            ("after_lease_committed", 97, 0),
            ("after_data_sync", 98, 0),
            ("after_piece_durable_append", 99, 0),
            ("after_commit", 100, 4),
        ] {
            let directory = TestDirectory::new();
            let status = Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--ignored",
                    "--exact",
                    "storage_engine::tests::forced_process_crash_child",
                    "--nocapture",
                ])
                .env("ARIAX_STORAGE_CRASH_CHILD", &directory.0)
                .env("ARIAX_STORAGE_CRASH_PHASE", phase)
                .status()
                .expect("spawn storage crash child");
            assert_eq!(status.code(), Some(exit_code), "{phase}");
            assert_eq!(
                fs::read(directory.0.join("output/output.bin")).expect("read child output bytes"),
                [1, 2, 3, 4],
                "{phase}"
            );

            if phase == "after_piece_durable_append" {
                let journal_path = journal_segment_path(directory.0.join("journal"), 0);
                let bytes = fs::read(&journal_path).expect("read unflushed durable record");
                let replay = replay_ordered_segments(&[&bytes], ReplayLimits::default());
                let last = replay.records.last().expect("piece durable record");
                assert!(matches!(
                    last.decode_payload(),
                    Ok(JournalPayload::PieceDurable { .. })
                ));
                let lost_tail = RECORD_OVERHEAD
                    .checked_add(last.payload.len())
                    .and_then(|length| bytes.len().checked_sub(length))
                    .expect("piece durable record length");
                let file = OpenOptions::new()
                    .write(true)
                    .open(&journal_path)
                    .expect("open journal for power-loss cut");
                file.set_len(u64::try_from(lost_tail).expect("journal length fits u64"))
                    .expect("drop unflushed journal tail");
                file.sync_all().expect("persist simulated power-loss cut");
            }

            let recovered = recover_known_length_http(&crash_recovery_request(&directory.0))
                .expect("recover storage crash child");
            assert_eq!(recovered.replay.stop, JournalStateStop::CleanEnd, "{phase}");
            assert_eq!(recovered.durable_prefix, expected_prefix, "{phase}");
            assert_eq!(
                recovered
                    .replay
                    .state
                    .as_ref()
                    .expect("recovered crash state")
                    .durable_pieces()
                    .len(),
                usize::from(expected_prefix != 0),
                "{phase}"
            );
        }
    }

    #[test]
    fn forced_process_kills_cover_every_storage_durability_barrier() {
        for (phase, expected_prefix) in [
            ("after_write", 0),
            ("after_lease_committed", 0),
            ("after_data_sync", 0),
            ("after_piece_durable_append", 4),
            ("after_commit", 4),
        ] {
            let directory = TestDirectory::new();
            let ready = directory.0.join("kill-ready");
            let mut child = Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--ignored",
                    "--exact",
                    "storage_engine::tests::forced_process_crash_child",
                    "--nocapture",
                ])
                .env("ARIAX_STORAGE_CRASH_CHILD", &directory.0)
                .env("ARIAX_STORAGE_CRASH_PHASE", phase)
                .env("ARIAX_STORAGE_CRASH_READY", &ready)
                .spawn()
                .expect("spawn storage kill child");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !ready.exists() {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("storage kill child did not reach {phase} barrier");
                }
                thread::sleep(Duration::from_millis(5));
            }
            child.kill().expect("kill storage child process");
            let status = child.wait().expect("wait for killed storage child");
            assert!(!status.success(), "{phase}");

            assert_eq!(
                fs::read(directory.0.join("output/output.bin"))
                    .expect("read killed child output bytes"),
                [1, 2, 3, 4],
                "{phase}"
            );
            let recovered = recover_known_length_http(&crash_recovery_request(&directory.0))
                .expect("recover killed storage child");
            assert_eq!(recovered.replay.stop, JournalStateStop::CleanEnd, "{phase}");
            assert_eq!(recovered.durable_prefix, expected_prefix, "{phase}");
            assert_eq!(
                recovered
                    .replay
                    .state
                    .as_ref()
                    .expect("recovered killed-child state")
                    .durable_pieces()
                    .len(),
                usize::from(expected_prefix != 0),
                "{phase}"
            );
        }
    }

    #[tokio::test]
    #[ignore = "spawned by forced_process_crashes_preserve_only_the_piece_durable_prefix"]
    async fn forced_process_crash_child() {
        let Some(directory) = std::env::var_os("ARIAX_STORAGE_CRASH_CHILD") else {
            return;
        };
        let phase = std::env::var("ARIAX_STORAGE_CRASH_PHASE").expect("storage crash phase");
        let crash_point = match phase.as_str() {
            "after_lease_committed" => Some(StorageEngineCrashPoint::LeaseCommitted),
            "after_data_sync" => Some(StorageEngineCrashPoint::DataSyncBeforePieceDurable),
            "after_piece_durable_append" => {
                Some(StorageEngineCrashPoint::PieceDurableBeforeJournalSync)
            }
            "after_write" | "after_commit" => None,
            other => panic!("unknown storage crash phase: {other}"),
        };
        let mut engine = crash_engine(&PathBuf::from(directory), crash_point);
        let lease = LeaseId::new(1).expect("lease");
        let validator = JournalHash::new([7; 32]).expect("validator");
        engine
            .begin_lease(lease_plan(
                lease,
                1,
                GlobalSpan { offset: 0, len: 4 },
                validator,
                None,
            ))
            .expect("child begin lease");
        write_piece(&mut engine, lease, &[1, 2, 3, 4]).await;
        if phase == "after_write" {
            wait_for_forced_kill_if_requested();
            std::process::exit(96);
        }
        let acknowledgements = engine
            .commit_lease(LeaseCommit {
                task: TaskId::new(1).expect("task"),
                generation: Generation::INITIAL,
                lease,
                received_len: 4,
                validator,
                response_digest: None,
            })
            .expect("child commit lease");
        assert_eq!(phase, "after_commit", "crash hook failed to exit");
        assert!(
            acknowledgements
                .iter()
                .any(|ack| matches!(ack, WriteAck::PieceDurable { .. }))
        );
        wait_for_forced_kill_if_requested();
        std::process::exit(100);
    }

    #[test]
    fn profile_file_budget_rejects_before_opening_the_registered_descriptor() {
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
            JournalId::new([2; 16]).expect("journal id"),
            Generation::INITIAL,
            1,
        )
        .expect("journal");
        append_initial_admission(&mut journal, Generation::INITIAL).expect("admission");
        append_layout(&mut journal, &layout).expect("layout journal");

        // A selected output needs one permit for the capability descriptor and
        // another for the blocking-lane clone. One file-domain permit must
        // therefore reject without opening/registering the clone.
        let budgets = HandleBudgets::new(ariax_runtime::HandleBudgetLimits {
            process: 2,
            sockets: 2,
            files: 1,
        })
        .expect("handle budgets");
        let config = StorageEngineConfig {
            handle_budgets: Some(budgets.clone()),
            ..StorageEngineConfig::default()
        };
        let result =
            StorageEngine::open_layout(layout, [(FileId::new(0), output_file)], journal, config);
        assert!(matches!(
            result,
            Err(StorageEngineError {
                reject: WriteReject::NativeFile,
                ..
            })
        ));
        assert_eq!(budgets.available_files(), 1);
        assert_eq!(budgets.available_process(), 2);
    }
}
