use crate::{
    BufferLease, BufferState, BufferTransitionError, ByteBudget, BytePermit, CompletionDrain,
    CompletionPermit, OwnerTag,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::num::NonZeroU64;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TryRecvError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const CANCELLATION_QUEUED: u8 = 0;
const CANCELLATION_REQUESTED: u8 = 1;
const CANCELLATION_RUNNING: u8 = 2;
const CANCELLATION_COMPLETE: u8 = 3;

/// Hard implementation limit for one blocking disk worker set.
pub const MAX_BLOCKING_DISK_WORKERS: usize = 256;
/// Hard implementation limit for queued blocking disk operations.
pub const MAX_BLOCKING_DISK_QUEUE_CAPACITY: usize = 1_048_576;
/// Hard implementation limit for reserved blocking disk completions.
pub const MAX_BLOCKING_DISK_COMPLETION_CAPACITY: usize = 1_048_576;
/// Hard ceiling applied to an explicit blocking-lane shutdown timeout.
pub const MAX_BLOCKING_DISK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(300);
/// Drop never waits for a blocking executor; it joins only workers already exited.
pub const BLOCKING_DISK_DROP_TIMEOUT: Duration = Duration::ZERO;

/// One process-local generation of blocking disk handles and operations.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BlockingBackendEpoch(NonZeroU64);

impl BlockingBackendEpoch {
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Stable correlation identity for one accepted blocking disk operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BlockingDiskOperationId(NonZeroU64);

impl BlockingDiskOperationId {
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Opaque authority for one already-open file in one backend epoch.
///
/// The lane deliberately stores no path. The executor interprets `id` only
/// after a secure open layer has minted and registered this handle.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BlockingFileHandle {
    id: NonZeroU64,
    backend_epoch: BlockingBackendEpoch,
    authorized_len: u64,
}

impl BlockingFileHandle {
    // The secure-open adapter will use this crate-private minting point. Keeping
    // it non-public prevents callers from forging an authority from a raw id.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) const fn new(
        id: NonZeroU64,
        backend_epoch: BlockingBackendEpoch,
        authorized_len: u64,
    ) -> Self {
        Self {
            id,
            backend_epoch,
            authorized_len,
        }
    }

    #[must_use]
    pub const fn id(self) -> NonZeroU64 {
        self.id
    }

    #[must_use]
    pub const fn backend_epoch(self) -> BlockingBackendEpoch {
        self.backend_epoch
    }

    #[must_use]
    pub const fn authorized_len(self) -> u64 {
        self.authorized_len
    }
}

/// Cloneable registry that mints opaque blocking-file authority only for
/// already-open native files in one backend epoch.
///
/// The registry stores descriptors rather than paths. Removing a registration
/// prevents later operations from resolving it; a worker that already cloned
/// the descriptor remains safe to finish its positional write.
#[derive(Clone)]
pub struct BlockingFileRegistry {
    backend_epoch: BlockingBackendEpoch,
    inner: Arc<Mutex<BlockingFileRegistryState>>,
}

struct BlockingFileRegistryState {
    next_id: u64,
    files: HashMap<NonZeroU64, Arc<File>>,
}

impl BlockingFileRegistry {
    #[must_use]
    pub fn new(backend_epoch: BlockingBackendEpoch) -> Self {
        Self {
            backend_epoch,
            inner: Arc::new(Mutex::new(BlockingFileRegistryState {
                next_id: 1,
                files: HashMap::new(),
            })),
        }
    }

    #[must_use]
    pub const fn backend_epoch(&self) -> BlockingBackendEpoch {
        self.backend_epoch
    }

    pub fn register(
        &self,
        file: File,
        authorized_len: u64,
    ) -> Result<BlockingFileHandle, BlockingFileRegistryError> {
        let mut state = registry_lock(&self.inner);
        let id =
            NonZeroU64::new(state.next_id).ok_or(BlockingFileRegistryError::IdentifierExhausted)?;
        state
            .files
            .try_reserve(1)
            .map_err(|_| BlockingFileRegistryError::AllocationFailed)?;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or(BlockingFileRegistryError::IdentifierExhausted)?;
        let replaced = state.files.insert(id, Arc::new(file));
        debug_assert!(replaced.is_none(), "monotonic file ids do not collide");
        Ok(BlockingFileHandle::new(
            id,
            self.backend_epoch,
            authorized_len,
        ))
    }

    pub fn unregister(&self, handle: BlockingFileHandle) -> Result<(), BlockingFileRegistryError> {
        if handle.backend_epoch != self.backend_epoch {
            return Err(BlockingFileRegistryError::BackendEpochMismatch {
                registry: self.backend_epoch,
                handle: handle.backend_epoch,
            });
        }
        let removed = registry_lock(&self.inner).files.remove(&handle.id);
        if removed.is_none() {
            return Err(BlockingFileRegistryError::NotRegistered);
        }
        Ok(())
    }

    #[must_use]
    pub fn registered_count(&self) -> usize {
        registry_lock(&self.inner).files.len()
    }

    fn resolve(&self, handle: BlockingFileHandle) -> Option<Arc<File>> {
        if handle.backend_epoch != self.backend_epoch {
            return None;
        }
        registry_lock(&self.inner).files.get(&handle.id).cloned()
    }
}

impl fmt::Debug for BlockingFileRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingFileRegistry")
            .field("backend_epoch", &self.backend_epoch)
            .field("registered_count", &self.registered_count())
            .finish_non_exhaustive()
    }
}

impl BlockingDiskExecutor for BlockingFileRegistry {
    fn write_at(
        &self,
        handle: BlockingFileHandle,
        offset: u64,
        bytes: &[u8],
    ) -> Result<usize, BlockingDiskIoError> {
        let file = self.resolve(handle).ok_or(BlockingDiskIoError {
            kind: BlockingDiskIoErrorKind::InvalidHandle,
            raw_os_error: None,
        })?;
        positional_write(&file, bytes, offset).map_err(classify_io_error)
    }
}

/// Why an opened file could not be installed in the opaque handle registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingFileRegistryError {
    AllocationFailed,
    IdentifierExhausted,
    BackendEpochMismatch {
        registry: BlockingBackendEpoch,
        handle: BlockingBackendEpoch,
    },
    NotRegistered,
}

impl fmt::Display for BlockingFileRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllocationFailed => {
                formatter.write_str("blocking file registry allocation failed")
            }
            Self::IdentifierExhausted => {
                formatter.write_str("blocking file registry identifier exhausted")
            }
            Self::BackendEpochMismatch { registry, handle } => write!(
                formatter,
                "blocking file handle epoch {} does not match registry epoch {}",
                handle.get(),
                registry.get()
            ),
            Self::NotRegistered => formatter.write_str("blocking file handle is not registered"),
        }
    }
}

impl Error for BlockingFileRegistryError {}

#[cfg(unix)]
fn positional_write(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, bytes, offset)
}

#[cfg(windows)]
fn positional_write(file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, bytes, offset)
}

#[cfg(not(any(unix, windows)))]
fn positional_write(_file: &File, _bytes: &[u8], _offset: u64) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native positional file writes are unavailable",
    ))
}

fn classify_io_error(error: io::Error) -> BlockingDiskIoError {
    let kind = match error.kind() {
        io::ErrorKind::NotFound => BlockingDiskIoErrorKind::NotFound,
        io::ErrorKind::PermissionDenied => BlockingDiskIoErrorKind::PermissionDenied,
        io::ErrorKind::StorageFull => BlockingDiskIoErrorKind::OutOfSpace,
        io::ErrorKind::QuotaExceeded => BlockingDiskIoErrorKind::QuotaExceeded,
        io::ErrorKind::Interrupted => BlockingDiskIoErrorKind::Interrupted,
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
            BlockingDiskIoErrorKind::InvalidHandle
        }
        _ => BlockingDiskIoErrorKind::Other,
    };
    BlockingDiskIoError {
        kind,
        raw_os_error: error.raw_os_error(),
    }
}

fn registry_lock(
    mutex: &Mutex<BlockingFileRegistryState>,
) -> MutexGuard<'_, BlockingFileRegistryState> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One explicit operation supported by the first blocking-lane slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingDiskOperation {
    WriteAt {
        handle: BlockingFileHandle,
        offset: u64,
        expected_len: usize,
    },
}

impl BlockingDiskOperation {
    #[must_use]
    pub const fn expected_len(self) -> usize {
        match self {
            Self::WriteAt { expected_len, .. } => expected_len,
        }
    }
}

/// Cloneable user handle for cancelling one blocking disk operation.
#[derive(Clone)]
pub struct BlockingDiskCancelHandle {
    state: Arc<AtomicU8>,
}

impl BlockingDiskCancelHandle {
    /// Mint one cloneable handle and its only submission registration.
    #[must_use]
    pub fn pair() -> (Self, BlockingDiskCancellationRegistration) {
        let state = Arc::new(AtomicU8::new(CANCELLATION_QUEUED));
        (
            Self {
                state: Arc::clone(&state),
            },
            BlockingDiskCancellationRegistration { state },
        )
    }

    /// Request cancellation while the operation is still queued.
    #[must_use]
    pub fn cancel(&self) -> BlockingDiskCancelResult {
        loop {
            match self.state.load(Ordering::Acquire) {
                CANCELLATION_QUEUED => {
                    if self
                        .state
                        .compare_exchange_weak(
                            CANCELLATION_QUEUED,
                            CANCELLATION_REQUESTED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return BlockingDiskCancelResult::Requested;
                    }
                }
                CANCELLATION_REQUESTED => {
                    return BlockingDiskCancelResult::AlreadyRequested;
                }
                CANCELLATION_RUNNING => return BlockingDiskCancelResult::TooLate,
                CANCELLATION_COMPLETE => return BlockingDiskCancelResult::Complete,
                _ => unreachable!("cancellation state is private and bounded"),
            }
        }
    }
}

impl fmt::Debug for BlockingDiskCancelHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingDiskCancelHandle")
            .field("state", &self.state.load(Ordering::Acquire))
            .finish()
    }
}

/// Non-cloneable authority for one accepted blocking disk operation.
///
/// Rejected submissions retain this registration and may be retried, but safe
/// code cannot copy it into a second submission:
///
/// ```compile_fail
/// use ariax_runtime::BlockingDiskCancelHandle;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// let (_handle, registration) = BlockingDiskCancelHandle::pair();
/// require_clone(&registration);
/// ```
pub struct BlockingDiskCancellationRegistration {
    state: Arc<AtomicU8>,
}

impl BlockingDiskCancellationRegistration {
    fn claim_for_worker(&self) -> bool {
        loop {
            match self.state.load(Ordering::Acquire) {
                CANCELLATION_QUEUED => {
                    if self
                        .state
                        .compare_exchange_weak(
                            CANCELLATION_QUEUED,
                            CANCELLATION_RUNNING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return true;
                    }
                }
                CANCELLATION_REQUESTED => {
                    self.state.store(CANCELLATION_COMPLETE, Ordering::Release);
                    return false;
                }
                CANCELLATION_RUNNING | CANCELLATION_COMPLETE => return false,
                _ => unreachable!("cancellation state is private and bounded"),
            }
        }
    }

    fn finish(&self) {
        self.state.store(CANCELLATION_COMPLETE, Ordering::Release);
    }
}

impl fmt::Debug for BlockingDiskCancellationRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingDiskCancellationRegistration")
            .field("state", &self.state.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for BlockingDiskCancellationRegistration {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Observable result of a cancellation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingDiskCancelResult {
    Requested,
    AlreadyRequested,
    TooLate,
    Complete,
}

/// Move-only write submission. Rejection returns this value intact.
pub struct BlockingDiskSubmission {
    operation_id: BlockingDiskOperationId,
    backend_epoch: BlockingBackendEpoch,
    operation: BlockingDiskOperation,
    cancellation: BlockingDiskCancellationRegistration,
    lease: BufferLease,
}

impl BlockingDiskSubmission {
    #[must_use]
    pub fn new(
        operation_id: BlockingDiskOperationId,
        backend_epoch: BlockingBackendEpoch,
        operation: BlockingDiskOperation,
        cancellation: BlockingDiskCancellationRegistration,
        lease: BufferLease,
    ) -> Self {
        Self {
            operation_id,
            backend_epoch,
            operation,
            cancellation,
            lease,
        }
    }

    #[must_use]
    pub const fn operation_id(&self) -> BlockingDiskOperationId {
        self.operation_id
    }

    #[must_use]
    pub const fn backend_epoch(&self) -> BlockingBackendEpoch {
        self.backend_epoch
    }

    #[must_use]
    pub const fn operation(&self) -> BlockingDiskOperation {
        self.operation
    }

    #[must_use]
    pub fn cancellation_registration(&self) -> &BlockingDiskCancellationRegistration {
        &self.cancellation
    }

    #[must_use]
    pub fn lease(&self) -> &BufferLease {
        &self.lease
    }

    #[must_use]
    pub fn into_lease(self) -> BufferLease {
        self.lease
    }
}

impl fmt::Debug for BlockingDiskSubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingDiskSubmission")
            .field("operation_id", &self.operation_id)
            .field("backend_epoch", &self.backend_epoch)
            .field("operation", &self.operation)
            .field("cancellation", &self.cancellation)
            .field("lease", &self.lease)
            .finish()
    }
}

/// Fixed sizing for one blocking lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockingDiskLaneConfig {
    pub worker_count: usize,
    pub queue_capacity: usize,
    pub completion_capacity: usize,
    pub max_accepted_bytes: usize,
}

/// Why a fixed blocking lane could not be started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingDiskLaneStartError {
    ZeroWorkers,
    ZeroQueueCapacity,
    ZeroCompletionCapacity,
    ZeroByteCapacity,
    WorkerCountTooLarge {
        requested: usize,
        maximum: usize,
    },
    QueueCapacityTooLarge {
        requested: usize,
        maximum: usize,
    },
    CompletionCapacityTooLarge {
        requested: usize,
        maximum: usize,
    },
    AllocationFailed {
        resource: BlockingDiskLaneResource,
    },
    WorkerSpawn {
        worker: usize,
        kind: std::io::ErrorKind,
    },
}

/// Which bounded lane resource could not reserve its configured capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingDiskLaneResource {
    CompletionQueue,
    SubmissionQueue,
    AcceptedOperationSet,
    WorkerHandleSet,
}

impl fmt::Display for BlockingDiskLaneStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroWorkers => formatter.write_str("blocking disk worker count must be nonzero"),
            Self::ZeroQueueCapacity => {
                formatter.write_str("blocking disk queue capacity must be nonzero")
            }
            Self::ZeroCompletionCapacity => {
                formatter.write_str("blocking disk completion capacity must be nonzero")
            }
            Self::ZeroByteCapacity => {
                formatter.write_str("blocking disk byte capacity must be nonzero")
            }
            Self::WorkerCountTooLarge { requested, maximum } => write!(
                formatter,
                "blocking disk worker count {requested} exceeds maximum {maximum}"
            ),
            Self::QueueCapacityTooLarge { requested, maximum } => write!(
                formatter,
                "blocking disk queue capacity {requested} exceeds maximum {maximum}"
            ),
            Self::CompletionCapacityTooLarge { requested, maximum } => write!(
                formatter,
                "blocking disk completion capacity {requested} exceeds maximum {maximum}"
            ),
            Self::AllocationFailed { resource } => {
                write!(formatter, "blocking disk {resource:?} allocation failed")
            }
            Self::WorkerSpawn { worker, kind } => {
                write!(
                    formatter,
                    "blocking disk worker {worker} failed to spawn: {kind:?}"
                )
            }
        }
    }
}

impl Error for BlockingDiskLaneStartError {}

/// Stable, non-secret classification of a concrete blocking I/O failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingDiskIoErrorKind {
    NotFound,
    PermissionDenied,
    OutOfSpace,
    QuotaExceeded,
    Interrupted,
    InvalidHandle,
    Other,
}

/// Error returned by the platform-specific positional-write executor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockingDiskIoError {
    pub kind: BlockingDiskIoErrorKind,
    pub raw_os_error: Option<i32>,
}

impl fmt::Display for BlockingDiskIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.raw_os_error {
            Some(code) => write!(formatter, "blocking disk {:?} error ({code})", self.kind),
            None => write!(formatter, "blocking disk {:?} error", self.kind),
        }
    }
}

impl Error for BlockingDiskIoError {}

/// Safe positional-write adapter run by fixed blocking workers.
///
/// Implementations map an opaque handle id to an already-open OS handle. They
/// must not reinterpret it as a path or use a shared mutable file cursor.
pub trait BlockingDiskExecutor: Send + Sync + 'static {
    fn write_at(
        &self,
        handle: BlockingFileHandle,
        offset: u64,
        bytes: &[u8],
    ) -> Result<usize, BlockingDiskIoError>;
}

/// Confirmed successful result for one positional write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockingDiskCompletion {
    pub bytes_written: usize,
}

/// Terminal failure for one accepted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingDiskError {
    Cancelled,
    ShortWrite { expected: usize, actual: usize },
    InvalidCompletionLength { expected: usize, actual: usize },
    Backend(BlockingDiskIoError),
    BufferTransition(BufferTransitionError),
    ExecutorPanicked,
    WorkerAborted,
}

impl fmt::Display for BlockingDiskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("blocking disk operation was cancelled"),
            Self::ShortWrite { expected, actual } => {
                write!(
                    formatter,
                    "short write: expected {expected} bytes, wrote {actual}"
                )
            }
            Self::InvalidCompletionLength { expected, actual } => write!(
                formatter,
                "invalid write completion: expected at most {expected} bytes, reported {actual}"
            ),
            Self::Backend(error) => error.fmt(formatter),
            Self::BufferTransition(error) => error.fmt(formatter),
            Self::ExecutorPanicked => formatter.write_str("blocking disk executor panicked"),
            Self::WorkerAborted => formatter.write_str("blocking disk worker aborted"),
        }
    }
}

impl Error for BlockingDiskError {}

/// Exactly one terminal result for one accepted operation.
pub struct BlockingDiskOutcome {
    operation_id: BlockingDiskOperationId,
    backend_epoch: BlockingBackendEpoch,
    operation: BlockingDiskOperation,
    result: Result<BlockingDiskCompletion, BlockingDiskError>,
    lease: Option<BufferLease>,
    accepted_bytes: Option<BytePermit>,
}

impl BlockingDiskOutcome {
    #[must_use]
    pub const fn operation_id(&self) -> BlockingDiskOperationId {
        self.operation_id
    }

    #[must_use]
    pub const fn backend_epoch(&self) -> BlockingBackendEpoch {
        self.backend_epoch
    }

    #[must_use]
    pub const fn operation(&self) -> BlockingDiskOperation {
        self.operation
    }

    pub const fn result(&self) -> &Result<BlockingDiskCompletion, BlockingDiskError> {
        &self.result
    }

    #[must_use]
    pub fn lease(&self) -> &BufferLease {
        self.lease.as_ref().expect("live outcome owns one lease")
    }

    pub fn into_parts(
        mut self,
    ) -> (
        BlockingDiskOperationId,
        BlockingBackendEpoch,
        BlockingDiskOperation,
        Result<BlockingDiskCompletion, BlockingDiskError>,
        BufferLease,
    ) {
        let lease = self.lease.take().expect("live outcome owns one lease");
        let _accepted_bytes = self.accepted_bytes.take();
        (
            self.operation_id,
            self.backend_epoch,
            self.operation,
            self.result,
            lease,
        )
    }
}

impl fmt::Debug for BlockingDiskOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingDiskOutcome")
            .field("operation_id", &self.operation_id)
            .field("backend_epoch", &self.backend_epoch)
            .field("operation", &self.operation)
            .field("result", &self.result)
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

/// Why admission rejected a submission before ownership changed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingDiskSubmitErrorKind {
    ReactorContext,
    ShuttingDown,
    QueueFull,
    CompletionFull,
    ByteLimit {
        requested: usize,
        available: usize,
    },
    DuplicateOperation(BlockingDiskOperationId),
    BackendEpochMismatch {
        lane: BlockingBackendEpoch,
        submitted: BlockingBackendEpoch,
    },
    HandleEpochMismatch {
        submitted: BlockingBackendEpoch,
        handle: BlockingBackendEpoch,
    },
    ZeroLength,
    LengthMismatch {
        expected: usize,
        actual: usize,
    },
    LengthOutOfRange {
        length: usize,
    },
    OffsetOverflow {
        offset: u64,
        length: usize,
    },
    OutsideAuthorizedSpan {
        end: u64,
        authorized_len: u64,
    },
    InvalidLeaseState(BufferState),
    BufferTransition(BufferTransitionError),
}

impl fmt::Display for BlockingDiskSubmitErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReactorContext => {
                formatter.write_str("blocking disk submission from reactor context")
            }
            Self::ShuttingDown => formatter.write_str("blocking disk lane is shutting down"),
            Self::QueueFull => formatter.write_str("blocking disk submission queue is full"),
            Self::CompletionFull => {
                formatter.write_str("blocking disk completion capacity is full")
            }
            Self::ByteLimit {
                requested,
                available,
            } => write!(
                formatter,
                "blocking disk byte capacity exhausted: requested {requested}, available {available}"
            ),
            Self::DuplicateOperation(operation) => {
                write!(formatter, "duplicate blocking disk operation {operation:?}")
            }
            Self::BackendEpochMismatch { lane, submitted } => write!(
                formatter,
                "blocking disk epoch mismatch: lane {lane:?}, submitted {submitted:?}"
            ),
            Self::HandleEpochMismatch { submitted, handle } => write!(
                formatter,
                "blocking disk handle epoch mismatch: submitted {submitted:?}, handle {handle:?}"
            ),
            Self::ZeroLength => formatter.write_str("zero-length disk writes are invalid"),
            Self::LengthMismatch { expected, actual } => write!(
                formatter,
                "disk write length mismatch: expected {expected}, lease contains {actual}"
            ),
            Self::LengthOutOfRange { length } => {
                write!(formatter, "disk write length {length} does not fit u64")
            }
            Self::OffsetOverflow { offset, length } => write!(
                formatter,
                "disk write range overflows: offset {offset}, length {length}"
            ),
            Self::OutsideAuthorizedSpan {
                end,
                authorized_len,
            } => write!(
                formatter,
                "disk write ends at {end}, beyond authorized length {authorized_len}"
            ),
            Self::InvalidLeaseState(state) => {
                write!(
                    formatter,
                    "buffer state {state:?} cannot enter the disk queue"
                )
            }
            Self::BufferTransition(error) => error.fmt(formatter),
        }
    }
}

impl Error for BlockingDiskSubmitErrorKind {}

/// Admission failure retaining the exact move-only submission and lease.
pub struct BlockingDiskSubmitError {
    reason: BlockingDiskSubmitErrorKind,
    submission: Box<BlockingDiskSubmission>,
}

impl BlockingDiskSubmitError {
    fn new(reason: BlockingDiskSubmitErrorKind, submission: BlockingDiskSubmission) -> Self {
        Self {
            reason,
            submission: Box::new(submission),
        }
    }

    #[must_use]
    pub const fn reason(&self) -> BlockingDiskSubmitErrorKind {
        self.reason
    }

    #[must_use]
    pub fn submission(&self) -> &BlockingDiskSubmission {
        &self.submission
    }

    #[must_use]
    pub fn into_parts(self) -> (BlockingDiskSubmitErrorKind, BlockingDiskSubmission) {
        (self.reason, *self.submission)
    }
}

impl fmt::Debug for BlockingDiskSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingDiskSubmitError")
            .field("reason", &self.reason)
            .field("submission", &self.submission)
            .finish()
    }
}

impl fmt::Display for BlockingDiskSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "blocking disk submission rejected: {}",
            self.reason
        )
    }
}

impl Error for BlockingDiskSubmitError {}

/// Bounded point-in-time lane accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockingDiskLaneMetrics {
    pub worker_count: usize,
    pub queue_len: usize,
    pub queue_capacity: usize,
    pub in_flight: usize,
    pub accepted_bytes: usize,
    pub byte_capacity: usize,
    pub accepted: u64,
    pub completed: u64,
    pub received: u64,
    pub cancelled: u64,
    pub rejected: u64,
    pub peak_queue_len: usize,
    pub accepting: bool,
}

/// Result of explicit drain-then-close shutdown.
pub struct BlockingDiskShutdown {
    outcomes: Vec<BlockingDiskOutcome>,
    worker_panics: usize,
    detached_workers: usize,
    in_flight_at_timeout: usize,
    completion_closed: bool,
}

impl BlockingDiskShutdown {
    #[must_use]
    pub fn outcomes(&self) -> &[BlockingDiskOutcome] {
        &self.outcomes
    }

    #[must_use]
    pub fn worker_panics(&self) -> usize {
        self.worker_panics
    }

    #[must_use]
    pub fn detached_workers(&self) -> usize {
        self.detached_workers
    }

    #[must_use]
    pub fn in_flight_at_timeout(&self) -> usize {
        self.in_flight_at_timeout
    }

    #[must_use]
    pub fn completion_closed(&self) -> bool {
        self.completion_closed
    }

    #[must_use]
    pub fn into_outcomes(self) -> Vec<BlockingDiskOutcome> {
        self.outcomes
    }
}

impl fmt::Debug for BlockingDiskShutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingDiskShutdown")
            .field("outcome_count", &self.outcomes.len())
            .field("worker_panics", &self.worker_panics)
            .field("detached_workers", &self.detached_workers)
            .field("in_flight_at_timeout", &self.in_flight_at_timeout)
            .field("completion_closed", &self.completion_closed)
            .finish()
    }
}

/// Fixed-worker, bounded, ownership-preserving blocking write lane.
pub struct BlockingDiskLane {
    backend_epoch: BlockingBackendEpoch,
    shared: Arc<BlockingDiskShared>,
    completion: CompletionDrain<BlockingDiskOutcome>,
    accepted_bytes: ByteBudget,
    workers: Vec<BlockingDiskWorker>,
    worker_exits: Mutex<Receiver<usize>>,
}

impl BlockingDiskLane {
    pub fn new<E>(
        config: BlockingDiskLaneConfig,
        backend_epoch: BlockingBackendEpoch,
        executor: E,
    ) -> Result<Self, BlockingDiskLaneStartError>
    where
        E: BlockingDiskExecutor,
    {
        if config.worker_count == 0 {
            return Err(BlockingDiskLaneStartError::ZeroWorkers);
        }
        if config.queue_capacity == 0 {
            return Err(BlockingDiskLaneStartError::ZeroQueueCapacity);
        }
        if config.completion_capacity == 0 {
            return Err(BlockingDiskLaneStartError::ZeroCompletionCapacity);
        }
        if config.max_accepted_bytes == 0 {
            return Err(BlockingDiskLaneStartError::ZeroByteCapacity);
        }
        if config.worker_count > MAX_BLOCKING_DISK_WORKERS {
            return Err(BlockingDiskLaneStartError::WorkerCountTooLarge {
                requested: config.worker_count,
                maximum: MAX_BLOCKING_DISK_WORKERS,
            });
        }
        if config.queue_capacity > MAX_BLOCKING_DISK_QUEUE_CAPACITY {
            return Err(BlockingDiskLaneStartError::QueueCapacityTooLarge {
                requested: config.queue_capacity,
                maximum: MAX_BLOCKING_DISK_QUEUE_CAPACITY,
            });
        }
        if config.completion_capacity > MAX_BLOCKING_DISK_COMPLETION_CAPACITY {
            return Err(BlockingDiskLaneStartError::CompletionCapacityTooLarge {
                requested: config.completion_capacity,
                maximum: MAX_BLOCKING_DISK_COMPLETION_CAPACITY,
            });
        }

        let completion = CompletionDrain::try_new(config.completion_capacity).map_err(|_| {
            BlockingDiskLaneStartError::AllocationFailed {
                resource: BlockingDiskLaneResource::CompletionQueue,
            }
        })?;
        let accepted_bytes = ByteBudget::new(config.max_accepted_bytes);
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(config.queue_capacity)
            .map_err(|_| BlockingDiskLaneStartError::AllocationFailed {
                resource: BlockingDiskLaneResource::SubmissionQueue,
            })?;
        let mut accepted_operations = HashSet::new();
        accepted_operations
            .try_reserve(config.completion_capacity)
            .map_err(|_| BlockingDiskLaneStartError::AllocationFailed {
                resource: BlockingDiskLaneResource::AcceptedOperationSet,
            })?;
        let mut workers = Vec::new();
        workers
            .try_reserve_exact(config.worker_count)
            .map_err(|_| BlockingDiskLaneStartError::AllocationFailed {
                resource: BlockingDiskLaneResource::WorkerHandleSet,
            })?;
        let (exit_sender, worker_exits) = sync_channel(config.worker_count);
        let shared = Arc::new(BlockingDiskShared {
            state: Mutex::new(BlockingDiskState {
                queue,
                accepted_operations,
                queue_capacity: config.queue_capacity,
                accepting: true,
                stopping: false,
                in_flight: 0,
                accepted: 0,
                completed: 0,
                received: 0,
                cancelled: 0,
                rejected: 0,
                peak_queue_len: 0,
            }),
            available: Condvar::new(),
        });
        let executor: Arc<dyn BlockingDiskExecutor> = Arc::new(executor);
        for worker in 0..config.worker_count {
            let worker_shared = Arc::clone(&shared);
            let worker_executor = Arc::clone(&executor);
            let worker_exit_sender = exit_sender.clone();
            let spawn = thread::Builder::new()
                .name(format!("ariax-disk-{worker}"))
                .spawn(move || {
                    let _exit_reporter = WorkerExitReporter {
                        worker,
                        sender: worker_exit_sender,
                    };
                    worker_main(worker_shared, worker_executor);
                });
            match spawn {
                Ok(handle) => workers.push(BlockingDiskWorker {
                    handle: Some(handle),
                    exit_reported: false,
                }),
                Err(error) => {
                    {
                        let mut state = shared.lock();
                        state.accepting = false;
                        state.stopping = true;
                    }
                    shared.available.notify_all();
                    drop(exit_sender);
                    for mut worker in workers {
                        if let Some(handle) = worker.handle.take() {
                            let _ = handle.join();
                        }
                    }
                    return Err(BlockingDiskLaneStartError::WorkerSpawn {
                        worker,
                        kind: error.kind(),
                    });
                }
            }
        }
        drop(exit_sender);

        Ok(Self {
            backend_epoch,
            shared,
            completion,
            accepted_bytes,
            workers,
            worker_exits: Mutex::new(worker_exits),
        })
    }

    /// Attempt one nonblocking submission. Every failure retains the lease.
    pub fn try_submit(
        &self,
        mut submission: BlockingDiskSubmission,
    ) -> Result<(), BlockingDiskSubmitError> {
        if in_test_reactor_context() {
            self.record_rejection();
            return Err(BlockingDiskSubmitError::new(
                BlockingDiskSubmitErrorKind::ReactorContext,
                submission,
            ));
        }
        if let Err(reason) = self.validate_submission(&submission) {
            self.record_rejection();
            return Err(BlockingDiskSubmitError::new(reason, submission));
        }
        if !self.shared.lock().accepting {
            self.record_rejection();
            return Err(BlockingDiskSubmitError::new(
                BlockingDiskSubmitErrorKind::ShuttingDown,
                submission,
            ));
        }

        let expected_len = submission.operation.expected_len();
        let byte_permit = match self.accepted_bytes.try_acquire(expected_len) {
            Ok(permit) => permit,
            Err(_) => {
                self.record_rejection();
                return Err(BlockingDiskSubmitError::new(
                    BlockingDiskSubmitErrorKind::ByteLimit {
                        requested: expected_len,
                        available: self.accepted_bytes.available(),
                    },
                    submission,
                ));
            }
        };
        let Some(completion_permit) = self.completion.try_reserve() else {
            drop(byte_permit);
            self.record_rejection();
            return Err(BlockingDiskSubmitError::new(
                BlockingDiskSubmitErrorKind::CompletionFull,
                submission,
            ));
        };

        let mut state = self.shared.lock();
        let rejection = if !state.accepting {
            Some(BlockingDiskSubmitErrorKind::ShuttingDown)
        } else if state.queue.len() >= state.queue_capacity {
            Some(BlockingDiskSubmitErrorKind::QueueFull)
        } else if state.accepted_operations.contains(&submission.operation_id) {
            Some(BlockingDiskSubmitErrorKind::DuplicateOperation(
                submission.operation_id,
            ))
        } else {
            None
        };
        if let Some(reason) = rejection {
            state.rejected += 1;
            drop(state);
            completion_permit.reject();
            drop(byte_permit);
            return Err(BlockingDiskSubmitError::new(reason, submission));
        }

        if let Err(error) = submission
            .lease
            .transition(BufferState::DiskQueued, OwnerTag::Disk)
        {
            state.rejected += 1;
            drop(state);
            completion_permit.reject();
            drop(byte_permit);
            return Err(BlockingDiskSubmitError::new(
                BlockingDiskSubmitErrorKind::BufferTransition(error),
                submission,
            ));
        }

        let operation_id = submission.operation_id;
        state.accepted_operations.insert(operation_id);
        state.queue.push_back(AcceptedWork {
            operation_id,
            backend_epoch: submission.backend_epoch,
            operation: submission.operation,
            cancellation: submission.cancellation,
            lease: Some(submission.lease),
            completion_permit: Some(completion_permit),
            byte_permit: Some(byte_permit),
        });
        state.accepted += 1;
        state.peak_queue_len = state.peak_queue_len.max(state.queue.len());
        drop(state);
        self.shared.available.notify_one();
        Ok(())
    }

    /// Receive one terminal result without blocking.
    pub fn try_recv(&self) -> Option<BlockingDiskOutcome> {
        let outcome = self.completion.try_recv()?;
        let mut state = self.shared.lock();
        let removed = state.accepted_operations.remove(&outcome.operation_id);
        debug_assert!(removed, "accepted operation identity must remain reserved");
        state.received += 1;
        drop(state);
        Some(outcome)
    }

    /// Stop new submissions and return every not-yet-claimed item as aborted.
    pub fn begin_shutdown(&self) -> bool {
        let (changed, queued) = {
            let mut state = self.shared.lock();
            let changed = state.accepting;
            state.accepting = false;
            state.stopping = true;
            let queued = std::mem::take(&mut state.queue);
            state.completed = state.completed.saturating_add(
                u64::try_from(queued.len()).expect("bounded queue length fits u64"),
            );
            (changed, queued)
        };
        self.completion.close_admission();
        self.shared.available.notify_all();
        drop(queued);
        changed
    }

    #[must_use]
    pub fn metrics(&self) -> BlockingDiskLaneMetrics {
        let state = self.shared.lock();
        BlockingDiskLaneMetrics {
            worker_count: self.workers.len(),
            queue_len: state.queue.len(),
            queue_capacity: state.queue_capacity,
            in_flight: state.in_flight,
            accepted_bytes: self.accepted_bytes.used(),
            byte_capacity: self.accepted_bytes.limit(),
            accepted: state.accepted,
            completed: state.completed,
            received: state.received,
            cancelled: state.cancelled,
            rejected: state.rejected,
            peak_queue_len: state.peak_queue_len,
            accepting: state.accepting,
        }
    }

    /// Stop admission and wait up to `timeout` for already-running workers.
    #[must_use]
    pub fn shutdown(mut self, timeout: Duration) -> BlockingDiskShutdown {
        self.begin_shutdown();
        let worker_drain = self.drain_workers(timeout);
        let mut outcomes = Vec::with_capacity(self.completion.metrics().len);
        while let Some(outcome) = self.try_recv() {
            outcomes.push(outcome);
        }
        let drain_closed = self.completion.finish_close();
        let completion_closed = worker_drain.detached_workers == 0
            && worker_drain.in_flight_at_timeout == 0
            && drain_closed;
        BlockingDiskShutdown {
            outcomes,
            worker_panics: worker_drain.worker_panics,
            detached_workers: worker_drain.detached_workers,
            in_flight_at_timeout: worker_drain.in_flight_at_timeout,
            completion_closed,
        }
    }

    fn validate_submission(
        &self,
        submission: &BlockingDiskSubmission,
    ) -> Result<(), BlockingDiskSubmitErrorKind> {
        if submission.backend_epoch != self.backend_epoch {
            return Err(BlockingDiskSubmitErrorKind::BackendEpochMismatch {
                lane: self.backend_epoch,
                submitted: submission.backend_epoch,
            });
        }
        let BlockingDiskOperation::WriteAt {
            handle,
            offset,
            expected_len,
        } = submission.operation;
        if handle.backend_epoch != submission.backend_epoch {
            return Err(BlockingDiskSubmitErrorKind::HandleEpochMismatch {
                submitted: submission.backend_epoch,
                handle: handle.backend_epoch,
            });
        }
        if expected_len == 0 {
            return Err(BlockingDiskSubmitErrorKind::ZeroLength);
        }
        if expected_len != submission.lease.len() {
            return Err(BlockingDiskSubmitErrorKind::LengthMismatch {
                expected: expected_len,
                actual: submission.lease.len(),
            });
        }
        if !submission
            .lease
            .state()
            .can_transition_to(BufferState::DiskQueued)
        {
            return Err(BlockingDiskSubmitErrorKind::InvalidLeaseState(
                submission.lease.state(),
            ));
        }
        let length = u64::try_from(expected_len).map_err(|_| {
            BlockingDiskSubmitErrorKind::LengthOutOfRange {
                length: expected_len,
            }
        })?;
        let end =
            offset
                .checked_add(length)
                .ok_or(BlockingDiskSubmitErrorKind::OffsetOverflow {
                    offset,
                    length: expected_len,
                })?;
        if end > handle.authorized_len {
            return Err(BlockingDiskSubmitErrorKind::OutsideAuthorizedSpan {
                end,
                authorized_len: handle.authorized_len,
            });
        }
        Ok(())
    }

    fn record_rejection(&self) {
        self.shared.lock().rejected += 1;
    }

    fn drain_workers(&mut self, timeout: Duration) -> BlockingDiskWorkerDrain {
        let timeout = timeout.min(MAX_BLOCKING_DISK_SHUTDOWN_TIMEOUT);
        let deadline = Instant::now()
            .checked_add(timeout)
            .expect("bounded shutdown timeout fits Instant");
        let mut worker_panics = 0;

        loop {
            self.drain_worker_exit_reports();
            worker_panics += self.join_reported_finished_workers();
            if self.workers.iter().all(|worker| worker.handle.is_none()) {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            self.wait_for_one_worker_exit(remaining.min(Duration::from_millis(1)));
        }

        self.drain_worker_exit_reports();
        worker_panics += self.join_reported_finished_workers();
        let in_flight_at_timeout = self.shared.lock().in_flight;
        let detached_workers = self
            .workers
            .iter()
            .filter(|worker| worker.handle.is_some())
            .count();
        for worker in &mut self.workers {
            drop(worker.handle.take());
        }
        BlockingDiskWorkerDrain {
            worker_panics,
            detached_workers,
            in_flight_at_timeout,
        }
    }

    fn drain_worker_exit_reports(&mut self) {
        loop {
            let report = self
                .worker_exits
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .try_recv();
            match report {
                Ok(worker) => self.mark_worker_exit_reported(worker),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    fn wait_for_one_worker_exit(&mut self, timeout: Duration) {
        let report = self
            .worker_exits
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .recv_timeout(timeout);
        match report {
            Ok(worker) => self.mark_worker_exit_reported(worker),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
        }
    }

    fn mark_worker_exit_reported(&mut self, worker: usize) {
        if let Some(worker) = self.workers.get_mut(worker) {
            worker.exit_reported = true;
        } else {
            debug_assert!(false, "worker exit report index must be in range");
        }
    }

    fn join_reported_finished_workers(&mut self) -> usize {
        let mut panics = 0;
        for worker in &mut self.workers {
            let finished =
                worker.exit_reported && worker.handle.as_ref().is_some_and(JoinHandle::is_finished);
            if finished {
                let handle = worker.handle.take().expect("checked live handle");
                panics += usize::from(handle.join().is_err());
            }
        }
        panics
    }
}

impl fmt::Debug for BlockingDiskLane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingDiskLane")
            .field("backend_epoch", &self.backend_epoch)
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}

impl Drop for BlockingDiskLane {
    fn drop(&mut self) {
        self.begin_shutdown();
        let _worker_drain = self.drain_workers(BLOCKING_DISK_DROP_TIMEOUT);
        while self.try_recv().is_some() {}
        let _closed = self.completion.finish_close();
    }
}

struct BlockingDiskWorker {
    handle: Option<JoinHandle<()>>,
    exit_reported: bool,
}

struct BlockingDiskWorkerDrain {
    worker_panics: usize,
    detached_workers: usize,
    in_flight_at_timeout: usize,
}

struct WorkerExitReporter {
    worker: usize,
    sender: SyncSender<usize>,
}

impl Drop for WorkerExitReporter {
    fn drop(&mut self) {
        let _report = self.sender.try_send(self.worker);
    }
}

struct BlockingDiskShared {
    state: Mutex<BlockingDiskState>,
    available: Condvar,
}

impl BlockingDiskShared {
    fn lock(&self) -> MutexGuard<'_, BlockingDiskState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct BlockingDiskState {
    queue: VecDeque<AcceptedWork>,
    accepted_operations: HashSet<BlockingDiskOperationId>,
    queue_capacity: usize,
    accepting: bool,
    stopping: bool,
    in_flight: usize,
    accepted: u64,
    completed: u64,
    received: u64,
    cancelled: u64,
    rejected: u64,
    peak_queue_len: usize,
}

struct AcceptedWork {
    operation_id: BlockingDiskOperationId,
    backend_epoch: BlockingBackendEpoch,
    operation: BlockingDiskOperation,
    cancellation: BlockingDiskCancellationRegistration,
    lease: Option<BufferLease>,
    completion_permit: Option<CompletionPermit<BlockingDiskOutcome>>,
    byte_permit: Option<BytePermit>,
}

impl AcceptedWork {
    fn finish(&mut self, result: Result<BlockingDiskCompletion, BlockingDiskError>) {
        self.cancellation.finish();
        let outcome = BlockingDiskOutcome {
            operation_id: self.operation_id,
            backend_epoch: self.backend_epoch,
            operation: self.operation,
            result,
            lease: self.lease.take(),
            accepted_bytes: self.byte_permit.take(),
        };
        self.completion_permit
            .take()
            .expect("accepted work owns one completion permit")
            .send(outcome);
    }

    fn normalize_terminal_state(&mut self) -> Result<(), BufferTransitionError> {
        let Some(lease) = self.lease.as_mut() else {
            return Ok(());
        };
        match lease.state() {
            BufferState::DiskQueued => lease.transition(BufferState::Releasable, OwnerTag::Storage),
            BufferState::DiskInFlight => lease.transition(BufferState::DiskDone, OwnerTag::Storage),
            _ => Ok(()),
        }
    }
}

impl Drop for AcceptedWork {
    fn drop(&mut self) {
        if self.lease.is_some() && self.completion_permit.is_some() && self.byte_permit.is_some() {
            let result = match self.normalize_terminal_state() {
                Ok(()) => BlockingDiskError::WorkerAborted,
                Err(error) => BlockingDiskError::BufferTransition(error),
            };
            self.finish(Err(result));
        }
    }
}

fn worker_main(shared: Arc<BlockingDiskShared>, executor: Arc<dyn BlockingDiskExecutor>) {
    loop {
        let Some(mut work) = next_work(&shared) else {
            return;
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            execute_work(&mut work, executor.as_ref())
        }))
        .unwrap_or(Err(BlockingDiskError::ExecutorPanicked));
        let result = match work.normalize_terminal_state() {
            Ok(()) => result,
            Err(error) => Err(BlockingDiskError::BufferTransition(error)),
        };
        let cancelled = matches!(result, Err(BlockingDiskError::Cancelled));
        work.finish(result);
        let mut state = shared.lock();
        state.in_flight -= 1;
        state.completed += 1;
        if cancelled {
            state.cancelled += 1;
        }
    }
}

fn next_work(shared: &BlockingDiskShared) -> Option<AcceptedWork> {
    let mut state = shared.lock();
    loop {
        if state.stopping {
            return None;
        }
        if let Some(work) = state.queue.pop_front() {
            state.in_flight += 1;
            return Some(work);
        }
        if !state.accepting {
            return None;
        }
        state = shared
            .available
            .wait(state)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

fn execute_work(
    work: &mut AcceptedWork,
    executor: &dyn BlockingDiskExecutor,
) -> Result<BlockingDiskCompletion, BlockingDiskError> {
    let lease = work
        .lease
        .as_mut()
        .expect("accepted work owns exactly one lease");
    if !work.cancellation.claim_for_worker() {
        lease
            .transition(BufferState::Releasable, OwnerTag::Storage)
            .map_err(BlockingDiskError::BufferTransition)?;
        return Err(BlockingDiskError::Cancelled);
    }
    lease
        .transition(BufferState::DiskInFlight, OwnerTag::Disk)
        .map_err(BlockingDiskError::BufferTransition)?;

    let BlockingDiskOperation::WriteAt {
        handle,
        offset,
        expected_len,
    } = work.operation;
    let write_result = executor.write_at(
        handle,
        offset,
        lease.bytes().map_err(BlockingDiskError::BufferTransition)?,
    );
    lease
        .transition(BufferState::DiskDone, OwnerTag::Storage)
        .map_err(BlockingDiskError::BufferTransition)?;
    match write_result {
        Ok(actual) if actual == expected_len => Ok(BlockingDiskCompletion {
            bytes_written: actual,
        }),
        Ok(actual) if actual < expected_len => Err(BlockingDiskError::ShortWrite {
            expected: expected_len,
            actual,
        }),
        Ok(actual) => Err(BlockingDiskError::InvalidCompletionLength {
            expected: expected_len,
            actual,
        }),
        Err(error) => Err(BlockingDiskError::Backend(error)),
    }
}

#[cfg(test)]
thread_local! {
    static TEST_REACTOR_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn in_test_reactor_context() -> bool {
    TEST_REACTOR_DEPTH.with(|depth| depth.get() != 0)
}

#[cfg(not(test))]
const fn in_test_reactor_context() -> bool {
    false
}

/// Test-only proof that a caller is executing on reactor context.
#[cfg(test)]
pub struct TestReactorContextGuard;

#[cfg(test)]
impl TestReactorContextGuard {
    #[must_use]
    pub fn enter() -> Self {
        TEST_REACTOR_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}

#[cfg(test)]
impl Drop for TestReactorContextGuard {
    fn drop(&mut self) {
        TEST_REACTOR_DEPTH.with(|depth| {
            let current = depth.get();
            debug_assert!(current > 0);
            depth.set(current.saturating_sub(1));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BufferPool, BufferPoolConfig};
    use ariax_core::BufferId;
    use std::collections::VecDeque;
    use std::fs::{self, OpenOptions};
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::mpsc;
    use std::time::Instant;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    static REGISTRY_TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn backend_epoch(value: u64) -> BlockingBackendEpoch {
        BlockingBackendEpoch::new(value).expect("nonzero epoch")
    }

    #[test]
    fn registered_native_file_is_positionally_written_and_revoked() {
        let path = std::env::temp_dir().join(format!(
            "ariax-blocking-registry-{}-{}",
            std::process::id(),
            REGISTRY_TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create registry test file");
        file.set_len(8).expect("set file length");
        let registry = BlockingFileRegistry::new(backend_epoch(9));
        let handle = registry.register(file, 8).expect("register file");
        assert_eq!(registry.registered_count(), 1);
        assert_eq!(registry.write_at(handle, 3, b"xy"), Ok(2));
        assert_eq!(fs::read(&path).expect("read file"), b"\0\0\0xy\0\0\0");
        registry.unregister(handle).expect("unregister file");
        assert_eq!(registry.registered_count(), 0);
        assert_eq!(
            registry.write_at(handle, 0, b"z"),
            Err(BlockingDiskIoError {
                kind: BlockingDiskIoErrorKind::InvalidHandle,
                raw_os_error: None,
            })
        );
        fs::remove_file(path).expect("remove registry test file");
    }

    fn operation_id(value: u64) -> BlockingDiskOperationId {
        BlockingDiskOperationId::new(value).expect("nonzero operation id")
    }

    fn handle(epoch: BlockingBackendEpoch, authorized_len: u64) -> BlockingFileHandle {
        BlockingFileHandle::new(
            NonZeroU64::new(11).expect("nonzero handle"),
            epoch,
            authorized_len,
        )
    }

    fn config(
        worker_count: usize,
        queue_capacity: usize,
        completion_capacity: usize,
        max_accepted_bytes: usize,
    ) -> BlockingDiskLaneConfig {
        BlockingDiskLaneConfig {
            worker_count,
            queue_capacity,
            completion_capacity,
            max_accepted_bytes,
        }
    }

    fn pool() -> BufferPool {
        BufferPool::new(BufferPoolConfig::new(4 * 1024 * 1024, 4 * 1024 * 1024))
            .expect("buffer pool")
    }

    fn filled_lease(pool: &BufferPool, bytes: &[u8]) -> BufferLease {
        let mut lease = pool
            .try_reserve(bytes.len(), OwnerTag::Network, None, None)
            .expect("reserve lease");
        lease
            .transition(BufferState::NetworkFill, OwnerTag::Network)
            .expect("network fill");
        lease.writable().expect("writable")[..bytes.len()].copy_from_slice(bytes);
        lease
            .mark_filled(bytes.len(), OwnerTag::Storage)
            .expect("filled");
        lease
    }

    fn submission(
        pool: &BufferPool,
        operation: u64,
        backend_epoch: BlockingBackendEpoch,
        file: BlockingFileHandle,
        offset: u64,
        bytes: &[u8],
    ) -> (BlockingDiskSubmission, BlockingDiskCancelHandle, BufferId) {
        let lease = filled_lease(pool, bytes);
        let buffer_id = lease.id();
        let (cancellation, registration) = BlockingDiskCancelHandle::pair();
        (
            BlockingDiskSubmission::new(
                operation_id(operation),
                backend_epoch,
                BlockingDiskOperation::WriteAt {
                    handle: file,
                    offset,
                    expected_len: bytes.len(),
                },
                registration,
                lease,
            ),
            cancellation,
            buffer_id,
        )
    }

    fn wait_for_outcome(lane: &BlockingDiskLane) -> BlockingDiskOutcome {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            if let Some(outcome) = lane.try_recv() {
                return outcome;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for disk outcome"
            );
            thread::yield_now();
        }
    }

    fn release_lease(pool: &BufferPool, mut lease: BufferLease) {
        if lease.state() != BufferState::Releasable {
            lease
                .transition(BufferState::Releasable, OwnerTag::Pool)
                .expect("lease becomes releasable");
        }
        pool.release(lease).expect("release lease");
    }

    fn release_outcome(
        pool: &BufferPool,
        outcome: BlockingDiskOutcome,
    ) -> Result<BlockingDiskCompletion, BlockingDiskError> {
        let (_, _, _, result, lease) = outcome.into_parts();
        release_lease(pool, lease);
        result
    }

    fn wait_for_quarantine_and_resolve(pool: &BufferPool, buffer: BufferId) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        while !pool.resolve_quarantine(buffer) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for detached lease quarantine"
            );
            thread::yield_now();
        }
    }

    #[derive(Clone, Copy)]
    enum ScriptAction {
        Exact,
        Short(usize),
        Long(usize),
        Error(BlockingDiskIoError),
        Panic,
    }

    struct ScriptExecutor {
        actions: Mutex<VecDeque<ScriptAction>>,
    }

    impl ScriptExecutor {
        fn new(actions: impl IntoIterator<Item = ScriptAction>) -> Self {
            Self {
                actions: Mutex::new(actions.into_iter().collect()),
            }
        }
    }

    impl BlockingDiskExecutor for ScriptExecutor {
        fn write_at(
            &self,
            _handle: BlockingFileHandle,
            _offset: u64,
            bytes: &[u8],
        ) -> Result<usize, BlockingDiskIoError> {
            match self
                .actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or(ScriptAction::Exact)
            {
                ScriptAction::Exact => Ok(bytes.len()),
                ScriptAction::Short(actual) | ScriptAction::Long(actual) => Ok(actual),
                ScriptAction::Error(error) => Err(error),
                ScriptAction::Panic => panic!("injected blocking executor panic"),
            }
        }
    }

    struct GateState {
        entered: usize,
        released: bool,
    }

    #[derive(Clone)]
    struct GateExecutor {
        state: Arc<(Mutex<GateState>, Condvar)>,
    }

    impl GateExecutor {
        fn new() -> Self {
            Self {
                state: Arc::new((
                    Mutex::new(GateState {
                        entered: 0,
                        released: false,
                    }),
                    Condvar::new(),
                )),
            }
        }

        fn wait_for_entered(&self, expected: usize) {
            let deadline = Instant::now() + TEST_TIMEOUT;
            let (lock, available) = &*self.state;
            let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            while state.entered < expected {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "worker did not enter executor");
                let (next, timeout) = available
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state = next;
                assert!(!timeout.timed_out(), "worker did not enter executor");
            }
        }

        fn release(&self) {
            let (lock, available) = &*self.state;
            lock.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .released = true;
            available.notify_all();
        }
    }

    impl BlockingDiskExecutor for GateExecutor {
        fn write_at(
            &self,
            _handle: BlockingFileHandle,
            _offset: u64,
            bytes: &[u8],
        ) -> Result<usize, BlockingDiskIoError> {
            let (lock, available) = &*self.state;
            let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            state.entered += 1;
            available.notify_all();
            while !state.released {
                state = available
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            Ok(bytes.len())
        }
    }

    #[test]
    fn terminal_results_return_the_exact_lease() {
        let pool = pool();
        let epoch = backend_epoch(1);
        let file = handle(epoch, 1024);
        let backend_error = BlockingDiskIoError {
            kind: BlockingDiskIoErrorKind::OutOfSpace,
            raw_os_error: Some(28),
        };
        let lane = BlockingDiskLane::new(
            config(1, 5, 5, 20),
            epoch,
            ScriptExecutor::new([
                ScriptAction::Exact,
                ScriptAction::Short(2),
                ScriptAction::Long(5),
                ScriptAction::Error(backend_error),
                ScriptAction::Panic,
            ]),
        )
        .expect("lane");

        let mut ids = Vec::new();
        for operation in 1..=5 {
            let (submission, _cancellation, id) =
                submission(&pool, operation, epoch, file, 0, b"aria");
            ids.push(id);
            lane.try_submit(submission).expect("accepted");
        }
        let expected = [
            Ok(BlockingDiskCompletion { bytes_written: 4 }),
            Err(BlockingDiskError::ShortWrite {
                expected: 4,
                actual: 2,
            }),
            Err(BlockingDiskError::InvalidCompletionLength {
                expected: 4,
                actual: 5,
            }),
            Err(BlockingDiskError::Backend(backend_error)),
            Err(BlockingDiskError::ExecutorPanicked),
        ];
        for (id, expected) in ids.into_iter().zip(expected) {
            let outcome = wait_for_outcome(&lane);
            assert_eq!(outcome.lease().id(), id);
            assert_eq!(outcome.lease().state(), BufferState::DiskDone);
            assert_eq!(release_outcome(&pool, outcome), expected);
        }

        let shutdown = lane.shutdown(TEST_TIMEOUT);
        assert_eq!(shutdown.detached_workers(), 0);
        assert!(shutdown.completion_closed());
    }

    #[test]
    fn validation_rejections_retain_the_exact_lease() {
        let pool = pool();
        let epoch = backend_epoch(2);
        let other_epoch = backend_epoch(3);
        let file = handle(epoch, 8);
        let lane = BlockingDiskLane::new(config(1, 2, 4, 64), epoch, ScriptExecutor::new([]))
            .expect("lane");

        let cases = [
            (
                submission(&pool, 1, other_epoch, file, 0, b"aria").0,
                BlockingDiskSubmitErrorKind::BackendEpochMismatch {
                    lane: epoch,
                    submitted: other_epoch,
                },
            ),
            (
                submission(&pool, 2, epoch, handle(other_epoch, 8), 0, b"aria").0,
                BlockingDiskSubmitErrorKind::HandleEpochMismatch {
                    submitted: epoch,
                    handle: other_epoch,
                },
            ),
            (
                submission(&pool, 3, epoch, file, u64::MAX - 1, b"aria").0,
                BlockingDiskSubmitErrorKind::OffsetOverflow {
                    offset: u64::MAX - 1,
                    length: 4,
                },
            ),
            (
                submission(&pool, 4, epoch, file, 6, b"aria").0,
                BlockingDiskSubmitErrorKind::OutsideAuthorizedSpan {
                    end: 10,
                    authorized_len: 8,
                },
            ),
        ];
        for (submission, expected) in cases {
            let original = submission.lease().id();
            let error = lane.try_submit(submission).expect_err("rejected");
            assert_eq!(error.reason(), expected);
            let submission = error.into_parts().1;
            assert_eq!(submission.lease().id(), original);
            assert_eq!(submission.lease().state(), BufferState::Filled);
            release_lease(&pool, submission.into_lease());
        }

        assert!(lane.shutdown(TEST_TIMEOUT).completion_closed());
    }

    #[test]
    fn rejected_submission_keeps_registration_retryable() {
        let pool = pool();
        let epoch = backend_epoch(4);
        let file = handle(epoch, 1024);
        let executor = GateExecutor::new();
        let lane =
            BlockingDiskLane::new(config(1, 1, 1, 8), epoch, executor.clone()).expect("lane");
        lane.try_submit(submission(&pool, 1, epoch, file, 0, b"aria").0)
            .expect("first accepted");
        executor.wait_for_entered(1);

        let (retry, cancellation, retry_id) = submission(&pool, 2, epoch, file, 0, b"aria");
        let error = lane.try_submit(retry).expect_err("completion full");
        assert_eq!(error.reason(), BlockingDiskSubmitErrorKind::CompletionFull);
        let retry = error.into_parts().1;
        assert_eq!(retry.lease().id(), retry_id);
        assert_eq!(retry.lease().state(), BufferState::Filled);

        executor.release();
        release_outcome(&pool, wait_for_outcome(&lane)).expect("first success");
        lane.try_submit(retry).expect("same registration retries");
        let outcome = wait_for_outcome(&lane);
        assert_eq!(outcome.lease().id(), retry_id);
        release_outcome(&pool, outcome).expect("retry success");
        assert_eq!(cancellation.cancel(), BlockingDiskCancelResult::Complete);
        assert!(lane.shutdown(TEST_TIMEOUT).completion_closed());
    }

    #[test]
    fn queued_cancellation_is_confirmed_once() {
        let pool = pool();
        let epoch = backend_epoch(5);
        let file = handle(epoch, 1024);
        let executor = GateExecutor::new();
        let lane =
            BlockingDiskLane::new(config(1, 1, 2, 8), epoch, executor.clone()).expect("lane");
        lane.try_submit(submission(&pool, 1, epoch, file, 0, b"aria").0)
            .expect("running accepted");
        executor.wait_for_entered(1);
        let (queued, cancellation, queued_id) = submission(&pool, 2, epoch, file, 0, b"aria");
        lane.try_submit(queued).expect("queued accepted");
        assert_eq!(cancellation.cancel(), BlockingDiskCancelResult::Requested);
        assert_eq!(
            cancellation.cancel(),
            BlockingDiskCancelResult::AlreadyRequested
        );

        executor.release();
        let mut saw_cancelled = false;
        for _ in 0..2 {
            let outcome = wait_for_outcome(&lane);
            if outcome.operation_id() == operation_id(2) {
                assert_eq!(outcome.lease().id(), queued_id);
                assert_eq!(outcome.lease().state(), BufferState::Releasable);
                assert_eq!(
                    release_outcome(&pool, outcome),
                    Err(BlockingDiskError::Cancelled)
                );
                saw_cancelled = true;
            } else {
                release_outcome(&pool, outcome).expect("running success");
            }
        }
        assert!(saw_cancelled);
        assert_eq!(cancellation.cancel(), BlockingDiskCancelResult::Complete);
        assert!(lane.shutdown(TEST_TIMEOUT).completion_closed());
    }

    #[test]
    fn timed_shutdown_aborts_queue_and_detaches_only_in_flight_lease() {
        let pool = pool();
        let epoch = backend_epoch(6);
        let file = handle(epoch, 1024);
        let executor = GateExecutor::new();
        let lane =
            BlockingDiskLane::new(config(1, 1, 3, 12), epoch, executor.clone()).expect("lane");
        let (running, running_cancellation, running_id) =
            submission(&pool, 1, epoch, file, 0, b"aria");
        lane.try_submit(running).expect("running accepted");
        executor.wait_for_entered(1);
        let (queued, queued_cancellation, queued_id) =
            submission(&pool, 2, epoch, file, 0, b"aria");
        lane.try_submit(queued).expect("queued accepted");
        let (rejected, rejected_cancellation, rejected_id) =
            submission(&pool, 3, epoch, file, 0, b"aria");
        let error = lane.try_submit(rejected).expect_err("queue full");
        assert_eq!(error.reason(), BlockingDiskSubmitErrorKind::QueueFull);
        let rejected = error.into_parts().1;
        assert_eq!(rejected.lease().id(), rejected_id);
        release_lease(&pool, rejected.into_lease());
        assert_eq!(
            rejected_cancellation.cancel(),
            BlockingDiskCancelResult::Complete
        );

        assert!(lane.begin_shutdown());
        let queued_outcome = wait_for_outcome(&lane);
        assert_eq!(queued_outcome.operation_id(), operation_id(2));
        assert_eq!(queued_outcome.lease().id(), queued_id);
        assert_eq!(
            release_outcome(&pool, queued_outcome),
            Err(BlockingDiskError::WorkerAborted)
        );
        assert_eq!(
            queued_cancellation.cancel(),
            BlockingDiskCancelResult::Complete
        );
        assert_eq!(
            running_cancellation.cancel(),
            BlockingDiskCancelResult::TooLate
        );

        let shutdown = lane.shutdown(Duration::from_millis(25));
        assert_eq!(shutdown.detached_workers(), 1);
        assert_eq!(shutdown.in_flight_at_timeout(), 1);
        assert!(!shutdown.completion_closed());
        assert!(shutdown.outcomes().is_empty());

        executor.release();
        wait_for_quarantine_and_resolve(&pool, running_id);
        assert_eq!(
            running_cancellation.cancel(),
            BlockingDiskCancelResult::Complete
        );
    }

    #[test]
    fn drop_uses_zero_wait_detach_for_hung_executor() {
        let pool = pool();
        let epoch = backend_epoch(7);
        let file = handle(epoch, 1024);
        let executor = GateExecutor::new();
        let lane =
            BlockingDiskLane::new(config(1, 1, 1, 4), epoch, executor.clone()).expect("lane");
        let (running, cancellation, running_id) = submission(&pool, 1, epoch, file, 0, b"aria");
        lane.try_submit(running).expect("running accepted");
        executor.wait_for_entered(1);

        let (finished, observed) = mpsc::channel();
        let dropper = thread::spawn(move || {
            drop(lane);
            finished.send(()).expect("drop completion receiver");
        });
        let returned_without_release = observed.recv_timeout(Duration::from_millis(250)).is_ok();
        executor.release();
        dropper.join().expect("dropper thread");
        assert!(returned_without_release, "Drop waited for the executor");
        wait_for_quarantine_and_resolve(&pool, running_id);
        assert_eq!(cancellation.cancel(), BlockingDiskCancelResult::Complete);
    }

    #[test]
    fn reactor_rejection_tests_the_real_module_and_remains_retryable() {
        let pool = pool();
        let epoch = backend_epoch(8);
        let file = handle(epoch, 1024);
        let lane = BlockingDiskLane::new(config(1, 1, 1, 4), epoch, ScriptExecutor::new([]))
            .expect("lane");
        let (submission, cancellation, id) = submission(&pool, 1, epoch, file, 0, b"aria");
        let guard = TestReactorContextGuard::enter();
        let error = lane
            .try_submit(submission)
            .expect_err("reactor submission rejected");
        drop(guard);
        assert_eq!(error.reason(), BlockingDiskSubmitErrorKind::ReactorContext);
        let submission = error.into_parts().1;
        assert_eq!(submission.lease().id(), id);
        assert_eq!(submission.lease().state(), BufferState::Filled);
        lane.try_submit(submission)
            .expect("rejection remains retryable");
        release_outcome(&pool, wait_for_outcome(&lane)).expect("retry success");
        assert_eq!(cancellation.cancel(), BlockingDiskCancelResult::Complete);
        assert!(lane.shutdown(TEST_TIMEOUT).completion_closed());
    }

    #[test]
    fn dropping_unused_registration_completes_the_cancel_handle() {
        let (cancellation, registration) = BlockingDiskCancelHandle::pair();
        drop(registration);
        assert_eq!(cancellation.cancel(), BlockingDiskCancelResult::Complete);
    }
}
