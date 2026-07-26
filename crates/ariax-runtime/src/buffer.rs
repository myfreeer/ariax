use crate::{BudgetError, ByteBudget, BytePermit};
use ariax_core::{BufferId, Generation, TaskId};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

/// Canonical transfer-buffer size classes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(usize)]
pub enum SizeClass {
    KiB16 = 16 * 1024,
    KiB64 = 64 * 1024,
    KiB256 = 256 * 1024,
    MiB1 = 1024 * 1024,
}

impl SizeClass {
    pub const ALL: [Self; 4] = [Self::KiB16, Self::KiB64, Self::KiB256, Self::MiB1];

    #[must_use]
    pub const fn capacity(self) -> usize {
        self as usize
    }

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::KiB16 => "16_kib",
            Self::KiB64 => "64_kib",
            Self::KiB256 => "256_kib",
            Self::MiB1 => "1_mib",
        }
    }

    fn choose(minimum: usize) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|class| class.capacity() >= minimum)
    }
}

/// Per-class allocation limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SizeClassConfig {
    pub class: SizeClass,
    pub max_buffers: usize,
}

/// Pool limits and shared accounting domains.
#[derive(Clone, Debug)]
pub struct BufferPoolConfig {
    pub classes: [SizeClassConfig; 4],
    pub buffer_budget: ByteBudget,
    pub resident_budget: ByteBudget,
    pub quarantine_budget: ByteBudget,
    pub quarantine_timeout: Duration,
}

impl BufferPoolConfig {
    #[must_use]
    pub fn new(total_bytes: usize, quarantine_bytes: usize) -> Self {
        Self {
            classes: SizeClass::ALL.map(|class| SizeClassConfig {
                class,
                max_buffers: total_bytes / class.capacity(),
            }),
            buffer_budget: ByteBudget::new(total_bytes),
            resident_budget: ByteBudget::new(total_bytes),
            quarantine_budget: ByteBudget::new(quarantine_bytes),
            quarantine_timeout: Duration::from_secs(30),
        }
    }
}

/// The one mutable/immutable ownership state vocabulary for transfer buffers.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BufferState {
    Free,
    Reserved,
    NetworkFill,
    TransformOwned,
    Filled,
    Validating,
    DiskQueued,
    DiskInFlight,
    DiskDone,
    HashBorrowed,
    JournalPending,
    Releasable,
}

impl BufferState {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Reserved => "reserved",
            Self::NetworkFill => "network_fill",
            Self::TransformOwned => "transform_owned",
            Self::Filled => "filled",
            Self::Validating => "validating",
            Self::DiskQueued => "disk_queued",
            Self::DiskInFlight => "disk_in_flight",
            Self::DiskDone => "disk_done",
            Self::HashBorrowed => "hash_borrowed",
            Self::JournalPending => "journal_pending",
            Self::Releasable => "releasable",
        }
    }

    #[must_use]
    pub const fn is_mutable(self) -> bool {
        matches!(
            self,
            Self::Reserved | Self::NetworkFill | Self::TransformOwned
        )
    }

    #[must_use]
    pub const fn is_readable(self) -> bool {
        !matches!(
            self,
            Self::Free | Self::Reserved | Self::NetworkFill | Self::TransformOwned
        )
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        allowed_transition(self, next)
    }
}

pub const ALL_BUFFER_STATES: [BufferState; 12] = [
    BufferState::Free,
    BufferState::Reserved,
    BufferState::NetworkFill,
    BufferState::TransformOwned,
    BufferState::Filled,
    BufferState::Validating,
    BufferState::DiskQueued,
    BufferState::DiskInFlight,
    BufferState::DiskDone,
    BufferState::HashBorrowed,
    BufferState::JournalPending,
    BufferState::Releasable,
];

/// The lane that currently owns a lease.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OwnerTag {
    Pool,
    Network,
    Transform,
    Storage,
    Disk,
    Cpu,
    Journal,
    Quarantine,
}

impl OwnerTag {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Pool => "pool",
            Self::Network => "network",
            Self::Transform => "transform",
            Self::Storage => "storage",
            Self::Disk => "disk",
            Self::Cpu => "cpu",
            Self::Journal => "journal",
            Self::Quarantine => "quarantine",
        }
    }
}

pub const ALL_OWNER_TAGS: [OwnerTag; 8] = [
    OwnerTag::Pool,
    OwnerTag::Network,
    OwnerTag::Transform,
    OwnerTag::Storage,
    OwnerTag::Disk,
    OwnerTag::Cpu,
    OwnerTag::Journal,
    OwnerTag::Quarantine,
];

/// One move-only stable-capacity payload buffer.
pub struct BufferLease {
    storage: Option<PooledStorage>,
    pool: Weak<PoolInner>,
    len: usize,
    state: BufferState,
    owner: OwnerTag,
    task: Option<TaskId>,
    generation: Option<Generation>,
}

impl BufferLease {
    #[must_use]
    pub fn id(&self) -> BufferId {
        self.storage().id
    }

    #[must_use]
    pub fn class(&self) -> SizeClass {
        self.storage().class
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.storage().bytes.len()
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub const fn state(&self) -> BufferState {
        self.state
    }

    #[must_use]
    pub const fn owner(&self) -> OwnerTag {
        self.owner
    }

    #[must_use]
    pub const fn task(&self) -> Option<TaskId> {
        self.task
    }

    #[must_use]
    pub const fn generation(&self) -> Option<Generation> {
        self.generation
    }

    pub fn writable(&mut self) -> Result<&mut [u8], BufferTransitionError> {
        if !self.state.is_mutable() {
            return Err(BufferTransitionError::NotMutable(self.state));
        }
        Ok(&mut self.storage_mut().bytes)
    }

    pub fn mark_filled(
        &mut self,
        len: usize,
        owner: OwnerTag,
    ) -> Result<(), BufferTransitionError> {
        if !self.state.is_mutable() {
            return Err(BufferTransitionError::NotMutable(self.state));
        }
        if len > self.capacity() {
            return Err(BufferTransitionError::LengthExceedsCapacity {
                len,
                capacity: self.capacity(),
            });
        }
        self.len = len;
        self.transition(BufferState::Filled, owner)
    }

    pub fn bytes(&self) -> Result<&[u8], BufferTransitionError> {
        if !self.state.is_readable() {
            return Err(BufferTransitionError::NotReadable(self.state));
        }
        Ok(&self.storage().bytes[..self.len])
    }

    pub fn transition(
        &mut self,
        next: BufferState,
        owner: OwnerTag,
    ) -> Result<(), BufferTransitionError> {
        if !self.state.can_transition_to(next) {
            return Err(BufferTransitionError::InvalidTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        self.owner = owner;
        Ok(())
    }

    fn storage(&self) -> &PooledStorage {
        self.storage.as_ref().expect("live lease owns storage")
    }

    fn storage_mut(&mut self) -> &mut PooledStorage {
        self.storage.as_mut().expect("live lease owns storage")
    }
}

impl fmt::Debug for BufferLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BufferLease")
            .field("id", &self.id())
            .field("class", &self.class())
            .field("len", &self.len)
            .field("capacity", &self.capacity())
            .field("state", &self.state)
            .field("owner", &self.owner)
            .field("task", &self.task)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        let Some(storage) = self.storage.take() else {
            return;
        };
        if let Some(pool) = self.pool.upgrade() {
            pool.quarantine_dropped(storage, self.state);
        } else {
            leak_storage(storage);
        }
    }
}

/// Why a lease state or view operation was invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BufferTransitionError {
    InvalidTransition { from: BufferState, to: BufferState },
    NotMutable(BufferState),
    NotReadable(BufferState),
    LengthExceedsCapacity { len: usize, capacity: usize },
}

impl fmt::Display for BufferTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransition { from, to } => {
                write!(formatter, "invalid buffer transition {from:?} -> {to:?}")
            }
            Self::NotMutable(state) => write!(formatter, "buffer state {state:?} is immutable"),
            Self::NotReadable(state) => write!(formatter, "buffer state {state:?} is not readable"),
            Self::LengthExceedsCapacity { len, capacity } => {
                write!(formatter, "buffer length {len} exceeds capacity {capacity}")
            }
        }
    }
}

impl Error for BufferTransitionError {}

const fn allowed_transition(from: BufferState, to: BufferState) -> bool {
    use BufferState::{
        DiskDone, DiskInFlight, DiskQueued, Filled, HashBorrowed, JournalPending, NetworkFill,
        Releasable, Reserved, TransformOwned, Validating,
    };
    matches!(
        (from, to),
        (Reserved, NetworkFill | TransformOwned | Releasable)
            | (NetworkFill, TransformOwned | Filled | Releasable)
            | (TransformOwned, Filled | Releasable)
            | (
                Filled,
                Validating | DiskQueued | HashBorrowed | JournalPending | Releasable
            )
            | (
                Validating,
                DiskQueued | HashBorrowed | JournalPending | Releasable
            )
            | (DiskQueued, DiskInFlight | Releasable)
            | (DiskInFlight, DiskDone)
            | (DiskDone, HashBorrowed | JournalPending | Releasable)
            | (
                HashBorrowed,
                DiskQueued | DiskDone | JournalPending | Releasable
            )
            | (JournalPending, Releasable)
    )
}

/// A bounded pool with LIFO class reuse and explicit quarantine.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<PoolInner>,
}

impl BufferPool {
    pub fn new(config: BufferPoolConfig) -> Result<Self, PoolError> {
        for (index, class) in config.classes.iter().enumerate() {
            if config.classes[..index]
                .iter()
                .any(|existing| existing.class == class.class)
            {
                return Err(PoolError::InvalidClassConfiguration);
            }
        }
        for expected in SizeClass::ALL {
            let Some(_class) = config.classes.iter().find(|class| class.class == expected) else {
                return Err(PoolError::InvalidClassConfiguration);
            };
        }
        Ok(Self {
            inner: Arc::new(PoolInner {
                buffer_budget: config.buffer_budget,
                resident_budget: config.resident_budget,
                quarantine_budget: config.quarantine_budget,
                quarantine_timeout: config.quarantine_timeout,
                state: Mutex::new(PoolState {
                    classes: config
                        .classes
                        .map(|config| ClassPool {
                            class: config.class,
                            max_buffers: config.max_buffers,
                            allocated_buffers: 0,
                            free: Vec::new(),
                        })
                        .into_iter()
                        .collect(),
                    next_id: 1,
                    metrics: BufferPoolMetrics::default(),
                    quarantine: Vec::new(),
                }),
            }),
        })
    }

    pub fn try_reserve(
        &self,
        minimum_capacity: usize,
        owner: OwnerTag,
        task: Option<TaskId>,
        generation: Option<Generation>,
    ) -> Result<BufferLease, PoolError> {
        let class = SizeClass::choose(minimum_capacity).ok_or(PoolError::RequestTooLarge {
            requested: minimum_capacity,
        })?;
        let mut state = self.inner.lock();
        if state.metrics.faulted {
            return Err(PoolError::Faulted);
        }
        let class_index = state
            .classes
            .iter()
            .position(|pool| pool.class == class)
            .ok_or(PoolError::InvalidClassConfiguration)?;
        let storage = if let Some(storage) = state.classes[class_index].free.pop() {
            state.metrics.free_bytes -= class.capacity();
            storage
        } else {
            if state.classes[class_index].allocated_buffers
                >= state.classes[class_index].max_buffers
            {
                state.metrics.exhaustion_count += 1;
                return Err(PoolError::ClassExhausted(class));
            }
            let buffer_permit = self
                .inner
                .buffer_budget
                .try_acquire(class.capacity())
                .map_err(PoolError::BufferBudget)?;
            let resident_permit = self
                .inner
                .resident_budget
                .try_acquire(class.capacity())
                .map_err(PoolError::ResidentBudget)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(class.capacity())
                .map_err(|_| PoolError::AllocationFailed(class))?;
            bytes.resize(class.capacity(), 0);
            let id = BufferId::new(state.next_id).ok_or(PoolError::IdentifierExhausted)?;
            state.next_id = state
                .next_id
                .checked_add(1)
                .ok_or(PoolError::IdentifierExhausted)?;
            state.classes[class_index].allocated_buffers += 1;
            state.metrics.allocated_bytes += class.capacity();
            state.metrics.allocation_count += 1;
            state.metrics.peak_allocated_bytes = state
                .metrics
                .peak_allocated_bytes
                .max(state.metrics.allocated_bytes);
            PooledStorage {
                id,
                class,
                bytes: bytes.into_boxed_slice(),
                _buffer_permit: buffer_permit,
                _resident_permit: resident_permit,
            }
        };
        state.metrics.leased_bytes += class.capacity();
        Ok(BufferLease {
            storage: Some(storage),
            pool: Arc::downgrade(&self.inner),
            len: 0,
            state: BufferState::Reserved,
            owner,
            task,
            generation,
        })
    }

    pub fn release(&self, mut lease: BufferLease) -> Result<(), ReleaseError> {
        if !lease_belongs_to(&lease, &self.inner) {
            return Err(ReleaseError::new(PoolError::ForeignLease, lease));
        }
        if lease.state != BufferState::Releasable {
            return Err(ReleaseError::new(
                PoolError::NotReleasable(lease.state),
                lease,
            ));
        }
        let storage = lease.storage.take().expect("live lease");
        lease.pool = Weak::new();
        let capacity = storage.class.capacity();
        let mut state = self.inner.lock();
        let class = state
            .classes
            .iter_mut()
            .find(|pool| pool.class == storage.class)
            .expect("validated class");
        class.free.push(storage);
        state.metrics.leased_bytes -= capacity;
        state.metrics.free_bytes += capacity;
        state.metrics.release_count += 1;
        Ok(())
    }

    pub fn quarantine(&self, mut lease: BufferLease, now: Instant) -> Result<(), PoolError> {
        if !lease_belongs_to(&lease, &self.inner) {
            return Err(PoolError::ForeignLease);
        }
        if lease.state != BufferState::DiskInFlight {
            return Err(PoolError::NotInFlight(lease.state));
        }
        let storage = lease.storage.take().expect("live lease");
        lease.pool = Weak::new();
        self.inner.quarantine_storage(storage, now)
    }

    pub fn resolve_quarantine(&self, id: BufferId) -> bool {
        let mut state = self.inner.lock();
        let Some(index) = state
            .quarantine
            .iter()
            .position(|entry| entry.storage.id == id)
        else {
            return false;
        };
        let entry = state.quarantine.swap_remove(index);
        let capacity = entry.storage.class.capacity();
        drop(entry.quarantine_permit);
        let class = state
            .classes
            .iter_mut()
            .find(|pool| pool.class == entry.storage.class)
            .expect("validated class");
        class.free.push(entry.storage);
        state.metrics.quarantined_bytes -= capacity;
        state.metrics.free_bytes += capacity;
        state.metrics.quarantine_resolved_count += 1;
        true
    }

    pub fn reap_quarantine(&self, now: Instant) -> usize {
        let mut state = self.inner.lock();
        let mut retired = 0;
        let mut index = 0;
        while index < state.quarantine.len() {
            if state.quarantine[index].deadline > now {
                index += 1;
                continue;
            }
            let entry = state.quarantine.swap_remove(index);
            let capacity = entry.storage.class.capacity();
            drop(entry.quarantine_permit);
            state.metrics.quarantined_bytes -= capacity;
            state.metrics.retired_bytes += capacity;
            state.metrics.quarantine_timeout_count += 1;
            leak_storage(entry.storage);
            retired += 1;
        }
        retired
    }

    #[must_use]
    pub fn metrics(&self) -> BufferPoolMetrics {
        self.inner.lock().metrics
    }
}

impl fmt::Debug for BufferPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BufferPool")
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}

/// Point-in-time pool accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BufferPoolMetrics {
    pub allocated_bytes: usize,
    pub peak_allocated_bytes: usize,
    pub free_bytes: usize,
    pub leased_bytes: usize,
    pub quarantined_bytes: usize,
    pub retired_bytes: usize,
    pub allocation_count: u64,
    pub release_count: u64,
    pub exhaustion_count: u64,
    pub quarantine_count: u64,
    pub quarantine_resolved_count: u64,
    pub quarantine_timeout_count: u64,
    pub leaked_lease_count: u64,
    pub faulted: bool,
}

/// Why pool ownership or accounting could not advance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolError {
    InvalidClassConfiguration,
    RequestTooLarge { requested: usize },
    ClassExhausted(SizeClass),
    BufferBudget(BudgetError),
    ResidentBudget(BudgetError),
    QuarantineBudget(BudgetError),
    AllocationFailed(SizeClass),
    IdentifierExhausted,
    ForeignLease,
    NotReleasable(BufferState),
    NotInFlight(BufferState),
    Faulted,
}

impl fmt::Display for PoolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClassConfiguration => {
                formatter.write_str("invalid buffer class configuration")
            }
            Self::RequestTooLarge { requested } => {
                write!(
                    formatter,
                    "buffer request {requested} exceeds the largest size class"
                )
            }
            Self::ClassExhausted(class) => write!(formatter, "buffer class {class:?} is exhausted"),
            Self::BufferBudget(error) => write!(formatter, "buffer budget: {error}"),
            Self::ResidentBudget(error) => write!(formatter, "resident budget: {error}"),
            Self::QuarantineBudget(error) => write!(formatter, "quarantine budget: {error}"),
            Self::AllocationFailed(class) => write!(formatter, "allocation failed for {class:?}"),
            Self::IdentifierExhausted => formatter.write_str("buffer identifier space exhausted"),
            Self::ForeignLease => formatter.write_str("buffer lease belongs to another pool"),
            Self::NotReleasable(state) => {
                write!(formatter, "buffer state {state:?} is not releasable")
            }
            Self::NotInFlight(state) => {
                write!(formatter, "buffer state {state:?} is not disk-in-flight")
            }
            Self::Faulted => formatter.write_str("buffer pool is faulted"),
        }
    }
}

impl Error for PoolError {}

/// A failed normal release that returns ownership of the lease to the caller.
pub struct ReleaseError {
    reason: PoolError,
    lease: Box<BufferLease>,
}

impl ReleaseError {
    fn new(reason: PoolError, lease: BufferLease) -> Self {
        Self {
            reason,
            lease: Box::new(lease),
        }
    }

    #[must_use]
    pub const fn reason(&self) -> PoolError {
        self.reason
    }

    #[must_use]
    pub fn lease(&self) -> &BufferLease {
        &self.lease
    }

    #[must_use]
    pub fn into_parts(self) -> (PoolError, BufferLease) {
        (self.reason, *self.lease)
    }
}

impl fmt::Debug for ReleaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleaseError")
            .field("reason", &self.reason)
            .field("lease", &self.lease)
            .finish()
    }
}

impl fmt::Display for ReleaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "buffer release failed: {}", self.reason)
    }
}

impl Error for ReleaseError {}

struct PoolInner {
    buffer_budget: ByteBudget,
    resident_budget: ByteBudget,
    quarantine_budget: ByteBudget,
    quarantine_timeout: Duration,
    state: Mutex<PoolState>,
}

impl PoolInner {
    fn lock(&self) -> MutexGuard<'_, PoolState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn quarantine_storage(&self, storage: PooledStorage, now: Instant) -> Result<(), PoolError> {
        let capacity = storage.class.capacity();
        let quarantine_permit = match self.quarantine_budget.try_acquire(capacity) {
            Ok(permit) => permit,
            Err(error) => {
                let mut state = self.lock();
                state.metrics.leased_bytes -= capacity;
                state.metrics.retired_bytes += capacity;
                state.metrics.faulted = true;
                leak_storage(storage);
                return Err(PoolError::QuarantineBudget(error));
            }
        };
        let mut state = self.lock();
        state.metrics.leased_bytes -= capacity;
        state.metrics.quarantined_bytes += capacity;
        state.metrics.quarantine_count += 1;
        state.quarantine.push(QuarantineEntry {
            storage,
            quarantine_permit,
            deadline: now + self.quarantine_timeout,
        });
        Ok(())
    }

    fn quarantine_dropped(&self, storage: PooledStorage, _state: BufferState) {
        let capacity = storage.class.capacity();
        let now = Instant::now();
        if self.quarantine_storage(storage, now).is_ok() {
            self.lock().metrics.leaked_lease_count += 1;
        } else {
            let mut state = self.lock();
            state.metrics.leaked_lease_count += 1;
            debug_assert!(state.metrics.faulted);
        }
        debug_assert!(capacity > 0);
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for entry in state.quarantine.drain(..) {
            leak_storage(entry.storage);
        }
    }
}

struct PoolState {
    classes: Vec<ClassPool>,
    next_id: u64,
    metrics: BufferPoolMetrics,
    quarantine: Vec<QuarantineEntry>,
}

struct ClassPool {
    class: SizeClass,
    max_buffers: usize,
    allocated_buffers: usize,
    free: Vec<PooledStorage>,
}

struct PooledStorage {
    id: BufferId,
    class: SizeClass,
    bytes: Box<[u8]>,
    _buffer_permit: BytePermit,
    _resident_permit: BytePermit,
}

struct QuarantineEntry {
    storage: PooledStorage,
    quarantine_permit: BytePermit,
    deadline: Instant,
}

fn lease_belongs_to(lease: &BufferLease, pool: &Arc<PoolInner>) -> bool {
    lease
        .pool
        .upgrade()
        .is_some_and(|owner| Arc::ptr_eq(&owner, pool))
}

fn leak_storage(storage: PooledStorage) {
    let _ = Box::leak(Box::new(storage));
}

#[cfg(test)]
mod tests {
    use super::{
        BufferPool, BufferPoolConfig, BufferState, BufferTransitionError, OwnerTag, PoolError,
        SizeClass,
    };
    use crate::ByteBudget;
    use ariax_core::{Generation, TaskId};
    use std::time::{Duration, Instant};

    fn pool(total: usize, quarantine: usize) -> BufferPool {
        BufferPool::new(BufferPoolConfig::new(total, quarantine)).expect("pool")
    }

    fn releasable(mut lease: super::BufferLease) -> super::BufferLease {
        lease
            .transition(BufferState::NetworkFill, OwnerTag::Network)
            .expect("network fill");
        lease.mark_filled(4, OwnerTag::Storage).expect("filled");
        lease
            .transition(BufferState::Releasable, OwnerTag::Pool)
            .expect("releasable");
        lease
    }

    fn in_flight(mut lease: super::BufferLease) -> super::BufferLease {
        lease
            .transition(BufferState::NetworkFill, OwnerTag::Network)
            .expect("network fill");
        lease.mark_filled(4, OwnerTag::Storage).expect("filled");
        lease
            .transition(BufferState::DiskQueued, OwnerTag::Storage)
            .expect("disk queued");
        lease
            .transition(BufferState::DiskInFlight, OwnerTag::Disk)
            .expect("disk in flight");
        lease
    }

    #[test]
    fn lease_has_one_mutable_then_immutable_ownership_path() {
        let pool = pool(64 * 1024, 16 * 1024);
        let task = TaskId::new(7).expect("task");
        let mut lease = pool
            .try_reserve(
                1024,
                OwnerTag::Network,
                Some(task),
                Some(Generation::new(2)),
            )
            .expect("lease");
        lease
            .transition(BufferState::NetworkFill, OwnerTag::Network)
            .expect("fill");
        let pointer = lease.writable().expect("writable").as_ptr();
        lease.writable().expect("writable")[..4].copy_from_slice(b"aria");
        lease.mark_filled(4, OwnerTag::Storage).expect("filled");
        assert_eq!(lease.bytes().expect("readable"), b"aria");
        assert_eq!(lease.bytes().expect("readable").as_ptr(), pointer);
        assert_eq!(lease.task(), Some(task));
        assert_eq!(lease.generation(), Some(Generation::new(2)));
        assert_eq!(
            lease.writable().expect_err("immutable"),
            BufferTransitionError::NotMutable(BufferState::Filled)
        );
        lease
            .transition(BufferState::Releasable, OwnerTag::Pool)
            .expect("release state");
        pool.release(lease).expect("release");
    }

    #[test]
    fn invalid_transition_and_length_fail_without_losing_the_lease() {
        let pool = pool(16 * 1024, 16 * 1024);
        let mut lease = pool
            .try_reserve(1, OwnerTag::Network, None, None)
            .expect("lease");
        assert!(matches!(
            lease.transition(BufferState::DiskInFlight, OwnerTag::Disk),
            Err(BufferTransitionError::InvalidTransition { .. })
        ));
        assert!(matches!(
            lease.mark_filled(lease.capacity() + 1, OwnerTag::Storage),
            Err(BufferTransitionError::LengthExceedsCapacity { .. })
        ));
        lease
            .transition(BufferState::Releasable, OwnerTag::Pool)
            .expect("releasable");
        pool.release(lease).expect("release");
    }

    #[test]
    fn free_list_is_lifo_and_buffer_ids_are_stable() {
        let pool = pool(32 * 1024, 16 * 1024);
        let first = pool
            .try_reserve(1, OwnerTag::Network, None, None)
            .expect("first");
        let first_id = first.id();
        let second = pool
            .try_reserve(1, OwnerTag::Network, None, None)
            .expect("second");
        let second_id = second.id();
        pool.release(releasable(first)).expect("release first");
        pool.release(releasable(second)).expect("release second");
        let reused = pool
            .try_reserve(1, OwnerTag::Network, None, None)
            .expect("reused");
        assert_eq!(reused.id(), second_id);
        assert_ne!(reused.id(), first_id);
        pool.release(releasable(reused)).expect("release reused");
    }

    #[test]
    fn every_allocation_holds_buffer_and_resident_permits() {
        let mut config = BufferPoolConfig::new(32 * 1024, 16 * 1024);
        config.buffer_budget = ByteBudget::new(32 * 1024);
        config.resident_budget = ByteBudget::new(16 * 1024);
        let resident = config.resident_budget.clone();
        let pool = BufferPool::new(config).expect("pool");
        let first = pool
            .try_reserve(1, OwnerTag::Network, None, None)
            .expect("first");
        assert_eq!(resident.used(), 16 * 1024);
        assert!(matches!(
            pool.try_reserve(1, OwnerTag::Network, None, None),
            Err(PoolError::ResidentBudget(_))
        ));
        pool.release(releasable(first)).expect("release");
        assert_eq!(resident.used(), 16 * 1024, "free buffers remain resident");
    }

    #[test]
    fn dropped_lease_moves_to_quarantine_and_can_be_resolved() {
        let pool = pool(16 * 1024, 16 * 1024);
        let lease = pool
            .try_reserve(1, OwnerTag::Network, None, None)
            .expect("lease");
        let id = lease.id();
        drop(lease);
        let metrics = pool.metrics();
        assert_eq!(metrics.quarantined_bytes, 16 * 1024);
        assert_eq!(metrics.leaked_lease_count, 1);
        assert!(pool.resolve_quarantine(id));
        assert_eq!(pool.metrics().free_bytes, 16 * 1024);
    }

    #[test]
    fn quarantine_timeout_retires_without_releasing_pool_budgets() {
        let mut config = BufferPoolConfig::new(16 * 1024, 16 * 1024);
        config.quarantine_timeout = Duration::ZERO;
        let buffer_budget = config.buffer_budget.clone();
        let pool = BufferPool::new(config).expect("pool");
        let lease = in_flight(
            pool.try_reserve(1, OwnerTag::Network, None, None)
                .expect("lease"),
        );
        pool.quarantine(lease, Instant::now()).expect("quarantine");
        assert_eq!(pool.reap_quarantine(Instant::now()), 1);
        assert_eq!(pool.metrics().retired_bytes, 16 * 1024);
        assert_eq!(buffer_budget.used(), 16 * 1024);
        assert!(pool.try_reserve(1, OwnerTag::Network, None, None).is_err());
    }

    #[test]
    fn quarantine_exhaustion_faults_instead_of_growing_memory() {
        let pool = pool(32 * 1024, 16 * 1024);
        let first = in_flight(
            pool.try_reserve(1, OwnerTag::Network, None, None)
                .expect("first"),
        );
        let second = in_flight(
            pool.try_reserve(1, OwnerTag::Network, None, None)
                .expect("second"),
        );
        pool.quarantine(first, Instant::now())
            .expect("first quarantine");
        assert!(matches!(
            pool.quarantine(second, Instant::now()),
            Err(PoolError::QuarantineBudget(_))
        ));
        let metrics = pool.metrics();
        assert!(metrics.faulted);
        assert_eq!(metrics.quarantined_bytes, 16 * 1024);
        assert_eq!(metrics.retired_bytes, 16 * 1024);
        assert!(matches!(
            pool.try_reserve(1, OwnerTag::Network, None, None),
            Err(PoolError::Faulted)
        ));
    }

    #[test]
    fn class_limit_applies_backpressure() {
        let pool = pool(16 * 1024, 16 * 1024);
        let lease = pool
            .try_reserve(1, OwnerTag::Network, None, None)
            .expect("lease");
        assert!(matches!(
            pool.try_reserve(1, OwnerTag::Network, None, None),
            Err(PoolError::ClassExhausted(SizeClass::KiB16))
        ));
        pool.release(releasable(lease)).expect("release");
    }
}
