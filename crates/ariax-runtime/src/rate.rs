//! Hierarchical, bounded payload-rate admission.
//!
//! A permit is reserved atomically across the global, host, task, and stream
//! buckets. It is deliberately consumed when the protocol accepts bytes, not
//! when disk durability later completes. Dropping an unsettled permit returns
//! its unused reservation; settling it with more bytes than reserved records
//! bounded library-frame overshoot as debt that later reads must repay.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

/// Default maximum payload reservation for one protocol read/poll.
pub const DEFAULT_RATE_QUANTUM_BYTES: usize = 64 * 1024;
/// Hard upper bound on one protocol read/poll reservation.
pub const MAX_RATE_QUANTUM_BYTES: usize = 1024 * 1024;
/// Default maximum number of independently waiting streams per direction.
pub const DEFAULT_RATE_MAX_WAITERS: usize = 100_000;
/// Hard maximum number of independently waiting streams per direction.
pub const MAX_RATE_WAITERS: usize = 100_000;
/// Hard cap on dynamically tracked host/task/stream bucket records.
pub const MAX_RATE_TRACKED_SCOPES: usize = 300_000;
/// A bounded idle burst avoids an unlimited catch-up after a long pause.
pub const MAX_RATE_BURST_BYTES: u64 = 4 * 1024 * 1024;

/// Transfer direction owned by an independent arbiter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateDirection {
    Download,
    Upload,
}

impl RateDirection {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Upload => "upload",
        }
    }
}

/// One token-bucket limit. A rate of zero is unlimited and must use a zero
/// burst; a finite rate always has a finite, nonzero burst.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimit {
    pub bytes_per_second: u64,
    pub burst_bytes: u64,
}

impl RateLimit {
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            bytes_per_second: 0,
            burst_bytes: 0,
        }
    }

    /// Constructs a finite rate with a one-second burst, capped so an idle
    /// task cannot release an unbounded catch-up burst.
    #[must_use]
    pub const fn per_second(bytes_per_second: u64) -> Self {
        if bytes_per_second == 0 {
            Self::unlimited()
        } else {
            Self {
                bytes_per_second,
                burst_bytes: if bytes_per_second > MAX_RATE_BURST_BYTES {
                    MAX_RATE_BURST_BYTES
                } else {
                    bytes_per_second
                },
            }
        }
    }

    #[must_use]
    pub const fn is_unlimited(self) -> bool {
        self.bytes_per_second == 0
    }

    fn validate(self) -> bool {
        (self.bytes_per_second == 0 && self.burst_bytes == 0)
            || (self.bytes_per_second != 0
                && self.burst_bytes != 0
                && self.burst_bytes <= MAX_RATE_BURST_BYTES)
    }
}

impl Default for RateLimit {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Stable opaque keys forming the global → host → task → stream path.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RatePath {
    pub host: u64,
    pub task: u64,
    pub stream: u64,
}

/// Profile-resolved defaults for one transfer direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateArbiterConfig {
    pub global: RateLimit,
    pub default_host: RateLimit,
    pub default_task: RateLimit,
    pub default_stream: RateLimit,
    pub quantum_bytes: NonZeroUsize,
    pub max_waiters: NonZeroUsize,
}

impl Default for RateArbiterConfig {
    fn default() -> Self {
        Self {
            global: RateLimit::unlimited(),
            default_host: RateLimit::unlimited(),
            default_task: RateLimit::unlimited(),
            default_stream: RateLimit::unlimited(),
            quantum_bytes: NonZeroUsize::new(DEFAULT_RATE_QUANTUM_BYTES)
                .expect("default rate quantum is nonzero"),
            max_waiters: NonZeroUsize::new(DEFAULT_RATE_MAX_WAITERS)
                .expect("default rate waiter cap is nonzero"),
        }
    }
}

impl RateArbiterConfig {
    fn validate(self) -> Result<Self, RateArbiterError> {
        if !self.global.validate()
            || !self.default_host.validate()
            || !self.default_task.validate()
            || !self.default_stream.validate()
            || self.quantum_bytes.get() > MAX_RATE_QUANTUM_BYTES
            || self.max_waiters.get() > MAX_RATE_WAITERS
        {
            return Err(RateArbiterError::InvalidConfig);
        }
        Ok(self)
    }
}

/// A live summary useful for rate/backpressure diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateArbiterStats {
    pub queued_waiters: usize,
    pub pending_grants: usize,
    pub tracked_scopes: usize,
    pub global_available_bytes: u64,
    pub global_debt_bytes: u64,
}

/// Result of settling a move-only rate permit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateCharge {
    pub accepted_bytes: usize,
    pub reserved_bytes: usize,
    pub debt_bytes: u64,
}

/// Why rate admission or configuration failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateArbiterError {
    InvalidConfig,
    ZeroReservation,
    WaiterLimit,
    TrackedScopeLimit,
}

impl RateArbiterError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_rate_arbiter_config",
            Self::ZeroReservation => "zero_rate_reservation",
            Self::WaiterLimit => "rate_waiter_limit",
            Self::TrackedScopeLimit => "rate_tracked_scope_limit",
        }
    }
}

impl fmt::Display for RateArbiterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for RateArbiterError {}

/// One process-owned direction arbiter. Clones address the same atomically
/// reserved bucket hierarchy.
#[derive(Clone)]
pub struct RateArbiter {
    inner: Arc<RateArbiterInner>,
}

impl fmt::Debug for RateArbiter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RateArbiter")
            .field("direction", &self.inner.direction)
            .field("stats", &self.stats())
            .finish()
    }
}

struct RateArbiterInner {
    direction: RateDirection,
    state: Mutex<RateArbiterState>,
    notify: Notify,
}

struct RateArbiterState {
    global_suspended: bool,
    config: RateArbiterConfig,
    global: Bucket,
    hosts: BTreeMap<u64, BucketEntry>,
    tasks: BTreeMap<u64, BucketEntry>,
    streams: BTreeMap<(u64, u64), BucketEntry>,
    waiters: VecDeque<Waiter>,
    grants: BTreeMap<u64, Grant>,
    next_waiter_id: u64,
}

#[derive(Clone, Copy)]
struct BucketEntry {
    bucket: Bucket,
    explicit: bool,
}

#[derive(Clone, Copy)]
struct Bucket {
    limit: RateLimit,
    tokens: i128,
    remainder: u128,
    updated_at: Instant,
}

struct Waiter {
    id: u64,
    path: RatePath,
    requested: usize,
}

#[derive(Clone, Copy)]
struct Grant {
    path: RatePath,
    bytes: usize,
}

impl Bucket {
    fn new(limit: RateLimit, now: Instant) -> Self {
        Self {
            limit,
            tokens: i128::from(limit.burst_bytes),
            remainder: 0,
            updated_at: now,
        }
    }

    fn reconfigure(&mut self, limit: RateLimit, now: Instant) {
        self.refill(now);
        self.limit = limit;
        self.remainder = 0;
        if limit.is_unlimited() {
            self.tokens = 0;
        } else {
            self.tokens = self.tokens.min(i128::from(limit.burst_bytes));
        }
        self.updated_at = now;
    }

    fn refill(&mut self, now: Instant) {
        if self.limit.is_unlimited() {
            self.updated_at = now;
            return;
        }
        let elapsed = now.saturating_duration_since(self.updated_at);
        self.updated_at = now;
        let numerator = u128::from(self.limit.bytes_per_second)
            .saturating_mul(elapsed.as_nanos())
            .saturating_add(self.remainder);
        let added = numerator / 1_000_000_000;
        self.remainder = numerator % 1_000_000_000;
        let added = i128::try_from(added).unwrap_or(i128::MAX);
        self.tokens = self
            .tokens
            .saturating_add(added)
            .min(i128::from(self.limit.burst_bytes));
    }

    fn available(&self) -> u64 {
        if self.limit.is_unlimited() {
            u64::MAX
        } else {
            u64::try_from(self.tokens.max(0)).unwrap_or(u64::MAX)
        }
    }

    fn reserve(&mut self, bytes: usize) {
        if !self.limit.is_unlimited() {
            self.tokens = self
                .tokens
                .saturating_sub(i128::try_from(bytes).unwrap_or(i128::MAX));
        }
    }

    fn refund(&mut self, bytes: usize) {
        if !self.limit.is_unlimited() {
            self.tokens = self
                .tokens
                .saturating_add(i128::try_from(bytes).unwrap_or(i128::MAX))
                .min(i128::from(self.limit.burst_bytes));
        }
    }

    fn debt(&self) -> u64 {
        if self.tokens >= 0 {
            0
        } else {
            u64::try_from(self.tokens.saturating_neg()).unwrap_or(u64::MAX)
        }
    }

    fn delay_until_one(&self) -> Duration {
        if self.limit.is_unlimited() || self.tokens >= 1 {
            return Duration::ZERO;
        }
        let needed = self.tokens.saturating_neg().saturating_add(1);
        let needed = u128::try_from(needed).unwrap_or(u128::MAX);
        let rate = u128::from(self.limit.bytes_per_second);
        let nanos = needed
            .saturating_mul(1_000_000_000)
            .saturating_add(rate.saturating_sub(1))
            / rate;
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

impl RateArbiterState {
    fn new(config: RateArbiterConfig, now: Instant) -> Self {
        Self {
            global_suspended: false,
            global: Bucket::new(config.global, now),
            config,
            hosts: BTreeMap::new(),
            tasks: BTreeMap::new(),
            streams: BTreeMap::new(),
            waiters: VecDeque::new(),
            grants: BTreeMap::new(),
            next_waiter_id: 1,
        }
    }

    fn tracked_scopes(&self) -> usize {
        self.hosts.len() + self.tasks.len() + self.streams.len()
    }

    fn ensure_path(&mut self, path: RatePath, now: Instant) -> Result<(), RateArbiterError> {
        let missing = usize::from(!self.hosts.contains_key(&path.host))
            + usize::from(!self.tasks.contains_key(&path.task))
            + usize::from(!self.streams.contains_key(&(path.task, path.stream)));
        if self.tracked_scopes().saturating_add(missing) > MAX_RATE_TRACKED_SCOPES {
            return Err(RateArbiterError::TrackedScopeLimit);
        }
        self.hosts.entry(path.host).or_insert_with(|| BucketEntry {
            bucket: Bucket::new(self.config.default_host, now),
            explicit: false,
        });
        self.tasks.entry(path.task).or_insert_with(|| BucketEntry {
            bucket: Bucket::new(self.config.default_task, now),
            explicit: false,
        });
        self.streams
            .entry((path.task, path.stream))
            .or_insert_with(|| BucketEntry {
                bucket: Bucket::new(self.config.default_stream, now),
                explicit: false,
            });
        Ok(())
    }

    fn refill_path(&mut self, path: RatePath, now: Instant) {
        self.global.refill(now);
        if let Some(entry) = self.hosts.get_mut(&path.host) {
            entry.bucket.refill(now);
        }
        if let Some(entry) = self.tasks.get_mut(&path.task) {
            entry.bucket.refill(now);
        }
        if let Some(entry) = self.streams.get_mut(&(path.task, path.stream)) {
            entry.bucket.refill(now);
        }
    }

    fn available_for(&mut self, path: RatePath, now: Instant) -> u64 {
        if self.global_suspended {
            return 0;
        }
        self.refill_path(path, now);
        [
            self.global.available(),
            self.hosts
                .get(&path.host)
                .map_or(u64::MAX, |entry| entry.bucket.available()),
            self.tasks
                .get(&path.task)
                .map_or(u64::MAX, |entry| entry.bucket.available()),
            self.streams
                .get(&(path.task, path.stream))
                .map_or(u64::MAX, |entry| entry.bucket.available()),
        ]
        .into_iter()
        .min()
        .unwrap_or(u64::MAX)
    }

    fn reserve(&mut self, path: RatePath, bytes: usize, now: Instant) {
        self.refill_path(path, now);
        self.global.reserve(bytes);
        if let Some(entry) = self.hosts.get_mut(&path.host) {
            entry.bucket.reserve(bytes);
        }
        if let Some(entry) = self.tasks.get_mut(&path.task) {
            entry.bucket.reserve(bytes);
        }
        if let Some(entry) = self.streams.get_mut(&(path.task, path.stream)) {
            entry.bucket.reserve(bytes);
        }
    }

    fn refund(&mut self, path: RatePath, bytes: usize, now: Instant) {
        self.refill_path(path, now);
        self.global.refund(bytes);
        if let Some(entry) = self.hosts.get_mut(&path.host) {
            entry.bucket.refund(bytes);
        }
        if let Some(entry) = self.tasks.get_mut(&path.task) {
            entry.bucket.refund(bytes);
        }
        if let Some(entry) = self.streams.get_mut(&(path.task, path.stream)) {
            entry.bucket.refund(bytes);
        }
    }

    fn charge_extra(&mut self, path: RatePath, bytes: usize, now: Instant) {
        self.reserve(path, bytes, now);
    }

    fn debt_for(&mut self, path: RatePath, now: Instant) -> u64 {
        self.refill_path(path, now);
        [
            self.global.debt(),
            self.hosts
                .get(&path.host)
                .map_or(0, |entry| entry.bucket.debt()),
            self.tasks
                .get(&path.task)
                .map_or(0, |entry| entry.bucket.debt()),
            self.streams
                .get(&(path.task, path.stream))
                .map_or(0, |entry| entry.bucket.debt()),
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }

    fn next_delay_for(&mut self, path: RatePath, now: Instant) -> Duration {
        if self.global_suspended {
            return Duration::from_secs(3600);
        }
        self.refill_path(path, now);
        [
            self.global.delay_until_one(),
            self.hosts
                .get(&path.host)
                .map_or(Duration::ZERO, |entry| entry.bucket.delay_until_one()),
            self.tasks
                .get(&path.task)
                .map_or(Duration::ZERO, |entry| entry.bucket.delay_until_one()),
            self.streams
                .get(&(path.task, path.stream))
                .map_or(Duration::ZERO, |entry| entry.bucket.delay_until_one()),
        ]
        .into_iter()
        .max()
        .unwrap_or(Duration::ZERO)
    }

    fn enqueue(&mut self, path: RatePath, requested: usize) -> Result<u64, RateArbiterError> {
        if self.waiters.len() >= self.config.max_waiters.get() {
            return Err(RateArbiterError::WaiterLimit);
        }
        let id = self.next_waiter_id;
        self.next_waiter_id = self.next_waiter_id.checked_add(1).unwrap_or(1);
        self.waiters.push_back(Waiter {
            id,
            path,
            requested,
        });
        Ok(id)
    }

    fn dispatch(&mut self, now: Instant) -> (Option<Duration>, bool) {
        let mut next_delay: Option<Duration> = None;
        let mut granted_any = false;
        let turns = self.waiters.len();
        for _ in 0..turns {
            let Some(waiter) = self.waiters.pop_front() else {
                break;
            };
            let available = self.available_for(waiter.path, now);
            let requested = waiter.requested.min(self.config.quantum_bytes.get());
            let grant = requested.min(usize::try_from(available).unwrap_or(usize::MAX));
            if grant != 0 {
                self.reserve(waiter.path, grant, now);
                self.grants.insert(
                    waiter.id,
                    Grant {
                        path: waiter.path,
                        bytes: grant,
                    },
                );
                granted_any = true;
            } else {
                let delay = self.next_delay_for(waiter.path, now);
                next_delay = Some(match next_delay {
                    Some(existing) => existing.min(delay),
                    None => delay,
                });
                self.waiters.push_back(waiter);
            }
        }
        (next_delay, granted_any)
    }

    fn remove_waiter(&mut self, id: u64, now: Instant) {
        if let Some(index) = self.waiters.iter().position(|waiter| waiter.id == id) {
            let _removed = self.waiters.remove(index);
        }
        if let Some(grant) = self.grants.remove(&id) {
            self.refund(grant.path, grant.bytes, now);
        }
    }

    fn reconfigure(&mut self, config: RateArbiterConfig, now: Instant) {
        self.global_suspended = false;
        self.global.reconfigure(config.global, now);
        for entry in self.hosts.values_mut().filter(|entry| !entry.explicit) {
            entry.bucket.reconfigure(config.default_host, now);
        }
        for entry in self.tasks.values_mut().filter(|entry| !entry.explicit) {
            entry.bucket.reconfigure(config.default_task, now);
        }
        for entry in self.streams.values_mut().filter(|entry| !entry.explicit) {
            entry.bucket.reconfigure(config.default_stream, now);
        }
        self.config = config;
    }

    fn set_scoped_limit(
        &mut self,
        scope: RateScope,
        limit: RateLimit,
        now: Instant,
    ) -> Result<(), RateArbiterError> {
        if !limit.validate() {
            return Err(RateArbiterError::InvalidConfig);
        }
        let tracked_scopes = self.tracked_scopes();
        match scope {
            RateScope::Host(key) => {
                set_limit_entry(&mut self.hosts, key, tracked_scopes, limit, now)?;
            }
            RateScope::Task(key) => {
                set_limit_entry(&mut self.tasks, key, tracked_scopes, limit, now)?;
            }
            RateScope::Stream { task, stream } => {
                set_limit_entry(
                    &mut self.streams,
                    (task, stream),
                    tracked_scopes,
                    limit,
                    now,
                )?;
            }
        }
        Ok(())
    }

    fn contains_scope(&self, scope: RateScope) -> bool {
        match scope {
            RateScope::Host(key) => self.hosts.contains_key(&key),
            RateScope::Task(key) => self.tasks.contains_key(&key),
            RateScope::Stream { task, stream } => self.streams.contains_key(&(task, stream)),
        }
    }
}

/// Hierarchy member for a live scoped limit change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateScope {
    Host(u64),
    Task(u64),
    Stream { task: u64, stream: u64 },
}

/// Validated update to an existing bucket, committed after option persistence.
#[must_use]
pub struct PreparedRateLimit {
    inner: Arc<RateArbiterInner>,
    scope: RateScope,
    limit: RateLimit,
}

impl PreparedRateLimit {
    pub fn apply(self) {
        let mut state = lock_unpoisoned(&self.inner.state);
        // Tracked buckets are never removed, so preparation reserves no new state.
        let entry = match self.scope {
            RateScope::Host(key) => state.hosts.get_mut(&key),
            RateScope::Task(key) => state.tasks.get_mut(&key),
            RateScope::Stream { task, stream } => state.streams.get_mut(&(task, stream)),
        }
        .expect("prepared rate bucket remains tracked");
        entry.bucket.reconfigure(self.limit, Instant::now());
        entry.explicit = true;
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

fn set_limit_entry<K: Ord>(
    map: &mut BTreeMap<K, BucketEntry>,
    key: K,
    tracked_scopes: usize,
    limit: RateLimit,
    now: Instant,
) -> Result<(), RateArbiterError> {
    if !map.contains_key(&key) && tracked_scopes >= MAX_RATE_TRACKED_SCOPES {
        return Err(RateArbiterError::TrackedScopeLimit);
    }
    map.entry(key)
        .and_modify(|entry| {
            entry.bucket.reconfigure(limit, now);
            entry.explicit = true;
        })
        .or_insert_with(|| BucketEntry {
            bucket: Bucket::new(limit, now),
            explicit: true,
        });
    Ok(())
}

impl RateArbiter {
    pub fn new(
        direction: RateDirection,
        config: RateArbiterConfig,
    ) -> Result<Self, RateArbiterError> {
        let config = config.validate()?;
        Ok(Self {
            inner: Arc::new(RateArbiterInner {
                direction,
                state: Mutex::new(RateArbiterState::new(config, Instant::now())),
                notify: Notify::new(),
            }),
        })
    }

    #[must_use]
    pub fn direction(&self) -> RateDirection {
        self.inner.direction
    }

    pub fn reconfigure(&self, config: RateArbiterConfig) -> Result<(), RateArbiterError> {
        let config = config.validate()?;
        let mut state = lock_unpoisoned(&self.inner.state);
        state.reconfigure(config, Instant::now());
        drop(state);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    /// A process share of `Some(0)` suspends new grants; `None` is unlimited.
    /// Accepted grants remain owned and configuration wakes every waiting stream.
    pub fn set_global_allocation(&self, bytes_per_second: Option<u64>) {
        let mut state = lock_unpoisoned(&self.inner.state);
        state.global_suspended = bytes_per_second == Some(0);
        let limit = RateLimit::per_second(bytes_per_second.unwrap_or(0));
        state.global.reconfigure(limit, Instant::now());
        state.config.global = limit;
        drop(state);
        self.inner.notify.notify_waiters();
    }

    pub fn set_global_limit(&self, limit: RateLimit) -> Result<(), RateArbiterError> {
        if !limit.validate() {
            return Err(RateArbiterError::InvalidConfig);
        }
        let mut state = lock_unpoisoned(&self.inner.state);
        state.global_suspended = false;
        state.global.reconfigure(limit, Instant::now());
        state.config.global = limit;
        drop(state);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    pub fn set_scoped_limit(
        &self,
        scope: RateScope,
        limit: RateLimit,
    ) -> Result<(), RateArbiterError> {
        let mut state = lock_unpoisoned(&self.inner.state);
        state.set_scoped_limit(scope, limit, Instant::now())?;
        drop(state);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    /// Returns no handle until the worker has registered the requested scope.
    pub fn prepare_scoped_limit(
        &self,
        scope: RateScope,
        limit: RateLimit,
    ) -> Result<Option<PreparedRateLimit>, RateArbiterError> {
        if !limit.validate() {
            return Err(RateArbiterError::InvalidConfig);
        }
        let state = lock_unpoisoned(&self.inner.state);
        let tracked = state.contains_scope(scope);
        Ok(tracked.then(|| PreparedRateLimit {
            inner: self.inner.clone(),
            scope,
            limit,
        }))
    }

    /// Attempts immediate admission. Existing queued readers retain priority,
    /// preventing a newly arriving stream from bypassing their fair turn.
    pub fn try_acquire(
        &self,
        path: RatePath,
        requested: NonZeroUsize,
    ) -> Result<Option<RatePermit>, RateArbiterError> {
        let now = Instant::now();
        let mut state = lock_unpoisoned(&self.inner.state);
        state.ensure_path(path, now)?;
        if !state.waiters.is_empty() {
            return Ok(None);
        }
        let available = state.available_for(path, now);
        let bytes = requested
            .get()
            .min(state.config.quantum_bytes.get())
            .min(usize::try_from(available).unwrap_or(usize::MAX));
        if bytes == 0 {
            return Ok(None);
        }
        state.reserve(path, bytes, now);
        Ok(Some(RatePermit::new(Arc::clone(&self.inner), path, bytes)))
    }

    /// Waits for one fair bounded reservation. Dropping this future removes
    /// its waiter and refunds any grant that raced with cancellation.
    pub async fn acquire(
        &self,
        path: RatePath,
        requested: NonZeroUsize,
    ) -> Result<RatePermit, RateArbiterError> {
        let mut registration = RateWaiterRegistration::new(Arc::clone(&self.inner));
        loop {
            let notified = self.inner.notify.notified();
            let wait = {
                let now = Instant::now();
                let mut state = lock_unpoisoned(&self.inner.state);
                state.ensure_path(path, now)?;
                if registration.id.is_none() {
                    registration.id = Some(state.enqueue(path, requested.get())?);
                }
                let (wait, granted_any) = state.dispatch(now);
                let id = registration.id.expect("waiter registration has an id");
                if let Some(grant) = state.grants.remove(&id) {
                    registration.id = None;
                    return Ok(RatePermit::new(
                        Arc::clone(&self.inner),
                        grant.path,
                        grant.bytes,
                    ));
                }
                (wait, granted_any)
            };
            if wait.1 {
                self.inner.notify.notify_waiters();
            }
            let wait = wait
                .0
                .unwrap_or(Duration::from_millis(1))
                .max(Duration::from_millis(1));
            tokio::select! {
                _ = notified => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    #[must_use]
    pub fn stats(&self) -> RateArbiterStats {
        let now = Instant::now();
        let mut state = lock_unpoisoned(&self.inner.state);
        state.global.refill(now);
        RateArbiterStats {
            queued_waiters: state.waiters.len(),
            pending_grants: state.grants.len(),
            tracked_scopes: state.tracked_scopes(),
            global_available_bytes: state.global.available(),
            global_debt_bytes: state.global.debt(),
        }
    }
}

/// Reservation that must be settled with bytes actually accepted by the
/// protocol. It cannot be cloned, preventing accidental double charging.
pub struct RatePermit {
    inner: Arc<RateArbiterInner>,
    path: RatePath,
    reserved: usize,
    settled: bool,
}

impl fmt::Debug for RatePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RatePermit")
            .field("path", &self.path)
            .field("reserved", &self.reserved)
            .finish_non_exhaustive()
    }
}

impl RatePermit {
    fn new(inner: Arc<RateArbiterInner>, path: RatePath, reserved: usize) -> Self {
        Self {
            inner,
            path,
            reserved,
            settled: false,
        }
    }

    #[must_use]
    pub const fn reserved_bytes(&self) -> usize {
        self.reserved
    }

    /// Consumes the reservation. Bytes above the reservation are charged as
    /// rate debt so a bounded library frame overshoot cannot lead to additional
    /// polling until ordinary refill repays it.
    #[must_use]
    pub fn settle(mut self, accepted_bytes: usize) -> RateCharge {
        let now = Instant::now();
        let mut state = lock_unpoisoned(&self.inner.state);
        if accepted_bytes < self.reserved {
            state.refund(self.path, self.reserved - accepted_bytes, now);
        } else if accepted_bytes > self.reserved {
            state.charge_extra(self.path, accepted_bytes - self.reserved, now);
        }
        let debt_bytes = state.debt_for(self.path, now);
        self.settled = true;
        drop(state);
        self.inner.notify.notify_waiters();
        RateCharge {
            accepted_bytes,
            reserved_bytes: self.reserved,
            debt_bytes,
        }
    }
}

impl Drop for RatePermit {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let mut state = lock_unpoisoned(&self.inner.state);
        state.refund(self.path, self.reserved, Instant::now());
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

struct RateWaiterRegistration {
    inner: Arc<RateArbiterInner>,
    id: Option<u64>,
}

impl RateWaiterRegistration {
    fn new(inner: Arc<RateArbiterInner>) -> Self {
        Self { inner, id: None }
    }
}

impl Drop for RateWaiterRegistration {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let mut state = lock_unpoisoned(&self.inner.state);
        state.remove_waiter(id, Instant::now());
        drop(state);
        self.inner.notify.notify_waiters();
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex as StdMutex};

    const PATH: RatePath = RatePath {
        host: 1,
        task: 2,
        stream: 3,
    };

    fn request(bytes: usize) -> NonZeroUsize {
        NonZeroUsize::new(bytes).expect("test request is nonzero")
    }

    #[test]
    fn unlimited_path_admits_and_returns_unused_reservation() {
        let arbiter = RateArbiter::new(RateDirection::Download, RateArbiterConfig::default())
            .expect("arbiter");
        let permit = arbiter
            .try_acquire(PATH, request(64))
            .expect("admission")
            .expect("unlimited permit");
        assert_eq!(permit.reserved_bytes(), 64);
        let charge = permit.settle(7);
        assert_eq!(charge.accepted_bytes, 7);
        assert_eq!(charge.debt_bytes, 0);
        assert_eq!(arbiter.stats().global_available_bytes, u64::MAX);
    }

    #[tokio::test]
    async fn zero_share_suspends_new_grants_and_allocation_wakes_waiters() {
        let arbiter = RateArbiter::new(RateDirection::Download, RateArbiterConfig::default())
            .expect("arbiter");
        let in_flight = arbiter.try_acquire(PATH, request(8)).unwrap().unwrap();
        arbiter.set_global_allocation(Some(0));
        assert!(arbiter.try_acquire(PATH, request(1)).unwrap().is_none());
        let _ = in_flight.settle(8);
        let waiting = {
            let arbiter = arbiter.clone();
            tokio::spawn(async move { arbiter.acquire(PATH, request(1)).await })
        };
        tokio::task::yield_now().await;
        assert_eq!(arbiter.stats().queued_waiters, 1);
        arbiter.set_global_allocation(Some(64));
        let permit = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _ = permit.settle(1);
        arbiter.set_global_allocation(Some(0));
        assert!(arbiter.try_acquire(PATH, request(1)).unwrap().is_none());
        arbiter.reconfigure(RateArbiterConfig::default()).unwrap();
        assert!(arbiter.try_acquire(PATH, request(1)).unwrap().is_some());
    }

    #[test]
    fn reservation_is_atomic_across_all_buckets() {
        let arbiter = RateArbiter::new(
            RateDirection::Download,
            RateArbiterConfig {
                global: RateLimit {
                    bytes_per_second: 100,
                    burst_bytes: 100,
                },
                default_host: RateLimit {
                    bytes_per_second: 80,
                    burst_bytes: 80,
                },
                default_task: RateLimit {
                    bytes_per_second: 60,
                    burst_bytes: 60,
                },
                default_stream: RateLimit {
                    bytes_per_second: 40,
                    burst_bytes: 40,
                },
                quantum_bytes: request(64),
                max_waiters: request(4),
            },
        )
        .expect("arbiter");

        let permit = arbiter
            .try_acquire(PATH, request(64))
            .expect("admission")
            .expect("permit");
        assert_eq!(permit.reserved_bytes(), 40);
        let _charge = permit.settle(40);
        assert!(
            arbiter
                .try_acquire(PATH, request(1))
                .expect("admission")
                .is_none()
        );
    }

    #[test]
    fn dropped_permit_refunds_every_bucket() {
        let arbiter = RateArbiter::new(
            RateDirection::Download,
            RateArbiterConfig {
                global: RateLimit {
                    bytes_per_second: 10,
                    burst_bytes: 10,
                },
                ..RateArbiterConfig::default()
            },
        )
        .expect("arbiter");
        let permit = arbiter
            .try_acquire(PATH, request(10))
            .expect("admission")
            .expect("permit");
        assert_eq!(arbiter.stats().global_available_bytes, 0);
        drop(permit);
        assert_eq!(arbiter.stats().global_available_bytes, 10);
    }

    #[tokio::test]
    async fn reconfiguration_wakes_a_queued_reader() {
        let arbiter = RateArbiter::new(
            RateDirection::Download,
            RateArbiterConfig {
                global: RateLimit {
                    bytes_per_second: 1,
                    burst_bytes: 1,
                },
                ..RateArbiterConfig::default()
            },
        )
        .expect("arbiter");
        let _ = arbiter
            .try_acquire(PATH, request(1))
            .expect("admission")
            .expect("initial permit")
            .settle(10_001);
        let waiting = {
            let arbiter = arbiter.clone();
            tokio::spawn(async move { arbiter.acquire(PATH, request(1)).await })
        };
        tokio::task::yield_now().await;
        assert_eq!(arbiter.stats().queued_waiters, 1);
        arbiter
            .set_global_limit(RateLimit::unlimited())
            .expect("reconfigure");
        let permit = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("waiter wake")
            .expect("task")
            .expect("permit");
        let _ = permit.settle(1);
    }

    #[tokio::test]
    async fn dropping_waiting_future_unregisters_it_without_token_leak() {
        let arbiter = RateArbiter::new(
            RateDirection::Download,
            RateArbiterConfig {
                global: RateLimit {
                    bytes_per_second: 1,
                    burst_bytes: 1,
                },
                ..RateArbiterConfig::default()
            },
        )
        .expect("arbiter");
        let _ = arbiter
            .try_acquire(PATH, request(1))
            .expect("admission")
            .expect("initial permit")
            .settle(10_001);
        let waiting = {
            let arbiter = arbiter.clone();
            tokio::spawn(async move { arbiter.acquire(PATH, request(1)).await })
        };
        tokio::task::yield_now().await;
        assert_eq!(arbiter.stats().queued_waiters, 1);
        waiting.abort();
        let _ = waiting.await;
        tokio::task::yield_now().await;
        assert_eq!(arbiter.stats().queued_waiters, 0);
        assert_eq!(arbiter.stats().pending_grants, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn settled_overshoot_creates_bounded_debt_until_refill() {
        let arbiter = RateArbiter::new(
            RateDirection::Download,
            RateArbiterConfig {
                global: RateLimit {
                    bytes_per_second: 100,
                    burst_bytes: 16,
                },
                quantum_bytes: request(64),
                ..RateArbiterConfig::default()
            },
        )
        .expect("arbiter");
        let permit = arbiter
            .try_acquire(PATH, request(16))
            .expect("admission")
            .expect("permit");
        let charge = permit.settle(32);
        assert_eq!(charge.reserved_bytes, 16);
        assert_eq!(charge.accepted_bytes, 32);
        assert_eq!(charge.debt_bytes, 16);
        assert_eq!(arbiter.stats().global_debt_bytes, 16);
        assert!(
            arbiter
                .try_acquire(PATH, request(1))
                .expect("debt admission")
                .is_none()
        );

        tokio::time::advance(Duration::from_millis(169)).await;
        assert_eq!(arbiter.stats().global_debt_bytes, 0);
        assert!(
            arbiter
                .try_acquire(PATH, request(1))
                .expect("pre-repayment admission")
                .is_none()
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        let permit = arbiter
            .try_acquire(PATH, request(1))
            .expect("repaid admission")
            .expect("one byte is available after debt repayment");
        let _charge = permit.settle(1);
    }

    #[tokio::test(start_paused = true)]
    async fn queued_streams_are_served_in_fifo_order_at_refill_boundaries() {
        let arbiter = RateArbiter::new(
            RateDirection::Download,
            RateArbiterConfig {
                global: RateLimit {
                    bytes_per_second: 10,
                    burst_bytes: 1,
                },
                quantum_bytes: request(1),
                max_waiters: request(8),
                ..RateArbiterConfig::default()
            },
        )
        .expect("arbiter");
        let _ = arbiter
            .try_acquire(PATH, request(1))
            .expect("initial admission")
            .expect("initial permit")
            .settle(1);
        let order = Arc::new(StdMutex::new(Vec::new()));
        let mut workers = Vec::new();
        for stream in 1..=4 {
            let arbiter = arbiter.clone();
            let order = Arc::clone(&order);
            workers.push(tokio::spawn(async move {
                let path = RatePath {
                    host: PATH.host,
                    task: PATH.task,
                    stream,
                };
                let permit = arbiter.acquire(path, request(1)).await.expect("permit");
                order.lock().expect("order lock").push(stream);
                let _charge = permit.settle(1);
            }));
            tokio::task::yield_now().await;
        }
        assert_eq!(arbiter.stats().queued_waiters, 4);

        for expected in 1..=4 {
            tokio::time::advance(Duration::from_millis(100)).await;
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                order.lock().expect("order lock").as_slice(),
                &(1..=expected).collect::<Vec<_>>()[..]
            );
        }
        for worker in workers {
            worker.await.expect("worker");
        }
    }

    #[test]
    fn prepared_scoped_limit_is_inert_until_committed() {
        let arbiter = RateArbiter::new(RateDirection::Download, RateArbiterConfig::default())
            .expect("arbiter");
        let scope = RateScope::Task(PATH.task);
        assert!(
            arbiter
                .prepare_scoped_limit(scope, RateLimit::per_second(1))
                .expect("untracked scope")
                .is_none()
        );
        arbiter
            .set_scoped_limit(scope, RateLimit::per_second(4))
            .expect("worker registers scope");
        assert!(matches!(
            arbiter.prepare_scoped_limit(
                scope,
                RateLimit {
                    bytes_per_second: 1,
                    burst_bytes: 0
                }
            ),
            Err(RateArbiterError::InvalidConfig)
        ));
        let update = arbiter
            .prepare_scoped_limit(scope, RateLimit::per_second(1))
            .expect("prepare")
            .expect("tracked");
        let permit = arbiter
            .try_acquire(PATH, request(4))
            .expect("admit old rate")
            .expect("old credit");
        assert_eq!(permit.reserved_bytes(), 4);
        drop(permit);
        drop(update);
        let permit = arbiter
            .try_acquire(PATH, request(4))
            .expect("drop retains rate")
            .expect("old credit");
        assert_eq!(permit.reserved_bytes(), 4);
        drop(permit);
        arbiter
            .prepare_scoped_limit(scope, RateLimit::per_second(1))
            .expect("prepare")
            .expect("tracked")
            .apply();
        let permit = arbiter
            .try_acquire(PATH, request(4))
            .expect("admit new rate")
            .expect("new credit");
        assert_eq!(permit.reserved_bytes(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn explicit_scoped_limits_survive_default_reconfiguration() {
        let arbiter = RateArbiter::new(
            RateDirection::Download,
            RateArbiterConfig {
                global: RateLimit {
                    bytes_per_second: 1_000,
                    burst_bytes: 1_000,
                },
                quantum_bytes: request(64),
                ..RateArbiterConfig::default()
            },
        )
        .expect("arbiter");
        arbiter
            .set_scoped_limit(
                RateScope::Task(PATH.task),
                RateLimit {
                    bytes_per_second: 10,
                    burst_bytes: 10,
                },
            )
            .expect("task limit");
        let permit = arbiter
            .try_acquire(PATH, request(64))
            .expect("admission")
            .expect("scoped permit");
        assert_eq!(permit.reserved_bytes(), 10);
        let _charge = permit.settle(10);
        arbiter
            .reconfigure(RateArbiterConfig {
                global: RateLimit::unlimited(),
                default_task: RateLimit::unlimited(),
                ..RateArbiterConfig::default()
            })
            .expect("default reconfigure");
        assert!(
            arbiter
                .try_acquire(PATH, request(1))
                .expect("explicit scope admission")
                .is_none()
        );
        tokio::time::advance(Duration::from_millis(100)).await;
        let permit = arbiter
            .try_acquire(PATH, request(1))
            .expect("refilled explicit scope admission")
            .expect("explicit scope refilled");
        let _charge = permit.settle(1);
    }

    #[test]
    fn one_thousand_active_streams_stay_within_scope_tracking_bound() {
        let arbiter = RateArbiter::new(RateDirection::Download, RateArbiterConfig::default())
            .expect("arbiter");
        for stream in 0..1_000 {
            let path = RatePath {
                host: stream,
                task: stream,
                stream,
            };
            let permit = arbiter
                .try_acquire(path, request(1))
                .expect("admission")
                .expect("unlimited permit");
            let _charge = permit.settle(1);
        }
        let stats = arbiter.stats();
        assert_eq!(stats.tracked_scopes, 3_000);
        assert!(stats.tracked_scopes <= MAX_RATE_TRACKED_SCOPES);
        assert_eq!(stats.queued_waiters, 0);
        assert_eq!(stats.pending_grants, 0);
    }
}
