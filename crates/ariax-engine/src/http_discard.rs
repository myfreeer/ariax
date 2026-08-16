//! Finite hierarchical accounting for HTTP payload that was read but cannot
//! become useful or durable progress.

use ariax_core::TaskId;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Default process-wide cumulative discard ceiling. The guard is an abuse
/// bound, not a resident-memory reservation, so a large but finite value keeps
/// normal retry/endgame behavior possible without permitting unbounded waste.
pub const DEFAULT_HTTP_DISCARD_PROCESS_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// Default per-origin cumulative discard ceiling.
pub const DEFAULT_HTTP_DISCARD_HOST_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Default per-task cumulative discard ceiling.
pub const DEFAULT_HTTP_DISCARD_TASK_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Default per-attempt discard ceiling. Task admission scales this from the
/// configured piece length and bounded ingress frame.
pub const DEFAULT_HTTP_DISCARD_ATTEMPT_BYTES: u64 = 1024 * 1024 * 1024;
/// Internal multiplier used when deriving task/host ceilings from one lease.
pub const DEFAULT_HTTP_DISCARD_SCOPE_MULTIPLIER: u64 = 64;
/// Bound the process-owned host-key table so hostile source churn cannot turn
/// discard accounting into an unbounded metadata allocation.
pub const MAX_HTTP_DISCARD_HOST_SCOPES: usize = 4096;
/// Bound retained task counters; overflow tasks share one stricter bucket.
pub const MAX_HTTP_DISCARD_TASK_SCOPES: usize = 100_000;

/// Process and scope ceilings for the HTTP discard ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpDiscardBudgetLimits {
    pub process_bytes: u64,
    pub host_bytes: u64,
    pub task_bytes: u64,
    pub attempt_bytes: u64,
}

impl Default for HttpDiscardBudgetLimits {
    fn default() -> Self {
        Self {
            process_bytes: DEFAULT_HTTP_DISCARD_PROCESS_BYTES,
            host_bytes: DEFAULT_HTTP_DISCARD_HOST_BYTES,
            task_bytes: DEFAULT_HTTP_DISCARD_TASK_BYTES,
            attempt_bytes: DEFAULT_HTTP_DISCARD_ATTEMPT_BYTES,
        }
    }
}

impl HttpDiscardBudgetLimits {
    /// Derive conservative internal scope limits from the current HTTP lease
    /// and retry/endgame caps. The process ceiling remains the configured
    /// process-wide hard stop; task and host scopes are deliberately finite so
    /// one bad source cannot consume the entire process allowance.
    #[must_use]
    pub fn for_http_task(
        process_bytes: u64,
        piece_length: u64,
        ingress_frame_bytes: u64,
        max_attempts: u32,
        max_attempts_per_mirror: u32,
        endgame_max_duplicates: usize,
    ) -> Self {
        let attempt_bytes = piece_length.saturating_add(ingress_frame_bytes).max(1);
        let retry_factor = u64::from(max_attempts.max(1));
        let mirror_factor = u64::from(max_attempts_per_mirror.max(1));
        let endgame_factor = u64::try_from(endgame_max_duplicates)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let task_bytes = attempt_bytes
            .saturating_mul(retry_factor)
            .saturating_mul(endgame_factor)
            .saturating_mul(DEFAULT_HTTP_DISCARD_SCOPE_MULTIPLIER)
            .max(attempt_bytes);
        let host_bytes = attempt_bytes
            .saturating_mul(mirror_factor)
            .saturating_mul(endgame_factor)
            .saturating_mul(DEFAULT_HTTP_DISCARD_SCOPE_MULTIPLIER)
            .max(attempt_bytes);
        Self {
            process_bytes: process_bytes.max(1),
            host_bytes,
            task_bytes,
            attempt_bytes,
        }
    }

    pub(crate) fn scope(self) -> HttpDiscardScopeLimits {
        HttpDiscardScopeLimits {
            host_bytes: self.host_bytes,
            task_bytes: self.task_bytes,
            attempt_bytes: self.attempt_bytes,
        }
    }

    #[must_use]
    pub fn clamp_to(self, configured: Self) -> Self {
        Self {
            process_bytes: self.process_bytes.min(configured.process_bytes).max(1),
            host_bytes: self.host_bytes.min(configured.host_bytes).max(1),
            task_bytes: self.task_bytes.min(configured.task_bytes).max(1),
            attempt_bytes: self.attempt_bytes.min(configured.attempt_bytes).max(1),
        }
    }
}

/// Scope limits used when a task and its attempts are admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpDiscardScopeLimits {
    pub host_bytes: u64,
    pub task_bytes: u64,
    pub attempt_bytes: u64,
}

impl HttpDiscardScopeLimits {
    fn validate(self) -> Result<(), HttpDiscardBudgetError> {
        if self.host_bytes == 0 || self.task_bytes == 0 || self.attempt_bytes == 0 {
            return Err(HttpDiscardBudgetError::ZeroLimit);
        }
        Ok(())
    }
}

/// Why a discard ledger could not be constructed or allocated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpDiscardBudgetError {
    ZeroLimit,
    AttemptIdExhausted,
}

impl fmt::Display for HttpDiscardBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroLimit => "HTTP discard budgets must be nonzero",
            Self::AttemptIdExhausted => "HTTP discard attempt identity exhausted",
        })
    }
}

impl Error for HttpDiscardBudgetError {}

#[derive(Debug)]
struct ScopeState {
    limit: AtomicU64,
    consumed: AtomicU64,
}

#[derive(Debug)]
struct DiscardBudgetInner {
    configured: HttpDiscardBudgetLimits,
    process: Arc<ScopeState>,
    hosts: BTreeMap<String, Arc<ScopeState>>,
    overflow_host: Arc<ScopeState>,
    tasks: BTreeMap<u64, Arc<ScopeState>>,
    overflow_task: Arc<ScopeState>,
    next_attempt: u64,
}

/// One process-owned cumulative discard ledger. Clones share all hierarchy
/// levels and all charges are serialized so no partial hierarchy reservation
/// can leak when a scope reaches its ceiling.
#[derive(Clone, Debug)]
pub struct HttpDiscardBudget {
    inner: Arc<Mutex<DiscardBudgetInner>>,
}

impl Default for HttpDiscardBudget {
    fn default() -> Self {
        Self::new(HttpDiscardBudgetLimits::default())
            .expect("default HTTP discard limits are valid")
    }
}

impl HttpDiscardBudget {
    pub fn new(limits: HttpDiscardBudgetLimits) -> Result<Self, HttpDiscardBudgetError> {
        if limits.process_bytes == 0
            || limits.host_bytes == 0
            || limits.task_bytes == 0
            || limits.attempt_bytes == 0
        {
            return Err(HttpDiscardBudgetError::ZeroLimit);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(DiscardBudgetInner {
                configured: limits,
                process: Arc::new(ScopeState {
                    limit: AtomicU64::new(limits.process_bytes),
                    consumed: AtomicU64::new(0),
                }),
                hosts: BTreeMap::new(),
                overflow_host: Arc::new(ScopeState {
                    limit: AtomicU64::new(limits.host_bytes),
                    consumed: AtomicU64::new(0),
                }),
                tasks: BTreeMap::new(),
                overflow_task: Arc::new(ScopeState {
                    limit: AtomicU64::new(limits.task_bytes),
                    consumed: AtomicU64::new(0),
                }),
                next_attempt: 0,
            })),
        })
    }

    /// Begin or join one task scope. A repeated admission of the same task
    /// shares its cumulative counter; the larger requested limit wins.
    pub fn begin_task(
        &self,
        task: TaskId,
        limits: HttpDiscardScopeLimits,
    ) -> Result<HttpDiscardTaskGuard, HttpDiscardBudgetError> {
        limits.validate()?;
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scope = if let Some(scope) = inner.tasks.get(&task.get()).cloned() {
            scope
        } else if inner.tasks.len() >= MAX_HTTP_DISCARD_TASK_SCOPES {
            Arc::clone(&inner.overflow_task)
        } else {
            let scope = Arc::new(ScopeState {
                limit: AtomicU64::new(limits.task_bytes),
                consumed: AtomicU64::new(0),
            });
            inner.tasks.insert(task.get(), Arc::clone(&scope));
            scope
        };
        raise_limit(&scope.limit, limits.task_bytes);
        Ok(HttpDiscardTaskGuard {
            budget: self.clone(),
            task,
            scope,
            limits,
        })
    }

    #[must_use]
    pub fn process_snapshot(&self) -> HttpDiscardScopeSnapshot {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process = scope_snapshot(&inner.process);
        HttpDiscardScopeSnapshot {
            process_consumed: process.consumed,
            process_remaining: process.remaining,
            ..HttpDiscardScopeSnapshot::default()
        }
    }

    #[must_use]
    pub fn process_limit(&self) -> u64 {
        self.configured_limits().process_bytes
    }

    #[must_use]
    pub fn configured_limits(&self) -> HttpDiscardBudgetLimits {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .configured
    }
}

/// A task-owned discard scope.
#[derive(Clone, Debug)]
pub struct HttpDiscardTaskGuard {
    budget: HttpDiscardBudget,
    task: TaskId,
    scope: Arc<ScopeState>,
    limits: HttpDiscardScopeLimits,
}

impl HttpDiscardTaskGuard {
    #[must_use]
    pub fn task(&self) -> TaskId {
        self.task
    }

    pub fn begin_attempt(
        &self,
        host_key: impl Into<String>,
    ) -> Result<HttpDiscardAttemptGuard, HttpDiscardBudgetError> {
        self.begin_attempt_with_limit(host_key, self.limits.attempt_bytes)
    }

    pub fn begin_attempt_with_limit(
        &self,
        host_key: impl Into<String>,
        attempt_bytes: u64,
    ) -> Result<HttpDiscardAttemptGuard, HttpDiscardBudgetError> {
        let host_key = host_key.into();
        if host_key.is_empty() || attempt_bytes == 0 {
            return Err(HttpDiscardBudgetError::ZeroLimit);
        }
        let mut inner = self
            .budget
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let host = if let Some(host) = inner.hosts.get(&host_key).cloned() {
            host
        } else if inner.hosts.len() >= MAX_HTTP_DISCARD_HOST_SCOPES {
            Arc::clone(&inner.overflow_host)
        } else {
            let scope = Arc::new(ScopeState {
                limit: AtomicU64::new(self.limits.host_bytes),
                consumed: AtomicU64::new(0),
            });
            inner.hosts.insert(host_key.clone(), Arc::clone(&scope));
            scope
        };
        raise_limit(&host.limit, self.limits.host_bytes);
        inner.next_attempt = inner
            .next_attempt
            .checked_add(1)
            .ok_or(HttpDiscardBudgetError::AttemptIdExhausted)?;
        let attempt = Arc::new(ScopeState {
            limit: AtomicU64::new(attempt_bytes.min(self.limits.attempt_bytes)),
            consumed: AtomicU64::new(0),
        });
        Ok(HttpDiscardAttemptGuard {
            budget: self.budget.clone(),
            task: Arc::clone(&self.scope),
            host,
            attempt,
            host_key,
            attempt_id: inner.next_attempt,
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> HttpDiscardScopeSnapshot {
        let inner = self
            .budget
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process = scope_snapshot(&inner.process);
        let task = scope_snapshot(&self.scope);
        HttpDiscardScopeSnapshot {
            process_consumed: process.consumed,
            process_remaining: process.remaining,
            host_consumed: 0,
            host_remaining: 0,
            task_consumed: task.consumed,
            task_remaining: task.remaining,
            attempt_consumed: 0,
            attempt_remaining: 0,
        }
    }
}

/// One attempt's four-level discard scope.
#[derive(Clone, Debug)]
pub struct HttpDiscardAttemptGuard {
    budget: HttpDiscardBudget,
    task: Arc<ScopeState>,
    host: Arc<ScopeState>,
    attempt: Arc<ScopeState>,
    host_key: String,
    attempt_id: u64,
}

impl HttpDiscardAttemptGuard {
    #[must_use]
    pub fn host_key(&self) -> &str {
        &self.host_key
    }

    #[must_use]
    pub fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub fn available(&self) -> u64 {
        let snapshot = self.snapshot();
        snapshot
            .process_remaining
            .min(snapshot.host_remaining)
            .min(snapshot.task_remaining)
            .min(snapshot.attempt_remaining)
    }

    #[must_use]
    pub fn exhausted_scope(&self) -> Option<HttpDiscardScope> {
        let snapshot = self.snapshot();
        if snapshot.process_remaining == 0 {
            Some(HttpDiscardScope::Process)
        } else if snapshot.host_remaining == 0 {
            Some(HttpDiscardScope::Host)
        } else if snapshot.task_remaining == 0 {
            Some(HttpDiscardScope::Task)
        } else if snapshot.attempt_remaining == 0 {
            Some(HttpDiscardScope::Attempt)
        } else {
            None
        }
    }

    /// Charge bytes already read but known to be discarded. Charges are
    /// atomic across process/host/task/attempt; if the request crosses one
    /// remaining ceiling, only the common available prefix is consumed and
    /// the returned result tells the caller to stop immediately.
    pub fn charge(&self, bytes: usize) -> HttpDiscardCharge {
        self.charge_u64(u64::try_from(bytes).unwrap_or(u64::MAX))
    }

    pub fn charge_u64(&self, requested: u64) -> HttpDiscardCharge {
        if requested == 0 {
            return HttpDiscardCharge::charged(0);
        }
        let inner = self
            .budget
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scopes = [&inner.process, &self.host, &self.task, &self.attempt];
        let mut available = u64::MAX;
        let mut exhausted = None;
        for (index, scope) in scopes.iter().enumerate() {
            let remaining = scope
                .limit
                .load(Ordering::Acquire)
                .saturating_sub(scope.consumed.load(Ordering::Acquire));
            if remaining < available {
                available = remaining;
                exhausted = Some(match index {
                    0 => HttpDiscardScope::Process,
                    1 => HttpDiscardScope::Host,
                    2 => HttpDiscardScope::Task,
                    _ => HttpDiscardScope::Attempt,
                });
            }
        }
        let charged = requested.min(available);
        add_consumed(&inner.process, charged);
        add_consumed(&self.host, charged);
        add_consumed(&self.task, charged);
        add_consumed(&self.attempt, charged);
        if charged < requested {
            HttpDiscardCharge {
                requested,
                charged,
                exhausted,
            }
        } else {
            HttpDiscardCharge::charged(charged)
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> HttpDiscardScopeSnapshot {
        let inner = self
            .budget
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process = scope_snapshot(&inner.process);
        let host = scope_snapshot(&self.host);
        let task = scope_snapshot(&self.task);
        let attempt = scope_snapshot(&self.attempt);
        HttpDiscardScopeSnapshot {
            process_consumed: process.consumed,
            process_remaining: process.remaining,
            host_consumed: host.consumed,
            host_remaining: host.remaining,
            task_consumed: task.consumed,
            task_remaining: task.remaining,
            attempt_consumed: attempt.consumed,
            attempt_remaining: attempt.remaining,
        }
    }
}

/// The scope that stopped a charge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpDiscardScope {
    Process,
    Host,
    Task,
    Attempt,
}

impl HttpDiscardScope {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Host => "host",
            Self::Task => "task",
            Self::Attempt => "attempt",
        }
    }
}

/// Result of a cumulative discard charge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpDiscardCharge {
    pub requested: u64,
    pub charged: u64,
    pub exhausted: Option<HttpDiscardScope>,
}

impl HttpDiscardCharge {
    const fn charged(bytes: u64) -> Self {
        Self {
            requested: bytes,
            charged: bytes,
            exhausted: None,
        }
    }

    #[must_use]
    pub const fn exhausted(self) -> bool {
        self.exhausted.is_some()
    }
}

/// Cumulative accounting at all hierarchy levels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpDiscardScopeSnapshot {
    pub process_consumed: u64,
    pub process_remaining: u64,
    pub host_consumed: u64,
    pub host_remaining: u64,
    pub task_consumed: u64,
    pub task_remaining: u64,
    pub attempt_consumed: u64,
    pub attempt_remaining: u64,
}

#[derive(Clone, Copy, Debug)]
struct ScopeSnapshot {
    consumed: u64,
    remaining: u64,
}

fn scope_snapshot(scope: &ScopeState) -> ScopeSnapshot {
    let consumed = scope.consumed.load(Ordering::Acquire);
    let limit = scope.limit.load(Ordering::Acquire);
    ScopeSnapshot {
        consumed,
        remaining: limit.saturating_sub(consumed),
    }
}

fn add_consumed(scope: &ScopeState, bytes: u64) {
    scope.consumed.fetch_add(bytes, Ordering::AcqRel);
}

fn raise_limit(limit: &AtomicU64, requested: u64) {
    let mut current = limit.load(Ordering::Acquire);
    while current < requested {
        match limit.compare_exchange_weak(current, requested, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ariax_core::TaskId;

    #[test]
    fn hierarchical_charge_is_atomic_and_never_refunded() {
        let budget = HttpDiscardBudget::new(HttpDiscardBudgetLimits {
            process_bytes: 10,
            host_bytes: 8,
            task_bytes: 6,
            attempt_bytes: 4,
        })
        .expect("limits");
        let task = budget
            .begin_task(
                TaskId::new(1).expect("task"),
                HttpDiscardScopeLimits {
                    host_bytes: 8,
                    task_bytes: 6,
                    attempt_bytes: 4,
                },
            )
            .expect("task guard");
        let attempt = task.begin_attempt("https://example.test").expect("attempt");
        let first = attempt.charge(3);
        assert_eq!(first.charged, 3);
        assert!(!first.exhausted());
        let second = attempt.charge(3);
        assert_eq!(second.charged, 1);
        assert!(second.exhausted());
        assert_eq!(second.exhausted, Some(HttpDiscardScope::Attempt));
        let snapshot = attempt.snapshot();
        assert_eq!(snapshot.process_consumed, 4);
        assert_eq!(snapshot.host_consumed, 4);
        assert_eq!(snapshot.task_consumed, 4);
        assert_eq!(snapshot.attempt_consumed, 4);
        drop(attempt);
        assert_eq!(task.snapshot().task_consumed, 4);
    }

    #[test]
    fn host_and_process_scopes_are_shared_across_tasks() {
        let budget = HttpDiscardBudget::new(HttpDiscardBudgetLimits {
            process_bytes: 5,
            host_bytes: 4,
            task_bytes: 10,
            attempt_bytes: 10,
        })
        .expect("limits");
        let limits = HttpDiscardScopeLimits {
            host_bytes: 4,
            task_bytes: 10,
            attempt_bytes: 10,
        };
        let first = budget
            .begin_task(TaskId::new(1).expect("task"), limits)
            .expect("task");
        let second = budget
            .begin_task(TaskId::new(2).expect("task"), limits)
            .expect("task");
        let first_attempt = first.begin_attempt("same-host").expect("attempt");
        let second_attempt = second.begin_attempt("same-host").expect("attempt");
        assert_eq!(first_attempt.charge(3).charged, 3);
        let charge = second_attempt.charge(3);
        assert_eq!(charge.charged, 1);
        assert_eq!(charge.exhausted, Some(HttpDiscardScope::Host));
        assert_eq!(first_attempt.snapshot().host_consumed, 4);
        assert_eq!(budget.process_snapshot().process_consumed, 4);
    }

    #[test]
    fn host_charge_survives_finished_attempts() {
        let budget = HttpDiscardBudget::new(HttpDiscardBudgetLimits {
            process_bytes: 20,
            host_bytes: 4,
            task_bytes: 20,
            attempt_bytes: 4,
        })
        .expect("limits");
        let limits = HttpDiscardScopeLimits {
            host_bytes: 4,
            task_bytes: 20,
            attempt_bytes: 4,
        };
        let task = budget
            .begin_task(TaskId::new(1).expect("task"), limits)
            .expect("task");
        {
            let attempt = task.begin_attempt("same-host").expect("attempt");
            assert_eq!(attempt.charge(4).charged, 4);
        }
        let next = task.begin_attempt("same-host").expect("attempt");
        let charge = next.charge(1);
        assert_eq!(charge.charged, 0);
        assert_eq!(charge.exhausted, Some(HttpDiscardScope::Host));
    }
}
