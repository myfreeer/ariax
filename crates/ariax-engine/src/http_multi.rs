//! Journal-backed non-overlapping multi-mirror HTTP range worker.

use crate::http_first_slice::{
    KnownLengthHttpError, append_http_strong_validator, append_initial_admission_with_options,
    append_layout, build_single_file_layout, now_unix_ms,
};
use crate::{
    HttpCancellation, HttpClientRequest, HttpContentChecksum, HttpDiscardAttemptGuard,
    HttpDiscardBudget, HttpDiscardBudgetError, HttpDiscardBudgetLimits, HttpDiscardScope,
    HttpDiscardTaskGuard, HttpMirrorIdentityPolicy, HttpOverlapSettlement, HttpPolicyClient,
    HttpPolicyClientError, HttpRangeAssignment, HttpRangeCoordinator, HttpRangeCoordinatorConfig,
    HttpRangeCoordinatorError, HttpRangeFailure, HttpRangePoll, HttpRangeResponseError,
    HttpRangeResponseValidator, HttpRangeSource, HttpRepresentationDigest, HttpRetryBudget,
    HttpRetryCause, HttpRetryDecision, HttpRetryDelaySource, HttpRetryError, HttpRetryPolicy,
    HttpRetryStopReason, HttpRetryTransportFailure, HttpStaleValidatorPolicy, HttpTaskSpec,
    HttpTaskWorker, HttpTransportError, HttpWorkerFuture, HttpWorkerSuccess, LeaseCommit,
    LeaseWritePlan, RetryStateWrite, StorageEngine, StorageEngineConfig, StorageEngineError,
    WriteAck, WriteBlock, WriteReject,
};
use ariax_core::{
    ErrorKind, FileId, Generation, Gid, LeaseId, MonotonicInstant, PersistedDelayDecision, PieceId,
    PublicError, RetryClass, TaskId, TransferAttemptId, UriId,
};
use ariax_runtime::{
    BudgetError, BufferLease, ByteBudget, BytePermit, ConnectionCondition,
    ConnectionConditionReason, OwnerTag, RateArbiter, RateArbiterConfig, RateDirection, RateLimit,
    RatePath, RatePermit, RateScope, SizeClass, StatsCounters, StatsDiagnostic, StatsProfile,
    StatsSampler, StatsSamplerConfig,
};
use ariax_storage::{
    ControlJournalAppender, FileLayout, GlobalSpan, JournalContributor, JournalDigest,
    JournalDigestAlgorithm, JournalDirectoryCapability, JournalId, JournalStateLimits,
    LeaseAbortReason, PersistedId, PersistedSpan, PlatformPath, RecoveredDurablePiece,
    RecoveredHttpRangeIdentity, RecoveredHttpStrongValidator, RecoveredJournalState,
    RecoveredRetryState, ReplayLimits, RetryReason, RetryScope, RootDirectoryCapability,
    RootFileCapability, SessionCommand, SessionHandle, SessionOwnerError, SessionPersistenceError,
    calculate_validator_set_fingerprint, recover_journal_state,
};
use hyper::header::RETRY_AFTER;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinSet};

pub const MAX_HTTP_RANGE_EVENT_CAPACITY: usize = 4096;
pub const DEFAULT_HTTP_RANGE_EVENT_CAPACITY: usize = 64;
pub const DEFAULT_HTTP_INGRESS_BUDGET_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_HTTP_INGRESS_FRAME_BYTES: usize = SizeClass::MiB1.capacity();
pub const DEFAULT_HTTP_DIGEST_WORKERS: usize = 1;
pub const MAX_HTTP_DIGEST_WORKERS: usize = 64;
const HTTP_JOURNAL_ID_DOMAIN: &str = "ariax/http-journal-id/v1\0";
const HTTP_RECOVERY_READ_BUFFER_BYTES: usize = 64 * 1024;
const HTTP_FINAL_DIGEST_READ_BUFFER_BYTES: usize = 1024 * 1024;

#[cfg(test)]
fn oversized_range_body_faults() -> &'static Mutex<BTreeSet<String>> {
    static FAULTS: std::sync::OnceLock<Mutex<BTreeSet<String>>> = std::sync::OnceLock::new();
    FAULTS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
struct OversizedRangeBodyFaultGuard {
    uri: String,
}

#[cfg(test)]
impl Drop for OversizedRangeBodyFaultGuard {
    fn drop(&mut self) {
        oversized_range_body_faults()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.uri);
    }
}

#[cfg(test)]
fn arm_oversized_range_body_fault(uri: String) -> OversizedRangeBodyFaultGuard {
    assert!(
        oversized_range_body_faults()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(uri.clone()),
        "oversized range-body fault was already armed for {uri}"
    );
    OversizedRangeBodyFaultGuard { uri }
}

#[cfg(test)]
fn inject_oversized_range_body_fault(
    uri: &str,
    data: bytes::Bytes,
    buffer_capacity: usize,
) -> bytes::Bytes {
    let armed = oversized_range_body_faults()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(uri);
    if !armed || data.len() > buffer_capacity {
        return data;
    }
    let oversized_len = buffer_capacity
        .checked_add(1)
        .expect("HTTP ingress capacity is bounded below usize::MAX");
    let mut oversized = Vec::with_capacity(oversized_len);
    oversized.extend_from_slice(&data);
    oversized.resize(oversized_len, 0xa5);
    bytes::Bytes::from(oversized)
}

/// Process-owned HTTP ingress domain paired with the global resident budget.
#[derive(Clone, Debug)]
pub struct HttpIngressBudgets {
    ingress: ByteBudget,
    resident: ByteBudget,
}

impl HttpIngressBudgets {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            ingress: ByteBudget::new(limit),
            resident: ByteBudget::new(limit),
        }
    }

    pub(crate) fn with_shared_resident(limit: usize, resident: ByteBudget) -> Self {
        Self {
            ingress: ByteBudget::new(limit),
            resident,
        }
    }

    #[must_use]
    pub fn limit(&self) -> usize {
        self.ingress.limit()
    }

    #[must_use]
    pub fn used(&self) -> usize {
        self.ingress.used()
    }

    #[must_use]
    pub fn resident_used(&self) -> usize {
        self.resident.used()
    }

    pub fn try_acquire(&self, bytes: usize) -> Result<HttpIngressPermit, BudgetError> {
        let ingress = self.ingress.try_acquire(bytes)?;
        let resident = self.resident.try_acquire(bytes)?;
        Ok(HttpIngressPermit {
            _ingress: ingress,
            _resident: resident,
        })
    }
}

/// Dual domain/global charge retained until one response frame leaves ingress.
pub struct HttpIngressPermit {
    _ingress: BytePermit,
    _resident: BytePermit,
}

impl fmt::Debug for HttpIngressPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpIngressPermit")
            .finish_non_exhaustive()
    }
}

/// Stable, non-secret trigger retained for the latest task-local retry
/// decision. Recovered journal state preserves the durable error class even
/// when the original protocol-specific trigger was not persisted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryDiagnosticCause {
    Live(HttpRetryCause),
    Recovered(ErrorKind),
    WorkerPanic,
}

impl HttpRetryDiagnosticCause {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Live(cause) => cause.code(),
            Self::Recovered(_) => "recovered",
            Self::WorkerPanic => "worker-panic",
        }
    }

    #[must_use]
    pub const fn http_status(self) -> Option<u16> {
        match self {
            Self::Live(cause) => cause.http_status(),
            Self::Recovered(_) | Self::WorkerPanic => None,
        }
    }

    #[must_use]
    pub const fn recovered_error_class(self) -> Option<ErrorKind> {
        match self {
            Self::Recovered(error) => Some(error),
            Self::Live(_) | Self::WorkerPanic => None,
        }
    }
}

/// Live policy source or the coarser durable reason reconstructed at restart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryDelayDiagnostic {
    Live(HttpRetryDelaySource),
    Recovered(RetryReason),
}

impl HttpRetryDelayDiagnostic {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Live(source) => source.code(),
            Self::Recovered(reason) => reason.code(),
        }
    }
}

/// Scheduler action selected by the latest retry decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryNextAction {
    RetryRange,
    SameSource,
    DifferentSource,
    DisableSource,
    RevalidateSource,
    RestartGeneration,
    TerminalFailure,
}

impl HttpRetryNextAction {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RetryRange => "retry-range",
            Self::SameSource => "same-source",
            Self::DifferentSource => "different-source",
            Self::DisableSource => "disable-source",
            Self::RevalidateSource => "revalidate-source",
            Self::RestartGeneration => "restart-generation",
            Self::TerminalFailure => "terminal-failure",
        }
    }
}

/// Storage disposition of the failed provisional lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryLeaseDisposition {
    Unopened,
    Aborted,
    RolledBack,
    UnknownRecovered,
}

impl HttpRetryLeaseDisposition {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Unopened => "unopened",
            Self::Aborted => "aborted",
            Self::RolledBack => "rolled-back",
            Self::UnknownRecovered => "unknown-recovered",
        }
    }
}

/// One bounded task-local retry decision. Numeric source, piece, and lease
/// identities avoid publishing credential-bearing URI text through RPC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRetryDiagnosticSnapshot {
    pub cause: HttpRetryDiagnosticCause,
    pub source: UriId,
    pub piece: PieceId,
    pub prior_lease: Option<LeaseId>,
    pub next_lease: Option<LeaseId>,
    pub total_attempt: u32,
    pub total_remaining: u32,
    pub source_attempt: u32,
    pub source_remaining: u32,
    pub scheduled_at_unix_ms: u64,
    pub delay_ms: u64,
    pub retry_at_unix_ms: u64,
    pub delay: Option<HttpRetryDelayDiagnostic>,
    pub stop_reason: Option<HttpRetryStopReason>,
    pub next_action: HttpRetryNextAction,
    pub lease_disposition: HttpRetryLeaseDisposition,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpTransferStatsSnapshot {
    pub total_length: u64,
    pub raw_body_bytes: u64,
    pub accepted_bytes: u64,
    pub provisional_bytes: u64,
    pub durable_bytes: u64,
    pub discarded_bytes: u64,
    pub discard_budget_consumed: u64,
    pub discard_budget_remaining: u64,
    pub retry_count: u64,
    pub active_connections: u64,
    pub current_speed: u64,
    pub wire_speed: u64,
    pub useful_speed: u64,
    pub durable_speed: u64,
    pub smoothed_speed: u64,
    pub sample_age: Duration,
    pub connection_condition: ConnectionCondition,
    pub condition_reason: Option<ConnectionConditionReason>,
    pub rate_debt_bytes: u64,
    pub retry_diagnostic: Option<HttpRetryDiagnosticSnapshot>,
}

/// Durable completion evidence written by the worker before the scheduler
/// persists its stopped-result row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpCompletedEvidence {
    pub layout_hash: ariax_storage::JournalHash,
    pub total_length: u64,
    pub completed_at_unix_ms: u64,
    pub terminal_sequence: u64,
}

#[derive(Debug)]
struct HttpTransferStatsInner {
    total_length: AtomicU64,
    raw_body_bytes: AtomicU64,
    accepted_bytes: AtomicU64,
    provisional_bytes: AtomicU64,
    durable_bytes: AtomicU64,
    discarded_bytes: AtomicU64,
    discard_budget_consumed: AtomicU64,
    discard_budget_remaining: AtomicU64,
    retry_count: AtomicU64,
    active_connections: AtomicU64,
    rate_debt_bytes: AtomicU64,
    diagnostic: Mutex<StatsDiagnostic>,
    retry_diagnostic: Mutex<Option<HttpRetryDiagnosticSnapshot>>,
    speed: Mutex<HttpSpeedState>,
}

impl Default for HttpTransferStatsInner {
    fn default() -> Self {
        Self {
            total_length: AtomicU64::new(0),
            raw_body_bytes: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
            provisional_bytes: AtomicU64::new(0),
            durable_bytes: AtomicU64::new(0),
            discarded_bytes: AtomicU64::new(0),
            discard_budget_consumed: AtomicU64::new(0),
            discard_budget_remaining: AtomicU64::new(0),
            retry_count: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            rate_debt_bytes: AtomicU64::new(0),
            diagnostic: Mutex::new(StatsDiagnostic::default()),
            retry_diagnostic: Mutex::new(None),
            speed: Mutex::new(HttpSpeedState::new(ariax_core::MonotonicInstant::now())),
        }
    }
}

#[derive(Debug)]
struct HttpSpeedState {
    sampler: StatsSampler<()>,
    current_speed: u64,
    wire_speed: u64,
    useful_speed: u64,
    durable_speed: u64,
    smoothed_speed: u64,
    sampled_at: ariax_core::MonotonicInstant,
}

impl HttpSpeedState {
    fn new(at: ariax_core::MonotonicInstant) -> Self {
        let mut sampler = StatsSampler::new(StatsSamplerConfig::for_profile(
            StatsProfile::Latency,
            NonZeroUsize::new(1).expect("HTTP stats capacity is nonzero"),
        ))
        .expect("one HTTP stats entry is within the runtime sampler cap");
        sampler
            .register((), StatsCounters::default(), StatsDiagnostic::default(), at)
            .expect("fresh HTTP stats sampler accepts its first entry");
        Self {
            sampler,
            current_speed: 0,
            wire_speed: 0,
            useful_speed: 0,
            durable_speed: 0,
            smoothed_speed: 0,
            sampled_at: at,
        }
    }

    fn sample(
        &mut self,
        counters: StatsCounters,
        diagnostic: StatsDiagnostic,
        at: ariax_core::MonotonicInstant,
    ) {
        if self.sampler.update(&(), counters, diagnostic).is_err() {
            *self = Self::new(at);
            return;
        }
        if let Ok(Some(samples)) = self.sampler.sample_at(at)
            && let Some(sample) = samples.into_iter().next()
        {
            self.current_speed = sample.current_speed;
            self.wire_speed = sample.wire_speed;
            self.useful_speed = sample.useful_speed;
            self.durable_speed = sample.durable_speed;
            self.smoothed_speed = sample.smoothed_speed;
            self.sampled_at = sample.sampled_at;
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct HttpTransferStats {
    inner: Arc<HttpTransferStatsInner>,
}

impl HttpTransferStats {
    fn begin(&self) {
        for value in [
            &self.inner.total_length,
            &self.inner.raw_body_bytes,
            &self.inner.accepted_bytes,
            &self.inner.provisional_bytes,
            &self.inner.durable_bytes,
            &self.inner.discarded_bytes,
            &self.inner.discard_budget_consumed,
            &self.inner.discard_budget_remaining,
            &self.inner.retry_count,
            &self.inner.active_connections,
            &self.inner.rate_debt_bytes,
        ] {
            value.store(0, Ordering::Relaxed);
        }
        *self
            .inner
            .diagnostic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = StatsDiagnostic::default();
        *self
            .inner
            .retry_diagnostic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.reset_sampling_at(ariax_core::MonotonicInstant::now());
    }

    #[must_use]
    pub fn snapshot(&self) -> HttpTransferStatsSnapshot {
        self.snapshot_at(ariax_core::MonotonicInstant::now())
    }

    fn set_total_length(&self, value: u64) {
        self.inner.total_length.store(value, Ordering::Relaxed);
    }

    fn add_raw(&self, value: usize) {
        self.inner
            .raw_body_bytes
            .fetch_add(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn add_accepted(&self, value: usize) {
        self.inner
            .accepted_bytes
            .fetch_add(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn add_provisional(&self, value: usize) {
        self.inner
            .provisional_bytes
            .fetch_add(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn remove_provisional(&self, value: usize) {
        let value = u64::try_from(value).unwrap_or(u64::MAX);
        let _updated = self.inner.provisional_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_sub(value)),
        );
    }

    fn add_durable(&self, value: usize) {
        self.inner
            .durable_bytes
            .fetch_add(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn set_durable(&self, value: u64) {
        self.inner.durable_bytes.store(value, Ordering::Relaxed);
        self.inner.accepted_bytes.store(value, Ordering::Relaxed);
    }

    fn add_discarded_u64(&self, value: u64) {
        self.inner
            .discarded_bytes
            .fetch_add(value, Ordering::Relaxed);
    }

    fn set_discarded(&self, value: u64) {
        self.inner.discarded_bytes.store(value, Ordering::Relaxed);
    }

    fn set_discard_budget(&self, consumed: u64, remaining: u64) {
        self.inner
            .discard_budget_consumed
            .store(consumed, Ordering::Relaxed);
        self.inner
            .discard_budget_remaining
            .store(remaining, Ordering::Relaxed);
    }

    fn add_retry(&self) {
        self.inner.retry_count.fetch_add(1, Ordering::Relaxed);
    }

    fn set_retry_count(&self, value: u64) {
        self.inner.retry_count.store(value, Ordering::Relaxed);
    }

    fn set_rate_debt(&self, value: u64) {
        self.inner.rate_debt_bytes.store(value, Ordering::Relaxed);
    }

    fn set_diagnostic(&self, diagnostic: StatsDiagnostic) {
        *self
            .inner
            .diagnostic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = diagnostic;
    }

    fn clear_diagnostic(&self) {
        self.set_diagnostic(StatsDiagnostic::default());
    }

    fn set_retry_diagnostic(&self, diagnostic: HttpRetryDiagnosticSnapshot) {
        *self
            .inner
            .retry_diagnostic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(diagnostic);
    }

    fn bind_retry_lease(&self, piece: PieceId, source: UriId, lease: LeaseId) {
        let mut diagnostic = self
            .inner
            .retry_diagnostic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(current) = diagnostic.as_mut() else {
            return;
        };
        if current.piece != piece || current.next_lease.is_some() {
            return;
        }
        current.next_lease = Some(lease);
        current.next_action = if current.source == source {
            HttpRetryNextAction::SameSource
        } else {
            HttpRetryNextAction::DifferentSource
        };
    }

    fn set_active(&self, value: usize) {
        self.inner
            .active_connections
            .store(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn reset_sampling_at(&self, at: ariax_core::MonotonicInstant) {
        *self
            .inner
            .speed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = HttpSpeedState::new(at);
    }

    fn snapshot_at(&self, at: ariax_core::MonotonicInstant) -> HttpTransferStatsSnapshot {
        let total_length = self.inner.total_length.load(Ordering::Relaxed);
        let raw_body_bytes = self.inner.raw_body_bytes.load(Ordering::Relaxed);
        let accepted_bytes = self.inner.accepted_bytes.load(Ordering::Relaxed);
        let provisional_bytes = self.inner.provisional_bytes.load(Ordering::Relaxed);
        let durable_bytes = self.inner.durable_bytes.load(Ordering::Relaxed);
        let discarded_bytes = self.inner.discarded_bytes.load(Ordering::Relaxed);
        let discard_budget_consumed = self.inner.discard_budget_consumed.load(Ordering::Relaxed);
        let discard_budget_remaining = self.inner.discard_budget_remaining.load(Ordering::Relaxed);
        let retry_count = self.inner.retry_count.load(Ordering::Relaxed);
        let active_connections = self.inner.active_connections.load(Ordering::Relaxed);
        let rate_debt_bytes = self.inner.rate_debt_bytes.load(Ordering::Relaxed);
        let retry_diagnostic = *self
            .inner
            .retry_diagnostic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let diagnostic = *self
            .inner
            .diagnostic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let accepted_for_sample = accepted_bytes.max(durable_bytes);
        let received_for_sample = raw_body_bytes.max(accepted_for_sample).max(discarded_bytes);
        let counters = StatsCounters {
            received_payload_bytes: received_for_sample,
            accepted_bytes: accepted_for_sample,
            submitted_bytes: accepted_for_sample,
            provisional_in_flight_bytes: 0,
            committed_bytes: durable_bytes,
            durable_bytes,
            discarded_bytes,
            discard_budget_consumed,
            discard_budget_remaining,
            ..StatsCounters::default()
        };
        let (current_speed, wire_speed, useful_speed, durable_speed, smoothed_speed, sample_age) = {
            let mut speed = self
                .inner
                .speed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            speed.sample(counters, diagnostic, at);
            (
                speed.current_speed,
                speed.wire_speed,
                speed.useful_speed,
                speed.durable_speed,
                speed.smoothed_speed,
                at.duration_since(speed.sampled_at),
            )
        };
        HttpTransferStatsSnapshot {
            total_length,
            raw_body_bytes,
            accepted_bytes,
            provisional_bytes,
            durable_bytes,
            discarded_bytes,
            discard_budget_consumed,
            discard_budget_remaining,
            retry_count,
            active_connections,
            current_speed,
            wire_speed,
            useful_speed,
            durable_speed,
            smoothed_speed,
            sample_age,
            connection_condition: diagnostic.condition,
            condition_reason: diagnostic.reason,
            rate_debt_bytes,
            retry_diagnostic,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SharedHttpTransferStats {
    capacity: NonZeroUsize,
    by_task: Arc<RwLock<BTreeMap<TaskId, HttpTransferStats>>>,
    completed: Arc<RwLock<BTreeMap<TaskId, HttpCompletedEvidence>>>,
}

impl SharedHttpTransferStats {
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            by_task: Arc::new(RwLock::new(BTreeMap::new())),
            completed: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn get_or_create(&self, task: TaskId) -> Result<HttpTransferStats, HttpStatsCatalogError> {
        if let Some(stats) = read_unpoisoned(&self.by_task).get(&task).cloned() {
            return Ok(stats);
        }
        let mut catalog = write_unpoisoned(&self.by_task);
        if let Some(stats) = catalog.get(&task).cloned() {
            return Ok(stats);
        }
        if catalog.len() == self.capacity.get() {
            return Err(HttpStatsCatalogError::Full);
        }
        let stats = HttpTransferStats::default();
        catalog.insert(task, stats.clone());
        Ok(stats)
    }

    #[must_use]
    pub fn get(&self, task: TaskId) -> Option<HttpTransferStats> {
        read_unpoisoned(&self.by_task).get(&task).cloned()
    }

    pub fn remove(&self, task: TaskId) -> Option<HttpTransferStats> {
        write_unpoisoned(&self.completed).remove(&task);
        write_unpoisoned(&self.by_task).remove(&task)
    }

    pub fn record_completion(&self, task: TaskId, evidence: HttpCompletedEvidence) {
        write_unpoisoned(&self.completed).insert(task, evidence);
    }

    #[must_use]
    pub fn completion(&self, task: TaskId) -> Option<HttpCompletedEvidence> {
        read_unpoisoned(&self.completed).get(&task).copied()
    }

    pub fn clear_completion(&self, task: TaskId) {
        write_unpoisoned(&self.completed).remove(&task);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpStatsCatalogError {
    Full,
}

impl fmt::Display for HttpStatsCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HTTP stats catalog is full")
    }
}

impl Error for HttpStatsCatalogError {}

#[derive(Clone, Debug)]
pub struct HttpMultiRangeWorkerConfig {
    pub journal_root: PathBuf,
    pub storage: StorageEngineConfig,
    pub retry: HttpRetryPolicy,
    /// Process-owned download limiter shared by all workers constructed from
    /// this config. Per-task limits are installed at task admission.
    pub download_rate: RateArbiter,
    /// Process-owned cumulative discard ledger shared by every worker clone.
    /// Task admission derives finite host/task/attempt ceilings from the
    /// current piece, retry, ingress-frame, and endgame bounds.
    pub discard_budget: HttpDiscardBudget,
    /// Bounds body frames retained between Hyper and storage. The permit moves
    /// with the frame until positional disk submission has consumed it.
    pub ingress_budget: HttpIngressBudgets,
    pub ingress_frame_bytes: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    /// Process-local cap for CPU-heavy whole-file digest verification. Worker
    /// clones share the semaphore created from this value.
    pub digest_workers: NonZeroUsize,
}

impl HttpMultiRangeWorkerConfig {
    pub fn validate(self) -> Result<Self, HttpMultiRangeError> {
        if self.journal_root.as_os_str().is_empty()
            || !self.journal_root.is_absolute()
            || self.event_capacity.get() > MAX_HTTP_RANGE_EVENT_CAPACITY
            || self.ingress_frame_bytes.get() > SizeClass::MiB1.capacity()
            || self.ingress_budget.limit() < self.ingress_frame_bytes.get()
            || self.digest_workers.get() > MAX_HTTP_DIGEST_WORKERS
            || HttpRetryBudget::new(self.retry.clone()).is_err()
        {
            return Err(HttpMultiRangeError::InvalidConfig);
        }
        Ok(self)
    }
}

impl Default for HttpMultiRangeWorkerConfig {
    fn default() -> Self {
        Self {
            journal_root: PathBuf::new(),
            storage: StorageEngineConfig::default(),
            retry: HttpRetryPolicy::default(),
            download_rate: RateArbiter::new(RateDirection::Download, RateArbiterConfig::default())
                .expect("default download rate arbiter is valid"),
            discard_budget: HttpDiscardBudget::default(),
            ingress_budget: HttpIngressBudgets::new(DEFAULT_HTTP_INGRESS_BUDGET_BYTES),
            ingress_frame_bytes: NonZeroUsize::new(DEFAULT_HTTP_INGRESS_FRAME_BYTES)
                .expect("default ingress frame is nonzero"),
            event_capacity: NonZeroUsize::new(DEFAULT_HTTP_RANGE_EVENT_CAPACITY)
                .expect("default range event capacity is nonzero"),
            digest_workers: NonZeroUsize::new(DEFAULT_HTTP_DIGEST_WORKERS)
                .expect("default digest worker count is nonzero"),
        }
    }
}

#[derive(Clone)]
pub struct HttpMultiRangeWorker {
    client: HttpPolicyClient,
    config: HttpMultiRangeWorkerConfig,
    stats: SharedHttpTransferStats,
    session: Option<SessionHandle>,
    digest_slots: Arc<Semaphore>,
}

impl fmt::Debug for HttpMultiRangeWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpMultiRangeWorker")
            .field("config", &self.config)
            .field("session_attached", &self.session.is_some())
            .finish_non_exhaustive()
    }
}

impl HttpMultiRangeWorker {
    pub fn new(
        client: HttpPolicyClient,
        config: HttpMultiRangeWorkerConfig,
        stats: SharedHttpTransferStats,
    ) -> Result<Self, HttpMultiRangeError> {
        let config = config.validate()?;
        let digest_slots = Arc::new(Semaphore::new(config.digest_workers.get()));
        Ok(Self {
            client,
            config,
            stats,
            session: None,
            digest_slots,
        })
    }

    #[must_use]
    pub fn with_session_owner(mut self, session: SessionHandle) -> Self {
        self.session = Some(session);
        self
    }

    pub async fn run_task(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        self.initialize_task_rate(&task)?;
        self.run_initialized_task(task, generation, cancellation)
            .await
    }

    fn initialize_task_rate(&self, task: &HttpTaskSpec) -> Result<(), HttpMultiRangeError> {
        self.config
            .download_rate
            .set_scoped_limit(
                RateScope::Task(task.task().get()),
                RateLimit::per_second(task.options().max_download_limit),
            )
            .map_err(|_| HttpMultiRangeError::InvalidConfig)
    }

    async fn run_initialized_task(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        let retry_policy = task.options().retry.as_ref().unwrap_or(&self.config.retry);
        let discard_limits = HttpDiscardBudgetLimits::for_http_task(
            self.config.discard_budget.process_limit(),
            task.options().piece_length,
            u64::try_from(self.config.ingress_frame_bytes.get()).unwrap_or(u64::MAX),
            retry_policy.max_attempts.get(),
            retry_policy.max_attempts_per_mirror.get(),
            task.options().endgame_max_duplicates,
        )
        .clamp_to(self.config.discard_budget.configured_limits());
        let discard_task = self
            .config
            .discard_budget
            .begin_task(task.task(), discard_limits.scope())
            .map_err(|_| HttpMultiRangeError::InvalidConfig)?;
        let stats = self
            .stats
            .get_or_create(task.task())
            .map_err(|_| HttpMultiRangeError::StatsCatalogFull)?;
        stats.begin();
        let discard_snapshot = discard_task.snapshot();
        stats.set_discarded(discard_snapshot.task_consumed);
        stats.set_discard_budget(
            discard_snapshot.task_consumed,
            discard_snapshot.task_remaining,
        );
        self.stats.clear_completion(task.task());
        self.close_owned_journal(task.gid()).await?;
        let prepared_storage = match self.prepare_storage(&task, generation) {
            Ok(storage) => storage,
            Err(error) => {
                self.handoff_new_or_recovered_journal(&task, generation)
                    .await?;
                return Err(error);
            }
        };
        if task.options().checksum.is_some()
            && let Some(total_length) = prepared_storage.fully_durable_length()
        {
            stats.set_total_length(total_length);
            let mut no_sources = Vec::new();
            let OpenedHttpStorage {
                storage,
                layout_hash,
                durable_bytes,
                verification_output,
                ..
            } = match self.finish_storage(
                prepared_storage,
                &task,
                generation,
                total_length,
                &mut no_sources,
            ) {
                Ok(storage) => storage,
                Err(error) => {
                    self.handoff_new_or_recovered_journal(&task, generation)
                        .await?;
                    return Err(error);
                }
            };
            stats.set_durable(durable_bytes);
            let outcome = self
                .verify_expected_checksum(
                    task.options().checksum,
                    verification_output,
                    total_length,
                    &cancellation,
                )
                .await;
            let outcome =
                account_checksum_outcome(&task, &discard_task, &stats, total_length, outcome);
            return self
                .complete_storage_outcome(&task, storage, layout_hash, total_length, outcome)
                .await;
        }
        let recovered_range_identity = prepared_storage.range_identity().cloned();
        let mut sources = match self
            .probe_sources(
                &task,
                &cancellation,
                &stats,
                &discard_task,
                recovered_range_identity.as_ref(),
            )
            .await
        {
            Ok(sources) => sources,
            Err(error) => {
                self.handoff_journal(task.gid(), prepared_storage.into_appender())
                    .await?;
                return Err(error);
            }
        };
        let total_length = sources
            .first()
            .map(|source| source.validator.total_length())
            .ok_or(HttpMultiRangeError::NoUsableSources)?;
        if let Some(identity) = recovered_range_identity.as_ref() {
            let verification = self
                .verify_recovered_range_digests(RecoveredRangeDigestVerification {
                    task: &task,
                    cancellation: &cancellation,
                    stats: &stats,
                    discard_task: &discard_task,
                    identity,
                    evidence: prepared_storage.durable_evidence(),
                    sources: &sources,
                })
                .await;
            if let Err(error) = verification {
                self.handoff_journal(task.gid(), prepared_storage.into_appender())
                    .await?;
                return Err(error);
            }
        }
        let mirror_identity = mirror_identity_context(&task, &sources);
        stats.set_total_length(total_length);
        let OpenedHttpStorage {
            mut storage,
            layout_hash,
            durable_pieces,
            durable_bytes,
            recovered_retry_states,
            verification_output,
        } = match self.finish_storage(
            prepared_storage,
            &task,
            generation,
            total_length,
            &mut sources,
        ) {
            Ok(storage) => storage,
            Err(error) => {
                self.handoff_new_or_recovered_journal(&task, generation)
                    .await?;
                return Err(error);
            }
        };
        if recovered_range_identity.is_none()
            && let Some(shared) = mirror_identity.shared_range_digest
            && let Err(error) =
                storage.record_http_range_identity(total_length, shared.journal_digest())
        {
            drop(storage);
            self.handoff_new_or_recovered_journal(&task, generation)
                .await?;
            return Err(HttpMultiRangeError::Storage(error));
        }
        stats.set_durable(durable_bytes);
        let range_outcome = self
            .run_ranges(
                &task,
                generation,
                &cancellation,
                &stats,
                &sources,
                mirror_identity,
                &durable_pieces,
                &recovered_retry_states,
                &mut storage,
                &discard_task,
            )
            .await;
        let outcome = match range_outcome {
            Ok(()) => {
                self.verify_expected_checksum(
                    task.options().checksum,
                    verification_output,
                    total_length,
                    &cancellation,
                )
                .await
            }
            Err(error) => Err(error),
        };
        let outcome = account_checksum_outcome(&task, &discard_task, &stats, total_length, outcome);
        self.complete_storage_outcome(&task, storage, layout_hash, total_length, outcome)
            .await
    }

    async fn complete_storage_outcome(
        &self,
        task: &HttpTaskSpec,
        mut storage: StorageEngine,
        layout_hash: ariax_storage::JournalHash,
        total_length: u64,
        outcome: Result<Option<JournalDigest>, HttpMultiRangeError>,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        match outcome {
            Ok(final_digest) => {
                let completed_at_unix_ms = now_unix_ms().unwrap_or(0);
                let terminal_sequence = match storage.complete(final_digest, completed_at_unix_ms) {
                    Ok(sequence) => sequence,
                    Err(error) => {
                        let journal = storage
                            .into_flushed_journal()
                            .map_err(HttpMultiRangeError::Storage)?;
                        self.handoff_journal(task.gid(), journal).await?;
                        return Err(HttpMultiRangeError::Storage(error));
                    }
                };
                let journal = storage
                    .into_flushed_journal()
                    .map_err(HttpMultiRangeError::Storage)?;
                self.handoff_journal(task.gid(), journal).await?;
                self.stats.record_completion(
                    task.task(),
                    HttpCompletedEvidence {
                        layout_hash,
                        total_length,
                        completed_at_unix_ms,
                        terminal_sequence,
                    },
                );
                Ok(HttpWorkerSuccess { seed: false })
            }
            Err(error) => {
                let journal = storage
                    .into_flushed_journal()
                    .map_err(HttpMultiRangeError::Storage)?;
                self.handoff_journal(task.gid(), journal).await?;
                Err(error)
            }
        }
    }

    async fn probe_sources(
        &self,
        task: &HttpTaskSpec,
        cancellation: &HttpCancellation,
        stats: &HttpTransferStats,
        discard_task: &HttpDiscardTaskGuard,
        recovered_identity: Option<&RecoveredHttpRangeIdentity>,
    ) -> Result<Vec<PreparedSource>, HttpMultiRangeError> {
        // Strict identity inspects every submitted mirror so matching
        // range-digest responders can remain available for exact-span
        // endgame races. Ordinary pieces still use one source unless a
        // separately persisted whole-entity checksum authorizes the pool.
        let source_limit = task.sources().len();
        let mut prepared = Vec::new();
        let mut settled_total = None;
        let mut last_error = None;
        let mirror_identity = HttpMirrorIdentityContext {
            policy: task.options().mirror_identity,
            shared_whole_entity_digest: task.options().checksum.is_some(),
            shared_range_digest: None,
        };
        for source in &task.sources()[..source_limit] {
            match probe_source(
                &self.client,
                source.id(),
                source.uri(),
                mirror_identity,
                task.options().response_body_timeout,
                cancellation,
                stats,
                discard_task,
            )
            .await
            {
                Ok(validator) => {
                    if settled_total.is_none() {
                        settled_total = Some(validator.total_length());
                    }
                    if settled_total == Some(validator.total_length()) {
                        prepared.push(PreparedSource {
                            lease_fingerprint: validator.fingerprint(),
                            validator: Arc::new(validator),
                            ordinary_assignments: true,
                            range_digest_endgame: false,
                        });
                    } else {
                        last_error = Some(HttpMultiRangeError::SourceLengthMismatch);
                    }
                }
                Err(HttpMultiRangeError::Cancelled) => return Err(HttpMultiRangeError::Cancelled),
                Err(error) => last_error = Some(error),
            }
        }
        if prepared.is_empty() {
            return Err(last_error.unwrap_or(HttpMultiRangeError::NoUsableSources));
        }
        if let Some(identity) = recovered_identity
            && task.options().mirror_identity == HttpMirrorIdentityPolicy::RequireSharedDigest
            && task.options().checksum.is_none()
        {
            let expected = identity.representation_digest();
            prepared.retain(|source| {
                source.validator.total_length() == identity.total_length()
                    && source.validator.representation_digest()
                        == Some(HttpRepresentationDigest::sha256(
                            expected.value().try_into().expect("SHA-256 digest length"),
                        ))
            });
            if prepared.is_empty() {
                return Err(HttpMultiRangeError::Setup(
                    KnownLengthHttpError::StaleValidator,
                ));
            }
            for (index, source) in prepared.iter_mut().enumerate() {
                source.lease_fingerprint = identity.identity_fingerprint();
                source.ordinary_assignments = index == 0;
                source.range_digest_endgame = true;
            }
        } else if task.options().mirror_identity == HttpMirrorIdentityPolicy::RequireSharedDigest
            && task.options().checksum.is_none()
            && prepared.len() > 1
        {
            let shared = prepared[0].validator.representation_digest();
            let all_match = shared.is_some()
                && prepared
                    .iter()
                    .all(|source| source.validator.representation_digest() == shared);
            if !all_match {
                prepared.truncate(1);
            } else if let Some(shared) = shared {
                let fingerprint =
                    shared_range_identity_fingerprint(shared, prepared[0].validator.total_length());
                for (index, source) in prepared.iter_mut().enumerate() {
                    source.lease_fingerprint = fingerprint;
                    source.ordinary_assignments = index == 0;
                    source.range_digest_endgame = true;
                }
            }
        }
        Ok(prepared)
    }

    /// Revalidates every durable piece against the persisted representation
    /// digest before the scheduler can release any new range lease.  Local
    /// readback proves the bytes survived the previous process; this second
    /// proof binds those bytes to the currently selected digest-only mirrors.
    async fn verify_recovered_range_digests(
        &self,
        verification: RecoveredRangeDigestVerification<'_>,
    ) -> Result<(), HttpMultiRangeError> {
        let RecoveredRangeDigestVerification {
            task,
            cancellation,
            stats,
            discard_task,
            identity,
            evidence,
            sources,
        } = verification;
        if evidence.is_empty() {
            return Ok(());
        }
        let source = sources
            .iter()
            .find(|source| source.ordinary_assignments)
            .or_else(|| sources.first())
            .ok_or(HttpMultiRangeError::NoUsableSources)?;
        if source
            .validator
            .representation_digest()
            .is_none_or(|digest| {
                digest.value().as_slice() != identity.representation_digest().value()
            })
        {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::StaleValidator,
            ));
        }
        for evidence in evidence.values() {
            let digest = evidence
                .digest()
                .filter(|digest| digest.algorithm() == JournalDigestAlgorithm::Sha256)
                .ok_or(HttpMultiRangeError::Setup(
                    KnownLengthHttpError::DurablePieceDigestMismatch {
                        piece: evidence.piece_id(),
                    },
                ))?;
            let span = GlobalSpan {
                offset: evidence.piece_span().offset(),
                len: usize::try_from(evidence.piece_span().len())
                    .map_err(|_| HttpMultiRangeError::Protocol)?,
            };
            let mut request = HttpClientRequest::get(source.validator.final_uri().to_owned());
            request.range = Some(span);
            request.mirror_identity = task.options().mirror_identity;
            request.want_repr_digest = true;
            let response = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled),
                response = self.client.execute(request) => {
                    response.map_err(HttpMultiRangeError::Client)?
                }
            };
            let mut response = response;
            let response_digest = source
                .validator
                .validate_range(
                    response.final_uri(),
                    response.status(),
                    response.headers(),
                    span,
                )
                .map_err(|error| match error {
                    HttpRangeResponseError::RepresentationDigestChanged
                    | HttpRangeResponseError::RepresentationDigestMismatch => {
                        HttpMultiRangeError::ChecksumMismatch
                    }
                    _ => HttpMultiRangeError::StaleValidator,
                })?;
            let discard = discard_task
                .begin_attempt(discard_host_key(source.validator.final_uri())?)
                .map_err(discard_setup_error)?;
            let mut body_digest = Sha256::new();
            let mut received = 0_u64;
            loop {
                let data = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled),
                    data = response.next_data(task.options().response_body_timeout) => {
                        data.map_err(HttpMultiRangeError::Client)?
                    }
                };
                let Some(data) = data else {
                    break;
                };
                body_digest.update(&data);
                stats.add_raw(data.len());
                record_discarded(&discard, stats, data.len())?;
                received = received
                    .checked_add(
                        u64::try_from(data.len()).map_err(|_| HttpMultiRangeError::Protocol)?,
                    )
                    .ok_or(HttpMultiRangeError::Protocol)?;
                if received > evidence.piece_span().len() {
                    return Err(HttpMultiRangeError::OversizedBody);
                }
            }
            if received != evidence.piece_span().len() {
                return Err(HttpMultiRangeError::ShortBody);
            }
            let actual: [u8; 32] = body_digest.finalize().into();
            if actual.as_slice() != digest.value() {
                return Err(HttpMultiRangeError::ChecksumMismatch);
            }
            if response_digest.is_some_and(|value| value.value().as_slice() != actual) {
                return Err(HttpMultiRangeError::ChecksumMismatch);
            }
            response.finish().await;
        }
        Ok(())
    }

    fn prepare_storage(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
    ) -> Result<PreparedHttpStorage, HttpMultiRangeError> {
        let root = RootDirectoryCapability::open_trusted(task.output_root())
            .map_err(KnownLengthHttpError::from)?;
        let OpenedTaskJournal { appender, state } = self.open_task_journal(task, generation)?;
        let Some(state) = state else {
            return Ok(PreparedHttpStorage::Fresh(Box::new(
                FreshPreparedHttpStorage { root, appender },
            )));
        };
        if state.terminal().is_some() {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::AlreadyComplete,
            ));
        }
        let Some(recovered) = state.layout() else {
            return Ok(PreparedHttpStorage::Fresh(Box::new(
                FreshPreparedHttpStorage { root, appender },
            )));
        };
        let total_length = recovered
            .layout()
            .total_length()
            .ok_or(HttpMultiRangeError::Setup(
                KnownLengthHttpError::RecoveryState,
            ))?;
        if state.generation() != generation
            || recovered.layout().piece_length() != task.options().piece_length
            || PlatformPath::from_current(root.display()).map_err(KnownLengthHttpError::from)?
                != *recovered.layout().root_binding().path()
            || root.identity().encode().as_ref()
                != recovered.layout().root_binding().root_identity().bytes()
        {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::RecoveryState,
            ));
        }
        let mut selected = recovered
            .layout()
            .files()
            .iter()
            .filter(|entry| entry.selected());
        let entry = selected
            .next()
            .filter(|entry| entry.id() == FileId::new(0))
            .ok_or(HttpMultiRangeError::Setup(
                KnownLengthHttpError::RecoveryState,
            ))?;
        if selected.next().is_some() || entry.safe_path() != task.output() {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::RecoveryState,
            ));
        }
        let output = root
            .open_existing_file(
                entry.safe_path(),
                entry.identity().ok_or(HttpMultiRangeError::Setup(
                    KnownLengthHttpError::RecoveryState,
                ))?,
            )
            .map_err(KnownLengthHttpError::from)?;
        let actual_length = output.len().map_err(KnownLengthHttpError::from)?;
        if actual_length != total_length {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::ExistingLengthMismatch {
                    expected: total_length,
                    actual: actual_length,
                },
            ));
        }
        let layout = FileLayout::new(
            task.task(),
            generation,
            recovered.layout().root_binding().clone(),
            recovered.layout().files().to_vec(),
            Some(total_length),
            task.options().piece_length,
        )
        .map_err(KnownLengthHttpError::from)?;
        let durable_evidence = state.durable_pieces().clone();
        let (durable_pieces, durable_bytes) =
            verify_recovered_piece_digests(&output, &durable_evidence)?;
        if recovered.layout().generation() != generation {
            return Ok(PreparedHttpStorage::Readmission(Box::new(
                ReadmissionPreparedHttpStorage {
                    root,
                    appender,
                    previous_layout_hash: recovered.layout().layout_hash(),
                    output,
                    durable_evidence,
                    durable_pieces,
                    durable_bytes,
                    retry_states: state.retry_states().values().cloned().collect(),
                    range_identity: state.http_range_identity().cloned(),
                },
            )));
        }
        Ok(PreparedHttpStorage::Recovered(Box::new(
            RecoveredPreparedHttpStorage {
                appender,
                layout,
                output,
                durable_evidence,
                durable_pieces,
                durable_bytes,
                retry_states: state.retry_states().values().cloned().collect(),
                strong_validator: state.http_strong_validator().cloned(),
                range_identity: state.http_range_identity().cloned(),
            },
        )))
    }

    fn finish_storage(
        &self,
        prepared: PreparedHttpStorage,
        task: &HttpTaskSpec,
        generation: Generation,
        total_length: u64,
        sources: &mut Vec<PreparedSource>,
    ) -> Result<OpenedHttpStorage, HttpMultiRangeError> {
        let (appender, layout, output, durable_pieces, durable_bytes, retry_states) = match prepared
        {
            PreparedHttpStorage::Fresh(fresh) => {
                let FreshPreparedHttpStorage { root, mut appender } = *fresh;
                let output = root
                    .create_new_file(task.output())
                    .map_err(KnownLengthHttpError::from)?;
                output
                    .set_len(total_length)
                    .map_err(KnownLengthHttpError::from)?;
                let layout = build_single_file_layout(
                    task.task(),
                    generation,
                    &root,
                    task.output(),
                    &output,
                    total_length,
                    task.options().piece_length,
                )?;
                append_layout(&mut appender, &layout)?;
                if task.options().checksum.is_none()
                    && let [source] = sources.as_mut_slice()
                    && let (Some(etag), Some(validator_fingerprint)) = (
                        source.validator.if_range(),
                        source.validator.strong_validator_fingerprint(),
                    )
                {
                    append_http_strong_validator(
                        &mut appender,
                        generation,
                        source.validator.resource_fingerprint(),
                        validator_fingerprint,
                        total_length,
                        etag,
                    )?;
                    source.lease_fingerprint = validator_fingerprint;
                }
                (appender, layout, output, Vec::new(), 0, Vec::new())
            }
            PreparedHttpStorage::Recovered(recovered) => {
                let RecoveredPreparedHttpStorage {
                    appender,
                    layout,
                    output,
                    durable_evidence,
                    durable_pieces,
                    durable_bytes,
                    retry_states,
                    strong_validator,
                    range_identity,
                } = *recovered;
                if layout.total_length() != Some(total_length) {
                    return Err(HttpMultiRangeError::Setup(
                        KnownLengthHttpError::RecoveryState,
                    ));
                }
                if task.options().checksum.is_none() {
                    bind_recovered_strong_validator(
                        sources,
                        strong_validator.as_ref(),
                        total_length,
                    )?;
                    bind_recovered_range_identity(sources, range_identity.as_ref(), total_length)?;
                    verify_recovered_piece_validators(&durable_evidence, sources)?;
                }
                (
                    appender,
                    layout,
                    output,
                    durable_pieces,
                    durable_bytes,
                    retry_states,
                )
            }
            PreparedHttpStorage::Readmission(readmission) => {
                let ReadmissionPreparedHttpStorage {
                    root,
                    mut appender,
                    previous_layout_hash,
                    output,
                    durable_evidence,
                    mut durable_pieces,
                    mut durable_bytes,
                    retry_states,
                    range_identity,
                } = *readmission;
                output
                    .set_len(total_length)
                    .map_err(KnownLengthHttpError::from)?;
                let layout = build_single_file_layout(
                    task.task(),
                    generation,
                    &root,
                    task.output(),
                    &output,
                    total_length,
                    task.options().piece_length,
                )?;
                let retains_layout_identity = layout.layout_hash() == previous_layout_hash;
                append_layout(&mut appender, &layout)?;
                if !retains_layout_identity {
                    durable_pieces.clear();
                    durable_bytes = 0;
                }
                if task.options().checksum.is_none() {
                    if retains_layout_identity {
                        bind_recovered_range_identity(
                            sources,
                            range_identity.as_ref(),
                            total_length,
                        )?;
                        verify_recovered_piece_validators(&durable_evidence, sources)?;
                    }
                    if let [source] = sources.as_mut_slice()
                        && let (Some(etag), Some(validator_fingerprint)) = (
                            source.validator.if_range(),
                            source.validator.strong_validator_fingerprint(),
                        )
                    {
                        append_http_strong_validator(
                            &mut appender,
                            generation,
                            source.validator.resource_fingerprint(),
                            validator_fingerprint,
                            total_length,
                            etag,
                        )?;
                        source.lease_fingerprint = validator_fingerprint;
                    }
                }
                (
                    appender,
                    layout,
                    output,
                    durable_pieces,
                    durable_bytes,
                    retry_states,
                )
            }
        };
        let layout_hash = ariax_storage::JournalHash::new(*layout.layout_hash().as_bytes())
            .expect("layout SHA-256 is nonzero");
        let verification_output = task
            .options()
            .checksum
            .map(|_| output.try_clone_capability())
            .transpose()
            .map_err(KnownLengthHttpError::from)?;
        let storage = StorageEngine::open_layout(
            layout,
            [(ariax_core::FileId::new(0), output)],
            appender,
            self.config.storage.clone(),
        )
        .map_err(HttpMultiRangeError::Storage)?;
        Ok(OpenedHttpStorage {
            storage,
            layout_hash,
            durable_pieces,
            durable_bytes,
            recovered_retry_states: retry_states,
            verification_output,
        })
    }

    async fn verify_expected_checksum(
        &self,
        expected: Option<HttpContentChecksum>,
        output: Option<RootFileCapability>,
        total_length: u64,
        cancellation: &HttpCancellation,
    ) -> Result<Option<JournalDigest>, HttpMultiRangeError> {
        let Some(expected) = expected else {
            debug_assert!(output.is_none());
            return Ok(None);
        };
        let output = output.ok_or(HttpMultiRangeError::Protocol)?;
        let slots = Arc::clone(&self.digest_slots);
        let permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled),
            permit = slots.acquire_owned() => permit.map_err(|_| HttpMultiRangeError::Protocol)?,
        };
        let hash_cancellation = cancellation.clone();
        let actual = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            hash_output_sha256(&output, total_length, &hash_cancellation)
        })
        .await
        .map_err(|_| HttpMultiRangeError::Protocol)??;
        let expected_value = expected.value();
        if actual.algorithm() != expected.algorithm() || actual.value() != expected_value.as_slice()
        {
            return Err(HttpMultiRangeError::ChecksumMismatch);
        }
        Ok(Some(actual))
    }

    fn open_task_journal(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
    ) -> Result<OpenedTaskJournal, HttpMultiRangeError> {
        let directory = http_journal_directory(&self.config.journal_root, task.gid());
        if directory.exists() {
            let capability = JournalDirectoryCapability::open_trusted(&directory)
                .map_err(|error| HttpMultiRangeError::Setup(KnownLengthHttpError::from(error)))?;
            let paths = ControlJournalAppender::discover_segment_paths(
                &capability,
                ReplayLimits::default().max_segments,
            )
            .map_err(KnownLengthHttpError::from)?;
            if !paths.is_empty() {
                let (appender, framing) = ControlJournalAppender::open_recovered(
                    &directory,
                    &paths,
                    task.gid(),
                    derive_http_journal_id(task.task(), task.gid()),
                    ReplayLimits::default(),
                    generation,
                    now_unix_ms().unwrap_or(0),
                )
                .map_err(KnownLengthHttpError::from)?;
                let replay = recover_journal_state(
                    &framing.records,
                    task.task(),
                    &|_: &str| true,
                    JournalStateLimits::default(),
                );
                if replay.accepted_records != framing.records.len() {
                    return Err(HttpMultiRangeError::Setup(
                        KnownLengthHttpError::RecoveryState,
                    ));
                }
                let state = replay.state.ok_or(HttpMultiRangeError::Setup(
                    KnownLengthHttpError::RecoveryState,
                ))?;
                return Ok(OpenedTaskJournal {
                    appender,
                    state: Some(state),
                });
            }
        }
        let mut journal = ControlJournalAppender::create(
            &directory,
            task.gid(),
            derive_http_journal_id(task.task(), task.gid()),
            generation,
            now_unix_ms().unwrap_or(0),
        )
        .map_err(KnownLengthHttpError::from)?;
        let options = task
            .persistence_options()
            .map_err(|_| HttpMultiRangeError::InvalidConfig)?;
        append_initial_admission_with_options(&mut journal, generation, options)?;
        Ok(OpenedTaskJournal {
            appender: journal,
            state: None,
        })
    }

    async fn close_owned_journal(&self, gid: Gid) -> Result<(), HttpMultiRangeError> {
        let Some(session) = self.session.clone() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || session.execute(SessionCommand::CloseJournal { gid }))
            .await
            .map_err(|_| HttpMultiRangeError::Protocol)?
            .map(|_| ())
            .or_else(|error| match error {
                SessionOwnerError::Persistence(SessionPersistenceError::MissingJournal {
                    ..
                }) => Ok(()),
                _other => Err(HttpMultiRangeError::Protocol),
            })
    }

    async fn handoff_new_or_recovered_journal(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
    ) -> Result<(), HttpMultiRangeError> {
        let journal = self.open_task_journal(task, generation)?;
        self.handoff_journal(task.gid(), journal.appender).await
    }

    async fn handoff_journal(
        &self,
        gid: Gid,
        mut journal: ControlJournalAppender,
    ) -> Result<(), HttpMultiRangeError> {
        let Some(session) = self.session.clone() else {
            journal
                .close_flushed()
                .map_err(KnownLengthHttpError::from)?;
            return Ok(());
        };
        tokio::task::spawn_blocking(move || {
            session
                .execute(SessionCommand::InstallJournalAppender {
                    gid,
                    appender: journal,
                })
                .map(|_| ())
        })
        .await
        .map_err(|_| HttpMultiRangeError::Protocol)?
        .map_err(|_| HttpMultiRangeError::Protocol)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_ranges(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
        cancellation: &HttpCancellation,
        stats: &HttpTransferStats,
        sources: &[PreparedSource],
        mirror_identity: HttpMirrorIdentityContext,
        durable_pieces: &[PieceId],
        recovered_retry_states: &[RecoveredRetryState],
        storage: &mut StorageEngine,
        discard_task: &HttpDiscardTaskGuard,
    ) -> Result<(), HttpMultiRangeError> {
        let total_length = sources[0].validator.total_length();
        let retry_policy = task.options().retry.as_ref().unwrap_or(&self.config.retry);
        let recovered_retries = recover_range_retries(recovered_retry_states, retry_policy)?;
        if let Some(diagnostic) =
            recovered_retries.latest_diagnostic(retry_policy, now_unix_ms().unwrap_or(0))
        {
            stats.set_retry_diagnostic(diagnostic);
        }
        stats.set_retry_count(
            recovered_retries
                .pieces
                .values()
                .map(|retry| u64::from(retry.attempts))
                .fold(0_u64, u64::saturating_add),
        );
        let split = if total_length <= task.options().min_split_size {
            NonZeroUsize::new(1).expect("one is nonzero")
        } else {
            task.options().split
        };
        let range_sources = sources
            .iter()
            .map(|source| {
                HttpRangeSource::from_uri(source.validator.source(), source.validator.final_uri())
                    .map(|source_id| {
                        source_id
                            .with_ordinary_assignments(source.ordinary_assignments)
                            .with_same_source_endgame(source.validator.if_range().is_some())
                            .with_shared_identity_endgame(
                                mirror_identity.shared_range_digest.is_some()
                                    && source.range_digest_endgame,
                            )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut coordinator = HttpRangeCoordinator::new(
            HttpRangeCoordinatorConfig {
                total_length,
                piece_length: task.options().piece_length,
                split,
                max_connections_per_origin: task.options().max_connections_per_server,
                max_total_attempts: retry_policy.max_attempts.get(),
                max_attempts_per_source: retry_policy.max_attempts_per_mirror.get(),
                endgame_max_duplicates: task.options().endgame_max_duplicates,
            },
            range_sources,
        )?;
        coordinator.restore_durable(durable_pieces.iter().copied())?;
        let durable_pieces = durable_pieces.iter().copied().collect::<BTreeSet<_>>();
        let mut budgets = restore_range_retry_budgets(
            &mut coordinator,
            recovered_retries.pieces,
            &durable_pieces,
            retry_policy,
        )?;
        let retry_elapsed_offset_ms = recovered_retries.elapsed_ms;
        let mut validators = sources
            .iter()
            .map(|source| (source.validator.source(), source.clone()))
            .collect::<BTreeMap<_, _>>();
        let source_hosts = source_host_keys(sources)?;
        let (events, mut receiver) = mpsc::channel(self.config.event_capacity.get());
        let mut joins = JoinSet::new();
        let mut by_join = HashMap::new();
        let mut abort_handles = BTreeMap::<LeaseId, AbortHandle>::new();
        let mut active = BTreeMap::new();
        let mut pending_endgame = BTreeMap::new();
        let mut endgame_losers = BTreeMap::new();
        let mut cancelled_leases = BTreeSet::new();
        let started = Instant::now();
        let mut next_attempt = 1_u64;

        let outcome = 'download: loop {
            let now_ms = elapsed_ms(started);
            let mut retry_at = None;
            loop {
                let eligible_endgame = endgame_eligible_originals(
                    &active,
                    now_ms,
                    task.options().response_body_timeout,
                );
                match coordinator.poll_with_endgame(now_ms, &eligible_endgame)? {
                    poll @ (HttpRangePoll::Assignment(_) | HttpRangePoll::Endgame { .. }) => {
                        let (assignment, original) = match poll {
                            HttpRangePoll::Assignment(assignment) => (assignment, None),
                            HttpRangePoll::Endgame {
                                assignment,
                                original,
                            } => (assignment, Some(original)),
                            _ => unreachable!("matched assignment poll"),
                        };
                        let source = validators
                            .get(&assignment.source)
                            .cloned()
                            .ok_or(HttpMultiRangeError::Protocol)?;
                        let validator = Arc::clone(&source.validator);
                        let budget = budgets.entry(assignment.piece).or_insert(
                            HttpRetryBudget::new(retry_policy.clone())
                                .map_err(HttpMultiRangeError::Retry)?,
                        );
                        if budget.begin_attempt(assignment.source).is_err() {
                            coordinator
                                .fail(assignment.lease, HttpRangeFailure::RetryAt(now_ms))?;
                            continue;
                        }
                        stats.bind_retry_lease(
                            assignment.piece,
                            assignment.source,
                            assignment.lease,
                        );
                        if let Some(original) = original {
                            let group = assignment
                                .overlap_group
                                .ok_or(HttpMultiRangeError::Protocol)?;
                            storage.register_overlap_group(
                                task.task(),
                                generation,
                                group,
                                original,
                                assignment.lease,
                                assignment.span,
                                source.lease_fingerprint,
                            )?;
                            active
                                .get_mut(&original)
                                .ok_or(HttpMultiRangeError::Protocol)?
                                .assignment
                                .overlap_group = assignment.overlap_group;
                        }
                        let transfer_attempt = TransferAttemptId::new(next_attempt)
                            .ok_or(HttpMultiRangeError::IdentifierExhausted)?;
                        next_attempt = next_attempt
                            .checked_add(1)
                            .ok_or(HttpMultiRangeError::IdentifierExhausted)?;
                        let discard = discard_task
                            .begin_attempt(discard_host_key(validator.final_uri())?)
                            .map_err(discard_setup_error)?;
                        active.insert(
                            assignment.lease,
                            ActiveAttempt {
                                assignment,
                                transfer_attempt,
                                validator: source.lease_fingerprint,
                                response_digest: None,
                                opened: false,
                                received: 0,
                                last_progress_ms: now_ms,
                                discard: discard.clone(),
                            },
                        );
                        let client = self.client.clone();
                        let cancellation = cancellation.clone();
                        let sender = events.clone();
                        let attempt_stats = stats.clone();
                        let body_timeout = task.options().response_body_timeout;
                        let lowest_speed_limit = task.options().lowest_speed_limit;
                        let rate_path = RatePath {
                            host: *source_hosts
                                .get(&assignment.source)
                                .ok_or(HttpMultiRangeError::Protocol)?,
                            task: task.task().get(),
                            stream: assignment.lease.get(),
                        };
                        let rate = self.config.download_rate.clone();
                        let ingress_budget = self.config.ingress_budget.clone();
                        let ingress_frame_bytes = self.config.ingress_frame_bytes;
                        let abort = joins.spawn(async move {
                            range_attempt(
                                client,
                                assignment,
                                validator,
                                mirror_identity,
                                body_timeout,
                                lowest_speed_limit,
                                rate,
                                rate_path,
                                ingress_budget,
                                ingress_frame_bytes,
                                discard,
                                cancellation,
                                sender,
                                attempt_stats,
                            )
                            .await;
                            assignment.lease
                        });
                        by_join.insert(abort.id(), assignment.lease);
                        abort_handles.insert(assignment.lease, abort);
                        stats.set_active(active.len());
                    }
                    HttpRangePoll::Saturated => break,
                    HttpRangePoll::Complete => break 'download Ok(()),
                    HttpRangePoll::RetryAt(deadline) => {
                        retry_at = Some(deadline);
                        break;
                    }
                    HttpRangePoll::Exhausted => {
                        break 'download Err(HttpMultiRangeError::Exhausted);
                    }
                }
            }

            if active.is_empty()
                && let Some(deadline) = retry_at
            {
                let wait = Duration::from_millis(deadline.saturating_sub(elapsed_ms(started)));
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        break 'download Err(HttpMultiRangeError::Cancelled);
                    }
                    () = tokio::time::sleep(wait) => {}
                }
                continue;
            }

            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    break 'download Err(HttpMultiRangeError::Cancelled);
                }
                event = receiver.recv() => {
                    let Some(event) = event else {
                        break 'download Err(HttpMultiRangeError::Protocol);
                    };
                    match process_attempt_event(
                        event,
                        task.task(),
                        generation,
                        started,
                        retry_elapsed_offset_ms,
                        storage,
                        &mut coordinator,
                        &mut budgets,
                        &mut active,
                        &mut pending_endgame,
                        &mut endgame_losers,
                        &mut cancelled_leases,
                        stats,
                    ).await {
                        Ok(AttemptAction::None) => {}
                        Ok(AttemptAction::CancelLease(lease)) => {
                            let Some(handle) = abort_handles.get(&lease) else {
                                break 'download Err(HttpMultiRangeError::Protocol);
                            };
                            handle.abort();
                        }
                        Err(HttpMultiRangeError::RevalidateSource(source)) => {
                            let current = validators
                                .get(&source)
                                .cloned()
                                .ok_or(HttpMultiRangeError::Protocol)?;
                            match self
                                .revalidate_range_source(
                                    task,
                                    current,
                                    total_length,
                                    cancellation,
                                    stats,
                                    discard_task,
                                )
                                .await
                            {
                                Ok(revalidated) => {
                                    validators.insert(source, revalidated);
                                }
                                Err(error) => break 'download Err(error),
                            }
                        }
                        Err(error) => break 'download Err(error),
                    }
                }
                joined = joins.join_next_with_id(), if !joins.is_empty() => {
                    let Some(joined) = joined else {
                        continue;
                    };
                    match joined {
                        Ok((join_id, lease)) => {
                            by_join.remove(&join_id);
                            abort_handles.remove(&lease);
                            if cancelled_leases.remove(&lease) {
                                continue;
                            }
                            if let Some(group) = endgame_losers.remove(&lease)
                                && let Err(error) = settle_endgame_loser(
                                    lease,
                                    group,
                                    task.task(),
                                    generation,
                                    storage,
                                    &mut coordinator,
                                    &mut active,
                                    &mut pending_endgame,
                                    stats,
                                )
                            {
                                break 'download Err(error);
                            }
                        }
                        Err(error) => {
                            let Some(lease) = by_join.remove(&error.id()) else {
                                break 'download Err(HttpMultiRangeError::Protocol);
                            };
                            abort_handles.remove(&lease);
                            if cancelled_leases.remove(&lease) {
                                continue;
                            }
                            if let Some(group) = endgame_losers.remove(&lease) {
                                if let Err(error) = settle_endgame_loser(
                                    lease,
                                    group,
                                    task.task(),
                                    generation,
                                    storage,
                                    &mut coordinator,
                                    &mut active,
                                    &mut pending_endgame,
                                    stats,
                                ) {
                                    break 'download Err(error);
                                }
                                continue;
                            }
                            if let Err(error) = fail_panicked_attempt(
                                lease,
                                task.task(),
                                generation,
                                elapsed_ms(started),
                                storage,
                                &mut coordinator,
                                &budgets,
                                &mut active,
                                stats,
                            ) {
                                break 'download Err(error);
                            }
                        }
                    }
                }
            }
        };

        joins.abort_all();
        while joins.join_next().await.is_some() {}
        let mut cleanup_error = None;
        while let Ok(event) = receiver.try_recv() {
            match event {
                AttemptEvent::Head { start, .. } => {
                    let _ignored = start.send(Err(RangeAttemptFailure::Cancelled));
                }
                AttemptEvent::PrepareRead { response, .. } => {
                    let _ignored = response.send(None);
                }
                AttemptEvent::Chunk {
                    buffer, discard, ..
                } => {
                    let data_len = buffer.len();
                    storage.discard_network_buffer(buffer)?;
                    if let Err(error) = record_discarded(&discard, stats, data_len) {
                        cleanup_error.get_or_insert(error);
                    }
                }
                AttemptEvent::Terminal { .. } => {}
            }
        }
        let reason = match &outcome {
            Err(HttpMultiRangeError::Cancelled) => LeaseAbortReason::Cancelled,
            Err(HttpMultiRangeError::Storage(_)) => LeaseAbortReason::StorageRejected,
            _ => LeaseAbortReason::Retry,
        };
        let pending_candidates = pending_endgame
            .values()
            .map(|candidate| candidate.attempt.clone())
            .collect::<Vec<_>>();
        let active_attempts = active.values().cloned().collect::<Vec<_>>();
        for attempt in pending_candidates.into_iter().chain(active_attempts) {
            if attempt.opened {
                match storage.abort_lease(task.task(), generation, attempt.assignment.lease, reason)
                {
                    Ok(_) => {}
                    Err(error) if error.reject() == WriteReject::UnknownLease => {}
                    Err(error) => return Err(HttpMultiRangeError::Storage(error)),
                }
            }
            stats.remove_provisional(attempt.received);
            if let Err(error) = record_discarded(&attempt.discard, stats, attempt.received) {
                cleanup_error.get_or_insert(error);
            }
        }
        stats.set_active(0);
        match cleanup_error {
            Some(error) => Err(error),
            None => outcome,
        }
    }

    async fn revalidate_range_source(
        &self,
        task: &HttpTaskSpec,
        current: PreparedSource,
        total_length: u64,
        cancellation: &HttpCancellation,
        stats: &HttpTransferStats,
        discard_task: &HttpDiscardTaskGuard,
    ) -> Result<PreparedSource, HttpMultiRangeError> {
        let submitted = task
            .sources()
            .iter()
            .find(|source| source.id() == current.validator.source())
            .ok_or(HttpMultiRangeError::Protocol)?;
        let mirror_identity = HttpMirrorIdentityContext {
            policy: task.options().mirror_identity,
            shared_whole_entity_digest: task.options().checksum.is_some(),
            shared_range_digest: current
                .range_digest_endgame
                .then(|| current.validator.representation_digest())
                .flatten(),
        };
        let fresh = probe_source(
            &self.client,
            submitted.id(),
            submitted.uri(),
            mirror_identity,
            task.options().response_body_timeout,
            cancellation,
            stats,
            discard_task,
        )
        .await
        .map_err(|error| match error {
            HttpMultiRangeError::Cancelled => HttpMultiRangeError::Cancelled,
            _ => HttpMultiRangeError::StaleValidator,
        })?;
        if fresh.total_length() != total_length
            || fresh.final_uri() != current.validator.final_uri()
            || (task.options().checksum.is_none() && fresh != *current.validator)
        {
            return Err(HttpMultiRangeError::StaleValidator);
        }
        let lease_fingerprint = if task.options().checksum.is_some() {
            fresh.fingerprint()
        } else {
            current.lease_fingerprint
        };
        Ok(PreparedSource {
            validator: Arc::new(fresh),
            lease_fingerprint,
            ordinary_assignments: current.ordinary_assignments,
            range_digest_endgame: current.range_digest_endgame,
        })
    }
}

impl HttpTaskWorker for HttpMultiRangeWorker {
    fn start(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
    ) -> HttpWorkerFuture {
        let worker = self.clone();
        let retry_policy = task
            .options()
            .retry
            .clone()
            .unwrap_or_else(|| worker.config.retry.clone());
        let rate_initialization = worker
            .initialize_task_rate(&task)
            .map_err(|error| error.into_public(&retry_policy, generation));
        Box::pin(async move {
            rate_initialization?;
            worker
                .run_initialized_task(task, generation, cancellation)
                .await
                .map_err(|error| error.into_public(&retry_policy, generation))
        })
    }
}

#[derive(Clone, Debug)]
struct PreparedSource {
    validator: Arc<HttpRangeResponseValidator>,
    lease_fingerprint: ariax_storage::JournalHash,
    ordinary_assignments: bool,
    range_digest_endgame: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HttpMirrorIdentityContext {
    policy: HttpMirrorIdentityPolicy,
    shared_whole_entity_digest: bool,
    shared_range_digest: Option<HttpRepresentationDigest>,
}

fn mirror_identity_context(
    task: &HttpTaskSpec,
    sources: &[PreparedSource],
) -> HttpMirrorIdentityContext {
    HttpMirrorIdentityContext {
        policy: task.options().mirror_identity,
        shared_whole_entity_digest: task.options().checksum.is_some(),
        shared_range_digest: (sources.len() > 1
            && sources.iter().all(|source| source.range_digest_endgame))
        .then(|| {
            sources
                .first()
                .and_then(|source| source.validator.representation_digest())
        })
        .flatten(),
    }
}

fn shared_range_identity_fingerprint(
    digest: HttpRepresentationDigest,
    total_length: u64,
) -> ariax_storage::JournalHash {
    ariax_storage::calculate_http_range_identity_fingerprint(&digest.journal_digest(), total_length)
        .expect("SHA-256 shared representation identity is canonical")
}

fn bind_recovered_strong_validator(
    sources: &mut Vec<PreparedSource>,
    recovered: Option<&RecoveredHttpStrongValidator>,
    total_length: u64,
) -> Result<(), HttpMultiRangeError> {
    let Some(recovered) = recovered else {
        return Ok(());
    };
    if recovered.total_length() != total_length {
        return Err(HttpMultiRangeError::Setup(
            KnownLengthHttpError::StaleValidator,
        ));
    }
    sources.retain_mut(|source| {
        let matches = source.validator.resource_fingerprint() == recovered.resource_fingerprint()
            && source.validator.strong_validator_fingerprint()
                == Some(recovered.validator_fingerprint())
            && source.validator.if_range() == Some(recovered.etag());
        if matches {
            source.lease_fingerprint = recovered.validator_fingerprint();
        }
        matches
    });
    if sources.is_empty() {
        return Err(HttpMultiRangeError::Setup(
            KnownLengthHttpError::StaleValidator,
        ));
    }
    Ok(())
}

fn bind_recovered_range_identity(
    sources: &mut Vec<PreparedSource>,
    recovered: Option<&RecoveredHttpRangeIdentity>,
    total_length: u64,
) -> Result<(), HttpMultiRangeError> {
    let Some(recovered) = recovered else {
        return Ok(());
    };
    if recovered.total_length() != total_length
        || recovered.representation_digest().algorithm() != JournalDigestAlgorithm::Sha256
    {
        return Err(HttpMultiRangeError::Setup(
            KnownLengthHttpError::StaleValidator,
        ));
    }
    let expected = recovered.representation_digest();
    sources.retain_mut(|source| {
        let matches = source.validator.total_length() == total_length
            && source.range_digest_endgame
            && source
                .validator
                .representation_digest()
                .is_some_and(|digest| digest.value().as_slice() == expected.value());
        if matches {
            source.lease_fingerprint = recovered.identity_fingerprint();
        }
        matches
    });
    if sources.is_empty() {
        return Err(HttpMultiRangeError::Setup(
            KnownLengthHttpError::StaleValidator,
        ));
    }
    Ok(())
}

fn verify_recovered_piece_validators(
    pieces: &BTreeMap<PieceId, RecoveredDurablePiece>,
    sources: &[PreparedSource],
) -> Result<(), HttpMultiRangeError> {
    if pieces.is_empty() {
        return Ok(());
    }
    let placeholder_span = PersistedSpan::new(0, 1).expect("placeholder span is valid");
    let placeholder_lease = LeaseId::new(1).expect("placeholder lease is nonzero");
    let mut accepted_validator_sets = BTreeSet::new();
    for source in sources
        .iter()
        .filter(|source| source.validator.if_range().is_some() || source.range_digest_endgame)
    {
        let contributor = JournalContributor::new(
            placeholder_lease,
            placeholder_span,
            source.lease_fingerprint,
        );
        accepted_validator_sets.insert(
            calculate_validator_set_fingerprint(&[contributor])
                .map_err(|_| KnownLengthHttpError::RecoveryState)?,
        );
    }
    if accepted_validator_sets.is_empty() {
        return Err(HttpMultiRangeError::Setup(
            KnownLengthHttpError::MissingStrongValidator,
        ));
    }
    for (&piece, evidence) in pieces {
        if evidence.piece_id() != piece
            || !accepted_validator_sets.contains(&evidence.validator_set_fingerprint())
        {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::StaleValidator,
            ));
        }
    }
    Ok(())
}

fn verify_recovered_piece_digests(
    output: &RootFileCapability,
    pieces: &BTreeMap<PieceId, RecoveredDurablePiece>,
) -> Result<(Vec<PieceId>, u64), HttpMultiRangeError> {
    let mut durable_pieces = Vec::new();
    durable_pieces
        .try_reserve(pieces.len())
        .map_err(|_| HttpMultiRangeError::Setup(KnownLengthHttpError::RecoveryState))?;
    let mut durable_bytes = 0_u64;
    let mut buffer = vec![0_u8; HTTP_RECOVERY_READ_BUFFER_BYTES];
    for (&piece, evidence) in pieces {
        if evidence.piece_id() != piece {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::RecoveryState,
            ));
        }
        let digest = evidence
            .digest()
            .filter(|digest| digest.algorithm() == JournalDigestAlgorithm::Sha256)
            .ok_or(HttpMultiRangeError::Setup(
                KnownLengthHttpError::DurablePieceDigestMismatch { piece },
            ))?;
        let span = evidence.piece_span();
        let mut piece_digest = Sha256::new();
        let mut offset = span.offset();
        let mut remaining = span.len();
        while remaining != 0 {
            let take = usize::try_from(remaining.min(HTTP_RECOVERY_READ_BUFFER_BYTES as u64))
                .expect("bounded recovery read fits usize");
            output
                .read_exact_at(offset, &mut buffer[..take])
                .map_err(KnownLengthHttpError::from)?;
            piece_digest.update(&buffer[..take]);
            let taken = u64::try_from(take).expect("recovery read fits u64");
            offset = offset.checked_add(taken).ok_or(HttpMultiRangeError::Setup(
                KnownLengthHttpError::RecoveryState,
            ))?;
            remaining -= taken;
        }
        let actual: [u8; 32] = piece_digest.finalize().into();
        if actual.as_slice() != digest.value() {
            return Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::DurablePieceDigestMismatch { piece },
            ));
        }
        durable_bytes = durable_bytes
            .checked_add(span.len())
            .ok_or(HttpMultiRangeError::Setup(
                KnownLengthHttpError::RecoveryState,
            ))?;
        durable_pieces.push(piece);
    }
    Ok((durable_pieces, durable_bytes))
}

fn hash_output_sha256(
    output: &RootFileCapability,
    total_length: u64,
    cancellation: &HttpCancellation,
) -> Result<JournalDigest, HttpMultiRangeError> {
    let actual_length = output.len().map_err(KnownLengthHttpError::from)?;
    if actual_length != total_length {
        return Err(HttpMultiRangeError::Setup(
            KnownLengthHttpError::ExistingLengthMismatch {
                expected: total_length,
                actual: actual_length,
            },
        ));
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; HTTP_FINAL_DIGEST_READ_BUFFER_BYTES];
    let mut offset = 0_u64;
    while offset != total_length {
        if cancellation.is_cancelled() {
            return Err(HttpMultiRangeError::Cancelled);
        }
        let take = usize::try_from(
            (total_length - offset).min(HTTP_FINAL_DIGEST_READ_BUFFER_BYTES as u64),
        )
        .expect("bounded digest read fits usize");
        output
            .read_exact_at(offset, &mut buffer[..take])
            .map_err(KnownLengthHttpError::from)?;
        digest.update(&buffer[..take]);
        offset = offset
            .checked_add(u64::try_from(take).expect("digest read length fits u64"))
            .ok_or(HttpMultiRangeError::Protocol)?;
    }
    if cancellation.is_cancelled() {
        return Err(HttpMultiRangeError::Cancelled);
    }
    let final_length = output.len().map_err(KnownLengthHttpError::from)?;
    if final_length != total_length {
        return Err(HttpMultiRangeError::Setup(
            KnownLengthHttpError::ExistingLengthMismatch {
                expected: total_length,
                actual: final_length,
            },
        ));
    }
    JournalDigest::new(JournalDigestAlgorithm::Sha256, digest.finalize().to_vec())
        .map_err(KnownLengthHttpError::from)
        .map_err(HttpMultiRangeError::from)
}

/// Assigns compact process-local host keys without hashing. Sources sharing an
/// HTTP origin share the host bucket, while distinct origins cannot collide.
fn source_host_keys(
    sources: &[PreparedSource],
) -> Result<BTreeMap<UriId, u64>, HttpMultiRangeError> {
    let mut origin_ids = BTreeMap::<String, u64>::new();
    let mut source_ids = BTreeMap::new();
    for source in sources {
        let origin = discard_host_key(source.validator.final_uri())?;
        let next = u64::try_from(origin_ids.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(HttpMultiRangeError::IdentifierExhausted)?;
        let host = *origin_ids.entry(origin).or_insert(next);
        source_ids.insert(source.validator.source(), host);
    }
    Ok(source_ids)
}

fn discard_host_key(uri: &str) -> Result<String, HttpMultiRangeError> {
    let uri: hyper::Uri = uri.parse().map_err(|_| HttpMultiRangeError::Protocol)?;
    let scheme = uri.scheme_str().ok_or(HttpMultiRangeError::Protocol)?;
    let authority = uri.authority().ok_or(HttpMultiRangeError::Protocol)?;
    Ok(format!("{scheme}://{authority}"))
}

struct RecoveredRangeDigestVerification<'a> {
    task: &'a HttpTaskSpec,
    cancellation: &'a HttpCancellation,
    stats: &'a HttpTransferStats,
    discard_task: &'a HttpDiscardTaskGuard,
    identity: &'a RecoveredHttpRangeIdentity,
    evidence: &'a BTreeMap<PieceId, RecoveredDurablePiece>,
    sources: &'a [PreparedSource],
}

struct OpenedTaskJournal {
    appender: ControlJournalAppender,
    state: Option<RecoveredJournalState>,
}

enum PreparedHttpStorage {
    Fresh(Box<FreshPreparedHttpStorage>),
    Recovered(Box<RecoveredPreparedHttpStorage>),
    Readmission(Box<ReadmissionPreparedHttpStorage>),
}

struct FreshPreparedHttpStorage {
    root: RootDirectoryCapability,
    appender: ControlJournalAppender,
}

struct RecoveredPreparedHttpStorage {
    appender: ControlJournalAppender,
    layout: FileLayout,
    output: RootFileCapability,
    durable_evidence: BTreeMap<PieceId, RecoveredDurablePiece>,
    durable_pieces: Vec<PieceId>,
    durable_bytes: u64,
    retry_states: Vec<RecoveredRetryState>,
    strong_validator: Option<RecoveredHttpStrongValidator>,
    range_identity: Option<RecoveredHttpRangeIdentity>,
}

struct ReadmissionPreparedHttpStorage {
    root: RootDirectoryCapability,
    appender: ControlJournalAppender,
    previous_layout_hash: ariax_storage::LayoutHash,
    output: RootFileCapability,
    durable_evidence: BTreeMap<PieceId, RecoveredDurablePiece>,
    durable_pieces: Vec<PieceId>,
    durable_bytes: u64,
    retry_states: Vec<RecoveredRetryState>,
    range_identity: Option<RecoveredHttpRangeIdentity>,
}

impl PreparedHttpStorage {
    fn fully_durable_length(&self) -> Option<u64> {
        let Self::Recovered(recovered) = self else {
            return None;
        };
        let total_length = recovered.layout.total_length()?;
        (recovered.durable_bytes == total_length).then_some(total_length)
    }

    fn range_identity(&self) -> Option<&RecoveredHttpRangeIdentity> {
        match self {
            Self::Fresh(_) => None,
            Self::Recovered(value) => value.range_identity.as_ref(),
            Self::Readmission(value) => value.range_identity.as_ref(),
        }
    }

    fn durable_evidence(&self) -> &BTreeMap<PieceId, RecoveredDurablePiece> {
        match self {
            Self::Fresh(_) => {
                static EMPTY: std::sync::OnceLock<BTreeMap<PieceId, RecoveredDurablePiece>> =
                    std::sync::OnceLock::new();
                EMPTY.get_or_init(BTreeMap::new)
            }
            Self::Recovered(value) => &value.durable_evidence,
            Self::Readmission(value) => &value.durable_evidence,
        }
    }

    fn into_appender(self) -> ControlJournalAppender {
        match self {
            Self::Fresh(fresh) => fresh.appender,
            Self::Recovered(recovered) => recovered.appender,
            Self::Readmission(readmission) => readmission.appender,
        }
    }
}

struct OpenedHttpStorage {
    storage: StorageEngine,
    layout_hash: ariax_storage::JournalHash,
    durable_pieces: Vec<PieceId>,
    durable_bytes: u64,
    recovered_retry_states: Vec<RecoveredRetryState>,
    verification_output: Option<RootFileCapability>,
}

#[derive(Clone, Debug, Default)]
struct RecoveredRangeRetries {
    pieces: BTreeMap<PieceId, RecoveredPieceRetry>,
    elapsed_ms: u64,
}

#[derive(Clone, Debug, Default)]
struct RecoveredPieceRetry {
    attempts: u32,
    attempts_by_mirror: BTreeMap<UriId, u32>,
    retry_at_by_mirror: BTreeMap<UriId, u64>,
    diagnostics_by_mirror: BTreeMap<UriId, RecoveredRetryDiagnostic>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecoveredRetryDiagnostic {
    scheduled_at_unix_ms: u64,
    delay_ms: u64,
    remaining_ms: u64,
    error_class: ErrorKind,
    retry_reason: RetryReason,
}

impl RecoveredRangeRetries {
    fn latest_diagnostic(
        &self,
        policy: &HttpRetryPolicy,
        now_unix_ms: u64,
    ) -> Option<HttpRetryDiagnosticSnapshot> {
        let (piece, source, retry, diagnostic) = self
            .pieces
            .iter()
            .flat_map(|(piece, retry)| {
                retry
                    .diagnostics_by_mirror
                    .iter()
                    .map(move |(source, diagnostic)| (*piece, *source, retry, *diagnostic))
            })
            .max_by_key(|(piece, source, _, diagnostic)| {
                (diagnostic.scheduled_at_unix_ms, piece.get(), source.get())
            })?;
        let source_attempt = retry.attempts_by_mirror.get(&source).copied().unwrap_or(0);
        Some(HttpRetryDiagnosticSnapshot {
            cause: HttpRetryDiagnosticCause::Recovered(diagnostic.error_class),
            source,
            piece,
            prior_lease: None,
            next_lease: None,
            total_attempt: retry.attempts,
            total_remaining: policy.max_attempts.get().saturating_sub(retry.attempts),
            source_attempt,
            source_remaining: policy
                .max_attempts_per_mirror
                .get()
                .saturating_sub(source_attempt),
            scheduled_at_unix_ms: diagnostic.scheduled_at_unix_ms,
            delay_ms: diagnostic.delay_ms,
            retry_at_unix_ms: now_unix_ms.saturating_add(diagnostic.remaining_ms),
            delay: Some(HttpRetryDelayDiagnostic::Recovered(diagnostic.retry_reason)),
            stop_reason: None,
            next_action: HttpRetryNextAction::RetryRange,
            lease_disposition: HttpRetryLeaseDisposition::UnknownRecovered,
        })
    }
}

fn piece_retry_scope_id(piece: PieceId) -> Option<PersistedId> {
    PersistedId::new(piece.get().checked_add(1)?)
}

fn span_retry_scope_id(piece: PieceId, source: UriId) -> Option<PersistedId> {
    let piece = piece.get().checked_add(1)?;
    let source = u64::from(source.get()).checked_add(1)?;
    if piece > u64::from(u32::MAX) || source > u64::from(u32::MAX) {
        return None;
    }
    PersistedId::new((piece << 32) | source)
}

fn decode_piece_retry_scope_id(scope_id: PersistedId) -> Option<PieceId> {
    let value = scope_id.get().checked_sub(1)?;
    Some(PieceId::new(value))
}

fn decode_span_retry_scope_id(scope_id: PersistedId) -> Option<(PieceId, UriId)> {
    let value = scope_id.get();
    let piece = (value >> 32).checked_sub(1)?;
    let source = (value & u64::from(u32::MAX)).checked_sub(1)?;
    Some((PieceId::new(piece), UriId::new(u32::try_from(source).ok()?)))
}

fn recover_range_retries(
    states: &[RecoveredRetryState],
    policy: &HttpRetryPolicy,
) -> Result<RecoveredRangeRetries, HttpMultiRangeError> {
    recover_range_retries_at(
        states,
        policy,
        now_unix_ms().unwrap_or(0),
        MonotonicInstant::now(),
    )
}

fn recover_range_retries_at(
    states: &[RecoveredRetryState],
    policy: &HttpRetryPolicy,
    now_wall: u64,
    now_monotonic: MonotonicInstant,
) -> Result<RecoveredRangeRetries, HttpMultiRangeError> {
    let max_wait_ms = u64::try_from(policy.max_wait.as_millis())
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(HttpMultiRangeError::Retry(
            HttpRetryError::InvalidRecoveredState,
        ))?;
    let max_elapsed_ms = u64::try_from(policy.max_elapsed.as_millis()).unwrap_or(u64::MAX);
    let mut recovered = RecoveredRangeRetries::default();
    for state in states {
        if matches!(state.scope, RetryScope::Task | RetryScope::Uri) {
            continue;
        }
        let decision = PersistedDelayDecision::new(state.scheduled_at_unix_ms, state.delay_ms)
            .and_then(|decision| decision.recover(now_wall, now_monotonic, max_wait_ms))
            .map_err(|_| HttpMultiRangeError::Retry(HttpRetryError::InvalidRecoveredState))?;
        recovered.elapsed_ms = recovered
            .elapsed_ms
            .max(decision.retry_budget_elapsed_ms(state.elapsed_before_wait_ms, max_elapsed_ms));
        match state.scope {
            RetryScope::Piece => {
                let piece = decode_piece_retry_scope_id(state.scope_id).ok_or(
                    HttpMultiRangeError::Retry(HttpRetryError::InvalidRecoveredState),
                )?;
                let entry = recovered.pieces.entry(piece).or_default();
                if entry.attempts != 0 {
                    return Err(HttpMultiRangeError::Retry(
                        HttpRetryError::InvalidRecoveredState,
                    ));
                }
                entry.attempts = state.attempt;
            }
            RetryScope::Span => {
                let (piece, source) = decode_span_retry_scope_id(state.scope_id).ok_or(
                    HttpMultiRangeError::Retry(HttpRetryError::InvalidRecoveredState),
                )?;
                let entry = recovered.pieces.entry(piece).or_default();
                if entry
                    .attempts_by_mirror
                    .insert(source, state.attempt)
                    .is_some()
                    || entry
                        .retry_at_by_mirror
                        .insert(source, decision.remaining_ms())
                        .is_some()
                    || entry
                        .diagnostics_by_mirror
                        .insert(
                            source,
                            RecoveredRetryDiagnostic {
                                scheduled_at_unix_ms: state.scheduled_at_unix_ms,
                                delay_ms: state.delay_ms,
                                remaining_ms: decision.remaining_ms(),
                                error_class: state.error_class,
                                retry_reason: state.retry_reason,
                            },
                        )
                        .is_some()
                {
                    return Err(HttpMultiRangeError::Retry(
                        HttpRetryError::InvalidRecoveredState,
                    ));
                }
            }
            RetryScope::Task | RetryScope::Uri => unreachable!("generic retries were filtered"),
        }
    }
    for retry in recovered.pieces.values() {
        if retry.attempts == 0
            || retry.attempts > policy.max_attempts.get()
            || retry
                .attempts_by_mirror
                .values()
                .any(|attempts| *attempts == 0 || *attempts > policy.max_attempts_per_mirror.get())
            || retry
                .attempts_by_mirror
                .values()
                .copied()
                .try_fold(0_u32, u32::checked_add)
                != Some(retry.attempts)
        {
            return Err(HttpMultiRangeError::Retry(
                HttpRetryError::InvalidRecoveredState,
            ));
        }
    }
    Ok(recovered)
}

fn restore_range_retry_budgets(
    coordinator: &mut HttpRangeCoordinator,
    retries: BTreeMap<PieceId, RecoveredPieceRetry>,
    durable_pieces: &BTreeSet<PieceId>,
    policy: &HttpRetryPolicy,
) -> Result<BTreeMap<PieceId, HttpRetryBudget>, HttpMultiRangeError> {
    let mut budgets = BTreeMap::new();
    for (piece, retry) in retries {
        if durable_pieces.contains(&piece) {
            continue;
        }
        coordinator.restore_piece_attempts(piece, retry.attempts)?;
        for (&source, &attempts) in &retry.attempts_by_mirror {
            let retry_at_ms = retry.retry_at_by_mirror.get(&source).copied().ok_or(
                HttpMultiRangeError::Retry(HttpRetryError::InvalidRecoveredState),
            )?;
            coordinator.restore_source_piece_retry(piece, source, attempts, retry_at_ms)?;
        }
        let mut budget =
            HttpRetryBudget::new(policy.clone()).map_err(HttpMultiRangeError::Retry)?;
        budget
            .restore_attempts(retry.attempts, retry.attempts_by_mirror)
            .map_err(HttpMultiRangeError::Retry)?;
        budgets.insert(piece, budget);
    }
    Ok(budgets)
}

#[derive(Clone, Debug)]
struct ActiveAttempt {
    assignment: HttpRangeAssignment,
    transfer_attempt: TransferAttemptId,
    validator: ariax_storage::JournalHash,
    response_digest: Option<HttpRepresentationDigest>,
    opened: bool,
    received: usize,
    last_progress_ms: u64,
    discard: HttpDiscardAttemptGuard,
}

#[derive(Debug)]
struct PendingEndgameCandidate {
    attempt: ActiveAttempt,
    competitor: LeaseId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptAction {
    None,
    CancelLease(LeaseId),
}

enum AttemptEvent {
    Head {
        lease: LeaseId,
        response_digest: Option<HttpRepresentationDigest>,
        start: oneshot::Sender<Result<(), RangeAttemptFailure>>,
    },
    PrepareRead {
        lease: LeaseId,
        minimum_capacity: usize,
        response: oneshot::Sender<Option<BufferLease>>,
    },
    Chunk {
        lease: LeaseId,
        offset: u64,
        buffer: BufferLease,
        _ingress: HttpIngressPermit,
        discard: HttpDiscardAttemptGuard,
    },
    Terminal {
        lease: LeaseId,
        result: Result<Option<JournalDigest>, RangeAttemptFailure>,
    },
}

#[derive(Debug)]
enum RangeAttemptFailure {
    Client(HttpPolicyClientError),
    Response {
        error: HttpRangeResponseError,
        retry_after: Option<String>,
    },
    ShortBody,
    OversizedBody,
    Timeout,
    Hang,
    LowestSpeed,
    DiscardBudgetExhausted(HttpDiscardScope),
    Cancelled,
}

#[derive(Debug)]
pub enum HttpMultiRangeError {
    InvalidConfig,
    StatsCatalogFull,
    DiscardBudgetExhausted(HttpDiscardScope),
    NoUsableSources,
    SourceLengthMismatch,
    Client(HttpPolicyClientError),
    Response(HttpRangeResponseError),
    ShortBody,
    OversizedBody,
    Cancelled,
    Exhausted,
    IdentifierExhausted,
    Protocol,
    ChecksumMismatch,
    StaleValidator,
    RepresentationRestart,
    RevalidateSource(UriId),
    Setup(KnownLengthHttpError),
    Storage(StorageEngineError),
    Coordinator(HttpRangeCoordinatorError),
    Retry(HttpRetryError),
}

impl HttpMultiRangeError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_multi_range_config",
            Self::StatsCatalogFull => "http_stats_catalog_full",
            Self::DiscardBudgetExhausted(_) => "http_discard_budget_exhausted",
            Self::NoUsableSources => "no_usable_http_sources",
            Self::SourceLengthMismatch => "mirror_length_mismatch",
            Self::Client(error) => error.code(),
            Self::Response(error) => error.code(),
            Self::ShortBody => "short_range_body",
            Self::OversizedBody => "oversized_range_body",
            Self::Cancelled => "cancelled",
            Self::Exhausted => "http_range_attempts_exhausted",
            Self::IdentifierExhausted => "http_identifier_exhausted",
            Self::Protocol => "http_range_protocol_invariant",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::StaleValidator => "stale_validator",
            Self::RepresentationRestart => "representation_restart_required",
            Self::RevalidateSource(_) => "http_revalidation_not_completed",
            Self::Setup(error) => error.code(),
            Self::Storage(error) => error.reject().code(),
            Self::Coordinator(error) => error.code(),
            Self::Retry(error) => error.code(),
        }
    }

    fn into_public(self, policy: &HttpRetryPolicy, generation: Generation) -> PublicError {
        let (kind, retry) = match &self {
            Self::Cancelled => (ErrorKind::Cancelled, RetryClass::Never),
            Self::Client(error) if error.retriable() => {
                (ErrorKind::Network, RetryClass::AnotherSource)
            }
            Self::Client(_) => (ErrorKind::Network, RetryClass::Never),
            Self::Response(
                HttpRangeResponseError::ValidatorChanged | HttpRangeResponseError::ResourceChanged,
            ) => (
                ErrorKind::StaleValidator,
                stale_validator_retry_class(policy, generation),
            ),
            Self::Response(
                HttpRangeResponseError::RepresentationDigestChanged
                | HttpRangeResponseError::RepresentationDigestMismatch,
            ) => (ErrorKind::ChecksumMismatch, RetryClass::AnotherSource),
            Self::Response(_) | Self::ShortBody | Self::OversizedBody => {
                (ErrorKind::InvalidRange, RetryClass::AnotherSource)
            }
            Self::Setup(
                KnownLengthHttpError::MissingStrongValidator
                | KnownLengthHttpError::ResumeResourceMismatch
                | KnownLengthHttpError::StaleValidator,
            ) => (
                ErrorKind::StaleValidator,
                stale_validator_retry_class(policy, generation),
            ),
            Self::Setup(KnownLengthHttpError::DurablePieceDigestMismatch { .. }) => {
                (ErrorKind::ChecksumMismatch, RetryClass::RestartGeneration)
            }
            Self::ChecksumMismatch => (ErrorKind::ChecksumMismatch, RetryClass::RestartGeneration),
            Self::StaleValidator | Self::RevalidateSource(_) => {
                (ErrorKind::StaleValidator, RetryClass::Never)
            }
            Self::RepresentationRestart => (
                ErrorKind::StaleValidator,
                bounded_representation_restart(policy, generation),
            ),
            Self::Storage(_) | Self::Setup(_) => (ErrorKind::Disk, RetryClass::Never),
            Self::StatsCatalogFull | Self::DiscardBudgetExhausted(_) => {
                (ErrorKind::ResourceLimit, RetryClass::Never)
            }
            Self::NoUsableSources | Self::SourceLengthMismatch | Self::Exhausted => {
                (ErrorKind::Network, RetryClass::Never)
            }
            _ => (ErrorKind::InternalInvariant, RetryClass::Never),
        };
        PublicError::new(kind, self.code(), retry)
    }
}

fn stale_validator_retry_class(policy: &HttpRetryPolicy, generation: Generation) -> RetryClass {
    if policy.stale_validator_policy == HttpStaleValidatorPolicy::RestartIfSafe {
        bounded_representation_restart(policy, generation)
    } else {
        RetryClass::Never
    }
}

fn bounded_representation_restart(policy: &HttpRetryPolicy, generation: Generation) -> RetryClass {
    let attempts_used = generation.get().saturating_add(1);
    if attempts_used < u64::from(policy.max_attempts.get()) {
        RetryClass::RestartGeneration
    } else {
        RetryClass::Never
    }
}

impl fmt::Display for HttpMultiRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => error.fmt(formatter),
            Self::Response(error) => error.fmt(formatter),
            Self::Setup(error) => error.fmt(formatter),
            Self::Storage(error) => error.fmt(formatter),
            Self::Coordinator(error) => error.fmt(formatter),
            Self::Retry(error) => error.fmt(formatter),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for HttpMultiRangeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Client(error) => Some(error),
            Self::Response(error) => Some(error),
            Self::Setup(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Coordinator(error) => Some(error),
            Self::Retry(error) => Some(error),
            _ => None,
        }
    }
}

impl From<KnownLengthHttpError> for HttpMultiRangeError {
    fn from(error: KnownLengthHttpError) -> Self {
        Self::Setup(error)
    }
}

impl From<StorageEngineError> for HttpMultiRangeError {
    fn from(error: StorageEngineError) -> Self {
        Self::Storage(error)
    }
}

impl From<HttpRangeCoordinatorError> for HttpMultiRangeError {
    fn from(error: HttpRangeCoordinatorError) -> Self {
        Self::Coordinator(error)
    }
}

#[allow(clippy::too_many_arguments)]
async fn probe_source(
    client: &HttpPolicyClient,
    source: UriId,
    uri: &str,
    mirror_identity: HttpMirrorIdentityContext,
    body_timeout: Duration,
    cancellation: &HttpCancellation,
    stats: &HttpTransferStats,
    discard_task: &HttpDiscardTaskGuard,
) -> Result<HttpRangeResponseValidator, HttpMultiRangeError> {
    let mut request = HttpClientRequest::get(uri.to_owned());
    request.range = Some(GlobalSpan { offset: 0, len: 1 });
    request.mirror_identity = mirror_identity.policy;
    request.shared_whole_entity_digest = mirror_identity.shared_whole_entity_digest;
    request.want_repr_digest = mirror_identity.policy
        == HttpMirrorIdentityPolicy::RequireSharedDigest
        && !mirror_identity.shared_whole_entity_digest;
    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled),
        response = client.execute(request) => response.map_err(HttpMultiRangeError::Client)?,
    };
    let mut response = response;
    let validator = HttpRangeResponseValidator::from_probe(
        source,
        response.final_uri(),
        response.status(),
        response.headers(),
    )
    .map_err(HttpMultiRangeError::Response)?;
    let discard = discard_task
        .begin_attempt(discard_host_key(validator.final_uri())?)
        .map_err(discard_setup_error)?;
    let mut received = 0_usize;
    let mut probe_digest = validator.representation_digest().map(|_| Sha256::new());
    loop {
        if let Some(scope) = discard.exhausted_scope() {
            return Err(HttpMultiRangeError::DiscardBudgetExhausted(scope));
        }
        let data = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled),
            data = response.next_data(body_timeout) => data.map_err(HttpMultiRangeError::Client)?,
        };
        let Some(data) = data else {
            break;
        };
        if let Some(digest) = &mut probe_digest {
            digest.update(&data);
        }
        stats.add_raw(data.len());
        record_discarded(&discard, stats, data.len())?;
        received = received.saturating_add(data.len());
        if received > 1 {
            return Err(HttpMultiRangeError::OversizedBody);
        }
    }
    if received != 1 {
        return Err(HttpMultiRangeError::ShortBody);
    }
    if let (Some(expected), Some(actual)) = (validator.representation_digest(), probe_digest) {
        let actual: [u8; 32] = actual.finalize().into();
        if actual != expected.value() {
            return Err(HttpMultiRangeError::Response(
                HttpRangeResponseError::RepresentationDigestMismatch,
            ));
        }
    }
    response.finish().await;
    Ok(validator)
}

fn discard_setup_error(error: HttpDiscardBudgetError) -> HttpMultiRangeError {
    match error {
        HttpDiscardBudgetError::AttemptIdExhausted => HttpMultiRangeError::IdentifierExhausted,
        HttpDiscardBudgetError::ZeroLimit => HttpMultiRangeError::InvalidConfig,
    }
}

fn record_discarded(
    discard: &HttpDiscardAttemptGuard,
    stats: &HttpTransferStats,
    bytes: usize,
) -> Result<(), HttpMultiRangeError> {
    record_discarded_u64(discard, stats, u64::try_from(bytes).unwrap_or(u64::MAX))
}

fn record_discarded_u64(
    discard: &HttpDiscardAttemptGuard,
    stats: &HttpTransferStats,
    bytes: u64,
) -> Result<(), HttpMultiRangeError> {
    stats.add_discarded_u64(bytes);
    let charge = discard.charge_u64(bytes);
    let snapshot = discard.snapshot();
    stats.set_discard_budget(snapshot.task_consumed, snapshot.task_remaining);
    match charge.exhausted {
        Some(scope) => Err(HttpMultiRangeError::DiscardBudgetExhausted(scope)),
        None => Ok(()),
    }
}

fn record_attempt_discarded(
    discard: &HttpDiscardAttemptGuard,
    stats: &HttpTransferStats,
    bytes: usize,
) -> Result<(), RangeAttemptFailure> {
    record_discarded(discard, stats, bytes).map_err(|error| match error {
        HttpMultiRangeError::DiscardBudgetExhausted(scope) => {
            RangeAttemptFailure::DiscardBudgetExhausted(scope)
        }
        _ => RangeAttemptFailure::Cancelled,
    })
}

fn account_checksum_outcome(
    task: &HttpTaskSpec,
    discard_task: &HttpDiscardTaskGuard,
    stats: &HttpTransferStats,
    total_length: u64,
    outcome: Result<Option<JournalDigest>, HttpMultiRangeError>,
) -> Result<Option<JournalDigest>, HttpMultiRangeError> {
    if matches!(&outcome, Err(HttpMultiRangeError::ChecksumMismatch)) {
        let host = task
            .sources()
            .first()
            .map(|source| discard_host_key(source.uri()))
            .transpose()?
            .unwrap_or_else(|| format!("http-checksum-task-{}", task.task().get()));
        let discard = discard_task
            .begin_attempt_with_limit(host, total_length.max(1))
            .map_err(discard_setup_error)?;
        record_discarded_u64(&discard, stats, total_length)?;
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn range_attempt(
    client: HttpPolicyClient,
    assignment: HttpRangeAssignment,
    validator: Arc<HttpRangeResponseValidator>,
    mirror_identity: HttpMirrorIdentityContext,
    body_timeout: Duration,
    lowest_speed_limit: u64,
    rate: RateArbiter,
    rate_path: RatePath,
    ingress_budget: HttpIngressBudgets,
    ingress_frame_bytes: NonZeroUsize,
    discard: HttpDiscardAttemptGuard,
    cancellation: HttpCancellation,
    events: mpsc::Sender<AttemptEvent>,
    stats: HttpTransferStats,
) {
    let result = range_attempt_inner(
        &client,
        assignment,
        &validator,
        mirror_identity,
        body_timeout,
        lowest_speed_limit,
        &rate,
        rate_path,
        &ingress_budget,
        ingress_frame_bytes,
        &discard,
        &cancellation,
        &events,
        &stats,
    )
    .await;
    let _sent = events
        .send(AttemptEvent::Terminal {
            lease: assignment.lease,
            result,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn range_attempt_inner(
    client: &HttpPolicyClient,
    assignment: HttpRangeAssignment,
    validator: &HttpRangeResponseValidator,
    mirror_identity: HttpMirrorIdentityContext,
    body_timeout: Duration,
    lowest_speed_limit: u64,
    rate: &RateArbiter,
    rate_path: RatePath,
    ingress_budget: &HttpIngressBudgets,
    ingress_frame_bytes: NonZeroUsize,
    discard: &HttpDiscardAttemptGuard,
    cancellation: &HttpCancellation,
    events: &mpsc::Sender<AttemptEvent>,
    stats: &HttpTransferStats,
) -> Result<Option<JournalDigest>, RangeAttemptFailure> {
    let mut request = HttpClientRequest::get(validator.final_uri().to_owned());
    request.range = Some(assignment.span);
    request.if_range = validator
        .if_range()
        .map(|value| value.to_vec().into_boxed_slice());
    request.mirror_identity = mirror_identity.policy;
    request.shared_whole_entity_digest = mirror_identity.shared_whole_entity_digest;
    request.want_repr_digest =
        mirror_identity.policy == HttpMirrorIdentityPolicy::RequireSharedDigest;
    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(RangeAttemptFailure::Cancelled),
        response = client.execute(request) => response.map_err(RangeAttemptFailure::Client)?,
    };
    let mut response = response;
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let response_digest = validator
        .validate_range(
            response.final_uri(),
            response.status(),
            response.headers(),
            assignment.span,
        )
        .map_err(|error| RangeAttemptFailure::Response { error, retry_after })?;
    let (start, proceed) = oneshot::channel();
    events
        .send(AttemptEvent::Head {
            lease: assignment.lease,
            response_digest,
            start,
        })
        .await
        .map_err(|_| RangeAttemptFailure::Cancelled)?;
    proceed
        .await
        .map_err(|_| RangeAttemptFailure::Cancelled)??;
    let expected = assignment.span.len;
    let mut received = 0_usize;
    let mut body_digest = response_digest.map(|_| Sha256::new());
    let mut speed_window_bytes = 0_usize;
    let mut speed_window_elapsed = Duration::ZERO;
    loop {
        let remaining = expected
            .checked_sub(received)
            .ok_or(RangeAttemptFailure::OversizedBody)?;
        if remaining == 0 {
            break;
        }
        let (mut buffer, ingress, permit) = acquire_read_slot(
            events,
            assignment.lease,
            remaining.min(ingress_frame_bytes.get()),
            rate,
            rate_path,
            ingress_budget,
            discard,
            cancellation,
            stats,
        )
        .await?;
        let read_started = Instant::now();
        let data = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(RangeAttemptFailure::Cancelled),
            data = response.next_data(body_timeout) => data,
        };
        let data = match data {
            Ok(data) => data,
            Err(error) => {
                if matches!(error, HttpPolicyClientError::BodyTimeout) {
                    stats.set_diagnostic(StatsDiagnostic {
                        condition: ConnectionCondition::Stalled,
                        reason: Some(if received == 0 {
                            ConnectionConditionReason::FirstByteTimeout
                        } else {
                            ConnectionConditionReason::BetweenBytesTimeout
                        }),
                    });
                }
                return Err(if received == 0 {
                    RangeAttemptFailure::Timeout
                } else {
                    RangeAttemptFailure::Hang
                });
            }
        };
        speed_window_elapsed = speed_window_elapsed.saturating_add(read_started.elapsed());
        let Some(data) = data else {
            break;
        };
        #[cfg(test)]
        let data =
            inject_oversized_range_body_fault(validator.final_uri(), data, buffer.capacity());
        stats.add_raw(data.len());
        let charge = permit.settle(data.len());
        stats.set_rate_debt(charge.debt_bytes);
        let next = received
            .checked_add(data.len())
            .ok_or(RangeAttemptFailure::OversizedBody)?;
        if next > expected || data.len() > buffer.capacity() {
            record_attempt_discarded(discard, stats, data.len())?;
            return Err(RangeAttemptFailure::OversizedBody);
        }
        speed_window_bytes = speed_window_bytes.saturating_add(data.len());
        if let Some(digest) = &mut body_digest {
            digest.update(&data);
        }
        if lowest_speed_limit != 0
            && speed_window_elapsed >= body_timeout
            && below_lowest_speed(speed_window_bytes, speed_window_elapsed, lowest_speed_limit)
        {
            stats.set_diagnostic(StatsDiagnostic {
                condition: ConnectionCondition::Stalled,
                reason: Some(ConnectionConditionReason::LowestSpeed),
            });
            record_attempt_discarded(discard, stats, data.len())?;
            return Err(RangeAttemptFailure::LowestSpeed);
        }
        if speed_window_elapsed >= body_timeout {
            speed_window_elapsed = Duration::ZERO;
            speed_window_bytes = 0;
        }
        let offset = assignment
            .span
            .offset
            .checked_add(u64::try_from(received).map_err(|_| RangeAttemptFailure::OversizedBody)?)
            .ok_or(RangeAttemptFailure::OversizedBody)?;
        buffer
            .writable()
            .map_err(|_| RangeAttemptFailure::Cancelled)?[..data.len()]
            .copy_from_slice(&data);
        buffer
            .mark_filled(data.len(), OwnerTag::Storage)
            .map_err(|_| RangeAttemptFailure::Cancelled)?;
        if events
            .send(AttemptEvent::Chunk {
                lease: assignment.lease,
                offset,
                buffer,
                _ingress: ingress,
                discard: discard.clone(),
            })
            .await
            .is_err()
        {
            record_attempt_discarded(discard, stats, data.len())?;
            return Err(RangeAttemptFailure::Cancelled);
        }
        received = next;
    }
    if received != expected {
        return Err(RangeAttemptFailure::ShortBody);
    }
    response.finish().await;
    if let (Some(expected), Some(actual)) = (response_digest, body_digest) {
        let actual: [u8; 32] = actual.finalize().into();
        if actual != expected.value() {
            return Err(RangeAttemptFailure::Response {
                error: HttpRangeResponseError::RepresentationDigestMismatch,
                retry_after: None,
            });
        }
        return Ok(Some(expected.journal_digest()));
    }
    Ok(None)
}

fn below_lowest_speed(bytes: usize, elapsed: Duration, limit: u64) -> bool {
    if elapsed.is_zero() {
        return false;
    }
    u128::try_from(bytes)
        .unwrap_or(u128::MAX)
        .saturating_mul(1_000_000_000)
        < u128::from(limit).saturating_mul(elapsed.as_nanos())
}

#[allow(clippy::too_many_arguments)]
async fn acquire_read_slot(
    events: &mpsc::Sender<AttemptEvent>,
    lease: LeaseId,
    minimum_capacity: usize,
    rate: &RateArbiter,
    rate_path: RatePath,
    ingress_budget: &HttpIngressBudgets,
    discard: &HttpDiscardAttemptGuard,
    cancellation: &HttpCancellation,
    stats: &HttpTransferStats,
) -> Result<(BufferLease, HttpIngressPermit, RatePermit), RangeAttemptFailure> {
    let requested = NonZeroUsize::new(minimum_capacity).ok_or(RangeAttemptFailure::Cancelled)?;
    loop {
        let (response, receiver) = oneshot::channel();
        events
            .send(AttemptEvent::PrepareRead {
                lease,
                minimum_capacity,
                response,
            })
            .await
            .map_err(|_| RangeAttemptFailure::Cancelled)?;
        let buffer = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(RangeAttemptFailure::Cancelled),
            buffer = receiver => buffer.ok().flatten(),
        };
        let Some(buffer) = buffer else {
            stats.set_diagnostic(StatsDiagnostic {
                condition: ConnectionCondition::Backpressured,
                reason: Some(ConnectionConditionReason::BufferBackpressure),
            });
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(RangeAttemptFailure::Cancelled),
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
            continue;
        };
        if let Some(scope) = discard.exhausted_scope() {
            drop(buffer);
            return Err(RangeAttemptFailure::DiscardBudgetExhausted(scope));
        }
        let rate_request = NonZeroUsize::new(buffer.capacity())
            .expect("a pooled HTTP ingress buffer has nonzero capacity");
        let permit = match rate
            .try_acquire(rate_path, rate_request)
            .map_err(|_| RangeAttemptFailure::Cancelled)?
        {
            Some(permit) => permit,
            None => {
                stats.set_diagnostic(StatsDiagnostic {
                    condition: ConnectionCondition::RateLimited,
                    reason: Some(ConnectionConditionReason::IngressRateLimit),
                });
                drop(buffer);
                let permit = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(RangeAttemptFailure::Cancelled),
                    permit = rate.acquire(rate_path, requested) => permit
                        .map_err(|_| RangeAttemptFailure::Cancelled)?,
                };
                drop(permit);
                continue;
            }
        };
        let ingress = match ingress_budget.try_acquire(buffer.capacity()) {
            Ok(permit) => permit,
            Err(_) => {
                stats.set_diagnostic(StatsDiagnostic {
                    condition: ConnectionCondition::Backpressured,
                    reason: Some(ConnectionConditionReason::BufferBackpressure),
                });
                drop(permit);
                drop(buffer);
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(RangeAttemptFailure::Cancelled),
                    () = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
                continue;
            }
        };
        stats.clear_diagnostic();
        return Ok((buffer, ingress, permit));
    }
}

fn endgame_eligible_originals(
    active: &BTreeMap<LeaseId, ActiveAttempt>,
    now_ms: u64,
    body_timeout: Duration,
) -> Vec<LeaseId> {
    let timeout_ms = u64::try_from(body_timeout.as_millis()).unwrap_or(u64::MAX);
    let grace_ms = timeout_ms.checked_div(4).unwrap_or(0).max(1_000);
    active
        .values()
        .filter(|attempt| {
            attempt.opened
                && attempt.assignment.overlap_group.is_none()
                && now_ms.saturating_sub(attempt.last_progress_ms)
                    >= timeout_ms.saturating_sub(grace_ms)
        })
        .map(|attempt| attempt.assignment.lease)
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn settle_endgame_loser(
    lease: LeaseId,
    group: ariax_core::OverlapGroupId,
    task: TaskId,
    generation: Generation,
    storage: &mut StorageEngine,
    coordinator: &mut HttpRangeCoordinator,
    active: &mut BTreeMap<LeaseId, ActiveAttempt>,
    pending_endgame: &mut BTreeMap<ariax_core::OverlapGroupId, PendingEndgameCandidate>,
    stats: &HttpTransferStats,
) -> Result<(), HttpMultiRangeError> {
    let pending = pending_endgame
        .remove(&group)
        .ok_or(HttpMultiRangeError::Protocol)?;
    if pending.competitor != lease {
        return Err(HttpMultiRangeError::Protocol);
    }
    let loser = active.remove(&lease).ok_or(HttpMultiRangeError::Protocol)?;
    let acknowledgements = if loser.opened {
        storage.abort_lease(task, generation, lease, LeaseAbortReason::OverlapLost)?
    } else {
        storage.settle_unopened_overlap_member(task, generation, group, lease)?
    };
    let rolled_back = acknowledgements
        .iter()
        .any(|ack| matches!(ack, WriteAck::SpanRolledBack { group: id, .. } if *id == group));
    let committed = acknowledgements
        .iter()
        .any(|ack| matches!(ack, WriteAck::PieceDurable { .. }));
    if rolled_back == committed {
        return Err(HttpMultiRangeError::Protocol);
    }
    coordinator.settle_overlap(
        group,
        pending.attempt.assignment.lease,
        if rolled_back {
            HttpOverlapSettlement::RolledBack
        } else {
            HttpOverlapSettlement::CandidateCommitted
        },
    )?;
    stats.remove_provisional(loser.received);
    record_discarded(&loser.discard, stats, loser.received)?;
    stats.remove_provisional(pending.attempt.received);
    if committed {
        stats.add_durable(pending.attempt.received);
    } else {
        record_discarded(&pending.attempt.discard, stats, pending.attempt.received)?;
    }
    stats.set_active(active.len());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_attempt_event(
    event: AttemptEvent,
    task: TaskId,
    generation: Generation,
    started: Instant,
    retry_elapsed_offset_ms: u64,
    storage: &mut StorageEngine,
    coordinator: &mut HttpRangeCoordinator,
    budgets: &mut BTreeMap<PieceId, HttpRetryBudget>,
    active: &mut BTreeMap<LeaseId, ActiveAttempt>,
    pending_endgame: &mut BTreeMap<ariax_core::OverlapGroupId, PendingEndgameCandidate>,
    endgame_losers: &mut BTreeMap<LeaseId, ariax_core::OverlapGroupId>,
    cancelled_leases: &mut BTreeSet<LeaseId>,
    stats: &HttpTransferStats,
) -> Result<AttemptAction, HttpMultiRangeError> {
    match event {
        AttemptEvent::Head {
            lease,
            response_digest,
            start,
        } => {
            if endgame_losers.contains_key(&lease) || cancelled_leases.contains(&lease) {
                let _ignored = start.send(Err(RangeAttemptFailure::Cancelled));
                return Ok(AttemptAction::None);
            }
            let Some(attempt) = active.get(&lease) else {
                let _ignored = start.send(Err(RangeAttemptFailure::Cancelled));
                return Ok(AttemptAction::None);
            };
            if attempt.opened {
                let _ignored = start.send(Err(RangeAttemptFailure::Cancelled));
                return Err(HttpMultiRangeError::Protocol);
            }
            if let Some(group) = attempt.assignment.overlap_group {
                let peer = active
                    .iter()
                    .find(|(peer_lease, peer)| {
                        **peer_lease != lease && peer.assignment.overlap_group == Some(group)
                    })
                    .map(|(_, peer)| peer)
                    .ok_or(HttpMultiRangeError::Protocol)?;
                if peer.assignment.source != attempt.assignment.source
                    && (response_digest.is_none() || peer.response_digest != response_digest)
                {
                    let _ignored = start.send(Err(RangeAttemptFailure::Response {
                        error: HttpRangeResponseError::RepresentationDigestChanged,
                        retry_after: None,
                    }));
                    return Ok(AttemptAction::None);
                }
            }
            let attempt = active
                .get_mut(&lease)
                .ok_or(HttpMultiRangeError::Protocol)?;
            if let Err(error) = storage.begin_lease(LeaseWritePlan {
                task,
                generation,
                transfer_attempt: attempt.transfer_attempt,
                lease,
                span: attempt.assignment.span,
                validator: attempt.validator,
                overlap_group: attempt.assignment.overlap_group,
            }) {
                let _ignored = start.send(Err(RangeAttemptFailure::Cancelled));
                return Err(HttpMultiRangeError::Storage(error));
            }
            attempt.response_digest = response_digest;
            attempt.opened = true;
            attempt.last_progress_ms = elapsed_ms(started);
            start
                .send(Ok(()))
                .map_err(|_| HttpMultiRangeError::Protocol)?;
        }
        AttemptEvent::PrepareRead {
            lease,
            minimum_capacity,
            response,
        } => {
            if endgame_losers.contains_key(&lease) || cancelled_leases.contains(&lease) {
                let _sent = response.send(None);
                return Ok(AttemptAction::None);
            }
            let buffer = active
                .get(&lease)
                .filter(|attempt| attempt.opened)
                .and_then(|_| storage.reserve_network_buffer(minimum_capacity).ok());
            let _sent = response.send(buffer);
        }
        AttemptEvent::Chunk {
            lease,
            offset,
            buffer,
            _ingress: _,
            discard,
        } => {
            let data_len = buffer.len();
            if endgame_losers.contains_key(&lease) || cancelled_leases.contains(&lease) {
                storage.discard_network_buffer(buffer)?;
                record_discarded(&discard, stats, data_len)?;
                return Ok(AttemptAction::None);
            }
            let Some(attempt) = active.get(&lease).cloned() else {
                storage.discard_network_buffer(buffer)?;
                record_discarded(&discard, stats, data_len)?;
                return Ok(AttemptAction::None);
            };
            if !attempt.opened
                || offset
                    != attempt.assignment.span.offset
                        + u64::try_from(attempt.received)
                            .map_err(|_| HttpMultiRangeError::Protocol)?
                || attempt.received.saturating_add(data_len) > attempt.assignment.span.len
            {
                return Err(HttpMultiRangeError::Protocol);
            }
            storage
                .write_block(WriteBlock {
                    task,
                    generation,
                    lease,
                    global_offset: offset,
                    expected_len: data_len,
                    buffer,
                    piece: attempt.assignment.piece,
                })
                .await?;
            active
                .get_mut(&lease)
                .ok_or(HttpMultiRangeError::Protocol)?
                .received += data_len;
            active
                .get_mut(&lease)
                .ok_or(HttpMultiRangeError::Protocol)?
                .last_progress_ms = elapsed_ms(started);
            stats.add_accepted(data_len);
            stats.add_provisional(data_len);
        }
        AttemptEvent::Terminal { lease, result } => {
            if endgame_losers.contains_key(&lease) || cancelled_leases.contains(&lease) {
                return Ok(AttemptAction::None);
            }
            let Some(attempt) = active.remove(&lease) else {
                return Ok(AttemptAction::None);
            };
            match result {
                Ok(response_digest) => {
                    if !attempt.opened || attempt.received != attempt.assignment.span.len {
                        return Err(HttpMultiRangeError::Protocol);
                    }
                    let acknowledgements = storage.commit_lease(LeaseCommit {
                        task,
                        generation,
                        lease,
                        received_len: u64::try_from(attempt.received)
                            .map_err(|_| HttpMultiRangeError::Protocol)?,
                        validator: attempt.validator,
                        response_digest,
                    })?;
                    if let [
                        WriteAck::LeaseCommitPending {
                            group,
                            lease: pending,
                        },
                    ] = acknowledgements.as_slice()
                    {
                        if *pending != lease {
                            return Err(HttpMultiRangeError::Protocol);
                        }
                        let fence = coordinator.begin_overlap_commit(lease)?;
                        if fence.group != *group {
                            return Err(HttpMultiRangeError::Protocol);
                        }
                        if endgame_losers.contains_key(&fence.competitor)
                            || pending_endgame.contains_key(&fence.group)
                        {
                            return Err(HttpMultiRangeError::Protocol);
                        }
                        endgame_losers.insert(fence.competitor, fence.group);
                        pending_endgame.insert(
                            fence.group,
                            PendingEndgameCandidate {
                                attempt,
                                competitor: fence.competitor,
                            },
                        );
                        stats.set_active(active.len());
                        return Ok(AttemptAction::CancelLease(fence.competitor));
                    }
                    if !matches!(
                        acknowledgements.as_slice(),
                        [
                            WriteAck::LeaseCommitted { .. },
                            WriteAck::PieceDurable { .. }
                        ]
                    ) {
                        return Err(HttpMultiRangeError::Protocol);
                    }
                    coordinator.complete(lease)?;
                    stats.add_durable(attempt.received);
                    stats.remove_provisional(attempt.received);
                }
                Err(failure) => {
                    let discard_exhaustion = match &failure {
                        RangeAttemptFailure::DiscardBudgetExhausted(scope) => Some(*scope),
                        _ => None,
                    };
                    let mut range_released = false;
                    let mut action = AttemptAction::None;
                    if attempt.opened {
                        let acknowledgements =
                            storage.abort_lease(task, generation, lease, abort_reason(&failure))?;
                        if let Some(group) = acknowledgements.iter().find_map(|ack| match ack {
                            WriteAck::SpanRolledBack { group, .. } => Some(*group),
                            _ => None,
                        }) {
                            let members = coordinator.rollback_overlap(group)?;
                            let peer = members
                                .into_iter()
                                .find(|member| *member != lease)
                                .ok_or(HttpMultiRangeError::Protocol)?;
                            if let Some(peer_attempt) = active.remove(&peer) {
                                stats.remove_provisional(peer_attempt.received);
                                record_discarded(
                                    &peer_attempt.discard,
                                    stats,
                                    peer_attempt.received,
                                )?;
                            }
                            cancelled_leases.insert(peer);
                            action = AttemptAction::CancelLease(peer);
                            range_released = true;
                        }
                    } else if let Some(group) = attempt.assignment.overlap_group {
                        storage.settle_unopened_overlap_member(task, generation, group, lease)?;
                    }
                    stats.remove_provisional(attempt.received);
                    record_discarded(&attempt.discard, stats, attempt.received)?;
                    if let Some(scope) = discard_exhaustion {
                        return Err(HttpMultiRangeError::DiscardBudgetExhausted(scope));
                    }
                    let now_ms = elapsed_ms(started);
                    apply_attempt_failure(
                        failure,
                        attempt.clone(),
                        AttemptFailureContext {
                            now_ms,
                            retry_elapsed_ms: retry_elapsed_offset_ms.saturating_add(now_ms),
                            storage,
                            coordinator,
                            budgets,
                            released: range_released,
                            stats,
                        },
                    )?;
                    if let Some(group) = attempt.assignment.overlap_group
                        && !range_released
                    {
                        for other in active.values_mut() {
                            if other.assignment.overlap_group == Some(group) {
                                other.assignment.overlap_group = None;
                            }
                        }
                    }
                    stats.set_active(active.len());
                    return Ok(action);
                }
            }
            stats.set_active(active.len());
        }
    }
    Ok(AttemptAction::None)
}

struct AttemptFailureContext<'a> {
    now_ms: u64,
    retry_elapsed_ms: u64,
    storage: &'a mut StorageEngine,
    coordinator: &'a mut HttpRangeCoordinator,
    budgets: &'a mut BTreeMap<PieceId, HttpRetryBudget>,
    released: bool,
    stats: &'a HttpTransferStats,
}

fn apply_attempt_failure(
    failure: RangeAttemptFailure,
    attempt: ActiveAttempt,
    context: AttemptFailureContext<'_>,
) -> Result<(), HttpMultiRangeError> {
    let AttemptFailureContext {
        now_ms,
        retry_elapsed_ms,
        storage,
        coordinator,
        budgets,
        released,
        stats,
    } = context;
    if matches!(failure, RangeAttemptFailure::Cancelled) {
        return Err(HttpMultiRangeError::Cancelled);
    }
    let cause = retry_cause(&failure);
    let retry_after = match &failure {
        RangeAttemptFailure::Response { retry_after, .. } => retry_after.as_deref(),
        _ => None,
    };
    let budget = budgets
        .get(&attempt.assignment.piece)
        .ok_or(HttpMultiRangeError::Protocol)?;
    if cause == HttpRetryCause::StaleValidator {
        let next_action = match budget.policy().stale_validator_policy {
            HttpStaleValidatorPolicy::Fail => HttpRetryNextAction::TerminalFailure,
            HttpStaleValidatorPolicy::RestartIfSafe => HttpRetryNextAction::RestartGeneration,
            HttpStaleValidatorPolicy::Revalidate => HttpRetryNextAction::RevalidateSource,
        };
        stats.set_retry_diagnostic(live_retry_diagnostic(
            &attempt,
            budget,
            HttpRetryDiagnosticCause::Live(cause),
            None,
            None,
            next_action,
            released,
        ));
        record_coordinator_failure(
            coordinator,
            &attempt,
            HttpRangeFailure::RetryAt(now_ms),
            released,
        )?;
        return match budget.policy().stale_validator_policy {
            HttpStaleValidatorPolicy::Fail => Err(HttpMultiRangeError::StaleValidator),
            HttpStaleValidatorPolicy::RestartIfSafe => {
                Err(HttpMultiRangeError::RepresentationRestart)
            }
            HttpStaleValidatorPolicy::Revalidate => {
                stats.add_retry();
                Err(HttpMultiRangeError::RevalidateSource(
                    attempt.assignment.source,
                ))
            }
        };
    }
    let decision = budget
        .decide_after_failure(
            attempt.assignment.source,
            cause,
            Duration::from_millis(retry_elapsed_ms),
            retry_after,
            SystemTime::now(),
            attempt.assignment.lease.get() ^ u64::from(attempt.assignment.source.get()),
        )
        .map_err(HttpMultiRangeError::Retry)?;
    let (delay, stop_reason, next_action) = match decision {
        HttpRetryDecision::Retry { delay, source } => {
            (Some((delay, source)), None, HttpRetryNextAction::RetryRange)
        }
        HttpRetryDecision::Stop(HttpRetryStopReason::NonRetriable) => (
            None,
            Some(HttpRetryStopReason::NonRetriable),
            HttpRetryNextAction::DisableSource,
        ),
        HttpRetryDecision::Stop(HttpRetryStopReason::MirrorAttemptCap) => (
            None,
            Some(HttpRetryStopReason::MirrorAttemptCap),
            HttpRetryNextAction::DifferentSource,
        ),
        HttpRetryDecision::Stop(
            reason @ (HttpRetryStopReason::TotalAttemptCap | HttpRetryStopReason::ElapsedCap),
        ) => (None, Some(reason), HttpRetryNextAction::TerminalFailure),
    };
    stats.set_retry_diagnostic(live_retry_diagnostic(
        &attempt,
        budget,
        HttpRetryDiagnosticCause::Live(cause),
        delay,
        stop_reason,
        next_action,
        released,
    ));
    let range_failure = match decision {
        HttpRetryDecision::Retry { delay, source } => {
            persist_range_retry_state(
                storage,
                attempt.clone(),
                budget,
                cause,
                retry_elapsed_ms,
                delay,
                source,
            )?;
            stats.add_retry();
            HttpRangeFailure::RetryAt(
                now_ms.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)),
            )
        }
        HttpRetryDecision::Stop(HttpRetryStopReason::NonRetriable) => {
            HttpRangeFailure::DisableSource
        }
        HttpRetryDecision::Stop(HttpRetryStopReason::MirrorAttemptCap) => {
            HttpRangeFailure::RetryAt(now_ms)
        }
        HttpRetryDecision::Stop(
            HttpRetryStopReason::TotalAttemptCap | HttpRetryStopReason::ElapsedCap,
        ) => {
            record_coordinator_failure(
                coordinator,
                &attempt,
                HttpRangeFailure::RetryAt(now_ms),
                released,
            )?;
            return Err(HttpMultiRangeError::Exhausted);
        }
    };
    record_coordinator_failure(coordinator, &attempt, range_failure, released)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn live_retry_diagnostic(
    attempt: &ActiveAttempt,
    budget: &HttpRetryBudget,
    cause: HttpRetryDiagnosticCause,
    delay: Option<(Duration, HttpRetryDelaySource)>,
    stop_reason: Option<HttpRetryStopReason>,
    next_action: HttpRetryNextAction,
    released: bool,
) -> HttpRetryDiagnosticSnapshot {
    let totals = budget.stats();
    let source_attempt = budget.attempts_for_mirror(attempt.assignment.source);
    let scheduled_at_unix_ms = now_unix_ms().unwrap_or(0);
    let (delay_ms, delay) = delay.map_or((0, None), |(delay, source)| {
        (
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Some(HttpRetryDelayDiagnostic::Live(source)),
        )
    });
    HttpRetryDiagnosticSnapshot {
        cause,
        source: attempt.assignment.source,
        piece: attempt.assignment.piece,
        prior_lease: Some(attempt.assignment.lease),
        next_lease: None,
        total_attempt: totals.attempts,
        total_remaining: budget
            .policy()
            .max_attempts
            .get()
            .saturating_sub(totals.attempts),
        source_attempt,
        source_remaining: budget
            .policy()
            .max_attempts_per_mirror
            .get()
            .saturating_sub(source_attempt),
        scheduled_at_unix_ms,
        delay_ms,
        retry_at_unix_ms: scheduled_at_unix_ms.saturating_add(delay_ms),
        delay,
        stop_reason,
        next_action,
        lease_disposition: if released {
            HttpRetryLeaseDisposition::RolledBack
        } else if attempt.opened {
            HttpRetryLeaseDisposition::Aborted
        } else {
            HttpRetryLeaseDisposition::Unopened
        },
    }
}

fn record_coordinator_failure(
    coordinator: &mut HttpRangeCoordinator,
    attempt: &ActiveAttempt,
    failure: HttpRangeFailure,
    released: bool,
) -> Result<(), HttpMultiRangeError> {
    if released {
        coordinator.record_released_failure(
            attempt.assignment.piece,
            attempt.assignment.source,
            failure,
        )?;
    } else {
        coordinator.fail(attempt.assignment.lease, failure)?;
    }
    Ok(())
}

fn persist_range_retry_state(
    storage: &mut StorageEngine,
    attempt: ActiveAttempt,
    budget: &HttpRetryBudget,
    cause: HttpRetryCause,
    retry_elapsed_ms: u64,
    delay: Duration,
    source: HttpRetryDelaySource,
) -> Result<(), HttpMultiRangeError> {
    let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
    if delay_ms == 0 {
        return Ok(());
    }
    let piece = attempt.assignment.piece;
    let mirror = attempt.assignment.source;
    let piece_scope_id =
        piece_retry_scope_id(piece).ok_or(HttpMultiRangeError::IdentifierExhausted)?;
    let span_scope_id =
        span_retry_scope_id(piece, mirror).ok_or(HttpMultiRangeError::IdentifierExhausted)?;
    let max_elapsed_ms = u64::try_from(budget.policy().max_elapsed.as_millis()).unwrap_or(u64::MAX);
    let scheduled_at_unix_ms = now_unix_ms().unwrap_or(0);
    let error_class = retry_error_kind(cause);
    let retry_reason = retry_reason(source);
    let common = RetryStateWrite {
        scope: RetryScope::Piece,
        scope_id: piece_scope_id,
        attempt: budget.stats().attempts,
        elapsed_before_wait_ms: retry_elapsed_ms.min(max_elapsed_ms),
        scheduled_at_unix_ms,
        delay_ms,
        error_class,
        retry_reason,
    };
    storage.record_retry_state(common)?;
    storage.record_retry_state(RetryStateWrite {
        scope: RetryScope::Span,
        scope_id: span_scope_id,
        attempt: budget.attempts_for_mirror(mirror),
        ..common
    })?;
    Ok(())
}

const fn retry_error_kind(cause: HttpRetryCause) -> ErrorKind {
    match cause {
        HttpRetryCause::Transport(
            HttpRetryTransportFailure::Timeout
            | HttpRetryTransportFailure::Hang
            | HttpRetryTransportFailure::LowestSpeed,
        ) => ErrorKind::Timeout,
        HttpRetryCause::Transport(_) | HttpRetryCause::HttpStatus(_) => ErrorKind::Network,
        HttpRetryCause::Authentication => ErrorKind::NeedsCredentials,
        HttpRetryCause::InvalidRange => ErrorKind::InvalidRange,
        HttpRetryCause::StaleValidator => ErrorKind::StaleValidator,
        HttpRetryCause::Checksum => ErrorKind::ChecksumMismatch,
        HttpRetryCause::Storage => ErrorKind::Disk,
        HttpRetryCause::Cancelled => ErrorKind::Cancelled,
        HttpRetryCause::Policy => ErrorKind::Config,
    }
}

const fn retry_reason(source: HttpRetryDelaySource) -> RetryReason {
    match source {
        HttpRetryDelaySource::RetryAfter => RetryReason::RetryAfter,
        HttpRetryDelaySource::RetryAfterClamped => RetryReason::PolicyClamp,
        HttpRetryDelaySource::FixedBackoff
        | HttpRetryDelaySource::ExponentialBackoff
        | HttpRetryDelaySource::EqualJitterBackoff
        | HttpRetryDelaySource::RetryAfterIgnored
        | HttpRetryDelaySource::BackoffAfterInvalidRetryAfter => RetryReason::Backoff,
    }
}

#[allow(clippy::too_many_arguments)]
fn fail_panicked_attempt(
    lease: LeaseId,
    task: TaskId,
    generation: Generation,
    now_ms: u64,
    storage: &mut StorageEngine,
    coordinator: &mut HttpRangeCoordinator,
    budgets: &BTreeMap<PieceId, HttpRetryBudget>,
    active: &mut BTreeMap<LeaseId, ActiveAttempt>,
    stats: &HttpTransferStats,
) -> Result<(), HttpMultiRangeError> {
    let Some(attempt) = active.remove(&lease) else {
        return Ok(());
    };
    if attempt.opened {
        storage.abort_lease(task, generation, lease, LeaseAbortReason::Retry)?;
    }
    stats.remove_provisional(attempt.received);
    record_discarded(&attempt.discard, stats, attempt.received)?;
    let budget = budgets
        .get(&attempt.assignment.piece)
        .ok_or(HttpMultiRangeError::Protocol)?;
    stats.set_retry_diagnostic(live_retry_diagnostic(
        &attempt,
        budget,
        HttpRetryDiagnosticCause::WorkerPanic,
        None,
        None,
        HttpRetryNextAction::RetryRange,
        false,
    ));
    coordinator.fail(lease, HttpRangeFailure::RetryAt(now_ms))?;
    stats.add_retry();
    stats.set_active(active.len());
    Ok(())
}

fn retry_cause(failure: &RangeAttemptFailure) -> HttpRetryCause {
    match failure {
        RangeAttemptFailure::Client(error) if error.retriable() => {
            HttpRetryCause::Transport(retry_transport_failure(error))
        }
        RangeAttemptFailure::Client(_) => HttpRetryCause::Policy,
        RangeAttemptFailure::Response {
            error: HttpRangeResponseError::UnexpectedStatus(status),
            ..
        } => HttpRetryCause::HttpStatus(status.as_u16()),
        RangeAttemptFailure::Response {
            error:
                HttpRangeResponseError::ValidatorChanged | HttpRangeResponseError::ResourceChanged,
            ..
        } => HttpRetryCause::StaleValidator,
        RangeAttemptFailure::Response {
            error:
                HttpRangeResponseError::RepresentationDigestChanged
                | HttpRangeResponseError::RepresentationDigestMismatch,
            ..
        } => HttpRetryCause::Checksum,
        RangeAttemptFailure::Response { .. } | RangeAttemptFailure::OversizedBody => {
            HttpRetryCause::InvalidRange
        }
        RangeAttemptFailure::ShortBody => {
            HttpRetryCause::Transport(HttpRetryTransportFailure::UnexpectedEof)
        }
        RangeAttemptFailure::LowestSpeed => {
            HttpRetryCause::Transport(HttpRetryTransportFailure::LowestSpeed)
        }
        RangeAttemptFailure::Timeout => {
            HttpRetryCause::Transport(HttpRetryTransportFailure::Timeout)
        }
        RangeAttemptFailure::Hang => HttpRetryCause::Transport(HttpRetryTransportFailure::Hang),
        RangeAttemptFailure::DiscardBudgetExhausted(_) => HttpRetryCause::Policy,
        RangeAttemptFailure::Cancelled => HttpRetryCause::Cancelled,
    }
}

fn retry_transport_failure(error: &HttpPolicyClientError) -> HttpRetryTransportFailure {
    match error {
        HttpPolicyClientError::BodyTimeout => HttpRetryTransportFailure::Timeout,
        HttpPolicyClientError::Destination(_) => HttpRetryTransportFailure::DnsTransient,
        HttpPolicyClientError::ProxyRequest(_) => HttpRetryTransportFailure::ProxyConnect,
        HttpPolicyClientError::Transport(
            HttpTransportError::ConnectTimeout
            | HttpTransportError::TlsHandshakeTimeout
            | HttpTransportError::HandshakeTimeout,
        ) => HttpRetryTransportFailure::Timeout,
        HttpPolicyClientError::Transport(HttpTransportError::Hyper(_)) => {
            HttpRetryTransportFailure::StaleConnection
        }
        _ => HttpRetryTransportFailure::Reset,
    }
}

fn abort_reason(failure: &RangeAttemptFailure) -> LeaseAbortReason {
    match failure {
        RangeAttemptFailure::ShortBody => LeaseAbortReason::ShortBody,
        RangeAttemptFailure::OversizedBody => LeaseAbortReason::OversizedBody,
        RangeAttemptFailure::Cancelled => LeaseAbortReason::Cancelled,
        RangeAttemptFailure::Timeout
        | RangeAttemptFailure::Hang
        | RangeAttemptFailure::LowestSpeed
        | RangeAttemptFailure::DiscardBudgetExhausted(_)
        | RangeAttemptFailure::Client(_)
        | RangeAttemptFailure::Response { .. } => LeaseAbortReason::Retry,
    }
}

#[must_use]
pub fn derive_http_journal_id(task: TaskId, gid: Gid) -> JournalId {
    let mut digest = Sha256::new();
    digest.update(HTTP_JOURNAL_ID_DOMAIN.as_bytes());
    digest.update(task.get().to_le_bytes());
    digest.update(gid.get().to_le_bytes());
    let digest: [u8; 32] = digest.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    if bytes == [0; 16] {
        bytes[15] = 1;
    }
    JournalId::new(bytes).expect("derived HTTP journal identifier is nonzero")
}

#[must_use]
pub fn http_journal_directory(root: &Path, gid: Gid) -> PathBuf {
    root.join(format!("{:016x}", gid.get()))
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engine::StorageEngineDiskFault;
    use crate::{
        DEFAULT_HTTP_DISCARD_ATTEMPT_BYTES, DEFAULT_HTTP_DISCARD_HOST_BYTES,
        DEFAULT_HTTP_DISCARD_TASK_BYTES, HTTP_CONNECTION_RESERVATION_BYTES, HttpDestinationPolicy,
        HttpDirectTransportConfig, HttpDiscardScopeLimits, HttpPolicyClientConfig, HttpResolver,
        HttpResolverBackend, HttpResolverConfig, HttpRetryBackoff, HttpTaskOptions,
        HttpTransportBudgets, KnownLengthHttpRecoveryRequest, recover_known_length_http,
    };
    use ariax_storage::{
        GenerationStartReason, JournalPayload, JournalStateLimits, OptionsSnapshotScope,
        PathPlatform, ReplayLimits, SafePathBuilder,
    };
    use std::fs;
    use std::io::{Seek as _, SeekFrom, Write as _};
    use std::net::SocketAddr;
    use std::num::NonZeroU32;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);
    const MIB: usize = 1024 * 1024;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let id = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "ariax-http-multi-{label}-{}-{id}",
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

    #[derive(Clone, Copy, Debug)]
    enum MirrorMode {
        Valid,
        SharedRepresentationDigest,
        IncorrectRepresentationDigest,
        IncorrectRangeRepresentationDigest,
        IgnoreRange,
        ShortRange,
    }

    async fn serve_mirror(
        data: Arc<[u8]>,
        mode: MirrorMode,
        connections: usize,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            for _ in 0..connections {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let request = read_request_head(&mut stream).await;
                let (start, end) = request_range(&request).expect("range request");
                let probe = start == 0 && end == 0;
                if !probe && matches!(mode, MirrorMode::IgnoreRange) {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    let _written = stream.write_all(response.as_bytes()).await;
                    continue;
                }
                let body = &data[start..=end];
                let digest_header = match mode {
                    MirrorMode::SharedRepresentationDigest => format!(
                        "Repr-Digest: sha-256=:{}:\r\n",
                        base64_encode(&Sha256::digest(body))
                    ),
                    MirrorMode::IncorrectRepresentationDigest => {
                        format!("Repr-Digest: sha-256=:{}:\r\n", base64_encode(&[0x55; 32]))
                    }
                    MirrorMode::IncorrectRangeRepresentationDigest if probe => format!(
                        "Repr-Digest: sha-256=:{}:\r\n",
                        base64_encode(&Sha256::digest(body))
                    ),
                    MirrorMode::IncorrectRangeRepresentationDigest => {
                        format!("Repr-Digest: sha-256=:{}:\r\n", base64_encode(&[0x55; 32]))
                    }
                    MirrorMode::Valid | MirrorMode::IgnoreRange | MirrorMode::ShortRange => {
                        String::new()
                    }
                };
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"v1\"\r\n{digest_header}Connection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream.write_all(response.as_bytes()).await.expect("head");
                let body = if !probe && matches!(mode, MirrorMode::ShortRange) {
                    &body[..body.len() / 2]
                } else {
                    body
                };
                let _written = stream.write_all(body).await;
            }
        });
        (address, task)
    }

    async fn serve_restart_mirror(
        data: Arc<[u8]>,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);
        let hold_second_piece = Arc::new(AtomicBool::new(true));
        let task = tokio::spawn(async move {
            let mut handled = 0_usize;
            while handled < 5 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let Some(request) = try_read_request_head(&mut stream).await else {
                    continue;
                };
                handled += 1;
                let (start, end) = request_range(&request).expect("range request");
                recorded
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((start, end));
                let body = &data[start..=end];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream.write_all(response.as_bytes()).await.expect("head");
                if start >= MIB && hold_second_piece.swap(false, Ordering::AcqRel) {
                    let mut closed = [0_u8; 1];
                    let _closed = stream.read(&mut closed).await;
                } else {
                    stream.write_all(body).await.expect("body");
                }
            }
        });
        (address, ranges, task)
    }

    async fn serve_retry_wait_mirror(
        data: Arc<[u8]>,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize, Instant)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut range_attempts = 0_usize;
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let Some(request) = try_read_request_head(&mut stream).await else {
                    continue;
                };
                let (start, end) = request_range(&request).expect("range request");
                recorded
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((start, end, Instant::now()));
                let probe = start == 0 && end == 0;
                if !probe {
                    range_attempts += 1;
                }
                if !probe && range_attempts == 1 {
                    stream
                        .write_all(
                            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nRetry-After: 5\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .expect("retry response");
                    continue;
                }
                let body = &data[start..=end];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream.write_all(response.as_bytes()).await.expect("head");
                stream.write_all(body).await.expect("body");
                if !probe {
                    break;
                }
            }
        });
        (address, requests, task)
    }

    async fn serve_recovery_validator_mirror(
        data: Arc<[u8]>,
        initial_etag: &str,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize)>>>,
        Arc<Mutex<String>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);
        let etag = Arc::new(Mutex::new(initial_etag.to_owned()));
        let current_etag = Arc::clone(&etag);
        let hold_second_piece = Arc::new(AtomicBool::new(true));
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let data = Arc::clone(&data);
                let recorded = Arc::clone(&recorded);
                let current_etag = Arc::clone(&current_etag);
                let hold_second_piece = Arc::clone(&hold_second_piece);
                handlers.spawn(async move {
                    let Some(request) = try_read_request_head(&mut stream).await else {
                        return;
                    };
                    let (start, end) = request_range(&request).expect("range request");
                    recorded
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push((start, end));
                    let body = &data[start..=end];
                    let etag = current_etag
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: {etag}\r\nConnection: close\r\n\r\n",
                        body.len(),
                        data.len()
                    );
                    stream.write_all(response.as_bytes()).await.expect("head");
                    if start >= MIB && hold_second_piece.swap(false, Ordering::AcqRel) {
                        let mut closed = [0_u8; 1];
                        let _closed = stream.read(&mut closed).await;
                    } else {
                        stream.write_all(body).await.expect("body");
                    }
                });
            }
        });
        (address, ranges, etag, task)
    }

    async fn serve_recovery_shared_digest_mirror(
        data: Arc<[u8]>,
        hold_second_piece: Arc<AtomicBool>,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize)>>>,
        Arc<Mutex<Arc<[u8]>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);
        let data = Arc::new(Mutex::new(data));
        let served_data = Arc::clone(&data);
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let Some(request) = try_read_request_head(&mut stream).await else {
                    continue;
                };
                let (start, end) = request_range(&request).expect("range request");
                recorded
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((start, end));
                let data = served_data
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let body = &data[start..=end];
                let digest_header = format!(
                    "Repr-Digest: sha-256=:{}:\r\n",
                    base64_encode(&Sha256::digest(body))
                );
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"v1\"\r\n{digest_header}Connection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream.write_all(response.as_bytes()).await.expect("head");
                if start >= MIB && hold_second_piece.swap(false, Ordering::AcqRel) {
                    let mut closed = [0_u8; 1];
                    let _closed = stream.read(&mut closed).await;
                } else {
                    stream.write_all(body).await.expect("body");
                }
            }
        });
        (address, ranges, data, task)
    }

    async fn serve_validator_sequence(
        data: Arc<[u8]>,
        etags: Vec<&'static str>,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);
        let task = tokio::spawn(async move {
            for (index, etag) in etags.into_iter().enumerate() {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let request = read_request_head(&mut stream).await;
                let (start, end) = request_range(&request).expect("range request");
                recorded
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((start, end));
                let body = &data[start..=end];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: {etag}\r\nConnection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream.write_all(response.as_bytes()).await.expect("head");
                if index != 1 {
                    stream.write_all(body).await.expect("body");
                }
            }
        });
        (address, ranges, task)
    }

    async fn serve_representation_restart_mirror(
        old: Arc<[u8]>,
        replacement: Arc<[u8]>,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        assert_eq!(old.len(), replacement.len());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);
        let task = tokio::spawn(async move {
            for index in 0..6 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let request = read_request_head(&mut stream).await;
                let (start, end) = request_range(&request).expect("range request");
                recorded
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((start, end));
                let (etag, data) = if index < 2 {
                    ("\"v1\"", &old)
                } else {
                    ("\"v2\"", &replacement)
                };
                let body = &data[start..=end];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: {etag}\r\nConnection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream.write_all(response.as_bytes()).await.expect("head");
                if index != 2 {
                    stream.write_all(body).await.expect("body");
                }
            }
        });
        (address, ranges, task)
    }

    async fn read_request_head(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut byte = [0_u8; 1];
        while !bytes.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.expect("request byte");
            bytes.push(byte[0]);
        }
        String::from_utf8(bytes).expect("ASCII request")
    }

    async fn try_read_request_head(stream: &mut TcpStream) -> Option<String> {
        let mut bytes = Vec::new();
        let mut byte = [0_u8; 1];
        while !bytes.ends_with(b"\r\n\r\n") {
            match stream.read_exact(&mut byte).await {
                Ok(_) => bytes.push(byte[0]),
                Err(_) => return None,
            }
        }
        String::from_utf8(bytes).ok()
    }

    fn request_range(request: &str) -> Option<(usize, usize)> {
        request.lines().find_map(|line| {
            let value = line
                .strip_prefix("range: bytes=")
                .or_else(|| line.strip_prefix("Range: bytes="))?;
            let (start, end) = value.split_once('-')?;
            Some((start.parse().ok()?, end.trim().parse().ok()?))
        })
    }

    fn data(length: usize) -> Arc<[u8]> {
        (0..length)
            .map(|index| u8::try_from(index % 251).expect("bounded byte"))
            .collect::<Vec<_>>()
            .into()
    }

    fn base64_encode(bytes: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut output = String::new();
        for chunk in bytes.chunks(3) {
            let first = chunk[0];
            let second = chunk.get(1).copied();
            let third = chunk.get(2).copied();
            output.push(char::from(TABLE[usize::from(first >> 2)]));
            output.push(char::from(
                TABLE[usize::from((first & 0x03) << 4 | second.unwrap_or(0) >> 4)],
            ));
            match second {
                Some(second) => {
                    output.push(char::from(
                        TABLE[usize::from((second & 0x0f) << 2 | third.unwrap_or(0) >> 6)],
                    ));
                    output.push(
                        third.map_or('=', |third| char::from(TABLE[usize::from(third & 0x3f)])),
                    );
                }
                None => output.push_str("=="),
            }
        }
        output
    }

    fn policy_client(max_sockets: usize) -> HttpPolicyClient {
        let resolver = HttpResolver::new(HttpResolverConfig {
            backend: HttpResolverBackend::System,
            ..HttpResolverConfig::default()
        })
        .expect("resolver");
        HttpPolicyClient::new(
            resolver,
            HttpPolicyClientConfig {
                destination: HttpDestinationPolicy {
                    allow_loopback: true,
                    ..HttpDestinationPolicy::default()
                },
                direct: HttpDirectTransportConfig {
                    connect_timeout: Duration::from_secs(5),
                    handshake_timeout: Duration::from_secs(5),
                    max_connections_per_origin: max_sockets,
                    max_idle_connections_per_origin: 0,
                    budgets: HttpTransportBudgets::new(
                        max_sockets,
                        max_sockets * HTTP_CONNECTION_RESERVATION_BYTES,
                    )
                    .expect("budgets"),
                    ..HttpDirectTransportConfig::default()
                },
                ..HttpPolicyClientConfig::default()
            },
        )
    }

    fn task(
        root: &TestDirectory,
        sources: impl IntoIterator<Item = SocketAddr>,
        total_length: usize,
    ) -> HttpTaskSpec {
        task_with_retry(root, sources, total_length, None)
    }

    fn task_with_retry(
        root: &TestDirectory,
        sources: impl IntoIterator<Item = SocketAddr>,
        total_length: usize,
        retry: Option<HttpRetryPolicy>,
    ) -> HttpTaskSpec {
        task_with_identity(
            root,
            sources,
            total_length,
            retry,
            HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
        )
    }

    fn endgame_task(root: &TestDirectory, source: SocketAddr, total_length: usize) -> HttpTaskSpec {
        assert!(total_length >= MIB);
        let options = HttpTaskOptions {
            split: NonZeroUsize::new(1).expect("split"),
            max_connections_per_server: NonZeroUsize::new(2).expect("per server"),
            min_split_size: MIB as u64,
            piece_length: MIB as u64,
            connect_timeout: Duration::from_secs(5),
            response_head_timeout: Duration::from_secs(5),
            response_body_timeout: Duration::from_secs(1),
            max_download_limit: 0,
            lowest_speed_limit: 0,
            endgame_max_duplicates: 2,
            mirror_identity: HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
            checksum: None,
            retry: None,
        };
        HttpTaskSpec::new(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            [format!("http://{source}/file")],
            root.0.clone(),
            SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
                .expect("safe output"),
            options,
            false,
        )
        .expect("endgame task")
    }

    fn strict_cross_mirror_endgame_task(
        root: &TestDirectory,
        sources: [SocketAddr; 2],
        total_length: usize,
    ) -> HttpTaskSpec {
        assert!(total_length >= MIB);
        let options = HttpTaskOptions {
            split: NonZeroUsize::new(1).expect("split"),
            max_connections_per_server: NonZeroUsize::new(1).expect("per server"),
            min_split_size: MIB as u64,
            piece_length: MIB as u64,
            connect_timeout: Duration::from_secs(5),
            response_head_timeout: Duration::from_secs(5),
            response_body_timeout: Duration::from_secs(1),
            max_download_limit: 0,
            lowest_speed_limit: 0,
            endgame_max_duplicates: 1,
            mirror_identity: HttpMirrorIdentityPolicy::RequireSharedDigest,
            checksum: None,
            retry: None,
        };
        HttpTaskSpec::new(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            sources
                .into_iter()
                .map(|source| format!("http://{source}/file")),
            root.0.clone(),
            SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
                .expect("safe output"),
            options,
            false,
        )
        .expect("strict cross-mirror endgame task")
    }

    async fn serve_endgame_mirror(
        responses: Vec<Arc<[u8]>>,
        dirty: bool,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);
        let ordinal = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(if dirty { 2 } else { 1 }));
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let recorded = Arc::clone(&recorded);
                let ordinal = Arc::clone(&ordinal);
                let responses = responses.clone();
                let barrier = Arc::clone(&barrier);
                handlers.spawn(async move {
                    let request = read_request_head(&mut stream).await;
                    let (start, end) = request_range(&request).expect("range request");
                    recorded
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push((start, end));
                    let request_index = ordinal.fetch_add(1, Ordering::AcqRel);
                    let probe = start == 0 && end == 0;
                    let response_index = request_index.saturating_sub(1);
                    let body = if probe {
                        &responses[0][start..=end]
                    } else {
                        let index = response_index.min(responses.len().saturating_sub(1));
                        &responses[index][start..=end]
                    };
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"endgame\"\r\nConnection: close\r\n\r\n",
                        body.len(),
                        responses[0].len()
                    );
                    stream.write_all(head.as_bytes()).await.expect("head");
                    if probe {
                        stream.write_all(body).await.expect("probe body");
                    } else if !dirty && response_index > 0 {
                        let mut closed = [0_u8; 1];
                        let _closed = stream.read(&mut closed).await;
                    } else {
                        if dirty && response_index < 2 {
                            barrier.wait().await;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        stream.write_all(body).await.expect("range body");
                    }
                });
            }
        });
        (address, ranges, task)
    }

    async fn serve_digest_endgame_mirror(
        data: Arc<[u8]>,
        range_delay: Duration,
    ) -> (
        SocketAddr,
        Arc<Mutex<Vec<(usize, usize)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let recorded = Arc::clone(&recorded);
                let data = Arc::clone(&data);
                handlers.spawn(async move {
                    let request = read_request_head(&mut stream).await;
                    let (start, end) = request_range(&request).expect("range request");
                    recorded
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push((start, end));
                    let body = &data[start..=end];
                    let digest = format!(
                        "sha-256=:{}:",
                        base64_encode(&Sha256::digest(body))
                    );
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"endgame\"\r\nRepr-Digest: {digest}\r\nConnection: close\r\n\r\n",
                        body.len(),
                        data.len()
                    );
                    if stream.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    if start != 0 || end != 0 {
                        tokio::time::sleep(range_delay).await;
                    }
                    let _written = stream.write_all(body).await;
                });
            }
        });
        (address, ranges, task)
    }

    fn task_with_identity(
        root: &TestDirectory,
        sources: impl IntoIterator<Item = SocketAddr>,
        total_length: usize,
        retry: Option<HttpRetryPolicy>,
        mirror_identity: HttpMirrorIdentityPolicy,
    ) -> HttpTaskSpec {
        task_with_identity_and_checksum(root, sources, total_length, retry, mirror_identity, None)
    }

    fn task_with_identity_and_checksum(
        root: &TestDirectory,
        sources: impl IntoIterator<Item = SocketAddr>,
        total_length: usize,
        retry: Option<HttpRetryPolicy>,
        mirror_identity: HttpMirrorIdentityPolicy,
        checksum: Option<HttpContentChecksum>,
    ) -> HttpTaskSpec {
        let options = HttpTaskOptions {
            split: NonZeroUsize::new(2).expect("split"),
            max_connections_per_server: NonZeroUsize::new(1).expect("per server"),
            min_split_size: MIB as u64,
            piece_length: MIB as u64,
            connect_timeout: Duration::from_secs(5),
            response_head_timeout: Duration::from_secs(5),
            response_body_timeout: Duration::from_secs(5),
            max_download_limit: 0,
            lowest_speed_limit: 0,
            endgame_max_duplicates: 0,
            mirror_identity,
            checksum,
            retry,
        };
        assert!(total_length >= MIB);
        HttpTaskSpec::new(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            sources
                .into_iter()
                .map(|address| format!("http://{address}/file")),
            root.0.clone(),
            SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
                .expect("safe output"),
            options,
            false,
        )
        .expect("task spec")
    }

    fn single_stream_task_with_retry(
        root: &TestDirectory,
        source: SocketAddr,
        total_length: usize,
        retry: HttpRetryPolicy,
    ) -> HttpTaskSpec {
        assert!(total_length >= MIB);
        let options = HttpTaskOptions {
            split: NonZeroUsize::new(1).expect("split"),
            max_connections_per_server: NonZeroUsize::new(1).expect("per server"),
            min_split_size: MIB as u64,
            piece_length: MIB as u64,
            connect_timeout: Duration::from_secs(5),
            response_head_timeout: Duration::from_secs(5),
            response_body_timeout: Duration::from_secs(5),
            max_download_limit: 0,
            lowest_speed_limit: 0,
            endgame_max_duplicates: 0,
            mirror_identity: HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
            checksum: None,
            retry: Some(retry),
        };
        HttpTaskSpec::new(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            [format!("http://{source}/file")],
            root.0.clone(),
            SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
                .expect("safe output"),
            options,
            false,
        )
        .expect("task spec")
    }

    fn append_representation_generation(
        journal: &TestDirectory,
        task: &HttpTaskSpec,
        generation: Generation,
    ) {
        let directory = http_journal_directory(&journal.0, task.gid());
        let capability = JournalDirectoryCapability::open_trusted(&directory)
            .expect("journal directory capability");
        let paths = ControlJournalAppender::discover_segment_paths(
            &capability,
            ReplayLimits::default().max_segments,
        )
        .expect("journal paths");
        let (mut appender, replay) = ControlJournalAppender::open_recovered(
            &directory,
            &paths,
            task.gid(),
            derive_http_journal_id(task.task(), task.gid()),
            ReplayLimits::default(),
            generation,
            now_unix_ms().unwrap_or(0),
        )
        .expect("open journal for generation advance");
        assert_eq!(replay.stop, ariax_storage::ReplayStop::CleanEnd);
        let previous = Generation::new(generation.get().saturating_sub(1));
        let options = task.persistence_options().expect("persistence options");
        let snapshot_hash = options.snapshot_hash();
        let snapshot = appender
            .append_payload(
                previous,
                &JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::NextAdmission,
                    patch_id: None,
                    snapshot_hash,
                    options,
                },
            )
            .expect("append next options");
        appender
            .flush(snapshot.sequence())
            .expect("flush next options");
        let started = appender
            .append_payload(
                generation,
                &JournalPayload::GenerationStarted {
                    previous_generation: previous,
                    reason: GenerationStartReason::RepresentationRestart,
                    next_snapshot_hash: snapshot_hash,
                    patch_id: None,
                },
            )
            .expect("append generation start");
        appender
            .flush(started.sequence())
            .expect("flush generation start");
        appender.close_flushed().expect("close advanced journal");
    }

    fn replay_payloads(
        journal: &TestDirectory,
        task: &HttpTaskSpec,
        generation: Generation,
    ) -> Vec<JournalPayload> {
        let directory = http_journal_directory(&journal.0, task.gid());
        let capability = JournalDirectoryCapability::open_trusted(&directory)
            .expect("journal directory capability");
        let paths = ControlJournalAppender::discover_segment_paths(
            &capability,
            ReplayLimits::default().max_segments,
        )
        .expect("journal paths");
        let (mut appender, framing) = ControlJournalAppender::open_recovered(
            &directory,
            &paths,
            task.gid(),
            derive_http_journal_id(task.task(), task.gid()),
            ReplayLimits::default(),
            generation,
            now_unix_ms().unwrap_or(0),
        )
        .expect("open journal for payload replay");
        assert_eq!(framing.stop, ariax_storage::ReplayStop::CleanEnd);
        let payloads = framing
            .records
            .iter()
            .map(|record| record.decode_payload().expect("decode journal payload"))
            .collect();
        appender.close_flushed().expect("close replay journal");
        payloads
    }

    fn checksum(data: &[u8]) -> HttpContentChecksum {
        HttpContentChecksum::sha256(Sha256::digest(data).into())
    }

    fn worker(
        journal: &TestDirectory,
        stats: SharedHttpTransferStats,
        max_sockets: usize,
    ) -> HttpMultiRangeWorker {
        HttpMultiRangeWorker::new(
            policy_client(max_sockets),
            HttpMultiRangeWorkerConfig {
                journal_root: journal.0.clone(),
                storage: StorageEngineConfig::default(),
                retry: HttpRetryPolicy::default(),
                event_capacity: NonZeroUsize::new(16).expect("events"),
                ..HttpMultiRangeWorkerConfig::default()
            },
            stats,
        )
        .expect("worker")
    }

    async fn interrupt_after_first_durable_piece(
        worker: HttpMultiRangeWorker,
        spec: &HttpTaskSpec,
        stats: &SharedHttpTransferStats,
    ) {
        let cancellation = HttpCancellation::new();
        let worker_cancellation = cancellation.clone();
        let worker_spec = Arc::new(spec.clone());
        let running = tokio::spawn(async move {
            worker
                .run_task(worker_spec, Generation::INITIAL, worker_cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if stats
                    .get(spec.task())
                    .is_some_and(|stats| stats.snapshot().durable_bytes == MIB as u64)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first piece became durable");
        cancellation.cancel();
        assert!(matches!(
            running.await.expect("worker join"),
            Err(HttpMultiRangeError::Cancelled)
        ));
    }

    #[test]
    fn packet_independent_stats_publish_speed_and_decay_to_zero() {
        let stats = HttpTransferStats::default();
        let started = ariax_core::MonotonicInstant::now();
        stats.reset_sampling_at(started);
        stats.add_raw(1_000);
        stats.add_accepted(1_000);
        stats.add_durable(1_000);

        let first = stats.snapshot_at(
            started
                .checked_add(Duration::from_secs(1))
                .expect("sample instant"),
        );
        assert_eq!(first.current_speed, 1_000);
        assert_eq!(first.durable_speed, 1_000);

        let idle = stats.snapshot_at(
            started
                .checked_add(Duration::from_secs(2))
                .expect("idle sample instant"),
        );
        assert_eq!(idle.current_speed, 0);
        assert_eq!(idle.durable_speed, 0);
    }

    #[test]
    fn stats_keep_rate_and_backpressure_conditions_outside_task_state() {
        let stats = HttpTransferStats::default();
        let started = ariax_core::MonotonicInstant::now();
        stats.reset_sampling_at(started);
        stats.set_diagnostic(StatsDiagnostic {
            condition: ConnectionCondition::RateLimited,
            reason: Some(ConnectionConditionReason::IngressRateLimit),
        });
        stats.set_rate_debt(17);
        let snapshot = stats.snapshot_at(
            started
                .checked_add(Duration::from_secs(1))
                .expect("sample instant"),
        );
        assert_eq!(
            snapshot.connection_condition,
            ConnectionCondition::RateLimited
        );
        assert_eq!(
            snapshot.condition_reason,
            Some(ConnectionConditionReason::IngressRateLimit)
        );
        assert_eq!(snapshot.rate_debt_bytes, 17);
    }

    #[tokio::test]
    async fn storage_backpressure_withholds_the_next_read_slot_until_cancellation() {
        let stats = HttpTransferStats::default();
        let cancellation = HttpCancellation::new();
        let rate = RateArbiter::new(RateDirection::Download, RateArbiterConfig::default())
            .expect("rate arbiter");
        let ingress = HttpIngressBudgets::new(DEFAULT_HTTP_INGRESS_BUDGET_BYTES);
        let discard_budget =
            HttpDiscardBudget::new(HttpDiscardBudgetLimits::default()).expect("discard budget");
        let task_guard = discard_budget
            .begin_task(
                TaskId::new(1).expect("task"),
                HttpDiscardScopeLimits {
                    host_bytes: DEFAULT_HTTP_DISCARD_HOST_BYTES,
                    task_bytes: DEFAULT_HTTP_DISCARD_TASK_BYTES,
                    attempt_bytes: DEFAULT_HTTP_DISCARD_ATTEMPT_BYTES,
                },
            )
            .expect("task discard guard");
        let attempt = task_guard
            .begin_attempt("https://backpressure.test")
            .expect("attempt discard guard");
        let (events, mut received) = mpsc::channel(4);
        let worker = acquire_read_slot(
            &events,
            LeaseId::new(1).expect("lease"),
            DEFAULT_HTTP_INGRESS_FRAME_BYTES,
            &rate,
            RatePath {
                host: 1,
                task: 1,
                stream: 1,
            },
            &ingress,
            &attempt,
            &cancellation,
            &stats,
        );
        tokio::pin!(worker);

        let Some(AttemptEvent::PrepareRead { response, .. }) = (tokio::select! {
            event = received.recv() => event,
            result = &mut worker => panic!("read-slot worker stopped early: {result:?}"),
        }) else {
            panic!("read-slot request was not published");
        };
        response.send(None).expect("reject read-slot admission");
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(5)) => {}
            result = &mut worker => panic!("read-slot worker stopped while backpressured: {result:?}"),
        }
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot.connection_condition,
            ConnectionCondition::Backpressured
        );
        assert_eq!(
            snapshot.condition_reason,
            Some(ConnectionConditionReason::BufferBackpressure)
        );

        cancellation.cancel();
        assert!(matches!(worker.await, Err(RangeAttemptFailure::Cancelled)));
    }

    #[test]
    fn discard_charge_updates_stats_and_stops_at_the_attempt_cap() {
        let stats = HttpTransferStats::default();
        stats.begin();
        let budget = HttpDiscardBudget::new(HttpDiscardBudgetLimits {
            process_bytes: 16,
            host_bytes: 16,
            task_bytes: 16,
            attempt_bytes: 4,
        })
        .expect("limits");
        let task = budget
            .begin_task(
                TaskId::new(1).expect("task"),
                crate::HttpDiscardScopeLimits {
                    host_bytes: 16,
                    task_bytes: 16,
                    attempt_bytes: 4,
                },
            )
            .expect("task guard");
        let attempt = task.begin_attempt("https://discard.test").expect("attempt");
        record_discarded(&attempt, &stats, 3).expect("first charge");
        let error = record_discarded(&attempt, &stats, 3).expect_err("cap");
        assert_eq!(
            error.code(),
            "http_discard_budget_exhausted",
            "the caller must stop polling after a partial overrun charge"
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.discarded_bytes, 6);
        assert_eq!(snapshot.discard_budget_consumed, 4);
        assert_eq!(snapshot.discard_budget_remaining, 12);
    }

    #[test]
    fn lowest_speed_check_uses_elapsed_useful_bytes() {
        assert!(below_lowest_speed(100, Duration::from_secs(2), 100,));
        assert!(!below_lowest_speed(200, Duration::from_secs(2), 100,));
        assert!(!below_lowest_speed(0, Duration::ZERO, 1));
    }

    #[test]
    fn retry_causes_keep_timeout_hang_and_lowest_speed_separate() {
        assert_eq!(
            retry_cause(&RangeAttemptFailure::Timeout),
            HttpRetryCause::Transport(HttpRetryTransportFailure::Timeout)
        );
        assert_eq!(
            retry_cause(&RangeAttemptFailure::Hang),
            HttpRetryCause::Transport(HttpRetryTransportFailure::Hang)
        );
        assert_eq!(
            retry_cause(&RangeAttemptFailure::LowestSpeed),
            HttpRetryCause::Transport(HttpRetryTransportFailure::LowestSpeed)
        );
        for error in [
            HttpRangeResponseError::RepresentationDigestChanged,
            HttpRangeResponseError::RepresentationDigestMismatch,
        ] {
            assert_eq!(
                retry_cause(&RangeAttemptFailure::Response {
                    error,
                    retry_after: None,
                }),
                HttpRetryCause::Checksum
            );
        }
    }

    #[test]
    fn retry_scope_ids_round_trip_without_zero_or_truncation() {
        let piece = PieceId::new(17);
        let source = UriId::new(9);
        let piece_scope = piece_retry_scope_id(piece).expect("piece scope");
        let span_scope = span_retry_scope_id(piece, source).expect("span scope");
        assert_eq!(decode_piece_retry_scope_id(piece_scope), Some(piece));
        assert_eq!(
            decode_span_retry_scope_id(span_scope),
            Some((piece, source))
        );
        assert_eq!(
            decode_span_retry_scope_id(PersistedId::new(1).unwrap()),
            None
        );
        assert_eq!(piece_retry_scope_id(PieceId::new(u64::MAX)), None);
        assert_eq!(
            span_retry_scope_id(PieceId::new(u64::from(u32::MAX)), UriId::new(0)),
            None
        );
        assert_eq!(
            span_retry_scope_id(PieceId::new(0), UriId::new(u32::MAX)),
            None
        );
    }

    #[test]
    fn recovered_range_retry_restores_remaining_wait_and_elapsed_budget() {
        let piece = PieceId::new(2);
        let source = UriId::new(3);
        let common = RecoveredRetryState {
            scope: RetryScope::Piece,
            scope_id: piece_retry_scope_id(piece).expect("piece scope"),
            attempt: 2,
            elapsed_before_wait_ms: 700,
            scheduled_at_unix_ms: 1_000,
            delay_ms: 5_000,
            error_class: ErrorKind::Network,
            retry_reason: RetryReason::Backoff,
        };
        let states = vec![
            common.clone(),
            RecoveredRetryState {
                scope: RetryScope::Span,
                scope_id: span_retry_scope_id(piece, source).expect("span scope"),
                ..common.clone()
            },
            RecoveredRetryState {
                scope: RetryScope::Task,
                scope_id: PersistedId::new(1).expect("task scope"),
                delay_ms: 0,
                ..common.clone()
            },
        ];
        let recovered = recover_range_retries_at(
            &states,
            &HttpRetryPolicy::default(),
            3_000,
            MonotonicInstant::now(),
        )
        .expect("retry state recovers");
        assert_eq!(recovered.elapsed_ms, 2_700);
        let retry = recovered.pieces.get(&piece).expect("piece retry");
        assert_eq!(retry.attempts, 2);
        assert_eq!(retry.attempts_by_mirror.get(&source), Some(&2));
        assert_eq!(retry.retry_at_by_mirror.get(&source), Some(&3_000));
        assert_eq!(
            recovered.latest_diagnostic(&HttpRetryPolicy::default(), 3_000),
            Some(HttpRetryDiagnosticSnapshot {
                cause: HttpRetryDiagnosticCause::Recovered(ErrorKind::Network),
                source,
                piece,
                prior_lease: None,
                next_lease: None,
                total_attempt: 2,
                total_remaining: 3,
                source_attempt: 2,
                source_remaining: 1,
                scheduled_at_unix_ms: 1_000,
                delay_ms: 5_000,
                retry_at_unix_ms: 6_000,
                delay: Some(HttpRetryDelayDiagnostic::Recovered(RetryReason::Backoff)),
                stop_reason: None,
                next_action: HttpRetryNextAction::RetryRange,
                lease_disposition: HttpRetryLeaseDisposition::UnknownRecovered,
            })
        );

        assert!(matches!(
            recover_range_retries_at(
                &states[..1],
                &HttpRetryPolicy::default(),
                3_000,
                MonotonicInstant::now(),
            ),
            Err(HttpMultiRangeError::Retry(
                HttpRetryError::InvalidRecoveredState
            ))
        ));
    }

    #[test]
    fn retry_diagnostic_binds_the_actual_next_source_and_lease_once() {
        let stats = HttpTransferStats::default();
        let piece = PieceId::new(4);
        let original = UriId::new(1);
        stats.set_retry_diagnostic(HttpRetryDiagnosticSnapshot {
            cause: HttpRetryDiagnosticCause::Live(HttpRetryCause::Transport(
                HttpRetryTransportFailure::Timeout,
            )),
            source: original,
            piece,
            prior_lease: LeaseId::new(7),
            next_lease: None,
            total_attempt: 1,
            total_remaining: 4,
            source_attempt: 1,
            source_remaining: 2,
            scheduled_at_unix_ms: 1_000,
            delay_ms: 100,
            retry_at_unix_ms: 1_100,
            delay: Some(HttpRetryDelayDiagnostic::Live(
                HttpRetryDelaySource::FixedBackoff,
            )),
            stop_reason: None,
            next_action: HttpRetryNextAction::RetryRange,
            lease_disposition: HttpRetryLeaseDisposition::Aborted,
        });

        stats.bind_retry_lease(piece, UriId::new(2), LeaseId::new(8).expect("lease"));
        stats.bind_retry_lease(piece, original, LeaseId::new(9).expect("later lease"));

        let diagnostic = stats.snapshot().retry_diagnostic.expect("retry diagnostic");
        assert_eq!(diagnostic.next_lease, LeaseId::new(8));
        assert_eq!(diagnostic.next_action, HttpRetryNextAction::DifferentSource);

        stats.begin();
        assert!(stats.snapshot().retry_diagnostic.is_none());
    }

    #[test]
    fn retry_restore_ignores_stale_waits_for_durable_pieces() {
        let policy = HttpRetryPolicy::default();
        let source = UriId::new(0);
        let mut coordinator = HttpRangeCoordinator::new(
            HttpRangeCoordinatorConfig {
                total_length: (2 * MIB) as u64,
                piece_length: MIB as u64,
                split: NonZeroUsize::new(1).expect("split"),
                max_connections_per_origin: NonZeroUsize::new(1).expect("origin cap"),
                max_total_attempts: policy.max_attempts.get(),
                max_attempts_per_source: policy.max_attempts_per_mirror.get(),
                endgame_max_duplicates: 0,
            },
            [HttpRangeSource::from_uri(source, "http://one.example/file").expect("source")],
        )
        .expect("coordinator");
        let durable = PieceId::new(0);
        let pending = PieceId::new(1);
        coordinator
            .restore_durable([durable])
            .expect("durable piece restores");
        let retries = BTreeMap::from([
            (
                durable,
                RecoveredPieceRetry {
                    attempts: 1,
                    attempts_by_mirror: BTreeMap::from([(source, 1)]),
                    retry_at_by_mirror: BTreeMap::from([(source, 999)]),
                    diagnostics_by_mirror: BTreeMap::new(),
                },
            ),
            (
                pending,
                RecoveredPieceRetry {
                    attempts: 1,
                    attempts_by_mirror: BTreeMap::from([(source, 1)]),
                    retry_at_by_mirror: BTreeMap::from([(source, 100)]),
                    diagnostics_by_mirror: BTreeMap::new(),
                },
            ),
        ]);
        let mut budgets = restore_range_retry_budgets(
            &mut coordinator,
            retries,
            &BTreeSet::from([durable]),
            &policy,
        )
        .expect("retry budgets restore");
        assert!(!budgets.contains_key(&durable));
        assert_eq!(
            coordinator.poll(99).expect("wait poll"),
            HttpRangePoll::RetryAt(100)
        );
        let HttpRangePoll::Assignment(assignment) = coordinator.poll(100).expect("retry poll")
        else {
            panic!("pending retry becomes assignable");
        };
        assert_eq!(assignment.piece, pending);
        assert_eq!(
            budgets
                .get_mut(&pending)
                .expect("pending budget")
                .begin_attempt(source)
                .expect("next attempt"),
            2
        );
    }

    #[tokio::test]
    async fn stale_validator_fail_stops_without_releasing_an_unvalidated_range() {
        let root = TestDirectory::new("stale-fail-root");
        let journal = TestDirectory::new("stale-fail-journal");
        let expected = data(MIB);
        let (mirror, ranges, server) =
            serve_validator_sequence(Arc::clone(&expected), vec!["\"v1\"", "\"v2\""]).await;
        let retry = HttpRetryPolicy {
            stale_validator_policy: HttpStaleValidatorPolicy::Fail,
            ..HttpRetryPolicy::default()
        };
        let spec = single_stream_task_with_retry(&root, mirror, expected.len(), retry);
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(1).expect("stats"));

        assert!(matches!(
            worker(&journal, stats.clone(), 1)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::StaleValidator)
        ));
        server.await.expect("server");
        assert_eq!(
            *ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![(0, 0), (0, MIB - 1)]
        );
        assert_eq!(stats.get(spec.task()).unwrap().snapshot().durable_bytes, 0);
    }

    #[tokio::test]
    async fn stale_validator_revalidate_requires_the_original_identity_then_retries() {
        let root = TestDirectory::new("stale-revalidate-root");
        let journal = TestDirectory::new("stale-revalidate-journal");
        let expected = data(MIB);
        let (mirror, ranges, server) = serve_validator_sequence(
            Arc::clone(&expected),
            vec!["\"v1\"", "\"v2\"", "\"v1\"", "\"v1\""],
        )
        .await;
        let retry = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(3).expect("attempts"),
            max_attempts_per_mirror: NonZeroU32::new(3).expect("mirror attempts"),
            stale_validator_policy: HttpStaleValidatorPolicy::Revalidate,
            ..HttpRetryPolicy::default()
        };
        let spec = single_stream_task_with_retry(&root, mirror, expected.len(), retry);
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(1).expect("stats"));

        worker(&journal, stats.clone(), 1)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("revalidated transfer");
        server.await.expect("server");
        assert_eq!(
            *ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![(0, 0), (0, MIB - 1), (0, 0), (0, MIB - 1)]
        );
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        assert_eq!(stats.get(spec.task()).unwrap().snapshot().retry_count, 1);
    }

    #[tokio::test]
    async fn restart_if_safe_discards_old_progress_and_rewrites_the_whole_entity() {
        let root = TestDirectory::new("representation-restart-root");
        let journal = TestDirectory::new("representation-restart-journal");
        let old: Arc<[u8]> = vec![0x11; 2 * MIB].into();
        let replacement: Arc<[u8]> = vec![0x77; 2 * MIB].into();
        let (mirror, ranges, server) =
            serve_representation_restart_mirror(Arc::clone(&old), Arc::clone(&replacement)).await;
        let retry = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(3).expect("attempts"),
            max_attempts_per_mirror: NonZeroU32::new(3).expect("mirror attempts"),
            stale_validator_policy: HttpStaleValidatorPolicy::RestartIfSafe,
            ..HttpRetryPolicy::default()
        };
        let spec = single_stream_task_with_retry(&root, mirror, old.len(), retry);
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(1).expect("stats"));

        assert!(matches!(
            worker(&journal, stats.clone(), 1)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::RepresentationRestart)
        ));
        let partial = fs::read(root.0.join("output.bin")).expect("partial output");
        assert_eq!(&partial[..MIB], &old[..MIB]);
        assert_ne!(partial, replacement.as_ref());

        let next = Generation::new(1);
        append_representation_generation(&journal, &spec, next);
        worker(&journal, stats, 1)
            .run_task(Arc::new(spec.clone()), next, HttpCancellation::new())
            .await
            .expect("replacement generation");
        server.await.expect("server");

        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("replacement output"),
            replacement.as_ref()
        );
        assert_eq!(
            *ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![
                (0, 0),
                (0, MIB - 1),
                (MIB, 2 * MIB - 1),
                (0, 0),
                (0, MIB - 1),
                (MIB, 2 * MIB - 1),
            ]
        );
        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: next,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover replacement generation");
        assert_eq!(recovered.durable_prefix, (2 * MIB) as u64);
    }

    #[test]
    fn representation_restart_public_class_is_policy_and_generation_bounded() {
        let policy = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(2).expect("attempts"),
            stale_validator_policy: HttpStaleValidatorPolicy::RestartIfSafe,
            ..HttpRetryPolicy::default()
        };
        let first =
            HttpMultiRangeError::RepresentationRestart.into_public(&policy, Generation::INITIAL);
        assert_eq!(first.kind(), ErrorKind::StaleValidator);
        assert_eq!(first.retry_class(), RetryClass::RestartGeneration);

        let exhausted =
            HttpMultiRangeError::RepresentationRestart.into_public(&policy, Generation::new(1));
        assert_eq!(exhausted.retry_class(), RetryClass::Never);

        let fail = HttpRetryPolicy {
            stale_validator_policy: HttpStaleValidatorPolicy::Fail,
            ..policy
        };
        let terminal = HttpMultiRangeError::Setup(KnownLengthHttpError::StaleValidator)
            .into_public(&fail, Generation::INITIAL);
        assert_eq!(terminal.retry_class(), RetryClass::Never);
    }

    #[tokio::test]
    async fn two_mirrors_commit_non_overlapping_pieces_and_recover_durable_total() {
        let root = TestDirectory::new("parallel-root");
        let journal = TestDirectory::new("parallel-journal");
        let expected = data(2 * MIB);
        let (first, first_server) = serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let (second, second_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let spec = task(&root, [first, second], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats"));
        worker(&journal, stats.clone(), 4)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("parallel transfer");
        first_server.await.expect("first server");
        second_server.await.expect("second server");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        let snapshot = stats.get(spec.task()).expect("stats").snapshot();
        assert_eq!(snapshot.total_length, (2 * MIB) as u64);
        assert_eq!(snapshot.durable_bytes, (2 * MIB) as u64);
        assert_eq!(snapshot.active_connections, 0);

        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover multi-range journal");
        assert_eq!(recovered.durable_prefix, (2 * MIB) as u64);
    }

    #[tokio::test]
    async fn strict_identity_without_shared_digest_uses_one_origin_and_persists_validator() {
        let root = TestDirectory::new("strict-root");
        let journal = TestDirectory::new("strict-journal");
        let expected = data(MIB);
        let (primary, primary_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let (secondary, secondary_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 1).await;
        let spec = task_with_identity(
            &root,
            [primary, secondary],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        worker(&journal, stats, 2)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("strict single-origin transfer");
        primary_server.await.expect("primary server");
        secondary_server.await.expect("secondary probe server");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover strict journal");
        assert!(recovered.strong_validator.is_some());
        assert_eq!(recovered.durable_prefix, MIB as u64);
    }

    #[tokio::test]
    async fn strict_identity_with_shared_repr_digest_keeps_secondary_for_range_endgame() {
        let root = TestDirectory::new("strict-repr-digest-root");
        let journal = TestDirectory::new("strict-repr-digest-journal");
        let expected = data(2 * MIB);
        let (first, first_server) = serve_mirror(
            Arc::clone(&expected),
            MirrorMode::SharedRepresentationDigest,
            3,
        )
        .await;
        let (second, second_server) = serve_mirror(
            Arc::clone(&expected),
            MirrorMode::SharedRepresentationDigest,
            1,
        )
        .await;
        let spec = task_with_identity(
            &root,
            [first, second],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats"));
        worker(&journal, stats, 4)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("strict digest transfer");
        first_server.await.expect("first server");
        second_server.await.expect("second server");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        let committed_digests = replay_payloads(&journal, &spec, Generation::INITIAL)
            .into_iter()
            .filter_map(|payload| match payload {
                JournalPayload::LeaseCommitted {
                    response_digest, ..
                } => Some(response_digest),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(committed_digests.len(), 2);
        assert!(committed_digests.into_iter().all(|digest| {
            digest.is_some_and(|digest| digest.algorithm() == JournalDigestAlgorithm::Sha256)
        }));
        let range_identities = replay_payloads(&journal, &spec, Generation::INITIAL)
            .into_iter()
            .filter_map(|payload| match payload {
                JournalPayload::HttpRangeIdentity {
                    total_length,
                    representation_digest,
                    ..
                } => Some((total_length, representation_digest)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(range_identities.len(), 1);
        assert_eq!(range_identities[0].0, expected.len() as u64);
        assert_eq!(
            range_identities[0].1.algorithm(),
            JournalDigestAlgorithm::Sha256
        );
        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover strict representation-digest journal");
        assert!(recovered.strong_validator.is_none());
        assert!(matches!(
            recovered
                .replay
                .state
                .as_ref()
                .and_then(RecoveredJournalState::terminal),
            Some(ariax_storage::RecoveredTerminal::Complete {
                final_digest: None,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn digest_only_restart_revalidates_durable_ranges_before_releasing_pending_work() {
        let root = TestDirectory::new("digest-range-restart-root");
        let journal = TestDirectory::new("digest-range-restart-journal");
        let expected = data(2 * MIB);
        let hold_second_piece = Arc::new(AtomicBool::new(true));
        let (first, first_ranges, _first_data, first_server) = serve_recovery_shared_digest_mirror(
            Arc::clone(&expected),
            Arc::clone(&hold_second_piece),
        )
        .await;
        let (second, second_ranges, _second_data, second_server) =
            serve_recovery_shared_digest_mirror(Arc::clone(&expected), hold_second_piece).await;
        let spec = task_with_identity(
            &root,
            [first, second],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats"));
        interrupt_after_first_durable_piece(worker(&journal, stats.clone(), 4), &spec, &stats)
            .await;

        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover digest-only interrupted journal");
        assert!(recovered.strong_validator.is_none());
        assert_eq!(recovered.durable_prefix, MIB as u64);
        assert!(
            recovered
                .replay
                .state
                .as_ref()
                .and_then(RecoveredJournalState::http_range_identity)
                .is_some(),
            "the flushed range identity must survive the interrupted process"
        );
        let durable_requests_before_restart = [&first_ranges, &second_ranges]
            .into_iter()
            .map(|ranges| {
                ranges
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|range| **range == (0, MIB - 1))
                    .count()
            })
            .sum::<usize>();

        worker(&journal, stats, 4)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("digest-only restart completes");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        let durable_requests_after_restart = [&first_ranges, &second_ranges]
            .into_iter()
            .map(|ranges| {
                ranges
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|range| **range == (0, MIB - 1))
                    .count()
            })
            .sum::<usize>();
        assert_eq!(
            durable_requests_after_restart,
            durable_requests_before_restart + 1,
            "restart must read back the durable range from one matching mirror exactly once"
        );
        assert_eq!(
            replay_payloads(&journal, &spec, Generation::INITIAL)
                .into_iter()
                .filter(|payload| matches!(payload, JournalPayload::HttpRangeIdentity { .. }))
                .count(),
            1,
            "recovery must not append a duplicate range identity"
        );
        first_server.abort();
        second_server.abort();
        assert!(
            first_server
                .await
                .expect_err("first server was cancelled")
                .is_cancelled()
        );
        assert!(
            second_server
                .await
                .expect_err("second server was cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn digest_only_restart_rejects_changed_durable_range_before_pending_work() {
        let root = TestDirectory::new("digest-range-restart-mismatch-root");
        let journal = TestDirectory::new("digest-range-restart-mismatch-journal");
        let expected = data(2 * MIB);
        let hold_second_piece = Arc::new(AtomicBool::new(true));
        let (first, first_ranges, first_data, first_server) = serve_recovery_shared_digest_mirror(
            Arc::clone(&expected),
            Arc::clone(&hold_second_piece),
        )
        .await;
        let (second, second_ranges, second_data, second_server) =
            serve_recovery_shared_digest_mirror(Arc::clone(&expected), hold_second_piece).await;
        let spec = task_with_identity(
            &root,
            [first, second],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats"));
        interrupt_after_first_durable_piece(worker(&journal, stats.clone(), 4), &spec, &stats)
            .await;

        let pending_span = (MIB, 2 * MIB - 1);
        let pending_requests_before_restart = [&first_ranges, &second_ranges]
            .into_iter()
            .map(|ranges| {
                ranges
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|range| **range == pending_span)
                    .count()
            })
            .sum::<usize>();
        let mut changed = expected.as_ref().to_vec();
        changed[1] ^= 0xff;
        let changed: Arc<[u8]> = changed.into();
        for mirror_data in [&first_data, &second_data] {
            *mirror_data
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::clone(&changed);
        }

        assert!(matches!(
            worker(&journal, stats, 4)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::ChecksumMismatch)
        ));
        let pending_requests_after_restart = [&first_ranges, &second_ranges]
            .into_iter()
            .map(|ranges| {
                ranges
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|range| **range == pending_span)
                    .count()
            })
            .sum::<usize>();
        assert_eq!(
            pending_requests_after_restart, pending_requests_before_restart,
            "changed durable-range evidence must fail before pending work is released"
        );
        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("reopen journal after digest-only recovery rejection");
        assert_eq!(recovered.durable_prefix, MIB as u64);
        assert_eq!(
            &fs::read(root.0.join("output.bin")).expect("durable output")[..MIB],
            &expected[..MIB],
            "network revalidation must not overwrite the durable local range"
        );

        first_server.abort();
        second_server.abort();
        assert!(
            first_server
                .await
                .expect_err("first server was cancelled")
                .is_cancelled()
        );
        assert!(
            second_server
                .await
                .expect_err("second server was cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn strict_identity_with_different_repr_digests_falls_back_to_one_mirror() {
        let root = TestDirectory::new("strict-repr-mismatch-root");
        let journal = TestDirectory::new("strict-repr-mismatch-journal");
        let expected = data(2 * MIB);
        let divergent = (0..expected.len())
            .map(|index| u8::try_from((index + 1) % 251).expect("bounded byte"))
            .collect::<Vec<_>>()
            .into();
        let (first, first_server) = serve_mirror(
            Arc::clone(&expected),
            MirrorMode::SharedRepresentationDigest,
            3,
        )
        .await;
        let (second, second_server) =
            serve_mirror(divergent, MirrorMode::SharedRepresentationDigest, 3).await;
        let spec = task_with_identity(
            &root,
            [first, second],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
        );
        worker(
            &journal,
            SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats")),
            4,
        )
        .run_task(Arc::new(spec), Generation::INITIAL, HttpCancellation::new())
        .await
        .expect("strict fallback transfer");
        first_server.abort();
        second_server.abort();
        let _ = first_server.await;
        let _ = second_server.await;
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
    }

    #[tokio::test]
    async fn strict_identity_rejects_a_shared_but_incorrect_repr_digest() {
        let root = TestDirectory::new("strict-repr-invalid-root");
        let journal = TestDirectory::new("strict-repr-invalid-journal");
        let expected = data(2 * MIB);
        let (first, first_server) = serve_mirror(
            Arc::clone(&expected),
            MirrorMode::IncorrectRepresentationDigest,
            1,
        )
        .await;
        let (second, second_server) = serve_mirror(
            Arc::clone(&expected),
            MirrorMode::IncorrectRepresentationDigest,
            1,
        )
        .await;
        let retry = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(1).expect("one attempt"),
            max_attempts_per_mirror: NonZeroU32::new(1).expect("one mirror attempt"),
            ..HttpRetryPolicy::default()
        };
        let spec = task_with_identity(
            &root,
            [first, second],
            expected.len(),
            Some(retry),
            HttpMirrorIdentityPolicy::RequireSharedDigest,
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats"));
        assert!(matches!(
            worker(&journal, stats.clone(), 4)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::Response(
                HttpRangeResponseError::RepresentationDigestMismatch,
            ))
        ));
        first_server.await.expect("first server");
        second_server.await.expect("second server");
        // Probe rejection happens before the length-dependent storage layout is
        // admitted, so recovery has no layout to reopen.  The observable
        // invariant is that the failed admission leaves no output or durable
        // progress behind.
        assert!(!root.0.join("output.bin").exists());
        assert_eq!(stats.get(spec.task()).unwrap().snapshot().durable_bytes, 0);
    }

    #[tokio::test]
    async fn strict_range_digest_mismatch_aborts_without_durable_progress() {
        let root = TestDirectory::new("strict-range-digest-invalid-root");
        let journal = TestDirectory::new("strict-range-digest-invalid-journal");
        let expected = data(MIB);
        let (mirror, server) = serve_mirror(
            Arc::clone(&expected),
            MirrorMode::IncorrectRangeRepresentationDigest,
            2,
        )
        .await;
        let retry = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(1).expect("one attempt"),
            max_attempts_per_mirror: NonZeroU32::new(1).expect("one mirror attempt"),
            ..HttpRetryPolicy::default()
        };
        let spec = task_with_identity(
            &root,
            [mirror],
            expected.len(),
            Some(retry),
            HttpMirrorIdentityPolicy::RequireSharedDigest,
        );
        assert!(matches!(
            worker(
                &journal,
                SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats")),
                2,
            )
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await,
            Err(HttpMultiRangeError::Exhausted)
        ));
        server.await.expect("server");
        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover range digest mismatch journal");
        assert_eq!(recovered.durable_prefix, 0);
        assert!(
            recovered
                .replay
                .state
                .as_ref()
                .and_then(RecoveredJournalState::terminal)
                .is_none()
        );
    }

    #[tokio::test]
    async fn strict_identity_with_user_checksum_uses_all_mirrors_and_persists_final_digest() {
        let root = TestDirectory::new("strict-checksum-root");
        let journal = TestDirectory::new("strict-checksum-journal");
        let expected = data(2 * MIB);
        let expected_checksum = checksum(expected.as_ref());
        let (first, first_server) = serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let (second, second_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let spec = task_with_identity_and_checksum(
            &root,
            [first, second],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
            Some(expected_checksum),
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats"));
        worker(&journal, stats, 4)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("strict digest transfer");
        first_server.await.expect("first server");
        second_server.await.expect("second server");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );

        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover strict digest journal");
        assert!(recovered.strong_validator.is_none());
        let terminal = recovered
            .replay
            .state
            .as_ref()
            .and_then(RecoveredJournalState::terminal)
            .expect("terminal evidence");
        assert!(matches!(
            terminal,
            ariax_storage::RecoveredTerminal::Complete {
                final_digest: Some(digest),
                ..
            } if digest == &expected_checksum.journal_digest()
        ));
    }

    #[tokio::test]
    async fn strict_checksum_mismatch_never_publishes_terminal_completion() {
        let root = TestDirectory::new("strict-checksum-mismatch-root");
        let journal = TestDirectory::new("strict-checksum-mismatch-journal");
        let expected = data(2 * MIB);
        let divergent: Arc<[u8]> = expected
            .iter()
            .map(|byte| byte ^ 0xff)
            .collect::<Vec<_>>()
            .into();
        let expected_checksum = checksum(expected.as_ref());
        let (first, first_server) = serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let (second, second_server) = serve_mirror(divergent, MirrorMode::Valid, 2).await;
        let spec = task_with_identity_and_checksum(
            &root,
            [first, second],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
            Some(expected_checksum),
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).expect("stats"));
        assert!(matches!(
            worker(&journal, stats.clone(), 4)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::ChecksumMismatch)
        ));
        first_server.await.expect("first server");
        second_server.await.expect("second server");
        assert!(
            matches!(
                worker(&journal, stats, 4)
                    .run_task(
                        Arc::new(spec.clone()),
                        Generation::INITIAL,
                        HttpCancellation::new(),
                    )
                    .await,
                Err(HttpMultiRangeError::ChecksumMismatch)
            ),
            "a fully durable digest-bound task must reverify locally without reconnecting"
        );

        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover checksum mismatch journal");
        assert_eq!(recovered.durable_prefix, (2 * MIB) as u64);
        assert!(
            recovered
                .replay
                .state
                .as_ref()
                .and_then(RecoveredJournalState::terminal)
                .is_none(),
            "a checksum mismatch must not append TaskComplete"
        );
    }

    #[tokio::test]
    async fn checksum_bound_restart_accepts_weak_validator_and_verifies_whole_file() {
        let root = TestDirectory::new("checksum-resume-root");
        let journal = TestDirectory::new("checksum-resume-journal");
        let expected = data(2 * MIB);
        let expected_checksum = checksum(expected.as_ref());
        let (mirror, ranges, _etag, server) =
            serve_recovery_validator_mirror(Arc::clone(&expected), "W/\"v1\"").await;
        let spec = task_with_identity_and_checksum(
            &root,
            [mirror],
            expected.len(),
            None,
            HttpMirrorIdentityPolicy::RequireSharedDigest,
            Some(expected_checksum),
        );
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        interrupt_after_first_durable_piece(worker(&journal, stats.clone(), 2), &spec, &stats)
            .await;
        let interrupted = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover interrupted digest task");
        assert!(interrupted.strong_validator.is_none());

        worker(&journal, stats, 2)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("digest-bound restart");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        let piece_zero_requests = ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|range| **range == (0, MIB - 1))
            .count();
        assert_eq!(
            piece_zero_requests, 1,
            "durable piece must not be fetched again"
        );
        server.abort();
        assert!(
            server
                .await
                .expect_err("server was cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn invalid_range_response_never_opens_or_commits_a_piece() {
        let root = TestDirectory::new("invalid-root");
        let journal = TestDirectory::new("invalid-journal");
        let expected = data(MIB);
        let (mirror, server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::IgnoreRange, 2).await;
        let spec = task(&root, [mirror], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        assert!(matches!(
            worker(&journal, stats.clone(), 2)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::Exhausted)
        ));
        server.await.expect("server");
        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover rejected range");
        assert_eq!(recovered.durable_prefix, 0);
        assert_eq!(stats.get(spec.task()).unwrap().snapshot().durable_bytes, 0);
    }

    #[tokio::test]
    async fn oversized_body_is_cut_off_charged_and_disables_the_source() {
        let root = TestDirectory::new("oversized-root");
        let journal = TestDirectory::new("oversized-journal");
        let expected = data(MIB);
        let (mirror, server) = serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let source_uri = format!("http://{mirror}/file");
        let _fault = arm_oversized_range_body_fault(source_uri);
        let spec = task(&root, [mirror], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        let worker = worker(&journal, stats.clone(), 2);

        assert!(matches!(
            worker
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::Exhausted)
        ));
        server.await.expect("server");

        let snapshot = stats.get(spec.task()).expect("stats").snapshot();
        assert_eq!(snapshot.accepted_bytes, 0);
        assert_eq!(snapshot.provisional_bytes, 0);
        assert_eq!(snapshot.durable_bytes, 0);
        assert_eq!(snapshot.retry_count, 0);
        assert!(snapshot.discarded_bytes >= (MIB + 1) as u64);
        let diagnostic = snapshot.retry_diagnostic.expect("oversized diagnostic");
        assert!(matches!(
            diagnostic.cause,
            HttpRetryDiagnosticCause::Live(HttpRetryCause::InvalidRange)
        ));
        assert_eq!(diagnostic.next_action, HttpRetryNextAction::DisableSource);
        assert_eq!(
            diagnostic.stop_reason,
            Some(HttpRetryStopReason::NonRetriable)
        );
        assert_eq!(
            diagnostic.lease_disposition,
            HttpRetryLeaseDisposition::Aborted
        );

        let payloads = replay_payloads(&journal, &spec, Generation::INITIAL);
        let started = payloads
            .iter()
            .filter_map(|payload| match payload {
                JournalPayload::LeaseStarted { lease_id, .. } => Some(*lease_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let committed = payloads
            .iter()
            .filter_map(|payload| match payload {
                JournalPayload::LeaseCommitted { lease_id, .. } => Some(*lease_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let aborted = payloads
            .iter()
            .filter_map(|payload| match payload {
                JournalPayload::LeaseAborted {
                    lease_id, reason, ..
                } => Some((*lease_id, *reason)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(started.len(), 1);
        assert!(committed.is_empty());
        assert_eq!(aborted, [(started[0], LeaseAbortReason::OversizedBody)]);
        assert!(!payloads.iter().any(|payload| matches!(
            payload,
            JournalPayload::PieceDurable { .. } | JournalPayload::TaskComplete { .. }
        )));
    }

    #[tokio::test]
    async fn storage_rejection_aborts_the_opened_lease_once_with_storage_reason() {
        let root = TestDirectory::new("storage-rejection-root");
        let journal = TestDirectory::new("storage-rejection-journal");
        let expected = data(MIB);
        let (mirror, server) = serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let spec = task(&root, [mirror], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        let worker = HttpMultiRangeWorker::new(
            policy_client(2),
            HttpMultiRangeWorkerConfig {
                journal_root: journal.0.clone(),
                storage: StorageEngineConfig {
                    disk_fault: Some(StorageEngineDiskFault::OutOfSpace),
                    ..StorageEngineConfig::default()
                },
                event_capacity: NonZeroUsize::new(16).expect("events"),
                ..HttpMultiRangeWorkerConfig::default()
            },
            stats.clone(),
        )
        .expect("worker");

        let error = worker
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect_err("storage rejection stops the range worker");
        assert!(matches!(
            error,
            HttpMultiRangeError::Storage(ref storage)
                if storage.reject() == WriteReject::DiskCompletion
        ));
        server.await.expect("server");

        let payloads = replay_payloads(&journal, &spec, Generation::INITIAL);
        let started = payloads
            .iter()
            .filter_map(|payload| match payload {
                JournalPayload::LeaseStarted { lease_id, .. } => Some(*lease_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let committed = payloads
            .iter()
            .filter_map(|payload| match payload {
                JournalPayload::LeaseCommitted { lease_id, .. } => Some(*lease_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let aborted = payloads
            .iter()
            .filter_map(|payload| match payload {
                JournalPayload::LeaseAborted {
                    lease_id, reason, ..
                } => Some((*lease_id, *reason)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(started.len(), 1);
        assert!(committed.is_empty());
        assert_eq!(aborted, [(started[0], LeaseAbortReason::StorageRejected)]);
        assert!(!payloads.iter().any(|payload| matches!(
            payload,
            JournalPayload::PieceDurable { .. } | JournalPayload::TaskComplete { .. }
        )));
        assert_eq!(stats.get(spec.task()).unwrap().snapshot().durable_bytes, 0);
    }

    #[tokio::test]
    async fn short_first_mirror_releases_whole_piece_for_second_mirror_retry() {
        let root = TestDirectory::new("retry-root");
        let journal = TestDirectory::new("retry-journal");
        let expected = data(MIB);
        let (first, first_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::ShortRange, 2).await;
        let (second, second_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 2).await;
        let spec = task(&root, [first, second], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        worker(&journal, stats.clone(), 3)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("retry succeeds");
        first_server.await.expect("first server");
        second_server.await.expect("second server");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        assert_eq!(stats.get(spec.task()).unwrap().snapshot().retry_count, 1);
    }

    #[tokio::test]
    async fn short_body_exhausts_attempt_discard_budget_before_another_retry() {
        let root = TestDirectory::new("discard-cap-root");
        let journal = TestDirectory::new("discard-cap-journal");
        let expected = data(MIB);
        let (mirror, server) = serve_mirror(Arc::clone(&expected), MirrorMode::ShortRange, 2).await;
        let spec = task(&root, [mirror], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        let discard_budget = HttpDiscardBudget::new(HttpDiscardBudgetLimits {
            process_bytes: MIB as u64,
            host_bytes: MIB as u64,
            task_bytes: MIB as u64,
            attempt_bytes: 128 * 1024,
        })
        .expect("discard budget");
        let worker = HttpMultiRangeWorker::new(
            policy_client(2),
            HttpMultiRangeWorkerConfig {
                journal_root: journal.0.clone(),
                storage: StorageEngineConfig::default(),
                discard_budget,
                event_capacity: NonZeroUsize::new(16).expect("events"),
                ..HttpMultiRangeWorkerConfig::default()
            },
            stats.clone(),
        )
        .expect("worker");
        let error = worker
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect_err("discard cap must stop the retry cycle");
        assert!(matches!(
            error,
            HttpMultiRangeError::DiscardBudgetExhausted(HttpDiscardScope::Attempt)
        ));
        server.await.expect("server");
        let snapshot = stats.get(spec.task()).expect("stats").snapshot();
        assert_eq!(snapshot.retry_count, 0);
        assert!(snapshot.discarded_bytes >= (MIB / 2) as u64);
        assert_eq!(snapshot.discard_budget_consumed, 128 * 1024 + 1);
        assert_eq!(snapshot.durable_bytes, 0);
    }

    #[tokio::test]
    async fn short_body_exhausts_process_discard_budget_before_another_retry() {
        let root = TestDirectory::new("discard-process-cap-root");
        let journal = TestDirectory::new("discard-process-cap-journal");
        let expected = data(MIB);
        let (mirror, server) = serve_mirror(Arc::clone(&expected), MirrorMode::ShortRange, 2).await;
        let spec = task(&root, [mirror], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        let discard_budget = HttpDiscardBudget::new(HttpDiscardBudgetLimits {
            process_bytes: (128 * 1024) as u64,
            host_bytes: MIB as u64,
            task_bytes: MIB as u64,
            attempt_bytes: MIB as u64,
        })
        .expect("discard budget");
        let worker = HttpMultiRangeWorker::new(
            policy_client(2),
            HttpMultiRangeWorkerConfig {
                journal_root: journal.0.clone(),
                storage: StorageEngineConfig::default(),
                discard_budget,
                event_capacity: NonZeroUsize::new(16).expect("events"),
                ..HttpMultiRangeWorkerConfig::default()
            },
            stats.clone(),
        )
        .expect("worker");
        let error = worker
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect_err("process discard cap must stop the retry cycle");
        assert!(matches!(
            error,
            HttpMultiRangeError::DiscardBudgetExhausted(HttpDiscardScope::Process)
        ));
        server.await.expect("server");
        let snapshot = stats.get(spec.task()).expect("stats").snapshot();
        assert_eq!(snapshot.retry_count, 0);
        assert_eq!(snapshot.discard_budget_consumed, 128 * 1024);
        assert_eq!(
            snapshot.discard_budget_remaining,
            MIB as u64 - (128 * 1024) as u64
        );
        assert!(snapshot.discarded_bytes >= (128 * 1024) as u64);
        assert_eq!(snapshot.durable_bytes, 0);
    }

    #[tokio::test]
    async fn task_retry_policy_overrides_the_worker_attempt_cap() {
        let root = TestDirectory::new("task-retry-cap-root");
        let journal = TestDirectory::new("task-retry-cap-journal");
        let expected = data(MIB);
        let (first, first_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::ShortRange, 2).await;
        let (second, second_server) =
            serve_mirror(Arc::clone(&expected), MirrorMode::Valid, 1).await;
        let retry = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(1).expect("one attempt"),
            max_attempts_per_mirror: NonZeroU32::new(1).expect("one mirror attempt"),
            ..HttpRetryPolicy::default()
        };
        let spec = task_with_retry(&root, [first, second], expected.len(), Some(retry));
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        assert!(matches!(
            worker(&journal, stats.clone(), 3)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::Exhausted)
        ));
        first_server.await.expect("first server");
        second_server.await.expect("second probe");
        let snapshot = stats.get(spec.task()).unwrap().snapshot();
        assert_eq!(snapshot.retry_count, 0);
        let diagnostic = snapshot
            .retry_diagnostic
            .expect("terminal retry diagnostic");
        assert!(matches!(
            diagnostic.cause,
            HttpRetryDiagnosticCause::Live(HttpRetryCause::Transport(
                HttpRetryTransportFailure::UnexpectedEof | HttpRetryTransportFailure::Hang
            ))
        ));
        assert_eq!(diagnostic.total_attempt, 1);
        assert_eq!(diagnostic.total_remaining, 0);
        assert_eq!(
            diagnostic.stop_reason,
            Some(HttpRetryStopReason::TotalAttemptCap)
        );
        assert_eq!(diagnostic.next_action, HttpRetryNextAction::TerminalFailure);
        assert_eq!(diagnostic.delay, None);
        assert_eq!(
            diagnostic.lease_disposition,
            HttpRetryLeaseDisposition::Aborted
        );
    }

    #[tokio::test]
    async fn restart_keeps_a_persisted_retry_wait_from_releasing_the_span_early() {
        let root = TestDirectory::new("retry-wait-restart-root");
        let journal = TestDirectory::new("retry-wait-restart-journal");
        let expected = data(MIB);
        let (mirror, requests, server) = serve_retry_wait_mirror(Arc::clone(&expected)).await;
        let retry = HttpRetryPolicy {
            max_wait: Duration::from_secs(5),
            retry_after_max: Duration::from_secs(5),
            backoff: HttpRetryBackoff::Fixed,
            ..HttpRetryPolicy::default()
        };
        let spec = task_with_retry(&root, [mirror], expected.len(), Some(retry));
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        let first_worker = worker(&journal, stats.clone(), 2);
        let first_cancellation = HttpCancellation::new();
        let cancellation = first_cancellation.clone();
        let first_spec = Arc::new(spec.clone());
        let first = tokio::spawn(async move {
            first_worker
                .run_task(first_spec, Generation::INITIAL, cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if stats
                    .get(spec.task())
                    .is_some_and(|stats| stats.snapshot().retry_count == 1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retry wait was durably selected");
        let live_diagnostic = stats
            .get(spec.task())
            .expect("live retry stats")
            .snapshot()
            .retry_diagnostic
            .expect("live retry diagnostic");
        assert_eq!(
            live_diagnostic.cause,
            HttpRetryDiagnosticCause::Live(HttpRetryCause::HttpStatus(503))
        );
        assert_eq!(live_diagnostic.source, UriId::new(0));
        assert_eq!(live_diagnostic.piece, PieceId::new(0));
        assert_eq!(live_diagnostic.total_attempt, 1);
        assert_eq!(live_diagnostic.total_remaining, 4);
        assert_eq!(live_diagnostic.source_attempt, 1);
        assert_eq!(live_diagnostic.source_remaining, 2);
        assert_eq!(live_diagnostic.delay_ms, 5_000);
        assert_eq!(
            live_diagnostic.delay,
            Some(HttpRetryDelayDiagnostic::Live(
                HttpRetryDelaySource::RetryAfter
            ))
        );
        assert_eq!(live_diagnostic.next_action, HttpRetryNextAction::RetryRange);
        assert_eq!(
            live_diagnostic.lease_disposition,
            HttpRetryLeaseDisposition::Unopened
        );
        assert!(live_diagnostic.prior_lease.is_some());
        assert!(live_diagnostic.next_lease.is_none());
        first_cancellation.cancel();
        assert!(matches!(
            first.await.expect("first worker join"),
            Err(HttpMultiRangeError::Cancelled)
        ));

        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover retry wait journal");
        let retry_states = recovered
            .replay
            .state
            .as_ref()
            .expect("recovered state")
            .retry_states();
        assert_eq!(retry_states.len(), 2);
        assert!(retry_states.values().any(|retry| {
            retry.scope == RetryScope::Piece
                && retry.attempt == 1
                && retry.delay_ms == 5_000
                && retry.retry_reason == RetryReason::RetryAfter
        }));
        assert!(retry_states.values().any(|retry| {
            retry.scope == RetryScope::Span
                && retry.attempt == 1
                && retry.delay_ms == 5_000
                && retry.retry_reason == RetryReason::RetryAfter
        }));

        let second_worker = worker(&journal, stats.clone(), 2);
        let second_cancellation = HttpCancellation::new();
        let cancellation = second_cancellation.clone();
        let second_spec = Arc::new(spec.clone());
        let second = tokio::spawn(async move {
            second_worker
                .run_task(second_spec, Generation::INITIAL, cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if requests
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len()
                    >= 3
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("restart probe completed");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            stats
                .get(spec.task())
                .expect("restart stats")
                .snapshot()
                .retry_count,
            1,
            "recovered retry accounting remains visible"
        );
        let recovered_diagnostic = stats
            .get(spec.task())
            .expect("restart stats")
            .snapshot()
            .retry_diagnostic
            .expect("recovered retry diagnostic");
        assert_eq!(
            recovered_diagnostic.cause,
            HttpRetryDiagnosticCause::Recovered(ErrorKind::Network)
        );
        assert_eq!(
            recovered_diagnostic.delay,
            Some(HttpRetryDelayDiagnostic::Recovered(RetryReason::RetryAfter))
        );
        assert_eq!(
            recovered_diagnostic.lease_disposition,
            HttpRetryLeaseDisposition::UnknownRecovered
        );
        assert!(recovered_diagnostic.prior_lease.is_none());
        assert!(recovered_diagnostic.next_lease.is_none());
        let range_attempts = requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(_, end, _)| *end != 0)
            .count();
        assert_eq!(
            range_attempts, 1,
            "the recovered span wait must remain armed"
        );
        second_cancellation.cancel();
        assert!(matches!(
            second.await.expect("second worker join"),
            Err(HttpMultiRangeError::Cancelled)
        ));
        server.abort();
        assert!(
            server
                .await
                .expect_err("server was cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn restart_rejects_corrupt_durable_piece_before_releasing_pending_range() {
        let root = TestDirectory::new("corrupt-resume-root");
        let journal = TestDirectory::new("corrupt-resume-journal");
        let expected = data(2 * MIB);
        let (mirror, ranges, _etag, server) =
            serve_recovery_validator_mirror(Arc::clone(&expected), "\"v1\"").await;
        let spec = task(&root, [mirror], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        interrupt_after_first_durable_piece(worker(&journal, stats.clone(), 2), &spec, &stats)
            .await;

        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover interrupted journal");
        assert!(recovered.strong_validator.is_some());
        let mut output = fs::OpenOptions::new()
            .write(true)
            .open(root.0.join("output.bin"))
            .expect("open output for corruption");
        output.seek(SeekFrom::Start(0)).expect("seek output");
        output.write_all(&[255]).expect("corrupt output");
        output.sync_all().expect("sync corruption");
        let requests_before_restart = ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        let pending_attempts_before_restart = ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|range| **range == (MIB, 2 * MIB - 1))
            .count();

        assert!(matches!(
            worker(&journal, stats.clone(), 2)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await,
            Err(HttpMultiRangeError::Setup(
                KnownLengthHttpError::DurablePieceDigestMismatch { piece }
            )) if piece == PieceId::new(0)
        ));
        let (requests_after_restart, pending_attempts_after_restart) = {
            let ranges = ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                ranges.len(),
                ranges
                    .iter()
                    .filter(|range| **range == (MIB, 2 * MIB - 1))
                    .count(),
            )
        };
        assert_eq!(
            requests_after_restart, requests_before_restart,
            "descriptor-bound readback must reject corruption before probing"
        );
        assert_eq!(
            pending_attempts_after_restart, pending_attempts_before_restart,
            "corrupt recovery must not release the pending span"
        );
        server.abort();
        assert!(
            server
                .await
                .expect_err("server was cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn restart_rejects_changed_or_weak_validator_before_releasing_pending_range() {
        for (label, initial_etag, replacement, expected_code) in [
            ("changed", "\"v1\"", Some("\"v2\""), "stale_validator"),
            ("weak", "W/\"v1\"", None, "missing_strong_validator"),
        ] {
            let root = TestDirectory::new(&format!("{label}-validator-root"));
            let journal = TestDirectory::new(&format!("{label}-validator-journal"));
            let expected = data(2 * MIB);
            let (mirror, ranges, etag, server) =
                serve_recovery_validator_mirror(Arc::clone(&expected), initial_etag).await;
            let spec = task(&root, [mirror], expected.len());
            let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
            interrupt_after_first_durable_piece(worker(&journal, stats.clone(), 2), &spec, &stats)
                .await;
            if let Some(replacement) = replacement {
                *etag
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = replacement.to_owned();
            }
            let pending_attempts_before_restart = ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|range| **range == (MIB, 2 * MIB - 1))
                .count();

            let error = worker(&journal, stats.clone(), 2)
                .run_task(
                    Arc::new(spec.clone()),
                    Generation::INITIAL,
                    HttpCancellation::new(),
                )
                .await
                .expect_err("unsafe resume is rejected");
            assert_eq!(error.code(), expected_code);
            let pending_attempts_after_restart = ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|range| **range == (MIB, 2 * MIB - 1))
                .count();
            assert_eq!(
                pending_attempts_after_restart, pending_attempts_before_restart,
                "validator rejection must not release the pending span"
            );
            server.abort();
            assert!(
                server
                    .await
                    .expect_err("server was cancelled")
                    .is_cancelled()
            );
        }
    }

    #[tokio::test]
    async fn restart_restores_durable_piece_without_requesting_it_again() {
        let root = TestDirectory::new("restart-root");
        let journal = TestDirectory::new("restart-journal");
        let expected = data(2 * MIB);
        let (mirror, ranges, server) = serve_restart_mirror(Arc::clone(&expected)).await;
        let spec = task(&root, [mirror], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        let first_worker = worker(&journal, stats.clone(), 2);
        let cancellation = HttpCancellation::new();
        let first_cancellation = cancellation.clone();
        let first_spec = Arc::new(spec.clone());
        let first = tokio::spawn(async move {
            first_worker
                .run_task(first_spec, Generation::INITIAL, first_cancellation)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if stats
                    .get(spec.task())
                    .is_some_and(|stats| stats.snapshot().durable_bytes == MIB as u64)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first piece became durable");
        cancellation.cancel();
        assert!(matches!(
            first.await.expect("first worker join"),
            Err(HttpMultiRangeError::Cancelled)
        ));

        let recovered = recover_known_length_http(&KnownLengthHttpRecoveryRequest {
            task: spec.task(),
            gid: spec.gid(),
            journal_id: derive_http_journal_id(spec.task(), spec.gid()),
            generation: Generation::INITIAL,
            journal_directory: http_journal_directory(&journal.0, spec.gid()),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        })
        .expect("recover interrupted journal");
        assert_eq!(recovered.durable_prefix, MIB as u64);

        worker(&journal, stats.clone(), 2)
            .run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            )
            .await
            .expect("restart completes");
        server.await.expect("server");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        let ranges = ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            ranges
                .iter()
                .filter(|range| **range == (0, MIB - 1))
                .count(),
            1,
            "the durable first piece must not be leased again"
        );
        assert_eq!(
            ranges
                .iter()
                .filter(|range| **range == (MIB, 2 * MIB - 1))
                .count(),
            2,
            "the interrupted second piece is retried once after restart"
        );
        assert_eq!(
            stats
                .get(spec.task())
                .expect("stats")
                .snapshot()
                .durable_bytes,
            (2 * MIB) as u64
        );
    }

    #[tokio::test]
    async fn same_source_endgame_clean_fence_commits_after_unopened_or_empty_loser() {
        let root = TestDirectory::new("endgame-clean-root");
        let journal = TestDirectory::new("endgame-clean-journal");
        let expected = data(MIB);
        let (mirror, ranges, server) =
            serve_endgame_mirror(vec![Arc::clone(&expected)], false).await;
        let spec = endgame_task(&root, mirror, expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        tokio::time::timeout(
            Duration::from_secs(5),
            worker(&journal, stats.clone(), 2).run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            ),
        )
        .await
        .expect("endgame worker deadline")
        .expect("clean endgame completes");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        let snapshot = stats.get(spec.task()).expect("stats").snapshot();
        assert_eq!(snapshot.durable_bytes, MIB as u64);
        assert!(
            snapshot.discarded_bytes < MIB as u64,
            "a clean loser may have a discarded network fragment but must not write a full piece"
        );
        assert!(
            ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                >= 2,
            "probe plus the original range must be observed"
        );
        server.abort();
        assert!(server.await.expect_err("server cancelled").is_cancelled());
    }

    #[tokio::test]
    async fn shared_repr_digest_endgame_races_a_different_origin() {
        let root = TestDirectory::new("cross-endgame-root");
        let journal = TestDirectory::new("cross-endgame-journal");
        let expected = data(MIB);
        let (slow, slow_ranges, slow_server) =
            serve_digest_endgame_mirror(Arc::clone(&expected), Duration::from_millis(500)).await;
        let (fast, fast_ranges, fast_server) =
            serve_digest_endgame_mirror(Arc::clone(&expected), Duration::from_millis(10)).await;
        let spec = strict_cross_mirror_endgame_task(&root, [slow, fast], expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        tokio::time::timeout(
            Duration::from_secs(5),
            worker(&journal, stats.clone(), 4).run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            ),
        )
        .await
        .expect("cross-mirror endgame deadline")
        .expect("cross-mirror endgame completes");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        for (label, ranges) in [("slow", slow_ranges), ("fast", fast_ranges)] {
            assert!(
                ranges
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains(&(0, MIB - 1)),
                "{label} origin must receive the same endgame range"
            );
        }
        let snapshot = stats.get(spec.task()).expect("stats").snapshot();
        assert_eq!(snapshot.durable_bytes, MIB as u64);
        slow_server.abort();
        fast_server.abort();
        assert!(
            slow_server
                .await
                .expect_err("slow server cancelled")
                .is_cancelled()
        );
        assert!(
            fast_server
                .await
                .expect_err("fast server cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn cross_mirror_endgame_rejects_a_different_exact_range_digest() {
        let root = TestDirectory::new("cross-endgame-mismatch-root");
        let journal = TestDirectory::new("cross-endgame-mismatch-journal");
        let expected = data(MIB);
        let mut divergent = expected.to_vec();
        for byte in &mut divergent[1..] {
            *byte ^= 0xff;
        }
        let divergent: Arc<[u8]> = divergent.into();
        let (primary, primary_ranges, primary_server) =
            serve_digest_endgame_mirror(Arc::clone(&expected), Duration::from_millis(100)).await;
        let (secondary, secondary_ranges, secondary_server) =
            serve_digest_endgame_mirror(divergent, Duration::from_millis(10)).await;
        let spec = strict_cross_mirror_endgame_task(&root, [primary, secondary], expected.len());
        tokio::time::timeout(
            Duration::from_secs(5),
            worker(
                &journal,
                SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats")),
                4,
            )
            .run_task(Arc::new(spec), Generation::INITIAL, HttpCancellation::new()),
        )
        .await
        .expect("cross-mirror mismatch deadline")
        .expect("primary exact range remains eligible");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref()
        );
        for (label, ranges) in [("primary", primary_ranges), ("secondary", secondary_ranges)] {
            assert!(
                ranges
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains(&(0, MIB - 1)),
                "{label} origin must expose its exact-range digest"
            );
        }
        primary_server.abort();
        secondary_server.abort();
        assert!(
            primary_server
                .await
                .expect_err("primary server cancelled")
                .is_cancelled()
        );
        assert!(
            secondary_server
                .await
                .expect_err("secondary server cancelled")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn same_source_endgame_dirty_overlap_rolls_back_and_overwrites() {
        let root = TestDirectory::new("endgame-dirty-root");
        let journal = TestDirectory::new("endgame-dirty-journal");
        let expected = data(MIB);
        let first = vec![0xA5_u8; MIB].into();
        let second = vec![0x5A_u8; MIB].into();
        let (mirror, ranges, server) =
            serve_endgame_mirror(vec![first, second, Arc::clone(&expected)], true).await;
        let spec = endgame_task(&root, mirror, expected.len());
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).expect("stats"));
        tokio::time::timeout(
            Duration::from_secs(5),
            worker(&journal, stats.clone(), 3).run_task(
                Arc::new(spec.clone()),
                Generation::INITIAL,
                HttpCancellation::new(),
            ),
        )
        .await
        .expect("endgame worker deadline")
        .expect("dirty overlap is retried and completes");
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            expected.as_ref(),
            "the post-rollback ordinary lease must overwrite both provisional bodies"
        );
        let snapshot = stats.get(spec.task()).expect("stats").snapshot();
        assert_eq!(snapshot.durable_bytes, MIB as u64);
        assert!(
            snapshot.discarded_bytes > 0,
            "overlap losers consume discard accounting"
        );
        assert!(
            ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                >= 4,
            "probe, two duplicate attempts, and a replacement lease are required"
        );
        server.abort();
        assert!(server.await.expect_err("server cancelled").is_cancelled());
    }
}
