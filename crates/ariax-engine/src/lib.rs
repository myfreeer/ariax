#![forbid(unsafe_code)]

//! Cross-crate composition for bounded startup reconciliation.

mod effect_sink;
mod http_auth;
mod http_capacity;
mod http_client;
mod http_connector;
mod http_control;
mod http_cookie;
mod http_discard;
mod http_first_slice;
mod http_happy_eyeballs;
mod http_multi;
mod http_proxy;
mod http_proxy_client;
mod http_proxy_io;
mod http_range;
mod http_redirect;
mod http_request;
mod http_resolver;
mod http_response;
mod http_retry;
mod http_rpc;
mod http_supervisor;
mod http_task;
mod http_transport;
mod process_bootstrap;
mod runtime_effects;
mod startup_executor;
mod startup_filesystem;
mod startup_native;
mod storage_engine;

pub use effect_sink::{
    MAX_PERSISTENCE_CATALOG_ENTRIES, MAX_PERSISTENCE_PLAN_STEPS, PersistenceCatalogError,
    PersistenceEffectCatalog, PersistenceEffectPlan, PersistencePlanError, PersistencePlanStep,
    PersistenceSchedulerEffectSink, PersistenceSchedulerPreparation,
    PersistenceSchedulerPrepareError,
};
pub use http_auth::{
    HttpAuthError, HttpAuthPolicy, HttpAuthorization, HttpBasicCredentials, HttpNetrc,
    MAX_HTTP_BASIC_PASSWORD_BYTES, MAX_HTTP_BASIC_USERNAME_BYTES, MAX_HTTP_NETRC_BYTES,
    MAX_HTTP_NETRC_ENTRIES, MAX_HTTP_NETRC_TOKEN_BYTES,
};
pub use http_capacity::{HttpCapacityError, HttpProcessResources};
pub use http_client::{
    DEFAULT_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY, HttpClientRequest, HttpPolicyClient,
    HttpPolicyClientConfig, HttpPolicyClientError, HttpStreamingResponse,
    MAX_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY,
};
pub use http_connector::{
    DEFAULT_HTTP_RESOLUTION_TIMEOUT, HttpAddressClass, HttpDestinationError, HttpDestinationPolicy,
    MAX_HTTP_DESTINATION_ADDRESSES, MAX_HTTP_DESTINATION_HOST_BYTES, ResolvedHttpDestination,
    classify_http_address, resolve_http_destination, resolve_http_destination_with_resolver,
};
pub use http_control::{
    HttpControlBackend, HttpControlError, HttpControlPlane, HttpControlPlaneConfig,
};
pub use http_cookie::{
    DEFAULT_HTTP_COOKIE_TOTAL_ENTRIES, HTTP_PSL_SHA256, HTTP_PSL_SNAPSHOT_ID, HttpCookieError,
    HttpCookieHeader, HttpCookieJar, HttpCookieLimits, MAX_HTTP_COOKIE_BYTES,
    MAX_HTTP_COOKIE_BYTES_PER_DOMAIN, MAX_HTTP_COOKIE_ENTRIES_PER_DOMAIN,
    MAX_HTTP_COOKIE_FILE_BYTES, MAX_HTTP_COOKIE_FILE_LINES, MAX_HTTP_COOKIE_HEADER_BYTES,
    MAX_HTTP_PSL_BYTES, MAX_HTTP_PSL_SNAPSHOT_ID_BYTES,
};
pub use http_discard::{
    DEFAULT_HTTP_DISCARD_ATTEMPT_BYTES, DEFAULT_HTTP_DISCARD_HOST_BYTES,
    DEFAULT_HTTP_DISCARD_PROCESS_BYTES, DEFAULT_HTTP_DISCARD_SCOPE_MULTIPLIER,
    DEFAULT_HTTP_DISCARD_TASK_BYTES, HttpDiscardAttemptGuard, HttpDiscardBudget,
    HttpDiscardBudgetError, HttpDiscardBudgetLimits, HttpDiscardCharge, HttpDiscardScope,
    HttpDiscardScopeLimits, HttpDiscardScopeSnapshot, HttpDiscardTaskGuard,
    MAX_HTTP_DISCARD_HOST_SCOPES, MAX_HTTP_DISCARD_TASK_SCOPES,
};
pub use http_first_slice::{
    HttpCancellation, KnownLengthHttpError, KnownLengthHttpRecovery,
    KnownLengthHttpRecoveryRequest, KnownLengthHttpRequest, KnownLengthHttpResult,
    KnownLengthHttpResumeRequest, KnownLengthHttpRuntimeError, KnownLengthHttpTransfer,
    download_known_length_http, download_known_length_http_blocking,
    download_known_length_http_resolved, download_known_length_http_resolved_blocking,
    recover_known_length_http, resume_known_length_http, resume_known_length_http_blocking,
    resume_known_length_http_resolved, resume_known_length_http_resolved_blocking,
    run_known_length_http_runtime, run_known_length_http_runtime_blocking,
    run_known_length_http_runtime_resolved, run_known_length_http_runtime_resolved_blocking,
};
pub use http_happy_eyeballs::{
    DEFAULT_HTTP_HAPPY_EYEBALLS_DELAY, HttpConnectedPeer, HttpHappyEyeballsConfig,
    HttpHappyEyeballsError, MAX_HTTP_HAPPY_EYEBALLS_ADDRESSES, connect_http_happy_eyeballs,
};
pub use http_multi::{
    DEFAULT_HTTP_DIGEST_WORKERS, DEFAULT_HTTP_INGRESS_BUDGET_BYTES,
    DEFAULT_HTTP_INGRESS_FRAME_BYTES, DEFAULT_HTTP_RANGE_EVENT_CAPACITY, HttpCompletedEvidence,
    HttpIngressBudgets, HttpIngressPermit, HttpMultiRangeError, HttpMultiRangeWorker,
    HttpMultiRangeWorkerConfig, HttpStatsCatalogError, HttpTransferStats,
    HttpTransferStatsSnapshot, MAX_HTTP_DIGEST_WORKERS, MAX_HTTP_RANGE_EVENT_CAPACITY,
    SharedHttpTransferStats, derive_http_journal_id, http_journal_directory,
};
pub use http_proxy::{
    HttpProxyEndpoint, HttpProxyKind, HttpProxyNameResolution, HttpProxyPolicy,
    HttpProxyPolicyError, HttpProxyRoute, HttpSocksTarget, MAX_HTTP_NO_PROXY_RULE_BYTES,
    MAX_HTTP_NO_PROXY_RULES, TrustedProxyEnforcement,
};
pub use http_proxy_client::{
    DEFAULT_HTTP_PROXY_MAX_BODY_BYTES, HttpBufferedResponse, HttpProxyRequestConfig,
    HttpProxyRequestError, execute_http_proxy_request,
};
pub use http_proxy_io::{
    HttpProxyAuthorization, HttpProxyConnectConfig, HttpProxyConnectError, HttpProxyConnection,
    MAX_HTTP_PROXY_RESPONSE_HEAD_BYTES, MAX_SOCKS5_DOMAIN_BYTES, connect_http_proxy_route,
};
pub use http_range::{
    DEFAULT_HTTP_MAX_ATTEMPTS_PER_SOURCE, DEFAULT_HTTP_MAX_TOTAL_ATTEMPTS, HttpOverlapFence,
    HttpOverlapSettlement, HttpRangeAssignment, HttpRangeCoordinator, HttpRangeCoordinatorConfig,
    HttpRangeCoordinatorError, HttpRangeFailure, HttpRangePoll, HttpRangeSource, HttpRangeStats,
    MAX_HTTP_RANGE_PIECES,
};
pub use http_redirect::{
    DEFAULT_HTTP_MAX_REDIRECTS, HttpRedirectContext, HttpRedirectDecision, HttpRedirectError,
    HttpRedirectPolicy, HttpRedirectState, MAX_HTTP_LOCATION_BYTES, MAX_HTTP_REDIRECTS,
};
pub use http_request::{
    HttpCustomHeader, HttpCustomHeaders, HttpPolicyRequest, HttpRequestPolicy,
    HttpRequestPolicyError, MAX_HTTP_CUSTOM_HEADER_NAME_BYTES, MAX_HTTP_CUSTOM_HEADER_VALUE_BYTES,
    MAX_HTTP_CUSTOM_HEADERS, MAX_HTTP_CUSTOM_HEADERS_BYTES, build_http_request,
};
pub use http_resolver::{
    DEFAULT_HTTP_DNS_MAX_ADDRESSES, DEFAULT_HTTP_DNS_MAX_IN_FLIGHT,
    DEFAULT_HTTP_DNS_MAX_NEGATIVE_TTL, DEFAULT_HTTP_DNS_MAX_POSITIVE_TTL,
    DEFAULT_HTTP_DNS_MAX_TOTAL_WAITERS, DEFAULT_HTTP_DNS_MAX_WAITERS_PER_NAME,
    DEFAULT_HTTP_DNS_NEGATIVE_CACHE_CAPACITY, DEFAULT_HTTP_DNS_POSITIVE_CACHE_CAPACITY,
    HttpResolvedHost, HttpResolver, HttpResolverBackend, HttpResolverConfig, HttpResolverError,
    MAX_HTTP_DNS_HOST_BYTES,
};
pub use http_response::{HttpRangeResponseError, HttpRangeResponseValidator};
pub use http_retry::{
    DEFAULT_HTTP_RETRY_BASE_WAIT, DEFAULT_HTTP_RETRY_MAX_ATTEMPTS,
    DEFAULT_HTTP_RETRY_MAX_ATTEMPTS_PER_MIRROR, DEFAULT_HTTP_RETRY_MAX_ELAPSED,
    DEFAULT_HTTP_RETRY_MAX_WAIT, HttpRetryAfterPolicy, HttpRetryBackoff, HttpRetryBudget,
    HttpRetryCause, HttpRetryDecision, HttpRetryDelaySource, HttpRetryError, HttpRetryPolicy,
    HttpRetryProfile, HttpRetryStats, HttpRetryStatusSet, HttpRetryStopReason,
    HttpRetryTransportFailure, HttpRetryTrigger, HttpRetryTriggerSet, HttpStaleValidatorPolicy,
    MAX_HTTP_RETRY_STATUS_CODES, MAX_HTTP_RETRY_STATUS_SPEC_BYTES,
    MAX_HTTP_RETRY_TRIGGER_SPEC_BYTES,
};
pub use http_rpc::{
    DEFAULT_HTTP_RPC_SHUTDOWN_TIMEOUT, HttpRpcBackend, HttpRpcBackendError, HttpRpcTransportError,
    MAX_HTTP_RPC_CONNECTIONS, MAX_HTTP_RPC_HEADER_BYTES, MAX_HTTP_RPC_REQUEST_BYTES,
    MAX_HTTP_RPC_RESPONSE_BYTES, RpcFuture, dispatch_json, run_content_length_stdio,
    serve_loopback_http, serve_loopback_http_listener, serve_loopback_http_listener_until,
    serve_loopback_http_until,
};
pub use http_supervisor::{
    DEFAULT_HTTP_SUPERVISOR_POLL_INTERVAL, DEFAULT_HTTP_SUPERVISOR_SHUTDOWN_TIMEOUT,
    HttpTaskWorker, HttpWorkerFuture, HttpWorkerSuccess, HttpWorkerSupervisor,
    HttpWorkerSupervisorConfig, HttpWorkerSupervisorConfigError, HttpWorkerSupervisorError,
    HttpWorkerSupervisorPoll, MAX_HTTP_SUPERVISOR_ACTIVE_WORKERS,
    MAX_HTTP_SUPERVISOR_PENDING_EVENTS,
};
pub use http_task::{
    DEFAULT_HTTP_ENDGAME_MAX_DUPLICATES, DEFAULT_HTTP_MAX_CONNECTIONS_PER_SERVER,
    DEFAULT_HTTP_MIN_SPLIT_SIZE, DEFAULT_HTTP_PIECE_LENGTH, DEFAULT_HTTP_SPLIT,
    HTTP_SHA256_CHECKSUM_TEXT_BYTES, HTTP_SOURCE_FINGERPRINT_DOMAIN, HttpContentChecksum,
    HttpContentChecksumError, HttpMirrorIdentityPolicy, HttpSourceSpec, HttpTaskCatalog,
    HttpTaskCatalogError, HttpTaskOptions, HttpTaskSpec, HttpTaskSpecError,
    MAX_HTTP_ENDGAME_MAX_DUPLICATES, MAX_HTTP_PIECE_LENGTH, MAX_HTTP_TASK_SOURCES,
    MAX_HTTP_TIMEOUT_SECS, SharedHttpTaskCatalog,
};
pub use http_transport::{
    DEFAULT_HTTP_IDLE_TIMEOUT, DEFAULT_HTTP_MAX_CONNECTIONS_PER_ORIGIN,
    DEFAULT_HTTP_MAX_IDLE_CONNECTIONS_PER_ORIGIN, HTTP_CONNECTION_RESERVATION_BYTES,
    HttpDirectTransport, HttpDirectTransportConfig, HttpMinimumTlsVersion, HttpTlsPolicy,
    HttpTransportBudgets, HttpTransportCapacityPermit, HttpTransportError, HttpTransportStats,
    HttpTrustSource, MAX_HTTP_CONNECTIONS_PER_ORIGIN, MAX_HTTP_IDLE_CONNECTIONS_PER_ORIGIN,
    MAX_HTTP_TLS_BUNDLE_BYTES, MAX_HTTP_TLS_BUNDLE_CERTIFICATES,
};
pub use process_bootstrap::{
    BootstrappedEngine, ProcessBootstrapConfig, ProcessBootstrapError, ProcessBootstrapFailure,
    ProcessSchedulerDriver, ProcessSchedulerPreparation, ProcessSchedulerPrepareError,
    ProcessSchedulerSink, ProcessShutdownError, ProcessShutdownReport, bootstrap_process,
};
pub use runtime_effects::{
    ActiveTransferRequest, AllocationRequest, CancellationRequest, MAX_RUNTIME_EFFECT_CAPACITY,
    NoSpaceProbeRequest, OptionApplicationOutcome, OptionApplicationPlan,
    OptionApplicationPlanError, RuntimeEffectConfig, RuntimeEffectConfigError, RuntimeEffectHandle,
    RuntimeEffectPreparation, RuntimeEffectPrepareError, RuntimeEventRejection,
    RuntimeEventSubmission, RuntimeEventSubmitError, RuntimeSchedulerEffectSink,
    VerifyingTransferRequest,
};
pub use startup_executor::{
    StartupSessionRepairError, StartupSessionRepairExecutor, StartupSessionRepairFinishError,
    StartupSessionRepairPoll,
};
pub use startup_filesystem::{
    NativeFilesystemBackend, NativeFilesystemError, NativeFilesystemPolicy,
};
pub use startup_native::{
    NativeEngineStartup, NativeInstallOutcome, NativeStartupBackend, NativeStartupError,
    NativeStartupExecutor, NativeStartupPoll, NativeStartupResult, complete_native_startup,
};
pub use storage_engine::{
    LeaseCommit, LeaseWritePlan, RetryStateWrite, StorageEngine, StorageEngineConfig,
    StorageEngineError, WriteAck, WriteBlock, WriteReject,
};

use ariax_core::{
    ALL_QUEUE_CLASSES, Aria2Status, CredentialKind, CredentialRequirement, Generation, Gid,
    HostKeyChallenge, MonotonicInstant, NoSpaceCondition, NoSpaceProbeId, NoSpaceProbeOrigin,
    PersistedDelayDecision, PersistedDelayError, PresentedHostKeyChallenge,
    PresentedHostKeyChallengeError, PublicError, QueueClass, QueueOrder, RecoveredDelayDecision,
    RecoveredSchedulerTask, RequestScheduler, RetryClass, SchedulerConfig, SchedulerRestoreBatch,
    SchedulerRestoreError, SchedulerRestorePlan, SlowReadmissionDecision, SlowSlotPersistence,
    TaskConditions, TaskId, TaskState, TransitionEffect, UriId,
};
use ariax_storage::{
    JournalId, JournalInstallIntent, JournalInstallPhase, PlatformPath, RecoveredCheckpoint,
    RecoveredJournalState, RecoveredRetryState, RecoveredTerminal, RetryScope,
    SESSION_MAX_SAFE_URI_BYTES, SESSION_MAX_SOURCES_PER_TASK, SessionHostKeyChallengeRecord,
    SessionJournalCache, SessionNoSpaceCondition, SessionQueueOrder, SessionQueueState,
    SessionQueueTransition, SessionStartupSnapshot, SessionStoppedResultRecord, SessionTaskRecord,
    SessionTaskSourceRecord, SessionTaskSourceSet, SessionTerminalStatus, TaskPauseReason,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;

const REDACTED_NO_SPACE_TARGET: &str = "<persisted output target>";

/// Fixed scheduler and clock bounds applied to one startup batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartupRecoveryConfig {
    pub scheduler: SchedulerConfig,
    pub now_wall_unix_ms: u64,
    pub now_monotonic: MonotonicInstant,
    pub max_retry_wait_ms: NonZeroU64,
    pub max_slow_wait_ms: NonZeroU64,
    pub max_no_space_wait_ms: NonZeroU64,
    pub max_retry_elapsed_ms: u64,
}

/// One journal whose structural and semantic replay already completed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredTaskJournal {
    pub gid: Gid,
    pub journal_id: JournalId,
    pub state: RecoveredJournalState,
}

/// One complete caller-derived credential admission decision for a task.
///
/// The caller must derive this value from the restored redacted source and
/// option set. The pure reconciler intentionally cannot infer that absence of
/// a record means absence of a credential requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedCredentialAdmission {
    pub gid: Gid,
    pub requirement: Option<CredentialRequirement>,
}

/// Why persisted source metadata cannot produce one exact admission decision
/// per startup task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialDerivationError {
    AllocationFailed,
    TooManyTasks,
    TooManySourceSets,
    DuplicateTask(Gid),
    DuplicateSourceSet(Gid),
    MissingSourceSet(Gid),
    ExtraSourceSet(Gid),
    TooManySources(Gid),
    DuplicateSourceId(Gid),
    NonCanonicalSourceOrder(Gid),
    UnsafeSourceUri(Gid),
    UnmarkedRedactedSource(Gid),
}

impl fmt::Display for CredentialDerivationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllocationFailed => {
                formatter.write_str("credential derivation allocation failed")
            }
            Self::TooManyTasks => formatter.write_str("too many startup tasks"),
            Self::TooManySourceSets => formatter.write_str("too many startup source sets"),
            Self::DuplicateTask(gid) => write!(formatter, "duplicate startup task {gid}"),
            Self::DuplicateSourceSet(gid) => {
                write!(formatter, "duplicate startup source set for {gid}")
            }
            Self::MissingSourceSet(gid) => {
                write!(formatter, "startup task {gid} has no source set")
            }
            Self::ExtraSourceSet(gid) => {
                write!(formatter, "startup source set {gid} has no task")
            }
            Self::TooManySources(gid) => {
                write!(formatter, "startup task {gid} has too many sources")
            }
            Self::DuplicateSourceId(gid) => {
                write!(formatter, "startup task {gid} has duplicate source ids")
            }
            Self::NonCanonicalSourceOrder(gid) => {
                write!(
                    formatter,
                    "startup task {gid} sources are not canonically ordered"
                )
            }
            Self::UnsafeSourceUri(gid) => {
                write!(
                    formatter,
                    "startup task {gid} has an oversized persisted source URI"
                )
            }
            Self::UnmarkedRedactedSource(gid) => write!(
                formatter,
                "startup task {gid} has a redacted source without a credential blocker"
            ),
        }
    }
}

impl Error for CredentialDerivationError {}

/// Derives the ordinary plaintext-free startup credential admissions from the
/// session owner's exact bounded source sets.
pub fn derive_credential_admissions(
    snapshot: &SessionStartupSnapshot,
) -> Result<Vec<DerivedCredentialAdmission>, CredentialDerivationError> {
    if snapshot.tasks.len() > ariax_storage::SESSION_MAX_TASKS {
        return Err(CredentialDerivationError::TooManyTasks);
    }
    if snapshot.task_sources.len() > ariax_storage::SESSION_MAX_TASKS {
        return Err(CredentialDerivationError::TooManySourceSets);
    }
    let mut tasks = BTreeMap::new();
    for task in &snapshot.tasks {
        if tasks.insert(task.gid, task.queue_state).is_some() {
            return Err(CredentialDerivationError::DuplicateTask(task.gid));
        }
    }
    let mut source_sets = BTreeMap::new();
    for source_set in &snapshot.task_sources {
        if source_sets.insert(source_set.gid, source_set).is_some() {
            return Err(CredentialDerivationError::DuplicateSourceSet(
                source_set.gid,
            ));
        }
    }
    if let Some(gid) = source_sets
        .keys()
        .copied()
        .find(|gid| !tasks.contains_key(gid))
    {
        return Err(CredentialDerivationError::ExtraSourceSet(gid));
    }

    let mut output = Vec::new();
    output
        .try_reserve_exact(tasks.len())
        .map_err(|_| CredentialDerivationError::AllocationFailed)?;
    for (gid, queue_state) in tasks {
        let source_set = source_sets
            .remove(&gid)
            .ok_or(CredentialDerivationError::MissingSourceSet(gid))?;
        validate_source_set(source_set)?;
        let requirement = if queue_state == SessionQueueState::Stopped
            || source_set.sources.iter().any(is_runnable_source)
        {
            None
        } else {
            source_set
                .sources
                .iter()
                .find(|source| source.needs_credentials)
                .map(credential_requirement)
        };
        output.push(DerivedCredentialAdmission { gid, requirement });
    }
    debug_assert!(source_sets.is_empty());
    Ok(output)
}

fn validate_source_set(source_set: &SessionTaskSourceSet) -> Result<(), CredentialDerivationError> {
    if source_set.sources.len() > SESSION_MAX_SOURCES_PER_TASK {
        return Err(CredentialDerivationError::TooManySources(source_set.gid));
    }
    let mut source_ids = BTreeSet::new();
    let mut previous = None;
    for source in &source_set.sources {
        if !source_ids.insert(source.uri_id) {
            return Err(CredentialDerivationError::DuplicateSourceId(source_set.gid));
        }
        let order = (source.priority, source.uri_id);
        if previous.is_some_and(|previous| previous >= order) {
            return Err(CredentialDerivationError::NonCanonicalSourceOrder(
                source_set.gid,
            ));
        }
        previous = Some(order);
        if source
            .persistence_safe_uri
            .as_ref()
            .is_some_and(|uri| uri.len() > SESSION_MAX_SAFE_URI_BYTES)
        {
            return Err(CredentialDerivationError::UnsafeSourceUri(source_set.gid));
        }
        if source.persistence_safe_uri.is_none() && !source.needs_credentials {
            return Err(CredentialDerivationError::UnmarkedRedactedSource(
                source_set.gid,
            ));
        }
    }
    Ok(())
}

fn is_runnable_source(source: &SessionTaskSourceRecord) -> bool {
    source.persistence_safe_uri.is_some() && !source.needs_credentials
}

fn credential_requirement(source: &SessionTaskSourceRecord) -> CredentialRequirement {
    let kind = source
        .persistence_safe_uri
        .as_deref()
        .map_or(CredentialKind::SourceUri, credential_kind_for_uri);
    let safe_description = match kind {
        CredentialKind::HttpAuthentication => "HTTP credentials required after restart",
        CredentialKind::FtpAuthentication => "FTP credentials required after restart",
        CredentialKind::SftpAuthentication => "SFTP credentials required after restart",
        CredentialKind::SourceUri => "source URI or credentials required after restart",
        CredentialKind::ProxyAuthentication => "proxy credentials required after restart",
        CredentialKind::PrivateKeyPassphrase => "private-key passphrase required after restart",
    }
    .to_owned();
    CredentialRequirement {
        kind,
        source: Some(UriId::new(source.uri_id)),
        safe_description,
    }
}

fn credential_kind_for_uri(uri: &str) -> CredentialKind {
    let scheme = uri.split_once("://").map(|(scheme, _)| scheme);
    match scheme {
        Some(scheme)
            if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") =>
        {
            CredentialKind::HttpAuthentication
        }
        Some(scheme)
            if scheme.eq_ignore_ascii_case("ftp") || scheme.eq_ignore_ascii_case("ftps") =>
        {
            CredentialKind::FtpAuthentication
        }
        Some(scheme)
            if scheme.eq_ignore_ascii_case("sftp") || scheme.eq_ignore_ascii_case("ssh") =>
        {
            CredentialKind::SftpAuthentication
        }
        _ => CredentialKind::SourceUri,
    }
}

/// Exact descriptor-opening work that still requires a native filesystem layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredAppenderRecovery {
    pub task_id: TaskId,
    pub gid: Gid,
    pub journal_id: JournalId,
    pub primary_path: PlatformPath,
    pub replica: Option<DeferredReplicaRecovery>,
    pub expected_last_sequence: u64,
}

/// A bounded replica checkpoint that may be considered only after primary validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredReplicaRecovery {
    pub path: PlatformPath,
    pub copied_through_sequence: u64,
}

/// A persisted install protocol row requiring native resolution before use.
///
/// For `Installing`, no direct appender request is emitted: native recovery
/// must either reject the candidate and return an old-set appender request or
/// install the validated checkpoint and return a new-set request. If the
/// authoritative sequence is later than the frozen source sequence, the
/// candidate cannot replace that prefix and must be rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredJournalInstallRecovery {
    pub task_id: TaskId,
    pub intent: JournalInstallIntent,
    pub authoritative_journal_id: JournalId,
    pub authoritative_last_sequence: u64,
    pub authoritative_checkpoint: Option<RecoveredCheckpoint>,
}

/// One directly executable SQLite repair authorized by journal completion evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredTerminalSessionRepair {
    pub result: SessionStoppedResultRecord,
    pub transition: SessionQueueTransition,
}

#[derive(Debug, Eq, PartialEq)]
struct NoSpaceProbeTargetEntry {
    task_id: TaskId,
    gid: Gid,
    generation: Generation,
    decision: RecoveredDelayDecision,
    target: Option<PlatformPath>,
}

/// One exact scheduler-owned probe identity paired with its native target.
///
/// This binding is move-only so the normal execution path can transfer it to
/// exactly one native probe operation.
#[derive(Debug, Eq, PartialEq)]
pub struct BoundNoSpaceProbe {
    task_id: TaskId,
    gid: Gid,
    generation: Generation,
    probe_id: NoSpaceProbeId,
    origin: NoSpaceProbeOrigin,
    at: MonotonicInstant,
    target: PlatformPath,
    decision: RecoveredDelayDecision,
}

impl BoundNoSpaceProbe {
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        self.gid
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn probe_id(&self) -> NoSpaceProbeId {
        self.probe_id
    }

    #[must_use]
    pub const fn origin(&self) -> NoSpaceProbeOrigin {
        self.origin
    }

    #[must_use]
    pub const fn at(&self) -> MonotonicInstant {
        self.at
    }

    #[must_use]
    pub const fn target(&self) -> &PlatformPath {
        &self.target
    }

    #[must_use]
    pub const fn decision(&self) -> RecoveredDelayDecision {
        self.decision
    }

    #[must_use]
    pub fn into_target(self) -> PlatformPath {
        self.target
    }
}

/// Why a deferred native target cannot be bound to a scheduler effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoSpaceProbeTargetError {
    NotProbeEffect,
    UnexpectedOrigin,
    UnknownIdentity,
    DeadlineMismatch,
    AlreadyConsumed,
}

impl fmt::Display for NoSpaceProbeTargetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotProbeEffect => "effect is not a no-space probe",
            Self::UnexpectedOrigin => "startup target requires an automatic retry probe",
            Self::UnknownIdentity => "no startup target matches the probe task identity",
            Self::DeadlineMismatch => "startup target and probe deadline disagree",
            Self::AlreadyConsumed => "startup no-space target was already consumed",
        })
    }
}

impl Error for NoSpaceProbeTargetError {}

/// Bounded, move-only native targets for scheduler-owned startup probes.
///
/// The catalog consumes each exact task/GID/generation/deadline entry once and
/// adopts the fresh scheduler-owned probe id from the matching effect.
///
/// ```compile_fail
/// use ariax_engine::NoSpaceProbeTargetCatalog;
///
/// fn duplicate(catalog: NoSpaceProbeTargetCatalog) {
///     let _second_authority = catalog.clone();
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct NoSpaceProbeTargetCatalog {
    entries: Vec<NoSpaceProbeTargetEntry>,
}

impl NoSpaceProbeTargetCatalog {
    fn new(entries: Vec<NoSpaceProbeTargetEntry>) -> Self {
        Self { entries }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.remaining()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.target.is_some())
            .count()
    }

    pub fn consume(
        &mut self,
        effect: &TransitionEffect,
    ) -> Result<BoundNoSpaceProbe, NoSpaceProbeTargetError> {
        let TransitionEffect::ProbeNoSpace {
            task_id,
            gid,
            generation,
            probe_id,
            origin,
            at,
        } = effect
        else {
            return Err(NoSpaceProbeTargetError::NotProbeEffect);
        };
        if *origin != NoSpaceProbeOrigin::AutomaticRetry {
            return Err(NoSpaceProbeTargetError::UnexpectedOrigin);
        }
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| {
                entry.task_id == *task_id && entry.gid == *gid && entry.generation == *generation
            })
            .ok_or(NoSpaceProbeTargetError::UnknownIdentity)?;
        if entry.decision.deadline() != *at {
            return Err(NoSpaceProbeTargetError::DeadlineMismatch);
        }
        let target = entry
            .target
            .take()
            .ok_or(NoSpaceProbeTargetError::AlreadyConsumed)?;
        Ok(BoundNoSpaceProbe {
            task_id: *task_id,
            gid: *gid,
            generation: *generation,
            probe_id: *probe_id,
            origin: *origin,
            at: *at,
            target,
            decision: entry.decision,
        })
    }
}

/// Journal-owned SQLite cache values that differ from the startup snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionAuthorityRepair {
    pub gid: Gid,
    pub expected_journal_id: JournalId,
    pub cache: Option<SessionJournalCache>,
    pub root_display: Option<PlatformPath>,
    pub updated_ms: u64,
}

/// Task-local durable state retained beside the fresh scheduler identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredEngineTask {
    pub task_id: TaskId,
    pub gid: Gid,
    pub journal_id: JournalId,
    pub journal: RecoveredJournalState,
    pub recovered_retry_budget_elapsed_ms: Option<u64>,
}

/// Pure output produced before the scheduler is constructed.
///
/// `queue_session_repairs` must be durably applied in order before
/// `terminal_session_repairs`, followed by `authority_repairs`; neither
/// scheduler effects nor snapshots may be published before every repair and
/// all native recovery work succeed.
#[derive(Debug, Eq, PartialEq)]
pub struct StartupReconciliation {
    pub scheduler_batch: SchedulerRestoreBatch,
    pub tasks: Vec<RecoveredEngineTask>,
    pub authority_repairs: Vec<SessionAuthorityRepair>,
    pub appender_recoveries: Vec<DeferredAppenderRecovery>,
    pub journal_install_recoveries: Vec<DeferredJournalInstallRecovery>,
    pub no_space_probe_targets: NoSpaceProbeTargetCatalog,
    pub queue_session_repairs: Vec<SessionQueueTransition>,
    pub terminal_session_repairs: Vec<DeferredTerminalSessionRepair>,
}

/// Scheduler reconstruction plus mandatory pre-publication recovery metadata.
///
/// Construction is atomic in memory, but this value is not permission to
/// publish or admit work. Any nonempty repair vectors must first be applied in
/// queue/terminal/authority order. Callers must also resolve journal installs
/// before affected appenders, bind native roots, and install the final
/// appenders.
pub struct EngineStartup {
    pub scheduler: RequestScheduler,
    pub restore_plan: SchedulerRestorePlan,
    pub tasks: Vec<RecoveredEngineTask>,
    pub authority_repairs: Vec<SessionAuthorityRepair>,
    pub appender_recoveries: Vec<DeferredAppenderRecovery>,
    pub journal_install_recoveries: Vec<DeferredJournalInstallRecovery>,
    pub no_space_probe_targets: NoSpaceProbeTargetCatalog,
    pub queue_session_repairs: Vec<SessionQueueTransition>,
    pub terminal_session_repairs: Vec<DeferredTerminalSessionRepair>,
}

/// Which persisted wall-clock decision failed validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupDeadlineKind {
    Retry,
    SlowReadmission,
    NoSpace,
}

impl StartupDeadlineKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::SlowReadmission => "slow_readmission",
            Self::NoSpace => "no_space",
        }
    }
}

/// Fail-closed startup reconciliation errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupRecoveryError {
    TaskLimitReached,
    AllocationFailed,
    MissingSession,
    TaskSessionMismatch(Gid),
    DuplicateTask(Gid),
    DuplicateJournalGid(Gid),
    MissingJournal(Gid),
    ExtraJournal(Gid),
    DuplicateJournalId(JournalId),
    DuplicatePersistedTaskId(TaskId),
    DuplicateCredentialAdmission(Gid),
    MissingCredentialAdmission(Gid),
    ExtraCredentialAdmission(Gid),
    JournalIdMismatch(Gid),
    ReplicaAheadOfPrimary(Gid),
    QueueNotDense {
        state: SessionQueueState,
        expected: u32,
        actual: u32,
    },
    MissingCurrentOptions(Gid),
    DuplicateStoppedResult(Gid),
    MissingStoppedResult(Gid),
    ExtraStoppedResult(Gid),
    TerminalQueueMismatch(Gid),
    StoppedResultMismatch(Gid),
    DuplicateHostKeyChallenge(Gid),
    HostKeyChallengeMismatch(Gid),
    InvalidHostKeyChallenge {
        gid: Gid,
        source: PresentedHostKeyChallengeError,
    },
    DuplicateJournalInstall(Gid),
    JournalInstallMismatch(Gid),
    JournalInstallIdentityConflict(Gid),
    JournalInstallSourceAhead(Gid),
    InstalledCheckpointMismatch(Gid),
    InvalidTaskRetryIdentity(Gid),
    DuplicateTaskRetry(Gid),
    MissingSlowDeadline(Gid),
    ConflictingDeadlines(Gid),
    InvalidDeadline {
        gid: Gid,
        kind: StartupDeadlineKind,
        source: PersistedDelayError,
    },
    TaskIdExhausted,
    QueueRepairInvariant,
    SchedulerRestore(SchedulerRestoreError),
}

impl fmt::Display for StartupRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskLimitReached => formatter.write_str("startup task limit exceeded"),
            Self::AllocationFailed => formatter.write_str("startup allocation failed"),
            Self::MissingSession => formatter.write_str("startup tasks have no session row"),
            Self::TaskSessionMismatch(gid) => {
                write!(formatter, "task {gid} references a different session")
            }
            Self::DuplicateTask(gid) => write!(formatter, "duplicate SQLite task {gid}"),
            Self::DuplicateJournalGid(gid) => {
                write!(formatter, "duplicate recovered journal GID {gid}")
            }
            Self::MissingJournal(gid) => write!(formatter, "task {gid} has no recovered journal"),
            Self::ExtraJournal(gid) => {
                write!(formatter, "recovered journal {gid} has no SQLite task")
            }
            Self::DuplicateJournalId(_) => formatter.write_str("duplicate recovered journal id"),
            Self::DuplicatePersistedTaskId(task_id) => write!(
                formatter,
                "duplicate persisted journal task id {}",
                task_id.get()
            ),
            Self::DuplicateCredentialAdmission(gid) => {
                write!(formatter, "duplicate credential admission for task {gid}")
            }
            Self::MissingCredentialAdmission(gid) => {
                write!(formatter, "task {gid} has no derived credential admission")
            }
            Self::ExtraCredentialAdmission(gid) => {
                write!(formatter, "credential admission {gid} has no SQLite task")
            }
            Self::JournalIdMismatch(gid) => {
                write!(formatter, "task {gid} journal id does not match SQLite")
            }
            Self::ReplicaAheadOfPrimary(gid) => {
                write!(
                    formatter,
                    "task {gid} replica is ahead of the recovered primary"
                )
            }
            Self::QueueNotDense {
                state,
                expected,
                actual,
            } => write!(
                formatter,
                "{} queue position {actual} is not dense; expected {expected}",
                state.code()
            ),
            Self::MissingCurrentOptions(gid) => {
                write!(
                    formatter,
                    "task {gid} journal has no current option snapshot"
                )
            }
            Self::DuplicateStoppedResult(gid) => {
                write!(formatter, "duplicate stopped result for {gid}")
            }
            Self::MissingStoppedResult(gid) => {
                write!(formatter, "terminal task {gid} has no stopped result")
            }
            Self::ExtraStoppedResult(gid) => {
                write!(formatter, "stopped result {gid} has no terminal journal")
            }
            Self::TerminalQueueMismatch(gid) => {
                write!(
                    formatter,
                    "task {gid} terminal journal and SQLite queue disagree"
                )
            }
            Self::StoppedResultMismatch(gid) => {
                write!(
                    formatter,
                    "task {gid} stopped result contradicts its journal"
                )
            }
            Self::DuplicateHostKeyChallenge(gid) => {
                write!(formatter, "duplicate host-key challenge for {gid}")
            }
            Self::HostKeyChallengeMismatch(gid) => {
                write!(
                    formatter,
                    "task {gid} host-key challenge and state disagree"
                )
            }
            Self::InvalidHostKeyChallenge { gid, source } => {
                write!(
                    formatter,
                    "task {gid} has an invalid host-key challenge: {source}"
                )
            }
            Self::DuplicateJournalInstall(gid) => {
                write!(formatter, "duplicate journal install for {gid}")
            }
            Self::JournalInstallMismatch(gid) => {
                write!(
                    formatter,
                    "task {gid} journal install contradicts its primary pointer"
                )
            }
            Self::JournalInstallIdentityConflict(gid) => {
                write!(
                    formatter,
                    "task {gid} journal install reuses an unsafe identity or path"
                )
            }
            Self::JournalInstallSourceAhead(gid) => {
                write!(
                    formatter,
                    "task {gid} journal install source is ahead of the recovered prefix"
                )
            }
            Self::InstalledCheckpointMismatch(gid) => {
                write!(
                    formatter,
                    "task {gid} installed journal does not match its checkpoint intent"
                )
            }
            Self::InvalidTaskRetryIdentity(gid) => {
                write!(
                    formatter,
                    "task {gid} has retry evidence for another task id"
                )
            }
            Self::DuplicateTaskRetry(gid) => {
                write!(
                    formatter,
                    "task {gid} has multiple task-scope retry decisions"
                )
            }
            Self::MissingSlowDeadline(gid) => {
                write!(
                    formatter,
                    "demoted task {gid} has no slow-readmission decision"
                )
            }
            Self::ConflictingDeadlines(gid) => {
                write!(formatter, "task {gid} has conflicting scheduler deadlines")
            }
            Self::InvalidDeadline { gid, kind, source } => write!(
                formatter,
                "task {gid} has an invalid {} deadline: {source}",
                kind.code()
            ),
            Self::TaskIdExhausted => formatter.write_str("fresh task id allocation exhausted"),
            Self::QueueRepairInvariant => {
                formatter.write_str("startup queue repair plan does not reach the restored order")
            }
            Self::SchedulerRestore(error) => write!(formatter, "scheduler restore failed: {error}"),
        }
    }
}

impl Error for StartupRecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidHostKeyChallenge { source, .. } => Some(source),
            Self::InvalidDeadline { source, .. } => Some(source),
            Self::SchedulerRestore(source) => Some(source),
            _ => None,
        }
    }
}

impl From<SchedulerRestoreError> for StartupRecoveryError {
    fn from(error: SchedulerRestoreError) -> Self {
        Self::SchedulerRestore(error)
    }
}

/// Failures from the ordinary source-derived startup path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DerivedStartupError {
    Credentials(CredentialDerivationError),
    Recovery(StartupRecoveryError),
}

impl fmt::Display for DerivedStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Credentials(error) => write!(formatter, "credential derivation failed: {error}"),
            Self::Recovery(error) => error.fmt(formatter),
        }
    }
}

impl Error for DerivedStartupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Credentials(error) => Some(error),
            Self::Recovery(error) => Some(error),
        }
    }
}

impl From<CredentialDerivationError> for DerivedStartupError {
    fn from(error: CredentialDerivationError) -> Self {
        Self::Credentials(error)
    }
}

impl From<StartupRecoveryError> for DerivedStartupError {
    fn from(error: StartupRecoveryError) -> Self {
        Self::Recovery(error)
    }
}

/// Reconciles the ordinary plaintext-free startup path after deriving one
/// admission decision from every materialized source set.
pub fn reconcile_startup_derived(
    snapshot: SessionStartupSnapshot,
    journals: Vec<RecoveredTaskJournal>,
    config: StartupRecoveryConfig,
) -> Result<StartupReconciliation, DerivedStartupError> {
    let credential_admissions = derive_credential_admissions(&snapshot)?;
    reconcile_startup(snapshot, journals, credential_admissions, config).map_err(Into::into)
}

/// Constructs the in-memory scheduler through the ordinary source-derived
/// startup path. This does not apply repair vectors or authorize publication.
pub fn reconcile_and_restore_derived(
    snapshot: SessionStartupSnapshot,
    journals: Vec<RecoveredTaskJournal>,
    config: StartupRecoveryConfig,
) -> Result<EngineStartup, DerivedStartupError> {
    let credential_admissions = derive_credential_admissions(&snapshot)?;
    reconcile_and_restore(snapshot, journals, credential_admissions, config).map_err(Into::into)
}

/// Reconciles durable authorities without opening files or publishing state.
pub fn reconcile_startup(
    snapshot: SessionStartupSnapshot,
    journals: Vec<RecoveredTaskJournal>,
    credential_admissions: Vec<DerivedCredentialAdmission>,
    config: StartupRecoveryConfig,
) -> Result<StartupReconciliation, StartupRecoveryError> {
    let task_count = snapshot.tasks.len();
    let repair_updated_ms = snapshot
        .tasks
        .iter()
        .map(|task| task.updated_ms)
        .chain(
            snapshot
                .stopped_results
                .iter()
                .map(|result| result.completed_ms),
        )
        .fold(config.now_wall_unix_ms, u64::max);
    if task_count > config.scheduler.max_tasks.get()
        || journals.len() > config.scheduler.max_tasks.get()
        || snapshot.stopped_results.len() > config.scheduler.max_tasks.get()
        || snapshot.host_key_challenges.len() > config.scheduler.max_tasks.get()
        || snapshot.journal_installs.len() > config.scheduler.max_tasks.get()
        || credential_admissions.len() > config.scheduler.max_tasks.get()
    {
        return Err(StartupRecoveryError::TaskLimitReached);
    }

    let mut tasks = BTreeMap::new();
    for task in snapshot.tasks {
        let gid = task.gid;
        if tasks.insert(gid, task).is_some() {
            return Err(StartupRecoveryError::DuplicateTask(gid));
        }
    }
    validate_dense_queues(tasks.values())?;
    let persisted_queues = collect_persisted_queues(tasks.values());

    let mut credentials = BTreeMap::new();
    for admission in credential_admissions {
        let gid = admission.gid;
        if credentials.insert(gid, admission.requirement).is_some() {
            return Err(StartupRecoveryError::DuplicateCredentialAdmission(gid));
        }
    }

    if !tasks.is_empty() {
        let session = snapshot
            .session
            .as_ref()
            .ok_or(StartupRecoveryError::MissingSession)?;
        if let Some(task) = tasks
            .values()
            .find(|task| task.session_id != session.session_id)
        {
            return Err(StartupRecoveryError::TaskSessionMismatch(task.gid));
        }
    }

    let mut recovered = BTreeMap::new();
    let mut journal_ids = BTreeSet::new();
    let mut persisted_task_ids = BTreeSet::new();
    for journal in journals {
        let gid = journal.gid;
        if !journal_ids.insert(journal.journal_id) {
            return Err(StartupRecoveryError::DuplicateJournalId(journal.journal_id));
        }
        if !persisted_task_ids.insert(journal.state.task()) {
            return Err(StartupRecoveryError::DuplicatePersistedTaskId(
                journal.state.task(),
            ));
        }
        if recovered.insert(gid, journal).is_some() {
            return Err(StartupRecoveryError::DuplicateJournalGid(gid));
        }
    }
    if let Some(gid) = tasks
        .keys()
        .copied()
        .find(|gid| !recovered.contains_key(gid))
    {
        return Err(StartupRecoveryError::MissingJournal(gid));
    }
    if let Some(gid) = recovered
        .keys()
        .copied()
        .find(|gid| !tasks.contains_key(gid))
    {
        return Err(StartupRecoveryError::ExtraJournal(gid));
    }

    let mut stopped_results = index_stopped_results(snapshot.stopped_results)?;
    let mut challenges = index_host_key_challenges(snapshot.host_key_challenges)?;
    let mut installs = validate_installs(&tasks, &recovered, snapshot.journal_installs)?;

    let mut scheduler_tasks = Vec::new();
    let mut engine_tasks = Vec::new();
    let mut authority_repairs = Vec::new();
    let mut appender_recoveries = Vec::new();
    let mut no_space_probe_targets = Vec::new();
    let mut queue_repair_candidates = Vec::new();
    let mut terminal_repair_candidates = Vec::new();
    let mut install_recoveries = Vec::new();
    scheduler_tasks
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    engine_tasks
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    authority_repairs
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    appender_recoveries
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    no_space_probe_targets
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    queue_repair_candidates
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    terminal_repair_candidates
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    install_recoveries
        .try_reserve(installs.len())
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    let mut memberships = Vec::new();
    memberships
        .try_reserve(task_count)
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;

    for (index, (gid, session_task)) in tasks.into_iter().enumerate() {
        let credential_requirement = credentials
            .remove(&gid)
            .ok_or(StartupRecoveryError::MissingCredentialAdmission(gid))?;
        let journal = recovered
            .remove(&gid)
            .ok_or(StartupRecoveryError::MissingJournal(gid))?;
        if journal.journal_id != session_task.primary_journal_id {
            return Err(StartupRecoveryError::JournalIdMismatch(gid));
        }
        if session_task
            .replica_sequence
            .is_some_and(|sequence| sequence > journal.state.last_sequence())
        {
            return Err(StartupRecoveryError::ReplicaAheadOfPrimary(gid));
        }
        let task_id_value = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(StartupRecoveryError::TaskIdExhausted)?;
        let task_id = TaskId::new(task_id_value).ok_or(StartupRecoveryError::TaskIdExhausted)?;

        let current_options = journal
            .state
            .current_options()
            .ok_or(StartupRecoveryError::MissingCurrentOptions(gid))?;
        let authoritative_cache = SessionJournalCache {
            layout_hash: journal.state.layout().map(|layout| layout.layout_hash()),
            root_binding_hash: journal
                .state
                .layout()
                .map(|layout| layout.root_binding_hash()),
            snapshot_hash: current_options.snapshot_hash(),
        };
        let persisted_cache = SessionJournalCache {
            layout_hash: session_task.cached_layout_hash,
            root_binding_hash: session_task.cached_root_binding_hash,
            snapshot_hash: session_task.cached_snapshot_hash,
        };
        let authoritative_root = journal
            .state
            .layout()
            .map(|layout| layout.layout().root_binding().path().clone());
        let root_repair = authoritative_root
            .as_ref()
            .filter(|path| *path != &session_task.root_display)
            .cloned();
        if persisted_cache != authoritative_cache || root_repair.is_some() {
            authority_repairs.push(SessionAuthorityRepair {
                gid,
                expected_journal_id: journal.journal_id,
                cache: (persisted_cache != authoritative_cache).then_some(authoritative_cache),
                root_display: root_repair,
                updated_ms: repair_updated_ms,
            });
        }

        let stopped = stopped_results.remove(&gid);
        let terminal = reconcile_terminal(gid, &session_task, &journal.state, stopped)?;
        if let Some(result) = terminal
            .as_ref()
            .and_then(|terminal| terminal.repair_result.clone())
        {
            terminal_repair_candidates.push(TerminalRepairCandidate {
                result,
                expected_state: session_task.queue_state,
                slow_demotion_count: session_task.slow_demotion_count,
                updated_ms: repair_updated_ms,
            });
        }
        let challenge =
            reconcile_host_key(gid, &session_task, &journal.state, challenges.remove(&gid))?;
        let task_retry = if terminal.is_some() {
            None
        } else {
            task_retry(gid, &journal.state)?
        };
        let no_space = recover_no_space(
            gid,
            task_id,
            journal.state.generation(),
            terminal
                .is_none()
                .then_some(session_task.no_space.as_ref())
                .flatten(),
            config,
        )?;
        if let Some(target) = no_space.target {
            no_space_probe_targets.push(target);
        }
        let slow_slot = if terminal.is_some() {
            None
        } else {
            recover_slow_slot(gid, &session_task, config)?
        };
        if slow_slot.is_some() && task_retry.is_some() {
            return Err(StartupRecoveryError::ConflictingDeadlines(gid));
        }
        let recovered_retry = task_retry
            .map(|retry| recover_retry(gid, retry, config))
            .transpose()?;
        let is_terminal = terminal.is_some();

        let normalized = normalize_task(
            &session_task,
            &journal.state,
            terminal,
            challenge,
            slow_slot,
            recovered_retry.as_ref(),
            no_space.condition,
            credential_requirement,
        )?;
        memberships.push(QueueMembership {
            gid,
            class: normalized.queue,
            source: session_task.queue_state,
            position: session_task.queue_position,
        });
        let normalized_state = queue_state_for_class(normalized.queue);
        if !is_terminal && normalized_state != session_task.queue_state {
            queue_repair_candidates.push(QueueRepairCandidate {
                gid,
                expected_state: session_task.queue_state,
                target_state: normalized_state,
                desired_paused: session_task.desired_paused,
                slow_demotion_count: session_task.slow_demotion_count,
                updated_ms: repair_updated_ms,
            });
        }
        scheduler_tasks.push(RecoveredSchedulerTask {
            task_id,
            gid,
            state: normalized.state,
            generation: journal.state.generation(),
            generation_started: true,
            desired_paused: session_task.desired_paused,
            conditions: normalized.conditions,
            slow_demotion_count: session_task.slow_demotion_count,
            slow_slot: normalized.slow_slot,
            retry_at: normalized.retry_at,
            host_key_challenge: normalized.challenge,
            error: normalized.error,
            stopped_status: normalized.stopped_status,
        });
        let install = installs.remove(&gid);
        if let Some(intent) = install.as_ref() {
            install_recoveries.push(DeferredJournalInstallRecovery {
                task_id,
                intent: intent.clone(),
                authoritative_journal_id: journal.journal_id,
                authoritative_last_sequence: journal.state.last_sequence(),
                authoritative_checkpoint: journal.state.checkpoint().cloned(),
            });
        }
        if install
            .as_ref()
            .is_none_or(|intent| intent.phase != JournalInstallPhase::Installing)
        {
            appender_recoveries.push(DeferredAppenderRecovery {
                task_id,
                gid,
                journal_id: journal.journal_id,
                primary_path: session_task.primary_journal_path.clone(),
                replica: match (
                    session_task.replica_journal_path.clone(),
                    session_task.replica_sequence,
                ) {
                    (Some(path), Some(copied_through_sequence)) => Some(DeferredReplicaRecovery {
                        path,
                        copied_through_sequence,
                    }),
                    _ => None,
                },
                expected_last_sequence: journal.state.last_sequence(),
            });
        }
        engine_tasks.push(RecoveredEngineTask {
            task_id,
            gid,
            journal_id: journal.journal_id,
            journal: journal.state,
            recovered_retry_budget_elapsed_ms: recovered_retry
                .map(|retry| retry.recovered_budget_elapsed_ms),
        });
    }

    if let Some(gid) = stopped_results.keys().next().copied() {
        return Err(StartupRecoveryError::ExtraStoppedResult(gid));
    }
    if let Some(gid) = challenges.keys().next().copied() {
        return Err(StartupRecoveryError::HostKeyChallengeMismatch(gid));
    }
    if let Some(gid) = credentials.keys().next().copied() {
        return Err(StartupRecoveryError::ExtraCredentialAdmission(gid));
    }
    debug_assert!(installs.is_empty());

    let queues = build_scheduler_queues(memberships)?;
    let (queues_after_normalization, queue_session_repairs) =
        build_queue_session_repairs(persisted_queues, queue_repair_candidates, &queues)?;
    let (final_persisted_queues, terminal_session_repairs) = build_terminal_session_repairs(
        queues_after_normalization,
        terminal_repair_candidates,
        &queues,
    )?;
    validate_repaired_queues(&final_persisted_queues, &queues)?;
    Ok(StartupReconciliation {
        scheduler_batch: SchedulerRestoreBatch::new(scheduler_tasks, queues),
        tasks: engine_tasks,
        authority_repairs,
        appender_recoveries,
        journal_install_recoveries: install_recoveries,
        no_space_probe_targets: NoSpaceProbeTargetCatalog::new(no_space_probe_targets),
        queue_session_repairs,
        terminal_session_repairs,
    })
}

/// Reconciles all inputs and atomically constructs the scheduler.
pub fn reconcile_and_restore(
    snapshot: SessionStartupSnapshot,
    journals: Vec<RecoveredTaskJournal>,
    credential_admissions: Vec<DerivedCredentialAdmission>,
    config: StartupRecoveryConfig,
) -> Result<EngineStartup, StartupRecoveryError> {
    let reconciliation = reconcile_startup(snapshot, journals, credential_admissions, config)?;
    restore_reconciliation(reconciliation, config.scheduler)
}

fn restore_reconciliation(
    reconciliation: StartupReconciliation,
    scheduler_config: SchedulerConfig,
) -> Result<EngineStartup, StartupRecoveryError> {
    let (scheduler, restore_plan) =
        RequestScheduler::restore(scheduler_config, reconciliation.scheduler_batch)?;
    Ok(EngineStartup {
        scheduler,
        restore_plan,
        tasks: reconciliation.tasks,
        authority_repairs: reconciliation.authority_repairs,
        appender_recoveries: reconciliation.appender_recoveries,
        journal_install_recoveries: reconciliation.journal_install_recoveries,
        no_space_probe_targets: reconciliation.no_space_probe_targets,
        queue_session_repairs: reconciliation.queue_session_repairs,
        terminal_session_repairs: reconciliation.terminal_session_repairs,
    })
}

fn validate_dense_queues<'a>(
    tasks: impl Iterator<Item = &'a SessionTaskRecord>,
) -> Result<(), StartupRecoveryError> {
    let mut queues: [Vec<(u32, Gid)>; 5] = std::array::from_fn(|_| Vec::new());
    for task in tasks {
        queues[queue_state_index(task.queue_state)].push((task.queue_position, task.gid));
    }
    for (index, queue) in queues.iter_mut().enumerate() {
        queue.sort_unstable();
        for (expected, (actual, _)) in queue.iter().copied().enumerate() {
            let expected =
                u32::try_from(expected).map_err(|_| StartupRecoveryError::TaskLimitReached)?;
            if actual != expected {
                return Err(StartupRecoveryError::QueueNotDense {
                    state: queue_state_from_index(index),
                    expected,
                    actual,
                });
            }
        }
    }
    Ok(())
}

fn index_stopped_results(
    results: Vec<SessionStoppedResultRecord>,
) -> Result<BTreeMap<Gid, SessionStoppedResultRecord>, StartupRecoveryError> {
    let mut indexed = BTreeMap::new();
    for result in results {
        let gid = result.gid;
        if indexed.insert(gid, result).is_some() {
            return Err(StartupRecoveryError::DuplicateStoppedResult(gid));
        }
    }
    Ok(indexed)
}

fn index_host_key_challenges(
    challenges: Vec<SessionHostKeyChallengeRecord>,
) -> Result<BTreeMap<Gid, SessionHostKeyChallengeRecord>, StartupRecoveryError> {
    let mut indexed = BTreeMap::new();
    for challenge in challenges {
        let gid = challenge.gid;
        if indexed.insert(gid, challenge).is_some() {
            return Err(StartupRecoveryError::DuplicateHostKeyChallenge(gid));
        }
    }
    Ok(indexed)
}

fn validate_installs(
    tasks: &BTreeMap<Gid, SessionTaskRecord>,
    journals: &BTreeMap<Gid, RecoveredTaskJournal>,
    installs: Vec<JournalInstallIntent>,
) -> Result<BTreeMap<Gid, JournalInstallIntent>, StartupRecoveryError> {
    let mut seen = BTreeSet::new();
    let mut checkpoint_ids = BTreeSet::new();
    let mut new_journal_ids = BTreeSet::new();
    let mut new_paths = Vec::new();
    new_paths
        .try_reserve(installs.len())
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    let mut output = BTreeMap::new();
    for intent in installs {
        if !seen.insert(intent.gid) {
            return Err(StartupRecoveryError::DuplicateJournalInstall(intent.gid));
        }
        let task = tasks
            .get(&intent.gid)
            .ok_or(StartupRecoveryError::JournalInstallMismatch(intent.gid))?;
        let journal = journals
            .get(&intent.gid)
            .ok_or(StartupRecoveryError::JournalInstallMismatch(intent.gid))?;
        if intent.source_last_sequence == 0
            || intent.old_journal_id == intent.new_journal_id
            || intent.old_path == intent.new_path
            || !checkpoint_ids.insert(intent.checkpoint_id)
            || !new_journal_ids.insert(intent.new_journal_id)
            || new_paths.iter().any(|path| path == &intent.new_path)
            || journals.iter().any(|(gid, recovered)| {
                *gid != intent.gid
                    && (recovered.journal_id == intent.old_journal_id
                        || recovered.journal_id == intent.new_journal_id)
            })
            || tasks.iter().any(|(gid, task)| {
                *gid != intent.gid && task.primary_journal_path == intent.new_path
            })
        {
            return Err(StartupRecoveryError::JournalInstallIdentityConflict(
                intent.gid,
            ));
        }
        let matches = match intent.phase {
            JournalInstallPhase::Installing => {
                task.primary_journal_id == intent.old_journal_id
                    && task.primary_journal_path == intent.old_path
                    && journal.journal_id == intent.old_journal_id
            }
            JournalInstallPhase::Installed => {
                task.primary_journal_id == intent.new_journal_id
                    && task.primary_journal_path == intent.new_path
                    && journal.journal_id == intent.new_journal_id
            }
        };
        if !matches {
            return Err(StartupRecoveryError::JournalInstallMismatch(intent.gid));
        }
        match intent.phase {
            JournalInstallPhase::Installing => {
                if intent.source_last_sequence > journal.state.last_sequence() {
                    return Err(StartupRecoveryError::JournalInstallSourceAhead(intent.gid));
                }
            }
            JournalInstallPhase::Installed => {
                let checkpoint = journal.state.checkpoint().ok_or(
                    StartupRecoveryError::InstalledCheckpointMismatch(intent.gid),
                )?;
                if checkpoint.checkpoint_id != intent.checkpoint_id
                    || checkpoint.source_last_sequence != intent.source_last_sequence
                {
                    return Err(StartupRecoveryError::InstalledCheckpointMismatch(
                        intent.gid,
                    ));
                }
            }
        }
        new_paths.push(intent.new_path.clone());
        output.insert(intent.gid, intent);
    }
    Ok(output)
}

struct ReconciledTerminal {
    status: Aria2Status,
    error: Option<PublicError>,
    repair_result: Option<SessionStoppedResultRecord>,
}

fn reconcile_terminal(
    gid: Gid,
    task: &SessionTaskRecord,
    journal: &RecoveredJournalState,
    result: Option<SessionStoppedResultRecord>,
) -> Result<Option<ReconciledTerminal>, StartupRecoveryError> {
    let Some(terminal) = journal.terminal() else {
        if task.queue_state == SessionQueueState::Stopped {
            return Err(StartupRecoveryError::TerminalQueueMismatch(gid));
        }
        if result.is_some() {
            return Err(StartupRecoveryError::ExtraStoppedResult(gid));
        }
        return Ok(None);
    };
    let needs_queue_repair = task.queue_state != SessionQueueState::Stopped;
    if needs_queue_repair && result.is_some() {
        return Err(StartupRecoveryError::TerminalQueueMismatch(gid));
    }
    let reconciled = match terminal {
        RecoveredTerminal::Complete {
            layout_hash,
            final_length,
            completed_at_unix_ms,
            ..
        } => {
            if result.as_ref().is_some_and(|result| {
                result.status != SessionTerminalStatus::Complete
                    || result.error_kind.is_some()
                    || !result.safe_message.is_empty()
                    || result.total_length != Some(*final_length)
                    || result.layout_hash != Some(*layout_hash)
                    || result.completed_ms != *completed_at_unix_ms
            }) {
                return Err(StartupRecoveryError::StoppedResultMismatch(gid));
            }
            let canonical = SessionStoppedResultRecord {
                gid,
                status: SessionTerminalStatus::Complete,
                error_kind: None,
                safe_message: String::new(),
                total_length: Some(*final_length),
                layout_hash: Some(*layout_hash),
                completed_ms: *completed_at_unix_ms,
            };
            ReconciledTerminal {
                status: Aria2Status::Complete,
                error: None,
                repair_result: (needs_queue_repair || result.is_none()).then_some(canonical),
            }
        }
        RecoveredTerminal::Error {
            error_class,
            retriable,
            diagnostic_id,
        } => {
            if needs_queue_repair {
                return Err(StartupRecoveryError::MissingStoppedResult(gid));
            }
            let result = result
                .as_ref()
                .ok_or(StartupRecoveryError::MissingStoppedResult(gid))?;
            if result.status != SessionTerminalStatus::Error
                || result.error_kind != Some(*error_class)
                || result.total_length.is_some()
                || result.layout_hash.is_some()
            {
                return Err(StartupRecoveryError::StoppedResultMismatch(gid));
            }
            let retry = if *retriable {
                RetryClass::UserAction
            } else {
                RetryClass::Never
            };
            ReconciledTerminal {
                status: Aria2Status::Error,
                error: Some(
                    PublicError::new(*error_class, result.safe_message.clone(), retry)
                        .with_diagnostic_id(*diagnostic_id),
                ),
                repair_result: needs_queue_repair.then_some(result.clone()),
            }
        }
        RecoveredTerminal::Removed { .. } => {
            if needs_queue_repair {
                return Err(StartupRecoveryError::MissingStoppedResult(gid));
            }
            let result = result
                .as_ref()
                .ok_or(StartupRecoveryError::MissingStoppedResult(gid))?;
            if result.status != SessionTerminalStatus::Removed
                || result.error_kind.is_some()
                || !result.safe_message.is_empty()
                || result.total_length.is_some()
                || result.layout_hash.is_some()
            {
                return Err(StartupRecoveryError::StoppedResultMismatch(gid));
            }
            ReconciledTerminal {
                status: Aria2Status::Removed,
                error: None,
                repair_result: needs_queue_repair.then_some(result.clone()),
            }
        }
    };
    Ok(Some(reconciled))
}

fn reconcile_host_key(
    gid: Gid,
    task: &SessionTaskRecord,
    journal: &RecoveredJournalState,
    challenge: Option<SessionHostKeyChallengeRecord>,
) -> Result<Option<PresentedHostKeyChallenge>, StartupRecoveryError> {
    if journal.terminal().is_some() {
        return if challenge.is_some() {
            Err(StartupRecoveryError::HostKeyChallengeMismatch(gid))
        } else {
            Ok(None)
        };
    }
    let journal_requires_challenge = journal.paused() == Some(TaskPauseReason::HostKeyApproval);
    if challenge.is_none() && journal_requires_challenge {
        return Err(StartupRecoveryError::HostKeyChallengeMismatch(gid));
    }
    let Some(challenge) = challenge else {
        return Ok(None);
    };
    if journal.terminal().is_some()
        || task.queue_state != SessionQueueState::Paused
        || !journal_requires_challenge
    {
        return Err(StartupRecoveryError::HostKeyChallengeMismatch(gid));
    }
    let summary = HostKeyChallenge {
        id: challenge.challenge_id,
        canonical_host: challenge.canonical_host,
        port: challenge.port,
        algorithm: challenge.algorithm,
        fingerprint_sha256: challenge.fingerprint_sha256,
    };
    PresentedHostKeyChallenge::new(summary, challenge.presented_public_key)
        .map(Some)
        .map_err(|source| StartupRecoveryError::InvalidHostKeyChallenge { gid, source })
}

fn task_retry(
    gid: Gid,
    journal: &RecoveredJournalState,
) -> Result<Option<&RecoveredRetryState>, StartupRecoveryError> {
    let mut found = None;
    for retry in journal.retry_states().values() {
        if retry.scope != RetryScope::Task {
            continue;
        }
        if retry.scope_id.get() != journal.task().get() {
            return Err(StartupRecoveryError::InvalidTaskRetryIdentity(gid));
        }
        if found.replace(retry).is_some() {
            return Err(StartupRecoveryError::DuplicateTaskRetry(gid));
        }
    }
    Ok(found)
}

struct RecoveredRetry {
    decision: RecoveredDelayDecision,
    recovered_budget_elapsed_ms: u64,
}

fn recover_retry(
    gid: Gid,
    retry: &RecoveredRetryState,
    config: StartupRecoveryConfig,
) -> Result<RecoveredRetry, StartupRecoveryError> {
    let decision = recover_delay(
        gid,
        StartupDeadlineKind::Retry,
        retry.scheduled_at_unix_ms,
        retry.delay_ms,
        config.now_wall_unix_ms,
        config.now_monotonic,
        config.max_retry_wait_ms,
    )?;
    Ok(RecoveredRetry {
        recovered_budget_elapsed_ms: decision
            .retry_budget_elapsed_ms(retry.elapsed_before_wait_ms, config.max_retry_elapsed_ms),
        decision,
    })
}

struct RecoveredNoSpace {
    condition: Option<NoSpaceCondition>,
    target: Option<NoSpaceProbeTargetEntry>,
}

fn recover_no_space(
    gid: Gid,
    task_id: TaskId,
    generation: Generation,
    no_space: Option<&SessionNoSpaceCondition>,
    config: StartupRecoveryConfig,
) -> Result<RecoveredNoSpace, StartupRecoveryError> {
    let Some(no_space) = no_space else {
        return Ok(RecoveredNoSpace {
            condition: None,
            target: None,
        });
    };
    let decision = recover_delay(
        gid,
        StartupDeadlineKind::NoSpace,
        no_space.scheduled_at_ms,
        no_space.delay_ms,
        config.now_wall_unix_ms,
        config.now_monotonic,
        config.max_no_space_wait_ms,
    )?;
    Ok(RecoveredNoSpace {
        condition: Some(NoSpaceCondition {
            redacted_path: REDACTED_NO_SPACE_TARGET.to_owned(),
            retry_at: Some(decision.deadline()),
        }),
        target: Some(NoSpaceProbeTargetEntry {
            task_id,
            gid,
            generation,
            target: Some(no_space.target.clone()),
            decision,
        }),
    })
}

fn recover_slow_slot(
    gid: Gid,
    task: &SessionTaskRecord,
    config: StartupRecoveryConfig,
) -> Result<Option<SlowSlotPersistence>, StartupRecoveryError> {
    if task.queue_state != SessionQueueState::Demoted {
        return Ok(None);
    }
    let slow = task
        .slow_slot
        .as_ref()
        .ok_or(StartupRecoveryError::MissingSlowDeadline(gid))?;
    let retry = slow
        .retry
        .ok_or(StartupRecoveryError::MissingSlowDeadline(gid))?;
    let recovered = recover_delay(
        gid,
        StartupDeadlineKind::SlowReadmission,
        retry.scheduled_at_ms,
        retry.delay_ms,
        config.now_wall_unix_ms,
        config.now_monotonic,
        config.max_slow_wait_ms,
    )?;
    Ok(Some(SlowSlotPersistence {
        original_position: slow.original_position as usize,
        demotion_count: task.slow_demotion_count,
        decision: SlowReadmissionDecision {
            readmit_at: recovered.deadline(),
            scheduled_at_ms: retry.scheduled_at_ms,
            delay_ms: retry.delay_ms,
        },
    }))
}

fn recover_delay(
    gid: Gid,
    kind: StartupDeadlineKind,
    scheduled_at_ms: u64,
    delay_ms: u64,
    now_wall_unix_ms: u64,
    now_monotonic: MonotonicInstant,
    max_wait_ms: NonZeroU64,
) -> Result<RecoveredDelayDecision, StartupRecoveryError> {
    PersistedDelayDecision::new(scheduled_at_ms, delay_ms)
        .and_then(|decision| decision.recover(now_wall_unix_ms, now_monotonic, max_wait_ms))
        .map_err(|source| StartupRecoveryError::InvalidDeadline { gid, kind, source })
}

struct NormalizedTask {
    state: TaskState,
    queue: QueueClass,
    conditions: TaskConditions,
    slow_slot: Option<SlowSlotPersistence>,
    retry_at: Option<MonotonicInstant>,
    challenge: Option<PresentedHostKeyChallenge>,
    error: Option<PublicError>,
    stopped_status: Option<Aria2Status>,
}

#[allow(clippy::too_many_arguments)]
fn normalize_task(
    task: &SessionTaskRecord,
    journal: &RecoveredJournalState,
    terminal: Option<ReconciledTerminal>,
    challenge: Option<PresentedHostKeyChallenge>,
    slow_slot: Option<SlowSlotPersistence>,
    retry: Option<&RecoveredRetry>,
    no_space: Option<NoSpaceCondition>,
    needs_credentials: Option<CredentialRequirement>,
) -> Result<NormalizedTask, StartupRecoveryError> {
    let gid = task.gid;
    let conditions = TaskConditions {
        needs_credentials,
        no_space,
    };
    if let Some(terminal) = terminal {
        if challenge.is_some() || slow_slot.is_some() || retry.is_some() {
            return Err(StartupRecoveryError::TerminalQueueMismatch(gid));
        }
        return Ok(NormalizedTask {
            state: TaskState::StoppedResult,
            queue: QueueClass::Stopped,
            conditions,
            slow_slot: None,
            retry_at: None,
            challenge: None,
            error: terminal.error,
            stopped_status: Some(terminal.status),
        });
    }
    if let Some(challenge) = challenge {
        return Ok(NormalizedTask {
            state: TaskState::PausedHostKey,
            queue: QueueClass::Paused,
            conditions,
            slow_slot: None,
            retry_at: None,
            challenge: Some(challenge),
            error: None,
            stopped_status: None,
        });
    }
    if task.desired_paused {
        return Ok(NormalizedTask {
            state: if journal.paused() == Some(TaskPauseReason::SlowSlot) {
                TaskState::PausedSlow
            } else {
                TaskState::Paused
            },
            queue: QueueClass::Paused,
            conditions,
            slow_slot: None,
            retry_at: None,
            challenge: None,
            error: None,
            stopped_status: None,
        });
    }
    match task.queue_state {
        SessionQueueState::Stopped => Err(StartupRecoveryError::TerminalQueueMismatch(gid)),
        SessionQueueState::Demoted => Ok(NormalizedTask {
            state: TaskState::WaitingSlow,
            queue: QueueClass::Demoted,
            conditions,
            slow_slot,
            retry_at: None,
            challenge: None,
            error: None,
            stopped_status: None,
        }),
        SessionQueueState::Paused => Ok(NormalizedTask {
            state: if journal.paused() == Some(TaskPauseReason::SlowSlot) {
                TaskState::PausedSlow
            } else {
                TaskState::Paused
            },
            queue: QueueClass::Paused,
            conditions,
            slow_slot: None,
            retry_at: None,
            challenge: None,
            error: None,
            stopped_status: None,
        }),
        SessionQueueState::Waiting | SessionQueueState::Active => {
            let retry_at = retry.map(|retry| retry.decision.deadline());
            Ok(NormalizedTask {
                state: if retry_at.is_some() {
                    TaskState::RetryWait
                } else {
                    TaskState::Waiting
                },
                queue: QueueClass::Waiting,
                conditions,
                slow_slot: None,
                retry_at,
                challenge: None,
                error: None,
                stopped_status: None,
            })
        }
    }
}

#[derive(Clone, Copy)]
struct QueueMembership {
    gid: Gid,
    class: QueueClass,
    source: SessionQueueState,
    position: u32,
}

fn build_scheduler_queues(
    memberships: Vec<QueueMembership>,
) -> Result<Vec<QueueOrder>, StartupRecoveryError> {
    let mut queues: [Vec<QueueMembership>; 5] = std::array::from_fn(|_| Vec::new());
    for membership in memberships {
        queues[queue_class_index(membership.class)].push(membership);
    }
    let mut output = Vec::new();
    output
        .try_reserve(ALL_QUEUE_CLASSES.len())
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    for class in ALL_QUEUE_CLASSES.iter().copied() {
        let queue = &mut queues[queue_class_index(class)];
        queue.sort_unstable_by_key(|entry| {
            (
                queue_source_rank(class, entry.source),
                entry.position,
                entry.gid,
            )
        });
        let mut order = Vec::new();
        order
            .try_reserve(queue.len())
            .map_err(|_| StartupRecoveryError::AllocationFailed)?;
        order.extend(queue.iter().map(|entry| entry.gid));
        output.push(QueueOrder { class, order });
    }
    Ok(output)
}

fn collect_persisted_queues<'a>(
    tasks: impl Iterator<Item = &'a SessionTaskRecord>,
) -> [Vec<Gid>; 5] {
    let mut queues: [Vec<(u32, Gid)>; 5] = std::array::from_fn(|_| Vec::new());
    for task in tasks {
        queues[queue_state_index(task.queue_state)].push((task.queue_position, task.gid));
    }
    std::array::from_fn(|index| {
        queues[index].sort_unstable();
        queues[index].iter().map(|(_, gid)| *gid).collect()
    })
}

struct TerminalRepairCandidate {
    result: SessionStoppedResultRecord,
    expected_state: SessionQueueState,
    slow_demotion_count: u32,
    updated_ms: u64,
}

struct QueueRepairCandidate {
    gid: Gid,
    expected_state: SessionQueueState,
    target_state: SessionQueueState,
    desired_paused: bool,
    slow_demotion_count: u32,
    updated_ms: u64,
}

fn build_queue_session_repairs(
    mut queues: [Vec<Gid>; 5],
    candidates: Vec<QueueRepairCandidate>,
    final_queues: &[QueueOrder],
) -> Result<([Vec<Gid>; 5], Vec<SessionQueueTransition>), StartupRecoveryError> {
    let final_ranks = final_queue_ranks(final_queues);
    let mut output = Vec::new();
    output
        .try_reserve(candidates.len())
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    for candidate in candidates {
        if candidate.expected_state == candidate.target_state
            || candidate.expected_state == SessionQueueState::Stopped
            || candidate.target_state == SessionQueueState::Stopped
        {
            return Err(StartupRecoveryError::QueueRepairInvariant);
        }
        let source = &mut queues[queue_state_index(candidate.expected_state)];
        let position = source
            .iter()
            .position(|gid| *gid == candidate.gid)
            .ok_or(StartupRecoveryError::QueueRepairInvariant)?;
        source.remove(position);
        let source_order = source.clone();

        let target = &mut queues[queue_state_index(candidate.target_state)];
        target.push(candidate.gid);
        sort_intermediate_queue(
            target,
            &final_ranks[queue_state_index(candidate.target_state)],
        );
        let target_order = target.clone();
        output.push(SessionQueueTransition {
            gid: candidate.gid,
            expected_state: candidate.expected_state,
            target_state: candidate.target_state,
            desired_paused: candidate.desired_paused,
            slow_demotion_count: candidate.slow_demotion_count,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: candidate.expected_state,
                    gids: source_order,
                },
                SessionQueueOrder {
                    state: candidate.target_state,
                    gids: target_order,
                },
            ],
            updated_ms: candidate.updated_ms,
        });
    }
    Ok((queues, output))
}

fn build_terminal_session_repairs(
    mut queues: [Vec<Gid>; 5],
    candidates: Vec<TerminalRepairCandidate>,
    final_queues: &[QueueOrder],
) -> Result<([Vec<Gid>; 5], Vec<DeferredTerminalSessionRepair>), StartupRecoveryError> {
    let final_ranks = final_queue_ranks(final_queues);

    let mut output = Vec::new();
    output
        .try_reserve(candidates.len())
        .map_err(|_| StartupRecoveryError::AllocationFailed)?;
    for candidate in candidates {
        if candidate.expected_state == SessionQueueState::Stopped {
            return Err(StartupRecoveryError::TerminalQueueMismatch(
                candidate.result.gid,
            ));
        }
        let source = &mut queues[queue_state_index(candidate.expected_state)];
        let position = source
            .iter()
            .position(|gid| *gid == candidate.result.gid)
            .ok_or(StartupRecoveryError::TerminalQueueMismatch(
                candidate.result.gid,
            ))?;
        source.remove(position);
        let source_order = source.clone();

        let stopped = &mut queues[queue_state_index(SessionQueueState::Stopped)];
        stopped.push(candidate.result.gid);
        sort_intermediate_queue(
            stopped,
            &final_ranks[queue_state_index(SessionQueueState::Stopped)],
        );
        let stopped_order = stopped.clone();
        output.push(DeferredTerminalSessionRepair {
            transition: SessionQueueTransition {
                gid: candidate.result.gid,
                expected_state: candidate.expected_state,
                target_state: SessionQueueState::Stopped,
                desired_paused: false,
                slow_demotion_count: candidate.slow_demotion_count,
                slow_slot: None,
                final_orders: vec![
                    SessionQueueOrder {
                        state: candidate.expected_state,
                        gids: source_order,
                    },
                    SessionQueueOrder {
                        state: SessionQueueState::Stopped,
                        gids: stopped_order,
                    },
                ],
                updated_ms: candidate.updated_ms.max(candidate.result.completed_ms),
            },
            result: candidate.result,
        });
    }
    Ok((queues, output))
}

fn final_queue_ranks(final_queues: &[QueueOrder]) -> [BTreeMap<Gid, usize>; 5] {
    let mut ranks = std::array::from_fn(|_| BTreeMap::new());
    for queue in final_queues {
        let state = queue_state_for_class(queue.class);
        for (position, gid) in queue.order.iter().copied().enumerate() {
            ranks[queue_state_index(state)].insert(gid, position);
        }
    }
    ranks
}

fn sort_intermediate_queue(queue: &mut [Gid], final_ranks: &BTreeMap<Gid, usize>) {
    queue.sort_unstable_by_key(|gid| {
        final_ranks
            .get(gid)
            .map_or((1_u8, usize::MAX, *gid), |position| (0_u8, *position, *gid))
    });
}

fn validate_repaired_queues(
    queues: &[Vec<Gid>; 5],
    final_queues: &[QueueOrder],
) -> Result<(), StartupRecoveryError> {
    if final_queues.len() != ALL_QUEUE_CLASSES.len()
        || final_queues.iter().any(|queue| {
            queues[queue_state_index(queue_state_for_class(queue.class))] != queue.order
        })
    {
        return Err(StartupRecoveryError::QueueRepairInvariant);
    }
    Ok(())
}

const fn queue_state_index(state: SessionQueueState) -> usize {
    match state {
        SessionQueueState::Waiting => 0,
        SessionQueueState::Active => 1,
        SessionQueueState::Paused => 2,
        SessionQueueState::Stopped => 3,
        SessionQueueState::Demoted => 4,
    }
}

const fn queue_state_from_index(index: usize) -> SessionQueueState {
    match index {
        0 => SessionQueueState::Waiting,
        1 => SessionQueueState::Active,
        2 => SessionQueueState::Paused,
        3 => SessionQueueState::Stopped,
        _ => SessionQueueState::Demoted,
    }
}

const fn queue_class_index(class: QueueClass) -> usize {
    match class {
        QueueClass::Waiting => 0,
        QueueClass::Demoted => 1,
        QueueClass::Paused => 2,
        QueueClass::Active => 3,
        QueueClass::Stopped => 4,
    }
}

const fn queue_state_for_class(class: QueueClass) -> SessionQueueState {
    match class {
        QueueClass::Waiting => SessionQueueState::Waiting,
        QueueClass::Demoted => SessionQueueState::Demoted,
        QueueClass::Paused => SessionQueueState::Paused,
        QueueClass::Active => SessionQueueState::Active,
        QueueClass::Stopped => SessionQueueState::Stopped,
    }
}

const fn queue_source_rank(class: QueueClass, source: SessionQueueState) -> u8 {
    match (class, source) {
        (QueueClass::Waiting, SessionQueueState::Waiting) => 0,
        (QueueClass::Waiting, SessionQueueState::Active) => 1,
        (QueueClass::Paused, SessionQueueState::Paused) => 0,
        (QueueClass::Paused, SessionQueueState::Waiting) => 1,
        (QueueClass::Paused, SessionQueueState::Active) => 2,
        (QueueClass::Paused, SessionQueueState::Demoted) => 3,
        (QueueClass::Stopped, SessionQueueState::Stopped) => 0,
        (QueueClass::Stopped, SessionQueueState::Waiting) => 1,
        (QueueClass::Stopped, SessionQueueState::Active) => 2,
        (QueueClass::Stopped, SessionQueueState::Paused) => 3,
        (QueueClass::Stopped, SessionQueueState::Demoted) => 4,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CredentialDerivationError, DerivedCredentialAdmission, NoSpaceProbeTargetError,
        RecoveredTaskJournal, StartupDeadlineKind, StartupRecoveryConfig, StartupRecoveryError,
        derive_credential_admissions,
        reconcile_and_restore as reconcile_and_restore_with_credentials,
        reconcile_and_restore_derived, reconcile_startup as reconcile_startup_with_credentials,
    };
    use ariax_core::{
        CredentialKind, CredentialRequirement, CredentialRequirementKey, ErrorKind, FileId,
        Generation, Gid, HostKeyChallengeId, HostKeyFingerprint, MonotonicInstant, NoSpaceProbeId,
        NoSpaceProbeOrigin, OptionPatchId, QueueClass, SchedulerCommand, SchedulerConfig,
        SchedulerError, TaskId, TaskState, TransitionEffect, ValidatedOptionPatchKind,
    };
    use ariax_storage::{
        CheckpointId, DurabilityMode, FileEntry, FileIdentity, FileLayout, JournalFileLayoutEntry,
        JournalHash, JournalId, JournalInstallIntent, JournalInstallPhase, JournalPayload,
        JournalRecord, JournalRelativePath, JournalStateLimits, OptionsSnapshotScope, PathPlatform,
        PersistedId, PlatformPath, RetryReason, RetryScope, RootBinding, RootIdentity,
        SafePathBuilder, SanitizedOptionMap, SessionHostKeyChallengeRecord, SessionId,
        SessionJournalMode, SessionNoSpaceCondition, SessionQueueState, SessionRecord,
        SessionSlowRetryDecision, SessionSlowSlotState, SessionStartupSnapshot,
        SessionStoppedResultRecord, SessionStoreSettings, SessionTaskRecord,
        SessionTaskSourceRecord, SessionTaskSourceSet, SessionTerminalStatus, TaskPauseReason,
        TaskRemoveReason, calculate_checkpoint_state_hash, recover_journal_state,
    };
    use std::collections::BTreeMap;
    use std::num::{NonZeroU64, NonZeroUsize};

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("nonzero GID")
    }

    fn task_id(value: u64) -> TaskId {
        TaskId::new(value).expect("nonzero task id")
    }

    fn journal_id(value: u8) -> JournalId {
        JournalId::new([value; 16]).expect("nonzero journal id")
    }

    fn checkpoint_id(value: u8) -> CheckpointId {
        CheckpointId::new([value; 16]).expect("nonzero checkpoint id")
    }

    fn no_credentials(gids: impl IntoIterator<Item = Gid>) -> Vec<DerivedCredentialAdmission> {
        gids.into_iter()
            .map(|gid| DerivedCredentialAdmission {
                gid,
                requirement: None,
            })
            .collect()
    }

    fn install_intent(
        gid: Gid,
        phase: JournalInstallPhase,
        old_journal_id: JournalId,
        new_journal_id: JournalId,
        source_last_sequence: u64,
    ) -> JournalInstallIntent {
        JournalInstallIntent {
            gid,
            checkpoint_id: checkpoint_id(42),
            old_journal_id,
            old_path: path("/journals/old"),
            new_journal_id,
            new_path: path("/journals/new"),
            source_last_sequence,
            phase,
            created_ms: 1_500,
        }
    }

    fn path(value: &str) -> PlatformPath {
        PlatformPath::from_native_bytes(PathPlatform::Unix, value.as_bytes()).expect("path")
    }

    fn options() -> SanitizedOptionMap {
        SanitizedOptionMap::new([("out".to_owned(), "payload.bin".to_owned())]).expect("options")
    }

    fn record(sequence: u64, payload: JournalPayload) -> JournalRecord {
        JournalRecord {
            record_type: payload.record_type(),
            generation: Generation::INITIAL,
            sequence,
            payload: payload.encode().expect("payload"),
        }
    }

    fn recovered_journal(
        gid: Gid,
        persisted_task_id: TaskId,
        id: JournalId,
        extra: Vec<JournalPayload>,
    ) -> RecoveredTaskJournal {
        let options = options();
        let mut records = vec![
            record(
                1,
                JournalPayload::TaskCreated {
                    durability: DurabilityMode::Balanced,
                    creator_version: 1,
                },
            ),
            record(
                2,
                JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::CurrentGeneration,
                    patch_id: None,
                    snapshot_hash: options.snapshot_hash(),
                    options,
                },
            ),
        ];
        for payload in extra {
            let sequence = u64::try_from(records.len() + 1).expect("sequence");
            records.push(record(sequence, payload));
        }
        let replay = recover_journal_state(
            &records,
            persisted_task_id,
            &allow_all_options,
            JournalStateLimits::default(),
        );
        assert_eq!(replay.accepted_records, records.len());
        RecoveredTaskJournal {
            gid,
            journal_id: id,
            state: replay.state.expect("recovered state"),
        }
    }

    fn recovered_checkpoint_journal(
        gid: Gid,
        persisted_task_id: TaskId,
        id: JournalId,
        checkpoint_id: CheckpointId,
        source_last_sequence: u64,
    ) -> RecoveredTaskJournal {
        let options = options();
        let state_records = vec![
            record(
                2,
                JournalPayload::TaskCreated {
                    durability: DurabilityMode::Balanced,
                    creator_version: 1,
                },
            ),
            record(
                3,
                JournalPayload::OptionsSnapshot {
                    scope: OptionsSnapshotScope::CurrentGeneration,
                    patch_id: None,
                    snapshot_hash: options.snapshot_hash(),
                    options,
                },
            ),
        ];
        let state_hash =
            calculate_checkpoint_state_hash(&state_records).expect("checkpoint state hash");
        let state_record_count = u32::try_from(state_records.len()).expect("state record count");
        let mut records = vec![record(
            1,
            JournalPayload::CheckpointStart {
                checkpoint_id,
                source_last_sequence,
                source_segment_hash: JournalHash::new([29; 32]).expect("source hash"),
                state_record_count,
                created_at_unix_ms: 1_000,
            },
        )];
        records.extend(state_records);
        records.push(record(
            4,
            JournalPayload::CheckpointEnd {
                checkpoint_id,
                state_record_count,
                state_hash,
            },
        ));
        let replay = recover_journal_state(
            &records,
            persisted_task_id,
            &allow_all_options,
            JournalStateLimits::default(),
        );
        assert_eq!(replay.accepted_records, records.len());
        RecoveredTaskJournal {
            gid,
            journal_id: id,
            state: replay.state.expect("recovered checkpoint state"),
        }
    }

    fn completed_journal(
        gid: Gid,
        persisted_task_id: TaskId,
        id: JournalId,
    ) -> RecoveredTaskJournal {
        let root_display = path("/srv/downloads");
        let root_identity = RootIdentity::new(b"root-identity".to_vec()).expect("root identity");
        let file_identity = FileIdentity::new(b"file-identity".to_vec()).expect("file identity");
        let root_binding = RootBinding::new(
            root_display.clone(),
            root_identity.clone(),
            [(FileId::new(0), file_identity.clone())],
        )
        .expect("root binding");
        let relative = SafePathBuilder::from_user_path("payload.bin", PathPlatform::Unix)
            .expect("safe relative path");
        let layout = FileLayout::new(
            persisted_task_id,
            Generation::INITIAL,
            root_binding,
            vec![FileEntry::new(
                FileId::new(0),
                relative,
                Some(file_identity.clone()),
                2_048,
                0,
                2_048,
                true,
            )],
            Some(2_048),
            1_024,
        )
        .expect("layout");
        let layout_hash = JournalHash::new(*layout.layout_hash().as_bytes()).expect("layout hash");
        let root_binding_hash =
            JournalHash::new(*layout.root_binding().hash().as_bytes()).expect("root binding hash");
        let entry = JournalFileLayoutEntry::new(
            FileId::new(0),
            0,
            2_048,
            2_048,
            true,
            JournalRelativePath::new("payload.bin").expect("journal path"),
            file_identity.bytes().to_vec(),
        )
        .expect("journal layout entry");
        recovered_journal(
            gid,
            persisted_task_id,
            id,
            vec![
                JournalPayload::LayoutCommitted {
                    layout_hash,
                    root_binding_hash,
                    root_display,
                    root_identity: root_identity.bytes().to_vec().into_boxed_slice(),
                    total_length: Some(2_048),
                    piece_length: 1_024,
                    total_file_count: 1,
                    chunk_count: 1,
                    inline_files: vec![entry].into_boxed_slice(),
                },
                JournalPayload::TaskComplete {
                    layout_hash,
                    final_length: 2_048,
                    final_digest: None,
                    completed_at_unix_ms: 1_900,
                },
            ],
        )
    }

    fn allow_all_options(_: &str) -> bool {
        true
    }

    fn session_id() -> SessionId {
        SessionId::new([1; 16])
    }

    fn task_record(
        gid: Gid,
        queue_state: SessionQueueState,
        queue_position: u32,
        id: JournalId,
        snapshot_hash: JournalHash,
    ) -> SessionTaskRecord {
        SessionTaskRecord {
            gid,
            session_id: session_id(),
            queue_state,
            queue_position,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            primary_journal_id: id,
            primary_journal_path: path(&format!("/journals/{gid}")),
            replica_journal_path: None,
            replica_sequence: None,
            root_display: path(&format!("/outputs/{gid}")),
            cached_layout_hash: None,
            cached_root_binding_hash: None,
            cached_snapshot_hash: snapshot_hash,
            no_space: None,
            created_ms: 100,
            updated_ms: 200,
        }
    }

    fn settings() -> SessionStoreSettings {
        SessionStoreSettings {
            journal_mode: SessionJournalMode::Delete,
            page_size: 4096,
            synchronous: 2,
            foreign_keys: true,
            cache_size: -1024,
            mmap_size: 0,
            wal_auto_checkpoint: 0,
            limits: BTreeMap::new(),
        }
    }

    fn snapshot(tasks: Vec<SessionTaskRecord>) -> SessionStartupSnapshot {
        let task_sources = tasks
            .iter()
            .map(|task| SessionTaskSourceSet {
                gid: task.gid,
                sources: Vec::new(),
            })
            .collect();
        SessionStartupSnapshot {
            settings: settings(),
            session: Some(SessionRecord {
                session_id: session_id(),
                created_ms: 100,
                updated_ms: 200,
                clean_shutdown: false,
            }),
            tasks,
            task_sources,
            stopped_results: Vec::new(),
            host_key_challenges: Vec::new(),
            journal_installs: Vec::new(),
        }
    }

    fn config(now: MonotonicInstant) -> StartupRecoveryConfig {
        StartupRecoveryConfig {
            scheduler: SchedulerConfig::new(
                NonZeroUsize::new(64).expect("task cap"),
                NonZeroUsize::new(4).expect("active cap"),
                false,
            )
            .expect("scheduler config"),
            now_wall_unix_ms: 2_000,
            now_monotonic: now,
            max_retry_wait_ms: NonZeroU64::new(10_000).expect("retry wait"),
            max_slow_wait_ms: NonZeroU64::new(10_000).expect("slow wait"),
            max_no_space_wait_ms: NonZeroU64::new(10_000).expect("no-space wait"),
            max_retry_elapsed_ms: 20_000,
        }
    }

    fn reconcile_startup(
        snapshot: SessionStartupSnapshot,
        journals: Vec<RecoveredTaskJournal>,
        config: StartupRecoveryConfig,
    ) -> Result<super::StartupReconciliation, StartupRecoveryError> {
        let admissions = no_credentials(snapshot.tasks.iter().map(|task| task.gid));
        reconcile_startup_with_credentials(snapshot, journals, admissions, config)
    }

    fn reconcile_and_restore(
        snapshot: SessionStartupSnapshot,
        journals: Vec<RecoveredTaskJournal>,
        config: StartupRecoveryConfig,
    ) -> Result<super::EngineStartup, StartupRecoveryError> {
        let admissions = no_credentials(snapshot.tasks.iter().map(|task| task.gid));
        reconcile_and_restore_with_credentials(snapshot, journals, admissions, config)
    }

    fn journal_snapshot_hash(journal: &RecoveredTaskJournal) -> JournalHash {
        journal
            .state
            .current_options()
            .expect("current options")
            .snapshot_hash()
    }

    #[test]
    fn reconciles_all_recoverable_states_and_allocates_task_ids_by_gid() {
        let now = MonotonicInstant::now();
        let first_gid = gid(1);
        let paused_gid = gid(2);
        let retry_gid = gid(3);
        let slow_gid = gid(4);
        let host_gid = gid(5);
        let first = recovered_journal(first_gid, task_id(50), journal_id(1), Vec::new());
        let paused = recovered_journal(paused_gid, task_id(40), journal_id(2), Vec::new());
        let retry = recovered_journal(
            retry_gid,
            task_id(30),
            journal_id(3),
            vec![JournalPayload::RetryState {
                scope: RetryScope::Task,
                scope_id: PersistedId::new(30).expect("retry owner"),
                attempt: 2,
                elapsed_before_wait_ms: 500,
                scheduled_at_unix_ms: 1_000,
                delay_ms: 5_000,
                error_class: ErrorKind::Network,
                retry_reason: RetryReason::Backoff,
            }],
        );
        let slow = recovered_journal(slow_gid, task_id(20), journal_id(4), Vec::new());
        let key = vec![7_u8; 32];
        let fingerprint = HostKeyFingerprint::for_presented_key(&key);
        let host = recovered_journal(
            host_gid,
            task_id(10),
            journal_id(5),
            vec![JournalPayload::TaskPaused {
                reason: TaskPauseReason::HostKeyApproval,
            }],
        );

        let mut first_task = task_record(
            first_gid,
            SessionQueueState::Waiting,
            0,
            first.journal_id,
            journal_snapshot_hash(&first),
        );
        first_task.no_space = Some(SessionNoSpaceCondition {
            target: path("/outputs/first"),
            scheduled_at_ms: 1_000,
            delay_ms: 500,
        });
        let mut paused_task = task_record(
            paused_gid,
            SessionQueueState::Active,
            0,
            paused.journal_id,
            journal_snapshot_hash(&paused),
        );
        paused_task.desired_paused = true;
        let mut retry_task = task_record(
            retry_gid,
            SessionQueueState::Active,
            1,
            retry.journal_id,
            journal_snapshot_hash(&retry),
        );
        retry_task.no_space = Some(SessionNoSpaceCondition {
            target: path("/outputs/retry"),
            scheduled_at_ms: 1_100,
            delay_ms: 4_000,
        });
        let mut slow_task = task_record(
            slow_gid,
            SessionQueueState::Demoted,
            0,
            slow.journal_id,
            journal_snapshot_hash(&slow),
        );
        slow_task.slow_demotion_count = 3;
        slow_task.slow_slot = Some(SessionSlowSlotState {
            original_position: 0,
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: 1_000,
                delay_ms: 4_000,
            }),
        });
        slow_task.no_space = Some(SessionNoSpaceCondition {
            target: path("/outputs/slow"),
            scheduled_at_ms: 1_200,
            delay_ms: 4_000,
        });
        let host_task = task_record(
            host_gid,
            SessionQueueState::Paused,
            0,
            host.journal_id,
            journal_snapshot_hash(&host),
        );
        let mut startup_snapshot = snapshot(vec![
            host_task,
            retry_task,
            first_task,
            slow_task,
            paused_task,
        ]);
        startup_snapshot
            .host_key_challenges
            .push(SessionHostKeyChallengeRecord {
                gid: host_gid,
                challenge_id: HostKeyChallengeId::new([9; 16]),
                canonical_host: "example.test".to_owned(),
                port: 22,
                algorithm: "ssh-ed25519".to_owned(),
                presented_public_key: key,
                fingerprint_sha256: fingerprint,
                created_ms: 1_500,
            });

        let mut startup = reconcile_and_restore(
            startup_snapshot,
            vec![host, slow, retry, paused, first],
            config(now),
        )
        .expect("reconcile and restore");

        assert_eq!(startup.tasks.len(), 5);
        assert_eq!(startup.tasks[0].gid, first_gid);
        assert_eq!(startup.tasks[0].task_id.get(), 1);
        assert_eq!(startup.tasks[4].gid, host_gid);
        assert_eq!(startup.tasks[4].task_id.get(), 5);
        assert_eq!(
            startup.scheduler.queue_snapshot(QueueClass::Waiting),
            vec![first_gid, retry_gid]
        );
        assert_eq!(
            startup.scheduler.queue_snapshot(QueueClass::Demoted),
            vec![slow_gid]
        );
        assert_eq!(
            startup.scheduler.queue_snapshot(QueueClass::Paused),
            vec![host_gid, paused_gid]
        );
        assert!(
            startup
                .scheduler
                .queue_snapshot(QueueClass::Active)
                .is_empty()
        );
        assert_eq!(
            startup.scheduler.task(first_gid).expect("first task").state,
            TaskState::Waiting
        );
        assert!(
            startup
                .scheduler
                .task(first_gid)
                .expect("first task")
                .conditions
                .no_space
        );
        assert_eq!(
            startup.scheduler.task(retry_gid).expect("retry task").state,
            TaskState::RetryWait
        );
        assert!(
            startup
                .scheduler
                .task(retry_gid)
                .expect("retry task")
                .conditions
                .no_space
        );
        assert_eq!(
            startup.scheduler.task(slow_gid).expect("slow task").state,
            TaskState::WaitingSlow
        );
        assert!(
            startup
                .scheduler
                .task(slow_gid)
                .expect("slow task")
                .conditions
                .no_space
        );
        assert_eq!(
            startup.scheduler.task(host_gid).expect("host task").state,
            TaskState::PausedHostKey
        );
        assert_eq!(startup.no_space_probe_targets.len(), 3);
        assert_eq!(startup.no_space_probe_targets.remaining(), 3);
        assert_eq!(startup.queue_session_repairs.len(), 2);
        assert!(startup.restore_plan.remaining() >= 7);
        let mut restore_effects = Vec::new();
        while !startup.restore_plan.is_empty() {
            restore_effects.extend(startup.restore_plan.next_batch());
        }
        let first_probe = restore_effects
            .iter()
            .find(|effect| {
                matches!(
                    effect,
                    TransitionEffect::ProbeNoSpace { gid, .. } if *gid == first_gid
                )
            })
            .expect("first startup no-space probe");
        let bound = startup
            .no_space_probe_targets
            .consume(first_probe)
            .expect("bind exact startup target");
        assert_eq!(bound.gid(), first_gid);
        assert_eq!(bound.target(), &path("/outputs/first"));
        assert!(bound.decision().is_expired());
        assert_eq!(
            startup.no_space_probe_targets.consume(first_probe),
            Err(NoSpaceProbeTargetError::AlreadyConsumed)
        );
        assert_eq!(
            startup.tasks[2].recovered_retry_budget_elapsed_ms,
            Some(1_500)
        );
    }

    #[test]
    fn requires_complete_credential_derivation_and_restores_the_exact_requirement() {
        let now = MonotonicInstant::now();
        let task_gid = gid(6);
        let journal = recovered_journal(task_gid, task_id(60), journal_id(6), Vec::new());
        let task = task_record(
            task_gid,
            SessionQueueState::Waiting,
            0,
            journal.journal_id,
            journal_snapshot_hash(&journal),
        );
        let startup_snapshot = snapshot(vec![task]);
        assert_eq!(
            reconcile_startup_with_credentials(
                startup_snapshot.clone(),
                vec![journal.clone()],
                Vec::new(),
                config(now),
            ),
            Err(StartupRecoveryError::MissingCredentialAdmission(task_gid))
        );
        assert_eq!(
            reconcile_startup_with_credentials(
                startup_snapshot.clone(),
                vec![journal.clone()],
                vec![
                    DerivedCredentialAdmission {
                        gid: task_gid,
                        requirement: None,
                    },
                    DerivedCredentialAdmission {
                        gid: task_gid,
                        requirement: None,
                    },
                ],
                config(now),
            ),
            Err(StartupRecoveryError::DuplicateCredentialAdmission(task_gid))
        );
        assert_eq!(
            reconcile_startup_with_credentials(
                startup_snapshot.clone(),
                vec![journal.clone()],
                vec![
                    DerivedCredentialAdmission {
                        gid: task_gid,
                        requirement: None,
                    },
                    DerivedCredentialAdmission {
                        gid: gid(99),
                        requirement: None,
                    },
                ],
                config(now),
            ),
            Err(StartupRecoveryError::ExtraCredentialAdmission(gid(99)))
        );

        let requirement = CredentialRequirement {
            kind: CredentialKind::HttpAuthentication,
            source: None,
            safe_description: "HTTP credentials required".to_owned(),
        };
        let mut startup = reconcile_and_restore_with_credentials(
            startup_snapshot,
            vec![journal],
            vec![DerivedCredentialAdmission {
                gid: task_gid,
                requirement: Some(requirement.clone()),
            }],
            config(now),
        )
        .expect("credential-blocked startup");
        assert!(
            startup
                .scheduler
                .task(task_gid)
                .expect("restored task")
                .conditions
                .needs_credentials
        );
        let patch_id = OptionPatchId::new(1).expect("patch id");
        assert_eq!(
            startup
                .scheduler
                .execute_command(SchedulerCommand::ApplyOptionPatch {
                    gid: task_gid,
                    patch_id,
                    kind: ValidatedOptionPatchKind::InPlace,
                    satisfies_credentials: Some(CredentialRequirementKey {
                        kind: CredentialKind::ProxyAuthentication,
                        source: None,
                    }),
                }),
            Err(SchedulerError::StaleCredentialRequirement)
        );
        let outcome = startup
            .scheduler
            .execute_command(SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: Some(requirement.key()),
            })
            .expect("the exact recovered credential key is accepted");
        assert!(outcome.effects.iter().any(|effect| {
            matches!(
                effect,
                TransitionEffect::ApplyOptionPatch {
                    satisfies_credentials: Some(key),
                    ..
                } if *key == requirement.key()
            )
        }));
    }

    #[test]
    fn derives_exact_credential_admissions_from_canonical_source_sets() {
        let now = MonotonicInstant::now();
        let blocked_gid = gid(61);
        let runnable_gid = gid(62);
        let terminal_gid = gid(63);
        let blocked = recovered_journal(blocked_gid, task_id(61), journal_id(61), Vec::new());
        let runnable = recovered_journal(runnable_gid, task_id(62), journal_id(62), Vec::new());
        let terminal = recovered_journal(
            terminal_gid,
            task_id(63),
            journal_id(63),
            vec![JournalPayload::TaskRemoved {
                reason: TaskRemoveReason::User,
            }],
        );
        let blocked_task = task_record(
            blocked_gid,
            SessionQueueState::Waiting,
            0,
            blocked.journal_id,
            journal_snapshot_hash(&blocked),
        );
        let runnable_task = task_record(
            runnable_gid,
            SessionQueueState::Waiting,
            1,
            runnable.journal_id,
            journal_snapshot_hash(&runnable),
        );
        let terminal_task = task_record(
            terminal_gid,
            SessionQueueState::Stopped,
            0,
            terminal.journal_id,
            journal_snapshot_hash(&terminal),
        );
        let mut startup_snapshot = snapshot(vec![blocked_task, runnable_task, terminal_task]);
        startup_snapshot.task_sources = vec![
            SessionTaskSourceSet {
                gid: blocked_gid,
                sources: vec![SessionTaskSourceRecord {
                    uri_id: 7,
                    persistence_safe_uri: Some("sftp://files.example/payload".to_owned()),
                    redacted_fingerprint: [7; 32],
                    needs_credentials: true,
                    priority: 0,
                }],
            },
            SessionTaskSourceSet {
                gid: runnable_gid,
                sources: vec![
                    SessionTaskSourceRecord {
                        uri_id: 1,
                        persistence_safe_uri: None,
                        redacted_fingerprint: [1; 32],
                        needs_credentials: true,
                        priority: 0,
                    },
                    SessionTaskSourceRecord {
                        uri_id: 2,
                        persistence_safe_uri: Some("https://mirror.example/payload".to_owned()),
                        redacted_fingerprint: [2; 32],
                        needs_credentials: false,
                        priority: 1,
                    },
                ],
            },
            SessionTaskSourceSet {
                gid: terminal_gid,
                sources: vec![SessionTaskSourceRecord {
                    uri_id: 3,
                    persistence_safe_uri: None,
                    redacted_fingerprint: [3; 32],
                    needs_credentials: true,
                    priority: 0,
                }],
            },
        ];
        startup_snapshot
            .stopped_results
            .push(SessionStoppedResultRecord {
                gid: terminal_gid,
                status: SessionTerminalStatus::Removed,
                error_kind: None,
                safe_message: String::new(),
                total_length: None,
                layout_hash: None,
                completed_ms: 2_000,
            });
        let admissions = derive_credential_admissions(&startup_snapshot).expect("derive sources");
        assert_eq!(
            admissions,
            vec![
                DerivedCredentialAdmission {
                    gid: blocked_gid,
                    requirement: Some(CredentialRequirement {
                        kind: CredentialKind::SftpAuthentication,
                        source: Some(ariax_core::UriId::new(7)),
                        safe_description: "SFTP credentials required after restart".to_owned(),
                    }),
                },
                DerivedCredentialAdmission {
                    gid: runnable_gid,
                    requirement: None,
                },
                DerivedCredentialAdmission {
                    gid: terminal_gid,
                    requirement: None,
                },
            ]
        );

        let startup = reconcile_and_restore_derived(
            startup_snapshot,
            vec![blocked, runnable, terminal],
            config(now),
        )
        .expect("source-derived startup");
        assert!(
            startup
                .scheduler
                .task(blocked_gid)
                .expect("blocked task")
                .conditions
                .needs_credentials
        );
        assert!(
            !startup
                .scheduler
                .task(runnable_gid)
                .expect("runnable task")
                .conditions
                .needs_credentials
        );
    }

    #[test]
    fn credential_derivation_rejects_missing_extra_and_noncanonical_sources() {
        let task = task_record(
            gid(70),
            SessionQueueState::Waiting,
            0,
            journal_id(70),
            JournalHash::new([70; 32]).expect("snapshot hash"),
        );
        let mut startup_snapshot = snapshot(vec![task]);
        startup_snapshot.task_sources.clear();
        assert_eq!(
            derive_credential_admissions(&startup_snapshot),
            Err(CredentialDerivationError::MissingSourceSet(gid(70)))
        );

        startup_snapshot.task_sources.push(SessionTaskSourceSet {
            gid: gid(99),
            sources: Vec::new(),
        });
        assert_eq!(
            derive_credential_admissions(&startup_snapshot),
            Err(CredentialDerivationError::ExtraSourceSet(gid(99)))
        );

        startup_snapshot.task_sources[0] = SessionTaskSourceSet {
            gid: gid(70),
            sources: vec![SessionTaskSourceRecord {
                uri_id: 1,
                persistence_safe_uri: None,
                redacted_fingerprint: [1; 32],
                needs_credentials: false,
                priority: 0,
            }],
        };
        assert_eq!(
            derive_credential_admissions(&startup_snapshot),
            Err(CredentialDerivationError::UnmarkedRedactedSource(gid(70)))
        );

        startup_snapshot.task_sources[0].sources = vec![
            SessionTaskSourceRecord {
                uri_id: 2,
                persistence_safe_uri: None,
                redacted_fingerprint: [2; 32],
                needs_credentials: true,
                priority: 1,
            },
            SessionTaskSourceRecord {
                uri_id: 1,
                persistence_safe_uri: None,
                redacted_fingerprint: [1; 32],
                needs_credentials: true,
                priority: 0,
            },
        ];
        assert_eq!(
            derive_credential_admissions(&startup_snapshot),
            Err(CredentialDerivationError::NonCanonicalSourceOrder(gid(70)))
        );
    }

    #[test]
    fn credential_derivation_rejects_oversized_snapshot_sets_before_indexing() {
        let mut startup_snapshot = snapshot(Vec::new());
        let task = task_record(
            gid(71),
            SessionQueueState::Waiting,
            0,
            journal_id(71),
            JournalHash::new([71; 32]).expect("snapshot hash"),
        );
        startup_snapshot.tasks = vec![task; ariax_storage::SESSION_MAX_TASKS + 1];
        assert_eq!(
            derive_credential_admissions(&startup_snapshot),
            Err(CredentialDerivationError::TooManyTasks)
        );

        startup_snapshot.tasks.clear();
        startup_snapshot.task_sources = vec![
            SessionTaskSourceSet {
                gid: gid(71),
                sources: Vec::new(),
            };
            ariax_storage::SESSION_MAX_TASKS + 1
        ];
        assert_eq!(
            derive_credential_admissions(&startup_snapshot),
            Err(CredentialDerivationError::TooManySourceSets)
        );
    }

    #[test]
    fn no_space_target_catalog_rejects_nonexact_effects_without_consuming() {
        let now = MonotonicInstant::now();
        let task_gid = gid(7);
        let journal = recovered_journal(task_gid, task_id(70), journal_id(7), Vec::new());
        let mut task = task_record(
            task_gid,
            SessionQueueState::Waiting,
            0,
            journal.journal_id,
            journal_snapshot_hash(&journal),
        );
        task.no_space = Some(SessionNoSpaceCondition {
            target: path("/outputs/exact"),
            scheduled_at_ms: 1_500,
            delay_ms: 3_000,
        });
        let mut startup = reconcile_and_restore(snapshot(vec![task]), vec![journal], config(now))
            .expect("no-space startup");
        let mut effects = Vec::new();
        while !startup.restore_plan.is_empty() {
            effects.extend(startup.restore_plan.next_batch());
        }
        let non_probe = effects
            .iter()
            .find(|effect| !matches!(effect, TransitionEffect::ProbeNoSpace { .. }))
            .expect("restore snapshot effect");
        assert_eq!(
            startup.no_space_probe_targets.consume(non_probe),
            Err(NoSpaceProbeTargetError::NotProbeEffect)
        );
        let exact = effects
            .into_iter()
            .find(|effect| matches!(effect, TransitionEffect::ProbeNoSpace { .. }))
            .expect("startup probe effect");
        let TransitionEffect::ProbeNoSpace {
            task_id,
            gid,
            generation,
            probe_id,
            origin,
            at,
        } = exact
        else {
            unreachable!("selected probe effect")
        };
        let wrong_deadline = TransitionEffect::ProbeNoSpace {
            task_id,
            gid,
            generation,
            probe_id,
            origin,
            at: at
                .checked_add(std::time::Duration::from_millis(1))
                .expect("later deadline"),
        };
        assert_eq!(
            startup.no_space_probe_targets.consume(&wrong_deadline),
            Err(NoSpaceProbeTargetError::DeadlineMismatch)
        );
        let wrong_origin = TransitionEffect::ProbeNoSpace {
            task_id,
            gid,
            generation,
            probe_id,
            origin: NoSpaceProbeOrigin::ExplicitResume,
            at,
        };
        assert_eq!(
            startup.no_space_probe_targets.consume(&wrong_origin),
            Err(NoSpaceProbeTargetError::UnexpectedOrigin)
        );
        let wrong_identity = TransitionEffect::ProbeNoSpace {
            task_id: TaskId::new(task_id.get() + 1).expect("different task id"),
            gid,
            generation,
            probe_id: NoSpaceProbeId::new(probe_id.get() + 1).expect("different probe id"),
            origin,
            at,
        };
        assert_eq!(
            startup.no_space_probe_targets.consume(&wrong_identity),
            Err(NoSpaceProbeTargetError::UnknownIdentity)
        );
        assert_eq!(startup.no_space_probe_targets.remaining(), 1);
        let exact = TransitionEffect::ProbeNoSpace {
            task_id,
            gid,
            generation,
            probe_id,
            origin,
            at,
        };
        let bound = startup
            .no_space_probe_targets
            .consume(&exact)
            .expect("exact effect consumes target");
        assert_eq!(bound.probe_id(), probe_id);
        assert_eq!(bound.target(), &path("/outputs/exact"));
        assert!(startup.no_space_probe_targets.is_empty());
        assert_eq!(startup.no_space_probe_targets.len(), 0);
    }

    #[test]
    fn journal_terminal_and_sqlite_stopped_result_must_pair_exactly() {
        let now = MonotonicInstant::now();
        let gid = gid(7);
        let journal = recovered_journal(
            gid,
            task_id(70),
            journal_id(7),
            vec![JournalPayload::TaskError {
                error_class: ErrorKind::Network,
                retriable: false,
                diagnostic_id: 91,
            }],
        );
        let task = task_record(
            gid,
            SessionQueueState::Stopped,
            0,
            journal.journal_id,
            journal_snapshot_hash(&journal),
        );
        let mut startup_snapshot = snapshot(vec![task]);
        startup_snapshot
            .stopped_results
            .push(SessionStoppedResultRecord {
                gid,
                status: SessionTerminalStatus::Error,
                error_kind: Some(ErrorKind::Network),
                safe_message: "network failure".to_owned(),
                total_length: None,
                layout_hash: None,
                completed_ms: 2_000,
            });

        let startup = reconcile_and_restore(startup_snapshot, vec![journal], config(now))
            .expect("terminal restore");
        let snapshot = startup.scheduler.snapshot(gid).expect("stopped snapshot");
        assert_eq!(snapshot.state, TaskState::StoppedResult);
        assert_eq!(
            snapshot.stopped_status,
            Some(ariax_core::Aria2Status::Error)
        );
        assert_eq!(
            snapshot
                .error
                .as_ref()
                .and_then(|error| error.diagnostic_id()),
            Some(91)
        );
    }

    #[test]
    fn terminal_cache_payload_fields_must_match_the_journal_exactly() {
        let now = MonotonicInstant::now();

        let complete_gid = gid(8);
        let complete = completed_journal(complete_gid, task_id(80), journal_id(8));
        let complete_task = task_record(
            complete_gid,
            SessionQueueState::Stopped,
            0,
            complete.journal_id,
            journal_snapshot_hash(&complete),
        );
        let mut complete_snapshot = snapshot(vec![complete_task]);
        complete_snapshot
            .stopped_results
            .push(SessionStoppedResultRecord {
                gid: complete_gid,
                status: SessionTerminalStatus::Complete,
                error_kind: None,
                safe_message: String::new(),
                total_length: None,
                layout_hash: None,
                completed_ms: 1_900,
            });
        assert_eq!(
            reconcile_startup(complete_snapshot, vec![complete], config(now)),
            Err(StartupRecoveryError::StoppedResultMismatch(complete_gid))
        );

        let error_gid = gid(9);
        let error = recovered_journal(
            error_gid,
            task_id(90),
            journal_id(9),
            vec![JournalPayload::TaskError {
                error_class: ErrorKind::Network,
                retriable: false,
                diagnostic_id: 92,
            }],
        );
        let error_task = task_record(
            error_gid,
            SessionQueueState::Stopped,
            0,
            error.journal_id,
            journal_snapshot_hash(&error),
        );
        let mut error_snapshot = snapshot(vec![error_task]);
        error_snapshot
            .stopped_results
            .push(SessionStoppedResultRecord {
                gid: error_gid,
                status: SessionTerminalStatus::Error,
                error_kind: Some(ErrorKind::Network),
                safe_message: "network failure".to_owned(),
                total_length: Some(1),
                layout_hash: None,
                completed_ms: 2_000,
            });
        assert_eq!(
            reconcile_startup(error_snapshot, vec![error], config(now)),
            Err(StartupRecoveryError::StoppedResultMismatch(error_gid))
        );

        let removed_gid = gid(10);
        let removed = recovered_journal(
            removed_gid,
            task_id(100),
            journal_id(10),
            vec![JournalPayload::TaskRemoved {
                reason: TaskRemoveReason::User,
            }],
        );
        let removed_task = task_record(
            removed_gid,
            SessionQueueState::Stopped,
            0,
            removed.journal_id,
            journal_snapshot_hash(&removed),
        );
        let mut removed_snapshot = snapshot(vec![removed_task]);
        removed_snapshot
            .stopped_results
            .push(SessionStoppedResultRecord {
                gid: removed_gid,
                status: SessionTerminalStatus::Removed,
                error_kind: None,
                safe_message: String::new(),
                total_length: None,
                layout_hash: Some(JournalHash::new([44; 32]).expect("layout hash")),
                completed_ms: 2_000,
            });
        assert_eq!(
            reconcile_startup(removed_snapshot, vec![removed], config(now)),
            Err(StartupRecoveryError::StoppedResultMismatch(removed_gid))
        );
    }

    #[test]
    fn journal_complete_repairs_missing_results_with_executable_intermediate_orders() {
        let now = MonotonicInstant::now();
        let first_gid = gid(11);
        let second_gid = gid(12);
        let first = completed_journal(first_gid, task_id(110), journal_id(11));
        let second = completed_journal(second_gid, task_id(120), journal_id(12));
        let first_task = task_record(
            first_gid,
            SessionQueueState::Waiting,
            0,
            first.journal_id,
            journal_snapshot_hash(&first),
        );
        let second_task = task_record(
            second_gid,
            SessionQueueState::Active,
            0,
            second.journal_id,
            journal_snapshot_hash(&second),
        );

        let startup = reconcile_and_restore(
            snapshot(vec![second_task, first_task]),
            vec![second, first],
            config(now),
        )
        .expect("journal-authorized terminal repairs");

        assert_eq!(
            startup
                .scheduler
                .task(first_gid)
                .expect("first completed task")
                .state,
            TaskState::StoppedResult
        );
        assert_eq!(
            startup.scheduler.queue_snapshot(QueueClass::Stopped),
            vec![first_gid, second_gid]
        );
        assert!(
            startup
                .scheduler
                .queue_snapshot(QueueClass::Active)
                .is_empty()
        );
        assert!(
            startup
                .scheduler
                .queue_snapshot(QueueClass::Waiting)
                .is_empty()
        );
        assert_eq!(startup.terminal_session_repairs.len(), 2);
        let first_repair = &startup.terminal_session_repairs[0];
        assert_eq!(first_repair.result.gid, first_gid);
        assert_eq!(first_repair.result.status, SessionTerminalStatus::Complete);
        assert_eq!(first_repair.result.total_length, Some(2_048));
        assert_eq!(
            first_repair
                .transition
                .final_orders
                .iter()
                .find(|order| order.state == SessionQueueState::Stopped)
                .expect("first stopped order")
                .gids,
            vec![first_gid]
        );
        assert_eq!(
            first_repair
                .transition
                .final_orders
                .iter()
                .find(|order| order.state == SessionQueueState::Waiting)
                .expect("waiting order")
                .gids,
            Vec::<Gid>::new()
        );
        let second_repair = &startup.terminal_session_repairs[1];
        assert_eq!(second_repair.result.gid, second_gid);
        assert_eq!(
            second_repair
                .transition
                .final_orders
                .iter()
                .find(|order| order.state == SessionQueueState::Stopped)
                .expect("final stopped order")
                .gids,
            vec![first_gid, second_gid]
        );
    }

    #[test]
    fn queue_normalization_repairs_precede_terminal_repair_with_exact_intermediate_orders() {
        let now = MonotonicInstant::now();
        let active_wait_gid = gid(1);
        let demoted_pause_gid = gid(2);
        let waiting_pause_gid = gid(3);
        let active_pause_gid = gid(4);
        let terminal_gid = gid(5);
        let waiting_gid = gid(10);
        let paused_gid = gid(11);

        let active_wait = recovered_journal(active_wait_gid, task_id(1), journal_id(1), Vec::new());
        let demoted_pause =
            recovered_journal(demoted_pause_gid, task_id(2), journal_id(2), Vec::new());
        let waiting_pause =
            recovered_journal(waiting_pause_gid, task_id(3), journal_id(3), Vec::new());
        let active_pause =
            recovered_journal(active_pause_gid, task_id(4), journal_id(4), Vec::new());
        let terminal = completed_journal(terminal_gid, task_id(5), journal_id(5));
        let waiting = recovered_journal(waiting_gid, task_id(10), journal_id(10), Vec::new());
        let paused = recovered_journal(paused_gid, task_id(11), journal_id(11), Vec::new());

        let active_wait_task = task_record(
            active_wait_gid,
            SessionQueueState::Active,
            0,
            active_wait.journal_id,
            journal_snapshot_hash(&active_wait),
        );
        let mut active_pause_task = task_record(
            active_pause_gid,
            SessionQueueState::Active,
            1,
            active_pause.journal_id,
            journal_snapshot_hash(&active_pause),
        );
        active_pause_task.desired_paused = true;
        let waiting_task = task_record(
            waiting_gid,
            SessionQueueState::Waiting,
            0,
            waiting.journal_id,
            journal_snapshot_hash(&waiting),
        );
        let mut waiting_pause_task = task_record(
            waiting_pause_gid,
            SessionQueueState::Waiting,
            1,
            waiting_pause.journal_id,
            journal_snapshot_hash(&waiting_pause),
        );
        waiting_pause_task.desired_paused = true;
        let terminal_task = task_record(
            terminal_gid,
            SessionQueueState::Waiting,
            2,
            terminal.journal_id,
            journal_snapshot_hash(&terminal),
        );
        let paused_task = task_record(
            paused_gid,
            SessionQueueState::Paused,
            0,
            paused.journal_id,
            journal_snapshot_hash(&paused),
        );
        let mut demoted_pause_task = task_record(
            demoted_pause_gid,
            SessionQueueState::Demoted,
            0,
            demoted_pause.journal_id,
            journal_snapshot_hash(&demoted_pause),
        );
        demoted_pause_task.desired_paused = true;
        demoted_pause_task.slow_demotion_count = 1;
        demoted_pause_task.slow_slot = Some(SessionSlowSlotState {
            original_position: 0,
            retry: Some(SessionSlowRetryDecision {
                scheduled_at_ms: 1_000,
                delay_ms: 4_000,
            }),
        });

        let startup = reconcile_and_restore(
            snapshot(vec![
                paused_task,
                terminal_task,
                active_pause_task,
                waiting_pause_task,
                demoted_pause_task,
                waiting_task,
                active_wait_task,
            ]),
            vec![
                terminal,
                paused,
                waiting,
                active_pause,
                waiting_pause,
                demoted_pause,
                active_wait,
            ],
            config(now),
        )
        .expect("mixed queue reconciliation");

        assert_eq!(
            startup.scheduler.queue_snapshot(QueueClass::Waiting),
            vec![waiting_gid, active_wait_gid]
        );
        assert_eq!(
            startup.scheduler.queue_snapshot(QueueClass::Paused),
            vec![
                paused_gid,
                waiting_pause_gid,
                active_pause_gid,
                demoted_pause_gid,
            ]
        );
        assert_eq!(
            startup.scheduler.queue_snapshot(QueueClass::Stopped),
            vec![terminal_gid]
        );
        assert_eq!(startup.queue_session_repairs.len(), 4);
        assert_eq!(
            startup.queue_session_repairs[0]
                .final_orders
                .iter()
                .find(|order| order.state == SessionQueueState::Waiting)
                .expect("first waiting order")
                .gids,
            vec![
                waiting_gid,
                active_wait_gid,
                waiting_pause_gid,
                terminal_gid
            ]
        );
        assert_eq!(
            startup.queue_session_repairs[3]
                .final_orders
                .iter()
                .find(|order| order.state == SessionQueueState::Paused)
                .expect("final paused order")
                .gids,
            vec![
                paused_gid,
                waiting_pause_gid,
                active_pause_gid,
                demoted_pause_gid,
            ]
        );
        assert_eq!(startup.terminal_session_repairs.len(), 1);
        let terminal_repair = &startup.terminal_session_repairs[0];
        assert_eq!(terminal_repair.result.gid, terminal_gid);
        assert_eq!(
            terminal_repair
                .transition
                .final_orders
                .iter()
                .find(|order| order.state == SessionQueueState::Waiting)
                .expect("terminal source order")
                .gids,
            vec![waiting_gid, active_wait_gid]
        );
    }

    #[test]
    fn rejects_missing_extra_and_duplicate_journal_identities() {
        let now = MonotonicInstant::now();
        let first = recovered_journal(gid(1), task_id(1), journal_id(1), Vec::new());
        let task = task_record(
            gid(1),
            SessionQueueState::Waiting,
            0,
            first.journal_id,
            journal_snapshot_hash(&first),
        );
        assert_eq!(
            reconcile_startup(snapshot(vec![task.clone()]), Vec::new(), config(now)),
            Err(StartupRecoveryError::MissingJournal(gid(1)))
        );

        let extra = recovered_journal(gid(2), task_id(2), journal_id(2), Vec::new());
        assert_eq!(
            reconcile_startup(
                snapshot(vec![task.clone()]),
                vec![first.clone(), extra],
                config(now)
            ),
            Err(StartupRecoveryError::ExtraJournal(gid(2)))
        );

        let duplicate_id = recovered_journal(gid(2), task_id(2), journal_id(1), Vec::new());
        let second_task = task_record(
            gid(2),
            SessionQueueState::Waiting,
            1,
            duplicate_id.journal_id,
            journal_snapshot_hash(&duplicate_id),
        );
        assert!(matches!(
            reconcile_startup(
                snapshot(vec![task, second_task]),
                vec![first, duplicate_id],
                config(now)
            ),
            Err(StartupRecoveryError::DuplicateJournalId(_))
        ));
    }

    #[test]
    fn rejects_non_dense_queue_and_terminal_result_mismatch() {
        let now = MonotonicInstant::now();
        let journal = recovered_journal(gid(1), task_id(1), journal_id(1), Vec::new());
        let sparse = task_record(
            gid(1),
            SessionQueueState::Waiting,
            1,
            journal.journal_id,
            journal_snapshot_hash(&journal),
        );
        assert!(matches!(
            reconcile_startup(snapshot(vec![sparse]), vec![journal.clone()], config(now)),
            Err(StartupRecoveryError::QueueNotDense { .. })
        ));

        let stopped = task_record(
            gid(1),
            SessionQueueState::Stopped,
            0,
            journal.journal_id,
            journal_snapshot_hash(&journal),
        );
        let mut stopped_snapshot = snapshot(vec![stopped]);
        stopped_snapshot
            .stopped_results
            .push(SessionStoppedResultRecord {
                gid: gid(1),
                status: SessionTerminalStatus::Complete,
                error_kind: None,
                safe_message: String::new(),
                total_length: None,
                layout_hash: None,
                completed_ms: 2_000,
            });
        assert_eq!(
            reconcile_startup(stopped_snapshot, vec![journal], config(now)),
            Err(StartupRecoveryError::TerminalQueueMismatch(gid(1)))
        );
    }

    #[test]
    fn rejects_host_key_state_mismatch_and_invalid_deadline() {
        let now = MonotonicInstant::now();
        let host_gid = gid(8);
        let host = recovered_journal(host_gid, task_id(8), journal_id(8), Vec::new());
        let host_task = task_record(
            host_gid,
            SessionQueueState::Paused,
            0,
            host.journal_id,
            journal_snapshot_hash(&host),
        );
        let key = vec![3_u8; 32];
        let mut host_snapshot = snapshot(vec![host_task]);
        host_snapshot
            .host_key_challenges
            .push(SessionHostKeyChallengeRecord {
                gid: host_gid,
                challenge_id: HostKeyChallengeId::new([8; 16]),
                canonical_host: "example.test".to_owned(),
                port: 22,
                algorithm: "ssh-ed25519".to_owned(),
                fingerprint_sha256: HostKeyFingerprint::for_presented_key(&key),
                presented_public_key: key,
                created_ms: 1_000,
            });
        assert_eq!(
            reconcile_startup(host_snapshot, vec![host], config(now)),
            Err(StartupRecoveryError::HostKeyChallengeMismatch(host_gid))
        );

        let no_space_gid = gid(9);
        let journal = recovered_journal(no_space_gid, task_id(9), journal_id(9), Vec::new());
        let mut task = task_record(
            no_space_gid,
            SessionQueueState::Waiting,
            0,
            journal.journal_id,
            journal_snapshot_hash(&journal),
        );
        task.no_space = Some(SessionNoSpaceCondition {
            target: path("/outputs/no-space"),
            scheduled_at_ms: 1_000,
            delay_ms: 0,
        });
        assert_eq!(
            reconcile_startup(snapshot(vec![task]), vec![journal], config(now)),
            Err(StartupRecoveryError::InvalidDeadline {
                gid: no_space_gid,
                kind: StartupDeadlineKind::NoSpace,
                source: ariax_core::PersistedDelayError::ZeroDelay,
            })
        );
    }

    #[test]
    fn journal_install_recovery_binds_checkpoint_evidence_and_orders_appender_opening() {
        let now = MonotonicInstant::now();
        let installing_gid = gid(20);
        let old_id = journal_id(20);
        let new_id = journal_id(21);
        let installing = recovered_journal(installing_gid, task_id(20), old_id, Vec::new());
        let mut installing_task = task_record(
            installing_gid,
            SessionQueueState::Waiting,
            0,
            old_id,
            journal_snapshot_hash(&installing),
        );
        installing_task.primary_journal_path = path("/journals/old");
        let installing_intent = install_intent(
            installing_gid,
            JournalInstallPhase::Installing,
            old_id,
            new_id,
            1,
        );
        let mut installing_snapshot = snapshot(vec![installing_task]);
        installing_snapshot
            .journal_installs
            .push(installing_intent.clone());
        let installing_startup =
            reconcile_and_restore(installing_snapshot, vec![installing], config(now))
                .expect("installing recovery");
        assert!(installing_startup.appender_recoveries.is_empty());
        assert_eq!(installing_startup.journal_install_recoveries.len(), 1);
        let deferred = &installing_startup.journal_install_recoveries[0];
        assert_eq!(deferred.task_id.get(), 1);
        assert_eq!(deferred.intent, installing_intent);
        assert_eq!(deferred.authoritative_journal_id, old_id);
        assert_eq!(deferred.authoritative_last_sequence, 2);
        assert!(
            deferred.authoritative_last_sequence > deferred.intent.source_last_sequence,
            "native recovery must see that the frozen candidate cannot replace the later old prefix"
        );
        assert_eq!(deferred.authoritative_checkpoint, None);

        let installed_gid = gid(22);
        let installed_old_id = journal_id(22);
        let installed_new_id = journal_id(23);
        let installed = recovered_checkpoint_journal(
            installed_gid,
            task_id(22),
            installed_new_id,
            checkpoint_id(42),
            77,
        );
        let mut installed_task = task_record(
            installed_gid,
            SessionQueueState::Waiting,
            0,
            installed_new_id,
            journal_snapshot_hash(&installed),
        );
        installed_task.primary_journal_path = path("/journals/new");
        let installed_intent = install_intent(
            installed_gid,
            JournalInstallPhase::Installed,
            installed_old_id,
            installed_new_id,
            77,
        );
        let mut installed_snapshot = snapshot(vec![installed_task]);
        installed_snapshot
            .journal_installs
            .push(installed_intent.clone());
        let installed_startup =
            reconcile_and_restore(installed_snapshot, vec![installed], config(now))
                .expect("installed recovery");
        assert_eq!(installed_startup.appender_recoveries.len(), 1);
        assert_eq!(installed_startup.journal_install_recoveries.len(), 1);
        let deferred = &installed_startup.journal_install_recoveries[0];
        assert_eq!(deferred.intent, installed_intent);
        assert_eq!(deferred.authoritative_journal_id, installed_new_id);
        assert_eq!(deferred.authoritative_last_sequence, 4);
        assert_eq!(
            deferred
                .authoritative_checkpoint
                .as_ref()
                .expect("installed checkpoint")
                .source_last_sequence,
            77
        );
    }

    #[test]
    fn rejects_stale_install_path_sequence_and_checkpoint_evidence() {
        let now = MonotonicInstant::now();
        let task_gid = gid(24);
        let old_id = journal_id(24);
        let new_id = journal_id(25);
        let journal = recovered_journal(task_gid, task_id(24), old_id, Vec::new());
        let mut task = task_record(
            task_gid,
            SessionQueueState::Waiting,
            0,
            old_id,
            journal_snapshot_hash(&journal),
        );
        task.primary_journal_path = path("/journals/stale");
        let mut stale_path_snapshot = snapshot(vec![task]);
        stale_path_snapshot.journal_installs.push(install_intent(
            task_gid,
            JournalInstallPhase::Installing,
            old_id,
            new_id,
            2,
        ));
        assert_eq!(
            reconcile_startup(stale_path_snapshot, vec![journal.clone()], config(now)),
            Err(StartupRecoveryError::JournalInstallMismatch(task_gid))
        );

        let mut task = task_record(
            task_gid,
            SessionQueueState::Waiting,
            0,
            old_id,
            journal_snapshot_hash(&journal),
        );
        task.primary_journal_path = path("/journals/old");
        let mut source_ahead_snapshot = snapshot(vec![task]);
        source_ahead_snapshot.journal_installs.push(install_intent(
            task_gid,
            JournalInstallPhase::Installing,
            old_id,
            new_id,
            3,
        ));
        assert_eq!(
            reconcile_startup(source_ahead_snapshot, vec![journal], config(now)),
            Err(StartupRecoveryError::JournalInstallSourceAhead(task_gid))
        );

        let installed =
            recovered_checkpoint_journal(task_gid, task_id(24), new_id, checkpoint_id(42), 77);
        let mut task = task_record(
            task_gid,
            SessionQueueState::Waiting,
            0,
            new_id,
            journal_snapshot_hash(&installed),
        );
        task.primary_journal_path = path("/journals/new");
        let mut checkpoint_mismatch_snapshot = snapshot(vec![task]);
        checkpoint_mismatch_snapshot
            .journal_installs
            .push(install_intent(
                task_gid,
                JournalInstallPhase::Installed,
                old_id,
                new_id,
                78,
            ));
        assert_eq!(
            reconcile_startup(checkpoint_mismatch_snapshot, vec![installed], config(now)),
            Err(StartupRecoveryError::InstalledCheckpointMismatch(task_gid))
        );

        let installed =
            recovered_checkpoint_journal(task_gid, task_id(24), new_id, checkpoint_id(42), 77);
        let mut task = task_record(
            task_gid,
            SessionQueueState::Waiting,
            0,
            new_id,
            journal_snapshot_hash(&installed),
        );
        task.primary_journal_path = path("/journals/new");
        let mut wrong_checkpoint_id =
            install_intent(task_gid, JournalInstallPhase::Installed, old_id, new_id, 77);
        wrong_checkpoint_id.checkpoint_id = checkpoint_id(43);
        let mut checkpoint_id_mismatch_snapshot = snapshot(vec![task]);
        checkpoint_id_mismatch_snapshot
            .journal_installs
            .push(wrong_checkpoint_id);
        assert_eq!(
            reconcile_startup(
                checkpoint_id_mismatch_snapshot,
                vec![installed],
                config(now)
            ),
            Err(StartupRecoveryError::InstalledCheckpointMismatch(task_gid))
        );
    }

    #[test]
    fn rejects_demoted_task_without_representable_slow_decision() {
        let now = MonotonicInstant::now();
        let gid = gid(10);
        let journal = recovered_journal(gid, task_id(10), journal_id(10), Vec::new());
        let mut task = task_record(
            gid,
            SessionQueueState::Demoted,
            0,
            journal.journal_id,
            journal_snapshot_hash(&journal),
        );
        task.slow_demotion_count = 1;
        task.slow_slot = Some(SessionSlowSlotState {
            original_position: 0,
            retry: None,
        });
        assert_eq!(
            reconcile_startup(snapshot(vec![task]), vec![journal], config(now)),
            Err(StartupRecoveryError::MissingSlowDeadline(gid))
        );
    }
}
