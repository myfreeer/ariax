use crate::http_connector::{
    HttpDestinationError, HttpDestinationPolicy, resolve_http_destination,
};
use crate::http_transport::{
    HttpDirectTransport, HttpDirectTransportConfig, HttpResponseLease, HttpTransportError,
};
use crate::{
    ActiveTransferRequest, AllocationRequest, LeaseCommit, LeaseWritePlan, RuntimeEffectHandle,
    RuntimeEventSubmission, RuntimeEventSubmitError, StorageEngine, StorageEngineConfig,
    StorageEngineError, WriteBlock,
};
use ariax_core::{
    ErrorKind, FileId, Generation, Gid, LeaseId, MonotonicInstant, PieceId, PublicError,
    RetryClass, TaskId, TransferAttemptId,
};
use ariax_runtime::{OwnerTag, SizeClass};
use ariax_storage::{
    ControlJournalAppender, DurabilityMode, FileEntry, FileIdentity, FileLayout,
    JournalContributor, JournalDigest, JournalDigestAlgorithm, JournalDirectoryCapability,
    JournalFileLayoutEntry, JournalHash, JournalId, JournalPayload, JournalRelativePath,
    JournalStateLimits, JournalStateReplay, JournalStateStop, LayoutError, LeaseAbortReason,
    NativeCapabilityError, OptionsSnapshotScope, PayloadCodecError, PlatformPath,
    RecoveredHttpStrongValidator, ReplayLimits, RootBinding, RootBindingError,
    RootDirectoryCapability, RootFileCapability, RootIdentity, SafeRelativePath,
    SanitizedOptionMap, calculate_http_strong_validator_fingerprint,
    calculate_validator_set_fingerprint, recover_journal_state,
};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper::body::Incoming;
use hyper::header::{
    ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HOST, IF_RANGE,
    LAST_MODIFIED, RANGE, TRANSFER_ENCODING,
};
use hyper::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::watch;
use tokio::time::timeout;

pub(crate) const MAX_RESPONSE_HEADERS: usize = 128;
pub(crate) const MAX_RESPONSE_HEAD_BYTES: usize = 64 * 1024;
const HTTP_METADATA_VALIDATOR_HASH_DOMAIN: &str = "ariax/http-metadata-validator/v1\0";
const HTTP_RESOURCE_HASH_DOMAIN: &str = "ariax/http-resource/v1\0";
const RECOVERY_READ_BUFFER_BYTES: usize = 1024 * 1024;

/// Explicit cancellation authority shared with one HTTP response worker.
#[derive(Clone, Debug)]
pub struct HttpCancellation {
    sender: Arc<watch::Sender<bool>>,
}

impl HttpCancellation {
    #[must_use]
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self {
            sender: Arc::new(sender),
        }
    }

    pub fn cancel(&self) {
        let _previous = self.sender.send_replace(true);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        let _changed = receiver.changed().await;
    }
}

impl Default for HttpCancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// One caller-approved, resolve-and-pinned first-slice transfer.
///
/// `peer` is the numeric destination after external DNS/SSRF policy. The
/// original URI authority remains the HTTP `Host`; this module never resolves
/// or reinterprets the authority itself.
#[derive(Clone, Debug)]
pub struct KnownLengthHttpRequest {
    pub task: TaskId,
    pub gid: Gid,
    pub generation: Generation,
    pub journal_id: JournalId,
    pub uri: String,
    pub peer: SocketAddr,
    pub output_root: PathBuf,
    pub output: SafeRelativePath,
    pub journal_directory: PathBuf,
    pub piece_length: u64,
    pub created_at_unix_ms: u64,
    pub connect_timeout: Duration,
    pub response_head_timeout: Duration,
    pub response_body_timeout: Duration,
    pub storage: StorageEngineConfig,
    pub cancellation: HttpCancellation,
}

/// Durable result returned only after exact response framing and journal flush.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KnownLengthHttpResult {
    pub content_length: u64,
    pub resumed_from: u64,
    pub durable_piece_count: u64,
    pub terminal_sequence: u64,
    pub validator_fingerprint: JournalHash,
    pub final_digest: JournalDigest,
}

/// Descriptor-revalidated recovery report for a standalone first-slice journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KnownLengthHttpRecovery {
    pub replay: JournalStateReplay,
    pub durable_prefix: u64,
    pub strong_validator: Option<RecoveredHttpStrongValidator>,
}

/// Exact identities, roots, and budgets required to recover one standalone
/// first-slice transfer without reopening an untrusted persisted path.
#[derive(Clone, Debug)]
pub struct KnownLengthHttpRecoveryRequest {
    pub task: TaskId,
    pub gid: Gid,
    pub journal_id: JournalId,
    pub generation: Generation,
    pub journal_directory: PathBuf,
    pub output_root: PathBuf,
    pub replay_limits: ReplayLimits,
    pub state_limits: JournalStateLimits,
}

/// One strict continuation of a recovered fresh-download journal. Layout,
/// output identity, piece length, and validator authority come only from
/// replay; the caller supplies a newly policy-approved numeric peer.
#[derive(Clone, Debug)]
pub struct KnownLengthHttpResumeRequest {
    pub recovery: KnownLengthHttpRecoveryRequest,
    pub uri: String,
    pub peer: SocketAddr,
    pub resumed_at_unix_ms: u64,
    pub connect_timeout: Duration,
    pub response_head_timeout: Duration,
    pub response_body_timeout: Duration,
    pub storage: StorageEngineConfig,
    pub cancellation: HttpCancellation,
}

/// Fresh or recovered HTTP work bound to one scheduler allocation authority.
#[derive(Clone, Debug)]
pub enum KnownLengthHttpTransfer {
    Fresh(KnownLengthHttpRequest),
    Resume(KnownLengthHttpResumeRequest),
}

impl KnownLengthHttpTransfer {
    fn identity(&self) -> (TaskId, Gid, Generation) {
        match self {
            Self::Fresh(request) => (request.task, request.gid, request.generation),
            Self::Resume(request) => (
                request.recovery.task,
                request.recovery.gid,
                request.recovery.generation,
            ),
        }
    }

    fn cancellation(&self) -> HttpCancellation {
        match self {
            Self::Fresh(request) => request.cancellation.clone(),
            Self::Resume(request) => request.cancellation.clone(),
        }
    }
}

#[derive(Debug)]
pub enum KnownLengthHttpRuntimeError {
    AllocationIdentityMismatch,
    RuntimeEffectClosed,
    Transfer(KnownLengthHttpError),
}

impl fmt::Display for KnownLengthHttpRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllocationIdentityMismatch => {
                formatter.write_str("HTTP allocation identity does not match transfer")
            }
            Self::RuntimeEffectClosed => formatter.write_str("runtime effect mailbox is closed"),
            Self::Transfer(error) => error.fmt(formatter),
        }
    }
}

impl Error for KnownLengthHttpRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transfer(error) => Some(error),
            _ => None,
        }
    }
}

/// Stable failure classes exposed to scheduler/retry integration.
#[derive(Debug)]
pub enum KnownLengthHttpError {
    InvalidUri,
    UnsupportedScheme,
    MissingAuthority,
    UserInfoForbidden,
    PeerPortMismatch { expected: u16, actual: u16 },
    Destination(HttpDestinationError),
    Transport(HttpTransportError),
    ZeroPieceLength,
    ConnectTimeout,
    Connect(std::io::Error),
    HandshakeTimeout,
    Hyper(hyper::Error),
    Request(hyper::http::Error),
    ResponseHeadTimeout,
    UnexpectedStatus(StatusCode),
    TransferEncoding,
    MissingContentLength,
    DuplicateContentLength,
    InvalidContentLength,
    ContentEncoding,
    InvalidValidator,
    MissingStrongValidator,
    ResumeResourceMismatch,
    NoDurablePrefix,
    AlreadyComplete,
    RangeIgnored,
    RangeNotSatisfiable,
    MissingContentRange,
    DuplicateContentRange,
    InvalidContentRange,
    RangeLengthMismatch,
    StaleValidator,
    ExistingLengthMismatch { expected: u64, actual: u64 },
    DurablePieceDigestMismatch { piece: PieceId },
    OversizedBody,
    ShortBody { expected: u64, actual: u64 },
    ResponseBodyTimeout,
    Cancelled,
    Native(NativeCapabilityError),
    RootBinding(RootBindingError),
    Layout(LayoutError),
    Payload(PayloadCodecError),
    Journal(ariax_storage::JournalAppenderError),
    Storage(StorageEngineError),
    RecoveryState,
    Runtime(std::io::Error),
    RuntimeEffectClosed,
}

impl KnownLengthHttpError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidUri => "invalid_uri",
            Self::UnsupportedScheme => "unsupported_scheme",
            Self::MissingAuthority => "missing_authority",
            Self::UserInfoForbidden => "uri_userinfo_forbidden",
            Self::PeerPortMismatch { .. } => "peer_port_mismatch",
            Self::Destination(error) => error.code(),
            Self::Transport(error) => error.code(),
            Self::ZeroPieceLength => "zero_piece_length",
            Self::ConnectTimeout => "connect_timeout",
            Self::Connect(_) => "connect",
            Self::HandshakeTimeout => "handshake_timeout",
            Self::Hyper(_) => "http_protocol",
            Self::Request(_) => "request_build",
            Self::ResponseHeadTimeout => "response_head_timeout",
            Self::UnexpectedStatus(_) => "unexpected_status",
            Self::TransferEncoding => "transfer_encoding",
            Self::MissingContentLength => "missing_content_length",
            Self::DuplicateContentLength => "duplicate_content_length",
            Self::InvalidContentLength => "invalid_content_length",
            Self::ContentEncoding => "content_encoding",
            Self::InvalidValidator => "invalid_validator",
            Self::MissingStrongValidator => "missing_strong_validator",
            Self::ResumeResourceMismatch => "resume_resource_mismatch",
            Self::NoDurablePrefix => "no_durable_prefix",
            Self::AlreadyComplete => "already_complete",
            Self::RangeIgnored => "range_ignored",
            Self::RangeNotSatisfiable => "range_not_satisfiable",
            Self::MissingContentRange => "missing_content_range",
            Self::DuplicateContentRange => "duplicate_content_range",
            Self::InvalidContentRange => "invalid_content_range",
            Self::RangeLengthMismatch => "range_length_mismatch",
            Self::StaleValidator => "stale_validator",
            Self::ExistingLengthMismatch { .. } => "existing_length_mismatch",
            Self::DurablePieceDigestMismatch { .. } => "durable_piece_digest_mismatch",
            Self::OversizedBody => "oversized_body",
            Self::ShortBody { .. } => "short_body",
            Self::ResponseBodyTimeout => "response_body_timeout",
            Self::Cancelled => "cancelled",
            Self::Native(_) => "native_file",
            Self::RootBinding(_) => "root_binding",
            Self::Layout(_) => "layout",
            Self::Payload(_) => "journal_payload",
            Self::Journal(_) => "journal",
            Self::Storage(error) => error.reject().code(),
            Self::RecoveryState => "recovery_state",
            Self::Runtime(_) => "runtime",
            Self::RuntimeEffectClosed => "runtime_effect_closed",
        }
    }

    #[must_use]
    pub const fn retriable(&self) -> bool {
        match self {
            Self::Destination(error) => error.retriable(),
            Self::Transport(error) => error.retriable(),
            Self::ConnectTimeout
            | Self::Connect(_)
            | Self::HandshakeTimeout
            | Self::Hyper(_)
            | Self::ResponseHeadTimeout
            | Self::ShortBody { .. }
            | Self::ResponseBodyTimeout => true,
            _ => false,
        }
    }
}

impl fmt::Display for KnownLengthHttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerPortMismatch { expected, actual } => {
                write!(
                    formatter,
                    "pinned peer port {actual} does not match URI port {expected}"
                )
            }
            Self::Connect(error) => write!(formatter, "HTTP connect failed: {error}"),
            Self::Hyper(error) => write!(formatter, "HTTP protocol failed: {error}"),
            Self::Request(error) => write!(formatter, "HTTP request build failed: {error}"),
            Self::UnexpectedStatus(status) => write!(formatter, "HTTP status {status} is not 200"),
            Self::ShortBody { expected, actual } => {
                write!(formatter, "HTTP body ended at {actual} of {expected} bytes")
            }
            Self::Destination(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::ExistingLengthMismatch { expected, actual } => write!(
                formatter,
                "existing output length {actual} does not match recovered length {expected}"
            ),
            Self::DurablePieceDigestMismatch { piece } => {
                write!(
                    formatter,
                    "recovered durable piece {} failed readback",
                    piece.get()
                )
            }
            Self::Native(error) => write!(formatter, "native output failed: {error}"),
            Self::RootBinding(error) => write!(formatter, "root binding failed: {error}"),
            Self::Layout(error) => write!(formatter, "file layout failed: {error}"),
            Self::Payload(error) => write!(formatter, "journal payload failed: {error}"),
            Self::Journal(error) => write!(formatter, "journal failed: {error}"),
            Self::Storage(error) => error.fmt(formatter),
            Self::Runtime(error) => write!(formatter, "HTTP runtime failed: {error}"),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for KnownLengthHttpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect(error) | Self::Runtime(error) => Some(error),
            Self::Destination(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Hyper(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::Native(error) => Some(error),
            Self::RootBinding(error) => Some(error),
            Self::Layout(error) => Some(error),
            Self::Payload(error) => Some(error),
            Self::Journal(error) => Some(error),
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<NativeCapabilityError> for KnownLengthHttpError {
    fn from(error: NativeCapabilityError) -> Self {
        Self::Native(error)
    }
}

impl From<HttpDestinationError> for KnownLengthHttpError {
    fn from(error: HttpDestinationError) -> Self {
        Self::Destination(error)
    }
}

impl From<HttpTransportError> for KnownLengthHttpError {
    fn from(error: HttpTransportError) -> Self {
        match error {
            HttpTransportError::Destination(error) => Self::Destination(error),
            error => Self::Transport(error),
        }
    }
}

impl From<RootBindingError> for KnownLengthHttpError {
    fn from(error: RootBindingError) -> Self {
        Self::RootBinding(error)
    }
}

impl From<LayoutError> for KnownLengthHttpError {
    fn from(error: LayoutError) -> Self {
        Self::Layout(error)
    }
}

impl From<PayloadCodecError> for KnownLengthHttpError {
    fn from(error: PayloadCodecError) -> Self {
        Self::Payload(error)
    }
}

impl From<ariax_storage::JournalAppenderError> for KnownLengthHttpError {
    fn from(error: ariax_storage::JournalAppenderError) -> Self {
        Self::Journal(error)
    }
}

impl From<StorageEngineError> for KnownLengthHttpError {
    fn from(error: StorageEngineError) -> Self {
        Self::Storage(error)
    }
}

struct RuntimeHttpLifecycle {
    runtime: RuntimeEffectHandle,
    allocation: Option<AllocationRequest>,
    active: Option<ActiveTransferRequest>,
}

impl RuntimeHttpLifecycle {
    async fn activate(&mut self) -> Result<(), KnownLengthHttpError> {
        if self.active.is_some() {
            return Ok(());
        }
        let allocation = self
            .allocation
            .take()
            .ok_or(KnownLengthHttpError::RuntimeEffectClosed)?;
        let (submission, active) = allocation.activate();
        submit_runtime_event(&self.runtime, submission)
            .await
            .map_err(|_| KnownLengthHttpError::RuntimeEffectClosed)?;
        self.active = Some(active);
        Ok(())
    }
}

/// Executes one exact scheduler-issued allocation through the HTTP worker and
/// returns every lifecycle transition through the bounded runtime mailbox.
pub async fn run_known_length_http_runtime(
    runtime: RuntimeEffectHandle,
    allocation: AllocationRequest,
    transfer: KnownLengthHttpTransfer,
    retry_at: MonotonicInstant,
) -> Result<KnownLengthHttpResult, KnownLengthHttpRuntimeError> {
    run_known_length_http_runtime_with_policy(runtime, allocation, transfer, None, retry_at).await
}

async fn run_known_length_http_runtime_with_policy(
    runtime: RuntimeEffectHandle,
    allocation: AllocationRequest,
    transfer: KnownLengthHttpTransfer,
    policy: Option<HttpDestinationPolicy>,
    retry_at: MonotonicInstant,
) -> Result<KnownLengthHttpResult, KnownLengthHttpRuntimeError> {
    let identity = transfer.identity();
    if (
        allocation.task_id(),
        allocation.gid(),
        allocation.generation(),
    ) != identity
    {
        return Err(KnownLengthHttpRuntimeError::AllocationIdentityMismatch);
    }
    let cancellation = transfer.cancellation();
    let mut lifecycle = RuntimeHttpLifecycle {
        runtime: runtime.clone(),
        allocation: Some(allocation),
        active: None,
    };
    let (result, cancellation_request) = {
        let transfer = async {
            match transfer {
                KnownLengthHttpTransfer::Fresh(request) => {
                    let transport = if let Some(policy) = policy {
                        resolve_http_destination(&request.uri, policy).await?;
                        Some(HttpDirectTransport::resolved(
                            &request.uri,
                            transport_config(
                                policy,
                                request.connect_timeout,
                                request.response_head_timeout,
                            ),
                        )?)
                    } else {
                        None
                    };
                    download_known_length_http_inner(request, Some(&mut lifecycle), transport).await
                }
                KnownLengthHttpTransfer::Resume(request) => {
                    let transport = if let Some(policy) = policy {
                        resolve_http_destination(&request.uri, policy).await?;
                        Some(HttpDirectTransport::resolved(
                            &request.uri,
                            transport_config(
                                policy,
                                request.connect_timeout,
                                request.response_head_timeout,
                            ),
                        )?)
                    } else {
                        None
                    };
                    resume_known_length_http_inner(request, Some(&mut lifecycle), transport).await
                }
            }
        };
        tokio::pin!(transfer);
        if let Some(cancellation_request) =
            runtime.take_cancellation_for(identity.0, identity.1, identity.2)
        {
            cancellation.cancel();
            let result = (&mut transfer).await;
            (result, Some(cancellation_request))
        } else {
            let cancellation_request = wait_for_runtime_cancellation(runtime.clone(), identity);
            tokio::pin!(cancellation_request);
            tokio::select! {
                result = &mut transfer => (result, None),
                cancellation_request = &mut cancellation_request => {
                    cancellation.cancel();
                    let result = (&mut transfer).await;
                    (result, Some(cancellation_request))
                }
            }
        }
    };
    if let Some(cancellation_request) = cancellation_request {
        submit_runtime_event(&runtime, cancellation_request.drained())
            .await
            .map_err(|_| KnownLengthHttpRuntimeError::RuntimeEffectClosed)?;
        return result.map_err(KnownLengthHttpRuntimeError::Transfer);
    }

    match result {
        Ok(result) => {
            let active = lifecycle
                .active
                .take()
                .ok_or(KnownLengthHttpRuntimeError::RuntimeEffectClosed)?;
            let (data_complete, verifying) = active.data_complete(false);
            submit_runtime_event(&runtime, data_complete)
                .await
                .map_err(|_| KnownLengthHttpRuntimeError::RuntimeEffectClosed)?;
            submit_runtime_event(&runtime, verifying.succeeded())
                .await
                .map_err(|_| KnownLengthHttpRuntimeError::RuntimeEffectClosed)?;
            Ok(result)
        }
        Err(KnownLengthHttpError::RuntimeEffectClosed) => {
            Err(KnownLengthHttpRuntimeError::RuntimeEffectClosed)
        }
        Err(error) => {
            let retriable = error.retriable();
            let public = public_http_error(&error);
            let submission = if let Some(active) = lifecycle.active.take() {
                if retriable {
                    active.retryable(retry_at)
                } else {
                    active.failed(public)
                }
            } else {
                let allocation = lifecycle
                    .allocation
                    .take()
                    .ok_or(KnownLengthHttpRuntimeError::RuntimeEffectClosed)?;
                if retriable {
                    allocation.retryable(retry_at)
                } else {
                    allocation.failed(public)
                }
            };
            submit_runtime_event(&runtime, submission)
                .await
                .map_err(|_| KnownLengthHttpRuntimeError::RuntimeEffectClosed)?;
            Err(KnownLengthHttpRuntimeError::Transfer(error))
        }
    }
}

/// Resolves the transfer destination before entering the bounded scheduler
/// lifecycle adapter. The resulting worker still preserves the original URI
/// authority for `Host` and uses only the admitted numeric peer for connect.
pub async fn run_known_length_http_runtime_resolved(
    runtime: RuntimeEffectHandle,
    allocation: AllocationRequest,
    transfer: KnownLengthHttpTransfer,
    policy: HttpDestinationPolicy,
    retry_at: MonotonicInstant,
) -> Result<KnownLengthHttpResult, KnownLengthHttpRuntimeError> {
    run_known_length_http_runtime_with_policy(runtime, allocation, transfer, Some(policy), retry_at)
        .await
}

pub fn run_known_length_http_runtime_blocking(
    runtime: RuntimeEffectHandle,
    allocation: AllocationRequest,
    transfer: KnownLengthHttpTransfer,
    retry_at: MonotonicInstant,
) -> Result<KnownLengthHttpResult, KnownLengthHttpRuntimeError> {
    RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|error| {
            KnownLengthHttpRuntimeError::Transfer(KnownLengthHttpError::Runtime(error))
        })?
        .block_on(run_known_length_http_runtime(
            runtime, allocation, transfer, retry_at,
        ))
}

/// Blocking wrapper for [`run_known_length_http_runtime_resolved`].
pub fn run_known_length_http_runtime_resolved_blocking(
    runtime: RuntimeEffectHandle,
    allocation: AllocationRequest,
    transfer: KnownLengthHttpTransfer,
    policy: HttpDestinationPolicy,
    retry_at: MonotonicInstant,
) -> Result<KnownLengthHttpResult, KnownLengthHttpRuntimeError> {
    RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|error| {
            KnownLengthHttpRuntimeError::Transfer(KnownLengthHttpError::Runtime(error))
        })?
        .block_on(run_known_length_http_runtime_resolved(
            runtime, allocation, transfer, policy, retry_at,
        ))
}

async fn wait_for_runtime_cancellation(
    runtime: RuntimeEffectHandle,
    identity: (TaskId, Gid, Generation),
) -> crate::CancellationRequest {
    loop {
        if let Some(request) = runtime.take_cancellation_for(identity.0, identity.1, identity.2) {
            return request;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn submit_runtime_event(
    runtime: &RuntimeEffectHandle,
    mut submission: RuntimeEventSubmission,
) -> Result<(), RuntimeEventSubmitError> {
    loop {
        match runtime.try_submit_event(submission) {
            Ok(()) => return Ok(()),
            Err(rejection) => {
                let error = rejection.error();
                submission = rejection.into_submission();
                match error {
                    RuntimeEventSubmitError::Full => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    RuntimeEventSubmitError::Closed => return Err(error),
                }
            }
        }
    }
}

fn public_http_error(error: &KnownLengthHttpError) -> PublicError {
    let (kind, retry) = match error {
        KnownLengthHttpError::ConnectTimeout
        | KnownLengthHttpError::HandshakeTimeout
        | KnownLengthHttpError::ResponseHeadTimeout
        | KnownLengthHttpError::ResponseBodyTimeout => (ErrorKind::Timeout, RetryClass::SameSource),
        KnownLengthHttpError::Connect(_)
        | KnownLengthHttpError::Hyper(_)
        | KnownLengthHttpError::ShortBody { .. } => (ErrorKind::Network, RetryClass::SameSource),
        KnownLengthHttpError::Transport(
            HttpTransportError::ConnectTimeout
            | HttpTransportError::TlsHandshakeTimeout
            | HttpTransportError::HandshakeTimeout,
        ) => (ErrorKind::Timeout, RetryClass::SameSource),
        KnownLengthHttpError::Transport(error) if error.retriable() => {
            (ErrorKind::Network, RetryClass::SameSource)
        }
        KnownLengthHttpError::Transport(_) => (ErrorKind::Network, RetryClass::Never),
        KnownLengthHttpError::RangeIgnored
        | KnownLengthHttpError::RangeNotSatisfiable
        | KnownLengthHttpError::MissingContentRange
        | KnownLengthHttpError::DuplicateContentRange
        | KnownLengthHttpError::InvalidContentRange
        | KnownLengthHttpError::RangeLengthMismatch => {
            (ErrorKind::InvalidRange, RetryClass::RestartGeneration)
        }
        KnownLengthHttpError::StaleValidator
        | KnownLengthHttpError::MissingStrongValidator
        | KnownLengthHttpError::ResumeResourceMismatch => {
            (ErrorKind::StaleValidator, RetryClass::RestartGeneration)
        }
        KnownLengthHttpError::Cancelled => (ErrorKind::Cancelled, RetryClass::Never),
        KnownLengthHttpError::Storage(_) | KnownLengthHttpError::Native(_) => {
            (ErrorKind::Disk, RetryClass::SameSource)
        }
        KnownLengthHttpError::Journal(_)
        | KnownLengthHttpError::RecoveryState
        | KnownLengthHttpError::ExistingLengthMismatch { .. }
        | KnownLengthHttpError::DurablePieceDigestMismatch { .. } => {
            (ErrorKind::DirtyCheckpoint, RetryClass::RestartGeneration)
        }
        KnownLengthHttpError::OversizedBody => {
            (ErrorKind::ResponseTooLarge, RetryClass::AnotherSource)
        }
        KnownLengthHttpError::Destination(error) if error.retriable() => {
            (ErrorKind::Network, RetryClass::SameSource)
        }
        KnownLengthHttpError::Destination(_) => (ErrorKind::Network, RetryClass::Never),
        _ => (ErrorKind::Network, RetryClass::Never),
    };
    PublicError::new(kind, error.code(), retry)
}

fn transport_config(
    destination: HttpDestinationPolicy,
    connect_timeout: Duration,
    handshake_timeout: Duration,
) -> HttpDirectTransportConfig {
    HttpDirectTransportConfig {
        destination,
        connect_timeout,
        handshake_timeout,
        ..HttpDirectTransportConfig::default()
    }
}

/// Runs one standalone worker on a bounded current-thread Tokio runtime.
pub fn download_known_length_http_blocking(
    request: KnownLengthHttpRequest,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(KnownLengthHttpError::Runtime)?
        .block_on(download_known_length_http(request))
}

/// Blocking wrapper for [`download_known_length_http_resolved`].
pub fn download_known_length_http_resolved_blocking(
    request: KnownLengthHttpRequest,
    policy: HttpDestinationPolicy,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(KnownLengthHttpError::Runtime)?
        .block_on(download_known_length_http_resolved(request, policy))
}

/// Runs one recovered range continuation on a bounded current-thread Tokio
/// runtime.
pub fn resume_known_length_http_blocking(
    request: KnownLengthHttpResumeRequest,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(KnownLengthHttpError::Runtime)?
        .block_on(resume_known_length_http(request))
}

/// Blocking wrapper for [`resume_known_length_http_resolved`].
pub fn resume_known_length_http_resolved_blocking(
    request: KnownLengthHttpResumeRequest,
    policy: HttpDestinationPolicy,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(KnownLengthHttpError::Runtime)?
        .block_on(resume_known_length_http_resolved(request, policy))
}

/// Revalidates one recovered durable prefix, sends an exact open-ended range
/// request with the persisted strong ETag, and continues piece leases without
/// truncating or path-reopening the output after descriptor admission.
pub async fn resume_known_length_http(
    request: KnownLengthHttpResumeRequest,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    resume_known_length_http_inner(request, None, None).await
}

/// Resolves and pins a continuation URI before entering the existing resume
/// worker. Replayed validators and layout state remain authoritative.
/// Runs a recovered range continuation after local destination admission.
pub async fn resume_known_length_http_resolved(
    request: KnownLengthHttpResumeRequest,
    policy: HttpDestinationPolicy,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    resolve_http_destination(&request.uri, policy).await?;
    let transport = HttpDirectTransport::resolved(
        &request.uri,
        transport_config(
            policy,
            request.connect_timeout,
            request.response_head_timeout,
        ),
    )?;
    resume_known_length_http_inner(request, None, Some(transport)).await
}

async fn resume_known_length_http_inner(
    request: KnownLengthHttpResumeRequest,
    mut lifecycle: Option<&mut RuntimeHttpLifecycle>,
    transport: Option<HttpDirectTransport>,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    let uri: Uri = request
        .uri
        .parse()
        .map_err(|_| KnownLengthHttpError::InvalidUri)?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) {
        return Err(KnownLengthHttpError::UnsupportedScheme);
    }
    let authority = uri
        .authority()
        .ok_or(KnownLengthHttpError::MissingAuthority)?;
    if authority.as_str().contains('@') {
        return Err(KnownLengthHttpError::UserInfoForbidden);
    }
    let expected_port = authority
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        });
    if request.peer.port() != expected_port {
        return Err(KnownLengthHttpError::PeerPortMismatch {
            expected: expected_port,
            actual: request.peer.port(),
        });
    }

    let prepared = prepare_known_length_recovery(&request.recovery)?;
    if !matches!(prepared.replay.stop, JournalStateStop::CleanEnd) {
        return Err(KnownLengthHttpError::RecoveryState);
    }
    let state = prepared
        .replay
        .state
        .as_ref()
        .ok_or(KnownLengthHttpError::RecoveryState)?;
    if state.terminal().is_some() {
        return Err(KnownLengthHttpError::AlreadyComplete);
    }
    let validator = prepared
        .strong_validator
        .clone()
        .ok_or(KnownLengthHttpError::MissingStrongValidator)?;
    if validator.resource_fingerprint() != http_resource_fingerprint(&uri) {
        return Err(KnownLengthHttpError::ResumeResourceMismatch);
    }
    let total_length = prepared
        .layout
        .total_length()
        .ok_or(KnownLengthHttpError::RecoveryState)?;
    if validator.total_length() != total_length {
        return Err(KnownLengthHttpError::StaleValidator);
    }
    if prepared.durable_prefix == 0 {
        return Err(KnownLengthHttpError::NoDurablePrefix);
    }
    if prepared.durable_prefix > total_length {
        return Err(KnownLengthHttpError::RecoveryState);
    }
    let final_digest = hash_recovered_durable_prefix(&prepared, &validator)?;

    if prepared.durable_prefix == total_length {
        let PreparedKnownLengthRecovery {
            appender,
            layout,
            output_file,
            durable_prefix,
            durable_piece_count,
            ..
        } = prepared;
        let mut storage = StorageEngine::open_layout(
            layout,
            [(FileId::new(0), output_file)],
            appender,
            request.storage,
        )?;
        if let Some(lifecycle) = lifecycle.as_mut() {
            lifecycle.activate().await?;
        }
        let final_digest = JournalDigest::new(
            JournalDigestAlgorithm::Sha256,
            final_digest.finalize().to_vec(),
        )?;
        let terminal_sequence = storage.complete(
            Some(final_digest.clone()),
            now_unix_ms().unwrap_or(request.resumed_at_unix_ms),
        )?;
        storage.close()?;
        return Ok(KnownLengthHttpResult {
            content_length: total_length,
            resumed_from: durable_prefix,
            durable_piece_count,
            terminal_sequence,
            validator_fingerprint: validator.validator_fingerprint(),
            final_digest,
        });
    }

    let transport = transport.unwrap_or(HttpDirectTransport::pinned(
        &request.uri,
        request.peer,
        transport_config(
            HttpDestinationPolicy::default(),
            request.connect_timeout,
            request.response_head_timeout,
        ),
    )?);
    let range = format!("bytes={}-", prepared.durable_prefix);
    let if_range = hyper::header::HeaderValue::from_bytes(validator.etag())
        .map_err(|_| KnownLengthHttpError::InvalidValidator)?;
    let outbound = Request::builder()
        .method(Method::GET)
        .uri(uri.clone())
        .header(HOST, authority.as_str())
        .header(RANGE, range)
        .header(IF_RANGE, if_range)
        .header(ACCEPT_ENCODING, "identity")
        .body(Empty::<Bytes>::new())
        .map_err(KnownLengthHttpError::Request)?;
    let response = transport.send(outbound).await?;
    let crate::http_transport::HttpTransportResponse { response, lease } = response;
    validate_resume_response_head(&response, prepared.durable_prefix, total_length, &validator)?;

    let PreparedKnownLengthRecovery {
        appender,
        layout,
        output_file,
        durable_prefix,
        durable_piece_count,
        next_transfer_attempt_id,
        next_lease_id,
        ..
    } = prepared;
    let storage = StorageEngine::open_layout(
        layout.clone(),
        [(FileId::new(0), output_file)],
        appender,
        request.storage,
    )?;
    if let Some(lifecycle) = lifecycle.as_mut() {
        lifecycle.activate().await?;
    }
    stream_known_length_body(
        storage,
        response,
        lease,
        BodyTransferPlan {
            task: layout.task(),
            generation: layout.generation(),
            piece_length: layout.piece_length(),
            content_length: total_length,
            start_offset: durable_prefix,
            transfer_attempt: TransferAttemptId::new(next_transfer_attempt_id)
                .ok_or(KnownLengthHttpError::RecoveryState)?,
            next_lease_id,
            validator_fingerprint: validator.validator_fingerprint(),
            durable_piece_count,
            final_digest,
            completed_at_unix_ms: request.resumed_at_unix_ms,
            response_body_timeout: request.response_body_timeout,
            cancellation: request.cancellation,
        },
    )
    .await
}

/// Downloads one identity-coded, known-length HTTP/1.1 response through the
/// concrete `StorageEngine` and strict per-piece durable journal ordering.
pub async fn download_known_length_http(
    request: KnownLengthHttpRequest,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    download_known_length_http_inner(request, None, None).await
}

/// Runs a fresh transfer after local destination admission and peer pinning.
pub async fn download_known_length_http_resolved(
    request: KnownLengthHttpRequest,
    policy: HttpDestinationPolicy,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    resolve_http_destination(&request.uri, policy).await?;
    let transport = HttpDirectTransport::resolved(
        &request.uri,
        transport_config(
            policy,
            request.connect_timeout,
            request.response_head_timeout,
        ),
    )?;
    download_known_length_http_inner(request, None, Some(transport)).await
}

async fn download_known_length_http_inner(
    request: KnownLengthHttpRequest,
    mut lifecycle: Option<&mut RuntimeHttpLifecycle>,
    transport: Option<HttpDirectTransport>,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    if request.piece_length == 0 {
        return Err(KnownLengthHttpError::ZeroPieceLength);
    }
    let uri: Uri = request
        .uri
        .parse()
        .map_err(|_| KnownLengthHttpError::InvalidUri)?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) {
        return Err(KnownLengthHttpError::UnsupportedScheme);
    }
    let authority = uri
        .authority()
        .ok_or(KnownLengthHttpError::MissingAuthority)?;
    if authority.as_str().contains('@') {
        return Err(KnownLengthHttpError::UserInfoForbidden);
    }
    let expected_port = authority
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        });
    if request.peer.port() != expected_port {
        return Err(KnownLengthHttpError::PeerPortMismatch {
            expected: expected_port,
            actual: request.peer.port(),
        });
    }
    let resource_fingerprint = http_resource_fingerprint(&uri);

    let root = RootDirectoryCapability::open_trusted(&request.output_root)?;
    let output_file = root.create_new_file(&request.output)?;
    let mut journal = ControlJournalAppender::create(
        &request.journal_directory,
        request.gid,
        request.journal_id,
        request.generation,
        request.created_at_unix_ms,
    )?;
    append_initial_admission(&mut journal, request.generation)?;

    let transport = transport.unwrap_or(HttpDirectTransport::pinned(
        &request.uri,
        request.peer,
        transport_config(
            HttpDestinationPolicy::default(),
            request.connect_timeout,
            request.response_head_timeout,
        ),
    )?);
    let outbound = Request::builder()
        .method(Method::GET)
        .uri(uri.clone())
        .header(HOST, authority.as_str())
        .header(ACCEPT_ENCODING, "identity")
        .body(Empty::<Bytes>::new())
        .map_err(KnownLengthHttpError::Request)?;
    let response = transport.send(outbound).await?;
    let crate::http_transport::HttpTransportResponse { response, lease } = response;
    let head = validate_fresh_response_head(&response)?;

    output_file.set_len(head.content_length)?;
    let layout = build_single_file_layout(
        request.task,
        request.generation,
        &root,
        &request.output,
        &output_file,
        head.content_length,
        request.piece_length,
    )?;
    append_layout(&mut journal, &layout)?;
    if let Some(etag) = head.strong_etag.as_ref() {
        append_http_strong_validator(
            &mut journal,
            request.generation,
            resource_fingerprint,
            head.validator_fingerprint,
            head.content_length,
            etag,
        )?;
    }
    let storage = StorageEngine::open_layout(
        layout,
        [(FileId::new(0), output_file)],
        journal,
        request.storage,
    )?;
    if let Some(lifecycle) = lifecycle.as_mut() {
        lifecycle.activate().await?;
    }

    stream_known_length_body(
        storage,
        response,
        lease,
        BodyTransferPlan {
            task: request.task,
            generation: request.generation,
            piece_length: request.piece_length,
            content_length: head.content_length,
            start_offset: 0,
            transfer_attempt: TransferAttemptId::new(1)
                .expect("the first HTTP transfer-attempt identifier is nonzero"),
            next_lease_id: 1,
            validator_fingerprint: head.validator_fingerprint,
            durable_piece_count: 0,
            final_digest: Sha256::new(),
            completed_at_unix_ms: request.created_at_unix_ms,
            response_body_timeout: request.response_body_timeout,
            cancellation: request.cancellation,
        },
    )
    .await
}

struct BodyTransferPlan {
    task: TaskId,
    generation: Generation,
    piece_length: u64,
    content_length: u64,
    start_offset: u64,
    transfer_attempt: TransferAttemptId,
    next_lease_id: u64,
    validator_fingerprint: JournalHash,
    durable_piece_count: u64,
    final_digest: Sha256,
    completed_at_unix_ms: u64,
    response_body_timeout: Duration,
    cancellation: HttpCancellation,
}

async fn stream_known_length_body(
    mut storage: StorageEngine,
    mut response: Response<Incoming>,
    connection: HttpResponseLease,
    mut plan: BodyTransferPlan,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    let mut offset = plan.start_offset;
    let mut current: Option<(LeaseId, PieceId, u64)> = None;

    loop {
        let frame = tokio::select! {
            _ = plan.cancellation.cancelled() => {
                abort_current(&mut storage, plan.task, plan.generation, current, LeaseAbortReason::Cancelled)?;
                return Err(KnownLengthHttpError::Cancelled);
            }
            frame = timeout(plan.response_body_timeout, response.body_mut().frame()) => {
                match frame {
                    Ok(frame) => frame,
                    Err(_) => {
                        abort_current(
                            &mut storage,
                            plan.task,
                            plan.generation,
                            current,
                            LeaseAbortReason::Retry,
                        )?;
                        return Err(KnownLengthHttpError::ResponseBodyTimeout);
                    }
                }
            }
        };
        let Some(frame) = frame else {
            break;
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                let oversized = offset == plan.content_length;
                abort_current(
                    &mut storage,
                    plan.task,
                    plan.generation,
                    current,
                    if oversized {
                        LeaseAbortReason::OversizedBody
                    } else {
                        LeaseAbortReason::ShortBody
                    },
                )?;
                return Err(if oversized {
                    let _protocol_error = error;
                    KnownLengthHttpError::OversizedBody
                } else {
                    KnownLengthHttpError::ShortBody {
                        expected: plan.content_length,
                        actual: offset,
                    }
                });
            }
        };
        let Ok(data) = frame.into_data() else {
            abort_current(
                &mut storage,
                plan.task,
                plan.generation,
                current,
                LeaseAbortReason::OversizedBody,
            )?;
            return Err(KnownLengthHttpError::OversizedBody);
        };
        if data.is_empty() {
            continue;
        }
        let data_len = u64::try_from(data.len()).expect("HTTP frame length fits u64");
        if body_frame_end(offset, data_len, plan.content_length).is_none() {
            abort_current(
                &mut storage,
                plan.task,
                plan.generation,
                current,
                LeaseAbortReason::OversizedBody,
            )?;
            return Err(KnownLengthHttpError::OversizedBody);
        }
        plan.final_digest.update(&data);
        let mut consumed = 0_usize;
        while consumed < data.len() {
            if current.is_none() {
                let piece = PieceId::new(offset / plan.piece_length);
                let lease =
                    LeaseId::new(plan.next_lease_id).ok_or(KnownLengthHttpError::RecoveryState)?;
                plan.next_lease_id = plan
                    .next_lease_id
                    .checked_add(1)
                    .ok_or(KnownLengthHttpError::RecoveryState)?;
                let lease_len = plan.piece_length.min(plan.content_length - offset);
                let lease_len =
                    usize::try_from(lease_len).map_err(|_| KnownLengthHttpError::RecoveryState)?;
                storage.begin_lease(LeaseWritePlan {
                    task: plan.task,
                    generation: plan.generation,
                    transfer_attempt: plan.transfer_attempt,
                    lease,
                    span: ariax_storage::GlobalSpan {
                        offset,
                        len: lease_len,
                    },
                    validator: plan.validator_fingerprint,
                })?;
                current = Some((
                    lease,
                    piece,
                    u64::try_from(lease_len).expect("lease len fits u64"),
                ));
            }
            let (lease, piece, lease_remaining) = current.expect("lease was opened");
            let available = data.len() - consumed;
            let take = available
                .min(usize::try_from(lease_remaining).expect("piece length fits usize"))
                .min(SizeClass::MiB1.capacity());
            let mut buffer = storage.reserve_network_buffer(take)?;
            buffer.writable().map_err(|error| {
                KnownLengthHttpError::Storage(StorageEngineError::from_buffer_transition(error))
            })?[..take]
                .copy_from_slice(&data[consumed..consumed + take]);
            buffer
                .mark_filled(take, OwnerTag::Storage)
                .map_err(|error| {
                    KnownLengthHttpError::Storage(StorageEngineError::from_buffer_transition(error))
                })?;
            let write = storage
                .write_block(WriteBlock {
                    task: plan.task,
                    generation: plan.generation,
                    lease,
                    global_offset: offset,
                    expected_len: take,
                    buffer,
                    piece,
                })
                .await;
            if let Err(error) = write {
                let _aborted = storage.abort_lease(
                    plan.task,
                    plan.generation,
                    lease,
                    LeaseAbortReason::StorageRejected,
                );
                return Err(KnownLengthHttpError::Storage(error));
            }
            let taken = u64::try_from(take).expect("buffer take fits u64");
            offset += taken;
            consumed += take;
            let remaining = lease_remaining - taken;
            current = Some((lease, piece, remaining));
            if remaining == 0 && offset < plan.content_length {
                storage.commit_lease(LeaseCommit {
                    task: plan.task,
                    generation: plan.generation,
                    lease,
                    received_len: plan.piece_length,
                    validator: plan.validator_fingerprint,
                    response_digest: None,
                })?;
                plan.durable_piece_count += 1;
                current = None;
            }
        }
    }

    if offset != plan.content_length {
        abort_current(
            &mut storage,
            plan.task,
            plan.generation,
            current,
            LeaseAbortReason::ShortBody,
        )?;
        return Err(KnownLengthHttpError::ShortBody {
            expected: plan.content_length,
            actual: offset,
        });
    }
    if let Some((lease, _piece, remaining)) = current {
        debug_assert_eq!(remaining, 0);
        let final_piece_len = plan.content_length % plan.piece_length;
        let final_piece_len = if final_piece_len == 0 {
            plan.piece_length
        } else {
            final_piece_len
        };
        storage.commit_lease(LeaseCommit {
            task: plan.task,
            generation: plan.generation,
            lease,
            received_len: final_piece_len,
            validator: plan.validator_fingerprint,
            response_digest: None,
        })?;
        plan.durable_piece_count += 1;
    }
    let final_digest = JournalDigest::new(
        JournalDigestAlgorithm::Sha256,
        plan.final_digest.finalize().to_vec(),
    )
    .expect("SHA-256 output has canonical length");
    let terminal_sequence = storage.complete(
        Some(final_digest.clone()),
        now_unix_ms().unwrap_or(plan.completed_at_unix_ms),
    )?;
    storage.close()?;
    connection.recycle().await;
    Ok(KnownLengthHttpResult {
        content_length: plan.content_length,
        resumed_from: plan.start_offset,
        durable_piece_count: plan.durable_piece_count,
        terminal_sequence,
        validator_fingerprint: plan.validator_fingerprint,
        final_digest,
    })
}

/// Replays a first-slice journal, reopens its persisted output beneath the
/// supplied trusted root, and reports only the contiguous durable prefix.
pub fn recover_known_length_http(
    request: &KnownLengthHttpRecoveryRequest,
) -> Result<KnownLengthHttpRecovery, KnownLengthHttpError> {
    let prepared = prepare_known_length_recovery(request)?;
    let PreparedKnownLengthRecovery {
        mut appender,
        replay,
        durable_prefix,
        strong_validator,
        ..
    } = prepared;
    appender.close_flushed()?;
    Ok(KnownLengthHttpRecovery {
        replay,
        durable_prefix,
        strong_validator,
    })
}

struct PreparedKnownLengthRecovery {
    appender: ControlJournalAppender,
    replay: JournalStateReplay,
    layout: FileLayout,
    output_file: RootFileCapability,
    durable_prefix: u64,
    durable_piece_count: u64,
    strong_validator: Option<RecoveredHttpStrongValidator>,
    next_transfer_attempt_id: u64,
    next_lease_id: u64,
}

fn prepare_known_length_recovery(
    request: &KnownLengthHttpRecoveryRequest,
) -> Result<PreparedKnownLengthRecovery, KnownLengthHttpError> {
    let journal_capability = JournalDirectoryCapability::open_trusted(&request.journal_directory)?;
    let paths = ControlJournalAppender::discover_segment_paths(
        &journal_capability,
        request.replay_limits.max_segments,
    )?;
    let prepared = ControlJournalAppender::prepare_recovered_in(
        journal_capability,
        &paths,
        request.gid,
        request.journal_id,
        request.replay_limits,
    )?;
    let (appender, framing) = ControlJournalAppender::open_prepared(
        prepared,
        request.generation,
        now_unix_ms().unwrap_or(0),
    )?;
    let replay = recover_journal_state(
        &framing.records,
        request.task,
        &|_: &str| true,
        request.state_limits,
    );
    let state = replay
        .state
        .as_ref()
        .ok_or(KnownLengthHttpError::RecoveryState)?;
    if state.generation() != request.generation || state.task() != request.task {
        return Err(KnownLengthHttpError::RecoveryState);
    }
    let layout = state
        .layout()
        .ok_or(KnownLengthHttpError::RecoveryState)?
        .layout()
        .clone();
    let root = RootDirectoryCapability::open_trusted(&request.output_root)?;
    if PlatformPath::from_current(root.display())? != *layout.root_binding().path()
        || root.identity().encode().as_ref() != layout.root_binding().root_identity().bytes()
    {
        return Err(KnownLengthHttpError::RecoveryState);
    }
    let mut selected = layout.files().iter().filter(|entry| entry.selected());
    let entry = selected
        .next()
        .filter(|entry| entry.id() == FileId::new(0))
        .ok_or(KnownLengthHttpError::RecoveryState)?;
    if selected.next().is_some() {
        return Err(KnownLengthHttpError::RecoveryState);
    }
    let output_file = root.open_existing_file(
        entry.safe_path(),
        entry
            .identity()
            .ok_or(KnownLengthHttpError::RecoveryState)?,
    )?;
    let expected_length = layout
        .total_length()
        .ok_or(KnownLengthHttpError::RecoveryState)?;
    let actual_length = output_file.len()?;
    if actual_length != expected_length {
        return Err(KnownLengthHttpError::ExistingLengthMismatch {
            expected: expected_length,
            actual: actual_length,
        });
    }
    let mut durable_prefix = 0_u64;
    for (piece, evidence) in state.durable_pieces() {
        let expected_piece = PieceId::new(durable_prefix / layout.piece_length());
        if *piece != expected_piece || evidence.piece_span().offset() != durable_prefix {
            break;
        }
        durable_prefix = durable_prefix
            .checked_add(evidence.piece_span().len())
            .ok_or(KnownLengthHttpError::RecoveryState)?;
    }
    let strong_validator = state.http_strong_validator().cloned();
    let durable_piece_count = u64::try_from(state.durable_pieces().len())
        .map_err(|_| KnownLengthHttpError::RecoveryState)?;
    let (next_transfer_attempt_id, next_lease_id) = next_http_identifiers(
        &framing.records[..replay.accepted_records.min(framing.records.len())],
    )?;
    Ok(PreparedKnownLengthRecovery {
        appender,
        replay,
        layout,
        output_file,
        durable_prefix,
        durable_piece_count,
        strong_validator,
        next_transfer_attempt_id,
        next_lease_id,
    })
}

fn next_http_identifiers(
    records: &[ariax_storage::JournalRecord],
) -> Result<(u64, u64), KnownLengthHttpError> {
    let mut max_attempt = 0_u64;
    let mut max_lease = 0_u64;
    for record in records {
        if let JournalPayload::LeaseStarted {
            transfer_attempt_id,
            lease_id,
            ..
        } = record.decode_payload()?
        {
            max_attempt = max_attempt.max(transfer_attempt_id.get());
            max_lease = max_lease.max(lease_id.get());
        }
    }
    Ok((
        max_attempt
            .checked_add(1)
            .ok_or(KnownLengthHttpError::RecoveryState)?,
        max_lease
            .checked_add(1)
            .ok_or(KnownLengthHttpError::RecoveryState)?,
    ))
}

fn hash_recovered_durable_prefix(
    prepared: &PreparedKnownLengthRecovery,
    validator: &RecoveredHttpStrongValidator,
) -> Result<Sha256, KnownLengthHttpError> {
    let state = prepared
        .replay
        .state
        .as_ref()
        .ok_or(KnownLengthHttpError::RecoveryState)?;
    let mut whole = Sha256::new();
    let mut read_offset = 0_u64;
    let mut buffer = vec![0_u8; RECOVERY_READ_BUFFER_BYTES];
    for (piece_id, evidence) in state.durable_pieces() {
        if evidence.piece_span().offset() >= prepared.durable_prefix {
            break;
        }
        if evidence.piece_span().offset() != read_offset {
            return Err(KnownLengthHttpError::RecoveryState);
        }
        let expected_validator_set =
            calculate_validator_set_fingerprint(&[JournalContributor::new(
                LeaseId::new(1).expect("readback contributor lease is nonzero"),
                evidence.piece_span(),
                validator.validator_fingerprint(),
            )])
            .map_err(|_| KnownLengthHttpError::RecoveryState)?;
        if evidence.validator_set_fingerprint() != expected_validator_set {
            return Err(KnownLengthHttpError::StaleValidator);
        }
        let digest = evidence
            .digest()
            .filter(|digest| digest.algorithm() == JournalDigestAlgorithm::Sha256)
            .ok_or(KnownLengthHttpError::DurablePieceDigestMismatch { piece: *piece_id })?;
        let mut piece_digest = Sha256::new();
        let mut remaining = evidence.piece_span().len();
        while remaining != 0 {
            let take = usize::try_from(remaining.min(RECOVERY_READ_BUFFER_BYTES as u64))
                .expect("bounded recovery read fits usize");
            prepared
                .output_file
                .read_exact_at(read_offset, &mut buffer[..take])?;
            piece_digest.update(&buffer[..take]);
            whole.update(&buffer[..take]);
            let taken = u64::try_from(take).expect("recovery read fits u64");
            read_offset = read_offset
                .checked_add(taken)
                .ok_or(KnownLengthHttpError::RecoveryState)?;
            remaining -= taken;
        }
        let actual: [u8; 32] = piece_digest.finalize().into();
        if actual.as_slice() != digest.value() {
            return Err(KnownLengthHttpError::DurablePieceDigestMismatch { piece: *piece_id });
        }
    }
    if read_offset != prepared.durable_prefix {
        return Err(KnownLengthHttpError::RecoveryState);
    }
    Ok(whole)
}

struct ValidatedResponseHead {
    content_length: u64,
    validator_fingerprint: JournalHash,
    strong_etag: Option<Box<[u8]>>,
}

fn validate_resume_response_head(
    response: &Response<Incoming>,
    start: u64,
    total_length: u64,
    validator: &RecoveredHttpStrongValidator,
) -> Result<(), KnownLengthHttpError> {
    match response.status() {
        StatusCode::PARTIAL_CONTENT => {}
        StatusCode::OK => return Err(KnownLengthHttpError::RangeIgnored),
        StatusCode::RANGE_NOT_SATISFIABLE => {
            return Err(KnownLengthHttpError::RangeNotSatisfiable);
        }
        status => return Err(KnownLengthHttpError::UnexpectedStatus(status)),
    }
    validate_identity_coding(response)?;
    let response_etag = response_strong_etag(response.headers())
        .map_err(|_| KnownLengthHttpError::StaleValidator)?
        .ok_or(KnownLengthHttpError::StaleValidator)?;
    if response_etag.as_ref() != validator.etag()
        || calculate_http_strong_validator_fingerprint(&response_etag, total_length)?
            != validator.validator_fingerprint()
    {
        return Err(KnownLengthHttpError::StaleValidator);
    }

    let ranges = response.headers().get_all(CONTENT_RANGE);
    let mut ranges = ranges.iter();
    let content_range = ranges
        .next()
        .ok_or(KnownLengthHttpError::MissingContentRange)?;
    if ranges.next().is_some() {
        return Err(KnownLengthHttpError::DuplicateContentRange);
    }
    let content_range = content_range
        .to_str()
        .map_err(|_| KnownLengthHttpError::InvalidContentRange)?;
    let (actual_start, end, actual_total) = parse_content_range(content_range)?;
    let expected_end = total_length
        .checked_sub(1)
        .ok_or(KnownLengthHttpError::InvalidContentRange)?;
    if actual_start != start || end != expected_end || actual_total != total_length {
        return Err(KnownLengthHttpError::InvalidContentRange);
    }
    let expected_body_length = total_length
        .checked_sub(start)
        .ok_or(KnownLengthHttpError::InvalidContentRange)?;
    let content_length = single_content_length(response)?;
    if content_length != expected_body_length {
        return Err(KnownLengthHttpError::RangeLengthMismatch);
    }
    Ok(())
}

fn parse_content_range(value: &str) -> Result<(u64, u64, u64), KnownLengthHttpError> {
    let value = value
        .strip_prefix("bytes ")
        .ok_or(KnownLengthHttpError::InvalidContentRange)?;
    let (range, total) = value
        .split_once('/')
        .ok_or(KnownLengthHttpError::InvalidContentRange)?;
    if total.contains('/') {
        return Err(KnownLengthHttpError::InvalidContentRange);
    }
    let (start, end) = range
        .split_once('-')
        .ok_or(KnownLengthHttpError::InvalidContentRange)?;
    if end.contains('-') {
        return Err(KnownLengthHttpError::InvalidContentRange);
    }
    let parse = |input: &str| {
        if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(KnownLengthHttpError::InvalidContentRange);
        }
        input
            .parse::<u64>()
            .map_err(|_| KnownLengthHttpError::InvalidContentRange)
    };
    let start = parse(start)?;
    let end = parse(end)?;
    let total = parse(total)?;
    if start > end || end >= total {
        return Err(KnownLengthHttpError::InvalidContentRange);
    }
    Ok((start, end, total))
}

fn single_content_length(response: &Response<Incoming>) -> Result<u64, KnownLengthHttpError> {
    let lengths = response.headers().get_all(CONTENT_LENGTH);
    let mut lengths = lengths.iter();
    let first = lengths
        .next()
        .ok_or(KnownLengthHttpError::MissingContentLength)?;
    if lengths.next().is_some() {
        return Err(KnownLengthHttpError::DuplicateContentLength);
    }
    let value = first
        .to_str()
        .map_err(|_| KnownLengthHttpError::InvalidContentLength)?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KnownLengthHttpError::InvalidContentLength);
    }
    value
        .parse::<u64>()
        .map_err(|_| KnownLengthHttpError::InvalidContentLength)
}

fn validate_identity_coding(response: &Response<Incoming>) -> Result<(), KnownLengthHttpError> {
    if response.headers().contains_key(TRANSFER_ENCODING) {
        return Err(KnownLengthHttpError::TransferEncoding);
    }
    let encodings = response.headers().get_all(CONTENT_ENCODING);
    let mut encodings = encodings.iter();
    if let Some(encoding) = encodings.next()
        && (encodings.next().is_some()
            || !encoding
                .to_str()
                .is_ok_and(|value| value.eq_ignore_ascii_case("identity")))
    {
        return Err(KnownLengthHttpError::ContentEncoding);
    }
    Ok(())
}

fn validate_fresh_response_head(
    response: &Response<Incoming>,
) -> Result<ValidatedResponseHead, KnownLengthHttpError> {
    if response.status() != StatusCode::OK {
        return Err(KnownLengthHttpError::UnexpectedStatus(response.status()));
    }
    validate_identity_coding(response)?;
    let content_length = single_content_length(response)?;
    let strong_etag = response_strong_etag(response.headers())?;
    let validator_fingerprint = match strong_etag.as_deref() {
        Some(etag) => calculate_http_strong_validator_fingerprint(etag, content_length)?,
        None => http_metadata_validator_fingerprint(response, content_length),
    };
    Ok(ValidatedResponseHead {
        content_length,
        validator_fingerprint,
        strong_etag,
    })
}

fn http_metadata_validator_fingerprint(
    response: &Response<Incoming>,
    content_length: u64,
) -> JournalHash {
    let mut digest = Sha256::new();
    digest.update(HTTP_METADATA_VALIDATOR_HASH_DOMAIN.as_bytes());
    digest.update(content_length.to_le_bytes());
    for name in [ETAG, LAST_MODIFIED] {
        let values = response.headers().get_all(name);
        let count = u32::try_from(values.iter().count()).expect("response header cap fits u32");
        digest.update(count.to_le_bytes());
        for value in values {
            let bytes = value.as_bytes();
            digest.update((bytes.len() as u32).to_le_bytes());
            digest.update(bytes);
        }
    }
    JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero")
}

fn response_strong_etag(headers: &HeaderMap) -> Result<Option<Box<[u8]>>, KnownLengthHttpError> {
    let values = headers.get_all(ETAG);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(KnownLengthHttpError::InvalidValidator);
    }
    let bytes = value.as_bytes();
    if let Some(strong) = bytes.strip_prefix(b"W/") {
        calculate_http_strong_validator_fingerprint(strong, 0)
            .map_err(|_| KnownLengthHttpError::InvalidValidator)?;
        return Ok(None);
    }
    calculate_http_strong_validator_fingerprint(bytes, 0)
        .map_err(|_| KnownLengthHttpError::InvalidValidator)?;
    Ok(Some(bytes.to_vec().into_boxed_slice()))
}

fn http_resource_fingerprint(uri: &Uri) -> JournalHash {
    let mut digest = Sha256::new();
    digest.update(HTTP_RESOURCE_HASH_DOMAIN.as_bytes());
    for component in [
        uri.scheme_str().unwrap_or_default().as_bytes(),
        uri.authority()
            .map_or(&[][..], |value| value.as_str().as_bytes()),
        uri.path_and_query()
            .map_or(b"/".as_slice(), |value| value.as_str().as_bytes()),
    ] {
        digest.update(
            u32::try_from(component.len())
                .expect("URI component length fits u32")
                .to_le_bytes(),
        );
        digest.update(component);
    }
    JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero")
}

pub(crate) fn append_initial_admission(
    journal: &mut ControlJournalAppender,
    generation: Generation,
) -> Result<(), KnownLengthHttpError> {
    append_initial_admission_with_options(journal, generation, SanitizedOptionMap::new([])?)
}

pub(crate) fn append_initial_admission_with_options(
    journal: &mut ControlJournalAppender,
    generation: Generation,
    options: SanitizedOptionMap,
) -> Result<(), KnownLengthHttpError> {
    let created = journal.append_payload(
        generation,
        &JournalPayload::TaskCreated {
            durability: DurabilityMode::Strict,
            creator_version: 1,
        },
    )?;
    let options = journal.append_payload(
        generation,
        &JournalPayload::OptionsSnapshot {
            scope: OptionsSnapshotScope::CurrentGeneration,
            patch_id: None,
            snapshot_hash: options.snapshot_hash(),
            options,
        },
    )?;
    debug_assert!(options.sequence() > created.sequence());
    journal.flush(options.sequence())?;
    Ok(())
}

pub(crate) fn build_single_file_layout(
    task: TaskId,
    generation: Generation,
    root: &RootDirectoryCapability,
    output: &SafeRelativePath,
    output_file: &ariax_storage::RootFileCapability,
    content_length: u64,
    piece_length: u64,
) -> Result<FileLayout, KnownLengthHttpError> {
    let file_id = FileId::new(0);
    let file_identity = FileIdentity::new(output_file.identity().encode())?;
    let root_binding = RootBinding::new(
        PlatformPath::from_current(root.display())?,
        RootIdentity::new(root.identity().encode())?,
        [(file_id, file_identity.clone())],
    )?;
    Ok(FileLayout::new(
        task,
        generation,
        root_binding,
        vec![FileEntry::new(
            file_id,
            output.clone(),
            Some(file_identity),
            content_length,
            0,
            content_length,
            true,
        )],
        Some(content_length),
        piece_length,
    )?)
}

pub(crate) fn append_layout(
    journal: &mut ControlJournalAppender,
    layout: &FileLayout,
) -> Result<(), KnownLengthHttpError> {
    let entry = &layout.files()[0];
    let identity = entry
        .identity()
        .ok_or(KnownLengthHttpError::RecoveryState)?;
    let file = JournalFileLayoutEntry::new(
        entry.id(),
        entry.global_start(),
        entry.global_end(),
        entry.length(),
        entry.selected(),
        JournalRelativePath::new(entry.safe_path().canonical_string())?,
        identity.bytes().to_vec(),
    )?;
    let appended = journal.append_payload(
        layout.generation(),
        &JournalPayload::LayoutCommitted {
            layout_hash: JournalHash::new(*layout.layout_hash().as_bytes())
                .expect("layout SHA-256 is nonzero"),
            root_binding_hash: JournalHash::new(*layout.root_binding().hash().as_bytes())
                .expect("root-binding SHA-256 is nonzero"),
            root_display: layout.root_binding().path().clone(),
            root_identity: layout
                .root_binding()
                .root_identity()
                .bytes()
                .to_vec()
                .into_boxed_slice(),
            total_length: layout.total_length(),
            piece_length: layout.piece_length(),
            total_file_count: 1,
            chunk_count: 1,
            inline_files: vec![file].into_boxed_slice(),
        },
    )?;
    journal.flush(appended.sequence())?;
    Ok(())
}

fn append_http_strong_validator(
    journal: &mut ControlJournalAppender,
    generation: Generation,
    resource_fingerprint: JournalHash,
    validator_fingerprint: JournalHash,
    total_length: u64,
    etag: &[u8],
) -> Result<(), KnownLengthHttpError> {
    let appended = journal.append_payload(
        generation,
        &JournalPayload::HttpStrongValidator {
            resource_fingerprint,
            validator_fingerprint,
            total_length,
            etag: etag.to_vec().into_boxed_slice(),
        },
    )?;
    journal.flush(appended.sequence())?;
    Ok(())
}

fn abort_current(
    storage: &mut StorageEngine,
    task: TaskId,
    generation: Generation,
    current: Option<(LeaseId, PieceId, u64)>,
    reason: LeaseAbortReason,
) -> Result<(), KnownLengthHttpError> {
    if let Some((lease, _, _)) = current {
        storage.abort_lease(task, generation, lease, reason)?;
    }
    Ok(())
}

pub(crate) fn now_unix_ms() -> Option<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(duration.as_millis()).ok()
}

fn body_frame_end(offset: u64, frame_len: u64, content_length: u64) -> Option<u64> {
    offset
        .checked_add(frame_len)
        .filter(|end| *end <= content_length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSpaceProbeTargetCatalog, RuntimeEffectConfig, RuntimeSchedulerEffectSink};
    use ariax_core::{ErrorKind, RetryClass, TaskEvent};
    use ariax_storage::{JournalStateStop, PathPlatform, SafePathBuilder};
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener};
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "ariax-http-{label}-{}-{}",
                std::process::id(),
                now_unix_ms().unwrap_or(0)
            ));
            create_private_test_directory(&path);
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _removed = fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(not(windows))]
    fn create_private_test_directory(path: &Path) {
        fs::create_dir(path).expect("create test directory");
    }

    #[cfg(windows)]
    fn create_private_test_directory(path: &Path) {
        ariax_windows_security::create_private_directory(path)
            .expect("create private test directory");
    }

    fn serve(response: &'static [u8]) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("server address");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            let mut request = [0_u8; 4096];
            let mut used = 0_usize;
            while used < request.len() {
                let read = stream.read(&mut request[used..]).expect("read request");
                if read == 0 {
                    break;
                }
                used += read;
                if request[..used]
                    .windows(4)
                    .any(|window| window == b"\r\n\r\n")
                {
                    break;
                }
            }
            stream.write_all(response).expect("write response");
            stream.flush().expect("flush response");
            thread::sleep(Duration::from_millis(25));
            stream
                .shutdown(Shutdown::Both)
                .expect("close response body");
        });
        address
    }

    fn serve_stalled(
        response_prefix: &'static [u8],
        stall: Duration,
    ) -> (SocketAddr, mpsc::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("server address");
        let (sent, received) = mpsc::channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 4096];
            let mut used = 0_usize;
            while used < request.len() {
                let read = stream.read(&mut request[used..]).expect("read request");
                if read == 0 {
                    break;
                }
                used += read;
                if request[..used]
                    .windows(4)
                    .any(|window| window == b"\r\n\r\n")
                {
                    break;
                }
            }
            stream.write_all(response_prefix).expect("write prefix");
            stream.flush().expect("flush prefix");
            sent.send(()).expect("publish prefix");
            thread::sleep(stall);
        });
        (address, received)
    }

    fn serve_sequence(responses: Vec<&'static [u8]>) -> (SocketAddr, mpsc::Receiver<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("server address");
        let (requests, received) = mpsc::channel();
        thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept request");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout");
                let mut request = vec![0_u8; 4096];
                let mut used = 0_usize;
                while used < request.len() {
                    let read = stream.read(&mut request[used..]).expect("read request");
                    if read == 0 {
                        break;
                    }
                    used += read;
                    if request[..used]
                        .windows(4)
                        .any(|window| window == b"\r\n\r\n")
                    {
                        break;
                    }
                }
                request.truncate(used);
                requests.send(request).expect("publish request");
                stream.write_all(response).expect("write response");
                stream.flush().expect("flush response");
                thread::sleep(Duration::from_millis(25));
                stream
                    .shutdown(Shutdown::Both)
                    .expect("close response body");
            }
        });
        (address, received)
    }

    fn journal_id(value: u8) -> JournalId {
        JournalId::new([value; 16]).expect("journal id")
    }

    fn request(
        root: &TestDirectory,
        journal: &TestDirectory,
        peer: SocketAddr,
        id: JournalId,
    ) -> KnownLengthHttpRequest {
        KnownLengthHttpRequest {
            task: TaskId::new(1).expect("task"),
            gid: Gid::new(7).expect("gid"),
            generation: Generation::INITIAL,
            journal_id: id,
            uri: format!("http://127.0.0.1:{}/file", peer.port()),
            peer,
            output_root: root.0.clone(),
            output: SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
                .expect("safe output"),
            journal_directory: journal.0.join("task"),
            piece_length: 4,
            created_at_unix_ms: now_unix_ms().unwrap_or(1),
            connect_timeout: Duration::from_secs(5),
            response_head_timeout: Duration::from_secs(5),
            response_body_timeout: Duration::from_secs(5),
            storage: StorageEngineConfig::default(),
            cancellation: HttpCancellation::new(),
        }
    }

    fn resume_request(
        root: &TestDirectory,
        journal: &TestDirectory,
        peer: SocketAddr,
        id: JournalId,
    ) -> KnownLengthHttpResumeRequest {
        KnownLengthHttpResumeRequest {
            recovery: recovery_request(root, journal, id),
            uri: format!("http://127.0.0.1:{}/file", peer.port()),
            peer,
            resumed_at_unix_ms: now_unix_ms().unwrap_or(1),
            connect_timeout: Duration::from_secs(5),
            response_head_timeout: Duration::from_secs(5),
            response_body_timeout: Duration::from_secs(5),
            storage: StorageEngineConfig::default(),
            cancellation: HttpCancellation::new(),
        }
    }

    fn runtime_handle(capacity: usize) -> RuntimeEffectHandle {
        let capacity = NonZeroUsize::new(capacity).expect("runtime capacity");
        let (_sink, handle) = RuntimeSchedulerEffectSink::new(
            RuntimeEffectConfig {
                request_capacity: capacity,
                event_capacity: capacity,
                timer_capacity: capacity,
                option_plan_capacity: capacity,
            },
            NoSpaceProbeTargetCatalog::new(Vec::new()),
        )
        .expect("runtime effect mailbox");
        handle
    }

    #[test]
    fn known_length_response_commits_each_piece_and_recovers_complete_state() {
        let root = TestDirectory::new("complete-root");
        let journal = TestDirectory::new("complete-journal");
        let peer = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdefghij");
        let id = journal_id(1);
        let result = download_known_length_http_resolved_blocking(
            request(&root, &journal, peer, id),
            HttpDestinationPolicy {
                allow_loopback: true,
                ..HttpDestinationPolicy::default()
            },
        )
        .expect("download succeeds");
        assert_eq!(result.content_length, 10);
        assert_eq!(result.durable_piece_count, 3);
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("output"),
            b"abcdefghij"
        );

        let recovered = recover_known_length_http(&recovery_request(&root, &journal, id))
            .expect("recover complete download");
        assert_eq!(recovered.durable_prefix, 10);
        assert_eq!(recovered.replay.stop, JournalStateStop::CleanEnd);
        assert!(
            recovered
                .replay
                .state
                .as_ref()
                .expect("state")
                .terminal()
                .is_some()
        );
    }

    #[test]
    fn short_body_aborts_only_current_piece_and_preserves_prior_checkpoint() {
        let root = TestDirectory::new("short-root");
        let journal = TestDirectory::new("short-journal");
        let peer =
            serve(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabcdef");
        let id = journal_id(2);
        let error = download_known_length_http_blocking(request(&root, &journal, peer, id))
            .expect_err("short body rejected");
        assert!(
            matches!(
                error,
                KnownLengthHttpError::ShortBody {
                    expected: 10,
                    actual: 6
                }
            ),
            "unexpected short-body error: {error:?}"
        );

        let recovered = recover_known_length_http(&recovery_request(&root, &journal, id))
            .expect("recover partial download");
        assert_eq!(recovered.durable_prefix, 4);
        let state = recovered.replay.state.as_ref().expect("state");
        assert_eq!(state.durable_pieces().len(), 1);
        assert!(state.terminal().is_none());
    }

    #[test]
    fn recovered_range_resume_reuses_strong_etag_and_completes_without_truncation() {
        let root = TestDirectory::new("resume-root");
        let journal = TestDirectory::new("resume-journal");
        let (peer, requests) = serve_sequence(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdef",
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nefghij",
        ]);
        let id = journal_id(6);
        let first = download_known_length_http_resolved_blocking(
            request(&root, &journal, peer, id),
            HttpDestinationPolicy {
                allow_loopback: true,
                ..HttpDestinationPolicy::default()
            },
        )
        .expect_err("short first response is resumable");
        assert!(matches!(first, KnownLengthHttpError::ShortBody { .. }));
        let _fresh_request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("fresh request");

        let recovered = recover_known_length_http(&recovery_request(&root, &journal, id))
            .expect("recover partial download");
        assert_eq!(recovered.durable_prefix, 4);
        assert_eq!(
            recovered
                .strong_validator
                .as_ref()
                .expect("strong validator")
                .etag(),
            b"\"v1\""
        );

        let runtime = runtime_handle(8);
        runtime.enqueue_allocation_for_test(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            Generation::INITIAL,
        );
        let result = run_known_length_http_runtime_resolved_blocking(
            runtime.clone(),
            runtime.take_allocation().expect("allocation request"),
            KnownLengthHttpTransfer::Resume(resume_request(&root, &journal, peer, id)),
            HttpDestinationPolicy {
                allow_loopback: true,
                ..HttpDestinationPolicy::default()
            },
            MonotonicInstant::now(),
        )
        .expect("range resume succeeds");
        assert_eq!(result.resumed_from, 4);
        assert_eq!(result.content_length, 10);
        assert_eq!(result.durable_piece_count, 3);
        assert_eq!(
            fs::read(root.0.join("output.bin")).expect("resumed output"),
            b"abcdefghij"
        );
        let resume_request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("resume request");
        let resume_request = String::from_utf8_lossy(&resume_request).to_ascii_lowercase();
        assert!(resume_request.contains("range: bytes=4-\r\n"));
        assert!(resume_request.contains("if-range: \"v1\"\r\n"));
        assert!(resume_request.contains("accept-encoding: identity\r\n"));

        let now = MonotonicInstant::now();
        assert!(matches!(
            runtime
                .poll_event_at(now)
                .expect("allocation event")
                .into_event(),
            TaskEvent::AllocationSucceeded { .. }
        ));
        assert!(matches!(
            runtime.poll_event_at(now).expect("data event").into_event(),
            TaskEvent::DataComplete { seed: false, .. }
        ));
        assert!(matches!(
            runtime
                .poll_event_at(now)
                .expect("verification event")
                .into_event(),
            TaskEvent::VerificationSucceeded { .. }
        ));
        assert!(runtime.poll_event_at(now).is_none());

        let recovered = recover_known_length_http(&recovery_request(&root, &journal, id))
            .expect("recover completed resume");
        assert_eq!(recovered.durable_prefix, 10);
        assert!(
            recovered
                .replay
                .state
                .as_ref()
                .expect("state")
                .terminal()
                .is_some()
        );
    }

    #[test]
    fn resume_rejects_ignored_range_without_overwriting_durable_prefix() {
        let root = TestDirectory::new("ignored-range-root");
        let journal = TestDirectory::new("ignored-range-journal");
        let (peer, requests) = serve_sequence(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdef",
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n0123456789",
        ]);
        let id = journal_id(7);
        let _short = download_known_length_http_blocking(request(&root, &journal, peer, id))
            .expect_err("short first response");
        let _fresh_request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("fresh");
        let error = resume_known_length_http_blocking(resume_request(&root, &journal, peer, id))
            .expect_err("ignored range is rejected");
        assert!(matches!(error, KnownLengthHttpError::RangeIgnored));
        assert_eq!(
            &fs::read(root.0.join("output.bin")).expect("partial output")[..4],
            b"abcd"
        );
    }

    #[test]
    fn resume_rejects_stale_etag_and_invalid_content_range_before_writes() {
        for (label, response, expected) in [
            (
                "stale",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/10\r\nETag: \"v2\"\r\nConnection: close\r\n\r\nefghij".as_slice(),
                "stale_validator",
            ),
            (
                "range",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\nContent-Range: bytes 5-9/10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nfghij".as_slice(),
                "invalid_content_range",
            ),
        ] {
            let root = TestDirectory::new(&format!("{label}-root"));
            let journal = TestDirectory::new(&format!("{label}-journal"));
            let (peer, requests) = serve_sequence(vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdef",
                response,
            ]);
            let id = if label == "stale" {
                journal_id(8)
            } else {
                journal_id(9)
            };
            let _short = download_known_length_http_blocking(request(&root, &journal, peer, id))
                .expect_err("short first response");
            let _fresh_request = requests.recv_timeout(Duration::from_secs(5)).expect("fresh");
            let error = resume_known_length_http_blocking(resume_request(
                &root, &journal, peer, id,
            ))
            .expect_err("invalid resume response");
            assert_eq!(error.code(), expected);
            assert_eq!(
                &fs::read(root.0.join("output.bin")).expect("partial output")[..4],
                b"abcd"
            );
        }
    }

    #[test]
    fn resume_revalidates_durable_piece_bytes_before_connecting() {
        let root = TestDirectory::new("readback-root");
        let journal = TestDirectory::new("readback-journal");
        let peer = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdef");
        let id = journal_id(10);
        let _short = download_known_length_http_blocking(request(&root, &journal, peer, id))
            .expect_err("short first response");
        let mut output = fs::OpenOptions::new()
            .write(true)
            .open(root.0.join("output.bin"))
            .expect("open output for corruption");
        output.write_all(b"X").expect("corrupt durable byte");
        drop(output);

        let error = resume_known_length_http_blocking(resume_request(&root, &journal, peer, id))
            .expect_err("corrupt recovered piece is rejected before connect");
        assert!(matches!(
            error,
            KnownLengthHttpError::DurablePieceDigestMismatch { piece }
                if piece == PieceId::new(0)
        ));
    }

    #[test]
    fn runtime_worker_reports_allocation_data_and_verification_completion_in_order() {
        let root = TestDirectory::new("runtime-success-root");
        let journal = TestDirectory::new("runtime-success-journal");
        let (peer, requests) = serve_sequence(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nariax!",
        ]);
        let runtime = runtime_handle(8);
        runtime.enqueue_allocation_for_test(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            Generation::INITIAL,
        );
        let allocation = runtime.take_allocation().expect("allocation request");
        let retry_at = MonotonicInstant::now()
            .checked_add(Duration::from_secs(1))
            .expect("retry deadline");
        let mut transfer_request = request(&root, &journal, peer, journal_id(11));
        transfer_request.uri = format!("http://2130706433:{}/file", peer.port());
        transfer_request.peer = SocketAddr::new(
            "203.0.113.1".parse().expect("unapproved placeholder"),
            peer.port(),
        );
        let result = run_known_length_http_runtime_resolved_blocking(
            runtime.clone(),
            allocation,
            KnownLengthHttpTransfer::Fresh(transfer_request),
            HttpDestinationPolicy {
                allow_loopback: true,
                ..HttpDestinationPolicy::default()
            },
            retry_at,
        )
        .expect("runtime transfer succeeds");
        assert_eq!(result.content_length, 6);
        let request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("captured request");
        let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
        assert!(request.starts_with("get /file http/1.1\r\n"));
        assert!(request.contains(&format!("host: 2130706433:{}\r\n", peer.port())));

        let now = MonotonicInstant::now();
        assert!(matches!(
            runtime
                .poll_event_at(now)
                .expect("allocation event")
                .into_event(),
            TaskEvent::AllocationSucceeded { .. }
        ));
        assert!(matches!(
            runtime.poll_event_at(now).expect("data event").into_event(),
            TaskEvent::DataComplete { seed: false, .. }
        ));
        assert!(matches!(
            runtime
                .poll_event_at(now)
                .expect("verification event")
                .into_event(),
            TaskEvent::VerificationSucceeded { .. }
        ));
        assert!(runtime.poll_event_at(now).is_none());
    }

    #[test]
    fn resolved_runtime_rejects_denied_destination_before_activation() {
        let root = TestDirectory::new("runtime-destination-denied-root");
        let journal = TestDirectory::new("runtime-destination-denied-journal");
        let peer = "127.0.0.1:80".parse().expect("peer");
        let runtime = runtime_handle(8);
        runtime.enqueue_allocation_for_test(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            Generation::INITIAL,
        );
        let error = run_known_length_http_runtime_resolved_blocking(
            runtime.clone(),
            runtime.take_allocation().expect("allocation request"),
            KnownLengthHttpTransfer::Fresh(request(&root, &journal, peer, journal_id(14))),
            HttpDestinationPolicy::default(),
            MonotonicInstant::now(),
        )
        .expect_err("loopback is denied before connect");
        assert!(matches!(
            error,
            KnownLengthHttpRuntimeError::Transfer(KnownLengthHttpError::Destination(
                HttpDestinationError::AddressDenied {
                    class: crate::HttpAddressClass::Loopback,
                    ..
                }
            ))
        ));
        let now = MonotonicInstant::now();
        let event = runtime
            .poll_event_at(now)
            .expect("allocation failure event")
            .into_event();
        assert!(matches!(
            event,
            TaskEvent::AllocationFailed { error, .. }
                if error.kind() == ErrorKind::Network
                    && error.safe_message() == "destination_denied"
                    && error.retry_class() == RetryClass::Never
        ));
        assert!(runtime.poll_event_at(now).is_none());
        assert!(!root.0.join("output.bin").exists());
        assert!(!journal.0.join("task").exists());
    }

    #[test]
    fn destination_dns_failures_keep_pre_activation_retry_classification() {
        for error in [
            HttpDestinationError::ResolveTimeout,
            HttpDestinationError::NoAddresses,
            HttpDestinationError::Resolve(std::io::Error::other("resolver unavailable")),
        ] {
            let error = KnownLengthHttpError::Destination(error);
            assert!(error.retriable());
            let public = public_http_error(&error);
            assert_eq!(public.kind(), ErrorKind::Network);
            assert_eq!(public.safe_message(), error.code());
            assert_eq!(public.retry_class(), RetryClass::SameSource);
        }
        let denied = KnownLengthHttpError::Destination(HttpDestinationError::AddressDenied {
            address: "127.0.0.1".parse().expect("address"),
            class: crate::HttpAddressClass::Loopback,
        });
        assert!(!denied.retriable());
        assert_eq!(public_http_error(&denied).retry_class(), RetryClass::Never);
    }

    #[test]
    fn runtime_worker_classifies_active_short_body_as_retryable() {
        let root = TestDirectory::new("runtime-retry-root");
        let journal = TestDirectory::new("runtime-retry-journal");
        let peer = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdef");
        let runtime = runtime_handle(8);
        runtime.enqueue_allocation_for_test(
            TaskId::new(1).expect("task"),
            Gid::new(7).expect("gid"),
            Generation::INITIAL,
        );
        let retry_at = MonotonicInstant::now()
            .checked_add(Duration::from_secs(1))
            .expect("retry deadline");
        let error = run_known_length_http_runtime_blocking(
            runtime.clone(),
            runtime.take_allocation().expect("allocation request"),
            KnownLengthHttpTransfer::Fresh(request(&root, &journal, peer, journal_id(12))),
            retry_at,
        )
        .expect_err("short body is reported to scheduler");
        assert!(matches!(
            error,
            KnownLengthHttpRuntimeError::Transfer(KnownLengthHttpError::ShortBody { .. })
        ));
        let now = MonotonicInstant::now();
        assert!(matches!(
            runtime
                .poll_event_at(now)
                .expect("allocation event")
                .into_event(),
            TaskEvent::AllocationSucceeded { .. }
        ));
        assert!(matches!(
            runtime.poll_event_at(now).expect("retry event").into_event(),
            TaskEvent::ActiveRetryIdle { retry_at: actual, .. } if actual == retry_at
        ));
    }

    #[test]
    fn runtime_worker_drains_exact_scheduler_cancellation_after_lease_abort() {
        let root = TestDirectory::new("runtime-cancel-root");
        let journal = TestDirectory::new("runtime-cancel-journal");
        let (peer, prefix_sent) = serve_stalled(
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcd",
            Duration::from_secs(1),
        );
        let runtime = runtime_handle(8);
        let task = TaskId::new(1).expect("task");
        let gid = Gid::new(7).expect("gid");
        runtime.enqueue_allocation_for_test(task, gid, Generation::INITIAL);
        let cancellation_runtime = runtime.clone();
        let cancellation_thread = thread::spawn(move || {
            prefix_sent
                .recv_timeout(Duration::from_secs(5))
                .expect("server sent cancellation prefix");
            thread::sleep(Duration::from_millis(50));
            cancellation_runtime.enqueue_cancellation_for_test(
                task,
                gid,
                Generation::INITIAL,
                false,
            );
        });
        let error = run_known_length_http_runtime_blocking(
            runtime.clone(),
            runtime.take_allocation().expect("allocation request"),
            KnownLengthHttpTransfer::Fresh(request(&root, &journal, peer, journal_id(13))),
            MonotonicInstant::now(),
        )
        .expect_err("scheduler cancellation stops transfer");
        cancellation_thread.join().expect("cancellation thread");
        assert!(
            matches!(
                error,
                KnownLengthHttpRuntimeError::Transfer(KnownLengthHttpError::Cancelled)
            ),
            "unexpected runtime cancellation result: {error:?}"
        );
        let now = MonotonicInstant::now();
        assert!(matches!(
            runtime
                .poll_event_at(now)
                .expect("allocation event")
                .into_event(),
            TaskEvent::AllocationSucceeded { .. }
        ));
        assert!(matches!(
            runtime
                .poll_event_at(now)
                .expect("cancellation event")
                .into_event(),
            TaskEvent::CancellationDrained { .. }
        ));
        let recovered =
            recover_known_length_http(&recovery_request(&root, &journal, journal_id(13)))
                .expect("cancelled runtime transfer remains recoverable");
        assert_eq!(recovered.durable_prefix, 4);
    }

    #[test]
    fn cancellation_before_body_subscription_is_retained() {
        let cancellation = HttpCancellation::new();
        cancellation.cancel();
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn missing_length_is_rejected_before_layout_or_body_polling() {
        let root = TestDirectory::new("missing-root");
        let journal = TestDirectory::new("missing-journal");
        let peer = serve(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nbody");
        let id = journal_id(3);
        let error = download_known_length_http_blocking(request(&root, &journal, peer, id))
            .expect_err("missing length rejected");
        assert!(matches!(error, KnownLengthHttpError::MissingContentLength));

        let recovered = recover_known_length_http(&recovery_request(&root, &journal, id));
        assert!(matches!(
            recovered,
            Err(KnownLengthHttpError::RecoveryState)
        ));
    }

    #[test]
    fn chunked_response_is_rejected_before_storage_open() {
        let root = TestDirectory::new("chunked-root");
        let journal = TestDirectory::new("chunked-journal");
        let peer = serve(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nbody\r\n0\r\n\r\n");
        let error =
            download_known_length_http_blocking(request(&root, &journal, peer, journal_id(4)))
                .expect_err("chunked rejected");
        assert!(matches!(error, KnownLengthHttpError::TransferEncoding));
    }

    #[test]
    fn cancellation_aborts_partial_piece_and_preserves_prior_checkpoint() {
        let root = TestDirectory::new("cancel-root");
        let journal = TestDirectory::new("cancel-journal");
        let (peer, prefix_sent) = serve_stalled(
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabcd",
            Duration::from_secs(1),
        );
        let id = journal_id(5);
        let input = request(&root, &journal, peer, id);
        let cancellation = input.cancellation.clone();
        thread::spawn(move || {
            prefix_sent
                .recv_timeout(Duration::from_secs(5))
                .expect("server sent cancellation prefix");
            thread::sleep(Duration::from_millis(50));
            cancellation.cancel();
        });
        let error =
            download_known_length_http_blocking(input).expect_err("cancelled download is rejected");
        assert!(matches!(error, KnownLengthHttpError::Cancelled));
        let recovered = recover_known_length_http(&recovery_request(&root, &journal, id))
            .expect("recover cancelled download");
        assert_eq!(recovered.durable_prefix, 4);
        assert_eq!(
            recovered
                .replay
                .state
                .expect("state")
                .durable_pieces()
                .len(),
            1
        );
    }

    #[test]
    fn ingress_frame_larger_than_remaining_body_is_rejected() {
        assert_eq!(body_frame_end(7, 3, 10), Some(10));
        assert_eq!(body_frame_end(7, 4, 10), None);
        assert_eq!(body_frame_end(u64::MAX, 1, u64::MAX), None);
    }

    fn recovery_request(
        root: &TestDirectory,
        journal: &TestDirectory,
        id: JournalId,
    ) -> KnownLengthHttpRecoveryRequest {
        KnownLengthHttpRecoveryRequest {
            task: TaskId::new(1).expect("task"),
            gid: Gid::new(7).expect("gid"),
            journal_id: id,
            generation: Generation::INITIAL,
            journal_directory: journal.0.join("task"),
            output_root: root.0.clone(),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        }
    }
}
