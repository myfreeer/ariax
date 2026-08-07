use crate::{
    LeaseCommit, LeaseWritePlan, StorageEngine, StorageEngineConfig, StorageEngineError, WriteBlock,
};
use ariax_core::{FileId, Generation, Gid, LeaseId, PieceId, TaskId, TransferAttemptId};
use ariax_runtime::{OwnerTag, SizeClass};
use ariax_storage::{
    ControlJournalAppender, DurabilityMode, FileEntry, FileIdentity, FileLayout, JournalDigest,
    JournalDigestAlgorithm, JournalDirectoryCapability, JournalFileLayoutEntry, JournalHash,
    JournalId, JournalPayload, JournalRelativePath, JournalStateLimits, JournalStateReplay,
    LayoutError, LeaseAbortReason, NativeCapabilityError, OptionsSnapshotScope, PayloadCodecError,
    PlatformPath, ReplayLimits, RootBinding, RootBindingError, RootDirectoryCapability,
    RootIdentity, SafeRelativePath, SanitizedOptionMap, recover_journal_state,
};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::header::{
    ACCEPT_ENCODING, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, ETAG, HOST, LAST_MODIFIED,
    TRANSFER_ENCODING,
};
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::watch;
use tokio::time::timeout;

const MAX_RESPONSE_HEADERS: usize = 128;
const MAX_RESPONSE_HEAD_BYTES: usize = 64 * 1024;
const HTTP_VALIDATOR_HASH_DOMAIN: &str = "ariax/http-validator/v1\0";

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
        let _changed = self.sender.send(true);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    async fn cancelled(&self) {
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

/// Stable failure classes exposed to scheduler/retry integration.
#[derive(Debug)]
pub enum KnownLengthHttpError {
    InvalidUri,
    UnsupportedScheme,
    MissingAuthority,
    UserInfoForbidden,
    PeerPortMismatch { expected: u16, actual: u16 },
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
        }
    }

    #[must_use]
    pub const fn retriable(&self) -> bool {
        matches!(
            self,
            Self::ConnectTimeout
                | Self::Connect(_)
                | Self::HandshakeTimeout
                | Self::Hyper(_)
                | Self::ResponseHeadTimeout
                | Self::ShortBody { .. }
                | Self::ResponseBodyTimeout
        )
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

/// Downloads one identity-coded, known-length HTTP/1.1 response through the
/// concrete `StorageEngine` and strict per-piece durable journal ordering.
pub async fn download_known_length_http(
    request: KnownLengthHttpRequest,
) -> Result<KnownLengthHttpResult, KnownLengthHttpError> {
    if request.piece_length == 0 {
        return Err(KnownLengthHttpError::ZeroPieceLength);
    }
    let uri: Uri = request
        .uri
        .parse()
        .map_err(|_| KnownLengthHttpError::InvalidUri)?;
    if uri.scheme_str() != Some("http") {
        return Err(KnownLengthHttpError::UnsupportedScheme);
    }
    let authority = uri
        .authority()
        .ok_or(KnownLengthHttpError::MissingAuthority)?;
    if authority.as_str().contains('@') {
        return Err(KnownLengthHttpError::UserInfoForbidden);
    }
    let expected_port = authority.port_u16().unwrap_or(80);
    if request.peer.port() != expected_port {
        return Err(KnownLengthHttpError::PeerPortMismatch {
            expected: expected_port,
            actual: request.peer.port(),
        });
    }

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

    let stream = timeout(request.connect_timeout, TcpStream::connect(request.peer))
        .await
        .map_err(|_| KnownLengthHttpError::ConnectTimeout)?
        .map_err(KnownLengthHttpError::Connect)?;
    let mut builder = http1::Builder::new();
    builder
        .max_headers(MAX_RESPONSE_HEADERS)
        .max_buf_size(MAX_RESPONSE_HEAD_BYTES);
    let (mut sender, connection) = timeout(
        request.response_head_timeout,
        builder.handshake(TokioIo::new(stream)),
    )
    .await
    .map_err(|_| KnownLengthHttpError::HandshakeTimeout)?
    .map_err(KnownLengthHttpError::Hyper)?;
    let connection = tokio::spawn(connection);
    let path = uri
        .path_and_query()
        .map_or("/", hyper::http::uri::PathAndQuery::as_str);
    let outbound = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(HOST, authority.as_str())
        .header(ACCEPT_ENCODING, "identity")
        .header(CONNECTION, "close")
        .body(Empty::<Bytes>::new())
        .map_err(KnownLengthHttpError::Request)?;
    let response = timeout(request.response_head_timeout, sender.send_request(outbound))
        .await
        .map_err(|_| KnownLengthHttpError::ResponseHeadTimeout)?
        .map_err(KnownLengthHttpError::Hyper)?;
    drop(sender);
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
    let mut storage = StorageEngine::open_layout(
        layout,
        [(FileId::new(0), output_file)],
        journal,
        request.storage,
    )?;

    let transfer_attempt =
        TransferAttemptId::new(1).expect("the first HTTP transfer-attempt identifier is nonzero");
    let mut response = response;
    let mut offset = 0_u64;
    let mut next_lease_id = 1_u64;
    let mut current: Option<(LeaseId, PieceId, u64)> = None;
    let mut final_digest = Sha256::new();
    let mut durable_piece_count = 0_u64;

    loop {
        let frame = tokio::select! {
            _ = request.cancellation.cancelled() => {
                abort_current(&mut storage, request.task, request.generation, current, LeaseAbortReason::Cancelled)?;
                return Err(KnownLengthHttpError::Cancelled);
            }
            frame = timeout(request.response_body_timeout, response.body_mut().frame()) => {
                match frame {
                    Ok(frame) => frame,
                    Err(_) => {
                        abort_current(
                            &mut storage,
                            request.task,
                            request.generation,
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
                let oversized = offset == head.content_length;
                abort_current(
                    &mut storage,
                    request.task,
                    request.generation,
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
                        expected: head.content_length,
                        actual: offset,
                    }
                });
            }
        };
        let Ok(data) = frame.into_data() else {
            abort_current(
                &mut storage,
                request.task,
                request.generation,
                current,
                LeaseAbortReason::OversizedBody,
            )?;
            return Err(KnownLengthHttpError::OversizedBody);
        };
        if data.is_empty() {
            continue;
        }
        let data_len = u64::try_from(data.len()).expect("HTTP frame length fits u64");
        if body_frame_end(offset, data_len, head.content_length).is_none() {
            abort_current(
                &mut storage,
                request.task,
                request.generation,
                current,
                LeaseAbortReason::OversizedBody,
            )?;
            return Err(KnownLengthHttpError::OversizedBody);
        }
        final_digest.update(&data);
        let mut consumed = 0_usize;
        while consumed < data.len() {
            if current.is_none() {
                let piece = PieceId::new(offset / request.piece_length);
                let lease =
                    LeaseId::new(next_lease_id).ok_or(KnownLengthHttpError::RecoveryState)?;
                next_lease_id = next_lease_id
                    .checked_add(1)
                    .ok_or(KnownLengthHttpError::RecoveryState)?;
                let lease_len = request.piece_length.min(head.content_length - offset);
                let lease_len =
                    usize::try_from(lease_len).map_err(|_| KnownLengthHttpError::RecoveryState)?;
                storage.begin_lease(LeaseWritePlan {
                    task: request.task,
                    generation: request.generation,
                    transfer_attempt,
                    lease,
                    span: ariax_storage::GlobalSpan {
                        offset,
                        len: lease_len,
                    },
                    validator: head.validator_fingerprint,
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
                    task: request.task,
                    generation: request.generation,
                    lease,
                    global_offset: offset,
                    expected_len: take,
                    buffer,
                    piece,
                })
                .await;
            if let Err(error) = write {
                let _aborted = storage.abort_lease(
                    request.task,
                    request.generation,
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
            if remaining == 0 && offset < head.content_length {
                storage.commit_lease(LeaseCommit {
                    task: request.task,
                    generation: request.generation,
                    lease,
                    received_len: request.piece_length,
                    validator: head.validator_fingerprint,
                    response_digest: None,
                })?;
                durable_piece_count += 1;
                current = None;
            }
        }
    }

    if offset != head.content_length {
        abort_current(
            &mut storage,
            request.task,
            request.generation,
            current,
            LeaseAbortReason::ShortBody,
        )?;
        return Err(KnownLengthHttpError::ShortBody {
            expected: head.content_length,
            actual: offset,
        });
    }
    let connection_result = timeout(request.response_body_timeout, connection)
        .await
        .map_err(|_| KnownLengthHttpError::ResponseBodyTimeout)?
        .map_err(|_| KnownLengthHttpError::RecoveryState)?;
    if let Err(error) = connection_result {
        abort_current(
            &mut storage,
            request.task,
            request.generation,
            current,
            LeaseAbortReason::OversizedBody,
        )?;
        let _protocol_error = error;
        return Err(KnownLengthHttpError::OversizedBody);
    }
    if let Some((lease, _piece, remaining)) = current {
        debug_assert_eq!(remaining, 0);
        let final_piece_len = head.content_length % request.piece_length;
        let final_piece_len = if final_piece_len == 0 {
            request.piece_length
        } else {
            final_piece_len
        };
        storage.commit_lease(LeaseCommit {
            task: request.task,
            generation: request.generation,
            lease,
            received_len: final_piece_len,
            validator: head.validator_fingerprint,
            response_digest: None,
        })?;
        durable_piece_count += 1;
    }
    let final_digest = JournalDigest::new(
        JournalDigestAlgorithm::Sha256,
        final_digest.finalize().to_vec(),
    )
    .expect("SHA-256 output has canonical length");
    let terminal_sequence = storage.complete(
        Some(final_digest.clone()),
        now_unix_ms().unwrap_or(request.created_at_unix_ms),
    )?;
    storage.close()?;
    Ok(KnownLengthHttpResult {
        content_length: head.content_length,
        durable_piece_count,
        terminal_sequence,
        validator_fingerprint: head.validator_fingerprint,
        final_digest,
    })
}

/// Replays a first-slice journal, reopens its persisted output beneath the
/// supplied trusted root, and reports only the contiguous durable prefix.
pub fn recover_known_length_http(
    request: &KnownLengthHttpRecoveryRequest,
) -> Result<KnownLengthHttpRecovery, KnownLengthHttpError> {
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
    let (mut appender, framing) = ControlJournalAppender::open_prepared(
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
    let layout = state
        .layout()
        .ok_or(KnownLengthHttpError::RecoveryState)?
        .layout();
    let root = RootDirectoryCapability::open_trusted(&request.output_root)?;
    if PlatformPath::from_current(root.display())? != *layout.root_binding().path()
        || root.identity().encode().as_ref() != layout.root_binding().root_identity().bytes()
    {
        return Err(KnownLengthHttpError::RecoveryState);
    }
    for entry in layout.files().iter().filter(|entry| entry.selected()) {
        root.verify_file(
            entry.safe_path(),
            entry
                .identity()
                .ok_or(KnownLengthHttpError::RecoveryState)?,
        )?;
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
    appender.close_flushed()?;
    Ok(KnownLengthHttpRecovery {
        replay,
        durable_prefix,
    })
}

struct ValidatedResponseHead {
    content_length: u64,
    validator_fingerprint: JournalHash,
}

fn validate_fresh_response_head(
    response: &Response<Incoming>,
) -> Result<ValidatedResponseHead, KnownLengthHttpError> {
    if response.status() != StatusCode::OK {
        return Err(KnownLengthHttpError::UnexpectedStatus(response.status()));
    }
    if response.headers().contains_key(TRANSFER_ENCODING) {
        return Err(KnownLengthHttpError::TransferEncoding);
    }
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
    let content_length = value
        .parse::<u64>()
        .map_err(|_| KnownLengthHttpError::InvalidContentLength)?;
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
    Ok(ValidatedResponseHead {
        content_length,
        validator_fingerprint: http_validator_fingerprint(response, content_length),
    })
}

fn http_validator_fingerprint(response: &Response<Incoming>, content_length: u64) -> JournalHash {
    let mut digest = Sha256::new();
    digest.update(HTTP_VALIDATOR_HASH_DOMAIN.as_bytes());
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

fn append_initial_admission(
    journal: &mut ControlJournalAppender,
    generation: Generation,
) -> Result<(), KnownLengthHttpError> {
    let created = journal.append_payload(
        generation,
        &JournalPayload::TaskCreated {
            durability: DurabilityMode::Strict,
            creator_version: 1,
        },
    )?;
    let options = SanitizedOptionMap::new([])?;
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

fn build_single_file_layout(
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

fn append_layout(
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

fn now_unix_ms() -> Option<u64> {
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
    use ariax_storage::{JournalStateStop, PathPlatform, SafePathBuilder};
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener};
    use std::path::Path;
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
            stream
                .shutdown(Shutdown::Both)
                .expect("close response body");
        });
        address
    }

    fn serve_stalled(response_prefix: &'static [u8], stall: Duration) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("server address");
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
            thread::sleep(stall);
        });
        address
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

    #[test]
    fn known_length_response_commits_each_piece_and_recovers_complete_state() {
        let root = TestDirectory::new("complete-root");
        let journal = TestDirectory::new("complete-journal");
        let peer = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdefghij");
        let id = journal_id(1);
        let result = download_known_length_http_blocking(request(&root, &journal, peer, id))
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
        let peer = serve_stalled(
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabcdef",
            Duration::from_secs(1),
        );
        let id = journal_id(5);
        let input = request(&root, &journal, peer, id);
        let cancellation = input.cancellation.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
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
