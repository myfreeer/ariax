use crate::HttpResolver;
use crate::http_connector::{
    HttpDestinationError, HttpDestinationPolicy, resolve_http_destination,
    resolve_http_destination_with_resolver,
};
use crate::http_happy_eyeballs::{
    DEFAULT_HTTP_HAPPY_EYEBALLS_DELAY, HttpHappyEyeballsConfig, HttpHappyEyeballsError,
    connect_http_happy_eyeballs,
};
use ariax_runtime::{ByteBudget, BytePermit, HandleBudgetLimits, HandleBudgets, HandlePermit};
use bytes::Bytes;
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::header::HOST;
use hyper::{Request, Response, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use rustls::{ClientConfig, RootCertStore};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout};
use tower_service::Service;

pub const DEFAULT_HTTP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_HTTP_MAX_CONNECTIONS_PER_ORIGIN: usize = 1;
pub const DEFAULT_HTTP_MAX_IDLE_CONNECTIONS_PER_ORIGIN: usize = 1;
pub const MAX_HTTP_CONNECTIONS_PER_ORIGIN: usize = 8;
pub const MAX_HTTP_IDLE_CONNECTIONS_PER_ORIGIN: usize = 8;
pub const MAX_HTTP_TLS_BUNDLE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_HTTP_TLS_BUNDLE_CERTIFICATES: usize = 4096;
pub const HTTP_CONNECTION_RESERVATION_BYTES: usize = 256 * 1024;

type HttpBody = Empty<Bytes>;
type BoxError = Box<dyn Error + Send + Sync>;
type ConnectorStream = TokioIo<BudgetedTcpStream>;
type HttpConnector = HttpsConnector<PolicyConnector>;
type SendRequest = http1::SendRequest<HttpBody>;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum HttpMinimumTlsVersion {
    #[default]
    Tls12,
    Tls13,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum HttpTrustSource {
    #[default]
    System,
    CustomPem(PathBuf),
    SystemAndCustom(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpTlsPolicy {
    pub minimum_version: HttpMinimumTlsVersion,
    pub trust: HttpTrustSource,
}

impl Default for HttpTlsPolicy {
    fn default() -> Self {
        Self {
            minimum_version: HttpMinimumTlsVersion::Tls12,
            trust: HttpTrustSource::System,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpTransportBudgets {
    handles: HandleBudgets,
    connection_memory: ByteBudget,
    resident_memory: ByteBudget,
    connection_reservation_bytes: usize,
}

impl HttpTransportBudgets {
    pub fn new(
        max_sockets: usize,
        connection_memory_bytes: usize,
    ) -> Result<Self, HttpTransportError> {
        if max_sockets == 0 || connection_memory_bytes < HTTP_CONNECTION_RESERVATION_BYTES {
            return Err(HttpTransportError::InvalidPolicy);
        }
        let handles = HandleBudgets::new(HandleBudgetLimits {
            process: max_sockets,
            sockets: max_sockets,
            files: max_sockets,
        })
        .map_err(|_| HttpTransportError::InvalidPolicy)?;
        Ok(Self {
            handles,
            connection_memory: ByteBudget::new(connection_memory_bytes),
            resident_memory: ByteBudget::new(connection_memory_bytes),
            connection_reservation_bytes: HTTP_CONNECTION_RESERVATION_BYTES,
        })
    }

    pub(crate) fn with_shared_resident(
        handles: HandleBudgets,
        connection_memory_bytes: usize,
        resident_memory: ByteBudget,
        connection_reservation_bytes: usize,
    ) -> Result<Self, HttpTransportError> {
        if connection_reservation_bytes == 0
            || connection_memory_bytes < connection_reservation_bytes
            || resident_memory.limit() < connection_reservation_bytes
        {
            return Err(HttpTransportError::InvalidPolicy);
        }
        Ok(Self {
            handles,
            connection_memory: ByteBudget::new(connection_memory_bytes),
            resident_memory,
            connection_reservation_bytes,
        })
    }

    #[must_use]
    pub fn socket_limit(&self) -> usize {
        self.handles.limits().sockets
    }

    #[must_use]
    pub fn available_sockets(&self) -> usize {
        self.handles.available_sockets()
    }

    #[must_use]
    pub fn connection_reservation_bytes(&self) -> usize {
        self.connection_reservation_bytes
    }

    #[must_use]
    pub fn connection_memory_used(&self) -> usize {
        self.connection_memory.used()
    }

    #[must_use]
    pub fn resident_memory_used(&self) -> usize {
        self.resident_memory.used()
    }

    /// Returns the process-owned handle domains shared by every transport
    /// consumer constructed from this budget. Proxy sockets and storage file
    /// descriptors use this same object so the process cap cannot be bypassed
    /// by an adapter-specific path.
    #[must_use]
    pub fn handle_budgets(&self) -> HandleBudgets {
        self.handles.clone()
    }

    pub fn try_acquire_connection(
        &self,
    ) -> Result<HttpTransportCapacityPermit, HttpTransportError> {
        let handle = self
            .handles
            .try_acquire_socket()
            .map_err(|_| HttpTransportError::PoolExhausted)?;
        let connection_memory = self
            .connection_memory
            .try_acquire(self.connection_reservation_bytes)
            .map_err(|_| HttpTransportError::PoolExhausted)?;
        let resident_memory = self
            .resident_memory
            .try_acquire(self.connection_reservation_bytes)
            .map_err(|_| HttpTransportError::PoolExhausted)?;
        Ok(HttpTransportCapacityPermit {
            _handle: handle,
            _connection_memory: connection_memory,
            _resident_memory: resident_memory,
        })
    }
}

/// Capacity retained for the full lifetime of one physical HTTP socket.
pub struct HttpTransportCapacityPermit {
    _handle: HandlePermit,
    _connection_memory: BytePermit,
    _resident_memory: BytePermit,
}

impl fmt::Debug for HttpTransportCapacityPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpTransportCapacityPermit")
            .finish_non_exhaustive()
    }
}

impl Default for HttpTransportBudgets {
    fn default() -> Self {
        Self::new(
            DEFAULT_HTTP_MAX_CONNECTIONS_PER_ORIGIN,
            HTTP_CONNECTION_RESERVATION_BYTES,
        )
        .expect("default HTTP transport budgets are valid")
    }
}

#[derive(Clone, Debug)]
pub struct HttpDirectTransportConfig {
    pub destination: HttpDestinationPolicy,
    pub tls: HttpTlsPolicy,
    pub connect_timeout: Duration,
    pub happy_eyeballs_delay: Duration,
    pub handshake_timeout: Duration,
    pub keep_alive: bool,
    pub max_connections_per_origin: usize,
    pub max_idle_connections_per_origin: usize,
    pub idle_timeout: Duration,
    pub budgets: HttpTransportBudgets,
}

impl Default for HttpDirectTransportConfig {
    fn default() -> Self {
        Self {
            destination: HttpDestinationPolicy::default(),
            tls: HttpTlsPolicy::default(),
            connect_timeout: Duration::from_secs(30),
            happy_eyeballs_delay: DEFAULT_HTTP_HAPPY_EYEBALLS_DELAY,
            handshake_timeout: Duration::from_secs(30),
            keep_alive: true,
            max_connections_per_origin: DEFAULT_HTTP_MAX_CONNECTIONS_PER_ORIGIN,
            max_idle_connections_per_origin: DEFAULT_HTTP_MAX_IDLE_CONNECTIONS_PER_ORIGIN,
            idle_timeout: DEFAULT_HTTP_IDLE_TIMEOUT,
            budgets: HttpTransportBudgets::default(),
        }
    }
}

#[derive(Debug)]
pub enum HttpTransportError {
    InvalidPolicy,
    InvalidOrigin,
    OriginMismatch,
    Destination(HttpDestinationError),
    PoolExhausted,
    ConnectTimeout,
    Connect(io::Error),
    TlsConfiguration(String),
    TlsServerName(String),
    TlsHandshakeTimeout,
    TlsHandshake(String),
    HandshakeTimeout,
    Hyper(hyper::Error),
    Request(hyper::http::Error),
}

impl HttpTransportError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidPolicy => "http_transport_invalid_policy",
            Self::InvalidOrigin => "http_transport_invalid_origin",
            Self::OriginMismatch => "http_transport_origin_mismatch",
            Self::Destination(error) => error.code(),
            Self::PoolExhausted => "http_transport_pool_exhausted",
            Self::ConnectTimeout => "connect_timeout",
            Self::Connect(_) => "connect",
            Self::TlsConfiguration(_) => "tls_configuration",
            Self::TlsServerName(_) => "tls_server_name",
            Self::TlsHandshakeTimeout => "tls_handshake_timeout",
            Self::TlsHandshake(_) => "tls_handshake",
            Self::HandshakeTimeout => "handshake_timeout",
            Self::Hyper(_) => "http_protocol",
            Self::Request(_) => "request_build",
        }
    }

    #[must_use]
    pub const fn retriable(&self) -> bool {
        match self {
            Self::Destination(error) => error.retriable(),
            Self::PoolExhausted
            | Self::ConnectTimeout
            | Self::Connect(_)
            | Self::TlsHandshakeTimeout
            | Self::HandshakeTimeout
            | Self::Hyper(_) => true,
            Self::TlsHandshake(_) => false,
            _ => false,
        }
    }
}

impl fmt::Display for HttpTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Destination(error) => error.fmt(formatter),
            Self::Connect(error) => write!(formatter, "HTTP connect failed: {error}"),
            Self::TlsConfiguration(error) => write!(formatter, "TLS configuration failed: {error}"),
            Self::TlsServerName(error) => write!(formatter, "TLS server name failed: {error}"),
            Self::TlsHandshake(error) => write!(formatter, "TLS handshake failed: {error}"),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for HttpTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Destination(error) => Some(error),
            Self::Connect(error) => Some(error),
            Self::Hyper(error) => Some(error),
            Self::Request(error) => Some(error),
            _ => None,
        }
    }
}

impl From<HttpDestinationError> for HttpTransportError {
    fn from(error: HttpDestinationError) -> Self {
        Self::Destination(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpTransportStats {
    pub connections_opened: u64,
    pub connections_reused: u64,
    pub connections_expired: u64,
    pub connections_poisoned: u64,
    pub pool_exhausted: u64,
    pub tls_handshakes: u64,
    pub tls_failures: u64,
}

#[derive(Default)]
struct Stats {
    connections_opened: AtomicU64,
    connections_reused: AtomicU64,
    connections_expired: AtomicU64,
    connections_poisoned: AtomicU64,
    pool_exhausted: AtomicU64,
    tls_handshakes: AtomicU64,
    tls_failures: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> HttpTransportStats {
        HttpTransportStats {
            connections_opened: self.connections_opened.load(Ordering::Relaxed),
            connections_reused: self.connections_reused.load(Ordering::Relaxed),
            connections_expired: self.connections_expired.load(Ordering::Relaxed),
            connections_poisoned: self.connections_poisoned.load(Ordering::Relaxed),
            pool_exhausted: self.pool_exhausted.load(Ordering::Relaxed),
            tls_handshakes: self.tls_handshakes.load(Ordering::Relaxed),
            tls_failures: self.tls_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct HttpDirectTransport {
    inner: Arc<TransportInner>,
}

impl fmt::Debug for HttpDirectTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpDirectTransport")
            .field("origin", &self.inner.origin)
            .field("keep_alive", &self.inner.config.keep_alive)
            .finish_non_exhaustive()
    }
}

struct TransportInner {
    origin: Uri,
    config: HttpDirectTransportConfig,
    connector: Mutex<HttpConnector>,
    idle: Mutex<VecDeque<IdleConnection>>,
    next_connection: AtomicU64,
    next_idle_token: AtomicU64,
    stats: Arc<Stats>,
}

#[derive(Clone, Debug)]
enum ConnectPeer {
    Pinned(SocketAddr),
    Admitted(Arc<[SocketAddr]>),
    Resolved,
    PolicyResolved(HttpResolver),
}

struct IdleConnection {
    id: u64,
    token: u64,
    inserted: Instant,
    sender: SendRequest,
    driver: tokio::task::JoinHandle<()>,
}

struct ActiveConnection {
    id: u64,
    sender: SendRequest,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for TransportInner {
    fn drop(&mut self) {
        for entry in self.idle.get_mut().drain(..) {
            entry.driver.abort();
        }
    }
}

pub(crate) struct HttpResponseLease {
    transport: Arc<TransportInner>,
    connection: Option<ActiveConnection>,
    reusable: bool,
}

pub(crate) struct HttpTransportResponse {
    pub response: Response<Incoming>,
    pub lease: HttpResponseLease,
}

impl HttpDirectTransport {
    pub fn resolved(
        uri_text: &str,
        config: HttpDirectTransportConfig,
    ) -> Result<Self, HttpTransportError> {
        Self::new(uri_text, ConnectPeer::Resolved, config)
    }

    pub fn pinned(
        uri_text: &str,
        peer: SocketAddr,
        mut config: HttpDirectTransportConfig,
    ) -> Result<Self, HttpTransportError> {
        config.destination = HttpDestinationPolicy {
            resolve_timeout: config.destination.resolve_timeout,
            max_addresses: config.destination.max_addresses,
            allow_loopback: true,
            allow_private: true,
        };
        Self::new(uri_text, ConnectPeer::Pinned(peer), config)
    }

    /// Connects only to a previously policy-admitted answer set. The caller
    /// retains the original URI authority for HTTP and TLS identity while the
    /// connector uses these exact numeric peers for Happy Eyeballs.
    pub fn admitted(
        uri_text: &str,
        addresses: Arc<[SocketAddr]>,
        config: HttpDirectTransportConfig,
    ) -> Result<Self, HttpTransportError> {
        if addresses.is_empty() || addresses.len() > crate::MAX_HTTP_HAPPY_EYEBALLS_ADDRESSES {
            return Err(HttpTransportError::InvalidPolicy);
        }
        let uri: Uri = uri_text
            .parse()
            .map_err(|_| HttpTransportError::InvalidOrigin)?;
        validate_origin(&uri)?;
        let expected_port = uri.port_u16().unwrap_or_else(|| {
            if uri.scheme_str() == Some("https") {
                443
            } else {
                80
            }
        });
        if addresses
            .iter()
            .any(|address| address.port() != expected_port)
        {
            return Err(HttpTransportError::OriginMismatch);
        }
        Self::new(uri_text, ConnectPeer::Admitted(addresses), config)
    }

    pub fn resolved_with(
        uri_text: &str,
        resolver: HttpResolver,
        config: HttpDirectTransportConfig,
    ) -> Result<Self, HttpTransportError> {
        Self::new(uri_text, ConnectPeer::PolicyResolved(resolver), config)
    }

    fn new(
        uri_text: &str,
        peer: ConnectPeer,
        config: HttpDirectTransportConfig,
    ) -> Result<Self, HttpTransportError> {
        validate_config(&config)?;
        let origin: Uri = uri_text
            .parse()
            .map_err(|_| HttpTransportError::InvalidOrigin)?;
        validate_origin(&origin)?;
        let tls = if origin.scheme_str() == Some("https") {
            build_tls_config(&config.tls)?
        } else {
            build_plaintext_tls_config(config.tls.minimum_version)?
        };
        let stats = Arc::new(Stats::default());
        let active_permits = Arc::new(Semaphore::new(config.max_connections_per_origin));
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(PolicyConnector {
                peer,
                destination: config.destination,
                connect_timeout: config.connect_timeout,
                happy_eyeballs_delay: config.happy_eyeballs_delay,
                budgets: config.budgets.clone(),
                active_permits,
                stats: Arc::clone(&stats),
            });
        Ok(Self {
            inner: Arc::new(TransportInner {
                origin,
                config,
                connector: Mutex::new(connector),
                idle: Mutex::new(VecDeque::new()),
                next_connection: AtomicU64::new(1),
                next_idle_token: AtomicU64::new(1),
                stats,
            }),
        })
    }

    pub fn stats(&self) -> HttpTransportStats {
        self.inner.stats.snapshot()
    }

    pub(crate) async fn send(
        &self,
        mut request: Request<HttpBody>,
    ) -> Result<HttpTransportResponse, HttpTransportError> {
        let request_uri = request.uri().clone();
        let path = request_uri
            .path_and_query()
            .map_or("/", hyper::http::uri::PathAndQuery::as_str);
        *request.uri_mut() = path
            .parse()
            .map_err(|_| HttpTransportError::InvalidOrigin)?;
        if request_uri.scheme() != self.inner.origin.scheme()
            || request_uri.authority() != self.inner.origin.authority()
        {
            return Err(HttpTransportError::OriginMismatch);
        }
        let authority = self
            .inner
            .origin
            .authority()
            .ok_or(HttpTransportError::InvalidOrigin)?;
        request.headers_mut().insert(
            HOST,
            authority
                .as_str()
                .parse()
                .map_err(|_| HttpTransportError::InvalidOrigin)?,
        );
        let mut connection = self.take_idle().await;
        let reused = connection.is_some();
        if !reused {
            connection = Some(self.open_connection().await?);
        }
        let mut connection = connection.expect("connection is present");
        if connection.sender.is_closed() {
            self.inner
                .stats
                .connections_poisoned
                .fetch_add(1, Ordering::Relaxed);
            connection.driver.abort();
            connection = self.open_connection().await?;
        } else if reused {
            self.inner
                .stats
                .connections_reused
                .fetch_add(1, Ordering::Relaxed);
        }
        let response = match timeout(
            self.inner.config.handshake_timeout,
            connection.sender.send_request(request),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                connection.driver.abort();
                self.inner
                    .stats
                    .connections_poisoned
                    .fetch_add(1, Ordering::Relaxed);
                return Err(HttpTransportError::Hyper(error));
            }
            Err(_) => {
                connection.driver.abort();
                self.inner
                    .stats
                    .connections_poisoned
                    .fetch_add(1, Ordering::Relaxed);
                return Err(HttpTransportError::HandshakeTimeout);
            }
        };
        Ok(HttpTransportResponse {
            response,
            lease: HttpResponseLease {
                transport: Arc::clone(&self.inner),
                connection: Some(connection),
                reusable: true,
            },
        })
    }

    async fn open_connection(&self) -> Result<ActiveConnection, HttpTransportError> {
        let is_https = self.inner.origin.scheme_str() == Some("https");
        if is_https {
            self.inner
                .stats
                .tls_handshakes
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut connector = self.inner.connector.lock().await;
        std::future::poll_fn(|cx| connector.poll_ready(cx))
            .await
            .map_err(classify_box_error)?;
        let stream = match timeout(
            self.inner.config.handshake_timeout,
            connector.call(self.inner.origin.clone()),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Err(_) => {
                if is_https {
                    self.inner
                        .stats
                        .tls_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(HttpTransportError::TlsHandshakeTimeout);
                }
                return Err(HttpTransportError::ConnectTimeout);
            }
            Ok(Err(error)) => {
                let error = classify_box_error(error);
                if is_https
                    && matches!(
                        error,
                        HttpTransportError::TlsServerName(_) | HttpTransportError::TlsHandshake(_)
                    )
                {
                    self.inner
                        .stats
                        .tls_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(error);
            }
        };
        drop(connector);
        let mut builder = http1::Builder::new();
        builder
            .max_headers(crate::http_first_slice::MAX_RESPONSE_HEADERS)
            .max_buf_size(crate::http_first_slice::MAX_RESPONSE_HEAD_BYTES);
        let (sender, connection) = timeout(
            self.inner.config.handshake_timeout,
            builder.handshake(stream),
        )
        .await
        .map_err(|_| HttpTransportError::HandshakeTimeout)?
        .map_err(HttpTransportError::Hyper)?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let id = self.inner.next_connection.fetch_add(1, Ordering::Relaxed);
        self.inner
            .stats
            .connections_opened
            .fetch_add(1, Ordering::Relaxed);
        Ok(ActiveConnection { id, sender, driver })
    }

    async fn take_idle(&self) -> Option<ActiveConnection> {
        if !self.inner.config.keep_alive {
            return None;
        }
        let now = Instant::now();
        let mut idle = self.inner.idle.lock().await;
        while let Some(entry) = idle.front() {
            if now.duration_since(entry.inserted) >= self.inner.config.idle_timeout {
                let expired = idle.pop_front().expect("front entry exists");
                expired.driver.abort();
                self.inner
                    .stats
                    .connections_expired
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
        idle.pop_back().map(|entry| ActiveConnection {
            id: entry.id,
            sender: entry.sender,
            driver: entry.driver,
        })
    }

    async fn recycle(&self, connection: ActiveConnection) {
        if !self.inner.config.keep_alive || self.inner.config.max_idle_connections_per_origin == 0 {
            connection.driver.abort();
            return;
        }
        if connection.sender.is_closed() {
            connection.driver.abort();
            self.inner
                .stats
                .connections_poisoned
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let token = self.inner.next_idle_token.fetch_add(1, Ordering::Relaxed);
        let mut idle = self.inner.idle.lock().await;
        while idle.len() >= self.inner.config.max_idle_connections_per_origin {
            if let Some(evicted) = idle.pop_front() {
                evicted.driver.abort();
                self.inner
                    .stats
                    .connections_expired
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        idle.push_back(IdleConnection {
            id: connection.id,
            token,
            inserted: Instant::now(),
            sender: connection.sender,
            driver: connection.driver,
        });
        let weak = Arc::downgrade(&self.inner);
        let timeout = self.inner.config.idle_timeout;
        drop(idle);
        tokio::spawn(async move {
            sleep(timeout).await;
            expire_idle(weak, token).await;
        });
    }
}

impl HttpResponseLease {
    pub(crate) async fn recycle(mut self) {
        if let Some(connection) = self.connection.take() {
            if self.reusable {
                let transport = HttpDirectTransport {
                    inner: Arc::clone(&self.transport),
                };
                transport.recycle(connection).await;
            } else {
                connection.driver.abort();
                self.transport
                    .stats
                    .connections_poisoned
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) async fn discard(mut self) {
        if let Some(connection) = self.connection.take() {
            let ActiveConnection { sender, driver, .. } = connection;
            drop(sender);
            driver.abort();
            let _joined = driver.await;
            self.transport
                .stats
                .connections_poisoned
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for HttpResponseLease {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            connection.driver.abort();
            self.transport
                .stats
                .connections_poisoned
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn expire_idle(weak: Weak<TransportInner>, token: u64) {
    let Some(inner) = weak.upgrade() else {
        return;
    };
    let mut idle = inner.idle.lock().await;
    if let Some(index) = idle.iter().position(|entry| entry.token == token)
        && let Some(entry) = idle.remove(index)
    {
        entry.driver.abort();
        inner
            .stats
            .connections_expired
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct PolicyConnector {
    peer: ConnectPeer,
    destination: HttpDestinationPolicy,
    connect_timeout: Duration,
    happy_eyeballs_delay: Duration,
    budgets: HttpTransportBudgets,
    active_permits: Arc<Semaphore>,
    stats: Arc<Stats>,
}

impl Service<Uri> for PolicyConnector {
    type Response = ConnectorStream;
    type Error = HttpTransportError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let peer = self.peer.clone();
        let destination = self.destination;
        let connect_timeout = self.connect_timeout;
        let happy_eyeballs_delay = self.happy_eyeballs_delay;
        let budgets = self.budgets.clone();
        let active_permits = Arc::clone(&self.active_permits);
        let stats = Arc::clone(&self.stats);
        Box::pin(async move {
            let addresses = match peer {
                ConnectPeer::Pinned(peer) => {
                    let expected = uri.port_u16().unwrap_or_else(|| {
                        if uri.scheme_str() == Some("https") {
                            443
                        } else {
                            80
                        }
                    });
                    if peer.port() != expected {
                        return Err(HttpTransportError::OriginMismatch);
                    }
                    vec![peer]
                }
                ConnectPeer::Admitted(addresses) => addresses.to_vec(),
                ConnectPeer::Resolved => {
                    resolve_http_destination(uri.to_string().as_str(), destination)
                        .await
                        .map_err(HttpTransportError::Destination)?
                        .addresses()
                        .to_vec()
                }
                ConnectPeer::PolicyResolved(resolver) => resolve_http_destination_with_resolver(
                    uri.to_string().as_str(),
                    destination,
                    &resolver,
                )
                .await
                .map_err(HttpTransportError::Destination)?
                .addresses()
                .to_vec(),
            };
            let capacity = budgets.try_acquire_connection().inspect_err(|_error| {
                stats.pool_exhausted.fetch_add(1, Ordering::Relaxed);
            })?;
            let origin_socket_permit = active_permits.try_acquire_owned().map_err(|_| {
                stats.pool_exhausted.fetch_add(1, Ordering::Relaxed);
                HttpTransportError::PoolExhausted
            })?;
            let stream = connect_http_happy_eyeballs(
                &addresses,
                HttpHappyEyeballsConfig {
                    connect_timeout,
                    fallback_delay: happy_eyeballs_delay,
                },
            )
            .await
            .map_err(map_happy_eyeballs_error)?
            .stream;
            Ok(TokioIo::new(BudgetedTcpStream {
                stream,
                _capacity: Some(capacity),
                _origin_socket_permit: Some(origin_socket_permit),
            }))
        })
    }
}

struct BudgetedTcpStream {
    stream: TcpStream,
    _capacity: Option<HttpTransportCapacityPermit>,
    _origin_socket_permit: Option<OwnedSemaphorePermit>,
}

impl AsyncRead for BudgetedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for BudgetedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl Connection for BudgetedTcpStream {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

fn validate_config(config: &HttpDirectTransportConfig) -> Result<(), HttpTransportError> {
    if config.connect_timeout.is_zero()
        || config.happy_eyeballs_delay.is_zero()
        || config.handshake_timeout.is_zero()
        || config.idle_timeout.is_zero()
        || config.max_connections_per_origin == 0
        || config.max_connections_per_origin > MAX_HTTP_CONNECTIONS_PER_ORIGIN
        || config.max_idle_connections_per_origin > config.max_connections_per_origin
        || config.max_idle_connections_per_origin > MAX_HTTP_IDLE_CONNECTIONS_PER_ORIGIN
    {
        return Err(HttpTransportError::InvalidPolicy);
    }
    Ok(())
}

fn map_happy_eyeballs_error(error: HttpHappyEyeballsError) -> HttpTransportError {
    match error {
        HttpHappyEyeballsError::Timeout => HttpTransportError::ConnectTimeout,
        HttpHappyEyeballsError::Connect(error) => HttpTransportError::Connect(error),
        HttpHappyEyeballsError::InvalidConfig
        | HttpHappyEyeballsError::NoAddresses
        | HttpHappyEyeballsError::TooManyAddresses => HttpTransportError::InvalidPolicy,
    }
}

fn validate_origin(uri: &Uri) -> Result<(), HttpTransportError> {
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.authority().is_none()
        || uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
    {
        return Err(HttpTransportError::InvalidOrigin);
    }
    Ok(())
}

pub(crate) fn build_tls_config(policy: &HttpTlsPolicy) -> Result<ClientConfig, HttpTransportError> {
    let mut roots = RootCertStore::empty();
    match &policy.trust {
        HttpTrustSource::System | HttpTrustSource::SystemAndCustom(_) => {
            let native = rustls_native_certs::load_native_certs();
            if native.certs.is_empty() {
                return Err(HttpTransportError::TlsConfiguration(
                    "no native trust roots were found".to_owned(),
                ));
            }
            for certificate in native.certs {
                let _ = roots.add(certificate);
            }
        }
        HttpTrustSource::CustomPem(_) => {}
    }
    if let HttpTrustSource::CustomPem(path) | HttpTrustSource::SystemAndCustom(path) = &policy.trust
    {
        let bytes = std::fs::read(path).map_err(|error| {
            HttpTransportError::TlsConfiguration(format!("{}: {error}", path.display()))
        })?;
        if bytes.is_empty() || bytes.len() > MAX_HTTP_TLS_BUNDLE_BYTES {
            return Err(HttpTransportError::TlsConfiguration(
                "custom trust bundle size is outside the bounded range".to_owned(),
            ));
        }
        let mut reader = &bytes[..];
        let mut count = 0_usize;
        while let Some(item) = rustls_pemfile::read_one(&mut reader)
            .map_err(|error| HttpTransportError::TlsConfiguration(error.to_string()))?
        {
            let rustls_pemfile::Item::X509Certificate(certificate) = item else {
                continue;
            };
            count = count
                .checked_add(1)
                .ok_or(HttpTransportError::TlsConfiguration(
                    "custom trust bundle certificate count overflow".to_owned(),
                ))?;
            if count > MAX_HTTP_TLS_BUNDLE_CERTIFICATES {
                return Err(HttpTransportError::TlsConfiguration(
                    "custom trust bundle has too many certificates".to_owned(),
                ));
            }
            roots
                .add(certificate)
                .map_err(|error| HttpTransportError::TlsConfiguration(error.to_string()))?;
        }
        if count == 0 {
            return Err(HttpTransportError::TlsConfiguration(
                "custom trust bundle contains no certificates".to_owned(),
            ));
        }
    }
    if roots.is_empty() {
        return Err(HttpTransportError::TlsConfiguration(
            "TLS trust store is empty".to_owned(),
        ));
    }
    let builder = match policy.minimum_version {
        HttpMinimumTlsVersion::Tls12 => {
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|error| HttpTransportError::TlsConfiguration(error.to_string()))?
        }
        HttpMinimumTlsVersion::Tls13 => {
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|error| HttpTransportError::TlsConfiguration(error.to_string()))?
        }
    };
    Ok(builder.with_root_certificates(roots).with_no_client_auth())
}

fn build_plaintext_tls_config(
    minimum_version: HttpMinimumTlsVersion,
) -> Result<ClientConfig, HttpTransportError> {
    let builder = match minimum_version {
        HttpMinimumTlsVersion::Tls12 => {
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|error| HttpTransportError::TlsConfiguration(error.to_string()))?
        }
        HttpMinimumTlsVersion::Tls13 => {
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|error| HttpTransportError::TlsConfiguration(error.to_string()))?
        }
    };
    Ok(builder
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth())
}

fn classify_box_error(error: BoxError) -> HttpTransportError {
    match error.downcast::<HttpTransportError>() {
        Ok(error) => *error,
        Err(error) => {
            if error
                .downcast_ref::<rustls::pki_types::InvalidDnsNameError>()
                .is_some()
            {
                HttpTransportError::TlsServerName(error.to_string())
            } else {
                HttpTransportError::TlsHandshake(error.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HttpDirectTransport, HttpDirectTransportConfig, HttpMinimumTlsVersion, HttpTlsPolicy,
        HttpTransportBudgets, HttpTransportError, HttpTransportResponse, HttpTrustSource,
        MAX_HTTP_CONNECTIONS_PER_ORIGIN,
    };
    use bytes::Bytes;
    use http_body_util::{BodyExt as _, Empty};
    use hyper::Request;
    use rustls::{ServerConfig, ServerConnection, StreamOwned};
    use std::fs;
    use std::io::{self, Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::mpsc::{self, Sender};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    const TEST_ROOT_CERTIFICATE_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUYRDo2MpuEgKXCDw0hfysW5VzQBcwDQYJKoZIhvcNAQEL
BQAwGjEYMBYGA1UEAwwPQXJpYXgtVGVzdC1Sb290MB4XDTI2MDgxMDEzMjYzNFoX
DTQ2MDgwNTEzMjYzNFowGjEYMBYGA1UEAwwPQXJpYXgtVGVzdC1Sb290MIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzZ/d+WQ3lS7Nt5PwwaDKZUvJXiCy
bkHNfq7orqbMMUgUP2JBCWo2tzqQPXvRxthFkFBDH3NRR/vnSDg21+rQ3qhXXz00
TXcgJhhj/FkSKLHO1ZhC14Rc2xs2h7Aj+n0eGgnbAWJP82GzWUexUitl0xokVel7
UpcO4umEahNHqh71odnUgw+QsvVqXrpSbtPalNEZcsSQPLfl3/GsvToguPugQium
8Ao/r/em+u8aFit8fVQGMI3f6VZ/QnaCvZY8879egsOST7vb/IpyCvm7Ts6n3yb8
GA5vHCtNTl9gtqBP8sXaDexDd6hFnugMK+yf9aI5AR9Dg0+8CpgypPOMTQIDAQAB
o2MwYTAdBgNVHQ4EFgQUbxCdEAE77Mh61dEBJNIqerMBt+AwHwYDVR0jBBgwFoAU
bxCdEAE77Mh61dEBJNIqerMBt+AwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8E
BAMCAQYwDQYJKoZIhvcNAQELBQADggEBADua7hiQAVuIV9lBVqWCPBBb+nxy/aLi
Z2cihYYVU8TOXpSdafs88ilKHKwjsGjQJ/EMtwuodVMUvDkNnbCPaL9cn7ndOsiH
BLog6kvtnrsEJg73TNHFLZqCZtXZh6DtkpdzSkgKBsv0dM6xDP1h++SFo1FfqrMf
DBVSr43QT49dhD+h7stSsUk2SkFZZIKRr3rgLK311SrcAyA57FsxeWnVN3Y/7Oqp
RUhsngPVjApKFaY1OeX85OnQDVoZx/HXrW/MalHbVzgHA02Chkb2niZcZSsAGaYW
2Xv4TpH7Iif2/VboynFidH8mJvtIZQ0XovJ3MJj9s5mI4BhKpuHsnMs=
-----END CERTIFICATE-----
"#;
    const TEST_SERVER_CERTIFICATE_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDSTCCAjGgAwIBAgIUAkgSlGiapxoB0M1+ZzWwnXhrZlcwDQYJKoZIhvcNAQEL
BQAwGjEYMBYGA1UEAwwPQXJpYXgtVGVzdC1Sb290MB4XDTI2MDgxMDEzMjYzNVoX
DTQ2MDgwNTEzMjYzNVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwFWTDQ5quzqVr95CLXn07N53cms/F3O73xPR
5nSdk/Ia19xvVHjOvwNEd8jMl6dOw3D+pgnc0JzsTPBOZ/a+Ni267Eq+V6LyCVE+
gjcbo9mHqr0fn5HbQ/C+2dVAjD8eh6t3esw1ArD3ty2RwnQ2LjAdU30hV3xPv1/0
c9X7IzSl/U57oDs7Moc9Zjwbc5UDx4r9VVybhsN9cbf8vkKMYRlQn06HGZljWaXg
4zSJJrk9iKAUPvPZN6BROD99/1IRXW79AMwxO08Wdb4g3XLkblfU2vTUnWWdZpQ9
pV+PJr87ntP0RzMxBdyVX5jd+d8Y1dEBtE5lbJZJXakWkTi2aQIDAQABo4GMMIGJ
MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUF
BwMBMBQGA1UdEQQNMAuCCWxvY2FsaG9zdDAdBgNVHQ4EFgQUxIHQAGEY+ZcfJF/M
xzhnZCRdAEIwHwYDVR0jBBgwFoAUbxCdEAE77Mh61dEBJNIqerMBt+AwDQYJKoZI
hvcNAQELBQADggEBACdrnaQyGy9jZ9LzGa0Lc1tHrXcAJdSZiOWyrAudtT2jLPXY
Qfko2K0STdYhhwrpzrmaVZsnK8GgD9bTRDRF16oK4FYiEhWeiSvdSMUzWoYd/jpF
3pF6vhhiWB66s2U1dlqBK7lHH9Sx2bdSepGd6ZZMDHcFPwtld4DVG6FH39XrFfLg
jFBLJ8tVVbMnHicJy7yHCK8al02lIitaeRrhoYFo/D1xjedbqbM3X0yjVBPUFitf
yppm99zRyd7uVBG2d/P9DLwLOOrhBWxMieNaNWUY7ndoGy9/iAftsFjW0t0Ec0Y0
n7wzt2edf/EBFmQAanKpn2DYXZ1nxNsqkxtGcRo=
-----END CERTIFICATE-----
"#;
    const TEST_SERVER_PRIVATE_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDAVZMNDmq7OpWv
3kItefTs3ndyaz8Xc7vfE9HmdJ2T8hrX3G9UeM6/A0R3yMyXp07DcP6mCdzQnOxM
8E5n9r42LbrsSr5XovIJUT6CNxuj2YeqvR+fkdtD8L7Z1UCMPx6Hq3d6zDUCsPe3
LZHCdDYuMB1TfSFXfE+/X/Rz1fsjNKX9TnugOzsyhz1mPBtzlQPHiv1VXJuGw31x
t/y+QoxhGVCfTocZmWNZpeDjNIkmuT2IoBQ+89k3oFE4P33/UhFdbv0AzDE7TxZ1
viDdcuRuV9Ta9NSdZZ1mlD2lX48mvzue0/RHMzEF3JVfmN353xjV0QG0TmVslkld
qRaROLZpAgMBAAECggEADFaI/wPLXryevNgPCnwJ9VZ9ltxQoSxiU0iCFOyq7aJQ
rMOGLEbuK1R2NFFwJ3PbAoBZg5T7InJImdRqETMDx3W2SaFvVa+dI3If5opKtn92
O6KTeGDqzhBP6+kpUX3ck2MxYFIgfe5Ui76LfMwH7D1XqkMLDCHMO1h4VeR2jm7e
FVHO4opactkTzN0D/3Hd2QZzZnKeV9p4v7VMpcV0Cw23vrKHz+BcDeSEihZHyVHE
l5S7Sy932hYvHBsZGagrqhGIdHL1/BX7f7NMvd6FotkHiPelfZUKGMVbI1hNlbeR
AJzoAlOcis2+sJzFhC7Hi0GjMrYXdHy0BlZPVnEEMQKBgQDfXNRngXgM+TpGlkTg
rOmEOmXJesQGqyOZ9/tnkCNMcx/8bX9X4BaZp2YYMTFCCjzznm8VLvrqJ1qLnYlC
XSqd3xL9osp90U9ApxfaucBUp1S+Ni3Ar4vFmhoiaeES6s1VGy9qJZP3fNItBxSZ
obVLbVvId9BCD3BoGIntvOK0kQKBgQDccBbPrbNq6+WgxGRjU0d07qOU/7tTaF+x
p4pHbmc8bYcq5juDBLytmB8yAqNPqR0UugswjtGKq9P7blLfyMYiAytV1+dK702F
dqwIJ94pDz5QSUps+mpzsf3Ya8ciaKVl/VG7xHbWj0HOFUdEgcu5iiahFxZIiLMJ
LM4hmyDwWQKBgQDRqWRjiB71JphyG6mpsAU+LkbPOeJ5U/mGFEUzcBQCNepnWyz2
go0UTBLEUKCpGc0e7K/elYvHcYtHlGd8GNHhALzlwgIK2gdna7Ezibqke7FLHrYR
sXYk1MMFXJd911NIOM1n+MAMxmjPBV9r2mO/2nYWFYkyCSX9QFNwCiZPUQKBgQDY
RFwM6oTRDJjfzm4TCHxdm1b/8pmtLgRcflvq0sUUAv0OuIyAcSBPS6SnYvEoUWlH
kXMy85te6k9yKP3Dse25JtTYRpcT7I1ouFH1Om/6ZosjJ5SOMGxKD8FVGABpoLNM
yWfryMcyn5/W+QdPjev6nzBg8Q6aoQrNoJinXdPGGQKBgQDLxOPf03/GVDe51FQk
apEvJAPxzGTXJXfmcRsWOHVuzOeONpa5XDXCXOO0QcVtKIYJ/xtP36zmb4sg0ZBl
e31pxMIvRBTw+dGS6spzZo+W4ft31it0tEUmShjy5iE5lqwPpp9GaF3UadN+fWJy
2ZAoPkzw2eKtZ3TZOYa422yctg==
-----END PRIVATE KEY-----
"#;

    #[test]
    fn default_transport_policy_is_bounded_and_tls12_minimum() {
        let config = HttpDirectTransportConfig::default();
        assert_eq!(config.tls.minimum_version, HttpMinimumTlsVersion::Tls12);
        assert!(config.keep_alive);
        assert_eq!(config.max_connections_per_origin, 1);
        assert_eq!(config.max_idle_connections_per_origin, 1);
        assert_eq!(config.budgets.available_sockets(), 1);
    }

    #[test]
    fn invalid_pool_shape_is_rejected() {
        let config = HttpDirectTransportConfig {
            max_connections_per_origin: MAX_HTTP_CONNECTIONS_PER_ORIGIN + 1,
            ..HttpDirectTransportConfig::default()
        };
        let error = super::HttpDirectTransport::resolved("http://example.com/", config)
            .expect_err("oversized connection cap");
        assert!(matches!(error, HttpTransportError::InvalidPolicy));
    }

    #[test]
    fn custom_trust_policy_is_cloneable_and_bounded() {
        let config = HttpDirectTransportConfig {
            tls: super::HttpTlsPolicy {
                minimum_version: HttpMinimumTlsVersion::Tls13,
                trust: HttpTrustSource::CustomPem(PathBuf::from("ca.pem")),
            },
            max_connections_per_origin: NonZeroUsize::new(1).unwrap().get(),
            idle_timeout: Duration::from_secs(1),
            ..HttpDirectTransportConfig::default()
        };
        assert_eq!(config.tls.minimum_version, HttpMinimumTlsVersion::Tls13);
        assert!(matches!(config.tls.trust, HttpTrustSource::CustomPem(_)));
        let _ = HttpTransportBudgets::default();
    }

    #[test]
    fn plaintext_origin_does_not_require_loading_tls_roots() {
        let config = HttpDirectTransportConfig {
            tls: HttpTlsPolicy {
                minimum_version: HttpMinimumTlsVersion::Tls12,
                trust: HttpTrustSource::CustomPem(PathBuf::from(
                    "this-trust-bundle-does-not-exist.pem",
                )),
            },
            ..HttpDirectTransportConfig::default()
        };
        HttpDirectTransport::pinned(
            "http://127.0.0.1:8080/file",
            "127.0.0.1:8080".parse().expect("peer"),
            config,
        )
        .expect("plaintext transport construction does not load TLS roots");
    }

    #[tokio::test]
    async fn trusted_https_reuses_one_origin_connection() {
        let trust = TestTrustFile::new();
        let (peer, server) = spawn_tls_server(2);
        let uri = format!("https://localhost:{}/file", peer.port());
        let transport = HttpDirectTransport::pinned(&uri, peer, tls_transport_config(trust.path()))
            .expect("transport");

        for _ in 0..2 {
            let HttpTransportResponse { response, lease } = transport
                .send(empty_request(&uri))
                .await
                .expect("HTTPS response");
            let body = response
                .into_body()
                .collect()
                .await
                .expect("response body")
                .to_bytes();
            assert_eq!(body, Bytes::from_static(b"ok"));
            lease.recycle().await;
        }

        server.join().expect("TLS server");
        let stats = transport.stats();
        assert_eq!(stats.connections_opened, 1);
        assert_eq!(stats.connections_reused, 1);
        assert_eq!(stats.tls_handshakes, 1);
        assert_eq!(stats.tls_failures, 0);
    }

    #[tokio::test]
    async fn https_hostname_mismatch_is_terminal_and_not_pooled() {
        let trust = TestTrustFile::new();
        let (peer, server) = spawn_tls_rejection_server();
        let uri = format!("https://127.0.0.1:{}/file", peer.port());
        let transport = HttpDirectTransport::pinned(&uri, peer, tls_transport_config(trust.path()))
            .expect("transport");

        let error = match transport.send(empty_request(&uri)).await {
            Err(error) => error,
            Ok(_) => panic!("certificate name mismatch was accepted"),
        };
        assert!(matches!(error, HttpTransportError::TlsHandshake(_)));
        assert_eq!(error.code(), "tls_handshake");
        assert!(!error.retriable());

        server.join().expect("TLS rejection server");
        let stats = transport.stats();
        assert_eq!(stats.connections_opened, 0);
        assert_eq!(stats.connections_reused, 0);
        assert_eq!(stats.tls_handshakes, 1);
        assert_eq!(stats.tls_failures, 1);
    }

    #[tokio::test]
    async fn held_response_lease_exhausts_bounded_pool() {
        let (peer, server, release) = spawn_plaintext_hold_server();
        let uri = format!("http://127.0.0.1:{}/file", peer.port());
        let transport =
            HttpDirectTransport::pinned(&uri, peer, HttpDirectTransportConfig::default())
                .expect("transport");
        let HttpTransportResponse { response, lease } = transport
            .send(empty_request(&uri))
            .await
            .expect("first response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        assert_eq!(body, Bytes::from_static(b"ok"));

        let error = match transport.send(empty_request(&uri)).await {
            Err(error) => error,
            Ok(_) => panic!("held lease did not consume the only connection slot"),
        };
        assert!(matches!(error, HttpTransportError::PoolExhausted));
        assert_eq!(error.code(), "http_transport_pool_exhausted");
        assert!(error.retriable());
        drop(lease);
        release.send(()).expect("release plaintext server");

        server.join().expect("plaintext server");
        let stats = transport.stats();
        assert_eq!(stats.pool_exhausted, 1);
        assert_eq!(stats.connections_poisoned, 1);
        assert_eq!(stats.tls_handshakes, 0);
        assert_eq!(stats.tls_failures, 0);
    }

    #[tokio::test]
    async fn disabled_keep_alive_closes_without_poisoning() {
        let (peer, server, release) = spawn_plaintext_hold_server();
        let uri = format!("http://127.0.0.1:{}/file", peer.port());
        let config = HttpDirectTransportConfig {
            keep_alive: false,
            ..HttpDirectTransportConfig::default()
        };
        let transport = HttpDirectTransport::pinned(&uri, peer, config).expect("transport");
        let HttpTransportResponse { response, lease } =
            transport.send(empty_request(&uri)).await.expect("response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        assert_eq!(body, Bytes::from_static(b"ok"));
        lease.recycle().await;
        release.send(()).expect("release plaintext server");
        server.join().expect("plaintext server");

        let stats = transport.stats();
        assert_eq!(stats.connections_opened, 1);
        assert_eq!(stats.connections_poisoned, 0);
    }

    #[tokio::test]
    async fn dropping_transport_releases_idle_connection_budgets() {
        let (peer, server, release) = spawn_plaintext_hold_server();
        let uri = format!("http://127.0.0.1:{}/file", peer.port());
        let budgets = HttpTransportBudgets::new(1, super::HTTP_CONNECTION_RESERVATION_BYTES)
            .expect("budgets");
        let config = HttpDirectTransportConfig {
            budgets: budgets.clone(),
            ..HttpDirectTransportConfig::default()
        };
        let transport = HttpDirectTransport::pinned(&uri, peer, config).expect("transport");
        let HttpTransportResponse { response, lease } =
            transport.send(empty_request(&uri)).await.expect("response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        assert_eq!(body, Bytes::from_static(b"ok"));
        lease.recycle().await;
        assert_eq!(budgets.available_sockets(), 0);
        assert_eq!(
            budgets.connection_memory_used(),
            super::HTTP_CONNECTION_RESERVATION_BYTES
        );

        drop(transport);
        tokio::time::timeout(Duration::from_secs(1), async {
            while budgets.available_sockets() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("idle connection permit release");
        assert_eq!(budgets.available_sockets(), 1);
        assert_eq!(budgets.connection_memory_used(), 0);
        release.send(()).expect("release plaintext server");
        server.join().expect("plaintext server");
    }

    fn empty_request(uri: &str) -> Request<Empty<Bytes>> {
        Request::builder()
            .uri(uri)
            .body(Empty::new())
            .expect("request")
    }

    fn tls_transport_config(trust: &std::path::Path) -> HttpDirectTransportConfig {
        HttpDirectTransportConfig {
            tls: HttpTlsPolicy {
                minimum_version: HttpMinimumTlsVersion::Tls12,
                trust: HttpTrustSource::CustomPem(trust.to_path_buf()),
            },
            ..HttpDirectTransportConfig::default()
        }
    }

    struct TestTrustFile(PathBuf);

    impl TestTrustFile {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "ariax-http-test-root-{}-{}.pem",
                std::process::id(),
                NEXT.fetch_add(1, AtomicOrdering::Relaxed)
            ));
            fs::write(&path, TEST_ROOT_CERTIFICATE_PEM).expect("write test trust root");
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TestTrustFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn test_server_config() -> Arc<ServerConfig> {
        let mut certificates = TEST_SERVER_CERTIFICATE_PEM.as_bytes();
        let certificates = rustls_pemfile::certs(&mut certificates)
            .collect::<Result<Vec<_>, _>>()
            .expect("server certificate");
        let mut private_key = TEST_SERVER_PRIVATE_KEY_PEM.as_bytes();
        let private_key = rustls_pemfile::private_key(&mut private_key)
            .expect("parse server private key")
            .expect("server private key");
        let mut config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("TLS versions")
                .with_no_client_auth()
                .with_single_cert(certificates, private_key)
                .expect("server identity");
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Arc::new(config)
    }

    fn spawn_tls_server(request_count: usize) -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("TLS listener");
        let peer = listener.local_addr().expect("TLS listener address");
        let config = test_server_config();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("TLS accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("TLS read timeout");
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .expect("TLS write timeout");
            let connection = ServerConnection::new(config).expect("TLS server connection");
            let mut stream = StreamOwned::new(connection, stream);
            for _ in 0..request_count {
                let head = read_http_head(&mut stream).expect("HTTP request over TLS");
                assert!(head.starts_with(b"GET /file HTTP/1.1\r\n"));
                assert!(stream.conn.alpn_protocol().is_none());
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .expect("TLS response");
                stream.flush().expect("TLS response flush");
            }
        });
        (peer, server)
    }

    fn spawn_tls_rejection_server() -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("TLS listener");
        let peer = listener.local_addr().expect("TLS listener address");
        let config = test_server_config();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("TLS accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("TLS read timeout");
            let connection = ServerConnection::new(config).expect("TLS server connection");
            let mut stream = StreamOwned::new(connection, stream);
            let mut byte = [0_u8; 1];
            let _ = stream.read(&mut byte);
        });
        (peer, server)
    }

    fn spawn_plaintext_hold_server() -> (SocketAddr, JoinHandle<()>, Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("HTTP listener");
        let peer = listener.local_addr().expect("HTTP listener address");
        let (release, released) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("HTTP accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("HTTP read timeout");
            let head = read_http_head(&mut stream).expect("HTTP request");
            assert!(head.starts_with(b"GET /file HTTP/1.1\r\n"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .expect("HTTP response");
            stream.flush().expect("HTTP response flush");
            released.recv().expect("plaintext server release");
        });
        (peer, server, release)
    }

    fn read_http_head(reader: &mut impl Read) -> io::Result<Vec<u8>> {
        let mut head = Vec::new();
        let mut byte = [0_u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if head.len() >= 16 * 1024 {
                return Err(io::Error::other("test HTTP head exceeded bound"));
            }
            if reader.read(&mut byte)? == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            head.push(byte[0]);
        }
        Ok(head)
    }
}
