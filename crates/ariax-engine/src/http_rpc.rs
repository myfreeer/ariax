//! Bounded JSON-RPC 2.0 framing shared by loopback HTTP and stdio.

use crate::RpcClientContext;
use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _};
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

pub const MAX_HTTP_RPC_REQUEST_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_HTTP_RPC_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_HTTP_RPC_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_HTTP_RPC_CONNECTIONS: usize = 64;
pub const MAX_RPC_BATCH_MEMBERS: usize = 256;
pub const MAX_RPC_MULTICALL_MEMBERS: usize = 256;
pub const DEFAULT_HTTP_RPC_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

const RPC_UNAUTHORIZED: i64 = -32001;
const RPC_RESPONSE_TOO_LARGE: i64 = -32006;
const MAX_MULTICALL_RESULT_BYTES: usize =
    MAX_HTTP_RPC_RESPONSE_BYTES - MAX_HTTP_RPC_REQUEST_BYTES - 1024;

/// The aria2-compatible method surface implemented or explicitly rejected by
/// the shared control dispatcher. Keeping the catalog here ensures every
/// transport reports the same vocabulary.
pub const RPC_METHODS: &[&str] = &[
    "aria2.addUri",
    "aria2.addTorrent",
    "aria2.getPeers",
    "aria2.addMetalink",
    "aria2.remove",
    "aria2.pause",
    "aria2.forcePause",
    "aria2.pauseAll",
    "aria2.forcePauseAll",
    "aria2.unpause",
    "aria2.unpauseAll",
    "aria2.forceRemove",
    "aria2.changePosition",
    "aria2.tellStatus",
    "aria2.getUris",
    "aria2.getFiles",
    "aria2.getServers",
    "aria2.tellActive",
    "aria2.tellWaiting",
    "aria2.tellStopped",
    "aria2.getOption",
    "aria2.changeUri",
    "aria2.changeOption",
    "aria2.getGlobalOption",
    "aria2.changeGlobalOption",
    "aria2.purgeDownloadResult",
    "aria2.removeDownloadResult",
    "aria2.getVersion",
    "aria2.getSessionInfo",
    "aria2.shutdown",
    "aria2.forceShutdown",
    "aria2.getGlobalStat",
    "aria2.saveSession",
    "system.multicall",
    "system.listMethods",
    "system.listNotifications",
    "ariax.subscribe",
    "ariax.unsubscribe",
    "ariax.pollEvents",
    "ariax.replaceSources",
    "ariax.checkConfig",
    "ariax.reloadConfig",
    "ariax.dumpConfig",
    "ariax.exportSession",
    "ariax.importSession",
];

pub const RPC_NOTIFICATIONS: &[&str] = &[
    "aria2.onDownloadStart",
    "aria2.onDownloadPause",
    "aria2.onDownloadStop",
    "aria2.onDownloadComplete",
    "aria2.onDownloadError",
    "aria2.onBtDownloadComplete",
    "ariax.onStatus",
    "ariax.onShutdown",
    "ariax.onError",
];

pub type RpcFuture = Pin<Box<dyn Future<Output = Result<Value, HttpRpcBackendError>> + Send>>;

/// Backend implemented by the real scheduler control plane.
pub trait HttpRpcBackend: Send + Sync + 'static {
    fn call(&self, method: &str, params: Value) -> RpcFuture;

    fn call_with_context(
        &self,
        method: &str,
        params: Value,
        _context: RpcClientContext,
    ) -> RpcFuture {
        self.call(method, params)
    }

    fn authentication_required(&self) -> bool {
        false
    }
}

/// Method-token authentication shared by all JSON-RPC transports. The secret
/// is deliberately omitted from `Debug` output.
#[derive(Clone, Default)]
pub struct RpcAuthPolicy {
    secret: Option<Arc<str>>,
}

impl fmt::Debug for RpcAuthPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcAuthPolicy")
            .field("secret_configured", &self.secret.is_some())
            .finish()
    }
}

impl RpcAuthPolicy {
    #[must_use]
    pub fn with_secret(secret: impl Into<Arc<str>>) -> Self {
        Self {
            secret: Some(secret.into()),
        }
    }

    #[must_use]
    pub const fn is_required(&self) -> bool {
        self.secret.is_some()
    }

    fn authorize(&self, params: Value) -> Result<Value, HttpRpcBackendError> {
        let Some(secret) = &self.secret else {
            return Ok(params);
        };
        let Value::Array(mut params) = params else {
            return Err(unauthorized());
        };
        let supplied = params
            .first()
            .and_then(Value::as_str)
            .and_then(|value| value.strip_prefix("token:"))
            .ok_or_else(unauthorized)?;
        if !constant_time_eq(supplied.as_bytes(), secret.as_bytes()) {
            return Err(unauthorized());
        }
        params.remove(0);
        Ok(Value::Array(params))
    }
}

/// Transport-neutral method dispatcher. HTTP, stdio, WebSocket, CLI, and
/// embedding adapters all wrap their backend with this type.
#[derive(Clone)]
pub struct RpcDispatcher<B> {
    backend: Arc<B>,
    auth: RpcAuthPolicy,
}

impl<B> RpcDispatcher<B> {
    #[must_use]
    pub fn new(backend: Arc<B>, auth: RpcAuthPolicy) -> Self {
        Self { backend, auth }
    }

    #[must_use]
    pub fn backend(&self) -> Arc<B> {
        self.backend.clone()
    }
}

impl<B: HttpRpcBackend> HttpRpcBackend for RpcDispatcher<B> {
    fn call(&self, method: &str, params: Value) -> RpcFuture {
        self.call_with_context(method, params, RpcClientContext::default())
    }

    fn authentication_required(&self) -> bool {
        self.auth.is_required()
    }

    fn call_with_context(
        &self,
        method: &str,
        params: Value,
        context: RpcClientContext,
    ) -> RpcFuture {
        let backend = self.backend.clone();
        let auth = self.auth.clone();
        let method = method.to_owned();
        Box::pin(async move {
            if method == "system.multicall" {
                return multicall(backend, auth, params, context).await;
            }
            let params = auth.authorize(params)?;
            authorize_client_events(&context)?;
            match method.as_str() {
                "system.listMethods" => {
                    require_empty_params(&params, "listMethods")?;
                    Ok(Value::Array(
                        RPC_METHODS
                            .iter()
                            .map(|method| Value::String((*method).to_owned()))
                            .collect(),
                    ))
                }
                "system.listNotifications" => {
                    require_empty_params(&params, "listNotifications")?;
                    Ok(Value::Array(
                        RPC_NOTIFICATIONS
                            .iter()
                            .map(|method| Value::String((*method).to_owned()))
                            .collect(),
                    ))
                }
                _ => backend.call_with_context(&method, params, context).await,
            }
        })
    }
}

fn authorize_client_events(context: &RpcClientContext) -> Result<(), HttpRpcBackendError> {
    context
        .authorize()
        .map_err(|_| HttpRpcBackendError::new(-32005, "Event subscription unavailable"))
}

impl<B: RpcWebSocketBackend> RpcWebSocketBackend for RpcDispatcher<B> {
    fn event_broker(&self) -> crate::RpcEventBroker {
        self.backend.event_broker()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRpcBackendError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl HttpRpcBackendError {
    #[must_use]
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

impl fmt::Display for HttpRpcBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for HttpRpcBackendError {}

#[derive(Debug)]
pub enum HttpRpcTransportError {
    Io(io::Error),
    Hyper(hyper::Error),
    InvalidBind(SocketAddr),
    RequestTooLarge,
    ResponseTooLarge,
    InvalidFrame(&'static str),
    Backend(HttpRpcBackendError),
    WebSocket(tokio_tungstenite::tungstenite::Error),
    Event(crate::RpcEventError),
}

impl fmt::Display for HttpRpcTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Hyper(error) => error.fmt(formatter),
            Self::InvalidBind(address) => {
                write!(formatter, "RPC bind address is not loopback: {address}")
            }
            Self::RequestTooLarge => formatter.write_str("RPC request exceeds the bounded size"),
            Self::ResponseTooLarge => formatter.write_str("RPC response exceeds the bounded size"),
            Self::InvalidFrame(message) => formatter.write_str(message),
            Self::Backend(error) => error.fmt(formatter),
            Self::WebSocket(error) => error.fmt(formatter),
            Self::Event(error) => error.fmt(formatter),
        }
    }
}

impl Error for HttpRpcTransportError {}

impl From<io::Error> for HttpRpcTransportError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<hyper::Error> for HttpRpcTransportError {
    fn from(error: hyper::Error) -> Self {
        Self::Hyper(error)
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for HttpRpcTransportError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(error)
    }
}

impl From<crate::RpcEventError> for HttpRpcTransportError {
    fn from(error: crate::RpcEventError) -> Self {
        Self::Event(error)
    }
}

pub trait RpcWebSocketBackend: HttpRpcBackend {
    fn event_broker(&self) -> crate::RpcEventBroker;
}

/// Dispatches one JSON-RPC request. Parsing and response serialization are
/// bounded before the backend is called.
pub async fn dispatch_json<B: HttpRpcBackend>(backend: &B, bytes: &[u8]) -> Vec<u8> {
    dispatch_json_with_context(backend, bytes, &RpcClientContext::default()).await
}

async fn dispatch_json_with_context<B: HttpRpcBackend>(
    backend: &B,
    bytes: &[u8],
    context: &RpcClientContext,
) -> Vec<u8> {
    let parsed = serde_json::from_slice::<Value>(bytes);
    let response = match parsed {
        Ok(Value::Array(requests)) if requests.is_empty() => {
            error_response(Value::Null, -32600, "Invalid Request", None)
        }
        Ok(Value::Array(requests)) if requests.len() > MAX_RPC_BATCH_MEMBERS => error_response(
            Value::Null,
            -32600,
            "Batch limit exceeded",
            Some(json!({"limit": MAX_RPC_BATCH_MEMBERS})),
        ),
        Ok(Value::Array(requests)) => {
            let mut responses = Vec::with_capacity(requests.len());
            let mut response_bytes = 2_usize;
            for request in requests {
                if let Some(response) = dispatch_value(backend, request, context).await {
                    let separator = usize::from(!responses.is_empty());
                    let remaining = MAX_HTTP_RPC_RESPONSE_BYTES
                        .saturating_sub(response_bytes)
                        .saturating_sub(separator);
                    let Some(member_bytes) = serialized_value_size(&response, remaining) else {
                        return bounded_response_too_large(responses.len());
                    };
                    response_bytes = response_bytes
                        .saturating_add(separator)
                        .saturating_add(member_bytes);
                    responses.push(response);
                }
            }
            if responses.is_empty() {
                return Vec::new();
            }
            Value::Array(responses)
        }
        Ok(request) => {
            let Some(response) = dispatch_value(backend, request, context).await else {
                return Vec::new();
            };
            response
        }
        Err(_) => error_response(Value::Null, -32700, "Parse error", None),
    };
    serialize_response(response).unwrap_or_else(|_| bounded_response_too_large(0))
}

async fn dispatch_value<B: HttpRpcBackend>(
    backend: &B,
    request: Value,
    context: &RpcClientContext,
) -> Option<Value> {
    let object = match request.as_object() {
        Some(object) => object,
        None => return Some(error_response(Value::Null, -32600, "Invalid Request", None)),
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Some(error_response(
            object.get("id").cloned().unwrap_or(Value::Null),
            -32600,
            "Invalid Request",
            None,
        ));
    }
    let notification = !object.contains_key("id");
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if !notification && (id.is_null() || !(id.is_string() || id.is_number())) {
        return Some(error_response(Value::Null, -32600, "Invalid Request", None));
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Some(error_response(id, -32600, "Invalid Request", None));
    };
    let params = object
        .get("params")
        .cloned()
        .unwrap_or(Value::Array(Vec::new()));
    if !(params.is_array() || params.is_object()) {
        return Some(error_response(id, -32602, "Invalid params", None));
    }
    if notification {
        let _ = backend
            .call_with_context(method, params, context.clone())
            .await;
        return None;
    }
    Some(
        match backend
            .call_with_context(method, params, context.clone())
            .await
        {
            Ok(result) => json!({"jsonrpc":"2.0", "id":id, "result":result}),
            Err(error) => error_response(id, error.code, &error.message, error.data),
        },
    )
}

fn error_response(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = serde_json::Map::new();
    error.insert("code".to_owned(), Value::from(code));
    error.insert("message".to_owned(), Value::from(message.to_owned()));
    if let Some(data) = data {
        error.insert("data".to_owned(), data);
    }
    json!({"jsonrpc":"2.0", "id":id, "error":Value::Object(error)})
}

fn serialize_response(response: Value) -> Result<Vec<u8>, HttpRpcTransportError> {
    let mut writer = BoundedVecWriter::new(MAX_HTTP_RPC_RESPONSE_BYTES);
    serde_json::to_writer(&mut writer, &response)
        .map_err(|_| HttpRpcTransportError::ResponseTooLarge)?;
    Ok(writer.finish())
}

struct BoundedVecWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedVecWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedVecWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "serialized RPC response exceeds limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn multicall<B: HttpRpcBackend>(
    backend: Arc<B>,
    auth: RpcAuthPolicy,
    params: Value,
    context: RpcClientContext,
) -> Result<Value, HttpRpcBackendError> {
    let values = params
        .as_array()
        .ok_or_else(|| HttpRpcBackendError::new(-32602, "multicall params must be an array"))?;
    let calls = values
        .first()
        .and_then(Value::as_array)
        .filter(|_| values.len() == 1)
        .ok_or_else(|| HttpRpcBackendError::new(-32602, "multicall requires one method array"))?;
    if calls.len() > MAX_RPC_MULTICALL_MEMBERS {
        return Err(HttpRpcBackendError::new(-32602, "multicall limit exceeded")
            .with_data(json!({"limit": MAX_RPC_MULTICALL_MEMBERS})));
    }
    let mut results = Vec::with_capacity(calls.len());
    let mut result_bytes = 2_usize;
    for call in calls {
        let Some(object) = call.as_object() else {
            append_multicall_result(
                &mut results,
                &mut result_bytes,
                multicall_error(HttpRpcBackendError::new(
                    -32602,
                    "multicall member must be an object",
                )),
            )?;
            continue;
        };
        let Some(method) = object.get("methodName").and_then(Value::as_str) else {
            append_multicall_result(
                &mut results,
                &mut result_bytes,
                multicall_error(HttpRpcBackendError::new(
                    -32602,
                    "multicall member requires methodName",
                )),
            )?;
            continue;
        };
        if method == "system.multicall" {
            append_multicall_result(
                &mut results,
                &mut result_bytes,
                multicall_error(HttpRpcBackendError::new(
                    -32602,
                    "nested multicall is not supported",
                )),
            )?;
            continue;
        }
        let member_params = object
            .get("params")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let member_params = match auth.authorize(member_params) {
            Ok(params) => params,
            Err(error) => {
                append_multicall_result(&mut results, &mut result_bytes, multicall_error(error))?;
                continue;
            }
        };
        if let Err(error) = authorize_client_events(&context) {
            append_multicall_result(&mut results, &mut result_bytes, multicall_error(error))?;
            continue;
        }
        let result = match method {
            "system.listMethods" => require_empty_params(&member_params, "listMethods").map(|()| {
                Value::Array(
                    RPC_METHODS
                        .iter()
                        .map(|method| Value::String((*method).to_owned()))
                        .collect(),
                )
            }),
            "system.listNotifications" => require_empty_params(&member_params, "listNotifications")
                .map(|()| {
                    Value::Array(
                        RPC_NOTIFICATIONS
                            .iter()
                            .map(|method| Value::String((*method).to_owned()))
                            .collect(),
                    )
                }),
            _ => {
                backend
                    .call_with_context(method, member_params, context.clone())
                    .await
            }
        };
        let member = match result {
            Ok(value) => Value::Array(vec![value]),
            Err(error) => multicall_error(error),
        };
        append_multicall_result(&mut results, &mut result_bytes, member)?;
    }
    Ok(Value::Array(results))
}

fn append_multicall_result(
    results: &mut Vec<Value>,
    result_bytes: &mut usize,
    member: Value,
) -> Result<(), HttpRpcBackendError> {
    let separator = usize::from(!results.is_empty());
    let remaining = MAX_MULTICALL_RESULT_BYTES
        .saturating_sub(*result_bytes)
        .saturating_sub(separator);
    let Some(member_bytes) = serialized_value_size(&member, remaining) else {
        return Err(
            HttpRpcBackendError::new(RPC_RESPONSE_TOO_LARGE, "Response too large").with_data(
                json!({
                    "limit": MAX_MULTICALL_RESULT_BYTES,
                    "completed": results.len().saturating_add(1),
                }),
            ),
        );
    };
    *result_bytes = (*result_bytes)
        .saturating_add(separator)
        .saturating_add(member_bytes);
    results.push(member);
    Ok(())
}

fn multicall_error(error: HttpRpcBackendError) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("code".to_owned(), Value::from(error.code));
    object.insert("message".to_owned(), Value::from(error.message));
    if let Some(data) = error.data {
        object.insert("data".to_owned(), data);
    }
    Value::Object(object)
}

fn require_empty_params(params: &Value, method: &str) -> Result<(), HttpRpcBackendError> {
    if params.as_array().is_some_and(Vec::is_empty) {
        Ok(())
    } else {
        Err(HttpRpcBackendError::new(
            -32602,
            format!("{method} takes no params"),
        ))
    }
}

fn unauthorized() -> HttpRpcBackendError {
    HttpRpcBackendError::new(RPC_UNAUTHORIZED, "Unauthorized")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

fn bounded_response_too_large(completed: usize) -> Vec<u8> {
    serialize_response(error_response(
        Value::Null,
        RPC_RESPONSE_TOO_LARGE,
        "Response too large",
        Some(json!({
            "limit": MAX_HTTP_RPC_RESPONSE_BYTES,
            "completed": completed,
        })),
    ))
    .expect("bounded JSON-RPC error fits the response cap")
}

fn serialized_value_size(value: &impl serde::Serialize, limit: usize) -> Option<usize> {
    struct Counter {
        used: usize,
        limit: usize,
    }

    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let next = self.used.saturating_add(bytes.len());
            if next > self.limit {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "serialized RPC response exceeds limit",
                ));
            }
            self.used = next;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter { used: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .ok()
        .map(|()| counter.used)
}

/// Serves JSON-RPC over HTTP/1.1 on an explicitly loopback listener.
pub async fn serve_loopback_http<B: HttpRpcBackend>(
    bind: SocketAddr,
    backend: Arc<B>,
) -> Result<(), HttpRpcTransportError> {
    if !bind.ip().is_loopback() {
        return Err(HttpRpcTransportError::InvalidBind(bind));
    }
    let listener = TcpListener::bind(bind).await?;
    serve_loopback_http_listener(listener, backend).await
}

/// Serves loopback HTTP until the supplied shutdown future resolves. New
/// connections stop immediately; established HTTP/1.1 connections receive a
/// bounded graceful-drain window before any remaining tasks are aborted.
pub async fn serve_loopback_http_until<B, F>(
    bind: SocketAddr,
    backend: Arc<B>,
    shutdown: F,
) -> Result<(), HttpRpcTransportError>
where
    B: HttpRpcBackend,
    F: Future<Output = io::Result<()>> + Send,
{
    if !bind.ip().is_loopback() {
        return Err(HttpRpcTransportError::InvalidBind(bind));
    }
    let listener = TcpListener::bind(bind).await?;
    serve_loopback_http_listener_until(listener, backend, shutdown).await
}

/// Serves one already-bound loopback listener. Keeping binding separate lets
/// callers reserve a port and attach an orderly shutdown policy in tests or
/// embedding hosts without widening the public bind surface.
pub async fn serve_loopback_http_listener<B: HttpRpcBackend>(
    listener: TcpListener,
    backend: Arc<B>,
) -> Result<(), HttpRpcTransportError> {
    serve_loopback_http_listener_until(listener, backend, std::future::pending()).await
}

/// Listener form of [`serve_loopback_http_until`], used by embedding hosts and
/// tests that reserve the loopback port before starting the server.
pub async fn serve_loopback_http_listener_until<B, F>(
    listener: TcpListener,
    backend: Arc<B>,
    shutdown: F,
) -> Result<(), HttpRpcTransportError>
where
    B: HttpRpcBackend,
    F: Future<Output = io::Result<()>> + Send,
{
    if !listener
        .local_addr()
        .map_err(HttpRpcTransportError::Io)?
        .ip()
        .is_loopback()
    {
        return Err(HttpRpcTransportError::InvalidBind(
            listener.local_addr().map_err(HttpRpcTransportError::Io)?,
        ));
    }
    let permits = Arc::new(Semaphore::new(MAX_HTTP_RPC_CONNECTIONS));
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            result = &mut shutdown => break result.map_err(HttpRpcTransportError::Io),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(HttpRpcTransportError::Io(error)),
                };
                let permit = match permits.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => continue,
                };
                let backend = backend.clone();
                let shutdown = shutdown_receiver.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    serve_http_connection(stream, backend, shutdown).await
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if joined.is_some_and(|joined| joined.is_err()) {
                    break Err(HttpRpcTransportError::InvalidFrame(
                        "RPC connection task failed",
                    ));
                }
            }
        }
    };

    drop(listener);
    let _ = shutdown_sender.send(true);
    if tokio::time::timeout(DEFAULT_HTTP_RPC_SHUTDOWN_TIMEOUT, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    result
}

async fn serve_http_connection<B: HttpRpcBackend>(
    stream: TcpStream,
    backend: Arc<B>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), HttpRpcTransportError> {
    let io = TokioIo::new(stream);
    let connection = http1::Builder::new().keep_alive(true).serve_connection(
        io,
        service_fn(move |request| {
            let backend = backend.clone();
            async move { Ok::<_, Infallible>(http_request(backend, request).await) }
        }),
    );
    tokio::pin!(connection);
    loop {
        tokio::select! {
            result = &mut connection => return result.map_err(HttpRpcTransportError::Hyper),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    connection.as_mut().graceful_shutdown();
                    return connection.await.map_err(HttpRpcTransportError::Hyper);
                }
            }
        }
    }
}

async fn http_request<B: HttpRpcBackend>(
    backend: Arc<B>,
    request: Request<Incoming>,
) -> Response<Full<Bytes>> {
    if request.method() != Method::POST || request.uri().path() != "/jsonrpc" {
        return plain_response(StatusCode::NOT_FOUND, b"not found");
    }
    let body = match Limited::new(request.into_body(), MAX_HTTP_RPC_REQUEST_BYTES)
        .collect()
        .await
    {
        Ok(body) => body.to_bytes(),
        Err(_) => return plain_response(StatusCode::PAYLOAD_TOO_LARGE, b"request too large"),
    };
    let bytes = dispatch_json(backend.as_ref(), &body).await;
    if bytes.len() > MAX_HTTP_RPC_RESPONSE_BYTES {
        return plain_response(StatusCode::INTERNAL_SERVER_ERROR, b"response too large");
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .header("content-length", bytes.len())
        .body(Full::new(Bytes::from(bytes)))
        .expect("static HTTP response headers are valid")
}

fn plain_response(status: StatusCode, body: &[u8]) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("content-length", body.len())
        .body(Full::new(Bytes::copy_from_slice(body)))
        .expect("static HTTP response headers are valid")
}

/// Serves JSON-RPC and bounded pushed events over a dedicated loopback
/// WebSocket listener.
pub async fn serve_loopback_websocket_until<B, F>(
    bind: SocketAddr,
    backend: Arc<B>,
    shutdown: F,
) -> Result<(), HttpRpcTransportError>
where
    B: RpcWebSocketBackend,
    F: Future<Output = io::Result<()>> + Send,
{
    if !bind.ip().is_loopback() {
        return Err(HttpRpcTransportError::InvalidBind(bind));
    }
    let listener = TcpListener::bind(bind).await?;
    serve_loopback_websocket_listener_until(listener, backend, shutdown).await
}

pub async fn serve_loopback_websocket_listener_until<B, F>(
    listener: TcpListener,
    backend: Arc<B>,
    shutdown: F,
) -> Result<(), HttpRpcTransportError>
where
    B: RpcWebSocketBackend,
    F: Future<Output = io::Result<()>> + Send,
{
    let address = listener.local_addr()?;
    if !address.ip().is_loopback() {
        return Err(HttpRpcTransportError::InvalidBind(address));
    }
    let permits = Arc::new(Semaphore::new(MAX_HTTP_RPC_CONNECTIONS));
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            result = &mut shutdown => break result.map_err(HttpRpcTransportError::Io),
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(HttpRpcTransportError::Io)?;
                let permit = match permits.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => continue,
                };
                let backend = backend.clone();
                let receiver = shutdown_receiver.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    serve_websocket_connection(stream, backend, receiver).await
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if joined.is_some_and(|joined| joined.is_err()) {
                    break Err(HttpRpcTransportError::InvalidFrame("RPC WebSocket task failed"));
                }
            }
        }
    };
    drop(listener);
    let _ = shutdown_sender.send(true);
    if tokio::time::timeout(DEFAULT_HTTP_RPC_SHUTDOWN_TIMEOUT, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    result
}

async fn serve_websocket_connection<B: RpcWebSocketBackend>(
    stream: TcpStream,
    backend: Arc<B>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), HttpRpcTransportError> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_HTTP_RPC_REQUEST_BYTES))
        .max_frame_size(Some(MAX_HTTP_RPC_REQUEST_BYTES));
    let mut socket = tokio_tungstenite::accept_async_with_config(stream, Some(config)).await?;
    let context =
        RpcClientContext::with_events(backend.event_broker(), backend.authentication_required())?;
    let mut event_poll = tokio::time::interval(Duration::from_millis(10));
    event_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let notice = json!({
                        "jsonrpc":"2.0",
                        "method":"ariax.onShutdown",
                        "params":{},
                    });
                    if context.is_authenticated() {
                        socket.send(Message::Text(notice.to_string().into())).await?;
                    }
                    socket.close(None).await?;
                    return Ok(());
                }
            }
            _ = event_poll.tick() => {
                match context.try_next_event() {
                    Ok(Some(delivery)) => {
                        let bytes = serde_json::to_vec(&delivery.into_value())
                            .map_err(|_| HttpRpcTransportError::ResponseTooLarge)?;
                        if bytes.len() > MAX_HTTP_RPC_RESPONSE_BYTES {
                            return Err(HttpRpcTransportError::ResponseTooLarge);
                        }
                        let text = String::from_utf8(bytes).map_err(|_| {
                            HttpRpcTransportError::InvalidFrame("event JSON is not UTF-8")
                        })?;
                        socket.send(Message::Text(text.into())).await?;
                    }
                    Ok(None) => {}
                    Err(crate::RpcEventError::Disconnected(
                        crate::RpcEventDisconnect::SlowConsumer,
                    )) => {
                        let error = json!({
                            "jsonrpc":"2.0",
                            "method":"ariax.onError",
                            "params":{"code":"slow_consumer"},
                        });
                        socket.send(Message::Text(error.to_string().into())).await?;
                        socket.close(None).await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            message = socket.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                match message? {
                    Message::Text(text) => {
                        let response = dispatch_json_with_context(backend.as_ref(), text.as_bytes(), &context).await;
                        if response.is_empty() {
                            continue;
                        }
                        let response = String::from_utf8(response).map_err(|_| {
                            HttpRpcTransportError::InvalidFrame("response JSON is not UTF-8")
                        })?;
                        socket.send(Message::Text(response.into())).await?;
                    }
                    Message::Binary(bytes) => {
                        let response = dispatch_json_with_context(backend.as_ref(), &bytes, &context).await;
                        if response.is_empty() {
                            continue;
                        }
                        socket.send(Message::Binary(response.into())).await?;
                    }
                    Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => return Ok(()),
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

/// Runs the same dispatcher over LSP-style `Content-Length` stdio frames.
pub async fn run_content_length_stdio<B, R, W>(
    backend: Arc<B>,
    mut reader: R,
    mut writer: W,
) -> Result<(), HttpRpcTransportError>
where
    B: HttpRpcBackend,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let Some(length) = read_content_length(&mut reader).await? else {
            return Ok(());
        };
        if length > MAX_HTTP_RPC_REQUEST_BYTES {
            return Err(HttpRpcTransportError::RequestTooLarge);
        }
        let mut body = vec![0_u8; length];
        reader.read_exact(&mut body).await?;
        let response = dispatch_json(backend.as_ref(), &body).await;
        if response.is_empty() {
            continue;
        }
        if response.len() > MAX_HTTP_RPC_RESPONSE_BYTES {
            return Err(HttpRpcTransportError::ResponseTooLarge);
        }
        write_content_length_message(&mut writer, &response).await?;
    }
}

/// Runs Content-Length stdio with the same bounded pushed event stream used by
/// WebSocket clients. A bounded reader channel applies request backpressure and
/// a single writer serializes responses and notifications without interleaving.
pub async fn run_content_length_stdio_with_events<B, R, W>(
    backend: Arc<B>,
    mut reader: R,
    mut writer: W,
) -> Result<(), HttpRpcTransportError>
where
    B: RpcWebSocketBackend,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    const PENDING_REQUESTS: usize = 4;
    let (sender, mut requests) = mpsc::channel(PENDING_REQUESTS);
    let reader_task = tokio::spawn(async move {
        loop {
            let length = match read_content_length(&mut reader).await {
                Ok(Some(length)) => length,
                Ok(None) => return,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            if length > MAX_HTTP_RPC_REQUEST_BYTES {
                let _ = sender
                    .send(Err(HttpRpcTransportError::RequestTooLarge))
                    .await;
                return;
            }
            let mut body = vec![0_u8; length];
            if let Err(error) = reader.read_exact(&mut body).await {
                let _ = sender.send(Err(HttpRpcTransportError::Io(error))).await;
                return;
            }
            if sender.send(Ok(body)).await.is_err() {
                return;
            }
        }
    });
    let context =
        RpcClientContext::with_events(backend.event_broker(), backend.authentication_required())?;
    let mut event_poll = tokio::time::interval(Duration::from_millis(10));
    event_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else {
                    break Ok(());
                };
                let body = request?;
                let response = dispatch_json_with_context(backend.as_ref(), &body, &context).await;
                if !response.is_empty() {
                    write_content_length_message(&mut writer, &response).await?;
                }
            }
            _ = event_poll.tick() => {
                match context.try_next_event() {
                    Ok(Some(delivery)) => {
                        let event = serialize_response(delivery.into_value())?;
                        write_content_length_message(&mut writer, &event).await?;
                    }
                    Ok(None) => {}
                    Err(crate::RpcEventError::Disconnected(
                        crate::RpcEventDisconnect::SlowConsumer,
                    )) => {
                        let event = serialize_response(json!({
                            "jsonrpc":"2.0",
                            "method":"ariax.onError",
                            "params":{"code":"slow_consumer"},
                        }))?;
                        write_content_length_message(&mut writer, &event).await?;
                        break Ok(());
                    }
                    Err(error) => break Err(error.into()),
                }
            }
        }
    };
    reader_task.abort();
    let _ = reader_task.await;
    result
}

async fn write_content_length_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &[u8],
) -> Result<(), HttpRpcTransportError> {
    writer
        .write_all(format!("Content-Length: {}\r\n\r\n", message.len()).as_bytes())
        .await?;
    writer.write_all(message).await?;
    writer.flush().await?;
    Ok(())
}

/// Runs the same bounded dispatcher over newline-delimited JSON. Blank lines
/// are ignored; each non-empty line is one complete JSON-RPC request or batch.
pub async fn run_ndjson_stdio<B, R, W>(
    backend: Arc<B>,
    mut reader: R,
    mut writer: W,
) -> Result<(), HttpRpcTransportError>
where
    B: HttpRpcBackend,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut line = Vec::new();
    loop {
        line.clear();
        let mut byte = [0_u8; 1];
        loop {
            let read = reader.read(&mut byte).await?;
            if read == 0 {
                if line.is_empty() {
                    return Ok(());
                }
                break;
            }
            if byte[0] == b'\n' {
                break;
            }
            if byte[0] != b'\r' {
                line.push(byte[0]);
                if line.len() > MAX_HTTP_RPC_REQUEST_BYTES {
                    return Err(HttpRpcTransportError::RequestTooLarge);
                }
            }
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let response = dispatch_json(backend.as_ref(), &line).await;
        if response.is_empty() {
            continue;
        }
        if response.len() > MAX_HTTP_RPC_RESPONSE_BYTES {
            return Err(HttpRpcTransportError::ResponseTooLarge);
        }
        writer.write_all(&response).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
}

async fn read_content_length<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<usize>, HttpRpcTransportError> {
    let mut header = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        let read = reader.read(&mut byte).await?;
        if read == 0 {
            if header.is_empty() {
                return Ok(None);
            }
            return Err(HttpRpcTransportError::InvalidFrame("truncated RPC header"));
        }
        header.push(byte[0]);
        if header.len() > MAX_HTTP_RPC_HEADER_BYTES {
            return Err(HttpRpcTransportError::RequestTooLarge);
        }
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = std::str::from_utf8(&header)
        .map_err(|_| HttpRpcTransportError::InvalidFrame("RPC header is not UTF-8"))?;
    let mut length = None;
    for line in text.split("\r\n").filter(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            return Err(HttpRpcTransportError::InvalidFrame("malformed RPC header"));
        };
        if name.eq_ignore_ascii_case("content-length") {
            if length.is_some() {
                return Err(HttpRpcTransportError::InvalidFrame(
                    "duplicate Content-Length",
                ));
            }
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| HttpRpcTransportError::InvalidFrame("invalid Content-Length"))?;
            length = Some(parsed);
        }
    }
    length
        .ok_or(HttpRpcTransportError::InvalidFrame(
            "missing Content-Length",
        ))
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::duplex;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    struct Echo;

    impl HttpRpcBackend for Echo {
        fn call(&self, method: &str, params: Value) -> RpcFuture {
            let method = method.to_owned();
            Box::pin(async move { Ok(json!({"method": method, "params": params})) })
        }
    }

    #[derive(Clone)]
    struct EventEcho {
        events: crate::RpcEventBroker,
    }

    impl HttpRpcBackend for EventEcho {
        fn call(&self, method: &str, params: Value) -> RpcFuture {
            let method = method.to_owned();
            Box::pin(async move { Ok(json!({"method": method, "params": params})) })
        }
    }

    impl RpcWebSocketBackend for EventEcho {
        fn event_broker(&self) -> crate::RpcEventBroker {
            self.events.clone()
        }
    }

    #[tokio::test]
    async fn dispatcher_rejects_invalid_and_keeps_json_rpc_shape() {
        let backend = Echo;
        let response = dispatch_json(
            &backend,
            br#"{"jsonrpc":"2.0","id":1,"method":"x","params":[]}"#,
        )
        .await;
        let value: Value = serde_json::from_slice(&response).expect("response JSON");
        assert_eq!(value["result"]["method"], "x");
        let invalid = dispatch_json(&backend, b"not-json").await;
        assert_eq!(
            serde_json::from_slice::<Value>(&invalid).expect("error JSON")["error"]["code"],
            -32700
        );
    }

    #[tokio::test]
    async fn dispatcher_bounds_batches_and_reports_catalogs() {
        let backend = Echo;
        let response = dispatch_json(
            &backend,
            br#"[{"jsonrpc":"2.0","id":1,"method":"x","params":[]},{"jsonrpc":"2.0","id":2,"method":"y","params":[]}]"#,
        )
        .await;
        let value: Value = serde_json::from_slice(&response).expect("batch response JSON");
        assert_eq!(value.as_array().expect("batch").len(), 2);

        let dispatcher = RpcDispatcher::new(Arc::new(Echo), RpcAuthPolicy::default());
        let methods = dispatcher
            .call("system.listMethods", json!([]))
            .await
            .expect("method catalog");
        assert!(
            methods
                .as_array()
                .expect("methods")
                .contains(&Value::String("aria2.tellStatus".to_owned()))
        );
        let notifications = dispatcher
            .call("system.listNotifications", json!([]))
            .await
            .expect("notification catalog");
        assert!(
            notifications
                .as_array()
                .expect("notifications")
                .contains(&Value::String("aria2.onDownloadComplete".to_owned()))
        );
    }

    #[tokio::test]
    async fn notifications_are_dispatched_without_a_response() {
        let backend = Echo;
        let response =
            dispatch_json(&backend, br#"{"jsonrpc":"2.0","method":"x","params":[]}"#).await;
        assert!(response.is_empty());
        let response = dispatch_json(
            &backend,
            br#"[{"jsonrpc":"2.0","method":"x","params":[]},{"jsonrpc":"2.0","id":2,"method":"y","params":[]}]"#,
        )
        .await;
        let value: Value = serde_json::from_slice(&response).expect("response JSON");
        assert_eq!(value.as_array().expect("batch").len(), 1);
        assert_eq!(value[0]["id"], 2);
    }

    #[tokio::test]
    async fn method_token_is_required_for_every_multicall_member() {
        let dispatcher = RpcDispatcher::new(
            Arc::new(Echo),
            RpcAuthPolicy::with_secret(Arc::<str>::from("correct")),
        );
        let unauthorized = dispatcher
            .call("x", json!(["token:wrong", 1]))
            .await
            .expect_err("wrong token");
        assert_eq!(unauthorized.code, RPC_UNAUTHORIZED);

        let result = dispatcher
            .call(
                "system.multicall",
                json!([
                    [
                        {"methodName":"one", "params":["token:correct", 1]},
                        {"methodName":"two", "params":["token:wrong", 2]}
                    ]
                ]),
            )
            .await
            .expect("member-token multicall");
        let results = result.as_array().expect("multicall results");
        assert_eq!(results[0][0]["params"], json!([1]));
        assert_eq!(results[1]["code"], RPC_UNAUTHORIZED);
    }

    #[tokio::test]
    async fn multicall_rejects_members_beyond_the_bound() {
        let dispatcher = RpcDispatcher::new(Arc::new(Echo), RpcAuthPolicy::default());
        let calls = (0..=MAX_RPC_MULTICALL_MEMBERS)
            .map(|_| json!({"methodName":"x", "params":[]}))
            .collect::<Vec<_>>();
        let error = dispatcher
            .call("system.multicall", json!([calls]))
            .await
            .expect_err("oversized multicall");
        assert_eq!(error.code, -32602);
        assert_eq!(
            error.data,
            Some(json!({"limit": MAX_RPC_MULTICALL_MEMBERS}))
        );
    }

    #[tokio::test]
    async fn invalid_envelopes_and_tokens_never_authorize_events() {
        let events = crate::RpcEventBroker::new();
        let dispatcher = RpcDispatcher::new(Arc::new(Echo), RpcAuthPolicy::with_secret("correct"));
        let context = RpcClientContext::with_events(events.clone(), true).expect("context");
        let invalid = [
            json!({"jsonrpc":"2.0", "id":1, "method":"system.multicall", "params":[]}),
            json!({"jsonrpc":"2.0", "id":1, "method":"system.multicall", "params":[[]]}),
            json!({"jsonrpc":"2.0", "id":1, "method":"system.multicall", "params":["token:correct", []]}),
            json!({"jsonrpc":"2.0", "id":1, "method":"system.multicall", "params":[[
                {"methodName":"system.multicall", "params":["token:correct", []]},
                {"methodName":"x", "params":["token:wrong"]},
                {"methodName":"x"},
                {"params":["token:correct"]},
                null
            ]]}),
            json!({"jsonrpc":"2.0", "id":1, "method":"system.multicall", "params":[
                vec![json!({"methodName":"x", "params":["token:correct"]}); MAX_RPC_MULTICALL_MEMBERS + 1]
            ]}),
            json!({"jsonrpc":"invalid", "id":1, "method":"x", "params":["token:correct"]}),
        ];
        for request in invalid {
            let bytes = serde_json::to_vec(&request).expect("request JSON");
            let _ = dispatch_json_with_context(&dispatcher, &bytes, &context).await;
            assert!(!context.is_authenticated(), "invalid request: {request}");
            assert_eq!(events.subscriber_count(), 0);
        }
        let response = dispatch_json_with_context(
            &dispatcher,
            br#"[{"jsonrpc":"2.0","id":1,"method":"x","params":["token:wrong"]},{"jsonrpc":"2.0","id":2,"method":"system.multicall","params":[[{"methodName":"x","params":["token:correct",3]}]]}]"#,
            &context,
        ).await;
        let response: Value = serde_json::from_slice(&response).expect("response");
        assert_eq!(response[0]["error"]["code"], RPC_UNAUTHORIZED);
        assert_eq!(response[1]["result"][0][0]["params"], json!([3]));
        assert!(context.is_authenticated());
        assert_eq!(events.subscriber_count(), 1);
        drop(context);
        assert_eq!(events.subscriber_count(), 0);
    }

    struct PublishingBackend {
        events: crate::RpcEventBroker,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl HttpRpcBackend for PublishingBackend {
        fn call(&self, _method: &str, _params: Value) -> RpcFuture {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.events.publish(
                crate::RpcEvent::notification(
                    "ariax.onTest",
                    json!({"firstCall":true}),
                    crate::RpcEventClass::Reliable,
                    None,
                )
                .expect("event"),
            );
            Box::pin(async { Err(HttpRpcBackendError::new(-32004, "Not found")) })
        }
    }

    impl RpcWebSocketBackend for PublishingBackend {
        fn event_broker(&self) -> crate::RpcEventBroker {
            self.events.clone()
        }
    }

    async fn read_stdio_value(stream: &mut (impl AsyncRead + Unpin)) -> Value {
        let length = read_content_length(stream)
            .await
            .expect("frame header")
            .expect("frame");
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await.expect("frame body");
        serde_json::from_slice(&bytes).expect("frame JSON")
    }

    #[tokio::test]
    async fn stdio_authentication_precedes_first_method_event_and_survives_method_error() {
        let events = crate::RpcEventBroker::new();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let backend = Arc::new(RpcDispatcher::new(
            Arc::new(PublishingBackend {
                events: events.clone(),
                calls: calls.clone(),
            }),
            RpcAuthPolicy::with_secret("correct"),
        ));
        let (mut client, server) = duplex(16 * 1024);
        let (reader, writer) = tokio::io::split(server);
        let task = tokio::spawn(run_content_length_stdio_with_events(
            backend, reader, writer,
        ));
        write_content_length_message(
            &mut client,
            br#"{"jsonrpc":"2.0","id":1,"method":"x","params":["token:wrong"]}"#,
        )
        .await
        .expect("invalid request");
        assert_eq!(
            read_stdio_value(&mut client).await["error"]["code"],
            RPC_UNAUTHORIZED
        );
        assert_eq!(events.subscriber_count(), 0);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
        write_content_length_message(&mut client, br#"{"jsonrpc":"2.0","id":2,"method":"system.multicall","params":[[{"methodName":"x","params":["token:correct"]}]]}"#).await.expect("authorized request");
        assert_eq!(
            read_stdio_value(&mut client).await["result"][0]["code"],
            -32004
        );
        let event = tokio::time::timeout(Duration::from_secs(1), read_stdio_value(&mut client))
            .await
            .expect("first-call event");
        assert_eq!(event["method"], "ariax.onTest");
        assert_eq!(events.subscriber_count(), 1);
        write_content_length_message(
            &mut client,
            br#"{"jsonrpc":"2.0","id":3,"method":"x","params":[]}"#,
        )
        .await
        .expect("missing token");
        assert_eq!(
            read_stdio_value(&mut client).await["error"]["code"],
            RPC_UNAUTHORIZED
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        client.shutdown().await.expect("EOF");
        task.await.expect("join").expect("stdio shutdown");
        assert_eq!(events.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn websocket_authentication_is_connection_local_and_resets_on_reconnect() {
        let events = crate::RpcEventBroker::new();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let backend = Arc::new(RpcDispatcher::new(
            Arc::new(PublishingBackend {
                events: events.clone(),
                calls: calls.clone(),
            }),
            RpcAuthPolicy::with_secret("correct"),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(serve_loopback_websocket_listener_until(
            listener,
            backend,
            async move {
                shutdown_rx
                    .await
                    .map_err(|_| io::Error::other("shutdown dropped"))
            },
        ));
        let url = format!("ws://{address}/jsonrpc");
        let (mut authorized, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("client one");
        let (mut anonymous, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("client two");
        assert_eq!(events.subscriber_count(), 0);
        authorized.send(Message::Text(r#"{"jsonrpc":"2.0","id":1,"method":"system.multicall","params":[[{"methodName":"x","params":["token:correct"]}]]}"#.into())).await.expect("request");
        let response = authorized.next().await.expect("response").expect("frame");
        assert_eq!(
            serde_json::from_str::<Value>(response.to_text().expect("text")).expect("JSON")["result"]
                [0]["code"],
            -32004
        );
        let event = tokio::time::timeout(Duration::from_secs(1), authorized.next())
            .await
            .expect("event timeout")
            .expect("event")
            .expect("frame");
        assert_eq!(
            serde_json::from_str::<Value>(event.to_text().expect("text")).expect("JSON")["method"],
            "ariax.onTest"
        );
        anonymous
            .send(Message::Text(
                r#"{"jsonrpc":"2.0","id":2,"method":"x","params":[]}"#.into(),
            ))
            .await
            .expect("anonymous request");
        let response = anonymous
            .next()
            .await
            .expect("anonymous response")
            .expect("frame");
        assert_eq!(
            serde_json::from_str::<Value>(response.to_text().expect("text")).expect("JSON")["error"]
                ["code"],
            RPC_UNAUTHORIZED
        );
        assert_eq!(events.subscriber_count(), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        authorized
            .close(None)
            .await
            .expect("close authenticated client");
        let (mut reconnected, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("reconnect");
        reconnected
            .send(Message::Text(
                r#"{"jsonrpc":"2.0","id":3,"method":"x","params":["token:wrong"]}"#.into(),
            ))
            .await
            .expect("bad reconnect token");
        let response = reconnected
            .next()
            .await
            .expect("reconnect response")
            .expect("frame");
        assert_eq!(
            serde_json::from_str::<Value>(response.to_text().expect("text")).expect("JSON")["error"]
                ["code"],
            RPC_UNAUTHORIZED
        );
        shutdown_tx.send(()).expect("shutdown");
        assert!(matches!(
            anonymous.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
        assert!(matches!(
            reconnected.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
        task.await.expect("server join").expect("server shutdown");
        assert_eq!(events.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn http_accepts_member_tokens_without_an_outer_token() {
        let backend = Arc::new(RpcDispatcher::new(
            Arc::new(Echo),
            RpcAuthPolicy::with_secret("correct"),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(serve_loopback_http_listener_until(
            listener,
            backend,
            async move {
                shutdown_rx
                    .await
                    .map_err(|_| io::Error::other("shutdown dropped"))
            },
        ));
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"system.multicall","params":[[{"methodName":"x","params":["token:correct",1]},{"methodName":"x","params":[]}]]}"#;
        let request = format!(
            "POST /jsonrpc HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let response = raw_http(address, request.as_bytes()).await;
        let start = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .expect("headers")
            + 4;
        let value: Value = serde_json::from_slice(&response[start..]).expect("response JSON");
        assert_eq!(value["result"][0][0]["params"], json!([1]));
        assert_eq!(value["result"][1]["code"], RPC_UNAUTHORIZED);
        shutdown_tx.send(()).expect("shutdown");
        task.await.expect("join").expect("shutdown");
    }

    #[tokio::test]
    async fn content_length_stdio_round_trips_one_frame() {
        let (mut client, server) = duplex(4096);
        let (server_reader, server_writer) = tokio::io::split(server);
        let backend = Arc::new(Echo);
        let task = tokio::spawn(run_content_length_stdio(
            backend,
            server_reader,
            server_writer,
        ));
        client
            .write_all(b"Content-Length: 49\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"x\",\"params\":[]}")
            .await
            .expect("write frame");
        client.shutdown().await.expect("close request side");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");
        task.await.expect("stdio task join").expect("stdio task");
        assert!(response.starts_with(b"Content-Length: "));
        let separator = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response separator");
        let body = &response[separator + 4..];
        let value: Value = serde_json::from_slice(body).expect("response JSON");
        assert_eq!(value["result"]["method"], "x");
    }

    #[tokio::test]
    async fn content_length_stdio_pushes_bounded_events() {
        let (mut client, server) = duplex(16 * 1024);
        let (server_reader, server_writer) = tokio::io::split(server);
        let events = crate::RpcEventBroker::new();
        let backend = Arc::new(EventEcho {
            events: events.clone(),
        });
        let task = tokio::spawn(run_content_length_stdio_with_events(
            backend,
            server_reader,
            server_writer,
        ));
        client
            .write_all(b"Content-Length: 49\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"x\",\"params\":[]}")
            .await
            .expect("write request");
        let response_length = read_content_length(&mut client)
            .await
            .expect("response header")
            .expect("response length");
        let mut response = vec![0_u8; response_length];
        client
            .read_exact(&mut response)
            .await
            .expect("response body");
        assert_eq!(
            serde_json::from_slice::<Value>(&response).expect("response JSON")["result"]["method"],
            "x"
        );

        events.publish(
            crate::RpcEvent::notification(
                "ariax.test",
                json!({"sequence": 1}),
                crate::RpcEventClass::Reliable,
                None,
            )
            .expect("event"),
        );
        let event_length =
            tokio::time::timeout(Duration::from_secs(1), read_content_length(&mut client))
                .await
                .expect("event deadline")
                .expect("event header")
                .expect("event length");
        let mut event = vec![0_u8; event_length];
        client.read_exact(&mut event).await.expect("event body");
        assert_eq!(
            serde_json::from_slice::<Value>(&event).expect("event JSON")["method"],
            "ariax.test"
        );
        client.shutdown().await.expect("close request side");
        task.await.expect("stdio task join").expect("stdio task");
    }

    #[tokio::test]
    async fn content_length_stdio_rejects_ambiguous_and_missing_lengths() {
        for (frame, expected) in [
            (
                b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}".as_slice(),
                "duplicate Content-Length",
            ),
            (
                b"Content-Type: application/json\r\n\r\n{}".as_slice(),
                "missing Content-Length",
            ),
            (
                b"Content-Length: no\r\n\r\n".as_slice(),
                "invalid Content-Length",
            ),
        ] {
            let (mut client, server) = duplex(4096);
            let (server_reader, server_writer) = tokio::io::split(server);
            let task = tokio::spawn(run_content_length_stdio(
                Arc::new(Echo),
                server_reader,
                server_writer,
            ));
            client.write_all(frame).await.expect("write invalid frame");
            client.shutdown().await.expect("close request side");
            let error = task
                .await
                .expect("stdio task join")
                .expect_err("invalid frame must fail");
            assert!(matches!(
                error,
                HttpRpcTransportError::InvalidFrame(message) if message == expected
            ));
        }
    }

    #[tokio::test]
    async fn ndjson_stdio_round_trips_and_bounds_lines() {
        let (mut client, server) = duplex(4096);
        let (server_reader, server_writer) = tokio::io::split(server);
        let task = tokio::spawn(run_ndjson_stdio(
            Arc::new(Echo),
            server_reader,
            server_writer,
        ));
        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"x\",\"params\":[]}\n")
            .await
            .expect("write NDJSON frame");
        client.shutdown().await.expect("close request side");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read NDJSON response");
        task.await.expect("NDJSON task join").expect("NDJSON task");
        let value: Value = serde_json::from_slice(response.trim_ascii()).expect("response JSON");
        assert_eq!(value["result"]["method"], "x");
    }

    async fn raw_http(address: SocketAddr, request: &[u8]) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(5), async move {
            let mut stream = TcpStream::connect(address).await.expect("connect");
            stream.write_all(request).await.expect("request");
            let mut response = Vec::new();
            let header_end = loop {
                if let Some(offset) = response.windows(4).position(|window| window == b"\r\n\r\n") {
                    break offset + 4;
                }
                assert!(response.len() <= MAX_HTTP_RPC_HEADER_BYTES);
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.expect("response header");
                assert_ne!(read, 0, "truncated response header");
                response.extend_from_slice(&chunk[..read]);
            };
            let header =
                std::str::from_utf8(&response[..header_end]).expect("response header UTF-8");
            let body_length = header
                .split("\r\n")
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("response body length"))
                })
                .expect("response Content-Length");
            let response_length = header_end
                .checked_add(body_length)
                .expect("bounded response length");
            assert!(response_length <= MAX_HTTP_RPC_RESPONSE_BYTES + MAX_HTTP_RPC_HEADER_BYTES);
            while response.len() < response_length {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.expect("response body");
                assert_ne!(read, 0, "truncated response body");
                response.extend_from_slice(&chunk[..read]);
            }
            response.truncate(response_length);
            response
        })
        .await
        .expect("bounded HTTP response timeout")
    }

    #[tokio::test]
    async fn loopback_http_serves_only_json_rpc_path_and_drains_on_shutdown() {
        let backend = Arc::new(Echo);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server_backend = Arc::clone(&backend);
        let task = tokio::spawn(serve_loopback_http_listener_until(
            listener,
            server_backend,
            async move {
                shutdown_receiver.await.map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "shutdown sender dropped")
                })
            },
        ));
        let response = raw_http(
            address,
            b"POST /jsonrpc HTTP/1.1\r\nHost: localhost\r\nContent-Length: 49\r\nConnection: close\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"x\",\"params\":[]}",
        )
        .await;
        assert!(
            response.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "unexpected response: {}",
            String::from_utf8_lossy(&response)
        );
        assert!(
            response
                .windows(b"\r\n\r\n".len())
                .any(|window| window == b"\r\n\r\n")
        );
        assert!(
            response
                .windows(b"\"method\":\"x\"".len())
                .any(|window| window == b"\"method\":\"x\"")
        );

        let wrong_path = raw_http(
            address,
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        )
        .await;
        assert!(wrong_path.starts_with(b"HTTP/1.1 404 Not Found\r\n"));
        let wrong_method = raw_http(
            address,
            b"GET /jsonrpc HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(wrong_method.starts_with(b"HTTP/1.1 404 Not Found\r\n"));

        shutdown_sender.send(()).expect("request shutdown");
        task.await
            .expect("server task join")
            .expect("orderly server shutdown");
        assert_eq!(Arc::strong_count(&backend), 1);
    }

    #[tokio::test]
    async fn loopback_http_rejects_non_loopback_bind() {
        let non_loopback = "0.0.0.0:0".parse().expect("address");
        assert!(matches!(
            serve_loopback_http(non_loopback, Arc::new(Echo)).await,
            Err(HttpRpcTransportError::InvalidBind(_))
        ));
    }

    #[tokio::test]
    async fn loopback_websocket_shares_dispatch_and_pushes_bounded_events() {
        let events = crate::RpcEventBroker::new();
        let backend = Arc::new(EventEcho {
            events: events.clone(),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let task = tokio::spawn(serve_loopback_websocket_listener_until(
            listener,
            backend,
            async move {
                shutdown_receiver.await.map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "shutdown sender dropped")
                })
            },
        ));
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/jsonrpc"))
            .await
            .expect("connect WebSocket");
        socket
            .send(Message::Text(
                r#"{"jsonrpc":"2.0","id":1,"method":"x","params":[]}"#
                    .to_owned()
                    .into(),
            ))
            .await
            .expect("send request");
        let response = socket.next().await.expect("response").expect("message");
        let response: Value = serde_json::from_str(response.to_text().expect("text response"))
            .expect("response JSON");
        assert_eq!(response["result"]["method"], "x");

        events.publish(
            crate::RpcEvent::notification(
                "ariax.onTest",
                json!({"sequence": 1}),
                crate::RpcEventClass::Reliable,
                None,
            )
            .expect("event"),
        );
        let event = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("event timeout")
            .expect("event message")
            .expect("event frame");
        let event: Value =
            serde_json::from_str(event.to_text().expect("event text")).expect("event JSON");
        assert_eq!(event["method"], "ariax.onTest");

        shutdown_sender.send(()).expect("request shutdown");
        let notice = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("shutdown notice timeout")
            .expect("shutdown notice")
            .expect("shutdown frame");
        let notice: Value =
            serde_json::from_str(notice.to_text().expect("notice text")).expect("notice JSON");
        assert_eq!(notice["method"], "ariax.onShutdown");
        task.await
            .expect("WebSocket server join")
            .expect("WebSocket server shutdown");
    }
}
