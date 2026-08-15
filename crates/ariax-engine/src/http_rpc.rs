//! Bounded JSON-RPC 2.0 framing shared by loopback HTTP and stdio.

use bytes::Bytes;
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
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

pub const MAX_HTTP_RPC_REQUEST_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_HTTP_RPC_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_HTTP_RPC_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_HTTP_RPC_CONNECTIONS: usize = 64;
pub const DEFAULT_HTTP_RPC_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub type RpcFuture = Pin<Box<dyn Future<Output = Result<Value, HttpRpcBackendError>> + Send>>;

/// Backend implemented by the real scheduler control plane.
pub trait HttpRpcBackend: Send + Sync + 'static {
    fn call(&self, method: &str, params: Value) -> RpcFuture;
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

/// Dispatches one JSON-RPC request. Parsing and response serialization are
/// bounded before the backend is called.
pub async fn dispatch_json<B: HttpRpcBackend>(backend: &B, bytes: &[u8]) -> Vec<u8> {
    let parsed = serde_json::from_slice::<Value>(bytes);
    let response = match parsed {
        Ok(request) => dispatch_value(backend, request).await,
        Err(_) => error_response(Value::Null, -32700, "Parse error", None),
    };
    serialize_response(response).unwrap_or_else(|_| {
        serialize_response(error_response(Value::Null, -32603, "Internal error", None))
            .expect("bounded JSON-RPC error fits the response cap")
    })
}

async fn dispatch_value<B: HttpRpcBackend>(backend: &B, request: Value) -> Value {
    let object = match request.as_object() {
        Some(object) => object,
        None => return error_response(Value::Null, -32600, "Invalid Request", None),
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return error_response(
            object.get("id").cloned().unwrap_or(Value::Null),
            -32600,
            "Invalid Request",
            None,
        );
    }
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if id.is_null() || !(id.is_string() || id.is_number()) {
        return error_response(Value::Null, -32600, "Invalid Request", None);
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return error_response(id, -32600, "Invalid Request", None);
    };
    let params = object
        .get("params")
        .cloned()
        .unwrap_or(Value::Array(Vec::new()));
    if !(params.is_array() || params.is_object()) {
        return error_response(id, -32602, "Invalid params", None);
    }
    match backend.call(method, params).await {
        Ok(result) => json!({"jsonrpc":"2.0", "id":id, "result":result}),
        Err(error) => error_response(id, error.code, &error.message, error.data),
    }
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
    let bytes =
        serde_json::to_vec(&response).map_err(|_| HttpRpcTransportError::ResponseTooLarge)?;
    if bytes.len() > MAX_HTTP_RPC_RESPONSE_BYTES {
        return Err(HttpRpcTransportError::ResponseTooLarge);
    }
    Ok(bytes)
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
        if response.len() > MAX_HTTP_RPC_RESPONSE_BYTES {
            return Err(HttpRpcTransportError::ResponseTooLarge);
        }
        writer
            .write_all(format!("Content-Length: {}\r\n\r\n", response.len()).as_bytes())
            .await?;
        writer.write_all(&response).await?;
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
}
