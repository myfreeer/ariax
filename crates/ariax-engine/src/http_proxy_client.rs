//! One-request bounded HTTP/1.1 execution through admitted proxy routes.

use crate::http_transport::build_tls_config;
use crate::{
    HttpPolicyRequest, HttpProxyConnectConfig, HttpProxyConnectError, HttpProxyRoute,
    HttpTlsPolicy, HttpTransportError, connect_http_proxy_route,
};
use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt as _;
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::{HeaderMap, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

pub const DEFAULT_HTTP_PROXY_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct HttpProxyRequestConfig {
    pub connect: HttpProxyConnectConfig,
    pub tls: HttpTlsPolicy,
    pub tls_handshake_timeout: Duration,
    pub request_timeout: Duration,
    pub body_frame_timeout: Duration,
    pub max_body_bytes: usize,
}

impl Default for HttpProxyRequestConfig {
    fn default() -> Self {
        Self {
            connect: HttpProxyConnectConfig::default(),
            tls: HttpTlsPolicy::default(),
            tls_handshake_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            body_frame_timeout: Duration::from_secs(60),
            max_body_bytes: DEFAULT_HTTP_PROXY_MAX_BODY_BYTES,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpBufferedResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub(crate) struct HttpProxyStreamingResponse {
    response: Option<Response<Incoming>>,
    driver: Option<tokio::task::JoinHandle<Result<(), hyper::Error>>>,
}

impl HttpProxyStreamingResponse {
    pub fn take_response(&mut self) -> Option<Response<Incoming>> {
        self.response.take()
    }

    pub fn abort(mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
    }

    pub async fn abort_and_wait(mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abort();
            let _joined = driver.await;
        }
    }
}

impl Drop for HttpProxyStreamingResponse {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
    }
}

#[derive(Debug)]
pub enum HttpProxyRequestError {
    InvalidConfig,
    InvalidRoute,
    Proxy(HttpProxyConnectError),
    Tls(HttpTransportError),
    TlsHandshakeTimeout,
    TlsHandshake(String),
    HttpHandshakeTimeout,
    Http(hyper::Error),
    RequestTimeout,
    BodyTimeout,
    BodyTooLarge,
}

impl HttpProxyRequestError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_proxy_request_config",
            Self::InvalidRoute => "invalid_proxy_request_route",
            Self::Proxy(error) => error.code(),
            Self::Tls(error) => error.code(),
            Self::TlsHandshakeTimeout => "tls_handshake_timeout",
            Self::TlsHandshake(_) => "tls_handshake",
            Self::HttpHandshakeTimeout => "http_handshake_timeout",
            Self::Http(_) => "http_protocol",
            Self::RequestTimeout => "http_response_head_timeout",
            Self::BodyTimeout => "http_response_body_timeout",
            Self::BodyTooLarge => "http_response_body_too_large",
        }
    }

    #[must_use]
    pub const fn retriable(&self) -> bool {
        match self {
            Self::Proxy(error) => error.retriable(),
            Self::TlsHandshakeTimeout
            | Self::HttpHandshakeTimeout
            | Self::Http(_)
            | Self::RequestTimeout
            | Self::BodyTimeout => true,
            Self::Tls(error) => error.retriable(),
            _ => false,
        }
    }
}

impl fmt::Display for HttpProxyRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proxy(error) => error.fmt(formatter),
            Self::Tls(error) => error.fmt(formatter),
            Self::TlsHandshake(error) => write!(formatter, "TLS handshake failed: {error}"),
            Self::Http(error) => write!(formatter, "HTTP protocol failed: {error}"),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for HttpProxyRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Proxy(error) => Some(error),
            Self::Tls(error) => Some(error),
            Self::Http(error) => Some(error),
            _ => None,
        }
    }
}

pub async fn execute_http_proxy_request(
    route: &HttpProxyRoute,
    proxy_addresses: &[SocketAddr],
    request: HttpPolicyRequest,
    target_is_https: bool,
    config: HttpProxyRequestConfig,
) -> Result<HttpBufferedResponse, HttpProxyRequestError> {
    let mut streaming = open_http_proxy_request(
        route,
        proxy_addresses,
        request,
        target_is_https,
        config.clone(),
    )
    .await?;
    let response = streaming
        .take_response()
        .ok_or(HttpProxyRequestError::InvalidRoute)?;
    let result = collect_response(response, &config).await;
    streaming.abort();
    result
}

pub(crate) async fn open_http_proxy_request(
    route: &HttpProxyRoute,
    proxy_addresses: &[SocketAddr],
    request: HttpPolicyRequest,
    target_is_https: bool,
    config: HttpProxyRequestConfig,
) -> Result<HttpProxyStreamingResponse, HttpProxyRequestError> {
    if config.tls_handshake_timeout.is_zero()
        || config.request_timeout.is_zero()
        || config.body_frame_timeout.is_zero()
        || config.max_body_bytes == 0
        || config.max_body_bytes > DEFAULT_HTTP_PROXY_MAX_BODY_BYTES
    {
        return Err(HttpProxyRequestError::InvalidConfig);
    }
    if matches!(route, HttpProxyRoute::Direct { .. }) {
        return Err(HttpProxyRequestError::InvalidRoute);
    }
    if matches!(route, HttpProxyRoute::HttpForward { .. }) && target_is_https {
        return Err(HttpProxyRequestError::InvalidRoute);
    }
    let connection = connect_http_proxy_route(route, proxy_addresses, config.connect.clone())
        .await
        .map_err(HttpProxyRequestError::Proxy)?;
    if target_is_https {
        let server_name = route_server_name(route)?;
        let server_name = ServerName::try_from(server_name.to_owned())
            .map_err(|_| HttpProxyRequestError::InvalidRoute)?;
        let tls = build_tls_config(&config.tls).map_err(HttpProxyRequestError::Tls)?;
        let stream = timeout(
            config.tls_handshake_timeout,
            TlsConnector::from(Arc::new(tls)).connect(server_name, connection.stream),
        )
        .await
        .map_err(|_| HttpProxyRequestError::TlsHandshakeTimeout)?
        .map_err(|error| HttpProxyRequestError::TlsHandshake(error.to_string()))?;
        open_over_stream(stream, request, &config).await
    } else {
        open_over_stream(connection.stream, request, &config).await
    }
}

async fn open_over_stream<S>(
    stream: S,
    request: HttpPolicyRequest,
    config: &HttpProxyRequestConfig,
) -> Result<HttpProxyStreamingResponse, HttpProxyRequestError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = http1::Builder::new();
    builder
        .max_headers(crate::http_first_slice::MAX_RESPONSE_HEADERS)
        .max_buf_size(crate::http_first_slice::MAX_RESPONSE_HEAD_BYTES);
    let (mut sender, connection) = timeout(
        config.request_timeout,
        builder.handshake(TokioIo::new(stream)),
    )
    .await
    .map_err(|_| HttpProxyRequestError::HttpHandshakeTimeout)?
    .map_err(HttpProxyRequestError::Http)?;
    let driver = tokio::spawn(connection);
    let response = timeout(config.request_timeout, sender.send_request(request))
        .await
        .map_err(|_| HttpProxyRequestError::RequestTimeout)?
        .map_err(HttpProxyRequestError::Http)?;
    Ok(HttpProxyStreamingResponse {
        response: Some(response),
        driver: Some(driver),
    })
}

async fn collect_response(
    response: Response<Incoming>,
    config: &HttpProxyRequestConfig,
) -> Result<HttpBufferedResponse, HttpProxyRequestError> {
    let (parts, mut body) = response.into_parts();
    let mut bytes = BytesMut::new();
    while let Some(frame) = timeout(config.body_frame_timeout, body.frame())
        .await
        .map_err(|_| HttpProxyRequestError::BodyTimeout)?
    {
        let frame = frame.map_err(HttpProxyRequestError::Http)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let next = bytes
            .len()
            .checked_add(data.len())
            .ok_or(HttpProxyRequestError::BodyTooLarge)?;
        if next > config.max_body_bytes {
            return Err(HttpProxyRequestError::BodyTooLarge);
        }
        bytes.extend_from_slice(&data);
    }
    Ok(HttpBufferedResponse {
        status: parts.status,
        headers: parts.headers,
        body: bytes.freeze(),
    })
}

fn route_server_name(route: &HttpProxyRoute) -> Result<&str, HttpProxyRequestError> {
    match route {
        HttpProxyRoute::HttpConnect { server_name, .. }
        | HttpProxyRoute::Socks5 { server_name, .. } => Ok(server_name),
        HttpProxyRoute::Direct { .. } | HttpProxyRoute::HttpForward { .. } => {
            Err(HttpProxyRequestError::InvalidRoute)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HttpCustomHeaders, HttpProxyEndpoint, HttpProxyKind, HttpProxyNameResolution,
        HttpProxyPolicy, HttpRequestPolicy, build_http_request,
    };
    use hyper::Method;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    fn forward_route(address: std::net::IpAddr) -> HttpProxyRoute {
        let endpoint = HttpProxyEndpoint::new(
            "http://proxy.example:8080",
            HttpProxyKind::Http,
            HttpProxyNameResolution::LocalPinned,
            None,
        )
        .expect("endpoint");
        HttpProxyPolicy::new(Some(endpoint), None, None, [])
            .expect("policy")
            .route("http://origin.example/file", address)
            .expect("route")
    }

    #[tokio::test]
    async fn executes_absolute_form_forward_proxy_request_and_collects_bounded_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let route = forward_route("203.0.113.8".parse().expect("IP"));
        let request = build_http_request(HttpRequestPolicy {
            method: Method::GET,
            uri: "http://origin.example/file",
            route: Some(&route),
            range: None,
            if_range: None,
            authorization: None,
            proxy_authorization: None,
            cookie: None,
            custom_headers: &HttpCustomHeaders::default(),
        })
        .expect("request");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            let mut byte = [0_u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.expect("request");
                request.push(byte[0]);
            }
            let text = String::from_utf8(request).expect("ASCII");
            assert!(text.starts_with("GET http://203.0.113.8:80/file HTTP/1.1\r\n"));
            assert!(text.contains("host: origin.example\r\n"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .expect("response");
        });
        let response = execute_http_proxy_request(
            &route,
            &[address],
            request,
            false,
            HttpProxyRequestConfig::default(),
        )
        .await
        .expect("response");
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body, Bytes::from_static(b"ok"));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn rejects_https_over_forward_route_and_oversized_body() {
        let route = forward_route("203.0.113.8".parse().expect("IP"));
        let request = build_http_request(HttpRequestPolicy {
            method: Method::GET,
            uri: "http://origin.example/file",
            route: Some(&route),
            range: None,
            if_range: None,
            authorization: None,
            proxy_authorization: None,
            cookie: None,
            custom_headers: &HttpCustomHeaders::default(),
        })
        .expect("request");
        assert!(matches!(
            execute_http_proxy_request(
                &route,
                &["127.0.0.1:1".parse().expect("address")],
                request,
                true,
                HttpProxyRequestConfig::default(),
            )
            .await,
            Err(HttpProxyRequestError::InvalidRoute)
        ));
    }
}
