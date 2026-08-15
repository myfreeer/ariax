//! Bounded HTTP CONNECT and SOCKS5 wire handshakes for admitted proxy routes.

use crate::{
    HttpHappyEyeballsConfig, HttpProxyRoute, HttpSocksTarget, connect_http_happy_eyeballs,
};
use std::error::Error;
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

pub const MAX_HTTP_PROXY_RESPONSE_HEAD_BYTES: usize = 16 * 1024;
pub const MAX_SOCKS5_DOMAIN_BYTES: usize = 253;

#[derive(Clone, Eq, PartialEq)]
pub struct HttpProxyAuthorization(String);

impl HttpProxyAuthorization {
    pub fn basic(username: &str, password: &str) -> Result<Self, HttpProxyConnectError> {
        if username.is_empty()
            || username.len() > 1024
            || password.len() > 4096
            || username.bytes().any(forbidden_secret_byte)
            || password.bytes().any(forbidden_secret_byte)
        {
            return Err(HttpProxyConnectError::InvalidCredentials);
        }
        let mut plaintext = Vec::with_capacity(username.len() + 1 + password.len());
        plaintext.extend_from_slice(username.as_bytes());
        plaintext.push(b':');
        plaintext.extend_from_slice(password.as_bytes());
        let value = format!("Basic {}", encode_base64(&plaintext));
        plaintext.fill(0);
        Ok(Self(value))
    }

    pub(crate) fn value(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for HttpProxyAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HttpProxyAuthorization(<redacted>)")
    }
}

#[derive(Clone, Debug)]
pub struct HttpProxyConnectConfig {
    pub connect_timeout: Duration,
    pub happy_eyeballs_delay: Duration,
    pub handshake_timeout: Duration,
    pub authorization: Option<HttpProxyAuthorization>,
}

impl Default for HttpProxyConnectConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            happy_eyeballs_delay: crate::DEFAULT_HTTP_HAPPY_EYEBALLS_DELAY,
            handshake_timeout: Duration::from_secs(30),
            authorization: None,
        }
    }
}

#[derive(Debug)]
pub struct HttpProxyConnection {
    pub stream: TcpStream,
    pub proxy_peer: SocketAddr,
    pub forwarded_http: bool,
}

#[derive(Debug)]
pub enum HttpProxyConnectError {
    InvalidConfig,
    InvalidRoute,
    InvalidCredentials,
    Connect(crate::HttpHappyEyeballsError),
    Io(io::Error),
    HandshakeTimeout,
    ResponseTooLarge,
    InvalidResponse,
    ConnectRejected(u16),
    SocksMethodRejected,
    SocksReply(u8),
    SocksTarget,
}

impl HttpProxyConnectError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_proxy_connect_config",
            Self::InvalidRoute => "invalid_proxy_route",
            Self::InvalidCredentials => "invalid_proxy_credentials",
            Self::Connect(error) => error.code(),
            Self::Io(_) => "proxy_connect_io",
            Self::HandshakeTimeout => "proxy_handshake_timeout",
            Self::ResponseTooLarge => "proxy_response_too_large",
            Self::InvalidResponse => "invalid_proxy_response",
            Self::ConnectRejected(_) => "proxy_connect_rejected",
            Self::SocksMethodRejected => "socks5_method_rejected",
            Self::SocksReply(_) => "socks5_connect_rejected",
            Self::SocksTarget => "invalid_socks5_target",
        }
    }

    #[must_use]
    pub const fn retriable(&self) -> bool {
        matches!(
            self,
            Self::Connect(_)
                | Self::Io(_)
                | Self::HandshakeTimeout
                | Self::ConnectRejected(408 | 425 | 429 | 500 | 502 | 503 | 504)
                | Self::SocksReply(1 | 3 | 4 | 5 | 6)
        )
    }
}

impl fmt::Display for HttpProxyConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => error.fmt(formatter),
            Self::Io(error) => write!(formatter, "proxy I/O failed: {error}"),
            Self::ConnectRejected(status) => write!(formatter, "proxy CONNECT rejected: {status}"),
            Self::SocksReply(reply) => write!(formatter, "SOCKS5 CONNECT rejected: {reply}"),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for HttpProxyConnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

pub async fn connect_http_proxy_route(
    route: &HttpProxyRoute,
    proxy_addresses: &[SocketAddr],
    config: HttpProxyConnectConfig,
) -> Result<HttpProxyConnection, HttpProxyConnectError> {
    if config.connect_timeout.is_zero()
        || config.happy_eyeballs_delay.is_zero()
        || config.handshake_timeout.is_zero()
    {
        return Err(HttpProxyConnectError::InvalidConfig);
    }
    if matches!(route, HttpProxyRoute::Direct { .. }) {
        return Err(HttpProxyConnectError::InvalidRoute);
    }
    let connected = connect_http_happy_eyeballs(
        proxy_addresses,
        HttpHappyEyeballsConfig {
            connect_timeout: config.connect_timeout,
            fallback_delay: config.happy_eyeballs_delay,
        },
    )
    .await
    .map_err(HttpProxyConnectError::Connect)?;
    let mut stream = connected.stream;
    let forwarded_http = match route {
        HttpProxyRoute::HttpForward { .. } => true,
        HttpProxyRoute::HttpConnect {
            connect_authority, ..
        } => {
            timeout(
                config.handshake_timeout,
                http_connect_handshake(
                    &mut stream,
                    connect_authority,
                    config.authorization.as_ref(),
                ),
            )
            .await
            .map_err(|_| HttpProxyConnectError::HandshakeTimeout)??;
            false
        }
        HttpProxyRoute::Socks5 { target, .. } => {
            if config.authorization.is_some() {
                return Err(HttpProxyConnectError::InvalidCredentials);
            }
            timeout(
                config.handshake_timeout,
                socks5_connect_handshake(&mut stream, target),
            )
            .await
            .map_err(|_| HttpProxyConnectError::HandshakeTimeout)??;
            false
        }
        HttpProxyRoute::Direct { .. } => return Err(HttpProxyConnectError::InvalidRoute),
    };
    Ok(HttpProxyConnection {
        stream,
        proxy_peer: connected.peer,
        forwarded_http,
    })
}

async fn http_connect_handshake(
    stream: &mut TcpStream,
    authority: &str,
    authorization: Option<&HttpProxyAuthorization>,
) -> Result<(), HttpProxyConnectError> {
    if authority.is_empty()
        || authority.len() > 1024
        || authority
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(HttpProxyConnectError::InvalidRoute);
    }
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(authorization) = authorization {
        request.push_str("Proxy-Authorization: ");
        request.push_str(authorization.value());
        request.push_str("\r\n");
    }
    request.push_str("Proxy-Connection: keep-alive\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(HttpProxyConnectError::Io)?;
    let head = read_response_head(stream).await?;
    let line_end = head
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or(HttpProxyConnectError::InvalidResponse)?;
    let status_line = std::str::from_utf8(&head[..line_end])
        .map_err(|_| HttpProxyConnectError::InvalidResponse)?;
    let mut parts = status_line.split_ascii_whitespace();
    let version = parts.next().ok_or(HttpProxyConnectError::InvalidResponse)?;
    let status = parts
        .next()
        .ok_or(HttpProxyConnectError::InvalidResponse)?
        .parse::<u16>()
        .map_err(|_| HttpProxyConnectError::InvalidResponse)?;
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") || !(100..=599).contains(&status) {
        return Err(HttpProxyConnectError::InvalidResponse);
    }
    if status != 200 {
        return Err(HttpProxyConnectError::ConnectRejected(status));
    }
    Ok(())
}

async fn read_response_head(stream: &mut TcpStream) -> Result<Vec<u8>, HttpProxyConnectError> {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() == MAX_HTTP_PROXY_RESPONSE_HEAD_BYTES {
            return Err(HttpProxyConnectError::ResponseTooLarge);
        }
        let read = stream
            .read(&mut byte)
            .await
            .map_err(HttpProxyConnectError::Io)?;
        if read == 0 {
            return Err(HttpProxyConnectError::InvalidResponse);
        }
        head.push(byte[0]);
    }
    Ok(head)
}

async fn socks5_connect_handshake(
    stream: &mut TcpStream,
    target: &HttpSocksTarget,
) -> Result<(), HttpProxyConnectError> {
    stream
        .write_all(&[5, 1, 0])
        .await
        .map_err(HttpProxyConnectError::Io)?;
    let mut method = [0_u8; 2];
    stream
        .read_exact(&mut method)
        .await
        .map_err(HttpProxyConnectError::Io)?;
    if method != [5, 0] {
        return Err(HttpProxyConnectError::SocksMethodRejected);
    }
    let request = socks5_request(target)?;
    stream
        .write_all(&request)
        .await
        .map_err(HttpProxyConnectError::Io)?;
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(HttpProxyConnectError::Io)?;
    if prefix[0] != 5 || prefix[2] != 0 {
        return Err(HttpProxyConnectError::InvalidResponse);
    }
    if prefix[1] != 0 {
        return Err(HttpProxyConnectError::SocksReply(prefix[1]));
    }
    let remaining = match prefix[3] {
        1 => 4 + 2,
        4 => 16 + 2,
        3 => {
            let mut length = [0_u8; 1];
            stream
                .read_exact(&mut length)
                .await
                .map_err(HttpProxyConnectError::Io)?;
            usize::from(length[0]) + 2
        }
        _ => return Err(HttpProxyConnectError::InvalidResponse),
    };
    let mut ignored = vec![0_u8; remaining];
    stream
        .read_exact(&mut ignored)
        .await
        .map_err(HttpProxyConnectError::Io)?;
    Ok(())
}

fn socks5_request(target: &HttpSocksTarget) -> Result<Vec<u8>, HttpProxyConnectError> {
    let mut request = vec![5, 1, 0];
    let port = match target {
        HttpSocksTarget::Address(IpAddr::V4(address), port) => {
            request.push(1);
            request.extend_from_slice(&address.octets());
            *port
        }
        HttpSocksTarget::Address(IpAddr::V6(address), port) => {
            request.push(4);
            request.extend_from_slice(&address.octets());
            *port
        }
        HttpSocksTarget::Domain(domain, port) => {
            if domain.is_empty()
                || domain.len() > MAX_SOCKS5_DOMAIN_BYTES
                || !domain.is_ascii()
                || domain.bytes().any(|byte| byte.is_ascii_control())
            {
                return Err(HttpProxyConnectError::SocksTarget);
            }
            request.push(3);
            request
                .push(u8::try_from(domain.len()).map_err(|_| HttpProxyConnectError::SocksTarget)?);
            request.extend_from_slice(domain.as_bytes());
            *port
        }
    };
    if port == 0 {
        return Err(HttpProxyConnectError::SocksTarget);
    }
    request.extend_from_slice(&port.to_be_bytes());
    Ok(request)
}

const fn forbidden_secret_byte(byte: u8) -> bool {
    byte == 0 || byte == b'\r' || byte == b'\n'
}

fn encode_base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(TABLE[(first >> 2) as usize] as char);
        output.push(TABLE[(((first & 3) << 4) | (second >> 4)) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[(((second & 15) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(third & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HttpProxyEndpoint, HttpProxyKind, HttpProxyNameResolution, HttpProxyPolicy,
        TrustedProxyEnforcement,
    };
    use tokio::net::TcpListener;

    async fn proxy_address() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        (listener, address)
    }

    fn http_route() -> HttpProxyRoute {
        let endpoint = HttpProxyEndpoint::new(
            "http://proxy.example:8080",
            HttpProxyKind::Http,
            HttpProxyNameResolution::LocalPinned,
            None,
        )
        .expect("endpoint");
        HttpProxyPolicy::new(None, Some(endpoint), None, [])
            .expect("policy")
            .route(
                "https://origin.example/file",
                "203.0.113.8".parse().expect("IP"),
            )
            .expect("route")
    }

    #[tokio::test]
    async fn performs_bounded_authenticated_http_connect() {
        let (listener, address) = proxy_address().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let request = read_response_head(&mut stream).await.expect("request");
            let text = String::from_utf8(request).expect("ASCII");
            assert!(text.starts_with("CONNECT 203.0.113.8:443 HTTP/1.1\r\n"));
            assert!(text.contains("Proxy-Authorization: Basic dXNlcjpzZWNyZXQ=\r\n"));
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .expect("reply");
        });
        let connection = connect_http_proxy_route(
            &http_route(),
            &[address],
            HttpProxyConnectConfig {
                authorization: Some(HttpProxyAuthorization::basic("user", "secret").expect("auth")),
                ..HttpProxyConnectConfig::default()
            },
        )
        .await
        .expect("connect");
        assert!(!connection.forwarded_http);
        drop(connection.stream);
        server.await.expect("server");
    }

    #[tokio::test]
    async fn performs_socks5_domain_connect_only_for_trusted_route() {
        let endpoint = HttpProxyEndpoint::new(
            "socks5://proxy.example:1080",
            HttpProxyKind::Socks5,
            HttpProxyNameResolution::TrustedProxyEnforced,
            Some(TrustedProxyEnforcement::from_startup_admin_policy()),
        )
        .expect("endpoint");
        let route = HttpProxyPolicy::new(None, None, Some(endpoint), [])
            .expect("policy")
            .route(
                "https://origin.example/file",
                "203.0.113.8".parse().expect("IP"),
            )
            .expect("route");
        let (listener, address) = proxy_address().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut greeting = [0_u8; 3];
            stream.read_exact(&mut greeting).await.expect("greeting");
            assert_eq!(greeting, [5, 1, 0]);
            stream.write_all(&[5, 0]).await.expect("method");
            let mut prefix = [0_u8; 5];
            stream
                .read_exact(&mut prefix)
                .await
                .expect("request prefix");
            assert_eq!(&prefix[..4], &[5, 1, 0, 3]);
            let length = usize::from(prefix[4]);
            let mut domain_and_port = vec![0_u8; length + 2];
            stream
                .read_exact(&mut domain_and_port)
                .await
                .expect("target");
            assert_eq!(&domain_and_port[..length], b"origin.example");
            assert_eq!(&domain_and_port[length..], &443_u16.to_be_bytes());
            stream
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 1])
                .await
                .expect("reply");
        });
        let connection =
            connect_http_proxy_route(&route, &[address], HttpProxyConnectConfig::default())
                .await
                .expect("connect");
        assert!(!connection.forwarded_http);
        drop(connection.stream);
        server.await.expect("server");
    }

    #[tokio::test]
    async fn surfaces_connect_rejection_without_leaking_authorization_debug() {
        let (listener, address) = proxy_address().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let _request = read_response_head(&mut stream).await.expect("request");
            stream
                .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                .await
                .expect("reply");
        });
        let authorization = HttpProxyAuthorization::basic("user", "secret").expect("auth");
        assert!(!format!("{authorization:?}").contains("dXNlcg"));
        let error = connect_http_proxy_route(
            &http_route(),
            &[address],
            HttpProxyConnectConfig {
                authorization: Some(authorization),
                ..HttpProxyConnectConfig::default()
            },
        )
        .await
        .expect_err("407 rejected");
        assert!(matches!(error, HttpProxyConnectError::ConnectRejected(407)));
        assert!(!error.retriable());
        server.await.expect("server");
    }
}
