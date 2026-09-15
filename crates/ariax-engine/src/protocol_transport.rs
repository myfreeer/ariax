//! Owned FTP/SSH sockets use the same destination, proxy and resource policy.
#[cfg(any(feature = "ftp", feature = "sftp"))]
use crate::{HttpPolicyClient, HttpProxyRoute, HttpTransportCapacityPermit, TransferProtocol};
use ariax_core::{ErrorKind, PublicError, RetryClass};
use std::fmt;
#[cfg(any(feature = "ftp", feature = "sftp"))]
use std::{net::SocketAddr, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolFailure {
    UnsafeDestination,
    Connect,
    Control,
    Data,
    SizeUnsupported,
    ResumeUnsupported,
    AuthFailure,
    Malformed,
    Tls,
    HostKeyMismatch,
    HostKeyApprovalRequired,
    ResourceLimit,
    Cancelled,
    Timeout,
    StaleValidator,
}
impl ProtocolFailure {
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnsafeDestination => "UnsafeDestination",
            Self::Connect => "ProtocolConnect",
            Self::Control => "ControlConnection",
            Self::Data => "DataConnection",
            Self::SizeUnsupported => "SizeUnsupported",
            Self::ResumeUnsupported => "ResumeUnsupported",
            Self::AuthFailure => "AuthFailure",
            Self::Malformed => "MalformedProtocol",
            Self::Tls => "ProtocolTls",
            Self::HostKeyMismatch => "HostKeyMismatch",
            Self::HostKeyApprovalRequired => "HostKeyApprovalRequired",
            Self::ResourceLimit => "ResourceLimit",
            Self::Cancelled => "Cancelled",
            Self::Timeout => "ProtocolTimeout",
            Self::StaleValidator => "StaleValidator",
        }
    }
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::Connect | Self::Control | Self::Data | Self::Timeout
        )
    }
    pub fn into_public(self) -> PublicError {
        let kind = match self {
            Self::UnsafeDestination | Self::Tls | Self::HostKeyMismatch => ErrorKind::Permission,
            Self::AuthFailure => ErrorKind::NeedsCredentials,
            Self::HostKeyApprovalRequired => ErrorKind::HostKeyApprovalRequired,
            Self::ResourceLimit => ErrorKind::ResourceLimit,
            Self::Cancelled => ErrorKind::Cancelled,
            Self::Timeout => ErrorKind::Timeout,
            Self::StaleValidator => ErrorKind::StaleValidator,
            _ => ErrorKind::Network,
        };
        PublicError::new(
            kind,
            self.code(),
            if self.retryable() {
                RetryClass::AnotherSource
            } else {
                RetryClass::Never
            },
        )
    }
}
impl fmt::Display for ProtocolFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for ProtocolFailure {}

#[cfg(any(feature = "ftp", feature = "sftp"))]
pub(crate) struct OwnedProtocolConnection {
    pub stream: tokio::net::TcpStream,
    pub peer: SocketAddr,
    pub proxied: bool,
    pub capacity: HttpTransportCapacityPermit,
}
#[cfg(any(feature = "ftp", feature = "sftp"))]
impl HttpPolicyClient {
    pub(crate) async fn connect_protocol(
        &self,
        uri: &str,
        pinned: Option<SocketAddr>,
        timeout: Duration,
    ) -> Result<OwnedProtocolConnection, ProtocolFailure> {
        let target = url::Url::parse(uri).map_err(|_| ProtocolFailure::UnsafeDestination)?;
        let protocol = TransferProtocol::parse(target.scheme())
            .map_err(|_| ProtocolFailure::UnsafeDestination)?;
        let host = target
            .host_str()
            .ok_or(ProtocolFailure::UnsafeDestination)?;
        let port = target.port().unwrap_or(protocol.default_port());
        if port == 0 {
            return Err(ProtocolFailure::UnsafeDestination);
        }
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let target_uri = authority_uri(host, port);
        let (resolver, config) = self.protocol_policy();
        let resolution_uri = pinned.map_or_else(
            || target_uri.clone(),
            |peer| authority_uri(&peer.ip().to_string(), peer.port()),
        );
        let destination = crate::resolve_http_destination_with_resolver(
            &resolution_uri,
            config.destination,
            resolver,
        )
        .await
        .map_err(|_| ProtocolFailure::UnsafeDestination)?;
        let route = config
            .proxy
            .route_protocol(&target_uri, destination.peer().ip())
            .map_err(|_| ProtocolFailure::UnsafeDestination)?;
        match route {
            HttpProxyRoute::Direct { .. } => {
                let capacity = config
                    .direct
                    .budgets
                    .try_acquire_connection()
                    .map_err(|_| ProtocolFailure::ResourceLimit)?;
                let second = (destination.addresses().len() > 1)
                    .then(|| config.direct.budgets.try_acquire_connection())
                    .transpose()
                    .map_err(|_| ProtocolFailure::ResourceLimit)?;
                let connected = crate::connect_http_happy_eyeballs(
                    destination.addresses(),
                    crate::HttpHappyEyeballsConfig {
                        connect_timeout: timeout,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| ProtocolFailure::Connect)?;
                drop(second);
                Ok(OwnedProtocolConnection {
                    stream: connected.stream,
                    peer: connected.peer,
                    proxied: false,
                    capacity,
                })
            }
            HttpProxyRoute::HttpConnect { ref proxy, .. }
            | HttpProxyRoute::Socks5 { ref proxy, .. } => {
                let proxy_uri = authority_uri(proxy.host(), proxy.port());
                let proxy_destination = crate::resolve_http_destination_with_resolver(
                    &proxy_uri,
                    config.proxy_destination,
                    resolver,
                )
                .await
                .map_err(|_| ProtocolFailure::UnsafeDestination)?;
                let mut connect = config.proxy_request.connect.clone();
                connect.connect_timeout = timeout;
                connect.handshake_timeout = timeout;
                let connection =
                    crate::connect_http_proxy_route(&route, proxy_destination.addresses(), connect)
                        .await
                        .map_err(|_| ProtocolFailure::Connect)?;
                Ok(OwnedProtocolConnection {
                    stream: connection.stream,
                    peer: destination.peer(),
                    proxied: true,
                    capacity: connection.capacity,
                })
            }
            HttpProxyRoute::HttpForward { .. } => Err(ProtocolFailure::UnsafeDestination),
        }
    }
}
#[cfg(any(feature = "ftp", feature = "sftp"))]
fn authority_uri(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("https://[{host}]:{port}/")
    } else {
        format!("https://{host}:{port}/")
    }
}

#[cfg(feature = "sftp")]
mod owned_stream {
    use super::*;
    use crate::{HttpCancellation, HttpIngressPermit};
    use std::{
        future::Future,
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    pub(crate) struct ProtocolStream {
        stream: tokio::net::TcpStream,
        cancelled: Pin<Box<dyn Future<Output = ()> + Send + Sync>>,
        _capacity: HttpTransportCapacityPermit,
        _metadata: HttpIngressPermit,
        // Rust drops fields in declaration order. Acknowledgement must follow
        // both the actual socket close and the release of its reservations.
        _closed: Closed,
    }
    pub(crate) struct ProtocolStreamGuard {
        cancel: HttpCancellation,
        closed: tokio::sync::watch::Receiver<bool>,
    }
    impl ProtocolStreamGuard {
        pub(crate) async fn drain(&mut self) {
            self.cancel.cancel();
            while !*self.closed.borrow_and_update() {
                if self.closed.changed().await.is_err() {
                    break;
                }
            }
        }
    }
    impl Drop for ProtocolStreamGuard {
        fn drop(&mut self) {
            self.cancel.cancel();
        }
    }
    struct Closed(tokio::sync::watch::Sender<bool>);
    impl Drop for Closed {
        fn drop(&mut self) {
            self.0.send_replace(true);
        }
    }
    impl OwnedProtocolConnection {
        pub(crate) fn into_guarded(
            self,
            metadata: HttpIngressPermit,
        ) -> (ProtocolStream, ProtocolStreamGuard) {
            let cancel = HttpCancellation::new();
            let signal = cancel.clone();
            let (closed, receiver) = tokio::sync::watch::channel(false);
            (
                ProtocolStream {
                    stream: self.stream,
                    cancelled: Box::pin(async move { signal.cancelled().await }),
                    _capacity: self.capacity,
                    _metadata: metadata,
                    _closed: Closed(closed),
                },
                ProtocolStreamGuard {
                    cancel,
                    closed: receiver,
                },
            )
        }
    }
    impl ProtocolStream {
        fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
            if self.cancelled.as_mut().poll(cx).is_ready() {
                Err(io::Error::from(io::ErrorKind::ConnectionAborted))
            } else {
                Ok(())
            }
        }
    }
    impl AsyncRead for ProtocolStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.check(cx)?;
            Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for ProtocolStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.check(cx)?;
            Pin::new(&mut self.stream).poll_write(cx, buf)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.check(cx)?;
            Pin::new(&mut self.stream).poll_flush(cx)
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }
}
#[cfg(feature = "sftp")]
pub(crate) use owned_stream::ProtocolStreamGuard;
