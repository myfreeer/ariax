//! One policy-owned streaming HTTP request path for direct and proxy routes.

use crate::http_proxy_client::{HttpProxyStreamingResponse, open_http_proxy_request};
use crate::http_transport::{HttpResponseLease, HttpTransportResponse};
use crate::{
    HttpAuthError, HttpAuthPolicy, HttpCookieError, HttpCookieJar, HttpCustomHeaders,
    HttpDestinationError, HttpDestinationPolicy, HttpDirectTransport, HttpDirectTransportConfig,
    HttpMirrorIdentityPolicy, HttpPolicyRequest, HttpProxyAuthorization, HttpProxyPolicy,
    HttpProxyPolicyError, HttpProxyRequestConfig, HttpProxyRequestError, HttpProxyRoute,
    HttpRedirectContext, HttpRedirectError, HttpRedirectPolicy, HttpRedirectState,
    HttpRequestPolicy, HttpRequestPolicyError, HttpResolver, HttpTransportError,
    build_http_request, resolve_http_destination_with_resolver,
};
use ariax_storage::GlobalSpan;
use bytes::Bytes;
use http_body_util::BodyExt as _;
use hyper::body::Incoming;
use hyper::header::{LOCATION, SET_COOKIE};
use hyper::{HeaderMap, Method, Response, StatusCode, Uri};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Default number of admitted direct-origin transports retained by one policy
/// client. With the direct transport's default one idle connection per origin,
/// this is also the default process-local idle connection cap.
pub const DEFAULT_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY: usize = 32;

/// Hard cap on retained direct-origin transports. This is deliberately aligned
/// with the concurrency profile's global HTTP idle-pool ceiling; profile
/// resolution can select a lower value but must never make the cache unbounded.
pub const MAX_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY: usize = 512;

#[derive(Clone)]
pub struct HttpPolicyClientConfig {
    pub destination: HttpDestinationPolicy,
    pub proxy_destination: HttpDestinationPolicy,
    pub proxy: HttpProxyPolicy,
    pub redirects: HttpRedirectPolicy,
    pub direct: HttpDirectTransportConfig,
    pub proxy_request: HttpProxyRequestConfig,
    pub auth: HttpAuthPolicy,
    pub cookies: Option<Arc<Mutex<HttpCookieJar>>>,
    pub custom_headers: HttpCustomHeaders,
    /// Number of direct-origin transport entries retained for keep-alive reuse.
    /// A value of zero disables client-level retention. The effective value is
    /// clamped to the documented hard global cap and, when each origin can hold
    /// more than one idle connection, to a count that cannot exceed that cap.
    pub direct_transport_cache_capacity: usize,
}

impl Default for HttpPolicyClientConfig {
    fn default() -> Self {
        Self {
            destination: HttpDestinationPolicy::default(),
            proxy_destination: HttpDestinationPolicy::default(),
            proxy: HttpProxyPolicy::default(),
            redirects: HttpRedirectPolicy::default(),
            direct: HttpDirectTransportConfig::default(),
            proxy_request: HttpProxyRequestConfig::default(),
            auth: HttpAuthPolicy::default(),
            cookies: None,
            custom_headers: HttpCustomHeaders::default(),
            direct_transport_cache_capacity: DEFAULT_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY,
        }
    }
}

impl fmt::Debug for HttpPolicyClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpPolicyClientConfig")
            .field("destination", &self.destination)
            .field("proxy_destination", &self.proxy_destination)
            .field("proxy", &self.proxy)
            .field("redirects", &self.redirects)
            .field("direct", &self.direct)
            .field("proxy_request", &self.proxy_request)
            .field("auth", &self.auth)
            .field("cookies", &self.cookies.as_ref().map(|_| "<cookie-jar>"))
            .field("custom_headers", &self.custom_headers)
            .field(
                "direct_transport_cache_capacity",
                &self.direct_transport_cache_capacity,
            )
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct HttpClientRequest {
    pub method: Method,
    pub uri: String,
    pub top_level_uri: String,
    pub range: Option<GlobalSpan>,
    pub if_range: Option<Box<[u8]>>,
    pub mirror_identity: HttpMirrorIdentityPolicy,
    pub shared_whole_entity_digest: bool,
    pub want_repr_digest: bool,
}

impl HttpClientRequest {
    #[must_use]
    pub fn get(uri: String) -> Self {
        Self {
            top_level_uri: uri.clone(),
            method: Method::GET,
            uri,
            range: None,
            if_range: None,
            mirror_identity: HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
            shared_whole_entity_digest: false,
            want_repr_digest: false,
        }
    }
}

#[derive(Clone)]
pub struct HttpPolicyClient {
    resolver: HttpResolver,
    config: Arc<HttpPolicyClientConfig>,
    header_origin: Option<Arc<str>>,
    task_headers: Option<Arc<HttpCustomHeaders>>,
    direct_transports: Arc<Mutex<DirectTransportCache>>,
}

impl fmt::Debug for HttpPolicyClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpPolicyClient")
            .field("resolver", &self.resolver)
            .field("config", &self.config)
            .field("direct_transport_cache", &"<bounded>")
            .finish()
    }
}

impl HttpPolicyClient {
    #[cfg(any(feature = "ftp", feature = "sftp"))]
    pub(crate) fn protocol_policy(&self) -> (&HttpResolver, &HttpPolicyClientConfig) {
        (&self.resolver, &self.config)
    }
    #[must_use]
    pub fn new(resolver: HttpResolver, config: HttpPolicyClientConfig) -> Self {
        let direct_transport_cache_capacity = effective_direct_transport_cache_capacity(&config);
        Self {
            resolver,
            config: Arc::new(config),
            header_origin: None,
            task_headers: None,
            direct_transports: Arc::new(Mutex::new(DirectTransportCache::new(
                direct_transport_cache_capacity,
            ))),
        }
    }

    pub(crate) fn with_task_headers(
        &self,
        task: &crate::TransferTaskSpec,
    ) -> Result<Self, HttpRequestPolicyError> {
        let mut client = self.clone();
        let task_headers = task
            .options()
            .transfer
            .request_headers()
            .map_err(|_| HttpRequestPolicyError::InvalidHeader)?;
        client.task_headers = if task_headers.values().is_empty() {
            None
        } else {
            let mut headers = std::collections::BTreeMap::new();
            for header in self
                .config
                .custom_headers
                .values()
                .iter()
                .chain(task_headers.values())
            {
                headers.insert(
                    header.name().to_string(),
                    header.value().to_str().expect("validated text").to_owned(),
                );
            }
            Some(Arc::new(HttpCustomHeaders::new(headers)?))
        };
        client.header_origin = task
            .options()
            .transfer
            .http_header_origin
            .as_ref()
            .map(|origin| Arc::from(origin.ascii_serialization()))
            .or_else(|| {
                task.sources()
                    .iter()
                    .find_map(|source| source.uri())
                    .map(Arc::from)
            });
        Ok(client)
    }

    pub async fn execute(
        &self,
        request: HttpClientRequest,
    ) -> Result<HttpStreamingResponse, HttpPolicyClientError> {
        if !matches!(request.method, Method::GET | Method::HEAD)
            || request.uri.is_empty()
            || request.top_level_uri.is_empty()
        {
            return Err(HttpPolicyClientError::InvalidRequest);
        }
        let header_uri = self
            .header_origin
            .as_deref()
            .unwrap_or(&request.top_level_uri);
        let header_origin = url::Url::parse(header_uri)
            .map_err(|_| HttpPolicyClientError::InvalidRequest)?
            .origin();
        let mut redirects =
            HttpRedirectState::new(&request.uri, request.method.clone(), self.config.redirects)
                .map_err(HttpPolicyClientError::Redirect)?;
        let mut method = request.method;
        let mut if_range = request.if_range;

        loop {
            let current = redirects.current_uri().to_owned();
            let destination = resolve_http_destination_with_resolver(
                &current,
                self.config.destination,
                &self.resolver,
            )
            .await
            .map_err(HttpPolicyClientError::Destination)?;
            let route = self
                .config
                .proxy
                .route(&current, destination.peer().ip())
                .map_err(HttpPolicyClientError::ProxyPolicy)?;
            let uri: Uri = current
                .parse()
                .map_err(|_| HttpPolicyClientError::InvalidRequest)?;
            let authority = uri
                .authority()
                .ok_or(HttpPolicyClientError::InvalidRequest)?;
            let authorization = self
                .config
                .auth
                .authorization_for(authority.host(), None)
                .map_err(HttpPolicyClientError::Auth)?;
            let cookie = if let Some(jar) = self.config.cookies.as_ref() {
                jar.lock()
                    .await
                    .header_for(&current, &request.top_level_uri, &method)
                    .map_err(HttpPolicyClientError::Cookie)?
            } else {
                None
            };
            let original_route = self
                .config
                .proxy
                .route(header_uri, destination.peer().ip())
                .map_err(HttpPolicyClientError::ProxyPolicy)?;
            let same_proxy = proxy_endpoint(&original_route).is_some()
                && proxy_endpoint(&original_route) == proxy_endpoint(&route);
            let custom_headers = self
                .task_headers
                .as_deref()
                .unwrap_or(&self.config.custom_headers)
                .for_destination(
                    url::Url::parse(&current)
                        .map_err(|_| HttpPolicyClientError::InvalidRequest)?
                        .origin()
                        == header_origin,
                    same_proxy,
                );
            let custom_proxy_authorization = custom_headers.proxy_authorization();
            let proxy_authorization = forward_proxy_authorization(
                &route,
                custom_proxy_authorization.as_ref().or(self
                    .config
                    .proxy_request
                    .connect
                    .authorization
                    .as_ref()),
            );
            let outbound = build_http_request(HttpRequestPolicy {
                method: method.clone(),
                uri: &current,
                route: Some(&route),
                range: request.range,
                if_range: if_range.as_deref(),
                want_repr_digest: request.want_repr_digest,
                authorization: authorization.as_ref(),
                proxy_authorization,
                cookie: cookie.as_ref(),
                custom_headers: &custom_headers,
            })
            .map_err(HttpPolicyClientError::Request)?;
            let (response, lease) = self
                .open_route(
                    &current,
                    &route,
                    destination.addresses(),
                    outbound,
                    custom_proxy_authorization,
                )
                .await?;
            self.store_response_cookies(&current, response.headers())
                .await?;

            if !is_redirect(response.status()) {
                return Ok(HttpStreamingResponse {
                    response,
                    lease: Some(lease),
                    final_uri: current.into(),
                    redirect_count: redirects.hops(),
                });
            }

            let status = response.status();
            let location = single_location(response.headers())?;
            drop(response);
            lease.discard().await;
            let decision = redirects
                .follow(
                    status,
                    location.as_deref(),
                    HttpRedirectContext {
                        open_lease: false,
                        nonzero_durable_prefix: request.range.is_some_and(|span| span.offset != 0),
                        shared_whole_entity_digest: request.shared_whole_entity_digest,
                        mirror_identity: request.mirror_identity,
                    },
                )
                .map_err(HttpPolicyClientError::Redirect)?;
            if decision.restart_from_zero {
                return Err(HttpPolicyClientError::RedirectRequiresRestart);
            }
            method = decision.method;
            if decision.drop_if_range {
                if_range = None;
            }
        }
    }

    async fn open_route(
        &self,
        uri: &str,
        route: &HttpProxyRoute,
        target_addresses: &[SocketAddr],
        request: HttpPolicyRequest,
        custom_proxy_authorization: Option<HttpProxyAuthorization>,
    ) -> Result<(Response<Incoming>, HttpClientLease), HttpPolicyClientError> {
        match route {
            HttpProxyRoute::Direct { .. } => {
                // `execute` re-resolves and applies destination policy before
                // every route admission. The cache key includes that admitted
                // answer set, so a changed answer cannot cause a newly opened
                // connection to reuse an older address decision. A retained
                // connection itself remains bound to the peer that was already
                // admitted when it was opened.
                let transport = self.direct_transport(uri, target_addresses).await?;
                let HttpTransportResponse { response, lease } = transport
                    .send(request)
                    .await
                    .map_err(HttpPolicyClientError::Transport)?;
                Ok((response, HttpClientLease::Direct(lease)))
            }
            HttpProxyRoute::HttpForward { proxy, .. }
            | HttpProxyRoute::HttpConnect { proxy, .. }
            | HttpProxyRoute::Socks5 { proxy, .. } => {
                let proxy_uri = proxy_resolution_uri(proxy.host(), proxy.port());
                let proxy_destination = resolve_http_destination_with_resolver(
                    &proxy_uri,
                    self.config.proxy_destination,
                    &self.resolver,
                )
                .await
                .map_err(HttpPolicyClientError::Destination)?;
                let mut proxy_request = self.config.proxy_request.clone();
                if let Some(authorization) = custom_proxy_authorization {
                    proxy_request.connect.authorization = Some(authorization);
                }
                let mut streaming = open_http_proxy_request(
                    route,
                    proxy_destination.addresses(),
                    request,
                    uri.starts_with("https://"),
                    proxy_request,
                )
                .await
                .map_err(HttpPolicyClientError::ProxyRequest)?;
                let response = streaming
                    .take_response()
                    .ok_or(HttpPolicyClientError::InvalidRequest)?;
                Ok((response, HttpClientLease::Proxy(streaming)))
            }
        }
    }

    async fn direct_transport(
        &self,
        uri: &str,
        target_addresses: &[SocketAddr],
    ) -> Result<HttpDirectTransport, HttpPolicyClientError> {
        let key = DirectTransportCacheKey::new(uri, target_addresses)?;
        let mut cache = self.direct_transports.lock().await;
        if let Some(transport) = cache.take(&key) {
            return Ok(transport);
        }

        let mut config = self.config.direct.clone();
        config.destination = self.config.destination;
        let transport =
            HttpDirectTransport::admitted(uri, Arc::from(target_addresses.to_vec()), config)
                .map_err(HttpPolicyClientError::Transport)?;
        cache.insert(key, transport.clone());
        Ok(transport)
    }

    async fn store_response_cookies(
        &self,
        uri: &str,
        headers: &HeaderMap,
    ) -> Result<(), HttpPolicyClientError> {
        let Some(jar) = self.config.cookies.as_ref() else {
            return Ok(());
        };
        let mut jar = jar.lock().await;
        for value in headers.get_all(SET_COOKIE) {
            let value = value
                .to_str()
                .map_err(|_| HttpPolicyClientError::InvalidHeader)?;
            jar.store_set_cookie(uri, value)
                .map_err(HttpPolicyClientError::Cookie)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectTransportCacheKey {
    origin: Arc<str>,
    addresses: Arc<[SocketAddr]>,
}

impl DirectTransportCacheKey {
    fn new(uri: &str, addresses: &[SocketAddr]) -> Result<Self, HttpPolicyClientError> {
        let uri: Uri = uri
            .parse()
            .map_err(|_| HttpPolicyClientError::InvalidRequest)?;
        let scheme = uri
            .scheme_str()
            .ok_or(HttpPolicyClientError::InvalidRequest)?;
        let authority = uri
            .authority()
            .ok_or(HttpPolicyClientError::InvalidRequest)?;
        Ok(Self {
            origin: format!("{scheme}://{authority}").into(),
            addresses: Arc::from(addresses.to_vec()),
        })
    }
}

#[derive(Debug)]
struct DirectTransportCache {
    capacity: usize,
    entries: VecDeque<(DirectTransportCacheKey, HttpDirectTransport)>,
}

impl DirectTransportCache {
    const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::new(),
        }
    }

    fn take(&mut self, key: &DirectTransportCacheKey) -> Option<HttpDirectTransport> {
        let index = self.entries.iter().position(|(entry, _)| entry == key)?;
        let entry = self.entries.remove(index)?;
        let transport = entry.1.clone();
        self.entries.push_back(entry);
        Some(transport)
    }

    fn insert(&mut self, key: DirectTransportCacheKey, transport: HttpDirectTransport) {
        if self.capacity == 0 {
            return;
        }
        while self.entries.len() >= self.capacity {
            let _evicted = self.entries.pop_front();
        }
        self.entries.push_back((key, transport));
    }
}

fn effective_direct_transport_cache_capacity(config: &HttpPolicyClientConfig) -> usize {
    let per_origin_idle = config.direct.max_idle_connections_per_origin;
    let global_cap = MAX_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY
        .checked_div(per_origin_idle)
        .unwrap_or(MAX_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY);
    config.direct_transport_cache_capacity.min(global_cap)
}

enum HttpClientLease {
    Direct(HttpResponseLease),
    Proxy(HttpProxyStreamingResponse),
}

impl HttpClientLease {
    async fn discard(self) {
        match self {
            Self::Direct(lease) => lease.discard().await,
            Self::Proxy(proxy) => proxy.abort_and_wait().await,
        }
    }
}

pub struct HttpStreamingResponse {
    response: Response<Incoming>,
    lease: Option<HttpClientLease>,
    final_uri: Arc<str>,
    redirect_count: usize,
}

impl fmt::Debug for HttpStreamingResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpStreamingResponse")
            .field("status", &self.response.status())
            .field("headers", self.response.headers())
            .field("final_uri", &self.final_uri)
            .field("redirect_count", &self.redirect_count)
            .finish_non_exhaustive()
    }
}

impl HttpStreamingResponse {
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.response.status()
    }

    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        self.response.headers()
    }

    #[must_use]
    pub fn final_uri(&self) -> &str {
        &self.final_uri
    }

    #[must_use]
    pub const fn redirect_count(&self) -> usize {
        self.redirect_count
    }

    pub async fn next_data(
        &mut self,
        frame_timeout: Duration,
    ) -> Result<Option<Bytes>, HttpPolicyClientError> {
        if frame_timeout.is_zero() {
            return Err(HttpPolicyClientError::InvalidRequest);
        }
        loop {
            let frame = timeout(frame_timeout, self.response.body_mut().frame())
                .await
                .map_err(|_| HttpPolicyClientError::BodyTimeout)?;
            let Some(frame) = frame else {
                return Ok(None);
            };
            let frame = frame.map_err(HttpPolicyClientError::Body)?;
            match frame.into_data() {
                Ok(data) if data.is_empty() => {}
                Ok(data) => return Ok(Some(data)),
                Err(_) => return Err(HttpPolicyClientError::UnexpectedTrailers),
            }
        }
    }

    pub async fn finish(mut self) {
        match self.lease.take() {
            Some(HttpClientLease::Direct(lease)) => lease.recycle().await,
            Some(HttpClientLease::Proxy(proxy)) => proxy.abort(),
            None => {}
        }
    }
}

#[derive(Debug)]
pub enum HttpPolicyClientError {
    InvalidRequest,
    InvalidHeader,
    Destination(HttpDestinationError),
    ProxyPolicy(HttpProxyPolicyError),
    Request(HttpRequestPolicyError),
    Auth(HttpAuthError),
    Cookie(HttpCookieError),
    Redirect(HttpRedirectError),
    RedirectRequiresRestart,
    Transport(HttpTransportError),
    ProxyRequest(HttpProxyRequestError),
    BodyTimeout,
    Body(hyper::Error),
    UnexpectedTrailers,
}

impl HttpPolicyClientError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_http_client_request",
            Self::InvalidHeader => "invalid_http_response_header",
            Self::Destination(error) => error.code(),
            Self::ProxyPolicy(error) => error.code(),
            Self::Request(error) => error.code(),
            Self::Auth(error) => error.code(),
            Self::Cookie(error) => error.code(),
            Self::Redirect(error) => error.code(),
            Self::RedirectRequiresRestart => "redirect_requires_restart",
            Self::Transport(error) => error.code(),
            Self::ProxyRequest(error) => error.code(),
            Self::BodyTimeout => "http_response_body_timeout",
            Self::Body(_) => "http_protocol",
            Self::UnexpectedTrailers => "unexpected_http_trailers",
        }
    }

    #[must_use]
    pub const fn retriable(&self) -> bool {
        match self {
            Self::Destination(error) => error.retriable(),
            Self::Transport(error) => error.retriable(),
            Self::ProxyRequest(error) => error.retriable(),
            Self::BodyTimeout | Self::Body(_) => true,
            _ => false,
        }
    }
}

impl fmt::Display for HttpPolicyClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Destination(error) => error.fmt(formatter),
            Self::ProxyPolicy(error) => error.fmt(formatter),
            Self::Request(error) => error.fmt(formatter),
            Self::Auth(error) => error.fmt(formatter),
            Self::Cookie(error) => error.fmt(formatter),
            Self::Redirect(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::ProxyRequest(error) => error.fmt(formatter),
            Self::Body(error) => error.fmt(formatter),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for HttpPolicyClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Destination(error) => Some(error),
            Self::ProxyPolicy(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::Auth(error) => Some(error),
            Self::Cookie(error) => Some(error),
            Self::Redirect(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::ProxyRequest(error) => Some(error),
            Self::Body(error) => Some(error),
            _ => None,
        }
    }
}

fn forward_proxy_authorization<'a>(
    route: &HttpProxyRoute,
    authorization: Option<&'a HttpProxyAuthorization>,
) -> Option<&'a HttpProxyAuthorization> {
    matches!(route, HttpProxyRoute::HttpForward { .. })
        .then_some(authorization)
        .flatten()
}

fn proxy_resolution_uri(host: &str, port: u16) -> String {
    if host.parse::<Ipv6Addr>().is_ok() {
        format!("http://[{host}]:{port}/")
    } else {
        format!("http://{host}:{port}/")
    }
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

fn single_location(headers: &HeaderMap) -> Result<Option<String>, HttpPolicyClientError> {
    let mut values = headers.get_all(LOCATION).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(HttpPolicyClientError::InvalidHeader);
    }
    first
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| HttpPolicyClientError::InvalidHeader)
        })
        .transpose()
}

fn proxy_endpoint(route: &HttpProxyRoute) -> Option<&crate::HttpProxyEndpoint> {
    match route {
        HttpProxyRoute::HttpForward { proxy, .. } | HttpProxyRoute::HttpConnect { proxy, .. } => {
            Some(proxy)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HttpResolverBackend, HttpResolverConfig, HttpTransportBudgets};
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    fn test_client() -> HttpPolicyClient {
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
                    max_connections_per_origin: 2,
                    max_idle_connections_per_origin: 1,
                    ..HttpDirectTransportConfig::default()
                },
                ..HttpPolicyClientConfig::default()
            },
        )
    }

    async fn read_head(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut byte = [0_u8; 1];
        while !bytes.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.expect("request byte");
            bytes.push(byte[0]);
        }
        String::from_utf8(bytes).expect("ASCII request")
    }

    #[tokio::test]
    async fn custom_headers_follow_same_origin_but_not_redirected_range_destinations() {
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_uri = format!("http://{}/start", origin.local_addr().unwrap());
        let final_uri = format!("http://{}/file", other.local_addr().unwrap());
        let target = final_uri.clone();
        let server = tokio::spawn(async move {
            for location in ["/next".to_owned(), target] {
                let (mut stream, _) = origin.accept().await.unwrap();
                let head = read_head(&mut stream).await.to_ascii_lowercase();
                assert!(head.contains("host: virtual.test\r\n"));
                assert!(head.contains("authorization: bearer canary"));
                assert!(head.contains("cookie: session=canary"));
                assert!(head.contains("referer: https://ref.test/canary"));
                assert!(!head.contains("proxy-authorization:"));
                stream.write_all(format!("HTTP/1.1 302 Found\r\nConnection: close\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
            }
        });
        let destination = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = other.accept().await.unwrap();
                let head = read_head(&mut stream).await.to_ascii_lowercase();
                assert!(!head.contains("canary"));
                assert!(!head.contains("virtual.test"));
                assert!(head.contains("user-agent: client/1"));
                assert!(head.contains("range: bytes=0-1"));
                stream.write_all(b"HTTP/1.1 206 Partial Content\r\nConnection: close\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/2\r\n\r\nok").await.unwrap();
            }
        });
        let mut client = test_client();
        Arc::make_mut(&mut client.config).custom_headers = HttpCustomHeaders::parse("Host: virtual.test\nAuthorization: Bearer canary\nCookie: session=canary\nProxy-Authorization: Bearer proxy-canary\nReferer: https://ref.test/canary\nX-Key: canary\nUser-Agent: client/1").unwrap();
        client.header_origin = Some(Arc::from(origin_uri.clone()));
        for uri in [origin_uri, final_uri] {
            let mut request = HttpClientRequest::get(uri);
            request.range = Some(GlobalSpan { offset: 0, len: 2 });
            let mut response = client.execute(request).await.unwrap();
            assert_eq!(
                response.next_data(Duration::from_secs(2)).await.unwrap(),
                Some(Bytes::from_static(b"ok"))
            );
            assert!(
                response
                    .next_data(Duration::from_secs(2))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        server.await.unwrap();
        destination.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_header_override_reaches_forward_and_connect_handshakes() {
        for secure in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy = crate::HttpProxyEndpoint::new(
                &format!("http://{}", listener.local_addr().unwrap()),
                crate::HttpProxyKind::Http,
                crate::HttpProxyNameResolution::LocalPinned,
                None,
            )
            .unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let head = read_head(&mut stream).await.to_ascii_lowercase();
                assert!(head.contains("proxy-authorization: bearer proxy-canary"));
                assert!(!head.contains("basic "));
                if secure {
                    assert!(head.starts_with("connect "));
                    assert!(!head.contains("origin-canary"));
                    stream
                        .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                } else {
                    assert!(head.starts_with("get http://"));
                    assert!(head.contains("authorization: bearer origin-canary"));
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                }
            });
            let mut client = test_client();
            let config = Arc::make_mut(&mut client.config);
            config.proxy_destination.allow_loopback = true;
            config.proxy =
                HttpProxyPolicy::new(Some(proxy.clone()), Some(proxy), None, []).unwrap();
            config.proxy_request.connect.authorization =
                Some(HttpProxyAuthorization::basic("old", "value").unwrap());
            config.custom_headers = HttpCustomHeaders::parse(
                "Proxy-Authorization: Bearer proxy-canary\nAuthorization: Bearer origin-canary",
            )
            .unwrap();
            let response = client
                .execute(HttpClientRequest::get(format!(
                    "{}://127.0.0.1:12345/file",
                    if secure { "https" } else { "http" }
                )))
                .await;
            assert_eq!(response.is_err(), secure);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn same_origin_redirect_rebuilds_range_request_and_streams_final_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.expect("first");
            let first_head = read_head(&mut first).await;
            assert!(
                first_head
                    .to_ascii_lowercase()
                    .contains("range: bytes=0-1\r\n")
            );
            first
                .write_all(b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\n\r\n")
                .await
                .expect("redirect");
            let (mut second, _) = listener.accept().await.expect("second");
            let second_head = read_head(&mut second).await;
            assert!(second_head.starts_with("GET /final HTTP/1.1\r\n"));
            assert!(
                second_head
                    .to_ascii_lowercase()
                    .contains("range: bytes=0-1\r\n")
            );
            second
                .write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/2\r\n\r\nok")
                .await
                .expect("final");
        });
        let uri = format!("http://{address}/start");
        let mut request = HttpClientRequest::get(uri.clone());
        request.range = Some(GlobalSpan { offset: 0, len: 2 });
        let mut response = test_client().execute(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.redirect_count(), 1);
        assert_eq!(
            response.next_data(Duration::from_secs(1)).await.unwrap(),
            Some(Bytes::from_static(b"ok"))
        );
        assert_eq!(
            response.next_data(Duration::from_secs(1)).await.unwrap(),
            None
        );
        response.finish().await;
        server.await.expect("server");
    }

    #[tokio::test]
    async fn retained_direct_transport_reuses_connection_across_client_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            for _ in 0..2 {
                let head = read_head(&mut stream).await;
                assert!(head.starts_with("GET /file HTTP/1.1\r\n"));
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await
                    .expect("response");
            }
        });
        let client = test_client();
        let uri = format!("http://{address}/file");

        for _ in 0..2 {
            let mut response = client
                .execute(HttpClientRequest::get(uri.clone()))
                .await
                .expect("response");
            assert_eq!(
                response
                    .next_data(Duration::from_secs(1))
                    .await
                    .expect("body frame"),
                Some(Bytes::from_static(b"ok"))
            );
            assert_eq!(
                response
                    .next_data(Duration::from_secs(1))
                    .await
                    .expect("body end"),
                None
            );
            response.finish().await;
        }

        server.await.expect("server");
    }

    #[tokio::test]
    async fn changed_dns_answer_set_opens_a_revalidated_direct_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.expect("first connection");
            let first_head = read_head(&mut first).await;
            assert!(first_head.starts_with("GET /file HTTP/1.1\r\n"));
            first
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .expect("first response");

            let accept = listener.accept();
            tokio::pin!(accept);
            let reused = read_head(&mut first);
            tokio::pin!(reused);
            tokio::select! {
                accepted = &mut accept => {
                    let (mut second, _) = accepted.expect("second connection");
                    let second_head = read_head(&mut second).await;
                    assert!(second_head.starts_with("GET /file HTTP/1.1\r\n"));
                    second
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                        .await
                        .expect("second response");
                }
                _ = &mut reused => panic!("changed DNS answer set reused the old connection"),
            }
        });
        let resolver = HttpResolver::scripted_for_test(
            HttpResolverConfig::default(),
            vec![
                Ok((vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], Duration::ZERO)),
                Ok((
                    vec![
                        IpAddr::V4(Ipv4Addr::LOCALHOST),
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
                    ],
                    Duration::ZERO,
                )),
            ],
        )
        .expect("scripted resolver");
        let budgets = HttpTransportBudgets::new(
            2,
            crate::HTTP_CONNECTION_RESERVATION_BYTES
                .checked_mul(2)
                .expect("two-connection budget size"),
        )
        .expect("two-connection budgets");
        let client = HttpPolicyClient::new(
            resolver,
            HttpPolicyClientConfig {
                destination: HttpDestinationPolicy {
                    allow_loopback: true,
                    ..HttpDestinationPolicy::default()
                },
                direct: HttpDirectTransportConfig {
                    max_connections_per_origin: 2,
                    max_idle_connections_per_origin: 1,
                    budgets,
                    ..HttpDirectTransportConfig::default()
                },
                ..HttpPolicyClientConfig::default()
            },
        );
        let uri = format!("http://revalidate.example:{}/file", address.port());

        for _ in 0..2 {
            let mut response = client
                .execute(HttpClientRequest::get(uri.clone()))
                .await
                .expect("response");
            assert_eq!(
                response
                    .next_data(Duration::from_secs(1))
                    .await
                    .expect("body frame"),
                Some(Bytes::from_static(b"ok"))
            );
            assert_eq!(
                response
                    .next_data(Duration::from_secs(1))
                    .await
                    .expect("body end"),
                None
            );
            response.finish().await;
        }

        let cache = client.direct_transports.lock().await;
        assert_eq!(cache.entries.len(), 2);
        assert_ne!(cache.entries[0].0.addresses, cache.entries[1].0.addresses);
        drop(cache);
        server.await.expect("server");
    }

    #[test]
    fn direct_transport_cache_cap_accounts_for_per_origin_idle_cap() {
        let config = HttpPolicyClientConfig {
            direct: HttpDirectTransportConfig {
                max_idle_connections_per_origin: 2,
                ..HttpDirectTransportConfig::default()
            },
            direct_transport_cache_capacity: MAX_HTTP_DIRECT_TRANSPORT_CACHE_CAPACITY,
            ..HttpPolicyClientConfig::default()
        };
        assert_eq!(effective_direct_transport_cache_capacity(&config), 256);
    }

    #[tokio::test]
    async fn cross_origin_nonzero_range_requires_generation_restart() {
        let first = TcpListener::bind("127.0.0.1:0").await.expect("first");
        let second = TcpListener::bind("127.0.0.1:0").await.expect("second");
        let first_address = first.local_addr().expect("first address");
        let second_address = second.local_addr().expect("second address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = first.accept().await.expect("accept");
            let _head = read_head(&mut stream).await;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://{second_address}/file\r\nContent-Length: 0\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .expect("redirect");
        });
        let uri = format!("http://{first_address}/file");
        let mut request = HttpClientRequest::get(uri);
        request.range = Some(GlobalSpan { offset: 1, len: 1 });
        assert!(matches!(
            test_client().execute(request).await,
            Err(HttpPolicyClientError::RedirectRequiresRestart)
        ));
        server.await.expect("server");
        drop(second);
    }
}
