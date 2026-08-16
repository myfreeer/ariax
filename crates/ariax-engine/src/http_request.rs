//! Redirect-safe HTTP request rebuilding with generated-header ownership.

use crate::{HttpAuthorization, HttpCookieHeader, HttpProxyAuthorization, HttpProxyRoute};
use ariax_storage::GlobalSpan;
use bytes::Bytes;
use http_body_util::Empty;
use hyper::header::{
    ACCEPT_ENCODING, AUTHORIZATION, COOKIE, HOST, HeaderName, HeaderValue, IF_RANGE,
    PROXY_AUTHORIZATION, RANGE,
};
use hyper::{Method, Request, Uri};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

pub const MAX_HTTP_CUSTOM_HEADERS: usize = 64;
pub const MAX_HTTP_CUSTOM_HEADER_NAME_BYTES: usize = 256;
pub const MAX_HTTP_CUSTOM_HEADER_VALUE_BYTES: usize = 8192;
pub const MAX_HTTP_CUSTOM_HEADERS_BYTES: usize = 32 * 1024;

const RESERVED_HEADERS: &[&str] = &[
    "accept-encoding",
    "authorization",
    "connection",
    "content-digest",
    "content-length",
    "cookie",
    "expect",
    "host",
    "if-range",
    "proxy-authorization",
    "range",
    "repr-digest",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "want-repr-digest",
];

const WANT_REPR_DIGEST: HeaderName = HeaderName::from_static("want-repr-digest");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpCustomHeader {
    name: HeaderName,
    value: HeaderValue,
}

impl HttpCustomHeader {
    #[must_use]
    pub const fn name(&self) -> &HeaderName {
        &self.name
    }

    #[must_use]
    pub const fn value(&self) -> &HeaderValue {
        &self.value
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HttpCustomHeaders {
    values: Vec<HttpCustomHeader>,
}

impl HttpCustomHeaders {
    pub fn new(
        headers: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, HttpRequestPolicyError> {
        let mut values = Vec::new();
        let mut names = BTreeSet::new();
        let mut total_bytes = 0_usize;
        for (name, value) in headers {
            if values.len() == MAX_HTTP_CUSTOM_HEADERS {
                return Err(HttpRequestPolicyError::TooManyHeaders);
            }
            if name.is_empty()
                || name.len() > MAX_HTTP_CUSTOM_HEADER_NAME_BYTES
                || value.len() > MAX_HTTP_CUSTOM_HEADER_VALUE_BYTES
            {
                return Err(HttpRequestPolicyError::HeaderTooLarge);
            }
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| HttpRequestPolicyError::InvalidHeader)?;
            if RESERVED_HEADERS.contains(&name.as_str()) {
                return Err(HttpRequestPolicyError::ReservedHeader);
            }
            if !names.insert(name.as_str().to_owned()) {
                return Err(HttpRequestPolicyError::DuplicateHeader);
            }
            let value =
                HeaderValue::from_str(&value).map_err(|_| HttpRequestPolicyError::InvalidHeader)?;
            total_bytes = total_bytes
                .checked_add(name.as_str().len() + value.as_bytes().len())
                .ok_or(HttpRequestPolicyError::HeaderTooLarge)?;
            if total_bytes > MAX_HTTP_CUSTOM_HEADERS_BYTES {
                return Err(HttpRequestPolicyError::HeaderTooLarge);
            }
            values.push(HttpCustomHeader { name, value });
        }
        Ok(Self { values })
    }

    #[must_use]
    pub fn values(&self) -> &[HttpCustomHeader] {
        &self.values
    }
}

pub struct HttpRequestPolicy<'a> {
    pub method: Method,
    pub uri: &'a str,
    pub route: Option<&'a HttpProxyRoute>,
    pub range: Option<GlobalSpan>,
    pub if_range: Option<&'a [u8]>,
    /// Requests a SHA-256 `Repr-Digest` response when strict mirror identity
    /// is being negotiated. The preference is advisory; absence fails only
    /// after the probe has selected the bounded range-digest profile.
    pub want_repr_digest: bool,
    pub authorization: Option<&'a HttpAuthorization>,
    pub proxy_authorization: Option<&'a HttpProxyAuthorization>,
    pub cookie: Option<&'a HttpCookieHeader>,
    pub custom_headers: &'a HttpCustomHeaders,
}

pub type HttpPolicyRequest = Request<Empty<Bytes>>;

pub fn build_http_request(
    policy: HttpRequestPolicy<'_>,
) -> Result<HttpPolicyRequest, HttpRequestPolicyError> {
    if !matches!(policy.method, Method::GET | Method::HEAD) {
        return Err(HttpRequestPolicyError::UnsupportedMethod);
    }
    let uri: Uri = policy
        .uri
        .parse()
        .map_err(|_| HttpRequestPolicyError::InvalidUri)?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) {
        return Err(HttpRequestPolicyError::InvalidUri);
    }
    let authority = uri
        .authority()
        .filter(|authority| !authority.as_str().contains('@'))
        .ok_or(HttpRequestPolicyError::InvalidUri)?;
    let request_uri: Uri = match policy.route {
        Some(HttpProxyRoute::HttpForward { absolute_uri, .. }) => absolute_uri
            .parse()
            .map_err(|_| HttpRequestPolicyError::InvalidRoute)?,
        Some(
            HttpProxyRoute::Direct { .. }
            | HttpProxyRoute::HttpConnect { .. }
            | HttpProxyRoute::Socks5 { .. },
        )
        | None => uri.clone(),
    };
    let mut request = Request::builder()
        .method(policy.method)
        .uri(request_uri)
        .body(Empty::new())
        .map_err(|_| HttpRequestPolicyError::InvalidHeader)?;
    let headers = request.headers_mut();
    for header in policy.custom_headers.values() {
        headers.insert(header.name.clone(), header.value.clone());
    }
    headers.insert(
        HOST,
        HeaderValue::from_str(authority.as_str())
            .map_err(|_| HttpRequestPolicyError::InvalidUri)?,
    );
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    if policy.want_repr_digest {
        headers.insert(WANT_REPR_DIGEST, HeaderValue::from_static("sha-256=10"));
    }
    if let Some(span) = policy.range {
        if span.len == 0 {
            return Err(HttpRequestPolicyError::InvalidRange);
        }
        let length = u64::try_from(span.len).map_err(|_| HttpRequestPolicyError::InvalidRange)?;
        let end = span
            .offset
            .checked_add(length)
            .and_then(|end| end.checked_sub(1))
            .ok_or(HttpRequestPolicyError::InvalidRange)?;
        headers.insert(
            RANGE,
            HeaderValue::from_str(&format!("bytes={}-{}", span.offset, end))
                .map_err(|_| HttpRequestPolicyError::InvalidRange)?,
        );
    }
    if let Some(if_range) = policy.if_range {
        if policy.range.is_none() {
            return Err(HttpRequestPolicyError::IfRangeWithoutRange);
        }
        headers.insert(
            IF_RANGE,
            HeaderValue::from_bytes(if_range)
                .map_err(|_| HttpRequestPolicyError::InvalidValidator)?,
        );
    }
    if let Some(authorization) = policy.authorization {
        headers.insert(AUTHORIZATION, authorization.as_header_value().clone());
    }
    if let Some(cookie) = policy.cookie {
        headers.insert(COOKIE, cookie.as_header_value().clone());
    }
    if let Some(proxy_authorization) = policy.proxy_authorization {
        let forward = matches!(policy.route, Some(HttpProxyRoute::HttpForward { .. }));
        if !forward {
            return Err(HttpRequestPolicyError::ProxyAuthorizationOutsideForward);
        }
        headers.insert(
            PROXY_AUTHORIZATION,
            HeaderValue::from_str(proxy_authorization.value())
                .map_err(|_| HttpRequestPolicyError::InvalidHeader)?,
        );
    }
    Ok(request)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRequestPolicyError {
    InvalidUri,
    InvalidRoute,
    UnsupportedMethod,
    TooManyHeaders,
    HeaderTooLarge,
    InvalidHeader,
    ReservedHeader,
    DuplicateHeader,
    InvalidRange,
    IfRangeWithoutRange,
    InvalidValidator,
    ProxyAuthorizationOutsideForward,
}

impl HttpRequestPolicyError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidUri => "invalid_request_uri",
            Self::InvalidRoute => "invalid_request_route",
            Self::UnsupportedMethod => "unsupported_request_method",
            Self::TooManyHeaders => "too_many_custom_headers",
            Self::HeaderTooLarge => "custom_headers_too_large",
            Self::InvalidHeader => "invalid_custom_header",
            Self::ReservedHeader => "reserved_custom_header",
            Self::DuplicateHeader => "duplicate_custom_header",
            Self::InvalidRange => "invalid_generated_range",
            Self::IfRangeWithoutRange => "if_range_without_range",
            Self::InvalidValidator => "invalid_if_range_validator",
            Self::ProxyAuthorizationOutsideForward => "proxy_authorization_outside_forward",
        }
    }
}

impl fmt::Display for HttpRequestPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpRequestPolicyError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HttpBasicCredentials, HttpProxyEndpoint, HttpProxyKind, HttpProxyNameResolution,
        HttpProxyPolicy,
    };

    #[test]
    fn rejects_reserved_case_variants_duplicates_and_invalid_values() {
        assert_eq!(
            HttpCustomHeaders::new([("rAnGe".to_owned(), "bytes=0-1".to_owned())]),
            Err(HttpRequestPolicyError::ReservedHeader)
        );
        assert_eq!(
            HttpCustomHeaders::new([
                ("x-test".to_owned(), "one".to_owned()),
                ("X-Test".to_owned(), "two".to_owned()),
            ]),
            Err(HttpRequestPolicyError::DuplicateHeader)
        );
        assert_eq!(
            HttpCustomHeaders::new([("x-test".to_owned(), "bad\r\nvalue".to_owned())]),
            Err(HttpRequestPolicyError::InvalidHeader)
        );
    }

    #[test]
    fn builds_generated_range_auth_cookie_and_harmless_custom_headers() {
        let custom =
            HttpCustomHeaders::new([("x-client".to_owned(), "ariax".to_owned())]).expect("custom");
        let authorization = HttpBasicCredentials::new("user".to_owned(), "secret".to_owned())
            .expect("credentials")
            .authorization_header()
            .expect("authorization");
        let request = build_http_request(HttpRequestPolicy {
            method: Method::GET,
            uri: "https://example.test/file",
            route: None,
            range: Some(GlobalSpan { offset: 4, len: 6 }),
            if_range: Some(b"\"v1\""),
            want_repr_digest: false,
            authorization: Some(&authorization),
            proxy_authorization: None,
            cookie: None,
            custom_headers: &custom,
        })
        .expect("request");
        assert_eq!(request.headers()[HOST], "example.test");
        assert_eq!(request.headers()[ACCEPT_ENCODING], "identity");
        assert_eq!(request.headers()[RANGE], "bytes=4-9");
        assert_eq!(request.headers()[IF_RANGE], "\"v1\"");
        assert_eq!(request.headers()["x-client"], "ariax");
        assert_eq!(request.headers()[AUTHORIZATION], "Basic dXNlcjpzZWNyZXQ=");
    }

    #[test]
    fn strict_digest_preference_is_generated_and_reserved() {
        for name in ["Want-Repr-Digest", "Repr-Digest", "Content-Digest"] {
            assert_eq!(
                HttpCustomHeaders::new([(name.to_owned(), "sha-256=1".to_owned(),)]),
                Err(HttpRequestPolicyError::ReservedHeader),
                "{name}"
            );
        }
        let request = build_http_request(HttpRequestPolicy {
            method: Method::GET,
            uri: "https://example.test/file",
            route: None,
            range: Some(GlobalSpan { offset: 0, len: 1 }),
            if_range: None,
            want_repr_digest: true,
            authorization: None,
            proxy_authorization: None,
            cookie: None,
            custom_headers: &HttpCustomHeaders::default(),
        })
        .expect("request");
        assert_eq!(request.headers()[WANT_REPR_DIGEST], "sha-256=10");
    }

    #[test]
    fn forward_proxy_uses_pinned_absolute_form_and_scopes_proxy_authorization() {
        let proxy = HttpProxyEndpoint::new(
            "http://proxy.example:8080",
            HttpProxyKind::Http,
            HttpProxyNameResolution::LocalPinned,
            None,
        )
        .expect("proxy");
        let route = HttpProxyPolicy::new(Some(proxy), None, None, [])
            .expect("policy")
            .route(
                "http://origin.example/file?q=1",
                "203.0.113.8".parse().expect("IP"),
            )
            .expect("route");
        let proxy_auth = HttpProxyAuthorization::basic("proxy", "secret").expect("auth");
        let request = build_http_request(HttpRequestPolicy {
            method: Method::GET,
            uri: "http://origin.example/file?q=1",
            route: Some(&route),
            range: None,
            if_range: None,
            want_repr_digest: false,
            authorization: None,
            proxy_authorization: Some(&proxy_auth),
            cookie: None,
            custom_headers: &HttpCustomHeaders::default(),
        })
        .expect("request");
        assert_eq!(request.uri(), "http://203.0.113.8:80/file?q=1");
        assert_eq!(request.headers()[HOST], "origin.example");
        assert!(request.headers().contains_key(PROXY_AUTHORIZATION));

        assert!(matches!(
            build_http_request(HttpRequestPolicy {
                method: Method::GET,
                uri: "http://origin.example/file",
                route: None,
                range: None,
                if_range: None,
                want_repr_digest: false,
                authorization: None,
                proxy_authorization: Some(&proxy_auth),
                cookie: None,
                custom_headers: &HttpCustomHeaders::default(),
            }),
            Err(HttpRequestPolicyError::ProxyAuthorizationOutsideForward)
        ));
    }
}
