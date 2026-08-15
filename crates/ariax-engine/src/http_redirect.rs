//! Redirect decisions with explicit credential, validator, and lease boundaries.

use crate::HttpMirrorIdentityPolicy;
use hyper::{Method, StatusCode};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use url::Url;

pub const DEFAULT_HTTP_MAX_REDIRECTS: usize = 20;
pub const MAX_HTTP_REDIRECTS: usize = 100;
pub const MAX_HTTP_LOCATION_BYTES: usize = 8192;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRedirectPolicy {
    pub max_redirects: usize,
    pub allow_https_downgrade: bool,
}

impl Default for HttpRedirectPolicy {
    fn default() -> Self {
        Self {
            max_redirects: DEFAULT_HTTP_MAX_REDIRECTS,
            allow_https_downgrade: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRedirectContext {
    pub open_lease: bool,
    pub nonzero_durable_prefix: bool,
    pub shared_whole_entity_digest: bool,
    pub mirror_identity: HttpMirrorIdentityPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRedirectDecision {
    pub target: String,
    pub method: Method,
    pub hop: usize,
    pub origin_changed: bool,
    pub abort_open_lease: bool,
    pub drop_authorization: bool,
    pub drop_if_range: bool,
    pub restart_from_zero: bool,
    pub exclusive_source_replacement: bool,
    pub target_may_join_mirror_pool: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRedirectError {
    InvalidPolicy,
    NotRedirectStatus,
    MissingLocation,
    LocationTooLong,
    InvalidLocation,
    UnsupportedScheme,
    UserInfoForbidden,
    MissingHost,
    DowngradeForbidden,
    LimitExceeded,
    Loop,
}

impl HttpRedirectError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPolicy => "invalid_redirect_policy",
            Self::NotRedirectStatus => "not_redirect_status",
            Self::MissingLocation => "redirect_missing_location",
            Self::LocationTooLong => "redirect_location_too_long",
            Self::InvalidLocation => "invalid_redirect_location",
            Self::UnsupportedScheme => "redirect_unsupported_scheme",
            Self::UserInfoForbidden => "redirect_userinfo_forbidden",
            Self::MissingHost => "redirect_missing_host",
            Self::DowngradeForbidden => "redirect_downgrade_forbidden",
            Self::LimitExceeded => "redirect_limit",
            Self::Loop => "redirect_loop",
        }
    }
}

impl fmt::Display for HttpRedirectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpRedirectError {}

#[derive(Clone, Debug)]
pub struct HttpRedirectState {
    policy: HttpRedirectPolicy,
    current: Url,
    method: Method,
    hops: usize,
    visited: BTreeSet<(String, String)>,
}

impl HttpRedirectState {
    pub fn new(
        initial_uri: &str,
        method: Method,
        policy: HttpRedirectPolicy,
    ) -> Result<Self, HttpRedirectError> {
        if policy.max_redirects == 0 || policy.max_redirects > MAX_HTTP_REDIRECTS {
            return Err(HttpRedirectError::InvalidPolicy);
        }
        let current = parse_http_url(initial_uri)?;
        let mut visited = BTreeSet::new();
        visited.insert((method.as_str().to_owned(), canonical_url(&current)));
        Ok(Self {
            policy,
            current,
            method,
            hops: 0,
            visited,
        })
    }

    #[must_use]
    pub fn current_uri(&self) -> &str {
        self.current.as_str()
    }

    #[must_use]
    pub const fn hops(&self) -> usize {
        self.hops
    }

    pub fn follow(
        &mut self,
        status: StatusCode,
        location: Option<&str>,
        context: HttpRedirectContext,
    ) -> Result<HttpRedirectDecision, HttpRedirectError> {
        if !matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            return Err(HttpRedirectError::NotRedirectStatus);
        }
        if self.hops >= self.policy.max_redirects {
            return Err(HttpRedirectError::LimitExceeded);
        }
        let location = location
            .filter(|location| !location.is_empty())
            .ok_or(HttpRedirectError::MissingLocation)?;
        if location.len() > MAX_HTTP_LOCATION_BYTES {
            return Err(HttpRedirectError::LocationTooLong);
        }
        let mut target = self
            .current
            .join(location)
            .map_err(|_| HttpRedirectError::InvalidLocation)?;
        validate_http_url(&target)?;
        target.set_fragment(None);
        if self.current.scheme() == "https"
            && target.scheme() == "http"
            && !self.policy.allow_https_downgrade
        {
            return Err(HttpRedirectError::DowngradeForbidden);
        }
        let method = redirected_method(&self.method, status);
        let target_text = canonical_url(&target);
        if !self
            .visited
            .insert((method.as_str().to_owned(), target_text.clone()))
        {
            return Err(HttpRedirectError::Loop);
        }
        let origin_changed = origin(&self.current) != origin(&target);
        self.current = target;
        self.method = method.clone();
        self.hops += 1;

        let restart_from_zero =
            origin_changed && context.nonzero_durable_prefix && !context.shared_whole_entity_digest;
        let exclusive_source_replacement = origin_changed
            && context.mirror_identity == HttpMirrorIdentityPolicy::TrustSubmittedMirrors;
        let target_may_join_mirror_pool = !origin_changed
            || (context.mirror_identity == HttpMirrorIdentityPolicy::RequireSharedDigest
                && context.shared_whole_entity_digest);
        Ok(HttpRedirectDecision {
            target: target_text,
            method,
            hop: self.hops,
            origin_changed,
            abort_open_lease: context.open_lease,
            drop_authorization: origin_changed,
            drop_if_range: origin_changed,
            restart_from_zero,
            exclusive_source_replacement,
            target_may_join_mirror_pool,
        })
    }
}

fn parse_http_url(input: &str) -> Result<Url, HttpRedirectError> {
    let mut url = Url::parse(input).map_err(|_| HttpRedirectError::InvalidLocation)?;
    validate_http_url(&url)?;
    url.set_fragment(None);
    Ok(url)
}

fn validate_http_url(url: &Url) -> Result<(), HttpRedirectError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(HttpRedirectError::UnsupportedScheme);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(HttpRedirectError::UserInfoForbidden);
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(HttpRedirectError::MissingHost);
    }
    Ok(())
}

fn redirected_method(method: &Method, status: StatusCode) -> Method {
    if status == StatusCode::SEE_OTHER && *method != Method::HEAD
        || matches!(status.as_u16(), 301 | 302) && *method == Method::POST
    {
        Method::GET
    } else {
        method.clone()
    }
}

fn canonical_url(url: &Url) -> String {
    url.as_str().to_owned()
}

fn origin(url: &Url) -> (&str, Option<&str>, Option<u16>) {
    (url.scheme(), url.host_str(), url.port_or_known_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> HttpRedirectContext {
        HttpRedirectContext {
            open_lease: true,
            nonzero_durable_prefix: true,
            shared_whole_entity_digest: false,
            mirror_identity: HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
        }
    }

    #[test]
    fn same_origin_redirect_keeps_scoped_fields_but_aborts_the_old_lease() {
        let mut state = HttpRedirectState::new(
            "https://example.test/a/file",
            Method::GET,
            HttpRedirectPolicy::default(),
        )
        .expect("state");
        let decision = state
            .follow(StatusCode::TEMPORARY_REDIRECT, Some("../next"), context())
            .expect("redirect");
        assert_eq!(decision.target, "https://example.test/next");
        assert!(!decision.origin_changed);
        assert!(decision.abort_open_lease);
        assert!(!decision.drop_authorization);
        assert!(!decision.drop_if_range);
        assert!(!decision.restart_from_zero);
        assert!(decision.target_may_join_mirror_pool);
    }

    #[test]
    fn cross_origin_redirect_strips_secrets_and_requires_safe_resume_restart() {
        let mut state = HttpRedirectState::new(
            "https://one.example/file",
            Method::GET,
            HttpRedirectPolicy::default(),
        )
        .expect("state");
        let decision = state
            .follow(
                StatusCode::FOUND,
                Some("https://two.example/file"),
                context(),
            )
            .expect("redirect");
        assert!(decision.origin_changed);
        assert!(decision.drop_authorization);
        assert!(decision.drop_if_range);
        assert!(decision.restart_from_zero);
        assert!(decision.exclusive_source_replacement);
        assert!(!decision.target_may_join_mirror_pool);
    }

    #[test]
    fn strict_digest_gate_can_admit_cross_origin_mirror_identity() {
        let mut state = HttpRedirectState::new(
            "https://one.example/file",
            Method::GET,
            HttpRedirectPolicy::default(),
        )
        .expect("state");
        let mut strict = context();
        strict.shared_whole_entity_digest = true;
        strict.mirror_identity = HttpMirrorIdentityPolicy::RequireSharedDigest;
        let decision = state
            .follow(
                StatusCode::PERMANENT_REDIRECT,
                Some("https://two.example/file"),
                strict,
            )
            .expect("redirect");
        assert!(!decision.restart_from_zero);
        assert!(!decision.exclusive_source_replacement);
        assert!(decision.target_may_join_mirror_pool);
    }

    #[test]
    fn rejects_downgrades_loops_userinfo_missing_locations_and_limits() {
        let mut state = HttpRedirectState::new(
            "https://example.test/a",
            Method::GET,
            HttpRedirectPolicy {
                max_redirects: 1,
                allow_https_downgrade: false,
            },
        )
        .expect("state");
        assert_eq!(
            state.follow(StatusCode::FOUND, Some("http://example.test/a"), context()),
            Err(HttpRedirectError::DowngradeForbidden)
        );
        assert_eq!(
            state.follow(
                StatusCode::FOUND,
                Some("https://user@example.test/a"),
                context()
            ),
            Err(HttpRedirectError::UserInfoForbidden)
        );
        assert_eq!(
            state.follow(StatusCode::FOUND, None, context()),
            Err(HttpRedirectError::MissingLocation)
        );
        state
            .follow(StatusCode::FOUND, Some("/b"), context())
            .expect("one redirect");
        assert_eq!(
            state.follow(StatusCode::FOUND, Some("/a"), context()),
            Err(HttpRedirectError::LimitExceeded)
        );

        let mut loop_state = HttpRedirectState::new(
            "https://example.test/a",
            Method::GET,
            HttpRedirectPolicy::default(),
        )
        .expect("state");
        loop_state
            .follow(StatusCode::FOUND, Some("/b"), context())
            .expect("first hop");
        assert_eq!(
            loop_state.follow(StatusCode::FOUND, Some("/a"), context()),
            Err(HttpRedirectError::Loop)
        );
    }
}
