//! Startup-owned HTTP/SOCKS5 proxy selection and final-hop SSRF policy.

use std::error::Error;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use url::{Host, Url};

pub const MAX_HTTP_NO_PROXY_RULES: usize = 256;
pub const MAX_HTTP_NO_PROXY_RULE_BYTES: usize = 253;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpProxyKind {
    Http,
    Socks5,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpProxyNameResolution {
    LocalPinned,
    TrustedProxyEnforced,
}

/// Evidence that startup configuration, rather than an ordinary task/RPC
/// option, selected a destination-filtering proxy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustedProxyEnforcement(());

impl TrustedProxyEnforcement {
    #[must_use]
    pub const fn from_startup_admin_policy() -> Self {
        Self(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpProxyEndpoint {
    kind: HttpProxyKind,
    uri: Arc<str>,
    host: Arc<str>,
    port: u16,
    resolution: HttpProxyNameResolution,
}

impl HttpProxyEndpoint {
    pub fn new(
        endpoint: &str,
        kind: HttpProxyKind,
        resolution: HttpProxyNameResolution,
        trusted: Option<TrustedProxyEnforcement>,
    ) -> Result<Self, HttpProxyPolicyError> {
        if resolution == HttpProxyNameResolution::TrustedProxyEnforced && trusted.is_none() {
            return Err(HttpProxyPolicyError::TrustedProxyEvidenceRequired);
        }
        let url = Url::parse(endpoint).map_err(|_| HttpProxyPolicyError::InvalidProxyUri)?;
        let expected_scheme = match kind {
            HttpProxyKind::Http => "http",
            HttpProxyKind::Socks5 => "socks5",
        };
        if url.scheme() != expected_scheme {
            return Err(HttpProxyPolicyError::UnsupportedProxyScheme);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(HttpProxyPolicyError::ProxyUserInfoForbidden);
        }
        if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
            return Err(HttpProxyPolicyError::InvalidProxyUri);
        }
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(HttpProxyPolicyError::MissingProxyHost)?;
        let port = url
            .port_or_known_default()
            .ok_or(HttpProxyPolicyError::MissingProxyPort)?;
        if port == 0 {
            return Err(HttpProxyPolicyError::MissingProxyPort);
        }
        Ok(Self {
            kind,
            uri: url.as_str().into(),
            host: host.to_ascii_lowercase().into(),
            port,
            resolution,
        })
    }

    #[must_use]
    pub const fn kind(&self) -> HttpProxyKind {
        self.kind
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    #[must_use]
    pub const fn resolution(&self) -> HttpProxyNameResolution {
        self.resolution
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NoProxyRule {
    Any,
    ExactHost(Arc<str>),
    DomainSuffix(Arc<str>),
    Address(IpAddr),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HttpProxyPolicy {
    http_proxy: Option<HttpProxyEndpoint>,
    https_proxy: Option<HttpProxyEndpoint>,
    all_proxy: Option<HttpProxyEndpoint>,
    no_proxy: Arc<[NoProxyRule]>,
}

impl HttpProxyPolicy {
    #[cfg(any(feature = "ftp", feature = "sftp"))]
    pub(crate) fn route_protocol(
        &self,
        target_uri: &str,
        pinned_address: IpAddr,
    ) -> Result<HttpProxyRoute, HttpProxyPolicyError> {
        Self {
            http_proxy: None,
            https_proxy: None,
            all_proxy: self.all_proxy.clone(),
            no_proxy: self.no_proxy.clone(),
        }
        .route(target_uri, pinned_address)
    }
    pub fn new(
        http_proxy: Option<HttpProxyEndpoint>,
        https_proxy: Option<HttpProxyEndpoint>,
        all_proxy: Option<HttpProxyEndpoint>,
        no_proxy: impl IntoIterator<Item = String>,
    ) -> Result<Self, HttpProxyPolicyError> {
        let mut rules = Vec::new();
        for text in no_proxy {
            if rules.len() == MAX_HTTP_NO_PROXY_RULES {
                return Err(HttpProxyPolicyError::TooManyNoProxyRules);
            }
            rules.push(parse_no_proxy_rule(&text)?);
        }
        Ok(Self {
            http_proxy,
            https_proxy,
            all_proxy,
            no_proxy: rules.into(),
        })
    }

    pub fn route(
        &self,
        target_uri: &str,
        pinned_address: IpAddr,
    ) -> Result<HttpProxyRoute, HttpProxyPolicyError> {
        let target = parse_target(target_uri)?;
        let target_host = target.host_str().expect("validated target host");
        if self.bypasses(target_host) {
            return Ok(HttpProxyRoute::Direct {
                peer: pinned_address,
            });
        }
        let proxy = match target.scheme() {
            "http" => self.http_proxy.as_ref().or(self.all_proxy.as_ref()),
            "https" => self.https_proxy.as_ref().or(self.all_proxy.as_ref()),
            _ => None,
        };
        let Some(proxy) = proxy else {
            return Ok(HttpProxyRoute::Direct {
                peer: pinned_address,
            });
        };
        let port = target
            .port_or_known_default()
            .ok_or(HttpProxyPolicyError::InvalidTargetUri)?;
        let original_authority = authority(target_host, port);
        let routed_host = match proxy.resolution {
            HttpProxyNameResolution::LocalPinned => authority(&pinned_address.to_string(), port),
            HttpProxyNameResolution::TrustedProxyEnforced => original_authority.clone(),
        };
        match (proxy.kind, target.scheme()) {
            (HttpProxyKind::Http, "http") => Ok(HttpProxyRoute::HttpForward {
                proxy: proxy.clone(),
                absolute_uri: absolute_uri_with_authority(&target, &routed_host),
                host_header: original_authority,
                pinned_target: (proxy.resolution == HttpProxyNameResolution::LocalPinned)
                    .then_some(pinned_address),
            }),
            (HttpProxyKind::Http, "https") => Ok(HttpProxyRoute::HttpConnect {
                proxy: proxy.clone(),
                connect_authority: routed_host,
                server_name: target_host.to_owned(),
                host_header: original_authority,
                pinned_target: (proxy.resolution == HttpProxyNameResolution::LocalPinned)
                    .then_some(pinned_address),
            }),
            (HttpProxyKind::Socks5, _) => Ok(HttpProxyRoute::Socks5 {
                proxy: proxy.clone(),
                target: match proxy.resolution {
                    HttpProxyNameResolution::LocalPinned => {
                        HttpSocksTarget::Address(pinned_address, port)
                    }
                    HttpProxyNameResolution::TrustedProxyEnforced => {
                        HttpSocksTarget::Domain(target_host.to_owned(), port)
                    }
                },
                server_name: target_host.to_owned(),
                host_header: original_authority,
            }),
            _ => Err(HttpProxyPolicyError::InvalidTargetUri),
        }
    }

    fn bypasses(&self, host: &str) -> bool {
        let normalized = host.trim_end_matches('.').to_ascii_lowercase();
        let address = normalized.parse::<IpAddr>().ok();
        self.no_proxy.iter().any(|rule| match rule {
            NoProxyRule::Any => true,
            NoProxyRule::ExactHost(expected) => normalized == expected.as_ref(),
            NoProxyRule::DomainSuffix(suffix) => {
                normalized == suffix.as_ref()
                    || normalized
                        .strip_suffix(suffix.as_ref())
                        .is_some_and(|prefix| prefix.ends_with('.'))
            }
            NoProxyRule::Address(expected) => address == Some(*expected),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HttpSocksTarget {
    Address(IpAddr, u16),
    Domain(String, u16),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HttpProxyRoute {
    Direct {
        peer: IpAddr,
    },
    HttpForward {
        proxy: HttpProxyEndpoint,
        absolute_uri: String,
        host_header: String,
        pinned_target: Option<IpAddr>,
    },
    HttpConnect {
        proxy: HttpProxyEndpoint,
        connect_authority: String,
        server_name: String,
        host_header: String,
        pinned_target: Option<IpAddr>,
    },
    Socks5 {
        proxy: HttpProxyEndpoint,
        target: HttpSocksTarget,
        server_name: String,
        host_header: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpProxyPolicyError {
    InvalidProxyUri,
    UnsupportedProxyScheme,
    ProxyUserInfoForbidden,
    MissingProxyHost,
    MissingProxyPort,
    TrustedProxyEvidenceRequired,
    TooManyNoProxyRules,
    InvalidNoProxyRule,
    InvalidTargetUri,
}

impl HttpProxyPolicyError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidProxyUri => "invalid_proxy_uri",
            Self::UnsupportedProxyScheme => "unsupported_proxy_scheme",
            Self::ProxyUserInfoForbidden => "proxy_userinfo_forbidden",
            Self::MissingProxyHost => "missing_proxy_host",
            Self::MissingProxyPort => "missing_proxy_port",
            Self::TrustedProxyEvidenceRequired => "trusted_proxy_evidence_required",
            Self::TooManyNoProxyRules => "too_many_no_proxy_rules",
            Self::InvalidNoProxyRule => "invalid_no_proxy_rule",
            Self::InvalidTargetUri => "invalid_proxy_target_uri",
        }
    }
}

impl fmt::Display for HttpProxyPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpProxyPolicyError {}

fn parse_no_proxy_rule(input: &str) -> Result<NoProxyRule, HttpProxyPolicyError> {
    let input = input.trim();
    if input.is_empty() || input.len() > MAX_HTTP_NO_PROXY_RULE_BYTES || !input.is_ascii() {
        return Err(HttpProxyPolicyError::InvalidNoProxyRule);
    }
    if input == "*" {
        return Ok(NoProxyRule::Any);
    }
    let input = input.trim_end_matches('.').to_ascii_lowercase();
    if let Ok(address) = input.parse() {
        return Ok(NoProxyRule::Address(address));
    }
    let (suffix, domain_rule) = input
        .strip_prefix('.')
        .map_or((input.as_str(), false), |suffix| (suffix, true));
    if suffix.is_empty()
        || suffix.split('.').any(|label| {
            label.is_empty()
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(HttpProxyPolicyError::InvalidNoProxyRule);
    }
    Ok(if domain_rule {
        NoProxyRule::DomainSuffix(suffix.to_owned().into())
    } else {
        NoProxyRule::ExactHost(suffix.to_owned().into())
    })
}

fn parse_target(input: &str) -> Result<Url, HttpProxyPolicyError> {
    let target = Url::parse(input).map_err(|_| HttpProxyPolicyError::InvalidTargetUri)?;
    if !matches!(target.scheme(), "http" | "https")
        || target.host_str().is_none_or(str::is_empty)
        || !target.username().is_empty()
        || target.password().is_some()
    {
        return Err(HttpProxyPolicyError::InvalidTargetUri);
    }
    Ok(target)
}

fn authority(host: &str, port: u16) -> String {
    match Host::parse(host) {
        Ok(Host::Ipv6(_)) => format!("[{host}]:{port}"),
        _ => format!("{host}:{port}"),
    }
}

fn absolute_uri_with_authority(target: &Url, routed_authority: &str) -> String {
    let mut output = format!("{}://{routed_authority}{}", target.scheme(), target.path());
    if let Some(query) = target.query() {
        output.push('?');
        output.push_str(query);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip() -> IpAddr {
        "203.0.113.9".parse().expect("IP")
    }

    #[test]
    fn untrusted_http_and_connect_routes_use_pinned_numeric_targets() {
        let proxy = HttpProxyEndpoint::new(
            "http://proxy.example:8080",
            HttpProxyKind::Http,
            HttpProxyNameResolution::LocalPinned,
            None,
        )
        .expect("proxy");
        let policy =
            HttpProxyPolicy::new(Some(proxy.clone()), Some(proxy), None, []).expect("policy");
        assert!(matches!(
            policy.route("http://origin.example/a?q=1", ip()).expect("route"),
            HttpProxyRoute::HttpForward {
                absolute_uri,
                host_header,
                pinned_target: Some(address),
                ..
            } if absolute_uri == "http://203.0.113.9:80/a?q=1"
                && host_header == "origin.example:80"
                && address == ip()
        ));
        assert!(matches!(
            policy.route("https://origin.example/a", ip()).expect("route"),
            HttpProxyRoute::HttpConnect {
                connect_authority,
                server_name,
                pinned_target: Some(address),
                ..
            } if connect_authority == "203.0.113.9:443"
                && server_name == "origin.example"
                && address == ip()
        ));
    }

    #[test]
    fn proxy_side_hostname_resolution_requires_startup_trust_evidence() {
        assert_eq!(
            HttpProxyEndpoint::new(
                "socks5://proxy.example:1080",
                HttpProxyKind::Socks5,
                HttpProxyNameResolution::TrustedProxyEnforced,
                None,
            ),
            Err(HttpProxyPolicyError::TrustedProxyEvidenceRequired)
        );
        let proxy = HttpProxyEndpoint::new(
            "socks5://proxy.example:1080",
            HttpProxyKind::Socks5,
            HttpProxyNameResolution::TrustedProxyEnforced,
            Some(TrustedProxyEnforcement::from_startup_admin_policy()),
        )
        .expect("trusted proxy");
        let policy = HttpProxyPolicy::new(None, None, Some(proxy), []).expect("policy");
        assert!(matches!(
            policy.route("https://origin.example/a", ip()).expect("route"),
            HttpProxyRoute::Socks5 {
                target: HttpSocksTarget::Domain(host, 443),
                server_name,
                ..
            } if host == "origin.example" && server_name == "origin.example"
        ));
    }

    #[test]
    fn no_proxy_rules_bypass_only_exact_or_label_boundary_matches() {
        let proxy = HttpProxyEndpoint::new(
            "http://proxy.example:8080",
            HttpProxyKind::Http,
            HttpProxyNameResolution::LocalPinned,
            None,
        )
        .expect("proxy");
        let policy = HttpProxyPolicy::new(
            Some(proxy),
            None,
            None,
            ["exact.example".to_owned(), ".internal.example".to_owned()],
        )
        .expect("policy");
        assert!(matches!(
            policy.route("http://exact.example/a", ip()).expect("route"),
            HttpProxyRoute::Direct { .. }
        ));
        assert!(matches!(
            policy
                .route("http://api.internal.example/a", ip())
                .expect("route"),
            HttpProxyRoute::Direct { .. }
        ));
        assert!(matches!(
            policy
                .route("http://notinternal.example/a", ip())
                .expect("route"),
            HttpProxyRoute::HttpForward { .. }
        ));
    }

    #[test]
    fn rejects_proxy_userinfo_wrong_schemes_and_malformed_bypass_rules() {
        assert_eq!(
            HttpProxyEndpoint::new(
                "http://user:secret@proxy.example:8080",
                HttpProxyKind::Http,
                HttpProxyNameResolution::LocalPinned,
                None,
            ),
            Err(HttpProxyPolicyError::ProxyUserInfoForbidden)
        );
        assert_eq!(
            HttpProxyEndpoint::new(
                "https://proxy.example:8080",
                HttpProxyKind::Http,
                HttpProxyNameResolution::LocalPinned,
                None,
            ),
            Err(HttpProxyPolicyError::UnsupportedProxyScheme)
        );
        assert!(matches!(
            HttpProxyPolicy::new(None, None, None, ["bad..example".to_owned()]),
            Err(HttpProxyPolicyError::InvalidNoProxyRule)
        ));
    }
}
