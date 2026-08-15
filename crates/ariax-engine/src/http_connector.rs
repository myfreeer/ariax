//! Bounded HTTP destination admission and final-peer binding.
//!
//! The first connector milestone deliberately stops at plaintext HTTP. It
//! parses the authority, resolves a hostname through the system resolver with
//! a bounded timeout/cardinality, classifies every answer, and returns the
//! exact numeric peer selected for one connection attempt. TLS, proxies,
//! redirects, cache/TTL policy, and Happy Eyeballs are separate gates.

use crate::{HttpResolver, HttpResolverError};
use hyper::Uri;
use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::time::timeout;

/// Hard maximum for one resolver result set before policy filtering.
pub const MAX_HTTP_DESTINATION_ADDRESSES: usize = GENERATED_HTTP_MAX_DESTINATION_ADDRESSES;
/// Maximum DNS host text accepted by the ordinary-URI connector.
pub const MAX_HTTP_DESTINATION_HOST_BYTES: usize = GENERATED_HTTP_MAX_DESTINATION_HOST_BYTES;
/// Default timeout for one bounded system-resolver operation.
pub const DEFAULT_HTTP_RESOLUTION_TIMEOUT: Duration =
    Duration::from_secs(GENERATED_HTTP_DEFAULT_RESOLUTION_TIMEOUT_SECONDS);

/// Address classes used by the HTTP destination guardrail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpAddressClass {
    GlobalUnicast,
    Loopback,
    Private,
    UniqueLocal,
    CarrierGradeNat,
    LinkLocal,
    Metadata,
    Documentation,
    Benchmark,
    Unspecified,
    Multicast,
    Reserved,
}

impl HttpAddressClass {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::GlobalUnicast => "global_unicast",
            Self::Loopback => "loopback",
            Self::Private => "private",
            Self::UniqueLocal => "unique_local",
            Self::CarrierGradeNat => "carrier_grade_nat",
            Self::LinkLocal => "link_local",
            Self::Metadata => "metadata",
            Self::Documentation => "documentation",
            Self::Benchmark => "benchmark",
            Self::Unspecified => "unspecified",
            Self::Multicast => "multicast",
            Self::Reserved => "reserved",
        }
    }
}

include!("http_special_purpose_generated.rs");

/// Explicit policy for one HTTP destination admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpDestinationPolicy {
    pub resolve_timeout: Duration,
    pub max_addresses: usize,
    /// Loopback is denied by default and may only be enabled by a local
    /// administrator/test harness.
    pub allow_loopback: bool,
    /// RFC 1918 IPv4 and IPv6 unique-local addresses are denied by default.
    pub allow_private: bool,
}

impl Default for HttpDestinationPolicy {
    fn default() -> Self {
        Self {
            resolve_timeout: DEFAULT_HTTP_RESOLUTION_TIMEOUT,
            max_addresses: MAX_HTTP_DESTINATION_ADDRESSES,
            allow_loopback: false,
            allow_private: false,
        }
    }
}

impl HttpDestinationPolicy {
    fn validate(self) -> Result<Self, HttpDestinationError> {
        if self.resolve_timeout.is_zero()
            || self.max_addresses == 0
            || self.max_addresses > MAX_HTTP_DESTINATION_ADDRESSES
        {
            return Err(HttpDestinationError::InvalidPolicy);
        }
        Ok(self)
    }
}

/// A destination whose numeric peer was admitted and pinned for one attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedHttpDestination {
    uri: Uri,
    authority: Box<str>,
    addresses: Box<[SocketAddr]>,
    peer: SocketAddr,
}

impl ResolvedHttpDestination {
    #[must_use]
    pub const fn uri(&self) -> &Uri {
        &self.uri
    }

    #[must_use]
    pub fn authority(&self) -> &str {
        &self.authority
    }

    #[must_use]
    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    #[must_use]
    pub const fn peer(&self) -> SocketAddr {
        self.peer
    }
}

/// Stable destination-admission failures. Raw URI/host values are never
/// included in the display text, keeping diagnostics safe for remote RPC.
#[derive(Debug)]
pub enum HttpDestinationError {
    InvalidUri,
    UnsupportedScheme,
    MissingAuthority,
    UserInfoForbidden,
    MissingHost,
    InvalidHost,
    InvalidPort,
    InvalidPolicy,
    ResolveTimeout,
    Resolve(std::io::Error),
    PolicyResolver(HttpResolverError),
    NoAddresses,
    TooManyAddresses {
        max: usize,
    },
    AddressDenied {
        address: IpAddr,
        class: HttpAddressClass,
    },
}

impl HttpDestinationError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidUri => "invalid_uri",
            Self::UnsupportedScheme => "unsupported_scheme",
            Self::MissingAuthority => "missing_authority",
            Self::UserInfoForbidden => "uri_userinfo_forbidden",
            Self::MissingHost => "missing_host",
            Self::InvalidHost => "invalid_host",
            Self::InvalidPort => "invalid_port",
            Self::InvalidPolicy => "invalid_destination_policy",
            Self::ResolveTimeout => "dns_timeout",
            Self::Resolve(_) => "dns_resolve",
            Self::PolicyResolver(error) => error.code(),
            Self::NoAddresses => "no_destination_addresses",
            Self::TooManyAddresses { .. } => "too_many_destination_addresses",
            Self::AddressDenied { .. } => "destination_denied",
        }
    }

    #[must_use]
    pub const fn retriable(&self) -> bool {
        match self {
            Self::ResolveTimeout | Self::Resolve(_) | Self::NoAddresses => true,
            Self::PolicyResolver(error) => error.retriable(),
            _ => false,
        }
    }
}

impl fmt::Display for HttpDestinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddressDenied { address, class } => {
                write!(
                    formatter,
                    "destination address {address} denied ({})",
                    class.code()
                )
            }
            Self::TooManyAddresses { max } => {
                write!(
                    formatter,
                    "destination resolver returned more than {max} addresses"
                )
            }
            Self::Resolve(error) => write!(formatter, "destination resolution failed: {error}"),
            Self::PolicyResolver(error) => error.fmt(formatter),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for HttpDestinationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Resolve(error) => Some(error),
            Self::PolicyResolver(error) => Some(error),
            _ => None,
        }
    }
}

/// Classifies an address using the conservative first-milestone special-use
/// table. IPv4-mapped IPv6 values are classified as their embedded IPv4 value.
#[must_use]
pub fn classify_http_address(address: IpAddr) -> HttpAddressClass {
    match address {
        IpAddr::V4(address) => classify_ipv4(address),
        IpAddr::V6(address) => {
            if let Some(mapped) = mapped_ipv4(address) {
                return classify_ipv4(mapped);
            }
            classify_ipv6(address)
        }
    }
}

/// Resolves and admits one HTTP URI, returning a numeric peer pinned for this
/// connection attempt. Every answer is classified before any peer is exposed.
pub async fn resolve_http_destination(
    uri_text: &str,
    policy: HttpDestinationPolicy,
) -> Result<ResolvedHttpDestination, HttpDestinationError> {
    let policy = policy.validate()?;
    let uri: Uri = uri_text
        .parse()
        .map_err(|_| HttpDestinationError::InvalidUri)?;
    let scheme = uri.scheme_str();
    if !matches!(scheme, Some("http" | "https")) {
        return Err(HttpDestinationError::UnsupportedScheme);
    }
    let authority = uri
        .authority()
        .ok_or(HttpDestinationError::MissingAuthority)?;
    if authority.as_str().contains('@') {
        return Err(HttpDestinationError::UserInfoForbidden);
    }
    let authority_host = authority.host();
    let bracketed = authority_host.starts_with('[');
    let host = authority_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(authority_host);
    if host.is_empty() {
        return Err(HttpDestinationError::MissingHost);
    }
    let explicit_port = if bracketed {
        authority
            .as_str()
            .strip_prefix(authority_host)
            .is_some_and(|suffix| !suffix.is_empty())
    } else {
        authority.as_str()[host.len()..].starts_with(':')
    };
    if explicit_port && authority.port_u16().is_none() {
        return Err(HttpDestinationError::InvalidPort);
    }
    let port = authority
        .port_u16()
        .unwrap_or(if scheme == Some("https") { 443 } else { 80 });
    if port == 0 {
        return Err(HttpDestinationError::InvalidPort);
    }
    validate_host(host, bracketed)?;
    let authority_text = authority.as_str().to_owned();

    let addresses = if let Some(address) = parse_numeric_host(host, bracketed)? {
        admit_addresses([SocketAddr::new(address, port)], policy)?
    } else {
        let lookup = timeout(policy.resolve_timeout, lookup_host((host, port)))
            .await
            .map_err(|_| HttpDestinationError::ResolveTimeout)?
            .map_err(HttpDestinationError::Resolve)?;
        admit_addresses(lookup, policy)?
    };

    Ok(ResolvedHttpDestination {
        uri,
        authority: authority_text.into(),
        peer: addresses[0],
        addresses,
    })
}

/// Resolves a hostname through the shared project-owned resolver, then applies
/// the same fail-closed full-answer destination policy and numeric peer pinning
/// as the bootstrap system-resolver path.
pub async fn resolve_http_destination_with_resolver(
    uri_text: &str,
    policy: HttpDestinationPolicy,
    resolver: &HttpResolver,
) -> Result<ResolvedHttpDestination, HttpDestinationError> {
    let policy = policy.validate()?;
    let uri: Uri = uri_text
        .parse()
        .map_err(|_| HttpDestinationError::InvalidUri)?;
    let scheme = uri.scheme_str();
    if !matches!(scheme, Some("http" | "https")) {
        return Err(HttpDestinationError::UnsupportedScheme);
    }
    let authority = uri
        .authority()
        .ok_or(HttpDestinationError::MissingAuthority)?;
    if authority.as_str().contains('@') {
        return Err(HttpDestinationError::UserInfoForbidden);
    }
    let authority_host = authority.host();
    let bracketed = authority_host.starts_with('[');
    let host = authority_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(authority_host);
    if host.is_empty() {
        return Err(HttpDestinationError::MissingHost);
    }
    let explicit_port = if bracketed {
        authority
            .as_str()
            .strip_prefix(authority_host)
            .is_some_and(|suffix| !suffix.is_empty())
    } else {
        authority.as_str()[host.len()..].starts_with(':')
    };
    if explicit_port && authority.port_u16().is_none() {
        return Err(HttpDestinationError::InvalidPort);
    }
    let port = authority
        .port_u16()
        .unwrap_or(if scheme == Some("https") { 443 } else { 80 });
    if port == 0 {
        return Err(HttpDestinationError::InvalidPort);
    }
    validate_host(host, bracketed)?;
    let authority_text = authority.as_str().to_owned();

    let addresses = if let Some(address) = parse_numeric_host(host, bracketed)? {
        admit_addresses([SocketAddr::new(address, port)], policy)?
    } else {
        let resolved = resolver
            .resolve(host)
            .await
            .map_err(HttpDestinationError::PolicyResolver)?;
        admit_addresses(
            resolved
                .addresses()
                .iter()
                .copied()
                .map(|address| SocketAddr::new(address, port)),
            policy,
        )?
    };

    Ok(ResolvedHttpDestination {
        uri,
        authority: authority_text.into(),
        peer: addresses[0],
        addresses,
    })
}

fn address_allowed(class: HttpAddressClass, policy: HttpDestinationPolicy) -> bool {
    match class {
        HttpAddressClass::GlobalUnicast => true,
        HttpAddressClass::Loopback => policy.allow_loopback,
        HttpAddressClass::Private | HttpAddressClass::UniqueLocal => policy.allow_private,
        HttpAddressClass::CarrierGradeNat
        | HttpAddressClass::LinkLocal
        | HttpAddressClass::Metadata
        | HttpAddressClass::Documentation
        | HttpAddressClass::Benchmark
        | HttpAddressClass::Unspecified
        | HttpAddressClass::Multicast
        | HttpAddressClass::Reserved => false,
    }
}

fn validate_host(host: &str, bracketed: bool) -> Result<(), HttpDestinationError> {
    if bracketed {
        if host.parse::<Ipv6Addr>().is_err() {
            return Err(HttpDestinationError::InvalidHost);
        }
        return Ok(());
    }
    if host.len() > MAX_HTTP_DESTINATION_HOST_BYTES || !host.is_ascii() {
        return Err(HttpDestinationError::InvalidHost);
    }
    let name = host.strip_suffix('.').unwrap_or(host);
    if name.is_empty() {
        return Err(HttpDestinationError::InvalidHost);
    }
    for label in name.split('.') {
        if label.is_empty() || label.len() > 63 || label.starts_with('-') || label.ends_with('-') {
            return Err(HttpDestinationError::InvalidHost);
        }
        if !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'~'))
        {
            return Err(HttpDestinationError::InvalidHost);
        }
    }
    Ok(())
}

fn parse_numeric_host(host: &str, bracketed: bool) -> Result<Option<IpAddr>, HttpDestinationError> {
    if bracketed {
        return host
            .parse::<Ipv6Addr>()
            .map(IpAddr::V6)
            .map(Some)
            .map_err(|_| HttpDestinationError::InvalidHost);
    }
    if let Ok(address) = host.parse::<IpAddr>() {
        return Ok(Some(address));
    }
    if !looks_like_legacy_ipv4(host) {
        return Ok(None);
    }
    parse_legacy_ipv4(host)
        .map(IpAddr::V4)
        .map(Some)
        .map_err(|_| HttpDestinationError::InvalidHost)
}

fn looks_like_legacy_ipv4(host: &str) -> bool {
    host.bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
        || host
            .split('.')
            .any(|part| part.len() > 2 && part[..2].eq_ignore_ascii_case("0x"))
}

fn parse_legacy_ipv4(host: &str) -> Result<Ipv4Addr, ()> {
    let parts: Vec<_> = host.split('.').collect();
    if parts.is_empty() || parts.len() > 4 || parts.iter().any(|part| part.is_empty()) {
        return Err(());
    }
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        values.push(parse_legacy_component(part)?);
    }
    let value = match values.as_slice() {
        [a] => *a,
        [a, b] if *a <= 0xff && *b <= 0x00ff_ffff => (*a << 24) | *b,
        [a, b, c] if *a <= 0xff && *b <= 0xff && *c <= 0xffff => (*a << 24) | (*b << 16) | *c,
        [a, b, c, d] if [a, b, c, d].iter().all(|value| **value <= 0xff) => {
            (*a << 24) | (*b << 16) | (*c << 8) | *d
        }
        _ => return Err(()),
    };
    Ok(Ipv4Addr::from(value))
}

fn parse_legacy_component(part: &str) -> Result<u32, ()> {
    let (digits, radix) = if part.len() > 2 && part[..2].eq_ignore_ascii_case("0x") {
        (&part[2..], 16)
    } else if part.len() > 1 && part.starts_with('0') {
        (part, 8)
    } else {
        (part, 10)
    };
    if digits.is_empty() {
        return Err(());
    }
    u32::from_str_radix(digits, radix).map_err(|_| ())
}

fn admit_addresses(
    addresses: impl IntoIterator<Item = SocketAddr>,
    policy: HttpDestinationPolicy,
) -> Result<Box<[SocketAddr]>, HttpDestinationError> {
    let mut admitted = Vec::with_capacity(policy.max_addresses);
    for address in addresses {
        let address = canonicalize_socket_addr(address);
        if admitted.contains(&address) {
            continue;
        }
        if admitted.len() == policy.max_addresses {
            return Err(HttpDestinationError::TooManyAddresses {
                max: policy.max_addresses,
            });
        }
        let class = classify_http_address(address.ip());
        if !address_allowed(class, policy) {
            return Err(HttpDestinationError::AddressDenied {
                address: address.ip(),
                class,
            });
        }
        admitted.push(address);
    }
    if admitted.is_empty() {
        return Err(HttpDestinationError::NoAddresses);
    }
    Ok(admitted.into_boxed_slice())
}

fn canonicalize_socket_addr(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V6(address) => mapped_ipv4(*address.ip()).map_or(address.into(), |mapped| {
            SocketAddr::new(IpAddr::V4(mapped), address.port())
        }),
        SocketAddr::V4(_) => address,
    }
}

fn classify_ipv4(address: Ipv4Addr) -> HttpAddressClass {
    let value = u32::from(address);
    GENERATED_HTTP_IPV4_PREFIXES
        .iter()
        .find(|(network, length, _)| prefix_matches_u32(value, *network, *length))
        .map_or(HttpAddressClass::GlobalUnicast, |(_, _, class)| *class)
}

fn classify_ipv6(address: Ipv6Addr) -> HttpAddressClass {
    let value = u128::from(address);
    GENERATED_HTTP_IPV6_PREFIXES
        .iter()
        .find(|(network, length, _)| prefix_matches_u128(value, *network, *length))
        .map_or_else(
            || {
                if value & (0xe0_u128 << 120) == (0x20_u128 << 120) {
                    HttpAddressClass::GlobalUnicast
                } else {
                    HttpAddressClass::Reserved
                }
            },
            |(_, _, class)| *class,
        )
}

fn mapped_ipv4(address: Ipv6Addr) -> Option<Ipv4Addr> {
    let segments = address.segments();
    (segments[..5] == [0, 0, 0, 0, 0] && segments[5] == 0xffff)
        .then(|| Ipv4Addr::from(u32::from(segments[6]) << 16 | u32::from(segments[7])))
}

fn prefix_matches_u32(value: u32, network: u32, length: u8) -> bool {
    if length == 0 {
        true
    } else {
        value & (u32::MAX << (32 - length)) == network
    }
}

fn prefix_matches_u128(value: u128, network: u128, length: u8) -> bool {
    if length == 0 {
        true
    } else {
        value & (u128::MAX << (128 - length)) == network
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_classifier_canonicalizes_mapped_and_special_values() {
        assert_eq!(
            classify_http_address("127.0.0.1".parse().expect("ip")),
            HttpAddressClass::Loopback
        );
        assert_eq!(
            classify_http_address("::ffff:169.254.169.254".parse().expect("ip")),
            HttpAddressClass::Metadata
        );
        assert_eq!(
            classify_http_address("::ffff:127.0.0.1".parse().expect("ip")),
            HttpAddressClass::Loopback
        );
        assert_eq!(
            classify_http_address("::1".parse().expect("ip")),
            HttpAddressClass::Loopback
        );
        assert_eq!(
            classify_http_address("10.0.0.1".parse().expect("ip")),
            HttpAddressClass::Private
        );
        assert_eq!(
            classify_http_address("8.8.8.8".parse().expect("ip")),
            HttpAddressClass::GlobalUnicast
        );
        assert_eq!(
            classify_http_address("2001:db8::1".parse().expect("ip")),
            HttpAddressClass::Documentation
        );
        assert_eq!(
            classify_http_address("224.0.0.1".parse().expect("ip")),
            HttpAddressClass::Multicast
        );
        assert_eq!(
            classify_http_address("ff02::1".parse().expect("ip")),
            HttpAddressClass::Multicast
        );
        assert_eq!(
            classify_http_address("100.64.0.1".parse().expect("ip")),
            HttpAddressClass::CarrierGradeNat
        );
        assert_eq!(
            classify_http_address("8000::1".parse().expect("ip")),
            HttpAddressClass::Reserved
        );
    }

    #[test]
    fn generated_prefix_tables_are_canonical_and_longest_first() {
        for prefixes in [
            GENERATED_HTTP_IPV4_PREFIXES
                .iter()
                .map(|(network, length, _)| (u128::from(*network), *length, 32_u8))
                .collect::<Vec<_>>(),
            GENERATED_HTTP_IPV6_PREFIXES
                .iter()
                .map(|(network, length, _)| (*network, *length, 128_u8))
                .collect::<Vec<_>>(),
        ] {
            assert!(prefixes.windows(2).all(|pair| pair[0].1 >= pair[1].1));
            for (network, length, bits) in prefixes {
                let mask = if length == 0 {
                    0
                } else {
                    u128::MAX << (bits - length)
                };
                assert_eq!(network & mask, network);
            }
        }
    }

    #[tokio::test]
    async fn numeric_destination_requires_explicit_loopback_policy() {
        let uri = "http://127.0.0.1:8080/file";
        assert!(matches!(
            resolve_http_destination(uri, HttpDestinationPolicy::default()).await,
            Err(HttpDestinationError::AddressDenied {
                class: HttpAddressClass::Loopback,
                ..
            })
        ));
        let policy = HttpDestinationPolicy {
            allow_loopback: true,
            ..HttpDestinationPolicy::default()
        };
        let resolved = resolve_http_destination(uri, policy)
            .await
            .expect("resolve");
        assert_eq!(resolved.peer(), "127.0.0.1:8080".parse().expect("peer"));
        assert_eq!(
            resolved.addresses(),
            &["127.0.0.1:8080".parse().expect("peer")]
        );
        assert_eq!(resolved.authority(), "127.0.0.1:8080");
    }

    #[tokio::test]
    async fn malformed_numeric_hosts_and_invalid_policy_fail_before_lookup() {
        assert!(matches!(
            resolve_http_destination("http://2130706433/file", HttpDestinationPolicy::default())
                .await,
            Err(HttpDestinationError::AddressDenied {
                class: HttpAddressClass::Loopback,
                ..
            })
        ));
        assert!(matches!(
            resolve_http_destination(
                "http://8.8.8.8/file",
                HttpDestinationPolicy {
                    max_addresses: 0,
                    ..HttpDestinationPolicy::default()
                }
            )
            .await,
            Err(HttpDestinationError::InvalidPolicy)
        ));
    }

    #[tokio::test]
    async fn authority_validation_rejects_unsupported_or_ambiguous_inputs() {
        for (uri, expected) in [
            ("http://user@8.8.8.8/file", "uri_userinfo_forbidden"),
            ("http://8.8.8.8:0/file", "invalid_port"),
            ("http://-bad.example/file", "invalid_host"),
            ("http://1.2.3.999/file", "invalid_host"),
            ("http://[v1.invalid]/file", "invalid_host"),
        ] {
            let error = resolve_http_destination(uri, HttpDestinationPolicy::default())
                .await
                .expect_err("authority rejected");
            assert_eq!(error.code(), expected, "unexpected result for {uri}");
        }
        let https =
            resolve_http_destination("https://8.8.8.8/file", HttpDestinationPolicy::default())
                .await
                .expect("HTTPS is a supported direct transport scheme");
        assert_eq!(
            https.peer(),
            "8.8.8.8:443".parse().expect("HTTPS default port")
        );
        let unsupported =
            resolve_http_destination("ftp://8.8.8.8/file", HttpDestinationPolicy::default())
                .await
                .expect_err("unsupported scheme");
        assert_eq!(unsupported.code(), "unsupported_scheme");
        let overlong = format!("http://{}/file", "a".repeat(254));
        assert!(matches!(
            resolve_http_destination(&overlong, HttpDestinationPolicy::default()).await,
            Err(HttpDestinationError::InvalidHost | HttpDestinationError::InvalidUri)
        ));
    }

    #[tokio::test]
    async fn legacy_and_bracketed_numeric_hosts_are_canonicalized_before_policy() {
        let policy = HttpDestinationPolicy {
            allow_loopback: true,
            ..HttpDestinationPolicy::default()
        };
        let legacy = resolve_http_destination("http://0x7f000001:8080/file", policy)
            .await
            .expect("legacy IPv4");
        assert_eq!(legacy.peer(), "127.0.0.1:8080".parse().expect("peer"));
        assert_eq!(legacy.authority(), "0x7f000001:8080");

        let ipv6 = resolve_http_destination("http://[::1]:8080/file", policy)
            .await
            .expect("bracketed IPv6");
        assert_eq!(ipv6.peer(), "[::1]:8080".parse().expect("peer"));
        assert_eq!(ipv6.authority(), "[::1]:8080");
    }

    #[tokio::test]
    async fn private_policy_is_narrow_and_special_use_remains_denied() {
        let policy = HttpDestinationPolicy {
            allow_loopback: true,
            allow_private: true,
            ..HttpDestinationPolicy::default()
        };
        for (uri, class) in [
            ("http://10.0.0.1/file", HttpAddressClass::Private),
            ("http://192.168.1.1/file", HttpAddressClass::Private),
            ("http://[fd00::1]/file", HttpAddressClass::UniqueLocal),
        ] {
            let resolved = resolve_http_destination(uri, policy)
                .await
                .expect("private allowed");
            assert_eq!(classify_http_address(resolved.peer().ip()), class);
        }
        for (uri, class) in [
            ("http://100.64.0.1/file", HttpAddressClass::CarrierGradeNat),
            ("http://169.254.1.1/file", HttpAddressClass::LinkLocal),
            ("http://169.254.169.254/file", HttpAddressClass::Metadata),
            ("http://192.0.2.1/file", HttpAddressClass::Documentation),
            ("http://198.18.0.1/file", HttpAddressClass::Benchmark),
            ("http://224.0.0.1/file", HttpAddressClass::Multicast),
            ("http://0.0.0.0/file", HttpAddressClass::Unspecified),
            ("http://240.0.0.1/file", HttpAddressClass::Reserved),
            ("http://[fe80::1]/file", HttpAddressClass::LinkLocal),
            ("http://[2001:db8::1]/file", HttpAddressClass::Documentation),
            ("http://[ff02::1]/file", HttpAddressClass::Multicast),
        ] {
            assert!(matches!(
                resolve_http_destination(uri, policy).await,
                Err(HttpDestinationError::AddressDenied { class: actual, .. }) if actual == class
            ));
        }
    }

    #[test]
    fn admission_deduplicates_and_rejects_mixed_or_excess_answers() {
        let policy = HttpDestinationPolicy {
            max_addresses: 2,
            ..HttpDestinationPolicy::default()
        };
        let deduplicated = admit_addresses(
            [
                "8.8.8.8:80".parse().expect("peer"),
                "8.8.8.8:80".parse().expect("peer"),
                "1.1.1.1:80".parse().expect("peer"),
            ],
            policy,
        )
        .expect("deduplicate");
        assert_eq!(deduplicated.len(), 2);
        assert!(matches!(
            admit_addresses(
                [
                    "8.8.8.8:80".parse().expect("peer"),
                    "127.0.0.1:80".parse().expect("peer"),
                ],
                policy,
            ),
            Err(HttpDestinationError::AddressDenied {
                class: HttpAddressClass::Loopback,
                ..
            })
        ));
        assert!(matches!(
            admit_addresses(
                [
                    "8.8.8.8:80".parse().expect("peer"),
                    "1.1.1.1:80".parse().expect("peer"),
                    "9.9.9.9:80".parse().expect("peer"),
                ],
                policy,
            ),
            Err(HttpDestinationError::TooManyAddresses { max: 2 })
        ));
    }

    #[tokio::test]
    async fn hostname_resolution_applies_the_same_loopback_policy() {
        let denied =
            resolve_http_destination("http://localhost:80/file", HttpDestinationPolicy::default())
                .await;
        assert!(matches!(
            denied,
            Err(HttpDestinationError::AddressDenied { .. })
                | Err(HttpDestinationError::Resolve(_))
                | Err(HttpDestinationError::NoAddresses)
        ));
        let policy = HttpDestinationPolicy {
            allow_loopback: true,
            ..HttpDestinationPolicy::default()
        };
        let resolved = resolve_http_destination("http://localhost:80/file", policy)
            .await
            .expect("localhost resolves");
        assert!(!resolved.addresses().is_empty());
        assert!(
            resolved
                .addresses()
                .iter()
                .all(|address| classify_http_address(address.ip()) == HttpAddressClass::Loopback)
        );
    }
}
