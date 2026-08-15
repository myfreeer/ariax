//! Bounded DNS resolution with project-owned caching and single-flight work.

use hickory_resolver::TokioResolver;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::lookup_host;
use tokio::sync::{Mutex, watch};
use tokio::time::timeout;

pub const DEFAULT_HTTP_DNS_POSITIVE_CACHE_CAPACITY: usize = 4096;
pub const DEFAULT_HTTP_DNS_NEGATIVE_CACHE_CAPACITY: usize = 512;
pub const DEFAULT_HTTP_DNS_MAX_POSITIVE_TTL: Duration = Duration::from_secs(86_400);
pub const DEFAULT_HTTP_DNS_MAX_NEGATIVE_TTL: Duration = Duration::from_secs(30);
pub const DEFAULT_HTTP_DNS_MAX_IN_FLIGHT: usize = 128;
pub const DEFAULT_HTTP_DNS_MAX_TOTAL_WAITERS: usize = 4096;
pub const DEFAULT_HTTP_DNS_MAX_WAITERS_PER_NAME: usize = 1024;
pub const DEFAULT_HTTP_DNS_MAX_ADDRESSES: usize = 32;
pub const MAX_HTTP_DNS_HOST_BYTES: usize = 253;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpResolverBackend {
    System,
    #[default]
    Hickory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpResolverConfig {
    pub backend: HttpResolverBackend,
    pub timeout: Duration,
    pub positive_cache_capacity: usize,
    pub negative_cache_capacity: usize,
    pub max_positive_ttl: Duration,
    pub max_negative_ttl: Duration,
    pub max_in_flight: usize,
    pub max_total_waiters: usize,
    pub max_waiters_per_name: usize,
    pub max_addresses: usize,
}

impl Default for HttpResolverConfig {
    fn default() -> Self {
        Self {
            backend: HttpResolverBackend::Hickory,
            timeout: Duration::from_secs(10),
            positive_cache_capacity: DEFAULT_HTTP_DNS_POSITIVE_CACHE_CAPACITY,
            negative_cache_capacity: DEFAULT_HTTP_DNS_NEGATIVE_CACHE_CAPACITY,
            max_positive_ttl: DEFAULT_HTTP_DNS_MAX_POSITIVE_TTL,
            max_negative_ttl: DEFAULT_HTTP_DNS_MAX_NEGATIVE_TTL,
            max_in_flight: DEFAULT_HTTP_DNS_MAX_IN_FLIGHT,
            max_total_waiters: DEFAULT_HTTP_DNS_MAX_TOTAL_WAITERS,
            max_waiters_per_name: DEFAULT_HTTP_DNS_MAX_WAITERS_PER_NAME,
            max_addresses: DEFAULT_HTTP_DNS_MAX_ADDRESSES,
        }
    }
}

impl HttpResolverConfig {
    fn validate(self) -> Result<Self, HttpResolverError> {
        if self.timeout.is_zero()
            || self.positive_cache_capacity == 0
            || self.positive_cache_capacity > DEFAULT_HTTP_DNS_POSITIVE_CACHE_CAPACITY
            || self.negative_cache_capacity == 0
            || self.negative_cache_capacity > DEFAULT_HTTP_DNS_NEGATIVE_CACHE_CAPACITY
            || self.max_positive_ttl.is_zero()
            || self.max_positive_ttl > DEFAULT_HTTP_DNS_MAX_POSITIVE_TTL
            || self.max_negative_ttl.is_zero()
            || self.max_negative_ttl > DEFAULT_HTTP_DNS_MAX_NEGATIVE_TTL
            || self.max_in_flight == 0
            || self.max_in_flight > DEFAULT_HTTP_DNS_MAX_IN_FLIGHT
            || self.max_total_waiters == 0
            || self.max_total_waiters > DEFAULT_HTTP_DNS_MAX_TOTAL_WAITERS
            || self.max_waiters_per_name == 0
            || self.max_waiters_per_name > DEFAULT_HTTP_DNS_MAX_WAITERS_PER_NAME
            || self.max_addresses == 0
            || self.max_addresses > DEFAULT_HTTP_DNS_MAX_ADDRESSES
        {
            return Err(HttpResolverError::InvalidConfig);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResolvedHost {
    addresses: Arc<[IpAddr]>,
    from_cache: bool,
}

impl HttpResolvedHost {
    #[must_use]
    pub fn addresses(&self) -> &[IpAddr] {
        &self.addresses
    }

    #[must_use]
    pub const fn from_cache(&self) -> bool {
        self.from_cache
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpResolverError {
    InvalidConfig,
    InvalidHost,
    Startup,
    Busy,
    TooManyWaiters,
    Timeout,
    ResolutionFailed,
    NoAddresses,
    TooManyAddresses,
    WorkerClosed,
}

impl HttpResolverError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_resolver_config",
            Self::InvalidHost => "invalid_dns_host",
            Self::Startup => "dns_startup",
            Self::Busy => "dns_busy",
            Self::TooManyWaiters => "dns_too_many_waiters",
            Self::Timeout => "dns_timeout",
            Self::ResolutionFailed => "dns_resolve",
            Self::NoAddresses => "no_destination_addresses",
            Self::TooManyAddresses => "too_many_destination_addresses",
            Self::WorkerClosed => "dns_worker_closed",
        }
    }

    #[must_use]
    pub const fn retriable(self) -> bool {
        matches!(
            self,
            Self::Busy
                | Self::TooManyWaiters
                | Self::Timeout
                | Self::ResolutionFailed
                | Self::NoAddresses
                | Self::WorkerClosed
        )
    }
}

impl fmt::Display for HttpResolverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpResolverError {}

#[derive(Clone, Debug)]
struct BackendLookup {
    addresses: Vec<IpAddr>,
    ttl: Duration,
}

type BackendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BackendLookup, HttpResolverError>> + Send + 'a>>;

trait LookupBackend: Send + Sync {
    fn lookup<'a>(&'a self, host: &'a str) -> BackendFuture<'a>;
}

#[derive(Debug)]
struct SystemLookup;

impl LookupBackend for SystemLookup {
    fn lookup<'a>(&'a self, host: &'a str) -> BackendFuture<'a> {
        Box::pin(async move {
            let addresses = lookup_host((host, 0))
                .await
                .map_err(|_| HttpResolverError::ResolutionFailed)?
                .map(|address| address.ip())
                .collect();
            Ok(BackendLookup {
                addresses,
                ttl: Duration::ZERO,
            })
        })
    }
}

#[derive(Debug)]
struct HickoryLookup {
    resolver: TokioResolver,
}

impl HickoryLookup {
    fn new() -> Result<Self, HttpResolverError> {
        let mut builder = TokioResolver::builder_tokio().map_err(|_| HttpResolverError::Startup)?;
        builder.options_mut().cache_size = 0;
        let resolver = builder.build().map_err(|_| HttpResolverError::Startup)?;
        Ok(Self { resolver })
    }
}

impl LookupBackend for HickoryLookup {
    fn lookup<'a>(&'a self, host: &'a str) -> BackendFuture<'a> {
        Box::pin(async move {
            let lookup = self
                .resolver
                .lookup_ip(host)
                .await
                .map_err(|_| HttpResolverError::ResolutionFailed)?;
            let ttl = lookup
                .valid_until()
                .saturating_duration_since(Instant::now());
            Ok(BackendLookup {
                addresses: lookup.iter().collect(),
                ttl,
            })
        })
    }
}

#[derive(Clone, Debug)]
enum SharedLookup {
    Positive(Arc<[IpAddr]>),
    Negative(HttpResolverError),
}

#[derive(Clone, Debug)]
struct CacheEntry {
    value: SharedLookup,
    expires_at: Instant,
    last_used: u64,
}

#[derive(Debug, Default)]
struct ResolverState {
    cache: BTreeMap<String, CacheEntry>,
    in_flight: BTreeMap<String, watch::Sender<Option<SharedLookup>>>,
    sequence: u64,
}

#[derive(Clone)]
pub struct HttpResolver {
    config: HttpResolverConfig,
    backend: Arc<dyn LookupBackend>,
    state: Arc<Mutex<ResolverState>>,
}

impl fmt::Debug for HttpResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpResolver")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl HttpResolver {
    pub fn new(config: HttpResolverConfig) -> Result<Self, HttpResolverError> {
        let config = config.validate()?;
        let backend: Arc<dyn LookupBackend> = match config.backend {
            HttpResolverBackend::System => Arc::new(SystemLookup),
            HttpResolverBackend::Hickory => Arc::new(HickoryLookup::new()?),
        };
        Ok(Self::with_backend(config, backend))
    }

    fn with_backend(config: HttpResolverConfig, backend: Arc<dyn LookupBackend>) -> Self {
        Self {
            config,
            backend,
            state: Arc::new(Mutex::new(ResolverState::default())),
        }
    }

    pub async fn resolve(&self, host: &str) -> Result<HttpResolvedHost, HttpResolverError> {
        let host = normalize_host(host)?;
        let now = Instant::now();
        let mut state = self.state.lock().await;
        prune_expired(&mut state, now);
        state.sequence = state.sequence.saturating_add(1);
        let sequence = state.sequence;
        if let Some(entry) = state.cache.get_mut(&host) {
            entry.last_used = sequence;
            return match &entry.value {
                SharedLookup::Positive(addresses) => Ok(HttpResolvedHost {
                    addresses: Arc::clone(addresses),
                    from_cache: true,
                }),
                SharedLookup::Negative(error) => Err(*error),
            };
        }

        let receiver = if let Some(sender) = state.in_flight.get(&host) {
            if sender.receiver_count() >= self.config.max_waiters_per_name {
                return Err(HttpResolverError::TooManyWaiters);
            }
            if total_waiters(&state) >= self.config.max_total_waiters {
                return Err(HttpResolverError::TooManyWaiters);
            }
            sender.subscribe()
        } else {
            if state.in_flight.len() >= self.config.max_in_flight {
                return Err(HttpResolverError::Busy);
            }
            let (sender, receiver) = watch::channel(None);
            state.in_flight.insert(host.clone(), sender.clone());
            let resolver = self.clone();
            let worker_host = host.clone();
            tokio::spawn(async move {
                resolver.run_lookup(worker_host, sender).await;
            });
            receiver
        };
        drop(state);
        self.await_lookup(receiver).await
    }

    async fn await_lookup(
        &self,
        mut receiver: watch::Receiver<Option<SharedLookup>>,
    ) -> Result<HttpResolvedHost, HttpResolverError> {
        let result = timeout(self.config.timeout, async {
            loop {
                if let Some(result) = receiver.borrow_and_update().clone() {
                    return Ok(result);
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| HttpResolverError::WorkerClosed)?;
            }
        })
        .await
        .map_err(|_| HttpResolverError::Timeout)??;
        match result {
            SharedLookup::Positive(addresses) => Ok(HttpResolvedHost {
                addresses,
                from_cache: false,
            }),
            SharedLookup::Negative(error) => Err(error),
        }
    }

    async fn run_lookup(&self, host: String, sender: watch::Sender<Option<SharedLookup>>) {
        let lookup = tokio::select! {
            lookup = timeout(self.config.timeout, self.backend.lookup(&host)) => Some(lookup),
            () = sender.closed() => None,
        };
        let Some(lookup) = lookup else {
            self.state.lock().await.in_flight.remove(&host);
            return;
        };
        let (result, ttl) = match lookup {
            Err(_) => (
                SharedLookup::Negative(HttpResolverError::Timeout),
                Duration::ZERO,
            ),
            Ok(Err(error)) => (SharedLookup::Negative(error), self.config.max_negative_ttl),
            Ok(Ok(lookup)) => {
                match normalize_addresses(lookup.addresses, self.config.max_addresses) {
                    Ok(addresses) => (
                        SharedLookup::Positive(addresses),
                        lookup.ttl.min(self.config.max_positive_ttl),
                    ),
                    Err(error) => (SharedLookup::Negative(error), self.config.max_negative_ttl),
                }
            }
        };

        let mut state = self.state.lock().await;
        state.in_flight.remove(&host);
        if !ttl.is_zero() {
            state.sequence = state.sequence.saturating_add(1);
            let sequence = state.sequence;
            state.cache.insert(
                host,
                CacheEntry {
                    value: result.clone(),
                    expires_at: Instant::now() + ttl,
                    last_used: sequence,
                },
            );
            enforce_cache_bounds(&mut state, self.config);
        }
        drop(state);
        let _receivers_notified = sender.send(Some(result));
    }
}

fn normalize_host(host: &str) -> Result<String, HttpResolverError> {
    let host = host.strip_suffix('.').unwrap_or(host);
    if host.is_empty()
        || host.len() > MAX_HTTP_DNS_HOST_BYTES
        || !host.is_ascii()
        || host.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(HttpResolverError::InvalidHost);
    }
    Ok(host.to_ascii_lowercase())
}

fn normalize_addresses(
    addresses: Vec<IpAddr>,
    max_addresses: usize,
) -> Result<Arc<[IpAddr]>, HttpResolverError> {
    let mut seen = BTreeSet::new();
    let mut ipv4 = Vec::new();
    let mut ipv6 = Vec::new();
    let mut first_is_ipv6 = false;
    for address in addresses {
        if !seen.insert(address) {
            continue;
        }
        if seen.len() == 1 {
            first_is_ipv6 = address.is_ipv6();
        }
        match address {
            IpAddr::V4(_) => ipv4.push(address),
            IpAddr::V6(_) => ipv6.push(address),
        }
    }
    if seen.is_empty() {
        return Err(HttpResolverError::NoAddresses);
    }
    if seen.len() > max_addresses {
        return Err(HttpResolverError::TooManyAddresses);
    }
    let mut ordered = Vec::with_capacity(seen.len());
    let mut ipv4 = ipv4.into_iter();
    let mut ipv6 = ipv6.into_iter();
    loop {
        let mut pushed = false;
        if first_is_ipv6 {
            if let Some(address) = ipv6.next() {
                ordered.push(address);
                pushed = true;
            }
            if let Some(address) = ipv4.next() {
                ordered.push(address);
                pushed = true;
            }
        } else {
            if let Some(address) = ipv4.next() {
                ordered.push(address);
                pushed = true;
            }
            if let Some(address) = ipv6.next() {
                ordered.push(address);
                pushed = true;
            }
        }
        if !pushed {
            break;
        }
    }
    Ok(ordered.into())
}

fn total_waiters(state: &ResolverState) -> usize {
    state
        .in_flight
        .values()
        .map(watch::Sender::receiver_count)
        .sum()
}

fn prune_expired(state: &mut ResolverState, now: Instant) {
    state.cache.retain(|_, entry| entry.expires_at > now);
}

fn enforce_cache_bounds(state: &mut ResolverState, config: HttpResolverConfig) {
    enforce_cache_kind(state, config.positive_cache_capacity, true);
    enforce_cache_kind(state, config.negative_cache_capacity, false);
}

fn enforce_cache_kind(state: &mut ResolverState, capacity: usize, positive: bool) {
    loop {
        let count = state
            .cache
            .values()
            .filter(|entry| matches!(entry.value, SharedLookup::Positive(_)) == positive)
            .count();
        if count <= capacity {
            return;
        }
        let Some(key) = state
            .cache
            .iter()
            .filter(|(_, entry)| matches!(entry.value, SharedLookup::Positive(_)) == positive)
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        state.cache.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct FakeLookup {
        calls: AtomicUsize,
        result: Result<BackendLookup, HttpResolverError>,
        delay: Duration,
    }

    impl LookupBackend for FakeLookup {
        fn lookup<'a>(&'a self, _host: &'a str) -> BackendFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(self.delay).await;
                self.result.clone()
            })
        }
    }

    fn resolver_with(
        result: Result<BackendLookup, HttpResolverError>,
        delay: Duration,
    ) -> (HttpResolver, Arc<FakeLookup>) {
        let backend = Arc::new(FakeLookup {
            calls: AtomicUsize::new(0),
            result,
            delay,
        });
        let resolver = HttpResolver::with_backend(HttpResolverConfig::default(), backend.clone());
        (resolver, backend)
    }

    #[tokio::test]
    async fn coalesces_concurrent_lookups_and_serves_bounded_positive_cache() {
        let (resolver, backend) = resolver_with(
            Ok(BackendLookup {
                addresses: vec!["203.0.113.1".parse().expect("IP")],
                ttl: Duration::from_secs(60),
            }),
            Duration::from_millis(10),
        );
        let (first, second) = tokio::join!(
            resolver.resolve("Example.TEST"),
            resolver.resolve("example.test.")
        );
        assert_eq!(
            first.expect("first lookup").addresses(),
            second.expect("second lookup").addresses()
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert!(
            resolver
                .resolve("example.test")
                .await
                .expect("cached lookup")
                .from_cache()
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn system_style_zero_ttl_results_are_not_cached() {
        let (resolver, backend) = resolver_with(
            Ok(BackendLookup {
                addresses: vec!["2001:db8::1".parse().expect("IP")],
                ttl: Duration::ZERO,
            }),
            Duration::ZERO,
        );
        assert!(
            !resolver
                .resolve("one.example")
                .await
                .expect("first")
                .from_cache()
        );
        assert!(
            !resolver
                .resolve("one.example")
                .await
                .expect("second")
                .from_cache()
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn rejects_invalid_names_and_oversized_answer_sets() {
        let (resolver, _) = resolver_with(
            Ok(BackendLookup {
                addresses: (1..=33)
                    .map(|last| IpAddr::from([198, 51, 100, last]))
                    .collect(),
                ttl: Duration::from_secs(60),
            }),
            Duration::ZERO,
        );
        assert_eq!(
            resolver.resolve("bad..example").await,
            Err(HttpResolverError::InvalidHost)
        );
        assert_eq!(
            resolver.resolve("many.example").await,
            Err(HttpResolverError::TooManyAddresses)
        );
        assert_eq!(
            resolver.resolve("many.example").await,
            Err(HttpResolverError::TooManyAddresses)
        );
    }
}
