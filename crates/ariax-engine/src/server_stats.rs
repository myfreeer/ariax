//! Bounded, non-secret origin feedback shared by transfer workers.
use crate::{HttpIngressBudgets, HttpIngressPermit};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(crate) fn transfer_origin(text: &str) -> Option<String> {
    let uri: hyper::Uri = text.parse().ok()?;
    let protocol = crate::TransferProtocol::parse(uri.scheme_str()?).ok()?;
    let authority = uri.authority()?;
    if authority.as_str().contains('@') || authority.as_str().len() > 500 {
        return None;
    }
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = match authority.port_u16() {
        Some(port) => port,
        None if authority.as_str().ends_with(']') || !authority.as_str().contains(':') => {
            protocol.default_port()
        }
        None => return None,
    };
    if host.is_empty() || port == 0 {
        return None;
    }
    let host = host.to_ascii_lowercase();
    Some(if host.contains(':') {
        format!("{}://[{host}]:{port}", protocol.code())
    } else {
        format!("{}://{host}:{port}", protocol.code())
    })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServerFeedback {
    pub bytes_per_second: u64,
    pub samples: u32,
    pub failures: u32,
}
impl ServerFeedback {
    pub(crate) fn observe(&mut self, bytes: u64, elapsed: Duration, succeeded: bool) {
        self.samples = self.samples.saturating_add(1);
        if succeeded {
            let micros = elapsed.as_micros().max(1);
            let speed = (u128::from(bytes) * 1_000_000 / micros).min(u128::from(u64::MAX)) as u64;
            self.bytes_per_second = if self.samples == 1 {
                speed
            } else {
                self.bytes_per_second
                    .saturating_mul(3)
                    .saturating_add(speed)
                    / 4
            };
            self.failures = self.failures.saturating_sub(1);
        } else {
            self.failures = self.failures.saturating_add(1).min(32);
        }
    }
    pub(crate) fn score(self, concurrent: usize) -> u64 {
        let throughput = if self.samples == 0 {
            1_048_576
        } else {
            self.bytes_per_second
        };
        throughput / (u64::from(self.failures) + 1) / (concurrent as u64 + 1)
    }
}
struct Entry {
    feedback: ServerFeedback,
    updated_ms: u64,
    timeout_ms: u64,
    used: u64,
    _permit: HttpIngressPermit,
}
struct State {
    entries: BTreeMap<String, Entry>,
    tick: u64,
}
struct Inner {
    capacity: usize,
    timeout_ms: u64,
    bytes: HttpIngressBudgets,
    clock: Instant,
    state: Mutex<State>,
}
#[derive(Clone)]
pub struct ServerStatistics(Arc<Inner>);
impl std::fmt::Debug for ServerStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerStatistics")
            .field("capacity", &self.0.capacity)
            .field("entries", &self.len())
            .finish()
    }
}
impl Default for ServerStatistics {
    fn default() -> Self {
        Self::new(
            4096,
            Duration::from_secs(86400),
            HttpIngressBudgets::new(4 * 1024 * 1024),
        )
    }
}
impl ServerStatistics {
    pub fn new(capacity: usize, timeout: Duration, bytes: HttpIngressBudgets) -> Self {
        Self(Arc::new(Inner {
            capacity: capacity.min(4096),
            timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            bytes,
            clock: Instant::now(),
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                tick: 0,
            }),
        }))
    }
    pub fn len(&self) -> usize {
        self.0
            .state
            .lock()
            .expect("server statistics")
            .entries
            .len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn feedback(&self, origin: &str) -> ServerFeedback {
        self.feedback_at(origin, self.now_ms())
    }
    pub fn observe(
        &self,
        origin: &str,
        bytes: u64,
        elapsed: Duration,
        succeeded: bool,
    ) -> ServerFeedback {
        self.observe_at(origin, bytes, elapsed, succeeded, self.now_ms())
    }
    fn now_ms(&self) -> u64 {
        self.0.clock.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
    }
    pub fn feedback_with_timeout(&self, origin: &str, timeout: Duration) -> ServerFeedback {
        self.feedback_at_with_timeout(
            origin,
            self.now_ms(),
            timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        )
    }
    fn feedback_at(&self, origin: &str, now: u64) -> ServerFeedback {
        self.feedback_at_with_timeout(origin, now, self.0.timeout_ms)
    }
    fn feedback_at_with_timeout(&self, origin: &str, now: u64, timeout_ms: u64) -> ServerFeedback {
        if timeout_ms == 0 {
            return ServerFeedback::default();
        }
        let mut state = self.0.state.lock().expect("server statistics");
        state.tick = state.tick.saturating_add(1);
        let tick = state.tick;
        if state
            .entries
            .get(origin)
            .is_some_and(|entry| now.saturating_sub(entry.updated_ms) >= entry.timeout_ms)
        {
            state.entries.remove(origin);
        }
        state
            .entries
            .get_mut(origin)
            .filter(|entry| now.saturating_sub(entry.updated_ms) < timeout_ms)
            .map(|entry| {
                entry.used = tick;
                entry.feedback
            })
            .unwrap_or_default()
    }
    fn observe_at(
        &self,
        origin: &str,
        bytes: u64,
        elapsed: Duration,
        succeeded: bool,
        now: u64,
    ) -> ServerFeedback {
        self.observe_at_with_timeout(origin, bytes, elapsed, succeeded, now, self.0.timeout_ms)
    }
    pub fn observe_with_timeout(
        &self,
        origin: &str,
        bytes: u64,
        elapsed: Duration,
        succeeded: bool,
        timeout: Duration,
    ) -> ServerFeedback {
        self.observe_at_with_timeout(
            origin,
            bytes,
            elapsed,
            succeeded,
            self.now_ms(),
            timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn observe_at_with_timeout(
        &self,
        origin: &str,
        bytes: u64,
        elapsed: Duration,
        succeeded: bool,
        now: u64,
        timeout_ms: u64,
    ) -> ServerFeedback {
        // Origin keys are bounded and never retain userinfo, paths or queries.
        if self.0.capacity == 0
            || timeout_ms == 0
            || origin.len() > 512
            || origin.contains('@')
            || !origin.parse::<hyper::Uri>().is_ok_and(|uri| {
                uri.authority().is_some()
                    && matches!(
                        uri.scheme_str(),
                        Some("http" | "https" | "ftp" | "ftps" | "sftp")
                    )
                    && matches!(uri.path(), "" | "/")
                    && uri.query().is_none()
            })
        {
            return ServerFeedback::default();
        }
        let mut state = self.0.state.lock().expect("server statistics");
        state.tick = state.tick.saturating_add(1);
        let tick = state.tick;
        if !state.entries.contains_key(origin) {
            state
                .entries
                .retain(|_, entry| now.saturating_sub(entry.updated_ms) < entry.timeout_ms);
            if state.entries.len() >= self.0.capacity
                && let Some(oldest) = state
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.used)
                    .map(|(key, _)| key.clone())
            {
                state.entries.remove(&oldest);
            }
            let Ok(permit) = self.0.bytes.try_acquire(origin.len().saturating_add(256)) else {
                return ServerFeedback::default();
            };
            state.entries.insert(
                origin.to_owned(),
                Entry {
                    feedback: ServerFeedback::default(),
                    updated_ms: now,
                    timeout_ms,
                    used: tick,
                    _permit: permit,
                },
            );
        }
        let entry = state
            .entries
            .get_mut(origin)
            .expect("inserted server entry");
        if now.saturating_sub(entry.updated_ms) >= entry.timeout_ms.min(timeout_ms) {
            entry.feedback = ServerFeedback::default();
        }
        entry.feedback.observe(bytes, elapsed, succeeded);
        entry.updated_ms = now;
        entry.timeout_ms = timeout_ms;
        entry.used = tick;
        entry.feedback
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn origin_feedback_covers_protocols_without_retaining_authority_or_path_secrets() {
        for (uri, expected) in [
            (
                "http://Example.test/path?token=secret",
                "http://example.test:80",
            ),
            ("https://example.test/path", "https://example.test:443"),
            ("ftp://example.test/path", "ftp://example.test:21"),
            ("ftps://example.test/path", "ftps://example.test:990"),
            ("sftp://example.test:2222/path", "sftp://example.test:2222"),
            ("ftp://[::1]:2121/path", "ftp://[::1]:2121"),
        ] {
            assert_eq!(transfer_origin(uri).as_deref(), Some(expected));
        }
        for uri in [
            "ftp://secret@example.test/path",
            "ftp://example.test:0/path",
            "ftp://example.test:65536/path",
            "file:///path",
            "/relative",
        ] {
            assert_eq!(transfer_origin(uri), None);
        }
        assert!(
            crate::HttpRangeSource::from_transfer_uri(
                ariax_core::UriId::new(0),
                "ftp://example.test/path"
            )
            .is_err()
        );
    }
    #[test]
    fn timeout_override_expires_feedback_and_zero_disables_updates() {
        let stats = ServerStatistics::default();
        stats.observe_at_with_timeout("sftp://a:22", 10, Duration::from_secs(1), true, 0, 100);
        assert_eq!(
            stats.feedback_at_with_timeout("sftp://a:22", 9, 10).samples,
            1
        );
        assert_eq!(
            stats.feedback_at_with_timeout("sftp://a:22", 10, 10),
            ServerFeedback::default()
        );
        assert_eq!(
            stats
                .feedback_at_with_timeout("sftp://a:22", 11, 100)
                .samples,
            1
        );
        stats.observe_with_timeout(
            "sftp://b:22",
            10,
            Duration::from_secs(1),
            true,
            Duration::ZERO,
        );
        assert_eq!(stats.len(), 1);
        assert_eq!(
            stats.feedback_with_timeout("sftp://a:22", Duration::ZERO),
            ServerFeedback::default()
        );
    }
    #[test]
    fn expires_before_lru_and_charges_and_refunds_entries() {
        let bytes = HttpIngressBudgets::new(1024);
        let stats = ServerStatistics::new(2, Duration::from_millis(10), bytes.clone());
        stats.observe_at("http://a:80", 100, Duration::from_secs(1), true, 0);
        stats.observe_at("http://b:80", 100, Duration::from_secs(1), true, 0);
        stats.feedback_at("http://a:80", 1);
        stats.observe_at("http://c:80", 50, Duration::from_secs(1), true, 1);
        assert_eq!(
            stats.feedback_at("http://b:80", 1),
            ServerFeedback::default()
        );
        assert_eq!(stats.len(), 2);
        assert!(bytes.used() > 0);
        assert_eq!(
            stats.feedback_at("http://a:80", 10),
            ServerFeedback::default()
        );
        assert_eq!(stats.len(), 1);
        drop(stats);
        assert_eq!(bytes.used(), 0);
        let denied = ServerStatistics::new(1, Duration::from_secs(1), HttpIngressBudgets::new(1));
        denied.observe("http://a:80", 1, Duration::from_secs(1), true);
        assert!(denied.is_empty());
        let bad = ServerStatistics::default();
        bad.observe("http://secret@a:80", 1, Duration::from_secs(1), true);
        assert!(bad.is_empty());
    }
    #[test]
    fn failed_samples_reduce_score_and_success_recovers_it() {
        let mut value = ServerFeedback::default();
        value.observe(1000, Duration::from_secs(1), true);
        let healthy = value.score(0);
        value.observe(0, Duration::from_secs(1), false);
        assert!(value.score(0) < healthy);
        value.observe(1000, Duration::from_secs(1), true);
        assert_eq!(value.score(0), healthy);
        assert_eq!(value.score(1), healthy / 2);
    }
}
