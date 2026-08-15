//! Conservative per-range HTTP retry accounting and bounded delay selection.

use ariax_core::UriId;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

pub const DEFAULT_HTTP_RETRY_MAX_ATTEMPTS: u32 = 5;
pub const DEFAULT_HTTP_RETRY_MAX_ATTEMPTS_PER_MIRROR: u32 = 3;
pub const DEFAULT_HTTP_RETRY_MAX_ELAPSED: Duration = Duration::from_secs(3600);
pub const DEFAULT_HTTP_RETRY_MAX_WAIT: Duration = Duration::from_secs(300);
pub const DEFAULT_HTTP_RETRY_BASE_WAIT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRetryPolicy {
    pub max_attempts: NonZeroU32,
    pub max_attempts_per_mirror: NonZeroU32,
    pub max_elapsed: Duration,
    pub base_wait: Duration,
    pub max_wait: Duration,
    pub retry_after_min: Duration,
    pub retry_after_max: Duration,
    pub respect_retry_after: bool,
}

impl Default for HttpRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(DEFAULT_HTTP_RETRY_MAX_ATTEMPTS)
                .expect("default attempt cap is nonzero"),
            max_attempts_per_mirror: NonZeroU32::new(DEFAULT_HTTP_RETRY_MAX_ATTEMPTS_PER_MIRROR)
                .expect("default mirror cap is nonzero"),
            max_elapsed: DEFAULT_HTTP_RETRY_MAX_ELAPSED,
            base_wait: DEFAULT_HTTP_RETRY_BASE_WAIT,
            max_wait: DEFAULT_HTTP_RETRY_MAX_WAIT,
            retry_after_min: Duration::ZERO,
            retry_after_max: DEFAULT_HTTP_RETRY_MAX_WAIT,
            respect_retry_after: true,
        }
    }
}

impl HttpRetryPolicy {
    fn validate(self) -> Result<Self, HttpRetryError> {
        if self.max_attempts_per_mirror > self.max_attempts
            || self.max_elapsed.is_zero()
            || self.base_wait.is_zero()
            || self.max_wait.is_zero()
            || self.retry_after_max > self.max_wait
            || self.retry_after_min > self.retry_after_max
        {
            return Err(HttpRetryError::InvalidPolicy);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryTransportFailure {
    Reset,
    UnexpectedEof,
    Timeout,
    Hang,
    LowestSpeed,
    StaleConnection,
    DnsTransient,
    ProxyConnect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryCause {
    Transport(HttpRetryTransportFailure),
    HttpStatus(u16),
    Authentication,
    InvalidRange,
    StaleValidator,
    Checksum,
    Storage,
    Cancelled,
    Policy,
}

impl HttpRetryCause {
    #[must_use]
    pub const fn retriable(self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::HttpStatus(status) => {
                matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
            }
            Self::Authentication
            | Self::InvalidRange
            | Self::StaleValidator
            | Self::Checksum
            | Self::Storage
            | Self::Cancelled
            | Self::Policy => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryDelaySource {
    EqualJitterBackoff,
    RetryAfter,
    BackoffAfterInvalidRetryAfter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryStopReason {
    NonRetriable,
    TotalAttemptCap,
    MirrorAttemptCap,
    ElapsedCap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryDecision {
    Retry {
        delay: Duration,
        source: HttpRetryDelaySource,
    },
    Stop(HttpRetryStopReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRetryStats {
    pub attempts: u32,
    pub retries_started: u32,
    pub mirrors_tried: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryError {
    InvalidPolicy,
    AttemptCap,
    FailureWithoutAttempt,
}

impl HttpRetryError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPolicy => "invalid_retry_policy",
            Self::AttemptCap => "retry_attempt_cap",
            Self::FailureWithoutAttempt => "retry_failure_without_attempt",
        }
    }
}

impl fmt::Display for HttpRetryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpRetryError {}

#[derive(Clone, Debug)]
pub struct HttpRetryBudget {
    policy: HttpRetryPolicy,
    attempts: u32,
    attempts_by_mirror: BTreeMap<UriId, u32>,
}

impl HttpRetryBudget {
    pub fn new(policy: HttpRetryPolicy) -> Result<Self, HttpRetryError> {
        Ok(Self {
            policy: policy.validate()?,
            attempts: 0,
            attempts_by_mirror: BTreeMap::new(),
        })
    }

    pub fn begin_attempt(&mut self, mirror: UriId) -> Result<u32, HttpRetryError> {
        let mirror_attempts = self.attempts_by_mirror.get(&mirror).copied().unwrap_or(0);
        if self.attempts >= self.policy.max_attempts.get()
            || mirror_attempts >= self.policy.max_attempts_per_mirror.get()
        {
            return Err(HttpRetryError::AttemptCap);
        }
        self.attempts += 1;
        *self.attempts_by_mirror.entry(mirror).or_default() += 1;
        Ok(self.attempts)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn decide_after_failure(
        &self,
        mirror: UriId,
        cause: HttpRetryCause,
        active_elapsed: Duration,
        retry_after: Option<&str>,
        now: SystemTime,
        entropy: u64,
    ) -> Result<HttpRetryDecision, HttpRetryError> {
        let mirror_attempts = self.attempts_by_mirror.get(&mirror).copied().unwrap_or(0);
        if mirror_attempts == 0 || self.attempts == 0 {
            return Err(HttpRetryError::FailureWithoutAttempt);
        }
        if !cause.retriable() {
            return Ok(HttpRetryDecision::Stop(HttpRetryStopReason::NonRetriable));
        }
        if self.attempts >= self.policy.max_attempts.get() {
            return Ok(HttpRetryDecision::Stop(
                HttpRetryStopReason::TotalAttemptCap,
            ));
        }
        if mirror_attempts >= self.policy.max_attempts_per_mirror.get() {
            return Ok(HttpRetryDecision::Stop(
                HttpRetryStopReason::MirrorAttemptCap,
            ));
        }
        if active_elapsed >= self.policy.max_elapsed {
            return Ok(HttpRetryDecision::Stop(HttpRetryStopReason::ElapsedCap));
        }

        let (cap, source) = if let Some(value) = retry_after
            && self.policy.respect_retry_after
        {
            match parse_retry_after(value, now) {
                Some(delay) => (
                    delay
                        .max(self.policy.retry_after_min)
                        .min(self.policy.retry_after_max)
                        .min(self.policy.max_wait),
                    HttpRetryDelaySource::RetryAfter,
                ),
                None => (
                    self.backoff_cap(),
                    HttpRetryDelaySource::BackoffAfterInvalidRetryAfter,
                ),
            }
        } else {
            (self.backoff_cap(), HttpRetryDelaySource::EqualJitterBackoff)
        };
        let delay = equal_jitter(cap, entropy);
        if active_elapsed.saturating_add(delay) > self.policy.max_elapsed {
            return Ok(HttpRetryDecision::Stop(HttpRetryStopReason::ElapsedCap));
        }
        Ok(HttpRetryDecision::Retry { delay, source })
    }

    #[must_use]
    pub fn stats(&self) -> HttpRetryStats {
        HttpRetryStats {
            attempts: self.attempts,
            retries_started: self.attempts.saturating_sub(1),
            mirrors_tried: self.attempts_by_mirror.len(),
        }
    }

    fn backoff_cap(&self) -> Duration {
        let retry_ordinal = self.attempts.max(1);
        let shift = retry_ordinal.saturating_sub(1).min(63);
        self.policy
            .base_wait
            .saturating_mul(1_u32.checked_shl(shift).unwrap_or(u32::MAX))
            .min(self.policy.max_wait)
    }
}

fn parse_retry_after(input: &str, now: SystemTime) -> Option<Duration> {
    if input.is_empty() || input.len() > 128 || input.trim() != input {
        return None;
    }
    if input.bytes().all(|byte| byte.is_ascii_digit()) {
        return input.parse::<u64>().ok().map(Duration::from_secs);
    }
    let deadline = httpdate::parse_http_date(input).ok()?;
    Some(deadline.duration_since(now).unwrap_or(Duration::ZERO))
}

fn equal_jitter(cap: Duration, entropy: u64) -> Duration {
    let cap_ms = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX);
    if cap_ms == 0 {
        return Duration::ZERO;
    }
    let lower = cap_ms / 2;
    let width = cap_ms - lower;
    Duration::from_millis(lower.saturating_add(entropy % width.saturating_add(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mirror(value: u32) -> UriId {
        UriId::new(value)
    }

    #[test]
    fn conservative_caps_include_initial_attempt_and_use_equal_jitter() {
        let mut budget = HttpRetryBudget::new(HttpRetryPolicy::default()).expect("budget");
        assert_eq!(budget.begin_attempt(mirror(0)).expect("initial"), 1);
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::Transport(HttpRetryTransportFailure::Reset),
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Retry {
                delay: Duration::from_millis(500),
                source: HttpRetryDelaySource::EqualJitterBackoff,
            }
        );
        assert_eq!(budget.begin_attempt(mirror(0)).expect("retry"), 2);
        assert_eq!(budget.begin_attempt(mirror(0)).expect("retry"), 3);
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(503),
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    1,
                )
                .expect("decision"),
            HttpRetryDecision::Stop(HttpRetryStopReason::MirrorAttemptCap)
        );
    }

    #[test]
    fn retry_after_accepts_delta_and_http_date_then_clamps_and_jitters() {
        let mut budget = HttpRetryBudget::new(HttpRetryPolicy::default()).expect("budget");
        budget.begin_attempt(mirror(0)).expect("initial");
        let decision = budget
            .decide_after_failure(
                mirror(0),
                HttpRetryCause::HttpStatus(429),
                Duration::ZERO,
                Some("9999"),
                SystemTime::UNIX_EPOCH,
                0,
            )
            .expect("decision");
        assert_eq!(
            decision,
            HttpRetryDecision::Retry {
                delay: Duration::from_secs(150),
                source: HttpRetryDelaySource::RetryAfter,
            }
        );
        let decision = budget
            .decide_after_failure(
                mirror(0),
                HttpRetryCause::HttpStatus(503),
                Duration::ZERO,
                Some("Thu, 01 Jan 1970 00:00:10 GMT"),
                SystemTime::UNIX_EPOCH,
                5_000,
            )
            .expect("date decision");
        assert!(matches!(
            decision,
            HttpRetryDecision::Retry {
                delay,
                source: HttpRetryDelaySource::RetryAfter,
            } if (Duration::from_secs(5)..=Duration::from_secs(10)).contains(&delay)
        ));
    }

    #[test]
    fn invalid_retry_after_falls_back_and_terminal_causes_never_retry() {
        let mut budget = HttpRetryBudget::new(HttpRetryPolicy::default()).expect("budget");
        budget.begin_attempt(mirror(0)).expect("initial");
        assert!(matches!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(500),
                    Duration::ZERO,
                    Some("-1"),
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Retry {
                source: HttpRetryDelaySource::BackoffAfterInvalidRetryAfter,
                ..
            }
        ));
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::InvalidRange,
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Stop(HttpRetryStopReason::NonRetriable)
        );
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(404),
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Stop(HttpRetryStopReason::NonRetriable)
        );
    }

    #[test]
    fn elapsed_and_total_caps_stop_without_admitting_an_extra_attempt() {
        let policy = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(2).expect("nonzero"),
            max_attempts_per_mirror: NonZeroU32::new(2).expect("nonzero"),
            ..HttpRetryPolicy::default()
        };
        let mut budget = HttpRetryBudget::new(policy).expect("budget");
        budget.begin_attempt(mirror(0)).expect("initial");
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(500),
                    DEFAULT_HTTP_RETRY_MAX_ELAPSED,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Stop(HttpRetryStopReason::ElapsedCap)
        );
        budget.begin_attempt(mirror(0)).expect("last attempt");
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(500),
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Stop(HttpRetryStopReason::TotalAttemptCap)
        );
        assert_eq!(budget.stats().attempts, 2);
    }
}
