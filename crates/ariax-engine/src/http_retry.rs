//! Bounded, policy-driven per-range HTTP retry accounting and delay selection.

use ariax_core::UriId;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

pub const DEFAULT_HTTP_RETRY_MAX_ATTEMPTS: u32 = 5;
pub const DEFAULT_HTTP_RETRY_MAX_ATTEMPTS_PER_MIRROR: u32 = 3;
pub const DEFAULT_HTTP_RETRY_MAX_ELAPSED: Duration = Duration::from_secs(3600);
pub const DEFAULT_HTTP_RETRY_MAX_WAIT: Duration = Duration::from_secs(300);
pub const DEFAULT_HTTP_RETRY_BASE_WAIT: Duration = Duration::from_secs(1);
pub const MAX_HTTP_RETRY_STATUS_CODES: usize = 128;
pub const MAX_HTTP_RETRY_STATUS_SPEC_BYTES: usize = 4096;
pub const MAX_HTTP_RETRY_TRIGGER_SPEC_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpRetryProfile {
    Aria2,
    #[default]
    Conservative,
    Aggressive,
    Custom,
}

impl HttpRetryProfile {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Aria2 => "aria2",
            Self::Conservative => "conservative",
            Self::Aggressive => "aggressive",
            Self::Custom => "custom",
        }
    }

    pub fn parse(value: &str) -> Result<Self, HttpRetryError> {
        match value {
            "aria2" => Ok(Self::Aria2),
            "conservative" => Ok(Self::Conservative),
            "aggressive" => Ok(Self::Aggressive),
            "custom" => Ok(Self::Custom),
            _ => Err(HttpRetryError::InvalidProfile),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpRetryBackoff {
    Fixed,
    Exponential,
    #[default]
    ExponentialJitter,
}

impl HttpRetryBackoff {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::Exponential => "exponential",
            Self::ExponentialJitter => "exponential-jitter",
        }
    }

    pub fn parse(value: &str) -> Result<Self, HttpRetryError> {
        match value {
            "fixed" => Ok(Self::Fixed),
            "exponential" => Ok(Self::Exponential),
            "exponential-jitter" => Ok(Self::ExponentialJitter),
            _ => Err(HttpRetryError::InvalidPolicy),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpRetryAfterPolicy {
    #[default]
    Respect,
    Ignore,
}

impl HttpRetryAfterPolicy {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Respect => "respect",
            Self::Ignore => "ignore",
        }
    }

    pub fn parse(value: &str) -> Result<Self, HttpRetryError> {
        match value {
            "respect" => Ok(Self::Respect),
            "ignore" => Ok(Self::Ignore),
            _ => Err(HttpRetryError::InvalidPolicy),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpStaleValidatorPolicy {
    #[default]
    Fail,
    RestartIfSafe,
    Revalidate,
}

impl HttpStaleValidatorPolicy {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Fail => "fail",
            Self::RestartIfSafe => "restart-if-safe",
            Self::Revalidate => "revalidate",
        }
    }

    pub fn parse(value: &str) -> Result<Self, HttpRetryError> {
        match value {
            "fail" => Ok(Self::Fail),
            "restart-if-safe" => Ok(Self::RestartIfSafe),
            "revalidate" => Ok(Self::Revalidate),
            _ => Err(HttpRetryError::InvalidPolicy),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HttpRetryTrigger {
    Reset,
    Eof,
    Timeout,
    Hang,
    LowestSpeed,
    StaleConnection,
    DnsTransient,
    ProxyConnect,
}

impl HttpRetryTrigger {
    const ALL: [Self; 8] = [
        Self::Reset,
        Self::Eof,
        Self::Timeout,
        Self::Hang,
        Self::LowestSpeed,
        Self::StaleConnection,
        Self::DnsTransient,
        Self::ProxyConnect,
    ];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Reset => "reset",
            Self::Eof => "eof",
            Self::Timeout => "timeout",
            Self::Hang => "hang",
            Self::LowestSpeed => "lowest-speed",
            Self::StaleConnection => "stale-connection",
            Self::DnsTransient => "dns-transient",
            Self::ProxyConnect => "proxy-connect",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "reset" => Some(Self::Reset),
            "eof" => Some(Self::Eof),
            "timeout" => Some(Self::Timeout),
            "hang" => Some(Self::Hang),
            "lowest-speed" => Some(Self::LowestSpeed),
            "stale-connection" => Some(Self::StaleConnection),
            "dns-transient" => Some(Self::DnsTransient),
            "proxy-connect" => Some(Self::ProxyConnect),
            _ => None,
        }
    }

    const fn bit(self) -> u16 {
        match self {
            Self::Reset => 1 << 0,
            Self::Eof => 1 << 1,
            Self::Timeout => 1 << 2,
            Self::Hang => 1 << 3,
            Self::LowestSpeed => 1 << 4,
            Self::StaleConnection => 1 << 5,
            Self::DnsTransient => 1 << 6,
            Self::ProxyConnect => 1 << 7,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpRetryTriggerSet {
    bits: u16,
}

impl HttpRetryTriggerSet {
    #[must_use]
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    #[must_use]
    pub const fn all() -> Self {
        Self { bits: (1 << 8) - 1 }
    }

    #[must_use]
    pub const fn conservative() -> Self {
        Self::all()
    }

    pub fn parse(input: &str) -> Result<Self, HttpRetryError> {
        if input.is_empty() || input.len() > MAX_HTTP_RETRY_TRIGGER_SPEC_BYTES {
            return Err(HttpRetryError::InvalidTriggerSet);
        }
        let mut value = Self::empty();
        for part in input.split(',') {
            let trigger =
                HttpRetryTrigger::parse(part.trim()).ok_or(HttpRetryError::InvalidTriggerSet)?;
            value.insert(trigger);
        }
        if value.is_empty() {
            return Err(HttpRetryError::InvalidTriggerSet);
        }
        Ok(value)
    }

    #[must_use]
    pub const fn contains(self, trigger: HttpRetryTrigger) -> bool {
        self.bits & trigger.bit() != 0
    }

    pub fn insert(&mut self, trigger: HttpRetryTrigger) {
        self.bits |= trigger.bit();
    }

    pub fn remove(&mut self, trigger: HttpRetryTrigger) {
        self.bits &= !trigger.bit();
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.bits == 0
    }

    #[must_use]
    pub fn canonical(self) -> String {
        HttpRetryTrigger::ALL
            .into_iter()
            .filter(|trigger| self.contains(*trigger))
            .map(|trigger| trigger.code())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HttpRetryStatusSet {
    codes: BTreeSet<u16>,
}

impl HttpRetryStatusSet {
    pub fn from_codes(codes: impl IntoIterator<Item = u16>) -> Result<Self, HttpRetryError> {
        let mut value = Self::default();
        for code in codes {
            value.insert(code)?;
        }
        Ok(value)
    }

    #[must_use]
    pub fn aria2() -> Self {
        Self::from_codes([504]).expect("static aria2 status set is valid")
    }

    #[must_use]
    pub fn conservative() -> Self {
        Self::from_codes([408, 425, 429, 500, 502, 503, 504])
            .expect("static conservative status set is valid")
    }

    #[must_use]
    pub fn aggressive() -> Self {
        Self::from_codes([408, 421, 425, 429, 500, 502, 503, 504])
            .expect("static aggressive status set is valid")
    }

    pub fn parse(input: &str) -> Result<Self, HttpRetryError> {
        if input.is_empty() || input.len() > MAX_HTTP_RETRY_STATUS_SPEC_BYTES {
            return Err(HttpRetryError::InvalidStatusSet);
        }
        let mut value = Self::default();
        for part in input.split(',') {
            let part = part.trim();
            if part.is_empty() {
                return Err(HttpRetryError::InvalidStatusSet);
            }
            let mut bounds = part.split('-');
            let first = parse_status_code(bounds.next().ok_or(HttpRetryError::InvalidStatusSet)?)?;
            let Some(last) = bounds.next() else {
                value.insert(first)?;
                continue;
            };
            if bounds.next().is_some() {
                return Err(HttpRetryError::InvalidStatusSet);
            }
            let last = parse_status_code(last)?;
            if first > last {
                return Err(HttpRetryError::InvalidStatusSet);
            }
            for code in first..=last {
                value.insert(code)?;
            }
        }
        if value.is_empty() {
            return Err(HttpRetryError::InvalidStatusSet);
        }
        Ok(value)
    }

    pub fn insert(&mut self, code: u16) -> Result<(), HttpRetryError> {
        if !(100..=599).contains(&code)
            || (!self.codes.contains(&code) && self.codes.len() >= MAX_HTTP_RETRY_STATUS_CODES)
        {
            return Err(HttpRetryError::InvalidStatusSet);
        }
        self.codes.insert(code);
        Ok(())
    }

    pub fn remove(&mut self, code: u16) -> bool {
        self.codes.remove(&code)
    }

    #[must_use]
    pub fn contains(&self, code: u16) -> bool {
        self.codes.contains(&code)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.codes.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.codes.iter().copied()
    }

    #[must_use]
    pub fn canonical(&self) -> String {
        self.codes
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn parse_status_code(input: &str) -> Result<u16, HttpRetryError> {
    if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HttpRetryError::InvalidStatusSet);
    }
    let code = input
        .parse()
        .map_err(|_| HttpRetryError::InvalidStatusSet)?;
    if !(100..=599).contains(&code) {
        return Err(HttpRetryError::InvalidStatusSet);
    }
    Ok(code)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRetryPolicy {
    pub max_attempts: NonZeroU32,
    pub max_attempts_per_mirror: NonZeroU32,
    pub max_elapsed: Duration,
    pub base_wait: Duration,
    pub max_wait: Duration,
    pub retry_after_min: Duration,
    pub retry_after_max: Duration,
    pub respect_retry_after: bool,
    pub profile: HttpRetryProfile,
    pub backoff: HttpRetryBackoff,
    pub retry_on: HttpRetryTriggerSet,
    pub retryable_statuses: HttpRetryStatusSet,
    pub stale_validator_policy: HttpStaleValidatorPolicy,
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
            profile: HttpRetryProfile::Conservative,
            backoff: HttpRetryBackoff::ExponentialJitter,
            retry_on: HttpRetryTriggerSet::conservative(),
            retryable_statuses: HttpRetryStatusSet::conservative(),
            stale_validator_policy: HttpStaleValidatorPolicy::Fail,
        }
    }
}

impl HttpRetryPolicy {
    #[must_use]
    pub fn aria2() -> Self {
        Self {
            profile: HttpRetryProfile::Aria2,
            base_wait: Duration::ZERO,
            backoff: HttpRetryBackoff::Fixed,
            retryable_statuses: HttpRetryStatusSet::aria2(),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn conservative() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn aggressive() -> Self {
        Self {
            max_attempts: NonZeroU32::new(10).expect("aggressive attempt cap is nonzero"),
            max_attempts_per_mirror: NonZeroU32::new(5).expect("aggressive mirror cap is nonzero"),
            max_elapsed: Duration::from_secs(7200),
            max_wait: Duration::from_secs(600),
            retry_after_max: Duration::from_secs(600),
            profile: HttpRetryProfile::Aggressive,
            retryable_statuses: HttpRetryStatusSet::aggressive(),
            ..Self::default()
        }
    }

    pub fn custom(
        retry_on: HttpRetryTriggerSet,
        retryable_statuses: HttpRetryStatusSet,
    ) -> Result<Self, HttpRetryError> {
        let policy = Self {
            profile: HttpRetryProfile::Custom,
            retry_on,
            retryable_statuses,
            ..Self::default()
        };
        policy.validate()?;
        Ok(policy)
    }

    #[must_use]
    pub fn from_profile(profile: HttpRetryProfile) -> Self {
        match profile {
            HttpRetryProfile::Aria2 => Self::aria2(),
            HttpRetryProfile::Conservative => Self::conservative(),
            HttpRetryProfile::Aggressive => Self::aggressive(),
            HttpRetryProfile::Custom => Self {
                profile,
                retry_on: HttpRetryTriggerSet::empty(),
                retryable_statuses: HttpRetryStatusSet::default(),
                ..Self::default()
            },
        }
    }

    pub fn validate(&self) -> Result<(), HttpRetryError> {
        if self.max_attempts_per_mirror > self.max_attempts
            || self.max_elapsed.is_zero()
            || self.max_wait.is_zero()
            || self.retry_after_max > self.max_wait
            || self.retry_after_min > self.retry_after_max
            || (matches!(self.profile, HttpRetryProfile::Custom)
                && (self.retry_on.is_empty() || self.retryable_statuses.is_empty()))
        {
            return Err(HttpRetryError::InvalidPolicy);
        }
        Ok(())
    }

    #[must_use]
    pub const fn retry_after_policy(&self) -> HttpRetryAfterPolicy {
        if self.respect_retry_after {
            HttpRetryAfterPolicy::Respect
        } else {
            HttpRetryAfterPolicy::Ignore
        }
    }

    #[must_use]
    pub fn is_retriable(&self, cause: HttpRetryCause) -> bool {
        match cause {
            HttpRetryCause::Transport(failure) => self.retry_on.contains(failure.trigger()),
            HttpRetryCause::HttpStatus(status) => {
                self.retryable_statuses.contains(status)
                    || (matches!(self.profile, HttpRetryProfile::Aria2)
                        && !self.base_wait.is_zero()
                        && matches!(status, 502 | 503))
            }
            HttpRetryCause::Authentication
            | HttpRetryCause::InvalidRange
            | HttpRetryCause::StaleValidator
            | HttpRetryCause::Checksum
            | HttpRetryCause::Storage
            | HttpRetryCause::Cancelled
            | HttpRetryCause::Policy => false,
        }
    }

    fn effective_base_wait(&self) -> Duration {
        if self.base_wait.is_zero() && !matches!(self.profile, HttpRetryProfile::Aria2) {
            DEFAULT_HTTP_RETRY_BASE_WAIT
        } else {
            self.base_wait
        }
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

impl HttpRetryTransportFailure {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Reset => "reset",
            Self::UnexpectedEof => "eof",
            Self::Timeout => "timeout",
            Self::Hang => "hang",
            Self::LowestSpeed => "lowest-speed",
            Self::StaleConnection => "stale-connection",
            Self::DnsTransient => "dns-transient",
            Self::ProxyConnect => "proxy-connect",
        }
    }

    const fn trigger(self) -> HttpRetryTrigger {
        match self {
            Self::Reset => HttpRetryTrigger::Reset,
            Self::UnexpectedEof => HttpRetryTrigger::Eof,
            Self::Timeout => HttpRetryTrigger::Timeout,
            Self::Hang => HttpRetryTrigger::Hang,
            Self::LowestSpeed => HttpRetryTrigger::LowestSpeed,
            Self::StaleConnection => HttpRetryTrigger::StaleConnection,
            Self::DnsTransient => HttpRetryTrigger::DnsTransient,
            Self::ProxyConnect => HttpRetryTrigger::ProxyConnect,
        }
    }
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
    pub const fn code(self) -> &'static str {
        match self {
            Self::Transport(failure) => failure.code(),
            Self::HttpStatus(_) => "http-status",
            Self::Authentication => "authentication",
            Self::InvalidRange => "invalid-range",
            Self::StaleValidator => "stale-validator",
            Self::Checksum => "checksum",
            Self::Storage => "storage",
            Self::Cancelled => "cancelled",
            Self::Policy => "policy",
        }
    }

    #[must_use]
    pub const fn http_status(self) -> Option<u16> {
        match self {
            Self::HttpStatus(status) => Some(status),
            _ => None,
        }
    }

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
    FixedBackoff,
    ExponentialBackoff,
    EqualJitterBackoff,
    RetryAfter,
    RetryAfterClamped,
    RetryAfterIgnored,
    BackoffAfterInvalidRetryAfter,
}

impl HttpRetryDelaySource {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::FixedBackoff => "fixed-backoff",
            Self::ExponentialBackoff => "exponential-backoff",
            Self::EqualJitterBackoff => "equal-jitter-backoff",
            Self::RetryAfter => "retry-after",
            Self::RetryAfterClamped => "retry-after-clamped",
            Self::RetryAfterIgnored => "retry-after-ignored",
            Self::BackoffAfterInvalidRetryAfter => "retry-after-invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRetryStopReason {
    NonRetriable,
    TotalAttemptCap,
    MirrorAttemptCap,
    ElapsedCap,
}

impl HttpRetryStopReason {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::NonRetriable => "non-retriable",
            Self::TotalAttemptCap => "total-attempt-cap",
            Self::MirrorAttemptCap => "mirror-attempt-cap",
            Self::ElapsedCap => "elapsed-cap",
        }
    }
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
    InvalidProfile,
    InvalidTriggerSet,
    InvalidStatusSet,
    InvalidRecoveredState,
    AttemptCap,
    FailureWithoutAttempt,
}

impl HttpRetryError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPolicy => "invalid_retry_policy",
            Self::InvalidProfile => "invalid_retry_profile",
            Self::InvalidTriggerSet => "invalid_retry_trigger_set",
            Self::InvalidStatusSet => "invalid_retry_status_set",
            Self::InvalidRecoveredState => "invalid_recovered_retry_state",
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
        policy.validate()?;
        Ok(Self {
            policy,
            attempts: 0,
            attempts_by_mirror: BTreeMap::new(),
        })
    }

    #[must_use]
    pub fn policy(&self) -> &HttpRetryPolicy {
        &self.policy
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

    pub fn restore_attempts(
        &mut self,
        attempts: u32,
        attempts_by_mirror: BTreeMap<UriId, u32>,
    ) -> Result<(), HttpRetryError> {
        if self.attempts != 0
            || !self.attempts_by_mirror.is_empty()
            || attempts == 0
            || attempts > self.policy.max_attempts.get()
            || attempts_by_mirror.values().any(|attempts| {
                *attempts == 0 || *attempts > self.policy.max_attempts_per_mirror.get()
            })
            || attempts_by_mirror
                .values()
                .copied()
                .try_fold(0_u32, u32::checked_add)
                != Some(attempts)
        {
            return Err(HttpRetryError::InvalidRecoveredState);
        }
        self.attempts = attempts;
        self.attempts_by_mirror = attempts_by_mirror;
        Ok(())
    }

    #[must_use]
    pub fn attempts_for_mirror(&self, mirror: UriId) -> u32 {
        self.attempts_by_mirror.get(&mirror).copied().unwrap_or(0)
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
        if !self.policy.is_retriable(cause) {
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

        let (delay, source) = match retry_after {
            Some(value) if self.policy.respect_retry_after => match parse_retry_after(value, now) {
                Some(parsed) => {
                    let capped = parsed
                        .max(self.policy.retry_after_min)
                        .min(self.policy.retry_after_max)
                        .min(self.policy.max_wait);
                    let clamped = capped != parsed;
                    (
                        self.retry_after_delay(capped, clamped, entropy),
                        if clamped {
                            HttpRetryDelaySource::RetryAfterClamped
                        } else {
                            HttpRetryDelaySource::RetryAfter
                        },
                    )
                }
                None => {
                    let cap = self.backoff_cap();
                    (
                        self.backoff_delay(cap, entropy),
                        HttpRetryDelaySource::BackoffAfterInvalidRetryAfter,
                    )
                }
            },
            Some(_) => {
                let cap = self.backoff_cap();
                (
                    self.backoff_delay(cap, entropy),
                    HttpRetryDelaySource::RetryAfterIgnored,
                )
            }
            None => {
                let cap = self.backoff_cap();
                (self.backoff_delay(cap, entropy), self.backoff_source())
            }
        };
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
        let base = self.policy.effective_base_wait();
        if matches!(self.policy.backoff, HttpRetryBackoff::Fixed) {
            return base.min(self.policy.max_wait);
        }
        let retry_ordinal = self.attempts.max(1);
        let shift = retry_ordinal.saturating_sub(1).min(63);
        base.saturating_mul(1_u32.checked_shl(shift).unwrap_or(u32::MAX))
            .min(self.policy.max_wait)
    }

    fn backoff_source(&self) -> HttpRetryDelaySource {
        match self.policy.backoff {
            HttpRetryBackoff::Fixed => HttpRetryDelaySource::FixedBackoff,
            HttpRetryBackoff::Exponential => HttpRetryDelaySource::ExponentialBackoff,
            HttpRetryBackoff::ExponentialJitter => HttpRetryDelaySource::EqualJitterBackoff,
        }
    }

    fn backoff_delay(&self, cap: Duration, entropy: u64) -> Duration {
        match self.policy.backoff {
            HttpRetryBackoff::Fixed | HttpRetryBackoff::Exponential => cap,
            HttpRetryBackoff::ExponentialJitter => equal_jitter(cap, entropy),
        }
    }

    fn retry_after_delay(&self, cap: Duration, clamped: bool, entropy: u64) -> Duration {
        if matches!(self.policy.backoff, HttpRetryBackoff::Fixed) && !clamped {
            cap
        } else {
            equal_jitter(cap, entropy)
        }
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
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(429),
                    Duration::ZERO,
                    Some("9999"),
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Retry {
                delay: Duration::from_secs(150),
                source: HttpRetryDelaySource::RetryAfterClamped,
            }
        );
        assert!(matches!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(503),
                    Duration::ZERO,
                    Some("Thu, 01 Jan 1970 00:00:10 GMT"),
                    SystemTime::UNIX_EPOCH,
                    5_000,
                )
                .expect("date decision"),
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
    }

    #[test]
    fn profiles_and_custom_sets_are_resolved_and_bounded() {
        let conservative = HttpRetryPolicy::conservative();
        assert_eq!(conservative.max_attempts.get(), 5);
        assert_eq!(conservative.max_attempts_per_mirror.get(), 3);
        assert_eq!(conservative.backoff, HttpRetryBackoff::ExponentialJitter);
        let aggressive = HttpRetryPolicy::aggressive();
        assert_eq!(aggressive.max_attempts.get(), 10);
        assert_eq!(aggressive.max_wait, Duration::from_secs(600));
        assert!(aggressive.is_retriable(HttpRetryCause::HttpStatus(421)));
        assert_eq!(
            HttpRetryPolicy::custom(
                HttpRetryTriggerSet::empty(),
                HttpRetryStatusSet::parse("429").expect("status"),
            ),
            Err(HttpRetryError::InvalidPolicy)
        );
        assert_eq!(
            HttpRetryStatusSet::parse("408, 502-504")
                .expect("status set")
                .canonical(),
            "408,502,503,504"
        );
        assert!(HttpRetryStatusSet::parse("99").is_err());
        assert!(HttpRetryTriggerSet::parse("reset,nope").is_err());
    }

    #[test]
    fn aria2_zero_wait_is_immediate_and_fixed_exact_retry_after_is_not_jittered() {
        let mut aria2 = HttpRetryBudget::new(HttpRetryPolicy::aria2()).expect("aria2");
        aria2.begin_attempt(mirror(0)).expect("initial");
        assert_eq!(
            aria2
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::Transport(HttpRetryTransportFailure::Timeout),
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Retry {
                delay: Duration::ZERO,
                source: HttpRetryDelaySource::FixedBackoff,
            }
        );
        assert_eq!(
            aria2
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(502),
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Stop(HttpRetryStopReason::NonRetriable)
        );

        let policy = HttpRetryPolicy {
            backoff: HttpRetryBackoff::Fixed,
            ..HttpRetryPolicy::default()
        };
        let mut fixed = HttpRetryBudget::new(policy).expect("fixed");
        fixed.begin_attempt(mirror(1)).expect("initial");
        assert_eq!(
            fixed
                .decide_after_failure(
                    mirror(1),
                    HttpRetryCause::HttpStatus(429),
                    Duration::ZERO,
                    Some("10"),
                    SystemTime::UNIX_EPOCH,
                    9_999,
                )
                .expect("decision"),
            HttpRetryDecision::Retry {
                delay: Duration::from_secs(10),
                source: HttpRetryDelaySource::RetryAfter,
            }
        );
    }

    #[test]
    fn retry_status_policy_and_trigger_set_are_authoritative() {
        let mut policy = HttpRetryPolicy::custom(
            HttpRetryTriggerSet::parse("timeout").expect("triggers"),
            HttpRetryStatusSet::parse("418").expect("statuses"),
        )
        .expect("custom");
        policy.backoff = HttpRetryBackoff::Fixed;
        policy.base_wait = Duration::from_secs(2);
        let mut budget = HttpRetryBudget::new(policy).expect("budget");
        budget.begin_attempt(mirror(0)).expect("initial");
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
            HttpRetryDecision::Stop(HttpRetryStopReason::NonRetriable)
        );
        assert_eq!(
            budget
                .decide_after_failure(
                    mirror(0),
                    HttpRetryCause::HttpStatus(418),
                    Duration::ZERO,
                    None,
                    SystemTime::UNIX_EPOCH,
                    0,
                )
                .expect("decision"),
            HttpRetryDecision::Retry {
                delay: Duration::from_secs(2),
                source: HttpRetryDelaySource::FixedBackoff,
            }
        );
    }

    #[test]
    fn restored_attempts_preserve_total_and_per_mirror_caps() {
        let mut budget = HttpRetryBudget::new(HttpRetryPolicy::default()).expect("budget");
        let attempts = BTreeMap::from([(mirror(0), 2), (mirror(1), 1)]);
        budget
            .restore_attempts(3, attempts)
            .expect("restored attempts");
        assert_eq!(budget.stats().attempts, 3);
        assert_eq!(budget.attempts_for_mirror(mirror(0)), 2);
        assert_eq!(budget.attempts_for_mirror(mirror(1)), 1);
        assert_eq!(
            HttpRetryBudget::new(HttpRetryPolicy::default())
                .expect("new budget")
                .restore_attempts(3, BTreeMap::from([(mirror(0), 2)])),
            Err(HttpRetryError::InvalidRecoveredState)
        );
    }
}
