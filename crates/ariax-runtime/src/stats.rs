use ariax_core::MonotonicInstant;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::time::Duration;

pub const MIN_STATS_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_STATS_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
pub const MAX_STATS_ACTIVE_ENTRIES: usize = 100_000;

/// Profile-owned packet-independent sampling cadence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatsProfile {
    Concurrency,
    Throughput,
    Latency,
    Compact,
}

impl StatsProfile {
    pub const ALL: [Self; 4] = [
        Self::Concurrency,
        Self::Throughput,
        Self::Latency,
        Self::Compact,
    ];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Concurrency => "concurrency",
            Self::Throughput => "throughput",
            Self::Latency => "latency",
            Self::Compact => "compact",
        }
    }

    #[must_use]
    pub const fn interval(self) -> Duration {
        match self {
            Self::Concurrency | Self::Compact => Duration::from_secs(1),
            Self::Throughput => Duration::from_millis(500),
            Self::Latency => Duration::from_millis(250),
        }
    }
}

/// Validated sampler interval and active-entry bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatsSamplerConfig {
    interval: Duration,
    max_active: NonZeroUsize,
}

impl StatsSamplerConfig {
    #[must_use]
    pub fn for_profile(profile: StatsProfile, max_active: NonZeroUsize) -> Self {
        Self {
            interval: profile.interval(),
            max_active,
        }
    }

    #[must_use]
    pub fn with_override(
        profile: StatsProfile,
        max_active: NonZeroUsize,
        requested: Option<Duration>,
    ) -> Self {
        let interval = requested.map_or_else(
            || profile.interval(),
            |requested| requested.clamp(MIN_STATS_SAMPLE_INTERVAL, MAX_STATS_SAMPLE_INTERVAL),
        );
        Self {
            interval,
            max_active,
        }
    }

    #[must_use]
    pub const fn interval(self) -> Duration {
        self.interval
    }

    #[must_use]
    pub const fn max_active(self) -> NonZeroUsize {
        self.max_active
    }
}

/// Diagnostic condition independent of the task lifecycle state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConnectionCondition {
    #[default]
    None,
    Idle,
    Stalled,
    Backpressured,
    RateLimited,
}

impl ConnectionCondition {
    pub const ALL: [Self; 5] = [
        Self::None,
        Self::Idle,
        Self::Stalled,
        Self::Backpressured,
        Self::RateLimited,
    ];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Idle => "idle",
            Self::Stalled => "stalled",
            Self::Backpressured => "backpressured",
            Self::RateLimited => "rate_limited",
        }
    }
}

/// Closed, non-secret diagnostic reason vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionConditionReason {
    ConnectTimeout,
    HandshakeTimeout,
    FirstByteTimeout,
    BetweenBytesTimeout,
    LowestSpeed,
    DiskWriteTimeout,
    DiskBackpressure,
    BufferBackpressure,
    CpuBackpressure,
    JournalBackpressure,
    IngressRateLimit,
}

impl ConnectionConditionReason {
    pub const ALL: [Self; 11] = [
        Self::ConnectTimeout,
        Self::HandshakeTimeout,
        Self::FirstByteTimeout,
        Self::BetweenBytesTimeout,
        Self::LowestSpeed,
        Self::DiskWriteTimeout,
        Self::DiskBackpressure,
        Self::BufferBackpressure,
        Self::CpuBackpressure,
        Self::JournalBackpressure,
        Self::IngressRateLimit,
    ];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ConnectTimeout => "connect_timeout",
            Self::HandshakeTimeout => "handshake_timeout",
            Self::FirstByteTimeout => "first_byte_timeout",
            Self::BetweenBytesTimeout => "between_bytes_timeout",
            Self::LowestSpeed => "lowest_speed",
            Self::DiskWriteTimeout => "disk_write_timeout",
            Self::DiskBackpressure => "disk_backpressure",
            Self::BufferBackpressure => "buffer_backpressure",
            Self::CpuBackpressure => "cpu_backpressure",
            Self::JournalBackpressure => "journal_backpressure",
            Self::IngressRateLimit => "ingress_rate_limit",
        }
    }

    const fn condition(self) -> ConnectionCondition {
        match self {
            Self::ConnectTimeout
            | Self::HandshakeTimeout
            | Self::FirstByteTimeout
            | Self::BetweenBytesTimeout
            | Self::LowestSpeed
            | Self::DiskWriteTimeout => ConnectionCondition::Stalled,
            Self::DiskBackpressure
            | Self::BufferBackpressure
            | Self::CpuBackpressure
            | Self::JournalBackpressure => ConnectionCondition::Backpressured,
            Self::IngressRateLimit => ConnectionCondition::RateLimited,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsDiagnostic {
    pub condition: ConnectionCondition,
    pub reason: Option<ConnectionConditionReason>,
}

/// One caller-owned cumulative counter observation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsCounters {
    pub received_payload_bytes: u64,
    pub accepted_bytes: u64,
    pub submitted_bytes: u64,
    pub provisional_in_flight_bytes: u64,
    pub committed_bytes: u64,
    pub durable_bytes: u64,
    pub discarded_bytes: u64,
    pub discard_budget_consumed: u64,
    pub discard_budget_remaining: u64,
    pub last_byte_arrival_at: Option<MonotonicInstant>,
    pub last_committed_progress_at: Option<MonotonicInstant>,
    pub last_successful_write_at: Option<MonotonicInstant>,
    pub last_progress_at: Option<MonotonicInstant>,
}

/// Why a sampler mutation was rejected without changing its baseline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatsSamplerError {
    CapacityTooLarge { requested: usize, maximum: usize },
    Full,
    DuplicateEntry,
    UnknownEntry,
    CounterRegression(&'static str),
    TimestampRegression(&'static str),
    InvalidCounterRelation(&'static str),
    InvalidDiagnostic,
    AllocationFailed,
    ClockExhausted,
}

/// Immutable sampled rates plus the exact cumulative observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsSample<K> {
    pub key: K,
    pub sampled_at: MonotonicInstant,
    pub counters: StatsCounters,
    pub diagnostic: StatsDiagnostic,
    pub current_speed: u64,
    pub wire_speed: u64,
    pub useful_speed: u64,
    pub durable_speed: u64,
    pub average_speed: u64,
    pub smoothed_speed: u64,
}

impl<K> StatsSample<K> {
    #[must_use]
    pub fn sample_age(&self, now: MonotonicInstant) -> Duration {
        now.duration_since(self.sampled_at)
    }
}

#[derive(Debug)]
struct StatsEntry {
    started_at: MonotonicInstant,
    started_received: u64,
    previous_sample_at: MonotonicInstant,
    previous: StatsCounters,
    current: StatsCounters,
    diagnostic: StatsDiagnostic,
    smoothed_speed: Option<u64>,
}

/// Bounded lane-local sampler. Idle subjects leave the map instead of being
/// scanned forever; registered active subjects are sampled independently of
/// packet arrival.
#[derive(Debug)]
pub struct StatsSampler<K> {
    config: StatsSamplerConfig,
    entries: BTreeMap<K, StatsEntry>,
    next_sample_at: Option<MonotonicInstant>,
}

impl<K: Clone + Ord> StatsSampler<K> {
    pub fn new(config: StatsSamplerConfig) -> Result<Self, StatsSamplerError> {
        if config.max_active.get() > MAX_STATS_ACTIVE_ENTRIES {
            return Err(StatsSamplerError::CapacityTooLarge {
                requested: config.max_active.get(),
                maximum: MAX_STATS_ACTIVE_ENTRIES,
            });
        }
        Ok(Self {
            config,
            entries: BTreeMap::new(),
            next_sample_at: None,
        })
    }

    #[must_use]
    pub const fn config(&self) -> StatsSamplerConfig {
        self.config
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn register(
        &mut self,
        key: K,
        counters: StatsCounters,
        diagnostic: StatsDiagnostic,
        at: MonotonicInstant,
    ) -> Result<(), StatsSamplerError> {
        validate_counters(counters)?;
        validate_diagnostic(diagnostic)?;
        if self.entries.contains_key(&key) {
            return Err(StatsSamplerError::DuplicateEntry);
        }
        if self.entries.len() == self.config.max_active.get() {
            return Err(StatsSamplerError::Full);
        }
        let next = at
            .checked_add(self.config.interval)
            .ok_or(StatsSamplerError::ClockExhausted)?;
        self.entries.insert(
            key,
            StatsEntry {
                started_at: at,
                started_received: counters.received_payload_bytes,
                previous_sample_at: at,
                previous: counters,
                current: counters,
                diagnostic,
                smoothed_speed: None,
            },
        );
        self.next_sample_at = Some(
            self.next_sample_at
                .map_or(next, |current| current.min(next)),
        );
        Ok(())
    }

    pub fn update(
        &mut self,
        key: &K,
        counters: StatsCounters,
        diagnostic: StatsDiagnostic,
    ) -> Result<(), StatsSamplerError> {
        validate_counters(counters)?;
        validate_diagnostic(diagnostic)?;
        let entry = self
            .entries
            .get_mut(key)
            .ok_or(StatsSamplerError::UnknownEntry)?;
        validate_counter_update(entry.current, counters)?;
        entry.current = counters;
        entry.diagnostic = diagnostic;
        Ok(())
    }

    pub fn unregister(&mut self, key: &K) -> Result<(), StatsSamplerError> {
        if self.entries.remove(key).is_none() {
            return Err(StatsSamplerError::UnknownEntry);
        }
        if self.entries.is_empty() {
            self.next_sample_at = None;
        }
        Ok(())
    }

    /// Samples every active entry when the shared cadence is due. `None`
    /// means callers can return to their lane without publishing anything.
    pub fn sample_at(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<Option<Vec<StatsSample<K>>>, StatsSamplerError> {
        let Some(next_sample_at) = self.next_sample_at else {
            return Ok(None);
        };
        if now < next_sample_at {
            return Ok(None);
        }
        self.next_sample_at = Some(
            now.checked_add(self.config.interval)
                .ok_or(StatsSamplerError::ClockExhausted)?,
        );
        let mut samples = Vec::new();
        samples
            .try_reserve_exact(self.entries.len())
            .map_err(|_| StatsSamplerError::AllocationFailed)?;
        for (key, entry) in &mut self.entries {
            let elapsed = now.duration_since(entry.previous_sample_at);
            if elapsed.is_zero() {
                continue;
            }
            let current_speed = rate(
                entry
                    .current
                    .received_payload_bytes
                    .saturating_sub(entry.previous.received_payload_bytes),
                elapsed,
            );
            let useful_speed = rate(
                entry
                    .current
                    .committed_bytes
                    .saturating_sub(entry.previous.committed_bytes),
                elapsed,
            );
            let durable_speed = rate(
                entry
                    .current
                    .durable_bytes
                    .saturating_sub(entry.previous.durable_bytes),
                elapsed,
            );
            let average_speed = rate(
                entry
                    .current
                    .received_payload_bytes
                    .saturating_sub(entry.started_received),
                now.duration_since(entry.started_at),
            );
            let smoothed_speed = entry.smoothed_speed.map_or(current_speed, |previous| {
                weighted_average(previous, current_speed)
            });
            entry.smoothed_speed = Some(smoothed_speed);
            entry.previous = entry.current;
            entry.previous_sample_at = now;
            samples.push(StatsSample {
                key: key.clone(),
                sampled_at: now,
                counters: entry.current,
                diagnostic: entry.diagnostic,
                current_speed,
                wire_speed: current_speed,
                useful_speed,
                durable_speed,
                average_speed,
                smoothed_speed,
            });
        }
        Ok(Some(samples))
    }
}

fn validate_counters(counters: StatsCounters) -> Result<(), StatsSamplerError> {
    if counters.accepted_bytes > counters.received_payload_bytes {
        return Err(StatsSamplerError::InvalidCounterRelation(
            "accepted_bytes_exceed_received_payload_bytes",
        ));
    }
    if counters.committed_bytes > counters.accepted_bytes {
        return Err(StatsSamplerError::InvalidCounterRelation(
            "committed_bytes_exceed_accepted_bytes",
        ));
    }
    if counters.durable_bytes > counters.committed_bytes {
        return Err(StatsSamplerError::InvalidCounterRelation(
            "durable_bytes_exceed_committed_bytes",
        ));
    }
    if counters.provisional_in_flight_bytes > counters.submitted_bytes {
        return Err(StatsSamplerError::InvalidCounterRelation(
            "provisional_in_flight_bytes_exceed_submitted_bytes",
        ));
    }
    if counters.discarded_bytes > counters.received_payload_bytes {
        return Err(StatsSamplerError::InvalidCounterRelation(
            "discarded_bytes_exceed_received_payload_bytes",
        ));
    }
    if counters.discard_budget_consumed > counters.discarded_bytes {
        return Err(StatsSamplerError::InvalidCounterRelation(
            "discard_budget_consumed_exceeds_discarded_bytes",
        ));
    }
    Ok(())
}

fn validate_diagnostic(diagnostic: StatsDiagnostic) -> Result<(), StatsSamplerError> {
    if diagnostic
        .reason
        .is_some_and(|reason| reason.condition() != diagnostic.condition)
    {
        return Err(StatsSamplerError::InvalidDiagnostic);
    }
    Ok(())
}

fn validate_counter_update(
    previous: StatsCounters,
    current: StatsCounters,
) -> Result<(), StatsSamplerError> {
    for (name, previous, current) in [
        (
            "received_payload_bytes",
            previous.received_payload_bytes,
            current.received_payload_bytes,
        ),
        (
            "accepted_bytes",
            previous.accepted_bytes,
            current.accepted_bytes,
        ),
        (
            "submitted_bytes",
            previous.submitted_bytes,
            current.submitted_bytes,
        ),
        (
            "committed_bytes",
            previous.committed_bytes,
            current.committed_bytes,
        ),
        (
            "durable_bytes",
            previous.durable_bytes,
            current.durable_bytes,
        ),
        (
            "discarded_bytes",
            previous.discarded_bytes,
            current.discarded_bytes,
        ),
        (
            "discard_budget_consumed",
            previous.discard_budget_consumed,
            current.discard_budget_consumed,
        ),
    ] {
        if current < previous {
            return Err(StatsSamplerError::CounterRegression(name));
        }
    }
    for (name, previous, current) in [
        (
            "last_byte_arrival_at",
            previous.last_byte_arrival_at,
            current.last_byte_arrival_at,
        ),
        (
            "last_committed_progress_at",
            previous.last_committed_progress_at,
            current.last_committed_progress_at,
        ),
        (
            "last_successful_write_at",
            previous.last_successful_write_at,
            current.last_successful_write_at,
        ),
        (
            "last_progress_at",
            previous.last_progress_at,
            current.last_progress_at,
        ),
    ] {
        if matches!((previous, current), (Some(previous), Some(current)) if current < previous)
            || matches!((previous, current), (Some(_), None))
        {
            return Err(StatsSamplerError::TimestampRegression(name));
        }
    }
    Ok(())
}

fn rate(bytes: u64, elapsed: Duration) -> u64 {
    let nanos = elapsed.as_nanos();
    if nanos == 0 {
        return 0;
    }
    let per_second = u128::from(bytes)
        .saturating_mul(1_000_000_000)
        .checked_div(nanos)
        .unwrap_or(0);
    u64::try_from(per_second).unwrap_or(u64::MAX)
}

fn weighted_average(previous: u64, instant: u64) -> u64 {
    let value = u128::from(previous)
        .saturating_mul(3)
        .saturating_add(u128::from(instant))
        / 4;
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionCondition, ConnectionConditionReason, MAX_STATS_ACTIVE_ENTRIES,
        MAX_STATS_SAMPLE_INTERVAL, MIN_STATS_SAMPLE_INTERVAL, StatsCounters, StatsDiagnostic,
        StatsProfile, StatsSampler, StatsSamplerConfig, StatsSamplerError,
    };
    use ariax_core::{Aria2Status, MonotonicInstant, TaskState};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn capacity(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("nonzero capacity")
    }

    fn later(now: MonotonicInstant, millis: u64) -> MonotonicInstant {
        now.checked_add(Duration::from_millis(millis))
            .expect("test time")
    }

    fn counters(received: u64, committed: u64, durable: u64) -> StatsCounters {
        StatsCounters {
            received_payload_bytes: received,
            accepted_bytes: received,
            submitted_bytes: received,
            committed_bytes: committed,
            durable_bytes: durable,
            ..StatsCounters::default()
        }
    }

    #[test]
    fn profile_intervals_overrides_and_capacity_are_exact() {
        assert_eq!(StatsProfile::Concurrency.interval(), Duration::from_secs(1));
        assert_eq!(
            StatsProfile::Throughput.interval(),
            Duration::from_millis(500)
        );
        assert_eq!(StatsProfile::Latency.interval(), Duration::from_millis(250));
        assert_eq!(StatsProfile::Compact.interval(), Duration::from_secs(1));
        assert_eq!(
            StatsSamplerConfig::with_override(
                StatsProfile::Latency,
                capacity(1),
                Some(Duration::ZERO),
            )
            .interval(),
            MIN_STATS_SAMPLE_INTERVAL
        );
        assert_eq!(
            StatsSamplerConfig::with_override(
                StatsProfile::Latency,
                capacity(1),
                Some(Duration::from_secs(60)),
            )
            .interval(),
            MAX_STATS_SAMPLE_INTERVAL
        );
        assert!(matches!(
            StatsSampler::<u64>::new(StatsSamplerConfig::for_profile(
                StatsProfile::Compact,
                capacity(MAX_STATS_ACTIVE_ENTRIES + 1),
            )),
            Err(StatsSamplerError::CapacityTooLarge {
                requested,
                maximum,
            }) if requested == MAX_STATS_ACTIVE_ENTRIES + 1
                && maximum == MAX_STATS_ACTIVE_ENTRIES
        ));
    }

    #[test]
    fn current_speed_reaches_zero_and_ewma_decays_without_packets() {
        let now = MonotonicInstant::now();
        let config = StatsSamplerConfig::with_override(
            StatsProfile::Latency,
            capacity(1),
            Some(Duration::from_secs(1)),
        );
        let mut sampler = StatsSampler::new(config).expect("sampler");
        sampler
            .register(1_u64, counters(0, 0, 0), StatsDiagnostic::default(), now)
            .expect("register");
        sampler
            .update(&1, counters(1_000, 800, 600), StatsDiagnostic::default())
            .expect("update");
        let first = sampler
            .sample_at(later(now, 1_000))
            .expect("sample")
            .expect("due")
            .remove(0);
        assert_eq!(first.current_speed, 1_000);
        assert_eq!(first.smoothed_speed, 1_000);

        let second = sampler
            .sample_at(later(now, 2_000))
            .expect("sample")
            .expect("due")
            .remove(0);
        assert_eq!(second.current_speed, 0);
        assert_eq!(second.useful_speed, 0);
        assert_eq!(second.durable_speed, 0);
        assert_eq!(second.smoothed_speed, 750);
    }

    #[test]
    fn diagnostic_conditions_do_not_change_closed_task_status() {
        let now = MonotonicInstant::now();
        let mut sampler = StatsSampler::new(StatsSamplerConfig::for_profile(
            StatsProfile::Concurrency,
            capacity(1),
        ))
        .expect("sampler");
        let lifecycle = (TaskState::Active, Aria2Status::Active);
        sampler
            .register(
                1_u64,
                counters(0, 0, 0),
                StatsDiagnostic {
                    condition: ConnectionCondition::Backpressured,
                    reason: Some(ConnectionConditionReason::DiskBackpressure),
                },
                now,
            )
            .expect("register");
        let backpressured = sampler
            .sample_at(later(now, 1_000))
            .expect("sample")
            .expect("due")
            .remove(0);
        assert_eq!(
            backpressured.diagnostic.condition,
            ConnectionCondition::Backpressured
        );
        assert_eq!(lifecycle, (TaskState::Active, Aria2Status::Active));

        sampler
            .update(
                &1,
                counters(0, 0, 0),
                StatsDiagnostic {
                    condition: ConnectionCondition::RateLimited,
                    reason: Some(ConnectionConditionReason::IngressRateLimit),
                },
            )
            .expect("rate limited");
        let limited = sampler
            .sample_at(later(now, 2_000))
            .expect("sample")
            .expect("due")
            .remove(0);
        assert_eq!(
            limited.diagnostic.condition,
            ConnectionCondition::RateLimited
        );
        assert_eq!(lifecycle, (TaskState::Active, Aria2Status::Active));
    }

    #[test]
    fn rejected_updates_preserve_the_previous_rate_baseline() {
        let now = MonotonicInstant::now();
        let mut sampler = StatsSampler::new(StatsSamplerConfig::for_profile(
            StatsProfile::Concurrency,
            capacity(1),
        ))
        .expect("sampler");
        sampler
            .register(1_u64, counters(10, 8, 6), StatsDiagnostic::default(), now)
            .expect("register");
        assert_eq!(
            sampler.update(&1, counters(9, 8, 6), StatsDiagnostic::default()),
            Err(StatsSamplerError::CounterRegression(
                "received_payload_bytes"
            ))
        );
        sampler
            .update(&1, counters(110, 88, 66), StatsDiagnostic::default())
            .expect("valid update");
        let sample = sampler
            .sample_at(later(now, 1_000))
            .expect("sample")
            .expect("due")
            .remove(0);
        assert_eq!(sample.current_speed, 100);
        assert_eq!(sample.useful_speed, 80);
        assert_eq!(sample.durable_speed, 60);
    }

    #[test]
    fn bounds_unknown_entries_and_diagnostic_mismatches_are_typed() {
        let now = MonotonicInstant::now();
        let mut sampler = StatsSampler::new(StatsSamplerConfig::for_profile(
            StatsProfile::Compact,
            capacity(1),
        ))
        .expect("sampler");
        sampler
            .register(1_u64, counters(0, 0, 0), StatsDiagnostic::default(), now)
            .expect("register");
        assert_eq!(
            sampler.register(1, counters(0, 0, 0), StatsDiagnostic::default(), now),
            Err(StatsSamplerError::DuplicateEntry)
        );
        assert_eq!(
            sampler.register(2, counters(0, 0, 0), StatsDiagnostic::default(), now),
            Err(StatsSamplerError::Full)
        );
        assert_eq!(
            sampler.update(
                &1,
                counters(0, 0, 0),
                StatsDiagnostic {
                    condition: ConnectionCondition::Stalled,
                    reason: Some(ConnectionConditionReason::BufferBackpressure),
                },
            ),
            Err(StatsSamplerError::InvalidDiagnostic)
        );
        assert_eq!(sampler.unregister(&2), Err(StatsSamplerError::UnknownEntry));
        sampler.unregister(&1).expect("unregister");
        assert!(sampler.is_empty());
        assert_eq!(sampler.sample_at(later(now, 10_000)), Ok(None));
    }

    #[test]
    fn discard_and_sample_age_accounting_are_query_derived() {
        let now = MonotonicInstant::now();
        let mut sampler = StatsSampler::new(StatsSamplerConfig::for_profile(
            StatsProfile::Concurrency,
            capacity(1),
        ))
        .expect("sampler");
        sampler
            .register(1_u64, counters(0, 0, 0), StatsDiagnostic::default(), now)
            .expect("register");
        let mut observation = counters(1_000, 400, 300);
        observation.accepted_bytes = 600;
        observation.submitted_bytes = 600;
        observation.discarded_bytes = 400;
        observation.discard_budget_consumed = 400;
        observation.discard_budget_remaining = 100;
        sampler
            .update(&1, observation, StatsDiagnostic::default())
            .expect("update");
        assert_eq!(sampler.sample_at(later(now, 999)), Ok(None));
        let sample = sampler
            .sample_at(later(now, 1_000))
            .expect("sample")
            .expect("due")
            .remove(0);
        assert_eq!(sample.current_speed, 1_000);
        assert_eq!(sample.useful_speed, 400);
        assert_eq!(sample.durable_speed, 300);
        assert_eq!(sample.counters.discarded_bytes, 400);
        assert_eq!(sample.counters.discard_budget_remaining, 100);
        assert_eq!(
            sample.sample_age(later(now, 1_250)),
            Duration::from_millis(250)
        );
    }
}
