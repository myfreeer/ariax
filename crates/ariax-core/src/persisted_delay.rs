use crate::{MAX_PERSISTED_MILLISECONDS, MonotonicInstant};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

/// A persistence-safe wall-clock scheduling decision.
///
/// The wall timestamp is diagnostic/recovery evidence only. Live waits always
/// run against a fresh process-local monotonic deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistedDelayDecision {
    scheduled_at_unix_ms: u64,
    delay_ms: u64,
}

impl PersistedDelayDecision {
    pub fn new(scheduled_at_unix_ms: u64, delay_ms: u64) -> Result<Self, PersistedDelayError> {
        if scheduled_at_unix_ms > MAX_PERSISTED_MILLISECONDS {
            return Err(PersistedDelayError::ScheduledAtOutOfRange);
        }
        if delay_ms == 0 {
            return Err(PersistedDelayError::ZeroDelay);
        }
        if delay_ms > MAX_PERSISTED_MILLISECONDS {
            return Err(PersistedDelayError::DelayOutOfRange);
        }
        Ok(Self {
            scheduled_at_unix_ms,
            delay_ms,
        })
    }

    #[must_use]
    pub const fn scheduled_at_unix_ms(self) -> u64 {
        self.scheduled_at_unix_ms
    }

    #[must_use]
    pub const fn delay_ms(self) -> u64 {
        self.delay_ms
    }

    /// Reconstructs a conservative monotonic wait from persisted wall-clock
    /// evidence. A wall clock before the recorded scheduling time waits the
    /// full chosen delay again; a clock beyond the delay treats it as expired.
    pub fn recover(
        self,
        now_wall_unix_ms: u64,
        now_monotonic: MonotonicInstant,
        max_wait_ms: NonZeroU64,
    ) -> Result<RecoveredDelayDecision, PersistedDelayError> {
        let (clock, recovered_wait_elapsed_ms, remaining_ms) =
            if now_wall_unix_ms < self.scheduled_at_unix_ms {
                (RecoveredWallClock::BeforeScheduled, 0, self.delay_ms)
            } else {
                let wall_elapsed_ms = now_wall_unix_ms - self.scheduled_at_unix_ms;
                let recovered_wait_elapsed_ms = wall_elapsed_ms.min(self.delay_ms);
                let remaining_ms = self.delay_ms - recovered_wait_elapsed_ms;
                let clock = if remaining_ms == 0 {
                    RecoveredWallClock::Expired
                } else {
                    RecoveredWallClock::WithinDelay
                };
                (clock, recovered_wait_elapsed_ms, remaining_ms)
            };

        let remaining_ms = remaining_ms.min(max_wait_ms.get());
        let deadline = now_monotonic
            .checked_add(Duration::from_millis(remaining_ms))
            .ok_or(PersistedDelayError::MonotonicDeadlineOutOfRange)?;
        Ok(RecoveredDelayDecision {
            clock,
            deadline,
            remaining_ms,
            recovered_wait_elapsed_ms,
        })
    }
}

/// How the current wall clock related to a persisted delay decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveredWallClock {
    WithinDelay,
    BeforeScheduled,
    Expired,
}

/// Validated recovery output for a new process-local monotonic timer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveredDelayDecision {
    clock: RecoveredWallClock,
    deadline: MonotonicInstant,
    remaining_ms: u64,
    recovered_wait_elapsed_ms: u64,
}

impl RecoveredDelayDecision {
    #[must_use]
    pub const fn clock(self) -> RecoveredWallClock {
        self.clock
    }

    #[must_use]
    pub const fn deadline(self) -> MonotonicInstant {
        self.deadline
    }

    #[must_use]
    pub const fn remaining_ms(self) -> u64 {
        self.remaining_ms
    }

    #[must_use]
    pub const fn recovered_wait_elapsed_ms(self) -> u64 {
        self.recovered_wait_elapsed_ms
    }

    #[must_use]
    pub const fn is_expired(self) -> bool {
        self.remaining_ms == 0
    }

    /// Adds recovered elapsed time to the previously persisted retry budget,
    /// saturating before clamping to the configured maximum.
    #[must_use]
    pub const fn retry_budget_elapsed_ms(
        self,
        elapsed_before_wait_ms: u64,
        max_elapsed_ms: u64,
    ) -> u64 {
        let elapsed = elapsed_before_wait_ms.saturating_add(self.recovered_wait_elapsed_ms);
        if elapsed > max_elapsed_ms {
            max_elapsed_ms
        } else {
            elapsed
        }
    }
}

/// Why persisted delay evidence could not be used safely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistedDelayError {
    ScheduledAtOutOfRange,
    ZeroDelay,
    DelayOutOfRange,
    MonotonicDeadlineOutOfRange,
}

impl fmt::Display for PersistedDelayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ScheduledAtOutOfRange => "persisted scheduling time is out of range",
            Self::ZeroDelay => "persisted delay must be nonzero",
            Self::DelayOutOfRange => "persisted delay is out of range",
            Self::MonotonicDeadlineOutOfRange => "recovered monotonic deadline is out of range",
        })
    }
}

impl Error for PersistedDelayError {}

#[cfg(test)]
mod tests {
    use super::{PersistedDelayDecision, PersistedDelayError, RecoveredWallClock};
    use crate::{MAX_PERSISTED_MILLISECONDS, MonotonicInstant};
    use std::num::NonZeroU64;

    fn max_wait(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("nonzero max wait")
    }

    #[test]
    fn within_delay_recovers_remaining_wait_and_retry_budget() {
        let now = MonotonicInstant::now();
        let recovered = PersistedDelayDecision::new(1_000, 5_000)
            .expect("valid decision")
            .recover(3_000, now, max_wait(10_000))
            .expect("recover decision");

        assert_eq!(recovered.clock(), RecoveredWallClock::WithinDelay);
        assert_eq!(recovered.remaining_ms(), 3_000);
        assert_eq!(recovered.recovered_wait_elapsed_ms(), 2_000);
        assert_eq!(recovered.deadline().duration_since(now).as_millis(), 3_000);
        assert_eq!(recovered.retry_budget_elapsed_ms(7_000, 8_000), 8_000);
        assert!(!recovered.is_expired());
    }

    #[test]
    fn backward_clock_waits_full_delay_but_honors_max_wait() {
        let now = MonotonicInstant::now();
        let recovered = PersistedDelayDecision::new(10_000, 60_000)
            .expect("valid decision")
            .recover(9_999, now, max_wait(5_000))
            .expect("recover decision");

        assert_eq!(recovered.clock(), RecoveredWallClock::BeforeScheduled);
        assert_eq!(recovered.remaining_ms(), 5_000);
        assert_eq!(recovered.recovered_wait_elapsed_ms(), 0);
        assert_eq!(recovered.deadline().duration_since(now).as_millis(), 5_000);
    }

    #[test]
    fn forward_clock_past_delay_expires_immediately() {
        let now = MonotonicInstant::now();
        let recovered = PersistedDelayDecision::new(1_000, 5_000)
            .expect("valid decision")
            .recover(10_000, now, max_wait(5_000))
            .expect("recover decision");

        assert_eq!(recovered.clock(), RecoveredWallClock::Expired);
        assert_eq!(recovered.remaining_ms(), 0);
        assert_eq!(recovered.recovered_wait_elapsed_ms(), 5_000);
        assert_eq!(recovered.deadline(), now);
        assert!(recovered.is_expired());
    }

    #[test]
    fn invalid_persisted_fields_are_rejected() {
        assert_eq!(
            PersistedDelayDecision::new(MAX_PERSISTED_MILLISECONDS + 1, 1),
            Err(PersistedDelayError::ScheduledAtOutOfRange)
        );
        assert_eq!(
            PersistedDelayDecision::new(0, 0),
            Err(PersistedDelayError::ZeroDelay)
        );
        assert_eq!(
            PersistedDelayDecision::new(0, MAX_PERSISTED_MILLISECONDS + 1),
            Err(PersistedDelayError::DelayOutOfRange)
        );
    }
}
