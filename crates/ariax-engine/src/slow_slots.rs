//! Bounded process policy shared by HTTP workers and the scheduler adapter.

use ariax_core::{Generation, MonotonicInstant, SlowReadmissionPolicy};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SlowSlotPolicy {
    #[default]
    Off,
    Demote,
    Pause,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetryWaitSlotPolicy {
    #[default]
    Retain,
    Release,
    Auto,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlowSlotConfig {
    pub policy: SlowSlotPolicy,
    pub speed_limit: u64,
    pub grace_period: Duration,
    pub min_active_time: Duration,
    pub max_demotions: u32,
    pub readmit_after: Duration,
    pub readmit_policy: SlowReadmissionPolicy,
    pub retry_wait: RetryWaitSlotPolicy,
}

impl Default for SlowSlotConfig {
    fn default() -> Self {
        Self {
            policy: SlowSlotPolicy::Off,
            speed_limit: 0,
            grace_period: Duration::from_secs(60),
            min_active_time: Duration::from_secs(30),
            max_demotions: 3,
            readmit_after: Duration::from_secs(60),
            readmit_policy: SlowReadmissionPolicy::OriginalPosition,
            retry_wait: RetryWaitSlotPolicy::Retain,
        }
    }
}

impl SlowSlotConfig {
    pub(crate) fn options(self) -> BTreeMap<String, String> {
        [
            (
                "slow-slot-policy",
                match self.policy {
                    SlowSlotPolicy::Off => "off",
                    SlowSlotPolicy::Demote => "demote",
                    SlowSlotPolicy::Pause => "pause",
                }
                .to_owned(),
            ),
            ("slow-slot-speed-limit", self.speed_limit.to_string()),
            (
                "slow-slot-grace-period",
                self.grace_period.as_secs().to_string(),
            ),
            (
                "slow-slot-min-active-time",
                self.min_active_time.as_secs().to_string(),
            ),
            ("slow-slot-max-demotions", self.max_demotions.to_string()),
            (
                "slow-slot-readmit-after",
                self.readmit_after.as_secs().to_string(),
            ),
            (
                "slow-slot-readmit-policy",
                match self.readmit_policy {
                    SlowReadmissionPolicy::Front => "front",
                    SlowReadmissionPolicy::OriginalPosition => "original-position",
                    SlowReadmissionPolicy::Back => "back",
                }
                .to_owned(),
            ),
            (
                "retry-wait-consumes-slot",
                match self.retry_wait {
                    RetryWaitSlotPolicy::Retain => "true",
                    RetryWaitSlotPolicy::Release => "false",
                    RetryWaitSlotPolicy::Auto => "auto",
                }
                .to_owned(),
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
    }

    pub fn validate(self) -> Result<Self, crate::HttpControlError> {
        if self.grace_period.is_zero()
            || self.grace_period.subsec_nanos() != 0
            || self.grace_period > Duration::from_secs(86400)
            || self.min_active_time.subsec_nanos() != 0
            || self.min_active_time > Duration::from_secs(86400)
            || self.readmit_after.is_zero()
            || self.readmit_after.subsec_nanos() != 0
            || self.readmit_after > Duration::from_secs(86400)
            || self.max_demotions == 0
            || self.max_demotions > 1024
        {
            return Err(crate::HttpControlError::InvalidConfig);
        }
        Ok(self)
    }

    pub(crate) fn from_options(
        values: &BTreeMap<String, String>,
    ) -> Result<Self, crate::HttpControlError> {
        let defaults = Self::default();
        let number = |key: &str, default| {
            values.get(key).map_or(Ok(default), |value| {
                value
                    .parse::<u64>()
                    .map_err(|_| crate::HttpControlError::InvalidConfig)
            })
        };
        let policy = match values
            .get("slow-slot-policy")
            .map(String::as_str)
            .unwrap_or("off")
        {
            "off" => SlowSlotPolicy::Off,
            "demote" => SlowSlotPolicy::Demote,
            "pause" => SlowSlotPolicy::Pause,
            _ => return Err(crate::HttpControlError::InvalidConfig),
        };
        let readmit_policy = match values
            .get("slow-slot-readmit-policy")
            .map(String::as_str)
            .unwrap_or("original-position")
        {
            "front" => SlowReadmissionPolicy::Front,
            "original-position" => SlowReadmissionPolicy::OriginalPosition,
            "back" => SlowReadmissionPolicy::Back,
            _ => return Err(crate::HttpControlError::InvalidConfig),
        };
        let retry_wait = match values
            .get("retry-wait-consumes-slot")
            .map(String::as_str)
            .unwrap_or("true")
        {
            "true" => RetryWaitSlotPolicy::Retain,
            "false" => RetryWaitSlotPolicy::Release,
            "auto" => RetryWaitSlotPolicy::Auto,
            _ => return Err(crate::HttpControlError::InvalidConfig),
        };
        Self {
            policy,
            speed_limit: number("slow-slot-speed-limit", 0)?,
            grace_period: Duration::from_secs(number(
                "slow-slot-grace-period",
                defaults.grace_period.as_secs(),
            )?),
            min_active_time: Duration::from_secs(number(
                "slow-slot-min-active-time",
                defaults.min_active_time.as_secs(),
            )?),
            max_demotions: u32::try_from(number("slow-slot-max-demotions", 3)?)
                .map_err(|_| crate::HttpControlError::InvalidConfig)?,
            readmit_after: Duration::from_secs(number("slow-slot-readmit-after", 60)?),
            readmit_policy,
            retry_wait,
        }
        .validate()
    }
}

#[derive(Clone, Debug, Default)]
pub struct HttpSchedulingPolicy {
    config: Arc<Mutex<SlowSlotConfig>>,
    all_active_idle: Arc<AtomicBool>,
}

impl HttpSchedulingPolicy {
    pub fn config(&self) -> SlowSlotConfig {
        *self.config.lock().expect("HTTP scheduling policy")
    }
    pub(crate) fn replace(&self, config: SlowSlotConfig) {
        *self.config.lock().expect("HTTP scheduling policy") = config;
    }
    pub(crate) fn set_all_active_idle(&self, idle: bool) {
        self.all_active_idle.store(idle, Ordering::Relaxed);
    }
    pub(crate) fn release_retry_wait(&self, wait: Duration) -> bool {
        let config = self.config();
        match config.retry_wait {
            RetryWaitSlotPolicy::Retain => false,
            RetryWaitSlotPolicy::Release => true,
            RetryWaitSlotPolicy::Auto => {
                wait >= config.readmit_after || self.all_active_idle.load(Ordering::Relaxed)
            }
        }
    }
}

pub(crate) struct SlowObservation {
    pub generation: Generation,
    pub active_since: MonotonicInstant,
    pub slow_since: Option<MonotonicInstant>,
}

pub(crate) const SLOW_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduling_options_round_trip_and_reject_lossy_or_unbounded_values() {
        for policy in [
            SlowSlotPolicy::Off,
            SlowSlotPolicy::Demote,
            SlowSlotPolicy::Pause,
        ] {
            for retry_wait in [
                RetryWaitSlotPolicy::Retain,
                RetryWaitSlotPolicy::Release,
                RetryWaitSlotPolicy::Auto,
            ] {
                let config = SlowSlotConfig {
                    policy,
                    retry_wait,
                    ..SlowSlotConfig::default()
                };
                assert_eq!(
                    SlowSlotConfig::from_options(&config.options()).unwrap(),
                    config
                );
            }
        }
        for config in [
            SlowSlotConfig {
                grace_period: Duration::ZERO,
                ..SlowSlotConfig::default()
            },
            SlowSlotConfig {
                grace_period: Duration::from_millis(1500),
                ..SlowSlotConfig::default()
            },
            SlowSlotConfig {
                min_active_time: Duration::from_millis(1),
                ..SlowSlotConfig::default()
            },
            SlowSlotConfig {
                readmit_after: Duration::from_millis(1500),
                ..SlowSlotConfig::default()
            },
            SlowSlotConfig {
                max_demotions: 0,
                ..SlowSlotConfig::default()
            },
            SlowSlotConfig {
                readmit_after: Duration::from_secs(86401),
                ..SlowSlotConfig::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
        let shared = HttpSchedulingPolicy::default();
        assert!(!shared.release_retry_wait(Duration::from_secs(86400)));
        shared.replace(SlowSlotConfig {
            retry_wait: RetryWaitSlotPolicy::Auto,
            ..SlowSlotConfig::default()
        });
        assert!(!shared.release_retry_wait(Duration::from_secs(1)));
        assert!(shared.release_retry_wait(Duration::from_secs(60)));
        shared.set_all_active_idle(true);
        assert!(shared.release_retry_wait(Duration::from_secs(1)));
    }
}
