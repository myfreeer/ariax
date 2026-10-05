//! Fixed-size burst diagnostics, formatted only on failure or after measurement.

use serde_json::{Value, json};
use std::fmt;
use std::time::Duration;

#[derive(Clone, Copy)]
pub enum Phase {
    Primary,
    Verification,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Verification => "verification",
        }
    }
}

#[derive(Clone, Copy)]
struct Step {
    method: &'static str,
    phase: Phase,
    sample_index: usize,
    start: Duration,
    elapsed: Duration,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = self.phase.name();
        write!(
            f,
            "operation={},phase={phase},sampleIndex={},startUs={},durationUs={}",
            self.method,
            self.sample_index,
            self.start.as_micros(),
            self.elapsed.as_micros()
        )
    }
}

impl Step {
    fn report(self) -> Value {
        json!({"operation": self.method, "phase": self.phase.name(),
               "sampleIndex": self.sample_index, "startUs": self.start.as_micros(),
               "durationUs": self.elapsed.as_micros()})
    }
}

#[derive(Clone, Copy)]
pub struct BurstTiming {
    first_sample: usize,
    primary_calls: usize,
    verification_calls: usize,
    primary: Duration,
    verification: Duration,
    last: Option<Step>,
    slowest: Option<Step>,
}

impl BurstTiming {
    pub fn new(first_sample: usize) -> Self {
        Self {
            first_sample,
            primary_calls: 0,
            verification_calls: 0,
            primary: Duration::ZERO,
            verification: Duration::ZERO,
            last: None,
            slowest: None,
        }
    }

    pub fn record(
        &mut self,
        method: &'static str,
        phase: Phase,
        sample_index: usize,
        start: Duration,
        elapsed: Duration,
    ) {
        match phase {
            Phase::Primary => {
                self.primary_calls += 1;
                self.primary += elapsed;
            }
            Phase::Verification => {
                self.verification_calls += 1;
                self.verification += elapsed;
            }
        }
        let step = Step {
            method,
            phase,
            sample_index,
            start,
            elapsed,
        };
        self.last = Some(step);
        if self
            .slowest
            .is_none_or(|previous| elapsed > previous.elapsed)
        {
            self.slowest = Some(step);
        }
    }

    pub fn check_limit(
        &self,
        scenario: &str,
        burst: usize,
        elapsed: Duration,
    ) -> Result<(), String> {
        if elapsed <= Duration::from_millis(500) {
            return Ok(());
        }
        let describe =
            |step: Option<Step>| step.map_or_else(|| "none".to_owned(), |step| step.to_string());
        Err(format!(
            "{scenario} burst exceeded 500 ms: {} us; burst={burst} firstSampleIndex={} \
             primaryCalls={} verificationCalls={} timedStepUs={} otherUs={} \
             last=[{}] slowest=[{}] primaryStepUs={} verificationStepUs={}",
            elapsed.as_micros(),
            self.first_sample,
            self.primary_calls,
            self.verification_calls,
            (self.primary + self.verification).as_micros(),
            elapsed
                .saturating_sub(self.primary + self.verification)
                .as_micros(),
            describe(self.last),
            describe(self.slowest),
            self.primary.as_micros(),
            self.verification.as_micros(),
        ))
    }
}

struct CompletedBurst {
    burst: usize,
    started_unix_ns: u128,
    elapsed: Duration,
    timing: BurstTiming,
}

#[derive(Default)]
pub struct CompletedBursts {
    primary: Duration,
    verification: Duration,
    other: Duration,
    worst: Option<CompletedBurst>,
}

impl CompletedBursts {
    pub fn record(
        &mut self,
        scenario: &str,
        burst: usize,
        started_unix_ns: u128,
        elapsed: Duration,
        timing: BurstTiming,
    ) -> Result<(), String> {
        timing
            .check_limit(scenario, burst, elapsed)
            .map_err(|error| format!("{error} burstStartedUnixNs={started_unix_ns}"))?;
        let other = elapsed
            .checked_sub(timing.primary + timing.verification)
            .ok_or("timed steps exceed their burst duration")?;
        self.primary += timing.primary;
        self.verification += timing.verification;
        self.other += other;
        if self
            .worst
            .as_ref()
            .is_none_or(|prior| elapsed > prior.elapsed)
        {
            self.worst = Some(CompletedBurst {
                burst,
                started_unix_ns,
                elapsed,
                timing,
            });
        }
        Ok(())
    }

    pub fn report(&self) -> Value {
        let worst = self.worst.as_ref().map(|worst| {
            let timing = worst.timing;
            json!({"burst": worst.burst, "elapsedUs": worst.elapsed.as_micros(),
                   "startedUnixNs": worst.started_unix_ns,
                   "elapsedNs": worst.elapsed.as_nanos(), "firstSampleIndex": timing.first_sample,
                   "primaryCalls": timing.primary_calls, "verificationCalls": timing.verification_calls,
                   "primaryUs": timing.primary.as_micros(), "verificationUs": timing.verification.as_micros(),
                   "otherUs": (worst.elapsed - timing.primary - timing.verification).as_micros(),
                   "last": timing.last.map(Step::report), "slowest": timing.slowest.map(Step::report)})
        });
        json!({"version": 1, "primaryUs": self.primary.as_micros(),
               "verificationUs": self.verification.as_micros(), "otherUs": self.other.as_micros(),
               "worstCompletedBurst": worst})
    }
}
