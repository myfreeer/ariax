//! Bounded failure diagnostics; only rejected bursts allocate formatted output.

use std::fmt;
use std::time::Duration;

#[derive(Clone, Copy)]
pub enum Phase {
    Primary,
    Verification,
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
        let phase = match self.phase {
            Phase::Primary => "primary",
            Phase::Verification => "verification",
        };
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

pub struct BurstTiming {
    first_sample: usize,
    primary_calls: usize,
    verification_calls: usize,
    timed: Duration,
    last: Option<Step>,
    slowest: Option<Step>,
}

impl BurstTiming {
    pub fn new(first_sample: usize) -> Self {
        Self {
            first_sample,
            primary_calls: 0,
            verification_calls: 0,
            timed: Duration::ZERO,
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
            Phase::Primary => self.primary_calls += 1,
            Phase::Verification => self.verification_calls += 1,
        }
        let step = Step {
            method,
            phase,
            sample_index,
            start,
            elapsed,
        };
        self.timed += elapsed;
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
             last=[{}] slowest=[{}]",
            elapsed.as_micros(),
            self.first_sample,
            self.primary_calls,
            self.verification_calls,
            self.timed.as_micros(),
            elapsed.saturating_sub(self.timed).as_micros(),
            describe(self.last),
            describe(self.slowest),
        ))
    }
}
