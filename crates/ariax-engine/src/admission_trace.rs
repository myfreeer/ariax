//! Bounded, opt-in local diagnostics. Never stores request parameters or secrets.

use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Admitted,
    Dispatched,
    PreparationQueued,
    PreparationStarted,
    PreparationFinished,
    PreparationReceived,
    FinalizationQueued,
    FinalizationStarted,
    FinalizationFinished,
    FinalizationReceived,
    SchedulerStarted,
    Published,
    Delivered,
}

#[derive(Default)]
struct Samples {
    offsets: [u64; 13],
    count: usize,
    invalid: bool,
}

/// Thirteen request-local offsets from the benchmark's backend start instant.
/// Available only with the `control-diagnostics` feature.
#[derive(Clone)]
pub struct AdmissionTrace {
    started: Instant,
    samples: Arc<Mutex<Samples>>,
}

impl AdmissionTrace {
    #[must_use]
    pub fn new(started: Instant) -> Self {
        Self {
            started,
            samples: Arc::new(Mutex::new(Samples::default())),
        }
    }

    pub(crate) fn mark(&self, stage: Stage) {
        let mut samples = self.samples.lock().expect("admission trace");
        if samples.count != stage as usize || samples.count == 13 {
            samples.invalid = true;
            return;
        }
        let Ok(offset) = u64::try_from(self.started.elapsed().as_nanos()) else {
            samples.invalid = true;
            return;
        };
        let index = samples.count;
        samples.offsets[index] = offset;
        samples.count += 1;
    }

    /// Rejects partial, repeated and misordered traces instead of hiding them.
    pub fn offsets_ns(&self) -> Result<[u64; 13], &'static str> {
        let samples = self.samples.lock().expect("admission trace");
        if samples.invalid || samples.count != 13 {
            return Err("incomplete or invalid admission stages");
        }
        Ok(samples.offsets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAGES: [Stage; 13] = [
        Stage::Admitted,
        Stage::Dispatched,
        Stage::PreparationQueued,
        Stage::PreparationStarted,
        Stage::PreparationFinished,
        Stage::PreparationReceived,
        Stage::FinalizationQueued,
        Stage::FinalizationStarted,
        Stage::FinalizationFinished,
        Stage::FinalizationReceived,
        Stage::SchedulerStarted,
        Stage::Published,
        Stage::Delivered,
    ];

    #[test]
    fn clones_follow_one_bounded_monotonic_request() {
        let trace = AdmissionTrace::new(Instant::now());
        for stage in STAGES {
            trace.clone().mark(stage);
        }
        let offsets = trace.offsets_ns().unwrap();
        assert!(offsets.windows(2).all(|pair| pair[0] <= pair[1]));
        trace.mark(Stage::Delivered);
        assert!(trace.offsets_ns().is_err());
    }

    #[test]
    fn missing_duplicate_and_out_of_order_stages_reject() {
        for missing in 0..STAGES.len() {
            let trace = AdmissionTrace::new(Instant::now());
            for stage in &STAGES[..missing] {
                trace.mark(*stage);
            }
            assert!(trace.offsets_ns().is_err());
        }
        for stages in [
            vec![],
            vec![Stage::Admitted],
            vec![Stage::Admitted, Stage::Admitted],
            vec![Stage::Dispatched],
        ] {
            let trace = AdmissionTrace::new(Instant::now());
            for stage in stages {
                trace.mark(stage);
            }
            assert!(trace.offsets_ns().is_err());
        }
    }
}
