//! Bounded, opt-in local diagnostics. Never stores request parameters or secrets.

use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Admitted,
    Dispatched,
    Prepared,
    CommitStarted,
    Published,
    Delivered,
}

#[derive(Default)]
struct Samples {
    offsets: [u64; 6],
    count: usize,
    invalid: bool,
}

/// Six request-local offsets from the benchmark's backend start instant.
/// Available only with the `control-diagnostics` feature.
#[derive(Clone)]
pub struct SourceMutationTrace {
    started: Instant,
    samples: Arc<Mutex<Samples>>,
}

impl SourceMutationTrace {
    #[must_use]
    pub fn new(started: Instant) -> Self {
        Self {
            started,
            samples: Arc::new(Mutex::new(Samples::default())),
        }
    }

    pub(crate) fn mark(&self, stage: Stage) {
        let mut samples = self.samples.lock().expect("source trace");
        if samples.count != stage as usize || samples.count == 6 {
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
    pub fn offsets_ns(&self) -> Result<[u64; 6], &'static str> {
        let samples = self.samples.lock().expect("source trace");
        if samples.invalid || samples.count != 6 {
            return Err("incomplete or invalid source stages");
        }
        Ok(samples.offsets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAGES: [Stage; 6] = [
        Stage::Admitted,
        Stage::Dispatched,
        Stage::Prepared,
        Stage::CommitStarted,
        Stage::Published,
        Stage::Delivered,
    ];

    #[test]
    fn clones_follow_one_bounded_monotonic_request() {
        let trace = SourceMutationTrace::new(Instant::now());
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
        for stages in [
            vec![],
            vec![Stage::Admitted],
            vec![Stage::Admitted, Stage::Admitted],
            vec![Stage::Dispatched],
        ] {
            let trace = SourceMutationTrace::new(Instant::now());
            for stage in stages {
                trace.mark(stage);
            }
            assert!(trace.offsets_ns().is_err());
        }
    }
}
