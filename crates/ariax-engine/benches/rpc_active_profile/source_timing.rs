//! Opt-in, bounded correlation of the benchmark's single source-mutation stream.

use serde_json::{Value, json};
use std::time::Duration;

pub const CAPACITY: usize = 125;

#[derive(Clone, Copy)]
struct BackendSample {
    started_unix_ns: u128,
    elapsed: Duration,
    succeeded: bool,
    stages: Option<[u64; 6]>,
}

pub struct BackendLog {
    samples: Vec<Option<BackendSample>>,
    invalid: bool,
}

impl Default for BackendLog {
    fn default() -> Self {
        Self {
            samples: Vec::with_capacity(CAPACITY),
            invalid: false,
        }
    }
}

impl BackendLog {
    pub fn begin(&mut self) -> Option<usize> {
        if self.samples.len() == CAPACITY {
            self.invalid = true;
            return None;
        }
        let ordinal = self.samples.len();
        self.samples.push(None);
        Some(ordinal)
    }

    pub fn finish(
        &mut self,
        ordinal: usize,
        started_unix_ns: u128,
        elapsed: Duration,
        succeeded: bool,
    ) {
        match self.samples.get_mut(ordinal) {
            Some(slot @ None) => {
                *slot = Some(BackendSample {
                    started_unix_ns,
                    elapsed,
                    succeeded,
                    stages: None,
                });
            }
            _ => self.invalid = true,
        }
    }

    #[cfg(any(feature = "control-diagnostics", test))]
    pub fn attach_stages(&mut self, ordinal: usize, stages: Result<[u64; 6], &'static str>) {
        match (self.samples.get_mut(ordinal), stages) {
            (Some(Some(sample)), Ok(stages))
                if sample.stages.is_none()
                    && stages.windows(2).all(|pair| pair[0] <= pair[1])
                    && u128::from(stages[5]) <= sample.elapsed.as_nanos() =>
            {
                sample.stages = Some(stages);
            }
            _ => self.invalid = true,
        }
    }

    pub fn report(&self) -> Result<Value, &'static str> {
        if self.invalid {
            return Err("source timing overflow or duplicate completion");
        }
        self.samples
            .iter()
            .enumerate()
            .map(|(ordinal, sample)| {
                let sample = sample.ok_or("incomplete source timing")?;
                let mut row = json!({"ordinal": ordinal, "startedUnixNs": sample.started_unix_ns,
                    "elapsedNs": sample.elapsed.as_nanos(), "succeeded": sample.succeeded});
                if let Some(stages) = sample.stages {
                    row["stageOffsetsNs"] = json!(stages);
                }
                Ok(row)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::from)
    }
}

pub struct ClientSample {
    pub sample_index: usize,
    pub burst: usize,
    pub started_unix_ns: u128,
    pub elapsed: Duration,
}

pub fn correlate(
    client: &[ClientSample],
    backend: &Value,
    total_samples: usize,
) -> Result<Value, &'static str> {
    let backend = backend.as_array().ok_or("missing backend source timing")?;
    let expected = total_samples / 160;
    if total_samples == 0
        || !total_samples.is_multiple_of(160)
        || expected > CAPACITY
        || client.len() != expected
        || backend.len() != expected
    {
        return Err("source timing count mismatch");
    }
    let mut samples = Vec::with_capacity(expected);
    for (ordinal, (client, backend)) in client.iter().zip(backend).enumerate() {
        if client.sample_index != 99 + ordinal * 160
            || client.burst == 0
            || client.started_unix_ns == 0
            || backend["ordinal"].as_u64() != Some(ordinal as u64)
            || backend["succeeded"].as_bool() != Some(true)
        {
            return Err("source timing correlation mismatch");
        }
        let backend_started = backend["startedUnixNs"]
            .as_u64()
            .filter(|value| *value > 0)
            .ok_or("invalid backend source timing anchor")?;
        let elapsed = backend["elapsedNs"]
            .as_u64()
            .ok_or("invalid backend source timing duration")?;
        let outside = client
            .elapsed
            .checked_sub(Duration::from_nanos(elapsed))
            .ok_or("backend source timing exceeds round trip")?;
        let mut row = json!({"ordinal": ordinal, "sampleIndex": client.sample_index,
            "burst": client.burst, "clientStartedUnixNs": client.started_unix_ns,
            "backendStartedUnixNs": backend_started, "roundTripNs": client.elapsed.as_nanos(),
            "backendNs": elapsed, "outsideBackendNs": outside.as_nanos()});
        if let Some(stages) = backend.get("stageOffsetsNs") {
            let offsets = stages.as_array().ok_or("invalid source stages")?;
            if offsets.len() != 6 {
                return Err("invalid source stage count");
            }
            let mut previous = 0;
            for offset in offsets {
                let value = offset.as_u64().ok_or("invalid source stage offset")?;
                if value < previous || value > elapsed {
                    return Err("invalid source stage order");
                }
                previous = value;
            }
            row["stageOffsetsNs"] = stages.clone();
        }
        samples.push(row);
    }
    let staged = samples
        .iter()
        .filter(|row| row.get("stageOffsetsNs").is_some())
        .count();
    if staged != 0 && staged != expected {
        return Err("missing source stages");
    }
    Ok(json!({"version": 1, "capacity": CAPACITY, "calls": expected, "samples": samples}))
}
