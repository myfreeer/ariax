//! Opt-in, bounded correlation of the benchmark's single source-mutation stream.

use serde_json::{Value, json};
use std::time::Duration;

pub const CAPACITY: usize = 125;

#[derive(Clone, Copy)]
struct BackendSample {
    started_unix_ns: u128,
    elapsed: Duration,
    succeeded: bool,
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
                });
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
                Ok(
                    json!({"ordinal": ordinal, "startedUnixNs": sample.started_unix_ns,
                          "elapsedNs": sample.elapsed.as_nanos(), "succeeded": sample.succeeded}),
                )
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
        samples.push(
            json!({"ordinal": ordinal, "sampleIndex": client.sample_index,
            "burst": client.burst, "clientStartedUnixNs": client.started_unix_ns,
            "backendStartedUnixNs": backend_started, "roundTripNs": client.elapsed.as_nanos(),
            "backendNs": elapsed, "outsideBackendNs": outside.as_nanos()}),
        );
    }
    Ok(json!({"version": 1, "capacity": CAPACITY, "calls": expected, "samples": samples}))
}
