//! Consumer-attributed benchmark evidence; process totals cannot prove ownership.

use serde_json::Value;

pub const CONSUMERS: [&str; 2] = ["events", "response"];
pub const MIN_RETAINED_BYTES: u64 = 256 * 1024;

pub fn consumer_index(name: &str) -> Result<usize, &'static str> {
    CONSUMERS
        .iter()
        .position(|candidate| *candidate == name)
        .ok_or("unknown stalled consumer")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Sample {
    pub bytes: u64,
    pub responses: u64,
}

impl Sample {
    pub fn retains(self, baseline: u64) -> bool {
        self.responses == 1 && self.bytes.saturating_sub(baseline) >= MIN_RETAINED_BYTES
    }
}

/// Missing evidence is an error; only an explicitly expired observer is `None`.
pub fn sample(metrics: &Value, name: &str) -> Result<Option<Sample>, &'static str> {
    consumer_index(name)?;
    let value = metrics["stalledConsumers"]
        .get(name)
        .ok_or("missing stalled consumer observer")?;
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(Sample {
        bytes: value["bytes"].as_u64().ok_or("missing consumer bytes")?,
        responses: value["responses"]
            .as_u64()
            .ok_or("missing consumer responses")?,
    }))
}
