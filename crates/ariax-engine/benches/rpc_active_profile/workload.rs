//! Explicit geometry for full acceptance and bounded local diagnostics.

use std::ffi::OsStr;

pub const SETTING: &str = "ARIAX_BENCH_DIAGNOSTIC_SMALL";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Workload {
    pub ranges: usize,
    pub samples: usize,
    pub diagnostic_small: bool,
}

impl Workload {
    pub fn from_setting(value: Option<&OsStr>) -> Result<Self, &'static str> {
        let diagnostic_small = match value {
            None => false,
            Some(value) if value == "0" => false,
            Some(value) if value == "1" => true,
            Some(_) => return Err("ARIAX_BENCH_DIAGNOSTIC_SMALL must be 0 or 1"),
        };
        Ok(if diagnostic_small {
            Self {
                ranges: 16,
                samples: 1_600,
                diagnostic_small,
            }
        } else {
            Self {
                ranges: 1_000,
                samples: 20_000,
                diagnostic_small,
            }
        })
    }

    pub fn total_bytes(self) -> usize {
        self.ranges * 2 * 1024 * 1024
    }

    pub fn scenario_seconds(self) -> u64 {
        if self.diagnostic_small { 30 } else { 90 }
    }

    pub fn measurement_kind(self) -> &'static str {
        if self.diagnostic_small {
            "diagnostic-small"
        } else {
            "full"
        }
    }

    pub fn validate_scenario(self, scenario: &str) -> Result<(), &'static str> {
        match scenario {
            "http" | "websocket" | "content-length" | "ndjson" => Ok(()),
            "mixed-bt" | "administrative" if !self.diagnostic_small => Ok(()),
            "mixed-bt" | "administrative" => {
                Err("small diagnostics support only the four transport scenarios")
            }
            _ => Err("unknown benchmark scenario"),
        }
    }
}
