#![forbid(unsafe_code)]

//! Core identifiers and contracts shared by ariax components.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

/// The engine and command-line product name.
pub const ENGINE_NAME: &str = "ariax";

/// A stable aria2-compatible task identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Gid(NonZeroU64);

impl Gid {
    /// Creates a GID, rejecting the reserved zero value.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric GID value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for Gid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:016x}", self.get())
    }
}

impl FromStr for Gid {
    type Err = ParseGidError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() != 16 {
            return Err(ParseGidError::InvalidLength);
        }
        if !input.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ParseGidError::InvalidHex);
        }

        let value = u64::from_str_radix(input, 16).map_err(|_| ParseGidError::InvalidHex)?;
        Self::new(value).ok_or(ParseGidError::Zero)
    }
}

/// Why a full wire-format GID could not be parsed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseGidError {
    /// The input was not exactly 16 characters.
    InvalidLength,
    /// The input contained a non-hexadecimal character.
    InvalidHex,
    /// The input represented the reserved zero value.
    Zero,
}

impl fmt::Display for ParseGidError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidLength => "GID must contain exactly 16 hexadecimal digits",
            Self::InvalidHex => "GID contains a non-hexadecimal character",
            Self::Zero => "GID zero is reserved",
        };
        formatter.write_str(message)
    }
}

impl Error for ParseGidError {}

#[cfg(test)]
mod tests {
    use super::{Gid, ParseGidError};
    use std::str::FromStr;

    #[test]
    fn gid_round_trips_full_wire_value() {
        let gid = Gid::new(0xaf).expect("non-zero GID");
        assert_eq!(gid.to_string(), "00000000000000af");
        assert_eq!(Gid::from_str(&gid.to_string()), Ok(gid));
    }

    #[test]
    fn gid_round_trips_boundary_and_sample_values() {
        let values = [1, 2, 0xaf, u32::MAX.into(), 1_u64 << 63, u64::MAX];
        for value in values {
            let gid = Gid::new(value).expect("sample is non-zero");
            let wire = gid.to_string();
            assert_eq!(wire.len(), 16);
            assert_eq!(Gid::from_str(&wire), Ok(gid));
        }
    }

    #[test]
    fn uppercase_gid_input_normalizes_to_lowercase() {
        let gid = Gid::from_str("00000000000000AF").expect("valid uppercase GID");
        assert_eq!(gid.to_string(), "00000000000000af");
    }

    #[test]
    fn gid_rejects_wrong_length() {
        assert_eq!(Gid::from_str("af"), Err(ParseGidError::InvalidLength));
    }

    #[test]
    fn gid_rejects_non_hex_input() {
        assert_eq!(
            Gid::from_str("00000000000000x1"),
            Err(ParseGidError::InvalidHex)
        );
    }

    #[test]
    fn gid_rejects_zero() {
        assert_eq!(Gid::from_str("0000000000000000"), Err(ParseGidError::Zero));
    }
}
