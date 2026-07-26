use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

macro_rules! nonzero_id {
    ($(#[$meta:meta])* $name:ident, $description:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(NonZeroU64);

        impl $name {
            #[doc = concat!("Creates ", $description, ", rejecting zero.")]
            #[must_use]
            pub const fn new(value: u64) -> Option<Self> {
                match NonZeroU64::new(value) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }

            #[doc = concat!("Returns the numeric value of ", $description, ".")]
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }
    };
}

nonzero_id!(TaskId, "an internal task identifier");
nonzero_id!(
    TransferAttemptId,
    "a protocol response or data-stream attempt identifier"
);
nonzero_id!(LeaseId, "a provisional storage lease identifier");
nonzero_id!(OverlapGroupId, "an endgame overlap-group identifier");
nonzero_id!(BufferId, "a pooled buffer identifier");
nonzero_id!(OptionPatchId, "an atomic option-patch identifier");

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

/// A normalized one-to-sixteen-digit aria2 GID lookup prefix.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct GidPrefix {
    value: u64,
    digits: u8,
}

impl GidPrefix {
    /// Returns the number of hexadecimal digits in the prefix.
    #[must_use]
    pub const fn digits(self) -> u8 {
        self.digits
    }

    /// Returns whether this prefix matches the high-order wire digits of `gid`.
    #[must_use]
    pub const fn matches(self, gid: Gid) -> bool {
        let shift = (16 - self.digits as u32) * 4;
        gid.get() >> shift == self.value
    }
}

impl fmt::Display for GidPrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:01$x}", self.value, self.digits as usize)
    }
}

impl FromStr for GidPrefix {
    type Err = ParseGidPrefixError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() || input.len() > 16 {
            return Err(ParseGidPrefixError::InvalidLength);
        }
        if !input.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ParseGidPrefixError::InvalidHex);
        }
        let value = u64::from_str_radix(input, 16).map_err(|_| ParseGidPrefixError::InvalidHex)?;
        if value == 0 {
            return Err(ParseGidPrefixError::Zero);
        }
        Ok(Self {
            value,
            digits: input.len() as u8,
        })
    }
}

/// Why a GID lookup prefix could not be parsed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseGidPrefixError {
    /// The prefix was empty or longer than the full wire form.
    InvalidLength,
    /// The prefix contained a non-hexadecimal character.
    InvalidHex,
    /// The prefix represented only the reserved zero value.
    Zero,
}

impl fmt::Display for ParseGidPrefixError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidLength => "GID prefix must contain one to sixteen hexadecimal digits",
            Self::InvalidHex => "GID prefix contains a non-hexadecimal character",
            Self::Zero => "GID prefix zero is reserved",
        };
        formatter.write_str(message)
    }
}

impl Error for ParseGidPrefixError {}

/// Why a syntactically valid GID prefix did not resolve to one task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GidLookupError {
    /// No live task or retained stopped result matched the prefix.
    NotFound,
    /// More than one GID matched the prefix.
    Ambiguous,
}

impl fmt::Display for GidLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotFound => "GID prefix did not match a task",
            Self::Ambiguous => "GID prefix matched more than one task",
        })
    }
}

impl Error for GidLookupError {}

/// Resolves one parsed prefix against live and retained GIDs.
pub fn resolve_gid_prefix(
    prefix: GidPrefix,
    gids: impl IntoIterator<Item = Gid>,
) -> Result<Gid, GidLookupError> {
    let mut matched = None;
    for gid in gids {
        if !prefix.matches(gid) {
            continue;
        }
        match matched {
            None => matched = Some(gid),
            Some(existing) if existing == gid => {}
            Some(_) => return Err(GidLookupError::Ambiguous),
        }
    }
    matched.ok_or(GidLookupError::NotFound)
}

/// A task worker generation. Generation zero is the initial generation.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Generation(u64);

impl Generation {
    /// The initial task generation.
    pub const INITIAL: Self = Self(0);

    /// Creates a generation from its persisted value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the persisted generation value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next generation, or `None` on numeric exhaustion.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

macro_rules! plain_id {
    ($(#[$meta:meta])* $name:ident, $value:ty) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($value);

        impl $name {
            /// Creates the identifier from its canonical integer value.
            #[must_use]
            pub const fn new(value: $value) -> Self {
                Self(value)
            }

            /// Returns the canonical integer value.
            #[must_use]
            pub const fn get(self) -> $value {
                self.0
            }
        }
    };
}

plain_id!(/// A durability or verification piece index.
    PieceId, u64);
plain_id!(/// A file index in an immutable layout.
    FileId, u32);
plain_id!(/// A source URI index in a task source list.
    UriId, u32);

/// A persisted challenge identifier for explicit SFTP host-key approval.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HostKeyChallengeId([u8; 16]);

impl HostKeyChallengeId {
    /// Creates an identifier from its exact persisted bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the exact persisted bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// A raw SHA-256 host-key fingerprint.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HostKeyFingerprint([u8; 32]);

impl HostKeyFingerprint {
    /// Creates a fingerprint from its exact SHA-256 bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact SHA-256 bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Generation, Gid, GidLookupError, GidPrefix, ParseGidError, ParseGidPrefixError,
        resolve_gid_prefix,
    };
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
    fn gid_rejects_wrong_length_non_hex_and_zero() {
        assert_eq!(Gid::from_str("af"), Err(ParseGidError::InvalidLength));
        assert_eq!(
            Gid::from_str("00000000000000x1"),
            Err(ParseGidError::InvalidHex)
        );
        assert_eq!(Gid::from_str("0000000000000000"), Err(ParseGidError::Zero));
    }

    #[test]
    fn gid_prefix_normalizes_and_matches_high_wire_digits() {
        let prefix = GidPrefix::from_str("Ab").expect("valid prefix");
        let gid = Gid::new(0xab00_0000_0000_0001).expect("non-zero GID");
        assert_eq!(prefix.to_string(), "ab");
        assert_eq!(prefix.digits(), 2);
        assert!(prefix.matches(gid));
        assert!(!prefix.matches(Gid::new(0xac00_0000_0000_0001).expect("GID")));

        let leading_zero = GidPrefix::from_str("000a").expect("non-zero prefix");
        assert_eq!(leading_zero.to_string(), "000a");
        assert!(
            leading_zero.matches(Gid::from_str("000a000000000001").expect("matching full GID"))
        );
    }

    #[test]
    fn gid_prefix_rejects_invalid_inputs() {
        assert_eq!(
            GidPrefix::from_str(""),
            Err(ParseGidPrefixError::InvalidLength)
        );
        assert_eq!(
            GidPrefix::from_str("00000000000000000"),
            Err(ParseGidPrefixError::InvalidLength)
        );
        assert_eq!(
            GidPrefix::from_str("xyz"),
            Err(ParseGidPrefixError::InvalidHex)
        );
        assert_eq!(GidPrefix::from_str("000"), Err(ParseGidPrefixError::Zero));
    }

    #[test]
    fn gid_prefix_resolution_is_unique_or_typed() {
        let one = Gid::new(0xab00_0000_0000_0001).expect("GID");
        let two = Gid::new(0xab00_0000_0000_0002).expect("GID");
        let other = Gid::new(0xcd00_0000_0000_0001).expect("GID");
        assert_eq!(
            resolve_gid_prefix(GidPrefix::from_str("cd").expect("prefix"), [one, other]),
            Ok(other)
        );
        assert_eq!(
            resolve_gid_prefix(GidPrefix::from_str("ab").expect("prefix"), [one, two]),
            Err(GidLookupError::Ambiguous)
        );
        assert_eq!(
            resolve_gid_prefix(GidPrefix::from_str("ef").expect("prefix"), [one, other]),
            Err(GidLookupError::NotFound)
        );
    }

    #[test]
    fn generation_advances_once_and_detects_exhaustion() {
        assert_eq!(Generation::INITIAL.checked_next(), Some(Generation::new(1)));
        assert_eq!(Generation::new(u64::MAX).checked_next(), None);
    }
}
