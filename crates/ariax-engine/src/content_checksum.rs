//! Bounded, protocol-independent content checksums and cloneable streaming states.

use ariax_storage::{JournalDigest, JournalDigestAlgorithm};
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};
use std::error::Error;
use std::fmt;

pub const MAX_CONTENT_CHECKSUM_TEXT_BYTES: usize = 136;
pub const CONTENT_IDENTITY_DOMAIN: &[u8] = b"ariax/content-identity/v1\0";

/// A checksum of the exact declared byte span, never a digest of lease digests.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ContentChecksum {
    Md5([u8; 16]),
    Sha1([u8; 20]),
    Sha256([u8; 32]),
    Sha512([u8; 64]),
}

impl ContentChecksum {
    pub fn parse(text: &str) -> Result<Self, ContentChecksumError> {
        if text.len() > MAX_CONTENT_CHECKSUM_TEXT_BYTES {
            return Err(ContentChecksumError::InvalidLength);
        }
        let (algorithm, value) = text
            .split_once('=')
            .ok_or(ContentChecksumError::InvalidFormat)?;
        let algorithm = JournalDigestAlgorithm::try_from(algorithm)
            .map_err(|_| ContentChecksumError::UnsupportedAlgorithm)?;
        Self::from_hex(algorithm, value)
    }

    pub fn from_hex(
        algorithm: JournalDigestAlgorithm,
        text: &str,
    ) -> Result<Self, ContentChecksumError> {
        if text.len() != algorithm.value_len() * 2 {
            return Err(ContentChecksumError::InvalidLength);
        }
        let mut bytes = [0_u8; 64];
        for (target, pair) in bytes.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
            let high = hex(pair[0]).ok_or(ContentChecksumError::InvalidHex)?;
            let low = hex(pair[1]).ok_or(ContentChecksumError::InvalidHex)?;
            *target = high << 4 | low;
        }
        Self::from_bytes(algorithm, &bytes[..algorithm.value_len()])
    }

    pub fn from_bytes(
        algorithm: JournalDigestAlgorithm,
        bytes: &[u8],
    ) -> Result<Self, ContentChecksumError> {
        fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], ContentChecksumError> {
            bytes
                .try_into()
                .map_err(|_| ContentChecksumError::InvalidLength)
        }
        Ok(match algorithm {
            JournalDigestAlgorithm::Md5 => Self::Md5(array(bytes)?),
            JournalDigestAlgorithm::Sha1 => Self::Sha1(array(bytes)?),
            JournalDigestAlgorithm::Sha256 => Self::Sha256(array(bytes)?),
            JournalDigestAlgorithm::Sha512 => Self::Sha512(array(bytes)?),
        })
    }

    #[must_use]
    pub const fn algorithm(self) -> JournalDigestAlgorithm {
        match self {
            Self::Md5(_) => JournalDigestAlgorithm::Md5,
            Self::Sha1(_) => JournalDigestAlgorithm::Sha1,
            Self::Sha256(_) => JournalDigestAlgorithm::Sha256,
            Self::Sha512(_) => JournalDigestAlgorithm::Sha512,
        }
    }

    #[must_use]
    pub const fn value(&self) -> &[u8] {
        match self {
            Self::Md5(bytes) => bytes,
            Self::Sha1(bytes) => bytes,
            Self::Sha256(bytes) => bytes,
            Self::Sha512(bytes) => bytes,
        }
    }

    /// Only these algorithms may authorize strict concurrent cross-origin work.
    #[must_use]
    pub const fn proves_strict_identity(self) -> bool {
        matches!(self, Self::Sha256(_) | Self::Sha512(_))
    }

    #[must_use]
    pub fn canonical(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut text =
            String::with_capacity(self.algorithm().code().len() + 1 + self.value().len() * 2);
        text.push_str(self.algorithm().code());
        text.push('=');
        for byte in self.value() {
            text.push(char::from(HEX[usize::from(byte >> 4)]));
            text.push(char::from(HEX[usize::from(byte & 15)]));
        }
        text
    }

    #[must_use]
    pub fn journal_digest(self) -> JournalDigest {
        JournalDigest::new(self.algorithm(), self.value().to_vec())
            .expect("content checksum has an exact digest length")
    }

    /// Domain separation and an exact length prevent using a range digest as
    /// evidence for a different span or representation.
    #[must_use]
    pub fn identity_fingerprint(self, length: u64) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(CONTENT_IDENTITY_DOMAIN);
        digest.update(length.to_le_bytes());
        digest.update((self.algorithm().code().len() as u32).to_le_bytes());
        digest.update(self.algorithm().code().as_bytes());
        digest.update(self.value());
        digest.finalize().into()
    }
}

impl fmt::Debug for ContentChecksum {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Expected checksums are public metadata, not authentication secrets.
        formatter
            .debug_tuple("ContentChecksum")
            .field(&self.canonical())
            .finish()
    }
}

impl From<crate::HttpContentChecksum> for ContentChecksum {
    fn from(value: crate::HttpContentChecksum) -> Self {
        Self::Sha256(value.value())
    }
}

impl TryFrom<ContentChecksum> for crate::HttpContentChecksum {
    type Error = ContentChecksumError;

    fn try_from(value: ContentChecksum) -> Result<Self, Self::Error> {
        match value {
            ContentChecksum::Sha256(bytes) => Ok(Self::sha256(bytes)),
            _ => Err(ContentChecksumError::UnsupportedAlgorithm),
        }
    }
}

impl TryFrom<&JournalDigest> for ContentChecksum {
    type Error = ContentChecksumError;

    fn try_from(value: &JournalDigest) -> Result<Self, Self::Error> {
        Self::from_bytes(value.algorithm(), value.value())
    }
}

impl serde::Serialize for ContentChecksum {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.canonical())
    }
}

impl<'de> serde::Deserialize<'de> for ContentChecksum {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = ContentChecksum;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded TYPE=hex content checksum")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                ContentChecksum::parse(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

/// Clone is a bounded digest checkpoint; payload buffers are never cloned.
#[derive(Clone)]
pub enum ContentHasher {
    Md5(Md5),
    Sha1(Sha1),
    Sha256(Sha256),
    Sha512(Sha512),
}

impl ContentHasher {
    #[must_use]
    pub fn new(algorithm: JournalDigestAlgorithm) -> Self {
        match algorithm {
            JournalDigestAlgorithm::Md5 => Self::Md5(Md5::new()),
            JournalDigestAlgorithm::Sha1 => Self::Sha1(Sha1::new()),
            JournalDigestAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
            JournalDigestAlgorithm::Sha512 => Self::Sha512(Sha512::new()),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Md5(state) => state.update(bytes),
            Self::Sha1(state) => state.update(bytes),
            Self::Sha256(state) => state.update(bytes),
            Self::Sha512(state) => state.update(bytes),
        }
    }

    #[must_use]
    pub fn finalize(self) -> ContentChecksum {
        match self {
            Self::Md5(state) => ContentChecksum::Md5(state.finalize().into()),
            Self::Sha1(state) => ContentChecksum::Sha1(state.finalize().into()),
            Self::Sha256(state) => ContentChecksum::Sha256(state.finalize().into()),
            Self::Sha512(state) => ContentChecksum::Sha512(state.finalize().into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentChecksumError {
    InvalidFormat,
    UnsupportedAlgorithm,
    InvalidLength,
    InvalidHex,
}

impl fmt::Display for ContentChecksumError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidFormat => "checksum must use TYPE=hex syntax",
            Self::UnsupportedAlgorithm => "unsupported content checksum algorithm",
            Self::InvalidLength => "content checksum has the wrong length",
            Self::InvalidHex => "content checksum is not hexadecimal",
        })
    }
}

impl Error for ContentChecksumError {}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ABC: &[&str] = &[
        "md5=900150983cd24fb0d6963f7d28e17f72",
        "sha-1=a9993e364706816aba3e25717850c26c9cd0d89d",
        "sha-256=ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "sha-512=ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
    ];

    #[test]
    fn published_vectors_hold_for_every_stream_partition_and_checkpoint() {
        for text in ABC {
            let expected = ContentChecksum::parse(text).expect("known vector");
            for first in 0..=3 {
                for second in first..=3 {
                    let mut state = ContentHasher::new(expected.algorithm());
                    state.update(&b"abc"[..first]);
                    let checkpoint = state.clone();
                    state.update(b"failed attempt");
                    assert_ne!(state.finalize(), expected);
                    let mut state = checkpoint;
                    state.update(&b"abc"[first..second]);
                    state.update(&b"abc"[second..]);
                    assert_eq!(state.finalize(), expected);
                }
            }
            assert_eq!(expected.canonical(), *text);
            assert_eq!(
                ContentChecksum::try_from(&expected.journal_digest()),
                Ok(expected)
            );
            let json = serde_json::to_string(&expected).expect("serialize");
            assert_eq!(
                serde_json::from_str::<ContentChecksum>(&json).expect("parse"),
                expected
            );
        }
    }

    #[test]
    fn algorithm_lengths_and_invalid_hex_are_rejected_without_echoing_input() {
        for algorithm in JournalDigestAlgorithm::ALL {
            let expected = algorithm.value_len() * 2;
            for length in 0..=130 {
                let text = "A".repeat(length);
                let result = ContentChecksum::from_hex(algorithm, &text);
                assert_eq!(result.is_ok(), length == expected);
                if let Ok(value) = result {
                    assert_eq!(value.value(), vec![0xaa; algorithm.value_len()]);
                }
            }
            for index in 0..expected {
                let mut bytes = vec![b'0'; expected];
                bytes[index] = b'!';
                assert_eq!(
                    ContentChecksum::from_hex(
                        algorithm,
                        std::str::from_utf8(&bytes).expect("ASCII")
                    ),
                    Err(ContentChecksumError::InvalidHex)
                );
            }
        }
        for invalid in [
            "sha-256",
            "blake3=private-canary",
            "SHA-256=00",
            "md5==",
            "md5=私",
        ] {
            let error = ContentChecksum::parse(invalid).expect_err("invalid");
            assert!(!error.to_string().contains("private-canary"));
        }
    }

    #[test]
    fn strict_identity_is_algorithm_and_length_bound_and_legacy_sha256_survives() {
        for text in ABC {
            let value = ContentChecksum::parse(text).expect("vector");
            assert_eq!(
                value.proves_strict_identity(),
                matches!(
                    value.algorithm(),
                    JournalDigestAlgorithm::Sha256 | JournalDigestAlgorithm::Sha512
                )
            );
            assert_ne!(value.identity_fingerprint(3), value.identity_fingerprint(4));
            let legacy = crate::HttpContentChecksum::try_from(value);
            assert_eq!(
                legacy.is_ok(),
                value.algorithm() == JournalDigestAlgorithm::Sha256
            );
            if let Ok(legacy) = legacy {
                assert_eq!(ContentChecksum::from(legacy), value);
            }
        }
    }
}
