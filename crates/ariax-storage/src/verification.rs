use crate::{JournalDigest, JournalDigestAlgorithm, JournalHash, JournalPayload, PersistedSpan};
use sha2::{Digest, Sha256};
use std::{error::Error, fmt};

pub const MAX_VERIFICATION_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_VERIFICATION_CHUNKS: usize = 1_048_576;
pub const VERIFICATION_MANIFEST_PART_BYTES: usize = 64 * 1024;
pub const VERIFICATION_MANIFEST_DOMAIN: &[u8] = b"ariax/verification-manifest/v1\0";

/// The complete immutable checksum requirement for one fixed-length output.
/// Piece digests use one strongest complete algorithm set; whole-file digests
/// include the independent user requirement when supplied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationManifest {
    total_length: u64,
    chunk_length: u64,
    chunks: Box<[JournalDigest]>,
    whole: Box<[JournalDigest]>,
    fingerprint: JournalHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationManifestError {
    InvalidGeometry,
    InvalidDigestSet,
    Limit,
    Malformed,
}

impl fmt::Display for VerificationManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidGeometry => "invalid verification geometry",
            Self::InvalidDigestSet => "invalid verification digest set",
            Self::Limit => "verification metadata exceeds its bound",
            Self::Malformed => "malformed verification manifest",
        })
    }
}
impl Error for VerificationManifestError {}

impl VerificationManifest {
    pub fn new(
        total_length: u64,
        chunk_length: u64,
        chunks: Vec<JournalDigest>,
        whole: Vec<JournalDigest>,
    ) -> Result<Self, VerificationManifestError> {
        if chunk_length == 0
            || (!chunks.is_empty() && total_length.div_ceil(chunk_length) != chunks.len() as u64)
        {
            return Err(VerificationManifestError::InvalidGeometry);
        }
        if chunks.len() > MAX_VERIFICATION_CHUNKS
            || whole.len() > 2
            || chunks
                .iter()
                .chain(&whole)
                .map(|digest| digest.value().len() + 48)
                .sum::<usize>()
                > MAX_VERIFICATION_MANIFEST_BYTES
        {
            return Err(VerificationManifestError::Limit);
        }
        if chunks
            .windows(2)
            .any(|pair| pair[0].algorithm() != pair[1].algorithm())
            || (whole.len() == 2
                && whole[0].algorithm() == whole[1].algorithm()
                && whole[0] != whole[1])
        {
            return Err(VerificationManifestError::InvalidDigestSet);
        }
        let mut manifest = Self {
            total_length,
            chunk_length,
            chunks: chunks.into_boxed_slice(),
            whole: whole.into_boxed_slice(),
            fingerprint: JournalHash::new([1; 32]).expect("nonzero placeholder"),
        };
        manifest.fingerprint = Self::hash_bytes(&manifest.encode());
        Ok(manifest)
    }

    pub const fn total_length(&self) -> u64 {
        self.total_length
    }
    pub const fn chunk_length(&self) -> u64 {
        self.chunk_length
    }
    pub fn chunks(&self) -> &[JournalDigest] {
        &self.chunks
    }
    pub fn whole(&self) -> &[JournalDigest] {
        &self.whole
    }
    pub const fn fingerprint(&self) -> JournalHash {
        self.fingerprint
    }
    pub fn retained_bytes(&self) -> usize {
        128 + self
            .chunks
            .iter()
            .chain(self.whole.iter())
            .map(|d| d.value().len() + 48)
            .sum::<usize>()
    }
    pub fn proves_strict_identity(&self) -> bool {
        let strong = |d: &JournalDigest| {
            matches!(
                d.algorithm(),
                JournalDigestAlgorithm::Sha256 | JournalDigestAlgorithm::Sha512
            )
        };
        self.whole.iter().any(strong) || (!self.chunks.is_empty() && self.chunks.iter().all(strong))
    }
    pub fn chunk_span(&self, index: u64) -> Option<PersistedSpan> {
        let start = index.checked_mul(self.chunk_length)?;
        let len = self.chunk_length.min(self.total_length.checked_sub(start)?);
        PersistedSpan::new(start, len).ok()
    }

    pub fn hash_bytes(bytes: &[u8]) -> JournalHash {
        let mut hash = Sha256::new();
        hash.update(VERIFICATION_MANIFEST_DOMAIN);
        hash.update(bytes);
        JournalHash::new(hash.finalize().into()).expect("SHA-256 is nonzero")
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            22 + self
                .chunks
                .iter()
                .chain(self.whole.iter())
                .map(|digest| 1 + digest.value().len())
                .sum::<usize>(),
        );
        bytes.push(1);
        bytes.extend_from_slice(&self.total_length.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_length.to_le_bytes());
        bytes.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes());
        bytes.push(self.whole.len() as u8);
        for digest in self.chunks.iter().chain(self.whole.iter()) {
            bytes.push(algorithm_tag(digest.algorithm()));
            bytes.extend_from_slice(digest.value());
        }
        bytes
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self, VerificationManifestError> {
        use VerificationManifestError::{Limit, Malformed};
        if bytes.len() > MAX_VERIFICATION_MANIFEST_BYTES {
            return Err(Limit);
        }
        fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8], VerificationManifestError> {
            if bytes.len() < n {
                return Err(Malformed);
            }
            let (value, tail) = bytes.split_at(n);
            *bytes = tail;
            Ok(value)
        }
        if take(&mut bytes, 1)? != [1] {
            return Err(Malformed);
        }
        let total = u64::from_le_bytes(take(&mut bytes, 8)?.try_into().map_err(|_| Malformed)?);
        let length = u64::from_le_bytes(take(&mut bytes, 8)?.try_into().map_err(|_| Malformed)?);
        let count =
            u32::from_le_bytes(take(&mut bytes, 4)?.try_into().map_err(|_| Malformed)?) as usize;
        let whole_count = usize::from(take(&mut bytes, 1)?[0]);
        if count > MAX_VERIFICATION_CHUNKS || whole_count > 2 {
            return Err(Limit);
        }
        if (count + whole_count).saturating_mul(17) > bytes.len() {
            return Err(Malformed);
        }
        let mut chunks = Vec::new();
        let mut whole = Vec::new();
        chunks.try_reserve_exact(count).map_err(|_| Limit)?;
        for index in 0..count + whole_count {
            let algorithm = match take(&mut bytes, 1)?[0] {
                1 => JournalDigestAlgorithm::Md5,
                2 => JournalDigestAlgorithm::Sha1,
                3 => JournalDigestAlgorithm::Sha256,
                4 => JournalDigestAlgorithm::Sha512,
                _ => return Err(Malformed),
            };
            let digest = JournalDigest::new(algorithm, take(&mut bytes, algorithm.value_len())?)
                .map_err(|_| Malformed)?;
            if index < count {
                chunks.push(digest);
            } else {
                whole.push(digest);
            }
        }
        if !bytes.is_empty() {
            return Err(Malformed);
        }
        Self::new(total, length, chunks, whole)
    }

    /// Every continuation remains well below the v1 record size cap.
    pub fn journal_payloads(&self) -> Vec<JournalPayload> {
        let bytes = self.encode();
        let count = bytes.len().div_ceil(VERIFICATION_MANIFEST_PART_BYTES) as u32;
        bytes
            .chunks(VERIFICATION_MANIFEST_PART_BYTES)
            .enumerate()
            .map(|(index, part)| {
                if index == 0 {
                    JournalPayload::VerificationManifest {
                        fingerprint: self.fingerprint,
                        total_bytes: bytes.len() as u32,
                        chunk_count: count,
                        bytes: part.into(),
                    }
                } else {
                    JournalPayload::VerificationManifestChunk {
                        fingerprint: self.fingerprint,
                        chunk_index: index as u32,
                        chunk_count: count,
                        bytes: part.into(),
                    }
                }
            })
            .collect()
    }
}

pub(crate) const fn algorithm_tag(algorithm: JournalDigestAlgorithm) -> u8 {
    match algorithm {
        JournalDigestAlgorithm::Md5 => 1,
        JournalDigestAlgorithm::Sha1 => 2,
        JournalDigestAlgorithm::Sha256 => 3,
        JournalDigestAlgorithm::Sha512 => 4,
    }
}

/// A source-local resume validator. Timestamp equality never proves content
/// identity across different sources. SFTP additionally binds the host key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolValidator {
    pub protocol: u8,
    pub source: JournalHash,
    pub total_length: u64,
    pub modified_unix_seconds: Option<u64>,
    pub host_key: Option<JournalHash>,
}

impl ProtocolValidator {
    pub fn validate(&self) -> bool {
        matches!(self.protocol, 1..=3) && (self.protocol == 3) == self.host_key.is_some()
    }
    pub fn fingerprint(&self) -> JournalHash {
        let mut hash = Sha256::new();
        hash.update(b"ariax/protocol-validator/v1\0");
        hash.update([self.protocol]);
        hash.update(self.source.as_bytes());
        hash.update(self.total_length.to_le_bytes());
        hash.update([u8::from(self.modified_unix_seconds.is_some())]);
        hash.update(self.modified_unix_seconds.unwrap_or(0).to_le_bytes());
        hash.update(self.host_key.map_or([0; 32], |key| *key.as_bytes()));
        JournalHash::new(hash.finalize().into()).expect("SHA-256 is nonzero")
    }
    pub fn permits_resume(&self, previous: &Self, has_checksum: bool) -> bool {
        self == previous && (self.modified_unix_seconds.is_some() || has_checksum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn digest(algorithm: JournalDigestAlgorithm) -> JournalDigest {
        JournalDigest::new(algorithm, vec![7; algorithm.value_len()]).unwrap()
    }
    #[test]
    fn manifest_is_canonical_geometry_bound_and_fails_closed() {
        for algorithm in JournalDigestAlgorithm::ALL {
            let value = VerificationManifest::new(
                5,
                3,
                vec![digest(algorithm); 2],
                vec![digest(algorithm)],
            )
            .unwrap();
            assert_eq!(
                VerificationManifest::decode(&value.encode()).unwrap(),
                value
            );
            assert_eq!(value.chunk_span(1).unwrap().len(), 2);
            assert_eq!(
                value.proves_strict_identity(),
                matches!(
                    algorithm,
                    JournalDigestAlgorithm::Sha256 | JournalDigestAlgorithm::Sha512
                )
            );
            for cut in 0..value.encode().len() {
                assert!(VerificationManifest::decode(&value.encode()[..cut]).is_err());
            }
            let mut trailing = value.encode();
            trailing.push(0);
            assert!(VerificationManifest::decode(&trailing).is_err());
        }
        assert!(VerificationManifest::new(5, 0, vec![], vec![]).is_err());
        assert!(
            VerificationManifest::new(5, 3, vec![digest(JournalDigestAlgorithm::Sha256)], vec![])
                .is_err()
        );
        assert!(
            VerificationManifest::new(
                5,
                3,
                vec![
                    digest(JournalDigestAlgorithm::Sha256),
                    digest(JournalDigestAlgorithm::Md5)
                ],
                vec![]
            )
            .is_err()
        );
    }
    #[test]
    fn continuation_parts_and_resume_identity_are_bounded() {
        let manifest = VerificationManifest::new(
            2100,
            1,
            vec![digest(JournalDigestAlgorithm::Sha256); 2100],
            vec![],
        )
        .unwrap();
        assert_eq!(manifest.journal_payloads().len(), 2);
        let mut previous = ProtocolValidator {
            protocol: 3,
            source: JournalHash::new([3; 32]).unwrap(),
            total_length: 5,
            modified_unix_seconds: None,
            host_key: Some(JournalHash::new([4; 32]).unwrap()),
        };
        assert!(previous.validate());
        assert!(!previous.permits_resume(&previous, false));
        assert!(previous.permits_resume(&previous, true));
        let old = previous.clone();
        previous.modified_unix_seconds = Some(1);
        assert_ne!(old.fingerprint(), previous.fingerprint());
        assert!(!previous.permits_resume(&old, true));
    }
}
