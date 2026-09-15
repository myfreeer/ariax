//! Self-contained, bounded JSON migration representation of verification.
use crate::{ContentChecksum, HttpControlError, VerificationManifest};
use ariax_storage::JournalDigest;
use serde::{
    Serialize, Serializer,
    ser::{SerializeSeq, SerializeStruct},
};
use serde_json::Value;
use std::sync::Arc;

pub(crate) struct VerificationView<'a> {
    pub manifest: &'a VerificationManifest,
    pub index: Option<u32>,
}
struct Digests<'a>(&'a [JournalDigest]);
impl Serialize for Digests<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for digest in self.0 {
            let checksum = ContentChecksum::try_from(digest).map_err(serde::ser::Error::custom)?;
            sequence.serialize_element(&checksum)?;
        }
        sequence.end()
    }
}
impl Serialize for VerificationView<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serializer.serialize_struct("Verification", 5)?;
        value.serialize_field("length", &self.manifest.total_length().to_string())?;
        value.serialize_field("chunkLength", &self.manifest.chunk_length().to_string())?;
        value.serialize_field("chunks", &Digests(self.manifest.chunks()))?;
        value.serialize_field("whole", &Digests(self.manifest.whole()))?;
        value.serialize_field("fileIndex", &self.index)?;
        value.end()
    }
}
pub(crate) fn parse_verification(
    value: &Value,
) -> Result<(Arc<VerificationManifest>, Option<u32>), HttpControlError> {
    let invalid =
        || HttpControlError::InvalidParams("invalid self-contained verification metadata");
    let object = value.as_object().ok_or_else(invalid)?;
    if object.keys().any(|key| {
        !["length", "chunkLength", "chunks", "whole", "fileIndex"].contains(&key.as_str())
    }) {
        return Err(invalid());
    }
    let integer = |name: &str| {
        object
            .get(name)
            .and_then(|value| {
                value
                    .as_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    .or_else(|| value.as_u64())
            })
            .ok_or_else(invalid)
    };
    let total = integer("length")?;
    let length = integer("chunkLength")?;
    if length == 0 || length > crate::MAX_HTTP_PIECE_LENGTH {
        return Err(invalid());
    }
    let digests = |name: &str, max: usize| -> Result<Vec<JournalDigest>, HttpControlError> {
        let values = object
            .get(name)
            .and_then(Value::as_array)
            .filter(|values| values.len() <= max)
            .ok_or_else(invalid)?;
        values
            .iter()
            .map(|value| {
                ContentChecksum::parse(value.as_str().ok_or_else(invalid)?)
                    .map(|checksum| checksum.journal_digest())
                    .map_err(|_| invalid())
            })
            .collect()
    };
    let chunks = digests("chunks", ariax_storage::MAX_VERIFICATION_CHUNKS)?;
    let whole = digests("whole", 2)?;
    let index = match object.get("fileIndex") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_u64()
                .filter(|value| (1..=262144).contains(value))
                .ok_or_else(invalid)? as u32,
        ),
    };
    Ok((
        Arc::new(VerificationManifest::new(total, length, chunks, whole).map_err(|_| invalid())?),
        index,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentHasher;
    use ariax_storage::JournalDigestAlgorithm;
    #[test]
    fn verification_round_trip_needs_no_xml_and_rejects_geometry() {
        let mut hash = ContentHasher::new(JournalDigestAlgorithm::Sha512);
        hash.update(b"abc");
        let manifest =
            VerificationManifest::new(3, 3, vec![hash.finalize().journal_digest()], vec![])
                .unwrap();
        let mut value = serde_json::to_value(VerificationView {
            manifest: &manifest,
            index: Some(2),
        })
        .unwrap();
        let (restored, index) = parse_verification(&value).unwrap();
        assert_eq!(*restored, manifest);
        assert_eq!(index, Some(2));
        value["length"] = Value::String("4".into());
        assert!(parse_verification(&value).is_err());
        value["chunkLength"] = Value::String("0".into());
        assert!(parse_verification(&value).is_err());
    }
}
