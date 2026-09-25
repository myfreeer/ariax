//! Atomic, non-secret parent/child identity for followed metadata admission.
use crate::{JournalHash, SESSION_MAX_IMPORT_TASKS, SanitizedOptionMap};
use ariax_core::{Generation, Gid};
use std::collections::BTreeSet;
pub const METADATA_EXPANSION_OPTION: &str = "metadata-expansion";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataParent {
    pub gid: Gid,
    pub generation: Generation,
    pub snapshot_hash: JournalHash,
    pub document_hash: JournalHash,
    pub document_bytes: u64,
    pub retained: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataExpansion {
    pub parent: MetadataParent,
    pub children: Vec<Gid>,
}
impl MetadataExpansion {
    pub fn validate(&self) -> bool {
        !self.children.is_empty()
            && self.children.len() <= SESSION_MAX_IMPORT_TASKS
            && self.parent.document_bytes <= 256 * 1024 * 1024
            && !self.children.contains(&self.parent.gid)
            && self.children.iter().copied().collect::<BTreeSet<_>>().len() == self.children.len()
    }
    pub fn canonical(&self) -> String {
        let p = self.parent;
        format!(
            "1|{}|{}|{}|{}|{}|{}|{}",
            p.gid,
            p.generation.get(),
            p.snapshot_hash,
            p.document_hash,
            p.document_bytes,
            u8::from(p.retained),
            self.children
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
    pub fn parse(value: &str) -> Option<Self> {
        if value.len() > 64 * 1024 {
            return None;
        }
        let mut fields = value.split('|');
        if fields.next()? != "1" {
            return None;
        }
        let gid = fields.next()?.parse().ok()?;
        let generation = Generation::new(fields.next()?.parse().ok()?);
        let snapshot_hash = parse_hash(fields.next()?)?;
        let document_hash = parse_hash(fields.next()?)?;
        let document_bytes = fields.next()?.parse().ok()?;
        let retained = match fields.next()? {
            "0" => false,
            "1" => true,
            _ => return None,
        };
        let children = fields
            .next()?
            .split(',')
            .take(SESSION_MAX_IMPORT_TASKS + 1)
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        if fields.next().is_some() {
            return None;
        }
        let value = Self {
            parent: MetadataParent {
                gid,
                generation,
                snapshot_hash,
                document_hash,
                document_bytes,
                retained,
            },
            children,
        };
        value.validate().then_some(value)
    }
    pub fn with_options(
        &self,
        options: &SanitizedOptionMap,
    ) -> Result<SanitizedOptionMap, crate::PayloadCodecError> {
        SanitizedOptionMap::new(
            options
                .entries()
                .filter(|(name, _)| *name != METADATA_EXPANSION_OPTION)
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .chain([(METADATA_EXPANSION_OPTION.to_owned(), self.canonical())]),
        )
    }
}
fn parse_hash(text: &str) -> Option<JournalHash> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    JournalHash::new(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expansion_round_trip_rejects_duplicates_and_forged_shapes() {
        let value = MetadataExpansion {
            parent: MetadataParent {
                gid: Gid::new(1).unwrap(),
                generation: Generation::INITIAL,
                snapshot_hash: JournalHash::new([1; 32]).unwrap(),
                document_hash: JournalHash::new([2; 32]).unwrap(),
                document_bytes: 30,
                retained: false,
            },
            children: vec![Gid::new(2).unwrap()],
        };
        assert_eq!(
            MetadataExpansion::parse(&value.canonical()),
            Some(value.clone())
        );
        let mut bad = value.clone();
        bad.children.push(bad.children[0]);
        assert!(MetadataExpansion::parse(&bad.canonical()).is_none());
        bad.children = vec![value.parent.gid];
        assert!(MetadataExpansion::parse(&bad.canonical()).is_none());
        assert!(MetadataExpansion::parse(&(value.canonical() + "|extra")).is_none());
    }
}
