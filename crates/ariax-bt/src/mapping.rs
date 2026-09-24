use crate::{BtError, TorrentMetadata};
use ariax_storage::{PathPlatform, SafePathBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MappingOptions {
    /// Aria2 file indexes are one-based; native indexes remain zero-based.
    pub selected: Option<BTreeSet<u32>>,
    pub index_out: BTreeMap<u32, String>,
    pub output: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMapping {
    pub index: u32,
    pub path: String,
    pub length: u64,
    pub offset: u64,
    pub selected: bool,
    pub padding: bool,
}

fn sanitize(value: &str) -> Result<String, BtError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains(['/', '\\'])
        || value.chars().any(char::is_control)
    {
        return Err(BtError::UnsafePath);
    }
    let mut result = value
        .chars()
        .map(|character| match character {
            ':' | '<' | '>' | '"' | '|' | '?' | '*' => '_',
            other => other,
        })
        .collect::<String>();
    result.truncate(result.trim_end_matches(['.', ' ']).len());
    if result.is_empty() {
        result.push('_');
    }
    if matches!(
        SafePathBuilder::from_metadata_components([&result], PathPlatform::Windows),
        Err(ariax_storage::PathValidationError::ReservedWindowsName)
    ) {
        result.insert(0, '_');
    }
    let safe = SafePathBuilder::from_metadata_components([result], PathPlatform::Windows)
        .map_err(|_| BtError::UnsafePath)?;
    let result = safe.canonical_string();
    if result.len() > 255 {
        return Err(BtError::UnsafePath);
    }
    Ok(result)
}

fn suffixed(value: &str, index: u32, attempt: usize) -> Result<String, BtError> {
    let suffix = if attempt == 0 {
        format!(".ariax-{}", index + 1)
    } else {
        format!(".ariax-{}-{attempt}", index + 1)
    };
    let mut end = value.len().min(255usize.saturating_sub(suffix.len()));
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    sanitize(&format!("{}{suffix}", &value[..end]))
}

pub fn map_files(
    metadata: &TorrentMetadata,
    options: &MappingOptions,
) -> Result<Vec<FileMapping>, BtError> {
    let file_count = metadata.files.len();
    let real_count = metadata.files.iter().filter(|file| !file.padding).count();
    if options.output.is_some() && real_count != 1 {
        return Err(BtError::Selection);
    }
    let valid_index = |index: &u32| {
        *index > 0
            && metadata
                .files
                .get((*index - 1) as usize)
                .is_some_and(|file| !file.padding)
    };
    if options.selected.as_ref().is_some_and(|selected| {
        selected.is_empty() || selected.iter().any(|index| !valid_index(index))
    }) || options.index_out.keys().any(|index| !valid_index(index))
    {
        return Err(BtError::Selection);
    }
    let mut files = BTreeSet::new();
    let mut directories = BTreeSet::new();
    let mut mapping = Vec::with_capacity(file_count);
    for file in &metadata.files {
        if file.index as usize != mapping.len() {
            return Err(BtError::InvalidMetadata);
        }
        if file.padding {
            mapping.push(FileMapping {
                index: file.index,
                path: format!(".ariax-padding/{}", file.index),
                length: file.length,
                offset: file.offset,
                selected: false,
                padding: true,
            });
            continue;
        }
        let override_path = options
            .index_out
            .get(&(file.index + 1))
            .or(options.output.as_ref());
        let mut parts = if let Some(path) = override_path {
            let safe = SafePathBuilder::from_user_path(path, PathPlatform::Windows)
                .map_err(|_| BtError::UnsafePath)?;
            safe.components().map(str::to_owned).collect::<Vec<_>>()
        } else {
            file.components
                .iter()
                .map(|part| sanitize(part))
                .collect::<Result<Vec<_>, _>>()?
        };
        let base = parts.clone();
        let mut attempts = vec![0usize; parts.len()];
        for _ in 0..=file_count.saturating_mul(parts.len()) {
            let key = parts.join("/").to_lowercase();
            let collision = (0..parts.len())
                .find(|end| files.contains(&parts[..=*end].join("/").to_lowercase()))
                .or_else(|| directories.contains(&key).then_some(parts.len() - 1));
            let Some(component) = collision else {
                let safe = SafePathBuilder::from_metadata_components(&parts, PathPlatform::Windows)
                    .map_err(|_| BtError::UnsafePath)?;
                for end in 1..parts.len() {
                    directories.insert(parts[..end].join("/").to_lowercase());
                }
                files.insert(key);
                mapping.push(FileMapping {
                    index: file.index,
                    path: safe.canonical_string(),
                    length: file.length,
                    offset: file.offset,
                    selected: options
                        .selected
                        .as_ref()
                        .is_none_or(|selected| selected.contains(&(file.index + 1))),
                    padding: false,
                });
                break;
            };
            parts[component] = suffixed(&base[component], file.index, attempts[component])?;
            attempts[component] += 1;
        }
        if mapping.len() != file.index as usize + 1 {
            return Err(BtError::Collision);
        }
    }
    Ok(mapping)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BtIdentity, MetadataFile};

    fn metadata(names: &[&str]) -> TorrentMetadata {
        TorrentMetadata {
            identity: BtIdentity {
                v1: Some("a".repeat(40)),
                v2: None,
            },
            name: "bundle".into(),
            files: names
                .iter()
                .enumerate()
                .map(|(index, name)| MetadataFile {
                    index: index as u32,
                    components: std::iter::once("bundle")
                        .chain(name.split('/'))
                        .map(str::to_owned)
                        .collect(),
                    length: 1,
                    offset: index as u64,
                    padding: false,
                })
                .collect(),
            piece_length: 16384,
            pieces: 1,
            total_length: names.len() as u64,
            private: false,
            trackers: Vec::new(),
            web_seeds: Vec::new(),
        }
    }

    #[test]
    fn collisions_reserved_names_and_prefixes_are_stable_across_selection() {
        let metadata = metadata(&["a", "A", "a/b", "CON", "space. ", "ques?tion", "ques*tion"]);
        let full = map_files(&metadata, &MappingOptions::default()).unwrap();
        assert_eq!(full[0].path, "bundle/a");
        assert_eq!(full[1].path, "bundle/A.ariax-2");
        assert_eq!(full[2].path, "bundle/a.ariax-3/b");
        assert_eq!(full[3].path, "bundle/_CON");
        assert_eq!(full[4].path, "bundle/space");
        for selected in 1..=metadata.files.len() as u32 {
            let options = MappingOptions {
                selected: Some(BTreeSet::from([selected])),
                ..MappingOptions::default()
            };
            let subset = map_files(&metadata, &options).unwrap();
            assert_eq!(
                full.iter().map(|file| &file.path).collect::<Vec<_>>(),
                subset.iter().map(|file| &file.path).collect::<Vec<_>>()
            );
            assert_eq!(subset.iter().filter(|file| file.selected).count(), 1);
        }
    }

    #[test]
    fn every_generated_mapping_is_unique_and_has_no_file_directory_alias() {
        for count in 1..=128 {
            let mut metadata = metadata(&["x"]);
            metadata.files = (0..count)
                .map(|index| MetadataFile {
                    index,
                    components: vec![
                        "bundle".into(),
                        if index % 2 == 0 {
                            "a".into()
                        } else {
                            "A".into()
                        },
                    ],
                    length: 1,
                    offset: u64::from(index),
                    padding: false,
                })
                .collect();
            let mapping = map_files(&metadata, &MappingOptions::default()).unwrap();
            let paths = mapping
                .iter()
                .map(|file| file.path.to_lowercase())
                .collect::<BTreeSet<_>>();
            assert_eq!(paths.len(), count as usize);
            for path in &paths {
                assert!(
                    !paths
                        .iter()
                        .any(|other| other.starts_with(&(path.clone() + "/")))
                );
            }
            assert_eq!(
                mapping,
                map_files(&metadata, &MappingOptions::default()).unwrap()
            );
        }
    }

    #[test]
    fn unsafe_paths_and_invalid_selection_do_not_publish_a_mapping_prefix() {
        for path in ["../escape", ".", "a/../b", "bad\0name", "\u{202e}txt"] {
            assert_eq!(
                map_files(&metadata(&[path]), &MappingOptions::default()),
                Err(BtError::UnsafePath)
            );
        }
        let options = MappingOptions {
            selected: Some(BTreeSet::from([0])),
            ..MappingOptions::default()
        };
        assert_eq!(
            map_files(&metadata(&["a"]), &options),
            Err(BtError::Selection)
        );
        let options = MappingOptions {
            index_out: BTreeMap::from([(1, "../escape".into())]),
            ..MappingOptions::default()
        };
        assert_eq!(
            map_files(&metadata(&["a"]), &options),
            Err(BtError::UnsafePath)
        );
    }
}
