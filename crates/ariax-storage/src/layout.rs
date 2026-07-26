use crate::{FileIdentity, RootBinding, SafeRelativePath};
use ariax_core::{FileId, Generation, TaskId};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

pub const MAX_LAYOUT_ENTRIES: usize = 262_144;
pub const MAX_LAYOUT_BYTES: usize = 64 * 1024 * 1024;
pub const LAYOUT_HASH_DOMAIN: &str = "ariax/layout/v1\0";

/// SHA-256 over the domain-separated immutable relative layout and piece geometry.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LayoutHash([u8; 32]);

impl LayoutHash {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for LayoutHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LayoutHash({self})")
    }
}

impl fmt::Display for LayoutHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One immutable file placement inside a task-global byte layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileEntry {
    id: FileId,
    safe_path: SafeRelativePath,
    identity: Option<FileIdentity>,
    length: u64,
    global_start: u64,
    global_end: u64,
    selected: bool,
}

impl FileEntry {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        id: FileId,
        safe_path: SafeRelativePath,
        identity: Option<FileIdentity>,
        length: u64,
        global_start: u64,
        global_end: u64,
        selected: bool,
    ) -> Self {
        Self {
            id,
            safe_path,
            identity,
            length,
            global_start,
            global_end,
            selected,
        }
    }

    #[must_use]
    pub const fn id(&self) -> FileId {
        self.id
    }

    #[must_use]
    pub const fn safe_path(&self) -> &SafeRelativePath {
        &self.safe_path
    }

    #[must_use]
    pub const fn identity(&self) -> Option<&FileIdentity> {
        self.identity.as_ref()
    }

    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }

    #[must_use]
    pub const fn global_start(&self) -> u64 {
        self.global_start
    }

    #[must_use]
    pub const fn global_end(&self) -> u64 {
        self.global_end
    }

    #[must_use]
    pub const fn selected(&self) -> bool {
        self.selected
    }
}

/// An immutable, validated file map for one task generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileLayout {
    task: TaskId,
    generation: Generation,
    root_binding: RootBinding,
    files: Arc<[FileEntry]>,
    total_length: Option<u64>,
    piece_length: u64,
    layout_hash: LayoutHash,
    canonical_bytes: usize,
}

impl FileLayout {
    pub fn new(
        task: TaskId,
        generation: Generation,
        root_binding: RootBinding,
        files: Vec<FileEntry>,
        total_length: Option<u64>,
        piece_length: u64,
    ) -> Result<Self, LayoutError> {
        validate_layout(&root_binding, &files, total_length, piece_length)?;
        let canonical_bytes = canonical_size(&files)?;
        if canonical_bytes > MAX_LAYOUT_BYTES {
            return Err(LayoutError::CanonicalLayoutTooLarge);
        }
        let layout_hash = calculate_layout_hash(&files, total_length, piece_length);
        Ok(Self {
            task,
            generation,
            root_binding,
            files: files.into(),
            total_length,
            piece_length,
            layout_hash,
            canonical_bytes,
        })
    }

    #[must_use]
    pub const fn task(&self) -> TaskId {
        self.task
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn root_binding(&self) -> &RootBinding {
        &self.root_binding
    }

    #[must_use]
    pub fn files(&self) -> &[FileEntry] {
        &self.files
    }

    #[must_use]
    pub const fn total_length(&self) -> Option<u64> {
        self.total_length
    }

    #[must_use]
    pub const fn piece_length(&self) -> u64 {
        self.piece_length
    }

    #[must_use]
    pub const fn layout_hash(&self) -> LayoutHash {
        self.layout_hash
    }

    #[must_use]
    pub const fn canonical_bytes(&self) -> usize {
        self.canonical_bytes
    }
}

/// Why an immutable layout could not be admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutError {
    EmptyLayout,
    TooManyFiles,
    ZeroPieceLength,
    NonCanonicalFileId,
    NonContiguousFile,
    FileEndOverflow,
    FileEndMismatch,
    TotalLengthMismatch,
    UnknownLengthShape,
    SelectedFileMissingIdentity,
    UnselectedFileHasIdentity,
    RootBindingMismatch,
    PathCollision,
    CanonicalLayoutTooLarge,
}

impl LayoutError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::EmptyLayout => "empty_layout",
            Self::TooManyFiles => "too_many_files",
            Self::ZeroPieceLength => "zero_piece_length",
            Self::NonCanonicalFileId => "noncanonical_file_id",
            Self::NonContiguousFile => "noncontiguous_file",
            Self::FileEndOverflow => "file_end_overflow",
            Self::FileEndMismatch => "file_end_mismatch",
            Self::TotalLengthMismatch => "total_length_mismatch",
            Self::UnknownLengthShape => "unknown_length_shape",
            Self::SelectedFileMissingIdentity => "selected_file_missing_identity",
            Self::UnselectedFileHasIdentity => "unselected_file_has_identity",
            Self::RootBindingMismatch => "root_binding_mismatch",
            Self::PathCollision => "path_collision",
            Self::CanonicalLayoutTooLarge => "canonical_layout_too_large",
        }
    }
}

pub const ALL_LAYOUT_ERRORS: [LayoutError; 14] = [
    LayoutError::EmptyLayout,
    LayoutError::TooManyFiles,
    LayoutError::ZeroPieceLength,
    LayoutError::NonCanonicalFileId,
    LayoutError::NonContiguousFile,
    LayoutError::FileEndOverflow,
    LayoutError::FileEndMismatch,
    LayoutError::TotalLengthMismatch,
    LayoutError::UnknownLengthShape,
    LayoutError::SelectedFileMissingIdentity,
    LayoutError::UnselectedFileHasIdentity,
    LayoutError::RootBindingMismatch,
    LayoutError::PathCollision,
    LayoutError::CanonicalLayoutTooLarge,
];

impl fmt::Display for LayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyLayout => "file layout is empty",
            Self::TooManyFiles => "file layout exceeds 262144 entries",
            Self::ZeroPieceLength => "piece length must be nonzero",
            Self::NonCanonicalFileId => "file IDs must equal their canonical layout indexes",
            Self::NonContiguousFile => "file layout contains a gap or overlap",
            Self::FileEndOverflow => "file end overflows u64",
            Self::FileEndMismatch => "file end does not equal start plus length",
            Self::TotalLengthMismatch => "known total length does not match the layout end",
            Self::UnknownLengthShape => {
                "unknown total length requires one zero-placeholder selected file"
            }
            Self::SelectedFileMissingIdentity => {
                "selected file is missing opened identity evidence"
            }
            Self::UnselectedFileHasIdentity => "unselected file carries identity evidence",
            Self::RootBindingMismatch => "layout identities do not match the root binding",
            Self::PathCollision => "normalized file paths collide on the target platform",
            Self::CanonicalLayoutTooLarge => "canonical layout exceeds 64 MiB",
        })
    }
}

impl Error for LayoutError {}

/// A requested task-global byte span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GlobalSpan {
    pub offset: u64,
    pub len: usize,
}

/// One file-local mapping result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileSpan {
    pub file: FileId,
    pub file_offset: u64,
    pub len: usize,
}

/// Immutable mapper that never relies on a mutable file cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlobalOffsetMapper {
    files: Arc<[FileEntry]>,
    total_length: u64,
}

impl GlobalOffsetMapper {
    pub fn new(layout: &FileLayout) -> Result<Self, MapSpanError> {
        let total_length = layout
            .total_length
            .ok_or(MapSpanError::UnknownTotalLength)?;
        Ok(Self {
            files: Arc::clone(&layout.files),
            total_length,
        })
    }

    pub fn map(&self, span: GlobalSpan) -> Result<FileSpan, MapSpanError> {
        if span.len == 0 {
            return Err(MapSpanError::EmptySpan);
        }
        let len = u64::try_from(span.len).map_err(|_| MapSpanError::Overflow)?;
        let end = span.offset.checked_add(len).ok_or(MapSpanError::Overflow)?;
        if end > self.total_length {
            return Err(MapSpanError::OutsideLayout);
        }
        let file = self
            .files
            .iter()
            .find(|file| span.offset >= file.global_start && span.offset < file.global_end)
            .ok_or(MapSpanError::OutsideLayout)?;
        if end > file.global_end {
            return Err(MapSpanError::CrossFileSpan);
        }
        if !file.selected {
            return Err(MapSpanError::UnselectedFile);
        }
        Ok(FileSpan {
            file: file.id,
            file_offset: span.offset - file.global_start,
            len: span.len,
        })
    }
}

/// Why a global byte span could not map to one selected file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MapSpanError {
    UnknownTotalLength,
    EmptySpan,
    Overflow,
    OutsideLayout,
    CrossFileSpan,
    UnselectedFile,
}

impl MapSpanError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnknownTotalLength => "unknown_total_length",
            Self::EmptySpan => "empty_span",
            Self::Overflow => "overflow",
            Self::OutsideLayout => "outside_layout",
            Self::CrossFileSpan => "cross_file_span",
            Self::UnselectedFile => "unselected_file",
        }
    }
}

pub const ALL_MAP_SPAN_ERRORS: [MapSpanError; 6] = [
    MapSpanError::UnknownTotalLength,
    MapSpanError::EmptySpan,
    MapSpanError::Overflow,
    MapSpanError::OutsideLayout,
    MapSpanError::CrossFileSpan,
    MapSpanError::UnselectedFile,
];

impl fmt::Display for MapSpanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownTotalLength => "global offset mapping requires a known total length",
            Self::EmptySpan => "global span is empty",
            Self::Overflow => "global span end overflows u64",
            Self::OutsideLayout => "global span is outside the known layout",
            Self::CrossFileSpan => "global span crosses a file boundary",
            Self::UnselectedFile => "global span targets an unselected file",
        })
    }
}

impl Error for MapSpanError {}

fn validate_layout(
    root_binding: &RootBinding,
    files: &[FileEntry],
    total_length: Option<u64>,
    piece_length: u64,
) -> Result<(), LayoutError> {
    if files.is_empty() {
        return Err(LayoutError::EmptyLayout);
    }
    if files.len() > MAX_LAYOUT_ENTRIES {
        return Err(LayoutError::TooManyFiles);
    }
    if piece_length == 0 {
        return Err(LayoutError::ZeroPieceLength);
    }
    if total_length.is_none()
        && (files.len() != 1
            || files[0].length != 0
            || files[0].global_start != 0
            || files[0].global_end != 0
            || !files[0].selected)
    {
        return Err(LayoutError::UnknownLengthShape);
    }

    let platform = root_binding.path().platform();
    let mut expected_start = 0_u64;
    let mut paths = BTreeSet::new();
    let mut selected_count = 0_usize;
    for (index, file) in files.iter().enumerate() {
        if file.id != FileId::new(index as u32) {
            return Err(LayoutError::NonCanonicalFileId);
        }
        if file.global_start != expected_start {
            return Err(LayoutError::NonContiguousFile);
        }
        let expected_end = file
            .global_start
            .checked_add(file.length)
            .ok_or(LayoutError::FileEndOverflow)?;
        if file.global_end != expected_end {
            return Err(LayoutError::FileEndMismatch);
        }
        expected_start = file.global_end;
        if !paths.insert(file.safe_path.collision_key(platform)) {
            return Err(LayoutError::PathCollision);
        }
        match (file.selected, file.identity.as_ref()) {
            (true, Some(identity)) => {
                selected_count += 1;
                if root_binding.file_identity(file.id) != Some(identity) {
                    return Err(LayoutError::RootBindingMismatch);
                }
            }
            (true, None) => return Err(LayoutError::SelectedFileMissingIdentity),
            (false, Some(_)) => return Err(LayoutError::UnselectedFileHasIdentity),
            (false, None) => {
                if root_binding.file_identity(file.id).is_some() {
                    return Err(LayoutError::RootBindingMismatch);
                }
            }
        }
    }
    if root_binding.file_identities().len() != selected_count {
        return Err(LayoutError::RootBindingMismatch);
    }
    if total_length.is_some_and(|total| total != expected_start) {
        return Err(LayoutError::TotalLengthMismatch);
    }
    Ok(())
}

fn canonical_size(files: &[FileEntry]) -> Result<usize, LayoutError> {
    files.iter().try_fold(0_usize, |total, file| {
        let identity_len = file
            .identity
            .as_ref()
            .map_or(0, |identity| identity.bytes().len());
        total
            .checked_add(4 + 8 + 8 + 8 + 1 + 4 + file.safe_path.encoded_len() + 4 + identity_len)
            .ok_or(LayoutError::CanonicalLayoutTooLarge)
    })
}

fn calculate_layout_hash(
    files: &[FileEntry],
    total_length: Option<u64>,
    piece_length: u64,
) -> LayoutHash {
    let mut digest = Sha256::new();
    digest.update(LAYOUT_HASH_DOMAIN.as_bytes());
    match total_length {
        Some(total) => {
            digest.update([1]);
            digest.update(total.to_le_bytes());
        }
        None => digest.update([0]),
    }
    digest.update(piece_length.to_le_bytes());
    digest.update((files.len() as u32).to_le_bytes());
    for file in files {
        digest.update(file.id.get().to_le_bytes());
        digest.update(file.global_start.to_le_bytes());
        digest.update(file.global_end.to_le_bytes());
        digest.update(file.length.to_le_bytes());
        digest.update([u8::from(file.selected)]);
        let path = file.safe_path.canonical_string();
        digest.update((path.len() as u32).to_le_bytes());
        digest.update(path.as_bytes());
    }
    LayoutHash(digest.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::{FileEntry, FileLayout, GlobalOffsetMapper, GlobalSpan, LayoutError, MapSpanError};
    use crate::{
        FileIdentity, PathPlatform, PlatformPath, RootBinding, RootIdentity, SafePathBuilder,
    };
    use ariax_core::{FileId, Generation, TaskId};

    fn identity(value: &[u8]) -> FileIdentity {
        FileIdentity::new(value.to_vec()).expect("identity")
    }

    fn safe_path(value: &str) -> crate::SafeRelativePath {
        SafePathBuilder::from_user_path(value, PathPlatform::Unix).expect("safe path")
    }

    fn binding(path: &[u8], files: &[(u32, &[u8])]) -> RootBinding {
        RootBinding::new(
            PlatformPath::from_native_bytes(PathPlatform::Unix, path).expect("root path"),
            RootIdentity::new(b"root-id".to_vec()).expect("root identity"),
            files
                .iter()
                .map(|(file, value)| (FileId::new(*file), identity(value))),
        )
        .expect("binding")
    }

    fn known_single(root_path: &[u8]) -> FileLayout {
        let file_identity = identity(b"file-id");
        FileLayout::new(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            binding(root_path, &[(0, b"file-id")]),
            vec![FileEntry::new(
                FileId::new(0),
                safe_path("image.iso"),
                Some(file_identity),
                1024,
                0,
                1024,
                true,
            )],
            Some(1024),
            256,
        )
        .expect("layout")
    }

    #[test]
    fn known_single_file_maps_exact_offsets() {
        let layout = known_single(b"/srv/one");
        let mapper = GlobalOffsetMapper::new(&layout).expect("mapper");
        assert_eq!(
            mapper.map(GlobalSpan {
                offset: 128,
                len: 512,
            }),
            Ok(super::FileSpan {
                file: FileId::new(0),
                file_offset: 128,
                len: 512,
            })
        );
        assert_eq!(layout.layout_hash().to_string().len(), 64);
    }

    #[test]
    fn mapper_rejects_empty_overflow_and_outside_spans() {
        let mapper = GlobalOffsetMapper::new(&known_single(b"/srv/one")).expect("mapper");
        assert_eq!(
            mapper.map(GlobalSpan { offset: 0, len: 0 }),
            Err(MapSpanError::EmptySpan)
        );
        assert_eq!(
            mapper.map(GlobalSpan {
                offset: u64::MAX,
                len: 2,
            }),
            Err(MapSpanError::Overflow)
        );
        assert_eq!(
            mapper.map(GlobalSpan {
                offset: 1023,
                len: 2,
            }),
            Err(MapSpanError::OutsideLayout)
        );
    }

    #[test]
    fn mapper_rejects_unknown_cross_file_and_unselected_spans() {
        let file_identity = identity(b"file-id");
        let unknown = FileLayout::new(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            binding(b"/srv", &[(0, b"file-id")]),
            vec![FileEntry::new(
                FileId::new(0),
                safe_path("unknown.bin"),
                Some(file_identity),
                0,
                0,
                0,
                true,
            )],
            None,
            1024,
        )
        .expect("unknown layout");
        assert_eq!(
            GlobalOffsetMapper::new(&unknown),
            Err(MapSpanError::UnknownTotalLength)
        );

        let first_identity = identity(b"first");
        let layout = FileLayout::new(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            binding(b"/srv", &[(0, b"first")]),
            vec![
                FileEntry::new(
                    FileId::new(0),
                    safe_path("one"),
                    Some(first_identity),
                    10,
                    0,
                    10,
                    true,
                ),
                FileEntry::new(FileId::new(1), safe_path("two"), None, 10, 10, 20, false),
            ],
            Some(20),
            4,
        )
        .expect("multi-file layout");
        let mapper = GlobalOffsetMapper::new(&layout).expect("mapper");
        assert_eq!(
            mapper.map(GlobalSpan { offset: 8, len: 4 }),
            Err(MapSpanError::CrossFileSpan)
        );
        assert_eq!(
            mapper.map(GlobalSpan { offset: 10, len: 1 }),
            Err(MapSpanError::UnselectedFile)
        );
    }

    #[test]
    fn layout_hash_excludes_root_location_but_binding_hash_does_not() {
        let first = known_single(b"/srv/one");
        let relocated = known_single(b"/srv/two");
        assert_eq!(first.layout_hash(), relocated.layout_hash());
        assert_ne!(first.root_binding().hash(), relocated.root_binding().hash());
    }

    #[test]
    fn layout_validation_rejects_gaps_identity_drift_and_path_collisions() {
        let identity_a = identity(b"a");
        let gap = FileLayout::new(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            binding(b"/srv", &[(0, b"a")]),
            vec![FileEntry::new(
                FileId::new(0),
                safe_path("one"),
                Some(identity_a),
                10,
                1,
                11,
                true,
            )],
            Some(11),
            4,
        );
        assert_eq!(gap, Err(LayoutError::NonContiguousFile));

        let identity_a = identity(b"a");
        let mismatch = FileLayout::new(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            binding(b"/srv", &[(0, b"different")]),
            vec![FileEntry::new(
                FileId::new(0),
                safe_path("one"),
                Some(identity_a),
                10,
                0,
                10,
                true,
            )],
            Some(10),
            4,
        );
        assert_eq!(mismatch, Err(LayoutError::RootBindingMismatch));

        let first = identity(b"first");
        let second = identity(b"second");
        let collision = FileLayout::new(
            TaskId::new(1).expect("task"),
            Generation::INITIAL,
            RootBinding::new(
                PlatformPath::from_native_bytes(PathPlatform::Windows, &[b'C', 0, b':', 0])
                    .expect("path"),
                RootIdentity::new(b"root".to_vec()).expect("root"),
                [
                    (FileId::new(0), first.clone()),
                    (FileId::new(1), second.clone()),
                ],
            )
            .expect("binding"),
            vec![
                FileEntry::new(
                    FileId::new(0),
                    safe_path("Readme"),
                    Some(first),
                    1,
                    0,
                    1,
                    true,
                ),
                FileEntry::new(
                    FileId::new(1),
                    safe_path("README"),
                    Some(second),
                    1,
                    1,
                    2,
                    true,
                ),
            ],
            Some(2),
            1,
        );
        assert_eq!(collision, Err(LayoutError::PathCollision));
    }
}
