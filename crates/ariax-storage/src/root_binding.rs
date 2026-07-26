use crate::PathPlatform;
use ariax_core::FileId;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::Path;

pub const MAX_PLATFORM_PATH_BYTES: usize = 64 * 1024;
pub const MAX_IDENTITY_BYTES: usize = 256;
pub const ROOT_BINDING_HASH_DOMAIN: &str = "ariax/root-binding/v1\0";

/// Canonical native root-path bytes tagged with their originating platform.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlatformPath {
    platform: PathPlatform,
    bytes: Box<[u8]>,
}

impl PlatformPath {
    pub fn from_current(path: &Path) -> Result<Self, RootBindingError> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Self::from_native_bytes(PathPlatform::Unix, path.as_os_str().as_bytes())
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            let bytes = path
                .as_os_str()
                .encode_wide()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            Self::from_native_bytes(PathPlatform::Windows, &bytes)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            Err(RootBindingError::UnsupportedPlatform)
        }
    }

    pub fn from_native_bytes(
        platform: PathPlatform,
        bytes: &[u8],
    ) -> Result<Self, RootBindingError> {
        if bytes.is_empty() {
            return Err(RootBindingError::EmptyPlatformPath);
        }
        if bytes.len() > MAX_PLATFORM_PATH_BYTES {
            return Err(RootBindingError::PlatformPathTooLong);
        }
        match platform {
            PathPlatform::Unix if bytes.contains(&0) => {
                return Err(RootBindingError::InvalidPlatformPath);
            }
            PathPlatform::Windows
                if !bytes.len().is_multiple_of(2)
                    || bytes
                        .chunks_exact(2)
                        .any(|unit| u16::from_le_bytes([unit[0], unit[1]]) == 0) =>
            {
                return Err(RootBindingError::InvalidPlatformPath);
            }
            PathPlatform::Unix | PathPlatform::Windows => {}
        }
        Ok(Self {
            platform,
            bytes: bytes.into(),
        })
    }

    #[must_use]
    pub const fn platform(&self) -> PathPlatform {
        self.platform
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

macro_rules! identity_type {
    ($name:ident, $empty_error:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name(Box<[u8]>);

        impl $name {
            pub fn new(bytes: impl Into<Box<[u8]>>) -> Result<Self, RootBindingError> {
                let bytes = bytes.into();
                if bytes.is_empty() {
                    return Err(RootBindingError::$empty_error);
                }
                if bytes.len() > MAX_IDENTITY_BYTES {
                    return Err(RootBindingError::IdentityTooLong);
                }
                Ok(Self(bytes))
            }

            #[must_use]
            pub fn bytes(&self) -> &[u8] {
                &self.0
            }
        }
    };
}

identity_type!(RootIdentity, EmptyRootIdentity);
identity_type!(FileIdentity, EmptyFileIdentity);

/// SHA-256 over the domain-separated root path and opened identities.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RootBindingHash([u8; 32]);

impl RootBindingHash {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for RootBindingHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RootBindingHash({self})")
    }
}

impl fmt::Display for RootBindingHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Persistable evidence required before journal progress can be trusted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootBinding {
    path: PlatformPath,
    root_identity: RootIdentity,
    file_identities: BTreeMap<FileId, FileIdentity>,
    hash: RootBindingHash,
}

impl RootBinding {
    pub fn new(
        path: PlatformPath,
        root_identity: RootIdentity,
        file_identities: impl IntoIterator<Item = (FileId, FileIdentity)>,
    ) -> Result<Self, RootBindingError> {
        let mut identities = BTreeMap::new();
        for (file, identity) in file_identities {
            if identities.insert(file, identity).is_some() {
                return Err(RootBindingError::DuplicateFileIdentity(file));
            }
        }
        let hash = calculate_hash(&path, &root_identity, &identities);
        Ok(Self {
            path,
            root_identity,
            file_identities: identities,
            hash,
        })
    }

    #[must_use]
    pub const fn path(&self) -> &PlatformPath {
        &self.path
    }

    #[must_use]
    pub const fn root_identity(&self) -> &RootIdentity {
        &self.root_identity
    }

    #[must_use]
    pub fn file_identity(&self, file: FileId) -> Option<&FileIdentity> {
        self.file_identities.get(&file)
    }

    pub fn file_identities(&self) -> impl ExactSizeIterator<Item = (FileId, &FileIdentity)> {
        self.file_identities
            .iter()
            .map(|(file, identity)| (*file, identity))
    }

    #[must_use]
    pub const fn hash(&self) -> RootBindingHash {
        self.hash
    }
}

/// Why persisted root-binding evidence was not canonical.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootBindingError {
    UnsupportedPlatform,
    EmptyPlatformPath,
    PlatformPathTooLong,
    InvalidPlatformPath,
    EmptyRootIdentity,
    EmptyFileIdentity,
    IdentityTooLong,
    DuplicateFileIdentity(FileId),
}

impl RootBindingError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::EmptyPlatformPath => "empty_platform_path",
            Self::PlatformPathTooLong => "platform_path_too_long",
            Self::InvalidPlatformPath => "invalid_platform_path",
            Self::EmptyRootIdentity => "empty_root_identity",
            Self::EmptyFileIdentity => "empty_file_identity",
            Self::IdentityTooLong => "identity_too_long",
            Self::DuplicateFileIdentity(_) => "duplicate_file_identity",
        }
    }
}

pub const ALL_ROOT_BINDING_ERROR_CLASSES: [RootBindingError; 8] = [
    RootBindingError::UnsupportedPlatform,
    RootBindingError::EmptyPlatformPath,
    RootBindingError::PlatformPathTooLong,
    RootBindingError::InvalidPlatformPath,
    RootBindingError::EmptyRootIdentity,
    RootBindingError::EmptyFileIdentity,
    RootBindingError::IdentityTooLong,
    RootBindingError::DuplicateFileIdentity(FileId::new(0)),
];

impl fmt::Display for RootBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => {
                formatter.write_str("platform path encoding is unsupported")
            }
            Self::EmptyPlatformPath => formatter.write_str("canonical root path is empty"),
            Self::PlatformPathTooLong => formatter.write_str("canonical root path exceeds 64 KiB"),
            Self::InvalidPlatformPath => {
                formatter.write_str("canonical root path bytes are invalid")
            }
            Self::EmptyRootIdentity => formatter.write_str("root identity is empty"),
            Self::EmptyFileIdentity => formatter.write_str("file identity is empty"),
            Self::IdentityTooLong => formatter.write_str("platform identity exceeds 256 bytes"),
            Self::DuplicateFileIdentity(file) => {
                write!(formatter, "duplicate identity for file {}", file.get())
            }
        }
    }
}

impl Error for RootBindingError {}

fn calculate_hash(
    path: &PlatformPath,
    root_identity: &RootIdentity,
    files: &BTreeMap<FileId, FileIdentity>,
) -> RootBindingHash {
    let mut digest = Sha256::new();
    digest.update(ROOT_BINDING_HASH_DOMAIN.as_bytes());
    digest.update([path.platform() as u8]);
    update_bytes(&mut digest, path.bytes());
    update_bytes(&mut digest, root_identity.bytes());
    digest.update((files.len() as u32).to_le_bytes());
    for (file, identity) in files {
        digest.update(file.get().to_le_bytes());
        update_bytes(&mut digest, identity.bytes());
    }
    RootBindingHash(digest.finalize().into())
}

fn update_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u32).to_le_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::{FileIdentity, PlatformPath, RootBinding, RootBindingError, RootIdentity};
    use crate::PathPlatform;
    use ariax_core::FileId;

    fn path(value: &[u8]) -> PlatformPath {
        PlatformPath::from_native_bytes(PathPlatform::Unix, value).expect("path")
    }

    fn root(value: &[u8]) -> RootIdentity {
        RootIdentity::new(value.to_vec()).expect("root identity")
    }

    fn file(value: &[u8]) -> FileIdentity {
        FileIdentity::new(value.to_vec()).expect("file identity")
    }

    #[test]
    fn binding_hash_is_order_independent_but_evidence_sensitive() {
        let first = RootBinding::new(
            path(b"/srv/downloads"),
            root(b"dev=1,ino=2"),
            [
                (FileId::new(1), file(b"file-b")),
                (FileId::new(0), file(b"file-a")),
            ],
        )
        .expect("binding");
        let reordered = RootBinding::new(
            path(b"/srv/downloads"),
            root(b"dev=1,ino=2"),
            [
                (FileId::new(0), file(b"file-a")),
                (FileId::new(1), file(b"file-b")),
            ],
        )
        .expect("binding");
        assert_eq!(first.hash(), reordered.hash());

        let relocated = RootBinding::new(
            path(b"/mnt/downloads"),
            root(b"dev=1,ino=2"),
            [(FileId::new(0), file(b"file-a"))],
        )
        .expect("binding");
        assert_ne!(first.hash(), relocated.hash());
        assert_eq!(first.hash().to_string().len(), 64);
    }

    #[test]
    fn native_path_and_identity_caps_fail_before_storage() {
        assert_eq!(
            PlatformPath::from_native_bytes(PathPlatform::Unix, b"bad\0path"),
            Err(RootBindingError::InvalidPlatformPath)
        );
        assert_eq!(
            PlatformPath::from_native_bytes(PathPlatform::Windows, &[1]),
            Err(RootBindingError::InvalidPlatformPath)
        );
        assert_eq!(
            RootIdentity::new(Vec::<u8>::new()),
            Err(RootBindingError::EmptyRootIdentity)
        );
        assert_eq!(
            FileIdentity::new(vec![0; 257]),
            Err(RootBindingError::IdentityTooLong)
        );
    }

    #[test]
    fn duplicate_file_identity_is_rejected() {
        assert_eq!(
            RootBinding::new(
                path(b"/srv/downloads"),
                root(b"root"),
                [
                    (FileId::new(0), file(b"one")),
                    (FileId::new(0), file(b"two")),
                ],
            ),
            Err(RootBindingError::DuplicateFileIdentity(FileId::new(0)))
        );
    }
}
