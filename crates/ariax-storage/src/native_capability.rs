use crate::{
    FileIdentity, NativeIdentityError, NativeIdentityV1, PathPlatform, PlatformPath, RootIdentity,
    SafeRelativePath,
};
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

pub const MAX_NATIVE_ALLOWED_ROOTS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeObjectKind {
    Directory,
    RegularFile,
}

impl NativeObjectKind {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::RegularFile => "regular_file",
        }
    }
}

#[derive(Debug)]
pub enum NativeCapabilityError {
    UnsupportedPlatform,
    PlatformPathMismatch,
    InvalidAbsolutePath,
    UnsafePathComponent,
    OutsideAllowedRoot,
    TooManyAllowedRoots,
    SafeOpenUnavailable,
    ObjectKindMismatch { expected: NativeObjectKind },
    HardLinkAlias,
    Identity(NativeIdentityError),
    IdentityMismatch,
    Io(io::Error),
}

impl NativeCapabilityError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::PlatformPathMismatch => "platform_path_mismatch",
            Self::InvalidAbsolutePath => "invalid_absolute_path",
            Self::UnsafePathComponent => "unsafe_path_component",
            Self::OutsideAllowedRoot => "outside_allowed_root",
            Self::TooManyAllowedRoots => "too_many_allowed_roots",
            Self::SafeOpenUnavailable => "safe_open_unavailable",
            Self::ObjectKindMismatch { .. } => "object_kind_mismatch",
            Self::HardLinkAlias => "hard_link_alias",
            Self::Identity(_) => "invalid_native_identity",
            Self::IdentityMismatch => "identity_mismatch",
            Self::Io(_) => "io",
        }
    }
}

impl fmt::Display for NativeCapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => formatter.write_str("native filesystem is unsupported"),
            Self::PlatformPathMismatch => {
                formatter.write_str("persisted path belongs to another platform")
            }
            Self::InvalidAbsolutePath => {
                formatter.write_str("native capability root must be an absolute path")
            }
            Self::UnsafePathComponent => {
                formatter.write_str("native capability path contains an unsafe component")
            }
            Self::OutsideAllowedRoot => {
                formatter.write_str("native path is outside every allowed root")
            }
            Self::TooManyAllowedRoots => {
                formatter.write_str("native allowed-root list exceeds its hard cap")
            }
            Self::SafeOpenUnavailable => {
                formatter.write_str("race-resistant no-follow open is unavailable")
            }
            Self::ObjectKindMismatch { expected } => {
                write!(formatter, "opened object is not a {}", expected.code())
            }
            Self::HardLinkAlias => {
                formatter.write_str("opened regular file has more than one hard link")
            }
            Self::Identity(error) => error.fmt(formatter),
            Self::IdentityMismatch => {
                formatter.write_str("opened object identity does not match persisted evidence")
            }
            Self::Io(error) => write!(formatter, "native capability I/O failed: {error}"),
        }
    }
}

impl Error for NativeCapabilityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<NativeIdentityError> for NativeCapabilityError {
    fn from(error: NativeIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<io::Error> for NativeCapabilityError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
struct DirectoryCapability {
    display: PathBuf,
    native: platform::DirectoryHandle,
    identity: NativeIdentityV1,
}

#[derive(Clone, Debug)]
pub struct RootDirectoryCapability(Arc<DirectoryCapability>);

#[derive(Clone, Debug)]
pub struct JournalDirectoryCapability(Arc<DirectoryCapability>);

/// One regular file opened relative to a trusted root without retaining a
/// pathname as write authority.
#[derive(Debug)]
pub struct RootFileCapability {
    file: File,
    identity: NativeIdentityV1,
}

impl RootFileCapability {
    #[must_use]
    pub const fn identity(&self) -> NativeIdentityV1 {
        self.identity
    }

    pub fn set_len(&self, len: u64) -> Result<(), NativeCapabilityError> {
        self.file.set_len(len).map_err(Into::into)
    }

    pub fn len(&self) -> Result<u64, NativeCapabilityError> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(Into::into)
    }

    /// Returns whether the descriptor currently has zero bytes.
    pub fn is_empty(&self) -> Result<bool, NativeCapabilityError> {
        self.len().map(|length| length == 0)
    }

    pub fn read_exact_at(
        &self,
        mut offset: u64,
        mut output: &mut [u8],
    ) -> Result<(), NativeCapabilityError> {
        while !output.is_empty() {
            let read = platform::read_at(&self.file, offset, output)?;
            if read == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
            }
            offset = offset
                .checked_add(u64::try_from(read).expect("native read length fits u64"))
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
            output = &mut output[read..];
        }
        Ok(())
    }

    pub fn sync_data(&self) -> Result<(), NativeCapabilityError> {
        self.file.sync_data().map_err(Into::into)
    }

    pub fn sync_all(&self) -> Result<(), NativeCapabilityError> {
        self.file.sync_all().map_err(Into::into)
    }

    pub fn try_clone_file(&self) -> Result<File, NativeCapabilityError> {
        self.file.try_clone().map_err(Into::into)
    }

    /// Duplicates the already-open descriptor while preserving its verified
    /// native identity. The clone carries no path authority and is suitable
    /// for independent descriptor-bound verification work.
    pub fn try_clone_capability(&self) -> Result<Self, NativeCapabilityError> {
        Ok(Self {
            file: self.file.try_clone()?,
            identity: self.identity,
        })
    }

    #[must_use]
    pub fn into_file(self) -> File {
        self.file
    }
}

#[derive(Clone, Copy)]
enum FileAccess {
    Read,
    /// Read native identity while allowing a transient publication alias.
    Identity,
    Append,
    RandomWrite,
}

impl RootDirectoryCapability {
    pub fn open_trusted(path: impl AsRef<Path>) -> Result<Self, NativeCapabilityError> {
        DirectoryCapability::open_absolute(path.as_ref())
            .map(|capability| Self(Arc::new(capability)))
    }

    pub fn open_bound(
        path: &PlatformPath,
        expected: &RootIdentity,
        allowed_roots: &[Self],
    ) -> Result<Self, NativeCapabilityError> {
        if allowed_roots.len() > MAX_NATIVE_ALLOWED_ROOTS {
            return Err(NativeCapabilityError::TooManyAllowedRoots);
        }
        let path = platform_path_to_current(path)?;
        let expected = NativeIdentityV1::decode_for_current(expected.bytes())?;
        for allowed in allowed_roots {
            let Ok(relative) = path.strip_prefix(allowed.display()) else {
                continue;
            };
            let opened = if relative.as_os_str().is_empty() {
                allowed.clone()
            } else {
                Self(Arc::new(allowed.0.open_directory(relative)?))
            };
            if opened.identity() != expected {
                return Err(NativeCapabilityError::IdentityMismatch);
            }
            return Ok(opened);
        }
        Err(NativeCapabilityError::OutsideAllowedRoot)
    }

    #[must_use]
    pub fn display(&self) -> &Path {
        &self.0.display
    }

    #[must_use]
    pub fn identity(&self) -> NativeIdentityV1 {
        self.0.identity
    }

    pub fn verify_file(
        &self,
        relative: &SafeRelativePath,
        expected: &FileIdentity,
    ) -> Result<(), NativeCapabilityError> {
        let expected = NativeIdentityV1::decode_for_current(expected.bytes())?;
        let file = self
            .0
            .open_regular_file(&relative.display_path(), FileAccess::Read)?;
        let actual = platform::file_identity(&file, NativeObjectKind::RegularFile)?;
        if actual != expected {
            return Err(NativeCapabilityError::IdentityMismatch);
        }
        Ok(())
    }

    /// Creates one new final component below this root. Parent directories must
    /// already exist and are opened component-by-component without following
    /// links or reparse points.
    pub fn create_new_file(
        &self,
        relative: &SafeRelativePath,
    ) -> Result<RootFileCapability, NativeCapabilityError> {
        let path = relative.display_path();
        let name = path
            .file_name()
            .ok_or(NativeCapabilityError::UnsafePathComponent)?;
        let parent = path.parent().filter(|value| !value.as_os_str().is_empty());
        let file = match parent {
            Some(parent) => self.0.open_directory(parent)?.create_new_file(name)?,
            None => self.0.create_new_file(name)?,
        };
        let identity = platform::file_identity(&file, NativeObjectKind::RegularFile)?;
        Ok(RootFileCapability { file, identity })
    }

    /// Reopens one persisted output for positional writes and verifies the
    /// exact identity before returning descriptor authority.
    pub fn open_existing_file(
        &self,
        relative: &SafeRelativePath,
        expected: &FileIdentity,
    ) -> Result<RootFileCapability, NativeCapabilityError> {
        let expected = NativeIdentityV1::decode_for_current(expected.bytes())?;
        let file = self
            .0
            .open_regular_file(&relative.display_path(), FileAccess::RandomWrite)?;
        let identity = platform::file_identity(&file, NativeObjectKind::RegularFile)?;
        if identity != expected {
            return Err(NativeCapabilityError::IdentityMismatch);
        }
        Ok(RootFileCapability { file, identity })
    }
}

impl JournalDirectoryCapability {
    pub fn open_trusted(path: impl AsRef<Path>) -> Result<Self, NativeCapabilityError> {
        DirectoryCapability::open_absolute(path.as_ref())
            .map(|capability| Self(Arc::new(capability)))
    }

    pub fn open_persisted_under(
        control_root: &Self,
        path: &PlatformPath,
    ) -> Result<Self, NativeCapabilityError> {
        let path = platform_path_to_current(path)?;
        let relative = path
            .strip_prefix(control_root.display())
            .map_err(|_| NativeCapabilityError::OutsideAllowedRoot)?;
        if relative.as_os_str().is_empty() {
            return Ok(control_root.clone());
        }
        control_root.open_directory(relative)
    }

    pub fn open_directory(&self, relative: &Path) -> Result<Self, NativeCapabilityError> {
        self.0
            .open_directory(relative)
            .map(|capability| Self(Arc::new(capability)))
    }

    #[must_use]
    pub fn display(&self) -> &Path {
        &self.0.display
    }

    #[must_use]
    pub fn identity(&self) -> NativeIdentityV1 {
        self.0.identity
    }

    pub(crate) fn entries(&self) -> Result<Vec<OsString>, NativeCapabilityError> {
        self.0.entries()
    }

    pub(crate) fn open_regular_file(
        &self,
        name: &OsStr,
        write: bool,
    ) -> Result<File, NativeCapabilityError> {
        validate_single_name(name)?;
        self.0.open_regular_file(
            Path::new(name),
            if write {
                FileAccess::Append
            } else {
                FileAccess::Read
            },
        )
    }

    /// Opens a regular file read-only while permitting the transient extra
    /// hard link created by journal segment publication. Callers must first
    /// prove that the private candidate and installed name identify the same
    /// file, validate the bytes through this descriptor, remove the alias,
    /// and reopen with ordinary authority before mutating the file.
    pub(crate) fn open_regular_file_for_publication_validation(
        &self,
        name: &OsStr,
    ) -> Result<File, NativeCapabilityError> {
        validate_single_name(name)?;
        self.0
            .open_regular_file(Path::new(name), FileAccess::Identity)
    }

    pub(crate) fn create_new_file(&self, name: &OsStr) -> Result<File, NativeCapabilityError> {
        validate_single_name(name)?;
        self.0.create_new_file(name)
    }

    pub(crate) fn link_no_replace(
        &self,
        source: &OsStr,
        destination: &OsStr,
    ) -> Result<(), NativeCapabilityError> {
        validate_single_name(source)?;
        validate_single_name(destination)?;
        self.0.link_no_replace(source, destination)
    }

    pub(crate) fn remove_file(&self, name: &OsStr) -> Result<(), NativeCapabilityError> {
        validate_single_name(name)?;
        self.0.remove_file(name)
    }

    /// Returns whether two names below this trusted directory identify the
    /// same regular file. This narrow identity-only operation permits a
    /// transient hard-link count greater than one; callers must remove the
    /// alias before opening the file for ordinary authority.
    pub(crate) fn same_regular_file(
        &self,
        left: &OsStr,
        right: &OsStr,
    ) -> Result<bool, NativeCapabilityError> {
        validate_single_name(left)?;
        validate_single_name(right)?;
        self.0.same_regular_file(left, right)
    }

    /// Returns the native hard-link count for a regular file below this
    /// trusted directory. Identity-only opening permits a transient
    /// publication alias; callers use the count to prove that they know every
    /// name before unlinking any residue.
    pub(crate) fn regular_file_link_count(
        &self,
        name: &OsStr,
    ) -> Result<u64, NativeCapabilityError> {
        validate_single_name(name)?;
        self.0.regular_file_link_count(name)
    }

    pub(crate) fn sync(&self) -> Result<(), NativeCapabilityError> {
        self.0.sync()
    }
}

impl DirectoryCapability {
    fn open_absolute(path: &Path) -> Result<Self, NativeCapabilityError> {
        validate_absolute_path(path)?;
        let native = platform::open_absolute_directory(path)?;
        let identity = platform::directory_identity(&native)?;
        Ok(Self {
            display: path.to_path_buf(),
            native,
            identity,
        })
    }

    fn open_directory(&self, relative: &Path) -> Result<Self, NativeCapabilityError> {
        validate_relative_path(relative)?;
        let native = platform::open_relative_directory(&self.native, relative)?;
        let identity = platform::directory_identity(&native)?;
        Ok(Self {
            display: self.display.join(relative),
            native,
            identity,
        })
    }

    fn open_regular_file(
        &self,
        relative: &Path,
        access: FileAccess,
    ) -> Result<File, NativeCapabilityError> {
        validate_relative_path(relative)?;
        platform::open_relative_regular_file(&self.native, relative, access)
    }

    fn same_regular_file(
        &self,
        left: &OsStr,
        right: &OsStr,
    ) -> Result<bool, NativeCapabilityError> {
        let left_file = self.open_regular_file(Path::new(left), FileAccess::Identity)?;
        let right_file = self.open_regular_file(Path::new(right), FileAccess::Identity)?;
        let left_identity =
            platform::file_identity_allow_alias(&left_file, NativeObjectKind::RegularFile)?;
        let right_identity =
            platform::file_identity_allow_alias(&right_file, NativeObjectKind::RegularFile)?;
        Ok(left_identity == right_identity)
    }

    fn regular_file_link_count(&self, name: &OsStr) -> Result<u64, NativeCapabilityError> {
        let file = self.open_regular_file(Path::new(name), FileAccess::Identity)?;
        platform::regular_file_link_count(&file)
    }

    fn create_new_file(&self, name: &OsStr) -> Result<File, NativeCapabilityError> {
        platform::create_new_file(&self.native, name)
    }

    fn entries(&self) -> Result<Vec<OsString>, NativeCapabilityError> {
        platform::directory_entries(&self.native)
    }

    fn link_no_replace(
        &self,
        source: &OsStr,
        destination: &OsStr,
    ) -> Result<(), NativeCapabilityError> {
        platform::link_no_replace(&self.native, source, destination)
    }

    fn remove_file(&self, name: &OsStr) -> Result<(), NativeCapabilityError> {
        platform::remove_file(&self.native, name)
    }

    fn sync(&self) -> Result<(), NativeCapabilityError> {
        platform::sync_directory(&self.native)
    }
}

pub fn platform_path_to_current(path: &PlatformPath) -> Result<PathBuf, NativeCapabilityError> {
    if path.platform() != PathPlatform::current() {
        return Err(NativeCapabilityError::PlatformPathMismatch);
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        Ok(PathBuf::from(OsString::from_vec(path.bytes().to_vec())))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let units = path
            .bytes()
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        Ok(PathBuf::from(OsString::from_wide(&units)))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(NativeCapabilityError::UnsupportedPlatform)
    }
}

fn validate_absolute_path(path: &Path) -> Result<(), NativeCapabilityError> {
    if !path.is_absolute() {
        return Err(NativeCapabilityError::InvalidAbsolutePath);
    }
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {}
            Component::CurDir | Component::ParentDir => {
                return Err(NativeCapabilityError::UnsafePathComponent);
            }
        }
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), NativeCapabilityError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(NativeCapabilityError::UnsafePathComponent);
    }
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(NativeCapabilityError::UnsafePathComponent);
        }
    }
    Ok(())
}

fn validate_single_name(name: &OsStr) -> Result<(), NativeCapabilityError> {
    validate_relative_path(Path::new(name))?;
    if Path::new(name).components().count() != 1 {
        return Err(NativeCapabilityError::UnsafePathComponent);
    }
    Ok(())
}

#[cfg(unix)]
mod platform {
    use super::{FileAccess, NativeCapabilityError, NativeIdentityV1, NativeObjectKind};
    use rustix::fd::OwnedFd;
    use rustix::fs::{
        self, AtFlags, Dir, FileType, Mode, OFlags, fstat, linkat, open, openat, unlinkat,
    };
    #[cfg(target_os = "linux")]
    use rustix::fs::{ResolveFlags, openat2};
    use std::ffi::{OsStr, OsString};
    use std::fs::File;
    use std::os::unix::ffi::OsStringExt as _;
    use std::os::unix::fs::FileExt as _;
    use std::path::{Component, Path};

    pub(super) type DirectoryHandle = OwnedFd;

    pub(super) fn read_at(
        file: &File,
        offset: u64,
        output: &mut [u8],
    ) -> Result<usize, NativeCapabilityError> {
        file.read_at(output, offset).map_err(Into::into)
    }

    pub(super) fn open_absolute_directory(
        path: &Path,
    ) -> Result<DirectoryHandle, NativeCapabilityError> {
        let mut current = open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(std::io::Error::from)?;
        for component in path.components() {
            match component {
                Component::RootDir => {}
                Component::Normal(name) => {
                    current = openat(
                        &current,
                        name,
                        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                        Mode::empty(),
                    )
                    .map_err(std::io::Error::from)?;
                }
                Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                    return Err(NativeCapabilityError::UnsafePathComponent);
                }
            }
        }
        validate_fd_kind(&current, NativeObjectKind::Directory)?;
        Ok(current)
    }

    pub(super) fn open_relative_directory(
        root: &DirectoryHandle,
        relative: &Path,
    ) -> Result<DirectoryHandle, NativeCapabilityError> {
        #[cfg(target_os = "linux")]
        match openat2(
            root,
            relative,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        ) {
            Ok(fd) => {
                validate_fd_kind(&fd, NativeObjectKind::Directory)?;
                return Ok(fd);
            }
            Err(
                rustix::io::Errno::NOSYS | rustix::io::Errno::INVAL | rustix::io::Errno::OPNOTSUPP,
            ) => {}
            Err(error) => return Err(std::io::Error::from(error).into()),
        }
        open_relative_components(
            root,
            relative,
            NativeObjectKind::Directory,
            FileAccess::Read,
        )
        .map(|opened| opened.expect_directory())
    }

    pub(super) fn open_relative_regular_file(
        root: &DirectoryHandle,
        relative: &Path,
        access: FileAccess,
    ) -> Result<File, NativeCapabilityError> {
        #[cfg(target_os = "linux")]
        {
            let flags = match access {
                FileAccess::Read => OFlags::RDONLY,
                FileAccess::Identity => OFlags::RDONLY,
                FileAccess::Append => OFlags::RDWR | OFlags::APPEND,
                FileAccess::RandomWrite => OFlags::RDWR,
            };
            match openat2(
                root,
                relative,
                flags | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
                ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
            ) {
                Ok(fd) => {
                    validate_fd_kind_with_alias(
                        &fd,
                        NativeObjectKind::RegularFile,
                        matches!(access, FileAccess::Identity),
                    )?;
                    return Ok(File::from(fd));
                }
                Err(
                    rustix::io::Errno::NOSYS
                    | rustix::io::Errno::INVAL
                    | rustix::io::Errno::OPNOTSUPP,
                ) => {}
                Err(error) => return Err(std::io::Error::from(error).into()),
            }
        }
        open_relative_components(root, relative, NativeObjectKind::RegularFile, access)
            .map(|opened| File::from(opened.expect_file()))
    }

    enum Opened {
        Directory(OwnedFd),
        File(OwnedFd),
    }

    impl Opened {
        fn expect_directory(self) -> OwnedFd {
            match self {
                Self::Directory(fd) => fd,
                Self::File(_) => unreachable!("requested directory open returned file"),
            }
        }

        fn expect_file(self) -> OwnedFd {
            match self {
                Self::File(fd) => fd,
                Self::Directory(_) => unreachable!("requested file open returned directory"),
            }
        }
    }

    fn open_relative_components(
        root: &DirectoryHandle,
        relative: &Path,
        final_kind: NativeObjectKind,
        access: FileAccess,
    ) -> Result<Opened, NativeCapabilityError> {
        let components = relative.components().collect::<Vec<_>>();
        let mut current = rustix::io::dup(root).map_err(std::io::Error::from)?;
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(name) = component else {
                return Err(NativeCapabilityError::UnsafePathComponent);
            };
            let last = index + 1 == components.len();
            let kind = if last {
                final_kind
            } else {
                NativeObjectKind::Directory
            };
            let flags = if last {
                match access {
                    FileAccess::Read => OFlags::RDONLY,
                    FileAccess::Identity => OFlags::RDONLY,
                    FileAccess::Append => OFlags::RDWR | OFlags::APPEND,
                    FileAccess::RandomWrite => OFlags::RDWR,
                }
            } else {
                OFlags::RDONLY
            };
            let directory = if kind == NativeObjectKind::Directory {
                OFlags::DIRECTORY
            } else {
                OFlags::empty()
            };
            current = openat(
                &current,
                *name,
                flags | directory | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(std::io::Error::from)?;
            validate_fd_kind_with_alias(
                &current,
                kind,
                last && matches!(access, FileAccess::Identity),
            )?;
        }
        Ok(match final_kind {
            NativeObjectKind::Directory => Opened::Directory(current),
            NativeObjectKind::RegularFile => Opened::File(current),
        })
    }

    pub(super) fn directory_identity(
        handle: &DirectoryHandle,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_fd_kind(handle, NativeObjectKind::Directory)
    }

    pub(super) fn file_identity(
        file: &File,
        expected: NativeObjectKind,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_fd_kind(file, expected)
    }

    pub(super) fn file_identity_allow_alias(
        file: &File,
        expected: NativeObjectKind,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_fd_kind_with_alias(file, expected, true)
    }

    pub(super) fn regular_file_link_count(file: &File) -> Result<u64, NativeCapabilityError> {
        let stat = fstat(file).map_err(std::io::Error::from)?;
        let actual = FileType::from_raw_mode(stat.st_mode);
        if actual != FileType::RegularFile {
            return Err(NativeCapabilityError::ObjectKindMismatch {
                expected: NativeObjectKind::RegularFile,
            });
        }
        Ok(stat.st_nlink)
    }

    fn validate_fd_kind(
        fd: impl rustix::fd::AsFd,
        expected: NativeObjectKind,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_fd_kind_with_alias(fd, expected, false)
    }

    fn validate_fd_kind_with_alias(
        fd: impl rustix::fd::AsFd,
        expected: NativeObjectKind,
        allow_hard_link: bool,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        let stat = fstat(fd).map_err(std::io::Error::from)?;
        let actual = FileType::from_raw_mode(stat.st_mode);
        let matches = match expected {
            NativeObjectKind::Directory => actual == FileType::Directory,
            NativeObjectKind::RegularFile => actual == FileType::RegularFile,
        };
        if !matches {
            return Err(NativeCapabilityError::ObjectKindMismatch { expected });
        }
        if expected == NativeObjectKind::RegularFile && !allow_hard_link && stat.st_nlink != 1 {
            return Err(NativeCapabilityError::HardLinkAlias);
        }
        Ok(NativeIdentityV1::Unix {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
        })
    }

    pub(super) fn directory_entries(
        handle: &DirectoryHandle,
    ) -> Result<Vec<OsString>, NativeCapabilityError> {
        let directory = Dir::read_from(handle).map_err(std::io::Error::from)?;
        let mut names = Vec::new();
        for entry in directory {
            let entry = entry.map_err(std::io::Error::from)?;
            let bytes = entry.file_name().to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            names.push(OsString::from_vec(bytes.to_vec()));
        }
        Ok(names)
    }

    pub(super) fn create_new_file(
        directory: &DirectoryHandle,
        name: &OsStr,
    ) -> Result<File, NativeCapabilityError> {
        let fd = openat(
            directory,
            name,
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::from_bits_retain(0o600),
        )
        .map_err(std::io::Error::from)?;
        validate_fd_kind(&fd, NativeObjectKind::RegularFile)?;
        Ok(File::from(fd))
    }

    pub(super) fn link_no_replace(
        directory: &DirectoryHandle,
        source: &OsStr,
        destination: &OsStr,
    ) -> Result<(), NativeCapabilityError> {
        linkat(directory, source, directory, destination, AtFlags::empty())
            .map_err(std::io::Error::from)
            .map_err(Into::into)
    }

    pub(super) fn remove_file(
        directory: &DirectoryHandle,
        name: &OsStr,
    ) -> Result<(), NativeCapabilityError> {
        unlinkat(directory, name, AtFlags::empty())
            .map_err(std::io::Error::from)
            .map_err(Into::into)
    }

    pub(super) fn sync_directory(directory: &DirectoryHandle) -> Result<(), NativeCapabilityError> {
        match fs::fsync(directory) {
            Ok(()) => Ok(()),
            Err(rustix::io::Errno::INVAL | rustix::io::Errno::OPNOTSUPP) => Ok(()),
            Err(error) => Err(std::io::Error::from(error).into()),
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::{FileAccess, NativeCapabilityError, NativeIdentityV1, NativeObjectKind};
    use ariax_windows_security::{
        create_relative_file_no_reparse, directory_names, link_relative_no_replace,
        open_absolute_directory_no_reparse, open_relative_directory_no_reparse,
        open_relative_regular_file_no_reparse, query_native_file_information,
        remove_relative_file_no_reparse,
    };
    use std::ffi::{OsStr, OsString};
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::FileExt as _;
    use std::path::Path;

    pub(super) type DirectoryHandle = File;

    pub(super) fn read_at(
        file: &File,
        offset: u64,
        output: &mut [u8],
    ) -> Result<usize, NativeCapabilityError> {
        file.seek_read(output, offset).map_err(Into::into)
    }

    pub(super) fn open_absolute_directory(
        path: &Path,
    ) -> Result<DirectoryHandle, NativeCapabilityError> {
        let handle = open_absolute_directory_no_reparse(path)?;
        validate_file_kind(&handle, NativeObjectKind::Directory)?;
        Ok(handle)
    }

    pub(super) fn open_relative_directory(
        root: &DirectoryHandle,
        relative: &Path,
    ) -> Result<DirectoryHandle, NativeCapabilityError> {
        let handle = open_relative_directory_no_reparse(root, relative)?;
        validate_file_kind(&handle, NativeObjectKind::Directory)?;
        Ok(handle)
    }

    pub(super) fn open_relative_regular_file(
        root: &DirectoryHandle,
        relative: &Path,
        access: FileAccess,
    ) -> Result<File, NativeCapabilityError> {
        let file = open_relative_regular_file_no_reparse(
            root,
            relative,
            matches!(access, FileAccess::Append | FileAccess::RandomWrite),
        )?;
        validate_file_kind_with_alias(
            &file,
            NativeObjectKind::RegularFile,
            matches!(access, FileAccess::Identity),
        )?;
        Ok(file)
    }

    pub(super) fn directory_identity(
        handle: &DirectoryHandle,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_file_kind(handle, NativeObjectKind::Directory)
    }

    pub(super) fn file_identity(
        file: &File,
        expected: NativeObjectKind,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_file_kind(file, expected)
    }

    pub(super) fn file_identity_allow_alias(
        file: &File,
        expected: NativeObjectKind,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_file_kind_with_alias(file, expected, true)
    }

    pub(super) fn regular_file_link_count(file: &File) -> Result<u64, NativeCapabilityError> {
        let information = query_native_file_information(file)?;
        if information.is_directory {
            return Err(NativeCapabilityError::ObjectKindMismatch {
                expected: NativeObjectKind::RegularFile,
            });
        }
        Ok(u64::from(information.number_of_links))
    }

    pub(super) fn directory_entries(
        handle: &DirectoryHandle,
    ) -> Result<Vec<OsString>, NativeCapabilityError> {
        directory_names(handle).map_err(Into::into)
    }

    pub(super) fn create_new_file(
        directory: &DirectoryHandle,
        name: &OsStr,
    ) -> Result<File, NativeCapabilityError> {
        let file = create_relative_file_no_reparse(directory, name)?;
        validate_file_kind(&file, NativeObjectKind::RegularFile)?;
        Ok(file)
    }

    pub(super) fn link_no_replace(
        directory: &DirectoryHandle,
        source: &OsStr,
        destination: &OsStr,
    ) -> Result<(), NativeCapabilityError> {
        link_relative_no_replace(directory, source, destination).map_err(Into::into)
    }

    pub(super) fn remove_file(
        directory: &DirectoryHandle,
        name: &OsStr,
    ) -> Result<(), NativeCapabilityError> {
        remove_relative_file_no_reparse(directory, name).map_err(Into::into)
    }

    pub(super) fn sync_directory(directory: &DirectoryHandle) -> Result<(), NativeCapabilityError> {
        match directory.sync_all() {
            Ok(()) => Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::InvalidInput
                        | io::ErrorKind::PermissionDenied
                        | io::ErrorKind::Unsupported
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn validate_file_kind(
        file: &File,
        expected: NativeObjectKind,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        validate_file_kind_with_alias(file, expected, false)
    }

    fn validate_file_kind_with_alias(
        file: &File,
        expected: NativeObjectKind,
        allow_hard_link: bool,
    ) -> Result<NativeIdentityV1, NativeCapabilityError> {
        let information = query_native_file_information(file)?;
        let matches = match expected {
            NativeObjectKind::Directory => information.is_directory,
            NativeObjectKind::RegularFile => !information.is_directory,
        };
        if !matches {
            return Err(NativeCapabilityError::ObjectKindMismatch { expected });
        }
        if expected == NativeObjectKind::RegularFile
            && !allow_hard_link
            && information.number_of_links != 1
        {
            return Err(NativeCapabilityError::HardLinkAlias);
        }
        Ok(NativeIdentityV1::Windows {
            volume_serial: information.volume_serial,
            file_id: information.file_id,
        })
    }
}

#[cfg(not(any(unix, windows)))]
mod platform {
    use super::{FileAccess, NativeCapabilityError, NativeIdentityV1, NativeObjectKind};
    use std::ffi::{OsStr, OsString};
    use std::fs::File;
    use std::path::Path;

    #[derive(Debug)]
    pub(super) struct DirectoryHandle;

    macro_rules! unavailable {
        ($name:ident($($argument:ident: $type:ty),*) -> $result:ty) => {
            pub(super) fn $name($($argument: $type),*) -> Result<$result, NativeCapabilityError> {
                $(let _ = $argument;)*
                Err(NativeCapabilityError::UnsupportedPlatform)
            }
        };
    }

    unavailable!(open_absolute_directory(path: &Path) -> DirectoryHandle);
    unavailable!(open_relative_directory(root: &DirectoryHandle, relative: &Path) -> DirectoryHandle);
    unavailable!(open_relative_regular_file(root: &DirectoryHandle, relative: &Path, access: FileAccess) -> File);
    unavailable!(read_at(file: &File, offset: u64, output: &mut [u8]) -> usize);
    unavailable!(directory_identity(handle: &DirectoryHandle) -> NativeIdentityV1);
    unavailable!(file_identity(file: &File, expected: NativeObjectKind) -> NativeIdentityV1);
    unavailable!(file_identity_allow_alias(file: &File, expected: NativeObjectKind) -> NativeIdentityV1);
    unavailable!(regular_file_link_count(file: &File) -> u64);
    unavailable!(directory_entries(handle: &DirectoryHandle) -> Vec<OsString>);
    unavailable!(create_new_file(directory: &DirectoryHandle, name: &OsStr) -> File);
    unavailable!(link_no_replace(directory: &DirectoryHandle, source: &OsStr, destination: &OsStr) -> ());
    unavailable!(remove_file(directory: &DirectoryHandle, name: &OsStr) -> ());
    unavailable!(sync_directory(directory: &DirectoryHandle) -> ());
}

#[cfg(test)]
mod tests {
    use super::{
        JournalDirectoryCapability, NativeCapabilityError, NativeIdentityV1,
        RootDirectoryCapability,
    };
    use crate::{FileIdentity, PathPlatform, PlatformPath, RootIdentity, SafePathBuilder};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "ariax-native-capability-{}-{}",
                std::process::id(),
                TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn native_file_identity(file: &std::fs::File) -> NativeIdentityV1 {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let metadata = file.metadata().expect("metadata");
            NativeIdentityV1::Unix {
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
        #[cfg(windows)]
        {
            let information = ariax_windows_security::query_native_file_information(file)
                .expect("native file information");
            NativeIdentityV1::Windows {
                volume_serial: information.volume_serial,
                file_id: information.file_id,
            }
        }
    }

    #[cfg(unix)]
    fn write_at(file: &std::fs::File, bytes: &[u8], offset: u64) {
        use std::os::unix::fs::FileExt as _;
        assert_eq!(
            file.write_at(bytes, offset).expect("positional write"),
            bytes.len()
        );
    }

    #[cfg(windows)]
    fn write_at(file: &std::fs::File, bytes: &[u8], offset: u64) {
        use std::os::windows::fs::FileExt as _;
        assert_eq!(
            file.seek_write(bytes, offset).expect("positional write"),
            bytes.len()
        );
    }

    #[test]
    fn trusted_directory_identity_is_stable_and_bound_reopen_matches() {
        let directory = TestDirectory::new();
        let allowed = RootDirectoryCapability::open_trusted(&directory.0).expect("allowed root");
        let child = directory.0.join("child");
        fs::create_dir(&child).expect("child");
        let child_capability = RootDirectoryCapability::open_trusted(&child).expect("child root");
        let binding_path = PlatformPath::from_current(&child).expect("platform path");
        let identity = RootIdentity::new(child_capability.identity().encode()).expect("identity");
        let reopened = RootDirectoryCapability::open_bound(&binding_path, &identity, &[allowed])
            .expect("bound reopen");
        assert_eq!(reopened.identity(), child_capability.identity());
    }

    #[test]
    fn bound_reopen_rejects_identity_mismatch_and_outside_root() {
        let allowed_directory = TestDirectory::new();
        let outside_directory = TestDirectory::new();
        let allowed =
            RootDirectoryCapability::open_trusted(&allowed_directory.0).expect("allowed root");
        let outside_path = PlatformPath::from_current(&outside_directory.0).expect("path");
        let outside_identity = RootDirectoryCapability::open_trusted(&outside_directory.0)
            .expect("outside")
            .identity();
        let expected = RootIdentity::new(outside_identity.encode()).expect("identity");
        assert!(matches!(
            RootDirectoryCapability::open_bound(
                &outside_path,
                &expected,
                std::slice::from_ref(&allowed),
            ),
            Err(NativeCapabilityError::OutsideAllowedRoot)
        ));

        let allowed_path = PlatformPath::from_current(&allowed_directory.0).expect("path");
        assert!(matches!(
            RootDirectoryCapability::open_bound(&allowed_path, &expected, &[allowed]),
            Err(NativeCapabilityError::IdentityMismatch)
        ));
    }

    #[test]
    fn selected_regular_file_identity_is_revalidated() {
        let directory = TestDirectory::new();
        let path = directory.0.join("selected.bin");
        fs::write(&path, b"payload").expect("file");
        let root = RootDirectoryCapability::open_trusted(&directory.0).expect("root");
        let journal = JournalDirectoryCapability::open_trusted(&directory.0).expect("journal");
        let file = journal
            .open_regular_file("selected.bin".as_ref(), false)
            .expect("file open");
        let identity = native_file_identity(&file);
        let safe = SafePathBuilder::from_user_path("selected.bin", PathPlatform::current())
            .expect("safe path");
        root.verify_file(
            &safe,
            &FileIdentity::new(identity.encode()).expect("identity"),
        )
        .expect("verified file");
    }

    #[test]
    fn root_file_capability_creates_and_reopens_for_random_writes() {
        let directory = TestDirectory::new();
        let root = RootDirectoryCapability::open_trusted(&directory.0).expect("root");
        let safe = SafePathBuilder::from_user_path("output.bin", PathPlatform::current())
            .expect("safe path");
        let created = root.create_new_file(&safe).expect("create output");
        created.set_len(6).expect("set output length");
        let identity = FileIdentity::new(created.identity().encode()).expect("identity");
        let created_file = created.try_clone_file().expect("clone created file");
        write_at(&created_file, b"cd", 2);
        drop(created_file);
        drop(created);

        let reopened = root
            .open_existing_file(&safe, &identity)
            .expect("reopen output");
        let reopened_file = reopened.try_clone_file().expect("clone reopened file");
        write_at(&reopened_file, b"ab", 0);
        write_at(&reopened_file, b"ef", 4);
        reopened.sync_all().expect("sync output");
        assert_eq!(reopened.len().expect("output length"), 6);
        let mut readback = [0_u8; 4];
        reopened
            .read_exact_at(1, &mut readback)
            .expect("positional readback");
        assert_eq!(&readback, b"bcde");
        let verification = reopened
            .try_clone_capability()
            .expect("clone verification capability");
        assert_eq!(verification.identity(), reopened.identity());
        let mut cloned_readback = [0_u8; 6];
        verification
            .read_exact_at(0, &mut cloned_readback)
            .expect("cloned positional readback");
        assert_eq!(&cloned_readback, b"abcdef");
        assert_eq!(
            fs::read(directory.0.join("output.bin")).expect("read output"),
            b"abcdef"
        );
    }

    #[test]
    fn selected_regular_file_rejects_replacement_and_hard_link_aliases() {
        let directory = TestDirectory::new();
        let path = directory.0.join("selected.bin");
        fs::write(&path, b"first").expect("file");
        let root = RootDirectoryCapability::open_trusted(&directory.0).expect("root");
        let journal = JournalDirectoryCapability::open_trusted(&directory.0).expect("journal");
        let file = journal
            .open_regular_file("selected.bin".as_ref(), false)
            .expect("file open");
        let identity = FileIdentity::new(native_file_identity(&file).encode()).expect("identity");
        drop(file);
        let safe = SafePathBuilder::from_user_path("selected.bin", PathPlatform::current())
            .expect("safe path");

        fs::remove_file(&path).expect("remove original");
        fs::write(&path, b"replacement").expect("replacement");
        assert!(matches!(
            root.verify_file(&safe, &identity),
            Err(NativeCapabilityError::IdentityMismatch)
        ));

        let replacement = journal
            .open_regular_file("selected.bin".as_ref(), false)
            .expect("replacement open");
        let replacement_identity =
            FileIdentity::new(native_file_identity(&replacement).encode()).expect("identity");
        drop(replacement);
        fs::hard_link(&path, directory.0.join("alias.bin")).expect("hard link alias");
        assert_eq!(
            journal
                .regular_file_link_count("selected.bin".as_ref())
                .expect("publication link count"),
            2
        );
        assert!(
            journal
                .same_regular_file("selected.bin".as_ref(), "alias.bin".as_ref())
                .expect("same-file identity")
        );
        assert!(matches!(
            root.verify_file(&safe, &replacement_identity),
            Err(NativeCapabilityError::HardLinkAlias)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn intermediate_symlink_is_never_followed() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let outside = TestDirectory::new();
        symlink(&outside.0, directory.0.join("linked")).expect("symlink");
        let root = JournalDirectoryCapability::open_trusted(&directory.0).expect("root");
        assert!(
            root.open_directory(PathBuf::from("linked").as_path())
                .is_err()
        );
    }
}
