use crate::{BtError, FileMapping};
use ariax_storage::{PathPlatform, RootDirectoryCapability, SafePathBuilder};
use std::path::Path;

/// A stable native root plus the first full-build protection precondition.
/// Libtorrent still owns payload I/O; this does not claim custom-storage safety.
#[derive(Clone, Debug)]
pub struct ProtectedRoot {
    capability: RootDirectoryCapability,
}

impl ProtectedRoot {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BtError> {
        let capability = RootDirectoryCapability::open_trusted(path.as_ref())
            .map_err(|_| BtError::UnsafePath)?;
        protected(capability.display(), true)?;
        Ok(Self { capability })
    }

    pub fn path(&self) -> &Path {
        self.capability.display()
    }
    pub fn identity(&self) -> Box<[u8]> {
        self.capability.identity().encode()
    }

    pub fn revalidate(&self) -> Result<(), BtError> {
        let current = Self::open(self.path())?;
        if current.capability.identity() != self.capability.identity() {
            return Err(BtError::IdentityMismatch);
        }
        Ok(())
    }

    pub fn validate_mapping(
        &self,
        mapping: &[FileMapping],
        allow_existing: bool,
    ) -> Result<(), BtError> {
        self.revalidate()?;
        for file in mapping.iter().filter(|file| !file.padding) {
            let safe = SafePathBuilder::from_user_path(&file.path, PathPlatform::Windows)
                .map_err(|_| BtError::UnsafePath)?;
            let components = safe.components().collect::<Vec<_>>();
            let mut path = self.path().to_owned();
            for (index, component) in components.iter().enumerate() {
                path.push(component);
                match std::fs::symlink_metadata(&path) {
                    Ok(metadata) => {
                        let directory = index + 1 < components.len();
                        if metadata.file_type().is_symlink()
                            || metadata.is_dir() != directory
                            || !directory && (!metadata.is_file() || !allow_existing)
                        {
                            return Err(BtError::UnsafePath);
                        }
                        protected(&path, directory)?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                    Err(_) => return Err(BtError::UnsafePath),
                }
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn protected(path: &Path, directory: bool) -> Result<(), BtError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path).map_err(|_| BtError::UnprotectedRoot)?;
    if metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o022 != 0
        || !directory && metadata.nlink() != 1
    {
        return Err(BtError::UnprotectedRoot);
    }
    Ok(())
}

#[cfg(windows)]
fn protected(path: &Path, directory: bool) -> Result<(), BtError> {
    if directory {
        ariax_windows_security::verify_private_directory(path)
    } else {
        ariax_windows_security::verify_private_file(path)
    }
    .map_err(|_| BtError::UnprotectedRoot)
}

#[cfg(not(any(unix, windows)))]
fn protected(_: &Path, _: bool) -> Result<(), BtError> {
    Err(BtError::UnprotectedRoot)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn protected_root_rejects_shared_writes_links_and_replacement() {
        let root = std::env::temp_dir().join(format!("ariax-bt-root-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let protected = ProtectedRoot::open(&root).unwrap();
        let mapping = [FileMapping {
            index: 0,
            path: "payload".into(),
            length: 1,
            offset: 0,
            selected: true,
            padding: false,
        }];
        protected.validate_mapping(&mapping, false).unwrap();
        symlink(root.join("outside"), root.join("payload")).unwrap();
        assert_eq!(
            protected.validate_mapping(&mapping, true),
            Err(BtError::UnsafePath)
        );
        std::fs::remove_file(root.join("payload")).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert_eq!(
            ProtectedRoot::open(&root).unwrap_err(),
            BtError::UnprotectedRoot
        );
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let moved = root.with_extension("moved");
        std::fs::rename(&root, &moved).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(protected.revalidate(), Err(BtError::IdentityMismatch));
        std::fs::remove_dir(&root).unwrap();
        std::fs::remove_dir(moved).unwrap();
    }
}
