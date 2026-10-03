use crate::{BtError, FileMapping};
use ariax_storage::{PathPlatform, RootDirectoryCapability, SafePathBuilder};
use std::path::Path;

/// A stable native root with optional strict permission protection.
/// Libtorrent still owns payload I/O; this does not claim custom-storage safety.
#[derive(Clone, Debug)]
pub struct ProtectedRoot {
    capability: RootDirectoryCapability,
    require_private_permissions: bool,
}

impl ProtectedRoot {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BtError> {
        Self::open_with_permissions(path, false)
    }

    pub fn open_with_permissions(
        path: impl AsRef<Path>,
        require_private_permissions: bool,
    ) -> Result<Self, BtError> {
        let capability = RootDirectoryCapability::open_trusted(path.as_ref())
            .map_err(|_| BtError::UnsafePath)?;
        protected(capability.display(), true, require_private_permissions)?;
        Ok(Self {
            capability,
            require_private_permissions,
        })
    }

    pub fn path(&self) -> &Path {
        self.capability.display()
    }
    pub fn identity(&self) -> Box<[u8]> {
        self.capability.identity().encode()
    }

    pub fn revalidate(&self) -> Result<(), BtError> {
        let current = Self::open_with_permissions(self.path(), self.require_private_permissions)?;
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
                        protected(&path, directory, self.require_private_permissions)?;
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
fn protected(
    path: &Path,
    directory: bool,
    require_private_permissions: bool,
) -> Result<(), BtError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path).map_err(|_| BtError::UnprotectedRoot)?;
    if (require_private_permissions
        && (metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o022 != 0))
        || !directory && metadata.nlink() != 1
    {
        return Err(BtError::UnprotectedRoot);
    }
    Ok(())
}

#[cfg(windows)]
fn protected(
    path: &Path,
    directory: bool,
    require_private_permissions: bool,
) -> Result<(), BtError> {
    if !require_private_permissions {
        return if directory {
            ariax_storage::RootDirectoryCapability::open_trusted(path)
                .map(|_| ())
                .map_err(|_| BtError::UnsafePath)
        } else {
            ariax_windows_security::verify_single_link_regular_file(path)
                .map_err(|_| BtError::UnsafePath)
        };
    }
    if directory {
        ariax_windows_security::verify_private_directory(path)
    } else {
        ariax_windows_security::verify_private_file(path)
    }
    .map_err(|_| BtError::UnprotectedRoot)
}

#[cfg(not(any(unix, windows)))]
fn protected(_: &Path, _: bool, _: bool) -> Result<(), BtError> {
    Err(BtError::UnprotectedRoot)
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;

    #[test]
    fn removable_root_revalidates_identity_and_mapping() {
        let path = std::env::temp_dir().join(format!("ariax-bt-removable-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let root = ProtectedRoot::open(&path).unwrap();
        root.revalidate().unwrap();
        let handle = ariax_windows_security::open_absolute_directory_no_reparse(&path).unwrap();
        let info = ariax_windows_security::query_native_file_information(&handle).unwrap();
        let legacy = matches!(
            info.file_id,
            ariax_windows_security::NativeFileId::Legacy { .. }
        );
        assert_eq!(root.identity()[0], if legacy { 2 } else { 1 });
        let mapping = [FileMapping {
            index: 0,
            path: "payload".into(),
            length: 1,
            offset: 0,
            selected: true,
            padding: false,
        }];
        std::fs::write(path.join("payload"), b"x").unwrap();
        root.validate_mapping(&mapping, true).unwrap();
        assert_eq!(
            root.validate_mapping(&mapping, false),
            Err(BtError::UnsafePath)
        );
        drop(handle);
        drop(root);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn optional_acl_check_preserves_strict_roots_and_rejects_payload_aliases() {
        struct Root(std::path::PathBuf);
        impl Drop for Root {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let root = Root(
            std::env::temp_dir().join(format!("ariax-bt-permission-policy-{}", std::process::id())),
        );
        ariax_windows_security::create_private_directory(&root.0).unwrap();
        ProtectedRoot::open_with_permissions(&root.0, true)
            .unwrap()
            .revalidate()
            .unwrap();
        let shared = root.0.join("inherited");
        std::fs::create_dir(&shared).unwrap();
        assert_eq!(
            ProtectedRoot::open_with_permissions(&shared, true).unwrap_err(),
            BtError::UnprotectedRoot
        );
        let accepted = ProtectedRoot::open(&shared).unwrap();
        let mapping = [FileMapping {
            index: 0,
            path: "payload".into(),
            length: 1,
            offset: 0,
            selected: true,
            padding: false,
        }];
        std::fs::write(shared.join("payload"), b"x").unwrap();
        accepted.validate_mapping(&mapping, true).unwrap();
        std::fs::hard_link(shared.join("payload"), shared.join("alias")).unwrap();
        assert_eq!(
            accepted.validate_mapping(&mapping, true),
            Err(BtError::UnsafePath)
        );
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn default_root_allows_shared_permissions_but_rejects_payload_aliases() {
        let root =
            std::env::temp_dir().join(format!("ariax-bt-shared-root-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
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
        assert_eq!(
            ProtectedRoot::open_with_permissions(&root, true).unwrap_err(),
            BtError::UnprotectedRoot
        );
        std::fs::write(root.join("payload"), b"x").unwrap();
        protected.validate_mapping(&mapping, true).unwrap();
        std::fs::hard_link(root.join("payload"), root.join("alias")).unwrap();
        assert_eq!(
            protected.validate_mapping(&mapping, true),
            Err(BtError::UnprotectedRoot)
        );
        std::fs::remove_file(root.join("alias")).unwrap();
        std::fs::remove_file(root.join("payload")).unwrap();
        symlink(root.join("outside"), root.join("payload")).unwrap();
        assert_eq!(
            protected.validate_mapping(&mapping, true),
            Err(BtError::UnsafePath)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn protected_root_rejects_shared_writes_links_and_replacement() {
        let root = std::env::temp_dir().join(format!("ariax-bt-root-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let protected = ProtectedRoot::open_with_permissions(&root, true).unwrap();
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
            ProtectedRoot::open_with_permissions(&root, true).unwrap_err(),
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
