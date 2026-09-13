use crate::{JournalDirectoryCapability, NativeCapabilityError, PathPlatform, SafePathBuilder};
use std::ffi::OsString;
use std::io::{self, Read as _, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const SESSION_EXPORT_MAX_BYTES: usize = 4 * 1024 * 1024;
static NEXT_EXPORT: AtomicU64 = AtomicU64::new(1);

/// Reads only a bounded, no-follow regular file below an absolute native parent.
pub fn read_session_document(path: &Path) -> Result<String, NativeCapabilityError> {
    let parent = path
        .parent()
        .ok_or(NativeCapabilityError::InvalidAbsolutePath)?;
    let name = path
        .file_name()
        .ok_or(NativeCapabilityError::UnsafePathComponent)?;
    let directory = JournalDirectoryCapability::open_trusted(parent)?;
    let file = directory.open_regular_file(name, false)?;
    let length = file.metadata()?.len();
    if length > SESSION_EXPORT_MAX_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session input exceeds its byte limit",
        )
        .into());
    }
    let mut bytes = vec![0; length as usize + 1];
    let mut file = file;
    let mut used = 0;
    while used < bytes.len() {
        let read = file.read(&mut bytes[used..])?;
        if read == 0 {
            break;
        }
        used += read;
    }
    if used > length as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session input grew while reading",
        )
        .into());
    }
    bytes.truncate(used);
    String::from_utf8(bytes).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "session input is not UTF-8").into()
    })
}

/// A local destination bound to an existing native directory, independent of
/// later path substitutions. Remote RPC arguments never construct this type.
#[derive(Clone, Debug)]
pub struct SessionExportDestination {
    directory: JournalDirectoryCapability,
    name: OsString,
}

impl SessionExportDestination {
    pub fn new(path: &Path) -> Result<Self, NativeCapabilityError> {
        let parent = path
            .parent()
            .ok_or(NativeCapabilityError::InvalidAbsolutePath)?;
        let name = path
            .file_name()
            .ok_or(NativeCapabilityError::UnsafePathComponent)?;
        SafePathBuilder::from_user_path(
            name.to_str()
                .ok_or(NativeCapabilityError::UnsafePathComponent)?,
            PathPlatform::current(),
        )
        .map_err(|_| NativeCapabilityError::UnsafePathComponent)?;
        let destination = Self {
            directory: JournalDirectoryCapability::open_trusted(parent)?,
            name: name.to_owned(),
        };
        destination.validate_existing()?;
        Ok(destination)
    }

    pub fn publish(&self, bytes: &[u8]) -> Result<(), NativeCapabilityError> {
        self.publish_with_checkpoint(bytes, |_| Ok(()))
    }

    fn validate_existing(&self) -> Result<(), NativeCapabilityError> {
        match self.directory.open_regular_file(&self.name, false) {
            Ok(_) => Ok(()),
            Err(NativeCapabilityError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn publish_with_checkpoint(
        &self,
        bytes: &[u8],
        mut checkpoint: impl FnMut(PublishStage) -> Result<(), NativeCapabilityError>,
    ) -> Result<(), NativeCapabilityError> {
        if bytes.len() > SESSION_EXPORT_MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session export exceeds its byte limit",
            )
            .into());
        }
        self.validate_existing()?;
        let mut candidate = None;
        for _ in 0..32 {
            let id = NEXT_EXPORT.fetch_add(1, Ordering::Relaxed);
            let name = OsString::from(format!(
                ".ariax-session-export-{}-{id}.tmp",
                std::process::id()
            ));
            match self.directory.create_new_file(&name) {
                Ok(file) => {
                    candidate = Some((name, file));
                    break;
                }
                Err(NativeCapabilityError::Io(error))
                    if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        let (name, mut file) = candidate.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "session export temporary names are occupied",
            )
        })?;
        let result = (|| {
            file.write_all(bytes)?;
            checkpoint(PublishStage::Written)?;
            file.sync_all()?;
            checkpoint(PublishStage::Synced)?;
            drop(file);
            self.directory.rename_replace(&name, &self.name)?;
            checkpoint(PublishStage::Renamed)?;
            self.directory.sync()
        })();
        // After rename this name no longer exists. Before rename it is only
        // the private temporary created by this operation.
        let _ = self.directory.remove_file(&name);
        result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishStage {
    Written,
    Synced,
    Renamed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "ariax-session-export-{}-{}",
                std::process::id(),
                NEXT_EXPORT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).expect("directory");
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn export_publication_keeps_complete_old_or_new_bytes_at_every_failure_boundary() {
        let root = Directory::new();
        let path = root.0.join("session-会话.json");
        let destination = SessionExportDestination::new(&path).expect("destination");
        destination
            .publish(b"old complete document")
            .expect("initial export");
        for point in [
            PublishStage::Written,
            PublishStage::Synced,
            PublishStage::Renamed,
        ] {
            destination
                .publish(b"old complete document")
                .expect("reset");
            assert!(
                destination
                    .publish_with_checkpoint(b"new complete document", |stage| {
                        if stage == point {
                            Err(io::Error::other("injected export failure").into())
                        } else {
                            Ok(())
                        }
                    })
                    .is_err()
            );
            assert_eq!(
                fs::read(&path).expect("export"),
                if point == PublishStage::Renamed {
                    b"new complete document"
                } else {
                    b"old complete document"
                }
            );
            assert_eq!(fs::read_dir(&root.0).expect("directory").count(), 1);
        }
        assert!(
            destination
                .publish(&vec![0; SESSION_EXPORT_MAX_BYTES + 1])
                .is_err()
        );
        assert!(SessionExportDestination::new(Path::new("relative.json")).is_err());
        assert!(SessionExportDestination::new(&root.0.join("../escape.json")).is_err());
        let directory_target = root.0.join("directory");
        fs::create_dir(&directory_target).expect("directory target");
        assert!(SessionExportDestination::new(&directory_target).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
                0o600
            );
        }
        #[cfg(windows)]
        ariax_windows_security::verify_private_file(&path).expect("private export ACL");
    }

    #[cfg(unix)]
    #[test]
    fn export_rejects_symlink_destinations_and_uses_held_parent_identity() {
        use std::os::unix::fs::symlink;
        let root = Directory::new();
        let original = root.0.join("original");
        let moved = root.0.join("moved");
        fs::create_dir(&original).expect("original");
        let outside = root.0.join("outside");
        fs::write(&outside, b"untouched").expect("outside");
        symlink(&outside, original.join("link.json")).expect("symlink");
        assert!(SessionExportDestination::new(&original.join("link.json")).is_err());
        let destination = SessionExportDestination::new(&original.join("session.json"))
            .expect("bound destination");
        fs::rename(&original, &moved).expect("move parent");
        fs::create_dir(&original).expect("replacement parent");
        destination
            .publish(b"bound export")
            .expect("descriptor publication");
        assert_eq!(
            fs::read(moved.join("session.json")).expect("held directory file"),
            b"bound export"
        );
        assert!(!original.join("session.json").exists());
        assert_eq!(fs::read(outside).expect("outside"), b"untouched");
    }

    #[test]
    fn export_crash_recovers_a_complete_document() {
        for point in ["written", "synced", "renamed"] {
            let root = Directory::new();
            let path = root.0.join("session.json");
            SessionExportDestination::new(&path)
                .expect("destination")
                .publish(b"old")
                .expect("old");
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "session_export::tests::export_crash_child",
                        "--nocapture",
                    ])
                    .env("ARIAX_EXPORT_CRASH_PATH", &path)
                    .env("ARIAX_EXPORT_CRASH_POINT", point)
                    .status()
                    .expect("export child");
            assert_eq!(status.code(), Some(77));
            assert_eq!(
                fs::read(path).expect("complete export"),
                if point == "renamed" { b"new" } else { b"old" }
            );
        }
    }

    #[test]
    fn local_session_input_rejects_nonfiles_invalid_text_and_oversized_documents() {
        let root = Directory::new();
        let path = root.0.join("input.txt");
        fs::write(&path, b"http://example.test/file\n  pause=true\n").expect("input");
        assert_eq!(
            read_session_document(&path).expect("read"),
            "http://example.test/file\n  pause=true\n"
        );
        fs::write(&path, [0xff]).expect("non-UTF8");
        assert!(read_session_document(&path).is_err());
        fs::File::create(&path)
            .expect("large input")
            .set_len(SESSION_EXPORT_MAX_BYTES as u64 + 1)
            .expect("length");
        assert!(read_session_document(&path).is_err());
        assert!(read_session_document(&root.0).is_err());
        assert!(read_session_document(Path::new("relative.txt")).is_err());
        #[cfg(unix)]
        {
            let link = root.0.join("link.txt");
            std::os::unix::fs::symlink(&path, &link).expect("symlink");
            assert!(read_session_document(&link).is_err());
        }
    }

    #[test]
    fn export_crash_child() {
        let Some(path) = std::env::var_os("ARIAX_EXPORT_CRASH_PATH") else {
            return;
        };
        let point = std::env::var("ARIAX_EXPORT_CRASH_POINT").expect("point");
        SessionExportDestination::new(Path::new(&path))
            .expect("destination")
            .publish_with_checkpoint(b"new", |stage| {
                if matches!(
                    (point.as_str(), stage),
                    ("written", PublishStage::Written)
                        | ("synced", PublishStage::Synced)
                        | ("renamed", PublishStage::Renamed)
                ) {
                    std::process::exit(77);
                }
                Ok(())
            })
            .expect("export");
        panic!("crash point was not reached");
    }
}
