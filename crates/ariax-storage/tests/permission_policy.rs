#![forbid(unsafe_code)]

use ariax_storage::{SessionStore, SessionStoreConfig, SessionStoreError};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "ariax-permission-policy-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o777)).unwrap();
        }
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn default_policy_accepts_shared_directory_preserves_lock_and_reopens() {
    let root = Root::new();
    let database = root.0.join("session.db");
    let config = SessionStoreConfig::default();
    assert!(!config.require_private_permissions);
    let store = SessionStore::open(&database, config).unwrap();
    store.integrity_check().unwrap();
    assert!(matches!(
        SessionStore::open(&database, config),
        Err(SessionStoreError::OwnerLockBusy)
    ));
    drop(store);
    SessionStore::open(&database, config)
        .unwrap()
        .integrity_check()
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&root.0).unwrap().permissions().mode() & 0o777,
            0o777
        );
    }
}

#[test]
fn strict_policy_rejects_shared_directory_before_creating_state() {
    let root = Root::new();
    let config = SessionStoreConfig {
        require_private_permissions: true,
        ..SessionStoreConfig::default()
    };
    let error = SessionStore::open(root.0.join("session.db"), config)
        .err()
        .expect("shared parent rejected");
    assert!(matches!(
        error,
        SessionStoreError::Io {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        }
    ));
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 0);
}

#[test]
fn default_policy_still_rejects_orphan_sidecars_without_mutation() {
    let root = Root::new();
    let sidecar = root.0.join("session.db-wal");
    fs::write(&sidecar, b"untrusted sidecar").unwrap();
    assert!(SessionStore::open(root.0.join("session.db"), SessionStoreConfig::default()).is_err());
    assert_eq!(fs::read(&sidecar).unwrap(), b"untrusted sidecar");
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn default_policy_preserves_existing_file_mode_and_rejects_symlinks_and_hardlinks() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let root = Root::new();
    let database = root.0.join("session.db");
    let config = SessionStoreConfig::default();
    drop(SessionStore::open(&database, config).unwrap());
    fs::set_permissions(&database, fs::Permissions::from_mode(0o666)).unwrap();
    let before = fs::metadata(&database).unwrap().permissions().mode();
    drop(SessionStore::open(&database, config).unwrap());
    assert_eq!(
        fs::metadata(&database).unwrap().permissions().mode(),
        before
    );
    let alias = root.0.join("alias.db");
    symlink(&database, &alias).unwrap();
    assert!(SessionStore::open(&alias, config).is_err());
    fs::remove_file(&alias).unwrap();
    fs::hard_link(&database, &alias).unwrap();
    assert!(SessionStore::open(&database, config).is_err());
    assert!(SessionStore::open(&alias, config).is_err());
}

#[test]
fn backup_inherits_default_permission_policy_and_preserves_no_clobber() {
    let root = Root::new();
    let store =
        SessionStore::open(root.0.join("session.db"), SessionStoreConfig::default()).unwrap();
    let backup = root.0.join("backup.db");
    store.backup_to(&backup).unwrap();
    let before = fs::read(&backup).unwrap();
    assert!(matches!(
        store.backup_to(&backup),
        Err(SessionStoreError::BackupPathExists)
    ));
    assert_eq!(fs::read(&backup).unwrap(), before);
}
