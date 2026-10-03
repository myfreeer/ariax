#![forbid(unsafe_code)]

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "ariax-cli-permissions-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        for name in ["state", "control", "output"] {
            fs::create_dir(root.join(name)).unwrap();
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root.join("state"), fs::Permissions::from_mode(0o777)).unwrap();
        }
        Self(root)
    }
    fn run(&self, option: Option<&str>) -> std::process::Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ariax"));
        if let Some(option) = option {
            command.arg(option);
        }
        command
            .arg("--check-bootstrap")
            .arg(self.0.join("state/session.db"))
            .arg(self.0.join("control"))
            .arg(self.0.join("output"))
            .output()
            .unwrap()
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn cli_default_and_explicit_false_bootstrap_shared_state() {
    for option in [None, Some("--require-private-permissions=false")] {
        let root = Root::new();
        let output = root.run(option);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(root.0.join("state/session.db").is_file());
        assert!(root.run(option).status.success());
    }
}

#[test]
fn cli_strict_permissions_reject_shared_state_before_database_creation() {
    let root = Root::new();
    let output = root.run(Some("--require-private-permissions=true"));
    assert!(!output.status.success());
    assert!(!root.0.join("state/session.db").exists());
    assert_eq!(fs::read_dir(root.0.join("state")).unwrap().count(), 0);
}
