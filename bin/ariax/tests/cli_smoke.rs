#![forbid(unsafe_code)]

use std::process::Command;

#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
static TEST_ID: AtomicU64 = AtomicU64::new(1);

fn ariax() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ariax"))
}

#[test]
fn version_reports_product_and_workspace_version() {
    let output = ariax().arg("--version").output().expect("run ariax");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "ariax 0.1.0\n");
    assert!(output.stderr.is_empty());
}

#[test]
fn help_succeeds() {
    let output = ariax().arg("--help").output().expect("run ariax");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: ariax"));
}

#[test]
fn no_arguments_show_help() {
    let output = ariax().output().expect("run ariax");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: ariax"));
}

#[test]
fn unknown_argument_is_rejected() {
    let output = ariax().arg("--not-an-option").output().expect("run ariax");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown argument"));
}

#[test]
fn multiple_arguments_are_rejected() {
    let output = ariax()
        .args(["--help", "--version"])
        .output()
        .expect("run ariax");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("only one"));
}

#[cfg(unix)]
#[test]
fn bootstrap_check_runs_the_publication_last_empty_session_path() {
    let root = private_test_directory();
    let control = root.join("control");
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(&control).expect("create control directory");
    let output = ariax()
        .arg("--check-bootstrap")
        .arg(root.join("session.db"))
        .arg(&control)
        .output()
        .expect("run bootstrap check");
    let _ = fs::remove_dir_all(&root);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("bootstrap ok: 0 tasks"));
}

#[cfg(unix)]
fn private_test_directory() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "ariax-cli-bootstrap-{}-{}",
        std::process::id(),
        TEST_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(&path).expect("create private test root");
    path
}
