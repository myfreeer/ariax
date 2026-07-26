#![forbid(unsafe_code)]

use std::process::Command;

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
