#![forbid(unsafe_code)]

use std::process::Command;

#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::net::{Shutdown, TcpListener};
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(unix)]
use std::sync::mpsc;
#[cfg(unix)]
use std::thread;

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
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage: ariax"));
    assert!(stdout.contains("--download-http-pinned"));
    assert!(stdout.contains("--resume-http-pinned"));
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

#[test]
fn pinned_http_control_rejects_invalid_identity_before_network_or_filesystem_work() {
    let output = ariax()
        .args([
            "--download-http-pinned",
            "bad-gid",
            "01010101010101010101010101010101",
            "http://127.0.0.1:1/file",
            "127.0.0.1:1",
            ".",
            "output.bin",
            "journal",
        ])
        .output()
        .expect("run pinned HTTP control");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid GID"));
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
#[test]
fn pinned_http_control_streams_through_storage_and_reports_completion() {
    let root = private_test_directory();
    let output_root = root.join("output");
    let journal_root = root.join("journal");
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(&output_root)
        .expect("create output directory");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP fixture");
    let peer = listener.local_addr().expect("fixture address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept HTTP request");
        let mut request = [0_u8; 4096];
        let mut used = 0_usize;
        while used < request.len() {
            let read = stream
                .read(&mut request[used..])
                .expect("read HTTP request");
            if read == 0 {
                break;
            }
            used += read;
            if request[..used]
                .windows(4)
                .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nariax!")
            .expect("write HTTP response");
    });
    let output = ariax()
        .args([
            "--download-http-pinned".into(),
            "0000000000000007".into(),
            "01010101010101010101010101010101".into(),
            format!("http://127.0.0.1:{}/file", peer.port()).into(),
            peer.to_string().into(),
            output_root.as_os_str().to_owned(),
            "download.bin".into(),
            journal_root.as_os_str().to_owned(),
            "4".into(),
        ])
        .output()
        .expect("run pinned HTTP transfer");
    server.join().expect("join HTTP fixture");
    let payload = fs::read(output_root.join("download.bin")).expect("read downloaded output");
    let _removed = fs::remove_dir_all(&root);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(payload, b"ariax!");
    assert!(String::from_utf8_lossy(&output.stdout).contains("download complete"));
}

#[cfg(unix)]
#[test]
fn pinned_http_control_recovers_and_resumes_with_range_and_if_range() {
    let root = private_test_directory();
    let output_root = root.join("output");
    let journal_root = root.join("journal");
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(&output_root)
        .expect("create output directory");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP fixture");
    let peer = listener.local_addr().expect("fixture address");
    let (requests, received) = mpsc::channel();
    let server = thread::spawn(move || {
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdef".as_slice(),
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nefghij".as_slice(),
        ] {
            let (mut stream, _) = listener.accept().expect("accept HTTP request");
            let mut request = vec![0_u8; 4096];
            let mut used = 0_usize;
            while used < request.len() {
                let read = stream
                    .read(&mut request[used..])
                    .expect("read HTTP request");
                if read == 0 {
                    break;
                }
                used += read;
                if request[..used]
                    .windows(4)
                    .any(|window| window == b"\r\n\r\n")
                {
                    break;
                }
            }
            request.truncate(used);
            requests.send(request).expect("publish request");
            stream.write_all(response).expect("write HTTP response");
            stream.flush().expect("flush HTTP response");
            thread::sleep(std::time::Duration::from_millis(25));
            stream
                .shutdown(Shutdown::Both)
                .expect("close HTTP response");
        }
    });
    let uri = format!("http://127.0.0.1:{}/file", peer.port());
    let gid = "0000000000000007";
    let journal_id = "02020202020202020202020202020202";
    let first = ariax()
        .args([
            "--download-http-pinned".into(),
            gid.into(),
            journal_id.into(),
            uri.clone().into(),
            peer.to_string().into(),
            output_root.as_os_str().to_owned(),
            "download.bin".into(),
            journal_root.as_os_str().to_owned(),
            "4".into(),
        ])
        .output()
        .expect("run initial pinned HTTP transfer");
    assert!(!first.status.success());
    assert!(String::from_utf8_lossy(&first.stderr).contains("short_body"));
    let _fresh_request = received
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("fresh request");

    let resumed = ariax()
        .args([
            "--resume-http-pinned".into(),
            gid.into(),
            journal_id.into(),
            uri.into(),
            peer.to_string().into(),
            output_root.as_os_str().to_owned(),
            journal_root.as_os_str().to_owned(),
        ])
        .output()
        .expect("run pinned HTTP resume");
    server.join().expect("join HTTP fixture");
    let resume_request = received
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("resume request");
    let resume_request = String::from_utf8_lossy(&resume_request).to_ascii_lowercase();
    let payload = fs::read(output_root.join("download.bin")).expect("read resumed output");
    let _removed = fs::remove_dir_all(&root);
    assert!(
        resumed.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert_eq!(payload, b"abcdefghij");
    assert!(resume_request.contains("range: bytes=4-\r\n"));
    assert!(resume_request.contains("if-range: \"v1\"\r\n"));
    assert!(String::from_utf8_lossy(&resumed.stdout).contains("download resumed"));
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
