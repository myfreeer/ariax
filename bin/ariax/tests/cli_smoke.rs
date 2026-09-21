#![forbid(unsafe_code)]

use std::process::Command;

#[cfg(unix)]
use std::process::Stdio;

#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::net::TcpListener;
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
    let mut command = Command::new(env!("CARGO_BIN_EXE_ariax"));
    command
        .env_remove("ARIAX_RPC_SECRET")
        .env_remove("ARIAX_RPC_USER")
        .env_remove("ARIAX_RPC_PASSWD");
    command
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
    assert!(stdout.contains("--profile=auto|concurrency|throughput|latency|compact"));
    assert!(stdout.contains("--download-http-pinned"));
    assert!(stdout.contains("--resume-http-pinned"));
    assert!(stdout.contains("--add-uri"));
    assert!(stdout.contains("--status"));
}

#[test]
fn no_arguments_show_help() {
    let output = ariax().output().expect("run ariax");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: ariax"));
}

#[test]
fn local_session_input_and_shutdown_export_round_trip_without_partial_import() {
    let root = std::env::temp_dir().join(format!("ariax-cli-session-{}", std::process::id()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::create_dir_all(&root).expect("root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("private root");
    }
    #[cfg(windows)]
    ariax_windows_security::create_private_directory(&root).expect("private root");
    let input = root.join("input.txt");
    let export = root.join("export.json");
    std::fs::write(
        &input,
        "http://example.test/file\n  pause=true\n  split=3\n",
    )
    .expect("input");
    let output = ariax()
        .arg(format!("--input-file={}", input.display()))
        .arg(format!("--save-session={}", export.display()))
        .arg("--save-session-format=json")
        .arg("--rpc-stdio")
        .arg(root.join("session.db"))
        .arg(root.join("control"))
        .arg(root.join("output"))
        .output()
        .expect("run local session CLI");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&export).expect("export")).expect("JSON");
    assert_eq!(document["tasks"].as_array().expect("tasks").len(), 1);
    assert_eq!(document["tasks"][0]["options"]["split"], "3");
    assert_eq!(document["tasks"][0]["options"]["pause"], "true");
    std::fs::write(
        &input,
        "http://example.test/valid\n  pause=true\nhttp://example.test/invalid\n  split=0\n",
    )
    .expect("invalid suffix");
    let rejected = ariax()
        .arg(format!("--input-file={}", input.display()))
        .arg(format!("--save-session={}", export.display()))
        .arg("--save-session-format=json")
        .arg("--rpc-stdio")
        .arg(root.join("session.db"))
        .arg(root.join("control"))
        .arg(root.join("output"))
        .output()
        .expect("reject invalid import");
    assert!(!rejected.status.success());
    let document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(export).expect("last export")).expect("JSON");
    assert_eq!(
        document["tasks"]
            .as_array()
            .expect("no partial import")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).expect("cleanup");
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
fn invalid_runtime_profile_is_rejected_before_startup_work() {
    let output = ariax()
        .args([
            "--profile=unbounded",
            "--rpc-stdio",
            "unused-session.db",
            "unused-control",
            "unused-output",
        ])
        .output()
        .expect("run ariax");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid runtime profile"));
}

#[test]
fn invalid_rpc_credentials_are_redacted_and_rejected_before_startup_work() {
    let root = std::env::temp_dir().join(format!(
        "ariax-invalid-rpc-auth-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    for options in [
        vec!["--rpc-user=user-canary".to_owned()],
        vec![
            "--rpc-user=user:canary".to_owned(),
            "--rpc-passwd=pass-canary".to_owned(),
        ],
        vec![
            "--rpc-user=user-canary".to_owned(),
            format!("--rpc-passwd={}pass-canary", "x".repeat(4096)),
        ],
    ] {
        let output = ariax()
            .args(options)
            .arg("--rpc-http")
            .arg(root.join("session.db"))
            .arg(root.join("control"))
            .arg(root.join("output"))
            .arg("127.0.0.1:0")
            .output()
            .expect("run ariax");
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("canary"));
        assert!(!root.exists());
    }
    let output = ariax()
        .env("ARIAX_RPC_PASSWD", "pass-canary")
        .arg("--rpc-stdio")
        .arg(root.join("session.db"))
        .arg(root.join("control"))
        .arg(root.join("output"))
        .output()
        .expect("run ariax");
    assert_eq!(output.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("canary"));
    assert!(!root.exists());
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

#[test]
fn direct_control_rejects_invalid_gid_before_bootstrap_work() {
    let output = ariax()
        .args([
            "--status",
            "unused-session.db",
            "unused-control",
            "unused-output",
            "bad-gid",
        ])
        .output()
        .expect("run direct status control");
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
            drop(stream);
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
#[test]
fn stdio_rpc_admits_paused_http_task_persists_metadata_and_shuts_down_on_eof() {
    let root = private_test_directory();
    let database = root.join("session.db");
    let control = root.join("control");
    let output_root = root.join("output");
    let mut child = ariax()
        .arg("--profile=compact")
        .arg("--rpc-stdio")
        .arg(&database)
        .arg(&control)
        .arg(&output_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start stdio RPC process");
    let request = serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "aria2.addUri",
        "params": [["http://example.invalid/file"], {
            "out": "queued.bin",
            "pause": true
        }]
    }))
    .expect("encode JSON-RPC request");
    {
        let mut stdin = child.stdin.take().expect("child stdin");
        write!(stdin, "Content-Length: {}\r\n\r\n", request.len()).expect("write frame header");
        stdin.write_all(&request).expect("write frame body");
    }
    let process = child.wait_with_output().expect("wait for stdio RPC exit");
    assert!(
        process.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&process.stderr)
    );
    let separator = process
        .stdout
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response header terminator");
    let header = std::str::from_utf8(&process.stdout[..separator]).expect("response header UTF-8");
    let declared = header
        .strip_prefix("Content-Length: ")
        .expect("Content-Length response header")
        .parse::<usize>()
        .expect("numeric response length");
    let body_start = separator + 4;
    let body_end = body_start + declared;
    assert!(process.stdout.len() >= body_end);
    let body = &process.stdout[body_start..body_end];
    let response: serde_json::Value = serde_json::from_slice(body).expect("JSON-RPC response");
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    let gid = response["result"].as_str().expect("admitted GID");

    let store =
        ariax_storage::SessionStore::open(&database, ariax_storage::SessionStoreConfig::default())
            .expect("open persisted RPC session");
    let tasks = store.tasks().expect("persisted tasks");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].gid.to_string(), gid);
    assert_eq!(
        tasks[0].queue_state,
        ariax_storage::SessionQueueState::Paused
    );
    assert!(tasks[0].desired_paused);
    let sources = store.task_sources(tasks[0].gid).expect("persisted sources");
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0].persistence_safe_uri.as_deref(),
        Some("http://example.invalid/file")
    );
    assert!(!sources[0].needs_credentials);
    let options = store
        .task_options(
            tasks[0].gid,
            ariax_storage::OptionsSnapshotScope::CurrentGeneration,
            &|_: &str| true,
        )
        .expect("persisted HTTP options");
    assert!(
        options
            .entries()
            .any(|entry| entry == ("out", "queued.bin"))
    );
    drop(store);
    let _removed = fs::remove_dir_all(&root);
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
