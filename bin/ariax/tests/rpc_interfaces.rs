#![forbid(unsafe_code)]

use serde_json::{Value, json};
use std::io::{BufRead as _, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "ariax-rpc-interfaces-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .expect("private root");
        }
        #[cfg(windows)]
        ariax_windows_security::create_private_directory(&path).expect("private root");
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Process(Child);
impl Process {
    fn finish(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(8);
        let status = loop {
            if let Some(status) = self.0.try_wait().expect("process state") {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "RPC process did not stop within its drain deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        let mut stderr = String::new();
        self.0
            .stderr
            .take()
            .expect("stderr")
            .read_to_string(&mut stderr)
            .expect("stderr text");
        assert!(status.success(), "{stderr}");
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ariax"));
    command
        .env_remove("ARIAX_RPC_SECRET")
        .env_remove("ARIAX_RPC_USER")
        .env_remove("ARIAX_RPC_PASSWD");
    command
}

fn http_call(address: SocketAddr, request: Value) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut socket = loop {
        if let Ok(socket) = TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            break socket;
        }
        assert!(Instant::now() < deadline, "RPC listener did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("read timeout");
    socket
        .set_write_timeout(Some(Duration::from_secs(3)))
        .expect("write timeout");
    let body = serde_json::to_vec(&request).expect("request");
    write!(socket, "POST /jsonrpc HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).expect("header");
    socket.write_all(&body).expect("body");
    let mut response = Vec::new();
    socket
        .take(16 * 1024 * 1024)
        .read_to_end(&mut response)
        .expect("bounded response");
    let start = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("header end")
        + 4;
    serde_json::from_slice(&response[start..]).expect("JSON reply")
}

fn stdio_reader(
    mut output: std::process::ChildStdout,
    ndjson: bool,
) -> std::sync::mpsc::Receiver<Value> {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        if ndjson {
            for line in std::io::BufReader::new(output).lines() {
                let Ok(line) = line else {
                    break;
                };
                let Ok(value) = serde_json::from_str(&line) else {
                    break;
                };
                if sender.send(value).is_err() {
                    break;
                }
            }
        } else {
            loop {
                let mut header = Vec::new();
                let mut byte = [0];
                while !header.ends_with(b"\r\n\r\n") {
                    if output.read_exact(&mut byte).is_err() || header.len() > 16 * 1024 {
                        return;
                    }
                    header.push(byte[0]);
                }
                let Some(size) = std::str::from_utf8(&header)
                    .ok()
                    .and_then(|header| header.strip_prefix("Content-Length: "))
                    .and_then(|number| number.trim().parse::<usize>().ok())
                    .filter(|size| *size <= 16 * 1024 * 1024)
                else {
                    return;
                };
                let mut body = vec![0; size];
                if output.read_exact(&mut body).is_err() {
                    return;
                }
                let Ok(value) = serde_json::from_slice(&body) else {
                    return;
                };
                if sender.send(value).is_err() {
                    return;
                }
            }
        }
    });
    receiver
}

#[test]
fn combined_http_and_both_stdio_framings_share_tasks_and_honor_eof() {
    for framing in ["content-length", "ndjson"] {
        for eof in ["shutdown", "close-transport", "ignore", "open-input"] {
            let root = Root::new();
            let listener = TcpListener::bind("127.0.0.1:0").expect("reserve address");
            let address = listener.local_addr().expect("address");
            drop(listener);
            let config = root.0.join("ariax.conf");
            std::fs::write(&config, "split=3\n").expect("config");
            let mut process = Process(
                command()
                    .arg("--profile=compact")
                    .arg("--rpc-transport=http+stdio")
                    .arg(format!("--rpc-stdio-framing={framing}"))
                    .arg(format!(
                        "--rpc-stdio-eof={}",
                        if eof == "open-input" { "shutdown" } else { eof }
                    ))
                    .arg("--rpc-stdio-events=false")
                    .arg("--rpc-compat=extended")
                    .arg(format!("--conf-path={}", config.display()))
                    .arg("--rpc")
                    .arg(root.0.join("session.db"))
                    .arg(root.0.join("control"))
                    .arg(root.0.join("output"))
                    .arg(address.to_string())
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("combined RPC"),
            );
            let responses = stdio_reader(
                process.0.stdout.take().expect("stdout"),
                framing == "ndjson",
            );
            let mut input = process.0.stdin.take().expect("stdin");
            let request = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":"aria2.addUri","params":[["http://example.test/file"],{"pause":true}]})).expect("request");
            if framing == "content-length" {
                write!(input, "Content-Length: {}\r\n\r\n", request.len()).expect("header");
            }
            input.write_all(&request).expect("request");
            if framing == "ndjson" {
                input.write_all(b"\n").expect("delimiter");
            }
            input.flush().expect("flush");
            let response = responses
                .recv_timeout(Duration::from_secs(5))
                .expect("stdio reply");
            let gid = response["result"].as_str().expect("GID");
            let mut input = Some(input);
            if eof != "open-input" {
                drop(input.take());
            }
            if eof != "shutdown" {
                let options = http_call(
                    address,
                    json!({"jsonrpc":"2.0","id":2,"method":"aria2.getOption","params":[gid]}),
                );
                assert_eq!(options["result"]["split"], "3");
                let diagnostics = http_call(
                    address,
                    json!({"jsonrpc":"2.0","id":3,"method":"ariax.getDiagnostics"}),
                );
                assert_eq!(diagnostics["result"]["profile"], "compact");
                let shutdown = http_call(
                    address,
                    json!({"jsonrpc":"2.0","id":4,"method":"aria2.shutdown"}),
                );
                assert_eq!(shutdown["result"], "OK");
            }
            process.finish();
            drop(input);
        }
    }
}

#[test]
fn cli_json_call_uses_the_shared_catalog_and_compatibility_rejections() {
    let root = Root::new();
    let output = command().arg("--rpc-compat=strict").arg("--rpc-call")
        .arg(root.0.join("session.db")).arg(root.0.join("control")).arg(root.0.join("output"))
        .arg(r#"[{"jsonrpc":"2.0","id":1,"method":"system.listMethods"},{"jsonrpc":"2.0","id":2,"method":"aria2.addMetalink"}]"#)
        .output().expect("CLI JSON call");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: Value = serde_json::from_slice(&output.stdout).expect("JSON");
    assert!(
        response[0]["result"]
            .as_array()
            .expect("catalog")
            .contains(&json!("ariax.reloadConfig"))
    );
    assert_eq!(response[1]["error"]["data"]["feature"], "metalink");
}

#[test]
fn cli_json_call_rejects_service_only_options_before_creating_state() {
    for option in [
        "--rpc-transport=http",
        "--rpc-stdio-events=false",
        "--conf-path=missing.conf",
        "--slow-slot-policy=demote",
    ] {
        let root = Root::new();
        let output = command()
            .arg(option)
            .arg("--rpc-call")
            .arg(root.0.join("session.db"))
            .arg(root.0.join("control"))
            .arg(root.0.join("output"))
            .arg(r#"{"jsonrpc":"2.0","id":1,"method":"system.listMethods"}"#)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(!root.0.join("session.db").exists());
    }
}
