#![forbid(unsafe_code)]

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::mpsc;
use std::thread;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("ariax-removable-http-{}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn ariax() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ariax"))
}

#[test]
fn pinned_http_control_recovers_and_resumes_with_range_and_if_range() {
    let directory = Directory::new();
    let root = &directory.0;
    let output_root = root.join("output");
    let journal_root = root.join("journal");
    fs::create_dir(&output_root).expect("create output directory");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP fixture");
    listener.set_nonblocking(true).unwrap();
    let peer = listener.local_addr().expect("fixture address");
    let (requests, received) = mpsc::channel();
    let server = thread::spawn(move || {
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabcdef".as_slice(),
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nefghij".as_slice(),
        ] {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(std::time::Instant::now() < deadline, "HTTP fixture timed out");
                        thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
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

    let payload_path = output_root.join("download.bin");
    let before = fs::read(&payload_path).unwrap();
    let mut corrupted = before.clone();
    corrupted[0] ^= 1;
    fs::write(&payload_path, &corrupted).unwrap();
    let rejected = ariax()
        .arg("--resume-http-pinned")
        .args([gid, journal_id, &uri, &peer.to_string()])
        .arg(&output_root)
        .arg(&journal_root)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("durable_piece_digest_mismatch"),
        "{}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    fs::write(&payload_path, &before).unwrap();

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
