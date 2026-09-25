use super::tests::TestDirectory;
use super::*;
use base64ct::Encoding as _;

const V1: &[u8] = include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent");
const V2: &[u8] = include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v2.torrent");
const HYBRID: &[u8] =
    include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/hybrid.torrent");

async fn metadata_server(bytes: Vec<u8>) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let bytes = bytes.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let Ok(byte) = stream.read_u8().await else {
                        return;
                    };
                    request.push(byte);
                    if request.len() > 16384 {
                        return;
                    }
                }
                let text = String::from_utf8(request).unwrap();
                let range = text.lines().find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("range: bytes=")
                        .map(str::to_owned)
                });
                let (status, extra, body) = if let Some(range) = range {
                    let (start, end) = range.split_once('-').unwrap();
                    let start = start.parse::<usize>().unwrap();
                    let end = end.parse::<usize>().unwrap().min(bytes.len() - 1);
                    (
                        "206 Partial Content",
                        format!("Content-Range: bytes {start}-{end}/{}\r\n", bytes.len()),
                        bytes[start..=end].to_vec(),
                    )
                } else {
                    ("200 OK", String::new(), bytes)
                };
                let head = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/x-bittorrent\r\nETag: \"metadata\"\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",
                    body.len()
                );
                if stream.write_all(head.as_bytes()).await.is_ok() {
                    let _ = stream.write_all(&body).await;
                }
            });
        }
    });
    (address, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrent_following_is_atomic_respects_retention_and_survives_restart() {
    for mode in ["true", "mem", "false", "invalid"] {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        super::tests::attach_loopback_worker(&mut plane, &directory);
        let (address, server) = metadata_server(if mode == "invalid" {
            b"de".to_vec()
        } else {
            V1.to_vec()
        })
        .await;
        let parent: Gid = plane.call("aria2.addUri", json!([[format!("http://{address}/metadata.torrent")], {
            "out":"metadata.torrent", "follow-torrent":if mode == "invalid" {"mem"} else {mode},
            "bt-metadata-only":"true", "enable-dht":"false", "enable-peer-exchange":"false"
        }])).unwrap().as_str().unwrap().parse().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            plane.poll_once().unwrap();
            if plane
                .engine
                .snapshot_reader()
                .load()
                .tasks()
                .values()
                .all(|task| task.snapshot.state == ariax_core::TaskState::StoppedResult)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{mode}: {:?}",
                plane.call("aria2.tellStatus", json!([parent.to_string()]))
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let status = plane
            .call("aria2.tellStatus", json!([parent.to_string()]))
            .unwrap();
        assert_eq!(
            status["status"],
            if mode == "invalid" {
                "error"
            } else {
                "complete"
            },
            "{status}"
        );
        let expanded = matches!(mode, "true" | "mem");
        assert_eq!(plane.bt.catalog.len(), usize::from(expanded));
        assert_eq!(plane.tasks.len(), 1);
        if expanded {
            assert_eq!(status["followedBy"].as_array().unwrap().len(), 1);
        }
        assert_eq!(
            std::fs::read_dir(&directory.output).unwrap().count(),
            usize::from(matches!(mode, "true" | "false"))
        );
        if matches!(mode, "true" | "false") {
            assert_eq!(
                std::fs::read(directory.output.join("metadata.torrent")).unwrap(),
                V1
            );
        } else {
            assert!(!directory.output.join("metadata.torrent").exists());
        }
        server.abort();
        plane.shutdown().unwrap();
        let mut recovered = directory.control_plane();
        assert_eq!(recovered.bt.catalog.len(), usize::from(expanded));
        assert_eq!(
            recovered
                .call("aria2.tellStatus", json!([parent.to_string()]))
                .unwrap()["followedBy"],
            status["followedBy"]
        );
        recovered.shutdown().unwrap();
    }
}

fn add_torrent(plane: &mut HttpControlPlane, bytes: &[u8], options: Value) -> Gid {
    plane
        .call(
            "aria2.addTorrent",
            json!([base64ct::Base64::encode_string(bytes), [], options]),
        )
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn bt_options_reject_invalid_selection_limits_and_unsupported_settings() {
    let options = bittorrent::Options::parse(&json!({"select-file":"1,3-5", "index-out":"1=renamed.bin", "seed-time":"0.5", "max-upload-limit":"1M"})).unwrap();
    assert_eq!(
        options
            .mapping
            .selected
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        [1, 3, 4, 5]
    );
    assert_eq!(options.settings.seed_seconds, Some(30));
    assert_eq!(options.settings.upload_limit, 1024 * 1024);
    for options in [
        json!({"select-file":"0"}),
        json!({"select-file":"3-2"}),
        json!({"index-out":"1=../outside"}),
        json!({"bt-max-peers":0}),
        json!({"bt-resume-data-limit":"65M"}),
        json!({"bt-resume-timeout":301}),
        json!({"seed-ratio":"NaN"}),
        json!({"seed-time":"-1"}),
        json!({"proxy":"http://secret@example.test"}),
        json!({"bt-tracker":"https://tracker.test/announce?passkey=secret-canary"}),
        json!({"bt-tracker":"https://user:secret-canary@tracker.test/announce"}),
    ] {
        assert!(bittorrent::Options::parse(&options).is_err());
    }
}

#[test]
fn paused_torrent_options_commit_without_native_start_and_reject_unsafe_replacements() {
    let directory = TestDirectory::new();
    let mut plane = directory.control_plane();
    let gid = add_torrent(
        &mut plane,
        V1,
        json!({"pause":true,"enable-dht":false,"enable-peer-exchange":false}),
    );
    assert!(plane.bittorrent_handle().is_none());
    let old = plane
        .call("aria2.getFiles", json!([gid.to_string()]))
        .unwrap();
    let identity = plane.bt.catalog[&gid].spec.record.binding.identity.clone();
    for invalid in [
        json!({"out":"../escape"}),
        json!({"select-file":"2"}),
        json!({"bt-tracker":"https://tracker.test/announce?token=secret"}),
    ] {
        assert!(
            plane
                .call("aria2.changeOption", json!([gid.to_string(), invalid]))
                .is_err()
        );
        assert_eq!(
            plane
                .call("aria2.getFiles", json!([gid.to_string()]))
                .unwrap(),
            old
        );
    }
    std::fs::write(directory.output.join("occupied.bin"), b"unrelated data").unwrap();
    assert!(
        plane
            .call(
                "aria2.changeOption",
                json!([gid.to_string(), {"out":"occupied.bin"}])
            )
            .is_err()
    );
    let ControlReply::Deferred(reply) = plane
        .begin_call_admitted(
            "aria2.changeOption",
            json!([gid.to_string(), {
                "out":"renamed.bin", "bt-metadata-only":true, "bt-resume-timeout":15,
                "bt-tracker":"https://tracker.example.test/announce"
            }]),
            None,
        )
        .unwrap()
    else {
        panic!("owned option continuation");
    };
    assert!(matches!(
        plane.begin_call_admitted("aria2.unpause", json!([gid.to_string()]), None),
        Err(HttpControlError::Busy)
    ));
    assert!(matches!(
        plane.begin_call_admitted(
            "aria2.addUri",
            json!([["https://example.test/renamed.bin"]]),
            None
        ),
        Err(HttpControlError::Busy)
    ));
    drop(reply);
    let deadline = Instant::now() + Duration::from_secs(10);
    while plane.admission_fenced() {
        plane.poll_once().unwrap();
        assert!(
            Instant::now() < deadline,
            "disconnected option owner did not settle"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(plane.bittorrent_handle().is_none());
    assert_eq!(
        plane.bt.catalog[&gid].spec.record.binding.identity,
        identity
    );
    assert_eq!(
        plane.bt.catalog[&gid].spec.record.binding.files[0].path,
        "renamed.bin"
    );
    assert_eq!(
        plane
            .call("aria2.tellStatus", json!([gid.to_string()]))
            .unwrap()["status"],
        "paused"
    );
    let options = plane
        .call("aria2.getOption", json!([gid.to_string()]))
        .unwrap();
    assert_eq!(options["bt-resume-timeout"], "15");
    plane.shutdown().unwrap();
    let mut recovered = directory.control_plane();
    assert!(recovered.bittorrent_handle().is_none());
    assert_eq!(
        recovered
            .call("aria2.getOption", json!([gid.to_string()]))
            .unwrap(),
        options
    );
    assert_eq!(
        recovered.bt.catalog[&gid].spec.record.binding.files[0].path,
        "renamed.bin"
    );
    recovered
        .call("aria2.remove", json!([gid.to_string()]))
        .unwrap();
    assert_eq!(
        recovered
            .call("aria2.tellStatus", json!([gid.to_string()]))
            .unwrap()["status"],
        "removed"
    );
    assert_eq!(
        std::fs::read(directory.output.join("occupied.bin")).unwrap(),
        b"unrelated data"
    );
    recovered.shutdown().unwrap();
}

#[test]
fn mixed_json_v3_import_validates_identity_mapping_and_atomic_rejection() {
    let original = TestDirectory::new();
    let destination = TestDirectory::new();
    let mut source = original.control_plane();
    let torrent = add_torrent(
        &mut source,
        V1,
        json!({"pause":true, "enable-dht":false, "enable-peer-exchange":false}),
    );
    source
        .call(
            "aria2.addUri",
            json!([["https://example.test/transfer.bin"], {"pause":true}]),
        )
        .unwrap();
    assert_eq!(
        source
            .call("aria2.getFiles", json!([torrent.to_string()]))
            .unwrap()[0]["length"],
        "5000"
    );
    assert_eq!(
        source
            .call("aria2.getPeers", json!([torrent.to_string()]))
            .unwrap(),
        json!([])
    );
    assert!(
        source
            .call("ariax.exportSession", json!(["aria2"]))
            .is_err()
    );
    assert!(
        source
            .call(
                "aria2.addTorrent",
                json!([base64ct::Base64::encode_string(V1), [], {"pause":true}])
            )
            .is_err()
    );
    let document = source.call("ariax.exportSession", json!(["json"])).unwrap();
    assert_eq!(document["formatVersion"], 3);
    let bt_index = document["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .position(|task| task["kind"] == "bittorrent")
        .unwrap();
    assert!(
        document["tasks"][bt_index]["bittorrent"]
            .get("root_identity")
            .is_none()
    );
    source.shutdown().unwrap();
    let mut target = destination.control_plane();
    for tamper in 0..5 {
        let mut invalid = document.clone();
        match tamper {
            0 => invalid["formatVersion"] = json!(2),
            1 => {
                invalid["tasks"][bt_index]["bittorrent"]["identity"]["v1"] = json!("00".repeat(20))
            }
            2 => invalid["tasks"][bt_index]["bittorrent"]["files"][0]["path"] = json!("../outside"),
            3 => {
                invalid["tasks"][bt_index]["bittorrent"]["files"][0]["path"] = json!("changed.bin")
            }
            _ => invalid["tasks"][bt_index]["bittorrent"]["resumeData"] = json!("ZGU="),
        }
        assert!(
            target
                .call("ariax.importSession", json!([invalid]))
                .is_err()
        );
        assert!(target.tasks.is_empty());
        assert!(target.bt.catalog.is_empty());
    }
    let imported = target
        .call("ariax.importSession", json!([document]))
        .unwrap();
    assert_eq!(imported.as_array().unwrap().len(), 2);
    assert_eq!(
        target
            .engine
            .scheduler()
            .queue_snapshot(QueueClass::Paused)
            .len(),
        2
    );
    let gid = *target.bt.catalog.keys().next().unwrap();
    assert_eq!(
        target.bt.catalog[&gid].spec.root.path(),
        std::fs::canonicalize(&destination.output).unwrap()
    );
    target.shutdown().unwrap();
    let recovered = destination.control_plane();
    assert!(recovered.bt.catalog.contains_key(&gid));
    assert_eq!(recovered.tasks.len(), 1);
    recovered.shutdown().unwrap();
}

#[test]
fn metadata_only_finishes_all_torrent_versions_without_payload_files_or_transfer_journals() {
    for bytes in [V1, V2, HYBRID] {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let subscription = plane.call("ariax.subscribe", json!([32, 65536])).unwrap();
        let gid = add_torrent(
            &mut plane,
            bytes,
            json!({"bt-metadata-only":true, "seed-ratio":"0", "enable-dht":false, "enable-peer-exchange":false}),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            plane.poll_once().unwrap();
            let status = plane
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .unwrap();
            assert_ne!(status["status"], "error", "{status}");
            if status["status"] == "complete" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "metadata-only task did not complete: {status}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(std::fs::read_dir(&directory.output).unwrap().count(), 0);
        assert!(plane.tasks.is_empty());
        let SessionCommandResult::Tasks(transfers) =
            plane.session.execute(SessionCommand::ReadTasks).unwrap()
        else {
            panic!("transfer task list")
        };
        assert!(transfers.is_empty());
        let events = plane
            .call(
                "ariax.pollEvents",
                json!([subscription["subscriptionId"], 32]),
            )
            .unwrap();
        let events = events.as_array().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event["method"] == "aria2.onBtDownloadComplete")
                .count(),
            1
        );
        let seeding: Vec<_> = events
            .iter()
            .filter(|event| event["method"] == "ariax.onSeeding")
            .map(|event| event["params"]["seeding"].as_bool().unwrap())
            .collect();
        assert_eq!(seeding, [true, false]);
        plane.shutdown().unwrap();
    }
}
