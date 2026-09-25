use super::tests::TestDirectory;
use super::*;
use base64ct::Encoding as _;

const V1: &[u8] = include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent");
const V2: &[u8] = include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v2.torrent");
const HYBRID: &[u8] =
    include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/hybrid.torrent");

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
    ] {
        assert!(bittorrent::Options::parse(&options).is_err());
    }
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
