//! Real native peers exercise the shared engine and durable lifecycle owners.

use super::tests::TestDirectory;
use super::*;
use ariax_bt::{
    BtAdapter, BtAdapterConfig, BtAdmission, BtCommand, BtHandle, BtReply, BtTaskSettings,
    MappingOptions, MetadataLimits, ProtectedRoot, map_files, parse_torrent,
};
use ariax_storage::{SessionBtResumeRecord, SessionStore, SessionStoreConfig};
use base64ct::Encoding as _;

const V1: &[u8] = include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent");
const TORRENTS: [&[u8]; 5] = [
    V1,
    include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/v2.torrent"),
    include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/hybrid.torrent"),
    include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/multi-piece-v2.torrent"),
    include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/multi-piece-hybrid.torrent"),
];
const SELECTION: &[u8] =
    include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/selection-v1.torrent");

fn config() -> BtAdapterConfig {
    BtAdapterConfig {
        peers: 32,
        files: 8,
        max_torrents: 4,
        allow_private: true,
        dht: false,
        pex: false,
        ..BtAdapterConfig::default()
    }
}

fn resources() -> crate::HttpProcessResources {
    crate::HttpProcessResources::for_profile(ariax_runtime::RuntimeProfile::Concurrency).unwrap()
}

fn plane(directory: &TestDirectory) -> HttpControlPlane {
    let mut plane = directory.control_plane();
    plane.attach_process_resources(resources()).unwrap();
    plane.configure_bittorrent(config()).unwrap();
    plane
}

fn native(handle: &BtHandle, command: BtCommand) -> BtReply {
    handle
        .submit(command)
        .unwrap()
        .wait(Duration::from_secs(10))
        .unwrap()
}

fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !predicate() {
        assert!(Instant::now() < deadline, "native engine fixture deadline");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn progress(
    plane: &mut HttpControlPlane,
    mut predicate: impl FnMut(&mut HttpControlPlane) -> bool,
) {
    until(|| {
        plane.poll_once().unwrap();
        predicate(plane)
    });
}

fn begin_when_ready(plane: &mut HttpControlPlane, method: &str, params: Value) -> ControlReply {
    let mut accepted = None;
    progress(plane, |plane| {
        match plane.begin_call_admitted(method, params.clone(), None) {
            Ok(reply) => {
                accepted = Some(reply);
                true
            }
            Err(HttpControlError::Busy) => false,
            Err(error) => panic!("native fixture command {method} failed: {error:?}"),
        }
    });
    accepted.unwrap()
}

fn status(plane: &mut HttpControlPlane, gid: Gid) -> Value {
    plane
        .call("aria2.tellStatus", json!([gid.to_string()]))
        .unwrap()
}

fn payload(length: usize) -> Vec<u8> {
    b"ariax BitTorrent fixture\n"
        .iter()
        .copied()
        .cycle()
        .take(length)
        .collect()
}

struct Seed {
    adapter: BtAdapter,
    directory: TestDirectory,
}
impl Seed {
    fn new(torrent: &[u8], contents: &[Vec<u8>]) -> Self {
        let directory = TestDirectory::new();
        let metadata = parse_torrent(torrent, MetadataLimits::default()).unwrap();
        let mapping = map_files(&metadata, &MappingOptions::default()).unwrap();
        for (file, contents) in mapping.iter().filter(|file| !file.padding).zip(contents) {
            assert_eq!(file.length as usize, contents.len());
            let path = directory.output.join(&file.path);
            let mut parent = directory.output.clone();
            for component in Path::new(&file.path).parent().unwrap().components() {
                parent.push(component);
                if !parent.exists() {
                    super::tests::create_private_directory(&parent);
                }
            }
            #[cfg(unix)]
            let mut output = {
                use std::os::unix::fs::OpenOptionsExt as _;
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&path)
                    .unwrap()
            };
            #[cfg(windows)]
            let mut output = ariax_windows_security::create_private_file(&path).unwrap();
            std::io::Write::write_all(&mut output, contents).unwrap();
        }
        let adapter = BtAdapter::start(config(), resources().bt_resources()).unwrap();
        let handle = adapter.handle();
        native(
            &handle,
            BtCommand::Add(Box::new(BtAdmission {
                gid: 1,
                root: ProtectedRoot::open(&directory.output).unwrap(),
                torrent: Some(handle.blob(torrent.to_vec()).unwrap()),
                magnet: None,
                resume: None,
                mapping: MappingOptions::default(),
                settings: BtTaskSettings {
                    peers: 16,
                    dht: false,
                    pex: false,
                    ..BtTaskSettings::default()
                },
                expected_identity: None,
                expected_mapping: None,
                allow_existing: true,
            })),
        );
        native(&handle, BtCommand::Approve { gid: 1, mapping });
        native(&handle, BtCommand::Resume { gid: 1 });
        until(|| {
            handle.listen_port() != 0 && handle.snapshot(1).is_some_and(|state| state.seeding)
        });
        Self { adapter, directory }
    }

    fn connect(&self, plane: &mut HttpControlPlane, gid: Gid) -> BtHandle {
        progress(plane, |plane| {
            plane
                .bittorrent_handle()
                .is_some_and(|handle| handle.snapshot(gid.get()).is_some())
        });
        let handle = plane.bittorrent_handle().unwrap();
        native(
            &handle,
            BtCommand::ConnectPeer {
                gid: gid.get(),
                address: ([127, 0, 0, 1], self.adapter.handle().listen_port()).into(),
            },
        );
        handle
    }

    fn stop(mut self) {
        self.adapter.request_stop();
        until(|| self.adapter.poll_stopped());
        assert!(self.directory.output.exists());
    }
}

fn add(plane: &mut HttpControlPlane, bytes: &[u8], magnet: bool, options: Value) -> Gid {
    let result = if magnet {
        let identity = parse_torrent(bytes, MetadataLimits::default())
            .unwrap()
            .identity;
        let topics = identity
            .v1
            .iter()
            .map(|hash| format!("xt=urn:btih:{hash}"))
            .chain(
                identity
                    .v2
                    .iter()
                    .map(|hash| format!("xt=urn:btmh:1220{hash}")),
            )
            .collect::<Vec<_>>()
            .join("&");
        plane.call(
            "aria2.addUri",
            json!([[format!("magnet:?{topics}")], options]),
        )
    } else {
        plane.call(
            "aria2.addTorrent",
            json!([base64ct::Base64::encode_string(bytes), [], options]),
        )
    };
    result.unwrap().as_str().unwrap().parse().unwrap()
}

fn checkpoint(plane: &HttpControlPlane, gid: Gid) -> SessionBtResumeRecord {
    let SessionCommandResult::BtResume(resume) = plane
        .session
        .execute(SessionCommand::ReadBtResume {
            gid,
            limit: 16 * 1024 * 1024,
        })
        .unwrap()
    else {
        panic!("BT resume record")
    };
    resume
}

#[test]
fn mixed_global_statistics_include_bt_rates_and_bound_selected_progress() {
    // Retain the native snapshot's memory permit while making its counters
    // deterministic after its producer stops.
    let seed = Seed::new(V1, &[payload(5000)]);
    let mut snapshot = seed.adapter.handle().snapshot(1).unwrap();
    seed.stop();
    let native = Arc::get_mut(&mut snapshot).expect("stopped producer released the snapshot");
    native.download_rate = 321;
    native.upload_rate = 654;
    native.done_bytes = u64::MAX;

    let directory = TestDirectory::new();
    let mut control = plane(&directory);
    let gid = add(&mut control, V1, false, json!({"pause":true}));
    native.gid = gid.get();
    let http = control
        .call(
            "aria2.addUri",
            json!([["https://example.test/http.bin"], {"pause":true}]),
        )
        .unwrap();
    let previous = control.bt.catalog[&gid].clone();
    Arc::make_mut(&mut control.bt.catalog).insert(
        gid,
        Arc::new(bittorrent::QueryTask {
            resume_data: previous.resume_data.clone(),
            spec: previous.spec.clone(),
            snapshot: Some(snapshot),
            peers: previous.peers.clone(),
            dirty: previous.dirty,
            checkpoint_failed: previous.checkpoint_failed,
            downloaded: previous.downloaded,
            uploaded: previous.uploaded,
            seed_millis: previous.seed_millis,
        }),
    );
    let query = control.capture_query();
    let bt = query.tell_status(json!([gid.to_string()])).unwrap();
    let http = query.tell_status(json!([http])).unwrap();
    let global = query.global_stat(json!([])).unwrap();
    assert_eq!(global["downloadSpeed"], "321");
    assert_eq!(global["uploadSpeed"], "654");
    assert_eq!(global["completedLength"], "5000");
    assert_eq!(global["numWaiting"], "2");
    assert_eq!(global["downloadSpeed"], bt["downloadSpeed"]);
    assert_eq!(global["uploadSpeed"], bt["uploadSpeed"]);
    assert_eq!(global["completedLength"], bt["completedLength"]);
    assert_eq!(http["downloadSpeed"], "0");
    assert!(query.global_stat(json!([true])).is_err());

    Arc::make_mut(&mut control.bt.catalog).insert(gid, previous);
    assert_eq!(
        control.capture_query().global_stat(json!([])).unwrap()["uploadSpeed"],
        "0"
    );
    assert_eq!(query.global_stat(json!([])).unwrap(), global);
    drop(query);
    assert!(control.shutdown().unwrap().is_clean());
}

#[test]
fn torrent_and_magnet_versions_transfer_checkpoint_recheck_and_remove_through_engine() {
    for (index, torrent) in TORRENTS.into_iter().enumerate() {
        for magnet in [false, true] {
            let contents = payload(if index < 3 { 5000 } else { 70_000 });
            let seed = Seed::new(torrent, std::slice::from_ref(&contents));
            let directory = TestDirectory::new();
            let assert_payload = |stage: &str| {
                let actual = std::fs::read(directory.output.join("renamed.bin")).unwrap();
                assert_eq!(
                    actual.len(),
                    contents.len(),
                    "fixture {index}, magnet={magnet}, {stage}: payload length"
                );
                assert!(
                    actual == contents,
                    "fixture {index}, magnet={magnet}, {stage}: first differing byte {:?}",
                    actual.iter().zip(&contents).position(|(a, b)| a != b)
                );
            };
            let mut control = plane(&directory);
            let gid = add(
                &mut control,
                torrent,
                magnet,
                json!({
                    "enable-dht":false, "enable-peer-exchange":false, "bt-max-peers":16,
                    "seed-ratio":100, "index-out":"1=renamed.bin"
                }),
            );
            let handle = seed.connect(&mut control, gid);
            progress(&mut control, |_| {
                handle
                    .snapshot(gid.get())
                    .is_some_and(|state| state.metadata && !state.held && !state.paused)
            });
            seed.connect(&mut control, gid); // metadata approval may have paused the original peer
            progress(&mut control, |plane| {
                let status = status(plane, gid);
                assert_ne!(status["status"], "error", "{status}");
                status["status"] == "active" && status["seeder"] == "true"
            });
            let binding = control.bt.catalog[&gid].spec.record.binding.clone();
            let generation = control.bt.catalog[&gid].spec.record.generation;
            assert!(
                control
                    .call(
                        "aria2.changeOption",
                        json!([gid.to_string(), {"out":"other.bin"}])
                    )
                    .is_err()
            );
            // The accepted live patch owns completion even after the RPC caller disappears.
            drop(begin_when_ready(
                &mut control,
                "aria2.changeOption",
                json!([gid.to_string(), {"max-upload-limit":4096}]),
            ));
            progress(&mut control, |plane| {
                plane
                    .call("aria2.getOption", json!([gid.to_string()]))
                    .unwrap()["max-upload-limit"]
                    == "4096"
            });
            assert_eq!(control.bt.catalog[&gid].spec.record.generation, generation);
            drop(begin_when_ready(
                &mut control,
                "aria2.pause",
                json!([gid.to_string()]),
            ));
            progress(&mut control, |plane| {
                let state = status(plane, gid);
                state["status"] == "paused"
                    && state["btCheckpointDirty"] == false
                    && handle.snapshot(gid.get()).is_none()
            });
            let safe = checkpoint(&control, gid);
            assert!(!safe.dirty && !safe.resume_blob.is_empty() && safe.request > 0);
            // Seeding can precede disk completion; the clean pause checkpoint
            // drains native writes before an independent filesystem read.
            assert_payload("paused checkpoint");
            assert_eq!(control.bt.catalog[&gid].spec.record.binding, binding);
            assert!(control.shutdown().unwrap().is_clean());

            // Simulate dirty recovery with the last safe checkpoint and changed
            // bytes. Resume data must not make unverified payload look complete.
            let mut store = SessionStore::open(
                directory.root.join("session.db"),
                SessionStoreConfig::default(),
            )
            .unwrap();
            let task = store.bt_tasks().unwrap().remove(0);
            store.mark_bt_dirty(gid, task.generation).unwrap();
            drop(store);
            let mut corrupted = contents.clone();
            corrupted[0] ^= 1;
            std::fs::write(directory.output.join("renamed.bin"), corrupted).unwrap();
            let mut recovered = plane(&directory);
            assert_eq!(recovered.bt.catalog[&gid].spec.record.binding, binding);
            assert!(checkpoint(&recovered, gid).dirty);
            assert_eq!(
                recovered
                    .call("aria2.getOption", json!([gid.to_string()]))
                    .unwrap()["max-upload-limit"],
                "4096"
            );
            recovered
                .call("aria2.unpause", json!([gid.to_string()]))
                .unwrap();
            progress(&mut recovered, |plane| {
                plane.bittorrent_handle().is_some_and(|handle| {
                    handle.snapshot(gid.get()).is_some_and(|state| {
                        state.metadata
                            && !state.held
                            && !state.paused
                            && !state.checking
                            && state.done_bytes < contents.len() as u64
                    })
                })
            });
            let handle = seed.connect(&mut recovered, gid);
            progress(&mut recovered, |plane| {
                status(plane, gid)["seeder"] == "true"
            });
            recovered
                .call("aria2.remove", json!([gid.to_string()]))
                .unwrap();
            progress(&mut recovered, |plane| {
                status(plane, gid)["status"] == "removed" && handle.snapshot(gid.get()).is_none()
            });
            assert!(!checkpoint(&recovered, gid).dirty);
            assert_payload("removed checkpoint after dirty recovery");
            assert!(directory.output.join("renamed.bin").exists());
            assert!(recovered.shutdown().unwrap().is_clean());
            seed.stop();
        }
    }
}

#[test]
fn selected_collision_mapping_downloads_only_its_disjoint_piece_and_survives_restart() {
    let seed = Seed::new(SELECTION, &[vec![b'a'; 16384], vec![b'b'; 16384]]);
    let directory = TestDirectory::new();
    let mut control = plane(&directory);
    let gid = add(
        &mut control,
        SELECTION,
        false,
        json!({
            "select-file":"2", "seed-ratio":0, "bt-max-peers":16,
            "enable-dht":false, "enable-peer-exchange":false
        }),
    );
    let mapping = control.bt.catalog[&gid].spec.record.binding.files.clone();
    assert!(!mapping[0].selected && mapping[1].selected);
    assert_ne!(mapping[0].path, mapping[1].path);
    progress(&mut control, |plane| {
        plane.bittorrent_handle().is_some_and(|handle| {
            handle
                .snapshot(gid.get())
                .is_some_and(|state| !state.held && !state.paused && !state.checking)
        })
    });
    seed.connect(&mut control, gid);
    progress(&mut control, |plane| {
        let state = status(plane, gid);
        assert_ne!(state["status"], "error", "{state}");
        state["status"] == "complete"
    });
    assert!(!directory.output.join(&mapping[0].path).exists());
    assert_eq!(
        std::fs::read(directory.output.join(&mapping[1].path)).unwrap(),
        vec![b'b'; 16384]
    );
    let files = control
        .call("aria2.getFiles", json!([gid.to_string()]))
        .unwrap();
    assert_eq!(files[0]["selected"], "false");
    assert_eq!(files[1]["completedLength"], "16384");
    assert!(!checkpoint(&control, gid).dirty);
    assert!(control.shutdown().unwrap().is_clean());
    let recovered = plane(&directory);
    assert_eq!(
        recovered.bt.catalog[&gid].spec.record.binding.files,
        mapping
    );
    assert!(recovered.shutdown().unwrap().is_clean());
    seed.stop();
}

#[test]
fn late_invalid_magnet_selection_fails_without_creating_payload_files() {
    let seed = Seed::new(V1, &[payload(5000)]);
    let directory = TestDirectory::new();
    let mut control = plane(&directory);
    let gid = add(
        &mut control,
        V1,
        true,
        json!({
            "select-file":"2", "bt-max-peers":16,
            "enable-dht":false, "enable-peer-exchange":false
        }),
    );
    seed.connect(&mut control, gid);
    progress(&mut control, |plane| {
        status(plane, gid)["status"] == "error"
    });
    assert_eq!(std::fs::read_dir(&directory.output).unwrap().count(), 0);
    assert!(control.tasks.is_empty());
    control.shutdown().unwrap();
    seed.stop();
}

struct MetadataPeer {
    address: std::net::SocketAddr,
    stop: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
}

impl MetadataPeer {
    fn new(info: Vec<u8>, hash: [u8; 20]) -> Self {
        use std::io::{Read as _, Write as _};
        use std::sync::atomic::Ordering;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled = stop.clone();
        let worker = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut stream = loop {
                if cancelled.load(Ordering::Acquire) {
                    return Ok(());
                }
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return Err(std::io::Error::other("metadata peer accept deadline"));
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => return Err(error),
                }
            };
            // Windows accepts inherit the listener's nonblocking mode.
            stream.set_nonblocking(false)?;
            stream.set_read_timeout(Some(Duration::from_secs(10)))?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            let mut handshake = [0; 68];
            stream.read_exact(&mut handshake)?;
            if handshake[28..48] != hash || &handshake[..20] != b"\x13BitTorrent protocol" {
                return Err(std::io::Error::other("metadata peer identity mismatch"));
            }
            handshake[20..28].fill(0);
            handshake[25] = 0x10; // BEP 10 extension protocol
            handshake[48..].fill(b'm');
            stream.write_all(&handshake)?;
            let send = |stream: &mut std::net::TcpStream,
                        extension: u8,
                        bytes: &[u8]|
             -> std::io::Result<()> {
                stream.write_all(&((bytes.len() + 2) as u32).to_be_bytes())?;
                stream.write_all(&[20, extension])?;
                stream.write_all(bytes)
            };
            send(
                &mut stream,
                0,
                format!("d1:md11:ut_metadatai1ee13:metadata_sizei{}ee", info.len()).as_bytes(),
            )?;
            let mut remote_extension = None;
            for _ in 0..64 {
                let mut length = [0; 4];
                stream.read_exact(&mut length)?;
                let length = u32::from_be_bytes(length) as usize;
                if length > 32768 {
                    return Err(std::io::Error::other("metadata peer frame limit"));
                }
                let mut frame = vec![0; length];
                stream.read_exact(&mut frame)?;
                if frame.first() != Some(&20) || frame.len() < 2 {
                    continue;
                }
                if frame[1] == 0 {
                    let key = b"11:ut_metadatai";
                    let start = frame
                        .windows(key.len())
                        .position(|bytes| bytes == key)
                        .ok_or_else(|| std::io::Error::other("missing metadata extension"))?
                        + key.len();
                    let end = frame[start..]
                        .iter()
                        .position(|byte| *byte == b'e')
                        .ok_or_else(|| std::io::Error::other("invalid metadata extension"))?
                        + start;
                    remote_extension = std::str::from_utf8(&frame[start..end])
                        .ok()
                        .and_then(|text| text.parse::<u8>().ok())
                        .filter(|id| *id != 0);
                } else if frame[1] == 1 {
                    let kind = b"8:msg_typei0e";
                    let piece = b"5:piecei0e";
                    if !frame.windows(kind.len()).any(|bytes| bytes == kind)
                        || !frame.windows(piece.len()).any(|bytes| bytes == piece)
                    {
                        return Err(std::io::Error::other("unexpected metadata request"));
                    }
                    let extension = remote_extension.ok_or_else(|| {
                        std::io::Error::other("metadata requested before negotiation")
                    })?;
                    let mut response =
                        format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len())
                            .into_bytes();
                    response.extend_from_slice(&info);
                    send(&mut stream, extension, &response)?;
                    return Ok(());
                }
            }
            Err(std::io::Error::other("metadata request limit"))
        });
        Self {
            address,
            stop,
            worker: Some(worker),
        }
    }

    fn finish(mut self) {
        self.worker.take().unwrap().join().unwrap().unwrap();
    }
}

impl Drop for MetadataPeer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[test]
fn late_traversal_and_symlink_metadata_are_rejected_before_payload_creation() {
    use sha1::{Digest as _, Sha1};
    for torrent in [
        include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/unsafe-path-v1.torrent")
            .as_slice(),
        include_bytes!("../../../ariax-bt-libtorrent-sys/tests/fixtures/symlink-v1.torrent")
            .as_slice(),
    ] {
        assert!(parse_torrent(torrent, MetadataLimits::default()).is_err());
        let info = ariax_bt::info_section(torrent, MetadataLimits::default()).unwrap();
        assert!(info.len() < 16384);
        let hash = Sha1::digest(info);
        let topic = hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let magnet = format!("magnet:?xt=urn:btih:{topic}");
        let peer = MetadataPeer::new(info.to_vec(), hash.into());
        let directory = TestDirectory::new();
        let mut control = plane(&directory);
        // This bounded BEP 10 peer implements the plaintext wire handshake.
        control
            .configure_bittorrent(BtAdapterConfig {
                encryption: 2,
                ..config()
            })
            .unwrap();
        let gid: Gid = control
            .call(
                "aria2.addUri",
                json!([[magnet], {
                    "enable-dht":false, "enable-peer-exchange":false, "bt-max-peers":16
                }]),
            )
            .unwrap()
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        progress(&mut control, |plane| {
            plane
                .bittorrent_handle()
                .is_some_and(|handle| handle.snapshot(gid.get()).is_some())
        });
        native(
            &control.bittorrent_handle().unwrap(),
            BtCommand::ConnectPeer {
                gid: gid.get(),
                address: peer.address,
            },
        );
        peer.finish();
        let deadline = Instant::now() + Duration::from_secs(30);
        progress(&mut control, |plane| {
            let current = status(plane, gid);
            assert!(
                Instant::now() < deadline,
                "unsafe metadata rejection deadline: {current}; native {:?}",
                plane
                    .bittorrent_handle()
                    .and_then(|handle| handle.snapshot(gid.get()))
            );
            current["status"] == "error"
        });
        assert_eq!(std::fs::read_dir(&directory.output).unwrap().count(), 0);
        assert!(!directory.root.join("escape").exists());
        control.shutdown().unwrap();
    }
}

#[test]
fn active_shutdown_persists_bt_resume_before_closing_the_global_session() {
    let directory = TestDirectory::new();
    let mut control = plane(&directory);
    let gid = add(
        &mut control,
        V1,
        false,
        json!({
            "enable-dht":false, "enable-peer-exchange":false, "bt-max-peers":16
        }),
    );
    progress(&mut control, |plane| {
        plane.bittorrent_handle().is_some_and(|handle| {
            handle
                .snapshot(gid.get())
                .is_some_and(|state| !state.held && !state.paused && !state.checking)
        })
    });
    assert!(control.shutdown().unwrap().is_clean());
    let store = SessionStore::open(
        directory.root.join("session.db"),
        SessionStoreConfig::default(),
    )
    .unwrap();
    let resume = store.bt_resume(gid, 16 * 1024 * 1024).unwrap();
    assert!(!resume.dirty && resume.request > 0 && !resume.resume_blob.is_empty());
}

#[test]
fn expired_shutdown_retains_safe_resume_and_dirty_generation_on_recovery() {
    let directory = TestDirectory::new();
    let mut control = plane(&directory);
    let gid = add(
        &mut control,
        V1,
        false,
        json!({
            "enable-dht":false, "enable-peer-exchange":false, "bt-max-peers":16
        }),
    );
    let downloading = |plane: &mut HttpControlPlane| {
        plane.bittorrent_handle().is_some_and(|handle| {
            handle.snapshot(gid.get()).is_some_and(|state| {
                state.metadata && !state.held && !state.paused && !state.checking
            })
        })
    };
    progress(&mut control, downloading);
    control
        .call("aria2.pause", json!([gid.to_string()]))
        .unwrap();
    progress(&mut control, |plane| {
        status(plane, gid)["status"] == "paused"
            && plane.engine_idle()
            && plane
                .engine
                .scheduler()
                .task(gid)
                .is_some_and(|task| task.pending_barrier.is_none())
            && plane
                .bittorrent_handle()
                .is_some_and(|handle| handle.snapshot(gid.get()).is_none())
    });
    let safe = checkpoint(&control, gid);
    assert!(!safe.dirty && !safe.resume_blob.is_empty());
    let binding = control.bt.catalog[&gid].spec.record.binding.clone();
    control
        .call("aria2.unpause", json!([gid.to_string()]))
        .unwrap();
    progress(&mut control, downloading);
    let running = checkpoint(&control, gid);
    assert!(running.dirty && running.generation > safe.generation);
    assert_eq!(running.resume_blob, safe.resume_blob);
    assert_eq!(running.saved_ms, safe.saved_ms);

    // Expire the existing supervisor deadline deterministically, after native
    // I/O starts and before shutdown can submit a replacement checkpoint.
    control.config.supervisor.shutdown_timeout = Duration::ZERO;
    let report = control.shutdown().unwrap();
    assert!(!report.is_clean());
    let recovered = plane(&directory);
    assert_eq!(checkpoint(&recovered, gid), running);
    assert_eq!(recovered.bt.catalog[&gid].spec.record.binding, binding);
    recovered.shutdown().unwrap();
}
