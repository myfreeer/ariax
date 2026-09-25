use super::tests::TestDirectory;
use super::*;

#[cfg(feature = "metalink")]
fn upload(names: &[&str]) -> Value {
    use base64ct::Encoding;
    let files = names
        .iter()
        .map(|name| {
            format!(
                "<file name=\"{name}\"><size>0</size><url>http://example.test/{name}</url></file>"
            )
        })
        .collect::<String>();
    json!([base64ct::Base64::encode_string(format!("<metalink xmlns=\"urn:ietf:params:xml:ns:metalink\">{files}</metalink>").as_bytes()), {"pause":true}, 0])
}

#[cfg(feature = "metalink")]
#[test]
fn phase5_metalink_admission_is_atomic_ordered_and_self_contained() {
    let directory = TestDirectory::new();
    let mut plane = directory.control_plane();
    let first = super::tests::add_paused(&mut plane);
    let gids = plane
        .call("aria2.addMetalink", upload(&["one", "two"]))
        .unwrap();
    let one: Gid = gids[0].as_str().unwrap().parse().unwrap();
    let two: Gid = gids[1].as_str().unwrap().parse().unwrap();
    assert_eq!(
        plane.engine.scheduler().queue_snapshot(QueueClass::Paused),
        [one, two, first]
    );
    let before = plane.tasks.len();
    for names in [&["three", "ONE"][..], &["safe", "../bad"][..]] {
        assert!(plane.call("aria2.addMetalink", upload(names)).is_err());
        assert_eq!(plane.tasks.len(), before);
    }
    std::fs::write(plane.config.output_root.join("exists"), b"unchanged").unwrap();
    assert!(
        plane
            .call("aria2.addMetalink", upload(&["four", "exists"]))
            .is_err()
    );
    assert_eq!(plane.tasks.len(), before);
    let document = plane.call("ariax.exportSession", json!(["json"])).unwrap();
    assert_eq!(document["formatVersion"], 3);
    assert!(document.to_string().contains("verification"));
    assert!(plane.call("ariax.exportSession", json!(["aria2"])).is_err());
    assert!(plane.shutdown().unwrap().is_clean());
    let mut recovered = directory.control_plane();
    assert_eq!(
        recovered
            .engine
            .scheduler()
            .queue_snapshot(QueueClass::Paused),
        [one, two, first]
    );
    assert_eq!(
        recovered
            .tasks
            .get_gid(one)
            .unwrap()
            .verification()
            .unwrap()
            .total_length(),
        0
    );
    assert!(
        recovered
            .call(
                "aria2.changeUri",
                json!([
                    one.to_string(),
                    1,
                    ["http://example.test/one"],
                    ["https://example.test/replacement"]
                ])
            )
            .is_ok()
    );
    assert!(
        recovered
            .tasks
            .get_gid(one)
            .unwrap()
            .verification()
            .is_some()
    );
    recovered.shutdown().unwrap();
}

#[cfg(feature = "metalink")]
#[test]
fn phase5_json_v3_imports_selected_verification_into_a_new_root_atomically() {
    use base64ct::Encoding;
    let original = TestDirectory::new();
    let destination = TestDirectory::new();
    let mut source = original.control_plane();
    let mut sha = crate::ContentHasher::new(ariax_storage::JournalDigestAlgorithm::Sha512);
    sha.update(b"abc");
    let sha = sha.finalize().canonical();
    let digest = sha.split_once('=').unwrap().1;
    let xml = format!(
        "<metalink xmlns='urn:ietf:params:xml:ns:metalink'><file name='skip'><size>0</size><url>https://example.test/skip</url></file><file name='selected'><size>3</size><pieces type='sha-512' length='3'><hash>{digest}</hash></pieces><hash type='md5'>900150983cd24fb0d6963f7d28e17f72</hash><url priority='2'>https://example.test/two</url><url priority='1'>https://mirror.test/two</url></file></metalink>"
    );
    let gids = source.call("aria2.addMetalink", json!([base64ct::Base64::encode_string(xml.as_bytes()), {"select-file":"2", "pause":true}])).unwrap();
    let original_gid: Gid = gids[0].as_str().unwrap().parse().unwrap();
    let manifest = source
        .tasks
        .get_gid(original_gid)
        .unwrap()
        .verification()
        .unwrap()
        .clone();
    let document = source.call("ariax.exportSession", json!(["json"])).unwrap();
    assert_eq!(document["formatVersion"], 3);
    assert_eq!(document["tasks"].as_array().unwrap().len(), 1);
    source.shutdown().unwrap();
    let mut target = destination.control_plane();
    for tamper in 0..4 {
        let mut bad = document.clone();
        match tamper {
            0 => bad["tasks"][0]["verification"]["length"] = json!("4"),
            1 => bad["tasks"][0]["verification"]["chunks"][0] = json!("sha-512=00"),
            2 => bad["tasks"][0]["options"]["metadata-expansion"] = json!("forged"),
            _ => bad["formatVersion"] = json!(1),
        }
        assert!(target.call("ariax.importSession", json!([bad])).is_err());
        assert_eq!(target.tasks.len(), 0);
    }
    let imported = target
        .call("ariax.importSession", json!([document]))
        .unwrap();
    let gid: Gid = imported[0].as_str().unwrap().parse().unwrap();
    let spec = target.tasks.get_gid(gid).unwrap();
    assert_eq!(spec.output_root(), &destination.output);
    assert_eq!(spec.metalink_index(), Some(2));
    assert_eq!(spec.verification().unwrap().as_ref(), manifest.as_ref());
    assert!(
        target
            .engine
            .scheduler()
            .queue_snapshot(QueueClass::Paused)
            .contains(&gid)
    );
    // Older development documents cannot modify the current session.
    let v1 = json!({"formatVersion":1,"tasks":[{"uris":["https://example.test/legacy"],"options":{"out":"legacy","pause":false}}]});
    assert!(target.call("ariax.importSession", json!([v1])).is_err());
    assert_eq!(
        target
            .engine
            .scheduler()
            .queue_snapshot(QueueClass::Paused)
            .len(),
        1
    );
    target.shutdown().unwrap();
    let recovered = destination.control_plane();
    assert_eq!(
        recovered
            .tasks
            .get_gid(gid)
            .unwrap()
            .verification()
            .unwrap()
            .as_ref(),
        manifest.as_ref()
    );
    recovered.shutdown().unwrap();
}

#[test]
fn phase5_metadata_pressure_rejects_mutations_before_persistence() {
    let directory = TestDirectory::new();
    let mut plane = directory.control_plane();
    let gid = super::tests::add_paused(&mut plane);
    let before = plane
        .tasks
        .get_gid(gid)
        .unwrap()
        .persistence_options()
        .unwrap();
    let budget = crate::HttpIngressBudgets::new(plane.tasks.snapshot().retained_bytes());
    plane.tasks.set_metadata_budget(budget.clone()).unwrap();
    assert!(
        plane
            .call(
                "aria2.changeOption",
                json!([gid.to_string(), {"split":"2"}])
            )
            .is_err()
    );
    assert!(
        plane
            .call(
                "aria2.changeUri",
                json!([gid.to_string(), 1, [], ["https://mirror.test/other"]])
            )
            .is_err()
    );
    assert_eq!(
        plane
            .tasks
            .get_gid(gid)
            .unwrap()
            .persistence_options()
            .unwrap(),
        before
    );
    assert_eq!(budget.used(), budget.limit());
    plane.shutdown().unwrap();
    let recovered = directory.control_plane();
    assert_eq!(
        recovered
            .tasks
            .get_gid(gid)
            .unwrap()
            .persistence_options()
            .unwrap(),
        before
    );
    assert_eq!(recovered.tasks.get_gid(gid).unwrap().sources().len(), 1);
    recovered.shutdown().unwrap();
}

#[cfg(feature = "sftp")]
#[test]
fn phase5_remote_authentication_does_not_grant_local_file_or_bypass_authority() {
    let directory = TestDirectory::new();
    let mut plane = directory.control_plane();
    let uris = vec!["sftp://example.test/file".to_owned()];
    for options in [
        json!({"sftp-check-host-key":false}),
        json!({"sftp-private-key":"/private/key"}),
        json!({"sftp-use-agent":true}),
        json!({"ftp-pasv-address":"server"}),
    ] {
        assert!(parse_add_options(&options, &plane.config.output_root, &uris).is_err());
        assert!(
            parse_add_options_authorized(&options, &plane.config.output_root, &uris, true).is_ok()
        );
    }
    let request = plane.direct_client.try_request(0).unwrap();
    let reply=plane.begin_call_authorized("aria2.addUri",json!([uris,{"pause":true,"ftp-user":"alice","ftp-passwd":"secret-canary","sftp-check-host-key":false}]),Some(request),true).unwrap();
    let gid = match reply {
        ControlReply::Deferred(reply) => plane.wait_for_mutation(reply).unwrap(),
        _ => panic!("deferred"),
    };
    let spec = plane
        .tasks
        .get_gid(gid.as_str().unwrap().parse().unwrap())
        .unwrap();
    assert!(!spec.options().transfer.sftp_check_host_key);
    assert!(
        spec.persistence_sources()
            .iter()
            .all(|source| source.needs_credentials)
    );
    let persisted = format!("{:?}", spec.persistence_options().unwrap());
    assert!(!persisted.contains("secret-canary"));
    assert!(!persisted.contains("ftp-user"));
    assert!(!persisted.contains("sftp-check-host-key"));
    assert!(!format!("{:?}", spec.options()).contains("secret-canary"));
    assert!(
        parse_registry_options(&json!({"sftp-check-host-key":false}), Scope::RpcChange).is_err()
    );
    plane.shutdown().unwrap();
}

struct ChallengeWorker(ariax_core::PresentedHostKeyChallenge);
impl HttpTaskWorker for ChallengeWorker {
    fn start(
        &self,
        _task: Arc<HttpTaskSpec>,
        _generation: Generation,
        _cancel: crate::HttpCancellation,
    ) -> crate::HttpWorkerFuture {
        let challenge = self.0.clone();
        Box::pin(async move {
            Ok(crate::HttpWorkerSuccess {
                host_key_challenge: Some(challenge),
                ..Default::default()
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase5_drained_host_challenge_persists_and_only_exact_approval_clears_it() {
    let directory = TestDirectory::new();
    let mut plane = directory.control_plane();
    let key = b"test presented key".to_vec();
    let fingerprint = ariax_core::HostKeyFingerprint::for_presented_key(&key);
    let challenge = ariax_core::PresentedHostKeyChallenge::new(
        ariax_core::HostKeyChallenge {
            id: ariax_core::HostKeyChallengeId::new([7; 16]),
            canonical_host: "example.test".into(),
            port: 22,
            algorithm: "ssh-ed25519".into(),
            fingerprint_sha256: fingerprint,
        },
        key,
    )
    .unwrap();
    plane
        .attach_worker(Arc::new(ChallengeWorker(challenge)))
        .unwrap();
    let gid = plane
        .call("aria2.addUri", json!([["http://example.test/file"]]))
        .unwrap();
    let gid: Gid = gid.as_str().unwrap().parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        plane.poll_once().unwrap();
        if plane
            .engine
            .snapshot_reader()
            .load()
            .task(gid)
            .unwrap()
            .snapshot
            .host_key_challenge
            .is_some()
        {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let status = plane
        .call("aria2.tellStatus", json!([gid.to_string()]))
        .unwrap();
    assert_eq!(status["status"], "paused");
    assert_eq!(status["hostKeyChallenge"]["host"], "example.test");
    assert!(
        plane
            .call("aria2.unpause", json!([gid.to_string()]))
            .is_err()
    );
    assert!(
        plane
            .call(
                "ariax.approveHostKey",
                json!([gid.to_string(), "00".repeat(16), "00".repeat(32)])
            )
            .is_err()
    );
    assert!(plane.shutdown().unwrap().is_clean());
    let mut plane = directory.control_plane();
    assert!(
        plane
            .engine
            .snapshot_reader()
            .load()
            .task(gid)
            .unwrap()
            .snapshot
            .host_key_challenge
            .is_some()
    );
    plane
        .call(
            "ariax.approveHostKey",
            json!([
                gid.to_string(),
                "07".repeat(16),
                ariax_storage::session_host_key_pin_value(fingerprint)
            ]),
        )
        .unwrap();
    assert!(
        plane
            .engine
            .snapshot_reader()
            .load()
            .task(gid)
            .unwrap()
            .snapshot
            .host_key_challenge
            .is_none()
    );
    assert_eq!(
        plane
            .tasks
            .get_gid(gid)
            .unwrap()
            .options()
            .transfer
            .sftp_host_key_sha256,
        Some(ariax_storage::session_host_key_pin_value(fingerprint))
    );
    plane.shutdown().unwrap();
    let plane = directory.control_plane();
    assert_eq!(
        plane
            .tasks
            .get_gid(gid)
            .unwrap()
            .options()
            .transfer
            .sftp_host_key_sha256,
        Some(ariax_storage::session_host_key_pin_value(fingerprint))
    );
    plane.shutdown().unwrap();
}

fn trust_state(decision: ariax_storage::HostKeyDecision) -> ariax_storage::JournalHostKeyState {
    let key = b"crash-bound-presented-key".to_vec();
    ariax_storage::JournalHostKeyState {
        challenge: ariax_core::PresentedHostKeyChallenge::new(
            ariax_core::HostKeyChallenge {
                id: ariax_core::HostKeyChallengeId::new([11; 16]),
                canonical_host: "example.test".into(),
                port: 22,
                algorithm: "ssh-ed25519".into(),
                fingerprint_sha256: ariax_core::HostKeyFingerprint::for_presented_key(&key),
            },
            key,
        )
        .unwrap(),
        decision,
        created_ms: 1000,
    }
}
fn flush_trust(plane: &HttpControlPlane, gid: Gid, state: ariax_storage::JournalHostKeyState) {
    let generation = plane.engine.scheduler().task(gid).unwrap().generation;
    let SessionCommandResult::JournalAppended(appended) = plane
        .session
        .execute(SessionCommand::AppendJournal {
            gid,
            generation,
            payload: JournalPayload::HostKeyState { state },
        })
        .unwrap()
    else {
        panic!("journal append");
    };
    plane
        .session
        .execute(SessionCommand::FlushJournal {
            gid,
            through_sequence: appended.sequence(),
        })
        .unwrap();
}
#[test]
fn phase5_interrupted_trust_transactions_recover_before_publication() {
    use ariax_storage::HostKeyDecision;
    for decision in [HostKeyDecision::Approved, HostKeyDecision::Rejected] {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid: Gid = plane
            .call("aria2.addUri", json!([["http://example.test/file"]]))
            .unwrap()
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        // Power loss after the pending journal flush, before SQLite queue/challenge writes.
        flush_trust(&plane, gid, trust_state(HostKeyDecision::Pending));
        plane.shutdown().unwrap();
        let plane = directory.control_plane();
        assert!(
            plane
                .engine
                .snapshot_reader()
                .load()
                .task(gid)
                .unwrap()
                .snapshot
                .host_key_challenge
                .is_some()
        );
        // Power loss after the decision flush, before clearing the SQLite challenge.
        flush_trust(&plane, gid, trust_state(decision));
        plane.shutdown().unwrap();
        let plane = directory.control_plane();
        assert!(
            plane
                .engine
                .snapshot_reader()
                .load()
                .task(gid)
                .unwrap()
                .snapshot
                .host_key_challenge
                .is_none()
        );
        let spec = plane.tasks.get_gid(gid).unwrap();
        assert_eq!(
            spec.options().transfer.sftp_host_key_sha256.is_some(),
            decision == HostKeyDecision::Approved
        );
        let expected = if decision == HostKeyDecision::Approved {
            QueueClass::Waiting
        } else {
            QueueClass::Paused
        };
        assert!(
            plane
                .engine
                .scheduler()
                .queue_snapshot(expected)
                .contains(&gid)
        );
        plane.shutdown().unwrap();
    }
}

#[cfg(feature = "metalink")]
struct HoldWorker;
#[cfg(feature = "metalink")]
impl HttpTaskWorker for HoldWorker {
    fn start(
        &self,
        _: Arc<HttpTaskSpec>,
        _: Generation,
        cancel: crate::HttpCancellation,
    ) -> crate::HttpWorkerFuture {
        Box::pin(async move {
            cancel.cancelled().await;
            Ok(Default::default())
        })
    }
}
#[cfg(feature = "metalink")]
fn crash_phase5(directory: &TestDirectory, mode: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "http_control::phase5_tests::phase5_crash_child",
            "--nocapture",
        ])
        .env("ARIAX_PHASE5_CRASH_ROOT", &directory.root)
        .env("ARIAX_PHASE5_CRASH_MODE", mode)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(77), "crash mode {mode}");
}

#[cfg(feature = "metalink")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase5_process_exit_recovers_trust_and_atomic_expansion_without_duplicate_children() {
    for decision in ["approved", "rejected"] {
        let directory = TestDirectory::new();
        crash_phase5(&directory, "pending");
        let recovered = directory.control_plane();
        let gid: Gid = std::fs::read_to_string(directory.root.join("parent-gid"))
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            recovered
                .engine
                .snapshot_reader()
                .load()
                .task(gid)
                .unwrap()
                .snapshot
                .host_key_challenge
                .is_some()
        );
        recovered.shutdown().unwrap();
        crash_phase5(&directory, decision);
        let recovered = directory.control_plane();
        assert!(
            recovered
                .engine
                .snapshot_reader()
                .load()
                .task(gid)
                .unwrap()
                .snapshot
                .host_key_challenge
                .is_none()
        );
        assert_eq!(
            recovered
                .tasks
                .get_gid(gid)
                .unwrap()
                .options()
                .transfer
                .sftp_host_key_sha256
                .is_some(),
            decision == "approved"
        );
        recovered.shutdown().unwrap();
    }
    for committed in [false, true] {
        let directory = TestDirectory::new();
        crash_phase5(
            &directory,
            if committed {
                "expansion-after"
            } else {
                "expansion-before"
            },
        );
        let gid: Gid = std::fs::read_to_string(directory.root.join("parent-gid"))
            .unwrap()
            .parse()
            .unwrap();
        let mut recovered = directory.control_plane();
        assert_eq!(recovered.tasks.len(), if committed { 3 } else { 1 });
        if committed {
            let expansion = recovered
                .tasks
                .get_gid(gid)
                .unwrap()
                .options()
                .transfer
                .metadata_expansion
                .clone()
                .unwrap();
            assert_eq!(expansion.children.len(), 2);
            super::tests::attach_loopback_worker(&mut recovered, &directory);
            let deadline = Instant::now() + Duration::from_secs(5);
            while recovered
                .engine
                .snapshot_reader()
                .load()
                .task(gid)
                .unwrap()
                .snapshot
                .state
                != ariax_core::TaskState::StoppedResult
            {
                recovered.poll_once().unwrap();
                assert!(Instant::now() < deadline, "metadata parent restart");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let status = recovered
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .unwrap();
            assert_eq!(status["status"], "complete", "{status}");
            assert_eq!(
                status["followedBy"],
                json!(
                    expansion
                        .children
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                )
            );
            assert_eq!(recovered.tasks.len(), 3);
        }
        recovered.shutdown().unwrap();
        let recovered = directory.control_plane();
        assert_eq!(recovered.tasks.len(), if committed { 3 } else { 1 });
        recovered.shutdown().unwrap();
    }
}

#[cfg(feature = "metalink")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase5_crash_child() {
    let Some(root) = std::env::var_os("ARIAX_PHASE5_CRASH_ROOT") else {
        return;
    };
    let directory = TestDirectory::at(root.into());
    let mode = std::env::var("ARIAX_PHASE5_CRASH_MODE").unwrap();
    let mut plane = directory.control_plane();
    if mode == "pending" || mode.starts_with("expansion-") {
        if mode.starts_with("expansion-") {
            plane.attach_worker(Arc::new(HoldWorker)).unwrap();
        }
        let gid: Gid = plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/metadata"], {"out":"metadata"}]),
            )
            .unwrap()
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        std::fs::write(directory.root.join("parent-gid"), gid.to_string()).unwrap();
        if mode == "pending" {
            flush_trust(
                &plane,
                gid,
                trust_state(ariax_storage::HostKeyDecision::Pending),
            );
        } else {
            let deadline = Instant::now() + Duration::from_secs(5);
            while plane.engine.scheduler().task(gid).unwrap().state != ariax_core::TaskState::Active
                || !plane.engine_idle()
            {
                plane.poll_once().unwrap();
                assert!(Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let parent = ariax_storage::MetadataParent {
                gid,
                generation: plane.engine.scheduler().task(gid).unwrap().generation,
                snapshot_hash: plane
                    .tasks
                    .get_gid(gid)
                    .unwrap()
                    .persistence_options()
                    .unwrap()
                    .snapshot_hash(),
                document_hash: ariax_storage::JournalHash::new([7; 32]).unwrap(),
                document_bytes: 128,
                retained: false,
            };
            let request = plane.direct_client.try_request(128).unwrap();
            let _reply = plane
                .begin_admission(
                    upload(&["one", "two"]),
                    request,
                    admission::AdmissionKind::Follow(parent),
                    false,
                )
                .unwrap();
            while plane.pending_admission.is_some() {
                plane.poll_admission().unwrap();
                assert!(Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if mode == "expansion-after" {
                while !plane.engine.is_idle() {
                    assert!(!matches!(
                        plane.engine.poll_at(MonotonicInstant::now()),
                        ariax_runtime::SchedulerDriverPoll::Faulted(_)
                    ));
                    assert!(Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            assert!(
                matches!(plane.session.execute(SessionCommand::ReadTasks).unwrap(), SessionCommandResult::Tasks(tasks) if tasks.len() == if mode == "expansion-after" { 3 } else { 1 })
            );
        }
    } else {
        let gid = std::fs::read_to_string(directory.root.join("parent-gid"))
            .unwrap()
            .parse()
            .unwrap();
        flush_trust(
            &plane,
            gid,
            trust_state(if mode == "approved" {
                ariax_storage::HostKeyDecision::Approved
            } else {
                ariax_storage::HostKeyDecision::Rejected
            }),
        );
    }
    std::process::exit(77);
}

#[cfg(feature = "metalink")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase5_automatic_follow_is_atomic_retains_or_omits_xml_and_completes_empty_children() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for mode in ["true", "mem", "false", "invalid"] {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        super::tests::attach_loopback_worker(&mut plane, &directory);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let xml = if mode == "invalid" {
            b"<bad>".to_vec()
        } else {
            b"<metalink xmlns='urn:ietf:params:xml:ns:metalink'><file name='empty-child'><size>0</size><url>child</url></file></metalink>".to_vec()
        };
        let expected = xml.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let xml = xml.clone();
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
                    if text.starts_with("GET /child ") {
                        let _=stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                        return;
                    }
                    let range = text.lines().find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("range: bytes=")
                            .map(str::to_owned)
                    });
                    let (status, extra, body) = if let Some(range) = range {
                        let (start, end) = range.split_once('-').unwrap();
                        let start = start.parse::<usize>().unwrap();
                        let end = end.parse::<usize>().unwrap();
                        (
                            "206 Partial Content",
                            format!("Content-Range: bytes {start}-{end}/{}\r\n", xml.len()),
                            xml[start..=end].to_vec(),
                        )
                    } else {
                        ("200 OK", String::new(), xml)
                    };
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/metalink4+xml\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",
                        body.len()
                    );
                    if stream.write_all(head.as_bytes()).await.is_ok() {
                        let _ = stream.write_all(&body).await;
                    }
                });
            }
        });
        let gid: Gid=plane.call("aria2.addUri",json!([[format!("http://{address}/metadata")],{"out":"metadata.meta4","follow-metalink":if mode=="invalid" {"mem"} else {mode}}])).unwrap().as_str().unwrap().parse().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            plane.poll_once().unwrap();
            let status = plane.engine.snapshot_reader().load();
            if status.task(gid).unwrap().snapshot.state == ariax_core::TaskState::StoppedResult
                && status
                    .tasks()
                    .values()
                    .all(|task| task.snapshot.state == ariax_core::TaskState::StoppedResult)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{mode}: {:?}",
                plane.call("aria2.tellStatus", json!([gid.to_string()]))
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let status = plane
            .call("aria2.tellStatus", json!([gid.to_string()]))
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
        assert_eq!(plane.tasks.len(), if expanded { 2 } else { 1 });
        assert_eq!(directory.output.join("empty-child").exists(), expanded);
        assert_eq!(
            directory.output.join("metadata.meta4").exists(),
            matches!(mode, "true" | "false")
        );
        if matches!(mode, "true" | "false") {
            assert_eq!(
                std::fs::read(directory.output.join("metadata.meta4")).unwrap(),
                expected
            );
        }
        server.abort();
        plane.shutdown().unwrap();
        let plane = directory.control_plane();
        assert_eq!(plane.tasks.len(), if expanded { 2 } else { 1 });
        plane.shutdown().unwrap();
    }
}
