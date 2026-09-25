#![cfg(feature = "libtorrent")]

use ariax_bt::*;
use ariax_runtime::{ByteBudget, HandleBudgetLimits, HandleBudgets};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const V1: &[u8] = include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent");
const PAYLOAD: &[u8] = include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/payload.bin");
static NEXT: AtomicU64 = AtomicU64::new(1);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "ariax-bt-adapter-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        #[cfg(windows)]
        ariax_windows_security::create_private_directory(&path).unwrap();
        #[cfg(not(windows))]
        std::fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }
    fn root(&self) -> ProtectedRoot {
        ProtectedRoot::open(&self.0).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn resources() -> BtResources {
    BtResources {
        resident: ByteBudget::new(512 * 1024 * 1024),
        threads: ByteBudget::new(32),
        handles: HandleBudgets::new(HandleBudgetLimits {
            process: 1024,
            sockets: 768,
            files: 256,
        })
        .unwrap(),
    }
}
fn config() -> BtAdapterConfig {
    BtAdapterConfig {
        max_torrents: 4,
        peers: 32,
        files: 8,
        allow_private: true,
        dht: false,
        pex: false,
        metadata: MetadataLimits {
            bytes: 1024 * 1024,
            ..MetadataLimits::default()
        },
        ..BtAdapterConfig::default()
    }
}
fn settings() -> BtTaskSettings {
    BtTaskSettings {
        peers: 16,
        dht: false,
        pex: false,
        ..BtTaskSettings::default()
    }
}
fn admission(handle: &BtHandle, root: &Directory, gid: u64) -> BtAdmission {
    BtAdmission {
        gid,
        root: root.root(),
        torrent: Some(handle.blob(V1.to_vec()).unwrap()),
        magnet: None,
        resume: None,
        mapping: MappingOptions::default(),
        settings: settings(),
        expected_identity: None,
        expected_mapping: None,
        allow_existing: false,
    }
}
fn call(handle: &BtHandle, command: BtCommand) -> Result<BtReply, BtError> {
    handle.submit(command)?.wait(Duration::from_secs(10))
}
fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !condition() {
        assert!(Instant::now() < deadline, "adapter fixture deadline");
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn mapping(handle: &BtHandle, gid: u64) -> Vec<FileMapping> {
    match call(handle, BtCommand::ReadMetadata { gid }).unwrap() {
        BtReply::Metadata { mapping, .. } => mapping,
        _ => panic!("metadata completion"),
    }
}
fn stop(adapter: &mut BtAdapter) {
    adapter.request_stop();
    until(|| adapter.poll_stopped());
}

#[test]
fn magnet_storage_waits_for_exact_approval_and_transfers_through_owned_commands() {
    let seed_root = Directory::new();
    let output_root = Directory::new();
    std::fs::write(seed_root.0.join("payload.bin"), PAYLOAD).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            seed_root.0.join("payload.bin"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    let mut seed = BtAdapter::start(config(), resources()).unwrap();
    let mut output = BtAdapter::start(config(), resources()).unwrap();
    let sh = seed.handle();
    let oh = output.handle();
    let mut add_seed = admission(&sh, &seed_root, 1);
    add_seed.allow_existing = true;
    call(&sh, BtCommand::Add(Box::new(add_seed))).unwrap();
    call(
        &sh,
        BtCommand::Approve {
            gid: 1,
            mapping: mapping(&sh, 1),
        },
    )
    .unwrap();
    call(&sh, BtCommand::Resume { gid: 1 }).unwrap();
    until(|| sh.snapshot(1).is_some_and(|status| status.seeding) && sh.listen_port() != 0);
    let mut add = admission(&oh, &output_root, 2);
    add.torrent = None;
    let identity = parse_torrent(V1, MetadataLimits::default())
        .unwrap()
        .identity;
    add.magnet = Some(format!("magnet:?xt=urn:btih:{}", identity.v1.unwrap()));
    call(&oh, BtCommand::Add(Box::new(add))).unwrap();
    call(
        &oh,
        BtCommand::ConnectPeer {
            gid: 2,
            address: ([127, 0, 0, 1], sh.listen_port()).into(),
        },
    )
    .unwrap();
    until(|| {
        oh.snapshot(2)
            .is_some_and(|status| status.metadata && status.held)
    });
    assert_eq!(std::fs::read_dir(&output_root.0).unwrap().count(), 0);
    let approved_mapping = mapping(&oh, 2);
    let mut wrong = approved_mapping.clone();
    wrong[0].path = "different.bin".into();
    assert!(matches!(
        call(
            &oh,
            BtCommand::Approve {
                gid: 2,
                mapping: wrong
            }
        ),
        Err(BtError::IdentityMismatch)
    ));
    assert_eq!(std::fs::read_dir(&output_root.0).unwrap().count(), 0);
    call(
        &oh,
        BtCommand::Approve {
            gid: 2,
            mapping: approved_mapping,
        },
    )
    .unwrap();
    call(&oh, BtCommand::Resume { gid: 2 }).unwrap();
    call(
        &oh,
        BtCommand::ConnectPeer {
            gid: 2,
            address: ([127, 0, 0, 1], sh.listen_port()).into(),
        },
    )
    .unwrap();
    until(|| oh.snapshot(2).is_some_and(|status| status.seeding));
    assert_eq!(
        std::fs::read(output_root.0.join("payload.bin")).unwrap(),
        PAYLOAD
    );
    assert!(matches!(
        call(&oh, BtCommand::Remove { gid: 2 }),
        Err(BtError::CheckpointFailed)
    ));
    let checkpoint = call(
        &oh,
        BtCommand::Checkpoint {
            gid: 2,
            request: 1,
            limit: 1024 * 1024,
            timeout: Duration::from_secs(5),
        },
    )
    .unwrap();
    let BtReply::Checkpoint { request: 1, data } = checkpoint else {
        panic!("tracked checkpoint")
    };
    let identity = parse_torrent(V1, MetadataLimits::default())
        .unwrap()
        .identity;
    validate_resume(data.bytes(), &identity).unwrap();
    call(&oh, BtCommand::Remove { gid: 2 }).unwrap();
    // A new native handle validates the saved identity and rechecks payload bytes.
    let mut restored = admission(&oh, &output_root, 3);
    restored.resume = Some(data);
    restored.allow_existing = true;
    call(&oh, BtCommand::Add(Box::new(restored))).unwrap();
    call(
        &oh,
        BtCommand::Approve {
            gid: 3,
            mapping: mapping(&oh, 3),
        },
    )
    .unwrap();
    call(&oh, BtCommand::Resume { gid: 3 }).unwrap();
    until(|| oh.snapshot(3).is_some_and(|status| status.seeding));
    stop(&mut output);
    stop(&mut seed);
}

#[test]
fn unread_completions_retain_credit_live_versions_reject_stale_and_shutdown_releases_resources() {
    let root = Directory::new();
    let resources = resources();
    let mut adapter = BtAdapter::start(config(), resources.clone()).unwrap();
    let handle = adapter.handle();
    call(
        &handle,
        BtCommand::Add(Box::new(admission(&handle, &root, 1))),
    )
    .unwrap();
    let mut changed = settings();
    changed.download_limit = 12345;
    assert!(matches!(
        call(
            &handle,
            BtCommand::SetTask {
                gid: 1,
                version: 1,
                settings: changed.clone()
            }
        )
        .unwrap(),
        BtReply::Applied { version: 1 }
    ));
    assert!(matches!(
        call(
            &handle,
            BtCommand::SetTask {
                gid: 1,
                version: 1,
                settings: changed
            }
        ),
        Err(BtError::StaleCompletion)
    ));
    assert!(matches!(
        call(
            &handle,
            BtCommand::Add(Box::new(admission(&handle, &root, 2)))
        ),
        Err(BtError::IdentityMismatch)
    ));
    let pending = handle
        .submit(BtCommand::Checkpoint {
            gid: 1,
            request: 42,
            limit: 1024 * 1024,
            timeout: Duration::from_secs(1),
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(handle.pending_commands() >= 1);
    drop(pending);
    until(|| {
        while handle.event().is_some() {}
        handle.pending_commands() == 0
    });
    call(&handle, BtCommand::Remove { gid: 1 }).unwrap();
    stop(&mut adapter);
    assert!(matches!(
        handle.submit(BtCommand::Peers { gid: 1, limit: 1 }),
        Err(BtError::Closed)
    ));
    assert_eq!(resources.resident.used(), 0);
    assert_eq!(resources.threads.used(), 0);
    assert_eq!(resources.handles.available_process(), 1024);
}

#[test]
fn resource_exhaustion_and_private_peer_rejection_have_no_unowned_native_work() {
    let root = Directory::new();
    let mut exhausted = resources();
    exhausted.threads = ByteBudget::new(1);
    assert!(matches!(
        BtAdapter::start(config(), exhausted.clone()),
        Err(BtError::Overloaded)
    ));
    assert_eq!(exhausted.resident.used(), 0);
    let mut config = config();
    config.allow_private = false;
    let mut adapter = BtAdapter::start(config, resources()).unwrap();
    let handle = adapter.handle();
    call(
        &handle,
        BtCommand::Add(Box::new(admission(&handle, &root, 1))),
    )
    .unwrap();
    assert!(matches!(
        call(
            &handle,
            BtCommand::ConnectPeer {
                gid: 1,
                address: ([127, 0, 0, 1], 6881).into()
            }
        ),
        Err(BtError::Destination)
    ));
    assert!(matches!(
        call(
            &handle,
            BtCommand::Checkpoint {
                gid: 1,
                request: 1,
                limit: 1,
                timeout: Duration::from_secs(5)
            }
        ),
        Err(BtError::CheckpointFailed)
    ));
    stop(&mut adapter);
    assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
}

#[test]
fn full_completion_capacity_returns_unaccepted_blob_ownership_for_retry() {
    let root = Directory::new();
    let mut config = config();
    config.bridge.completions = 1;
    let mut adapter = BtAdapter::start(config, resources()).unwrap();
    let handle = adapter.handle();
    let held = handle
        .submit(BtCommand::SetRates {
            download: Some(0),
            upload: 0,
        })
        .unwrap();
    let command = BtCommand::Add(Box::new(admission(&handle, &root, 1)));
    let (returned, error) = match handle.try_submit_owned(command) {
        Ok(_) => panic!("unread completion must retain its capacity"),
        Err(rejected) => rejected,
    };
    assert_eq!(error, BtError::Overloaded);
    assert!(
        matches!(&returned, BtCommand::Add(admission) if admission.torrent.as_ref().unwrap().bytes() == V1)
    );
    held.wait(Duration::from_secs(10)).unwrap();
    call(&handle, returned).unwrap();
    call(
        &handle,
        BtCommand::SetRates {
            download: None,
            upload: 0,
        },
    )
    .unwrap();
    stop(&mut adapter);
    assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
}
