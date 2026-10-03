#![forbid(unsafe_code)]

// Exercise the same bootstrap used by the custom, opt-in benchmark.
#[path = "../benches/rpc_active_profile/setup.rs"]
mod setup;

#[cfg(feature = "bt")]
use ariax_core::OptionPatchRejectReason;
#[cfg(feature = "bt")]
use ariax_engine::HttpControlError;
use ariax_engine::HttpProcessResources;
use ariax_runtime::RuntimeProfile;
#[cfg(feature = "bt")]
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "ariax-benchmark-setup-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        setup::private_directory(&path).unwrap();
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn scheduler_slots_allow_both_transfers_and_reject_insufficient_capacity() {
    let ordinary = setup::scheduler_config(32, false).unwrap();
    let mixed = setup::scheduler_config(32, true).unwrap();
    assert_eq!(ordinary.max_active_tasks.get(), 1);
    assert_eq!(mixed.max_active_tasks.get(), 2);
    assert_eq!(mixed.max_tasks.get(), 32);
    assert!(setup::scheduler_config(1, true).is_err());
    assert!(setup::scheduler_config(0, false).is_err());
}

#[cfg(all(feature = "bt", target_os = "linux"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_peer_admission_reaches_its_configured_cap_and_rejects_excess() {
    use base64ct::Encoding as _;
    use sha1::{Digest as _, Sha1};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpSocket;

    const PEERS: usize = 12;
    let root = Root::new();
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency).unwrap();
    let (mut plane, _) = setup::build_control_plane(&root.0, &resources, 32, true).unwrap();
    let mut info =
        b"d6:lengthi1048576e4:name11:payload.bin12:piece lengthi1048576e6:pieces20:".to_vec();
    info.extend_from_slice(&Sha1::digest(vec![0xa5; 1024 * 1024]));
    info.push(b'e');
    let identity = Sha1::digest(&info);
    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    let gid = plane.call("aria2.addTorrent", json!([
        base64ct::Base64::encode_string(&torrent), [],
        {"bt-max-peers":PEERS,"enable-dht":false,"enable-peer-exchange":false,"seed-ratio":0}
    ])).unwrap();
    let native_id = u64::from_str_radix(gid.as_str().unwrap(), 16).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let native = loop {
        plane.poll_once().unwrap();
        if let Some(handle) = plane.bittorrent_handle()
            && handle.listen_port() != 0
            && handle
                .snapshot(native_id)
                .is_some_and(|s| s.metadata && !s.held && !s.paused && !s.checking && s.error == 0)
        {
            break handle;
        }
        assert!(Instant::now() < deadline, "torrent did not become ready");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let mut sockets = Vec::new();
    for index in 0..=PEERS {
        let socket = TcpSocket::new_v4().unwrap();
        socket
            .bind(SocketAddr::from((
                Ipv4Addr::new(127, 2, 0, index as u8 + 1),
                0,
            )))
            .unwrap();
        let mut stream = socket
            .connect(([127, 0, 0, 1], native.listen_port()).into())
            .await
            .unwrap();
        let mut handshake = b"\x13BitTorrent protocol\0\0\0\0\0\0\0\0".to_vec();
        handshake.extend_from_slice(&identity);
        handshake.extend_from_slice(format!("-AX0600-{index:012}").as_bytes());
        stream.write_all(&handshake).await.unwrap();
        if index < PEERS {
            let mut reply = [0; 68];
            tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut reply))
                .await
                .unwrap()
                .unwrap_or_else(|error| panic!("peer {index} rejected below cap {PEERS}: {error}"));
            assert_eq!(&reply[28..48], identity.as_slice());
            stream
                .write_all(&[0, 0, 0, 2, 5, 0x80, 0, 0, 0, 1, 1])
                .await
                .unwrap();
        }
        sockets.push(stream);
        if index == PEERS - 1 {
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                plane.poll_once().unwrap();
                if native.snapshot(native_id).unwrap().peers as usize == PEERS {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "native peer count did not reach configured cap"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        plane.poll_once().unwrap();
        assert!(native.snapshot(native_id).unwrap().peers as usize <= PEERS);
        let mut closed = false;
        for stream in &sockets {
            let mut bytes = [0; 4096];
            match stream.try_read(&mut bytes) {
                Ok(0) => closed = true,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    closed = true
                }
                Err(error) => panic!("unexpected peer read: {error}"),
            }
        }
        if closed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "excess connection was not rejected"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(sockets);
    plane.shutdown_async().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_setup_bootstraps_and_shuts_down() {
    let root = Root::new();
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency).unwrap();
    let (plane, _) = setup::build_control_plane(&root.0, &resources, 32, false).unwrap();
    assert_eq!(plane.diagnostics().task_count, 0);
    plane.shutdown_async().await.unwrap();
}

#[cfg(feature = "bt")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_setup_bootstraps_without_unsupported_runtime_patch() {
    let root = Root::new();
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency).unwrap();
    let (mut plane, _) = setup::build_control_plane(&root.0, &resources, 32, true).unwrap();
    // Keep the public rejection contract; the fixture must configure slots at startup.
    let error = plane
        .call(
            "aria2.changeGlobalOption",
            json!([{"max-concurrent-downloads":2}]),
        )
        .unwrap_err();
    match error {
        HttpControlError::OptionPatchRejected(rejected) => {
            assert_eq!(rejected.len(), 1);
            assert_eq!(rejected[0].name, "max-concurrent-downloads");
            assert_eq!(rejected[0].reason, OptionPatchRejectReason::Unsupported);
        }
        error => panic!("unexpected rejection: {error}"),
    }
    assert_eq!(plane.diagnostics().task_count, 0);
    plane.shutdown_async().await.unwrap();
}

#[cfg(not(feature = "bt"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_setup_without_bt_rejects_before_creating_state() {
    let root = Root::new();
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency).unwrap();
    let result = setup::build_control_plane(&root.0, &resources, 32, true);
    assert!(result.is_err());
    assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
}

#[cfg(feature = "bt")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_setup_runs_bt_beside_http_and_keeps_a_third_task_waiting() {
    use ariax_core::{ErrorKind, Generation, PublicError, RetryClass};
    use ariax_engine::{HttpCancellation, HttpTaskWorker, HttpWorkerFuture, TransferTaskSpec};
    use base64ct::Encoding as _;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    struct WaitingHttpWorker;
    impl HttpTaskWorker for WaitingHttpWorker {
        fn start(
            &self,
            _task: Arc<TransferTaskSpec>,
            _generation: Generation,
            cancellation: HttpCancellation,
        ) -> HttpWorkerFuture {
            Box::pin(async move {
                cancellation.cancelled().await;
                Err(PublicError::new(
                    ErrorKind::Cancelled,
                    "cancelled",
                    RetryClass::Never,
                ))
            })
        }
    }

    let root = Root::new();
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency).unwrap();
    let (mut plane, _) = setup::build_control_plane(&root.0, &resources, 32, true).unwrap();
    plane.attach_worker(Arc::new(WaitingHttpWorker)).unwrap();
    let http = plane
        .call("aria2.addUri", json!([["http://127.0.0.1:9/held.bin"]]))
        .unwrap();
    let torrent = include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent");
    let bt = plane
        .call(
            "aria2.addTorrent",
            json!([
                base64ct::Base64::encode_string(torrent), [],
                {"enable-dht":false,"enable-peer-exchange":false,"seed-ratio":0}
            ]),
        )
        .unwrap();
    let native_id = u64::from_str_radix(bt.as_str().unwrap(), 16).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        plane.poll_once().unwrap();
        let active = plane.call("aria2.tellActive", json!([])).unwrap();
        let tasks = active.as_array().unwrap();
        let both = tasks.iter().any(|task| task["gid"] == http)
            && tasks.iter().any(|task| task["gid"] == bt);
        let native_ready = plane.bittorrent_handle().is_some_and(|handle| {
            handle.snapshot(native_id).is_some_and(|state| {
                state.metadata
                    && !state.held
                    && !state.paused
                    && !state.checking
                    && state.error == 0
            })
        });
        if both && native_ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "both transfers did not become active: {active}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let waiting = plane
        .call("aria2.addUri", json!([["http://127.0.0.1:9/waiting.bin"]]))
        .unwrap();
    plane.poll_once().unwrap();
    let status = plane.call("aria2.tellStatus", json!([waiting])).unwrap();
    assert_eq!(status["status"], "waiting");
    assert_eq!(
        plane
            .call("aria2.tellActive", json!([]))
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    plane.shutdown_async().await.unwrap();
}
