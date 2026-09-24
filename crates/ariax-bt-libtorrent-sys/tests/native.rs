#![cfg(feature = "native")]

use ariax_bt_libtorrent_sys::{NativeOptions, new_session};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const PAYLOAD: &[u8] = include_bytes!("fixtures/payload.bin");
const TORRENTS: [&[u8]; 3] = [
    include_bytes!("fixtures/v1.torrent"),
    include_bytes!("fixtures/v2.torrent"),
    include_bytes!("fixtures/hybrid.torrent"),
];
static NEXT: AtomicU64 = AtomicU64::new(1);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "ariax-bt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(std::fs::canonicalize(path).unwrap())
    }
    fn text(&self) -> &str {
        self.0.to_str().unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn options() -> NativeOptions {
    NativeOptions {
        listen: "127.0.0.1:0".into(),
        max_torrents: 16,
        connections: 32,
        files: 32,
        disk_threads: 1,
        metadata_bytes: 16 * 1024 * 1024,
        max_files: 10_000,
        max_pieces: 1_000_000,
        decode_depth: 32,
        decode_tokens: 200_000,
        alert_items: 1,
        download_limit: 0,
        upload_limit: 0,
        dht: false,
        pex: false,
        allow_private: true,
        encryption: 1,
    }
}

fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !condition() {
        assert!(Instant::now() < deadline, "native fixture deadline");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn native_v1_v2_hybrid_transfers_hold_storage_and_checkpoint_without_alert_delivery() {
    for torrent in TORRENTS {
        let seed_root = Directory::new();
        let output_root = Directory::new();
        std::fs::write(seed_root.0.join("payload.bin"), PAYLOAD).unwrap();
        let mut seed = new_session(&options()).unwrap();
        let mut download = new_session(&options()).unwrap();
        seed.pin_mut()
            .add(1, torrent, "", seed_root.text(), &[])
            .unwrap();
        download
            .pin_mut()
            .add(2, torrent, "", output_root.text(), &[])
            .unwrap();
        assert!(download.status(2).unwrap().held);
        assert!(!output_root.0.join("payload.bin").exists());
        assert!(
            download
                .pin_mut()
                .approve(2, &vec!["../escape".into()], &[4])
                .is_err()
        );
        assert!(!output_root.0.join("payload.bin").exists());
        let metadata = seed.metadata(1).unwrap();
        let paths = metadata
            .padding
            .iter()
            .enumerate()
            .map(|(index, padding)| {
                if *padding == 0 {
                    "payload.bin".into()
                } else {
                    format!(".ariax-padding/{index}")
                }
            })
            .collect::<Vec<_>>();
        let priorities = metadata
            .padding
            .iter()
            .map(|padding| if *padding == 0 { 4 } else { 0 })
            .collect::<Vec<_>>();
        seed.pin_mut().approve(1, &paths, &priorities).unwrap();
        download.pin_mut().approve(2, &paths, &priorities).unwrap();
        assert!(download.pin_mut().approve(2, &paths, &priorities).is_err());
        seed.pin_mut().resume(1).unwrap();
        download.pin_mut().resume(2).unwrap();
        until(|| seed.status(1).unwrap().seeding && seed.listen_port() != 0);
        download
            .pin_mut()
            .connect_peer(2, "127.0.0.1", seed.listen_port())
            .unwrap();
        until(|| download.status(2).unwrap().seeding);
        assert_eq!(
            std::fs::read(output_root.0.join("payload.bin")).unwrap(),
            PAYLOAD
        );
        download
            .pin_mut()
            .checkpoint(2, 10, 16 * 1024 * 1024)
            .unwrap();
        assert!(download.pin_mut().poll_checkpoint(2, 9).is_err());
        let mut data = Vec::new();
        until(|| {
            let result = download.pin_mut().poll_checkpoint(2, 10).unwrap();
            if result.state == 0 {
                return false;
            }
            assert_eq!(result.state, 1);
            data = result.data;
            true
        });
        assert!(!data.is_empty());
        assert!(download.pin_mut().poll_checkpoint(2, 10).is_err());
        // No alert was drained while metadata, pieces, completion and resume
        // were processed through the one-item upstream alert queue.
        assert!(download.pin_mut().drain_alerts().unwrap() > 0);
        download.pin_mut().checkpoint(2, 11, 1).unwrap();
        until(|| {
            let result = download.pin_mut().poll_checkpoint(2, 11).unwrap();
            if result.state == 0 {
                return false;
            }
            assert_eq!(result.state, 2);
            assert!(result.data.is_empty());
            true
        });
        download.pin_mut().remove(2).unwrap();
        assert!(download.status(2).is_err());
        drop(download);
        let mut restored = new_session(&options()).unwrap();
        restored
            .pin_mut()
            .add(2, torrent, "", output_root.text(), &data)
            .unwrap();
        restored.pin_mut().approve(2, &paths, &priorities).unwrap();
        restored.pin_mut().resume(2).unwrap();
        until(|| restored.status(2).unwrap().seeding);
    }
}

#[test]
fn native_rejection_is_bounded_redacted_and_keeps_callbacks_owned_on_drop() {
    let root = Directory::new();
    let mut invalid = options();
    invalid.connections = u32::MAX;
    assert!(new_session(&invalid).is_err());
    let mut session = new_session(&options()).unwrap();
    let error = session
        .pin_mut()
        .add(1, b"secret-canary", "", root.text(), &[])
        .unwrap_err();
    assert!(!error.to_string().contains("secret-canary"));
    assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
    session
        .pin_mut()
        .add(1, TORRENTS[0], "", root.text(), &[])
        .unwrap();
    assert!(
        session
            .pin_mut()
            .add(2, TORRENTS[0], "", root.text(), &[])
            .is_err()
    );
    session.pin_mut().checkpoint(1, 1, 1024 * 1024).unwrap();
    drop(session); // The native callback retains its state until session drain.
    assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
}
