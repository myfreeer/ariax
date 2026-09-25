//! Narrow, owned libtorrent bridge. Native work is confined to the adapter lane.
#![cfg(feature = "native")]

// SAFETY: C++ methods validate sizes/identifiers, copy all asynchronous inputs,
// and translate exceptions. No raw pointer, borrowed callback, or native object
// is returned to Rust. The opaque session is neither Send nor Sync; it is born,
// used, and destroyed on one adapter thread. Callback state is C++ owned.
#[cxx::bridge(namespace = "ariax::bt")]
pub mod ffi {
    #[derive(Clone, Debug)]
    struct NativeOptions {
        listen: String,
        max_torrents: u32,
        connections: u32,
        files: u32,
        disk_threads: u32,
        metadata_bytes: u32,
        max_files: u32,
        max_pieces: u32,
        decode_depth: u32,
        decode_tokens: u32,
        alert_items: u32,
        download_limit: u32,
        upload_limit: u32,
        dht: bool,
        pex: bool,
        allow_private: bool,
        encryption: u8,
    }

    #[derive(Clone, Debug, Default)]
    struct NativeMetadata {
        info: Vec<u8>,
        v1: Vec<u8>,
        v2: Vec<u8>,
        file_sizes: Vec<u64>,
        padding: Vec<u8>,
        symlinks: Vec<u8>,
        piece_length: u32,
        pieces: u32,
    }

    #[derive(Clone, Debug, Default)]
    struct NativeStatus {
        metadata: bool,
        held: bool,
        paused: bool,
        checking: bool,
        finished: bool,
        seeding: bool,
        error: u32,
        total_bytes: u64,
        done_bytes: u64,
        downloaded: u64,
        uploaded: u64,
        download_rate: u32,
        upload_rate: u32,
        peers: u32,
        seeds: u32,
        progress_ppm: u32,
        file_progress: Vec<u64>,
    }

    #[derive(Clone, Debug)]
    struct NativePeer {
        address: String,
        port: u16,
        peer_id: String,
        download_rate: u32,
        upload_rate: u32,
        seeder: bool,
        am_choking: bool,
        peer_choking: bool,
    }

    #[derive(Debug)]
    struct NativeCheckpoint {
        request: u64,
        state: u8,
        data: Vec<u8>,
    }

    unsafe extern "C++" {
        include!("bridge.h");
        type NativeSession;
        fn new_session(options: &NativeOptions) -> Result<UniquePtr<NativeSession>>;
        fn native_version() -> String;
        fn add(
            self: Pin<&mut NativeSession>,
            gid: u64,
            torrent: &[u8],
            magnet: &str,
            root: &str,
            resume: &[u8],
        ) -> Result<()>;
        fn metadata(self: &NativeSession, gid: u64) -> Result<NativeMetadata>;
        fn approve(
            self: Pin<&mut NativeSession>,
            gid: u64,
            paths: &Vec<String>,
            priorities: &[u8],
        ) -> Result<()>;
        fn resume(self: Pin<&mut NativeSession>, gid: u64) -> Result<()>;
        fn remove(self: Pin<&mut NativeSession>, gid: u64) -> Result<()>;
        fn status(self: &NativeSession, gid: u64) -> Result<NativeStatus>;
        fn peers(self: &NativeSession, gid: u64, limit: u32) -> Result<Vec<NativePeer>>;
        fn connect_peer(
            self: Pin<&mut NativeSession>,
            gid: u64,
            address: &str,
            port: u16,
        ) -> Result<()>;
        fn checkpoint(
            self: Pin<&mut NativeSession>,
            gid: u64,
            request: u64,
            limit: u32,
        ) -> Result<()>;
        fn poll_checkpoint(
            self: Pin<&mut NativeSession>,
            gid: u64,
            request: u64,
        ) -> Result<NativeCheckpoint>;
        fn set_rates(
            self: Pin<&mut NativeSession>,
            download: u32,
            upload: u32,
            suspended: bool,
        ) -> Result<()>;
        fn set_task_options(
            self: Pin<&mut NativeSession>,
            gid: u64,
            download: u32,
            upload: u32,
            peers: u32,
            dht: bool,
            pex: bool,
        ) -> Result<()>;
        fn drain_alerts(self: Pin<&mut NativeSession>) -> Result<u64>;
        fn listen_port(self: &NativeSession) -> u16;
    }
}

pub use ffi::*;
pub type NativeSessionOwner = cxx::UniquePtr<NativeSession>;
