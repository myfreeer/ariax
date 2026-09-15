pub mod error;
pub mod fs;
mod handler;
pub mod rawsession;
pub(crate) mod runtime;
mod session;

pub use handler::Handler;
pub use rawsession::RawSftpSession;
pub use session::SftpSession;

use bytes::Bytes;
use tokio::{
    io::{self, AsyncRead, AsyncWrite, AsyncWriteExt},
    select,
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use crate::{error::Error, protocol::Packet, utils::read_packet};

macro_rules! into_wrap {
    ($handler:expr) => {
        match $handler.await {
            Err(error) => Err(error.into()),
            Ok(()) => Ok(()),
        }
    };
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum size of a single packet in bytes. Default: 128 KiB; hard maximum: 1 MiB.
    pub max_packet_len: u32,
    /// Maximum number of concurrent in-flight write requests. Default: 8.
    pub max_concurrent_writes: usize,
    /// Timeout in seconds for each request. Default: 10.
    pub request_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_packet_len: 131072,
            max_concurrent_writes: 8,
            request_timeout_secs: 10,
        }
    }
}

async fn execute_handler<H>(bytes: &mut Bytes, handler: &mut H) -> Result<(), error::Error>
where
    H: Handler + Send,
{
    match Packet::try_from(bytes)? {
        Packet::Version(p) => into_wrap!(handler.version(p)),
        Packet::Status(p) => into_wrap!(handler.status(p)),
        Packet::Handle(p) => into_wrap!(handler.handle(p)),
        Packet::Data(p) => into_wrap!(handler.data(p)),
        Packet::Name(p) => into_wrap!(handler.name(p)),
        Packet::Attrs(p) => into_wrap!(handler.attrs(p)),
        Packet::ExtendedReply(p) => into_wrap!(handler.extended_reply(p)),
        _ => Err(error::Error::UnexpectedBehavior(
            "A packet was received that could not be processed.".to_owned(),
        )),
    }
}

/// Ariax framing/admission patch. Checked against vendored source provenance.
pub const ARIAX_BOUNDED_PROTOCOL_REVISION: u32 = 1;
pub const MAX_PENDING_REQUESTS: usize = 65;
pub const MAX_PACKET_LENGTH: u32 = 1024 * 1024;

/// Run a bounded client with the default receive cap.
pub fn run<S, H>(stream: S, handler: H) -> mpsc::Sender<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    run_bounded(stream, handler,
        std::sync::Arc::new(std::sync::atomic::AtomicU32::new(Config::default().max_packet_len)),
        CancellationToken::new())
}

fn run_bounded<S, H>(stream: S, handler: H,
    cap: std::sync::Arc<std::sync::atomic::AtomicU32>, cancel: CancellationToken) -> mpsc::Sender<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    run_bounded_tracked(stream,handler,cap,cancel).0
}

struct DrainGuard(std::sync::Arc<(std::sync::atomic::AtomicUsize,tokio::sync::watch::Sender<bool>)>);
impl Drop for DrainGuard {
    fn drop(&mut self) {
        if self.0.0.fetch_sub(1,std::sync::atomic::Ordering::AcqRel) == 1 { self.0.1.send_replace(true); }
    }
}

fn run_bounded_tracked<S,H>(stream:S,mut handler:H,
    cap:std::sync::Arc<std::sync::atomic::AtomicU32>,cancel:CancellationToken)->(mpsc::Sender<Bytes>,tokio::sync::watch::Receiver<bool>)
where S:AsyncRead+AsyncWrite+Unpin+Send+'static,H:Handler+Send+'static {
    let (drained,receiver) = tokio::sync::watch::channel(false);
    let remaining = std::sync::Arc::new((std::sync::atomic::AtomicUsize::new(2),drained));
    let read_guard = DrainGuard(remaining.clone()); let write_guard = DrainGuard(remaining);
    let (tx, mut rx) = mpsc::channel::<Bytes>(MAX_PENDING_REQUESTS);
    let (mut rd, mut wr) = io::split(stream);
    let rc = cancel.clone();
    runtime::spawn(async move {
        let _drain = read_guard;
        let failure = loop {
            let limit = cap.load(std::sync::atomic::Ordering::Acquire).clamp(1, MAX_PACKET_LENGTH);
            let result = select! {
                biased;
                _ = rc.cancelled() => break error::Error::SessionClosed,
                result = read_packet(&mut rd, limit) => result,
            };
            match result {
                Ok(mut bytes) => if let Err(error) = execute_handler(&mut bytes, &mut handler).await {
                    break error;
                },
                Err(Error::PacketTooLarge) => break error::Error::PacketTooLarge,
                Err(Error::UnexpectedEof) => break error::Error::SessionClosed,
                Err(_) => break error::Error::MalformedPacket,
            }
        };
        // Drop decoder buffers before completing reservations retained by callers.
        drop(rd);
        rc.cancel();
        handler.closed(failure).await;
    });
    runtime::spawn(async move {
        let _drain = write_guard;
        loop {
            let data = select! {
                biased;
                _ = cancel.cancelled() => break,
                data = rx.recv() => match data { Some(data) => data, None => break },
            };
            if data.is_empty() { break; }
            let result = select! {
                biased;
                _ = cancel.cancelled() => break,
                result = wr.write_all(&data) => result,
            };
            if result.is_err() { break; }
        }
        rx.close();
        drop(rx);
        cancel.cancel();
        drop(wr);
    });
    (tx,receiver)
}

#[cfg(test)]
mod bounded_tests;
