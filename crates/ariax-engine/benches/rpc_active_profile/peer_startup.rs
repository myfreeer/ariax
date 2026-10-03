//! Bound connection startup separately from the number of live benchmark peers.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::Semaphore;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// The pinned libtorrent listener has a five-entry backlog. Keep startup below
// that bound; the permit does not limit the established peer population.
pub const HANDSHAKES: usize = 4;

pub async fn connect(
    index: usize,
    port: u16,
    identity: [u8; 20],
    handshakes: &Semaphore,
) -> Result<TcpStream> {
    let _permit = handshakes.acquire().await?;
    tokio::time::timeout(Duration::from_secs(15), async {
        let socket = TcpSocket::new_v4()?;
        // Libtorrent rejects duplicate peer IPs. Linux routes all 127/8 locally.
        let address = Ipv4Addr::new(127, 1, (index / 250 + 1) as u8, (index % 250 + 1) as u8);
        socket.bind(SocketAddr::from((address, 0)))?;
        let mut stream = socket
            .connect(([127, 0, 0, 1], port).into())
            .await
            .map_err(|error| format!("connect from {address}: {error}"))?;
        stream.set_nodelay(true)?;
        let mut handshake = b"\x13BitTorrent protocol\0\0\0\0\0\0\0\0".to_vec();
        handshake.extend_from_slice(&identity);
        handshake.extend_from_slice(format!("-AX0600-{index:012}").as_bytes());
        stream
            .write_all(&handshake)
            .await
            .map_err(|error| format!("handshake write: {error}"))?;
        let mut reply = [0; 68];
        stream
            .read_exact(&mut reply)
            .await
            .map_err(|error| format!("handshake reply: {error}"))?;
        if reply[..20] != handshake[..20] || reply[28..48] != identity {
            return Err("peer handshake identity mismatch".into());
        }
        Ok(stream)
    })
    .await
    .map_err(|_| "connection handshake exceeded 15 seconds")?
}
