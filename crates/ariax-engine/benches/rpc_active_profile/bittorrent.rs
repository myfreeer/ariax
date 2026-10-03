//! A real peer fixture outside the engine's measured resident set.

use super::*;
use ariax_bt::BtHandle;
use sha1::{Digest as _, Sha1};
use std::net::Ipv4Addr;
use tokio::net::TcpSocket;

#[path = "peer_wire.rs"]
mod peer_wire;
use peer_wire::{BLOCK_BYTES, PEERS, PIECE_BYTES, PIECES, PeerWire};

pub(super) fn admit(plane: &mut HttpControlPlane) -> Result<(Value, String)> {
    use base64ct::Encoding as _;
    let digest = Sha1::digest(vec![0xa5; PIECE_BYTES]);
    let mut info = format!(
        "d6:lengthi{}e4:name11:payload.bin12:piece lengthi{PIECE_BYTES}e6:pieces{}:",
        PIECES * PIECE_BYTES,
        PIECES * digest.len()
    )
    .into_bytes();
    for _ in 0..PIECES {
        info.extend_from_slice(&digest);
    }
    info.push(b'e');
    let hash = Sha1::digest(&info)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    let gid = plane.call(
        "aria2.addTorrent",
        json!([
            base64ct::Base64::encode_string(&torrent), [],
            {"bt-max-peers":PEERS, "enable-dht":false, "enable-peer-exchange":false, "seed-ratio":0}
        ]),
    )?;
    Ok((gid, hash))
}

pub(super) async fn ready(plane: &mut HttpControlPlane, gid: &Value) -> Result<(BtHandle, u64)> {
    let gid: ariax_core::Gid = gid.as_str().ok_or("BT GID")?.parse()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        plane.poll_once()?;
        if let Some(handle) = plane.bittorrent_handle()
            && handle.listen_port() != 0
            && handle.snapshot(gid.get()).is_some_and(|state| {
                state.metadata
                    && !state.held
                    && !state.paused
                    && !state.checking
                    && state.error == 0
            })
        {
            return Ok((handle, gid.get()));
        }
        if Instant::now() >= deadline {
            return Err("BT fixture did not enter downloading state".into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub(super) fn metrics(value: &mut Value, bt: &Option<(BtHandle, u64)>) {
    if let Some((handle, gid)) = bt
        && let Some(state) = handle.snapshot(*gid)
    {
        value["btPeers"] = json!(state.peers);
        value["btDownloaded"] = json!(state.downloaded);
        value["btDone"] = json!(state.finished);
        value["btError"] = json!(state.error);
    }
}

pub(super) async fn peers(port: &str, hash: &str) -> Result<()> {
    let port: u16 = port.parse()?;
    if hash.len() != 40 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid peer fixture identity".into());
    }
    let mut identity = [0; 20];
    for (index, byte) in identity.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16)?;
    }
    let state = Arc::new(OriginState::default());
    let (pulse, _) = watch::channel(0);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    println!("{}", listener.local_addr()?);
    io::stdout().flush()?;
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(origin_listener(listener, state.clone(), pulse.clone()));
    for index in 0..PEERS {
        tasks.spawn(peer(
            index,
            port,
            identity,
            state.clone(),
            pulse.subscribe(),
        ));
    }
    tasks
        .join_next()
        .await
        .ok_or("missing peer fixture task")???;
    Ok(()) // Engine shutdown closes its peers; barriers detect any earlier exit.
}

async fn peer(
    index: usize,
    port: u16,
    identity: [u8; 20],
    state: Arc<OriginState>,
    mut pulse: watch::Receiver<usize>,
) -> Result<()> {
    let socket = TcpSocket::new_v4()?;
    // Libtorrent rejects duplicate peer IPs by default. Linux routes all 127/8
    // locally, so every fixture connection has its own real source address.
    let address = Ipv4Addr::new(127, 1, (index / 250 + 1) as u8, (index % 250 + 1) as u8);
    socket.bind(SocketAddr::from((address, 0)))?;
    let mut stream = socket.connect(([127, 0, 0, 1], port).into()).await?;
    stream.set_nodelay(true)?;
    let mut handshake = b"\x13BitTorrent protocol\0\0\0\0\0\0\0\0".to_vec();
    handshake.extend_from_slice(&identity);
    handshake.extend_from_slice(format!("-AX0600-{index:012}").as_bytes());
    stream.write_all(&handshake).await?;
    let mut reply = [0; 68];
    tokio::time::timeout(Duration::from_secs(15), stream.read_exact(&mut reply)).await??;
    if reply[..20] != handshake[..20] || reply[28..48] != identity {
        return Err("peer handshake identity mismatch".into());
    }
    let mut bitfield = vec![0; PIECES.div_ceil(8) + 1];
    bitfield[0] = 5;
    for piece in (index..PIECES).step_by(PEERS) {
        bitfield[1 + piece / 8] |= 0x80 >> (piece % 8);
    }
    stream
        .write_all(&(bitfield.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&bitfield).await?;
    stream.write_all(&[0, 0, 0, 1, 1]).await?; // unchoke
    state.active.fetch_add(1, Ordering::SeqCst);
    let _active = ActiveRange(state.clone());
    let mut wire = PeerWire::new(index);
    let mut bytes = [0; 8192];
    let mut credit = true;
    let mut epoch = *pulse.borrow_and_update();
    loop {
        if credit && let Some(request) = wire.next_request() {
            let mut block = Vec::with_capacity(BLOCK_BYTES + 13);
            block.extend_from_slice(&((BLOCK_BYTES + 9) as u32).to_be_bytes());
            block.push(7);
            block.extend_from_slice(&request.piece.to_be_bytes());
            block.extend_from_slice(&request.offset.to_be_bytes());
            block.resize(BLOCK_BYTES + 13, 0xa5);
            stream.write_all(&block).await?;
            credit = false;
            if epoch != 0 {
                state.acknowledgements.fetch_add(1, Ordering::SeqCst);
            }
        }
        tokio::select! {
            changed = pulse.changed() => {
                changed?;
                let next = *pulse.borrow_and_update();
                if credit || next != epoch + 1 {
                    return Err("peer pulse overtook an unacknowledged block".into());
                }
                epoch = next;
                credit = true;
            }
            count = stream.read(&mut bytes) => {
                let count = count?;
                if count == 0 { return Ok(()); }
                wire.push(&bytes[..count])?;
            }
        }
    }
}

pub(super) struct PeerProcess {
    child: Child,
    address: SocketAddr,
    gid: Value,
    errors: tokio::task::JoinHandle<()>,
    pub(super) renewed: usize,
    pub(super) downloaded: u64,
}

impl PeerProcess {
    pub(super) async fn start(info: &Value) -> Result<Self> {
        let mut child = spawn(&[
            "--bt-peers",
            &info["btPort"].to_string(),
            info["btHash"].as_str().ok_or("BT hash")?,
        ])?;
        let mut output = BufReader::new(async_pipe(child.stdout.take().ok_or("peer stdout")?));
        let mut errors = BufReader::new(async_pipe(child.stderr.take().ok_or("peer stderr")?));
        let errors = tokio::spawn(async move {
            loop {
                let mut line = String::new();
                match errors.read_line(&mut line).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => eprint!("BT peers: {line}"),
                }
            }
        });
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(10), output.read_line(&mut line)).await??;
        Ok(Self {
            child,
            address: line.trim().parse()?,
            gid: info["btGid"].clone(),
            errors,
            renewed: 0,
            downloaded: 0,
        })
    }

    pub(super) async fn barrier(&mut self, client: &mut Client, renew: bool) -> Result<Value> {
        let metric_request = request("bench.metrics", json!([]));
        let peer_request = request("aria2.getPeers", json!([self.gid]));
        let before = client.call(&metric_request).await?;
        let expected = if renew {
            before["btDownloaded"]
                .as_u64()
                .ok_or("missing BT byte metric")?
                + (PEERS * BLOCK_BYTES) as u64
        } else {
            ((self.renewed + 1) * PEERS * BLOCK_BYTES) as u64
        };
        if renew {
            origin_metrics(self.address, true).await?;
        }
        let deadline = Instant::now() + Duration::from_secs(25);
        loop {
            let metrics = client.call(&metric_request).await?;
            let remote = origin_metrics(self.address, false).await?;
            if remote["active"] == PEERS
                && metrics["btPeers"] == PEERS
                && metrics["btDone"] == false
                && metrics["btError"] == 0
                && metrics["connections"] == RANGES
                && metrics["btDownloaded"].as_u64().unwrap_or(0) >= expected
                && (!renew || remote["acks"] == PEERS)
                && client.call(&peer_request).await?.as_array().map(Vec::len) == Some(PEERS)
            {
                self.renewed += usize::from(renew);
                self.downloaded = metrics["btDownloaded"].as_u64().unwrap();
                return Ok(metrics);
            }
            if self.child.try_wait()?.is_some() || Instant::now() >= deadline {
                return Err(
                    format!("BT peer barrier failed: peers={remote} engine={metrics}").into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub(super) async fn stop(mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            tokio::time::timeout(Duration::from_secs(5), self.child.kill()).await??;
        }
        tokio::time::timeout(Duration::from_secs(5), self.errors).await??;
        Ok(())
    }
}
