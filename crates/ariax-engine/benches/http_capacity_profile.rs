use ariax_engine::{
    HttpClientRequest, HttpDestinationPolicy, HttpDirectTransportConfig, HttpPolicyClient,
    HttpPolicyClientConfig, HttpProcessResources, HttpResolver, HttpResolverConfig,
};
use ariax_runtime::{C10K_LOW_ACTIVITY_SOCKET_TARGET, RuntimeProfile};
use ariax_storage::GlobalSpan;
use hyper::header::CONTENT_RANGE;
use std::env;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
use tokio::sync::Barrier;

const ACTIVE_RANGES: usize = 1_000;
const ACTIVE_FRAME_BYTES: usize = 64 * 1024;
const RANGE_TOTAL_BYTES: usize = ACTIVE_RANGES * ACTIVE_FRAME_BYTES;

fn main() {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("--server") => {
            let count = arguments
                .next()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            if let Err(error) = run_server(count) {
                eprintln!("http capacity benchmark server failed: {error}");
                std::process::exit(1);
            }
            return;
        }
        Some("--range-server") => {
            let count = arguments
                .next()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            if let Err(error) = run_range_server(count) {
                eprintln!("http range benchmark server failed: {error}");
                std::process::exit(1);
            }
            return;
        }
        _ => {}
    }

    if env::var_os("ARIAX_RUN_C10K_BENCH").is_none() {
        println!(
            "http capacity profile benchmark compiled; set ARIAX_RUN_C10K_BENCH=1 to open {C10K_LOW_ACTIVITY_SOCKET_TARGET} loopback sockets"
        );
        return;
    }
    if let Err(error) = run_parent() {
        eprintln!("http capacity benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run_server(count: usize) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    println!("{}", listener.local_addr()?);
    std::io::stdout().flush()?;
    let mut sockets = Vec::with_capacity(count);
    for _ in 0..count {
        let (stream, _) = listener.accept()?;
        sockets.push(stream);
    }
    println!("READY");
    std::io::stdout().flush()?;
    let mut signal = [0_u8; 1];
    let _ = std::io::stdin().read(&mut signal);
    drop(sockets);
    Ok(())
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency)?;
    resources.require_c10k()?;
    let transport = resources.transport_budgets();
    let ingress = resources.ingress_budgets();
    let executable = env::current_exe()?;
    let mut child = ChildGuard::spawn(
        Command::new(executable)
            .arg("--server")
            .arg(C10K_LOW_ACTIVITY_SOCKET_TARGET.to_string())
            .stdout(Stdio::piped())
            .stdin(Stdio::piped()),
    )?;
    let stdout = child
        .child
        .stdout
        .take()
        .ok_or("benchmark server stdout was not piped")?;
    let mut lines = BufReader::new(stdout).lines();
    let address: std::net::SocketAddr = lines
        .next()
        .ok_or("benchmark server exited before publishing its address")??
        .parse()?;

    let started = Instant::now();
    let mut connections = Vec::with_capacity(C10K_LOW_ACTIVITY_SOCKET_TARGET);
    for _ in 0..C10K_LOW_ACTIVITY_SOCKET_TARGET {
        let permit = transport.try_acquire_connection()?;
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(10))?;
        connections.push((permit, stream));
    }
    let connect_elapsed = started.elapsed();
    match lines.next() {
        Some(Ok(line)) if line == "READY" => {}
        _ => return Err("benchmark server did not accept the full socket set".into()),
    }

    let active_started = Instant::now();
    let mut active = Vec::with_capacity(ACTIVE_RANGES);
    for _ in 0..ACTIVE_RANGES {
        let permit = ingress.try_acquire(ACTIVE_FRAME_BYTES)?;
        active.push((permit, vec![0_u8; ACTIVE_FRAME_BYTES]));
    }
    let active_elapsed = active_started.elapsed();
    let rss_kib = resident_set_kib();
    let limits = resources.profile().limits();
    let resident_reserved = resources.resident_budget().used();
    if resident_reserved > limits.accounted_resident_limit_bytes {
        return Err(format!(
            "modeled resident reservation {resident_reserved} exceeds accounted profile limit {}",
            limits.accounted_resident_limit_bytes
        )
        .into());
    }
    if rss_kib.is_some_and(|rss| {
        rss.saturating_mul(1024) > u64::try_from(limits.resident_target_bytes).unwrap_or(u64::MAX)
    }) {
        return Err(format!(
            "measured RSS {rss_kib:?} KiB exceeds profile target {} bytes",
            limits.resident_target_bytes
        )
        .into());
    }
    println!(
        "profile={} sockets={} active_ranges={} connect_ms={} active_reservation_ms={} resident_reserved_bytes={} accounted_limit_bytes={} resident_target_bytes={} rss_kib={rss_kib:?}",
        resources.profile().requested().code(),
        connections.len(),
        active.len(),
        connect_elapsed.as_millis(),
        active_elapsed.as_millis(),
        resident_reserved,
        limits.accounted_resident_limit_bytes,
        limits.resident_target_bytes,
    );

    drop(active);
    drop(connections);
    if resources.resident_budget().used() != 0 {
        return Err("resident budget did not release after benchmark permits dropped".into());
    }
    if let Some(stdin) = child.child.stdin.as_mut() {
        stdin.write_all(b"x")?;
        stdin.flush()?;
    }
    let status = child.child.wait()?;
    child.disarmed = true;
    if !status.success() {
        return Err(format!("benchmark server exited with {status}").into());
    }
    run_http_range_benchmark(&resources)
}

fn run_range_server(count: usize) -> Result<(), Box<dyn std::error::Error>> {
    if count == 0 {
        return Err("range benchmark server requires a nonzero connection count".into());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let listener = TokioTcpListener::bind("127.0.0.1:0").await?;
        println!("{}", listener.local_addr()?);
        std::io::stdout().flush()?;
        let mut workers = Vec::with_capacity(count);
        for _ in 0..count {
            let (stream, _) = listener.accept().await?;
            workers.push(tokio::spawn(serve_range_connection(stream)));
        }
        for worker in workers {
            worker.await??;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

async fn serve_range_connection(mut stream: TokioTcpStream) -> std::io::Result<()> {
    let request = read_request_head(&mut stream).await?;
    let (start, end) = parse_range(&request).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing or invalid range")
    })?;
    let expected = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "range overflow"))?;
    let total = u64::try_from(RANGE_TOTAL_BYTES).expect("benchmark total fits u64");
    if expected != u64::try_from(ACTIVE_FRAME_BYTES).expect("frame size fits u64") || end >= total {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "range is outside the benchmark contract",
        ));
    }
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {expected}\r\nContent-Range: bytes {start}-{end}/{total}\r\nETag: \"ariax-http-capacity\"\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await?;
    let chunk = [0x5a_u8; 8192];
    let mut remaining = usize::try_from(expected)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "range too large"))?;
    while remaining != 0 {
        let length = remaining.min(chunk.len());
        stream.write_all(&chunk[..length]).await?;
        remaining -= length;
    }
    Ok(())
}

async fn read_request_head(stream: &mut TokioTcpStream) -> std::io::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(1024);
    let mut buffer = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        if head.len() >= 16 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request head too large",
            ));
        }
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request ended before headers",
            ));
        }
        head.extend_from_slice(&buffer[..read]);
    }
    Ok(head)
}

fn parse_range(request: &[u8]) -> Option<(u64, u64)> {
    let request = std::str::from_utf8(request).ok()?;
    let value = request.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("range")
            .then_some(value.trim().strip_prefix("bytes=")?)
    })?;
    let (start, end) = value.trim().split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

fn run_http_range_benchmark(
    resources: &HttpProcessResources,
) -> Result<(), Box<dyn std::error::Error>> {
    let executable = env::current_exe()?;
    let mut child = ChildGuard::spawn(
        Command::new(executable)
            .arg("--range-server")
            .arg(ACTIVE_RANGES.to_string())
            .stdout(Stdio::piped())
            .stdin(Stdio::null()),
    )?;
    let stdout = child
        .child
        .stdout
        .take()
        .ok_or("range benchmark server stdout was not piped")?;
    let mut lines = BufReader::new(stdout).lines();
    let address: std::net::SocketAddr = lines
        .next()
        .ok_or("range benchmark server exited before publishing its address")??
        .parse()?;
    drop(lines);

    let transport = resources.transport_budgets();
    let ingress = resources.ingress_budgets();
    let resolver = HttpResolver::new(HttpResolverConfig::default())?;
    let client_config = HttpPolicyClientConfig {
        destination: HttpDestinationPolicy {
            allow_loopback: true,
            ..HttpDestinationPolicy::default()
        },
        direct: HttpDirectTransportConfig {
            keep_alive: false,
            max_connections_per_origin: 1,
            max_idle_connections_per_origin: 0,
            budgets: transport.clone(),
            ..HttpDirectTransportConfig::default()
        },
        direct_transport_cache_capacity: 0,
        ..HttpPolicyClientConfig::default()
    };
    let uri = format!("http://{address}/range");
    let barrier = Arc::new(Barrier::new(ACTIVE_RANGES + 1));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let started = Instant::now();
    let result = runtime.block_on(async {
        let mut workers = Vec::with_capacity(ACTIVE_RANGES);
        for index in 0..ACTIVE_RANGES {
            let resolver = resolver.clone();
            let config = client_config.clone();
            let uri = uri.clone();
            let ingress = ingress.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(tokio::spawn(async move {
                let mut failure = None;
                let permit = match ingress.try_acquire(ACTIVE_FRAME_BYTES) {
                    Ok(permit) => Some(permit),
                    Err(error) => {
                        failure = Some(format!("ingress admission failed: {error}"));
                        None
                    }
                };
                let mut response = None;
                if permit.is_some() {
                    let client = HttpPolicyClient::new(resolver, config);
                    let mut request = HttpClientRequest::get(uri);
                    request.range = Some(GlobalSpan {
                        offset: u64::try_from(index * ACTIVE_FRAME_BYTES)
                            .expect("range offset fits u64"),
                        len: ACTIVE_FRAME_BYTES,
                    });
                    match client.execute(request).await {
                        Ok(value) if value.status().as_u16() == 206 => {
                            let expected = format!(
                                "bytes {}-{}/{}",
                                index * ACTIVE_FRAME_BYTES,
                                (index + 1) * ACTIVE_FRAME_BYTES - 1,
                                RANGE_TOTAL_BYTES
                            );
                            let actual = value
                                .headers()
                                .get(CONTENT_RANGE)
                                .and_then(|header| header.to_str().ok())
                                .map(str::to_owned);
                            if actual.as_deref() == Some(expected.as_str()) {
                                response = Some(value);
                            } else {
                                failure = Some(format!(
                                    "unexpected Content-Range {actual:?}, expected {expected}"
                                ));
                                value.finish().await;
                            }
                        }
                        Ok(value) => {
                            failure = Some(format!("unexpected HTTP status {}", value.status()));
                            value.finish().await;
                        }
                        Err(error) => failure = Some(error.to_string()),
                    }
                }
                barrier.wait().await;
                let Some(mut response) = response else {
                    return Err(failure.unwrap_or_else(|| "range request failed".to_owned()));
                };
                let mut received = 0_usize;
                while let Some(data) = response
                    .next_data(Duration::from_secs(10))
                    .await
                    .map_err(|error| error.to_string())?
                {
                    received = received
                        .checked_add(data.len())
                        .ok_or_else(|| "received length overflow".to_owned())?;
                    if received > ACTIVE_FRAME_BYTES {
                        return Err("range body exceeded the requested span".to_owned());
                    }
                }
                response.finish().await;
                drop(permit);
                if received != ACTIVE_FRAME_BYTES {
                    return Err(format!(
                        "range body length {received} != {ACTIVE_FRAME_BYTES}"
                    ));
                }
                Ok::<usize, String>(received)
            }));
        }
        barrier.wait().await;
        let resident_at_barrier = resources.resident_budget().used();
        let sockets_at_barrier = resources
            .transport_budgets()
            .socket_limit()
            .saturating_sub(transport.available_sockets());
        let mut transferred = 0_usize;
        for worker in workers {
            transferred = transferred
                .checked_add(worker.await.map_err(|error| error.to_string())??)
                .ok_or_else(|| "transferred length overflow".to_owned())?;
        }
        Ok::<(usize, usize, usize), Box<dyn std::error::Error>>((
            transferred,
            resident_at_barrier,
            sockets_at_barrier,
        ))
    })?;
    let elapsed = started.elapsed();
    let status = child.child.wait()?;
    child.disarmed = true;
    if !status.success() {
        return Err(format!("range benchmark server exited with {status}").into());
    }
    let limits = resources.profile().limits();
    if result.0 != RANGE_TOTAL_BYTES {
        return Err(format!(
            "transferred {} bytes, expected {RANGE_TOTAL_BYTES}",
            result.0
        )
        .into());
    }
    if result.1 > limits.accounted_resident_limit_bytes {
        return Err(format!(
            "active HTTP resident reservation {} exceeds profile limit {}",
            result.1, limits.accounted_resident_limit_bytes
        )
        .into());
    }
    println!(
        "profile={} http_ranges={} http_bytes={} http_ms={} active_http_resident_bytes={} active_http_sockets={} accounted_limit_bytes={} rss_kib={:?}",
        resources.profile().requested().code(),
        ACTIVE_RANGES,
        result.0,
        elapsed.as_millis(),
        result.1,
        result.2,
        limits.accounted_resident_limit_bytes,
        resident_set_kib(),
    );
    if resources.resident_budget().used() != 0 {
        return Err("HTTP range benchmark leaked resident permits".into());
    }
    Ok(())
}

struct ChildGuard {
    child: Child,
    disarmed: bool,
}

impl ChildGuard {
    fn spawn(command: &mut Command) -> std::io::Result<Self> {
        Ok(Self {
            child: command.spawn()?,
            disarmed: false,
        })
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[cfg(target_os = "linux")]
fn resident_set_kib() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/self/status").ok()?;
    contents.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?.split_whitespace().next()?;
        value.parse().ok()
    })
}

#[cfg(not(target_os = "linux"))]
fn resident_set_kib() -> Option<u64> {
    None
}
