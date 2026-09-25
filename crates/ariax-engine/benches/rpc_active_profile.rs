//! Short real-worker RPC bursts. Origin, engine and client have separate processes.
#![forbid(unsafe_code)]

use ariax_core::{MonotonicInstant, SchedulerConfig, TaskId};
use ariax_engine::*;
use ariax_runtime::RuntimeProfile;
use ariax_storage::{JournalStateLimits, ReplayLimits, SessionOwnerConfig};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use std::io::{self, Write as _};
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{
    AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

type Failure = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Failure>;
const RANGES: usize = 1_000;
const RANGES_PER_ORIGIN: usize = 8;
const SAMPLES: usize = 20_000;
const PROJECTION_TASKS: usize = 128;
const PROJECTION_SOURCES: usize = 32;
const BURST_LAUNCH_MS: u64 = 400;

#[path = "rpc_active_profile/admin.rs"]
mod admin;
const TOTAL_BYTES: usize = RANGES * 2 * 1024 * 1024;
const PULSE_BYTES: usize = 1024;
const EVENT_BYTES: usize = 512 * 1024;
static METRICS_CALLS: AtomicUsize = AtomicUsize::new(0);

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn phase5_metalink() -> bool {
    std::env::var("ARIAX_BENCH_METALINK").as_deref() == Ok("1")
}

// Keep process-pipe reads on the blocking pool, separate from the socket
// reactor, matching the standard-I/O adapter used by the engine.
#[cfg(unix)]
fn async_pipe(pipe: impl Into<std::os::fd::OwnedFd>) -> tokio::fs::File {
    tokio::fs::File::from_std(std::fs::File::from(pipe.into()))
}

#[cfg(windows)]
fn async_pipe(pipe: impl Into<std::os::windows::io::OwnedHandle>) -> tokio::fs::File {
    tokio::fs::File::from_std(std::fs::File::from(pipe.into()))
}

struct Child(std::process::Child);
impl std::ops::Deref for Child {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Child {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Child {
    async fn wait(&mut self) -> io::Result<ExitStatus> {
        loop {
            if let Some(status) = self.0.try_wait()? {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn kill(&mut self) -> io::Result<()> {
        self.0.kill()?;
        self.wait().await?;
        Ok(())
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        match args.first().map(String::as_str) {
            Some("--origin") => origin().await,
            Some("--administrative")
                if std::env::var_os("ARIAX_RUN_ACTIVE_RPC_BENCH").is_some() =>
            {
                tokio::time::timeout(Duration::from_secs(90), admin::measure())
                    .await
                    .map_err(|_| "administrative scenario exceeded its 90-second deadline")?
            }
            Some("--engine") => {
                let origins = args[1]
                    .split(',')
                    .map(str::parse)
                    .collect::<std::result::Result<Vec<SocketAddr>, _>>()?;
                engine(&origins, &args[2]).await
            }
            _ if std::env::var_os("ARIAX_RUN_ACTIVE_RPC_BENCH").is_none() => {
                println!(
                    "compiled; set ARIAX_RUN_ACTIVE_RPC_BENCH=1 for short real-worker RPC bursts"
                );
                Ok(())
            }
            _ => {
                let selected = args.iter().find_map(|arg| arg.strip_prefix("--scenario="));
                if selected.is_some_and(|name| {
                    !["http", "websocket", "content-length", "ndjson"].contains(&name)
                }) {
                    return Err("unknown scenario".into());
                }
                for scenario in ["http", "websocket", "content-length", "ndjson"] {
                    if selected.is_none_or(|name| name == scenario) {
                        tokio::time::timeout(Duration::from_secs(90), measure(scenario))
                            .await
                            .map_err(|_| format!("{scenario} exceeded its 90-second deadline"))??;
                    }
                }
                Ok(())
            }
        }
    });
    // Tokio's OS stdin read is a blocking task; all engine owners drain first.
    runtime.shutdown_timeout(Duration::from_millis(100));
    if let Err(error) = result {
        eprintln!("active RPC benchmark failed: {error}");
        std::process::exit(1);
    }
}

struct Root(PathBuf);
impl Root {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!("ariax-rpc-active-{}", std::process::id()));
        private_directory(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn private_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        std::fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(windows)]
    {
        ariax_windows_security::create_private_directory(path)
    }
}

fn rss_bytes() -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")?
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
            .map(|kib| kib * 1024)
            .ok_or_else(|| io::Error::other("missing RSS"))
    }
    #[cfg(windows)]
    {
        ariax_windows_security::current_process_working_set_bytes().map(|bytes| bytes as u64)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        Err(io::Error::other("native RSS instrumentation unavailable"))
    }
}

#[derive(Default)]
struct OriginState {
    active: AtomicUsize,
    acknowledgements: AtomicUsize,
}
struct ActiveRange(Arc<OriginState>);
impl Drop for ActiveRange {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn head(stream: &mut TcpStream) -> Result<String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(stream.read_u8().await?);
        if bytes.len() > 16384 {
            return Err("oversized fixture header".into());
        }
    }
    Ok(String::from_utf8(bytes)?)
}

async fn origin() -> Result<()> {
    let state = Arc::new(OriginState::default());
    let (pulse, _) = watch::channel(0_usize);
    let mut listeners = tokio::task::JoinSet::new();
    let mut addresses = Vec::new();
    for _ in 0..RANGES / RANGES_PER_ORIGIN {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        addresses.push(listener.local_addr()?.to_string());
        listeners.spawn(origin_listener(listener, state.clone(), pulse.clone()));
    }
    println!("{}", addresses.join(","));
    io::stdout().flush()?;
    listeners
        .join_next()
        .await
        .ok_or("missing origin listener")??
}

async fn origin_listener(
    listener: TcpListener,
    state: Arc<OriginState>,
    pulse: watch::Sender<usize>,
) -> Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        let pulse = pulse.clone();
        tasks.spawn(async move {
            let _ = origin_connection(stream, state, pulse).await;
        });
        while tasks.try_join_next().is_some() {}
    }
}

async fn origin_connection(
    mut stream: TcpStream,
    state: Arc<OriginState>,
    pulse: watch::Sender<usize>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let request = head(&mut stream).await?;
    if request.starts_with("GET /metrics ") || request.starts_with("GET /pulse ") {
        let first = METRICS_CALLS.fetch_add(1, Ordering::Relaxed) == 0;
        if first {
            eprintln!("first metrics query received");
        }
        if request.starts_with("GET /pulse ") {
            state.acknowledgements.store(0, Ordering::SeqCst);
            pulse.send_modify(|epoch| *epoch += 1);
        }
        let body = json!({"active":state.active.load(Ordering::SeqCst), "acks":state.acknowledgements.load(Ordering::SeqCst)}).to_string();
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
        if first {
            eprintln!("first metrics query answered");
        }
        return Ok(());
    }
    let (start, end) = request
        .lines()
        .find_map(|line| {
            let value = line
                .strip_prefix("range: bytes=")
                .or_else(|| line.strip_prefix("Range: bytes="))?;
            let (start, end) = value.split_once('-')?;
            Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
        })
        .ok_or("missing fixture range")?;
    let length = end
        .checked_sub(start)
        .and_then(|value| value.checked_add(1))
        .ok_or("invalid range")?;
    let mut epochs = pulse.subscribe();
    stream.write_all(format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {length}\r\nContent-Range: bytes {start}-{end}/{TOTAL_BYTES}\r\nETag: \"rpc-active-v1\"\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    if length == 1 {
        stream.write_all(&[0x5a]).await?;
        return Ok(());
    }
    stream.write_all(&[0x5a; PULSE_BYTES]).await?;
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    if active.is_multiple_of(250) {
        eprintln!("{active} active range responses");
    }
    let _active = ActiveRange(state.clone());
    let mut sent = PULSE_BYTES;
    let mut byte = [0];
    loop {
        tokio::select! {
            changed = epochs.changed() => {
                changed?;
                if sent + PULSE_BYTES >= length { return Err("benchmark exhausted a range".into()); }
                stream.write_all(&[0x5a; PULSE_BYTES]).await?;
                sent += PULSE_BYTES;
                state.acknowledgements.fetch_add(1, Ordering::SeqCst);
            }
            _ = stream.read(&mut byte) => return Ok(()),
        }
    }
}

struct BenchBackend {
    inner: Arc<HttpControlBackend>,
    resources: HttpProcessResources,
    stats: SharedHttpTransferStats,
    event: RpcEvent,
}
impl HttpRpcBackend for BenchBackend {
    fn call(&self, method: &str, params: Value) -> RpcFuture {
        self.call_with_context(method, params, RpcClientContext::default())
    }
    fn call_with_context(
        &self,
        method: &str,
        params: Value,
        context: RpcClientContext,
    ) -> RpcFuture {
        if method == "bench.events" {
            let broker = self.inner.event_broker();
            let event = self.event.clone();
            return Box::pin(async move {
                broker.publish(event);
                Ok(json!("OK"))
            });
        }
        if method != "bench.metrics" {
            return self.inner.call_with_context(method, params, context);
        }
        let resource = self.resources.clone();
        let control = self.inner.clone();
        let stats = self.stats.clone();
        Box::pin(async move {
            let first = METRICS_CALLS.fetch_add(1, Ordering::Relaxed) == 0;
            if first {
                eprintln!("benchmark setup: first metrics request dispatched");
            }
            let stats = stats
                .get(TaskId::new(1).unwrap())
                .map(|stats| stats.snapshot())
                .unwrap_or_default();
            let budget = resource.rpc_budgets().snapshot();
            let rss =
                rss_bytes().map_err(|error| HttpRpcBackendError::new(-32000, error.to_string()))?;
            if first {
                eprintln!("benchmark setup: first metrics response ready");
            }
            Ok(
                json!({"connections":stats.active_connections, "network":stats.network_phase,
                "received":stats.raw_body_bytes, "rss":rss, "resident":budget.resident_bytes,
                "residentLimit":budget.resident_limit, "rssLimit":resource.profile().limits().resident_target_bytes,
                "rpc":budget.bytes, "rpcLimit":budget.byte_limit, "items":budget.items,
                "controlRuntime":control.control_runtime_metrics()}),
            )
        })
    }
    fn rpc_budgets(&self) -> RpcBudgets {
        self.resources.rpc_budgets()
    }
}
impl RpcWebSocketBackend for BenchBackend {
    fn event_broker(&self) -> RpcEventBroker {
        self.inner.event_broker()
    }
}

async fn stopped(mut receiver: watch::Receiver<bool>) -> io::Result<()> {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
    Ok(())
}

fn build_control_plane(
    root: &Root,
    resources: &HttpProcessResources,
    capacity: usize,
) -> Result<(HttpControlPlane, PathBuf)> {
    let control = root.0.join("control");
    let output = root.0.join("output");
    let journals = control.join("http-journals");
    for path in [&control, &output, &journals] {
        private_directory(path)?;
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let config = ProcessBootstrapConfig {
        session_owner: SessionOwnerConfig::new(root.0.join("session.db")),
        control_directory: control,
        allowed_output_roots: vec![output.clone()],
        replay_limits: ReplayLimits::default(),
        journal_state_limits: JournalStateLimits::default(),
        recovery: StartupRecoveryConfig {
            scheduler: SchedulerConfig::new(nz(capacity), nz(1), true)?,
            now_wall_unix_ms: now,
            now_monotonic: MonotonicInstant::now(),
            max_retry_wait_ms: NonZeroU64::new(60000).unwrap(),
            max_slow_wait_ms: NonZeroU64::new(60000).unwrap(),
            max_no_space_wait_ms: NonZeroU64::new(60000).unwrap(),
            max_retry_elapsed_ms: 60000,
        },
        runtime: RuntimeEffectConfig {
            request_capacity: nz(64),
            event_capacity: nz(64),
            timer_capacity: nz(64),
            option_plan_capacity: nz(64),
        },
        persistence_plan_capacity: nz(64),
        shutdown_step_timeout_ms: DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS,
        updated_ms: now,
        recovery_created_at_unix_ms: now,
    };
    let mut plane = HttpControlPlane::new(
        bootstrap_process(config, ariax_config::persisted_option_is_safe)?,
        HttpControlPlaneConfig {
            output_root: output,
            journal_root: journals.clone(),
            task_capacity: nz(capacity),
            supervisor: HttpWorkerSupervisorConfig::default(),
        },
    )?;
    plane.attach_process_resources(resources.clone())?;
    Ok((plane, journals))
}

async fn engine(origins: &[SocketAddr], scenario: &str) -> Result<()> {
    eprintln!("benchmark setup: process bootstrap");
    let root = Root::new()?;
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency)?;
    let (mut plane, journals) = build_control_plane(&root, &resources, 256)?;
    let mut transport = resources.policy_client_config();
    transport.destination.allow_loopback = true;
    transport.direct.max_connections_per_origin = RANGES_PER_ORIGIN;
    transport.direct.max_idle_connections_per_origin = 0;
    let client =
        HttpPolicyClient::new(HttpResolver::new(HttpResolverConfig::default())?, transport);
    let mut worker = resources.worker_config(journals);
    worker.ingress_frame_bytes = nz(64 * 1024);
    plane.attach_global_download_rate(worker.download_rate.clone())?;
    let worker = HttpMultiRangeWorker::new(client, worker, plane.stats_catalog())?
        .with_session_owner(plane.session_handle());
    eprintln!("benchmark setup: range task admission");
    let sources: Vec<_> = origins
        .iter()
        .map(|origin| format!("http://{origin}/work.bin"))
        .collect();
    let options = json!({
        "split":RANGES, "max-connection-per-server":RANGES_PER_ORIGIN, "min-split-size":"1M", "piece-length":"1M", "timeout":600, "endgame-max-duplicates":0
    });
    let gid = if phase5_metalink() {
        if !cfg!(feature = "metalink") {
            return Err("Phase 5 benchmark requires the metalink feature".into());
        }
        use base64ct::Encoding;
        let chunk = 1024 * 1024;
        let mut hash = ContentHasher::new(ariax_storage::JournalDigestAlgorithm::Sha256);
        hash.update(&vec![0; chunk]);
        let checksum = hash.finalize().canonical();
        let digest = checksum.split_once('=').unwrap().1;
        let pieces = format!("<hash>{digest}</hash>").repeat(TOTAL_BYTES.div_ceil(chunk));
        let urls = sources
            .iter()
            .map(|uri| format!("<url>{uri}</url>"))
            .collect::<String>();
        let xml = format!(
            "<metalink xmlns='urn:ietf:params:xml:ns:metalink'><file name='work.bin'><size>{TOTAL_BYTES}</size><pieces type='sha-256' length='{chunk}'>{pieces}</pieces>{urls}</file></metalink>"
        );
        plane.call(
            "aria2.addMetalink",
            json!([base64ct::Base64::encode_string(xml.as_bytes()), options]),
        )?[0]
            .clone()
    } else {
        plane.call("aria2.addUri", json!([sources, options]))?
    };
    eprintln!("benchmark setup: stalled-consumer source admission");
    let sources: Vec<_> = (0..64)
        .map(|index| format!("http://example.test/{index}/{}", "x".repeat(8000)))
        .collect();
    let slow_gid = plane.call(
        "aria2.addUri",
        json!([sources, {"pause":true,"out":"stalled-consumer.bin"}]),
    )?;
    eprintln!("benchmark setup: query projection and ordinary control tasks");
    let projection_tasks: Vec<_> = (0..PROJECTION_TASKS)
        .map(|index| {
            json!({
                "kind":"transfer",
                "uris":[format!("http://example.test/projection/{index}.bin")],
                "options":{"pause":true,"out":format!("projection-{index}.bin")}
            })
        })
        .collect();
    plane.call(
        "ariax.importSession",
        json!([{"formatVersion":3,"tasks":projection_tasks}]),
    )?;
    let metadata_sources: Vec<_> = (0..PROJECTION_SOURCES)
        .map(|index| format!("http://example.test/metadata/{index}/{}", "m".repeat(2048)))
        .collect();
    let metadata_gid = plane.call(
        "aria2.addUri",
        json!([metadata_sources, {"pause":true,"out":"metadata.bin"}]),
    )?;
    let auxiliary_gid = plane.call(
        "aria2.addUri",
        json!([["http://example.test/auxiliary.bin"], {"pause":true,"out":"auxiliary.bin"}]),
    )?;
    plane.attach_worker(Arc::new(worker))?;
    eprintln!("benchmark setup: transport listeners");
    let stats = plane.stats_catalog();
    let backend = Arc::new(HttpControlBackend::new(plane));
    let metrics = Arc::new(BenchBackend {
        inner: backend.clone(),
        resources,
        stats,
        event: RpcEvent::notification(
            "bench.onSample",
            json!({"padding":"x".repeat(EVENT_BYTES)}),
            RpcEventClass::Coalesced,
            Some(RpcEventKey::new(None, "benchmark")),
        )?,
    });
    let dispatcher = Arc::new(
        RpcDispatcher::new(metrics.clone(), RpcAuthPolicy::default())
            .with_compatibility(RpcCompatibility::Extended),
    );
    let http = TcpListener::bind("127.0.0.1:0").await?;
    let websocket = TcpListener::bind("127.0.0.1:0").await?;
    let slow_stdio = TcpListener::bind("127.0.0.1:0").await?;
    let info = json!({"http":http.local_addr()?.to_string(),"websocket":websocket.local_addr()?.to_string(),"slowStdio":slow_stdio.local_addr()?.to_string(),"gid":gid,"slowGid":slow_gid,"metadataGid":metadata_gid,"auxiliaryGid":auxiliary_gid,"projectionTasks":PROJECTION_TASKS,"metadataSources":PROJECTION_SOURCES});
    let (stop, _) = watch::channel(false);
    let mut transports = tokio::task::JoinSet::new();
    transports.spawn(serve_loopback_http_listener_until(
        http,
        dispatcher.clone(),
        stopped(stop.subscribe()),
    ));
    transports.spawn(serve_loopback_websocket_listener_until(
        websocket,
        dispatcher.clone(),
        stopped(stop.subscribe()),
    ));
    if matches!(scenario, "content-length" | "ndjson") {
        let options = RpcStdioOptions {
            framing: if scenario == "ndjson" {
                RpcStdioFraming::Ndjson
            } else {
                RpcStdioFraming::ContentLength
            },
            events: false,
            ..RpcStdioOptions::default()
        };
        transports.spawn(run_stdio_until(
            dispatcher.clone(),
            tokio::io::stdin(),
            tokio::io::stdout(),
            options,
            stopped(stop.subscribe()),
        ));
        let dispatcher = dispatcher.clone();
        let receiver = stop.subscribe();
        transports.spawn(async move {
            tokio::select! {
                accepted = slow_stdio.accept() => {
                    let (stream, _) = accepted?;
                    let (read, write) = stream.into_split();
                    match run_stdio_until(dispatcher, read, write, options, stopped(receiver)).await {
                        // The fixture deliberately closes a socket with unread replies.
                        // Treat the resulting peer abort like a listener connection ending.
                        Err(HttpRpcTransportError::Io(error))
                            if matches!(error.kind(), io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe) => Ok(()),
                        result => result,
                    }
                }
                _ = stopped(receiver.clone()) => Ok(()),
            }
        });
    }
    backend.start_control_runtime()?;
    let mut failure = backend.control_failure_receiver();
    eprintln!("{info}");
    let mut shutdown = backend.shutdown_receiver();
    tokio::select! {
        _ = shutdown.changed() => {},
        result = failure.changed() => { result?; return Err(failure.borrow().clone().unwrap_or_else(|| "engine progress ended unexpectedly".to_owned()).into()); }
    }
    stop.send_replace(true);
    while let Some(result) = transports.join_next().await {
        result??;
    }
    backend.drain_control_runtime().await?;
    drop(dispatcher);
    drop(metrics);
    let backend = Arc::try_unwrap(backend).map_err(|_| "benchmark retained backend")?;
    let plane = backend
        .try_into_control_plane()
        .map_err(|_| "benchmark retained plane")?;
    if !plane.shutdown_async().await?.is_clean() {
        return Err("unclean benchmark shutdown".into());
    }
    Ok(())
}

enum Wire {
    Http,
    Framed(bool),
}
enum Client {
    Stream {
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        wire: Wire,
    },
    Websocket(Box<WebSocketStream<MaybeTlsStream<TcpStream>>>),
}
impl Client {
    fn pipes(
        read: impl AsyncRead + Unpin + Send + 'static,
        write: impl AsyncWrite + Unpin + Send + 'static,
        wire: Wire,
    ) -> Self {
        Self::Stream {
            reader: BufReader::new(Box::new(read)),
            writer: Box::new(write),
            wire,
        }
    }
    async fn connect(address: SocketAddr, scenario: &str) -> Result<Self> {
        tokio::time::timeout(
            Duration::from_secs(5),
            Self::connect_inner(address, scenario),
        )
        .await?
    }
    async fn connect_inner(address: SocketAddr, scenario: &str) -> Result<Self> {
        if scenario == "websocket" {
            return Ok(Self::Websocket(Box::new(
                tokio_tungstenite::connect_async(format!("ws://{address}/jsonrpc"))
                    .await?
                    .0,
            )));
        }
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (read, write) = stream.into_split();
        Ok(Self::pipes(
            read,
            write,
            if scenario == "http" {
                Wire::Http
            } else {
                Wire::Framed(scenario == "ndjson")
            },
        ))
    }
    async fn send(&mut self, request: &[u8]) -> Result<()> {
        match self {
            Self::Websocket(socket) => {
                socket
                    .send(Message::Text(String::from_utf8(request.to_vec())?.into()))
                    .await?
            }
            Self::Stream { writer, wire, .. } => {
                match wire {
                    Wire::Http => writer.write_all(format!("POST /jsonrpc HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", request.len()).as_bytes()).await?,
                    Wire::Framed(false) => writer.write_all(format!("Content-Length: {}\r\n\r\n", request.len()).as_bytes()).await?,
                    Wire::Framed(true) => {},
                }
                writer.write_all(request).await?;
                if matches!(wire, Wire::Framed(true)) {
                    writer.write_all(b"\n").await?;
                }
                writer.flush().await?;
            }
        }
        Ok(())
    }
    async fn receive(&mut self) -> Result<Value> {
        loop {
            let value: Value = match self {
                Self::Websocket(socket) => {
                    let message = socket.next().await.ok_or("websocket EOF")??;
                    if !message.is_text() && !message.is_binary() {
                        continue;
                    }
                    serde_json::from_slice(&message.into_data())?
                }
                Self::Stream {
                    reader,
                    wire: Wire::Framed(true),
                    ..
                } => {
                    let mut line = String::new();
                    reader.read_line(&mut line).await?;
                    if line.len() > MAX_HTTP_RPC_RESPONSE_BYTES {
                        return Err("oversized NDJSON response".into());
                    }
                    serde_json::from_str(&line)?
                }
                Self::Stream { reader, .. } => {
                    let mut length = None;
                    let mut total = 0;
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).await? == 0 {
                            return Err("framed response EOF".into());
                        }
                        total += line.len();
                        if total > MAX_HTTP_RPC_HEADER_BYTES {
                            return Err("oversized response header".into());
                        }
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((key, value)) = line.split_once(':')
                            && key.eq_ignore_ascii_case("content-length")
                        {
                            length = Some(value.trim().parse::<usize>()?);
                        }
                    }
                    let length = length
                        .filter(|length| *length <= MAX_HTTP_RPC_RESPONSE_BYTES)
                        .ok_or("missing bounded response length")?;
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).await?;
                    serde_json::from_slice(&body)?
                }
            };
            if value.get("id").is_none() {
                continue;
            }
            if value.get("error").is_some() {
                return Err(format!("RPC rejection: {value}").into());
            }
            return value
                .get("result")
                .cloned()
                .ok_or_else(|| "missing RPC result".into());
        }
    }
    async fn call(&mut self, request: &[u8]) -> Result<Value> {
        let mut phase = "send";
        tokio::time::timeout(Duration::from_secs(10), async {
            self.send(request).await?;
            phase = "receive";
            self.receive().await
        })
        .await
        .map_err(|_| {
            format!(
                "RPC timed out during {phase}: {}",
                String::from_utf8_lossy(request)
            )
        })?
    }
}
fn request(method: &str, params: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params})).unwrap()
}

fn latency_report(samples: &mut std::collections::BTreeMap<&str, Vec<Duration>>) -> Value {
    samples.iter_mut().map(|(method, samples)| {
        samples.sort_unstable();
        ((*method).to_owned(), json!({"calls":samples.len(), "p50Us":samples[samples.len()/2].as_micros(),
            "p99Us":samples[(samples.len()*99).div_ceil(100)-1].as_micros(), "maxUs":samples.last().unwrap().as_micros()}))
    }).collect::<serde_json::Map<_, _>>().into()
}

struct Auxiliary {
    gid: Value,
    uri: String,
    cycle: usize,
    phase: usize,
}

impl Auxiliary {
    fn request(&self) -> (&'static str, Vec<u8>) {
        let (method, params) = match self.phase {
            0 => ("unpause", json!([self.gid])),
            1 => ("pause", json!([self.gid])),
            2 => ("changeOption", json!([self.gid, {"split":3}])),
            3 => ("changePosition", json!([self.gid, 0, "POS_SET"])),
            4 => (
                "changeUri",
                json!([
                    self.gid,
                    1,
                    [self.uri],
                    [format!("http://example.test/changed-{}.bin", self.cycle)]
                ]),
            ),
            5 => ("remove", json!([self.gid])),
            6 => ("removeDownloadResult", json!([self.gid])),
            _ => (
                "addUri",
                json!([["http://example.test/auxiliary.bin"], {"pause":true,"out":"auxiliary.bin"}]),
            ),
        };
        (method, request(&format!("aria2.{method}"), params))
    }

    async fn verify(&mut self, client: &mut Client, result: Value) -> Result<()> {
        match self.phase {
            0 | 1 | 5 => {
                if result != self.gid {
                    return Err("control GID mismatch".into());
                }
            }
            2 | 6 => {
                if result != "OK" {
                    return Err("control acknowledgement mismatch".into());
                }
            }
            3 => {
                if result != 0 {
                    return Err("queue move did not reach position zero".into());
                }
            }
            4 => {
                if result != json!([1, 1]) {
                    return Err("source change was not applied".into());
                }
            }
            _ => {
                if !result.is_string() || result == self.gid {
                    return Err("admission did not create a fresh GID".into());
                }
                self.gid = result;
                self.uri = "http://example.test/auxiliary.bin".to_owned();
            }
        }
        let verification = match self.phase {
            2 => request("aria2.getOption", json!([self.gid])),
            3 => request("aria2.tellWaiting", json!([0, 1, ["gid"]])),
            4 => request("aria2.getUris", json!([self.gid])),
            6 => request("aria2.tellStopped", json!([0, 1000, ["gid"]])),
            _ => request("aria2.tellStatus", json!([self.gid, ["gid", "status"]])),
        };
        let state = client.call(&verification).await?;
        let valid = match self.phase {
            0 => state["status"] == "waiting",
            1 | 7 => state["status"] == "paused",
            2 => state["split"] == "3",
            3 => state[0]["gid"] == self.gid,
            4 => {
                self.uri = format!("http://example.test/changed-{}.bin", self.cycle);
                state[0]["uri"] == self.uri
            }
            5 => state["status"] == "removed",
            6 => state.as_array().is_some_and(Vec::is_empty),
            _ => false,
        };
        if !valid {
            return Err(format!(
                "mutation phase {} failed its state check: {state}",
                self.phase
            )
            .into());
        }
        self.phase = (self.phase + 1) % 8;
        if self.phase == 0 {
            self.cycle += 1;
        }
        Ok(())
    }
}

async fn origin_metrics(address: SocketAddr, pulse: bool) -> Result<Value> {
    let mut phase = "connect";
    tokio::time::timeout(
        Duration::from_secs(5),
        origin_metrics_inner(address, pulse, &mut phase),
    )
    .await
    .map_err(|_| format!("origin metrics {phase} exceeded five seconds"))?
}

async fn origin_metrics_inner(
    address: SocketAddr,
    pulse: bool,
    phase: &mut &'static str,
) -> Result<Value> {
    let mut stream = TcpStream::connect(address).await?;
    *phase = "write";
    stream
        .write_all(if pulse {
            b"GET /pulse HTTP/1.1\r\nHost: localhost\r\n\r\n"
        } else {
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"
        })
        .await?;
    *phase = "read";
    let mut response = Vec::new();
    stream.take(16385).read_to_end(&mut response).await?;
    if response.len() > 16384 {
        return Err("oversized origin metrics response".into());
    }
    let body = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .map(|offset| &response[offset + 4..])
        .ok_or("missing origin response header")?;
    Ok(serde_json::from_slice(body)?)
}

async fn barrier(origin: SocketAddr, client: &mut Client, renew: bool) -> Result<Value> {
    let metric_request = request("bench.metrics", json!([]));
    let before = client.call(&metric_request).await?;
    if !renew && before["connections"] != RANGES {
        eprintln!("benchmark barrier: waiting for range startup: {before}");
    }
    let expected = before["received"].as_u64().unwrap_or(0)
        + if renew {
            (RANGES * PULSE_BYTES) as u64
        } else {
            0
        };
    if renew {
        origin_metrics(origin, true).await?;
    }
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let metrics = client.call(&metric_request).await?;
        let remote = origin_metrics(origin, false).await?;
        if remote["active"] == RANGES
            && metrics["connections"] == RANGES
            && metrics["network"] == true
            && (!renew
                || (remote["acks"] == RANGES
                    && metrics["received"].as_u64().unwrap_or(0) >= expected))
        {
            return Ok(metrics);
        }
        if Instant::now() >= deadline {
            return Err(format!("range barrier failed: origin={remote} engine={metrics}").into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn spawn(args: &[&str]) -> Result<Child> {
    Ok(Child(
        Command::new(std::env::current_exe()?)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    ))
}

async fn measure(scenario: &str) -> Result<()> {
    let scenario_started = Instant::now();
    if ariax_runtime::native_process_handle_limit()
        .is_some_and(|limit| limit < RANGES + RANGES / RANGES_PER_ORIGIN + 128)
    {
        return Err("benchmark requires a larger process handle limit; use an isolated Linux shell with ulimit -n 20000".into());
    }
    let mut origin_child = spawn(&["--origin"])?;
    let mut origin_stdout = BufReader::new(async_pipe(
        origin_child.stdout.take().ok_or("origin stdout")?,
    ));
    let mut origin_errors = BufReader::new(async_pipe(
        origin_child.stderr.take().ok_or("origin stderr")?,
    ));
    let origin_errors = tokio::spawn(async move {
        loop {
            let mut line = String::new();
            match origin_errors.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => eprint!("origin: {line}"),
            }
        }
    });
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(10), origin_stdout.read_line(&mut line)).await??;
    let origin: SocketAddr = line
        .trim()
        .split(',')
        .next()
        .ok_or("missing origin address")?
        .parse()?;
    eprintln!("benchmark {scenario}: origin listening on {origin}");
    let remote = origin_metrics(origin, false).await?;
    eprintln!("benchmark {scenario}: origin ready: {remote}");
    let mut child = spawn(&["--engine", line.trim(), scenario])?;
    let mut stderr = BufReader::new(async_pipe(child.stderr.take().ok_or("engine stderr")?));
    let line = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let mut line = String::new();
            if stderr.read_line(&mut line).await? == 0 {
                return Err(io::Error::other("engine exited during setup"));
            }
            if line.starts_with('{') {
                return Ok(line);
            }
            eprint!("{line}");
        }
    })
    .await??;
    let info: Value =
        serde_json::from_str(&line).map_err(|error| format!("engine setup: {line}: {error}"))?;
    eprintln!("benchmark {scenario}: listener metadata received");
    let errors = tokio::spawn(async move {
        let mut text = String::new();
        loop {
            let mut line = String::new();
            match stderr.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    eprint!("{line}");
                    text.push_str(&line);
                }
            }
        }
        text
    });
    let address = |field| -> Result<SocketAddr> {
        Ok(info[field].as_str().ok_or("listener metadata")?.parse()?)
    };
    let mut client = if matches!(scenario, "content-length" | "ndjson") {
        Client::pipes(
            async_pipe(child.stdout.take().ok_or("engine stdout")?),
            async_pipe(child.stdin.take().ok_or("engine stdin")?),
            Wire::Framed(scenario == "ndjson"),
        )
    } else {
        Client::connect(address(scenario)?, scenario).await?
    };
    let status = request(
        "aria2.tellStatus",
        json!([
            info["gid"],
            ["gid", "status", "completedLength", "connections"]
        ]),
    );
    let list = request(
        "aria2.tellWaiting",
        json!([
            0,
            PROJECTION_TASKS,
            [
                "gid",
                "status",
                "totalLength",
                "completedLength",
                "downloadSpeed",
                "connections",
                "verifiedLength",
                "retryCount"
            ]
        ]),
    );
    let files = request("aria2.getFiles", json!([info["metadataGid"]]));
    let uris = request("aria2.getUris", json!([info["metadataGid"]]));
    let options = request("aria2.getOption", json!([info["metadataGid"]]));
    let mut auxiliary = Auxiliary {
        gid: info["auxiliaryGid"].clone(),
        uri: "http://example.test/auxiliary.bin".to_owned(),
        cycle: 0,
        phase: 0,
    };
    let no_fixture_events = request(
        "ariax.setEventFilter",
        json!([{"methods":["aria2.onDownloadError"]}]),
    );
    if scenario == "websocket" {
        client.call(&no_fixture_events).await?;
    }
    eprintln!("benchmark {scenario}: entering initial active-range barrier");
    barrier(origin, &mut client, false).await?;
    eprintln!("benchmark {scenario}: 1,000 active HTTP ranges confirmed");
    let before = client.call(&request("bench.metrics", json!([]))).await?;
    let mut slow_events = Client::connect(address("websocket")?, "websocket").await?;
    slow_events
        .call(&request(
            "ariax.setEventFilter",
            json!([{"methods":["bench.onSample"]}]),
        ))
        .await?;
    let refresh_event = request("bench.events", json!([]));
    for _ in 0..32 {
        client.call(&refresh_event).await?;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let event_retained = client.call(&request("bench.metrics", json!([]))).await?;
    if event_retained["rpc"].as_u64().unwrap() < before["rpc"].as_u64().unwrap() + 256 * 1024 {
        return Err(format!("stalled event consumer did not retain credit: before={before} retained={event_retained}").into());
    }
    let slow_address = if matches!(scenario, "content-length" | "ndjson") {
        address("slowStdio")?
    } else {
        address(scenario)?
    };
    let mut slow = Client::connect(slow_address, scenario).await?;
    if scenario == "websocket" {
        slow.call(&no_fixture_events).await?;
    }
    let large = request("aria2.getUris", json!([info["slowGid"]]));
    for _ in 0..64 {
        tokio::time::timeout(Duration::from_secs(5), slow.send(&large)).await??;
    }
    eprintln!("benchmark {scenario}: stalled-consumer requests sent");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let retained = client.call(&request("bench.metrics", json!([]))).await?;
    if retained["rpc"].as_u64().unwrap() < event_retained["rpc"].as_u64().unwrap() + 256 * 1024 {
        return Err(format!(
            "stalled consumer did not retain response credit: before={event_retained} retained={retained}"
        )
        .into());
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut bursts = 0;
    let mut controls = 0;
    let mut verification_calls = 0;
    let mut per_operation = std::collections::BTreeMap::<&str, Vec<Duration>>::new();
    let mut response_bytes = std::collections::BTreeMap::<&str, usize>::new();
    let mut max_burst_calls = 0;
    let mut max_burst = Duration::ZERO;
    let mut measured_bursts = Duration::ZERO;
    let mut max_rss = 0;
    let mut max_rpc = 0;
    let mut max_resident = 0;
    let mut observe = |metrics: &Value| -> Result<()> {
        for (value, limit) in [
            ("rss", "rssLimit"),
            ("rpc", "rpcLimit"),
            ("resident", "residentLimit"),
        ] {
            if metrics[value].as_u64().ok_or("metric")? > metrics[limit].as_u64().ok_or("limit")? {
                return Err(format!("{value} exceeded {limit}: {metrics}").into());
            }
        }
        max_rss = max_rss.max(metrics["rss"].as_u64().unwrap());
        max_rpc = max_rpc.max(metrics["rpc"].as_u64().unwrap());
        max_resident = max_resident.max(metrics["resident"].as_u64().unwrap());
        Ok(())
    };
    observe(&retained)?;
    while samples.len() < SAMPLES {
        client.call(&refresh_event).await?;
        for _ in 0..32 {
            client.call(&status).await?;
        }
        observe(&barrier(origin, &mut client, true).await?)?;
        let start = Instant::now();
        let mut count = 0;
        while samples.len() < SAMPLES
            && count < 1_000
            && start.elapsed() < Duration::from_millis(BURST_LAUNCH_MS)
        {
            let index = samples.len() % 20;
            let (method, payload) = match index {
                0..=11 => ("tellStatus", status.clone()),
                12..=15 => ("tellWaiting", list.clone()),
                16 => ("getFiles", files.clone()),
                17 => ("getUris", uris.clone()),
                18 => ("getOption", options.clone()),
                _ => auxiliary.request(),
            };
            // A verification call accompanies each real mutation and counts
            // against the same 1,000-call burst limit.
            if count + if index == 19 { 2 } else { 1 } > 1_000 {
                break;
            }
            let sent = Instant::now();
            let result = client.call(&payload).await?;
            let elapsed = sent.elapsed();
            samples.push(elapsed);
            per_operation.entry(method).or_default().push(elapsed);
            if let std::collections::btree_map::Entry::Vacant(entry) = response_bytes.entry(method)
            {
                entry.insert(serde_json::to_vec(&result)?.len());
            }
            count += 1;
            match index {
                0..=11 => {
                    if result["status"] != "active" || result["connections"] != "1000" {
                        return Err(format!("download left active state: {result}").into());
                    }
                }
                12..=15 => {
                    if result.as_array().map(Vec::len) != Some(PROJECTION_TASKS) {
                        return Err("list projection cardinality changed".into());
                    }
                }
                16 => {
                    if result[0]["uris"].as_array().map(Vec::len) != Some(PROJECTION_SOURCES) {
                        return Err("file projection cardinality changed".into());
                    }
                }
                17 => {
                    if result.as_array().map(Vec::len) != Some(PROJECTION_SOURCES) {
                        return Err("URI projection cardinality changed".into());
                    }
                }
                18 => {
                    if !result.is_object() {
                        return Err("option projection shape changed".into());
                    }
                }
                _ => {
                    auxiliary.verify(&mut client, result).await?;
                    controls += 1;
                    verification_calls += 1;
                    count += 1;
                }
            }
        }
        let elapsed = start.elapsed();
        if elapsed > Duration::from_millis(500) {
            return Err(format!(
                "{scenario} burst exceeded 500 ms: {} us",
                elapsed.as_micros()
            )
            .into());
        }
        max_burst_calls = max_burst_calls.max(count);
        max_burst = max_burst.max(elapsed);
        measured_bursts += elapsed;
        observe(&barrier(origin, &mut client, false).await?)?;
        bursts += 1;
        if bursts % 5 == 0 {
            eprintln!(
                "benchmark {scenario}: {} measured calls in {bursts} bursts",
                samples.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("benchmark {scenario}: samples complete, draining stalled consumer");
    let held = client.call(&request("bench.metrics", json!([]))).await?;
    drop(slow);
    let deadline = Instant::now() + Duration::from_secs(6);
    let released = loop {
        let metrics = client.call(&request("bench.metrics", json!([]))).await?;
        if metrics["rpc"].as_u64().unwrap() + 128 * 1024 < held["rpc"].as_u64().unwrap() {
            break metrics;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "stalled writer credit did not release: held={held} current={metrics}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    drop(slow_events);
    let deadline = Instant::now() + Duration::from_secs(6);
    let events_released = loop {
        let metrics = client.call(&request("bench.metrics", json!([]))).await?;
        if metrics["rpc"].as_u64().unwrap() + 256 * 1024 < released["rpc"].as_u64().unwrap() {
            break metrics;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "stalled event credit did not release: held={released} current={metrics}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    samples.sort_unstable();
    let p99 = samples[(SAMPLES * 99 / 100) - 1];
    let operations = latency_report(&mut per_operation);
    let ordinary_pass = per_operation.values().all(|samples| {
        samples[(samples.len() * 99).div_ceil(100) - 1] <= Duration::from_millis(50)
    });
    if events_released["controlRuntime"]["maxSteps"]
        .as_u64()
        .ok_or("owner step metric")?
        > 32
    {
        return Err("owner exceeded its step budget".into());
    }
    let measured_round_trips: Duration = samples.iter().copied().sum();
    let mut report = json!({"scenario":scenario,"profile":"concurrency","rangeAdmission":if phase5_metalink() {"metalink"} else {"addUri"},"ranges":RANGES,"samples":samples.len(),"controlCalls":controls,"bursts":bursts,"cooldownMs":250,
        "os":std::env::consts::OS,"arch":std::env::consts::ARCH,"origins":RANGES / RANGES_PER_ORIGIN,"workerThreads":2,
        "burstLimitCalls":1000,"burstLimitMs":500,"launchCutoffMs":BURST_LAUNCH_MS,"maxBurstCalls":max_burst_calls,"verificationCalls":verification_calls,"operations":operations,"firstResponseBytes":response_bytes,
        "projectionTasks":PROJECTION_TASKS,"metadataSources":PROJECTION_SOURCES,"auxiliaryMutationTargets":1,"controlRuntime":events_released["controlRuntime"],"measuredBurstUs":measured_bursts.as_micros(),"measuredRoundTripUs":measured_round_trips.as_micros(),
        "p50Us":samples[SAMPLES / 2].as_micros(),"p99Us":p99.as_micros(),"maxBurstMs":max_burst.as_millis(),
        "maxSampledRssBytes":max_rss,"maxRpcBytes":max_rpc,"maxResidentBytes":max_resident,
        "rpcLimit":retained["rpcLimit"],"residentLimit":retained["residentLimit"],"rssLimit":retained["rssLimit"],
        "stalledCreditBytes":held["rpc"].as_u64().unwrap().saturating_sub(released["rpc"].as_u64().unwrap()),
        "stalledEventCreditBytes":released["rpc"].as_u64().unwrap().saturating_sub(events_released["rpc"].as_u64().unwrap()),
        "stalledEvents":"WebSocket; coalesced 512 KiB fixture notifications through production broker",
        "stdioStalledWriter":"loopback socket; measured stdio uses OS pipes","renewedBarrierAfterWarmup":true,"perStatusRangeCheck":true});
    let shutdown_started = Instant::now();
    client.call(&request("aria2.shutdown", json!([]))).await?;
    report["shutdownAcknowledgementUs"] = json!(shutdown_started.elapsed().as_micros());
    eprintln!("benchmark {scenario}: shutdown acknowledged");
    drop(client);
    let status = tokio::time::timeout(Duration::from_secs(15), child.wait()).await??;
    report["shutdownDrainUs"] = json!(shutdown_started.elapsed().as_micros());
    report["shutdownDrainBoundary"] = json!("engine process exit");
    let stderr = tokio::time::timeout(Duration::from_secs(5), errors).await??;
    if !status.success() {
        return Err(format!("engine shutdown {status}: {stderr}").into());
    }
    let cleanup_started = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), origin_child.kill()).await??;
    tokio::time::timeout(Duration::from_secs(5), origin_errors).await??;
    report["fixtureCleanupUs"] = json!(cleanup_started.elapsed().as_micros());
    report["elapsedScenarioMs"] = json!(scenario_started.elapsed().as_millis());
    report["complete"] = json!(samples.len() == SAMPLES);
    report["passed"] = json!(ordinary_pass && p99 <= Duration::from_millis(50));
    println!("{report}");
    if !ordinary_pass || p99 > Duration::from_millis(50) {
        return Err("p99 exceeded 50 ms".into());
    }
    Ok(())
}
