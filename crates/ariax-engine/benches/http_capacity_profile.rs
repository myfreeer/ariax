use ariax_engine::HttpProcessResources;
use ariax_runtime::{C10K_LOW_ACTIVITY_SOCKET_TARGET, RuntimeProfile};
use std::env;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const ACTIVE_RANGES: usize = 1_000;
const ACTIVE_FRAME_BYTES: usize = 64 * 1024;

fn main() {
    let mut arguments = env::args().skip(1);
    if arguments.next().as_deref() == Some("--server") {
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
    let address = lines
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
