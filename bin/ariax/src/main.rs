#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ariax_config::{SecurityClass, builtin_registry};
use ariax_core::{Generation, Gid, MonotonicInstant, SchedulerConfig, TaskId};
use ariax_engine::{
    HttpCancellation, HttpControlBackend, HttpControlPlane, HttpControlPlaneConfig, HttpCookieJar,
    HttpCookieLimits, HttpDestinationPolicy, HttpMultiRangeWorker, HttpPolicyClient,
    HttpProcessResources, HttpResolver, HttpResolverConfig, KnownLengthHttpRecoveryRequest,
    KnownLengthHttpRequest, KnownLengthHttpResumeRequest, ProcessBootstrapConfig,
    RuntimeEffectConfig, StartupRecoveryConfig, StorageEngineConfig,
    download_known_length_http_blocking, resume_known_length_http_blocking,
    run_content_length_stdio, serve_loopback_http_until,
};
use ariax_runtime::RuntimeProfile;
use ariax_storage::{
    JournalId, JournalStateLimits, PathPlatform, ReplayLimits, SafePathBuilder, SessionOwnerConfig,
};

const DEFAULT_HTTP_PIECE_LENGTH: u64 = 1024 * 1024;
const HELP: &str = "ariax — experimental bounded downloader\n\nUsage: ariax [--help|--version]\n       ariax --check-bootstrap SESSION_DB CONTROL_DIR [OUTPUT_ROOT ...]\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --rpc-http SESSION_DB CONTROL_DIR OUTPUT_ROOT LOOPBACK_ADDR\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --rpc-stdio SESSION_DB CONTROL_DIR OUTPUT_ROOT\n       ariax --download-http-pinned GID JOURNAL_ID URI PEER OUTPUT_ROOT OUTPUT_PATH JOURNAL_DIR [PIECE_LENGTH]\n       ariax --resume-http-pinned GID JOURNAL_ID URI PEER OUTPUT_ROOT JOURNAL_DIR\n\nRPC is JSON-RPC 2.0 over loopback HTTP/1.1 or Content-Length-framed stdio. The pinned HTTP commands accept an already policy-approved numeric PEER (IP:port); they do not perform DNS or SSRF-policy resolution.\n";

fn main() -> ExitCode {
    run(env::args_os().skip(1))
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> ExitCode {
    let arguments: Vec<_> = arguments.into_iter().collect();
    let (profile, arguments) = match split_runtime_profile_argument(&arguments) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("ariax: {error}");
            return ExitCode::from(2);
        }
    };
    if profile.is_some()
        && !matches!(
            arguments.first(),
            Some(command) if command == "--rpc-http" || command == "--rpc-stdio"
        )
    {
        eprintln!("ariax: --profile is accepted only with --rpc-http or --rpc-stdio");
        return ExitCode::from(2);
    }
    match arguments {
        [] => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        [arg] if arg == "--help" || arg == "-h" => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        [arg] if arg == "--version" || arg == "-V" => {
            println!("{} {}", ariax_core::ENGINE_NAME, env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [command, database, control, roots @ ..] if command == "--check-bootstrap" => {
            check_bootstrap(
                PathBuf::from(database),
                PathBuf::from(control),
                roots.iter().map(PathBuf::from).collect(),
            )
        }
        [command, database, control, output_root, bind] if command == "--rpc-http" => {
            let bind = match SocketAddr::from_str(&bind.to_string_lossy()) {
                Ok(bind) => bind,
                Err(error) => {
                    eprintln!("ariax: invalid RPC bind address: {error}");
                    return ExitCode::from(2);
                }
            };
            run_rpc(
                PathBuf::from(database),
                PathBuf::from(control),
                PathBuf::from(output_root),
                Some(bind),
                profile.unwrap_or_default(),
            )
        }
        [command, database, control, output_root] if command == "--rpc-stdio" => run_rpc(
            PathBuf::from(database),
            PathBuf::from(control),
            PathBuf::from(output_root),
            None,
            profile.unwrap_or_default(),
        ),
        [
            command,
            gid,
            journal_id,
            uri,
            peer,
            output_root,
            output,
            journal_directory,
        ] if command == "--download-http-pinned" => download_http_pinned(
            gid,
            journal_id,
            uri,
            peer,
            output_root,
            output,
            journal_directory,
            None,
        ),
        [
            command,
            gid,
            journal_id,
            uri,
            peer,
            output_root,
            output,
            journal_directory,
            piece_length,
        ] if command == "--download-http-pinned" => download_http_pinned(
            gid,
            journal_id,
            uri,
            peer,
            output_root,
            output,
            journal_directory,
            Some(piece_length),
        ),
        [
            command,
            gid,
            journal_id,
            uri,
            peer,
            output_root,
            journal_directory,
        ] if command == "--resume-http-pinned" => {
            resume_http_pinned(gid, journal_id, uri, peer, output_root, journal_directory)
        }
        [arg] => {
            eprintln!("ariax: unknown argument: {}", arg.to_string_lossy());
            ExitCode::from(2)
        }
        _ => {
            eprintln!(
                "ariax: only one ordinary argument or the exact --check-bootstrap form is accepted"
            );
            ExitCode::from(2)
        }
    }
}

fn split_runtime_profile_argument(
    arguments: &[OsString],
) -> Result<(Option<RuntimeProfile>, &[OsString]), String> {
    let Some(first) = arguments.first() else {
        return Ok((None, arguments));
    };
    let Some(first) = first.to_str() else {
        return Ok((None, arguments));
    };
    let Some(value) = first.strip_prefix("--profile=") else {
        if first == "--profile" {
            return Err(
                "--profile requires =auto|concurrency|throughput|latency|compact".to_owned(),
            );
        }
        return Ok((None, arguments));
    };
    let profile = RuntimeProfile::parse(value).map_err(|_| {
        format!(
            "invalid runtime profile {value:?}; expected auto, concurrency, throughput, latency, or compact"
        )
    })?;
    Ok((Some(profile), &arguments[1..]))
}

fn resume_http_pinned(
    gid: &OsString,
    journal_id: &OsString,
    uri: &OsString,
    peer: &OsString,
    output_root: &OsString,
    journal_directory: &OsString,
) -> ExitCode {
    let gid = match Gid::from_str(&gid.to_string_lossy()) {
        Ok(gid) => gid,
        Err(error) => {
            eprintln!("ariax: invalid GID: {error}");
            return ExitCode::from(2);
        }
    };
    let journal_id = match parse_journal_id(&journal_id.to_string_lossy()) {
        Some(journal_id) => journal_id,
        None => {
            eprintln!("ariax: JOURNAL_ID must be 32 nonzero hexadecimal digits");
            return ExitCode::from(2);
        }
    };
    let peer = match SocketAddr::from_str(&peer.to_string_lossy()) {
        Ok(peer) => peer,
        Err(error) => {
            eprintln!("ariax: invalid pinned peer: {error}");
            return ExitCode::from(2);
        }
    };
    let resumed_at_unix_ms = match now_unix_ms() {
        Some(value) => value,
        None => {
            eprintln!("ariax: system wall clock is before the Unix epoch");
            return ExitCode::FAILURE;
        }
    };
    let request = KnownLengthHttpResumeRequest {
        recovery: KnownLengthHttpRecoveryRequest {
            task: TaskId::new(1).expect("standalone HTTP task id is nonzero"),
            gid,
            journal_id,
            generation: Generation::INITIAL,
            journal_directory: PathBuf::from(journal_directory),
            output_root: PathBuf::from(output_root),
            replay_limits: ReplayLimits::default(),
            state_limits: JournalStateLimits::default(),
        },
        uri: uri.to_string_lossy().into_owned(),
        peer,
        resumed_at_unix_ms,
        connect_timeout: std::time::Duration::from_secs(30),
        response_head_timeout: std::time::Duration::from_secs(30),
        response_body_timeout: std::time::Duration::from_secs(60),
        storage: StorageEngineConfig::default(),
        cancellation: HttpCancellation::new(),
    };
    match resume_known_length_http_blocking(request) {
        Ok(result) => {
            println!(
                "download resumed: gid={gid} from={} bytes={} durable_pieces={} journal_sequence={}",
                result.resumed_from,
                result.content_length,
                result.durable_piece_count,
                result.terminal_sequence
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!(
                "ariax: HTTP resume failed [{}{}]: {error}",
                error.code(),
                if error.retriable() { ", retriable" } else { "" }
            );
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn download_http_pinned(
    gid: &OsString,
    journal_id: &OsString,
    uri: &OsString,
    peer: &OsString,
    output_root: &OsString,
    output: &OsString,
    journal_directory: &OsString,
    piece_length: Option<&OsString>,
) -> ExitCode {
    let gid = match Gid::from_str(&gid.to_string_lossy()) {
        Ok(gid) => gid,
        Err(error) => {
            eprintln!("ariax: invalid GID: {error}");
            return ExitCode::from(2);
        }
    };
    let journal_id = match parse_journal_id(&journal_id.to_string_lossy()) {
        Some(journal_id) => journal_id,
        None => {
            eprintln!("ariax: JOURNAL_ID must be 32 nonzero hexadecimal digits");
            return ExitCode::from(2);
        }
    };
    let peer = match SocketAddr::from_str(&peer.to_string_lossy()) {
        Ok(peer) => peer,
        Err(error) => {
            eprintln!("ariax: invalid pinned peer: {error}");
            return ExitCode::from(2);
        }
    };
    let output =
        match SafePathBuilder::from_user_path(&output.to_string_lossy(), PathPlatform::current()) {
            Ok(output) => output,
            Err(error) => {
                eprintln!("ariax: invalid output path: {error}");
                return ExitCode::from(2);
            }
        };
    let piece_length = match piece_length {
        Some(value) => match value.to_string_lossy().parse::<u64>() {
            Ok(value) if value != 0 => value,
            _ => {
                eprintln!("ariax: PIECE_LENGTH must be a nonzero decimal byte count");
                return ExitCode::from(2);
            }
        },
        None => DEFAULT_HTTP_PIECE_LENGTH,
    };
    let created_at_unix_ms = match now_unix_ms() {
        Some(value) => value,
        None => {
            eprintln!("ariax: system wall clock is before the Unix epoch");
            return ExitCode::FAILURE;
        }
    };
    let request = KnownLengthHttpRequest {
        task: TaskId::new(1).expect("standalone HTTP task id is nonzero"),
        gid,
        generation: Generation::INITIAL,
        journal_id,
        uri: uri.to_string_lossy().into_owned(),
        peer,
        output_root: PathBuf::from(output_root),
        output,
        journal_directory: PathBuf::from(journal_directory),
        piece_length,
        created_at_unix_ms,
        connect_timeout: std::time::Duration::from_secs(30),
        response_head_timeout: std::time::Duration::from_secs(30),
        response_body_timeout: std::time::Duration::from_secs(60),
        storage: StorageEngineConfig::default(),
        cancellation: HttpCancellation::new(),
    };
    match download_known_length_http_blocking(request) {
        Ok(result) => {
            println!(
                "download complete: gid={gid} bytes={} durable_pieces={} journal_sequence={}",
                result.content_length, result.durable_piece_count, result.terminal_sequence
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!(
                "ariax: HTTP download failed [{}{}]: {error}",
                error.code(),
                if error.retriable() { ", retriable" } else { "" }
            );
            ExitCode::FAILURE
        }
    }
}

fn parse_journal_id(value: &str) -> Option<JournalId> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&value[start..start + 2], 16).ok()?;
    }
    JournalId::new(bytes)
}

fn now_unix_ms() -> Option<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(duration.as_millis()).ok()
}

fn process_bootstrap_config(
    database_path: PathBuf,
    control_directory: PathBuf,
    allowed_output_roots: Vec<PathBuf>,
) -> Result<ProcessBootstrapConfig, String> {
    let now_wall_unix_ms =
        now_unix_ms().ok_or_else(|| "system wall clock is before the Unix epoch".to_owned())?;
    let task_capacity = NonZeroUsize::new(1024).expect("bootstrap task capacity is nonzero");
    let active_capacity = NonZeroUsize::new(64).expect("active capacity is nonzero");
    let runtime_capacity = NonZeroUsize::new(1024).expect("bootstrap runtime capacity is nonzero");
    let plan_capacity = NonZeroUsize::new(64).expect("plan capacity is nonzero");
    let max_wait_ms = NonZeroU64::new(86_400_000).expect("maximum wait is nonzero");
    let scheduler = SchedulerConfig::new(task_capacity, active_capacity, false)
        .map_err(|error| format!("invalid scheduler bootstrap policy: {error}"))?;
    Ok(ProcessBootstrapConfig {
        session_owner: SessionOwnerConfig::new(database_path),
        control_directory,
        allowed_output_roots,
        replay_limits: ReplayLimits::default(),
        journal_state_limits: JournalStateLimits::default(),
        recovery: StartupRecoveryConfig {
            scheduler,
            now_wall_unix_ms,
            now_monotonic: MonotonicInstant::now(),
            max_retry_wait_ms: max_wait_ms,
            max_slow_wait_ms: max_wait_ms,
            max_no_space_wait_ms: max_wait_ms,
            max_retry_elapsed_ms: max_wait_ms.get(),
        },
        runtime: RuntimeEffectConfig {
            request_capacity: runtime_capacity,
            event_capacity: runtime_capacity,
            timer_capacity: runtime_capacity,
            option_plan_capacity: plan_capacity,
        },
        persistence_plan_capacity: plan_capacity,
        updated_ms: now_wall_unix_ms,
        recovery_created_at_unix_ms: now_wall_unix_ms,
    })
}

fn run_rpc(
    database_path: PathBuf,
    control_directory: PathBuf,
    output_root: PathBuf,
    bind: Option<SocketAddr>,
    profile: RuntimeProfile,
) -> ExitCode {
    if let Err(error) = std::fs::create_dir_all(&control_directory) {
        eprintln!("ariax: cannot create control directory: {error}");
        return ExitCode::FAILURE;
    }
    if let Err(error) = std::fs::create_dir_all(&output_root) {
        eprintln!("ariax: cannot create output root: {error}");
        return ExitCode::FAILURE;
    }
    let journal_root = control_directory.join("http-journals");
    if let Err(error) = std::fs::create_dir_all(&journal_root) {
        eprintln!("ariax: cannot create HTTP journal root: {error}");
        return ExitCode::FAILURE;
    }
    let config =
        match process_bootstrap_config(database_path, control_directory, vec![output_root.clone()])
        {
            Ok(config) => config,
            Err(error) => {
                eprintln!("ariax: {error}");
                return ExitCode::FAILURE;
            }
        };
    let engine = match ariax_engine::bootstrap_process(config, persisted_option_is_safe) {
        Ok(engine) => engine,
        Err(error) => {
            eprintln!("ariax: bootstrap failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let resources = match HttpProcessResources::for_profile(profile) {
        Ok(resources) => resources,
        Err(error) => {
            eprintln!("ariax: HTTP profile capacity resolution failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let task_capacity = NonZeroUsize::new(1024).expect("task capacity is nonzero");
    let mut plane = match HttpControlPlane::new(
        engine,
        HttpControlPlaneConfig {
            output_root,
            journal_root: journal_root.clone(),
            task_capacity,
            supervisor: ariax_engine::HttpWorkerSupervisorConfig::default(),
        },
    ) {
        Ok(plane) => plane,
        Err(error) => {
            eprintln!("ariax: control plane initialization failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let resolver = match HttpResolver::new(HttpResolverConfig::default()) {
        Ok(resolver) => resolver,
        Err(error) => {
            eprintln!("ariax: resolver initialization failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let cookies = match HttpCookieJar::bundled(HttpCookieLimits::default()) {
        Ok(cookies) => Arc::new(tokio::sync::Mutex::new(cookies)),
        Err(error) => {
            eprintln!("ariax: bundled cookie policy failed verification: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mut client_config = resources.policy_client_config();
    client_config.destination = HttpDestinationPolicy::default();
    client_config.cookies = Some(cookies);
    let client = HttpPolicyClient::new(resolver, client_config);
    let worker_config = resources.worker_config(journal_root);
    let worker = match HttpMultiRangeWorker::new(client, worker_config, plane.stats_catalog()) {
        Ok(worker) => worker.with_session_owner(plane.session_handle()),
        Err(error) => {
            eprintln!("ariax: HTTP worker initialization failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = plane.attach_worker(Arc::new(worker)) {
        eprintln!("ariax: HTTP supervisor initialization failed: {error}");
        return ExitCode::FAILURE;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("ariax: cannot start async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        let backend = Arc::new(HttpControlBackend::new(plane));
        let progress_plane = backend.plane();
        let progress = tokio::spawn(async move {
            loop {
                let result = {
                    let mut plane = progress_plane.lock().await;
                    plane.poll_once()
                };
                if let Err(error) = result {
                    eprintln!("ariax: control progress failed: {error}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        let transport_result = if let Some(bind) = bind {
            serve_loopback_http_until(bind, backend.clone(), tokio::signal::ctrl_c()).await
        } else {
            run_content_length_stdio(backend.clone(), tokio::io::stdin(), tokio::io::stdout()).await
        };
        progress.abort();
        let _ = progress.await;
        let shutdown_result = shutdown_rpc_backend(backend).await;
        if let Err(error) = &transport_result {
            eprintln!("ariax: RPC transport failed: {error}");
        }
        if let Err(error) = &shutdown_result {
            eprintln!("ariax: RPC shutdown failed: {error}");
        }
        if transport_result.is_ok() && shutdown_result.is_ok() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        }
    })
}

async fn shutdown_rpc_backend(backend: Arc<HttpControlBackend>) -> Result<(), String> {
    let backend = Arc::try_unwrap(backend)
        .map_err(|_| "RPC transport retained a backend reference after drain".to_owned())?;
    let plane = backend.try_into_control_plane().map_err(|_| {
        "RPC progress loop retained a control-plane reference after drain".to_owned()
    })?;
    plane
        .shutdown_async()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn check_bootstrap(
    database_path: PathBuf,
    control_directory: PathBuf,
    allowed_output_roots: Vec<PathBuf>,
) -> ExitCode {
    let config =
        match process_bootstrap_config(database_path, control_directory, allowed_output_roots) {
            Ok(config) => config,
            Err(error) => {
                eprintln!("ariax: {error}");
                return ExitCode::FAILURE;
            }
        };
    match ariax_engine::bootstrap_process(config, persisted_option_is_safe) {
        Ok(engine) => {
            let tasks = engine.task_count();
            let retirement_failures = engine.retirement_failures().len();
            match engine.shutdown() {
                Ok(report) => {
                    println!(
                        "bootstrap ok: {tasks} tasks, {} journals closed, {retirement_failures} deferred retirements",
                        report.journals_closed
                    );
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("ariax: bootstrap succeeded but shutdown failed: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        Err(error) => {
            eprintln!("ariax: bootstrap failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn persisted_option_is_safe(name: &str) -> bool {
    builtin_registry()
        .find(name)
        .is_some_and(|definition| definition.security == SecurityClass::Normal)
}
