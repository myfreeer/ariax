#![forbid(unsafe_code)]

mod rpc_service;
mod startup;

use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ariax_config::persisted_option_is_safe;
use ariax_core::{Aria2Status, Generation, Gid, MonotonicInstant, SchedulerConfig, TaskId};
use ariax_engine::{
    AddMetalink, AddUri, ApproveHostKey, DownloadOptions, Engine, HttpCancellation,
    HttpControlBackend, HttpControlPlane, HttpControlPlaneConfig, HttpCookieJar, HttpCookieLimits,
    HttpDestinationPolicy, HttpMultiRangeWorker, HttpPolicyClient, HttpProcessResources,
    HttpResolver, HttpResolverConfig, KnownLengthHttpRecoveryRequest, KnownLengthHttpRequest,
    KnownLengthHttpResumeRequest, ProcessBootstrapConfig, RpcAuthPolicy, RuntimeEffectConfig,
    StartupRecoveryConfig, StorageEngineConfig, download_known_length_http_blocking,
    resume_known_length_http_blocking,
};
use ariax_runtime::RuntimeProfile;
use ariax_storage::{
    JournalId, JournalStateLimits, PathPlatform, ReplayLimits, SafePathBuilder, SessionOwnerConfig,
};

const DEFAULT_HTTP_PIECE_LENGTH: u64 = 1024 * 1024;
const HELP: &str = "ariax — experimental bounded downloader\n\nUsage: ariax [--help|--version]\n       ariax --check-bootstrap SESSION_DB CONTROL_DIR [OUTPUT_ROOT ...]\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --add-uri SESSION_DB CONTROL_DIR OUTPUT_ROOT URI [URI ...]\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --status SESSION_DB CONTROL_DIR OUTPUT_ROOT GID\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --pause SESSION_DB CONTROL_DIR OUTPUT_ROOT GID\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --resume SESSION_DB CONTROL_DIR OUTPUT_ROOT GID\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --remove SESSION_DB CONTROL_DIR OUTPUT_ROOT GID\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --rpc-http SESSION_DB CONTROL_DIR OUTPUT_ROOT LOOPBACK_ADDR\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --rpc-ws SESSION_DB CONTROL_DIR OUTPUT_ROOT LOOPBACK_ADDR\n       ariax [--profile=auto|concurrency|throughput|latency|compact] --rpc-stdio SESSION_DB CONTROL_DIR OUTPUT_ROOT\n       ariax --add-metalink SESSION_DB CONTROL_DIR OUTPUT_ROOT FILE [--NAME=VALUE ...]\n       ariax approve-host-key SESSION_DB CONTROL_DIR OUTPUT_ROOT GID CHALLENGE SHA256_FINGERPRINT\n       ariax --download-http-pinned GID JOURNAL_ID URI PEER OUTPUT_ROOT OUTPUT_PATH JOURNAL_DIR [PIECE_LENGTH]\n       ariax --resume-http-pinned GID JOURNAL_ID URI PEER OUTPUT_ROOT JOURNAL_DIR\n\nRPC is JSON-RPC 2.0 over loopback HTTP/1.1, loopback WebSocket, or Content-Length-framed stdio. Direct control commands use the same engine/control plane. Add commands accept --NAME=VALUE download options, including checksum, uri-selector, server-stat-timeout, FTP/SFTP settings, follow-metalink and Metalink selection filters. Explicit Metalink input also accepts --metalink-base-uri and --position. Supported checksums: sha-512, sha-256, sha-1 and md5. The pinned HTTP commands accept an already policy-approved numeric PEER (IP:port); they do not perform DNS or SSRF-policy resolution.\n";

fn main() -> ExitCode {
    run(env::args_os().skip(1))
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> ExitCode {
    let arguments: Vec<_> = arguments.into_iter().collect();
    let (startup, arguments) = match startup::StartupOptions::parse(&arguments) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("ariax: {error}");
            return ExitCode::from(2);
        }
    };
    let profile = startup.profile;
    let rpc = matches!(
        arguments.first().and_then(|arg| arg.to_str()),
        Some("--rpc" | "--rpc-http" | "--rpc-ws" | "--rpc-stdio")
    );
    let rpc_call = arguments
        .first()
        .is_some_and(|argument| argument == "--rpc-call");
    if startup.has_rpc_arguments() && !rpc && (!rpc_call || startup.requires_rpc_service()) {
        eprintln!(
            "ariax: transport, credentials and configuration startup options require an RPC service; --rpc-call accepts --rpc-compat"
        );
        return ExitCode::from(2);
    }
    if (startup.session_export.is_some() || startup.input_file.is_some()) && !rpc {
        eprintln!("ariax: session startup options require an RPC command");
        return ExitCode::from(2);
    }
    let auth = if rpc {
        match startup.auth_from_environment() {
            Ok(auth) => auth,
            Err(error) => {
                eprintln!("ariax: {error}");
                return ExitCode::from(2);
            }
        }
    } else {
        RpcAuthPolicy::default()
    };
    if profile.is_some()
        && !matches!(
            arguments.first(),
            Some(command)
                if command == "--rpc-http"
                    || command == "--rpc"
                    || command == "--rpc-call"
                    || command == "--rpc-ws"
                    || command == "--rpc-stdio"
                    || command == "--add-uri"
                    || command == "--add-metalink"
                    || command == "--metalink-file"
                    || command == "approve-host-key"
                    || command == "--approve-host-key"
                    || command == "--status"
                    || command == "--pause"
                    || command == "--resume"
                    || command == "--remove"
        )
    {
        eprintln!("ariax: --profile is accepted only with RPC or direct control commands");
        return ExitCode::from(2);
    }
    match arguments {
        [] => {
            print!("{HELP}{RPC_STARTUP_HELP}{RPC_INTERFACE_HELP}");
            ExitCode::SUCCESS
        }
        [arg] if arg == "--help" || arg == "-h" => {
            print!("{HELP}{RPC_STARTUP_HELP}{RPC_INTERFACE_HELP}");
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
                false,
                auth,
                &startup,
            )
        }
        [command, database, control, output_root, bind] if command == "--rpc" => {
            let transport = startup.rpc_transport.unwrap_or(startup::RpcTransport::Http);
            let bind = match bind
                .to_str()
                .and_then(|value| value.parse::<SocketAddr>().ok())
            {
                Some(bind) if transport.has_network() => bind,
                _ => {
                    eprintln!("ariax: a network RPC transport requires a loopback address");
                    return ExitCode::from(2);
                }
            };
            run_rpc(
                PathBuf::from(database),
                PathBuf::from(control),
                PathBuf::from(output_root),
                Some(bind),
                profile.unwrap_or_default(),
                transport.websocket(),
                auth,
                &startup,
            )
        }
        [command, database, control, output_root]
            if command == "--rpc"
                && startup.rpc_transport == Some(startup::RpcTransport::Stdio) =>
        {
            run_rpc(
                PathBuf::from(database),
                PathBuf::from(control),
                PathBuf::from(output_root),
                None,
                profile.unwrap_or_default(),
                false,
                auth,
                &startup,
            )
        }
        [command, database, control, output_root, request] if command == "--rpc-call" => {
            let Some(request) = request
                .to_str()
                .filter(|request| request.len() <= ariax_engine::MAX_HTTP_RPC_REQUEST_BYTES)
            else {
                eprintln!("ariax: RPC document must be bounded UTF-8 text");
                return ExitCode::from(2);
            };
            run_direct_control(
                PathBuf::from(database),
                PathBuf::from(control),
                PathBuf::from(output_root),
                profile.unwrap_or_default(),
                DirectControl::RpcJson(request.to_owned(), startup.compatibility),
            )
        }
        [command, database, control, output_root, bind] if command == "--rpc-ws" => {
            let bind = match SocketAddr::from_str(&bind.to_string_lossy()) {
                Ok(bind) => bind,
                Err(error) => {
                    eprintln!("ariax: invalid RPC WebSocket bind address: {error}");
                    return ExitCode::from(2);
                }
            };
            run_rpc(
                PathBuf::from(database),
                PathBuf::from(control),
                PathBuf::from(output_root),
                Some(bind),
                profile.unwrap_or_default(),
                true,
                auth,
                &startup,
            )
        }
        [command, database, control, output_root] if command == "--rpc-stdio" => run_rpc(
            PathBuf::from(database),
            PathBuf::from(control),
            PathBuf::from(output_root),
            None,
            profile.unwrap_or_default(),
            false,
            auth,
            &startup,
        ),
        [
            command,
            database,
            control,
            output_root,
            gid,
            challenge,
            fingerprint,
        ] if command == "approve-host-key" || command == "--approve-host-key" => {
            let request = gid
                .to_str()
                .and_then(|gid| gid.parse().ok())
                .ok_or_else(|| "invalid GID".to_owned())
                .and_then(|gid| {
                    ApproveHostKey::from_text(
                        gid,
                        &challenge.to_string_lossy(),
                        &fingerprint.to_string_lossy(),
                    )
                    .map_err(|error| error.to_string())
                });
            match request {
                Ok(request) => run_direct_control(
                    PathBuf::from(database),
                    PathBuf::from(control),
                    PathBuf::from(output_root),
                    profile.unwrap_or_default(),
                    DirectControl::Approve(request),
                ),
                Err(error) => {
                    eprintln!("ariax: {error}");
                    ExitCode::from(2)
                }
            }
        }
        [command, database, control, output_root, path, flags @ ..]
            if command == "--add-metalink" || command == "--metalink-file" =>
        {
            match read_metalink_file(&PathBuf::from(path)).and_then(|bytes| {
                let (positional, options, selection, position) = parse_transfer_flags(flags, true)?;
                if !positional.is_empty() {
                    return Err("unexpected Metalink argument".into());
                }
                Ok(AddMetalink {
                    bytes,
                    options,
                    selection,
                    position,
                })
            }) {
                Ok(request) => run_direct_control(
                    PathBuf::from(database),
                    PathBuf::from(control),
                    PathBuf::from(output_root),
                    profile.unwrap_or_default(),
                    DirectControl::Metalink(request),
                ),
                Err(error) => {
                    eprintln!("ariax: {error}");
                    ExitCode::from(2)
                }
            }
        }
        [command, database, control, output_root, uris @ ..]
            if command == "--add-uri" && !uris.is_empty() =>
        {
            match parse_transfer_flags(uris, false) {
                Ok((uris, options, _, _)) if !uris.is_empty() => run_direct_control(
                    PathBuf::from(database),
                    PathBuf::from(control),
                    PathBuf::from(output_root),
                    profile.unwrap_or_default(),
                    DirectControl::Add(AddUri { uris, options }),
                ),
                Ok(_) => {
                    eprintln!("ariax: add-uri requires a URI");
                    ExitCode::from(2)
                }
                Err(error) => {
                    eprintln!("ariax: {error}");
                    ExitCode::from(2)
                }
            }
        }
        [command, database, control, output_root, gid]
            if command == "--status"
                || command == "--pause"
                || command == "--resume"
                || command == "--remove" =>
        {
            let gid = match Gid::from_str(&gid.to_string_lossy()) {
                Ok(gid) => gid,
                Err(error) => {
                    eprintln!("ariax: invalid GID: {error}");
                    return ExitCode::from(2);
                }
            };
            let command = if command == "--status" {
                DirectControl::Status(gid)
            } else if command == "--pause" {
                DirectControl::Pause(gid)
            } else if command == "--resume" {
                DirectControl::Resume(gid)
            } else {
                DirectControl::Remove(gid)
            };
            run_direct_control(
                PathBuf::from(database),
                PathBuf::from(control),
                PathBuf::from(output_root),
                profile.unwrap_or_default(),
                command,
            )
        }
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

const RPC_STARTUP_HELP: &str = "\nRPC startup options (before the command):\n  --rpc-secret=VALUE   Method token; defaults to ARIAX_RPC_SECRET\n  --rpc-user=VALUE     HTTP Basic user; defaults to ARIAX_RPC_USER\n  --rpc-passwd=VALUE   HTTP Basic password; defaults to ARIAX_RPC_PASSWD\nSession startup options:\n  --save-session=FILE  Atomically save unfinished downloads at shutdown\n  --save-session-format=aria2|json  Default aria2\n  --save-session-interval=SECONDS  Periodic saving; 0 disables it\n  --input-file=FILE    Import a complete bounded session before workers start\n  --input-file-format=aria2|json   Default aria2\nBoth Basic fields must be configured together. HTTP Basic applies to HTTP and WebSocket; method tokens also apply to stdio.\n";

const RPC_INTERFACE_HELP: &str = "\nCombined RPC and compatibility commands:\n  --rpc SESSION_DB CONTROL_DIR OUTPUT_ROOT [LOOPBACK_ADDR]\n  --rpc-call SESSION_DB CONTROL_DIR OUTPUT_ROOT JSON_RPC_DOCUMENT\nAdditional startup options (before the command):\n  --rpc-transport=http|websocket|stdio|http+stdio|websocket+stdio\n  --rpc-stdio-framing=content-length|ndjson\n  --rpc-stdio-eof=shutdown|close-transport|ignore\n  --rpc-stdio-events=true|false\n  --rpc-stdio-max-request-size=SIZE  At most 2M\n  --rpc-compat=aria2|extended|strict\n  --conf-path=FILE   Reloadable HTTP task defaults\n  --url-rules=FILE   Bounded TOML rules\n";

enum DirectControl {
    Add(AddUri),
    Metalink(AddMetalink),
    Approve(ApproveHostKey),
    Status(Gid),
    Pause(Gid),
    Resume(Gid),
    Remove(Gid),
    RpcJson(String, ariax_engine::RpcCompatibility),
}

fn run_direct_control(
    database_path: PathBuf,
    control_directory: PathBuf,
    output_root: PathBuf,
    profile: RuntimeProfile,
    command: DirectControl,
) -> ExitCode {
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
        let engine = match Engine::builder()
            .database_path(database_path)
            .control_directory(control_directory)
            .output_root(output_root)
            .profile(profile)
            .build()
            .await
        {
            Ok(engine) => engine,
            Err(error) => {
                eprintln!("ariax: direct control bootstrap failed: {error}");
                return ExitCode::FAILURE;
            }
        };
        let result = match command {
            DirectControl::Add(uris) => direct_add(&engine, uris).await,
            DirectControl::Metalink(bytes) => direct_metalink(&engine, bytes).await,
            DirectControl::Approve(request) => engine
                .approve_host_key(request)
                .await
                .map_err(|error| error.to_string()),
            DirectControl::Status(gid) => direct_status(&engine, gid).await,
            DirectControl::Pause(gid) => engine
                .pause(gid)
                .await
                .map(|()| println!("paused {gid}"))
                .map_err(|error| error.to_string()),
            DirectControl::Resume(gid) => engine
                .resume(gid)
                .await
                .map(|()| println!("resumed {gid}"))
                .map_err(|error| error.to_string()),
            DirectControl::Remove(gid) => engine
                .remove(gid)
                .await
                .map(|()| println!("removed {gid}"))
                .map_err(|error| error.to_string()),
            DirectControl::RpcJson(request, mode) => {
                match engine.rpc_json(request.as_bytes(), mode).await {
                    Ok(response) => {
                        use std::io::Write as _;
                        std::io::stdout()
                            .write_all(&response)
                            .and_then(|()| std::io::stdout().write_all(b"\n"))
                            .map_err(|error| error.to_string())
                    }
                    Err(error) => Err(error.to_string()),
                }
            }
        };
        let shutdown = engine.shutdown().await.map_err(|error| error.to_string());
        if let Err(error) = result {
            eprintln!("ariax: direct control failed: {error}");
            return ExitCode::FAILURE;
        }
        if let Err(error) = shutdown {
            eprintln!("ariax: direct control shutdown failed: {error}");
            return ExitCode::FAILURE;
        }
        ExitCode::SUCCESS
    })
}

async fn direct_add(engine: &Engine, request: AddUri) -> Result<(), String> {
    let gid = engine
        .add_uri(request)
        .await
        .map_err(|error| error.to_string())?;
    println!("added {gid}");
    direct_wait(engine, gid).await
}

type TransferArguments = (
    Vec<String>,
    DownloadOptions,
    ariax_engine::MetalinkSelection,
    Option<i64>,
);
fn parse_transfer_flags(
    arguments: &[OsString],
    metalink: bool,
) -> Result<TransferArguments, String> {
    let mut positional = Vec::new();
    let mut options = Vec::new();
    let mut selection = ariax_engine::MetalinkSelection::default();
    let mut position = None;
    let mut literal = false;
    for argument in arguments {
        let text = argument
            .to_str()
            .ok_or("transfer arguments must be UTF-8")?;
        if text == "--" && !literal {
            literal = true;
            continue;
        }
        if !literal && let Some(flag) = text.strip_prefix("--") {
            let (name, value) = flag
                .split_once('=')
                .ok_or("download options require --NAME=VALUE")?;
            if name == "metalink-base-uri" && metalink {
                if selection.base_uri.replace(value.to_owned()).is_some() {
                    return Err("duplicate Metalink base URI".into());
                }
            } else if name == "position" && metalink {
                let value = value
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value >= -1)
                    .ok_or("invalid queue position")?;
                if position.replace(value).is_some() {
                    return Err("duplicate queue position".into());
                }
            } else {
                options.push((name.to_owned(), value.to_owned()));
            }
        } else {
            positional.push(text.to_owned());
        }
    }
    Ok((
        positional,
        DownloadOptions::from_pairs(options).map_err(|error| error.to_string())?,
        selection,
        position,
    ))
}

fn read_metalink_file(path: &std::path::Path) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|_| "cannot open Metalink file")?;
    const LIMIT: usize = 64 * 1024 * 1024;
    if !file
        .metadata()
        .map_err(|_| "cannot inspect Metalink file")?
        .is_file()
    {
        return Err("Metalink input must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take((LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read Metalink file")?;
    if bytes.len() > LIMIT {
        return Err("Metalink file exceeds 64 MiB".into());
    }
    Ok(bytes)
}

async fn direct_metalink(engine: &Engine, request: AddMetalink) -> Result<(), String> {
    let gids = engine
        .add_metalink(request)
        .await
        .map_err(|error| error.to_string())?;
    for gid in &gids {
        println!("added {gid}");
    }
    for gid in gids {
        direct_wait(engine, gid).await?;
    }
    Ok(())
}

fn read_key_approval(input: impl std::io::Read) -> Option<bool> {
    use std::io::BufRead;
    let mut text = Vec::new();
    std::io::BufReader::new(input.take(32))
        .read_until(b'\n', &mut text)
        .ok()?;
    Some(text == b"yes\n" || text == b"yes\r\n")
}

async fn direct_wait(engine: &Engine, gid: Gid) -> Result<(), String> {
    let mut pending = std::collections::VecDeque::from([gid]);
    let mut seen = std::collections::BTreeSet::new();
    while let Some(gid) = pending.pop_front() {
        if !seen.insert(gid) {
            continue;
        }
        pending.extend(direct_wait_one(engine, gid).await?);
        if pending.len() > 1000 {
            return Err("too many followed tasks".into());
        }
    }
    Ok(())
}

async fn direct_wait_one(engine: &Engine, gid: Gid) -> Result<Vec<Gid>, String> {
    loop {
        let status = engine
            .status(gid)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(challenge) = status.host_key_challenge {
            use std::io::IsTerminal;
            let id = challenge
                .id
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let fingerprint =
                ariax_storage::session_host_key_pin_value(challenge.fingerprint_sha256);
            eprintln!(
                "host key for {}:{}: {} SHA-256 {} (challenge {})",
                challenge.canonical_host, challenge.port, challenge.algorithm, fingerprint, id
            );
            if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
                return Err(format!(
                    "task {gid} is paused; verify the fingerprint, then use ariax approve-host-key SESSION_DB CONTROL_DIR OUTPUT_ROOT {gid} {id} {fingerprint}, or supply an explicit host-key pin"
                ));
            }
            eprintln!("Approve this key? Type yes to allow, or anything else to stop:");
            let approved =
                tokio::task::spawn_blocking(|| read_key_approval(std::io::stdin().lock()))
                    .await
                    .map_err(|_| "approval input stopped")?
                    .unwrap_or(false);
            if !approved {
                engine
                    .remove(gid)
                    .await
                    .map_err(|error| error.to_string())?;
                return Err(format!("host key rejected for {gid}"));
            }
            engine
                .approve_host_key(ApproveHostKey {
                    gid,
                    challenge: challenge.id,
                    fingerprint_sha256: challenge.fingerprint_sha256,
                })
                .await
                .map_err(|error| error.to_string())?;
            continue;
        }
        if status.status == Aria2Status::Paused {
            println!("paused {gid}");
            return Ok(Vec::new());
        }
        if status.status.is_terminal() {
            println!(
                "gid={} status={} completed={} total={}",
                gid, status.status, status.completed_length, status.total_length
            );
            return if status.status == Aria2Status::Complete {
                Ok(status.followed_by)
            } else {
                Err(format!("task {gid} finished with status {}", status.status))
            };
        }
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.map_err(|error| error.to_string())?;
                return Err(format!("task {gid} interrupted"));
            }
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}

async fn direct_status(engine: &Engine, gid: Gid) -> Result<(), String> {
    let status = engine
        .status(gid)
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "gid={} status={} completed={} total={}",
        gid, status.status, status.completed_length, status.total_length
    );
    Ok(())
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
    let scheduler = SchedulerConfig::new(task_capacity, active_capacity, true)
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
        shutdown_step_timeout_ms: ariax_engine::DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS,
        updated_ms: now_wall_unix_ms,
        recovery_created_at_unix_ms: now_wall_unix_ms,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_rpc(
    database_path: PathBuf,
    control_directory: PathBuf,
    output_root: PathBuf,
    bind: Option<SocketAddr>,
    profile: RuntimeProfile,
    websocket: bool,
    auth: RpcAuthPolicy,
    startup: &startup::StartupOptions,
) -> ExitCode {
    let transport = startup.rpc_transport.unwrap_or(if bind.is_none() {
        startup::RpcTransport::Stdio
    } else if websocket {
        startup::RpcTransport::Websocket
    } else {
        startup::RpcTransport::Http
    });
    if transport.has_network() != bind.is_some()
        || bind.is_some_and(|bind| !bind.ip().is_loopback())
    {
        eprintln!("ariax: network RPC requires an IP-loopback bind address");
        return ExitCode::from(2);
    }
    let configuration = match rpc_service::read_configuration(startup) {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("ariax: {error}");
            return ExitCode::from(2);
        }
    };
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
    if let Err(error) = plane.attach_process_resources(resources.clone()) {
        eprintln!("ariax: RPC budget initialization failed: {error}");
        return ExitCode::FAILURE;
    }
    if let Some(configuration) = configuration
        && let Err(error) = plane.call("ariax.reloadConfig", configuration)
    {
        eprintln!("ariax: configuration was rejected: {error}");
        let _ = plane.shutdown();
        return ExitCode::FAILURE;
    }
    if let Some(config) = &startup.session_export
        && let Err(error) = plane.configure_session_export(config.clone())
    {
        eprintln!("ariax: session export configuration failed: {error}");
        let _ = plane.shutdown();
        return ExitCode::FAILURE;
    }
    if let Some((path, format)) = &startup.input_file
        && let Err(error) = plane.import_session_file(path, *format)
    {
        eprintln!("ariax: session import failed: {error}");
        let _ = plane.shutdown();
        return ExitCode::FAILURE;
    }
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
    let global_download_rate = worker_config.download_rate.clone();
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
    if let Err(error) = plane.attach_global_download_rate(global_download_rate) {
        eprintln!("ariax: global download limiter initialization failed: {error}");
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
    let outcome = runtime.block_on(async move {
        let backend = Arc::new(HttpControlBackend::new(plane));
        let transport_result =
            rpc_service::serve(backend.clone(), auth, bind, transport, startup).await;
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
    });
    // Tokio stdin may still own an OS read after the framed reader is cancelled.
    // All engine and transport owners have drained before this bounded runtime stop.
    runtime.shutdown_timeout(Duration::from_millis(100));
    outcome
}

async fn wait_for_rpc_shutdown(
    receiver: &mut tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    loop {
        if *receiver.borrow() {
            return Ok(());
        }
        receiver.changed().await.map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "RPC shutdown channel closed",
            )
        })?;
    }
}

async fn shutdown_rpc_backend(backend: Arc<HttpControlBackend>) -> Result<(), String> {
    let backend = Arc::try_unwrap(backend)
        .map_err(|_| "RPC transport retained a backend reference after drain".to_owned())?;
    let drained = backend
        .drain_control_runtime()
        .await
        .map_err(|error| error.to_string());
    let plane = backend.try_into_control_plane().map_err(|_| {
        "RPC control runtime retained a control-plane reference after drain".to_owned()
    })?;
    plane
        .shutdown_async()
        .await
        .map_err(|error| error.to_string())
        .and_then(|report| {
            if report.is_clean() {
                drained
            } else {
                let failure = report
                    .shutdown()
                    .first_failure()
                    .map_or("unknown", |failure| failure.step.code());
                Err(format!(
                    "shutdown completed with dirty checkpoint at {failure}"
                ))
            }
        })
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
                    if !report.is_clean() {
                        let failure = report
                            .shutdown()
                            .first_failure()
                            .map_or("unknown", |failure| failure.step.code());
                        eprintln!(
                            "ariax: bootstrap shutdown completed with dirty checkpoint at {failure}"
                        );
                        return ExitCode::FAILURE;
                    }
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

#[cfg(test)]
mod phase5_cli_tests {
    use super::*;
    #[test]
    fn cli_options_use_shared_validation_and_reject_unknown_or_duplicate_flags() {
        let flags = [
            "https://example.test/file",
            "--checksum=md5=d41d8cd98f00b204e9800998ecf8427e",
            "--follow-metalink=mem",
            "--server-stat-timeout=2",
        ];
        let (uris, options, _, _) =
            parse_transfer_flags(&flags.map(OsString::from), false).unwrap();
        assert_eq!(uris.len(), 1);
        let transfer = options.transfer.unwrap();
        assert_eq!(
            transfer.follow_metalink,
            ariax_engine::FollowMetalink::Memory
        );
        assert_eq!(transfer.server_stat_timeout, Duration::from_secs(2));
        assert!(transfer.checksum.is_some());
        for flags in [
            vec!["--follow-metalink=bad"],
            vec!["--server-stat-timeout=-1"],
            vec!["--split=1", "--split=2"],
            vec!["--metalink-expansion=forged"],
            vec!["--position=-2"],
            vec!["--unknown=true"],
        ] {
            assert!(
                parse_transfer_flags(
                    &flags.into_iter().map(OsString::from).collect::<Vec<_>>(),
                    true
                )
                .is_err()
            );
        }
        let (_, options, selection, position) = parse_transfer_flags(
            &[
                "--select-file=2",
                "--metalink-base-uri=https://example.test/",
                "--position=0",
            ]
            .map(OsString::from),
            true,
        )
        .unwrap();
        assert_eq!(position, Some(0));
        assert!(selection.base_uri.is_some());
        assert_eq!(
            options.transfer.unwrap().metalink_filters["select-file"],
            "2"
        );
    }
    #[test]
    fn host_key_prompt_accepts_only_a_complete_explicit_yes_line() {
        assert_eq!(read_key_approval(&b"yes\n"[..]), Some(true));
        assert_eq!(read_key_approval(&b"yes\r\n"[..]), Some(true));
        for input in [
            &b"yes"[..],
            b"y\n",
            b"no\n",
            b"yes please\n",
            b"YES\n",
            b"",
            &[b'y'; 33],
        ] {
            assert_eq!(read_key_approval(input), Some(false));
        }
    }
}
