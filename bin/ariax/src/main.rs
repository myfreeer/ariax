#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use ariax_config::{SecurityClass, builtin_registry};
use ariax_core::{Generation, Gid, MonotonicInstant, SchedulerConfig, TaskId};
use ariax_engine::{
    HttpCancellation, KnownLengthHttpRequest, ProcessBootstrapConfig, RuntimeEffectConfig,
    StartupRecoveryConfig, StorageEngineConfig, download_known_length_http_blocking,
};
use ariax_storage::{
    JournalId, JournalStateLimits, PathPlatform, ReplayLimits, SafePathBuilder, SessionOwnerConfig,
};

const DEFAULT_HTTP_PIECE_LENGTH: u64 = 1024 * 1024;
const HELP: &str = "ariax — experimental bounded downloader\n\nUsage: ariax [--help|--version]\n       ariax --check-bootstrap SESSION_DB CONTROL_DIR [OUTPUT_ROOT ...]\n       ariax --download-http-pinned GID JOURNAL_ID URI PEER OUTPUT_ROOT OUTPUT_PATH JOURNAL_DIR [PIECE_LENGTH]\n\nThe pinned HTTP command accepts an already policy-approved numeric PEER (IP:port); it does not perform DNS or SSRF-policy resolution.\n";

fn main() -> ExitCode {
    run(env::args_os().skip(1))
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> ExitCode {
    let arguments: Vec<_> = arguments.into_iter().collect();
    match arguments.as_slice() {
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

fn check_bootstrap(
    database_path: PathBuf,
    control_directory: PathBuf,
    allowed_output_roots: Vec<PathBuf>,
) -> ExitCode {
    let now_wall_unix_ms = match now_unix_ms() {
        Some(value) => value,
        None => {
            eprintln!("ariax: system wall clock is before the Unix epoch");
            return ExitCode::FAILURE;
        }
    };
    let task_capacity = NonZeroUsize::new(1024).expect("bootstrap task capacity is nonzero");
    let active_capacity = NonZeroUsize::new(64).expect("active capacity is nonzero");
    let runtime_capacity = NonZeroUsize::new(1024).expect("bootstrap runtime capacity is nonzero");
    let plan_capacity = NonZeroUsize::new(64).expect("plan capacity is nonzero");
    let max_wait_ms = NonZeroU64::new(86_400_000).expect("maximum wait is nonzero");
    let scheduler = match SchedulerConfig::new(task_capacity, active_capacity, false) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("ariax: invalid scheduler bootstrap policy: {error}");
            return ExitCode::FAILURE;
        }
    };
    let config = ProcessBootstrapConfig {
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
