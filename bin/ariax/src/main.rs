#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsString;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use ariax_config::{SecurityClass, builtin_registry};
use ariax_core::{MonotonicInstant, SchedulerConfig};
use ariax_engine::{ProcessBootstrapConfig, RuntimeEffectConfig, StartupRecoveryConfig};
use ariax_storage::{JournalStateLimits, ReplayLimits, SessionOwnerConfig};

const HELP: &str = "ariax — experimental bounded downloader\n\nUsage: ariax [--help|--version]\n       ariax --check-bootstrap SESSION_DB CONTROL_DIR [OUTPUT_ROOT ...]\n";

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

fn check_bootstrap(
    database_path: PathBuf,
    control_directory: PathBuf,
    allowed_output_roots: Vec<PathBuf>,
) -> ExitCode {
    let now_wall_unix_ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => {
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
