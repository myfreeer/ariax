//! Shared benchmark bootstrap; exercised without starting the load fixtures.

use ariax_core::{MonotonicInstant, SchedulerConfig};
use ariax_engine::*;
use ariax_storage::{JournalStateLimits, ReplayLimits, SessionOwnerConfig};
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("nonzero fixture capacity")
}

pub(super) fn scheduler_config(capacity: usize, mixed_bt: bool) -> Result<SchedulerConfig> {
    let capacity = NonZeroUsize::new(capacity).ok_or("zero fixture capacity")?;
    Ok(SchedulerConfig::new(
        capacity,
        nz(if mixed_bt { 2 } else { 1 }),
        true,
    )?)
}

pub(super) fn private_directory(path: &Path) -> io::Result<()> {
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

pub(super) fn build_control_plane(
    root: &Path,
    resources: &HttpProcessResources,
    capacity: usize,
    mixed_bt: bool,
) -> Result<(HttpControlPlane, PathBuf)> {
    if mixed_bt && !cfg!(feature = "bt") {
        return Err("mixed BT setup requires the bt feature".into());
    }
    let control = root.join("control");
    let output = root.join("output");
    let journals = control.join("http-journals");
    for path in [&control, &output, &journals] {
        private_directory(path)?;
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let config = ProcessBootstrapConfig {
        session_owner: SessionOwnerConfig::new(root.join("session.db")),
        control_directory: control,
        allowed_output_roots: vec![output.clone()],
        replay_limits: ReplayLimits::default(),
        journal_state_limits: JournalStateLimits::default(),
        recovery: StartupRecoveryConfig {
            scheduler: scheduler_config(capacity, mixed_bt)?,
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
    #[cfg(feature = "bt")]
    if mixed_bt {
        BitTorrentConfig {
            allow_private_destinations: true,
            dht: false,
            peer_exchange: false,
            encryption: BitTorrentEncryption::Disabled,
            max_tasks: 4,
            max_peers: 1024,
            max_open_files: 4,
            ..BitTorrentConfig::default()
        }
        .apply(&mut plane)?;
    }
    Ok((plane, journals))
}
