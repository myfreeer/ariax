//! Typed Rust embedding facade over the process-owned HTTP control plane.

use crate::{
    HttpControlError, HttpControlPlane, HttpControlPlaneConfig, HttpCookieJar, HttpCookieLimits,
    HttpDestinationPolicy, HttpMultiRangeWorker, HttpPolicyClient, HttpProcessResources,
    HttpResolver, HttpResolverConfig, HttpWorkerSupervisorConfig, ProcessBootstrapConfig,
    RpcEventBroker, RpcEventError, RpcEventLimits, RpcEventSubscriber, RuntimeEffectConfig,
    StartupRecoveryConfig,
};
use ariax_config::{SecurityClass, builtin_registry};
use ariax_core::{Aria2Status, Gid, MonotonicInstant, SchedulerConfig};
use ariax_runtime::RuntimeProfile;
use ariax_storage::{JournalStateLimits, ReplayLimits, SessionOwnerConfig};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Default)]
pub struct DownloadOptions {
    pub pause: bool,
    pub output: Option<String>,
    pub split: Option<NonZeroUsize>,
    pub timeout_seconds: Option<u64>,
    pub max_download_limit: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct AddUri {
    pub uris: Vec<String>,
    pub options: DownloadOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskStatus {
    pub gid: Gid,
    pub status: Aria2Status,
    pub total_length: u64,
    pub completed_length: u64,
}

#[derive(Debug)]
pub enum NativeApiError {
    InvalidConfiguration(&'static str),
    Bootstrap(String),
    Control(HttpControlError),
    InvalidResponse(&'static str),
    Event(RpcEventError),
    Shutdown(String),
}

impl fmt::Display for NativeApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
            Self::Bootstrap(message) => write!(formatter, "engine bootstrap failed: {message}"),
            Self::Control(error) => error.fmt(formatter),
            Self::InvalidResponse(message) => formatter.write_str(message),
            Self::Event(error) => error.fmt(formatter),
            Self::Shutdown(message) => write!(formatter, "engine shutdown failed: {message}"),
        }
    }
}

impl Error for NativeApiError {}

#[derive(Clone, Debug, Default)]
pub struct EngineBuilder {
    output_root: Option<PathBuf>,
    control_directory: Option<PathBuf>,
    database_path: Option<PathBuf>,
    profile: RuntimeProfile,
}

impl EngineBuilder {
    #[must_use]
    pub fn output_root(mut self, output_root: impl Into<PathBuf>) -> Self {
        self.output_root = Some(output_root.into());
        self
    }

    #[must_use]
    pub fn control_directory(mut self, control_directory: impl Into<PathBuf>) -> Self {
        self.control_directory = Some(control_directory.into());
        self
    }

    #[must_use]
    pub fn database_path(mut self, database_path: impl Into<PathBuf>) -> Self {
        self.database_path = Some(database_path.into());
        self
    }

    #[must_use]
    pub fn profile(mut self, profile: RuntimeProfile) -> Self {
        self.profile = profile;
        self
    }

    pub async fn build(self) -> Result<Engine, NativeApiError> {
        let output_root = self
            .output_root
            .ok_or(NativeApiError::InvalidConfiguration(
                "output root is required",
            ))?;
        if !output_root.is_absolute() {
            return Err(NativeApiError::InvalidConfiguration(
                "output root must be absolute",
            ));
        }
        std::fs::create_dir_all(&output_root)
            .map_err(|_| NativeApiError::InvalidConfiguration("cannot create output root"))?;
        let control_directory = self
            .control_directory
            .unwrap_or_else(|| output_root.join(".ariax-control"));
        create_private_directory(&control_directory, "cannot create control directory")?;
        let database_path = self
            .database_path
            .unwrap_or_else(|| control_directory.join("session.db"));
        let journal_root = control_directory.join("http-journals");
        create_private_directory(&journal_root, "cannot create journal directory")?;

        let config = process_config(database_path, control_directory, output_root.clone())?;
        let engine = crate::bootstrap_process(config, persisted_option_is_safe)
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;
        let resources = HttpProcessResources::for_profile(self.profile)
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;
        let mut plane = HttpControlPlane::new(
            engine,
            HttpControlPlaneConfig {
                output_root,
                journal_root: journal_root.clone(),
                task_capacity: NonZeroUsize::new(1024).expect("task capacity"),
                supervisor: HttpWorkerSupervisorConfig::default(),
            },
        )
        .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;
        let resolver = HttpResolver::new(HttpResolverConfig::default())
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;
        let cookies = HttpCookieJar::bundled(HttpCookieLimits::default())
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;
        let cookies = Arc::new(Mutex::new(cookies));
        let mut client_config = resources.policy_client_config();
        client_config.destination = HttpDestinationPolicy::default();
        client_config.cookies = Some(cookies);
        let client = HttpPolicyClient::new(resolver, client_config);
        let worker_config = resources.worker_config(journal_root);
        let global_download_rate = worker_config.download_rate.clone();
        let worker = HttpMultiRangeWorker::new(client, worker_config, plane.stats_catalog())
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?
            .with_session_owner(plane.session_handle());
        plane
            .attach_worker(Arc::new(worker))
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;
        plane
            .attach_global_download_rate(global_download_rate)
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;

        let events = plane.event_broker();
        let plane = Arc::new(Mutex::new(plane));
        let progress_plane = plane.clone();
        let progress = tokio::spawn(async move {
            loop {
                let result = {
                    let mut plane = progress_plane.lock().await;
                    plane.poll_once()
                };
                if result.is_err() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });
        Ok(Engine {
            plane,
            events,
            progress: Mutex::new(Some(progress)),
        })
    }
}

pub struct Engine {
    plane: Arc<Mutex<HttpControlPlane>>,
    events: RpcEventBroker,
    progress: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("running", &true)
            .finish_non_exhaustive()
    }
}

impl Engine {
    #[must_use]
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }

    pub async fn add_uri(&self, request: AddUri) -> Result<Gid, NativeApiError> {
        if request.uris.is_empty() {
            return Err(NativeApiError::InvalidConfiguration(
                "at least one URI is required",
            ));
        }
        let mut options = serde_json::Map::new();
        options.insert("pause".to_owned(), Value::Bool(request.options.pause));
        if let Some(output) = request.options.output {
            options.insert("out".to_owned(), Value::String(output));
        }
        if let Some(split) = request.options.split {
            options.insert("split".to_owned(), Value::from(split.get()));
        }
        if let Some(timeout) = request.options.timeout_seconds {
            options.insert("timeout".to_owned(), Value::from(timeout));
        }
        if let Some(limit) = request.options.max_download_limit {
            options.insert("max-download-limit".to_owned(), Value::from(limit));
        }
        let value = self
            .call_control("aria2.addUri", json!([request.uris, options]))
            .await?;
        value
            .as_str()
            .ok_or(NativeApiError::InvalidResponse(
                "addUri did not return a GID",
            ))?
            .parse()
            .map_err(|_| NativeApiError::InvalidResponse("addUri returned an invalid GID"))
    }

    pub async fn status(&self, gid: Gid) -> Result<TaskStatus, NativeApiError> {
        let value = self
            .call_control("aria2.tellStatus", json!([gid.to_string()]))
            .await?;
        let status = match value.get("status").and_then(Value::as_str) {
            Some("active") => Aria2Status::Active,
            Some("waiting") => Aria2Status::Waiting,
            Some("paused") => Aria2Status::Paused,
            Some("complete") => Aria2Status::Complete,
            Some("error") => Aria2Status::Error,
            Some("removed") => Aria2Status::Removed,
            _ => return Err(NativeApiError::InvalidResponse("unknown task status")),
        };
        Ok(TaskStatus {
            gid,
            status,
            total_length: decimal_field(&value, "totalLength")?,
            completed_length: decimal_field(&value, "completedLength")?,
        })
    }

    pub async fn pause(&self, gid: Gid) -> Result<(), NativeApiError> {
        self.control("aria2.pause", gid).await
    }

    pub async fn resume(&self, gid: Gid) -> Result<(), NativeApiError> {
        self.control("aria2.unpause", gid).await
    }

    pub async fn remove(&self, gid: Gid) -> Result<(), NativeApiError> {
        self.control("aria2.remove", gid).await
    }

    pub async fn options(&self, gid: Gid) -> Result<BTreeMap<String, String>, NativeApiError> {
        let value = self
            .call_control("aria2.getOption", json!([gid.to_string()]))
            .await?;
        value
            .as_object()
            .ok_or(NativeApiError::InvalidResponse(
                "options response is not an object",
            ))
            .map(|object| {
                object
                    .iter()
                    .filter_map(|(name, value)| {
                        value.as_str().map(|value| (name.clone(), value.to_owned()))
                    })
                    .collect()
            })
    }

    pub fn subscribe(
        &self,
        limits: RpcEventLimits,
    ) -> Result<NativeEventSubscription, NativeApiError> {
        self.events
            .subscribe(limits)
            .map(|subscriber| NativeEventSubscription { subscriber })
            .map_err(NativeApiError::Event)
    }

    pub async fn shutdown(self) -> Result<(), NativeApiError> {
        let _ = self
            .call_control("aria2.shutdown", Value::Array(Vec::new()))
            .await;
        if let Some(progress) = self.progress.lock().await.take() {
            progress.abort();
            let _ = progress.await;
        }
        let plane = Arc::try_unwrap(self.plane).map_err(|_| {
            NativeApiError::Shutdown("control plane is still referenced".to_owned())
        })?;
        let plane = plane.into_inner();
        plane
            .shutdown_async()
            .await
            .map_err(|error| NativeApiError::Shutdown(error.to_string()))?;
        Ok(())
    }

    async fn control(&self, method: &str, gid: Gid) -> Result<(), NativeApiError> {
        self.call_control(method, json!([gid.to_string()])).await?;
        Ok(())
    }

    async fn call_control(&self, method: &str, params: Value) -> Result<Value, NativeApiError> {
        self.plane
            .lock()
            .await
            .call(method, params)
            .map_err(NativeApiError::Control)
    }
}

pub struct NativeEventSubscription {
    subscriber: RpcEventSubscriber,
}

impl NativeEventSubscription {
    pub fn try_next(&mut self) -> Result<Option<Value>, NativeApiError> {
        self.subscriber
            .try_next()
            .map(|delivery| delivery.map(|delivery| delivery.into_value()))
            .map_err(NativeApiError::Event)
    }
}

fn decimal_field(value: &Value, name: &'static str) -> Result<u64, NativeApiError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .ok_or(NativeApiError::InvalidResponse(name))
}

fn process_config(
    database_path: PathBuf,
    control_directory: PathBuf,
    output_root: PathBuf,
) -> Result<ProcessBootstrapConfig, NativeApiError> {
    let now_wall_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or(NativeApiError::InvalidConfiguration(
            "system clock is before Unix epoch",
        ))?;
    let task_capacity = NonZeroUsize::new(1024).expect("task capacity");
    let active_capacity = NonZeroUsize::new(64).expect("active capacity");
    let runtime_capacity = NonZeroUsize::new(1024).expect("runtime capacity");
    let plan_capacity = NonZeroUsize::new(64).expect("plan capacity");
    let max_wait_ms = NonZeroU64::new(86_400_000).expect("max wait");
    let scheduler = SchedulerConfig::new(task_capacity, active_capacity, false)
        .map_err(|_| NativeApiError::InvalidConfiguration("invalid scheduler bounds"))?;
    Ok(ProcessBootstrapConfig {
        session_owner: SessionOwnerConfig::new(database_path),
        control_directory,
        allowed_output_roots: vec![output_root],
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
        shutdown_step_timeout_ms: crate::DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS,
        updated_ms: now_wall_unix_ms,
        recovery_created_at_unix_ms: now_wall_unix_ms,
    })
}

fn persisted_option_is_safe(name: &str) -> bool {
    builtin_registry()
        .find(name)
        .is_some_and(|definition| definition.security == SecurityClass::Normal)
}

fn create_private_directory(
    path: &std::path::Path,
    error: &'static str,
) -> Result<(), NativeApiError> {
    #[cfg(windows)]
    {
        ariax_windows_security::create_private_directory(path)
            .map_err(|_| NativeApiError::InvalidConfiguration(error))
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::create_dir_all(path).map_err(|_| NativeApiError::InvalidConfiguration(error))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| NativeApiError::InvalidConfiguration(error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn builder_requires_absolute_output_root() {
        let error = Engine::builder()
            .output_root("relative")
            .build()
            .await
            .expect_err("relative output root must reject");
        assert!(matches!(error, NativeApiError::InvalidConfiguration(_)));
    }

    #[tokio::test]
    async fn builder_adds_and_queries_a_paused_task() {
        let root = std::env::temp_dir().join(format!("ariax-native-api-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                .expect("private root permissions");
        }
        let engine = Engine::builder()
            .output_root(&root)
            .build()
            .await
            .expect("build engine");
        let gid = engine
            .add_uri(AddUri {
                uris: vec!["http://example.test/file.bin".to_owned()],
                options: DownloadOptions {
                    pause: true,
                    ..DownloadOptions::default()
                },
            })
            .await
            .expect("add URI");
        let status = engine.status(gid).await.expect("status");
        assert_eq!(status.status, Aria2Status::Paused);
        engine.shutdown().await.expect("shutdown");
        let _ = fs::remove_dir_all(root);
    }
}
