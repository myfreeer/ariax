//! Typed Rust embedding facade over the process-owned HTTP control plane.

mod bittorrent;
pub use bittorrent::{
    AddMagnet, AddTorrent, BitTorrentConfig, BitTorrentEncryption, BitTorrentOptions,
    BitTorrentPeer, BitTorrentStatus,
};

use crate::{
    HttpControlError, HttpControlPlane, HttpControlPlaneConfig, HttpCookieJar, HttpCookieLimits,
    HttpDestinationPolicy, HttpMultiRangeWorker, HttpPolicyClient, HttpProcessResources,
    HttpResolver, HttpResolverConfig, HttpWorkerSupervisorConfig, ProcessBootstrapConfig,
    RpcClientBudget, RpcClientContext, RpcEventBroker, RpcEventError, RpcEventLimits,
    RpcEventSubscriber, RuntimeEffectConfig, StartupRecoveryConfig,
};
use ariax_config::persisted_option_is_safe;
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

#[derive(Clone, Debug, Default)]
pub struct DownloadOptions {
    pub pause: bool,
    pub output: Option<String>,
    pub split: Option<NonZeroUsize>,
    pub timeout_seconds: Option<u64>,
    pub max_download_limit: Option<u64>,
    pub max_connections_per_server: Option<NonZeroUsize>,
    pub min_split_size: Option<u64>,
    pub piece_length: Option<u64>,
    pub connect_timeout_seconds: Option<u64>,
    pub lowest_speed_limit: Option<u64>,
    pub endgame_max_duplicates: Option<usize>,
    pub checksum: Option<crate::HttpContentChecksum>,
    /// Protocol-neutral options, including all four supported content digests.
    pub transfer: Option<crate::TransferOptions>,
    pub mirror_identity: Option<crate::HttpMirrorIdentityPolicy>,
    pub retry: Option<crate::HttpRetryPolicy>,
}

impl DownloadOptions {
    /// Parses local download settings through the shared admission registry.
    pub fn from_pairs(
        settings: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, NativeApiError> {
        let mut values = serde_json::Map::new();
        let mut bytes = 0usize;
        for (name, value) in settings {
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(value.len())
                .saturating_add(128);
            if bytes > crate::MAX_HTTP_RPC_REQUEST_BYTES
                || values.len() >= ariax_storage::MAX_OPTION_MAP_ENTRIES
                || name == "dir"
                || values.insert(name, Value::String(value)).is_some()
            {
                return Err(NativeApiError::InvalidConfiguration(
                    "duplicate, oversized or unsupported download option",
                ));
            }
        }
        let root = std::env::current_dir().map_err(|_| {
            NativeApiError::InvalidConfiguration("cannot resolve local output root")
        })?;
        let (parsed, _, _, paused) = crate::http_control::parse_add_options_authorized(
            &Value::Object(values.clone()),
            &root,
            &["https://options.invalid/download".to_owned()],
            true,
        )
        .map_err(NativeApiError::Control)?;
        Ok(Self {
            pause: paused,
            output: values.get("out").and_then(Value::as_str).map(str::to_owned),
            split: values.contains_key("split").then_some(parsed.split),
            timeout_seconds: values
                .contains_key("timeout")
                .then_some(parsed.response_body_timeout.as_secs()),
            max_download_limit: values
                .contains_key("max-download-limit")
                .then_some(parsed.max_download_limit),
            max_connections_per_server: values
                .contains_key("max-connection-per-server")
                .then_some(parsed.max_connections_per_server),
            min_split_size: values
                .contains_key("min-split-size")
                .then_some(parsed.min_split_size),
            piece_length: values
                .contains_key("piece-length")
                .then_some(parsed.piece_length),
            connect_timeout_seconds: values
                .contains_key("connect-timeout")
                .then_some(parsed.connect_timeout.as_secs()),
            lowest_speed_limit: values
                .contains_key("lowest-speed-limit")
                .then_some(parsed.lowest_speed_limit),
            endgame_max_duplicates: values
                .contains_key("endgame-max-duplicates")
                .then_some(parsed.endgame_max_duplicates),
            checksum: parsed.checksum,
            transfer: Some(parsed.transfer),
            mirror_identity: values
                .contains_key("verify-mirror-identity")
                .then_some(parsed.mirror_identity),
            retry: parsed.retry,
        })
    }

    fn input_bytes(&self) -> usize {
        // Includes canonical retry fields, JSON nodes and bounded conversion scratch.
        (64 * 1024_usize)
            .saturating_add(self.output.as_ref().map_or(0, String::capacity))
            .saturating_add(
                self.transfer
                    .as_ref()
                    .map_or(0, |options| options.retained_bytes().saturating_mul(3)),
            )
    }

    fn into_value(self, admission: bool) -> Result<Value, NativeApiError> {
        if !admission && self.pause {
            return Err(NativeApiError::InvalidConfiguration(
                "use pause() to change pause intent",
            ));
        }
        let mut options = serde_json::Map::new();
        if admission {
            options.insert("pause".to_owned(), Value::Bool(self.pause));
        }
        if let Some(output) = self.output {
            options.insert("out".to_owned(), Value::String(output));
        }
        for (name, value) in [
            ("split", self.split.map(|value| value.get() as u64)),
            (
                "max-connection-per-server",
                self.max_connections_per_server
                    .map(|value| value.get() as u64),
            ),
            ("min-split-size", self.min_split_size),
            ("piece-length", self.piece_length),
            ("connect-timeout", self.connect_timeout_seconds),
            ("timeout", self.timeout_seconds),
            ("max-download-limit", self.max_download_limit),
            ("lowest-speed-limit", self.lowest_speed_limit),
            (
                "endgame-max-duplicates",
                self.endgame_max_duplicates.map(|value| value as u64),
            ),
        ] {
            if let Some(value) = value {
                options.insert(name.to_owned(), Value::from(value));
            }
        }
        if let Some(checksum) = self.checksum {
            options.insert("checksum".to_owned(), Value::String(checksum.canonical()));
        }
        if let Some(transfer) = self.transfer {
            transfer
                .validate()
                .map_err(|error| NativeApiError::Control(HttpControlError::TaskSpec(error)))?;
            for (name, value) in transfer.persisted() {
                if !matches!(
                    name.as_str(),
                    "verification-manifest" | "metalink-expansion"
                ) {
                    options.insert(name, Value::String(value));
                }
            }
            if let Some(checksum) = transfer.checksum {
                if options.contains_key("checksum") {
                    return Err(NativeApiError::InvalidConfiguration(
                        "only one user checksum may be supplied",
                    ));
                }
                options.insert("checksum".into(), Value::String(checksum.canonical()));
            }
            if let Some(credentials) = transfer.credentials {
                options.insert(
                    "ftp-user".into(),
                    Value::String(credentials.username.to_string()),
                );
                if let Some(password) = credentials.password {
                    options.insert("ftp-passwd".into(), Value::String(password.to_string()));
                }
            }
            for (name, path) in [
                ("sftp-known-hosts", transfer.sftp_known_hosts),
                ("sftp-private-key", transfer.sftp_private_key),
                ("netrc-path", transfer.netrc_path),
            ] {
                if let Some(path) = path {
                    options.insert(
                        name.into(),
                        Value::String(
                            path.to_str()
                                .ok_or(NativeApiError::InvalidConfiguration(
                                    "credential paths must be UTF-8",
                                ))?
                                .to_owned(),
                        ),
                    );
                }
            }
            if let Some(passphrase) = transfer.sftp_private_key_passphrase {
                options.insert(
                    "sftp-private-key-passphrase".into(),
                    Value::String(passphrase.expose().to_owned()),
                );
            }
            if transfer.sftp_use_agent {
                options.insert("sftp-use-agent".into(), Value::Bool(true));
            }
            if !transfer.sftp_check_host_key {
                options.insert("sftp-check-host-key".into(), Value::Bool(false));
            }
            if transfer.ftp_pasv_server_address {
                options.insert("ftp-pasv-address".into(), Value::String("server".into()));
            }
        }
        if let Some(policy) = self.mirror_identity {
            options.insert(
                "verify-mirror-identity".to_owned(),
                Value::String(policy.code().to_owned()),
            );
        }
        if let Some(retry) = self.retry {
            let canonical = crate::HttpTaskOptions {
                retry: Some(retry),
                ..crate::HttpTaskOptions::default()
            }
            .sanitized()
            .map_err(|error| NativeApiError::Control(HttpControlError::TaskSpec(error)))?;
            options.extend(
                canonical
                    .entries()
                    .filter(|(name, _)| {
                        name.starts_with("retry-")
                            || matches!(*name, "max-tries" | "stale-validator-policy")
                    })
                    .map(|(name, value)| (name.to_owned(), Value::String(value.to_owned()))),
            );
        }
        Ok(Value::Object(options))
    }
}

#[derive(Clone, Debug, Default)]
pub struct GlobalOptions {
    pub task_defaults: DownloadOptions,
    pub max_overall_download_limit: Option<u64>,
    pub max_overall_upload_limit: Option<u32>,
    pub bittorrent_defaults: Option<BitTorrentOptions>,
    pub scheduling: Option<crate::SlowSlotConfig>,
}

#[derive(Clone, Debug, Default)]
pub struct ConfigurationUpdate {
    pub text: String,
    pub url_rules: Option<String>,
    pub expected_generation: Option<u64>,
    pub allow_known_unsupported: bool,
}

impl ConfigurationUpdate {
    fn into_params(self) -> Result<Value, NativeApiError> {
        if self
            .text
            .len()
            .saturating_add(self.url_rules.as_ref().map_or(0, String::len))
            > crate::MAX_HTTP_RPC_REQUEST_BYTES
        {
            return Err(NativeApiError::InvalidConfiguration(
                "configuration input exceeds its byte limit",
            ));
        }
        let mut settings = serde_json::Map::new();
        if let Some(rules) = self.url_rules {
            settings.insert("urlRules".to_owned(), Value::String(rules));
        }
        if let Some(generation) = self.expected_generation {
            settings.insert("expectedGeneration".to_owned(), Value::from(generation));
        }
        settings.insert(
            "compatibility".to_owned(),
            json!(if self.allow_known_unsupported {
                "aria2"
            } else {
                "strict"
            }),
        );
        Ok(Value::Array(vec![
            Value::String(self.text),
            Value::Object(settings),
        ]))
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigurationReport {
    pub config_generation: u64,
    pub options: usize,
    pub warnings: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum ConfigDumpMode {
    Defaults,
    Effective,
    TaskEffective(Gid),
    UrlRules,
}

#[derive(Clone, Copy, Debug)]
pub enum ConfigDumpFormat {
    Flat,
    Json,
    Toml,
}

#[derive(Clone, Copy, Debug)]
pub enum PositionOrigin {
    Start,
    Current,
    End,
}

#[derive(Clone, Debug)]
pub struct AddUri {
    pub uris: Vec<String>,
    pub options: DownloadOptions,
}

#[derive(Clone, Debug, Default)]
pub struct MetalinkSelection {
    pub base_uri: Option<String>,
    pub select_file: Option<String>,
    pub language: Option<String>,
    pub os: Option<String>,
    pub version: Option<String>,
    pub location: Option<String>,
    pub preferred_protocol: Option<crate::TransferProtocol>,
    pub unique_protocol: Option<bool>,
}

pub struct AddMetalink {
    pub bytes: Vec<u8>,
    pub options: DownloadOptions,
    pub selection: MetalinkSelection,
    pub position: Option<i64>,
}
impl fmt::Debug for AddMetalink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AddMetalink")
            .field("bytes", &self.bytes.len())
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct ApproveHostKey {
    pub gid: Gid,
    pub challenge: ariax_core::HostKeyChallengeId,
    pub fingerprint_sha256: ariax_core::HostKeyFingerprint,
}
impl ApproveHostKey {
    pub fn from_text(gid: Gid, challenge: &str, fingerprint: &str) -> Result<Self, NativeApiError> {
        Ok(Self {
            gid,
            challenge: ariax_core::HostKeyChallengeId::new(
                crate::transfer_task::parse_hex_bytes(challenge).map_err(|_| {
                    NativeApiError::InvalidConfiguration(
                        "challenge id must be 32 hexadecimal characters",
                    )
                })?,
            ),
            fingerprint_sha256: ariax_core::HostKeyFingerprint::new(
                crate::transfer_task::parse_host_key_fingerprint(fingerprint).map_err(|_| {
                    NativeApiError::InvalidConfiguration("invalid SHA-256 host-key fingerprint")
                })?,
            ),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SshConnectionStatus {
    pub source: u32,
    pub kex: String,
    pub host_key: String,
    pub cipher: String,
    pub client_mac: String,
    pub server_mac: String,
    pub insecure_host_key: bool,
    pub legacy_host_key_digest: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskStatus {
    pub gid: Gid,
    pub status: Aria2Status,
    pub total_length: u64,
    pub completed_length: u64,
    pub bittorrent: Option<BitTorrentStatus>,
    pub host_key_challenge: Option<ariax_core::HostKeyChallenge>,
    pub followed_by: Vec<Gid>,
    pub ssh_connection: Option<SshConnectionStatus>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UriUsage {
    Used,
    Waiting,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
pub struct DownloadUri {
    pub uri: String,
    pub status: UriUsage,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadFile {
    #[serde(deserialize_with = "decimal_u64")]
    pub index: u64,
    pub path: String,
    #[serde(deserialize_with = "decimal_u64")]
    pub length: u64,
    #[serde(deserialize_with = "decimal_u64")]
    pub completed_length: u64,
    #[serde(deserialize_with = "text_bool")]
    pub selected: bool,
    pub uris: Vec<DownloadUri>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadServer {
    pub uri: String,
    pub current_uri: String,
    #[serde(deserialize_with = "decimal_u64")]
    pub download_speed: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct DownloadServers {
    #[serde(deserialize_with = "decimal_u64")]
    pub index: u64,
    pub servers: Vec<DownloadServer>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalStatistics {
    #[serde(deserialize_with = "decimal_u64")]
    pub download_speed: u64,
    #[serde(deserialize_with = "decimal_u64")]
    pub upload_speed: u64,
    #[serde(deserialize_with = "decimal_u64")]
    pub num_active: u64,
    #[serde(deserialize_with = "decimal_u64")]
    pub num_waiting: u64,
    #[serde(deserialize_with = "decimal_u64")]
    pub num_stopped: u64,
    #[serde(deserialize_with = "decimal_u64")]
    pub completed_length: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineVersion {
    pub version: String,
    pub enabled_features: Vec<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineSession {
    pub session_id: String,
}

fn decimal_u64<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = <String as serde::Deserialize>::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}
fn text_bool<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    let value = <String as serde::Deserialize>::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
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
    session_export: Option<crate::SessionExportConfig>,
    input_file: Option<(PathBuf, crate::SessionFormat)>,
    bittorrent: Option<BitTorrentConfig>,
}

impl EngineBuilder {
    #[must_use]
    pub fn bittorrent(mut self, config: BitTorrentConfig) -> Self {
        self.bittorrent = Some(config);
        self
    }
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

    #[must_use]
    pub fn session_export(mut self, config: crate::SessionExportConfig) -> Self {
        self.session_export = Some(config);
        self
    }

    #[must_use]
    pub fn input_file(mut self, path: impl Into<PathBuf>, format: crate::SessionFormat) -> Self {
        self.input_file = Some((path.into(), format));
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
        plane
            .attach_process_resources(resources.clone())
            .map_err(|error| NativeApiError::Bootstrap(error.to_string()))?;
        if let Some(config) = self.bittorrent {
            config.apply(&mut plane)?;
        }
        if let Some(config) = self.session_export {
            plane
                .configure_session_export(config)
                .map_err(NativeApiError::Control)?;
        }
        if let Some((path, format)) = self.input_file {
            plane
                .import_session_file(&path, format)
                .map_err(NativeApiError::Control)?;
        }
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
        let client = resources
            .rpc_budgets()
            .client()
            .map_err(|_| NativeApiError::Control(HttpControlError::Busy))?;
        let backend = crate::HttpControlBackend::new(plane);
        let plane = backend.plane();
        let control = backend.control_runtime();
        control.start().map_err(NativeApiError::Control)?;
        Ok(Engine {
            control,
            plane,
            events,
            client,
        })
    }
}

pub struct Engine {
    control: crate::http_control::control_runtime::ControlRuntime,
    plane: Arc<Mutex<HttpControlPlane>>,
    events: RpcEventBroker,
    client: RpcClientBudget,
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

    pub async fn add_metalink(&self, request: AddMetalink) -> Result<Vec<Gid>, NativeApiError> {
        use base64ct::Encoding;
        if request.bytes.len() > crate::MAX_METALINK_DOCUMENT_BYTES {
            return Err(NativeApiError::InvalidConfiguration(
                "Metalink exceeds document limit",
            ));
        }
        let lease = self.client.try_request(0).map_err(native_budget_error)?;
        lease
            .reserve(
                request
                    .bytes
                    .capacity()
                    .saturating_add(request.bytes.len().saturating_mul(4).div_ceil(3))
                    .saturating_add(256 * 1024),
            )
            .map_err(native_budget_error)?;
        let mut options = request.options.into_value(true)?;
        let object = options.as_object_mut().expect("download options");
        for (name, value) in [
            ("metalink-base-uri", request.selection.base_uri),
            ("select-file", request.selection.select_file),
            ("metalink-language", request.selection.language),
            ("metalink-os", request.selection.os),
            ("metalink-version", request.selection.version),
            ("metalink-location", request.selection.location),
        ] {
            if let Some(value) = value {
                object.insert(name.into(), Value::String(value));
            }
        }
        if let Some(protocol) = request.selection.preferred_protocol {
            object.insert(
                "metalink-preferred-protocol".into(),
                Value::String(protocol.code().into()),
            );
        }
        if let Some(unique) = request.selection.unique_protocol {
            object.insert(
                "metalink-enable-unique-protocol".into(),
                Value::Bool(unique),
            );
        }
        let value = self
            .call_control_admitted(
                "aria2.addMetalink",
                json!([
                    base64ct::Base64::encode_string(&request.bytes),
                    options,
                    request.position.unwrap_or(-1)
                ]),
                lease,
            )
            .await?;
        value
            .as_array()
            .ok_or(NativeApiError::InvalidResponse(
                "addMetalink must return GIDs",
            ))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .ok_or(NativeApiError::InvalidResponse("invalid Metalink GID"))
            })
            .collect()
    }

    pub async fn approve_host_key(&self, request: ApproveHostKey) -> Result<(), NativeApiError> {
        self.call_control(
            "ariax.approveHostKey",
            json!([
                request.gid.to_string(),
                crate::transfer_task::hex_bytes(request.challenge.as_bytes()),
                ariax_storage::session_host_key_pin_value(request.fingerprint_sha256)
            ]),
        )
        .await?;
        Ok(())
    }

    pub async fn add_uri(&self, request: AddUri) -> Result<Gid, NativeApiError> {
        if request.uris.is_empty() || request.uris.len() > crate::MAX_HTTP_TASK_SOURCES {
            return Err(NativeApiError::InvalidConfiguration(
                "URI count is outside the supported bound",
            ));
        }
        let lease = self.client.try_request(0).map_err(native_budget_error)?;
        let input_bytes = request
            .uris
            .iter()
            .map(|uri| uri.capacity().saturating_add(1024))
            .fold(
                (64 * 1024_usize).saturating_add(
                    request
                        .uris
                        .capacity()
                        .saturating_mul(std::mem::size_of::<String>()),
                ),
                usize::saturating_add,
            )
            .saturating_add(request.options.output.as_ref().map_or(0, String::capacity));
        lease.reserve(input_bytes).map_err(native_budget_error)?;
        let options = request.options.into_value(true)?;
        let value = self
            .call_control_admitted(
                "aria2.addUri",
                Value::Array(vec![
                    Value::Array(request.uris.into_iter().map(Value::String).collect()),
                    options,
                ]),
                lease,
            )
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
        task_status(&value)
    }

    pub async fn active(&self) -> Result<Vec<TaskStatus>, NativeApiError> {
        self.status_list("aria2.tellActive", json!([])).await
    }
    pub async fn waiting(
        &self,
        offset: i64,
        count: usize,
    ) -> Result<Vec<TaskStatus>, NativeApiError> {
        self.status_list("aria2.tellWaiting", json!([offset, count]))
            .await
    }
    pub async fn stopped(
        &self,
        offset: i64,
        count: usize,
    ) -> Result<Vec<TaskStatus>, NativeApiError> {
        self.status_list("aria2.tellStopped", json!([offset, count]))
            .await
    }
    async fn status_list(
        &self,
        method: &str,
        params: Value,
    ) -> Result<Vec<TaskStatus>, NativeApiError> {
        let result = self.call_control(method, params).await?;
        result
            .as_array()
            .ok_or(NativeApiError::InvalidResponse("expected status list"))?
            .iter()
            .map(task_status)
            .collect()
    }
    pub async fn uris(&self, gid: Gid) -> Result<Vec<DownloadUri>, NativeApiError> {
        self.decode_control("aria2.getUris", json!([gid.to_string()]))
            .await
    }
    pub async fn files(&self, gid: Gid) -> Result<Vec<DownloadFile>, NativeApiError> {
        self.decode_control("aria2.getFiles", json!([gid.to_string()]))
            .await
    }
    pub async fn servers(&self, gid: Gid) -> Result<Vec<DownloadServers>, NativeApiError> {
        self.decode_control("aria2.getServers", json!([gid.to_string()]))
            .await
    }
    pub async fn global_statistics(&self) -> Result<GlobalStatistics, NativeApiError> {
        self.decode_control("aria2.getGlobalStat", json!([])).await
    }
    pub async fn version(&self) -> Result<EngineVersion, NativeApiError> {
        self.decode_control("aria2.getVersion", json!([])).await
    }
    pub async fn session_info(&self) -> Result<EngineSession, NativeApiError> {
        self.decode_control("aria2.getSessionInfo", json!([])).await
    }
    async fn decode_control<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T, NativeApiError> {
        let result = self.call_control(method, params).await?;
        serde_json::from_value(result.value.clone())
            .map_err(|_| NativeApiError::InvalidResponse("invalid typed query result"))
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

    pub async fn force_pause(&self, gid: Gid) -> Result<(), NativeApiError> {
        self.control("aria2.forcePause", gid).await
    }
    pub async fn force_remove(&self, gid: Gid) -> Result<(), NativeApiError> {
        self.control("aria2.forceRemove", gid).await
    }
    pub async fn remove_result(&self, gid: Gid) -> Result<(), NativeApiError> {
        self.control("aria2.removeDownloadResult", gid).await
    }

    pub async fn pause_all(&self, force: bool) -> Result<(), NativeApiError> {
        self.call_control(
            if force {
                "aria2.forcePauseAll"
            } else {
                "aria2.pauseAll"
            },
            json!([]),
        )
        .await?;
        Ok(())
    }
    pub async fn resume_all(&self) -> Result<(), NativeApiError> {
        self.call_control("aria2.unpauseAll", json!([])).await?;
        Ok(())
    }
    pub async fn purge_results(&self) -> Result<(), NativeApiError> {
        self.call_control("aria2.purgeDownloadResult", json!([]))
            .await?;
        Ok(())
    }

    pub async fn change_options(
        &self,
        gid: Gid,
        options: DownloadOptions,
        restart: bool,
    ) -> Result<(), NativeApiError> {
        let lease = self
            .client
            .try_request(options.input_bytes())
            .map_err(native_budget_error)?;
        self.call_control_admitted(
            "aria2.changeOption",
            Value::Array(vec![
                Value::String(gid.to_string()),
                options.into_value(false)?,
                json!({"restart":restart}),
            ]),
            lease,
        )
        .await?;
        Ok(())
    }

    pub async fn change_global_options(
        &self,
        options: GlobalOptions,
    ) -> Result<(), NativeApiError> {
        let lease = self
            .client
            .try_request(options.task_defaults.input_bytes())
            .map_err(native_budget_error)?;
        let mut values = options.task_defaults.into_value(false)?;
        if let Some(limit) = options.max_overall_download_limit {
            values
                .as_object_mut()
                .expect("option object")
                .insert("max-overall-download-limit".to_owned(), Value::from(limit));
        }
        if let Some(limit) = options.max_overall_upload_limit {
            values
                .as_object_mut()
                .expect("option object")
                .insert("max-overall-upload-limit".into(), json!(limit));
        }
        if let Some(defaults) = options.bittorrent_defaults {
            lease
                .reserve(defaults.input_bytes())
                .map_err(native_budget_error)?;
            values
                .as_object_mut()
                .expect("option object")
                .extend(defaults.value().as_object().expect("BT options").clone());
        }
        if let Some(scheduling) = options.scheduling {
            scheduling.validate().map_err(NativeApiError::Control)?;
            values.as_object_mut().expect("option object").extend(
                scheduling
                    .options()
                    .into_iter()
                    .map(|(name, value)| (name, Value::String(value))),
            );
        }
        self.call_control_admitted(
            "aria2.changeGlobalOption",
            Value::Array(vec![values]),
            lease,
        )
        .await?;
        Ok(())
    }

    pub async fn replace_sources(&self, gid: Gid, uris: Vec<String>) -> Result<(), NativeApiError> {
        if uris.is_empty() || uris.len() > crate::MAX_HTTP_TASK_SOURCES {
            return Err(NativeApiError::InvalidConfiguration(
                "URI count is outside the supported bound",
            ));
        }
        let bytes = uris.iter().fold(
            (64 * 1024_usize).saturating_add(
                uris.capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            ),
            |bytes, uri| bytes.saturating_add(uri.capacity()).saturating_add(1024),
        );
        let lease = self
            .client
            .try_request(bytes)
            .map_err(native_budget_error)?;
        self.call_control_admitted(
            "ariax.replaceSources",
            Value::Array(vec![
                Value::String(gid.to_string()),
                Value::Array(uris.into_iter().map(Value::String).collect()),
            ]),
            lease,
        )
        .await?;
        Ok(())
    }

    pub async fn change_position(
        &self,
        gid: Gid,
        offset: i64,
        origin: PositionOrigin,
    ) -> Result<usize, NativeApiError> {
        let mode = match origin {
            PositionOrigin::Start => "POS_SET",
            PositionOrigin::Current => "POS_CUR",
            PositionOrigin::End => "POS_END",
        };
        let result = self
            .call_control(
                "aria2.changePosition",
                json!([gid.to_string(), offset, mode]),
            )
            .await?;
        result
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(NativeApiError::InvalidResponse("invalid queue position"))
    }

    pub async fn check_config(
        &self,
        update: ConfigurationUpdate,
    ) -> Result<ConfigurationReport, NativeApiError> {
        self.configuration("ariax.checkConfig", update).await
    }
    pub async fn reload_config(
        &self,
        update: ConfigurationUpdate,
    ) -> Result<ConfigurationReport, NativeApiError> {
        self.configuration("ariax.reloadConfig", update).await
    }
    async fn configuration(
        &self,
        method: &str,
        update: ConfigurationUpdate,
    ) -> Result<ConfigurationReport, NativeApiError> {
        let bytes = update
            .text
            .capacity()
            .saturating_add(update.url_rules.as_ref().map_or(0, String::capacity))
            .saturating_add(64 * 1024);
        let lease = self
            .client
            .try_request(bytes)
            .map_err(native_budget_error)?;
        let result = self
            .call_control_admitted(method, update.into_params()?, lease)
            .await?;
        serde_json::from_value(result.value.clone())
            .map_err(|_| NativeApiError::InvalidResponse("invalid configuration report"))
    }

    pub async fn dump_config(
        &self,
        mode: ConfigDumpMode,
        format: ConfigDumpFormat,
    ) -> Result<String, NativeApiError> {
        let (mode, gid) = match mode {
            ConfigDumpMode::Defaults => ("defaults", None),
            ConfigDumpMode::Effective => ("effective", None),
            ConfigDumpMode::TaskEffective(gid) => ("task-effective", Some(gid)),
            ConfigDumpMode::UrlRules => ("url-rules", None),
        };
        let format = match format {
            ConfigDumpFormat::Flat => "flat",
            ConfigDumpFormat::Json => "json",
            ConfigDumpFormat::Toml => "toml",
        };
        let mut params = vec![json!(mode), json!(format)];
        if let Some(gid) = gid {
            params.push(json!(gid.to_string()));
        }
        let result = self
            .call_control("ariax.dumpConfig", Value::Array(params))
            .await?;
        if let Some(text) = result.as_str() {
            Ok(text.to_owned())
        } else {
            serde_json::to_string(&result.value)
                .map_err(|_| NativeApiError::InvalidResponse("invalid configuration dump"))
        }
    }

    pub async fn diagnostics(&self) -> Result<crate::ControlDiagnostics, NativeApiError> {
        let result = self.call_control("ariax.getDiagnostics", json!([])).await?;
        serde_json::from_value(result.value.clone())
            .map_err(|_| NativeApiError::InvalidResponse("invalid diagnostics"))
    }

    /// Explicit JSON compatibility entry point; returned bytes retain the response permit.
    pub async fn rpc_json(
        &self,
        request: &[u8],
        compatibility: crate::RpcCompatibility,
    ) -> Result<bytes::Bytes, NativeApiError> {
        if request.len() > crate::MAX_HTTP_RPC_REQUEST_BYTES {
            return Err(NativeApiError::InvalidConfiguration(
                "RPC request exceeds its byte limit",
            ));
        }
        let lease = self
            .client
            .try_request(request.len())
            .map_err(native_budget_error)?;
        let backend = crate::HttpControlBackend::from_shared(self.plane.clone()).await;
        let dispatcher =
            crate::RpcDispatcher::new(Arc::new(backend), crate::RpcAuthPolicy::default())
                .with_compatibility(compatibility);
        Ok(crate::http_rpc::dispatch_admitted_json(
            &dispatcher,
            request,
            &RpcClientContext::default(),
            &self.client,
            lease,
        )
        .await)
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

    pub async fn save_session(&self) -> Result<(), NativeApiError> {
        self.call_control("aria2.saveSession", json!([])).await?;
        Ok(())
    }

    pub async fn export_session(
        &self,
        format: crate::SessionFormat,
    ) -> Result<String, NativeApiError> {
        let result = self.call_control("ariax.exportSession", json!([])).await?;
        let _bytes = self
            .client
            .charge(crate::MAX_SESSION_DOCUMENT_BYTES)
            .map_err(native_budget_error)?;
        let bytes =
            crate::session_file::render(&result, format).map_err(NativeApiError::Control)?;
        String::from_utf8(bytes)
            .map_err(|_| NativeApiError::InvalidResponse("session export is not UTF-8"))
    }

    pub async fn import_session(
        &self,
        document: String,
        format: crate::SessionFormat,
    ) -> Result<Vec<Gid>, NativeApiError> {
        if document.len() > crate::MAX_SESSION_DOCUMENT_BYTES {
            return Err(NativeApiError::InvalidConfiguration(
                "session input exceeds its byte limit",
            ));
        }
        let result = self
            .call_control("ariax.importSession", json!([document, format.as_str()]))
            .await?;
        result
            .as_array()
            .ok_or(NativeApiError::InvalidResponse(
                "session import did not return GIDs",
            ))?
            .iter()
            .map(|value| {
                value.as_str().and_then(|value| value.parse().ok()).ok_or(
                    NativeApiError::InvalidResponse("session import returned an invalid GID"),
                )
            })
            .collect()
    }

    pub fn subscribe(
        &self,
        limits: RpcEventLimits,
    ) -> Result<NativeEventSubscription, NativeApiError> {
        self.subscribe_filtered(limits, crate::RpcEventFilter::default())
    }

    pub fn subscribe_filtered(
        &self,
        limits: RpcEventLimits,
        filter: crate::RpcEventFilter,
    ) -> Result<NativeEventSubscription, NativeApiError> {
        self.events
            .subscribe_filtered_with_client(limits, filter, self.client.clone())
            .map(|subscriber| NativeEventSubscription {
                subscriber,
                client: self.client.clone(),
            })
            .map_err(NativeApiError::Event)
    }

    pub async fn shutdown(self) -> Result<(), NativeApiError> {
        let _ = self
            .call_control("aria2.shutdown", Value::Array(Vec::new()))
            .await;
        let drained = self.control.drain().await;
        let plane = Arc::try_unwrap(self.plane).map_err(|_| {
            NativeApiError::Shutdown("control plane is still referenced".to_owned())
        })?;
        let plane = plane.into_inner();
        let report = plane
            .shutdown_async()
            .await
            .map_err(|error| NativeApiError::Shutdown(error.to_string()))?;
        drained.map_err(NativeApiError::Control)?;
        if !report.is_clean() {
            return Err(NativeApiError::Shutdown(
                "engine did not complete a clean shutdown".to_owned(),
            ));
        }
        Ok(())
    }

    async fn control(&self, method: &str, gid: Gid) -> Result<(), NativeApiError> {
        self.call_control(method, json!([gid.to_string()])).await?;
        Ok(())
    }

    async fn call_control(
        &self,
        method: &str,
        params: Value,
    ) -> Result<NativeResult, NativeApiError> {
        let lease = self.client.try_request(0).map_err(native_budget_error)?;
        lease
            .reserve(crate::rpc_json::owned_value_bytes(&params))
            .map_err(native_budget_error)?;
        self.call_control_admitted(method, params, lease).await
    }

    async fn call_control_admitted(
        &self,
        method: &str,
        params: Value,
        lease: crate::rpc_budget::RpcRequestLease,
    ) -> Result<NativeResult, NativeApiError> {
        let response = self
            .client
            .response(Some(lease.clone()))
            .map_err(native_budget_error)?;
        let workspace = response.workspace().map_err(native_budget_error)?;
        let context = RpcClientContext::local().with_request(lease);
        let value = self
            .control
            .call(method, params, context)
            .await
            .map_err(NativeApiError::Control)?;
        Ok(NativeResult {
            value,
            _response: response,
            _workspace: workspace,
        })
    }
}

fn native_budget_error(_: crate::RpcBudgetError) -> NativeApiError {
    NativeApiError::Control(HttpControlError::Busy)
}

struct NativeResult {
    value: Value,
    _response: crate::rpc_budget::RpcResponseLease,
    _workspace: crate::rpc_budget::RpcByteCharge,
}

impl std::ops::Deref for NativeResult {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.value
    }
}

pub struct NativeEventSubscription {
    subscriber: RpcEventSubscriber,
    client: RpcClientBudget,
}

impl NativeEventSubscription {
    pub fn set_filter(&mut self, filter: crate::RpcEventFilter) -> Result<(), NativeApiError> {
        self.subscriber
            .set_filter(filter)
            .map_err(NativeApiError::Event)
    }

    pub fn try_next(&mut self) -> Result<Option<Value>, NativeApiError> {
        let response = self.client.response(None).map_err(native_budget_error)?;
        let _workspace = response.workspace().map_err(native_budget_error)?;
        self.subscriber
            .try_next_bounded(crate::rpc_result::RESULT_VALUE_BYTES)
            .map(|delivery| delivery.map(|delivery| delivery.into_value()))
            .map_err(NativeApiError::Event)
    }
}

fn task_status(value: &Value) -> Result<TaskStatus, NativeApiError> {
    let gid = value
        .get("gid")
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .ok_or(NativeApiError::InvalidResponse("invalid GID"))?;
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
        total_length: decimal_field(value, "totalLength")?,
        completed_length: decimal_field(value, "completedLength")?,
        bittorrent: BitTorrentStatus::from_status(value)?,
        ssh_connection: value
            .get("sshConnection")
            .map(|value| {
                serde_json::from_value(value.clone())
                    .map_err(|_| NativeApiError::InvalidResponse("invalid SSH diagnostics"))
            })
            .transpose()?,
        followed_by: match value.get("followedBy") {
            None => Vec::new(),
            Some(value) => value
                .as_array()
                .filter(|values| values.len() <= 1000)
                .ok_or(NativeApiError::InvalidResponse("invalid followedBy"))?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .and_then(|text| text.parse().ok())
                        .ok_or(NativeApiError::InvalidResponse("invalid child GID"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        },
        host_key_challenge: value
            .get("hostKeyChallenge")
            .map(|value| {
                let invalid = || NativeApiError::InvalidResponse("invalid host-key challenge");
                Ok(ariax_core::HostKeyChallenge {
                    id: ariax_core::HostKeyChallengeId::new(
                        crate::transfer_task::parse_hex_bytes(
                            value["id"].as_str().ok_or_else(invalid)?,
                        )
                        .map_err(|_| invalid())?,
                    ),
                    canonical_host: value["host"].as_str().ok_or_else(invalid)?.to_owned(),
                    port: value["port"]
                        .as_u64()
                        .and_then(|port| u16::try_from(port).ok())
                        .filter(|port| *port != 0)
                        .ok_or_else(invalid)?,
                    algorithm: value["algorithm"].as_str().ok_or_else(invalid)?.to_owned(),
                    fingerprint_sha256: ariax_core::HostKeyFingerprint::new(
                        crate::transfer_task::parse_host_key_fingerprint(
                            value["fingerprintSha256"].as_str().ok_or_else(invalid)?,
                        )
                        .map_err(|_| invalid())?,
                    ),
                })
            })
            .transpose()?,
    })
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
    let scheduler = SchedulerConfig::new(task_capacity, active_capacity, true)
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

fn create_private_directory(
    path: &std::path::Path,
    error: &'static str,
) -> Result<(), NativeApiError> {
    #[cfg(windows)]
    {
        match ariax_windows_security::create_private_directory(path) {
            Ok(()) => Ok(()),
            Err(failure) if failure.kind() == std::io::ErrorKind::AlreadyExists => {
                ariax_windows_security::verify_private_directory(path)
            }
            Err(failure) => Err(failure),
        }
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
    async fn typed_configuration_mutations_diagnostics_and_json_share_one_engine() {
        let root = std::env::temp_dir().join(format!("ariax-native-parity-{}", std::process::id()));
        let engine = Engine::builder()
            .output_root(root.join("output"))
            .profile(RuntimeProfile::Compact)
            .build()
            .await
            .expect("engine");
        let report = engine
            .reload_config(ConfigurationUpdate {
                text: "split=3\n".to_owned(),
                expected_generation: Some(0),
                ..ConfigurationUpdate::default()
            })
            .await
            .expect("reload");
        assert_eq!(report.config_generation, 1);
        assert!(
            engine
                .reload_config(ConfigurationUpdate {
                    text: "split=0\n".to_owned(),
                    ..ConfigurationUpdate::default()
                })
                .await
                .is_err()
        );
        engine
            .change_global_options(GlobalOptions {
                task_defaults: DownloadOptions {
                    split: NonZeroUsize::new(4),
                    ..DownloadOptions::default()
                },
                max_overall_download_limit: Some(1024 * 1024),
                scheduling: None,
                ..GlobalOptions::default()
            })
            .await
            .expect("global");
        let gid = engine
            .add_uri(AddUri {
                uris: vec!["http://example.test/first".to_owned()],
                options: DownloadOptions {
                    pause: true,
                    split: NonZeroUsize::new(6),
                    retry: Some(crate::HttpRetryPolicy::aggressive()),
                    ..DownloadOptions::default()
                },
            })
            .await
            .expect("add");
        assert_eq!(engine.options(gid).await.expect("options")["split"], "6");
        assert!(engine.active().await.unwrap().is_empty());
        assert_eq!(engine.waiting(0, 10).await.unwrap()[0].gid, gid);
        assert!(engine.stopped(0, 10).await.unwrap().is_empty());
        assert!(engine.waiting(0, 1001).await.is_err());
        assert_eq!(engine.uris(gid).await.unwrap()[0].status, UriUsage::Used);
        let files = engine.files(gid).await.unwrap();
        assert_eq!(files[0].index, 1);
        assert!(files[0].selected);
        assert_eq!(engine.servers(gid).await.unwrap()[0].index, 1);
        assert_eq!(engine.global_statistics().await.unwrap().num_waiting, 1);
        assert!(
            engine
                .version()
                .await
                .unwrap()
                .enabled_features
                .contains(&"HTTP".to_owned())
        );
        assert!(!engine.session_info().await.unwrap().session_id.is_empty());
        let missing = Gid::new(u64::MAX).unwrap();
        assert!(engine.uris(missing).await.is_err());
        assert!(engine.files(missing).await.is_err());
        assert!(engine.servers(missing).await.is_err());
        let held: Vec<_> = (0..crate::MAX_RPC_CLIENT_REQUESTS)
            .map(|_| engine.client.try_request(0).unwrap())
            .collect();
        for result in [
            engine
                .change_options(
                    gid,
                    DownloadOptions {
                        output: Some("bounded.bin".to_owned()),
                        ..DownloadOptions::default()
                    },
                    false,
                )
                .await,
            engine.change_global_options(GlobalOptions::default()).await,
            engine
                .replace_sources(gid, vec!["http://example.test/rejected".to_owned()])
                .await,
            engine
                .reload_config(ConfigurationUpdate::default())
                .await
                .map(|_| ()),
        ] {
            assert!(matches!(
                result,
                Err(NativeApiError::Control(HttpControlError::Busy))
            ));
            assert_eq!(
                engine.client.outstanding_requests(),
                crate::MAX_RPC_CLIENT_REQUESTS
            );
        }
        drop(held);
        assert_eq!(engine.options(gid).await.unwrap()["split"], "6");
        assert_eq!(
            engine.options(gid).await.expect("retry")["retry-profile"],
            "aggressive"
        );
        assert!(matches!(
            engine
                .change_options(
                    gid,
                    DownloadOptions {
                        timeout_seconds: Some(601),
                        ..DownloadOptions::default()
                    },
                    false
                )
                .await,
            Err(NativeApiError::Control(
                HttpControlError::OptionPatchRejected(_)
            ))
        ));
        engine
            .change_options(
                gid,
                DownloadOptions {
                    split: NonZeroUsize::new(2),
                    max_download_limit: Some(1024),
                    ..DownloadOptions::default()
                },
                false,
            )
            .await
            .expect("change");
        engine
            .replace_sources(gid, vec!["http://example.test/second".to_owned()])
            .await
            .expect("sources");
        assert_eq!(
            engine
                .change_position(gid, 0, PositionOrigin::Start)
                .await
                .expect("position"),
            0
        );
        let dump = engine
            .dump_config(ConfigDumpMode::TaskEffective(gid), ConfigDumpFormat::Json)
            .await
            .expect("dump");
        assert_eq!(
            serde_json::from_str::<Value>(&dump).expect("JSON")["options"]["split"],
            "2"
        );
        let diagnostics = engine.diagnostics().await.expect("diagnostics");
        assert_eq!(diagnostics.profile.as_deref(), Some("compact"));
        assert_eq!(diagnostics.task_count, 1);
        assert!(diagnostics.rpc_bytes <= diagnostics.rpc_byte_limit);
        let response = engine
            .rpc_json(
                br#"{"jsonrpc":"2.0","id":1,"method":"system.listMethods"}"#,
                crate::RpcCompatibility::Strict,
            )
            .await
            .expect("JSON API");
        let result: Value = serde_json::from_slice(&response).expect("catalog");
        assert!(
            result["result"]
                .as_array()
                .expect("methods")
                .contains(&json!("ariax.getDiagnostics"))
        );
        assert!(engine.client.outstanding_requests() > 0);
        drop(response);
        assert_eq!(engine.client.outstanding_requests(), 0);
        for (keys, accepted) in [
            (json!(["gid", "status", "slotState", "wireSpeed"]), true),
            (json!(["bitfield"]), false),
            (json!(["unknownField"]), false),
        ] {
            let request = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":2,"method":"aria2.tellStatus","params":[gid.to_string(), keys]})).unwrap();
            let response = engine
                .rpc_json(&request, crate::RpcCompatibility::Strict)
                .await
                .unwrap();
            let result: Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(result.get("result").is_some(), accepted, "{result}");
            if accepted {
                assert_eq!(result["result"].as_object().unwrap().len(), 4);
            }
        }
        engine.pause_all(false).await.expect("bulk pause");
        engine.shutdown().await.expect("shutdown");
        fs::remove_dir_all(root).expect("cleanup");
    }

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
    async fn native_session_operations_preserve_atomic_import_and_final_shutdown_save() {
        let root =
            std::env::temp_dir().join(format!("ariax-native-session-{}", std::process::id()));
        fs::create_dir_all(&root).expect("root");
        let input = root.join("input.txt");
        let export = root.join("export.json");
        fs::write(
            &input,
            "http://example.test/file\n  pause=true\n  split=3\n",
        )
        .expect("input");
        let engine = Engine::builder()
            .output_root(root.join("output"))
            .input_file(&input, crate::SessionFormat::Aria2)
            .session_export(crate::SessionExportConfig {
                path: export.clone(),
                format: crate::SessionFormat::Json,
                interval: None,
            })
            .build()
            .await
            .expect("build with import");
        let document = engine
            .export_session(crate::SessionFormat::Json)
            .await
            .expect("export");
        let gids = engine
            .import_session(document, crate::SessionFormat::Json)
            .await
            .expect("atomic import");
        assert_eq!(gids.len(), 1);
        assert_eq!(
            engine.status(gids[0]).await.expect("status").status,
            Aria2Status::Paused
        );
        assert!(
            engine
                .import_session("{\"tasks\":null}".to_owned(), crate::SessionFormat::Json)
                .await
                .is_err()
        );
        assert_eq!(engine.plane.lock().await.task_catalog().len(), 2);
        engine.save_session().await.expect("explicit save");
        let bytes = fs::read(&export).expect("saved bytes");
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).expect("JSON")["tasks"]
                .as_array()
                .expect("tasks")
                .len(),
            2
        );
        engine.shutdown().await.expect("final save and shutdown");
        let recovered = Engine::builder()
            .output_root(root.join("output"))
            .build()
            .await
            .expect("recover");
        assert_eq!(
            recovered.status(gids[0]).await.expect("recovered").status,
            Aria2Status::Paused
        );
        recovered.shutdown().await.expect("shutdown recovery");
        fs::remove_dir_all(root).expect("cleanup");
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
        let baseline = engine.client.bytes();
        let retained = engine
            .call_control("aria2.tellStatus", json!([gid.to_string()]))
            .await
            .expect("retained native projection");
        assert_eq!(engine.client.outstanding_requests(), 1);
        assert!(engine.client.bytes() > baseline);
        assert!(matches!(
            engine.status(gid).await,
            Err(NativeApiError::Control(HttpControlError::Busy))
        ));
        drop(retained);
        assert_eq!(engine.client.outstanding_requests(), 0);
        assert_eq!(engine.client.bytes(), baseline);
        let held = (0..crate::MAX_RPC_CLIENT_REQUESTS)
            .map(|_| engine.client.try_request(1).expect("outstanding request"))
            .collect::<Vec<_>>();
        assert!(matches!(
            engine
                .add_uri(AddUri {
                    uris: vec!["http://example.test/second.bin".to_owned()],
                    options: DownloadOptions {
                        pause: true,
                        ..DownloadOptions::default()
                    }
                })
                .await,
            Err(NativeApiError::Control(HttpControlError::Busy))
        ));
        assert_eq!(engine.plane.lock().await.task_catalog().len(), 1);
        drop(held);
        assert!(matches!(
            engine
                .add_uri(AddUri {
                    uris: vec![format!("http://example.test/large?{}", "x".repeat(700_000))],
                    options: DownloadOptions {
                        pause: true,
                        ..DownloadOptions::default()
                    }
                })
                .await,
            Err(NativeApiError::Control(HttpControlError::Busy))
        ));
        assert_eq!(engine.client.outstanding_requests(), 0);
        assert_eq!(engine.client.bytes(), baseline);
        assert_eq!(
            engine
                .status(gid)
                .await
                .expect("query after rejection")
                .status,
            Aria2Status::Paused
        );
        engine.shutdown().await.expect("shutdown");
        let _ = fs::remove_dir_all(root);
    }
}
