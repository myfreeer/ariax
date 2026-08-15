//! Immutable public HTTP task specifications and their bounded process catalog.

use ariax_core::{Gid, TaskId, UriId};
use ariax_storage::{
    SafePathBuilder, SafeRelativePath, SanitizedOptionMap, SessionTaskSourceRecord,
};
use hyper::Uri;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub const MAX_HTTP_TASK_SOURCES: usize = 1024;
pub const DEFAULT_HTTP_SPLIT: usize = 5;
pub const DEFAULT_HTTP_MAX_CONNECTIONS_PER_SERVER: usize = 1;
pub const DEFAULT_HTTP_MIN_SPLIT_SIZE: u64 = 20 * 1024 * 1024;
pub const DEFAULT_HTTP_PIECE_LENGTH: u64 = 1024 * 1024;
pub const MAX_HTTP_PIECE_LENGTH: u64 = 1024 * 1024 * 1024;
pub const MAX_HTTP_TIMEOUT_SECS: u64 = 600;
pub const HTTP_SOURCE_FINGERPRINT_DOMAIN: &str = "ariax/http-source/v1\0";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpMirrorIdentityPolicy {
    #[default]
    TrustSubmittedMirrors,
    RequireSharedDigest,
}

impl HttpMirrorIdentityPolicy {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::TrustSubmittedMirrors => "off",
            Self::RequireSharedDigest => "strict",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpTaskOptions {
    pub split: NonZeroUsize,
    pub max_connections_per_server: NonZeroUsize,
    pub min_split_size: u64,
    pub piece_length: u64,
    pub connect_timeout: Duration,
    pub response_head_timeout: Duration,
    pub response_body_timeout: Duration,
    pub mirror_identity: HttpMirrorIdentityPolicy,
}

impl Default for HttpTaskOptions {
    fn default() -> Self {
        Self {
            split: NonZeroUsize::new(DEFAULT_HTTP_SPLIT).expect("default split is nonzero"),
            max_connections_per_server: NonZeroUsize::new(DEFAULT_HTTP_MAX_CONNECTIONS_PER_SERVER)
                .expect("default per-server connection count is nonzero"),
            min_split_size: DEFAULT_HTTP_MIN_SPLIT_SIZE,
            piece_length: DEFAULT_HTTP_PIECE_LENGTH,
            connect_timeout: Duration::from_secs(60),
            response_head_timeout: Duration::from_secs(60),
            response_body_timeout: Duration::from_secs(60),
            mirror_identity: HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
        }
    }
}

impl HttpTaskOptions {
    fn validate(&self) -> Result<(), HttpTaskSpecError> {
        if self.split.get() > MAX_HTTP_TASK_SOURCES
            || self.max_connections_per_server.get() > MAX_HTTP_TASK_SOURCES
            || self.min_split_size < DEFAULT_HTTP_PIECE_LENGTH
            || self.piece_length < DEFAULT_HTTP_PIECE_LENGTH
            || self.piece_length > MAX_HTTP_PIECE_LENGTH
            || !self.piece_length.is_power_of_two()
            || self.connect_timeout.is_zero()
            || self.response_head_timeout.is_zero()
            || self.response_body_timeout.is_zero()
            || self.connect_timeout.as_secs() > MAX_HTTP_TIMEOUT_SECS
            || self.response_head_timeout.as_secs() > MAX_HTTP_TIMEOUT_SECS
            || self.response_body_timeout.as_secs() > MAX_HTTP_TIMEOUT_SECS
        {
            return Err(HttpTaskSpecError::InvalidOptions);
        }
        Ok(())
    }

    pub fn sanitized(&self) -> Result<SanitizedOptionMap, HttpTaskSpecError> {
        SanitizedOptionMap::new([
            (
                "connect-timeout".to_owned(),
                self.connect_timeout.as_secs().to_string(),
            ),
            (
                "max-connection-per-server".to_owned(),
                self.max_connections_per_server.get().to_string(),
            ),
            ("min-split-size".to_owned(), self.min_split_size.to_string()),
            ("piece-length".to_owned(), self.piece_length.to_string()),
            (
                "timeout".to_owned(),
                self.response_body_timeout.as_secs().to_string(),
            ),
            ("split".to_owned(), self.split.get().to_string()),
            (
                "verify-mirror-identity".to_owned(),
                self.mirror_identity.code().to_owned(),
            ),
        ])
        .map_err(|_| HttpTaskSpecError::InvalidOptions)
    }

    /// Reconstructs the bounded HTTP options from the redacted persisted
    /// option snapshot used during startup recovery.
    pub fn from_sanitized(options: &SanitizedOptionMap) -> Result<Self, HttpTaskSpecError> {
        let mut value = Self::default();
        for (name, setting) in options.entries() {
            match name {
                "connect-timeout" => {
                    value.connect_timeout = Duration::from_secs(
                        setting
                            .parse()
                            .map_err(|_| HttpTaskSpecError::InvalidOptions)?,
                    )
                }
                "max-connection-per-server" => {
                    value.max_connections_per_server = NonZeroUsize::new(
                        setting
                            .parse()
                            .map_err(|_| HttpTaskSpecError::InvalidOptions)?,
                    )
                    .ok_or(HttpTaskSpecError::InvalidOptions)?;
                }
                "min-split-size" => {
                    value.min_split_size = setting
                        .parse()
                        .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
                }
                "piece-length" => {
                    value.piece_length = setting
                        .parse()
                        .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
                }
                "timeout" => {
                    value.response_body_timeout = Duration::from_secs(
                        setting
                            .parse()
                            .map_err(|_| HttpTaskSpecError::InvalidOptions)?,
                    )
                }
                "split" => {
                    value.split = NonZeroUsize::new(
                        setting
                            .parse()
                            .map_err(|_| HttpTaskSpecError::InvalidOptions)?,
                    )
                    .ok_or(HttpTaskSpecError::InvalidOptions)?;
                }
                "verify-mirror-identity" => {
                    value.mirror_identity = match setting {
                        "strict" => HttpMirrorIdentityPolicy::RequireSharedDigest,
                        "off" => HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
                        _ => return Err(HttpTaskSpecError::InvalidOptions),
                    };
                }
                // Task placement is persisted in the same atomic option
                // snapshot but is owned by `HttpTaskSpec`, not this protocol
                // tuning structure.
                "out" => {}
                _ => return Err(HttpTaskSpecError::InvalidOptions),
            }
        }
        value.validate()?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpSourceSpec {
    id: UriId,
    uri: Arc<str>,
    redacted_fingerprint: [u8; 32],
    priority: i64,
    needs_credentials: bool,
}

impl HttpSourceSpec {
    #[must_use]
    pub const fn id(&self) -> UriId {
        self.id
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[must_use]
    pub const fn redacted_fingerprint(&self) -> &[u8; 32] {
        &self.redacted_fingerprint
    }

    #[must_use]
    pub const fn priority(&self) -> i64 {
        self.priority
    }

    #[must_use]
    pub const fn needs_credentials(&self) -> bool {
        self.needs_credentials
    }

    #[must_use]
    pub fn persistence_record(&self) -> SessionTaskSourceRecord {
        SessionTaskSourceRecord {
            uri_id: self.id.get(),
            persistence_safe_uri: Some(self.uri.to_string()),
            redacted_fingerprint: self.redacted_fingerprint,
            needs_credentials: self.needs_credentials,
            priority: self.priority,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpTaskSpec {
    task: TaskId,
    gid: Gid,
    sources: Arc<[HttpSourceSpec]>,
    output_root: Arc<PathBuf>,
    output: SafeRelativePath,
    options: HttpTaskOptions,
}

impl HttpTaskSpec {
    pub fn new(
        task: TaskId,
        gid: Gid,
        source_uris: impl IntoIterator<Item = String>,
        output_root: PathBuf,
        output: SafeRelativePath,
        options: HttpTaskOptions,
        needs_credentials: bool,
    ) -> Result<Self, HttpTaskSpecError> {
        options.validate()?;
        if output_root.as_os_str().is_empty() || !output_root.is_absolute() {
            return Err(HttpTaskSpecError::InvalidOutputRoot);
        }
        let mut seen = BTreeSet::new();
        let mut sources = Vec::new();
        for (index, uri_text) in source_uris.into_iter().enumerate() {
            if sources.len() == MAX_HTTP_TASK_SOURCES {
                return Err(HttpTaskSpecError::TooManySources);
            }
            let uri: Uri = uri_text
                .parse()
                .map_err(|_| HttpTaskSpecError::InvalidUri)?;
            if !matches!(uri.scheme_str(), Some("http" | "https")) {
                return Err(HttpTaskSpecError::UnsupportedScheme);
            }
            let authority = uri.authority().ok_or(HttpTaskSpecError::MissingAuthority)?;
            if authority.as_str().contains('@') {
                return Err(HttpTaskSpecError::UserInfoForbidden);
            }
            let canonical = uri.to_string();
            if !seen.insert(canonical.clone()) {
                return Err(HttpTaskSpecError::DuplicateSource);
            }
            let id = u32::try_from(index)
                .ok()
                .map(UriId::new)
                .ok_or(HttpTaskSpecError::TooManySources)?;
            let priority = i64::try_from(index).map_err(|_| HttpTaskSpecError::TooManySources)?;
            sources.push(HttpSourceSpec {
                id,
                redacted_fingerprint: source_fingerprint(&canonical),
                uri: canonical.into(),
                priority,
                needs_credentials,
            });
        }
        if sources.is_empty() {
            return Err(HttpTaskSpecError::NoSources);
        }
        Ok(Self {
            task,
            gid,
            sources: sources.into(),
            output_root: Arc::new(output_root),
            output,
            options,
        })
    }

    #[must_use]
    pub const fn task(&self) -> TaskId {
        self.task
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        self.gid
    }

    #[must_use]
    pub fn sources(&self) -> &[HttpSourceSpec] {
        &self.sources
    }

    #[must_use]
    pub fn output_root(&self) -> &PathBuf {
        &self.output_root
    }

    #[must_use]
    pub const fn output(&self) -> &SafeRelativePath {
        &self.output
    }

    #[must_use]
    pub const fn options(&self) -> &HttpTaskOptions {
        &self.options
    }

    #[must_use]
    pub fn persistence_sources(&self) -> Vec<SessionTaskSourceRecord> {
        self.sources
            .iter()
            .map(HttpSourceSpec::persistence_record)
            .collect()
    }

    pub fn persistence_options(&self) -> Result<SanitizedOptionMap, HttpTaskSpecError> {
        let base = self.options.sanitized()?;
        SanitizedOptionMap::new(
            base.entries()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .chain([("out".to_owned(), self.output.canonical_string())]),
        )
        .map_err(|_| HttpTaskSpecError::InvalidOptions)
    }

    pub fn persisted_output(
        options: &SanitizedOptionMap,
    ) -> Result<SafeRelativePath, HttpTaskSpecError> {
        let output = options
            .entries()
            .find_map(|(name, value)| (name == "out").then_some(value))
            .ok_or(HttpTaskSpecError::InvalidOptions)?;
        SafePathBuilder::from_user_path(output, ariax_storage::PathPlatform::current())
            .map_err(|_| HttpTaskSpecError::InvalidOptions)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpTaskSpecError {
    NoSources,
    TooManySources,
    DuplicateSource,
    InvalidUri,
    UnsupportedScheme,
    MissingAuthority,
    UserInfoForbidden,
    InvalidOutputRoot,
    InvalidOptions,
}

impl HttpTaskSpecError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::NoSources => "no_http_sources",
            Self::TooManySources => "too_many_http_sources",
            Self::DuplicateSource => "duplicate_http_source",
            Self::InvalidUri => "invalid_http_uri",
            Self::UnsupportedScheme => "unsupported_http_scheme",
            Self::MissingAuthority => "missing_http_authority",
            Self::UserInfoForbidden => "http_uri_userinfo_forbidden",
            Self::InvalidOutputRoot => "invalid_http_output_root",
            Self::InvalidOptions => "invalid_http_task_options",
        }
    }
}

impl fmt::Display for HttpTaskSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpTaskSpecError {}

#[derive(Debug)]
pub struct HttpTaskCatalog {
    capacity: NonZeroUsize,
    by_task: BTreeMap<TaskId, Arc<HttpTaskSpec>>,
    by_gid: BTreeMap<Gid, TaskId>,
}

impl HttpTaskCatalog {
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            by_task: BTreeMap::new(),
            by_gid: BTreeMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        spec: HttpTaskSpec,
    ) -> Result<Arc<HttpTaskSpec>, HttpTaskCatalogError> {
        if self.by_task.len() == self.capacity.get() {
            return Err(HttpTaskCatalogError::Full);
        }
        if self.by_task.contains_key(&spec.task) || self.by_gid.contains_key(&spec.gid) {
            return Err(HttpTaskCatalogError::Collision);
        }
        let spec = Arc::new(spec);
        self.by_gid.insert(spec.gid, spec.task);
        self.by_task.insert(spec.task, Arc::clone(&spec));
        Ok(spec)
    }

    #[must_use]
    pub fn get(&self, task: TaskId) -> Option<Arc<HttpTaskSpec>> {
        self.by_task.get(&task).cloned()
    }

    #[must_use]
    pub fn get_gid(&self, gid: Gid) -> Option<Arc<HttpTaskSpec>> {
        self.by_gid.get(&gid).and_then(|task| self.get(*task))
    }

    pub fn remove(&mut self, task: TaskId) -> Option<Arc<HttpTaskSpec>> {
        let spec = self.by_task.remove(&task)?;
        self.by_gid.remove(&spec.gid);
        Some(spec)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_task.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_task.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpTaskCatalogError {
    Full,
    Collision,
}

/// Cloneable, process-local ownership of the bounded public HTTP task catalog.
///
/// Admission and worker supervision share this registry so an allocation can
/// only start from the exact immutable specification that was admitted and
/// persisted for its task/GID pair.
#[derive(Clone, Debug)]
pub struct SharedHttpTaskCatalog {
    inner: Arc<RwLock<HttpTaskCatalog>>,
}

impl SharedHttpTaskCatalog {
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HttpTaskCatalog::new(capacity))),
        }
    }

    pub fn insert(&self, spec: HttpTaskSpec) -> Result<Arc<HttpTaskSpec>, HttpTaskCatalogError> {
        write_unpoisoned(&self.inner).insert(spec)
    }

    #[must_use]
    pub fn get(&self, task: TaskId) -> Option<Arc<HttpTaskSpec>> {
        read_unpoisoned(&self.inner).get(task)
    }

    #[must_use]
    pub fn get_gid(&self, gid: Gid) -> Option<Arc<HttpTaskSpec>> {
        read_unpoisoned(&self.inner).get_gid(gid)
    }

    pub fn remove(&self, task: TaskId) -> Option<Arc<HttpTaskSpec>> {
        write_unpoisoned(&self.inner).remove(task)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        read_unpoisoned(&self.inner).len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        read_unpoisoned(&self.inner).is_empty()
    }
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn source_fingerprint(uri: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(HTTP_SOURCE_FINGERPRINT_DOMAIN.as_bytes());
    digest.update((uri.len() as u64).to_le_bytes());
    digest.update(uri.as_bytes());
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ariax_storage::{PathPlatform, SafePathBuilder};

    fn task(value: u64) -> TaskId {
        TaskId::new(value).expect("task id")
    }

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("gid")
    }

    fn output() -> SafeRelativePath {
        SafePathBuilder::from_user_path("downloads/file.bin", PathPlatform::current())
            .expect("safe output")
    }

    #[test]
    fn task_spec_canonicalizes_multiple_sources_and_builds_restart_rows() {
        let spec = HttpTaskSpec::new(
            task(1),
            gid(1),
            [
                "https://first.example/file".to_owned(),
                "http://second.example:8080/file".to_owned(),
            ],
            std::env::temp_dir(),
            output(),
            HttpTaskOptions::default(),
            true,
        )
        .expect("task spec");
        assert_eq!(spec.sources().len(), 2);
        assert_eq!(spec.sources()[0].id().get(), 0);
        assert_ne!(
            spec.sources()[0].redacted_fingerprint(),
            spec.sources()[1].redacted_fingerprint()
        );
        let rows = spec.persistence_sources();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.needs_credentials));
        assert_eq!(
            spec.options()
                .sanitized()
                .expect("sanitized")
                .entries()
                .find(|(name, _)| *name == "split")
                .map(|(_, value)| value),
            Some("5")
        );
    }

    #[test]
    fn task_spec_rejects_unsafe_or_ambiguous_sources_and_roots() {
        for (sources, expected) in [
            (Vec::new(), HttpTaskSpecError::NoSources),
            (
                vec!["ftp://example.com/file".to_owned()],
                HttpTaskSpecError::UnsupportedScheme,
            ),
            (
                vec!["https://user:secret@example.com/file".to_owned()],
                HttpTaskSpecError::UserInfoForbidden,
            ),
            (
                vec![
                    "https://example.com/file".to_owned(),
                    "https://example.com/file".to_owned(),
                ],
                HttpTaskSpecError::DuplicateSource,
            ),
        ] {
            assert_eq!(
                HttpTaskSpec::new(
                    task(1),
                    gid(1),
                    sources,
                    std::env::temp_dir(),
                    output(),
                    HttpTaskOptions::default(),
                    false,
                ),
                Err(expected)
            );
        }
        assert_eq!(
            HttpTaskSpec::new(
                task(1),
                gid(1),
                ["https://example.com/file".to_owned()],
                PathBuf::from("relative"),
                output(),
                HttpTaskOptions::default(),
                false,
            ),
            Err(HttpTaskSpecError::InvalidOutputRoot)
        );
    }

    #[test]
    fn catalog_rejects_identity_collisions_and_releases_both_indexes() {
        let mut catalog = HttpTaskCatalog::new(NonZeroUsize::new(2).expect("capacity"));
        let spec = HttpTaskSpec::new(
            task(1),
            gid(1),
            ["https://example.com/file".to_owned()],
            std::env::temp_dir(),
            output(),
            HttpTaskOptions::default(),
            false,
        )
        .expect("spec");
        catalog.insert(spec.clone()).expect("insert");
        assert_eq!(catalog.get_gid(gid(1)).expect("by gid").task(), task(1));
        assert_eq!(catalog.insert(spec), Err(HttpTaskCatalogError::Collision));
        assert!(catalog.remove(task(1)).is_some());
        assert!(catalog.get_gid(gid(1)).is_none());
    }
}
