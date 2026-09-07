//! Immutable public HTTP task specifications and their bounded process catalog.

use crate::http_retry::{
    HttpRetryAfterPolicy, HttpRetryBackoff, HttpRetryPolicy, HttpRetryProfile, HttpRetryStatusSet,
    HttpRetryTriggerSet, HttpStaleValidatorPolicy,
};
use ariax_core::{Gid, TaskId, UriId};
use ariax_storage::{
    JournalDigest, JournalDigestAlgorithm, SafePathBuilder, SafeRelativePath, SanitizedOptionMap,
    SessionTaskSourceRecord,
};
use hyper::Uri;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
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
pub const DEFAULT_HTTP_ENDGAME_MAX_DUPLICATES: usize = 2;
pub const MAX_HTTP_ENDGAME_MAX_DUPLICATES: usize = 8;
pub const HTTP_SOURCE_FINGERPRINT_DOMAIN: &str = "ariax/http-source/v1\0";
pub const HTTP_SHA256_CHECKSUM_TEXT_BYTES: usize = 72;

/// One canonical user-supplied whole-representation checksum accepted by the
/// executable HTTP slice. The enum leaves room for the reviewed digest
/// vocabulary while this milestone intentionally admits only SHA-256.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpContentChecksum {
    Sha256([u8; 32]),
}

impl HttpContentChecksum {
    pub fn parse(value: &str) -> Result<Self, HttpContentChecksumError> {
        if value.len() > HTTP_SHA256_CHECKSUM_TEXT_BYTES {
            return Err(HttpContentChecksumError::InvalidLength);
        }
        let (algorithm, digest) = value
            .split_once('=')
            .ok_or(HttpContentChecksumError::InvalidFormat)?;
        if digest.contains('=') {
            return Err(HttpContentChecksumError::InvalidFormat);
        }
        if algorithm != JournalDigestAlgorithm::Sha256.code() {
            return Err(HttpContentChecksumError::UnsupportedAlgorithm);
        }
        if digest.len() != JournalDigestAlgorithm::Sha256.value_len() * 2 {
            return Err(HttpContentChecksumError::InvalidLength);
        }
        let mut bytes = [0_u8; 32];
        for (target, pair) in bytes.iter_mut().zip(digest.as_bytes().chunks_exact(2)) {
            let high = decode_hex_digit(pair[0]).ok_or(HttpContentChecksumError::InvalidHex)?;
            let low = decode_hex_digit(pair[1]).ok_or(HttpContentChecksumError::InvalidHex)?;
            *target = (high << 4) | low;
        }
        Ok(Self::Sha256(bytes))
    }

    #[must_use]
    pub const fn sha256(value: [u8; 32]) -> Self {
        Self::Sha256(value)
    }

    #[must_use]
    pub const fn algorithm(self) -> JournalDigestAlgorithm {
        match self {
            Self::Sha256(_) => JournalDigestAlgorithm::Sha256,
        }
    }

    #[must_use]
    pub const fn value(self) -> [u8; 32] {
        match self {
            Self::Sha256(value) => value,
        }
    }

    #[must_use]
    pub fn canonical(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let value = self.value();
        let mut canonical = String::with_capacity(HTTP_SHA256_CHECKSUM_TEXT_BYTES);
        canonical.push_str(self.algorithm().code());
        canonical.push('=');
        for byte in value {
            canonical.push(char::from(HEX[usize::from(byte >> 4)]));
            canonical.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        canonical
    }

    #[must_use]
    pub fn journal_digest(self) -> JournalDigest {
        JournalDigest::new(self.algorithm(), self.value().to_vec())
            .expect("HTTP checksum has the canonical algorithm length")
    }
}

fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpContentChecksumError {
    InvalidFormat,
    UnsupportedAlgorithm,
    InvalidLength,
    InvalidHex,
}

impl fmt::Display for HttpContentChecksumError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidFormat => "checksum must use TYPE=DIGEST syntax",
            Self::UnsupportedAlgorithm => "only sha-256 checksums are supported",
            Self::InvalidLength => "checksum has the wrong length",
            Self::InvalidHex => "checksum digest is not hexadecimal",
        })
    }
}

impl Error for HttpContentChecksumError {}

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
    /// Per-task accepted download-payload ceiling in bytes/second. Zero keeps
    /// the task bucket unlimited, matching aria2's rate-limit convention.
    pub max_download_limit: u64,
    /// Useful protocol progress threshold in bytes/second. Zero disables the
    /// lowest-speed retry trigger.
    pub lowest_speed_limit: u64,
    /// Task-wide concurrent duplicate-attempt cap for the bounded endgame
    /// path. Zero disables endgame; one original may have only one duplicate.
    pub endgame_max_duplicates: usize,
    pub mirror_identity: HttpMirrorIdentityPolicy,
    /// Optional whole-representation checksum used for terminal verification
    /// and as the shared identity proof for strict concurrent mirrors.
    pub checksum: Option<HttpContentChecksum>,
    /// An explicitly resolved per-task retry policy. Tasks without one inherit
    /// the process worker policy at admission.
    pub retry: Option<HttpRetryPolicy>,
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
            max_download_limit: 0,
            lowest_speed_limit: 0,
            endgame_max_duplicates: DEFAULT_HTTP_ENDGAME_MAX_DUPLICATES,
            mirror_identity: HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
            checksum: None,
            retry: None,
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
            || self.endgame_max_duplicates > MAX_HTTP_ENDGAME_MAX_DUPLICATES
            || self
                .retry
                .as_ref()
                .is_some_and(|retry| retry.validate().is_err())
        {
            return Err(HttpTaskSpecError::InvalidOptions);
        }
        Ok(())
    }

    pub fn sanitized(&self) -> Result<SanitizedOptionMap, HttpTaskSpecError> {
        let mut entries = vec![
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
            (
                "max-download-limit".to_owned(),
                self.max_download_limit.to_string(),
            ),
            (
                "lowest-speed-limit".to_owned(),
                self.lowest_speed_limit.to_string(),
            ),
            (
                "endgame-max-duplicates".to_owned(),
                self.endgame_max_duplicates.to_string(),
            ),
            ("split".to_owned(), self.split.get().to_string()),
            (
                "verify-mirror-identity".to_owned(),
                self.mirror_identity.code().to_owned(),
            ),
        ];
        if let Some(retry) = &self.retry {
            entries.extend(retry_sanitized_entries(retry));
        }
        if let Some(checksum) = self.checksum {
            entries.push(("checksum".to_owned(), checksum.canonical()));
        }
        SanitizedOptionMap::new(entries).map_err(|_| HttpTaskSpecError::InvalidOptions)
    }

    /// Reconstructs the bounded HTTP options from the redacted persisted
    /// option snapshot used during startup recovery.
    pub fn from_sanitized(options: &SanitizedOptionMap) -> Result<Self, HttpTaskSpecError> {
        let mut value = Self::default();
        let mut retry_options = BTreeMap::new();
        for (name, setting) in options.entries() {
            if is_retry_option(name) {
                retry_options.insert(name, setting);
                continue;
            }
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
                "max-download-limit" => {
                    value.max_download_limit = setting
                        .parse()
                        .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
                }
                "lowest-speed-limit" => {
                    value.lowest_speed_limit = setting
                        .parse()
                        .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
                }
                "endgame-max-duplicates" => {
                    value.endgame_max_duplicates = setting
                        .parse()
                        .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
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
                "checksum" => {
                    value.checksum = Some(
                        HttpContentChecksum::parse(setting)
                            .map_err(|_| HttpTaskSpecError::InvalidOptions)?,
                    );
                }
                // Task placement is persisted in the same atomic option
                // snapshot but is owned by `HttpTaskSpec`, not this protocol
                // tuning structure.
                "out" => {}
                _ => return Err(HttpTaskSpecError::InvalidOptions),
            }
        }
        if !retry_options.is_empty() {
            value.retry = Some(retry_from_sanitized(&retry_options)?);
        }
        value.validate()?;
        Ok(value)
    }
}

fn is_retry_option(name: &str) -> bool {
    matches!(
        name,
        "max-tries"
            | "retry-wait"
            | "retry-profile"
            | "retry-on"
            | "retry-on-http-status"
            | "retry-on-http-status-add"
            | "retry-on-http-status-remove"
            | "retry-after"
            | "retry-after-max"
            | "retry-after-min"
            | "retry-backoff"
            | "retry-max-wait"
            | "retry-max-attempts"
            | "retry-max-attempts-per-mirror"
            | "retry-max-elapsed"
            | "stale-validator-policy"
    )
}

fn retry_sanitized_entries(policy: &HttpRetryPolicy) -> Vec<(String, String)> {
    vec![
        (
            "max-tries".to_owned(),
            policy.max_attempts.get().to_string(),
        ),
        (
            "retry-wait".to_owned(),
            policy.base_wait.as_secs().to_string(),
        ),
        ("retry-profile".to_owned(), policy.profile.code().to_owned()),
        ("retry-on".to_owned(), policy.retry_on.canonical()),
        (
            "retry-on-http-status".to_owned(),
            policy.retryable_statuses.canonical(),
        ),
        (
            "retry-after".to_owned(),
            policy.retry_after_policy().code().to_owned(),
        ),
        (
            "retry-after-max".to_owned(),
            policy.retry_after_max.as_secs().to_string(),
        ),
        (
            "retry-after-min".to_owned(),
            policy.retry_after_min.as_secs().to_string(),
        ),
        ("retry-backoff".to_owned(), policy.backoff.code().to_owned()),
        (
            "retry-max-wait".to_owned(),
            policy.max_wait.as_secs().to_string(),
        ),
        (
            "retry-max-attempts".to_owned(),
            policy.max_attempts.get().to_string(),
        ),
        (
            "retry-max-attempts-per-mirror".to_owned(),
            policy.max_attempts_per_mirror.get().to_string(),
        ),
        (
            "retry-max-elapsed".to_owned(),
            policy.max_elapsed.as_secs().to_string(),
        ),
        (
            "stale-validator-policy".to_owned(),
            policy.stale_validator_policy.code().to_owned(),
        ),
    ]
}

fn retry_from_sanitized(
    options: &BTreeMap<&str, &str>,
) -> Result<HttpRetryPolicy, HttpTaskSpecError> {
    let profile = retry_value(options, "retry-profile")
        .map(HttpRetryProfile::parse)
        .transpose()
        .map_err(|_| HttpTaskSpecError::InvalidOptions)?
        .unwrap_or_default();
    let mut policy = HttpRetryPolicy::from_profile(profile);

    if let Some(value) = retry_value(options, "retry-on") {
        policy.retry_on =
            HttpRetryTriggerSet::parse(value).map_err(|_| HttpTaskSpecError::InvalidOptions)?;
    }
    if let Some(value) = retry_value(options, "retry-on-http-status") {
        policy.retryable_statuses = if value.is_empty() {
            HttpRetryStatusSet::default()
        } else {
            HttpRetryStatusSet::parse(value).map_err(|_| HttpTaskSpecError::InvalidOptions)?
        };
    }
    if let Some(value) = retry_value(options, "retry-on-http-status-add") {
        for code in HttpRetryStatusSet::parse(value)
            .map_err(|_| HttpTaskSpecError::InvalidOptions)?
            .iter()
        {
            policy
                .retryable_statuses
                .insert(code)
                .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
        }
    }
    if let Some(value) = retry_value(options, "retry-on-http-status-remove") {
        for code in HttpRetryStatusSet::parse(value)
            .map_err(|_| HttpTaskSpecError::InvalidOptions)?
            .iter()
        {
            policy.retryable_statuses.remove(code);
        }
    }

    let max_tries = retry_value(options, "max-tries")
        .map(parse_retry_attempt_cap)
        .transpose()?;
    let max_attempts = retry_value(options, "retry-max-attempts")
        .map(parse_retry_attempt_cap)
        .transpose()?;
    if let Some(value) = stricter_attempt_cap(max_tries, max_attempts) {
        policy.max_attempts = value;
    }
    if let Some(value) = retry_value(options, "retry-max-attempts-per-mirror") {
        policy.max_attempts_per_mirror = parse_retry_attempt_cap(value)?;
    }
    if let Some(value) = retry_value(options, "retry-wait") {
        policy.base_wait = parse_retry_duration(value)?;
    }
    if let Some(value) = retry_value(options, "retry-max-wait") {
        policy.max_wait = parse_retry_duration(value)?;
    }
    if let Some(value) = retry_value(options, "retry-max-elapsed") {
        policy.max_elapsed = parse_retry_duration(value)?;
    }
    if let Some(value) = retry_value(options, "retry-after-min") {
        policy.retry_after_min = parse_retry_duration(value)?;
    }
    if let Some(value) = retry_value(options, "retry-after-max") {
        policy.retry_after_max = parse_retry_duration(value)?;
    }
    if let Some(value) = retry_value(options, "retry-after") {
        policy.respect_retry_after = matches!(
            HttpRetryAfterPolicy::parse(value).map_err(|_| HttpTaskSpecError::InvalidOptions)?,
            HttpRetryAfterPolicy::Respect
        );
    }
    if let Some(value) = retry_value(options, "retry-backoff") {
        policy.backoff =
            HttpRetryBackoff::parse(value).map_err(|_| HttpTaskSpecError::InvalidOptions)?;
    }
    if let Some(value) = retry_value(options, "stale-validator-policy") {
        policy.stale_validator_policy = HttpStaleValidatorPolicy::parse(value)
            .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
    }
    policy
        .validate()
        .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
    Ok(policy)
}

fn retry_value<'a>(options: &'a BTreeMap<&str, &str>, name: &str) -> Option<&'a str> {
    options.get(name).copied()
}

fn parse_retry_attempt_cap(value: &str) -> Result<NonZeroU32, HttpTaskSpecError> {
    value
        .parse()
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(HttpTaskSpecError::InvalidOptions)
}

fn stricter_attempt_cap(
    max_tries: Option<NonZeroU32>,
    max_attempts: Option<NonZeroU32>,
) -> Option<NonZeroU32> {
    match (max_tries, max_attempts) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn parse_retry_duration(value: &str) -> Result<Duration, HttpTaskSpecError> {
    value
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| HttpTaskSpecError::InvalidOptions)
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

    pub fn replace(
        &mut self,
        spec: HttpTaskSpec,
    ) -> Result<Arc<HttpTaskSpec>, HttpTaskCatalogError> {
        if self.by_gid.get(&spec.gid) != Some(&spec.task) || !self.by_task.contains_key(&spec.task)
        {
            return Err(HttpTaskCatalogError::Collision);
        }
        let spec = Arc::new(spec);
        self.by_task.insert(spec.task, Arc::clone(&spec));
        Ok(spec)
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

    pub fn replace(&self, spec: HttpTaskSpec) -> Result<Arc<HttpTaskSpec>, HttpTaskCatalogError> {
        write_unpoisoned(&self.inner).replace(spec)
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
        let options = HttpTaskOptions {
            max_download_limit: 64 * 1024,
            checksum: Some(HttpContentChecksum::sha256([0xab; 32])),
            ..HttpTaskOptions::default()
        };
        let spec = HttpTaskSpec::new(
            task(1),
            gid(1),
            [
                "https://first.example/file".to_owned(),
                "http://second.example:8080/file".to_owned(),
            ],
            std::env::temp_dir(),
            output(),
            options,
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
        assert_eq!(
            spec.options()
                .sanitized()
                .expect("sanitized")
                .entries()
                .find(|(name, _)| *name == "max-download-limit")
                .map(|(_, value)| value),
            Some("65536")
        );
        assert_eq!(
            HttpTaskOptions::from_sanitized(&spec.options().sanitized().expect("sanitized"))
                .expect("recover options")
                .max_download_limit,
            64 * 1024
        );
        assert_eq!(
            spec.options()
                .sanitized()
                .expect("sanitized")
                .entries()
                .find(|(name, _)| *name == "checksum")
                .map(|(_, value)| value),
            Some("sha-256=abababababababababababababababababababababababababababababababab")
        );
        assert_eq!(
            HttpTaskOptions::from_sanitized(&spec.options().sanitized().expect("sanitized"))
                .expect("recover options")
                .checksum,
            Some(HttpContentChecksum::sha256([0xab; 32]))
        );
    }

    #[test]
    fn checksum_parser_canonicalizes_sha256_and_rejects_unsafe_shapes() {
        let uppercase = "sha-256=ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD";
        let checksum = HttpContentChecksum::parse(uppercase).expect("valid checksum");
        assert_eq!(
            checksum.canonical(),
            "sha-256=abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        assert_eq!(checksum.journal_digest().value(), checksum.value());

        for invalid in [
            "sha-256",
            "sha-512=abcdef",
            "sha-256=abcdef",
            "sha-256=ggcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "sha-256=abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd=",
        ] {
            assert!(HttpContentChecksum::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn resolved_retry_policy_survives_sanitized_option_round_trip() {
        let mut retry = HttpRetryPolicy::custom(
            HttpRetryTriggerSet::parse("timeout,lowest-speed").expect("triggers"),
            HttpRetryStatusSet::parse("418,429").expect("statuses"),
        )
        .expect("custom retry");
        retry.backoff = HttpRetryBackoff::Fixed;
        retry.base_wait = Duration::ZERO;
        retry.max_attempts = NonZeroU32::new(4).expect("attempt cap");
        retry.max_attempts_per_mirror = NonZeroU32::new(2).expect("mirror cap");
        retry.respect_retry_after = false;
        let options = HttpTaskOptions {
            retry: Some(retry.clone()),
            endgame_max_duplicates: 7,
            ..HttpTaskOptions::default()
        };
        let snapshot = options.sanitized().expect("sanitized");
        assert_eq!(
            snapshot
                .entries()
                .find(|(name, _)| *name == "endgame-max-duplicates")
                .map(|(_, value)| value),
            Some("7")
        );
        assert_eq!(
            snapshot
                .entries()
                .find(|(name, _)| *name == "retry-profile")
                .map(|(_, value)| value),
            Some("custom")
        );
        let restored = HttpTaskOptions::from_sanitized(&snapshot).expect("restored");
        assert_eq!(restored.retry, Some(retry));
        assert_eq!(restored.endgame_max_duplicates, 7);
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
