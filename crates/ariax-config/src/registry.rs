use crate::value::parse_option_value_shape;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

/// How an accepted option change affects existing work.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuntimeUpdate {
    None,
    Live,
    WaitingOnly,
    ActiveRestart,
    NewGeneration,
    StartupOnly,
    UnsafeCompatOnly,
    BtLive,
    BtRestartRequired,
}

impl RuntimeUpdate {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Live => "live",
            Self::WaitingOnly => "waiting_only",
            Self::ActiveRestart => "active_restart",
            Self::NewGeneration => "new_generation",
            Self::StartupOnly => "startup_only",
            Self::UnsafeCompatOnly => "unsafe_compat_only",
            Self::BtLive => "bt_live",
            Self::BtRestartRequired => "bt_restart_required",
        }
    }
}

pub const ALL_RUNTIME_UPDATES: [RuntimeUpdate; 9] = [
    RuntimeUpdate::None,
    RuntimeUpdate::Live,
    RuntimeUpdate::WaitingOnly,
    RuntimeUpdate::ActiveRestart,
    RuntimeUpdate::NewGeneration,
    RuntimeUpdate::StartupOnly,
    RuntimeUpdate::UnsafeCompatOnly,
    RuntimeUpdate::BtLive,
    RuntimeUpdate::BtRestartRequired,
];

/// The declared compatibility status of an option.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompatStatus {
    Implemented,
    Partial,
    Unsupported,
    UnsafeCompat,
    FeatureGated,
}

impl CompatStatus {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::Partial => "partial",
            Self::Unsupported => "unsupported",
            Self::UnsafeCompat => "unsafe_compat",
            Self::FeatureGated => "feature_gated",
        }
    }
}

pub const ALL_COMPAT_STATUSES: [CompatStatus; 5] = [
    CompatStatus::Implemented,
    CompatStatus::Partial,
    CompatStatus::Unsupported,
    CompatStatus::UnsafeCompat,
    CompatStatus::FeatureGated,
];

/// Why ariax intentionally differs from aria2 for an option.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompatibilityDifference {
    None,
    Required,
    Intentional,
    Unresolved,
}

impl CompatibilityDifference {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Required => "required",
            Self::Intentional => "intentional",
            Self::Unresolved => "unresolved",
        }
    }
}

pub const ALL_COMPATIBILITY_DIFFERENCES: [CompatibilityDifference; 4] = [
    CompatibilityDifference::None,
    CompatibilityDifference::Required,
    CompatibilityDifference::Intentional,
    CompatibilityDifference::Unresolved,
];

/// Security handling required for an option value or effect.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SecurityClass {
    Normal,
    Sensitive,
    UnsafeExec,
    LocalAdmin,
}

impl SecurityClass {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Sensitive => "sensitive",
            Self::UnsafeExec => "unsafe_exec",
            Self::LocalAdmin => "local_admin",
        }
    }
}

pub const ALL_SECURITY_CLASSES: [SecurityClass; 4] = [
    SecurityClass::Normal,
    SecurityClass::Sensitive,
    SecurityClass::UnsafeExec,
    SecurityClass::LocalAdmin,
];

/// One surface on which an option may appear.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u16)]
pub enum Scope {
    Startup = 1 << 0,
    Global = 1 << 1,
    PerDownload = 1 << 2,
    InputFile = 1 << 3,
    RpcChange = 1 << 4,
    RpcGlobal = 1 << 5,
    UrlRule = 1 << 6,
}

impl Scope {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Global => "global",
            Self::PerDownload => "per_download",
            Self::InputFile => "input_file",
            Self::RpcChange => "rpc_change",
            Self::RpcGlobal => "rpc_global",
            Self::UrlRule => "url_rule",
        }
    }
}

pub const ALL_SCOPES: [Scope; 7] = [
    Scope::Startup,
    Scope::Global,
    Scope::PerDownload,
    Scope::InputFile,
    Scope::RpcChange,
    Scope::RpcGlobal,
    Scope::UrlRule,
];

/// A compact set of allowed option surfaces.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ScopeSet(u16);

impl ScopeSet {
    pub const EMPTY: Self = Self(0);

    #[must_use]
    pub const fn one(scope: Scope) -> Self {
        Self(scope as u16)
    }

    #[must_use]
    pub const fn with(self, scope: Scope) -> Self {
        Self(self.0 | scope as u16)
    }

    #[must_use]
    pub const fn contains(self, scope: Scope) -> bool {
        self.0 & scope as u16 != 0
    }

    pub fn iter(self) -> impl Iterator<Item = Scope> {
        ALL_SCOPES
            .into_iter()
            .filter(move |scope| self.contains(*scope))
    }
}

/// The bounded parser shape of an option value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueType {
    Bool,
    Integer {
        min: i64,
        max: i64,
    },
    SizeBytes {
        min: u64,
        max: u64,
    },
    DurationSeconds {
        min: u64,
        max: u64,
    },
    Enum {
        values: &'static [&'static str],
    },
    String {
        max_len: usize,
    },
    Path {
        max_len: usize,
        expand_home: bool,
    },
    HeaderList {
        max_items: usize,
        max_item_len: usize,
    },
    StatusCodeSet {
        max_items: usize,
    },
    SecretString {
        max_len: usize,
    },
}

impl ValueType {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Integer { .. } => "integer",
            Self::SizeBytes { .. } => "size_bytes",
            Self::DurationSeconds { .. } => "duration_seconds",
            Self::Enum { .. } => "enum",
            Self::String { .. } => "string",
            Self::Path { .. } => "path",
            Self::HeaderList { .. } => "header_list",
            Self::StatusCodeSet { .. } => "status_code_set",
            Self::SecretString { .. } => "secret_string",
        }
    }
}

/// One canonical option-registry entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OptionDef {
    pub name: &'static str,
    pub short: Option<char>,
    pub value_type: ValueType,
    pub default: Option<&'static str>,
    pub category: &'static str,
    pub scopes: ScopeSet,
    pub runtime_update: RuntimeUpdate,
    pub owner: &'static str,
    pub build_features: &'static [&'static str],
    pub security: SecurityClass,
    pub compat: CompatStatus,
    pub aria2_available: bool,
    pub aria2_runtime_update: RuntimeUpdate,
    pub compatibility_difference: CompatibilityDifference,
    pub docs: &'static str,
    pub behavior_tests: &'static [&'static str],
}

const STARTUP_GLOBAL_DOWNLOAD: ScopeSet = ScopeSet::one(Scope::Startup)
    .with(Scope::Global)
    .with(Scope::PerDownload)
    .with(Scope::InputFile)
    .with(Scope::RpcChange)
    .with(Scope::RpcGlobal);
const DOWNLOAD_SCOPES: ScopeSet = ScopeSet::one(Scope::Global)
    .with(Scope::PerDownload)
    .with(Scope::InputFile)
    .with(Scope::RpcChange)
    .with(Scope::RpcGlobal)
    .with(Scope::UrlRule);
const STARTUP_ONLY: ScopeSet = ScopeSet::one(Scope::Startup);
const GLOBAL_LIVE: ScopeSet = ScopeSet::one(Scope::Global).with(Scope::RpcGlobal);

const PARTIAL: CompatStatus = CompatStatus::Partial;
const MINIMAL: &[&str] = &["minimal"];
const COMPAT: &[&str] = &["compat"];
const NONE: &[&str] = &[];

/// The first reviewed registry slice. It grows until every upstream and extension option is covered.
pub const BUILTIN_OPTIONS: &[OptionDef] = &[
    OptionDef {
        name: "allow-overwrite",
        short: None,
        value_type: ValueType::Bool,
        default: Some("false"),
        category: "file",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::WaitingOnly,
        owner: "storage",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#compatibility-matrix-categories",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "continue",
        short: Some('c'),
        value_type: ValueType::Bool,
        default: Some("false"),
        category: "transfer",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::WaitingOnly,
        owner: "storage",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#compatibility-matrix-categories",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "connect-timeout",
        short: None,
        value_type: ValueType::DurationSeconds { min: 1, max: 600 },
        default: Some("60"),
        category: "retry",
        scopes: STARTUP_GLOBAL_DOWNLOAD,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "http",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::None,
        docs: "retry-policy.md#policy-options",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "dir",
        short: Some('d'),
        value_type: ValueType::Path {
            max_len: 65_536,
            expand_home: true,
        },
        default: Some("."),
        category: "file",
        scopes: STARTUP_GLOBAL_DOWNLOAD,
        runtime_update: RuntimeUpdate::NewGeneration,
        owner: "safe_path",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::Required,
        docs: "detailed-storage.md#safepathbuilder",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "out",
        short: Some('o'),
        value_type: ValueType::String { max_len: 65_536 },
        default: None,
        category: "file",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::NewGeneration,
        owner: "safe_path",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::Required,
        docs: "detailed-storage.md#safepathbuilder",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "split",
        short: Some('s'),
        value_type: ValueType::Integer { min: 1, max: 1024 },
        default: Some("5"),
        category: "transfer",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "segment_scheduler",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::ActiveRestart,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#rpc-option-compatibility",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "timeout",
        short: Some('t'),
        value_type: ValueType::DurationSeconds { min: 1, max: 600 },
        default: Some("60"),
        category: "retry",
        scopes: STARTUP_GLOBAL_DOWNLOAD,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "http",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::None,
        docs: "retry-policy.md#policy-options",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "max-connection-per-server",
        short: Some('x'),
        value_type: ValueType::Integer { min: 1, max: 1024 },
        default: Some("1"),
        category: "transfer",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "segment_scheduler",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::ActiveRestart,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#rpc-option-compatibility",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "min-split-size",
        short: Some('k'),
        value_type: ValueType::SizeBytes {
            min: 1_048_576,
            max: u64::MAX,
        },
        default: Some("20M"),
        category: "transfer",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "segment_scheduler",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::ActiveRestart,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#rpc-option-compatibility",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "piece-length",
        short: None,
        value_type: ValueType::SizeBytes {
            min: 1_048_576,
            max: 1_073_741_824,
        },
        default: Some("1M"),
        category: "storage",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::NewGeneration,
        owner: "storage",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::Required,
        docs: "detailed-storage.md#piece-model",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "max-download-limit",
        short: None,
        value_type: ValueType::SizeBytes {
            min: 0,
            max: u64::MAX,
        },
        default: Some("0"),
        category: "rate",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::Live,
        owner: "rate_limiter",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::Live,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#rpc-option-compatibility",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "max-overall-download-limit",
        short: None,
        value_type: ValueType::SizeBytes {
            min: 0,
            max: u64::MAX,
        },
        default: Some("0"),
        category: "rate",
        scopes: GLOBAL_LIVE,
        runtime_update: RuntimeUpdate::Live,
        owner: "rate_limiter",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::Live,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#rpc-option-compatibility",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "lowest-speed-limit",
        short: None,
        value_type: ValueType::SizeBytes {
            min: 0,
            max: u64::MAX,
        },
        default: Some("0"),
        category: "retry",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "stall_detector",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::ActiveRestart,
        compatibility_difference: CompatibilityDifference::None,
        docs: "configuration.md#rpc-option-compatibility",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "retry-on-http-status",
        short: None,
        value_type: ValueType::StatusCodeSet { max_items: 128 },
        default: Some("408,425,429,500,502,503,504"),
        category: "retry",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::Live,
        owner: "retry_policy",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: false,
        aria2_runtime_update: RuntimeUpdate::None,
        compatibility_difference: CompatibilityDifference::Intentional,
        docs: "retry-policy.md",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "checksum",
        short: None,
        value_type: ValueType::String { max_len: 72 },
        default: None,
        category: "integrity",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "checksum_verifier",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::None,
        compatibility_difference: CompatibilityDifference::Intentional,
        docs: "detailed-http-first-slice.md#cross-mirror-entity-identity",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "verify-mirror-identity",
        short: None,
        value_type: ValueType::Enum {
            values: &["off", "strict"],
        },
        default: Some("off"),
        category: "integrity",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::ActiveRestart,
        owner: "segment_scheduler",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: false,
        aria2_runtime_update: RuntimeUpdate::None,
        compatibility_difference: CompatibilityDifference::Intentional,
        docs: "detailed-http-first-slice.md#cross-mirror-entity-identity",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "durability",
        short: None,
        value_type: ValueType::Enum {
            values: &["fast", "balanced", "strict"],
        },
        default: Some("balanced"),
        category: "storage",
        scopes: STARTUP_GLOBAL_DOWNLOAD,
        runtime_update: RuntimeUpdate::NewGeneration,
        owner: "storage",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: false,
        aria2_runtime_update: RuntimeUpdate::None,
        compatibility_difference: CompatibilityDifference::Intentional,
        docs: "detailed-storage.md#write-and-journal-ordering",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "session-store",
        short: None,
        value_type: ValueType::Enum {
            values: &["hybrid", "memory"],
        },
        default: Some("hybrid"),
        category: "session",
        scopes: STARTUP_ONLY,
        runtime_update: RuntimeUpdate::StartupOnly,
        owner: "session_store",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: false,
        aria2_runtime_update: RuntimeUpdate::None,
        compatibility_difference: CompatibilityDifference::Intentional,
        docs: "configuration.md#new-options",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "rpc-secret",
        short: None,
        value_type: ValueType::SecretString { max_len: 4096 },
        default: None,
        category: "rpc",
        scopes: STARTUP_ONLY,
        runtime_update: RuntimeUpdate::StartupOnly,
        owner: "rpc",
        build_features: MINIMAL,
        security: SecurityClass::Sensitive,
        compat: PARTIAL,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::StartupOnly,
        compatibility_difference: CompatibilityDifference::None,
        docs: "apis-and-embedding.md#rpc-authentication",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "event-backend",
        short: None,
        value_type: ValueType::Enum {
            values: &["auto", "tokio"],
        },
        default: Some("auto"),
        category: "runtime",
        scopes: STARTUP_ONLY,
        runtime_update: RuntimeUpdate::StartupOnly,
        owner: "runtime",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: false,
        aria2_runtime_update: RuntimeUpdate::None,
        compatibility_difference: CompatibilityDifference::Required,
        docs: "configuration.md#compatibility-matrix-categories",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "disk-io-backend",
        short: None,
        value_type: ValueType::Enum {
            values: &["auto", "uring", "iocp", "blocking"],
        },
        default: Some("auto"),
        category: "runtime",
        scopes: STARTUP_ONLY,
        runtime_update: RuntimeUpdate::StartupOnly,
        owner: "disk_backend",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: PARTIAL,
        aria2_available: false,
        aria2_runtime_update: RuntimeUpdate::None,
        compatibility_difference: CompatibilityDifference::Intentional,
        docs: "configuration.md#compatibility-matrix-categories",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "on-download-complete",
        short: None,
        value_type: ValueType::String { max_len: 65_536 },
        default: None,
        category: "hook",
        scopes: STARTUP_GLOBAL_DOWNLOAD,
        runtime_update: RuntimeUpdate::UnsafeCompatOnly,
        owner: "event_hook_service",
        build_features: COMPAT,
        security: SecurityClass::UnsafeExec,
        compat: CompatStatus::UnsafeCompat,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::StartupOnly,
        compatibility_difference: CompatibilityDifference::Required,
        docs: "security-recovery.md#no-rce-policy",
        behavior_tests: NONE,
    },
    OptionDef {
        name: "enable-http-pipelining",
        short: None,
        value_type: ValueType::Bool,
        default: Some("false"),
        category: "http",
        scopes: DOWNLOAD_SCOPES,
        runtime_update: RuntimeUpdate::None,
        owner: "http",
        build_features: MINIMAL,
        security: SecurityClass::Normal,
        compat: CompatStatus::Unsupported,
        aria2_available: true,
        aria2_runtime_update: RuntimeUpdate::WaitingOnly,
        compatibility_difference: CompatibilityDifference::Required,
        docs: "configuration.md#compatibility-matrix-categories",
        behavior_tests: NONE,
    },
];

/// A validated immutable view of option definitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OptionRegistry {
    definitions: &'static [OptionDef],
}

impl OptionRegistry {
    pub fn new(definitions: &'static [OptionDef]) -> Result<Self, RegistryError> {
        let registry = Self { definitions };
        registry.validate()?;
        Ok(registry)
    }

    #[must_use]
    pub const fn definitions(self) -> &'static [OptionDef] {
        self.definitions
    }

    #[must_use]
    pub fn find(self, name: &str) -> Option<&'static OptionDef> {
        self.definitions
            .iter()
            .find(|definition| definition.name == name)
    }

    fn validate(self) -> Result<(), RegistryError> {
        let mut names = BTreeSet::new();
        let mut shorts = BTreeSet::new();
        for definition in self.definitions {
            if definition.name.is_empty()
                || !definition
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            {
                return Err(RegistryError::InvalidName(definition.name));
            }
            if !names.insert(definition.name) {
                return Err(RegistryError::DuplicateName(definition.name));
            }
            if let Some(short) = definition.short
                && !shorts.insert(short)
            {
                return Err(RegistryError::DuplicateShort(short));
            }
            if definition.compat == CompatStatus::Implemented
                && definition.behavior_tests.is_empty()
            {
                return Err(RegistryError::MissingBehaviorTest(definition.name));
            }
            if definition.compat == CompatStatus::UnsafeCompat
                && (definition.security != SecurityClass::UnsafeExec
                    || definition.runtime_update != RuntimeUpdate::UnsafeCompatOnly)
            {
                return Err(RegistryError::UnsafeCompatMismatch(definition.name));
            }
            if let Some(default) = definition.default {
                parse_option_value_shape(definition, default, None).map_err(|error| {
                    RegistryError::InvalidDefault {
                        name: definition.name,
                        message: error.to_string(),
                    }
                })?;
            }
        }
        Ok(())
    }
}

/// Returns the validated built-in registry.
pub fn builtin_registry() -> OptionRegistry {
    OptionRegistry::new(BUILTIN_OPTIONS).expect("built-in option registry must be valid")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    InvalidName(&'static str),
    DuplicateName(&'static str),
    DuplicateShort(char),
    MissingBehaviorTest(&'static str),
    UnsafeCompatMismatch(&'static str),
    InvalidDefault { name: &'static str, message: String },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName(name) => write!(formatter, "invalid option name {name}"),
            Self::DuplicateName(name) => write!(formatter, "duplicate option name {name}"),
            Self::DuplicateShort(short) => write!(formatter, "duplicate short option -{short}"),
            Self::MissingBehaviorTest(name) => {
                write!(formatter, "implemented option {name} has no behavior test")
            }
            Self::UnsafeCompatMismatch(name) => {
                write!(
                    formatter,
                    "unsafe compatibility metadata is inconsistent for {name}"
                )
            }
            Self::InvalidDefault { name, message } => {
                write!(formatter, "invalid default for {name}: {message}")
            }
        }
    }
}

impl Error for RegistryError {}

#[cfg(test)]
mod tests {
    use super::{
        BUILTIN_OPTIONS, CompatStatus, OptionDef, OptionRegistry, RegistryError, RuntimeUpdate,
        Scope, ScopeSet, SecurityClass, ValueType, builtin_registry,
    };

    #[test]
    fn built_in_registry_is_unique_and_has_valid_defaults() {
        let registry = builtin_registry();
        assert_eq!(registry.definitions(), BUILTIN_OPTIONS);
        assert_eq!(registry.find("split").expect("split").short, Some('s'));
        assert_eq!(
            registry.find("checksum").expect("checksum").value_type,
            ValueType::String { max_len: 72 }
        );
        assert!(registry.find("missing").is_none());
    }

    #[test]
    fn implemented_options_require_behavior_tests() {
        static BAD: &[OptionDef] = &[OptionDef {
            name: "test-option",
            short: None,
            value_type: ValueType::Bool,
            default: Some("false"),
            category: "test",
            scopes: ScopeSet::one(Scope::Startup),
            runtime_update: RuntimeUpdate::StartupOnly,
            owner: "test",
            build_features: &[],
            security: SecurityClass::Normal,
            compat: CompatStatus::Implemented,
            aria2_available: false,
            aria2_runtime_update: RuntimeUpdate::None,
            compatibility_difference: super::CompatibilityDifference::None,
            docs: "test",
            behavior_tests: &[],
        }];
        assert_eq!(
            OptionRegistry::new(BAD),
            Err(RegistryError::MissingBehaviorTest("test-option"))
        );
    }
}
