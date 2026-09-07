#![forbid(unsafe_code)]

//! Typed option metadata and bounded configuration parsing.

mod flat;
mod registry;
mod value;

pub use flat::{
    ConfigEntry, ConfigWarning, FlatConfig, FlatConfigError, FlatConfigLimits, SourceSpan,
    UnknownOptionMode, parse_flat_config, parse_flat_config_with_source,
};
pub use registry::{
    ALL_COMPAT_STATUSES, ALL_COMPATIBILITY_DIFFERENCES, ALL_RUNTIME_UPDATES, ALL_SCOPES,
    ALL_SECURITY_CLASSES, BUILTIN_OPTIONS, CompatStatus, CompatibilityDifference, OptionDef,
    OptionRegistry, RegistryError, RuntimeUpdate, Scope, ScopeSet, SecurityClass, ValueType,
    builtin_registry, persisted_option_is_safe,
};
pub use value::{OptionValue, ParseOptionValueError, SecretString, parse_option_value};
