use crate::{CompatStatus, OptionDef, OptionRegistry, OptionValue, Scope, parse_option_value};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::Path;

/// Unknown/unsupported-option behavior for the flat compatibility format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownOptionMode {
    Strict,
    WarnKnownUnsupported,
}

/// Resource limits applied before proportional parser allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatConfigLimits {
    pub max_document_bytes: usize,
    pub max_line_bytes: usize,
    pub max_options: usize,
}

impl Default for FlatConfigLimits {
    fn default() -> Self {
        Self {
            max_document_bytes: 16 * 1024 * 1024,
            max_line_bytes: 64 * 1024,
            max_options: 16_384,
        }
    }
}

/// A one-based source location for a parsed flat option.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceSpan {
    pub line: usize,
    pub column_start: usize,
    pub column_end: usize,
}

/// One typed entry retained with its registry definition and source location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigEntry {
    pub definition: &'static OptionDef,
    pub value: OptionValue,
    pub source: SourceSpan,
}

/// A non-fatal compatibility diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigWarning {
    UnsupportedIgnored {
        name: &'static str,
        line: usize,
    },
    DuplicateReplaced {
        name: &'static str,
        previous_line: usize,
        line: usize,
    },
}

/// A fully validated flat configuration document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlatConfig {
    source_name: Option<String>,
    entries: BTreeMap<&'static str, ConfigEntry>,
    warnings: Vec<ConfigWarning>,
}

impl FlatConfig {
    #[must_use]
    pub fn source_name(&self) -> Option<&str> {
        self.source_name.as_deref()
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.get(name)
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = (&'static str, &ConfigEntry)> {
        self.entries.iter().map(|(name, entry)| (*name, entry))
    }

    #[must_use]
    pub fn warnings(&self) -> &[ConfigWarning] {
        &self.warnings
    }
}

/// A fatal flat configuration error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlatConfigError {
    DocumentTooLarge {
        maximum: usize,
    },
    LineTooLong {
        line: usize,
        maximum: usize,
    },
    TooManyOptions {
        maximum: usize,
    },
    MissingEquals {
        line: usize,
    },
    EmptyName {
        line: usize,
    },
    UnknownOption {
        line: usize,
        name: String,
    },
    UnsupportedOption {
        line: usize,
        name: &'static str,
    },
    UnsafeCompatibilityRequired {
        line: usize,
        name: &'static str,
    },
    FeatureUnavailable {
        line: usize,
        name: &'static str,
    },
    InvalidScope {
        line: usize,
        name: &'static str,
    },
    InvalidValue {
        line: usize,
        name: &'static str,
        message: String,
    },
}

impl fmt::Display for FlatConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { maximum } => {
                write!(
                    formatter,
                    "configuration exceeds the {maximum}-byte document limit"
                )
            }
            Self::LineTooLong { line, maximum } => {
                write!(
                    formatter,
                    "line {line} exceeds the {maximum}-byte line limit"
                )
            }
            Self::TooManyOptions { maximum } => {
                write!(
                    formatter,
                    "configuration exceeds the {maximum}-option limit"
                )
            }
            Self::MissingEquals { line } => write!(formatter, "line {line} is missing '='"),
            Self::EmptyName { line } => write!(formatter, "line {line} has an empty option name"),
            Self::UnknownOption { line, name } => {
                write!(formatter, "line {line} contains unknown option {name}")
            }
            Self::UnsupportedOption { line, name } => {
                write!(formatter, "line {line} contains unsupported option {name}")
            }
            Self::UnsafeCompatibilityRequired { line, name } => write!(
                formatter,
                "line {line} requires explicit unsafe compatibility for option {name}"
            ),
            Self::FeatureUnavailable { line, name } => {
                write!(
                    formatter,
                    "line {line} requires an unavailable feature for option {name}"
                )
            }
            Self::InvalidScope { line, name } => {
                write!(
                    formatter,
                    "line {line} cannot set option {name} in flat config"
                )
            }
            Self::InvalidValue {
                line,
                name,
                message,
            } => write!(
                formatter,
                "line {line} has an invalid value for {name}: {message}"
            ),
        }
    }
}

impl Error for FlatConfigError {}

/// Parses an unnamed aria2-style flat configuration document.
pub fn parse_flat_config(
    registry: OptionRegistry,
    input: &str,
    mode: UnknownOptionMode,
    limits: FlatConfigLimits,
    home: Option<&Path>,
) -> Result<FlatConfig, FlatConfigError> {
    parse_flat_config_with_source(registry, input, None, mode, limits, home)
}

/// Parses an aria2-style flat configuration document and records its source label.
pub fn parse_flat_config_with_source(
    registry: OptionRegistry,
    input: &str,
    source_name: Option<&str>,
    mode: UnknownOptionMode,
    limits: FlatConfigLimits,
    home: Option<&Path>,
) -> Result<FlatConfig, FlatConfigError> {
    if input.len() > limits.max_document_bytes {
        return Err(FlatConfigError::DocumentTooLarge {
            maximum: limits.max_document_bytes,
        });
    }

    let mut entries = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut option_count = 0_usize;
    for (line_index, raw_line) in input.split('\n').enumerate() {
        let line_number = line_index + 1;
        if raw_line.len() > limits.max_line_bytes {
            return Err(FlatConfigError::LineTooLong {
                line: line_number,
                maximum: limits.max_line_bytes,
            });
        }
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        option_count += 1;
        if option_count > limits.max_options {
            return Err(FlatConfigError::TooManyOptions {
                maximum: limits.max_options,
            });
        }

        let (name, value) = line
            .split_once('=')
            .ok_or(FlatConfigError::MissingEquals { line: line_number })?;
        let name = name.trim();
        if name.is_empty() {
            return Err(FlatConfigError::EmptyName { line: line_number });
        }
        let Some(definition) = registry.find(name) else {
            return Err(FlatConfigError::UnknownOption {
                line: line_number,
                name: name.to_owned(),
            });
        };

        match definition.compat {
            CompatStatus::Unsupported if mode == UnknownOptionMode::WarnKnownUnsupported => {
                warnings.push(ConfigWarning::UnsupportedIgnored {
                    name: definition.name,
                    line: line_number,
                });
                continue;
            }
            CompatStatus::Unsupported => {
                return Err(FlatConfigError::UnsupportedOption {
                    line: line_number,
                    name: definition.name,
                });
            }
            CompatStatus::UnsafeCompat => {
                return Err(FlatConfigError::UnsafeCompatibilityRequired {
                    line: line_number,
                    name: definition.name,
                });
            }
            CompatStatus::FeatureGated => {
                return Err(FlatConfigError::FeatureUnavailable {
                    line: line_number,
                    name: definition.name,
                });
            }
            CompatStatus::Implemented | CompatStatus::Partial => {}
        }

        if !has_flat_scope(definition) {
            return Err(FlatConfigError::InvalidScope {
                line: line_number,
                name: definition.name,
            });
        }
        let value = parse_option_value(definition, value.trim(), home).map_err(|error| {
            FlatConfigError::InvalidValue {
                line: line_number,
                name: definition.name,
                message: error.to_string(),
            }
        })?;
        let source = SourceSpan {
            line: line_number,
            column_start: 1,
            column_end: line.len() + 1,
        };
        let entry = ConfigEntry {
            definition,
            value,
            source,
        };
        if let Some(previous) = entries.insert(definition.name, entry) {
            warnings.push(ConfigWarning::DuplicateReplaced {
                name: definition.name,
                previous_line: previous.source.line,
                line: line_number,
            });
        }
    }

    Ok(FlatConfig {
        source_name: source_name.map(str::to_owned),
        entries,
        warnings,
    })
}

fn has_flat_scope(definition: &OptionDef) -> bool {
    [Scope::Startup, Scope::Global, Scope::PerDownload]
        .into_iter()
        .any(|scope| definition.scopes.contains(scope))
}

#[cfg(test)]
mod tests {
    use super::{
        ConfigWarning, FlatConfigError, FlatConfigLimits, UnknownOptionMode, parse_flat_config,
        parse_flat_config_with_source,
    };
    use crate::{OptionValue, builtin_registry};
    use std::path::{Path, PathBuf};

    fn parse(input: &str) -> Result<super::FlatConfig, FlatConfigError> {
        parse_flat_config(
            builtin_registry(),
            input,
            UnknownOptionMode::Strict,
            FlatConfigLimits::default(),
            Some(Path::new("/home/tester")),
        )
    }

    #[test]
    fn parses_comments_crlf_first_equals_and_source_spans() {
        let config = parse_flat_config_with_source(
            builtin_registry(),
            "  # comment\r\ndir=${HOME}/Downloads\r\nout=name=part.iso\r\n",
            Some("ariax.conf"),
            UnknownOptionMode::Strict,
            FlatConfigLimits::default(),
            Some(Path::new("/home/tester")),
        )
        .expect("valid config");
        assert_eq!(config.source_name(), Some("ariax.conf"));
        assert_eq!(
            config.get("dir").expect("dir").value,
            OptionValue::Path(PathBuf::from("/home/tester/Downloads"))
        );
        assert_eq!(
            config.get("out").expect("out").value,
            OptionValue::String("name=part.iso".to_owned())
        );
        assert_eq!(config.get("dir").expect("dir").source.line, 2);
    }

    #[test]
    fn strict_and_compatibility_modes_distinguish_known_unsupported_options() {
        assert_eq!(
            parse("enable-http-pipelining=false"),
            Err(FlatConfigError::UnsupportedOption {
                line: 1,
                name: "enable-http-pipelining",
            })
        );
        let config = parse_flat_config(
            builtin_registry(),
            "enable-http-pipelining=false\nsplit=8",
            UnknownOptionMode::WarnKnownUnsupported,
            FlatConfigLimits::default(),
            None,
        )
        .expect("known unsupported option warns");
        assert_eq!(
            config.warnings(),
            &[ConfigWarning::UnsupportedIgnored {
                name: "enable-http-pipelining",
                line: 1,
            }]
        );
        assert_eq!(
            config.get("split").expect("split").value,
            OptionValue::Integer(8)
        );
        assert!(config.get("enable-http-pipelining").is_none());
    }

    #[test]
    fn compatibility_mode_never_ignores_unknown_options() {
        assert_eq!(
            parse_flat_config(
                builtin_registry(),
                "made-up=true",
                UnknownOptionMode::WarnKnownUnsupported,
                FlatConfigLimits::default(),
                None,
            ),
            Err(FlatConfigError::UnknownOption {
                line: 1,
                name: "made-up".to_owned(),
            })
        );
    }

    #[test]
    fn duplicate_options_replace_deterministically_with_a_warning() {
        let config = parse("split=4\nsplit=9").expect("valid duplicate");
        assert_eq!(
            config.get("split").expect("split").value,
            OptionValue::Integer(9)
        );
        assert_eq!(
            config.warnings(),
            &[ConfigWarning::DuplicateReplaced {
                name: "split",
                previous_line: 1,
                line: 2,
            }]
        );
    }

    #[test]
    fn limits_are_checked_before_unbounded_collection_growth() {
        let limits = FlatConfigLimits {
            max_document_bytes: 8,
            max_line_bytes: 4,
            max_options: 1,
        };
        assert_eq!(
            parse_flat_config(
                builtin_registry(),
                "123456789",
                UnknownOptionMode::Strict,
                limits,
                None,
            ),
            Err(FlatConfigError::DocumentTooLarge { maximum: 8 })
        );
        let limits = FlatConfigLimits {
            max_document_bytes: 128,
            ..limits
        };
        assert_eq!(
            parse_flat_config(
                builtin_registry(),
                "split=8",
                UnknownOptionMode::Strict,
                limits,
                None,
            ),
            Err(FlatConfigError::LineTooLong {
                line: 1,
                maximum: 4,
            })
        );
        let limits = FlatConfigLimits {
            max_line_bytes: 64,
            ..limits
        };
        assert_eq!(
            parse_flat_config(
                builtin_registry(),
                "split=8\ncontinue=true",
                UnknownOptionMode::Strict,
                limits,
                None,
            ),
            Err(FlatConfigError::TooManyOptions { maximum: 1 })
        );
    }

    #[test]
    fn unsafe_compatibility_is_never_enabled_by_parser_mode() {
        assert_eq!(
            parse_flat_config(
                builtin_registry(),
                "on-download-complete=touch /tmp/done",
                UnknownOptionMode::WarnKnownUnsupported,
                FlatConfigLimits::default(),
                None,
            ),
            Err(FlatConfigError::UnsafeCompatibilityRequired {
                line: 1,
                name: "on-download-complete",
            })
        );
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let config = parse("rpc-secret=super-secret-value").expect("secret config");
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("super-secret-value"));
    }
}
