use crate::{CompatStatus, OptionDef, ValueType};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

/// A value whose formatting is always redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString([REDACTED])")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// A successfully parsed option value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptionValue {
    Bool(bool),
    Integer(i64),
    SizeBytes(u64),
    DurationSeconds(u64),
    Enum(String),
    String(String),
    Path(PathBuf),
    HeaderList(Vec<String>),
    StatusCodeSet(BTreeSet<u16>),
    Secret(SecretString),
}

/// A bounded option-value parsing failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseOptionValueError {
    Unsupported(&'static str),
    InvalidBoolean(&'static str),
    InvalidInteger(&'static str),
    InvalidSize(&'static str),
    InvalidDuration(&'static str),
    InvalidEnum {
        option: &'static str,
        allowed: &'static [&'static str],
    },
    OutOfRange {
        option: &'static str,
        minimum: i128,
        maximum: i128,
    },
    TooLong {
        option: &'static str,
        maximum: usize,
    },
    NulByte(&'static str),
    InvalidHomeExpansion(&'static str),
    MissingHome(&'static str),
    InvalidHeader(&'static str),
    InvalidStatusCode(&'static str),
    TooManyItems {
        option: &'static str,
        maximum: usize,
    },
}

impl fmt::Display for ParseOptionValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(option) => write!(formatter, "option {option} is unsupported"),
            Self::InvalidBoolean(option) => {
                write!(formatter, "option {option} expects true or false")
            }
            Self::InvalidInteger(option) => write!(formatter, "option {option} expects an integer"),
            Self::InvalidSize(option) => write!(
                formatter,
                "option {option} expects an integer byte count with an optional K, M, G, or T suffix"
            ),
            Self::InvalidDuration(option) => {
                write!(
                    formatter,
                    "option {option} expects an integer number of seconds"
                )
            }
            Self::InvalidEnum { option, allowed } => {
                write!(
                    formatter,
                    "option {option} expects one of {}",
                    allowed.join(", ")
                )
            }
            Self::OutOfRange {
                option,
                minimum,
                maximum,
            } => write!(
                formatter,
                "option {option} must be between {minimum} and {maximum}"
            ),
            Self::TooLong { option, maximum } => {
                write!(
                    formatter,
                    "option {option} exceeds the {maximum}-byte limit"
                )
            }
            Self::NulByte(option) => write!(formatter, "option {option} contains a NUL byte"),
            Self::InvalidHomeExpansion(option) => write!(
                formatter,
                "option {option} only permits the ${{HOME}} path expansion"
            ),
            Self::MissingHome(option) => write!(
                formatter,
                "option {option} uses ${{HOME}}, but no home directory was supplied"
            ),
            Self::InvalidHeader(option) => write!(
                formatter,
                "option {option} contains an empty header or a control character"
            ),
            Self::InvalidStatusCode(option) => write!(
                formatter,
                "option {option} expects comma-separated HTTP status codes or inclusive ranges"
            ),
            Self::TooManyItems { option, maximum } => {
                write!(
                    formatter,
                    "option {option} exceeds the {maximum}-item limit"
                )
            }
        }
    }
}

impl Error for ParseOptionValueError {}

/// Parses one value according to its registry definition.
pub fn parse_option_value(
    definition: &OptionDef,
    input: &str,
    home: Option<&Path>,
) -> Result<OptionValue, ParseOptionValueError> {
    if definition.compat == CompatStatus::Unsupported {
        return Err(ParseOptionValueError::Unsupported(definition.name));
    }

    parse_option_value_shape(definition, input, home)
}

pub(crate) fn parse_option_value_shape(
    definition: &OptionDef,
    input: &str,
    home: Option<&Path>,
) -> Result<OptionValue, ParseOptionValueError> {
    match definition.value_type {
        ValueType::Bool => parse_bool(definition.name, input),
        ValueType::Integer { min, max } => parse_integer(definition.name, input, min, max),
        ValueType::SizeBytes { min, max } => parse_size(definition.name, input, min, max),
        ValueType::DurationSeconds { min, max } => parse_duration(definition.name, input, min, max),
        ValueType::Enum { values } => parse_enum(definition.name, input, values),
        ValueType::String { max_len } => {
            validate_text(definition.name, input, max_len)?;
            Ok(OptionValue::String(input.to_owned()))
        }
        ValueType::Path {
            max_len,
            expand_home,
        } => parse_path(definition.name, input, max_len, expand_home, home),
        ValueType::HeaderList {
            max_items,
            max_item_len,
        } => parse_headers(definition.name, input, max_items, max_item_len),
        ValueType::StatusCodeSet { max_items } => {
            parse_status_codes(definition.name, input, max_items)
        }
        ValueType::SecretString { max_len } => {
            validate_text(definition.name, input, max_len)?;
            Ok(OptionValue::Secret(SecretString(input.to_owned())))
        }
    }
}

fn parse_bool(option: &'static str, input: &str) -> Result<OptionValue, ParseOptionValueError> {
    match input.trim() {
        "true" => Ok(OptionValue::Bool(true)),
        "false" => Ok(OptionValue::Bool(false)),
        _ => Err(ParseOptionValueError::InvalidBoolean(option)),
    }
}

fn parse_integer(
    option: &'static str,
    input: &str,
    min: i64,
    max: i64,
) -> Result<OptionValue, ParseOptionValueError> {
    let value = input
        .trim()
        .parse::<i64>()
        .map_err(|_| ParseOptionValueError::InvalidInteger(option))?;
    if !(min..=max).contains(&value) {
        return Err(ParseOptionValueError::OutOfRange {
            option,
            minimum: i128::from(min),
            maximum: i128::from(max),
        });
    }
    Ok(OptionValue::Integer(value))
}

fn parse_size(
    option: &'static str,
    input: &str,
    min: u64,
    max: u64,
) -> Result<OptionValue, ParseOptionValueError> {
    let normalized = input.trim().to_ascii_uppercase();
    let (digits, multiplier) = [
        ("KB", 1_u64 << 10),
        ("MB", 1_u64 << 20),
        ("GB", 1_u64 << 30),
        ("TB", 1_u64 << 40),
        ("K", 1_u64 << 10),
        ("M", 1_u64 << 20),
        ("G", 1_u64 << 30),
        ("T", 1_u64 << 40),
        ("B", 1),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        normalized
            .strip_suffix(suffix)
            .map(|digits| (digits, multiplier))
    })
    .unwrap_or((&normalized, 1));

    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ParseOptionValueError::InvalidSize(option));
    }
    let value = digits
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(multiplier))
        .ok_or(ParseOptionValueError::InvalidSize(option))?;
    validate_unsigned_range(option, value, min, max)?;
    Ok(OptionValue::SizeBytes(value))
}

fn parse_duration(
    option: &'static str,
    input: &str,
    min: u64,
    max: u64,
) -> Result<OptionValue, ParseOptionValueError> {
    let value = input
        .trim()
        .parse::<u64>()
        .map_err(|_| ParseOptionValueError::InvalidDuration(option))?;
    validate_unsigned_range(option, value, min, max)?;
    Ok(OptionValue::DurationSeconds(value))
}

fn parse_enum(
    option: &'static str,
    input: &str,
    values: &'static [&'static str],
) -> Result<OptionValue, ParseOptionValueError> {
    let input = input.trim();
    if values.contains(&input) {
        Ok(OptionValue::Enum(input.to_owned()))
    } else {
        Err(ParseOptionValueError::InvalidEnum {
            option,
            allowed: values,
        })
    }
}

fn parse_path(
    option: &'static str,
    input: &str,
    max_len: usize,
    expand_home: bool,
    home: Option<&Path>,
) -> Result<OptionValue, ParseOptionValueError> {
    validate_text(option, input, max_len)?;
    let Some(remainder) = input.strip_prefix("${HOME}") else {
        if input.contains("${") {
            return Err(ParseOptionValueError::InvalidHomeExpansion(option));
        }
        return Ok(OptionValue::Path(PathBuf::from(input)));
    };
    if !expand_home || remainder.contains("${") {
        return Err(ParseOptionValueError::InvalidHomeExpansion(option));
    }
    let home = home.ok_or(ParseOptionValueError::MissingHome(option))?;
    let remainder = remainder.trim_start_matches(['/', '\\']);
    let expanded = if remainder.is_empty() {
        home.to_path_buf()
    } else {
        home.join(remainder)
    };
    if expanded.as_os_str().to_string_lossy().len() > max_len {
        return Err(ParseOptionValueError::TooLong {
            option,
            maximum: max_len,
        });
    }
    Ok(OptionValue::Path(expanded))
}

fn parse_headers(
    option: &'static str,
    input: &str,
    max_items: usize,
    max_item_len: usize,
) -> Result<OptionValue, ParseOptionValueError> {
    let headers = input.split('\n').collect::<Vec<_>>();
    if headers.len() > max_items {
        return Err(ParseOptionValueError::TooManyItems {
            option,
            maximum: max_items,
        });
    }
    if headers.iter().any(|header| {
        header.is_empty()
            || header.len() > max_item_len
            || header.bytes().any(|byte| byte.is_ascii_control())
    }) {
        return Err(ParseOptionValueError::InvalidHeader(option));
    }
    Ok(OptionValue::HeaderList(
        headers.into_iter().map(str::to_owned).collect(),
    ))
}

fn parse_status_codes(
    option: &'static str,
    input: &str,
    max_items: usize,
) -> Result<OptionValue, ParseOptionValueError> {
    let mut codes = BTreeSet::new();
    if input.trim().is_empty() {
        return Ok(OptionValue::StatusCodeSet(codes));
    }
    for item in input.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(ParseOptionValueError::InvalidStatusCode(option));
        }
        if let Some((start, end)) = item.split_once('-') {
            if end.contains('-') {
                return Err(ParseOptionValueError::InvalidStatusCode(option));
            }
            let start = parse_status_code(option, start.trim())?;
            let end = parse_status_code(option, end.trim())?;
            if start > end {
                return Err(ParseOptionValueError::InvalidStatusCode(option));
            }
            for code in start..=end {
                codes.insert(code);
                enforce_item_limit(option, codes.len(), max_items)?;
            }
        } else {
            codes.insert(parse_status_code(option, item)?);
            enforce_item_limit(option, codes.len(), max_items)?;
        }
    }
    Ok(OptionValue::StatusCodeSet(codes))
}

fn parse_status_code(option: &'static str, input: &str) -> Result<u16, ParseOptionValueError> {
    let code = input
        .parse::<u16>()
        .map_err(|_| ParseOptionValueError::InvalidStatusCode(option))?;
    if !(100..=599).contains(&code) {
        return Err(ParseOptionValueError::InvalidStatusCode(option));
    }
    Ok(code)
}

fn validate_unsigned_range(
    option: &'static str,
    value: u64,
    min: u64,
    max: u64,
) -> Result<(), ParseOptionValueError> {
    if !(min..=max).contains(&value) {
        return Err(ParseOptionValueError::OutOfRange {
            option,
            minimum: i128::from(min),
            maximum: i128::from(max),
        });
    }
    Ok(())
}

fn validate_text(
    option: &'static str,
    input: &str,
    max_len: usize,
) -> Result<(), ParseOptionValueError> {
    if input.len() > max_len {
        return Err(ParseOptionValueError::TooLong {
            option,
            maximum: max_len,
        });
    }
    if input.as_bytes().contains(&0) {
        return Err(ParseOptionValueError::NulByte(option));
    }
    Ok(())
}

fn enforce_item_limit(
    option: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), ParseOptionValueError> {
    if actual > maximum {
        Err(ParseOptionValueError::TooManyItems { option, maximum })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{OptionValue, ParseOptionValueError, parse_option_value};
    use crate::builtin_registry;
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    fn parse(name: &str, value: &str) -> Result<OptionValue, ParseOptionValueError> {
        let registry = builtin_registry();
        parse_option_value(registry.find(name).expect("registered option"), value, None)
    }

    #[test]
    fn parses_scalar_values_and_binary_size_suffixes() {
        assert_eq!(parse("continue", "true"), Ok(OptionValue::Bool(true)));
        assert_eq!(parse("split", "17"), Ok(OptionValue::Integer(17)));
        assert_eq!(
            parse("min-split-size", "2MB"),
            Ok(OptionValue::SizeBytes(2 * 1024 * 1024))
        );
        assert_eq!(
            parse("durability", "strict"),
            Ok(OptionValue::Enum("strict".to_owned()))
        );
    }

    #[test]
    fn rejects_invalid_scalars_ranges_and_overflow() {
        assert_eq!(
            parse("continue", "yes"),
            Err(ParseOptionValueError::InvalidBoolean("continue"))
        );
        assert!(matches!(
            parse("split", "0"),
            Err(ParseOptionValueError::OutOfRange { .. })
        ));
        assert_eq!(
            parse("max-download-limit", "18446744073709551615T"),
            Err(ParseOptionValueError::InvalidSize("max-download-limit"))
        );
        assert!(matches!(
            parse("durability", "usually"),
            Err(ParseOptionValueError::InvalidEnum { .. })
        ));
    }

    #[test]
    fn parses_and_bounds_status_code_sets() {
        assert_eq!(
            parse("retry-on-http-status", "408,500-502,502"),
            Ok(OptionValue::StatusCodeSet(BTreeSet::from([
                408, 500, 501, 502
            ])))
        );
        assert_eq!(
            parse("retry-on-http-status", "99"),
            Err(ParseOptionValueError::InvalidStatusCode(
                "retry-on-http-status"
            ))
        );
        assert!(matches!(
            parse("retry-on-http-status", "100-599"),
            Err(ParseOptionValueError::TooManyItems { .. })
        ));
    }

    #[test]
    fn expands_only_explicit_home_paths() {
        let registry = builtin_registry();
        let definition = registry.find("dir").expect("dir");
        assert_eq!(
            parse_option_value(
                definition,
                "${HOME}/downloads",
                Some(Path::new("/srv/user"))
            ),
            Ok(OptionValue::Path(PathBuf::from("/srv/user/downloads")))
        );
        assert_eq!(
            parse_option_value(
                definition,
                "${USER}/downloads",
                Some(Path::new("/srv/user"))
            ),
            Err(ParseOptionValueError::InvalidHomeExpansion("dir"))
        );
        assert_eq!(
            parse_option_value(definition, "${HOME}/downloads", None),
            Err(ParseOptionValueError::MissingHome("dir"))
        );
    }

    #[test]
    fn secret_formatting_never_exposes_the_value() {
        let value = parse("rpc-secret", "correct horse battery staple").expect("secret");
        let OptionValue::Secret(secret) = value else {
            panic!("expected secret")
        };
        assert_eq!(secret.expose_secret(), "correct horse battery staple");
        assert_eq!(secret.to_string(), "[REDACTED]");
        assert_eq!(format!("{secret:?}"), "SecretString([REDACTED])");
    }

    #[test]
    fn known_unsupported_options_are_not_parseable_as_values() {
        assert_eq!(
            parse("enable-http-pipelining", "false"),
            Err(ParseOptionValueError::Unsupported("enable-http-pipelining"))
        );
    }
}
