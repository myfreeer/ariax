//! Closed TOML rule schema with bounded, non-backtracking URL matching.

use crate::{
    CompatStatus, OptionValue, RuntimeUpdate, Scope, SecurityClass, builtin_registry,
    parse_option_value,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

pub const MAX_URL_RULE_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_URL_RULES: usize = 4096;
pub const MAX_URL_GLOB_BYTES: usize = 4096;
pub const MAX_URL_RULE_METADATA_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_URL_RULE_MATCH_WORK: usize = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UrlRuleError {
    DocumentLimit,
    InvalidDocument,
    RuleLimit,
    InvalidMatch,
    InvalidOption,
    MetadataLimit,
    MatchWorkLimit,
    InvalidUrl,
}

impl fmt::Display for UrlRuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "URL rules rejected: {self:?}")
    }
}
impl std::error::Error for UrlRuleError {}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UrlRules {
    #[serde(rename = "rule")]
    rules: Vec<UrlRule>,
    #[serde(skip)]
    max_options_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct UrlRule {
    name: String,
    stop: bool,
    #[serde(rename = "match")]
    matcher: UrlMatch,
    options: BTreeMap<String, String>,
}

impl UrlRule {
    pub fn options(&self) -> &BTreeMap<String, String> {
        &self.options
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct UrlMatch {
    scheme: Vec<String>,
    protocol: Vec<String>,
    host: Option<String>,
    host_suffix: Option<String>,
    port: Option<u16>,
    path_glob: Option<String>,
    url_glob: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    #[serde(default)]
    rule: Vec<RawRule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    #[serde(default)]
    name: String,
    #[serde(default)]
    stop: bool,
    #[serde(rename = "match")]
    matcher: UrlMatch,
    options: BTreeMap<String, Scalar>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Scalar {
    String(String),
    Integer(i64),
    Bool(bool),
}

impl UrlRules {
    /// Conservative temporary TOML allocation bound, checked before parsing.
    pub fn parse_memory_bound(text: &str) -> Result<usize, UrlRuleError> {
        if text.len() > MAX_URL_RULE_DOCUMENT_BYTES {
            return Err(UrlRuleError::DocumentLimit);
        }
        let nodes = text
            .bytes()
            .filter(|byte| matches!(byte, b'=' | b'[' | b']' | b'{' | b'}' | b','))
            .count();
        let bytes = text
            .len()
            .saturating_mul(4)
            .saturating_add(nodes.saturating_mul(512))
            .saturating_add(4096);
        if bytes > MAX_URL_RULE_METADATA_BYTES {
            return Err(UrlRuleError::MetadataLimit);
        }
        Ok(bytes)
    }

    pub fn parse(text: &str) -> Result<Self, UrlRuleError> {
        Self::parse_memory_bound(text)?;
        let document: Document = toml::from_str(text).map_err(|_| UrlRuleError::InvalidDocument)?;
        if document.rule.len() > MAX_URL_RULES {
            return Err(UrlRuleError::RuleLimit);
        }
        let mut rules = Vec::with_capacity(document.rule.len());
        for raw in document.rule {
            if raw.name.len() > 1024 || raw.name.chars().any(char::is_control) {
                return Err(UrlRuleError::InvalidMatch);
            }
            let mut matcher = raw.matcher;
            matcher.validate()?;
            if raw.options.is_empty() || raw.options.len() > 1024 {
                return Err(UrlRuleError::InvalidOption);
            }
            let mut options = BTreeMap::new();
            for (name, value) in raw.options {
                let definition = builtin_registry()
                    .find(&name)
                    .ok_or(UrlRuleError::InvalidOption)?;
                if !definition.scopes.contains(Scope::PerDownload)
                    || !definition.scopes.contains(Scope::UrlRule)
                    || definition.security != SecurityClass::Normal
                    || matches!(
                        definition.compat,
                        CompatStatus::Unsupported
                            | CompatStatus::UnsafeCompat
                            | CompatStatus::FeatureGated
                    )
                    || matches!(
                        definition.runtime_update,
                        RuntimeUpdate::None
                            | RuntimeUpdate::StartupOnly
                            | RuntimeUpdate::UnsafeCompatOnly
                            | RuntimeUpdate::BtLive
                            | RuntimeUpdate::BtRestartRequired
                    )
                {
                    return Err(UrlRuleError::InvalidOption);
                }
                let text = match value {
                    Scalar::String(value) => value,
                    Scalar::Integer(value) => value.to_string(),
                    Scalar::Bool(value) => value.to_string(),
                };
                let value = parse_option_value(definition, &text, None)
                    .map_err(|_| UrlRuleError::InvalidOption)?;
                let canonical = match value {
                    OptionValue::Bool(value) => value.to_string(),
                    OptionValue::Integer(value) => value.to_string(),
                    OptionValue::SizeBytes(value) | OptionValue::DurationSeconds(value) => {
                        value.to_string()
                    }
                    OptionValue::String(value) | OptionValue::Enum(value) => value,
                    OptionValue::Path(value) => value.to_string_lossy().into_owned(),
                    OptionValue::HeaderList(_) | OptionValue::Secret(_) => {
                        return Err(UrlRuleError::InvalidOption);
                    }
                    OptionValue::StatusCodeSet(value) => value
                        .iter()
                        .map(u16::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                };
                options.insert(name, canonical);
            }
            rules.push(UrlRule {
                name: raw.name,
                stop: raw.stop,
                matcher,
                options,
            });
        }
        let mut sizes = BTreeMap::<&str, usize>::new();
        for rule in &rules {
            for (key, value) in &rule.options {
                let size = key.len() + value.len() + 512;
                sizes
                    .entry(key)
                    .and_modify(|old| *old = (*old).max(size))
                    .or_insert(size);
            }
        }
        let max_options_bytes = sizes.values().sum();
        let result = Self {
            rules,
            max_options_bytes,
        };
        if result.owned_bytes() > MAX_URL_RULE_METADATA_BYTES {
            return Err(UrlRuleError::MetadataLimit);
        }
        Ok(result)
    }

    pub fn rules(&self) -> &[UrlRule] {
        &self.rules
    }

    pub fn to_toml(&self) -> Result<String, UrlRuleError> {
        toml::to_string(self).map_err(|_| UrlRuleError::InvalidDocument)
    }

    pub fn owned_bytes(&self) -> usize {
        self.rules
            .iter()
            .map(|rule| {
                rule.name.capacity()
                    + 2048
                    + rule
                        .options
                        .iter()
                        .map(|(key, value)| key.capacity() + value.capacity() + 512)
                        .sum::<usize>()
                    + rule.matcher.owned_bytes()
            })
            .sum()
    }

    /// Maximum merged per-task map, independent of the number of matching rules.
    pub fn max_options_bytes(&self) -> usize {
        self.max_options_bytes
    }

    pub fn apply(&self, uri: &str) -> Result<BTreeMap<String, String>, UrlRuleError> {
        let mut result = BTreeMap::new();
        for rule in self.matching_rules(uri)? {
            result.extend(
                rule.options
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone())),
            );
        }
        Ok(result)
    }

    /// Returns borrowed layers in file order, honoring the first matching stop.
    pub fn matching_rules(&self, uri: &str) -> Result<Vec<&UrlRule>, UrlRuleError> {
        if self.rules.is_empty() {
            return Ok(Vec::new());
        }
        if uri.len() > 64 * 1024 {
            return Err(UrlRuleError::InvalidUrl);
        }
        let mut url = url::Url::parse(uri).map_err(|_| UrlRuleError::InvalidUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(UrlRuleError::InvalidUrl);
        }
        url.set_username("").map_err(|_| UrlRuleError::InvalidUrl)?;
        url.set_password(None)
            .map_err(|_| UrlRuleError::InvalidUrl)?;
        url.set_query(None);
        url.set_fragment(None);
        let mut work = MAX_URL_RULE_MATCH_WORK;
        let mut result = Vec::new();
        for rule in &self.rules {
            if rule.matcher.matches(&url, &mut work)? {
                result.push(rule);
                if rule.stop {
                    break;
                }
            }
        }
        Ok(result)
    }
}

impl UrlMatch {
    fn validate(&mut self) -> Result<(), UrlRuleError> {
        if self.scheme.is_empty()
            && self.protocol.is_empty()
            && self.host.is_none()
            && self.host_suffix.is_none()
            && self.port.is_none()
            && self.path_glob.is_none()
            && self.url_glob.is_none()
        {
            return Err(UrlRuleError::InvalidMatch);
        }
        for schemes in [&mut self.scheme, &mut self.protocol] {
            if schemes.len() > 2
                || schemes
                    .iter()
                    .any(|scheme| !matches!(scheme.as_str(), "http" | "https"))
            {
                return Err(UrlRuleError::InvalidMatch);
            }
            schemes.sort();
            schemes.dedup();
        }
        for value in [&mut self.host, &mut self.host_suffix]
            .into_iter()
            .flatten()
        {
            let suffix = value.starts_with('.');
            let name = value.trim_start_matches('.');
            if name.is_empty()
                || name.len() > 253
                || name.contains(['/', ':', '@', '*', '?', '#'])
                || name.chars().any(char::is_whitespace)
            {
                return Err(UrlRuleError::InvalidMatch);
            }
            let host = url::Host::parse(name)
                .map_err(|_| UrlRuleError::InvalidMatch)?
                .to_string();
            *value = if suffix { format!(".{host}") } else { host };
        }
        if self.port == Some(0) {
            return Err(UrlRuleError::InvalidMatch);
        }
        for glob in [&self.path_glob, &self.url_glob].into_iter().flatten() {
            if glob.is_empty()
                || glob.len() > MAX_URL_GLOB_BYTES
                || glob.bytes().any(|byte| {
                    byte.is_ascii_control() || matches!(byte, b'[' | b']' | b'{' | b'}' | b'\\')
                })
            {
                return Err(UrlRuleError::InvalidMatch);
            }
        }
        if self
            .url_glob
            .as_ref()
            .is_some_and(|glob| glob.contains(['@', '#']))
        {
            return Err(UrlRuleError::InvalidMatch);
        }
        Ok(())
    }

    fn owned_bytes(&self) -> usize {
        self.scheme
            .iter()
            .chain(&self.protocol)
            .map(|value| value.capacity() + 24)
            .sum::<usize>()
            + [
                &self.host,
                &self.host_suffix,
                &self.path_glob,
                &self.url_glob,
            ]
            .into_iter()
            .flatten()
            .map(String::capacity)
            .sum::<usize>()
    }

    fn matches(&self, url: &url::Url, work: &mut usize) -> Result<bool, UrlRuleError> {
        *work = work.checked_sub(1).ok_or(UrlRuleError::MatchWorkLimit)?;
        if (!self.scheme.is_empty() && !self.scheme.iter().any(|scheme| scheme == url.scheme()))
            || (!self.protocol.is_empty()
                && !self.protocol.iter().any(|scheme| scheme == url.scheme()))
            || self
                .port
                .is_some_and(|port| url.port_or_known_default() != Some(port))
        {
            return Ok(false);
        }
        let host = url.host_str().ok_or(UrlRuleError::InvalidUrl)?;
        if self.host.as_ref().is_some_and(|expected| host != expected) {
            return Ok(false);
        }
        if let Some(suffix) = &self.host_suffix {
            let domain = suffix.trim_start_matches('.');
            if host != domain
                && !host
                    .strip_suffix(domain)
                    .is_some_and(|prefix| prefix.ends_with('.'))
            {
                return Ok(false);
            }
        }
        if let Some(pattern) = &self.path_glob
            && !glob_matches(pattern, url.path(), work)?
        {
            return Ok(false);
        }
        if let Some(pattern) = &self.url_glob
            && !glob_matches(pattern, url.as_str(), work)?
        {
            return Ok(false);
        }
        Ok(true)
    }
}

fn glob_matches(pattern: &str, text: &str, work: &mut usize) -> Result<bool, UrlRuleError> {
    let cost = (pattern.len() + 1).saturating_mul(text.len() + 1);
    *work = work.checked_sub(cost).ok_or(UrlRuleError::MatchWorkLimit)?;
    let pattern = pattern.as_bytes();
    let mut current = vec![false; pattern.len() + 1];
    let mut next = vec![false; pattern.len() + 1];
    current[0] = true;
    for index in 0..pattern.len() {
        if pattern[index] == b'*' && current[index] {
            current[index + 1] = true;
        }
    }
    for byte in text.bytes() {
        next.fill(false);
        for (index, token) in pattern.iter().enumerate() {
            if *token == b'*' {
                next[index] |= current[index];
                next[index + 1] |= next[index];
            } else if *token == b'?' || *token == byte {
                next[index + 1] |= current[index];
            }
        }
        for index in 0..pattern.len() {
            if pattern[index] == b'*' && next[index] {
                next[index + 1] = true;
            }
        }
        std::mem::swap(&mut current, &mut next);
    }
    Ok(current[pattern.len()])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_rules_match_in_order_stop_and_never_use_uri_credentials() {
        let rules = UrlRules::parse(
            r#"
[[rule]]
name = "domain"
match.host_suffix = ".example.test"
options.split = 3
[[rule]]
stop = true
match.scheme = ["https"]
match.path_glob = "/release/*.iso"
match.port = 443
options.split = 7
[[rule]]
match.host = "example.test"
options.split = 11
"#,
        )
        .expect("rules");
        assert_eq!(
            rules
                .apply("https://user:secret@example.test/release/file.iso?secret=token#hidden")
                .expect("match")["split"],
            "7"
        );
        assert_eq!(
            rules.apply("http://sub.example.test/file").expect("suffix")["split"],
            "3"
        );
        assert!(
            rules
                .apply("https://badexample.test/file")
                .expect("boundary")
                .is_empty()
        );
        assert!(
            rules
                .apply("https://example.test.attacker.test/file")
                .expect("boundary")
                .is_empty()
        );
    }

    #[test]
    fn rules_reject_unknown_unsafe_oversized_and_expensive_inputs() {
        for text in [
            "[[rule]]\nmatch = {}\noptions.split=2",
            "[[rule]]\nmatch.unknown='x'\noptions.split=2",
            "[[rule]]\nmatch.host='a'\noptions.rpc-secret='secret'",
            "[[rule]]\nmatch.host='a'\noptions.on-download-complete='exec'",
            "[[rule]]\nmatch.host='a'\noptions.event-backend='auto'",
            "[[rule]]\nmatch.host='a'\noptions.split=0",
            "[[rule]]\nmatch.host='a'\noptions.split=1\noptions.split=2",
            "unknown = []",
            "[[rule]]\nmatch.host='a'\noptions.split=[[1]]",
        ] {
            assert!(UrlRules::parse(text).is_err(), "{text}");
        }
        assert_eq!(
            UrlRules::parse_memory_bound(&" ".repeat(MAX_URL_RULE_DOCUMENT_BYTES + 1)),
            Err(UrlRuleError::DocumentLimit)
        );
        let mut work = 2;
        assert_eq!(
            glob_matches("*abc", "aaaaabc", &mut work),
            Err(UrlRuleError::MatchWorkLimit)
        );
    }

    #[test]
    fn bounded_glob_automaton_matches_exhaustive_small_reference_language() {
        fn reference(pattern: &[u8], text: &[u8]) -> bool {
            match pattern.first() {
                None => text.is_empty(),
                Some(b'*') => {
                    reference(&pattern[1..], text)
                        || (!text.is_empty() && reference(pattern, &text[1..]))
                }
                Some(token) => {
                    !text.is_empty()
                        && (*token == b'?' || *token == text[0])
                        && reference(&pattern[1..], &text[1..])
                }
            }
        }
        for length in 0..=4 {
            for pattern_id in 0..4_usize.pow(length) {
                let mut id = pattern_id;
                let pattern: String = (0..length)
                    .map(|_| {
                        let token = b"ab?*"[id % 4];
                        id /= 4;
                        token as char
                    })
                    .collect();
                for text_id in 0..32_u32 {
                    let text: String = (0..text_id.count_ones())
                        .map(|i| if text_id & (1 << i) == 0 { 'a' } else { 'b' })
                        .collect();
                    let mut work = MAX_URL_RULE_MATCH_WORK;
                    assert_eq!(
                        glob_matches(&pattern, &text, &mut work).expect("bounded"),
                        reference(pattern.as_bytes(), text.as_bytes()),
                        "{pattern:?} {text:?}"
                    );
                }
            }
        }
    }
}
