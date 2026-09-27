//! Bounded current-format session documents. Progress and caller GIDs are never recovery authority.

use crate::{HttpControlError, MAX_HTTP_TASK_SOURCES, rpc_budget::RpcRequestLease};
use ariax_storage::{SESSION_MAX_IMPORT_TASKS, SessionTaskSourceRecord};
use serde_json::{Map, Value, json};
use std::io::Write as _;

pub const MAX_SESSION_DOCUMENT_BYTES: usize = ariax_storage::SESSION_EXPORT_MAX_BYTES;
pub const MAX_SESSION_LINE_BYTES: usize = 64 * 1024;

/// Validates current-format session syntax under the process budget, without admitting tasks.
/// Task option and destination policy validation is performed again at import.
pub fn validate_session_syntax(
    text: &str,
    format: SessionFormat,
) -> Result<usize, HttpControlError> {
    if text.len() > MAX_SESSION_DOCUMENT_BYTES {
        return Err(invalid("session document exceeds its byte limit"));
    }
    let client = crate::RpcBudgets::process_default()
        .client()
        .map_err(|_| HttpControlError::Busy)?;
    let request = client
        .try_request(text.len())
        .map_err(|_| HttpControlError::Busy)?;
    parse_import(json!([text, format.as_str()]), &request).map(|tasks| tasks.len())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SessionFormat {
    #[default]
    Json,
    Aria2,
}

impl SessionFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Aria2 => "aria2",
        }
    }

    pub fn parse(value: &str) -> Result<Self, HttpControlError> {
        match value {
            "json" => Ok(Self::Json),
            "aria2" => Ok(Self::Aria2),
            _ => Err(invalid("session format must be json or aria2")),
        }
    }
}

#[derive(Default)]
pub(crate) struct ImportedTask {
    #[cfg(feature = "bt")]
    pub bittorrent: Option<ImportedBt>,
    pub sources: ImportedSources,
    pub options: Value,
    pub verification: Option<std::sync::Arc<crate::VerificationManifest>>,
    pub metalink_index: Option<u32>,
    pub priorities: Option<Vec<i64>>,
}

pub(crate) enum ImportedSources {
    Uris(Vec<String>),
    Persisted(Vec<SessionTaskSourceRecord>),
}

impl Default for ImportedSources {
    fn default() -> Self {
        Self::Uris(Vec::new())
    }
}

impl ImportedSources {
    pub(crate) fn uris(&self) -> impl Iterator<Item = &str> {
        let (uris, sources) = match self {
            Self::Uris(uris) => (uris.as_slice(), &[][..]),
            Self::Persisted(sources) => (&[][..], sources.as_slice()),
        };
        uris.iter().map(String::as_str).chain(
            sources
                .iter()
                .filter_map(|source| source.persistence_safe_uri.as_deref()),
        )
    }
}

#[cfg(feature = "bt")]
pub(crate) struct ImportedBt {
    pub binding: ariax_storage::SessionBtBinding,
    pub resume_data: Vec<u8>,
}

#[cfg(feature = "bt")]
fn parse_bittorrent(value: &Value) -> Result<ImportedBt, HttpControlError> {
    use base64ct::Encoding as _;
    let object = value
        .as_object()
        .ok_or_else(|| invalid("invalid BitTorrent session metadata"))?;
    reject_unknown(
        object,
        &["identity", "metainfo", "magnet", "files", "resumeData"],
    )?;
    let decode = |name: &str, limit: usize| -> Result<Vec<u8>, HttpControlError> {
        let encoded = object
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| value.len() <= limit.div_ceil(3).saturating_mul(4))
            .ok_or_else(|| invalid("invalid BitTorrent session blob"))?;
        let bytes = base64ct::Base64::decode_vec(encoded)
            .map_err(|_| invalid("invalid BitTorrent session base64"))?;
        if bytes.len() > limit {
            return Err(invalid("BitTorrent session blob exceeds its limit"));
        }
        Ok(bytes)
    };
    let metainfo = decode("metainfo", 16 * 1024 * 1024)?;
    let info = if metainfo.is_empty() {
        Vec::new()
    } else {
        ariax_bt::info_section(&metainfo, ariax_bt::MetadataLimits::default())
            .map_err(|_| invalid("invalid BitTorrent session metainfo"))?
            .to_vec()
    };
    let identity = serde_json::from_value(
        object
            .get("identity")
            .cloned()
            .ok_or_else(|| invalid("missing BitTorrent identity"))?,
    )
    .map_err(|_| invalid("invalid BitTorrent identity"))?;
    let files = object
        .get("files")
        .and_then(Value::as_array)
        .filter(|files| files.len() <= 10_000)
        .ok_or_else(|| invalid("invalid BitTorrent file mapping"))?;
    let files = files
        .iter()
        .map(|file| {
            serde_json::from_value(file.clone())
                .map_err(|_| invalid("invalid BitTorrent file mapping"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let magnet = match object.get("magnet") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if value.len() <= 65536 => Some(value.clone()),
        _ => return Err(invalid("invalid BitTorrent magnet")),
    };
    // The receiver supplies a new protected root. This marker is used only to
    // validate identity and portable paths, and never leaves preparation.
    let binding = ariax_storage::SessionBtBinding {
        identity,
        root_identity: vec![1],
        metainfo,
        info,
        magnet,
        files,
    };
    binding
        .validate()
        .map_err(|_| invalid("invalid BitTorrent session binding"))?;
    let resume_data = decode("resumeData", ariax_storage::SESSION_MAX_BT_RESUME_BYTES)?;
    if !resume_data.is_empty() {
        ariax_bt::validate_resume(&resume_data, &binding.identity)
            .map_err(|_| invalid("invalid BitTorrent resume data"))?;
    }
    Ok(ImportedBt {
        binding,
        resume_data,
    })
}

pub(crate) fn parse_import(
    params: Value,
    request: &RpcRequestLease,
) -> Result<Vec<ImportedTask>, HttpControlError> {
    let values = params
        .as_array()
        .filter(|values| (1..=2).contains(&values.len()))
        .ok_or_else(|| invalid("importSession requires a document and optional format"))?;
    let format = values
        .get(1)
        .map(|value| {
            SessionFormat::parse(
                value
                    .as_str()
                    .ok_or_else(|| invalid("session format must be text"))?,
            )
        })
        .transpose()?
        .unwrap_or_default();
    if format == SessionFormat::Aria2 {
        return parse_aria2(
            values[0]
                .as_str()
                .ok_or_else(|| invalid("aria2 session must be text"))?,
            request,
        );
    }
    let decoded;
    let document = if let Some(text) = values[0].as_str() {
        decoded = parse_json(text, request)?;
        &decoded
    } else {
        &values[0]
    };
    let object = document
        .as_object()
        .ok_or_else(|| invalid("session must be an object"))?;
    reject_unknown(object, &["formatVersion", "sessionId", "tasks"])?;
    if object.get("formatVersion").and_then(Value::as_u64) != Some(3) {
        return Err(invalid(
            "unsupported session format version; expected version 3",
        ));
    }
    if object
        .get("sessionId")
        .is_some_and(|value| !value.as_str().is_some_and(|text| is_hex(text, 32)))
    {
        return Err(invalid("invalid session identity hint"));
    }
    let tasks = object
        .get("tasks")
        .and_then(Value::as_array)
        .filter(|tasks| tasks.len() <= SESSION_MAX_IMPORT_TASKS)
        .ok_or_else(|| invalid("invalid session task array"))?;
    tasks.iter().map(|task| parse_task(task, true)).collect()
}

fn parse_json(text: &str, request: &RpcRequestLease) -> Result<Value, HttpControlError> {
    if text.len() > MAX_SESSION_DOCUMENT_BYTES {
        return Err(invalid("session document exceeds its byte limit"));
    }
    let value =
        crate::rpc_json::parse_strict(text.as_bytes(), request).map_err(|error| match error {
            crate::rpc_json::RpcJsonError::Budget(_) => HttpControlError::Busy,
            crate::rpc_json::RpcJsonError::Parse => invalid("invalid session JSON"),
        })?;
    request
        .reserve(crate::rpc_json::command_value_bytes(&value))
        .map_err(|_| HttpControlError::Busy)?;
    Ok(value)
}

fn parse_task(task: &Value, force_pause: bool) -> Result<ImportedTask, HttpControlError> {
    let object = task
        .as_object()
        .ok_or_else(|| invalid("session task must be an object"))?;
    reject_unknown(
        object,
        &[
            "kind",
            "gid",
            "sources",
            "options",
            "state",
            "verification",
            "bittorrent",
        ],
    )?;
    let is_bt = match object.get("kind").and_then(Value::as_str) {
        Some("transfer") => false,
        Some("bittorrent") => true,
        _ => return Err(invalid("session task requires an explicit supported kind")),
    };
    if is_bt {
        if ["sources", "verification"]
            .iter()
            .any(|key| object.contains_key(*key))
        {
            return Err(invalid(
                "BitTorrent session member contains transfer fields",
            ));
        }
        #[cfg(not(feature = "bt"))]
        return Err(HttpControlError::Unsupported(
            "BitTorrent feature unavailable",
        ));
    } else if object.contains_key("bittorrent") {
        return Err(invalid("session task kind differs from its metadata"));
    }
    #[cfg(feature = "bt")]
    let bittorrent = if is_bt {
        Some(parse_bittorrent(object.get("bittorrent").ok_or_else(
            || invalid("missing BitTorrent session metadata"),
        )?)?)
    } else {
        None
    };
    let verification = object
        .get("verification")
        .map(crate::verification_document::parse_verification)
        .transpose()?;
    if object
        .get("gid")
        .is_some_and(|value| !value.as_str().is_some_and(|text| is_hex(text, 16)))
    {
        return Err(invalid("invalid task identity hint"));
    }
    if object.get("state").is_some_and(|value| {
        !value.as_str().is_some_and(|text| {
            ariax_core::ALL_TASK_STATES
                .iter()
                .any(|state| state.code() == text)
        })
    }) {
        return Err(invalid("invalid task state hint"));
    }
    let sources = if is_bt {
        ImportedSources::default()
    } else {
        ImportedSources::Persisted(
            object
                .get("sources")
                .and_then(Value::as_array)
                .filter(|values| !values.is_empty() && values.len() <= MAX_HTTP_TASK_SOURCES)
                .ok_or_else(|| invalid("invalid session sources"))?
                .iter()
                .map(parse_source)
                .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let mut options = object.get("options").cloned().unwrap_or_else(|| json!({}));
    validate_options(&mut options, force_pause)?;
    if verification.is_some() {
        options
            .as_object_mut()
            .expect("validated options")
            .remove("piece-length");
    }
    let (verification, metalink_index) =
        verification.map_or((None, None), |(manifest, index)| (Some(manifest), index));
    Ok(ImportedTask {
        #[cfg(feature = "bt")]
        bittorrent,
        sources,
        options,
        verification,
        metalink_index,
        priorities: None,
    })
}

fn validate_options(options: &mut Value, force_pause: bool) -> Result<(), HttpControlError> {
    let option_map = options
        .as_object_mut()
        .ok_or_else(|| invalid("session options must be an object"))?;
    if option_map.get("pause").is_some_and(|value| {
        value.as_bool().is_none() && !matches!(value.as_str(), Some("true" | "false"))
    }) {
        return Err(invalid("invalid session pause option"));
    }
    if force_pause {
        option_map.insert("pause".to_owned(), Value::Bool(true));
    }
    if option_map.contains_key("metadata-expansion")
        || option_map.contains_key("verification-manifest")
        || option_map.contains_key("metalink-file-index")
    {
        return Err(invalid(
            "internal verification bindings cannot be imported as options",
        ));
    }
    Ok(())
}

fn parse_source(value: &Value) -> Result<SessionTaskSourceRecord, HttpControlError> {
    let source = value
        .as_object()
        .ok_or_else(|| invalid("session source must be an object"))?;
    reject_unknown(
        source,
        &[
            "uriId",
            "uri",
            "fingerprint",
            "needsCredentials",
            "priority",
        ],
    )?;
    let uri_id = source
        .get("uriId")
        .and_then(|value| value.as_str().and_then(|text| text.parse::<u32>().ok()))
        .ok_or_else(|| invalid("invalid source identity"))?;
    let priority = source
        .get("priority")
        .and_then(|value| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        .ok_or_else(|| invalid("invalid source priority"))?;
    let needs_credentials = source
        .get("needsCredentials")
        .and_then(Value::as_bool)
        .ok_or_else(|| invalid("invalid source credential marker"))?;
    let uri = match source.get("uri") {
        Some(Value::Null) if needs_credentials => None,
        Some(Value::String(uri))
            if uri.len() <= ariax_storage::SESSION_MAX_SAFE_URI_BYTES
                && ariax_storage::uri_is_safe_to_persist(uri) =>
        {
            Some(uri.clone())
        }
        _ => return Err(invalid("session source URI is not persistence-safe")),
    };
    let fingerprint = source
        .get("fingerprint")
        .and_then(Value::as_str)
        .filter(|text| is_hex(text, 64))
        .ok_or_else(|| invalid("invalid source fingerprint"))?;
    let mut bytes = [0_u8; 32];
    for (byte, pair) in bytes.iter_mut().zip(fingerprint.as_bytes().chunks_exact(2)) {
        *byte = hex(pair[0]) * 16 + hex(pair[1]);
    }
    Ok(SessionTaskSourceRecord {
        uri_id,
        persistence_safe_uri: uri,
        redacted_fingerprint: bytes,
        needs_credentials,
        priority,
    })
}

fn is_hex(text: &str, length: usize) -> bool {
    text.len() == length
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex(byte: u8) -> u8 {
    if byte.is_ascii_digit() {
        byte - b'0'
    } else {
        byte - b'a' + 10
    }
}

fn reject_unknown(object: &Map<String, Value>, names: &[&str]) -> Result<(), HttpControlError> {
    if object.keys().any(|name| !names.contains(&name.as_str())) {
        return Err(invalid("unknown session field"));
    }
    Ok(())
}

fn invalid(message: &'static str) -> HttpControlError {
    HttpControlError::InvalidParams(message)
}

fn parse_aria2(
    text: &str,
    request: &RpcRequestLease,
) -> Result<Vec<ImportedTask>, HttpControlError> {
    if text.len() > MAX_SESSION_DOCUMENT_BYTES {
        return Err(invalid("session document exceeds its byte limit"));
    }
    let mut tasks = Vec::new();
    let mut current = None;
    let mut marker = None;
    for line in text.lines() {
        if line.len() > MAX_SESSION_LINE_BYTES {
            return Err(invalid("session line exceeds its byte limit"));
        }
        if let Some(json) = line.strip_prefix("# ariax-task ") {
            finish_aria2_task(&mut current, &mut marker, &mut tasks)?;
            let value = parse_json(json, request)?;
            require_aria2_metadata(&value)?;
            let task = parse_task(&value, false)?;
            if task.sources.uris().next().is_none() {
                push_task(&mut tasks, task)?;
            } else {
                marker = Some(task);
            }
        } else if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        } else if line.starts_with([' ', '\t']) {
            let (name, value) = line
                .trim_start()
                .split_once('=')
                .ok_or_else(|| invalid("session option requires '='"))?;
            let options = current
                .as_mut()
                .map(|task: &mut ImportedTask| &mut task.options)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| invalid("session option has no URI line"))?;
            let name = name.trim();
            if name.is_empty()
                || name.len() > 1024
                || options.len() >= 1024
                || options.contains_key(name)
            {
                return Err(invalid("invalid or duplicate session option"));
            }
            request
                .reserve(2048_usize.saturating_add(line.len().saturating_mul(12)))
                .map_err(|_| HttpControlError::Busy)?;
            options.insert(name.to_owned(), Value::String(value.trim().to_owned()));
        } else {
            if current.is_some() {
                finish_aria2_task(&mut current, &mut marker, &mut tasks)?;
            }
            request
                .reserve(4096_usize.saturating_add(line.len().saturating_mul(12)))
                .map_err(|_| HttpControlError::Busy)?;
            let uris = line
                .split_ascii_whitespace()
                .take(MAX_HTTP_TASK_SOURCES + 1)
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if uris.is_empty() || uris.len() > MAX_HTTP_TASK_SOURCES {
                return Err(invalid("invalid session URI line"));
            }
            current = Some(ImportedTask {
                sources: ImportedSources::Uris(uris),
                options: json!({}),
                ..Default::default()
            });
        }
    }
    finish_aria2_task(&mut current, &mut marker, &mut tasks)?;
    Ok(tasks)
}

fn finish_aria2_task(
    current: &mut Option<ImportedTask>,
    marker: &mut Option<ImportedTask>,
    tasks: &mut Vec<ImportedTask>,
) -> Result<(), HttpControlError> {
    if let Some(mut task) = current.take() {
        validate_options(&mut task.options, false)?;
        let task = if let Some(marker) = marker.take() {
            let projected_options_match = task
                .options
                .as_object()
                .zip(marker.options.as_object())
                .is_some_and(|(actual, expected)| {
                    actual
                        .iter()
                        .eq(expected.iter().filter(|(name, _)| aria2_option(name)))
                });
            if !task.sources.uris().eq(marker.sources.uris()) || !projected_options_match {
                return Err(invalid("aria2 projection differs from its source metadata"));
            }
            marker
        } else {
            task
        };
        push_task(tasks, task)?;
    } else if marker.is_some() {
        return Err(invalid("session metadata has no URI projection"));
    }
    Ok(())
}

fn aria2_option(name: &str) -> bool {
    name == "pause"
        || ariax_config::builtin_registry()
            .find(name)
            .is_some_and(|definition| definition.aria2_available)
}

fn push_task(tasks: &mut Vec<ImportedTask>, task: ImportedTask) -> Result<(), HttpControlError> {
    if tasks.len() >= SESSION_MAX_IMPORT_TASKS {
        return Err(invalid("too many session tasks"));
    }
    tasks.push(task);
    Ok(())
}

fn require_aria2_metadata(task: &Value) -> Result<(), HttpControlError> {
    if task
        .get("verification")
        .is_some_and(|value| !value.is_null())
    {
        return Err(HttpControlError::Unsupported(
            "VerificationMetadataRequiresJson",
        ));
    }
    if task.get("kind").and_then(Value::as_str) == Some("bittorrent") {
        return Err(HttpControlError::Unsupported(
            "BitTorrentMetadataRequiresJson",
        ));
    }
    Ok(())
}

pub(crate) fn render(document: &Value, format: SessionFormat) -> Result<Vec<u8>, HttpControlError> {
    let mut writer = SessionWriter { bytes: Vec::new() };
    if format == SessionFormat::Json {
        serde_json::to_writer(&mut writer, document)
            .map_err(|_| HttpControlError::ResponseTooLarge)?;
        return Ok(writer.bytes);
    }
    let tasks = document
        .get("tasks")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("invalid export document"))?;
    for task in tasks {
        require_aria2_metadata(task)?;
        let sources = task
            .get("sources")
            .and_then(Value::as_array)
            .filter(|sources| !sources.is_empty() && sources.len() <= MAX_HTTP_TASK_SOURCES)
            .ok_or_else(|| invalid("invalid export sources"))?;
        let options = task
            .get("options")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("invalid export options"))?;
        let line_start = writer.bytes.len();
        writer
            .write_all(b"# ariax-task ")
            .map_err(|_| HttpControlError::ResponseTooLarge)?;
        serde_json::to_writer(&mut writer, task).map_err(|_| HttpControlError::ResponseTooLarge)?;
        if writer.bytes.len() - line_start > MAX_SESSION_LINE_BYTES {
            return Err(HttpControlError::ResponseTooLarge);
        }
        writer
            .write_all(b"\n")
            .map_err(|_| HttpControlError::ResponseTooLarge)?;
        let line_start = writer.bytes.len();
        let mut has_uris = false;
        for source in sources {
            if source.get("uri") == Some(&Value::Null)
                && source.get("needsCredentials") == Some(&Value::Bool(true))
            {
                continue;
            }
            let uri = source
                .get("uri")
                .and_then(Value::as_str)
                .filter(|uri| {
                    ariax_storage::uri_is_safe_to_persist(uri)
                        && !uri.bytes().any(|byte| byte.is_ascii_whitespace())
                })
                .ok_or_else(|| invalid("URI cannot be exported as aria2 text"))?;
            if has_uris {
                writer
                    .write_all(b"\t")
                    .map_err(|_| HttpControlError::ResponseTooLarge)?;
            }
            writer
                .write_all(uri.as_bytes())
                .map_err(|_| HttpControlError::ResponseTooLarge)?;
            has_uris = true;
        }
        if !has_uris {
            continue;
        }
        if writer.bytes.len() - line_start > MAX_SESSION_LINE_BYTES {
            return Err(HttpControlError::ResponseTooLarge);
        }
        writer
            .write_all(b"\n")
            .map_err(|_| HttpControlError::ResponseTooLarge)?;
        for (name, value) in options {
            if !aria2_option(name) {
                continue;
            }
            let value = value
                .as_str()
                .filter(|value| !value.bytes().any(|byte| byte.is_ascii_control()))
                .ok_or_else(|| invalid("option cannot be exported as aria2 text"))?;
            if name.len().saturating_add(value.len()).saturating_add(3) > MAX_SESSION_LINE_BYTES {
                return Err(HttpControlError::ResponseTooLarge);
            }
            writeln!(writer, "  {name}={value}").map_err(|_| HttpControlError::ResponseTooLarge)?;
        }
    }
    Ok(writer.bytes)
}

struct SessionWriter {
    bytes: Vec<u8>,
}

impl std::io::Write for SessionWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > MAX_SESSION_DOCUMENT_BYTES {
            return Err(std::io::Error::other(
                "session export exceeds its byte limit",
            ));
        }
        let required = self.bytes.len().saturating_add(bytes.len());
        if required > self.bytes.capacity() {
            let capacity = required
                .max(self.bytes.capacity().saturating_mul(2))
                .min(MAX_SESSION_DOCUMENT_BYTES);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| std::io::Error::other("session export allocation failed"))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(uri: &str) -> Value {
        json!({
            "uriId": "11", "uri": uri, "fingerprint": "02".repeat(32),
            "needsCredentials": false, "priority": "1"
        })
    }

    fn parse(params: Value) -> Result<Vec<ImportedTask>, HttpControlError> {
        let budgets = crate::RpcBudgets::process_default();
        let client = budgets.client().expect("client");
        let request = client.try_request(128).expect("request");
        let result = parse_import(params, &request);
        assert!(client.request_bytes() <= crate::MAX_RPC_CLIENT_REQUEST_BYTES);
        drop(request);
        assert_eq!(client.request_bytes(), 0);
        result
    }

    #[test]
    fn session_documents_reject_unknown_duplicate_and_malformed_fields() {
        for text in [
            r#"{"formatVersion":3,"tasks":[],"tasks":[]}"#,
            r#"{"formatVersion":3,"tasks":[],"extra":true}"#,
            r#"{"tasks":[],"formatVersion":1}"#,
            r#"{"tasks":[],"formatVersion":2}"#,
            r#"{"tasks":[]}"#,
            r#"{"tasks":[],"formatVersion":4}"#,
            r#"{"tasks":[]} trailing"#,
        ] {
            assert!(matches!(
                parse(json!([text])),
                Err(HttpControlError::InvalidParams(_))
            ));
        }
        assert!(
            parse(json!([{"formatVersion":3,"tasks": []}]))
                .expect("empty document")
                .is_empty()
        );
        for text in [
            "  split=2\n",
            "http://example.test/file\n  split=2\n  split=3\n",
            "# ariax-task {\"uris\":[\"http://example.test/file\"]}\n",
        ] {
            assert!(parse(json!([text, "aria2"])).is_err());
        }
        let huge_line = format!("http://example.test/{}", "a".repeat(MAX_SESSION_LINE_BYTES));
        assert!(parse(json!([huge_line, "aria2"])).is_err());
        let too_many = "http://example.test/file\n".repeat(SESSION_MAX_IMPORT_TASKS + 1);
        assert!(parse(json!([too_many, "aria2"])).is_err());
    }

    #[test]
    fn json_requires_one_source_representation_and_an_explicit_kind() {
        let task = json!({"kind":"transfer","sources":[source("http://example.test/file")]});
        let tasks =
            parse(json!([{"formatVersion":3,"tasks":[task.clone()]}])).expect("source records");
        let ImportedSources::Persisted(records) = &tasks[0].sources else {
            panic!("JSON sources must retain their persisted identities");
        };
        assert_eq!(records[0].uri_id, 11);
        assert_eq!(records[0].priority, 1);
        assert_eq!(records[0].redacted_fingerprint, [2; 32]);
        assert_eq!(tasks[0].options["pause"], true);

        for name in ["kind", "sources"] {
            let mut missing = task.clone();
            missing.as_object_mut().expect("task").remove(name);
            assert!(matches!(
                parse(json!([{"formatVersion":3,"tasks":[missing]}])),
                Err(HttpControlError::InvalidParams(_))
            ));
        }
        for keep_sources in [false, true] {
            let mut legacy = task.clone();
            legacy["uris"] = json!(["http://example.test/file"]);
            if !keep_sources {
                legacy.as_object_mut().expect("task").remove("sources");
            }
            assert!(matches!(
                parse(json!([{"formatVersion":3,"tasks":[legacy]}])),
                Err(HttpControlError::InvalidParams("unknown session field"))
            ));
        }
        for (name, value, error) in [
            ("sources", json!([]), "invalid session sources"),
            ("sources", Value::Null, "invalid session sources"),
            ("options", json!(false), "session options must be an object"),
            (
                "options",
                json!({"pause":"invalid"}),
                "invalid session pause option",
            ),
            ("state", json!("unknown"), "invalid task state hint"),
        ] {
            let mut invalid = task.clone();
            invalid[name] = value;
            assert!(matches!(
                parse(json!([{"formatVersion":3,"tasks":[invalid]}])),
                Err(HttpControlError::InvalidParams(actual)) if actual == error
            ));
        }
    }

    #[test]
    fn aria2_source_comments_preserve_placeholders_and_reject_projection_changes() {
        let unavailable = json!({"uriId":"9", "uri":null, "fingerprint":"01".repeat(32), "needsCredentials":true, "priority":"0"});
        let document = json!({"formatVersion":3,"tasks":[
            {"kind":"transfer","sources":[unavailable.clone()],"options":{"out":"blocked.bin","pause":"true"}},
            {"kind":"transfer","sources":[unavailable,source("http://example.test/file")],"options":{"out":"file.bin","pause":"false"}}
        ]});
        let text =
            String::from_utf8(render(&document, SessionFormat::Aria2).expect("aria2 export"))
                .expect("UTF-8");
        assert!(!text.contains("\"uris\""));
        let tasks = parse(json!([text, "aria2"])).expect("aria2 import");
        assert_eq!(tasks.len(), 2);
        assert!(tasks[0].sources.uris().next().is_none());
        let ImportedSources::Persisted(blocked) = &tasks[0].sources else {
            panic!("placeholder source records");
        };
        assert_eq!(blocked[0].uri_id, 9);
        assert!(blocked[0].needs_credentials);
        let ImportedSources::Persisted(mixed) = &tasks[1].sources else {
            panic!("mixed source records");
        };
        assert_eq!(mixed.len(), 2);
        assert_eq!(tasks[1].options["pause"], "false");
        let changed = text.replace("  pause=false", "  pause=true");
        assert!(parse(json!([changed, "aria2"])).is_err());
        let changed = text.replace("\nhttp://example.test/file\n", "\nhttp://other.test/file\n");
        assert!(matches!(
            parse(json!([changed, "aria2"])),
            Err(HttpControlError::InvalidParams(
                "aria2 projection differs from its source metadata"
            ))
        ));
        let json_bytes = render(&document, SessionFormat::Json).expect("JSON export");
        assert_eq!(
            serde_json::from_slice::<Value>(&json_bytes).expect("JSON"),
            document
        );
        assert!(
            parse(json!([String::from_utf8(json_bytes).expect("UTF-8")]))
                .expect("JSON import")
                .iter()
                .all(|task| task.options["pause"] == true)
        );
    }

    #[test]
    fn aria2_option_lines_omit_extensions_while_metadata_round_trips_them() {
        let document = json!({"formatVersion":3,"tasks":[{"kind":"transfer","sources":[source("http://example.test/file")],"options":{"pause":"true", "split":"3", "piece-length":"1048576", "retry-profile":"standard"}}]});
        let text = String::from_utf8(render(&document, SessionFormat::Aria2).expect("export"))
            .expect("text");
        assert!(text.contains("  split=3\n"));
        assert!(text.contains("  piece-length=1048576\n"));
        assert!(!text.contains("  retry-profile="));
        let tasks = parse(json!([text.clone(), "aria2"])).expect("reimport");
        assert_eq!(tasks[0].options["piece-length"], "1048576");
        assert_eq!(tasks[0].options["retry-profile"], "standard");
        assert!(
            parse(json!([
                format!("{text}  retry-profile=standard\n"),
                "aria2"
            ]))
            .is_err()
        );
    }

    #[test]
    fn plain_aria2_input_retains_uri_lists_and_validates_options() {
        let text = "http://first.test/file\thttp://second.test/file\n  pause=false\n  split=2\nhttp://third.test/file\n  pause=true\n";
        let tasks = parse(json!([text, "aria2"])).expect("ordinary aria2 input");
        assert_eq!(tasks.len(), 2);
        assert!(matches!(&tasks[0].sources, ImportedSources::Uris(uris) if uris.len() == 2));
        assert_eq!(
            tasks[0].sources.uris().collect::<Vec<_>>(),
            ["http://first.test/file", "http://second.test/file"]
        );
        assert_eq!(tasks[0].options, json!({"pause":"false", "split":"2"}));
        assert_eq!(tasks[1].options["pause"], "true");
        assert!(matches!(
            parse(json!([
                text.replace("pause=false", "pause=invalid"),
                "aria2"
            ])),
            Err(HttpControlError::InvalidParams(
                "invalid session pause option"
            ))
        ));
        for name in [
            "verification-manifest",
            "metadata-expansion",
            "metalink-file-index",
        ] {
            assert!(matches!(
                parse(json!([
                    format!("http://example.test/file\n  {name}=1\n"),
                    "aria2"
                ])),
                Err(HttpControlError::InvalidParams(
                    "internal verification bindings cannot be imported as options"
                ))
            ));
        }
    }

    #[test]
    fn aria2_rejects_metadata_that_requires_json() {
        for (task, error) in [
            (
                json!({"kind":"bittorrent"}),
                "BitTorrentMetadataRequiresJson",
            ),
            (
                json!({"kind":"transfer", "verification":{}}),
                "VerificationMetadataRequiresJson",
            ),
        ] {
            assert!(matches!(
                render(&json!({"formatVersion":3,"tasks":[task.clone()]}), SessionFormat::Aria2),
                Err(HttpControlError::Unsupported(actual)) if actual == error
            ));
            assert!(matches!(
                parse(json!([format!("# ariax-task {task}\n"), "aria2"])),
                Err(HttpControlError::Unsupported(actual)) if actual == error
            ));
        }
    }
}
