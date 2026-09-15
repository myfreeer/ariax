use super::*;
use base64ct::Encoding;

pub(super) fn parse_upload(
    params: Value,
    request: &crate::rpc_budget::RpcRequestLease,
) -> Result<Vec<crate::session_file::ImportedTask>, HttpControlError> {
    let values = params
        .as_array()
        .filter(|values| (1..=3).contains(&values.len()))
        .ok_or(HttpControlError::InvalidParams(
            "addMetalink requires base64 bytes, optional options and position",
        ))?;
    let encoded = values[0].as_str().ok_or(HttpControlError::InvalidParams(
        "Metalink must be base64 text",
    ))?;
    if encoded.len()
        > crate::metalink::MAX_METALINK_DOCUMENT_BYTES
            .saturating_mul(4)
            .div_ceil(3)
    {
        return Err(HttpControlError::Busy);
    }
    let mut options = values.get(1).cloned().unwrap_or_else(|| json!({}));
    let options = options
        .as_object_mut()
        .ok_or(HttpControlError::InvalidParams(
            "Metalink options must be an object",
        ))?;
    let mut parsing = crate::MetalinkOptions {
        metadata_bytes: encoded
            .len()
            .saturating_mul(8)
            .saturating_add(64 * 1024)
            .min(4 * 1024 * 1024),
        ..Default::default()
    };
    let names = [
        "metalink-base-uri",
        "select-file",
        "metalink-language",
        "metalink-os",
        "metalink-version",
        "metalink-location",
        "metalink-preferred-protocol",
        "metalink-enable-unique-protocol",
        "metadata-max-document-size",
        "metadata-max-files",
        "metadata-max-sources",
    ];
    for name in names {
        let Some(value) = options.remove(name) else {
            continue;
        };
        let registry = builtin_registry();
        let definition = registry.find(name).expect("Metalink option registered");
        let parsed = parse_option_value(definition, &option_input_text(&value)?, None)
            .map_err(|_| HttpControlError::InvalidParams("invalid Metalink option"))?;
        let value = canonical_option_value(&parsed)?;
        match name {
            "metalink-base-uri" => parsing.base_uri = Some(value),
            "select-file" => parsing.select_file = Some(value),
            "metalink-language" => parsing.language = Some(value),
            "metalink-os" => parsing.os = Some(value),
            "metalink-version" => parsing.version = Some(value),
            "metalink-location" => parsing.location = Some(value),
            "metalink-preferred-protocol" => {
                parsing.preferred_protocol = Some(
                    crate::TransferProtocol::parse(&value).map_err(HttpControlError::TaskSpec)?,
                )
            }
            "metalink-enable-unique-protocol" => parsing.unique_protocol = value == "true",
            "metadata-max-document-size" => {
                parsing.max_document_bytes = value
                    .parse()
                    .map_err(|_| HttpControlError::InvalidParams("metadata size"))?
            }
            "metadata-max-files" => {
                parsing.max_files = value
                    .parse()
                    .map_err(|_| HttpControlError::InvalidParams("metadata files"))?
            }
            "metadata-max-sources" => {
                parsing.max_sources = value
                    .parse()
                    .map_err(|_| HttpControlError::InvalidParams("metadata sources"))?
            }
            _ => unreachable!(),
        }
    }
    if values.get(2).is_some_and(|position| {
        parse_i64(position, "position").map_or(true, |position| position < -1)
    }) {
        return Err(HttpControlError::InvalidParams(
            "Metalink queue position must be -1 or nonnegative",
        ));
    }
    if let Some(value) = options.get("checksum") {
        parsing.user_checksum = Some(
            crate::ContentChecksum::parse(
                value
                    .as_str()
                    .ok_or(HttpControlError::InvalidParams("checksum must be text"))?,
            )
            .map_err(|_| HttpControlError::InvalidParams("invalid checksum"))?,
        );
    }
    if options.contains_key("out") {
        return Err(HttpControlError::InvalidParams(
            "Metalink output names come from selected files",
        ));
    }
    request
        .reserve(
            encoded
                .len()
                .saturating_add(parsing.metadata_bytes)
                .saturating_add(64 * 1024),
        )
        .map_err(|_| HttpControlError::Busy)?;
    let bytes = base64ct::Base64::decode_vec(encoded)
        .map_err(|_| HttpControlError::InvalidParams("invalid Metalink base64"))?;
    let document = crate::parse_metalink(&bytes, &parsing).map_err(|error| match error {
        crate::MetalinkError::Limit => HttpControlError::Busy,
        crate::MetalinkError::NoUsableSource | crate::MetalinkError::UnsupportedMetaurl => {
            HttpControlError::Unsupported("Metalink has no enabled source protocol")
        }
        _ => HttpControlError::InvalidParams("invalid Metalink metadata"),
    })?;
    if document.files.len() > ariax_storage::SESSION_MAX_IMPORT_TASKS {
        return Err(HttpControlError::Busy);
    }
    Ok(document
        .files
        .into_iter()
        .map(|file| {
            let mut options = options.clone();
            options.remove("piece-length");
            options.insert("out".into(), Value::String(file.name.canonical_string()));
            crate::session_file::ImportedTask {
                uris: file
                    .sources
                    .iter()
                    .map(|source| source.uri.clone())
                    .collect(),
                sources: None,
                options: Value::Object(options),
                verification: Some(file.verification),
                metalink_index: Some(file.index),
                priorities: Some(file.sources.iter().map(|source| source.priority).collect()),
            }
        })
        .collect())
}
