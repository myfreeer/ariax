use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_storage::{
    ALL_LAYOUT_ERRORS, ALL_MAP_SPAN_ERRORS, ALL_PATH_VALIDATION_ERRORS,
    ALL_ROOT_BINDING_ERROR_CLASSES, LAYOUT_HASH_DOMAIN, MAX_IDENTITY_BYTES, MAX_LAYOUT_BYTES,
    MAX_LAYOUT_ENTRIES, MAX_PLATFORM_PATH_BYTES, MAX_SAFE_RELATIVE_BYTES, PathPlatform,
    ROOT_BINDING_HASH_DOMAIN,
};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const STORAGE_OUTPUT: &str = "generated/storage_layout.json";

pub(crate) fn generate_storage_contracts(
    workspace_root: &Path,
    mode: GenerationMode,
) -> Result<String, String> {
    let outputs = [(PathBuf::from(STORAGE_OUTPUT), render_storage_contracts())];
    apply_outputs(workspace_root, &outputs, mode)?;
    Ok(format!(
        "{} portable storage contracts: {} path errors, {} layout errors, {} mapper errors",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        ALL_PATH_VALIDATION_ERRORS.len(),
        ALL_LAYOUT_ERRORS.len(),
        ALL_MAP_SPAN_ERRORS.len()
    ))
}

fn render_storage_contracts() -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n  \"hashes\": [\n");
    writeln!(
        output,
        "    {{\"name\": \"layout_hash\", \"algorithm\": \"sha-256\", \"domain_hex\": {}, \"includes\": [\"relative_file_map\", \"lengths\", \"selection\", \"piece_geometry\"], \"excludes\": [\"root_location\", \"file_identity\"]}},",
        json_string(&hex_bytes(LAYOUT_HASH_DOMAIN.as_bytes()))
    )
    .expect("write to string");
    writeln!(
        output,
        "    {{\"name\": \"root_binding_hash\", \"algorithm\": \"sha-256\", \"domain_hex\": {}, \"includes\": [\"platform_tag\", \"canonical_root_path\", \"root_identity\", \"opened_file_identities\"]}}",
        json_string(&hex_bytes(ROOT_BINDING_HASH_DOMAIN.as_bytes()))
    )
    .expect("write to string");
    write!(
        output,
        "  ],\n  \"caps\": {{\"safe_relative_path_bytes\": {MAX_SAFE_RELATIVE_BYTES}, \"platform_path_bytes\": {MAX_PLATFORM_PATH_BYTES}, \"identity_bytes\": {MAX_IDENTITY_BYTES}, \"layout_entries\": {MAX_LAYOUT_ENTRIES}, \"canonical_layout_bytes\": {MAX_LAYOUT_BYTES}}},\n"
    )
    .expect("write to string");
    output.push_str("  \"platform_tags\": [\n");
    for (index, platform) in [PathPlatform::Unix, PathPlatform::Windows]
        .into_iter()
        .enumerate()
    {
        writeln!(
            output,
            "    {{\"code\": {}, \"tag\": {}}}{}",
            json_string(platform.code()),
            platform as u8,
            comma(index, 2)
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"path_rejections\": ");
    write_code_array(
        &mut output,
        &ALL_PATH_VALIDATION_ERRORS.map(|error| error.code()),
    );
    output.push_str(",\n  \"root_binding_rejections\": ");
    write_code_array(
        &mut output,
        &ALL_ROOT_BINDING_ERROR_CLASSES.map(|error| error.code()),
    );
    output.push_str(",\n  \"layout_rejections\": ");
    write_code_array(&mut output, &ALL_LAYOUT_ERRORS.map(|error| error.code()));
    output.push_str(",\n  \"offset_mapper_rejections\": ");
    write_code_array(&mut output, &ALL_MAP_SPAN_ERRORS.map(|error| error.code()));
    output.push_str(",\n  \"native_capability_contract\": {\"check_then_open_allowed\": false, \"status\": \"native_backend_required\"}\n}\n");
    output
}

fn write_code_array(output: &mut String, values: &[&str]) {
    output.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(value));
    }
    output.push(']');
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("write to string");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::render_storage_contracts;

    #[test]
    fn generated_contract_preserves_security_and_mapping_boundaries() {
        let contract = render_storage_contracts();
        assert!(contract.contains("\"check_then_open_allowed\": false"));
        assert!(contract.contains("\"absolute_path\""));
        assert!(contract.contains("\"root_binding_mismatch\""));
        assert!(contract.contains("\"cross_file_span\""));
        assert!(contract.contains("\"algorithm\": \"sha-256\""));
    }
}
