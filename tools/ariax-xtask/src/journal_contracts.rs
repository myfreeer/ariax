use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_core::ALL_ERROR_KINDS;
use ariax_storage::{
    ALL_DATA_BARRIER_KINDS, ALL_DURABILITY_MODES, ALL_GENERATION_START_REASONS,
    ALL_HEADER_DECODE_ERRORS, ALL_LEASE_ABORT_REASONS, ALL_OPTIONS_SNAPSHOT_SCOPES,
    ALL_RECORD_STOP_REASONS, ALL_RECORD_TYPES, ALL_REPLAY_RESOURCES, ALL_RETRY_REASONS,
    ALL_RETRY_SCOPES, ALL_TASK_PAUSE_REASONS, ALL_TASK_REMOVE_REASONS, COMMIT_MAGIC, HEADER_MAGIC,
    JOURNAL_ENDIANNESS_ASSERTION, JOURNAL_FORMAT_VERSION, MAX_RECORD_PAYLOAD, RECORD_MAGIC,
    RECORD_OVERHEAD, RECORD_PREFIX_LEN, ReplayLimits, SEGMENT_HASH_DOMAIN, SEGMENT_HEADER_LEN,
};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const JOURNAL_OUTPUT: &str = "generated/journal_v1.json";

pub(crate) fn generate_journal_contracts(
    workspace_root: &Path,
    mode: GenerationMode,
) -> Result<String, String> {
    let outputs = [(PathBuf::from(JOURNAL_OUTPUT), render_journal_contracts())];
    apply_outputs(workspace_root, &outputs, mode)?;
    Ok(format!(
        "{} journal v1 contracts: {} record types, {} header rejections, {} record stop reasons",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        ALL_RECORD_TYPES.len(),
        ALL_HEADER_DECODE_ERRORS.len(),
        ALL_RECORD_STOP_REASONS.len()
    ))
}

fn render_journal_contracts() -> String {
    let limits = ReplayLimits::default();
    let mut output = String::new();
    writeln!(output, "{{\n  \"schema\": 1,").expect("write to string");
    writeln!(
        output,
        "  \"format\": {{\"version\": {JOURNAL_FORMAT_VERSION}, \"byte_order\": \"little_endian\", \"endianness_assertion\": {JOURNAL_ENDIANNESS_ASSERTION}, \"sequence_origin\": 1}},"
    )
    .expect("write to string");
    writeln!(
        output,
        "  \"segment_header\": {{\"magic\": {}, \"length\": {SEGMENT_HEADER_LEN}, \"flags\": 0, \"crc\": {{\"algorithm\": \"crc-32c-castagnoli\", \"coverage_bytes\": {}}}}},",
        json_string(ascii_magic(HEADER_MAGIC)),
        SEGMENT_HEADER_LEN - 4
    )
    .expect("write to string");
    writeln!(
        output,
        "  \"record\": {{\"magic\": {}, \"prefix_length\": {RECORD_PREFIX_LEN}, \"fixed_overhead\": {RECORD_OVERHEAD}, \"max_payload_bytes\": {MAX_RECORD_PAYLOAD}, \"flags\": 0, \"crc\": {{\"algorithm\": \"crc-32c-castagnoli\", \"coverage\": \"record_magic_through_payload\"}}, \"commit\": {}, \"commit_written_after_crc\": true}},",
        json_string(ascii_magic(RECORD_MAGIC)),
        json_string(ascii_magic(COMMIT_MAGIC))
    )
    .expect("write to string");
    writeln!(
        output,
        "  \"segment_hash\": {{\"algorithm\": \"sha-256\", \"domain_hex\": {}, \"coverage\": [\"domain\", \"encoded_length_le_u64\", \"valid_segment_bytes\"]}},",
        json_string(&hex_bytes(SEGMENT_HASH_DOMAIN.as_bytes()))
    )
    .expect("write to string");
    output.push_str("  \"record_types\": [\n");
    for (index, record_type) in ALL_RECORD_TYPES.iter().copied().enumerate() {
        writeln!(
            output,
            "    {{\"number\": {}, \"code\": {}}}{}",
            record_type as u16,
            json_string(record_type.code()),
            comma(index, ALL_RECORD_TYPES.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"tag_vocabularies\": {\n");
    write_tag_vocabulary(
        &mut output,
        "durability",
        ALL_DURABILITY_MODES
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "options_snapshot_scope",
        ALL_OPTIONS_SNAPSHOT_SCOPES
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "generation_start_reason",
        ALL_GENERATION_START_REASONS
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "lease_abort_reason",
        ALL_LEASE_ABORT_REASONS
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "data_barrier",
        ALL_DATA_BARRIER_KINDS
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "retry_scope",
        ALL_RETRY_SCOPES
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "retry_reason",
        ALL_RETRY_REASONS
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "task_pause_reason",
        ALL_TASK_PAUSE_REASONS
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "task_remove_reason",
        ALL_TASK_REMOVE_REASONS
            .iter()
            .map(|value| (value.number(), value.code())),
        true,
    );
    write_tag_vocabulary(
        &mut output,
        "error_class",
        ALL_ERROR_KINDS
            .iter()
            .map(|value| (value.number(), value.code())),
        false,
    );
    output.push_str("  },\n  \"header_rejections\": [");
    for (index, error) in ALL_HEADER_DECODE_ERRORS.iter().copied().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(error.code()));
    }
    output.push_str("],\n  \"record_stop_reasons\": [");
    for (index, reason) in ALL_RECORD_STOP_REASONS.iter().copied().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(reason.code()));
    }
    output.push_str("],\n  \"replay_limits\": {");
    write!(
        output,
        "\"segments\": {}, \"records\": {}, \"payload_bytes\": {}",
        limits.max_segments, limits.max_records, limits.max_payload_bytes
    )
    .expect("write to string");
    output.push_str("},\n  \"replay_resources\": [");
    for (index, resource) in ALL_REPLAY_RESOURCES.iter().copied().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(resource.code()));
    }
    output.push_str(
        "],\n  \"replay_contract\": {\"caller_orders_by_segment_index\": true, \"validates_task_and_journal_identity\": true, \"validates_previous_segment_hash\": true, \"global_sequence_contiguous\": true, \"stop_at_first_invalid_byte\": true, \"valid_prefix_authoritative\": true, \"newer_segments_after_failure_ignored\": true}\n}\n",
    );
    output
}

fn write_tag_vocabulary<'a>(
    output: &mut String,
    name: &str,
    values: impl ExactSizeIterator<Item = (u8, &'a str)>,
    trailing_comma: bool,
) {
    let length = values.len();
    writeln!(output, "    {}: [", json_string(name)).expect("write to string");
    for (index, (number, code)) in values.enumerate() {
        writeln!(
            output,
            "      {{\"number\": {number}, \"code\": {}}}{}",
            json_string(code),
            comma(index, length)
        )
        .expect("write to string");
    }
    writeln!(output, "    ]{}", if trailing_comma { "," } else { "" }).expect("write to string");
}

fn ascii_magic(magic: [u8; 4]) -> &'static str {
    match magic {
        HEADER_MAGIC => "ARXJ",
        RECORD_MAGIC => "ARXR",
        COMMIT_MAGIC => "CMIT",
        _ => unreachable!("journal magic is a closed contract"),
    }
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
    use super::render_journal_contracts;

    #[test]
    fn generated_contract_freezes_framing_and_replay_boundaries() {
        let contract = render_journal_contracts();
        assert!(contract.contains("\"magic\": \"ARXJ\""));
        assert!(contract.contains("\"commit\": \"CMIT\""));
        assert!(contract.contains("\"number\": 24, \"code\": \"piece_state_chunk\""));
        assert!(contract.contains("\"algorithm\": \"crc-32c-castagnoli\""));
        assert!(contract.contains("\"generation_start_reason\""));
        assert!(contract.contains("\"number\": 29, \"code\": \"InternalInvariant\""));
        assert!(contract.contains("\"valid_prefix_authoritative\": true"));
        assert!(contract.contains("\"payload_too_large\""));
    }
}
