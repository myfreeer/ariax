use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_core::ALL_ERROR_KINDS;
use ariax_storage::{
    ALL_DATA_BARRIER_KINDS, ALL_DURABILITY_MODES, ALL_GENERATION_START_REASONS,
    ALL_HEADER_DECODE_ERRORS, ALL_JOURNAL_APPENDER_ERROR_CODES, ALL_JOURNAL_APPENDER_FAULTS,
    ALL_JOURNAL_DIGEST_ALGORITHMS, ALL_JOURNAL_IO_OPERATIONS, ALL_JOURNAL_STATE_ERROR_CODES,
    ALL_JOURNAL_TAIL_MISMATCHES, ALL_LEASE_ABORT_REASONS, ALL_OPTIONS_SNAPSHOT_SCOPES,
    ALL_PAYLOAD_CODEC_ERROR_CLASSES, ALL_RECORD_STOP_REASONS, ALL_RECORD_TYPES,
    ALL_REPLAY_RESOURCES, ALL_RETRY_REASONS, ALL_RETRY_SCOPES, ALL_TASK_PAUSE_REASONS,
    ALL_TASK_REMOVE_REASONS, CHECKPOINT_STATE_HASH_DOMAIN, COMMIT_MAGIC, CONTRIBUTORS_HASH_DOMAIN,
    HEADER_MAGIC, JOURNAL_ENDIANNESS_ASSERTION, JOURNAL_FORMAT_VERSION,
    JOURNAL_SEGMENT_FILE_PREFIX, JOURNAL_SEGMENT_FILE_SUFFIX, JOURNAL_TEMP_FILE_SUFFIX,
    JournalStateLimits, MAX_DIGEST_ALGORITHM_BYTES, MAX_DIGEST_VALUE_BYTES, MAX_IDENTITY_BYTES,
    MAX_LAYOUT_BYTES, MAX_LAYOUT_ENTRIES, MAX_OPTION_KEY_BYTES, MAX_OPTION_MAP_BYTES,
    MAX_OPTION_MAP_ENTRIES, MAX_OPTION_VALUE_BYTES, MAX_PIECE_STATE_BITMAP_BYTES,
    MAX_PIECE_STATE_COVERED_PIECES, MAX_PLATFORM_PATH_BYTES, MAX_RECORD_PAYLOAD,
    MAX_SAFE_RELATIVE_BYTES, OPTIONS_SNAPSHOT_HASH_DOMAIN, PAYLOAD_CODEC_RECORD_TYPES,
    REBIND_VALIDATOR_SET_HASH_DOMAIN, RECORD_MAGIC, RECORD_OVERHEAD, RECORD_PREFIX_LEN, RecordType,
    ReplayLimits, SEGMENT_HASH_DOMAIN, SEGMENT_HEADER_LEN, VALIDATOR_SET_HASH_DOMAIN,
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
        "{} journal v1 contracts: {} record types, {} header rejections, {} record stop reasons, {} semantic rejections, {} appender fault classes",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        ALL_RECORD_TYPES.len(),
        ALL_HEADER_DECODE_ERRORS.len(),
        ALL_RECORD_STOP_REASONS.len(),
        ALL_JOURNAL_STATE_ERROR_CODES.len(),
        ALL_JOURNAL_APPENDER_FAULTS.len(),
    ))
}

fn render_journal_contracts() -> String {
    let limits = ReplayLimits::default();
    let state_limits = JournalStateLimits::default();
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
    output.push_str("  },\n  \"payload_codec\": {\n");
    output.push_str("    \"implemented_record_types\": [");
    write_record_type_codes(&mut output, PAYLOAD_CODEC_RECORD_TYPES.iter().copied());
    output.push_str("],\n    \"pending_record_types\": [");
    write_record_type_codes(
        &mut output,
        ALL_RECORD_TYPES
            .iter()
            .copied()
            .filter(|record_type| !PAYLOAD_CODEC_RECORD_TYPES.contains(record_type)),
    );
    writeln!(
        output,
        "],\n    \"caps\": {{\"digest_algorithm_bytes\": {MAX_DIGEST_ALGORITHM_BYTES}, \"digest_value_bytes\": {MAX_DIGEST_VALUE_BYTES}, \"option_map_entries\": {MAX_OPTION_MAP_ENTRIES}, \"option_key_bytes\": {MAX_OPTION_KEY_BYTES}, \"option_value_bytes\": {MAX_OPTION_VALUE_BYTES}, \"option_map_bytes\": {MAX_OPTION_MAP_BYTES}, \"layout_entries\": {MAX_LAYOUT_ENTRIES}, \"layout_canonical_bytes\": {MAX_LAYOUT_BYTES}, \"safe_relative_path_bytes\": {MAX_SAFE_RELATIVE_BYTES}, \"platform_path_bytes\": {MAX_PLATFORM_PATH_BYTES}, \"identity_bytes\": {MAX_IDENTITY_BYTES}, \"piece_state_covered_pieces\": {MAX_PIECE_STATE_COVERED_PIECES}, \"piece_state_bitmap_bytes\": {MAX_PIECE_STATE_BITMAP_BYTES}}},"
    )
    .expect("write to string");
    output.push_str("    \"digest_algorithms\": [\n");
    for (index, algorithm) in ALL_JOURNAL_DIGEST_ALGORITHMS.iter().copied().enumerate() {
        writeln!(
            output,
            "      {{\"code\": {}, \"value_bytes\": {}}}{}",
            json_string(algorithm.code()),
            algorithm.value_len(),
            comma(index, ALL_JOURNAL_DIGEST_ALGORITHMS.len())
        )
        .expect("write to string");
    }
    output.push_str("    ],\n    \"rejections\": [");
    for (index, error) in ALL_PAYLOAD_CODEC_ERROR_CLASSES.iter().copied().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(error.code()));
    }
    output.push_str("],\n    \"rules\": {\"exact_little_endian_scalars\": true, \"nonzero_typed_ids\": true, \"nonempty_nonoverflowing_spans\": true, \"unknown_tags_rejected\": true, \"trailing_bytes_rejected\": true, \"typed_append_prevents_record_type_mismatch\": true, \"counts_bounded_before_allocation\": true, \"option_map_utf8_sorted_unique\": true, \"layout_entries_sorted_contiguous\": true, \"piece_state_bitmap_exact_length\": true, \"piece_state_evidence_exact_coverage\": true}\n  },\n  \"semantic_recovery\": {\n");
    writeln!(
        output,
        "    \"hash_domains\": {{\"options_snapshot\": {}, \"contributors\": {}, \"validator_set\": {}, \"rebind_validator_set\": {}, \"checkpoint_state\": {}}},",
        json_string(&hex_bytes(OPTIONS_SNAPSHOT_HASH_DOMAIN.as_bytes())),
        json_string(&hex_bytes(CONTRIBUTORS_HASH_DOMAIN.as_bytes())),
        json_string(&hex_bytes(VALIDATOR_SET_HASH_DOMAIN.as_bytes())),
        json_string(&hex_bytes(REBIND_VALIDATOR_SET_HASH_DOMAIN.as_bytes())),
        json_string(&hex_bytes(CHECKPOINT_STATE_HASH_DOMAIN.as_bytes())),
    )
    .expect("write to string");
    output.push_str("    \"hash_coverage\": {\"options_snapshot\": [\"domain\", \"entry_count_le_u32\", \"sorted_key_length_key_value_length_value\"], \"contributors\": [\"domain\", \"contributor_count_le_u32\", \"sorted_lease_id_span_validator_fingerprint\"], \"validator_set\": [\"domain\", \"distinct_validator_count_le_u32\", \"sorted_distinct_validator_fingerprints\"], \"rebind_validator_set\": [\"domain\", \"previous_root_binding_hash\", \"new_root_binding_hash\", \"digest_algorithm_and_value\"], \"checkpoint_state\": [\"domain\", \"state_record_count_le_u32\", \"repeated_record_type_generation_payload_length_payload\"]},\n");
    writeln!(
        output,
        "    \"limits\": {{\"records\": {}, \"leases\": {}, \"durable_pieces\": {}, \"retry_states\": {}, \"finalizations\": {}}},",
        state_limits.max_records,
        state_limits.max_leases,
        state_limits.max_durable_pieces,
        state_limits.max_retry_states,
        state_limits.max_finalizations,
    )
    .expect("write to string");
    output.push_str("    \"rejections\": [");
    for (index, code) in ALL_JOURNAL_STATE_ERROR_CODES.iter().copied().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(code));
    }
    output.push_str("],\n    \"rules\": {\"live_invalid_record_preserves_prior_semantic_prefix\": true, \"invalid_checkpoint_rejected_whole\": true, \"checkpoint_hash_excludes_sequence_crc_commit\": true, \"generation_started_only_advancer\": true, \"staged_snapshot_exact_match_required\": true, \"option_policy_required\": true, \"layout_chunks_immediate_and_recomputed\": true, \"provisional_leases_never_recovered_as_durable\": true, \"contributors_and_validator_sets_recomputed\": true, \"durability_barrier_must_match_task_mode\": true, \"piece_state_checkpoint_only\": true, \"different_identity_rebind_requires_digest_bound_lease_free_evidence\": true, \"terminal_marker_is_safety_veto\": true, \"finalization_pairs_exact\": true}\n  },\n  \"serialized_appender\": {\n");
    writeln!(
        output,
        "    \"segment_file_name\": {{\"prefix\": {}, \"decimal_index_width\": 10, \"suffix\": {}, \"temporary_suffix\": {}}},",
        json_string(JOURNAL_SEGMENT_FILE_PREFIX),
        json_string(JOURNAL_SEGMENT_FILE_SUFFIX),
        json_string(JOURNAL_TEMP_FILE_SUFFIX),
    )
    .expect("write to string");
    output.push_str("    \"acknowledgements\": {\"appended\": \"complete_record_bytes_written_to_active_handle\", \"flushed\": \"sync_all_completed_through_sequence\"},\n    \"errors\": [");
    write_codes(
        &mut output,
        ALL_JOURNAL_APPENDER_ERROR_CODES.iter().copied(),
    );
    output.push_str("],\n    \"faults\": [");
    write_codes(
        &mut output,
        ALL_JOURNAL_APPENDER_FAULTS
            .iter()
            .copied()
            .map(|value| value.code()),
    );
    output.push_str("],\n    \"io_operations\": [");
    write_codes(
        &mut output,
        ALL_JOURNAL_IO_OPERATIONS
            .iter()
            .copied()
            .map(|value| value.code()),
    );
    output.push_str("],\n    \"tail_mismatches\": [");
    write_codes(
        &mut output,
        ALL_JOURNAL_TAIL_MISMATCHES
            .iter()
            .copied()
            .map(|value| value.code()),
    );
    output.push_str("],\n    \"rules\": {\"single_sequence_owner\": true, \"typed_payloads_only\": true, \"codec_rejection_does_not_consume_sequence\": true, \"write_or_reopen_failure_latches_fault\": true, \"fault_blocks_later_sequences\": true, \"flush_may_cover_later_already_appended_records\": true, \"close_requires_fully_flushed_tail\": true, \"reopen_validates_length_header_tail_sequence_and_fingerprint\": true, \"rotation_requires_nonempty_fully_flushed_segment\": true, \"rotation_hashes_without_whole_segment_buffering\": true, \"new_header_synced_before_atomic_install\": true, \"parent_directory_synced_where_supported\": true, \"old_segments_immutable_after_rotation\": true}\n  },\n  \"header_rejections\": [");
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

fn write_record_type_codes(output: &mut String, values: impl Iterator<Item = RecordType>) {
    for (index, record_type) in values.enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(record_type.code()));
    }
}

fn write_codes<'a>(output: &mut String, values: impl Iterator<Item = &'a str>) {
    for (index, code) in values.enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(code));
    }
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
        assert!(contract.contains("\"typed_append_prevents_record_type_mismatch\": true"));
        assert!(contract.contains("\"pending_record_types\": []"));
        assert!(contract.contains("\"option_map_entries\": 4096"));
        assert!(contract.contains("\"piece_state_bitmap_bytes\": 16384"));
        assert!(contract.contains("\"piece_state_evidence_exact_coverage\": true"));
        assert!(contract.contains("\"semantic_recovery\""));
        assert!(contract.contains("\"durable_pieces\": 262144"));
        assert!(contract.contains("\"checkpoint_hash_excludes_sequence_crc_commit\": true"));
        assert!(contract.contains("\"forbidden_persisted_option\""));
        assert!(contract.contains("\"noncanonical_contributors\""));
        assert!(contract.contains("\"serialized_appender\""));
        assert!(contract.contains("\"single_sequence_owner\": true"));
        assert!(contract.contains("\"rotation_hashes_without_whole_segment_buffering\": true"));
        assert!(contract.contains("\"decimal_index_width\": 10"));
        assert!(contract.contains("\"code\": \"sha-512\", \"value_bytes\": 64"));
        assert!(contract.contains("\"valid_prefix_authoritative\": true"));
        assert!(contract.contains("\"payload_too_large\""));
    }
}
