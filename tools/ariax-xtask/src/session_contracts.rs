use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_storage::{
    ALL_SESSION_IO_OPERATIONS, ALL_SESSION_OWNER_ERROR_CODES, ALL_SESSION_SQLITE_LIMITS,
    ALL_SESSION_STORE_ERROR_CODES, JournalInstallPhase, MAX_OPTION_MAP_BYTES,
    SESSION_BUNDLED_SQLITE_FLAGS, SESSION_BUSY_TIMEOUT_MS, SESSION_DEFAULT_CACHE_KIB,
    SESSION_HOST_KEY_PIN_OPTION, SESSION_INSTALL_READ_BUDGET_BYTES, SESSION_MAX_ALGORITHM_BYTES,
    SESSION_MAX_BT_RESUME_BYTES, SESSION_MAX_CACHE_KIB, SESSION_MAX_HOST_KEY_BYTES,
    SESSION_MAX_OPTIONS_PER_TASK, SESSION_MAX_SAFE_MESSAGE_BYTES, SESSION_MAX_SAFE_URI_BYTES,
    SESSION_MAX_SOURCES_PER_TASK, SESSION_MAX_TASKS, SESSION_MIN_CACHE_KIB,
    SESSION_MMAP_SIZE_BYTES, SESSION_OWNER_DEFAULT_CAPACITY,
    SESSION_OWNER_DEFAULT_SHUTDOWN_TIMEOUT, SESSION_OWNER_DEFAULT_STARTUP_TIMEOUT,
    SESSION_OWNER_LOCK_SUFFIX, SESSION_OWNER_MAX_CAPACITY, SESSION_OWNER_MAX_WAIT,
    SESSION_PAGE_SIZE_BYTES, SESSION_RUSQLITE_FEATURES, SESSION_RUSQLITE_VERSION,
    SESSION_SCHEMA_OBJECTS, SESSION_SCHEMA_VERSION, SESSION_SOURCE_READ_BUDGET_BYTES,
    SESSION_TASK_READ_BUDGET_BYTES, SESSION_WAL_AUTO_CHECKPOINT_PAGES, SessionQueueState,
    SessionSchemaObjectKind, SessionTerminalStatus,
};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

const SESSION_OUTPUT: &str = "generated/session_v2.json";

pub(crate) fn generate_session_contracts(
    workspace_root: &Path,
    mode: GenerationMode,
) -> Result<String, String> {
    validate_build_contract(workspace_root)?;
    let outputs = [(PathBuf::from(SESSION_OUTPUT), render_session_contracts())];
    apply_outputs(workspace_root, &outputs, mode)?;
    let table_count = SESSION_SCHEMA_OBJECTS
        .iter()
        .filter(|object| object.kind == SessionSchemaObjectKind::Table)
        .count();
    let index_count = SESSION_SCHEMA_OBJECTS.len() - table_count;
    Ok(format!(
        "{} SQLite session v{} contracts: {} tables, {} indexes, {} connection limits",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        SESSION_SCHEMA_VERSION,
        table_count,
        index_count,
        ALL_SESSION_SQLITE_LIMITS.len(),
    ))
}

fn validate_build_contract(workspace_root: &Path) -> Result<(), String> {
    let manifest = fs::read_to_string(workspace_root.join("Cargo.toml"))
        .map_err(|error| format!("failed to read workspace Cargo.toml: {error}"))?;
    let cargo_config = fs::read_to_string(workspace_root.join(".cargo/config.toml"))
        .map_err(|error| format!("failed to read .cargo/config.toml: {error}"))?;
    validate_build_contract_text(&manifest, &cargo_config)
}

fn expected_rusqlite_dependency() -> String {
    format!(
        "rusqlite = {{ version = \"={}\", default-features = false, features = [{}] }}",
        SESSION_RUSQLITE_VERSION,
        SESSION_RUSQLITE_FEATURES
            .iter()
            .map(|feature| json_string(feature))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn expected_bundled_sqlite_flags_config() -> String {
    format!(
        "LIBSQLITE3_FLAGS = {{ value = {}, force = true }}",
        json_string(SESSION_BUNDLED_SQLITE_FLAGS)
    )
}

fn validate_build_contract_text(manifest: &str, cargo_config: &str) -> Result<(), String> {
    let expected_dependency = expected_rusqlite_dependency();
    if !manifest.lines().any(|line| line == expected_dependency) {
        return Err(format!(
            "workspace rusqlite dependency must be exactly: {expected_dependency}"
        ));
    }
    let expected_flag = expected_bundled_sqlite_flags_config();
    if !cargo_config.lines().any(|line| line == expected_flag) {
        return Err(format!(
            "bundled SQLite compile flag must be forced exactly: {expected_flag}"
        ));
    }
    Ok(())
}

fn render_session_contracts() -> String {
    let owner_default_startup_timeout_ms = SESSION_OWNER_DEFAULT_STARTUP_TIMEOUT.as_millis();
    let owner_default_shutdown_timeout_ms = SESSION_OWNER_DEFAULT_SHUTDOWN_TIMEOUT.as_millis();
    let owner_max_wait_ms = SESSION_OWNER_MAX_WAIT.as_millis();
    let mut output = String::new();
    writeln!(output, "{{\n  \"schema\": {SESSION_SCHEMA_VERSION},").expect("write to string");
    writeln!(
        output,
        "  \"dependency\": {{\"crate\": \"rusqlite\", \"version\": {}, \"default_features\": false, \"features\": [{}], \"bundled_sqlite_flags\": {}}},",
        json_string(SESSION_RUSQLITE_VERSION),
        SESSION_RUSQLITE_FEATURES
            .iter()
            .map(|feature| json_string(feature))
            .collect::<Vec<_>>()
            .join(", "),
        json_string(SESSION_BUNDLED_SQLITE_FLAGS),
    )
    .expect("write to string");
    writeln!(
        output,
        "  \"pragmas\": {{\"new_page_size_bytes\": {SESSION_PAGE_SIZE_BYTES}, \"foreign_keys\": true, \"synchronous\": \"FULL\", \"busy_timeout_ms\": {SESSION_BUSY_TIMEOUT_MS}, \"cache_kib_default\": {SESSION_DEFAULT_CACHE_KIB}, \"cache_kib_minimum\": {SESSION_MIN_CACHE_KIB}, \"cache_kib_maximum\": {SESSION_MAX_CACHE_KIB}, \"mmap_size_bytes\": {SESSION_MMAP_SIZE_BYTES}, \"wal_auto_checkpoint_pages\": {SESSION_WAL_AUTO_CHECKPOINT_PAGES}, \"preferred_journal_mode\": \"WAL\", \"fallback_journal_mode\": \"DELETE\"}},"
    )
    .expect("write to string");
    output.push_str("  \"connection_limits\": [\n");
    for (index, limit) in ALL_SESSION_SQLITE_LIMITS.iter().copied().enumerate() {
        writeln!(
            output,
            "    {{\"code\": {}, \"value\": {}}}{}",
            json_string(limit.code()),
            limit.value(),
            comma(index, ALL_SESSION_SQLITE_LIMITS.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"queue_states\": [\n");
    for (index, state) in SessionQueueState::ALL.iter().copied().enumerate() {
        writeln!(
            output,
            "    {{\"number\": {}, \"code\": {}}}{}",
            state as i64,
            json_string(state.code()),
            comma(index, SessionQueueState::ALL.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"terminal_statuses\": [\n");
    for (index, status) in SessionTerminalStatus::ALL.iter().copied().enumerate() {
        writeln!(
            output,
            "    {{\"number\": {}, \"code\": {}}}{}",
            status as i64,
            json_string(status.code()),
            comma(index, SessionTerminalStatus::ALL.len())
        )
        .expect("write to string");
    }
    output.push_str(
        "  ],\n  \"migrations\": [{\"from\": 1, \"to\": 2, \"operation\": \"transactional_task_and_host_key_challenge_table_rebuild\"}],\n  \"journal_install_phases\": [\n",
    );
    for (index, phase) in JournalInstallPhase::ALL.iter().copied().enumerate() {
        writeln!(
            output,
            "    {{\"number\": {}, \"code\": {}}}{}",
            phase as i64,
            json_string(phase.code()),
            comma(index, JournalInstallPhase::ALL.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"schema_objects\": [\n");
    for (index, object) in SESSION_SCHEMA_OBJECTS.iter().enumerate() {
        writeln!(
            output,
            "    {{\"kind\": {}, \"name\": {}, \"sql\": {}}}{}",
            json_string(object.kind.code()),
            json_string(object.name),
            json_string(object.sql),
            comma(index, SESSION_SCHEMA_OBJECTS.len())
        )
        .expect("write to string");
    }
    writeln!(
        output,
        "  ],\n  \"caps\": {{\"safe_uri_bytes\": {SESSION_MAX_SAFE_URI_BYTES}, \"safe_message_bytes\": {SESSION_MAX_SAFE_MESSAGE_BYTES}, \"host_key_bytes\": {SESSION_MAX_HOST_KEY_BYTES}, \"algorithm_bytes\": {SESSION_MAX_ALGORITHM_BYTES}, \"bt_resume_bytes\": {SESSION_MAX_BT_RESUME_BYTES}, \"tasks\": {SESSION_MAX_TASKS}, \"options_per_task\": {SESSION_MAX_OPTIONS_PER_TASK}, \"option_map_bytes\": {MAX_OPTION_MAP_BYTES}, \"sources_per_task\": {SESSION_MAX_SOURCES_PER_TASK}, \"source_read_budget_bytes\": {SESSION_SOURCE_READ_BUDGET_BYTES}, \"task_read_budget_bytes\": {SESSION_TASK_READ_BUDGET_BYTES}, \"journal_install_read_budget_bytes\": {SESSION_INSTALL_READ_BUDGET_BYTES}, \"owner_request_default_capacity\": {SESSION_OWNER_DEFAULT_CAPACITY}, \"owner_request_max_capacity\": {SESSION_OWNER_MAX_CAPACITY}, \"owner_default_startup_timeout_ms\": {owner_default_startup_timeout_ms}, \"owner_default_shutdown_timeout_ms\": {owner_default_shutdown_timeout_ms}, \"owner_max_wait_ms\": {owner_max_wait_ms}}},"
    )
    .expect("write to string");
    output.push_str("  \"io_operations\": [");
    write_codes(
        &mut output,
        ALL_SESSION_IO_OPERATIONS
            .iter()
            .copied()
            .map(|operation| operation.code()),
    );
    output.push_str("],\n  \"errors\": [");
    write_codes(&mut output, ALL_SESSION_STORE_ERROR_CODES.iter().copied());
    output.push_str(
        "],\n  \"semantic_guards\": {\"host_key_departure_requires_challenge_clear\": true, \"host_key_read_budget_charges_owned_record_overhead\": true, \"task_source_startup_rechecks_per_task_budget\": true, \"startup_task_source_global_budget_charges_owned_sets_and_rows\": true, \"startup_materializes_one_source_set_per_task\": true, \"redacted_task_source_requires_credentials\": true, \"startup_session_repairs_are_owner_ordered\": true, \"journal_authority_repair_rechecks_primary_pointer\": true, \"native_startup_requires_repair_handoff\": true, \"native_startup_resolves_installs_before_appenders\": true, \"native_startup_installs_appenders_on_owner\": true, \"native_startup_publishes_scheduler_last\": true, \"stopped_result_status_payload_is_canonical\": true},\n  \"owner_errors\": [",
    );
    write_codes(&mut output, ALL_SESSION_OWNER_ERROR_CODES.iter().copied());
    writeln!(
        output,
        "],\n  \"host_key_pin_option\": {},\n  \"journal_install_token_fields\": [\"gid\", \"checkpoint_id\", \"new_journal_id\"],\n  \"rules\": {{\"strict_tables\": true, \"u64_may_use_exact_le_blob8\": true, \"tagged_platform_path_matches_journal_codec\": true, \"newer_schema_rejected_before_database_mutation\": true, \"v1_schema_validated_before_journal_mode_change\": true, \"v1_preflight_precedes_backup_creation\": true, \"invalid_v1_semantics_create_no_backup\": true, \"v1_to_v2_requires_private_timestamped_no_clobber_backup\": true, \"v1_to_v2_rebuild_and_user_version_share_transaction\": true, \"v1_to_v2_rebuilds_task_and_host_key_challenge_tables\": true, \"hot_rollback_recovery_supported\": true, \"rollback_journal_page_one_preflight\": true, \"wal_user_version_preflight_validates_committed_frames\": true, \"unversioned_nonempty_database_rejected\": true, \"required_limits_verified_exactly\": true, \"journal_modes_require_transactional_page_one_write_probe\": true, \"wal_write_probe_failure_falls_back_to_delete\": true, \"wal_truncate_checkpoint_reports_busy\": true, \"persistence_paths_require_explicit_parent\": true, \"existing_parent_must_be_private\": true, \"created_directories_private_at_creation\": true, \"created_directory_mode\": \"0700\", \"unix_database_mode\": \"0600\", \"windows_acl_uses_native_adapter_without_subprocesses\": true, \"owner_lock_suffix\": {}, \"owner_lock_is_cooperative_single_writer\": true, \"orphan_sidecars_rejected_when_database_missing_or_empty\": true, \"orphan_sidecars_rejected_before_backup_publish\": true, \"backup_reserved_sqlite_suffixes_rejected_ascii_insensitively\": true, \"hard_linked_persistence_artifacts_rejected\": true, \"artifact_symlinks_and_nonregular_files_rejected\": true, \"intermediate_links_and_windows_reparse_points_rejected\": true, \"queue_reorder_is_one_immediate_dense_transaction\": true, \"queue_transition_is_one_immediate_dense_transaction\": true, \"queue_transition_requires_exact_supplied_final_orders\": true, \"queue_transition_updates_slow_slot_metadata_atomically\": true, \"no_space_condition_update_is_atomic_and_queue_gated\": true, \"demoted_queue_requires_slow_slot_metadata\": true, \"demoted_queue_requires_nonzero_slow_demotion_count\": true, \"slow_demotion_count_persists_outside_demoted_queue\": true, \"slow_retry_decision_contains_only_wall_schedule\": true, \"slow_retry_delay_is_nonzero\": true, \"stopped_task_is_authoritative_queue_owner\": true, \"stopped_task_and_result_are_one_to_one\": true, \"terminal_retention_is_one_immediate_dense_transaction\": true, \"stopped_result_deletion_is_one_immediate_dense_transaction\": true, \"task_source_replacement_is_atomic_and_bounded\": true, \"task_source_semantic_validation_is_streaming\": true, \"host_key_text_requires_utf8\": true, \"host_key_fingerprint_matches_presented_key_sha256\": true, \"host_key_challenge_requires_paused_task\": true, \"host_key_resolution_is_challenge_and_key_bound\": true, \"host_key_pin_and_challenge_clear_share_transaction\": true, \"secret_options_rejected_before_sql\": true, \"task_option_policy_rechecked_on_read\": true, \"bounded_task_stopped_host_and_install_reads\": true, \"spawned_session_store_and_appenders_remain_on_owner_thread\": true, \"session_owner_admission_is_hard_capped\": true, \"accepted_commands_reserve_completion_capacity\": true, \"session_owner_shutdown_is_out_of_band_and_drains_accepted\": true, \"journal_cache_reconciliation_never_changes_queue_authority\": true, \"task_queue_and_pointer_immutable_via_put\": true, \"journal_install_begin_checks_old_pointer\": true, \"journal_install_commands_require_identity_token\": true, \"journal_install_abort_rechecks_old_pointer\": true, \"journal_install_complete_rechecks_old_pointer\": true, \"journal_install_pointer_relation_validated_on_open\": true, \"installed_pointer_and_phase_change_share_transaction\": true, \"hot_backup_refuses_overwrite_and_runs_integrity_check\": true, \"hot_backup_flushes_file_before_publish\": true, \"hot_backup_publishes_with_no_clobber\": true, \"hot_backup_temp_uses_delete_journal_mode\": true, \"hot_backup_removes_owned_temporary_sidecars\": true}}\n}}",
        json_string(SESSION_HOST_KEY_PIN_OPTION),
        json_string(SESSION_OWNER_LOCK_SUFFIX),
    )
    .expect("write to string");
    output
}

fn write_codes<'a>(output: &mut String, values: impl Iterator<Item = &'a str>) {
    for (index, code) in values.enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(code));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SESSION_BUNDLED_SQLITE_FLAGS, SESSION_OUTPUT, expected_bundled_sqlite_flags_config,
        expected_rusqlite_dependency, render_session_contracts, validate_build_contract,
        validate_build_contract_text,
    };
    use std::fs;
    use std::path::Path;

    #[test]
    fn generated_session_contract_freezes_schema_limits_and_authority_rules() {
        let contract = render_session_contracts();
        assert!(contract.contains("\"schema\": 2"));
        assert!(contract.contains("\"number\": 5, \"code\": \"demoted\""));
        assert!(contract.contains("\"number\": 1, \"code\": \"error\""));
        assert!(contract.contains("\"number\": 2, \"code\": \"complete\""));
        assert!(contract.contains("\"number\": 3, \"code\": \"removed\""));
        assert!(contract.contains("\"from\": 1, \"to\": 2"));
        assert!(contract.contains(
            "\"operation\": \"transactional_task_and_host_key_challenge_table_rebuild\""
        ));
        assert!(contract.contains("slow_original_position"));
        assert!(contract.contains("\"name\": \"journal_install\""));
        assert!(contract.contains("\"code\": \"like_pattern_length\", \"value\": 65536"));
        assert!(contract.contains("SQLITE_MAX_LIKE_PATTERN_LENGTH=65536"));
        assert!(contract.contains("\"tasks\": 100000"));
        assert!(contract.contains("\"option_map_bytes\": 4194304"));
        assert!(contract.contains("\"sources_per_task\": 4096"));
        assert!(contract.contains("\"source_read_budget_bytes\": 4194304"));
        assert!(contract.contains("\"owner_request_default_capacity\": 64"));
        assert!(contract.contains("\"owner_request_max_capacity\": 64"));
        assert!(contract.contains("\"owner_default_startup_timeout_ms\": 30000"));
        assert!(contract.contains("\"owner_default_shutdown_timeout_ms\": 30000"));
        assert!(contract.contains("\"owner_max_wait_ms\": 300000"));
        assert!(contract.contains("\"task_read_budget_bytes\": 67108864"));
        assert!(contract.contains("\"host_key_pin_option\": \"sftp-host-key-sha256\""));
        assert!(contract.contains("\"host_key_challenge_mismatch\""));
        assert!(contract.contains("\"owner_panicked\""));
        assert!(contract.contains("\"host_key_departure_requires_challenge_clear\": true"));
        assert!(contract.contains("\"host_key_read_budget_charges_owned_record_overhead\": true"));
        assert!(contract.contains("\"task_source_startup_rechecks_per_task_budget\": true"));
        assert!(
            contract.contains(
                "\"startup_task_source_global_budget_charges_owned_sets_and_rows\": true"
            )
        );
        assert!(contract.contains("\"startup_materializes_one_source_set_per_task\": true"));
        assert!(contract.contains("\"redacted_task_source_requires_credentials\": true"));
        assert!(contract.contains("\"startup_session_repairs_are_owner_ordered\": true"));
        assert!(contract.contains("\"native_startup_requires_repair_handoff\": true"));
        assert!(contract.contains("\"native_startup_resolves_installs_before_appenders\": true"));
        assert!(contract.contains("\"native_startup_publishes_scheduler_last\": true"));
        assert!(contract.contains("\"journal_authority_repair_rechecks_primary_pointer\": true"));
        assert!(contract.contains("\"stopped_result_status_payload_is_canonical\": true"));
        assert!(contract.contains(
            "\"journal_install_token_fields\": [\"gid\", \"checkpoint_id\", \"new_journal_id\"]"
        ));
        assert!(contract.contains("\"journal_install_complete_rechecks_old_pointer\": true"));
        assert!(contract.contains("\"journal_install_pointer_relation_validated_on_open\": true"));
        assert!(contract.contains("\"queue_transition_is_one_immediate_dense_transaction\": true"));
        assert!(
            contract.contains("\"queue_transition_requires_exact_supplied_final_orders\": true")
        );
        assert!(contract.contains("\"task_source_replacement_is_atomic_and_bounded\": true"));
        assert!(contract.contains("\"host_key_resolution_is_challenge_and_key_bound\": true"));
        assert!(contract.contains("\"session_owner_admission_is_hard_capped\": true"));
        assert!(contract.contains("\"accepted_commands_reserve_completion_capacity\": true"));
        assert!(
            contract
                .contains("\"session_owner_shutdown_is_out_of_band_and_drains_accepted\": true")
        );
        assert!(
            contract.contains("\"v1_to_v2_requires_private_timestamped_no_clobber_backup\": true")
        );
        assert!(contract.contains("\"v1_preflight_precedes_backup_creation\": true"));
        assert!(contract.contains("\"invalid_v1_semantics_create_no_backup\": true"));
        assert!(
            contract.contains("\"v1_to_v2_rebuilds_task_and_host_key_challenge_tables\": true")
        );
        assert!(
            contract.contains("\"queue_transition_updates_slow_slot_metadata_atomically\": true")
        );
        assert!(contract.contains("\"demoted_queue_requires_nonzero_slow_demotion_count\": true"));
        assert!(contract.contains("\"slow_demotion_count_persists_outside_demoted_queue\": true"));
        assert!(contract.contains("\"slow_retry_decision_contains_only_wall_schedule\": true"));
        assert!(contract.contains("\"owner_lock_suffix\": \".ariax-owner-lock\""));
        assert!(contract.contains("\"rollback_journal_page_one_preflight\": true"));
        assert!(
            contract.contains("\"orphan_sidecars_rejected_when_database_missing_or_empty\": true")
        );
        assert!(contract.contains("\"orphan_sidecars_rejected_before_backup_publish\": true"));
        assert!(
            contract
                .contains("\"backup_reserved_sqlite_suffixes_rejected_ascii_insensitively\": true")
        );
        assert!(contract.contains("\"hard_linked_persistence_artifacts_rejected\": true"));
        assert!(
            contract.contains("\"windows_acl_uses_native_adapter_without_subprocesses\": true")
        );
        assert!(contract.contains("\"task_option_policy_rechecked_on_read\": true"));
        assert!(contract.contains("\"secret_options_rejected_before_sql\": true"));
        assert!(contract.contains("\"stopped_task_and_result_are_one_to_one\": true"));
        assert!(
            contract.contains("\"terminal_retention_is_one_immediate_dense_transaction\": true")
        );
        assert!(
            contract
                .contains("\"stopped_result_deletion_is_one_immediate_dense_transaction\": true")
        );
        assert!(contract.contains("\"host_key_text_requires_utf8\": true"));
        assert!(contract.contains("\"host_key_fingerprint_matches_presented_key_sha256\": true"));
        assert!(contract.contains("\"host_key_challenge_requires_paused_task\": true"));
        assert!(contract.contains("\"bounded_task_stopped_host_and_install_reads\": true"));
        assert!(
            contract.contains("\"hot_backup_refuses_overwrite_and_runs_integrity_check\": true")
        );
        assert!(contract.contains("\"hot_backup_temp_uses_delete_journal_mode\": true"));
        assert!(contract.contains("\"hot_backup_removes_owned_temporary_sidecars\": true"));
    }

    #[test]
    fn workspace_manifest_and_bundled_sqlite_flags_match_the_contract() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("xtask is nested under the workspace");
        validate_build_contract(workspace_root).expect("build contract");
    }

    #[test]
    fn committed_session_contract_matches_the_renderer() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("xtask is nested under the workspace");
        let committed = fs::read_to_string(workspace_root.join(SESSION_OUTPUT))
            .expect("read committed session contract");
        assert_eq!(committed, render_session_contracts());
    }

    #[test]
    fn bundled_sqlite_flags_must_override_ambient_environment() {
        let manifest = expected_rusqlite_dependency();
        let forced = expected_bundled_sqlite_flags_config();
        validate_build_contract_text(&manifest, &forced).expect("forced build contract");

        let unforced = forced
            .strip_suffix(", force = true }")
            .map(|prefix| format!("{prefix} }}"))
            .expect("forced config shape");
        assert!(validate_build_contract_text(&manifest, &unforced).is_err());
    }

    #[test]
    fn cargo_build_uses_repository_sqlite_flags() {
        assert_eq!(
            option_env!("LIBSQLITE3_FLAGS"),
            Some(SESSION_BUNDLED_SQLITE_FLAGS)
        );
    }
}
