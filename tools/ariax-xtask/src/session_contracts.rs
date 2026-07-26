use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_storage::{
    ALL_SESSION_IO_OPERATIONS, ALL_SESSION_SQLITE_LIMITS, ALL_SESSION_STORE_ERROR_CODES,
    JournalInstallPhase, SESSION_BUNDLED_SQLITE_FLAGS, SESSION_BUSY_TIMEOUT_MS,
    SESSION_DEFAULT_CACHE_KIB, SESSION_MAX_ALGORITHM_BYTES, SESSION_MAX_BT_RESUME_BYTES,
    SESSION_MAX_CACHE_KIB, SESSION_MAX_HOST_KEY_BYTES, SESSION_MAX_SAFE_MESSAGE_BYTES,
    SESSION_MAX_SAFE_URI_BYTES, SESSION_MIN_CACHE_KIB, SESSION_MMAP_SIZE_BYTES,
    SESSION_PAGE_SIZE_BYTES, SESSION_RUSQLITE_FEATURES, SESSION_RUSQLITE_VERSION,
    SESSION_SCHEMA_OBJECTS, SESSION_SCHEMA_VERSION, SESSION_WAL_AUTO_CHECKPOINT_PAGES,
    SessionQueueState, SessionSchemaObjectKind,
};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

const SESSION_OUTPUT: &str = "generated/session_v1.json";

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
    let expected_dependency = format!(
        "rusqlite = {{ version = \"={}\", default-features = false, features = [{}] }}",
        SESSION_RUSQLITE_VERSION,
        SESSION_RUSQLITE_FEATURES
            .iter()
            .map(|feature| json_string(feature))
            .collect::<Vec<_>>()
            .join(", ")
    );
    if !manifest.lines().any(|line| line == expected_dependency) {
        return Err(format!(
            "workspace rusqlite dependency must be exactly: {expected_dependency}"
        ));
    }
    let cargo_config = fs::read_to_string(workspace_root.join(".cargo/config.toml"))
        .map_err(|error| format!("failed to read .cargo/config.toml: {error}"))?;
    let expected_flag = format!(
        "LIBSQLITE3_FLAGS = {}",
        json_string(SESSION_BUNDLED_SQLITE_FLAGS)
    );
    if !cargo_config.lines().any(|line| line == expected_flag) {
        return Err(format!(
            "bundled SQLite compile flag must be exactly: {expected_flag}"
        ));
    }
    Ok(())
}

fn render_session_contracts() -> String {
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
    output.push_str("  ],\n  \"journal_install_phases\": [\n");
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
        "  ],\n  \"caps\": {{\"safe_uri_bytes\": {SESSION_MAX_SAFE_URI_BYTES}, \"safe_message_bytes\": {SESSION_MAX_SAFE_MESSAGE_BYTES}, \"host_key_bytes\": {SESSION_MAX_HOST_KEY_BYTES}, \"algorithm_bytes\": {SESSION_MAX_ALGORITHM_BYTES}, \"bt_resume_bytes\": {SESSION_MAX_BT_RESUME_BYTES}}},"
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
    output.push_str("],\n  \"rules\": {\"strict_tables\": true, \"u64_may_use_exact_le_blob8\": true, \"tagged_platform_path_matches_journal_codec\": true, \"newer_schema_rejected_before_database_mutation\": true, \"unversioned_nonempty_database_rejected\": true, \"required_limits_verified_exactly\": true, \"wal_failure_falls_back_to_delete\": true, \"queue_reorder_is_one_immediate_dense_transaction\": true, \"secret_options_rejected_before_sql\": true, \"journal_cache_reconciliation_never_changes_queue_authority\": true, \"journal_install_begin_checks_old_pointer\": true, \"journal_install_complete_rechecks_old_pointer\": true, \"installed_pointer_and_phase_change_share_transaction\": true, \"hot_backup_refuses_overwrite_and_runs_integrity_check\": true, \"unix_database_mode\": \"0600\"}\n}\n");
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
    use super::{render_session_contracts, validate_build_contract};
    use std::path::Path;

    #[test]
    fn generated_session_contract_freezes_schema_limits_and_authority_rules() {
        let contract = render_session_contracts();
        assert!(contract.contains("\"schema\": 1"));
        assert!(contract.contains("\"name\": \"journal_install\""));
        assert!(contract.contains("\"code\": \"like_pattern_length\", \"value\": 65536"));
        assert!(contract.contains("SQLITE_MAX_LIKE_PATTERN_LENGTH=65536"));
        assert!(contract.contains("\"journal_install_complete_rechecks_old_pointer\": true"));
        assert!(contract.contains("\"secret_options_rejected_before_sql\": true"));
        assert!(
            contract.contains("\"hot_backup_refuses_overwrite_and_runs_integrity_check\": true")
        );
    }

    #[test]
    fn workspace_manifest_and_bundled_sqlite_flags_match_the_contract() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("xtask is nested under the workspace");
        validate_build_contract(workspace_root).expect("build contract");
    }
}
