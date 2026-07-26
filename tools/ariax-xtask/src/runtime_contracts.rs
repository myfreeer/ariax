use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_runtime::{ALL_BUFFER_STATES, ALL_OWNER_TAGS, SizeClass};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const RUNTIME_OUTPUT: &str = "generated/runtime_buffers.json";

pub(crate) fn generate_runtime_contracts(
    workspace_root: &Path,
    mode: GenerationMode,
) -> Result<String, String> {
    let outputs = [(PathBuf::from(RUNTIME_OUTPUT), render_runtime_contracts())];
    apply_outputs(workspace_root, &outputs, mode)?;
    let transitions = ALL_BUFFER_STATES
        .iter()
        .flat_map(|from| {
            ALL_BUFFER_STATES
                .iter()
                .filter(move |to| from.can_transition_to(**to))
        })
        .count();
    Ok(format!(
        "{} runtime buffer contracts: {} states, {} transitions, {} size classes",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        ALL_BUFFER_STATES.len(),
        transitions,
        SizeClass::ALL.len()
    ))
}

fn render_runtime_contracts() -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n  \"size_classes\": [\n");
    for (index, class) in SizeClass::ALL.iter().copied().enumerate() {
        writeln!(
            output,
            "    {{\"code\": {}, \"capacity\": {}}}{}",
            json_string(class.code()),
            class.capacity(),
            comma(index, SizeClass::ALL.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"owners\": [");
    for (index, owner) in ALL_OWNER_TAGS.iter().copied().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(owner.code()));
    }
    output.push_str("],\n  \"buffer_states\": [\n");
    for (index, state) in ALL_BUFFER_STATES.iter().copied().enumerate() {
        write!(
            output,
            "    {{\"code\": {}, \"mutable\": {}, \"readable\": {}, \"transitions_to\": [",
            json_string(state.code()),
            state.is_mutable(),
            state.is_readable()
        )
        .expect("write to string");
        let targets = ALL_BUFFER_STATES
            .iter()
            .copied()
            .filter(|target| state.can_transition_to(*target))
            .collect::<Vec<_>>();
        for (target_index, target) in targets.iter().copied().enumerate() {
            if target_index > 0 {
                output.push_str(", ");
            }
            output.push_str(&json_string(target.code()));
        }
        writeln!(output, "]}}{}", comma(index, ALL_BUFFER_STATES.len())).expect("write to string");
    }
    output.push_str(
        "  ],\n  \"budget_contract\": {\"domain_and_resident_permits_required\": true, \"zero_byte_reservation_allowed\": false},\n  \"pool_contract\": {\"free_list\": \"lifo_per_size_class\", \"stable_capacity\": true, \"drop_destination\": \"quarantine\", \"quarantine_exhaustion\": \"fault_backend\", \"timeout_reclamation\": \"retire_without_releasing_pool_budget\"},\n  \"queue_contract\": {\"item_bounded\": true, \"byte_bounded\": true, \"credit_before_read\": true, \"close_returns_queued_ownership\": true},\n  \"completion_contract\": {\"reserve_before_backend_acceptance\": true, \"send_after_admission_close\": true, \"capacity_failure_after_acceptance\": false}\n}\n",
    );
    output
}

#[cfg(test)]
mod tests {
    use super::render_runtime_contracts;

    #[test]
    fn generated_runtime_contract_closes_ownership_and_backpressure_rules() {
        let contract = render_runtime_contracts();
        assert!(contract.contains("\"disk_in_flight\""));
        assert!(contract.contains("\"transitions_to\": [\"disk_done\"]"));
        assert!(contract.contains("\"drop_destination\": \"quarantine\""));
        assert!(contract.contains("\"capacity_failure_after_acceptance\": false"));
        assert!(!contract.contains("\"free\", \"mutable\": true"));
    }
}
