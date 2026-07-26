use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_core::{
    ALL_ARIA2_STATUSES, ALL_ERROR_KINDS, ALL_OPTION_PATCH_REJECT_REASONS, ALL_TASK_STATES,
    Aria2Status, TaskConditionsSnapshot, TaskState, WireProjection,
};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const ERROR_OUTPUT: &str = "generated/error_codes.json";
const STATE_OUTPUT: &str = "generated/state_wire.json";

pub(crate) fn generate_core_contracts(
    workspace_root: &Path,
    mode: GenerationMode,
) -> Result<String, String> {
    let outputs = [
        (PathBuf::from(ERROR_OUTPUT), render_errors()),
        (PathBuf::from(STATE_OUTPUT), render_state_wire()?),
    ];
    apply_outputs(workspace_root, &outputs, mode)?;
    Ok(format!(
        "{} executable core contracts: {} errors, {} patch rejection reasons, {} task states, {} wire statuses",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        ALL_ERROR_KINDS.len(),
        ALL_OPTION_PATCH_REJECT_REASONS.len(),
        ALL_TASK_STATES.len(),
        ALL_ARIA2_STATUSES.len()
    ))
}

fn render_errors() -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n  \"errors\": [\n");
    for (index, kind) in ALL_ERROR_KINDS.iter().enumerate() {
        writeln!(
            output,
            "    {{\"code\": {}}}{}",
            json_string(kind.code()),
            comma(index, ALL_ERROR_KINDS.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"option_patch_rejection_reasons\": [\n");
    for (index, reason) in ALL_OPTION_PATCH_REJECT_REASONS.iter().enumerate() {
        writeln!(
            output,
            "    {{\"code\": {}}}{}",
            json_string(reason.code()),
            comma(index, ALL_OPTION_PATCH_REJECT_REASONS.len())
        )
        .expect("write to string");
    }
    output.push_str("  ]\n}\n");
    output
}

fn render_state_wire() -> Result<String, String> {
    let default = WireProjection::default();
    let retry_holds_slot = WireProjection {
        retry_wait_holds_slot: true,
        ..WireProjection::default()
    };
    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n  \"wire_statuses\": [");
    for (index, status) in ALL_ARIA2_STATUSES.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(status.as_str()));
    }
    output.push_str("],\n  \"states\": [\n");
    for (index, state) in ALL_TASK_STATES.iter().copied().enumerate() {
        write!(output, "    {{\"internal\": {}", json_string(state.code()))
            .expect("write to string");
        if state == TaskState::StoppedResult {
            output.push_str(
                ", \"default_status\": null, \"retained_terminal_statuses\": [\"error\", \"complete\", \"removed\"]",
            );
            assert_stopped_projections()?;
        } else {
            let projection = default.project(state).map_err(|error| error.to_string())?;
            write!(
                output,
                ", \"default_status\": {}",
                json_string(projection.as_str())
            )
            .expect("write to string");
        }
        if state == TaskState::RetryWait {
            let active = retry_holds_slot
                .project(state)
                .map_err(|error| error.to_string())?;
            write!(
                output,
                ", \"status_when_slot_retained\": {}",
                json_string(active.as_str())
            )
            .expect("write to string");
        }
        writeln!(output, "}}{}", comma(index, ALL_TASK_STATES.len())).expect("write to string");
    }
    output.push_str(
        "  ],\n  \"condition_overrides\": [\n    {\"condition\": \"no_space\", \"status\": \"paused\"},\n    {\"condition\": \"needs_credentials\", \"desired_paused\": false, \"status\": \"waiting\"},\n    {\"condition\": \"needs_credentials\", \"desired_paused\": true, \"status\": \"paused\"}\n  ]\n}\n",
    );

    assert_condition_projection()?;
    Ok(output)
}

fn assert_stopped_projections() -> Result<(), String> {
    for status in [
        Aria2Status::Error,
        Aria2Status::Complete,
        Aria2Status::Removed,
    ] {
        let actual = WireProjection {
            stopped_status: Some(status),
            ..WireProjection::default()
        }
        .project(TaskState::StoppedResult)
        .map_err(|error| error.to_string())?;
        if actual != status {
            return Err(format!(
                "stopped-result projection drifted: expected {status}, got {actual}"
            ));
        }
    }
    Ok(())
}

fn assert_condition_projection() -> Result<(), String> {
    let cases = [
        (
            WireProjection {
                conditions: TaskConditionsSnapshot {
                    needs_credentials: false,
                    no_space: true,
                },
                ..WireProjection::default()
            },
            Aria2Status::Paused,
        ),
        (
            WireProjection {
                conditions: TaskConditionsSnapshot {
                    needs_credentials: true,
                    no_space: false,
                },
                ..WireProjection::default()
            },
            Aria2Status::Waiting,
        ),
        (
            WireProjection {
                conditions: TaskConditionsSnapshot {
                    needs_credentials: true,
                    no_space: false,
                },
                desired_paused: true,
                ..WireProjection::default()
            },
            Aria2Status::Paused,
        ),
    ];
    for (projection, expected) in cases {
        let actual = projection
            .project(TaskState::Waiting)
            .map_err(|error| error.to_string())?;
        if actual != expected {
            return Err(format!(
                "condition projection drifted: expected {expected}, got {actual}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{render_errors, render_state_wire};

    #[test]
    fn generated_core_contracts_contain_closed_vocabularies() {
        let errors = render_errors();
        let states = render_state_wire().expect("state matrix");
        assert!(errors.contains("\"OptionPatchRejected\""));
        assert!(errors.contains("\"requires_explicit_bt_restart\""));
        assert!(states.contains("\"paused_restarting\""));
        assert!(states.contains("\"status_when_slot_retained\": \"active\""));
        assert!(!states.contains("\"status\": \"retry_wait\""));
    }
}
