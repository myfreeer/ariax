use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use ariax_core::{
    ALL_ARIA2_STATUSES, ALL_DRAIN_TARGETS, ALL_ERROR_KINDS, ALL_EVENT_DISPOSITIONS,
    ALL_NO_SPACE_PROBE_ORIGINS, ALL_OPTION_PATCH_REJECT_REASONS, ALL_SCHEDULER_ACTIONS,
    ALL_SCHEDULER_COMMAND_HANDLINGS, ALL_SCHEDULER_COMMAND_KINDS, ALL_STATE_REASONS,
    ALL_TASK_EVENT_KINDS, ALL_TASK_STATES, ALL_TRANSITION_CONTRACT_KINDS,
    ALL_TRANSITION_REJECTIONS, Aria2Status, SchedulerAction, SchedulerActionSource,
    SchedulerCommandHandling, TaskConditionsSnapshot, TaskState, WireProjection,
    transition_contract,
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
        "{} executable core contracts: {} errors, {} patch rejection reasons, {} task states, {} wire statuses, {} scheduler commands, {} task events, {} semantic actions, {} transition cells",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        ALL_ERROR_KINDS.len(),
        ALL_OPTION_PATCH_REJECT_REASONS.len(),
        ALL_TASK_STATES.len(),
        ALL_ARIA2_STATUSES.len(),
        ALL_SCHEDULER_COMMAND_KINDS.len(),
        ALL_TASK_EVENT_KINDS.len(),
        ALL_SCHEDULER_ACTIONS.len(),
        ALL_TASK_STATES.len() * ALL_SCHEDULER_ACTIONS.len()
    ))
}

fn render_errors() -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n  \"errors\": [\n");
    for (index, kind) in ALL_ERROR_KINDS.iter().enumerate() {
        writeln!(
            output,
            "    {{\"number\": {}, \"code\": {}}}{}",
            kind.number(),
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
    output.push_str("{\n  \"schema\": 2,\n  \"wire_statuses\": [");
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
                ", \"default_status\": null, \"retained_terminal_statuses\": [\"error\", \"complete\", \"removed\"], \"requires_terminal_persistence\": true, \"snapshot_publication\": \"retained_result\"",
            );
            assert_stopped_projections()?;
        } else {
            let projection = WireProjection {
                terminal_persisted: state.is_terminal(),
                ..default
            }
            .project(state)
            .map_err(|error| error.to_string())?;
            write!(
                output,
                ", \"default_status\": {}",
                json_string(projection.as_str())
            )
            .expect("write to string");
            if state.is_terminal() {
                output.push_str(", \"requires_terminal_persistence\": true");
            }
            if state.is_terminal_pending() {
                output.push_str(", \"snapshot_publication\": \"deferred_until_stopped_result\"");
            }
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
        "  ],\n  \"condition_overrides\": [\n    {\"condition\": \"no_space\", \"status\": \"paused\"},\n    {\"condition\": \"needs_credentials\", \"desired_paused\": false, \"status\": \"waiting\"},\n    {\"condition\": \"needs_credentials\", \"desired_paused\": true, \"status\": \"paused\"}\n  ],\n",
    );
    write_code_array(
        &mut output,
        "scheduler_commands",
        ALL_SCHEDULER_COMMAND_KINDS.iter().map(|kind| kind.code()),
    );
    write_code_array(
        &mut output,
        "scheduler_command_handlings",
        ALL_SCHEDULER_COMMAND_HANDLINGS
            .iter()
            .map(|handling| handling.code()),
    );
    write_code_array(
        &mut output,
        "task_events",
        ALL_TASK_EVENT_KINDS.iter().map(|kind| kind.code()),
    );
    write_code_array(
        &mut output,
        "semantic_actions",
        ALL_SCHEDULER_ACTIONS.iter().map(|action| action.code()),
    );
    write_code_array(
        &mut output,
        "no_space_probe_origins",
        ALL_NO_SPACE_PROBE_ORIGINS
            .iter()
            .map(|origin| origin.code()),
    );
    write_code_array(
        &mut output,
        "drain_targets",
        ALL_DRAIN_TARGETS.iter().map(|target| target.code()),
    );
    write_code_array(
        &mut output,
        "event_dispositions",
        ALL_EVENT_DISPOSITIONS
            .iter()
            .map(|disposition| disposition.code()),
    );
    write_code_array(
        &mut output,
        "transition_kinds",
        ALL_TRANSITION_CONTRACT_KINDS.iter().map(|kind| kind.code()),
    );
    write_code_array(
        &mut output,
        "transition_reasons",
        ALL_STATE_REASONS.iter().map(|reason| reason.code()),
    );
    write_code_array(
        &mut output,
        "transition_rejections",
        ALL_TRANSITION_REJECTIONS
            .iter()
            .map(|rejection| rejection.code()),
    );
    write_action_sources(&mut output);
    write_command_action_mappings(&mut output)?;
    write_event_action_mappings(&mut output)?;
    output.push_str("  \"transition_matrix\": [\n");
    for (state_index, state) in ALL_TASK_STATES.iter().copied().enumerate() {
        writeln!(
            output,
            "    {{\"state\": {}, \"cells\": [",
            json_string(state.code())
        )
        .expect("write to string");
        for (action_index, action) in ALL_SCHEDULER_ACTIONS.iter().copied().enumerate() {
            let contract = transition_contract(state, action);
            write!(
                output,
                "      {{\"action\": {}, \"kind\": {}, \"reason\": {}, \"rejection\": ",
                json_string(action.code()),
                json_string(contract.kind().code()),
                json_string(contract.reason().code()),
            )
            .expect("write to string");
            match contract.rejection() {
                Some(rejection) => output.push_str(&json_string(rejection.code())),
                None => output.push_str("null"),
            }
            output.push_str(", \"targets\": [");
            for (target_index, target) in contract.targets().iter().enumerate() {
                if target_index > 0 {
                    output.push_str(", ");
                }
                output.push_str(&json_string(target.code()));
            }
            writeln!(
                output,
                "]}}{}",
                comma(action_index, ALL_SCHEDULER_ACTIONS.len())
            )
            .expect("write to string");
        }
        writeln!(
            output,
            "    ]}}{}",
            comma(state_index, ALL_TASK_STATES.len())
        )
        .expect("write to string");
    }
    output.push_str("  ]\n}\n");

    assert_condition_projection()?;
    Ok(output)
}

fn write_action_sources(output: &mut String) {
    output.push_str("  \"action_sources\": [\n");
    for (index, action) in ALL_SCHEDULER_ACTIONS.iter().copied().enumerate() {
        let source = action.source();
        write!(
            output,
            "    {{\"action\": {}, \"source_kind\": {}, \"input\": ",
            json_string(action.code()),
            json_string(source.kind_code()),
        )
        .expect("write to string");
        match source.input_code() {
            Some(input) => output.push_str(&json_string(input)),
            None => output.push_str("null"),
        }
        writeln!(output, "}}{}", comma(index, ALL_SCHEDULER_ACTIONS.len()))
            .expect("write to string");
    }
    output.push_str("  ],\n");
}

fn write_command_action_mappings(output: &mut String) -> Result<(), String> {
    output.push_str("  \"command_action_mappings\": [\n");
    for (index, command) in ALL_SCHEDULER_COMMAND_KINDS.iter().copied().enumerate() {
        match command.handling() {
            SchedulerCommandHandling::StateMatrix | SchedulerCommandHandling::BatchOperation => {
                write!(
                    output,
                    "    {{\"command\": {}, \"handling\": {}, \"actions\": [",
                    json_string(command.code()),
                    json_string(command.handling().code()),
                )
                .expect("write to string");
                let count = write_matching_actions(output, |action| {
                    action.source() == SchedulerActionSource::Command(command)
                });
                if count == 0 {
                    return Err(format!(
                        "matrix-backed command {} has no semantic action",
                        command.code()
                    ));
                }
                writeln!(
                    output,
                    "]}}{}",
                    comma(index, ALL_SCHEDULER_COMMAND_KINDS.len())
                )
                .expect("write to string");
            }
            SchedulerCommandHandling::QueueOperation => {
                writeln!(
                    output,
                    "    {{\"command\": {}, \"handling\": {}, \"actions\": null}}{}",
                    json_string(command.code()),
                    json_string(command.handling().code()),
                    comma(index, ALL_SCHEDULER_COMMAND_KINDS.len())
                )
                .expect("write to string");
            }
        }
    }
    output.push_str("  ],\n");
    Ok(())
}

fn write_event_action_mappings(output: &mut String) -> Result<(), String> {
    output.push_str("  \"event_action_mappings\": [\n");
    for (index, event) in ALL_TASK_EVENT_KINDS.iter().copied().enumerate() {
        write!(
            output,
            "    {{\"event\": {}, \"fresh_actions\": [",
            json_string(event.code())
        )
        .expect("write to string");
        let count = write_matching_actions(output, |action| {
            action.source() == SchedulerActionSource::TaskEvent(event)
        });
        if count == 0 {
            return Err(format!(
                "task event {} has no fresh semantic action",
                event.code()
            ));
        }
        writeln!(
            output,
            "], \"duplicate_action\": {}, \"stale_action\": {}}}{}",
            json_string(SchedulerAction::DuplicateEventIgnored.code()),
            json_string(SchedulerAction::StaleEventIgnored.code()),
            comma(index, ALL_TASK_EVENT_KINDS.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n");
    Ok(())
}

fn write_matching_actions(
    output: &mut String,
    mut matches: impl FnMut(SchedulerAction) -> bool,
) -> usize {
    let mut first = true;
    let mut count = 0;
    for action in ALL_SCHEDULER_ACTIONS
        .iter()
        .copied()
        .filter(|action| matches(*action))
    {
        if !first {
            output.push_str(", ");
        }
        output.push_str(&json_string(action.code()));
        first = false;
        count += 1;
    }
    count
}

fn write_code_array<'a>(
    output: &mut String,
    name: &str,
    codes: impl ExactSizeIterator<Item = &'a str>,
) {
    write!(output, "  {}: [", json_string(name)).expect("write to string");
    let length = codes.len();
    for (index, code) in codes.enumerate() {
        output.push_str(&json_string(code));
        if index + 1 < length {
            output.push_str(", ");
        }
    }
    output.push_str("],\n");
}

fn assert_stopped_projections() -> Result<(), String> {
    for status in [
        Aria2Status::Error,
        Aria2Status::Complete,
        Aria2Status::Removed,
    ] {
        let actual = WireProjection {
            stopped_status: Some(status),
            terminal_persisted: true,
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
    use ariax_core::{ALL_SCHEDULER_ACTIONS, ALL_TASK_STATES};

    #[test]
    fn generated_core_contracts_contain_closed_vocabularies() {
        let errors = render_errors();
        let states = render_state_wire().expect("state matrix");
        assert!(errors.contains("\"OptionPatchRejected\""));
        assert!(errors.contains("\"requires_explicit_bt_restart\""));
        assert!(states.contains("\"paused_restarting\""));
        assert!(states.contains("\"status_when_slot_retained\": \"active\""));
        assert!(states.contains("\"scheduler_commands\": [\"add_validated_task\""));
        assert!(states.contains("\"action\": \"mid_transfer_no_space\""));
        assert!(states.contains("\"paused_no_space_pause_preserving_probe_succeeded\""));
        assert!(
            states.contains(
                "\"action\": \"pause\", \"source_kind\": \"command\", \"input\": \"pause\""
            )
        );
        assert!(
            states
                .contains("\"action\": \"pause\", \"kind\": \"stay\", \"reason\": \"user_pause\"")
        );
        assert!(states.contains(
            "\"event\": \"retry_ready\", \"fresh_actions\": [\"retry_readmission_succeeded\", \"retry_readmission_blocked\"]"
        ));
        assert!(states.contains(
            "\"command\": \"change_position\", \"handling\": \"queue_operation\", \"actions\": null"
        ));
        assert!(states.contains(
            "\"event\": \"generation_persisted\", \"fresh_actions\": [\"generation_persistence_succeeded\"]"
        ));
        assert!(states.contains(
            "\"event\": \"cancellation_drained\", \"fresh_actions\": [\"cancellation_drain_succeeded\", \"restart_quiesced\"]"
        ));
        assert!(states.contains(
            "\"command\": \"resume\", \"handling\": \"state_matrix\", \"actions\": [\"resume\", \"resume_deferred\", \"explicit_no_space_probe_requested\"]"
        ));
        assert!(states.contains(
            "\"internal\": \"stopped_result\", \"default_status\": null, \"retained_terminal_statuses\": [\"error\", \"complete\", \"removed\"], \"requires_terminal_persistence\": true"
        ));
        assert!(states.contains("\"rejection\": \"host_key_approval_required\""));
        assert!(states.contains("\"event_dispositions\": [\"fresh\", \"duplicate\", \"stale\"]"));
        assert!(
            states
                .contains("\"no_space_probe_origins\": [\"explicit_resume\", \"automatic_retry\"]")
        );
        assert_eq!(
            states.matches("      {\"action\":").count(),
            ALL_TASK_STATES.len() * ALL_SCHEDULER_ACTIONS.len()
        );
        assert!(!states.contains("\"actions\": []"));
        assert!(!states.contains("\"fresh_actions\": []"));
        assert!(!states.contains("\"status\": \"retry_wait\""));
    }
}
