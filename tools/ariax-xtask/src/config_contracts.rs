use crate::inventory::{GenerationMode, apply_outputs, comma, json_string};
use crate::{ARIA2_REPOSITORY, Aria2Reference};
use ariax_config::{
    ALL_COMPAT_STATUSES, ALL_COMPATIBILITY_DIFFERENCES, ALL_RUNTIME_UPDATES, ALL_SCOPES,
    ALL_SECURITY_CLASSES, CompatibilityDifference, OptionDef, ValueType, builtin_registry,
};
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const OPTIONS_OUTPUT: &str = "generated/options.json";
const ARIA2_COMPAT_OUTPUT: &str = "generated/aria2_compat.json";
const RUNTIME_UPDATES_OUTPUT: &str = "generated/runtime_updates.json";
const RUNTIME_COMPATIBILITY_OUTPUT: &str = "generated/runtime_compatibility.json";

pub(crate) fn generate_config_contracts(
    workspace_root: &Path,
    reference: &Aria2Reference,
    upstream_options: &BTreeSet<String>,
    mode: GenerationMode,
) -> Result<String, String> {
    let registry = builtin_registry();
    let mut definitions = registry.definitions().iter().collect::<Vec<_>>();
    definitions.sort_unstable_by_key(|definition| definition.name);

    let outputs = [
        (PathBuf::from(OPTIONS_OUTPUT), render_options(&definitions)),
        (
            PathBuf::from(ARIA2_COMPAT_OUTPUT),
            render_aria2_compat(reference, upstream_options, &definitions),
        ),
        (
            PathBuf::from(RUNTIME_UPDATES_OUTPUT),
            render_runtime_updates(&definitions),
        ),
        (
            PathBuf::from(RUNTIME_COMPATIBILITY_OUTPUT),
            render_runtime_compatibility(reference, &definitions),
        ),
    ];
    apply_outputs(workspace_root, &outputs, mode)?;

    let reviewed_upstream = definitions
        .iter()
        .filter(|definition| upstream_options.contains(definition.name))
        .count();
    Ok(format!(
        "{} config contracts: {} reviewed options ({} of {} pinned aria2 handlers)",
        match mode {
            GenerationMode::Write => "generated",
            GenerationMode::Check => "verified",
        },
        definitions.len(),
        reviewed_upstream,
        upstream_options.len()
    ))
}

fn render_options(definitions: &[&OptionDef]) -> String {
    let mut output = String::new();
    writeln!(
        output,
        "{{\n  \"schema\": 1,\n  \"count\": {},",
        definitions.len()
    )
    .expect("write to string");
    write_code_array(
        &mut output,
        "compatibility_statuses",
        ALL_COMPAT_STATUSES.map(|value| value.code()).as_slice(),
        true,
    );
    write_code_array(
        &mut output,
        "security_classes",
        ALL_SECURITY_CLASSES.map(|value| value.code()).as_slice(),
        true,
    );
    write_code_array(
        &mut output,
        "scopes",
        ALL_SCOPES.map(|value| value.code()).as_slice(),
        true,
    );
    output.push_str("  \"options\": [\n");
    for (index, definition) in definitions.iter().enumerate() {
        write!(
            output,
            "    {{\"name\": {}, \"short\": ",
            json_string(definition.name)
        )
        .expect("write to string");
        write_optional_char(&mut output, definition.short);
        output.push_str(", \"value_type\": ");
        write_value_type(&mut output, definition.value_type);
        output.push_str(", \"default\": ");
        write_optional_str(&mut output, definition.default);
        write!(
            output,
            ", \"category\": {}, \"scopes\": ",
            json_string(definition.category)
        )
        .expect("write to string");
        let scopes = definition
            .scopes
            .iter()
            .map(|scope| scope.code())
            .collect::<Vec<_>>();
        write_str_array(&mut output, &scopes);
        write!(
            output,
            ", \"runtime_update\": {}, \"owner\": {}, \"build_features\": ",
            json_string(definition.runtime_update.code()),
            json_string(definition.owner)
        )
        .expect("write to string");
        write_str_array(&mut output, definition.build_features);
        write!(
            output,
            ", \"security\": {}, \"compatibility_status\": {}, \"aria2_available\": {}, \"aria2_runtime_update\": {}, \"compatibility_difference\": {}, \"docs\": {}, \"behavior_tests\": ",
            json_string(definition.security.code()),
            json_string(definition.compat.code()),
            definition.aria2_available,
            json_string(definition.aria2_runtime_update.code()),
            json_string(definition.compatibility_difference.code()),
            json_string(definition.docs)
        )
        .expect("write to string");
        write_str_array(&mut output, definition.behavior_tests);
        writeln!(output, "}}{}", comma(index, definitions.len())).expect("write to string");
    }
    output.push_str("  ]\n}\n");
    output
}

fn render_aria2_compat(
    reference: &Aria2Reference,
    upstream_options: &BTreeSet<String>,
    definitions: &[&OptionDef],
) -> String {
    let registered = definitions
        .iter()
        .map(|definition| definition.name)
        .collect::<BTreeSet<_>>();
    let upstream_without_registry = upstream_options
        .iter()
        .filter(|name| !registered.contains(name.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    let claimed_but_missing = definitions
        .iter()
        .filter(|definition| {
            definition.aria2_available && !upstream_options.contains(definition.name)
        })
        .map(|definition| definition.name)
        .collect::<Vec<_>>();
    let reviewed_upstream = upstream_options.len() - upstream_without_registry.len();
    let extensions = definitions
        .iter()
        .filter(|definition| !definition.aria2_available)
        .count();

    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n");
    write_source(&mut output, reference);
    writeln!(
        output,
        "  \"counts\": {{\"pinned_aria2_handlers\": {}, \"reviewed_registry_entries\": {}, \"reviewed_upstream_entries\": {}, \"extensions\": {}, \"upstream_without_registry\": {}, \"registry_claims_missing_upstream\": {}}},",
        upstream_options.len(),
        definitions.len(),
        reviewed_upstream,
        extensions,
        upstream_without_registry.len(),
        claimed_but_missing.len()
    )
    .expect("write to string");
    output.push_str("  \"reviewed\": [\n");
    for (index, definition) in definitions.iter().enumerate() {
        writeln!(
            output,
            "    {{\"name\": {}, \"status\": {}, \"aria2_available\": {}, \"security\": {}, \"features\": {}, \"docs\": {}}}{}",
            json_string(definition.name),
            json_string(definition.compat.code()),
            definition.aria2_available,
            json_string(definition.security.code()),
            render_str_array(definition.build_features),
            json_string(definition.docs),
            comma(index, definitions.len())
        )
        .expect("write to string");
    }
    output.push_str("  ],\n  \"coverage\": {\n    \"upstream_without_registry\": ");
    write_str_array(&mut output, &upstream_without_registry);
    output.push_str(",\n    \"registry_claims_missing_upstream\": ");
    write_str_array(&mut output, &claimed_but_missing);
    output.push_str("\n  }\n}\n");
    output
}

fn render_runtime_updates(definitions: &[&OptionDef]) -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n");
    write_code_array(
        &mut output,
        "allowed_runtime_updates",
        ALL_RUNTIME_UPDATES.map(|value| value.code()).as_slice(),
        true,
    );
    output.push_str("  \"options\": [\n");
    for (index, definition) in definitions.iter().enumerate() {
        writeln!(
            output,
            "    {{\"name\": {}, \"runtime_update\": {}, \"scopes\": {}}}{}",
            json_string(definition.name),
            json_string(definition.runtime_update.code()),
            render_str_array(
                &definition
                    .scopes
                    .iter()
                    .map(|scope| scope.code())
                    .collect::<Vec<_>>()
            ),
            comma(index, definitions.len())
        )
        .expect("write to string");
    }
    output.push_str("  ]\n}\n");
    output
}

fn render_runtime_compatibility(reference: &Aria2Reference, definitions: &[&OptionDef]) -> String {
    let unresolved = definitions
        .iter()
        .filter(|definition| {
            definition.compatibility_difference == CompatibilityDifference::Unresolved
        })
        .count();
    let mut output = String::new();
    output.push_str("{\n  \"schema\": 1,\n");
    write_source(&mut output, reference);
    write_code_array(
        &mut output,
        "difference_classes",
        ALL_COMPATIBILITY_DIFFERENCES
            .map(|value| value.code())
            .as_slice(),
        true,
    );
    writeln!(
        output,
        "  \"counts\": {{\"options\": {}, \"unresolved\": {}}},",
        definitions.len(),
        unresolved
    )
    .expect("write to string");
    output.push_str("  \"options\": [\n");
    for (index, definition) in definitions.iter().enumerate() {
        writeln!(
            output,
            "    {{\"name\": {}, \"aria2_available\": {}, \"aria2_runtime_update\": {}, \"ariax_runtime_update\": {}, \"difference\": {}}}{}",
            json_string(definition.name),
            definition.aria2_available,
            json_string(definition.aria2_runtime_update.code()),
            json_string(definition.runtime_update.code()),
            json_string(definition.compatibility_difference.code()),
            comma(index, definitions.len())
        )
        .expect("write to string");
    }
    output.push_str("  ]\n}\n");
    output
}

fn write_value_type(output: &mut String, value_type: ValueType) {
    write!(output, "{{\"kind\": {}", json_string(value_type.code())).expect("write to string");
    match value_type {
        ValueType::Bool => {}
        ValueType::Integer { min, max } => {
            write!(
                output,
                ", \"minimum\": {}, \"maximum\": {}",
                json_string(&min.to_string()),
                json_string(&max.to_string())
            )
            .expect("write to string");
        }
        ValueType::SizeBytes { min, max } | ValueType::DurationSeconds { min, max } => {
            write!(
                output,
                ", \"minimum\": {}, \"maximum\": {}",
                json_string(&min.to_string()),
                json_string(&max.to_string())
            )
            .expect("write to string");
        }
        ValueType::Enum { values } => {
            output.push_str(", \"values\": ");
            write_str_array(output, values);
        }
        ValueType::String { max_len } | ValueType::SecretString { max_len } => {
            write!(output, ", \"max_bytes\": {max_len}").expect("write to string");
        }
        ValueType::Path {
            max_len,
            expand_home,
        } => {
            write!(
                output,
                ", \"max_bytes\": {max_len}, \"expand_home\": {expand_home}"
            )
            .expect("write to string");
        }
        ValueType::HeaderList {
            max_items,
            max_item_len,
        } => {
            write!(
                output,
                ", \"max_items\": {max_items}, \"max_item_bytes\": {max_item_len}"
            )
            .expect("write to string");
        }
        ValueType::StatusCodeSet { max_items } => {
            write!(output, ", \"max_items\": {max_items}").expect("write to string");
        }
    }
    output.push('}');
}

fn write_source(output: &mut String, reference: &Aria2Reference) {
    writeln!(
        output,
        "  \"source\": {{\"repository\": {}, \"commit\": {}}},",
        json_string(ARIA2_REPOSITORY),
        json_string(&reference.commit)
    )
    .expect("write to string");
}

fn write_optional_char(output: &mut String, value: Option<char>) {
    match value {
        Some(value) => output.push_str(&json_string(&value.to_string())),
        None => output.push_str("null"),
    }
}

fn write_optional_str(output: &mut String, value: Option<&str>) {
    match value {
        Some(value) => output.push_str(&json_string(value)),
        None => output.push_str("null"),
    }
}

fn write_code_array(output: &mut String, name: &str, values: &[&str], trailing_comma: bool) {
    write!(output, "  {}: ", json_string(name)).expect("write to string");
    write_str_array(output, values);
    output.push_str(if trailing_comma { ",\n" } else { "\n" });
}

fn write_str_array(output: &mut String, values: &[&str]) {
    output.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(value));
    }
    output.push(']');
}

fn render_str_array(values: &[&str]) -> String {
    let mut output = String::new();
    write_str_array(&mut output, values);
    output
}

#[cfg(test)]
mod tests {
    use super::{render_aria2_compat, render_options, render_runtime_compatibility};
    use crate::{ARIA2_REPOSITORY, Aria2Reference};
    use ariax_config::builtin_registry;
    use std::collections::BTreeSet;

    fn definitions() -> Vec<&'static ariax_config::OptionDef> {
        let mut definitions = builtin_registry().definitions().iter().collect::<Vec<_>>();
        definitions.sort_unstable_by_key(|definition| definition.name);
        definitions
    }

    fn reference() -> Aria2Reference {
        Aria2Reference {
            schema: 1,
            repository: ARIA2_REPOSITORY.to_owned(),
            commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        }
    }

    #[test]
    fn option_contract_is_sorted_typed_and_redaction_safe() {
        let options = render_options(&definitions());
        assert!(options.contains("\"kind\": \"secret_string\""));
        assert!(options.contains("\"name\": \"retry-on-http-status\""));
        assert!(!options.contains("correct horse"));
        assert!(options.find("\"allow-overwrite\"") < options.find("\"continue\""));
    }

    #[test]
    fn aria2_coverage_keeps_unreviewed_and_mismatched_claims_visible() {
        let upstream = BTreeSet::from(["dir".to_owned(), "upstream-only".to_owned()]);
        let compatibility = render_aria2_compat(&reference(), &upstream, &definitions());
        assert!(compatibility.contains("\"upstream_without_registry\": [\"upstream-only\"]"));
        assert!(compatibility.contains("\"registry_claims_missing_upstream\""));
        assert!(compatibility.contains("\"split\""));
    }

    #[test]
    fn runtime_compatibility_has_no_hidden_unresolved_entries() {
        let compatibility = render_runtime_compatibility(&reference(), &definitions());
        assert!(compatibility.contains("\"unresolved\": 0"));
        assert!(compatibility.contains("\"ariax_runtime_update\": \"active_restart\""));
    }
}
