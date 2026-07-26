use super::{ARIA2_REPOSITORY, Aria2Reference, git_blob};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

const OPTIONS_OUTPUT: &str = "generated/aria2_options.json";
const RPC_OUTPUT: &str = "generated/aria2_rpc.json";
const PREFS_PATH: &str = "src/prefs.cc";
const OPTION_FACTORY_PATH: &str = "src/OptionHandlerFactory.cc";
const MANUAL_PATH: &str = "doc/manual-src/en/aria2c.rst";
const RPC_FACTORY_PATH: &str = "src/RpcMethodFactory.cc";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenerationMode {
    Write,
    Check,
}

pub(crate) fn generate_aria2_inventory(
    workspace_root: &Path,
    source_dir: &Path,
    reference: &Aria2Reference,
    mode: GenerationMode,
) -> Result<(String, BTreeSet<String>), String> {
    let preferences = parse_preferences(&git_blob(source_dir, &reference.commit, PREFS_PATH)?)?;
    let handlers = parse_option_handlers(
        &git_blob(source_dir, &reference.commit, OPTION_FACTORY_PATH)?,
        &preferences,
    )?;
    let manual = parse_manual_options(&git_blob(source_dir, &reference.commit, MANUAL_PATH)?)?;
    let rpc_source = git_blob(source_dir, &reference.commit, RPC_FACTORY_PATH)?;
    let methods = parse_rpc_vector(&rpc_source, "rpcMethodNames")?;
    let notifications = parse_rpc_vector(&rpc_source, "rpcNotificationsNames")?;

    let inventory = Aria2Inventory {
        preferences,
        handlers,
        manual,
        methods,
        notifications,
    };
    inventory.validate()?;

    let outputs = [
        (
            PathBuf::from(OPTIONS_OUTPUT),
            inventory.render_options(reference),
        ),
        (PathBuf::from(RPC_OUTPUT), inventory.render_rpc(reference)),
    ];
    apply_outputs(workspace_root, &outputs, mode)?;

    let action = match mode {
        GenerationMode::Write => "generated",
        GenerationMode::Check => "verified",
    };
    let option_names = inventory
        .handlers
        .iter()
        .map(|handler| handler.name.clone())
        .collect();
    Ok((
        format!(
            "{action} aria2 inventories: {} preferences, {} handlers, {} manual directives, {} RPC methods, {} notifications",
            inventory.preferences.len(),
            inventory.handlers.len(),
            inventory.manual.len(),
            inventory.methods.len(),
            inventory.notifications.len()
        ),
        option_names,
    ))
}

#[derive(Debug)]
struct Aria2Inventory {
    preferences: Vec<Preference>,
    handlers: Vec<OptionHandler>,
    manual: Vec<ManualOption>,
    methods: Vec<RpcEntry>,
    notifications: Vec<RpcEntry>,
}

impl Aria2Inventory {
    fn validate(&self) -> Result<(), String> {
        require_unique(
            self.preferences.iter().map(|item| item.symbol.as_str()),
            "preference symbol",
        )?;
        require_unique(
            self.preferences.iter().map(|item| item.name.as_str()),
            "preference name",
        )?;
        require_unique(
            self.handlers.iter().map(|item| item.name.as_str()),
            "option handler",
        )?;
        require_unique(
            self.methods.iter().map(|item| item.name.as_str()),
            "RPC method",
        )?;
        require_unique(
            self.notifications.iter().map(|item| item.name.as_str()),
            "RPC notification",
        )?;
        if self.preferences.is_empty()
            || self.handlers.is_empty()
            || self.manual.is_empty()
            || self.methods.is_empty()
            || self.notifications.is_empty()
        {
            return Err("aria2 inventory contains an unexpectedly empty section".to_owned());
        }
        Ok(())
    }

    fn render_options(&self, reference: &Aria2Reference) -> String {
        let handled: BTreeSet<&str> = self
            .handlers
            .iter()
            .map(|handler| handler.name.as_str())
            .collect();
        let documented: BTreeSet<&str> = self
            .manual
            .iter()
            .flat_map(|entry| entry.names.iter().map(String::as_str))
            .collect();
        let unhandled: Vec<&Preference> = self
            .preferences
            .iter()
            .filter(|preference| !handled.contains(preference.name.as_str()))
            .collect();
        let handlers_missing_manual: Vec<&str> = handled.difference(&documented).copied().collect();
        let manual_missing_handlers: Vec<&str> = documented.difference(&handled).copied().collect();

        let mut output = String::new();
        output.push_str("{\n  \"schema\": 1,\n");
        write_source(&mut output, reference);
        writeln!(
            output,
            "  \"counts\": {{\"preferences\": {}, \"handlers\": {}, \"manual_directives\": {}}},",
            self.preferences.len(),
            self.handlers.len(),
            self.manual.len()
        )
        .expect("write to string");

        output.push_str("  \"preferences\": [\n");
        for (index, preference) in self.preferences.iter().enumerate() {
            writeln!(
                output,
                "    {{\"symbol\": {}, \"name\": {}, \"source_line\": {}}}{}",
                json_string(&preference.symbol),
                json_string(&preference.name),
                preference.source_line,
                comma(index, self.preferences.len())
            )
            .expect("write to string");
        }
        output.push_str("  ],\n  \"handlers\": [\n");
        for (index, handler) in self.handlers.iter().enumerate() {
            write!(
                output,
                "    {{\"name\": {}, \"preference\": {}, \"handler\": {}, \"source_line\": {}, ",
                json_string(&handler.name),
                json_string(&handler.preference),
                json_string(&handler.handler),
                handler.source_line
            )
            .expect("write to string");
            write!(output, "\"compile_conditions\": ").expect("write to string");
            write_string_array(&mut output, &handler.compile_conditions);
            write!(output, ", \"source_directives\": ").expect("write to string");
            write_string_array(&mut output, &handler.source_directives);
            write!(output, ", \"tags\": ").expect("write to string");
            write_string_array(&mut output, &handler.tags);
            writeln!(
                output,
                ", \"initial\": {}, \"change_global\": {}, \"change_reserved\": {}, \"change_active\": {}, \"hidden\": {}, \"erase_after_parse\": {}, \"source_expression\": {}}}{}",
                handler.initial,
                handler.change_global,
                handler.change_reserved,
                handler.change_active,
                handler.hidden,
                handler.erase_after_parse,
                json_string(&handler.source_expression),
                comma(index, self.handlers.len())
            )
            .expect("write to string");
        }
        output.push_str("  ],\n  \"manual\": [\n");
        for (index, manual) in self.manual.iter().enumerate() {
            write!(
                output,
                "    {{\"source_line\": {}, \"directive\": {}, \"names\": ",
                manual.source_line,
                json_string(&manual.directive)
            )
            .expect("write to string");
            write_string_array(&mut output, &manual.names);
            writeln!(output, "}}{}", comma(index, self.manual.len())).expect("write to string");
        }
        output.push_str("  ],\n  \"differences\": {\n    \"preferences_without_handlers\": [\n");
        for (index, preference) in unhandled.iter().enumerate() {
            writeln!(
                output,
                "      {{\"symbol\": {}, \"name\": {}}}{}",
                json_string(&preference.symbol),
                json_string(&preference.name),
                comma(index, unhandled.len())
            )
            .expect("write to string");
        }
        output.push_str("    ],\n    \"handlers_without_manual_entries\": ");
        write_str_array(&mut output, &handlers_missing_manual);
        output.push_str(",\n    \"manual_entries_without_handlers\": ");
        write_str_array(&mut output, &manual_missing_handlers);
        output.push_str("\n  }\n}\n");
        output
    }

    fn render_rpc(&self, reference: &Aria2Reference) -> String {
        let mut output = String::new();
        output.push_str("{\n  \"schema\": 1,\n");
        write_source(&mut output, reference);
        writeln!(
            output,
            "  \"counts\": {{\"methods\": {}, \"notifications\": {}}},",
            self.methods.len(),
            self.notifications.len()
        )
        .expect("write to string");
        write_rpc_entries(&mut output, "methods", &self.methods, true);
        write_rpc_entries(&mut output, "notifications", &self.notifications, false);
        output.push_str("}\n");
        output
    }
}

#[derive(Debug)]
struct Preference {
    symbol: String,
    name: String,
    source_line: usize,
}

#[derive(Debug)]
struct OptionHandler {
    name: String,
    preference: String,
    handler: String,
    source_line: usize,
    compile_conditions: Vec<String>,
    source_directives: Vec<String>,
    tags: Vec<String>,
    initial: bool,
    change_global: bool,
    change_reserved: bool,
    change_active: bool,
    hidden: bool,
    erase_after_parse: bool,
    source_expression: String,
}

#[derive(Debug)]
struct ManualOption {
    directive: String,
    names: Vec<String>,
    source_line: usize,
}

#[derive(Debug)]
struct RpcEntry {
    name: String,
    compile_conditions: Vec<String>,
    source_line: usize,
}

fn parse_preferences(source: &str) -> Result<Vec<Preference>, String> {
    let mut preferences = Vec::new();
    let mut statement = String::new();
    let mut source_line = 0;

    for (index, line) in source.lines().enumerate() {
        if statement.is_empty() {
            if !line.trim_start().starts_with("PrefPtr PREF_") {
                continue;
            }
            source_line = index + 1;
        }
        if !statement.is_empty() {
            statement.push(' ');
        }
        statement.push_str(line.trim());
        if !line.contains(';') {
            continue;
        }

        let symbol_start = statement
            .find("PREF_")
            .ok_or_else(|| format!("preference at line {source_line} has no symbol"))?;
        let symbol = take_identifier(&statement[symbol_start..]);
        let make_pref = statement
            .find("makePref(")
            .ok_or_else(|| format!("preference {symbol} does not call makePref"))?;
        let name = first_quoted(&statement[make_pref..])
            .ok_or_else(|| format!("preference {symbol} has no quoted name"))?;
        preferences.push(Preference {
            symbol: symbol.to_owned(),
            name,
            source_line,
        });
        statement.clear();
    }
    if !statement.is_empty() {
        return Err(format!(
            "unterminated preference declaration at line {source_line}"
        ));
    }
    Ok(preferences)
}

fn parse_option_handlers(
    source: &str,
    preferences: &[Preference],
) -> Result<Vec<OptionHandler>, String> {
    let names: BTreeMap<&str, &str> = preferences
        .iter()
        .map(|preference| (preference.symbol.as_str(), preference.name.as_str()))
        .collect();
    let mut conditions = ConditionStack::default();
    let mut handlers = Vec::new();
    let mut block = Vec::new();
    let mut block_line = 0;
    let mut block_conditions = Vec::new();

    for (index, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        let starts_handler =
            line.contains("OptionHandler* op") || trimmed.starts_with("auto op = new ");
        if block.is_empty() && starts_handler {
            block_line = index + 1;
            block_conditions = conditions.current().to_vec();
        }
        if !block.is_empty() || starts_handler {
            block.push(line.to_owned());
        }

        if trimmed.starts_with('#') {
            conditions.apply(trimmed, index + 1)?;
        }

        if line.contains("handlers.push_back(op);") {
            if block.is_empty() {
                return Err(format!(
                    "handler terminator at line {} has no declaration",
                    index + 1
                ));
            }
            handlers.push(parse_handler_block(
                &block,
                block_line,
                &block_conditions,
                &names,
            )?);
            block.clear();
        }
    }
    if !block.is_empty() {
        return Err(format!("unterminated option handler at line {block_line}"));
    }
    conditions.finish()?;

    let expected = source.matches("handlers.push_back(op);").count();
    if handlers.len() != expected {
        return Err(format!(
            "parsed {} option handlers but source contains {expected} terminators",
            handlers.len()
        ));
    }
    Ok(handlers)
}

fn parse_handler_block(
    lines: &[String],
    source_line: usize,
    conditions: &[String],
    preference_names: &BTreeMap<&str, &str>,
) -> Result<OptionHandler, String> {
    let source_expression = lines
        .iter()
        .flat_map(|line| line.split_whitespace())
        .collect::<Vec<_>>()
        .join(" ");
    let preference_start = source_expression
        .find("PREF_")
        .ok_or_else(|| format!("option handler at line {source_line} has no preference"))?;
    let preference = take_identifier(&source_expression[preference_start..]).to_owned();
    let name = preference_names
        .get(preference.as_str())
        .ok_or_else(|| format!("unknown preference {preference} at line {source_line}"))?
        .to_string();
    let handler = source_expression[..preference_start]
        .rfind("new ")
        .and_then(|start| {
            let candidate = &source_expression[start + 4..];
            candidate.find('(').map(|end| candidate[..end].to_owned())
        })
        .ok_or_else(|| format!("option {name} has no handler type"))?;
    let tags = extract_call_arguments(&source_expression, "op->addTag(");
    let source_directives = lines
        .iter()
        .map(|line| line.trim())
        .filter(|line| line.starts_with('#'))
        .map(str::to_owned)
        .collect();

    Ok(OptionHandler {
        name,
        preference,
        handler,
        source_line,
        compile_conditions: conditions.to_vec(),
        source_directives,
        tags,
        initial: source_expression.contains("op->setInitialOption(true)"),
        change_global: source_expression.contains("op->setChangeGlobalOption(true)"),
        change_reserved: source_expression.contains("op->setChangeOptionForReserved(true)"),
        change_active: source_expression.contains("op->setChangeOption(true)"),
        hidden: source_expression.contains("op->hide()"),
        erase_after_parse: source_expression.contains("op->setEraseAfterParse(true)"),
        source_expression,
    })
}

fn parse_manual_options(source: &str) -> Result<Vec<ManualOption>, String> {
    let mut options = Vec::new();
    for (index, line) in source.lines().enumerate() {
        let Some(directive) = line.trim_start().strip_prefix(".. option::") else {
            continue;
        };
        let directive = directive.trim().to_owned();
        let names = extract_long_options(&directive);
        if names.is_empty() {
            return Err(format!(
                "manual option directive at line {} has no long option",
                index + 1
            ));
        }
        options.push(ManualOption {
            directive,
            names,
            source_line: index + 1,
        });
    }
    Ok(options)
}

fn parse_rpc_vector(source: &str, vector_name: &str) -> Result<Vec<RpcEntry>, String> {
    let marker = format!("std::vector<std::string> {vector_name} = {{");
    let start = source
        .lines()
        .position(|line| line.contains(&marker))
        .ok_or_else(|| format!("RPC source has no {vector_name} vector"))?;
    let mut entries = Vec::new();
    let mut conditions = ConditionStack::default();
    let mut closed = false;

    for (index, line) in source.lines().enumerate().skip(start + 1) {
        let trimmed = line.trim();
        if trimmed == "};" {
            closed = true;
            break;
        }
        if trimmed.starts_with('#') {
            conditions.apply(trimmed, index + 1)?;
            continue;
        }
        for name in quoted_strings(line)
            .into_iter()
            .filter(|name| name.starts_with("aria2.") || name.starts_with("system."))
        {
            entries.push(RpcEntry {
                name,
                compile_conditions: conditions.current().to_vec(),
                source_line: index + 1,
            });
        }
    }
    if !closed {
        return Err(format!("unterminated RPC vector {vector_name}"));
    }
    conditions.finish()?;
    Ok(entries)
}

#[derive(Default)]
struct ConditionStack {
    conditions: Vec<String>,
}

impl ConditionStack {
    fn current(&self) -> &[String] {
        &self.conditions
    }

    fn apply(&mut self, directive: &str, line: usize) -> Result<(), String> {
        if let Some(value) = directive.strip_prefix("#ifdef ") {
            self.conditions.push(format!("defined({})", value.trim()));
        } else if let Some(value) = directive.strip_prefix("#ifndef ") {
            self.conditions.push(format!("!defined({})", value.trim()));
        } else if let Some(value) = directive.strip_prefix("#if ") {
            self.conditions.push(value.trim().to_owned());
        } else if let Some(value) = directive.strip_prefix("#elif ") {
            let condition = self
                .conditions
                .last_mut()
                .ok_or_else(|| format!("#elif without #if at line {line}"))?;
            *condition = format!("elif({})", value.trim());
        } else if directive.starts_with("#else") {
            let condition = self
                .conditions
                .last_mut()
                .ok_or_else(|| format!("#else without #if at line {line}"))?;
            *condition = format!("else({condition})");
        } else if directive.starts_with("#endif") {
            self.conditions
                .pop()
                .ok_or_else(|| format!("#endif without #if at line {line}"))?;
        }
        Ok(())
    }

    fn finish(self) -> Result<(), String> {
        if self.conditions.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "unterminated preprocessor conditions: {}",
                self.conditions.join(", ")
            ))
        }
    }
}

pub(crate) fn apply_outputs(
    workspace_root: &Path,
    outputs: &[(PathBuf, String)],
    mode: GenerationMode,
) -> Result<(), String> {
    for (relative, expected) in outputs {
        let path = workspace_root.join(relative);
        match mode {
            GenerationMode::Write => {
                if fs::read_to_string(&path).ok().as_deref() == Some(expected.as_str()) {
                    continue;
                }
                fs::create_dir_all(
                    path.parent()
                        .ok_or_else(|| format!("{} has no parent", path.display()))?,
                )
                .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
                fs::write(&path, expected)
                    .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
            }
            GenerationMode::Check => match fs::read_to_string(&path) {
                Ok(actual) if actual == *expected => {}
                Ok(_) => {
                    return Err(format!(
                        "{} is out of date; run cargo xtask generate",
                        relative.display()
                    ));
                }
                Err(error) => {
                    return Err(format!(
                        "cannot check {}: {error}; run cargo xtask generate",
                        relative.display()
                    ));
                }
            },
        }
    }
    Ok(())
}

fn require_unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
    description: &str,
) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(format!("duplicate {description} {value}"));
        }
    }
    Ok(())
}

fn take_identifier(value: &str) -> &str {
    let end = value
        .find(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .unwrap_or(value.len());
    &value[..end]
}

fn first_quoted(value: &str) -> Option<String> {
    quoted_strings(value).into_iter().next()
}

fn quoted_strings(value: &str) -> Vec<String> {
    let bytes = value.as_bytes();
    let mut strings = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        index += 1;
        let mut string = String::new();
        while index < bytes.len() {
            match bytes[index] {
                b'"' => {
                    strings.push(string);
                    index += 1;
                    break;
                }
                b'\\' if index + 1 < bytes.len() => {
                    string.push(bytes[index] as char);
                    string.push(bytes[index + 1] as char);
                    index += 2;
                }
                byte => {
                    string.push(byte as char);
                    index += 1;
                }
            }
        }
    }
    strings
}

fn extract_call_arguments(value: &str, marker: &str) -> Vec<String> {
    let mut arguments = Vec::new();
    let mut remaining = value;
    while let Some(start) = remaining.find(marker) {
        remaining = &remaining[start + marker.len()..];
        let Some(end) = remaining.find(')') else {
            break;
        };
        arguments.push(remaining[..end].trim().to_owned());
        remaining = &remaining[end + 1..];
    }
    arguments
}

fn extract_long_options(directive: &str) -> Vec<String> {
    let bytes = directive.as_bytes();
    let mut names = Vec::new();
    let mut index = 0;
    while index + 2 <= bytes.len() {
        if &bytes[index..index + 2] != b"--" {
            index += 1;
            continue;
        }
        index += 2;
        let start = index;
        while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'-')
        {
            index += 1;
        }
        if index > start {
            names.push(directive[start..index].to_owned());
        }
    }
    names
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

fn write_rpc_entries(output: &mut String, name: &str, entries: &[RpcEntry], comma_after: bool) {
    writeln!(output, "  \"{name}\": [").expect("write to string");
    for (index, entry) in entries.iter().enumerate() {
        write!(
            output,
            "    {{\"name\": {}, \"source_line\": {}, \"compile_conditions\": ",
            json_string(&entry.name),
            entry.source_line
        )
        .expect("write to string");
        write_string_array(output, &entry.compile_conditions);
        writeln!(output, "}}{}", comma(index, entries.len())).expect("write to string");
    }
    writeln!(output, "  ]{}", if comma_after { "," } else { "" }).expect("write to string");
}

fn write_string_array(output: &mut String, values: &[String]) {
    output.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&json_string(value));
    }
    output.push(']');
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

pub(crate) fn json_string(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                write!(output, "\\u{:04x}", character as u32).expect("write to string");
            }
            character => output.push(character),
        }
    }
    output.push('"');
    output
}

pub(crate) fn comma(index: usize, len: usize) -> &'static str {
    if index + 1 == len { "" } else { "," }
}

#[cfg(test)]
mod tests {
    use super::{
        ConditionStack, extract_long_options, json_string, parse_manual_options,
        parse_option_handlers, parse_preferences, parse_rpc_vector,
    };

    const PREFS: &str = r#"
PrefPtr PREF_DIR = makePref("dir");
PrefPtr PREF_SPLIT =
    makePref("split");
"#;

    const HANDLERS: &str = r#"
{
  OptionHandler* op(new LocalFilePathOptionHandler(
      PREF_DIR, TEXT_DIR, "/tmp", false));
  op->addTag(TAG_BASIC);
  op->setInitialOption(true);
  handlers.push_back(op);
}
#ifdef ENABLE_SPLIT
{
  OptionHandler* op(new NumberOptionHandler(PREF_SPLIT, TEXT_SPLIT, "5", 1));
  op->setChangeGlobalOption(true);
  handlers.push_back(op);
}
#endif
"#;

    #[test]
    fn preferences_and_handlers_capture_upstream_facts() {
        let preferences = parse_preferences(PREFS).expect("preferences");
        let handlers = parse_option_handlers(HANDLERS, &preferences).expect("handlers");
        assert_eq!(preferences.len(), 2);
        assert_eq!(preferences[1].name, "split");
        assert_eq!(handlers.len(), 2);
        assert_eq!(handlers[0].handler, "LocalFilePathOptionHandler");
        assert_eq!(handlers[0].tags, ["TAG_BASIC"]);
        assert!(handlers[0].initial);
        assert_eq!(handlers[1].compile_conditions, ["defined(ENABLE_SPLIT)"]);
        assert!(handlers[1].change_global);
    }

    #[test]
    fn parsers_reject_unknown_or_unbalanced_source_shapes() {
        let preferences = parse_preferences(PREFS).expect("preferences");
        assert!(parse_option_handlers("handlers.push_back(op);", &preferences).is_err());
        assert!(
            parse_option_handlers(
                "#ifdef X\nOptionHandler* op(new NumberOptionHandler(PREF_SPLIT));\nhandlers.push_back(op);\n",
                &preferences,
            )
            .is_err()
        );
    }

    #[test]
    fn manual_and_rpc_parsers_preserve_aliases_and_guards() {
        let manual = parse_manual_options(".. option:: -d, --dir=<DIR>\n").expect("manual options");
        assert_eq!(manual[0].names, ["dir"]);
        let rpc = parse_rpc_vector(
            "std::vector<std::string> rpcMethodNames = {\n#ifdef BT\n\"aria2.addTorrent\",\n#endif\n\"aria2.addUri\", \"system.multicall\",\n};\n",
            "rpcMethodNames",
        )
        .expect("RPC entries");
        assert_eq!(rpc.len(), 3);
        assert_eq!(rpc[0].compile_conditions, ["defined(BT)"]);
        assert!(rpc[1].compile_conditions.is_empty());
    }

    #[test]
    fn helper_encodings_are_deterministic() {
        assert_eq!(
            extract_long_options("-x, --one=A, --two[=B]"),
            ["one", "two"]
        );
        assert_eq!(json_string("a\n\"b"), "\"a\\n\\\"b\"");
        let mut conditions = ConditionStack::default();
        conditions.apply("#if A", 1).expect("if");
        conditions.apply("#else", 2).expect("else");
        assert_eq!(conditions.current(), ["else(A)"]);
        conditions.apply("#endif", 3).expect("endif");
        conditions.finish().expect("balanced");
    }
}
