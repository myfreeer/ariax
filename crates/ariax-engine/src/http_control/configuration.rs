use super::*;
use std::fmt::Write as _;

struct Candidate {
    flat: BTreeMap<String, String>,
    rules: Arc<ariax_config::UrlRules>,
    warnings: usize,
}

impl HttpControlPlane {
    fn parse_configuration(
        &self,
        params: &Value,
        reload: bool,
    ) -> Result<Candidate, HttpControlError> {
        let values = params
            .as_array()
            .filter(|values| (1..=2).contains(&values.len()))
            .ok_or(HttpControlError::InvalidParams(
                "configuration requires text and optional settings",
            ))?;
        let text = values[0].as_str().ok_or(HttpControlError::InvalidParams(
            "configuration must be text",
        ))?;
        let mut mode = UnknownOptionMode::Strict;
        let mut rules = self.url_rules.clone();
        if let Some(settings) = values.get(1) {
            let settings = settings.as_object().ok_or(HttpControlError::InvalidParams(
                "configuration settings must be an object",
            ))?;
            if settings.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "expectedGeneration" | "urlRules" | "compatibility"
                )
            }) {
                return Err(HttpControlError::InvalidParams(
                    "unknown configuration setting",
                ));
            }
            if let Some(expected) = settings.get("expectedGeneration")
                && expected.as_u64() != Some(self.config_generation)
            {
                return Err(HttpControlError::InvalidParams(
                    "stale configuration generation",
                ));
            }
            if let Some(compatibility) = settings.get("compatibility") {
                mode = match compatibility.as_str() {
                    Some("strict") => UnknownOptionMode::Strict,
                    Some("aria2") => UnknownOptionMode::WarnKnownUnsupported,
                    _ => {
                        return Err(HttpControlError::InvalidParams(
                            "invalid configuration compatibility mode",
                        ));
                    }
                };
            }
            if let Some(text) = settings.get("urlRules") {
                rules = Arc::new(
                    ariax_config::UrlRules::parse(text.as_str().ok_or(
                        HttpControlError::InvalidParams("URL rules must be TOML text"),
                    )?)
                    .map_err(|_| HttpControlError::InvalidParams("URL rules are invalid"))?,
                );
            }
        }
        let parsed = parse_flat_config(
            builtin_registry(),
            text,
            mode,
            FlatConfigLimits::default(),
            None,
        )
        .map_err(|_| HttpControlError::InvalidParams("configuration is invalid"))?;
        let mut flat = BTreeMap::new();
        for (name, entry) in parsed.entries() {
            if reload
                && (!is_executable_global_option(name)
                    || !entry.definition.scopes.contains(Scope::Global)
                    || entry.definition.security != SecurityClass::Normal)
            {
                return Err(HttpControlError::InvalidParams(
                    "configuration contains a non-reloadable option",
                ));
            }
            if entry.definition.security == SecurityClass::Normal {
                flat.insert(name.to_owned(), canonical_option_value(&entry.value)?);
            }
        }
        let task_flat: BTreeMap<_, _> = flat
            .iter()
            .filter(|(name, _)| is_executable_global_option(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        self.validate_template(&task_flat)?;
        let mut effective = task_flat.clone();
        merge_resolved_layer(&mut effective, &self.rpc_template)?;
        self.validate_template(&effective)?;
        let mut cumulative = task_flat.clone();
        for rule in rules.rules() {
            self.validate_template(rule.options())?;
            let mut merged = task_flat.clone();
            merge_resolved_layer(&mut merged, rule.options())?;
            merge_resolved_layer(&mut cumulative, rule.options())?;
            self.validate_template(&merged)?;
            self.validate_template(&cumulative)?;
            merge_resolved_layer(&mut merged, &self.rpc_template)?;
            self.validate_template(&merged)?;
            let mut resolved_cumulative = cumulative.clone();
            merge_resolved_layer(&mut resolved_cumulative, &self.rpc_template)?;
            self.validate_template(&resolved_cumulative)?;
        }
        Ok(Candidate {
            flat,
            rules,
            warnings: parsed.warnings().len(),
        })
    }

    fn validate_template(&self, values: &BTreeMap<String, String>) -> Result<(), HttpControlError> {
        crate::SlowSlotConfig::from_options(values)?;
        if values.keys().any(|name| !is_executable_global_option(name)) {
            return Err(HttpControlError::InvalidParams(
                "configuration option is not implemented by HTTP tasks",
            ));
        }
        let value = Value::Object(
            values
                .iter()
                .filter(|(name, _)| is_executable_download_option(name))
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect(),
        );
        let (options, _, _, _) = parse_add_options(&value, &self.config.output_root, &[])?;
        HttpTaskOptions::from_sanitized(&options.sanitized().map_err(HttpControlError::TaskSpec)?)
            .map_err(HttpControlError::TaskSpec)?;
        Ok(())
    }

    fn publish_configuration(
        &mut self,
        flat: BTreeMap<String, String>,
        rpc: BTreeMap<String, String>,
        rules: Arc<ariax_config::UrlRules>,
    ) -> Result<(), HttpControlError> {
        let mut effective = default_global_options()?;
        merge_resolved_layer(&mut effective, &flat)?;
        merge_resolved_layer(&mut effective, &rpc)?;
        self.validate_template(&effective)?;
        let scheduling = crate::SlowSlotConfig::from_options(&effective)?;
        if !self.engine.is_idle() {
            return Err(HttpControlError::Busy);
        }
        for rule in rules.rules() {
            let mut candidate = flat.clone();
            merge_resolved_layer(&mut candidate, rule.options())?;
            merge_resolved_layer(&mut candidate, &rpc)?;
            self.validate_template(&candidate)?;
        }
        let generation = self
            .config_generation
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        let bytes = rules
            .owned_bytes()
            .saturating_add(map_bytes(&flat))
            .saturating_add(map_bytes(&rpc))
            .saturating_add(map_bytes(&effective))
            .saturating_add(4096)
            .saturating_add(
                if scheduling.policy != crate::SlowSlotPolicy::Off
                    || scheduling.retry_wait == crate::RetryWaitSlotPolicy::Auto
                {
                    self.config.task_capacity.get().saturating_mul(512)
                } else {
                    0
                },
            );
        let charge = self
            .owner_client
            .charge(bytes)
            .map_err(|_| HttpControlError::Busy)?;
        if let Some(value) = effective.get("max-overall-download-limit") {
            self.apply_global_download_limit(value)?;
        }
        self.engine
            .configure_queue_policies(
                scheduling.retry_wait == crate::RetryWaitSlotPolicy::Retain,
                scheduling.readmit_policy,
            )
            .map_err(|_| HttpControlError::Busy)?;
        self.scheduling.replace(scheduling);
        if scheduling.policy == crate::SlowSlotPolicy::Off {
            self.slow_observations.clear();
        }
        self.global_options = effective;
        self.flat_options = flat;
        self.rpc_template = rpc;
        self.url_rules = rules;
        self.config_generation = generation;
        self.config_charge = Some(charge);
        Ok(())
    }

    pub(super) fn change_global_option(
        &mut self,
        params: Value,
    ) -> Result<Value, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("changeGlobalOption requires one option object"),
        )?;
        let patch = parse_registry_options(&values[0], Scope::RpcGlobal)?;
        let mut next = self.rpc_template.clone();
        merge_layer(
            &mut next,
            &patch
                .into_iter()
                .map(|(name, entry)| (name, entry.canonical))
                .collect(),
        );
        self.publish_configuration(self.flat_options.clone(), next, self.url_rules.clone())
            .map_err(|error| match error {
                HttpControlError::InvalidParams(_) | HttpControlError::TaskSpec(_) => {
                    rejected_option_names(
                        values[0].as_object().expect("validated patch").keys(),
                        ariax_core::OptionPatchRejectReason::InvalidValue,
                    )
                }
                other => other,
            })?;
        Ok(Value::String("OK".to_owned()))
    }

    pub(super) fn check_config(&self, params: Value) -> Result<Value, HttpControlError> {
        let candidate = self.parse_configuration(&params, false)?;
        Ok(
            json!({"valid":true, "options":candidate.flat.len(), "warnings":candidate.warnings, "configGeneration":self.config_generation}),
        )
    }

    pub(super) fn reload_config(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let candidate = self.parse_configuration(&params, true)?;
        let count = candidate.flat.len();
        let warnings = candidate.warnings;
        self.publish_configuration(candidate.flat, self.rpc_template.clone(), candidate.rules)?;
        Ok(
            json!({"reloaded":true, "options":count, "warnings":warnings, "configGeneration":self.config_generation}),
        )
    }

    pub(super) fn apply_global_download_limit(
        &self,
        canonical: &str,
    ) -> Result<(), HttpControlError> {
        let bytes = canonical
            .parse::<u64>()
            .map_err(|_| HttpControlError::InvalidConfig)?;
        if let Some(rate) = &self.global_download_rate {
            rate.set_global_limit(RateLimit::per_second(bytes))
                .map_err(|_| HttpControlError::InvalidConfig)?;
        }
        Ok(())
    }

    pub(super) fn merged_add_options(
        &self,
        explicit: Value,
        uris: &[String],
        input_file: bool,
    ) -> Result<Value, HttpControlError> {
        let explicit = explicit.as_object().ok_or(HttpControlError::InvalidParams(
            "addUri options must be an object",
        ))?;
        let explicit = explicit
            .iter()
            .map(|(name, value)| Ok((name.clone(), option_input_text(value)?)))
            .collect::<Result<BTreeMap<_, _>, HttpControlError>>()?;
        let mut options = BTreeMap::new();
        let flat = self
            .flat_options
            .iter()
            .filter(|(key, _)| is_executable_download_option(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        merge_resolved_layer(&mut options, &flat)?;
        if let Some(uri) = uris.first() {
            let rules = self.url_rules.matching_rules(uri).map_err(|_| {
                HttpControlError::InvalidParams("URL rule matching exceeded its limits")
            })?;
            for rule in rules {
                merge_resolved_layer(&mut options, rule.options())?;
            }
        }
        if input_file {
            merge_resolved_layer(&mut options, &explicit)?;
        }
        let rpc = self
            .rpc_template
            .iter()
            .filter(|(key, _)| is_executable_download_option(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        merge_resolved_layer(&mut options, &rpc)?;
        if !input_file {
            merge_resolved_layer(&mut options, &explicit)?;
        }
        Ok(Value::Object(
            options
                .into_iter()
                .map(|(name, value)| (name, Value::String(value)))
                .collect(),
        ))
    }

    pub(super) fn configuration_command_bytes(
        &self,
        method: &str,
        params: &Value,
    ) -> Result<usize, HttpControlError> {
        let mut bytes = 0_usize;
        if matches!(method, "aria2.addUri" | "addUri") {
            bytes = self.configuration_defaults_bytes();
        }
        if matches!(method, "ariax.checkConfig" | "ariax.reloadConfig")
            && let Some(text) = params
                .get(1)
                .and_then(|settings| settings.get("urlRules"))
                .and_then(Value::as_str)
        {
            bytes =
                bytes.saturating_add(ariax_config::UrlRules::parse_memory_bound(text).map_err(
                    |_| HttpControlError::InvalidParams("URL rules exceed their allocation limit"),
                )?);
        }
        if matches!(
            method,
            "aria2.changeGlobalOption"
                | "changeGlobalOption"
                | "ariax.reloadConfig"
                | "ariax.checkConfig"
                | "ariax.dumpConfig"
        ) {
            bytes = bytes.saturating_add(
                map_bytes(&self.flat_options)
                    .saturating_add(map_bytes(&self.rpc_template))
                    .saturating_add(map_bytes(&self.global_options))
                    .saturating_mul(8),
            );
            bytes = bytes.saturating_add(self.url_rules.max_options_bytes().saturating_mul(16));
            if method == "ariax.dumpConfig"
                && params.get(0).and_then(Value::as_str) == Some("url-rules")
            {
                bytes = bytes.saturating_add(self.url_rules.owned_bytes().saturating_mul(8));
            }
        }
        Ok(bytes)
    }

    pub(super) fn configuration_defaults_bytes(&self) -> usize {
        map_bytes(&self.flat_options)
            .saturating_add(map_bytes(&self.rpc_template))
            .saturating_add(self.url_rules.max_options_bytes())
            .saturating_mul(12)
    }

    pub(super) fn dump_config(&self, params: Value) -> Result<Value, HttpControlError> {
        let args = params.as_array().filter(|args| args.len() <= 3).ok_or(
            HttpControlError::InvalidParams("dumpConfig accepts mode, format, and optional GID"),
        )?;
        let mode = args
            .first()
            .map(|value| {
                value
                    .as_str()
                    .ok_or(HttpControlError::InvalidParams("dump mode must be text"))
            })
            .transpose()?
            .unwrap_or("effective");
        let format = args
            .get(1)
            .map(|value| {
                value
                    .as_str()
                    .ok_or(HttpControlError::InvalidParams("dump format must be text"))
            })
            .transpose()?
            .unwrap_or("legacy");
        if !matches!(format, "legacy" | "flat" | "json" | "toml") {
            return Err(HttpControlError::InvalidParams("unknown dump format"));
        }
        if mode == "url-rules" {
            return match format {
                "json" | "legacy" => {
                    crate::rpc_result::to_value(self.url_rules.as_ref(), RESULT_VALUE_BYTES)
                        .map_err(Into::into)
                }
                "toml" => self
                    .url_rules
                    .to_toml()
                    .map(Value::String)
                    .map_err(|_| HttpControlError::InvalidConfig),
                _ => Err(HttpControlError::InvalidParams(
                    "URL rules require JSON or TOML format",
                )),
            };
        }
        let options =
            match mode {
                "effective" => self.global_options.clone(),
                "defaults" => default_global_options()?,
                "task-effective" => {
                    let gid = args.get(2).and_then(Value::as_str).ok_or(
                        HttpControlError::InvalidParams("task-effective dump requires a GID"),
                    )?;
                    let gid = self.resolve_gid_text(gid)?;
                    let spec = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
                    spec.persistence_options()
                        .map_err(HttpControlError::TaskSpec)?
                        .entries()
                        .map(|(name, value)| (name.to_owned(), value.to_owned()))
                        .collect()
                }
                _ => return Err(HttpControlError::InvalidParams("unknown dump mode")),
            };
        if format == "legacy" {
            return string_map_value(
                options
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .map_err(Into::into);
        }
        let sources = options
            .keys()
            .map(|key| {
                (
                    key.clone(),
                    if mode == "task-effective" {
                        "task"
                    } else if mode == "defaults" {
                        "default"
                    } else if layer_owns_option(&self.rpc_template, key) {
                        "rpc"
                    } else if layer_owns_option(&self.flat_options, key) {
                        "config"
                    } else {
                        "default"
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if format == "json" {
            return Ok(
                json!({"configGeneration":self.config_generation, "options":options, "sources":sources}),
            );
        }
        let mut output = String::from("# Generated Ariax configuration\n");
        if format == "toml" {
            writeln!(
                output,
                "configGeneration = {}\n[options]",
                self.config_generation
            )
            .expect("string");
        }
        for (name, value) in &options {
            if format == "toml" {
                writeln!(output, "{} = {}", json!(name), json!(value)).expect("string");
            } else {
                writeln!(output, "{name}={value}").expect("string");
            }
        }
        if format == "toml" {
            output.push_str("[sources]\n");
            for (name, source) in sources {
                writeln!(output, "{} = {}", json!(name), json!(source)).expect("string");
            }
        }
        if output.len() > crate::MAX_HTTP_RPC_RESPONSE_BYTES {
            return Err(HttpControlError::ResponseTooLarge);
        }
        Ok(Value::String(output))
    }
}

fn layer_owns_option(layer: &BTreeMap<String, String>, key: &str) -> bool {
    layer.contains_key(key)
        || (is_retry_option(key) && layer.contains_key("retry-profile"))
        || (matches!(key, "max-tries" | "retry-max-attempts")
            && (layer.contains_key("max-tries") || layer.contains_key("retry-max-attempts")))
        || (key == "retry-on-http-status"
            && (layer.contains_key("retry-on-http-status-add")
                || layer.contains_key("retry-on-http-status-remove")))
}

pub(super) fn discard_inherited_retry(name: &str, has: impl Fn(&str) -> bool) -> bool {
    (has("retry-profile") && is_retry_option(name))
        || (name == "max-tries" && has("retry-max-attempts") && !has("max-tries"))
        || (name == "retry-max-attempts" && has("max-tries") && !has("retry-max-attempts"))
}

fn merge_layer(current: &mut BTreeMap<String, String>, next: &BTreeMap<String, String>) {
    current.retain(|name, _| !discard_inherited_retry(name, |key| next.contains_key(key)));
    current.extend(next.clone());
}

fn merge_resolved_layer(
    current: &mut BTreeMap<String, String>,
    next: &BTreeMap<String, String>,
) -> Result<(), HttpControlError> {
    merge_layer(current, next);
    if next.keys().any(|name| is_retry_option(name)) {
        let values = current
            .iter()
            .filter(|(name, _)| is_retry_option(name))
            .map(|(name, value)| (name.clone(), Value::String(value.clone())))
            .collect();
        let options = HttpTaskOptions {
            retry: Some(parse_retry_options(&values)?),
            ..HttpTaskOptions::default()
        }
        .sanitized()
        .map_err(HttpControlError::TaskSpec)?;
        current.retain(|name, _| !is_retry_option(name));
        current.extend(
            options
                .entries()
                .filter(|(name, _)| is_retry_option(name))
                .map(|(name, value)| (name.to_owned(), value.to_owned())),
        );
    }
    Ok(())
}

fn map_bytes(values: &BTreeMap<String, String>) -> usize {
    values
        .iter()
        .map(|(name, value)| {
            name.capacity()
                .saturating_add(value.capacity())
                .saturating_add(512)
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layer(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn retry_layers_resolve_modifiers_once_and_reset_profiles_and_aliases() {
        let mut values = BTreeMap::new();
        merge_resolved_layer(
            &mut values,
            &layer(&[
                ("retry-on-http-status", "500"),
                ("retry-on-http-status-add", "404"),
            ]),
        )
        .expect("base layer");
        assert_eq!(values["retry-on-http-status"], "404,500");
        merge_resolved_layer(
            &mut values,
            &layer(&[
                ("retry-on-http-status", "502"),
                ("retry-on-http-status-remove", "500"),
            ]),
        )
        .expect("replacement");
        assert_eq!(values["retry-on-http-status"], "502");
        assert!(
            !values
                .keys()
                .any(|key| key.ends_with("-add") || key.ends_with("-remove"))
        );
        merge_resolved_layer(&mut values, &layer(&[("retry-max-attempts", "9")]))
            .expect("one cap alias");
        assert_eq!(values["max-tries"], "9");
        merge_resolved_layer(
            &mut values,
            &layer(&[("max-tries", "8"), ("retry-max-attempts", "7")]),
        )
        .expect("strictest same-layer alias");
        assert_eq!(values["max-tries"], "7");
        merge_resolved_layer(&mut values, &layer(&[("retry-profile", "aggressive")]))
            .expect("profile reset");
        let expected = crate::HttpRetryPolicy::from_profile(crate::HttpRetryProfile::Aggressive);
        assert_eq!(values["max-tries"], expected.max_attempts.to_string());
        assert_eq!(
            values["retry-on-http-status"],
            expected.retryable_statuses.canonical()
        );
        assert!(
            merge_resolved_layer(
                &mut values,
                &layer(&[("retry-wait", "30"), ("retry-max-wait", "5")])
            )
            .is_err()
        );
    }
}
