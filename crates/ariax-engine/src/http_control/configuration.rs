use super::*;

pub(super) struct Candidate {
    flat: BTreeMap<String, String>,
    rules: Arc<ariax_config::UrlRules>,
    warnings: usize,
}

struct PreparedConfiguration {
    flat: Arc<BTreeMap<String, String>>,
    rpc: Arc<BTreeMap<String, String>>,
    effective: Arc<BTreeMap<String, String>>,
    rules: Arc<ariax_config::UrlRules>,
    scheduling: crate::SlowSlotConfig,
    previous_generation: u64,
    result: Value,
    charge: crate::rpc_budget::RpcByteCharge,
}

pub(super) struct PendingConfiguration {
    ready: Option<PreparedConfiguration>,
    receiver: std::sync::mpsc::Receiver<Result<PreparedConfiguration, HttpControlError>>,
    reply: oneshot::Sender<Result<Value, HttpControlError>>,
    _request: crate::rpc_budget::RpcRequestLease,
}

impl HttpControlPlane {
    pub(super) fn begin_configuration(
        &mut self,
        method: &str,
        params: Value,
        request: crate::rpc_budget::RpcRequestLease,
    ) -> Result<ControlReply, HttpControlError> {
        if self.pending_configuration.is_some()
            || self.pending_admission.is_some()
            || self.admission_fenced()
            || self.bt_admission_pending()
        {
            return Err(HttpControlError::Busy);
        }
        let slot = self.queries.reserve_projection()?;
        let configuration = self.configuration_snapshot();
        let owner = self.owner_client.clone();
        let retained = request.clone();
        let reload = method == "ariax.reloadConfig";
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("ariax-configuration".to_owned())
            .spawn(move || {
                let (_slot, _retained) = (slot, retained);
                let result = configuration.prepare_configuration(params, reload, &owner);
                let _ = sender.send(result);
            })
            .map_err(|_| HttpControlError::Busy)?;
        let (reply, response) = oneshot::channel();
        self.pending_configuration = Some(PendingConfiguration {
            ready: None,
            receiver,
            reply,
            _request: request,
        });
        Ok(ControlReply::Deferred(response))
    }

    pub(super) fn poll_configuration(&mut self) {
        if !self.engine_idle() || self.pending_mutation.is_some() || self.admission_fenced() {
            return;
        }
        let Some(mut pending) = self.pending_configuration.take() else {
            return;
        };
        if pending.ready.is_none() {
            match pending.receiver.try_recv() {
                Ok(Ok(prepared)) => pending.ready = Some(prepared),
                Ok(Err(error)) => {
                    let _ = pending.reply.send(Err(error));
                    self.turn.mark_progress();
                    return;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    self.pending_configuration = Some(pending);
                    return;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    let _ = pending.reply.send(Err(HttpControlError::Persistence(
                        "configuration preparation stopped".to_owned(),
                    )));
                    self.turn.mark_progress();
                    return;
                }
            }
        }
        let prepared = pending.ready.as_ref().expect("prepared configuration");
        let limit = prepared
            .effective
            .get("max-overall-download-limit")
            .map_or("0", String::as_str);
        match self.apply_global_download_limit(limit) {
            Ok(false) => {
                self.pending_configuration = Some(pending);
                return;
            }
            Err(error) => {
                let _ = pending.reply.send(Err(error));
                self.turn.mark_progress();
                return;
            }
            Ok(true) => {}
        }
        let result =
            self.publish_configuration(pending.ready.take().expect("prepared configuration"));
        self.turn.mark_progress();
        if result.is_ok() {
            self.publish_query();
        }
        let _ = pending.reply.send(result);
    }

    fn publish_configuration(
        &mut self,
        prepared: PreparedConfiguration,
    ) -> Result<Value, HttpControlError> {
        if self.config_generation != prepared.previous_generation {
            return Err(HttpControlError::InvalidParams(
                "stale configuration generation",
            ));
        }
        self.engine
            .configure_queue_policies(
                prepared.scheduling.retry_wait == crate::RetryWaitSlotPolicy::Retain,
                prepared.scheduling.readmit_policy,
            )
            .map_err(|_| HttpControlError::Busy)?;
        self.scheduling.replace(prepared.scheduling);
        if prepared.scheduling.policy == crate::SlowSlotPolicy::Off {
            self.slow_observations = Arc::new(BTreeMap::new());
        }
        self.global_options = prepared.effective;
        self.flat_options = prepared.flat;
        self.rpc_template = prepared.rpc;
        self.url_rules = prepared.rules;
        self.config_generation = prepared.previous_generation + 1;
        self.config_charge = Some(prepared.charge);
        Ok(prepared.result)
    }

    pub(super) fn check_config(&self, params: Value) -> Result<Value, HttpControlError> {
        self.configuration_snapshot().check_config(params)
    }

    pub(super) fn apply_global_download_limit(
        &mut self,
        canonical: &str,
    ) -> Result<bool, HttpControlError> {
        let bytes = canonical
            .parse::<u64>()
            .map_err(|_| HttpControlError::InvalidConfig)?;
        #[cfg(feature = "bt")]
        {
            self.require_bt_bandwidth(&self.engine.scheduler().clone(), bytes)
        }
        #[cfg(not(feature = "bt"))]
        {
            if let Some(rate) = &self.global_download_rate {
                rate.set_global_limit(RateLimit::per_second(bytes))
                    .map_err(|_| HttpControlError::InvalidConfig)?;
            }
            Ok(true)
        }
    }

    pub(super) fn configuration_command_bytes(
        &self,
        method: &str,
        params: &Value,
    ) -> Result<usize, HttpControlError> {
        self.configuration_snapshot()
            .configuration_command_bytes(method, params)
    }

    pub(super) fn dump_config(&self, params: Value) -> Result<Value, HttpControlError> {
        self.capture_query().dump_config(params)
    }
}

pub(super) fn layer_owns_option(layer: &BTreeMap<String, String>, key: &str) -> bool {
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

impl super::query::ConfigurationSnapshot {
    fn prepare_configuration(
        &self,
        params: Value,
        reload: bool,
        owner: &crate::RpcClientBudget,
    ) -> Result<PreparedConfiguration, HttpControlError> {
        let (flat, rpc, rules, result) = if reload {
            let candidate = self.parse_configuration(&params, true)?;
            let result = json!({"reloaded":true, "options":candidate.flat.len(), "warnings":candidate.warnings,
                "configGeneration": self.config_generation.checked_add(1).ok_or(HttpControlError::InvalidConfig)?});
            (
                candidate.flat,
                (*self.rpc_template).clone(),
                candidate.rules,
                result,
            )
        } else {
            let values = params.as_array().filter(|values| values.len() == 1).ok_or(
                HttpControlError::InvalidParams("changeGlobalOption requires one option object"),
            )?;
            let patch = parse_registry_options(&values[0], Scope::RpcGlobal)?;
            let mut rpc = (*self.rpc_template).clone();
            merge_layer(
                &mut rpc,
                &patch
                    .into_iter()
                    .map(|(name, entry)| (name, entry.canonical))
                    .collect(),
            );
            (
                (*self.flat_options).clone(),
                rpc,
                self.url_rules.clone(),
                Value::String("OK".to_owned()),
            )
        };
        let validate = || -> Result<_, HttpControlError> {
            let mut effective = default_global_options()?;
            merge_resolved_layer(&mut effective, &flat)?;
            merge_resolved_layer(&mut effective, &rpc)?;
            self.validate_template(&effective)?;
            let scheduling = crate::SlowSlotConfig::from_options(&effective)?;
            for rule in rules.rules() {
                let mut candidate = flat.clone();
                merge_resolved_layer(&mut candidate, rule.options())?;
                merge_resolved_layer(&mut candidate, &rpc)?;
                self.validate_template(&candidate)?;
            }
            self.config_generation
                .checked_add(1)
                .ok_or(HttpControlError::InvalidConfig)?;
            Ok((effective, scheduling))
        };
        let (effective, scheduling) = validate().map_err(|error| {
            if !reload
                && matches!(
                    error,
                    HttpControlError::InvalidParams(_) | HttpControlError::TaskSpec(_)
                )
            {
                rejected_option_names(
                    params[0].as_object().expect("validated patch").keys(),
                    OptionPatchRejectReason::InvalidValue,
                )
            } else {
                error
            }
        })?;
        // Current configuration storage and publication pointers are charged
        // before installation; readers retaining older generations add their
        // own conservative retention charge.
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
        let charge = owner.charge(bytes).map_err(|_| HttpControlError::Busy)?;
        Ok(PreparedConfiguration {
            flat: Arc::new(flat),
            rpc: Arc::new(rpc),
            effective: Arc::new(effective),
            rules,
            scheduling,
            previous_generation: self.config_generation,
            result,
            charge,
        })
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

    pub(super) fn parse_configuration(
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

    pub(super) fn validate_template(
        &self,
        values: &BTreeMap<String, String>,
    ) -> Result<(), HttpControlError> {
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

    pub(super) fn check_config(&self, params: Value) -> Result<Value, HttpControlError> {
        let candidate = self.parse_configuration(&params, false)?;
        Ok(
            json!({"valid":true, "options":candidate.flat.len(), "warnings":candidate.warnings, "configGeneration":self.config_generation}),
        )
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
