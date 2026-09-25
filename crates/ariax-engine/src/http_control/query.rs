//! Immutable, identity-bound control queries. No query acquires the mutable owner.

use super::*;
use ariax_core::TaskState;
use ariax_runtime::ConnectionCondition;
use std::fmt::Write as _;
use std::sync::RwLock;
use tokio::sync::Semaphore;

pub(super) fn publication_bytes(task_capacity: usize) -> usize {
    // Applied scheduler views, change identities, and the observer's preceding
    // revision have permanent credit; query jobs charge any further retention.
    task_capacity
        .saturating_mul(
            (std::mem::size_of::<ariax_core::SchedulerTaskView>() + 128).saturating_mul(2),
        )
        .saturating_add(64 * 1024)
}

#[derive(Clone)]
pub(crate) struct ControlQueryReader {
    root: Arc<RwLock<Option<Arc<ControlQueryRoot>>>>,
    execution: Arc<Semaphore>,
    #[cfg(test)]
    gate: Arc<std::sync::Mutex<Option<Arc<ProjectionGate>>>>,
}

impl ControlQueryReader {
    pub(super) fn new() -> Self {
        Self {
            root: Arc::new(RwLock::new(None)),
            execution: Arc::new(Semaphore::new(2)),
            #[cfg(test)]
            gate: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(super) fn occupy_execution(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.execution
            .clone()
            .try_acquire_many_owned(2)
            .expect("both projection slots")
    }

    pub(super) fn reserve_projection(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, HttpControlError> {
        self.execution
            .clone()
            .try_acquire_owned()
            .map_err(|_| HttpControlError::Busy)
    }

    fn publish(&self, root: Arc<ControlQueryRoot>) {
        let previous = {
            let mut published = self
                .root
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            published.replace(root)
        };
        // Releasing a generation can free large metadata trees. The pointer
        // publication lock never covers that destruction.
        drop(previous);
    }

    pub(super) fn current(&self) -> Option<Arc<ControlQueryRoot>> {
        self.root
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) async fn call(
        &self,
        method: &str,
        params: Value,
        context: crate::RpcClientContext,
    ) -> Result<Value, HttpControlError> {
        let slot = self.reserve_projection()?;
        let root = self.current().ok_or(HttpControlError::Busy)?;
        let request = match context.request_lease() {
            Some(request) => request,
            None => root
                .direct_client
                .try_request(0)
                .map_err(|_| HttpControlError::Busy)?,
        };
        let bytes = crate::rpc_json::command_value_bytes(&params)
            .saturating_add(root.configuration_command_bytes(method, &params)?)
            .saturating_add(64 * 1024);
        let command = request
            .reserve_command(bytes)
            .map_err(|_| HttpControlError::Busy)?;
        // The live root shares its status/catalog storage with their owners. A
        // reader can pin an old generation after publication, so charge that
        // retention before handing it to independently executing projection.
        let retained = request
            .client()
            .charge(root.retained_bytes())
            .map_err(|_| HttpControlError::Busy)?;
        // Blocking projection can outlive cancellation of its awaiting RPC.
        // Keep its own workspace through result delivery/drop in that case.
        let workspace = request
            .client()
            .charge(crate::rpc_budget::RPC_RESULT_WORKSPACE_BYTES)
            .map_err(|_| HttpControlError::Busy)?;
        let method = method.to_owned();
        #[cfg(test)]
        let gate = self.gate.lock().expect("test gate").clone();
        tokio::task::spawn_blocking(move || {
            let (_slot, _command, _retained) = (slot, command, retained);
            #[cfg(test)]
            if let Some(gate) = gate {
                gate.wait();
            }
            let result = root.call(&method, params)?;
            if matches!(method.as_str(), "aria2.tellStatus" | "tellStatus") {
                let gid = result
                    .get("gid")
                    .and_then(Value::as_str)
                    .and_then(|gid| gid.parse().ok());
                if let Ok(event) = RpcEvent::notification(
                    "ariax.onStatus",
                    result.clone(),
                    RpcEventClass::Coalesced,
                    Some(RpcEventKey::new(gid, "ariax.onStatus")),
                ) {
                    root.events.publish(event);
                }
            }
            Ok(ProjectedValue {
                value: result,
                _workspace: workspace,
            })
        })
        .await
        .map_err(|_| HttpControlError::Scheduler("query executor stopped".to_owned()))?
        .map(|projected| projected.value)
    }
}

struct ProjectedValue {
    value: Value,
    _workspace: crate::rpc_budget::RpcByteCharge,
}

pub(crate) fn is_query(method: &str) -> bool {
    matches!(
        method.strip_prefix("aria2.").unwrap_or(method),
        "tellStatus"
            | "tellActive"
            | "tellWaiting"
            | "tellStopped"
            | "getUris"
            | "getFiles"
            | "getServers"
            | "getPeers"
            | "getOption"
            | "getGlobalOption"
            | "getVersion"
            | "getSessionInfo"
            | "getGlobalStat"
            | "ariax.checkConfig"
            | "ariax.dumpConfig"
            | "ariax.exportSession"
            | "ariax.getDiagnostics"
    )
}

pub(super) struct ConfigurationSnapshot {
    pub(super) config: HttpControlPlaneConfig,
    pub(super) global_options: Arc<BTreeMap<String, String>>,
    pub(super) flat_options: Arc<BTreeMap<String, String>>,
    pub(super) rpc_template: Arc<BTreeMap<String, String>>,
    pub(super) url_rules: Arc<ariax_config::UrlRules>,
    pub(super) config_generation: u64,
}

pub(super) struct ControlQueryRoot {
    #[cfg(feature = "bt")]
    bt_tasks: Arc<BTreeMap<Gid, Arc<super::bittorrent::QueryTask>>>,
    status: Arc<ariax_runtime::StatusSnapshotRoot>,
    tasks: Arc<crate::HttpTaskCatalog>,
    stats: Arc<BTreeMap<TaskId, crate::HttpTransferStats>>,
    slow_observations: Arc<BTreeMap<Gid, crate::slow_slots::SlowObservation>>,
    _publication: Arc<crate::rpc_budget::RpcByteCharge>,
    sample: Option<MonotonicInstant>,
    configuration: Arc<ConfigurationSnapshot>,
    session_id: SessionId,
    diagnostics: ControlDiagnostics,
    direct_client: crate::RpcClientBudget,
    budgets: crate::RpcBudgets,
    events: RpcEventBroker,
}

impl std::ops::Deref for ControlQueryRoot {
    type Target = ConfigurationSnapshot;
    fn deref(&self) -> &Self::Target {
        &self.configuration
    }
}

impl HttpControlPlane {
    pub(crate) fn query_reader(&self) -> ControlQueryReader {
        let root = self.capture_query();
        self.queries.publish(root);
        self.queries.clone()
    }

    pub(super) fn configuration_snapshot(&self) -> Arc<ConfigurationSnapshot> {
        if let Some(root) = self.queries.current()
            && root.config_generation == self.config_generation
        {
            return root.configuration.clone();
        }
        Arc::new(ConfigurationSnapshot {
            config: self.config.clone(),
            global_options: self.global_options.clone(),
            flat_options: self.flat_options.clone(),
            rpc_template: self.rpc_template.clone(),
            url_rules: self.url_rules.clone(),
            config_generation: self.config_generation,
        })
    }

    pub(super) fn capture_query(&self) -> Arc<ControlQueryRoot> {
        let status = self.engine.snapshot_reader().load();
        let tasks = self.tasks.snapshot();
        let stats = self.stats.snapshot_handles();
        #[cfg(feature = "bt")]
        let bt_unchanged = self
            .queries
            .current()
            .is_some_and(|root| Arc::ptr_eq(&root.bt_tasks, &self.bt.catalog));
        #[cfg(not(feature = "bt"))]
        let bt_unchanged = true;
        if let Some(root) = self.queries.current()
            && Arc::ptr_eq(&root.status, &status)
            && Arc::ptr_eq(&root.tasks, &tasks)
            && Arc::ptr_eq(&root.stats, &stats)
            && root.config_generation == self.config_generation
            && root.sample == self.next_slow_sample
            && bt_unchanged
        {
            return root;
        }
        Arc::new(ControlQueryRoot {
            #[cfg(feature = "bt")]
            bt_tasks: self.bt.catalog.clone(),
            slow_observations: self.slow_observations.clone(),
            _publication: self.query_publication_charge.clone(),
            sample: self.next_slow_sample,
            status,
            tasks,
            stats,
            configuration: self.configuration_snapshot(),
            session_id: self.session_id,
            diagnostics: self.diagnostics(),
            direct_client: self.direct_client.clone(),
            budgets: self.rpc_budgets.clone(),
            events: self.events.clone(),
        })
    }

    pub(super) fn publish_query(&self) {
        if self.queries.current().is_some() {
            let root = self.capture_query();
            self.queries.publish(root);
        }
    }
}

impl ControlQueryRoot {
    pub(super) fn get_peers(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        #[cfg(feature = "bt")]
        {
            let bt = self.bt_task(gid).ok_or(HttpControlError::NotFound)?;
            let mut peers = ResultList::new();
            for peer in bt.peers.iter() {
                peers.push_scratch(json!({ "peerId": peer.peer_id, "ip": peer.address, "port": peer.port.to_string(),
                    "bitfield": "", "amChoking": peer.am_choking.to_string(), "peerChoking": peer.peer_choking.to_string(),
                    "downloadSpeed": peer.download_rate.to_string(), "uploadSpeed": peer.upload_rate.to_string(), "seeder": peer.seeder.to_string() }))?;
            }
            Ok(peers.finish())
        }
        #[cfg(not(feature = "bt"))]
        {
            let _ = gid;
            Err(HttpControlError::Unsupported(
                "BitTorrent feature unavailable",
            ))
        }
    }

    #[cfg(feature = "bt")]
    fn bt_task(&self, gid: Gid) -> Option<&super::bittorrent::QueryTask> {
        let applied = self.status.task(gid)?;
        self.bt_tasks
            .get(&gid)
            .filter(|bt| bt.spec.task_id == applied.task_id)
            .map(Arc::as_ref)
    }

    #[cfg(feature = "bt")]
    fn bt_files(&self, bt: &super::bittorrent::QueryTask) -> Result<Value, HttpControlError> {
        let mut files = ResultList::new();
        for file in bt
            .spec
            .record
            .binding
            .files
            .iter()
            .filter(|file| !file.padding)
        {
            let progress = bt
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.file_progress.get(file.index as usize))
                .copied()
                .unwrap_or(0)
                .min(file.length);
            files.push_scratch(json!({ "index": (file.index+1).to_string(), "path": bt.spec.root.path().join(&file.path).to_string_lossy(),
                "length": file.length.to_string(), "completedLength": progress.to_string(), "selected": file.selected.to_string(), "uris": [] }))?;
        }
        Ok(files.finish())
    }

    #[cfg(feature = "bt")]
    fn bt_status(
        &self,
        mut value: Value,
        bt: &super::bittorrent::QueryTask,
        keys: Option<&[String]>,
    ) -> Result<Value, HttpControlError> {
        let identity = &bt.spec.record.binding.identity;
        let total: u64 = bt
            .spec
            .record
            .binding
            .files
            .iter()
            .filter(|file| file.selected && !file.padding)
            .map(|file| file.length)
            .sum();
        let snapshot = bt.snapshot.as_deref();
        value["totalLength"] = json!(total.to_string());
        value["completedLength"] = json!(
            snapshot
                .map_or(0, |snapshot| snapshot.done_bytes)
                .min(total)
                .to_string()
        );
        value["downloadSpeed"] = json!(
            snapshot
                .map_or(0, |snapshot| snapshot.download_rate)
                .to_string()
        );
        value["uploadSpeed"] = json!(
            snapshot
                .map_or(0, |snapshot| snapshot.upload_rate)
                .to_string()
        );
        value["uploadLength"] = json!(bt.uploaded.to_string());
        value["connections"] = json!(snapshot.map_or(0, |snapshot| snapshot.peers).to_string());
        value["numSeeders"] = json!(snapshot.map_or(0, |snapshot| snapshot.seeds).to_string());
        value["seeder"] = json!(
            snapshot
                .is_some_and(|snapshot| snapshot.seeding)
                .to_string()
        );
        value["infoHash"] = json!(identity.v1.as_ref().or(identity.v2.as_ref()));
        if let Some(v2) = &identity.v2 {
            value["infoHashV2"] = json!(v2);
        }
        value["dir"] = json!(bt.spec.root.path().to_string_lossy());
        value["btCheckpointDirty"] = json!(bt.dirty);
        value["btDownloadedLength"] = json!(bt.downloaded.to_string());
        value["btSeedTime"] = json!(bt.seed_millis / 1000);
        if bt.checkpoint_failed {
            value["btCheckpointError"] = json!("DirtyCheckpoint");
        }
        if let Some(metadata) = &bt.spec.metadata {
            value["pieceLength"] = json!(metadata.piece_length.to_string());
            value["numPieces"] = json!(metadata.pieces.to_string());
            value["bittorrent"] = json!({ "info": { "name": metadata.name }, "mode": if metadata.files.iter().filter(|file| !file.padding).count() == 1 { "single" } else { "multi" },
                "announceList": metadata.trackers.iter().map(|tracker| vec![tracker]).collect::<Vec<_>>() });
        }
        if keys.is_some_and(|keys| keys.iter().any(|key| key == "files")) {
            value["files"] = self.bt_files(bt)?;
        }
        Ok(project_status(value, keys))
    }

    pub(super) fn applied_root(&self) -> Arc<ariax_runtime::StatusSnapshotRoot> {
        self.status.clone()
    }

    pub(super) fn retained_bytes(&self) -> usize {
        #[cfg(feature = "bt")]
        let bt_bytes = self
            .bt_tasks
            .values()
            .map(|task| task.spec.retained_bytes())
            .sum();
        #[cfg(not(feature = "bt"))]
        let bt_bytes = 0;
        self.status
            .estimated_draft_bytes()
            .saturating_add(bt_bytes)
            .saturating_add(self.status.len().saturating_mul(1024))
            .saturating_add(self.tasks.retained_bytes())
            .saturating_add(self.configuration_defaults_bytes())
            .saturating_add(self.url_rules.owned_bytes())
            .saturating_add(
                self.global_options
                    .iter()
                    .map(|(key, value)| {
                        key.capacity()
                            .saturating_add(value.capacity())
                            .saturating_add(512)
                    })
                    .sum::<usize>(),
            )
            .saturating_add(64 * 1024)
    }

    pub(super) fn task_spec(&self, gid: Gid) -> Option<Arc<HttpTaskSpec>> {
        let applied = self.status.task(gid)?;
        self.tasks
            .get(applied.task_id)
            .filter(|spec| spec.gid() == gid)
    }

    fn call(&self, method: &str, params: Value) -> Result<Value, HttpControlError> {
        match method.strip_prefix("aria2.").unwrap_or(method) {
            "tellStatus" => self.tell_status(params),
            "tellActive" => self.tell_active(params),
            "tellWaiting" => self.tell_waiting(params),
            "tellStopped" => self.tell_stopped(params),
            "getUris" => self.get_uris(params),
            "getFiles" => self.get_files(params),
            "getServers" => self.get_servers(params),
            "getPeers" => self.get_peers(params),
            "getOption" => self.get_option(params),
            "getGlobalOption" => self.get_global_option(params),
            "getVersion" => self.get_version(params),
            "getSessionInfo" => self.get_session_info(params),
            "getGlobalStat" => self.global_stat(params),
            "ariax.checkConfig" => self.check_config(params),
            "ariax.dumpConfig" => self.dump_config(params),
            "ariax.exportSession" => self.export_session(params),
            "ariax.getDiagnostics" => {
                require_no_params(&params, "getDiagnostics")?;
                let mut value = self.diagnostics.clone();
                let budgets = self.budgets.snapshot();
                value.rpc_items = budgets.items;
                value.rpc_bytes = budgets.bytes;
                value.resident_bytes = budgets.resident_bytes;
                value.event_subscribers = self.events.subscriber_count();
                crate::rpc_result::to_value(&value, RESULT_VALUE_BYTES).map_err(Into::into)
            }
            _ => Err(HttpControlError::Unsupported("method not found")),
        }
    }
    pub(super) fn tell_status(&self, params: Value) -> Result<Value, HttpControlError> {
        let (gid, keys) = self.resolve_gid_and_keys(&params)?;
        let root = self.status.clone();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        self.applied_status(task, keys.as_deref())
    }

    pub(super) fn tell_active(&self, params: Value) -> Result<Value, HttpControlError> {
        let keys = parse_optional_keys_only(&params)?;
        self.list_statuses(
            &[QueueClass::Active],
            0,
            MAX_RPC_LIST_ITEMS,
            keys.as_deref(),
        )
    }

    pub(super) fn tell_waiting(&self, params: Value) -> Result<Value, HttpControlError> {
        let (offset, count, keys) = parse_list_params(&params)?;
        self.list_statuses(
            &[QueueClass::Waiting, QueueClass::Demoted, QueueClass::Paused],
            offset,
            count,
            keys.as_deref(),
        )
    }

    pub(super) fn tell_stopped(&self, params: Value) -> Result<Value, HttpControlError> {
        let (offset, count, keys) = parse_list_params(&params)?;
        self.list_statuses(&[QueueClass::Stopped], offset, count, keys.as_deref())
    }

    pub(super) fn list_statuses(
        &self,
        classes: &[QueueClass],
        offset: i64,
        count: usize,
        keys: Option<&[String]>,
    ) -> Result<Value, HttpControlError> {
        let root = self.status.clone();
        let total = classes.iter().map(|class| root.queue(*class).len()).sum();
        let start = normalized_offset(offset, total);
        let mut values = ResultList::new();
        for gid in classes
            .iter()
            .flat_map(|class| root.queue(*class))
            .skip(start)
            .take(count)
        {
            let task = root.task(*gid).ok_or_else(|| {
                HttpControlError::Scheduler("queue index references a missing task".to_owned())
            })?;
            values.push_scratch(self.applied_status(task, keys)?)?;
        }
        Ok(values.finish())
    }

    pub(super) fn applied_status(
        &self,
        task: &ariax_runtime::AppliedTaskSnapshot,
        keys: Option<&[String]>,
    ) -> Result<Value, HttpControlError> {
        let snapshot = &task.snapshot;
        let status = snapshot
            .wire_status()
            .map_err(|_| HttpControlError::Scheduler("invalid public snapshot".to_owned()))?;
        let stats = self
            .stats
            .get(&task.task_id)
            .map(|stats| stats.snapshot())
            .unwrap_or_default();
        let mut value = status_value(snapshot, status, stats);
        #[cfg(feature = "bt")]
        if let Some(bt) = self.bt_task(snapshot.gid) {
            return self.bt_status(value, bt, keys);
        }
        self.add_slot_diagnostics(&mut value, snapshot, stats);
        if let Some(expansion) = self
            .task_spec(snapshot.gid)
            .and_then(|spec| spec.options().transfer.metalink_expansion.as_ref().cloned())
        {
            value["followedBy"] = json!(
                expansion
                    .children
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            );
        }
        debug_assert!(crate::rpc_json::owned_value_bytes(&value) < 256 * 1024);
        Ok(project_status(value, keys))
    }

    pub(super) fn get_uris(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        #[cfg(feature = "bt")]
        if self.bt_task(gid).is_some() {
            return Ok(json!([]));
        }
        let spec = self.task_spec(gid).ok_or(HttpControlError::NotFound)?;
        Ok(crate::rpc_result::to_value(
            &SourceUris {
                sources: spec.sources(),
                status: true,
            },
            RESULT_VALUE_BYTES,
        )?)
    }

    pub(super) fn get_files(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        #[cfg(feature = "bt")]
        if let Some(bt) = self.bt_task(gid) {
            return self.bt_files(bt);
        }
        let root = self.status.clone();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        let spec = self.task_spec(gid).ok_or(HttpControlError::NotFound)?;
        let stats = self
            .stats
            .get(&task.task_id)
            .map(|stats| stats.snapshot())
            .unwrap_or_default();
        let completed = task.snapshot.completed_length.max(stats.durable_bytes);
        let total = task
            .snapshot
            .total_length
            .unwrap_or(stats.total_length)
            .max(completed);
        let path = spec.output_root().join(spec.output().canonical_string());
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct File<'a> {
            index: &'static str,
            path: std::borrow::Cow<'a, str>,
            length: DisplayValue<u64>,
            completed_length: DisplayValue<u64>,
            selected: &'static str,
            uris: SourceUris<'a>,
        }
        Ok(crate::rpc_result::to_value(
            &[File {
                index: "1",
                path: path.to_string_lossy(),
                length: DisplayValue(total),
                completed_length: DisplayValue(completed),
                selected: "true",
                uris: SourceUris {
                    sources: spec.sources(),
                    status: true,
                },
            }],
            RESULT_VALUE_BYTES,
        )?)
    }

    pub(super) fn get_servers(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        #[cfg(feature = "bt")]
        if self.bt_task(gid).is_some() {
            return Ok(json!([]));
        }
        let spec = self.task_spec(gid).ok_or(HttpControlError::NotFound)?;
        Ok(crate::rpc_result::to_value(
            &SourceServers(spec.sources()),
            RESULT_VALUE_BYTES,
        )?)
    }

    pub(super) fn get_option(&self, params: Value) -> Result<Value, HttpControlError> {
        let gid = self.resolve_gid_param(&params)?;
        #[cfg(feature = "bt")]
        if let Some(bt) = self.bt_task(gid) {
            return crate::rpc_result::to_value(
                &OptionMap(&bt.spec.options.persisted),
                RESULT_VALUE_BYTES,
            )
            .map_err(Into::into);
        }
        let spec = self.task_spec(gid).ok_or(HttpControlError::NotFound)?;
        let options = spec
            .persistence_options()
            .map_err(HttpControlError::TaskSpec)?;
        Ok(crate::rpc_result::to_value(
            &OptionMap(&options),
            RESULT_VALUE_BYTES,
        )?)
    }

    pub(super) fn get_global_option(&self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "getGlobalOption")?;
        Ok(string_map_value(
            self.global_options
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        )?)
    }

    pub(super) fn get_version(&self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "getVersion")?;
        let mut features = vec!["HTTP", "HTTPS", "JSON-RPC", "Session", "Async DNS"];
        if cfg!(feature = "bt") {
            features.push("BitTorrent");
        }
        if cfg!(feature = "metalink") {
            features.push("Metalink");
        }
        if cfg!(feature = "ftp") {
            features.push("FTP");
        }
        if cfg!(feature = "sftp") {
            features.push("SFTP");
        }
        Ok(json!({
            "version": env!("CARGO_PKG_VERSION"),
            "enabledFeatures": features,
        }))
    }

    pub(super) fn get_session_info(&self, params: Value) -> Result<Value, HttpControlError> {
        require_no_params(&params, "getSessionInfo")?;
        Ok(json!({"sessionId": self.session_id.to_string()}))
    }

    pub(super) fn export_session(&self, params: Value) -> Result<Value, HttpControlError> {
        let args = params.as_array().filter(|args| args.len() <= 1).ok_or(
            HttpControlError::InvalidParams("exportSession accepts an optional format"),
        )?;
        let format = args
            .first()
            .map(|value| {
                crate::SessionFormat::parse(value.as_str().ok_or(
                    HttpControlError::InvalidParams("session format must be text"),
                )?)
            })
            .transpose()?
            .unwrap_or_default();
        let root = self.status.clone();
        if root.len() > MAX_RPC_LIST_ITEMS {
            return Err(HttpControlError::Busy);
        }
        let mut tasks = ResultList::new();

        for applied in root.tasks().values() {
            if matches!(
                applied.snapshot.wire_status(),
                Ok(Aria2Status::Complete | Aria2Status::Removed)
            ) {
                continue;
            }
            #[cfg(feature = "bt")]
            if let Some(bt) = self.bt_task(applied.snapshot.gid) {
                use crate::rpc_result::Base64Value;
                #[derive(serde::Serialize)]
                #[serde(rename_all = "camelCase")]
                struct Metadata<'a> {
                    identity: &'a ariax_bt::BtIdentity,
                    metainfo: DisplayValue<Base64Value<'a>>,
                    magnet: &'a Option<String>,
                    files: &'a [ariax_storage::SessionBtFile],
                    resume_data: DisplayValue<Base64Value<'a>>,
                }
                #[derive(serde::Serialize)]
                struct Task<'a> {
                    kind: &'static str,
                    gid: DisplayValue<Gid>,
                    state: &'static str,
                    options: SessionOptions<'a>,
                    bittorrent: Metadata<'a>,
                }
                let binding = &bt.spec.record.binding;
                tasks.push(&Task {
                    kind: "bittorrent",
                    gid: DisplayValue(applied.snapshot.gid),
                    state: applied.snapshot.state.code(),
                    options: SessionOptions {
                        options: &bt.spec.options.persisted,
                        paused: applied.snapshot.desired_paused,
                    },
                    bittorrent: Metadata {
                        identity: &binding.identity,
                        metainfo: DisplayValue(Base64Value(&binding.metainfo)),
                        magnet: &binding.magnet,
                        files: &binding.files,
                        resume_data: DisplayValue(Base64Value(
                            bt.resume_data
                                .as_deref()
                                .map_or(&[], |resume| &resume.bytes),
                        )),
                    },
                })?;
                continue;
            }
            let spec = self
                .tasks
                .get(applied.task_id)
                .ok_or(HttpControlError::NotFound)?;
            if spec.options().transfer.metalink_expansion.is_some() {
                continue;
            }
            let options = spec
                .persistence_options()
                .map_err(HttpControlError::TaskSpec)?;
            #[derive(serde::Serialize)]
            struct Task<'a> {
                kind: &'static str,
                gid: DisplayValue<Gid>,
                uris: PersistedUris<'a>,
                sources: PersistedSources<'a>,
                options: SessionOptions<'a>,
                state: &'static str,
                #[serde(skip_serializing_if = "Option::is_none")]
                verification: Option<crate::verification_document::VerificationView<'a>>,
            }
            tasks.push(&Task {
                kind: "transfer",
                gid: DisplayValue(applied.snapshot.gid),
                uris: PersistedUris(spec.sources()),
                sources: PersistedSources(spec.sources()),
                options: SessionOptions {
                    options: &options,
                    paused: applied.snapshot.desired_paused,
                },
                state: applied.snapshot.state.code(),
                verification: spec.verification().map(|manifest| {
                    crate::verification_document::VerificationView {
                        manifest,
                        index: spec.metalink_index(),
                    }
                }),
            })?;
        }
        let mut result = serde_json::Map::new();
        result.insert(
            "sessionId".to_owned(),
            Value::String(self.session_id.to_string()),
        );
        result.insert("tasks".to_owned(), tasks.finish());
        result.insert("formatVersion".to_owned(), Value::from(3));
        let document = Value::Object(result);
        if format == crate::SessionFormat::Aria2 {
            let bytes = crate::session_file::render(&document, format)?;
            return String::from_utf8(bytes)
                .map(Value::String)
                .map_err(|_| HttpControlError::InvalidConfig);
        }
        Ok(document)
    }

    pub(super) fn resolve_gid_param(&self, params: &Value) -> Result<Gid, HttpControlError> {
        let values = params.as_array().filter(|values| values.len() == 1).ok_or(
            HttpControlError::InvalidParams("exactly one hexadecimal GID is required"),
        )?;
        let value = values[0].as_str().ok_or(HttpControlError::InvalidParams(
            "a hexadecimal GID is required",
        ))?;
        self.resolve_gid_text(value)
    }

    pub(super) fn resolve_gid_and_keys(
        &self,
        params: &Value,
    ) -> Result<(Gid, Option<Vec<String>>), HttpControlError> {
        let values = params
            .as_array()
            .filter(|values| (1..=2).contains(&values.len()))
            .ok_or(HttpControlError::InvalidParams(
                "tellStatus requires GID and optional key array",
            ))?;
        let value = values[0].as_str().ok_or(HttpControlError::InvalidParams(
            "a hexadecimal GID is required",
        ))?;
        let keys = values.get(1).map(parse_keys).transpose()?;
        Ok((self.resolve_gid_text(value)?, keys))
    }

    pub(super) fn resolve_gid_text(&self, value: &str) -> Result<Gid, HttpControlError> {
        resolve_gid(&self.status, value)
    }

    pub(super) fn global_stat(&self, params: Value) -> Result<Value, HttpControlError> {
        if !params.as_array().is_some_and(Vec::is_empty) {
            return Err(HttpControlError::InvalidParams(
                "getGlobalStat takes no params",
            ));
        }
        let root = self.status.clone();
        let mut active = 0_u64;
        let mut waiting = 0_u64;
        let mut stopped = 0_u64;
        let mut download_speed = 0_u64;
        let mut completed = 0_u64;
        for applied in root.tasks().values() {
            match applied.snapshot.wire_status().ok() {
                Some(Aria2Status::Active) => active += 1,
                Some(Aria2Status::Waiting | Aria2Status::Paused) => waiting += 1,
                Some(Aria2Status::Complete | Aria2Status::Error | Aria2Status::Removed) => {
                    stopped += 1
                }
                None => {}
            }
            let stats = self
                .stats
                .get(&applied.task_id)
                .map(|stats| stats.snapshot())
                .unwrap_or_default();
            download_speed = download_speed.saturating_add(stats.current_speed);
            completed = completed
                .saturating_add(applied.snapshot.completed_length.max(stats.durable_bytes));
        }
        Ok(json!({
            "downloadSpeed": download_speed.to_string(),
            "uploadSpeed": "0",
            "numActive": active.to_string(),
            "numWaiting": waiting.to_string(),
            "numStopped": stopped.to_string(),
            "completedLength": completed.to_string(),
        }))
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
                "effective" => (*self.global_options).clone(),
                "defaults" => default_global_options()?,
                "task-effective" => {
                    let gid = args.get(2).and_then(Value::as_str).ok_or(
                        HttpControlError::InvalidParams("task-effective dump requires a GID"),
                    )?;
                    let gid = self.resolve_gid_text(gid)?;
                    let spec = self.task_spec(gid).ok_or(HttpControlError::NotFound)?;
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
                    } else if configuration::layer_owns_option(&self.rpc_template, key) {
                        "rpc"
                    } else if configuration::layer_owns_option(&self.flat_options, key) {
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

    pub(super) fn add_slot_diagnostics(
        &self,
        value: &mut Value,
        snapshot: &TaskSnapshot,
        stats: HttpTransferStatsSnapshot,
    ) {
        let Some(task) = self
            .status
            .task(snapshot.gid)
            .and_then(ariax_runtime::AppliedTaskSnapshot::scheduler)
            .filter(|task| task.generation == snapshot.generation && task.state == snapshot.state)
        else {
            return;
        };
        let slot_state = match task.state {
            TaskState::WaitingSlow => "waitingSlow",
            TaskState::PausedSlow => "pausedSlow",
            TaskState::Paused => "pausedUser",
            TaskState::RetryWait => "retryWait",
            TaskState::Active if stats.retry_wait_until.is_some() => "retryWait",
            TaskState::Active => "active",
            TaskState::StoppedResult => "stopped",
            _ => "waiting",
        };
        let reason = if stats.local_pressure
            || matches!(
                stats.connection_condition,
                ConnectionCondition::Backpressured | ConnectionCondition::RateLimited
            ) {
            "backpressure"
        } else if matches!(task.state, TaskState::WaitingSlow | TaskState::PausedSlow) {
            "remoteSlow"
        } else if slot_state == "retryWait" {
            "retryWait"
        } else if task.desired_paused {
            "user"
        } else if stats.connection_condition == ConnectionCondition::Stalled {
            "stalled"
        } else {
            "none"
        };
        let now = MonotonicInstant::now();
        let slow_since = self
            .slow_observations
            .get(&task.gid)
            .and_then(|observation| observation.slow_since)
            .map_or(0, |since| {
                now_unix_ms().saturating_sub(now.duration_since(since).as_millis() as u64)
            });
        let readmit_after = task.slow_slot.map_or(0, |slot| {
            slot.decision.readmit_at.duration_since(now).as_millis() as u64
        });
        if let Some(object) = value.as_object_mut() {
            object.extend([
                ("slotState".to_owned(), json!(slot_state)),
                ("slotReason".to_owned(), json!(reason)),
                ("slowSince".to_owned(), json!(slow_since.to_string())),
                (
                    "demotionCount".to_owned(),
                    json!(task.slow_demotion_count.to_string()),
                ),
                ("readmitAfter".to_owned(), json!(readmit_after.to_string())),
                (
                    "retryWaitConsumesSlot".to_owned(),
                    json!(task.slot.owns_slot()),
                ),
            ]);
        }
    }
}

pub(super) fn resolve_gid(
    root: &ariax_runtime::StatusSnapshotRoot,
    value: &str,
) -> Result<Gid, HttpControlError> {
    resolve_gid_in_map(root.tasks(), value)
}

fn resolve_gid_in_map<T>(tasks: &BTreeMap<Gid, T>, value: &str) -> Result<Gid, HttpControlError> {
    if value.is_empty() || value.len() > 16 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HttpControlError::InvalidParams("invalid GID"));
    }
    let prefix = u64::from_str_radix(value, 16)
        .map_err(|_| HttpControlError::InvalidParams("invalid GID"))?;
    let shift = (16 - value.len()) * 4;
    let lower = prefix << shift;
    let upper = lower
        | if shift == 0 {
            0
        } else {
            u64::MAX >> (64 - shift)
        };
    let low = Gid::new(lower.max(1)).ok_or(HttpControlError::NotFound)?;
    let high = Gid::new(upper).ok_or(HttpControlError::NotFound)?;
    let mut matches = tasks.range(low..=high);
    let gid = *matches.next().ok_or(HttpControlError::NotFound)?.0;
    if matches.next().is_some() {
        return Err(HttpControlError::InvalidParams("GID prefix is ambiguous"));
    }
    Ok(gid)
}

#[cfg(test)]
#[derive(Default)]
struct ProjectionGate {
    entered: tokio::sync::Notify,
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl ProjectionGate {
    fn wait(&self) {
        self.entered.notify_one();
        let mut released = self.released.lock().expect("release flag");
        while !*released {
            released = self.changed.wait(released).expect("release signal");
        }
    }
    fn release(&self) {
        *self.released.lock().expect("release flag") = true;
        self.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_projection_keeps_its_workspace_until_background_work_finishes() {
        struct Release(Arc<ProjectionGate>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let directory = super::super::tests::TestDirectory::new();
        let plane = directory.control_plane();
        let reader = plane.query_reader();
        let gate = Arc::new(ProjectionGate::default());
        let release = Release(gate.clone());
        *reader.gate.lock().expect("gate") = Some(gate.clone());
        let client = plane.rpc_budgets.client().expect("client");
        let baseline = client.bytes();
        let request = client.try_request(0).expect("request");
        let task = tokio::spawn(async move {
            reader
                .call(
                    "aria2.getVersion",
                    json!([]),
                    crate::RpcClientContext::default().with_request(request),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .expect("projection entered");
        task.abort();
        let _ = task.await;
        assert_eq!(client.outstanding_requests(), 1);
        assert!(client.bytes() >= baseline + crate::rpc_budget::RPC_RESULT_WORKSPACE_BYTES);
        drop(release);
        let deadline = Instant::now() + Duration::from_secs(2);
        while client.bytes() != baseline {
            assert!(Instant::now() < deadline, "projection credit refund");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(client.outstanding_requests(), 0);
        plane.shutdown().expect("shutdown");
    }

    #[test]
    fn indexed_prefix_lookup_matches_the_scan_oracle_at_large_cardinality() {
        let mut seed = 0x9e3779b97f4a7c15_u64;
        let tasks: BTreeMap<_, _> = (0..1000)
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                (Gid::new(seed.max(1)).expect("nonzero GID"), ())
            })
            .collect();
        for (i, gid) in tasks.keys().step_by(4).enumerate() {
            let text = gid.to_string();
            let prefix = &text[..1 + i % 16];
            let expected: Vec<_> = tasks
                .keys()
                .filter(|candidate| candidate.to_string().starts_with(prefix))
                .copied()
                .collect();
            for input in [prefix.to_owned(), prefix.to_uppercase()] {
                match expected.as_slice() {
                    [only] => assert_eq!(
                        resolve_gid_in_map(&tasks, &input).expect("unique prefix"),
                        *only
                    ),
                    _ => assert!(matches!(
                        resolve_gid_in_map(&tasks, &input),
                        Err(HttpControlError::InvalidParams("GID prefix is ambiguous"))
                    )),
                }
            }
        }
        for invalid in ["", "xyz", "0123456789abcdef0", "é"] {
            assert!(matches!(
                resolve_gid_in_map(&tasks, invalid),
                Err(HttpControlError::InvalidParams("invalid GID"))
            ));
        }
        assert!(matches!(
            resolve_gid_in_map(&tasks, "0000000000000000"),
            Err(HttpControlError::NotFound)
        ));
    }
}
