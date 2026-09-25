//! One bounded filesystem preparation job and staged atomic admission.

use super::*;
use control_io::SessionWrites;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::task::Poll;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum AdmissionKind {
    Uri,
    Session,
    Metalink,
    Follow(ariax_storage::MetalinkParent),
}

struct Preparation {
    configuration: Arc<query::ConfigurationSnapshot>,
    policy: Arc<dyn ariax_storage::PersistedOptionPolicy + Send + Sync>,
    session_id: SessionId,
    next_id: u64,
    scheduler: RequestScheduler,
    local_admin: bool,
    tasks: Arc<crate::HttpTaskCatalog>,
    #[cfg(feature = "bt")]
    bt_catalog: Arc<BTreeMap<Gid, Arc<super::bittorrent::QueryTask>>>,
    #[cfg(feature = "bt")]
    bt_resources: ariax_bt::BtResources,
    #[cfg(feature = "bt")]
    bt_config: ariax_bt::BtAdapterConfig,
}

struct TransferMember {
    spec: HttpTaskSpec,
    metadata: SessionTaskMetadata,
    conditions: TaskConditions,
    appender: ControlJournalAppender,
    requested_position: Option<usize>,
}

enum Member {
    Transfer(TransferMember),
    #[cfg(feature = "bt")]
    BitTorrent(Arc<super::bittorrent::Spec>),
}

impl Member {
    fn task_id(&self) -> TaskId {
        match self {
            Self::Transfer(member) => member.spec.task(),
            #[cfg(feature = "bt")]
            Self::BitTorrent(spec) => spec.task_id,
        }
    }
    fn gid(&self) -> Gid {
        match self {
            Self::Transfer(member) => member.spec.gid(),
            #[cfg(feature = "bt")]
            Self::BitTorrent(spec) => spec.record.gid,
        }
    }
    fn paused(&self) -> bool {
        match self {
            Self::Transfer(member) => member.metadata.task.desired_paused,
            #[cfg(feature = "bt")]
            Self::BitTorrent(spec) => spec.record.desired_paused,
        }
    }
    fn position(&self) -> usize {
        match self {
            Self::Transfer(member) => member.metadata.task.queue_position as usize,
            #[cfg(feature = "bt")]
            Self::BitTorrent(spec) => spec.record.queue_position as usize,
        }
    }
    fn requested_position(&self) -> Option<usize> {
        match self {
            Self::Transfer(member) => member.requested_position,
            #[cfg(feature = "bt")]
            Self::BitTorrent(_) => None,
        }
    }
    fn conditions(&self) -> TaskConditions {
        match self {
            Self::Transfer(member) => member.conditions.clone(),
            #[cfg(feature = "bt")]
            Self::BitTorrent(_) => TaskConditions::default(),
        }
    }
    fn set_position(&mut self, position: u32) -> Result<(), HttpControlError> {
        match self {
            Self::Transfer(member) => member.metadata.task.queue_position = position,
            #[cfg(feature = "bt")]
            Self::BitTorrent(spec) => {
                let spec = Arc::get_mut(spec).ok_or(HttpControlError::Busy)?;
                Arc::get_mut(&mut spec.record)
                    .ok_or(HttpControlError::Busy)?
                    .queue_position = position;
            }
        }
        Ok(())
    }
}

enum Catalog {
    Transfer(HttpTaskSpec, ControlJournalAppender),
    #[cfg(feature = "bt")]
    BitTorrent(Arc<super::bittorrent::Spec>),
}

struct Prepared {
    members: Vec<Member>,
    next_id: u64,
    parent: Option<ariax_storage::MetalinkParent>,
    parent_spec: Option<Box<HttpTaskSpec>>,
}

struct Revalidation {
    prepared: Prepared,
    scheduler: RequestScheduler,
    cursor: usize,
}

struct Finalized {
    catalogs: VecDeque<Catalog>,
    first: ImportMember,
    remaining: VecDeque<ImportMember>,
    result: Value,
    next_id: u64,
    parent_spec: Option<Box<HttpTaskSpec>>,
}

enum Stage {
    Preparing(Receiver<Result<Prepared, HttpControlError>>),
    Ready(Prepared),
    Revalidating(Box<Revalidation>),
    Finalizing(Receiver<Result<Finalized, HttpControlError>>),
    Installing(Box<Finalized>),
}

pub(super) struct PendingAdmission {
    stage: Stage,
    import: bool,
    reply: Option<oneshot::Sender<Result<Value, HttpControlError>>>,
    work: ControlWorkReservation,
    request: crate::rpc_budget::RpcRequestLease,
    writes: SessionWrites,
    installing: Option<HttpTaskSpec>,
    installing_sequence: u64,
    installation_started: bool,
}

impl PendingAdmission {
    pub(super) fn fenced(&self) -> bool {
        !matches!(self.stage, Stage::Preparing(_) | Stage::Ready(_))
    }
}

fn receive<T>(
    receiver: &Receiver<Result<T, HttpControlError>>,
) -> Result<Option<T>, HttpControlError> {
    match receiver.try_recv() {
        Ok(result) => result.map(Some),
        Err(TryRecvError::Empty) => Ok(None),
        Err(TryRecvError::Disconnected) => Err(HttpControlError::Persistence(
            "admission preparation stopped".to_owned(),
        )),
    }
}

fn spawn<T: Send + 'static>(
    pool: &ariax_runtime::CpuPool,
    work: ControlWorkReservation,
    operation: impl FnOnce() -> Result<T, HttpControlError> + Send + 'static,
) -> Result<Receiver<Result<T, HttpControlError>>, HttpControlError> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let reservation = pool
        .reserve(64 * 1024)
        .map_err(|_| HttpControlError::Busy)?;
    drop(reservation.spawn(move || {
        let _work = work;
        let result = operation();
        let _ = sender.send(result);
    }));
    Ok(receiver)
}

impl HttpControlPlane {
    pub(super) fn begin_admission(
        &mut self,
        params: Value,
        request: crate::rpc_budget::RpcRequestLease,
        kind: AdmissionKind,
        local_admin: bool,
    ) -> Result<ControlReply, HttpControlError> {
        let import = kind != AdmissionKind::Uri;
        #[cfg(feature = "bt")]
        if self.bt.admission.is_some() {
            return Err(HttpControlError::Busy);
        }
        if self.pending_admission.is_some()
            || self.pending_configuration.is_some()
            || !self.engine_idle()
            || self.pending_mutation.is_some()
            || (import && !self.pending_source_replacements.is_empty())
        {
            return Err(HttpControlError::Busy);
        }
        let work = self.reserve_scheduler_work(Some(&request), usize::from(!import))?;
        #[cfg(feature = "bt")]
        self.ensure_bt_resources()?;
        let preparation = Preparation {
            #[cfg(feature = "bt")]
            bt_catalog: self.bt.catalog.clone(),
            #[cfg(feature = "bt")]
            bt_resources: self.bt.resources.clone().expect("BT resources"),
            #[cfg(feature = "bt")]
            bt_config: self.bt.config.clone(),
            configuration: self.configuration_snapshot(),
            policy: self.engine.persisted_option_policy(),
            session_id: self.session_id,
            next_id: self.next_task_id,
            scheduler: self.engine.scheduler().clone(),
            local_admin,
            tasks: self.tasks.snapshot(),
        };
        let retained_request = request.clone();
        #[cfg(test)]
        let gate = self.admission_gate.clone();
        let receiver = spawn(&self.cpu_pool, work.clone(), move || {
            #[cfg(test)]
            if let Some(gate) = gate {
                gate.wait();
            }
            preparation.prepare(params, &retained_request, kind)
        })?;
        let (reply, receiver_reply) = oneshot::channel();
        self.pending_admission = Some(PendingAdmission {
            stage: Stage::Preparing(receiver),
            import,
            reply: Some(reply),
            work,
            request,
            writes: SessionWrites::default(),
            installing: None,
            installing_sequence: 0,
            installation_started: false,
        });
        Ok(ControlReply::Deferred(receiver_reply))
    }

    pub(super) fn admission_fenced(&self) -> bool {
        self.pending_admission
            .as_ref()
            .is_some_and(PendingAdmission::fenced)
            || matches!(
                self.pending_mutation
                    .as_ref()
                    .map(|pending| &pending.publication),
                Some(MutationPublication::Import { .. })
            )
    }

    /// Returns true while publication owns the atomic mutation/admission fence.
    pub(super) fn poll_admission(&mut self) -> Result<bool, HttpControlError> {
        let Some(mut pending) = self.pending_admission.take() else {
            return Ok(false);
        };
        match self.poll_admission_stage(&mut pending) {
            Ok(true) => Ok(self.admission_fenced()),
            Ok(false) => {
                let fenced = pending.fenced();
                self.pending_admission = Some(pending);
                Ok(fenced)
            }
            Err(error) => {
                self.turn.mark_progress();
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(error.clone()));
                }
                if pending.installation_started {
                    self.engine.fail_control_publication();
                    Err(error)
                } else {
                    Ok(false)
                }
            }
        }
    }

    fn poll_admission_stage(
        &mut self,
        pending: &mut PendingAdmission,
    ) -> Result<bool, HttpControlError> {
        match &mut pending.stage {
            Stage::Preparing(receiver) => {
                if let Some(prepared) = receive(receiver)? {
                    pending.stage = Stage::Ready(prepared);
                    self.turn.mark_progress();
                }
            }
            Stage::Ready(prepared) => {
                if !self.engine_idle()
                    || self.pending_mutation.is_some()
                    || !self.pending_source_replacements.is_empty()
                {
                    return Ok(false);
                }
                if let Some(parent) = prepared.parent
                    && self.engine.scheduler().task(parent.gid).is_none_or(|task| {
                        task.generation != parent.generation
                            || task.state != ariax_core::TaskState::Active
                            || task.pending_barrier.is_some()
                    })
                {
                    return Err(HttpControlError::Busy);
                }
                if prepared.members.is_empty() {
                    self.turn.mark_progress();
                    let _ = pending
                        .reply
                        .take()
                        .expect("admission reply")
                        .send(Ok(json!([])));
                    return Ok(true);
                }
                if self
                    .engine
                    .snapshot_reader()
                    .load()
                    .len()
                    .saturating_add(prepared.members.len())
                    > self.config.task_capacity.get()
                {
                    return Err(HttpControlError::Busy);
                }
                // The preparation snapshot may precede urgent queue changes.
                // Revalidate every member against the current scheduler under
                // the fence, with exactly one member processed per turn.
                let prepared = std::mem::replace(
                    prepared,
                    Prepared {
                        members: Vec::new(),
                        next_id: 0,
                        parent: None,
                        parent_spec: None,
                    },
                );
                pending.stage = Stage::Revalidating(Box::new(Revalidation {
                    prepared,
                    scheduler: self.engine.scheduler().clone(),
                    cursor: 0,
                }));
                self.turn.mark_progress();
            }
            Stage::Revalidating(validation) => {
                if let Some(member) = validation.prepared.members.get_mut(validation.cursor) {
                    let queue = if member.paused() {
                        QueueClass::Paused
                    } else {
                        QueueClass::Waiting
                    };
                    member.set_position(
                        u32::try_from(
                            member
                                .requested_position()
                                .unwrap_or(usize::MAX)
                                .min(validation.scheduler.queue_snapshot(queue).len()),
                        )
                        .map_err(|_| HttpControlError::InvalidConfig)?,
                    )?;
                    validation
                        .scheduler
                        .execute_command_at(command(member), MonotonicInstant::now())
                        .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
                    validation.cursor += 1;
                    self.turn.mark_progress();
                    return Ok(false);
                }
                let prepared = std::mem::replace(
                    &mut validation.prepared,
                    Prepared {
                        members: Vec::new(),
                        next_id: 0,
                        parent: None,
                        parent_spec: None,
                    },
                );
                // The admission fence stops mapping publication while this
                // background job checks the current catalogs before installation.
                let tasks = self.tasks.snapshot();
                #[cfg(feature = "bt")]
                let bt = self.bt.catalog.clone();
                let import = pending.import;
                pending.stage =
                    Stage::Finalizing(spawn(&self.cpu_pool, pending.work.clone(), move || {
                        for member in &prepared.members {
                            match member {
                                Member::Transfer(member) => {
                                    if member.spec.verification().is_some() {
                                        preflight_output(
                                            &member.spec,
                                            tasks.entries().map(Arc::as_ref),
                                        )?;
                                    }
                                    #[cfg(feature = "bt")]
                                    super::bittorrent::collision_transfer(
                                        &member.spec,
                                        bt.values().map(|task| task.spec.as_ref()),
                                    )?;
                                }
                                #[cfg(feature = "bt")]
                                Member::BitTorrent(spec) => {
                                    super::bittorrent::collision(spec, &tasks, &bt)?
                                }
                            }
                        }
                        finalize(prepared, import)
                    })?);
                self.turn.mark_progress();
            }
            Stage::Finalizing(receiver) => {
                if let Some(finalized) = receive(receiver)? {
                    pending.stage = Stage::Installing(Box::new(finalized));
                    self.turn.mark_progress();
                }
            }
            Stage::Installing(finalized) => {
                if pending.installing.is_some() {
                    match pending.writes.poll(&self.session, &mut self.turn) {
                        Poll::Pending => return Ok(false),
                        Poll::Ready(result) => result?,
                    }
                    let spec = pending.installing.take().expect("installed journal task");
                    self.journal_sequences
                        .insert(spec.gid(), pending.installing_sequence);
                    self.tasks.insert(spec).map_err(HttpControlError::Catalog)?;
                    self.turn.mark_progress();
                    return Ok(false);
                }
                if let Some(catalog) = finalized.catalogs.pop_front() {
                    pending.installation_started = true;
                    match catalog {
                        Catalog::Transfer(spec, appender) => {
                            pending.installing_sequence = appender.appended_sequence();
                            pending.writes.unit(SessionCommand::InstallJournalAppender {
                                gid: spec.gid(),
                                appender,
                            });
                            pending.installing = Some(spec);
                        }
                        #[cfg(feature = "bt")]
                        Catalog::BitTorrent(spec) => {
                            self.engine.runtime_handle().register_bt_task(spec.task_id);
                            self.bt.install(spec);
                        }
                    }
                    self.turn.mark_progress();
                    return Ok(false);
                }
                self.next_task_id = finalized.next_id;
                self.prepare_and_begin(
                    finalized.first.plan.clone(),
                    finalized.first.command.clone(),
                )?;
                let publication = if pending.import {
                    MutationPublication::Import {
                        remaining: std::mem::take(&mut finalized.remaining),
                        result: std::mem::take(&mut finalized.result),
                        parent_spec: finalized.parent_spec.take(),
                    }
                } else {
                    let SchedulerCommand::AddValidatedTask { gid, .. } = finalized.first.command
                    else {
                        unreachable!()
                    };
                    MutationPublication::Admission {
                        gid,
                        readmission_started: false,
                    }
                };
                self.pending_mutation = Some(PendingMutation {
                    publication,
                    reply: pending.reply.take().expect("admission reply"),
                });
                self.retain_pending_work(Some(pending.work.clone()))?;
                self.turn.mark_progress();
                // The request is also held by work after this staging value is dropped.
                let _ = &pending.request;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn command(member: &Member) -> SchedulerCommand {
    if member.requested_position().is_some() {
        return SchedulerCommand::AddValidatedTaskAt {
            task_id: member.task_id(),
            gid: member.gid(),
            desired_paused: member.paused(),
            conditions: member.conditions(),
            position: member.position(),
        };
    }
    SchedulerCommand::AddValidatedTask {
        task_id: member.task_id(),
        gid: member.gid(),
        desired_paused: member.paused(),
        conditions: member.conditions(),
    }
}

fn effect(member: &Member) -> TransitionEffect {
    TransitionEffect::PersistTask {
        task_id: member.task_id(),
        gid: member.gid(),
        queue: if member.paused() {
            QueueClass::Paused
        } else {
            QueueClass::Waiting
        },
        position: member.position(),
        desired_paused: member.paused(),
        slow_demotion_count: 0,
        conditions: member.conditions(),
    }
}

fn finalize(prepared: Prepared, import: bool) -> Result<Finalized, HttpControlError> {
    let mut metadata = Vec::with_capacity(prepared.members.len());
    let mut members = VecDeque::with_capacity(prepared.members.len());
    let mut catalogs = VecDeque::with_capacity(prepared.members.len());
    let mut gids = Vec::with_capacity(prepared.members.len());
    for member in prepared.members {
        let command = command(&member);
        let effect = effect(&member);
        gids.push(Value::String(member.gid().to_string()));
        let step = match member {
            Member::Transfer(member) => {
                let step = if import {
                    PersistencePlanStep::ConfirmTaskMetadata(Arc::new(member.metadata.clone()))
                } else {
                    PersistencePlanStep::CreateTaskWithMetadata {
                        task: member.metadata.task.clone(),
                        sources: member.metadata.sources.clone(),
                        options: member.metadata.options.clone(),
                    }
                };
                metadata.push(ariax_storage::SessionAdmissionMetadata::Transfer(
                    member.metadata,
                ));
                catalogs.push_back(Catalog::Transfer(member.spec, member.appender));
                step
            }
            #[cfg(feature = "bt")]
            Member::BitTorrent(spec) => {
                let resume = spec
                    .resume_data
                    .as_ref()
                    .map_or_else(|| Arc::from([]), |data| data.bytes.clone());
                let step = PersistencePlanStep::ConfirmBtTask {
                    task: spec.record.clone(),
                    options: spec.options.persisted.clone(),
                    resume: Arc::clone(&resume),
                };
                metadata.push(ariax_storage::SessionAdmissionMetadata::BitTorrent {
                    task: (*spec.record).clone(),
                    options: spec.options.persisted.clone(),
                    resume,
                });
                catalogs.push_back(Catalog::BitTorrent(spec));
                step
            }
        };
        let plan = PersistenceEffectPlan::new(effect, vec![step])
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
        members.push_back(ImportMember { command, plan });
    }
    let mut first = members.pop_front().expect("nonempty admission");
    if import {
        let step = if let Some(parent) = prepared.parent {
            let tasks = metadata
                .into_iter()
                .map(|entry| match entry {
                    ariax_storage::SessionAdmissionMetadata::Transfer(task) => Ok(task),
                    _ => Err(HttpControlError::InvalidConfig),
                })
                .collect::<Result<Vec<_>, _>>()?;
            PersistencePlanStep::CreateFollowedMetalink {
                tasks: tasks.into(),
                parent,
            }
        } else {
            PersistencePlanStep::CreateSessionBatch(metadata.into())
        };
        first.plan = PersistenceEffectPlan::new(first.plan.effect().clone(), vec![step])
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
    }
    Ok(Finalized {
        catalogs,
        first,
        remaining: members,
        result: Value::Array(gids),
        next_id: prepared.next_id,
        parent_spec: prepared.parent_spec,
    })
}

impl Preparation {
    fn prepare(
        mut self,
        params: Value,
        request: &crate::rpc_budget::RpcRequestLease,
        kind: AdmissionKind,
    ) -> Result<Prepared, HttpControlError> {
        let import = kind == AdmissionKind::Session;
        let insertion = if matches!(kind, AdmissionKind::Metalink | AdmissionKind::Follow(_)) {
            params
                .get(2)
                .map(|value| parse_i64(value, "position"))
                .transpose()?
                .filter(|position| *position >= 0)
                .map(|position| {
                    usize::try_from(position).map_err(|_| {
                        HttpControlError::InvalidParams("position exceeds platform limit")
                    })
                })
                .transpose()?
        } else {
            None
        };
        let imported = if matches!(kind, AdmissionKind::Metalink | AdmissionKind::Follow(_)) {
            #[cfg(feature = "metalink")]
            {
                super::metalink_admission::parse_upload(params, request)?
            }
            #[cfg(not(feature = "metalink"))]
            {
                return Err(HttpControlError::Unsupported(
                    "Metalink feature unavailable",
                ));
            }
        } else if import {
            crate::session_file::parse_import(params, request)?
        } else {
            let values = params
                .as_array()
                .filter(|values| (1..=2).contains(&values.len()))
                .ok_or(HttpControlError::InvalidParams(
                    "addUri accepts a URI array and optional options object",
                ))?;
            vec![crate::session_file::ImportedTask {
                uris: parse_uri_array(&values[0])?,
                sources: None,
                options: values.get(1).cloned().unwrap_or_else(|| json!({})),
                ..Default::default()
            }]
        };
        if self.scheduler.len().saturating_add(imported.len())
            > self.configuration.config.task_capacity.get()
        {
            return Err(HttpControlError::Busy);
        }
        request
            .reserve(
                self.configuration
                    .configuration_defaults_bytes()
                    .saturating_add(8192)
                    .saturating_mul(imported.len()),
            )
            .map_err(|_| HttpControlError::Busy)?;
        let mut validated: Vec<(
            HttpTaskSpec,
            SanitizedOptionMap,
            bool,
            usize,
            TaskConditions,
            Option<usize>,
        )> = Vec::with_capacity(imported.len());
        #[cfg(feature = "bt")]
        let mut bt_validated = Vec::new();
        for (index, task) in imported.into_iter().enumerate() {
            let task_id = self.next_available_task_id()?;
            self.next_id = task_id
                .get()
                .checked_add(1)
                .ok_or(HttpControlError::InvalidConfig)?;
            #[cfg(feature = "bt")]
            if let Some(imported) = task.bittorrent {
                let spec = super::bittorrent::prepare_import(
                    imported,
                    self.configuration
                        .merged_bt_options(task.options, true, &self.bt_config)?,
                    task_id,
                    self.session_id,
                    &self.configuration.config.output_root,
                    &self.bt_resources.resident,
                    request,
                )?;
                if spec.options.settings.peers > self.bt_config.peers
                    || !spec
                        .options
                        .persisted
                        .entries()
                        .all(|(name, _)| self.policy.permits(name))
                {
                    return Err(HttpControlError::InvalidParams(
                        "BitTorrent import exceeds its configured policy",
                    ));
                }
                super::bittorrent::collision(&spec, &self.tasks, &self.bt_catalog)?;
                for entry in &validated {
                    super::bittorrent::collision_transfer(
                        &entry.0,
                        std::iter::once(spec.as_ref()),
                    )?;
                }
                self.scheduler
                    .execute_command_at(
                        SchedulerCommand::AddValidatedTask {
                            task_id,
                            gid: spec.record.gid,
                            desired_paused: true,
                            conditions: TaskConditions::default(),
                        },
                        MonotonicInstant::now(),
                    )
                    .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
                Arc::make_mut(&mut self.bt_catalog).insert(
                    spec.record.gid,
                    super::bittorrent::query_for_import(spec.clone()),
                );
                bt_validated.push(spec);
                continue;
            }
            let gid = derive_http_gid(self.session_id, task_id);
            let options =
                self.configuration
                    .merged_add_options(task.options, &task.uris, import)?;
            let (mut options, root, output, paused) = parse_add_options_authorized(
                &options,
                &self.configuration.config.output_root,
                &task.uris,
                self.local_admin,
            )?;
            if let Some(manifest) = &task.verification {
                options.transfer.verification_fingerprint = Some(manifest.fingerprint());
                options.piece_length = manifest.chunk_length();
            }
            let spec = match task.sources {
                Some(sources) => HttpTaskSpec::from_persisted_sources(
                    task_id, gid, sources, root, output, options,
                ),
                None => HttpTaskSpec::new(task_id, gid, task.uris, root, output, options, false),
            }
            .map_err(HttpControlError::TaskSpec)?;
            let spec = if let Some(manifest) = task.verification {
                spec.with_verification(manifest, task.metalink_index)
                    .map_err(HttpControlError::TaskSpec)?
            } else {
                spec
            };
            let spec = if let Some(priorities) = task.priorities {
                spec.with_source_priorities(&priorities)
                    .map_err(HttpControlError::TaskSpec)?
            } else {
                spec
            };
            let spec = self
                .tasks
                .reserve_spec(spec)
                .map_err(|_| HttpControlError::Busy)?;
            if spec.verification().is_some() {
                preflight_output(
                    &spec,
                    self.tasks
                        .entries()
                        .map(Arc::as_ref)
                        .chain(validated.iter().map(
                            |entry: &(
                                HttpTaskSpec,
                                SanitizedOptionMap,
                                bool,
                                usize,
                                TaskConditions,
                                Option<usize>,
                            )| &entry.0,
                        )),
                )?;
            }
            #[cfg(feature = "bt")]
            super::bittorrent::collision_transfer(
                &spec,
                self.bt_catalog.values().map(|entry| entry.spec.as_ref()),
            )?;
            let sanitized = spec
                .persistence_options()
                .map_err(HttpControlError::TaskSpec)?;
            if sanitized.entries().len() > ariax_storage::SESSION_MAX_OPTIONS_PER_TASK
                || !sanitized
                    .entries()
                    .all(|(name, _)| self.policy.permits(name))
                || !HttpTaskOptions::from_sanitized(&sanitized)
                    .is_ok_and(|value| value == spec.options().without_live_authority())
                || spec.sources().iter().any(|source| {
                    source
                        .persistence_safe_uri()
                        .is_some_and(|uri| uri.len() > ariax_storage::SESSION_MAX_SAFE_URI_BYTES)
                })
            {
                return Err(HttpControlError::InvalidParams(
                    "task cannot be recovered under the persistence policy",
                ));
            }
            let conditions = TaskConditions {
                needs_credentials: if spec.sources().iter().any(|source| source.uri().is_some()) {
                    None
                } else {
                    spec.persistence_sources()
                        .first()
                        .map(crate::credential_requirement)
                },
                no_space: None,
            };
            let queue = if paused {
                QueueClass::Paused
            } else {
                QueueClass::Waiting
            };
            let position = self.scheduler.queue_snapshot(queue).len();
            let requested_position = insertion.map(|position| position.saturating_add(index));
            self.scheduler
                .execute_command_at(
                    SchedulerCommand::AddValidatedTask {
                        task_id,
                        gid,
                        desired_paused: paused,
                        conditions: conditions.clone(),
                    },
                    MonotonicInstant::now(),
                )
                .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
            validated.push((
                spec,
                sanitized,
                paused,
                position,
                conditions,
                requested_position,
            ));
        }
        let parent_spec = if let AdmissionKind::Follow(parent) = kind {
            let spec = self
                .tasks
                .get_gid(parent.gid)
                .ok_or(HttpControlError::NotFound)?;
            if spec
                .persistence_options()
                .map_err(HttpControlError::TaskSpec)?
                .snapshot_hash()
                != parent.snapshot_hash
                || spec.options().transfer.metalink_expansion.is_some()
            {
                return Err(HttpControlError::Busy);
            }
            let mut options = spec.options().clone();
            options.transfer.metalink_expansion = Some(ariax_storage::MetalinkExpansion {
                parent,
                children: validated.iter().map(|entry| entry.0.gid()).collect(),
            });
            let replacement = spec
                .with_options(spec.output().clone(), options)
                .map_err(HttpControlError::TaskSpec)?;
            Some(Box::new(
                self.tasks
                    .reserve_spec(replacement)
                    .map_err(|_| HttpControlError::Busy)?,
            ))
        } else {
            None
        };
        // No journal exists until syntax, all task policies, and the complete
        // provisional scheduler sequence have passed preflight.
        let mut members = Vec::with_capacity(validated.len());
        for (spec, sanitized, paused, position, conditions, requested_position) in validated {
            let gid = spec.gid();
            let journal_id = derive_http_journal_id(spec.task(), gid);
            let directory = http_journal_directory(&self.configuration.config.journal_root, gid);
            let mut appender = ControlJournalAppender::create(
                &directory,
                gid,
                journal_id,
                Generation::INITIAL,
                now_unix_ms(),
            )
            .map_err(|error| HttpControlError::Journal(error.to_string()))?;
            append_initial_admission_with_options(
                &mut appender,
                Generation::INITIAL,
                sanitized.clone(),
            )
            .map_err(|error| HttpControlError::Journal(error.to_string()))?;
            if let Some(manifest) = spec.verification() {
                for payload in manifest.journal_payloads() {
                    let appended = appender
                        .append_payload(Generation::INITIAL, &payload)
                        .map_err(|error| HttpControlError::Journal(error.to_string()))?;
                    appender
                        .flush(appended.sequence())
                        .map_err(|error| HttpControlError::Journal(error.to_string()))?;
                }
            }
            let task = SessionTaskRecord {
                gid,
                session_id: self.session_id,
                queue_state: if paused {
                    SessionQueueState::Paused
                } else {
                    SessionQueueState::Waiting
                },
                queue_position: u32::try_from(position)
                    .map_err(|_| HttpControlError::InvalidConfig)?,
                desired_paused: paused,
                slow_demotion_count: 0,
                slow_slot: None,
                primary_journal_id: journal_id,
                primary_journal_path: PlatformPath::from_current(&directory)
                    .map_err(|error| HttpControlError::Journal(error.to_string()))?,
                replica_journal_path: None,
                replica_sequence: None,
                root_display: PlatformPath::from_current(spec.output_root()).map_err(|_| {
                    HttpControlError::InvalidParams("output root is not representable")
                })?,
                cached_layout_hash: None,
                cached_root_binding_hash: None,
                cached_snapshot_hash: sanitized.snapshot_hash(),
                no_space: None,
                created_ms: now_unix_ms(),
                updated_ms: now_unix_ms(),
            };
            let metadata = SessionTaskMetadata {
                task,
                sources: spec.persistence_sources(),
                options: sanitized,
            };
            members.push(Member::Transfer(TransferMember {
                spec,
                metadata,
                conditions,
                appender,
                requested_position,
            }));
        }
        #[cfg(feature = "bt")]
        members.extend(bt_validated.into_iter().map(Member::BitTorrent));
        members.sort_by_key(Member::task_id);
        Ok(Prepared {
            members,
            next_id: self.next_id,
            parent: match kind {
                AdmissionKind::Follow(parent) => Some(parent),
                _ => None,
            },
            parent_spec,
        })
    }

    fn next_available_task_id(&self) -> Result<TaskId, HttpControlError> {
        let mut candidate = self.next_id;
        for _ in 0..=ariax_storage::SESSION_MAX_TASKS {
            let task = TaskId::new(candidate).ok_or(HttpControlError::InvalidConfig)?;
            let gid = derive_http_gid(self.session_id, task);
            match std::fs::symlink_metadata(http_journal_directory(
                &self.configuration.config.journal_root,
                gid,
            )) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(task),
                Ok(_) => {
                    candidate = candidate
                        .checked_add(1)
                        .ok_or(HttpControlError::InvalidConfig)?
                }
                Err(_) => {
                    return Err(HttpControlError::Journal(
                        "cannot inspect journal destination".to_owned(),
                    ));
                }
            }
        }
        Err(HttpControlError::Busy)
    }
}

fn preflight_output<'a>(
    spec: &HttpTaskSpec,
    existing: impl Iterator<Item = &'a HttpTaskSpec>,
) -> Result<(), HttpControlError> {
    let name = spec.output().canonical_string().to_lowercase();
    for other in existing {
        if spec.output_root() == other.output_root() {
            let other = other.output().canonical_string().to_lowercase();
            if name == other
                || name.starts_with(&(other.clone() + "/"))
                || other.starts_with(&(name.clone() + "/"))
            {
                return Err(HttpControlError::InvalidParams(
                    "Metalink output collides with another task",
                ));
            }
        }
    }
    let mut path = spec.output_root().clone();
    let parts = spec.output().canonical_string();
    let mut parts = parts.split('/').peekable();
    while let Some(part) = parts.next() {
        // Check each existing directory component using portable case folding.
        match std::fs::read_dir(&path) {
            Ok(entries) => {
                for (index, entry) in entries.enumerate() {
                    if index >= 262_144 {
                        return Err(HttpControlError::Busy);
                    }
                    let entry = entry.map_err(|_| {
                        HttpControlError::InvalidParams("cannot inspect output directory")
                    })?;
                    if entry.file_name().to_string_lossy().to_lowercase() == part.to_lowercase() {
                        let kind = entry.file_type().map_err(|_| {
                            HttpControlError::InvalidParams("cannot inspect output entry")
                        })?;
                        if parts.peek().is_none() || !kind.is_dir() || entry.file_name() != part {
                            return Err(HttpControlError::InvalidParams(
                                "Metalink output already exists or collides",
                            ));
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(_) => {
                return Err(HttpControlError::InvalidParams(
                    "cannot inspect output directory",
                ));
            }
        }
        path.push(part);
    }
    Ok(())
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct PreparationGate {
    pub(super) entered: std::sync::atomic::AtomicBool,
    released: (std::sync::Mutex<bool>, std::sync::Condvar),
}

#[cfg(test)]
impl PreparationGate {
    fn wait(&self) {
        self.entered
            .store(true, std::sync::atomic::Ordering::Release);
        let (lock, wake) = &self.released;
        let released = lock.lock().expect("preparation gate");
        drop(
            wake.wait_while(released, |released| !*released)
                .expect("preparation gate"),
        );
    }

    pub(super) fn release(&self) {
        *self.released.0.lock().expect("preparation gate") = true;
        self.released.1.notify_all();
    }
}
