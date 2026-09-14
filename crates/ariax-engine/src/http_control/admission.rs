//! One bounded filesystem preparation job and staged atomic admission.

use super::*;
use control_io::SessionWrites;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::task::Poll;

struct Preparation {
    configuration: Arc<query::ConfigurationSnapshot>,
    policy: Arc<dyn ariax_storage::PersistedOptionPolicy + Send + Sync>,
    session_id: SessionId,
    next_id: u64,
    scheduler: RequestScheduler,
}

struct Member {
    spec: HttpTaskSpec,
    metadata: SessionTaskMetadata,
    conditions: TaskConditions,
    appender: ControlJournalAppender,
}

struct Prepared {
    members: Vec<Member>,
    next_id: u64,
}

struct Revalidation {
    prepared: Prepared,
    scheduler: RequestScheduler,
    cursor: usize,
}

struct Finalized {
    catalogs: VecDeque<(HttpTaskSpec, ControlJournalAppender)>,
    first: ImportMember,
    remaining: VecDeque<ImportMember>,
    result: Value,
    next_id: u64,
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
    work: ControlWorkReservation,
    operation: impl FnOnce() -> Result<T, HttpControlError> + Send + 'static,
) -> Result<Receiver<Result<T, HttpControlError>>, HttpControlError> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("ariax-admission".to_owned())
        .spawn(move || {
            let _work = work;
            let result = operation();
            let _ = sender.send(result);
        })
        .map_err(|_| HttpControlError::Busy)?;
    Ok(receiver)
}

impl HttpControlPlane {
    pub(super) fn begin_admission(
        &mut self,
        params: Value,
        request: crate::rpc_budget::RpcRequestLease,
        import: bool,
    ) -> Result<ControlReply, HttpControlError> {
        if self.pending_admission.is_some()
            || self.pending_configuration.is_some()
            || !self.engine_idle()
            || self.pending_mutation.is_some()
            || (import && !self.pending_source_replacements.is_empty())
        {
            return Err(HttpControlError::Busy);
        }
        let work = self.reserve_scheduler_work(Some(&request), usize::from(!import))?;
        let preparation = Preparation {
            configuration: self.configuration_snapshot(),
            policy: self.engine.persisted_option_policy(),
            session_id: self.session_id,
            next_id: self.next_task_id,
            scheduler: self.engine.scheduler().clone(),
        };
        let retained_request = request.clone();
        #[cfg(test)]
        let gate = self.admission_gate.clone();
        let receiver = spawn(work.clone(), move || {
            #[cfg(test)]
            if let Some(gate) = gate {
                gate.wait();
            }
            preparation.prepare(params, &retained_request, import)
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
                if prepared.members.is_empty() {
                    self.turn.mark_progress();
                    let _ = pending
                        .reply
                        .take()
                        .expect("admission reply")
                        .send(Ok(json!([])));
                    return Ok(true);
                }
                if self.tasks.len().saturating_add(prepared.members.len())
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
                    let queue = if member.metadata.task.desired_paused {
                        QueueClass::Paused
                    } else {
                        QueueClass::Waiting
                    };
                    member.metadata.task.queue_position =
                        u32::try_from(validation.scheduler.queue_snapshot(queue).len())
                            .map_err(|_| HttpControlError::InvalidConfig)?;
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
                    },
                );
                if pending.import {
                    // Complete batch plan validation and materialization can
                    // visit all members; it also executes outside the owner.
                    pending.stage = Stage::Finalizing(spawn(pending.work.clone(), move || {
                        finalize(prepared, true)
                    })?);
                } else {
                    pending.stage = Stage::Installing(Box::new(finalize(prepared, false)?));
                }
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
                    self.journal_sequences.insert(spec.gid(), 2);
                    self.tasks.insert(spec).map_err(HttpControlError::Catalog)?;
                    self.turn.mark_progress();
                    return Ok(false);
                }
                if let Some((spec, appender)) = finalized.catalogs.pop_front() {
                    pending.installation_started = true;
                    pending.writes.unit(SessionCommand::InstallJournalAppender {
                        gid: spec.gid(),
                        appender,
                    });
                    pending.installing = Some(spec);
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
    SchedulerCommand::AddValidatedTask {
        task_id: member.spec.task(),
        gid: member.spec.gid(),
        desired_paused: member.metadata.task.desired_paused,
        conditions: member.conditions.clone(),
    }
}

fn effect(member: &Member) -> TransitionEffect {
    TransitionEffect::PersistTask {
        task_id: member.spec.task(),
        gid: member.spec.gid(),
        queue: if member.metadata.task.desired_paused {
            QueueClass::Paused
        } else {
            QueueClass::Waiting
        },
        position: member.metadata.task.queue_position as usize,
        desired_paused: member.metadata.task.desired_paused,
        slow_demotion_count: 0,
        conditions: member.conditions.clone(),
    }
}

fn finalize(prepared: Prepared, import: bool) -> Result<Finalized, HttpControlError> {
    let mut metadata = Vec::with_capacity(prepared.members.len());
    let mut members = VecDeque::with_capacity(prepared.members.len());
    let mut catalogs = VecDeque::with_capacity(prepared.members.len());
    let mut gids = Vec::with_capacity(prepared.members.len());
    for member in prepared.members {
        let step = if import {
            PersistencePlanStep::ConfirmTaskMetadata(Arc::new(member.metadata.clone()))
        } else {
            PersistencePlanStep::CreateTaskWithMetadata {
                task: member.metadata.task.clone(),
                sources: member.metadata.sources.clone(),
                options: member.metadata.options.clone(),
            }
        };
        let plan = PersistenceEffectPlan::new(effect(&member), vec![step])
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
        members.push_back(ImportMember {
            command: command(&member),
            plan,
        });
        metadata.push(member.metadata);
        gids.push(Value::String(member.spec.gid().to_string()));
        catalogs.push_back((member.spec, member.appender));
    }
    let mut first = members.pop_front().expect("nonempty admission");
    if import {
        first.plan = PersistenceEffectPlan::new(
            first.plan.effect().clone(),
            vec![PersistencePlanStep::CreateTaskBatch(metadata.into())],
        )
        .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
    }
    Ok(Finalized {
        catalogs,
        first,
        remaining: members,
        result: Value::Array(gids),
        next_id: prepared.next_id,
    })
}

impl Preparation {
    fn prepare(
        mut self,
        params: Value,
        request: &crate::rpc_budget::RpcRequestLease,
        import: bool,
    ) -> Result<Prepared, HttpControlError> {
        let imported = if import {
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
        let mut validated = Vec::with_capacity(imported.len());
        for task in imported {
            let task_id = self.next_available_task_id()?;
            self.next_id = task_id
                .get()
                .checked_add(1)
                .ok_or(HttpControlError::InvalidConfig)?;
            let gid = derive_http_gid(self.session_id, task_id);
            let options =
                self.configuration
                    .merged_add_options(task.options, &task.uris, import)?;
            let (options, root, output, paused) =
                parse_add_options(&options, &self.configuration.config.output_root, &task.uris)?;
            let spec = match task.sources {
                Some(sources) => HttpTaskSpec::from_persisted_sources(
                    task_id, gid, sources, root, output, options,
                ),
                None => HttpTaskSpec::new(task_id, gid, task.uris, root, output, options, false),
            }
            .map_err(HttpControlError::TaskSpec)?;
            let sanitized = spec
                .persistence_options()
                .map_err(HttpControlError::TaskSpec)?;
            if sanitized.entries().len() > ariax_storage::SESSION_MAX_OPTIONS_PER_TASK
                || !sanitized
                    .entries()
                    .all(|(name, _)| self.policy.permits(name))
                || !HttpTaskOptions::from_sanitized(&sanitized)
                    .is_ok_and(|value| &value == spec.options())
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
            validated.push((spec, sanitized, paused, position, conditions));
        }
        // No journal exists until syntax, all task policies, and the complete
        // provisional scheduler sequence have passed preflight.
        let mut members = Vec::with_capacity(validated.len());
        for (spec, sanitized, paused, position, conditions) in validated {
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
            members.push(Member {
                spec,
                metadata,
                conditions,
                appender,
            });
        }
        Ok(Prepared {
            members,
            next_id: self.next_id,
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
