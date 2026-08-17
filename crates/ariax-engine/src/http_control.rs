//! The bounded Phase-3 HTTP control plane.
//!
//! The control plane owns the only mutable bridge between RPC requests,
//! scheduler effects, persisted task metadata, and the worker catalog.  RPC
//! transports never mutate the scheduler directly.

use crate::http_first_slice::append_initial_admission_with_options;
use crate::{
    HttpContentChecksum, HttpRetryAfterPolicy, HttpRetryBackoff, HttpRetryPolicy, HttpRetryProfile,
    HttpRetryStatusSet, HttpRetryTriggerSet, HttpRpcBackend, HttpRpcBackendError,
    HttpTaskCatalogError, HttpTaskOptions, HttpTaskSpec, HttpTaskSpecError, HttpTaskWorker,
    HttpTransferStatsSnapshot, HttpWorkerSupervisor, HttpWorkerSupervisorConfig,
    HttpWorkerSupervisorShutdown, MAX_HTTP_ENDGAME_MAX_DUPLICATES, PersistenceEffectPlan,
    PersistencePlanStep, ProcessDrainOutcome, SharedHttpTaskCatalog, SharedHttpTransferStats,
    derive_http_journal_id, http_journal_directory,
};
use ariax_core::{
    Aria2Status, Generation, Gid, MonotonicInstant, PublicError, QueueClass, QueueOrder,
    RequestScheduler, RetryClass, SchedulerCommand, TaskConditions, TaskEvent, TaskEventEnvelope,
    TaskId, TaskSnapshot, TransitionEffect,
};
use ariax_storage::{
    ControlJournalAppender, GenerationStartReason, JournalPayload, OptionsSnapshotScope,
    PathPlatform, PlatformPath, SafePathBuilder, SessionCommand, SessionCommandResult,
    SessionHandle, SessionId, SessionQueueOrder, SessionQueueState, SessionSlowSlotState,
    SessionStoppedResultRecord, SessionTaskRecord, SessionTerminalStatus, TaskRemoveReason,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const CONTROL_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_PROGRESS_POLL: Duration = Duration::from_micros(50);
const MAX_HTTP_RETRY_ATTEMPTS: u32 = 1024;
const MAX_HTTP_RETRY_WAIT_SECS: u64 = 600;
const MAX_HTTP_RETRY_ELAPSED_SECS: u64 = 7200;

/// Bounded process configuration needed by public HTTP task admission.
#[derive(Clone, Debug)]
pub struct HttpControlPlaneConfig {
    pub output_root: PathBuf,
    pub journal_root: PathBuf,
    pub task_capacity: NonZeroUsize,
    pub supervisor: HttpWorkerSupervisorConfig,
}

impl HttpControlPlaneConfig {
    pub fn validate(&self) -> Result<(), HttpControlError> {
        if !self.output_root.is_absolute()
            || self.output_root.as_os_str().is_empty()
            || !self.journal_root.is_absolute()
            || self.journal_root.as_os_str().is_empty()
        {
            return Err(HttpControlError::InvalidConfig);
        }
        self.supervisor
            .validate()
            .map_err(|_| HttpControlError::InvalidConfig)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum HttpControlError {
    InvalidConfig,
    InvalidParams(&'static str),
    TaskSpec(HttpTaskSpecError),
    Catalog(HttpTaskCatalogError),
    Persistence(String),
    Scheduler(String),
    Journal(String),
    NotFound,
    Unsupported(&'static str),
    Busy,
}

impl fmt::Display for HttpControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid HTTP control configuration"),
            Self::InvalidParams(message) => formatter.write_str(message),
            Self::TaskSpec(error) => error.fmt(formatter),
            Self::Catalog(error) => write!(formatter, "HTTP task catalog error: {error:?}"),
            Self::Persistence(error) => write!(formatter, "persistence failed: {error}"),
            Self::Scheduler(error) => write!(formatter, "scheduler rejected command: {error}"),
            Self::Journal(error) => write!(formatter, "journal failed: {error}"),
            Self::NotFound => formatter.write_str("task was not found"),
            Self::Unsupported(message) => formatter.write_str(message),
            Self::Busy => formatter.write_str("control plane is busy"),
        }
    }
}

impl Error for HttpControlError {}

/// Shared mutable control plane used by both transports.
pub struct HttpControlPlane {
    engine: crate::BootstrappedEngine,
    config: HttpControlPlaneConfig,
    tasks: SharedHttpTaskCatalog,
    stats: SharedHttpTransferStats,
    supervisor: Option<HttpWorkerSupervisor>,
    session: SessionHandle,
    session_id: SessionId,
    journal_sequences: BTreeMap<Gid, u64>,
    next_task_id: u64,
}

impl fmt::Debug for HttpControlPlane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpControlPlane")
            .field("task_count", &self.tasks.len())
            .field("supervisor_attached", &self.supervisor.is_some())
            .finish_non_exhaustive()
    }
}

impl HttpControlPlane {
    pub fn new(
        engine: crate::BootstrappedEngine,
        config: HttpControlPlaneConfig,
    ) -> Result<Self, HttpControlError> {
        config.validate()?;
        let tasks = SharedHttpTaskCatalog::new(config.task_capacity);
        let stats = SharedHttpTransferStats::new(config.task_capacity);
        let next_task_id = engine
            .snapshot_reader()
            .load()
            .tasks()
            .values()
            .map(|task| task.task_id.get())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        let mut plane = Self {
            session: engine.session_handle(),
            session_id: engine.session_id(),
            engine,
            config,
            tasks,
            stats,
            supervisor: None,
            journal_sequences: BTreeMap::new(),
            next_task_id,
        };
        plane.restore_catalog()?;
        Ok(plane)
    }

    /// Attaches the real HTTP worker supervisor.  The catalog is shared with
    /// the supervisor, but remains private until persistence succeeds.
    pub fn attach_worker(
        &mut self,
        worker: Arc<dyn HttpTaskWorker>,
    ) -> Result<(), HttpControlError> {
        if self.supervisor.is_some() {
            return Err(HttpControlError::InvalidConfig);
        }
        let supervisor = HttpWorkerSupervisor::new(
            self.engine.runtime_handle(),
            self.tasks.clone(),
            worker,
            self.config.supervisor,
        )
        .map_err(|_| HttpControlError::InvalidConfig)?;
        self.supervisor = Some(supervisor);
        Ok(())
    }

    pub fn shutdown(self) -> Result<crate::ProcessShutdownReport, crate::ProcessShutdownError> {
        let Self {
            engine,
            mut supervisor,
            ..
        } = self;
        let mut shutdown = engine.begin_shutdown()?;
        let drain = if let Some(supervisor) = supervisor.as_mut() {
            let active_workers = supervisor.active_workers();
            supervisor.cancel_all();
            if active_workers == 0 {
                ProcessDrainOutcome::Drained
            } else {
                ProcessDrainOutcome::Failed
            }
        } else {
            ProcessDrainOutcome::Drained
        };
        shutdown.complete_drain(drain)?;
        shutdown.finish()
    }

    /// Drains live HTTP workers before closing journals and the session owner.
    pub async fn shutdown_async(
        self,
    ) -> Result<crate::ProcessShutdownReport, crate::ProcessShutdownError> {
        let Self {
            engine, supervisor, ..
        } = self;
        let mut shutdown = engine.begin_shutdown()?;
        let drain_timeout = shutdown.drain_timeout();
        let drain = match supervisor {
            Some(supervisor) => match supervisor.shutdown_with_timeout(drain_timeout).await {
                HttpWorkerSupervisorShutdown::Drained => ProcessDrainOutcome::Drained,
                HttpWorkerSupervisorShutdown::TimedOut { .. } => ProcessDrainOutcome::TimedOut,
            },
            None => ProcessDrainOutcome::Drained,
        };
        shutdown.complete_drain(drain)?;
        shutdown.finish()
    }

    fn restore_catalog(&mut self) -> Result<(), HttpControlError> {
        let recovered = self.engine.recovered_tasks().to_vec();
        let records = match self.session.execute(SessionCommand::ReadTasks) {
            Ok(SessionCommandResult::Tasks(records)) => records
                .into_iter()
                .map(|record| (record.gid, record))
                .collect::<BTreeMap<_, _>>(),
            _ => BTreeMap::new(),
        };
        for recovered in recovered {
            let Some(record) = records.get(&recovered.gid) else {
                continue;
            };
            let Ok(output_root) = ariax_storage::platform_path_to_current(&record.root_display)
            else {
                continue;
            };
            let sources = match self
                .session
                .execute(SessionCommand::ReadTaskSources { gid: recovered.gid })
            {
                Ok(SessionCommandResult::TaskSources(sources)) => sources,
                _ => continue,
            };
            let uris = sources
                .into_iter()
                .filter_map(|source| source.persistence_safe_uri)
                .collect::<Vec<_>>();
            if uris.is_empty() {
                continue;
            }
            let persisted_options = match self.session.execute(SessionCommand::ReadTaskOptions {
                gid: recovered.gid,
                scope: OptionsSnapshotScope::CurrentGeneration,
            }) {
                Ok(SessionCommandResult::TaskOptions(options)) => options,
                _ => continue,
            };
            let options = match HttpTaskOptions::from_sanitized(&persisted_options) {
                Ok(options) => options,
                Err(_) => continue,
            };
            let output = HttpTaskSpec::persisted_output(&persisted_options).or_else(|_| {
                recovered
                    .journal
                    .layout()
                    .and_then(|layout| layout.layout().files().first())
                    .map(|file| file.safe_path().clone())
                    .ok_or(HttpTaskSpecError::InvalidOptions)
            });
            let Ok(output) = output else {
                continue;
            };
            let spec = match HttpTaskSpec::new(
                recovered.task_id,
                recovered.gid,
                uris,
                output_root,
                output,
                options,
                false,
            ) {
                Ok(spec) => spec,
                Err(_) => continue,
            };
            if self.tasks.insert(spec).is_ok() {
                self.journal_sequences
                    .insert(recovered.gid, recovered.journal.last_sequence());
                let _ = self.stats.get_or_create(recovered.task_id);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn task_catalog(&self) -> SharedHttpTaskCatalog {
        self.tasks.clone()
    }

    #[must_use]
    pub fn stats_catalog(&self) -> SharedHttpTransferStats {
        self.stats.clone()
    }

    #[must_use]
    pub fn session_handle(&self) -> SessionHandle {
        self.session.clone()
    }

    /// Performs one bounded engine/supervisor progress turn.
    pub fn poll_once(&mut self) -> Result<(), HttpControlError> {
        let now = MonotonicInstant::now();
        if let Some(supervisor) = self.supervisor.as_mut() {
            supervisor
                .poll_once(now)
                .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        }
        if self.engine.is_idle()
            && let Some(event) = self.engine.runtime_handle().poll_event_at(now)
        {
            self.prepare_runtime_event(&event, now)?;
            self.engine
                .handle_event_at(&event, now)
                .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
            self.drive_engine()?;
        }
        if self.engine.is_idle() {
            self.try_admit_one(now)?;
        }
        self.drive_engine()
    }

    pub fn call(&mut self, method: &str, params: Value) -> Result<Value, HttpControlError> {
        match method {
            "aria2.addUri" | "addUri" => self.add_uri(params),
            "aria2.tellStatus" | "tellStatus" => self.tell_status(params),
            "aria2.pause" | "pause" => self.pause(params),
            "aria2.remove" | "remove" => self.remove(params),
            "aria2.getGlobalStat" | "getGlobalStat" => self.global_stat(params),
            _ => Err(HttpControlError::Unsupported("method not found")),
        }
    }

    fn add_uri(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let array = params.as_array().ok_or(HttpControlError::InvalidParams(
            "addUri params must be an array",
        ))?;
        if !(1..=2).contains(&array.len()) {
            return Err(HttpControlError::InvalidParams(
                "addUri accepts a URI array and optional options object",
            ));
        }
        let uris =
            array
                .first()
                .and_then(Value::as_array)
                .ok_or(HttpControlError::InvalidParams(
                    "addUri requires a URI array",
                ))?;
        let uris = uris
            .iter()
            .map(|uri| {
                uri.as_str()
                    .map(str::to_owned)
                    .ok_or(HttpControlError::InvalidParams("URI must be a string"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if uris.is_empty() {
            return Err(HttpControlError::InvalidParams(
                "addUri requires at least one URI",
            ));
        }
        let options = array.get(1).cloned().unwrap_or_else(|| json!({}));
        let (spec, record, plan, appender) = self.build_admission(uris, options)?;
        let task_id = spec.task();
        let gid = spec.gid();

        // Keep the catalog private to this serialized control call while the
        // journal and SQLite admission are committed in order.
        let inserted = self.tasks.insert(spec).map_err(HttpControlError::Catalog)?;
        if let Err(error) = self.install_journal(gid, appender) {
            self.tasks.remove(task_id);
            return Err(error);
        }
        if let Err(error) = self.prepare_and_execute(
            plan,
            SchedulerCommand::AddValidatedTask {
                task_id,
                gid,
                desired_paused: record.desired_paused,
                conditions: TaskConditions::default(),
            },
        ) {
            self.tasks.remove(task_id);
            let _ = self.session.execute(SessionCommand::CloseJournal { gid });
            return Err(error);
        }
        self.journal_sequences.insert(gid, 2);
        self.try_admit_one(MonotonicInstant::now())?;
        self.drive_engine()?;
        Ok(Value::String(inserted.gid().to_string()))
    }

    fn pause(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let gid = parse_gid_param(&params)?;
        self.execute_control_command(SchedulerCommand::Pause { gid, force: false })?;
        Ok(Value::String(gid.to_string()))
    }

    fn remove(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let gid = parse_gid_param(&params)?;
        self.execute_control_command(SchedulerCommand::Remove { gid, force: false })?;
        Ok(Value::String(gid.to_string()))
    }

    fn tell_status(&mut self, params: Value) -> Result<Value, HttpControlError> {
        let gid = parse_gid_param(&params)?;
        let root = self.engine.snapshot_reader().load();
        let task = root.task(gid).ok_or(HttpControlError::NotFound)?;
        let snapshot = &task.snapshot;
        let status = snapshot
            .wire_status()
            .map_err(|_| HttpControlError::Scheduler("invalid public snapshot".to_owned()))?;
        let stats = self
            .stats
            .get(task.task_id)
            .map(|stats| stats.snapshot())
            .unwrap_or_default();
        Ok(status_value(snapshot, status, stats))
    }

    fn global_stat(&mut self, params: Value) -> Result<Value, HttpControlError> {
        if !params.as_array().is_some_and(Vec::is_empty) {
            return Err(HttpControlError::InvalidParams(
                "getGlobalStat takes no params",
            ));
        }
        let root = self.engine.snapshot_reader().load();
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
                .get(applied.task_id)
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

    fn execute_control_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<(), HttpControlError> {
        self.prepare_and_execute_command(command)
    }

    fn prepare_and_execute_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<(), HttpControlError> {
        let mut simulation = self.engine.scheduler().clone();
        let outcome = simulation
            .execute_command_at(command.clone(), MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        self.prepare_outcome_plans(&mut simulation, outcome.effects, None)?;
        self.engine
            .execute_command_at(command, MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
        self.drive_engine()
    }

    fn prepare_and_execute(
        &mut self,
        plan: PersistenceEffectPlan,
        command: SchedulerCommand,
    ) -> Result<(), HttpControlError> {
        self.engine
            .prepare_persistence(plan)
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
        self.engine
            .execute_command_at(command, MonotonicInstant::now())
            .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
        self.drive_engine()
    }

    fn drive_engine(&mut self) -> Result<(), HttpControlError> {
        let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
        loop {
            match self.engine.poll_at(MonotonicInstant::now()) {
                ariax_runtime::SchedulerDriverPoll::Idle
                | ariax_runtime::SchedulerDriverPoll::Completed { .. } => {
                    if self.engine.is_idle() {
                        return Ok(());
                    }
                }
                ariax_runtime::SchedulerDriverPoll::Progressed
                | ariax_runtime::SchedulerDriverPoll::WaitingForCompletion { .. }
                | ariax_runtime::SchedulerDriverPoll::Backpressured { .. } => {}
                ariax_runtime::SchedulerDriverPoll::Faulted(error) => {
                    return Err(HttpControlError::Scheduler(format!("{error:?}")));
                }
            }
            if Instant::now() >= deadline {
                return Err(HttpControlError::Busy);
            }
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
    }

    fn try_admit_one(&mut self, now: MonotonicInstant) -> Result<(), HttpControlError> {
        if self.supervisor.is_none() {
            return Ok(());
        }
        let mut simulation = self.engine.scheduler().clone();
        let outcome = match simulation.admit_next_at(now) {
            Ok(outcome) => outcome,
            Err(_) => return Ok(()),
        };
        self.prepare_outcome_plans(
            &mut simulation,
            outcome.effects,
            Some(GenerationStartReason::RetryReadmission),
        )?;
        self.engine
            .admit_next_at(now)
            .map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))?;
        Ok(())
    }

    fn prepare_outcome_plans(
        &mut self,
        simulation: &mut RequestScheduler,
        effects: Vec<TransitionEffect>,
        generation_reason: Option<GenerationStartReason>,
    ) -> Result<(), HttpControlError> {
        let mut queue = VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            if effect.kind().persistence_effect() {
                self.engine
                    .prepare_persistence(self.plan_for_effect(&effect, generation_reason)?)
                    .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
                if let Some(ack) = persistence_ack(&effect) {
                    let outcome = simulation
                        .handle_event_at(&ack, MonotonicInstant::now())
                        .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
                    queue.extend(outcome.effects);
                }
            }
        }
        Ok(())
    }

    fn prepare_runtime_event(
        &mut self,
        event: &TaskEventEnvelope,
        at: MonotonicInstant,
    ) -> Result<(), HttpControlError> {
        let mut simulation = self.engine.scheduler().clone();
        let outcome = simulation
            .handle_event_at(event, at)
            .map_err(|error| HttpControlError::Scheduler(error.to_string()))?;
        let generation_reason = match event.event() {
            TaskEvent::RetryReady { .. } => Some(GenerationStartReason::RetryReadmission),
            TaskEvent::ActiveRepresentationRestart { .. } => {
                Some(GenerationStartReason::RepresentationRestart)
            }
            _ => None,
        };
        self.prepare_outcome_plans(&mut simulation, outcome.effects, generation_reason)
    }

    fn plan_for_effect(
        &self,
        effect: &TransitionEffect,
        generation_reason: Option<GenerationStartReason>,
    ) -> Result<PersistenceEffectPlan, HttpControlError> {
        match effect {
            TransitionEffect::PersistTask { .. } => Err(HttpControlError::Unsupported(
                "task admission must use metadata plan",
            )),
            TransitionEffect::PersistGenerationStarted {
                task_id,
                gid,
                generation,
            } => {
                let steps = if *generation == Generation::INITIAL {
                    vec![PersistencePlanStep::FlushJournal {
                        gid: *gid,
                        through_sequence: self.journal_sequences.get(gid).copied().unwrap_or(2),
                    }]
                } else {
                    let options = self
                        .tasks
                        .get(*task_id)
                        .ok_or(HttpControlError::NotFound)?
                        .persistence_options()
                        .map_err(HttpControlError::TaskSpec)?;
                    let previous_generation = Generation::new(generation.get().saturating_sub(1));
                    let snapshot_hash = options.snapshot_hash();
                    vec![
                        PersistencePlanStep::AppendAndFlushJournal {
                            gid: *gid,
                            generation: previous_generation,
                            payload: JournalPayload::OptionsSnapshot {
                                scope: OptionsSnapshotScope::NextAdmission,
                                patch_id: None,
                                snapshot_hash,
                                options: options.clone(),
                            },
                        },
                        PersistencePlanStep::AppendAndFlushJournal {
                            gid: *gid,
                            generation: *generation,
                            payload: JournalPayload::GenerationStarted {
                                previous_generation,
                                reason: generation_reason
                                    .unwrap_or(GenerationStartReason::RecoveryRepair),
                                next_snapshot_hash: snapshot_hash,
                                patch_id: None,
                            },
                        },
                    ]
                };
                PersistenceEffectPlan::new(effect.clone(), steps)
                    .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
            TransitionEffect::PersistQueueTransition {
                task_id: _,
                gid,
                from: Some(from),
                to: Some(to),
                desired_paused,
                slow_demotion_count,
                slow_slot,
                orders,
            } => {
                let transition = ariax_storage::SessionQueueTransition {
                    gid: *gid,
                    expected_state: session_queue(*from),
                    target_state: session_queue(*to),
                    desired_paused: *desired_paused,
                    slow_demotion_count: *slow_demotion_count,
                    slow_slot: slow_slot.map(|slot| SessionSlowSlotState {
                        original_position: u32::try_from(slot.original_position)
                            .unwrap_or(u32::MAX),
                        retry: Some(ariax_storage::SessionSlowRetryDecision {
                            scheduled_at_ms: slot.decision.scheduled_at_ms,
                            delay_ms: slot.decision.delay_ms,
                        }),
                    }),
                    final_orders: orders
                        .iter()
                        .map(|order| SessionQueueOrder {
                            state: session_queue(order.class),
                            gids: order.order.clone(),
                        })
                        .collect(),
                    updated_ms: now_unix_ms(),
                };
                PersistenceEffectPlan::new(
                    effect.clone(),
                    vec![PersistencePlanStep::TransitionTaskQueue(transition)],
                )
                .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
            }
            TransitionEffect::PersistTerminal {
                task_id,
                gid,
                generation,
                status,
                error,
                from,
                to,
                desired_paused,
                slow_demotion_count,
                slow_slot,
                orders,
            } => self.terminal_plan(
                effect,
                *task_id,
                *gid,
                *generation,
                *status,
                error.as_ref(),
                *from,
                *to,
                *desired_paused,
                *slow_demotion_count,
                *slow_slot,
                orders,
            ),
            _ => Err(HttpControlError::Unsupported(
                "HTTP control effect is not implemented",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn terminal_plan(
        &self,
        effect: &TransitionEffect,
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        status: Aria2Status,
        error: Option<&PublicError>,
        from: QueueClass,
        to: QueueClass,
        desired_paused: bool,
        slow_demotion_count: u32,
        slow_slot: Option<ariax_core::SlowSlotPersistence>,
        orders: &[QueueOrder],
    ) -> Result<PersistenceEffectPlan, HttpControlError> {
        let transition = ariax_storage::SessionQueueTransition {
            gid,
            expected_state: session_queue(from),
            target_state: session_queue(to),
            desired_paused,
            slow_demotion_count,
            slow_slot: slow_slot.map(|slot| SessionSlowSlotState {
                original_position: u32::try_from(slot.original_position).unwrap_or(u32::MAX),
                retry: Some(ariax_storage::SessionSlowRetryDecision {
                    scheduled_at_ms: slot.decision.scheduled_at_ms,
                    delay_ms: slot.decision.delay_ms,
                }),
            }),
            final_orders: orders
                .iter()
                .map(|order| SessionQueueOrder {
                    state: session_queue(order.class),
                    gids: order.order.clone(),
                })
                .collect(),
            updated_ms: now_unix_ms(),
        };
        if status == Aria2Status::Complete && error.is_none() {
            let evidence = self
                .stats
                .completion(task_id)
                .ok_or(HttpControlError::Unsupported(
                    "successful terminal evidence is not available yet",
                ))?;
            let result = SessionStoppedResultRecord {
                gid,
                status: SessionTerminalStatus::Complete,
                error_kind: None,
                safe_message: String::new(),
                total_length: Some(evidence.total_length),
                layout_hash: Some(evidence.layout_hash),
                completed_ms: evidence.completed_at_unix_ms,
            };
            return PersistenceEffectPlan::new(
                effect.clone(),
                vec![
                    PersistencePlanStep::FlushJournal {
                        gid,
                        through_sequence: evidence.terminal_sequence,
                    },
                    PersistencePlanStep::PersistStoppedResult { result, transition },
                ],
            )
            .map_err(|error| HttpControlError::Persistence(format!("{error:?}")));
        }
        let (payload, result) = match (status, error) {
            (Aria2Status::Error, Some(error)) => (
                JournalPayload::TaskError {
                    error_class: error.kind(),
                    retriable: !matches!(
                        error.retry_class(),
                        RetryClass::Never | RetryClass::UserAction
                    ),
                    diagnostic_id: error.diagnostic_id().unwrap_or(0),
                },
                SessionStoppedResultRecord {
                    gid,
                    status: SessionTerminalStatus::Error,
                    error_kind: Some(error.kind()),
                    safe_message: error.safe_message().to_owned(),
                    total_length: None,
                    layout_hash: None,
                    completed_ms: now_unix_ms(),
                },
            ),
            (Aria2Status::Removed, None) => (
                JournalPayload::TaskRemoved {
                    reason: TaskRemoveReason::User,
                },
                SessionStoppedResultRecord {
                    gid,
                    status: SessionTerminalStatus::Removed,
                    error_kind: None,
                    safe_message: String::new(),
                    total_length: None,
                    layout_hash: None,
                    completed_ms: now_unix_ms(),
                },
            ),
            _ => {
                return Err(HttpControlError::Unsupported(
                    "terminal result does not match scheduler status",
                ));
            }
        };
        PersistenceEffectPlan::new(
            effect.clone(),
            vec![
                PersistencePlanStep::AppendAndFlushJournal {
                    gid,
                    generation,
                    payload,
                },
                PersistencePlanStep::PersistStoppedResult { result, transition },
            ],
        )
        .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))
    }

    fn build_admission(
        &mut self,
        uris: Vec<String>,
        options: Value,
    ) -> Result<
        (
            HttpTaskSpec,
            SessionTaskRecord,
            PersistenceEffectPlan,
            ControlJournalAppender,
        ),
        HttpControlError,
    > {
        let task_id = TaskId::new(self.next_task_id).ok_or(HttpControlError::InvalidConfig)?;
        self.next_task_id = self
            .next_task_id
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        let gid = derive_http_gid(self.session_id, task_id);
        let (http_options, output_root, output, paused) =
            parse_add_options(&options, &self.config.output_root, &uris)?;
        let spec = HttpTaskSpec::new(
            task_id,
            gid,
            uris,
            output_root.clone(),
            output,
            http_options,
            false,
        )
        .map_err(HttpControlError::TaskSpec)?;
        let sanitized = spec
            .persistence_options()
            .map_err(HttpControlError::TaskSpec)?;
        let journal_id = derive_http_journal_id(task_id, gid);
        let journal_directory = http_journal_directory(&self.config.journal_root, gid);
        let mut appender = ControlJournalAppender::create(
            &journal_directory,
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
        let queue = if paused {
            SessionQueueState::Paused
        } else {
            SessionQueueState::Waiting
        };
        let queue_class = if paused {
            QueueClass::Paused
        } else {
            QueueClass::Waiting
        };
        let position = self.engine.scheduler().queue_snapshot(queue_class).len();
        let record = SessionTaskRecord {
            gid,
            session_id: self.session_id,
            queue_state: queue,
            queue_position: u32::try_from(position).map_err(|_| HttpControlError::InvalidConfig)?,
            desired_paused: paused,
            slow_demotion_count: 0,
            slow_slot: None,
            primary_journal_id: journal_id,
            primary_journal_path: PlatformPath::from_current(&journal_directory)
                .map_err(|error| HttpControlError::Journal(error.to_string()))?,
            replica_journal_path: None,
            replica_sequence: None,
            root_display: PlatformPath::from_current(&output_root).map_err(|_error| {
                HttpControlError::InvalidParams("output root is not representable")
            })?,
            cached_layout_hash: None,
            cached_root_binding_hash: None,
            cached_snapshot_hash: sanitized.snapshot_hash(),
            no_space: None,
            created_ms: now_unix_ms(),
            updated_ms: now_unix_ms(),
        };
        let effect = TransitionEffect::PersistTask {
            task_id,
            gid,
            queue: queue_class,
            position,
            desired_paused: paused,
            slow_demotion_count: 0,
            conditions: TaskConditions::default(),
        };
        let plan = PersistenceEffectPlan::new(
            effect,
            vec![PersistencePlanStep::CreateTaskWithMetadata {
                task: record.clone(),
                sources: spec.persistence_sources(),
                options: sanitized,
            }],
        )
        .map_err(|error| HttpControlError::Persistence(format!("{error:?}")))?;
        Ok((spec, record, plan, appender))
    }

    fn install_journal(
        &self,
        gid: Gid,
        appender: ControlJournalAppender,
    ) -> Result<(), HttpControlError> {
        match self
            .session
            .execute(SessionCommand::InstallJournalAppender { gid, appender })
        {
            Ok(SessionCommandResult::Unit) => Ok(()),
            Ok(_) => Err(HttpControlError::Persistence(
                "unexpected journal install result".to_owned(),
            )),
            Err(error) => Err(HttpControlError::Persistence(error.to_string())),
        }
    }
}

/// Tokio-facing backend handle. Calls are serialized through one bounded
/// async mutex, while the scheduler remains the sole mutable engine owner.
#[derive(Clone)]
pub struct HttpControlBackend {
    plane: Arc<Mutex<HttpControlPlane>>,
}

impl HttpControlBackend {
    #[must_use]
    pub fn new(plane: HttpControlPlane) -> Self {
        Self {
            plane: Arc::new(Mutex::new(plane)),
        }
    }

    #[must_use]
    pub fn plane(&self) -> Arc<Mutex<HttpControlPlane>> {
        self.plane.clone()
    }

    #[must_use]
    pub fn into_plane(self) -> Arc<Mutex<HttpControlPlane>> {
        self.plane
    }

    /// Recovers the sole control-plane owner once all transport and progress
    /// handles have been drained.
    pub fn try_into_control_plane(self) -> Result<HttpControlPlane, Self> {
        match Arc::try_unwrap(self.plane) {
            Ok(plane) => Ok(plane.into_inner()),
            Err(plane) => Err(Self { plane }),
        }
    }
}

impl HttpRpcBackend for HttpControlBackend {
    fn call(&self, method: &str, params: Value) -> crate::RpcFuture {
        let plane = self.plane.clone();
        let method = method.to_owned();
        Box::pin(async move {
            let mut plane = plane.lock().await;
            plane.poll_once().map_err(control_backend_error)?;
            plane.call(&method, params).map_err(control_backend_error)
        })
    }
}

fn control_backend_error(error: HttpControlError) -> HttpRpcBackendError {
    let code = match error {
        HttpControlError::InvalidParams(_) | HttpControlError::TaskSpec(_) => -32602,
        HttpControlError::Unsupported(_) => -32601,
        HttpControlError::NotFound => -32004,
        HttpControlError::Busy => -32005,
        _ => -32000,
    };
    HttpRpcBackendError::new(code, error.to_string())
}

fn parse_gid_param(params: &Value) -> Result<Gid, HttpControlError> {
    let values = params.as_array().filter(|values| values.len() == 1).ok_or(
        HttpControlError::InvalidParams("exactly one hexadecimal GID is required"),
    )?;
    let value = values
        .first()
        .and_then(Value::as_str)
        .ok_or(HttpControlError::InvalidParams(
            "a hexadecimal GID is required",
        ))?;
    value
        .parse()
        .map_err(|_| HttpControlError::InvalidParams("invalid GID"))
}

fn status_value(
    snapshot: &TaskSnapshot,
    status: Aria2Status,
    stats: HttpTransferStatsSnapshot,
) -> Value {
    let completed = snapshot.completed_length.max(stats.durable_bytes);
    let total = snapshot
        .total_length
        .or((stats.total_length != 0).then_some(stats.total_length));
    let mut value = json!({
        "gid": snapshot.gid.to_string(),
        "status": status.as_str(),
        "totalLength": total.unwrap_or(0).to_string(),
        "completedLength": completed.to_string(),
        "downloadSpeed": stats.current_speed.to_string(),
        "uploadSpeed": "0",
        "connections": stats.active_connections.to_string(),
        "errorCode": snapshot.error.as_ref().map_or_else(|| "0".to_owned(), |error| error.kind().number().to_string()),
        "errorMessage": snapshot.error.as_ref().map_or_else(String::new, |error| error.safe_message().to_owned()),
    });
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "verifiedLength".to_owned(),
            Value::String(stats.durable_bytes.to_string()),
        );
        object.insert(
            "verifiedSpeed".to_owned(),
            Value::String(stats.durable_speed.to_string()),
        );
        object.insert(
            "retryCount".to_owned(),
            Value::String(stats.retry_count.to_string()),
        );
        object.insert(
            "discardedLength".to_owned(),
            Value::String(stats.discarded_bytes.to_string()),
        );
        object.insert(
            "discardBudgetConsumed".to_owned(),
            Value::String(stats.discard_budget_consumed.to_string()),
        );
        object.insert(
            "discardBudgetRemaining".to_owned(),
            Value::String(stats.discard_budget_remaining.to_string()),
        );
        object.insert(
            "receivedPayloadLength".to_owned(),
            Value::String(stats.raw_body_bytes.to_string()),
        );
        object.insert(
            "acceptedLength".to_owned(),
            Value::String(stats.accepted_bytes.to_string()),
        );
        object.insert(
            "wireSpeed".to_owned(),
            Value::String(stats.wire_speed.to_string()),
        );
        object.insert(
            "usefulSpeed".to_owned(),
            Value::String(stats.useful_speed.to_string()),
        );
        object.insert(
            "smoothedSpeed".to_owned(),
            Value::String(stats.smoothed_speed.to_string()),
        );
        object.insert(
            "sampleAge".to_owned(),
            Value::String(stats.sample_age.as_millis().to_string()),
        );
        object.insert(
            "connectionCondition".to_owned(),
            Value::String(stats.connection_condition.code().to_owned()),
        );
        object.insert(
            "conditionReason".to_owned(),
            Value::String(
                stats
                    .condition_reason
                    .map_or("", |reason| reason.code())
                    .to_owned(),
            ),
        );
        object.insert(
            "rateDebt".to_owned(),
            Value::String(stats.rate_debt_bytes.to_string()),
        );
    }
    value
}

fn parse_add_options(
    options: &Value,
    default_root: &Path,
    uris: &[String],
) -> Result<
    (
        HttpTaskOptions,
        PathBuf,
        ariax_storage::SafeRelativePath,
        bool,
    ),
    HttpControlError,
> {
    let object = options.as_object().ok_or(HttpControlError::InvalidParams(
        "addUri options must be an object",
    ))?;
    let mut parsed = HttpTaskOptions::default();
    let mut root = default_root.to_path_buf();
    let mut out = None;
    let mut paused = false;
    for (name, value) in object {
        if is_retry_option(name) {
            continue;
        }
        match name.as_str() {
            "dir" => {
                let requested = PathBuf::from(
                    value
                        .as_str()
                        .ok_or(HttpControlError::InvalidParams("dir must be a string"))?,
                );
                if requested != default_root {
                    return Err(HttpControlError::InvalidParams(
                        "dir must equal the configured output root",
                    ));
                }
                root = requested;
            }
            "out" => {
                out = Some(
                    value
                        .as_str()
                        .ok_or(HttpControlError::InvalidParams("out must be a string"))?
                        .to_owned(),
                )
            }
            "pause" => {
                paused = match value {
                    Value::Bool(value) => *value,
                    Value::String(value) if value == "true" => true,
                    Value::String(value) if value == "false" => false,
                    _ => {
                        return Err(HttpControlError::InvalidParams(
                            "pause must be true or false",
                        ));
                    }
                }
            }
            "split" => parsed.split = parse_nonzero(value, "split", 1024)?,
            "max-connection-per-server" => {
                parsed.max_connections_per_server = parse_nonzero(value, name, 1024)?
            }
            "min-split-size" => parsed.min_split_size = parse_size(value)?,
            "piece-length" => parsed.piece_length = parse_size(value)?,
            "connect-timeout" => {
                parsed.connect_timeout = Duration::from_secs(parse_timeout(value)?)
            }
            "timeout" => parsed.response_body_timeout = Duration::from_secs(parse_timeout(value)?),
            "max-download-limit" => parsed.max_download_limit = parse_size(value)?,
            "lowest-speed-limit" => parsed.lowest_speed_limit = parse_size(value)?,
            "endgame-max-duplicates" => {
                parsed.endgame_max_duplicates =
                    usize::try_from(parse_u64(value, name)?).map_err(|_| {
                        HttpControlError::InvalidParams("endgame duplicate cap is too large")
                    })?;
                if parsed.endgame_max_duplicates > MAX_HTTP_ENDGAME_MAX_DUPLICATES {
                    return Err(HttpControlError::InvalidParams(
                        "endgame duplicate cap is too large",
                    ));
                }
            }
            "checksum" => {
                parsed.checksum = Some(
                    HttpContentChecksum::parse(
                        value
                            .as_str()
                            .ok_or(HttpControlError::InvalidParams("checksum must be a string"))?,
                    )
                    .map_err(|_| HttpControlError::InvalidParams("invalid checksum"))?,
                );
            }
            "verify-mirror-identity" => {
                parsed.mirror_identity = match value.as_str() {
                    Some("strict") => crate::HttpMirrorIdentityPolicy::RequireSharedDigest,
                    Some("off") | None => crate::HttpMirrorIdentityPolicy::TrustSubmittedMirrors,
                    _ => {
                        return Err(HttpControlError::InvalidParams(
                            "invalid mirror identity policy",
                        ));
                    }
                }
            }
            _ => return Err(HttpControlError::InvalidParams("unsupported addUri option")),
        }
    }
    if object.keys().any(|name| is_retry_option(name)) {
        parsed.retry = Some(parse_retry_options(object)?);
    }
    if !root.is_absolute() {
        return Err(HttpControlError::InvalidParams("dir must be absolute"));
    }
    let output = out.unwrap_or_else(|| default_output_name(uris));
    let output = SafePathBuilder::from_user_path(&output, PathPlatform::current())
        .map_err(|_| HttpControlError::InvalidParams("out is not a safe relative path"))?;
    Ok((parsed, root, output, paused))
}

fn is_retry_option(name: &str) -> bool {
    matches!(
        name,
        "max-tries"
            | "retry-wait"
            | "retry-profile"
            | "retry-on"
            | "retry-on-http-status"
            | "retry-on-http-status-add"
            | "retry-on-http-status-remove"
            | "retry-after"
            | "retry-after-max"
            | "retry-after-min"
            | "retry-backoff"
            | "retry-max-wait"
            | "retry-max-attempts"
            | "retry-max-attempts-per-mirror"
            | "retry-max-elapsed"
            | "stale-validator-policy"
    )
}

fn parse_retry_options(
    object: &serde_json::Map<String, Value>,
) -> Result<HttpRetryPolicy, HttpControlError> {
    let profile = match object.get("retry-profile") {
        Some(value) => HttpRetryProfile::parse(retry_text(value, "retry-profile")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry profile"))?,
        None => HttpRetryProfile::Conservative,
    };
    let mut policy = HttpRetryPolicy::from_profile(profile);

    if let Some(value) = object.get("retry-on") {
        policy.retry_on = HttpRetryTriggerSet::parse(retry_text(value, "retry-on")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry trigger set"))?;
    }
    if let Some(value) = object.get("retry-on-http-status") {
        policy.retryable_statuses =
            HttpRetryStatusSet::parse(retry_text(value, "retry-on-http-status")?)
                .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?;
    }
    if let Some(value) = object.get("retry-on-http-status-add") {
        for code in HttpRetryStatusSet::parse(retry_text(value, "retry-on-http-status-add")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?
            .iter()
        {
            policy
                .retryable_statuses
                .insert(code)
                .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?;
        }
    }
    if let Some(value) = object.get("retry-on-http-status-remove") {
        for code in HttpRetryStatusSet::parse(retry_text(value, "retry-on-http-status-remove")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry status set"))?
            .iter()
        {
            policy.retryable_statuses.remove(code);
        }
    }

    let max_tries = object
        .get("max-tries")
        .map(|value| parse_retry_attempt_cap(value, "max-tries"))
        .transpose()?;
    let max_attempts = object
        .get("retry-max-attempts")
        .map(|value| parse_retry_attempt_cap(value, "retry-max-attempts"))
        .transpose()?;
    if let Some(value) = stricter_retry_cap(max_tries, max_attempts) {
        policy.max_attempts = value;
    }
    if let Some(value) = object.get("retry-max-attempts-per-mirror") {
        policy.max_attempts_per_mirror =
            parse_retry_attempt_cap(value, "retry-max-attempts-per-mirror")?;
    }
    if let Some(value) = object.get("retry-wait") {
        policy.base_wait =
            parse_retry_duration(value, "retry-wait", MAX_HTTP_RETRY_WAIT_SECS, true)?;
    }
    if let Some(value) = object.get("retry-max-wait") {
        policy.max_wait =
            parse_retry_duration(value, "retry-max-wait", MAX_HTTP_RETRY_WAIT_SECS, false)?;
    }
    if let Some(value) = object.get("retry-max-elapsed") {
        policy.max_elapsed = parse_retry_duration(
            value,
            "retry-max-elapsed",
            MAX_HTTP_RETRY_ELAPSED_SECS,
            false,
        )?;
    }
    if let Some(value) = object.get("retry-after-min") {
        policy.retry_after_min =
            parse_retry_duration(value, "retry-after-min", MAX_HTTP_RETRY_WAIT_SECS, true)?;
    }
    if let Some(value) = object.get("retry-after-max") {
        policy.retry_after_max =
            parse_retry_duration(value, "retry-after-max", MAX_HTTP_RETRY_WAIT_SECS, true)?;
    }
    if let Some(value) = object.get("retry-after") {
        policy.respect_retry_after = matches!(
            HttpRetryAfterPolicy::parse(retry_text(value, "retry-after")?)
                .map_err(|_| HttpControlError::InvalidParams("invalid retry-after policy"))?,
            HttpRetryAfterPolicy::Respect
        );
    }
    if let Some(value) = object.get("retry-backoff") {
        policy.backoff = HttpRetryBackoff::parse(retry_text(value, "retry-backoff")?)
            .map_err(|_| HttpControlError::InvalidParams("invalid retry backoff"))?;
    }
    if let Some(value) = object.get("stale-validator-policy") {
        policy.stale_validator_policy =
            crate::HttpStaleValidatorPolicy::parse(retry_text(value, "stale-validator-policy")?)
                .map_err(|_| HttpControlError::InvalidParams("invalid stale validator policy"))?;
    }
    policy
        .validate()
        .map_err(|_| HttpControlError::InvalidParams("invalid retry policy"))?;
    Ok(policy)
}

fn retry_text<'a>(value: &'a Value, name: &str) -> Result<&'a str, HttpControlError> {
    value
        .as_str()
        .ok_or(HttpControlError::InvalidParams(match name {
            "retry-profile" => "retry-profile must be a string",
            "retry-on" => "retry-on must be a string",
            "retry-on-http-status" | "retry-on-http-status-add" | "retry-on-http-status-remove" => {
                "retry status set must be a string"
            }
            "retry-after" => "retry-after must be a string",
            "retry-backoff" => "retry-backoff must be a string",
            "stale-validator-policy" => "stale-validator-policy must be a string",
            _ => "retry option must be a string",
        }))
}

fn parse_retry_attempt_cap(value: &Value, name: &str) -> Result<NonZeroU32, HttpControlError> {
    let value = parse_u64(value, name)?;
    let value = u32::try_from(value)
        .map_err(|_| HttpControlError::InvalidParams("retry attempt cap is too large"))?;
    if value > MAX_HTTP_RETRY_ATTEMPTS {
        return Err(HttpControlError::InvalidParams(
            "retry attempt cap is too large",
        ));
    }
    NonZeroU32::new(value).ok_or(HttpControlError::InvalidParams(
        "retry attempt cap must be nonzero",
    ))
}

fn stricter_retry_cap(
    max_tries: Option<NonZeroU32>,
    max_attempts: Option<NonZeroU32>,
) -> Option<NonZeroU32> {
    match (max_tries, max_attempts) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn parse_retry_duration(
    value: &Value,
    name: &str,
    maximum: u64,
    allow_zero: bool,
) -> Result<Duration, HttpControlError> {
    let seconds = parse_u64(value, name)?;
    if seconds > maximum || (!allow_zero && seconds == 0) {
        return Err(HttpControlError::InvalidParams(
            "retry duration is out of range",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_nonzero(
    value: &Value,
    name: &str,
    maximum: usize,
) -> Result<NonZeroUsize, HttpControlError> {
    let number = parse_u64(value, name)?;
    let number = usize::try_from(number)
        .map_err(|_| HttpControlError::InvalidParams("option is too large"))?;
    if number > maximum {
        return Err(HttpControlError::InvalidParams("option is too large"));
    }
    NonZeroUsize::new(number).ok_or(HttpControlError::InvalidParams("option must be nonzero"))
}

fn parse_u64(value: &Value, _name: &str) -> Result<u64, HttpControlError> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or(HttpControlError::InvalidParams("numeric option is invalid"))
}

fn parse_size(value: &Value) -> Result<u64, HttpControlError> {
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    let text = value
        .as_str()
        .ok_or(HttpControlError::InvalidParams("size option is invalid"))?
        .trim();
    let (digits, multiplier) = match text.as_bytes().last().copied() {
        Some(b'K' | b'k') => (&text[..text.len() - 1], 1024_u64),
        Some(b'M' | b'm') => (&text[..text.len() - 1], 1024_u64.pow(2)),
        Some(b'G' | b'g') => (&text[..text.len() - 1], 1024_u64.pow(3)),
        Some(b'T' | b't') => (&text[..text.len() - 1], 1024_u64.pow(4)),
        _ => (text, 1),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(multiplier))
        .ok_or(HttpControlError::InvalidParams("size option is invalid"))
}

fn parse_timeout(value: &Value) -> Result<u64, HttpControlError> {
    let seconds = parse_u64(value, "timeout")?;
    if !(1..=crate::MAX_HTTP_TIMEOUT_SECS).contains(&seconds) {
        return Err(HttpControlError::InvalidParams(
            "timeout must be between 1 and 600 seconds",
        ));
    }
    Ok(seconds)
}

fn default_output_name(uris: &[String]) -> String {
    uris.first()
        .and_then(|uri| uri.rsplit('/').find(|part| !part.is_empty()))
        .filter(|part| !part.contains('?') && !part.contains('#'))
        .unwrap_or("download")
        .to_owned()
}

fn derive_http_gid(session: SessionId, task: TaskId) -> Gid {
    let mut digest = Sha256::new();
    digest.update(b"ariax/http-gid/v1\0");
    digest.update(session.as_bytes());
    digest.update(task.get().to_le_bytes());
    let bytes: [u8; 32] = digest.finalize().into();
    let mut value_bytes = [0_u8; 8];
    value_bytes.copy_from_slice(&bytes[..8]);
    let mut value = u64::from_le_bytes(value_bytes) | (1_u64 << 63);
    if value == 0 {
        value = 1;
    }
    Gid::new(value).expect("derived HTTP GID is nonzero")
}

fn session_queue(class: QueueClass) -> SessionQueueState {
    match class {
        QueueClass::Waiting => SessionQueueState::Waiting,
        QueueClass::Demoted => SessionQueueState::Demoted,
        QueueClass::Paused => SessionQueueState::Paused,
        QueueClass::Active => SessionQueueState::Active,
        QueueClass::Stopped => SessionQueueState::Stopped,
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn persistence_ack(effect: &TransitionEffect) -> Option<TaskEventEnvelope> {
    let task_id = effect.task_id();
    let gid = effect.gid();
    let generation = effect.generation().unwrap_or(Generation::INITIAL);
    let event = match effect {
        TransitionEffect::PersistGenerationStarted { .. } => {
            TaskEvent::GenerationPersisted { gid, generation }
        }
        TransitionEffect::PersistTerminal { status, .. } => TaskEvent::TerminalPersisted {
            gid,
            generation,
            status: *status,
        },
        _ => return None,
    };
    Some(event.for_task(task_id))
}

trait PersistenceKindExt {
    fn persistence_effect(self) -> bool;
}

impl PersistenceKindExt for ariax_core::TransitionEffectKind {
    fn persistence_effect(self) -> bool {
        matches!(
            self,
            Self::PersistTask
                | Self::PersistQueueTransition
                | Self::StageOptionPatch
                | Self::PersistGenerationStarted
                | Self::PersistConditions
                | Self::PersistHostKeyChallenge
                | Self::PersistHostKeyPinAndClearChallenge
                | Self::PersistHostKeyChallengeRejected
                | Self::PersistTerminal
                | Self::DeleteStoppedTaskMetadata
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HTTP_CONNECTION_RESERVATION_BYTES, HttpDestinationPolicy, HttpDirectTransportConfig,
        HttpMultiRangeWorker, HttpMultiRangeWorkerConfig, HttpPolicyClient, HttpPolicyClientConfig,
        HttpResolver, HttpResolverConfig, HttpTransportBudgets, ProcessBootstrapConfig,
        RuntimeEffectConfig, StartupRecoveryConfig, StorageEngineConfig, bootstrap_process,
    };
    use ariax_core::SchedulerConfig;
    use ariax_runtime::ShutdownStep;
    use ariax_storage::{
        JournalStateLimits, ReplayLimits, SessionOwnerConfig, SessionStore, SessionStoreConfig,
        SessionTerminalStatus,
    };
    use std::fs;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory {
        root: PathBuf,
        control: PathBuf,
        output: PathBuf,
        journals: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "ariax-http-control-{}-{}",
                std::process::id(),
                TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            create_private_directory(&root);
            let control = root.join("control");
            let output = root.join("output");
            let journals = control.join("http-journals");
            create_private_directory(&control);
            create_private_directory(&output);
            create_private_directory(&journals);
            Self {
                root,
                control,
                output,
                journals,
            }
        }

        fn process_config(&self) -> ProcessBootstrapConfig {
            let capacity = NonZeroUsize::new(16).expect("capacity");
            ProcessBootstrapConfig {
                session_owner: SessionOwnerConfig::new(self.root.join("session.db")),
                control_directory: self.control.clone(),
                allowed_output_roots: vec![self.output.clone()],
                replay_limits: ReplayLimits::default(),
                journal_state_limits: JournalStateLimits::default(),
                recovery: StartupRecoveryConfig {
                    scheduler: SchedulerConfig::new(capacity, capacity, false)
                        .expect("scheduler config"),
                    now_wall_unix_ms: 1_000,
                    now_monotonic: MonotonicInstant::now(),
                    max_retry_wait_ms: NonZeroU64::new(60_000).expect("retry wait"),
                    max_slow_wait_ms: NonZeroU64::new(60_000).expect("slow wait"),
                    max_no_space_wait_ms: NonZeroU64::new(60_000).expect("no-space wait"),
                    max_retry_elapsed_ms: 60_000,
                },
                runtime: RuntimeEffectConfig {
                    request_capacity: capacity,
                    event_capacity: capacity,
                    timer_capacity: capacity,
                    option_plan_capacity: capacity,
                },
                persistence_plan_capacity: capacity,
                shutdown_step_timeout_ms: crate::DEFAULT_PROCESS_SHUTDOWN_STEP_TIMEOUT_MS,
                updated_ms: 1_000,
                recovery_created_at_unix_ms: 1_000,
            }
        }

        fn control_plane(&self) -> HttpControlPlane {
            self.control_plane_with_supervisor(HttpWorkerSupervisorConfig::default())
        }

        fn control_plane_with_supervisor(
            &self,
            supervisor: HttpWorkerSupervisorConfig,
        ) -> HttpControlPlane {
            let engine = bootstrap_process(self.process_config(), allow_all_options)
                .expect("bootstrap process");
            HttpControlPlane::new(
                engine,
                HttpControlPlaneConfig {
                    output_root: self.output.clone(),
                    journal_root: self.journals.clone(),
                    task_capacity: NonZeroUsize::new(16).expect("task capacity"),
                    supervisor,
                },
            )
            .expect("control plane")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[cfg(unix)]
    fn create_private_directory(path: &Path) {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path).expect("create private test directory");
    }

    #[cfg(windows)]
    fn create_private_directory(path: &Path) {
        ariax_windows_security::create_private_directory(path)
            .expect("create private test directory");
    }

    fn allow_all_options(_name: &str) -> bool {
        true
    }

    fn add_paused(plane: &mut HttpControlPlane) -> Gid {
        let result = plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/file.bin"], {"pause": true}]),
            )
            .expect("add paused HTTP task");
        result
            .as_str()
            .expect("GID result")
            .parse()
            .expect("valid GID")
    }

    async fn serve_control_file(data: Arc<[u8]>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).await.expect("request");
                    request.push(byte[0]);
                }
                let text = String::from_utf8(request).expect("ASCII request");
                let range = text
                    .lines()
                    .find_map(|line| {
                        let value = line
                            .strip_prefix("Range: bytes=")
                            .or_else(|| line.strip_prefix("range: bytes="))?;
                        let (start, end) = value.split_once('-')?;
                        Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                    })
                    .expect("range");
                let body = &data[range.0..=range.1];
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nETag: \"control-v1\"\r\nConnection: close\r\n\r\n",
                    body.len(),
                    range.0,
                    range.1,
                    data.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
                stream.write_all(body).await.expect("body");
            }
        });
        (format!("http://{address}/file.bin"), task)
    }

    async fn serve_control_restart_file(data: Arc<[u8]>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            for index in 0..4 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = Vec::new();
                let mut byte = [0_u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).await.expect("request");
                    request.push(byte[0]);
                }
                let text = String::from_utf8(request).expect("ASCII request");
                let (start, end) = text
                    .lines()
                    .find_map(|line| {
                        let value = line
                            .strip_prefix("Range: bytes=")
                            .or_else(|| line.strip_prefix("range: bytes="))?;
                        let (start, end) = value.split_once('-')?;
                        Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                    })
                    .expect("range");
                let body = &data[start..=end];
                let etag = if index == 0 { "\"v1\"" } else { "\"v2\"" };
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nAccept-Ranges: bytes\r\nETag: {etag}\r\nConnection: close\r\n\r\n",
                    body.len(),
                    data.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
                if index != 1 {
                    stream.write_all(body).await.expect("body");
                }
            }
        });
        (format!("http://{address}/restart.bin"), task)
    }

    fn attach_loopback_worker(plane: &mut HttpControlPlane, directory: &TestDirectory) {
        let resolver = HttpResolver::new(HttpResolverConfig::default()).expect("resolver");
        let client = HttpPolicyClient::new(
            resolver,
            HttpPolicyClientConfig {
                destination: HttpDestinationPolicy {
                    allow_loopback: true,
                    ..HttpDestinationPolicy::default()
                },
                direct: HttpDirectTransportConfig {
                    connect_timeout: Duration::from_secs(5),
                    handshake_timeout: Duration::from_secs(5),
                    max_connections_per_origin: 2,
                    max_idle_connections_per_origin: 0,
                    budgets: HttpTransportBudgets::new(2, 2 * HTTP_CONNECTION_RESERVATION_BYTES)
                        .expect("transport budgets"),
                    ..HttpDirectTransportConfig::default()
                },
                ..HttpPolicyClientConfig::default()
            },
        );
        let worker = HttpMultiRangeWorker::new(
            client,
            HttpMultiRangeWorkerConfig {
                journal_root: directory.journals.clone(),
                storage: StorageEngineConfig::default(),
                ..HttpMultiRangeWorkerConfig::default()
            },
            plane.stats_catalog(),
        )
        .expect("worker")
        .with_session_owner(plane.session_handle());
        plane
            .attach_worker(Arc::new(worker))
            .expect("attach worker");
    }

    struct UncooperativeShutdownWorker {
        started: Arc<Notify>,
    }

    impl HttpTaskWorker for UncooperativeShutdownWorker {
        fn start(
            &self,
            _task: Arc<HttpTaskSpec>,
            _generation: Generation,
            _cancellation: crate::HttpCancellation,
        ) -> crate::HttpWorkerFuture {
            let started = Arc::clone(&self.started);
            Box::pin(async move {
                started.notify_one();
                std::future::pending::<Result<crate::HttpWorkerSuccess, PublicError>>().await
            })
        }
    }

    #[test]
    fn paused_add_persists_task_sources_and_options_before_publication() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);

        let status = plane
            .call("aria2.tellStatus", json!([gid.to_string()]))
            .expect("tell status");
        assert_eq!(status["status"], "paused");
        assert_eq!(status["completedLength"], "0");
        assert_eq!(status["downloadSpeed"], "0");
        assert_eq!(status["verifiedLength"], "0");
        assert_eq!(status["retryCount"], "0");
        assert_eq!(status["discardBudgetConsumed"], "0");
        assert_eq!(status["discardBudgetRemaining"], "0");

        let global = plane
            .call("aria2.getGlobalStat", json!([]))
            .expect("global stat");
        assert_eq!(global["numWaiting"], "1");
        assert_eq!(global["numActive"], "0");

        let session = plane.session_handle();
        let tasks = match session
            .execute(SessionCommand::ReadTasks)
            .expect("read tasks")
        {
            SessionCommandResult::Tasks(tasks) => tasks,
            result => panic!("unexpected task result: {result:?}"),
        };
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].gid, gid);
        assert_eq!(tasks[0].queue_state, SessionQueueState::Paused);
        let sources = match session
            .execute(SessionCommand::ReadTaskSources { gid })
            .expect("read sources")
        {
            SessionCommandResult::TaskSources(sources) => sources,
            result => panic!("unexpected source result: {result:?}"),
        };
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].persistence_safe_uri.as_deref(),
            Some("http://example.test/file.bin")
        );
        let options = match session
            .execute(SessionCommand::ReadTaskOptions {
                gid,
                scope: OptionsSnapshotScope::CurrentGeneration,
            })
            .expect("read options")
        {
            SessionCommandResult::TaskOptions(options) => options,
            result => panic!("unexpected option result: {result:?}"),
        };
        assert!(
            options
                .entries()
                .any(|(name, value)| name == "out" && value == "file.bin")
        );
        assert!(
            options
                .entries()
                .any(|(name, value)| name == "connect-timeout" && value == "60")
        );
        drop(session);
        assert_eq!(plane.shutdown().expect("shutdown").journals_closed, 1);
    }

    #[test]
    fn remove_persists_terminal_result_and_recovered_catalog_keeps_sources() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let gid = add_paused(&mut plane);
        assert_eq!(plane.shutdown().expect("first shutdown").journals_closed, 1);

        let mut recovered = directory.control_plane();
        let task = recovered
            .task_catalog()
            .get(TaskId::new(1).expect("task id"))
            .expect("recovered HTTP task");
        assert_eq!(task.gid(), gid);
        assert_eq!(task.sources()[0].uri(), "http://example.test/file.bin");
        assert_eq!(task.output().canonical_string(), "file.bin");

        let removed = recovered
            .call("aria2.remove", json!([gid.to_string()]))
            .expect("remove recovered task");
        assert_eq!(removed, gid.to_string());
        let status = recovered
            .call("aria2.tellStatus", json!([gid.to_string()]))
            .expect("removed status");
        assert_eq!(status["status"], "removed");

        let session = recovered.session_handle();
        let stopped = match session
            .execute(SessionCommand::ReadStoppedResults)
            .expect("read stopped results")
        {
            SessionCommandResult::StoppedResults(results) => results,
            result => panic!("unexpected stopped-result response: {result:?}"),
        };
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].gid, gid);
        assert_eq!(stopped[0].status, SessionTerminalStatus::Removed);
        drop(session);
        assert_eq!(recovered.shutdown().expect("shutdown").journals_closed, 1);
    }

    #[test]
    fn rejected_output_paths_and_options_leave_no_task_metadata() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        let outside = directory.root.join("outside");
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"dir": outside}]),
            ),
            Err(HttpControlError::InvalidParams(
                "dir must equal the configured output root"
            ))
        ));
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"out": "../escape"}]),
            ),
            Err(HttpControlError::InvalidParams(
                "out is not a safe relative path"
            ))
        ));
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"unknown": true}]),
            ),
            Err(HttpControlError::InvalidParams("unsupported addUri option"))
        ));
        assert!(matches!(
            plane.call(
                "aria2.addUri",
                json!([["http://example.test/file"], {"piece-length": "3M"}]),
            ),
            Err(HttpControlError::TaskSpec(
                HttpTaskSpecError::InvalidOptions
            ))
        ));
        assert!(matches!(
            plane.call("aria2.addUri", json!([])),
            Err(HttpControlError::InvalidParams(
                "addUri accepts a URI array and optional options object"
            ))
        ));
        assert!(matches!(
            plane.call("aria2.addUri", json!([["http://example.test/file"], {}, 0]),),
            Err(HttpControlError::InvalidParams(
                "addUri accepts a URI array and optional options object"
            ))
        ));
        assert!(matches!(
            plane.call("aria2.pause", json!(["0000000000000001", false])),
            Err(HttpControlError::InvalidParams(
                "exactly one hexadecimal GID is required"
            ))
        ));
        let tasks = match plane
            .session_handle()
            .execute(SessionCommand::ReadTasks)
            .expect("read tasks")
        {
            SessionCommandResult::Tasks(tasks) => tasks,
            result => panic!("unexpected task response: {result:?}"),
        };
        assert!(tasks.is_empty());
        assert!(
            fs::read_dir(&directory.journals)
                .expect("read journals")
                .next()
                .is_none()
        );
        assert_eq!(plane.shutdown().expect("shutdown").journals_closed, 0);
    }

    #[test]
    fn add_option_parser_accepts_aria2_sizes_and_rejects_bounds() {
        let directory = TestDirectory::new();
        let uris = vec!["http://example.test/file".to_owned()];
        let (options, root, output, paused) = parse_add_options(
            &json!({
                "piece-length": "1M",
                "min-split-size": "1M",
                "split": "4",
                "max-connection-per-server": 2,
                "connect-timeout": "1",
                "timeout": 600,
                "max-download-limit": "64K",
                "retry-profile": "custom",
                "retry-on": "timeout,lowest-speed",
                "retry-on-http-status": "418,429",
                "retry-on-http-status-add": "500-501",
                "retry-on-http-status-remove": "429",
                "max-tries": 5,
                "retry-max-attempts": 4,
                "retry-max-attempts-per-mirror": 2,
                "retry-wait": 0,
                "retry-backoff": "fixed",
                "retry-after": "ignore",
                "retry-after-min": 0,
                "retry-after-max": 60,
                "retry-max-wait": 60,
                "retry-max-elapsed": 600,
                "stale-validator-policy": "revalidate",
                "endgame-max-duplicates": 8,
                "checksum": "sha-256=abababababababababababababababababababababababababababababababab",
                "pause": "true",
            }),
            &directory.output,
            &uris,
        )
        .expect("valid bounded options");
        assert_eq!(options.piece_length, 1024 * 1024);
        assert_eq!(options.min_split_size, 1024 * 1024);
        assert_eq!(options.split.get(), 4);
        assert_eq!(options.max_connections_per_server.get(), 2);
        assert_eq!(options.connect_timeout, Duration::from_secs(1));
        assert_eq!(options.response_body_timeout, Duration::from_secs(600));
        assert_eq!(options.max_download_limit, 64 * 1024);
        assert_eq!(options.endgame_max_duplicates, 8);
        assert_eq!(
            options.checksum,
            Some(HttpContentChecksum::sha256([0xab; 32]))
        );
        let retry = options.retry.expect("resolved retry policy");
        assert_eq!(retry.profile, HttpRetryProfile::Custom);
        assert_eq!(retry.max_attempts.get(), 4);
        assert_eq!(retry.max_attempts_per_mirror.get(), 2);
        assert_eq!(retry.retry_on.canonical(), "timeout,lowest-speed");
        assert_eq!(retry.retryable_statuses.canonical(), "418,500,501");
        assert_eq!(retry.backoff, HttpRetryBackoff::Fixed);
        assert!(!retry.respect_retry_after);
        assert_eq!(
            retry.stale_validator_policy,
            crate::HttpStaleValidatorPolicy::Revalidate
        );
        assert_eq!(root, directory.output);
        assert_eq!(output.canonical_string(), "file");
        assert!(paused);

        let (disabled, _, _, _) = parse_add_options(
            &json!({"endgame-max-duplicates": 0}),
            &directory.output,
            &uris,
        )
        .expect("zero disables endgame");
        assert_eq!(disabled.endgame_max_duplicates, 0);

        for invalid in [
            json!({"split": 1025}),
            json!({"max-connection-per-server": 0}),
            json!({"connect-timeout": 0}),
            json!({"timeout": 601}),
            json!({"retry-profile": "custom"}),
            json!({"retry-max-attempts": 0}),
            json!({"retry-max-wait": 0}),
            json!({"retry-on-http-status": "99"}),
            json!({"endgame-max-duplicates": 9}),
            json!({"endgame-max-duplicates": -1}),
            json!({"stale-validator-policy": "unsafe"}),
            json!({"stale-validator-policy": 7}),
            json!({"piece-length": "18446744073709551615T"}),
            json!({"checksum": "sha-512=abcd"}),
            json!({"checksum": 7}),
        ] {
            assert!(matches!(
                parse_add_options(&invalid, &directory.output, &uris),
                Err(HttpControlError::InvalidParams(_))
            ));
        }
    }

    #[tokio::test]
    async fn live_shutdown_timeout_aborts_worker_and_persists_dirty_checkpoint() {
        let directory = TestDirectory::new();
        let started = Arc::new(Notify::new());
        let supervisor = HttpWorkerSupervisorConfig {
            shutdown_timeout: Duration::from_millis(10),
            ..HttpWorkerSupervisorConfig::default()
        };
        let mut plane = directory.control_plane_with_supervisor(supervisor);
        plane
            .attach_worker(Arc::new(UncooperativeShutdownWorker {
                started: Arc::clone(&started),
            }))
            .expect("attach uncooperative worker");
        plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/hung.bin"], {"pause": false}]),
            )
            .expect("add live task");
        plane.poll_once().expect("start live worker");
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("worker started");

        let report = plane.shutdown_async().await.expect("bounded shutdown");
        assert!(!report.is_clean());
        assert!(report.shutdown().step_timed_out(ShutdownStep::DrainDiskCpu));
        assert_eq!(report.journals_flushed, 1);
        assert_eq!(report.journals_closed, 1);

        let store = SessionStore::open(
            directory.root.join("session.db"),
            SessionStoreConfig::default(),
        )
        .expect("reopen dirty session");
        assert!(
            !store
                .session()
                .expect("session")
                .expect("record")
                .clean_shutdown
        );
    }

    #[test]
    fn synchronous_shutdown_with_an_idle_supervisor_remains_clean() {
        let directory = TestDirectory::new();
        let mut plane = directory.control_plane();
        attach_loopback_worker(&mut plane, &directory);

        let report = plane.shutdown().expect("shutdown idle supervisor");
        assert!(report.is_clean());
        assert_eq!(report.journals_flushed, 0);
        assert_eq!(report.journals_closed, 0);
    }

    #[tokio::test]
    async fn synchronous_shutdown_with_an_active_worker_persists_dirty_checkpoint() {
        let directory = TestDirectory::new();
        let started = Arc::new(Notify::new());
        let mut plane = directory.control_plane();
        plane
            .attach_worker(Arc::new(UncooperativeShutdownWorker {
                started: Arc::clone(&started),
            }))
            .expect("attach uncooperative worker");
        plane
            .call(
                "aria2.addUri",
                json!([["http://example.test/sync-hung.bin"], {"pause": false}]),
            )
            .expect("add live task");
        plane.poll_once().expect("start live worker");
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("worker started");

        let report = plane.shutdown().expect("synchronous shutdown");
        assert!(!report.is_clean());
        assert!(report.shutdown().step_failed(ShutdownStep::DrainDiskCpu));
        assert!(!report.shutdown().step_timed_out(ShutdownStep::DrainDiskCpu));
        let store = SessionStore::open(
            directory.root.join("session.db"),
            SessionStoreConfig::default(),
        )
        .expect("reopen dirty session");
        assert!(
            !store
                .session()
                .expect("session")
                .expect("record")
                .clean_shutdown
        );
    }

    #[tokio::test]
    async fn live_supervisor_completes_http_task_and_persists_terminal_evidence() {
        let directory = TestDirectory::new();
        let data: Arc<[u8]> = vec![0x5a; 1024 * 1024].into();
        let (uri, server) = serve_control_file(Arc::clone(&data)).await;
        let mut plane = directory.control_plane();
        attach_loopback_worker(&mut plane, &directory);
        let checksum =
            HttpContentChecksum::sha256(Sha256::digest(data.as_ref()).into()).canonical();
        let gid = plane
            .call(
                "aria2.addUri",
                json!([[uri], {"pause": false, "checksum": checksum}]),
            )
            .expect("add URI")
            .as_str()
            .expect("GID")
            .parse::<Gid>()
            .expect("valid GID");

        let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
        loop {
            plane.poll_once().expect("control progress");
            let status = plane
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .expect("status");
            if status["status"] == "complete" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "live worker did not reach complete status"
            );
            tokio::task::yield_now().await;
        }
        server.await.expect("server");
        assert_eq!(
            fs::read(directory.output.join("file.bin")).expect("output"),
            data.as_ref()
        );
        let stopped = match plane
            .session_handle()
            .execute(SessionCommand::ReadStoppedResults)
            .expect("stopped results")
        {
            SessionCommandResult::StoppedResults(results) => results,
            result => panic!("unexpected stopped response: {result:?}"),
        };
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].gid, gid);
        assert_eq!(stopped[0].status, SessionTerminalStatus::Complete);
        assert_eq!(stopped[0].total_length, Some(data.len() as u64));
        let report = plane.shutdown_async().await.expect("shutdown");
        assert!(report.is_clean());
        assert_eq!(report.journals_closed, 1);
    }

    #[tokio::test]
    async fn live_stale_validator_restart_persists_and_completes_the_next_generation() {
        let directory = TestDirectory::new();
        let data: Arc<[u8]> = vec![0x73; 1024 * 1024].into();
        let (uri, server) = serve_control_restart_file(Arc::clone(&data)).await;
        let mut plane = directory.control_plane();
        attach_loopback_worker(&mut plane, &directory);
        let gid = plane
            .call(
                "aria2.addUri",
                json!([[uri], {
                    "pause": false,
                    "split": 1,
                    "retry-max-attempts": 2,
                    "retry-max-attempts-per-mirror": 2,
                    "stale-validator-policy": "restart-if-safe"
                }]),
            )
            .expect("add restarting URI")
            .as_str()
            .expect("GID")
            .parse::<Gid>()
            .expect("valid GID");

        let deadline = Instant::now() + CONTROL_PROGRESS_TIMEOUT;
        loop {
            plane.poll_once().expect("restart control progress");
            let status = plane
                .call("aria2.tellStatus", json!([gid.to_string()]))
                .expect("status");
            if status["status"] == "complete" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "restarted worker did not reach complete status"
            );
            tokio::task::yield_now().await;
        }
        server.await.expect("server");
        assert_eq!(
            fs::read(directory.output.join("restart.bin")).expect("output"),
            data.as_ref()
        );
        let stopped = match plane
            .session_handle()
            .execute(SessionCommand::ReadStoppedResults)
            .expect("stopped results")
        {
            SessionCommandResult::StoppedResults(results) => results,
            result => panic!("unexpected stopped response: {result:?}"),
        };
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].gid, gid);
        assert_eq!(stopped[0].status, SessionTerminalStatus::Complete);
        assert_eq!(stopped[0].total_length, Some(data.len() as u64));
        let report = plane.shutdown_async().await.expect("shutdown");
        assert!(report.is_clean());
        assert_eq!(report.journals_closed, 1);
    }
}
