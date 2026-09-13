use crate::{
    ALL_QUEUE_CLASSES, Aria2Status, CredentialRequirementKey, DrainTarget, EventDisposition,
    Generation, Gid, HostKeyResolutionId, MAX_PERSISTED_MILLISECONDS, MonotonicInstant,
    NoSpaceProbeId, NoSpaceProbeOrigin, OptionPatchId, PendingBarrier, PresentedHostKeyChallenge,
    PublicError, QueueClass, QueueOrder, RetryTimerId, SchedulerAction, SchedulerCommand,
    SchedulerConfig, SchedulerError, SchedulerOutcome, SlotOwnership, SlowReadmissionDecision,
    SlowReadmissionId, SlowSlotPersistence, StateTransition, StoppedResultDeletionId,
    TaskConditions, TaskConditionsSnapshot, TaskDeletion, TaskEvent, TaskEventEnvelope,
    TaskEventKind, TaskId, TaskSnapshot, TaskState, TransitionContractKind, TransitionEffect,
    TransitionRejection, ValidatedOptionPatchKind, transition_contract,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

/// Provenance retained until a correlated option-patch acknowledgement arrives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingOptionPatchMode {
    InPlace,
    ActiveRestart,
    MatchingHostKey,
}

/// Latest user control request waiting behind an asynchronous persistence barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingUserControl {
    Pause { force: bool },
    Resume,
    Remove { force: bool },
}

/// Read-only scheduler-owned task metadata. Payload/options remain in their
/// owning adapters and are referenced by immutable identifiers in effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerTaskView {
    pub task_id: TaskId,
    pub gid: Gid,
    pub state: TaskState,
    pub generation: Generation,
    pub queue: Option<QueueClass>,
    pub slot: SlotOwnership,
    pub desired_paused: bool,
    pub conditions: TaskConditionsSnapshot,
    pub pending_barrier: Option<PendingBarrier>,
    pub retry_timer: Option<RetryTimerId>,
    pub slow_readmission: Option<SlowReadmissionId>,
    pub slow_demotion_count: u32,
    pub slow_slot: Option<SlowSlotPersistence>,
    pub no_space_probe: Option<NoSpaceProbeId>,
    pub pending_option_patch: Option<OptionPatchId>,
    pub pending_option_patch_mode: Option<PendingOptionPatchMode>,
    pub pending_credential_requirement: Option<CredentialRequirementKey>,
    pub pending_user_control: Option<PendingUserControl>,
    pub pending_source_replacement: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledTask {
    task_id: TaskId,
    gid: Gid,
    state: TaskState,
    generation: Generation,
    generation_started: bool,
    desired_paused: bool,
    conditions: TaskConditions,
    slot: SlotOwnership,
    pending_barrier: Option<PendingBarrier>,
    retry_timer: Option<(RetryTimerId, MonotonicInstant)>,
    retry_ready: bool,
    slow_readmission: Option<(SlowReadmissionId, MonotonicInstant)>,
    slow_readmission_ready: bool,
    pending_slow_readmission: Option<SlowReadmissionDecision>,
    slow_demotion_count: u32,
    slow_slot: Option<SlowSlotPersistence>,
    slow_remaining_position: usize,
    no_space_probe: Option<(NoSpaceProbeId, NoSpaceProbeOrigin)>,
    pending_option_patch: Option<OptionPatchId>,
    pending_option_patch_mode: Option<PendingOptionPatchMode>,
    pending_credential_requirement: Option<CredentialRequirementKey>,
    pending_user_control: Option<PendingUserControl>,
    pending_source_replacement: bool,
    host_key_challenge: Option<PresentedHostKeyChallenge>,
    error: Option<PublicError>,
    stopped_status: Option<Aria2Status>,
    terminal_persisted: bool,
    seen_events: BTreeSet<TaskEventKind>,
    last_retry_timer: Option<RetryTimerId>,
    last_slow_readmission: Option<SlowReadmissionId>,
    last_no_space_probe: Option<NoSpaceProbeId>,
    highest_option_patch_id: Option<OptionPatchId>,
    last_option_patch_persistence: Option<OptionPatchId>,
    last_option_patch_application: Option<OptionPatchId>,
    last_host_key_resolution: Option<HostKeyResolutionId>,
    last_stopped_deletion: Option<StoppedResultDeletionId>,
}

impl ScheduledTask {
    fn new(task_id: TaskId, gid: Gid, desired_paused: bool, conditions: TaskConditions) -> Self {
        Self {
            task_id,
            gid,
            state: TaskState::Accepted,
            generation: Generation::INITIAL,
            generation_started: false,
            desired_paused,
            conditions,
            slot: SlotOwnership::None,
            pending_barrier: None,
            retry_timer: None,
            retry_ready: false,
            slow_readmission: None,
            slow_readmission_ready: false,
            pending_slow_readmission: None,
            slow_demotion_count: 0,
            slow_slot: None,
            slow_remaining_position: 0,
            no_space_probe: None,
            pending_option_patch: None,
            pending_option_patch_mode: None,
            pending_credential_requirement: None,
            pending_user_control: None,
            pending_source_replacement: false,
            host_key_challenge: None,
            error: None,
            stopped_status: None,
            terminal_persisted: false,
            seen_events: BTreeSet::new(),
            last_retry_timer: None,
            last_slow_readmission: None,
            last_no_space_probe: None,
            highest_option_patch_id: None,
            last_option_patch_persistence: None,
            last_option_patch_application: None,
            last_host_key_resolution: None,
            last_stopped_deletion: None,
        }
    }

    fn queue_class(&self) -> Option<QueueClass> {
        match self.state {
            TaskState::Accepted | TaskState::Waiting => Some(QueueClass::Waiting),
            TaskState::WaitingSlow => Some(QueueClass::Demoted),
            TaskState::Allocating
            | TaskState::Active
            | TaskState::Verifying
            | TaskState::Seeding => Some(QueueClass::Active),
            TaskState::RetryWait => Some(if self.slot.owns_slot() {
                QueueClass::Active
            } else {
                QueueClass::Waiting
            }),
            TaskState::Paused | TaskState::PausedSlow | TaskState::PausedHostKey => {
                Some(QueueClass::Paused)
            }
            TaskState::PausedRestarting => Some(if self.slot.owns_slot() {
                QueueClass::Active
            } else {
                QueueClass::Waiting
            }),
            TaskState::Complete | TaskState::Error | TaskState::Removed => {
                if matches!(
                    self.pending_barrier,
                    Some(PendingBarrier::TerminalPersistence { .. })
                ) {
                    Some(QueueClass::Stopped)
                } else {
                    self.slot.owns_slot().then_some(QueueClass::Active)
                }
            }
            TaskState::StoppedResult => Some(QueueClass::Stopped),
        }
    }

    fn snapshot(&self, retry_wait_holds_slot: bool) -> Result<TaskSnapshot, SchedulerError> {
        let snapshot = TaskSnapshot {
            gid: self.gid,
            state: self.state,
            generation: self.generation,
            total_length: None,
            completed_length: 0,
            durable_length: 0,
            current_speed: 0,
            average_speed: 0,
            active_leases: 0,
            retry_wait_leases: 0,
            retry_wait_until: self.retry_timer.map(|(_, at)| at),
            last_progress_at: None,
            conditions: self.conditions.snapshot(),
            desired_paused: self.desired_paused,
            retry_wait_holds_slot: retry_wait_holds_slot
                && self.slot == SlotOwnership::RetryRetained,
            stopped_status: self.stopped_status,
            host_key_challenge: self
                .host_key_challenge
                .as_ref()
                .map(|challenge| challenge.summary().clone()),
            error: self.error.clone(),
            terminal_persisted: self.terminal_persisted,
        };
        snapshot
            .validate()
            .map_err(|_| SchedulerError::InternalInvariant)?;
        Ok(snapshot)
    }

    fn view(&self) -> SchedulerTaskView {
        SchedulerTaskView {
            task_id: self.task_id,
            gid: self.gid,
            state: self.state,
            generation: self.generation,
            queue: self.queue_class(),
            slot: self.slot,
            desired_paused: self.desired_paused,
            conditions: self.conditions.snapshot(),
            pending_barrier: self.pending_barrier,
            retry_timer: self.retry_timer.map(|(id, _)| id),
            slow_readmission: self.slow_readmission.map(|(id, _)| id),
            slow_demotion_count: self.slow_demotion_count,
            slow_slot: self.slow_slot,
            no_space_probe: self.no_space_probe.map(|(id, _)| id),
            pending_option_patch: self.pending_option_patch,
            pending_option_patch_mode: self.pending_option_patch_mode,
            pending_credential_requirement: self.pending_credential_requirement,
            pending_user_control: self.pending_user_control,
            pending_source_replacement: self.pending_source_replacement,
        }
    }

    fn clear_generation_event_history(&mut self) {
        self.seen_events.clear();
        self.last_retry_timer = None;
        self.last_slow_readmission = None;
        self.last_no_space_probe = None;
        self.last_option_patch_persistence = None;
        self.last_option_patch_application = None;
        self.last_host_key_resolution = None;
        self.slow_slot = None;
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SchedulerQueues {
    waiting: Vec<Gid>,
    demoted: Vec<Gid>,
    paused: Vec<Gid>,
    active: Vec<Gid>,
    stopped: Vec<Gid>,
}

impl SchedulerQueues {
    fn get(&self, class: QueueClass) -> &[Gid] {
        match class {
            QueueClass::Waiting => &self.waiting,
            QueueClass::Demoted => &self.demoted,
            QueueClass::Paused => &self.paused,
            QueueClass::Active => &self.active,
            QueueClass::Stopped => &self.stopped,
        }
    }

    fn get_mut(&mut self, class: QueueClass) -> &mut Vec<Gid> {
        match class {
            QueueClass::Waiting => &mut self.waiting,
            QueueClass::Demoted => &mut self.demoted,
            QueueClass::Paused => &mut self.paused,
            QueueClass::Active => &mut self.active,
            QueueClass::Stopped => &mut self.stopped,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IdCounters {
    retry_timer: u64,
    slow_readmission: u64,
    no_space_probe: u64,
    host_key_resolution: u64,
    stopped_deletion: u64,
}

impl Default for IdCounters {
    fn default() -> Self {
        Self {
            retry_timer: 1,
            slow_readmission: 1,
            no_space_probe: 1,
            host_key_resolution: 1,
            stopped_deletion: 1,
        }
    }
}

impl IdCounters {
    fn retry_timer(&mut self) -> Result<RetryTimerId, SchedulerError> {
        next_id(&mut self.retry_timer, RetryTimerId::new)
    }

    fn slow_readmission(&mut self) -> Result<SlowReadmissionId, SchedulerError> {
        next_id(&mut self.slow_readmission, SlowReadmissionId::new)
    }

    fn no_space_probe(&mut self) -> Result<NoSpaceProbeId, SchedulerError> {
        next_id(&mut self.no_space_probe, NoSpaceProbeId::new)
    }

    fn host_key_resolution(&mut self) -> Result<HostKeyResolutionId, SchedulerError> {
        next_id(&mut self.host_key_resolution, HostKeyResolutionId::new)
    }

    fn stopped_deletion(&mut self) -> Result<StoppedResultDeletionId, SchedulerError> {
        next_id(&mut self.stopped_deletion, StoppedResultDeletionId::new)
    }
}

fn next_id<T>(
    next: &mut u64,
    constructor: impl FnOnce(u64) -> Option<T>,
) -> Result<T, SchedulerError> {
    let value = constructor(*next).ok_or(SchedulerError::InternalInvariant)?;
    *next = next.checked_add(1).unwrap_or(0);
    Ok(value)
}

/// One recovery-normalized task accepted by [`RequestScheduler::restore`].
///
/// Filesystem, option, URI, and durable-piece payloads remain owned by their
/// storage/config adapters. This value contains only scheduler-owned state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredSchedulerTask {
    pub task_id: TaskId,
    pub gid: Gid,
    pub state: TaskState,
    pub generation: Generation,
    pub generation_started: bool,
    pub desired_paused: bool,
    pub conditions: TaskConditions,
    pub slow_demotion_count: u32,
    pub slow_slot: Option<SlowSlotPersistence>,
    pub retry_at: Option<MonotonicInstant>,
    pub host_key_challenge: Option<PresentedHostKeyChallenge>,
    pub error: Option<PublicError>,
    pub stopped_status: Option<Aria2Status>,
}

/// Complete, exact scheduler membership reconstructed by startup recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerRestoreBatch {
    pub tasks: Vec<RecoveredSchedulerTask>,
    pub queues: Vec<QueueOrder>,
}

impl SchedulerRestoreBatch {
    #[must_use]
    pub fn new(tasks: Vec<RecoveredSchedulerTask>, queues: Vec<QueueOrder>) -> Self {
        Self { tasks, queues }
    }
}

/// Bounded, move-only initial effects required to make recovered timers and
/// snapshots visible through the normal ordered dispatcher.
///
/// A plan is the sole authority to publish one restored scheduler lineage and
/// therefore cannot be cloned for replay through another driver.
///
/// ```compile_fail
/// use ariax_core::SchedulerRestorePlan;
///
/// fn duplicate(plan: SchedulerRestorePlan) {
///     let _second_authority = plan.clone();
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct SchedulerRestorePlan {
    effects: VecDeque<TransitionEffect>,
    initial_effect_count: usize,
    bound_scheduler: RequestScheduler,
}

impl SchedulerRestorePlan {
    /// Returns whether this plan was produced for the scheduler's exact current
    /// state, correlation identities, snapshots, and queue orders.
    #[must_use]
    pub fn is_bound_to(&self, scheduler: &RequestScheduler) -> bool {
        self.effects.len() == self.initial_effect_count && &self.bound_scheduler == scheduler
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.effects.len()
    }

    /// Removes the next dispatcher-sized effect batch.
    pub fn next_batch(&mut self) -> Vec<TransitionEffect> {
        let count = self.effects.len().min(crate::MAX_SCHEDULER_EFFECTS);
        self.effects.drain(..count).collect()
    }
}

/// Why an atomic scheduler recovery batch was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerRestoreError {
    TaskLimitReached,
    DuplicateTaskId(TaskId),
    DuplicateGid(Gid),
    MissingQueue(QueueClass),
    DuplicateQueue(QueueClass),
    DuplicateQueueMember(Gid),
    UnknownQueueTask(Gid),
    MissingQueueTask(Gid),
    QueueClassMismatch {
        gid: Gid,
        expected: QueueClass,
        actual: QueueClass,
    },
    InvalidState {
        gid: Gid,
        state: TaskState,
    },
    InvalidConditions(Gid),
    InvalidRetryWait(Gid),
    InvalidSlowState(Gid),
    InvalidHostKeyState(Gid),
    InvalidStoppedResult(Gid),
    UnexpectedTaskMetadata(Gid),
    InvalidSnapshot(Gid),
    CorrelationIdExhausted,
}

impl fmt::Display for SchedulerRestoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TaskLimitReached => formatter.write_str("recovered task limit exceeded"),
            Self::DuplicateTaskId(task_id) => {
                write!(formatter, "duplicate recovered task id {}", task_id.get())
            }
            Self::DuplicateGid(gid) => write!(formatter, "duplicate recovered GID {gid}"),
            Self::MissingQueue(class) => {
                write!(formatter, "missing recovered {} queue", class.code())
            }
            Self::DuplicateQueue(class) => {
                write!(formatter, "duplicate recovered {} queue", class.code())
            }
            Self::DuplicateQueueMember(gid) => {
                write!(
                    formatter,
                    "recovered GID {gid} occurs in multiple queue positions"
                )
            }
            Self::UnknownQueueTask(gid) => {
                write!(formatter, "recovered queue references unknown GID {gid}")
            }
            Self::MissingQueueTask(gid) => {
                write!(formatter, "recovered GID {gid} has no queue membership")
            }
            Self::QueueClassMismatch {
                gid,
                expected,
                actual,
            } => write!(
                formatter,
                "recovered GID {gid} belongs to {} queue, not {}",
                expected.code(),
                actual.code()
            ),
            Self::InvalidState { gid, state } => write!(
                formatter,
                "recovered GID {gid} uses unsafe state {}",
                state.code()
            ),
            Self::InvalidConditions(gid) => {
                write!(formatter, "recovered GID {gid} has invalid conditions")
            }
            Self::InvalidRetryWait(gid) => {
                write!(
                    formatter,
                    "recovered GID {gid} has invalid retry-wait metadata"
                )
            }
            Self::InvalidSlowState(gid) => {
                write!(
                    formatter,
                    "recovered GID {gid} has invalid slow-slot metadata"
                )
            }
            Self::InvalidHostKeyState(gid) => {
                write!(
                    formatter,
                    "recovered GID {gid} has invalid host-key metadata"
                )
            }
            Self::InvalidStoppedResult(gid) => {
                write!(
                    formatter,
                    "recovered GID {gid} has invalid stopped-result metadata"
                )
            }
            Self::UnexpectedTaskMetadata(gid) => {
                write!(
                    formatter,
                    "recovered GID {gid} has metadata outside its state"
                )
            }
            Self::InvalidSnapshot(gid) => {
                write!(
                    formatter,
                    "recovered GID {gid} cannot publish a valid snapshot"
                )
            }
            Self::CorrelationIdExhausted => {
                formatter.write_str("recovered scheduler correlation id exhausted")
            }
        }
    }
}

impl Error for SchedulerRestoreError {}

/// Deterministic, bounded in-memory owner of task state, queue membership, and
/// scheduler-issued correlation identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestScheduler {
    config: SchedulerConfig,
    tasks: BTreeMap<Gid, ScheduledTask>,
    task_ids: BTreeMap<TaskId, Gid>,
    highest_task_id: Option<TaskId>,
    queues: SchedulerQueues,
    published_snapshots: BTreeMap<Gid, TaskSnapshot>,
    ids: IdCounters,
}

impl RequestScheduler {
    /// Changes only policy for future decisions; accepted timers and task intent remain intact.
    pub fn configure_queue_policies(
        &mut self,
        retry_wait_holds_slot: bool,
        slow_readmission_policy: crate::SlowReadmissionPolicy,
    ) {
        self.config.retry_wait_holds_slot = retry_wait_holds_slot;
        self.config.slow_readmission_policy = slow_readmission_policy;
    }

    #[must_use]
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            tasks: BTreeMap::new(),
            task_ids: BTreeMap::new(),
            highest_task_id: None,
            queues: SchedulerQueues::default(),
            published_snapshots: BTreeMap::new(),
            ids: IdCounters::default(),
        }
    }

    /// Atomically reconstructs scheduler-owned state after cross-store startup
    /// recovery has normalized durable and session authorities.
    pub fn restore(
        config: SchedulerConfig,
        batch: SchedulerRestoreBatch,
    ) -> Result<(Self, SchedulerRestorePlan), SchedulerRestoreError> {
        if batch.tasks.len() > config.max_tasks.get() {
            return Err(SchedulerRestoreError::TaskLimitReached);
        }

        let mut queues = SchedulerQueues::default();
        let mut seen_classes = BTreeSet::new();
        let mut memberships = BTreeMap::new();
        for order in batch.queues {
            if !seen_classes.insert(order.class) {
                return Err(SchedulerRestoreError::DuplicateQueue(order.class));
            }
            if order.order.len() > config.max_tasks.get() {
                return Err(SchedulerRestoreError::TaskLimitReached);
            }
            for gid in order.order.iter().copied() {
                if memberships.insert(gid, order.class).is_some() {
                    return Err(SchedulerRestoreError::DuplicateQueueMember(gid));
                }
            }
            *queues.get_mut(order.class) = order.order;
        }
        for class in ALL_QUEUE_CLASSES.iter().copied() {
            if !seen_classes.contains(&class) {
                return Err(SchedulerRestoreError::MissingQueue(class));
            }
        }

        let mut scheduler = Self::new(config);
        let mut timer_effects: BTreeMap<Gid, Vec<TransitionEffect>> = BTreeMap::new();
        for recovered in batch.tasks {
            if scheduler.tasks.contains_key(&recovered.gid) {
                return Err(SchedulerRestoreError::DuplicateGid(recovered.gid));
            }
            if scheduler.task_ids.contains_key(&recovered.task_id) {
                return Err(SchedulerRestoreError::DuplicateTaskId(recovered.task_id));
            }
            recovered
                .conditions
                .validate()
                .map_err(|_| SchedulerRestoreError::InvalidConditions(recovered.gid))?;
            Self::validate_recovered_task(&recovered, config.max_tasks.get())?;

            let mut task = ScheduledTask::new(
                recovered.task_id,
                recovered.gid,
                recovered.desired_paused,
                recovered.conditions,
            );
            task.state = recovered.state;
            task.generation = recovered.generation;
            task.generation_started = recovered.generation_started;
            task.slow_demotion_count = recovered.slow_demotion_count;
            task.slow_slot = recovered.slow_slot;
            task.slow_remaining_position =
                recovered.slow_slot.map_or(0, |slot| slot.original_position);
            task.host_key_challenge = recovered.host_key_challenge;
            task.error = recovered.error;
            task.stopped_status = recovered.stopped_status;
            task.terminal_persisted = recovered.state == TaskState::StoppedResult;

            match recovered.state {
                TaskState::RetryWait => {
                    let at = recovered
                        .retry_at
                        .ok_or(SchedulerRestoreError::InvalidRetryWait(recovered.gid))?;
                    let retry_timer_id = scheduler
                        .ids
                        .retry_timer()
                        .map_err(|_| SchedulerRestoreError::CorrelationIdExhausted)?;
                    task.retry_timer = Some((retry_timer_id, at));
                    timer_effects.entry(recovered.gid).or_default().push(
                        TransitionEffect::ScheduleRetry {
                            task_id: recovered.task_id,
                            gid: recovered.gid,
                            generation: recovered.generation,
                            retry_timer_id,
                            at,
                        },
                    );
                }
                TaskState::WaitingSlow => {
                    let slow_slot = recovered
                        .slow_slot
                        .ok_or(SchedulerRestoreError::InvalidSlowState(recovered.gid))?;
                    let readmission_id = scheduler
                        .ids
                        .slow_readmission()
                        .map_err(|_| SchedulerRestoreError::CorrelationIdExhausted)?;
                    task.slow_readmission = Some((readmission_id, slow_slot.decision.readmit_at));
                    timer_effects.entry(recovered.gid).or_default().push(
                        TransitionEffect::ScheduleSlowReadmission {
                            task_id: recovered.task_id,
                            gid: recovered.gid,
                            generation: recovered.generation,
                            readmission_id,
                            at: slow_slot.decision.readmit_at,
                        },
                    );
                }
                _ => {}
            }
            if let Some(at) = task
                .conditions
                .no_space
                .as_ref()
                .and_then(|condition| condition.retry_at)
            {
                let probe_id = scheduler
                    .ids
                    .no_space_probe()
                    .map_err(|_| SchedulerRestoreError::CorrelationIdExhausted)?;
                task.no_space_probe = Some((probe_id, NoSpaceProbeOrigin::AutomaticRetry));
                timer_effects.entry(recovered.gid).or_default().push(
                    TransitionEffect::ProbeNoSpace {
                        task_id: recovered.task_id,
                        gid: recovered.gid,
                        generation: recovered.generation,
                        probe_id,
                        origin: NoSpaceProbeOrigin::AutomaticRetry,
                        at,
                    },
                );
            }

            scheduler.highest_task_id = Some(
                scheduler
                    .highest_task_id
                    .map_or(recovered.task_id, |current| current.max(recovered.task_id)),
            );
            scheduler.task_ids.insert(recovered.task_id, recovered.gid);
            scheduler.tasks.insert(recovered.gid, task);
        }

        if memberships.len() != scheduler.tasks.len() {
            if let Some(gid) = scheduler
                .tasks
                .keys()
                .copied()
                .find(|gid| !memberships.contains_key(gid))
            {
                return Err(SchedulerRestoreError::MissingQueueTask(gid));
            }
            if let Some(gid) = memberships
                .keys()
                .copied()
                .find(|gid| !scheduler.tasks.contains_key(gid))
            {
                return Err(SchedulerRestoreError::UnknownQueueTask(gid));
            }
            return Err(SchedulerRestoreError::TaskLimitReached);
        }
        for (gid, actual) in memberships.iter().map(|(gid, class)| (*gid, *class)) {
            let task = scheduler
                .tasks
                .get(&gid)
                .ok_or(SchedulerRestoreError::UnknownQueueTask(gid))?;
            let expected = task
                .queue_class()
                .ok_or(SchedulerRestoreError::InvalidState {
                    gid,
                    state: task.state,
                })?;
            if expected != actual {
                return Err(SchedulerRestoreError::QueueClassMismatch {
                    gid,
                    expected,
                    actual,
                });
            }
        }
        scheduler.queues = queues;

        let mut effects = VecDeque::new();
        for class in ALL_QUEUE_CLASSES.iter().copied() {
            for gid in scheduler.queues.get(class).iter().copied() {
                if let Some(task_effects) = timer_effects.remove(&gid) {
                    effects.extend(task_effects);
                }
                let task = scheduler
                    .tasks
                    .get(&gid)
                    .ok_or(SchedulerRestoreError::UnknownQueueTask(gid))?;
                let snapshot = task
                    .snapshot(config.retry_wait_holds_slot)
                    .map_err(|_| SchedulerRestoreError::InvalidSnapshot(gid))?;
                scheduler.published_snapshots.insert(gid, snapshot.clone());
                effects.push_back(TransitionEffect::PublishSnapshot {
                    task_id: task.task_id,
                    snapshot,
                });
            }
        }
        debug_assert!(timer_effects.is_empty());

        let bound_scheduler = scheduler.clone();
        let initial_effect_count = effects.len();
        Ok((
            scheduler,
            SchedulerRestorePlan {
                effects,
                initial_effect_count,
                bound_scheduler,
            },
        ))
    }

    fn validate_recovered_task(
        task: &RecoveredSchedulerTask,
        max_tasks: usize,
    ) -> Result<(), SchedulerRestoreError> {
        if !matches!(
            task.state,
            TaskState::Waiting
                | TaskState::WaitingSlow
                | TaskState::RetryWait
                | TaskState::Paused
                | TaskState::PausedSlow
                | TaskState::PausedHostKey
                | TaskState::StoppedResult
        ) {
            return Err(SchedulerRestoreError::InvalidState {
                gid: task.gid,
                state: task.state,
            });
        }

        if (task.state == TaskState::RetryWait) != task.retry_at.is_some() {
            return Err(SchedulerRestoreError::InvalidRetryWait(task.gid));
        }
        if task.state == TaskState::WaitingSlow {
            let slow_slot = task
                .slow_slot
                .ok_or(SchedulerRestoreError::InvalidSlowState(task.gid))?;
            if task.slow_demotion_count == 0
                || slow_slot.demotion_count != task.slow_demotion_count
                || slow_slot.original_position >= max_tasks
                || slow_slot.decision.delay_ms == 0
                || slow_slot.decision.scheduled_at_ms > MAX_PERSISTED_MILLISECONDS
            {
                return Err(SchedulerRestoreError::InvalidSlowState(task.gid));
            }
        } else if task.slow_slot.is_some() {
            return Err(SchedulerRestoreError::InvalidSlowState(task.gid));
        }

        if (task.state == TaskState::PausedHostKey) != task.host_key_challenge.is_some() {
            return Err(SchedulerRestoreError::InvalidHostKeyState(task.gid));
        }
        if task.state == TaskState::StoppedResult {
            let status = task
                .stopped_status
                .ok_or(SchedulerRestoreError::InvalidStoppedResult(task.gid))?;
            if !status.is_terminal() || (status == Aria2Status::Error) != task.error.is_some() {
                return Err(SchedulerRestoreError::InvalidStoppedResult(task.gid));
            }
        } else if task.stopped_status.is_some() || task.error.is_some() {
            return Err(SchedulerRestoreError::UnexpectedTaskMetadata(task.gid));
        }
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Conservative heap reservation for a planning clone, including sparse
    /// tree nodes and nested text/key storage. This method does not allocate.
    #[must_use]
    pub fn estimated_clone_bytes(&self) -> usize {
        let mut bytes = tree_clone_bytes::<Gid, ScheduledTask>(self.tasks.len())
            .saturating_add(tree_clone_bytes::<TaskId, Gid>(self.task_ids.len()))
            .saturating_add(tree_clone_bytes::<Gid, TaskSnapshot>(
                self.published_snapshots.len(),
            ));
        for task in self.tasks.values() {
            bytes = bytes
                .saturating_add(tree_clone_bytes::<TaskEventKind, ()>(
                    task.seen_events.len(),
                ))
                .saturating_add(
                    task.error
                        .as_ref()
                        .map_or(0, |error| error.safe_message().len().saturating_add(64)),
                );
            if let Some(requirement) = &task.conditions.needs_credentials {
                bytes = bytes
                    .saturating_add(requirement.safe_description.len())
                    .saturating_add(64);
            }
            if let Some(condition) = &task.conditions.no_space {
                bytes = bytes
                    .saturating_add(condition.redacted_path.len())
                    .saturating_add(64);
            }
            if let Some(challenge) = &task.host_key_challenge {
                bytes = bytes
                    .saturating_add(challenge.summary().canonical_host.len())
                    .saturating_add(challenge.summary().algorithm.len())
                    .saturating_add(challenge.presented_public_key().len())
                    .saturating_add(192);
            }
        }
        for snapshot in self.published_snapshots.values() {
            bytes = bytes.saturating_add(snapshot.estimated_clone_bytes());
        }
        for class in ALL_QUEUE_CLASSES {
            bytes = bytes.saturating_add(
                self.queues
                    .get(*class)
                    .len()
                    .saturating_mul(std::mem::size_of::<Gid>())
                    .saturating_add(64),
            );
        }
        bytes
    }

    /// Returns the scheduler's current planned state, which may be ahead of
    /// persistence acknowledgements and must not be exposed as a public snapshot.
    #[must_use]
    pub fn task(&self, gid: Gid) -> Option<SchedulerTaskView> {
        self.tasks.get(&gid).map(ScheduledTask::view)
    }

    #[must_use]
    pub fn credential_requirement_key(&self, gid: Gid) -> Option<CredentialRequirementKey> {
        self.tasks.get(&gid).and_then(|task| {
            task.conditions
                .needs_credentials
                .as_ref()
                .map(crate::CredentialRequirement::key)
        })
    }

    /// Returns the last snapshot emitted through `PublishSnapshot`.
    pub fn snapshot(&self, gid: Gid) -> Result<TaskSnapshot, SchedulerError> {
        Ok(self
            .published_snapshots
            .get(&gid)
            .ok_or(SchedulerError::TaskNotFound)?
            .clone())
    }

    /// Returns the scheduler's current in-memory queue plan. Persistence may
    /// still be awaiting an acknowledgement for the owning transition.
    #[must_use]
    pub fn queue_snapshot(&self, class: QueueClass) -> Vec<Gid> {
        self.queues.get(class).to_vec()
    }

    #[must_use]
    pub fn active_slot_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| task.slot.owns_slot())
            .count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueUpdate {
    class: QueueClass,
    order: Vec<Gid>,
}

fn tree_clone_bytes<K, V>(count: usize) -> usize {
    if count == 0 {
        return 0;
    }
    count.saturating_add(4).saturating_mul(
        std::mem::size_of::<(K, V)>()
            .saturating_mul(3)
            .saturating_add(128),
    )
}

impl RequestScheduler {
    fn task_clone(&self, gid: Gid) -> Result<ScheduledTask, SchedulerError> {
        self.tasks
            .get(&gid)
            .cloned()
            .ok_or(SchedulerError::TaskNotFound)
    }

    fn reject_pending(task: &ScheduledTask, operation: &'static str) -> Result<(), SchedulerError> {
        if let Some(barrier) = task.pending_barrier {
            return Err(SchedulerError::PendingBarrier { barrier, operation });
        }
        Ok(())
    }

    fn defers_user_control(barrier: PendingBarrier) -> bool {
        matches!(
            barrier,
            PendingBarrier::OptionPatchPersistence { .. }
                | PendingBarrier::OptionPatchApplication { .. }
                | PendingBarrier::HostKeyResolution { .. }
        )
    }

    fn pending_stopped_deletion(&self) -> Option<PendingBarrier> {
        self.tasks
            .values()
            .find_map(|task| match task.pending_barrier {
                Some(barrier @ PendingBarrier::StoppedResultDeletion { .. }) => Some(barrier),
                _ => None,
            })
    }

    fn target_for_action(
        task: &ScheduledTask,
        action: SchedulerAction,
    ) -> Result<Option<TaskState>, SchedulerError> {
        let contract = transition_contract(task.state, action);
        match contract.kind() {
            TransitionContractKind::Transition | TransitionContractKind::Stay => contract
                .targets()
                .first()
                .copied()
                .map(Some)
                .ok_or(SchedulerError::InternalInvariant),
            TransitionContractKind::Delete => Ok(None),
            TransitionContractKind::NoOp | TransitionContractKind::Ignore => Ok(Some(task.state)),
            TransitionContractKind::Conditional => Err(SchedulerError::ShutdownBatchRequired),
            TransitionContractKind::Conflict => Err(match contract.rejection() {
                Some(TransitionRejection::HostKeyApprovalRequired) => {
                    SchedulerError::HostKeyApprovalRequired
                }
                Some(TransitionRejection::Conflict) | None => SchedulerError::Conflict {
                    state: task.state,
                    operation: action.code(),
                },
            }),
        }
    }

    fn queue_updates(
        &self,
        gid: Gid,
        old_class: Option<QueueClass>,
        new_class: Option<QueueClass>,
    ) -> Result<Vec<QueueUpdate>, SchedulerError> {
        let memberships = [
            QueueClass::Waiting,
            QueueClass::Demoted,
            QueueClass::Paused,
            QueueClass::Active,
            QueueClass::Stopped,
        ]
        .into_iter()
        .filter(|class| self.queues.get(*class).contains(&gid))
        .collect::<Vec<_>>();

        match old_class {
            Some(class) if memberships.as_slice() != [class] => {
                return Err(SchedulerError::InternalInvariant);
            }
            None if !memberships.is_empty() => return Err(SchedulerError::InternalInvariant),
            Some(_) | None => {}
        }

        if old_class == new_class {
            return Ok(Vec::new());
        }

        let mut updates = Vec::with_capacity(2);
        if let Some(class) = old_class {
            let mut order = self.queues.get(class).to_vec();
            let position = order
                .iter()
                .position(|candidate| *candidate == gid)
                .ok_or(SchedulerError::InternalInvariant)?;
            order.remove(position);
            updates.push(QueueUpdate { class, order });
        }
        if let Some(class) = new_class {
            let mut order = if old_class == Some(class) {
                updates
                    .iter()
                    .find(|update| update.class == class)
                    .map(|update| update.order.clone())
                    .ok_or(SchedulerError::InternalInvariant)?
            } else {
                self.queues.get(class).to_vec()
            };
            if order.contains(&gid) {
                return Err(SchedulerError::InternalInvariant);
            }
            order.push(gid);
            updates.push(QueueUpdate { class, order });
        }
        Ok(updates)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the atomic plan keeps mutation, visibility, and id commits explicit"
    )]
    fn finish_action(
        &mut self,
        original: ScheduledTask,
        updated: Option<ScheduledTask>,
        original_present: bool,
        action: SchedulerAction,
        at: MonotonicInstant,
        mut effects: Vec<TransitionEffect>,
        publish: bool,
        ids: IdCounters,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let contract = transition_contract(original.state, action);
        if !contract.accepts() {
            return Self::target_for_action(&original, action)
                .and(Err(SchedulerError::InternalInvariant));
        }

        let old_class = original_present.then(|| original.queue_class()).flatten();
        let new_class = updated.as_ref().and_then(ScheduledTask::queue_class);
        let queue_updates = self.queue_updates(original.gid, old_class, new_class)?;
        if action != SchedulerAction::StoppedResultDeletionSucceeded
            && queue_updates
                .iter()
                .any(|update| update.class == QueueClass::Stopped)
            && let Some(barrier) = self.pending_stopped_deletion()
        {
            return Err(SchedulerError::PendingBarrier {
                barrier,
                operation: action.code(),
            });
        }

        let desired_paused = updated
            .as_ref()
            .map_or(original.desired_paused, |task| task.desired_paused);
        let desired_changed = updated
            .as_ref()
            .is_some_and(|task| task.desired_paused != original.desired_paused);
        let slow_changed = updated.as_ref().is_some_and(|task| {
            task.slow_demotion_count != original.slow_demotion_count
                || task.slow_slot != original.slow_slot
        });
        let terminal_persistence = updated
            .as_ref()
            .and_then(|task| match task.pending_barrier {
                Some(PendingBarrier::TerminalPersistence { generation, status })
                    if original.pending_barrier != task.pending_barrier =>
                {
                    Some((generation, status, task.error.clone()))
                }
                _ => None,
            });
        if let Some((generation, status, error)) = terminal_persistence {
            let from = old_class.ok_or(SchedulerError::InternalInvariant)?;
            let to = new_class.ok_or(SchedulerError::InternalInvariant)?;
            if to != QueueClass::Stopped || queue_updates.is_empty() {
                return Err(SchedulerError::InternalInvariant);
            }
            effects.push(TransitionEffect::PersistTerminal {
                task_id: original.task_id,
                gid: original.gid,
                generation,
                status,
                error,
                from,
                to,
                desired_paused,
                slow_demotion_count: updated
                    .as_ref()
                    .map_or(original.slow_demotion_count, |task| {
                        task.slow_demotion_count
                    }),
                slow_slot: updated.as_ref().and_then(|task| task.slow_slot),
                orders: queue_updates
                    .iter()
                    .map(|update| QueueOrder {
                        class: update.class,
                        order: update.order.clone(),
                    })
                    .collect(),
            });
        } else if original_present
            && updated.is_some()
            && (!queue_updates.is_empty() || desired_changed || slow_changed)
        {
            let orders = if queue_updates.is_empty() {
                let class = old_class
                    .filter(|class| new_class == Some(*class))
                    .ok_or(SchedulerError::InternalInvariant)?;
                vec![QueueOrder {
                    class,
                    order: self.queues.get(class).to_vec(),
                }]
            } else {
                queue_updates
                    .iter()
                    .map(|update| QueueOrder {
                        class: update.class,
                        order: update.order.clone(),
                    })
                    .collect()
            };
            let queue_effect = TransitionEffect::PersistQueueTransition {
                task_id: original.task_id,
                gid: original.gid,
                from: old_class,
                to: new_class,
                desired_paused,
                slow_demotion_count: updated
                    .as_ref()
                    .map_or(original.slow_demotion_count, |task| {
                        task.slow_demotion_count
                    }),
                slow_slot: updated.as_ref().and_then(|task| task.slow_slot),
                orders,
            };
            if action == SchedulerAction::ExplicitNoSpaceProbeRequested {
                effects.insert(0, queue_effect);
            } else {
                effects.push(queue_effect);
            }
        }

        let published_snapshot = if publish {
            Some(
                updated
                    .as_ref()
                    .ok_or(SchedulerError::InternalInvariant)?
                    .snapshot(self.config.retry_wait_holds_slot)?,
            )
        } else {
            None
        };
        if let Some(snapshot) = published_snapshot.as_ref() {
            effects.push(TransitionEffect::PublishSnapshot {
                task_id: original.task_id,
                snapshot: snapshot.clone(),
            });
        }

        let (transition, deletion) = match contract.kind() {
            TransitionContractKind::Transition | TransitionContractKind::Stay => {
                let task = updated.as_ref().ok_or(SchedulerError::InternalInvariant)?;
                if !contract.targets().contains(&task.state) {
                    return Err(SchedulerError::InternalInvariant);
                }
                (
                    Some(StateTransition {
                        task: task.task_id,
                        gid: task.gid,
                        generation: task.generation,
                        from: original.state,
                        to: task.state,
                        reason: contract.reason(),
                        at,
                    }),
                    None,
                )
            }
            TransitionContractKind::Delete => {
                if updated.is_some() {
                    return Err(SchedulerError::InternalInvariant);
                }
                (
                    None,
                    Some(TaskDeletion {
                        task: original.task_id,
                        gid: original.gid,
                        generation: original.generation,
                        from: original.state,
                        reason: contract.reason(),
                        at,
                    }),
                )
            }
            TransitionContractKind::NoOp | TransitionContractKind::Ignore => {
                return Err(SchedulerError::InternalInvariant);
            }
            TransitionContractKind::Conditional => {
                return Err(SchedulerError::ShutdownBatchRequired);
            }
            TransitionContractKind::Conflict => {
                return Err(SchedulerError::InternalInvariant);
            }
        };

        let outcome = SchedulerOutcome::checked(transition, deletion, effects)?;

        for update in queue_updates {
            *self.queues.get_mut(update.class) = update.order;
        }
        match updated {
            Some(task) => {
                self.task_ids.insert(task.task_id, task.gid);
                self.tasks.insert(task.gid, task);
            }
            None => {
                self.tasks.remove(&original.gid);
                self.task_ids.remove(&original.task_id);
                self.published_snapshots.remove(&original.gid);
            }
        }
        if let Some(snapshot) = published_snapshot {
            self.published_snapshots.insert(original.gid, snapshot);
        }
        self.ids = ids;
        Ok(outcome)
    }

    fn ignored_outcome(
        task: &ScheduledTask,
        action: SchedulerAction,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let contract = transition_contract(task.state, action);
        if contract.kind() != TransitionContractKind::Ignore {
            return Err(SchedulerError::InternalInvariant);
        }
        SchedulerOutcome::checked(
            Some(StateTransition {
                task: task.task_id,
                gid: task.gid,
                generation: task.generation,
                from: task.state,
                to: task.state,
                reason: contract.reason(),
                at,
            }),
            None,
            Vec::new(),
        )
    }

    fn queue_effects_for_reorder(
        &mut self,
        gid: Gid,
        class: QueueClass,
        position: usize,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let current = self.queues.get(class);
        let queue_len = current.len();
        if position >= queue_len {
            return Err(SchedulerError::InvalidPosition {
                position,
                queue_len,
            });
        }
        let old_position = current
            .iter()
            .position(|candidate| *candidate == gid)
            .ok_or(SchedulerError::InternalInvariant)?;
        if old_position == position {
            return Ok(SchedulerOutcome::default());
        }
        let mut order = current.to_vec();
        order.remove(old_position);
        order.insert(position, gid);
        let task = self.tasks.get(&gid).ok_or(SchedulerError::TaskNotFound)?;
        let outcome = SchedulerOutcome::checked(
            None,
            None,
            vec![TransitionEffect::PersistQueueTransition {
                task_id: task.task_id,
                gid,
                from: Some(class),
                to: Some(class),
                desired_paused: task.desired_paused,
                slow_demotion_count: task.slow_demotion_count,
                slow_slot: task.slow_slot,
                orders: vec![QueueOrder {
                    class,
                    order: order.clone(),
                }],
            }],
        )?;
        *self.queues.get_mut(class) = order;
        Ok(outcome)
    }
}

impl RequestScheduler {
    fn release_slot(task: &mut ScheduledTask, effects: &mut Vec<TransitionEffect>) {
        if task.slot.owns_slot() {
            effects.push(TransitionEffect::ReleaseSlot {
                task_id: task.task_id,
                gid: task.gid,
                ownership: task.slot,
            });
            task.slot = SlotOwnership::None;
        }
    }

    fn cancel_timers(task: &mut ScheduledTask, effects: &mut Vec<TransitionEffect>) {
        if let Some((retry_timer_id, _)) = task.retry_timer.take() {
            effects.push(TransitionEffect::CancelRetry {
                task_id: task.task_id,
                gid: task.gid,
                generation: task.generation,
                retry_timer_id,
            });
        }
        task.retry_ready = false;
        if let Some((readmission_id, _)) = task.slow_readmission.take() {
            effects.push(TransitionEffect::CancelSlowReadmission {
                task_id: task.task_id,
                gid: task.gid,
                generation: task.generation,
                readmission_id,
            });
        }
        task.slow_readmission_ready = false;
        task.pending_slow_readmission = None;
        task.slow_slot = None;
    }

    fn schedule_no_space_probe(
        task: &mut ScheduledTask,
        origin: NoSpaceProbeOrigin,
        at: MonotonicInstant,
        effects: &mut Vec<TransitionEffect>,
        ids: &mut IdCounters,
    ) -> Result<(), SchedulerError> {
        if task.no_space_probe.is_some() {
            return Ok(());
        }
        if task.conditions.no_space.is_none() {
            return Err(SchedulerError::InternalInvariant);
        }
        let probe_id = ids.no_space_probe()?;
        task.no_space_probe = Some((probe_id, origin));
        effects.push(TransitionEffect::ProbeNoSpace {
            task_id: task.task_id,
            gid: task.gid,
            generation: task.generation,
            probe_id,
            origin,
            at,
        });
        Ok(())
    }

    fn begin_cancellation(
        task: &mut ScheduledTask,
        target: DrainTarget,
        force: bool,
        effects: &mut Vec<TransitionEffect>,
        operation: &'static str,
    ) -> Result<(), SchedulerError> {
        match task.pending_barrier {
            None | Some(PendingBarrier::GenerationPersistence { .. }) => {
                effects.push(TransitionEffect::CancelGeneration {
                    task_id: task.task_id,
                    gid: task.gid,
                    generation: task.generation,
                    force,
                });
            }
            Some(PendingBarrier::CancellationDrain {
                generation,
                force: existing_force,
                ..
            }) if generation == task.generation => {
                if force && !existing_force {
                    effects.push(TransitionEffect::CancelGeneration {
                        task_id: task.task_id,
                        gid: task.gid,
                        generation: task.generation,
                        force: true,
                    });
                }
                task.pending_barrier = Some(PendingBarrier::CancellationDrain {
                    generation,
                    target,
                    force: existing_force || force,
                });
                return Ok(());
            }
            Some(barrier) => {
                return Err(SchedulerError::PendingBarrier { barrier, operation });
            }
        }
        task.pending_barrier = Some(PendingBarrier::CancellationDrain {
            generation: task.generation,
            target,
            force,
        });
        Ok(())
    }

    fn begin_terminal(
        task: &mut ScheduledTask,
        status: Aria2Status,
        error: Option<PublicError>,
    ) -> Result<(), SchedulerError> {
        if !status.is_terminal() || (status == Aria2Status::Error && error.is_none()) {
            return Err(SchedulerError::InternalInvariant);
        }
        task.error = error.clone();
        task.stopped_status = None;
        task.terminal_persisted = false;
        task.pending_barrier = Some(PendingBarrier::TerminalPersistence {
            generation: task.generation,
            status,
        });
        Ok(())
    }

    fn complete_pending_user_control(
        original: &ScheduledTask,
        updated: &mut ScheduledTask,
        effects: &mut Vec<TransitionEffect>,
    ) -> Result<Option<SchedulerAction>, SchedulerError> {
        let Some(control) = updated.pending_user_control.take() else {
            return Ok(None);
        };
        match control {
            PendingUserControl::Pause { force } => {
                let action = SchedulerAction::DeferredPauseCompleted;
                updated.state = Self::target_for_action(original, action)?
                    .ok_or(SchedulerError::InternalInvariant)?;
                updated.desired_paused = true;
                if updated.slot.owns_slot() {
                    Self::begin_cancellation(
                        updated,
                        DrainTarget::Paused,
                        force,
                        effects,
                        "deferred_pause",
                    )?;
                }
                Ok(Some(action))
            }
            PendingUserControl::Resume => {
                let action = SchedulerAction::DeferredResumeCompleted;
                updated.state = Self::target_for_action(original, action)?
                    .ok_or(SchedulerError::InternalInvariant)?;
                updated.desired_paused = false;
                updated.slow_demotion_count = 0;
                Self::cancel_timers(updated, effects);
                Ok(Some(action))
            }
            PendingUserControl::Remove { force } => {
                let action = SchedulerAction::DeferredRemoveCompleted;
                updated.state = Self::target_for_action(original, action)?
                    .ok_or(SchedulerError::InternalInvariant)?;
                Self::cancel_timers(updated, effects);
                updated.no_space_probe = None;
                if let Some(challenge) = updated.host_key_challenge.take() {
                    effects.push(TransitionEffect::PersistHostKeyChallengeRejected {
                        task_id: updated.task_id,
                        gid: updated.gid,
                        challenge: challenge.summary().id,
                    });
                }
                updated.pending_option_patch = None;
                updated.pending_option_patch_mode = None;
                updated.pending_credential_requirement = None;
                if updated.slot.owns_slot() {
                    Self::begin_cancellation(
                        updated,
                        DrainTarget::Removed,
                        force,
                        effects,
                        "deferred_remove",
                    )?;
                } else {
                    Self::begin_terminal(updated, Aria2Status::Removed, None)?;
                }
                Ok(Some(action))
            }
        }
    }

    fn plan_admission(
        &self,
        task: &mut ScheduledTask,
        action: SchedulerAction,
        effects: &mut Vec<TransitionEffect>,
    ) -> Result<(), SchedulerError> {
        Self::reject_pending(task, action.code())?;
        if task.desired_paused || task.conditions.blocks_admission() {
            return Err(SchedulerError::NoEligibleTask);
        }
        if !task.slot.owns_slot() && self.active_slot_count() >= self.config.max_active_tasks.get()
        {
            return Err(SchedulerError::ActiveLimitReached);
        }
        let target =
            Self::target_for_action(task, action)?.ok_or(SchedulerError::InternalInvariant)?;
        if target != TaskState::Allocating {
            return Err(SchedulerError::InternalInvariant);
        }

        let generation = if task.generation_started {
            task.generation
                .checked_next()
                .ok_or(SchedulerError::GenerationExhausted)?
        } else {
            task.generation
        };
        if generation != task.generation {
            task.generation = generation;
            task.clear_generation_event_history();
        }
        task.generation_started = true;
        task.state = target;
        task.slot = SlotOwnership::Reserved;
        task.retry_ready = false;
        task.slow_readmission_ready = false;
        task.pending_barrier = Some(PendingBarrier::GenerationPersistence { generation });
        effects.push(TransitionEffect::PersistGenerationStarted {
            task_id: task.task_id,
            gid: task.gid,
            generation,
        });
        Ok(())
    }

    pub fn execute_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        self.execute_command_at(command, MonotonicInstant::now())
    }

    pub fn execute_command_at(
        &mut self,
        command: SchedulerCommand,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        match command {
            SchedulerCommand::AddValidatedTask {
                task_id,
                gid,
                desired_paused,
                conditions,
            } => self.add_validated_task(task_id, gid, desired_paused, conditions, at),
            SchedulerCommand::Pause { gid, force } => self.pause(gid, force, at),
            SchedulerCommand::Resume { gid } => self.resume(gid, at),
            SchedulerCommand::BeginSourceReplacement { gid } => {
                self.begin_source_replacement(gid, at)
            }
            SchedulerCommand::CommitSourceReplacement {
                gid,
                satisfies_credentials,
            } => self.commit_source_replacement(gid, satisfies_credentials, at),
            SchedulerCommand::ApproveHostKey {
                gid,
                challenge,
                fingerprint_sha256,
            } => self.approve_host_key(gid, challenge, fingerprint_sha256, at),
            SchedulerCommand::ApplyOptionPatch {
                gid,
                patch_id,
                kind,
                satisfies_credentials,
            } => self.apply_option_patch(gid, patch_id, kind, satisfies_credentials, at),
            SchedulerCommand::Remove { gid, force } => self.remove(gid, force, at),
            SchedulerCommand::RemoveStoppedResult { gid } => self.remove_stopped_result(gid, at),
            SchedulerCommand::ChangePosition { gid, position } => {
                let task = self.task_clone(gid)?;
                Self::reject_pending(&task, "change_position")?;
                let class = task.queue_class().ok_or(SchedulerError::Conflict {
                    state: task.state,
                    operation: "change_position",
                })?;
                if class == QueueClass::Stopped
                    && let Some(barrier) = self.pending_stopped_deletion()
                {
                    return Err(SchedulerError::PendingBarrier {
                        barrier,
                        operation: "change_position",
                    });
                }
                self.queue_effects_for_reorder(gid, class, position)
            }
            SchedulerCommand::OrderlyShutdown => Err(SchedulerError::ShutdownBatchRequired),
        }
    }

    fn add_validated_task(
        &mut self,
        task_id: TaskId,
        gid: Gid,
        desired_paused: bool,
        conditions: TaskConditions,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        conditions
            .validate()
            .map_err(|_| SchedulerError::InvalidTaskConditions)?;
        if self.tasks.len() >= self.config.max_tasks.get() {
            return Err(SchedulerError::TaskLimitReached);
        }
        if self.tasks.contains_key(&gid) {
            return Err(SchedulerError::GidCollision);
        }
        if self.task_ids.contains_key(&task_id) {
            return Err(SchedulerError::TaskIdCollision);
        }
        if self
            .highest_task_id
            .is_some_and(|highest_task_id| task_id <= highest_task_id)
        {
            return Err(SchedulerError::TaskIdCollision);
        }

        let original = ScheduledTask::new(task_id, gid, desired_paused, conditions.clone());
        let action = SchedulerAction::for_validated_add(desired_paused);
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let effects = vec![TransitionEffect::PersistTask {
            task_id,
            gid,
            queue: updated
                .queue_class()
                .ok_or(SchedulerError::InternalInvariant)?,
            position: self
                .queues
                .get(
                    updated
                        .queue_class()
                        .ok_or(SchedulerError::InternalInvariant)?,
                )
                .len(),
            desired_paused,
            slow_demotion_count: 0,
            conditions,
        }];
        let outcome = self.finish_action(
            original,
            Some(updated),
            false,
            action,
            at,
            effects,
            true,
            self.ids,
        )?;
        self.highest_task_id = Some(task_id);
        Ok(outcome)
    }

    fn pause(
        &mut self,
        gid: Gid,
        force: bool,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        if let Some(barrier) = original.pending_barrier
            && Self::defers_user_control(barrier)
        {
            if matches!(
                original.pending_user_control,
                Some(PendingUserControl::Remove { .. })
            ) {
                return Err(SchedulerError::PendingBarrier {
                    barrier,
                    operation: "pause_after_remove",
                });
            }
            let action = SchedulerAction::PauseDeferred;
            let mut updated = original.clone();
            let mut effects = Vec::new();
            updated.desired_paused = true;
            Self::cancel_timers(&mut updated, &mut effects);
            if !matches!(barrier, PendingBarrier::HostKeyResolution { .. }) {
                let force = force
                    || matches!(
                        original.pending_user_control,
                        Some(PendingUserControl::Pause { force: true })
                    );
                updated.pending_user_control = Some(PendingUserControl::Pause { force });
            }
            return self.finish_action(
                original,
                Some(updated),
                true,
                action,
                at,
                effects,
                true,
                self.ids,
            );
        }
        let action = SchedulerAction::Pause;
        if transition_contract(original.state, action).kind() == TransitionContractKind::NoOp {
            return Ok(SchedulerOutcome::default());
        }
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut updated = original.clone();
        let mut effects = Vec::new();
        updated.desired_paused = true;
        Self::cancel_timers(&mut updated, &mut effects);

        if matches!(
            original.state,
            TaskState::Allocating
                | TaskState::Active
                | TaskState::PausedRestarting
                | TaskState::Verifying
                | TaskState::Seeding
        ) || matches!(
            original.pending_barrier,
            Some(PendingBarrier::CancellationDrain { .. })
        ) {
            Self::begin_cancellation(
                &mut updated,
                DrainTarget::Paused,
                force,
                &mut effects,
                "pause",
            )?;
        } else if original.state == TaskState::RetryWait {
            Self::release_slot(&mut updated, &mut effects);
        } else if let Some(barrier) = original.pending_barrier
            && !matches!(
                (original.state, barrier),
                (
                    TaskState::PausedHostKey,
                    PendingBarrier::HostKeyResolution { .. }
                )
            )
        {
            return Err(SchedulerError::PendingBarrier {
                barrier,
                operation: "pause",
            });
        }
        updated.state = target;
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn resume(
        &mut self,
        gid: Gid,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        if original.pending_source_replacement {
            return Err(SchedulerError::Conflict {
                state: original.state,
                operation: "source_replacement_pending",
            });
        }
        if let Some(barrier) = original.pending_barrier
            && Self::defers_user_control(barrier)
        {
            if matches!(
                original.pending_user_control,
                Some(PendingUserControl::Remove { .. })
            ) {
                return Err(SchedulerError::PendingBarrier {
                    barrier,
                    operation: "resume_after_remove",
                });
            }
            if !original.desired_paused
                && !matches!(
                    original.pending_user_control,
                    Some(PendingUserControl::Pause { .. })
                )
            {
                return Ok(SchedulerOutcome::default());
            }
            let action = SchedulerAction::ResumeDeferred;
            let mut updated = original.clone();
            let mut effects = Vec::new();
            updated.desired_paused = false;
            if matches!(
                original.state,
                TaskState::WaitingSlow | TaskState::PausedSlow
            ) {
                updated.slow_demotion_count = 0;
            }
            if let Some((readmission_id, _)) = updated.slow_readmission.take() {
                effects.push(TransitionEffect::CancelSlowReadmission {
                    task_id: updated.task_id,
                    gid,
                    generation: updated.generation,
                    readmission_id,
                });
            }
            updated.slow_readmission_ready = false;
            updated.slow_slot = None;
            updated.pending_user_control =
                if matches!(barrier, PendingBarrier::HostKeyResolution { .. }) {
                    None
                } else if matches!(original.state, TaskState::Paused | TaskState::PausedSlow) {
                    Some(PendingUserControl::Resume)
                } else {
                    None
                };
            return self.finish_action(
                original,
                Some(updated),
                true,
                action,
                at,
                effects,
                true,
                self.ids,
            );
        }
        Self::reject_pending(&original, "resume")?;
        let action = SchedulerAction::for_resume(original.state, original.conditions.snapshot());
        let contract = transition_contract(original.state, action);
        if contract.kind() == TransitionContractKind::NoOp {
            return Ok(SchedulerOutcome::default());
        }
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut updated = original.clone();
        let mut effects = Vec::new();
        if action == SchedulerAction::ExplicitNoSpaceProbeRequested {
            let mut ids = self.ids;
            if original.no_space_probe.is_some()
                && !original.desired_paused
                && !matches!(
                    original.state,
                    TaskState::WaitingSlow | TaskState::PausedSlow
                )
            {
                return Ok(SchedulerOutcome::default());
            }
            updated.desired_paused = false;
            updated.state = target;
            if matches!(
                original.state,
                TaskState::WaitingSlow | TaskState::PausedSlow
            ) {
                updated.slow_demotion_count = 0;
                if let Some((readmission_id, _)) = updated.slow_readmission.take() {
                    effects.push(TransitionEffect::CancelSlowReadmission {
                        task_id: updated.task_id,
                        gid,
                        generation: updated.generation,
                        readmission_id,
                    });
                }
                updated.slow_readmission_ready = false;
                updated.pending_slow_readmission = None;
                updated.slow_slot = None;
            }
            Self::schedule_no_space_probe(
                &mut updated,
                NoSpaceProbeOrigin::ExplicitResume,
                at,
                &mut effects,
                &mut ids,
            )?;
            return self.finish_action(
                original,
                Some(updated),
                true,
                action,
                at,
                effects,
                true,
                ids,
            );
        }

        updated.desired_paused = false;
        if matches!(
            original.state,
            TaskState::WaitingSlow | TaskState::PausedSlow
        ) {
            updated.slow_demotion_count = 0;
        }
        if let Some((readmission_id, _)) = updated.slow_readmission.take() {
            effects.push(TransitionEffect::CancelSlowReadmission {
                task_id: updated.task_id,
                gid,
                generation: updated.generation,
                readmission_id,
            });
        }
        updated.slow_readmission_ready = false;
        updated.slow_slot = None;
        updated.state = target;
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn approve_host_key(
        &mut self,
        gid: Gid,
        challenge: crate::HostKeyChallengeId,
        fingerprint_sha256: crate::HostKeyFingerprint,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        Self::reject_pending(&original, "approve_host_key")?;
        let presented = original
            .host_key_challenge
            .as_ref()
            .ok_or(SchedulerError::StaleChallenge)?;
        if presented.summary().id != challenge
            || presented.summary().fingerprint_sha256 != fingerprint_sha256
        {
            return Err(SchedulerError::StaleChallenge);
        }
        let action = SchedulerAction::ApproveHostKey;
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut ids = self.ids;
        let resolution_id = ids.host_key_resolution()?;
        let mut updated = original.clone();
        updated.state = target;
        updated.pending_barrier = Some(PendingBarrier::HostKeyResolution {
            generation: updated.generation,
            resolution_id,
            challenge,
        });
        let effects = vec![TransitionEffect::PersistHostKeyPinAndClearChallenge {
            task_id: updated.task_id,
            gid,
            resolution_id,
            challenge,
            fingerprint_sha256,
            presented_public_key: presented.presented_public_key().to_vec(),
            option_patch: None,
        }];
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            ids,
        )
    }

    fn begin_source_replacement(
        &mut self,
        gid: Gid,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        Self::reject_pending(&original, "begin_source_replacement")?;
        if original.pending_source_replacement || original.pending_option_patch.is_some() {
            return Err(SchedulerError::Conflict {
                state: original.state,
                operation: "source_replacement_pending",
            });
        }
        let action = SchedulerAction::SourceReplacementRequested;
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut updated = original.clone();
        let mut effects = Vec::new();
        updated.state = target;
        updated.pending_source_replacement = true;
        Self::cancel_timers(&mut updated, &mut effects);
        if updated.slot.owns_slot() {
            Self::begin_cancellation(
                &mut updated,
                DrainTarget::PausedRestarting,
                false,
                &mut effects,
                "source_replacement",
            )?;
        }
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn commit_source_replacement(
        &mut self,
        gid: Gid,
        satisfies_credentials: Option<CredentialRequirementKey>,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        Self::reject_pending(&original, "commit_source_replacement")?;
        if satisfies_credentials.is_some_and(|key| {
            !matches!(
                key.kind,
                crate::CredentialKind::SourceUri | crate::CredentialKind::HttpAuthentication
            )
        }) || (satisfies_credentials.is_some()
            && original
                .conditions
                .needs_credentials
                .as_ref()
                .map(crate::CredentialRequirement::key)
                != satisfies_credentials)
        {
            return Err(SchedulerError::StaleCredentialRequirement);
        }
        if !original.pending_source_replacement || original.slot.owns_slot() {
            return Err(SchedulerError::Conflict {
                state: original.state,
                operation: "source_replacement_not_quiesced",
            });
        }
        let action = SchedulerAction::SourceReplacementCommitted;
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut updated = original.clone();
        updated.state = target;
        updated.pending_source_replacement = false;
        if satisfies_credentials.is_some() {
            updated.conditions.needs_credentials = None;
        }
        let queue = original
            .queue_class()
            .ok_or(SchedulerError::InternalInvariant)?;
        if updated.queue_class() != Some(queue) {
            return Err(SchedulerError::InternalInvariant);
        }
        let effects = vec![TransitionEffect::PersistQueueTransition {
            task_id: original.task_id,
            gid,
            from: Some(queue),
            to: Some(queue),
            desired_paused: updated.desired_paused,
            slow_demotion_count: updated.slow_demotion_count,
            slow_slot: updated.slow_slot,
            orders: vec![QueueOrder {
                class: queue,
                order: self.queues.get(queue).to_vec(),
            }],
        }];
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn apply_option_patch(
        &mut self,
        gid: Gid,
        patch_id: OptionPatchId,
        kind: ValidatedOptionPatchKind,
        satisfies_credentials: Option<CredentialRequirementKey>,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        Self::reject_pending(&original, "apply_option_patch")?;
        if original.pending_source_replacement {
            return Err(SchedulerError::Conflict {
                state: original.state,
                operation: "source_replacement_pending",
            });
        }
        if original
            .highest_option_patch_id
            .is_some_and(|highest| patch_id <= highest)
        {
            return Err(SchedulerError::OptionPatchIdCollision);
        }
        if let Some(expected) = satisfies_credentials {
            if original
                .conditions
                .needs_credentials
                .as_ref()
                .map(crate::CredentialRequirement::key)
                != Some(expected)
            {
                return Err(SchedulerError::StaleCredentialRequirement);
            }
            if matches!(kind, ValidatedOptionPatchKind::MatchingHostKey { .. }) {
                return Err(SchedulerError::Conflict {
                    state: original.state,
                    operation: "credential_satisfying_host_key_patch",
                });
            }
        }

        let action = match kind {
            ValidatedOptionPatchKind::InPlace => SchedulerAction::InPlaceOptionPatchAccepted,
            ValidatedOptionPatchKind::ActiveRestart
                if matches!(
                    original.state,
                    TaskState::Allocating | TaskState::Active | TaskState::Verifying
                ) =>
            {
                SchedulerAction::ActiveRestartOptionPatchAccepted
            }
            ValidatedOptionPatchKind::ActiveRestart => SchedulerAction::InPlaceOptionPatchAccepted,
            ValidatedOptionPatchKind::MatchingHostKey {
                challenge,
                fingerprint_sha256,
            } => {
                let presented = original
                    .host_key_challenge
                    .as_ref()
                    .ok_or(SchedulerError::StaleChallenge)?;
                if presented.summary().id != challenge
                    || presented.summary().fingerprint_sha256 != fingerprint_sha256
                {
                    return Err(SchedulerError::StaleChallenge);
                }
                SchedulerAction::ApplyMatchingHostKeyOption
            }
        };
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut updated = original.clone();
        updated.state = target;
        updated.highest_option_patch_id = Some(patch_id);
        let mut effects = Vec::new();
        let mut ids = self.ids;

        match kind {
            ValidatedOptionPatchKind::InPlace | ValidatedOptionPatchKind::ActiveRestart
                if action == SchedulerAction::InPlaceOptionPatchAccepted =>
            {
                effects.push(TransitionEffect::ApplyOptionPatch {
                    task_id: updated.task_id,
                    gid,
                    patch_id,
                    satisfies_credentials,
                });
                updated.pending_option_patch = Some(patch_id);
                updated.pending_option_patch_mode = Some(PendingOptionPatchMode::InPlace);
                updated.pending_credential_requirement = satisfies_credentials;
                updated.pending_barrier = Some(PendingBarrier::OptionPatchApplication {
                    generation: updated.generation,
                    patch_id,
                });
            }
            ValidatedOptionPatchKind::ActiveRestart => {
                updated.pending_option_patch = Some(patch_id);
                updated.pending_option_patch_mode = Some(PendingOptionPatchMode::ActiveRestart);
                updated.pending_credential_requirement = satisfies_credentials;
                updated.pending_barrier = Some(PendingBarrier::OptionPatchPersistence {
                    generation: updated.generation,
                    patch_id,
                });
                effects.push(TransitionEffect::StageOptionPatch {
                    task_id: updated.task_id,
                    gid,
                    patch_id,
                    satisfies_credentials,
                });
            }
            ValidatedOptionPatchKind::MatchingHostKey {
                challenge,
                fingerprint_sha256,
            } => {
                let presented = original
                    .host_key_challenge
                    .as_ref()
                    .ok_or(SchedulerError::StaleChallenge)?;
                let resolution_id = ids.host_key_resolution()?;
                updated.pending_option_patch = Some(patch_id);
                updated.pending_option_patch_mode = Some(PendingOptionPatchMode::MatchingHostKey);
                updated.pending_barrier = Some(PendingBarrier::HostKeyResolution {
                    generation: updated.generation,
                    resolution_id,
                    challenge,
                });
                effects.push(TransitionEffect::PersistHostKeyPinAndClearChallenge {
                    task_id: updated.task_id,
                    gid,
                    resolution_id,
                    challenge,
                    fingerprint_sha256,
                    presented_public_key: presented.presented_public_key().to_vec(),
                    option_patch: Some(patch_id),
                });
            }
            ValidatedOptionPatchKind::InPlace => {
                return Err(SchedulerError::InternalInvariant);
            }
        }

        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            ids,
        )
    }

    fn remove(
        &mut self,
        gid: Gid,
        force: bool,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        if let Some(barrier) = original.pending_barrier
            && Self::defers_user_control(barrier)
        {
            let action = SchedulerAction::RemoveDeferred;
            let mut updated = original.clone();
            let mut effects = Vec::new();
            Self::cancel_timers(&mut updated, &mut effects);
            updated.no_space_probe = None;
            let force = force
                || matches!(
                    original.pending_user_control,
                    Some(PendingUserControl::Remove { force: true })
                );
            updated.pending_user_control = Some(PendingUserControl::Remove { force });
            return self.finish_action(
                original,
                Some(updated),
                true,
                action,
                at,
                effects,
                false,
                self.ids,
            );
        }
        let action = SchedulerAction::Remove;
        let contract = transition_contract(original.state, action);
        if contract.kind() == TransitionContractKind::NoOp {
            return Ok(SchedulerOutcome::default());
        }
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut updated = original.clone();
        let mut effects = Vec::new();
        Self::cancel_timers(&mut updated, &mut effects);
        updated.no_space_probe = None;
        if let Some(challenge) = updated.host_key_challenge.take() {
            effects.push(TransitionEffect::PersistHostKeyChallengeRejected {
                task_id: updated.task_id,
                gid,
                challenge: challenge.summary().id,
            });
        }
        updated.pending_option_patch = None;
        updated.pending_option_patch_mode = None;
        updated.pending_credential_requirement = None;
        updated.pending_user_control = None;
        updated.state = target;

        let needs_drain = matches!(
            original.state,
            TaskState::Allocating
                | TaskState::Active
                | TaskState::PausedRestarting
                | TaskState::Verifying
                | TaskState::Seeding
        ) || matches!(
            original.pending_barrier,
            Some(PendingBarrier::CancellationDrain { .. })
        );
        if needs_drain {
            Self::begin_cancellation(
                &mut updated,
                DrainTarget::Removed,
                force,
                &mut effects,
                "remove",
            )?;
        } else {
            if let Some(barrier) = original.pending_barrier {
                return Err(SchedulerError::PendingBarrier {
                    barrier,
                    operation: "remove",
                });
            }
            Self::release_slot(&mut updated, &mut effects);
            Self::begin_terminal(&mut updated, Aria2Status::Removed, None)?;
        }

        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            self.ids,
        )
    }

    fn remove_stopped_result(
        &mut self,
        gid: Gid,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let original = self.task_clone(gid)?;
        Self::reject_pending(&original, "remove_stopped_result")?;
        if let Some(barrier) = self.pending_stopped_deletion() {
            return Err(SchedulerError::PendingBarrier {
                barrier,
                operation: "remove_stopped_result",
            });
        }
        let action = SchedulerAction::RemoveStoppedResult;
        let target =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut ids = self.ids;
        let deletion_id = ids.stopped_deletion()?;
        let mut updated = original.clone();
        updated.state = target;
        updated.pending_barrier = Some(PendingBarrier::StoppedResultDeletion {
            generation: updated.generation,
            deletion_id,
        });
        let mut remaining_order = self.queues.get(QueueClass::Stopped).to_vec();
        let position = remaining_order
            .iter()
            .position(|candidate| *candidate == gid)
            .ok_or(SchedulerError::InternalInvariant)?;
        remaining_order.remove(position);
        let effects = vec![TransitionEffect::DeleteStoppedTaskMetadata {
            task_id: updated.task_id,
            gid,
            deletion_id,
            remaining_order,
        }];
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            ids,
        )
    }
}

impl RequestScheduler {
    pub fn admit_next(&mut self) -> Result<SchedulerOutcome, SchedulerError> {
        self.admit_next_at(MonotonicInstant::now())
    }

    pub fn admit_next_at(
        &mut self,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let pending_restart_patch =
            self.queues
                .get(QueueClass::Waiting)
                .iter()
                .copied()
                .find(|gid| {
                    self.tasks.get(gid).is_some_and(|task| {
                        task.state == TaskState::Waiting
                            && task.pending_barrier.is_none()
                            && !task.desired_paused
                            && task.pending_option_patch.is_some()
                            && task.pending_option_patch_mode
                                == Some(PendingOptionPatchMode::ActiveRestart)
                    })
                });
        let retained_retry = self
            .queues
            .get(QueueClass::Active)
            .iter()
            .copied()
            .find(|gid| {
                self.tasks.get(gid).is_some_and(|task| {
                    task.state == TaskState::RetryWait
                        && task.retry_ready
                        && task.slot == SlotOwnership::RetryRetained
                        && task.pending_barrier.is_none()
                        && !task.desired_paused
                        && !task.conditions.blocks_admission()
                })
            });

        let waiting = self
            .queues
            .get(QueueClass::Waiting)
            .iter()
            .copied()
            .find(|gid| {
                self.tasks.get(gid).is_some_and(|task| {
                    !task.pending_source_replacement
                        && (matches!(task.state, TaskState::Waiting)
                            || (task.state == TaskState::RetryWait && task.retry_ready))
                }) && self.tasks.get(gid).is_some_and(|task| {
                    task.pending_barrier.is_none()
                        && !task.desired_paused
                        && !task.conditions.blocks_admission()
                })
            });

        let demoted = self
            .queues
            .get(QueueClass::Demoted)
            .iter()
            .copied()
            .find(|gid| {
                self.tasks.get(gid).is_some_and(|task| {
                    task.state == TaskState::WaitingSlow
                        && task.slow_readmission_ready
                        && task.pending_barrier.is_none()
                        && !task.desired_paused
                        && !task.conditions.blocks_admission()
                })
            });

        let preferred_demoted = demoted.filter(|gid| match self.config.slow_readmission_policy {
            crate::SlowReadmissionPolicy::Front => true,
            crate::SlowReadmissionPolicy::OriginalPosition => self
                .tasks
                .get(gid)
                .is_some_and(|task| task.slow_remaining_position == 0),
            crate::SlowReadmissionPolicy::Back => false,
        });
        let gid = pending_restart_patch
            .or(retained_retry)
            .or(preferred_demoted)
            .or(waiting)
            .or(demoted)
            .ok_or_else(|| {
                let blocked_by_slots = self.tasks.values().any(|task| {
                    !task.slot.owns_slot()
                        && task.pending_barrier.is_none()
                        && !task.desired_paused
                        && !task.conditions.blocks_admission()
                        && (task.state == TaskState::Waiting
                            || (task.state == TaskState::RetryWait && task.retry_ready)
                            || (task.state == TaskState::WaitingSlow
                                && task.slow_readmission_ready))
                }) && self.active_slot_count()
                    >= self.config.max_active_tasks.get();
                if blocked_by_slots {
                    SchedulerError::ActiveLimitReached
                } else {
                    SchedulerError::NoEligibleTask
                }
            })?;

        let original = self.task_clone(gid)?;
        if original.state == TaskState::Waiting
            && original.pending_option_patch_mode == Some(PendingOptionPatchMode::ActiveRestart)
        {
            let patch_id = original
                .pending_option_patch
                .ok_or(SchedulerError::InternalInvariant)?;
            let action = SchedulerAction::RestartApplicationScheduled;
            let mut updated = original.clone();
            updated.pending_barrier = Some(PendingBarrier::OptionPatchApplication {
                generation: updated.generation,
                patch_id,
            });
            let effects = vec![TransitionEffect::ApplyOptionPatch {
                task_id: updated.task_id,
                gid: updated.gid,
                patch_id,
                satisfies_credentials: updated.pending_credential_requirement,
            }];
            return self.finish_action(
                original,
                Some(updated),
                true,
                action,
                at,
                effects,
                false,
                self.ids,
            );
        }
        let action = match original.state {
            TaskState::Waiting => SchedulerAction::SchedulerAdmission,
            TaskState::RetryWait => SchedulerAction::RetryReadmissionSucceeded,
            TaskState::WaitingSlow => SchedulerAction::SlowReadmissionSucceeded,
            _ => return Err(SchedulerError::InternalInvariant),
        };
        let mut updated = original.clone();
        let mut effects = Vec::new();
        self.plan_admission(&mut updated, action, &mut effects)?;
        let ordinary = original.state != TaskState::WaitingSlow;
        let result = self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )?;
        if ordinary {
            for task in self
                .tasks
                .values_mut()
                .filter(|task| task.slow_readmission_ready)
            {
                task.slow_remaining_position = task.slow_remaining_position.saturating_sub(1);
            }
        }
        Ok(result)
    }
}

impl RequestScheduler {
    fn generation_disposition(
        expected: Generation,
        actual: Generation,
    ) -> Result<EventDisposition, SchedulerError> {
        if actual < expected {
            return Ok(EventDisposition::Stale);
        }
        if actual > expected {
            return Err(SchedulerError::StaleGeneration { expected, actual });
        }
        Ok(EventDisposition::Fresh)
    }

    fn token_disposition<T: Copy + Eq>(
        expected: Option<T>,
        last: Option<T>,
        actual: T,
    ) -> EventDisposition {
        if expected == Some(actual) {
            EventDisposition::Fresh
        } else if last == Some(actual) {
            EventDisposition::Duplicate
        } else {
            EventDisposition::Stale
        }
    }

    fn ignored_for_disposition(
        task: &ScheduledTask,
        disposition: EventDisposition,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        match disposition {
            EventDisposition::Fresh => Err(SchedulerError::InternalInvariant),
            EventDisposition::Duplicate => {
                Self::ignored_outcome(task, SchedulerAction::DuplicateEventIgnored, at)
            }
            EventDisposition::Stale => {
                Self::ignored_outcome(task, SchedulerAction::StaleEventIgnored, at)
            }
        }
    }

    pub fn handle_event(
        &mut self,
        event: &TaskEventEnvelope,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        self.handle_event_at(event, MonotonicInstant::now())
    }

    pub fn handle_event_at(
        &mut self,
        event: &TaskEventEnvelope,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let task_id = event.task_id();
        let event = event.event();
        let gid = event.gid();
        let actual_generation = event.generation();
        let kind = event.kind();
        let original = self.task_clone(gid)?;
        if original.task_id != task_id {
            return Self::ignored_for_disposition(&original, EventDisposition::Stale, at);
        }
        let generation_disposition =
            Self::generation_disposition(original.generation, actual_generation)?;
        if generation_disposition != EventDisposition::Fresh {
            return Self::ignored_for_disposition(&original, generation_disposition, at);
        }

        if event.token().is_none()
            && kind != TaskEventKind::TerminalPersisted
            && original.seen_events.contains(&kind)
        {
            return Self::ignored_for_disposition(&original, EventDisposition::Duplicate, at);
        }
        let barrier_accepts_event = match original.pending_barrier {
            Some(PendingBarrier::GenerationPersistence { .. }) => {
                kind == TaskEventKind::GenerationPersisted
            }
            Some(PendingBarrier::OptionPatchPersistence { .. }) => matches!(
                kind,
                TaskEventKind::OptionPatchPersisted | TaskEventKind::OptionPatchPersistenceFailed
            ),
            Some(PendingBarrier::OptionPatchApplication { .. }) => matches!(
                kind,
                TaskEventKind::OptionPatchApplied | TaskEventKind::OptionPatchApplicationFailed
            ),
            Some(PendingBarrier::HostKeyResolution { .. }) => matches!(
                kind,
                TaskEventKind::HostKeyResolutionPersisted | TaskEventKind::HostKeyResolutionFailed
            ),
            Some(PendingBarrier::TerminalPersistence { .. }) => {
                kind == TaskEventKind::TerminalPersisted
            }
            Some(PendingBarrier::StoppedResultDeletion { .. }) => matches!(
                kind,
                TaskEventKind::StoppedResultDeleted | TaskEventKind::StoppedResultDeletionFailed
            ),
            Some(PendingBarrier::CancellationDrain { .. }) | None => true,
        };
        if !barrier_accepts_event {
            return Err(SchedulerError::PendingBarrier {
                barrier: original
                    .pending_barrier
                    .ok_or(SchedulerError::InternalInvariant)?,
                operation: kind.code(),
            });
        }
        if let TaskEvent::NoSpace { condition, .. } = event {
            condition
                .validate()
                .map_err(|_| SchedulerError::InvalidTaskConditions)?;
        }
        let event = event.clone();

        match event {
            TaskEvent::GenerationPersisted { generation, .. } => {
                self.generation_persisted(original, generation, at)
            }
            TaskEvent::OptionPatchPersisted { patch_id, .. } => {
                self.option_patch_persistence_completed(original, patch_id, true, at)
            }
            TaskEvent::OptionPatchPersistenceFailed { patch_id, .. } => {
                self.option_patch_persistence_completed(original, patch_id, false, at)
            }
            TaskEvent::OptionPatchApplied { patch_id, .. } => {
                self.option_patch_application_completed(original, patch_id, None, at)
            }
            TaskEvent::OptionPatchApplicationFailed {
                patch_id, error, ..
            } => self.option_patch_application_completed(original, patch_id, Some(error), at),
            TaskEvent::AllocationSucceeded { .. } => self.allocation_succeeded(original, at),
            TaskEvent::AllocationRetryable { retry_at, .. } => {
                self.allocation_retryable(original, retry_at, at)
            }
            TaskEvent::AllocationHostKeyChallenge { challenge, .. } => {
                self.allocation_host_key_challenge(original, challenge, at)
            }
            TaskEvent::AllocationFailed { error, .. } => {
                self.allocation_failed(original, error, at)
            }
            TaskEvent::RetryReady { retry_timer_id, .. } => {
                self.retry_ready(original, retry_timer_id, at)
            }
            TaskEvent::ActiveRetryIdle { retry_at, .. } => {
                self.active_retry_idle(original, retry_at, at)
            }
            TaskEvent::ActiveRepresentationRestart { .. } => {
                self.active_representation_restart(original, at)
            }
            TaskEvent::DataComplete { seed, .. } => self.data_complete(original, seed, at),
            TaskEvent::NoSpace { condition, .. } => self.no_space(original, condition, at),
            TaskEvent::TerminalFailure { error, .. } => self.terminal_failure(original, error, at),
            TaskEvent::SlowDemoted { decision, .. } => self.slow_demoted(original, decision, at),
            TaskEvent::SlowPaused { .. } => self.slow_paused(original, at),
            TaskEvent::SlowReadmit { readmission_id, .. } => {
                self.slow_readmit(original, readmission_id, at)
            }
            TaskEvent::VerificationSucceeded { .. } => self.verification_succeeded(original, at),
            TaskEvent::VerificationRecoverable { .. } => {
                self.verification_recoverable(original, at)
            }
            TaskEvent::VerificationFailed { error, .. } => {
                self.verification_failed(original, error, at)
            }
            TaskEvent::SeedingComplete { .. } => self.seeding_complete(original, at),
            TaskEvent::SeedingFailed { error, .. } => self.seeding_failed(original, error, at),
            TaskEvent::CancellationDrained { generation, .. } => {
                self.cancellation_drained(original, generation, at)
            }
            TaskEvent::NoSpaceProbeCompleted {
                probe_id,
                origin,
                ready,
                next_retry_at,
                ..
            } => {
                self.no_space_probe_completed(original, probe_id, origin, ready, next_retry_at, at)
            }
            TaskEvent::TerminalPersisted {
                generation, status, ..
            } => self.terminal_persisted(original, generation, status, at),
            TaskEvent::HostKeyResolutionPersisted { resolution_id, .. } => {
                self.host_key_resolution_completed(original, resolution_id, true, at)
            }
            TaskEvent::HostKeyResolutionFailed { resolution_id, .. } => {
                self.host_key_resolution_completed(original, resolution_id, false, at)
            }
            TaskEvent::StoppedResultDeleted { deletion_id, .. } => {
                self.stopped_result_deletion_completed(original, deletion_id, true, at)
            }
            TaskEvent::StoppedResultDeletionFailed { deletion_id, .. } => {
                self.stopped_result_deletion_completed(original, deletion_id, false, at)
            }
        }
    }

    fn stale_if_rejected_event(
        task: &ScheduledTask,
        action: SchedulerAction,
        at: MonotonicInstant,
    ) -> Result<Option<SchedulerOutcome>, SchedulerError> {
        if transition_contract(task.state, action).accepts() {
            Ok(None)
        } else {
            Self::ignored_for_disposition(task, EventDisposition::Stale, at).map(Some)
        }
    }

    fn mark_non_token_event(task: &mut ScheduledTask, kind: TaskEventKind) {
        task.seen_events.insert(kind);
    }

    fn generation_persisted(
        &mut self,
        original: ScheduledTask,
        generation: Generation,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::GenerationPersistenceSucceeded;
        let matches = matches!(
            original.pending_barrier,
            Some(PendingBarrier::GenerationPersistence {
                generation: expected
            }) if expected == generation
        );
        if !matches {
            return Self::ignored_for_disposition(&original, EventDisposition::Stale, at);
        }
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.pending_barrier = None;
        updated.slot = SlotOwnership::Active;
        Self::mark_non_token_event(&mut updated, TaskEventKind::GenerationPersisted);
        let effects = vec![TransitionEffect::StartAllocation {
            task_id: updated.task_id,
            gid: updated.gid,
            generation,
        }];
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn option_patch_persistence_completed(
        &mut self,
        original: ScheduledTask,
        patch_id: OptionPatchId,
        succeeded: bool,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let expected = match original.pending_barrier {
            Some(PendingBarrier::OptionPatchPersistence {
                generation,
                patch_id,
            }) if generation == original.generation => Some(patch_id),
            _ => None,
        };
        let disposition =
            Self::token_disposition(expected, original.last_option_patch_persistence, patch_id);
        if disposition != EventDisposition::Fresh {
            return Self::ignored_for_disposition(&original, disposition, at);
        }
        if original.pending_option_patch != Some(patch_id) {
            return Err(SchedulerError::InternalInvariant);
        }
        if original.pending_option_patch_mode != Some(PendingOptionPatchMode::ActiveRestart) {
            return Err(SchedulerError::InternalInvariant);
        }

        let mut updated = original.clone();
        updated.pending_barrier = None;
        updated.last_option_patch_persistence = Some(patch_id);
        let mut effects = Vec::new();
        if !succeeded {
            updated.pending_option_patch = None;
            updated.pending_option_patch_mode = None;
            updated.pending_credential_requirement = None;
        }
        let action = if let Some(action) =
            Self::complete_pending_user_control(&original, &mut updated, &mut effects)?
        {
            action
        } else {
            let action = if succeeded {
                SchedulerAction::ActiveRestartOptionPatchPersisted
            } else {
                SchedulerAction::ActiveRestartOptionPatchPersistenceFailed
            };
            if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
                return Ok(outcome);
            }
            updated.state = Self::target_for_action(&original, action)?
                .ok_or(SchedulerError::InternalInvariant)?;
            if succeeded {
                Self::begin_cancellation(
                    &mut updated,
                    DrainTarget::PausedRestarting,
                    false,
                    &mut effects,
                    "option_patch_persisted",
                )?;
            }
            action
        };
        let publish = !matches!(action, SchedulerAction::DeferredRemoveCompleted)
            && (succeeded
                || matches!(
                    action,
                    SchedulerAction::DeferredPauseCompleted
                        | SchedulerAction::DeferredResumeCompleted
                ));
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            publish,
            self.ids,
        )
    }

    fn option_patch_application_completed(
        &mut self,
        original: ScheduledTask,
        patch_id: OptionPatchId,
        error: Option<PublicError>,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let expected = match original.pending_barrier {
            Some(PendingBarrier::OptionPatchApplication {
                generation,
                patch_id,
            }) if generation == original.generation => Some(patch_id),
            _ => None,
        };
        let disposition =
            Self::token_disposition(expected, original.last_option_patch_application, patch_id);
        if disposition != EventDisposition::Fresh {
            return Self::ignored_for_disposition(&original, disposition, at);
        }
        if original.pending_option_patch != Some(patch_id) {
            return Err(SchedulerError::InternalInvariant);
        }
        let mode = original
            .pending_option_patch_mode
            .ok_or(SchedulerError::InternalInvariant)?;
        if mode == PendingOptionPatchMode::MatchingHostKey {
            return Err(SchedulerError::InternalInvariant);
        }

        let succeeded = error.is_none();
        let credential_requirement = original.pending_credential_requirement;
        if let Some(expected) = credential_requirement
            && original
                .conditions
                .needs_credentials
                .as_ref()
                .map(crate::CredentialRequirement::key)
                != Some(expected)
        {
            return Err(SchedulerError::StaleCredentialRequirement);
        }

        let mut updated = original.clone();
        updated.pending_barrier = None;
        updated.pending_option_patch = None;
        updated.pending_option_patch_mode = None;
        updated.pending_credential_requirement = None;
        updated.last_option_patch_application = Some(patch_id);
        if succeeded && credential_requirement.is_some() {
            updated.conditions.needs_credentials = None;
        }
        let mut effects = Vec::new();
        let action = if matches!(
            updated.pending_user_control,
            Some(PendingUserControl::Remove { .. })
        ) {
            Self::complete_pending_user_control(&original, &mut updated, &mut effects)?
                .ok_or(SchedulerError::InternalInvariant)?
        } else if mode == PendingOptionPatchMode::ActiveRestart && !succeeded {
            let action = SchedulerAction::RestartApplicationFailed;
            if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
                return Ok(outcome);
            }
            updated.pending_user_control = None;
            updated.state = Self::target_for_action(&original, action)?
                .ok_or(SchedulerError::InternalInvariant)?;
            Self::begin_terminal(
                &mut updated,
                Aria2Status::Error,
                Some(error.ok_or(SchedulerError::InternalInvariant)?),
            )?;
            action
        } else if let Some(action) =
            Self::complete_pending_user_control(&original, &mut updated, &mut effects)?
        {
            action
        } else {
            let action = match (mode, credential_requirement, succeeded) {
                (PendingOptionPatchMode::ActiveRestart, _, true) => {
                    SchedulerAction::RestartApplicationSucceeded
                }
                (PendingOptionPatchMode::ActiveRestart, _, false) => {
                    return Err(SchedulerError::InternalInvariant);
                }
                (PendingOptionPatchMode::InPlace, Some(_), true) => {
                    SchedulerAction::CredentialsSatisfied
                }
                (PendingOptionPatchMode::InPlace, Some(_), false) => {
                    SchedulerAction::CredentialSatisfactionFailed
                }
                (PendingOptionPatchMode::InPlace, None, true) => {
                    SchedulerAction::InPlaceOptionPatchApplied
                }
                (PendingOptionPatchMode::InPlace, None, false) => {
                    SchedulerAction::InPlaceOptionPatchApplicationFailed
                }
                (PendingOptionPatchMode::MatchingHostKey, _, _) => {
                    return Err(SchedulerError::InternalInvariant);
                }
            };
            if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
                return Ok(outcome);
            }
            updated.state = Self::target_for_action(&original, action)?
                .ok_or(SchedulerError::InternalInvariant)?;
            action
        };
        let publish = matches!(
            action,
            SchedulerAction::DeferredPauseCompleted
                | SchedulerAction::DeferredResumeCompleted
                | SchedulerAction::RestartApplicationSucceeded
                | SchedulerAction::CredentialsSatisfied
        );
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            publish,
            self.ids,
        )
    }

    fn host_key_resolution_completed(
        &mut self,
        original: ScheduledTask,
        resolution_id: HostKeyResolutionId,
        succeeded: bool,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let (expected, challenge) = match original.pending_barrier {
            Some(PendingBarrier::HostKeyResolution {
                generation,
                resolution_id,
                challenge,
            }) if generation == original.generation => (Some(resolution_id), Some(challenge)),
            _ => (None, None),
        };
        let disposition =
            Self::token_disposition(expected, original.last_host_key_resolution, resolution_id);
        if disposition != EventDisposition::Fresh {
            return Self::ignored_for_disposition(&original, disposition, at);
        }
        if original
            .host_key_challenge
            .as_ref()
            .map(|value| value.summary().id)
            != challenge
        {
            return Err(SchedulerError::InternalInvariant);
        }
        if original.pending_option_patch.is_some()
            && original.pending_option_patch_mode != Some(PendingOptionPatchMode::MatchingHostKey)
        {
            return Err(SchedulerError::InternalInvariant);
        }

        let mut updated = original.clone();
        updated.pending_barrier = None;
        updated.pending_option_patch = None;
        updated.pending_option_patch_mode = None;
        updated.last_host_key_resolution = Some(resolution_id);
        if succeeded {
            updated.host_key_challenge = None;
        }
        let mut effects = Vec::new();
        let action = if matches!(
            updated.pending_user_control,
            Some(PendingUserControl::Remove { .. })
        ) {
            Self::complete_pending_user_control(&original, &mut updated, &mut effects)?
                .ok_or(SchedulerError::InternalInvariant)?
        } else {
            let action = if succeeded {
                SchedulerAction::for_host_key_resolution(original.desired_paused)
            } else {
                SchedulerAction::HostKeyResolutionFailed
            };
            if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
                return Ok(outcome);
            }
            updated.state = Self::target_for_action(&original, action)?
                .ok_or(SchedulerError::InternalInvariant)?;
            action
        };
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            succeeded && action != SchedulerAction::DeferredRemoveCompleted,
            self.ids,
        )
    }

    fn allocation_succeeded(
        &mut self,
        original: ScheduledTask,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::AllocationSucceeded;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::AllocationSucceeded);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            Vec::new(),
            true,
            self.ids,
        )
    }

    fn allocation_retryable(
        &mut self,
        original: ScheduledTask,
        retry_at: MonotonicInstant,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::AllocationRetryableFailure;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut ids = self.ids;
        let retry_timer_id = ids.retry_timer()?;
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.retry_timer = Some((retry_timer_id, retry_at));
        updated.retry_ready = false;
        let mut effects = Vec::new();
        if self.config.retry_wait_holds_slot {
            updated.slot = SlotOwnership::RetryRetained;
        } else {
            Self::release_slot(&mut updated, &mut effects);
        }
        effects.push(TransitionEffect::ScheduleRetry {
            task_id: updated.task_id,
            gid: updated.gid,
            generation: updated.generation,
            retry_timer_id,
            at: retry_at,
        });
        Self::mark_non_token_event(&mut updated, TaskEventKind::AllocationRetryable);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            ids,
        )
    }

    fn allocation_host_key_challenge(
        &mut self,
        original: ScheduledTask,
        challenge: PresentedHostKeyChallenge,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::HostKeyChallengeRequired;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.host_key_challenge = Some(challenge.clone());
        let mut effects = vec![TransitionEffect::PersistHostKeyChallenge {
            task_id: updated.task_id,
            gid: updated.gid,
            challenge,
        }];
        Self::release_slot(&mut updated, &mut effects);
        Self::mark_non_token_event(&mut updated, TaskEventKind::AllocationHostKeyChallenge);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn allocation_failed(
        &mut self,
        original: ScheduledTask,
        error: PublicError,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::AllocationTerminalFailure;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut effects = Vec::new();
        Self::release_slot(&mut updated, &mut effects);
        Self::begin_terminal(&mut updated, Aria2Status::Error, Some(error))?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::AllocationFailed);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            self.ids,
        )
    }

    fn retry_ready(
        &mut self,
        original: ScheduledTask,
        retry_timer_id: RetryTimerId,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let disposition = Self::token_disposition(
            original.retry_timer.map(|(id, _)| id),
            original.last_retry_timer,
            retry_timer_id,
        );
        if disposition != EventDisposition::Fresh {
            return Self::ignored_for_disposition(&original, disposition, at);
        }
        let can_admit = original.slot.owns_slot()
            || self.active_slot_count() < self.config.max_active_tasks.get();
        let action =
            if can_admit && !original.desired_paused && !original.conditions.blocks_admission() {
                SchedulerAction::RetryReadmissionSucceeded
            } else {
                SchedulerAction::RetryReadmissionBlocked
            };
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.retry_timer = None;
        updated.last_retry_timer = Some(retry_timer_id);
        updated.retry_ready = true;
        let mut effects = Vec::new();
        if action == SchedulerAction::RetryReadmissionSucceeded {
            self.plan_admission(&mut updated, action, &mut effects)?;
        }
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn active_retry_idle(
        &mut self,
        original: ScheduledTask,
        retry_at: MonotonicInstant,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::LeaseRetryableWithoutRunnableWork;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut ids = self.ids;
        let retry_timer_id = ids.retry_timer()?;
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.retry_timer = Some((retry_timer_id, retry_at));
        updated.retry_ready = false;
        let mut effects = Vec::new();
        if self.config.retry_wait_holds_slot {
            updated.slot = SlotOwnership::RetryRetained;
        } else {
            Self::release_slot(&mut updated, &mut effects);
        }
        effects.push(TransitionEffect::ScheduleRetry {
            task_id: updated.task_id,
            gid: updated.gid,
            generation: updated.generation,
            retry_timer_id,
            at: retry_at,
        });
        Self::mark_non_token_event(&mut updated, TaskEventKind::ActiveRetryIdle);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            ids,
        )
    }

    fn active_representation_restart(
        &mut self,
        original: ScheduledTask,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::RepresentationRestart;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        let mut effects = Vec::new();
        self.plan_admission(&mut updated, action, &mut effects)?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::ActiveRepresentationRestart);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn data_complete(
        &mut self,
        original: ScheduledTask,
        seed: bool,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = if seed {
            SchedulerAction::BitTorrentPayloadComplete
        } else {
            SchedulerAction::AllRequiredDataReceived
        };
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        Self::mark_non_token_event(
            &mut updated,
            if seed {
                TaskEventKind::BitTorrentPayloadComplete
            } else {
                TaskEventKind::DataComplete
            },
        );
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            Vec::new(),
            true,
            self.ids,
        )
    }

    fn no_space(
        &mut self,
        original: ScheduledTask,
        condition: crate::NoSpaceCondition,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        condition
            .validate()
            .map_err(|_| SchedulerError::InvalidTaskConditions)?;
        let action = SchedulerAction::MidTransferNoSpace;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.conditions.no_space = Some(condition);
        let mut effects = vec![TransitionEffect::PersistConditions {
            task_id: updated.task_id,
            gid: updated.gid,
            conditions: updated.conditions.clone(),
        }];
        Self::begin_cancellation(
            &mut updated,
            DrainTarget::Waiting,
            false,
            &mut effects,
            "no_space",
        )?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::NoSpace);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn terminal_failure(
        &mut self,
        original: ScheduledTask,
        error: PublicError,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::TerminalWorkFailure;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.error = Some(error);
        let mut effects = Vec::new();
        Self::begin_cancellation(
            &mut updated,
            DrainTarget::Error,
            false,
            &mut effects,
            "terminal_failure",
        )?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::TerminalFailure);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            self.ids,
        )
    }

    fn slow_demoted(
        &mut self,
        original: ScheduledTask,
        decision: SlowReadmissionDecision,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        if decision.delay_ms == 0 || decision.scheduled_at_ms > MAX_PERSISTED_MILLISECONDS {
            return Err(SchedulerError::InvalidSlowReadmissionDecision);
        }
        let action = SchedulerAction::SlowSlotDemote;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.pending_slow_readmission = Some(decision);
        let original_position = self
            .queues
            .get(QueueClass::Active)
            .iter()
            .position(|candidate| *candidate == original.gid)
            .ok_or(SchedulerError::InternalInvariant)?;
        let demotion_count = original
            .slow_demotion_count
            .checked_add(1)
            .ok_or(SchedulerError::InternalInvariant)?;
        updated.slow_demotion_count = demotion_count;
        updated.slow_slot = Some(SlowSlotPersistence {
            original_position,
            demotion_count,
            decision,
        });
        updated.slow_remaining_position = original_position;
        let mut effects = Vec::new();
        Self::begin_cancellation(
            &mut updated,
            DrainTarget::WaitingSlow,
            false,
            &mut effects,
            "slow_demoted",
        )?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::SlowDemoted);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn slow_paused(
        &mut self,
        original: ScheduledTask,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::SlowSlotPause;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.desired_paused = true;
        let mut effects = Vec::new();
        Self::begin_cancellation(
            &mut updated,
            DrainTarget::PausedSlow,
            false,
            &mut effects,
            "slow_paused",
        )?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::SlowPaused);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn slow_readmit(
        &mut self,
        original: ScheduledTask,
        readmission_id: SlowReadmissionId,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let disposition = Self::token_disposition(
            original.slow_readmission.map(|(id, _)| id),
            original.last_slow_readmission,
            readmission_id,
        );
        if disposition != EventDisposition::Fresh {
            return Self::ignored_for_disposition(&original, disposition, at);
        }
        let waiting_first = self.queues.get(QueueClass::Waiting).iter().any(|gid| {
            self.tasks.get(gid).is_some_and(|task| {
                !task.desired_paused
                    && task.pending_barrier.is_none()
                    && !task.conditions.blocks_admission()
                    && (task.state == TaskState::Waiting
                        || (task.state == TaskState::RetryWait && task.retry_ready))
            })
        }) && match self.config.slow_readmission_policy {
            crate::SlowReadmissionPolicy::Front => false,
            crate::SlowReadmissionPolicy::OriginalPosition => original.slow_remaining_position != 0,
            crate::SlowReadmissionPolicy::Back => true,
        };
        let can_admit = !waiting_first
            && self.active_slot_count() < self.config.max_active_tasks.get()
            && !original.desired_paused
            && !original.conditions.blocks_admission();
        let action = if can_admit {
            SchedulerAction::SlowReadmissionSucceeded
        } else {
            SchedulerAction::SlowReadmissionBlocked
        };
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.slow_readmission = None;
        updated.last_slow_readmission = Some(readmission_id);
        updated.slow_readmission_ready = true;
        let mut effects = Vec::new();
        if action == SchedulerAction::SlowReadmissionSucceeded {
            self.plan_admission(&mut updated, action, &mut effects)?;
        }
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn verification_succeeded(
        &mut self,
        original: ScheduledTask,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::VerificationSucceeded;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut effects = Vec::new();
        Self::release_slot(&mut updated, &mut effects);
        Self::begin_terminal(&mut updated, Aria2Status::Complete, None)?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::VerificationSucceeded);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            self.ids,
        )
    }

    fn verification_recoverable(
        &mut self,
        original: ScheduledTask,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::VerificationRecoverableFailure;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut effects = Vec::new();
        Self::release_slot(&mut updated, &mut effects);
        Self::mark_non_token_event(&mut updated, TaskEventKind::VerificationRecoverable);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            self.ids,
        )
    }

    fn verification_failed(
        &mut self,
        original: ScheduledTask,
        error: PublicError,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::VerificationTerminalFailure;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut effects = Vec::new();
        Self::release_slot(&mut updated, &mut effects);
        Self::begin_terminal(&mut updated, Aria2Status::Error, Some(error))?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::VerificationFailed);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            self.ids,
        )
    }

    fn seeding_complete(
        &mut self,
        original: ScheduledTask,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::SeedingStopped;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut effects = Vec::new();
        Self::release_slot(&mut updated, &mut effects);
        Self::begin_terminal(&mut updated, Aria2Status::Complete, None)?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::SeedingComplete);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            self.ids,
        )
    }

    fn seeding_failed(
        &mut self,
        original: ScheduledTask,
        error: PublicError,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let action = SchedulerAction::BitTorrentFailure;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut effects = Vec::new();
        Self::release_slot(&mut updated, &mut effects);
        Self::begin_terminal(&mut updated, Aria2Status::Error, Some(error))?;
        Self::mark_non_token_event(&mut updated, TaskEventKind::SeedingFailed);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            false,
            self.ids,
        )
    }

    fn cancellation_drained(
        &mut self,
        original: ScheduledTask,
        generation: Generation,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let (target, barrier_force) = match original.pending_barrier {
            Some(PendingBarrier::CancellationDrain {
                generation: expected,
                target,
                force,
            }) if expected == generation => (target, force),
            _ => {
                return Self::ignored_for_disposition(&original, EventDisposition::Stale, at);
            }
        };
        let action = if target == DrainTarget::PausedRestarting {
            SchedulerAction::RestartQuiesced
        } else {
            SchedulerAction::CancellationDrainSucceeded
        };
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        if target != DrainTarget::PausedRestarting && original.state != target.state() {
            return Err(SchedulerError::InternalInvariant);
        }

        let mut ids = self.ids;
        let mut updated = original.clone();
        updated.pending_barrier = None;
        let mut effects = Vec::new();
        Self::release_slot(&mut updated, &mut effects);

        let publish = match target {
            DrainTarget::PausedRestarting if updated.pending_source_replacement => true,
            DrainTarget::PausedRestarting => {
                let patch_id = updated
                    .pending_option_patch
                    .as_ref()
                    .copied()
                    .ok_or(SchedulerError::InternalInvariant)?;
                if updated.pending_option_patch_mode != Some(PendingOptionPatchMode::ActiveRestart)
                {
                    return Err(SchedulerError::InternalInvariant);
                }
                updated.state = Self::target_for_action(&original, action)?
                    .ok_or(SchedulerError::InternalInvariant)?;
                updated.pending_barrier = Some(PendingBarrier::OptionPatchApplication {
                    generation: updated.generation,
                    patch_id,
                });
                effects.push(TransitionEffect::ApplyOptionPatch {
                    task_id: updated.task_id,
                    gid: updated.gid,
                    patch_id,
                    satisfies_credentials: updated.pending_credential_requirement,
                });
                false
            }
            DrainTarget::WaitingSlow => {
                if let Some(decision) = updated.pending_slow_readmission.take() {
                    let readmission_id = ids.slow_readmission()?;
                    updated.slow_readmission = Some((readmission_id, decision.readmit_at));
                    effects.push(TransitionEffect::ScheduleSlowReadmission {
                        task_id: updated.task_id,
                        gid: updated.gid,
                        generation: updated.generation,
                        readmission_id,
                        at: decision.readmit_at,
                    });
                }
                true
            }
            DrainTarget::Error => {
                let error = updated
                    .error
                    .clone()
                    .ok_or(SchedulerError::InternalInvariant)?;
                Self::begin_terminal(&mut updated, Aria2Status::Error, Some(error))?;
                false
            }
            DrainTarget::Removed => {
                Self::begin_terminal(&mut updated, Aria2Status::Removed, None)?;
                false
            }
            DrainTarget::Paused | DrainTarget::PausedSlow | DrainTarget::Waiting => true,
        };
        if matches!(
            updated.state,
            TaskState::Waiting
                | TaskState::WaitingSlow
                | TaskState::RetryWait
                | TaskState::Paused
                | TaskState::PausedSlow
        ) && let Some(probe_at) = updated
            .conditions
            .no_space
            .as_ref()
            .and_then(|condition| condition.retry_at)
        {
            Self::schedule_no_space_probe(
                &mut updated,
                NoSpaceProbeOrigin::AutomaticRetry,
                probe_at,
                &mut effects,
                &mut ids,
            )?;
        }
        let _ = barrier_force;
        Self::mark_non_token_event(&mut updated, TaskEventKind::CancellationDrained);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            publish,
            ids,
        )
    }

    fn no_space_probe_completed(
        &mut self,
        original: ScheduledTask,
        probe_id: NoSpaceProbeId,
        origin: NoSpaceProbeOrigin,
        ready: bool,
        next_retry_at: Option<MonotonicInstant>,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let disposition = Self::token_disposition(
            original.no_space_probe.map(|(id, _)| id),
            original.last_no_space_probe,
            probe_id,
        );
        if disposition != EventDisposition::Fresh
            || original.no_space_probe.map(|(_, expected)| expected) != Some(origin)
        {
            return Self::ignored_for_disposition(
                &original,
                if disposition == EventDisposition::Duplicate {
                    EventDisposition::Duplicate
                } else {
                    EventDisposition::Stale
                },
                at,
            );
        }
        let action = SchedulerAction::for_no_space_probe_result(
            original.state,
            origin,
            original.desired_paused,
            ready,
        )
        .ok_or(SchedulerError::Conflict {
            state: original.state,
            operation: "no_space_probe_completed",
        })?;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut ids = self.ids;
        let mut updated = original.clone();
        updated.no_space_probe = None;
        updated.last_no_space_probe = Some(probe_id);
        if ready {
            updated.conditions.no_space = None;
        } else if let Some(condition) = updated.conditions.no_space.as_mut() {
            condition.retry_at = next_retry_at;
        } else {
            return Err(SchedulerError::InternalInvariant);
        }
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        let mut effects = vec![TransitionEffect::PersistConditions {
            task_id: updated.task_id,
            gid: updated.gid,
            conditions: updated.conditions.clone(),
        }];
        if !ready && let Some(probe_at) = next_retry_at {
            Self::schedule_no_space_probe(
                &mut updated,
                NoSpaceProbeOrigin::AutomaticRetry,
                probe_at,
                &mut effects,
                &mut ids,
            )?;
        }
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            effects,
            true,
            ids,
        )
    }

    fn terminal_persisted(
        &mut self,
        original: ScheduledTask,
        generation: Generation,
        status: Aria2Status,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let matches = matches!(
            original.pending_barrier,
            Some(PendingBarrier::TerminalPersistence {
                generation: expected_generation,
                status: expected_status,
            }) if expected_generation == generation && expected_status == status
        );
        if !matches {
            if original.state == TaskState::StoppedResult
                && original.stopped_status == Some(status)
                && original.terminal_persisted
            {
                return Self::ignored_for_disposition(&original, EventDisposition::Duplicate, at);
            }
            return Err(SchedulerError::InvalidTerminalAcknowledgement);
        }
        let action = SchedulerAction::TerminalPersistenceSucceeded;
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        let mut updated = original.clone();
        updated.pending_barrier = None;
        updated.state =
            Self::target_for_action(&original, action)?.ok_or(SchedulerError::InternalInvariant)?;
        updated.stopped_status = Some(status);
        updated.terminal_persisted = true;
        Self::mark_non_token_event(&mut updated, TaskEventKind::TerminalPersisted);
        self.finish_action(
            original,
            Some(updated),
            true,
            action,
            at,
            Vec::new(),
            true,
            self.ids,
        )
    }

    fn stopped_result_deletion_completed(
        &mut self,
        original: ScheduledTask,
        deletion_id: StoppedResultDeletionId,
        succeeded: bool,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let expected = match original.pending_barrier {
            Some(PendingBarrier::StoppedResultDeletion {
                generation,
                deletion_id,
            }) if generation == original.generation => Some(deletion_id),
            _ => None,
        };
        let disposition =
            Self::token_disposition(expected, original.last_stopped_deletion, deletion_id);
        if disposition != EventDisposition::Fresh {
            return Self::ignored_for_disposition(&original, disposition, at);
        }
        let action = if succeeded {
            SchedulerAction::StoppedResultDeletionSucceeded
        } else {
            SchedulerAction::StoppedResultDeletionFailed
        };
        if let Some(outcome) = Self::stale_if_rejected_event(&original, action, at)? {
            return Ok(outcome);
        }
        if succeeded {
            self.finish_action(
                original,
                None,
                true,
                action,
                at,
                Vec::new(),
                false,
                self.ids,
            )
        } else {
            let mut updated = original.clone();
            updated.pending_barrier = None;
            updated.last_stopped_deletion = Some(deletion_id);
            self.finish_action(
                original,
                Some(updated),
                true,
                action,
                at,
                Vec::new(),
                false,
                self.ids,
            )
        }
    }
}
