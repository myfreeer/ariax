use ariax_core::{
    Aria2Status, Generation, Gid, QueueClass, QueueOrder, RetryClass, SlowSlotPersistence,
    TaskEvent, TaskEventEnvelope, TransitionEffect, TransitionEffectKind,
};
use ariax_runtime::{
    DispatchedEffect, EffectCompletion, EffectDispatchId, EffectSinkError, SchedulerEffectSink,
    SchedulerEffectSinkPrepare,
};
use ariax_storage::{
    JournalAppenderError, JournalPayload, OptionsSnapshotScope, SanitizedOptionMap, SessionCommand,
    SessionCommandResult, SessionCompletion, SessionHandle, SessionHostKeyChallengeRecord,
    SessionHostKeyResolution, SessionNoSpaceCondition, SessionOwnerError, SessionPersistenceError,
    SessionQueueOrder, SessionQueueState, SessionQueueTransition, SessionSlowSlotState,
    SessionStoppedResultRecord, SessionTaskRecord, SessionTaskSourceRecord, SessionTerminalStatus,
    TaskPauseReason, session_host_key_pin_value,
};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::task::Poll;

/// Hard ceiling for plans retained before their exact scheduler effect appears.
pub const MAX_PERSISTENCE_CATALOG_ENTRIES: usize = 1024;
/// Hard ceiling for logical owner operations in one scheduler persistence effect.
pub const MAX_PERSISTENCE_PLAN_STEPS: usize = 4;

/// One logical owner operation. Journal durability expands to append then flush.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistencePlanStep {
    PutTask(SessionTaskRecord),
    CreateTaskWithMetadata {
        task: SessionTaskRecord,
        sources: Vec<SessionTaskSourceRecord>,
        options: SanitizedOptionMap,
    },
    TransitionTaskQueue(SessionQueueTransition),
    ReplaceTaskSourcesAndQueue {
        transition: SessionQueueTransition,
        sources: Vec<SessionTaskSourceRecord>,
    },
    ReplaceTaskOptions {
        gid: Gid,
        scope: OptionsSnapshotScope,
        options: SanitizedOptionMap,
    },
    PromoteTaskOptions {
        gid: Gid,
        options: SanitizedOptionMap,
    },
    SetNoSpaceCondition {
        gid: Gid,
        condition: Option<SessionNoSpaceCondition>,
        updated_ms: u64,
    },
    PutHostKeyChallenge(SessionHostKeyChallengeRecord),
    ResolveHostKeyChallenge(SessionHostKeyResolution),
    RejectHostKeyChallenge {
        gid: Gid,
        challenge_id: ariax_core::HostKeyChallengeId,
    },
    PersistStoppedResult {
        result: SessionStoppedResultRecord,
        transition: SessionQueueTransition,
    },
    DeleteStoppedTaskMetadata {
        gid: Gid,
        remaining_order: Vec<Gid>,
        updated_ms: u64,
    },
    AppendAndFlushJournal {
        gid: Gid,
        generation: Generation,
        payload: JournalPayload,
    },
    FlushJournal {
        gid: Gid,
        through_sequence: u64,
    },
}

/// Why an effect-specific persistence plan is not safe to admit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistencePlanError {
    UnsupportedEffect,
    Empty,
    TooManySteps,
    InvalidSequence,
    IdentityMismatch,
    StateMismatch,
    GenerationMismatch,
    TokenMismatch,
    TerminalMismatch,
}

/// Complete missing data bound to one exact scheduler effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistenceEffectPlan {
    effect: TransitionEffect,
    steps: Vec<PersistencePlanStep>,
}

impl PersistenceEffectPlan {
    pub fn new(
        effect: TransitionEffect,
        steps: Vec<PersistencePlanStep>,
    ) -> Result<Self, PersistencePlanError> {
        if !is_persistence_effect(effect.kind()) {
            return Err(PersistencePlanError::UnsupportedEffect);
        }
        if steps.is_empty() {
            return Err(PersistencePlanError::Empty);
        }
        if steps.len() > MAX_PERSISTENCE_PLAN_STEPS {
            return Err(PersistencePlanError::TooManySteps);
        }
        validate_plan(&effect, &steps)?;
        Ok(Self { effect, steps })
    }

    #[must_use]
    pub const fn effect(&self) -> &TransitionEffect {
        &self.effect
    }

    #[must_use]
    pub fn steps(&self) -> &[PersistencePlanStep] {
        &self.steps
    }
}

/// Bounded catalog insertion failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistenceCatalogError {
    CapacityTooLarge,
    Full,
}

/// Typed preparation routed either to persistence or to the concrete runtime
/// delegate while the composed driver is idle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistenceSchedulerPreparation<P> {
    Persistence(Box<PersistenceEffectPlan>),
    Delegate(P),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistenceSchedulerPrepareError<E> {
    Persistence(PersistenceCatalogError),
    Delegate(E),
}

/// Bounded exact-effect catalog consumed by the composition sink.
#[derive(Debug)]
pub struct PersistenceEffectCatalog {
    capacity: NonZeroUsize,
    entries: VecDeque<PersistenceEffectPlan>,
}

impl PersistenceEffectCatalog {
    pub fn new(capacity: NonZeroUsize) -> Result<Self, PersistenceCatalogError> {
        if capacity.get() > MAX_PERSISTENCE_CATALOG_ENTRIES {
            return Err(PersistenceCatalogError::CapacityTooLarge);
        }
        Ok(Self {
            capacity,
            entries: VecDeque::with_capacity(capacity.get()),
        })
    }

    pub fn register(&mut self, plan: PersistenceEffectPlan) -> Result<(), PersistenceCatalogError> {
        if self.entries.len() == self.capacity.get() {
            return Err(PersistenceCatalogError::Full);
        }
        self.entries.push_back(plan);
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn exact_plan(&self, effect: &TransitionEffect) -> Option<PersistenceEffectPlan> {
        self.entries
            .iter()
            .find(|entry| entry.effect == *effect)
            .cloned()
    }

    fn consume_exact(&mut self, effect: &TransitionEffect) -> bool {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.effect == *effect)
        else {
            return false;
        };
        self.entries.remove(index).is_some()
    }
}

enum SessionEndpoint {
    Owner(SessionHandle),
    #[cfg(test)]
    Test(TestSessionEndpoint),
}

struct RejectedOwnerCommand {
    command: Box<SessionCommand>,
    error: SessionOwnerError,
}

impl SessionEndpoint {
    fn try_submit(
        &mut self,
        command: SessionCommand,
    ) -> Result<OwnerCompletion, RejectedOwnerCommand> {
        match self {
            Self::Owner(handle) => handle
                .try_submit_owned(command)
                .map(OwnerCompletion::Owner)
                .map_err(|rejection| {
                    let (command, error) = rejection.into_boxed_parts();
                    RejectedOwnerCommand { command, error }
                }),
            #[cfg(test)]
            Self::Test(endpoint) => endpoint.try_submit(command),
        }
    }
}

enum OwnerCompletion {
    Owner(SessionCompletion),
    #[cfg(test)]
    Test(TestSessionCompletion),
}

impl OwnerCompletion {
    fn try_wait(&self) -> Result<Option<SessionCommandResult>, SessionOwnerError> {
        match self {
            Self::Owner(completion) => completion.try_wait(),
            #[cfg(test)]
            Self::Test(completion) => completion.try_wait(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ExpectedResult {
    Unit,
    Appended,
    Flushed(u64),
}

struct PendingOwnerCommand {
    command: SessionCommand,
    expected: ExpectedResult,
}

struct OfferedPersistence {
    dispatch_id: EffectDispatchId,
    task_generation: Generation,
    plan: PersistenceEffectPlan,
    pending: PendingOwnerCommand,
}

struct ActivePersistence {
    dispatch_id: EffectDispatchId,
    task_generation: Generation,
    plan: PersistenceEffectPlan,
    step_index: usize,
    pending: Option<PendingOwnerCommand>,
    completion: Option<OwnerCompletion>,
    in_flight_expected: Option<ExpectedResult>,
}

/// Routes exact persistence effects through `SessionHandle` and delegates the rest.
pub struct PersistenceSchedulerEffectSink<D> {
    session: SessionEndpoint,
    delegate: D,
    catalog: PersistenceEffectCatalog,
    offered: Option<OfferedPersistence>,
    active: Option<ActivePersistence>,
    delegated: bool,
    immediate: Option<EffectCompletion>,
}

impl<D> PersistenceSchedulerEffectSink<D> {
    #[must_use]
    pub fn new(session: SessionHandle, delegate: D, catalog: PersistenceEffectCatalog) -> Self {
        Self {
            session: SessionEndpoint::Owner(session),
            delegate,
            catalog,
            offered: None,
            active: None,
            delegated: false,
            immediate: None,
        }
    }

    #[must_use]
    pub const fn delegate(&self) -> &D {
        &self.delegate
    }

    pub fn delegate_mut(&mut self) -> &mut D {
        &mut self.delegate
    }

    #[must_use]
    pub const fn catalog(&self) -> &PersistenceEffectCatalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut PersistenceEffectCatalog {
        &mut self.catalog
    }

    #[cfg(test)]
    fn new_for_test(
        session: TestSessionEndpoint,
        delegate: D,
        catalog: PersistenceEffectCatalog,
    ) -> Self {
        Self {
            session: SessionEndpoint::Test(session),
            delegate,
            catalog,
            offered: None,
            active: None,
            delegated: false,
            immediate: None,
        }
    }

    fn offer_persistence(
        &mut self,
        effect: &DispatchedEffect,
    ) -> Poll<Result<(), EffectSinkError>> {
        if self.active.is_some() || self.delegated || self.immediate.is_some() {
            return Poll::Ready(Err(EffectSinkError::Failed));
        }
        if let Some(offered) = self.offered.as_ref()
            && (offered.dispatch_id != effect.dispatch_id()
                || offered.plan.effect != *effect.effect())
        {
            return Poll::Ready(Err(EffectSinkError::Failed));
        }
        if self.offered.is_none() {
            let Some(plan) = self.catalog.exact_plan(effect.effect()) else {
                self.immediate = Some(EffectCompletion::UnrepresentableFailure {
                    dispatch_id: effect.dispatch_id(),
                });
                return Poll::Ready(Ok(()));
            };
            if validate_dispatched_plan(effect, &plan).is_err() {
                let consumed = self.catalog.consume_exact(effect.effect());
                debug_assert!(consumed, "the cloned exact plan must still exist");
                self.immediate = Some(EffectCompletion::UnrepresentableFailure {
                    dispatch_id: effect.dispatch_id(),
                });
                return Poll::Ready(Ok(()));
            }
            let pending = command_for_step(&plan.steps[0]);
            self.offered = Some(OfferedPersistence {
                dispatch_id: effect.dispatch_id(),
                task_generation: effect.task_generation(),
                plan,
                pending,
            });
        }

        let mut offered = self.offered.take().expect("offered persistence exists");
        match self.session.try_submit(offered.pending.command) {
            Ok(completion) => {
                let consumed = self.catalog.consume_exact(&offered.plan.effect);
                if !consumed {
                    self.immediate = Some(EffectCompletion::UnrepresentableFailure {
                        dispatch_id: offered.dispatch_id,
                    });
                    return Poll::Ready(Ok(()));
                }
                self.active = Some(ActivePersistence {
                    dispatch_id: offered.dispatch_id,
                    task_generation: offered.task_generation,
                    plan: offered.plan,
                    step_index: 0,
                    pending: None,
                    completion: Some(completion),
                    in_flight_expected: Some(offered.pending.expected),
                });
                Poll::Ready(Ok(()))
            }
            Err(rejection) => {
                offered.pending.command = *rejection.command;
                match rejection.error {
                    SessionOwnerError::QueueFull => {
                        self.offered = Some(offered);
                        Poll::Pending
                    }
                    SessionOwnerError::ShuttingDown | SessionOwnerError::Unavailable => {
                        Poll::Ready(Err(EffectSinkError::Closed))
                    }
                    SessionOwnerError::Persistence(_)
                    | SessionOwnerError::ThreadSpawn(_)
                    | SessionOwnerError::InvalidRequestCapacity { .. }
                    | SessionOwnerError::InvalidWaitTimeout(_)
                    | SessionOwnerError::StartupTimedOut { .. }
                    | SessionOwnerError::ShutdownTimedOut { .. }
                    | SessionOwnerError::OwnerPanicked => Poll::Ready(Err(EffectSinkError::Failed)),
                }
            }
        }
    }

    fn poll_active_completion(&mut self) -> Poll<EffectCompletion> {
        let mut active = self.active.take().expect("active persistence exists");
        if let Some(completion) = active.completion.as_ref() {
            match completion.try_wait() {
                Ok(None) => {
                    self.active = Some(active);
                    return Poll::Pending;
                }
                Err(error) => {
                    let expected = active
                        .in_flight_expected
                        .expect("accepted command retains its expected result");
                    return Poll::Ready(owner_command_failure(&active, expected, &error));
                }
                Ok(Some(result)) => {
                    active.completion.take();
                    let expected = active
                        .in_flight_expected
                        .take()
                        .expect("accepted command retains its expected result");
                    if !apply_result(&mut active, expected, result) {
                        return Poll::Ready(EffectCompletion::UnrepresentableFailure {
                            dispatch_id: active.dispatch_id,
                        });
                    }
                }
            }
        }

        if active.step_index == active.plan.steps.len() {
            return Poll::Ready(success_completion(&active));
        }
        if active.pending.is_none() {
            active.pending = Some(command_for_step(&active.plan.steps[active.step_index]));
        }
        let pending = active.pending.take().expect("pending command exists");
        match self.session.try_submit(pending.command) {
            Ok(completion) => {
                active.in_flight_expected = Some(pending.expected);
                active.completion = Some(completion);
                self.active = Some(active);
                Poll::Pending
            }
            Err(rejection) => {
                active.pending = Some(PendingOwnerCommand {
                    command: *rejection.command,
                    expected: pending.expected,
                });
                if matches!(rejection.error, SessionOwnerError::QueueFull) {
                    self.active = Some(active);
                    return Poll::Pending;
                }
                Poll::Ready(post_acceptance_admission_failure(&active))
            }
        }
    }
}

impl<D: SchedulerEffectSink> SchedulerEffectSink for PersistenceSchedulerEffectSink<D> {
    fn poll_dispatch(&mut self, effect: &DispatchedEffect) -> Poll<Result<(), EffectSinkError>> {
        if effect.effect().kind() == TransitionEffectKind::PublishSnapshot {
            return Poll::Ready(Err(EffectSinkError::Failed));
        }
        if is_persistence_effect(effect.effect().kind()) {
            return self.offer_persistence(effect);
        }
        if self.offered.is_some()
            || self.active.is_some()
            || self.delegated
            || self.immediate.is_some()
        {
            return Poll::Ready(Err(EffectSinkError::Failed));
        }
        match self.delegate.poll_dispatch(effect) {
            Poll::Ready(Ok(())) => {
                self.delegated = true;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }

    fn poll_completion(&mut self) -> Poll<EffectCompletion> {
        if let Some(completion) = self.immediate.take() {
            return Poll::Ready(completion);
        }
        if self.active.is_some() {
            return self.poll_active_completion();
        }
        if self.delegated {
            match self.delegate.poll_completion() {
                Poll::Ready(completion) => {
                    self.delegated = false;
                    Poll::Ready(completion)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

impl<D: SchedulerEffectSinkPrepare> SchedulerEffectSinkPrepare
    for PersistenceSchedulerEffectSink<D>
{
    type Preparation = PersistenceSchedulerPreparation<D::Preparation>;
    type Error = PersistenceSchedulerPrepareError<D::Error>;

    fn prepare(&mut self, preparation: Self::Preparation) -> Result<(), Self::Error> {
        match preparation {
            PersistenceSchedulerPreparation::Persistence(plan) => self
                .catalog
                .register(*plan)
                .map_err(PersistenceSchedulerPrepareError::Persistence),
            PersistenceSchedulerPreparation::Delegate(preparation) => self
                .delegate
                .prepare(preparation)
                .map_err(PersistenceSchedulerPrepareError::Delegate),
        }
    }
}

fn is_persistence_effect(kind: TransitionEffectKind) -> bool {
    matches!(
        kind,
        TransitionEffectKind::PersistTask
            | TransitionEffectKind::PersistQueueTransition
            | TransitionEffectKind::StageOptionPatch
            | TransitionEffectKind::PersistGenerationStarted
            | TransitionEffectKind::PersistConditions
            | TransitionEffectKind::PersistHostKeyChallenge
            | TransitionEffectKind::PersistHostKeyPinAndClearChallenge
            | TransitionEffectKind::PersistHostKeyChallengeRejected
            | TransitionEffectKind::PersistTerminal
            | TransitionEffectKind::DeleteStoppedTaskMetadata
    )
}

fn validate_plan(
    effect: &TransitionEffect,
    steps: &[PersistencePlanStep],
) -> Result<(), PersistencePlanError> {
    match (effect, steps) {
        (
            TransitionEffect::PersistTask {
                gid,
                queue,
                position,
                desired_paused,
                slow_demotion_count,
                conditions,
                ..
            },
            [PersistencePlanStep::PutTask(record)],
        ) => {
            if record.gid != *gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if record.queue_state != session_queue(*queue)
                || usize::try_from(record.queue_position).ok() != Some(*position)
                || record.desired_paused != *desired_paused
                || record.slow_demotion_count != *slow_demotion_count
                || record.slow_slot.is_some()
                || record.no_space.is_some() != conditions.no_space.is_some()
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistTask {
                gid,
                queue,
                position,
                desired_paused,
                slow_demotion_count,
                conditions,
                ..
            },
            [PersistencePlanStep::CreateTaskWithMetadata { task, sources, .. }],
        ) => {
            if task.gid != *gid
                || sources.is_empty()
                || sources.iter().any(|source| {
                    source.persistence_safe_uri.is_none() && !source.needs_credentials
                })
            {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if task.queue_state != session_queue(*queue)
                || usize::try_from(task.queue_position).ok() != Some(*position)
                || task.desired_paused != *desired_paused
                || task.slow_demotion_count != *slow_demotion_count
                || task.slow_slot.is_some()
                || task.no_space.is_some() != conditions.no_space.is_some()
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistQueueTransition {
                gid,
                from: Some(from),
                to: Some(to),
                desired_paused,
                slow_demotion_count,
                slow_slot,
                orders,
                ..
            },
            [PersistencePlanStep::TransitionTaskQueue(transition)],
        ) => validate_transition(
            *gid,
            *from,
            *to,
            *desired_paused,
            *slow_demotion_count,
            slow_slot.as_ref(),
            orders,
            transition,
        ),
        (
            TransitionEffect::StageOptionPatch { gid, patch_id, .. },
            [
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: journal_gid,
                    payload:
                        JournalPayload::OptionsSnapshot {
                            scope: journal_scope,
                            patch_id: journal_patch,
                            snapshot_hash,
                            options: journal_options,
                        },
                    ..
                },
                PersistencePlanStep::ReplaceTaskOptions {
                    gid: options_gid,
                    scope,
                    options,
                },
            ],
        ) => {
            if journal_gid != gid || options_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if *journal_patch != Some(*patch_id) {
                return Err(PersistencePlanError::TokenMismatch);
            }
            if *journal_scope != OptionsSnapshotScope::NextAdmission
                || *scope != OptionsSnapshotScope::NextAdmission
                || journal_options != options
                || *snapshot_hash != options.snapshot_hash()
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistQueueTransition {
                gid,
                from: Some(from),
                to: Some(to),
                desired_paused,
                slow_demotion_count,
                slow_slot,
                orders,
                ..
            },
            [
                PersistencePlanStep::ReplaceTaskSourcesAndQueue {
                    transition,
                    sources,
                },
            ],
        ) => {
            if sources.is_empty()
                || sources.iter().any(|source| {
                    source.persistence_safe_uri.is_none() && !source.needs_credentials
                })
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            validate_transition(
                *gid,
                *from,
                *to,
                *desired_paused,
                *slow_demotion_count,
                slow_slot.as_ref(),
                orders,
                transition,
            )
        }
        (
            TransitionEffect::PersistGenerationStarted {
                gid, generation, ..
            },
            [
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: journal_gid,
                    generation: journal_generation,
                    payload:
                        JournalPayload::GenerationStarted {
                            previous_generation,
                            reason,
                            next_snapshot_hash,
                            patch_id,
                        },
                },
                PersistencePlanStep::PromoteTaskOptions {
                    gid: options_gid,
                    options,
                },
            ],
        ) => {
            if journal_gid != gid || options_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if journal_generation != generation
                || previous_generation.checked_next() != Some(*generation)
            {
                return Err(PersistencePlanError::GenerationMismatch);
            }
            if *reason != ariax_storage::GenerationStartReason::OptionPatch
                || patch_id.is_none()
                || *next_snapshot_hash != options.snapshot_hash()
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistGenerationStarted {
                gid, generation, ..
            },
            [
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: marker_gid,
                    generation: marker_generation,
                    payload:
                        JournalPayload::TaskPaused {
                            reason: pause_reason,
                        },
                },
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: snapshot_gid,
                    generation: snapshot_generation,
                    payload:
                        JournalPayload::OptionsSnapshot {
                            scope,
                            patch_id: snapshot_patch,
                            snapshot_hash,
                            options,
                        },
                },
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: journal_gid,
                    generation: journal_generation,
                    payload:
                        JournalPayload::GenerationStarted {
                            previous_generation,
                            reason,
                            next_snapshot_hash,
                            patch_id: generation_patch,
                        },
                },
            ],
        ) => {
            if marker_gid != gid || snapshot_gid != gid || journal_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if journal_generation != generation
                || previous_generation.checked_next() != Some(*generation)
                || marker_generation != previous_generation
                || snapshot_generation != previous_generation
            {
                return Err(PersistencePlanError::GenerationMismatch);
            }
            if *pause_reason != TaskPauseReason::Restarting
                || *scope != OptionsSnapshotScope::NextAdmission
                || snapshot_patch.is_some()
                || generation_patch.is_some()
                || *reason != ariax_storage::GenerationStartReason::RepresentationRestart
                || snapshot_hash != next_snapshot_hash
                || *snapshot_hash != options.snapshot_hash()
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistGenerationStarted {
                gid, generation, ..
            },
            [
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: snapshot_gid,
                    generation: snapshot_generation,
                    payload:
                        JournalPayload::OptionsSnapshot {
                            scope,
                            patch_id: snapshot_patch,
                            snapshot_hash,
                            options,
                        },
                },
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: journal_gid,
                    generation: journal_generation,
                    payload:
                        JournalPayload::GenerationStarted {
                            previous_generation,
                            reason,
                            next_snapshot_hash,
                            patch_id: generation_patch,
                        },
                },
            ],
        ) => {
            if snapshot_gid != gid || journal_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if journal_generation != generation
                || previous_generation.checked_next() != Some(*generation)
                || snapshot_generation != previous_generation
            {
                return Err(PersistencePlanError::GenerationMismatch);
            }
            if *scope != OptionsSnapshotScope::NextAdmission
                || snapshot_patch.is_some()
                || generation_patch.is_some()
                || *reason == ariax_storage::GenerationStartReason::OptionPatch
                || snapshot_hash != next_snapshot_hash
                || *snapshot_hash != options.snapshot_hash()
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistGenerationStarted {
                gid, generation, ..
            },
            [
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: journal_gid,
                    generation: journal_generation,
                    payload:
                        JournalPayload::GenerationStarted {
                            previous_generation,
                            reason,
                            patch_id,
                            ..
                        },
                },
            ],
        ) => {
            if journal_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if journal_generation != generation
                || previous_generation.checked_next() != Some(*generation)
            {
                return Err(PersistencePlanError::GenerationMismatch);
            }
            let resumes_representation_restart = *reason
                == ariax_storage::GenerationStartReason::RepresentationRestart
                && patch_id.is_none();
            if !resumes_representation_restart {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistGenerationStarted {
                gid,
                generation: Generation::INITIAL,
                ..
            },
            [
                PersistencePlanStep::FlushJournal {
                    gid: journal_gid,
                    through_sequence,
                },
            ],
        ) => {
            if journal_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if *through_sequence == 0 {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistConditions {
                gid, conditions, ..
            },
            [
                PersistencePlanStep::SetNoSpaceCondition {
                    gid: command_gid,
                    condition,
                    ..
                },
            ],
        ) => {
            if command_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if condition.is_some() != conditions.no_space.is_some() {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistHostKeyChallenge { gid, challenge, .. },
            [PersistencePlanStep::PutHostKeyChallenge(record)],
        ) => {
            let summary = challenge.summary();
            if record.gid != *gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if record.challenge_id != summary.id
                || record.canonical_host != summary.canonical_host
                || record.port != summary.port
                || record.algorithm != summary.algorithm
                || record.presented_public_key != challenge.presented_public_key()
                || record.fingerprint_sha256 != summary.fingerprint_sha256
            {
                return Err(PersistencePlanError::TokenMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistHostKeyPinAndClearChallenge {
                gid,
                challenge,
                fingerprint_sha256,
                presented_public_key,
                ..
            },
            [PersistencePlanStep::ResolveHostKeyChallenge(resolution)],
        ) => {
            if resolution.gid != *gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if resolution.challenge_id != *challenge
                || resolution.fingerprint_sha256 != *fingerprint_sha256
                || resolution.presented_public_key != *presented_public_key
            {
                return Err(PersistencePlanError::TokenMismatch);
            }
            let expected_pin = session_host_key_pin_value(*fingerprint_sha256);
            if resolution.scope != OptionsSnapshotScope::CurrentGeneration
                || !resolution.pinned_options.entries().any(|(key, value)| {
                    key == ariax_storage::SESSION_HOST_KEY_PIN_OPTION && value == expected_pin
                })
            {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistHostKeyChallengeRejected { gid, challenge, .. },
            [
                PersistencePlanStep::RejectHostKeyChallenge {
                    gid: command_gid,
                    challenge_id,
                },
            ],
        ) => {
            if command_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if challenge_id != challenge {
                return Err(PersistencePlanError::TokenMismatch);
            }
            Ok(())
        }
        (
            TransitionEffect::PersistTerminal {
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
                ..
            },
            [
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: journal_gid,
                    generation: journal_generation,
                    payload,
                },
                PersistencePlanStep::PersistStoppedResult { result, transition },
            ],
        ) => {
            if journal_gid != gid || result.gid != *gid || transition.gid != *gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if journal_generation != generation {
                return Err(PersistencePlanError::GenerationMismatch);
            }
            validate_terminal(*status, error.as_ref(), payload, result)?;
            validate_transition(
                *gid,
                *from,
                *to,
                *desired_paused,
                *slow_demotion_count,
                slow_slot.as_ref(),
                orders,
                transition,
            )
        }
        (
            TransitionEffect::PersistTerminal {
                gid,
                status: Aria2Status::Complete,
                error: None,
                from,
                to,
                desired_paused,
                slow_demotion_count,
                slow_slot,
                orders,
                ..
            },
            [
                PersistencePlanStep::FlushJournal {
                    gid: journal_gid,
                    through_sequence,
                },
                PersistencePlanStep::PersistStoppedResult { result, transition },
            ],
        ) => {
            if journal_gid != gid || result.gid != *gid || transition.gid != *gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if *through_sequence == 0
                || result.status != SessionTerminalStatus::Complete
                || result.error_kind.is_some()
                || !result.safe_message.is_empty()
                || result.total_length.is_none()
                || result.layout_hash.is_none()
            {
                return Err(PersistencePlanError::TerminalMismatch);
            }
            validate_transition(
                *gid,
                *from,
                *to,
                *desired_paused,
                *slow_demotion_count,
                slow_slot.as_ref(),
                orders,
                transition,
            )
        }
        (
            TransitionEffect::DeleteStoppedTaskMetadata {
                gid,
                remaining_order,
                ..
            },
            [
                PersistencePlanStep::DeleteStoppedTaskMetadata {
                    gid: command_gid,
                    remaining_order: command_order,
                    ..
                },
            ],
        ) => {
            if command_gid != gid {
                return Err(PersistencePlanError::IdentityMismatch);
            }
            if command_order != remaining_order {
                return Err(PersistencePlanError::StateMismatch);
            }
            Ok(())
        }
        (TransitionEffect::PersistQueueTransition { .. }, _) => {
            Err(PersistencePlanError::StateMismatch)
        }
        _ => Err(PersistencePlanError::InvalidSequence),
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_transition(
    gid: Gid,
    from: QueueClass,
    to: QueueClass,
    desired_paused: bool,
    slow_demotion_count: u32,
    slow_slot: Option<&SlowSlotPersistence>,
    orders: &[QueueOrder],
    transition: &SessionQueueTransition,
) -> Result<(), PersistencePlanError> {
    if transition.gid != gid {
        return Err(PersistencePlanError::IdentityMismatch);
    }
    if transition.expected_state != session_queue(from)
        || transition.target_state != session_queue(to)
        || transition.desired_paused != desired_paused
        || transition.slow_demotion_count != slow_demotion_count
        || !slow_slot_matches(slow_slot, transition.slow_slot.as_ref())
        || !orders_match(orders, &transition.final_orders)
    {
        return Err(PersistencePlanError::StateMismatch);
    }
    Ok(())
}

fn validate_terminal(
    status: Aria2Status,
    error: Option<&ariax_core::PublicError>,
    payload: &JournalPayload,
    result: &SessionStoppedResultRecord,
) -> Result<(), PersistencePlanError> {
    match (status, error, payload, result.status) {
        (
            Aria2Status::Complete,
            None,
            JournalPayload::TaskComplete {
                layout_hash,
                final_length,
                completed_at_unix_ms,
                ..
            },
            SessionTerminalStatus::Complete,
        ) if result.error_kind.is_none()
            && result.safe_message.is_empty()
            && result.total_length == Some(*final_length)
            && result.layout_hash == Some(*layout_hash)
            && result.completed_ms == *completed_at_unix_ms =>
        {
            Ok(())
        }
        (
            Aria2Status::Error,
            Some(error),
            JournalPayload::TaskError {
                error_class,
                retriable,
                diagnostic_id,
            },
            SessionTerminalStatus::Error,
        ) if result.error_kind == Some(error.kind())
            && result.safe_message == error.safe_message()
            && result.total_length.is_none()
            && result.layout_hash.is_none()
            && *error_class == error.kind()
            && *diagnostic_id == error.diagnostic_id().unwrap_or(0)
            && *retriable
                == matches!(
                    error.retry_class(),
                    RetryClass::SameSource
                        | RetryClass::AnotherSource
                        | RetryClass::RestartGeneration
                ) =>
        {
            Ok(())
        }
        (
            Aria2Status::Removed,
            None,
            JournalPayload::TaskRemoved { .. },
            SessionTerminalStatus::Removed,
        ) if result.error_kind.is_none()
            && result.safe_message.is_empty()
            && result.total_length.is_none()
            && result.layout_hash.is_none() =>
        {
            Ok(())
        }
        _ => Err(PersistencePlanError::TerminalMismatch),
    }
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

fn slow_slot_matches(
    core: Option<&SlowSlotPersistence>,
    session: Option<&SessionSlowSlotState>,
) -> bool {
    match (core, session) {
        (None, None) => true,
        (Some(core), Some(session)) => {
            u32::try_from(core.original_position).ok() == Some(session.original_position)
                && session.retry.is_some_and(|retry| {
                    retry.scheduled_at_ms == core.decision.scheduled_at_ms
                        && retry.delay_ms == core.decision.delay_ms
                })
        }
        _ => false,
    }
}

fn orders_match(core: &[QueueOrder], session: &[SessionQueueOrder]) -> bool {
    core.len() == session.len()
        && core.iter().zip(session).all(|(core, session)| {
            session.state == session_queue(core.class) && session.gids == core.order
        })
}

fn validate_dispatched_plan(
    dispatched: &DispatchedEffect,
    plan: &PersistenceEffectPlan,
) -> Result<(), PersistencePlanError> {
    if plan.effect != *dispatched.effect() {
        return Err(PersistencePlanError::IdentityMismatch);
    }
    if dispatched
        .effect()
        .generation()
        .is_some_and(|generation| generation != dispatched.task_generation())
    {
        return Err(PersistencePlanError::GenerationMismatch);
    }
    for step in &plan.steps {
        if let PersistencePlanStep::AppendAndFlushJournal {
            generation,
            payload,
            ..
        } = step
            && *generation != dispatched.task_generation()
        {
            let stages_next_admission = matches!(
                (dispatched.effect(), payload),
                (
                    TransitionEffect::PersistGenerationStarted { generation: next, .. },
                    JournalPayload::OptionsSnapshot {
                        scope: OptionsSnapshotScope::NextAdmission,
                        patch_id: None,
                        ..
                    }
                ) if generation.checked_next() == Some(*next)
            );
            let stages_representation_restart = matches!(
                (dispatched.effect(), payload),
                (
                    TransitionEffect::PersistGenerationStarted { generation: next, .. },
                    JournalPayload::TaskPaused {
                        reason: TaskPauseReason::Restarting,
                    }
                ) if generation.checked_next() == Some(*next)
            );
            if !stages_next_admission && !stages_representation_restart {
                return Err(PersistencePlanError::GenerationMismatch);
            }
        }
    }
    Ok(())
}

fn command_for_step(step: &PersistencePlanStep) -> PendingOwnerCommand {
    let (command, expected) = match step {
        PersistencePlanStep::PutTask(record) => (
            SessionCommand::PutTask(record.clone()),
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::CreateTaskWithMetadata {
            task,
            sources,
            options,
        } => (
            SessionCommand::CreateTaskWithMetadata {
                task: task.clone(),
                sources: sources.clone(),
                options: options.clone(),
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::TransitionTaskQueue(transition) => (
            SessionCommand::TransitionTaskQueue(transition.clone()),
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::ReplaceTaskSourcesAndQueue {
            transition,
            sources,
        } => (
            SessionCommand::ReplaceTaskSourcesAndQueue {
                transition: transition.clone(),
                sources: sources.clone(),
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::ReplaceTaskOptions {
            gid,
            scope,
            options,
        } => (
            SessionCommand::ReplaceTaskOptions {
                gid: *gid,
                scope: *scope,
                options: options.clone(),
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::PromoteTaskOptions { gid, options } => (
            SessionCommand::PromoteTaskOptions {
                gid: *gid,
                options: options.clone(),
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::SetNoSpaceCondition {
            gid,
            condition,
            updated_ms,
        } => (
            SessionCommand::SetNoSpaceCondition {
                gid: *gid,
                condition: condition.clone(),
                updated_ms: *updated_ms,
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::PutHostKeyChallenge(challenge) => (
            SessionCommand::PutHostKeyChallenge(challenge.clone()),
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::ResolveHostKeyChallenge(resolution) => (
            SessionCommand::ResolveHostKeyChallenge(resolution.clone()),
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::RejectHostKeyChallenge { gid, challenge_id } => (
            SessionCommand::RejectHostKeyChallenge {
                gid: *gid,
                challenge_id: *challenge_id,
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::PersistStoppedResult { result, transition } => (
            SessionCommand::PersistStoppedResult {
                result: result.clone(),
                transition: transition.clone(),
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::DeleteStoppedTaskMetadata {
            gid,
            remaining_order,
            updated_ms,
        } => (
            SessionCommand::DeleteStoppedTaskMetadata {
                gid: *gid,
                remaining_order: remaining_order.clone(),
                updated_ms: *updated_ms,
            },
            ExpectedResult::Unit,
        ),
        PersistencePlanStep::AppendAndFlushJournal {
            gid,
            generation,
            payload,
        } => (
            SessionCommand::AppendJournal {
                gid: *gid,
                generation: *generation,
                payload: payload.clone(),
            },
            ExpectedResult::Appended,
        ),
        PersistencePlanStep::FlushJournal {
            gid,
            through_sequence,
        } => (
            SessionCommand::FlushJournal {
                gid: *gid,
                through_sequence: *through_sequence,
            },
            ExpectedResult::Flushed(*through_sequence),
        ),
    };
    PendingOwnerCommand { command, expected }
}

fn apply_result(
    active: &mut ActivePersistence,
    expected: ExpectedResult,
    result: SessionCommandResult,
) -> bool {
    match (expected, result) {
        (ExpectedResult::Unit, SessionCommandResult::Unit) => {
            active.step_index += 1;
            true
        }
        (ExpectedResult::Appended, SessionCommandResult::JournalAppended(appended)) => {
            let PersistencePlanStep::AppendAndFlushJournal { gid, .. } =
                &active.plan.steps[active.step_index]
            else {
                return false;
            };
            let sequence = appended.sequence();
            active.pending = Some(PendingOwnerCommand {
                command: SessionCommand::FlushJournal {
                    gid: *gid,
                    through_sequence: sequence,
                },
                expected: ExpectedResult::Flushed(sequence),
            });
            true
        }
        (ExpectedResult::Flushed(sequence), SessionCommandResult::JournalFlushed(flushed))
            if flushed.through_sequence() >= sequence =>
        {
            active.step_index += 1;
            true
        }
        _ => false,
    }
}

fn success_completion(active: &ActivePersistence) -> EffectCompletion {
    EffectCompletion::Completed {
        dispatch_id: active.dispatch_id,
        acknowledgement: success_acknowledgement(active),
    }
}

fn success_acknowledgement(active: &ActivePersistence) -> Option<TaskEventEnvelope> {
    let task_id = active.plan.effect.task_id();
    let gid = active.plan.effect.gid();
    let generation = active.task_generation;
    let event = match &active.plan.effect {
        TransitionEffect::StageOptionPatch { patch_id, .. } => TaskEvent::OptionPatchPersisted {
            gid,
            generation,
            patch_id: *patch_id,
        },
        TransitionEffect::PersistGenerationStarted { .. } => {
            TaskEvent::GenerationPersisted { gid, generation }
        }
        TransitionEffect::PersistHostKeyPinAndClearChallenge { resolution_id, .. } => {
            TaskEvent::HostKeyResolutionPersisted {
                gid,
                generation,
                resolution_id: *resolution_id,
            }
        }
        TransitionEffect::PersistTerminal { status, .. } => TaskEvent::TerminalPersisted {
            gid,
            generation,
            status: *status,
        },
        TransitionEffect::DeleteStoppedTaskMetadata { deletion_id, .. } => {
            TaskEvent::StoppedResultDeleted {
                gid,
                generation,
                deletion_id: *deletion_id,
            }
        }
        TransitionEffect::PersistTask { .. }
        | TransitionEffect::PersistQueueTransition { .. }
        | TransitionEffect::PersistConditions { .. }
        | TransitionEffect::PersistHostKeyChallenge { .. }
        | TransitionEffect::PersistHostKeyChallengeRejected { .. } => return None,
        _ => return None,
    };
    Some(event.for_task(task_id))
}

fn represented_stage_failure(active: &ActivePersistence) -> EffectCompletion {
    let task_id = active.plan.effect.task_id();
    let gid = active.plan.effect.gid();
    let generation = active.task_generation;
    let event = match &active.plan.effect {
        TransitionEffect::StageOptionPatch { patch_id, .. } => {
            TaskEvent::OptionPatchPersistenceFailed {
                gid,
                generation,
                patch_id: *patch_id,
            }
        }
        _ => {
            return EffectCompletion::UnrepresentableFailure {
                dispatch_id: active.dispatch_id,
            };
        }
    };
    EffectCompletion::Completed {
        dispatch_id: active.dispatch_id,
        acknowledgement: Some(event.for_task(task_id)),
    }
}

fn owner_command_failure(
    active: &ActivePersistence,
    expected: ExpectedResult,
    error: &SessionOwnerError,
) -> EffectCompletion {
    if matches!(
        active.plan.effect,
        TransitionEffect::StageOptionPatch { .. }
    ) && expected == ExpectedResult::Appended
        && definite_pre_append_failure(error)
    {
        return represented_stage_failure(active);
    }
    EffectCompletion::UnrepresentableFailure {
        dispatch_id: active.dispatch_id,
    }
}

fn definite_pre_append_failure(error: &SessionOwnerError) -> bool {
    matches!(
        error,
        SessionOwnerError::Persistence(SessionPersistenceError::MissingJournal { .. })
            | SessionOwnerError::Persistence(SessionPersistenceError::Journal {
                error: JournalAppenderError::Payload(_) | JournalAppenderError::Journal(_),
                ..
            })
    )
}

fn post_acceptance_admission_failure(active: &ActivePersistence) -> EffectCompletion {
    EffectCompletion::UnrepresentableFailure {
        dispatch_id: active.dispatch_id,
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
enum TestCommandSnapshot {
    PutTask(SessionTaskRecord),
    TransitionTaskQueue(SessionQueueTransition),
    ReplaceTaskOptions {
        gid: Gid,
        scope: OptionsSnapshotScope,
        options: SanitizedOptionMap,
    },
    AppendJournal {
        gid: Gid,
        generation: Generation,
        payload: JournalPayload,
    },
    FlushJournal {
        gid: Gid,
        through_sequence: u64,
    },
    Other(&'static str),
}

#[cfg(test)]
enum TestAdmission {
    Backpressured,
    Complete(SessionCommandResult),
    Fail(SessionOwnerError),
}

#[cfg(test)]
struct TestSessionState {
    admissions: VecDeque<TestAdmission>,
    submitted: Vec<TestCommandSnapshot>,
}

#[cfg(test)]
struct TestSessionEndpoint {
    shared: std::sync::Arc<std::sync::Mutex<TestSessionState>>,
}

#[cfg(test)]
#[derive(Clone)]
struct TestSessionControl {
    shared: std::sync::Arc<std::sync::Mutex<TestSessionState>>,
}

#[cfg(test)]
impl TestSessionEndpoint {
    fn new(admissions: impl IntoIterator<Item = TestAdmission>) -> (Self, TestSessionControl) {
        let shared = std::sync::Arc::new(std::sync::Mutex::new(TestSessionState {
            admissions: admissions.into_iter().collect(),
            submitted: Vec::new(),
        }));
        (
            Self {
                shared: std::sync::Arc::clone(&shared),
            },
            TestSessionControl { shared },
        )
    }

    fn try_submit(
        &mut self,
        command: SessionCommand,
    ) -> Result<OwnerCompletion, RejectedOwnerCommand> {
        let snapshot = snapshot_command(&command);
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.submitted.push(snapshot);
        let admission = state
            .admissions
            .pop_front()
            .expect("test session admission script exhausted");
        drop(state);
        match admission {
            TestAdmission::Backpressured => Err(RejectedOwnerCommand {
                command: Box::new(command),
                error: SessionOwnerError::QueueFull,
            }),
            TestAdmission::Complete(result) => Ok(OwnerCompletion::Test(TestSessionCompletion {
                result: std::cell::RefCell::new(Some(Ok(result))),
            })),
            TestAdmission::Fail(error) => Ok(OwnerCompletion::Test(TestSessionCompletion {
                result: std::cell::RefCell::new(Some(Err(error))),
            })),
        }
    }
}

#[cfg(test)]
impl TestSessionControl {
    fn push(&self, admission: TestAdmission) {
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .admissions
            .push_back(admission);
    }

    fn submitted(&self) -> Vec<TestCommandSnapshot> {
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .submitted
            .clone()
    }
}

#[cfg(test)]
struct TestSessionCompletion {
    result: std::cell::RefCell<Option<Result<SessionCommandResult, SessionOwnerError>>>,
}

#[cfg(test)]
impl TestSessionCompletion {
    fn try_wait(&self) -> Result<Option<SessionCommandResult>, SessionOwnerError> {
        self.result
            .borrow_mut()
            .take()
            .expect("test completion polled more than once")
            .map(Some)
    }
}

#[cfg(test)]
fn snapshot_command(command: &SessionCommand) -> TestCommandSnapshot {
    match command {
        SessionCommand::PutTask(record) => TestCommandSnapshot::PutTask(record.clone()),
        SessionCommand::TransitionTaskQueue(transition) => {
            TestCommandSnapshot::TransitionTaskQueue(transition.clone())
        }
        SessionCommand::ReplaceTaskOptions {
            gid,
            scope,
            options,
        } => TestCommandSnapshot::ReplaceTaskOptions {
            gid: *gid,
            scope: *scope,
            options: options.clone(),
        },
        SessionCommand::AppendJournal {
            gid,
            generation,
            payload,
        } => TestCommandSnapshot::AppendJournal {
            gid: *gid,
            generation: *generation,
            payload: payload.clone(),
        },
        SessionCommand::FlushJournal {
            gid,
            through_sequence,
        } => TestCommandSnapshot::FlushJournal {
            gid: *gid,
            through_sequence: *through_sequence,
        },
        _ => TestCommandSnapshot::Other("other"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ariax_core::{
        HostKeyChallengeId, HostKeyFingerprint, HostKeyResolutionId, MonotonicInstant,
        OptionPatchId, RecoveredSchedulerTask, RequestScheduler, SchedulerCommand, SchedulerConfig,
        SchedulerRestoreBatch, StoppedResultDeletionId, TaskConditions, TaskId, TaskState,
        ValidatedOptionPatchKind,
    };
    use ariax_runtime::{SchedulerDriver, SchedulerDriverFault, SchedulerDriverPoll};
    use ariax_storage::{
        Appended, ControlJournalAppender, Flushed, GenerationStartReason, JournalAppenderFault,
        JournalHash, JournalId, JournalIoOperation, PathPlatform, PlatformPath, SessionId,
        SessionStoreError, TaskPauseReason,
    };
    use std::fs;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory {
        path: std::path::PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "ariax-engine-effect-sink-{}-{id}",
                std::process::id()
            ));
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder
                    .create(&path)
                    .expect("create private test directory");
            }
            #[cfg(windows)]
            ariax_windows_security::create_private_directory(&path)
                .expect("create private test directory");
            Self { path }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct RecordingDelegate {
        kinds: Arc<Mutex<Vec<TransitionEffectKind>>>,
        in_flight: Option<EffectDispatchId>,
    }

    impl RecordingDelegate {
        fn new() -> (Self, Arc<Mutex<Vec<TransitionEffectKind>>>) {
            let kinds = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    kinds: Arc::clone(&kinds),
                    in_flight: None,
                },
                kinds,
            )
        }
    }

    impl SchedulerEffectSink for RecordingDelegate {
        fn poll_dispatch(
            &mut self,
            effect: &DispatchedEffect,
        ) -> Poll<Result<(), EffectSinkError>> {
            self.kinds
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(effect.effect().kind());
            self.in_flight = Some(effect.dispatch_id());
            Poll::Ready(Ok(()))
        }

        fn poll_completion(&mut self) -> Poll<EffectCompletion> {
            let dispatch_id = self.in_flight.take().expect("delegated effect in flight");
            Poll::Ready(EffectCompletion::Completed {
                dispatch_id,
                acknowledgement: None,
            })
        }
    }

    impl SchedulerEffectSinkPrepare for RecordingDelegate {
        type Preparation = ();
        type Error = std::convert::Infallible;

        fn prepare(&mut self, (): Self::Preparation) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("nonzero GID")
    }

    fn task_id(value: u64) -> TaskId {
        TaskId::new(value).expect("nonzero task id")
    }

    fn hash(value: u8) -> JournalHash {
        JournalHash::new([value; 32]).expect("nonzero journal hash")
    }

    fn path(value: &str) -> PlatformPath {
        PlatformPath::from_native_bytes(PathPlatform::Unix, value.as_bytes()).expect("path")
    }

    fn config() -> SchedulerConfig {
        SchedulerConfig::new(
            NonZeroUsize::new(8).expect("task cap"),
            NonZeroUsize::new(2).expect("active cap"),
            false,
        )
        .expect("scheduler config")
    }

    fn task_record(task_gid: Gid) -> SessionTaskRecord {
        SessionTaskRecord {
            gid: task_gid,
            session_id: SessionId::new([1; 16]),
            queue_state: SessionQueueState::Waiting,
            queue_position: 0,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            primary_journal_id: JournalId::new([2; 16]).expect("journal id"),
            primary_journal_path: path("/journal/task"),
            replica_journal_path: None,
            replica_sequence: None,
            root_display: path("/output/task"),
            cached_layout_hash: None,
            cached_root_binding_hash: None,
            cached_snapshot_hash: hash(3),
            no_space: None,
            created_ms: 10,
            updated_ms: 10,
        }
    }

    fn persist_task_effect(task_gid: Gid) -> TransitionEffect {
        TransitionEffect::PersistTask {
            task_id: task_id(1),
            gid: task_gid,
            queue: QueueClass::Waiting,
            position: 0,
            desired_paused: false,
            slow_demotion_count: 0,
            conditions: TaskConditions::default(),
        }
    }

    fn persist_task_plan(task_gid: Gid) -> PersistenceEffectPlan {
        PersistenceEffectPlan::new(
            persist_task_effect(task_gid),
            vec![PersistencePlanStep::PutTask(task_record(task_gid))],
        )
        .expect("valid task plan")
    }

    fn active_persistence(effect: TransitionEffect) -> ActivePersistence {
        ActivePersistence {
            dispatch_id: EffectDispatchId::new(1).expect("dispatch id"),
            task_generation: Generation::INITIAL,
            plan: PersistenceEffectPlan {
                effect,
                steps: Vec::new(),
            },
            step_index: 0,
            pending: None,
            completion: None,
            in_flight_expected: Some(ExpectedResult::Unit),
        }
    }

    fn catalog(plans: impl IntoIterator<Item = PersistenceEffectPlan>) -> PersistenceEffectCatalog {
        let mut catalog =
            PersistenceEffectCatalog::new(NonZeroUsize::new(16).expect("catalog capacity"))
                .expect("catalog");
        for plan in plans {
            catalog.register(plan).expect("register plan");
        }
        catalog
    }

    fn finish_chain<S: SchedulerEffectSink>(driver: &mut SchedulerDriver<S>) {
        for _ in 0..64 {
            match driver.poll() {
                SchedulerDriverPoll::Completed { .. } => return,
                SchedulerDriverPoll::Faulted(fault) => panic!("driver faulted: {fault:?}"),
                _ => {}
            }
        }
        panic!("driver chain did not finish within its bounded poll budget");
    }

    fn expect_unrepresentable_fault<S: SchedulerEffectSink>(driver: &mut SchedulerDriver<S>) {
        for _ in 0..32 {
            if matches!(
                driver.poll(),
                SchedulerDriverPoll::Faulted(
                    SchedulerDriverFault::UnrepresentableEffectFailure { .. }
                )
            ) {
                return;
            }
        }
        panic!("driver did not report the expected unrepresentable failure");
    }

    fn add_task<S: SchedulerEffectSink>(driver: &mut SchedulerDriver<S>, task_gid: Gid) {
        driver
            .execute_command(SchedulerCommand::AddValidatedTask {
                task_id: task_id(1),
                gid: task_gid,
                desired_paused: false,
                conditions: TaskConditions::default(),
            })
            .expect("start add task");
        finish_chain(driver);
    }

    fn journal_evidence(task_gid: Gid) -> (Appended, Flushed) {
        let directory = TestDirectory::new();
        let mut appender = ControlJournalAppender::create(
            directory.path.join("journal"),
            task_gid,
            JournalId::new([9; 16]).expect("journal id"),
            Generation::INITIAL,
            10,
        )
        .expect("create appender");
        let appended = appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::TaskPaused {
                    reason: TaskPauseReason::User,
                },
            )
            .expect("append evidence");
        let flushed = appender.flush(appended.sequence()).expect("flush evidence");
        (appended, flushed)
    }

    fn stage_plan(task_gid: Gid, patch_id: OptionPatchId) -> PersistenceEffectPlan {
        let options = SanitizedOptionMap::new([("split".to_owned(), "4".to_owned())])
            .expect("sanitized options");
        PersistenceEffectPlan::new(
            TransitionEffect::StageOptionPatch {
                task_id: task_id(1),
                gid: task_gid,
                patch_id,
                satisfies_credentials: None,
            },
            vec![
                PersistencePlanStep::AppendAndFlushJournal {
                    gid: task_gid,
                    generation: Generation::INITIAL,
                    payload: JournalPayload::OptionsSnapshot {
                        scope: OptionsSnapshotScope::NextAdmission,
                        patch_id: Some(patch_id),
                        snapshot_hash: options.snapshot_hash(),
                        options: options.clone(),
                    },
                },
                PersistencePlanStep::ReplaceTaskOptions {
                    gid: task_gid,
                    scope: OptionsSnapshotScope::NextAdmission,
                    options,
                },
            ],
        )
        .expect("valid stage plan")
    }

    fn assert_stage_admissions_fault(stage_admissions: Vec<TestAdmission>) {
        let task_gid = gid(1);
        let patch_id = OptionPatchId::new(4).expect("patch id");
        let (_, activation_flushed) = journal_evidence(task_gid);
        let mut admissions = vec![
            TestAdmission::Complete(SessionCommandResult::Unit),
            TestAdmission::Complete(SessionCommandResult::JournalFlushed(activation_flushed)),
            TestAdmission::Complete(SessionCommandResult::Unit),
        ];
        admissions.extend(stage_admissions);
        let (session, _) = TestSessionEndpoint::new(admissions);
        let (delegate, _) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog([persist_task_plan(task_gid)]),
        );
        let mut driver = SchedulerDriver::new(RequestScheduler::new(config()), sink);
        add_task(&mut driver, task_gid);
        activate_task(&mut driver, task_gid);
        driver
            .prepare_sink(PersistenceSchedulerPreparation::Persistence(Box::new(
                stage_plan(task_gid, patch_id),
            )))
            .expect("register stage plan");
        driver
            .execute_command(SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            })
            .expect("stage option patch");

        expect_unrepresentable_fault(&mut driver);
    }

    fn activation_plans(task_gid: Gid) -> [PersistenceEffectPlan; 2] {
        let orders = vec![
            QueueOrder {
                class: QueueClass::Waiting,
                order: Vec::new(),
            },
            QueueOrder {
                class: QueueClass::Active,
                order: vec![task_gid],
            },
        ];
        let transition = SessionQueueTransition {
            gid: task_gid,
            expected_state: SessionQueueState::Waiting,
            target_state: SessionQueueState::Active,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: vec![
                SessionQueueOrder {
                    state: SessionQueueState::Waiting,
                    gids: Vec::new(),
                },
                SessionQueueOrder {
                    state: SessionQueueState::Active,
                    gids: vec![task_gid],
                },
            ],
            updated_ms: 20,
        };
        [
            PersistenceEffectPlan::new(
                TransitionEffect::PersistQueueTransition {
                    task_id: task_id(1),
                    gid: task_gid,
                    from: Some(QueueClass::Waiting),
                    to: Some(QueueClass::Active),
                    desired_paused: false,
                    slow_demotion_count: 0,
                    slow_slot: None,
                    orders,
                },
                vec![PersistencePlanStep::TransitionTaskQueue(transition)],
            )
            .expect("activation queue plan"),
            PersistenceEffectPlan::new(
                TransitionEffect::PersistGenerationStarted {
                    task_id: task_id(1),
                    gid: task_gid,
                    generation: Generation::INITIAL,
                },
                vec![PersistencePlanStep::FlushJournal {
                    gid: task_gid,
                    through_sequence: 1,
                }],
            )
            .expect("initial generation plan"),
        ]
    }

    fn activate_task<D: SchedulerEffectSinkPrepare>(
        driver: &mut SchedulerDriver<PersistenceSchedulerEffectSink<D>>,
        task_gid: Gid,
    ) where
        D::Error: std::fmt::Debug,
    {
        for plan in activation_plans(task_gid) {
            driver
                .prepare_sink(PersistenceSchedulerPreparation::Persistence(Box::new(plan)))
                .expect("register activation plan");
        }
        driver.admit_next().expect("admit task");
        finish_chain(driver);
        driver
            .handle_event(
                &TaskEvent::AllocationSucceeded {
                    gid: task_gid,
                    generation: Generation::INITIAL,
                }
                .for_task(task_id(1)),
            )
            .expect("allocation succeeded");
        finish_chain(driver);
    }

    #[test]
    fn exact_task_plan_survives_backpressure_and_completes_once() {
        let task_gid = gid(1);
        let (session, control) = TestSessionEndpoint::new([
            TestAdmission::Backpressured,
            TestAdmission::Complete(SessionCommandResult::Unit),
        ]);
        let (delegate, _) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog([persist_task_plan(task_gid)]),
        );
        let mut driver = SchedulerDriver::new(RequestScheduler::new(config()), sink);
        driver
            .execute_command(SchedulerCommand::AddValidatedTask {
                task_id: task_id(1),
                gid: task_gid,
                desired_paused: false,
                conditions: TaskConditions::default(),
            })
            .expect("start add task");

        assert!(matches!(
            driver.poll(),
            SchedulerDriverPoll::Backpressured { .. }
        ));
        finish_chain(&mut driver);
        let submitted = control.submitted();
        assert_eq!(submitted.len(), 2);
        assert_eq!(submitted[0], submitted[1]);
        assert!(driver.sink().catalog().is_empty());
    }

    #[test]
    fn plan_validation_rejects_wrong_identity_token_and_capacity() {
        let task_gid = gid(1);
        assert_eq!(
            PersistenceEffectPlan::new(
                persist_task_effect(task_gid),
                vec![PersistencePlanStep::PutTask(task_record(gid(2)))],
            ),
            Err(PersistencePlanError::IdentityMismatch)
        );

        let patch_id = OptionPatchId::new(4).expect("patch id");
        let mut wrong_token = stage_plan(task_gid, patch_id);
        if let PersistencePlanStep::AppendAndFlushJournal {
            payload: JournalPayload::OptionsSnapshot { patch_id, .. },
            ..
        } = &mut wrong_token.steps[0]
        {
            *patch_id = Some(OptionPatchId::new(5).expect("other patch"));
        }
        assert_eq!(
            PersistenceEffectPlan::new(wrong_token.effect, wrong_token.steps),
            Err(PersistencePlanError::TokenMismatch)
        );
        assert!(matches!(
            PersistenceEffectCatalog::new(
                NonZeroUsize::new(MAX_PERSISTENCE_CATALOG_ENTRIES + 1).expect("oversized capacity")
            ),
            Err(PersistenceCatalogError::CapacityTooLarge)
        ));
    }

    #[test]
    fn metadata_task_plan_requires_an_atomic_recoverable_source_set() {
        let task_gid = gid(1);
        let source = SessionTaskSourceRecord {
            uri_id: 1,
            persistence_safe_uri: Some("https://example.test/file".to_owned()),
            redacted_fingerprint: [7; 32],
            needs_credentials: false,
            priority: 0,
        };
        let options = SanitizedOptionMap::new([("out".to_owned(), "file".to_owned())])
            .expect("sanitized options");
        assert!(
            PersistenceEffectPlan::new(
                persist_task_effect(task_gid),
                vec![PersistencePlanStep::CreateTaskWithMetadata {
                    task: task_record(task_gid),
                    sources: vec![source.clone()],
                    options: options.clone(),
                }],
            )
            .is_ok()
        );
        assert_eq!(
            PersistenceEffectPlan::new(
                persist_task_effect(task_gid),
                vec![PersistencePlanStep::CreateTaskWithMetadata {
                    task: task_record(task_gid),
                    sources: Vec::new(),
                    options: options.clone(),
                }],
            ),
            Err(PersistencePlanError::IdentityMismatch)
        );
        let mut unrecoverable = source;
        unrecoverable.persistence_safe_uri = None;
        assert_eq!(
            PersistenceEffectPlan::new(
                persist_task_effect(task_gid),
                vec![PersistencePlanStep::CreateTaskWithMetadata {
                    task: task_record(task_gid),
                    sources: vec![unrecoverable],
                    options,
                }],
            ),
            Err(PersistencePlanError::IdentityMismatch)
        );
    }

    #[test]
    fn completed_terminal_plan_flushes_exact_evidence_before_stopped_result() {
        let task_gid = gid(1);
        let orders = vec![
            QueueOrder {
                class: QueueClass::Active,
                order: Vec::new(),
            },
            QueueOrder {
                class: QueueClass::Stopped,
                order: vec![task_gid],
            },
        ];
        let effect = TransitionEffect::PersistTerminal {
            task_id: task_id(1),
            gid: task_gid,
            generation: Generation::INITIAL,
            status: Aria2Status::Complete,
            error: None,
            from: QueueClass::Active,
            to: QueueClass::Stopped,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            orders: orders.clone(),
        };
        let result = SessionStoppedResultRecord {
            gid: task_gid,
            status: SessionTerminalStatus::Complete,
            error_kind: None,
            safe_message: String::new(),
            total_length: Some(1024),
            layout_hash: Some(hash(9)),
            completed_ms: 30,
        };
        let transition = SessionQueueTransition {
            gid: task_gid,
            expected_state: SessionQueueState::Active,
            target_state: SessionQueueState::Stopped,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            final_orders: orders
                .into_iter()
                .map(|order| SessionQueueOrder {
                    state: session_queue(order.class),
                    gids: order.order,
                })
                .collect(),
            updated_ms: 30,
        };
        let valid_steps = vec![
            PersistencePlanStep::FlushJournal {
                gid: task_gid,
                through_sequence: 7,
            },
            PersistencePlanStep::PersistStoppedResult {
                result: result.clone(),
                transition: transition.clone(),
            },
        ];
        assert!(PersistenceEffectPlan::new(effect.clone(), valid_steps.clone()).is_ok());

        let mut reversed = valid_steps;
        reversed.reverse();
        assert_eq!(
            PersistenceEffectPlan::new(effect.clone(), reversed),
            Err(PersistencePlanError::InvalidSequence)
        );
        assert_eq!(
            PersistenceEffectPlan::new(
                effect,
                vec![
                    PersistencePlanStep::FlushJournal {
                        gid: task_gid,
                        through_sequence: 0,
                    },
                    PersistencePlanStep::PersistStoppedResult { result, transition },
                ],
            ),
            Err(PersistencePlanError::TerminalMismatch)
        );
    }

    #[test]
    fn multi_command_stage_advances_append_flush_and_sqlite_one_at_a_time() {
        let task_gid = gid(1);
        let patch_id = OptionPatchId::new(4).expect("patch id");
        let (appended, flushed) = journal_evidence(task_gid);
        let (session, control) = TestSessionEndpoint::new([
            TestAdmission::Complete(SessionCommandResult::Unit),
            TestAdmission::Complete(SessionCommandResult::JournalFlushed(flushed)),
            TestAdmission::Complete(SessionCommandResult::Unit),
            TestAdmission::Complete(SessionCommandResult::JournalAppended(appended)),
            TestAdmission::Complete(SessionCommandResult::JournalFlushed(flushed)),
            TestAdmission::Complete(SessionCommandResult::Unit),
        ]);
        let (delegate, _) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog([persist_task_plan(task_gid)]),
        );
        let mut driver = SchedulerDriver::new(RequestScheduler::new(config()), sink);
        add_task(&mut driver, task_gid);
        activate_task(&mut driver, task_gid);
        driver
            .prepare_sink(PersistenceSchedulerPreparation::Persistence(Box::new(
                stage_plan(task_gid, patch_id),
            )))
            .expect("register stage plan");
        driver
            .execute_command(SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            })
            .expect("stage option patch");
        finish_chain(&mut driver);

        let submitted = control.submitted();
        assert_eq!(submitted.len(), 6);
        assert!(matches!(
            submitted[3],
            TestCommandSnapshot::AppendJournal { .. }
        ));
        assert!(matches!(
            submitted[4],
            TestCommandSnapshot::FlushJournal {
                through_sequence: 1,
                ..
            }
        ));
        assert!(matches!(
            submitted[5],
            TestCommandSnapshot::ReplaceTaskOptions { .. }
        ));
    }

    #[test]
    fn representable_stage_failure_returns_correlated_failure_acknowledgement() {
        let task_gid = gid(1);
        let patch_id = OptionPatchId::new(4).expect("patch id");
        let (_, flushed) = journal_evidence(task_gid);
        let (session, control) = TestSessionEndpoint::new([
            TestAdmission::Complete(SessionCommandResult::Unit),
            TestAdmission::Complete(SessionCommandResult::JournalFlushed(flushed)),
            TestAdmission::Complete(SessionCommandResult::Unit),
        ]);
        let (delegate, _) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog([persist_task_plan(task_gid)]),
        );
        let mut driver = SchedulerDriver::new(RequestScheduler::new(config()), sink);
        add_task(&mut driver, task_gid);
        activate_task(&mut driver, task_gid);
        control.push(TestAdmission::Fail(SessionOwnerError::Persistence(
            SessionPersistenceError::MissingJournal { gid: task_gid },
        )));
        driver
            .prepare_sink(PersistenceSchedulerPreparation::Persistence(Box::new(
                stage_plan(task_gid, patch_id),
            )))
            .expect("register stage plan");
        driver
            .execute_command(SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            })
            .expect("stage option patch");
        finish_chain(&mut driver);

        assert_eq!(driver.fault(), None);
        assert!(driver.is_idle());
        assert!(matches!(
            control.submitted().last(),
            Some(TestCommandSnapshot::AppendJournal { .. })
        ));
    }

    #[test]
    fn stage_owner_uncertainty_and_unexpected_results_are_unrepresentable() {
        assert_stage_admissions_fault(vec![TestAdmission::Fail(SessionOwnerError::Unavailable)]);
        assert_stage_admissions_fault(vec![TestAdmission::Fail(
            SessionOwnerError::ShutdownTimedOut {
                timeout: Duration::from_millis(1),
            },
        )]);
        assert_stage_admissions_fault(vec![TestAdmission::Complete(SessionCommandResult::Unit)]);
        assert_stage_admissions_fault(vec![TestAdmission::Fail(SessionOwnerError::Persistence(
            SessionPersistenceError::Store(SessionStoreError::NotFound),
        ))]);
        assert_stage_admissions_fault(vec![TestAdmission::Fail(SessionOwnerError::Persistence(
            SessionPersistenceError::Journal {
                gid: gid(1),
                error: JournalAppenderError::Io {
                    operation: JournalIoOperation::WriteRecord,
                    kind: std::io::ErrorKind::WriteZero,
                },
            },
        ))]);
        assert_stage_admissions_fault(vec![TestAdmission::Fail(SessionOwnerError::Persistence(
            SessionPersistenceError::Journal {
                gid: gid(1),
                error: JournalAppenderError::Faulted(JournalAppenderFault::WriteRecord),
            },
        ))]);

        let (appended, _) = journal_evidence(gid(1));
        assert_stage_admissions_fault(vec![
            TestAdmission::Complete(SessionCommandResult::JournalAppended(appended)),
            TestAdmission::Complete(SessionCommandResult::Unit),
        ]);
    }

    #[test]
    fn accepted_sqlite_effect_failures_are_always_unrepresentable() {
        let task_gid = gid(1);
        let key = b"presented host key".to_vec();
        let host_resolution =
            active_persistence(TransitionEffect::PersistHostKeyPinAndClearChallenge {
                task_id: task_id(1),
                gid: task_gid,
                resolution_id: HostKeyResolutionId::new(1).expect("resolution id"),
                challenge: HostKeyChallengeId::new([1; 16]),
                fingerprint_sha256: HostKeyFingerprint::for_presented_key(&key),
                presented_public_key: key,
                option_patch: None,
            });
        let stopped_deletion = active_persistence(TransitionEffect::DeleteStoppedTaskMetadata {
            task_id: task_id(1),
            gid: task_gid,
            deletion_id: StoppedResultDeletionId::new(1).expect("deletion id"),
            remaining_order: Vec::new(),
        });
        let unavailable = SessionOwnerError::Unavailable;
        let ambiguous_store = SessionOwnerError::Persistence(SessionPersistenceError::Store(
            SessionStoreError::NotFound,
        ));
        let invalid_capacity = SessionOwnerError::InvalidRequestCapacity {
            requested: 65,
            maximum: 64,
        };
        let expected = EffectCompletion::UnrepresentableFailure {
            dispatch_id: EffectDispatchId::new(1).expect("dispatch id"),
        };

        for active in [&host_resolution, &stopped_deletion] {
            assert_eq!(
                owner_command_failure(active, ExpectedResult::Unit, &unavailable),
                expected
            );
            assert_eq!(
                owner_command_failure(active, ExpectedResult::Unit, &ambiguous_store),
                expected
            );
            assert_eq!(
                owner_command_failure(active, ExpectedResult::Unit, &invalid_capacity),
                expected
            );
            assert_eq!(post_acceptance_admission_failure(active), expected);
        }
    }

    #[test]
    fn stage_flush_failure_after_append_evidence_is_unrepresentable() {
        let task_gid = gid(1);
        let patch_id = OptionPatchId::new(4).expect("patch id");
        let (appended, flushed) = journal_evidence(task_gid);
        let (session, control) = TestSessionEndpoint::new([
            TestAdmission::Complete(SessionCommandResult::Unit),
            TestAdmission::Complete(SessionCommandResult::JournalFlushed(flushed)),
            TestAdmission::Complete(SessionCommandResult::Unit),
        ]);
        let (delegate, _) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog([persist_task_plan(task_gid)]),
        );
        let mut driver = SchedulerDriver::new(RequestScheduler::new(config()), sink);
        add_task(&mut driver, task_gid);
        activate_task(&mut driver, task_gid);
        control.push(TestAdmission::Complete(
            SessionCommandResult::JournalAppended(appended),
        ));
        control.push(TestAdmission::Fail(SessionOwnerError::Persistence(
            SessionPersistenceError::MissingJournal { gid: task_gid },
        )));
        driver
            .prepare_sink(PersistenceSchedulerPreparation::Persistence(Box::new(
                stage_plan(task_gid, patch_id),
            )))
            .expect("register stage plan");
        driver
            .execute_command(SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            })
            .expect("stage option patch");

        expect_unrepresentable_fault(&mut driver);
    }

    #[test]
    fn stage_sqlite_failure_after_durable_journal_is_unrepresentable() {
        let task_gid = gid(1);
        let patch_id = OptionPatchId::new(4).expect("patch id");
        let (appended, flushed) = journal_evidence(task_gid);
        let (session, control) = TestSessionEndpoint::new([
            TestAdmission::Complete(SessionCommandResult::Unit),
            TestAdmission::Complete(SessionCommandResult::JournalFlushed(flushed)),
            TestAdmission::Complete(SessionCommandResult::Unit),
        ]);
        let (delegate, _) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog([persist_task_plan(task_gid)]),
        );
        let mut driver = SchedulerDriver::new(RequestScheduler::new(config()), sink);
        add_task(&mut driver, task_gid);
        activate_task(&mut driver, task_gid);
        control.push(TestAdmission::Complete(
            SessionCommandResult::JournalAppended(appended),
        ));
        control.push(TestAdmission::Complete(
            SessionCommandResult::JournalFlushed(flushed),
        ));
        control.push(TestAdmission::Fail(SessionOwnerError::Persistence(
            SessionPersistenceError::Store(SessionStoreError::NotFound),
        )));
        driver
            .prepare_sink(PersistenceSchedulerPreparation::Persistence(Box::new(
                stage_plan(task_gid, patch_id),
            )))
            .expect("register stage plan");
        driver
            .execute_command(SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            })
            .expect("stage option patch");

        expect_unrepresentable_fault(&mut driver);
    }

    #[test]
    fn non_persistence_restore_effect_is_delegated_unchanged() {
        let task_gid = gid(1);
        let retry_at = MonotonicInstant::now()
            .checked_add(Duration::from_secs(1))
            .expect("retry deadline");
        let batch = SchedulerRestoreBatch::new(
            vec![RecoveredSchedulerTask {
                task_id: task_id(1),
                gid: task_gid,
                state: TaskState::RetryWait,
                generation: Generation::new(3),
                generation_started: true,
                desired_paused: false,
                conditions: TaskConditions::default(),
                slow_demotion_count: 0,
                slow_slot: None,
                retry_at: Some(retry_at),
                host_key_challenge: None,
                error: None,
                stopped_status: None,
            }],
            [
                QueueClass::Waiting,
                QueueClass::Demoted,
                QueueClass::Paused,
                QueueClass::Active,
                QueueClass::Stopped,
            ]
            .into_iter()
            .map(|class| QueueOrder {
                class,
                order: if class == QueueClass::Waiting {
                    vec![task_gid]
                } else {
                    Vec::new()
                },
            })
            .collect(),
        );
        let (scheduler, restore) = RequestScheduler::restore(config(), batch).expect("restore");
        let (session, _) = TestSessionEndpoint::new([]);
        let (delegate, kinds) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog(std::iter::empty()),
        );
        let mut driver = SchedulerDriver::new(scheduler, sink);
        driver.begin_restore(restore).expect("begin restore");
        finish_chain(&mut driver);

        assert_eq!(
            *kinds
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![TransitionEffectKind::ScheduleRetry]
        );
        assert_eq!(driver.fault(), None);
    }

    #[test]
    fn missing_plan_becomes_an_unrepresentable_driver_fault() {
        let task_gid = gid(1);
        let (session, _) = TestSessionEndpoint::new([]);
        let (delegate, _) = RecordingDelegate::new();
        let sink = PersistenceSchedulerEffectSink::new_for_test(
            session,
            delegate,
            catalog(std::iter::empty()),
        );
        let mut driver = SchedulerDriver::new(RequestScheduler::new(config()), sink);
        driver
            .execute_command(SchedulerCommand::AddValidatedTask {
                task_id: task_id(1),
                gid: task_gid,
                desired_paused: false,
                conditions: TaskConditions::default(),
            })
            .expect("start add task");
        assert!(matches!(
            driver.poll(),
            SchedulerDriverPoll::WaitingForCompletion { .. }
        ));
        assert!(matches!(
            driver.poll(),
            SchedulerDriverPoll::Faulted(SchedulerDriverFault::UnrepresentableEffectFailure { .. })
        ));
    }

    #[test]
    fn generation_started_plan_accepts_only_initial_flush_or_exact_advance() {
        let task_gid = gid(1);
        let initial = TransitionEffect::PersistGenerationStarted {
            task_id: task_id(1),
            gid: task_gid,
            generation: Generation::INITIAL,
        };
        assert!(
            PersistenceEffectPlan::new(
                initial,
                vec![PersistencePlanStep::FlushJournal {
                    gid: task_gid,
                    through_sequence: 1,
                }],
            )
            .is_ok()
        );

        let next = Generation::new(1);
        assert_eq!(
            PersistenceEffectPlan::new(
                TransitionEffect::PersistGenerationStarted {
                    task_id: task_id(1),
                    gid: task_gid,
                    generation: next,
                },
                vec![PersistencePlanStep::AppendAndFlushJournal {
                    gid: task_gid,
                    generation: next,
                    payload: JournalPayload::GenerationStarted {
                        previous_generation: Generation::INITIAL,
                        reason: GenerationStartReason::OptionPatch,
                        next_snapshot_hash: hash(7),
                        patch_id: Some(OptionPatchId::new(8).expect("patch id")),
                    },
                }],
            ),
            Err(PersistencePlanError::StateMismatch)
        );

        let options =
            SanitizedOptionMap::new([("out".to_owned(), "file.bin".to_owned())]).expect("options");
        let snapshot_hash = options.snapshot_hash();
        let effect = TransitionEffect::PersistGenerationStarted {
            task_id: task_id(1),
            gid: task_gid,
            generation: next,
        };
        let promotion = vec![
            PersistencePlanStep::AppendAndFlushJournal {
                gid: task_gid,
                generation: next,
                payload: JournalPayload::GenerationStarted {
                    previous_generation: Generation::INITIAL,
                    reason: GenerationStartReason::OptionPatch,
                    next_snapshot_hash: snapshot_hash,
                    patch_id: Some(OptionPatchId::new(8).expect("patch")),
                },
            },
            PersistencePlanStep::PromoteTaskOptions {
                gid: task_gid,
                options: options.clone(),
            },
        ];
        assert!(PersistenceEffectPlan::new(effect.clone(), promotion.clone()).is_ok());
        let mut mismatched = promotion.clone();
        if let PersistencePlanStep::PromoteTaskOptions { options, .. } = &mut mismatched[1] {
            *options = SanitizedOptionMap::new([("out".to_owned(), "wrong.bin".to_owned())])
                .expect("different snapshot");
        }
        assert_eq!(
            PersistenceEffectPlan::new(effect.clone(), mismatched),
            Err(PersistencePlanError::StateMismatch)
        );
        let mut mismatched = promotion.clone();
        if let PersistencePlanStep::PromoteTaskOptions { gid, .. } = &mut mismatched[1] {
            *gid = self::gid(2);
        }
        assert_eq!(
            PersistenceEffectPlan::new(effect.clone(), mismatched),
            Err(PersistencePlanError::IdentityMismatch)
        );
        let mut reversed = promotion;
        reversed.reverse();
        assert!(PersistenceEffectPlan::new(effect, reversed).is_err());
        assert!(
            PersistenceEffectPlan::new(
                TransitionEffect::PersistGenerationStarted {
                    task_id: task_id(1),
                    gid: task_gid,
                    generation: next,
                },
                vec![
                    PersistencePlanStep::AppendAndFlushJournal {
                        gid: task_gid,
                        generation: Generation::INITIAL,
                        payload: JournalPayload::OptionsSnapshot {
                            scope: OptionsSnapshotScope::NextAdmission,
                            patch_id: None,
                            snapshot_hash,
                            options: options.clone(),
                        },
                    },
                    PersistencePlanStep::AppendAndFlushJournal {
                        gid: task_gid,
                        generation: next,
                        payload: JournalPayload::GenerationStarted {
                            previous_generation: Generation::INITIAL,
                            reason: GenerationStartReason::RetryReadmission,
                            next_snapshot_hash: snapshot_hash,
                            patch_id: None,
                        },
                    },
                ],
            )
            .is_ok()
        );
        assert!(
            PersistenceEffectPlan::new(
                TransitionEffect::PersistGenerationStarted {
                    task_id: task_id(1),
                    gid: task_gid,
                    generation: next,
                },
                vec![
                    PersistencePlanStep::AppendAndFlushJournal {
                        gid: task_gid,
                        generation: Generation::INITIAL,
                        payload: JournalPayload::TaskPaused {
                            reason: TaskPauseReason::Restarting,
                        },
                    },
                    PersistencePlanStep::AppendAndFlushJournal {
                        gid: task_gid,
                        generation: Generation::INITIAL,
                        payload: JournalPayload::OptionsSnapshot {
                            scope: OptionsSnapshotScope::NextAdmission,
                            patch_id: None,
                            snapshot_hash,
                            options: options.clone(),
                        },
                    },
                    PersistencePlanStep::AppendAndFlushJournal {
                        gid: task_gid,
                        generation: next,
                        payload: JournalPayload::GenerationStarted {
                            previous_generation: Generation::INITIAL,
                            reason: GenerationStartReason::RepresentationRestart,
                            next_snapshot_hash: snapshot_hash,
                            patch_id: None,
                        },
                    },
                ],
            )
            .is_ok()
        );
        assert!(
            PersistenceEffectPlan::new(
                TransitionEffect::PersistGenerationStarted {
                    task_id: task_id(1),
                    gid: task_gid,
                    generation: next,
                },
                vec![PersistencePlanStep::AppendAndFlushJournal {
                    gid: task_gid,
                    generation: next,
                    payload: JournalPayload::GenerationStarted {
                        previous_generation: Generation::INITIAL,
                        reason: GenerationStartReason::RepresentationRestart,
                        next_snapshot_hash: snapshot_hash,
                        patch_id: None,
                    },
                }],
            )
            .is_ok()
        );
        assert_eq!(
            PersistenceEffectPlan::new(
                TransitionEffect::PersistGenerationStarted {
                    task_id: task_id(1),
                    gid: task_gid,
                    generation: next,
                },
                vec![PersistencePlanStep::AppendAndFlushJournal {
                    gid: task_gid,
                    generation: next,
                    payload: JournalPayload::GenerationStarted {
                        previous_generation: Generation::INITIAL,
                        reason: GenerationStartReason::RetryReadmission,
                        next_snapshot_hash: snapshot_hash,
                        patch_id: None,
                    },
                }],
            ),
            Err(PersistencePlanError::StateMismatch)
        );
    }
}
