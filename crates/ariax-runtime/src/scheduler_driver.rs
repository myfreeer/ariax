use crate::status_snapshot::{
    StatusPublication, StatusSnapshotDraft, StatusSnapshotError, StatusSnapshotReader,
    StatusSnapshotStore,
};
use ariax_core::{
    Generation, Gid, MonotonicInstant, QueueClass, RequestScheduler, SchedulerCommand,
    SchedulerError, SchedulerOutcome, SchedulerRestorePlan, TaskEventEnvelope, TaskEventKind,
    TaskId, TransitionEffect, TransitionEffectIdentity, TransitionEffectKind,
};
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;

static NEXT_EFFECT_DISPATCH_ID: AtomicU64 = AtomicU64::new(1);

/// Sequence number assigned from the driver's process-unique allocator when an
/// ordered scheduler effect is first offered to its adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectDispatchId(NonZeroU64);

impl EffectDispatchId {
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// One effect with the exact task generation and dispatch sequence expected by
/// its completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchedEffect {
    dispatch_id: EffectDispatchId,
    task_generation: Generation,
    effect: TransitionEffect,
}

impl DispatchedEffect {
    #[must_use]
    pub const fn dispatch_id(&self) -> EffectDispatchId {
        self.dispatch_id
    }

    #[must_use]
    pub const fn task_generation(&self) -> Generation {
        self.task_generation
    }

    #[must_use]
    pub const fn effect(&self) -> &TransitionEffect {
        &self.effect
    }

    #[must_use]
    pub const fn identity(&self) -> TransitionEffectIdentity {
        self.effect.identity()
    }
}

/// Admission failure after an effect was offered to a concrete adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectSinkError {
    Closed,
    Failed,
}

/// Nonblocking adapter contract used by the scheduler owner.
pub trait SchedulerEffectSink {
    /// `Pending` means bounded admission is full and the exact borrowed effect
    /// must be retried later. `Ready(Ok(()))` accepts ownership logically; the
    /// sink must subsequently return exactly one matching completion.
    fn poll_dispatch(&mut self, effect: &DispatchedEffect) -> Poll<Result<(), EffectSinkError>>;

    /// Returns the next completion for an already accepted effect.
    fn poll_completion(&mut self) -> Poll<EffectCompletion>;
}

/// One sink-defined, non-dispatch preparation accepted only while its driver is
/// unfaulted and idle.
///
/// Implementations must use this hook only for bounded configuration required
/// before the next scheduler input. Effect admission and completion remain
/// exclusive to [`SchedulerEffectSink`].
pub trait SchedulerEffectSinkPrepare: SchedulerEffectSink {
    type Preparation;
    type Error;

    fn prepare(&mut self, preparation: Self::Preparation) -> Result<(), Self::Error>;
}

/// Exactly one represented result for an accepted sink effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EffectCompletion {
    Completed {
        dispatch_id: EffectDispatchId,
        acknowledgement: Option<TaskEventEnvelope>,
    },
    UnrepresentableFailure {
        dispatch_id: EffectDispatchId,
    },
}

impl EffectCompletion {
    #[must_use]
    pub const fn dispatch_id(&self) -> EffectDispatchId {
        match self {
            Self::Completed { dispatch_id, .. } | Self::UnrepresentableFailure { dispatch_id } => {
                *dispatch_id
            }
        }
    }
}

/// Why no further scheduler mutation is safe in this driver instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerDriverFault {
    DispatchIdExhausted,
    EffectTaskMissing {
        identity: TransitionEffectIdentity,
    },
    EffectTaskIdentityMismatch {
        identity: TransitionEffectIdentity,
        actual_task_id: TaskId,
    },
    EffectGenerationMismatch {
        identity: TransitionEffectIdentity,
        actual_generation: Generation,
    },
    SinkClosed {
        dispatch_id: EffectDispatchId,
        identity: TransitionEffectIdentity,
    },
    SinkFailed {
        dispatch_id: EffectDispatchId,
        identity: TransitionEffectIdentity,
    },
    UnrepresentableEffectFailure {
        dispatch_id: EffectDispatchId,
        identity: TransitionEffectIdentity,
    },
    CompletionOutOfOrder {
        expected: EffectDispatchId,
        actual: EffectDispatchId,
    },
    MissingAcknowledgement {
        dispatch_id: EffectDispatchId,
        identity: TransitionEffectIdentity,
    },
    UnexpectedAcknowledgement {
        dispatch_id: EffectDispatchId,
        identity: TransitionEffectIdentity,
        event_kind: TaskEventKind,
    },
    MismatchedAcknowledgement {
        dispatch_id: EffectDispatchId,
        identity: TransitionEffectIdentity,
        event_task_id: TaskId,
        event_gid: Gid,
        event_kind: TaskEventKind,
    },
    Scheduler(SchedulerError),
    Snapshot(StatusSnapshotError),
    InternalInvariant,
}

/// Admission failure for an external scheduler input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerDriverInputError {
    Busy,
    Faulted(SchedulerDriverFault),
    RestorePlanMismatch,
    Scheduler(SchedulerError),
    Snapshot(StatusSnapshotError),
}

/// Failure to prepare a sink at the driver's external-input boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerDriverPrepareError<E> {
    Busy,
    Faulted(SchedulerDriverFault),
    Rejected(E),
}

/// One bounded unit of driver progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerDriverPoll {
    Idle,
    Progressed,
    Backpressured { dispatch_id: EffectDispatchId },
    WaitingForCompletion { dispatch_id: EffectDispatchId },
    Completed { revision: u64, published: bool },
    Faulted(SchedulerDriverFault),
}

#[derive(Debug)]
struct PendingOutcome {
    outcome: SchedulerOutcome,
    next_effect: usize,
}

/// Single mutable owner of scheduler state, ordered effect dispatch, and
/// applied public status publication.
///
/// The sink has no public mutable accessor. Host composition may expose only a
/// typed preparation via [`SchedulerEffectSinkPrepare`] and
/// [`Self::prepare_sink`].
///
/// ```compile_fail
/// use ariax_runtime::{SchedulerDriver, SchedulerEffectSink};
///
/// fn bypass_ordered_dispatch<S: SchedulerEffectSink>(driver: &mut SchedulerDriver<S>) {
///     let _ = driver.sink_mut();
/// }
/// ```
#[derive(Debug)]
pub struct SchedulerDriver<S> {
    scheduler: RequestScheduler,
    sink: S,
    snapshots: StatusSnapshotStore,
    pending: Option<PendingOutcome>,
    offered: Option<DispatchedEffect>,
    in_flight: Option<DispatchedEffect>,
    acknowledgements: VecDeque<TaskEventEnvelope>,
    draft: Option<StatusSnapshotDraft>,
    restore_plan: Option<SchedulerRestorePlan>,
    fault: Option<SchedulerDriverFault>,
}

impl<S: SchedulerEffectSink> SchedulerDriver<S> {
    #[must_use]
    pub fn new(scheduler: RequestScheduler, sink: S) -> Self {
        Self {
            scheduler,
            sink,
            snapshots: StatusSnapshotStore::new(),
            pending: None,
            offered: None,
            in_flight: None,
            acknowledgements: VecDeque::new(),
            draft: None,
            restore_plan: None,
            fault: None,
        }
    }

    #[must_use]
    pub fn scheduler(&self) -> &RequestScheduler {
        &self.scheduler
    }

    pub fn configure_queue_policies(
        &mut self,
        retry_wait_holds_slot: bool,
        slow_readmission_policy: ariax_core::SlowReadmissionPolicy,
    ) -> Result<(), SchedulerDriverInputError> {
        self.ensure_input_ready()?;
        self.scheduler
            .configure_queue_policies(retry_wait_holds_slot, slow_readmission_policy);
        Ok(())
    }

    /// Stops mutations when a caller cannot publish an already durable control
    /// operation. Existing snapshots remain readable; recovery owns resolution.
    pub fn fail_control_publication(&mut self) {
        self.fail(SchedulerDriverFault::InternalInvariant);
    }

    #[must_use]
    pub const fn sink(&self) -> &S {
        &self.sink
    }

    /// Applies one bounded, sink-defined preparation before the next external
    /// scheduler input. This never exposes the sink's dispatch methods and is
    /// rejected while any outcome, effect, acknowledgement, restore, or
    /// publication is pending.
    pub fn prepare_sink(
        &mut self,
        preparation: S::Preparation,
    ) -> Result<(), SchedulerDriverPrepareError<S::Error>>
    where
        S: SchedulerEffectSinkPrepare,
    {
        if let Some(fault) = self.fault {
            return Err(SchedulerDriverPrepareError::Faulted(fault));
        }
        if !self.is_idle() {
            return Err(SchedulerDriverPrepareError::Busy);
        }
        self.sink
            .prepare(preparation)
            .map_err(SchedulerDriverPrepareError::Rejected)
    }

    #[must_use]
    pub fn snapshot_reader(&self) -> StatusSnapshotReader {
        self.snapshots.reader()
    }

    #[must_use]
    pub const fn fault(&self) -> Option<SchedulerDriverFault> {
        self.fault
    }

    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.fault.is_none()
            && self.pending.is_none()
            && self.offered.is_none()
            && self.in_flight.is_none()
            && self.acknowledgements.is_empty()
            && self.draft.is_none()
            && self.restore_plan.is_none()
    }

    pub fn execute_command(
        &mut self,
        command: SchedulerCommand,
    ) -> Result<(), SchedulerDriverInputError> {
        self.execute_command_at(command, MonotonicInstant::now())
    }

    pub fn execute_command_at(
        &mut self,
        command: SchedulerCommand,
        at: MonotonicInstant,
    ) -> Result<(), SchedulerDriverInputError> {
        self.ensure_input_ready()?;
        let outcome = self
            .scheduler
            .execute_command_at(command, at)
            .map_err(SchedulerDriverInputError::Scheduler)?;
        self.begin_chain(outcome);
        Ok(())
    }

    pub fn handle_event(
        &mut self,
        event: &TaskEventEnvelope,
    ) -> Result<(), SchedulerDriverInputError> {
        self.handle_event_at(event, MonotonicInstant::now())
    }

    pub fn handle_event_at(
        &mut self,
        event: &TaskEventEnvelope,
        at: MonotonicInstant,
    ) -> Result<(), SchedulerDriverInputError> {
        self.ensure_input_ready()?;
        let outcome = self
            .scheduler
            .handle_event_at(event, at)
            .map_err(SchedulerDriverInputError::Scheduler)?;
        self.begin_chain(outcome);
        Ok(())
    }

    pub fn admit_next(&mut self) -> Result<(), SchedulerDriverInputError> {
        self.admit_next_at(MonotonicInstant::now())
    }

    pub fn admit_next_at(&mut self, at: MonotonicInstant) -> Result<(), SchedulerDriverInputError> {
        self.ensure_input_ready()?;
        let outcome = self
            .scheduler
            .admit_next_at(at)
            .map_err(SchedulerDriverInputError::Scheduler)?;
        self.begin_chain(outcome);
        Ok(())
    }

    /// Dispatches all bounded startup-recovery batches into one unpublished
    /// applied root. The driver and snapshot handle must remain private until
    /// this chain completes.
    pub fn begin_restore(
        &mut self,
        mut plan: SchedulerRestorePlan,
    ) -> Result<(), SchedulerDriverInputError> {
        self.ensure_input_ready()?;
        if !plan.is_bound_to(&self.scheduler) {
            return Err(SchedulerDriverInputError::RestorePlanMismatch);
        }
        let draft = self
            .snapshots
            .recovered_draft(&self.scheduler)
            .map_err(SchedulerDriverInputError::Snapshot)?;
        let first = SchedulerOutcome::checked(None, None, plan.next_batch())
            .map_err(SchedulerDriverInputError::Scheduler)?;
        self.draft = Some(draft);
        self.restore_plan = Some(plan);
        self.pending = Some(PendingOutcome {
            outcome: first,
            next_effect: 0,
        });
        Ok(())
    }

    /// Advances at most one local effect, sink admission, sink completion, or
    /// acknowledgement outcome.
    pub fn poll(&mut self) -> SchedulerDriverPoll {
        self.poll_at(MonotonicInstant::now())
    }

    /// Deterministic variant of [`Self::poll`] used by replay and tests.
    pub fn poll_at(&mut self, at: MonotonicInstant) -> SchedulerDriverPoll {
        if let Some(fault) = self.fault {
            return SchedulerDriverPoll::Faulted(fault);
        }

        if self.in_flight.is_some() {
            return self.poll_in_flight();
        }
        if self.offered.is_some() {
            return self.poll_offered();
        }
        if self.pending.is_some() {
            return self.poll_pending(at);
        }
        if let Some(acknowledgement) = self.acknowledgements.pop_front() {
            return self.apply_acknowledgement(acknowledgement, at);
        }
        if let Some(draft) = self.draft.take() {
            return match self.snapshots.publish(draft) {
                Ok(StatusPublication { revision, changed }) => SchedulerDriverPoll::Completed {
                    revision,
                    published: changed,
                },
                Err(error) => self.fail(SchedulerDriverFault::Snapshot(error)),
            };
        }
        SchedulerDriverPoll::Idle
    }

    fn ensure_input_ready(&self) -> Result<(), SchedulerDriverInputError> {
        if let Some(fault) = self.fault {
            return Err(SchedulerDriverInputError::Faulted(fault));
        }
        if !self.is_idle() {
            return Err(SchedulerDriverInputError::Busy);
        }
        Ok(())
    }

    fn begin_chain(&mut self, outcome: SchedulerOutcome) {
        debug_assert!(self.draft.is_none());
        debug_assert!(self.restore_plan.is_none());
        self.draft = Some(self.snapshots.draft());
        self.pending = Some(PendingOutcome {
            outcome,
            next_effect: 0,
        });
    }

    fn poll_pending(&mut self, at: MonotonicInstant) -> SchedulerDriverPoll {
        let effect = {
            let pending = self.pending.as_ref().expect("pending outcome");
            pending.outcome.effects.get(pending.next_effect).cloned()
        };
        let Some(effect) = effect else {
            return self.finish_pending_outcome(at);
        };

        let identity = effect.identity();
        let task_generation = match self.validate_effect_owner(identity) {
            Ok(generation) => generation,
            Err(fault) => return self.fail(fault),
        };

        if let TransitionEffect::PublishSnapshot { task_id, snapshot } = effect {
            let Some(draft) = self.draft.as_mut() else {
                return self.fail(SchedulerDriverFault::InternalInvariant);
            };
            let view = self.scheduler.task(snapshot.gid);
            let result = draft.insert_task_with_view(task_id, snapshot, view);
            if let Err(error) = result {
                return self.fail(SchedulerDriverFault::Snapshot(error));
            }
            self.advance_effect_cursor();
            return SchedulerDriverPoll::Progressed;
        }

        let Some(dispatch_id) = allocate_effect_dispatch_id() else {
            return self.fail(SchedulerDriverFault::DispatchIdExhausted);
        };
        self.offered = Some(DispatchedEffect {
            dispatch_id,
            task_generation,
            effect,
        });
        self.poll_offered()
    }

    fn validate_effect_owner(
        &self,
        identity: TransitionEffectIdentity,
    ) -> Result<Generation, SchedulerDriverFault> {
        let Some(task) = self.scheduler.task(identity.gid) else {
            return Err(SchedulerDriverFault::EffectTaskMissing { identity });
        };
        if task.task_id != identity.task_id {
            return Err(SchedulerDriverFault::EffectTaskIdentityMismatch {
                identity,
                actual_task_id: task.task_id,
            });
        }
        if identity
            .generation
            .is_some_and(|generation| generation != task.generation)
        {
            return Err(SchedulerDriverFault::EffectGenerationMismatch {
                identity,
                actual_generation: task.generation,
            });
        }
        Ok(task.generation)
    }

    fn poll_offered(&mut self) -> SchedulerDriverPoll {
        let offered = self.offered.as_ref().expect("offered effect");
        match self.sink.poll_dispatch(offered) {
            Poll::Pending => SchedulerDriverPoll::Backpressured {
                dispatch_id: offered.dispatch_id,
            },
            Poll::Ready(Ok(())) => {
                let dispatched = self.offered.take().expect("accepted offered effect");
                let dispatch_id = dispatched.dispatch_id;
                self.in_flight = Some(dispatched);
                SchedulerDriverPoll::WaitingForCompletion { dispatch_id }
            }
            Poll::Ready(Err(EffectSinkError::Closed)) => {
                let dispatch_id = offered.dispatch_id;
                let identity = offered.identity();
                self.fail(SchedulerDriverFault::SinkClosed {
                    dispatch_id,
                    identity,
                })
            }
            Poll::Ready(Err(EffectSinkError::Failed)) => {
                let dispatch_id = offered.dispatch_id;
                let identity = offered.identity();
                self.fail(SchedulerDriverFault::SinkFailed {
                    dispatch_id,
                    identity,
                })
            }
        }
    }

    fn poll_in_flight(&mut self) -> SchedulerDriverPoll {
        let expected = self.in_flight.as_ref().expect("in-flight effect");
        let expected_id = expected.dispatch_id;
        let completion = match self.sink.poll_completion() {
            Poll::Pending => {
                return SchedulerDriverPoll::WaitingForCompletion {
                    dispatch_id: expected_id,
                };
            }
            Poll::Ready(completion) => completion,
        };
        if completion.dispatch_id() != expected_id {
            return self.fail(SchedulerDriverFault::CompletionOutOfOrder {
                expected: expected_id,
                actual: completion.dispatch_id(),
            });
        }
        let dispatched = self.in_flight.take().expect("matching in-flight effect");
        match completion {
            EffectCompletion::UnrepresentableFailure { dispatch_id } => {
                return self.fail(SchedulerDriverFault::UnrepresentableEffectFailure {
                    dispatch_id,
                    identity: dispatched.identity(),
                });
            }
            EffectCompletion::Completed {
                dispatch_id,
                acknowledgement,
            } => {
                if let Err(fault) = validate_acknowledgement(&dispatched, acknowledgement.as_ref())
                {
                    return self.fail(fault.with_dispatch_id(dispatch_id));
                }
                if let Err(fault) =
                    self.stage_completed_effect(&dispatched, acknowledgement.as_ref())
                {
                    return self.fail(fault);
                }
                if let Some(acknowledgement) = acknowledgement {
                    self.acknowledgements.push_back(acknowledgement);
                }
            }
        }
        self.advance_effect_cursor();
        SchedulerDriverPoll::Progressed
    }

    fn stage_completed_effect(
        &mut self,
        dispatched: &DispatchedEffect,
        acknowledgement: Option<&TaskEventEnvelope>,
    ) -> Result<(), SchedulerDriverFault> {
        let draft = self
            .draft
            .as_mut()
            .ok_or(SchedulerDriverFault::InternalInvariant)?;
        match dispatched.effect() {
            TransitionEffect::PersistTask {
                gid,
                queue,
                position,
                ..
            } => draft
                .insert_queue_member(*queue, *position, *gid)
                .map_err(SchedulerDriverFault::Snapshot),
            TransitionEffect::PersistQueueTransition { orders, .. }
            | TransitionEffect::PersistTerminal { orders, .. } => {
                for order in orders {
                    draft
                        .replace_queue(order.class, order.order.clone())
                        .map_err(SchedulerDriverFault::Snapshot)?;
                }
                Ok(())
            }
            TransitionEffect::DeleteStoppedTaskMetadata {
                remaining_order, ..
            } if acknowledgement.is_some_and(|event| {
                event.event().kind() == TaskEventKind::StoppedResultDeleted
            }) =>
            {
                draft
                    .replace_queue(QueueClass::Stopped, remaining_order.clone())
                    .map_err(SchedulerDriverFault::Snapshot)
            }
            TransitionEffect::StageOptionPatch { .. }
            | TransitionEffect::ApplyOptionPatch { .. }
            | TransitionEffect::PersistGenerationStarted { .. }
            | TransitionEffect::StartAllocation { .. }
            | TransitionEffect::CancelGeneration { .. }
            | TransitionEffect::ReleaseSlot { .. }
            | TransitionEffect::ScheduleRetry { .. }
            | TransitionEffect::CancelRetry { .. }
            | TransitionEffect::ScheduleSlowReadmission { .. }
            | TransitionEffect::CancelSlowReadmission { .. }
            | TransitionEffect::ProbeNoSpace { .. }
            | TransitionEffect::PersistConditions { .. }
            | TransitionEffect::PersistHostKeyChallenge { .. }
            | TransitionEffect::PersistHostKeyPinAndClearChallenge { .. }
            | TransitionEffect::PersistHostKeyChallengeRejected { .. }
            | TransitionEffect::DeleteStoppedTaskMetadata { .. } => Ok(()),
            TransitionEffect::PublishSnapshot { .. } => {
                Err(SchedulerDriverFault::InternalInvariant)
            }
        }
    }

    fn finish_pending_outcome(&mut self, at: MonotonicInstant) -> SchedulerDriverPoll {
        let pending = self.pending.take().expect("finished pending outcome");
        if let Some(deletion) = pending.outcome.deletion {
            let Some(draft) = self.draft.as_mut() else {
                return self.fail(SchedulerDriverFault::InternalInvariant);
            };
            draft.remove_task(deletion.task, deletion.gid);
        }
        if let Some(acknowledgement) = self.acknowledgements.pop_front() {
            return self.apply_acknowledgement(acknowledgement, at);
        }
        if let Some(plan) = self.restore_plan.as_mut() {
            let effects = plan.next_batch();
            if !effects.is_empty() {
                let outcome = match SchedulerOutcome::checked(None, None, effects) {
                    Ok(outcome) => outcome,
                    Err(error) => return self.fail(SchedulerDriverFault::Scheduler(error)),
                };
                self.pending = Some(PendingOutcome {
                    outcome,
                    next_effect: 0,
                });
                return SchedulerDriverPoll::Progressed;
            }
            self.restore_plan = None;
        }
        SchedulerDriverPoll::Progressed
    }

    fn apply_acknowledgement(
        &mut self,
        acknowledgement: TaskEventEnvelope,
        at: MonotonicInstant,
    ) -> SchedulerDriverPoll {
        match self.scheduler.handle_event_at(&acknowledgement, at) {
            Ok(outcome) => {
                self.pending = Some(PendingOutcome {
                    outcome,
                    next_effect: 0,
                });
                SchedulerDriverPoll::Progressed
            }
            Err(error) => self.fail(SchedulerDriverFault::Scheduler(error)),
        }
    }

    fn advance_effect_cursor(&mut self) {
        let pending = self.pending.as_mut().expect("effect owns pending outcome");
        pending.next_effect += 1;
    }

    fn fail(&mut self, fault: SchedulerDriverFault) -> SchedulerDriverPoll {
        self.fault = Some(fault);
        SchedulerDriverPoll::Faulted(fault)
    }
}

fn allocate_effect_dispatch_id() -> Option<EffectDispatchId> {
    let mut current = NEXT_EFFECT_DISPATCH_ID.load(Ordering::Relaxed);
    loop {
        let dispatch_id = EffectDispatchId::new(current)?;
        let next = current.checked_add(1).unwrap_or(0);
        match NEXT_EFFECT_DISPATCH_ID.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Some(dispatch_id),
            Err(actual) => current = actual,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcknowledgementFault {
    Missing {
        identity: TransitionEffectIdentity,
    },
    Unexpected {
        identity: TransitionEffectIdentity,
        event_kind: TaskEventKind,
    },
    Mismatched {
        identity: TransitionEffectIdentity,
        event_task_id: TaskId,
        event_gid: Gid,
        event_kind: TaskEventKind,
    },
}

impl AcknowledgementFault {
    const fn with_dispatch_id(self, dispatch_id: EffectDispatchId) -> SchedulerDriverFault {
        match self {
            Self::Missing { identity } => SchedulerDriverFault::MissingAcknowledgement {
                dispatch_id,
                identity,
            },
            Self::Unexpected {
                identity,
                event_kind,
            } => SchedulerDriverFault::UnexpectedAcknowledgement {
                dispatch_id,
                identity,
                event_kind,
            },
            Self::Mismatched {
                identity,
                event_task_id,
                event_gid,
                event_kind,
            } => SchedulerDriverFault::MismatchedAcknowledgement {
                dispatch_id,
                identity,
                event_task_id,
                event_gid,
                event_kind,
            },
        }
    }
}

fn validate_acknowledgement(
    dispatched: &DispatchedEffect,
    acknowledgement: Option<&TaskEventEnvelope>,
) -> Result<(), AcknowledgementFault> {
    let identity = dispatched.identity();
    let required = acknowledgement_kinds(identity.kind);
    let Some(acknowledgement) = acknowledgement else {
        return if required.is_empty() {
            Ok(())
        } else {
            Err(AcknowledgementFault::Missing { identity })
        };
    };
    let event = acknowledgement.event();
    let kind = event.kind();
    if required.is_empty() {
        return Err(AcknowledgementFault::Unexpected {
            identity,
            event_kind: kind,
        });
    }
    let owner_matches = acknowledgement.task_id() == identity.task_id
        && event.gid() == identity.gid
        && event.generation() == dispatched.task_generation;
    let token_matches = identity.token.is_none() || event.token() == identity.token;
    let terminal_status_matches = match (dispatched.effect(), event) {
        (
            TransitionEffect::PersistTerminal { status, .. },
            ariax_core::TaskEvent::TerminalPersisted {
                status: acknowledged,
                ..
            },
        ) => status == acknowledged,
        (TransitionEffect::PersistTerminal { .. }, _) => false,
        _ => true,
    };
    if !required.contains(&kind) || !owner_matches || !token_matches || !terminal_status_matches {
        return Err(AcknowledgementFault::Mismatched {
            identity,
            event_task_id: acknowledgement.task_id(),
            event_gid: event.gid(),
            event_kind: kind,
        });
    }
    Ok(())
}

fn acknowledgement_kinds(kind: TransitionEffectKind) -> &'static [TaskEventKind] {
    match kind {
        TransitionEffectKind::StageOptionPatch => &[
            TaskEventKind::OptionPatchPersisted,
            TaskEventKind::OptionPatchPersistenceFailed,
        ],
        TransitionEffectKind::ApplyOptionPatch => &[
            TaskEventKind::OptionPatchApplied,
            TaskEventKind::OptionPatchApplicationFailed,
        ],
        TransitionEffectKind::PersistGenerationStarted => &[TaskEventKind::GenerationPersisted],
        TransitionEffectKind::PersistHostKeyPinAndClearChallenge => &[
            TaskEventKind::HostKeyResolutionPersisted,
            TaskEventKind::HostKeyResolutionFailed,
        ],
        TransitionEffectKind::PersistTerminal => &[TaskEventKind::TerminalPersisted],
        TransitionEffectKind::DeleteStoppedTaskMetadata => &[
            TaskEventKind::StoppedResultDeleted,
            TaskEventKind::StoppedResultDeletionFailed,
        ],
        TransitionEffectKind::PersistTask
        | TransitionEffectKind::PersistQueueTransition
        | TransitionEffectKind::StartAllocation
        | TransitionEffectKind::CancelGeneration
        | TransitionEffectKind::ReleaseSlot
        | TransitionEffectKind::ScheduleRetry
        | TransitionEffectKind::CancelRetry
        | TransitionEffectKind::ScheduleSlowReadmission
        | TransitionEffectKind::CancelSlowReadmission
        | TransitionEffectKind::ProbeNoSpace
        | TransitionEffectKind::PersistConditions
        | TransitionEffectKind::PersistHostKeyChallenge
        | TransitionEffectKind::PersistHostKeyChallengeRejected
        | TransitionEffectKind::PublishSnapshot => &[],
    }
}
