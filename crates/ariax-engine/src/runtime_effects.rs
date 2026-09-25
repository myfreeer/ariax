use crate::{BoundNoSpaceProbe, NoSpaceProbeTargetCatalog};
use ariax_core::{
    Generation, Gid, MonotonicInstant, NoSpaceProbeId, NoSpaceProbeOrigin,
    PresentedHostKeyChallenge, PublicError, RetryTimerId, SlowReadmissionId, TaskEvent,
    TaskEventEnvelope, TaskId, TransitionEffect,
};
use ariax_runtime::{
    DispatchedEffect, EffectCompletion, EffectSinkError, SchedulerEffectSink,
    SchedulerEffectSinkPrepare,
};
use ariax_storage::PlatformPath;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;

/// Hard ceiling for each runtime adapter queue or exact-effect catalog.
pub const MAX_RUNTIME_EFFECT_CAPACITY: usize = 1024;

/// Bounded queue sizes used by one scheduler runtime adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeEffectConfig {
    pub request_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub timer_capacity: NonZeroUsize,
    pub option_plan_capacity: NonZeroUsize,
}

impl RuntimeEffectConfig {
    pub fn validate(self) -> Result<Self, RuntimeEffectConfigError> {
        for (queue, capacity) in [
            ("request", self.request_capacity),
            ("event", self.event_capacity),
            ("timer", self.timer_capacity),
            ("option_plan", self.option_plan_capacity),
        ] {
            if capacity.get() > MAX_RUNTIME_EFFECT_CAPACITY {
                return Err(RuntimeEffectConfigError::CapacityTooLarge { queue, capacity });
            }
        }
        Ok(self)
    }
}

impl Default for RuntimeEffectConfig {
    fn default() -> Self {
        let capacity = NonZeroUsize::new(64).expect("the default runtime capacity is nonzero");
        Self {
            request_capacity: capacity,
            event_capacity: capacity,
            timer_capacity: capacity,
            option_plan_capacity: capacity,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeEffectConfigError {
    CapacityTooLarge {
        queue: &'static str,
        capacity: NonZeroUsize,
    },
}

impl fmt::Display for RuntimeEffectConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityTooLarge { queue, capacity } => write!(
                formatter,
                "runtime {queue} capacity {} exceeds hard maximum {MAX_RUNTIME_EFFECT_CAPACITY}",
                capacity.get()
            ),
        }
    }
}

impl Error for RuntimeEffectConfigError {}

/// Result that the owning configuration adapter will return for one exact
/// scheduler-issued option application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptionApplicationOutcome {
    Applied,
    Failed(PublicError),
}

/// Bounded, exact-effect authority for an option application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionApplicationPlan {
    effect: TransitionEffect,
    outcome: OptionApplicationOutcome,
}

impl OptionApplicationPlan {
    pub fn new(
        effect: TransitionEffect,
        outcome: OptionApplicationOutcome,
    ) -> Result<Self, OptionApplicationPlanError> {
        if !matches!(effect, TransitionEffect::ApplyOptionPatch { .. }) {
            return Err(OptionApplicationPlanError::NotOptionApplication);
        }
        Ok(Self { effect, outcome })
    }

    #[must_use]
    pub const fn effect(&self) -> &TransitionEffect {
        &self.effect
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptionApplicationPlanError {
    NotOptionApplication,
}

impl fmt::Display for OptionApplicationPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("runtime option plan does not describe ApplyOptionPatch")
    }
}

impl Error for OptionApplicationPlanError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeEffectPreparation {
    OptionApplication(Box<OptionApplicationPlan>),
    DiscardOptionApplications,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeEffectPrepareError {
    Full,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeIdentity {
    task_id: TaskId,
    gid: Gid,
    generation: Generation,
}

#[derive(Debug)]
struct AllocationEntry {
    identity: RuntimeIdentity,
}

#[derive(Debug)]
struct CancellationEntry {
    identity: RuntimeIdentity,
    force: bool,
}

#[derive(Debug)]
struct NoSpaceProbeEntry {
    identity: RuntimeIdentity,
    probe_id: NoSpaceProbeId,
    origin: NoSpaceProbeOrigin,
    at: MonotonicInstant,
    startup: Option<BoundNoSpaceProbe>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetryTimerEntry {
    identity: RuntimeIdentity,
    timer_id: RetryTimerId,
    at: MonotonicInstant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SlowTimerEntry {
    identity: RuntimeIdentity,
    readmission_id: SlowReadmissionId,
    at: MonotonicInstant,
}

struct RuntimeMailbox {
    closed: bool,
    bt_tasks: std::collections::BTreeSet<TaskId>,
    request_capacity: usize,
    event_capacity: usize,
    timer_capacity: usize,
    allocations: VecDeque<AllocationEntry>,
    cancellations: VecDeque<CancellationEntry>,
    probes: VecDeque<NoSpaceProbeEntry>,
    events: VecDeque<TaskEventEnvelope>,
    retry_timers: Vec<RetryTimerEntry>,
    slow_timers: Vec<SlowTimerEntry>,
}

impl RuntimeMailbox {
    fn request_count(&self) -> usize {
        self.allocations.len() + self.cancellations.len() + self.probes.len()
    }

    fn timer_count(&self) -> usize {
        self.retry_timers.len() + self.slow_timers.len()
    }
}

/// Cloneable worker-side access to bounded requests and scheduler events.
#[derive(Clone)]
pub struct RuntimeEffectHandle {
    mailbox: Arc<Mutex<RuntimeMailbox>>,
}

impl RuntimeEffectHandle {
    #[cfg(feature = "bt")]
    pub(crate) fn register_bt_task(&self, task: TaskId) {
        lock_unpoisoned(&self.mailbox).bt_tasks.insert(task);
    }

    #[cfg(feature = "bt")]
    pub(crate) fn unregister_bt_task(&self, task: TaskId) {
        lock_unpoisoned(&self.mailbox).bt_tasks.remove(&task);
    }

    #[cfg(feature = "bt")]
    /// Leave requests for other protocol owners in their original queue order.
    pub(crate) fn take_allocation_matching(
        &self,
        owns: impl Fn(TaskId) -> bool,
    ) -> Option<AllocationRequest> {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        let index = mailbox
            .allocations
            .iter()
            .position(|entry| owns(entry.identity.task_id))?;
        let entry = mailbox.allocations.remove(index)?;
        Some(AllocationRequest {
            identity: entry.identity,
        })
    }

    #[cfg(feature = "bt")]
    pub(crate) fn take_cancellation_matching(
        &self,
        owns: impl Fn(TaskId) -> bool,
    ) -> Option<CancellationRequest> {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        let index = mailbox
            .cancellations
            .iter()
            .position(|entry| owns(entry.identity.task_id))?;
        let entry = mailbox.cancellations.remove(index)?;
        Some(CancellationRequest {
            identity: entry.identity,
            force: entry.force,
        })
    }

    pub fn take_allocation(&self) -> Option<AllocationRequest> {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        let index = mailbox
            .allocations
            .iter()
            .position(|entry| !mailbox.bt_tasks.contains(&entry.identity.task_id))?;
        let entry = mailbox.allocations.remove(index)?;
        Some(AllocationRequest {
            identity: entry.identity,
        })
    }

    #[cfg(test)]
    pub(crate) fn enqueue_allocation_for_test(
        &self,
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
    ) {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        assert!(mailbox.request_count() < mailbox.request_capacity);
        mailbox.allocations.push_back(AllocationEntry {
            identity: RuntimeIdentity {
                task_id,
                gid,
                generation,
            },
        });
    }

    #[cfg(test)]
    pub(crate) fn enqueue_cancellation_for_test(
        &self,
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
        force: bool,
    ) {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        assert!(mailbox.request_count() < mailbox.request_capacity);
        mailbox.cancellations.push_back(CancellationEntry {
            identity: RuntimeIdentity {
                task_id,
                gid,
                generation,
            },
            force,
        });
    }

    pub fn take_cancellation(&self) -> Option<CancellationRequest> {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        let index = mailbox
            .cancellations
            .iter()
            .position(|entry| !mailbox.bt_tasks.contains(&entry.identity.task_id))?;
        let entry = mailbox.cancellations.remove(index)?;
        Some(CancellationRequest {
            identity: entry.identity,
            force: entry.force,
        })
    }

    pub fn take_cancellation_for(
        &self,
        task_id: TaskId,
        gid: Gid,
        generation: Generation,
    ) -> Option<CancellationRequest> {
        let identity = RuntimeIdentity {
            task_id,
            gid,
            generation,
        };
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        let index = mailbox
            .cancellations
            .iter()
            .position(|entry| entry.identity == identity)?;
        let entry = mailbox
            .cancellations
            .remove(index)
            .expect("the selected cancellation remains present");
        Some(CancellationRequest {
            identity: entry.identity,
            force: entry.force,
        })
    }

    pub fn take_no_space_probe(&self) -> Option<NoSpaceProbeRequest> {
        self.take_no_space_probe_at(MonotonicInstant::now())
    }

    pub fn take_no_space_probe_at(&self, now: MonotonicInstant) -> Option<NoSpaceProbeRequest> {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        let index = mailbox
            .probes
            .iter()
            .enumerate()
            .filter(|(_, probe)| probe.at <= now)
            .min_by_key(|(_, probe)| (probe.at, probe.identity.gid, probe.probe_id))
            .map(|(index, _)| index)?;
        let entry = mailbox
            .probes
            .remove(index)
            .expect("the selected no-space probe remains present");
        Some(NoSpaceProbeRequest {
            identity: entry.identity,
            probe_id: entry.probe_id,
            origin: entry.origin,
            at: entry.at,
            startup: entry.startup,
        })
    }

    pub fn try_submit_event(
        &self,
        submission: RuntimeEventSubmission,
    ) -> Result<(), RuntimeEventRejection> {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        if mailbox.closed {
            return Err(RuntimeEventRejection {
                submission: Box::new(submission),
                error: RuntimeEventSubmitError::Closed,
            });
        }
        if mailbox.events.len() == mailbox.event_capacity {
            return Err(RuntimeEventRejection {
                submission: Box::new(submission),
                error: RuntimeEventSubmitError::Full,
            });
        }
        mailbox.events.push_back(submission.event);
        Ok(())
    }

    /// Returns a worker result first, then the earliest due scheduler timer.
    pub fn poll_event_at(&self, now: MonotonicInstant) -> Option<TaskEventEnvelope> {
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        if let Some(event) = mailbox.events.pop_front() {
            return Some(event);
        }

        let retry = mailbox
            .retry_timers
            .iter()
            .enumerate()
            .filter(|(_, timer)| timer.at <= now)
            .min_by_key(|(_, timer)| (timer.at, timer.identity.gid, timer.timer_id))
            .map(|(index, _)| index);
        let slow = mailbox
            .slow_timers
            .iter()
            .enumerate()
            .filter(|(_, timer)| timer.at <= now)
            .min_by_key(|(_, timer)| (timer.at, timer.identity.gid, timer.readmission_id))
            .map(|(index, _)| index);

        match (retry, slow) {
            (None, None) => None,
            (Some(index), None) => Some(retry_event(mailbox.retry_timers.remove(index))),
            (None, Some(index)) => Some(slow_event(mailbox.slow_timers.remove(index))),
            (Some(retry_index), Some(slow_index)) => {
                let retry_key = {
                    let timer = mailbox.retry_timers[retry_index];
                    (timer.at, timer.identity.gid, timer.timer_id.get())
                };
                let slow_key = {
                    let timer = mailbox.slow_timers[slow_index];
                    (timer.at, timer.identity.gid, timer.readmission_id.get())
                };
                if retry_key <= slow_key {
                    Some(retry_event(mailbox.retry_timers.remove(retry_index)))
                } else {
                    Some(slow_event(mailbox.slow_timers.remove(slow_index)))
                }
            }
        }
    }

    pub fn close(&self) {
        lock_unpoisoned(&self.mailbox).closed = true;
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        lock_unpoisoned(&self.mailbox).closed
    }
}

fn retry_event(timer: RetryTimerEntry) -> TaskEventEnvelope {
    TaskEvent::RetryReady {
        gid: timer.identity.gid,
        generation: timer.identity.generation,
        retry_timer_id: timer.timer_id,
    }
    .for_task(timer.identity.task_id)
}

fn slow_event(timer: SlowTimerEntry) -> TaskEventEnvelope {
    TaskEvent::SlowReadmit {
        gid: timer.identity.gid,
        generation: timer.identity.generation,
        readmission_id: timer.readmission_id,
    }
    .for_task(timer.identity.task_id)
}

pub struct RuntimeEventSubmission {
    event: TaskEventEnvelope,
}

impl RuntimeEventSubmission {
    #[must_use]
    pub const fn event(&self) -> &TaskEventEnvelope {
        &self.event
    }
}

pub struct RuntimeEventRejection {
    submission: Box<RuntimeEventSubmission>,
    error: RuntimeEventSubmitError,
}

impl RuntimeEventRejection {
    #[must_use]
    pub const fn error(&self) -> RuntimeEventSubmitError {
        self.error
    }

    #[must_use]
    pub fn into_submission(self) -> RuntimeEventSubmission {
        *self.submission
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeEventSubmitError {
    Full,
    Closed,
}

pub struct AllocationRequest {
    identity: RuntimeIdentity,
}

impl AllocationRequest {
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.identity.task_id
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        self.identity.gid
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.identity.generation
    }

    #[must_use]
    pub fn succeeded(self) -> RuntimeEventSubmission {
        let identity = self.identity;
        self.submission(TaskEvent::AllocationSucceeded {
            gid: identity.gid,
            generation: identity.generation,
        })
    }

    #[must_use]
    pub fn activate(self) -> (RuntimeEventSubmission, ActiveTransferRequest) {
        let identity = self.identity;
        (
            RuntimeEventSubmission {
                event: TaskEvent::AllocationSucceeded {
                    gid: identity.gid,
                    generation: identity.generation,
                }
                .for_task(identity.task_id),
            },
            ActiveTransferRequest { identity },
        )
    }

    #[must_use]
    pub fn retryable(self, retry_at: MonotonicInstant) -> RuntimeEventSubmission {
        let identity = self.identity;
        self.submission(TaskEvent::AllocationRetryable {
            gid: identity.gid,
            generation: identity.generation,
            retry_at,
        })
    }

    #[must_use]
    pub fn host_key_challenge(
        self,
        challenge: PresentedHostKeyChallenge,
    ) -> RuntimeEventSubmission {
        let identity = self.identity;
        self.submission(TaskEvent::AllocationHostKeyChallenge {
            gid: identity.gid,
            generation: identity.generation,
            challenge,
        })
    }

    #[must_use]
    pub fn failed(self, error: PublicError) -> RuntimeEventSubmission {
        let identity = self.identity;
        self.submission(TaskEvent::AllocationFailed {
            gid: identity.gid,
            generation: identity.generation,
            error,
        })
    }

    fn submission(self, event: TaskEvent) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: event.for_task(self.identity.task_id),
        }
    }
}

/// Move-only authority for one scheduler-visible active transfer generation.
pub struct ActiveTransferRequest {
    identity: RuntimeIdentity,
}

impl ActiveTransferRequest {
    /// Transfers ownership to the seeding lifecycle after verified BT data completion.
    #[cfg(feature = "bt")]
    #[must_use]
    pub fn start_seeding(self) -> (RuntimeEventSubmission, SeedingRequest) {
        let identity = self.identity;
        (
            RuntimeEventSubmission {
                event: TaskEvent::DataComplete {
                    gid: identity.gid,
                    generation: identity.generation,
                    seed: true,
                }
                .for_task(identity.task_id),
            },
            SeedingRequest { identity },
        )
    }
    /// The supervisor may consume this only after the worker has joined.
    pub fn host_key_challenge(
        self,
        challenge: PresentedHostKeyChallenge,
    ) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::ActiveHostKeyChallenge {
                gid: self.identity.gid,
                generation: self.identity.generation,
                challenge,
            }
            .for_task(self.identity.task_id),
        }
    }
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.identity.task_id
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        self.identity.gid
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.identity.generation
    }

    #[must_use]
    pub fn retryable(self, retry_at: MonotonicInstant) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::ActiveRetryIdle {
                gid: self.identity.gid,
                generation: self.identity.generation,
                retry_at,
            }
            .for_task(self.identity.task_id),
        }
    }

    /// Requests a newly persisted generation after the worker has drained and
    /// determined that continuing the current representation is unsafe.
    #[must_use]
    pub fn restart_representation(self) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::ActiveRepresentationRestart {
                gid: self.identity.gid,
                generation: self.identity.generation,
            }
            .for_task(self.identity.task_id),
        }
    }

    #[must_use]
    pub fn failed(self, error: PublicError) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::TerminalFailure {
                gid: self.identity.gid,
                generation: self.identity.generation,
                error,
            }
            .for_task(self.identity.task_id),
        }
    }

    #[must_use]
    pub fn data_complete(self, seed: bool) -> (RuntimeEventSubmission, VerifyingTransferRequest) {
        let identity = self.identity;
        (
            RuntimeEventSubmission {
                event: TaskEvent::DataComplete {
                    gid: identity.gid,
                    generation: identity.generation,
                    seed,
                }
                .for_task(identity.task_id),
            },
            VerifyingTransferRequest { identity },
        )
    }
}

/// Move-only authority created only after the scheduler-visible data-complete
/// transition has been queued.
pub struct VerifyingTransferRequest {
    identity: RuntimeIdentity,
}

impl VerifyingTransferRequest {
    #[must_use]
    pub fn succeeded(self) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::VerificationSucceeded {
                gid: self.identity.gid,
                generation: self.identity.generation,
            }
            .for_task(self.identity.task_id),
        }
    }

    #[must_use]
    pub fn recoverable(self) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::VerificationRecoverable {
                gid: self.identity.gid,
                generation: self.identity.generation,
            }
            .for_task(self.identity.task_id),
        }
    }

    #[must_use]
    pub fn failed(self, error: PublicError) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::VerificationFailed {
                gid: self.identity.gid,
                generation: self.identity.generation,
                error,
            }
            .for_task(self.identity.task_id),
        }
    }
}

#[cfg(feature = "bt")]
pub struct SeedingRequest {
    identity: RuntimeIdentity,
}

#[cfg(feature = "bt")]
impl SeedingRequest {
    #[must_use]
    pub fn complete(self) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::SeedingComplete {
                gid: self.identity.gid,
                generation: self.identity.generation,
            }
            .for_task(self.identity.task_id),
        }
    }
    #[must_use]
    pub fn failed(self, error: PublicError) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::SeedingFailed {
                gid: self.identity.gid,
                generation: self.identity.generation,
                error,
            }
            .for_task(self.identity.task_id),
        }
    }
}

pub struct CancellationRequest {
    identity: RuntimeIdentity,
    force: bool,
}

impl CancellationRequest {
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.identity.task_id
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        self.identity.gid
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.identity.generation
    }

    #[must_use]
    pub const fn force(&self) -> bool {
        self.force
    }

    #[must_use]
    pub fn drained(self) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::CancellationDrained {
                gid: self.identity.gid,
                generation: self.identity.generation,
            }
            .for_task(self.identity.task_id),
        }
    }
}

pub struct NoSpaceProbeRequest {
    identity: RuntimeIdentity,
    probe_id: NoSpaceProbeId,
    origin: NoSpaceProbeOrigin,
    at: MonotonicInstant,
    startup: Option<BoundNoSpaceProbe>,
}

impl NoSpaceProbeRequest {
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.identity.task_id
    }

    #[must_use]
    pub const fn gid(&self) -> Gid {
        self.identity.gid
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.identity.generation
    }

    #[must_use]
    pub const fn probe_id(&self) -> NoSpaceProbeId {
        self.probe_id
    }

    #[must_use]
    pub const fn origin(&self) -> NoSpaceProbeOrigin {
        self.origin
    }

    #[must_use]
    pub const fn at(&self) -> MonotonicInstant {
        self.at
    }

    #[must_use]
    pub fn startup_target(&self) -> Option<&PlatformPath> {
        self.startup.as_ref().map(BoundNoSpaceProbe::target)
    }

    #[must_use]
    pub fn completed(
        self,
        ready: bool,
        next_retry_at: Option<MonotonicInstant>,
    ) -> RuntimeEventSubmission {
        RuntimeEventSubmission {
            event: TaskEvent::NoSpaceProbeCompleted {
                gid: self.identity.gid,
                generation: self.identity.generation,
                probe_id: self.probe_id,
                origin: self.origin,
                ready,
                next_retry_at,
            }
            .for_task(self.identity.task_id),
        }
    }
}

/// Concrete bounded adapter for scheduler-owned worker, timer, cancellation,
/// no-space, and option-application effects.
pub struct RuntimeSchedulerEffectSink {
    mailbox: Arc<Mutex<RuntimeMailbox>>,
    no_space_targets: NoSpaceProbeTargetCatalog,
    option_plan_capacity: usize,
    option_plans: VecDeque<OptionApplicationPlan>,
    immediate: Option<EffectCompletion>,
}

impl RuntimeSchedulerEffectSink {
    pub fn new(
        config: RuntimeEffectConfig,
        no_space_targets: NoSpaceProbeTargetCatalog,
    ) -> Result<(Self, RuntimeEffectHandle), RuntimeEffectConfigError> {
        let config = config.validate()?;
        let mailbox = Arc::new(Mutex::new(RuntimeMailbox {
            closed: false,
            bt_tasks: std::collections::BTreeSet::new(),
            request_capacity: config.request_capacity.get(),
            event_capacity: config.event_capacity.get(),
            timer_capacity: config.timer_capacity.get(),
            allocations: VecDeque::with_capacity(config.request_capacity.get()),
            cancellations: VecDeque::with_capacity(config.request_capacity.get()),
            probes: VecDeque::with_capacity(config.request_capacity.get()),
            events: VecDeque::with_capacity(config.event_capacity.get()),
            retry_timers: Vec::with_capacity(config.timer_capacity.get()),
            slow_timers: Vec::with_capacity(config.timer_capacity.get()),
        }));
        let handle = RuntimeEffectHandle {
            mailbox: Arc::clone(&mailbox),
        };
        Ok((
            Self {
                mailbox,
                no_space_targets,
                option_plan_capacity: config.option_plan_capacity.get(),
                option_plans: VecDeque::with_capacity(config.option_plan_capacity.get()),
                immediate: None,
            },
            handle,
        ))
    }

    #[must_use]
    pub fn remaining_startup_probe_targets(&self) -> usize {
        self.no_space_targets.remaining()
    }

    fn accept(
        &mut self,
        dispatch_id: ariax_runtime::EffectDispatchId,
    ) -> Poll<Result<(), EffectSinkError>> {
        self.immediate = Some(EffectCompletion::Completed {
            dispatch_id,
            acknowledgement: None,
        });
        Poll::Ready(Ok(()))
    }

    fn fail_representably(
        &mut self,
        dispatch_id: ariax_runtime::EffectDispatchId,
    ) -> Poll<Result<(), EffectSinkError>> {
        self.immediate = Some(EffectCompletion::UnrepresentableFailure { dispatch_id });
        Poll::Ready(Ok(()))
    }
}

impl SchedulerEffectSink for RuntimeSchedulerEffectSink {
    fn poll_dispatch(
        &mut self,
        dispatched: &DispatchedEffect,
    ) -> Poll<Result<(), EffectSinkError>> {
        if self.immediate.is_some() {
            return Poll::Ready(Err(EffectSinkError::Failed));
        }
        let mut mailbox = lock_unpoisoned(&self.mailbox);
        if mailbox.closed {
            return Poll::Ready(Err(EffectSinkError::Closed));
        }
        let dispatch_id = dispatched.dispatch_id();
        let effect = dispatched.effect();
        match effect {
            TransitionEffect::ApplyOptionPatch {
                task_id,
                gid,
                patch_id,
                ..
            } => {
                drop(mailbox);
                let Some(index) = self
                    .option_plans
                    .iter()
                    .position(|plan| plan.effect == *effect)
                else {
                    return self.fail_representably(dispatch_id);
                };
                let plan = self
                    .option_plans
                    .remove(index)
                    .expect("the exact option plan index remains present");
                let event = match plan.outcome {
                    OptionApplicationOutcome::Applied => TaskEvent::OptionPatchApplied {
                        gid: *gid,
                        generation: dispatched.task_generation(),
                        patch_id: *patch_id,
                    },
                    OptionApplicationOutcome::Failed(error) => {
                        TaskEvent::OptionPatchApplicationFailed {
                            gid: *gid,
                            generation: dispatched.task_generation(),
                            patch_id: *patch_id,
                            error,
                        }
                    }
                };
                self.immediate = Some(EffectCompletion::Completed {
                    dispatch_id,
                    acknowledgement: Some(event.for_task(*task_id)),
                });
                Poll::Ready(Ok(()))
            }
            TransitionEffect::StartAllocation {
                task_id,
                gid,
                generation,
            } => {
                if mailbox.request_count() == mailbox.request_capacity {
                    return Poll::Pending;
                }
                mailbox.allocations.push_back(AllocationEntry {
                    identity: RuntimeIdentity {
                        task_id: *task_id,
                        gid: *gid,
                        generation: *generation,
                    },
                });
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::CancelGeneration {
                task_id,
                gid,
                generation,
                force,
            } => {
                if mailbox.request_count() == mailbox.request_capacity {
                    return Poll::Pending;
                }
                mailbox.cancellations.push_back(CancellationEntry {
                    identity: RuntimeIdentity {
                        task_id: *task_id,
                        gid: *gid,
                        generation: *generation,
                    },
                    force: *force,
                });
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::ReleaseSlot { .. } => {
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::ScheduleRetry {
                task_id,
                gid,
                generation,
                retry_timer_id,
                at,
            } => {
                let identity = RuntimeIdentity {
                    task_id: *task_id,
                    gid: *gid,
                    generation: *generation,
                };
                if let Some(existing) = mailbox
                    .retry_timers
                    .iter()
                    .find(|timer| timer.identity == identity && timer.timer_id == *retry_timer_id)
                {
                    if existing.at != *at {
                        drop(mailbox);
                        return self.fail_representably(dispatch_id);
                    }
                } else {
                    if mailbox.timer_count() == mailbox.timer_capacity {
                        return Poll::Pending;
                    }
                    mailbox.retry_timers.push(RetryTimerEntry {
                        identity,
                        timer_id: *retry_timer_id,
                        at: *at,
                    });
                }
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::CancelRetry {
                task_id,
                gid,
                generation,
                retry_timer_id,
            } => {
                let identity = RuntimeIdentity {
                    task_id: *task_id,
                    gid: *gid,
                    generation: *generation,
                };
                mailbox.retry_timers.retain(|timer| {
                    timer.identity != identity || timer.timer_id != *retry_timer_id
                });
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::ScheduleSlowReadmission {
                task_id,
                gid,
                generation,
                readmission_id,
                at,
            } => {
                let identity = RuntimeIdentity {
                    task_id: *task_id,
                    gid: *gid,
                    generation: *generation,
                };
                if let Some(existing) = mailbox.slow_timers.iter().find(|timer| {
                    timer.identity == identity && timer.readmission_id == *readmission_id
                }) {
                    if existing.at != *at {
                        drop(mailbox);
                        return self.fail_representably(dispatch_id);
                    }
                } else {
                    if mailbox.timer_count() == mailbox.timer_capacity {
                        return Poll::Pending;
                    }
                    mailbox.slow_timers.push(SlowTimerEntry {
                        identity,
                        readmission_id: *readmission_id,
                        at: *at,
                    });
                }
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::CancelSlowReadmission {
                task_id,
                gid,
                generation,
                readmission_id,
            } => {
                let identity = RuntimeIdentity {
                    task_id: *task_id,
                    gid: *gid,
                    generation: *generation,
                };
                mailbox.slow_timers.retain(|timer| {
                    timer.identity != identity || timer.readmission_id != *readmission_id
                });
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::ProbeNoSpace {
                task_id,
                gid,
                generation,
                probe_id,
                origin,
                at,
            } => {
                if mailbox.request_count() == mailbox.request_capacity {
                    return Poll::Pending;
                }
                drop(mailbox);
                let startup = if *origin == NoSpaceProbeOrigin::AutomaticRetry
                    && self.no_space_targets.remaining() != 0
                {
                    match self.no_space_targets.consume(effect) {
                        Ok(target) => Some(target),
                        Err(_) => return self.fail_representably(dispatch_id),
                    }
                } else {
                    None
                };
                let mut mailbox = lock_unpoisoned(&self.mailbox);
                if mailbox.closed {
                    return Poll::Ready(Err(EffectSinkError::Closed));
                }
                if mailbox.request_count() == mailbox.request_capacity {
                    drop(mailbox);
                    return self.fail_representably(dispatch_id);
                }
                mailbox.probes.push_back(NoSpaceProbeEntry {
                    identity: RuntimeIdentity {
                        task_id: *task_id,
                        gid: *gid,
                        generation: *generation,
                    },
                    probe_id: *probe_id,
                    origin: *origin,
                    at: *at,
                    startup,
                });
                drop(mailbox);
                self.accept(dispatch_id)
            }
            TransitionEffect::PersistTask { .. }
            | TransitionEffect::PersistQueueTransition { .. }
            | TransitionEffect::StageOptionPatch { .. }
            | TransitionEffect::PersistGenerationStarted { .. }
            | TransitionEffect::PersistConditions { .. }
            | TransitionEffect::PersistHostKeyChallenge { .. }
            | TransitionEffect::PersistHostKeyPinAndClearChallenge { .. }
            | TransitionEffect::PersistHostKeyChallengeRejected { .. }
            | TransitionEffect::PersistTerminal { .. }
            | TransitionEffect::DeleteStoppedTaskMetadata { .. }
            | TransitionEffect::PublishSnapshot { .. } => {
                drop(mailbox);
                Poll::Ready(Err(EffectSinkError::Failed))
            }
        }
    }

    fn poll_completion(&mut self) -> Poll<EffectCompletion> {
        self.immediate.take().map_or(Poll::Pending, Poll::Ready)
    }
}

impl SchedulerEffectSinkPrepare for RuntimeSchedulerEffectSink {
    type Preparation = RuntimeEffectPreparation;
    type Error = RuntimeEffectPrepareError;

    fn prepare(&mut self, preparation: Self::Preparation) -> Result<(), Self::Error> {
        match preparation {
            RuntimeEffectPreparation::DiscardOptionApplications => {
                self.option_plans.clear();
                Ok(())
            }
            RuntimeEffectPreparation::OptionApplication(plan) => {
                if self
                    .option_plans
                    .iter()
                    .any(|entry| entry.effect == plan.effect)
                {
                    return Err(RuntimeEffectPrepareError::Duplicate);
                }
                if self.option_plans.len() == self.option_plan_capacity {
                    return Err(RuntimeEffectPrepareError::Full);
                }
                self.option_plans.push_back(*plan);
                Ok(())
            }
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::{
        NoSpaceProbeTargetCatalog, OptionApplicationOutcome, OptionApplicationPlan,
        RuntimeEffectConfig, RuntimeEffectPreparation, RuntimeEffectPrepareError,
        RuntimeSchedulerEffectSink,
    };
    use ariax_core::{
        ALL_QUEUE_CLASSES, Generation, Gid, MonotonicInstant, QueueClass, QueueOrder,
        RecoveredSchedulerTask, RequestScheduler, SchedulerConfig, SchedulerRestoreBatch,
        TaskConditions, TaskId, TaskState, TransitionEffect,
    };
    use ariax_runtime::{SchedulerDriver, SchedulerDriverPoll, SchedulerEffectSinkPrepare};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("nonzero gid")
    }

    fn task_id(value: u64) -> TaskId {
        TaskId::new(value).expect("nonzero task id")
    }

    fn config(capacity: usize) -> RuntimeEffectConfig {
        let capacity = NonZeroUsize::new(capacity).expect("nonzero capacity");
        RuntimeEffectConfig {
            request_capacity: capacity,
            event_capacity: capacity,
            timer_capacity: capacity,
            option_plan_capacity: capacity,
        }
    }

    fn restored_retry(task_id: TaskId, gid: Gid, at: MonotonicInstant) -> RecoveredSchedulerTask {
        RecoveredSchedulerTask {
            task_id,
            gid,
            state: TaskState::RetryWait,
            generation: Generation::INITIAL,
            generation_started: true,
            desired_paused: false,
            conditions: TaskConditions::default(),
            slow_demotion_count: 0,
            slow_slot: None,
            retry_at: Some(at),
            host_key_challenge: None,
            error: None,
            stopped_status: None,
        }
    }

    fn queues(waiting: Vec<Gid>) -> Vec<QueueOrder> {
        ALL_QUEUE_CLASSES
            .iter()
            .copied()
            .map(|class| QueueOrder {
                class,
                order: if class == QueueClass::Waiting {
                    waiting.clone()
                } else {
                    Vec::new()
                },
            })
            .collect()
    }

    #[test]
    fn recovered_retry_timer_is_not_visible_before_its_deadline() {
        let now = MonotonicInstant::now();
        let deadline = now.checked_add(Duration::from_secs(1)).expect("deadline");
        let task_gid = gid(1);
        let scheduler_config = SchedulerConfig::new(
            NonZeroUsize::new(1).expect("tasks"),
            NonZeroUsize::new(1).expect("active"),
            false,
        )
        .expect("scheduler config");
        let (scheduler, plan) = RequestScheduler::restore(
            scheduler_config,
            SchedulerRestoreBatch::new(
                vec![restored_retry(task_id(1), task_gid, deadline)],
                queues(vec![task_gid]),
            ),
        )
        .expect("restore scheduler");
        let (sink, handle) =
            RuntimeSchedulerEffectSink::new(config(2), NoSpaceProbeTargetCatalog::new(Vec::new()))
                .expect("runtime sink");
        let mut driver = SchedulerDriver::new(scheduler, sink);
        driver.begin_restore(plan).expect("begin restore");
        for _ in 0..32 {
            if matches!(driver.poll_at(now), SchedulerDriverPoll::Completed { .. }) {
                break;
            }
        }
        assert!(driver.is_idle());
        assert!(handle.poll_event_at(now).is_none());
        let event = handle
            .poll_event_at(deadline)
            .expect("retry event at deadline");
        assert_eq!(event.task_id(), task_id(1));
        assert_eq!(event.event().gid(), task_gid);
    }

    #[test]
    fn bounded_timer_admission_backpressures_without_losing_the_offer() {
        let now = MonotonicInstant::now();
        let deadline = now.checked_add(Duration::from_secs(1)).expect("deadline");
        let scheduler_config = SchedulerConfig::new(
            NonZeroUsize::new(2).expect("tasks"),
            NonZeroUsize::new(1).expect("active"),
            false,
        )
        .expect("scheduler config");
        let (scheduler, plan) = RequestScheduler::restore(
            scheduler_config,
            SchedulerRestoreBatch::new(
                vec![
                    restored_retry(task_id(1), gid(1), deadline),
                    restored_retry(task_id(2), gid(2), deadline),
                ],
                queues(vec![gid(1), gid(2)]),
            ),
        )
        .expect("restore scheduler");
        let (sink, _) =
            RuntimeSchedulerEffectSink::new(config(1), NoSpaceProbeTargetCatalog::new(Vec::new()))
                .expect("runtime sink");
        let mut driver = SchedulerDriver::new(scheduler, sink);
        driver.begin_restore(plan).expect("begin restore");
        let mut backpressured = false;
        for _ in 0..32 {
            if matches!(
                driver.poll_at(now),
                SchedulerDriverPoll::Backpressured { .. }
            ) {
                backpressured = true;
                break;
            }
        }
        assert!(backpressured);
        assert!(!driver.is_idle());
    }

    #[test]
    fn option_application_catalog_is_exact_and_bounded() {
        let (mut sink, _) =
            RuntimeSchedulerEffectSink::new(config(1), NoSpaceProbeTargetCatalog::new(Vec::new()))
                .expect("runtime sink");
        let effect = TransitionEffect::ApplyOptionPatch {
            task_id: task_id(1),
            gid: gid(1),
            patch_id: ariax_core::OptionPatchId::new(1).expect("patch id"),
            satisfies_credentials: None,
        };
        let plan = OptionApplicationPlan::new(effect, OptionApplicationOutcome::Applied)
            .expect("option plan");
        sink.prepare(RuntimeEffectPreparation::OptionApplication(Box::new(
            plan.clone(),
        )))
        .expect("register option plan");
        assert_eq!(
            sink.prepare(RuntimeEffectPreparation::OptionApplication(Box::new(plan))),
            Err(RuntimeEffectPrepareError::Duplicate)
        );
        let other = OptionApplicationPlan::new(
            TransitionEffect::ApplyOptionPatch {
                task_id: task_id(2),
                gid: gid(2),
                patch_id: ariax_core::OptionPatchId::new(2).expect("patch id"),
                satisfies_credentials: None,
            },
            OptionApplicationOutcome::Applied,
        )
        .expect("other option plan");
        assert_eq!(
            sink.prepare(RuntimeEffectPreparation::OptionApplication(Box::new(
                other.clone()
            ))),
            Err(RuntimeEffectPrepareError::Full)
        );
        sink.prepare(RuntimeEffectPreparation::DiscardOptionApplications)
            .expect("discard unused preparations");
        assert!(sink.option_plans.is_empty());
        sink.prepare(RuntimeEffectPreparation::OptionApplication(Box::new(other)))
            .expect("capacity restored");
    }
}
