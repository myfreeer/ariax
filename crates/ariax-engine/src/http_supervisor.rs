//! Bounded scheduler-owned supervision for public HTTP workers.

use crate::{
    ActiveTransferRequest, CancellationRequest, HttpCancellation, HttpTaskSpec,
    RuntimeEffectHandle, RuntimeEventSubmission, RuntimeEventSubmitError, SharedHttpTaskCatalog,
};
use ariax_core::{ErrorKind, Generation, Gid, MonotonicInstant, PublicError, RetryClass, TaskId};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{Id as JoinId, JoinSet};

pub const MAX_HTTP_SUPERVISOR_ACTIVE_WORKERS: usize = 1024;
pub const MAX_HTTP_SUPERVISOR_PENDING_EVENTS: usize = 4096;
pub const MAX_HTTP_SUPERVISOR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_HTTP_SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(1);
pub const DEFAULT_HTTP_SUPERVISOR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Successful completion metadata returned by one public HTTP worker.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpWorkerSuccess {
    pub seed: bool,
    /// All ranges are settled and the journal is flushed; retry may release its slot.
    pub retry_at: Option<MonotonicInstant>,
}

pub type HttpWorkerFuture =
    Pin<Box<dyn Future<Output = Result<HttpWorkerSuccess, PublicError>> + Send + 'static>>;

/// Factory for one transfer generation. The supervisor owns scheduler event
/// authorities; workers own only immutable task metadata and cancellation.
pub trait HttpTaskWorker: Send + Sync + 'static {
    fn start(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
    ) -> HttpWorkerFuture;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpWorkerSupervisorConfig {
    pub max_active_workers: NonZeroUsize,
    pub pending_event_capacity: NonZeroUsize,
    pub poll_interval: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for HttpWorkerSupervisorConfig {
    fn default() -> Self {
        Self {
            max_active_workers: NonZeroUsize::new(64).expect("default worker count is nonzero"),
            pending_event_capacity: NonZeroUsize::new(256)
                .expect("default pending event count is nonzero"),
            poll_interval: DEFAULT_HTTP_SUPERVISOR_POLL_INTERVAL,
            shutdown_timeout: DEFAULT_HTTP_SUPERVISOR_SHUTDOWN_TIMEOUT,
        }
    }
}

impl HttpWorkerSupervisorConfig {
    pub fn validate(self) -> Result<Self, HttpWorkerSupervisorConfigError> {
        if self.max_active_workers.get() > MAX_HTTP_SUPERVISOR_ACTIVE_WORKERS {
            return Err(HttpWorkerSupervisorConfigError::TooManyActiveWorkers);
        }
        if self.pending_event_capacity.get() > MAX_HTTP_SUPERVISOR_PENDING_EVENTS {
            return Err(HttpWorkerSupervisorConfigError::TooManyPendingEvents);
        }
        let minimum_pending = self
            .max_active_workers
            .get()
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or(HttpWorkerSupervisorConfigError::TooManyActiveWorkers)?;
        if self.pending_event_capacity.get() < minimum_pending {
            return Err(HttpWorkerSupervisorConfigError::PendingCapacityTooSmall);
        }
        if self.poll_interval.is_zero() || self.poll_interval > Duration::from_secs(1) {
            return Err(HttpWorkerSupervisorConfigError::InvalidPollInterval);
        }
        if self.shutdown_timeout.is_zero()
            || self.shutdown_timeout > MAX_HTTP_SUPERVISOR_SHUTDOWN_TIMEOUT
        {
            return Err(HttpWorkerSupervisorConfigError::InvalidShutdownTimeout);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpWorkerSupervisorConfigError {
    TooManyActiveWorkers,
    TooManyPendingEvents,
    PendingCapacityTooSmall,
    InvalidPollInterval,
    InvalidShutdownTimeout,
}

impl fmt::Display for HttpWorkerSupervisorConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TooManyActiveWorkers => "HTTP supervisor active-worker capacity is invalid",
            Self::TooManyPendingEvents => "HTTP supervisor pending-event capacity is invalid",
            Self::PendingCapacityTooSmall => {
                "HTTP supervisor pending-event capacity cannot retain worker completions"
            }
            Self::InvalidPollInterval => "HTTP supervisor poll interval is invalid",
            Self::InvalidShutdownTimeout => "HTTP supervisor shutdown timeout is invalid",
        })
    }
}

impl Error for HttpWorkerSupervisorConfigError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpWorkerSupervisorPoll {
    Progressed,
    Idle,
    Backpressured,
}

/// Bounded result of cancelling and joining every live HTTP worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpWorkerSupervisorShutdown {
    Drained,
    TimedOut { aborted_workers: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpWorkerSupervisorError {
    RuntimeClosed,
    PendingEventOverflow,
    LostWorkerAuthority,
}

impl fmt::Display for HttpWorkerSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RuntimeClosed => "HTTP supervisor runtime mailbox is closed",
            Self::PendingEventOverflow => "HTTP supervisor pending event queue is full",
            Self::LostWorkerAuthority => "HTTP supervisor lost worker lifecycle authority",
        })
    }
}

impl Error for HttpWorkerSupervisorError {}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct WorkerIdentity {
    task: TaskId,
    gid: Gid,
    generation: Generation,
}

struct ActiveWorker {
    identity: WorkerIdentity,
    authority: ActiveTransferRequest,
    cancellation: HttpCancellation,
    cancellation_request: Option<CancellationRequest>,
}

struct WorkerCompletion {
    identity: WorkerIdentity,
    result: Result<HttpWorkerSuccess, PublicError>,
}

/// Owns all live public HTTP workers and their exact scheduler-issued
/// lifecycle authorities.
pub struct HttpWorkerSupervisor {
    runtime: RuntimeEffectHandle,
    tasks: SharedHttpTaskCatalog,
    worker: Arc<dyn HttpTaskWorker>,
    config: HttpWorkerSupervisorConfig,
    active: BTreeMap<TaskId, ActiveWorker>,
    joins: JoinSet<WorkerCompletion>,
    by_join: HashMap<JoinId, TaskId>,
    pending_events: VecDeque<RuntimeEventSubmission>,
}

impl HttpWorkerSupervisor {
    pub fn new(
        runtime: RuntimeEffectHandle,
        tasks: SharedHttpTaskCatalog,
        worker: Arc<dyn HttpTaskWorker>,
        config: HttpWorkerSupervisorConfig,
    ) -> Result<Self, HttpWorkerSupervisorConfigError> {
        let config = config.validate()?;
        Ok(Self {
            runtime,
            tasks,
            worker,
            config,
            active: BTreeMap::new(),
            joins: JoinSet::new(),
            by_join: HashMap::new(),
            pending_events: VecDeque::with_capacity(config.pending_event_capacity.get()),
        })
    }

    #[must_use]
    pub fn active_workers(&self) -> usize {
        self.active.len()
    }

    #[must_use]
    pub fn pending_events(&self) -> usize {
        self.pending_events.len()
    }

    /// Runs until the scheduler runtime mailbox closes.
    pub async fn run(mut self) -> Result<(), HttpWorkerSupervisorError> {
        loop {
            match self.poll_once(MonotonicInstant::now())? {
                HttpWorkerSupervisorPoll::Progressed => tokio::task::yield_now().await,
                HttpWorkerSupervisorPoll::Idle | HttpWorkerSupervisorPoll::Backpressured => {
                    tokio::time::sleep(self.config.poll_interval).await;
                }
            }
        }
    }

    /// Cancels every live worker and waits for their futures to release file,
    /// network, and session-owner references. A bounded fallback aborts any
    /// worker that does not cooperate with cancellation.
    pub async fn shutdown(self) -> HttpWorkerSupervisorShutdown {
        let timeout = self.config.shutdown_timeout;
        self.shutdown_with_timeout(timeout).await
    }

    /// Applies the tighter of the supervisor policy and an outer process-step
    /// deadline so this lane cannot outlive its coordinator ticket.
    pub async fn shutdown_with_timeout(
        mut self,
        outer_timeout: Duration,
    ) -> HttpWorkerSupervisorShutdown {
        for worker in self.active.values() {
            worker.cancellation.cancel();
        }
        let timeout = self.config.shutdown_timeout.min(outer_timeout);
        if tokio::time::timeout(timeout, async {
            while self.joins.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            let aborted_workers = self.joins.len();
            self.joins.abort_all();
            while self.joins.join_next().await.is_some() {}
            HttpWorkerSupervisorShutdown::TimedOut { aborted_workers }
        } else {
            HttpWorkerSupervisorShutdown::Drained
        }
    }

    /// Best-effort synchronous cancellation used by non-async embedding
    /// callers before the supervisor is dropped.
    pub fn cancel_all(&mut self) {
        for worker in self.active.values() {
            worker.cancellation.cancel();
        }
        self.joins.abort_all();
    }

    /// Performs bounded, non-blocking progress for tests or an owning process
    /// loop. Older pending events are always submitted before newer events.
    pub fn poll_once(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<HttpWorkerSupervisorPoll, HttpWorkerSupervisorError> {
        if self.runtime.is_closed() {
            for worker in self.active.values() {
                worker.cancellation.cancel();
            }
            return Err(HttpWorkerSupervisorError::RuntimeClosed);
        }

        let mut progressed = self.flush_pending()?;
        if !self.pending_events.is_empty() {
            return Ok(HttpWorkerSupervisorPoll::Backpressured);
        }

        while self.pending_slots() >= 2 {
            let Some(joined) = self.joins.try_join_next_with_id() else {
                break;
            };
            progressed = true;
            self.complete_join(joined)?;
            let _flushed = self.flush_pending()?;
            if !self.pending_events.is_empty() {
                return Ok(HttpWorkerSupervisorPoll::Backpressured);
            }
        }

        while self.pending_slots() >= 1 {
            let Some(request) = self.runtime.take_cancellation() else {
                break;
            };
            progressed = true;
            self.accept_cancellation(request)?;
            let _flushed = self.flush_pending()?;
            if !self.pending_events.is_empty() {
                return Ok(HttpWorkerSupervisorPoll::Backpressured);
            }
        }

        while self.pending_slots() >= 1 {
            let Some(allocation) = self.runtime.take_allocation() else {
                break;
            };
            progressed = true;
            self.accept_allocation(allocation, now)?;
            let _flushed = self.flush_pending()?;
            if !self.pending_events.is_empty() {
                return Ok(HttpWorkerSupervisorPoll::Backpressured);
            }
        }

        Ok(if progressed {
            HttpWorkerSupervisorPoll::Progressed
        } else {
            HttpWorkerSupervisorPoll::Idle
        })
    }

    fn pending_slots(&self) -> usize {
        self.config
            .pending_event_capacity
            .get()
            .saturating_sub(self.pending_events.len())
    }

    fn flush_pending(&mut self) -> Result<bool, HttpWorkerSupervisorError> {
        let mut progressed = false;
        while let Some(submission) = self.pending_events.pop_front() {
            match self.runtime.try_submit_event(submission) {
                Ok(()) => progressed = true,
                Err(rejection) => {
                    let error = rejection.error();
                    self.pending_events.push_front(rejection.into_submission());
                    return match error {
                        RuntimeEventSubmitError::Full => Ok(progressed),
                        RuntimeEventSubmitError::Closed => {
                            Err(HttpWorkerSupervisorError::RuntimeClosed)
                        }
                    };
                }
            }
        }
        Ok(progressed)
    }

    fn enqueue_event(
        &mut self,
        submission: RuntimeEventSubmission,
    ) -> Result<(), HttpWorkerSupervisorError> {
        if self.pending_events.len() == self.config.pending_event_capacity.get() {
            return Err(HttpWorkerSupervisorError::PendingEventOverflow);
        }
        if self.pending_events.is_empty() {
            match self.runtime.try_submit_event(submission) {
                Ok(()) => return Ok(()),
                Err(rejection) => match rejection.error() {
                    RuntimeEventSubmitError::Full => {
                        self.pending_events.push_back(rejection.into_submission());
                        return Ok(());
                    }
                    RuntimeEventSubmitError::Closed => {
                        return Err(HttpWorkerSupervisorError::RuntimeClosed);
                    }
                },
            }
        }
        self.pending_events.push_back(submission);
        Ok(())
    }

    fn accept_allocation(
        &mut self,
        allocation: crate::AllocationRequest,
        now: MonotonicInstant,
    ) -> Result<(), HttpWorkerSupervisorError> {
        if self.active.len() == self.config.max_active_workers.get() {
            let retry_at = now.checked_add(self.config.poll_interval).unwrap_or(now);
            return self.enqueue_event(allocation.retryable(retry_at));
        }
        if self.active.contains_key(&allocation.task_id()) {
            return self.enqueue_event(
                allocation.failed(supervisor_public_error("http_worker_already_active")),
            );
        }
        let Some(task) = self.tasks.get(allocation.task_id()) else {
            return self.enqueue_event(allocation.failed(PublicError::new(
                ErrorKind::GidNotFound,
                "http_task_not_registered",
                RetryClass::Never,
            )));
        };
        if task.gid() != allocation.gid() {
            return self.enqueue_event(
                allocation.failed(supervisor_public_error("http_allocation_identity_mismatch")),
            );
        }

        let identity = WorkerIdentity {
            task: allocation.task_id(),
            gid: allocation.gid(),
            generation: allocation.generation(),
        };
        let cancellation = HttpCancellation::new();
        let future = match catch_unwind(AssertUnwindSafe(|| {
            self.worker
                .start(Arc::clone(&task), identity.generation, cancellation.clone())
        })) {
            Ok(future) => future,
            Err(_) => {
                return self.enqueue_event(
                    allocation.failed(supervisor_public_error("http_worker_start_panicked")),
                );
            }
        };
        let (allocated, authority) = allocation.activate();
        self.enqueue_event(allocated)?;
        let abort = self.joins.spawn(async move {
            WorkerCompletion {
                identity,
                result: future.await,
            }
        });
        self.by_join.insert(abort.id(), identity.task);
        self.active.insert(
            identity.task,
            ActiveWorker {
                identity,
                authority,
                cancellation,
                cancellation_request: None,
            },
        );
        Ok(())
    }

    fn accept_cancellation(
        &mut self,
        request: CancellationRequest,
    ) -> Result<(), HttpWorkerSupervisorError> {
        let identity = WorkerIdentity {
            task: request.task_id(),
            gid: request.gid(),
            generation: request.generation(),
        };
        let Some(worker) = self.active.get_mut(&identity.task) else {
            return self.enqueue_event(request.drained());
        };
        if worker.identity != identity || worker.cancellation_request.is_some() {
            return self.enqueue_event(request.drained());
        }
        worker.cancellation.cancel();
        worker.cancellation_request = Some(request);
        Ok(())
    }

    fn complete_join(
        &mut self,
        joined: Result<(JoinId, WorkerCompletion), tokio::task::JoinError>,
    ) -> Result<(), HttpWorkerSupervisorError> {
        let (join_id, completion) = match joined {
            Ok((join_id, completion)) => (join_id, Some(completion)),
            Err(error) => (error.id(), None),
        };
        let task = self
            .by_join
            .remove(&join_id)
            .ok_or(HttpWorkerSupervisorError::LostWorkerAuthority)?;
        let worker = self
            .active
            .remove(&task)
            .ok_or(HttpWorkerSupervisorError::LostWorkerAuthority)?;
        if let Some(request) = worker.cancellation_request {
            return self.enqueue_event(request.drained());
        }

        let result = completion
            .filter(|completion| completion.identity == worker.identity)
            .map_or_else(
                || Err(supervisor_public_error("http_worker_panicked")),
                |completion| completion.result,
            );
        match result {
            Ok(HttpWorkerSuccess {
                retry_at: Some(deadline),
                ..
            }) => self.enqueue_event(worker.authority.retryable(deadline)),
            Ok(success) => {
                let (data_complete, verifying) = worker.authority.data_complete(success.seed);
                self.enqueue_event(data_complete)?;
                self.enqueue_event(verifying.succeeded())
            }
            Err(error)
                if error.kind() == ErrorKind::StaleValidator
                    && error.retry_class() == RetryClass::RestartGeneration =>
            {
                self.enqueue_event(worker.authority.restart_representation())
            }
            Err(error) => self.enqueue_event(worker.authority.failed(error)),
        }
    }
}

fn supervisor_public_error(message: &'static str) -> PublicError {
    PublicError::new(ErrorKind::InternalInvariant, message, RetryClass::Never)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSpaceProbeTargetCatalog, RuntimeEffectConfig, RuntimeSchedulerEffectSink};
    use ariax_core::TaskEvent;
    use ariax_storage::{PathPlatform, SafePathBuilder};
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    fn task_id(value: u64) -> TaskId {
        TaskId::new(value).expect("task id")
    }

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("gid")
    }

    fn task(task: TaskId, gid: Gid) -> HttpTaskSpec {
        HttpTaskSpec::new(
            task,
            gid,
            ["https://example.test/file".to_owned()],
            PathBuf::from(if cfg!(windows) {
                r"C:\ariax"
            } else {
                "/tmp/ariax"
            }),
            SafePathBuilder::from_user_path("file.bin", PathPlatform::current())
                .expect("safe output"),
            crate::HttpTaskOptions::default(),
            false,
        )
        .expect("task spec")
    }

    fn runtime(capacity: usize) -> RuntimeEffectHandle {
        let capacity = NonZeroUsize::new(capacity).expect("capacity");
        let (_sink, handle) = RuntimeSchedulerEffectSink::new(
            RuntimeEffectConfig {
                request_capacity: capacity,
                event_capacity: capacity,
                timer_capacity: capacity,
                option_plan_capacity: capacity,
            },
            NoSpaceProbeTargetCatalog::new(Vec::new()),
        )
        .expect("runtime");
        handle
    }

    fn config(max_active: usize) -> HttpWorkerSupervisorConfig {
        HttpWorkerSupervisorConfig {
            max_active_workers: NonZeroUsize::new(max_active).expect("active"),
            pending_event_capacity: NonZeroUsize::new(max_active * 2 + 1).expect("pending"),
            poll_interval: Duration::from_millis(1),
            shutdown_timeout: DEFAULT_HTTP_SUPERVISOR_SHUTDOWN_TIMEOUT,
        }
    }

    struct ImmediateWorker {
        result: Mutex<Option<Result<HttpWorkerSuccess, PublicError>>>,
    }

    impl HttpTaskWorker for ImmediateWorker {
        fn start(
            &self,
            _task: Arc<HttpTaskSpec>,
            _generation: Generation,
            _cancellation: HttpCancellation,
        ) -> HttpWorkerFuture {
            let result = self
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("one worker result");
            Box::pin(async move { result })
        }
    }

    struct CancelWorker {
        started: Arc<Notify>,
        stopped: Arc<Notify>,
    }

    struct ShutdownWorker {
        started: Arc<Notify>,
        stopped: Arc<AtomicBool>,
    }

    impl HttpTaskWorker for ShutdownWorker {
        fn start(
            &self,
            _task: Arc<HttpTaskSpec>,
            _generation: Generation,
            cancellation: HttpCancellation,
        ) -> HttpWorkerFuture {
            let started = Arc::clone(&self.started);
            let stopped = Arc::clone(&self.stopped);
            Box::pin(async move {
                started.notify_one();
                cancellation.cancelled().await;
                stopped.store(true, Ordering::Release);
                Err(PublicError::new(
                    ErrorKind::Cancelled,
                    "cancelled",
                    RetryClass::Never,
                ))
            })
        }
    }

    impl HttpTaskWorker for CancelWorker {
        fn start(
            &self,
            _task: Arc<HttpTaskSpec>,
            _generation: Generation,
            cancellation: HttpCancellation,
        ) -> HttpWorkerFuture {
            let started = Arc::clone(&self.started);
            let stopped = Arc::clone(&self.stopped);
            Box::pin(async move {
                started.notify_one();
                cancellation.cancelled().await;
                stopped.notified().await;
                Err(PublicError::new(
                    ErrorKind::Cancelled,
                    "cancelled",
                    RetryClass::Never,
                ))
            })
        }
    }

    #[tokio::test]
    async fn successful_worker_reports_exact_lifecycle_order() {
        let runtime = runtime(8);
        let tasks = SharedHttpTaskCatalog::new(NonZeroUsize::new(4).expect("tasks"));
        tasks.insert(task(task_id(1), gid(7))).expect("insert");
        runtime.enqueue_allocation_for_test(task_id(1), gid(7), Generation::INITIAL);
        let worker = Arc::new(ImmediateWorker {
            result: Mutex::new(Some(Ok(HttpWorkerSuccess::default()))),
        });
        let mut supervisor = HttpWorkerSupervisor::new(runtime.clone(), tasks, worker, config(2))
            .expect("supervisor");

        assert_eq!(
            supervisor.poll_once(MonotonicInstant::now()),
            Ok(HttpWorkerSupervisorPoll::Progressed)
        );
        tokio::task::yield_now().await;
        assert_eq!(
            supervisor.poll_once(MonotonicInstant::now()),
            Ok(HttpWorkerSupervisorPoll::Progressed)
        );

        let events = (0..3)
            .map(|_| {
                runtime
                    .poll_event_at(MonotonicInstant::now())
                    .expect("event")
                    .into_event()
            })
            .collect::<Vec<_>>();
        assert!(matches!(events[0], TaskEvent::AllocationSucceeded { .. }));
        assert!(matches!(
            events[1],
            TaskEvent::DataComplete { seed: false, .. }
        ));
        assert!(matches!(events[2], TaskEvent::VerificationSucceeded { .. }));
        assert_eq!(supervisor.active_workers(), 0);
    }

    #[tokio::test]
    async fn stale_restart_class_requests_a_new_generation_after_worker_drain() {
        let runtime = runtime(8);
        let tasks = SharedHttpTaskCatalog::new(NonZeroUsize::new(1).expect("tasks"));
        tasks.insert(task(task_id(1), gid(7))).expect("insert");
        runtime.enqueue_allocation_for_test(task_id(1), gid(7), Generation::INITIAL);
        let worker = Arc::new(ImmediateWorker {
            result: Mutex::new(Some(Err(PublicError::new(
                ErrorKind::StaleValidator,
                "representation_restart_required",
                RetryClass::RestartGeneration,
            )))),
        });
        let mut supervisor = HttpWorkerSupervisor::new(runtime.clone(), tasks, worker, config(1))
            .expect("supervisor");

        supervisor
            .poll_once(MonotonicInstant::now())
            .expect("start");
        tokio::task::yield_now().await;
        supervisor.poll_once(MonotonicInstant::now()).expect("reap");

        assert!(matches!(
            runtime
                .poll_event_at(MonotonicInstant::now())
                .expect("allocation")
                .into_event(),
            TaskEvent::AllocationSucceeded { .. }
        ));
        assert!(matches!(
            runtime
                .poll_event_at(MonotonicInstant::now())
                .expect("restart")
                .into_event(),
            TaskEvent::ActiveRepresentationRestart { .. }
        ));
        assert!(runtime.poll_event_at(MonotonicInstant::now()).is_none());
    }

    #[tokio::test]
    async fn non_validator_restart_class_remains_a_terminal_worker_failure() {
        let runtime = runtime(8);
        let tasks = SharedHttpTaskCatalog::new(NonZeroUsize::new(1).expect("tasks"));
        tasks.insert(task(task_id(1), gid(7))).expect("insert");
        runtime.enqueue_allocation_for_test(task_id(1), gid(7), Generation::INITIAL);
        let worker = Arc::new(ImmediateWorker {
            result: Mutex::new(Some(Err(PublicError::new(
                ErrorKind::ChecksumMismatch,
                "checksum_mismatch",
                RetryClass::RestartGeneration,
            )))),
        });
        let mut supervisor = HttpWorkerSupervisor::new(runtime.clone(), tasks, worker, config(1))
            .expect("supervisor");

        supervisor
            .poll_once(MonotonicInstant::now())
            .expect("start");
        tokio::task::yield_now().await;
        supervisor.poll_once(MonotonicInstant::now()).expect("reap");
        let _allocation = runtime
            .poll_event_at(MonotonicInstant::now())
            .expect("allocation");
        assert!(matches!(
            runtime
                .poll_event_at(MonotonicInstant::now())
                .expect("failure")
                .into_event(),
            TaskEvent::TerminalFailure { error, .. }
                if error.kind() == ErrorKind::ChecksumMismatch
        ));
    }

    #[tokio::test]
    async fn cancellation_is_drained_only_after_worker_stops() {
        let runtime = runtime(8);
        let tasks = SharedHttpTaskCatalog::new(NonZeroUsize::new(4).expect("tasks"));
        tasks.insert(task(task_id(1), gid(7))).expect("insert");
        let started = Arc::new(Notify::new());
        let stopped = Arc::new(Notify::new());
        let worker = Arc::new(CancelWorker {
            started: Arc::clone(&started),
            stopped: Arc::clone(&stopped),
        });
        let mut supervisor = HttpWorkerSupervisor::new(runtime.clone(), tasks, worker, config(2))
            .expect("supervisor");
        runtime.enqueue_allocation_for_test(task_id(1), gid(7), Generation::INITIAL);
        supervisor
            .poll_once(MonotonicInstant::now())
            .expect("allocate");
        started.notified().await;
        runtime.enqueue_cancellation_for_test(task_id(1), gid(7), Generation::INITIAL, false);
        supervisor
            .poll_once(MonotonicInstant::now())
            .expect("cancel");
        assert!(matches!(
            runtime
                .poll_event_at(MonotonicInstant::now())
                .expect("allocation")
                .into_event(),
            TaskEvent::AllocationSucceeded { .. }
        ));
        assert!(runtime.poll_event_at(MonotonicInstant::now()).is_none());

        stopped.notify_one();
        tokio::task::yield_now().await;
        supervisor.poll_once(MonotonicInstant::now()).expect("reap");
        assert!(matches!(
            runtime
                .poll_event_at(MonotonicInstant::now())
                .expect("drained")
                .into_event(),
            TaskEvent::CancellationDrained { .. }
        ));
        assert!(runtime.poll_event_at(MonotonicInstant::now()).is_none());
    }

    #[tokio::test]
    async fn shutdown_cancels_and_drains_live_workers() {
        let runtime = runtime(4);
        let tasks = SharedHttpTaskCatalog::new(NonZeroUsize::new(1).expect("tasks"));
        tasks.insert(task(task_id(1), gid(7))).expect("insert");
        let started = Arc::new(Notify::new());
        let stopped = Arc::new(AtomicBool::new(false));
        let worker = Arc::new(ShutdownWorker {
            started: Arc::clone(&started),
            stopped: Arc::clone(&stopped),
        });
        let mut supervisor = HttpWorkerSupervisor::new(runtime.clone(), tasks, worker, config(1))
            .expect("supervisor");
        runtime.enqueue_allocation_for_test(task_id(1), gid(7), Generation::INITIAL);
        supervisor
            .poll_once(MonotonicInstant::now())
            .expect("allocate");
        started.notified().await;

        assert_eq!(
            supervisor.shutdown().await,
            HttpWorkerSupervisorShutdown::Drained
        );

        assert!(stopped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_timeout_aborts_an_uncooperative_worker() {
        let runtime = runtime(4);
        let tasks = SharedHttpTaskCatalog::new(NonZeroUsize::new(1).expect("tasks"));
        tasks.insert(task(task_id(1), gid(7))).expect("insert");
        let started = Arc::new(Notify::new());
        let worker = Arc::new(CancelWorker {
            started: Arc::clone(&started),
            stopped: Arc::new(Notify::new()),
        });
        let mut supervisor = HttpWorkerSupervisor::new(runtime.clone(), tasks, worker, config(1))
            .expect("supervisor");
        runtime.enqueue_allocation_for_test(task_id(1), gid(7), Generation::INITIAL);
        supervisor
            .poll_once(MonotonicInstant::now())
            .expect("allocate");
        started.notified().await;

        assert_eq!(
            supervisor
                .shutdown_with_timeout(Duration::from_millis(10))
                .await,
            HttpWorkerSupervisorShutdown::TimedOut { aborted_workers: 1 }
        );
    }

    #[test]
    fn shutdown_timeout_bounds_are_rejected() {
        let mut invalid = config(1);
        invalid.shutdown_timeout = Duration::ZERO;
        assert_eq!(
            invalid.validate(),
            Err(HttpWorkerSupervisorConfigError::InvalidShutdownTimeout)
        );
        invalid.shutdown_timeout = MAX_HTTP_SUPERVISOR_SHUTDOWN_TIMEOUT
            .checked_add(Duration::from_millis(1))
            .expect("timeout overflow");
        assert_eq!(
            invalid.validate(),
            Err(HttpWorkerSupervisorConfigError::InvalidShutdownTimeout)
        );
    }

    #[tokio::test]
    async fn missing_catalog_entry_fails_before_activation() {
        let runtime = runtime(4);
        let tasks = SharedHttpTaskCatalog::new(NonZeroUsize::new(1).expect("tasks"));
        runtime.enqueue_allocation_for_test(task_id(1), gid(7), Generation::INITIAL);
        let worker = Arc::new(ImmediateWorker {
            result: Mutex::new(Some(Ok(HttpWorkerSuccess::default()))),
        });
        let mut supervisor = HttpWorkerSupervisor::new(runtime.clone(), tasks, worker, config(1))
            .expect("supervisor");
        supervisor.poll_once(MonotonicInstant::now()).expect("poll");
        let event = runtime
            .poll_event_at(MonotonicInstant::now())
            .expect("failure")
            .into_event();
        assert!(matches!(
            event,
            TaskEvent::AllocationFailed { error, .. }
                if error.kind() == ErrorKind::GidNotFound
                    && error.safe_message() == "http_task_not_registered"
        ));
        assert_eq!(supervisor.active_workers(), 0);
    }
}
