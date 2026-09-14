//! Shared bounded admission and autonomous production progress.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex as StdMutex, Weak};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Bounded process-lifetime counters for the production mutable owner.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlRuntimeMetrics {
    pub turns: u64,
    pub total_lock_wait_us: u64,
    pub max_lock_wait_us: u64,
    pub max_turn_us: u64,
    pub max_steps: usize,
}

#[derive(Default)]
struct Metrics {
    turns: AtomicU64,
    wait: AtomicU64,
    max_wait: AtomicU64,
    max_turn: AtomicU64,
    max_steps: AtomicUsize,
}

#[derive(Clone)]
pub(crate) struct ControlRuntime {
    pub(super) shared: Arc<SharedRuntime>,
}

pub(super) struct SharedRuntime {
    plane: Weak<Mutex<HttpControlPlane>>,
    queries: query::ControlQueryReader,
    ordering: control_ops::ControlOrdering,
    direct_client: crate::RpcClientBudget,
    events: RpcEventBroker,
    mailbox: StdMutex<Mailbox>,
    urgent_slots: Arc<AtomicUsize>,
    bulk_slots: Arc<AtomicUsize>,
    reply_slots: Arc<AtomicUsize>,
    urgent_capacity: usize,
    bulk_capacity: usize,
    urgent_burst: usize,
    wake: Arc<Notify>,
    task: StdMutex<Option<JoinHandle<()>>>,
    closing: AtomicBool,
    force: AtomicBool,
    stopped: AtomicBool,
    shutdown: watch::Sender<bool>,
    failure: watch::Sender<Option<String>>,
    metrics: Metrics,
}

impl SharedRuntime {
    fn close_admission(&self) {
        let _mailbox = self
            .mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.closing.store(true, Ordering::Release);
    }
}

impl Drop for SharedRuntime {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

#[derive(Default)]
struct Mailbox {
    urgent: VecDeque<Envelope>,
    bulk: VecDeque<Envelope>,
}

struct Credit(Arc<AtomicUsize>);
impl Credit {
    fn acquire(used: &Arc<AtomicUsize>, limit: usize) -> Result<Self, HttpControlError> {
        used.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            (used < limit).then_some(used + 1)
        })
        .map_err(|_| HttpControlError::Busy)?;
        Ok(Self(used.clone()))
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

struct Caller {
    reply: oneshot::Sender<Result<Value, HttpControlError>>,
    _request: crate::rpc_budget::RpcRequestLease,
    _credit: Credit,
}

struct Envelope {
    method: String,
    params: Value,
    context: crate::RpcClientContext,
    sequence: u64,
    captured: Option<control_ops::PreparedBulk>,
    target: Option<Gid>,
    callers: VecDeque<Caller>,
    slot: Credit,
    plane: Arc<Mutex<HttpControlPlane>>,
}

struct PendingReplies {
    receiver: Option<oneshot::Receiver<Result<Value, HttpControlError>>>,
    result: Option<Result<Value, HttpControlError>>,
    callers: VecDeque<Caller>,
    _slot: Credit,
    _plane: Arc<Mutex<HttpControlPlane>>,
}

impl PendingReplies {
    /// Exactly one caller is delivered per turn, including coalesced replies.
    fn poll(&mut self) -> bool {
        if let Some(receiver) = &mut self.receiver {
            match receiver.try_recv() {
                Ok(result) => {
                    self.result = Some(result);
                    self.receiver = None;
                }
                Err(oneshot::error::TryRecvError::Empty) => return false,
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.result = Some(Err(HttpControlError::Persistence(
                        "mutation owner stopped".to_owned(),
                    )));
                    self.receiver = None;
                }
            }
        }
        if let Some(caller) = self.callers.pop_front() {
            let _ = caller
                .reply
                .send(self.result.as_ref().expect("completed result").clone());
        }
        self.callers.is_empty()
    }
}

impl ControlRuntime {
    pub(super) fn new(plane: &Arc<Mutex<HttpControlPlane>>, owner: &HttpControlPlane) -> Self {
        let limits = owner
            .process_resources
            .as_ref()
            .map(|resources| resources.profile())
            .unwrap_or_else(|| {
                ariax_runtime::ResolvedRuntimeProfile::resolve(
                    ariax_runtime::RuntimeProfile::Auto,
                    None,
                )
            })
            .limits();
        let (shutdown, _) = watch::channel(false);
        let (failure, _) = watch::channel(None);
        Self {
            shared: Arc::new(SharedRuntime {
                plane: Arc::downgrade(plane),
                queries: owner.query_reader(),
                ordering: owner.control_order.clone(),
                direct_client: owner.direct_client.clone(),
                events: owner.events.clone(),
                mailbox: StdMutex::new(Mailbox::default()),
                urgent_slots: Arc::new(AtomicUsize::new(0)),
                bulk_slots: Arc::new(AtomicUsize::new(0)),
                reply_slots: Arc::new(AtomicUsize::new(0)),
                urgent_capacity: limits.control_urgent_capacity,
                bulk_capacity: limits.control_bulk_capacity,
                urgent_burst: limits.urgent_burst,
                wake: Arc::new(Notify::new()),
                task: StdMutex::new(None),
                closing: AtomicBool::new(false),
                force: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                shutdown,
                failure,
                metrics: Metrics::default(),
            }),
        }
    }

    pub(crate) fn start(&self) -> Result<(), HttpControlError> {
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(HttpControlError::Busy);
        }
        let mut task = self
            .shared
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if task.is_none() {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|_| HttpControlError::InvalidConfig)?;
            *task =
                Some(runtime.spawn(run(Arc::downgrade(&self.shared), self.shared.wake.clone())));
        }
        Ok(())
    }

    pub(super) fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shared.shutdown.subscribe()
    }
    pub(super) fn failure_receiver(&self) -> watch::Receiver<Option<String>> {
        self.shared.failure.subscribe()
    }

    pub(super) fn metrics(&self) -> ControlRuntimeMetrics {
        let metrics = &self.shared.metrics;
        ControlRuntimeMetrics {
            turns: metrics.turns.load(Ordering::Relaxed),
            total_lock_wait_us: metrics.wait.load(Ordering::Relaxed),
            max_lock_wait_us: metrics.max_wait.load(Ordering::Relaxed),
            max_turn_us: metrics.max_turn.load(Ordering::Relaxed),
            max_steps: metrics.max_steps.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn call(
        &self,
        method: &str,
        params: Value,
        context: crate::RpcClientContext,
    ) -> Result<Value, HttpControlError> {
        if query::is_query(method) {
            return self.shared.queries.call(method, params, context).await;
        }
        self.start()?;
        match self.admit(method, params, context)? {
            ControlReply::Ready(value) => Ok(value),
            ControlReply::Deferred(reply) => reply
                .await
                .map_err(|_| HttpControlError::Persistence("control runtime stopped".to_owned()))?,
        }
    }

    fn admit(
        &self,
        method: &str,
        mut params: Value,
        context: crate::RpcClientContext,
    ) -> Result<ControlReply, HttpControlError> {
        let request = match context.request_lease() {
            Some(request) => request,
            None => self
                .shared
                .direct_client
                .try_request(0)
                .map_err(|_| HttpControlError::Busy)?,
        };
        let request = request
            .reserve_command(
                crate::rpc_json::command_value_bytes(&params).saturating_add(64 * 1024),
            )
            .map_err(|_| HttpControlError::Busy)?;
        let name = method.strip_prefix("aria2.").unwrap_or(method);
        if matches!(name, "shutdown" | "forceShutdown") {
            require_no_params(&params, name)?;
            self.shared
                .force
                .fetch_or(name == "forceShutdown", Ordering::AcqRel);
            self.shared.close_admission();
            if let Ok(event) = RpcEvent::notification(
                "ariax.onShutdown",
                json!({"force": self.shared.force.load(Ordering::Acquire)}),
                RpcEventClass::Reliable,
                None,
            ) {
                self.shared.events.publish(event);
            }
            self.shared.shutdown.send_replace(true);
            self.shared.wake.notify_one();
            return Ok(ControlReply::Ready(Value::String("OK".to_owned())));
        }
        if self.shared.closing.load(Ordering::Acquire) {
            return Err(HttpControlError::Busy);
        }
        let reply_credit = Credit::acquire(
            &self.shared.reply_slots,
            (self.shared.urgent_capacity + self.shared.bulk_capacity).saturating_mul(4),
        )?;
        let (send, receiver) = oneshot::channel();
        let caller = Caller {
            reply: send,
            _request: request.clone(),
            _credit: reply_credit,
        };
        let root = self
            .shared
            .queries
            .current()
            .ok_or(HttpControlError::Busy)?;
        let coalesce = matches!(name, "pause" | "forcePause" | "remove" | "forceRemove")
            .then(|| root.resolve_gid_param(&params))
            .transpose()?;
        let urgent = control_ops::is_task_control(method);
        // A noncoalescing control still orders actions for its task. Preserve
        // its dispatch-time validation, but resolve its identity for this
        // mailbox barrier when possible. An unresolved identity is a barrier
        // for every task, so malformed input cannot permit reordering.
        let target = coalesce.or_else(|| {
            urgent.then(|| {
                params
                    .as_array()?
                    .first()?
                    .as_str()
                    .and_then(|gid| root.resolve_gid_text(gid).ok())
            })?
        });
        let mut mailbox = self
            .shared
            .mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.shared.closing.load(Ordering::Acquire) {
            return Err(HttpControlError::Busy);
        }
        let captured = if control_ops::is_bulk_control(method) {
            require_no_params(&params, name)?;
            Some(
                self.shared
                    .ordering
                    .capture(&root.applied_root(), method, &request)?,
            )
        } else {
            None
        };
        let sequence = match &captured {
            Some(captured) => captured.sequence,
            None => self.shared.ordering.next()?,
        };
        if let Some(gid) = coalesce {
            params[0] = Value::String(gid.to_string());
        }
        if let Some(gid) = coalesce
            && let Some(queued) = mailbox
                .urgent
                .iter_mut()
                .rev()
                .find(|queued| queued.target.is_none() || queued.target == Some(gid))
            && queued.target == Some(gid)
            && let Some(merged) = coalesced_method(&queued.method, method)
        {
            queued.method = merged.to_owned();
            queued.sequence = sequence;
            queued.callers.push_back(caller);
        } else {
            let slot = if urgent {
                Credit::acquire(&self.shared.urgent_slots, self.shared.urgent_capacity)?
            } else {
                Credit::acquire(&self.shared.bulk_slots, self.shared.bulk_capacity)?
            };
            let context = context.with_request(request);
            let envelope = Envelope {
                method: method.to_owned(),
                params,
                context,
                sequence,
                captured,
                target,
                callers: VecDeque::from([caller]),
                slot,
                plane: self.shared.plane.upgrade().ok_or(HttpControlError::Busy)?,
            };
            if urgent {
                mailbox.urgent.push_back(envelope);
            } else {
                mailbox.bulk.push_back(envelope);
            }
        }
        drop(mailbox);
        self.shared.wake.notify_one();
        Ok(ControlReply::Deferred(receiver))
    }

    /// Called after transport drain, before recovering the sole mutable owner.
    pub(crate) async fn drain(&self) -> Result<(), HttpControlError> {
        self.shared.close_admission();
        if !self.shared.stopped.load(Ordering::Acquire) {
            self.start()?;
        }
        self.shared.wake.notify_one();
        let deadline = Instant::now() + crate::DEFAULT_HTTP_RPC_SHUTDOWN_TIMEOUT;
        let mut result = Ok(());
        while self.shared.urgent_slots.load(Ordering::Acquire) != 0
            || self.shared.bulk_slots.load(Ordering::Acquire) != 0
        {
            if let Some(error) = self.shared.failure.borrow().clone() {
                result = Err(HttpControlError::Persistence(error));
            }
            if Instant::now() >= deadline {
                result = Err(HttpControlError::Persistence(
                    "control runtime drain exceeded its deadline".to_owned(),
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        self.shared.stopped.store(true, Ordering::Release);
        let task = self
            .shared
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        reject_queued(
            &self.shared,
            &mut VecDeque::new(),
            HttpControlError::Persistence("control runtime stopped during drain".to_owned()),
        )
        .await;
        if let Some(plane) = self.shared.plane.upgrade() {
            match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), plane.lock())
                .await
            {
                Ok(mut owner) => {
                    owner.shutdown_requested = true;
                    owner.force_shutdown_requested |= self.shared.force.load(Ordering::Acquire);
                }
                Err(_) => {
                    result = Err(HttpControlError::Persistence(
                        "control owner drain exceeded its deadline".to_owned(),
                    ))
                }
            }
        }
        if let Some(error) = self.shared.failure.borrow().clone() {
            result = Err(HttpControlError::Persistence(error));
        }
        result
    }
}

async fn reject_queued(
    runtime: &SharedRuntime,
    pending: &mut VecDeque<PendingReplies>,
    error: HttpControlError,
) {
    let mut delivered = 0;
    loop {
        if pending.is_empty() {
            let command = {
                let mut mailbox = runtime
                    .mailbox
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                mailbox
                    .urgent
                    .pop_front()
                    .or_else(|| mailbox.bulk.pop_front())
            };
            let Some(command) = command else {
                return;
            };
            pending.push_back(PendingReplies {
                receiver: None,
                result: Some(Err(error.clone())),
                callers: command.callers,
                _slot: command.slot,
                _plane: command.plane,
            });
        }
        let mut reply = pending.pop_front().expect("pending rejection");
        reply.receiver = None;
        reply.result = Some(Err(error.clone()));
        if !reply.poll() {
            pending.push_front(reply);
        }
        delivered += 1;
        if delivered == 32 {
            delivered = 0;
            tokio::task::yield_now().await;
        }
    }
}

fn coalesced_method(queued: &str, incoming: &str) -> Option<&'static str> {
    let queued = queued.strip_prefix("aria2.").unwrap_or(queued);
    let incoming = incoming.strip_prefix("aria2.").unwrap_or(incoming);
    match (queued, incoming) {
        ("pause" | "forcePause", "pause" | "forcePause") => {
            Some(if queued == "forcePause" || incoming == "forcePause" {
                "aria2.forcePause"
            } else {
                "aria2.pause"
            })
        }
        ("pause" | "forcePause", "remove" | "forceRemove") => Some(if incoming == "forceRemove" {
            "aria2.forceRemove"
        } else {
            "aria2.remove"
        }),
        ("remove" | "forceRemove", "remove" | "forceRemove") => {
            Some(if queued == "forceRemove" || incoming == "forceRemove" {
                "aria2.forceRemove"
            } else {
                "aria2.remove"
            })
        }
        _ => None,
    }
}

async fn run(shared: Weak<SharedRuntime>, wake: Arc<Notify>) {
    let mut pending = VecDeque::<PendingReplies>::new();
    let mut urgent_streak = 0;
    loop {
        let Some(runtime) = shared.upgrade() else {
            return;
        };
        if runtime.stopped.load(Ordering::Acquire) {
            return;
        }
        let Some(plane) = runtime.plane.upgrade() else {
            return;
        };
        let waiting = Instant::now();
        let (result, progressed) = {
            let mut owner = plane.lock().await;
            let started = Instant::now();
            let wait = u64::try_from(waiting.elapsed().as_micros()).unwrap_or(u64::MAX);
            if runtime.closing.load(Ordering::Acquire) {
                if !owner.shutdown_requested {
                    owner.shutdown_requested = true;
                    owner.force_shutdown_requested = runtime.force.load(Ordering::Acquire);
                    owner.publish_control_event("aria2.shutdown", &Value::String("OK".to_owned()));
                }
                owner.force_shutdown_requested |= runtime.force.load(Ordering::Acquire);
            }
            let result = progress(&runtime, &mut owner, &mut pending, &mut urgent_streak);
            runtime.metrics.turns.fetch_add(1, Ordering::Relaxed);
            runtime.metrics.wait.fetch_add(wait, Ordering::Relaxed);
            runtime.metrics.max_wait.fetch_max(wait, Ordering::Relaxed);
            runtime.metrics.max_turn.fetch_max(
                u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            runtime
                .metrics
                .max_steps
                .fetch_max(owner.turn.used, Ordering::Relaxed);
            (result, owner.turn.progressed)
        };
        if let Err(error) = result {
            runtime.close_admission();
            runtime.failure.send_replace(Some(error.to_string()));
            runtime.shutdown.send_replace(true);
            drop(plane);
            reject_queued(&runtime, &mut pending, error).await;
            runtime.stopped.store(true, Ordering::Release);
            return;
        }
        drop(plane);
        drop(runtime);
        if progressed {
            tokio::task::yield_now().await;
        } else {
            tokio::select! { () = wake.notified() => {}, () = tokio::time::sleep(Duration::from_millis(1)) => {} }
        }
    }
}

fn progress(
    runtime: &SharedRuntime,
    owner: &mut HttpControlPlane,
    pending: &mut VecDeque<PendingReplies>,
    urgent_streak: &mut usize,
) -> Result<(), HttpControlError> {
    owner.turn = OwnerTurn::new();
    // Hard completions progress independently of external admission pressure.
    match owner.poll_once_inner() {
        Ok(()) | Err(HttpControlError::Busy) => {}
        Err(error) => return Err(error),
    }
    owner.publish_query();
    for _ in 0..pending.len().min(32) {
        if !owner.turn.take_step() {
            break;
        }
        let mut reply = pending.pop_front().expect("pending reply");
        let complete = reply.poll();
        if reply.result.is_some() {
            owner.turn.mark_progress();
        }
        if !complete {
            pending.push_back(reply);
        }
    }
    if !owner.engine_idle() || owner.pending_mutation.is_some() || owner.admission_fenced() {
        return Ok(());
    }
    if !owner.turn.take_step() {
        return Ok(());
    }
    let command = {
        let mut mailbox = runtime
            .mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let preparation_pending =
            owner.pending_admission.is_some() || owner.pending_configuration.is_some();
        let bulk_ready =
            owner.pending_bulk.is_some() || (!preparation_pending && !mailbox.bulk.is_empty());
        if !mailbox.urgent.is_empty() && (*urgent_streak < runtime.urgent_burst || !bulk_ready) {
            *urgent_streak = urgent_streak.saturating_add(1);
            mailbox.urgent.pop_front()
        } else if owner.pending_bulk.is_some() {
            *urgent_streak = 0;
            None
        } else if preparation_pending {
            None
        } else {
            *urgent_streak = 0;
            mailbox.bulk.pop_front()
        }
    };
    let Some(command) = command else {
        owner.poll_bulk_control()?;
        owner.publish_query();
        return Ok(());
    };
    let Envelope {
        method,
        params,
        context,
        sequence,
        captured,
        callers,
        slot,
        plane,
        ..
    } = command;
    owner.turn.mark_progress();
    owner.dispatch_sequence = Some(sequence);
    owner.dispatch_bulk = captured;
    let result = if method == "ariax.subscribe" {
        owner
            .reserve_command_memory(&method, &params, context.request_lease().as_ref())
            .and_then(|_command| {
                owner.subscribe_events_with_client(params, context.client_budget())
            })
            .map(ControlReply::Ready)
    } else if matches!(
        method.as_str(),
        "aria2.changeUri" | "changeUri" | "ariax.replaceSources"
    ) {
        (|| {
            let command =
                owner.reserve_command_memory(&method, &params, context.request_lease().as_ref())?;
            let work = owner.reserve_scheduler_work(command.as_ref(), 0)?;
            let result = owner
                .begin_source_call(&method, params, command)
                .map(ControlReply::Deferred);
            owner.retain_pending_work(Some(work))?;
            result
        })()
    } else {
        owner.begin_call_admitted(&method, params, context.request_lease())
    };
    owner.dispatch_sequence = None;
    owner.dispatch_bulk = None;
    owner.publish_query();
    let (receiver, result) = match result {
        Ok(ControlReply::Deferred(receiver)) => (Some(receiver), None),
        Ok(ControlReply::Ready(value)) => (None, Some(Ok(value))),
        Err(error) => (None, Some(Err(error))),
    };
    pending.push_back(PendingReplies {
        receiver,
        result,
        callers,
        _slot: slot,
        _plane: plane,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{TestDirectory, add_paused};
    use super::*;

    fn context(client: &crate::RpcClientBudget) -> crate::RpcClientContext {
        crate::RpcClientContext::default().with_request(client.try_request(0).expect("request"))
    }

    fn setup(
        plane: HttpControlPlane,
        urgent: usize,
        bulk: usize,
        burst: usize,
    ) -> (Arc<Mutex<HttpControlPlane>>, ControlRuntime) {
        let plane = Arc::new(Mutex::new(plane));
        let mut runtime = ControlRuntime::new(&plane, &plane.try_lock().expect("owner"));
        let shared = Arc::get_mut(&mut runtime.shared).expect("unshared runtime");
        shared.urgent_capacity = urgent;
        shared.bulk_capacity = bulk;
        shared.urgent_burst = burst;
        (plane, runtime)
    }

    #[tokio::test]
    async fn full_urgent_lane_coalesces_force_and_remove_and_shutdown_stays_out_of_band() {
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let gids = (0..3).map(|_| add_paused(&mut owner)).collect::<Vec<_>>();
        let client = owner.rpc_budgets.client().expect("client");
        let extra = owner.rpc_budgets.client().expect("extra client");
        let (plane, runtime) = setup(owner, 2, 2, 2);
        let mut replies = Vec::new();
        for (method, gid, who) in [
            ("aria2.pause", gids[0], &client),
            ("aria2.pause", gids[1], &client),
            ("aria2.forcePause", gids[0], &client),
            ("aria2.remove", gids[0], &client),
        ] {
            replies.push(
                runtime
                    .admit(method, json!([gid.to_string()]), context(who))
                    .expect("admit/coalesce"),
            );
        }
        assert!(matches!(
            runtime.admit("aria2.pause", json!([gids[2].to_string()]), context(&extra)),
            Err(HttpControlError::Busy)
        ));
        {
            let mailbox = runtime.shared.mailbox.lock().expect("mailbox");
            assert_eq!(mailbox.urgent.len(), 2);
            assert_eq!(mailbox.urgent[0].method, "aria2.remove");
            assert_eq!(mailbox.urgent[0].callers.len(), 3);
        }
        assert!(
            matches!(runtime.admit("aria2.forceShutdown", json!([]), context(&extra)), Ok(ControlReply::Ready(value)) if value == "OK")
        );
        assert!(*runtime.shutdown_receiver().borrow());
        assert_eq!(runtime.shared.urgent_slots.load(Ordering::Acquire), 2);
        drop(replies); // Accepted mutations must finish without any reply consumers.
        runtime.drain().await.expect("drain full lanes");
        runtime.drain().await.expect("idempotent drain");
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(extra.outstanding_requests(), 0);
        let owner = Arc::try_unwrap(plane).expect("sole owner").into_inner();
        assert!(owner.force_shutdown_requested());
        assert_eq!(
            owner
                .capture_query()
                .tell_status(json!([gids[0].to_string()]))
                .expect("removed")["status"],
            "removed"
        );
        owner.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn ready_controls_and_rejections_do_not_require_timer_ticks() {
        let directory = TestDirectory::new();
        let owner = directory.control_plane();
        let (plane, runtime) = setup(owner, 2, 2, 2);
        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let subscribe_runtime = runtime.clone();
        let subscribed = tokio::spawn(async move {
            subscribe_runtime
                .call(
                    "ariax.subscribe",
                    json!([]),
                    crate::RpcClientContext::default(),
                )
                .await
        });
        let rejected_runtime = runtime.clone();
        let rejected = tokio::spawn(async move {
            rejected_runtime
                .call(
                    "ariax.unknown",
                    json!([]),
                    crate::RpcClientContext::default(),
                )
                .await
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while !subscribed.is_finished() || !rejected.is_finished() {
            assert!(
                Instant::now() < deadline,
                "ready work cannot wait for the paused timer"
            );
            // Keep the executor runnable so its paused clock cannot auto-advance.
            tokio::task::yield_now().await;
        }
        assert!(
            subscribed
                .await
                .expect("subscribe task")
                .expect("subscription")["subscriptionId"]
                .is_string()
        );
        assert!(matches!(
            rejected.await.expect("rejected task"),
            Err(HttpControlError::Unsupported(_))
        ));
        assert_eq!(tokio::time::Instant::now(), started);
        runtime.drain().await.expect("drain");
        Arc::try_unwrap(plane)
            .expect("sole owner")
            .into_inner()
            .shutdown()
            .expect("shutdown");
    }

    #[tokio::test]
    async fn coalescing_preserves_intervening_task_actions_and_lane_limits() {
        for capacity in [2, 3] {
            let directory = TestDirectory::new();
            let mut owner = directory.control_plane();
            let gid = add_paused(&mut owner);
            let client = owner.rpc_budgets.client().expect("client");
            let (plane, runtime) = setup(owner, capacity, 2, 2);
            let mut replies = Vec::new();
            for method in ["aria2.pause", "aria2.unpause"] {
                replies.push(
                    runtime
                        .admit(method, json!([gid.to_string()]), context(&client))
                        .expect("ordered control"),
                );
            }
            let last = runtime.admit("aria2.pause", json!([gid.to_string()]), context(&client));
            if capacity == 2 {
                assert!(matches!(last, Err(HttpControlError::Busy)));
            } else {
                replies.push(last.expect("later pause uses its own slot"));
            }
            assert_eq!(
                runtime.shared.mailbox.lock().expect("mailbox").urgent.len(),
                capacity
            );
            runtime.drain().await.expect("drain ordered controls");
            for reply in replies {
                let ControlReply::Deferred(reply) = reply else {
                    panic!("queued control must have a deferred reply");
                };
                assert_eq!(
                    reply.await.expect("reply").expect("control"),
                    gid.to_string()
                );
            }
            assert_eq!(client.outstanding_requests(), 0);
            let owner = Arc::try_unwrap(plane).expect("sole owner").into_inner();
            assert_eq!(
                owner
                    .capture_query()
                    .tell_status(json!([gid.to_string()]))
                    .expect("status")["status"],
                if capacity == 2 { "waiting" } else { "paused" }
            );
            owner.shutdown().expect("shutdown");
        }
    }

    #[tokio::test]
    async fn accepted_commands_outlive_the_callers_last_backend_handle() {
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let gid = add_paused(&mut owner);
        let client = owner.rpc_budgets.client().expect("client");
        let backend = HttpControlBackend::new(owner);
        let plane = backend.plane();
        backend.start_control_runtime().expect("start runtime");
        let reply = backend
            .control
            .admit("aria2.unpause", json!([gid.to_string()]), context(&client))
            .expect("accepted command");
        drop(reply);
        drop(backend);
        let deadline = Instant::now() + Duration::from_secs(2);
        while client.outstanding_requests() != 0 {
            assert!(
                Instant::now() < deadline,
                "owner must retain admitted commands"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let runtime = ControlRuntime {
            shared: plane
                .lock()
                .await
                .managed_runtime
                .clone()
                .expect("owner retains runtime"),
        };
        runtime.drain().await.expect("drain");
        let owner = Arc::try_unwrap(plane).expect("sole owner").into_inner();
        assert_eq!(
            owner
                .capture_query()
                .tell_status(json!([gid.to_string()]))
                .expect("resumed")["status"],
            "waiting"
        );
        owner.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn admitted_envelope_retains_the_last_owner_until_its_write_completes() {
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let gid = add_paused(&mut owner);
        let session = owner.session.clone();
        let client = owner.rpc_budgets.client().expect("client");
        let backend = HttpControlBackend::new(owner);
        let weak = Arc::downgrade(&backend.plane());
        backend.start_control_runtime().expect("start");
        let reply = backend
            .control
            .admit("aria2.unpause", json!([gid.to_string()]), context(&client))
            .expect("admit");
        drop(reply);
        drop(backend);
        let deadline = Instant::now() + Duration::from_secs(3);
        while client.outstanding_requests() != 0 || weak.strong_count() != 0 {
            assert!(
                Instant::now() < deadline,
                "accepted work finishes and releases its last owner"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            matches!(session.execute(SessionCommand::ReadQueueOrder { state: SessionQueueState::Waiting }).expect("durable resume"), SessionCommandResult::QueueOrder(gids) if gids == [gid])
        );
        session.shutdown().expect("close observed session");
    }

    #[test]
    fn urgent_burst_services_queued_bulk_before_the_urgent_stream_finishes() {
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let gids = (0..4).map(|_| add_paused(&mut owner)).collect::<Vec<_>>();
        let urgent = owner.rpc_budgets.client().expect("urgent client");
        let bulk = owner.rpc_budgets.client().expect("bulk client");
        let (plane, runtime) = setup(owner, 4, 2, 2);
        let mut replies = Vec::new();
        for gid in &gids {
            replies.push(
                runtime
                    .admit("aria2.pause", json!([gid.to_string()]), context(&urgent))
                    .expect("urgent"),
            );
        }
        replies.push(
            runtime
                .admit(
                    "aria2.changeGlobalOption",
                    json!([{"timeout":45}]),
                    context(&bulk),
                )
                .expect("bulk"),
        );
        let mut owner = plane.try_lock().expect("owner");
        let mut pending = VecDeque::new();
        let mut streak = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while owner.pending_configuration.is_none() && owner.config_generation == 0 {
            assert!(Instant::now() < deadline, "bulk admission is not starved");
            progress(&runtime.shared, &mut owner, &mut pending, &mut streak).expect("bounded turn");
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
        assert_eq!(
            runtime.shared.mailbox.lock().expect("mailbox").urgent.len(),
            2
        );
        while !pending.is_empty()
            || !runtime
                .shared
                .mailbox
                .lock()
                .expect("mailbox")
                .urgent
                .is_empty()
        {
            assert!(Instant::now() < deadline, "all admitted calls complete");
            progress(&runtime.shared, &mut owner, &mut pending, &mut streak)
                .expect("finish replies");
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
        assert_eq!(owner.config_generation, 1);
        assert!(pending.is_empty());
        assert_eq!(urgent.outstanding_requests(), 0);
        assert_eq!(bulk.outstanding_requests(), 0);
        drop(replies);
        drop(owner);
        Arc::try_unwrap(plane)
            .expect("sole owner")
            .into_inner()
            .shutdown()
            .expect("shutdown");
    }

    #[test]
    fn queued_bulk_uses_admission_membership_and_later_urgent_intent() {
        let directory = TestDirectory::new();
        let mut owner = directory.control_plane();
        let gid = add_paused(&mut owner);
        let client = owner.rpc_budgets.client().expect("client");
        let (plane, runtime) = setup(owner, 2, 2, 2);
        let bulk = runtime
            .admit("aria2.unpauseAll", json!([]), context(&client))
            .expect("capture bulk");
        let pause = runtime
            .admit("aria2.pause", json!([gid.to_string()]), context(&client))
            .expect("later pause");
        let mut owner = plane.try_lock().expect("owner");
        let added_later = add_paused(&mut owner);
        let mut pending = VecDeque::new();
        let mut streak = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while runtime.shared.urgent_slots.load(Ordering::Acquire) != 0
            || runtime.shared.bulk_slots.load(Ordering::Acquire) != 0
        {
            assert!(Instant::now() < deadline, "captured bulk completes");
            progress(&runtime.shared, &mut owner, &mut pending, &mut streak).expect("progress");
            std::thread::park_timeout(CONTROL_PROGRESS_POLL);
        }
        assert!(pending.is_empty());
        assert!(owner.pending_bulk.is_none());
        for gid in [gid, added_later] {
            assert_eq!(
                owner
                    .capture_query()
                    .tell_status(json!([gid.to_string()]))
                    .expect("status")["status"],
                "paused"
            );
        }
        assert_eq!(owner.control_order.retained_identities(), 0);
        assert_eq!(client.outstanding_requests(), 0);
        drop((bulk, pause));
        drop(owner);
        Arc::try_unwrap(plane)
            .expect("sole owner")
            .into_inner()
            .shutdown()
            .expect("shutdown");
    }
}
