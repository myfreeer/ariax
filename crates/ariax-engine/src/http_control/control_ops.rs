//! Durable, resumable task control acknowledgements.

use super::*;

pub(super) fn is_task_control(method: &str) -> bool {
    matches!(
        method.strip_prefix("aria2.").unwrap_or(method),
        "pause"
            | "forcePause"
            | "unpause"
            | "remove"
            | "forceRemove"
            | "removeDownloadResult"
            | "changePosition"
            | "ariax.approveHostKey"
    )
}

impl HttpControlPlane {
    pub(super) fn begin_task_control(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<ControlReply, HttpControlError> {
        let sequence = self.next_control_sequence()?;
        self.begin_task_control_ordered(method, params, sequence, true)
    }

    fn begin_task_control_ordered(
        &mut self,
        method: &str,
        params: Value,
        sequence: u64,
        explicit: bool,
    ) -> Result<ControlReply, HttpControlError> {
        if !self.engine_idle() || self.pending_mutation.is_some() || self.admission_fenced() {
            return Err(HttpControlError::Busy);
        }
        let name = method.strip_prefix("aria2.").unwrap_or(method);
        let mut remove_task = None;
        let mut replacement = None;
        let (command, response) = if name == "ariax.approveHostKey" {
            let values = params.as_array().filter(|values| values.len() == 3).ok_or(
                HttpControlError::InvalidParams(
                    "approveHostKey requires GID, challenge id, and SHA-256 fingerprint",
                ),
            )?;
            let gid = self.resolve_gid_text(
                values[0]
                    .as_str()
                    .ok_or(HttpControlError::InvalidParams("GID must be a string"))?,
            )?;
            let challenge =
                crate::transfer_task::parse_hex_bytes::<16>(values[1].as_str().unwrap_or_default())
                    .map(ariax_core::HostKeyChallengeId::new)
                    .map_err(HttpControlError::TaskSpec)?;
            let fingerprint = crate::transfer_task::parse_host_key_fingerprint(
                values[2].as_str().unwrap_or_default(),
            )
            .map(ariax_core::HostKeyFingerprint::new)
            .map_err(HttpControlError::TaskSpec)?;
            replacement = Some(Box::new(self.pinned_host_key_spec(gid, fingerprint)?));
            (
                SchedulerCommand::ApproveHostKey {
                    gid,
                    challenge,
                    fingerprint_sha256: fingerprint,
                },
                Value::String(gid.to_string()),
            )
        } else if name == "changePosition" {
            let values = params.as_array().filter(|values| values.len() == 3).ok_or(
                HttpControlError::InvalidParams("changePosition requires GID, position, and mode"),
            )?;
            let gid = self.resolve_gid_text(
                values[0]
                    .as_str()
                    .ok_or(HttpControlError::InvalidParams("GID must be a string"))?,
            )?;
            let requested = parse_i64(&values[1], "position")?;
            let mode = values[2].as_str().ok_or(HttpControlError::InvalidParams(
                "position mode must be a string",
            ))?;
            let root = self.engine.snapshot_reader().load();
            let (order, current) = queue_order_and_position(&root, gid)?;
            let last = i64::try_from(order.len().saturating_sub(1)).unwrap_or(i64::MAX);
            let target = match mode {
                "POS_SET" => requested,
                "POS_CUR" => i64::try_from(current)
                    .unwrap_or(i64::MAX)
                    .saturating_add(requested),
                "POS_END" => last.saturating_add(requested),
                _ => return Err(HttpControlError::InvalidParams("invalid position mode")),
            }
            .clamp(0, last);
            let position = usize::try_from(target)
                .map_err(|_| HttpControlError::InvalidParams("position is out of range"))?;
            (
                SchedulerCommand::ChangePosition { gid, position },
                Value::from(position),
            )
        } else {
            let gid = self.resolve_gid_param(&params)?;
            let command = match name {
                "pause" | "forcePause" => SchedulerCommand::Pause {
                    gid,
                    force: name == "forcePause",
                },
                "unpause" => SchedulerCommand::Resume { gid },
                "remove" | "forceRemove" => SchedulerCommand::Remove {
                    gid,
                    force: name == "forceRemove",
                },
                "removeDownloadResult" => {
                    remove_task = Some(
                        self.engine
                            .snapshot_reader()
                            .load()
                            .task(gid)
                            .ok_or(HttpControlError::NotFound)?
                            .task_id,
                    );
                    SchedulerCommand::RemoveStoppedResult { gid }
                }
                _ => return Err(HttpControlError::Unsupported("method not found")),
            };
            let response = if remove_task.is_some() {
                Value::String("OK".to_owned())
            } else {
                Value::String(gid.to_string())
            };
            (command, response)
        };
        let identity = match &command {
            SchedulerCommand::Pause { gid, .. }
            | SchedulerCommand::Resume { gid }
            | SchedulerCommand::ApproveHostKey { gid, .. }
            | SchedulerCommand::Remove { gid, .. }
            | SchedulerCommand::RemoveStoppedResult { gid } => self
                .engine
                .snapshot_reader()
                .load()
                .task(*gid)
                .map(|task| task.task_id),
            _ => None,
        };
        self.prepare_and_begin_command(command)?;
        if explicit && let Some(task) = identity {
            self.control_order.mark(task, sequence);
        }
        let (reply, receiver) = oneshot::channel();
        self.pending_mutation = Some(PendingMutation {
            publication: MutationPublication::Control {
                response,
                remove_task,
                readmit: matches!(name, "unpause" | "ariax.approveHostKey"),
                replacement,
            },
            reply,
        });
        Ok(ControlReply::Deferred(receiver))
    }

    pub(super) fn pinned_host_key_spec(
        &self,
        gid: Gid,
        fingerprint: ariax_core::HostKeyFingerprint,
    ) -> Result<HttpTaskSpec, HttpControlError> {
        let spec = self.tasks.get_gid(gid).ok_or(HttpControlError::NotFound)?;
        let mut options = spec.options().clone();
        options.transfer.sftp_host_key_sha256 =
            Some(ariax_storage::session_host_key_pin_value(fingerprint));
        options.transfer.sftp_check_host_key = true;
        let replacement = spec
            .with_options(spec.output().clone(), options)
            .map_err(HttpControlError::TaskSpec)?;
        self.tasks
            .snapshot()
            .reserve_spec(replacement)
            .map_err(|_| HttpControlError::Busy)
    }
}

pub(super) fn is_bulk_control(method: &str) -> bool {
    matches!(
        method.strip_prefix("aria2.").unwrap_or(method),
        "pauseAll" | "forcePauseAll" | "unpauseAll" | "purgeDownloadResult"
    )
}

#[derive(Clone, Default)]
pub(super) struct ControlOrdering {
    inner: Arc<std::sync::Mutex<OrderState>>,
}

#[derive(Default)]
struct OrderState {
    sequence: u64,
    tasks: BTreeMap<TaskId, std::sync::Weak<std::sync::atomic::AtomicU64>>,
}

impl ControlOrdering {
    pub(super) fn next(&self) -> Result<u64, HttpControlError> {
        let mut order = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        order.sequence = order
            .sequence
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        Ok(order.sequence)
    }

    fn mark(&self, task: TaskId, sequence: u64) {
        let order = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(latest) = order.tasks.get(&task).and_then(std::sync::Weak::upgrade) {
            latest.fetch_max(sequence, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn prune(&self) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .retain(|_, latest| latest.strong_count() != 0);
    }

    #[cfg(test)]
    pub(super) fn retained_identities(&self) -> usize {
        self.prune();
        self.inner.lock().expect("ordering").tasks.len()
    }

    pub(super) fn capture(
        &self,
        root: &ariax_runtime::StatusSnapshotRoot,
        method: &str,
        request: &crate::rpc_budget::RpcRequestLease,
    ) -> Result<PreparedBulk, HttpControlError> {
        let (classes, _) = bulk_definition(method)?;
        let count = classes
            .iter()
            .map(|class| root.queue(*class).len())
            .sum::<usize>();
        let reservation = request
            .reserve_command(count.saturating_mul(256).saturating_add(4096))
            .map_err(|_| HttpControlError::Busy)?;
        let mut order = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        order.tasks.retain(|_, latest| latest.strong_count() != 0);
        order.sequence = order
            .sequence
            .checked_add(1)
            .ok_or(HttpControlError::InvalidConfig)?;
        let sequence = order.sequence;
        let targets = classes
            .iter()
            .flat_map(|class| root.queue(*class))
            .map(|gid| {
                let task = root.task(*gid).expect("applied membership").task_id;
                let latest = order.tasks.entry(task).or_default();
                let cell = latest.upgrade().unwrap_or_else(|| {
                    let cell = Arc::new(std::sync::atomic::AtomicU64::new(0));
                    *latest = Arc::downgrade(&cell);
                    cell
                });
                BulkTarget {
                    gid: *gid,
                    task,
                    latest: cell,
                }
            })
            .collect();
        Ok(PreparedBulk {
            sequence,
            targets,
            ordering: self.clone(),
            _reservation: reservation,
        })
    }
}

struct BulkTarget {
    gid: Gid,
    task: TaskId,
    latest: Arc<std::sync::atomic::AtomicU64>,
}

pub(super) struct PreparedBulk {
    pub(super) sequence: u64,
    targets: VecDeque<BulkTarget>,
    ordering: ControlOrdering,
    _reservation: crate::rpc_budget::RpcRequestLease,
}

impl Drop for PreparedBulk {
    fn drop(&mut self) {
        self.targets.clear();
        self.ordering.prune();
    }
}

pub(super) struct PendingBulkControl {
    captured: PreparedBulk,
    method: &'static str,
    member: Option<oneshot::Receiver<Result<Value, HttpControlError>>>,
    reply: oneshot::Sender<Result<Value, HttpControlError>>,
    request: crate::rpc_budget::RpcRequestLease,
}

fn bulk_definition(
    method: &str,
) -> Result<(&'static [QueueClass], &'static str), HttpControlError> {
    match method.strip_prefix("aria2.").unwrap_or(method) {
        "pauseAll" => Ok((
            &[QueueClass::Active, QueueClass::Waiting, QueueClass::Demoted],
            "pause",
        )),
        "forcePauseAll" => Ok((
            &[QueueClass::Active, QueueClass::Waiting, QueueClass::Demoted],
            "forcePause",
        )),
        "unpauseAll" => Ok((&[QueueClass::Paused], "unpause")),
        "purgeDownloadResult" => Ok((&[QueueClass::Stopped], "removeDownloadResult")),
        _ => Err(HttpControlError::Unsupported("method not found")),
    }
}

impl HttpControlPlane {
    fn next_control_sequence(&mut self) -> Result<u64, HttpControlError> {
        self.dispatch_sequence
            .take()
            .map_or_else(|| self.control_order.next(), Ok)
    }

    pub(super) fn begin_bulk_control(
        &mut self,
        method: &str,
        params: Value,
        request: crate::rpc_budget::RpcRequestLease,
    ) -> Result<ControlReply, HttpControlError> {
        require_no_params(&params, method.strip_prefix("aria2.").unwrap_or(method))?;
        if self.pending_bulk.is_some() || !self.engine_idle() || self.pending_mutation.is_some() {
            return Err(HttpControlError::Busy);
        }
        let (_, member) = bulk_definition(method)?;
        let captured = match self.dispatch_bulk.take() {
            Some(captured) => captured,
            None => self.control_order.capture(
                &self.engine.snapshot_reader().load(),
                method,
                &request,
            )?,
        };
        let (reply, receiver) = oneshot::channel();
        self.pending_bulk = Some(PendingBulkControl {
            captured,
            method: member,
            member: None,
            reply,
            request,
        });
        Ok(ControlReply::Deferred(receiver))
    }

    /// One target/completion per turn; no entire-list loops or persistence waits.
    pub(super) fn poll_bulk_control(&mut self) -> Result<(), HttpControlError> {
        if !self.turn.take_step() {
            return Ok(());
        }
        if !self.engine_idle() || self.pending_mutation.is_some() || self.admission_fenced() {
            return Ok(());
        }
        let Some(mut bulk) = self.pending_bulk.take() else {
            return Ok(());
        };
        if let Some(mut member) = bulk.member.take() {
            match member.try_recv() {
                Ok(Ok(_)) => self.turn.mark_progress(),
                Err(oneshot::error::TryRecvError::Empty) => {
                    bulk.member = Some(member);
                }
                outcome => {
                    self.turn.mark_progress();
                    let error = match outcome {
                        Ok(Err(error)) => error,
                        _ => HttpControlError::Persistence("bulk member owner stopped".to_owned()),
                    };
                    let _ = bulk.reply.send(Err(error));
                    return Ok(());
                }
            }
            self.pending_bulk = Some(bulk);
            return Ok(());
        }
        let Some(target) = bulk.captured.targets.front() else {
            self.turn.mark_progress();
            let _ = bulk.reply.send(Ok(Value::String("OK".to_owned())));
            return Ok(());
        };
        let gid = target.gid;
        let current = self.engine.snapshot_reader().load();
        if current
            .task(gid)
            .is_none_or(|task| task.task_id != target.task)
            || target.latest.load(std::sync::atomic::Ordering::Relaxed) > bulk.captured.sequence
        {
            bulk.captured.targets.pop_front();
            self.turn.mark_progress();
            self.pending_bulk = Some(bulk);
            return Ok(());
        }
        let work = match self.reserve_scheduler_work(Some(&bulk.request), 0) {
            Ok(work) => work,
            Err(HttpControlError::Busy) => {
                self.pending_bulk = Some(bulk);
                return Ok(());
            }
            Err(error) => {
                let _ = bulk.reply.send(Err(error));
                return Ok(());
            }
        };
        let result = self.begin_task_control_ordered(
            bulk.method,
            json!([gid.to_string()]),
            bulk.captured.sequence,
            false,
        );
        self.retain_pending_work(Some(work))?;
        match result {
            Ok(ControlReply::Deferred(member)) => {
                self.turn.mark_progress();
                bulk.member = Some(member);
                bulk.captured.targets.pop_front();
                self.pending_bulk = Some(bulk);
            }
            Ok(ControlReply::Ready(_)) => {
                self.turn.mark_progress();
                bulk.captured.targets.pop_front();
                self.pending_bulk = Some(bulk);
            }
            Err(error) => {
                self.turn.mark_progress();
                let _ = bulk.reply.send(Err(error));
            }
        }
        Ok(())
    }
}
