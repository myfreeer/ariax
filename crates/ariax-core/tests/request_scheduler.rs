use ariax_core::{
    Aria2Status, CredentialKind, CredentialRequirement, DrainTarget, ErrorKind, Generation, Gid,
    HostKeyChallenge, HostKeyChallengeId, HostKeyFingerprint, HostKeyResolutionId,
    MAX_CONDITION_DESCRIPTION_BYTES, MAX_PERSISTED_MILLISECONDS, MAX_REDACTED_PATH_BYTES,
    MAX_SCHEDULER_EFFECTS, MonotonicInstant, NoSpaceCondition, NoSpaceProbeId, NoSpaceProbeOrigin,
    OptionPatchId, PendingBarrier, PresentedHostKeyChallenge, PublicError, QueueClass, QueueOrder,
    RecoveredSchedulerTask, RequestScheduler, RetryClass, RetryTimerId, SchedulerCommand,
    SchedulerConfig, SchedulerError, SchedulerOutcome, SchedulerRestoreBatch,
    SchedulerRestoreError, SlotOwnership, SlowReadmissionDecision, SlowSlotPersistence,
    StateReason, StoppedResultDeletionId, TaskConditions, TaskEvent, TaskId, TaskState,
    TransitionEffect, ValidatedOptionPatchKind,
};
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

#[derive(Clone, Debug)]
struct TestScheduler(RequestScheduler);

impl Deref for TestScheduler {
    type Target = RequestScheduler;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TestScheduler {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TestScheduler {
    fn handle_event_at(
        &mut self,
        event: TaskEvent,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let task = self
            .task(event.gid())
            .ok_or(SchedulerError::TaskNotFound)?
            .task_id;
        let envelope = event.for_task(task);
        self.0.handle_event_at(&envelope, at)
    }

    fn handle_event_for_task_at(
        &mut self,
        task: TaskId,
        event: TaskEvent,
        at: MonotonicInstant,
    ) -> Result<SchedulerOutcome, SchedulerError> {
        let envelope = event.for_task(task);
        self.0.handle_event_at(&envelope, at)
    }
}

fn gid(value: u64) -> Gid {
    Gid::new(value).expect("non-zero GID")
}

fn task_id(value: u64) -> TaskId {
    TaskId::new(value).expect("non-zero task id")
}

fn new_scheduler(
    max_tasks: usize,
    max_active: usize,
    retry_wait_holds_slot: bool,
) -> TestScheduler {
    TestScheduler(RequestScheduler::new(
        SchedulerConfig::new(
            NonZeroUsize::new(max_tasks).expect("non-zero task limit"),
            NonZeroUsize::new(max_active).expect("non-zero active limit"),
            retry_wait_holds_slot,
        )
        .expect("valid scheduler limits"),
    ))
}

fn later(base: MonotonicInstant, seconds: u64) -> MonotonicInstant {
    base.checked_add(Duration::from_secs(seconds))
        .expect("representable test instant")
}

fn add_command(task: TaskId, gid: Gid, desired_paused: bool) -> SchedulerCommand {
    SchedulerCommand::AddValidatedTask {
        task_id: task,
        gid,
        desired_paused,
        conditions: TaskConditions::default(),
    }
}

fn add_task(
    scheduler: &mut TestScheduler,
    task: TaskId,
    gid: Gid,
    at: MonotonicInstant,
) -> SchedulerOutcome {
    scheduler
        .execute_command_at(add_command(task, gid, false), at)
        .expect("add task")
}

fn make_persisted_allocating(
    scheduler: &mut TestScheduler,
    task: TaskId,
    gid: Gid,
    at: MonotonicInstant,
) -> Generation {
    add_task(scheduler, task, gid, at);
    scheduler.admit_next_at(later(at, 1)).expect("admit task");
    let generation = scheduler.task(gid).expect("task view").generation;
    scheduler
        .handle_event_at(
            TaskEvent::GenerationPersisted { gid, generation },
            later(at, 2),
        )
        .expect("persist generation");
    generation
}

fn make_active(
    scheduler: &mut TestScheduler,
    task: TaskId,
    gid: Gid,
    at: MonotonicInstant,
) -> Generation {
    let generation = make_persisted_allocating(scheduler, task, gid, at);
    scheduler
        .handle_event_at(
            TaskEvent::AllocationSucceeded { gid, generation },
            later(at, 3),
        )
        .expect("finish allocation");
    generation
}

fn make_stopped_complete(
    scheduler: &mut TestScheduler,
    task: TaskId,
    gid: Gid,
    at: MonotonicInstant,
) -> Generation {
    let generation = make_active(scheduler, task, gid, at);
    scheduler
        .handle_event_at(
            TaskEvent::DataComplete {
                gid,
                generation,
                seed: false,
            },
            later(at, 4),
        )
        .expect("enter verification");
    scheduler
        .handle_event_at(
            TaskEvent::VerificationSucceeded { gid, generation },
            later(at, 5),
        )
        .expect("finish verification");
    scheduler
        .handle_event_at(
            TaskEvent::TerminalPersisted {
                gid,
                generation,
                status: Aria2Status::Complete,
            },
            later(at, 6),
        )
        .expect("retain stopped result");
    generation
}

fn make_active_restart_draining(
    scheduler: &mut TestScheduler,
    task: TaskId,
    gid: Gid,
    at: MonotonicInstant,
) -> (Generation, OptionPatchId) {
    let generation = make_active(scheduler, task, gid, at);
    let patch_id = OptionPatchId::new(1).expect("option patch id");
    scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            },
            later(at, 4),
        )
        .expect("stage active-restart patch");
    scheduler
        .handle_event_at(
            TaskEvent::OptionPatchPersisted {
                gid,
                generation,
                patch_id,
            },
            later(at, 5),
        )
        .expect("persist active-restart patch");
    assert_eq!(
        scheduler.task(gid).expect("restart drain").pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::PausedRestarting,
            force: false,
        })
    );
    (generation, patch_id)
}

fn make_host_key_resolution_pending(
    scheduler: &mut TestScheduler,
    task: TaskId,
    gid: Gid,
    at: MonotonicInstant,
) -> (Generation, HostKeyResolutionId) {
    let generation = make_persisted_allocating(scheduler, task, gid, at);
    let challenge_id = HostKeyChallengeId::new([7; 16]);
    let key = vec![9, 8, 7, 6];
    let fingerprint = HostKeyFingerprint::for_presented_key(&key);
    let challenge = PresentedHostKeyChallenge::new(
        HostKeyChallenge {
            id: challenge_id,
            canonical_host: "sftp.example".to_owned(),
            port: 22,
            algorithm: "ssh-ed25519".to_owned(),
            fingerprint_sha256: fingerprint,
        },
        key,
    )
    .expect("host-key challenge");
    scheduler
        .handle_event_at(
            TaskEvent::AllocationHostKeyChallenge {
                gid,
                generation,
                challenge,
            },
            later(at, 3),
        )
        .expect("record host-key challenge");
    let approval = scheduler
        .execute_command_at(
            SchedulerCommand::ApproveHostKey {
                gid,
                challenge: challenge_id,
                fingerprint_sha256: fingerprint,
            },
            later(at, 4),
        )
        .expect("request host-key resolution");
    (generation, host_key_resolution_id(&approval))
}

fn state_fingerprint(scheduler: &TestScheduler) -> String {
    format!("{scheduler:#?}")
}

fn assert_transition(
    outcome: &SchedulerOutcome,
    from: TaskState,
    to: TaskState,
    reason: StateReason,
) {
    let transition = outcome.transition.expect("state transition");
    assert_eq!(transition.from, from);
    assert_eq!(transition.to, to);
    assert_eq!(transition.reason, reason);
    assert!(outcome.deletion.is_none());
    assert!(outcome.effects.len() <= MAX_SCHEDULER_EFFECTS);
}

fn assert_ignored(outcome: &SchedulerOutcome, reason: StateReason) {
    let transition = outcome.transition.expect("ignored transition record");
    assert_eq!(transition.from, transition.to);
    assert_eq!(transition.reason, reason);
    assert!(outcome.deletion.is_none());
    assert!(outcome.effects.is_empty());
}

fn retry_timer(outcome: &SchedulerOutcome) -> RetryTimerId {
    outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            TransitionEffect::ScheduleRetry { retry_timer_id, .. } => Some(*retry_timer_id),
            _ => None,
        })
        .expect("scheduled retry timer")
}

fn no_space_probe(outcome: &SchedulerOutcome) -> NoSpaceProbeId {
    outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            TransitionEffect::ProbeNoSpace { probe_id, .. } => Some(*probe_id),
            _ => None,
        })
        .expect("scheduled no-space probe")
}

fn deletion_id(outcome: &SchedulerOutcome) -> StoppedResultDeletionId {
    outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            TransitionEffect::DeleteStoppedTaskMetadata { deletion_id, .. } => Some(*deletion_id),
            _ => None,
        })
        .expect("stopped-result deletion id")
}

fn host_key_resolution_id(outcome: &SchedulerOutcome) -> HostKeyResolutionId {
    outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            TransitionEffect::PersistHostKeyPinAndClearChallenge { resolution_id, .. } => {
                Some(*resolution_id)
            }
            _ => None,
        })
        .expect("host-key resolution id")
}

fn queue_transition(
    task_id: TaskId,
    gid: Gid,
    from: Option<QueueClass>,
    to: Option<QueueClass>,
    desired_paused: bool,
    orders: Vec<(QueueClass, Vec<Gid>)>,
) -> TransitionEffect {
    TransitionEffect::PersistQueueTransition {
        task_id,
        gid,
        from,
        to,
        desired_paused,
        slow_demotion_count: 0,
        slow_slot: None,
        orders: orders
            .into_iter()
            .map(|(class, order)| QueueOrder { class, order })
            .collect(),
    }
}

#[test]
fn add_enforces_collisions_limits_and_exact_effect_order() {
    let at = MonotonicInstant::now();
    let first_gid = gid(1);
    let first_task = task_id(1);
    let mut scheduler = new_scheduler(2, 1, false);

    let outcome = add_task(&mut scheduler, first_task, first_gid, at);
    let snapshot = scheduler.snapshot(first_gid).expect("waiting snapshot");
    assert_transition(
        &outcome,
        TaskState::Accepted,
        TaskState::Waiting,
        StateReason::ValidationSucceeded,
    );
    assert_eq!(
        outcome.effects,
        vec![
            TransitionEffect::PersistTask {
                task_id: first_task,
                gid: first_gid,
                queue: QueueClass::Waiting,
                position: 0,
                desired_paused: false,
                slow_demotion_count: 0,
                conditions: TaskConditions::default(),
            },
            TransitionEffect::PublishSnapshot {
                task_id: first_task,
                snapshot,
            },
        ]
    );

    let before_collision = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(add_command(task_id(2), first_gid, false), later(at, 1)),
        Err(SchedulerError::GidCollision)
    );
    assert_eq!(state_fingerprint(&scheduler), before_collision);

    assert_eq!(
        scheduler.execute_command_at(add_command(first_task, gid(2), false), later(at, 2)),
        Err(SchedulerError::TaskIdCollision)
    );
    assert_eq!(state_fingerprint(&scheduler), before_collision);

    scheduler
        .execute_command_at(add_command(task_id(2), gid(2), true), later(at, 3))
        .expect("add paused task");
    assert_eq!(scheduler.len(), 2);
    assert_eq!(
        scheduler.queue_snapshot(QueueClass::Waiting),
        vec![first_gid]
    );
    assert_eq!(scheduler.queue_snapshot(QueueClass::Paused), vec![gid(2)]);

    let before_limit = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(add_command(task_id(3), gid(3), false), later(at, 4)),
        Err(SchedulerError::TaskLimitReached)
    );
    assert_eq!(state_fingerprint(&scheduler), before_limit);
}

#[test]
fn change_position_preserves_dense_queue_order_and_rejects_atomically() {
    let at = MonotonicInstant::now();
    let mut scheduler = new_scheduler(4, 1, false);
    for value in 1..=3 {
        add_task(&mut scheduler, task_id(value), gid(value), later(at, value));
    }
    assert_eq!(
        scheduler.queue_snapshot(QueueClass::Waiting),
        vec![gid(1), gid(2), gid(3)]
    );

    let outcome = scheduler
        .execute_command_at(
            SchedulerCommand::ChangePosition {
                gid: gid(3),
                position: 0,
            },
            later(at, 4),
        )
        .expect("move queue entry");
    assert!(outcome.transition.is_none());
    assert!(outcome.deletion.is_none());
    assert_eq!(
        outcome.effects,
        vec![queue_transition(
            task_id(3),
            gid(3),
            Some(QueueClass::Waiting),
            Some(QueueClass::Waiting),
            false,
            vec![(QueueClass::Waiting, vec![gid(3), gid(1), gid(2)])],
        )]
    );
    assert_eq!(
        scheduler.queue_snapshot(QueueClass::Waiting),
        vec![gid(3), gid(1), gid(2)]
    );

    let noop = scheduler
        .execute_command_at(
            SchedulerCommand::ChangePosition {
                gid: gid(3),
                position: 0,
            },
            later(at, 5),
        )
        .expect("idempotent reorder");
    assert!(noop.is_noop());

    let before = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(
            SchedulerCommand::ChangePosition {
                gid: gid(3),
                position: 3,
            },
            later(at, 6),
        ),
        Err(SchedulerError::InvalidPosition {
            position: 3,
            queue_len: 3,
        })
    );
    assert_eq!(state_fingerprint(&scheduler), before);
}

#[test]
fn generation_must_persist_before_allocation_can_start() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(2, 1, false);
    add_task(&mut scheduler, task_id(1), task_gid, at);

    let admitted = scheduler.admit_next_at(later(at, 1)).expect("admission");
    let view = scheduler.task(task_gid).expect("allocating task");
    let generation = view.generation;
    let snapshot = scheduler.snapshot(task_gid).expect("allocating snapshot");
    assert_transition(
        &admitted,
        TaskState::Waiting,
        TaskState::Allocating,
        StateReason::SchedulerAdmission,
    );
    assert_eq!(view.slot, SlotOwnership::Reserved);
    assert_eq!(
        view.pending_barrier,
        Some(PendingBarrier::GenerationPersistence { generation })
    );
    assert_eq!(
        admitted.effects,
        vec![
            TransitionEffect::PersistGenerationStarted {
                task_id: task_id(1),
                gid: task_gid,
                generation,
            },
            queue_transition(
                task_id(1),
                task_gid,
                Some(QueueClass::Waiting),
                Some(QueueClass::Active),
                false,
                vec![
                    (QueueClass::Waiting, vec![]),
                    (QueueClass::Active, vec![task_gid]),
                ],
            ),
            TransitionEffect::PublishSnapshot {
                task_id: task_id(1),
                snapshot,
            },
        ]
    );
    assert!(
        !admitted
            .effects
            .iter()
            .any(|effect| matches!(effect, TransitionEffect::StartAllocation { .. }))
    );

    let before_early_event = state_fingerprint(&scheduler);
    assert!(matches!(
        scheduler.handle_event_at(
            TaskEvent::AllocationSucceeded {
                gid: task_gid,
                generation,
            },
            later(at, 2),
        ),
        Err(SchedulerError::PendingBarrier {
            barrier: PendingBarrier::GenerationPersistence { .. },
            ..
        })
    ));
    assert_eq!(state_fingerprint(&scheduler), before_early_event);

    let persisted = scheduler
        .handle_event_at(
            TaskEvent::GenerationPersisted {
                gid: task_gid,
                generation,
            },
            later(at, 3),
        )
        .expect("generation persistence acknowledgement");
    let snapshot = scheduler.snapshot(task_gid).expect("started snapshot");
    assert_transition(
        &persisted,
        TaskState::Allocating,
        TaskState::Allocating,
        StateReason::GenerationPersistenceSucceeded,
    );
    assert_eq!(
        persisted.effects,
        vec![
            TransitionEffect::StartAllocation {
                task_id: task_id(1),
                gid: task_gid,
                generation,
            },
            TransitionEffect::PublishSnapshot {
                task_id: task_id(1),
                snapshot,
            },
        ]
    );
    assert_eq!(
        scheduler.task(task_gid).expect("task view").slot,
        SlotOwnership::Active
    );

    let duplicate = scheduler
        .handle_event_at(
            TaskEvent::GenerationPersisted {
                gid: task_gid,
                generation,
            },
            later(at, 4),
        )
        .expect("duplicate acknowledgement is ignored");
    assert_ignored(&duplicate, StateReason::DuplicateEventIgnored);
}

#[test]
fn pause_and_remove_wait_for_the_matching_cancellation_drain() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut paused_scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut paused_scheduler, task_id(1), task_gid, at);

    let paused = paused_scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: true,
            },
            later(at, 4),
        )
        .expect("pause active task");
    let snapshot = paused_scheduler
        .snapshot(task_gid)
        .expect("paused snapshot");
    assert_transition(
        &paused,
        TaskState::Active,
        TaskState::Paused,
        StateReason::UserPause,
    );
    assert_eq!(
        paused.effects,
        vec![
            TransitionEffect::CancelGeneration {
                task_id: task_id(1),
                gid: task_gid,
                generation,
                force: true,
            },
            queue_transition(
                task_id(1),
                task_gid,
                Some(QueueClass::Active),
                Some(QueueClass::Paused),
                true,
                vec![
                    (QueueClass::Active, vec![]),
                    (QueueClass::Paused, vec![task_gid]),
                ],
            ),
            TransitionEffect::PublishSnapshot {
                task_id: task_id(1),
                snapshot,
            },
        ]
    );
    assert_eq!(paused_scheduler.active_slot_count(), 1);
    assert_eq!(
        paused_scheduler
            .task(task_gid)
            .expect("paused task")
            .pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::Paused,
            force: true,
        })
    );

    let drained = paused_scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 5),
        )
        .expect("pause drain acknowledgement");
    let snapshot = paused_scheduler
        .snapshot(task_gid)
        .expect("drained pause snapshot");
    assert_eq!(
        drained.effects,
        vec![
            TransitionEffect::ReleaseSlot {
                task_id: task_id(1),
                gid: task_gid,
                ownership: SlotOwnership::Active,
            },
            TransitionEffect::PublishSnapshot {
                task_id: task_id(1),
                snapshot,
            },
        ]
    );
    assert_eq!(paused_scheduler.active_slot_count(), 0);
    assert_eq!(
        paused_scheduler.task(task_gid).expect("paused task").slot,
        SlotOwnership::None
    );

    let mut removed_scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut removed_scheduler, task_id(2), gid(2), later(at, 10));
    let removed = removed_scheduler
        .execute_command_at(
            SchedulerCommand::Remove {
                gid: gid(2),
                force: false,
            },
            later(at, 14),
        )
        .expect("remove active task");
    assert_transition(
        &removed,
        TaskState::Active,
        TaskState::Removed,
        StateReason::UserRemove,
    );
    assert_eq!(
        removed.effects,
        vec![TransitionEffect::CancelGeneration {
            task_id: task_id(2),
            gid: gid(2),
            generation,
            force: false,
        }]
    );
    assert_eq!(removed_scheduler.active_slot_count(), 1);

    let drained = removed_scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: gid(2),
                generation,
            },
            later(at, 15),
        )
        .expect("remove drain acknowledgement");
    assert_eq!(
        drained.effects,
        vec![
            TransitionEffect::ReleaseSlot {
                task_id: task_id(2),
                gid: gid(2),
                ownership: SlotOwnership::Active,
            },
            TransitionEffect::PersistTerminal {
                task_id: task_id(2),
                gid: gid(2),
                generation,
                status: Aria2Status::Removed,
                error: None,
                from: QueueClass::Active,
                to: QueueClass::Stopped,
                desired_paused: false,
                slow_demotion_count: 0,
                slow_slot: None,
                orders: vec![
                    QueueOrder {
                        class: QueueClass::Active,
                        order: vec![],
                    },
                    QueueOrder {
                        class: QueueClass::Stopped,
                        order: vec![gid(2)],
                    },
                ],
            },
        ]
    );
    assert_eq!(
        removed_scheduler
            .task(gid(2))
            .expect("terminal pending task")
            .pending_barrier,
        Some(PendingBarrier::TerminalPersistence {
            generation,
            status: Aria2Status::Removed,
        })
    );
}

#[test]
fn pause_retargets_an_in_flight_cancellation_drain() {
    let at = MonotonicInstant::now();

    let mut pause_scheduler = new_scheduler(1, 1, false);
    let pause_gid = gid(1);
    let generation = make_active(&mut pause_scheduler, task_id(1), pause_gid, at);
    pause_scheduler
        .handle_event_at(
            TaskEvent::NoSpace {
                gid: pause_gid,
                generation,
                condition: NoSpaceCondition {
                    redacted_path: "pause-race.bin".to_owned(),
                    retry_at: None,
                },
            },
            later(at, 4),
        )
        .expect("start cancellation toward waiting");
    assert_eq!(
        pause_scheduler
            .task(pause_gid)
            .expect("draining task")
            .pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::Waiting,
            force: false,
        })
    );

    let paused = pause_scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: pause_gid,
                force: true,
            },
            later(at, 5),
        )
        .expect("pause retargets the existing drain");
    assert_transition(
        &paused,
        TaskState::Waiting,
        TaskState::Paused,
        StateReason::UserPause,
    );
    let pause_view = pause_scheduler.task(pause_gid).expect("retargeted pause");
    assert_eq!(
        pause_view.pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::Paused,
            force: true,
        })
    );
    assert!(paused.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::CancelGeneration {
            gid,
            generation: actual_generation,
            force: true,
            ..
        } if *gid == pause_gid && *actual_generation == generation
    )));
}

#[test]
fn remove_retargets_an_in_flight_cancellation_drain() {
    let at = MonotonicInstant::now();

    let mut remove_scheduler = new_scheduler(1, 1, false);
    let remove_gid = gid(2);
    let generation = make_active(&mut remove_scheduler, task_id(2), remove_gid, later(at, 10));
    remove_scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: remove_gid,
                force: false,
            },
            later(at, 14),
        )
        .expect("start cancellation toward paused");

    let removed = remove_scheduler
        .execute_command_at(
            SchedulerCommand::Remove {
                gid: remove_gid,
                force: true,
            },
            later(at, 15),
        )
        .expect("remove retargets the existing drain");
    assert_transition(
        &removed,
        TaskState::Paused,
        TaskState::Removed,
        StateReason::UserRemove,
    );
    let remove_view = remove_scheduler
        .task(remove_gid)
        .expect("retargeted removal");
    assert_eq!(
        remove_view.pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::Removed,
            force: true,
        })
    );
    assert!(removed.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::CancelGeneration {
            gid,
            generation: actual_generation,
            force: true,
            ..
        } if *gid == remove_gid && *actual_generation == generation
    )));
}

#[test]
fn slow_demotion_persists_original_active_position_and_first_readmission_slot() {
    let at = MonotonicInstant::now();
    let mut scheduler = new_scheduler(2, 2, false);
    make_active(&mut scheduler, task_id(1), gid(1), at);
    let generation = make_active(&mut scheduler, task_id(2), gid(2), later(at, 10));
    assert_eq!(
        scheduler.queue_snapshot(QueueClass::Active),
        vec![gid(1), gid(2)]
    );
    let readmit_at = later(at, 30);
    let decision = SlowReadmissionDecision {
        readmit_at,
        scheduled_at_ms: 14_000,
        delay_ms: 16_000,
    };

    let demoted = scheduler
        .handle_event_at(
            TaskEvent::SlowDemoted {
                gid: gid(2),
                generation,
                decision,
            },
            later(at, 14),
        )
        .expect("demote slow active task");
    let snapshot = scheduler.snapshot(gid(2)).expect("slow-demoted snapshot");
    let slow_slot = SlowSlotPersistence {
        original_position: 1,
        demotion_count: 1,
        decision,
    };
    assert_transition(
        &demoted,
        TaskState::Active,
        TaskState::WaitingSlow,
        StateReason::SlowSlotDemotion,
    );
    assert_eq!(
        demoted.effects,
        vec![
            TransitionEffect::CancelGeneration {
                task_id: task_id(2),
                gid: gid(2),
                generation,
                force: false,
            },
            TransitionEffect::PersistQueueTransition {
                task_id: task_id(2),
                gid: gid(2),
                from: Some(QueueClass::Active),
                to: Some(QueueClass::Demoted),
                desired_paused: false,
                slow_demotion_count: 1,
                slow_slot: Some(slow_slot),
                orders: vec![
                    QueueOrder {
                        class: QueueClass::Active,
                        order: vec![gid(1)],
                    },
                    QueueOrder {
                        class: QueueClass::Demoted,
                        order: vec![gid(2)],
                    },
                ],
            },
            TransitionEffect::PublishSnapshot {
                task_id: task_id(2),
                snapshot,
            },
        ]
    );
    let view = scheduler.task(gid(2)).expect("slow-demoted task");
    assert_eq!(view.slow_slot, Some(slow_slot));
    assert_eq!(
        view.pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::WaitingSlow,
            force: false,
        })
    );
}

#[test]
fn slow_demotion_rejects_nonpersistable_wall_decisions_without_mutation() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut scheduler, task_id(1), task_gid, at);

    for decision in [
        SlowReadmissionDecision {
            readmit_at: later(at, 10),
            scheduled_at_ms: MAX_PERSISTED_MILLISECONDS + 1,
            delay_ms: 1,
        },
        SlowReadmissionDecision {
            readmit_at: later(at, 10),
            scheduled_at_ms: 1,
            delay_ms: 0,
        },
    ] {
        let before = state_fingerprint(&scheduler);
        assert_eq!(
            scheduler.handle_event_at(
                TaskEvent::SlowDemoted {
                    gid: task_gid,
                    generation,
                    decision,
                },
                later(at, 4),
            ),
            Err(SchedulerError::InvalidSlowReadmissionDecision)
        );
        assert_eq!(state_fingerprint(&scheduler), before);
    }
}

#[test]
fn slow_pause_persists_desired_pause_with_the_queue_transition() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let internal_task = task_id(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut scheduler, internal_task, task_gid, at);

    let paused = scheduler
        .handle_event_at(
            TaskEvent::SlowPaused {
                gid: task_gid,
                generation,
            },
            later(at, 4),
        )
        .expect("pause slow active task");
    let snapshot = scheduler.snapshot(task_gid).expect("slow-paused snapshot");
    assert_transition(
        &paused,
        TaskState::Active,
        TaskState::PausedSlow,
        StateReason::SlowSlotPause,
    );
    assert_eq!(
        paused.effects,
        vec![
            TransitionEffect::CancelGeneration {
                task_id: internal_task,
                gid: task_gid,
                generation,
                force: false,
            },
            TransitionEffect::PersistQueueTransition {
                task_id: internal_task,
                gid: task_gid,
                from: Some(QueueClass::Active),
                to: Some(QueueClass::Paused),
                desired_paused: true,
                slow_demotion_count: 0,
                slow_slot: None,
                orders: vec![
                    QueueOrder {
                        class: QueueClass::Active,
                        order: vec![],
                    },
                    QueueOrder {
                        class: QueueClass::Paused,
                        order: vec![task_gid],
                    },
                ],
            },
            TransitionEffect::PublishSnapshot {
                task_id: internal_task,
                snapshot,
            },
        ]
    );
    assert!(
        scheduler
            .task(task_gid)
            .expect("slow-paused task")
            .desired_paused
    );
}

#[test]
fn retry_wait_slot_policy_controls_release_queue_and_wire_projection() {
    for holds_slot in [false, true] {
        let at = MonotonicInstant::now();
        let task_gid = gid(if holds_slot { 2 } else { 1 });
        let mut scheduler = new_scheduler(1, 1, holds_slot);
        let generation = make_persisted_allocating(&mut scheduler, task_id(1), task_gid, at);
        let retry_at = later(at, 20);

        let outcome = scheduler
            .handle_event_at(
                TaskEvent::AllocationRetryable {
                    gid: task_gid,
                    generation,
                    retry_at,
                },
                later(at, 3),
            )
            .expect("retryable allocation");
        let timer = retry_timer(&outcome);
        let snapshot = scheduler.snapshot(task_gid).expect("retry snapshot");
        assert_transition(
            &outcome,
            TaskState::Allocating,
            TaskState::RetryWait,
            StateReason::AllocationRetryableFailure,
        );

        if holds_slot {
            assert_eq!(
                outcome.effects,
                vec![
                    TransitionEffect::ScheduleRetry {
                        task_id: task_id(1),
                        gid: task_gid,
                        generation,
                        retry_timer_id: timer,
                        at: retry_at,
                    },
                    TransitionEffect::PublishSnapshot {
                        task_id: task_id(1),
                        snapshot: snapshot.clone(),
                    },
                ]
            );
            assert_eq!(
                scheduler.task(task_gid).expect("retry task").slot,
                SlotOwnership::RetryRetained
            );
            assert_eq!(scheduler.active_slot_count(), 1);
            assert_eq!(scheduler.queue_snapshot(QueueClass::Active), vec![task_gid]);
            assert_eq!(snapshot.wire_status(), Ok(Aria2Status::Active));
        } else {
            assert_eq!(
                outcome.effects,
                vec![
                    TransitionEffect::ReleaseSlot {
                        task_id: task_id(1),
                        gid: task_gid,
                        ownership: SlotOwnership::Active,
                    },
                    TransitionEffect::ScheduleRetry {
                        task_id: task_id(1),
                        gid: task_gid,
                        generation,
                        retry_timer_id: timer,
                        at: retry_at,
                    },
                    queue_transition(
                        task_id(1),
                        task_gid,
                        Some(QueueClass::Active),
                        Some(QueueClass::Waiting),
                        false,
                        vec![
                            (QueueClass::Active, vec![]),
                            (QueueClass::Waiting, vec![task_gid]),
                        ],
                    ),
                    TransitionEffect::PublishSnapshot {
                        task_id: task_id(1),
                        snapshot: snapshot.clone(),
                    },
                ]
            );
            assert_eq!(
                scheduler.task(task_gid).expect("retry task").slot,
                SlotOwnership::None
            );
            assert_eq!(scheduler.active_slot_count(), 0);
            assert_eq!(
                scheduler.queue_snapshot(QueueClass::Waiting),
                vec![task_gid]
            );
            assert_eq!(snapshot.wire_status(), Ok(Aria2Status::Waiting));
        }
    }
}

#[test]
fn stale_duplicate_and_wrong_retry_tokens_never_mutate_state() {
    let at = MonotonicInstant::now();
    let retry_gid = gid(1);
    let mut scheduler = new_scheduler(2, 1, false);
    let generation = make_persisted_allocating(&mut scheduler, task_id(1), retry_gid, at);
    let retry_outcome = scheduler
        .handle_event_at(
            TaskEvent::AllocationRetryable {
                gid: retry_gid,
                generation,
                retry_at: later(at, 20),
            },
            later(at, 3),
        )
        .expect("enter retry wait");
    let timer = retry_timer(&retry_outcome);

    let active_gid = gid(2);
    add_task(&mut scheduler, task_id(2), active_gid, later(at, 4));
    scheduler
        .admit_next_at(later(at, 5))
        .expect("admit blocker");
    scheduler
        .handle_event_at(
            TaskEvent::GenerationPersisted {
                gid: active_gid,
                generation: Generation::INITIAL,
            },
            later(at, 6),
        )
        .expect("persist blocker generation");
    scheduler
        .handle_event_at(
            TaskEvent::AllocationSucceeded {
                gid: active_gid,
                generation: Generation::INITIAL,
            },
            later(at, 7),
        )
        .expect("activate blocker");

    let wrong_timer = RetryTimerId::new(timer.get() + 100).expect("different timer");
    let before_wrong = state_fingerprint(&scheduler);
    let wrong = scheduler
        .handle_event_at(
            TaskEvent::RetryReady {
                gid: retry_gid,
                generation,
                retry_timer_id: wrong_timer,
            },
            later(at, 8),
        )
        .expect("wrong token is ignored");
    assert_ignored(&wrong, StateReason::StaleEventIgnored);
    assert_eq!(state_fingerprint(&scheduler), before_wrong);

    let blocked = scheduler
        .handle_event_at(
            TaskEvent::RetryReady {
                gid: retry_gid,
                generation,
                retry_timer_id: timer,
            },
            later(at, 9),
        )
        .expect("capacity-blocked retry acknowledgement");
    assert_transition(
        &blocked,
        TaskState::RetryWait,
        TaskState::RetryWait,
        StateReason::AdmissionBlocked,
    );
    assert!(
        blocked
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::PersistGenerationStarted { .. }))
    );

    let before_duplicate = state_fingerprint(&scheduler);
    let duplicate = scheduler
        .handle_event_at(
            TaskEvent::RetryReady {
                gid: retry_gid,
                generation,
                retry_timer_id: timer,
            },
            later(at, 10),
        )
        .expect("duplicate retry token is ignored");
    assert_ignored(&duplicate, StateReason::DuplicateEventIgnored);
    assert_eq!(state_fingerprint(&scheduler), before_duplicate);

    let future_generation = generation.checked_next().expect("future generation");
    let before_future = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.handle_event_at(
            TaskEvent::RetryReady {
                gid: retry_gid,
                generation: future_generation,
                retry_timer_id: timer,
            },
            later(at, 11),
        ),
        Err(SchedulerError::StaleGeneration {
            expected: generation,
            actual: future_generation,
        })
    );
    assert_eq!(state_fingerprint(&scheduler), before_future);

    let mut generation_scheduler = new_scheduler(1, 1, false);
    let old_generation = make_active(&mut generation_scheduler, task_id(3), gid(3), later(at, 20));
    generation_scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: gid(3),
                force: false,
            },
            later(at, 24),
        )
        .expect("pause task");
    generation_scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: gid(3),
                generation: old_generation,
            },
            later(at, 25),
        )
        .expect("drain task");
    generation_scheduler
        .execute_command_at(SchedulerCommand::Resume { gid: gid(3) }, later(at, 26))
        .expect("resume task");
    generation_scheduler
        .admit_next_at(later(at, 27))
        .expect("readmit task");
    let current_generation = generation_scheduler
        .task(gid(3))
        .expect("readmitted task")
        .generation;
    assert_eq!(
        current_generation,
        old_generation.checked_next().expect("next")
    );

    let before_stale = state_fingerprint(&generation_scheduler);
    let stale = generation_scheduler
        .handle_event_at(
            TaskEvent::GenerationPersisted {
                gid: gid(3),
                generation: old_generation,
            },
            later(at, 28),
        )
        .expect("old generation is ignored");
    assert_ignored(&stale, StateReason::StaleEventIgnored);
    assert_eq!(state_fingerprint(&generation_scheduler), before_stale);
}

#[test]
fn active_representation_restart_is_an_immediate_persisted_generation_fence() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let initial = make_active(&mut scheduler, task_id(1), task_gid, at);

    let outcome = scheduler
        .handle_event_at(
            TaskEvent::ActiveRepresentationRestart {
                gid: task_gid,
                generation: initial,
            },
            later(at, 4),
        )
        .expect("request representation restart");
    let next = initial.checked_next().expect("next generation");
    assert_transition(
        &outcome,
        TaskState::Active,
        TaskState::Allocating,
        StateReason::RepresentationRestart,
    );
    assert!(outcome.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::PersistGenerationStarted {
            task_id: effect_task,
            gid,
            generation,
        } if *effect_task == task_id(1) && *gid == task_gid && *generation == next
    )));
    assert!(
        outcome
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::ScheduleRetry { .. }))
    );
    let view = scheduler.task(task_gid).expect("restarting task");
    assert_eq!(view.generation, next);
    assert_eq!(view.slot, SlotOwnership::Reserved);
    assert_eq!(
        view.pending_barrier,
        Some(PendingBarrier::GenerationPersistence { generation: next })
    );

    let persisted = scheduler
        .handle_event_at(
            TaskEvent::GenerationPersisted {
                gid: task_gid,
                generation: next,
            },
            later(at, 5),
        )
        .expect("persist restart generation");
    assert!(persisted.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::StartAllocation { generation, .. } if *generation == next
    )));
}

#[test]
fn no_space_probe_completion_preserves_a_later_user_pause() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut scheduler, task_id(1), task_gid, at);
    let condition = NoSpaceCondition {
        redacted_path: "download.bin".to_owned(),
        retry_at: None,
    };

    scheduler
        .handle_event_at(
            TaskEvent::NoSpace {
                gid: task_gid,
                generation,
                condition,
            },
            later(at, 4),
        )
        .expect("enter no-space wait");
    scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 5),
        )
        .expect("drain no-space generation");
    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 6),
        )
        .expect("pause before explicitly resuming the no-space task");

    let probe_outcome = scheduler
        .execute_command_at(SchedulerCommand::Resume { gid: task_gid }, later(at, 7))
        .expect("request readiness probe");
    let probe_id = no_space_probe(&probe_outcome);
    let probing_snapshot = scheduler
        .snapshot(task_gid)
        .expect("no-space probe snapshot");
    assert_eq!(
        probe_outcome.effects,
        vec![
            queue_transition(
                task_id(1),
                task_gid,
                Some(QueueClass::Paused),
                Some(QueueClass::Paused),
                false,
                vec![(QueueClass::Paused, vec![task_gid])],
            ),
            TransitionEffect::ProbeNoSpace {
                task_id: task_id(1),
                gid: task_gid,
                generation,
                probe_id,
                origin: NoSpaceProbeOrigin::ExplicitResume,
                at: later(at, 7),
            },
            TransitionEffect::PublishSnapshot {
                task_id: task_id(1),
                snapshot: probing_snapshot,
            },
        ]
    );
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("probing task")
            .no_space_probe,
        Some(probe_id)
    );

    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 8),
        )
        .expect("re-pause while probe is pending");
    let repaused = scheduler.task(task_gid).expect("re-paused task");
    assert_eq!(repaused.state, TaskState::Paused);
    assert!(repaused.desired_paused);
    assert_eq!(repaused.no_space_probe, Some(probe_id));

    let wrong_probe = NoSpaceProbeId::new(probe_id.get() + 100).expect("different probe id");
    let before_wrong_probe = state_fingerprint(&scheduler);
    let stale = scheduler
        .handle_event_at(
            TaskEvent::NoSpaceProbeCompleted {
                gid: task_gid,
                generation,
                probe_id: wrong_probe,
                origin: NoSpaceProbeOrigin::ExplicitResume,
                ready: true,
                next_retry_at: None,
            },
            later(at, 9),
        )
        .expect("wrong no-space probe token is ignored");
    assert_ignored(&stale, StateReason::StaleEventIgnored);
    assert_eq!(state_fingerprint(&scheduler), before_wrong_probe);

    let completed = scheduler
        .handle_event_at(
            TaskEvent::NoSpaceProbeCompleted {
                gid: task_gid,
                generation,
                probe_id,
                origin: NoSpaceProbeOrigin::ExplicitResume,
                ready: true,
                next_retry_at: None,
            },
            later(at, 10),
        )
        .expect("complete readiness probe");
    assert_transition(
        &completed,
        TaskState::Paused,
        TaskState::Paused,
        StateReason::NoSpaceProbeSucceeded,
    );
    let view = scheduler.task(task_gid).expect("pause-preserved task");
    assert_eq!(view.state, TaskState::Paused);
    assert!(view.desired_paused);
    assert!(!view.conditions.no_space);
    assert_eq!(view.no_space_probe, None);
    assert_eq!(scheduler.queue_snapshot(QueueClass::Paused), vec![task_gid]);
}

#[test]
fn automatic_no_space_probe_reschedules_with_a_fresh_token_and_honors_pause() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut scheduler, task_id(1), task_gid, at);
    let first_deadline = later(at, 30);

    scheduler
        .handle_event_at(
            TaskEvent::NoSpace {
                gid: task_gid,
                generation,
                condition: NoSpaceCondition {
                    redacted_path: "download.bin".to_owned(),
                    retry_at: Some(first_deadline),
                },
            },
            later(at, 4),
        )
        .expect("enter no-space wait");
    let drained = scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 5),
        )
        .expect("drain no-space generation");
    let first_probe = no_space_probe(&drained);
    assert!(drained.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::ProbeNoSpace {
            probe_id,
            origin: NoSpaceProbeOrigin::AutomaticRetry,
            at,
            ..
        } if *probe_id == first_probe && *at == first_deadline
    )));

    let second_deadline = later(at, 60);
    let failed = scheduler
        .handle_event_at(
            TaskEvent::NoSpaceProbeCompleted {
                gid: task_gid,
                generation,
                probe_id: first_probe,
                origin: NoSpaceProbeOrigin::AutomaticRetry,
                ready: false,
                next_retry_at: Some(second_deadline),
            },
            later(at, 31),
        )
        .expect("reschedule failed probe");
    let second_probe = no_space_probe(&failed);
    assert_ne!(first_probe, second_probe);
    assert!(failed.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::ProbeNoSpace {
            probe_id,
            origin: NoSpaceProbeOrigin::AutomaticRetry,
            at,
            ..
        } if *probe_id == second_probe && *at == second_deadline
    )));

    let before_stale = state_fingerprint(&scheduler);
    let stale = scheduler
        .handle_event_at(
            TaskEvent::NoSpaceProbeCompleted {
                gid: task_gid,
                generation,
                probe_id: first_probe,
                origin: NoSpaceProbeOrigin::AutomaticRetry,
                ready: true,
                next_retry_at: None,
            },
            later(at, 32),
        )
        .expect("superseded probe is ignored");
    assert_ignored(&stale, StateReason::DuplicateEventIgnored);
    assert_eq!(state_fingerprint(&scheduler), before_stale);

    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 33),
        )
        .expect("pause while retry probe is pending");
    let completed = scheduler
        .handle_event_at(
            TaskEvent::NoSpaceProbeCompleted {
                gid: task_gid,
                generation,
                probe_id: second_probe,
                origin: NoSpaceProbeOrigin::AutomaticRetry,
                ready: true,
                next_retry_at: None,
            },
            later(at, 61),
        )
        .expect("complete current automatic probe");
    assert_transition(
        &completed,
        TaskState::Paused,
        TaskState::Paused,
        StateReason::NoSpaceProbeSucceeded,
    );
    let paused = scheduler
        .task(task_gid)
        .expect("pause remains authoritative");
    assert!(paused.desired_paused);
    assert!(!paused.conditions.no_space);
    assert_eq!(paused.no_space_probe, None);
}

#[test]
fn remove_during_host_key_resolution_waits_for_ack_then_terminalizes() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let (generation, resolution_id) =
        make_host_key_resolution_pending(&mut scheduler, internal_task, task_gid, at);

    let removed = scheduler
        .execute_command_at(
            SchedulerCommand::Remove {
                gid: task_gid,
                force: false,
            },
            later(at, 5),
        )
        .expect("remove wins over host-key resolution");
    assert!(removed.effects.iter().all(|effect| !matches!(
        effect,
        TransitionEffect::PersistTerminal { .. }
            | TransitionEffect::StartAllocation { .. }
            | TransitionEffect::ApplyOptionPatch { .. }
    )));
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("deferred host-key removal")
            .pending_barrier,
        Some(PendingBarrier::HostKeyResolution {
            generation,
            resolution_id,
            challenge: HostKeyChallengeId::new([7; 16]),
        })
    );

    let resolved = scheduler
        .handle_event_at(
            TaskEvent::HostKeyResolutionPersisted {
                gid: task_gid,
                generation,
                resolution_id,
            },
            later(at, 6),
        )
        .expect("acknowledge resolution before removal persistence");
    assert!(resolved.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::PersistTerminal {
            task_id,
            gid,
            status: Aria2Status::Removed,
            ..
        } if *task_id == internal_task && *gid == task_gid
    )));
    assert!(
        resolved
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::StartAllocation { .. }))
    );
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("terminal host-key removal")
            .pending_barrier,
        Some(PendingBarrier::TerminalPersistence {
            generation,
            status: Aria2Status::Removed,
        })
    );
}

#[test]
fn host_key_resolution_ack_honors_latest_pause_resume_intent() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let (generation, resolution_id) =
        make_host_key_resolution_pending(&mut scheduler, task_id(1), task_gid, at);
    let barrier = PendingBarrier::HostKeyResolution {
        generation,
        resolution_id,
        challenge: HostKeyChallengeId::new([7; 16]),
    };

    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 5),
        )
        .expect("record pause during host-key resolution");
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("paused host-key resolution")
            .pending_barrier,
        Some(barrier)
    );
    scheduler
        .execute_command_at(SchedulerCommand::Resume { gid: task_gid }, later(at, 6))
        .expect("latest resume wins during host-key resolution");
    let pending = scheduler
        .task(task_gid)
        .expect("resumed host-key resolution");
    assert!(!pending.desired_paused);
    assert_eq!(pending.pending_barrier, Some(barrier));

    let resolved = scheduler
        .handle_event_at(
            TaskEvent::HostKeyResolutionPersisted {
                gid: task_gid,
                generation,
                resolution_id,
            },
            later(at, 7),
        )
        .expect("complete resumed host-key resolution");
    assert!(
        resolved
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::StartAllocation { .. }))
    );
    let view = scheduler.task(task_gid).expect("resolved host-key task");
    assert_eq!(view.state, TaskState::Waiting);
    assert!(!view.desired_paused);
    assert_eq!(view.pending_barrier, None);
}

#[test]
fn host_key_approval_requires_exact_proof_and_preserves_later_pause() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_persisted_allocating(&mut scheduler, task_id(1), task_gid, at);
    let challenge_id = HostKeyChallengeId::new([1; 16]);
    let key = vec![3, 4, 5, 6];
    let fingerprint = HostKeyFingerprint::for_presented_key(&key);
    let challenge = PresentedHostKeyChallenge::new(
        HostKeyChallenge {
            id: challenge_id,
            canonical_host: "sftp.example".to_owned(),
            port: 22,
            algorithm: "ssh-ed25519".to_owned(),
            fingerprint_sha256: fingerprint,
        },
        key.clone(),
    )
    .expect("bounded host-key challenge");

    let challenged = scheduler
        .handle_event_at(
            TaskEvent::AllocationHostKeyChallenge {
                gid: task_gid,
                generation,
                challenge: challenge.clone(),
            },
            later(at, 3),
        )
        .expect("pause for host-key approval");
    let snapshot = scheduler.snapshot(task_gid).expect("challenge snapshot");
    assert_transition(
        &challenged,
        TaskState::Allocating,
        TaskState::PausedHostKey,
        StateReason::HostKeyChallenge,
    );
    assert_eq!(
        challenged.effects,
        vec![
            TransitionEffect::PersistHostKeyChallenge {
                task_id: task_id(1),
                gid: task_gid,
                challenge: challenge.clone(),
            },
            TransitionEffect::ReleaseSlot {
                task_id: task_id(1),
                gid: task_gid,
                ownership: SlotOwnership::Active,
            },
            queue_transition(
                task_id(1),
                task_gid,
                Some(QueueClass::Active),
                Some(QueueClass::Paused),
                false,
                vec![
                    (QueueClass::Active, vec![]),
                    (QueueClass::Paused, vec![task_gid]),
                ],
            ),
            TransitionEffect::PublishSnapshot {
                task_id: task_id(1),
                snapshot,
            },
        ]
    );

    let before_resume = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(SchedulerCommand::Resume { gid: task_gid }, later(at, 4),),
        Err(SchedulerError::HostKeyApprovalRequired)
    );
    assert_eq!(state_fingerprint(&scheduler), before_resume);

    for (wrong_challenge, wrong_fingerprint) in [
        (HostKeyChallengeId::new([9; 16]), fingerprint),
        (challenge_id, HostKeyFingerprint::new([9; 32])),
    ] {
        let before = state_fingerprint(&scheduler);
        assert_eq!(
            scheduler.execute_command_at(
                SchedulerCommand::ApproveHostKey {
                    gid: task_gid,
                    challenge: wrong_challenge,
                    fingerprint_sha256: wrong_fingerprint,
                },
                later(at, 5),
            ),
            Err(SchedulerError::StaleChallenge)
        );
        assert_eq!(state_fingerprint(&scheduler), before);
    }

    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 6),
        )
        .expect("record later user pause");
    let approval_requested = scheduler
        .execute_command_at(
            SchedulerCommand::ApproveHostKey {
                gid: task_gid,
                challenge: challenge_id,
                fingerprint_sha256: fingerprint,
            },
            later(at, 7),
        )
        .expect("approve exact challenge");
    let resolution_id = host_key_resolution_id(&approval_requested);
    assert_transition(
        &approval_requested,
        TaskState::PausedHostKey,
        TaskState::PausedHostKey,
        StateReason::HostKeyApproved,
    );
    assert_eq!(
        approval_requested.effects,
        vec![TransitionEffect::PersistHostKeyPinAndClearChallenge {
            task_id: task_id(1),
            gid: task_gid,
            resolution_id,
            challenge: challenge_id,
            fingerprint_sha256: fingerprint,
            presented_public_key: key,
            option_patch: None,
        }]
    );
    assert!(
        approval_requested
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::StartAllocation { .. }))
    );
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("resolution-pending task")
            .pending_barrier,
        Some(PendingBarrier::HostKeyResolution {
            generation,
            resolution_id,
            challenge: challenge_id,
        })
    );
    let pending_snapshot = scheduler
        .snapshot(task_gid)
        .expect("challenge remains visible before persistence acknowledgement");
    assert_eq!(pending_snapshot.state, TaskState::PausedHostKey);
    assert!(pending_snapshot.host_key_challenge.is_some());

    let wrong_resolution =
        HostKeyResolutionId::new(resolution_id.get() + 100).expect("different resolution id");
    let before_wrong_resolution = state_fingerprint(&scheduler);
    let stale = scheduler
        .handle_event_at(
            TaskEvent::HostKeyResolutionPersisted {
                gid: task_gid,
                generation,
                resolution_id: wrong_resolution,
            },
            later(at, 8),
        )
        .expect("wrong resolution token is ignored");
    assert_ignored(&stale, StateReason::StaleEventIgnored);
    assert_eq!(state_fingerprint(&scheduler), before_wrong_resolution);

    let resolved = scheduler
        .handle_event_at(
            TaskEvent::HostKeyResolutionPersisted {
                gid: task_gid,
                generation,
                resolution_id,
            },
            later(at, 9),
        )
        .expect("host-key persistence acknowledgement");
    let snapshot = scheduler.snapshot(task_gid).expect("approved snapshot");
    assert_transition(
        &resolved,
        TaskState::PausedHostKey,
        TaskState::Paused,
        StateReason::HostKeyApproved,
    );
    assert_eq!(
        resolved.effects,
        vec![TransitionEffect::PublishSnapshot {
            task_id: task_id(1),
            snapshot: snapshot.clone(),
        }]
    );
    assert_eq!(snapshot.state, TaskState::Paused);
    assert!(snapshot.desired_paused);
    assert!(snapshot.host_key_challenge.is_none());
}

#[test]
fn terminal_snapshot_is_published_only_after_matching_persistence_ack() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut scheduler, task_id(1), task_gid, at);

    scheduler
        .handle_event_at(
            TaskEvent::DataComplete {
                gid: task_gid,
                generation,
                seed: false,
            },
            later(at, 4),
        )
        .expect("enter verification");
    let last_published = scheduler
        .snapshot(task_gid)
        .expect("verification snapshot is published");
    let completed = scheduler
        .handle_event_at(
            TaskEvent::VerificationSucceeded {
                gid: task_gid,
                generation,
            },
            later(at, 5),
        )
        .expect("enter terminal-pending completion");
    assert_transition(
        &completed,
        TaskState::Verifying,
        TaskState::Complete,
        StateReason::VerificationSucceeded,
    );
    assert_eq!(
        completed.effects,
        vec![
            TransitionEffect::ReleaseSlot {
                task_id: task_id(1),
                gid: task_gid,
                ownership: SlotOwnership::Active,
            },
            TransitionEffect::PersistTerminal {
                task_id: task_id(1),
                gid: task_gid,
                generation,
                status: Aria2Status::Complete,
                error: None,
                from: QueueClass::Active,
                to: QueueClass::Stopped,
                desired_paused: false,
                slow_demotion_count: 0,
                slow_slot: None,
                orders: vec![
                    QueueOrder {
                        class: QueueClass::Active,
                        order: vec![],
                    },
                    QueueOrder {
                        class: QueueClass::Stopped,
                        order: vec![task_gid],
                    },
                ],
            },
        ]
    );
    assert!(
        completed
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::PublishSnapshot { .. }))
    );
    assert_eq!(scheduler.snapshot(task_gid), Ok(last_published));

    let before_wrong_ack = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.handle_event_at(
            TaskEvent::TerminalPersisted {
                gid: task_gid,
                generation,
                status: Aria2Status::Error,
            },
            later(at, 6),
        ),
        Err(SchedulerError::InvalidTerminalAcknowledgement)
    );
    assert_eq!(state_fingerprint(&scheduler), before_wrong_ack);

    let retained = scheduler
        .handle_event_at(
            TaskEvent::TerminalPersisted {
                gid: task_gid,
                generation,
                status: Aria2Status::Complete,
            },
            later(at, 7),
        )
        .expect("matching terminal acknowledgement");
    let snapshot = scheduler.snapshot(task_gid).expect("retained snapshot");
    assert_transition(
        &retained,
        TaskState::Complete,
        TaskState::StoppedResult,
        StateReason::TerminalPersistenceSucceeded,
    );
    assert_eq!(
        retained.effects,
        vec![TransitionEffect::PublishSnapshot {
            task_id: task_id(1),
            snapshot: snapshot.clone(),
        }]
    );
    assert_eq!(snapshot.stopped_status, Some(Aria2Status::Complete));
    assert!(snapshot.terminal_persisted);

    let duplicate = scheduler
        .handle_event_at(
            TaskEvent::TerminalPersisted {
                gid: task_gid,
                generation,
                status: Aria2Status::Complete,
            },
            later(at, 8),
        )
        .expect("duplicate terminal acknowledgement");
    assert_ignored(&duplicate, StateReason::DuplicateEventIgnored);

    let before_mismatched_ack = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.handle_event_at(
            TaskEvent::TerminalPersisted {
                gid: task_gid,
                generation,
                status: Aria2Status::Error,
            },
            later(at, 9),
        ),
        Err(SchedulerError::InvalidTerminalAcknowledgement)
    );
    assert_eq!(state_fingerprint(&scheduler), before_mismatched_ack);
}

#[test]
fn stopped_result_deletion_is_two_phase_and_failure_is_retryable() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let internal_task = task_id(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_stopped_complete(&mut scheduler, internal_task, task_gid, at);
    let retained_snapshot = scheduler
        .snapshot(task_gid)
        .expect("retained stopped-result snapshot");

    let requested = scheduler
        .execute_command_at(
            SchedulerCommand::RemoveStoppedResult { gid: task_gid },
            later(at, 7),
        )
        .expect("request stopped-result deletion");
    let first_deletion = deletion_id(&requested);
    assert_transition(
        &requested,
        TaskState::StoppedResult,
        TaskState::StoppedResult,
        StateReason::StoppedResultRemovalRequested,
    );
    assert_eq!(
        requested.effects,
        vec![TransitionEffect::DeleteStoppedTaskMetadata {
            task_id: internal_task,
            gid: task_gid,
            deletion_id: first_deletion,
            remaining_order: vec![],
        }]
    );
    assert!(scheduler.task(task_gid).is_some());
    assert_eq!(scheduler.snapshot(task_gid), Ok(retained_snapshot.clone()));

    let wrong_deletion =
        StoppedResultDeletionId::new(first_deletion.get() + 100).expect("different deletion id");
    let before_wrong = state_fingerprint(&scheduler);
    let wrong = scheduler
        .handle_event_at(
            TaskEvent::StoppedResultDeleted {
                gid: task_gid,
                generation,
                deletion_id: wrong_deletion,
            },
            later(at, 8),
        )
        .expect("wrong deletion token is ignored");
    assert_ignored(&wrong, StateReason::StaleEventIgnored);
    assert_eq!(state_fingerprint(&scheduler), before_wrong);

    let failed = scheduler
        .handle_event_at(
            TaskEvent::StoppedResultDeletionFailed {
                gid: task_gid,
                generation,
                deletion_id: first_deletion,
            },
            later(at, 9),
        )
        .expect("deletion failure acknowledgement");
    assert_transition(
        &failed,
        TaskState::StoppedResult,
        TaskState::StoppedResult,
        StateReason::StoppedResultRemovalFailed,
    );
    assert!(failed.effects.is_empty());
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("retained task after failure")
            .pending_barrier,
        None
    );
    assert_eq!(scheduler.snapshot(task_gid), Ok(retained_snapshot.clone()));

    let retried = scheduler
        .execute_command_at(
            SchedulerCommand::RemoveStoppedResult { gid: task_gid },
            later(at, 10),
        )
        .expect("retry deletion");
    let second_deletion = deletion_id(&retried);
    assert_ne!(second_deletion, first_deletion);
    assert_eq!(
        retried.effects,
        vec![TransitionEffect::DeleteStoppedTaskMetadata {
            task_id: internal_task,
            gid: task_gid,
            deletion_id: second_deletion,
            remaining_order: vec![],
        }]
    );

    let deleted = scheduler
        .handle_event_at(
            TaskEvent::StoppedResultDeleted {
                gid: task_gid,
                generation,
                deletion_id: second_deletion,
            },
            later(at, 11),
        )
        .expect("successful deletion acknowledgement");
    assert!(deleted.transition.is_none());
    let deletion = deleted.deletion.expect("task deletion record");
    assert_eq!(deletion.task, internal_task);
    assert_eq!(deletion.gid, task_gid);
    assert_eq!(deletion.generation, generation);
    assert_eq!(deletion.from, TaskState::StoppedResult);
    assert_eq!(deletion.reason, StateReason::StoppedResultRemoved);
    assert!(deleted.effects.is_empty());
    assert!(scheduler.is_empty());
    assert!(scheduler.task(task_gid).is_none());
    assert_eq!(
        scheduler.snapshot(task_gid),
        Err(SchedulerError::TaskNotFound)
    );

    let before_absent_event = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.handle_event_for_task_at(
            internal_task,
            TaskEvent::StoppedResultDeleted {
                gid: task_gid,
                generation,
                deletion_id: second_deletion,
            },
            later(at, 12),
        ),
        Err(SchedulerError::TaskNotFound)
    );
    assert_eq!(state_fingerprint(&scheduler), before_absent_event);

    scheduler
        .execute_command_at(add_command(task_id(2), task_gid, false), later(at, 13))
        .expect("a deleted GID is reusable by a fresh task identity");
    let stale_old_ack = scheduler
        .handle_event_for_task_at(
            internal_task,
            TaskEvent::StoppedResultDeleted {
                gid: task_gid,
                generation,
                deletion_id: second_deletion,
            },
            later(at, 14),
        )
        .expect("old deletion acknowledgement cannot delete replacement");
    assert_ignored(&stale_old_ack, StateReason::StaleEventIgnored);
    assert_eq!(scheduler.len(), 1);
}

#[test]
fn pending_stopped_deletion_serializes_queue_wide_mutations() {
    let at = MonotonicInstant::now();
    let first_gid = gid(1);
    let second_gid = gid(2);
    let mut scheduler = new_scheduler(2, 1, false);
    make_stopped_complete(&mut scheduler, task_id(1), first_gid, at);
    make_stopped_complete(&mut scheduler, task_id(2), second_gid, later(at, 10));
    assert_eq!(
        scheduler.queue_snapshot(QueueClass::Stopped),
        vec![first_gid, second_gid]
    );
    scheduler
        .execute_command_at(
            SchedulerCommand::RemoveStoppedResult { gid: first_gid },
            later(at, 17),
        )
        .expect("start first stopped-result deletion");
    let barrier = scheduler
        .task(first_gid)
        .expect("deleting stopped result")
        .pending_barrier
        .expect("stopped-result deletion barrier");

    let mut reordered = scheduler.clone();
    let before_reorder = state_fingerprint(&reordered);
    assert!(matches!(
        reordered.execute_command_at(
            SchedulerCommand::ChangePosition {
                gid: second_gid,
                position: 0,
            },
            later(at, 18),
        ),
        Err(SchedulerError::PendingBarrier {
            barrier: actual,
            operation: "change_position",
        }) if actual == barrier
    ));
    assert_eq!(state_fingerprint(&reordered), before_reorder);

    let mut second_deletion = scheduler.clone();
    let before_second_deletion = state_fingerprint(&second_deletion);
    assert!(matches!(
        second_deletion.execute_command_at(
            SchedulerCommand::RemoveStoppedResult { gid: second_gid },
            later(at, 19),
        ),
        Err(SchedulerError::PendingBarrier {
            barrier: actual,
            operation: "remove_stopped_result",
        }) if actual == barrier
    ));
    assert_eq!(state_fingerprint(&second_deletion), before_second_deletion);
}

#[test]
fn task_identity_collision_survives_stopped_result_deletion() {
    let at = MonotonicInstant::now();
    let old_task = task_id(1);
    let old_gid = gid(1);
    let mut scheduler = new_scheduler(2, 1, false);
    let generation = make_stopped_complete(&mut scheduler, old_task, old_gid, at);

    let requested = scheduler
        .execute_command_at(
            SchedulerCommand::RemoveStoppedResult { gid: old_gid },
            later(at, 7),
        )
        .expect("request deletion");
    scheduler
        .handle_event_at(
            TaskEvent::StoppedResultDeleted {
                gid: old_gid,
                generation,
                deletion_id: deletion_id(&requested),
            },
            later(at, 8),
        )
        .expect("complete deletion");

    let before = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(add_command(old_task, gid(2), false), later(at, 9)),
        Err(SchedulerError::TaskIdCollision)
    );
    assert_eq!(state_fingerprint(&scheduler), before);
}

#[test]
fn delayed_non_token_events_cannot_cross_a_deleted_gid_replacement() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(2, 1, false);
    let old_generation = make_stopped_complete(&mut scheduler, task_id(1), task_gid, at);

    let requested = scheduler
        .execute_command_at(
            SchedulerCommand::RemoveStoppedResult { gid: task_gid },
            later(at, 7),
        )
        .expect("request old task deletion");
    scheduler
        .handle_event_at(
            TaskEvent::StoppedResultDeleted {
                gid: task_gid,
                generation: old_generation,
                deletion_id: deletion_id(&requested),
            },
            later(at, 8),
        )
        .expect("delete old task");

    add_task(&mut scheduler, task_id(2), task_gid, later(at, 9));
    scheduler
        .admit_next_at(later(at, 10))
        .expect("admit replacement task");
    assert!(matches!(
        scheduler
            .task(task_gid)
            .expect("replacement awaiting generation persistence")
            .pending_barrier,
        Some(PendingBarrier::GenerationPersistence { .. })
    ));

    let delayed_events = [
        TaskEvent::GenerationPersisted {
            gid: task_gid,
            generation: old_generation,
        },
        TaskEvent::AllocationSucceeded {
            gid: task_gid,
            generation: old_generation,
        },
        TaskEvent::CancellationDrained {
            gid: task_gid,
            generation: old_generation,
        },
    ];
    for event in delayed_events {
        let mut candidate = scheduler.clone();
        let before = state_fingerprint(&candidate);
        let outcome = candidate
            .handle_event_for_task_at(task_id(1), event, later(at, 11))
            .expect("old task event is safely ignored");
        assert_ignored(&outcome, StateReason::StaleEventIgnored);
        assert_eq!(state_fingerprint(&candidate), before);
    }
}

#[test]
fn source_replacement_cancels_retry_timer_before_new_source_admission() {
    for retains_slot in [false, true] {
        let at = MonotonicInstant::now();
        let mut scheduler = new_scheduler(2, 1, retains_slot);
        let task_gid = gid(1);
        let generation = make_active(&mut scheduler, task_id(1), task_gid, at);
        scheduler
            .handle_event_at(
                TaskEvent::ActiveRetryIdle {
                    gid: task_gid,
                    generation,
                    retry_at: later(at, 30),
                },
                later(at, 4),
            )
            .expect("retry wait");
        let timer_id = scheduler
            .task(task_gid)
            .expect("retrying task")
            .retry_timer
            .expect("retry timer");
        let outcome = scheduler
            .execute_command_at(
                SchedulerCommand::BeginSourceReplacement { gid: task_gid },
                later(at, 5),
            )
            .expect("quiesce retry");
        assert!(outcome.effects.iter().any(|effect| matches!(effect, TransitionEffect::CancelRetry { retry_timer_id: id, .. } if *id == timer_id)));
        assert!(
            scheduler
                .task(task_gid)
                .expect("quiescing task")
                .retry_timer
                .is_none()
        );
        let stale = scheduler
            .handle_event_at(
                TaskEvent::RetryReady {
                    gid: task_gid,
                    generation,
                    retry_timer_id: timer_id,
                },
                later(at, 30),
            )
            .expect("late retry event");
        assert_ignored(&stale, StateReason::StaleEventIgnored);
        if retains_slot {
            scheduler
                .handle_event_at(
                    TaskEvent::CancellationDrained {
                        gid: task_gid,
                        generation,
                    },
                    later(at, 31),
                )
                .expect("drain retained slot");
        }
        assert!(scheduler.admit_next_at(later(at, 32)).is_err());
        scheduler
            .execute_command_at(
                SchedulerCommand::CommitSourceReplacement {
                    gid: task_gid,
                    satisfies_credentials: None,
                },
                later(at, 33),
            )
            .expect("commit replacement");
        scheduler
            .admit_next_at(later(at, 34))
            .expect("new sources can be admitted");
        assert_eq!(
            scheduler.task(task_gid).expect("admitted").generation,
            generation.checked_next().expect("next")
        );
    }
}

#[test]
fn source_replacement_requires_drain_and_commit_and_preserves_user_pause() {
    for pause in [false, true] {
        let at = MonotonicInstant::now();
        let mut scheduler = new_scheduler(2, 1, false);
        let task_gid = gid(1);
        let generation = make_active(&mut scheduler, task_id(1), task_gid, at);
        scheduler
            .execute_command_at(
                SchedulerCommand::BeginSourceReplacement { gid: task_gid },
                later(at, 4),
            )
            .expect("begin source quiescence");
        let view = scheduler.task(task_gid).expect("quiescing");
        assert_eq!(view.state, TaskState::PausedRestarting);
        assert!(!view.desired_paused);
        assert!(view.pending_source_replacement);
        let before = state_fingerprint(&scheduler);
        for command in [
            SchedulerCommand::BeginSourceReplacement { gid: task_gid },
            SchedulerCommand::CommitSourceReplacement {
                gid: task_gid,
                satisfies_credentials: None,
            },
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id: OptionPatchId::new(1).expect("patch"),
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: None,
            },
        ] {
            assert!(scheduler.execute_command_at(command, later(at, 5)).is_err());
            assert_eq!(state_fingerprint(&scheduler), before);
        }
        if pause {
            scheduler
                .execute_command_at(
                    SchedulerCommand::Pause {
                        gid: task_gid,
                        force: false,
                    },
                    later(at, 6),
                )
                .expect("user pause wins");
        }
        scheduler
            .handle_event_at(
                TaskEvent::CancellationDrained {
                    gid: task_gid,
                    generation,
                },
                later(at, 7),
            )
            .expect("drain");
        assert_eq!(scheduler.active_slot_count(), 0);
        assert!(matches!(
            scheduler.admit_next_at(later(at, 8)),
            Err(SchedulerError::NoEligibleTask)
        ));
        let outcome = scheduler
            .execute_command_at(
                SchedulerCommand::CommitSourceReplacement {
                    gid: task_gid,
                    satisfies_credentials: None,
                },
                later(at, 9),
            )
            .expect("commit source replacement");
        assert!(outcome.effects.iter().any(|effect| matches!(effect, TransitionEffect::PersistQueueTransition { desired_paused, .. } if *desired_paused == pause)));
        let view = scheduler.task(task_gid).expect("committed");
        assert!(!view.pending_source_replacement);
        assert_eq!(view.desired_paused, pause);
        assert_eq!(view.generation, generation);
        assert_eq!(
            view.state,
            if pause {
                TaskState::Paused
            } else {
                TaskState::Waiting
            }
        );
        if !pause {
            scheduler
                .admit_next_at(later(at, 10))
                .expect("ordinary admission after commit");
            assert_eq!(
                scheduler.task(task_gid).expect("admitted").generation,
                generation.checked_next().expect("next")
            );
        }
    }
}

#[test]
fn pause_during_restart_drain_preserves_patch_for_apply_before_readmission() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let (generation, patch_id) =
        make_active_restart_draining(&mut scheduler, internal_task, task_gid, at);

    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 6),
        )
        .expect("retarget restart drain to user pause");
    let view = scheduler.task(task_gid).expect("pause-retargeted restart");
    assert!(view.desired_paused);
    assert_eq!(view.pending_option_patch, Some(patch_id));
    assert_eq!(
        view.pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::Paused,
            force: false,
        })
    );

    scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 7),
        )
        .expect("drain retargeted restart");
    let paused = scheduler.task(task_gid).expect("paused staged patch");
    assert_eq!(paused.state, TaskState::Paused);
    assert_eq!(paused.pending_option_patch, Some(patch_id));
    assert_eq!(paused.pending_barrier, None);

    scheduler
        .execute_command_at(SchedulerCommand::Resume { gid: task_gid }, later(at, 8))
        .expect("resume staged patch");
    let apply = scheduler
        .admit_next_at(later(at, 9))
        .expect("apply staged patch before a new generation");
    assert!(apply.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::ApplyOptionPatch {
            task_id,
            gid,
            patch_id: actual_patch,
            satisfies_credentials: None,
        } if *task_id == internal_task && *gid == task_gid && *actual_patch == patch_id
    )));
    assert!(apply.effects.iter().all(|effect| !matches!(
        effect,
        TransitionEffect::PersistGenerationStarted { .. }
            | TransitionEffect::StartAllocation { .. }
    )));
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("patch application barrier")
            .pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id,
        })
    );

    scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id,
            },
            later(at, 10),
        )
        .expect("complete recovered patch application");
    let recovered = scheduler.task(task_gid).expect("recovered staged patch");
    assert_eq!(recovered.pending_option_patch, None);
    assert_eq!(recovered.pending_barrier, None);
}

#[test]
fn recovered_restart_patch_failure_retains_restart_provenance_and_terminalizes() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let (generation, patch_id) =
        make_active_restart_draining(&mut scheduler, internal_task, task_gid, at);
    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 6),
        )
        .expect("retarget restart drain to pause");
    scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 7),
        )
        .expect("finish retargeted drain");
    scheduler
        .execute_command_at(SchedulerCommand::Resume { gid: task_gid }, later(at, 8))
        .expect("resume staged restart patch");
    scheduler
        .admit_next_at(later(at, 9))
        .expect("apply recovered restart patch");

    let failed = scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplicationFailed {
                gid: task_gid,
                generation,
                patch_id,
                error: PublicError::new(
                    ErrorKind::OptionPatchRejected,
                    "restart option patch failed",
                    RetryClass::Never,
                ),
            },
            later(at, 10),
        )
        .expect("restart application failure");
    assert_transition(
        &failed,
        TaskState::Waiting,
        TaskState::Error,
        StateReason::RestartApplicationFailed,
    );
    assert!(failed.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::PersistTerminal {
            task_id,
            gid,
            status: Aria2Status::Error,
            ..
        } if *task_id == internal_task && *gid == task_gid
    )));
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("terminal restart failure")
            .pending_barrier,
        Some(PendingBarrier::TerminalPersistence {
            generation,
            status: Aria2Status::Error,
        })
    );
}

#[test]
fn pause_during_restart_application_preserves_barrier_and_lands_paused() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let (generation, patch_id) =
        make_active_restart_draining(&mut scheduler, internal_task, task_gid, at);

    let quiesced = scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 6),
        )
        .expect("quiesce restart generation");
    assert!(quiesced.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::ApplyOptionPatch {
            patch_id: actual_patch,
            ..
        } if *actual_patch == patch_id
    )));
    let application_barrier = PendingBarrier::OptionPatchApplication {
        generation,
        patch_id,
    };
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("restart application")
            .pending_barrier,
        Some(application_barrier)
    );

    let paused = scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 7),
        )
        .expect("record pause during patch application");
    assert!(paused.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::PersistQueueTransition {
            desired_paused: true,
            orders,
            ..
        } if orders == &vec![QueueOrder {
            class: QueueClass::Waiting,
            order: vec![task_gid],
        }]
    )));
    let view = scheduler.task(task_gid).expect("paused patch application");
    assert!(view.desired_paused);
    assert_eq!(view.pending_option_patch, Some(patch_id));
    assert_eq!(view.pending_barrier, Some(application_barrier));

    scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id,
            },
            later(at, 8),
        )
        .expect("complete patch after pause");
    let applied = scheduler.task(task_gid).expect("paused applied patch");
    assert_eq!(applied.state, TaskState::Paused);
    assert!(applied.desired_paused);
    assert_eq!(applied.pending_option_patch, None);
    assert_eq!(applied.pending_barrier, None);
}

#[test]
fn remove_during_restart_patch_persistence_waits_for_ack_then_drain() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut scheduler, internal_task, task_gid, at);
    let patch_id = OptionPatchId::new(1).expect("option patch id");
    scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            },
            later(at, 4),
        )
        .expect("stage restart patch");

    let removed = scheduler
        .execute_command_at(
            SchedulerCommand::Remove {
                gid: task_gid,
                force: false,
            },
            later(at, 5),
        )
        .expect("remove wins over patch persistence");
    assert!(removed.effects.iter().all(|effect| !matches!(
        effect,
        TransitionEffect::ApplyOptionPatch { .. } | TransitionEffect::PersistTerminal { .. }
    )));
    let deferred = scheduler.task(task_gid).expect("deferred removal");
    assert_eq!(
        deferred.pending_barrier,
        Some(PendingBarrier::OptionPatchPersistence {
            generation,
            patch_id,
        })
    );

    let persisted = scheduler
        .handle_event_at(
            TaskEvent::OptionPatchPersisted {
                gid: task_gid,
                generation,
                patch_id,
            },
            later(at, 6),
        )
        .expect("acknowledge persistence before removal drain");
    assert!(persisted.effects.iter().all(|effect| !matches!(
        effect,
        TransitionEffect::ApplyOptionPatch { .. } | TransitionEffect::PersistTerminal { .. }
    )));
    assert!(persisted.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::CancelGeneration {
            task_id,
            gid,
            generation: actual_generation,
            ..
        } if *task_id == internal_task && *gid == task_gid && *actual_generation == generation
    )));
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("removal drain")
            .pending_barrier,
        Some(PendingBarrier::CancellationDrain {
            generation,
            target: DrainTarget::Removed,
            force: false,
        })
    );

    let drained = scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 7),
        )
        .expect("finish removal drain");
    assert!(drained.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::PersistTerminal {
            task_id,
            gid,
            status: Aria2Status::Removed,
            ..
        } if *task_id == internal_task && *gid == task_gid
    )));
    assert!(
        drained
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::ApplyOptionPatch { .. }))
    );
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("terminal removal")
            .pending_option_patch,
        None
    );
}

#[test]
fn remove_during_restart_patch_application_waits_for_ack_then_terminalizes() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let (generation, patch_id) =
        make_active_restart_draining(&mut scheduler, internal_task, task_gid, at);
    scheduler
        .handle_event_at(
            TaskEvent::CancellationDrained {
                gid: task_gid,
                generation,
            },
            later(at, 6),
        )
        .expect("start restart patch application");

    let removed = scheduler
        .execute_command_at(
            SchedulerCommand::Remove {
                gid: task_gid,
                force: false,
            },
            later(at, 7),
        )
        .expect("remove wins over restart application");
    assert!(
        removed
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::PersistTerminal { .. }))
    );
    let deferred = scheduler.task(task_gid).expect("deferred restart removal");
    assert_eq!(
        deferred.pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id,
        })
    );

    let acknowledged = scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id,
            },
            later(at, 8),
        )
        .expect("acknowledge application before removal persistence");
    assert!(acknowledged.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::PersistTerminal {
            task_id,
            gid,
            status: Aria2Status::Removed,
            ..
        } if *task_id == internal_task && *gid == task_gid
    )));
    assert!(
        acknowledged
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::ApplyOptionPatch { .. }))
    );
    let view = scheduler.task(task_gid).expect("terminal restart removal");
    assert_eq!(view.pending_option_patch, None);
    assert_eq!(
        view.pending_barrier,
        Some(PendingBarrier::TerminalPersistence {
            generation,
            status: Aria2Status::Removed,
        })
    );
}

#[test]
fn remove_during_in_place_patch_application_waits_for_ack_then_terminalizes() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    add_task(&mut scheduler, internal_task, task_gid, at);
    let generation = scheduler.task(task_gid).expect("waiting task").generation;
    let patch_id = OptionPatchId::new(1).expect("option patch id");
    scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: None,
            },
            later(at, 1),
        )
        .expect("start in-place patch application");

    let removed = scheduler
        .execute_command_at(
            SchedulerCommand::Remove {
                gid: task_gid,
                force: false,
            },
            later(at, 2),
        )
        .expect("remove wins over in-place application");
    assert!(
        removed
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::PersistTerminal { .. }))
    );
    let deferred = scheduler.task(task_gid).expect("deferred in-place removal");
    assert_eq!(
        deferred.pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id,
        })
    );

    let acknowledged = scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id,
            },
            later(at, 3),
        )
        .expect("acknowledge in-place patch before removal persistence");
    assert!(acknowledged.effects.iter().any(|effect| matches!(
        effect,
        TransitionEffect::PersistTerminal {
            task_id,
            gid,
            status: Aria2Status::Removed,
            ..
        } if *task_id == internal_task && *gid == task_gid
    )));
    assert!(
        acknowledged
            .effects
            .iter()
            .all(|effect| !matches!(effect, TransitionEffect::ApplyOptionPatch { .. }))
    );
    let view = scheduler.task(task_gid).expect("terminal in-place removal");
    assert_eq!(view.pending_option_patch, None);
    assert_eq!(
        view.pending_barrier,
        Some(PendingBarrier::TerminalPersistence {
            generation,
            status: Aria2Status::Removed,
        })
    );
}

#[test]
fn credential_patch_ack_honors_pause_and_resume_during_application() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);

    let paused_requirement = CredentialRequirement {
        kind: CredentialKind::HttpAuthentication,
        source: None,
        safe_description: "HTTP credentials required".to_owned(),
    };
    let paused_key = paused_requirement.key();
    let paused_patch = OptionPatchId::new(1).expect("option patch id");
    let mut paused_scheduler = new_scheduler(1, 1, false);
    paused_scheduler
        .execute_command_at(
            SchedulerCommand::AddValidatedTask {
                task_id: internal_task,
                gid: task_gid,
                desired_paused: false,
                conditions: TaskConditions {
                    needs_credentials: Some(paused_requirement),
                    no_space: None,
                },
            },
            at,
        )
        .expect("add waiting credential task");
    let generation = paused_scheduler
        .task(task_gid)
        .expect("waiting credential task")
        .generation;
    paused_scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id: paused_patch,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: Some(paused_key),
            },
            later(at, 1),
        )
        .expect("start credential patch");
    paused_scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: task_gid,
                force: false,
            },
            later(at, 2),
        )
        .expect("record pause during credential application");
    let paused_pending = paused_scheduler
        .task(task_gid)
        .expect("pause-pending credential patch");
    assert!(paused_pending.desired_paused);
    assert_eq!(
        paused_pending.pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id: paused_patch,
        })
    );
    paused_scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id: paused_patch,
            },
            later(at, 3),
        )
        .expect("complete paused credential patch");
    let paused = paused_scheduler
        .task(task_gid)
        .expect("paused credential result");
    assert_eq!(paused.state, TaskState::Paused);
    assert!(paused.desired_paused);
    assert!(!paused.conditions.needs_credentials);
    assert_eq!(paused.pending_barrier, None);

    let resumed_requirement = CredentialRequirement {
        kind: CredentialKind::ProxyAuthentication,
        source: None,
        safe_description: "proxy credentials required".to_owned(),
    };
    let resumed_key = resumed_requirement.key();
    let resumed_patch = OptionPatchId::new(1).expect("option patch id");
    let mut resumed_scheduler = new_scheduler(1, 1, false);
    resumed_scheduler
        .execute_command_at(
            SchedulerCommand::AddValidatedTask {
                task_id: internal_task,
                gid: task_gid,
                desired_paused: true,
                conditions: TaskConditions {
                    needs_credentials: Some(resumed_requirement),
                    no_space: None,
                },
            },
            later(at, 10),
        )
        .expect("add paused credential task");
    let generation = resumed_scheduler
        .task(task_gid)
        .expect("paused credential task")
        .generation;
    resumed_scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id: resumed_patch,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: Some(resumed_key),
            },
            later(at, 11),
        )
        .expect("start paused credential patch");
    resumed_scheduler
        .execute_command_at(SchedulerCommand::Resume { gid: task_gid }, later(at, 12))
        .expect("record resume during credential application");
    let resumed_pending = resumed_scheduler
        .task(task_gid)
        .expect("resume-pending credential patch");
    assert!(!resumed_pending.desired_paused);
    assert_eq!(
        resumed_pending.pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id: resumed_patch,
        })
    );
    resumed_scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id: resumed_patch,
            },
            later(at, 13),
        )
        .expect("complete resumed credential patch");
    let resumed = resumed_scheduler
        .task(task_gid)
        .expect("resumed credential result");
    assert_eq!(resumed.state, TaskState::Waiting);
    assert!(!resumed.desired_paused);
    assert!(!resumed.conditions.needs_credentials);
    assert_eq!(resumed.pending_barrier, None);
}

#[test]
fn option_patch_ids_cannot_be_reused_or_aliased_by_delayed_acknowledgements() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    add_task(&mut scheduler, internal_task, task_gid, at);
    let generation = scheduler.task(task_gid).expect("waiting task").generation;
    let first_patch = OptionPatchId::new(7).expect("option patch id");

    let first = scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id: first_patch,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: None,
            },
            later(at, 1),
        )
        .expect("start first option patch");
    assert_transition(
        &first,
        TaskState::Waiting,
        TaskState::Waiting,
        StateReason::OptionPatch,
    );
    assert_eq!(
        first.effects,
        vec![TransitionEffect::ApplyOptionPatch {
            task_id: internal_task,
            gid: task_gid,
            patch_id: first_patch,
            satisfies_credentials: None,
        }]
    );
    scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id: first_patch,
            },
            later(at, 2),
        )
        .expect("complete first option patch");
    let completed = scheduler.task(task_gid).expect("completed first patch");
    assert_eq!(completed.task_id, internal_task);
    assert_eq!(completed.pending_option_patch, None);
    assert_eq!(completed.pending_barrier, None);

    let before_reuse = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id: first_patch,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: None,
            },
            later(at, 3),
        ),
        Err(SchedulerError::OptionPatchIdCollision)
    );
    assert_eq!(state_fingerprint(&scheduler), before_reuse);

    let next_patch = OptionPatchId::new(8).expect("higher option patch id");
    let next = scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id: next_patch,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: None,
            },
            later(at, 4),
        )
        .expect("start higher option patch");
    assert_transition(
        &next,
        TaskState::Waiting,
        TaskState::Waiting,
        StateReason::OptionPatch,
    );
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("higher patch pending")
            .pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id: next_patch,
        })
    );

    let before_delayed_ack = state_fingerprint(&scheduler);
    let delayed = scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id: first_patch,
            },
            later(at, 5),
        )
        .expect("ignore delayed first-patch acknowledgement");
    assert_ignored(&delayed, StateReason::DuplicateEventIgnored);
    assert_eq!(state_fingerprint(&scheduler), before_delayed_ack);
    let still_pending = scheduler
        .task(task_gid)
        .expect("higher patch still pending");
    assert_eq!(still_pending.task_id, internal_task);
    assert_eq!(still_pending.pending_option_patch, Some(next_patch));
    assert_eq!(
        still_pending.pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id: next_patch,
        })
    );

    let completed_next = scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id: next_patch,
            },
            later(at, 6),
        )
        .expect("complete higher option patch");
    assert_transition(
        &completed_next,
        TaskState::Waiting,
        TaskState::Waiting,
        StateReason::OptionPatch,
    );
    let final_view = scheduler.task(task_gid).expect("completed higher patch");
    assert_eq!(final_view.task_id, internal_task);
    assert_eq!(final_view.pending_option_patch, None);
    assert_eq!(final_view.pending_barrier, None);
}

#[test]
fn credential_satisfaction_requires_exact_key_and_clears_only_after_apply_ack() {
    let at = MonotonicInstant::now();
    let internal_task = task_id(1);
    let task_gid = gid(1);
    let requirement = CredentialRequirement {
        kind: CredentialKind::HttpAuthentication,
        source: None,
        safe_description: "HTTP credentials required".to_owned(),
    };
    let exact_key = requirement.key();
    let wrong_key = CredentialRequirement {
        kind: CredentialKind::ProxyAuthentication,
        source: None,
        safe_description: "proxy credentials required".to_owned(),
    }
    .key();
    let mut scheduler = new_scheduler(1, 1, false);
    scheduler
        .execute_command_at(
            SchedulerCommand::AddValidatedTask {
                task_id: internal_task,
                gid: task_gid,
                desired_paused: false,
                conditions: TaskConditions {
                    needs_credentials: Some(requirement),
                    no_space: None,
                },
            },
            at,
        )
        .expect("add credential-blocked task");
    let generation = scheduler.task(task_gid).expect("blocked task").generation;

    let before_mismatch = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id: OptionPatchId::new(1).expect("patch id"),
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: Some(wrong_key),
            },
            later(at, 1),
        ),
        Err(SchedulerError::StaleCredentialRequirement)
    );
    assert_eq!(state_fingerprint(&scheduler), before_mismatch);

    let patch_id = OptionPatchId::new(2).expect("patch id");
    let applied = scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::InPlace,
                satisfies_credentials: Some(exact_key),
            },
            later(at, 2),
        )
        .expect("apply exact credential-bearing patch");
    assert_transition(
        &applied,
        TaskState::Waiting,
        TaskState::Waiting,
        StateReason::OptionPatch,
    );
    assert_eq!(
        applied.effects,
        vec![TransitionEffect::ApplyOptionPatch {
            task_id: internal_task,
            gid: task_gid,
            patch_id,
            satisfies_credentials: Some(exact_key),
        }]
    );
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("patch pending")
            .pending_barrier,
        Some(PendingBarrier::OptionPatchApplication {
            generation,
            patch_id,
        })
    );
    assert!(
        scheduler
            .snapshot(task_gid)
            .expect("blocked snapshot before acknowledgement")
            .conditions
            .needs_credentials
    );

    let acknowledged = scheduler
        .handle_event_at(
            TaskEvent::OptionPatchApplied {
                gid: task_gid,
                generation,
                patch_id,
            },
            later(at, 3),
        )
        .expect("acknowledge exact credential patch");
    let snapshot = scheduler
        .snapshot(task_gid)
        .expect("unblocked snapshot after acknowledgement");
    assert_transition(
        &acknowledged,
        TaskState::Waiting,
        TaskState::Waiting,
        StateReason::CredentialsSatisfied,
    );
    assert_eq!(
        acknowledged.effects,
        vec![TransitionEffect::PublishSnapshot {
            task_id: internal_task,
            snapshot: snapshot.clone(),
        }]
    );
    assert!(!snapshot.conditions.needs_credentials);
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("unblocked task")
            .pending_barrier,
        None
    );
}

#[test]
fn source_commit_requires_matching_credential_key_and_preserves_pause() {
    for paused in [false, true] {
        let at = MonotonicInstant::now();
        let task_gid = gid(1);
        let requirement = CredentialRequirement {
            kind: CredentialKind::SourceUri,
            source: Some(ariax_core::UriId::new(0)),
            safe_description: "source required after restart".to_owned(),
        };
        let key = requirement.key();
        let mut scheduler = new_scheduler(1, 1, false);
        scheduler
            .execute_command_at(
                SchedulerCommand::AddValidatedTask {
                    task_id: task_id(1),
                    gid: task_gid,
                    desired_paused: paused,
                    conditions: TaskConditions {
                        needs_credentials: Some(requirement),
                        no_space: None,
                    },
                },
                at,
            )
            .expect("blocked task");
        assert_eq!(scheduler.credential_requirement_key(task_gid), Some(key));
        scheduler
            .execute_command_at(
                SchedulerCommand::BeginSourceReplacement { gid: task_gid },
                later(at, 1),
            )
            .expect("source replacement");
        let before = state_fingerprint(&scheduler);
        let wrong = CredentialRequirement {
            kind: CredentialKind::ProxyAuthentication,
            source: None,
            safe_description: String::new(),
        }
        .key();
        assert_eq!(
            scheduler.execute_command_at(
                SchedulerCommand::CommitSourceReplacement {
                    gid: task_gid,
                    satisfies_credentials: Some(wrong),
                },
                later(at, 2)
            ),
            Err(SchedulerError::StaleCredentialRequirement)
        );
        assert_eq!(state_fingerprint(&scheduler), before);
        let mut unsatisfied = scheduler.clone();
        unsatisfied
            .execute_command_at(
                SchedulerCommand::CommitSourceReplacement {
                    gid: task_gid,
                    satisfies_credentials: None,
                },
                later(at, 2),
            )
            .expect("commit without credential assertion");
        assert!(
            unsatisfied
                .task(task_gid)
                .expect("still blocked")
                .conditions
                .needs_credentials
        );
        let committed = scheduler
            .execute_command_at(
                SchedulerCommand::CommitSourceReplacement {
                    gid: task_gid,
                    satisfies_credentials: Some(key),
                },
                later(at, 3),
            )
            .expect("matching source replacement");
        assert!(
            committed
                .effects
                .iter()
                .any(|effect| matches!(effect, TransitionEffect::PersistQueueTransition { .. }))
        );
        let view = scheduler.task(task_gid).expect("committed");
        assert!(!view.conditions.needs_credentials);
        assert_eq!(view.desired_paused, paused);
        assert_eq!(
            view.state,
            if paused {
                TaskState::Paused
            } else {
                TaskState::Waiting
            }
        );
        assert_eq!(scheduler.credential_requirement_key(task_gid), None);
    }
}

#[test]
fn source_commit_cannot_clear_an_unrelated_matching_credential_requirement() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let requirement = CredentialRequirement {
        kind: CredentialKind::ProxyAuthentication,
        source: None,
        safe_description: "proxy credentials required".to_owned(),
    };
    let key = requirement.key();
    let mut scheduler = new_scheduler(1, 1, false);
    scheduler
        .execute_command_at(
            SchedulerCommand::AddValidatedTask {
                task_id: task_id(1),
                gid: task_gid,
                desired_paused: true,
                conditions: TaskConditions {
                    needs_credentials: Some(requirement),
                    no_space: None,
                },
            },
            at,
        )
        .expect("blocked task");
    scheduler
        .execute_command_at(
            SchedulerCommand::BeginSourceReplacement { gid: task_gid },
            later(at, 1),
        )
        .expect("begin");
    let before = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(
            SchedulerCommand::CommitSourceReplacement {
                gid: task_gid,
                satisfies_credentials: Some(key),
            },
            later(at, 2)
        ),
        Err(SchedulerError::StaleCredentialRequirement)
    );
    assert_eq!(state_fingerprint(&scheduler), before);
    scheduler
        .execute_command_at(
            SchedulerCommand::CommitSourceReplacement {
                gid: task_gid,
                satisfies_credentials: None,
            },
            later(at, 3),
        )
        .expect("source-only commit");
    assert_eq!(scheduler.credential_requirement_key(task_gid), Some(key));
}

#[test]
fn live_completion_is_rejected_atomically_during_option_patch_persistence() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    let generation = make_active(&mut scheduler, task_id(1), task_gid, at);
    let patch_id = OptionPatchId::new(1).expect("non-zero option patch id");

    let staged = scheduler
        .execute_command_at(
            SchedulerCommand::ApplyOptionPatch {
                gid: task_gid,
                patch_id,
                kind: ValidatedOptionPatchKind::ActiveRestart,
                satisfies_credentials: None,
            },
            later(at, 4),
        )
        .expect("stage active-restart option patch");
    assert_transition(
        &staged,
        TaskState::Active,
        TaskState::Active,
        StateReason::OptionPatch,
    );
    assert_eq!(
        scheduler
            .task(task_gid)
            .expect("staged task")
            .pending_barrier,
        Some(PendingBarrier::OptionPatchPersistence {
            generation,
            patch_id,
        })
    );

    let before = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.handle_event_at(
            TaskEvent::DataComplete {
                gid: task_gid,
                generation,
                seed: false,
            },
            later(at, 5),
        ),
        Err(SchedulerError::PendingBarrier {
            barrier: PendingBarrier::OptionPatchPersistence {
                generation,
                patch_id,
            },
            operation: "data_complete",
        })
    );
    assert_eq!(state_fingerprint(&scheduler), before);
}

#[test]
fn shutdown_guard_and_effect_cap_are_mutation_free() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(2, 1, false);
    add_task(&mut scheduler, task_id(1), task_gid, at);

    let before = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(SchedulerCommand::OrderlyShutdown, later(at, 1)),
        Err(SchedulerError::ShutdownBatchRequired)
    );
    assert_eq!(state_fingerprint(&scheduler), before);

    let over_cap = vec![
        TransitionEffect::PersistConditions {
            task_id: task_id(1),
            gid: task_gid,
            conditions: TaskConditions::default(),
        };
        MAX_SCHEDULER_EFFECTS + 1
    ];
    assert_eq!(
        SchedulerOutcome::checked(None, None, over_cap),
        Err(SchedulerError::EffectLimitExceeded)
    );

    let at_cap = vec![
        TransitionEffect::PersistConditions {
            task_id: task_id(1),
            gid: task_gid,
            conditions: TaskConditions::default(),
        };
        MAX_SCHEDULER_EFFECTS
    ];
    assert_eq!(
        SchedulerOutcome::checked(None, None, at_cap.clone())
            .expect("effect list at hard cap")
            .effects,
        at_cap
    );
}

#[test]
fn command_and_event_rejections_are_atomic() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    add_task(&mut scheduler, task_id(1), task_gid, at);
    scheduler.admit_next_at(later(at, 1)).expect("admit task");
    let generation = scheduler.task(task_gid).expect("task view").generation;

    let before_pending = state_fingerprint(&scheduler);
    assert!(matches!(
        scheduler.handle_event_at(
            TaskEvent::AllocationSucceeded {
                gid: task_gid,
                generation,
            },
            later(at, 2),
        ),
        Err(SchedulerError::PendingBarrier { .. })
    ));
    assert_eq!(state_fingerprint(&scheduler), before_pending);

    let future = generation.checked_next().expect("future generation");
    let before_future = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.handle_event_at(
            TaskEvent::GenerationPersisted {
                gid: task_gid,
                generation: future,
            },
            later(at, 3),
        ),
        Err(SchedulerError::StaleGeneration {
            expected: generation,
            actual: future,
        })
    );
    assert_eq!(state_fingerprint(&scheduler), before_future);

    let before_position = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.execute_command_at(
            SchedulerCommand::ChangePosition {
                gid: task_gid,
                position: 1,
            },
            later(at, 4),
        ),
        Err(SchedulerError::PendingBarrier {
            barrier: PendingBarrier::GenerationPersistence { generation },
            operation: "change_position",
        })
    );
    assert_eq!(state_fingerprint(&scheduler), before_position);
}

#[test]
fn planning_copy_forecast_tracks_nested_conditions_and_rejected_inputs() {
    let at = MonotonicInstant::now();
    for count in [1, 17, 257, 1000] {
        let mut scheduler = new_scheduler(count, count, false);
        let mut previous = scheduler.estimated_clone_bytes();
        for index in 1..=count {
            scheduler
                .execute_command_at(
                    SchedulerCommand::AddValidatedTask {
                        task_id: task_id(index as u64),
                        gid: gid(index as u64),
                        desired_paused: true,
                        conditions: TaskConditions {
                            needs_credentials: Some(CredentialRequirement {
                                kind: CredentialKind::HttpAuthentication,
                                source: None,
                                safe_description: "x".repeat(128),
                            }),
                            no_space: Some(NoSpaceCondition {
                                redacted_path: "y".repeat(128),
                                retry_at: None,
                            }),
                        },
                    },
                    at,
                )
                .expect("bounded blocked task");
            let estimate = scheduler.estimated_clone_bytes();
            assert!(estimate >= previous + 256);
            previous = estimate;
        }
        let copy = scheduler.clone();
        assert_eq!(copy.estimated_clone_bytes(), previous);
        assert_eq!(copy.0, scheduler.0);
        assert!(
            scheduler
                .execute_command_at(add_command(task_id(1), gid(1), false), at)
                .is_err()
        );
        assert_eq!(scheduler.estimated_clone_bytes(), previous);
    }
}

#[test]
fn oversized_condition_payloads_are_rejected_without_mutation() {
    let at = MonotonicInstant::now();
    let task_gid = gid(1);
    let mut scheduler = new_scheduler(1, 1, false);
    assert_eq!(
        scheduler.execute_command_at(
            SchedulerCommand::AddValidatedTask {
                task_id: task_id(1),
                gid: task_gid,
                desired_paused: false,
                conditions: TaskConditions {
                    needs_credentials: Some(CredentialRequirement {
                        kind: CredentialKind::HttpAuthentication,
                        source: None,
                        safe_description: "x".repeat(MAX_CONDITION_DESCRIPTION_BYTES + 1),
                    }),
                    no_space: None,
                },
            },
            at,
        ),
        Err(SchedulerError::InvalidTaskConditions)
    );
    assert!(scheduler.is_empty());

    let generation = make_active(&mut scheduler, task_id(1), task_gid, later(at, 1));
    let before = state_fingerprint(&scheduler);
    assert_eq!(
        scheduler.handle_event_at(
            TaskEvent::NoSpace {
                gid: task_gid,
                generation,
                condition: NoSpaceCondition {
                    redacted_path: "x".repeat(MAX_REDACTED_PATH_BYTES + 1),
                    retry_at: None,
                },
            },
            later(at, 5),
        ),
        Err(SchedulerError::InvalidTaskConditions)
    );
    assert_eq!(state_fingerprint(&scheduler), before);
}

fn recovered_task(task: u64, gid_value: u64, state: TaskState) -> RecoveredSchedulerTask {
    RecoveredSchedulerTask {
        task_id: task_id(task),
        gid: gid(gid_value),
        state,
        generation: Generation::new(2),
        generation_started: true,
        desired_paused: matches!(state, TaskState::Paused),
        conditions: TaskConditions::default(),
        slow_demotion_count: 0,
        slow_slot: None,
        retry_at: None,
        host_key_challenge: None,
        error: None,
        stopped_status: None,
    }
}

fn recovered_queues(
    waiting: Vec<Gid>,
    demoted: Vec<Gid>,
    paused: Vec<Gid>,
    stopped: Vec<Gid>,
) -> Vec<QueueOrder> {
    vec![
        QueueOrder {
            class: QueueClass::Waiting,
            order: waiting,
        },
        QueueOrder {
            class: QueueClass::Demoted,
            order: demoted,
        },
        QueueOrder {
            class: QueueClass::Paused,
            order: paused,
        },
        QueueOrder {
            class: QueueClass::Active,
            order: vec![],
        },
        QueueOrder {
            class: QueueClass::Stopped,
            order: stopped,
        },
    ]
}

#[test]
fn restore_rebuilds_exact_membership_timers_snapshots_and_task_identity() {
    let at = MonotonicInstant::now();
    let mut waiting = recovered_task(1, 1, TaskState::Waiting);
    waiting.generation_started = false;

    let mut retry = recovered_task(2, 2, TaskState::RetryWait);
    retry.retry_at = Some(later(at, 5));
    retry.conditions.no_space = Some(NoSpaceCondition {
        redacted_path: "retry-output.bin".to_owned(),
        retry_at: Some(later(at, 6)),
    });

    let mut slow = recovered_task(3, 3, TaskState::WaitingSlow);
    slow.slow_demotion_count = 2;
    slow.slow_slot = Some(SlowSlotPersistence {
        original_position: 1,
        demotion_count: 2,
        decision: SlowReadmissionDecision {
            readmit_at: later(at, 7),
            scheduled_at_ms: 1_000,
            delay_ms: 7_000,
        },
    });
    slow.conditions.no_space = Some(NoSpaceCondition {
        redacted_path: "slow-output.bin".to_owned(),
        retry_at: Some(later(at, 8)),
    });

    let key = vec![9, 8, 7, 6];
    let fingerprint = HostKeyFingerprint::for_presented_key(&key);
    let mut host_key = recovered_task(4, 4, TaskState::PausedHostKey);
    host_key.host_key_challenge = Some(
        PresentedHostKeyChallenge::new(
            HostKeyChallenge {
                id: HostKeyChallengeId::new([4; 16]),
                canonical_host: "sftp.example.test".to_owned(),
                port: 22,
                algorithm: "ssh-ed25519".to_owned(),
                fingerprint_sha256: fingerprint,
            },
            key,
        )
        .expect("bounded recovered challenge"),
    );

    let mut stopped = recovered_task(5, 5, TaskState::StoppedResult);
    stopped.stopped_status = Some(Aria2Status::Complete);

    let config = SchedulerConfig::new(
        NonZeroUsize::new(8).expect("task cap"),
        NonZeroUsize::new(2).expect("active cap"),
        true,
    )
    .expect("valid config");
    let (mut scheduler, mut plan) = RequestScheduler::restore(
        config,
        SchedulerRestoreBatch::new(
            vec![waiting, retry, slow, host_key, stopped],
            recovered_queues(
                vec![gid(1), gid(2)],
                vec![gid(3)],
                vec![gid(4)],
                vec![gid(5)],
            ),
        ),
    )
    .expect("valid recovery batch");

    assert_eq!(
        scheduler.queue_snapshot(QueueClass::Waiting),
        vec![gid(1), gid(2)]
    );
    assert_eq!(scheduler.queue_snapshot(QueueClass::Demoted), vec![gid(3)]);
    assert_eq!(scheduler.queue_snapshot(QueueClass::Paused), vec![gid(4)]);
    assert_eq!(
        scheduler.queue_snapshot(QueueClass::Active),
        Vec::<Gid>::new()
    );
    assert_eq!(scheduler.queue_snapshot(QueueClass::Stopped), vec![gid(5)]);
    assert_eq!(scheduler.active_slot_count(), 0);
    assert!(
        scheduler
            .task(gid(2))
            .expect("retry task")
            .retry_timer
            .is_some()
    );
    assert!(
        scheduler
            .task(gid(3))
            .expect("slow task")
            .slow_readmission
            .is_some()
    );
    assert!(
        scheduler
            .task(gid(2))
            .expect("retry task")
            .no_space_probe
            .is_some()
    );
    assert!(
        scheduler
            .task(gid(3))
            .expect("slow task")
            .no_space_probe
            .is_some()
    );
    assert_eq!(
        scheduler
            .snapshot(gid(5))
            .expect("stopped snapshot")
            .wire_status(),
        Ok(Aria2Status::Complete)
    );
    assert!(
        scheduler
            .snapshot(gid(4))
            .expect("host-key snapshot")
            .host_key_challenge
            .is_some()
    );
    assert!(plan.is_bound_to(&scheduler));

    let effects = plan.next_batch();
    assert_eq!(effects.len(), MAX_SCHEDULER_EFFECTS);
    assert!(matches!(
        effects[1],
        TransitionEffect::ScheduleRetry { gid: effect_gid, .. } if effect_gid == gid(2)
    ));
    assert!(matches!(
        effects[2],
        TransitionEffect::ProbeNoSpace {
            gid: effect_gid,
            origin: NoSpaceProbeOrigin::AutomaticRetry,
            at: probe_at,
            ..
        } if effect_gid == gid(2) && probe_at == later(at, 6)
    ));
    assert!(matches!(
        effects[4],
        TransitionEffect::ScheduleSlowReadmission { gid: effect_gid, .. } if effect_gid == gid(3)
    ));
    assert!(matches!(
        effects[5],
        TransitionEffect::ProbeNoSpace {
            gid: effect_gid,
            origin: NoSpaceProbeOrigin::AutomaticRetry,
            at: probe_at,
            ..
        } if effect_gid == gid(3) && probe_at == later(at, 8)
    ));
    assert_eq!(plan.next_batch().len(), 1);
    assert!(plan.is_empty());

    scheduler
        .execute_command_at(
            SchedulerCommand::Pause {
                gid: gid(1),
                force: false,
            },
            later(at, 4),
        )
        .expect("keep the ordinary waiting task out of the admission race");
    let retry_view = scheduler.task(gid(2)).expect("retry task");
    let retry_timer_id = retry_view.retry_timer.expect("retry timer");
    let probe_id = retry_view.no_space_probe.expect("no-space probe");
    let blocked = scheduler
        .handle_event_at(
            &TaskEvent::RetryReady {
                gid: gid(2),
                generation: retry_view.generation,
                retry_timer_id,
            }
            .for_task(retry_view.task_id),
            later(at, 5),
        )
        .expect("retry timer is ready but no-space still blocks admission");
    assert_transition(
        &blocked,
        TaskState::RetryWait,
        TaskState::RetryWait,
        StateReason::AdmissionBlocked,
    );
    let cleared = scheduler
        .handle_event_at(
            &TaskEvent::NoSpaceProbeCompleted {
                gid: gid(2),
                generation: retry_view.generation,
                probe_id,
                origin: NoSpaceProbeOrigin::AutomaticRetry,
                ready: true,
                next_retry_at: None,
            }
            .for_task(retry_view.task_id),
            later(at, 6),
        )
        .expect("clear the independent no-space gate");
    assert_transition(
        &cleared,
        TaskState::RetryWait,
        TaskState::RetryWait,
        StateReason::NoSpaceProbeSucceeded,
    );
    let admitted = scheduler
        .admit_next_at(later(at, 7))
        .expect("ready retry becomes eligible after no-space clears");
    assert_transition(
        &admitted,
        TaskState::RetryWait,
        TaskState::Allocating,
        StateReason::RetryReadmission,
    );
}

#[test]
fn restore_plan_never_exceeds_the_dispatcher_effect_bound() {
    let tasks = (1..=9)
        .map(|value| recovered_task(value, value, TaskState::Waiting))
        .collect::<Vec<_>>();
    let config = SchedulerConfig::new(
        NonZeroUsize::new(9).expect("task cap"),
        NonZeroUsize::new(1).expect("active cap"),
        false,
    )
    .expect("valid config");
    let (_, mut plan) = RequestScheduler::restore(
        config,
        SchedulerRestoreBatch::new(
            tasks,
            recovered_queues((1..=9).map(gid).collect(), vec![], vec![], vec![]),
        ),
    )
    .expect("valid recovery batch");

    assert_eq!(plan.next_batch().len(), MAX_SCHEDULER_EFFECTS);
    assert_eq!(plan.next_batch().len(), 1);
    assert!(plan.is_empty());
}

#[test]
fn restore_rejects_unsafe_states_and_inconsistent_state_metadata() {
    let config = SchedulerConfig::new(
        NonZeroUsize::new(2).expect("task cap"),
        NonZeroUsize::new(1).expect("active cap"),
        false,
    )
    .expect("valid config");

    let active = recovered_task(1, 1, TaskState::Active);
    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(
                vec![active],
                recovered_queues(vec![], vec![], vec![], vec![]),
            ),
        )
        .expect_err("active recovery state must be rejected"),
        SchedulerRestoreError::InvalidState {
            gid: gid(1),
            state: TaskState::Active,
        }
    );

    let retry = recovered_task(1, 1, TaskState::RetryWait);
    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(
                vec![retry],
                recovered_queues(vec![gid(1)], vec![], vec![], vec![]),
            ),
        )
        .expect_err("retry-wait without a deadline must be rejected"),
        SchedulerRestoreError::InvalidRetryWait(gid(1))
    );

    let host_key = recovered_task(1, 1, TaskState::PausedHostKey);
    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(
                vec![host_key],
                recovered_queues(vec![], vec![], vec![gid(1)], vec![]),
            ),
        )
        .expect_err("host-key pause without a challenge must be rejected"),
        SchedulerRestoreError::InvalidHostKeyState(gid(1))
    );

    let mut stopped = recovered_task(1, 1, TaskState::StoppedResult);
    stopped.stopped_status = Some(Aria2Status::Error);
    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(
                vec![stopped],
                recovered_queues(vec![], vec![], vec![], vec![gid(1)]),
            ),
        )
        .expect_err("error result without public error metadata must be rejected"),
        SchedulerRestoreError::InvalidStoppedResult(gid(1))
    );

    let mut slow = recovered_task(1, 1, TaskState::WaitingSlow);
    slow.slow_demotion_count = 1;
    slow.slow_slot = Some(SlowSlotPersistence {
        original_position: config.max_tasks.get(),
        demotion_count: 1,
        decision: SlowReadmissionDecision {
            readmit_at: MonotonicInstant::now(),
            scheduled_at_ms: 1,
            delay_ms: 1,
        },
    });
    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(
                vec![slow],
                recovered_queues(vec![], vec![gid(1)], vec![], vec![]),
            ),
        )
        .expect_err("slow original position at the task cap must be rejected"),
        SchedulerRestoreError::InvalidSlowState(gid(1))
    );
}

#[test]
fn restore_rejects_incomplete_duplicate_and_mismatched_membership() {
    let config = SchedulerConfig::new(
        NonZeroUsize::new(2).expect("task cap"),
        NonZeroUsize::new(1).expect("active cap"),
        false,
    )
    .expect("valid config");
    let task = recovered_task(1, 1, TaskState::Waiting);

    let mut missing_class = recovered_queues(vec![gid(1)], vec![], vec![], vec![]);
    missing_class.pop();
    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(vec![task.clone()], missing_class),
        )
        .expect_err("incomplete queue set must be rejected"),
        SchedulerRestoreError::MissingQueue(QueueClass::Stopped)
    );

    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(
                vec![task.clone()],
                recovered_queues(vec![], vec![], vec![gid(1)], vec![]),
            ),
        )
        .expect_err("wrong queue class must be rejected"),
        SchedulerRestoreError::QueueClassMismatch {
            gid: gid(1),
            expected: QueueClass::Waiting,
            actual: QueueClass::Paused,
        }
    );

    assert_eq!(
        RequestScheduler::restore(
            config,
            SchedulerRestoreBatch::new(
                vec![task],
                recovered_queues(vec![gid(1), gid(1)], vec![], vec![], vec![]),
            ),
        )
        .expect_err("duplicate queue membership must be rejected"),
        SchedulerRestoreError::DuplicateQueueMember(gid(1))
    );
}
