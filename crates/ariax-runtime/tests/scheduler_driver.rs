use ariax_core::{
    Aria2Status, Generation, Gid, MonotonicInstant, QueueClass, QueueOrder, RecoveredSchedulerTask,
    RequestScheduler, SchedulerCommand, SchedulerConfig, SchedulerRestoreBatch, TaskConditions,
    TaskEvent, TaskId, TaskState, TransitionEffect, TransitionEffectKind,
};
use ariax_runtime::{
    DispatchedEffect, EffectCompletion, EffectDispatchId, EffectSinkError, SchedulerDriver,
    SchedulerDriverFault, SchedulerDriverInputError, SchedulerDriverPoll,
    SchedulerDriverPrepareError, SchedulerEffectSink, SchedulerEffectSinkPrepare,
};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DispatchBehavior {
    Accept,
    Full,
    Closed,
    Failed,
}

#[derive(Debug)]
struct TestSink {
    behavior: VecDeque<DispatchBehavior>,
    offers: Vec<DispatchedEffect>,
    completions: Rc<RefCell<VecDeque<EffectCompletion>>>,
    auto_complete: Rc<Cell<bool>>,
}

#[derive(Clone, Debug)]
struct TestSinkControl {
    completions: Rc<RefCell<VecDeque<EffectCompletion>>>,
    auto_complete: Rc<Cell<bool>>,
}

impl TestSinkControl {
    fn push_completion(&self, completion: EffectCompletion) {
        self.completions.borrow_mut().push_back(completion);
    }

    fn set_auto_complete(&self, auto_complete: bool) {
        self.auto_complete.set(auto_complete);
    }
}

impl TestSink {
    fn automatic() -> Self {
        Self::controlled(true).0
    }

    fn manual_with_control() -> (Self, TestSinkControl) {
        Self::controlled(false)
    }

    fn automatic_with_control() -> (Self, TestSinkControl) {
        Self::controlled(true)
    }

    fn controlled(auto_complete: bool) -> (Self, TestSinkControl) {
        let completions = Rc::new(RefCell::new(VecDeque::new()));
        let auto_complete = Rc::new(Cell::new(auto_complete));
        let control = TestSinkControl {
            completions: Rc::clone(&completions),
            auto_complete: Rc::clone(&auto_complete),
        };
        let sink = Self {
            behavior: VecDeque::new(),
            offers: Vec::new(),
            completions,
            auto_complete,
        };
        (sink, control)
    }
}

impl SchedulerEffectSink for TestSink {
    fn poll_dispatch(&mut self, effect: &DispatchedEffect) -> Poll<Result<(), EffectSinkError>> {
        self.offers.push(effect.clone());
        match self
            .behavior
            .pop_front()
            .unwrap_or(DispatchBehavior::Accept)
        {
            DispatchBehavior::Accept => {
                if self.auto_complete.get() {
                    self.completions
                        .borrow_mut()
                        .push_back(auto_completion(effect));
                }
                Poll::Ready(Ok(()))
            }
            DispatchBehavior::Full => Poll::Pending,
            DispatchBehavior::Closed => Poll::Ready(Err(EffectSinkError::Closed)),
            DispatchBehavior::Failed => Poll::Ready(Err(EffectSinkError::Failed)),
        }
    }

    fn poll_completion(&mut self) -> Poll<EffectCompletion> {
        self.completions
            .borrow_mut()
            .pop_front()
            .map_or(Poll::Pending, Poll::Ready)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestSinkPreparation {
    SetAutoComplete(bool),
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestPreparationRejected;

impl SchedulerEffectSinkPrepare for TestSink {
    type Preparation = TestSinkPreparation;
    type Error = TestPreparationRejected;

    fn prepare(&mut self, preparation: Self::Preparation) -> Result<(), Self::Error> {
        match preparation {
            TestSinkPreparation::SetAutoComplete(auto_complete) => {
                self.auto_complete.set(auto_complete);
                Ok(())
            }
            TestSinkPreparation::Reject => Err(TestPreparationRejected),
        }
    }
}

fn auto_completion(effect: &DispatchedEffect) -> EffectCompletion {
    let task_id = effect.effect().task_id();
    let gid = effect.effect().gid();
    let generation = effect.task_generation();
    let acknowledgement = match effect.effect() {
        TransitionEffect::StageOptionPatch { patch_id, .. } => Some(
            TaskEvent::OptionPatchPersisted {
                gid,
                generation,
                patch_id: *patch_id,
            }
            .for_task(task_id),
        ),
        TransitionEffect::ApplyOptionPatch { patch_id, .. } => Some(
            TaskEvent::OptionPatchApplied {
                gid,
                generation,
                patch_id: *patch_id,
            }
            .for_task(task_id),
        ),
        TransitionEffect::PersistGenerationStarted { .. } => {
            Some(TaskEvent::GenerationPersisted { gid, generation }.for_task(task_id))
        }
        TransitionEffect::PersistHostKeyPinAndClearChallenge { resolution_id, .. } => Some(
            TaskEvent::HostKeyResolutionPersisted {
                gid,
                generation,
                resolution_id: *resolution_id,
            }
            .for_task(task_id),
        ),
        TransitionEffect::PersistTerminal { status, .. } => Some(
            TaskEvent::TerminalPersisted {
                gid,
                generation,
                status: *status,
            }
            .for_task(task_id),
        ),
        TransitionEffect::DeleteStoppedTaskMetadata { deletion_id, .. } => Some(
            TaskEvent::StoppedResultDeleted {
                gid,
                generation,
                deletion_id: *deletion_id,
            }
            .for_task(task_id),
        ),
        _ => None,
    };
    EffectCompletion::Completed {
        dispatch_id: effect.dispatch_id(),
        acknowledgement,
    }
}

fn gid(value: u64) -> Gid {
    Gid::new(value).expect("nonzero GID")
}

fn task_id(value: u64) -> TaskId {
    TaskId::new(value).expect("nonzero task id")
}

fn scheduler() -> RequestScheduler {
    RequestScheduler::new(
        SchedulerConfig::new(
            NonZeroUsize::new(8).expect("task cap"),
            NonZeroUsize::new(2).expect("active cap"),
            false,
        )
        .expect("scheduler config"),
    )
}

fn restored_scheduler() -> (RequestScheduler, ariax_core::SchedulerRestorePlan) {
    let now = MonotonicInstant::now();
    let tasks = (1..=9)
        .map(|value| RecoveredSchedulerTask {
            task_id: task_id(value),
            gid: gid(value),
            state: if value == 9 {
                TaskState::RetryWait
            } else {
                TaskState::Waiting
            },
            generation: Generation::INITIAL,
            generation_started: value == 9,
            desired_paused: false,
            conditions: TaskConditions::default(),
            slow_demotion_count: 0,
            slow_slot: None,
            retry_at: (value == 9).then(|| {
                now.checked_add(Duration::from_secs(5))
                    .expect("test retry deadline")
            }),
            host_key_challenge: None,
            error: None,
            stopped_status: None,
        })
        .collect::<Vec<_>>();
    let queues = vec![
        QueueOrder {
            class: QueueClass::Waiting,
            order: (1..=9).map(gid).collect(),
        },
        QueueOrder {
            class: QueueClass::Demoted,
            order: vec![],
        },
        QueueOrder {
            class: QueueClass::Paused,
            order: vec![],
        },
        QueueOrder {
            class: QueueClass::Active,
            order: vec![],
        },
        QueueOrder {
            class: QueueClass::Stopped,
            order: vec![],
        },
    ];
    RequestScheduler::restore(
        SchedulerConfig::new(
            NonZeroUsize::new(9).expect("task cap"),
            NonZeroUsize::new(1).expect("active cap"),
            false,
        )
        .expect("scheduler config"),
        SchedulerRestoreBatch::new(tasks, queues),
    )
    .expect("valid restored scheduler")
}

fn restored_retry_scheduler(
    retry_at: MonotonicInstant,
) -> (RequestScheduler, ariax_core::SchedulerRestorePlan) {
    let task = RecoveredSchedulerTask {
        task_id: task_id(1),
        gid: gid(1),
        state: TaskState::RetryWait,
        generation: Generation::INITIAL,
        generation_started: true,
        desired_paused: false,
        conditions: TaskConditions::default(),
        slow_demotion_count: 0,
        slow_slot: None,
        retry_at: Some(retry_at),
        host_key_challenge: None,
        error: None,
        stopped_status: None,
    };
    RequestScheduler::restore(
        SchedulerConfig::new(
            NonZeroUsize::new(1).expect("task cap"),
            NonZeroUsize::new(1).expect("active cap"),
            false,
        )
        .expect("scheduler config"),
        SchedulerRestoreBatch::new(
            vec![task],
            vec![
                QueueOrder {
                    class: QueueClass::Waiting,
                    order: vec![gid(1)],
                },
                QueueOrder {
                    class: QueueClass::Demoted,
                    order: vec![],
                },
                QueueOrder {
                    class: QueueClass::Paused,
                    order: vec![],
                },
                QueueOrder {
                    class: QueueClass::Active,
                    order: vec![],
                },
                QueueOrder {
                    class: QueueClass::Stopped,
                    order: vec![],
                },
            ],
        ),
    )
    .expect("valid retry scheduler")
}

fn add_command(task_id: TaskId, task_gid: Gid) -> SchedulerCommand {
    SchedulerCommand::AddValidatedTask {
        task_id,
        gid: task_gid,
        desired_paused: false,
        conditions: TaskConditions::default(),
    }
}

fn drive_to_idle(driver: &mut SchedulerDriver<TestSink>) -> SchedulerDriverPoll {
    for _ in 0..128 {
        let progress = driver.poll();
        match progress {
            SchedulerDriverPoll::Completed { .. } => {
                assert!(driver.is_idle());
                return progress;
            }
            SchedulerDriverPoll::Faulted(fault) => panic!("unexpected driver fault: {fault:?}"),
            SchedulerDriverPoll::Idle => panic!("driver became idle without completing a chain"),
            SchedulerDriverPoll::Progressed
            | SchedulerDriverPoll::Backpressured { .. }
            | SchedulerDriverPoll::WaitingForCompletion { .. } => {}
        }
    }
    panic!("driver did not settle within its bounded test budget")
}

fn add_and_publish(driver: &mut SchedulerDriver<TestSink>, task: TaskId, task_gid: Gid) {
    driver
        .execute_command(add_command(task, task_gid))
        .expect("submit add");
    assert_eq!(
        drive_to_idle(driver),
        SchedulerDriverPoll::Completed {
            revision: 1,
            published: true,
        }
    );
}

#[test]
fn add_dispatches_in_order_and_publishes_one_exact_snapshot_root() {
    let task = task_id(1);
    let task_gid = gid(1);
    let mut driver = SchedulerDriver::new(scheduler(), TestSink::automatic());

    driver
        .execute_command(add_command(task, task_gid))
        .expect("submit add");
    assert_eq!(driver.snapshot_reader().load().revision(), 0);
    assert_eq!(
        drive_to_idle(&mut driver),
        SchedulerDriverPoll::Completed {
            revision: 1,
            published: true,
        }
    );

    let root = driver.snapshot_reader().load();
    let task_snapshot = root.task(task_gid).expect("published task");
    assert_eq!(task_snapshot.task_id, task);
    assert_eq!(task_snapshot.snapshot.state, TaskState::Waiting);
    assert_eq!(root.queue(QueueClass::Waiting), [task_gid]);
    for class in [
        QueueClass::Demoted,
        QueueClass::Paused,
        QueueClass::Active,
        QueueClass::Stopped,
    ] {
        assert!(root.queue(class).is_empty(), "{class:?}");
    }
    assert_eq!(driver.sink().offers.len(), 1);
    assert_eq!(
        driver.sink().offers[0].effect().kind(),
        TransitionEffectKind::PersistTask
    );
}

#[test]
fn cloneable_snapshot_reader_observes_atomic_roots_without_writer_lineage() {
    let mut driver = SchedulerDriver::new(scheduler(), TestSink::automatic());
    let reader = driver.snapshot_reader();
    let old = reader.load();

    add_and_publish(&mut driver, task_id(1), gid(1));

    assert!(old.is_empty());
    assert_eq!(old.revision(), 0);
    let current = reader.load();
    assert_eq!(current.revision(), 1);
    assert_eq!(current.queue(QueueClass::Waiting), [gid(1)]);
    assert_eq!(
        current.task(gid(1)).expect("published task").task_id,
        task_id(1)
    );
}

#[test]
fn typed_sink_preparation_is_rejected_outside_the_idle_input_boundary() {
    let mut driver = SchedulerDriver::new(scheduler(), TestSink::automatic());
    assert_eq!(
        driver.prepare_sink(TestSinkPreparation::SetAutoComplete(false)),
        Ok(())
    );
    assert!(!driver.sink().auto_complete.get());
    assert_eq!(
        driver.prepare_sink(TestSinkPreparation::Reject),
        Err(SchedulerDriverPrepareError::Rejected(
            TestPreparationRejected
        ))
    );
    driver
        .prepare_sink(TestSinkPreparation::SetAutoComplete(true))
        .expect("restore automatic completion");
    driver
        .execute_command(add_command(task_id(1), gid(1)))
        .expect("submit add");
    assert_eq!(
        driver.prepare_sink(TestSinkPreparation::SetAutoComplete(false)),
        Err(SchedulerDriverPrepareError::Busy)
    );
    drive_to_idle(&mut driver);

    let mut sink = TestSink::automatic();
    sink.behavior.push_back(DispatchBehavior::Failed);
    let mut faulted = SchedulerDriver::new(scheduler(), sink);
    faulted
        .execute_command(add_command(task_id(1), gid(1)))
        .expect("submit add");
    let SchedulerDriverPoll::Faulted(fault) = faulted.poll() else {
        panic!("expected sink fault");
    };
    assert_eq!(
        faulted.prepare_sink(TestSinkPreparation::SetAutoComplete(true)),
        Err(SchedulerDriverPrepareError::Faulted(fault))
    );
}

#[test]
fn full_sink_retries_the_same_dispatch_without_advancing() {
    let mut sink = TestSink::automatic();
    sink.behavior = VecDeque::from([DispatchBehavior::Full, DispatchBehavior::Accept]);
    let mut driver = SchedulerDriver::new(scheduler(), sink);
    driver
        .execute_command(add_command(task_id(1), gid(1)))
        .expect("submit add");

    let first = driver.poll();
    let SchedulerDriverPoll::Backpressured {
        dispatch_id: first_id,
    } = first
    else {
        panic!("expected backpressure, got {first:?}");
    };
    let second = driver.poll();
    assert_eq!(
        second,
        SchedulerDriverPoll::WaitingForCompletion {
            dispatch_id: first_id,
        }
    );
    assert_eq!(driver.sink().offers.len(), 2);
    assert_eq!(driver.sink().offers[0], driver.sink().offers[1]);
    assert_eq!(driver.snapshot_reader().load().revision(), 0);
    drive_to_idle(&mut driver);
}

#[test]
fn cross_driver_completion_ids_do_not_alias() {
    let (first_sink, first_control) = TestSink::manual_with_control();
    let mut first = SchedulerDriver::new(scheduler(), first_sink);
    first
        .execute_command(add_command(task_id(1), gid(1)))
        .expect("first add");
    let SchedulerDriverPoll::WaitingForCompletion {
        dispatch_id: first_id,
    } = first.poll()
    else {
        panic!("first effect was not accepted");
    };

    let (second_sink, second_control) = TestSink::manual_with_control();
    let mut second = SchedulerDriver::new(scheduler(), second_sink);
    second
        .execute_command(add_command(task_id(2), gid(2)))
        .expect("second add");
    let SchedulerDriverPoll::WaitingForCompletion {
        dispatch_id: second_id,
    } = second.poll()
    else {
        panic!("second effect was not accepted");
    };

    assert_ne!(first_id, second_id);
    second_control.push_completion(EffectCompletion::Completed {
        dispatch_id: first_id,
        acknowledgement: None,
    });
    assert_eq!(
        second.poll(),
        SchedulerDriverPoll::Faulted(SchedulerDriverFault::CompletionOutOfOrder {
            expected: second_id,
            actual: first_id,
        })
    );
    assert_eq!(second.snapshot_reader().load().revision(), 0);

    first_control.push_completion(EffectCompletion::Completed {
        dispatch_id: first_id,
        acknowledgement: None,
    });
    assert_eq!(first.poll(), SchedulerDriverPoll::Progressed);
    assert!(matches!(
        drive_to_idle(&mut first),
        SchedulerDriverPoll::Completed {
            revision: 1,
            published: true,
        }
    ));
}

#[test]
fn closed_and_failed_sinks_fault_stickily_without_publication() {
    for (behavior, expected_kind) in [
        (DispatchBehavior::Closed, "closed"),
        (DispatchBehavior::Failed, "failed"),
    ] {
        let mut sink = TestSink::automatic();
        sink.behavior.push_back(behavior);
        let mut driver = SchedulerDriver::new(scheduler(), sink);
        driver
            .execute_command(add_command(task_id(1), gid(1)))
            .expect("submit add");
        let result = driver.poll();
        let fault = match result {
            SchedulerDriverPoll::Faulted(fault) => fault,
            other => panic!("expected {expected_kind} fault, got {other:?}"),
        };
        assert!(matches!(
            (behavior, fault),
            (
                DispatchBehavior::Closed,
                SchedulerDriverFault::SinkClosed { .. }
            ) | (
                DispatchBehavior::Failed,
                SchedulerDriverFault::SinkFailed { .. }
            )
        ));
        assert_eq!(driver.poll(), SchedulerDriverPoll::Faulted(fault));
        assert_eq!(driver.snapshot_reader().load().revision(), 0);
        assert_eq!(
            driver.execute_command(add_command(task_id(2), gid(2))),
            Err(SchedulerDriverInputError::Faulted(fault))
        );
    }
}

#[test]
fn forged_out_of_order_and_unrepresentable_completions_are_fatal() {
    let (sink, control) = TestSink::manual_with_control();
    let mut driver = SchedulerDriver::new(scheduler(), sink);
    driver
        .execute_command(add_command(task_id(1), gid(1)))
        .expect("submit add");
    let waiting = driver.poll();
    let SchedulerDriverPoll::WaitingForCompletion { dispatch_id } = waiting else {
        panic!("expected accepted dispatch, got {waiting:?}");
    };
    let forged = EffectDispatchId::new(dispatch_id.get() + 1).expect("next dispatch id");
    control.push_completion(EffectCompletion::Completed {
        dispatch_id: forged,
        acknowledgement: None,
    });
    assert_eq!(
        driver.poll(),
        SchedulerDriverPoll::Faulted(SchedulerDriverFault::CompletionOutOfOrder {
            expected: dispatch_id,
            actual: forged,
        })
    );
    assert_eq!(driver.snapshot_reader().load().revision(), 0);

    let (sink, control) = TestSink::manual_with_control();
    let mut second = SchedulerDriver::new(scheduler(), sink);
    second
        .execute_command(add_command(task_id(1), gid(1)))
        .expect("submit add");
    let SchedulerDriverPoll::WaitingForCompletion { dispatch_id } = second.poll() else {
        panic!("expected accepted dispatch");
    };
    control.push_completion(EffectCompletion::UnrepresentableFailure { dispatch_id });
    assert!(matches!(
        second.poll(),
        SchedulerDriverPoll::Faulted(SchedulerDriverFault::UnrepresentableEffectFailure {
            dispatch_id: actual,
            ..
        }) if actual == dispatch_id
    ));
    assert_eq!(second.snapshot_reader().load().revision(), 0);
}

#[test]
fn acknowledgement_must_match_task_generation_and_operation_identity() {
    let task = task_id(1);
    let task_gid = gid(1);
    let (sink, control) = TestSink::automatic_with_control();
    let mut driver = SchedulerDriver::new(scheduler(), sink);
    add_and_publish(&mut driver, task, task_gid);
    control.set_auto_complete(false);
    driver.admit_next().expect("admit waiting task");
    let waiting = driver.poll();
    let SchedulerDriverPoll::WaitingForCompletion { dispatch_id } = waiting else {
        panic!("expected generation persistence dispatch, got {waiting:?}");
    };
    let offered = driver.sink().offers.last().expect("offered effect").clone();
    assert_eq!(
        offered.effect().kind(),
        TransitionEffectKind::PersistGenerationStarted
    );
    control.push_completion(EffectCompletion::Completed {
        dispatch_id,
        acknowledgement: Some(
            TaskEvent::GenerationPersisted {
                gid: task_gid,
                generation: offered.task_generation(),
            }
            .for_task(task_id(99)),
        ),
    });
    assert!(matches!(
        driver.poll(),
        SchedulerDriverPoll::Faulted(SchedulerDriverFault::MismatchedAcknowledgement {
            dispatch_id: actual,
            event_task_id,
            ..
        }) if actual == dispatch_id && event_task_id == task_id(99)
    ));
    let root = driver.snapshot_reader().load();
    assert_eq!(root.revision(), 1);
    assert_eq!(
        root.task(task_gid).expect("old snapshot").snapshot.state,
        TaskState::Waiting
    );
}

#[test]
fn terminal_and_deletion_visibility_wait_for_acknowledgement_chains() {
    let task = task_id(1);
    let task_gid = gid(1);
    let mut driver = SchedulerDriver::new(scheduler(), TestSink::automatic());
    add_and_publish(&mut driver, task, task_gid);

    driver.admit_next().expect("admit task");
    drive_to_idle(&mut driver);
    let generation = driver
        .scheduler()
        .task(task_gid)
        .expect("allocating task")
        .generation;
    driver
        .handle_event(
            &TaskEvent::AllocationSucceeded {
                gid: task_gid,
                generation,
            }
            .for_task(task),
        )
        .expect("activate task");
    drive_to_idle(&mut driver);
    driver
        .handle_event(
            &TaskEvent::DataComplete {
                gid: task_gid,
                generation,
                seed: false,
            }
            .for_task(task),
        )
        .expect("enter verification");
    drive_to_idle(&mut driver);

    let before_terminal = driver.snapshot_reader().load();
    assert_eq!(
        before_terminal
            .task(task_gid)
            .expect("verification snapshot")
            .snapshot
            .state,
        TaskState::Verifying
    );
    assert_eq!(before_terminal.queue(QueueClass::Active), [task_gid]);
    driver
        .handle_event(
            &TaskEvent::VerificationSucceeded {
                gid: task_gid,
                generation,
            }
            .for_task(task),
        )
        .expect("finish verification");

    assert!(matches!(
        driver.poll(),
        SchedulerDriverPoll::WaitingForCompletion { .. }
    ));
    assert_eq!(driver.poll(), SchedulerDriverPoll::Progressed);
    assert!(matches!(
        driver.poll(),
        SchedulerDriverPoll::WaitingForCompletion { .. }
    ));
    assert_eq!(
        driver
            .sink()
            .offers
            .iter()
            .rev()
            .take(2)
            .map(|effect| effect.effect().kind())
            .collect::<Vec<_>>(),
        vec![
            TransitionEffectKind::PersistTerminal,
            TransitionEffectKind::ReleaseSlot,
        ]
    );
    let still_old = driver.snapshot_reader().load();
    assert_eq!(still_old.revision(), before_terminal.revision());
    assert_eq!(
        still_old
            .task(task_gid)
            .expect("old snapshot retained")
            .snapshot
            .state,
        TaskState::Verifying
    );

    assert_eq!(driver.poll(), SchedulerDriverPoll::Progressed);
    drive_to_idle(&mut driver);
    let terminal = driver.snapshot_reader().load();
    assert_eq!(terminal.revision(), before_terminal.revision() + 1);
    let terminal_task = terminal.task(task_gid).expect("retained result");
    assert_eq!(terminal_task.task_id, task);
    assert_eq!(terminal_task.snapshot.state, TaskState::StoppedResult);
    assert_eq!(
        terminal_task.snapshot.stopped_status,
        Some(Aria2Status::Complete)
    );
    assert!(terminal.queue(QueueClass::Active).is_empty());
    assert_eq!(terminal.queue(QueueClass::Stopped), [task_gid]);

    driver
        .execute_command(SchedulerCommand::RemoveStoppedResult { gid: task_gid })
        .expect("request deletion");
    assert!(matches!(
        driver.poll(),
        SchedulerDriverPoll::WaitingForCompletion { .. }
    ));
    let pending_deletion = driver.snapshot_reader().load();
    assert_eq!(pending_deletion.revision(), terminal.revision());
    assert!(pending_deletion.task(task_gid).is_some());
    assert_eq!(pending_deletion.queue(QueueClass::Stopped), [task_gid]);

    assert_eq!(driver.poll(), SchedulerDriverPoll::Progressed);
    drive_to_idle(&mut driver);
    let deleted = driver.snapshot_reader().load();
    assert_eq!(deleted.revision(), terminal.revision() + 1);
    assert!(deleted.is_empty());
    for class in [
        QueueClass::Waiting,
        QueueClass::Demoted,
        QueueClass::Paused,
        QueueClass::Active,
        QueueClass::Stopped,
    ] {
        assert!(deleted.queue(class).is_empty(), "{class:?}");
    }
}

#[test]
fn restore_batches_publish_one_complete_root_after_all_timer_dispatch() {
    let (scheduler, plan) = restored_scheduler();
    let mut driver = SchedulerDriver::new(scheduler, TestSink::automatic());
    driver.begin_restore(plan).expect("begin restore");

    for _ in 0..128 {
        let progress = driver.poll();
        if let SchedulerDriverPoll::Completed {
            revision,
            published,
        } = progress
        {
            assert_eq!(revision, 1);
            assert!(published);
            break;
        }
        assert!(!matches!(progress, SchedulerDriverPoll::Faulted(_)));
        assert_eq!(driver.snapshot_reader().load().revision(), 0);
    }
    assert!(driver.is_idle());

    let root = driver.snapshot_reader().load();
    assert_eq!(root.revision(), 1);
    assert_eq!(root.len(), 9);
    assert_eq!(
        root.queue(QueueClass::Waiting),
        (1..=9).map(gid).collect::<Vec<_>>()
    );
    assert_eq!(
        driver
            .sink()
            .offers
            .iter()
            .map(|effect| effect.effect().kind())
            .collect::<Vec<_>>(),
        vec![TransitionEffectKind::ScheduleRetry]
    );
}

#[test]
fn restore_sink_fault_leaves_revision_zero_unpublished() {
    let (scheduler, plan) = restored_scheduler();
    let mut sink = TestSink::automatic();
    sink.behavior = VecDeque::from([DispatchBehavior::Closed]);
    let mut driver = SchedulerDriver::new(scheduler, sink);
    driver.begin_restore(plan).expect("begin restore");

    let fault = loop {
        match driver.poll() {
            SchedulerDriverPoll::Faulted(fault) => break fault,
            SchedulerDriverPoll::Progressed => {}
            other => panic!("unexpected restore progress before fault: {other:?}"),
        }
    };
    assert!(matches!(fault, SchedulerDriverFault::SinkClosed { .. }));
    let root = driver.snapshot_reader().load();
    assert_eq!(root.revision(), 0);
    assert!(root.is_empty());
}

#[test]
fn restore_rejects_a_plan_bound_to_different_exact_scheduler_state() {
    let now = MonotonicInstant::now();
    let first_deadline = now
        .checked_add(Duration::from_secs(5))
        .expect("first retry deadline");
    let second_deadline = now
        .checked_add(Duration::from_secs(6))
        .expect("second retry deadline");
    let (_, first_plan) = restored_retry_scheduler(first_deadline);
    let (second_scheduler, _) = restored_retry_scheduler(second_deadline);
    let mut driver = SchedulerDriver::new(second_scheduler, TestSink::automatic());

    assert_eq!(
        driver.begin_restore(first_plan),
        Err(SchedulerDriverInputError::RestorePlanMismatch)
    );
    assert!(driver.is_idle());
    assert!(driver.sink().offers.is_empty());
    let root = driver.snapshot_reader().load();
    assert_eq!(root.revision(), 0);
    assert!(root.is_empty());
}

#[test]
fn restore_rejects_a_plan_whose_batch_cursor_was_already_consumed() {
    let (scheduler, mut plan) = restored_scheduler();
    assert!(!plan.next_batch().is_empty());
    let mut driver = SchedulerDriver::new(scheduler, TestSink::automatic());

    assert_eq!(
        driver.begin_restore(plan),
        Err(SchedulerDriverInputError::RestorePlanMismatch)
    );
    assert!(driver.is_idle());
    assert!(driver.sink().offers.is_empty());
    assert_eq!(driver.snapshot_reader().load().revision(), 0);
}
