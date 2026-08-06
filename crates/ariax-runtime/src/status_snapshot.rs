use ariax_core::{
    ALL_QUEUE_CLASSES, Gid, QueueClass, RequestScheduler, TaskId, TaskSnapshot, TaskState,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

const QUEUE_COUNT: usize = 5;

/// One publicly visible snapshot bound to the immutable task instance that
/// produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedTaskSnapshot {
    pub task_id: TaskId,
    pub snapshot: TaskSnapshot,
}

/// One immutable, internally consistent status view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusSnapshotRoot {
    revision: u64,
    tasks: BTreeMap<Gid, AppliedTaskSnapshot>,
    queues: [Arc<[Gid]>; QUEUE_COUNT],
}

impl StatusSnapshotRoot {
    fn empty() -> Self {
        Self {
            revision: 0,
            tasks: BTreeMap::new(),
            queues: std::array::from_fn(|_| Arc::from([])),
        }
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    #[must_use]
    pub fn task(&self, gid: Gid) -> Option<&AppliedTaskSnapshot> {
        self.tasks.get(&gid)
    }

    #[must_use]
    pub fn tasks(&self) -> &BTreeMap<Gid, AppliedTaskSnapshot> {
        &self.tasks
    }

    #[must_use]
    pub fn queue(&self, class: QueueClass) -> &[Gid] {
        &self.queues[queue_index(class)]
    }
}

/// Driver-owned writer lineage for the latest applied status root.
#[derive(Debug)]
pub(crate) struct StatusSnapshotStore {
    root: Arc<RwLock<Arc<StatusSnapshotRoot>>>,
}

/// Cloneable read-only handle for the latest applied status root.
#[derive(Clone, Debug)]
pub struct StatusSnapshotReader {
    root: Arc<RwLock<Arc<StatusSnapshotRoot>>>,
}

impl StatusSnapshotReader {
    /// Clones the current immutable root while holding the publication lock only
    /// for the pointer load.
    #[must_use]
    pub fn load(&self) -> Arc<StatusSnapshotRoot> {
        Arc::clone(&read_lock(&self.root))
    }
}

impl StatusSnapshotStore {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            root: Arc::new(RwLock::new(Arc::new(StatusSnapshotRoot::empty()))),
        }
    }

    #[must_use]
    pub(crate) fn reader(&self) -> StatusSnapshotReader {
        StatusSnapshotReader {
            root: Arc::clone(&self.root),
        }
    }

    #[must_use]
    fn load(&self) -> Arc<StatusSnapshotRoot> {
        self.reader().load()
    }

    pub(crate) fn draft(&self) -> StatusSnapshotDraft {
        StatusSnapshotDraft::from_root(&self.load())
    }

    pub(crate) fn recovered_draft(
        &self,
        scheduler: &RequestScheduler,
    ) -> Result<StatusSnapshotDraft, StatusSnapshotError> {
        let root = self.load();
        if root.revision != 0 || !root.is_empty() {
            return Err(StatusSnapshotError::BootstrapRequiresEmptyRoot);
        }
        let mut draft = StatusSnapshotDraft::from_root(&root);
        for class in ALL_QUEUE_CLASSES.iter().copied() {
            draft.replace_queue(class, scheduler.queue_snapshot(class))?;
        }
        Ok(draft)
    }

    pub(crate) fn publish(
        &self,
        draft: StatusSnapshotDraft,
    ) -> Result<StatusPublication, StatusSnapshotError> {
        draft.validate()?;
        let mut current = write_lock(&self.root);
        if draft.base_revision != current.revision {
            return Err(StatusSnapshotError::StaleDraft {
                expected: draft.base_revision,
                actual: current.revision,
            });
        }
        if !draft.dirty {
            return Ok(StatusPublication {
                revision: current.revision,
                changed: false,
            });
        }
        let revision = current
            .revision
            .checked_add(1)
            .ok_or(StatusSnapshotError::RevisionExhausted)?;
        *current = Arc::new(StatusSnapshotRoot {
            revision,
            tasks: draft.tasks,
            queues: draft.queues,
        });
        Ok(StatusPublication {
            revision,
            changed: true,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StatusPublication {
    pub revision: u64,
    pub changed: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct StatusSnapshotDraft {
    base_revision: u64,
    tasks: BTreeMap<Gid, AppliedTaskSnapshot>,
    queues: [Arc<[Gid]>; QUEUE_COUNT],
    dirty: bool,
}

impl StatusSnapshotDraft {
    fn from_root(root: &StatusSnapshotRoot) -> Self {
        Self {
            base_revision: root.revision,
            tasks: root.tasks.clone(),
            queues: root.queues.clone(),
            dirty: false,
        }
    }

    pub fn insert_task(
        &mut self,
        task_id: TaskId,
        snapshot: TaskSnapshot,
    ) -> Result<(), StatusSnapshotError> {
        snapshot
            .validate()
            .map_err(StatusSnapshotError::InvalidTaskSnapshot)?;
        let gid = snapshot.gid;
        let applied = AppliedTaskSnapshot { task_id, snapshot };
        if self.tasks.get(&gid) != Some(&applied) {
            self.tasks.insert(gid, applied);
            self.dirty = true;
        }
        Ok(())
    }

    pub fn remove_task(&mut self, task_id: TaskId, gid: Gid) {
        if self
            .tasks
            .get(&gid)
            .is_some_and(|snapshot| snapshot.task_id == task_id)
        {
            self.tasks.remove(&gid);
            self.dirty = true;
        }
    }

    pub fn insert_queue_member(
        &mut self,
        class: QueueClass,
        position: usize,
        gid: Gid,
    ) -> Result<(), StatusSnapshotError> {
        if self.queues.iter().any(|order| order.contains(&gid)) {
            return Err(StatusSnapshotError::DuplicateQueueMembership { gid });
        }
        let mut order = self.queue_vec(class);
        if position > order.len() {
            return Err(StatusSnapshotError::InvalidQueuePosition {
                class,
                position,
                queue_len: order.len(),
            });
        }
        order.insert(position, gid);
        self.replace_queue(class, order)
    }

    pub fn replace_queue(
        &mut self,
        class: QueueClass,
        order: Vec<Gid>,
    ) -> Result<(), StatusSnapshotError> {
        let unique: BTreeSet<_> = order.iter().copied().collect();
        if unique.len() != order.len() {
            return Err(StatusSnapshotError::DuplicateQueueEntry { class });
        }
        let index = queue_index(class);
        if self.queues[index].as_ref() != order.as_slice() {
            self.queues[index] = Arc::from(order);
            self.dirty = true;
        }
        Ok(())
    }

    fn queue_vec(&self, class: QueueClass) -> Vec<Gid> {
        self.queues[queue_index(class)].to_vec()
    }

    fn validate(&self) -> Result<(), StatusSnapshotError> {
        let mut memberships = BTreeMap::new();
        for class in ALL_QUEUE_CLASSES {
            for gid in self.queues[queue_index(*class)].iter().copied() {
                if memberships.insert(gid, *class).is_some() {
                    return Err(StatusSnapshotError::DuplicateQueueMembership { gid });
                }
                if !self.tasks.contains_key(&gid) {
                    return Err(StatusSnapshotError::QueueTaskMissing { class: *class, gid });
                }
            }
        }
        let mut task_ids = BTreeMap::new();
        for (gid, task) in &self.tasks {
            if task.snapshot.gid != *gid {
                return Err(StatusSnapshotError::SnapshotGidMismatch {
                    key: *gid,
                    snapshot: task.snapshot.gid,
                });
            }
            task.snapshot
                .validate()
                .map_err(StatusSnapshotError::InvalidTaskSnapshot)?;
            if let Some(first_gid) = task_ids.insert(task.task_id, *gid) {
                return Err(StatusSnapshotError::DuplicateTaskId {
                    task_id: task.task_id,
                    first_gid,
                    second_gid: *gid,
                });
            }
            let Some(class) = memberships.get(gid).copied() else {
                return Err(StatusSnapshotError::TaskQueueMissing { gid: *gid });
            };
            if !snapshot_matches_queue(&task.snapshot, class) {
                return Err(StatusSnapshotError::TaskQueueMismatch {
                    gid: *gid,
                    state: task.snapshot.state,
                    class,
                });
            }
        }
        Ok(())
    }
}

/// A staged status root failed an invariant required for public visibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusSnapshotError {
    BootstrapRequiresEmptyRoot,
    InvalidTaskSnapshot(&'static str),
    SnapshotGidMismatch {
        key: Gid,
        snapshot: Gid,
    },
    DuplicateTaskId {
        task_id: TaskId,
        first_gid: Gid,
        second_gid: Gid,
    },
    InvalidQueuePosition {
        class: QueueClass,
        position: usize,
        queue_len: usize,
    },
    DuplicateQueueEntry {
        class: QueueClass,
    },
    DuplicateQueueMembership {
        gid: Gid,
    },
    QueueTaskMissing {
        class: QueueClass,
        gid: Gid,
    },
    TaskQueueMissing {
        gid: Gid,
    },
    TaskQueueMismatch {
        gid: Gid,
        state: TaskState,
        class: QueueClass,
    },
    StaleDraft {
        expected: u64,
        actual: u64,
    },
    RevisionExhausted,
}

const fn snapshot_matches_queue(snapshot: &TaskSnapshot, class: QueueClass) -> bool {
    match snapshot.state {
        TaskState::Accepted | TaskState::Waiting => matches!(class, QueueClass::Waiting),
        TaskState::WaitingSlow => matches!(class, QueueClass::Demoted),
        TaskState::Allocating | TaskState::Active | TaskState::Verifying | TaskState::Seeding => {
            matches!(class, QueueClass::Active)
        }
        TaskState::RetryWait => {
            matches!(
                (snapshot.retry_wait_holds_slot, class),
                (true, QueueClass::Active) | (false, QueueClass::Waiting)
            )
        }
        TaskState::Paused | TaskState::PausedSlow | TaskState::PausedHostKey => {
            matches!(class, QueueClass::Paused)
        }
        TaskState::PausedRestarting => {
            matches!(class, QueueClass::Waiting | QueueClass::Active)
        }
        TaskState::StoppedResult => matches!(class, QueueClass::Stopped),
        TaskState::Complete | TaskState::Error | TaskState::Removed => false,
    }
}

const fn queue_index(class: QueueClass) -> usize {
    match class {
        QueueClass::Waiting => 0,
        QueueClass::Demoted => 1,
        QueueClass::Paused => 2,
        QueueClass::Active => 3,
        QueueClass::Stopped => 4,
    }
}

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{StatusSnapshotDraft, StatusSnapshotError, StatusSnapshotStore};
    use ariax_core::{
        Aria2Status, Generation, Gid, QueueClass, RequestScheduler, SchedulerConfig,
        TaskConditionsSnapshot, TaskId, TaskSnapshot, TaskState,
    };
    use std::num::NonZeroUsize;

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("nonzero GID")
    }

    fn task_id(value: u64) -> TaskId {
        TaskId::new(value).expect("nonzero task id")
    }

    fn snapshot(task_gid: Gid) -> TaskSnapshot {
        TaskSnapshot {
            gid: task_gid,
            state: TaskState::Waiting,
            generation: Generation::INITIAL,
            total_length: None,
            completed_length: 0,
            durable_length: 0,
            current_speed: 0,
            average_speed: 0,
            active_leases: 0,
            retry_wait_leases: 0,
            retry_wait_until: None,
            last_progress_at: None,
            conditions: TaskConditionsSnapshot::default(),
            desired_paused: false,
            retry_wait_holds_slot: false,
            stopped_status: None,
            host_key_challenge: None,
            error: None,
            terminal_persisted: false,
        }
    }

    #[test]
    fn publication_replaces_one_immutable_root_and_revisions_material_changes() {
        let store = StatusSnapshotStore::new();
        let old = store.load();
        let mut draft = store.draft();
        draft
            .insert_queue_member(QueueClass::Waiting, 0, gid(1))
            .expect("queue insertion");
        draft
            .insert_task(task_id(1), snapshot(gid(1)))
            .expect("snapshot insertion");
        let publication = store.publish(draft).expect("publish root");
        assert!(publication.changed);
        assert_eq!(publication.revision, 1);
        assert!(old.is_empty());

        let current = store.load();
        assert_eq!(current.revision(), 1);
        assert_eq!(current.queue(QueueClass::Waiting), [gid(1)]);
        assert_eq!(current.task(gid(1)).expect("task").task_id, task_id(1));

        let unchanged = store.publish(store.draft()).expect("unchanged root");
        assert!(!unchanged.changed);
        assert_eq!(unchanged.revision, 1);
        assert!(Arc::ptr_eq(&current, &store.load()));
    }

    #[test]
    fn exact_task_identity_guards_eviction() {
        let store = StatusSnapshotStore::new();
        let mut draft = store.draft();
        draft
            .insert_queue_member(QueueClass::Waiting, 0, gid(1))
            .expect("queue insertion");
        draft
            .insert_task(task_id(2), snapshot(gid(1)))
            .expect("snapshot insertion");
        store.publish(draft).expect("publish root");

        let mut stale = store.draft();
        stale.remove_task(task_id(1), gid(1));
        let unchanged = store.publish(stale).expect("stale eviction is ignored");
        assert!(!unchanged.changed);
        assert_eq!(
            store.load().task(gid(1)).expect("reused GID").task_id,
            task_id(2)
        );
    }

    #[test]
    fn malformed_membership_is_rejected_without_publication() {
        let store = StatusSnapshotStore::new();
        let mut draft = StatusSnapshotDraft::from_root(&store.load());
        draft
            .replace_queue(QueueClass::Waiting, vec![gid(1)])
            .expect("locally valid order");
        assert_eq!(
            store.publish(draft),
            Err(StatusSnapshotError::QueueTaskMissing {
                class: QueueClass::Waiting,
                gid: gid(1),
            })
        );
        assert_eq!(store.load().revision(), 0);
    }

    #[test]
    fn duplicate_task_ids_are_rejected_without_publication() {
        let store = StatusSnapshotStore::new();
        let mut draft = store.draft();
        draft
            .replace_queue(QueueClass::Waiting, vec![gid(1), gid(2)])
            .expect("locally valid order");
        draft
            .insert_task(task_id(7), snapshot(gid(1)))
            .expect("first snapshot");
        draft
            .insert_task(task_id(7), snapshot(gid(2)))
            .expect("second snapshot");
        assert_eq!(
            store.publish(draft),
            Err(StatusSnapshotError::DuplicateTaskId {
                task_id: task_id(7),
                first_gid: gid(1),
                second_gid: gid(2),
            })
        );
        assert_eq!(store.load().revision(), 0);
    }

    #[test]
    fn state_queue_mismatch_is_rejected_without_publication() {
        let store = StatusSnapshotStore::new();
        let mut draft = store.draft();
        let mut retry = snapshot(gid(1));
        retry.state = TaskState::RetryWait;
        retry.retry_wait_holds_slot = false;
        draft
            .replace_queue(QueueClass::Active, vec![gid(1)])
            .expect("locally valid order");
        draft
            .insert_task(task_id(1), retry)
            .expect("valid retry snapshot");
        assert_eq!(
            store.publish(draft),
            Err(StatusSnapshotError::TaskQueueMismatch {
                gid: gid(1),
                state: TaskState::RetryWait,
                class: QueueClass::Active,
            })
        );
        assert_eq!(store.load().revision(), 0);
    }

    #[test]
    fn state_derived_queue_memberships_publish_as_one_root() {
        let store = StatusSnapshotStore::new();
        let mut draft = store.draft();

        let mut retry = snapshot(gid(1));
        retry.state = TaskState::RetryWait;
        retry.retry_wait_holds_slot = true;
        let mut restarting = snapshot(gid(2));
        restarting.state = TaskState::PausedRestarting;
        let mut demoted = snapshot(gid(3));
        demoted.state = TaskState::WaitingSlow;
        let mut paused = snapshot(gid(4));
        paused.state = TaskState::PausedSlow;
        let mut stopped = snapshot(gid(5));
        stopped.state = TaskState::StoppedResult;
        stopped.stopped_status = Some(Aria2Status::Complete);
        stopped.terminal_persisted = true;

        for (task, task_snapshot) in [
            (task_id(1), retry),
            (task_id(2), restarting),
            (task_id(3), demoted),
            (task_id(4), paused),
            (task_id(5), stopped),
        ] {
            draft
                .insert_task(task, task_snapshot)
                .expect("valid state snapshot");
        }
        for (class, order) in [
            (QueueClass::Waiting, vec![gid(2)]),
            (QueueClass::Demoted, vec![gid(3)]),
            (QueueClass::Paused, vec![gid(4)]),
            (QueueClass::Active, vec![gid(1)]),
            (QueueClass::Stopped, vec![gid(5)]),
        ] {
            draft
                .replace_queue(class, order)
                .expect("locally valid order");
        }

        let publication = store.publish(draft).expect("publish valid root");
        assert!(publication.changed);
        assert_eq!(publication.revision, 1);
        assert_eq!(store.load().len(), 5);
    }

    #[test]
    fn stale_draft_cannot_overwrite_a_newer_root() {
        let store = StatusSnapshotStore::new();
        let stale = store.draft();
        let mut current = store.draft();
        current
            .insert_queue_member(QueueClass::Waiting, 0, gid(1))
            .expect("queue insertion");
        current
            .insert_task(task_id(1), snapshot(gid(1)))
            .expect("snapshot insertion");
        store.publish(current).expect("publish current root");

        assert_eq!(
            store.publish(stale),
            Err(StatusSnapshotError::StaleDraft {
                expected: 0,
                actual: 1,
            })
        );
        assert_eq!(store.load().revision(), 1);
    }

    #[test]
    fn recovered_draft_rejects_a_nonempty_writer_lineage() {
        let store = StatusSnapshotStore::new();
        let mut draft = store.draft();
        draft
            .insert_queue_member(QueueClass::Waiting, 0, gid(1))
            .expect("queue insertion");
        draft
            .insert_task(task_id(1), snapshot(gid(1)))
            .expect("snapshot insertion");
        store.publish(draft).expect("publish root");
        let scheduler = RequestScheduler::new(
            SchedulerConfig::new(
                NonZeroUsize::new(1).expect("task cap"),
                NonZeroUsize::new(1).expect("active cap"),
                false,
            )
            .expect("scheduler config"),
        );

        assert!(matches!(
            store.recovered_draft(&scheduler),
            Err(StatusSnapshotError::BootstrapRequiresEmptyRoot)
        ));
    }

    use std::sync::Arc;
}
