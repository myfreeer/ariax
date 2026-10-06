//! Preparation must leave the reactor responsive and drain journal ownership.
use super::*;

struct ReleaseGate(Arc<storage_preparation::PreparationGate>);

#[tokio::test]
async fn preparation_replay_budget_rejection_preserves_journal_and_allows_retry() {
    let root = TestDirectory::new("preparation-budget-output");
    let journal = TestDirectory::new("preparation-budget-journal");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let spec = Arc::new(task(&root, [listener.local_addr().unwrap()], MIB));
    let worker = worker(
        &journal,
        SharedHttpTransferStats::new(NonZeroUsize::new(1).unwrap()),
        1,
    );
    let prepared = worker
        .prepare_storage_async(spec.clone(), Generation::INITIAL, HttpCancellation::new())
        .await
        .unwrap();
    worker
        .handoff_journal(spec.gid(), prepared.into_appender())
        .await
        .unwrap();
    let path = http_journal_directory(&journal.0, spec.gid())
        .join(ariax_storage::journal_segment_file_name(0));
    let before = fs::read(&path).unwrap();
    let held = worker
        .config
        .journal_replay
        .try_acquire(worker.config.journal_replay.limit())
        .unwrap();
    assert!(
        worker
            .prepare_storage_async(spec.clone(), Generation::INITIAL, HttpCancellation::new())
            .await
            .is_err()
    );
    assert_eq!(worker.preparation_slot.available_permits(), 1);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    drop(held);
    let prepared = worker
        .prepare_storage_async(spec.clone(), Generation::INITIAL, HttpCancellation::new())
        .await
        .unwrap();
    assert_eq!(worker.config.journal_replay.used(), 0);
    worker
        .handoff_journal(spec.gid(), prepared.into_appender())
        .await
        .unwrap();
}

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn preparation_cancellation_waits_for_handoff_without_blocking_the_reactor() {
    let root = TestDirectory::new("preparation-drain-output");
    let journal = TestDirectory::new("preparation-drain-journal");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let spec = Arc::new(task(&root, [listener.local_addr().unwrap()], MIB));
    let worker = worker(
        &journal,
        SharedHttpTransferStats::new(NonZeroUsize::new(1).unwrap()),
        1,
    );
    let gate = Arc::new(storage_preparation::PreparationGate::default());
    let _release = ReleaseGate(Arc::clone(&gate));
    *worker.preparation_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let cancellation = HttpCancellation::new();
    let running = {
        let worker = worker.clone();
        let spec = Arc::clone(&spec);
        let cancellation = cancellation.clone();
        tokio::spawn(async move {
            worker
                .run_task(spec, Generation::INITIAL, cancellation)
                .await
        })
    };
    // On the single-thread Tokio runtime this can progress only if the
    // filesystem job runs elsewhere. The gate has a separate bounded watchdog.
    tokio::time::timeout(Duration::from_secs(2), async {
        while !gate.entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reactor remains available during preparation");
    cancellation.cancel();
    tokio::task::yield_now().await;
    assert!(!running.is_finished(), "accepted preparation must drain");
    assert_eq!(worker.preparation_slot.available_permits(), 0);
    gate.release();
    let result = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .expect("drained")
        .expect("worker join");
    assert!(matches!(result, Err(HttpMultiRangeError::Cancelled)));
    assert_eq!(worker.preparation_slot.available_permits(), 1);
    assert!(!root.0.join("output.bin").exists());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    let reopened = worker
        .open_task_journal(&spec, Generation::INITIAL)
        .expect("flushed journal");
    assert!(
        reopened.state.is_some(),
        "initial admission survives cancellation"
    );
    worker
        .handoff_journal(spec.gid(), reopened.appender)
        .await
        .expect("close journal");
}

#[tokio::test]
async fn preparation_cancelled_while_waiting_for_shared_slot_has_no_journal_effects() {
    let root = TestDirectory::new("preparation-queued-output");
    let journal = TestDirectory::new("preparation-queued-journal");
    let spec = Arc::new(task(&root, ["127.0.0.1:9".parse().unwrap()], MIB));
    let worker = worker(
        &journal,
        SharedHttpTransferStats::new(NonZeroUsize::new(1).unwrap()),
        1,
    );
    let held = worker.preparation_slot.acquire().await.unwrap();
    let cancellation = HttpCancellation::new();
    let running = {
        let worker = worker.clone();
        let spec = Arc::clone(&spec);
        let cancellation = cancellation.clone();
        tokio::spawn(async move {
            worker
                .run_task(spec, Generation::INITIAL, cancellation)
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!running.is_finished());
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .unwrap()
            .unwrap(),
        Err(HttpMultiRangeError::Cancelled)
    ));
    assert!(!http_journal_directory(&journal.0, spec.gid()).exists());
    assert_eq!(worker.preparation_slot.available_permits(), 0);
    drop(held);
    assert_eq!(worker.preparation_slot.available_permits(), 1);
}

#[tokio::test]
async fn preparation_success_and_path_rejection_release_the_slot_and_preserve_journal() {
    let root = TestDirectory::new("preparation-result-output");
    let journal = TestDirectory::new("preparation-result-journal");
    let spec = Arc::new(task(&root, ["127.0.0.1:9".parse().unwrap()], MIB));
    let worker = worker(
        &journal,
        SharedHttpTransferStats::new(NonZeroUsize::new(1).unwrap()),
        1,
    );
    let prepared = worker
        .prepare_storage_async(
            Arc::clone(&spec),
            Generation::INITIAL,
            HttpCancellation::new(),
        )
        .await
        .expect("fresh preparation");
    assert!(matches!(prepared, PreparedHttpStorage::Fresh(_)));
    assert_eq!(worker.preparation_slot.available_permits(), 1);
    worker
        .handoff_journal(spec.gid(), prepared.into_appender())
        .await
        .expect("close journal");
    fs::remove_dir(&root.0).expect("remove output root");
    let result = worker
        .prepare_storage_async(
            Arc::clone(&spec),
            Generation::INITIAL,
            HttpCancellation::new(),
        )
        .await;
    assert!(matches!(result, Err(HttpMultiRangeError::Setup(_))));
    assert_eq!(worker.preparation_slot.available_permits(), 1);
    let reopened = worker
        .open_task_journal(&spec, Generation::INITIAL)
        .expect("error handoff retained journal");
    assert!(reopened.state.is_some());
    worker
        .handoff_journal(spec.gid(), reopened.appender)
        .await
        .expect("close journal");
}
