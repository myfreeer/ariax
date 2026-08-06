use ariax_runtime::{
    BlockingBackendEpoch, BlockingDiskExecutor, BlockingDiskIoError, BlockingDiskLane,
    BlockingDiskLaneConfig, BlockingDiskLaneStartError, BlockingFileHandle,
    MAX_BLOCKING_DISK_COMPLETION_CAPACITY, MAX_BLOCKING_DISK_QUEUE_CAPACITY,
    MAX_BLOCKING_DISK_WORKERS,
};
use std::time::Duration;

struct UnusedExecutor;

impl BlockingDiskExecutor for UnusedExecutor {
    fn write_at(
        &self,
        _handle: BlockingFileHandle,
        _offset: u64,
        _bytes: &[u8],
    ) -> Result<usize, BlockingDiskIoError> {
        unreachable!("constructor tests submit no work")
    }
}

fn epoch() -> BlockingBackendEpoch {
    BlockingBackendEpoch::new(1).expect("nonzero epoch")
}

fn config(
    worker_count: usize,
    queue_capacity: usize,
    completion_capacity: usize,
    max_accepted_bytes: usize,
) -> BlockingDiskLaneConfig {
    BlockingDiskLaneConfig {
        worker_count,
        queue_capacity,
        completion_capacity,
        max_accepted_bytes,
    }
}

#[test]
fn exported_lane_rejects_oversized_capacities_before_allocation() {
    let cases = [
        (
            config(MAX_BLOCKING_DISK_WORKERS + 1, 1, 1, 1),
            BlockingDiskLaneStartError::WorkerCountTooLarge {
                requested: MAX_BLOCKING_DISK_WORKERS + 1,
                maximum: MAX_BLOCKING_DISK_WORKERS,
            },
        ),
        (
            config(1, MAX_BLOCKING_DISK_QUEUE_CAPACITY + 1, 1, 1),
            BlockingDiskLaneStartError::QueueCapacityTooLarge {
                requested: MAX_BLOCKING_DISK_QUEUE_CAPACITY + 1,
                maximum: MAX_BLOCKING_DISK_QUEUE_CAPACITY,
            },
        ),
        (
            config(1, 1, MAX_BLOCKING_DISK_COMPLETION_CAPACITY + 1, 1),
            BlockingDiskLaneStartError::CompletionCapacityTooLarge {
                requested: MAX_BLOCKING_DISK_COMPLETION_CAPACITY + 1,
                maximum: MAX_BLOCKING_DISK_COMPLETION_CAPACITY,
            },
        ),
    ];

    for (config, expected) in cases {
        let error = BlockingDiskLane::new(config, epoch(), UnusedExecutor)
            .expect_err("oversized capacity must be rejected");
        assert_eq!(error, expected);
    }
}

#[test]
fn exported_lane_rejects_zero_capacities() {
    let cases = [
        (config(0, 1, 1, 1), BlockingDiskLaneStartError::ZeroWorkers),
        (
            config(1, 0, 1, 1),
            BlockingDiskLaneStartError::ZeroQueueCapacity,
        ),
        (
            config(1, 1, 0, 1),
            BlockingDiskLaneStartError::ZeroCompletionCapacity,
        ),
        (
            config(1, 1, 1, 0),
            BlockingDiskLaneStartError::ZeroByteCapacity,
        ),
    ];

    for (config, expected) in cases {
        let error = BlockingDiskLane::new(config, epoch(), UnusedExecutor)
            .expect_err("zero capacity must be rejected");
        assert_eq!(error, expected);
    }
}

#[test]
fn exported_lane_starts_and_closes_without_work() {
    let lane =
        BlockingDiskLane::new(config(1, 1, 1, 1), epoch(), UnusedExecutor).expect("minimal lane");
    let shutdown = lane.shutdown(Duration::from_secs(1));
    assert_eq!(shutdown.detached_workers(), 0);
    assert_eq!(shutdown.in_flight_at_timeout(), 0);
    assert!(shutdown.completion_closed());
}
