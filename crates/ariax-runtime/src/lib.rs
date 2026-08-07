#![forbid(unsafe_code)]

//! Bounded resource, payload-buffer, and queue ownership primitives.

mod blocking_disk;
mod budget;
mod buffer;
mod queue;
mod scheduler_driver;
mod shutdown;
mod stats;
mod status_snapshot;

pub use blocking_disk::{
    BLOCKING_DISK_DROP_TIMEOUT, BlockingBackendEpoch, BlockingDiskCancelHandle,
    BlockingDiskCancelResult, BlockingDiskCancellationRegistration, BlockingDiskCompletion,
    BlockingDiskError, BlockingDiskExecutor, BlockingDiskIoError, BlockingDiskIoErrorKind,
    BlockingDiskLane, BlockingDiskLaneConfig, BlockingDiskLaneMetrics, BlockingDiskLaneResource,
    BlockingDiskLaneStartError, BlockingDiskOperation, BlockingDiskOperationId,
    BlockingDiskOutcome, BlockingDiskShutdown, BlockingDiskSubmission, BlockingDiskSubmitError,
    BlockingDiskSubmitErrorKind, BlockingFileHandle, BlockingFileRegistry,
    BlockingFileRegistryError, MAX_BLOCKING_DISK_COMPLETION_CAPACITY,
    MAX_BLOCKING_DISK_QUEUE_CAPACITY, MAX_BLOCKING_DISK_SHUTDOWN_TIMEOUT,
    MAX_BLOCKING_DISK_WORKERS,
};
pub use budget::{BudgetError, ByteBudget, BytePermit};
pub use buffer::{
    ALL_BUFFER_STATES, ALL_OWNER_TAGS, BufferLease, BufferPool, BufferPoolConfig,
    BufferPoolMetrics, BufferState, BufferTransitionError, OwnerTag, PoolError, ReleaseError,
    SizeClass, SizeClassConfig,
};
pub use queue::{
    BoundedQueue, CloseReason, CompletionDrain, CompletionDrainMetrics, CompletionPermit,
    QueueMetrics, QueuePermit, QueueReserveError, QueueSendError,
};
pub use scheduler_driver::{
    DispatchedEffect, EffectCompletion, EffectDispatchId, EffectSinkError, SchedulerDriver,
    SchedulerDriverFault, SchedulerDriverInputError, SchedulerDriverPoll,
    SchedulerDriverPrepareError, SchedulerEffectSink, SchedulerEffectSinkPrepare,
};
pub use shutdown::{
    ShutdownCoordinator, ShutdownCoordinatorError, ShutdownFailure, ShutdownFailureKind,
    ShutdownProfile, ShutdownProgress, ShutdownReport, ShutdownStep, ShutdownStepResult,
    ShutdownTicket,
};
pub use stats::{
    ConnectionCondition, ConnectionConditionReason, MAX_STATS_ACTIVE_ENTRIES,
    MAX_STATS_SAMPLE_INTERVAL, MIN_STATS_SAMPLE_INTERVAL, StatsCounters, StatsDiagnostic,
    StatsProfile, StatsSample, StatsSampler, StatsSamplerConfig, StatsSamplerError,
};
pub use status_snapshot::{
    AppliedTaskSnapshot, StatusSnapshotError, StatusSnapshotReader, StatusSnapshotRoot,
};
