#![forbid(unsafe_code)]

//! Bounded resource, payload-buffer, and queue ownership primitives.

mod budget;
mod buffer;
mod queue;

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
