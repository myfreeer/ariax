use ariax_runtime::{BufferLease, BufferPool, BufferState, OwnerTag};
use std::ops::{Deref, DerefMut};

/// Protocol reads copy into this allocation in safe Rust. No OS operation
/// borrows it; cancellation and failed channel delivery can release it.
pub(super) struct NetworkBuffer {
    lease: Option<BufferLease>,
    pool: BufferPool,
}

impl NetworkBuffer {
    pub(super) fn new(lease: BufferLease, pool: BufferPool) -> Self {
        Self {
            lease: Some(lease),
            pool,
        }
    }

    pub(super) fn into_lease(mut self) -> BufferLease {
        self.lease.take().expect("network buffer owns lease")
    }
}

impl std::fmt::Debug for NetworkBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NetworkBuffer").field(&self.lease).finish()
    }
}

impl Deref for NetworkBuffer {
    type Target = BufferLease;

    fn deref(&self) -> &Self::Target {
        self.lease.as_ref().expect("network buffer owns lease")
    }
}

impl DerefMut for NetworkBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.lease.as_mut().expect("network buffer owns lease")
    }
}

impl Drop for NetworkBuffer {
    fn drop(&mut self) {
        let Some(mut lease) = self.lease.take() else {
            return;
        };
        // Unexpected ownership still uses BufferLease's quarantine path.
        if matches!(
            lease.state(),
            BufferState::NetworkFill | BufferState::Filled
        ) && lease
            .transition(BufferState::Releasable, OwnerTag::Network)
            .is_ok()
        {
            let _ = self.pool.release(lease);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ariax_runtime::{BufferPoolConfig, ByteBudget};

    fn reserve(pool: &BufferPool) -> NetworkBuffer {
        let mut lease = pool
            .try_reserve(64 * 1024, OwnerTag::Network, None, None)
            .unwrap();
        lease
            .transition(BufferState::NetworkFill, OwnerTag::Network)
            .unwrap();
        NetworkBuffer::new(lease, pool.clone())
    }

    #[test]
    fn cancelled_reads_and_closed_reply_channels_reuse_one_allocation() {
        let resident = ByteBudget::new(64 * 1024);
        let mut config = BufferPoolConfig::new(64 * 1024, 64 * 1024);
        config.resident_budget = resident.clone();
        let pool = BufferPool::new(config).unwrap();
        for _ in 0..128 {
            drop(reserve(&pool));
            let (send, receive) = tokio::sync::oneshot::channel();
            drop(receive);
            drop(send.send(reserve(&pool)));
            let mut filled = reserve(&pool);
            filled.mark_filled(1, OwnerTag::Storage).unwrap();
            drop(filled);
        }
        assert_eq!(pool.metrics().allocation_count, 1);
        assert_eq!(pool.metrics().quarantine_count, 0);
        assert_eq!(pool.metrics().leased_bytes, 0);
        assert_eq!(resident.used(), 64 * 1024);
        let last = reserve(&pool);
        drop(pool);
        drop(last);
        assert_eq!(resident.used(), 0);
    }

    #[test]
    fn storage_handoff_retains_lease_until_explicit_release() {
        let pool = BufferPool::new(BufferPoolConfig::new(64 * 1024, 64 * 1024)).unwrap();
        let mut buffer = reserve(&pool);
        buffer.mark_filled(1, OwnerTag::Storage).unwrap();
        let mut lease = buffer.into_lease();
        assert_eq!(pool.metrics().leased_bytes, 64 * 1024);
        lease
            .transition(BufferState::Releasable, OwnerTag::Storage)
            .unwrap();
        pool.release(lease).unwrap();
        assert_eq!(pool.metrics().leased_bytes, 0);
        assert_eq!(pool.metrics().quarantine_count, 0);
    }

    #[tokio::test]
    async fn aborted_read_future_releases_its_buffer_before_pool_shutdown() {
        let pool = BufferPool::new(BufferPoolConfig::new(64 * 1024, 64 * 1024)).unwrap();
        let task_pool = pool.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let buffer = reserve(&task_pool);
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(buffer);
        });
        started.await.unwrap();
        assert_eq!(pool.metrics().leased_bytes, 64 * 1024);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pool.metrics().leased_bytes, 0);
        assert_eq!(pool.metrics().quarantine_count, 0);
        drop(reserve(&pool));
        assert_eq!(pool.metrics().allocation_count, 1);
    }

    #[test]
    fn unexpected_disk_ownership_is_quarantined() {
        let pool = BufferPool::new(BufferPoolConfig::new(64 * 1024, 64 * 1024)).unwrap();
        let mut buffer = reserve(&pool);
        buffer.mark_filled(1, OwnerTag::Storage).unwrap();
        buffer
            .transition(BufferState::DiskQueued, OwnerTag::Storage)
            .unwrap();
        let id = buffer.id();
        drop(buffer);
        assert_eq!(pool.metrics().free_bytes, 0);
        assert_eq!(pool.metrics().quarantine_count, 1);
        assert!(pool.resolve_quarantine(id));
    }
}
