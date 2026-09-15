use crate::{ByteBudget, BytePermit};
use std::{
    error::Error,
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
pub struct CpuPoolConfig {
    pub workers: usize,
    pub jobs: usize,
    pub bytes: usize,
    pub resident: ByteBudget,
    /// The same pool is passed to positional disk work in the compact profile.
    pub shared_disk: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CpuError {
    InvalidConfig,
    Start,
    Capacity,
    Closed,
    Panicked,
}
impl fmt::Display for CpuError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bounded CPU executor: {self:?}")
    }
}
impl Error for CpuError {}

struct Inner {
    pool: rayon::ThreadPool,
    jobs: ByteBudget,
    bytes: ByteBudget,
    resident: ByteBudget,
    accepting: AtomicBool,
    shared_disk: bool,
}

/// A private fixed worker set. Its unobservable internal queue is reachable
/// only after reserving a job, completion, scratch bytes and resident bytes.
#[derive(Clone)]
pub struct CpuPool(Arc<Inner>);
impl fmt::Debug for CpuPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CpuPool")
            .field("jobs", &self.0.jobs.used())
            .field("bytes", &self.0.bytes.used())
            .field("shared_disk", &self.0.shared_disk)
            .finish()
    }
}
struct Reservations {
    _job: BytePermit,
    _bytes: BytePermit,
    _resident: BytePermit,
}
pub struct CpuReservation {
    pool: CpuPool,
    reservations: Reservations,
}

pub struct CpuTask<T> {
    receiver: oneshot::Receiver<CpuOutput<Result<T, CpuError>>>,
}
/// Reservations survive result delivery until the receiver consumes it.
pub struct CpuOutput<T> {
    value: T,
    _reservations: Reservations,
}
impl<T> std::ops::Deref for CpuOutput<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T> CpuOutput<T> {
    /// Transfer any retained allocation to its next owner's reservation before
    /// consuming this result. Pooled buffers already carry that reservation.
    pub fn into_inner(self) -> T {
        self.value
    }
}
impl<T> CpuTask<T> {
    pub async fn join(self) -> Result<CpuOutput<T>, CpuError> {
        let output = self.receiver.await.map_err(|_| CpuError::Closed)?;
        Ok(CpuOutput {
            value: output.value?,
            _reservations: output._reservations,
        })
    }
}

impl CpuPool {
    pub fn new(config: CpuPoolConfig) -> Result<Self, CpuError> {
        if config.workers == 0
            || config.workers > 64
            || config.jobs == 0
            || config.jobs > 4096
            || config.bytes == 0
            || config.bytes > config.resident.limit()
            || (config.shared_disk && config.workers != 1)
        {
            return Err(CpuError::InvalidConfig);
        }
        let prefix = if config.shared_disk {
            "ariax-disk-cpu"
        } else {
            "ariax-cpu"
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.workers)
            .thread_name(move |id| format!("{prefix}-{id}"))
            .build()
            .map_err(|_| CpuError::Start)?;
        Ok(Self(Arc::new(Inner {
            pool,
            jobs: ByteBudget::new(config.jobs),
            bytes: ByteBudget::new(config.bytes),
            resident: config.resident,
            accepting: AtomicBool::new(true),
            shared_disk: config.shared_disk,
        })))
    }
    pub fn shared_disk(&self) -> bool {
        self.0.shared_disk
    }
    pub fn accepted_jobs(&self) -> usize {
        self.0.jobs.used()
    }
    pub fn reserved_bytes(&self) -> usize {
        self.0.bytes.used()
    }
    pub fn close(&self) {
        self.0.accepting.store(false, Ordering::Release);
    }

    pub fn submit<T: Send + 'static>(
        &self,
        bytes: usize,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<CpuTask<T>, CpuError> {
        Ok(self.reserve(bytes)?.spawn(work))
    }

    pub fn reserve(&self, bytes: usize) -> Result<CpuReservation, CpuError> {
        if !self.0.accepting.load(Ordering::Acquire) {
            return Err(CpuError::Closed);
        }
        if bytes == 0 {
            return Err(CpuError::InvalidConfig);
        }
        let reservations = Reservations {
            _job: self.0.jobs.try_acquire(1).map_err(|_| CpuError::Capacity)?,
            _bytes: self
                .0
                .bytes
                .try_acquire(bytes)
                .map_err(|_| CpuError::Capacity)?,
            _resident: self
                .0
                .resident
                .try_acquire(bytes)
                .map_err(|_| CpuError::Capacity)?,
        };
        if !self.0.accepting.load(Ordering::Acquire) {
            return Err(CpuError::Closed);
        }
        Ok(CpuReservation {
            pool: self.clone(),
            reservations,
        })
    }
}

impl CpuReservation {
    pub fn spawn<T: Send + 'static>(self, work: impl FnOnce() -> T + Send + 'static) -> CpuTask<T> {
        let (sender, receiver) = oneshot::channel();
        let reservations = self.reservations;
        self.pool.0.pool.spawn(move || {
            let value = catch_unwind(AssertUnwindSafe(work)).map_err(|_| CpuError::Panicked);
            let _ = sender.send(CpuOutput {
                value,
                _reservations: reservations,
            });
        });
        CpuTask { receiver }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pool() -> CpuPool {
        CpuPool::new(CpuPoolConfig {
            workers: 1,
            jobs: 1,
            bytes: 16,
            resident: ByteBudget::new(16),
            shared_disk: true,
        })
        .unwrap()
    }
    #[tokio::test]
    async fn reservations_survive_execution_and_completion() {
        let pool = pool();
        let task = pool.submit(16, || 42).unwrap();
        assert!(matches!(pool.submit(1, || 0), Err(CpuError::Capacity)));
        let output = task.join().await.unwrap();
        assert_eq!(*output, 42);
        assert_eq!(pool.accepted_jobs(), 1);
        drop(output);
        assert_eq!(pool.reserved_bytes(), 0);
        pool.close();
        assert!(matches!(pool.submit(1, || 0), Err(CpuError::Closed)));
    }
    #[tokio::test]
    async fn cancelled_receiver_cannot_release_running_work() {
        let pool = pool();
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        let task = pool.submit(16, move || wait.recv().unwrap()).unwrap();
        drop(task);
        assert_eq!(pool.accepted_jobs(), 1);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pool.accepted_jobs() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let task = pool
            .submit(1, || panic!("injected worker failure"))
            .unwrap();
        assert!(matches!(task.join().await, Err(CpuError::Panicked)));
        assert_eq!(pool.reserved_bytes(), 0);
    }
}
