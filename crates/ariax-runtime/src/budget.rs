use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A cloneable hard byte budget shared by all permits in one accounting domain.
#[derive(Clone, Debug)]
pub struct ByteBudget {
    inner: Arc<BudgetInner>,
}

#[derive(Debug)]
struct BudgetInner {
    limit: usize,
    used: AtomicUsize,
}

impl ByteBudget {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                limit,
                used: AtomicUsize::new(0),
            }),
        }
    }

    #[must_use]
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    #[must_use]
    pub fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn available(&self) -> usize {
        self.limit().saturating_sub(self.used())
    }

    pub fn try_acquire(&self, bytes: usize) -> Result<BytePermit, BudgetError> {
        if bytes == 0 {
            return Err(BudgetError::ZeroReservation);
        }
        let mut used = self.inner.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(bytes).ok_or(BudgetError::Exhausted {
                requested: bytes,
                available: self.inner.limit.saturating_sub(used),
            })?;
            if next > self.inner.limit {
                return Err(BudgetError::Exhausted {
                    requested: bytes,
                    available: self.inner.limit.saturating_sub(used),
                });
            }
            match self.inner.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(BytePermit {
                        inner: Some(Arc::clone(&self.inner)),
                        bytes,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }
}

/// A move-only byte charge released on drop.
pub struct BytePermit {
    inner: Option<Arc<BudgetInner>>,
    bytes: usize,
}

impl BytePermit {
    /// Returns unused credit without releasing the retained allocation's charge.
    /// A permit can only shrink; growing requires fresh admission.
    pub fn shrink_to(&mut self, bytes: usize) -> Result<(), BudgetError> {
        if bytes > self.bytes {
            return Err(BudgetError::Exhausted {
                requested: bytes,
                available: self.bytes,
            });
        }
        if let Some(inner) = &self.inner {
            inner.used.fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        }
        self.bytes = bytes;
        Ok(())
    }

    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
}

impl fmt::Debug for BytePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BytePermit")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let previous = inner.used.fetch_sub(self.bytes, Ordering::AcqRel);
            debug_assert!(previous >= self.bytes, "byte budget underflow");
        }
    }
}

/// Why a byte charge could not be reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetError {
    ZeroReservation,
    Exhausted { requested: usize, available: usize },
}

impl fmt::Display for BudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroReservation => formatter.write_str("zero-byte reservations are invalid"),
            Self::Exhausted {
                requested,
                available,
            } => write!(
                formatter,
                "byte budget exhausted: requested {requested}, available {available}"
            ),
        }
    }
}

impl Error for BudgetError {}

#[cfg(test)]
mod tests {
    use super::{BudgetError, ByteBudget};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn shrinking_preserves_retained_credit_and_cannot_grow_a_permit() {
        let budget = ByteBudget::new(100);
        let mut first = budget.try_acquire(80).unwrap();
        first.shrink_to(30).unwrap();
        let second = budget.try_acquire(70).unwrap();
        assert!(first.shrink_to(31).is_err());
        assert_eq!(budget.used(), 100);
        drop(first);
        assert_eq!(budget.used(), 70);
        drop(second);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn permits_enforce_and_release_the_hard_limit() {
        let budget = ByteBudget::new(10);
        let first = budget.try_acquire(7).expect("first permit");
        assert_eq!(budget.used(), 7);
        assert_eq!(
            budget.try_acquire(4).expect_err("exhausted"),
            BudgetError::Exhausted {
                requested: 4,
                available: 3,
            }
        );
        drop(first);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_acquire(10).is_ok());
    }

    #[test]
    fn zero_and_overflow_reservations_are_rejected() {
        let budget = ByteBudget::new(usize::MAX);
        assert_eq!(
            budget.try_acquire(0).expect_err("zero"),
            BudgetError::ZeroReservation
        );
        let _all = budget.try_acquire(usize::MAX).expect("entire budget");
        assert!(matches!(
            budget.try_acquire(1),
            Err(BudgetError::Exhausted { .. })
        ));
    }

    #[test]
    fn concurrent_acquisition_never_oversubscribes() {
        let budget = ByteBudget::new(8);
        let start = Arc::new(Barrier::new(16));
        let attempted = Arc::new(Barrier::new(16));
        let acquired = Arc::new(AtomicUsize::new(0));
        thread::scope(|scope| {
            for _ in 0..16 {
                let budget = budget.clone();
                let start = Arc::clone(&start);
                let attempted = Arc::clone(&attempted);
                let acquired = Arc::clone(&acquired);
                scope.spawn(move || {
                    start.wait();
                    let permit = budget.try_acquire(1).ok();
                    if permit.is_some() {
                        acquired.fetch_add(1, Ordering::Relaxed);
                    }
                    attempted.wait();
                    drop(permit);
                });
            }
        });
        assert_eq!(acquired.load(Ordering::Relaxed), 8);
        assert_eq!(budget.used(), 0);
    }
}
