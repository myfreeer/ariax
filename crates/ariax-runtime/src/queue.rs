use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

/// Why a queue stopped accepting work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    Shutdown,
    Cancelled,
    Faulted,
}

/// Snapshot metrics for an ordinary bounded queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueueMetrics {
    pub len: usize,
    pub capacity: usize,
    pub used_bytes: usize,
    pub byte_capacity: usize,
    pub reserved_items: usize,
    pub reserved_bytes: usize,
    pub enqueued: u64,
    pub dequeued: u64,
    pub full: u64,
    pub closed_rejections: u64,
    pub peak_len: usize,
}

/// A queue send failure that returns ownership of the message.
#[derive(Debug, Eq, PartialEq)]
pub enum QueueSendError<T> {
    Full(T),
    Closed { reason: CloseReason, value: T },
    ItemTooLarge(T),
}

/// A bounded item-and-byte queue with explicit pre-read credit reservations.
#[derive(Clone)]
pub struct BoundedQueue<T> {
    inner: Arc<QueueInner<T>>,
}

impl<T> BoundedQueue<T> {
    #[must_use]
    pub fn new(capacity: usize, byte_capacity: usize) -> Self {
        assert!(capacity > 0, "queue capacity must be nonzero");
        assert!(byte_capacity > 0, "queue byte capacity must be nonzero");
        Self {
            inner: Arc::new(QueueInner {
                state: Mutex::new(QueueState {
                    queue: VecDeque::with_capacity(capacity),
                    used_bytes: 0,
                    reserved_items: 0,
                    reserved_bytes: 0,
                    close_reason: None,
                    metrics: QueueMetrics {
                        capacity,
                        byte_capacity,
                        ..QueueMetrics::default()
                    },
                }),
            }),
        }
    }

    pub fn try_send(&self, value: T, bytes: usize) -> Result<(), QueueSendError<T>> {
        if bytes > self.byte_capacity() {
            return Err(QueueSendError::ItemTooLarge(value));
        }
        let mut state = self.inner.lock();
        if let Some(reason) = state.close_reason {
            state.metrics.closed_rejections += 1;
            return Err(QueueSendError::Closed { reason, value });
        }
        if !state.has_capacity(bytes) {
            state.metrics.full += 1;
            return Err(QueueSendError::Full(value));
        }
        state.enqueue(value, bytes);
        Ok(())
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<QueuePermit<T>, QueueReserveError> {
        if bytes > self.byte_capacity() {
            return Err(QueueReserveError::ItemTooLarge);
        }
        let mut state = self.inner.lock();
        if let Some(reason) = state.close_reason {
            state.metrics.closed_rejections += 1;
            return Err(QueueReserveError::Closed(reason));
        }
        if !state.has_capacity(bytes) {
            state.metrics.full += 1;
            return Err(QueueReserveError::Full);
        }
        state.reserved_items += 1;
        state.reserved_bytes += bytes;
        state.sync_metrics();
        Ok(QueuePermit {
            inner: Some(Arc::clone(&self.inner)),
            bytes,
        })
    }

    pub fn try_recv(&self) -> Option<T> {
        let mut state = self.inner.lock();
        let (value, bytes) = state.queue.pop_front()?;
        state.used_bytes -= bytes;
        state.metrics.dequeued += 1;
        state.sync_metrics();
        Some(value)
    }

    pub fn close(&self, reason: CloseReason) -> Vec<T> {
        let mut state = self.inner.lock();
        state.close_reason.get_or_insert(reason);
        let values = state.queue.drain(..).map(|(value, _)| value).collect();
        state.used_bytes = 0;
        state.sync_metrics();
        values
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.lock().metrics.capacity
    }

    #[must_use]
    pub fn byte_capacity(&self) -> usize {
        self.inner.lock().metrics.byte_capacity
    }

    #[must_use]
    pub fn metrics(&self) -> QueueMetrics {
        self.inner.lock().metrics
    }
}

impl<T> fmt::Debug for BoundedQueue<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedQueue")
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}

/// A move-only ordinary queue capacity reservation.
pub struct QueuePermit<T> {
    inner: Option<Arc<QueueInner<T>>>,
    bytes: usize,
}

impl<T> QueuePermit<T> {
    pub fn send(mut self, value: T) -> Result<(), QueueSendError<T>> {
        let inner = self.inner.take().expect("live permit");
        let mut state = inner.lock();
        state.release_reservation(self.bytes);
        if let Some(reason) = state.close_reason {
            state.metrics.closed_rejections += 1;
            return Err(QueueSendError::Closed { reason, value });
        }
        state.enqueue(value, self.bytes);
        Ok(())
    }

    pub fn cancel(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.lock().release_reservation(self.bytes);
        }
    }
}

impl<T> fmt::Debug for QueuePermit<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueuePermit")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl<T> Drop for QueuePermit<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.lock().release_reservation(self.bytes);
        }
    }
}

/// Why queue capacity could not be reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueReserveError {
    Full,
    Closed(CloseReason),
    ItemTooLarge,
}

struct QueueInner<T> {
    state: Mutex<QueueState<T>>,
}

impl<T> QueueInner<T> {
    fn lock(&self) -> MutexGuard<'_, QueueState<T>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct QueueState<T> {
    queue: VecDeque<(T, usize)>,
    used_bytes: usize,
    reserved_items: usize,
    reserved_bytes: usize,
    close_reason: Option<CloseReason>,
    metrics: QueueMetrics,
}

impl<T> QueueState<T> {
    fn has_capacity(&self, bytes: usize) -> bool {
        self.queue.len() + self.reserved_items < self.metrics.capacity
            && self
                .used_bytes
                .checked_add(self.reserved_bytes)
                .and_then(|used| used.checked_add(bytes))
                .is_some_and(|used| used <= self.metrics.byte_capacity)
    }

    fn enqueue(&mut self, value: T, bytes: usize) {
        self.queue.push_back((value, bytes));
        self.used_bytes += bytes;
        self.metrics.enqueued += 1;
        self.metrics.peak_len = self.metrics.peak_len.max(self.queue.len());
        self.sync_metrics();
    }

    fn release_reservation(&mut self, bytes: usize) {
        self.reserved_items -= 1;
        self.reserved_bytes -= bytes;
        self.sync_metrics();
    }

    fn sync_metrics(&mut self) {
        self.metrics.len = self.queue.len();
        self.metrics.used_bytes = self.used_bytes;
        self.metrics.reserved_items = self.reserved_items;
        self.metrics.reserved_bytes = self.reserved_bytes;
    }
}

/// Snapshot metrics for a permit-reserved completion drain.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompletionDrainMetrics {
    pub len: usize,
    pub capacity: usize,
    pub reserved: usize,
    pub sent: u64,
    pub received: u64,
    pub rejected_before_acceptance: u64,
    pub leaked_permits: u64,
    pub admission_closed: bool,
    pub receiver_closed: bool,
}

/// A completion queue whose capacity is acquired before backend acceptance.
#[derive(Clone)]
pub struct CompletionDrain<T> {
    inner: Arc<CompletionInner<T>>,
}

impl<T> CompletionDrain<T> {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "completion capacity must be nonzero");
        Self {
            inner: Arc::new(CompletionInner {
                state: Mutex::new(CompletionState {
                    queue: VecDeque::with_capacity(capacity),
                    capacity,
                    reserved: 0,
                    sent: 0,
                    received: 0,
                    rejected_before_acceptance: 0,
                    leaked_permits: 0,
                    admission_closed: false,
                    receiver_closed: false,
                }),
            }),
        }
    }

    pub fn try_reserve(&self) -> Option<CompletionPermit<T>> {
        let mut state = self.inner.lock();
        if state.admission_closed
            || state.receiver_closed
            || state.queue.len() + state.reserved >= state.capacity
        {
            return None;
        }
        state.reserved += 1;
        Some(CompletionPermit {
            inner: Some(Arc::clone(&self.inner)),
        })
    }

    pub fn try_recv(&self) -> Option<T> {
        let mut state = self.inner.lock();
        let value = state.queue.pop_front()?;
        state.received += 1;
        Some(value)
    }

    pub fn close_admission(&self) {
        self.inner.lock().admission_closed = true;
    }

    pub fn finish_close(&self) -> bool {
        let mut state = self.inner.lock();
        if !state.admission_closed || state.reserved != 0 || !state.queue.is_empty() {
            return false;
        }
        state.receiver_closed = true;
        true
    }

    #[must_use]
    pub fn metrics(&self) -> CompletionDrainMetrics {
        let state = self.inner.lock();
        CompletionDrainMetrics {
            len: state.queue.len(),
            capacity: state.capacity,
            reserved: state.reserved,
            sent: state.sent,
            received: state.received,
            rejected_before_acceptance: state.rejected_before_acceptance,
            leaked_permits: state.leaked_permits,
            admission_closed: state.admission_closed,
            receiver_closed: state.receiver_closed,
        }
    }
}

impl<T> fmt::Debug for CompletionDrain<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletionDrain")
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}

/// A move-only completion slot consumed by exactly one outcome or rejection.
pub struct CompletionPermit<T> {
    inner: Option<Arc<CompletionInner<T>>>,
}

impl<T> CompletionPermit<T> {
    pub fn send(mut self, value: T) {
        let inner = self.inner.take().expect("live completion permit");
        let mut state = inner.lock();
        debug_assert!(state.reserved > 0);
        debug_assert!(!state.receiver_closed);
        state.reserved -= 1;
        state.queue.push_back(value);
        state.sent += 1;
    }

    pub fn reject(mut self) {
        let inner = self.inner.take().expect("live completion permit");
        let mut state = inner.lock();
        debug_assert!(state.reserved > 0);
        state.reserved -= 1;
        state.rejected_before_acceptance += 1;
    }
}

impl<T> fmt::Debug for CompletionPermit<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CompletionPermit { .. }")
    }
}

impl<T> Drop for CompletionPermit<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let mut state = inner.lock();
            debug_assert!(state.reserved > 0);
            state.reserved -= 1;
            state.leaked_permits += 1;
        }
    }
}

struct CompletionInner<T> {
    state: Mutex<CompletionState<T>>,
}

impl<T> CompletionInner<T> {
    fn lock(&self) -> MutexGuard<'_, CompletionState<T>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct CompletionState<T> {
    queue: VecDeque<T>,
    capacity: usize,
    reserved: usize,
    sent: u64,
    received: u64,
    rejected_before_acceptance: u64,
    leaked_permits: u64,
    admission_closed: bool,
    receiver_closed: bool,
}

#[cfg(test)]
mod tests {
    use super::{BoundedQueue, CloseReason, CompletionDrain, QueueReserveError, QueueSendError};
    use std::thread;

    #[test]
    fn ordinary_queue_enforces_item_and_byte_capacity() {
        let queue = BoundedQueue::new(2, 5);
        queue.try_send("a", 3).expect("first");
        assert_eq!(queue.try_send("b", 3), Err(QueueSendError::Full("b")));
        queue.try_send("c", 2).expect("second");
        assert_eq!(queue.try_send("d", 1), Err(QueueSendError::Full("d")));
        assert_eq!(queue.try_recv(), Some("a"));
        assert_eq!(queue.try_recv(), Some("c"));
        assert_eq!(queue.metrics().peak_len, 2);
    }

    #[test]
    fn reserved_credit_prevents_a_capacity_race() {
        let queue = BoundedQueue::new(1, 8);
        let permit = queue.try_reserve(8).expect("permit");
        assert_eq!(
            queue.try_send("other", 1),
            Err(QueueSendError::Full("other"))
        );
        assert!(matches!(queue.try_reserve(1), Err(QueueReserveError::Full)));
        permit.send("reserved").expect("send reserved");
        assert_eq!(queue.try_recv(), Some("reserved"));
    }

    #[test]
    fn close_returns_queued_messages_and_preclosed_permits_return_values() {
        let queue = BoundedQueue::new(2, 8);
        queue.try_send("queued", 1).expect("queued");
        let permit = queue.try_reserve(1).expect("permit");
        assert_eq!(queue.close(CloseReason::Shutdown), ["queued"]);
        assert_eq!(
            permit.send("reserved"),
            Err(QueueSendError::Closed {
                reason: CloseReason::Shutdown,
                value: "reserved",
            })
        );
    }

    #[test]
    fn accepted_completion_always_sends_after_admission_close() {
        let drain = CompletionDrain::new(1);
        let permit = drain.try_reserve().expect("completion permit");
        assert!(drain.try_reserve().is_none());
        drain.close_admission();
        assert!(!drain.finish_close());
        permit.send("done");
        assert_eq!(drain.try_recv(), Some("done"));
        assert!(drain.finish_close());
        assert!(drain.metrics().receiver_closed);
    }

    #[test]
    fn rejected_and_leaked_completion_permits_restore_capacity_and_are_counted() {
        let drain = CompletionDrain::<()>::new(1);
        drain.try_reserve().expect("permit").reject();
        assert_eq!(drain.metrics().rejected_before_acceptance, 1);
        drop(drain.try_reserve().expect("leaked permit"));
        assert_eq!(drain.metrics().leaked_permits, 1);
        assert!(drain.try_reserve().is_some());
    }

    #[test]
    fn concurrent_accepted_completions_are_delivered_exactly_once() {
        let drain = CompletionDrain::new(32);
        let permits = (0..32)
            .map(|_| drain.try_reserve().expect("reserved completion"))
            .collect::<Vec<_>>();
        thread::scope(|scope| {
            for (value, permit) in permits.into_iter().enumerate() {
                scope.spawn(move || permit.send(value));
            }
        });
        let mut values = (0..32)
            .map(|_| drain.try_recv().expect("completion"))
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, (0..32).collect::<Vec<_>>());
        assert_eq!(drain.metrics().sent, 32);
        assert_eq!(drain.metrics().received, 32);
    }
}
