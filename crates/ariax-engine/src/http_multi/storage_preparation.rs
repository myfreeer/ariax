//! Bounded blocking preparation, awaited through cooperative transfer drain.

use super::*;
use tokio::sync::OwnedSemaphorePermit;

impl HttpMultiRangeWorker {
    pub(super) async fn blocking_storage<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, HttpMultiRangeError> {
        let slot = Arc::clone(&self.preparation_slot)
            .acquire_owned()
            .await
            .map_err(|_| HttpMultiRangeError::Protocol)?;
        run_blocking(slot, work).await
    }

    pub(super) async fn prepare_storage_async(
        &self,
        task: Arc<TransferTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
    ) -> Result<PreparedHttpStorage, HttpMultiRangeError> {
        let slot = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled),
            slot = Arc::clone(&self.preparation_slot).acquire_owned() =>
                slot.map_err(|_| HttpMultiRangeError::Protocol)?,
        };
        let worker = self.clone();
        run_blocking(slot, move || {
            // The job may have waited in Tokio's blocking queue. Cancellation
            // here still precedes every filesystem or session-owner effect.
            if cancellation.is_cancelled() {
                return Err(HttpMultiRangeError::Cancelled);
            }
            #[cfg(test)]
            if let Some(gate) = worker
                .preparation_gate
                .lock()
                .expect("preparation gate")
                .clone()
            {
                gate.wait();
            }
            let prepared = match worker.prepare_storage(&task, generation) {
                Ok(prepared) => prepared,
                Err(error) => {
                    worker.handoff_new_or_recovered_journal_blocking(&task, generation)?;
                    return Err(error);
                }
            };
            let prepared = prepared
                .manage(worker.session.as_ref(), task.gid())
                .map_err(KnownLengthHttpError::from)?;
            if cancellation.is_cancelled() {
                worker.handoff_journal_blocking(task.gid(), prepared.into_appender())?;
                return Err(HttpMultiRangeError::Cancelled);
            }
            Ok(prepared)
        })
        .await?
    }
}

async fn run_blocking<T: Send + 'static>(
    slot: OwnedSemaphorePermit,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, HttpMultiRangeError> {
    // Do not select against cancellation after submission. The supervised
    // transfer must retain this join until all journal effects have drained.
    let (result, _slot) = tokio::task::spawn_blocking(move || (work(), slot))
        .await
        .map_err(|_| HttpMultiRangeError::Protocol)?;
    Ok(result)
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct PreparationGate {
    pub(super) entered: AtomicBool,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}

#[cfg(test)]
impl PreparationGate {
    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        let (released, _) = self
            .wake
            .wait_timeout_while(
                self.released.lock().expect("gate"),
                Duration::from_secs(5),
                |released| !*released,
            )
            .expect("gate wait");
        assert!(
            *released,
            "preparation gate was not released by the async worker"
        );
    }

    pub(super) fn release(&self) {
        *self.released.lock().expect("gate") = true;
        self.wake.notify_all();
    }
}
