//! Durable connection and sequential-stream credit shared by FTP and SFTP.
use super::*;
use crate::storage_journal::JournalWrite;

pub(super) struct ProtocolRetryBudget {
    budget: HttpRetryBudget,
    elapsed: Duration,
    started: Instant,
    waits: BTreeMap<UriId, u64>,
}

impl ProtocolRetryBudget {
    pub(super) fn restore(
        states: &[RecoveredRetryState],
        policy: &HttpRetryPolicy,
    ) -> Result<Self, HttpMultiRangeError> {
        let mut attempts = BTreeMap::new();
        let mut waits = BTreeMap::new();
        let mut elapsed = 0;
        let now = now_unix_ms().unwrap_or(0);
        for state in states.iter().filter(|state| state.scope == RetryScope::Uri) {
            let id = u32::try_from(state.scope_id.get() - 1)
                .map_err(|_| HttpRetryError::InvalidRecoveredState)?;
            let id = UriId::new(id);
            if attempts.insert(id, state.attempt).is_some()
                || state.delay_ms > policy.max_wait.as_millis() as u64
            {
                return Err(HttpRetryError::InvalidRecoveredState.into());
            }
            waits.insert(
                id,
                state.scheduled_at_unix_ms.saturating_add(state.delay_ms),
            );
            elapsed = elapsed.max(
                state.elapsed_before_wait_ms.saturating_add(
                    now.saturating_sub(state.scheduled_at_unix_ms)
                        .min(state.delay_ms),
                ),
            );
        }
        let mut budget = HttpRetryBudget::new(policy.clone())?;
        let total = attempts
            .values()
            .try_fold(0u32, |sum, value| sum.checked_add(*value))
            .ok_or(HttpRetryError::InvalidRecoveredState)?;
        if total != 0 {
            budget.restore_attempts(total, attempts)?;
        }
        Ok(Self {
            budget,
            elapsed: Duration::from_millis(elapsed),
            started: Instant::now(),
            waits,
        })
    }
    pub(super) async fn wait(
        &self,
        source: UriId,
        cancellation: &HttpCancellation,
    ) -> Result<(), HttpMultiRangeError> {
        let millis = self
            .waits
            .get(&source)
            .copied()
            .unwrap_or(0)
            .saturating_sub(now_unix_ms().unwrap_or(0));
        let delay = Duration::from_millis(millis).min(self.budget.policy().max_wait);
        tokio::select! { biased; _ = cancellation.cancelled() => Err(HttpMultiRangeError::Cancelled),
        () = tokio::time::sleep(delay) => Ok(()) }
    }
    pub(super) fn begin(&mut self, source: UriId) -> Result<RetryStateWrite, HttpMultiRangeError> {
        if self.elapsed.saturating_add(self.started.elapsed()) >= self.budget.policy().max_elapsed {
            return Err(HttpMultiRangeError::Exhausted);
        }
        self.budget
            .begin_attempt(source)
            .map_err(|_| HttpMultiRangeError::Exhausted)?;
        Ok(self.record(
            source,
            Duration::ZERO,
            HttpRetryCause::Protocol(crate::ProtocolFailure::Connect),
            HttpRetryDelaySource::FixedBackoff,
        ))
    }
    pub(super) fn failure(
        &mut self,
        source: UriId,
        error: &HttpMultiRangeError,
    ) -> Result<Option<RetryStateWrite>, HttpMultiRangeError> {
        let cause = cause(error);
        let elapsed = self.elapsed.saturating_add(self.started.elapsed());
        match self.budget.decide_after_failure(
            source,
            cause,
            elapsed,
            None,
            SystemTime::now(),
            u64::from(self.budget.stats().attempts),
        )? {
            HttpRetryDecision::Stop(_) => Ok(None),
            HttpRetryDecision::Retry {
                delay,
                source: delay_source,
            } => {
                let record = self.record(source, delay, cause, delay_source);
                self.waits.insert(
                    source,
                    record.scheduled_at_unix_ms.saturating_add(record.delay_ms),
                );
                Ok(Some(record))
            }
        }
    }
    fn record(
        &self,
        source: UriId,
        delay: Duration,
        cause: HttpRetryCause,
        reason: HttpRetryDelaySource,
    ) -> RetryStateWrite {
        RetryStateWrite {
            scope: RetryScope::Uri,
            scope_id: PersistedId::new(u64::from(source.get()) + 1).expect("URI plus one"),
            attempt: self.budget.attempts_for_mirror(source),
            elapsed_before_wait_ms: self
                .elapsed
                .saturating_add(self.started.elapsed())
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            scheduled_at_unix_ms: now_unix_ms().unwrap_or(0),
            delay_ms: delay.as_millis().min(u128::from(u64::MAX)) as u64,
            error_class: retry_error_kind(cause),
            retry_reason: retry_reason(reason),
        }
    }
}

pub(super) fn cause(error: &HttpMultiRangeError) -> HttpRetryCause {
    match error {
        HttpMultiRangeError::Transfer(error) => HttpRetryCause::Protocol(*error),
        HttpMultiRangeError::ShortBody => {
            HttpRetryCause::Transport(HttpRetryTransportFailure::UnexpectedEof)
        }
        HttpMultiRangeError::Cancelled => HttpRetryCause::Cancelled,
        _ => HttpRetryCause::Policy,
    }
}

impl HttpMultiRangeWorker {
    pub(super) async fn connection_retry_budget(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
    ) -> Result<ProtocolRetryBudget, HttpMultiRangeError> {
        let worker = self.clone();
        let task = task.clone();
        self.cpu(128 * 1024, move || {
            let OpenedTaskJournal { appender, state } =
                worker.open_task_journal(&task, generation)?;
            let mut appender = appender
                .manage(worker.session.as_ref(), task.gid())
                .map_err(KnownLengthHttpError::from)?;
            let rows: Vec<_> = state
                .as_ref()
                .into_iter()
                .flat_map(|state| state.retry_states().values().cloned())
                .collect();
            let budget = ProtocolRetryBudget::restore(
                &rows,
                task.options()
                    .retry
                    .as_ref()
                    .unwrap_or(&worker.config.retry),
            )?;
            appender
                .close_flushed()
                .map_err(KnownLengthHttpError::from)?;
            Ok(budget)
        })
        .await?
    }
    pub(super) async fn persist_connection_retry(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
        retry: RetryStateWrite,
    ) -> Result<(), HttpMultiRangeError> {
        let worker = self.clone();
        let task = task.clone();
        self.cpu(128 * 1024, move || {
            let OpenedTaskJournal { appender, .. } = worker.open_task_journal(&task, generation)?;
            let mut appender = appender
                .manage(worker.session.as_ref(), task.gid())
                .map_err(KnownLengthHttpError::from)?;
            let appended = appender
                .append_payload(
                    generation,
                    &ariax_storage::JournalPayload::RetryState {
                        scope: retry.scope,
                        scope_id: retry.scope_id,
                        attempt: retry.attempt,
                        elapsed_before_wait_ms: retry.elapsed_before_wait_ms,
                        scheduled_at_unix_ms: retry.scheduled_at_unix_ms,
                        delay_ms: retry.delay_ms,
                        error_class: retry.error_class,
                        retry_reason: retry.retry_reason,
                    },
                )
                .map_err(KnownLengthHttpError::from)?;
            appender
                .flush(appended.sequence())
                .map_err(KnownLengthHttpError::from)?;
            appender
                .close_flushed()
                .map_err(KnownLengthHttpError::from)?;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;
    #[test]
    fn uri_credit_survives_restart_and_rejects_invalid_or_exhausted_state() {
        let policy = HttpRetryPolicy {
            max_attempts: NonZeroU32::new(2).unwrap(),
            max_attempts_per_mirror: NonZeroU32::new(2).unwrap(),
            ..Default::default()
        };
        let source = UriId::new(0);
        let mut first = ProtocolRetryBudget::restore(&[], &policy).unwrap();
        let record = first.begin(source).unwrap();
        let recovered = RecoveredRetryState {
            scope: record.scope,
            scope_id: record.scope_id,
            attempt: record.attempt,
            elapsed_before_wait_ms: record.elapsed_before_wait_ms,
            scheduled_at_unix_ms: record.scheduled_at_unix_ms,
            delay_ms: record.delay_ms,
            error_class: record.error_class,
            retry_reason: record.retry_reason,
        };
        let mut second =
            ProtocolRetryBudget::restore(std::slice::from_ref(&recovered), &policy).unwrap();
        assert_eq!(second.begin(source).unwrap().attempt, 2);
        assert!(second.begin(UriId::new(1)).is_err());
        assert!(
            second
                .failure(source, &crate::ProtocolFailure::AuthFailure.into())
                .unwrap()
                .is_none()
        );
        assert!(ProtocolRetryBudget::restore(&[recovered.clone(), recovered], &policy).is_err());
    }
}
