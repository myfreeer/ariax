use super::*;
use crate::{
    ProtocolFailure,
    sftp::{SftpSession, sftp_error},
};
use futures_util::{StreamExt, stream::FuturesOrdered};
#[cfg(test)]
mod tests;

impl HttpMultiRangeWorker {
    pub(super) async fn connect_sftp_with_retry(
        &self,
        task: &Arc<HttpTaskSpec>,
        source: &crate::HttpSourceSpec,
        generation: Generation,
        cancellation: &HttpCancellation,
        stats: &HttpTransferStats,
    ) -> Result<SftpSession, HttpMultiRangeError> {
        let mut budget = self.connection_retry_budget(task, generation).await?;
        loop {
            budget.wait(source.id(), cancellation).await?;
            let record = budget.begin(source.id())?;
            self.persist_connection_retry(task, generation, record)
                .await?;
            let result = SftpSession::connect(
                &self.client,
                task.clone(),
                source,
                generation,
                &self.config.protocol_metadata,
                &self.config.sftp_ingress,
                self.config
                    .storage
                    .cpu_pool
                    .as_ref()
                    .ok_or(HttpMultiRangeError::InvalidConfig)?,
                cancellation,
                stats.clone(),
            )
            .await;
            match result {
                Ok(session) => return Ok(session),
                Err(error) => {
                    if cancellation.is_cancelled() {
                        return Err(HttpMultiRangeError::Cancelled);
                    }
                    let Some(record) = budget.failure(source.id(), &error)? else {
                        return Err(error);
                    };
                    self.persist_connection_retry(task, generation, record)
                        .await?;
                    stats.add_retry();
                }
            }
        }
    }
}

struct AttemptDrain {
    session: Arc<SftpSession>,
    armed: bool,
}
impl Drop for AttemptDrain {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.session.raw.close_session();
        }
    }
}
struct ReadCompletion {
    offset: u64,
    result: Result<BufferLease, RangeAttemptFailure>,
    ingress: HttpIngressPermit,
    _decode: HttpIngressPermit,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::http_multi) async fn range_attempt(
    session: Arc<SftpSession>,
    assignment: HttpRangeAssignment,
    timeout: Duration,
    rate: RateArbiter,
    path: RatePath,
    ingress: HttpIngressBudgets,
    frame: NonZeroUsize,
    decode: HttpIngressBudgets,
    discard: HttpDiscardAttemptGuard,
    cancellation: HttpCancellation,
    events: mpsc::Sender<AttemptEvent>,
    stats: HttpTransferStats,
) {
    let mut guard = AttemptDrain {
        session: session.clone(),
        armed: true,
    };
    let mut requests = FuturesOrdered::new();
    let outcome=async {
        let (start,proceed)=oneshot::channel();
        events.send(AttemptEvent::Head {lease:assignment.lease,response_digest:None,start}).await.map_err(|_|RangeAttemptFailure::Cancelled)?;
        proceed.await.map_err(|_|RangeAttemptFailure::Cancelled)??;
        let mut offset=assignment.span.offset;
        let end=offset+assignment.span.len as u64;
        let mut progress=tokio::time::Instant::now();
        while offset<end || !requests.is_empty() {
            while offset<end && requests.len()<64 {
                let Ok(slot)=session.slots.clone().try_acquire_owned() else {break;};
                let count=((end-offset) as usize).min(session.max_read).min(frame.get());
                let Some(permit)=rate.try_acquire(path,NonZeroUsize::new(count).ok_or(RangeAttemptFailure::Cancelled)?).map_err(|_|RangeAttemptFailure::Cancelled)? else {break;};
                let count=count.min(permit.reserved_bytes());
                let Ok(decode_permit)=decode.try_acquire(session.raw.packet_cap() as usize+count) else {break;};
                let (send,receive)=oneshot::channel();
                events.send(AttemptEvent::PrepareRead {lease:assignment.lease,minimum_capacity:count,response:send}).await.map_err(|_|RangeAttemptFailure::Cancelled)?;
                let Some(buffer)=receive.await.ok().flatten() else {break;};
                let Ok(ingress_permit)=ingress.try_acquire(buffer.capacity()) else {drop(buffer);break;};
                let session=session.clone();let stats=stats.clone();let discard=discard.clone();
                // Dropping a JoinHandle detaches its bounded accepted work. Its
                // permits remain owned until RawSftpSession completes or drains.
                requests.push_back(tokio::spawn(read_exact(session,offset,count,buffer,permit,ingress_permit,decode_permit,slot,stats,discard)));
                offset+=count as u64;
            }
            if requests.is_empty() {
                if progress.elapsed() >= timeout { return Err(RangeAttemptFailure::Timeout); }
                tokio::select! {biased;_=cancellation.cancelled()=>return Err(RangeAttemptFailure::Cancelled),
                    ()=tokio::time::sleep(Duration::from_millis(1))=>{}}
                continue;
            }
            let completion=tokio::select! {biased;_=cancellation.cancelled()=>return Err(RangeAttemptFailure::Cancelled),
                completion=tokio::time::timeout(timeout,requests.next())=>completion.map_err(|_|RangeAttemptFailure::Timeout)?.ok_or(RangeAttemptFailure::Cancelled)?.map_err(|_|RangeAttemptFailure::Cancelled)?};
            let ReadCompletion {offset,result,ingress,..}=completion;
            let buffer=result?;
            progress=tokio::time::Instant::now();
            let len=buffer.len();
            if events.send(AttemptEvent::Chunk {lease:assignment.lease,offset,buffer,_ingress:ingress,discard:discard.clone()}).await.is_err() {
                record_attempt_discarded(&discard,&stats,len)?;return Err(RangeAttemptFailure::Cancelled);
            }
        }
        Ok(None)
    }.await;
    if outcome.is_err() {
        session.raw.drain().await;
        while let Some(completion) = requests.next().await {
            if let Ok(ReadCompletion {
                result: Ok(buffer), ..
            }) = completion
            {
                let _ = record_attempt_discarded(&discard, &stats, buffer.len());
            }
        }
    } else {
        guard.armed = false;
    }
    let _ = events
        .send(AttemptEvent::Terminal {
            lease: assignment.lease,
            result: outcome,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn read_exact(
    session: Arc<SftpSession>,
    offset: u64,
    count: usize,
    mut buffer: BufferLease,
    rate: RatePermit,
    ingress: HttpIngressPermit,
    decode: HttpIngressPermit,
    slot: tokio::sync::OwnedSemaphorePermit,
    stats: HttpTransferStats,
    discard: HttpDiscardAttemptGuard,
) -> ReadCompletion {
    let mut received = 0;
    let mut wire_bytes = 0;
    let result = async {
        while received < count {
            let response = session
                .raw
                .read(
                    session.handle.as_slice(),
                    offset + received as u64,
                    (count - received) as u32,
                )
                .await;
            let data = match response {
                Ok(data) => data.data,
                Err(russh_sftp::client::error::Error::Status(status))
                    if status.status_code == russh_sftp::protocol::StatusCode::Eof =>
                {
                    return Err(RangeAttemptFailure::Transfer(ProtocolFailure::Data));
                }
                Err(error) => return Err(RangeAttemptFailure::Transfer(sftp_error(error))),
            };
            stats.add_raw(data.len());
            wire_bytes += data.len();
            if data.is_empty() || data.len() > count - received {
                record_attempt_discarded(&discard, &stats, data.len())?;
                return Err(RangeAttemptFailure::Transfer(ProtocolFailure::Malformed));
            }
            buffer
                .writable()
                .map_err(|_| RangeAttemptFailure::Cancelled)?[received..received + data.len()]
                .copy_from_slice(&data);
            received += data.len();
            // The raw Vec is released here; the pooled allocation takes over.
        }
        buffer
            .mark_filled(count, OwnerTag::Storage)
            .map_err(|_| RangeAttemptFailure::Cancelled)?;
        Ok(())
    }
    .await;
    let charge = rate.settle(wire_bytes);
    stats.set_rate_debt(charge.debt_bytes);
    let result = match result {
        Ok(()) => Ok(buffer),
        Err(error) => {
            let _ = record_attempt_discarded(&discard, &stats, received);
            Err(error)
        }
    };
    ReadCompletion {
        offset,
        result,
        ingress,
        _decode: decode,
        _slot: slot,
    }
}
