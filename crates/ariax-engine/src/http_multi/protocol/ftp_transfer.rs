use super::*;
use crate::{HttpSourceSpec, ProtocolFailure, TransferProtocol, ftp::FtpSession};

impl HttpMultiRangeWorker {
    async fn connect_ftp(
        &self,
        task: &HttpTaskSpec,
        source: &HttpSourceSpec,
        cancellation: &HttpCancellation,
    ) -> Result<FtpSession, HttpMultiRangeError> {
        let tls = if source.protocol() == TransferProtocol::Ftps || task.options().transfer.ftp_tls
        {
            let policy = self.client.protocol_policy().1.direct.tls.clone();
            Some(Arc::new(
                self.cpu(128 * 1024, move || {
                    crate::http_transport::build_tls_config(&policy)
                })
                .await?
                .map_err(|_| ProtocolFailure::Tls)?,
            ))
        } else {
            None
        };
        let options = task.options().transfer.clone();
        let credential_source = source.clone();
        let credentials = self
            .cpu(crate::MAX_HTTP_NETRC_BYTES, move || {
                crate::transfer_task::load_protocol_credentials(&options, &credential_source)
            })
            .await??;
        FtpSession::connect(
            &self.client,
            source,
            task.options(),
            &self.config.protocol_metadata,
            tls,
            credentials,
            cancellation,
        )
        .await
        .map_err(Into::into)
    }

    pub(super) async fn run_ftp_task(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
        stats: HttpTransferStats,
        discard: HttpDiscardTaskGuard,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        let mut sources = task
            .sources()
            .iter()
            .filter(|source| {
                matches!(
                    source.protocol(),
                    TransferProtocol::Ftp | TransferProtocol::Ftps
                )
            })
            .collect::<Vec<_>>();
        if task.options().transfer.uri_selector != crate::UriSelector::InOrder {
            sources.sort_by_key(|source| {
                let score = crate::server_stats::transfer_origin(source.uri().unwrap_or(""))
                    .map(|origin| {
                        self.config
                            .server_stats
                            .feedback_with_timeout(
                                &origin,
                                task.options().transfer.server_stat_timeout,
                            )
                            .score(0)
                    })
                    .unwrap_or(0);
                (std::cmp::Reverse(score), source.priority())
            });
        }
        let mut first = None;
        let mut last = HttpMultiRangeError::NoUsableSources;
        let mut budget = self.connection_retry_budget(&task, generation).await?;
        for (index, source) in sources.iter().enumerate() {
            loop {
                budget.wait(source.id(), &cancellation).await?;
                let record = match budget.begin(source.id()) {
                    Ok(record) => record,
                    Err(_) => break,
                };
                self.persist_connection_retry(&task, generation, record)
                    .await?;
                match self.connect_ftp(&task, source, &cancellation).await {
                    Ok(session) => {
                        first = Some((index, session));
                        break;
                    }
                    Err(error) => {
                        if cancellation.is_cancelled() {
                            return Err(HttpMultiRangeError::Cancelled);
                        }
                        let retry = budget.failure(source.id(), &error)?;
                        last = error;
                        let Some(record) = retry else {
                            break;
                        };
                        self.persist_connection_retry(&task, generation, record)
                            .await?;
                        stats.add_retry();
                    }
                }
            }
            if first.is_some() {
                break;
            }
        }
        let (index, session) = first.ok_or(last)?;
        let total = session.validator.total_length;
        let manifest = self.transfer_manifest(&task, total)?;
        let prepare_task = task.clone();
        let worker = self.clone();
        let mut opened = self
            .cpu(128 * 1024, move || {
                worker.open_protocol_storage(&prepare_task, generation, manifest)
            })
            .await??;
        let outcome = async {
            opened.storage.finish_verification().await?;
            opened.storage.redownload_incomplete_recovery()?;
            stats.set_total_length(total);
            let _network = NetworkPhaseGuard::new(&stats);
            self.run_ftp_sources(
                &task,
                generation,
                &cancellation,
                &stats,
                &discard,
                &sources,
                index,
                Some(session),
                &mut opened,
            )
            .await
        }
        .await;
        let outcome = account_checksum_outcome(&task, &discard, &stats, total, outcome);
        self.complete_storage_outcome(&task, opened.storage, opened.layout_hash, total, outcome)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_ftp_sources(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
        cancellation: &HttpCancellation,
        stats: &HttpTransferStats,
        discard: &HttpDiscardTaskGuard,
        sources: &[&HttpSourceSpec],
        first: usize,
        mut session: Option<FtpSession>,
        opened: &mut OpenedProtocolStorage,
    ) -> Result<Option<ariax_storage::JournalDigest>, HttpMultiRangeError> {
        let policy = task.options().retry.as_ref().unwrap_or(&self.config.retry);
        let mut budget = super::retry::ProtocolRetryBudget::restore(&opened.retry_states, policy)?;
        let total = opened
            .storage
            .layout()
            .total_length()
            .ok_or(HttpMultiRangeError::Protocol)?;
        let mut index = first;
        let mut exhausted = BTreeSet::new();
        let mut last = HttpMultiRangeError::NoUsableSources;
        if !task.options().transfer.ftp_reuse_connection {
            session = None;
        }
        loop {
            if sources.is_empty() || exhausted.len() == sources.len() {
                return Err(last);
            }
            while exhausted.contains(&index) {
                index = (index + 1) % sources.len();
            }
            let source = sources[index];
            if session.is_none() {
                budget.wait(source.id(), cancellation).await?;
                match budget.begin(source.id()) {
                    Ok(record) => opened.storage.record_retry_state(record)?,
                    Err(_) => {
                        exhausted.insert(index);
                        continue;
                    }
                }
            }
            let started = Instant::now();
            let before = stats.snapshot().durable_bytes;
            let result = async {
                let mut ftp = match session.take() {
                    Some(session) => session,
                    None => self.connect_ftp(task, source, cancellation).await?,
                };
                if ftp.validator.total_length != total {
                    return Err(ProtocolFailure::StaleValidator.into());
                }
                let pieces = opened.storage.verified_pieces();
                let has_checksum = task.options().content_checksum().is_some()
                    || task.verification().is_some_and(|manifest| {
                        !manifest.chunks().is_empty() || !manifest.whole().is_empty()
                    });
                if !pieces.is_empty()
                    && !opened
                        .previous_validators
                        .get(&ftp.validator.source)
                        .is_some_and(|old| ftp.validator.permits_resume(old, has_checksum))
                    && !task.has_strict_content_identity()
                {
                    return Err(ProtocolFailure::StaleValidator.into());
                }
                opened
                    .storage
                    .record_protocol_validator(ftp.validator.clone())?;
                opened
                    .previous_validators
                    .insert(ftp.validator.source, ftp.validator.clone());
                let mut prefix = 0;
                for piece in &pieces {
                    if piece.get() * task.options().piece_length != prefix {
                        break;
                    }
                    prefix = (prefix + task.options().piece_length).min(total);
                }
                for piece in pieces
                    .into_iter()
                    .filter(|piece| piece.get() * task.options().piece_length >= prefix)
                {
                    opened.storage.invalidate_verified_piece(piece)?;
                }
                stats.set_durable(prefix);
                if prefix == total && total != 0 {
                    return opened
                        .storage
                        .verify_whole_file()
                        .await
                        .map_err(HttpMultiRangeError::Storage);
                }
                let attempt_discard = discard
                    .begin_attempt(discard_host_key(
                        source.uri().ok_or(ProtocolFailure::AuthFailure)?,
                    )?)
                    .map_err(discard_setup_error)?;
                let transfer = TransferAttemptId::new(opened.storage.next_lease_floor())
                    .ok_or(HttpMultiRangeError::IdentifierExhausted)?;
                self.stream_ftp(
                    task,
                    generation,
                    cancellation,
                    stats,
                    &attempt_discard,
                    &mut ftp,
                    prefix,
                    transfer,
                    &mut opened.storage,
                )
                .await?;
                opened
                    .storage
                    .verify_whole_file()
                    .await
                    .map_err(HttpMultiRangeError::Storage)
            }
            .await;
            if !cancellation.is_cancelled()
                && let Some(origin) =
                    crate::server_stats::transfer_origin(source.uri().unwrap_or(""))
            {
                self.config.server_stats.observe_with_timeout(
                    &origin,
                    stats.snapshot().durable_bytes.saturating_sub(before),
                    started.elapsed(),
                    result.is_ok(),
                    task.options().transfer.server_stat_timeout,
                );
            }
            match result {
                Ok(digest) => return Ok(digest),
                Err(error) => {
                    if cancellation.is_cancelled() {
                        return Err(HttpMultiRangeError::Cancelled);
                    }
                    if !policy.is_retriable(super::retry::cause(&error)) {
                        return Err(error);
                    }
                    opened.storage.redownload_incomplete_recovery()?;
                    if let Some(record) = budget.failure(source.id(), &error)? {
                        opened.storage.record_retry_state(record)?;
                        stats.add_retry();
                    } else {
                        exhausted.insert(index);
                    }
                    last = error;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_ftp(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
        cancellation: &HttpCancellation,
        stats: &HttpTransferStats,
        discard: &HttpDiscardAttemptGuard,
        ftp: &mut FtpSession,
        mut offset: u64,
        transfer: TransferAttemptId,
        storage: &mut StorageEngine,
    ) -> Result<(), HttpMultiRangeError> {
        let total = ftp.validator.total_length;
        let fingerprint = ftp.validator.fingerprint();
        let mut data = Some(
            tokio::select! { biased; _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled), result = ftp.retrieve(offset) => result? },
        );
        let mut current = None;
        let mut provisional = 0usize;
        let rate_path = RatePath {
            host: u64::from_le_bytes(
                ftp.validator.source.as_bytes()[..8]
                    .try_into()
                    .expect("hash prefix"),
            ),
            task: task.task().get(),
            stream: transfer.get(),
        };
        stats.set_active(1);
        let outcome = async {
            loop {
                let length = (total-offset).min(task.options().piece_length);
                let lease = LeaseId::new(storage.next_lease_floor()).ok_or(HttpMultiRangeError::IdentifierExhausted)?;
                if length != 0 {
                    storage.begin_lease(LeaseWritePlan { task:task.task(),generation,transfer_attempt:transfer,lease,
                        span:GlobalSpan {offset,len:usize::try_from(length).map_err(|_| HttpMultiRangeError::Protocol)?},validator:fingerprint,overlap_group:None })?;
                    current = Some(lease);
                }
                let end = offset + length;
                while offset < end {
                    let requested = usize::try_from((end-offset).min(self.config.ingress_frame_bytes.get() as u64)).map_err(|_| HttpMultiRangeError::Protocol)?;
                    let (mut buffer,ingress,rate) = self.acquire_storage_read(storage,requested,rate_path,cancellation,stats).await?;
                    let read_limit = requested.min(rate.reserved_bytes()).min(buffer.capacity());
                    let count = FtpSession::read(data.as_mut().expect("data is open"),&mut buffer.writable().map_err(|_| HttpMultiRangeError::Protocol)?[..read_limit],task.options().response_body_timeout,cancellation).await?;
                    let charge = rate.settle(count); stats.set_rate_debt(charge.debt_bytes); stats.add_raw(count);
                    if count == 0 { storage.discard_network_buffer(buffer)?; return Err(HttpMultiRangeError::ShortBody); }
                    buffer.mark_filled(count,OwnerTag::Storage).map_err(|_| HttpMultiRangeError::Protocol)?;
                    let write = storage.write_block(WriteBlock { task:task.task(),generation,lease,global_offset:offset,expected_len:count,buffer,piece:PieceId::new(offset/task.options().piece_length) }).await;
                    drop(ingress);
                    if let Err(error) = write { record_discarded(discard,stats,count)?; return Err(error.into()); }
                    offset += count as u64; provisional += count;
                    stats.add_accepted(count); stats.add_provisional(count);
                }
                if offset == total {
                    let (mut buffer,ingress,rate) = self.acquire_storage_read(storage,1,rate_path,cancellation,stats).await?;
                    let count = FtpSession::read(data.as_mut().expect("data is open"),&mut buffer.writable().map_err(|_| HttpMultiRangeError::Protocol)?[..1],task.options().response_body_timeout,cancellation).await?;
                    let charge = rate.settle(count); stats.set_rate_debt(charge.debt_bytes); stats.add_raw(count);
                    storage.discard_network_buffer(buffer)?; drop(ingress);
                    if count != 0 { record_discarded(discard,stats,count)?; return Err(HttpMultiRangeError::OversizedBody); }
                    tokio::select! { biased; _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled), result = ftp.finish(data.take().expect("data is open")) => result? };
                }
                if length != 0 {
                    storage.commit_lease(LeaseCommit { task:task.task(),generation,lease,received_len:length,validator:fingerprint,response_digest:None })?;
                    current = None;
                    storage.finish_verification().await?;
                    stats.remove_provisional(provisional); stats.add_durable(provisional); provisional = 0;
                }
                if offset == total { break; }
            }
            Ok(())
        }.await;
        // Closing the data stream precedes both lease abort and releasing its
        // connection reservation, including all cancellation/error paths.
        drop(data);
        if let Some(lease) = current {
            storage.abort_lease(
                task.task(),
                generation,
                lease,
                if cancellation.is_cancelled() {
                    LeaseAbortReason::Cancelled
                } else {
                    LeaseAbortReason::Retry
                },
            )?;
        }
        if provisional != 0 {
            stats.remove_provisional(provisional);
            record_discarded(discard, stats, provisional)?;
        }
        stats.set_active(0);
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::Directory;
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    async fn server(
        short_once: bool,
        extra: bool,
        pasv: bool,
        mut probe_failures: usize,
    ) -> (
        String,
        Arc<Mutex<Vec<u64>>>,
        Arc<AtomicU64>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let offsets = Arc::new(Mutex::new(Vec::new()));
        let recorded = offsets.clone();
        let short = Arc::new(AtomicBool::new(short_once));
        let connections = Arc::new(AtomicU64::new(0));
        let connected = connections.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut control, _) = listener.accept().await.unwrap();
                connected.fetch_add(1, Ordering::Relaxed);
                if probe_failures != 0 {
                    probe_failures -= 1;
                    continue;
                }
                let recorded = recorded.clone();
                let short = short.clone();
                tokio::spawn(async move {
                    control.write_all(b"220 ready\r\n").await.unwrap();
                    let mut control = BufReader::new(control);
                    let mut passive = None;
                    let mut offset = 0;
                    loop {
                        let mut line = String::new();
                        if control.read_line(&mut line).await.unwrap_or(0) == 0 {
                            break;
                        }
                        let (verb, arg) = line
                            .trim_end()
                            .split_once(' ')
                            .unwrap_or((line.trim_end(), ""));
                        let response = match verb {
                            "USER" => "331 password\r\n".into(),
                            "PASS" => "230 logged in\r\n".into(),
                            "TYPE" if arg == "I" => "200 binary\r\n".into(),
                            "SIZE" => "213 12\r\n".into(),
                            "MDTM" => "213 20260915000000\r\n".into(),
                            "EPSV" if pasv => "502 unsupported\r\n".into(),
                            "EPSV" | "PASV" => {
                                let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
                                let port = data.local_addr().unwrap().port();
                                passive = Some(data);
                                if verb == "EPSV" {
                                    format!("229 passive (|||{port}|)\r\n")
                                }
                                // This untrusted advertised address must be ignored.
                                else {
                                    format!(
                                        "227 passive (10,20,30,40,{},{})\r\n",
                                        port / 256,
                                        port % 256
                                    )
                                }
                            }
                            "REST" => {
                                offset = arg.parse::<u64>().unwrap();
                                "350 restart\r\n".into()
                            }
                            "RETR" => {
                                recorded.lock().unwrap().push(offset);
                                control
                                    .get_mut()
                                    .write_all(b"150 data follows\r\n")
                                    .await
                                    .unwrap();
                                let (mut stream, _) =
                                    passive.take().unwrap().accept().await.unwrap();
                                let end = if short.swap(false, Ordering::SeqCst) {
                                    5
                                } else {
                                    12
                                };
                                stream
                                    .write_all(&b"abcdefghijkl"[offset as usize..end])
                                    .await
                                    .unwrap();
                                if extra {
                                    stream.write_all(b"!").await.unwrap();
                                }
                                drop(stream);
                                "226 done\r\n".into()
                            }
                            _ => "500 unsupported\r\n".into(),
                        };
                        if control
                            .get_mut()
                            .write_all(response.as_bytes())
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            }
        });
        (
            format!("ftp://user:secret@{address}/file"),
            offsets,
            connections,
            server,
        )
    }

    #[tokio::test]
    async fn ftp_one_stream_checkpoints_resumes_and_pins_pasv_peer() {
        for (short, extra, pasv) in [
            (false, false, false),
            (true, false, false),
            (false, false, true),
            (false, true, false),
        ] {
            let directory = Directory::new();
            let (uri, offsets, _, server) = server(short, extra, pasv, 0).await;
            let task = TaskId::new(1).unwrap();
            let manifest = Arc::new(
                VerificationManifest::new(
                    12,
                    3,
                    b"abcdefghijkl"
                        .chunks(3)
                        .map(|bytes| {
                            let mut hash = ContentHasher::new(JournalDigestAlgorithm::Sha256);
                            hash.update(bytes);
                            hash.finalize().journal_digest()
                        })
                        .collect(),
                    vec![],
                )
                .unwrap(),
            );
            let spec = HttpTaskSpec::new(
                task,
                Gid::new(1).unwrap(),
                [uri],
                directory.0.clone(),
                ariax_storage::SafePathBuilder::from_user_path(
                    "result",
                    ariax_storage::PathPlatform::current(),
                )
                .unwrap(),
                Default::default(),
                false,
            )
            .unwrap()
            .with_verification(manifest, None)
            .unwrap();
            let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).unwrap());
            let client = HttpPolicyClient::new(
                crate::HttpResolver::new(Default::default()).unwrap(),
                crate::HttpPolicyClientConfig {
                    direct: crate::HttpDirectTransportConfig {
                        budgets: crate::HttpTransportBudgets::new(
                            8,
                            8 * crate::HTTP_CONNECTION_RESERVATION_BYTES,
                        )
                        .unwrap(),
                        ..Default::default()
                    },
                    destination: crate::HttpDestinationPolicy {
                        allow_loopback: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
            let worker = HttpMultiRangeWorker::new(
                client,
                HttpMultiRangeWorkerConfig {
                    journal_root: directory.0.join("journals"),
                    ..Default::default()
                },
                stats.clone(),
            )
            .unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                worker.run_task(Arc::new(spec), Generation::INITIAL, HttpCancellation::new()),
            )
            .await
            .unwrap();
            server.abort();
            if extra {
                assert!(
                    matches!(result, Err(HttpMultiRangeError::OversizedBody)),
                    "{result:?}"
                );
                assert_eq!(stats.get(task).unwrap().snapshot().durable_bytes, 9);
            } else {
                result.unwrap();
                assert_eq!(
                    std::fs::read(directory.0.join("result")).unwrap(),
                    b"abcdefghijkl"
                );
                assert_eq!(
                    *offsets.lock().unwrap(),
                    if short { vec![0, 3] } else { vec![0] }
                );
            }
        }
    }

    #[tokio::test]
    async fn ftp_probe_credit_reuse_and_exhaustion_survive_restart() {
        for (reuse, failures, wrong_checksum) in [
            (true, 1, false),
            (false, 1, false),
            (true, 100, false),
            (true, 0, true),
        ] {
            let directory = Directory::new();
            let (uri, offsets, connections, server) = server(false, false, false, failures).await;
            let mut options = crate::HttpTaskOptions::default();
            options.transfer.ftp_reuse_connection = reuse;
            if wrong_checksum {
                options.transfer.checksum = Some(crate::ContentChecksum::Md5([0; 16]));
            }
            options.retry = Some(HttpRetryPolicy {
                max_attempts: std::num::NonZeroU32::new(3).unwrap(),
                max_attempts_per_mirror: std::num::NonZeroU32::new(3).unwrap(),
                base_wait: Duration::ZERO,
                backoff: crate::HttpRetryBackoff::Fixed,
                ..Default::default()
            });
            let spec = Arc::new(
                HttpTaskSpec::new(
                    TaskId::new(1).unwrap(),
                    Gid::new(1).unwrap(),
                    [uri.clone()],
                    directory.0.clone(),
                    ariax_storage::SafePathBuilder::from_user_path(
                        "result",
                        ariax_storage::PathPlatform::current(),
                    )
                    .unwrap(),
                    options,
                    false,
                )
                .unwrap(),
            );
            let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).unwrap());
            let config = HttpMultiRangeWorkerConfig {
                journal_root: directory.0.join("journals"),
                ..Default::default()
            };
            let feedback = config.server_stats.clone();
            let client = HttpPolicyClient::new(
                crate::HttpResolver::new(Default::default()).unwrap(),
                crate::HttpPolicyClientConfig {
                    destination: crate::HttpDestinationPolicy {
                        allow_loopback: true,
                        ..Default::default()
                    },
                    direct: crate::HttpDirectTransportConfig {
                        budgets: crate::HttpTransportBudgets::new(
                            8,
                            8 * crate::HTTP_CONNECTION_RESERVATION_BYTES,
                        )
                        .unwrap(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
            let worker = HttpMultiRangeWorker::new(client, config, stats).unwrap();
            for _ in 0..if failures == 100 { 2 } else { 1 } {
                let result = tokio::time::timeout(
                    Duration::from_secs(3),
                    worker.run_task(spec.clone(), Generation::INITIAL, HttpCancellation::new()),
                )
                .await
                .unwrap();
                assert_eq!(
                    result.is_ok(),
                    failures != 100 && !wrong_checksum,
                    "{result:?}"
                );
            }
            assert_eq!(
                connections.load(Ordering::Relaxed),
                if failures == 100 {
                    3
                } else {
                    failures as u64 + if reuse { 1 } else { 2 }
                }
            );
            assert_eq!(offsets.lock().unwrap().len(), usize::from(failures != 100));
            if failures != 100 {
                let origin =
                    crate::server_stats::transfer_origin(spec.sources()[0].uri().unwrap()).unwrap();
                let observed = feedback.feedback(&origin);
                assert!(observed.samples > 0);
                assert_eq!(observed.failures > 0, wrong_checksum);
                if !wrong_checksum {
                    assert!(observed.bytes_per_second > 0);
                }
            }
            server.abort();
        }
    }
}

#[cfg(test)]
mod security_tests;
