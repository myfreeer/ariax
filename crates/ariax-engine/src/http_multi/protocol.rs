//! Shared fixed-layout verification and protocol dispatch inside the managed worker.
use super::*;
use crate::{ContentChecksum, ContentHasher, VerificationManifest};
#[cfg(feature = "ftp")]
mod ftp_transfer;
#[cfg(any(feature = "ftp", feature = "sftp"))]
mod retry;
#[cfg(feature = "sftp")]
pub(super) mod sftp_transfer;

#[derive(Clone)]
pub(super) enum PreparedValidator {
    Http(Arc<HttpRangeResponseValidator>),
    #[cfg(feature = "sftp")]
    Sftp {
        session: Arc<crate::sftp::SftpSession>,
        source: crate::HttpSourceSpec,
    },
}
impl fmt::Debug for PreparedValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedValidator")
            .field("source", &self.source())
            .field("length", &self.total_length())
            .finish_non_exhaustive()
    }
}
impl PreparedValidator {
    pub(super) fn http(&self) -> Option<&HttpRangeResponseValidator> {
        match self {
            Self::Http(validator) => Some(validator),
            #[cfg(feature = "sftp")]
            Self::Sftp { .. } => None,
        }
    }
    pub(super) fn source(&self) -> UriId {
        match self {
            Self::Http(validator) => validator.source(),
            #[cfg(feature = "sftp")]
            Self::Sftp { source, .. } => source.id(),
        }
    }
    pub(super) fn final_uri(&self) -> &str {
        match self {
            Self::Http(validator) => validator.final_uri(),
            #[cfg(feature = "sftp")]
            Self::Sftp { source, .. } => source.uri().expect("admitted source"),
        }
    }
    pub(super) fn total_length(&self) -> u64 {
        match self {
            Self::Http(validator) => validator.total_length(),
            #[cfg(feature = "sftp")]
            Self::Sftp { session, .. } => session.validator.total_length,
        }
    }
    pub(super) fn if_range(&self) -> Option<&[u8]> {
        self.http().and_then(HttpRangeResponseValidator::if_range)
    }
    pub(super) fn representation_digest(&self) -> Option<HttpRepresentationDigest> {
        self.http()
            .and_then(HttpRangeResponseValidator::representation_digest)
    }
    pub(super) fn resource_fingerprint(&self) -> ariax_storage::JournalHash {
        match self {
            Self::Http(validator) => validator.resource_fingerprint(),
            #[cfg(feature = "sftp")]
            Self::Sftp { session, .. } => session.validator.source,
        }
    }
    pub(super) fn strong_validator_fingerprint(&self) -> Option<ariax_storage::JournalHash> {
        self.http()
            .and_then(HttpRangeResponseValidator::strong_validator_fingerprint)
    }
    pub(super) fn validate_range(
        &self,
        uri: &str,
        status: hyper::StatusCode,
        headers: &hyper::HeaderMap,
        span: GlobalSpan,
    ) -> Result<Option<HttpRepresentationDigest>, HttpRangeResponseError> {
        self.http()
            .ok_or(HttpRangeResponseError::ResourceChanged)?
            .validate_range(uri, status, headers, span)
    }
}

struct OpenedProtocolStorage {
    storage: StorageEngine,
    layout_hash: ariax_storage::JournalHash,
    durable: Vec<PieceId>,
    retry_states: Vec<RecoveredRetryState>,
    #[cfg(any(feature = "ftp", feature = "sftp"))]
    previous_validators: BTreeMap<ariax_storage::JournalHash, crate::ProtocolValidator>,
}

impl HttpMultiRangeWorker {
    #[cfg(any(feature = "ftp", feature = "sftp"))]
    async fn acquire_storage_read(
        &self,
        storage: &StorageEngine,
        requested: usize,
        path: RatePath,
        cancellation: &HttpCancellation,
        stats: &HttpTransferStats,
    ) -> Result<(BufferLease, HttpIngressPermit, RatePermit), HttpMultiRangeError> {
        let _pressure = stats.local_wait();
        let deadline = tokio::time::Instant::now() + self.config.storage.shutdown_timeout;
        loop {
            if cancellation.is_cancelled() {
                return Err(HttpMultiRangeError::Cancelled);
            }
            if let Ok(buffer) = storage.reserve_network_buffer(requested) {
                if let Ok(ingress) = self.config.ingress_budget.try_acquire(buffer.capacity()) {
                    let requested =
                        NonZeroUsize::new(requested).ok_or(HttpMultiRangeError::Protocol)?;
                    let rate = tokio::select! { biased; _ = cancellation.cancelled() => return Err(HttpMultiRangeError::Cancelled),
                    rate = self.config.download_rate.acquire(path,requested) => rate.map_err(|_| HttpMultiRangeError::InvalidConfig)? };
                    return Ok((buffer, ingress, rate));
                }
                storage.discard_network_buffer(buffer)?;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(crate::ProtocolFailure::ResourceLimit.into());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    pub(super) async fn cpu<T: Send + 'static>(
        &self,
        bytes: usize,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, HttpMultiRangeError> {
        let pool = self
            .config
            .storage
            .cpu_pool
            .as_ref()
            .ok_or(HttpMultiRangeError::InvalidConfig)?;
        let deadline = tokio::time::Instant::now() + self.config.storage.shutdown_timeout;
        let reservation = loop {
            match pool.reserve(bytes) {
                Ok(reservation) => break reservation,
                Err(ariax_runtime::CpuError::Capacity)
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::task::yield_now().await
                }
                Err(_) => return Err(HttpMultiRangeError::InvalidConfig),
            }
        };
        reservation
            .spawn(work)
            .join()
            .await
            .map(|output| output.into_inner())
            .map_err(|_| HttpMultiRangeError::Protocol)
    }

    pub(super) async fn run_protocol_task(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        let stats = self
            .stats
            .get_or_create(task.task())
            .map_err(|_| HttpMultiRangeError::StatsCatalogFull)?;
        stats.begin();
        self.stats.clear_completion(task.task());
        let retry = task.options().retry.as_ref().unwrap_or(&self.config.retry);
        let limits = HttpDiscardBudgetLimits::for_http_task(
            self.config.discard_budget.process_limit(),
            task.options().piece_length,
            self.config.ingress_frame_bytes.get() as u64,
            retry.max_attempts.get(),
            retry.max_attempts_per_mirror.get(),
            0,
        )
        .clamp_to(self.config.discard_budget.configured_limits());
        let discard = self
            .config
            .discard_budget
            .begin_task(task.task(), limits.scope())
            .map_err(discard_setup_error)?;
        if task
            .sources()
            .iter()
            .any(|source| !source.protocol().is_http())
        {
            return self
                .run_other_protocols(task, generation, cancellation, stats, discard)
                .await;
        }
        let sources = self
            .probe_sources(&task, &cancellation, &stats, &discard, None)
            .await?;
        let total = sources
            .first()
            .ok_or(HttpMultiRangeError::NoUsableSources)?
            .validator
            .total_length();
        let manifest = self.transfer_manifest(&task, total)?;
        let worker = self.clone();
        let prepare_task = task.clone();
        let mut opened = self
            .cpu(128 * 1024, move || {
                worker.open_protocol_storage(&prepare_task, generation, manifest)
            })
            .await??;
        let outcome = async {
            let recovered = opened.storage.finish_verification().await?;
            for ack in recovered {
                if let WriteAck::PieceDurable { piece, .. } = ack {
                    opened.durable.push(piece);
                }
            }
            opened.storage.redownload_incomplete_recovery()?;
            stats.set_total_length(total);
            stats.set_durable(
                opened
                    .durable
                    .iter()
                    .map(|piece| {
                        (total - piece.get() * task.options().piece_length)
                            .min(task.options().piece_length)
                    })
                    .sum(),
            );
            let network = NetworkPhaseGuard::new(&stats);
            self.run_ranges(
                &task,
                generation,
                &cancellation,
                &stats,
                &sources,
                HttpMirrorIdentityContext {
                    policy: task.options().mirror_identity,
                    shared_whole_entity_digest: task
                        .verification()
                        .is_some_and(|manifest| manifest.proves_strict_identity())
                        || task.content_identity_strong(),
                    shared_range_digest: None,
                },
                &opened.durable,
                &opened.retry_states,
                &mut opened.storage,
                &discard,
            )
            .await?;
            drop(network);
            opened
                .storage
                .verify_whole_file()
                .await
                .map_err(HttpMultiRangeError::Storage)
        }
        .await;
        let outcome = account_checksum_outcome(&task, &discard, &stats, total, outcome);
        self.complete_storage_outcome(&task, opened.storage, opened.layout_hash, total, outcome)
            .await
    }

    fn transfer_manifest(
        &self,
        task: &HttpTaskSpec,
        total: u64,
    ) -> Result<Arc<VerificationManifest>, HttpMultiRangeError> {
        if let Some(manifest) = task.verification() {
            if manifest.total_length() != total {
                return Err(HttpMultiRangeError::SourceLengthMismatch);
            }
            return Ok(manifest.clone());
        }
        let whole = task
            .options()
            .content_checksum()
            .into_iter()
            .map(|checksum| checksum.journal_digest())
            .collect();
        Ok(Arc::new(
            VerificationManifest::new(total, task.options().piece_length, Vec::new(), whole)
                .map_err(|_| HttpMultiRangeError::InvalidConfig)?,
        ))
    }

    fn open_protocol_storage(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
        manifest: Arc<VerificationManifest>,
    ) -> Result<OpenedProtocolStorage, HttpMultiRangeError> {
        let OpenedTaskJournal {
            mut appender,
            state,
        } = self.open_task_journal(task, generation)?;
        appender = appender
            .manage(self.session.as_ref(), task.gid())
            .map_err(KnownLengthHttpError::from)?;
        let root = RootDirectoryCapability::open_trusted(task.output_root())
            .map_err(KnownLengthHttpError::from)?;
        if state
            .as_ref()
            .is_some_and(|state| state.terminal().is_some())
        {
            return Err(KnownLengthHttpError::AlreadyComplete.into());
        }
        let total = manifest.total_length();
        let mut durable = Vec::new();
        let mut contributors = Vec::new();
        #[cfg(any(feature = "ftp", feature = "sftp"))]
        let mut previous_validators = BTreeMap::new();
        let mut persisted = false;
        let retry_states = state
            .as_ref()
            .map(|state| state.retry_states().values().cloned().collect())
            .unwrap_or_default();
        let (layout, output) = if let Some(state) =
            state.as_ref().filter(|state| state.layout().is_some())
        {
            let old = state.layout().expect("checked layout").layout();
            if state.generation() != generation
                || old.files().len() != 1
                || old.files()[0].safe_path() != task.output()
                || old.total_length() != Some(total)
                || old.piece_length() != manifest.chunk_length()
                || root.identity().encode().as_ref() != old.root_binding().root_identity().bytes()
            {
                return Err(KnownLengthHttpError::RecoveryState.into());
            }
            let file = &old.files()[0];
            let output = root
                .open_existing_file(
                    file.safe_path(),
                    file.identity().ok_or(KnownLengthHttpError::RecoveryState)?,
                )
                .map_err(KnownLengthHttpError::from)?;
            if output.len().map_err(KnownLengthHttpError::from)? != total {
                return Err(KnownLengthHttpError::RecoveryState.into());
            }
            if state
                .verification_manifest()
                .is_some_and(|old| old.as_ref() != manifest.as_ref())
            {
                return Err(KnownLengthHttpError::RecoveryState.into());
            }
            persisted = state.verification_manifest().is_some();
            for (&piece, evidence) in state.durable_pieces() {
                let expected = evidence
                    .digest()
                    .ok_or(KnownLengthHttpError::RecoveryState)?;
                if manifest
                    .chunks()
                    .get(piece.get() as usize)
                    .is_some_and(|digest| digest != expected)
                {
                    return Err(HttpMultiRangeError::ChecksumMismatch);
                }
                let span = evidence.piece_span();
                let mut hash = ContentHasher::new(expected.algorithm());
                let mut bytes = [0; 64 * 1024];
                let mut offset = 0;
                while offset < span.len() {
                    let len = (span.len() - offset).min(bytes.len() as u64) as usize;
                    output
                        .read_exact_at(span.offset() + offset, &mut bytes[..len])
                        .map_err(KnownLengthHttpError::from)?;
                    hash.update(&bytes[..len]);
                    offset += len as u64;
                }
                if hash.finalize().journal_digest() != *expected {
                    return Err(HttpMultiRangeError::ChecksumMismatch);
                }
                durable.push(piece);
            }
            contributors.extend(state.committed_spans().values().copied());
            #[cfg(any(feature = "ftp", feature = "sftp"))]
            {
                previous_validators = state.protocol_validators().clone();
            }
            let layout = FileLayout::new(
                task.task(),
                generation,
                old.root_binding().clone(),
                old.files().to_vec(),
                Some(total),
                manifest.chunk_length(),
            )
            .map_err(KnownLengthHttpError::from)?;
            if old.generation() != generation {
                append_layout(&mut appender, &layout)?;
            }
            (layout, output)
        } else {
            if let Some(state) = &state {
                if state
                    .verification_manifest()
                    .is_some_and(|old| old.as_ref() != manifest.as_ref())
                {
                    return Err(KnownLengthHttpError::RecoveryState.into());
                }
                persisted = state.verification_manifest().is_some();
            }
            let output = root
                .create_new_file(task.output())
                .map_err(KnownLengthHttpError::from)?;
            output.set_len(total).map_err(KnownLengthHttpError::from)?;
            let layout = build_single_file_layout(
                task.task(),
                generation,
                &root,
                task.output(),
                &output,
                total,
                manifest.chunk_length(),
            )?;
            append_layout(&mut appender, &layout)?;
            (layout, output)
        };
        let layout_hash = ariax_storage::JournalHash::new(*layout.layout_hash().as_bytes())
            .ok_or(HttpMultiRangeError::Protocol)?;
        let mut storage = StorageEngine::open_with_journal(
            layout,
            [(FileId::new(0), output)],
            appender,
            self.config.storage.clone(),
        )?;
        storage.configure_verification(
            manifest,
            task.options().transfer.alignment,
            self.config.storage.buffer_pool_bytes / 4,
            persisted,
            &durable,
        )?;
        storage.restore_verified_contributors(contributors)?;
        if !task.options().transfer.realtime_checksum {
            storage.require_verification_readback();
        }
        Ok(OpenedProtocolStorage {
            storage,
            layout_hash,
            durable,
            retry_states,
            #[cfg(any(feature = "ftp", feature = "sftp"))]
            previous_validators,
        })
    }

    async fn run_other_protocols(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
        stats: HttpTransferStats,
        discard: HttpDiscardTaskGuard,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        let mut sources = Vec::new();
        let mut last = HttpMultiRangeError::NoUsableSources;
        if task
            .sources()
            .iter()
            .any(|source| source.protocol().is_http())
        {
            match self
                .probe_sources(&task, &cancellation, &stats, &discard, None)
                .await
            {
                Ok(http) => sources = http,
                Err(error) => last = error,
            }
        }
        #[cfg(feature = "sftp")]
        for source in task
            .sources()
            .iter()
            .filter(|source| source.protocol() == crate::TransferProtocol::Sftp)
        {
            match self
                .connect_sftp_with_retry(&task, source, generation, &cancellation, &stats)
                .await
            {
                Ok(session) => {
                    if sources.first().is_some_and(|first| {
                        first.validator.total_length() != session.validator.total_length
                    }) {
                        session.drain().await;
                        last = HttpMultiRangeError::SourceLengthMismatch;
                        continue;
                    }
                    let fingerprint = session.validator.fingerprint();
                    sources.push(PreparedSource {
                        validator: Arc::new(PreparedValidator::Sftp {
                            session: Arc::new(session),
                            source: source.clone(),
                        }),
                        lease_fingerprint: fingerprint,
                        ordinary_assignments: true,
                        range_digest_endgame: false,
                    });
                }
                Err(error @ HttpMultiRangeError::HostKeyChallenge(_)) => {
                    drain_sources(&sources).await;
                    return Err(error);
                }
                Err(error) => last = error,
            }
        }
        if cancellation.is_cancelled() {
            drain_sources(&sources).await;
            return Err(HttpMultiRangeError::Cancelled);
        }
        if !sources.is_empty() {
            if task.options().mirror_identity == HttpMirrorIdentityPolicy::RequireSharedDigest
                && !task.has_strict_content_identity()
                && sources.len() > 1
            {
                drain_sources(&sources[1..]).await;
                sources.truncate(1);
            }
            let total = sources[0].validator.total_length();
            let manifest = match self.transfer_manifest(&task, total) {
                Ok(manifest) => manifest,
                Err(error) => {
                    drain_sources(&sources).await;
                    return Err(error);
                }
            };
            let prepare = task.clone();
            let worker = self.clone();
            let opened = self
                .cpu(128 * 1024, move || {
                    worker.open_protocol_storage(&prepare, generation, manifest)
                })
                .await;
            let mut opened = match opened {
                Ok(Ok(opened)) => opened,
                Ok(Err(error)) | Err(error) => {
                    drain_sources(&sources).await;
                    return Err(error);
                }
            };
            let outcome = async {
                opened.storage.finish_verification().await?;
                opened.storage.redownload_incomplete_recovery()?;
                stats.set_total_length(total);
                let pieces = opened.storage.verified_pieces();
                stats.set_durable(
                    pieces
                        .iter()
                        .map(|piece| {
                            (total - piece.get() * task.options().piece_length)
                                .min(task.options().piece_length)
                        })
                        .sum(),
                );
                #[cfg(feature = "sftp")]
                for source in &sources {
                    if let PreparedValidator::Sftp { session, .. } = source.validator.as_ref() {
                        if !pieces.is_empty()
                            && !task.has_strict_content_identity()
                            && !opened
                                .previous_validators
                                .get(&session.validator.source)
                                .is_some_and(|old| {
                                    session.validator.permits_resume(
                                        old,
                                        task.options().content_checksum().is_some(),
                                    )
                                })
                        {
                            return Err(crate::ProtocolFailure::StaleValidator.into());
                        }
                        opened
                            .storage
                            .record_protocol_validator(session.validator.clone())?;
                    }
                }
                let network = NetworkPhaseGuard::new(&stats);
                let ranges = self
                    .run_ranges(
                        &task,
                        generation,
                        &cancellation,
                        &stats,
                        &sources,
                        mirror_identity_context(&task, &sources),
                        &pieces,
                        &opened.retry_states,
                        &mut opened.storage,
                        &discard,
                    )
                    .await;
                drain_sources(&sources).await;
                if let Err(error) = ranges {
                    if cancellation.is_cancelled() {
                        return Err(HttpMultiRangeError::Cancelled);
                    }
                    #[cfg(feature = "ftp")]
                    {
                        let ftp = task
                            .sources()
                            .iter()
                            .filter(|source| {
                                matches!(
                                    source.protocol(),
                                    crate::TransferProtocol::Ftp | crate::TransferProtocol::Ftps
                                )
                            })
                            .collect::<Vec<_>>();
                        if !ftp.is_empty() {
                            opened.storage.redownload_incomplete_recovery()?;
                            return self
                                .run_ftp_sources(
                                    &task,
                                    generation,
                                    &cancellation,
                                    &stats,
                                    &discard,
                                    &ftp,
                                    0,
                                    None,
                                    &mut opened,
                                )
                                .await;
                        } else {
                            return Err(error);
                        }
                    }
                    #[cfg(not(feature = "ftp"))]
                    return Err(error);
                }
                drop(network);
                opened
                    .storage
                    .verify_whole_file()
                    .await
                    .map_err(HttpMultiRangeError::Storage)
            }
            .await;
            drain_sources(&sources).await;
            let outcome = account_checksum_outcome(&task, &discard, &stats, total, outcome);
            return self
                .complete_storage_outcome(&task, opened.storage, opened.layout_hash, total, outcome)
                .await;
        }
        #[cfg(feature = "ftp")]
        if task.sources().iter().any(|source| {
            matches!(
                source.protocol(),
                crate::TransferProtocol::Ftp | crate::TransferProtocol::Ftps
            )
        }) {
            return self
                .run_ftp_task(task, generation, cancellation, stats, discard)
                .await;
        }
        let _ = (task, generation, cancellation, stats, discard);
        Err(last)
    }
}

async fn drain_sources(sources: &[PreparedSource]) {
    #[cfg(feature = "sftp")]
    for source in sources {
        if let PreparedValidator::Sftp { session, .. } = source.validator.as_ref() {
            session.drain().await;
        }
    }
    #[cfg(not(feature = "sftp"))]
    let _ = sources;
}

impl HttpTaskSpec {
    fn content_identity_strong(&self) -> bool {
        self.options().content_checksum().is_some_and(|checksum| {
            matches!(
                checksum,
                ContentChecksum::Sha256(_) | ContentChecksum::Sha512(_)
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    pub(super) struct Directory(pub(super) PathBuf);
    impl Directory {
        pub(super) fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "ariax-protocol-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    pub(super) async fn server(corrupt: bool) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    while !bytes.ends_with(b"\r\n\r\n") && bytes.len() < 16384 {
                        let byte = stream.read_u8().await.unwrap();
                        bytes.push(byte);
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    let range = request
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("range: bytes=")
                                .or_else(|| line.strip_prefix("Range: bytes="))
                        })
                        .unwrap();
                    let (start, end) = range.split_once('-').unwrap();
                    let start = start.parse::<usize>().unwrap();
                    let end = end.parse::<usize>().unwrap();
                    let mut body = b"abcdefghijkl"[start..=end].to_vec();
                    if corrupt && body.len() > 1 {
                        body[0] ^= 1;
                    }
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/12\r\nETag: \"stable\"\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(head.as_bytes()).await.unwrap();
                    stream.write_all(&body).await.unwrap();
                });
            }
        });
        (format!("http://{address}/file"), handle)
    }
    #[tokio::test]
    async fn manifest_http_streams_sha512_chunks_and_rejects_mismatch() {
        for corrupt in [false, true] {
            let directory = Directory::new();
            let (uri, server) = server(corrupt).await;
            let task = TaskId::new(1).unwrap();
            let gid = Gid::new(1).unwrap();
            let manifest = Arc::new(
                VerificationManifest::new(
                    12,
                    3,
                    b"abcdefghijkl"
                        .chunks(3)
                        .map(|bytes| {
                            let mut hash = ContentHasher::new(JournalDigestAlgorithm::Sha512);
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
                gid,
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
            .with_verification(manifest, Some(1))
            .unwrap();
            let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).unwrap());
            let client = HttpPolicyClient::new(
                crate::HttpResolver::new(Default::default()).unwrap(),
                crate::HttpPolicyClientConfig {
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
            let result = worker
                .run_task(Arc::new(spec), Generation::INITIAL, HttpCancellation::new())
                .await;
            server.abort();
            if corrupt {
                assert!(
                    matches!(result,Err(HttpMultiRangeError::Storage(ref error)) if error.reject() == WriteReject::ChecksumMismatch),
                    "{result:?}"
                );
            } else {
                result.unwrap();
                assert_eq!(
                    std::fs::read(directory.0.join("result")).unwrap(),
                    b"abcdefghijkl"
                );
                assert_eq!(stats.get(task).unwrap().snapshot().durable_bytes, 12);
            }
        }
    }
}
