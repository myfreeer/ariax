use super::*;
use crate::storage_journal::JournalWrite;
use ariax_storage::{JournalHash, JournalPayload, MetalinkExpansion, MetalinkParent};
use base64ct::Encoding;
use serde_json::{Value, json};

pub(super) fn is_metalink_type(headers: &hyper::HeaderMap) -> bool {
    let mut values = headers.get_all(hyper::header::CONTENT_TYPE).iter();
    let value = values.next().and_then(|value| value.to_str().ok());
    values.next().is_none()
        && value.is_some_and(|value| {
            matches!(
                value
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_ascii_lowercase()
                    .as_str(),
                "application/metalink4+xml" | "application/metalink+xml"
            )
        })
}
impl HttpMultiRangeWorker {
    pub(super) async fn follow_metalink(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        cancellation: HttpCancellation,
        uri: String,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        if !cfg!(feature = "metalink")
            || task.options().transfer.follow_metalink == crate::FollowMetalink::Never
        {
            return Err(crate::ProtocolFailure::Malformed.into());
        }
        let stats = self
            .stats
            .get_or_create(task.task())
            .map_err(|_| HttpMultiRangeError::StatsCatalogFull)?;
        let mut response = tokio::select! {biased;_=cancellation.cancelled()=>return Err(HttpMultiRangeError::Cancelled),result=self.client.execute(HttpClientRequest::get(uri))=>result.map_err(HttpMultiRangeError::Client)?};
        if response.status() != hyper::StatusCode::OK || !is_metalink_type(response.headers()) {
            return Err(crate::ProtocolFailure::Malformed.into());
        }
        let head = response.headers();
        if head.contains_key(hyper::header::CONTENT_ENCODING)
            && head
                .get(hyper::header::CONTENT_ENCODING)
                .is_none_or(|value| value != "identity")
        {
            return Err(crate::ProtocolFailure::Malformed.into());
        }
        let mut lengths = head.get_all(hyper::header::CONTENT_LENGTH).iter();
        let length = lengths
            .next()
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|text| text.parse::<usize>().ok())
                    .ok_or(crate::ProtocolFailure::Malformed)
            })
            .transpose()?;
        if lengths.next().is_some() {
            return Err(crate::ProtocolFailure::Malformed.into());
        }
        let cap = length.unwrap_or_else(|| {
            (self
                .config
                .protocol_metadata
                .limit()
                .saturating_sub(64 * 1024)
                / 4)
            .min(64 * 1024 * 1024)
        });
        if cap == 0 || cap > 64 * 1024 * 1024 {
            return Err(crate::ProtocolFailure::ResourceLimit.into());
        }
        // Includes the byte input, base64 handoff, JSON framing and parser result.
        let metadata = self
            .config
            .protocol_metadata
            .try_acquire(cap.saturating_mul(3).saturating_add(64 * 1024))
            .map_err(|_| crate::ProtocolFailure::ResourceLimit)?;
        let base = response.final_uri().to_owned();
        let origin = discard_host_key(&base)?;
        let task_discard = self
            .config
            .discard_budget
            .begin_task(
                task.task(),
                self.config.discard_budget.configured_limits().scope(),
            )
            .map_err(discard_setup_error)?;
        let attempt_discard = task_discard
            .begin_attempt(origin.clone())
            .map_err(discard_setup_error)?;
        let mut accounting = MetadataAccounting {
            stats: stats.clone(),
            discard: attempt_discard,
            bytes: 0,
            accepted: false,
        };
        let hash = Sha256::digest(origin.as_bytes());
        let host = u64::from_le_bytes(hash[..8].try_into().expect("hash prefix"));
        let path = RatePath {
            host,
            task: task.task().get(),
            stream: u64::MAX,
        };
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(cap)
            .map_err(|_| crate::ProtocolFailure::ResourceLimit)?;
        loop {
            let requested = NonZeroUsize::new(
                self.config
                    .ingress_frame_bytes
                    .get()
                    .min(cap.saturating_sub(bytes.len()).saturating_add(1)),
            )
            .ok_or(HttpMultiRangeError::Protocol)?;
            let rate = tokio::select! {biased;_=cancellation.cancelled()=>return Err(HttpMultiRangeError::Cancelled),result=self.config.download_rate.acquire(path,requested)=>result.map_err(|_|HttpMultiRangeError::Protocol)?};
            let ingress = self
                .config
                .ingress_budget
                .try_acquire(self.config.ingress_frame_bytes.get())
                .map_err(|_| crate::ProtocolFailure::ResourceLimit)?;
            let data = tokio::select! {biased;_=cancellation.cancelled()=>return Err(HttpMultiRangeError::Cancelled),result=response.next_data(task.options().response_body_timeout)=>result.map_err(HttpMultiRangeError::Client)?};
            let count = data.as_ref().map_or(0, bytes::Bytes::len);
            accounting.bytes = accounting.bytes.saturating_add(count);
            stats.add_raw(count);
            stats.set_rate_debt(rate.settle(count).debt_bytes);
            let Some(data) = data else {
                break;
            };
            if bytes.len().saturating_add(data.len()) > cap {
                return Err(crate::ProtocolFailure::ResourceLimit.into());
            }
            bytes.extend_from_slice(&data);
            drop(ingress);
        }
        response.finish().await;
        if length.is_some_and(|length| length != bytes.len()) {
            return Err(HttpMultiRangeError::ShortBody);
        }
        let retained = task.options().transfer.follow_metalink == crate::FollowMetalink::Follow;
        let input_task = task.clone();
        let worker = self.clone();
        let (parent, params) = self
            .cpu(256 * 1024, move || {
                let document_hash = JournalHash::new(Sha256::digest(&bytes).into())
                    .ok_or(HttpMultiRangeError::Protocol)?;
                if let Some(expected) = input_task.options().content_checksum() {
                    let mut hash = crate::ContentHasher::new(expected.algorithm());
                    hash.update(&bytes);
                    if hash.finalize() != expected {
                        return Err(HttpMultiRangeError::ChecksumMismatch);
                    }
                }
                let snapshot = input_task
                    .persistence_options()
                    .map_err(|_| HttpMultiRangeError::Protocol)?;
                let parent = MetalinkParent {
                    gid: input_task.gid(),
                    generation,
                    snapshot_hash: snapshot.snapshot_hash(),
                    document_hash,
                    document_bytes: bytes.len() as u64,
                    retained,
                };
                if retained {
                    worker.save_metalink(&input_task, generation, &bytes)?;
                }
                let mut options = snapshot
                    .entries()
                    .filter(|(name, _)| {
                        !matches!(
                            *name,
                            "out"
                                | "checksum"
                                | "verification-manifest"
                                | "metalink-file-index"
                                | "metalink-expansion"
                        )
                    })
                    .map(|(name, value)| (name.to_owned(), Value::String(value.to_owned())))
                    .collect::<serde_json::Map<_, _>>();
                options.insert("metalink-base-uri".into(), Value::String(base));
                options.insert("follow-metalink".into(), Value::String("false".into()));
                Ok::<_, HttpMultiRangeError>((
                    parent,
                    json!([base64ct::Base64::encode_string(&bytes), options]),
                ))
            })
            .await??;
        let (reply, wait) = oneshot::channel();
        self.config
            .metalink_follow
            .push(crate::metalink_follow::FollowRequest {
                parent,
                params,
                reply,
                _metadata: metadata,
            })
            .map_err(|_| crate::ProtocolFailure::ResourceLimit)?;
        // The control owner owns an accepted batch even if this worker is cancelled.
        let expansion = tokio::select! {biased;_=cancellation.cancelled()=>return Err(HttpMultiRangeError::Cancelled),result=wait=>result.map_err(|_|HttpMultiRangeError::Protocol)?.map_err(|_|crate::ProtocolFailure::Malformed)?};
        accounting.accepted = true;
        stats.add_accepted(accounting.bytes);
        self.finish_metalink_parent(task, generation, expansion)
            .await
    }

    fn metadata_file_handles(
        &self,
    ) -> Result<Vec<ariax_runtime::HandlePermit>, HttpMultiRangeError> {
        match &self.config.storage.handle_budgets {
            Some(budgets) => (0..3)
                .map(|_| {
                    budgets
                        .try_acquire_file()
                        .map_err(|_| crate::ProtocolFailure::ResourceLimit.into())
                })
                .collect(),
            None => Ok(Vec::new()),
        }
    }

    fn save_metalink(
        &self,
        task: &HttpTaskSpec,
        generation: Generation,
        bytes: &[u8],
    ) -> Result<(), HttpMultiRangeError> {
        use std::io::{Seek, Write};
        let OpenedTaskJournal { appender, state } = self.open_task_journal(task, generation)?;
        let mut appender = appender
            .manage(self.session.as_ref(), task.gid())
            .map_err(KnownLengthHttpError::from)?;
        let _handles = self.metadata_file_handles()?;
        let root = RootDirectoryCapability::open_trusted(task.output_root())
            .map_err(KnownLengthHttpError::from)?;
        let output = if let Some(layout) = state.as_ref().and_then(|state| state.layout()) {
            let file = layout
                .layout()
                .files()
                .first()
                .ok_or(HttpMultiRangeError::Protocol)?;
            if layout.layout().files().len() != 1
                || file.safe_path() != task.output()
                || file.length() != bytes.len() as u64
            {
                return Err(crate::ProtocolFailure::StaleValidator.into());
            }
            root.open_existing_file(
                file.safe_path(),
                file.identity().ok_or(HttpMultiRangeError::Protocol)?,
            )
            .map_err(KnownLengthHttpError::from)?
        } else {
            let file = root
                .create_new_file(task.output())
                .map_err(KnownLengthHttpError::from)?;
            file.set_len(bytes.len() as u64)
                .map_err(KnownLengthHttpError::from)?;
            let layout = build_single_file_layout(
                task.task(),
                generation,
                &root,
                task.output(),
                &file,
                bytes.len() as u64,
                task.options().piece_length,
            )?;
            append_layout(&mut appender, &layout)?;
            file
        };
        let mut file = output
            .try_clone_file()
            .map_err(KnownLengthHttpError::from)?;
        file.rewind().map_err(|_| crate::ProtocolFailure::Data)?;
        file.write_all(bytes)
            .map_err(|_| crate::ProtocolFailure::Data)?;
        output.sync_all().map_err(KnownLengthHttpError::from)?;
        appender
            .close_flushed()
            .map_err(KnownLengthHttpError::from)?;
        Ok(())
    }

    pub(super) async fn finish_metalink_parent(
        &self,
        task: Arc<HttpTaskSpec>,
        generation: Generation,
        expansion: MetalinkExpansion,
    ) -> Result<HttpWorkerSuccess, HttpMultiRangeError> {
        if expansion.parent.gid != task.gid() {
            return Err(HttpMultiRangeError::Protocol);
        }
        let worker = self.clone();
        let input = task.clone();
        let evidence = self
            .cpu(128 * 1024, move || {
                let OpenedTaskJournal { appender, state } =
                    worker.open_task_journal(&input, generation)?;
                let mut appender = appender
                    .manage(worker.session.as_ref(), input.gid())
                    .map_err(KnownLengthHttpError::from)?;
                let layout_hash = if expansion.parent.retained {
                    state
                        .as_ref()
                        .and_then(|state| state.layout())
                        .ok_or(HttpMultiRangeError::Protocol)?
                        .layout_hash()
                } else {
                    expansion.parent.document_hash
                };
                if expansion.parent.retained {
                    use std::io::Read;
                    let _handles = worker.metadata_file_handles()?;
                    let root = RootDirectoryCapability::open_trusted(input.output_root())
                        .map_err(KnownLengthHttpError::from)?;
                    let layout = state
                        .as_ref()
                        .and_then(|state| state.layout())
                        .ok_or(HttpMultiRangeError::Protocol)?
                        .layout();
                    let entry = layout
                        .files()
                        .first()
                        .ok_or(HttpMultiRangeError::Protocol)?;
                    let capability = root
                        .open_existing_file(
                            entry.safe_path(),
                            entry.identity().ok_or(HttpMultiRangeError::Protocol)?,
                        )
                        .map_err(KnownLengthHttpError::from)?;
                    if capability.len().map_err(KnownLengthHttpError::from)?
                        != expansion.parent.document_bytes
                    {
                        return Err(HttpMultiRangeError::ChecksumMismatch);
                    }
                    let mut file = capability
                        .try_clone_file()
                        .map_err(KnownLengthHttpError::from)?;
                    let mut buffer = [0; 64 * 1024];
                    let mut hash = Sha256::new();
                    loop {
                        let count = file
                            .read(&mut buffer)
                            .map_err(|_| crate::ProtocolFailure::Data)?;
                        if count == 0 {
                            break;
                        }
                        hash.update(&buffer[..count]);
                    }
                    if hash.finalize().as_slice() != expansion.parent.document_hash.as_bytes() {
                        return Err(HttpMultiRangeError::ChecksumMismatch);
                    }
                }
                let total_length = if expansion.parent.retained {
                    expansion.parent.document_bytes
                } else {
                    0
                };
                let completed_at_unix_ms = now_unix_ms().unwrap_or(0);
                let record = appender
                    .append_payload(
                        generation,
                        &JournalPayload::MetadataComplete {
                            expansion,
                            completed_at_unix_ms,
                        },
                    )
                    .map_err(KnownLengthHttpError::from)?;
                appender
                    .flush(record.sequence())
                    .map_err(KnownLengthHttpError::from)?;
                appender
                    .close_flushed()
                    .map_err(KnownLengthHttpError::from)?;
                Ok::<_, HttpMultiRangeError>(HttpCompletedEvidence {
                    layout_hash,
                    total_length,
                    completed_at_unix_ms,
                    terminal_sequence: record.sequence(),
                })
            })
            .await??;
        let stats = self
            .stats
            .get_or_create(task.task())
            .map_err(|_| HttpMultiRangeError::StatsCatalogFull)?;
        stats.set_total_length(evidence.total_length);
        stats.set_durable(evidence.total_length);
        self.stats.record_completion(task.task(), evidence);
        Ok(HttpWorkerSuccess::default())
    }
}

struct MetadataAccounting {
    stats: HttpTransferStats,
    discard: HttpDiscardAttemptGuard,
    bytes: usize,
    accepted: bool,
}
impl Drop for MetadataAccounting {
    fn drop(&mut self) {
        if !self.accepted {
            let _ = record_discarded(&self.discard, &self.stats, self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn retained_metadata_revalidates_content_identity_and_handle_capacity() {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        for case in 0..5 {
            let directory = std::env::temp_dir().join(format!(
                "ariax-metadata-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&directory).unwrap();
            let handles = ariax_runtime::HandleBudgets::new(ariax_runtime::HandleBudgetLimits {
                process: 4,
                sockets: 1,
                files: if case == 4 { 2 } else { 3 },
            })
            .unwrap();
            let mut config = HttpMultiRangeWorkerConfig {
                journal_root: directory.join("journals"),
                ..Default::default()
            };
            config.storage.handle_budgets = Some(handles.clone());
            let client = HttpPolicyClient::new(
                crate::HttpResolver::new(Default::default()).unwrap(),
                Default::default(),
            );
            let worker = HttpMultiRangeWorker::new(
                client,
                config,
                SharedHttpTransferStats::new(NonZeroUsize::new(2).unwrap()),
            )
            .unwrap();
            let spec = Arc::new(
                HttpTaskSpec::new(
                    TaskId::new(1).unwrap(),
                    Gid::new(1).unwrap(),
                    ["https://example.test/metadata".into()],
                    directory.clone(),
                    ariax_storage::SafePathBuilder::from_user_path(
                        "metadata",
                        ariax_storage::PathPlatform::current(),
                    )
                    .unwrap(),
                    Default::default(),
                    false,
                )
                .unwrap(),
            );
            let bytes = b"<metadata/>";
            let saved = worker.save_metalink(&spec, Generation::INITIAL, bytes);
            if case == 4 {
                assert!(saved.is_err());
                assert!(!directory.join("metadata").exists());
            } else {
                saved.unwrap();
                match case {
                    1 => std::fs::write(directory.join("metadata"), b"<tampered/>").unwrap(),
                    2 => std::fs::write(directory.join("metadata"), b"short").unwrap(),
                    3 => {
                        std::fs::rename(directory.join("metadata"), directory.join("old")).unwrap();
                        std::fs::write(directory.join("metadata"), bytes).unwrap();
                    }
                    _ => (),
                }
                let expansion = MetalinkExpansion {
                    parent: MetalinkParent {
                        gid: spec.gid(),
                        generation: Generation::INITIAL,
                        snapshot_hash: spec.persistence_options().unwrap().snapshot_hash(),
                        document_hash: JournalHash::new(Sha256::digest(bytes).into()).unwrap(),
                        document_bytes: bytes.len() as u64,
                        retained: true,
                    },
                    children: vec![Gid::new(2).unwrap()],
                };
                let result = worker
                    .finish_metalink_parent(spec, Generation::INITIAL, expansion)
                    .await;
                assert_eq!(result.is_ok(), case == 0, "case {case}: {result:?}");
            }
            assert_eq!(handles.available_files(), handles.limits().files);
            drop(worker);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
