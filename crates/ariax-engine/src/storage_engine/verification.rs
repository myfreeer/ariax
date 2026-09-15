use super::*;
use crate::{
    ChunkAlignment, ChunkHashCoordinator, ChunkHashError, ContentHasher, VerificationManifest,
};
use ariax_runtime::{CpuError, CpuPool, CpuPoolConfig, CpuReservation};

struct VerificationFile {
    capability: RootFileCapability,
    _permit: Option<HandlePermit>,
}
impl std::ops::Deref for VerificationFile {
    type Target = RootFileCapability;
    fn deref(&self) -> &Self::Target {
        &self.capability
    }
}

pub(super) fn hash_error(error: ChunkHashError) -> StorageEngineError {
    StorageEngineError::bare(if error == ChunkHashError::ChecksumMismatch {
        WriteReject::ChecksumMismatch
    } else {
        WriteReject::Verification
    })
}

impl StorageEngine {
    pub(crate) fn has_verification(&self) -> bool {
        self.verification.is_some()
    }
    pub(crate) fn verified_pieces(&self) -> Vec<PieceId> {
        self.verification
            .as_ref()
            .map_or_else(Vec::new, |verification| {
                verification.durable_pieces().collect()
            })
    }
    #[cfg(feature = "ftp")]
    pub(crate) fn invalidate_verified_piece(
        &mut self,
        piece: PieceId,
    ) -> Result<(), StorageEngineError> {
        if !self.active.is_empty() {
            return Err(StorageEngineError::bare(WriteReject::Verification));
        }
        let piece_span = self.piece_span(piece)?;
        let appended = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::PieceFailed {
                    lease_id: None,
                    piece_id: piece,
                    piece_span,
                    error_class: ErrorKind::StaleValidator,
                    attempt: 1,
                },
            )
            .map_err(journal_error)?;
        self.journal
            .flush(appended.sequence())
            .map_err(journal_error)?;
        self.verification
            .as_mut()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Verification))?
            .invalidate(piece)
            .map_err(hash_error)
    }
    pub(crate) fn next_lease_floor(&self) -> u64 {
        self.seen_leases
            .last()
            .map_or(0, |id| id.get())
            .max(self.journal.last_sequence())
            .saturating_add(1)
    }
    pub(crate) fn require_verification_readback(&mut self) {
        if let Some(verification) = self.verification.as_mut() {
            verification.force_readback();
        }
    }
    /// Called only before network admission. Complete recovered contributor
    /// sets were read back first; a remaining gap redownloads the entire chunk.
    pub(crate) fn redownload_incomplete_recovery(&mut self) -> Result<(), StorageEngineError> {
        let Some(verification) = self.verification.as_ref() else {
            return Ok(());
        };
        for (piece_id, piece_span) in verification.pending_chunks() {
            let appended = self
                .journal
                .append_payload(
                    self.generation,
                    &JournalPayload::PieceFailed {
                        lease_id: None,
                        piece_id,
                        piece_span,
                        error_class: ErrorKind::Cancelled,
                        attempt: 1,
                    },
                )
                .map_err(journal_error)?;
            self.journal
                .flush(appended.sequence())
                .map_err(journal_error)?;
            self.verification
                .as_mut()
                .expect("manifest installed")
                .invalidate(piece_id)
                .map_err(hash_error)?;
        }
        Ok(())
    }
    fn verification_file(&self, id: FileId) -> Result<VerificationFile, StorageEngineError> {
        let permit = self
            .handle_budgets
            .as_ref()
            .map(|budget| budget.try_acquire_file())
            .transpose()
            .map_err(|error| {
                StorageEngineError::with(
                    WriteReject::NativeFile,
                    StorageEngineErrorDetail::Handle(error),
                )
            })?;
        let capability = self
            .files
            .get(&id)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::NativeFile))?
            .capability
            .try_clone_capability()
            .map_err(|error| {
                StorageEngineError::with(
                    WriteReject::NativeFile,
                    StorageEngineErrorDetail::Native(error),
                )
            })?;
        Ok(VerificationFile {
            capability,
            _permit: permit,
        })
    }
    pub(crate) fn restore_verified_contributors(
        &mut self,
        contributors: impl IntoIterator<Item = JournalContributor>,
    ) -> Result<(), StorageEngineError> {
        let coordinator = self
            .verification
            .as_mut()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Verification))?;
        for contributor in contributors {
            self.seen_leases.insert(contributor.lease_id());
            coordinator
                .restore_committed(contributor)
                .map_err(hash_error)?;
        }
        Ok(())
    }
    pub fn install_verification(
        &mut self,
        manifest: Arc<VerificationManifest>,
        alignment: ChunkAlignment,
        reorder_bytes: usize,
    ) -> Result<(), StorageEngineError> {
        self.configure_verification(manifest, alignment, reorder_bytes, false, &[])
    }

    pub(crate) fn configure_verification(
        &mut self,
        manifest: Arc<VerificationManifest>,
        alignment: ChunkAlignment,
        reorder_bytes: usize,
        persisted: bool,
        durable: &[PieceId],
    ) -> Result<(), StorageEngineError> {
        if self.verification.is_some()
            || !self.seen_leases.is_empty()
            || self.layout.total_length() != Some(manifest.total_length())
            || self.layout.piece_length() != manifest.chunk_length()
        {
            return Err(StorageEngineError::bare(WriteReject::Verification));
        }
        if self.cpu_pool.is_none() {
            self.cpu_pool = Some(
                CpuPool::new(CpuPoolConfig {
                    workers: 1,
                    jobs: 16,
                    bytes: (1024 * 1024).min(self.resident_budget.limit()),
                    resident: self.resident_budget.clone(),
                    shared_disk: false,
                })
                .map_err(|_| StorageEngineError::bare(WriteReject::Verification))?,
            );
        }
        if !persisted {
            for payload in manifest.journal_payloads() {
                let appended = self
                    .journal
                    .append_payload(self.generation, &payload)
                    .map_err(journal_error)?;
                self.journal
                    .flush(appended.sequence())
                    .map_err(journal_error)?;
            }
        }
        let mut coordinator = ChunkHashCoordinator::new(
            manifest,
            alignment,
            reorder_bytes.min(self.resident_budget.limit()),
        );
        for &piece in durable {
            coordinator.mark_durable(piece);
        }
        self.verification = Some(coordinator);
        Ok(())
    }

    pub(crate) async fn reserve_cpu(
        &self,
        bytes: usize,
    ) -> Result<CpuReservation, StorageEngineError> {
        let pool = self
            .cpu_pool
            .as_ref()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Verification))?;
        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        loop {
            match pool.reserve(bytes) {
                Ok(reservation) => return Ok(reservation),
                Err(CpuError::Capacity) if tokio::time::Instant::now() < deadline => {
                    tokio::task::yield_now().await
                }
                Err(_) => return Err(StorageEngineError::bare(WriteReject::Verification)),
            }
        }
    }

    pub(super) async fn feed_verification(
        &mut self,
        lease: LeaseId,
        offset: u64,
        buffer: BufferLease,
    ) -> Result<(), StorageEngineError> {
        let reservation = match self.reserve_cpu(1024).await {
            Ok(reservation) => reservation,
            Err(error) => {
                self.release_buffer(buffer)?;
                return Err(error);
            }
        };
        let mut coordinator = self
            .verification
            .take()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Verification))?;
        let pool = self.pool.clone();
        let output = reservation
            .spawn(move || {
                let result = coordinator.feed(lease, offset, buffer, pool);
                (coordinator, result)
            })
            .join()
            .await
            .map_err(|_| StorageEngineError::bare(WriteReject::Verification))?;
        let (coordinator, result) = output.into_inner();
        self.verification = Some(coordinator);
        result.map_err(hash_error)
    }

    pub(super) fn commit_verified_lease(
        &mut self,
        commit: LeaseCommit,
    ) -> Result<Vec<WriteAck>, StorageEngineError> {
        let active = self
            .active
            .get(&commit.lease)
            .ok_or_else(|| StorageEngineError::bare(WriteReject::UnknownLease))?;
        self.validate_commit(active, &commit)?;
        let active = self
            .active
            .remove(&commit.lease)
            .expect("validated active lease");
        let span = PersistedSpan::new(active.plan.span.offset, active.plan.span.len as u64)
            .map_err(|_| StorageEngineError::bare(WriteReject::Mapping))?;
        let last = (span.offset() + span.len() - 1) / self.layout.piece_length();
        for index in active.piece.get()..=last {
            let piece = PieceId::new(index);
            let chunk = self.piece_span(piece)?;
            let start = span.offset().max(chunk.offset());
            let end = (span.offset() + span.len()).min(chunk.offset() + chunk.len());
            self.journal
                .append_payload(
                    self.generation,
                    &JournalPayload::PieceWritten {
                        lease_id: commit.lease,
                        piece_id: piece,
                        written_span: PersistedSpan::new(start, end - start)
                            .map_err(|_| StorageEngineError::bare(WriteReject::Mapping))?,
                    },
                )
                .map_err(journal_error)?;
        }
        let committed = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::LeaseCommitted {
                    lease_id: commit.lease,
                    span,
                    validator_fingerprint: commit.validator,
                    response_digest: commit.response_digest,
                },
            )
            .map_err(journal_error)?;
        // A committed contributor is recovery evidence, but does not publish a durable chunk.
        self.journal
            .flush(committed.sequence())
            .map_err(journal_error)?;
        #[cfg(test)]
        self.crash_at(StorageEngineCrashPoint::LeaseCommitted);
        let coordinator = self.verification.as_mut().expect("manifest installed");
        if coordinator.contains_lease(commit.lease) {
            coordinator.commit(commit.lease).map_err(hash_error)?;
        } else {
            coordinator
                .commit_written_after_fence(commit.lease, span, commit.validator)
                .map_err(hash_error)?;
        }
        Ok(vec![WriteAck::LeaseCommitted {
            lease: commit.lease,
            span: active.plan.span,
        }])
    }

    /// Drains ready verification chunks. The streaming path hashes no disk
    /// bytes; the bounded fallback reads only the exact fully committed chunk.
    pub async fn finish_verification(&mut self) -> Result<Vec<WriteAck>, StorageEngineError> {
        let Some(coordinator) = self.verification.as_ref() else {
            return Ok(Vec::new());
        };
        let ready = coordinator.ready().map_err(hash_error)?;
        let mut acknowledgements = Vec::new();
        for chunk in ready {
            let mapped = self
                .mapper
                .map(GlobalSpan {
                    offset: chunk.span.offset(),
                    len: usize::try_from(chunk.span.len())
                        .map_err(|_| StorageEngineError::bare(WriteReject::Mapping))?,
                })
                .map_err(|_| StorageEngineError::bare(WriteReject::Mapping))?;
            let file = self.verification_file(mapped.file)?;
            let algorithm = self
                .verification
                .as_ref()
                .expect("manifest installed")
                .manifest()
                .chunks()
                .get(chunk.piece.get() as usize)
                .map_or(JournalDigestAlgorithm::Sha256, JournalDigest::algorithm);
            let span_len = chunk.span.len();
            let offset = mapped.file_offset;
            let streaming_digest = chunk.digest;
            let reservation = self
                .reserve_cpu(if streaming_digest.is_some() {
                    1024
                } else {
                    64 * 1024
                })
                .await?;
            let (digest, file) = reservation
                .spawn(move || {
                    let digest = if let Some(digest) = streaming_digest {
                        digest
                    } else {
                        let mut hash = ContentHasher::new(algorithm);
                        let mut buffer = [0; 64 * 1024];
                        let mut done = 0;
                        while done < span_len {
                            let count = (span_len - done).min(buffer.len() as u64) as usize;
                            file.read_exact_at(offset + done, &mut buffer[..count])?;
                            hash.update(&buffer[..count]);
                            done += count as u64;
                        }
                        hash.finalize().journal_digest()
                    };
                    Ok::<_, NativeCapabilityError>((digest, file))
                })
                .join()
                .await
                .map_err(|_| StorageEngineError::bare(WriteReject::Verification))?
                .into_inner()
                .map_err(|error| {
                    StorageEngineError::with(
                        WriteReject::NativeFile,
                        StorageEngineErrorDetail::Native(error),
                    )
                })?;
            if self
                .verification
                .as_ref()
                .expect("manifest installed")
                .verify_readback(chunk.piece, &digest)
                .is_err()
            {
                let failed = self
                    .journal
                    .append_payload(
                        self.generation,
                        &JournalPayload::PieceFailed {
                            lease_id: None,
                            piece_id: chunk.piece,
                            piece_span: chunk.span,
                            error_class: ErrorKind::ChecksumMismatch,
                            attempt: 1,
                        },
                    )
                    .map_err(journal_error)?;
                self.journal
                    .flush(failed.sequence())
                    .map_err(journal_error)?;
                self.verification
                    .as_mut()
                    .expect("manifest installed")
                    .invalidate(chunk.piece)
                    .map_err(hash_error)?;
                return Err(StorageEngineError::bare(WriteReject::ChecksumMismatch));
            }
            let contributors_hash = calculate_contributors_hash(&chunk.contributors)
                .map_err(|_| StorageEngineError::bare(WriteReject::Journal))?;
            let validator_set_fingerprint =
                calculate_validator_set_fingerprint(&chunk.contributors)
                    .map_err(|_| StorageEngineError::bare(WriteReject::Journal))?;
            self.journal
                .append_payload(
                    self.generation,
                    &JournalPayload::PieceVerified {
                        piece_id: chunk.piece,
                        piece_span: chunk.span,
                        contributors_hash,
                        digest: digest.clone(),
                    },
                )
                .map_err(journal_error)?;
            self.reserve_cpu(1024)
                .await?
                .spawn(move || file.sync_all())
                .join()
                .await
                .map_err(|_| StorageEngineError::bare(WriteReject::Verification))?
                .into_inner()
                .map_err(|error| {
                    StorageEngineError::with(
                        WriteReject::NativeFile,
                        StorageEngineErrorDetail::Native(error),
                    )
                })?;
            #[cfg(test)]
            self.crash_at(StorageEngineCrashPoint::DataSyncBeforePieceDurable);
            let durable = self
                .journal
                .append_payload(
                    self.generation,
                    &JournalPayload::PieceDurable {
                        piece_id: chunk.piece,
                        piece_span: chunk.span,
                        contributors_hash,
                        validator_set_fingerprint,
                        digest: Some(digest),
                        data_barrier: DataBarrierKind::StrictPiece,
                    },
                )
                .map_err(journal_error)?;
            #[cfg(test)]
            self.crash_at(StorageEngineCrashPoint::PieceDurableBeforeJournalSync);
            let sequence = self
                .journal
                .flush(durable.sequence())
                .map_err(journal_error)?
                .through_sequence();
            self.verification
                .as_mut()
                .expect("manifest installed")
                .mark_durable(chunk.piece);
            acknowledgements.push(WriteAck::PieceDurable {
                piece: chunk.piece,
                sequence,
            });
        }
        Ok(acknowledgements)
    }

    pub fn record_protocol_validator(
        &mut self,
        validator: ariax_storage::ProtocolValidator,
    ) -> Result<(), StorageEngineError> {
        let appended = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::ProtocolValidator { validator },
            )
            .map_err(journal_error)?;
        self.journal
            .flush(appended.sequence())
            .map_err(journal_error)?;
        Ok(())
    }

    pub fn record_whole_file_verified(
        &mut self,
        digests: Vec<JournalDigest>,
    ) -> Result<(), StorageEngineError> {
        let coordinator = self
            .verification
            .as_ref()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Verification))?;
        if !coordinator.is_complete() {
            return Err(StorageEngineError::bare(WriteReject::Verification));
        }
        if digests != coordinator.manifest().whole() {
            return Err(StorageEngineError::bare(WriteReject::ChecksumMismatch));
        }
        let appended = self
            .journal
            .append_payload(
                self.generation,
                &JournalPayload::WholeFileVerified {
                    fingerprint: coordinator.manifest().fingerprint(),
                    digests: digests.into_boxed_slice(),
                },
            )
            .map_err(journal_error)?;
        self.journal
            .flush(appended.sequence())
            .map_err(journal_error)?;
        self.whole_file_verified = true;
        Ok(())
    }

    pub async fn verify_whole_file(&mut self) -> Result<Option<JournalDigest>, StorageEngineError> {
        let coordinator = self
            .verification
            .as_ref()
            .ok_or_else(|| StorageEngineError::bare(WriteReject::Verification))?;
        if !coordinator.is_complete() {
            return Err(StorageEngineError::bare(WriteReject::Verification));
        }
        let expected = coordinator.manifest().whole().to_vec();
        if expected.is_empty() {
            return Ok(None);
        }
        let total = coordinator.manifest().total_length();
        let file = self.verification_file(FileId::new(0))?;
        let hashes = expected
            .iter()
            .map(|digest| ContentHasher::new(digest.algorithm()))
            .collect::<Vec<_>>();
        let actual = self
            .reserve_cpu(66 * 1024)
            .await?
            .spawn(move || {
                let mut hashes = hashes;
                let mut buffer = [0; 64 * 1024];
                let mut offset = 0;
                while offset < total {
                    let count = (total - offset).min(buffer.len() as u64) as usize;
                    file.read_exact_at(offset, &mut buffer[..count])?;
                    for hash in &mut hashes {
                        hash.update(&buffer[..count]);
                    }
                    offset += count as u64;
                }
                Ok::<_, NativeCapabilityError>(
                    hashes
                        .into_iter()
                        .map(|hash| hash.finalize().journal_digest())
                        .collect::<Vec<_>>(),
                )
            })
            .join()
            .await
            .map_err(|_| StorageEngineError::bare(WriteReject::Verification))?
            .into_inner()
            .map_err(|error| {
                StorageEngineError::with(
                    WriteReject::NativeFile,
                    StorageEngineErrorDetail::Native(error),
                )
            })?;
        if actual != expected {
            return Err(StorageEngineError::bare(WriteReject::ChecksumMismatch));
        }
        let final_digest = actual.first().cloned();
        self.record_whole_file_verified(actual)?;
        Ok(final_digest)
    }
}
