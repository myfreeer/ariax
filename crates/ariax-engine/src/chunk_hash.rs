use crate::{ContentChecksum, ContentHasher};
use ariax_core::{LeaseId, PieceId};
use ariax_runtime::{BufferLease, BufferPool, BufferState, ByteBudget, BytePermit, OwnerTag};
use ariax_storage::{
    JournalContributor, JournalDigest, JournalDigestAlgorithm, JournalHash, PersistedSpan,
    VerificationManifest,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Arc,
};

pub const MAX_HASH_COORDINATOR_LEASES: usize = 65_536;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ChunkAlignment {
    #[default]
    Auto,
    Strict,
    Relaxed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChunkHashError {
    Geometry,
    UnknownLease,
    Overlap,
    Order,
    Incomplete,
    ChecksumMismatch,
    Buffer,
    Capacity,
}
impl fmt::Display for ChunkHashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chunk verification: {self:?}")
    }
}
impl Error for ChunkHashError {}

struct OwnedBuffer {
    buffer: Option<BufferLease>,
    pool: BufferPool,
    _reorder: Option<BytePermit>,
}
impl Drop for OwnedBuffer {
    fn drop(&mut self) {
        if let Some(mut buffer) = self.buffer.take() {
            if buffer.state() != BufferState::Releasable {
                let _ = buffer.transition(BufferState::Releasable, OwnerTag::Storage);
            }
            let _ = self.pool.release(buffer);
        }
    }
}
struct Fragment {
    lease: LeaseId,
    offset: u64,
    start: usize,
    len: usize,
    buffer: Arc<OwnedBuffer>,
}
struct Lease {
    span: PersistedSpan,
    validator: JournalHash,
    next: u64,
    committed: bool,
}
struct Chunk {
    span: PersistedSpan,
    next: u64,
    checkpoint_offset: u64,
    hash: ContentHasher,
    checkpoint: ContentHasher,
    feeder: Option<LeaseId>,
    pending: BTreeMap<u64, Fragment>,
    contributors: BTreeSet<LeaseId>,
    readback: bool,
}

/// Owns provisional digest checkpoints and bounded out-of-order buffers.
/// A lease commit advances a checkpoint; bytes from another lease cannot pass
/// that checkpoint until exact response validation commits the first lease.
pub struct ChunkHashCoordinator {
    manifest: Arc<VerificationManifest>,
    alignment: ChunkAlignment,
    reorder: ByteBudget,
    leases: BTreeMap<LeaseId, Lease>,
    chunks: BTreeMap<PieceId, Chunk>,
    durable: BTreeSet<PieceId>,
    readback_transitions: u64,
    force_readback: bool,
}

pub(crate) struct ReadyChunk {
    pub piece: PieceId,
    pub span: PersistedSpan,
    pub contributors: Vec<JournalContributor>,
    pub digest: Option<JournalDigest>,
}

impl ChunkHashCoordinator {
    pub fn new(
        manifest: Arc<VerificationManifest>,
        alignment: ChunkAlignment,
        reorder_bytes: usize,
    ) -> Self {
        Self {
            manifest,
            alignment,
            reorder: ByteBudget::new(reorder_bytes),
            leases: BTreeMap::new(),
            chunks: BTreeMap::new(),
            durable: BTreeSet::new(),
            readback_transitions: 0,
            force_readback: false,
        }
    }
    pub fn held_bytes(&self) -> usize {
        self.reorder.used()
    }
    pub(crate) fn durable_pieces(&self) -> impl Iterator<Item = PieceId> + '_ {
        self.durable.iter().copied()
    }
    pub const fn readback_transitions(&self) -> u64 {
        self.readback_transitions
    }
    pub fn manifest(&self) -> &Arc<VerificationManifest> {
        &self.manifest
    }
    pub fn is_complete(&self) -> bool {
        self.durable.len() as u64
            == self
                .manifest
                .total_length()
                .div_ceil(self.manifest.chunk_length())
            && self.chunks.is_empty()
    }

    fn pieces(&self, span: PersistedSpan) -> Result<std::ops::RangeInclusive<u64>, ChunkHashError> {
        let end = span
            .offset()
            .checked_add(span.len())
            .ok_or(ChunkHashError::Geometry)?;
        if end > self.manifest.total_length() {
            return Err(ChunkHashError::Geometry);
        }
        let first = span.offset() / self.manifest.chunk_length();
        let last = (end - 1) / self.manifest.chunk_length();
        Ok(first..=last)
    }
    fn fresh_chunk(&self, index: u64) -> Result<Chunk, ChunkHashError> {
        let span = self
            .manifest
            .chunk_span(index)
            .ok_or(ChunkHashError::Geometry)?;
        let algorithm = self
            .manifest
            .chunks()
            .get(index as usize)
            .map_or(JournalDigestAlgorithm::Sha256, JournalDigest::algorithm);
        let hash = ContentHasher::new(algorithm);
        Ok(Chunk {
            span,
            next: span.offset(),
            checkpoint_offset: span.offset(),
            checkpoint: hash.clone(),
            hash,
            feeder: None,
            pending: BTreeMap::new(),
            contributors: BTreeSet::new(),
            readback: self.force_readback,
        })
    }
    pub fn begin(
        &mut self,
        id: LeaseId,
        span: PersistedSpan,
        validator: JournalHash,
    ) -> Result<(), ChunkHashError> {
        if self.leases.len() >= MAX_HASH_COORDINATOR_LEASES {
            return Err(ChunkHashError::Capacity);
        }
        if self.leases.contains_key(&id) {
            return Err(ChunkHashError::Overlap);
        }
        let pieces = self.pieces(span)?;
        if self.alignment == ChunkAlignment::Strict && pieces.start() != pieces.end() {
            return Err(ChunkHashError::Geometry);
        }
        let end = span.offset() + span.len();
        if self.chunks.values().any(|chunk| {
            chunk.contributors.iter().any(|id| {
                let lease = &self.leases[id];
                lease.span.offset().max(chunk.span.offset()) < end
                    && span.offset()
                        < (lease.span.offset() + lease.span.len())
                            .min(chunk.span.offset() + chunk.span.len())
            })
        }) {
            return Err(ChunkHashError::Overlap);
        }
        // Preflight the complete span before changing any coordinator.
        for index in pieces.clone() {
            let piece = PieceId::new(index);
            if self.durable.contains(&piece) {
                return Err(ChunkHashError::Overlap);
            }
            if self.alignment != ChunkAlignment::Relaxed {
                let next = self
                    .chunks
                    .get(&piece)
                    .map_or(index * self.manifest.chunk_length(), |chunk| {
                        chunk.checkpoint_offset
                    });
                if span.offset().max(index * self.manifest.chunk_length()) != next
                    || self
                        .chunks
                        .get(&piece)
                        .is_some_and(|chunk| chunk.feeder.is_some())
                {
                    return Err(ChunkHashError::Order);
                }
            }
        }
        for index in pieces {
            let piece = PieceId::new(index);
            if !self.chunks.contains_key(&piece) {
                self.chunks.insert(piece, self.fresh_chunk(index)?);
            }
            self.chunks
                .get_mut(&piece)
                .expect("inserted chunk")
                .contributors
                .insert(id);
        }
        self.leases.insert(
            id,
            Lease {
                span,
                validator,
                next: span.offset(),
                committed: false,
            },
        );
        Ok(())
    }

    /// Receives a disk-completed buffer; retained fragments keep the original
    /// pool allocation. There is no unaccounted payload copy.
    pub fn feed(
        &mut self,
        id: LeaseId,
        offset: u64,
        buffer: BufferLease,
        pool: BufferPool,
    ) -> Result<(), ChunkHashError> {
        let len = buffer.len();
        let mut owned = OwnedBuffer {
            buffer: Some(buffer),
            pool,
            _reorder: None,
        };
        let lease = self.leases.get(&id).ok_or(ChunkHashError::UnknownLease)?;
        let end = offset
            .checked_add(len as u64)
            .ok_or(ChunkHashError::Geometry)?;
        if lease.committed
            || offset != lease.next
            || len == 0
            || end > lease.span.offset() + lease.span.len()
        {
            return Err(ChunkHashError::Order);
        }
        let pieces = self.pieces(
            PersistedSpan::new(offset, len as u64).map_err(|_| ChunkHashError::Geometry)?,
        )?;
        let need_hold = pieces.clone().any(|index| {
            let chunk = &self.chunks[&PieceId::new(index)];
            !chunk.readback
                && (chunk.next != offset.max(chunk.span.offset())
                    || chunk.feeder.is_some_and(|lease| lease != id))
        });
        if need_hold {
            match self
                .reorder
                .try_acquire(owned.buffer.as_ref().expect("owned buffer").capacity())
            {
                Ok(permit) => owned._reorder = Some(permit),
                Err(_) => {
                    for index in pieces.clone() {
                        self.require_readback(PieceId::new(index));
                    }
                }
            }
        }
        let owned = Arc::new(owned);
        for index in pieces {
            let chunk = self
                .chunks
                .get_mut(&PieceId::new(index))
                .ok_or(ChunkHashError::UnknownLease)?;
            if chunk.readback {
                continue;
            }
            let start = offset.max(chunk.span.offset());
            let stop = end.min(chunk.span.offset() + chunk.span.len());
            let fragment = Fragment {
                lease: id,
                offset: start,
                start: (start - offset) as usize,
                len: (stop - start) as usize,
                buffer: owned.clone(),
            };
            if chunk.pending.insert(start, fragment).is_some() {
                return Err(ChunkHashError::Overlap);
            }
        }
        self.leases.get_mut(&id).expect("validated lease").next = end;
        self.drain()
    }

    fn drain(&mut self) -> Result<(), ChunkHashError> {
        for chunk in self.chunks.values_mut() {
            if chunk.readback {
                continue;
            }
            while let Some(fragment) = chunk.pending.get(&chunk.next) {
                if chunk.feeder.is_some_and(|id| id != fragment.lease) {
                    break;
                }
                let fragment = chunk.pending.remove(&chunk.next).expect("present fragment");
                let buffer = fragment
                    .buffer
                    .buffer
                    .as_ref()
                    .ok_or(ChunkHashError::Buffer)?;
                let bytes = buffer.bytes().map_err(|_| ChunkHashError::Buffer)?;
                chunk
                    .hash
                    .update(&bytes[fragment.start..fragment.start + fragment.len]);
                chunk.next = fragment.offset + fragment.len as u64;
                chunk.feeder = Some(fragment.lease);
                let lease = self
                    .leases
                    .get(&fragment.lease)
                    .ok_or(ChunkHashError::UnknownLease)?;
                if lease.committed
                    && chunk.next
                        == (lease.span.offset() + lease.span.len())
                            .min(chunk.span.offset() + chunk.span.len())
                {
                    chunk.checkpoint = chunk.hash.clone();
                    chunk.checkpoint_offset = chunk.next;
                    chunk.feeder = None;
                }
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, id: LeaseId) -> Result<(), ChunkHashError> {
        let lease = self
            .leases
            .get_mut(&id)
            .ok_or(ChunkHashError::UnknownLease)?;
        if lease.committed || lease.next != lease.span.offset() + lease.span.len() {
            return Err(ChunkHashError::Incomplete);
        }
        lease.committed = true;
        for chunk in self.chunks.values_mut() {
            if chunk.feeder == Some(id) {
                chunk.checkpoint = chunk.hash.clone();
                chunk.checkpoint_offset = chunk.next;
                chunk.feeder = None;
            }
        }
        self.drain()
    }

    pub fn abort(&mut self, id: LeaseId) -> Result<(), ChunkHashError> {
        let lease = self.leases.get(&id).ok_or(ChunkHashError::UnknownLease)?;
        if lease.committed {
            return Err(ChunkHashError::Order);
        }
        self.leases.remove(&id);
        for chunk in self.chunks.values_mut() {
            chunk.pending.retain(|_, fragment| fragment.lease != id);
            chunk.contributors.remove(&id);
            if chunk.feeder == Some(id) {
                chunk.hash = chunk.checkpoint.clone();
                chunk.next = chunk.checkpoint_offset;
                chunk.feeder = None;
            }
        }
        Ok(())
    }

    pub(crate) fn require_readback(&mut self, piece: PieceId) {
        if let Some(chunk) = self.chunks.get_mut(&piece) {
            if !chunk.readback {
                self.readback_transitions += 1;
            }
            chunk.readback = true;
            chunk.pending.clear();
            chunk.feeder = None;
        }
    }

    pub(crate) fn force_readback(&mut self) {
        self.force_readback = true;
        for piece in self.chunks.keys().copied().collect::<Vec<_>>() {
            self.require_readback(piece);
        }
    }

    pub(crate) fn pending_chunks(&self) -> Vec<(PieceId, PersistedSpan)> {
        self.chunks
            .iter()
            .map(|(piece, chunk)| (*piece, chunk.span))
            .collect()
    }

    pub(crate) fn ready(&self) -> Result<Vec<ReadyChunk>, ChunkHashError> {
        let mut ready = Vec::new();
        for (&piece, chunk) in &self.chunks {
            let mut spans = Vec::new();
            let mut contributors = Vec::new();
            for id in &chunk.contributors {
                let lease = &self.leases[id];
                if !lease.committed {
                    continue;
                }
                let start = lease.span.offset().max(chunk.span.offset());
                let end = (lease.span.offset() + lease.span.len())
                    .min(chunk.span.offset() + chunk.span.len());
                spans.push((start, end));
                contributors.push(JournalContributor::new(*id, lease.span, lease.validator));
            }
            spans.sort_unstable();
            let mut next = chunk.span.offset();
            let mut complete = true;
            for (start, end) in spans {
                if start < next || end <= start {
                    return Err(ChunkHashError::Overlap);
                }
                if start != next {
                    complete = false;
                }
                next = end;
            }
            if !complete {
                continue;
            }
            if next != chunk.span.offset() + chunk.span.len() {
                continue;
            }
            let digest = if chunk.readback {
                None
            } else {
                if chunk.checkpoint_offset != next || chunk.feeder.is_some() {
                    continue;
                }
                let actual = chunk.checkpoint.clone().finalize().journal_digest();
                Some(actual)
            };
            ready.push(ReadyChunk {
                piece,
                span: chunk.span,
                contributors,
                digest,
            });
        }
        Ok(ready)
    }

    pub(crate) fn contains_lease(&self, id: LeaseId) -> bool {
        self.leases.contains_key(&id)
    }

    pub(crate) fn restore_committed(
        &mut self,
        contributor: JournalContributor,
    ) -> Result<(), ChunkHashError> {
        let id = contributor.lease_id();
        let span = contributor.span();
        if self.leases.contains_key(&id) || self.leases.len() >= MAX_HASH_COORDINATOR_LEASES {
            return Err(ChunkHashError::Capacity);
        }
        let pieces = self.pieces(span)?;
        let end = span.offset() + span.len();
        // Recovery is an admission boundary too. Do not allow overlapping
        // contributors to turn a complete prefix into apparent full coverage.
        for index in pieces.clone() {
            if let Some(chunk) = self.chunks.get(&PieceId::new(index)) {
                for existing in &chunk.contributors {
                    let existing = &self.leases[existing].span;
                    if span
                        .offset()
                        .max(existing.offset())
                        .max(chunk.span.offset())
                        < end
                            .min(existing.offset() + existing.len())
                            .min(chunk.span.offset() + chunk.span.len())
                    {
                        return Err(ChunkHashError::Overlap);
                    }
                }
            }
        }
        for index in pieces {
            let piece = PieceId::new(index);
            if self.durable.contains(&piece) {
                continue;
            }
            if !self.chunks.contains_key(&piece) {
                self.chunks.insert(piece, self.fresh_chunk(index)?);
            }
            self.chunks
                .get_mut(&piece)
                .expect("restored chunk")
                .contributors
                .insert(id);
            self.require_readback(piece);
        }
        self.leases.insert(
            id,
            Lease {
                span,
                validator: contributor.validator_fingerprint(),
                next: span.offset() + span.len(),
                committed: true,
            },
        );
        Ok(())
    }

    pub(crate) fn commit_written_after_fence(
        &mut self,
        id: LeaseId,
        span: PersistedSpan,
        validator: JournalHash,
    ) -> Result<(), ChunkHashError> {
        self.begin(id, span, validator)?;
        for index in self.pieces(span)? {
            self.require_readback(PieceId::new(index));
        }
        self.leases.get_mut(&id).expect("admitted lease").next = span.offset() + span.len();
        self.commit(id)
    }

    pub(crate) fn mark_durable(&mut self, piece: PieceId) {
        self.chunks.remove(&piece);
        self.durable.insert(piece);
        self.leases.retain(|id, lease| {
            !lease.committed
                || self
                    .chunks
                    .values()
                    .any(|chunk| chunk.contributors.contains(id))
        });
    }

    pub(crate) fn invalidate(&mut self, piece: PieceId) -> Result<(), ChunkHashError> {
        self.chunks.remove(&piece);
        self.leases.retain(|id, _| {
            self.chunks
                .values()
                .any(|chunk| chunk.contributors.contains(id))
        });
        self.durable.remove(&piece);
        Ok(())
    }

    pub(crate) fn verify_readback(
        &self,
        piece: PieceId,
        digest: &JournalDigest,
    ) -> Result<(), ChunkHashError> {
        let expected = self.manifest.chunks().get(piece.get() as usize);
        if expected.is_some_and(|expected| expected != digest) {
            return Err(ChunkHashError::ChecksumMismatch);
        }
        ContentChecksum::try_from(digest).map_err(|_| ChunkHashError::Geometry)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ariax_runtime::BufferPoolConfig;
    fn id(value: u64) -> LeaseId {
        LeaseId::new(value).unwrap()
    }
    fn span(start: u64, len: u64) -> PersistedSpan {
        PersistedSpan::new(start, len).unwrap()
    }
    fn validator() -> JournalHash {
        JournalHash::new([7; 32]).unwrap()
    }
    fn coordinator(
        data: &[u8],
        length: u64,
        mode: ChunkAlignment,
        budget: usize,
    ) -> ChunkHashCoordinator {
        let digests = data
            .chunks(length as usize)
            .map(|bytes| {
                let mut hash = ContentHasher::new(JournalDigestAlgorithm::Sha256);
                hash.update(bytes);
                hash.finalize().journal_digest()
            })
            .collect();
        ChunkHashCoordinator::new(
            Arc::new(
                VerificationManifest::new(data.len() as u64, length, digests, vec![]).unwrap(),
            ),
            mode,
            budget,
        )
    }
    fn feed(
        coordinator: &mut ChunkHashCoordinator,
        pool: &BufferPool,
        lease: u64,
        offset: u64,
        data: &[u8],
    ) {
        let mut buffer = pool
            .try_reserve(data.len(), OwnerTag::Network, None, None)
            .unwrap();
        buffer
            .transition(BufferState::NetworkFill, OwnerTag::Network)
            .unwrap();
        buffer.writable().unwrap()[..data.len()].copy_from_slice(data);
        buffer.mark_filled(data.len(), OwnerTag::Storage).unwrap();
        coordinator
            .feed(id(lease), offset, buffer, pool.clone())
            .unwrap();
    }
    fn pool() -> BufferPool {
        BufferPool::new(BufferPoolConfig::new(1024 * 1024, 1024 * 1024)).unwrap()
    }

    #[test]
    fn later_committed_lease_waits_for_the_gap_and_abort_restores_checkpoint() {
        let pool = pool();
        let mut hash = coordinator(b"abcdefgh", 8, ChunkAlignment::Relaxed, 64 * 1024);
        hash.begin(id(2), span(4, 4), validator()).unwrap();
        feed(&mut hash, &pool, 2, 4, b"efgh");
        hash.commit(id(2)).unwrap();
        assert!(hash.ready().unwrap().is_empty());
        assert_eq!(hash.held_bytes(), 16 * 1024);
        hash.begin(id(1), span(0, 4), validator()).unwrap();
        feed(&mut hash, &pool, 1, 0, b"XX");
        hash.abort(id(1)).unwrap();
        assert!(hash.ready().unwrap().is_empty());
        hash.begin(id(3), span(0, 4), validator()).unwrap();
        feed(&mut hash, &pool, 3, 0, b"abcd");
        hash.commit(id(3)).unwrap();
        let ready = hash.ready().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].contributors.len(), 2);
        hash.verify_readback(ready[0].piece, ready[0].digest.as_ref().unwrap())
            .unwrap();
        assert_eq!(hash.held_bytes(), 0);
        assert_eq!(pool.metrics().quarantined_bytes, 0);
    }

    #[test]
    fn reorder_exhaustion_requires_readback_and_never_accepts_a_bad_digest() {
        let pool = pool();
        let mut hash = coordinator(b"abcdefgh", 8, ChunkAlignment::Relaxed, 1);
        hash.begin(id(1), span(4, 4), validator()).unwrap();
        feed(&mut hash, &pool, 1, 4, b"efgh");
        hash.commit(id(1)).unwrap();
        hash.begin(id(2), span(0, 4), validator()).unwrap();
        feed(&mut hash, &pool, 2, 0, b"abcd");
        hash.commit(id(2)).unwrap();
        assert_eq!(hash.held_bytes(), 0);
        assert_eq!(hash.readback_transitions(), 1);
        assert!(hash.ready().unwrap()[0].digest.is_none());
        assert_eq!(
            hash.verify_readback(
                PieceId::new(0),
                &JournalDigest::new(JournalDigestAlgorithm::Sha256, vec![0; 32]).unwrap()
            ),
            Err(ChunkHashError::ChecksumMismatch)
        );
    }

    #[test]
    fn all_ordered_partitions_hash_the_declared_bytes_and_strict_rejects_crossing() {
        let pool = pool();
        for cut in 1..8 {
            let mut hash = coordinator(b"abcdefgh", 8, ChunkAlignment::Auto, 0);
            assert_eq!(
                hash.begin(id(5), span(cut, 8 - cut), validator()),
                Err(ChunkHashError::Order)
            );
            hash.begin(id(1), span(0, cut), validator()).unwrap();
            feed(&mut hash, &pool, 1, 0, &b"abcdefgh"[..cut as usize]);
            hash.commit(id(1)).unwrap();
            assert!(hash.ready().unwrap().is_empty());
            hash.begin(id(2), span(cut, 8 - cut), validator()).unwrap();
            feed(&mut hash, &pool, 2, cut, &b"abcdefgh"[cut as usize..]);
            hash.commit(id(2)).unwrap();
            let ready = hash.ready().unwrap();
            hash.verify_readback(ready[0].piece, ready[0].digest.as_ref().unwrap())
                .unwrap();
            assert_eq!(hash.readback_transitions(), 0);
        }
        let mut strict = coordinator(b"abcdefgh", 4, ChunkAlignment::Strict, 0);
        assert_eq!(
            strict.begin(id(1), span(0, 8), validator()),
            Err(ChunkHashError::Geometry)
        );
        let mut auto = coordinator(b"abcdefgh", 4, ChunkAlignment::Auto, 0);
        auto.begin(id(1), span(0, 8), validator()).unwrap();
        feed(&mut auto, &pool, 1, 0, b"abcdefgh");
        auto.commit(id(1)).unwrap();
        assert_eq!(auto.ready().unwrap().len(), 2);
        assert_eq!(pool.metrics().quarantined_bytes, 0);
    }

    #[test]
    fn recovered_overlap_is_rejected_without_changing_exact_coverage() {
        let mut hash = coordinator(b"abcdefgh", 8, ChunkAlignment::Auto, 0);
        hash.restore_committed(JournalContributor::new(id(1), span(0, 8), validator()))
            .unwrap();
        assert_eq!(
            hash.restore_committed(JournalContributor::new(id(2), span(4, 4), validator())),
            Err(ChunkHashError::Overlap)
        );
        let ready = hash.ready().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].contributors.len(), 1);
        assert!(ready[0].digest.is_none());
    }
}
