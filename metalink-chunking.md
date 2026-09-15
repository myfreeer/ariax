# Metalink Chunking And Checksums

Status: implementation is underway under the Phase-5
[shared transfer gates](detailed-protocol-transfers.md). Verification and parser
acceptance remain pending until their executable evidence is recorded.

Decision: when Metalink chunk hashes are present and enabled, verification
chunks are fixed by the Metalink checksum ranges. Network range leases should
prefer those boundaries. Larger leases are allowed only if the pipeline can
verify the Metalink chunks from streamed buffers without disk readback.

Readback is allowed, but it is a fallback or recovery path, not the normal
high-throughput path.

## Terms

- Metalink verification chunk: the byte range covered by one Metalink chunk
  checksum.
- Range lease: dynamic network transfer range assigned to a worker.
- Disk write batch: one or more validated buffer writes submitted to storage.

These do not have to be the same size, but their boundaries must be handled
explicitly.

## Default Rule

If chunk hashes are active:

- verification units are exactly the Metalink chunk ranges,
- leases should be aligned to one or more full Metalink chunks when practical,
- a lease may cover multiple Metalink chunks,
- a lease may be smaller than a Metalink chunk after retry or near EOF,
- the default scheduler has at most one hash-feeding lease at a time for any
  verification chunk and advances it in offset order,
- durable completion is recorded per Metalink verification chunk,
- final file completion requires all selected verification chunks durable.

When metadata offers multiple algorithms for the same range, the baseline
selects exactly the strongest supported set in this order:
`sha-512 > sha-256 > sha-1 > md5`. It does not fall back to a weaker digest
after a stronger mismatch. Unknown algorithms are ignored only when a supported
set exists; otherwise the checksum requirement is unsupported. A separately
configured user checksum is additional and must also pass. The selected
algorithm/value set is persisted in the generation snapshot and cannot change
mid-generation.

The preferred fast path is:

```text
network buffer -> per-chunk hash state -> disk write -> hash match -> durable
```

No disk readback is needed if all bytes for a Metalink chunk passed through one
offset-ordered hash state before buffer release and every contributing response
lease committed. Hashing independent lease digests and concatenating their
results is never a substitute for hashing the declared byte range.

## Larger Leases

A lease can be larger than one Metalink chunk if:

- the lease is split into subspans at checksum boundaries,
- hash states are updated for each verification chunk,
- disk writes keep exact global offsets,
- buffers are not released until the needed hash updates are complete,
- durable records are emitted independently per verification chunk.

Example:

```text
Metalink chunks:  [0..1M) [1M..2M) [2M..3M)
Network lease:    [0..3M)
Hash updates:     chunk0, chunk1, chunk2 separately
Durable records:  chunk0, chunk1, chunk2 independently
```

This is useful on high-throughput links where tiny checksum chunks would cause
too many requests.

## Smaller Or Misaligned Leases

Smaller or misaligned leases are allowed for retries and tail work:

```text
Metalink chunk: [0..1M)
lease A:        [0..512K)
lease B:        [512K..1M)
```

The verification chunk is not durable until both spans are present and the
chunk hash matches.

Misaligned leases across boundaries are accepted only if the storage/hash
pipeline can split the buffer into the affected verification chunks. Otherwise
the scheduler should avoid issuing them.

## Ordering And Reassembly

Each active verification chunk has one `ChunkHashCoordinator` keyed by task
generation and chunk id. Its logical state is:

```rust
pub struct ChunkHashState {
    pub next_offset: u64,
    pub committed_checkpoint: DigestState,
    pub pending: BTreeMap<u64, HashFragment>,
    pub held_bytes: usize,
    pub mode: HashMode, // Streaming or ReadbackRequired
}
```

The coordinator feeds the digest only at `next_offset`. Later fragments wait in
`pending`; when a gap closes, it drains the longest contiguous prefix in offset
order. A pending fragment retains the owning `BufferLease` (and its subspan
metadata), not an async borrow or an unbounded payload copy, and remains counted
against global/per-task buffer-pool limits.

The normal `auto` path avoids reassembly by serializing smaller leases within a
chunk. It may stream a response into a provisional digest state, but it keeps a
digest checkpoint at the start of that `LeaseId` and does not feed bytes from a
different lease past that boundary until exact response validation commits the
current lease. An abort restores the checkpoint and discards that attempt's
candidate state. A larger single lease may feed several chunk coordinators in
ascending body order; all candidate states are discarded if that response
lease aborts.

`relaxed` mode may allow out-of-order completions through the reorder map. The
hash reorder budget is a hard internal sub-budget of both the task buffer cap
and global pool cap; it cannot grow memory beyond either cap. If adding a later
fragment would exceed the sub-budget, the coordinator switches that chunk to
`ReadbackRequired`: retained and subsequent blocks proceed through provisional
disk writes, buffers are released after their `DiskWriteOutcome`, and no more
streamed digest state is trusted for that chunk. After all required leases
commit, the disk lane reads the exact chunk contiguously and verifies it under
normal backpressure. If readback is unavailable, the affected leases are
aborted and reissued in offset order.

Interaction rules:

- a gap keeps `next_offset` fixed; normal lease timeout/retry reissues the
  missing span rather than hashing later bytes first,
- `AbortLease` removes that lease's queued fragments and restores the last
  committed digest checkpoint if it had begun feeding,
- an endgame commit is only a candidate until every competing disk write is
  fenced. If a loser wrote or cannot be cancellation-confirmed, the whole
  overlap group and each touched Metalink chunk return to pending; physical
  bytes remain in place for the next lease to overwrite,
- a checksum mismatch appends `PieceFailed`, invalidates the whole Metalink
  verification chunk, and redownloads/overwrites it under the hash-mismatch
  retry policy,
- a memory-limit transition to readback is deterministic and observable in
  metrics; it is not an allocation failure or a reason to bypass verification.

## Readback Policy

Readback is allowed for:

- startup recovery when control state says bytes may exist but hash state was
  lost,
- strict verification requested by user,
- a partial chunk whose bytes were written before hash state completed,
- debugging or repair,
- fallback backends where streamed hash state was unavailable.

Readback is not the normal path because it:

- doubles disk I/O for verified data,
- hurts HDD behavior,
- can compete with active writes,
- increases completion latency.

If readback is needed, it is scheduled through the disk lane with low priority
relative to control/journal writes and with normal backpressure.

## Retry Interaction

Retry remains lease/span based:

- failed lease span returns to pending,
- only committed lease spans inside a Metalink chunk are remembered as written;
  provisional/aborted spans remain invisible and not durable,
- checksum failure invalidates the affected Metalink verification chunk,
- retry can request only the missing spans if the chunk's partial byte map is
  trustworthy,
- otherwise redownload the whole Metalink chunk.

## Configuration

Relevant options:

```text
--check-integrity=true|false
--realtime-chunk-checksum=true|false
--metalink-chunk-alignment=auto|strict|relaxed
```

`auto`:

- align leases when cheap,
- allow multi-chunk leases with streamed sub-hashing,
- serialize smaller leases within a checksum chunk in increasing offset order,
- avoid readback in normal operation.

`strict`:

- leases must not cross Metalink chunk boundaries while chunk hashes are active,
- simpler recovery,
- more requests for small checksum chunks.

`relaxed`:

- allow larger/misaligned leases if the pipeline can split/hash correctly,
- use the bounded reorder coordinator and fall back to readback when necessary,
- intended for high-throughput environments.

## Acceptance Rules

Never mark a Metalink chunk durable unless:

- all bytes in that verification chunk are present at correct offsets,
- every contributing `LeaseId` committed and every losing/failed attempt was
  excluded,
- the configured hash matches,
- storage write acks are complete,
- journal durable record is saved according to durability mode.

Required tests cover B-before-A arrival, a permanent gap, retry and abort after
partial hash feed, endgame winner/loser arbitration, reorder-budget exhaustion,
readback fallback, checksum failure rollback, and crash recovery with committed
spans but no `PieceDurable` record.
