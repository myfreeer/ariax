# Shared Protocol Transfer Design

Status: Phase 5 implementation and local validation are complete at `6a55b1f`,
following the accepted scope from `dfdeae6`. The
[validation record](performance-evidence/phase5-validation-2026-09-15.md) covers
Linux-under-WSL and native Windows-GNU tests, interoperability, bounded fuzzing
and native Windows measurements. Native Linux acceptance remains deferred until
CI is ready.

## Scope And Checkpoints

| Gate | Required Result | Current Status |
| --- | --- | --- |
| `P5-01` Shared foundations | Protocol-neutral source/task dispatch, bounded CPU work, real feature bundles, existing HTTP compatibility | Passed locally |
| `P5-02` Verification and recovery | Four content digests, immutable verification manifests, multiple leases per chunk, ordered hashing and bounded readback fallback | Passed locally |
| `P5-03` Metalink | Bounded v3/v4 parser, safe names, atomic selected-file admission, streaming downloads and follow behavior | Passed locally |
| `P5-04` FTP/FTPS | Pinned parser/logging/active-peer patches, owned socket paths, sequential resume, protected FTPS data | Passed locally |
| `P5-05` SFTP | Pinned framing patch, host-key approval, authentication policy and bounded offset pipeline | Passed locally |
| `P5-06` Integration | Mixed-source scheduling, selectors/statistics, public parity, self-contained JSON migration and recorded validation | Passed locally |

These local gates do not close the P4-11 native Linux gate. HTTP/2, BitTorrent,
growing layouts, new native disk backends and release/tag qualification remain
separate gates.

## Shared Ownership

Every transfer uses the existing managed control runtime, scheduler, immutable
query roots, resource manager, persistence owner and shutdown coordinator.
Protocol selection does not create another scheduler or progress driver.
Protocol-neutral source/task specifications preserve the HTTP API through
compatibility conversions while adding protocol-specific immutable options.
URI identity, persistence-safe URI text, live credentials and protocol identity
are distinct fields; debug output never formats a live URI or credential.

HTTP(S) and SFTP sources use one random-access lease coordinator. FTP/FTPS is an
exclusive sequential fallback for one output file: all competing writes drain
before REST/RETR starts from the contiguous durable prefix. Distinct FTP mirrors
fail over rather than writing concurrently into one file. A data stream keeps
one transfer-attempt identity across successive storage leases. Different files
and tasks remain independently schedulable.

Destination authorization, DNS/Happy Eyeballs, proxy final-hop pinning, handles,
rate permits, discard accounting and storage backpressure are shared. FTP
control and passive data sockets must pass through the owned connector/builder.
SFTP channels carry explicit bounded request identities. Accepted work and its
reservations survive caller cancellation until completion or acknowledged drain.

Server feedback uses canonical scheme/host/port keys for every enabled
protocol, including FTP/FTPS sequential transfers. Keys exclude credentials,
paths and queries. Successful feedback follows required whole-file verification;
checksum failures count as failed samples. Origin statistics do not authorize
FTP sources to receive random-access leases.

The P4-11 owner budget remains cooperative: 32 steps/about 1 ms, with at most
one bulk target per turn. Parsing, metadata rendering, filesystem preparation
and expensive hashing run outside that owner. A private Rayon pool is admitted
through bounded job/byte/completion permits; it never uses Rayon's global pool.
The compact minimum-thread mode uses its shared bounded disk/CPU lane.

## Verification And Persistence

Content checksums use canonical `TYPE=hex` values with exactly the digest's
byte length. Supported algorithms are `sha-512`, `sha-256`, `sha-1` and `md5`.
Metadata selects the strongest complete supported set in that order and never
downgrades after a mismatch. A separately supplied user checksum is additional.
SHA-1 and MD5 are compatibility checksums, not strict cross-mirror identity
evidence; strict concurrent mirrors require SHA-256/SHA-512 evidence covering
the bytes assigned to them.

The immutable verification manifest binds the selected file's length, chunk
geometry, expected digests and whole-file requirements to its task generation.
It is charged to metadata budgets and cannot change within that generation.
Storage records committed contributors separately from verification chunks;
neither a partial response nor an independent lease digest proves a whole
chunk. The exact coordinator, abort, overlap and readback rules are owned by
[Metalink chunking](metalink-chunking.md).

An empty file completes through the same manifest and whole-file verification
barriers without issuing data leases. HTTP accepts an exact empty `200` response
or a bodyless `416` with `Content-Range: bytes */0`; a nonempty response cannot
stand in for an empty file. SFTP still performs final attribute validation and
drains its handle before completion.

New required journal records carry verification manifests, bounded manifest
continuations and FTP/SFTP validator tuples. Existing record numbers/payloads,
v1 framing and SQLite schema v2 retain their meanings. Existing HTTP journals
remain readable; a reader that does not recognize a required record fails
closed. A manifest must be complete and fingerprint-validated before network
leases or recovered verification evidence can use it. Every continuation obeys
the 16 MiB record cap and the aggregate metadata budget. Protocol validators
bind the source identity, exact length and protocol-specific modification/key
evidence; weak timestamps are never cross-origin content identity.

## Metalink Admission And Migration

The accepted schemas are Metalink v3 and v4, parsed through quick-xml's bounded
pull-reader path without a DOM. DTDs, entity declarations, unsupported encodings,
unsafe names, malformed digest sets and resource overflow reject the document.
The document/token/nesting/attribute/file/source limits in `configuration.md`
apply before proportional allocation. Local inputs, native byte inputs, RPC
base64 uploads and automatic HTTP following share that parser and validation.
Transport request caps remain additional limits on uploaded metadata.

The parser defaults to a 64 MiB document cap, configurable up to the 256 MiB
hard cap. Shared admission limits the parser's retained metadata workspace to
4 MiB and the selected batch to 1,000 tasks. RPC request/resident budgets and
automatic-follow reservations can impose smaller limits; increasing the XML
document option does not increase those independent budgets.

Selection and language/OS/version filters produce one task per selected file,
in document order. Relative URIs use the explicit local base or final admitted
metadata URI. Every name passes the portable safe-path and collision rules.
Complete syntax, source, output, verification and capacity validation precedes
atomic batch publication. Unsupported mirrors may be omitted only if each
selected file still has a usable source; disabled-only files fail explicitly.
Torrent metaurls do not activate Phase 6.

Native JSON migration version 2 carries selected files, sanitized sources,
checksum algorithms/values and chunk geometry without requiring the original
XML. Version-1 import remains supported; exports requiring no new metadata
retain version 1. JSON imports remain paused by default and never treat exported
progress as crash-recovery authority. Existing export byte/item limits and
credential placeholders apply. Aria2 text export fails atomically with a typed
error when it cannot retain required Metalink verification metadata; JSON is
the self-contained migration format for those tasks.

`aria2.addMetalink` accepts base64 bytes plus optional options/queue position and
returns an array of GIDs. The native `AddMetalink` operation and CLI Metalink
inputs use the same admission. SFTP approval uses the current challenge id and
SHA-256 fingerprint through `ariax.approveHostKey`, the typed native operation
or `ariax approve-host-key`; generic resume never approves trust.

Direct `--add-uri` and `--add-metalink` commands accept trailing
`--NAME=VALUE` download options. Metalink accepts `--metalink-base-uri` and
`--position` in addition to the shared selection/filter options. The Rust
`DownloadOptions::from_pairs` conversion uses the same registry and admission
validation. Direct commands wait for automatically followed children; explicit
paused admission returns after publication. Noninteractive trust never prompts.

A Metalink queue position is `-1` for append or a nonnegative insertion index,
clamped to the selected queue length. Selected files occupy consecutive
positions in document order. Existing queue rows shift inside the same SQLite
transaction that installs the complete batch; scheduler publication remains
fenced until every member reflects that order. Output preflight rejects
portable case-folded collisions with queued tasks, batch members, or existing
filesystem entries before any task is published.

Automatic following recognizes Metalink MIME types on the ordinary HTTP probe;
ordinary file downloads do not acquire an extra request. A recognized document
is fetched through the same destination, rate, memory and cancellation controls.
The final metadata URI supplies its relative-URI base. Child tasks disable
recursive following. `follow-metalink=true` retains the metadata file;
`follow-metalink=mem` keeps it only in charged memory.

Selected children and a parent expansion marker commit in one SQLite
transaction. The marker binds the parent generation and option snapshot,
document digest and ordered child GIDs. A restarted parent with this marker
completes the metadata operation without downloading or admitting the children
again. A required `MetadataComplete` journal record permits a completed
metadata-only parent without inventing a data-file layout. Expanded parents are
omitted from migration exports; their independently self-contained children
remain exportable.

Admission reserves the replacement parent metadata and every child before
creating their journals or committing the batch. Publication retains those
reservations through the atomic scheduler fence, so memory pressure cannot
leave a durable expansion without its matching in-memory parent state.

Shutdown closes the bounded following handoff before draining admissions. It
rejects unaccepted metadata requests, while an accepted child batch retains its
reservation through atomic publication and the parent acknowledgement. Metadata
parents expose their ordered child GIDs as `followedBy`. Task metadata charges
include live credentials, local authority paths, verification tables and parent
expansion state; those charges remain live with immutable catalog snapshots.

## Dependencies And Feature Bundles

Pin quick-xml 0.41.0, Rayon 1.12.0 and RustCrypto sha2/sha1/md-5 0.11.0.
Create and vendor the required patches against SuppaFTP 10.0.1 and russh-sftp
2.3.0 through path-based `[patch.crates-io]` entries. Record upstream archive
checksums, VCS identity, patch diffs, licenses and patched-tree hashes.
Source/behavior assertions reject the unpatched allocation, logging and active
callback paths. Release dependencies do not acquire floating Git sources.

SuppaFTP selects only its Tokio/rustls-ring transport. Russh 0.62.4 disables
defaults and selects exactly `ring`, `flate2`, and `rsa`, with its exact
`ssh-key` 0.7.0-rc.11 re-export. Feature checks reject a second TLS provider or
accidental legacy DSA/DES features. `minimal` includes Metalink and excludes the
FTP/SSH dependency graph; `standard` adds FTP/FTPS and SFTP. Inherited `full` and
`compat` do not imply implemented BitTorrent support.

## Validation And Evidence

Each checkpoint adds success/rejection tests for changed behavior. Parser and
pure coordinator coverage includes bounded fuzz/property cases; persisted
state includes fault/crash/replay cases. Protocol tests use private loopback
servers and adversarial peers, including real FTP/FTPS and OpenSSH interop.
Generated contracts, feature/source assertions, MSRV 1.88 and the existing
HTTP/P4-11 regression guarantees remain required.

Final local checks use repository-local pinned toolchains for Linux-under-WSL
and native Windows-GNU. Native Clippy uses the explicit pinned frontend and its
separate target directory. Native Linux acceptance is not inferred from WSL or
from a configured CI job.

Benchmarks run after compilation, with no concurrent compiler load. Each burst
stops at 1,000 operations or 500 ms, whichever comes first, followed by at least
250 ms cooldown. Active-range barriers are renewed and each scenario has a
90-second deadline. Existing RPC latency and resource gates remain in force.
Incomplete or over-limit runs fail; reports retain failed attempts as well as
source/binary hashes. Phase-5 evidence is separate from historical P4 reports.

`ARIAX_BENCH_METALINK=1` admits the active-range benchmark through Metalink,
with a complete SHA-256 chunk manifest. The report records that admission path;
the default retains the ordinary `addUri` fixture for comparison.

The September 15 native Windows campaign passes all four 20,000-call transports
and the separate 128-task administrative scenario. Worst operation p99 is
38.946 ms, longest transport burst is 420 ms, and every scenario finishes within
90 seconds. Ten ASan/coverage fuzz targets each pass 512 executions in accepted
bursts no longer than 500 ms; three over-limit attempts remain excluded. The
[raw evidence](performance-evidence/phase5-windows-gnu-2026-09-15.json) records
source/binary hashes, reports, seed inventories and validation log hashes.
