# Ariax Documentation

[Project overview](../README.md) · [Development guide](development/README.md) ·
[Implementation readiness](project/implementation-readiness.md)

## Start Here

| Task | Read |
| --- | --- |
| Try the downloader | [CLI introduction](../README.md#run-the-cli), [build and validation guide](development/README.md) |
| Configure downloads | [Configuration and aria2 compatibility](interfaces/configuration.md), [performance profiles](runtime/performance-profiles.md) |
| Embed or control Ariax | [Rust API, RPC and embedding](interfaces/apis-and-embedding.md) |
| Understand the design | [Architecture overview](architecture/overview.md), [library choices](architecture/library-choice.md) |
| Find remaining work | [Implementation plan](project/implementation-plan.md), [readiness gates](project/implementation-readiness.md) |
| Assess validation evidence | [Requirements traceability](project/requirements-traceability.md), [CI baseline](../performance-evidence/ci-baseline-2026-09-22.md) |

## Source Of Truth

The focused subsystem documents below own the design contracts. Edit the owning
document before changing a contract. Detailed module documents define exact
types, formats and invariants; the architecture overview provides context.

[Implementation readiness](project/implementation-readiness.md) distinguishes
working behavior from uncompleted gates, and
[requirements traceability](project/requirements-traceability.md) maps claims to
evidence. A design requirement alone is not evidence of implementation.
[Generated contracts](../generated/README.md) record executable registry and
persistence definitions and are checked in CI.

Documents in [reviews](#historical-reviews) are historical and non-normative.
They explain earlier decisions; current subsystem contracts take precedence.

## Architecture And Security

- [Architecture overview](architecture/overview.md): goals, runtime lanes,
  lifecycle, transfer pipeline, recovery, build targets and testing strategy.
- [Core contracts](architecture/detailed-core.md): identifiers, task states,
  commands, immutable snapshots, cancellation and persistence hooks.
- [Library choices](architecture/library-choice.md): dependency decisions,
  toolchains, platform baseline and supply-chain policy.
- [Security and recovery](architecture/security-recovery.md): path boundaries,
  credentials, RPC, SSRF and cross-store recovery invariants.

## Configuration And APIs

- [Configuration and compatibility](interfaces/configuration.md)
- [Config parser and option registry](interfaces/detailed-config.md)
- [RPC, native Rust API and embedding](interfaces/apis-and-embedding.md)

## Protocols

- [HTTP first-slice contracts](protocols/detailed-http-first-slice.md)
- [Shared protocol transfers](protocols/detailed-protocol-transfers.md)
- [FTP, FTPS and SFTP](protocols/detailed-ftp-sftp.md)
- [BitTorrent integration and Phase 6 gates](protocols/libtorrent-integration.md)
- [Metalink chunking and checksums](protocols/metalink-chunking.md)
- [Protocol modernization](protocols/protocol-modernization.md)
- [Split downloads and range leases](protocols/split-download.md)
- [Retry policy](protocols/retry-policy.md)
- [Redirect policy](protocols/redirect-policy.md)

## Runtime And Performance

- [Runtime, queues and buffers](runtime/detailed-runtime.md)
- [Threading model](runtime/threading-model.md)
- [Messaging and queue topology](runtime/messaging-model.md)
- [Event backends](runtime/event-backends.md)
- [Download scheduling](runtime/download-scheduling.md)
- [Performance profiles and measured limits](runtime/performance-profiles.md)
- [Buffer pool](runtime/buffer-pool.md)
- [Backpressure](runtime/backpressure.md)
- [Rate limiting](runtime/rate-limiting.md)
- [Statistics and stall detection](runtime/stats-and-stalls.md)
- [Zero-copy policy](runtime/zero-copy.md)

## Storage And Persistence

- [Storage, layouts and journals](storage/detailed-storage.md)
- [Disk adapter](storage/disk-adapter.md)
- [Session persistence](storage/session-persistence.md)

## Implementation And Validation

- [Implementation plan](project/implementation-plan.md)
- [Implementation readiness](project/implementation-readiness.md)
- [Requirements traceability](project/requirements-traceability.md)
- [Implementation history](project/implementation-history.md): checkpoint
  narrative preserved from the former root README.
- [Development guide](development/README.md)
- [Continuous integration](development/continuous-integration.md)
- [Retained performance evidence](../performance-evidence)
- [Fuzz targets](../fuzz/README.md)
- [Compatibility inputs](../compat/README.md),
  [generated contracts](../generated/README.md) and
  [protocol fork provenance](../vendor/README.md)

## Historical Reviews

- [Prototype review response](reviews/review-findings-response.md)
- [Design review round 2](reviews/review-findings-round2.md)
- [Consolidated rounds 3 and 4](reviews/review-findings-round3.md)
- [Final pre-implementation review](reviews/final-preimplementation-review.md)

Component and fixture READMEs stay beside their code. Original benchmark data,
source hashes and validation records remain in `performance-evidence/`.
