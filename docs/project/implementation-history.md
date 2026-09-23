# Implementation History

[Documentation](../README.md)

This checkpoint narrative was retained from the former root README after the September 22, 2026 CI baseline. It records that milestone; ongoing work and remaining gates are tracked in [implementation readiness](implementation-readiness.md) and the [implementation plan](implementation-plan.md).

Phase 5 implementation and local validation are complete at `88d1a83`:
Metalink v3/v4, FTP/FTPS, SFTP, four content digests and self-contained Metalink
JSON migration use the shared scheduler and public interfaces. All six
[shared protocol transfer gates](../protocols/detailed-protocol-transfers.md) pass locally.
The [validation record](../../performance-evidence/phase5-validation-2026-09-15.md)
includes both local platform suites, OpenSSH interoperability, ten bounded
fuzz targets and five passing native Windows benchmark scenarios. The
[September 22 CI baseline](../../performance-evidence/ci-baseline-2026-09-22.md)
passes at `af5d193`, including the full platform/feature/MSRV matrix and all five
native Linux benchmark scenarios. Phase 6 BitTorrent implementation is next.

Status: overall implementation is underway. The scoped Phase-3B/3C HTTP(S)
downloader milestone is implemented and checkpointed at `7adddc1`, and the
Phase-4 control-plane checkpoint is executable at `ec415ff`, with the Phase-4B
implementation and native Windows benchmark evidence at `f9edb5c`. Phase 0
through the core Phase-1/2
scheduler, configuration, journal, SQLite session owner, bounded runtime,
descriptor-safe storage, startup recovery, and platform capability work have
executable checkpoints. The completed Phase-3B checkpoint adds the first public
multi-mirror HTTP(S) vertical slice: atomic task/source/option admission,
source-aware recovery, verified TLS 1.2/1.3, bounded HTTP/1.1 reuse,
policy-owned Hickory/system DNS caching and singleflight, generated SSRF
classification, deterministically timed two-racer Happy Eyeballs, changed-answer
reconnect revalidation, redirects, HTTP CONNECT/forward and SOCKS5 proxies,
Basic/private-netrc credentials, and a bounded cookie jar using a hash-pinned
Mozilla Public Suffix List. Scheduler-owned workers coordinate
non-overlapping range leases across mirrors, enforce strict response placement,
retry within total/per-source budgets, preserve durable pieces across restart,
and publish packet-independent speed, connection, retry, discard, and durable
progress counters. The experimental CLI exposes the real scheduler through a
shared bounded JSON-RPC dispatcher over loopback HTTP/1.1, loopback WebSocket,
and Content-Length/NDJSON stdio, plus direct add/status/pause/resume/remove commands.
The dispatcher enforces method tokens, batch/multicall/list/response bounds,
unique GID prefixes, typed option/source mutation, config/session extensions,
and orderly worker/journal/session shutdown. A bounded event broker supplies
scheduler-observed aria2 notifications and coalesced status updates, and a
typed Rust embedding API uses the same control plane. Phase 4B repairs
multicall envelope authentication, connection-local event authorization, and
production-policy retry admission and recovery, active option journal replay,
live-only rate changes, and source replacement after cancellation drain.
Shared RPC reservations, transport ownership, borrowed result preflight, and
typed input/native projection accounting are implemented. Scheduler simulations
and status drafts reserve before mutation and retain credit through pending
driver work. The [repair evidence](implementation-readiness.md#phase-4-repair-gates)
records these changes; sanitized atomic session import/export and configured
explicit, periodic, and shutdown saves now share the CLI/Rust/RPC control plane.
Versioned configuration reload, bounded URL rules, redacted dumps, real option
restarts, combined transports, compatibility modes and opt-in slow-slot/retry
scheduling are implemented and tested. P4-11 control progress at `8fefde2` adds
immutable query projection outside the control owner, one managed
native/transport runtime, nonblocking
persistence and admission preparation, and bounded bulk continuations with
later per-task controls taking precedence. Queries, queued commands and
accepted durable work retain their budgets through cancellation and disconnect.
Native Linux benchmark acceptance now passes at `af5d193` with the retained
September 22 CI campaign. The expanded Windows campaign passes 20,000 calls per transport
under 1,000 active ranges plus separate administrative measurements; worst
operation p99 is 37.543 ms and the longest burst is 421 ms. The
[recorded evidence](../runtime/performance-profiles.md#native-windows-control-plane-evidence)
separates the expanded Windows campaign from historical results and the
remaining platform gates.

Strict HTTP identity now distinguishes whole-file and exact-range evidence. A
persisted user SHA-256 admits ordinary concurrent mirrors and final verification;
the bounded SHA-256 `Repr-Digest` profile verifies probe/range bodies and keeps
secondary origins only for fenced exact-range endgame races. Its persisted
digest/length identity also makes restart fail closed: matching mirrors are
reprobed and each locally verified durable range is fetched and compared again
before pending work resumes.

This is still not a complete downloader or release/tag-ready. The first
hierarchical rate limiter, stall policy, process-owned discard guard, same-origin
endgame fencing, bounded exact-range cross-origin digest fencing, and local
capacity benchmarks are executable; HTTP/2, unknown-length/chunked layouts,
broader RFC 9530 and `Content-Digest` modes,
non-loopback RPC,
and the full native release matrix remain
gated by [implementation-readiness.md](implementation-readiness.md)
and [implementation-plan.md](implementation-plan.md). Deterministic ENOSPC, permission-denied,
short-write, partial-fsync, torn-tail, and same-inode publication faults are
executable; publication residue is removed only after full header/linkage
replay succeeds. Child-process exit/kill barriers and the Linux durable-prefix
power-loss cut model are executable too. Hot-backup publication residue now
recovers through exact same-file/link-count proof and preserves raced
destinations. The minimal real-process shutdown path now stops admission,
boundedly drains HTTP workers, flushes and closes every installed journal, and
publishes a clean session marker only after the session owner joins; worker or
lane timeout leaves an explicit dirty marker. Optimized Linux and native
Windows-GNU capacity runs now exercise 10,000 low-activity sockets and 1,000
active ranges under the shared permits. Retry waits now expose one
bounded, non-secret, lease-correlated decision through `tellStatus`, with exact
live cause/status/caps/action and explicitly coarser recovered journal evidence.
Real poweroff and the complete release matrix remain required before a
completion or tag claim.
