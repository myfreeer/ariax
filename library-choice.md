# Library Choice

Status: reviewed decision record. Ecosystem snapshot: 2026-07-25.

The downloader should reuse mature infrastructure, but the core should still
own correctness-sensitive behavior: range validation, disk placement,
recovery, scheduler state, and option semantics.

## Summary

Chosen stack:

- Rust for core engine and protocol glue.
- Tokio plus Mio for default async runtime and network readiness.
- First-slice bounded queues: Tokio channels for async/control lanes and
  crossbeam-channel for blocking worker pools. Thingbuf/rtrb are optional
  post-baseline substitutions only for a measured, topology-proven hot lane.
- Linux `tokio-uring` behind a project-owned disk adapter, Windows
  overlapped/IOCP, and a bounded blocking fallback everywhere.
- libtorrent-rasterbar for BitTorrent in full builds.
- Hyper plus hyper-util for HTTP/1.1 and HTTP/2, with downloader-owned
  connectors, redirect policy, validation, and backpressure.
- rustls by default for TLS and Hickory Resolver for the in-process async DNS
  backend.
- russh plus russh-sftp for the standard-build SFTP adapter. libssh2 remains an
  interoperability fallback only if the Phase-5 prototype gate fails.
- rusqlite on one bounded session-store worker thread.
- A dedicated, project-owned Rayon pool for CPU-heavy hashing/parsing work;
  never the process-global Rayon pool.
- Quinn plus h3/h3-quinn as the experimental HTTP/3 candidate only.

## Runtime Candidates

Tokio/Mio:

- Best fit for a Rust core.
- Mature executor, timers, channels, task supervision, TCP/UDP integration, and
  ecosystem support.
- Mio maps to platform selectors including epoll on Linux, kqueue on macOS/BSD,
  and IOCP on Windows.
- Weakness: regular file I/O is not uniformly true async on every platform, so
  the design adds explicit disk backends and a bounded blocking fallback.

libuv:

- Mature and portable, especially for C/C++ and Node-style event loops.
- Uses platform backends and has a familiar cross-platform abstraction.
- Not selected as the core runtime because Rust integration would either add an
  FFI boundary to every hot I/O path or force a C/C++ core. It remains a viable
  fallback backend only if a future C ABI layer needs it.

Boost.Asio / standalone Asio:

- Mature and battle-tested in C++ networking.
- A strong choice if the project were a C++ rewrite.
- Not selected for a Rust core because it would move scheduler and protocol
  ownership across FFI or require a C++ core, weakening the memory-safety goal.
- libtorrent can still use its own Asio-based internals behind the BT adapter.
  See `libtorrent-integration.md` for the isolation model.

libevent:

- Mature readiness event library.
- Good for C code and lightweight servers.
- Not selected because it is lower-level than Tokio for this design, lacks the
  Rust task ecosystem integration, and does not solve disk I/O or structured
  cancellation by itself.

Custom reactor:

- Rejected for the first production implementation. It would increase risk and
  duplicate mature community work.

## Queue And Messaging Choice

The Rust decision depends on using the right messaging primitives. The design
does not put payload bytes through generic async channels.

First slice:

- `tokio::sync::mpsc`/`oneshot`/`watch` for control-plane async commands,
- bounded Tokio MPSC behind project wrappers for transfer submission,
- `crossbeam-channel::bounded` for blocking OS-thread worker pools,
- one bounded Tokio MPSC `CompletionDrain` with a move-only permit reserved for
  each accepted operation, so disk/CPU work can always return its outcome and
  lease. Per-worker SPSC drains are post-baseline optimizations only.

After measurement, `thingbuf::mpsc` may replace a hot MPSC lane and `rtrb` may
replace a proven one-producer/one-consumer lane. `concurrent-queue` is not a
dependency until an owned topology demonstrates why a channel wrapper is
insufficient.

Payload memory lives in `BufferPool`. Queue messages carry `BufferLease`
descriptors and offsets, so the hot path is shared memory plus ownership
transfer rather than copied buffers. See `messaging-model.md`.

## Disk I/O

No single library gives ideal cross-platform async disk behavior today, so disk
I/O is selected through the concrete `DiskBackendKind` enum. The downloader's
storage engine is project-owned; low-level platform I/O is delegated to small
wrappers or OS APIs. See `detailed-runtime.md` and `disk-adapter.md`.

Linux:

- Use `tokio-uring` on one or more dedicated current-thread disk lanes behind
  the project-owned `DiskBackend` API when the runtime probe succeeds. Its
  ownership-returning buffer operations fit the `BufferLease` contract.
- Keep the low-level `io-uring` crate as an internal replacement candidate if
  the prototype cannot meet explicit cancellation, secure-open, or quarantine
  requirements. Neither crate's types escape the platform adapter.
- Fall back to bounded blocking pool.

Windows:

- Prefer overlapped file I/O/IOCP.
- Fall back to bounded blocking pool.

macOS/BSD:

- Use kqueue for network readiness through Mio/Tokio.
- Use bounded disk pool for regular files unless a better maintained backend is
  proven.

This avoids blocking network runtime threads while still preserving portability.

Secure path resolution and file opening remain project-owned. On Linux, resolve
and open through the `rustix`/`openat2`-style safe-path layer first, then hand the
already-open descriptor to the disk adapter. Accepted kernel operations are
drained to exactly one completion even after cancellation; dropping an in-flight
future is not the cancellation protocol.

`tokio::fs` is not selected as the transfer-data disk adapter. It is suitable
for small metadata/config operations, but the transfer path needs explicit
offset writes, allocation, fsync, queue limits, and backend capability
reporting.

## BitTorrent

libtorrent-rasterbar is selected for the full build because it already provides
a feature-complete and scalable BitTorrent implementation with DHT, PEX,
magnet metadata, HTTP seeding, disk cache, and resume support.

The downloader still controls:

- safe path mapping,
- user-visible queueing,
- RPC state,
- selected files and output roots,
- final result persistence,
- option compatibility.

Libtorrent is intentionally outside the main event loop. The bridge uses
bounded command/event channels so BT swarm activity cannot block RPC, HTTP
downloads, or queue maintenance.

## HTTP Client

Use Hyper directly for HTTP/1.1 and HTTP/2, with hyper-util for connection/client
utilities and hyper-rustls for TLS integration. Reqwest is intentionally not the
engine client: its higher-level redirect, connector, proxy, and body policies
would have to be bypassed for this downloader's reserved-header authority,
destination pinning, exact range validation, and lane-specific backpressure.

Hyper body frames are immutable `Bytes` owned by the HTTP stack. The baseline
therefore permits one explicit, bounded copy from a body frame into a
`BufferLease`; it does not claim that the socket fills registered storage buffers
directly. The HTTP ingress frame budget is separate from the storage buffer-pool
budget, and the adapter stops polling a body when downstream queue, memory, or
rate credit is unavailable. A future foreign-buffer lease may remove that copy
for backends that do not require registered or mutable buffers.

Downloader-owned checks:

- `206` required for range segments,
- `Content-Range` exact validation,
- exact body length,
- redirect policy,
- resume validators,
- content encoding constraints,
- proxy/no-proxy semantics at the final connection hop,
- generated reserved-header authority and redirect revalidation,
- provisional lease commit/abort before durability.

The connector is downloader-owned so DNS results, Happy Eyeballs attempts,
proxy policy, TLS SNI/Host identity, and SSRF destination pinning remain one
auditable operation.

## TLS

rustls is the default:

- memory-safe Rust implementation,
- small enough for static builds,
- good cross-platform behavior.

Native TLS is feature-gated where users need OS certificate behavior or
enterprise stores.

Use the latest stable rustls `0.23.x` line selected by the workspace lockfile;
do not adopt a `0.24.0-dev` prerelease in the production baseline. Desktop
builds use the platform trust-verifier integration where supported, while custom
CA modes remain downloader-owned policy.

## DNS

Use Hickory Resolver for the in-process async resolver, custom upstreams, TTL
cache policy, and future DoH/DoT feature gates. Keep `system` as a distinct
backend for libc/OS resolver behavior. The public option name is `hickory`;
`trust-dns` may be accepted only as a deprecated compatibility alias because the
project was renamed.

DNS answers are inputs to the downloader-owned connector, not authorization by
themselves. The connector pins the selected address through policy validation
and preserves the original hostname for Host and TLS SNI.

## SFTP

Use `russh` plus `russh-sftp` for the standard build because they integrate with
Tokio without a native libssh2 dependency. Use the raw offset request API behind
a project-owned bounded pipeline; do not assume the high-level `AsyncRead`
wrapper provides enough concurrent requests for bulk transfer throughput.

`ssh2`/libssh2 remains a prototype fallback for interoperability gaps. It is not
linked into the default build, and its seek-based high-level file API is not a
valid segmented-read abstraction while requests are outstanding.

The library choice does not supply the security policy. Host-key verification,
known-hosts handling, authentication ordering, algorithm policy, secrets, proxy
behavior, and rekey/timeouts must be specified in `detailed-ftp-sftp.md` before
Phase 5 begins.

## Session Database

Use `rusqlite` with bundled SQLite for reproducible first-slice desktop builds.
All access runs on one dedicated session-store thread behind a bounded command
queue; synchronous SQLite calls never run on network/control executor threads.
Distributions may add a system-SQLite build feature later.

The `minimal` first implementation still includes SQLite. A control-files-only
minimal profile remains deferred until it has its own queue/index/recovery
design and measured binary-size benefit.

## CPU Work Pool

Use a dedicated `rayon::ThreadPool` for pure CPU-heavy jobs such as hashing and
bounded metadata parsing. Admission is controlled by the project's byte/job
budgets, completions return through project-owned bounded lanes, and jobs carry
generation/cancellation metadata. Never initialize or depend on Rayon's global
pool, because an embedding process may already own it.

## HTTP/3

Quinn plus `h3`/`h3-quinn` is the selected experiment candidate. It remains
feature-gated and is not part of the first stable baseline: the h3 project still
describes itself as experimental, and the current `0.0.x` API does not justify a
compatibility promise. Shipping requires the Phase-5+ interoperability, proxy,
flow-control, binary-size, and fallback gates.

## Toolchain And Target Baseline

- Rust edition: 2024.
- Bootstrap toolchain: pin Rust `1.97.0` in `rust-toolchain.toml` and CI images,
  then update deliberately with `Cargo.lock` and dependency-audit changes.
- Initial declared MSRV: `1.88`, because Hickory Resolver `0.26.1` requires it;
  CI must test the MSRV if the project promises it.
- The available WSL Rust `1.85.0` can inspect and run experiments that do not
  include the full dependency set, but it cannot be the declared build baseline.
- Primary Windows release ABI: `x86_64-pc-windows-msvc`. The
  `x86_64-pc-windows-gnu`/MSYS2 toolchain is a supported secondary build only
  when every native dependency, especially libtorrent, uses the same MinGW ABI.
- CLI-only packaged artifacts may use `panic=abort`. Any C ABI/staticlib/cdylib
  artifact that promises panic containment must use an unwind-capable profile
  and catch panics at every exported boundary.

Exact direct versions observed during the 2026-07-25 review are a research
snapshot, not unconstrained version requirements: Hyper 1.11.0, hyper-util
0.1.20, Hickory Resolver 0.26.1, rustls 0.23.42 stable, russh 0.62.4,
russh-sftp 2.3.0, tokio-uring 0.5.0, io-uring 0.7.13, rusqlite 0.40.1,
Rayon 1.12.0, Quinn 0.11.11, h3 0.0.8, and h3-quinn 0.0.10. Phase 0 must pin,
audit, license-check, and build the resolved graph rather than copying this list
blindly.

## References

- Tokio docs: https://docs.rs/tokio/latest/tokio/
- Mio `Poll` implementation notes: https://docs.rs/mio/latest/mio/struct.Poll.html
- tokio-uring: https://github.com/tokio-rs/tokio-uring
- io-uring crate: https://docs.rs/io-uring/latest/io_uring/
- libtorrent features: https://libtorrent.org/features.html
- Hyper body API: https://docs.rs/hyper/latest/hyper/body/struct.Incoming.html
- Hickory Resolver: https://docs.rs/hickory-resolver/latest/hickory_resolver/
- russh: https://docs.rs/russh/latest/russh/
- russh-sftp: https://docs.rs/russh-sftp/latest/russh_sftp/
- rusqlite: https://docs.rs/rusqlite/latest/rusqlite/
- Rayon thread-pool builder: https://docs.rs/rayon/latest/rayon/struct.ThreadPoolBuilder.html
- h3 project status: https://github.com/hyperium/h3
