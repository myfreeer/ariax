# Library Choice

Status: draft.

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
- Platform disk backends for true async file I/O where available.
- libtorrent-rasterbar for BitTorrent in full builds.
- rustls by default for TLS.
- Feature-gated SFTP through libssh2 or russh after prototype benchmarks.

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
- a reserved MPSC completion drain (or one SPSC per worker) so accepted disk
  operations can always return their outcome and lease.

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

- Prefer io_uring when runtime probe succeeds.
- Fall back to bounded blocking pool.

Windows:

- Prefer overlapped file I/O/IOCP.
- Fall back to bounded blocking pool.

macOS/BSD:

- Use kqueue for network readiness through Mio/Tokio.
- Use bounded disk pool for regular files unless a better maintained backend is
  proven.

This avoids blocking network runtime threads while still preserving portability.

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

HTTP implementation can use hyper or another maintained Rust HTTP stack, but
the downloader must not delegate correctness blindly.

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

## TLS

rustls is the default:

- memory-safe Rust implementation,
- small enough for static builds,
- good cross-platform behavior.

Native TLS is feature-gated where users need OS certificate behavior or
enterprise stores.

## References

- Tokio docs: https://docs.rs/tokio/latest/tokio/
- Mio `Poll` implementation notes: https://docs.rs/mio/latest/mio/struct.Poll.html
- tokio-uring: https://github.com/tokio-rs/tokio-uring
- io-uring crate: https://docs.rs/io-uring/latest/io_uring/
- libtorrent features: https://libtorrent.org/features.html
