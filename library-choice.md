# Library Choice

Status: reviewed decision record. Ecosystem snapshot: 2026-07-25, with the
rusqlite MSRV maintenance pin updated on 2026-08-10.

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
- Linux low-level `io-uring` behind a project-owned disk adapter, Windows
  overlapped/IOCP, and a bounded blocking fallback everywhere.
- libtorrent-rasterbar for BitTorrent in full builds.
- Hyper plus hyper-util for HTTP/1.1 and HTTP/2, with downloader-owned
  connectors, redirect policy, validation, and backpressure.
- rustls by default for TLS and Hickory Resolver for the in-process async DNS
  backend.
- Pinned bounded-parser/logging patches for SuppaFTP 10.0.1 FTP/FTPS transport
  mechanics; downloader-owned validation and storage contracts remain
  authoritative.
- russh plus a pinned inbound-frame patch for russh-sftp 2.3.0 for the
  standard-build SFTP adapter. libssh2 remains an interoperability fallback only
  if the Phase-5 prototype gate fails.
- rusqlite with explicit bundled/backup/cache/limits features on one bounded
  session-store worker thread.
- A dedicated, project-owned Rayon pool for CPU-heavy hashing/parsing work in
  normal split profiles, never the process-global Rayon pool; the explicit
  minimum-thread compact profile may use its bounded shared worker instead.
- quick-xml's streaming reader for Metalink and XML-RPC parsing.
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

- Use the low-level `io-uring` crate on one or more dedicated project-owned ring
  lanes behind `DiskBackend` when the runtime probe succeeds. The lane owns ring
  submission/completion, registered buffers, `OpenAt2`, `AsyncCancel`, fsync,
  and drain-to-one-outcome behavior; crate types never escape the adapter.
- This closes the earlier `tokio-uring`-versus-raw choice. As of 2026-07-25,
  `tokio-uring` remains at 0.5.0 from 2024 while `io-uring` is at 0.7.13 and
  exposes the lower-level operation surface the design must own anyway. Keeping
  a Tokio-shaped file API would not remove the cancellation, secure-open,
  quarantine, and completion-permit work, so it is no longer the production
  default. It remains useful reference/prototype code only.
- `compio` 0.19.1 is active and capable, but its completion runtime/driver stack
  would introduce a second runtime architecture alongside Tokio. It is not a
  baseline dependency; reconsider only a separable driver layer after a
  measured raw-ring defect, not as an implicit wholesale runtime replacement.
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

Binding strategy (checked 2026-07-25): crates.io has no maintained
libtorrent-rasterbar binding — `libtorrent-sys`/`libtorrent` stopped in 2022
and `lt-rs` is a pre-0.2 experiment. The adapter therefore owns its FFI:
a narrow project `bt-libtorrent-sys` crate built with `cxx` (1.0.x) over a
pinned libtorrent 2.x, exposing only the session/torrent/alert surface the
adapter needs. The bridge lives entirely inside the isolated BT lane, follows
the panic/unwind rules for FFI, and is built per target ABI (MSVC libtorrent
for the MSVC artifact, MinGW for the all-MinGW build).

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

Framework-owned memory is an accepted cost of choosing Hyper. Hyper does not
expose one universal client buffer-pool size, but the selected client builder
does expose HTTP/1 exact/max read-buffer settings and HTTP/2 stream/connection
window, adaptive-window, and max-frame settings. The corresponding typed options
in `protocol-modernization.md` are user-visible, included in profile resolution,
and reported with their effective values. Any remaining hidden per-connection
overhead is measured, included in admission sizing/diagnostics, and bounded by
connection/stream limits rather than falsely counted as `BufferPool` memory.

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

Use the ring crypto provider in `minimal`/`standard` by explicitly disabling
dependency defaults and selecting ring consistently in rustls, hyper-rustls,
SuppaFTP, and russh. This keeps one provider and the simpler cross-platform
native build path. An `aws-lc` provider feature is mutually exclusive and
optional for measured performance, post-quantum preference, or a separately
validated FIPS build. CI fails if Cargo feature unification enables both
providers or silently restores a dependency's default provider. The active
provider is visible in diagnostics.

## DNS

Use Hickory Resolver for the in-process async resolver, custom upstreams, TTL
cache policy, and future DoH/DoT feature gates. Keep `system` as a distinct
backend for libc/OS resolver behavior. The public option name is `hickory`;
`trust-dns` may be accepted only as a deprecated compatibility alias because the
project was renamed.

Do not implement a separate c-ares backend in the baseline. The current
`c-ares-resolver`/`c-ares` crates are viable and maintained, but they add a C
library/build/vendoring path while duplicating the custom-upstream, async, and
cache role already owned by Hickory. Keeping both would also double the SSRF,
TTL, cancellation, diagnostics, and cross-target test matrix without supplying
a required compatibility behavior. `cares` is therefore a reserved unsupported
input, not a parsed-only backend; it can be reconsidered only for a measured
resolver/interoperability gap that `system` plus Hickory cannot cover.

DNS answers are inputs to the downloader-owned connector, not authorization by
themselves. The connector pins the selected address through policy validation
and preserves the original hostname for Host and TLS SNI.

## SFTP

Use `russh` plus a project-pinned patched `russh-sftp` for the standard build
because they integrate with Tokio without a native libssh2 dependency. Use the
raw offset request API behind a project-owned bounded pipeline; do not assume the
high-level `AsyncRead` wrapper provides enough concurrent requests for bulk
transfer throughput.

The crates.io `russh-sftp` 2.3.0 client must not ship unchanged. Its public
`Config::max_packet_len` is retained for high-level request sizing, but the raw
client receive loop calls its length-prefixed reader with `u32::MAX`; an
attacker-controlled prefix can therefore allocate far beyond the configured
cap. Its receive loop also logs most framing/parser errors and continues after
the stream is desynchronized. Phase 0 pins a reviewed fork/commit through
`[patch.crates-io]` that threads the configured cap into every inbound packet
read, treats an over-cap or malformed frame as fatal, cancels the channel, and
completes every outstanding request with a typed error. An upstream release may
replace the patch only after the same allocation-before-body, malformed-frame,
pending-request-drain, and cancellation tests pass. The source commit, patch
diff, license, checksum, and SBOM identity are release artifacts.

The patched raw API still returns `SSH_FXP_DATA` in an owned `Vec<u8>`, so the
baseline explicitly budgets the packet buffer plus returned vector and performs
one copy into `BufferLease`; it does not claim direct registered-buffer fill. A
future decoder may remove the copy only behind the same ingress, placement,
rate, and cancellation contracts.

Russh 0.62.4 pins and re-exports `ssh-key` 0.7.0-rc.11. Use that exact
`russh::keys::ssh_key` type/API for OpenSSH public/private keys, certificates,
and `known_hosts` parsing so the baseline has one key representation. This is a
documented, exact-lock prerelease exception inherited from the selected russh
version, not permission for floating prereleases. Keep matching, marker policy,
file-size caps, and task-scoped approval in the downloader adapter; moving to a
stable ssh-key line is a deliberate russh/workspace upgrade with compatibility
tests, not a second direct 0.6.x dependency.

Select russh with `default-features = false` and exactly
`["ring", "flate2", "rsa"]`. Its defaults select aws-lc-rs; the explicit ring
feature aligns the one-provider rule used by rustls and SuppaFTP, while RSA key
support remains available under the adapter's modern-signature algorithm policy.
CI rejects simultaneous ring/aws-lc providers and any accidental DSA/DES legacy
feature.

`ssh2`/libssh2 remains a prototype fallback for interoperability gaps. It is not
linked into the default build, and its seek-based high-level file API is not a
valid segmented-read abstraction while requests are outstanding.

The library choice does not supply the security policy. Host-key verification,
known-hosts handling, authentication ordering, algorithm policy, secrets, proxy
behavior, and rekey/timeouts must be specified in `detailed-ftp-sftp.md` before
Phase 5 begins.

## FTP And FTPS

Use a project-pinned patched SuppaFTP 10.0.1 with its Tokio/rustls-ring feature
for control/data-channel and FTP/FTPS protocol mechanics. It supplies the async
Tokio path, passive/active commands, restart offsets, and explicit/implicit FTPS
integration. The adapter still owns `SIZE`/`MDTM` policy, exact offset/EOF
accounting, binary-mode enforcement, retry classification, rate permits before
data reads, and storage lease checkpoints. Do not expose a generic
remote-filesystem abstraction that hides the control/data connection or
REST/RETR sequence.

The crates.io 10.0.1 control parser and current upstream main must not ship
unchanged: single and multiline replies use `read_until` into growable vectors,
and FEAT accumulates lines without a byte/line cap. The pinned fork replaces
that path with pre-allocation checks: at most 64 KiB per control line, 1 MiB and
4096 lines per complete reply/FEAT response. Bytes reserve
`task_metadata_budget` plus the global resident permit; overflow or malformed
multiline framing closes the control connection with a typed protocol/resource
error. The patch also removes raw command/reply/path/listing logging: wire
serialization is separate from diagnostics, and the dependency may emit only a
safe command verb, status code, and bounded byte/line counts. USER/PASS/ACCT,
SITE/custom arguments, remote paths, welcomes, FEAT text, and data listings are
never formatted into a log record at any level. Directory-list helpers are not
used by the downloader; any future use must receive equivalent data-line/
aggregate bounds. Phase 0 records the source commit, patch diff, checksum,
license, and SBOM identity, and an upstream release may replace it only after
the same prefix/line/FEAT allocation and canary-secret log tests pass. SuppaFTP's
`no-log` feature is not selected because it globally enables `log/max_level_off`
for the workspace rather than fixing the dependency's diagnostics boundary.

The adapter always creates the policy-approved control `TcpStream` through the
downloader connector, then calls `connect_with_stream`; it never calls
SuppaFTP's `connect`/`connect_timeout`, uses the default passive builder, or
enables the NAT-address rewrite. It immediately installs a project-owned
`passive_stream_builder` that applies the approved EPSV/PASV endpoint decision.
The same pinned patch adds an active-data listener hook/predicate because
upstream active mode accepts the first peer with no policy callback. The patched
loop binds only the approved local interface, closes mismatched peers before any
FTPS handshake, accepts the approved control peer until the one deadline, and
stops after 32 rejected peers. Active mode is unavailable through a proxy and
fails explicitly when the local bind/advertised address is not policy-valid.

`async_ftp` is not selected: it provides a smaller async FTP surface, but using
it would not improve the correctness boundary and has a narrower maintained
feature/integration surface than SuppaFTP for this design.

## Session Database

Use `rusqlite` 0.40.2 with `default-features = false` and exactly
`["bundled", "backup", "cache", "limits"]` for reproducible first-slice
desktop builds. `bundled` alone does not expose the hot-backup API, and the
crate's defaults include an unrelated WASM FFI path; the explicit feature set
keeps the native dependency graph and required APIs auditable. All access runs
on one dedicated session-store thread behind a bounded command queue;
synchronous SQLite calls never run on network/control executor threads.
Distributions may add a separately tested system-SQLite build feature later.
The bundled build is compiled with
`-DSQLITE_MAX_LIKE_PATTERN_LENGTH=65536` from repository Cargo configuration so
the required 64 KiB runtime limit is attainable and verified exactly.
The 0.40.2 patch pin selects `libsqlite3-sys` 0.38.2, whose build-script MSRV
shim preserves the declared Rust 1.88 check; 0.40.1/0.38.1 does not compile
that bundled build script on Rust 1.88.

The `minimal` first implementation still includes SQLite. A control-files-only
minimal profile remains deferred until it has its own queue/index/recovery
design and measured binary-size benefit.

## CPU Work Pool

Use a dedicated `rayon::ThreadPool` for pure CPU-heavy jobs such as hashing and
bounded metadata parsing. Admission is controlled by the project's byte/job
budgets, completions return through project-owned bounded lanes, and jobs carry
generation/cancellation metadata. Never initialize or depend on Rayon's global
pool, because an embedding process may already own it. The explicit compact
minimum-thread profile may route these jobs through its one bounded shared
disk/CPU worker instead; diagnostics report that it is not the split Rayon
profile and it carries no C10k claim.

## XML Parsing

Use quick-xml's pull/streaming reader with no Serde DOM for untrusted Metalink
and XML-RPC input. Reject DTD/DOCTYPE, entity declarations, unsupported
encodings, excessive depth/attributes/text, and namespace/element forms outside
the accepted schemas. Parser input and emitted metadata use the exact document,
depth, attribute, file, and source caps in `configuration.md` and reserve
`task_metadata_budget`; the same event-level adapter is fuzzed independently of
networking.

## HTTP/3

Quinn plus `h3`/`h3-quinn` is the selected experiment candidate. It remains
feature-gated and is not part of the first stable baseline: the h3 project still
describes itself as experimental, and the current `0.0.x` API does not justify a
compatibility promise. Shipping requires the Phase-5+ interoperability, proxy,
flow-control, binary-size, and fallback gates.

## Checksums And Digests

Journal CRC-32C: use `crc32c` (0.6.x) — hardware CRC32C on x86_64/aarch64 with
software fallback, tiny dependency surface, `forbid(unsafe_op_in_unsafe_fn)`
discipline, and exactly the one algorithm the journal needs. Its release cadence
is slow but the algorithm is frozen; if a maintenance or performance issue
appears, `crc-fast` (SIMD, all Rocksoft variants, active in 2026) is the
documented replacement behind the same internal `Crc32c` newtype. The generic
`crc` crate is table-based (~0.5 GiB/s) and not selected for the hot journal
path. The first journal slice pins `crc32c` 0.6.8 exactly.

Content digests: RustCrypto `sha2`/`sha1`/`md-5` (0.11.x stable line, `digest`
0.11 traits) for Metalink/aria2 checksum compatibility (`sha-256`, `sha-512`,
`sha-1`, `md5`), with `asm`/hardware features enabled where the target
supports them. SHA-256 for journal `Hash32` uses the same `sha2` crate. ring's
digest API is not used for content checksums: the checksum set is driven by
Metalink metadata, `md5`/`sha-1` must exist for compatibility regardless of
TLS provider, and keeping verification independent of the TLS provider choice
avoids feature-coupling. BLAKE3 is not exposed: aria2 compatibility defines
the accepted checksum vocabulary.
When Metalink offers several algorithms for one range, the project selects
`sha-512 > sha-256 > sha-1 > md5` and never downgrades after a stronger
mismatch; `metalink-chunking.md` owns the persistence/verification rule.

Portable path normalization uses `unicode-normalization` 0.1.25 for NFC before
validation, collision checks, persistence, and display. The first storage slice
pins `sha2` 0.11.0 with default features disabled and
`unicode-normalization` 0.1.25 exactly; both are MIT OR Apache-2.0 and support
the workspace MSRV.

## Timers

Retry, stall, idle-connection, and lease-expiry deadlines use
`tokio_util::time::DelayQueue` (tokio-util 0.7.x, already a Hyper/h2
dependency) — one delay queue per runtime shard as the timer wheel, instead of
one sleeping task per deadline. Plain `tokio::time::sleep` remains fine for
low-cardinality waits. No additional timer crate is added.

## Compression

First slice rejects non-identity content coding for fixed layouts, so
decompression is only for the future growing-sequential path and RPC gzip:

- `flate2` with the default pure-Rust `miniz_oxide` backend for gzip/deflate.
  The `zlib-rs` backend feature is the measured upgrade path (active 2026,
  memory-safe, zlib-ng-class performance) once the growing-layout decode path
  exists; both are API-compatible behind `flate2`.
- `async-compression` is not selected: the decode step sits inside the
  project-owned bounded transform stage (`TransformOwned` buffers), which
  needs explicit size caps and cancellation, not a stream adaptor that hides
  buffer ownership.
- zstd/brotli are out of scope until a compatibility need exists; both would
  add native or large pure-Rust dependencies to `minimal`.

## Platform Syscall Layer

- Unix: `rustix` (1.x) for `openat2`-style secure path resolution, `O_NOFOLLOW`
  open chains, `fallocate`/`posix_fadvise`, `fsync`/`fdatasync`, and directory
  sync. It is memory-safe over raw `libc` and already ubiquitous in the
  dependency graph.
- Windows: `windows-sys` (0.6x) for the overlapped/IOCP adapter, sharing-mode
  file opens, `SetFileValidData`/`SetFileInformationByHandle`, and ACL work.
  `windows-sys` is declaration-only (small compile cost, no COM runtime) which
  fits a narrow platform adapter better than the full `windows` crate.
- The blocking fallback uses `std::fs` plus these crates; no `nix` dependency.

## Cookies, netrc, And URL Handling

- Cookie jar: `cookie_store` (0.22.x) with its `public_suffix` feature (backed
  by `publicsuffix`) for the F14 host-scoping requirement, wrapped behind a
  downloader-owned jar API that enforces the redirect/credential-stripping
  policy and the aria2 `load-cookies`/`save-cookies` formats. The feature alone
  is insufficient: `CookieStore::default()` installs no list and
  `publicsuffix` no longer downloads/bundles one. The repository therefore pins
  a versioned Mozilla Public Suffix List snapshot (source, date, SHA-256, and
  license recorded), parses it at startup, and constructs every jar with that
  list. Parse/availability failure disables cookie use with a typed startup or
  option error; it never falls back to `None`. The active snapshot id/hash is in
  diagnostics and release/SBOM inputs. The raw `cookie` crate alone has no
  storage/matching model. The wrapper owns Netscape/aria2 file parsing because
  `cookie_store`'s native persistence is JSON/RON. `Cookie::matches` does not
  enforce SameSite, so the wrapper also applies the explicit schemeful-site/
  redirect context in `protocol-modernization.md`; `SameSite=None` without
  `Secure` is rejected at ingestion.
- netrc: no maintained crate is adequate (`netrc`/`netrc-rs` dormant for years,
  `rust-netrc` is reqwest-oriented); the format is a ~100-line parser. Implement
  a fuzzed project parser honoring aria2's `.netrc` semantics and permission
  checks.
- URL parsing stays on `url`/`idna`/`percent-encoding` (the WHATWG stack) as
  assumed throughout the design.

## Supply Chain, Licensing, And SBOM

Phase-0 CI artifacts are produced by:

- `cargo-deny` (0.20.x): license allowlist, advisory database, duplicate-crate
  and source policy checks; the committed `deny.toml` is the normative license
  and advisory policy.
- `cargo-audit` (0.22.x): advisory scanning of the committed lockfile
  (redundant with cargo-deny's advisories but kept for RustSec tooling
  compatibility in scheduled CI).
- `cargo-auditable` (0.7.x): embeds the dependency list into release binaries
  so deployed artifacts can be audited against advisories.
- `cargo-cyclonedx` (0.5.x): CycloneDX SBOM per release profile, alongside the
  `cargo tree` text dump recorded per artifact. (SPDX via `cargo-sbom` can be
  added on distributor request; CycloneDX is the primary format.)
- `cargo-vet` is not adopted initially; the audit burden does not fit the
  project size. Revisit if the dependency graph or contributor base grows.

The workspace forbids git dependencies in release profiles, pins the toolchain
via `rust-toolchain.toml`, commits `Cargo.lock`, and vendors crates for release
tarballs per the README build rules.

## Ancillary Choices

- CLI parsing: `clap` v4 with derive, generated from the option registry so
  help text and the registry cannot drift.
- Serialization: `serde`/`serde_json` for RPC, exports, and generated
  matrices. The control journal uses the hand-written versioned codec in
  `detailed-storage.md`, never serde.
- WebSocket events: `tokio-tungstenite` (0.30.x) behind the bounded
  client-event queues.
- Errors/diagnostics: `thiserror` for typed errors; `tracing` with
  `tracing-subscriber` for structured logs honoring the redaction rules.
- Secrets: `secrecy` (0.10.x) plus `zeroize` for the `Secret<T>` wrapper
  contract in `security-recovery.md`.
- Time: `std::time` plus `httpdate` for HTTP date parsing; jittered backoff
  uses `fastrand` (no crypto RNG requirement); `chrono`/`time` are avoided in
  the core to keep the dependency tree small unless a formatting need appears.
- Rate limiting is project-owned (`rate-limiting.md`): `governor` implements
  GCRA cell-based limiting, not the required hierarchical debit-at-read token
  buckets with fair queued permits, deficit round-robin, and runtime
  re-parameterization; a wrapper would replace most of its logic anyway.

## Toolchain And Target Baseline

- Rust edition: 2024.
- Bootstrap toolchain: pin Rust `1.97.1` in `rust-toolchain.toml` and CI images,
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

Exact direct versions selected by the review and subsequent compatibility
maintenance through 2026-08-10 are a controlled snapshot, not unconstrained
version requirements: Hyper 1.11.0, hyper-util
0.1.20, hyper-rustls 0.27.9, Hickory Resolver 0.26.1, rustls 0.23.42 stable,
Tokio 1.53.1, tokio-util 0.7.19, russh 0.62.4, russh-sftp 2.3.0 (patched),
ssh-key 0.7.0-rc.11 (exact russh dependency), SuppaFTP 10.0.1 (patched),
quick-xml 0.41.0, tokio-uring 0.5.0, io-uring 0.7.13, rusqlite 0.40.2,
Rayon 1.12.0, Quinn 0.11.11, h3 0.0.8, h3-quinn 0.0.10, crc32c 0.6.8,
sha2 0.11.0, sha1/md-5 0.11.x, unicode-normalization 0.1.25,
cookie_store 0.22.1, rustix 1.1.4, windows-sys 0.61.2,
flate2 1.1.9, tokio-tungstenite 0.30.0, clap 4.6.x, serde 1.0.x, secrecy
0.10.3, zeroize 1.9.0, cxx 1.0.x, cargo-deny 0.20.2, cargo-audit 0.22.2,
cargo-auditable 0.7.5, and cargo-cyclonedx 0.5.9. Phase 0 must pin, audit,
license-check, and build the resolved graph rather than copying this list
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
