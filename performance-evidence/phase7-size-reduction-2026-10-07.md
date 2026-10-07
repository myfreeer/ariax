# Validated Binary Size Reductions

The full/OpenSSL CLI is smaller on both supported local hosts after applying
bounded changes from the [dependency analysis](phase7-binary-size-2026-10-07.md).
The two final builds replace the retained package identities. Dependency versions,
optimization levels, supported protocol behavior and cryptographic algorithms
are unchanged. This is local validation, not P7 release approval.

| Target | Before, Bytes | After, Bytes | Saved |
| --- | ---: | ---: | ---: |
| Linux x86-64 | 24,991,648 | 21,603,400 | 3,388,248 (13.56%) |
| Windows GNU x86-64 | 25,228,288 | 24,444,416 | 783,872 (3.11%) |

Exact input hashes, native provenance, executable hashes, test results and raw
measurement samples are in the [machine-readable evidence](phase7-size-reduction-2026-10-07.json).
The source is the hash-bound worktree on analysis checkpoint
`ccd587b65463f56227802f030ebae200a4724de0`. These are fresh local builds with reused Rust
intermediates, not independently reproduced artifacts.

## Adopted Changes

- Compile GNU native libraries with function/data sections so the existing
  linker garbage collection can remove unreferenced functions independently.
  Native optimization levels, exception handling and callback registrations
  remain intact.
- Build native OpenSSL with `no-quic`. P7 exposes TLS over TCP and has no
  OpenSSL QUIC transport. TLS-facing hooks with QUIC names may remain;
  this does not remove every QUIC-named object or disable crypto algorithms.
- Construct and probe only the existing X25519/P-256/P-384 rustls/OpenSSL
  groups, in the same order. Cipher and signature availability filtering stays
  with the provider. Excluded hybrid groups no longer need speculative keys.
- Disable bundled SQLite FTS3/FTS4/FTS5 and RTree. Supported session schemas
  use ordinary strict tables/indexes; their format and migration paths are
  unchanged. Thread safety, API armor, STAT4, backup, transaction and durability
  behavior remain enabled. User-created virtual tables are outside the format.
- Pack relative relocations on Linux OpenSSL release-helper CLI links. The
  loader format requires glibc 2.36, below these artifacts' existing 2.38 floor.
  Ring-only smaller bundles retain their glibc 2.34 policy; Windows gets no
  ELF relocation flags. Build comparison/cache validation records this policy.

The table measures the combined changes. The earlier same-object experiment
isolated 1,097,880 bytes of Linux relocation savings; the remaining changes
were validated together, so their individual byte contributions are not claimed.
Native archives may grow from section metadata even while the final executable
shrinks. Final executable size is the optimization target.

Linux retains its exact system import set and glibc 2.38 / GLIBCXX 3.4.30
requirements, with `GLIBC_ABI_DT_RELR` recorded. Windows still imports only its
12 Windows-provided DLLs and requires no C++/GCC/OpenSSL runtime DLL distribution.
Both executable path audits pass. The lockfile and notice inventory are unchanged.

## Correctness And Packaging

Focused checks passed on both hosts:

| Check | Linux | Windows GNU |
| --- | ---: | ---: |
| Native callback, storage, boundary and TLS tests | 5 | 5 |
| SQLite session store | 90 | 89 |
| Journal recovery | 19 | 19 |
| HTTP/TLS | 11 | 11 |
| FTP | 4 | 4 |
| SFTP | 1 | 1 |
| Native FFI | 2 | 2 |
| BitTorrent adapter | 4 | 4 |
| CLI smoke | 14 | 14 |
| Contract generator | 35 | 35 |

The four ignored session-store entries are existing subprocess helper entry
points, exercised through their parent tests. Native TLS tests explicitly cover
TLS 1.2/1.3 trusted payload exchange and untrusted-certificate rejection.
SQLite tests cover rollback, rejected disabled modules, supported migrations,
backup/recovery and retained safety/planner options. Provider tests compare the
selected cipher/signature availability with the upstream provider.

Focused engine/storage/generator Clippy checks passed with warnings denied on
both hosts. Native-build policy tests (18), release-tool tests (41), protocol
feature selections and the SQLite feature-closure check passed. The generator's
stale synthetic aria2 fixture was completed with the registry-declared option
names; parser-specific fixture checks remain separate.

Each old executable created a session containing three paused tasks with
distinct output options. Its replacement reopened that database and preserved
all three tasks and options. Both staged packages passed notice/hash inventory,
reduced-environment help, live RPC/features, unknown-method rejection, EOF
shutdown and database reopening. Windows loaded only application/system modules.
Linux's pre-existing host preload remains recorded and unchanged. These checks
do not establish fresh/minimum-OS acceptance.

Linux filesystem test fixtures initially exposed the artifact NTFS mount's lack
of POSIX mode bits. Runtime-only fixtures were moved to physical `/run/shm`;
`/dev/shm` was correctly rejected as a symlink by safe-path checks. Compiler
scratch stayed on the artifact drives. Corrected fixture runs passed; the failed
harness attempts are not attributed to production regressions.

## Bounded Performance Comparison

Old and new executables ran sequentially after both compiler workers finished.
Each HTTP and HTTPS case transferred 16 MiB from a loopback range-capable server,
with one warmup and nine measured samples per executable. Ordering alternated
old/new and new/old. Every downloaded payload passed its SHA-256 check.
The HTTPS server used the existing repository test certificate and an explicit
trust anchor. Startup used one warmup and 30 paired `--version` measurements.

| Host | Median Measurement | Before | After |
| --- | --- | ---: | ---: |
| Linux | HTTP, MiB/s | 70.33 | 74.28 |
| Linux | HTTPS, MiB/s | 29.83 | 30.32 |
| Linux | Startup, ms | 9.86 | 11.41 |
| Windows GNU | HTTP, MiB/s | 63.63 | 65.56 |
| Windows GNU | HTTPS, MiB/s | 39.46 | 40.37 |
| Windows GNU | Startup, ms | 10.67 | 10.30 |

These are warm local wall-clock checks, including filesystem and process
costs. They do not isolate individual optimization effects, establish every
protocol's throughput, or resolve the deferred tail-latency failures.

The initial Linux startup median rose from 9.86 to 11.41 ms, with substantial
sample dispersion. A focused repeatability check ran three further batches of
100 alternating pairs with five warmups per executable per batch. Old/new
medians were 8.77/8.39, 8.24/7.84 and 8.25/7.67 ms. The initial slowdown did
not repeat; all three paired median differences favored the smaller binary
by 0.39–0.53 ms. Both the original samples and all repeats are retained in the
evidence. This supports no observed repeatable startup regression on this host,
not a claim of a universal startup speedup.

## Remaining Candidates

The original analysis found no conflicting strong global definitions and one
native OpenSSL implementation. Five multiversion runtime dependency families
remain for incompatible upstream APIs; a repeated package name is not proof of
duplicated live machine code. No lockfile-forced unification is justified.

Safe identical-code folding saved zero bytes and is not enabled. Aggressive
folding can change function identity. Journal specialization and FTP regex code
are live; replacing them needs dedicated replay/parser/performance work.
SSH AEAD still uses ring, so removing it requires a real cipher-backend change.
Size-oriented optimization, executable compression, unwind removal and Windows
runtime DLL substitution are not adopted.

The existing RSA advisory disposition, full release matrix, fresh/minimum-OS
acceptance, independent reproducibility, deferred tail-latency failures and VM
power-loss campaign are unchanged. No new sanitizer/std build, CI dispatch,
push, tag or release was performed for this work.

## Artifact Disposition

Cleanup removed **12,089,592,930 logical file bytes**, retaining the
toolchains/dependencies and one current release build root per host. Obsolete
debug/lint builds, previous executable copies, native build sources/intermediates,
temporary packages, test outputs and raw measurement logs were removed after
their useful results were consolidated here. Current executable and native
archive hashes were verified again after cleanup. No saved binary collection
or filesystem compression is used. Detailed counts are in the machine-readable
evidence; logical file bytes are not an exact filesystem-space measurement.
