# Continuous Integration

[Documentation](../README.md)

Status: the remote and CI baseline passes at `af5d193`.
[Run 35720518419 and its retained evidence](../../performance-evidence/ci-baseline-2026-09-22.md)
cover every required platform, feature and MSRV job and all five native Linux
benchmark scenarios. The prerequisite is complete; Phase 6 may proceed.

## Repository And Branch

The canonical remote is `git@github.com:myfreeer/ariax.git`. Publish the
existing history on `main`, which is the remote's default branch. CI runs on
pushes and pull requests with read-only repository permissions. Routine branch
and PR runs validate Linux and Windows MSVC. Pushes to `main`, tag pushes and
manual workflow dispatch select the full matrix. A newer run for the same ref
and validation scope cancels its predecessor; a manual full run is independent
of an automatic routine run.

## Fail-Fast Topology

The workflow has three ordered stages:

1. Preflight validates documentation links and navigation, formatting, workflow
   syntax, whitespace, generated contracts, pinned reference inputs, protocol
   forks and feature policies.
2. One event-selected validation matrix contains the required jobs. Routine
   runs retain the complete Linux and Windows MSVC checks, including native
   probes and default/all-feature workspace tests. Full runs additionally
   include macOS, Windows GNU, both MSRV targets, all CLI feature bundles and
   the sanitizer/fuzz job.
   The four CLI feature bundles run sequentially in one job and share Cargo
   outputs; each still runs its own tests and `release-cli` build.
   Its `fail-fast: true` cancels sibling jobs after a failure. Every script
   propagates native-command and pipeline failures; no required check uses
   `continue-on-error` or automatic job/test retry. Native archive downloads
   have bounded retries for transient transport failures; integrity failures
   and exhausted downloads still fail provisioning.
3. A successful push to `main` runs the reusable native Linux benchmark
   workflow against the same commit. Pull requests run functional validation;
   manual benchmark dispatch remains available for investigations.

The final `CI Required` job passes only if every stage required by the event
passes. A failed, cancelled or unexpectedly skipped required stage cannot
produce a green aggregate result. Job timeouts bound stuck work. Cleanup
uploads available logs even on failure or cancellation.
Routine success is not evidence that the full matrix passed. Run the full
workflow manually on the candidate commit before release approval; the explicit
release tag gate remains closed. Main pushes still require the full matrix and
native acceptance benchmarks.

Full builds, tests, native provisioning and benchmark collection run in CI.
Local documentation checks and focused debugging should avoid recreating the
platform matrix or accumulating native build artifacts. See the
[development guide](README.md) for the lightweight local workflow.

## Reproducible Validation

Install Rust 1.97.1 on each fresh runner and check the supported graph with
Rust 1.88.0. CI helpers use Python 3.13, or MSYS2's native Python for GNU jobs.
Use the locked dependency graph.

JavaScript actions in both workflows target Node.js 24 and use full commit
pins. Preflight, every validation matrix job and native benchmark collection
share the same Node.js 24 artifact uploader.

Native installation cache keys identify the platform ABI and instrumentation,
not the Rust feature bundle or Rust version. Only the dependency manifest,
patches and native builder invalidate those keys; changes to probes do not
rebuild dependencies. A successful explicit native verification permits saving
that installation even if a later test fails. Ordinary and sanitizer builds,
and Windows MSVC and GNU, retain separate caches.
Cargo caches use a commit-specific key with a toolchain/dependency-compatible
restore prefix, so successful compilation work can be updated across commits.
Validation and benchmark jobs save that work even after test failure; cancelled jobs do not
spend additional time uploading caches. Restored native files are still fully
verified, and Cargo validates its own build fingerprints before reuse.

The checkout must supply all
tracked fixtures and pinned fork sources; ignored workstation toolchains,
cached references and absolute local paths are not CI inputs.
Git attributes select LF for text sources and generated contracts and preserve
the exact bytes of integrity-pinned policy inputs, protocol forks and parser
seeds on every platform. Windows checkout newline conversion must not change
an integrity-pinned input or weaken its runtime hash check.
On Unix, CI resolves the runner's temporary-directory alias before selecting
the fixture parent. This avoids macOS system aliases without relaxing the
application's rejection of symlinks in persistence paths. Test output is
streamed immediately so a later blocked fixture cannot hide an earlier panic.

Before initial publication, preserve an ignored local history bundle and
sanitize every reachable commit, including historical file versions and commit
messages. Remove machine-specific paths and local-only files; prune commits
that become empty while retaining portable implementation and validation work.
Push only the sanitized `main` branch. Historical benchmark measurements remain
unaltered; replace workstation paths in invocation records with portable
placeholders and record that redaction explicitly.
Run `python3 -B scripts/publication.py --history` before the first push;
preflight also checks the tracked tree to prevent new workstation paths or
local configuration from entering CI.

Local WSL wrappers discover native tools from `PATH` or explicit overrides.
An optional ignored `toolchains/local-paths.json` can provide `msys2_root` and
`windows_system32` directories in the calling environment's path syntax.
`ARIAX_MSYS2_ROOT` and `ARIAX_WINDOWS_SYSTEM32` override those values. Missing
or invalid tools fail with a configuration error. Checkout, toolchain and
output paths are derived from the repository location, never a drive letter
or a particular developer's home directory.

Validation covers Linux, macOS, Windows MSVC and Windows GNU, default and
all-feature tests, strict Clippy, workspace builds, and the four existing
feature bundles. Windows GNU uses MSYS2 MINGW64 with its matching native
compiler/runtime first on `PATH`, explicitly selected Rust tools, and separate
Clippy and MSRV output directories. Native reparse-point tests are mandatory
on Windows. BT builds explicitly provision pinned libtorrent, Boost and OpenSSL
sources for each native target, verify the patched installation, and retain
its provenance and build logs. Cargo itself never downloads or builds those
native dependencies. The minimal and standard bundles exclude the native graph.
The native patcher reads source and patch text as UTF-8 and writes LF newlines,
independently of the host locale, while requiring every hunk to match exactly
before modifying any source file. Benchmark validator tests also read retained
JSON fixtures as UTF-8 on every platform.
Windows GNU links against the active MinGW toolchain with
`link-self-contained=no`, as configured in `.cargo/config.toml`. This keeps
Rust-linked C++ code on the same MinGW runtime as the native dependency build.
Mixing Rust's bundled runtime archives with MSYS2's C++ runtime can leave
Boost.Asio threads alive after session destruction and cause access violations.
Native peer fixtures create seed files and intermediate directories with the
same private permissions or Windows ACLs required by admission. Rate-allocation
tests advance a paused clock before expecting replenished tokens. Lifecycle
fixtures retry an unaccepted busy command within their deadline, then retain
the accepted command's completion obligations after dropping its reply.
The hand-written metadata peer explicitly disables encryption for its plaintext
BEP 10 exchange and restores blocking mode on accepted sockets before using
bounded synchronous reads, including on Windows where listener mode is inherited.
Native-to-native fixtures retain the default encryption policy. Transfer fixtures
wait for checkpoint completion before comparing payload bytes on disk; native
seeding status can precede completion of queued disk writes.
Native endpoint tests flush stage, session-shutdown and fixture-worker progress
so a timeout identifies the last operation reached, including shutdown during
exception unwinding. The 90-second CTest deadline remains unchanged; an
intermittent timeout still requires thread-stack capture and diagnosis.
Every platform job runs the native OpenSSL, destination-policy, private
storage and bounded-output probes before its Rust workspace checks. GNU Windows selects MinGW
Makefiles, MSVC selects NMake Makefiles, and Unix hosts select Unix Makefiles;
build and CTest use the same `RelWithDebInfo` configuration. A failing native
probe stops that platform job. Feature-bundle and MSRV jobs retain their own
focused checks without duplicating the platform probes.
The disposable macOS runner explicitly provisions the second loopback address
used by active-FTPS peer-rejection fixtures. Those tests must exercise a real
unapproved source address before the approved TLS data connection.

### Upstream MSVC Coverage

The pinned libtorrent 2.1.1 [Windows workflow](https://github.com/arvidn/libtorrent/blob/v2.1.1/.github/workflows/windows.yml)
runs deterministic and integration tests, simulations, and additional Debug,
Release, 32-bit and API/configuration builds. Its CMake job excludes tracker
and SOCKS5 web-seed cases; some upstream jobs retry failures. Ariax retains its
own real tracker/web-seed probes and fails tests without retries.
Upstream's bencoding tests use `std::back_inserter`; they cannot validate
Ariax's custom output iterator. Our early bounded-output probe compiles the
production iterator and checks reassignment, nested preformatted bencode,
exact limits and rejection paths on every platform, including MSVC.

Ariax provisions the production static dependency with `build_tests=OFF`.
Its probes and bridge/adapter tests do not constitute a run of the complete
upstream suite against the patched fork. That wider suite remains separate
dependency-upgrade evidence. Upstream's simulation-only IOCP/debug-iterator
workarounds are not applied to production builds or used to bypass our tests.

The fail-fast matrix also contains `bt-safety`. It builds a separate native
installation with AddressSanitizer and UndefinedBehaviorSanitizer, then runs the
real bridge and adapter tests, including asynchronous destruction and failed
checkpoints. A native security probe verifies redirect policy and DHT filtering
at an actual UDP socket. Instrumented builds have distinct provenance and cannot
be consumed by ordinary builds. The same job fuzzes the bounded BT parser for
20 seconds and retains its corpus, crash inputs and logs. Rust fuzz instrumentation
uses the pinned compiler with `RUSTC_BOOTSTRAP=1` confined to that command's
environment; it does not change production or MSRV compilation.
The Rust sanitizer link flags explicitly include `libstdc++`: Rust passes
`-nodefaultlibs`, while Clang's C++ sanitizer runtime requires C++ RTTI symbols
even when linking an otherwise pure Rust build script.
The pinned OpenSSL callback backport is hashed independently in native
provenance and applied before header generation. A standalone callback test
runs with strict sanitizers and known digest/cipher vectors, typed-stack
failure cleanup, and certificate success/rejection paths. Its baseline fails
against the unpatched dependency; sanitizer suppression is not a fix.

The benchmark's pure peer-framing regressions live in the engine integration-test
target. The custom benchmark harness imports only the shared parser, so ordinary
workspace tests execute the framing assertions and all-target checks stay clean.

Loopback RPC process fixtures send each HTTP request in one buffer and disable
Nagle's delay while retaining bounded response reads and shutdown waits. Failure
diagnostics identify the stdio framing, EOF policy and child process.

## Native Linux Measurements

Compile the optimized harness before collecting measurements and execute its
resolved binary directly. Every report records the source commit, binary hash,
host/toolchain and complete scenario results. Preserve failed-run diagnostics.

Run the four transport scenarios, the administrative scenario and the Phase-6
mixed HTTP/BT scenario sequentially. Each transport must finish 20,000 measured calls while its 1,000
Metalink-admitted ranges remain active. Existing per-operation p99, measured
residency, resource, mutation and clean-shutdown gates remain enforced.
The mixed scenario also requires 1,000 real BT peer connections, acknowledged
payload pulses, exact peer projections and 1,000 acknowledged live BT option
changes. Its 2,000 verification calls share the measured burst limits. Earlier
five-scenario evidence does not satisfy this additional gate.

Measured bursts contain at most 1,000 calls and last at most 500 ms, with at
least 250 ms cooldown. Each scenario has a 90-second internal deadline. An
outer process deadline additionally bounds a stuck child tree. Incomplete or
over-limit reports fail; they are never accepted because a process exited
successfully. Compiler processes must be absent during collection.

## Acceptance And Scope

The prerequisite is complete with the verified September 22 CI run and native
Linux measurement campaign retained for `af5d193`. Remaining
kernel/backend and release-platform requirements are separate gates; this
workflow does not authorize a release tag.

Phase 6 may then change unreleased internal APIs and persistence formats
without compatibility shims. Schema/JSON v3 will use fresh stores and reject
older development formats unchanged. Existing aria2-facing behavior, torrent
protocol support, feature bundles and MSRV remain product requirements.
