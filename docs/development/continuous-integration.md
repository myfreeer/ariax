# Continuous Integration

[Documentation](../README.md)

Status: the remote and CI baseline passes at `af5d193`.
[Run 35720518419 and its retained evidence](../../performance-evidence/ci-baseline-2026-09-22.md)
cover every required platform, feature and MSRV job and all five native Linux
benchmark scenarios. The prerequisite is complete; Phase 6 may proceed.

## Repository And Branch

The canonical remote is `git@github.com:myfreeer/ariax.git`. Publish the
existing history on `main`, which is the remote's default branch. CI runs on
pushes and pull requests with read-only repository permissions. A newer run
for the same ref cancels its predecessor.

## Fail-Fast Topology

The workflow has three ordered stages:

1. Preflight validates documentation links and navigation, formatting, workflow
   syntax, whitespace, generated contracts, pinned reference inputs, protocol
   forks and feature policies.
2. One validation matrix contains all platform, feature-bundle and MSRV jobs.
   Its `fail-fast: true` cancels sibling jobs after a failure. Every script
   propagates native-command and pipeline failures; no required check uses
   `continue-on-error` or automatic retry.
3. A successful push to `main` runs the reusable native Linux benchmark
   workflow against the same commit. Pull requests run functional validation;
   manual benchmark dispatch remains available for investigations.

The final `CI Required` job passes only if every stage required by the event
passes. A failed, cancelled or unexpectedly skipped required stage cannot
produce a green aggregate result. Job timeouts bound stuck work. Cleanup
uploads available logs even on failure or cancellation.

Full builds, tests, native provisioning and benchmark collection run in CI.
Local documentation checks and focused debugging should avoid recreating the
platform matrix or accumulating native build artifacts. See the
[development guide](README.md) for the lightweight local workflow.

## Reproducible Validation

Install Rust 1.97.1 on each fresh runner and check the supported graph with
Rust 1.88.0. CI helpers use Python 3.13, or MSYS2's native Python for GNU jobs.
Use the locked dependency graph. The checkout must supply all
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
on Windows. Additional libtorrent dependencies will be provisioned and pinned
when the BT feature lands.
The disposable macOS runner explicitly provisions the second loopback address
used by active-FTPS peer-rejection fixtures. Those tests must exercise a real
unapproved source address before the approved TLS data connection.

## Native Linux Measurements

Compile the optimized harness before collecting measurements and execute its
resolved binary directly. Every report records the source commit, binary hash,
host/toolchain and complete scenario results. Preserve failed-run diagnostics.

Run the four transport scenarios followed by the administrative scenario
sequentially. Each transport must finish 20,000 measured calls while its 1,000
Metalink-admitted ranges remain active. Existing per-operation p99, measured
residency, resource, mutation and clean-shutdown gates remain enforced.

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
