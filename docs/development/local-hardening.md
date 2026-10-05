# Local Phase 7 Hardening

[Documentation](../README.md)

## Scope And Order

The [Phase 7 work packages](../project/implementation-plan.md#phase-7-hardening)
have a local review and short-validation pass followed by a separate, longer
local campaign. Review confirmed defects, update their owning contracts, and
add success and rejection regressions before collecting longer measurements.
This campaign does not dispatch CI, push, or approve a release.

Local results retain their actual platform. WSL 1 does not establish native
Linux timing, kernel backend, or Unix private-permission coverage on DrvFs.
Windows GNU supplies native Windows coverage. Existing mixed-BT platform and
workload restrictions remain unchanged. Benchmarks remain manual-only.

## Execution And Evidence

Select the pinned standalone Rust distribution and locked dependencies. Keep
native ABIs and instrumentation in separate output directories, verify native
provenance before reuse, and use at most two compiler workers. All campaign
temporary files and outputs use an explicitly selected disk-backed directory.
Compile before measurements; run measured processes serially.

Every attempt retains command, exit status, elapsed time, source/binary identity,
log hashes, and cleanup results. Missing reports, zero selected tests, incomplete
execution counts, timeouts, and interrupted cleanup cannot count as passes.
Independent repetitions are observations, not retries that overwrite failures.
Raw failures and unfiltered timing gates remain authoritative.

Use `scripts/local_hardening.py run --output <new-directory> --timeout 120
--expected-tests 1 -- <test-binary> <exact-test-name> --exact` for a focused Rust
test. Set the expected count to include known child-harness results. A reused
output directory is rejected to preserve the previous attempt. The runner
records allowlisted toolchain/instrumentation environment values and kills its
owned process group/tree on timeout. On Windows, successful parent exit still
requires checking the fixture's child-process and filesystem cleanup.

Use `scripts/local_hardening.py fuzz --binary <instrumented-target> --seeds
<seed-directory> --output <new-directory>` for smoke, and add `--batches 20` for
the longer pass. Select temporary-directory environment variables before launch;
the helper does not select or install a global toolchain.

Fuzz smoke uses 512 accepted executions per target in 16–128 execution bursts,
a 500 ms accepted-process limit, at least 250 ms cooldown, an 8 KiB input cap,
and a 512 MiB RSS cap. Corpus initialization counts separately from mutation
work. Keep original failing inputs before minimizing them. The longer pass
uses 20 batches per target, capped at 15 minutes per target; an exhausted budget
is incomplete. Strict sanitizer reports remain failures without suppression.

Lifecycle and recovery checks run once, then five repetitions for selected
timing-sensitive cases. The longer pass permits 50 repetitions and a 30-minute
limit per family. Existing test deadlines and crash-parent fixtures remain in
force. Stop a correctness failure for diagnosis and retain subsequent reruns
as separate evidence.

## Review Boundaries

Review the native bridge and Windows unsafe adapter, callback and handle
ownership, bounded native buffers, untrusted metadata, RPC authentication,
reserved headers, destination/redirect policy, private files, cancellation,
lease/completion credit, and atomic persistence/recovery. Review pinned native
patches and dependency provenance alongside their callers. Track findings with
the owning contract, code, regression, disposition, and platform limitation.

Check generated contracts and the pinned compatibility inventory, feature
policies, licenses, dependency locks, queue caps, streaming behavior, and release
inputs. Public API additions, persistence migrations, hardware power-loss tests,
and unavailable platforms remain separate work. Previous CI remains usable for
unchanged inputs through the documented source-equivalence review.

## Sanitizer Boundaries

Rust parser fuzz binaries receive Rust ASan and coverage instrumentation.
Native ASan/UBSan covers the configured C/C++ libraries and bridge; ordinary
Rust harnesses linking those libraries are not Rust-ASan coverage. Verify a
clean program and a deliberately racing program with the available TSan
runtime. Unsupported runtime behavior is a capability blocker, not a passing
race check. Expensive native provisioning belongs to the second local pass.

The native builder accepts `--sanitizer none`, `address` (ASan and UBSan), or
`thread` (TSan). Instrumented builds require `x86_64-unknown-linux-gnu` and use separate
installation directories. Set `ARIAX_BT_SANITIZER` to the matching mode when
compiling the Rust bridge, and link with the same Clang sanitizer runtime.
Native probe CMake builds use `ARIAX_SANITIZER` or `ARIAX_THREAD_SANITIZER`;
enabling both is rejected. Cache verification includes the mode and builder
identity, so a stale or differently instrumented installation must be rebuilt.

Check the Rust test harness separately when linking native-only TSan. In this
campaign, an empty Rust test reproduced reports in the prebuilt `libtest`
completion channel, whose Rust synchronization was not instrumented. Retain
that failed configuration. A direct main-thread driver can exercise unchanged
native test bodies without the harness channel; record the source adaptation,
locked dependencies and coverage boundary. It still supplies no Rust race
coverage. Fully instrumented Rust validation requires a compatible instrumented
standard library and harness. Do not suppress reports to obtain a pass.

### Maintained Direct TSan Driver

[`scripts/bt_tsan.py`](../../scripts/bt_tsan.py) derives its two direct entry
points from `ariax-bt-libtorrent-sys/tests/native.rs`. It preserves the native
test bodies, assertions, fixtures and synchronization. An unexpected test
inventory fails preparation. The driver verifies the separate native TSan
installation without provisioning or downloading dependencies.

For a short run of a previously recorded driver, select absolute disk-backed
paths for `NATIVE_TSAN`, `PREVIOUS` and a fresh `OUTPUT`, then run:

```bash
python3 -B scripts/bt_tsan.py reuse --native-dir "$NATIVE_TSAN" \
  --record "$PREVIOUS/result.json" --output "$OUTPUT"
```

The default runs each case once, serially, with a 60-second process timeout.
`--repetitions` accepts one through five. `TSAN_OPTIONS` is set to
`halt_on_error=1:exitcode=66`; reports, missing case markers and leftover
fixture directories fail the attempt. Each run uses its output's `tmp`
directory and retains immutable logs, binary/source identities and cleanup
results. A failed case stops the run without retrying it.

Reuse checks current native/build inputs, the generated project and fixture
bytes, repository-locked dependency identities, the prior case inventory and
the executable hash. The initial temporary campaign predates embedded source
inventories: importing it additionally requires `--source-snapshot` for its
first-pass snapshot and again for its later partial second-pass snapshot, in
that order. Missing source entries fail closed. Subsequent maintained records
embed the complete relevant source inventory and need no snapshot arguments.

To create a fresh driver, select the pinned `RUSTC`, `RUSTDOC`, local Cargo
cache and an absolute, separate `CARGO_TARGET_DIR`; set `CC=clang`,
`CXX=clang++`, and
`RUSTFLAGS='-C linker=clang++ -C link-arg=-fsanitize=thread -C link-arg=-lstdc++'`.
Invoke `build` instead of `reuse` and supply `--cargo "$RUST_BIN/cargo"`.
Preparation copies the repository lock, runs `cargo update --offline
--workspace`, and rejects dependency identity drift before `cargo build
--locked --offline`. Two compiler workers and a 180-second build timeout bound
this focused build. An exhausted budget remains failed evidence; do not launch
native provisioning implicitly. Reuse does not invoke Cargo or rebuild.
The [short follow-up evidence](../../performance-evidence/phase7-short-followup-2026-10-05.md)
records the initial import and subsequent maintained-record reuse.

## Local Release Preparation

Build the supported CLI bundles through the documented `release-cli` profile
with locked dependencies. Compare independently built outputs, retaining the
source, toolchain, environment and dependency identities for both attempts.
Record any path remapping and timestamp controls explicitly. Windows GNU must
retain `-C link-self-contained=no` when overriding `RUSTFLAGS`; keep the matching
MinGW runtime first on `PATH`. A reproducible local subset does not establish
the remaining release-platform matrix or compatibility parity.

Remap Cargo-home registry/git source paths as well as repository and target
paths. Supply platform-native source prefixes to `--remap-path-prefix` and inspect
the final binaries for absolute workstation/cache paths. The retained October 5
minimal/standard artifacts match independent builds but retain dependency source
paths; the [package review](release-packaging.md#package-manifests) records this
release blocker. Preserve `-C link-self-contained=no` for Windows-GNU. Native
C/C++ sources may also require file/debug-prefix mapping, with their normal
provenance/cache checks retained. Do not patch the finished binary to hide paths.

The generated inventory remains the parity authority. Keep Ariax parallel to
aria2 while reviewed handler coverage is incomplete; do not publish an
`aria2c` replacement or infer a persistence migration from build success.
