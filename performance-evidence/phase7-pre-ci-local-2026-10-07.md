# Phase 7 Pre-CI Local Validation

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-pre-ci-local-2026-10-07.json)

Local validation completed on WSL1 and native Windows GNU for the scopes below.
The local branch is `validation/p7-initial-native-20261007`. No push, CI dispatch,
tag or release occurred. Ariax remains standalone with best-effort aria2
compatibility; the release tag gate stays closed.

## Fixes Found By Validation

- Windows libtorrent and the Rust bridge selected incompatible Asio thread
  implementations in different translation units. Shutdown could call
  `pthread_join` for Win32 threads and free live timer state. Native patch level
  6 configures Asio before platform headers throughout the Windows library and
  its CMake consumers; the bridge enforces the same boundary. Six native probes
  include timer/context cleanup, with a Windows compile rejection for POSIX
  thread selection. Full-feature transfer/checkpoint/destruction tests and GDB
  pass against the rebuilt library. The original access violations remain in
  the JSON, alongside the insufficient bridge-only attempt.
- A mixed benchmark setup test still expected live concurrency updates to be
  unsupported. It now verifies the implemented one/two-slot updates and rejects
  zero/above-bootstrap-capacity values without changing the effective limit.
  The benchmark workload and production behavior are unchanged.
- Windows Python tests now explicitly simulate Unix-only APIs and preserve
  canonical LF bytes for Linux TSan source fixtures. Production guards remain
  strict; no tests were skipped to hide these failures.

The debugger also exposed Windows `GetAdaptersAddresses` diagnostic messages.
A standalone C program using only that Windows API reproduces them and returns
success. Traces identify `DBG_PRINTEXCEPTION_C` from OS interface enumeration;
these messages remain visible and are separate from the repaired access
violation. The original temporary wrapper rejection and diagnostic attempts
are retained. C++ language-standard experiments were inconclusive and no
language-standard change was kept.

## Validation Results

| Check | WSL1/Linux | Native Windows GNU |
| --- | --- | --- |
| Default workspace tests | 996 harness passes; one exact WSL1 exclusion | 997 harness passes |
| All-feature workspace tests | 1050 harness passes; same exclusion | 1051 harness passes |
| Native CMake probes | Six passed | Six passed, plus POSIX-thread configuration rejected |
| Clippy, all targets/features, warnings denied | Passed | Passed |
| Rust 1.88, all targets/features | Passed | Passed |
| Python helpers | 163 tests passed | 163 tests, four existing platform skips |
| CLI minimal/standard/full/compat tests and release profiles | All four passed | Default/all-feature workspace coverage above |
| C API release profile | Passed | Separate CI coverage remains |
| Final full/OpenSSL CLI and package smoke | Passed | Passed; only Windows-provided DLL imports |

Harness counts include child test harnesses; they are not unique-test counts.
The JSON preserves individual command statuses, summaries, hashes and failed
attempts. Jobs ran serially with at most two compiler workers. No Rust standard
library was rebuilt or CI job launched in this pass. The mixed-BT fixtures
correctly rejected this shell's inherited 1,024-file limit; they pass with
65,536 available to the validation process, within the unchanged hard limit.
No system limit, fixture capacity or production rejection policy was changed.

WSL1 leaves writes blocked after peer close in the stalled-RPC socket fixture;
raw socket controls reproduce the host behavior. The production code, test and
six-second cleanup bound are unchanged. Only
`socket_fixture::socket_stalls_retain_each_consumers_credit_and_release_on_disconnect`
is excluded from the explicitly labeled local subsets. It passed unchanged on
native Windows and must run unchanged on native Linux. This is separate from
the deferred latency failures.

The first Linux all-feature run also hit a 30-second HTTP response timeout in
`changed_dns_answer_set_opens_a_revalidated_direct_connection`. The unchanged
test passed in isolation; the original failed attempt remains unattributed.
It was neither excluded nor given a longer deadline. A later pass is not a fix
or stability evidence; native Linux acceptance must retain this investigation
if the failure recurs. The full CLI feature test also timed out once reading
an HTTP `getOption` response with NDJSON stdio and EOF-ignore. Its unchanged
focused test passed; the original three-second socket bound stays in force.
That second timeout likewise remains unattributed and visible in the record.

## Final Artifacts And Cleanup

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Linux full/OpenSSL CLI | 21,603,400 | `76b4c0e577375c7bf522100c94ae2f98f576689f52a87a2c32bcf708eeb8c99d` |
| Windows full/OpenSSL CLI | 24,450,048 | `2472a2f5b98955100598e59024582109c9dcb49150f0cc6c2a34bcd342a46636` |

The [package catalog](../distribution/package-manifests.json) records these two
final builds. Native dependency inputs and installed file inventories were
verified; unchanged OpenSSL cache outputs were reused only after identity
checks. Known workstation-path audits pass. Reduced-environment package checks
cover help, RPC features, unknown-method rejection, EOF shutdown and SQLite
reopening. Linux's existing system preload remains recorded and unchanged.
These checks do not establish fresh/minimum-OS acceptance or independent
reproducibility of the latest binaries.

Removed 28.54 GiB of temporary validation/native build outputs.
Toolchains, dependency archives/installations, Cargo dependencies and one final
build root per platform remain on the existing volumes. Useful diagnostics are
consolidated in the JSON; redundant raw success logs, staged package copies and
disposable helpers/fixtures are removed. No saved-binary collection or filesystem
compression is used. The prior [size analysis](phase7-size-reduction-2026-10-07.md)
retains its original artifact and performance scope.

## Remaining Acceptance

The prepared manual `initial-native` CI selection runs native Linux, Windows
MSVC and macOS with one matrix job at a time. The branch is local and pushes are
held. This first batch does not replace the full feature/MSRV/platform/sanitizer
matrix, fresh/minimum-OS checks, final-candidate reproducibility or longer
platform stress. Historical latency failures and VM power-loss recovery remain
explicitly deferred. See [implementation readiness](../docs/project/implementation-readiness.md#phase-7-local-validation).
