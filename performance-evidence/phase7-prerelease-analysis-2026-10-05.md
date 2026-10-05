# Phase 7 Pre-Release Analysis

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-prerelease-analysis-2026-10-05.json)

This local slice starts at `ba4a141` on `local/phase7-hardening`. It prepares
host-load diagnostics, selects MIT for Ariax-owned code under the user's
authorization, and inventories dependency notices. It runs no performance
benchmark, full build, native dependency provisioning, CI dispatch or push.

## Host-Load Evidence

Fresh completed-burst reports and overrun errors now include a Unix-nanosecond
start anchor sampled immediately before the monotonic timer. Historical reports
may omit it; present anchors must validate. Duration limits still use monotonic
time, with the same exact 500 ms cap and unchanged workload and p99 thresholds.

The native CI collector writes `host-load.jsonl` at a nominal 250 ms interval,
bounded to 512 samples and 4 MiB per scenario. It captures host CPU ticks,
runnable/blocked counts, load averages, available pressure/cgroup-v2 counters
and bounded benchmark process-group CPU/I/O counters. Discovery runs at most
once per second; scan and group caps are explicit. Records include clock
frequency, Unix/monotonic timestamps, collection costs and unavailable metrics.
The run retains telemetry and its hash even when the benchmark fails. Sampler
exceptions cannot bypass ordinary benchmark child cleanup. Compiler activity
checks retain their five-second cadence.

The two-second WSL smoke collects eight samples, 6,275 bytes, with no dropped
samples or write errors. Maximum collection cost is 6,108 microseconds; recorded
sampler CPU time is 15,625,000 nanoseconds. These are smoke-test observations,
not a native overhead guarantee. WSL provides CPU/load/process-stat counters
but lacks PSI, usable cgroup-v2 membership and the sampled process-I/O file.
Unavailable metrics remain explicit. Native overhead, coverage and correlation
must be reviewed on the next manual benchmark.

This cannot retroactively explain the retained 530.097 ms failure. High load
alone does not establish causation; sampling gaps, wall-clock adjustments and
subinterval spikes limit interpretation. There is no overload exemption from
acceptance limits.

## License And Dependency Review

[MIT](../LICENSE) applies to Ariax-owned code and documentation. All 11 workspace
packages inherit it; `publish = false` remains in force and `Cargo.lock` is
unchanged. Vendored code, native dependencies and embedded data retain their
original terms.

The [inventory](phase7-release-license-inventory-2026-10-05.json) resolves each
CLI bundle separately with locked offline Cargo metadata and target-specific
`cargo tree --edges normal,build`. It contains 315 distinct packages, including
305 external packages and ten Ariax packages across the combined CLI closures.
The separate workspace metadata check covers all eleven, including `xtask`.
Build dependencies are included conservatively; these counts are not a list
of libraries linked into each executable.

| Bundle | Linux External Packages | Windows-GNU External Packages |
| --- | --- | --- |
| `minimal` | 175 | 180 |
| `standard` | 276 | 287 |
| `full` | 286 | 297 |
| `compat` | 286 | 297 |

The declared Rust license expressions offer permissive choices compatible with
MIT-owned application code. Mandatory Apache/ISC/Unicode terms remain separate.
Native libtorrent, Boost and OpenSSL carry BSD-3-Clause, BSL-1.0 and Apache-2.0
headline licenses. Libtorrent's complete `LICENSE` includes additional embedded
and platform-specific notices; those are retained alongside `COPYING`.

The [notice draft](phase7-third-party-notices-2026-10-05.txt) contains 237 unique
original text blocks, 768,499 bytes, with source labels, byte lengths and SHA-256
identifiers. Candidate notices from cached registry crates match their exact
locked archives. The three native archives match their pins, and selected
native notice files match their archive bytes. Vendored FTP/SFTP notices retain
their existing provenance. SQLite's source dedication and ring's nested crypto
notices are included. Conservative collection may include unused source files.

Six crates whose published archives omit standalone licenses have those files
recovered at their recorded upstream revisions: `delegate`,
`lazy-regex-proc_macros`, `pageant`, `russh`, `russh-cryptovec` and `russh-util`.
`crc32c` explicitly permits Apache-2.0 or MIT in its README; the draft selects
Apache-2.0 and includes that text. `winapi-x86_64-pc-windows-gnu` declares the
same choice but has neither standalone license nor VCS metadata; its declaration
and Apache text are retained, with import-library provenance still open.
The published `russh-util` VCS metadata is marked dirty; its crate checksum
remains pinned, and the root-license supplement does not erase that limitation.

Every bundle embeds the MPL-2.0 public-suffix snapshot. The draft retains its
header and the MPL text; final packages must include the exact covered source,
provenance and source-availability notice. IANA/ICANN data terms, extracted
aria2 inventory scope, platform-specific native terms and actual runtime/DLL
redistribution requirements remain explicit review items in the
[release checklist](../docs/development/release-packaging.md).

## Local Checks

- Eight Rust timing regressions pass, including overrun anchor retention and
  precise duration, longest-burst, tie and state-preservation behavior.
- All 110 Python helper tests pass. Telemetry cases cover parsing, malformed
  counters, bounded reads/samples/bytes/process discovery, missing metrics,
  PID reuse, failed writes, successful/failed collectors and compiler rejection.
- Focused Clippy passes for the benchmark and timing test with
  `metalink,ftp,sftp` and all features including BT. Rust 1.97.1 remains pinned;
  compilation uses two workers and retained native installations.
- Rust-generated JSON passes the Python validator. All eleven retained complete
  historical reports still validate; the old incomplete mixed failure stays
  failed. No historical performance result is promoted to current acceptance.
- Workspace license metadata, notice-block hashes/reference coverage, eight
  bundle closures, documentation, formatting, publication and whitespace checks
  pass. Raw commands, logs and assembly attempts remain on E:.

Two initial notice-assembly attempts fail before producing the draft: a filename
scan encounters the binary `libwinapi_oemlicense.a`, then the corrected scan
exposes the import-library crate's missing license text. The final attempt
excludes binary archive files and records the missing provenance explicitly.
No failed benchmark or native test is hidden by this tooling correction.

## Remaining Release Gates

The release candidate still needs its applicable native/platform matrix,
native Linux mixed-burst stability/attribution evidence, remaining race/kernel
and hardware-recovery gates, and reviewed final distribution archives. The
license metadata changes do not alter production behavior, but they do change
source fingerprints used by retained-evidence checks. Existing CI and TSan
evidence retains its original scope. Benchmarks remain manually triggered;
tagging and release approval remain disabled.
