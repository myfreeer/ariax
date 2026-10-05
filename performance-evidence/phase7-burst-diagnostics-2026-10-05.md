# Phase 7 Completed-Burst Diagnostics

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-burst-diagnostics-2026-10-05.json)

This local slice starts at `cadbb80` on `local/phase7-hardening`. It implements
the diagnostic follow-up to the [retained burst comparison](phase7-short-followup-2026-10-05.md#mixed-burst-comparison).
No performance benchmark, soak, push or CI dispatch is part of this slice.

## Behavior

Complete transport and mixed reports now contain `burstTiming` version 1.
It separates primary, verification and untimed totals across the run and
retains one snapshot of the longest completed burst. The snapshot includes
its number, first sample index, call counts, separate totals, last and slowest
steps, and precise elapsed duration. Exact duration ties retain the earliest
burst. Each step identifies the operation, phase, sample, start and duration.

Bookkeeping has fixed size. It creates no per-burst history and formats JSON
after measurement. Rejected bursts still fail, preserve the existing stderr
diagnostics and now include separate primary/verification totals. Rejected or
internally inconsistent durations cannot alter the completed-burst totals.

The workload, 400 ms launch cutoff, 500 ms/1,000-call burst cap, 250 ms cooldown
and 50 ms p99 gates remain unchanged. Nanosecond elapsed time lets the report
validator enforce the exact existing burst cap even when milliseconds truncate
to 500. Timing-component validation permits only the zero-to-two microsecond
difference caused by independently flooring three duration components.

Fresh CI measurements require the new diagnostics. Historical reports without
them remain readable; present fields must validate, including version, timing
composition, aggregate agreement, counts, sample positions and step durations.
Administrative reports retain their existing separate shape. No workflow,
production API, persistence format or protocol behavior changes.

## Local Checks

- Seven Rust regressions pass, covering failure attribution, the exact cap,
  longest-burst selection, separate totals, ties, empty bursts, rejected-state
  preservation and sub-microsecond accounting.
- All 99 Python helper tests pass, including eight new report-validation tests
  for complete and small reports, historical compatibility, malformed data,
  exact-limit rejection and fresh-collector enforcement.
- JSON emitted by the Rust helper passes the Python validator. This uses a
  synthetic three-burst fixture, not a performance measurement.
- All eleven complete reports from the two retained Phase 6 benchmark archives
  still pass the validator. The earlier incomplete mixed failure stays failed.
- Focused Clippy checks pass for the benchmark and timing test with
  `metalink,ftp,sftp` and with all features, including BT, using retained native
  dependencies. Rust 1.97.1 remains pinned and compilation uses two workers.
- Workspace formatting, documentation, publication-path and whitespace checks
  pass. Build outputs and temporary files remain on E:.

The test build takes about 39 seconds; the focused Clippy checks take about
3 seconds and 72 seconds. These are local build/check durations, not latency
results. No new native dependency provisioning is performed.

## Remaining Evidence

A later native Linux run is needed to populate this snapshot with real
mixed-workload measurements. A timed primary call still includes client,
transport, server and scheduling time; `otherUs` does not identify a subsystem.
The retained 530.097 ms failure remains unexplained. These diagnostics provide
attribution data for future runs without establishing stability or closing
the full Phase 7 gates.
