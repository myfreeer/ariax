# Pre-CI Cleanup Validation

This local cleanup follows Phase 6 checkpoint
`1162708779728aa71e74318f12cba978c5544ebd`. It removes redundant development-format
paths while retaining the supported aria2 RPC contract. It does not close any
native or CI acceptance gate from the
[Phase 6 validation record](phase6-local-validation-2026-09-26.md).

## Changes

JSON v3 transfers now use only `sources`. Imports reject the old `uris` field,
including documents that supply both representations. Live URI inputs and
persisted source records use distinct admission variants. Option resolution
borrows the first available URI without building another URI list, and aria2
text parsing constructs admission records directly instead of round-tripping
URI lines through JSON objects.

Aria2 text exports derive their URI lines from the source records. Metadata
comments retain credential placeholders and must agree with the URI and option
lines. JSON-only metadata is rejected in the text format. The unused string-only
RPC URI serializer branch is removed; `aria2.getUris` and `aria2.getFiles`
continue to emit the same `uri`/`status` objects. `aria2.addUri` and
`aria2.saveSession` keep their existing parameters and response shapes.

Tests cover current-format import, old-shape rejection, source identity and
priority preservation, credential placeholders, aria2 text options and
projection tampering, RPC URI response shapes, and session-save acknowledgement.
Existing batch/crash tests and benchmark inputs use current source records.
The fuzz corpus now contains actual v3 source/verification inputs, plus distinct
obsolete-format and duplicate-field rejection seeds.

## Local Checks

- Rust 1.97.1 rustfmt checks all 11 changed Rust files.
- Documentation checks pass across 54 Markdown files; whitespace checks pass.
- All seven JSON fuzz fixtures pass a syntax check, with duplicate-field
  rejection confirmed for the intentionally duplicated `tasks` seed. This is a
  fixture check, not a fuzz or engine execution result.
- The focused command below stops before compilation because the local cache
  lacks the pinned `md-5` dependency. None of the changed engine tests or
  benchmark code has been compiled or executed in this pass.

```text
cargo test --locked --offline -p ariax-engine --lib session_file::tests --jobs 2
```

The command used the repository's pinned Linux Cargo/Rustc/Rustdoc and a temporary
target directory that was removed afterward. No dependencies were downloaded or
changed. Full engine, feature/platform, native, fuzz and benchmark execution
remain pending. No GitHub API call, push, CI trigger or tag was performed.
