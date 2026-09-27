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

## Initial Offline Checks

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

## Isolated Follow-Up Validation

The authorized follow-up starts from cleanup commit
`b28e426e7aad57009094ef46fe1426f327509d2f`. It provisions the missing locked
dependencies and runs focused tests on x86-64 WSL1
(`Linux 4.4.0-17763-Microsoft`) with the repository's standalone Rust 1.97.1
(`8bab26f4f`, LLVM 22.1.6). The earlier offline results above remain historical.

The cache, build target, logs and temporary files are isolated under
`<temporary-root>/ariax-pre-ci-8cedt67d`, using the user-selected temporary
location. This record uses a portable placeholder; local execution records
retain the actual path. Existing caches are only read to seed verified archives.
`cargo fetch --locked --target x86_64-unknown-linux-gnu` completes;
subsequent Cargo commands use `--locked --offline`, two build jobs and the same
target directory. `Cargo.lock` remains unchanged, with SHA-256
`0a9174da9937800e0e077561527d2c9002dffe06e14e6d6534568bed1723d876`.

The ordinary E: mount reports `0777` after requests for private Unix modes, so
the first 11 session/control tests fail during persistence startup. An isolated
256 MiB `tmpfs` at the validation directory's `posix-tmp` path supplies real
`0700` directories and `0600` files for the retry and other persistence tests.
Each command removes its temporary mount afterward; the original E: mount and
global toolchains/configuration are unchanged. Application permission checks
are retained. This validates process-crash recovery, not power-loss durability
on a native disk filesystem.

The storage build exposes an unused `seed_owner_lock` test helper, and Clippy
finds three unnecessary fixture clones. Removing the helper and comparing
borrowed slices eliminates these warnings without changing production behavior
or aria2 RPC, persistence, authentication or feature contracts. Existing owner
locking and permission success/rejection tests pass after the helper removal;
the three tests with updated assertions also pass after the clone cleanup.

### Passing Focused Tests

| Check | Result |
| --- | --- |
| Engine `session_file::tests` | Six pass: current JSON source records, obsolete-shape rejection, aria2 URI/option projections and credential placeholders. |
| Engine `checksum` filter | Nine pass: all four algorithms, published vectors, strict mirror eligibility, persisted final digests and repeated offline mismatch rejection. |
| Selected engine session/control and Rust API tests | 11 pass after the filesystem correction: atomic import, interrupted import, bounded saving, shutdown failure, credential redaction and aria2 URI/save response shapes. |
| Engine `http_rpc::tests` | 31 pass: strict/extended responses, authentication, batches, multicall, HTTP, WebSocket and both stdio framings. |
| Engine Metalink/import/admission tests with `metalink,ftp,sftp` | Three pass: atomic Metalink admission, verification/source import into a new root and remote authentication restrictions. |
| Engine configuration-dump regressions with `metalink,ftp,sftp` | Two pass: JSON defaults, supported dump formats, legacy/invalid format rejection, source precedence and atomic reload rejection. |
| CLI `rpc_interfaces`, `--no-default-features` | Four pass, including actual-process HTTP/stdio sharing and all eight framing/EOF combinations. |
| CLI `rpc_interfaces`, `--no-default-features --features standard` | The same four pass with Metalink, FTP/FTPS and SFTP enabled. |
| Storage `session_store::bt::tests` | Six pass, including process exit before/after checkpoint commit in both WAL and DELETE modes, stale checkpoint rejection and option rollback. |
| Selected storage owner/permission tests | Four pass: process exclusion, explicit unlock with duplicated handles, private SQLite artifacts and rejection of a permissive parent. |
| Storage cleanup regressions | Three pass after removing fixture clones: checkpoint rejection, paused-option rollback and mixed transfer/BitTorrent batch rollback, exact binding and reopen. |

These are 83 passing test executions across focused selections, including the
four CLI tests in each feature configuration and two repeated BitTorrent tests
after the assertion cleanup. Child-process assertions run within the parent
tests and are not added to this total. Exact commands, per-command exit status
and full output remain in the isolated `logs` directory.

### Formatting And Static Checks

The final focused Clippy check passes with warnings denied after the fixture
cleanup:

```text
cargo clippy --locked --offline -p ariax-engine -p ariax-storage -p ariax-cli \
  --all-targets --no-default-features --features ariax-cli/standard -- -D warnings
```

This checks the engine, storage and CLI targets with the standard feature set,
including type-checking the updated RPC benchmark code. It does not execute
benchmarks. Rust 1.97.1 rustfmt checks both changed storage files; documentation
and whitespace checks pass. `Cargo.lock` retains the checksum recorded above,
and no temporary validation mount remains.

### Remaining Acceptance

Full workspace and feature/platform/MSRV checks, native libtorrent and backend
execution, sanitizer/fuzz campaigns and benchmark execution still require CI.
The native Phase 6 gates remain open; this follow-up does not turn the earlier
checkpoint into native acceptance. No remote Git operation, GitHub API call,
push, CI trigger or tag is performed.
