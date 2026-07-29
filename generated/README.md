# Generated Contracts

Files in this directory are deterministic implementation inputs generated from
the pinned aria2 Git objects and ariax's executable core and option registries.
The compatibility artifacts expose unreviewed upstream coverage instead of
silently implying parity. Executable journal and SQLite persistence contracts
are included. Do not edit generated JSON by hand.

`options.json`, `aria2_compat.json`, `runtime_updates.json`, and
`runtime_compatibility.json` come from `ariax-config`. The aria2 inventory files
come only from immutable blobs at the pinned commit. `storage_layout.json`
comes from executable path, root-binding, layout, and offset-mapper contracts.
`runtime_buffers.json` records the closed buffer lifecycle, size classes,
budget, quarantine, queue-credit, and completion-permit rules.
`journal_v1.json` freezes the executable segment/record framing, CRC and commit
coverage, record numbers, persisted enum tags, replay caps, and valid-prefix
rules. It also reports exact typed-payload coverage, scalar, collection, path,
identity, and bitmap caps, canonicalization rules, semantic recovery limits,
hash domains/coverage, and both codec and cross-record rejection vocabularies.
It also freezes the serialized appender's segment naming, acknowledgement,
latched-fault, tail-reopen, and flushed-boundary rotation rules.
`session_v1.json` retains the historical migration-source contract.
`session_v2.json` freezes the current strict SQLite schema SQL, direct rusqlite
feature/build contract, pragmas, hard connection limits, queue/terminal/install
enums, caps, reconciliation rules, and failure vocabularies. Version 2 adds the
demoted queue and bounded slow-slot metadata, plus the exact transactional v1
task and host-key-challenge table rebuild. The contract also records
private-artifact policy, bounded task/stopped/host/install reads, one-to-one
stopped metadata, atomic terminal retention/deletion and dense queue
transitions, host-key semantic validation, preflight-before-backup behavior,
cooperative owner locking, sidecar preflight/cleanup, WAL/DELETE behavior, and
backup requirements.
`error_codes.json` includes the stable journal numeric value for each closed
error class.

Run `cargo xtask generate ../aria2` to update the files and
`cargo xtask generate --check ../aria2` to verify that committed output is
current. Generation requires the checkout `HEAD` to match
`compat/aria2-reference.pin` and never reads mutable source files directly.
