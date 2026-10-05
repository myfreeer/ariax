# Phase 7 Local Security Review

[Documentation](../README.md)

This is a review record, not a replacement for the owning subsystem contracts.
It covers the local hardening changes starting from `b81b8ea`. Execution evidence
is recorded separately; reviewing a boundary does not establish sanitizer,
platform, or release acceptance.

## Findings And Repairs

| Finding | Boundary And Defect | Repair And Regression |
| --- | --- | --- |
| `P7-S01` | Torrent and magnet tracker overlays grew their intermediate vectors before rejecting more than 64 endpoints. | Check before retaining each new distinct endpoint. Exercise the exact cap, duplicates, and early rejection before a later malformed endpoint. |
| `P7-S02` | Unix and Windows recovery enumeration materialized all directory names before journal/backup callers applied their smaller limits. | Bound native enumeration at 65,536 names and 16 MiB of encoded names. Native tests exercise exact limits and overflow without filesystem mutation. |
| `P7-S03` | System and Hickory DNS adapters collected all answers, and normalization grew its sets before enforcing cardinality. | Bound project-owned answer collection at 32 distinct addresses and enforce configured smaller limits before normalization growth. An iterator regression proves processing stops on the first excess distinct address. |
| `P7-S04` | Windows directory parsing used a padded structure read after checking only the fixed header, then constructed UTF-16 slices using response offsets. | Decode fixed-width fields from checked byte slices and validate advancing aligned offsets. Regressions cover valid adjacent records, every truncation, bad lengths/offsets, and visitor rejection. This repairs the safety proof; no real-world memory fault is asserted. |
| `P7-D01` | The development overview still described the retired temporary CI workflow as active. | Point to the restored functional workflow, manual-only benchmarks, and current local campaign. |
| `P7-R01` | Release-input inspection found no repository license file or license declaration for the 11 workspace packages, which remain `publish = false`. | Keep distribution-license selection and notice packaging open. All 224 third-party packages in the inspected default workspace resolution declare a license; this metadata inventory does not establish legal compatibility. |
| `P7-T01` | Native-only TSan linked into the prebuilt Rust test harness reported races in the uninstrumented `libtest` completion channel. | An empty Rust-only test reproduces the report, while a direct `main` passes. A direct driver runs the two unchanged native test bodies five times each under strict TSan, with matching locked dependencies and no suppression. Retain the nine failed harness invocations; this supplies native FFI coverage, not Rust race coverage. |

The repairs add no command-line/RPC option, public API signature, persistence
format, or protocol capability. Oversized recovery directories now fail before
unbounded allocation. Destination policy, credentials, existing acceptance
thresholds, and native ABI settings remain unchanged.

## Regression Locations

- `P7-S01`: [metadata admission](../../crates/ariax-bt-metadata/src/metadata.rs),
  `tracker_overlays_bound_growth_before_processing_later_inputs`.
- `P7-S02`: [Unix enumeration](../../crates/ariax-storage/src/native_capability.rs)
  and [Windows enumeration](../../crates/ariax-windows-security/src/windows.rs),
  `directory_enumeration_enforces_both_limits_without_mutating_files`.
- `P7-S03`: [DNS collection](../../crates/ariax-engine/src/http_resolver.rs),
  `backend_answers_preserve_order_deduplicate_and_stop_at_the_bound`.
- `P7-S04`: [directory decoder](../../crates/ariax-windows-security/src/directory_response.rs),
  `names_decode_without_padding_reads_and_every_truncation_rejects` and
  `malformed_offsets_lengths_and_visitor_rejections_fail_closed`.

## Reviewed Boundaries

- **Native ownership:** the CXX bridge owns asynchronous inputs and checkpoint
  state, redacts exceptions, and confines the opaque session to one adapter
  thread. The patched checkpoint pauses before the disk-release fence; its
  callback owns the torrent and completion state. Worker field order destroys
  the native session before releasing pending checkpoints and resource shares.
- **Admission and completion:** command/completion leases survive unread replies;
  rejection returns unaccepted blob ownership. Metadata stays held before
  approval, selected paths are validated against protected roots, and resume
  import excludes cached endpoint/path authority. Existing tests cover stale
  versions, overload, cancellation, dirty removal, and resource refunds.
- **Windows capabilities:** relative opens retain live root handles, prohibit
  reparses, verify identities/link counts, and use RAII for owned handles and
  Win32 allocations. Existing ACL/SID validation retains bounded ACE parsing.
  The directory decoder changes are isolated to response interpretation.
- **RPC and networking:** bounded JSON ownership charges precede expansion;
  HTTP Basic duplication, per-member tokens, reserved headers, redirect secret
  stripping, numeric-address normalization, destination filtering, DNS waiter
  cancellation, and response-credit lifetime retain their existing regressions.
- **Persistence and trust:** transactional source/queue updates, private-file
  checks, no-clobber backup publication, journal install tokens, host-key
  challenge binding, and dirty checkpoint recovery remain the authority
  boundaries. Known-host parsing caps line, file, key, and retained bytes.
- **Dependency provenance:** pinned libtorrent 2.1.1, Boost 1.91.0 and OpenSSL
  3.6.3 source/patch identities are unchanged. Native cache builder identity is
  checked independently; a stale builder manifest cannot masquerade as a fresh
  current-source build. This review is not an exhaustive advisory-database scan.

## Remaining Scope

The prior 530.097 ms mixed-burst failure remains unattributed. Local diagnostic
regressions do not prove native Linux stability. WSL private-mode fixtures,
kernel-specific backend behavior, fully instrumented Rust TSan validation,
unavailable release platforms, distribution-license selection, and hardware
power-loss behavior retain separate evidence requirements. The completed
[local campaign](../../performance-evidence/phase7-local-hardening-2026-10-05.md)
retains failed attempts alongside later passing checks.
