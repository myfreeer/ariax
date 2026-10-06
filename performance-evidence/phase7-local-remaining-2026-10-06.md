# Phase 7 Remaining Local Work

This campaign starts at `23da28b` and completes the remaining local goal. Its scope is all
remaining work executable on the existing WSL1 and Windows-GNU hosts. The
historical Windows `changeUri`/`addUri` and native Linux mixed-burst latency
failures are explicitly deferred, with their artifacts and thresholds retained.
They are not reported as fixed. Unavailable external platform gates remain
separate from completion of this local goal.

The [portable evidence](phase7-local-remaining-2026-10-06.json) binds command
records, allocation observations, plans and advisory review to the retained
`phase7-local-remaining-20261006` campaign directory (`<run>` in the portable
record and `<remaining-local-run>` in the advisory inventory). No release is
approved.

## Replay Allocation And Ownership

A standalone allocator adapter measures the existing framing and semantic
replay functions against the retained 93,550-record, 8,018,088-byte journal.
The original journal remains unchanged. Each host completes control, fixed-size
and growing-prefix modes: ten stages, five replays per stage. Both live requested
bytes and live allocation counts return exactly to their warmed baseline after
every stage. Windows private memory varies after deallocation; neither replay
mode shows sustained growth in this isolated test. This establishes temporary
allocation pressure without attributing the whole-engine private-memory drift.

The first unoptimized Linux diagnostic reached its 180-second timeout after
40 complete full-history replays. That partial result remains retained. The
same 50-replay workload completes with a declared 600-second diagnostic limit;
this is not a product latency threshold or historical latency campaign.

Active HTTP preparation previously copied replay input and payloads without a
resident reservation. The repair injects a runtime-independent storage budget,
narrows replay limits from descriptor-checked file sizes and reserves the
enforced allocation envelope before reading. Directory enumeration is bounded
from the installed segment count. Immutable result clones share their data and
reservation, including through the session completion queue; final drop releases
the charge. The process journal domain is capped at 128 MiB and competes for the
profile's shared resident permits. Semantic state and operating-system private
memory are not measurements of this framing reservation.

Focused Linux and Windows checks pass admission, rejection, retry, file growth
after admission, shared result ownership, abandoned completion and cancellation
drain cases. The additional related regressions pass on both hosts. Active
whole-engine allocation findings and current-source resource runs are recorded
below.

The 60-second instrumented smoke and 180-second normal run pass, including
crash recovery, clean shutdown and second recovery of 132 tasks. The planned
1,800-second instrumented run stops at 661 seconds: resume reaches the scheduler
while its cancellation-drain barrier is still pending. Zero connections did
not establish that the barrier had completed. The failed run and every sample
remain retained. Its paused Rust requested allocations rise by 1,950,010 bytes
and 1,801 allocations; SQLite stays at 1,266,392 requested bytes.

Source review identifies retained implicit rate buckets for each new stream
identifier. An isolated before/after diagnostic exercises 1,800 cycles of 16
new streams on each host. The original arbiter retains 28,802 scopes and grows
by 5,205,808 requested bytes between its first and last released stages. The
repair retains one explicit task scope and has zero requested-byte or allocation
count growth across those stages. Two empty map roots retain a fixed 2,064
bytes after first use; dropping the arbiter releases them. This distinguishes
live rate-history retention from bounded container capacity.

Implicit buckets now remain owned through waiters, grants, permits and prepared
option updates. Final release and bounded incremental maintenance reclaim them
only when unlimited or fully replenished, preserving consumed tokens and
overshoot debt. All 15 rate tests pass on Linux and Windows, including rejection,
cancellation, unclaimed grants, active-stream ownership and configuration pins.
One initial test expected fresh tokens immediately after changing unlimited to
a finite rate; the corrected virtual-time test waits for the existing refill
contract. Both original failed test records remain retained.

The updated active-run plan keeps the workload and resource screens, records
the exact pending-drain rejection and bounds the resume handshake at 30 seconds.
Every other RPC error still fails. The repaired binaries pass the 60-second
instrumented smoke, 180-second normal run and full 1,800-second instrumented
run. The latter completes 1,798 pause/resume cycles and 900 pulses, then crash
recovery, clean RPC shutdown and second recovery of all 132 tasks. It needs no
pending-drain retry and observes no compiler activity. Some shorter runs record
uncontrolled compilation from another workspace; none of these runs claims
isolated latency acceptance.

In the repaired 30-minute run, first/last three-sample medians are 5,048,067 and
5,047,893 Rust requested live bytes, 12,940,753 and 12,939,729 accounted resident
bytes, 422 and 418 handles, and 14 and 11 threads. Private bytes rise from
12,468,224 to 21,258,240, below the unchanged 32 MiB growth screen. Paused rate
tracking remains one explicit task scope. SQLite remains stable. The isolated
probes separate retained rate history from temporary replay pressure and empty
container capacity; the whole-engine run establishes bounded stability after
the repair. It does not prove absence of every native allocation leak or
arbitrary-lifetime process stability.

## OpenSSL And Native Consumers

OpenSSL 3.6.5 and libtorrent 2.1.1 build successfully in isolated Linux and
Windows-GNU installations. Both directly linked callback fixtures pass. All
four CTest probes also pass on each host: callbacks, bounded output, destination
policy and private storage. Linux uses `/tmp` for the private-permission fixture.
These results do not replace unavailable native kernel or other-platform gates.
All 16,772 consumed native files match the independent Linux rebuild. Each host
also passes both Rust native integration tests and focused Clippy/formatting.
The independent Windows rebuild also matches all 16,772 consumed files. Both
hosts additionally pass the two Rust native integration tests in the release
profile. All four CLI bundles on each host match independently rebuilt binaries
with the updated native installation where applicable.

## Advisory Disposition

The current upstream review finds RSA 0.10.0-rc.19, published October 6 with a
declared Rust 1.85 MSRV. Its checksum-verified archive leaves the RSA primitive,
PKCS#1 v1.5 padding/signing and OAEP implementations byte-identical to rc.18.
It updates dependency APIs and adds an invalid-prime regression; it does not
resolve Marvin. PRs 702 and 680 remain open and unmerged. Current API metadata
marks 702 as draft and 680 as non-draft, correcting the reversed draft labels
in the previous prose review.

Production SFTP uses RSA for signature verification and optional private-key
authentication signing. No production RSA decryption API was found. This is a
scoped applicability finding, not a proof of signing timing safety or dependency
clearance. Local available-fix review is complete; upstream cryptographic
assurance remains unresolved. The RSA policy error and all other retained
advisory dispositions remain visible, with no ignore or homemade cryptographic
repair. A fresh RustSec fetch retains commit `ef6173cbc5c50ec8166f9a5b28f07834144373ee`;
license/source/backend policy checks pass, with only the RSA advisory error and
no yanked warning. The current OSV scan includes 355 registry packages plus the
vendored russh upstream version and retains the same eight findings.

The updated notice collection verifies 602 references to 236 distinct texts
across 314 packages, including 304 external/vendor packages. It retains the
existing winapi/IANA/source-material review and its provenance qualifications.
All eight updated archives contain the current collection and its hash-bound
`license-inventory.json`. Their actual imports and reviewed runtime closure are
verified against the extracted contents.

## Package And Local Goal Audit

The eight Linux/Windows-GNU minimal, standard, full and compat drafts are rebuilt
from the recorded source hashes, using separate target and temporary directories
for each comparison. Native bundles use OpenSSL 3.6.5, Boost 1.91.0 and libtorrent
2.1.1. Every binary has zero known absolute workstation-path matches. Windows
full/compat archives contain exactly the reviewed three-DLL runtime closure;
missing and modified DLL copies are rejected. Linux system dependencies remain
host prerequisites.

Every extracted package passes its feature query, RPC success and rejection,
EOF shutdown and SQLite reopening checks under a reduced environment. Windows
loads the reviewed DLLs from the package directory. Linux retains its configured
system preload. Neither result establishes fresh/minimum-OS acceptance.

The final staged check catches Git line-ending normalization of the new notice
collection. Extending the existing upstream-byte attributes to every dated
collection preserves its verified hash, whitespace and OpenSSL heading. The
initial failed check is retained; the corrected staged bytes match the packaged
notice hash.

| Goal Requirement | Local Disposition | Remaining Limit |
| --- | --- | --- |
| Memory, accounting and lifecycle | Replay admission and rate-history ownership repaired; focused checks and the full bounded active/recovery run pass. | Bounded stability is not an arbitrary-lifetime leak proof. |
| OpenSSL and native consumers | Independent installations match; linked probes, native integration and rebuilt consumers pass. | Unavailable kernel and other-platform acceptance remain separate. |
| Advisory review and available fixes | All eight findings have current local applicability/mitigation dispositions; available RSA releases and fixes reviewed. | RSA cryptographic assurance remains upstream-dependent; policy failure and findings remain visible. |
| Experimental compatibility and packaging | Eight exact-source graphs and rebuilt draft pairs, notices, archives and extracted-package checks complete. | Reviewed handler coverage remains 54/207; fresh/minimum OS and the final candidate matrix remain open. |
| Evidence and checkpoint | Source, binary, archive, command and notice identities are retained; owned-process cleanup and final integrity are verified in the portable record. | Historical failures and useful fixtures/caches remain retained; no release, push, CI dispatch or migration. |

Historical Windows `changeUri`/`addUri` and native Linux mixed-burst latency
attribution remain explicitly deferred. No reproduction campaign or threshold
change was made for them. Supporting native Linux kernel/`io_uring`, fully
instrumented Rust standard-library/harness TSan, macOS/MSVC, fresh/minimum OS,
hardware power loss and the complete release matrix remain external gates.
They are not recorded as passing and do not block completion of this scoped
local goal.
