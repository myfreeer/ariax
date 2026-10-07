# Dependency And Binary Size Analysis

The current full/OpenSSL CLI has one measured size reduction ready for a
follow-up build-policy change: Linux packed relative relocations save
**1,097,880 bytes (4.39%)** using the same compiled objects. No conflicting
strong global symbols were found. Native-library retention offers more useful
investigation targets than Cargo version deduplication alone.

This is an analysis checkpoint. Production binaries, dependency versions,
release flags and the package catalog remain unchanged. The
[measurements](phase7-binary-size-2026-10-07.json) supplement the
[current build evidence](phase7-compatibility-options-2026-10-07.md).
They do not close the outstanding P7 release gates.

The [implementation follow-up](phase7-size-reduction-2026-10-07.md) records
the adopted changes, replacement binaries, focused validation and cleanup.
The baseline measurements and recommendations below remain historical.

## Scope And Method

Source: `5d7f902869fd1c4d8ce66c79513b73535bd4d97e`. Both targets use
Rust 1.97.1, the `full` bundle and automatic OpenSSL TLS/RSA selection.
`release-cli` already has optimization level 3, fat LTO, one codegen unit,
abort-on-panic and symbol stripping. Both links already discard unused sections.

One temporary unstripped CLI build per host reused the final dependencies and
native installations. At most two compiler workers ran. Link maps and saved
objects allowed additional Linux linker experiments without recompilation.
Every baseline allocated section matches its symbol-bearing counterpart;
only the Linux build-id differs. Windows section bytes match exactly.
Original final binary hashes were preserved throughout.

Package counts come from target-filtered `cargo tree` with normal edges,
`--no-dedupe` and procedural-macro subtrees excluded. Build dependencies and
workspace-wide metadata feature unions are not counted as runtime code.
Post-LTO symbol names cannot attribute inlined bytes precisely: named-family
figures below are partial and must not be added together as crate totals.

## Baseline Size

| Measurement | Linux x86-64 | Windows GNU x86-64 |
| --- | ---: | ---: |
| Final executable | 24,991,648 B (23.83 MiB) | 25,228,288 B (24.06 MiB) |
| `.text` | 18,435,214 B | 20,261,920 B |
| Read-only data | 2,084,345 B | 3,503,968 B |
| Main unwind tables | 2,071,004 B | 1,077,256 B |
| Relocation records | 1,140,672 B | 103,992 B |
| Normal runtime packages, including workspace crates | 257 | 268 |

Linux unwind combines `.eh_frame` and `.eh_frame_hdr`; its C++ exception table
adds 141,292 bytes. Windows unwind combines `.pdata` and `.xdata`. Linux
relocations combine `.rela.dyn` and `.rela.plt`; Windows uses `.reloc`.
These rows are selected sections, not a complete file-size accounting.
BSS and TLS memory sizes are not executable-file savings.

Link maps attribute the following input code bytes, excluding alignment gaps:

| Code owner | Linux | Windows |
| --- | ---: | ---: |
| Rust after fat LTO, including Rust dependencies | 9,907,673 B | 10,082,264 B |
| libtorrent | 3,674,615 B | 4,039,344 B |
| OpenSSL crypto, including built-in provider objects | 2,333,087 B | 2,671,760 B |
| OpenSSL SSL | 612,749 B | 696,096 B |
| SQLite | 1,662,280 B | 1,726,688 B |
| Ariax C++ bridge | 66,461 B | 69,232 B |
| ring native routines, excluding Rust wrappers | 40,659 B | 43,909 B |
| Other native code/runtime | 126,325 B | 924,745 B |

Windows statically includes the C++/GCC/thread runtime code required by the
package policy; Linux imports its system C++ runtime. Switching Windows back
to runtime DLLs would violate that policy. Windows imports still contain only
Windows-provided DLLs. Linux retains its recorded glibc 2.38 / GLIBCXX 3.4.30
minimums and system runtime imports.

## Conflicts And Duplicates

Both symbol tables have **zero strong global names defined at multiple
addresses**. Neither link command permits conflicting multiple definitions.
All linked native OpenSSL code comes from the native adapter's archive members;
the additional `openssl-sys` dependency does not introduce another live native
OpenSSL copy. Representative `SSL_new`, `EVP_DigestInit_ex`, `EVP_PKEY_sign` and
`OPENSSL_init_crypto` symbols each have one definition. This checks these exact
artifacts, not every possible future feature combination or C++ ODR violation.

Windows has 26 import descriptors for 12 unique system DLLs, including nine
for `kernel32.dll`. Multiple Rust/native import-library producers retain
separate descriptors. This is import-table duplication, not additional runtime
DLL distributions. The entire `.idata` section is only 16,528 bytes; changing
import generation has low priority and requires Windows loader validation.

The five runtime dependency families with multiple versions are:

| Family | Versions | Cause And Disposition |
| --- | --- | --- |
| `getrandom` | 0.2.17 / 0.4.3 | ring's older interface and the current RustCrypto/random stack. Small platform shims; do not force an incompatible version. |
| `hashbrown` | 0.14.5 / 0.17.1 | DashMap versus rusqlite's Hashlink. Different API generations; the standard library also has its own internal hash-table implementation. |
| `generic-array` | 0.14.9 / 1.4.5 | russh explicitly enables `compat-0_14`. No standalone named functions remain in the inspected Linux output; this does not prove zero inlined code. |
| `sha3` | 0.11.0 / 0.12.0 | ML-KEM versus russh. Only 706 bytes of standalone SHA3-named text and one 1,385-byte Keccak permutation were identified after LTO; two crate versions do not imply two complete permutations. |
| `winnow` | 0.7.15 / 1.0.4 | TOML versus its newer parser dependency. No standalone named functions remain; parser code may be inlined. |

`syn` 2/3 and several repeated same-version entries in `cargo tree -d` are
build/procedural-macro or host/target feature contexts. They are not evidence
of multiple runtime copies. Unifying incompatible transitive versions by
patching the lockfile is not a safe size optimization.

OpenSSL is already the selected TLS and SSH RSA backend. The compiled
OpenSSL branch does not construct the ring TLS provider, and no corresponding
provider symbols remain. **ring itself is still live** for russh AES-GCM and
OpenSSH ChaCha20-Poly1305. Its named Rust routines account for another 18,480
bytes on Linux, with inlined code unattributed. Removing it requires an actual
SSH cipher backend change. The native OpenSSL build currently uses `no-asm`,
so substituting its routines requires performance measurements.

RustCrypto RSA/key-format code also remains: 39,111 bytes of RSA-matching
named text were identified, including private-key conversion and precomputation.
SSH key/certificate handling and other algorithms prevent simply deleting the
RustCrypto stack. The existing RSA advisory disposition is unchanged.

Exact Linux function-byte comparison found 81 duplicate groups among functions
at least 32 bytes long, after excluding aliases at the same address. Their
duplicate bytes total 39,979. Many are OpenSSL dispatch adapters. This is an
upper bound for those matched bytes, **not proven safe savings**: function
addresses can be observable, and identical bytes at different locations need
not have identical semantics. Safe ICF measured zero savings.

## Dead Code And Retained Functionality

Disabling Linux section garbage collection increases the executable by
108,096 bytes, to 25,099,744. The normal linker reports 4,883 removed-section
entries, including empty sections. This is additional to Rust LTO's earlier
elimination and archive members never extracted. Those counts are not a count
of remaining dead functions.

OpenSSL's native archives mostly use one `.text` section per object, rather
than one per function. libtorrent has many separate template sections but also
2,507,668 Linux / 3,053,648 Windows bytes in ordinary object `.text` sections.
The linker cannot discard an unused function independently when it shares a
section with a live function. Native function/data sections are therefore the
first cross-platform build experiment to try, keeping optimization level 3.

Other retained code deserves targeted review:

- **Native OpenSSL QUIC:** linked QUIC object code totals 188,975 bytes on
  Linux and 210,528 on Windows. Current P7 transports do not offer HTTP/3.
  These bytes are not automatically removable: libssl's shared entry points
  and dispatch structures can reference them. Test an explicit `no-quic`
  build against the current libtorrent/TLS contract before adopting it.
- **SQLite FTS and RTree:** Linux named text contains 351,742 bytes of
  FTS3/FTS5 routines and 29,702 bytes of RTree routines. The bundled SQLite
  builder enables these, but no corresponding schema/query use was found in
  Ariax storage code. Initialization registers the modules, so ordinary dead
  stripping keeps them. Removing them needs an explicit database compatibility
  decision and migration/recovery tests; these numbers are not measured savings.
- **Filtered TLS key exchange:** Ariax constructs the default OpenSSL provider
  before retaining its three classical groups. The provider first probes every
  default group with `start()`, including hybrid ML-KEM. The binary retains
  8,904 bytes of matching wrapper text despite the final group exclusion.
  Constructing and probing only the approved groups can avoid unnecessary
  work and retention. Preserve runtime availability checks, signature/cipher
  filtering, group order and failure behavior.
- **Journal replay specializations:** `process_records` has three bodies of
  33,217, 33,219 and 33,393 bytes. The policy type causes repeated compilation
  of substantial recovery logic. Factoring policy-independent logic may help;
  replacing everything with dynamic dispatch needs replay benchmarks.
- **FTP regex engine:** 350,024 bytes match `regex_automata` names. SuppaFTP
  uses regexes for protocol responses. A bounded parser or narrower regex
  feature set is a possible later change, but must preserve accepted/rejected
  syntax, Unicode behavior and malicious-input bounds. This code is live.

Do not infer deadness from an absence of direct callers: C++ vtables,
constructors, OpenSSL providers, SQLite registrations and FFI callbacks retain
callable code. Removing algorithms, unwind tables or these registration roots
without their owning contract would threaten correctness.

## Measured Linker Experiments

All variants reuse the same optimized Linux objects and native archives.

| Variant | Executable Bytes | Reduction |
| --- | ---: | ---: |
| Current flags | 24,991,648 | — |
| `-Wl,-z,pack-relative-relocs` | 23,893,768 | 1,097,880 B / 4.39% |
| `-Wl,--icf=safe` | 24,991,648 | 0 |
| Both flags | 23,893,768 | 1,097,880 B / 4.39% |

Packed relocations replace 46,337 ordinary relative relocation entries with
a 14,112-byte `.relr.dyn` table; non-relative records remain. The linker emits
the `GLIBC_ABI_DT_RELR` requirement. glibc 2.36 or later supports this format,
so it does not raise this particular binary's existing glibc 2.38 floor.
Any future older-glibc release target needs its own policy. This changes
loader metadata and addresses, not compiler optimization or algorithm selection.

All four variants passed version/help and invalid-option rejection. Baseline
and packed-relocation binaries also passed RPC version/stat queries, unknown
method rejection, EOF shutdown and database reopen. Imports remained unchanged.
Fifty alternating warm `--version` launches each measured median elapsed time
of 6.51 ms for baseline and 6.21 ms with packed relocations. These WSL1 results
show no observed startup regression; they are not transfer-throughput or
tail-latency acceptance evidence. The existing host preload remained untouched.

## Recommended Order

1. Adopt packed relocations for the current compatible Linux target in a
   follow-up build-policy change, then regenerate its package identity and smoke
   evidence. Windows has no corresponding ELF relocation change.
2. Measure native function/data sections on both hosts, preserving current
   optimization and algorithms. Validate callbacks, FFI, TLS/RSA, BitTorrent
   and representative performance before replacing native installations.
3. Construct only approved OpenSSL TLS groups while preserving availability
   behavior; test handshakes and rejection paths.
4. Review unused SQLite modules and native QUIC against their owning contracts,
   then measure separately. Factor journal replay only with recovery/performance
   evidence. Dependency-version cleanup has lower priority.

Do not use size-oriented optimization levels, aggressive ICF, runtime DLL
substitution, executable compression, disabled exception handling or reduced
crypto/protocol support as unmeasured shortcuts. This analysis does not justify
changing them under the requirement to preserve performance and correctness.

## Artifact Disposition

Temporary symbol binaries, maps, saved LLVM/object files, extracted linker
archives, experiment executables and smoke databases were removed after
consolidation: 884,715,814 logical file bytes were removed. Only this report,
its machine-readable measurements and a documentation link are added.
The original two final build roots, toolchains and dependencies remain; no
saved binary collection, native rebuild, compression or CI run was created.
