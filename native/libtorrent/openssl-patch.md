# OpenSSL Callback Backport

`openssl.patch` applies exactly to the pinned OpenSSL 3.6.3 archive. It repairs
the incompatible callback invocations found by Clang UBSan during standalone
random generation and native BitTorrent validation. Function-type checking,
AddressSanitizer and fail-fast behavior remain enabled.

## Upstream Sources

These changes are adapted from the OpenSSL repository. Commit identifiers are
immutable references; the builder uses the reviewed local patch and never
downloads a moving development branch.

| Area | Upstream Commits |
| --- | --- |
| Stack comparison adapters | `25a51c5cacbe7b1d1aadb3037a96bdfd09faaf45`, `e94e75d23eb36e6d75aaa52c7efdbf273628381e` |
| Stack copy and failed-copy cleanup | `74edd30c9f6b3a583ca05fc97f296ddd0e161b78`, `b1389437f56ace99f2a59a8a78fbf3f69b487d34` |
| Typed constructors and duplication | `0b555646d05439a5a3609474dde80a3c410cff76`, `30db55ac33cf6b815067149c233f45e4199df9b0`, `a1e4bd14e59e62943fe048b944eac894cd0d8489` |
| Sparse-array and EVP enumeration adapters | `d4da2e74abcbecddf639c97f95d76e06bdbe76f3`, `c84affae0b848b8194a045d26895f47d52199d8d` |
| ASN.1 const callback dispatch | `39f46844c6e06e26bea34f300c7fc61c06bb20b8` |
| PEM certificate decoding | `0e8f2844ed3e6c8fde0e5da0db6322735eb6593f` |
| Decoded-key cleanup adapters | `ba15a13ffeca1e8b1a3042bdd383c3d45e7906d1` |
| SHA update adapter approach | `85f6102785af5b9382e5a449e5a2bc183c32e0f6`, `8dfa6cdc26a75589b129f453b5a5fa4807e4906f`, `1f2ae01f5ba4b5711f814942e23450e0d7e4dcb9`, `11e1a4841acecb3c30b835f0d16dfd3adf870637`, `18ca04616f69d7b1b173014ce3eb178b073a22cd` |

## Version Adjustments

The patch preserves the 3.6 binary-search overflow fix, mutable stack-lookup
signatures and insertion behavior. Null-stack copies zero-initialize the new
adapter fields. Generated stack constructors install comparison, copy and free
adapters, including when duplicating a null stack. Changes to the generator are
applied before OpenSSL generates its public headers in a fresh build.

SHA adapters live at provider dispatch and preserve the existing low-level
digest ABI; the 4.x digest serialization refactor is not required. Portable AES
encrypt/decrypt adapters match `block128_f` while calling the existing typed
AES functions directly. The builder retains its existing `no-asm` configuration.
The key-decoder changes apply to the 3.6 `.c.in` template; the later LMS code
does not exist in this version. The stack adapter helpers are linked through
the existing static-library build; 4.x shared-library symbol ordinals are not
copied into the 3.6 symbol table.

This is a backport for the observed callback paths, not a claim that every
OpenSSL API and cipher configuration has been audited for function-type UB.
The standalone regression covers success, rejection, sorted and unsorted
lookup, and cleanup after a partial deep copy. Real native bridge and endpoint
tests remain necessary alongside it. Replace the backport with an upstream
release only after checking the same strict regressions.
