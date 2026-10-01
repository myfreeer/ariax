# Pinned Libtorrent Build

`sources.json` pins the upstream source archives and build settings for
libtorrent 2.1.1, Boost 1.91.0 headers and OpenSSL 3.6.3. Their published
SHA-256 values were independently verified before use. Downloads, source trees,
native libraries and build logs live under ignored `toolchains/bt-native`.
No installed system libtorrent or OpenSSL is substituted.

CI runs `python3 scripts/bt_native.py --target <Rust-host-triple>` before building
the `bt` feature. Cargo verifies an existing installation and never downloads or
builds native dependencies implicitly. Full platform validation belongs in CI;
local work uses focused checks and minimal debugging. Native Windows GNU runs
the provisioning command through MSYS2 MINGW64;
MSVC runs in its native developer environment. The builder records the target,
compiler, source and patch digests, required settings and installed file hashes,
including every consumed Boost header,
in each target's `install/ariax-native.json`. Cross-target reuse is rejected.

`ariax.patch` adds a storage admission hold checked by every torrent
initialization path. Held metadata is queryable independently of alerts.
Approval installs every file mapping and priority together before initialization;
the safe adapter releases it only after durable metadata admission. The patch
also adds a tracked callback after pausing and completing the disk release
barrier. The callback owns its result independently of the alert queue; its
bridge implementation catches every exception and bounds serialized output.
Patch level 2 also filters outgoing DHT packets and validates tracker/web-seed
redirect credentials, schemes and hop bounds. The filter is installed before
the session starts. `tests/security.cc` and `tests/endpoints.cc` exercise URL
policy, allowed/blocked DHT traffic, real tracker and web-seed redirects, DNS
filtering and tracker-discovered peers. Allowed web-seed probes require an
actual payload response and check the downloaded bytes after native shutdown
has drained disk writes. Patch level 3 excludes web seeds with no remaining files
from the connection-slot count, allowing redirect chains to progress under the
default limit. Tests require payload delivery after 20 redirects and rejection
of the next hop. Patch level 4 creates Unix payload and part files with `0600`
permissions, executable payloads with `0700`, and directories with `0700`,
including stdio storage. Existing path permissions remain subject to adapter
validation. Native tests exercise creation under umask `0000`; adapter tests
cover nested payload restore and rejection of shared writable paths.
Patch level 5 gives Windows native and stdio payloads, part files, and directories
protected private ACLs at creation. Opening an existing path preserves its ACL.
Native storage tests and adapter restore tests verify the resulting protection.

The patcher reads UTF-8 and writes LF independently of the host locale. It
validates all source hunks before writing, including on Windows with a legacy
code page. The adapter and engine tests exercise real payload I/O and recovery.

`--sanitizer address` builds libtorrent and OpenSSL with ASan/UBSan under a
separate target directory. The manifest records the instrumentation, and Cargo
requires a matching `ARIAX_BT_SANITIZER` setting before linking. Configure,
build and install use the same `RelWithDebInfo` configuration so installed
CMake targets describe the instrumented library. This mode is used by native CI
only.

Libtorrent uses the upstream BSD license, Boost the Boost Software License 1.0,
and OpenSSL Apache 2.0. The builder retains their license texts in the native
installation. The project patch is distributed under the repository license.
