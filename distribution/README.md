# Distribution Materials

[Release packaging guide](../docs/development/release-packaging.md) ·
[License review](license-review.json)

These materials prepare CLI package manifests. They do not authorize a release.
The project [MIT license](../LICENSE) applies to Ariax-owned code; files under
`licenses/` preserve original third-party terms and bytes.

`package-manifests.json` describes all four bundles for Linux and Windows-GNU.
All eight entries now have concrete retained draft inventories. Full/compat
include verified native build identities and runtime imports; Windows packages
carry the exact three-DLL closure in [runtime-files.json](runtime-files.json).
macOS and MSVC need separate manifests.

Run `python3 -B scripts/release_manifest.py` to validate the catalog. To stage
the eight retained drafts, supply `--artifacts ARTIFACT_ROOT --output NEW_DIR`.
The input root must provide each catalog `artifactPath`: retained
`linux-complete`/`windows-complete` minimal/standard binaries, full/compat
`cli-linux-a`/`cli-windows-a` binaries and the reviewed `runtime` files.
Preparation verifies hashes before copying,
rejects unsafe or duplicate paths and unknown runtime dependencies, and writes
per-package manifests and checksums. It never builds, executes the binaries,
dispatches CI or publishes a package.

The four minimal/standard drafts pass their path and runtime inspections and match
[independent local rebuilds](../performance-evidence/phase7-release-verification-2026-10-05.md).
Compilation uses separate target/temp directories with the same installed
toolchain and source cache. Reduced-environment checks pass on both current
hosts; Linux also loads its configured system preload. These checks do not
establish fresh-OS or minimum-OS acceptance. Earlier binaries and failed path
audits remain in the prior evidence and Git history.

The [full/compat follow-up](../performance-evidence/phase7-release-blockers-2026-10-06.md)
adds four archives with verified extracted inventories, notices, independent
native/CLI build matches and reduced-environment package operation on both
hosts. All eight entries remain drafts; their distinct source identities are
retained. Temporary combined inputs and duplicate staging copies were removed
after verification; archives, extracted packages and independent binaries remain.

The conservative dependency notice collection includes normal/build dependency
closures across all eight bundles. It can contain unused source notices and
historical open items; `license-review.json` records this slice's resolutions.
Final release notices should be checked against the actual candidate archives.

The [aria2 source notice](aria2-source-notice.txt) and its GPL text belong to
source distributions. Extracted inventory JSON files and those source-only
notices are excluded from the binary package catalog. The exact MPL-covered
public-suffix data and its license are included in every binary package.
