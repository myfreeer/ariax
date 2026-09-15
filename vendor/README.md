# Protocol Forks

The workspace pins SuppaFTP 10.0.1 and russh-sftp 2.3.0 through local Cargo
patches. Upstream archive hashes, VCS commits and original file inventories are
in `*-upstream.json`. `protocol-patches.json` records the complete patched
inventories and tree/patch hashes; `*.patch` contains the reproducible changes.
Git attributes preserve these exact bytes across checkouts, including upstream
line endings and whitespace; the verifier checks the complete file inventory.

SuppaFTP enforces bounded control lines/replies/counts, matching multiline
terminators and metadata-only dependency logs. Owned implicit-TLS and active
listener APIs let Ariax authorize active peers before TLS and bound rejection.
russh-sftp checks frame lengths before allocation, closes malformed streams,
bounds outstanding requests and drains terminal replies exactly once. SFTP
handles are opaque bytes throughout the client, server and example APIs.

The original SuppaFTP archive omitted its parent-directory license files.
`suppaftp-licenses.json` identifies the MIT and Apache-2.0 files fetched from its
pinned upstream commit; both are retained in the fork. russh-sftp retains its
upstream Apache-2.0 `LICENSE`.

Run `python3 scripts/verify-protocol-vendors.py` and
`python3 scripts/verify-protocol-features.py` from the repository root. The
workspace's `vendor_contracts` integration test exercises the exact resolved
production dependencies without bringing upstream Docker test dependencies
into the workspace. Native FTP/FTPS/SFTP tests cover the complete adapters.

After an intentional fork change, regenerate evidence with
`python3 scripts/verify-protocol-vendors.py --refresh ARCHIVE_DIRECTORY`, using
the exact pinned `.crate` archives. Review the patch and new hashes together.
