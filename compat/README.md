# aria2 Compatibility Reference

`aria2-reference.pin` pins the source revision used to generate option, manual,
and RPC compatibility inventories. The source checkout is external to this
repository and defaults to `../aria2` locally.

The pin uses a strict three-key `key=value` format rather than general TOML, so
the standard-library-only bootstrap verifier can reject unknown or malformed
metadata without adding dependencies.

Run `cargo xtask verify-aria2` before generation. The verifier requires the
pinned commit and extraction paths, and requires checkout `HEAD` to match the
pin. Generators must read with `git show <commit>:<path>` instead of reading
worktree files. This prevents ignored or generated aria2 build artifacts from
changing compatibility output.

Primary inputs are `src/OptionHandlerFactory.cc`, `src/prefs.cc`,
`src/OptionHandlerImpl.{h,cc}`, `src/usage_text.h`, `src/help_tags.{h,cc}`,
`src/RpcMethodFactory.cc`, `src/RpcMethodImpl.{h,cc}`, `src/RpcMethod.cc`, the
RPC tests, and `doc/manual-src/en/aria2c.rst`.
