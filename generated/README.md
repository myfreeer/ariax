# Generated Contracts

Files in this directory are deterministic implementation inputs generated from
the pinned aria2 Git objects and, in later checkpoints, ariax's reviewed
registries. Do not edit generated JSON by hand.

Run `cargo xtask generate ../aria2` to update the files and
`cargo xtask generate --check ../aria2` to verify that committed output is
current. Generation requires the checkout `HEAD` to match
`compat/aria2-reference.pin` and never reads mutable source files directly.
