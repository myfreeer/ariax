# Generated Contracts

Files in this directory are deterministic implementation inputs generated from
the pinned aria2 Git objects and ariax's executable core and option registries.
The compatibility artifacts expose unreviewed upstream coverage instead of
silently implying parity. Later checkpoints add persistence contracts. Do not
edit generated JSON by hand.

`options.json`, `aria2_compat.json`, `runtime_updates.json`, and
`runtime_compatibility.json` come from `ariax-config`. The aria2 inventory files
come only from immutable blobs at the pinned commit. `storage_layout.json`
comes from executable path, root-binding, layout, and offset-mapper contracts.
`runtime_buffers.json` records the closed buffer lifecycle, size classes,
budget, quarantine, queue-credit, and completion-permit rules.

Run `cargo xtask generate ../aria2` to update the files and
`cargo xtask generate --check ../aria2` to verify that committed output is
current. Generation requires the checkout `HEAD` to match
`compat/aria2-reference.pin` and never reads mutable source files directly.
