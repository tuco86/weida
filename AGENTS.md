# Agent notes for weida

## Version cycle

The tree carries `0.1.0-alpha.N` only in the release commit and its tag. Every
other commit carries the next number with `-dev`
([docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md), "Library version"). After
`v0.1.0-alpha.6` the tree is at `0.1.0-alpha.7-dev`, and so on.

A release is two version commits, and it is not finished after the first one:

1. `chore(release): 0.1.0-alpha.N` drops `-dev`, then tag `v0.1.0-alpha.N`
   and publish.
2. `chore: begin 0.1.0-alpha.<N+1>-dev development cycle` follows directly,
   in the same session as the release, before any other commit. A docs
   commit that records the release (README "published" line) may sit
   between the two; nothing else may.

Both commits touch the same places:

- `Cargo.toml`: `[workspace.package] version` and every `weida*` entry in
  `[workspace.dependencies]`.
- `crates/mqtt/weida-mqtt-py/Cargo.toml`: the `weida-mqtt` path dependency
  names its version itself (it cannot inherit through `workspace = true`).
- Lockfiles: `cargo update -w --offline` in the root and with
  `--manifest-path` in each fuzz workspace (`crates/*/*/fuzz`,
  `crates/protocol/fuzz`).

`crates/py/weida-py/smoke.py` derives the PEP 440 version from the crate
version and needs no edit.

Check after the second commit: `git grep -n '0\.1\.0-alpha\.N"' -- '*.toml'
'*Cargo.lock'` finds nothing, and `git log --oneline` shows the `-dev` bump
on top of the release commit.
