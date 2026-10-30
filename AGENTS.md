# Agent notes for weida

## Tests

A test that touches the filesystem, an OS socket, a process, the environment
or a running clock is an integration (io) test. `SimNet`, the in-process
transports (`weida+inproc`, `inproc://`) and a paused Tokio clock
(`#[tokio::test(start_paused = true)]`) are unit-tier; a unit test runs well
under a second.

Layout (`.config/nextest.toml` selects by module path): in a source file, io
tests live in a `mod io { use super::*; .. }` nested in the test module; in a
`tests/main.rs` binary, an io file is `tests/io/<file>.rs`, declared inside
one `mod io { .. }` block of `tests/main.rs`.

No hard time limit in a test on a real clock: a limit that fires on a loaded
runner turns a healthy test red. Waits go through `weida-testkit`
(`within`, `until`, `poll_until`, `recv`), which take an expected time,
print "still waiting for ..." at each multiple of it and keep waiting; on a
paused clock the same `within` fails at the expected time, because there it
can only mean a hang. A test does not assert a wall-clock bound
(`elapsed() < x`): it runs on a paused clock where the code times itself on
the Tokio clock, and otherwise asserts the order of events. A negative window
("nothing arrives within 200 ms") is fine. nextest kills nothing
(`.config/nextest.toml`), and the CI job budget is the only hang guard.

Before a push: `cargo nextest run -p <crate>` for each crate the change
touches (the unit tier), plus the feature rows of docs/LOOP.md §6 for those
crates. The pipelines run the rest: a push to `main` runs the unit tier and
the doctests on four platforms, the nightly and a `v*` tag both tiers.
Never run `--profile integration` or `--profile ci` locally.

## Version cycle

The tree carries `0.1.0-alpha.N` only in the release commit and its tag. Every
other commit carries the next number with `-dev`
([docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md), "Library version"). After
`v0.1.0-alpha.6` the tree is at `0.1.0-alpha.7-dev`, and so on.

A release is two version commits, and it is not finished after the first one:

1. `chore(release): 0.1.0-alpha.N` drops `-dev`; tag it
   `git tag -a v0.1.0-alpha.N -m "Release 0.1.0-alpha.N"` and push both
   `git push origin main v0.1.0-alpha.N`. The tag's `release` pipeline builds
   and tests both tiers on four platforms and its `publish` job publishes the
   workspace to crates.io; wait for it with `ci_wait` (the tag's commit).
   Never `cargo publish` by hand.
2. `chore: begin 0.1.0-alpha.<N+1>-dev development cycle` follows directly,
   in the same session as the release, before any other commit. A docs
   commit that records the release (README "published" line) may sit
   between the two; nothing else may.

A failed `publish` is rerun with `ci_rerun`; it skips what crates.io already
has.

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

## Security

Advisories, fuzz crashes and agent security reviews follow
[docs/SECURITY.md](docs/SECURITY.md); an `ignore` in `deny.toml` always
names why weida is not affected.
