# Security checks

How weida checks its own security: what runs automatically, how a finding is
handled, and how an agent reviews the code.

## 1. What runs where

|Check|Runs|Catches|
|---|---|---|
|`audit` (`cargo deny --locked check advisories sources`, `deny.toml`)|nightly, release (gates `publish`)|known advisories and yanked crates in `Cargo.lock`, non-crates.io sources|
|`fuzz` (all `cargo fuzz` targets, 60 s each)|nightly|panics, overflows, OOM and timeouts in the codecs|
|`tests/fuzz_smoke.rs`, `hostile.rs`|every push|regressions of past findings|
|agent review (section 4)|on demand|logic flaws in weida's own code|

`audit` covers the root `Cargo.lock` only. The fuzz workspaces
(`crates/**/fuzz`) add nothing but `libfuzzer-sys` and `arbitrary`, and
nothing from them ships.

## 2. An advisory

- Find the path: `cargo tree -i <crate>@<version> -e normal,dev,build`.
- Fix with `cargo update -p <crate>@<version> --precise <patched>`, or raise
  the direct dependency.
- Only when weida is demonstrably not affected, add an `ignore` entry to
  `deny.toml` whose `reason` names why:
  `{ id = "RUSTSEC-...", reason = "<crate> is reached only as ..., which ..." }`.
- Never ignore without a reason. Remove an ignore once the path is gone or
  the advisory no longer matches; cargo-deny then reports it as
  `advisory-not-detected`.
- Check locally with `cargo deny --locked check advisories sources`.

## 3. A fuzz crash

1. Take the `artifact <path> base64 <data>` line from `ci_log` of the
   nightly's `fuzz` job (pattern `artifact|fuzz FAILED`).
2. Decode it into `<crate>/fuzz/artifacts/<target>/<name>` with `base64 -d`.
3. Reproduce from `<crate>`:
   `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<name>`.
4. Add the input as a deterministic case to that crate's
   `tests/fuzz_smoke.rs`, then fix. When the panic is the target's own
   assertion and the code under test behaves as specified, the oracle is
   wrong: fix the target instead.

## 4. An agent review within the generally available safeguards

**Scope.** Anthropic's
[Cyber Verification Program announcement](https://www.anthropic.com/news/cyber-verification-program)
lists code review, patching known issues, vulnerability finding in owned
source code and triage of security alerts as uses of its generally available
models. weida is our own code, so a review of it is in scope.

**Work unit.** One trust boundary per session. From the README crate table:

- the protocol codec and HELLO negotiation (`weida-protocol`)
- the connection engine, limits and overload (`weida`)
- the `AF_UNIX` peer credentials and named-pipe DACL (`weida-runtime`,
  `weida-winpipe`, the only `unsafe`)
- broker credit and settlement (`weida-broker`)
- Raft transport (`weida-raft`)
- the OpenBao client over `weida-http` (`weida-openbao`)
- each foreign-protocol codec

**Prompt shape.**

> Review `<crate/path>` of weida, which we maintain, for vulnerabilities:
> unbounded allocation, integer overflow, panics on peer input, missing
> limits, authentication or authorization gaps, `unsafe` preconditions. For
> each finding: file and line, the input or sequence that triggers it, the
> fix, and a regression test in the crate's `tests/` (`fuzz_smoke.rs` or
> `hostile.rs`).

**The reproducer is a test.** It is a test against the crate's own API:
bytes into a decoder, a `SimNet` or in-process peer. It is never a standalone
exploit, a network attack tool, a scan of hosts, or a payload against a
third-party service. That is the boundary the safeguards enforce, and the
test is what the repository needs anyway.

**Triage of tool output.** Paste the `audit` or `fuzz` output, then ask
whether weida is affected and for the fix.

**In omp.** The `security-reviewer` agent does this read-only. Its findings
go back to a normal session for the fix and the test.

**When blocked anyway.** Report the block with the
[false-positive form](https://claude.com/form/cyber-block-false-positive-report-cvp-rejection-appeal).
For deeper work (malware or exploit validation), apply for the CVP Defense
Access tier at <https://portal.anthropic.com/programs/cvp>; it lists
open-source maintainers explicitly and requires data retention.
