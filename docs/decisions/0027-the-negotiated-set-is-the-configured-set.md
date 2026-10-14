# 0027 — A live connection's guarantee set is the one it configured, so nothing reports it

- **Status:** accepted
- **Date:** 2026-09-14
- **Accepted:** 2026-10-13
- **Items:** B-262, found by B-256
- **Supersedes:** nothing
- **Related:** [0006](0006-guarantee-sets.md), [GUARANTEES.md](../GUARANTEES.md) §4,
  [PROTOCOL.md](../PROTOCOL.md) §6.1, [GUIDE.md](../GUIDE.md) §4.5

## 1. The question

Writing [GUIDE.md](../GUIDE.md) §4 — the chapter about what a chain of hops may claim — needed
a program that prints what a connection had negotiated. There is no way to ask.

`GuaranteeSet::intersect` is public and a caller can compute what two sets *would* agree on.
What no public accessor reports is what a **live** connection actually agreed to: `Agreed` and
`ConnCtx::guarantees` are `pub(crate)`, and nothing on `Runtime`, `Listener` or any `Endpoint`
exposes them. Meanwhile [GUARANTEES.md](../GUARANTEES.md) §4 describes the negotiated set as a
property of a connection, which reads like something an application can observe.

So: add the accessor, or say why there is none.

## 2. What the code actually does

Three facts, each checked rather than assumed.

**The declarations are on the wire.** HELLO carries `guarantees_offered` (key `5`) and
`guarantees_required` (key `6`), encoded and decoded, with a `core` set omitted so a v0 HELLO
stays byte-identical (`crates/protocol/src/header.rs`). A header that requires more than it
offers is rejected by the **decoder**, before negotiation. GUARANTEES.md §4 carried a caveat
saying these fields were "not on the wire yet"; that caveat was stale and is removed by this
note.

**Negotiation is symmetric and has no downgrade path.** `negotiate` intersects the two offered
sets and then checks the result against *both* sides' requirements, failing with
`NEGOTIATION_FAILED` if either is unmet (`crates/protocol/src/negotiate.rs`). Both peers run
the same function over the same two headers, so both reach the same verdict without a round
trip.

**Offered and required are one setting.** `RuntimeConfig::guarantees` is what a runtime offers
*and* what it requires — a decision [0006](0006-guarantee-sets.md) §4.4 already took, on the
grounds that a set is a statement of what this side runs and a peer that cannot match it should
fail the handshake rather than quietly give less.

## 3. The consequence

Those three facts make the accessor pointless, and the argument is two lines of arithmetic.
For a connection that is **live**, with `mine` and `theirs` the two configured sets:

> `agreed = min(mine, theirs)` per dimension, and `agreed ≥ mine` — or the connection would
> have failed — therefore **`agreed = mine`**.

A `connection.guarantees()` would return `config.guarantees`. It would also be a small trap: a
caller who asks would reasonably expect the answer to *vary*, and would write a branch that can
never be taken.

What a peer that wants more than its peer offers receives is not a weaker connection but
**none**: `Error::Negotiation` at connect time, before any message exists. Measured at this
commit, against a core listener:

| Dialling runtime's set | Outcome |
| --- | --- |
| `core` | `Ok(())` |
| `core` with `delivery: AtLeastOnce` | `Err(Negotiation("peer closed the connection: negotiation failed"))` |
| `core` with `acknowledgement: Accepted` | `Err(Negotiation("peer closed the connection: negotiation failed"))` |

So the observation an application needs is **the connection having succeeded**, and it already
has it.

## 4. The decision

**4.1 No accessor for a live connection's negotiated guarantee set.** Not on `Runtime`, not on
an `Endpoint`, not on a per-connection handle. The configured set is the answer for every live
connection, and the runtime's copy is an implementation detail of enforcement.

**4.2 The documents say so.** [GUARANTEES.md](../GUARANTEES.md) §4 gains the sentence, with the
arithmetic, and loses the stale caveat about the wire.

**4.3 The arithmetic is tested where negotiation is implemented.**
`crates/protocol/src/negotiate.rs` asserts that the effective set is the weaker offer per
dimension, that a required level the peer does not offer fails, and that unordered dimensions
must match exactly. Connection-level hostile tests separately assert that a negotiation
failure closes with `NEGOTIATION_FAILED`.

**4.4 Error wording remains observational.** Which side sees a negotiation error before the
connection close is still a race; tests assert refusal and the protocol code, not one
transport-dependent sentence.

**4.5 This is revisited when either half of "one setting" stops holding.** Two futures would
change the answer:

- **Separate offered and required** ([0006](0006-guarantee-sets.md) §4.4 keeps them together
  for v0). Then `agreed` can be strictly between the two and an accessor reports something a
  caller does not already know.
- **Per-connection sets** rather than per-runtime, which the control tier of
  [0002](0002-control-and-bulk-separation.md) §6.3 would want if it ever carried a different
  profile. Then "the configured set" is ambiguous and the accessor needs a connection to name.

Until one of those exists, an accessor is an API with one possible answer.

## 5. What this does not decide

**A hop's claim is still local.** Nothing here helps an application infer what a later hop
promised. A future managed Connector declares the guarantee of its one configured source–Queue
or Queue–sink path as resource policy; no runtime accessor turns that into a protocol-wide
mapping. The accessor rejected here would still report only this connection's configuration
([GUARANTEES.md](../GUARANTEES.md) §1).

**Nothing about broker-level completion.** `Accepted`, `Stored`, `Replicated` and `Processed`
are reserved for a hop that owns the message, and what a queue reports about them is
[0018](0018-minimal-broker.md) and [0023](0023-completion-is-a-cursor.md), not this note.
