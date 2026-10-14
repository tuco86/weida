# 0028 — A trace context is propagated, never minted

- **Status:** accepted
- **Date:** 2026-09-14
- **Accepted:** 2026-10-13
- **Items:** B-246
- **Supersedes:** nothing
- **Related:** [PROTOCOL.md](../PROTOCOL.md) §6.2, [0001](0001-sequence-field.md),
  [0026](0026-the-guide-and-the-c8b-question.md) §5, [GUIDE.md](../GUIDE.md) §0.3

## 1. The question

`data_header` in `crates/weida/src/transfer.rs` did this:

```rust
let trace = meta.trace.unwrap_or_else(new_trace_context);
… traceparent: Some(trace.to_traceparent()) …
```

So **every** DATA header of **every** pattern carried a 55-byte W3C `traceparent` plus its
CBOR framing — 58 bytes with the key and the text prefix — whether anything traced or not, and
a caller who supplied no context got a freshly minted root.

Measured, on a 64-byte push (B-009): a 135-byte DATA frame, of which 64 bytes are payload and
**60** are a trace context nobody asked for — 58 in the header and 2 in the frame's own length
field. That is the **largest single item in the frame**, against ZMTP's one to nine bytes of
framing per message and SP's eight. No decision note justified it;
[IMPLEMENTATION.md](../IMPLEMENTATION.md) §6 only defers the OpenTelemetry exporter, which is
a different question.

[GUIDE.md](../GUIDE.md) §0.3 is what made it urgent rather than merely untidy: at the C8B
arithmetic's leaf edge, one 64-byte message to eight billion people was ≈1.08 TB, and ≈480 GB
of that — **44 %** — was the unconditional trace context. After this decision the same message
is ≈608 GB, of which 512 GB is payload.

## 2. The three answers, with their costs

**(a) Keep it unconditional.** Every message is traceable and no caller can forget to
propagate. Costs 58 B on every frame of every pattern forever, and — the part that decides
it — **a minted root is not a safe default, it is a fabricated fact**. A hop that received a
`traceparent` and forgot to pass it on emits a *new trace* rather than nothing, so a collector
sees two unrelated traces instead of one broken chain. The failure mode of (a) is a lie that
looks like data; the failure mode of (b) is a gap that looks like a gap.

**(b) Propagate what the caller supplied, mint nothing.** Zero bytes when nobody traces, 58
when somebody does. A caller that propagates keeps every chain it had — the API for it already
exists (`TransferMeta::with_trace`, `Publisher::publish_with_trace`,
`Publisher::open_with_trace`) and `IncomingMeta::trace` is where an inbound context comes
from. A caller that forgets produces an **absent** key `3`, which a receiver reads as "this
hop did not trace" — the same `None` a malformed value already yields, so no new state exists
anywhere.

**(c) A runtime setting.** Makes the bytes a peer observes depend on a local configuration
nobody declared, which is exactly the class of thing the HELLO-negotiated dimensions of
[0006](0006-guarantee-sets.md) exist for. It is also a knob whose "on" position is (a) and
whose "off" position is (b), so it buys a third configuration key and settles nothing.

## 3. The decision

**3.1 (b).** `traceparent` is written **exactly when the caller supplied a context**, on every
pattern. Nothing in the library mints one on an application's behalf.

**3.2 Minting is a call, not a side effect.** `weida::new_trace()` is public, so a caller that
wants to *start* a trace says so in one line. That is the whole difference from (a): the root
exists because somebody asked for it.

**3.3 `tracestate` follows `traceparent`.** It is already only written where a context is
propagated, and W3C requires it to be forwarded unmodified; with no `traceparent` there is
nothing for it to be state of, and the encoder's rule is unchanged.

**3.4 The public shape changes with it**, because the old signature could not express the new
rule: `OutgoingTransfer::trace()` returns `Option<TraceContext>`, and `Publisher::publish`
and `Publisher::open` carry no context where their `*_with_trace` variants do.

**3.5 The specification says which**, so a second implementation knows: key `3` is optional and
this runtime writes it only on propagation ([PROTOCOL.md](../PROTOCOL.md) §6.2). A peer that
writes it on every frame is still conformant; a peer that reads an absent key as an error is
not.

## 4. What it measured

`cargo bench -p weida --bench patterns -- header`, release, loopback, same desktop as B-009,
two runs. The `wire_bytes` helper no longer rebuilds a header with a hardcoded `traceparent`:
it builds the header the send path builds, so the number cannot drift from the code again —
which it had, because the helper would have kept reporting 135 B after the code stopped
writing a context. The bench also gained a third variant, `push_64b_traced`, so a context's
cost is measured **in the same run** as its absence rather than against a figure from another
day.

| Measured, 64-byte payload | Frame | Message rate |
| --- | --- | --- |
| Minimal, no trace context | **76 B** | **192.6-195.9 Kmsg/s** |
| With a caller-supplied context | **136 B** (+60 B) | 168.9-171.2 Kmsg/s |
| With the two keys of [0001](0001-sequence-field.md)/[0008](0008-session-identity.md) | 157 B (+81 B) | 171.5-173.8 Kmsg/s |

Three readings, in order of how much they matter.

**A 64-byte message is now 12 bytes of framing.** 76 B minus 64 B of payload, and the 6-byte
endpoint path is half of what remains. Against ZMTP's one to nine bytes per message and SP's
eight, weida is now in the same class; it used to be 71 bytes worse than either.

**A context costs 60 bytes of frame**, not 58: 58 in the header (1 byte of key, 2 of the
`tstr` prefix, 55 of the value) and 2 more because the frame's own length field grows with it.
The unit test asserts the header's 58 and the bench reports the frame's 60, both in the tree,
so neither can quietly become the other.

**Not writing it is worth 13-14 % of the message rate** — 5.11-5.19 µs against 5.84-5.92 µs,
both variants in one run, which is the only honest way to compare them on a machine whose
run-to-run spread B-021 measured as wider than several of the effects it wanted to see. The
figure to carry forward is the pair, not a comparison with B-009's 179.4 Kmsg/s from a
different day.

## 5. What this does not decide

**Nothing about the OpenTelemetry exporter.** v0 still propagates and logs through `tracing`
and exports nothing; that is the observability phase, and this note narrows what is on the
wire rather than what is collected.

**Nothing about sampling.** A caller that supplies a context supplies its flags with it, and
weida neither reads nor rewrites them. The `sampled` bit of a minted root used to be `1`
unconditionally, which was a second fabricated fact and is now nobody's business but the
caller's.

**Nothing about the other per-message allocations.** The 55-byte `String` this change usually
stops building is one of five allocations a one-way send makes; the rest are B-250.
