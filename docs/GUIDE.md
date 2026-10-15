# The weida guide

This is the document that teaches. The others specify: [PROTOCOL.md](PROTOCOL.md) is the wire,
[PATTERNS.md](PATTERNS.md) is the reference for what each pattern does, [GUARANTEES.md](GUARANTEES.md)
is what a claim about a message is allowed to mean, [FAILURE_MODEL.md](FAILURE_MODEL.md) is what
happens when something breaks. None of them tells you how to **build** something, and that is
what a reader needs first.

The model is ZeroMQ's guide. Not its API documentation — its *guide*, which teaches ways of
constructing networked software and happens to use one library to do it. Whatever else is true
of ZeroMQ, that document is the reason a generation of people could build message-passing
systems without a distributed-systems course, and it earned that by being concrete: every
pattern is a program you can run, every claim is small enough to check, and the failure cases
are in the same chapter as the success case rather than in an appendix.

**Two rules keep this document honest, and they are enforced rather than promised.**

1. **Every program here is a file in this repository, and a test drives that file.** The
   chapters do not carry snippets written for the page. Chapter 1's program is
   `crates/weida/examples/guide_one_transfer.rs`, chapter 2's is
   `crates/weida/examples/guide_many_peers.rs` with §2.6's and §2.7's in
   `crates/weida/examples/guide_late_is_lost.rs`; each is included as a module by
   `crates/weida/tests/guide.rs`, which asserts the claims its chapter makes. A claim added
   without a program and an assertion fails
   `every_claim_the_chapters_make_has_a_program_and_a_test`.
2. **No number here is an estimate.** Measured figures cite the subsection of
   [IMPLEMENTATION.md](IMPLEMENTATION.md) §4 that holds them; arithmetic derived from them says so in the
   sentence. Where nothing has been measured, the guide says *that* instead of guessing — and
   §0.4 names the gaps.

Run the chapters before reading them:

```text
cargo run -p weida --example guide_one_transfer
cargo run -p weida --example guide_many_peers          # --release for the timing figures
cargo run -p weida --example guide_late_is_lost
cargo test -p weida --test it guide::
```

---

## 0. The question: C8B

nginx came out of a question with a number in it. C10K asked how one machine serves ten
thousand concurrent connections, and the answer — event loops instead of a thread per
connection — reshaped server software, because the question named a *structure* problem in the
shape of a quantity.

weida's question is **C8B: how do you scale a software system to eight billion people?**

It is utopian on purpose, and the road is the goal. Nothing here will serve eight billion
anybody. What the number is for is the same thing C10K's was: it makes structural questions
answerable. A design either has an answer for what happens at eight billion recipients or it
does not, and the arithmetic tells you which, in an afternoon, without building anything.

### 0.1 The arithmetic

Take the simplest possible C8B workload: **one 64-byte message to every person, once.**

The measured pieces, all from this repository's own runs on one desktop machine
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4):

| Measured | Value | Source |
| --- | --- | --- |
| Message rate, 64-byte payload, one producer | **192.6-195.9 Kmsg/s** | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.5 |
| Bytes on the wire for that message | **76 B** frame for 64 B of payload | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.5 |
| Resident memory per live connection, both ends together | **750-850 KiB** | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.8 |
| Cold handshake | **1.04-1.10 ms**; a pooled dial 3.8 µs | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.8 |
| Publish cost at 256 subscribers | **22.94 µs** per message → **88.8 ns per subscriber** | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11 |
| Fan-out delivery rate at 256 subscribers | **276-279 Kcopies/s** | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11 |
| Transport state per subscriber, both ends | **396-436 KiB** | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11 |
| Latency to a subscriber at width 256, idle | **507-539 µs** median, **816-989 µs** p99 | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11 |
| Topic filter match | **14 ns**, shape-independent | [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.10 |

Everything below is **arithmetic on those numbers**, and says so where it is.

**One sender cannot do it.** 8·10⁹ ÷ 194,000 ≈ **11.4 hours** for one producer to hand out one
message each. So the shape has to be a tree, and the only question is how wide and how deep.

**How wide can one node be?** Not a memory question, which is the first thing the measurement
corrected. At **396-436 KiB** of transport state per subscriber, 64 GiB holds on the order of
**1.5·10⁵** of them, and the publisher's CPU is no obstacle either: 88.8 ns per subscriber is
**0.9 ms** of work to hand one message to 10⁴ subscribers. What binds is the **delivery rate**,
276-279 Kcopies/s measured at width 256:

| Width *d* | One message to all of them | Messages per second that width sustains |
| --- | --- | --- |
| 10³ | ~3.6 ms | ~276 |
| 10⁴ | ~36 ms | ~27 |
| 10⁵ | ~360 ms | ~2.8 |

So the width is a **choice against a message rate** rather than a ceiling: a node serves 10³
subscribers at a few hundred messages a second, or 10⁴ at tens. Call the width *d* ∈ [10³, 10⁴].

**Then the depth is tiny.** log(8·10⁹) ÷ log(10³) ≈ 3.3, and ÷ log(10⁴) ≈ 2.5. So

> **two to four hops reach everybody.**

At *d* = 10⁴ that is **8·10⁵ leaf nodes**, 80 above them and one root; the leaf hop spends
36 ms of its delivery capacity on the message and the whole path is **~0.1 s** of fan-out work.

That single line is why this project's design decisions look the way they do. The scaling
problem at C8B is not throughput — 8·10⁹ ÷ 194,000 ÷ 10⁶ nodes is microseconds of work per
node — and it is not depth. It is **what a hop is allowed to claim**, because there are only
three or four of them between a publisher and a person, and every one of them is a place where
responsibility either transfers or is faked.

### 0.2 What the arithmetic decides

Four design decisions in this repository are what they are because of the numbers above, and
each one would be defensible without C8B and is *forced* with it.

**No end-to-end acknowledgement.** At *d* = 10³ and four hops, an acknowledgement per recipient
means 8·10⁹ acknowledgements converging back on one publisher: 8·10⁹ × 76 B ≈ **608 GB of
acknowledgement traffic for one message**, all of it funnelling into the root. The tree that
made the fan-out affordable makes the acknowledgement impossible. So a completion is **hop-local
by construction** ([GUARANTEES.md](GUARANTEES.md) §1): each hop says what it took
responsibility for, to the hop that handed it over, and nobody claims anything about the far
end.

**A completion is a cursor, not a verdict.** At depth, "did it arrive" has no answer that is
both cheap and true. "How far did this hop get" does: an absolute byte offset, which is
idempotent, coalesces for free, and stays meaningful when a transfer is interrupted
([decisions/0023](decisions/0023-completion-is-a-cursor.md)). Chapter 1 §1.4 is that mechanism
in twenty lines.

**Nothing materializes a payload.** A hop that must hold a message before forwarding it turns
every intermediary into a memory ceiling: three hops × 8·10⁹ recipients is a lot of copies of
anything. weida's transfers are streams end to end, and the measured consequence is the one
that matters — a 4 GiB payload crosses with **14.4 MiB of peak resident memory**
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.2). Chapter 1 §1.2 is that property at 64 MiB.

**Fan-out drops and counts; it never blocks.** One slow subscriber out of 10⁴ must not stall the
other 9,999, so a publisher's answer to overload is a counted drop rather than backpressure
([PATTERNS.md](PATTERNS.md) §4) — and the count is per topic and per cause, because "something
was dropped" is not an operational answer. What a stalled subscriber costs the publisher is
measured on both sides of the crossover the defaults put at a **32 KiB** message: below it the
queue refuses first (0.34-1.19 MiB held per stalled subscriber at 1 KiB), above it the byte
budget does (9.3-10.7 MiB at 64 KiB), and the counter says which
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11). Chapter 2 §2.3 is that experiment as a
program.

### 0.3 What the arithmetic says about the bytes

One more line of the same arithmetic, and it is the one that shaped the frame.

A 64-byte message to everybody is 8·10⁹ frames at the leaf edge. A frame that carried a W3C
`traceparent` whether anything traced or not would be **135 B**, of which 64 are payload and
**60** the trace context — ≈**480 GB per message-to-everyone**, 44 % of the total, spent on a
trace context nobody asked for. Against ZMTP's one to nine bytes of framing per message and
SP's eight, that is not a rounding error; it would be the third-largest line in the budget.

So weida does not write one. A context is **propagated and never minted**
([decisions/0028](decisions/0028-trace-propagation-is-the-callers.md)), so:

| | Frame | 8·10⁹ of them | Message rate |
| --- | --- | --- | --- |
| A minted context on every frame (refused) | 135 B | ≈1.08 TB | — |
| Untraced | **76 B** | **≈608 GB** | **192.6-195.9 Kmsg/s** |
| With a caller's context | 136 B | ≈1.09 TB | 168.9-171.2 Kmsg/s |

**Twelve bytes of framing around 64 bytes of payload**, half of it the endpoint path, and 84 %
of the wire is the message. The rate figure is the pair measured in one run, not a
comparison against the 179.4 Kmsg/s of another day ([IMPLEMENTATION.md](IMPLEMENTATION.md)
§4.4), and both numbers are the bench's ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.5).

Two things are worth taking from this, because they are the method rather than the result.
**The arithmetic finds it.** Writing down 8·10⁹ × 135 B and asking what the bytes are makes a
fixed 60-byte field indefensible in a way that a 9 % benchmark difference never does. And
**the decision is about honesty, not bytes**: a minted root is not a safe default but a
fabricated fact, because a hop that received a context and forgot to pass it on would emit a
*new trace* rather than nothing — two unrelated traces in a collector instead of one visible
gap. The bytes make it urgent; that makes it right.

### 0.4 What is not answered

Honesty is part of the method, so the gaps are named where the arithmetic needs them:

- **The width is measured to 256 subscribers, and *d* is 10³ to 10⁴.** The measurement
  ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11) is one order of magnitude below the number
  the depth argument uses: 256 subscribers on one desktop, both ends
  in one process, over loopback. The per-subscriber cost is flat from 16 to 256 — 396-436 KiB
  and a falling CPU cost per subscriber — which is the shape that extrapolates, and an
  extrapolation is what it remains. What a real 10⁴-subscriber node does with **one** network
  interface, across hosts, is not known here.
- **Nothing is measured across machines at all.** Every number in the table above is loopback
  with both ends in one process, which is the right way to isolate this library's own cost and
  the wrong way to learn what a deployment does: it has no propagation delay, no NIC, no packet
  loss, and a congestion controller that never sees a real bottleneck.
- **Federation across organisations is unbuilt.** Two to four hops inside one operator's
  network is the easy reading of §0.1. Eight billion people are not one operator's network, and
  what a hop between two *administrations* may claim is a question [0006](decisions/0006-guarantee-sets.md)
  answers in the protocol (a guarantee set per hop, intersected, never downgraded silently) and
  nothing yet answers in a deployment.
- **The cluster and the store are specified and not finished.** [STORE.md](STORE.md) and
  [0020](decisions/0020-cluster-and-discovery.md)-[0022](decisions/0022-consensus-topology.md)
  are the plan; `weida-broker` has queues, a publisher confirm, credit and settlement with
  requeue, and no durability. A chapter about a hop that survives a restart cannot be written yet, and this
  guide will not pretend otherwise.

---

## 1. One transfer, and what you may say about it

The program: `examples/guide_one_transfer.rs`. The test: `crates/weida/tests/guide.rs`. Run
both before reading on — the output of the first is the shape of this chapter.

If you come from ZeroMQ, one sentence orients everything below: **a weida transfer is a stream,
not a message.** A socket in ZeroMQ takes a message you already have; an endpoint in weida
hands you a stream you write into. Everything in this chapter follows from that one
difference, including the parts that look like they are about errors.

### 1.1 Hello

**Claim §1.1: an address and a trust decision are the whole setup.** No broker, no registry, no
certificate authority, no configuration file.

```rust,ignore
let identity = Identity::generate()?;                      // a key pair, in memory
let fingerprint = identity.fingerprint()?;
let binding = listener.bind_quic("127.0.0.1:0".parse()?, identity).await?;
let url = format!("weida://{fingerprint}@127.0.0.1:{}/hello", binding.local_addr().port());

let replier = listener.replier("/hello")?;                 // the serving side
let requester = client.requester(Trust::by_address());     // the dialling side
requester.connect(&url).await?;
let reply = requester.request(b"world").await?.collect(1024).await?;
```

Two things in that URL are worth more than they look.

**The peer is its public key.** `weida://sha256:…@host:port/path` names a key, and
`Trust::by_address` means *accept exactly the peer this URL names and nobody else*. There is no
name to resolve into an authority, no chain to validate, nothing to expire. An address is a
complete trust statement, which is why this chapter needs no setup section
([ARCHITECTURE.md](ARCHITECTURE.md) §2).

**The path is opaque and it is where dispatch happens.** `/hello` is not a topic, not a queue
name and not a hierarchy weida parses. It is the key a replier registered under, and the only
thing the acceptor matches on ([decisions/0007](decisions/0007-topic-namespace.md)).

And one thing that is *not* in the program: `accept` hands the replier a request whose payload
has not been read yet, and the reply half exists before the request has finished arriving. A
replier can answer a 4 GiB request without holding it — which is §1.2, and is the reason the
API looks like this rather than like `fn handle(request: Vec<u8>) -> Vec<u8>`.

### 1.2 The same four calls carry a gigabyte

**Claim §1.2: the payload never has to exist anywhere, and a cap is a refusal rather than a
bigger buffer.**

The program sends 64 MiB with the same `open`/`write_all`/`finish` that sent five bytes in
§1.1, and the reader folds it through one 64 KiB buffer:

```rust,ignore
let mut buffer = vec![0u8; CHUNK];          // the only buffer in the path
loop {
    let read = transfer.read(&mut buffer).await?;
    if read == 0 { break; }
    checksum = fold(checksum, &buffer[..read]);
}
```

There is no `collect` in that loop, and that is the point. `collect(max_bytes)` exists and is
the right call for a small payload, but it is **opt-in and capped**: you state what you are
willing to hold, and a payload that exceeds it is refused rather than admitted. The program
proves both halves — the streamed transfer arrives whole, and the same payload offered to
`collect(1 MiB)` is refused.

**The refusal reaches the sender.** This is the part that surprises people: the reader's
`LimitExceeded` becomes a `STOP_SENDING` on the wire, so the sender's `write_all` fails
mid-payload with `Error::Rejected` rather than succeeding into a void. A cap is a conversation,
not a silent truncation.

For the gigabyte itself, this repository has the measurement rather than a claim:
`examples/large_stream.rs` moves 4 GiB between two processes at 908.7 MiB/s with **14.4 MiB of
peak resident memory**, and `tests/large.rs` asserts a 512 MiB ceiling on a 1 GiB echo
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.2). §0.2 is why that number is load-bearing rather
than impressive.

### 1.3 The outcome is a value

**Claim §1.3: a send's outcome is a value with three cases — and which case you can be sure of
is decided by the pattern, not by the error type.**

The three cases, and they are not interchangeable:

| Outcome | What it means | What an application may do |
| --- | --- | --- |
| `Ok(())` from `delivered()` | the peer's **transport** holds every byte and the FIN | nothing more; it is not a claim the peer's application read them ([GUARANTEES.md](GUARANTEES.md) §3) |
| an error with `is_definite_failure()` | it definitely did not happen | retry without worrying about duplication |
| `Error::Indeterminate` | it may or may not have happened | decide — and the decision needs idempotency or a human ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5) |

`Indeterminate` is the one people delete when they write their own framework, and it is the one
that matters at depth: a connection that dies after the FIN went out and before the
acknowledgement came back leaves exactly that state, and calling it a failure is a lie that
costs duplicate work.

**Now the part the program is really for.** Run it three times and watch the second line
change:

```text
push, served path:   Delivered
push, unserved path: Delivered      <- and, on another run, Refused("peer has no such endpoint")
request, same path:  Refused("peer has no such endpoint")   <- every time
```

A one-way transfer to a path **nobody serves** may be reported as delivered. That is not a bug
and it is not a race in the implementation: the payload fit in the peer's stream window, so
QUIC acknowledged the FIN before the dispatcher's refusal travelled back, and the receipt
answered with what it knew. [decisions/0005](decisions/0005-refusal-race.md) is the decision
that keeps it that way rather than inventing a handshake to hide it, and it states the rule:

> a refusal is guaranteed only beyond the peer's stream window, or in Req/Rep.

The remedy is in the same three lines of output. An exchange has a reply half, and a reply half
cannot be acknowledged into existence — so `request` to an unserved path is `UnknownEndpoint`,
definitely, every time. **If you must know whether the far side accepted it, ask a question
instead of making a statement.** That is the first genuinely architectural choice in this guide,
and it costs a round trip, which is §1.5.

### 1.4 How far did it get

**Claim §1.4: "did it arrive" and "how far did the far end get" are two different questions, and
weida answers both, separately.**

```rust,ignore
let meta = TransferMeta::default().with_report([stage()]);   // order a report
let mut transfer = pusher.open(meta).await?;
let mut cursors = transfer.cursors().expect("a report was ordered");
transfer.write_all(&payload).await?;
let delivered = transfer.finish()?.delivered().await.is_ok();  // fact one: the transport
let reported = cursors.changed().await.and_then(|s| s.offset(stage()));  // fact two: the application
```

On the receiving side, the mechanism is three lines:

```rust,ignore
let mut reporter = transfer.reporter().expect("the sender ordered a report");
let body = transfer.collect(64 * 1024).await?;
reporter.report(stage(), body.len() as u64).await?;          // an absolute offset
```

Four properties, each of which is a decision rather than an implementation detail:

- **The topology does not change.** That push is still one unidirectional stream. The report
  rides a stream of its own, so ordering cursors costs no pattern its shape
  ([decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.4a).
- **The offset is absolute**, so a duplicated or reordered record is a no-op and coalescing is
  free. This is the same property that makes credit idempotent
  ([decisions/0003](decisions/0003-credit-unit.md)).
- **An order is not a guarantee.** A receiver that cannot reach a level simply does not report
  it, and the transfer does not fail for it. A level a peer *must* reach is the negotiated
  `acknowledgement` dimension of the handshake instead
  ([decisions/0006](decisions/0006-guarantee-sets.md) §4.4).
- **`stage()` is an application level** — the value 16, the first one an application may name.
  weida carries it and never interprets it, exactly as it carries a topic without parsing it
  ([PROTOCOL.md](PROTOCOL.md) §6.7).

The reason this exists is §0.2: at two to four hops, the only honest statement a hop can make is
about itself, and the only useful shape for it is a number.

### 1.5 What the receipt costs

**Claim §1.5: the receipt is a round trip, and it is not free.**

The program sends 1 KiB eight times each way and prints both:

```text
1 KiB, 8 rounds each: 165.864µs without the receipt, 19.978744ms with it
```

That is two orders of magnitude, and none of it is weida's overhead: it is QUIC's delayed
acknowledgement on an idle connection, which [GUARANTEES.md](GUARANTEES.md) §3 measured at
~26 ms against ~7.9 µs for the same push without the receipt. Under load the delay disappears
into the traffic — an acknowledgement rides the next packet — which is why the honest way to
present this number is to run it yourself rather than to quote it.

What to take from it:

- **Fire and forget is the default for a reason.** `send` returns when the FIN is queued.
- **A receipt per message is a design smell at scale.** If you need one per message you are
  asking for a request/reply pattern; use one.
- **A receipt on the *last* message of a batch is usually what you wanted**, and costs one
  round trip per batch. `examples/push_pull.rs` does exactly that.

### 1.6 What this chapter does not tell you

Chapter 1 is one transfer between two peers. Nothing in it is wrong at scale, and nothing in it
is *enough* at scale:

- one peer per side, so no selection policy, no fan-out, no drops — chapter 2;
- both ends in one process on loopback, so no reconnection and no partition;
- no hop in the middle, so responsibility never transfers;
- nothing durable, so no restart survives anything.

The rest are filed rather than written: a chapter is added only with its programs and its
assertions, because a guide whose examples do not run is worse than no guide
([decisions/0025](decisions/0025-the-website.md) §2 is the same argument about a landing
page). The arc is in
[decisions/0026](decisions/0026-the-guide-and-the-c8b-question.md) §4.4, and the backlog holds
the remaining chapters.

---

## 2. Many peers, and the drop

The program: `examples/guide_many_peers.rs`. The test: the same `crates/weida/tests/guide.rs`.

One peer on each side is the only configuration in which a send has no policy. From two peers
on, every pattern answers the same three questions — **which peer, what if it cannot keep up,
and who else is affected** — and the answers differ per pattern on purpose. This chapter is
those answers, and it is where the C8B arithmetic of §0.1 gets its numbers.

### 2.1 Selection is the pattern's, not the call's

**Claim §2.1: the pattern chooses the peer set; the call does not change.**

The program connects one sender to three peers three times over, and sends the same way each
time:

```text
6 pushes over 3 pullers:      [2, 2, 2]
2 publishes to 3 subscribers: [2, 2, 2]
3 BUS members, one send each: [2, 2, 2]   <- never itself
```

Three policies, one call shape:

| Pattern | Who gets it | Mechanism |
| --- | --- | --- |
| Push | **one** live peer per send, rotating | one `fetch_add` on a cursor, `crates/weida/src/stream.rs` |
| Pub/Sub | **every** subscriber whose filter matches the topic | one stream per subscriber per message |
| BUS | **every** member except the sender | one writer per joined member |

Nothing at the call site selects a policy, and that is the design: `pusher.send(&body)` and
`publisher.publish(topic, body)` are the same statement about one message, and the peer set is
a property of the endpoint you built ([PATTERNS.md](PATTERNS.md) §3, §4, §7). ZeroMQ readers
will recognise this exactly — it is `zmq_socket(3)`'s socket-type table, with the type chosen
by a factory call instead of a constant.

The round-robin is worth one sentence more, because it is the one people assume wrongly: it
rotates over **live peers at the moment of the send**, so a peer that died is skipped rather
than waited for, and a peer that arrives joins the rotation. There is no hashing, no affinity,
and no ordering across peers — a Push fan-in is a work queue, not a partitioned log.

### 2.2 A subscriber that stops reading

**Claim §2.2: a subscriber that stops reading costs its neighbour nothing — and costs itself
nothing either, until the publisher outruns its transport.**

Two subscribers at the default limits. One drains; the other never calls `recv` once. The
program runs the publisher twice over the same pair:

```text
paced:    512 published, all 512 enqueued for both, the reader got 512, 0 dropped
flat out: 1537 more absorbed by a subscriber that has never read, then 1 dropped, cause: the queue
```

**The first line is the isolation claim, and it is the whole argument for a stream per
subscriber.** The reading subscriber received every message while its neighbour read nothing at
all. In a design with one queue per socket, the silent subscriber's backlog is the reader's
problem; here it is not, because each subscriber has its own stream, its own queue and its own
byte budget ([PATTERNS.md](PATTERNS.md) §1.3). What that isolation costs is measured
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11) and quoted in §2.4.

**The second line is the part that surprises people, including the author of this program.**
A subscriber that has read nothing for two megabytes loses nothing at all while the publisher
is paced — and the reason is that a publisher's byte budget is released when the bytes reach
the **wire**, not when somebody reads them. QUIC absorbs the difference. The loss starts only
when the publisher outruns the silent subscriber's *transport*, which took another ~1500
messages in the run above.

> **Overload is a rate, not a backlog.**

That sentence is why the drop counters are per cause rather than per subscriber, and why the
program runs the publisher in two phases: a paced publisher reports zero drops for a
subscriber that has read nothing, and a tiny budget starves the reader as well as the silent
one. Either alone would measure the wrong thing.

### 2.3 Which ceiling binds, and how you know

**Claim §2.3: a stalled subscriber is bounded twice, and the counter says which bound it hit.**

A subscriber costs a publisher two bounded resources: `endpoint_queue` **messages** and
`Limits::subscriber_buffer_bytes` of **payload**. Which one refuses first is not a matter of
taste, and it is not the payload size either:

```text
budget   64 KiB:   321 messages ( 321 KiB) accepted, then the byte budget
budget 8192 KiB:  1665 messages (1665 KiB) accepted, then the queue
```

One variable, two outcomes, and the mechanism is QUIC. A copy's budget permit is released when
its bytes reach the wire, and a connection absorbs a window's worth before it stops — 0.6 to
1.5 MiB in the measured runs. So a budget **below** that band is spent while QUIC is still
accepting, and the budget refuses; a budget **above** it never fills, because the wire stops
the writer first and then the queue backs up. `benches/fanout.rs` sees the same thing at the
defaults ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11): the 8 MiB default means the
**queue** is what refuses, at 1153-1537 messages of 1 KiB.

**The operational point is the counter, not the rule.** `dropped_on(topic)` separates
`subscriber_budget` from `subscriber_queue` from `no_parked_connection`, so a starving
subscriber is a lookup rather than a packet capture — and the two answers mean different
remedies: a budget refusal wants a bigger budget or a smaller payload, a queue refusal wants a
faster consumer or a deeper queue. "Messages are being dropped" is not an operational answer,
which is why this API does not offer it.

What a stalled subscriber costs in **memory** is the second half, measured ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11): 0.34-1.19
MiB resident per stalled subscriber at a 1 KiB payload, 9.3-10.7 MiB at 64 KiB. And one
correction that a reader sizing a node will want: `subscribers × subscriber_buffer_bytes`
**overstates** the cost, because one `publish` is one `Bytes` that every copy shares. The
budget is an accounting bound; the resident cost is one payload per distinct message.

### 2.4 What width costs the publisher

**Claim §2.4: the publisher's cost per message grows with the width, and its cost per
subscriber falls.**

The program prints its own numbers — in a debug build unless you pass `--release`, and it says
so. The figures worth quoting are the benchmark's, from
[IMPLEMENTATION.md](IMPLEMENTATION.md) §4.11, because they are release builds over three runs:

| Width | One publish | Per subscriber | Idle latency, median | Transport state per subscriber |
| --- | --- | --- | --- | --- |
| 1 | **287 ns** | — | 34.7-37.9 µs | 1.63-1.76 MiB (includes the one-off endpoint state) |
| 16 | **3.04 µs** | 184 ns | 76.2-77.8 µs | 402-418 KiB |
| 256 | **22.94 µs** | **88.8 ns** | 507-539 µs | 396-436 KiB |

Three things to take from that table.

**The marginal cost per subscriber falls as the width grows** — 184 ns at 16 against 88.8 ns at
256 — because the fixed cost of a publish is amortized and the registry walk gets its cache.
So the number to plan with is the wide one, and a node's publisher CPU at 10⁴ subscribers is
about **0.9 ms per message**.

**Neither memory nor CPU is what limits the width.** At ~420 KiB per subscriber, 64 GiB of
transport state is on the order of 1.5·10⁵ subscribers. What binds is the delivery rate:
**276-279 Kcopies/s**, so one message to 10⁴ subscribers spends ~36 ms of a node's capacity and
one to 10³ spends ~3.6 ms. §0.1's width table is that division, and it is the reason the C8B
arithmetic has a width of 10³-10⁴ rather than 10⁵.

**A wide publisher is not a slow publisher, it is a *late* one.** The median latency at width
256 is 507 µs against 35 µs at width 1 — fourteen times worse, on an idle machine, with
nothing else running. A subscriber at the far end of a wide fan-out waits for the publisher to
get to it, and the only structural answer is another hop, which is §0.1's depth.

### 2.5 A peer that never answers

**Claim §2.5: a survey is a fan-out with a deadline, so a silent peer is a number rather than a
hang.**

Push rotates past a dead peer and Pub/Sub drops a copy, but neither has to answer the hardest
version of the question: what if a peer takes the message and then says nothing? SURVEY is the
pattern that must, and its answer is the deadline the caller passes:

```text
asked 3, answered 2, silent 1 within the deadline
```

The silent respondent in that program is not unreachable, not refusing and not slow to connect
— it completed the handshake, accepted the question and held it. The surveyor returns what
arrived, `late()` counts what arrived after the deadline, and `respondents()` says how many
were asked ([PATTERNS.md](PATTERNS.md) §5). No configuration, no retry policy, no circuit
breaker: **the deadline is the caller's and the result is a count**.

That shape is the pattern's whole contribution, and it is worth naming because the alternative
is so common: a fan-out that waits for everybody has its availability set by its worst member,
which at C8B scale is a certainty rather than a risk.

### 2.6 Late is lost

The program: `examples/guide_late_is_lost.rs`. The test: the same `crates/weida/tests/guide.rs`.

Everything so far keeps what it can and drops what it must. A voice frame or a video frame
turns that around: **a unit that arrives after its moment is worth nothing**, so the question
is not how to keep it but how to lose it on purpose, visibly, and without stalling anybody.
weida has three answers ([0034](decisions/0034-late-is-lost.md)): a **datagram flow** at L0,
**expiry and priority** on the streams that exist, and the pattern ZeroMQ named for lossy
fan-out, **RADIO/DISH** ([PATTERNS.md](PATTERNS.md) §6.4).

**Claim §2.6: a dish that falls behind loses the old segment, never the new one, and never at
another dish's expense.**

A radio sends a camera's frames — ten segments of 256 KiB at 25 per second — to two dishes.
One reads everything; the other never reads, behind a 64 KiB stream window:

```text
fast dish received segments [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]; the stalled dish lost 9 to supersession
```

**Opening segment *n+1* resets every copy of segment *n* on that topic that is still being
written or held back by the dish**, and nothing else; a finished copy gets the time its path
needs to be acknowledged before it is reset ([0037](decisions/0037-layered-segments.md) §4.5).
The stalled dish's copy never finishes, so each new frame
resets the old one: it holds at most the newest segment, which is the only one worth showing.
The fast dish acknowledged each frame before the next existed, so supersession never reaches
it. Nothing was held to send instead — supersession discards bytes that are already in
flight, which is why it costs the radio no memory — and a dish that joins late starts at the
next segment, because a segment is the only point a decoder can start from.

**Claim §2.7: an SFU and a relay are loops over opaque payload.**

The forwarding path of a selective forwarding unit is this, whole:

```rust
while let Ok(Incoming::Flow(mic)) = mics.accept().await {
    let radio = room.clone();
    tokio::spawn(async move {
        while let Some(frame) = mic.recv().await {
            let _ = radio.datagram("room.voice", frame);
        }
    });
}
```

A microphone is a flow into the SFU, and every frame goes back out as a one-packet segment on
the radio. A relay is the same program with a dish on one side: it reads each upstream stream
segment chunk by chunk and writes the chunks into a downstream segment as they arrive. The
program speaks 100 frames of noise and pushes five 64 KiB video segments through the relay:

```text
100 of 100 frames reached both listeners; 5 of 5 segments crossed the relay; payload untouched: true
```

**weida forwards on nobody's behalf**, and the SFU is still an ordinary program: authorization
is the acceptor's decision on each flow, and the payload — SFrame ciphertext, in griasdi's
case — is bytes to it. What weida contributes is that every loss on the way is a count with a
cause: `Radio::dropped_on` splits by budget, supersession, expiry, a datagram too large for a
dish's connection, and a dish that carries no datagrams, and a datagram segment is never turned
into a stream segment, which would deliver it late. Where both kinds share a bottleneck with a
bulk upload, [IMPLEMENTATION.md](IMPLEMENTATION.md) §4.17 measures what the path and the
congestion controller cost the voice.

### 2.7 Quality without re-encoding

The program: `examples/guide_late_is_lost.rs`, its `layered`. The test: the same
`crates/weida/tests/guide.rs`.

**Claim §2.8: a viewer's quality is a cap it states, not a second encoding.**

A sharer sends five segments of three layers each — 8 KiB of noise per layer — upstream with
`Peer::segment`. A relay forwards every upstream layer to its radio as it arrives. One viewer
joins uncapped, the other with `JoinTerms::default().with_max_layer(0)`:

```text
capped viewer received 5 segments of layer 0 and nothing else: true; uncapped viewer received 15 layer segments; payload untouched: true
```

**A segment carries ordered layers, and weida cuts them from the top**
([0037](decisions/0037-layered-segments.md) §4.1, §4.3). Layer *k* may depend on the layers
below it and never above, and each `(copy, layer)` is its own stream with its own queue, so a
copy without room for a chunk of layer *k* gives up its layers above *k* first, and only then
*k* and above. Every cut — by budget, queue, cap or supersession — removes a suffix of the
ladder, so whatever a viewer holds is a prefix a decoder can use, and which bytes form a layer
stays the application's.

**The cap is per dish.** `max_layer` travels in the join (SUBSCRIBE key `3`); the radio never
opens a layer above it for that dish, and that is not a drop. One encoding serves every
viewer, so the encoder follows the uplink rather than the slowest viewer, and a viewer whose
link degrades joins again with a lower cap instead of asking anybody to re-encode.

**Priority is layer-major.** A layer stream opens at `(15 - layer) * 65536 + priority`, so
every topic's base layer on a connection goes before any topic's enhancement layer, and the
voice datagrams of §2.6 outrank all of it — on one connection only, which is why a session's
voice and video share one path (§4.4).

**The relay finishes a layer only after every lower one.** Each upstream layer arrives as its
own `Incoming::Stream` with the topic, the segment number and the layer; a newer number opens a
radio segment that follows upstream, so the previous segment keeps taking the chunks still
arriving (§4.11); an upstream EOF on layer *k* becomes `finish_layer(k)` once every layer below
*k* is finished, and an upstream layer that was reset drops the segment, which resets every
layer not finished downstream. A relay never finishes a layer whose base it is about to lose.

### 2.8 What this chapter does not tell you

- **Every number here is one process on loopback.** No propagation delay, no NIC, no loss, and
  a congestion controller that never sees a real bottleneck (§0.4).
- **The widest fan-out measured anywhere in this repository is 256.** The C8B arithmetic uses
  10³-10⁴, which is an extrapolation from a flat per-subscriber cost between 16 and 256 (§0.4).
- **A drop is invisible to the subscriber here.** It need not be: the sequence key of
  [PROTOCOL.md](PROTOCOL.md) §6.2 and the `PerProducer` detect level turn a fan-out drop into a
  reported `Gap`, and [PATTERNS.md](PATTERNS.md) §4 has the mechanism. It is implemented and
  this chapter does not teach it yet.
- **Nothing here takes responsibility for a message.** Every peer in this chapter forgets
  immediately, which is the next question rather than an omission — a hop that takes
  responsibility is chapter 3, and it needs a queue that survives its consumer.

