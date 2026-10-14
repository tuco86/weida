# 0026 — The guide, and the question it answers

- **Status:** accepted
- **Date:** 2026-09-14
- **Accepted:** 2026-10-13
- **Related:** research/zeromq.md §1 sources, §9; decisions 0013 §4.7 clause 5, 0025; B-090..B-092

## 1. The question

Two statements from the owner, in one sentence each, and the second one is the larger.

> ZeroMQ's documentation, and above all the zguide, is the model for the documentation weida
> needs: not only *that*, but because the patterns describe **ways of constructing networked
> software**.

> nginx grew roughly out of the C10K question — ten thousand concurrent users on one host. With
> weida I want to settle the **C8B** question: how do you scale a software system to eight
> billion people? Utopian, of course; the road is the goal.

So there are two decisions here and they are one decision. What documentation does this project
owe a reader who wants to *build* something — and what is the question that documentation is
organised around, given that a reference organised around the API is what already exists and is
not what the owner asked for.

## 2. The evidence, condensed

### 2.1 What the zguide does, from this repository's own sheet

`docs/research/zeromq.md` read the guide chapter by chapter rather than by reputation, and what
it recorded is a method rather than a style.

**It defines reliability instead of claiming it.** Chapter 4, quoted in the sheet: "if we can
handle a certain set of well-defined and understood failures, then we are reliable with respect
to those failures. No more, no less", with a working definition — "keeping things working
properly when code freezes or crashes" — and an explicit failure list in descending probability:
application crashes, system out-of-memory, queue overflow, network failure, hardware, exotic
network failures, whole data centres. The chapter then says it covers the first five as
"99.9 % of real-world requirements outside large companies"
[research/zeromq.md](../research/zeromq.md) §9. That is the same move
[GUARANTEES.md](../GUARANTEES.md) makes with responsibility transfer, made in a *teaching*
document, and it is why the guide aged well: it never promised reliability as an adjective.

**Every pattern is a program with a name.** Lazy Pirate, Simple Pirate, Paranoid Pirate,
Majordomo, Titanic, Binary Star, Freelance, Clone, Espresso, Last Value Caching, Suicidal
Snail, Black Box — each with the problem it solves, the mechanism, the guarantee, the cost and
the failure modes it does *not* solve ([research/zeromq.md](../research/zeromq.md) §9). A named
recipe is citable in a design review, which is most of why they spread.

**It says what is hard.** "Heartbeating is difficult"; the guide's own estimate is roughly five
hours to get Paranoid Pirate right against ten minutes for the rest, and it names the
characteristic failure — false failure detection under load
([research/zeromq.md](../research/zeromq.md) §9). A guide that admits its own hardest chapter is
believable about the easy ones.

**The examples are the argument.** The recipes exist in many languages and each is runnable,
which is what makes a claim about a timeout checkable by a reader rather than a matter of trust.

### 2.2 What this repository already has

**The mechanism, already built and already used.** `crates/zmq/weida-zmq` carries nine zguide
recipes as `examples/` files, and `tests/zguide_pirates.rs`, `tests/zguide_majordomo.rs` and
their siblings include each example **as a module** (`#[path = "../examples/lazy_pirate.rs"]`)
and assert the guide's own claim against it: "the client gets an in-order reply or abandons,
never blocking indefinitely" is a test, not a sentence (B-090..B-092, 0013 §4.7 clause 5). Each
example also exposes its outcome as a **value** — `enum Outcome { Reply { attempts, … },
Abandoned { attempts } }` — precisely so the claim can be read by a test. That discipline is the
one thing this note does not have to invent.

**Eighteen runnable examples**, six of them weida's own: `push_pull`, `pub_sub`,
`transform_server`/`transform_client`, `large_stream`, `owned_runtime`. Each demonstrates one
property; none teaches a problem.

**No document teaches construction.** The set is a specification pyramid: ARCHITECTURE and
INVARIANTS state first principles, PROTOCOL and GUARANTEES specify, FAILURE_MODEL and PATTERNS
define observable behaviour, IMPLEMENTATION measures. The only "if you want X, do Y" passage in
the whole set is [PATTERNS.md](../PATTERNS.md) §7's chooser table. A reader who wants to build a
system is offered a reference and six demonstrations.

**And no claim in the set is about scale in the C8B sense.** Every scale statement is
subsystem-local and honest — 64 connections per peer, 750-850 KiB per connection, 1.04 ms per
handshake, a fan-out measured at **eight** subscribers. None of them composes into an answer
about a system, because nothing asked them to.

### 2.3 What the question is worth

The C10K precedent is not rhetorical. "Ten thousand connections on one host" did its work by
turning a structural question — thread per connection, or event loop — into an arithmetic one
that could be settled before anything was built. A number in the question is what makes a
design falsifiable early.

Run the same trick on eight billion and the arithmetic lands somewhere useful in one afternoon
(the numbers are in [GUIDE.md](../GUIDE.md) §0.1 with their sources): one producer needs
**12.4 hours** to hand one 64-byte message to everybody, a fan-out node holds on the order of
**10³-10⁴** subscribers at the measured per-connection and per-subscriber cost, and therefore
**two to four hops reach everybody**. The consequence is the part worth having: at three hops
the hard problem is not throughput and not depth, it is *what a hop may claim* — and an
acknowledgement per recipient converging on one root is **1.1 TB for one message**, which
settles end-to-end acknowledgement as an architecture rather than as a preference.

Every one of those conclusions is a decision this repository already took for other reasons
(0006, 0018, 0023, 0024, and the no-materialization invariant). The question does not change
the design; it explains it, and it makes the next gaps nameable — which is what §5 does with
B-247.

## 3. Options

| | what it is | named loss |
| --- | --- | --- |
| A — reference only, as today | PATTERNS as the pattern document, examples beside the crates | the reader who wants to build something is not served at all, and the patterns' *composition* — the thing the zguide actually teaches — is nowhere |
| B — a prose guide with snippets in it | the usual project guide: chapters, code fragments written for the page | the fragments rot. This repository has watched prose claims go false in every coherence sweep it has run (0025 §2); a snippet nobody compiles is the same defect with syntax highlighting |
| C — **a guide whose every program is an example in the tree, asserted by a test** | chapters in `docs/GUIDE.md`; programs in `crates/weida/examples/`; assertions in `crates/weida/tests/guide.rs`, including each example as a module | the guide can only teach what the tree can run today, so the chapters lag the ambition — and the discipline costs more per chapter than prose does |
| D — a separate book repository | a book, its own build, its own release | the examples stop being the tree's examples, which is the only property that keeps them true |

## 4. Decision

**C, with C8B as the spine and four rules.**

### 4.1 The guide is `docs/GUIDE.md`, and it is the document that teaches

One file that grows by chapters, published on the site beside the specifications
([0025](0025-the-website.md)). It does not restate a guarantee, a wire rule or a failure
outcome; it links to the document that owns it. Where it disagrees with a specification, the
specification is right and the guide is a bug.

### 4.2 Every program is a file in the tree, and a test drives that file

`examples/guide_*.rs` for the programs, `crates/weida/tests/guide.rs` for the assertions, the
example included as a module so what is asserted is the code a reader runs. Each program
returns its outcomes as **values** rather than printing them, for the same reason the zguide
recipes here do.

**The chapter's claims are numbered and counted by a test.** `every_claim_the_chapter_makes_has_a_program_and_a_test`
reads `docs/GUIDE.md`, counts the lines beginning `**Claim §1.`, and fails if the count or the
numbering moves. A claim added to the prose without a program and an assertion fails the gate.
That mechanism is the whole of option C's value; without it this is option B.

### 4.3 No number is an estimate, and a race is taught as a race

Measured figures name the item that measured them; arithmetic says it is arithmetic; a gap says
it is a gap (§0.4 of the guide names three). And where the protocol's behaviour is
non-deterministic by decision, the chapter demonstrates the non-determinism rather than picking
the convenient run: chapter 1 §1.3 is [0005](0005-refusal-race.md)'s refusal race, shown by
running the same program three times, with the remedy in the next paragraph. The test asserts
what survives the race — never `Indeterminate`, and definite in Req/Rep — rather than one side
of a coin flip.

### 4.4 The arc follows the question, not the API

Each chapter is a scaling step, and the failure cases live in the chapter that introduces the
mechanism rather than in an appendix:

| Chapter | The step | What it has to teach | Ready? |
| --- | --- | --- | --- |
| 0 | the question | the C8B arithmetic, what it decides, what it leaves open | **done** |
| 1 | one transfer | a transfer is a stream; the peer is a key; the outcome is a value; how far it got; what a receipt costs | **done** |
| 2 | many peers | selection, fan-out, the drop-versus-block decision, counted drops per cause, what a slow reader does to everybody else | needs B-247's measurement to state a width honestly |
| 3 | a hop that takes responsibility | the broker, `Accepted`, credit as the consumer's own bound, what a queue may promise before it has a store | after B-203 |
| 4 | depth | chains of native weida and broker hops, each claim scoped to the hop that earns it; no protocol translation | after chapter 3 and a runnable multi-hop example |
| 5 | a peer that comes back | reconnection, identity across a restart, what is *not* resumed | after B-018 |
| 6 | the cluster | consensus for metadata and never for payload, one group per replicated queue | after the store phase |
| 7 | eight billion | the arithmetic again with measured numbers in place of derived ones, and what is still missing | last, and only with numbers |

Chapter order is the reading arc, not permission to write ahead of evidence. A chapter is
written when its claims have a runnable program and assertions; chapter 4 therefore waits for a
native multi-hop example rather than borrowing a rejected cross-protocol product.

### 4.5 What the guide may not become

- **Not a marketing page.** No claim about performance that is not a measured number with its
  item, and no comparison that is not backed by [libraries/](../libraries/) or a named probe.
- **Not a second specification.** If a reader can answer a question from the guide and get a
  different answer from PROTOCOL, the guide is wrong.
- **Not a place where failure is optional.** The zguide's chapter 4 exists because chapters 1-3
  are not enough; this guide puts the failure case in the same section as the mechanism.

## 5. Consequences and follow-ups

- **B-255 is chapter 0 and 1**, done with this note: `docs/GUIDE.md`,
  `examples/guide_one_transfer.rs`, `crates/weida/tests/guide.rs` (7 tests), and the site
  manifest entry that publishes it.
- **B-247 stops being a nice-to-have.** The fan-out width *d* is the single number the depth
  argument rests on, and the largest fan-out anybody here has measured is eight subscribers.
  Chapter 2 cannot state a width honestly until that measurement exists, and the guide says so
  in §0.4 rather than rounding. The item is re-pointed at the question.
- **B-246 gains a second reason.** The unconditional `traceparent` is 44 % of the wire budget
  for a 64-byte message-to-everybody — ≈480 GB out of 1.08 TB. That is not why it should be
  decided, but it is why it should be decided *soon*. **Both of these landed the same day this
  note was written**: B-247 measured the width, and B-246 became
  [0028](0028-trace-propagation-is-the-callers.md) — a context is propagated and never minted,
  so a 64-byte push is now 76 B of which 12 are framing. The arithmetic of §3 is what found
  the second one, which is the strongest argument this note can make for having written it.
- **The remaining chapters are slices.** B-256's former cross-protocol chapter is retired;
  chapter 4 waits for the native multi-hop evidence above. The other slices keep §4.4's order.
- **Named loss: the guide will always lag the ambition.** Option C buys truth at the price of
  reach — chapter 6 cannot be written before the cluster exists, and no amount of wanting it
  changes that. The alternative was a chapter that reads well and is not true, and this
  repository has spent two sessions removing the smaller version of that mistake.
- **Named loss: C8B is a framing, not a claim.** Nothing here serves eight billion anybody, and
  the guide's §0 says so in its second sentence. The risk is that a number in a question gets
  quoted as a number in an answer; the mitigation is that every figure in §0.1 carries the item
  that measured it or the word "arithmetic".
