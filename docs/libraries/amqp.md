# AMQP 1.0 (OASIS Standard, 29 October 2012) — feature parity

Status: complete against the six clauses of
[0013](../decisions/0013-competitor-libraries.md) §4.7, read for AMQP. This document is the
sixth clause itself — the row-by-row table — and §11 gives the other five their verdicts.

The library is two crates: `weida-amqp-codec`, the sans-I/O codec whose `[dependencies]` is
empty, and `weida-amqp`, the client on top of it. Neither depends on `weida`; a caller who
wants an AMQP 1.0 client and nothing else uses `weida-amqp` alone
([0013](../decisions/0013-competitor-libraries.md) §4.3, §4.4).

**This is a client, and the standard defines no server.** AMQP 1.0 "defines no operation to
create, configure, enumerate or delete a node", so a broker cannot be built from the core
standard alone: everything a broker does with a queue is outside it
([0014](../decisions/0014-parallel-libraries.md) §2). Every row below is therefore read as
*the client half*. Where a concept exists at both ends — a terminus's expiry policy, a
delivery state, an unsettled map — the client half is what is present, and the row says so
once rather than in every line.

## 1. What a row means

Three verdicts and no fourth:

- **present** — implemented, with the module or test that carries it named. A present row may
  still say where the behaviour differs, and then the difference is the row.
- **refused with a reason** — the configuration exists in the specification and fails **here,
  at configuration time**, with a message naming why (0013 §4.4 item 4). Nothing is silently
  ignored.
- **absent with a reason** — not implemented and not configurable, with what is missing named.

"Partial" is not a verdict. Where something is present in part, the row says which part is
missing (0013 §4.7 clause 6).

**Versions every measured claim was measured against.** `fe2o3-amqp` **0.18.4**, the pure-Rust
peer, through its `acceptor` module over loopback TCP, by
`crates/amqp/weida-amqp/tests/interop_fe2o3.rs` — eight tests, nothing `#[ignore]`d. **RabbitMQ
4.x is not run**: no `rabbitmq-server` is available to the tests, and neither is `docker` or
`podman`, so `tests/interop_rabbitmq.rs` is four `#[ignore]`d tests carrying the install
command, and every RabbitMQ column below says *not run* rather than a verdict. Claims about
what the specification *says* cite [`../research/amqp10.md`](../research/amqp10.md), which
cites the five OASIS parts; claims about what a broker does cite that sheet's numbered sources
and are marked as documentation rather than measurement.

## 2. The nine performatives

Part 2 §2.7 defines exactly nine and there is no tenth. All nine are encoded and decoded by
`weida-amqp-codec`'s `performative.rs`, with the field layouts of §2.7.1-§2.7.9 asserted
against the specification's own figures in `tests/golden_performatives.rs`; the client's use of
each is in `weida-amqp`.

| Performative | Verdict | Where, and what the client does with it |
| --- | --- | --- |
| `open` (`0x10`) | present | `connection.rs`. Every field of §2.7.1 as a field of `ConnectionOptions`. Each negotiated value is read from the end that owns it: the **peer's** `max-frame-size` bounds what this client writes, floored at the 512 octets both peers MUST accept; the **peer's** `channel-max` bounds which channels it may use; and the idle timeout is two independent clocks rather than one agreed number |
| `begin` (`0x11`) | present | `session.rs`. Lowest free outgoing channel, the answering `begin` tying the two independent numberings through `remote-channel`, `handle-max` advertised at 255 |
| `attach` (`0x12`) | present | `link.rs`. Both roles, both settle-mode fields, `source` and `target` as described types, `initial-delivery-count` mandatory for a sender; the answering `attach` is read as **data and not an acknowledgement** — what the peer actually created |
| `flow` (`0x13`) | present | `link.rs`, `window.rs`, `credit.rs`. Both halves: the six session variables that every `flow` carries and the four link variables it may carry; `drain` honoured, `echo` answered exactly once and never with an `echo` |
| `transfer` (`0x14`) | present | `link.rs`. Multi-frame deliveries with `more`, `aborted` on a delivery this end cannot finish, `settled` per delivery under `mixed`, `delivery-tag` a per-link counter, `message-format` 0 |
| `disposition` (`0x15`) | present | `settlement.rs`, `connection.rs`. Ranges of delivery-ids in one frame, all four outcomes, `settled` distinguishing a provisional answer from a final one; offered to every link of the session in the addressed direction, because the frame names no handle |
| `detach` (`0x16`) | present | `link.rs`. `closed` both ways, an error carried, the peer's `detach` **answered** — a link endpoint is destroyed only when both ends have detached |
| `end` (`0x17`) | present | `session.rs`. With and without an error, and the `DISCARDING` state discarding input until the partner's `end` rather than acting on it |
| `close` (`0x18`) | present | `connection.rs`. The last frame ever written, and the queue of already-handed-over frames is flushed before it — see §10 |

## 3. The message: nine sections in one fixed order

Part 3 §3.2 fixes the order and `message.rs` enforces it as a monotonic stage counter: a
section out of order is `SectionOutOfOrder`, a repeat is `DuplicateSection`, and mixing body
kinds is `MixedBodySections`.

| Section | Descriptor | Verdict |
| --- | --- | --- |
| `header` | `0x70` | present — the five fields, `durable` as the demand it is, `priority` defaulting to 4, both `delivery-count` meanings kept apart |
| `delivery-annotations` | `0x71` | present — one hop only, sender to receiver |
| `message-annotations` | `0x72` | present — travels with the message indefinitely |
| `properties` | `0x73` | present — all thirteen fields, `message-id` and `correlation-id` in all four of their permitted types |
| `application-properties` | `0x74` | present — the section is required to be a map and a repeat is `DuplicateSection`. What is **not** enforced: Part 3 restricts the *values* to simple types (no map, list or array), and this decoder carries whatever the map held rather than refusing a nested one. The restriction is documented on the field; a peer that sends a nested value gets it back unchanged rather than an error |
| `data` | `0x75` | present — one or more, and the bare-message octets reported alongside the decode so a signature can be computed over what arrived |
| `amqp-sequence` | `0x76` | present — one or more |
| `amqp-value` | `0x77` | present — exactly one, any single value |
| `footer` | `0x78` | present — the tail annotations |
| *no body section at all* | — | present as `Body::Empty`, and **marked non-conformant** in the type's own documentation: Part 3 §3.2 enumerates the body as "one or more", so this is what a peer sent and not what this client will produce |

## 4. The two credit schemes

Both are implemented, both are checked before every frame, and they are two types rather than
one because the specification ties them together nowhere (sheet §5.3).

| Scheme | Unit | Verdict |
| --- | --- | --- |
| Link credit (Part 2 §2.6.7) | **messages** | present — `credit.rs`. All four variables; the absolute delivery-limit arithmetic in RFC 1982 serial numbers, so a duplicated or reordered `flow` cannot inflate credit; `drain` consuming all remaining credit and reporting; zero is a stall and not an error |
| Session window (Part 2 §2.5.6) | **`transfer` frames** | present — `window.rs`. All six variables including **both** branches of the `remote-incoming-window` recomputation, the one that decides whether a session can send its first frame at all; a peer exceeding our window is answered with `end` carrying `amqp:session:window-violation` |
| Both together | — | present, and asserted as such: `the_session_window_stalls_a_sender_that_still_has_link_credit` gives a sender four units of link credit and a one-frame window and watches it send one frame, and a 1600-octet message through a 512-octet frame costs **one** unit of credit and four window units |
| Per-connection credit | — | absent, and absent from the protocol: "there is no per-connection credit" (sheet §5). Nothing is missing here; the row exists so that a reader looking for it stops looking |

## 5. Settle modes and delivery states

What each mode *proves to an application* is the subject of `settlement.rs`'s own table and of
`tests/disposition.rs`, where every test asserts a conclusion — the message will not come back,
the message is available again, the peer knows that this end knows — rather than that a frame
arrived.

| Mode | Verdict |
| --- | --- |
| `snd-settle-mode=unsettled` | present — the sender keeps the delivery until the receiver's `disposition` retires it |
| `snd-settle-mode=settled` | present — settled on send, recorded nowhere, and `Link::settled` refuses with a message saying so, because an application that could wait for that answer would wait forever |
| `snd-settle-mode=mixed` | present — the per-delivery choice, and this client's choice for a `mixed` link is *unsettled*, because a settled transfer is one the receiver cannot report anything about |
| `rcv-settle-mode=first` | present — the receiver settles as it reports: one frame, done |
| `rcv-settle-mode=second` | present — the three-frame dance, driven by the **driver** rather than the application: when the receiver publishes an outcome without settling, the sender owes a settling `disposition` and nothing is left to decide. Measured against `fe2o3-amqp` 0.18.4, which **honours** it with `SupportedReceiverSettleModes::Both` — so the sheet's §13 "implementations refuse it" is not universal. RabbitMQ: not run; its documentation lists exactly-once unsupported (sheet source [16]), which is a claim and not a measurement |

| Delivery state | Descriptor | Verdict |
| --- | --- | --- |
| `accepted` | `0x24` | present — and `Outcome::may_be_redelivered` is false, `increments_delivery_count` false |
| `rejected` | `0x25` | present — with the peer's error carried through; not redelivered, and it **does** increment `delivery-count` |
| `released` | `0x26` | present — available again, `delivery-count` **not** incremented, so indistinguishable from a message never delivered |
| `modified` | `0x27` | present — `delivery-failed` and `undeliverable-here` as `Option<bool>` because the specification gives them no defaults, and the annotations map carried |
| `received` | `0x23` | present in the codec and **deliberately not an `Outcome`**: it is the one non-terminal state, it describes how far a partial body got, and an application that treated it as an answer would be acting on a progress report. A `disposition` carrying it is recorded as progress |

The **unsettled map** is present at each end (`settlement.rs`), with settlement idempotent and
one-way: a `disposition` naming a delivery this end has forgotten is nothing, which is what
makes the frame safe to repeat and why a lost one needs no recovery protocol.

## 6. SASL and TLS

| Mechanism | Verdict |
| --- | --- |
| `ANONYMOUS` | present — `sasl.rs`; what a server not requiring authentication SHOULD advertise |
| `PLAIN` | present — the whole exchange is one NUL-separated initial response |
| `EXTERNAL` | present — the identity comes from the layer below, in practice a TLS client certificate; RabbitMQ reaches x.509 authentication through it |
| No SASL layer at all | present — `AMQP %d0 1.0.0` and `open` immediately, which is legal and is the default |
| `CRAM-MD5`, `DIGEST-MD5`, `SCRAM-SHA-*`, `GSSAPI`, `NTLM` | **absent** — what is missing: the mechanisms and the cryptographic stacks under them. Part 5 delegates the registry to IANA and requires none of these; a mechanism this client cannot speak is **not offered**, and a server that offers only these is refused at the `sasl-mechanisms` step with the list it sent, rather than downgraded |
| Mechanism negotiation | **refused with a reason** — the mechanism is *chosen* by configuration and not negotiated. A server that does not offer the chosen one fails with a message naming what it did offer, because silently picking another is how a client ends up authenticating in a way its operator did not ask for |

| Transport | Verdict |
| --- | --- |
| TCP, port 5672 | present — `transport.rs`. The only mapping Part 2 §2.8.19 defines |
| TLS as a layer, protocol-id `%d2` | present — `AMQP %d2 1.0.0` exchanged, TLS negotiated, then `AMQP %d0 1.0.0` *inside* it: two header exchanges, the second encrypted |
| TLS out of band, `amqps` on 5671 | present — a pure TLS listener with no protocol header before the handshake, which is the documented alternative to `%d2` and what `amqps` means |
| The trust anchors | **deliberately not in the crate** — `Connection::connect_tls` takes the caller's `rustls::ClientConfig`, because which certificates an application trusts is the application's decision and a library that chooses cannot be audited |
| UDP, SCTP | **absent, and absent from the protocol**: Part 2 §2.8.19 reserves the port numbers and defines **no mapping**. Nothing is missing that a specification supplies |
| WebSocket (`AMQP-WSB-01`) | **absent** — what is missing: the binding itself, its subprotocol negotiation and the framing over it. It is a separate OASIS committee specification and not part of the core five parts this library is measured against |

## 7. What is absent, with the evidence

Three families, each named absent here rather than claimed in part.

### 7.1 Part 4: transactions

**Absent, by construction.** Part 4's transactional target is a `coordinator` — a *different
described type* from `target` — and this client's `LinkOptions::target` is typed as a
[`Target`], so there is no value of this API that attaches to a coordinator. What is missing:
the `coordinator` target, the `declare` and `discharge` messages, `txn-id` on `transfer`,
`disposition` and `flow`, the `transactional-state` delivery state, and the `amqp:txn:local`,
`amqp:txn:multi-txns-per-ssn`, `amqp:txn:multi-ssns-per-txn` and `amqp:txn:distributed-txns`
capabilities.

The evidence that this is the honest verdict and not a gap in the measurement: **Part 6,
Distributed Transactions, was never published** (sheet §6.5), so `amqp:txn:distributed-txns`
names a capability no specification defines. `fe2o3-amqp` gates its own transaction support
behind a non-default `transaction` feature, and the interop probe confirms neither end claims
an `amqp:txn:` capability. The codec does carry the five delivery-state descriptors of Part 3
§3.4 and not Part 4's sixth, which is the same boundary drawn in the other crate.

### 7.2 Link resumption

**Absent.** What is missing: `attach.unsettled` populated on the way out,
`incomplete-unsettled`, the `resume` flag on `transfer`, and the three-way rule that decides
which tags may be resumed (sheet §12: tags only the sender holds MAY be resumed, tags only the
receiver holds MUST be ignored, tags both hold MUST be resumed).

What *is* present is the half that makes the absence safe rather than silent: this client's
second `attach` of a name carries **no `unsettled` map at all**, which is exactly what
distinguishes a re-attach from a resuming attach (Part 2 §2.6.13), so a peer is never told that
state is being recovered when it is not. The delivery-**tag** is kept in the unsettled map
beside the delivery-id precisely because the tag is what a resumption would key on, so the data
a later implementation needs is already there. Measured: `fe2o3-amqp` 0.18.4 accepts a
re-attach of a name it has let go as a new link. A second attach of a name it still holds is
where the peer's version shows: 0.17.0 **refused** it rather than implementing Part 2 §2.6.1's
steal; 0.18.4 takes this client's steal — the `detach` with `amqp:link:stolen` that precedes the
second `attach` frees the name — answers the new attach and may reuse the freed handle. The
client reports whichever answer the peer gives.

### 7.3 The three coexisting filter families

**The `filter` field is present; no filter family is interpreted.** `Source::filter` carries a
filter-set — a symbol-keyed map of described types — through the `attach` in both directions
and reports what the peer echoed, so an application may set a filter its broker understands
and read the answer. What is missing is any *understanding* of the three families, which is the
part that cannot be written once:

1. **Apache Qpid's `apache.org:selector-filter:string` and `apache.org:no-local-filter:list`**
   (descriptors `0x0000468C:0x00000004` and `:0x00000003`), described as *proposed* AMQP 1.0
   filters and not a standard registry (sheet §13 source [33]).
2. **AMQP Filter Expressions CSD 01**, an entirely different family with codes in the
   `0x100`-`0x178` range and its own capability names (`APACHE.ORG:SELECTOR`,
   `APACHE.ORG:NO_LOCAL`) — a committee specification draft, not one of the five parts.
3. **Vendor filters**: `rabbitmq:stream-filter`, `rabbitmq:stream-match-unfiltered`,
   `com.microsoft:session-filter` and the rest, which are neither of the above.

A client MUST negotiate rather than assume (sheet §13), and negotiation over three
incompatible families with overlapping intent is a per-broker decision an application makes,
not a library default. So the field is carried and nothing is invented.

## 8. Bounds

Every remote-influenced allocation has a named bound. Where the protocol supplies one this
library uses **the protocol's**, and where it does not, the bound is ours and says so
([INVARIANTS.md](../INVARIANTS.md)).

| Bound | Default | Whose, and what it bounds |
| --- | --- | --- |
| `channel-max` | 255 advertised | the protocol's. The session table is bounded by the **peer's** value, because that is the number saying which channels the peer will accept |
| `handle-max` | 255 advertised | the protocol's. The link table per session, again the peer's value; and the **peer's `attach.handle` is refused above the value this end advertised** (`amqp:connection:framing-error`), so the table the peer's handles are mapped in is as small as that number says |
| `incoming-window` | 400 | the protocol's, and one of only three bounds in AMQP that are safe by construction: `begin.incoming-window` is mandatory and has no default. 400 is the number RabbitMQ 4.x uses |
| `link-credit` | 0 initially | the protocol's. A freshly attached link carries no permission to send anything at all |
| `max-message-size` | **16 MiB** | the protocol's field, **our default**. The specification's default is no limit, which is agreeing to allocate whatever a peer sends one bounded frame at a time; nothing bounds the *number* of frames a delivery may take, so the reassembly buffer is capped at this size, never grown past it, and released when the delivery aborts. Settable back to zero, never silently |
| `max-frame-size` | **128 KiB** | the protocol's field, **our default**. The specification's default is `4294967295` — a four-gigabyte frame on a peer's word |
| `idle-time-out` | **60 s** | the protocol's field, **our default**. The specification's default is unset, which is a connection that holds its resources until the OS notices |
| `ConnectionOptions::write_timeout` | **10 s** | ours. A peer that keeps the connection open and stops reading fills the kernel's send buffer, after which a write waits for as long as the peer likes. Every frame is queued and written inside the one `select!` that also reads, answers and times out, so a stalled peer cannot stop any of those; the stall clock restarts whenever the socket takes anything, and no progress for this long fails the connection with `Error::WriteTimeout` |
| `transport::OUTBOX_HIGH_WATER` | 256 KiB | ours. Past this many queued octets the driver stops taking frames from local senders, and it stops reading the peer once the *answers* to the peer's own frames alone are past it. The application's frames do not count against reading, so a peer streaming bulk in both directions goes on reading and the idle clock keeps seeing frames; what a peer that never reads can make the driver hold is this much of the application's frames plus this much of answers |
| `connection::MIN_KEEPALIVE` | 50 ms | ours. The peer chooses `idle-time-out` and half of it is the keep-alive period; a floor keeps a value of 1 ms from becoming a write every half millisecond |
| `options::MAX_TIMEOUT` | 1 year | ours. The longest any deadline (`handshake_timeout`, `write_timeout`, `close_budget`, `idle_time_out`) may be set to: a deadline is added to an instant, which has an end, so `Duration::MAX` for "never" is refused where it is configured instead of panicking in the driver. Zero is refused for the idle threshold and the close budget as it is for the other two; `idle_time_out: None` turns the expiry off |
| connect budget | `handshake_timeout` | ours. The name lookup and every address tried share one budget; each address but the last gets half of what is left and an attempt that runs out is aborted, so a resolver that does not answer or addresses that swallow packets cannot hold a caller for the budget again and again |
| `connection::OUTGOING_QUEUE` | 64 | ours. Frames a session or link has handed the driver and it has not yet queued: the driver **receives** from this queue, so a sender waits here and the driver never does |
| `link::MAX_QUEUED_REPORTS` | 1024 | ours. What a link tells a handle that never reads: the driver **never waits** for an application, so there is no queue capacity. A delivery is queued only against credit the application granted itself; outcomes and credit updates are state the link also keeps, so consecutive credit updates are merged and, past this many unread, the oldest report is let go. A sender keeps a delivery the peer has settled, with its outcome, until the application takes it (`Link::settled`, or reading the `LinkEvent::Outcome`), so an event being let go never loses an answer — and a producer that takes none stalls at `DEFAULT_MAX_UNSETTLED` rather than outrunning what it has not read |
| `settlement::DEFAULT_MAX_UNSETTLED` | 2048 | ours, on **both** sides. A sender's map has no protocol bound (a receiver may keep granting credit and settle nothing), so at the bound `send` waits. A receiver's is bounded by credit **it granted itself, and that is enforced**: a delivery past every limit it ever announced is refused with `amqp:link:transfer-limit-exceeded` (what was in flight when it lowered its credit is still accepted), an unsettled delivery that finds the map full is refused with `amqp:resource-limit-exceeded`, and `grant_credit` refuses a grant the map could not hold |
| `sasl::MAX_ROUNDS` | 16 | ours. Part 5 §5.3's challenge/response loop is "zero or more" rounds |
| codec `Limits` (`weida-amqp-codec`): `max_elements`, `max_depth`, `max_zero_width_elements` | 4096, 16, 16 per decode (`Limits::BODY` for message bodies: 65 536, 32, 256) | ours. A declared count is refused from the header before anything is reserved. An array of zero-width elements (`null`, `true`, `false`, `uint0`, `ulong0`, `list0`) costs the wire no octets per element, so its count is charged to one allowance per decode, shared by every array of a performative, a SASL body or a message |
| `ConnectionOptions::max_resolved_addresses` | 8 | ours. What a name may expand to |

## 9. Interop

`crates/amqp/weida-amqp/tests/interop_fe2o3.rs` — **8 tests, nothing ignored**, against
`fe2o3-amqp` 0.18.4's `acceptor` over loopback. Both roles: our sender read by its receiver,
our receiver reading its sender, and an 8 KiB message through a 1 KiB frame reassembled whole
at the peer. Four probes rather than assumptions, each recorded above: `rcv-settle-mode=second`
(§5), terminus expiry policy (echoed, not substituted), duplicate link name (the steal taken by
0.18.4, refused by 0.17.0 — §7.2), and transactions (§7.1). `cargo tree -p weida-amqp --no-dev-dependencies`
reaches neither `weida` nor `weida-protocol`, and the peer brings no second TLS stack.

`crates/amqp/weida-amqp/tests/interop_rabbitmq.rs` — **4 tests, all `#[ignore]`d**, because no
`rabbitmq-server` is available. The harness starts and stops the broker on a port of its own
and waits for the socket rather than assuming readiness; every test says what it *would*
establish. **Nothing in it is observed**, and the sheet's RabbitMQ claims stay
documentation until it runs.

## 10. Three rules a reader is likely to misread

Each is a rule of the specification that a reader of this library would be likely to misread.

1. **`close` does not overtake a completed transfer.** "`close` MUST be the last frame ever
   written" (Part 2 §2.4.3) makes it *last*; it is not a licence to drop what was already
   queued. The driver flushes the queue before writing `close`: preferring the command channel
   over the outbound queue would drop a frame a sender had already handed over as "raced the
   close".
2. **A `flow` for a just-detached handle does not end the session.** That frame is a race with
   the `detach`, not a fault, and `fe2o3-amqp` really sends one. Frames for the peer's handles
   of **cleanly** detached links are discarded; a handle detached **with an error** gets
   `amqp:session:errant-link`, which Part 2 §2.6.5 names and which is distinct from
   `amqp:session:unattached-handle`.
   A handle this end detaches **with an error** is the exception's exception until the peer
   answers: the peer's `flow` or `transfer` for it may already be on the wire when the `detach`
   leaves, so frames on it are discarded until the peer's answering `detach` arrives, and only
   input after that answer is errant. Found against `fe2o3-amqp` 0.18.4, whose `flow` crossed
   the client's steal and ended the session.
3. **A client that re-attaches a name does not steal its own just-detached link.** Sending
   `amqp:link:stolen` for a link the application had closed itself would poison the handle for
   the rest of the session, so links already detaching are not candidates for the steal.

A fourth rule completes them: the peer's `detach` is **answered**. A link endpoint is destroyed
only when both ends have detached, so a peer whose application held the link would wait
forever.

## 11. The definition of done

0013 §4.7's six clauses, read for AMQP, each with its verdict and where this document proves
it.

| Clause | Verdict |
| --- | --- |
| 1. Every performative of Part 2 §2.7 implemented or absent with a reason, with the specification's own field layouts asserted rather than described | **met** — §2: all nine present, with `tests/golden_performatives.rs` asserting the figures |
| 2. The transports the specification defines, with the rest named absent | **met** — §6: TCP and both routes to TLS present; UDP and SCTP are reserved with no mapping *in the specification*; the WebSocket binding is absent with what is missing named |
| 3. The security layers: SASL with the mechanisms a client can speak, and TLS | **met** — §6: `ANONYMOUS`, `PLAIN`, `EXTERNAL` and no-SASL present; the rest absent with the reason, and never offered |
| 4. Interop in both roles against a foreign implementation | **met** — §9: 8 tests against `fe2o3-amqp` 0.18.4, both roles, four probed facts. RabbitMQ 4.x is `#[ignore]`d and stated as not run |
| 5. The message model: every section, both credit schemes, the settle modes and the delivery states | **met** — §3, §4, §5 |
| 6. A feature-parity table, row by row, where no row says "partial" without saying what is missing | **met** — this document |

What a caller who compares this library with the OASIS core does **not** get, in one list: Part
4 transactions and the `coordinator` target; link resumption; any *interpretation* of the three
filter families, though the field is carried; SASL beyond `ANONYMOUS`, `PLAIN` and `EXTERNAL`;
the WebSocket binding; and a broker — no node model, because the standard defines none.
Nothing else.

## 12. Sources

- [`../research/amqp10.md`](../research/amqp10.md) — §1 (connection, session, link), §2 (the
  type system and the frames), §3 (transfer, disposition, settlement), §5 (both credit
  schemes), §6 (the outcomes and the distribution-node state machine), §12 (link resumption),
  §13 (the four things recorded as refused by peers, and the filter families), and the numbered
  sources [3], [16], [26], [27], [33], [57] behind the broker claims.
- [0013](../decisions/0013-competitor-libraries.md) §4.1 (crate layout), §4.2 (the dependency
  direction), §4.3 (the codec stays sans-I/O), §4.4 (the shape of the library, the option rule
  and the deliberate default changes), §4.7 (the six clauses).
- [0014](../decisions/0014-parallel-libraries.md) §2 (client and not broker), §3 (the interop
  peers).
- [0003](../decisions/0003-credit-unit.md) — the vocabulary §4 borrows, so that *credit* means
  the same thing in both places.
- Code the verdicts were read from, under `crates/amqp/`: `weida-amqp-codec/src/`
  (`performative.rs`, `message.rs`, `state.rs`, `frame.rs`, `sasl.rs`, `types.rs`) and
  `weida-amqp/src/` (`connection.rs`, `session.rs`, `link.rs`, `credit.rs`, `window.rs`,
  `settlement.rs`, `delivery.rs`, `terminus.rs`, `options.rs`, `sasl.rs`, `transport.rs`), plus
  `weida-amqp/tests/` (`handshake.rs`, `session.rs`, `link.rs`, `transfer.rs`,
  `disposition.rs`, `interop_fe2o3.rs`, `interop_rabbitmq.rs`) and
  `weida-amqp-codec/tests/golden_performatives.rs`.
