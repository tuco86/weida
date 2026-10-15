//! The L2 broker: queues at endpoint paths, a confirm in, settlement out.
//!
//! A broker is an ordinary weida process that holds messages for a while. It
//! registers a queue on each endpoint path its configuration names; a producer
//! sends a message to that path **as an exchange** and the reply half carries
//! the broker's certificate; a producer that will not wait sends a one-way
//! transfer instead and gets the transport receipt and nothing more. Both
//! shapes are the patterns this workspace already has: L2 is a layer, not a
//! fork (`docs/ARCHITECTURE.md` §1).
//!
//! # What this crate certifies, and what it refuses to
//!
//! [`Achieved::Accepted`] and nothing above it. Everything a queue holds is in
//! memory and vanishes with the process, so `Stored(Written)` — defined as
//! surviving "the broker process dying: crash, restart or orderly stop" —
//! would be a prohibited claim, not an optimistic one: "`Stored` MUST NOT be
//! reported for an in-memory buffer" (`docs/GUARANTEES.md` §1). The refusal is
//! in the type rather than in a comment: [`Achieved`] has one variant, so no
//! code path here can issue a durable certificate. `Stored` appears with a
//! durable store, and `Replicated` with replication.
//!
//! A certificate is also about **one hop**. `Accepted` says this broker took
//! responsibility for the message; it says nothing about a consumer, and the
//! broker never relays a consumer's outcome back to the producer
//! (`docs/GUARANTEES.md` §2,
//! [0018](https://github.com/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
//! §4.3). A producer that needs to know a consumer processed its message uses
//! an application reply.
//!
//! # Delivery, and what a consumer must do to get any
//!
//! A consumer is an ordinary [`weida::Subscriber`]: it subscribes to the
//! queue's path with a filter and then **grants credit** with
//! [`weida::Subscriber::grant`], an absolute delivery limit per subscription.
//! Until it does, it receives nothing — initial credit is zero, the only
//! default that cannot surprise a consumer with a flood. Each message goes to
//! exactly one consumer with credit, as a one-way transfer; a queue with no
//! credit anywhere keeps its messages, which is the temporal decoupling a
//! queue exists for.
//!
//! Two properties are worth stating because they are what a credit scheme is
//! bought for. The limit is **absolute, cumulative and monotone at the
//! broker** — the highest limit seen wins — so a duplicated or reordered
//! grant changes nothing, which is what makes credit safe on a transport that
//! does not order the streams control frames ride. And **the only pause is
//! the `0` a fresh subscription starts at**: restating the count already
//! delivered is not above the standing limit, so it changes nothing at all
//! unless that count had already reached it. v0 offers no way to lower a
//! standing limit, so a consumer that wants to stay in control grants in
//! increments it is willing to receive rather than one large number it means
//! to withdraw later.
//!
//! # Settlement
//!
//! A delivery orders `Processed` on its own DATA header, so the consumer
//! reports on a cursor stream and the delivery stays a one-way transfer. The
//! message moves from the queue to an `unsettled` table and keeps its charge
//! against `queue_bytes`: it may come back, so it is memory the queue still
//! owes. When the consumer reports `Processed` up to the end of the body (a
//! byte cursor: a smaller offset is progress, not completion) the message is
//! dropped and its charge released. When the report ends without it (the
//! reporter finishes, the stream is reset, or the connection goes away) the
//! message is requeued
//! and any consumer with credit may take it. A subscription with
//! [`BrokerConfig::max_unsettled`] deliveries outstanding is given no more,
//! and neither is a connection with [`BrokerConfig::max_unsettled_per_consumer`]
//! outstanding over all its subscriptions, so a consumer that stops reporting
//! stops receiving without starving the others.
//!
//! # Peers that stall
//!
//! A queue is served by one loop, and **that loop never waits on a peer**. A
//! producer's message is read in a task of its own and a delivery is written
//! by one, each under [`BrokerConfig::io_timeout`]; the loop only starts them
//! and reacts to how they end. A producer that stops half way through a
//! message, and a consumer that grants credit and stops reading, therefore cost
//! themselves a task and a deadline and cost every other producer and consumer
//! nothing. Four bounds keep that from becoming the problem it solves:
//!
//! - a message is charged to `queue_bytes` **as it arrives**, in steps, so
//!   concurrent producers cannot each be promised the whole remainder
//!   ([`QueueStats::reserved`] shows what is promised and unused);
//! - [`BrokerConfig::max_inflight_admissions`] admissions run at once; past
//!   that the queue stops reading new streams until one finishes, so producers
//!   wait on QUIC's flow control and nothing queues in a buffer of the
//!   broker's own;
//! - one connection holds at most
//!   [`BrokerConfig::max_inflight_admissions_per_connection`] of those, and so
//!   do the connections of one proved peer together, so the one that stalls is
//!   the one that is refused: a stream past its share is rejected at once and
//!   everybody else's is read;
//! - a peer that moves bytes slower than [`MIN_PEER_RATE`] is treated as
//!   stalled after about one window, however steadily it trickles.
//!
//! What a stall costs: a producer loses the message it was sending; a consumer
//! has its connection closed with `LIMIT_EXCEEDED` (`docs/PROTOCOL.md` §7), so
//! every subscription it held on it ends, the messages it was holding go back
//! to the head of the queue, and its client's redial subscribes again. A
//! delivery write that fails at the transport closes the connection the same
//! way.
//!
//! A delivery that never starts is not a stall. When a message's header is more
//! than a consumer's connection said it will read, the message goes back to the
//! head of the queue for other consumers, that connection is not offered it
//! again, and the connection stays open. A message that [`MAX_REFUSALS`]
//! connections refused is dropped and counted ([`QueueStats::dropped`]).
//!
//! Messages enter a queue in the order they **finish arriving**. QUIC orders
//! bytes within a stream and nothing between streams, and a queue that held a
//! finished message back for an unfinished one in front of it would give a
//! stalled producer the power this design takes from it. A producer that waits
//! for each confirm before sending the next has its own order preserved.
//!
//! # Not built yet
//!
//! - The redelivery count on the wire. The queue counts attempts per message;
//!   DATA key `12` (`delivery_attempt`) is reserved for carrying the count,
//!   and nothing writes it.
//! - Relaying a consumer's report to the producer
//!   ([0029](https://github.com/tuco86/weida/blob/main/docs/decisions/0029-a-report-is-relayed-a-certificate-is-not.md)
//!   §4.3).
//! - Retiring a queue
//!   ([0018](https://github.com/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
//!   §4.7).
//!
//! # Example
//!
//! ```no_run
//! use weida::{Runtime, RuntimeConfig, ServerTls, Identity};
//! use weida_broker::{Broker, BrokerConfig};
//!
//! # async fn run() -> Result<(), weida::Error> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//! let listener = runtime.listener();
//! let identity = Identity::generate()?;
//! listener
//!     .bind_quic("127.0.0.1:0".parse().unwrap(), ServerTls::new(identity))
//!     .await?;
//! let broker = Broker::new(&listener, BrokerConfig::with_queues(["/jobs"]))?;
//! let serving = broker.clone();
//! tokio::spawn(async move { serving.serve().await });
//! # Ok(())
//! # }
//! ```

mod admission;
mod broker;
mod consumers;
mod delivery;
mod peer;
mod queue;

pub use broker::{Achieved, Broker, BrokerConfig};
pub use peer::MIN_PEER_RATE;
pub use queue::{MAX_REFUSALS, PER_MESSAGE_OVERHEAD, QueueStats};
