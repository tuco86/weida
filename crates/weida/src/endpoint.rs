//! L1: typed endpoints, one `Endpoint<P>` per messaging pattern.
//!
//! The pattern is a type parameter rather than a runtime mode flag, so a
//! requester cannot be asked to accept and a replier cannot be asked to dial
//! (master doc §3). The trait is sealed: patterns are part of the protocol.
//!
//! Everything here is a thin wrapper over the stream core ([`crate::stream`]):
//! Req/Rep is one bidirectional stream per exchange, Push/Pull is a
//! unidirectional stream per message with round-robin peer selection, Pub/Sub
//! is the same stream with fan-out selection and publisher-side filtering. No
//! pattern adds a guarantee QUIC does not already provide; the vocabulary of
//! stored/replicated/processed belongs to a broker layer that does not exist
//! yet (`docs/GUARANTEES.md`).

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Mutex, Semaphore, mpsc};
use weida_core::{Error, TraceContext};
use weida_protocol::{CreditHeader, FrameKind, SubscriptionHeader};

use crate::config::ClientTls;
use crate::conn::{ConnHandle, Ctl, check_header_len, write_control};
use crate::listener::{PairOwner, Route};
use crate::pubsub::{FanOut, SubRegistry};
use crate::reconnect::PeerEvents;
use crate::runtime::RuntimeInner;
use crate::stream::{Attach, Peer};
use crate::transfer::{
    IncomingRequest, IncomingTransfer, OutgoingTransfer, ReplyStream, TransferMeta,
};

mod sealed {
    pub trait Sealed {}
}

/// A messaging pattern.
pub trait Pattern: sealed::Sealed {
    /// Pattern-specific endpoint state.
    type State;
}

/// The requesting side of Req/Rep.
pub enum Req {}
/// The replying side of Req/Rep.
pub enum Rep {}
/// The sending side of Push/Pull.
pub enum Push {}
/// The receiving side of Push/Pull.
pub enum Pull {}
/// The publishing side of Pub/Sub.
pub enum Pub {}
/// The subscribing side of Pub/Sub.
pub enum Sub {}
/// Either side of PAIR: one connection, one peer, both directions.
pub enum Pair {}
/// The asking side of SURVEY.
pub enum Survey {}
/// The answering side of SURVEY.
pub enum Respond {}
/// A member of a BUS.
pub enum Bus {}
/// The sending side of RADIO/DISH: lossy fan-out of segments
/// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
pub enum Cast {}
/// The receiving side of RADIO/DISH.
pub enum Tune {}
/// The bound side of SWARM: answers each fetch's orders about a blob from the
/// application's storage ([decisions/0040](../../../docs/decisions/0040-swarm-pattern.md),
/// [decisions/0041](../../../docs/decisions/0041-swarm-sessions.md)).
#[cfg(feature = "swarm")]
pub enum Seed {}
/// The dialling side of SWARM: gathers one blob from every connected seeder.
#[cfg(feature = "swarm")]
pub enum Gather {}

impl sealed::Sealed for Req {}
impl Pattern for Req {
    type State = ReqState;
}

impl sealed::Sealed for Rep {}
impl Pattern for Rep {
    type State = RepState;
}

impl sealed::Sealed for Push {}
impl Pattern for Push {
    type State = PushState;
}

impl sealed::Sealed for Pull {}
impl Pattern for Pull {
    type State = PullState;
}

impl sealed::Sealed for Pub {}
impl Pattern for Pub {
    type State = PubState;
}

impl sealed::Sealed for Sub {}
impl Pattern for Sub {
    type State = SubState;
}

impl sealed::Sealed for Pair {}
impl Pattern for Pair {
    type State = PairState;
}

impl sealed::Sealed for Survey {}
impl Pattern for Survey {
    type State = SurveyState;
}

impl sealed::Sealed for Respond {}
impl Pattern for Respond {
    type State = RespondState;
}

impl sealed::Sealed for Bus {}
impl Pattern for Bus {
    type State = BusState;
}

impl sealed::Sealed for Cast {}
impl Pattern for Cast {
    type State = crate::radio::RadioState;
}

impl sealed::Sealed for Tune {}
impl Pattern for Tune {
    type State = crate::radio::DishState;
}

#[cfg(feature = "swarm")]
impl sealed::Sealed for Seed {}
#[cfg(feature = "swarm")]
impl Pattern for Seed {
    type State = crate::swarm::SeedState;
}

#[cfg(feature = "swarm")]
impl sealed::Sealed for Gather {}
#[cfg(feature = "swarm")]
impl Pattern for Gather {
    type State = crate::swarm::GatherState;
}

/// A typed messaging endpoint.
pub struct Endpoint<P: Pattern> {
    state: P::State,
}

impl<P: Pattern> Endpoint<P> {
    pub(crate) fn from_state(state: P::State) -> Endpoint<P> {
        Endpoint { state }
    }

    /// The pattern's state, for patterns implemented outside this module.
    pub(crate) fn state(&self) -> &P::State {
        &self.state
    }
}

/// The requesting half of Req/Rep.
pub type Requester = Endpoint<Req>;
/// The replying half of Req/Rep.
pub type Replier = Endpoint<Rep>;
/// The sending half of Push/Pull.
pub type Pusher = Endpoint<Push>;
/// The receiving half of Push/Pull.
pub type Puller = Endpoint<Pull>;
/// The publishing half of Pub/Sub.
pub type Publisher = Endpoint<Pub>;
/// The subscribing half of Pub/Sub.
pub type Subscriber = Endpoint<Sub>;
/// Either half of PAIR — the pattern is symmetric, so there is one role.
pub type Paired = Endpoint<Pair>;
/// One member of a BUS.
pub type BusMember = Endpoint<Bus>;
/// The sending half of RADIO/DISH: binds, and fans segments out to every
/// dish whose filter matches, dropping rather than waiting.
pub type Radio = Endpoint<Cast>;
/// The receiving half of RADIO/DISH: connects, joins topics, and receives
/// the newest segment of each.
pub type Dish = Endpoint<Tune>;
/// The bound half of SWARM: hands each fetch's session about a blob to the
/// application, which answers its orders from its own storage.
#[cfg(feature = "swarm")]
pub type Seeder = Endpoint<Seed>;
/// The dialling half of SWARM: connects to seeders and fetches a blob from
/// all of them at once, verifying every piece against the root.
#[cfg(feature = "swarm")]
pub type Swarm = Endpoint<Gather>;

/// State of a requester.
pub struct ReqState {
    peer: Peer,
}
/// The asking half of SURVEY.
pub type Surveyor = Endpoint<Survey>;
/// The answering half of SURVEY.
pub type Respondent = Endpoint<Respond>;

impl ReqState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> ReqState {
        ReqState {
            peer: Peer::new(runtime, tls),
        }
    }
}

impl Requester {
    /// Connects to `weida://host:port/path`.
    ///
    /// The address is kept: a lost connection is redialled by the runtime
    /// under [`crate::RuntimeConfig::reconnect`], and [`Requester::events`]
    /// reports it.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state.peer.connect(url).await
    }

    /// Forgets a dialled address; see [`Peer::disconnect`].
    pub fn disconnect(&self, url: &str) -> bool {
        self.state.peer.disconnect(url)
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// One record per live connection, labelled by the URL as dialled; see
    /// [`Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state.peer.connection_stats()
    }

    /// The event stream of this requester's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state.peer.events()
    }

    /// Opens one exchange: a request stream and its reply half.
    ///
    /// Both halves belong to the same bidirectional QUIC stream, so no
    /// correlation identifier is needed and none is sent.
    pub async fn open(&self, meta: TransferMeta) -> Result<(OutgoingTransfer, ReplyStream), Error> {
        self.state.peer.open_bi(meta).await
    }

    /// Sends `body` as one request and returns the reply stream.
    ///
    /// Convenience over [`Requester::open`] for small payloads; it never
    /// materializes the reply. The delivery receipt is discarded: a reply is a
    /// stronger answer than a transport acknowledgement, and waiting for both
    /// would only add a round trip.
    pub async fn request(&self, body: &[u8]) -> Result<IncomingTransfer, Error> {
        self.request_with(TransferMeta::default(), body).await
    }

    /// Like [`Requester::request`], with explicit metadata.
    pub async fn request_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
    ) -> Result<IncomingTransfer, Error> {
        let (mut transfer, reply) = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish()?;
        reply.recv().await
    }
}

/// State of a replier.
pub struct RepState {
    path: Arc<str>,
    /// An async mutex so `accept(&self)` needs no `&mut self`; contention is
    /// between deliberate concurrent acceptors only.
    queue: Mutex<mpsc::Receiver<IncomingRequest>>,
}

impl RepState {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<IncomingRequest>) -> RepState {
        RepState {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }
}

impl Replier {
    /// The endpoint path this replier serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Waits for the next request.
    ///
    /// Requests queue up to `Limits::endpoint_queue`; beyond that the sending
    /// side blocks, which propagates backpressure to the peer through QUIC flow
    /// control rather than growing an unbounded queue (master doc §50).
    pub async fn accept(&self) -> Result<IncomingRequest, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

// --- Push/Pull ------------------------------------------------------------

/// State of a pusher.
pub struct PushState {
    peer: Peer,
}

impl PushState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> PushState {
        PushState {
            peer: Peer::new(runtime, tls),
        }
    }
}

impl Pusher {
    /// Connects to `weida://host:port/path`.
    ///
    /// The address is kept: a lost connection is redialled by the runtime
    /// under [`crate::RuntimeConfig::reconnect`], and [`Pusher::events`]
    /// reports it.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state.peer.connect(url).await
    }

    /// Forgets a dialled address; see [`Peer::disconnect`].
    pub fn disconnect(&self, url: &str) -> bool {
        self.state.peer.disconnect(url)
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// One record per live connection, labelled by the URL as dialled; see
    /// [`Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state.peer.connection_stats()
    }

    /// The event stream of this pusher's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state.peer.events()
    }

    /// Bodies the outbox discarded; see [`Peer::dropped`].
    pub fn dropped(&self) -> u64 {
        self.state.peer.dropped()
    }

    /// Opens a one-way transfer to the next peer, round-robin.
    ///
    /// Use this rather than [`Pusher::send`] when the transport receipt
    /// matters: [`OutgoingTransfer::finish`] hands back a
    /// [`crate::Delivery`] that resolves once the peer's transport holds every
    /// byte. The stream is the application's: with every peer down this
    /// waits for the redial, and a stream on a connection that dies fails
    /// with `ConnectionLost` and is not reopened by anyone but the caller.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        self.state.peer.open(meta).await
    }

    /// Sends `body` as one message: the runtime owns it from this call.
    ///
    /// Pipeline semantics: the delivery receipt is discarded, so a push costs
    /// no round trip and reports only failures the local side already knows
    /// about. With a live peer the body is written at once; with none it is
    /// held in the outbox and written once a peer is back, in order, and a
    /// full outbox applies the configured backpressure ([`Peer::send`]).
    /// Callers who want the receipt use [`Pusher::open`] and await
    /// [`crate::Delivery::delivered`] themselves.
    pub async fn send(&self, body: &[u8]) -> Result<(), Error> {
        self.send_with(TransferMeta::default(), body).await
    }

    /// Like [`Pusher::send`], with explicit metadata.
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        self.state.peer.send(meta, body).await
    }
}

/// State of a puller.
pub struct PullState {
    path: Arc<str>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
}

impl PullState {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<IncomingTransfer>) -> PullState {
        PullState {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }
}

impl Puller {
    /// The endpoint path this puller serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Waits for the next transfer.
    ///
    /// `recv`, not `accept`: a pulled transfer is a payload with metadata, not
    /// a request that owes a reply. Backpressure works exactly as it does for a
    /// replier — the queue is bounded by `Limits::endpoint_queue` and a full
    /// queue stalls the sender through QUIC flow control.
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

// --- Pub/Sub --------------------------------------------------------------

/// State of a publisher.
pub struct PubState {
    path: Arc<str>,
    registry: Arc<SubRegistry>,
    max_payload: usize,
}

impl PubState {
    pub(crate) fn new(path: &str, registry: Arc<SubRegistry>, max_payload: usize) -> PubState {
        PubState {
            path: Arc::from(path),
            registry,
            max_payload,
        }
    }
}

impl Publisher {
    /// The endpoint path this publisher serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Fans `payload` out to every subscriber whose filter is a prefix of
    /// `topic`, and returns how many it was enqueued to.
    ///
    /// Synchronous and non-blocking: publishing never waits on a subscriber.
    /// A subscriber whose byte budget is exhausted loses this message, which is
    /// counted in [`Publisher::dropped`]. Zero subscribers is not an error —
    /// a publisher with no audience is the normal startup state.
    ///
    /// Fails with [`Error::LimitExceeded`] when `payload` is larger than
    /// `Limits::subscriber_buffer_bytes`: such a message could never be
    /// enqueued for anyone, so reporting it beats silently dropping it for
    /// every subscriber. It fails the same way for a `topic` above 256 bytes,
    /// which a subscriber's decoder refuses by closing its connection.
    ///
    /// No `traceparent` is written unless one is propagated: see
    /// [`Publisher::publish_with_trace`] and
    /// [0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md).
    pub fn publish(&self, topic: &str, payload: impl Into<Bytes>) -> Result<usize, Error> {
        self.publish_inner(topic, payload.into(), None)
    }

    /// Like [`Publisher::publish`], propagating an existing trace context.
    ///
    /// The context comes from the caller — an inbound
    /// [`crate::IncomingMeta::trace`], or [`crate::new_trace`] to start one.
    pub fn publish_with_trace(
        &self,
        topic: &str,
        payload: impl Into<Bytes>,
        trace: TraceContext,
    ) -> Result<usize, Error> {
        self.publish_inner(topic, payload.into(), Some(trace))
    }

    fn publish_inner(
        &self,
        topic: &str,
        payload: Bytes,
        trace: Option<TraceContext>,
    ) -> Result<usize, Error> {
        if payload.len() > self.state.max_payload {
            return Err(Error::LimitExceeded);
        }
        let want = u32::try_from(payload.len()).map_err(|_| Error::LimitExceeded)?;
        self.state
            .registry
            .publish(&self.state.path, topic, payload, trace, want)
    }

    /// Opens a streamed publish: one stream per matched subscriber, written
    /// chunk by chunk.
    ///
    /// This is the fan-out for a payload that is not in memory and need not
    /// be — a video frame read from a capture device, a file, a response body
    /// being forwarded
    /// ([requirements/zeughaus-video.md](../../../docs/requirements/zeughaus-video.md)
    /// request 1). [`Publisher::publish`] refuses a payload above
    /// `Limits::subscriber_buffer_bytes` because such a message could not be
    /// enqueued for anybody; here that limit applies to a **chunk**, so the
    /// payload has no ceiling at all.
    ///
    /// The subscriber set is fixed at this call: a subscriber that arrives
    /// while the payload is in flight would receive a fragment with no way to
    /// know it, so it gets the next message instead. Zero subscribers is not
    /// an error, exactly as for [`Publisher::publish`]. A `topic` above 256
    /// bytes cannot be sent to anyone, since a subscriber's decoder refuses
    /// it by closing its connection: the fan-out reaches nobody and refuses
    /// every write with [`Error::LimitExceeded`].
    ///
    /// Every guarantee of [`Publisher::publish`] holds per subscriber rather
    /// than per publish: see [`FanOut`].
    pub fn open(&self, topic: &str) -> FanOut {
        self.state.registry.open(&self.state.path, topic, None)
    }

    /// Like [`Publisher::open`], propagating an existing trace context.
    pub fn open_with_trace(&self, topic: &str, trace: TraceContext) -> FanOut {
        self.state
            .registry
            .open(&self.state.path, topic, Some(trace))
    }

    /// Connections currently subscribed to this publisher.
    pub fn subscriber_count(&self) -> usize {
        self.state.registry.subscriber_count(&self.state.path)
    }

    /// Filters registered on this publisher, summed over subscribers.
    pub fn filter_count(&self) -> usize {
        self.state.registry.filter_count(&self.state.path)
    }

    /// Messages dropped because a subscriber could not take them, summed
    /// over topics and causes. "Something is being discarded"; for "which
    /// signal is starving", see [`Publisher::drops`].
    pub fn dropped(&self) -> u64 {
        self.state.registry.dropped(&self.state.path)
    }

    /// What was dropped on `topic`, by cause, or `None` if nothing was.
    ///
    /// Maintained as the drops happen, so this is a lookup and not a scan.
    /// The table holds at most `Limits::max_sequence_scopes` topics — the
    /// same ceiling the sequencer's per-topic table has — and a topic beyond
    /// it counts in [`Publisher::dropped`] only.
    pub fn dropped_on(&self, topic: &str) -> Option<crate::TopicDrops> {
        self.state.registry.dropped_on(&self.state.path, topic)
    }

    /// Every topic that lost a copy, with its counts by cause.
    pub fn drops(&self) -> Vec<crate::TopicDrops> {
        self.state.registry.drops(&self.state.path)
    }
}

/// What a subscriber does on every connection it gets, first dial and
/// redial alike: claim the path as its route, fill the reverse pool where
/// the transport needs one, and send the filters it holds
/// ([0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)
/// §4.6). The publisher kept nothing, so nothing else is restored.
struct SubAttach {
    filters: Arc<std::sync::Mutex<HashSet<String>>>,
    queue_tx: mpsc::Sender<IncomingTransfer>,
}

impl Attach for SubAttach {
    fn attach<'a>(
        &'a self,
        conn: &'a ConnHandle,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            if conn.conn.needs_reverse_pool() && conn.conn.park_reverse().await? == 0 {
                return Err(Error::Unsupported);
            }
            conn.namespace
                .register(path, Route::Transfer(self.queue_tx.clone()))?;
            if conn.conn.needs_reverse_pool() {
                let maintaining = Arc::clone(conn);
                conn.exec
                    .spawn(async move { maintaining.conn.maintain_reverse().await });
            }
            let filters: Vec<String> = self
                .filters
                .lock()
                .expect("filter set poisoned")
                .iter()
                .cloned()
                .collect();
            for filter in filters {
                send_subscription(conn, FrameKind::Subscribe, path, &filter, None, None).await?;
            }
            Ok(())
        })
    }
}

/// State of a subscriber.
pub struct SubState {
    peer: Peer,
    /// Filters this subscriber wants, remembered so a peer connected — or
    /// redialled — later receives the same subscriptions. Shared with the
    /// attachment the peer runs on every connection.
    filters: Arc<std::sync::Mutex<HashSet<String>>>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
}

impl SubState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>, depth: usize) -> SubState {
        let (queue_tx, queue) = mpsc::channel(depth);
        let filters = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let attach = SubAttach {
            filters: Arc::clone(&filters),
            queue_tx,
        };
        SubState {
            peer: Peer::with_attach(runtime, tls, Some(Arc::new(attach))),
            filters,
            queue: Mutex::new(queue),
        }
    }
}

impl Subscriber {
    /// Connects to `weida://host:port/path` and subscribes to every filter
    /// registered so far.
    ///
    /// The subscriber registers `path` in the *client* connection's namespace:
    /// fanned-out copies arrive as ordinary inbound one-way transfers, so the
    /// dialling side needs a route for them. Two subscribers sharing a pooled
    /// connection and claiming the same path therefore collide with
    /// [`Error::AlreadyRegistered`]; fanning one subscription out to several
    /// in-process consumers is the application's business, not the transport's.
    ///
    /// On a transport where the accepting side cannot open a stream back,
    /// this is also where the reverse pool is filled: the subscriber parks
    /// `Limits::max_parked_reverse` connections and keeps replacing them as
    /// the publisher spends them
    /// ([0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.4). A subscriber that parks nothing - because the limit is zero,
    /// or because the peer refused every attempt - cannot receive fan-out at
    /// all, and learns that here rather than by waiting forever.
    ///
    /// The address is kept: when the connection is lost the runtime redials
    /// it and does all of the above again on the new connection, filters
    /// included. What was published in between is gone — the publisher
    /// keeps no subscription across connections — and
    /// [`Subscriber::events`] says when the gap began and ended.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state.peer.connect(url).await
    }

    /// Forgets a dialled address; see [`Peer::disconnect`].
    pub fn disconnect(&self, url: &str) -> bool {
        self.state.peer.disconnect(url)
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// One record per live connection, labelled by the URL as dialled; see
    /// [`Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state.peer.connection_stats()
    }

    /// The event stream of this subscriber's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state.peer.events()
    }

    /// Registers interest in every topic `filter` matches.
    ///
    /// The filter is a **segmented pattern** ([`PROTOCOL.md`] §6.4): segments
    /// are split on `.`, `*` matches exactly one whole segment, a trailing
    /// `#` matches zero or more segments and must be the last one, and every
    /// other byte is literal — no escape character, no normalization, no case
    /// folding. `""` receives everything, and so does `#`. A published topic
    /// is never a pattern: `*` in a topic is an ordinary byte.
    ///
    /// So `sensors.*.temp` selects one segment in the middle, `sensors.#`
    /// selects `sensors` and everything under it, and `sensors.temp` does
    /// **not** select `sensors.temperature`.
    ///
    /// Subscribing twice to the same filter is idempotent. A filter that
    /// violates the grammar fails here with [`Error::Protocol`] rather than
    /// travelling to the publisher, which would answer it by closing the
    /// connection.
    ///
    /// [`PROTOCOL.md`]: https://github.com/tuco86/weida/blob/main/docs/PROTOCOL.md
    pub async fn subscribe(&self, filter: &str) -> Result<(), Error> {
        weida_protocol::filter::validate(filter)?;
        let fresh = self
            .state
            .filters
            .lock()
            .expect("filter set poisoned")
            .insert(filter.to_owned());
        if !fresh {
            return Ok(());
        }
        self.broadcast(FrameKind::Subscribe, filter).await
    }

    /// Withdraws a filter. Unknown filters are ignored.
    pub async fn unsubscribe(&self, filter: &str) -> Result<(), Error> {
        let known = self
            .state
            .filters
            .lock()
            .expect("filter set poisoned")
            .remove(filter);
        if !known {
            return Ok(());
        }
        self.broadcast(FrameKind::Unsubscribe, filter).await
    }

    /// Filters currently registered.
    pub fn filter_count(&self) -> usize {
        self.state
            .filters
            .lock()
            .expect("filter set poisoned")
            .len()
    }

    /// Waits for the next published message.
    ///
    /// The topic is on [`crate::IncomingMeta::topic`].
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }

    /// States how many messages this subscriber will accept in total on the
    /// subscription `filter`, and sends it to every connected peer.
    ///
    /// This is the L2 credit of
    /// [0003](https://github.com/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
    /// §4.2, and it matters only where a **queue** serves the path: a
    /// publisher fans a message out to every matching subscriber and consults
    /// no credit, while a queue delivers to one consumer and delivers nothing
    /// at all until that consumer has granted some.
    ///
    /// **The limit is absolute and cumulative**, counted from the
    /// subscription's creation — not a delta and not a window — so a lost
    /// grant costs nothing and a duplicated one changes nothing. A broker
    /// keeps the **highest** limit it has seen for a subscription, which is
    /// what makes a reordered grant harmless: every control frame rides its
    /// own unidirectional stream, and QUIC does not order those against each
    /// other.
    ///
    /// Two consequences worth stating, because they are the whole usage
    /// pattern:
    ///
    /// - **An unmentioned subscription has a limit of zero**: a consumer that
    ///   subscribes and grants nothing receives nothing. That is the only
    ///   default that cannot surprise a consumer with a flood.
    /// - **A standing limit cannot be lowered.** Monotone means what it
    ///   says: a grant below the standing limit is ignored, so restating the
    ///   count already delivered does *not* pause delivery — it changes
    ///   nothing at all unless that count had already reached the limit.
    ///   The only pause in v0 is the `0` a fresh subscription starts at, so
    ///   a consumer that wants to stay in control grants in increments it is
    ///   willing to receive rather than one large number it means to
    ///   withdraw later. Lowering a limit would need a wire field that
    ///   distinguishes a newer statement from an older one, which v0 does
    ///   not have — and without it a "pause" could be undone by a delayed
    ///   earlier grant.
    pub async fn grant(&self, filter: &str, limit: u64) -> Result<(), Error> {
        weida_protocol::filter::validate(filter)?;
        for (conn, path) in self.state.peer.live_peers() {
            let header = CreditHeader::new(path.as_ref(), filter, limit);
            // Refused before anything is written: a header no peer would read,
            // or one longer than the peer said it reads, makes it close the
            // connection.
            header.check().map_err(|_| Error::LimitExceeded)?;
            let header = header.encode();
            check_header_len(&conn, header.len())?;
            write_control(&conn.conn, FrameKind::Credit, &header).await?;
        }
        Ok(())
    }

    async fn broadcast(&self, kind: FrameKind, filter: &str) -> Result<(), Error> {
        for (conn, path) in self.state.peer.live_peers() {
            send_subscription(&conn, kind, &path, filter, None, None).await?;
        }
        Ok(())
    }
}

pub(crate) async fn send_subscription(
    conn: &ConnHandle,
    kind: FrameKind,
    path: &str,
    filter: &str,
    max_age_ms: Option<u64>,
    max_layer: Option<u8>,
) -> Result<(), Error> {
    let header = SubscriptionHeader {
        max_age_ms,
        max_layer,
        ..SubscriptionHeader::new(path, filter)
    };
    // Refused before anything is written: a filter or path no peer would
    // read, or a header longer than the peer said it reads, makes it close
    // the connection, and a subscriber re-sends its filters on every redial.
    header.check().map_err(|_| Error::LimitExceeded)?;
    let header = header.encode();
    check_header_len(conn, header.len())?;
    write_control(&conn.conn, kind, &header).await
}

impl Drop for SubState {
    fn drop(&mut self) {
        // Best effort: tell each peer we are gone and release the path so the
        // next subscriber on this pooled connection can claim it.
        //
        // The frames go through the connection actor with a non-blocking
        // `notify`, not `tokio::spawn`: a destructor may run on a thread with
        // no reactor, and panicking there aborts the process instead of
        // unwinding. The publisher drops the subscription anyway once the
        // connection closes, so a lost notification costs nothing.
        let filters: Vec<String> = self
            .filters
            .lock()
            .expect("filter set poisoned")
            .iter()
            .cloned()
            .collect();
        for (conn, path) in self.peer.live_peers() {
            conn.namespace.unregister(&path);
            for filter in &filters {
                conn.notify(Ctl::SendUnsubscribe {
                    path: Arc::clone(&path),
                    filter: filter.clone(),
                });
            }
        }
    }
}

// --- PAIR -----------------------------------------------------------------

/// State of a paired endpoint.
///
/// One type for both sides, because PAIR is symmetric: both send and receive
/// with the same calls. What differs is only *how each side reaches its
/// peer*, and that is the whole asymmetry of a bound endpoint in this
/// library — a dialling side holds a [`Peer`], a bound side learns its peer
/// when that peer first speaks, which is what [`PairOwner`] records.
pub struct PairState {
    /// The dialling side's peer. `None` on a bound pair.
    peer: Option<Peer>,
    /// The bound side's peer, once it has claimed the endpoint. `None` on a
    /// dialling pair.
    owner: Option<Arc<PairOwner>>,
    /// The bound path; empty on a dialling pair, whose path is the one it
    /// dialled and is already recorded in its [`Peer`].
    path: Arc<str>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
}

/// What a dialling pair does on every connection it gets: claim the path as
/// its route and fill the reverse pool where the transport needs one — on
/// the redialled connection as on the first (0031 §4.6).
struct PairAttach {
    queue_tx: mpsc::Sender<IncomingTransfer>,
}

impl Attach for PairAttach {
    fn attach<'a>(
        &'a self,
        conn: &'a ConnHandle,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            if conn.conn.needs_reverse_pool() && conn.conn.park_reverse().await? == 0 {
                return Err(Error::Unsupported);
            }
            conn.namespace.register(
                path,
                Route::Pair {
                    queue: self.queue_tx.clone(),
                    owner: Arc::new(PairOwner::new()),
                },
            )?;
            if conn.conn.needs_reverse_pool() {
                let maintaining = ConnHandle::clone(conn);
                conn.exec
                    .spawn(async move { maintaining.conn.maintain_reverse().await });
            }
            Ok(())
        })
    }
}

impl PairState {
    pub(crate) fn bound(
        path: &str,
        owner: Arc<PairOwner>,
        queue: mpsc::Receiver<IncomingTransfer>,
    ) -> PairState {
        PairState {
            peer: None,
            owner: Some(owner),
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }

    pub(crate) fn dialling(
        runtime: Arc<RuntimeInner>,
        tls: Arc<ClientTls>,
        depth: usize,
    ) -> PairState {
        let (queue_tx, queue) = mpsc::channel(depth);
        let attach = PairAttach { queue_tx };
        PairState {
            peer: Some(Peer::with_attach(runtime, tls, Some(Arc::new(attach)))),
            owner: None,
            path: Arc::from(""),
            queue: Mutex::new(queue),
        }
    }
}

impl Paired {
    /// The endpoint path this pair uses; empty on a dialling pair that has
    /// not connected yet.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Connects to `weida://host:port/path`, once.
    ///
    /// A pair has **one** peer, so a second call is refused with
    /// [`Error::LimitExceeded`] rather than quietly becoming a Push: two
    /// peers on one endpoint would need a selection policy, and PAIR is the
    /// pattern that has none. On a bound pair there is nothing to dial and
    /// this is [`Error::Unsupported`].
    ///
    /// Like a subscriber, a dialling pair registers the path in the *client*
    /// connection's namespace: the peer's transfers arrive as ordinary
    /// inbound one-way transfers, so the dialling side needs a route for
    /// them — and on a transport where the accepting side cannot open a
    /// stream back, the reverse pool is filled here
    /// ([0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.4).
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let Some(peer) = self.state.peer.as_ref() else {
            return Err(Error::Unsupported);
        };
        // One address, live or being redialled: a second one would be a
        // second peer.
        if peer.slot_count() > 0 {
            return Err(Error::LimitExceeded);
        }
        peer.connect(url).await
    }

    /// Number of connected peers: `0` or `1`.
    pub fn peer_count(&self) -> usize {
        self.state.peer.as_ref().map_or(0, Peer::peer_count)
    }

    /// The dialling side's connection, labelled by the URL as dialled; see
    /// [`Peer::connection_stats`]. Empty on a bound pair, which dialled
    /// nothing.
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state
            .peer
            .as_ref()
            .map_or_else(Vec::new, Peer::connection_stats)
    }

    /// The event stream of a dialling pair's address; see [`Peer::events`].
    /// A bound pair dials nothing and reports nothing: `None`.
    pub fn events(&self) -> Option<PeerEvents> {
        self.state.peer.as_ref().map(Peer::events)
    }

    /// Bodies a dialling pair's outbox discarded; see [`Peer::dropped`].
    pub fn dropped(&self) -> u64 {
        self.state.peer.as_ref().map_or(0, Peer::dropped)
    }

    /// Opens one outgoing transfer to the peer.
    ///
    /// The two sides reach their peer differently and that is the pattern's
    /// only asymmetry. A dialling pair sends on the connection it opened. A
    /// **bound** pair sends on the connection its peer dialled — and it can
    /// only know which one that is once the peer has spoken, so the first
    /// send on a bound pair **waits for the peer to appear**. That is not a
    /// queue: nothing is buffered, and a bound pair with no peer has nobody
    /// to address rather than a backlog to flush.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        if let Some(peer) = self.state.peer.as_ref() {
            return peer.open(meta).await;
        }
        let owner = self
            .state
            .owner
            .as_ref()
            .expect("a pair is either dialling or bound");
        let conn = owner.peer().await?;
        crate::stream::open_transfer_on(&conn, &self.state.path, &meta).await
    }

    /// Sends `body` as one message.
    ///
    /// On a dialling pair the runtime owns the body from this call: written
    /// at once while the peer is live, held in the outbox and written after
    /// the redial otherwise ([`Peer::send`]). On a bound pair it is written
    /// once the peer has appeared, as [`Paired::open`] describes.
    pub async fn send(&self, body: &[u8]) -> Result<(), Error> {
        self.send_with(TransferMeta::default(), body).await
    }

    /// Like [`Paired::send`], with explicit metadata.
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        if let Some(peer) = self.state.peer.as_ref() {
            return peer.send(meta, body).await;
        }
        let mut transfer = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish()?;
        Ok(())
    }

    /// Waits for the next transfer from the peer.
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

impl Drop for PairState {
    /// Releases the dialled path on a pooled connection.
    ///
    /// A dialling pair registers its path in the *client* connection's
    /// namespace, and connections are pooled by authority, trust terms and
    /// path — so without this the next `Runtime::pair().connect()` to the
    /// same URL reuses the connection, finds the dead pair's route still
    /// there and fails with `LimitExceeded`. This is what
    /// [`SubState::drop`] does for a subscription, for the same reason.
    fn drop(&mut self) {
        let Some(peer) = self.peer.as_ref() else {
            return;
        };
        for (conn, path) in peer.live_peers() {
            conn.namespace.unregister(&path);
        }
    }
}

// --- SURVEY ---------------------------------------------------------------

/// State of a surveyor.
pub struct SurveyState {
    peer: Peer,
}

impl SurveyState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> SurveyState {
        SurveyState {
            peer: Peer::new(runtime, tls),
        }
    }
}

/// State of a respondent.
pub struct RespondState {
    path: Arc<str>,
    queue: Mutex<mpsc::Receiver<IncomingRequest>>,
}

impl RespondState {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<IncomingRequest>) -> RespondState {
        RespondState {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }
}

impl Surveyor {
    /// Connects to `weida://host:port/path`.
    ///
    /// A surveyor addresses **every** connected respondent, so unlike a
    /// requester it accumulates peers on purpose: each `connect` adds one to
    /// the set a survey fans out over.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state.peer.connect(url).await
    }

    /// Number of connected respondents.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// One record per live connection, labelled by the URL as dialled; see
    /// [`Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state.peer.connection_stats()
    }

    /// Asks every connected respondent, and collects answers until
    /// `deadline`.
    ///
    /// One exchange per respondent, opened on the connection that respondent
    /// is on — not through the round-robin selection Req/Rep uses, which
    /// would ask exactly one. The deadline is the **caller's** and is not
    /// negotiated: nothing on the wire carries it, and a respondent never
    /// learns it.
    ///
    /// A survey with no respondents is not an error: it returns a
    /// [`SurveyRun`] whose first [`SurveyRun::next`] is `None`, because
    /// "nobody answered" is an answer.
    pub async fn survey(&self, body: &[u8], deadline: Duration) -> Result<SurveyRun, Error> {
        self.survey_with(TransferMeta::default(), body, deadline)
            .await
    }

    /// Like [`Surveyor::survey`], with explicit metadata.
    pub async fn survey_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
        deadline: Duration,
    ) -> Result<SurveyRun, Error> {
        let targets = self.state.peer.live_peers();

        let exec = self.state.peer.exec().clone();
        let late = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let expires = exec.now() + deadline;
        // One slot per respondent: every answer has somewhere to go, so a
        // reply is never late merely because the caller is slow to read.
        let (tx, rx) = mpsc::channel(targets.len().max(1));
        let mut respondents = 0usize;
        let mut collecting = Vec::with_capacity(targets.len());

        for (conn, path) in targets {
            // The deadline bounds the **asking** too, not just the
            // collecting. Both of these awaits are the peer's to stall:
            // `open_exchange_on` waits on its stream budget and `write_all`
            // on its flow-control window, so without this one respondent
            // that accepts an exchange and stops reading would hold a
            // 100 ms survey open forever and every respondent after it
            // would never be asked at all.
            let remaining = expires.saturating_duration_since(exec.now());
            let asked = exec.within(remaining, ask(&conn, &path, &meta, body)).await;
            let Some(asked) = asked else {
                tracing::debug!(%path, "the survey deadline passed before this respondent was asked");
                break;
            };
            let reply = match asked {
                Ok(reply) => reply,
                // A respondent that cannot even be asked is not an answer
                // and not a failure of the survey: the others are still
                // being asked.
                Err(e) => {
                    tracing::debug!(error = %e, %path, "a respondent could not be asked");
                    continue;
                }
            };
            respondents += 1;
            let tx = tx.clone();
            let late = Arc::clone(&late);
            let collector_exec = exec.clone();
            collecting.push(exec.spawn(async move {
                let answer = reply.recv().await;
                // Counted rather than delivered, and counted **here** so the
                // number means "after the deadline" whatever the caller does
                // with its handle (`docs/GUARANTEES.md` §6).
                if collector_exec.now() >= expires || tx.send(answer).await.is_err() {
                    late.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }));
        }
        drop(tx);

        Ok(SurveyRun {
            rx,
            exec,
            expires,
            late,
            respondents,
            collecting,
            done: false,
        })
    }
}

/// Asks one respondent: the whole question, header to FIN.
///
/// Separate from the loop so the deadline can bound all three awaits
/// together — a question half-written when the deadline passes is abandoned
/// by dropping the transfer, which resets the stream and is exactly what a
/// respondent should see for a question nobody waits for any more.
async fn ask(
    conn: &ConnHandle,
    path: &str,
    meta: &TransferMeta,
    body: &[u8],
) -> Result<ReplyStream, Error> {
    let (mut request, reply) = crate::stream::open_exchange_on(conn, path, meta.clone()).await?;
    request.write_all(body).await?;
    request.finish()?;
    Ok(reply)
}

/// One survey in progress: the answers, and what the deadline cost.
pub struct SurveyRun {
    rx: mpsc::Receiver<Result<IncomingTransfer, Error>>,
    exec: crate::runtime::Exec,
    expires: Instant,
    late: Arc<std::sync::atomic::AtomicU64>,
    respondents: usize,
    /// One collector per respondent, held so that dropping the run ends
    /// them: each owns a [`ReplyStream`], and therefore one of this side's
    /// bidirectional stream slots, until the answer arrives.
    collecting: Vec<tokio::task::JoinHandle<()>>,
    done: bool,
}

impl SurveyRun {
    /// The next answer, or `None` once the deadline has passed or every
    /// respondent has answered.
    ///
    /// `max_bytes` caps the reply this call reads; nothing is buffered before
    /// the caller states a cap. A respondent that refused or died is one
    /// `Some(Err(_))` among the answers and ends nothing: the other
    /// respondents are independent exchanges.
    pub async fn next(&mut self, max_bytes: usize) -> Option<Result<Vec<u8>, Error>> {
        if self.done {
            return None;
        }
        // An answer already in hand is an answer. `within` selects without
        // bias, so at `remaining == 0` — a caller that reads after its own
        // deadline, which is the ordinary case — the deadline branch and a
        // ready `recv` are both ready and the choice is a coin flip. Taking
        // the buffer first is what keeps an answer that arrived in time from
        // being lost to that flip, silently and without being counted.
        match self.rx.try_recv() {
            Ok(Ok(transfer)) => return Some(transfer.collect(max_bytes).await),
            Ok(Err(e)) => return Some(Err(e)),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.done = true;
                return None;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        let remaining = self.expires.saturating_duration_since(self.exec.now());
        match self.exec.within(remaining, self.rx.recv()).await {
            // The deadline. Anything that landed while this call was waiting
            // is counted here: its collector already succeeded in handing it
            // over, so it cannot count itself, and a dropped answer that
            // nothing counts is the one outcome `late` exists to prevent.
            None => {
                self.done = true;
                while self.rx.try_recv().is_ok() {
                    self.late.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                None
            }
            // Every respondent has answered or failed.
            Some(None) => {
                self.done = true;
                None
            }
            Some(Some(Ok(transfer))) => Some(transfer.collect(max_bytes).await),
            Some(Some(Err(e))) => Some(Err(e)),
        }
    }

    /// Answers that arrived after the deadline: dropped, and counted.
    pub fn late(&self) -> u64 {
        self.late.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many respondents were asked.
    pub fn respondents(&self) -> usize {
        self.respondents
    }
}

impl Drop for SurveyRun {
    /// Ends the collectors with the run.
    ///
    /// A collector's first await is the answer, so a respondent that accepts
    /// a question and never answers would keep its task — and the
    /// bidirectional stream slot its [`ReplyStream`] holds — until the
    /// connection dies. Surveying such a respondent repeatedly would then
    /// exhaust this side's stream budget and the *next* survey would block
    /// in `open_bi`. Dropping the run is the caller saying it wants no more
    /// answers, so the streams go with it: each abort drops a `ReplyStream`,
    /// which resets that exchange.
    fn drop(&mut self) {
        for handle in &self.collecting {
            handle.abort();
        }
    }
}

impl std::fmt::Debug for SurveyRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurveyRun")
            .field("respondents", &self.respondents)
            .field("late", &self.late())
            .finish_non_exhaustive()
    }
}

impl Respondent {
    /// The endpoint path this respondent serves.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Waits for the next survey question.
    ///
    /// A question is an exchange, so this is a [`Replier::accept`] in every
    /// respect — the respondent needs no route of its own and no new wire
    /// vocabulary. What differs is only that the asker fans out and holds a
    /// deadline.
    pub async fn accept(&self) -> Result<IncomingRequest, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

// --- BUS ------------------------------------------------------------------

/// State of a bus member.
///
/// The one role in this library that is **bound and dialling at once**: a
/// member accepts on its own path and dials every other member. That is why
/// its factory takes both a path and dialling terms, and why nothing else
/// does.
pub struct BusState {
    peer: Peer,
    path: Arc<str>,
    queue: Mutex<mpsc::Receiver<IncomingTransfer>>,
    /// One writer per joined member, so a member that stops reading stalls
    /// **its own** queue rather than the sender's task. This is Pub/Sub's
    /// shape for the same reason: a fan-out that can be blocked by one slow
    /// peer is not a fan-out (`docs/GUARANTEES.md` §6).
    writers: std::sync::Mutex<Vec<BusWriter>>,
    /// Copies that never reached a member, by the same rule Pub/Sub uses:
    /// best effort per peer, counted rather than retried.
    dropped: Arc<std::sync::atomic::AtomicU64>,
    /// Messages one member's writer may hold.
    depth: usize,
    /// Bytes one member's writer may hold, `Limits::subscriber_buffer_bytes`.
    ///
    /// The message count alone bounds nothing: a bus send costs
    /// `depth × body.len()` per member and multiplies by the members, and
    /// `body.len()` is the application's number. This is the second
    /// dimension Pub/Sub already charges per subscriber, for the identical
    /// fan-out (`docs/INVARIANTS.md`).
    budget: usize,
}

/// One joined member's writer queue.
struct BusWriter {
    tx: mpsc::Sender<BusMsg>,
    /// This member's share of the byte budget, returned by its writer once
    /// the bytes are on the wire.
    budget: Arc<Semaphore>,
}

/// One message on its way to one member.
///
/// The payload is a `Bytes`: a bus send makes **one** copy of the body and
/// every member's queue holds a reference to it, which is what Pub/Sub does
/// with a published payload and for the same reason — a copy per member is
/// the one cost a fan-out must not pay per member.
struct BusMsg {
    meta: TransferMeta,
    body: Bytes,
}

impl BusState {
    pub(crate) fn new(
        path: &str,
        runtime: Arc<RuntimeInner>,
        tls: Arc<ClientTls>,
        queue: mpsc::Receiver<IncomingTransfer>,
        depth: usize,
        budget: usize,
    ) -> BusState {
        BusState {
            peer: Peer::new(runtime, tls),
            path: Arc::from(path),
            queue: Mutex::new(queue),
            writers: std::sync::Mutex::new(Vec::new()),
            dropped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            depth,
            budget,
        }
    }
}

impl BusMember {
    /// The path this member accepts on.
    pub fn path(&self) -> &str {
        &self.state.path
    }

    /// Joins another member.
    ///
    /// Joining is an ordinary connect and leaving an ordinary disconnect:
    /// there is no membership protocol, no election and nothing to
    /// synchronize, which is what keeps a bus a fan-out rather than a group.
    ///
    /// One writer task per member is started here, which is what makes a
    /// slow member cost its own copies rather than the sender's time.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        let (conn, path) = self.state.peer.dial(url).await?;
        let (tx, rx) = mpsc::channel(self.state.depth);
        let budget = Arc::new(Semaphore::new(self.state.budget));
        conn.exec.spawn(bus_writer(
            conn.clone(),
            path,
            rx,
            Arc::clone(&budget),
            Arc::clone(&self.state.dropped),
        ));
        self.state
            .writers
            .lock()
            .expect("bus writers poisoned")
            .push(BusWriter { tx, budget });
        Ok(())
    }

    /// Number of members this one has joined.
    pub fn peer_count(&self) -> usize {
        self.state.peer.peer_count()
    }

    /// One record per member this one has dialled and is connected to,
    /// labelled by the URL as dialled; see [`Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state.peer.connection_stats()
    }

    /// Sends `body` to every **other** member, returning how many it reached.
    ///
    /// A message never reaches the sender: structurally, because `send`
    /// writes to the peers this member dialled and a member does not dial
    /// itself. There is also **no relay** — a bus of *n* members is
    /// *n* × (*n* − 1) deliveries, and weida forwards on nobody's behalf,
    /// which is the trade nanomsg's BUS makes too.
    ///
    /// Fan-out is best effort per peer with the same counted drops as Pub/Sub
    /// ([`BusMember::dropped`]): one member that cannot take a copy costs
    /// that copy, not the send.
    pub async fn send(&self, body: &[u8]) -> Result<usize, Error> {
        self.send_with(TransferMeta::default(), body).await
    }

    /// Like [`BusMember::send`], with explicit metadata.
    ///
    /// "Reached" means accepted for delivery to that member, which is what
    /// [`Publisher::publish`] counts too: the bytes are on their way, and a
    /// member whose writer queue is full — in messages or in bytes — loses
    /// **this** copy and is counted in [`BusMember::dropped`].
    ///
    /// Fails with [`Error::LimitExceeded`] for a body above
    /// `Limits::subscriber_buffer_bytes`, exactly as [`Publisher::publish`]
    /// does: such a message could never be enqueued for anybody, so
    /// reporting it beats dropping it for every member.
    pub async fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<usize, Error> {
        if body.len() > self.state.budget {
            return Err(Error::LimitExceeded);
        }
        // One copy for the whole fan-out. Each member's queue then holds a
        // reference to it rather than a `Vec` of its own.
        let body = Bytes::copy_from_slice(body);
        let want = body.len() as u32;
        let mut reached = 0usize;
        let mut dropped = 0u64;
        {
            let mut writers = self.state.writers.lock().expect("bus writers poisoned");
            writers.retain(|writer| !writer.tx.is_closed());
            for writer in writers.iter() {
                let Ok(permit) = writer.budget.try_acquire_many(want) else {
                    dropped += 1;
                    continue;
                };
                let msg = BusMsg {
                    meta: meta.clone(),
                    body: body.clone(),
                };
                match writer.tx.try_send(msg) {
                    Ok(()) => {
                        // The writer returns the permits once the bytes are
                        // gone; dropping one here would return them while
                        // the copy is still queued.
                        permit.forget();
                        reached += 1;
                    }
                    Err(_) => dropped += 1,
                }
            }
        }
        if dropped > 0 {
            tracing::debug!(dropped, "a bus member could not take a copy");
            self.state
                .dropped
                .fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(reached)
    }

    /// Waits for the next message from another member.
    pub async fn recv(&self) -> Result<IncomingTransfer, Error> {
        let mut queue = self.state.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }

    /// Copies that never reached a member.
    pub fn dropped(&self) -> u64 {
        self.state
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Serializes one member's copies, so a member that stops reading stalls only
/// its own queue.
///
/// A copy that fails on the wire is **counted here**, not silently lost: the
/// writer ends — which is what makes `send_with` prune this member on its
/// next call — and the copy it was carrying shows up in
/// [`BusMember::dropped`], exactly as Pub/Sub's writer counts a copy no
/// parked connection could take.
async fn bus_writer(
    conn: ConnHandle,
    path: Arc<str>,
    mut rx: mpsc::Receiver<BusMsg>,
    budget: Arc<Semaphore>,
    dropped: Arc<std::sync::atomic::AtomicU64>,
) {
    loop {
        let msg = tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => msg,
                None => return,
            },
            // The sender holds this writer's channel for the life of the
            // endpoint, so the channel never closes on its own: without this
            // arm a member whose connection died would park here forever
            // holding an `Arc<ConnCtx>`, which is the same reason the
            // fan-out's writer watches its connection.
            _ = conn.conn.closed() => {
                dropped.fetch_add(rx.len() as u64, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        };
        let len = msg.body.len();
        let outcome = send_one(&conn, &path, msg.meta, &msg.body).await;
        // Returned whatever happened: the copy is no longer queued, so the
        // bytes it held against this member are free either way.
        budget.add_permits(len);
        if let Err(e) = outcome {
            tracing::debug!(error = %e, %path, "a bus write failed; copy dropped");
            // This copy plus whatever was still queued for the member: a
            // member that died is dropped from the set without affecting the
            // others, and what it owed is counted rather than forgotten.
            dropped.fetch_add(1 + rx.len() as u64, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    }
}

/// Writes one whole one-way transfer on `conn`.
async fn send_one(
    conn: &ConnHandle,
    path: &str,
    meta: TransferMeta,
    body: &[u8],
) -> Result<(), Error> {
    let mut transfer = crate::stream::open_transfer_on(conn, path, &meta).await?;
    transfer.write_all(body).await?;
    transfer.finish()?;
    Ok(())
}
