//! L0: the stream core.
//!
//! This is the socket replacement, ZeroMQ's idea rebuilt directly on QUIC. Its
//! primitives are the two stream kinds QUIC gives us and nothing more:
//!
//! * a **unidirectional stream** — bytes in one direction, ended by a FIN, with
//!   a transport receipt ([`crate::Delivery`]) and refusal by stop code;
//! * a **bidirectional stream** — an *exchange*: the initiating half carries a
//!   request, the return half carries a reply or an ERROR.
//!
//! The guarantees are exactly QUIC's: in-order bytes within a stream, no order
//! across streams, flow control per stream and per connection, and cancellation
//! via reset/stop. Nothing here invents delivery semantics on top of that; the
//! patterns in [`crate::endpoint`] are thin wrappers over these two calls, and
//! broker semantics (queues, publisher confirms, consumer acks) belong to a
//! layer that does not exist yet.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use bytes::Bytes;
use tokio::sync::{Mutex, Notify, Semaphore, broadcast, mpsc, watch};
use weida_core::{Address, Error, Limits, LossCause, PeerIdentity};
use weida_protocol::header::limits::MAX_TOPIC_BYTES;

use crate::config::ClientTls;
use crate::conn::ConnHandle;
use crate::pubsub::{DropTable, TopicDrops};
use crate::reconnect::{EVENT_QUEUE, GiveUp, OutboxFull, PeerEvent, PeerEvents, ReconnectPolicy};
use crate::runtime::RuntimeInner;
use crate::segment::{CopyTarget, SegmentTerms, SegmentTopics, open_segment};
use crate::transfer::{
    DataHead, IncomingRequest, IncomingTransfer, OutgoingTransfer, ReplyStream, TransferMeta,
    outgoing_header,
};

/// What a pattern does on every connection its peer establishes, first dial
/// and redial alike.
///
/// A subscriber registers its route and re-sends its filters; a dialling
/// pair registers its route. Both do it here rather than inline in `connect`,
/// so a redialled connection gets its route and filters too. One
/// implementation, called from one place
/// ([0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)
/// §4.6).
pub(crate) trait Attach: Send + Sync {
    fn attach<'a>(
        &'a self,
        conn: &'a ConnHandle,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Undoes [`Attach::attach`] on a connection that stays open: the
    /// endpoint forgot the address or went away, but the pooled connection
    /// may serve others. Releases the route and withdraws what was sent, so
    /// the peer stops sending and a later dial can claim the path again.
    ///
    /// Synchronous and best effort: it runs in `disconnect` and in drops,
    /// which may run with no reactor, so frames go through the connection
    /// actor's non-blocking `notify`.
    fn detach(&self, conn: &ConnHandle, path: &Arc<str>);
}

/// Where one dialled address currently stands.
enum SlotState {
    Live(ConnHandle),
    /// Lost, and the redial task is working on it.
    Down(LossCause),
    /// Lost, and nothing will redial it: the policy ran out, the peer
    /// changed, or this side closed it.
    Gone(LossCause),
}

/// One dialled address: the slot that outlives its connection (0031 §4.1).
struct Slot {
    id: u64,
    url: Arc<str>,
    path: Arc<str>,
    state: SlotState,
    /// Successful redials of this address; the first dial is not one.
    redials: u64,
    /// Ends the slot's redial task when the slot is disconnected.
    stop: Arc<Notify>,
}

/// One `send` body the runtime owns until it is written (0031 §4.2).
struct Queued {
    meta: TransferMeta,
    body: Bytes,
}

/// The sender outbox: bounded in messages and bytes by `RuntimeConfig`.
struct Outbox {
    queue: std::sync::Mutex<VecDeque<Queued>>,
    bytes: AtomicUsize,
    /// Woken when the queue shrinks, for a sender blocked at the bound.
    room: Notify,
    /// Woken when the queue grows, for the drain task.
    filled: Notify,
    draining: AtomicBool,
    dropped: AtomicU64,
}

/// Everything a dialling endpoint's tasks share with it.
///
/// Tasks hold this weakly: the endpoint owns the only strong reference
/// through [`Peer`], so dropping the endpoint ends every task at its next
/// wake, and the `alive` channel in [`Peer`] wakes them for exactly that.
pub(crate) struct PeerShared {
    runtime: Arc<RuntimeInner>,
    tls: Arc<ClientTls>,
    /// What a QUIC connection of this peer is dialled with: the runtime's
    /// limits, or a swarm's with larger receive windows.
    limits: Limits,
    slots: std::sync::Mutex<Vec<Slot>>,
    cursor: AtomicUsize,
    next_slot: AtomicU64,
    /// Woken whenever a slot changes state, for a `pick` waiting on one.
    changed: Notify,
    events: broadcast::Sender<PeerEvent>,
    attach: Option<Arc<dyn Attach>>,
    outbox: Outbox,
    /// The copies the next [`Peer::segment`] on a topic supersedes; the
    /// numbers come from the connection.
    segment_topics: SegmentTopics,
    /// What [`Peer::segment`] copies lost, by topic and cause.
    segment_drops: Arc<DropTable>,
    /// Chunk bytes [`Peer::segment`] copies may hold unwritten, over all
    /// topics.
    segment_budget: Arc<Semaphore>,
}

impl PeerShared {
    /// The next live slot, round-robin, without waiting.
    ///
    /// `Ok(None)` is a slot that is down and being redialled — worth
    /// waiting for. `Err` is either an endpoint that was never connected or
    /// one whose every slot has been given up, with the cause of the last
    /// one examined: an application deciding what to do next needs to tell
    /// an idle timeout from a peer that closed deliberately.
    fn try_pick(&self) -> Result<Option<(ConnHandle, Arc<str>)>, Error> {
        let slots = self.slots.lock().expect("peer list poisoned");
        if slots.is_empty() {
            return Err(Error::NotConnected);
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        let mut cause = None;
        let mut pending = false;
        for offset in 0..slots.len() {
            let slot = &slots[(start + offset) % slots.len()];
            match &slot.state {
                // A connection may have closed before its task noticed. What
                // the task will decide is decided here the same way, so a
                // close that will not be redialled never looks like a wait.
                SlotState::Live(conn) => match conn.conn.close_reason() {
                    None => return Ok(Some((ConnHandle::clone(conn), Arc::clone(&slot.path)))),
                    Some(reason) => {
                        let c = loss_cause(&reason);
                        if self.runtime.config.reconnect.redials_after(c) {
                            pending = true;
                        }
                        cause = Some(c);
                    }
                },
                SlotState::Down(c) => {
                    pending = true;
                    cause = Some(*c);
                }
                SlotState::Gone(c) => cause = Some(*c),
            }
        }
        if pending {
            return Ok(None);
        }
        Err(Error::ConnectionLost(
            cause.unwrap_or(LossCause::LocallyClosed),
        ))
    }

    /// The next live slot, waiting for a redial when every slot is down
    /// (0031 §4.4), bounded by `RuntimeConfig::send_timeout`.
    pub(crate) async fn pick(&self) -> Result<(ConnHandle, Arc<str>), Error> {
        let wait = self.wait_live();
        match self.runtime.config.send_timeout {
            None => wait.await,
            Some(limit) => match self.runtime.exec.within(limit, wait).await {
                Some(picked) => picked,
                // The cause the waiter would have seen, not a new outcome.
                None => Err(match self.try_pick() {
                    Ok(Some(picked)) => return Ok(picked),
                    Ok(None) => Error::ConnectionLost(self.last_cause()),
                    Err(e) => e,
                }),
            },
        }
    }

    /// Waits until a slot is live, with no bound.
    async fn wait_live(&self) -> Result<(ConnHandle, Arc<str>), Error> {
        loop {
            // Registered before the check, so a change between the check and
            // the await is not missed.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_pick()? {
                Some(picked) => return Ok(picked),
                None => notified.await,
            }
        }
    }

    /// The cause of the most recently examined down or gone slot.
    fn last_cause(&self) -> LossCause {
        let slots = self.slots.lock().expect("peer list poisoned");
        slots
            .iter()
            .rev()
            .find_map(|slot| match &slot.state {
                SlotState::Down(c) | SlotState::Gone(c) => Some(*c),
                SlotState::Live(_) => None,
            })
            .unwrap_or(LossCause::LocallyClosed)
    }

    /// Number of slots whose connection is open.
    fn live_count(&self) -> usize {
        self.slots
            .lock()
            .expect("peer list poisoned")
            .iter()
            .filter(
                |slot| matches!(&slot.state, SlotState::Live(c) if c.conn.close_reason().is_none()),
            )
            .count()
    }

    /// Every live connection, collected.
    ///
    /// The slot list is a `std` mutex and must not be held across an
    /// `await`, so a caller that writes to each peer collects first and
    /// writes after.
    fn live_peers(&self) -> Vec<(ConnHandle, Arc<str>)> {
        self.slots
            .lock()
            .expect("peer list poisoned")
            .iter()
            .filter_map(|slot| match &slot.state {
                SlotState::Live(conn) if conn.conn.close_reason().is_none() => {
                    Some((ConnHandle::clone(conn), Arc::clone(&slot.path)))
                }
                _ => None,
            })
            .collect()
    }

    /// One record per live slot, in dial order
    /// ([decisions/0036](../../../docs/decisions/0036-connection-statistics.md)
    /// §4.2). The slot list is collected under its mutex and `quinn`'s
    /// counters are read after it is released.
    fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        let live: Vec<(Arc<str>, u64, ConnHandle)> = self
            .slots
            .lock()
            .expect("peer list poisoned")
            .iter()
            .filter_map(|slot| match &slot.state {
                SlotState::Live(conn) if conn.conn.close_reason().is_none() => {
                    Some((Arc::clone(&slot.url), slot.redials, ConnHandle::clone(conn)))
                }
                _ => None,
            })
            .collect();
        live.into_iter()
            .map(|(url, redials, conn)| {
                let now = conn.exec.now();
                crate::ConnectionStats {
                    url,
                    age: now.saturating_duration_since(conn.established),
                    redials,
                    transport: conn.conn.transport_stats(),
                    remote: conn.remote.latest(now),
                }
            })
            .collect()
    }

    /// Every live connection with the URL it was dialled as: what a pattern
    /// that tracks each peer by its address needs, the same filter as
    /// [`PeerShared::live_peers`].
    #[cfg(feature = "swarm")]
    pub(crate) fn live_slots(&self) -> Vec<(Arc<str>, ConnHandle, Arc<str>)> {
        self.slots
            .lock()
            .expect("peer list poisoned")
            .iter()
            .filter_map(|slot| match &slot.state {
                SlotState::Live(conn) if conn.conn.close_reason().is_none() => Some((
                    Arc::clone(&slot.url),
                    ConnHandle::clone(conn),
                    Arc::clone(&slot.path),
                )),
                _ => None,
            })
            .collect()
    }

    /// Whether some address `skip` does not name is live or being
    /// redialled: whether waiting can still bring a connection.
    #[cfg(feature = "swarm")]
    pub(crate) fn has_slot_besides(&self, skip: impl Fn(&str) -> bool) -> bool {
        self.slots
            .lock()
            .expect("peer list poisoned")
            .iter()
            .any(|slot| !skip(&slot.url) && !matches!(slot.state, SlotState::Gone(_)))
    }

    fn set_state(&self, id: u64, state: SlotState) {
        let mut slots = self.slots.lock().expect("peer list poisoned");
        if let Some(slot) = slots.iter_mut().find(|slot| slot.id == id) {
            slot.state = state;
        }
        drop(slots);
        self.changed.notify_waiters();
    }

    /// Makes slot `id` live on the connection a redial produced, and counts
    /// the redial.
    fn redialled(&self, id: u64, conn: ConnHandle) {
        let mut slots = self.slots.lock().expect("peer list poisoned");
        if let Some(slot) = slots.iter_mut().find(|slot| slot.id == id) {
            slot.state = SlotState::Live(conn);
            slot.redials += 1;
        }
        drop(slots);
        self.changed.notify_waiters();
    }

    fn emit(&self, event: PeerEvent) {
        // No reader is not an error: an endpoint nobody watches pays nothing.
        let _ = self.events.send(event);
    }

    /// Dials `url` and runs the pattern's attachment on the new connection.
    ///
    /// `proved` is what the slot's first connection proved: a redial over
    /// QUIC pins that key into the address, so a replacement server with a
    /// different key is refused **in the handshake**, before any connection
    /// exists for anyone to use (0031 §4.7). A local peer proves a principal
    /// or nothing, and neither is pinned: the address names a socket path
    /// or a bus, and whoever binds it is the peer.
    async fn dial_address(
        &self,
        address: &Address,
        proved: Option<&PeerIdentity>,
    ) -> Result<(ConnHandle, Arc<str>), Error> {
        let (conn, path) = match address {
            Address::Quic(addr) => {
                // The pool keys on the dialled path as well as the
                // authority, and checks the fingerprint it gets back.
                let path = addr.path.as_str();
                let pinned;
                let addr = match proved {
                    Some(PeerIdentity::Key(fp)) if addr.peer.is_none() => {
                        pinned = weida_core::EndpointAddr {
                            peer: Some(*fp),
                            ..addr.clone()
                        };
                        &pinned
                    }
                    _ => addr,
                };
                let conn = self.runtime.connect(addr, &self.tls, &self.limits).await?;
                (conn, path)
            }
            Address::Inproc(addr) => {
                let conn = self.runtime.connect_local(&addr.bus).await?;
                (conn, addr.path.as_str())
            }
            #[cfg(unix)]
            Address::Unix(addr) => {
                let conn = self.runtime.connect_unix(&addr.socket).await?;
                (conn, addr.path.as_str())
            }
            #[cfg(not(unix))]
            Address::Unix(addr) => {
                // The address parses everywhere — it is data — but the
                // transport exists only where the kernel offers `AF_UNIX`.
                // Refused by name rather than falling through to another
                // transport [0010 §4.6].
                return Err(Error::InvalidAddress(format!(
                    "{}: AF_UNIX is not available on this platform",
                    addr.socket
                )));
            }
            #[cfg(windows)]
            Address::Pipe(addr) => {
                let conn = self.runtime.connect_pipe(addr).await?;
                (conn, addr.path.as_str())
            }
            #[cfg(not(windows))]
            Address::Pipe(addr) => {
                return Err(Error::InvalidAddress(format!(
                    "{}: named pipes are not available on this platform",
                    addr.name
                )));
            }
        };
        if let Some(attach) = &self.attach {
            attach.attach(&conn, path).await?;
        }
        Ok((conn, Arc::from(path)))
    }
}

/// The dialling side of the stream core.
///
/// A `Peer` owns a set of dialled addresses and the trust anchors they are
/// authenticated against, and opens streams on their connections. Every
/// dialling pattern — Req, Push, Sub — is this plus a selection policy and a
/// bit of vocabulary.
///
/// An address, once dialled, is kept: when its connection is lost the
/// runtime redials it under [`ReconnectPolicy`], and the transitions are
/// reported on [`Peer::events`]
/// ([0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)).
pub struct Peer {
    shared: Arc<PeerShared>,
    /// Dropped with the peer; every task of this peer watches it.
    alive: watch::Sender<()>,
}

impl Peer {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> Peer {
        Peer::with_attach(runtime, tls, None)
    }

    pub(crate) fn with_attach(
        runtime: Arc<RuntimeInner>,
        tls: Arc<ClientTls>,
        attach: Option<Arc<dyn Attach>>,
    ) -> Peer {
        let limits = runtime.config.limits;
        Peer::build(runtime, tls, attach, limits)
    }

    /// A peer whose QUIC connections are dialled with `limits` instead of
    /// the runtime's: a swarm's, whose receive windows carry a seeder's
    /// pipelined answers (quinn fixes them per connection at dial).
    #[cfg(feature = "swarm")]
    pub(crate) fn with_limits(
        runtime: Arc<RuntimeInner>,
        tls: Arc<ClientTls>,
        limits: Limits,
    ) -> Peer {
        Peer::build(runtime, tls, None, limits)
    }

    fn build(
        runtime: Arc<RuntimeInner>,
        tls: Arc<ClientTls>,
        attach: Option<Arc<dyn Attach>>,
        limits: Limits,
    ) -> Peer {
        let (events, _) = broadcast::channel(EVENT_QUEUE);
        let (alive, _) = watch::channel(());
        Peer {
            shared: Arc::new(PeerShared {
                runtime,
                tls,
                limits,
                slots: std::sync::Mutex::new(Vec::new()),
                cursor: AtomicUsize::new(0),
                next_slot: AtomicU64::new(0),
                changed: Notify::new(),
                events,
                attach,
                outbox: Outbox {
                    queue: std::sync::Mutex::new(VecDeque::new()),
                    bytes: AtomicUsize::new(0),
                    room: Notify::new(),
                    filled: Notify::new(),
                    draining: AtomicBool::new(false),
                    dropped: AtomicU64::new(0),
                },
                segment_topics: SegmentTopics::new(limits.max_sequence_scopes),
                segment_drops: Arc::new(DropTable::new(limits.max_sequence_scopes)),
                // tokio panics above `Semaphore::MAX_PERMITS`.
                segment_budget: Arc::new(Semaphore::new(
                    limits.subscriber_buffer_bytes.min(Semaphore::MAX_PERMITS),
                )),
            }),
            alive,
        }
    }

    /// Connects to `weida://[fingerprint@]host:port/path`.
    ///
    /// Connections are pooled per `host:port`, trust configuration and the
    /// fingerprint the address names, so several endpoints addressing the
    /// same peer on the same terms share one QUIC connection while two
    /// different sets of terms never do. Returns once the HELLO exchange has
    /// been negotiated, so the first stream cannot race negotiation.
    ///
    /// A peer that answers with an identity the terms do not cover fails with
    /// [`Error::Untrusted`] carrying the fingerprint it presented.
    ///
    /// The address is kept after this returns: a lost connection is redialled
    /// by the runtime under [`crate::RuntimeConfig::reconnect`], and nothing
    /// here needs calling again.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.dial(url).await.map(|_| ())
    }

    /// Forgets `url`: the slot is removed and its redial task ends.
    ///
    /// The connection itself is pooled and may be serving another endpoint,
    /// so it is not closed here; an idle one goes when the pool reaps it.
    /// What this endpoint holds on it is released: a subscriber's route and
    /// subscriptions, so the peer stops sending and the address can be
    /// dialled again. Returns whether the address was known.
    pub fn disconnect(&self, url: &str) -> bool {
        let mut slots = self.shared.slots.lock().expect("peer list poisoned");
        let before = slots.len();
        let mut released = Vec::new();
        slots.retain(|slot| {
            let keep = &*slot.url != url;
            if !keep {
                slot.stop.notify_one();
                if let SlotState::Live(conn) = &slot.state {
                    released.push((ConnHandle::clone(conn), Arc::clone(&slot.path)));
                }
            }
            keep
        });
        let removed = slots.len() != before;
        drop(slots);
        if let Some(attach) = &self.shared.attach {
            for (conn, path) in &released {
                attach.detach(conn, path);
            }
        }
        if removed {
            self.shared.changed.notify_waiters();
        }
        removed
    }

    /// Undoes the pattern's attachment on every live connection, for the
    /// endpoint's drop.
    pub(crate) fn detach_live(&self) {
        if let Some(attach) = &self.shared.attach {
            for (conn, path) in self.shared.live_peers() {
                attach.detach(&conn, &path);
            }
        }
    }

    /// Number of peers whose connection is open.
    ///
    /// A peer that went away no longer counts while it is being redialled,
    /// and counts again once the redial succeeds.
    pub fn peer_count(&self) -> usize {
        self.shared.live_count()
    }

    /// Number of addresses this peer holds, live or not.
    pub(crate) fn slot_count(&self) -> usize {
        self.shared.slots.lock().expect("peer list poisoned").len()
    }

    /// One record per live connection of this peer, labelled by the URL as
    /// it was dialled; empty when none is live
    /// ([decisions/0036](../../../docs/decisions/0036-connection-statistics.md)).
    ///
    /// Synchronous and cheap: it reads counters `quinn` already keeps. A
    /// connection the pool shares with another endpoint that dialled the same
    /// URL on the same terms is reported by both, with the same transport
    /// numbers; `url` and `redials` are this endpoint's own.
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.shared.connection_stats()
    }

    /// The event stream of this peer's addresses.
    pub fn events(&self) -> PeerEvents {
        PeerEvents::new(self.shared.events.subscribe())
    }

    /// Messages the outbox discarded: at the bound under a `Drop`
    /// backpressure policy, or refused by the peer once written.
    pub fn dropped(&self) -> u64 {
        self.shared.outbox.dropped.load(Ordering::Relaxed)
    }

    /// Opens a unidirectional stream to the next peer, round-robin.
    ///
    /// With every peer down, waits for the redial — bounded by
    /// [`crate::RuntimeConfig::send_timeout`] — rather than failing; a peer
    /// that was never connected fails at once with [`Error::NotConnected`].
    ///
    /// The DATA header is written before this returns, so the caller gets a
    /// handle that is already addressed and can only carry payload.
    ///
    /// Under a negotiated `PerProducer` ordering the header also carries this
    /// producer's next sequence number for the dialled path (DATA key `6`);
    /// under `core` it carries none and the sequencer is never touched.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        let (conn, path) = self.shared.pick().await?;
        open_transfer_on(&conn, &path, &meta).await
    }

    /// Registers a datagram flow to the next peer, round-robin
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.2).
    ///
    /// Writes the FLOW header and returns without waiting for an answer: a
    /// refusal arrives later as the error of [`Flow::send`](crate::Flow::send).
    /// Fails with [`Error::DatagramsUnavailable`] where the connection did
    /// not agree capability `1` — both runtimes need
    /// `Limits::datagram_receive_bytes` above zero — and never falls back to
    /// a stream, which would deliver the units late.
    pub async fn open_flow(&self, meta: crate::FlowMeta) -> Result<crate::Flow, Error> {
        crate::flow::open_flow_via(&self.shared, meta).await
    }

    /// Opens segment *n+1* on `topic` as one stream to the next peer,
    /// round-robin, and supersedes this peer's copy of segment *n* there as
    /// a radio does
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.2): an unfinished copy at once, a finished one after the time its
    /// path needs.
    ///
    /// The bound side receives [`crate::Incoming::Stream`] with
    /// [`crate::IncomingMeta::topic`] and [`crate::IncomingMeta::segment`]
    /// set. The number is taken per `(path, topic)` from the connection, so
    /// every `Peer` sharing it sends one rising sequence (§4.11).
    /// `terms.max_age` resets the copy once it is that old, unfinished or
    /// unacknowledged; `terms.priority` orders it within its layer;
    /// `terms.follows_upstream` keeps it against its successor unless a
    /// write has to wait. [`crate::Segment::write`] never waits: a copy
    /// without room for a chunk — `Limits::subscriber_buffer_bytes` over all
    /// of this peer's segments — is lost, counted in
    /// [`Peer::segment_drops`].
    ///
    /// Waits for a peer exactly as [`Peer::open`] does. Fails with
    /// [`Error::LimitExceeded`] for a topic above 256 bytes, or when
    /// `max_sequence_scopes` topics all have a copy in flight.
    pub async fn segment(&self, topic: &str, terms: SegmentTerms) -> Result<crate::Segment, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let (conn, path) = self.shared.pick().await?;
        let shared = &self.shared;
        let topic: Arc<str> = Arc::from(topic);
        shared.segment_topics.supersede(&topic)?;
        let number = conn.segments_out.next(&path, &topic);
        let target = CopyTarget {
            conn,
            path,
            budget: Arc::clone(&shared.segment_budget),
            max_age: None,
            max_layer: None,
            dish: None,
        };
        Ok(open_segment(
            &shared.segment_topics,
            &shared.segment_drops,
            topic,
            number,
            &terms,
            vec![target],
        ))
    }

    /// What [`Peer::segment`] lost on `topic`, by cause, or `None` if
    /// nothing was. The table holds at most `Limits::max_sequence_scopes`
    /// topics.
    pub fn segment_drops(&self, topic: &str) -> Option<TopicDrops> {
        self.shared.segment_drops.on_topic(topic)
    }

    /// Opens a bidirectional stream — an exchange — to the next peer.
    ///
    /// The returned [`ReplyStream`] is the return half. It needs no correlation
    /// identifier: it *is* the correlation, and dropping it cancels the reply.
    /// Waits for a peer exactly as [`Peer::open`] does.
    pub async fn open_bi(
        &self,
        meta: TransferMeta,
    ) -> Result<(OutgoingTransfer, ReplyStream), Error> {
        let (conn, path) = self.shared.pick().await?;
        let (header, trace, cursors) = outgoing_header(&conn, Some(&path), &meta, None)?;
        let head = DataHead::new(&conn, &header)?;
        let (mut send, recv) = conn.open_bi().await?;
        // The peer learns of the stream with this write, so it never sees a
        // bidirectional stream it cannot classify.
        head.write(&mut send).await?;
        Ok((
            OutgoingTransfer::new(send, trace, Arc::clone(&conn), cursors),
            ReplyStream::new(recv, conn),
        ))
    }

    /// Sends `body` as one whole one-way transfer, and owns it from here.
    ///
    /// With a live peer and an empty outbox this is [`Peer::open`], a write
    /// and a FIN, with no copy. Otherwise the body is copied into the
    /// outbox and written, in order, once a peer is live — the runtime's
    /// responsibility from this call until the write, and not after it: a
    /// body written to a connection that then dies is gone, as in ZeroMQ
    /// (0031 §4.2, §4.3). A full outbox applies the configured backpressure:
    /// `Block` waits for room, `Drop` discards and counts, `Reject` fails
    /// with [`Error::LimitExceeded`]. A body larger than the outbox is
    /// refused with [`Error::LimitExceeded`] rather than blocking forever,
    /// and once every address is given up `send` fails with
    /// [`Error::ConnectionLost`] whether or not the outbox still holds
    /// bodies: nothing would ever write them.
    pub async fn send(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        let shared = &self.shared;
        if shared.outbox_is_empty()
            && let Some((conn, path)) = shared.try_pick()?
        {
            match open_transfer_on(&conn, &path, &meta).await {
                Ok(mut transfer) => {
                    transfer.write_all(body).await?;
                    transfer.finish()?;
                    return Ok(());
                }
                // Nothing was written: the connection died under the open.
                // The body is still whole and still ours.
                Err(Error::ConnectionLost(_)) => {}
                Err(e) => return Err(e),
            }
        }
        shared.enqueue(meta, body).await?;
        self.ensure_draining();
        Ok(())
    }

    /// Starts the outbox drain task unless it is running.
    fn ensure_draining(&self) {
        if self.shared.outbox.draining.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak = Arc::downgrade(&self.shared);
        let alive = self.alive.subscribe();
        self.shared.runtime.exec.spawn(drain_outbox(weak, alive));
    }

    /// The runtime this peer's tasks run on.
    pub(crate) fn exec(&self) -> &crate::runtime::Exec {
        &self.shared.runtime.exec
    }

    /// Dials and records a peer, returning the connection and the path.
    ///
    /// The scheme picks the transport and nothing falls back to anything
    /// else: `weida://` dials QUIC on `tls`'s terms, `weida+inproc://`
    /// reaches a bus in this process with no TLS at all
    /// ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.6, §4.8). The trust configuration is simply unused on a local
    /// address — there is no key to check.
    ///
    /// The first dial reports its failure to the caller: an address that is
    /// wrong, refused or untrusted is not something a redial fixes. Once it
    /// has succeeded, the address is a slot the runtime keeps live.
    pub(crate) async fn dial(&self, url: &str) -> Result<(ConnHandle, Arc<str>), Error> {
        let address = Address::parse(url)?;
        let (conn, path) = self.shared.dial_address(&address, None).await?;
        let url: Arc<str> = Arc::from(url);
        let id = self.shared.next_slot.fetch_add(1, Ordering::Relaxed);
        let stop = Arc::new(Notify::new());
        {
            let mut slots = self.shared.slots.lock().expect("peer list poisoned");
            slots.push(Slot {
                id,
                url: Arc::clone(&url),
                path: Arc::clone(&path),
                state: SlotState::Live(ConnHandle::clone(&conn)),
                redials: 0,
                stop: Arc::clone(&stop),
            });
        }
        self.shared.changed.notify_waiters();
        self.shared.emit(PeerEvent::Connected {
            url: Arc::clone(&url),
            peer: conn.peer.clone(),
        });
        self.shared.runtime.exec.spawn(watch_slot(
            Arc::downgrade(&self.shared),
            self.alive.subscribe(),
            stop,
            id,
            url,
            address,
            ConnHandle::clone(&conn),
        ));
        Ok((conn, path))
    }

    /// Every live peer, for a caller that has an `await` between the lookup
    /// and the write.
    pub(crate) fn live_peers(&self) -> Vec<(ConnHandle, Arc<str>)> {
        self.shared.live_peers()
    }

    /// A handle on this peer's state that does not keep it alive, for a
    /// value that outlives a borrow of the endpoint but must not outlive the
    /// endpoint.
    #[cfg(feature = "swarm")]
    pub(crate) fn downgrade(&self) -> Weak<PeerShared> {
        Arc::downgrade(&self.shared)
    }
}

/// Maps the error a closed connection reports onto the loss vocabulary.
///
/// A deliberate close with a code this side maps to a protocol outcome —
/// negotiation, violation, limit — is still the peer closing: whether to
/// redial after it is the policy's `stop_on_peer_closed`.
fn loss_cause(reason: &Error) -> LossCause {
    match reason {
        Error::ConnectionLost(cause) => *cause,
        Error::Negotiation(_) | Error::Protocol(_) | Error::LimitExceeded => LossCause::PeerClosed,
        _ => LossCause::TransportError,
    }
}

/// One slot's life after its first connection: notices the loss, redials
/// under the policy, and reports every step (0031 §4.1, §4.5, §4.8).
async fn watch_slot(
    weak: Weak<PeerShared>,
    mut alive: watch::Receiver<()>,
    stop: Arc<Notify>,
    id: u64,
    url: Arc<str>,
    address: Address,
    mut conn: ConnHandle,
) {
    let (policy, exec): (ReconnectPolicy, crate::runtime::Exec) = match weak.upgrade() {
        Some(shared) => (
            shared.runtime.config.reconnect.clone(),
            shared.runtime.exec.clone(),
        ),
        None => return,
    };
    let proved = conn.peer.clone();
    // Redial attempts since the last connection that stayed up, carried across
    // losses; see below for what "stayed up" means.
    let mut attempt = 0u32;
    let mut up_since = exec.now();
    loop {
        let reason = tokio::select! {
            reason = conn.conn.closed() => reason,
            _ = alive.changed() => return,
            _ = stop.notified() => return,
        };
        let Some(shared) = weak.upgrade() else { return };
        let cause = loss_cause(&reason);
        shared.set_state(id, SlotState::Down(cause));
        shared.emit(PeerEvent::Lost {
            url: Arc::clone(&url),
            cause,
        });
        // This side closed it: the application already decided, and a
        // runtime that is shutting down must not dial anything.
        let stopped = !policy.redials_after(cause);
        if stopped {
            give_up(&shared, id, cause, &url, GiveUp::Policy { attempts: 0 });
            return;
        }
        drop(shared);

        // A connection that lived at least as long as the longest wait the
        // policy ever imposes (`max`) earned a fresh schedule. One that did
        // not is a success in name only — a server that accepts, says HELLO
        // and closes, or a crash loop — and the schedule goes on from where
        // it stopped, so such a peer sees redials slow down instead of a
        // fresh handshake every `initial` for as long as it keeps doing it.
        if exec.now().saturating_duration_since(up_since) >= policy.max {
            attempt = 0;
        }
        conn = loop {
            attempt += 1;
            let Some(shared) = weak.upgrade() else { return };
            if !policy.allows(attempt) {
                give_up(
                    &shared,
                    id,
                    cause,
                    &url,
                    GiveUp::Policy {
                        attempts: attempt - 1,
                    },
                );
                return;
            }
            let delay = policy.delay_after(cause, attempt);
            shared.emit(PeerEvent::Retrying {
                url: Arc::clone(&url),
                attempt,
                delay,
            });
            let sleep = shared.runtime.exec.sleep(delay);
            drop(shared);
            // In process a bus is back exactly when its name is bound again,
            // so the wait is on the registry rather than on the clock
            // (0031 §4.10). Everything else has a network to wait out.
            let waited = match &address {
                Address::Inproc(addr) => tokio::select! {
                    _ = crate::inproc::wait_bound(&addr.bus) => true,
                    _ = alive.changed() => false,
                    _ = stop.notified() => false,
                },
                _ => tokio::select! {
                    _ = sleep => true,
                    _ = alive.changed() => false,
                    _ = stop.notified() => false,
                },
            };
            if !waited {
                return;
            }
            let Some(shared) = weak.upgrade() else { return };
            match shared.dial_address(&address, proved.as_ref()).await {
                Ok((conn, _)) => break conn,
                // The pinned key was not the one that answered: a different
                // server behind the address. Not a reconnect (0031 §4.7).
                Err(Error::Untrusted(presented)) => {
                    give_up(
                        &shared,
                        id,
                        cause,
                        &url,
                        GiveUp::PeerChanged {
                            presented: Some(presented),
                        },
                    );
                    return;
                }
                Err(Error::Tls(m)) if proved.is_some() => {
                    give_up(
                        &shared,
                        id,
                        cause,
                        &url,
                        GiveUp::PeerChanged { presented: None },
                    );
                    tracing::debug!(%url, reason = %m, "redial reached a peer that proved nothing");
                    return;
                }
                Err(
                    e @ (Error::Tls(_)
                    | Error::Negotiation(_)
                    | Error::AlreadyRegistered
                    | Error::Unsupported
                    | Error::DatagramsUnavailable
                    | Error::TooLarge { .. }
                    | Error::InvalidAddress(_)),
                ) => {
                    give_up(&shared, id, cause, &url, GiveUp::Failed(e.to_string()));
                    return;
                }
                Err(e) => {
                    tracing::debug!(%url, attempt, error = %e, "redial failed");
                }
            }
        };
        up_since = exec.now();
        let Some(shared) = weak.upgrade() else { return };
        shared.redialled(id, ConnHandle::clone(&conn));
        shared.emit(PeerEvent::Connected {
            url: Arc::clone(&url),
            peer: conn.peer.clone(),
        });
        // Whatever waited for a peer — the outbox drain among them.
        shared.outbox.filled.notify_waiters();
    }
}

fn give_up(shared: &PeerShared, id: u64, cause: LossCause, url: &Arc<str>, why: GiveUp) {
    shared.set_state(id, SlotState::Gone(cause));
    shared.emit(PeerEvent::GaveUp {
        url: Arc::clone(url),
        why,
    });
}

impl PeerShared {
    /// The runtime's executor, for work a handle hands off.
    pub(crate) fn exec(&self) -> &crate::runtime::Exec {
        &self.runtime.exec
    }

    fn outbox_is_empty(&self) -> bool {
        self.outbox
            .queue
            .lock()
            .expect("outbox poisoned")
            .is_empty()
    }

    /// Copies `body` into the outbox, applying the bound and the configured
    /// backpressure behaviour at it.
    ///
    /// An endpoint whose every address was given up has nothing that will
    /// ever drain the outbox, so the body is refused with the loss, as a
    /// `send` on an empty outbox would be, and a sender waiting at the
    /// bound is woken with it rather than left waiting forever.
    async fn enqueue(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        let config = &self.runtime.config;
        if body.len() > config.outbox_bytes {
            return Err(Error::LimitExceeded);
        }
        loop {
            // Both registered before the checks, so neither a pop nor a
            // give-up between a check and the await is missed.
            let room = self.outbox.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.try_pick()?;
            {
                let mut queue = self.outbox.queue.lock().expect("outbox poisoned");
                let bytes = self.outbox.bytes.load(Ordering::Relaxed);
                if queue.len() < config.outbox_messages && bytes + body.len() <= config.outbox_bytes
                {
                    queue.push_back(Queued {
                        meta,
                        body: Bytes::copy_from_slice(body),
                    });
                    self.outbox.bytes.fetch_add(body.len(), Ordering::Relaxed);
                    drop(queue);
                    self.outbox.filled.notify_waiters();
                    return Ok(());
                }
            }
            match config.outbox_full {
                OutboxFull::Drop => {
                    self.outbox.dropped.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                OutboxFull::Reject => return Err(Error::LimitExceeded),
                OutboxFull::Block => tokio::select! {
                    () = room => {}
                    () = changed => {}
                },
            }
        }
    }

    /// The oldest queued body, cloned out for the write.
    fn peek_outbox(&self) -> Option<(TransferMeta, Bytes)> {
        self.outbox
            .queue
            .lock()
            .expect("outbox poisoned")
            .front()
            .map(|q| (q.meta.clone(), q.body.clone()))
    }

    fn pop_outbox(&self) {
        let mut queue = self.outbox.queue.lock().expect("outbox poisoned");
        if let Some(queued) = queue.pop_front() {
            self.outbox
                .bytes
                .fetch_sub(queued.body.len(), Ordering::Relaxed);
        }
        drop(queue);
        self.outbox.room.notify_waiters();
    }
}

/// Writes the outbox, in order, onto whichever peer is live.
///
/// A body the connection refuses, or that a connection dies under once it
/// has started, is dropped and counted: the runtime's responsibility ended
/// at the write (0031 §4.3). A body no connection would take yet stays at
/// the front until one is live.
async fn drain_outbox(weak: Weak<PeerShared>, mut alive: watch::Receiver<()>) {
    loop {
        let Some(shared) = weak.upgrade() else { return };
        let Some((meta, body)) = shared.peek_outbox() else {
            // Empty. Hand the flag back, then look once more: a sender that
            // enqueued between the peek and the flag would otherwise wait
            // for a task that has left.
            shared.outbox.draining.store(false, Ordering::Release);
            if shared.outbox_is_empty() || shared.outbox.draining.swap(true, Ordering::AcqRel) {
                return;
            }
            continue;
        };
        let filled = shared.outbox.filled.notified();
        tokio::pin!(filled);
        filled.as_mut().enable();
        let picked = match shared.try_pick() {
            Ok(Some(picked)) => picked,
            // Down and being redialled, or every address gone: wait for the
            // next change. A new `connect` wakes this too.
            Ok(None) | Err(_) => {
                // The strong reference is held across the wait; a dropped
                // endpoint wakes `alive` and the task leaves at once.
                let changed = shared.changed.notified();
                tokio::select! {
                    _ = filled => {}
                    _ = changed => {}
                    _ = alive.changed() => return,
                }
                continue;
            }
        };
        let (conn, path) = picked;
        let mut transfer = match open_transfer_on(&conn, &path, &meta).await {
            Ok(transfer) => transfer,
            // Nothing was opened, so nothing was handed over: the body stays
            // at the front and the next pick finds a different, or a
            // redialled, connection.
            Err(Error::ConnectionLost(_)) => continue,
            Err(e) => {
                tracing::debug!(error = %e, "outbox body refused at the open");
                shared.outbox.dropped.fetch_add(1, Ordering::Relaxed);
                shared.pop_outbox();
                continue;
            }
        };
        let written = async {
            transfer.write_all(&body).await?;
            transfer.finish()
        }
        .await;
        // Written means handed to a connection, and the responsibility ended
        // there: a body the connection died under, or the peer refused, is
        // gone and counted, never resent (0031 §4.3).
        if let Err(e) = written {
            tracing::debug!(error = %e, "outbox body discarded");
            shared.outbox.dropped.fetch_add(1, Ordering::Relaxed);
        }
        shared.pop_outbox();
    }
}

/// Opens one exchange on a **named** connection.
///
/// [`Peer::open_bi`] picks one peer, which is the selection policy Req/Rep
/// wants. A pattern that addresses *every* peer — a survey — needs the same
/// write against a connection it chose itself, so the body lives here once
/// rather than twice.
pub(crate) async fn open_exchange_on(
    conn: &ConnHandle,
    path: &str,
    meta: TransferMeta,
) -> Result<(OutgoingTransfer, ReplyStream), Error> {
    let (header, trace, cursors) = outgoing_header(conn, Some(path), &meta, None)?;
    let head = DataHead::new(conn, &header)?;
    let (mut send, recv) = conn.open_bi().await?;
    head.write(&mut send).await?;
    Ok((
        OutgoingTransfer::new(send, trace, ConnHandle::clone(conn), cursors),
        ReplyStream::new(recv, ConnHandle::clone(conn)),
    ))
}

/// Opens one **one-way** transfer on a named connection.
///
/// The counterpart of [`open_exchange_on`], and for the same reason: three
/// patterns write this body — [`Peer::open`] for a dialling side, the bound
/// half of a pair, and each copy a bus member sends — and when each kept its
/// own copy they drifted. Only one of them assigned the producer sequence,
/// so a connection that negotiated `PerProducer` carried numbered transfers
/// in one direction of a pair and unnumbered ones in the other.
pub(crate) async fn open_transfer_on(
    conn: &ConnHandle,
    path: &str,
    meta: &TransferMeta,
) -> Result<OutgoingTransfer, Error> {
    let (mut header, trace, cursors) = outgoing_header(conn, Some(path), meta, None)?;
    // Under `core` ordering the sequencer is never touched and the key is
    // omitted; under `PerProducer` this is the number the peer's gap
    // detector and reassembler read. It is on loan until the transfer starts:
    // a header the peer would refuse, or an open that fails or is given up
    // on, sends nothing, and a number nobody carries would read as a lost
    // transfer.
    let number = conn.sequencer.issue(path);
    header.sequence = number.number();
    let head = DataHead::new(conn, &header)?;
    let mut stream = conn.open_uni().await?;
    number.spend();
    head.write(&mut stream).await?;
    Ok(OutgoingTransfer::new(
        stream,
        trace,
        ConnHandle::clone(conn),
        cursors,
    ))
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("peer_count", &self.peer_count())
            .finish_non_exhaustive()
    }
}

/// One inbound thing on a raw acceptor's path, whichever kind the peer sent.
///
/// The two stream kinds are what an [`Acceptor`] was built for. The three
/// subscription events exist because an L2 queue is served on such a path
/// (`weida-broker`): a consumer registers with SUBSCRIBE like a subscriber,
/// grants credit with the frame of
/// [0003](https://github.com/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
/// §4.2, and its subscription ends with UNSUBSCRIBE or with its connection.
/// They arrive **in the order the peer caused them** per connection, which is
/// what lets a queue apply a grant to a subscription it has already seen.
#[derive(Debug)]
pub enum Incoming {
    /// A unidirectional stream: payload only, no reply half.
    Stream(IncomingTransfer),
    /// A bidirectional stream: an exchange that owes a reply or an ERROR.
    Exchange(IncomingRequest),
    /// A consumer subscribed to this path with one filter.
    ///
    /// One event per filter, so a consumer holding several filters arrives
    /// several times with the same [`Consumer::id`]. A repeated SUBSCRIBE of
    /// a filter the connection already holds is not announced again.
    Subscribed(Consumer),
    /// A consumer stated the absolute delivery limit of one subscription.
    Credit(CreditGrant),
    /// A subscription ended: UNSUBSCRIBE, or the connection went away.
    ///
    /// An UNSUBSCRIBE names a filter the connection really held; one that
    /// matches nothing is ignored. A closed connection reports
    /// `filter: None` — it ends every subscription that connection still
    /// held, on every path it consumed on, and the peer is gone, so there is
    /// nothing to enumerate them against.
    Unsubscribed {
        /// Which consumer.
        id: ConsumerId,
        /// Which filter, or `None` for the whole connection.
        filter: Option<String>,
    },
    /// A datagram flow: one registration, then datagrams until it ends
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.2). Dropping it stops the flow with `CANCELED`.
    Flow(crate::IncomingFlow),
}

/// Identifies one consuming connection on one path.
///
/// A number this process gave the connection when it was made, never reused
/// for another one while the process lives, so an id a queue still holds
/// after its connection is gone — a credit with no subscription behind it,
/// say — can never name a later connection. It says nothing about *who* the
/// peer is — that is `IncomingMeta::peer`, proved in the handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerId(pub(crate) usize);

impl ConsumerId {
    pub(crate) fn from_conn(conn_id: usize) -> ConsumerId {
        ConsumerId(conn_id)
    }
}

/// A consumer's absolute delivery limit for one subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreditGrant {
    /// Which consumer granted it.
    pub id: ConsumerId,
    /// Which subscription: the filter it was granted for.
    pub filter: String,
    /// Messages that subscription will accept in total, counted from its
    /// creation.
    ///
    /// Absolute and cumulative, so a duplicate changes nothing. A receiver
    /// keeps the **highest** limit it has seen for the subscription, which is
    /// what makes a reordered grant harmless: control frames ride their own
    /// unidirectional streams and QUIC does not order those relative to each
    /// other.
    pub limit: u64,
}

/// One subscribed consumer, and the way to deliver to it.
///
/// Delivery is a one-way transfer on a fresh unidirectional stream, addressed
/// to the path the consumer subscribed on — the same shape a publisher's
/// fan-out copy has, because a consumer *is* an ordinary subscriber on the
/// wire (0018 §4.5). What differs is the selection: a queue picks one
/// consumer, a publisher writes to all of them.
#[derive(Clone)]
pub struct Consumer {
    conn: ConnHandle,
    path: Arc<str>,
    filter: String,
}

impl Consumer {
    pub(crate) fn new(conn: ConnHandle, path: Arc<str>, filter: String) -> Consumer {
        Consumer { conn, path, filter }
    }

    /// Which consuming connection this is.
    pub fn id(&self) -> ConsumerId {
        ConsumerId(self.conn.conn.stable_id())
    }

    /// The filter this subscription was created with.
    pub fn filter(&self) -> &str {
        &self.filter
    }

    /// The path it subscribed on.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Opens one delivery to this consumer.
    ///
    /// The transfer is addressed to the subscribed path, so it arrives at the
    /// consumer's own route for it — a [`crate::Subscriber`], which needs no
    /// new API to receive a queue's delivery. The topic a consumer filters on
    /// travels in `meta`, so a delivery carries the label the producer gave
    /// the message rather than one this hop invents.
    pub async fn open(&self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        // The one one-way open every pattern shares, so a delivery is checked
        // before a stream is paid for and numbered under `PerProducer` like
        // any other one-way transfer on its connection.
        open_transfer_on(&self.conn, &self.path, &meta).await
    }

    /// Delivers `body` as one whole transfer.
    pub async fn deliver(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        let mut transfer = self.open(meta).await?;
        transfer.write_all(body).await?;
        transfer.finish()?;
        Ok(())
    }

    /// Closes the connection this consumer subscribed on, with `code` and a
    /// human-readable `reason`.
    ///
    /// For a queue that must act on a consumer that stopped taking
    /// deliveries. Dropping the subscription alone leaves the consumer
    /// holding a connection that is open and a subscription that is gone,
    /// and nothing tells it so; a closed connection is the one signal every
    /// client already reacts to, and a [`Subscriber`](crate::Subscriber)
    /// that redials subscribes again. Every other subscription of the same
    /// connection ends with it, on every path.
    ///
    /// `code` is one of [`crate::codes`], which are what `docs/PROTOCOL.md`
    /// §7 defines for `CONNECTION_CLOSE`; a value too large for a QUIC
    /// varint is sent as [`crate::codes::INTERNAL`]. `reason` is for the
    /// peer's logs and is not interpreted. Closing an already closed
    /// connection does nothing.
    pub fn close(&self, code: u64, reason: &str) {
        /// The largest value a QUIC varint carries (RFC 9000 §16).
        const MAX_CODE: u64 = (1 << 62) - 1;
        let code = if code > MAX_CODE {
            weida_protocol::codes::INTERNAL
        } else {
            code
        };
        self.conn.conn.close(code, reason);
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("id", &self.id())
            .field("path", &self.path)
            .field("filter", &self.filter)
            .finish()
    }
}

/// The bound side of the stream core.
///
/// One path, both stream kinds, one queue. Where a [`crate::Replier`] accepts
/// exchanges and a [`crate::Puller`] receives one-way transfers, an `Acceptor`
/// takes whatever arrives and lets the application decide.
pub struct Acceptor {
    path: Arc<str>,
    /// An async mutex so `accept(&self)` needs no `&mut self`; contention is
    /// between deliberate concurrent acceptors only.
    queue: Mutex<mpsc::Receiver<Incoming>>,
}

impl Acceptor {
    pub(crate) fn new(path: &str, queue: mpsc::Receiver<Incoming>) -> Acceptor {
        Acceptor {
            path: Arc::from(path),
            queue: Mutex::new(queue),
        }
    }

    /// The endpoint path this acceptor serves.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Waits for the next inbound stream.
    ///
    /// Bounded by `Limits::endpoint_queue`, so a full queue stalls the peer
    /// through QUIC flow control instead of growing without limit.
    pub async fn accept(&self) -> Result<Incoming, Error> {
        let mut queue = self.queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }
}

impl std::fmt::Debug for Acceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acceptor")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
