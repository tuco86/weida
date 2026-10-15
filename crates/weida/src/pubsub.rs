//! Publisher-side subscription registry and fan-out.
//!
//! A publisher does not know its subscribers: they arrive as SUBSCRIBE frames
//! on whatever connections the listener has accepted. This module owns that
//! mapping and the one genuinely new selection policy of the pattern layer —
//! fan-out (`docs/ARCHITECTURE.md`, pattern primitive P3). Everything else a
//! published message needs is the ordinary one-way transfer path.
//!
//! **Delivery is per-subscriber best effort.** Each subscriber has a byte
//! budget; a message that does not fit is dropped for that subscriber, counted,
//! and the publisher moves on. A slow consumer therefore costs the publisher
//! nothing but its own messages (master doc §17: telemetry fan-out tolerates
//! drops and cannot tolerate a stalled producer).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use tokio::sync::mpsc::OwnedPermit;
use tokio::sync::{Semaphore, mpsc};
use weida_core::{Error, Limits, PeerIdentity, TraceContext};
use weida_protocol::header::OrderingMode;
use weida_protocol::{DataHeader, filter};

use crate::conn::ConnHandle;
use crate::ordering::Sequencer;
use crate::transfer::write_data_preamble;
use crate::transport::SendHalf;
use weida_protocol::codes;

/// Messages one subscriber's writer task may hold. The real bound is the byte
/// budget; this only keeps the channel itself from growing without limit when
/// messages are tiny.
const WRITER_QUEUE: usize = 1024;

/// One message queued for one subscriber.
///
/// The DATA header is *not* pre-encoded and shared: each copy carries the
/// subscriber's own path. The payload is a `Bytes`, so the fan-out shares one
/// allocation regardless of subscriber count.
struct PubMsg {
    topic: Arc<str>,
    payload: Bytes,
    /// The trace context the caller propagated, if any: weida mints none
    /// ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md)).
    trace: Option<TraceContext>,
    /// The producer's sequence number for this topic, assigned once per
    /// published message. `None` unless `PerProducer` ordering is negotiated.
    sequence: Option<u64>,
}

/// What one subscriber's writer is told to do.
///
/// A whole message is one item, which is what [`SubRegistry::publish`] sends.
/// A **streamed** publish ([`SubRegistry::open`]) is `Begin`, then a
/// `Chunk` per piece, then `Finish` — so a payload the publisher never
/// materializes still becomes one stream per subscriber, and the per-chunk
/// byte budget is what bounds this side rather than the payload's size.
enum PubItem {
    /// A message whose payload is already in memory: open, write, finish.
    Whole(PubMsg),
    /// Opens a stream for a streamed transfer and writes its DATA header,
    /// which carries no `content_len` because nobody knows it yet.
    Begin { id: u64, head: PubMsg },
    /// The next piece of the streamed transfer `id`.
    Chunk { id: u64, payload: Bytes },
    /// The end of `id`: FIN, and the receipt parked for the drain.
    Finish { id: u64 },
    /// `id` will not be completed — a chunk did not fit this subscriber's
    /// budget, or the publisher dropped the handle. The stream is reset, so
    /// the subscriber discards a partial payload instead of waiting for a FIN
    /// that will never arrive.
    Abort { id: u64 },
}

/// One subscribing connection's state for one publisher path.
struct SubEntry {
    /// `quinn::Connection::stable_id`, the identity a SUBSCRIBE arrives with.
    conn_id: usize,
    filters: HashSet<String>,
    tx: mpsc::Sender<PubItem>,
    /// Payload bytes this subscriber may hold queued. Permits are taken by
    /// `publish` and returned by the writer once the bytes are on the wire.
    budget: Arc<Semaphore>,
}

/// Why a published copy was dropped.
///
/// The first three are fan-out's: two are the subscriber not keeping up, the
/// third is the *local* transport having no connection to carry the copy
/// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
/// §4.4). The next four are RADIO's
/// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6):
/// a copy reset because its successor opened, because the `max_age`
/// passed, a datagram too large for the dish's connection, and a dish whose
/// connection carries no datagrams. The last is a segment copy that lost its
/// upper layers and kept layer 0
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DropCause {
    /// The subscriber's byte budget (`Limits::subscriber_buffer_bytes`) had
    /// no room for the payload.
    SubscriberBudget,
    /// The subscriber's writer queue was full of messages.
    SubscriberQueue,
    /// The subscriber had parked no connection for this copy: a socket
    /// transport's fan-out rides connections the subscriber parks, and the
    /// pool was empty.
    NoParkedConnection,
    /// A newer segment opened on the topic while this copy was unacknowledged.
    Superseded,
    /// The dish's `max_age` passed before this copy was acknowledged.
    Expired,
    /// A datagram segment exceeded the dish connection's datagram size.
    TooLarge,
    /// The dish's connection did not agree the datagram capability.
    NoDatagrams,
    /// A segment copy lost layers above 0 and kept layer 0
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.3).
    LayersCut,
}

/// What a publisher dropped on one topic, by cause.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicDrops {
    /// The topic the dropped copies were published on.
    pub topic: Arc<str>,
    /// Copies dropped for an exhausted subscriber byte budget.
    pub subscriber_budget: u64,
    /// Copies dropped for a full subscriber queue.
    pub subscriber_queue: u64,
    /// Copies dropped because the subscriber had parked no connection.
    pub no_parked_connection: u64,
    /// RADIO copies reset because a newer segment opened on the topic.
    pub superseded: u64,
    /// RADIO copies reset because the dish's `max_age` passed.
    pub expired: u64,
    /// RADIO datagram segments larger than the dish's connection carries.
    pub too_large: u64,
    /// RADIO datagram segments for a dish that carries no datagrams.
    pub no_datagrams: u64,
    /// Segment copies that lost layers above 0 and kept layer 0; at most one
    /// per copy and segment
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.3). A copy that later loses layer 0 too counts here and under
    /// that loss's cause.
    pub layers_cut: u64,
}

impl TopicDrops {
    /// Every cause summed.
    pub fn total(&self) -> u64 {
        self.subscriber_budget
            + self.subscriber_queue
            + self.no_parked_connection
            + self.superseded
            + self.expired
            + self.too_large
            + self.no_datagrams
            + self.layers_cut
    }
}

/// What a radio dropped toward one joined dish connection, summed over
/// topics ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.6).
///
/// One record per joined dish connection, removed when that connection
/// closes, so [`crate::Radio::dish_drops`] holds at most `max_connections`.
/// A copy that found no parked connection is counted per topic only.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DishDrops {
    /// The key or principal the dish's connection proved; `None` when it is
    /// anonymous.
    pub peer: Option<PeerIdentity>,
    /// Copies dropped for an exhausted byte budget.
    pub subscriber_budget: u64,
    /// Copies dropped for a full queue.
    pub subscriber_queue: u64,
    /// Copies reset because a newer segment opened on their topic.
    pub superseded: u64,
    /// Copies reset because the `max_age` passed.
    pub expired: u64,
    /// Datagram segments larger than the dish's connection carries.
    pub too_large: u64,
    /// Datagram segments for a dish that carries no datagrams.
    pub no_datagrams: u64,
    /// Segment copies that lost layers above 0 and kept layer 0.
    pub layers_cut: u64,
}

/// One counter per cause.
#[derive(Default)]
pub(crate) struct Causes {
    budget: AtomicU64,
    queue: AtomicU64,
    no_parked: AtomicU64,
    superseded: AtomicU64,
    expired: AtomicU64,
    too_large: AtomicU64,
    no_datagrams: AtomicU64,
    layers_cut: AtomicU64,
}

impl Causes {
    pub(crate) fn record(&self, cause: DropCause) {
        let counter = match cause {
            DropCause::SubscriberBudget => &self.budget,
            DropCause::SubscriberQueue => &self.queue,
            DropCause::NoParkedConnection => &self.no_parked,
            DropCause::Superseded => &self.superseded,
            DropCause::Expired => &self.expired,
            DropCause::TooLarge => &self.too_large,
            DropCause::NoDatagrams => &self.no_datagrams,
            DropCause::LayersCut => &self.layers_cut,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, topic: &Arc<str>) -> TopicDrops {
        TopicDrops {
            topic: Arc::clone(topic),
            subscriber_budget: self.budget.load(Ordering::Relaxed),
            subscriber_queue: self.queue.load(Ordering::Relaxed),
            no_parked_connection: self.no_parked.load(Ordering::Relaxed),
            superseded: self.superseded.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            too_large: self.too_large.load(Ordering::Relaxed),
            no_datagrams: self.no_datagrams.load(Ordering::Relaxed),
            layers_cut: self.layers_cut.load(Ordering::Relaxed),
        }
    }

    /// The counters as one dish's record.
    pub(crate) fn dish_snapshot(&self, peer: Option<PeerIdentity>) -> DishDrops {
        DishDrops {
            peer,
            subscriber_budget: self.budget.load(Ordering::Relaxed),
            subscriber_queue: self.queue.load(Ordering::Relaxed),
            superseded: self.superseded.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            too_large: self.too_large.load(Ordering::Relaxed),
            no_datagrams: self.no_datagrams.load(Ordering::Relaxed),
            layers_cut: self.layers_cut.load(Ordering::Relaxed),
        }
    }
}

/// The drops on one publisher path: a total, and a count per topic and
/// cause.
///
/// The per-topic table is bounded by `Limits::max_sequence_scopes`, the same
/// ceiling the sequencer's per-topic table has and for the same reason: the
/// topics are the publishing application's, but a table nobody bounds is a
/// table that grows for the life of the process. At the cap a new topic's
/// drops count in the total and nowhere else, which is what the sequencer
/// does with a new scope.
pub(crate) struct DropTable {
    total: AtomicU64,
    per_topic: RwLock<HashMap<Arc<str>, Causes>>,
    max_topics: usize,
}

impl DropTable {
    pub(crate) fn new(max_topics: usize) -> DropTable {
        DropTable {
            total: AtomicU64::new(0),
            per_topic: RwLock::new(HashMap::new()),
            max_topics,
        }
    }

    /// Counts one dropped copy of a message on `topic`.
    ///
    /// A read lock on the common path — the topic is known — and a write
    /// lock only for a topic's first drop. Drops are the exceptional path of
    /// fan-out, so neither is on the path of a message that gets through.
    pub(crate) fn record(&self, topic: &Arc<str>, cause: DropCause) {
        self.total.fetch_add(1, Ordering::Relaxed);
        {
            let table = self.per_topic.read().expect("drop table poisoned");
            if let Some(causes) = table.get(topic) {
                causes.record(cause);
                return;
            }
        }
        let mut table = self.per_topic.write().expect("drop table poisoned");
        if let Some(causes) = table.get(topic) {
            causes.record(cause);
        } else if table.len() < self.max_topics {
            let causes = Causes::default();
            causes.record(cause);
            table.insert(Arc::clone(topic), causes);
        }
    }

    /// Every drop on the path, over topics and causes.
    pub(crate) fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub(crate) fn on_topic(&self, topic: &str) -> Option<TopicDrops> {
        let table = self.per_topic.read().expect("drop table poisoned");
        table
            .get_key_value(topic)
            .map(|(topic, causes)| causes.snapshot(topic))
    }

    pub(crate) fn by_topic(&self) -> Vec<TopicDrops> {
        let table = self.per_topic.read().expect("drop table poisoned");
        table
            .iter()
            .map(|(topic, causes)| causes.snapshot(topic))
            .collect()
    }
}

/// Everything registered for one publisher path.
struct PathState {
    subs: Vec<SubEntry>,
    /// Copies dropped for a subscriber that could not take them, by topic
    /// and cause.
    drops: Arc<DropTable>,
}

/// Subscriptions for every publisher path served by one listener.
pub(crate) struct SubRegistry {
    paths: RwLock<HashMap<Arc<str>, PathState>>,
    /// Filters held per connection, summed over paths, for `max_subscriptions`.
    per_conn: RwLock<HashMap<usize, usize>>,
    limits: Limits,
    /// Numbers published messages per topic. The number belongs to the
    /// *message*, not to a subscriber's copy, which is what makes a dropped
    /// copy visible as a gap: the survivors keep the numbers the lost ones
    /// would have had.
    sequencer: Sequencer,
    /// Names one streamed publish, so a writer can tell the chunks of two
    /// concurrent ones apart. Local and never on the wire: what identifies a
    /// transfer to a subscriber is the stream it arrives on.
    next_stream: AtomicU64,
}

impl SubRegistry {
    pub(crate) fn new(limits: Limits, ordering: OrderingMode) -> SubRegistry {
        SubRegistry {
            paths: RwLock::new(HashMap::new()),
            per_conn: RwLock::new(HashMap::new()),
            limits,
            sequencer: Sequencer::new(ordering, limits.max_sequence_scopes),
            next_stream: AtomicU64::new(0),
        }
    }

    /// Records interest of `ctx` in `filter` on `path`.
    ///
    /// The path need not be registered by a publisher yet: a subscriber may
    /// arrive first, and the subscription is bounded by `max_subscriptions`
    /// either way. Duplicate filters are idempotent.
    ///
    /// Returns [`Error::LimitExceeded`] when the connection is already holding
    /// `max_subscriptions` filters; the caller closes the connection, because
    /// SUBSCRIBE carries no transfer id to answer with an ERROR frame.
    pub(crate) fn subscribe(
        &self,
        path: &str,
        ctx: &ConnHandle,
        filter: String,
    ) -> Result<(), Error> {
        let conn_id = ctx.conn.stable_id();
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");

        let state = paths.entry(Arc::from(path)).or_insert_with(|| PathState {
            subs: Vec::new(),
            drops: Arc::new(DropTable::new(self.limits.max_sequence_scopes)),
        });
        if let Some(entry) = state.subs.iter_mut().find(|e| e.conn_id == conn_id) {
            if entry.filters.contains(&filter) {
                return Ok(());
            }
            let count = per_conn.entry(conn_id).or_insert(0);
            if *count >= self.limits.max_subscriptions {
                return Err(Error::LimitExceeded);
            }
            *count += 1;
            entry.filters.insert(filter);
            return Ok(());
        }

        let count = per_conn.entry(conn_id).or_insert(0);
        if *count >= self.limits.max_subscriptions {
            return Err(Error::LimitExceeded);
        }
        *count += 1;

        let (tx, rx) = mpsc::channel(WRITER_QUEUE);
        let budget = Arc::new(Semaphore::new(self.limits.subscriber_buffer_bytes));
        let drops = Arc::clone(&state.drops);
        state.subs.push(SubEntry {
            conn_id,
            filters: HashSet::from([filter]),
            tx,
            budget: Arc::clone(&budget),
        });
        // One writer task per (connection, path): it serializes this
        // subscriber's messages, which is what makes delivery FIFO per
        // subscriber even though each message rides its own QUIC stream.
        ctx.exec
            .spawn(writer(Arc::clone(ctx), Arc::from(path), rx, budget, drops));
        Ok(())
    }

    /// Counts one subscription against `max_subscriptions` without recording
    /// a filter.
    ///
    /// For a subscription this registry does not serve: an L2 queue keeps its
    /// consumers itself, but the bound is per connection over *all* paths, so
    /// the count has to stay in one place or it bounds nothing
    /// (`docs/PROTOCOL.md` §10).
    pub(crate) fn reserve(&self, conn_id: usize) -> Result<(), Error> {
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");
        let count = per_conn.entry(conn_id).or_insert(0);
        if *count >= self.limits.max_subscriptions {
            return Err(Error::LimitExceeded);
        }
        *count += 1;
        Ok(())
    }

    /// Gives one reserved subscription back. An unknown connection is ignored:
    /// UNSUBSCRIBE is idempotent.
    pub(crate) fn release(&self, conn_id: usize) {
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");
        decrement(&mut per_conn, conn_id, 1);
    }

    /// Withdraws one filter. An unknown filter, path or connection is ignored:
    /// UNSUBSCRIBE is idempotent and races legitimately with teardown.
    pub(crate) fn unsubscribe(&self, path: &str, conn_id: usize, filter: &str) {
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");
        let Some(state) = paths.get_mut(path) else {
            return;
        };
        let Some(index) = state.subs.iter().position(|e| e.conn_id == conn_id) else {
            return;
        };
        if !state.subs[index].filters.remove(filter) {
            return;
        }
        decrement(&mut per_conn, conn_id, 1);
        if state.subs[index].filters.is_empty() {
            // The writer task ends when its channel closes.
            state.subs.swap_remove(index);
        }
    }

    /// Drops every subscription held by one connection. Called when the
    /// connection closes, so a churn of peers cannot grow the registry.
    pub(crate) fn remove_connection(&self, conn_id: usize) {
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut per_conn = self.per_conn.write().expect("subscription lock poisoned");
        for state in paths.values_mut() {
            if let Some(index) = state.subs.iter().position(|e| e.conn_id == conn_id) {
                state.subs.swap_remove(index);
            }
        }
        per_conn.remove(&conn_id);
    }

    /// Fans `payload` out to every subscriber of `path` whose filter matches
    /// `topic`, and returns how many it was enqueued to.
    ///
    /// Synchronous and non-blocking by design: a subscriber that cannot take
    /// the bytes right now loses this message rather than stalling everyone
    /// else. `want` is the payload length in permits.
    pub(crate) fn publish(
        &self,
        path: &str,
        topic: &str,
        payload: Bytes,
        trace: Option<TraceContext>,
        want: u32,
    ) -> usize {
        let paths = self.paths.read().expect("subscription lock poisoned");
        let Some(state) = paths.get(path) else {
            return 0;
        };

        let topic: Arc<str> = Arc::from(topic);
        // Before fan-out, so every copy of one message carries one number and
        // a copy dropped below leaves a hole rather than renumbering.
        let sequence = self.sequencer.next(&topic);
        let mut sent = 0usize;
        for entry in &state.subs {
            if !entry.filters.iter().any(|f| filter::matches(&topic, f)) {
                continue;
            }
            let Ok(permit) = entry.budget.try_acquire_many(want) else {
                state.drops.record(&topic, DropCause::SubscriberBudget);
                tracing::debug!(path, %topic, "subscriber budget exhausted; message dropped");
                continue;
            };
            let msg = PubMsg {
                topic: Arc::clone(&topic),
                payload: payload.clone(),
                trace,
                sequence,
            };
            match entry.tx.try_send(PubItem::Whole(msg)) {
                Ok(()) => {
                    // The writer returns the permits once the bytes are gone.
                    permit.forget();
                    sent += 1;
                }
                Err(_) => {
                    // Dropping `permit` returns the budget immediately.
                    state.drops.record(&topic, DropCause::SubscriberQueue);
                    tracing::debug!(path, %topic, "subscriber queue full; message dropped");
                }
            }
        }
        sent
    }

    /// Opens a streamed publish: one stream per matched subscriber, written
    /// chunk by chunk and never materialized whole.
    ///
    /// The subscriber set is fixed here, at `open`, because a stream is a
    /// stream: a subscriber that arrives mid-payload would receive a fragment
    /// and could not be told where it started. It gets the next message.
    pub(crate) fn open(&self, path: &str, topic: &str, trace: Option<TraceContext>) -> FanOut {
        let paths = self.paths.read().expect("subscription lock poisoned");
        let topic: Arc<str> = Arc::from(topic);
        let id = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let Some(state) = paths.get(path) else {
            return FanOut::empty(id, topic);
        };
        // One number for the message, as `publish` does: a subscriber whose
        // copy is aborted below leaves a hole rather than renumbering.
        let sequence = self.sequencer.next(&topic);
        let mut targets = Vec::new();
        for entry in &state.subs {
            if !entry.filters.iter().any(|f| filter::matches(&topic, f)) {
                continue;
            }
            // The permit for the ending — `Finish` or `Abort` — is taken
            // before the first chunk, so ending a transfer can never fail for
            // want of queue room. Without it a subscriber whose queue filled
            // mid-payload would hold an open stream waiting for a FIN nobody
            // could enqueue.
            let Ok(ending) = entry.tx.clone().try_reserve_owned() else {
                state.drops.record(&topic, DropCause::SubscriberQueue);
                continue;
            };
            let head = PubMsg {
                topic: Arc::clone(&topic),
                payload: Bytes::new(),
                trace,
                sequence,
            };
            if entry.tx.try_send(PubItem::Begin { id, head }).is_err() {
                state.drops.record(&topic, DropCause::SubscriberQueue);
                continue;
            }
            targets.push(Target {
                tx: entry.tx.clone(),
                budget: Arc::clone(&entry.budget),
                ending: Some(ending),
            });
        }
        FanOut {
            id,
            topic,
            targets,
            drops: Some(Arc::clone(&state.drops)),
            finished: false,
        }
    }

    /// Connections currently subscribed to `path`.
    pub(crate) fn subscriber_count(&self, path: &str) -> usize {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |s| s.subs.len())
    }

    /// Filters registered on `path`, summed over subscribers.
    pub(crate) fn filter_count(&self, path: &str) -> usize {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |s| s.subs.iter().map(|e| e.filters.len()).sum())
    }

    /// Messages dropped on `path` because a subscriber could not take them,
    /// summed over topics and causes.
    pub(crate) fn dropped(&self, path: &str) -> u64 {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |s| s.drops.total.load(Ordering::Relaxed))
    }

    /// The drops on `path` for one topic, or `None` if none was counted.
    pub(crate) fn dropped_on(&self, path: &str, topic: &str) -> Option<TopicDrops> {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .and_then(|s| s.drops.on_topic(topic))
    }

    /// The drops on `path`, one entry per topic that lost a copy.
    pub(crate) fn drops(&self, path: &str) -> Vec<TopicDrops> {
        self.paths
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or_else(Vec::new, |s| s.drops.by_topic())
    }
}

/// One subscriber a streamed publish is writing to.
struct Target {
    tx: mpsc::Sender<PubItem>,
    budget: Arc<Semaphore>,
    /// The reserved slot for this copy's `Finish` or `Abort`. Always `Some`
    /// until the transfer ends.
    ending: Option<OwnedPermit<PubItem>>,
}

/// A publish in progress: a payload written once and fanned out to one stream
/// per subscriber, without ever being held whole.
///
/// This is what [`Publisher::publish`](crate::Publisher::publish) cannot do:
/// that call takes a `Bytes` and refuses anything above
/// `Limits::subscriber_buffer_bytes`, because a message that large could not
/// be enqueued for anybody. Here the **chunk** is what the budget bounds, so
/// the payload is unbounded and a 33 MB frame is an ordinary publish
/// (`docs/requirements/zeughaus-video.md` request 1).
///
/// **The drop behaviour of [GUARANTEES](../../../docs/GUARANTEES.md) §6 is
/// per subscriber, not per publish.** A subscriber whose budget or queue
/// cannot take a chunk loses *this* transfer — its stream is reset, so it
/// never mistakes a partial payload for a whole one — and it is counted in
/// [`Publisher::drops`](crate::Publisher::drops) like any other fan-out drop.
/// Every other subscriber keeps receiving, and the publisher never waits for
/// the slowest one.
///
/// Dropping this handle without [`FanOut::finish`] aborts every copy, for the
/// same reason [`OutgoingTransfer`](crate::OutgoingTransfer) resets on drop.
pub struct FanOut {
    id: u64,
    topic: Arc<str>,
    targets: Vec<Target>,
    /// `None` only for a fan-out with no subscribers at all, which has
    /// nothing to count against.
    drops: Option<Arc<DropTable>>,
    finished: bool,
}

impl FanOut {
    fn empty(id: u64, topic: Arc<str>) -> FanOut {
        FanOut {
            id,
            topic,
            targets: Vec::new(),
            drops: None,
            finished: false,
        }
    }

    /// The topic this transfer is published on.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Subscribers still receiving this transfer.
    ///
    /// It only falls: a subscriber that loses a chunk is gone from this
    /// transfer, and one that subscribes while it is in flight receives the
    /// next message rather than half of this one.
    pub fn subscribers(&self) -> usize {
        self.targets.len()
    }

    /// Writes the next chunk to every subscriber still receiving, waiting up
    /// to `limit` for one that has no room, and returns how many subscribers
    /// are left.
    ///
    /// **The bound is mandatory and finite, and it is the caller's**, for the
    /// reason [`Runtime::drain`](crate::Runtime::drain) takes one
    /// (`docs/decisions/0009-drain.md` §4.4): waiting on a subscriber with no
    /// deadline is how a publisher hangs on a peer's behaviour, and refusing
    /// to wait at all would make a payload larger than
    /// `Limits::subscriber_buffer_bytes` impossible to send to anyone —
    /// the publisher would outrun its own budget and abort every copy. A
    /// subscriber that frees room inside `limit` keeps the transfer; one that
    /// does not loses it, and only it.
    ///
    /// One `Bytes` allocation is shared by every copy, so a chunk costs one
    /// buffer regardless of subscriber count, and the payload is never held
    /// whole anywhere.
    ///
    /// Fails with [`Error::LimitExceeded`] for a chunk larger than
    /// `Limits::subscriber_buffer_bytes`: such a chunk could never be
    /// enqueued for anybody, and the point of this API is that the *payload*
    /// need not fit while a chunk does.
    pub async fn write_within(
        &mut self,
        chunk: impl Into<Bytes>,
        limit: std::time::Duration,
    ) -> Result<usize, Error> {
        let chunk = chunk.into();
        let want = u32::try_from(chunk.len()).map_err(|_| Error::LimitExceeded)?;
        let mut kept = Vec::with_capacity(self.targets.len());
        for mut target in std::mem::take(&mut self.targets) {
            let budget = Arc::clone(&target.budget);
            let acquired = tokio::select! {
                permit = tokio::time::timeout(limit, budget.acquire_many_owned(want)) => {
                    match permit {
                        Ok(Ok(permit)) => Some(permit),
                        // Timed out, or the semaphore is closed.
                        _ => None,
                    }
                }
                // The writer task is gone — the connection closed — so no
                // permit will ever come back. Without this arm the wait would
                // run to `limit` for a subscriber that cannot exist.
                () = target.tx.closed() => None,
            };
            let Some(permit) = acquired else {
                self.abort_one(&mut target, DropCause::SubscriberBudget);
                continue;
            };
            match self.enqueue(&target, &chunk) {
                Ok(()) => {
                    // The writer returns the permits once the bytes are gone.
                    permit.forget();
                    kept.push(target);
                }
                Err(()) => {
                    // Dropping the permit returns the budget immediately.
                    drop(permit);
                    self.abort_one(&mut target, DropCause::SubscriberQueue);
                }
            }
        }
        self.targets = kept;
        Ok(self.targets.len())
    }

    /// Writes the next chunk without ever waiting: a subscriber with no room
    /// right now loses the transfer.
    ///
    /// Fan-out's `Drop` from [GUARANTEES](../../../docs/GUARANTEES.md) §6 in
    /// its purest form, and the right call where a later chunk supersedes an
    /// earlier one — a video frame, a market snapshot — because a subscriber
    /// that cannot keep up should be waiting for the *next* transfer rather
    /// than holding this one up. A publisher streaming a payload that must
    /// arrive whole wants [`FanOut::write_within`].
    pub fn write_now(&mut self, chunk: impl Into<Bytes>) -> Result<usize, Error> {
        let chunk = chunk.into();
        let want = u32::try_from(chunk.len()).map_err(|_| Error::LimitExceeded)?;
        let mut kept = Vec::with_capacity(self.targets.len());
        for mut target in std::mem::take(&mut self.targets) {
            let Ok(permit) = target.budget.try_acquire_many(want) else {
                self.abort_one(&mut target, DropCause::SubscriberBudget);
                continue;
            };
            match self.enqueue(&target, &chunk) {
                Ok(()) => {
                    permit.forget();
                    kept.push(target);
                }
                Err(()) => {
                    drop(permit);
                    self.abort_one(&mut target, DropCause::SubscriberQueue);
                }
            }
        }
        self.targets = kept;
        Ok(self.targets.len())
    }

    /// Hands one chunk to one subscriber's writer. `Err` means the writer's
    /// queue is full or gone; the caller aborts that copy.
    fn enqueue(&self, target: &Target, chunk: &Bytes) -> Result<(), ()> {
        let item = PubItem::Chunk {
            id: self.id,
            payload: chunk.clone(),
        };
        target.tx.try_send(item).map_err(|_| ())
    }

    /// Ends the transfer, and returns how many subscribers received all of
    /// it as far as this side can tell.
    ///
    /// "As far as this side can tell" is the honest claim: the count is the
    /// subscribers whose every chunk was enqueued and whose FIN is queued
    /// behind them. A fan-out copy carries no receipt — nobody holds a
    /// `Delivery` for it — so the transport acknowledgement is awaited by the
    /// drain and by nothing else (`docs/decisions/0009-drain.md` §4.2).
    pub fn finish(mut self) -> usize {
        self.finished = true;
        let id = self.id;
        let delivered = self.targets.len();
        for target in &mut self.targets {
            if let Some(ending) = target.ending.take() {
                ending.send(PubItem::Finish { id });
            }
        }
        delivered
    }

    /// Aborts one subscriber's copy, counting the cause.
    fn abort_one(&self, target: &mut Target, cause: DropCause) {
        if let Some(drops) = &self.drops {
            drops.record(&self.topic, cause);
        }
        if let Some(ending) = target.ending.take() {
            ending.send(PubItem::Abort { id: self.id });
        }
        tracing::debug!(topic = %self.topic, ?cause, "streamed fan-out copy aborted");
    }
}

impl Drop for FanOut {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Abandoned mid-payload: reset every copy rather than leave a
        // subscriber waiting for a FIN.
        for target in &mut self.targets {
            if let Some(ending) = target.ending.take() {
                ending.send(PubItem::Abort { id: self.id });
            }
        }
    }
}

fn decrement(counts: &mut HashMap<usize, usize>, conn_id: usize, by: usize) {
    if let Some(count) = counts.get_mut(&conn_id) {
        *count = count.saturating_sub(by);
        if *count == 0 {
            counts.remove(&conn_id);
        }
    }
}

/// Writes one subscriber's messages, in order, each on its own uni stream.
///
/// Fan-out is one-way by construction: a published copy owes nothing back, so
/// it rides a unidirectional stream and the publisher never waits on it.
async fn writer(
    ctx: ConnHandle,
    path: Arc<str>,
    mut rx: mpsc::Receiver<PubItem>,
    budget: Arc<Semaphore>,
    drops: Arc<DropTable>,
) {
    // The streams of the streamed publishes currently in flight for this
    // subscriber. Bounded by what this process opens, never by the peer: a
    // remote party cannot make a publisher open a transfer.
    let mut streaming: HashMap<u64, SendHalf> = HashMap::new();
    loop {
        let item = tokio::select! {
            item = rx.recv() => match item {
                Some(item) => item,
                None => break,
            },
            // Without this a subscriber on a dead connection would sit here
            // forever holding an `Arc<ConnCtx>`.
            _ = ctx.conn.closed() => break,
        };
        match item {
            PubItem::Whole(msg) => {
                let len = msg.payload.len();
                let outcome = write_one(&ctx, &path, &msg).await;
                budget.add_permits(len);
                match outcome {
                    Ok(()) => {}
                    // The subscriber parked no connection for this copy. That
                    // is a drop of the copy, not a failure of the
                    // subscription: the pool refills and the next message may
                    // well go out
                    // ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
                    // §4.4). Counted exactly like an exhausted byte budget.
                    Err(Error::NoParkedConnection) => {
                        drops.record(&msg.topic, DropCause::NoParkedConnection);
                        tracing::debug!(path = %path, "no parked connection; copy dropped");
                    }
                    Err(e) => {
                        tracing::debug!(path = %path, error = %e, "fan-out write failed; subscriber writer ending");
                        break;
                    }
                }
            }
            PubItem::Begin { id, head } => match begin_one(&ctx, &path, &head).await {
                Ok(stream) => {
                    streaming.insert(id, stream);
                }
                Err(Error::NoParkedConnection) => {
                    drops.record(&head.topic, DropCause::NoParkedConnection);
                    tracing::debug!(path = %path, "no parked connection; streamed copy dropped");
                }
                Err(e) => {
                    tracing::debug!(path = %path, error = %e, "fan-out open failed; subscriber writer ending");
                    break;
                }
            },
            PubItem::Chunk { id, payload } => {
                let len = payload.len();
                // A chunk for a transfer whose stream is gone — the open
                // failed, or a write did — is dropped here: the budget is
                // still returned, because the publisher charged it.
                if let Some(stream) = streaming.get_mut(&id)
                    && let Err(e) = stream.write_all(&payload).await
                {
                    tracing::debug!(path = %path, error = %e, "streamed fan-out write failed");
                    if let Some(mut stream) = streaming.remove(&id) {
                        stream.reset(codes::CANCELED);
                    }
                }
                budget.add_permits(len);
            }
            PubItem::Finish { id } => {
                if let Some(mut stream) = streaming.remove(&id)
                    && stream.finish().is_ok()
                    && ctx.parked.park(stream.stopped())
                {
                    ctx.shared.drain.evict();
                }
            }
            PubItem::Abort { id } => {
                if let Some(mut stream) = streaming.remove(&id) {
                    stream.reset(codes::CANCELED);
                }
            }
        }
    }
    // Whatever is still open was abandoned by the connection ending, not by
    // the publisher: reset it so the subscriber does not wait for a FIN.
    for (_, mut stream) in streaming {
        stream.reset(codes::CANCELED);
    }
    tracing::debug!(path = %path, "subscriber writer ended");
}

/// Opens one subscriber's stream for a streamed publish and writes its DATA
/// header.
///
/// No `content_len`: the key is optional at the decoder
/// (`docs/PROTOCOL.md` §6.2) and a streaming publisher does not know the
/// length. A subscriber therefore reads until FIN, which is what every
/// streamed transfer in weida does.
async fn begin_one(ctx: &ConnHandle, path: &str, head: &PubMsg) -> Result<SendHalf, Error> {
    let mut header = DataHeader::addressed(path);
    header.topic = Some(head.topic.to_string());
    header.traceparent = head.trace.map(|t| t.to_traceparent());
    header.sequence = head.sequence;
    let mut stream = ctx.open_uni().await?;
    write_data_preamble(&mut stream, &header).await?;
    Ok(stream)
}

async fn write_one(ctx: &ConnHandle, path: &str, msg: &PubMsg) -> Result<(), Error> {
    let mut header = DataHeader::addressed(path);
    header.topic = Some(msg.topic.to_string());
    header.content_len = Some(msg.payload.len() as u64);
    header.traceparent = msg.trace.map(|t| t.to_traceparent());
    // The number the publisher assigned to this *message* (`None` under
    // `core`): every subscriber's copy carries the same one, so a copy this
    // subscriber lost shows up as a hole in its own sequence.
    header.sequence = msg.sequence;

    let mut stream = ctx.open_uni().await?;
    write_data_preamble(&mut stream, &header).await?;
    stream.write_all(&msg.payload).await?;
    stream.finish()?;
    // A published copy is a finished transfer like any other, and nobody
    // holds a receipt for it: park it on this connection so a drain waits
    // for it (`docs/decisions/0009-drain.md` §4.2).
    if ctx.parked.park(stream.stopped()) {
        ctx.shared.drain.evict();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_literal_filter_matches_whole_segments_only() {
        assert!(filter::matches("px.eur", "px.eur"));
        assert!(!filter::matches("px.eur", "px.eur.spot"));
        assert!(!filter::matches("fx.usd", "px.eur"));
        // The boundary a byte prefix could not see: `px.` does not select
        // everything under `px`, and `sensors.temp` does not select
        // `sensors.temperature`.
        assert!(!filter::matches("px.eur", "px."));
        assert!(!filter::matches("sensors.temperature", "sensors.temp"));
    }

    #[test]
    fn the_empty_filter_and_the_rest_wildcard_take_everything() {
        assert!(filter::matches("", ""));
        assert!(filter::matches("anything.at.all", ""));
        assert!(filter::matches("anything.at.all", "#"));
        assert!(filter::matches("", "#"));
    }

    #[test]
    fn one_segment_wildcard_matches_exactly_one() {
        assert!(filter::matches("px.eur", "px.*"));
        assert!(filter::matches("sensors.a.temp", "sensors.*.temp"));
        assert!(filter::matches("px.eur", "*.eur"));
        // Exactly one: neither none nor two.
        assert!(!filter::matches("px", "px.*"));
        assert!(!filter::matches("px.eur.spot", "px.*"));
    }

    #[test]
    fn the_rest_wildcard_matches_zero_or_more_trailing_segments() {
        assert!(filter::matches("px", "px.#"));
        assert!(filter::matches("px.eur", "px.#"));
        assert!(filter::matches("px.eur.spot", "px.#"));
        assert!(!filter::matches("fx", "px.#"));
        assert!(!filter::matches("pxx", "px.#"));
    }

    #[test]
    fn a_topic_is_never_a_pattern() {
        // `*` and `#` in a published topic are ordinary bytes.
        assert!(filter::matches("px.*", "px.*"));
        assert!(!filter::matches("px.*", "px.eur"));
        assert!(filter::matches("px.#", "px.#"));
        assert!(filter::matches("px.*", "*.*"));
    }

    #[test]
    fn empty_segments_match_only_empty_segments() {
        assert!(filter::matches("px.", "px."));
        assert!(!filter::matches("px.eur", "px."));
        assert!(filter::matches("px.", "px.*"));
    }

    #[test]
    fn matching_is_byte_exact_not_case_folded() {
        assert!(!filter::matches("PX.EUR", "px.*"));
        assert!(!filter::matches("px.eur", "PX.*"));
    }
}
