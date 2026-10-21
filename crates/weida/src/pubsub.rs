//! Publisher-side subscription registry and fan-out.
//!
//! A publisher does not know its subscribers: they arrive as SUBSCRIBE frames
//! on whatever connections the listener has accepted. This module owns that
//! mapping and the one genuinely new selection policy of the pattern layer —
//! fan-out (`docs/ARCHITECTURE.md`, pattern primitive P3). Everything else a
//! published message needs is the ordinary one-way transfer path.
//!
//! **Delivery is per-subscriber best effort.** Each subscriber — one
//! connection, however many paths it listens on — has a byte budget; a
//! message that does not fit is dropped for that subscriber, counted, and the
//! publisher moves on. A slow consumer therefore costs the publisher nothing
//! but its own messages (master doc §17: telemetry fan-out tolerates drops
//! and cannot tolerate a stalled producer).
//!
//! **Two tables, because two parties fill them.** The subscriptions — a path,
//! a connection, its filters — are the peer's, so they are bounded by
//! `Limits::max_subscriptions` per connection, kept under the connection's
//! own record and gone with its last filter or its close; a path nobody
//! holds a filter on is not in the table at all, so naming paths grows
//! nothing. The publisher's record of a path — its drop counters and its
//! sequence numbers — is the application's, made by its own first publish.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc::OwnedPermit;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use weida_core::{Error, Limits, PeerIdentity, TraceContext};
use weida_protocol::header::OrderingMode;
use weida_protocol::header::limits::MAX_TOPIC_BYTES;
use weida_protocol::{DataHeader, FrameKind, MAX_PREAMBLE_LEN, filter, preamble_bytes};

use crate::conn::{ConnCtx, ConnHandle};
use crate::ordering::Sequencer;
use crate::runtime::Exec;
use crate::transport::SendHalf;
use weida_protocol::codes;

/// Messages one subscriber's writer task may hold. The real bound is the byte
/// budget; this only keeps the channel itself from growing without limit when
/// messages are tiny.
const WRITER_QUEUE: usize = 1024;

/// The preamble and DATA header of one outgoing transfer, encoded once.
///
/// Every subscriber's copy of a published message carries the same path,
/// topic, length, trace and sequence number, so the bytes are the same too:
/// each copy writes a clone of these rather than building a header of its
/// own. A segment layer's copy is one frame per (copy, layer).
#[derive(Clone)]
pub(crate) struct Frame {
    /// The preamble and the header, in wire order.
    pub(crate) bytes: Bytes,
    /// The DATA header's length alone, which is what a peer's
    /// `max_header_bytes` bounds.
    header_len: usize,
}

impl Frame {
    /// Encodes `header` after checking it against what a conformant decoder
    /// accepts, and refuses with [`Error::LimitExceeded`] what it would not:
    /// its answer is to close the connection, and a connection is shared by
    /// every subscription on it.
    pub(crate) fn of(header: &DataHeader) -> Result<Frame, Error> {
        header.check().map_err(|_| Error::LimitExceeded)?;
        // One buffer: the header goes after room reserved for the preamble,
        // and the preamble is right-aligned against it, so there is no second
        // buffer and no copy of the header.
        let mut buf = Vec::with_capacity(MAX_PREAMBLE_LEN + 128);
        buf.resize(MAX_PREAMBLE_LEN, 0);
        header.encode_into(&mut buf);
        let header_len = buf.len() - MAX_PREAMBLE_LEN;
        let (preamble, len) = preamble_bytes(FrameKind::Data, header_len as u64);
        let start = MAX_PREAMBLE_LEN - len;
        buf[start..MAX_PREAMBLE_LEN].copy_from_slice(&preamble[..len]);
        Ok(Frame {
            bytes: Bytes::from(buf).slice(start..),
            header_len,
        })
    }

    /// Refuses with [`Error::LimitExceeded`] a frame the peer's
    /// `max_header_bytes` cannot take: a longer header makes its parser close
    /// the connection, which would end every other transfer on it for the
    /// sake of this one.
    pub(crate) fn fits(&self, ctx: &ConnHandle) -> Result<(), Error> {
        match ctx.agreed_now() {
            Some(agreed) => agreed
                .check_header_len(self.header_len)
                .map_err(|_| Error::LimitExceeded),
            // Negotiation precedes every transfer a publisher or a radio
            // starts, so this is not reached; there is nothing to check
            // against.
            None => Ok(()),
        }
    }
}

/// The frame of a message published on `path`.
fn encode_frame(
    path: &str,
    topic: &str,
    content_len: Option<u64>,
    trace: Option<TraceContext>,
    sequence: Option<u64>,
) -> Result<Frame, Error> {
    Frame::of(&DataHeader {
        topic: Some(topic.to_owned()),
        content_len,
        traceparent: trace.map(|t| t.to_traceparent()),
        sequence,
        ..DataHeader::addressed(path)
    })
}

/// One message queued for one subscriber.
///
/// The frame is encoded once per message and shared, and so is the payload:
/// the fan-out holds one allocation of each regardless of subscriber count.
struct PubMsg {
    /// For counting a copy that is dropped.
    topic: Arc<str>,
    payload: Bytes,
    frame: Frame,
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
    Begin { id: u64, msg: PubMsg },
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
    /// A read lock on the common path — the topic is known, or the table is
    /// full and will never admit it — and a write lock only for a topic's
    /// first drop. Drops are the exceptional path of fan-out, so neither is
    /// on the path of a message that gets through.
    pub(crate) fn record(&self, topic: &Arc<str>, cause: DropCause) {
        self.total.fetch_add(1, Ordering::Relaxed);
        {
            let table = self.per_topic.read().expect("drop table poisoned");
            if let Some(causes) = table.get(topic) {
                causes.record(cause);
                return;
            }
            // At the cap a new topic counts in the total and nowhere else.
            // Taking the write lock for it would serialise every publishing
            // thread on drops that change nothing.
            if table.len() >= self.max_topics {
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

/// How a filter is looked up. The grammar of [`filter::matches`] has four
/// shapes and three of them are found from the topic instead of tested
/// against it, which is what keeps the cost of a message independent of how
/// many filters peers hold.
enum Shape<'a> {
    /// The empty filter and a lone `#`: every topic.
    All,
    /// No wildcard segment: only this very topic.
    Exact(&'a str),
    /// Literal segments and a final `#`: every topic that starts with those
    /// segments, the whole topic included (`px.#` takes `px`). Holds the
    /// segments, joined, without the `#`.
    Prefix(&'a str),
    /// A `*` segment, or a `#` where the grammar never puts one: tested
    /// against each topic with the matcher itself.
    Wild,
}

impl<'a> Shape<'a> {
    fn of(filter: &'a str) -> Shape<'a> {
        if filter.is_empty() {
            return Shape::All;
        }
        let (head, last) = match filter.rsplit_once(filter::SEPARATOR) {
            Some((head, last)) => (Some(head), last),
            None => (None, filter),
        };
        let wildcard = |segment: &str| segment == filter::ONE_SEGMENT || segment == filter::REST;
        if head.is_some_and(|head| head.split(filter::SEPARATOR).any(wildcard)) {
            return Shape::Wild;
        }
        match (head, last) {
            (None, filter::REST) => Shape::All,
            (Some(head), filter::REST) => Shape::Prefix(head),
            (_, filter::ONE_SEGMENT) => Shape::Wild,
            _ => Shape::Exact(filter),
        }
    }
}

/// One owner's hold on a filter, and what it attached to it.
struct Held<V> {
    owner: usize,
    value: V,
}

/// Who holds which filters, and which of them select a topic: the lookup
/// both a publisher's subscribers and a radio's dishes need.
///
/// An `owner` is a connection's id and `V` what the owner attached to the
/// filter. Every filter of the grammar except one with a `*` is found from
/// the topic's own text — one hash lookup for a filter without a wildcard,
/// one per segment for a filter ending in `#` — so a message costs the
/// number of its segments and of its matches, whatever the number of filters
/// held. A filter with a `*` is tested one by one, which is a cost
/// `Limits::max_subscriptions` bounds per connection.
///
/// The index is exactly [`filter::matches`], not an approximation of it: a
/// unit test compares the two over every short filter and topic of a small
/// alphabet.
pub(crate) struct FilterIndex<V> {
    exact: HashMap<Box<str>, Vec<Held<V>>>,
    prefix: HashMap<Box<str>, Vec<Held<V>>>,
    all: Vec<Held<V>>,
    /// By owner, so that one owner leaving costs its own filters and not
    /// everyone's.
    wild: HashMap<usize, Vec<(Box<str>, V)>>,
    len: usize,
}

impl<V> FilterIndex<V> {
    pub(crate) fn new() -> FilterIndex<V> {
        FilterIndex {
            exact: HashMap::new(),
            prefix: HashMap::new(),
            all: Vec::new(),
            wild: HashMap::new(),
            len: 0,
        }
    }

    /// The (owner, filter) pairs held.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Records that `owner` holds `filter`, with `value`. A filter the owner
    /// already held keeps its place and takes the new value; the old one is
    /// returned.
    pub(crate) fn insert(&mut self, owner: usize, filter: &str, value: V) -> Option<V> {
        let replaced = match Shape::of(filter) {
            Shape::All => put(&mut self.all, owner, value),
            Shape::Exact(key) => put(self.exact.entry(key.into()).or_default(), owner, value),
            Shape::Prefix(key) => put(self.prefix.entry(key.into()).or_default(), owner, value),
            Shape::Wild => {
                let held = self.wild.entry(owner).or_default();
                match held.iter_mut().find(|(f, _)| **f == *filter) {
                    Some((_, slot)) => Some(std::mem::replace(slot, value)),
                    None => {
                        held.push((filter.into(), value));
                        None
                    }
                }
            }
        };
        if replaced.is_none() {
            self.len += 1;
        }
        replaced
    }

    /// Withdraws `owner`'s `filter`, and returns what it carried if it was
    /// held.
    pub(crate) fn remove(&mut self, owner: usize, filter: &str) -> Option<V> {
        let removed = match Shape::of(filter) {
            Shape::All => take(&mut self.all, owner),
            Shape::Exact(key) => take_from(&mut self.exact, key, owner),
            Shape::Prefix(key) => take_from(&mut self.prefix, key, owner),
            Shape::Wild => {
                let held = self.wild.get_mut(&owner)?;
                let at = held.iter().position(|(f, _)| **f == *filter)?;
                let (_, value) = held.swap_remove(at);
                if held.is_empty() {
                    self.wild.remove(&owner);
                }
                Some(value)
            }
        };
        if removed.is_some() {
            self.len -= 1;
        }
        removed
    }

    /// Calls `hit` with the owner and value of every filter held that selects
    /// `topic`, once per filter: an owner holding two filters that both
    /// select it is reported twice.
    pub(crate) fn matching(&self, topic: &str, mut hit: impl FnMut(usize, &V)) {
        for held in &self.all {
            hit(held.owner, &held.value);
        }
        if let Some(held) = self.exact.get(topic) {
            for held in held {
                hit(held.owner, &held.value);
            }
        }
        if !self.prefix.is_empty() {
            // The topic cut after each of its segments in turn: a filter
            // `a.b.#` is the key `a.b`, and takes every topic that has `a.b`
            // as its first two segments.
            let mut end = 0;
            loop {
                let found = topic[end..].find(filter::SEPARATOR);
                let key = match found {
                    Some(offset) => &topic[..end + offset],
                    None => topic,
                };
                if let Some(held) = self.prefix.get(key) {
                    for held in held {
                        hit(held.owner, &held.value);
                    }
                }
                let Some(offset) = found else {
                    break;
                };
                end += offset + filter::SEPARATOR.len_utf8();
            }
        }
        for (owner, held) in &self.wild {
            for (filter, value) in held {
                if filter::matches(topic, filter) {
                    hit(*owner, value);
                }
            }
        }
    }

    /// Filters tested one by one instead of looked up.
    #[cfg(test)]
    fn tested_one_by_one(&self) -> usize {
        self.wild.values().map(Vec::len).sum()
    }
}

/// Gives `owner` a hold on a filter's bucket, or replaces the value of the
/// one it has.
fn put<V>(held: &mut Vec<Held<V>>, owner: usize, value: V) -> Option<V> {
    match held.iter_mut().find(|held| held.owner == owner) {
        Some(held) => Some(std::mem::replace(&mut held.value, value)),
        None => {
            held.push(Held { owner, value });
            None
        }
    }
}

fn take<V>(held: &mut Vec<Held<V>>, owner: usize) -> Option<V> {
    let at = held.iter().position(|held| held.owner == owner)?;
    Some(held.swap_remove(at).value)
}

/// [`take`] from the bucket `key` of `map`, which goes when it empties.
fn take_from<V>(map: &mut HashMap<Box<str>, Vec<Held<V>>>, key: &str, owner: usize) -> Option<V> {
    let held = map.get_mut(key)?;
    let value = take(held, owner)?;
    if held.is_empty() {
        map.remove(key);
    }
    Some(value)
}

/// What the application owns for one publisher path, as opposed to what
/// peers ask of it.
///
/// Made by the first publish that finds a subscriber and kept for as long as
/// the registry lives: the paths are the application's to choose, and a
/// sequence that restarted whenever the last subscriber left would read as
/// silence to a connection that stayed.
struct PubPath {
    path: Arc<str>,
    /// Copies dropped for a subscriber that could not take them, by topic
    /// and cause.
    drops: Arc<DropTable>,
    /// Numbers published messages per topic **on this path**: two publishers
    /// of one listener that use one topic string are two producers, and a
    /// subscriber of one must not see the other's numbers as holes in its
    /// own. The number belongs to the *message*, not to a subscriber's copy,
    /// which is what makes a dropped copy visible as a gap: the survivors
    /// keep the numbers the lost ones would have had.
    sequencer: Sequencer,
}

/// One subscribing connection's state for one publisher path.
struct SubEntry {
    /// For the writer task, which starts with the first copy. Weak: the
    /// registry must not keep a connection alive, and `remove_connection` is
    /// what ends the entry.
    conn: Weak<ConnCtx>,
    /// The runtime the connection runs on: the clock of a bounded wait on
    /// this subscriber.
    exec: Exec,
    /// What the connection holds on this path, for repeats and for
    /// withdrawing; [`PathState::index`] is what selects by it.
    filters: HashSet<Box<str>>,
    /// The connection's byte budget, shared by all its paths.
    budget: Arc<Semaphore>,
    /// The queue of this subscriber's writer, made with the first copy: a
    /// subscription that nothing is ever published to costs no channel and no
    /// task. Made again if the writer is gone and the connection is not.
    writer: Mutex<Option<mpsc::Sender<PubItem>>>,
}

impl SubEntry {
    /// The queue of this subscriber's writer, starting the writer when there
    /// is none. `None` when the connection is gone.
    fn sender(&self, publisher: &PubPath) -> Option<mpsc::Sender<PubItem>> {
        let mut slot = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if !slot.as_ref().is_some_and(|tx| !tx.is_closed()) {
            *slot = self.start_writer(publisher);
        }
        slot.clone()
    }

    /// Queues `item` for this subscriber's writer; `false` when the queue is
    /// full or the connection gone.
    fn send(&self, publisher: &PubPath, item: PubItem) -> bool {
        let mut slot = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if !slot.as_ref().is_some_and(|tx| !tx.is_closed()) {
            *slot = self.start_writer(publisher);
        }
        slot.as_ref().is_some_and(|tx| tx.try_send(item).is_ok())
    }

    /// One writer task per (connection, path): it serializes this
    /// subscriber's messages, which is what makes delivery FIFO per
    /// subscriber even though each message rides its own QUIC stream.
    fn start_writer(&self, publisher: &PubPath) -> Option<mpsc::Sender<PubItem>> {
        let ctx = self.conn.upgrade()?;
        if ctx.conn.close_reason().is_some() {
            return None;
        }
        let (tx, rx) = mpsc::channel(WRITER_QUEUE);
        self.exec.spawn(writer(
            ctx,
            Arc::clone(&publisher.path),
            rx,
            Arc::clone(&self.budget),
            Arc::clone(&publisher.drops),
        ));
        Some(tx)
    }
}

/// The subscriptions of every connection to one publisher path.
///
/// Present only while a connection holds a filter on the path: the last one
/// leaving removes it, so a peer that names paths nobody serves holds memory
/// only for as long as it is connected.
struct PathState {
    subs: HashMap<usize, SubEntry>,
    /// Which connections' filters select a topic.
    index: FilterIndex<()>,
}

impl PathState {
    fn new() -> PathState {
        PathState {
            subs: HashMap::new(),
            index: FilterIndex::new(),
        }
    }

    /// The connections holding at least one filter that selects `topic`,
    /// each once.
    fn selecting(&self, topic: &str) -> Vec<usize> {
        let mut owners = Vec::new();
        self.index.matching(topic, |owner, ()| owners.push(owner));
        owners.sort_unstable();
        owners.dedup();
        owners
    }
}

/// What one connection holds, over every path and every kind of
/// subscription. Made by the connection's first subscription and kept until
/// it closes.
#[derive(Default)]
struct ConnState {
    /// Filters held — on publisher paths, as radio joins and as queue
    /// consumers alike — for `Limits::max_subscriptions`.
    count: usize,
    /// The publisher paths it holds filters on, so that closing it visits
    /// those and no others.
    paths: HashSet<Arc<str>>,
    /// Payload bytes publishers may hold queued for it, over all its paths:
    /// a subscriber is a connection, however many paths it listens on. Kept
    /// for the connection's life, so that subscribing and withdrawing again
    /// does not hand it a fresh budget beside the bytes the old one still
    /// has queued.
    budget: Option<Arc<Semaphore>>,
}

/// Subscriptions for every publisher path served by one listener.
pub(crate) struct SubRegistry {
    /// What peers asked for: a path is here only while some connection holds
    /// a filter on it.
    paths: RwLock<HashMap<Arc<str>, PathState>>,
    conns: RwLock<HashMap<usize, ConnState>>,
    /// What the application owns per path.
    publishers: RwLock<HashMap<Arc<str>, Arc<PubPath>>>,
    limits: Limits,
    ordering: OrderingMode,
    /// Names one streamed publish, so a writer can tell the chunks of two
    /// concurrent ones apart. Local and never on the wire: what identifies a
    /// transfer to a subscriber is the stream it arrives on.
    next_stream: AtomicU64,
}

impl SubRegistry {
    pub(crate) fn new(limits: Limits, ordering: OrderingMode) -> SubRegistry {
        SubRegistry {
            paths: RwLock::new(HashMap::new()),
            conns: RwLock::new(HashMap::new()),
            publishers: RwLock::new(HashMap::new()),
            limits,
            ordering,
            next_stream: AtomicU64::new(0),
        }
    }

    /// The most payload bytes one subscriber's budget can hold: the
    /// configured bound, within what a semaphore can count.
    fn max_chunk(&self) -> usize {
        self.limits
            .subscriber_buffer_bytes
            .min(Semaphore::MAX_PERMITS)
    }

    /// The record the application keeps for `path`, made by the first publish
    /// that needs it.
    fn publisher(&self, path: &str) -> Arc<PubPath> {
        if let Some(publisher) = self
            .publishers
            .read()
            .expect("subscription lock poisoned")
            .get(path)
        {
            return Arc::clone(publisher);
        }
        let mut publishers = self.publishers.write().expect("subscription lock poisoned");
        if let Some(publisher) = publishers.get(path) {
            return Arc::clone(publisher);
        }
        let path: Arc<str> = Arc::from(path);
        let publisher = Arc::new(PubPath {
            path: Arc::clone(&path),
            drops: Arc::new(DropTable::new(self.limits.max_sequence_scopes)),
            sequencer: Sequencer::new(self.ordering, self.limits.max_sequence_scopes),
        });
        publishers.insert(path, Arc::clone(&publisher));
        publisher
    }

    /// Records interest of `ctx` in `filter` on `path`.
    ///
    /// The path need not be served by a publisher yet: a subscriber may
    /// arrive first, and the subscription is bounded by `max_subscriptions`
    /// either way. Duplicate filters are idempotent. A connection that is
    /// already closed registers nothing: nothing would remove what it
    /// registered.
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
        let mut conns = self.conns.write().expect("subscription lock poisoned");

        // `remove_connection` runs once the close is visible, and this check
        // is under the lock it takes: a connection that is closed here has
        // had it run or has it queued behind this call, so an entry made now
        // would be one nothing ever removes.
        if ctx.conn.close_reason().is_some() {
            return Ok(());
        }
        let held = paths
            .get(path)
            .and_then(|state| state.subs.get(&conn_id))
            .is_some_and(|entry| entry.filters.contains(filter.as_str()));
        if held {
            return Ok(());
        }
        let conn = conns.entry(conn_id).or_default();
        if conn.count >= self.limits.max_subscriptions {
            return Err(Error::LimitExceeded);
        }
        let budget = Arc::clone(
            conn.budget
                .get_or_insert_with(|| Arc::new(Semaphore::new(self.max_chunk()))),
        );

        let key: Arc<str> = match paths.get_key_value(path) {
            Some((key, _)) => Arc::clone(key),
            None => Arc::from(path),
        };
        let state = paths.entry(Arc::clone(&key)).or_insert_with(PathState::new);
        let entry = state.subs.entry(conn_id).or_insert_with(|| SubEntry {
            conn: Arc::downgrade(ctx),
            exec: ctx.exec.clone(),
            filters: HashSet::new(),
            budget,
            writer: Mutex::new(None),
        });
        entry.filters.insert(filter.as_str().into());
        state.index.insert(conn_id, &filter, ());
        conn.count += 1;
        conn.paths.insert(key);
        Ok(())
    }

    /// Counts one subscription against `max_subscriptions` without recording
    /// a filter.
    ///
    /// For a subscription this registry does not serve: an L2 queue keeps its
    /// consumers itself, but the bound is per connection over *all* paths, so
    /// the count has to stay in one place or it bounds nothing
    /// (`docs/PROTOCOL.md` §10). A connection that is already closed is not
    /// counted, for the reason [`SubRegistry::subscribe`] gives.
    pub(crate) fn reserve(&self, ctx: &ConnHandle) -> Result<(), Error> {
        let mut conns = self.conns.write().expect("subscription lock poisoned");
        if ctx.conn.close_reason().is_some() {
            return Ok(());
        }
        let conn = conns.entry(ctx.conn.stable_id()).or_default();
        if conn.count >= self.limits.max_subscriptions {
            return Err(Error::LimitExceeded);
        }
        conn.count += 1;
        Ok(())
    }

    /// Gives one reserved subscription back. An unknown connection is ignored:
    /// UNSUBSCRIBE is idempotent.
    pub(crate) fn release(&self, conn_id: usize) {
        let mut conns = self.conns.write().expect("subscription lock poisoned");
        if let Some(conn) = conns.get_mut(&conn_id) {
            conn.count = conn.count.saturating_sub(1);
        }
    }

    /// Withdraws one filter. An unknown filter, path or connection is ignored:
    /// UNSUBSCRIBE is idempotent and races legitimately with teardown.
    pub(crate) fn unsubscribe(&self, path: &str, conn_id: usize, filter: &str) {
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut conns = self.conns.write().expect("subscription lock poisoned");
        let Some(state) = paths.get_mut(path) else {
            return;
        };
        let Some(entry) = state.subs.get_mut(&conn_id) else {
            return;
        };
        if !entry.filters.remove(filter) {
            return;
        }
        let emptied = entry.filters.is_empty();
        state.index.remove(conn_id, filter);
        let conn = conns.get_mut(&conn_id);
        if let Some(conn) = conn {
            conn.count = conn.count.saturating_sub(1);
            if emptied {
                conn.paths.remove(path);
            }
        }
        if emptied {
            // The writer task ends when its channel closes, after what was
            // queued for it is written.
            state.subs.remove(&conn_id);
            if state.subs.is_empty() {
                paths.remove(path);
            }
        }
    }

    /// Drops every subscription held by one connection. Called when the
    /// connection closes, so a churn of peers cannot grow the registry: the
    /// work is the connection's own paths and filters, and a path left with
    /// no subscriber goes with it.
    pub(crate) fn remove_connection(&self, conn_id: usize) {
        // Most connections never subscribe to anything here, and none of
        // them is worth the write lock a publisher would wait behind.
        if !self
            .conns
            .read()
            .expect("subscription lock poisoned")
            .contains_key(&conn_id)
        {
            return;
        }
        let mut paths = self.paths.write().expect("subscription lock poisoned");
        let mut conns = self.conns.write().expect("subscription lock poisoned");
        let Some(conn) = conns.remove(&conn_id) else {
            return;
        };
        for path in &conn.paths {
            let Some(state) = paths.get_mut(path.as_ref()) else {
                continue;
            };
            if let Some(entry) = state.subs.remove(&conn_id) {
                for filter in &entry.filters {
                    state.index.remove(conn_id, filter);
                }
            }
            if state.subs.is_empty() {
                paths.remove(path.as_ref());
            }
        }
    }

    /// Fans `payload` out to every subscriber of `path` whose filter matches
    /// `topic`, and returns how many it was enqueued to.
    ///
    /// Synchronous and non-blocking by design: a subscriber that cannot take
    /// the bytes right now loses this message rather than stalling everyone
    /// else. `want` is the payload length in permits.
    ///
    /// The header of a message is the same for every subscriber, so it is
    /// encoded once here and each copy writes the same bytes.
    ///
    /// Fails with [`Error::LimitExceeded`], before anything is queued or
    /// written, for a topic above 256 bytes: a subscriber's decoder refuses
    /// it, and answers by closing the connection every other subscription on
    /// it shares.
    pub(crate) fn publish(
        &self,
        path: &str,
        topic: &str,
        payload: Bytes,
        trace: Option<TraceContext>,
        want: u32,
    ) -> Result<usize, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let paths = self.paths.read().expect("subscription lock poisoned");
        let Some(state) = paths.get(path) else {
            return Ok(0);
        };
        let publisher = self.publisher(path);

        let topic: Arc<str> = Arc::from(topic);
        // Before fan-out, so every copy of one message carries one number and
        // a copy dropped below leaves a hole rather than renumbering.
        let sequence = publisher.sequencer.next(&topic);
        let owners = state.selecting(&topic);
        if owners.is_empty() {
            return Ok(0);
        }
        let frame = encode_frame(path, &topic, Some(payload.len() as u64), trace, sequence)?;
        let mut sent = 0usize;
        for owner in owners {
            let Some(entry) = state.subs.get(&owner) else {
                continue;
            };
            let Ok(permit) = entry.budget.try_acquire_many(want) else {
                publisher.drops.record(&topic, DropCause::SubscriberBudget);
                tracing::debug!(path, %topic, "subscriber budget exhausted; message dropped");
                continue;
            };
            let msg = PubMsg {
                topic: Arc::clone(&topic),
                payload: payload.clone(),
                frame: frame.clone(),
            };
            if entry.send(&publisher, PubItem::Whole(msg)) {
                // The writer returns the permits once the bytes are gone.
                permit.forget();
                sent += 1;
            } else {
                // Full and Closed alike. Dropping `permit` returns the budget
                // immediately.
                publisher.drops.record(&topic, DropCause::SubscriberQueue);
                tracing::debug!(path, %topic, "subscriber queue full; message dropped");
            }
        }
        Ok(sent)
    }

    /// Opens a streamed publish: one stream per matched subscriber, written
    /// chunk by chunk and never materialized whole.
    ///
    /// The subscriber set is fixed here, at `open`, because a stream is a
    /// stream: a subscriber that arrives mid-payload would receive a fragment
    /// and could not be told where it started. It gets the next message.
    ///
    /// A topic above 256 bytes cannot be sent to anyone: the fan-out this
    /// returns reaches nobody and refuses every write with
    /// [`Error::LimitExceeded`].
    pub(crate) fn open(&self, path: &str, topic: &str, trace: Option<TraceContext>) -> FanOut {
        let id = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let topic: Arc<str> = Arc::from(topic);
        let max_chunk = self.max_chunk();
        if topic.len() > MAX_TOPIC_BYTES {
            return FanOut::empty(id, topic, max_chunk, true);
        }
        let paths = self.paths.read().expect("subscription lock poisoned");
        let Some(state) = paths.get(path) else {
            return FanOut::empty(id, topic, max_chunk, false);
        };
        let publisher = self.publisher(path);
        // One number for the message, as `publish` does: a subscriber whose
        // copy is aborted below leaves a hole rather than renumbering.
        let sequence = publisher.sequencer.next(&topic);
        let owners = state.selecting(&topic);
        let mut targets = Vec::new();
        let mut exec = None;
        if !owners.is_empty() {
            let Ok(frame) = encode_frame(path, &topic, None, trace, sequence) else {
                return FanOut::empty(id, topic, max_chunk, true);
            };
            for owner in owners {
                let Some(entry) = state.subs.get(&owner) else {
                    continue;
                };
                // The permit for the ending — `Finish` or `Abort` — is taken
                // before the first chunk, so ending a transfer can never fail
                // for want of queue room. Without it a subscriber whose queue
                // filled mid-payload would hold an open stream waiting for a
                // FIN nobody could enqueue.
                let Some(tx) = entry.sender(&publisher) else {
                    publisher.drops.record(&topic, DropCause::SubscriberQueue);
                    continue;
                };
                let Ok(ending) = tx.clone().try_reserve_owned() else {
                    publisher.drops.record(&topic, DropCause::SubscriberQueue);
                    continue;
                };
                let msg = PubMsg {
                    topic: Arc::clone(&topic),
                    payload: Bytes::new(),
                    frame: frame.clone(),
                };
                if tx.try_send(PubItem::Begin { id, msg }).is_err() {
                    publisher.drops.record(&topic, DropCause::SubscriberQueue);
                    continue;
                }
                exec.get_or_insert_with(|| entry.exec.clone());
                targets.push(Target {
                    tx,
                    budget: Arc::clone(&entry.budget),
                    ending: Some(ending),
                });
            }
        }
        FanOut {
            id,
            topic,
            targets,
            drops: Some(Arc::clone(&publisher.drops)),
            exec,
            max_chunk,
            refused: false,
            interrupted: false,
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
            .map_or(0, |s| s.index.len())
    }

    /// Messages dropped on `path` because a subscriber could not take them,
    /// summed over topics and causes.
    pub(crate) fn dropped(&self, path: &str) -> u64 {
        self.publishers
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or(0, |p| p.drops.total())
    }

    /// The drops on `path` for one topic, or `None` if none was counted.
    pub(crate) fn dropped_on(&self, path: &str, topic: &str) -> Option<TopicDrops> {
        self.publishers
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .and_then(|p| p.drops.on_topic(topic))
    }

    /// The drops on `path`, one entry per topic that lost a copy.
    pub(crate) fn drops(&self, path: &str) -> Vec<TopicDrops> {
        self.publishers
            .read()
            .expect("subscription lock poisoned")
            .get(path)
            .map_or_else(Vec::new, |p| p.drops.by_topic())
    }

    /// Paths some connection holds a filter on.
    #[cfg(test)]
    fn path_count(&self) -> usize {
        self.paths.read().expect("subscription lock poisoned").len()
    }

    /// Connections the registry holds a record for.
    #[cfg(test)]
    fn conn_count(&self) -> usize {
        self.conns.read().expect("subscription lock poisoned").len()
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
    /// The clock a bounded wait is measured on: the runtime the subscribers'
    /// connections run on, so that no timer is made on the caller's thread.
    /// `None` exactly when there is no subscriber to wait for.
    exec: Option<Exec>,
    /// The largest chunk a subscriber's budget can ever hold.
    max_chunk: usize,
    /// `open` found the topic above the DATA header's cap: nothing was
    /// opened, and every write fails.
    refused: bool,
    /// A [`FanOut::write_within`] was dropped while it waited: the chunk is
    /// queued for some copies and not for the rest, and the call that knew
    /// which is gone, so no copy may be finished. Set for the wait and
    /// cleared by the call that sees it through.
    interrupted: bool,
    finished: bool,
}

/// What one subscriber made of one chunk of a write.
enum Reach {
    /// Enqueued for the subscriber's writer.
    Queued,
    /// The budget had room and the writer's queue did not, or the writer is
    /// gone.
    QueueFull,
    /// The budget had no room, and none came back in time.
    NoRoom,
}

impl FanOut {
    /// A fan-out that reaches nobody: the path has no subscriber, or `open`
    /// `refused` the topic.
    fn empty(id: u64, topic: Arc<str>, max_chunk: usize, refused: bool) -> FanOut {
        FanOut {
            id,
            topic,
            targets: Vec::new(),
            drops: None,
            exec: None,
            max_chunk,
            refused,
            interrupted: false,
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
    /// to `limit` for those that have no room, and returns how many
    /// subscribers are left.
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
    /// **`limit` is for the whole call, not for each subscriber**: those
    /// with no room are waited for together, so a thousand stalled
    /// subscribers cost one `limit`. The wait is timed on the runtime the
    /// subscribers' connections run on, so this future may be driven from
    /// any executor.
    ///
    /// One `Bytes` allocation is shared by every copy, so a chunk costs one
    /// buffer regardless of subscriber count, and the payload is never held
    /// whole anywhere.
    ///
    /// **A call that is dropped while it waits abandons the transfer.** By
    /// then the chunk is queued for the subscribers that had room and not for
    /// the rest, and the outcome that said which is gone with the call. No
    /// copy may be finished after that, so later writes fail with
    /// [`Error::Canceled`] and [`FanOut::finish`] resets every copy and
    /// counts none.
    ///
    /// Fails with [`Error::LimitExceeded`] for a chunk larger than
    /// `Limits::subscriber_buffer_bytes`: such a chunk could never be
    /// enqueued for anybody, and the point of this API is that the *payload*
    /// need not fit while a chunk does. Nothing is written or aborted then.
    pub async fn write_within(
        &mut self,
        chunk: impl Into<Bytes>,
        limit: Duration,
    ) -> Result<usize, Error> {
        let chunk = chunk.into();
        let want = self.want(&chunk)?;
        // The targets stay in `self` until the chunk's outcome is known for
        // all of them, so a call that is dropped while it waits still leaves
        // every copy to the fan-out's drop, which aborts it.
        let mut reach = Vec::with_capacity(self.targets.len());
        let mut waiting = Vec::new();
        for (at, target) in self.targets.iter().enumerate() {
            match Arc::clone(&target.budget).try_acquire_many_owned(want) {
                Ok(permit) => reach.push(self.queue_chunk(target, permit, &chunk)),
                Err(_) => {
                    reach.push(Reach::NoRoom);
                    waiting.push(at);
                }
            }
        }
        if !waiting.is_empty() {
            // From here on the chunk is queued for some copies and not for the
            // rest, and only this call knows which. If it is dropped while it
            // waits, the flag stays set and the transfer cannot be finished.
            self.interrupted = true;
            self.wait_for_room(&waiting, &mut reach, want, limit, &chunk)
                .await;
            self.interrupted = false;
        }
        let mut kept = Vec::with_capacity(self.targets.len());
        for (mut target, reach) in std::mem::take(&mut self.targets).into_iter().zip(reach) {
            match reach {
                Reach::Queued => kept.push(target),
                Reach::QueueFull => self.abort_one(&mut target, DropCause::SubscriberQueue),
                Reach::NoRoom => self.abort_one(&mut target, DropCause::SubscriberBudget),
            }
        }
        self.targets = kept;
        Ok(self.targets.len())
    }

    /// Waits, all at once and under one deadline, for the budget of every
    /// target in `waiting` to have `want` bytes of room, and queues `chunk`
    /// for each as its room comes. What is still waiting at `limit` keeps
    /// [`Reach::NoRoom`].
    async fn wait_for_room(
        &self,
        waiting: &[usize],
        reach: &mut [Reach],
        want: u32,
        limit: Duration,
        chunk: &Bytes,
    ) {
        let Some(exec) = self.exec.as_ref() else {
            return;
        };
        let deadline = exec.sleep(limit);
        tokio::pin!(deadline);
        type Room = Pin<Box<dyn Future<Output = Option<OwnedSemaphorePermit>> + Send>>;
        let mut acquiring: Vec<(usize, Room)> = waiting
            .iter()
            .map(|&at| {
                let budget = Arc::clone(&self.targets[at].budget);
                let tx = self.targets[at].tx.clone();
                let room: Room = Box::pin(async move {
                    tokio::select! {
                        permit = budget.acquire_many_owned(want) => permit.ok(),
                        // The writer task is gone — the connection closed —
                        // so no permit will ever come back. Without this arm
                        // the wait would run to `limit` for a subscriber
                        // that cannot exist.
                        () = tx.closed() => None,
                    }
                });
                (at, room)
            })
            .collect();
        std::future::poll_fn(|cx| {
            let mut i = 0;
            while i < acquiring.len() {
                match acquiring[i].1.as_mut().poll(cx) {
                    Poll::Ready(permit) => {
                        let (at, _) = acquiring.swap_remove(i);
                        if let Some(permit) = permit {
                            reach[at] = self.queue_chunk(&self.targets[at], permit, chunk);
                        }
                    }
                    Poll::Pending => i += 1,
                }
            }
            if acquiring.is_empty() || deadline.as_mut().poll(cx).is_ready() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
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
    ///
    /// Fails with [`Error::LimitExceeded`] for a chunk larger than
    /// `Limits::subscriber_buffer_bytes`, as [`FanOut::write_within`] does,
    /// and with [`Error::Canceled`] after a `write_within` that was dropped
    /// while it waited.
    pub fn write_now(&mut self, chunk: impl Into<Bytes>) -> Result<usize, Error> {
        let chunk = chunk.into();
        let want = self.want(&chunk)?;
        let mut kept = Vec::with_capacity(self.targets.len());
        for mut target in std::mem::take(&mut self.targets) {
            let Ok(permit) = Arc::clone(&target.budget).try_acquire_many_owned(want) else {
                self.abort_one(&mut target, DropCause::SubscriberBudget);
                continue;
            };
            match self.queue_chunk(&target, permit, &chunk) {
                Reach::Queued => kept.push(target),
                _ => self.abort_one(&mut target, DropCause::SubscriberQueue),
            }
        }
        self.targets = kept;
        Ok(self.targets.len())
    }

    /// The chunk's size in budget permits, or the refusal that spares every
    /// copy a wait that could never end, or a write onto a transfer that a
    /// dropped `write_within` left with some copies short of a chunk.
    fn want(&self, chunk: &Bytes) -> Result<u32, Error> {
        if self.interrupted {
            return Err(Error::Canceled);
        }
        if self.refused || chunk.len() > self.max_chunk {
            return Err(Error::LimitExceeded);
        }
        u32::try_from(chunk.len()).map_err(|_| Error::LimitExceeded)
    }

    /// Hands one chunk, whose budget `permit` is held, to one subscriber's
    /// writer. The writer returns the permits once the bytes are gone, so
    /// they are forgotten here; a queue that is full or gone gives them back
    /// at once.
    fn queue_chunk(&self, target: &Target, permit: OwnedSemaphorePermit, chunk: &Bytes) -> Reach {
        let item = PubItem::Chunk {
            id: self.id,
            payload: chunk.clone(),
        };
        match target.tx.try_send(item) {
            Ok(()) => {
                permit.forget();
                Reach::Queued
            }
            Err(_) => Reach::QueueFull,
        }
    }

    /// Ends the transfer, and returns how many subscribers received all of
    /// it as far as this side can tell.
    ///
    /// "As far as this side can tell" is the honest claim: the count is the
    /// subscribers whose every chunk was enqueued and whose FIN is queued
    /// behind them. A fan-out copy carries no receipt — nobody holds a
    /// `Delivery` for it — so the transport acknowledgement is awaited by the
    /// drain and by nothing else (`docs/decisions/0009-drain.md` §4.2).
    ///
    /// A fan-out whose [`FanOut::write_within`] was dropped while it waited
    /// resets every copy instead and counts none.
    pub fn finish(mut self) -> usize {
        if self.interrupted {
            // Not finished, so the drop below aborts every copy.
            return 0;
        }
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

/// Writes one subscriber's messages, in order, each on its own uni stream.
///
/// Fan-out is one-way by construction: a published copy owes nothing back, so
/// it rides a unidirectional stream and the publisher never waits on it.
///
/// A copy the subscriber refuses — `STOP_SENDING` from a receiver that
/// dropped the transfer unread, or a header its `max_header_bytes` cannot
/// take — is that copy's end and nothing more. The subscription belongs to
/// the connection, so the writer ends only when the connection does.
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
    // Registered once for the task: registering takes the connection's state
    // lock, which a fast publisher would otherwise pay for on every message.
    let closed = ctx.conn.closed();
    tokio::pin!(closed);
    loop {
        let item = tokio::select! {
            item = rx.recv() => match item {
                Some(item) => item,
                None => break,
            },
            // Without this a subscriber on a dead connection would sit here
            // forever holding an `Arc<ConnCtx>`.
            _ = &mut closed => break,
        };
        match item {
            PubItem::Whole(msg) => {
                let outcome = write_one(&ctx, &msg).await;
                budget.add_permits(msg.payload.len());
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
                        if ctx.conn.close_reason().is_some() {
                            tracing::debug!(path = %path, error = %e, "connection closed; subscriber writer ending");
                            break;
                        }
                        tracing::debug!(path = %path, error = %e, "fan-out copy refused; subscription kept");
                    }
                }
            }
            PubItem::Begin { id, msg } => match begin_one(&ctx, &msg).await {
                Ok(stream) => {
                    streaming.insert(id, stream);
                }
                Err(Error::NoParkedConnection) => {
                    drops.record(&msg.topic, DropCause::NoParkedConnection);
                    tracing::debug!(path = %path, "no parked connection; streamed copy dropped");
                }
                Err(e) => {
                    if ctx.conn.close_reason().is_some() {
                        tracing::debug!(path = %path, error = %e, "connection closed; subscriber writer ending");
                        break;
                    }
                    // No stream was opened, so this transfer's chunks find
                    // none below and are dropped with their budget returned.
                    tracing::debug!(path = %path, error = %e, "streamed fan-out copy refused; subscription kept");
                }
            },
            PubItem::Chunk { id, payload } => {
                let len = payload.len();
                // A chunk for a transfer whose stream is gone — the open
                // failed, or a write did — is dropped here: the budget is
                // still returned, because the publisher charged it.
                if let Some(stream) = streaming.get_mut(&id)
                    && let Err(e) = stream.write_chunk(payload).await
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
async fn begin_one(ctx: &ConnHandle, msg: &PubMsg) -> Result<SendHalf, Error> {
    msg.frame.fits(ctx)?;
    let mut stream = ctx.open_uni().await?;
    stream.write_chunk(msg.frame.bytes.clone()).await?;
    Ok(stream)
}

async fn write_one(ctx: &ConnHandle, msg: &PubMsg) -> Result<(), Error> {
    msg.frame.fits(ctx)?;
    let mut stream = ctx.open_uni().await?;
    // Both are shared buffers: the header the publisher encoded once and the
    // payload it was handed, so N subscribers cost N reference counts and
    // not N copies.
    stream.write_chunk(msg.frame.bytes.clone()).await?;
    if !msg.payload.is_empty() {
        stream.write_chunk(msg.payload.clone()).await?;
    }
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

    // --- the filter index ---

    /// Every string of one to `max` segments over an alphabet that has both
    /// wildcards and the empty segment, joined as a filter or a topic is.
    fn words(max: usize) -> Vec<String> {
        let alphabet = ["a", "b", "", "*", "#"];
        let mut all = Vec::new();
        let mut layer: Vec<Vec<&str>> = vec![Vec::new()];
        for _ in 0..max {
            layer = layer
                .iter()
                .flat_map(|prefix| {
                    alphabet.iter().map(move |segment| {
                        let mut next = prefix.clone();
                        next.push(*segment);
                        next
                    })
                })
                .collect();
            all.extend(layer.iter().map(|segments| segments.join(".")));
        }
        all
    }

    /// What `index` says selects `topic`, sorted; and whether it said any
    /// owner twice, which it must not for owners that hold one filter each.
    fn selected(index: &FilterIndex<usize>, topic: &str) -> Vec<usize> {
        let mut owners = Vec::new();
        index.matching(topic, |owner, value| {
            assert_eq!(*value, owner * 10, "the value travels with its filter");
            owners.push(owner);
        });
        owners.sort_unstable();
        assert!(
            owners.windows(2).all(|pair| pair[0] != pair[1]),
            "{topic:?} reported an owner twice: {owners:?}"
        );
        owners
    }

    fn scanned(filters: &[String], held: impl Fn(usize) -> bool, topic: &str) -> Vec<usize> {
        filters
            .iter()
            .enumerate()
            .filter(|(owner, filter)| held(*owner) && filter::matches(topic, filter))
            .map(|(owner, _)| owner)
            .collect()
    }

    #[test]
    fn the_index_selects_exactly_what_the_matcher_selects() {
        // Every filter of up to three segments, the empty one, the wildcards
        // and the filters the grammar forbids included, against every topic
        // of up to four segments, topics that contain `*` and `#` as
        // ordinary bytes included.
        let filters = words(3);
        let topics = words(4);
        let mut index = FilterIndex::new();
        for (owner, filter) in filters.iter().enumerate() {
            assert!(index.insert(owner, filter, owner * 10).is_none());
        }
        assert_eq!(index.len(), filters.len());
        for topic in &topics {
            assert_eq!(
                selected(&index, topic),
                scanned(&filters, |_| true, topic),
                "topic {topic:?}"
            );
        }

        // Withdrawing every other filter leaves exactly the others.
        for (owner, filter) in filters
            .iter()
            .enumerate()
            .filter(|(owner, _)| owner % 2 == 0)
        {
            assert_eq!(index.remove(owner, filter), Some(owner * 10));
            assert_eq!(
                index.remove(owner, filter),
                None,
                "withdrawing is idempotent"
            );
        }
        assert_eq!(index.len(), filters.len() / 2);
        for topic in &topics {
            assert_eq!(
                selected(&index, topic),
                scanned(&filters, |owner| owner % 2 == 1, topic),
                "topic {topic:?} after withdrawals"
            );
        }

        for (owner, filter) in filters
            .iter()
            .enumerate()
            .filter(|(owner, _)| owner % 2 == 1)
        {
            index.remove(owner, filter);
        }
        assert_eq!(index.len(), 0);
        assert!(selected(&index, "a.b").is_empty());
    }

    #[test]
    fn an_owner_holding_a_filter_again_replaces_what_it_carried() {
        let mut index = FilterIndex::new();
        for filter in ["px.eur", "px.#", "px.*", "#"] {
            assert_eq!(index.insert(7, filter, 1), None);
            assert_eq!(index.insert(7, filter, 2), Some(1), "{filter}");
        }
        assert_eq!(index.len(), 4);
        let mut seen = Vec::new();
        index.matching("px.eur", |owner, value| seen.push((owner, *value)));
        assert_eq!(seen.len(), 4, "once per filter held");
        assert!(seen.iter().all(|&(owner, value)| owner == 7 && value == 2));
    }

    #[test]
    fn only_wildcard_filters_are_tested_one_by_one() {
        let mut index = FilterIndex::new();
        for owner in 0..1000 {
            index.insert(owner, &format!("px.sym{owner}"), ());
            index.insert(owner, &format!("fx.sym{owner}.#"), ());
            index.insert(owner, "audit.#", ());
        }
        assert_eq!(index.len(), 3000);
        assert_eq!(index.tested_one_by_one(), 0);
        index.insert(1, "px.*", ());
        assert_eq!(index.tested_one_by_one(), 1);
    }

    // --- the registry ---

    fn registry(limits: Limits) -> Arc<SubRegistry> {
        Arc::new(SubRegistry::new(limits, OrderingMode::None))
    }

    /// A connection the registry can hold: the accepted end of an in-process
    /// link. The dialled end comes back too, because dropping it ends the
    /// link.
    async fn connection(
        registry: &Arc<SubRegistry>,
        bus: &str,
    ) -> (ConnHandle, crate::inproc::LocalConn) {
        let mut dials = crate::inproc::bind(bus).expect("bus");
        let dialled = crate::inproc::dial(bus, 255, 1024).expect("dial");
        let accepted = dials.recv().await.expect("accepted end");
        let shared = Arc::new(crate::runtime::Shared {
            duplicates: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            drain: crate::drain::DrainState::new(),
        });
        let ctx = ConnCtx::spawn(
            crate::transport::Link::Local(accepted),
            Limits::default(),
            Arc::new(crate::listener::Namespace::new()),
            Some(Arc::clone(registry)),
            Exec::current().expect("a tokio runtime"),
            weida_protocol::header::GuaranteeSet::CORE,
            shared,
        );
        (ctx, dialled)
    }

    #[tokio::test]
    async fn a_peer_cannot_grow_the_registry_by_naming_paths() {
        let registry = registry(Limits::default());
        for round in 0..20 {
            let (ctx, _dialled) =
                connection(&registry, &format!("pubsub-unit-names-{round}")).await;
            for path in 0..256 {
                registry
                    .subscribe(&format!("/p{round}/{path}"), &ctx, "#".to_owned())
                    .expect("within max_subscriptions");
            }
            assert!(
                matches!(
                    registry.subscribe("/one/too/many", &ctx, "#".to_owned()),
                    Err(Error::LimitExceeded)
                ),
                "the 257th filter of a connection is refused"
            );
            assert_eq!(registry.path_count(), 256);

            registry.remove_connection(ctx.conn.stable_id());
            assert_eq!(registry.path_count(), 0, "round {round}: no path is left");
            assert_eq!(registry.conn_count(), 0);
        }
    }

    #[tokio::test]
    async fn a_path_goes_with_its_last_filter() {
        let registry = registry(Limits::default());
        let (one, _one) = connection(&registry, "pubsub-unit-last-filter-1").await;
        let (two, _two) = connection(&registry, "pubsub-unit-last-filter-2").await;
        registry.subscribe("/md", &one, "px.#".to_owned()).unwrap();
        registry.subscribe("/md", &one, "fx.#".to_owned()).unwrap();
        registry.subscribe("/md", &two, "px.#".to_owned()).unwrap();
        assert_eq!(registry.subscriber_count("/md"), 2);
        assert_eq!(registry.filter_count("/md"), 3);

        registry.unsubscribe("/md", one.conn.stable_id(), "px.#");
        registry.unsubscribe("/md", one.conn.stable_id(), "fx.#");
        assert_eq!(registry.subscriber_count("/md"), 1);
        assert_eq!(
            registry.path_count(),
            1,
            "another connection still holds it"
        );

        registry.unsubscribe("/md", two.conn.stable_id(), "px.#");
        assert_eq!(registry.path_count(), 0);
        // Withdrawing what is not held, from a path that is not there, is
        // nothing.
        registry.unsubscribe("/md", two.conn.stable_id(), "px.#");
        registry.unsubscribe("/never", two.conn.stable_id(), "#");
    }

    #[tokio::test]
    async fn a_connection_that_is_closed_registers_nothing() {
        let registry = registry(Limits::default());
        let (ctx, _dialled) = connection(&registry, "pubsub-unit-closed").await;
        ctx.conn
            .close(codes::SHUTDOWN, "closed before its subscription landed");

        // The caller does not close it again: nothing is wrong with the
        // frame, the connection is merely gone.
        registry.subscribe("/md", &ctx, "#".to_owned()).unwrap();
        registry.reserve(&ctx).unwrap();
        assert_eq!(registry.path_count(), 0);
        assert_eq!(
            registry.conn_count(),
            0,
            "nothing would remove a record made now"
        );
    }

    #[tokio::test]
    async fn a_subscribers_budget_is_one_for_all_its_paths() {
        let registry = registry(Limits {
            subscriber_buffer_bytes: 8192,
            ..Limits::default()
        });
        let (ctx, _dialled) = connection(&registry, "pubsub-unit-budget").await;
        for path in ["/a", "/b"] {
            registry.subscribe(path, &ctx, "#".to_owned()).unwrap();
        }
        // Nobody reads the other end of the link, so what the first publish
        // queued stays charged to the budget: the stream buffer is smaller
        // than the message.
        let message = Bytes::from(vec![7u8; 5000]);
        assert_eq!(
            registry
                .publish("/a", "t", message.clone(), None, 5000)
                .unwrap(),
            1
        );
        assert_eq!(
            registry.publish("/b", "t", message, None, 5000).unwrap(),
            0,
            "the second path draws on the same 8192 bytes"
        );
        assert_eq!(registry.dropped("/b"), 1);
        assert_eq!(registry.dropped("/a"), 0);
    }

    #[tokio::test]
    async fn a_topic_the_decoder_refuses_is_refused_before_anything_is_queued() {
        let registry = registry(Limits::default());
        let (ctx, _dialled) = connection(&registry, "pubsub-unit-long-topic").await;
        registry.subscribe("/md", &ctx, "#".to_owned()).unwrap();
        let topic = "t".repeat(MAX_TOPIC_BYTES + 1);
        assert!(matches!(
            registry.publish("/md", &topic, Bytes::from_static(b"x"), None, 1),
            Err(Error::LimitExceeded)
        ));
        let mut fan_out = registry.open("/md", &topic, None);
        assert_eq!(fan_out.subscribers(), 0);
        assert!(matches!(
            fan_out.write_now(Bytes::from_static(b"x")),
            Err(Error::LimitExceeded)
        ));
        let at_the_cap = "t".repeat(MAX_TOPIC_BYTES);
        assert_eq!(
            registry
                .publish("/md", &at_the_cap, Bytes::from_static(b"x"), None, 1)
                .unwrap(),
            1
        );
    }

    /// A `write_within` dropped while it waits has queued its chunk for the
    /// subscribers that had room and not for the rest. Finishing the transfer
    /// would hand the short ones a FIN after a hole, which they would take
    /// for a whole payload.
    #[tokio::test(start_paused = true)]
    async fn a_write_dropped_while_it_waited_leaves_nothing_to_finish() {
        let registry = registry(Limits {
            subscriber_buffer_bytes: 8192,
            ..Limits::default()
        });
        let (fast, _fast_end) = connection(&registry, "pubsub-unit-dropped-write-fast").await;
        let (slow, _slow_end) = connection(&registry, "pubsub-unit-dropped-write-slow").await;
        registry.subscribe("/md", &fast, "#".to_owned()).unwrap();
        registry.subscribe("/md", &slow, "#".to_owned()).unwrap();
        // Only the slow one subscribes here, and nobody reads the other end
        // of its link, so this message stays charged to its budget.
        registry
            .subscribe("/only-slow", &slow, "#".to_owned())
            .unwrap();
        let spent = Bytes::from(vec![7u8; 5000]);
        assert_eq!(
            registry
                .publish("/only-slow", "t", spent, None, 5000)
                .unwrap(),
            1
        );

        let mut fan_out = registry.open("/md", "t", None);
        assert_eq!(fan_out.subscribers(), 2);
        // The fast subscriber takes the chunk; the slow one has 3192 bytes of
        // room for 4000 and waits, longer than the caller will.
        let chunk = Bytes::from(vec![1u8; 4000]);
        let dropped = tokio::time::timeout(
            Duration::from_millis(100),
            fan_out.write_within(chunk, Duration::from_secs(60)),
        )
        .await;
        assert!(dropped.is_err(), "the write was still waiting for room");

        assert!(matches!(
            fan_out.write_now(Bytes::from_static(b"x")),
            Err(Error::Canceled)
        ));
        assert_eq!(fan_out.finish(), 0, "no copy is finished after a hole");
    }
}
