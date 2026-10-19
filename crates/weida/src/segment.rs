//! Segments: the L0 unit a radio fans out and a dialling `Peer` sends
//! ([decisions/0037](../../../docs/decisions/0037-layered-segments.md) §4.2).
//!
//! A **segment** is a unit whose parts may depend on each other and on
//! nothing earlier — a voice frame, a raw preview frame, a video GOP. It is
//! numbered per `(sender, path, topic)` in DATA key `13`. The machinery here
//! is shared by [`crate::Radio::segment`], one copy per joined dish, and
//! [`crate::Peer::segment`], one copy toward the bound path the peer dialled.
//!
//! A segment carries **layers** `0..MAX_SEGMENT_LAYERS`, ordered by
//! dependency: layer *k* may depend on layers below it and never above
//! (§4.3). Each `(copy, layer)` is its own uni stream, opened on that layer's
//! first chunk, with its own writer task and queue of [`COPY_QUEUE`] chunks;
//! the byte budget is per copy. Every cut removes a suffix of the layers, so
//! what a receiver gets is a prefix a decoder can use:
//!
//! * **the cut rule** — a copy without room for a chunk of layer *k* first
//!   cuts its layers above *k*, whose queued bytes return to the budget at
//!   once; still without room, in the budget or in layer *k*'s queue, it cuts
//!   *k* and above. Layer 0 cut means the copy lost the segment;
//! * **supersession** — opening segment *n+1* resets at once every layer of
//!   segment *n* on that topic whose writer had not finished it, or whose
//!   queued chunks the transport does not take at once, and every layer
//!   above it; a finished layer gets the time its path needs,
//!   `rtt + 50 ms + rtt * bytes / cwnd`, and is reset only if still
//!   unacknowledged then. A copy that follows upstream
//!   ([`SegmentTerms::follows_upstream`]) is treated as finished until a
//!   write has to wait. No other topic's copies are touched;
//! * **expiry** — the smaller of the sender's and the receiver's `max_age`,
//!   on the sender's clock from the segment's open.
//!
//! Each copy counts at most once in `TopicDrops::layers_cut` and at most once
//! under a whole-segment cause. Nothing here ever waits for a receiver:
//! [`Segment::write_layer`] is synchronous.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use weida_core::Error;
use weida_protocol::{DataHeader, codes};

use crate::conn::ConnHandle;
use crate::pubsub::{Causes, DropCause, DropTable, Frame};
use crate::transport::SendHalf;

/// How many layers a stream segment can carry: `0..=15`, the cap of DATA
/// key `14`
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.3).
pub const MAX_SEGMENT_LAYERS: u8 = 16;

/// Chunks one copy's layer may hold queued for its writer task. The real
/// bound is the receiver's byte budget; this keeps the channel from growing
/// on tiny chunks.
pub(crate) const COPY_QUEUE: usize = 64;

/// What a finished copy waits beyond one round trip before a successor
/// supersedes it: quinn's default `max_ack_delay` of 25 ms (the transport
/// parameter default, quinn-proto `transport_parameters.rs`; weida's
/// `transport_config` in `tls.rs` keeps it) plus scheduling margin.
const SUPERSEDE_SLACK: Duration = Duration::from_millis(50);

/// How long a finished copy of `sent` bytes may still take to be
/// acknowledged on a path with smoothed round trip `rtt` and congestion
/// window `cwnd`: the time the window needs to carry every byte, one round
/// trip for the receipt and [`SUPERSEDE_SLACK`]. `sent` bounds what is still
/// unsent from above, so a copy whose bytes are already out gets more than
/// it needs, never less.
fn finish_grace(rtt: Duration, cwnd: u64, sent: u64) -> Duration {
    let carry = Duration::try_from_secs_f64(rtt.as_secs_f64() * sent as f64 / cwnd.max(1) as f64)
        .unwrap_or(Duration::MAX);
    rtt.saturating_add(SUPERSEDE_SLACK).saturating_add(carry)
}

/// The `quinn` priority of a copy's stream for `layer`: layer-major, so
/// every base layer on a connection goes before any enhancement layer, and
/// the application's `priority` orders topics within a layer
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.4).
fn copy_priority(layer: u8, priority: i16) -> i32 {
    (15 - i32::from(layer)) * 65_536 + i32::from(priority)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How a segment is sent: what the sender decides for every copy of it
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.2).
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct SegmentTerms {
    /// The sender's latency budget: a copy still unfinished or
    /// unacknowledged this long after the segment opened is reset. At a
    /// radio the dish's own `max_age` applies too, and the smaller wins.
    pub max_age: Option<Duration>,
    /// Orders this segment's copies against other topics' copies of the
    /// same layer on one connection; higher goes first. Layer 0 of every
    /// topic still goes before any higher layer. A no-op on the local
    /// transports.
    pub priority: i16,
    /// The writer relays a segment its upstream is still streaming: the
    /// successor takes a copy's layer only where a write has to wait, and
    /// otherwise the layer keeps taking chunks and gets a finished layer's
    /// grace once it is finished
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.11). Dropping the segment unfinished still resets every
    /// unfinished layer.
    pub follows_upstream: bool,
}

impl SegmentTerms {
    /// Sets the sender's latency budget.
    #[must_use]
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    /// Sets the priority among topics within a layer.
    #[must_use]
    pub fn with_priority(mut self, priority: i16) -> Self {
        self.priority = priority;
        self
    }

    /// Marks the segment as relayed from an upstream that may still be
    /// streaming its predecessor.
    #[must_use]
    pub fn with_follows_upstream(mut self, follows_upstream: bool) -> Self {
        self.follows_upstream = follows_upstream;
        self
    }
}

/// One copy's control, shared by the [`Segment`] and every layer task of
/// the copy: how its segment's successor, a cut or its end reach it.
pub(crate) struct CopyCtl {
    superseded: watch::Sender<bool>,
    /// The lowest cut layer; [`MAX_SEGMENT_LAYERS`] while nothing is cut.
    /// It only ever goes down.
    cut_from: watch::Sender<u8>,
    /// Bytes queued for this copy over all its layers: what the finish
    /// grace has to carry.
    total: AtomicU64,
    /// The segment's handle plus one per running layer task: a copy with
    /// none left has nothing a successor could supersede.
    handles: AtomicUsize,
    /// Supersession treats a layer as finished until a write has to wait
    /// ([`SegmentTerms::follows_upstream`]).
    follows_upstream: bool,
    /// The copy already counted in `layers_cut`.
    cut_counted: AtomicBool,
    /// The copy already counted under a whole-segment cause.
    lost_counted: AtomicBool,
}

impl CopyCtl {
    fn new(follows_upstream: bool) -> CopyCtl {
        CopyCtl {
            superseded: watch::Sender::new(false),
            cut_from: watch::Sender::new(MAX_SEGMENT_LAYERS),
            total: AtomicU64::new(0),
            handles: AtomicUsize::new(1),
            follows_upstream,
            cut_counted: AtomicBool::new(false),
            lost_counted: AtomicBool::new(false),
        }
    }

    fn supersede(&self) {
        self.superseded.send_replace(true);
    }

    fn live(&self) -> bool {
        self.handles.load(Ordering::Acquire) > 0
    }

    fn is_superseded(&self) -> bool {
        *self.superseded.borrow()
    }

    fn cut_now(&self) -> u8 {
        *self.cut_from.borrow()
    }

    /// Cuts layers `from` and above. Counted at most once per copy for a
    /// cut that starts above layer 0 (`layers_cut`) and at most once for the
    /// loss of layer 0 (under `cause`); `None` counts nothing, for a
    /// receiver that refused or left. A layer already cut stays as it was.
    fn cut(&self, from: u8, cause: Option<DropCause>, spec: &CopySpec) {
        let lowered = self.cut_from.send_if_modified(|cut| {
            if from < *cut {
                *cut = from;
                true
            } else {
                false
            }
        });
        if !lowered {
            return;
        }
        let Some(cause) = cause else {
            return;
        };
        if from == 0 {
            if !self.lost_counted.swap(true, Ordering::AcqRel) {
                spec.record(cause);
            }
        } else if !self.cut_counted.swap(true, Ordering::AcqRel) {
            spec.record(DropCause::LayersCut);
        }
    }
}

/// Per-topic segment numbering and the copies the next segment supersedes.
#[derive(Default)]
struct TopicState {
    next: u64,
    live: Vec<Arc<CopyCtl>>,
}

/// A sender's topics: the number each takes next, and the copies the next
/// segment on it supersedes. A radio holds one per path, a [`crate::Peer`]
/// one for its own copies.
pub(crate) struct SegmentTopics {
    /// The topics: their state, their eviction order and the floor.
    topics: Mutex<TopicTable>,
    max_scopes: usize,
}

/// The topics of one [`SegmentTopics`], with the order they are evicted in.
#[derive(Default)]
struct TopicTable {
    /// Per-topic state.
    states: HashMap<Arc<str>, TopicState>,
    /// Every key of `states`, first to be evicted at the front. A topic
    /// passed over for its copy in flight moves to the back.
    order: VecDeque<Arc<str>>,
    /// The number a topic created after an eviction starts from: above every
    /// number an evicted topic reached.
    floor: u64,
}

impl TopicTable {
    /// Evicts the first topic in eviction order with no copy in flight and
    /// raises the floor above the number it reached. A topic with a copy in
    /// flight goes to the back as it is passed. Every topic is looked at at
    /// most once, so `false` means every topic has a copy in flight.
    fn evict_idle(&mut self) -> bool {
        for _ in 0..self.order.len() {
            let Some(topic) = self.order.pop_front() else {
                break;
            };
            let Some(state) = self.states.get_mut(&topic) else {
                continue;
            };
            state.live.retain(|copy| copy.live());
            if state.live.is_empty() {
                if let Some(state) = self.states.remove(&topic) {
                    self.floor = self.floor.max(state.next);
                }
                return true;
            }
            self.order.push_back(topic);
        }
        false
    }
}

impl SegmentTopics {
    /// A table of at most `max_scopes` topics.
    pub(crate) fn new(max_scopes: usize) -> SegmentTopics {
        SegmentTopics {
            topics: Mutex::new(TopicTable::default()),
            max_scopes,
        }
    }

    /// Takes the next number on `topic` and supersedes every copy of the
    /// previous segment there. Fails with [`Error::LimitExceeded`] when
    /// `max_scopes` topics all have a copy in flight.
    pub(crate) fn next(&self, topic: &Arc<str>) -> Result<u64, Error> {
        let mut guard = lock(&self.topics);
        let table = &mut *guard;
        // The table is the application's, but still bounded: a topic with no
        // copy in flight has nothing to supersede and can go.
        if !table.states.contains_key(topic)
            && table.states.len() >= self.max_scopes
            && !table.evict_idle()
        {
            return Err(Error::LimitExceeded);
        }
        // A topic that comes back must not restart below what a receiver
        // already delivered on it, or the receiver would discard it as stale:
        // numbering resumes above every number an evicted topic reached.
        let floor = table.floor;
        let state = match table.states.entry(Arc::clone(topic)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                table.order.push_back(Arc::clone(topic));
                entry.insert(TopicState {
                    next: floor,
                    live: Vec::new(),
                })
            }
        };
        let number = state.next;
        state.next += 1;
        for copy in state.live.drain(..) {
            copy.supersede();
        }
        Ok(number)
    }

    /// Supersedes the previous segment's copies on `topic` without using the
    /// number: the sender takes its numbers elsewhere
    /// ([`SegmentNumbers`]).
    pub(crate) fn supersede(&self, topic: &Arc<str>) -> Result<(), Error> {
        self.next(topic).map(drop)
    }

    fn track(&self, topic: &Arc<str>, copy: &Arc<CopyCtl>) {
        if let Some(state) = lock(&self.topics).states.get_mut(topic) {
            state.live.push(Arc::clone(copy));
        }
    }
}

/// The newest segment per path, per topic: its number and the layers of it
/// delivered, one bit each.
type PerPath = HashMap<Arc<str>, HashMap<Arc<str>, (u64, u16)>>;

/// The newest segment delivered per `(path, topic)` on one connection, with
/// the layers of it already delivered: what every receiver — a dish, an
/// acceptor, a transfer endpoint, a pair — checks an arriving segment
/// against
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.3, §4.11). A redial is a new connection and starts over.
pub(crate) struct Newest {
    /// Per path, per topic, the newest number and its layer mask; and the
    /// entries in total.
    table: Mutex<(PerPath, usize)>,
    max: usize,
}

impl Newest {
    /// A table of at most `max` `(path, topic)` entries.
    pub(crate) fn new(max: usize) -> Newest {
        Newest {
            table: Mutex::new((HashMap::new(), 0)),
            max,
        }
    }

    /// `true` when layer `layer` of `segment` is fresh on (`path`,
    /// `topic`): the segment is newer than the newest delivered there, or it
    /// is the newest and this layer of it was not delivered yet. A fresh
    /// arrival is recorded. At the table's cap an untracked key is always
    /// fresh: the check needs memory, and memory is bounded.
    pub(crate) fn fresh(&self, path: &str, topic: &str, segment: u64, layer: u8) -> bool {
        let bit = 1u16 << (layer % MAX_SEGMENT_LAYERS);
        let mut guard = lock(&self.table);
        let (paths, count) = &mut *guard;
        if let Some((last, mask)) = paths.get_mut(path).and_then(|t| t.get_mut(topic)) {
            if segment > *last {
                (*last, *mask) = (segment, bit);
                return true;
            }
            if segment == *last && *mask & bit == 0 {
                *mask |= bit;
                return true;
            }
            return false;
        }
        if *count < self.max {
            match paths.get_mut(path) {
                Some(topics) => {
                    topics.insert(Arc::from(topic), (segment, bit));
                }
                None => {
                    paths.insert(
                        Arc::from(path),
                        HashMap::from([(Arc::from(topic), (segment, bit))]),
                    );
                }
            }
            *count += 1;
        }
        true
    }
}

/// The next segment number per `(path, topic)`.
type NextNumbers = HashMap<(Arc<str>, Arc<str>), u64>;

/// The segment numbers per `(path, topic)` of one dialling connection: every
/// [`crate::Peer`] that sends segments over it shares one sequence, so the
/// receiver sees them only rise
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.11).
pub(crate) struct SegmentNumbers {
    table: Mutex<NumberTable>,
    max: usize,
}

/// The entries of one [`SegmentNumbers`], with the order they are evicted in.
#[derive(Default)]
struct NumberTable {
    /// The next number per `(path, topic)`.
    numbers: NextNumbers,
    /// Every key of `numbers`, oldest first: the front is evicted first.
    order: VecDeque<(Arc<str>, Arc<str>)>,
    /// The number a key created after an eviction starts from: above every
    /// number an evicted key reached.
    floor: u64,
}

impl NumberTable {
    /// Evicts the oldest key and raises the floor above the number it reached.
    fn evict_oldest(&mut self) {
        while let Some(key) = self.order.pop_front() {
            if let Some(next) = self.numbers.remove(&key) {
                self.floor = self.floor.max(next);
                return;
            }
        }
    }
}

impl SegmentNumbers {
    /// A table of at most `max` `(path, topic)` entries.
    pub(crate) fn new(max: usize) -> SegmentNumbers {
        SegmentNumbers {
            table: Mutex::new(NumberTable::default()),
            max,
        }
    }

    /// Takes the next number on (`path`, `topic`). Never fails: at the cap
    /// it evicts the oldest entry, and a key that comes back resumes above
    /// every number an evicted one reached, so a receiver never takes it as
    /// stale.
    pub(crate) fn next(&self, path: &Arc<str>, topic: &Arc<str>) -> u64 {
        let mut guard = lock(&self.table);
        let table = &mut *guard;
        let key = (Arc::clone(path), Arc::clone(topic));
        if let Some(next) = table.numbers.get_mut(&key) {
            let number = *next;
            *next += 1;
            return number;
        }
        if table.numbers.len() >= self.max {
            table.evict_oldest();
        }
        let number = table.floor;
        if table.numbers.len() < self.max {
            table.order.push_back(key.clone());
            table.numbers.insert(key, number + 1);
        } else {
            // A table of no entries at all: every segment takes the floor.
            table.floor += 1;
        }
        number
    }
}

/// What a layer's writer task is told.
enum SegItem {
    Chunk(Bytes),
    Finish,
}

/// What every layer task of one copy shares: where it goes and what it is.
struct CopySpec {
    conn: ConnHandle,
    path: Arc<str>,
    topic: Arc<str>,
    number: u64,
    priority: i16,
    /// The smaller of the sender's and the receiver's `max_age`, from the
    /// segment's open.
    deadline: Option<Instant>,
    drops: Arc<DropTable>,
    /// The receiving dish's own record, for a radio copy
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.6); `None` for a `Peer::segment` copy.
    dish: Option<Arc<Causes>>,
}

impl CopySpec {
    fn expired(&self) -> bool {
        self.deadline.is_some_and(|d| self.conn.exec.now() >= d)
    }

    /// Counts one drop on the topic, and on the dish's record when there
    /// is one.
    fn record(&self, cause: DropCause) {
        self.drops.record(&self.topic, cause);
        if let Some(dish) = &self.dish {
            dish.record(cause);
        }
    }
}

/// The sending side of one layer of one copy.
struct LayerTx {
    tx: mpsc::Sender<SegItem>,
    /// Bytes queued and not yet written: what a Segment-side cut returns to
    /// the budget.
    queued: Arc<AtomicU64>,
    /// The writer finished this layer: every chunk of it is queued.
    finished: Arc<AtomicBool>,
}

impl LayerTx {
    /// Queues `chunk` for the layer's writer; `permit` is the budget the
    /// chunk is charged against.
    ///
    /// The bytes are counted as queued **before** the writer can see the
    /// chunk. The writer gives back what it wrote against that count, and
    /// never more than the count holds, because a cut may have taken it
    /// already. A count made after the send could land after the writer's
    /// release, find nothing to return and leave a charge for a chunk that is
    /// gone, which nothing gives back. A chunk the queue refuses takes its
    /// count back, and its permit with it.
    ///
    /// On success the permit is spent: the writer or a cut returns its
    /// bytes.
    fn queue(
        &self,
        chunk: Bytes,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), mpsc::error::TrySendError<SegItem>> {
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.queued.fetch_add(len, Ordering::AcqRel);
        match self.tx.try_send(SegItem::Chunk(chunk)) {
            Ok(()) => {
                permit.forget();
                Ok(())
            }
            Err(refused) => {
                // This thread added `len` and only a cut on this thread
                // clears the count, so it still holds at least `len`.
                self.queued.fetch_sub(len, Ordering::AcqRel);
                drop(permit);
                Err(refused)
            }
        }
    }
}

/// The sending side of one copy, kept by the [`Segment`].
struct CopyTx {
    spec: Arc<CopySpec>,
    ctl: Arc<CopyCtl>,
    /// The highest layer the receiver wants.
    cap: u8,
    /// Chunk bytes this receiver's copies may hold unwritten.
    budget: Arc<Semaphore>,
    layers: [Option<LayerTx>; MAX_SEGMENT_LAYERS as usize],
}

impl CopyTx {
    /// Cuts layers `from` and above on the segment's side: the cut is
    /// recorded, and their queued bytes go back to the budget at once.
    fn cut(&mut self, from: u8, cause: DropCause) {
        self.ctl.cut(from, Some(cause), &self.spec);
        for slot in &mut self.layers[usize::from(from)..] {
            if let Some(layer) = slot.take() {
                let queued = layer.queued.swap(0, Ordering::AcqRel);
                self.budget
                    .add_permits(usize::try_from(queued).unwrap_or(usize::MAX));
            }
        }
    }

    /// Whether an uncut layer above `layer` is open, whose queued bytes a
    /// cut could free.
    fn upper_open(&self, layer: u8) -> bool {
        let cut = usize::from(self.ctl.cut_now());
        self.layers[usize::from(layer) + 1..cut.max(usize::from(layer) + 1)]
            .iter()
            .any(Option::is_some)
    }

    /// Opens layer `layer`: its writer task starts and opens its stream.
    fn open(&mut self, layer: u8) {
        let (tx, rx) = mpsc::channel(COPY_QUEUE);
        let queued = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        self.ctl.handles.fetch_add(1, Ordering::AcqRel);
        self.spec.conn.exec.spawn(layer_copy(
            Arc::clone(&self.spec),
            Arc::clone(&self.ctl),
            layer,
            rx,
            Arc::clone(&queued),
            Arc::clone(&finished),
            Arc::clone(&self.budget),
        ));
        self.layers[usize::from(layer)] = Some(LayerTx {
            tx,
            queued,
            finished,
        });
    }
}

impl Drop for CopyTx {
    fn drop(&mut self) {
        self.ctl.handles.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Where one copy of a segment goes.
pub(crate) struct CopyTarget {
    pub(crate) conn: ConnHandle,
    pub(crate) path: Arc<str>,
    /// Chunk bytes this receiver's copies may hold unwritten.
    pub(crate) budget: Arc<Semaphore>,
    /// The receiver's own latency budget; `None` for a `Peer::segment` copy.
    pub(crate) max_age: Option<Duration>,
    /// The highest layer the receiver wants; `None` for every layer.
    pub(crate) max_layer: Option<u8>,
    /// The receiving dish's record; `None` for a `Peer::segment` copy.
    pub(crate) dish: Option<Arc<Causes>>,
}

/// Opens segment `number` on `topic` toward every target, under `terms`.
///
/// The caller took `number` and superseded the previous segment on `topics`
/// already; the copies are tracked there so the next segment supersedes
/// them. No stream opens here: each layer's opens on its first chunk.
pub(crate) fn open_segment(
    topics: &SegmentTopics,
    drops: &Arc<DropTable>,
    topic: Arc<str>,
    number: u64,
    terms: &SegmentTerms,
    targets: Vec<CopyTarget>,
) -> Segment {
    let mut copies = Vec::with_capacity(targets.len());
    for target in targets {
        let opened = target.conn.exec.now();
        let ctl = Arc::new(CopyCtl::new(terms.follows_upstream));
        topics.track(&topic, &ctl);
        // Expiry is the smaller of the sender's and the receiver's budget,
        // MOQT's "smaller non-zero value"
        // ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
        // §4.2).
        let max_age = match (terms.max_age, target.max_age) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        copies.push(CopyTx {
            spec: Arc::new(CopySpec {
                conn: target.conn,
                path: target.path,
                topic: Arc::clone(&topic),
                number,
                priority: terms.priority,
                deadline: max_age.and_then(|age| opened.checked_add(age)),
                drops: Arc::clone(drops),
                dish: target.dish,
            }),
            ctl,
            cap: target.max_layer.unwrap_or(MAX_SEGMENT_LAYERS - 1),
            budget: target.budget,
            layers: Default::default(),
        });
    }
    Segment {
        number,
        topic,
        copies,
        finished: 0,
    }
}

/// One segment, open on every receiver it was opened toward.
///
/// [`Segment::write_layer`] never waits: a receiver without room for a chunk
/// loses that layer and every layer above it, counted, and loses the
/// segment if the layer is 0. Dropping a segment resets every layer not yet
/// finished, so no receiver mistakes a partial layer for a whole one.
pub struct Segment {
    number: u64,
    topic: Arc<str>,
    copies: Vec<CopyTx>,
    /// The layers [`Segment::finish_layer`] was called on, one bit each.
    finished: u16,
}

impl Segment {
    /// This segment's number on its topic.
    pub fn number(&self) -> u64 {
        self.number
    }

    /// The topic this segment belongs to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Hands `chunk` to layer 0 of every copy still open; returns how many
    /// copies took it. See [`Segment::write_layer`].
    ///
    /// # Errors
    ///
    /// As [`Segment::write_layer`].
    pub fn write(&mut self, chunk: impl Into<Bytes>) -> Result<usize, Error> {
        self.write_layer(0, chunk)
    }

    /// Hands `chunk` to layer `layer` of every copy that wants it; returns
    /// how many copies took it.
    ///
    /// A copy whose receiver capped its layers below `layer` is skipped, and
    /// that is not a drop. A copy without room cuts by the cut rule: its
    /// layers above `layer` first, then `layer` and above
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.3). A layer opened after the successor segment opened, or after
    /// the deadline, is cut at once. Never waits.
    ///
    /// # Errors
    ///
    /// [`Error::LimitExceeded`] for a layer of 16 or above, or a chunk above
    /// 4 GiB, which no budget can account; [`Error::Runtime`] for a layer
    /// [`Segment::finish_layer`] already finished.
    pub fn write_layer(&mut self, layer: u8, chunk: impl Into<Bytes>) -> Result<usize, Error> {
        if layer >= MAX_SEGMENT_LAYERS {
            return Err(Error::LimitExceeded);
        }
        let chunk = chunk.into();
        let len = u32::try_from(chunk.len()).map_err(|_| Error::LimitExceeded)?;
        if self.finished & (1 << layer) != 0 {
            return Err(Error::Runtime(format!(
                "layer {layer} of segment {} is finished",
                self.number
            )));
        }
        let index = usize::from(layer);
        let mut took = 0;
        for copy in &mut self.copies {
            if layer > copy.cap || copy.ctl.cut_now() <= layer {
                continue;
            }
            if copy.layers[index].is_none() {
                if copy.ctl.is_superseded() && !copy.ctl.follows_upstream {
                    copy.cut(layer, DropCause::Superseded);
                    continue;
                }
                if copy.spec.expired() {
                    copy.cut(layer, DropCause::Expired);
                    continue;
                }
            }
            let permit = match Arc::clone(&copy.budget).try_acquire_many_owned(len) {
                Ok(permit) => permit,
                Err(_) => {
                    // The cut rule: the layers above give their room first.
                    if copy.upper_open(layer) {
                        copy.cut(layer + 1, DropCause::SubscriberBudget);
                    }
                    match Arc::clone(&copy.budget).try_acquire_many_owned(len) {
                        Ok(permit) => permit,
                        Err(_) => {
                            copy.cut(layer, DropCause::SubscriberBudget);
                            continue;
                        }
                    }
                }
            };
            if copy.layers[index].is_none() {
                copy.open(layer);
            }
            let Some(tx) = copy.layers[index].as_ref() else {
                continue;
            };
            match tx.queue(chunk.clone(), permit) {
                Ok(()) => {
                    copy.ctl.total.fetch_add(u64::from(len), Ordering::AcqRel);
                    took += 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    copy.cut(layer, DropCause::SubscriberQueue);
                }
                // The layer's task already ended and cut what it had to.
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        self.copies.retain(|copy| copy.ctl.cut_now() > 0);
        Ok(took)
    }

    /// Ends layer `layer` on every copy that has it open and uncut; a layer
    /// of 16 or above is ignored. A later write to it is an error.
    pub fn finish_layer(&mut self, layer: u8) {
        if layer >= MAX_SEGMENT_LAYERS {
            return;
        }
        self.finished |= 1 << layer;
        let index = usize::from(layer);
        for copy in &mut self.copies {
            if copy.ctl.cut_now() <= layer {
                continue;
            }
            let Some(tx) = copy.layers[index].as_ref() else {
                continue;
            };
            match tx.tx.try_send(SegItem::Finish) {
                // Stored once the FIN is queued: a FIN that found the queue
                // full leaves the layer unfinished, and it is cut.
                Ok(()) => tx.finished.store(true, Ordering::Release),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    copy.cut(layer, DropCause::SubscriberQueue);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        self.copies.retain(|copy| copy.ctl.cut_now() > 0);
    }

    /// Ends every layer still open; returns how many copies still hold
    /// layer 0. A copy that holds no layer at all — nothing was written to
    /// it — gets an empty layer 0, so an empty segment arrives as an empty
    /// stream wherever this counts it.
    pub fn finish(mut self) -> usize {
        if self.finished & 1 == 0 {
            for copy in &mut self.copies {
                if copy.ctl.cut_now() == 0 || copy.layers.iter().any(Option::is_some) {
                    continue;
                }
                if copy.ctl.is_superseded() && !copy.ctl.follows_upstream {
                    copy.cut(0, DropCause::Superseded);
                } else if copy.spec.expired() {
                    copy.cut(0, DropCause::Expired);
                } else {
                    copy.open(0);
                }
            }
            self.copies.retain(|copy| copy.ctl.cut_now() > 0);
        }
        let opened = self.copies.iter().fold(0u16, |mask, copy| {
            copy.layers
                .iter()
                .enumerate()
                .filter(|(_, slot)| slot.is_some())
                .fold(mask, |mask, (layer, _)| mask | (1 << layer))
        });
        for layer in 0..MAX_SEGMENT_LAYERS {
            if opened & (1 << layer) != 0 && self.finished & (1 << layer) == 0 {
                self.finish_layer(layer);
            }
        }
        self.copies.len()
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Segment")
            .field("topic", &self.topic)
            .field("number", &self.number)
            .field("copies", &self.copies.len())
            .finish()
    }
}

/// Returns `len` bytes of a layer's queue to the budget, at most what is
/// still counted as queued: a Segment-side cut may have returned them
/// already, and a byte goes back exactly once.
fn release(queued: &AtomicU64, len: usize, budget: &Semaphore) {
    let len = u64::try_from(len).unwrap_or(u64::MAX);
    let mut before = queued.load(Ordering::Acquire);
    while let Err(seen) = queued.compare_exchange_weak(
        before,
        before.saturating_sub(len),
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        before = seen;
    }
    let returned = before.min(len);
    if returned > 0 {
        budget.add_permits(usize::try_from(returned).unwrap_or(usize::MAX));
    }
}

/// Resolves once layer `layer` or one below it is cut. `cut` is the copy's
/// receiver, subscribed once per layer task.
async fn cut_reached(cut: &mut watch::Receiver<u8>, layer: u8) {
    let _ = cut.wait_for(|c| *c <= layer).await;
}

/// Resolves once the copy's segment is superseded. `rx` is the copy's
/// receiver, subscribed once per layer task: `wait_for` reads the value, so a
/// supersession that came before the subscription counts too.
async fn superseded(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|s| *s).await;
}

/// Resolves when the copy is superseded while this layer's writer is still
/// writing it: such a layer is reset at once. A layer finished before the
/// successor opened, and a copy that follows upstream, go on: see
/// [`run_layer`]. Every poll decides as the first poll that saw the
/// supersession did, since `finished` only goes from false to true.
async fn superseded_unfinished(
    rx: &mut watch::Receiver<bool>,
    ctl: &CopyCtl,
    finished: &AtomicBool,
) {
    superseded(rx).await;
    if ctl.follows_upstream || finished.load(Ordering::Acquire) {
        std::future::pending::<()>().await;
    }
}

/// Resolves at the copy's deadline, or never.
async fn expiry(spec: &CopySpec) {
    match spec.deadline {
        Some(deadline) => {
            spec.conn
                .exec
                .sleep(deadline.saturating_duration_since(spec.conn.exec.now()))
                .await;
        }
        None => std::future::pending().await,
    }
}

/// Resolves when a layer past its FIN has had its grace after supersession:
/// the time its path needs to carry the copy's bytes at the congestion
/// window, one round trip for the receipt and [`SUPERSEDE_SLACK`].
async fn finish_grace_over(rx: &mut watch::Receiver<bool>, ctl: &CopyCtl, spec: &CopySpec) {
    superseded(rx).await;
    let wait = spec
        .conn
        .conn
        .transport_stats()
        .map(|t| t.path)
        .map_or(Duration::ZERO, |p| {
            finish_grace(p.rtt, p.cwnd, ctl.total.load(Ordering::Acquire))
        });
    spec.conn.exec.sleep(wait).await;
}

/// Writes one layer of one copy, racing every step against cuts,
/// supersession and the deadline; on exit, the chunks never written go back
/// to the budget.
async fn layer_copy(
    spec: Arc<CopySpec>,
    ctl: Arc<CopyCtl>,
    layer: u8,
    mut rx: mpsc::Receiver<SegItem>,
    queued: Arc<AtomicU64>,
    finished: Arc<AtomicBool>,
    budget: Arc<Semaphore>,
) {
    run_layer(&spec, &ctl, layer, &mut rx, &queued, &finished, &budget).await;
    rx.close();
    while let Ok(item) = rx.try_recv() {
        if let SegItem::Chunk(chunk) = item {
            release(&queued, chunk.len(), &budget);
        }
    }
    ctl.handles.fetch_sub(1, Ordering::AcqRel);
}

/// Opens and writes one layer. Supersession takes a layer the writer had not
/// finished at once. A layer the writer finished first, or of a copy that
/// follows upstream, is not stalled merely because its successor opened: it
/// hands its chunks to the transport if the transport takes them at once —
/// a write the receiver's flow control holds back means a stalled receiver,
/// and the layer is reset — and once its FIN is written it gets
/// [`finish_grace_over`] before the reset. Every reset cuts the layers
/// above too.
async fn run_layer(
    spec: &CopySpec,
    ctl: &CopyCtl,
    layer: u8,
    rx: &mut mpsc::Receiver<SegItem>,
    queued: &AtomicU64,
    finished: &AtomicBool,
    budget: &Semaphore,
) {
    // Subscribed and timed once per layer: every wake-up below polls these,
    // so a chunk costs neither a subscription nor a timer.
    let mut cut = ctl.cut_from.subscribe();
    let mut superseded_rx = ctl.superseded.subscribe();
    let mut expire = std::pin::pin!(expiry(spec));
    let opened = tokio::select! {
        biased;
        () = cut_reached(&mut cut, layer) => return,
        () = superseded_unfinished(&mut superseded_rx, ctl, finished) => {
            ctl.cut(layer, Some(DropCause::Superseded), spec);
            return;
        }
        () = &mut expire => {
            ctl.cut(layer, Some(DropCause::Expired), spec);
            return;
        }
        opened = open_layer(spec, layer) => opened,
    };
    let mut stream = match opened {
        Ok(stream) => stream,
        Err(Error::NoParkedConnection) => {
            ctl.cut(layer, Some(DropCause::NoParkedConnection), spec);
            return;
        }
        Err(e) => {
            tracing::debug!(error = %e, layer, "a segment layer could not be opened");
            ctl.cut(layer, None, spec);
            return;
        }
    };
    loop {
        let item = tokio::select! {
            biased;
            () = cut_reached(&mut cut, layer) => {
                stream.reset(codes::CANCELED);
                return;
            }
            () = superseded_unfinished(&mut superseded_rx, ctl, finished) => {
                stream.reset(codes::CANCELED);
                ctl.cut(layer, Some(DropCause::Superseded), spec);
                return;
            }
            () = &mut expire => {
                stream.reset(codes::CANCELED);
                ctl.cut(layer, Some(DropCause::Expired), spec);
                return;
            }
            item = rx.recv() => item,
        };
        match item {
            Some(SegItem::Chunk(chunk)) => {
                let len = chunk.len();
                // The write first: supersession only takes a write that has
                // to wait.
                let written = tokio::select! {
                    biased;
                    written = stream.write_chunk(chunk) => Ok(written),
                    () = cut_reached(&mut cut, layer) => Err(None),
                    () = superseded(&mut superseded_rx) => Err(Some(DropCause::Superseded)),
                    () = &mut expire => Err(Some(DropCause::Expired)),
                };
                release(queued, len, budget);
                match written {
                    Ok(Ok(())) => {}
                    // The receiver refused the stream or went away.
                    Ok(Err(_)) => {
                        ctl.cut(layer, None, spec);
                        return;
                    }
                    Err(cause) => {
                        stream.reset(codes::CANCELED);
                        if let Some(cause) = cause {
                            ctl.cut(layer, Some(cause), spec);
                        }
                        return;
                    }
                }
            }
            Some(SegItem::Finish) => {
                if stream.finish().is_err() {
                    ctl.cut(layer, None, spec);
                    return;
                }
                // Until the receiver's transport holds every byte, the layer
                // can still be cut, superseded or expire: a reset is
                // accepted after FIN until then.
                tokio::select! {
                    biased;
                    () = cut_reached(&mut cut, layer) => stream.reset(codes::CANCELED),
                    () = &mut expire => {
                        stream.reset(codes::CANCELED);
                        ctl.cut(layer, Some(DropCause::Expired), spec);
                    }
                    _ = stream.stopped() => {}
                    () = finish_grace_over(&mut superseded_rx, ctl, spec) => {
                        stream.reset(codes::CANCELED);
                        ctl.cut(layer, Some(DropCause::Superseded), spec);
                    }
                }
                return;
            }
            // The segment was dropped without finishing this layer.
            None => {
                stream.reset(codes::CANCELED);
                return;
            }
        }
    }
}

/// Opens one layer's stream: the DATA header names the path, the topic, the
/// segment number and, above layer 0, the layer (key `14`); the stream gets
/// its layer-major priority before any byte is queued, the only time
/// `quinn` guarantees it takes effect.
///
/// The header is checked before any stream opens: a header the decoder or the
/// peer's `max_header_bytes` refuses would close the connection every transfer
/// on it shares, so it is refused here with `LimitExceeded`.
async fn open_layer(spec: &CopySpec, layer: u8) -> Result<SendHalf, Error> {
    let header = DataHeader {
        topic: Some(spec.topic.to_string()),
        segment: Some(spec.number),
        layer: (layer > 0).then_some(layer),
        ..DataHeader::addressed(spec.path.as_ref())
    };
    let frame = Frame::of(&header)?;
    frame.fits(&spec.conn)?;
    let mut stream = spec.conn.open_uni().await?;
    stream.set_priority(copy_priority(layer, spec.priority));
    stream.write_chunk(frame.bytes).await?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(name: &str) -> Arc<str> {
        Arc::from(name)
    }

    /// A copy whose layer tasks are still running, as far as the table sees.
    fn in_flight() -> Arc<CopyCtl> {
        Arc::new(CopyCtl::new(false))
    }

    /// Ends a copy as its last holder does: its handle count drops to zero.
    fn end(copy: &CopyCtl) {
        copy.handles.fetch_sub(1, Ordering::AcqRel);
    }

    #[test]
    fn numbers_evict_the_oldest_key_and_keep_the_rest_counting() {
        let numbers = SegmentNumbers::new(2);
        let path = topic("path");
        let (a, b, c) = (topic("a"), topic("b"), topic("c"));
        assert_eq!(numbers.next(&path, &a), 0);
        assert_eq!(numbers.next(&path, &b), 0);
        assert_eq!(numbers.next(&path, &b), 1);
        // `c` evicts `a`, the oldest, which reached 0: the floor becomes 1.
        assert_eq!(numbers.next(&path, &c), 1);
        // `b` is not evicted and carries on from its own count.
        assert_eq!(numbers.next(&path, &b), 2);
    }

    #[test]
    fn a_key_that_comes_back_resumes_above_every_evicted_number() {
        let numbers = SegmentNumbers::new(1);
        let path = topic("path");
        let (a, b) = (topic("a"), topic("b"));
        assert_eq!(numbers.next(&path, &a), 0);
        assert_eq!(numbers.next(&path, &a), 1);
        assert_eq!(numbers.next(&path, &a), 2);
        // `b` evicts `a`, which reached 2: the floor becomes 3.
        assert_eq!(numbers.next(&path, &b), 3);
        // `a` evicts `b`, which reached 3: `a` resumes above it, at 4.
        assert_eq!(numbers.next(&path, &a), 4);
    }

    #[test]
    fn topics_evict_the_oldest_idle_topic_and_resume_above_it() {
        let topics = SegmentTopics::new(2);
        let (a, b, c) = (topic("a"), topic("b"), topic("c"));
        assert_eq!(topics.next(&a).ok(), Some(0));
        assert_eq!(topics.next(&a).ok(), Some(1));
        assert_eq!(topics.next(&b).ok(), Some(0));
        // `a` is the oldest and idle: it goes, and the floor is its next, 2.
        assert_eq!(topics.next(&c).ok(), Some(2));
        // `b` is the oldest and goes; the floor stays at 2, above its 1.
        assert_eq!(topics.next(&a).ok(), Some(2));
    }

    #[test]
    fn topics_keep_a_topic_with_a_copy_in_flight() {
        let topics = SegmentTopics::new(2);
        let (a, b, c) = (topic("a"), topic("b"), topic("c"));
        assert_eq!(topics.next(&a).ok(), Some(0));
        assert_eq!(topics.next(&a).ok(), Some(1));
        let copy = in_flight();
        topics.track(&a, &copy);
        assert_eq!(topics.next(&b).ok(), Some(0));
        // `a` is the oldest but has a copy in flight, so `b` goes: the floor
        // is `b`'s next, 1.
        assert_eq!(topics.next(&c).ok(), Some(1));
        // `a` keeps its count, and its next segment supersedes the copy.
        assert_eq!(topics.next(&a).ok(), Some(2));
        assert!(copy.is_superseded());
    }

    #[test]
    fn topics_refuse_a_new_topic_while_all_have_a_copy_in_flight() {
        let topics = SegmentTopics::new(2);
        let (a, b, c) = (topic("a"), topic("b"), topic("c"));
        assert_eq!(topics.next(&a).ok(), Some(0));
        assert_eq!(topics.next(&b).ok(), Some(0));
        let (copy_a, copy_b) = (in_flight(), in_flight());
        topics.track(&a, &copy_a);
        topics.track(&b, &copy_b);
        assert!(topics.next(&c).is_err());
        // A topic already in the table still takes its number at the cap.
        assert_eq!(topics.next(&b).ok(), Some(1));
        // `a`'s copy has ended, so `a` is idle and goes first; the floor is
        // its next, 1.
        end(&copy_a);
        assert_eq!(topics.next(&c).ok(), Some(1));
    }

    /// A layer's sending side with room for `depth` chunks, the writer's end
    /// of its queue, and a budget of `budget` bytes.
    fn layer_with(
        depth: usize,
        budget: usize,
    ) -> (LayerTx, mpsc::Receiver<SegItem>, Arc<Semaphore>) {
        let (tx, rx) = mpsc::channel(depth);
        let layer = LayerTx {
            tx,
            queued: Arc::new(AtomicU64::new(0)),
            finished: Arc::new(AtomicBool::new(false)),
        };
        (layer, rx, Arc::new(Semaphore::new(budget)))
    }

    fn chunk(len: u32) -> Bytes {
        Bytes::from(vec![0u8; len as usize])
    }

    fn charge(budget: &Arc<Semaphore>, len: u32) -> OwnedSemaphorePermit {
        Arc::clone(budget)
            .try_acquire_many_owned(len)
            .expect("room in the budget")
    }

    #[test]
    fn a_queued_chunk_is_counted_and_its_bytes_return_when_it_is_written() {
        let (layer, mut rx, budget) = layer_with(2, 100);
        layer.queue(chunk(10), charge(&budget, 10)).expect("queued");
        assert_eq!(layer.queued.load(Ordering::Acquire), 10);
        assert_eq!(budget.available_permits(), 90);

        let Ok(SegItem::Chunk(written)) = rx.try_recv() else {
            panic!("the queued chunk");
        };
        release(&layer.queued, written.len(), &budget);
        assert_eq!(layer.queued.load(Ordering::Acquire), 0);
        assert_eq!(budget.available_permits(), 100);
    }

    #[test]
    fn a_chunk_the_queue_refuses_is_neither_counted_nor_charged() {
        let (layer, mut rx, budget) = layer_with(1, 100);
        layer.queue(chunk(10), charge(&budget, 10)).expect("queued");
        let refused = layer.queue(chunk(10), charge(&budget, 10));
        assert!(matches!(refused, Err(mpsc::error::TrySendError::Full(_))));
        // Only the chunk that is in the queue is counted and charged: a
        // count left behind by the refused one would be returned to the
        // budget a second time by the next cut.
        assert_eq!(layer.queued.load(Ordering::Acquire), 10);
        assert_eq!(budget.available_permits(), 90);

        let Ok(SegItem::Chunk(written)) = rx.try_recv() else {
            panic!("the queued chunk");
        };
        release(&layer.queued, written.len(), &budget);
        assert_eq!(layer.queued.load(Ordering::Acquire), 0);
        assert_eq!(budget.available_permits(), 100);
    }

    #[test]
    fn a_chunk_for_a_layer_whose_writer_has_ended_is_neither_counted_nor_charged() {
        let (layer, rx, budget) = layer_with(1, 100);
        drop(rx);
        let refused = layer.queue(chunk(10), charge(&budget, 10));
        assert!(matches!(refused, Err(mpsc::error::TrySendError::Closed(_))));
        assert_eq!(layer.queued.load(Ordering::Acquire), 0);
        assert_eq!(budget.available_permits(), 100);
    }

    /// One sender and one writer on threads of their own: `chunks` chunks of
    /// one byte through a budget of [`RACE_BUDGET`]. The writer spins rather
    /// than parks, so it has each chunk within nanoseconds of the send and
    /// returns its bytes the moment it has it, which is the interleaving a
    /// count made after the send loses. Returns what the layer's count and
    /// the budget hold once everything is written.
    fn race_round(chunks: usize) -> (u64, usize) {
        let (layer, mut rx, budget) = layer_with(COPY_QUEUE, RACE_BUDGET);
        let queued = Arc::clone(&layer.queued);
        let returned = Arc::clone(&budget);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            loop {
                match rx.try_recv() {
                    Ok(SegItem::Chunk(chunk)) => release(&queued, chunk.len(), &returned),
                    Ok(SegItem::Finish) => {}
                    Err(mpsc::error::TryRecvError::Empty) => std::hint::spin_loop(),
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }
            let _ = done_tx.send(());
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut sent = 0;
        while sent < chunks {
            assert!(
                Instant::now() < deadline,
                "the writer stopped returning budget"
            );
            let Ok(permit) = Arc::clone(&budget).try_acquire_many_owned(1) else {
                std::thread::yield_now();
                continue;
            };
            match layer.queue(chunk(1), permit) {
                Ok(()) => sent += 1,
                // The queue is full: the writer has not caught up yet.
                Err(mpsc::error::TrySendError::Full(_)) => std::thread::yield_now(),
                Err(mpsc::error::TrySendError::Closed(_)) => panic!("the writer ended early"),
            }
        }
        let queued = Arc::clone(&layer.queued);
        drop(layer);
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the writer drained the queue");
        writer.join().expect("the writer thread");
        (queued.load(Ordering::Acquire), budget.available_permits())
    }

    const RACE_BUDGET: usize = 64;

    /// Whatever the schedule, the budget is whole and nothing is counted as
    /// queued once every chunk is written. Several rounds, each on threads of
    /// its own, because where the two land decides how often the writer's
    /// release falls inside the sender's window.
    #[test]
    fn a_writer_that_releases_at_once_never_strands_budget() {
        for round in 0..10 {
            let (counted, available) = std::thread::spawn(|| race_round(200_000))
                .join()
                .expect("the sender thread");
            assert_eq!(counted, 0, "round {round}: bytes still counted as queued");
            assert_eq!(available, RACE_BUDGET, "round {round}: budget stranded");
        }
    }
}
