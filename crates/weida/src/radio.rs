//! RADIO/DISH: lossy fan-out of segments
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
//!
//! A **segment** is a unit whose parts may depend on each other and on
//! nothing earlier — a voice frame, a raw preview frame, a video GOP — and it
//! is the join point. A radio numbers segments per topic (DATA key `13`) and
//! sends each to every dish joined when it opens, one uni stream per dish.
//! Three things drop a copy, and each is counted per topic and cause:
//!
//! * **supersession** — opening segment *n+1* resets every copy of segment
//!   *n* on that topic that is still unacknowledged, and no other topic's;
//! * **expiry** — a dish's `max_age`, on the radio's clock from the segment's
//!   open;
//! * **the dish's budget** — `subscriber_buffer_bytes` of chunks a copy may
//!   hold unwritten, and a queue of [`COPY_QUEUE`] chunks.
//!
//! Nothing here ever waits for a dish: [`Segment::write`] is synchronous.
//! The dish in turn discards a segment older than the newest it delivered
//! on the topic and never blocks its connection on a full queue.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Notify, Semaphore, mpsc};
use weida_core::{Error, Limits, PeerIdentity};
use weida_protocol::header::limits::MAX_TOPIC_BYTES;
use weida_protocol::{DataHeader, FrameKind, codes, encode_varint, filter, split_flow_datagram};

use crate::config::ClientTls;
use crate::conn::{ConnHandle, Ctl};
use crate::endpoint::{Dish, Radio, send_subscription};
use crate::flow::{Flow, FlowMeta, IncomingFlow, open_flow_on};
use crate::identity::PeerChain;
use crate::listener::{Namespace, Route};
use crate::pubsub::{DropCause, DropTable, TopicDrops};
use crate::reconnect::PeerEvents;
use crate::runtime::RuntimeInner;
use crate::stream::{Attach, Peer};
use crate::transfer::{IncomingTransfer, write_data_preamble};
use crate::transport::SendHalf;

/// Chunks one copy may hold queued for its writer task. The real bound is
/// the dish's byte budget; this keeps the channel from growing on tiny
/// chunks.
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A dish's request to join a filter, as a radio's admission sees it
/// ([`Radio::with_admission`],
/// [decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
/// §4.3).
#[derive(Debug)]
#[non_exhaustive]
pub struct Join<'a> {
    /// The key or principal the dish's connection proved; `None` when it is
    /// anonymous.
    pub peer: Option<&'a PeerIdentity>,
    /// The certificate chain behind `peer`
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.2).
    pub peer_chain: Option<&'a PeerChain>,
    /// The filter exactly as the dish sent it.
    pub filter: &'a str,
}

impl<'a> Join<'a> {
    fn of(conn: &'a ConnHandle, filter: &'a str) -> Join<'a> {
        Join {
            peer: conn.peer.as_ref(),
            peer_chain: conn.peer_chain.as_ref(),
            filter,
        }
    }
}

/// A radio's admission policy: `true` records the join.
type Admission = Arc<dyn Fn(&Join<'_>) -> bool + Send + Sync>;

// --- radio side ---------------------------------------------------------------

/// One joined dish: a connection and the filters it joined with.
struct DishEntry {
    conn: ConnHandle,
    conn_id: usize,
    /// Filter to `max_age`.
    filters: HashMap<String, Option<Duration>>,
    /// Chunk bytes this dish's copies may hold unwritten, over all topics.
    budget: Arc<Semaphore>,
    /// One datagram flow per topic, opened lazily on the connection the join
    /// arrived on; bounded by the topics the dish's filters match.
    flows: Arc<Mutex<HashMap<Arc<str>, FlowSlot>>>,
}

/// A dish's datagram flow for one topic.
enum FlowSlot {
    /// The FLOW header is on its way; holds the newest datagram meanwhile,
    /// never more than one.
    Opening(Option<Bytes>),
    Open(Flow),
}

impl DishEntry {
    /// The dish's latency budget for `topic`: the smallest `max_age` among
    /// the matching filters that carry one. `None` for no match at all.
    fn matching(&self, topic: &str) -> Option<Option<Duration>> {
        let mut matched = false;
        let mut max_age: Option<Duration> = None;
        for (f, age) in &self.filters {
            if filter::matches(topic, f) {
                matched = true;
                if let Some(age) = age {
                    max_age = Some(max_age.map_or(*age, |m| m.min(*age)));
                }
            }
        }
        matched.then_some(max_age)
    }

    /// Withdraws `filter` and releases its `max_subscriptions` slot; `false`
    /// when the dish did not hold it. A topic no remaining filter matches
    /// loses its flow: dropping it closes the FLOW stream with FIN.
    fn drop_filter(&mut self, withdrawn: &str) -> bool {
        if self.filters.remove(withdrawn).is_none() {
            return false;
        }
        if let Some(subs) = self.conn.subs.as_ref() {
            subs.release(self.conn_id);
        }
        let filters = &self.filters;
        lock(&self.flows).retain(|topic, _| filters.keys().any(|f| filter::matches(topic, f)));
        true
    }
}

/// One copy's control: how its segment's successor or its end reach it.
#[derive(Default)]
struct CopyCtl {
    superseded: AtomicBool,
    acked: AtomicBool,
    ended: AtomicBool,
    notify: Notify,
}

impl CopyCtl {
    fn supersede(&self) {
        if !self.acked.load(Ordering::Acquire) {
            self.superseded.store(true, Ordering::Release);
            self.notify.notify_one();
        }
    }

    fn live(&self) -> bool {
        !self.ended.load(Ordering::Acquire)
    }

    async fn superseded(&self) {
        loop {
            let notified = self.notify.notified();
            if self.superseded.load(Ordering::Acquire) {
                return;
            }
            // `notify_one` leaves a permit when nobody waits, so a
            // supersession between the check and this await is not lost.
            notified.await;
        }
    }
}

/// Per-topic segment numbering and the copies the next segment supersedes.
#[derive(Default)]
struct TopicState {
    next: u64,
    live: Vec<Arc<CopyCtl>>,
}

/// Everything a radio path holds: its dishes, its topics and its drops.
pub(crate) struct RadioHub {
    path: Arc<str>,
    dishes: Mutex<Vec<DishEntry>>,
    /// Per-topic state, and the number a topic created after an eviction
    /// starts from.
    topics: Mutex<(HashMap<Arc<str>, TopicState>, u64)>,
    drops: Arc<DropTable>,
    limits: Limits,
    /// The admission policy and its generation, which moves with every
    /// [`RadioHub::set_admission`] so a join screened under an older policy
    /// is screened again.
    admission: Mutex<(u64, Option<Admission>)>,
}

impl RadioHub {
    pub(crate) fn new(path: &str, limits: Limits) -> RadioHub {
        RadioHub {
            path: Arc::from(path),
            dishes: Mutex::new(Vec::new()),
            topics: Mutex::new((HashMap::new(), 0)),
            drops: Arc::new(DropTable::new(limits.max_sequence_scopes)),
            limits,
            admission: Mutex::new((0, None)),
        }
    }

    /// The dish table, with every entry whose connection closed removed.
    fn dishes(&self) -> MutexGuard<'_, Vec<DishEntry>> {
        let mut dishes = lock(&self.dishes);
        dishes.retain(|d| d.conn.conn.close_reason().is_none());
        dishes
    }

    /// Records a join. `reserve` counts a new filter against the
    /// connection's `max_subscriptions` and runs only for a filter the dish
    /// did not already hold; a repeated join updates its `max_age`.
    ///
    /// The admission, when there is one, decides first, every time: a
    /// refusal records nothing, reserves nothing and is silence
    /// ([decisions/0017](../../../docs/decisions/0017-subscription-verdict.md)
    /// §4.1). It runs outside the dish table's lock, so user code never runs
    /// under the hub's lock.
    pub(crate) fn join(
        &self,
        ctx: &ConnHandle,
        filter: String,
        max_age_ms: Option<u64>,
        reserve: impl FnOnce() -> Result<(), Error>,
    ) -> Result<(), Error> {
        let conn_id = ctx.conn.stable_id();
        let max_age = max_age_ms.map(Duration::from_millis);
        let mut dishes = loop {
            let (generation, admit) = {
                let admission = lock(&self.admission);
                (admission.0, admission.1.clone())
            };
            if let Some(admit) = admit
                && !admit(&Join::of(ctx, &filter))
            {
                tracing::debug!(path = %self.path, filter, "join refused by admission");
                return Ok(());
            }
            let dishes = self.dishes();
            // A policy installed meanwhile re-screens only what it finds
            // recorded; this join is not yet, so it is screened again.
            if lock(&self.admission).0 == generation {
                break dishes;
            }
        };
        let index = match dishes.iter().position(|d| d.conn_id == conn_id) {
            Some(index) => index,
            None => {
                dishes.push(DishEntry {
                    conn: ConnHandle::clone(ctx),
                    conn_id,
                    filters: HashMap::new(),
                    budget: Arc::new(Semaphore::new(self.limits.subscriber_buffer_bytes)),
                    flows: Arc::new(Mutex::new(HashMap::new())),
                });
                dishes.len() - 1
            }
        };
        let entry = &mut dishes[index];
        if let Some(age) = entry.filters.get_mut(&filter) {
            *age = max_age;
            return Ok(());
        }
        if let Err(e) = reserve() {
            if entry.filters.is_empty() {
                dishes.swap_remove(index);
            }
            return Err(e);
        }
        entry.filters.insert(filter, max_age);
        Ok(())
    }

    /// Withdraws a join the dish asked to leave, and releases its slot.
    pub(crate) fn leave(&self, conn_id: usize, filter: &str) {
        let mut dishes = self.dishes();
        let Some(index) = dishes.iter().position(|d| d.conn_id == conn_id) else {
            return;
        };
        dishes[index].drop_filter(filter);
        if dishes[index].filters.is_empty() {
            dishes.swap_remove(index);
        }
    }

    /// Withdraws `filter` from every connection `peer` joined it on; the
    /// number of joins withdrawn.
    pub(crate) fn evict(&self, peer: &PeerIdentity, filter: &str) -> usize {
        let mut dishes = self.dishes();
        let mut evicted = 0;
        for entry in dishes.iter_mut() {
            if entry.conn.peer.as_ref() == Some(peer) && entry.drop_filter(filter) {
                evicted += 1;
            }
        }
        dishes.retain(|d| !d.filters.is_empty());
        evicted
    }

    /// Installs `admit` and screens every join already recorded with it,
    /// withdrawing the ones it refuses. The policy runs outside the lock,
    /// on a snapshot; a join recorded meanwhile is screened in
    /// [`RadioHub::join`], because the generation moved.
    pub(crate) fn set_admission(&self, admit: Admission) {
        {
            let mut admission = lock(&self.admission);
            admission.0 += 1;
            admission.1 = Some(Arc::clone(&admit));
        }
        let recorded: Vec<(usize, ConnHandle, Vec<String>)> = self
            .dishes()
            .iter()
            .map(|d| {
                (
                    d.conn_id,
                    ConnHandle::clone(&d.conn),
                    d.filters.keys().cloned().collect(),
                )
            })
            .collect();
        let refused: Vec<(usize, String)> = recorded
            .iter()
            .flat_map(|(conn_id, conn, filters)| {
                filters
                    .iter()
                    .filter(|f| !admit(&Join::of(conn, f)))
                    .map(|f| (*conn_id, f.clone()))
            })
            .collect();
        if refused.is_empty() {
            return;
        }
        let mut dishes = self.dishes();
        for (conn_id, filter) in &refused {
            if let Some(entry) = dishes.iter_mut().find(|d| d.conn_id == *conn_id) {
                entry.drop_filter(filter);
            }
        }
        dishes.retain(|d| !d.filters.is_empty());
    }

    /// Takes the next number on `topic` and resets every unacknowledged copy
    /// of the previous segment there.
    fn next_segment(&self, topic: &Arc<str>) -> Result<u64, Error> {
        let mut guard = lock(&self.topics);
        let (topics, floor) = &mut *guard;
        if !topics.contains_key(topic) && topics.len() >= self.limits.max_sequence_scopes {
            // The table is the application's, but still bounded: a topic
            // with no copy in flight has nothing to supersede and can go.
            let idle = topics.iter_mut().find_map(|(topic, state)| {
                state.live.retain(|c| c.live());
                state
                    .live
                    .is_empty()
                    .then(|| (Arc::clone(topic), state.next))
            });
            match idle {
                Some((idle, next)) => {
                    topics.remove(&idle);
                    // A topic that comes back must not restart below what a
                    // dish already delivered on it, or the dish would
                    // discard it as stale: numbering resumes above every
                    // number an evicted topic reached.
                    *floor = (*floor).max(next);
                }
                None => return Err(Error::LimitExceeded),
            }
        }
        let state = topics
            .entry(Arc::clone(topic))
            .or_insert_with(|| TopicState {
                next: *floor,
                live: Vec::new(),
            });
        let number = state.next;
        state.next += 1;
        for copy in state.live.drain(..) {
            copy.supersede();
        }
        Ok(number)
    }

    fn track(&self, topic: &Arc<str>, copy: &Arc<CopyCtl>) {
        if let Some(state) = lock(&self.topics).0.get_mut(topic) {
            state.live.push(Arc::clone(copy));
        }
    }
}

/// State of a radio.
///
/// Clonable: a clone is another handle to the same radio, and the path is
/// released when the last one goes.
#[derive(Clone)]
pub struct RadioState {
    hub: Arc<RadioHub>,
    _claim: Arc<Claim>,
}

/// The radio's claim on its path, released with the last handle.
struct Claim {
    namespace: Arc<Namespace>,
    path: Arc<str>,
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.namespace.unregister(&self.path);
    }
}

impl RadioState {
    pub(crate) fn new(hub: Arc<RadioHub>, namespace: Arc<Namespace>) -> RadioState {
        let path = Arc::clone(&hub.path);
        RadioState {
            hub,
            _claim: Arc::new(Claim { namespace, path }),
        }
    }
}

impl Clone for Radio {
    fn clone(&self) -> Radio {
        Radio::from_state(self.state().clone())
    }
}

impl Radio {
    fn hub(&self) -> &RadioHub {
        &self.state().hub
    }

    /// Opens segment *n+1* on `topic` as one stream per joined dish, and
    /// resets every copy of segment *n* on that topic still unacknowledged.
    ///
    /// The dish set is fixed here: a dish that joins while the segment is in
    /// flight receives the next one. Zero dishes is not an error. Fails with
    /// [`Error::LimitExceeded`] for a topic above 256 bytes, or when
    /// `max_sequence_scopes` topics all have a copy in flight.
    pub fn segment(&self, topic: &str) -> Result<Segment, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let hub = self.hub();
        let topic: Arc<str> = Arc::from(topic);
        let number = hub.next_segment(&topic)?;
        let mut copies = Vec::new();
        for entry in hub.dishes().iter() {
            let Some(max_age) = entry.matching(&topic) else {
                continue;
            };
            let (tx, rx) = mpsc::channel(COPY_QUEUE);
            let ctl = Arc::new(CopyCtl::default());
            hub.track(&topic, &ctl);
            let deadline = max_age.map(|age| Box::pin(entry.conn.exec.sleep(age)));
            entry.conn.exec.spawn(segment_copy(
                ConnHandle::clone(&entry.conn),
                Copy {
                    path: Arc::clone(&hub.path),
                    topic: Arc::clone(&topic),
                    number,
                    ctl: Arc::clone(&ctl),
                    budget: Arc::clone(&entry.budget),
                    drops: Arc::clone(&hub.drops),
                },
                rx,
                deadline,
            ));
            copies.push(CopyTx {
                tx,
                budget: Arc::clone(&entry.budget),
            });
        }
        Ok(Segment {
            number,
            topic,
            copies,
            drops: Arc::clone(&hub.drops),
        })
    }

    /// Sends a one-packet segment on `topic` to every joined dish, as a
    /// datagram on that dish's flow for the topic; returns how many dishes it
    /// was handed to.
    ///
    /// It takes the topic's next segment number and supersedes the stream
    /// copies still in flight there, exactly as [`Radio::segment`] does. The
    /// flow is opened lazily on the connection the join arrived on; while it
    /// opens, the newest datagram waits and an older one is dropped. A dish
    /// whose connection carries no datagrams, and a payload larger than a
    /// dish's connection carries, are counted drops with their causes —
    /// never turned into a stream segment, which would arrive late
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.6 rule 4).
    pub fn datagram(&self, topic: &str, payload: impl Into<Bytes>) -> Result<usize, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let hub = self.hub();
        let topic: Arc<str> = Arc::from(topic);
        let number = hub.next_segment(&topic)?;
        let payload = payload.into();
        let mut body = Vec::with_capacity(8 + payload.len());
        encode_varint(number, &mut body).expect("segment numbers stay below 2^62");
        body.extend_from_slice(&payload);
        let body = Bytes::from(body);

        let mut handed = 0;
        for entry in hub.dishes().iter() {
            if entry.matching(&topic).is_none() {
                continue;
            }
            if !entry.conn.agreed_now().is_some_and(|a| a.datagrams) {
                hub.drops.record(&topic, DropCause::NoDatagrams);
                continue;
            }
            let mut flows = lock(&entry.flows);
            match flows.get_mut(&topic) {
                Some(FlowSlot::Open(flow)) => match flow.send(body.clone()) {
                    Ok(()) => handed += 1,
                    Err(Error::TooLarge { .. }) => hub.drops.record(&topic, DropCause::TooLarge),
                    Err(Error::DatagramsUnavailable) => {
                        hub.drops.record(&topic, DropCause::NoDatagrams);
                    }
                    // The flow ended — refused, released or its connection
                    // gone. This datagram is lost; the next one reopens.
                    Err(_) => {
                        flows.remove(&topic);
                        hub.drops.record(&topic, DropCause::SubscriberQueue);
                    }
                },
                Some(FlowSlot::Opening(pending)) => {
                    if pending.replace(body.clone()).is_some() {
                        hub.drops.record(&topic, DropCause::SubscriberQueue);
                    }
                    handed += 1;
                }
                None => {
                    flows.insert(Arc::clone(&topic), FlowSlot::Opening(Some(body.clone())));
                    entry.conn.exec.spawn(open_datagram_flow(
                        ConnHandle::clone(&entry.conn),
                        Arc::clone(&hub.path),
                        Arc::clone(&topic),
                        Arc::clone(&entry.flows),
                        Arc::clone(&hub.drops),
                    ));
                    handed += 1;
                }
            }
        }
        Ok(handed)
    }

    /// The path this radio serves.
    pub fn path(&self) -> &str {
        &self.hub().path
    }

    /// Dishes currently joined.
    pub fn dish_count(&self) -> usize {
        self.hub().dishes().len()
    }

    /// Copies dropped, over topics and causes.
    pub fn dropped(&self) -> u64 {
        self.hub().drops.total()
    }

    /// What was dropped on `topic`, by cause, or `None` if nothing was. The
    /// table holds at most `Limits::max_sequence_scopes` topics.
    pub fn dropped_on(&self, topic: &str) -> Option<TopicDrops> {
        self.hub().drops.on_topic(topic)
    }

    /// Every topic that lost a copy, with its counts by cause.
    pub fn drops(&self) -> Vec<TopicDrops> {
        self.hub().drops.by_topic()
    }

    /// Decides which dish may join which filter
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.3). `admit` sees each join — the proved peer, its chain and the
    /// filter — before it is recorded, on every repeat too, and a join it
    /// refuses is not recorded, reserves no `max_subscriptions` slot and
    /// closes nothing: the dish is told nothing, which is what
    /// [decisions/0017](../../../docs/decisions/0017-subscription-verdict.md)
    /// §4.1 defines silence to mean. A refused repeat leaves the recorded
    /// filter alone; withdrawing it is [`Radio::evict`]'s.
    ///
    /// Installing a policy screens the joins already recorded and withdraws
    /// the ones it refuses, so no join slips in between creating the radio
    /// and calling this. The policy is shared by every clone of this radio,
    /// and a later call replaces it. It runs on the dish connection's task
    /// and must not block.
    pub fn with_admission(
        self,
        admit: impl Fn(&Join<'_>) -> bool + Send + Sync + 'static,
    ) -> Radio {
        self.hub().set_admission(Arc::new(admit));
        self
    }

    /// Withdraws `filter` — the exact string the dish joined with — from
    /// every connection of `peer`, and frees its `max_subscriptions` slot.
    /// The dish is not told
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.3); it receives nothing more on that filter, and a dish that joins
    /// again meets the admission again. Returns how many joins went.
    pub fn evict(&self, peer: &PeerIdentity, filter: &str) -> usize {
        self.hub().evict(peer, filter)
    }
}

/// What a copy's writer task is told.
enum SegItem {
    Chunk(Bytes),
    Finish,
}

/// The sending side of one copy, kept by the [`Segment`].
struct CopyTx {
    tx: mpsc::Sender<SegItem>,
    budget: Arc<Semaphore>,
}

/// One segment, open on every dish that was joined when it opened.
///
/// [`Segment::write`] never waits: a dish without room for a chunk loses
/// the segment, counted. Dropping a segment without [`Segment::finish`]
/// resets every copy, so no dish mistakes a partial segment for a whole one.
pub struct Segment {
    number: u64,
    topic: Arc<str>,
    copies: Vec<CopyTx>,
    drops: Arc<DropTable>,
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

    /// Hands `chunk` to every copy still open; returns how many that is.
    ///
    /// A copy whose dish has no budget or queue room left for the chunk is
    /// dropped and counted, and its dish loses this segment. Fails with
    /// [`Error::LimitExceeded`] for a chunk above 4 GiB, which no budget can
    /// account.
    pub fn write(&mut self, chunk: impl Into<Bytes>) -> Result<usize, Error> {
        let chunk = chunk.into();
        let len = u32::try_from(chunk.len()).map_err(|_| Error::LimitExceeded)?;
        let (topic, drops) = (&self.topic, &self.drops);
        self.copies.retain(|copy| {
            let Ok(permit) = copy.budget.try_acquire_many(len) else {
                drops.record(topic, DropCause::SubscriberBudget);
                return false;
            };
            match copy.tx.try_send(SegItem::Chunk(chunk.clone())) {
                Ok(()) => {
                    // Given back by the writer once the bytes are written.
                    permit.forget();
                    true
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    drops.record(topic, DropCause::SubscriberQueue);
                    false
                }
                // The copy already ended — superseded or expired — and its
                // task counted why.
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
        Ok(self.copies.len())
    }

    /// Ends the segment on every copy still open; returns how many that is.
    pub fn finish(mut self) -> usize {
        let copies = std::mem::take(&mut self.copies);
        let mut finished = 0;
        for copy in copies {
            match copy.tx.try_send(SegItem::Finish) {
                Ok(()) => finished += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.drops.record(&self.topic, DropCause::SubscriberQueue);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        finished
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

/// What a copy's task needs besides its channel and deadline.
struct Copy {
    path: Arc<str>,
    topic: Arc<str>,
    number: u64,
    ctl: Arc<CopyCtl>,
    budget: Arc<Semaphore>,
    drops: Arc<DropTable>,
}

/// How a copy's race ended.
enum Stop {
    Superseded,
    Expired,
}

async fn expiry(deadline: &mut Option<Pin<Box<tokio::time::Sleep>>>) {
    match deadline {
        Some(sleep) => sleep.as_mut().await,
        None => std::future::pending().await,
    }
}

/// Writes one dish's copy of one segment, racing every step against
/// supersession and the dish's deadline.
async fn segment_copy(
    conn: ConnHandle,
    copy: Copy,
    mut rx: mpsc::Receiver<SegItem>,
    mut deadline: Option<Pin<Box<tokio::time::Sleep>>>,
) {
    let stop = run_copy(&conn, &copy, &mut rx, &mut deadline).await;
    match stop {
        Some(Stop::Superseded) => copy.drops.record(&copy.topic, DropCause::Superseded),
        Some(Stop::Expired) => copy.drops.record(&copy.topic, DropCause::Expired),
        None => {}
    }
    copy.ctl.ended.store(true, Ordering::Release);
    // Chunks still queued were charged to the dish's budget and will never
    // be written: give their bytes back.
    rx.close();
    while let Ok(item) = rx.try_recv() {
        if let SegItem::Chunk(chunk) = item {
            copy.budget.add_permits(chunk.len());
        }
    }
}

async fn run_copy(
    conn: &ConnHandle,
    copy: &Copy,
    rx: &mut mpsc::Receiver<SegItem>,
    deadline: &mut Option<Pin<Box<tokio::time::Sleep>>>,
) -> Option<Stop> {
    let ctl = &copy.ctl;
    let opened = tokio::select! {
        biased;
        () = ctl.superseded() => return Some(Stop::Superseded),
        () = expiry(deadline) => return Some(Stop::Expired),
        opened = open_copy(conn, copy) => opened,
    };
    let mut stream = match opened {
        Ok(stream) => stream,
        Err(Error::NoParkedConnection) => {
            copy.drops
                .record(&copy.topic, DropCause::NoParkedConnection);
            return None;
        }
        Err(e) => {
            tracing::debug!(error = %e, "a segment copy could not be opened");
            return None;
        }
    };
    // Bytes handed to the stream: what the finish grace has to carry.
    let mut sent: u64 = 0;
    loop {
        let item = tokio::select! {
            biased;
            () = ctl.superseded() => return reset(&mut stream, Stop::Superseded),
            () = expiry(deadline) => return reset(&mut stream, Stop::Expired),
            item = rx.recv() => item,
        };
        match item {
            Some(SegItem::Chunk(chunk)) => {
                let written = tokio::select! {
                    biased;
                    () = ctl.superseded() => Err(Stop::Superseded),
                    () = expiry(deadline) => Err(Stop::Expired),
                    written = stream.write_all(&chunk) => Ok(written),
                };
                copy.budget.add_permits(chunk.len());
                match written {
                    Ok(Ok(())) => sent += chunk.len() as u64,
                    // The dish refused the stream or went away.
                    Ok(Err(_)) => return None,
                    Err(stop) => return reset(&mut stream, stop),
                }
            }
            Some(SegItem::Finish) => {
                if stream.finish().is_err() {
                    return None;
                }
                let finished = Instant::now();
                // Until the dish's transport holds every byte, the copy can
                // still be superseded or expire: a reset is accepted after
                // FIN until then.
                let mut stopped = stream.stopped();
                tokio::select! {
                    biased;
                    () = expiry(deadline) => return reset(&mut stream, Stop::Expired),
                    receipt = &mut stopped => return acked(ctl, receipt),
                    () = ctl.superseded() => {}
                }
                // A whole copy that is only waiting for its acknowledgement
                // is not stalled. Its bytes may still be on their way at the
                // pace of the congestion window, and the receipt follows the
                // last of them by a round trip: a successor opened meanwhile
                // would reset a copy the dish is about to hold whole. It gets
                // that time before the supersession takes it.
                let grace = conn
                    .conn
                    .path_stats()
                    .map_or(Duration::ZERO, |p| finish_grace(p.rtt, p.cwnd, sent));
                return tokio::select! {
                    biased;
                    () = expiry(deadline) => reset(&mut stream, Stop::Expired),
                    receipt = &mut stopped => acked(ctl, receipt),
                    () = conn.exec.sleep(grace.saturating_sub(finished.elapsed())) => {
                        reset(&mut stream, Stop::Superseded)
                    }
                };
            }
            // The segment was dropped unfinished, or this copy was dropped
            // for its budget or queue, which the writer already counted.
            None => {
                stream.reset(codes::CANCELED);
                return None;
            }
        }
    }
}

/// Opens a dish's datagram flow for one topic and sends the datagram that
/// waited for it; on failure the slot goes, so the next datagram tries again.
async fn open_datagram_flow(
    conn: ConnHandle,
    path: Arc<str>,
    topic: Arc<str>,
    flows: Arc<Mutex<HashMap<Arc<str>, FlowSlot>>>,
    drops: Arc<DropTable>,
) {
    let meta = FlowMeta::default().with_topic(topic.as_ref());
    let opened = open_flow_on(&conn, &path, &meta).await;
    let mut flows = lock(&flows);
    match opened {
        Ok(flow) => {
            // The dish may have left the topic meanwhile, which removed the
            // slot; the new flow then just closes.
            let Some(slot) = flows.get_mut(&topic) else {
                return;
            };
            if let FlowSlot::Opening(pending) = slot
                && let Some(body) = pending.take()
            {
                match flow.send(body) {
                    Ok(()) => {}
                    Err(Error::TooLarge { .. }) => drops.record(&topic, DropCause::TooLarge),
                    Err(_) => drops.record(&topic, DropCause::SubscriberQueue),
                }
            }
            *slot = FlowSlot::Open(flow);
        }
        Err(e) => {
            if let Some(FlowSlot::Opening(Some(_))) = flows.remove(&topic) {
                let cause = match e {
                    Error::DatagramsUnavailable => DropCause::NoDatagrams,
                    _ => DropCause::SubscriberQueue,
                };
                drops.record(&topic, cause);
            }
        }
    }
}

fn reset(stream: &mut SendHalf, stop: Stop) -> Option<Stop> {
    stream.reset(codes::CANCELED);
    Some(stop)
}

/// Records a finished copy's receipt: delivered means acknowledged, which no
/// successor supersedes any more.
fn acked(ctl: &CopyCtl, receipt: Result<Option<u64>, Error>) -> Option<Stop> {
    if matches!(receipt, Ok(None)) {
        ctl.acked.store(true, Ordering::Release);
    }
    None
}

/// Moves a radio's datagram segments from one flow into its dish's queue:
/// stale ones against the newest delivered on the topic are discarded, and a
/// full queue discards rather than waits.
pub(crate) async fn pump_flow(route: DishRoute, flow: IncomingFlow) {
    let Some(topic) = flow.info().topic.clone() else {
        // A radio names its topic; a flow without one is not a segment.
        return;
    };
    while let Some(datagram) = flow.recv().await {
        let Some((segment, offset)) = split_flow_datagram(&datagram) else {
            route.shared.overflow.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if !route.shared.fresh(&topic, segment) {
            route.shared.stale.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let received = Received::Datagram {
            topic: topic.clone(),
            segment,
            payload: datagram.slice(offset..),
        };
        if route.queue.try_send(received).is_err() {
            route.shared.overflow.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn open_copy(conn: &ConnHandle, copy: &Copy) -> Result<SendHalf, Error> {
    let header = DataHeader {
        topic: Some(copy.topic.to_string()),
        segment: Some(copy.number),
        ..DataHeader::addressed(copy.path.as_ref())
    };
    let mut stream = conn.open_uni().await?;
    write_data_preamble(&mut stream, &header).await?;
    Ok(stream)
}

// --- dish side ----------------------------------------------------------------

/// What a dish receives.
#[derive(Debug)]
pub enum Received {
    /// A stream segment: read it as it arrives. Its topic and number are on
    /// [`crate::IncomingMeta::topic`] and [`crate::IncomingMeta::segment`].
    Segment(IncomingTransfer),
    /// A datagram segment: one packet, whole.
    Datagram {
        /// The topic it was sent on.
        topic: String,
        /// Its segment number on that topic.
        segment: u64,
        /// The payload.
        payload: Bytes,
    },
}

/// The dish's per-topic memory of the newest segment it delivered, and its
/// counters.
pub(crate) struct DishShared {
    newest: Mutex<HashMap<String, u64>>,
    stale: AtomicU64,
    overflow: AtomicU64,
    max_topics: usize,
}

impl DishShared {
    /// `true` when `segment` is newer than every segment delivered on
    /// `topic`, which it then becomes. At the table's cap an untracked topic
    /// is always fresh: the check needs memory, and memory is bounded.
    fn fresh(&self, topic: &str, segment: u64) -> bool {
        let mut newest = lock(&self.newest);
        match newest.get_mut(topic) {
            Some(last) if segment <= *last => false,
            Some(last) => {
                *last = segment;
                true
            }
            None => {
                if newest.len() < self.max_topics {
                    newest.insert(topic.to_owned(), segment);
                }
                true
            }
        }
    }
}

/// A dish's route on its connection's namespace.
#[derive(Clone)]
pub(crate) struct DishRoute {
    queue: mpsc::Sender<Received>,
    shared: Arc<DishShared>,
}

/// Delivers one arrived stream segment to its dish, or discards it: stale
/// against the newest delivered on its topic, or with the dish's queue full.
/// Never blocks the connection.
pub(crate) fn deliver_segment(route: &DishRoute, transfer: IncomingTransfer) {
    let meta = transfer.meta();
    let (Some(topic), Some(segment)) = (meta.topic.as_deref(), meta.segment) else {
        transfer.refuse(codes::UNSUPPORTED);
        return;
    };
    if !route.shared.fresh(topic, segment) {
        route.shared.stale.fetch_add(1, Ordering::Relaxed);
        transfer.refuse(codes::CANCELED);
        return;
    }
    if let Err(e) = route.queue.try_send(Received::Segment(transfer)) {
        route.shared.overflow.fetch_add(1, Ordering::Relaxed);
        if let Received::Segment(transfer) = e.into_inner() {
            transfer.refuse(codes::CANCELED);
        }
    }
}

/// What a dish does on every connection it gets: fill the reverse pool
/// where the transport needs one, claim its path, and re-send its joins.
struct DishAttach {
    joins: Arc<Mutex<HashMap<String, Option<Duration>>>>,
    route: DishRoute,
}

impl Attach for DishAttach {
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
                .register(path, Route::Dish(self.route.clone()))?;
            if conn.conn.needs_reverse_pool() {
                let maintaining = ConnHandle::clone(conn);
                conn.exec
                    .spawn(async move { maintaining.conn.maintain_reverse().await });
            }
            let joins: Vec<(String, Option<Duration>)> = lock(&self.joins)
                .iter()
                .map(|(f, age)| (f.clone(), *age))
                .collect();
            for (filter, max_age) in joins {
                send_subscription(
                    conn,
                    FrameKind::Subscribe,
                    path,
                    &filter,
                    max_age.map(millis),
                )
                .await?;
            }
            Ok(())
        })
    }
}

fn millis(age: Duration) -> u64 {
    u64::try_from(age.as_millis()).unwrap_or(u64::MAX)
}

/// State of a dish.
pub struct DishState {
    peer: Peer,
    joins: Arc<Mutex<HashMap<String, Option<Duration>>>>,
    queue: tokio::sync::Mutex<mpsc::Receiver<Received>>,
    shared: Arc<DishShared>,
}

impl DishState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>, depth: usize) -> DishState {
        let (queue_tx, queue) = mpsc::channel(depth);
        let joins = Arc::new(Mutex::new(HashMap::new()));
        let shared = Arc::new(DishShared {
            newest: Mutex::new(HashMap::new()),
            stale: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
            max_topics: runtime.config.limits.max_sequence_scopes,
        });
        let attach = DishAttach {
            joins: Arc::clone(&joins),
            route: DishRoute {
                queue: queue_tx,
                shared: Arc::clone(&shared),
            },
        };
        DishState {
            peer: Peer::with_attach(runtime, tls, Some(Arc::new(attach))),
            joins,
            queue: tokio::sync::Mutex::new(queue),
            shared,
        }
    }
}

impl Dish {
    /// Connects to a radio at `url` and joins every topic joined so far. A
    /// redial joins again: what was sent in between is gone.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state().peer.connect(url).await
    }

    /// Number of connected radios.
    pub fn peer_count(&self) -> usize {
        self.state().peer.peer_count()
    }

    /// The event stream of this dish's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state().peer.events()
    }

    /// Joins every topic `filter` matches, with the latency budget `max_age`:
    /// a copy the radio cannot get acknowledged within it, on the radio's
    /// clock from the segment's open, is reset and counted. The filter
    /// grammar is Pub/Sub's. Joining a filter again updates its `max_age`.
    pub async fn join(&self, filter: &str, max_age: Option<Duration>) -> Result<(), Error> {
        weida_protocol::filter::validate(filter)?;
        lock(&self.state().joins).insert(filter.to_owned(), max_age);
        for (conn, path) in self.state().peer.live_peers() {
            send_subscription(
                &conn,
                FrameKind::Subscribe,
                &path,
                filter,
                max_age.map(millis),
            )
            .await?;
        }
        Ok(())
    }

    /// Leaves a filter. An unknown filter is ignored.
    pub async fn leave(&self, filter: &str) -> Result<(), Error> {
        if lock(&self.state().joins).remove(filter).is_none() {
            return Ok(());
        }
        for (conn, path) in self.state().peer.live_peers() {
            send_subscription(&conn, FrameKind::Unsubscribe, &path, filter, None).await?;
        }
        Ok(())
    }

    /// Waits for the next segment.
    pub async fn recv(&self) -> Result<Received, Error> {
        let mut queue = self.state().queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }

    /// Segments discarded on arrival because a newer one on the same topic
    /// had already been delivered.
    pub fn stale(&self) -> u64 {
        self.state().shared.stale.load(Ordering::Relaxed)
    }

    /// Segments discarded on arrival because the receive queue was full.
    pub fn overflow(&self) -> u64 {
        self.state().shared.overflow.load(Ordering::Relaxed)
    }
}

impl Drop for DishState {
    fn drop(&mut self) {
        // Best effort, through the connection actor for the same reason a
        // subscriber's drop is: a destructor may run with no reactor.
        let joins: Vec<String> = lock(&self.joins).keys().cloned().collect();
        for (conn, path) in self.peer.live_peers() {
            conn.namespace.unregister(&path);
            for filter in &joins {
                conn.notify(Ctl::SendUnsubscribe {
                    path: Arc::clone(&path),
                    filter: filter.clone(),
                });
            }
        }
    }
}
