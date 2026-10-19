//! RADIO/DISH: lossy fan-out of segments
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
//!
//! A radio numbers segments per topic (DATA key `13`) and sends each to
//! every dish joined when it opens, one uni stream per dish. The segment
//! machinery — supersession, expiry, the budget and the write that never
//! waits — is [`crate::segment`]'s, shared with `Peer::segment`; a radio adds
//! the dish table, a dish's `max_age` and the datagram segments.
//!
//! The dish discards a segment older than the newest it delivered on the
//! topic and never blocks its connection on a full queue.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use weida_core::{Error, Limits, PeerIdentity};
use weida_protocol::header::limits::MAX_TOPIC_BYTES;
use weida_protocol::{FrameKind, codes, encode_varint, filter, split_flow_datagram};

use crate::config::ClientTls;
use crate::conn::{ConnHandle, Ctl};
use crate::endpoint::{Dish, Radio, send_subscription};
use crate::flow::{Flow, FlowMeta, IncomingFlow, open_flow_on};
use crate::identity::PeerChain;
use crate::listener::{Namespace, Route};
use crate::pubsub::{Causes, DishDrops, DropCause, DropTable, FilterIndex, TopicDrops};
use crate::reconnect::PeerEvents;
use crate::runtime::RuntimeInner;
use crate::segment::{CopyTarget, Segment, SegmentTerms, SegmentTopics, open_segment};
use crate::stream::{Attach, Peer};
use crate::transfer::IncomingTransfer;

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

/// What a dish asks for when it joins a filter
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.3).
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinTerms {
    /// The dish's latency budget: a copy still unfinished or unacknowledged
    /// this long after its segment opened is reset at the radio. With
    /// several matching filters the smallest applies.
    pub max_age: Option<Duration>,
    /// The highest layer the dish wants, `0..=15`; layers above it are
    /// never opened for this dish, and that is not a drop. With several
    /// matching filters the largest applies, and a filter without a cap
    /// means no cap.
    pub max_layer: Option<u8>,
}

impl JoinTerms {
    /// Sets the latency budget.
    #[must_use]
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    /// Caps the layers the dish is sent.
    #[must_use]
    pub fn with_max_layer(mut self, max_layer: u8) -> Self {
        self.max_layer = Some(max_layer);
        self
    }

    /// Combines the terms of two filters of one dish that both match a topic:
    /// the smallest `max_age` among those that carry one, and the largest
    /// `max_layer`, none if either has none.
    fn merge(&self, other: &JoinTerms) -> JoinTerms {
        JoinTerms {
            max_age: match (self.max_age, other.max_age) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            },
            max_layer: self.max_layer.zip(other.max_layer).map(|(a, b)| a.max(b)),
        }
    }
}

// --- radio side ---------------------------------------------------------------

/// What the copies and datagrams to one dish go through. Shared with every
/// send, so no send holds the dish table's lock.
struct DishPort {
    conn: ConnHandle,
    /// Chunk bytes this dish's copies may hold unwritten, over all topics.
    budget: Arc<Semaphore>,
    /// One datagram flow per topic, opened lazily on the connection the join
    /// arrived on; bounded by the topics the dish's filters match.
    flows: Mutex<HashMap<Arc<str>, FlowSlot>>,
    /// What this dish connection lost, over topics
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.6); goes with the entry when the connection closes.
    drops: Arc<Causes>,
}

/// One joined dish: the filters it joined with, and what its copies go
/// through.
struct DishEntry {
    port: Arc<DishPort>,
    /// Filter to what the dish asked for with it.
    filters: HashMap<String, JoinTerms>,
    /// Forgets this entry when its connection closes. Aborted with the entry,
    /// so a radio that goes away releases the connections it held.
    watch: JoinHandle<()>,
}

impl Drop for DishEntry {
    fn drop(&mut self) {
        self.watch.abort();
    }
}

/// The joined dishes of a radio path, by connection id, and the (dish,
/// filter) pairs that select a topic. Both change under one lock, so the
/// index and the dishes always agree.
struct DishTable {
    dishes: HashMap<usize, DishEntry>,
    index: FilterIndex<JoinTerms>,
}

impl DishTable {
    /// The dishes whose connection is still open. A closed connection's entry
    /// goes when its task runs, and a read must not count it in between.
    fn live(&self) -> impl Iterator<Item = (usize, &DishEntry)> {
        self.dishes
            .iter()
            .filter(|(_, dish)| dish.port.conn.conn.close_reason().is_none())
            .map(|(conn_id, dish)| (*conn_id, dish))
    }

    /// The dishes with a filter that selects `topic`, each with the terms its
    /// matching filters combine to. A snapshot: a send runs after the table
    /// is released.
    fn matching(&self, topic: &str) -> Vec<(Arc<DishPort>, JoinTerms)> {
        let mut combined: HashMap<usize, JoinTerms> = HashMap::new();
        self.index.matching(topic, |conn_id, terms| {
            combined
                .entry(conn_id)
                .and_modify(|seen| *seen = seen.merge(terms))
                .or_insert_with(|| terms.clone());
        });
        combined
            .into_iter()
            .filter_map(|(conn_id, terms)| {
                let dish = self.dishes.get(&conn_id)?;
                Some((Arc::clone(&dish.port), terms))
            })
            .collect()
    }

    /// Withdraws `withdrawn` from the dish on `conn_id`: its index entry, its
    /// `max_subscriptions` slot and the flows no remaining filter matches. A
    /// dish goes with its last filter. `false` when it did not hold it.
    fn withdraw(&mut self, conn_id: usize, withdrawn: &str) -> bool {
        let Some(dish) = self.dishes.get_mut(&conn_id) else {
            return false;
        };
        if dish.filters.remove(withdrawn).is_none() {
            return false;
        }
        self.index.remove(conn_id, withdrawn);
        if let Some(subs) = dish.port.conn.subs.as_ref() {
            subs.release(conn_id);
        }
        let filters = &dish.filters;
        lock(&dish.port.flows).retain(|topic, _| filters.keys().any(|f| filter::matches(topic, f)));
        if dish.filters.is_empty() {
            self.dishes.remove(&conn_id);
        }
        true
    }

    /// Forgets the dish on `conn_id`, but only if it still belongs to `conn`:
    /// the entry may be gone already, or hold a newer connection.
    fn forget(&mut self, conn_id: usize, conn: &ConnHandle) {
        let same = self
            .dishes
            .get(&conn_id)
            .is_some_and(|dish| Arc::ptr_eq(&dish.port.conn, conn));
        if !same {
            return;
        }
        if let Some(dish) = self.dishes.remove(&conn_id) {
            for filter in dish.filters.keys() {
                self.index.remove(conn_id, filter);
            }
        }
    }
}

/// A dish's datagram flow for one topic.
enum FlowSlot {
    /// The FLOW header is on its way; holds the newest datagram meanwhile,
    /// never more than one.
    Opening(Option<Bytes>),
    /// The flow is open. `since` is when it opened, and `backoff` is the wait
    /// its next refusal imposes.
    Open {
        flow: Flow,
        since: Instant,
        backoff: Duration,
    },
    /// The dish refused or ended the flow. Datagrams on the topic are counted
    /// drops until `until`; the first one after that opens a flow, and
    /// `backoff` is the wait that flow's refusal imposes.
    Refused { until: Instant, backoff: Duration },
}

/// The first wait after a dish refuses a datagram flow. A dish that answers
/// every FLOW with STOP_SENDING would otherwise cost the radio one stream
/// open per datagram.
const FLOW_BACKOFF_START: Duration = Duration::from_millis(100);

/// The longest wait between two attempts at a flow the dish refuses.
const FLOW_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// How long a flow must stay open before its refusals start over at
/// [`FLOW_BACKOFF_START`]. A refusal arrives after the open has returned, so
/// resetting at the open would hold a dish that refuses every flow at the
/// first wait.
const FLOW_SETTLE: Duration = FLOW_BACKOFF_MAX;

/// The wait a refusal imposes after `backoff`: twice as long, up to the cap.
fn doubled(backoff: Duration) -> Duration {
    (backoff * 2).min(FLOW_BACKOFF_MAX)
}

/// Everything a radio path holds: its dishes, its topics and its drops.
pub(crate) struct RadioHub {
    path: Arc<str>,
    /// The joined dishes. The tasks that forget a dish when its connection
    /// closes hold this weakly, so they do not keep a radio alive.
    dishes: Arc<Mutex<DishTable>>,
    /// Per-topic numbering and the copies the next segment supersedes.
    topics: SegmentTopics,
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
            dishes: Arc::new(Mutex::new(DishTable {
                dishes: HashMap::new(),
                index: FilterIndex::new(),
            })),
            topics: SegmentTopics::new(limits.max_sequence_scopes),
            drops: Arc::new(DropTable::new(limits.max_sequence_scopes)),
            limits,
            admission: Mutex::new((0, None)),
        }
    }

    /// The dish table.
    fn dishes(&self) -> MutexGuard<'_, DishTable> {
        lock(&self.dishes)
    }

    /// A new dish on `ctx`, and the task that forgets it when `ctx` closes.
    fn new_dish(&self, ctx: &ConnHandle, conn_id: usize) -> DishEntry {
        let port = Arc::new(DishPort {
            conn: ConnHandle::clone(ctx),
            // tokio panics above `Semaphore::MAX_PERMITS`.
            budget: Arc::new(Semaphore::new(
                self.limits
                    .subscriber_buffer_bytes
                    .min(Semaphore::MAX_PERMITS),
            )),
            flows: Mutex::new(HashMap::new()),
            drops: Arc::new(Causes::default()),
        });
        let watch = ctx.exec.spawn(forget_when_closed(
            Arc::downgrade(&self.dishes),
            ConnHandle::clone(ctx),
            conn_id,
        ));
        DishEntry {
            port,
            filters: HashMap::new(),
            watch,
        }
    }

    /// Records a join. `reserve` counts a new filter against the
    /// connection's `max_subscriptions` and runs only for a filter the dish
    /// did not already hold; a repeated join replaces its terms.
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
        max_layer: Option<u8>,
        reserve: impl FnOnce() -> Result<(), Error>,
    ) -> Result<(), Error> {
        let conn_id = ctx.conn.stable_id();
        let terms = JoinTerms {
            max_age: max_age_ms.map(Duration::from_millis),
            max_layer,
        };
        let mut guard = loop {
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
        let table = &mut *guard;
        if let Some(dish) = table.dishes.get_mut(&conn_id)
            && let Some(held) = dish.filters.get_mut(&filter)
        {
            *held = terms.clone();
            table.index.insert(conn_id, &filter, terms);
            return Ok(());
        }
        reserve()?;
        let dish = table
            .dishes
            .entry(conn_id)
            .or_insert_with(|| self.new_dish(ctx, conn_id));
        dish.filters.insert(filter.clone(), terms.clone());
        table.index.insert(conn_id, &filter, terms);
        Ok(())
    }

    /// Withdraws a join the dish asked to leave, and releases its slot.
    pub(crate) fn leave(&self, conn_id: usize, filter: &str) {
        self.dishes().withdraw(conn_id, filter);
    }

    /// Withdraws `filter` from every live connection `peer` joined it on; the
    /// number of joins withdrawn.
    pub(crate) fn evict(&self, peer: &PeerIdentity, filter: &str) -> usize {
        let mut table = self.dishes();
        let holders: Vec<usize> = table
            .live()
            .filter(|(_, dish)| {
                dish.port.conn.peer.as_ref() == Some(peer) && dish.filters.contains_key(filter)
            })
            .map(|(conn_id, _)| conn_id)
            .collect();
        let mut evicted = 0;
        for conn_id in holders {
            if table.withdraw(conn_id, filter) {
                evicted += 1;
            }
        }
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
            .live()
            .map(|(conn_id, dish)| {
                (
                    conn_id,
                    ConnHandle::clone(&dish.port.conn),
                    dish.filters.keys().cloned().collect(),
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
        let mut table = self.dishes();
        for (conn_id, filter) in &refused {
            table.withdraw(*conn_id, filter);
        }
    }
}

/// Forgets a dish once its connection closes, so the table holds only what is
/// still connected and no send has to ask each dish whether it is.
async fn forget_when_closed(dishes: Weak<Mutex<DishTable>>, conn: ConnHandle, conn_id: usize) {
    let _ = conn.conn.closed().await;
    if let Some(dishes) = dishes.upgrade() {
        lock(&dishes).forget(conn_id, &conn);
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
    /// supersedes segment *n* on that topic: a copy whose writer had not
    /// finished it, or whose queued chunks the transport does not take at
    /// once, is reset at once; a finished copy gets the time its path needs,
    /// `rtt + 50 ms + rtt * bytes / cwnd`, and is reset only if it is still
    /// unacknowledged then. On a local transport that grace is zero.
    ///
    /// `terms` set the sender's side
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.2): a copy expires at the smaller of `terms.max_age` and its
    /// dish's `max_age`, its stream carries `terms.priority` within its
    /// layer, and `terms.follows_upstream` keeps a copy against its
    /// successor unless a write has to wait.
    ///
    /// The dish set is fixed here: a dish that joins while the segment is in
    /// flight receives the next one. Zero dishes is not an error. Fails with
    /// [`Error::LimitExceeded`] for a topic above 256 bytes, or when
    /// `max_sequence_scopes` topics all have a copy in flight.
    pub fn segment(&self, topic: &str, terms: SegmentTerms) -> Result<Segment, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let hub = self.hub();
        let topic: Arc<str> = Arc::from(topic);
        let number = hub.topics.next(&topic)?;
        let targets: Vec<CopyTarget> = hub
            .dishes()
            .matching(&topic)
            .into_iter()
            .map(|(port, joined)| CopyTarget {
                conn: ConnHandle::clone(&port.conn),
                path: Arc::clone(&hub.path),
                budget: Arc::clone(&port.budget),
                max_age: joined.max_age,
                max_layer: joined.max_layer,
                dish: Some(Arc::clone(&port.drops)),
            })
            .collect();
        Ok(open_segment(
            &hub.topics,
            &hub.drops,
            topic,
            number,
            &terms,
            targets,
        ))
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
    /// §4.6 rule 4). A dish that refuses its flow costs a drop per datagram
    /// until the flow's backoff is over, which starts at 100 ms and doubles up
    /// to 30 s; no stream is opened meanwhile.
    ///
    /// The payload is copied once, into the frame that is sent, so the caller
    /// keeps its buffer.
    pub fn datagram(&self, topic: &str, payload: impl AsRef<[u8]>) -> Result<usize, Error> {
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(Error::LimitExceeded);
        }
        let hub = self.hub();
        let topic: Arc<str> = Arc::from(topic);
        let number = hub.topics.next(&topic)?;
        let payload = payload.as_ref();
        let mut frame = Vec::with_capacity(8 + payload.len());
        encode_varint(number, &mut frame).expect("segment numbers stay below 2^62");
        frame.extend_from_slice(payload);
        let body = Bytes::from(frame);

        let dishes = hub.dishes().matching(&topic);
        let mut handed = 0;
        for (port, _) in dishes {
            if !port.conn.agreed_now().is_some_and(|a| a.datagrams) {
                record(&hub.drops, &port.drops, &topic, DropCause::NoDatagrams);
                continue;
            }
            let mut flows = lock(&port.flows);
            match flows.get_mut(&topic) {
                None => {
                    flows.insert(Arc::clone(&topic), FlowSlot::Opening(Some(body.clone())));
                    spawn_flow_open(&port, hub, &topic, FLOW_BACKOFF_START);
                    handed += 1;
                }
                Some(slot) => {
                    let next = match &mut *slot {
                        FlowSlot::Opening(pending) => {
                            if pending.replace(body.clone()).is_some() {
                                record(&hub.drops, &port.drops, &topic, DropCause::SubscriberQueue);
                            }
                            handed += 1;
                            None
                        }
                        FlowSlot::Open {
                            flow,
                            since,
                            backoff,
                        } => match flow.send(body.clone()) {
                            Ok(()) => {
                                handed += 1;
                                if port.conn.exec.now().saturating_duration_since(*since)
                                    >= FLOW_SETTLE
                                {
                                    *backoff = FLOW_BACKOFF_START;
                                }
                                None
                            }
                            Err(Error::TooLarge { .. }) => {
                                record(&hub.drops, &port.drops, &topic, DropCause::TooLarge);
                                None
                            }
                            Err(Error::DatagramsUnavailable) => {
                                record(&hub.drops, &port.drops, &topic, DropCause::NoDatagrams);
                                None
                            }
                            // The flow ended: the dish refused or released it, or its
                            // connection is gone. The datagram is lost, and the flow
                            // waits out its backoff before the next one opens.
                            Err(_) => {
                                record(&hub.drops, &port.drops, &topic, DropCause::SubscriberQueue);
                                Some(FlowSlot::Refused {
                                    until: port.conn.exec.now() + *backoff,
                                    backoff: doubled(*backoff),
                                })
                            }
                        },
                        FlowSlot::Refused { until, backoff } => {
                            if port.conn.exec.now() < *until {
                                record(&hub.drops, &port.drops, &topic, DropCause::SubscriberQueue);
                                None
                            } else {
                                spawn_flow_open(&port, hub, &topic, *backoff);
                                handed += 1;
                                Some(FlowSlot::Opening(Some(body.clone())))
                            }
                        }
                    };
                    if let Some(next) = next {
                        *slot = next;
                    }
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
        self.hub().dishes().live().count()
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

    /// What each joined dish connection lost, summed over topics: one
    /// record per connection, gone when the connection closes, so at most
    /// `Limits::max_connections`
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.6). A copy that found no parked connection is counted per topic
    /// only.
    pub fn dish_drops(&self) -> Vec<DishDrops> {
        self.hub()
            .dishes()
            .live()
            .map(|(_, dish)| dish.port.drops.dish_snapshot(dish.port.conn.peer.clone()))
            .collect()
    }
}

/// Counts one drop on the topic and on the dish's record.
fn record(drops: &DropTable, dish: &Causes, topic: &Arc<str>, cause: DropCause) {
    drops.record(topic, cause);
    dish.record(cause);
}

/// Opens a dish's datagram flow for one topic and sends the datagram that
/// waited for it. A failed open leaves the topic `Refused` for `backoff`; a
/// flow that opens keeps `backoff` as the wait its next refusal imposes.
async fn open_datagram_flow(
    port: Arc<DishPort>,
    path: Arc<str>,
    topic: Arc<str>,
    drops: Arc<DropTable>,
    backoff: Duration,
) {
    let meta = FlowMeta::default().with_topic(topic.as_ref());
    let opened = open_flow_on(&port.conn, &path, &meta).await;
    let mut flows = lock(&port.flows);
    let Some(slot) = flows.get_mut(&topic) else {
        // The dish left the topic meanwhile, which removed the slot; a flow
        // opened now is dropped, and closes with FIN.
        return;
    };
    let pending = match &mut *slot {
        FlowSlot::Opening(pending) => pending.take(),
        // A newer open owns the slot; this flow is dropped, and closes with FIN.
        _ => return,
    };
    match opened {
        Ok(flow) => {
            if let Some(body) = pending {
                match flow.send(body) {
                    Ok(()) => {}
                    Err(Error::TooLarge { .. }) => {
                        record(&drops, &port.drops, &topic, DropCause::TooLarge);
                    }
                    Err(_) => record(&drops, &port.drops, &topic, DropCause::SubscriberQueue),
                }
            }
            *slot = FlowSlot::Open {
                flow,
                since: port.conn.exec.now(),
                backoff,
            };
        }
        Err(e) => {
            if pending.is_some() {
                let cause = match e {
                    Error::DatagramsUnavailable => DropCause::NoDatagrams,
                    _ => DropCause::SubscriberQueue,
                };
                record(&drops, &port.drops, &topic, cause);
            }
            *slot = FlowSlot::Refused {
                until: port.conn.exec.now() + backoff,
                backoff: doubled(backoff),
            };
        }
    }
}

/// Starts opening the flow for `topic` on `port`; `backoff` is the wait its
/// next refusal imposes.
fn spawn_flow_open(port: &Arc<DishPort>, hub: &RadioHub, topic: &Arc<str>, backoff: Duration) {
    port.conn.exec.spawn(open_datagram_flow(
        Arc::clone(port),
        Arc::clone(&hub.path),
        Arc::clone(topic),
        Arc::clone(&hub.drops),
        backoff,
    ));
}

/// Moves a radio's datagram segments from one flow into its dish's queue:
/// stale ones against the newest delivered on the connection, path and topic
/// are discarded, and a full queue discards rather than waits.
pub(crate) async fn pump_flow(ctx: ConnHandle, route: DishRoute, flow: IncomingFlow) {
    let Some(topic) = flow.info().topic.as_deref().map(Arc::<str>::from) else {
        // A radio names its topic; a flow without one is not a segment.
        return;
    };
    while let Some(datagram) = flow.recv().await {
        let Some((segment, offset)) = split_flow_datagram(&datagram) else {
            route.shared.overflow.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if !ctx
            .segments_in
            .fresh(&flow.info().endpoint, &topic, segment, 0)
        {
            route.shared.stale.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        // Copied: a slice would keep the whole receive batch alive, and the
        // queue charges nothing for that.
        let received = Received::Datagram {
            topic: Arc::clone(&topic),
            segment,
            payload: Bytes::copy_from_slice(&datagram[offset..]),
        };
        if route.queue.try_send(received).is_err() {
            route.shared.overflow.fetch_add(1, Ordering::Relaxed);
        }
    }
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
        topic: Arc<str>,
        /// Its segment number on that topic.
        segment: u64,
        /// The payload.
        payload: Bytes,
    },
}

/// A dish's counters. Which segment is stale is the connection's to know
/// (`ConnCtx::segments_in`), so a redial starts over
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.11).
pub(crate) struct DishShared {
    stale: AtomicU64,
    overflow: AtomicU64,
}

/// A dish's route on its connection's namespace.
#[derive(Clone)]
pub(crate) struct DishRoute {
    queue: mpsc::Sender<Received>,
    shared: Arc<DishShared>,
}

/// Delivers one arrived stream segment to its dish, or discards it: stale
/// against the newest delivered on its connection, path and topic, or with
/// the dish's queue full. Never blocks the connection.
pub(crate) fn deliver_segment(
    ctx: &ConnHandle,
    path: &str,
    route: &DishRoute,
    transfer: IncomingTransfer,
) {
    let meta = transfer.meta();
    let (Some(topic), Some(segment)) = (meta.topic.as_deref(), meta.segment) else {
        transfer.refuse(codes::UNSUPPORTED);
        return;
    };
    if !ctx
        .segments_in
        .fresh(path, topic, segment, meta.layer.unwrap_or(0))
    {
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
    joins: Arc<Mutex<HashMap<String, JoinTerms>>>,
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
            let joins: Vec<(String, JoinTerms)> = lock(&self.joins)
                .iter()
                .map(|(f, terms)| (f.clone(), terms.clone()))
                .collect();
            for (filter, terms) in joins {
                send_subscription(
                    conn,
                    FrameKind::Subscribe,
                    path,
                    &filter,
                    terms.max_age.map(millis),
                    terms.max_layer,
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
    joins: Arc<Mutex<HashMap<String, JoinTerms>>>,
    queue: tokio::sync::Mutex<mpsc::Receiver<Received>>,
    shared: Arc<DishShared>,
}

impl DishState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>, depth: usize) -> DishState {
        let (queue_tx, queue) = mpsc::channel(depth);
        let joins = Arc::new(Mutex::new(HashMap::new()));
        let shared = Arc::new(DishShared {
            stale: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
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
    /// redial joins again: what was sent in between is gone, and the numbers
    /// start over on the new connection.
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state().peer.connect(url).await
    }

    /// Number of connected radios.
    pub fn peer_count(&self) -> usize {
        self.state().peer.peer_count()
    }

    /// One record per live connection to a radio, labelled by the URL as
    /// dialled; see [`crate::Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state().peer.connection_stats()
    }

    /// The event stream of this dish's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state().peer.events()
    }

    /// Joins every topic `filter` matches, under `terms`
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.3): a copy the radio cannot get acknowledged within
    /// `terms.max_age`, on the radio's clock from the segment's open, is
    /// reset and counted, and layers above `terms.max_layer` are never sent
    /// to this dish. The filter grammar is Pub/Sub's. Joining a filter again
    /// replaces its terms.
    ///
    /// # Errors
    ///
    /// [`Error::LimitExceeded`] for a `max_layer` above 15; the filter
    /// grammar's error for an invalid filter; a send error from a live
    /// connection.
    pub async fn join(&self, filter: &str, terms: JoinTerms) -> Result<(), Error> {
        if terms
            .max_layer
            .is_some_and(|l| l >= crate::segment::MAX_SEGMENT_LAYERS)
        {
            return Err(Error::LimitExceeded);
        }
        weida_protocol::filter::validate(filter)?;
        lock(&self.state().joins).insert(filter.to_owned(), terms.clone());
        for (conn, path) in self.state().peer.live_peers() {
            send_subscription(
                &conn,
                FrameKind::Subscribe,
                &path,
                filter,
                terms.max_age.map(millis),
                terms.max_layer,
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
            send_subscription(&conn, FrameKind::Unsubscribe, &path, filter, None, None).await?;
        }
        Ok(())
    }

    /// Waits for the next segment.
    pub async fn recv(&self) -> Result<Received, Error> {
        let mut queue = self.state().queue.lock().await;
        queue.recv().await.ok_or(Error::NotConnected)
    }

    /// Segments discarded on arrival because a newer one on the same topic
    /// had already been delivered on the same connection.
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
