//! The broker: queues at endpoint paths, and the loop that serves each.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use weida::{Acceptor, Acknowledgement, Consumer, ConsumerId, Error, Incoming, Listener};

use crate::admission::{self, MAX_REPORTS_IN_FLIGHT};
use crate::consumers::{Consumers, SubKey};
use crate::delivery::{self, Dispatch};
use crate::queue::{DeliveryId, Queue, QueueStats};

/// What this broker can certify about a message, and the list is the whole
/// list.
///
/// One variant, on purpose. `Stored(Written)` is defined as surviving "the
/// broker process dying: crash, restart or orderly stop", and this broker
/// holds every message in memory, so reporting it would be a prohibited claim
/// rather than an optimistic one —
/// "`Stored` MUST NOT be reported for an in-memory buffer"
/// (`docs/GUARANTEES.md` §1,
/// [0018](https://github.com/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
/// §4.2). The enum is the enforcement: there is no value of this type that
/// names a durable level, so no code path here can accidentally issue one.
/// `Stored` appears with a durable store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Achieved {
    /// The broker has taken responsibility for the message in memory,
    /// according to its configured policy.
    Accepted,
}

impl Achieved {
    /// The wire level this certificate is.
    pub fn level(self) -> Acknowledgement {
        match self {
            Achieved::Accepted => Acknowledgement::Accepted,
        }
    }
}

/// How a broker is configured.
///
/// Every queue this broker serves is named here: there is no declare frame and
/// no way for a peer to create, configure or delete a queue, following AMQP
/// 1.0, where node lifecycle is "entirely out of scope of the core standard"
/// (0018 §4.5). A producer addressing a path with no queue gets ERROR
/// `{UNKNOWN_ENDPOINT}` from the runtime's own dispatch, because the broker
/// never registered that path.
#[derive(Clone, Debug)]
pub struct BrokerConfig {
    /// Endpoint paths to serve as queues.
    ///
    /// Local configuration, so its length is the operator's choice — but
    /// bounded anyway by `max_queues`, because a configuration file is still
    /// input.
    pub queues: Vec<String>,
    /// Bytes one queue may hold, charged as payload plus the retained labels
    /// plus [`crate::PER_MESSAGE_OVERHEAD`] per message.
    ///
    /// Default 8 MiB, the same number `weida-zmq`'s per-peer queue budget
    /// uses: a bound big enough that a queue decouples producer from consumer,
    /// small enough that a thousand queues are a gigabyte rather than a
    /// swap storm. A queue at this bound refuses admission (§4.8); it never
    /// discards a message it has already confirmed.
    ///
    /// What counts against it is everything the queue has promised room for:
    /// messages held, messages handed out and not yet reported on, and the
    /// bytes of payloads that producers are still sending.
    pub queue_bytes: usize,
    /// Queues this broker will register. Default 64.
    pub max_queues: usize,
    /// Deliveries one subscription may have **unsettled** at once. Default
    /// 256.
    ///
    /// An unsettled delivery is one this broker has handed out and not seen a
    /// consumer report on. The bound is **per subscription**, which is what
    /// the name says: one subscription that stops reporting costs its own
    /// slots, and its messages stay charged against
    /// [`BrokerConfig::queue_bytes`] until they settle or come back. A
    /// consumer holds many subscriptions, one per filter, and the number of
    /// filters is its own to choose, so what one *connection* may hold across
    /// all of them is [`BrokerConfig::max_unsettled_per_consumer`].
    ///
    /// It is not a credit limit. Credit is *cumulative* — the total a
    /// subscription will ever accept ([0003](https://github.com/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
    /// §4.3) — so clamping that would cap a subscription's lifetime delivery
    /// count rather than its outstanding one. This bounds what may be in
    /// flight without an outcome. Nothing here is an acknowledgement: a
    /// consumer reports, it does not acknowledge
    /// ([0029](https://github.com/tuco86/weida/blob/main/docs/decisions/0029-a-report-is-relayed-a-certificate-is-not.md)
    /// §4.7).
    ///
    /// `0` is a queue that admits and confirms and never delivers, because no
    /// subscription is ever eligible. That is the literal reading of the bound
    /// and it is left legal rather than refused, but it is not a pause switch:
    /// a consumer pauses with credit, which is its own to give.
    pub max_unsettled: usize,
    /// Deliveries one consumer **connection** may have unsettled at once, over
    /// every filter it subscribed with. Default 1024.
    ///
    /// [`BrokerConfig::max_unsettled`] alone is a bound times the number of
    /// filters a connection opens, and a connection may hold hundreds: with a
    /// bound of 256 each, eight filters pin more of a queue than most
    /// consumers' whole backlog, and a hostile one pins all of it. This is the
    /// bound on the connection, whatever it subscribed to.
    pub max_unsettled_per_consumer: usize,
    /// Admissions one queue runs at once. Default 1024, and at least 1.
    ///
    /// An admission is a producer's message being read, confirmed or refused:
    /// a task, a stream and a reservation, held for as long as the producer
    /// takes. This prevents a flood of producers that stop mid-message from
    /// costing the queue a task each without bound. At the limit the queue
    /// stops reading new streams until an admission finishes, which QUIC's flow
    /// control turns into producers waiting — nothing is refused for being
    /// busy, and nothing waits in a buffer of the broker's own. One producer
    /// that stops holds one admission, for at most
    /// [`BrokerConfig::io_timeout`]; it takes as many stalled producers as this
    /// number to make anyone else wait.
    pub max_inflight_admissions: usize,
    /// How long a wait on a peer may go without progress. Default 10 s.
    ///
    /// Every read of a producer's payload, every write of a delivery, every
    /// confirm, refusal and cursor report waits on a peer's flow-control
    /// credit, and a peer that never raises it would otherwise hold the task,
    /// the stream and — for a payload — the queue's reserved bytes for ever.
    /// A payload or delivery that moves slower than
    /// [`crate::peer::MIN_PEER_RATE`] is cut after about one window too, so a
    /// peer cannot hold on by sending a byte per window.
    ///
    /// A producer past it loses the message it was sending. A consumer past
    /// it loses its subscription — the message it was being given goes back
    /// to the head of the queue — and resubscribes like any consumer that was
    /// removed: the broker has no way to speak to it outside a delivery.
    pub io_timeout: Duration,
}

impl Default for BrokerConfig {
    fn default() -> BrokerConfig {
        BrokerConfig {
            queues: Vec::new(),
            queue_bytes: 8 * 1024 * 1024,
            max_queues: 64,
            max_unsettled: 256,
            max_unsettled_per_consumer: 1024,
            max_inflight_admissions: 1024,
            io_timeout: Duration::from_secs(10),
        }
    }
}

impl BrokerConfig {
    /// A configuration serving exactly the given queue paths.
    pub fn with_queues<I, S>(queues: I) -> BrokerConfig
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        BrokerConfig {
            queues: queues.into_iter().map(Into::into).collect(),
            ..BrokerConfig::default()
        }
    }
}

/// How an outstanding delivery's write or report ended.
///
/// Sent by the tasks that write a delivery and watch its report, and consumed
/// by the queue's own event loop — so every outcome is an event of the **one**
/// loop rather than a second writer into the queue's delivery state, which is
/// the invariant every comment in this file rests on.
#[derive(Debug)]
pub(crate) enum Event {
    /// The bytes are out: the subscription may be given another message while
    /// this one waits for its report.
    Written(DeliveryId),
    /// The write failed or the consumer stopped reading: the message goes back
    /// and the subscription is dropped.
    Failed(DeliveryId),
    /// The consumer reported the level the delivery ordered.
    Settled(DeliveryId),
    /// No further cursors are coming and the level never arrived: the
    /// reporter finished without it, the stream was reset, or the connection
    /// went away. Three cases deliberately indistinguishable
    /// ([0023](https://github.com/tuco86/weida/blob/main/docs/decisions/0023-completion-is-a-cursor.md)),
    /// and all three mean the same thing to a queue: nobody has taken
    /// responsibility, so the message is the queue's again.
    Lost(DeliveryId),
}

/// Who a delivery was handed to.
pub(crate) type Owner = SubKey<ConsumerId>;

/// Events a queue's loop may have waiting from its delivery tasks.
///
/// A task that finds the channel full waits, which makes an outcome applied a
/// moment later and never one lost; the loop never waits on anything a task
/// holds. Two outcomes per delivery at most, so this is a few hundred
/// deliveries' worth.
const EVENT_BACKLOG: usize = 1024;

/// One queue, its consumers, and the acceptor that feeds both.
pub(crate) struct Served {
    pub(crate) acceptor: Acceptor,
    /// Touched by the loop and by admission tasks, each for the length of one
    /// synchronous step.
    pub(crate) queue: Mutex<Queue<Owner>>,
    /// Two locks rather than one, and never held across an `await`: a delivery
    /// takes a message under one and a turn under the other, then writes with
    /// neither held. Always taken in this order — queue, then consumers — so
    /// two steps cannot deadlock against each other.
    pub(crate) consumers: Mutex<Consumers<ConsumerId, Consumer>>,
    /// How a finished write or report reaches this queue's loop.
    pub(crate) events: mpsc::Sender<Event>,
    /// The other end, taken once by [`serve_queue`].
    ///
    /// It lives here rather than being passed in so that `Served` stays the
    /// one object a queue is, and it is an `Option` so that taking it is
    /// visibly once: a second serving task for one queue would be two writers
    /// to one order, which this file's whole design refuses.
    event_rx: Mutex<Option<mpsc::Receiver<Event>>>,
    /// Rings when an admission put a message in the queue: the one change to
    /// the queue the loop does not make itself, and the reason to look for a
    /// delivery.
    pub(crate) wake: Notify,
    /// Admissions in flight, one permit each.
    pub(crate) admissions: Arc<Semaphore>,
    /// Cursor reports in flight, one permit each.
    pub(crate) reports: Arc<Semaphore>,
    /// See [`BrokerConfig::io_timeout`].
    pub(crate) io_timeout: Duration,
}

struct Inner {
    config: BrokerConfig,
    /// Registered once, at construction, and never mutated: a queue's
    /// existence is configuration, so the map needs no lock.
    queues: HashMap<String, Arc<Served>>,
}

/// A broker: queues at endpoint paths, admitting messages into memory.
///
/// Construction registers every configured queue, so a queue exists — and a
/// producer is answered — from the moment the broker is built, before
/// [`Broker::serve`] runs. Serving is the caller's task to drive: this crate
/// starts no reactor and owns no runtime.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

impl Broker {
    /// Registers every configured queue on `listener`.
    ///
    /// Fails with [`Error::LimitExceeded`] when the configuration names more
    /// than `max_queues` queues — checked before anything is registered — and
    /// with whatever [`Listener::acceptor`] refuses for an invalid or
    /// already-claimed path.
    ///
    /// **A failure part-way through leaves the earlier paths claimed.** A
    /// listener's namespace has no public release, and inventing one to
    /// half-undo a bad configuration would be the wrong repair: a path this
    /// broker could not take is a configuration error to fix and start again
    /// with, not a transient condition to retry. The queues registered before
    /// the failure are unusable by anyone else on that listener, which is
    /// exactly as visible as it should be.
    pub fn new(listener: &Listener, config: BrokerConfig) -> Result<Broker, Error> {
        if config.queues.len() > config.max_queues {
            return Err(Error::LimitExceeded);
        }
        let mut queues = HashMap::with_capacity(config.queues.len());
        for path in &config.queues {
            let acceptor = listener.acceptor(path)?;
            // The sender lives here so a task can reach it; the receiver lives
            // beside it and is taken once, by the one task allowed to move
            // this queue's delivery state.
            let (events, event_rx) = mpsc::channel(EVENT_BACKLOG);
            queues.insert(
                path.clone(),
                Arc::new(Served {
                    acceptor,
                    queue: Mutex::new(Queue::new(config.queue_bytes)),
                    consumers: Mutex::new(Consumers::new(
                        config.max_unsettled,
                        config.max_unsettled_per_consumer,
                    )),
                    events,
                    event_rx: Mutex::new(Some(event_rx)),
                    wake: Notify::new(),
                    admissions: Arc::new(Semaphore::new(config.max_inflight_admissions.max(1))),
                    reports: Arc::new(Semaphore::new(MAX_REPORTS_IN_FLIGHT)),
                    io_timeout: config.io_timeout,
                }),
            );
        }
        Ok(Broker {
            inner: Arc::new(Inner { config, queues }),
        })
    }

    /// The configuration this broker was built with.
    pub fn config(&self) -> &BrokerConfig {
        &self.inner.config
    }

    /// What a queue holds, or `None` when no queue is registered on `path`.
    pub fn stats(&self, path: &str) -> Option<QueueStats> {
        let served = self.inner.queues.get(path)?;
        Some(served.queue.lock().expect("queue mutex poisoned").stats())
    }

    /// How many subscriptions a queue serves, or `None` when no queue is
    /// registered on `path`.
    pub fn consumer_count(&self, path: &str) -> Option<usize> {
        let served = self.inner.queues.get(path)?;
        Some(
            served
                .consumers
                .lock()
                .expect("consumer mutex poisoned")
                .len(),
        )
    }

    /// Deliveries counted against one subscription's credit, or `None` when
    /// the queue or the subscription is unknown.
    pub fn delivered(&self, path: &str, id: ConsumerId, filter: &str) -> Option<u64> {
        let served = self.inner.queues.get(path)?;
        served
            .consumers
            .lock()
            .expect("consumer mutex poisoned")
            .delivered(id, filter)
    }

    /// Serves every queue until the listener goes away.
    ///
    /// One task per queue, because a queue is a serialization point in any
    /// case: it has one order of deliveries and one budget. That task never
    /// waits on a peer — it starts producers' reads and consumers' writes in
    /// tasks of their own and reacts to how they end — so a producer or a
    /// consumer that stops answering costs itself and nobody else. Backpressure
    /// is the budget's: a producer whose message does not fit is refused, and
    /// so is one that arrives while the queue is already admitting as many as
    /// it may.
    pub async fn serve(&self) {
        let mut tasks = JoinSet::new();
        // The path behind each task id. A `JoinError` carries the id of the
        // task that panicked and nothing else, and "some queue died" is not an
        // answer an operator can act on.
        let mut paths = HashMap::new();
        for (path, served) in &self.inner.queues {
            let served = Arc::clone(served);
            let handle = tasks.spawn(async move { serve_queue(served).await });
            paths.insert(handle.id(), path.as_str());
        }
        while let Some(joined) = tasks.join_next_with_id().await {
            match joined {
                Ok((id, ())) => {
                    let path = paths.get(&id).copied().unwrap_or("?");
                    tracing::debug!(path, "a queue task returned");
                }
                // A panicking queue and an orderly return are the same event
                // to a `JoinSet`, and they are not the same thing here: this
                // path has stopped serving while the others carry on.
                Err(e) => {
                    let path = paths.get(&e.id()).copied().unwrap_or("?");
                    tracing::error!(path, error = %e, "a queue task died");
                }
            }
        }
    }
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("queues", &self.inner.queues.len())
            .finish_non_exhaustive()
    }
}

/// One queue's event loop: subscriptions, credit, the start of admissions, and
/// what its deliveries report.
///
/// **Nothing in this loop waits on a peer.** Every arm either changes state
/// under a lock held for one synchronous step or starts a task that will wait
/// for it; an `.await` here is on the loop's own sources and nowhere else. That
/// is what lets one producer that stops mid-message and one consumer that
/// stops reading sit beside a healthy pair without the pair noticing: the
/// admission or the write that is stuck is a task, bounded by a deadline, and
/// the loop is already serving the next event.
///
/// Everything a consumer causes on this path arrives through one channel, so
/// the loop needs no lock of its own between events and a delivery scan runs
/// after whichever event could have made one possible.
///
/// **The channel does not order the peer's intent.** It orders the *tasks*
/// that reached it: every control frame rides its own unidirectional stream
/// and the accept loop spawns one task per inbound stream, so a CREDIT can and
/// does overtake the SUBSCRIBE it belongs to — the credit path only needs this
/// queue's route, while the subscription path also reserves against
/// `max_subscriptions` and builds a `Consumer`. That is why an unmatched grant
/// is held rather than dropped ([`Consumers::grant`]) and why a subscription
/// may arrive with credit already standing.
///
/// **An outcome is an event of this loop and not a second writer.** A task per
/// delivery writes it and a watcher holds its cursors; each sends an [`Event`]
/// and the loop is what moves the queue's delivery state. The sources are
/// selected rather than polled in turn, and `accept` returning an error ends
/// the loop either way.
///
/// **A permit comes before the accept.** The loop reads the next stream off
/// the transport only while it holds one of the queue's admission permits, and
/// hands it to the admission task it starts. At the limit the loop stops
/// reading — QUIC's flow control holds the producers back, with no message
/// refused and none queued in a second buffer — until an admission finishes,
/// and what an admission waits on is bounded by its deadline. Control frames
/// share the channel, so they wait their turn behind streams while it lasts.
async fn serve_queue(served: Arc<Served>) {
    let mut events = served
        .event_rx
        .lock()
        .expect("event mutex poisoned")
        .take()
        .expect("a queue is served once");
    let mut permit: Option<OwnedSemaphorePermit> = None;
    loop {
        let may_deliver = tokio::select! {
            got = Arc::clone(&served.admissions).acquire_owned(), if permit.is_none() => {
                match got {
                    Ok(got) => {
                        permit = Some(got);
                        false
                    }
                    // A semaphore nothing closes: the arm exists so the loop
                    // cannot spin if that ever stops being true.
                    Err(_) => return,
                }
            },
            accepted = served.acceptor.accept(), if permit.is_some() => match accepted {
                Ok(incoming) => handle(&served, incoming, &mut permit),
                Err(e) => {
                    tracing::debug!(path = served.acceptor.path(), error = %e, "queue stopped serving");
                    return;
                }
            },
            // `None` is unreachable while `served` lives, because `Served`
            // holds the sender: the arm exists so the loop cannot spin if
            // that ever stops being true.
            event = events.recv() => match event {
                Some(event) => {
                    apply(&served, event);
                    true
                }
                None => return,
            },
            () = served.wake.notified() => true,
        };
        if may_deliver {
            pump(&served);
        }
    }
}

/// Reacts to one inbound thing, and says whether it can have made a delivery
/// possible.
///
/// A producer's message does not: it is read in a task that takes the permit,
/// and when it lands the task rings [`Served::wake`] — only then is there
/// something to deliver. Anything else leaves the permit for the next accept.
fn handle(
    served: &Arc<Served>,
    incoming: Incoming,
    permit: &mut Option<OwnedSemaphorePermit>,
) -> bool {
    match incoming {
        Incoming::Exchange(request) => {
            let permit = permit.take().expect("an accept follows a permit");
            admission::start_exchange(served, request, permit);
            false
        }
        Incoming::Stream(transfer) => {
            let permit = permit.take().expect("an accept follows a permit");
            admission::start_stream(served, transfer, permit);
            false
        }
        Incoming::Subscribed(consumer) => {
            tracing::debug!(
                path = served.acceptor.path(),
                filter = consumer.filter(),
                "a consumer subscribed"
            );
            let id = consumer.id();
            let filter: Arc<str> = Arc::from(consumer.filter());
            let overtaken = served
                .consumers
                .lock()
                .expect("consumer mutex poisoned")
                .subscribe(id, filter, consumer);
            if overtaken {
                tracing::debug!(
                    path = served.acceptor.path(),
                    "a grant had arrived before its subscribe; delivering now"
                );
            }
            true
        }
        Incoming::Credit(grant) => {
            served
                .consumers
                .lock()
                .expect("consumer mutex poisoned")
                .grant(grant.id, &grant.filter, grant.limit);
            true
        }
        Incoming::Unsubscribed { id, filter } => {
            served
                .consumers
                .lock()
                .expect("consumer mutex poisoned")
                .remove(id, filter.as_deref());
            // **The requeue of 0029 §4.6, and the only one there is.**
            // A subscription that is gone reports nothing ever again, so
            // whatever it held is the queue's again — that is the answer
            // to a dead consumer, and it is why no timer exists: packet
            // loss is QUIC's retransmission, a consumer that holds a
            // delivery without reporting is bounded by `max_unsettled`,
            // and this is the third case.
            let requeued = served
                .queue
                .lock()
                .expect("queue mutex poisoned")
                .requeue_all(|owner| {
                    owner.id == id && filter.as_deref().is_none_or(|f| &*owner.filter == f)
                });
            if requeued > 0 {
                tracing::debug!(
                    path = served.acceptor.path(),
                    requeued,
                    "a subscription went with deliveries outstanding"
                );
            }
            // A scan, because this event can be the one that unblocks the
            // queue: the messages it put back at the head are now some other
            // consumer's to take.
            true
        }
        // A queue carries messages that must arrive, and a flow carries
        // units that are worthless once late: the two do not mix, so the
        // flow is refused rather than queued.
        Incoming::Flow(flow) => {
            tracing::debug!(path = served.acceptor.path(), "refusing a datagram flow");
            flow.refuse();
            false
        }
    }
}

/// Moves the queue's state for one outcome of a delivery.
///
/// The only place `unsettled` shrinks, and it runs on the queue's own loop, so
/// "one order, one budget" holds for an outcome nobody asked for as much as
/// for an admission somebody did. An outcome for a delivery the queue no
/// longer holds — its subscription was removed, which took the delivery back —
/// finds nothing and changes nothing: whichever of the two arrives second is
/// the stale one.
fn apply(served: &Served, event: Event) {
    let mut queue = served.queue.lock().expect("queue mutex poisoned");
    let mut consumers = served.consumers.lock().expect("consumer mutex poisoned");
    match event {
        Event::Written(id) => {
            if let Some(owner) = queue.written(id) {
                consumers.written(owner, id);
            }
        }
        Event::Settled(id) => {
            if let Some(owner) = queue.settle(id) {
                consumers.released(&owner, id);
                tracing::debug!(path = served.acceptor.path(), "a delivery settled");
            }
        }
        Event::Lost(id) => {
            if let Some(owner) = queue.requeue(id) {
                consumers.released(&owner, id);
                tracing::debug!(
                    path = served.acceptor.path(),
                    "a report ended without a settlement; the message is the queue's again"
                );
            }
        }
        Event::Failed(id) => {
            if let Some(owner) = queue.requeue(id) {
                consumers.fail(&owner);
                // The subscription is gone, so whatever else it held is the
                // queue's again, as for an UNSUBSCRIBE: an entry that outlived
                // its subscription would be charged to nobody's bound for as
                // long as the connection stayed open.
                let also = queue.requeue_all(|held| *held == owner);
                tracing::debug!(
                    path = served.acceptor.path(),
                    also,
                    "a delivery failed; the message is the queue's again and its consumer is dropped"
                );
            }
        }
    }
}

/// Starts every delivery that credit and messages allow.
///
/// One message to one consumer, each as a one-way transfer written by a task of
/// its own, until either the queue has nothing a ready subscription selects or
/// no subscription is ready. That is the whole scheduling policy: a queue
/// stops at the limit its consumers stated and resumes when one of them
/// raises it ([0003](https://github.com/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
/// §4.2).
///
/// Every iteration moves one message out of the line, so the loop ends. A
/// delivery that later fails puts its message back and drops its subscription,
/// and the event that says so runs this again: nobody is retried in a loop
/// here, which is what the old per-scan exclusion list was for.
fn pump(served: &Served) {
    while let Some(dispatch) = take_next(served) {
        delivery::spawn(served, dispatch);
    }
}

/// Takes the first message some ready subscription may have, and that
/// subscription's turn.
///
/// Both locks are taken here and released before anything is written, always
/// in this order — queue, then consumers. A message is taken **only** when a
/// consumer for it exists: a queue with no credit keeps its messages, which is
/// the point of a queue.
///
/// **What this costs is bounded by what changed, not by what is queued.** With
/// no ready subscription it is one comparison — however many messages wait
/// and however many subscriptions are silent. With some, the scan starts where
/// the last one stopped looking ([`Queue::scan_from`]), so an admission costs
/// one message's worth of matching, and the whole line is walked again only
/// when a subscription became ready or a message went back to the front. A
/// consumer with a filter nothing queued selects, and a backlog nobody ready
/// wants, were each a full pass per event.
///
/// **`max_unsettled` and `max_unsettled_per_consumer` are enforced by who is
/// ready**, which is the one place they can be: a subscription at its bound is
/// not in the set, so a consumer that stops reporting stops being given
/// messages — and the ones it already holds stay charged against the queue's
/// budget until it reports or its subscription goes.
fn take_next(served: &Served) -> Option<Dispatch> {
    let mut queue = served.queue.lock().expect("queue mutex poisoned");
    let mut consumers = served.consumers.lock().expect("consumer mutex poisoned");
    if consumers.take_gained() {
        queue.rescan();
    }
    if !consumers.any_ready() {
        return None;
    }
    for index in queue.scan_from()..queue.len() {
        // The topic is borrowed for exactly the length of the question, so a
        // scan that finds nothing allocates nothing.
        let topic = queue.topic_at(index).expect("index is in range");
        let Some(key) = consumers.pick(topic) else {
            continue;
        };
        let outgoing = queue
            .dispatch(index, key.clone())
            .expect("index is in range");
        let consumer = consumers
            .dispatch(&key, outgoing.id)
            .expect("a picked subscription exists");
        return Some(Dispatch { consumer, outgoing });
    }
    queue.scanned();
    None
}
