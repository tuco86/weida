//! The broker: queues at endpoint paths, and admission into them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use weida::{
    Acceptor, Acknowledgement, ConsumerId, CursorLevel, Cursors, Error, ErrorCode, Incoming,
    IncomingMeta, IncomingRequest, IncomingTransfer, Listener, Reporter, TransferMeta,
};

use crate::consumers::Consumers;

use crate::queue::{DeliveryId, Queue, QueueStats, QueuedMessage, Refusal};

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
    pub queue_bytes: usize,
    /// Queues this broker will register. Default 64.
    pub max_queues: usize,
    /// Deliveries one subscription may have **unsettled** at once. Default
    /// 256.
    ///
    /// An unsettled delivery is one this broker has handed out and not seen a
    /// consumer report on. The bound is **per subscription**, which is what
    /// the name says: one consumer that stops reporting costs its
    /// own slots and nobody else's, and its messages stay charged against
    /// [`BrokerConfig::queue_bytes`] until they settle or come back.
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
}

impl Default for BrokerConfig {
    fn default() -> BrokerConfig {
        BrokerConfig {
            queues: Vec::new(),
            queue_bytes: 8 * 1024 * 1024,
            max_queues: 64,
            max_unsettled: 256,
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

/// How one outstanding delivery ended.
///
/// Sent by the watcher task that holds the delivery's [`Reporter`]-side
/// cursors, and consumed by the queue's own event loop — so a settlement is
/// an event of the **one** loop rather than a second writer into the queue,
/// which is the invariant every comment in this file rests on.
#[derive(Debug)]
enum Settlement {
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

/// One queue, its consumers, and the acceptor that feeds both.
struct Served {
    acceptor: Acceptor,
    queue: Mutex<Queue>,
    /// Two locks rather than one, and never held together across an `await`:
    /// a delivery takes a message under one and a turn under the other, then
    /// writes with neither held.
    consumers: Mutex<Consumers>,
    /// How a finished report reaches this queue's loop.
    ///
    /// Capacity is `max_unsettled`, which is the per-*subscription* bound, so
    /// a queue with several consumers can fill it. That is deliberate and it
    /// is harmless: the receiving end is the queue's own loop, which never
    /// waits on anything a watcher holds, so a full channel makes a watcher
    /// wait and nothing else — and a watcher that waits is a settlement
    /// applied a moment later, never one lost. Sizing it by the bound rather
    /// than by consumer count keeps the number an operator already sets the
    /// only one there is.
    settled: mpsc::Sender<Settlement>,
    /// The other end, taken once by [`serve_queue`].
    ///
    /// It lives here rather than being passed in so that `Served` stays the
    /// one object a queue is, and it is an `Option` so that taking it is
    /// visibly once: a second serving task for one queue would be two writers
    /// to one order, which this file's whole design refuses.
    settlements: Mutex<Option<mpsc::Receiver<Settlement>>>,
    /// The bound, copied here so the delivery scan does not reach back into
    /// the broker's configuration on every candidate.
    max_unsettled: usize,
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
            // The sender lives here so a watcher task can reach it; the
            // receiver lives beside it and is taken once, by the one task
            // allowed to touch this queue's state.
            let (settled, settlements) = mpsc::channel(config.max_unsettled.max(1));
            queues.insert(
                path.clone(),
                Arc::new(Served {
                    acceptor,
                    queue: Mutex::new(Queue::new(config.queue_bytes)),
                    consumers: Mutex::new(Consumers::default()),
                    settled,
                    settlements: Mutex::new(Some(settlements)),
                    max_unsettled: config.max_unsettled,
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
    /// case: it has one order and one budget, so admitting one message at a
    /// time is the queue's own shape rather than a limitation. Backpressure is
    /// the runtime's: a producer whose payload the broker has not started
    /// reading is stalled by QUIC flow control, not queued in a second buffer.
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

/// One queue's event loop: admission, subscriptions, credit, delivery, and
/// the settlements its own deliveries report.
///
/// Everything the peer causes on this path arrives through one channel, so the
/// loop needs no lock of its own between events and a delivery scan runs after
/// whichever event could have made one possible.
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
/// **A settlement is an event of this loop and not a second writer.** A
/// watcher task per outstanding delivery holds that delivery's cursors and
/// sends one [`Settlement`] when the report resolves; the loop is what moves
/// the queue's state, so "a queue is one order and one budget" survives the
/// arrival of an outcome that nobody asked for. The two sources are selected
/// rather than polled in turn, and `accept` returning an error ends the loop
/// either way.
async fn serve_queue(served: Arc<Served>) {
    let mut settlements = served
        .settlements
        .lock()
        .expect("settlement mutex poisoned")
        .take()
        .expect("a queue is served once");
    loop {
        let event = tokio::select! {
            accepted = served.acceptor.accept() => accepted,
            // `None` is unreachable while `served` lives, because `Served`
            // holds the sender: the arm exists so the loop cannot spin if
            // that ever stops being true.
            settled = settlements.recv() => {
                match settled {
                    Some(settlement) => {
                        apply(&served, settlement);
                        pump(&served).await;
                        continue;
                    }
                    None => return,
                }
            }
        };
        match event {
            Ok(Incoming::Exchange(request)) => {
                admit_exchange(&served, request).await;
                pump(&served).await;
            }
            Ok(Incoming::Stream(transfer)) => {
                admit_transfer(&served, transfer).await;
                pump(&served).await;
            }
            Ok(Incoming::Subscribed(consumer)) => {
                tracing::debug!(
                    path = served.acceptor.path(),
                    filter = consumer.filter(),
                    "a consumer subscribed"
                );
                let with_credit = served
                    .consumers
                    .lock()
                    .expect("consumer mutex poisoned")
                    .subscribe(consumer);
                // Normally there is nothing to scan for: a fresh subscription
                // has zero credit. The exception is a grant that overtook this
                // SUBSCRIBE and was held for it — then this event is the one
                // that made a delivery possible, and no later one would.
                if with_credit {
                    tracing::debug!(
                        path = served.acceptor.path(),
                        "a grant had arrived before its subscribe; delivering now"
                    );
                    pump(&served).await;
                }
            }
            Ok(Incoming::Credit(grant)) => {
                let raised = served
                    .consumers
                    .lock()
                    .expect("consumer mutex poisoned")
                    .grant(grant.id, &grant.filter, grant.limit);
                if raised {
                    pump(&served).await;
                }
            }
            Ok(Incoming::Unsubscribed { id, filter }) => {
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
                    .requeue_all(id, filter.as_deref());
                if requeued > 0 {
                    tracing::debug!(
                        path = served.acceptor.path(),
                        requeued,
                        "a subscription went with deliveries outstanding"
                    );
                }
                // A scan, because this event can be the one that unblocks the
                // queue: the subscription that just left may be the consumer
                // whose failed write ended the last round, and the messages it
                // put back at the head are now some other consumer's to take.
                pump(&served).await;
            }
            // A queue carries messages that must arrive, and a flow carries
            // units that are worthless once late: the two do not mix, so the
            // flow is refused rather than queued.
            Ok(Incoming::Flow(flow)) => {
                tracing::debug!(path = served.acceptor.path(), "refusing a datagram flow");
                flow.refuse();
            }
            Err(e) => {
                tracing::debug!(path = served.acceptor.path(), error = %e, "queue stopped serving");
                return;
            }
        }
    }
}

/// Delivers as far as credit and messages allow.
///
/// One message to one consumer, each as a one-way transfer, until either the
/// queue is empty or no matching subscription has credit left. That is the
/// whole scheduling policy: a queue stops at the limit its consumers stated
/// and resumes when one of them raises it
/// ([0003](https://github.com/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
/// §4.2).
///
/// **A failed write costs that consumer the rest of the round, and nobody
/// else anything.** The message goes back to the head of the queue and the
/// credit is given back, so retrying the same pairing immediately would spin
/// on it forever. Abandoning the whole scan instead would strand every other
/// message and every other consumer until some producer happened to admit
/// another message, which on an idle queue is never. So the failing
/// subscription is excluded for the remainder of this scan and the scan
/// continues. The exclusion list is the termination argument: every
/// iteration either delivers a message or removes one subscription from
/// consideration, and both are finite.
///
/// **Serialized with admission, deliberately and at a cost.** A queue is one
/// order and one budget, so its events are one loop; the cost is that a
/// consumer whose flow-control window is full holds this queue's producers up
/// while a write waits. Nothing here invents a timeout to paper over it: an
/// unresponsive consumer costs its own `max_unsettled` slots and nothing
/// else, which is the answer 0029 §4.6 settles on.
///
/// **A delivery is not a deletion.** Each one orders `Processed` on
/// its own DATA header, keeps the cursors that come back, and hands the
/// message to [`Queue::hold`] — still charged — with a watcher task on the
/// report. The message leaves this queue only when a consumer says it may.
async fn pump(served: &Served) {
    // Empty in the ordinary case, so a round that delivers costs no
    // allocation for the failures it does not have.
    let mut failed: Vec<(ConsumerId, String)> = Vec::new();
    loop {
        let Some(next) = next_delivery(served, &failed) else {
            return;
        };
        let (id, filter, consumer, mut message) = next;
        let meta = TransferMeta {
            content_type: message.content_type.clone(),
            content_len: Some(message.body.len() as u64),
            trace: message.trace,
            topic: message.topic.clone(),
            achieved: None,
            ..TransferMeta::default()
        }
        // The order that makes settlement possible, and the only level this
        // queue asks its consumer for: `Processed` is the consumer's own
        // statement about its own hop (`GUARANTEES.md` §1).
        .with_report([SETTLEMENT]);
        match deliver(&consumer, meta, &message.body).await {
            Ok(cursors) => {
                // Counted here and not before the write, because this is the
                // attempt: the bytes are out, a consumer has them, and a
                // message in `unsettled` with `attempts == 1` is on its first
                // delivery. A write that failed above never reached anybody
                // and must not make the next delivery claim to be a repeat.
                // The count is not on the wire yet; here it is state.
                message.attempts += 1;
                let delivery = served
                    .queue
                    .lock()
                    .expect("queue mutex poisoned")
                    .hold(message, id, &filter);
                match cursors {
                    Some(cursors) => watch(served, delivery, cursors),
                    // No cursors means the header carried no report, which
                    // cannot happen for a delivery this function built — but
                    // a settlement that will never come would strand the
                    // message and its slot, so the honest answer is to give
                    // it back now rather than to hold it forever.
                    None => {
                        tracing::error!(
                            path = served.acceptor.path(),
                            "a delivery ordered a report and got no cursors; requeuing"
                        );
                        served
                            .queue
                            .lock()
                            .expect("queue mutex poisoned")
                            .requeue(delivery);
                        failed.push((id, filter));
                    }
                }
            }
            Err(e) => {
                // The write never landed: give the credit back, put the
                // message where it was — at the head of the queue — and carry
                // on without this consumer. Its subscription is removed for
                // good by its own `Unsubscribed` event, which pumps again.
                tracing::debug!(
                    path = served.acceptor.path(),
                    error = %e,
                    "a delivery failed; requeuing the message and skipping the consumer"
                );
                served
                    .consumers
                    .lock()
                    .expect("consumer mutex poisoned")
                    .undo(id, &filter);
                served
                    .queue
                    .lock()
                    .expect("queue mutex poisoned")
                    .push_front(message);
                failed.push((id, filter));
            }
        }
    }
}

/// The level a delivery orders and a settlement is.
///
/// One constant rather than a parameter: what a queue needs to know is that
/// its consumer processed the message, and a queue that let an operator
/// choose a different level would be letting them choose what "settled"
/// means.
const SETTLEMENT: CursorLevel = CursorLevel::Known(Acknowledgement::Processed);

/// Writes one delivery and keeps the report it ordered.
///
/// [`weida::Consumer::deliver`] is the whole-payload convenience and it drops
/// the cursors; this is the same three calls with the handle kept, which is
/// the only reason it exists here rather than in `weida`.
async fn deliver(
    consumer: &weida::Consumer,
    meta: TransferMeta,
    body: &[u8],
) -> Result<Option<Cursors>, Error> {
    let mut transfer = consumer.open(meta).await?;
    let cursors = transfer.cursors();
    transfer.write_all(body).await?;
    transfer.finish()?;
    Ok(cursors)
}

/// Watches one delivery's report and tells the queue's loop how it ended.
///
/// One task per outstanding delivery, bounded by `max_unsettled` per
/// subscription — the same shape a fan-out's writer and a survey's collector
/// have. It holds no lock and touches no queue state: it reads cursors and
/// sends one [`Settlement`], which is what keeps the queue's own state in one
/// loop.
///
/// The wait is unbounded on purpose and bounded in fact: `changed` resolves
/// with `None` when the reporter finishes, when the stream is reset **and**
/// when the connection goes away, so a consumer that vanishes ends this task
/// without a timer ([0029](https://github.com/tuco86/weida/blob/main/docs/decisions/0029-a-report-is-relayed-a-certificate-is-not.md)
/// §4.6).
fn watch(served: &Served, delivery: DeliveryId, mut cursors: Cursors) {
    let settled = served.settled.clone();
    let path = served.acceptor.path().to_owned();
    tokio::spawn(async move {
        let outcome = loop {
            match cursors.changed().await {
                Some(set) if set.offset(SETTLEMENT).is_some() => {
                    break Settlement::Settled(delivery);
                }
                // A level the delivery did not order, or an offset that moved
                // on a level that is not this one: not a settlement, keep
                // reading.
                Some(_) => continue,
                None => break Settlement::Lost(delivery),
            }
        };
        // A closed channel means the queue stopped serving, which is the one
        // case where nothing is owed to anybody.
        if settled.send(outcome).await.is_err() {
            tracing::debug!(%path, "a settlement arrived after its queue stopped");
        }
    });
}

/// Moves the queue's state for one settlement.
///
/// The only place `unsettled` shrinks, and it runs on the queue's own loop, so
/// "one order, one budget" holds for an outcome nobody asked for as much as
/// for an admission somebody did.
fn apply(served: &Served, settlement: Settlement) {
    let mut queue = served.queue.lock().expect("queue mutex poisoned");
    match settlement {
        Settlement::Settled(delivery) => {
            if queue.settle(delivery) {
                tracing::debug!(path = served.acceptor.path(), "a delivery settled");
            }
        }
        Settlement::Lost(delivery) => {
            if queue.requeue(delivery) {
                tracing::debug!(
                    path = served.acceptor.path(),
                    "a report ended without a settlement; the message is the queue's again"
                );
            }
        }
    }
}

/// Takes the first message some consumer may have, and that consumer's turn.
///
/// Both locks are taken here and released before the caller writes, always in
/// this order — queue, then consumers — so two queues cannot deadlock against
/// each other. A message is taken **only** when a consumer for it exists: a
/// queue with no credit keeps its messages, which is the point of a queue.
///
/// The credit pre-check is what keeps a filling queue linear. Without it, this
/// scan asked every subscription about every held message after every
/// admission — Θ(N²) eligible-set constructions for a queue nobody consumes,
/// with N bounded only by `queue_bytes / PER_MESSAGE_OVERHEAD` and driven
/// entirely by a remote producer. One pass over the subscriptions, bounded by
/// `max_subscriptions`, answers the whole question in that case.
///
/// **`max_unsettled` is enforced here, and it is the one place it can be.**
/// A subscription with that many deliveries outstanding is not eligible, so a
/// consumer that stops reporting stops being given messages — and the ones it
/// already holds stay charged against the queue's budget until it reports or
/// its subscription goes. The check is per subscription rather than per queue
/// on purpose: one silent consumer must not starve the ones that answer.
fn next_delivery(
    served: &Served,
    skip: &[(ConsumerId, String)],
) -> Option<(ConsumerId, String, weida::Consumer, QueuedMessage)> {
    let mut queue = served.queue.lock().expect("queue mutex poisoned");
    let mut consumers = served.consumers.lock().expect("consumer mutex poisoned");
    if !consumers.any_credit() {
        return None;
    }
    for index in 0..queue.len() {
        // The topic is borrowed for exactly the length of the question, so a
        // scan that finds nothing allocates nothing.
        let turn = {
            let topic = queue.topic_at(index).expect("index is in range");
            consumers.take_turn(topic, skip, &|id, filter| {
                queue.unsettled_for(id, filter) < served.max_unsettled
            })
        };
        let Some((id, filter, consumer)) = turn else {
            continue;
        };
        let message = queue.take(index).expect("index is in range");
        return Some((id, filter, consumer, message));
    }
    None
}

/// Admits a producer's exchange and confirms it, or refuses it on the reply
/// half.
///
/// The confirm is the reply: DATA carrying the achieved level and a FIN, no
/// payload. An exchange already has a reply half, so a publisher confirm needs
/// no frame kind of its own — and the stream is the correlation, so the confirm
/// names nothing (0018 §4.6).
async fn admit_exchange(served: &Served, mut request: IncomingRequest) {
    let Some(room) = room_for(served, request.meta()) else {
        // Refused before a byte is read, which is what `Reject` backpressure
        // means; `refuse` stops the request half too, so a producer still
        // writing stops instead of filling a window nobody will read. The
        // question is [`Queue::push`]'s own arithmetic asked in advance, so a
        // message that fits the queue exactly is admitted with an empty
        // payload rather than refused for having a read cap of zero.
        request.refuse(ErrorCode::Rejected).await;
        return;
    };
    let body = request.take_body();
    // Taken before the payload is consumed: `reporter()` reads the order the
    // producer put in its DATA header, and `read_message` consumes the
    // transfer.
    let reporter = body.reporter();
    let queued = match read_message(body, request.meta(), room).await {
        Ok(message) => message,
        Err(ReadFailed::TooLarge) => {
            request.refuse(ErrorCode::Rejected).await;
            return;
        }
        Err(ReadFailed::Broken) => return,
    };
    let admitted_bytes = queued.body.len() as u64;
    // The producer's trace, logged where it is still available: after the
    // exchange ends nothing can reconstruct which trace a queued message
    // belongs to, and a delivery continues it.
    if let Some(trace) = queued.trace {
        tracing::debug!(
            path = served.acceptor.path(),
            bytes = queued.body.len(),
            trace_id = %trace.trace_id_hex(),
            "admitted a message into a queue"
        );
    }
    // The guard ends with this statement, on purpose: holding a `std::sync`
    // lock across the confirm's `await` would make the whole loop `!Send` and,
    // worse, hold a queue shut for a network write.
    let admitted = served
        .queue
        .lock()
        .expect("queue mutex poisoned")
        .push(queued);
    match admitted {
        Ok(()) => {
            // Both answers, and they say different things. The reply half is
            // the application's channel and carries the confirm it always
            // carried (DATA key `8`); the cursor is for a producer that
            // ordered one, and a producer that ordered none pays nothing.
            report_accepted(reporter, admitted_bytes);
            confirm(request).await;
        }
        // A refusal reports **nothing**: the producer learns from the
        // refusal, not from a missing cursor.
        Err(Refusal::Full) => request.refuse(ErrorCode::Rejected).await,
    }
}

/// Admits a one-way transfer, and reports a cursor if the producer ordered
/// one.
///
/// This is the shape [0024](https://github.com/tuco86/weida/blob/main/docs/decisions/0024-three-families-one-back-channel.md)
/// §4.4a exists for: a producer that orders `Accepted` gets a reliable verdict
/// **on one unidirectional payload stream**, with no exchange and no reply
/// half. A producer that orders nothing gets the transport receipt and nothing
/// more — the honest spelling of `acks=0` (0018 §4.6). A refusal has
/// nowhere to go on a unidirectional stream, so a queue with no room is a
/// `STOP_SENDING(REJECTED)` — dropping the unread transfer — and no cursor.
async fn admit_transfer(served: &Served, transfer: IncomingTransfer) {
    let Some(room) = room_for(served, transfer.meta()) else {
        // Dropping an unread payload is the refusal: the handle stops the
        // stream with `REJECTED`, which is the only answer a unidirectional
        // stream can carry.
        drop(transfer);
        return;
    };
    let meta = transfer.meta().clone();
    let reporter = transfer.reporter();
    let queued = match read_message(transfer, &meta, room).await {
        Ok(message) => message,
        Err(_) => return,
    };
    let admitted_bytes = queued.body.len() as u64;
    if let Err(Refusal::Full) = served
        .queue
        .lock()
        .expect("queue mutex poisoned")
        .push(queued)
    {
        // Not a race: a queue's events are one loop, so no second admission
        // ran between the room question and this push. What is left is the
        // payload's own slack — [`QueuedMessage::charge`] counts the `Vec`'s
        // allocation, and a payload read up to the cap may have reserved past
        // it. The payload is already read, so there is no stream left to stop
        // and **nothing is sent**: the producer that ordered a cursor sees it
        // never arrive, which is the only answer a one-way stream has left
        // once its FIN is in.
        tracing::debug!(
            path = served.acceptor.path(),
            bytes = admitted_bytes,
            "dropping a one-way message whose allocation overran the queue's budget"
        );
        return;
    }
    report_accepted(reporter, admitted_bytes);
}

/// How long a spawned report may wait for the producer's stream credit.
///
/// Not a correctness parameter: a cursor is best effort and nothing waits on
/// it. It bounds how long a task may sit parked in `open_uni` for a peer that
/// advertised `initial_max_streams_uni = 0` and never raised it — without it,
/// one parked task per admitted message would be an allocation a remote
/// producer drives and only shutdown releases.
const REPORT_DEADLINE: Duration = Duration::from_secs(10);

/// Reports `Accepted` at the admitted body length, once, and FINs — in a task
/// of its own.
///
/// `Accepted` is a **verdict rather than a prefix** (0023 §4.2): it is true of
/// the whole message or of none of it, so there is exactly one record even in
/// `Progress` mode and there is no granularity to configure. A level this
/// broker cannot reach — `Stored`, with no store — is simply not reported, and
/// the producer sees it missing rather than failed.
///
/// **The one thing in this loop that may be abandoned, and the one thing that
/// must not be awaited in it.** `Reporter::report` opens the cursor stream
/// lazily, and opening a stream blocks until the peer raises
/// `max_concurrent_uni_streams`, with the deadline left to the caller. Awaited
/// from [`serve_queue`] — the only driver of this queue — a single producer
/// that orders `Accepted` and never raises its stream limit would park
/// admission for every other producer and delivery for every consumer on the
/// path, permanently. A cursor is best effort by construction and nothing
/// waits on it, so it is the one thing here that may be handed to a task with
/// a deadline and forgotten: losing it costs the producer a record it was
/// never promised, while parking the loop costs everyone the queue.
fn report_accepted(reporter: Option<Reporter>, bytes: u64) {
    let Some(mut reporter) = reporter else {
        return;
    };
    tokio::spawn(async move {
        let report = async move {
            let level = CursorLevel::Known(Achieved::Accepted.level());
            if let Err(e) = reporter.report(level, bytes).await {
                tracing::debug!(error = %e, "failed to report an accepted cursor");
            }
            if let Err(e) = reporter.finish().await {
                tracing::debug!(error = %e, "failed to finish a cursor report");
            }
        };
        let done = tokio::time::timeout(REPORT_DEADLINE, report).await;
        if done.is_err() {
            tracing::debug!("gave up on a cursor: the peer opened no stream for it");
        }
    });
}

/// Why a payload did not become a queued message.
enum ReadFailed {
    /// It exceeded the room the queue had; nothing was buffered past the cap.
    TooLarge,
    /// The peer or the connection failed mid-payload.
    Broken,
}

/// Reads a payload under the queue's remaining budget.
async fn read_message(
    body: IncomingTransfer,
    meta: &IncomingMeta,
    room: usize,
) -> Result<QueuedMessage, ReadFailed> {
    let topic = meta.topic.clone();
    let content_type = meta.content_type.clone();
    let trace = meta.trace;
    match body.collect(room).await {
        Ok(body) => Ok(QueuedMessage {
            body,
            topic,
            content_type,
            attempts: 0,
            trace,
        }),
        Err(Error::LimitExceeded) => Err(ReadFailed::TooLarge),
        Err(e) => {
            tracing::debug!(error = %e, "a producer's payload failed mid-read");
            Err(ReadFailed::Broken)
        }
    }
}

/// The payload cap for the next message on this queue, or `None` when the
/// queue has no room for a message at all.
fn room_for(served: &Served, meta: &IncomingMeta) -> Option<usize> {
    served
        .queue
        .lock()
        .expect("queue mutex poisoned")
        .room_for(meta)
}

/// Writes the confirm: the achieved level in the reply header, then FIN.
async fn confirm(request: IncomingRequest) {
    let meta = TransferMeta::default().with_achieved(Achieved::Accepted.level());
    match request.reply(meta).await {
        Ok(reply) => {
            if let Err(e) = reply.finish() {
                tracing::debug!(error = %e, "failed to finish a confirm");
            }
        }
        Err(e) => tracing::debug!(error = %e, "failed to send a confirm"),
    }
}
