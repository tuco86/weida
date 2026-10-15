//! One queue: messages held in memory, under one byte budget — and the
//! deliveries it has handed out and not yet seen settled.
//!
//! Nothing here talks to a peer. The queue answers two questions — is there
//! room, and what is held — and the broker turns those answers into a confirm
//! or a refusal. It holds no state a restart could recover, which is why
//! `Stored` is not expressible anywhere in this crate
//! ([0018](https://github.com/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
//! §4.2).
//!
//! **A delivered message is still this queue's.** Until a consumer reports
//! `Processed`, a message that has gone out lives in the `unsettled` table
//! and **keeps its charge** against the budget: it may come back, so it is
//! memory the queue still owes. A delivery that were a deletion would lose
//! the message with a consumer that died, which is RabbitMQ's `no-ack` mode
//! and which its own documentation calls unsafe.
//!
//! **A delivery lives exactly as long as its entry.** Each one is handed a
//! cancellation signal ([`Outgoing::cancelled`]) whose other end sits in the
//! entry, so that taking the entry away — settled, requeued or refused —
//! ends whatever is still waiting on the delivery: the write, or the task
//! watching its report and the report entry the consumer's connection keeps
//! for it.
//!
//! **Bytes are promised before they are read.** Producers are read
//! concurrently, so "how much room is left" cannot be asked once and acted on
//! later: two readers would each be told the whole remainder. A reader
//! [`Queue::reserve`]s what it is about to buffer, the reservation counts
//! against the budget exactly like a held message does, and
//! [`Queue::push`] turns it into a charge or [`Queue::release`] gives it
//! back. `charged + reserved` never exceeds the budget, whoever is reading.
//!
//! **What a scan has seen is remembered per subscription, not per scan.**
//! Messages are offered in queue order: first the ones that went back — in
//! the order they went back — then the ones that arrived, in the order they
//! finished arriving. A message that goes back is new to every subscription
//! whatever its place in the line, so the two groups are numbered apart
//! ([`Enqueued`]) and a subscription remembers the last of each it was
//! offered ([`Seen`]). A scan then offers a subscription only what it has
//! not seen, and a backlog that nobody ready wants is walked once per
//! subscription rather than once per delivery.

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::iter::Chain;
use std::ops::Range;
use std::sync::Arc;

use tokio::sync::oneshot;
use weida::TraceContext;

/// What one queued message costs beyond its payload and its retained labels.
///
/// A budget that counted payload bytes only would bound nothing for a producer
/// sending empty messages: a million zero-byte messages are zero bytes of
/// payload and a million live `QueuedMessage`s. Charging a fixed overhead per
/// message makes `queue_bytes` bound the message *count* as well —
/// `queue_bytes / PER_MESSAGE_OVERHEAD` is the ceiling — which is what
/// `docs/INVARIANTS.md` asks of any allocation a remote peer can drive.
///
/// The charge of a message is **one number for its whole life**, so it covers
/// the largest footprint the message has: while it is out for delivery it adds
/// an entry in the unsettled table, the task watching its report and the
/// cursor state the connection keeps for that report, which together are about
/// a kilobyte — several times a message at rest. Charging that only when the
/// delivery starts would need a second budget check at a moment when refusing
/// is not an option (the message is already confirmed), so the queue pays for
/// it up front, when refusing is still free.
///
/// The number is a deliberate over-estimate, not a measurement: it is a bound,
/// and a bound that tracks `size_of` would move with every field added here.
pub const PER_MESSAGE_OVERHEAD: usize = 1024;

/// How many consuming connections may refuse one message before the queue
/// gives up on it.
///
/// A consumer refuses a delivery for a reason about the message — a header
/// longer than that connection will read — so the message goes back for the
/// other consumers and the connection that refused is never offered it again.
/// What that cannot end by itself is a message no consumer will take: every
/// new connection is offered it once more, so the queue would keep it, and
/// offer it to every redial, for as long as the process lives. After this many
/// refusals it is dropped and counted in [`QueueStats::dropped`].
///
/// A message that is dropped was confirmed to its producer, so the number is
/// a trade: small enough that a message nobody can read costs a handful of
/// offers and not an unbounded number, large enough that a queue whose
/// consumers differ in what they will read does not lose a message one of
/// them could have taken. What a message remembers is one connection id per
/// refusal, so the number is also its whole memory cost.
pub const MAX_REFUSALS: usize = 16;

/// One message a queue holds.
///
/// The payload, plus what a delivery needs: the topic a consumer's filter
/// selects on, the content type the producer labelled it with, and the trace
/// context, so that a delivery continues the producer's trace instead of
/// starting a new one.
#[derive(Debug)]
pub struct QueuedMessage {
    /// The payload, as read.
    ///
    /// Shared with the task that writes a delivery: the queue keeps its own
    /// handle until the consumer reports, so that a delivery that fails or is
    /// never reported on can go back without the payload having been copied
    /// or handed to a task that may not return it.
    pub body: Arc<Vec<u8>>,
    /// The topic the producer addressed inside the queue's path, if any.
    pub topic: Option<String>,
    /// The producer's content-type label, forwarded unmodified.
    pub content_type: Option<String>,
    /// The producer's trace context, so a delivery is the same trace.
    pub trace: Option<TraceContext>,
    /// How often this message has been delivered. DATA key 12
    /// (`delivery_attempt`, PROTOCOL.md §6.2) is reserved for carrying it;
    /// nothing writes that key yet.
    pub attempts: u64,
}

/// What a message costs before its payload is read: the fixed overhead and the
/// labels the queue will retain.
///
/// The cost a reader must reserve **before** it buffers a byte, and the same
/// figure [`QueuedMessage::charge`] adds to the allocation, so the two cannot
/// disagree about a message with an empty payload.
pub fn labels_cost(topic: Option<&str>, content_type: Option<&str>) -> usize {
    PER_MESSAGE_OVERHEAD + topic.map_or(0, str::len) + content_type.map_or(0, str::len)
}

impl QueuedMessage {
    /// What this message charges against the queue's budget: its
    /// **allocation**, not its payload.
    ///
    /// `body.capacity()` rather than `body.len()`, so that slack a buffer
    /// grew past the payload is memory charged to whoever caused it, and the
    /// number `docs/INVARIANTS.md` states as the bound on what a queue *holds*
    /// is resident memory rather than a count of useful bytes. The broker
    /// trims a payload to its length once it is read, so for a message that
    /// came through admission the two differ by an allocator's rounding.
    ///
    /// [`Queue::push`], [`Queue::settle`] and the rest all recompute this from
    /// the same `Vec`, so an add and its release are the same number and the
    /// running total cannot drift.
    pub fn charge(&self) -> usize {
        labels_cost(self.topic.as_deref(), self.content_type.as_deref()) + self.body.capacity()
    }
}

/// Why a message was not admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The queue is at `queue_bytes`.
    ///
    /// The only refusal admission has: `Reject` backpressure, "a cap refused
    /// before buffering", answered with ERROR `{REJECTED}` on the reply half
    /// (0018 §4.8).
    Full,
}

/// A delivery this queue has handed out and not seen settled.
///
/// The id is the queue's own and is never on the wire: settlement arrives as a
/// cursor on the delivery's own report, so the broker needs no correlation
/// field of its own — the id names the entry to drop or requeue when that
/// report resolves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveryId(pub u64);

/// What a queue holds right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueStats {
    /// Messages held and deliverable.
    pub messages: usize,
    /// Deliveries handed out and not yet settled. Still charged.
    pub unsettled: usize,
    /// Bytes charged against `queue_bytes`, including the per-message
    /// overhead of [`PER_MESSAGE_OVERHEAD`] — **held and unsettled
    /// together**, because an unsettled delivery may come back.
    pub bytes: usize,
    /// Bytes promised to payloads that are still arriving, counted against
    /// `queue_bytes` beside `bytes`. Zero when no producer is mid-message; a
    /// number that stays above zero is a producer that stopped sending.
    pub reserved: usize,
    /// Deliveries a consumer refused because of the message — a header its
    /// connection will not read — since the queue was built. Each one went
    /// back for other consumers, and the connection that refused is never
    /// offered that message again.
    pub refused: u64,
    /// Messages dropped because [`MAX_REFUSALS`] connections refused them,
    /// since the queue was built. A message that is counted here was
    /// confirmed to its producer and delivered to nobody.
    pub dropped: u64,
}

/// What a delivery task needs, copied out of the queue while the queue keeps
/// the message.
#[derive(Debug)]
pub struct Outgoing {
    /// The id the settlement will carry.
    pub id: DeliveryId,
    /// The payload, shared with the queue's own entry.
    pub body: Arc<Vec<u8>>,
    /// The message's topic label.
    pub topic: Option<String>,
    /// The message's content-type label.
    pub content_type: Option<String>,
    /// The message's trace context.
    pub trace: Option<TraceContext>,
    /// Resolves when the queue no longer holds this delivery: it was settled,
    /// went back, or was refused, and nothing is waiting for its outcome any
    /// more. A task still writing it or watching its report stops there.
    pub cancelled: oneshot::Receiver<()>,
}

/// Who holds a delivery, as far as the queue needs to tell.
///
/// The queue stores and compares holders and otherwise never looks at them,
/// with one exception: a message that a **connection** refused is not offered
/// to that connection again, and which connection a holder belongs to is the
/// holder's to say. The broker names a subscription there; a test names
/// anything it likes.
pub trait Holder: Clone {
    /// What names one consuming connection.
    type Connection: Copy + Eq + Debug;

    /// The connection this holder belongs to.
    fn connection(&self) -> Self::Connection;
}

/// Where a message stands in the order a scan offers it in.
///
/// Two numberings, because a message that went back is new to every
/// subscription wherever it sits: the position in the line says where it is,
/// this says whether a subscription has been offered it yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Enqueued {
    /// The `n`th message to go back to the head of the line.
    Returned(u64),
    /// The `n`th message to finish arriving.
    Arrived(u64),
}

/// What one subscription has been offered: every message that went back up to
/// the return numbered `returned`, and every message that arrived up to the
/// arrival numbered `arrived`.
///
/// Messages are offered in queue order and returns come before arrivals, so
/// this is a position in that order that stays valid however messages are
/// taken out from behind it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Seen {
    returned: u64,
    arrived: u64,
}

impl Seen {
    /// Has a subscription with this much seen been offered the message?
    pub fn covers(self, at: Enqueued) -> bool {
        match at {
            Enqueued::Returned(n) => n <= self.returned,
            Enqueued::Arrived(n) => n <= self.arrived,
        }
    }

    /// Records that everything up to and including `at`, in queue order, was
    /// offered. `frontier` is the queue's own: an arrival comes after every
    /// message that went back, so having been through one means having been
    /// through all of those.
    pub fn through(&mut self, at: Enqueued, frontier: Seen) {
        match at {
            Enqueued::Returned(n) => self.returned = self.returned.max(n),
            Enqueued::Arrived(n) => {
                self.returned = self.returned.max(frontier.returned);
                self.arrived = self.arrived.max(n);
            }
        }
    }

    /// The less informed of two: what both of them have seen.
    pub fn oldest(self, other: Seen) -> Seen {
        Seen {
            returned: self.returned.min(other.returned),
            arrived: self.arrived.min(other.arrived),
        }
    }
}

/// The message at one place in the line, as a scan looks at it.
#[derive(Debug)]
pub struct Peek<'a, C> {
    /// The message's topic, or the empty topic when it has none.
    ///
    /// The empty string is not a special case: a message with no topic is
    /// matched by the empty filter — which matches every topic — and by no
    /// other, which is exactly what an unlabelled message should be selected
    /// by.
    pub topic: &'a str,
    /// Where it stands in the order messages are offered in.
    pub at: Enqueued,
    /// The connections that refused it. A scan offers it to none of them.
    pub refused: &'a [C],
}

/// What became of a delivery its consumer refused.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused<O> {
    /// The message is back at the head of the line, and the holder's
    /// connection is not offered it again.
    Returned(O),
    /// [`MAX_REFUSALS`] connections have refused it: it was dropped and
    /// counted, and its charge is released.
    Dropped(O),
}

/// A message in the line, with what the queue remembers about it.
#[derive(Debug)]
struct Slot<C> {
    message: QueuedMessage,
    /// Which arrival it was. Kept for good, so that deliveries taken back
    /// together go back in the order their messages arrived.
    stamp: u64,
    /// Which return it was, while it is in the group that went back.
    returned_at: u64,
    /// The connections that refused it: at most [`MAX_REFUSALS`] of them.
    refused: Vec<C>,
}

/// One outstanding delivery: the message to give back, and who holds it.
#[derive(Debug)]
struct Unsettled<O: Holder> {
    slot: Slot<O::Connection>,
    owner: O,
    /// Never sent on: dropping it, with the entry, is the signal.
    _cancel: oneshot::Sender<()>,
}

/// The positions a scan visits, in order.
pub type Unseen = Chain<Range<usize>, Range<usize>>;

/// A queue: a FIFO of messages, the deliveries owed on them, and the budget
/// both are charged against.
///
/// Generic over who a delivery was handed to, which the queue stores and
/// compares and otherwise never looks at ([`Holder`]): the broker names a
/// subscription there, and the tests need no connection to name one.
#[derive(Debug)]
pub struct Queue<O: Holder> {
    /// Messages that were handed out and went back, in the order they went
    /// back, ahead of every other message.
    returned: VecDeque<Slot<O::Connection>>,
    /// Messages in the order they finished arriving.
    arrived: VecDeque<Slot<O::Connection>>,
    /// Handed out, not settled. Keyed by this queue's own delivery id, with
    /// the subscription that holds it recorded so a lost subscription can be
    /// swept and a settlement can say whose bound it frees.
    unsettled: HashMap<DeliveryId, Unsettled<O>>,
    next_delivery: u64,
    /// The last arrival number given out.
    arrivals: u64,
    /// The last return number given out.
    returns: u64,
    /// Sum of [`QueuedMessage::charge`] over the line **and** `unsettled`,
    /// maintained on every move rather than recomputed: admission asks for it
    /// per message, and an unsettled delivery is memory this queue still
    /// owes.
    charged: usize,
    /// Bytes promised to payloads still being read. Not yet a message, so not
    /// in `charged`; counted against the budget all the same.
    reserved: usize,
    budget: usize,
    refused: u64,
    dropped: u64,
}

impl<O: Holder> Queue<O> {
    /// An empty queue with `budget` bytes.
    pub fn new(budget: usize) -> Queue<O> {
        Queue {
            returned: VecDeque::new(),
            arrived: VecDeque::new(),
            unsettled: HashMap::new(),
            next_delivery: 1,
            arrivals: 0,
            returns: 0,
            charged: 0,
            reserved: 0,
            budget,
            refused: 0,
            dropped: 0,
        }
    }

    /// Bytes neither held nor promised.
    fn free(&self) -> usize {
        self.budget
            .saturating_sub(self.charged.saturating_add(self.reserved))
    }

    /// Promises `bytes` to a payload about to be read, or says there is no
    /// room for that much.
    ///
    /// All or nothing: the caller is deciding whether a message may start at
    /// all, which is the refusal "before a byte is read" that `Reject`
    /// backpressure means.
    pub fn reserve(&mut self, bytes: usize) -> bool {
        if self.free() < bytes {
            return false;
        }
        self.reserved += bytes;
        true
    }

    /// Promises as much of `bytes` as there is room for, and says how much
    /// that was.
    ///
    /// For a payload that grows as it arrives: asking for the next step and
    /// being told "nothing" is the cap, and being told "less" is the last
    /// step the budget allows. A reader never learns how large a message will
    /// be from the producer, whose `content_len` is advisory, so the queue's
    /// own remaining budget is what bounds the read.
    pub fn reserve_up_to(&mut self, bytes: usize) -> usize {
        let granted = bytes.min(self.free());
        self.reserved += granted;
        granted
    }

    /// Gives a promise back unused: the payload failed, stalled or was
    /// refused.
    pub fn release(&mut self, bytes: usize) {
        debug_assert!(bytes <= self.reserved, "released more than was reserved");
        self.reserved = self.reserved.saturating_sub(bytes);
    }

    /// Admits a message, converting the `reserved` bytes its reader held into
    /// the message's own charge, or refuses it because the queue is full.
    ///
    /// The authority on the bound, and the only one. A reader that reserved
    /// every byte it buffered cannot be refused here, because the charge is
    /// at most what it reserved; the check is what keeps a mistaken
    /// reservation from becoming an over-full queue. The reservation is
    /// consumed either way.
    pub fn push(&mut self, reserved: usize, message: QueuedMessage) -> Result<(), Refusal> {
        self.release(reserved);
        let charge = message.charge();
        if self.free() < charge {
            return Err(Refusal::Full);
        }
        self.charged += charge;
        self.arrivals += 1;
        self.arrived.push_back(Slot {
            message,
            stamp: self.arrivals,
            returned_at: 0,
            refused: Vec::new(),
        });
        Ok(())
    }

    /// How many messages are held.
    pub fn len(&self) -> usize {
        self.returned.len() + self.arrived.len()
    }

    /// The slot at `index`, in the order messages are offered in, and where it
    /// stands in that order.
    fn slot_at(&self, index: usize) -> Option<(&Slot<O::Connection>, Enqueued)> {
        let returned = self.returned.len();
        if index < returned {
            let slot = self.returned.get(index)?;
            Some((slot, Enqueued::Returned(slot.returned_at)))
        } else {
            let slot = self.arrived.get(index - returned)?;
            Some((slot, Enqueued::Arrived(slot.stamp)))
        }
    }

    /// The message at `index`, in the order messages are offered in.
    pub fn peek(&self, index: usize) -> Option<Peek<'_, O::Connection>> {
        let (slot, at) = self.slot_at(index)?;
        Some(Peek {
            topic: slot.message.topic.as_deref().unwrap_or(""),
            at,
            refused: &slot.refused,
        })
    }

    /// The payload of the message at `index`, for tests that have to tell
    /// messages apart.
    #[cfg(test)]
    pub fn body_at(&self, index: usize) -> Option<&[u8]> {
        Some(self.slot_at(index)?.0.message.body.as_slice())
    }

    /// The newest return and arrival so far: what a subscription that has
    /// seen all of it has been offered every message.
    pub fn frontier(&self) -> Seen {
        Seen {
            returned: self.returns,
            arrived: self.arrivals,
        }
    }

    /// The positions of every message that a subscription which has seen
    /// `seen` has not been offered, in the order to offer them.
    ///
    /// Both groups are numbered in the order they sit in, so what a
    /// subscription has not seen is the tail of each: found by a search, and
    /// never by walking what it has seen.
    pub fn unseen_from(&self, seen: Seen) -> Unseen {
        let returned = self.returned.len();
        let first_returned = self
            .returned
            .partition_point(|slot| slot.returned_at <= seen.returned);
        let first_arrived = self
            .arrived
            .partition_point(|slot| slot.stamp <= seen.arrived);
        (first_returned..returned).chain(returned + first_arrived..self.len())
    }

    /// Takes the message at `index` out of the line.
    fn take(&mut self, index: usize) -> Option<Slot<O::Connection>> {
        let returned = self.returned.len();
        if index < returned {
            self.returned.remove(index)
        } else {
            self.arrived.remove(index - returned)
        }
    }

    /// Puts a message at the end of the group that went back, which is the
    /// head of the line.
    fn give_back(&mut self, mut slot: Slot<O::Connection>) {
        self.returns += 1;
        slot.returned_at = self.returns;
        self.returned.push_back(slot);
    }

    /// Hands the message at `index` to `owner`: it leaves the line and enters
    /// the unsettled table, **keeping its charge**, and what a delivery needs
    /// is copied out.
    ///
    /// Delivery selects **the first message some consumer may take**, not
    /// strictly the head: with consumer-side filters a queue holds messages
    /// for several filters at once, so a head nobody subscribes for would
    /// otherwise block every message behind it forever.
    ///
    /// The message is in `unsettled` from this moment and not from the moment
    /// its write finishes, so no instant exists in which it is neither queued
    /// nor owed, and an admission cannot be told there is room for bytes a
    /// delivery is about to give back.
    pub fn dispatch(&mut self, index: usize, owner: O) -> Option<Outgoing> {
        let slot = self.take(index)?;
        let id = DeliveryId(self.next_delivery);
        self.next_delivery += 1;
        let (cancel, cancelled) = oneshot::channel();
        let outgoing = Outgoing {
            id,
            body: Arc::clone(&slot.message.body),
            topic: slot.message.topic.clone(),
            content_type: slot.message.content_type.clone(),
            trace: slot.message.trace,
            cancelled,
        };
        self.unsettled.insert(
            id,
            Unsettled {
                slot,
                owner,
                _cancel: cancel,
            },
        );
        Some(outgoing)
    }

    /// Counts a delivery whose bytes are out, and says whose it was.
    ///
    /// The attempt is counted here and not when the delivery starts, because
    /// a write that never reached anybody must not make the next delivery
    /// claim to be a repeat. The count is not on the wire yet; here it is
    /// state.
    pub fn written(&mut self, id: DeliveryId) -> Option<&O> {
        let entry = self.unsettled.get_mut(&id)?;
        entry.slot.message.attempts += 1;
        Some(&entry.owner)
    }

    /// Drops a settled delivery and releases its charge, and says whose it
    /// was.
    ///
    /// `None` when the id is unknown, which is not a failure: a settlement
    /// and a requeue race on a connection that dies as its consumer reports,
    /// and whichever arrives second finds nothing to do.
    pub fn settle(&mut self, id: DeliveryId) -> Option<O> {
        let entry = self.unsettled.remove(&id)?;
        self.charged -= entry.slot.message.charge();
        Some(entry.owner)
    }

    /// Puts an unsettled delivery back at the head of the queue, and says
    /// whose it was.
    ///
    /// The charge does not move, because it never left: this is the same
    /// bytes changing state, which is why it cannot be refused. `None` when
    /// the id is unknown, for [`Queue::settle`]'s reason.
    ///
    /// It goes behind the messages that went back before it and ahead of
    /// everything that arrived, and it is new to every subscription: none of
    /// them has been offered it yet, wherever it sits.
    pub fn requeue(&mut self, id: DeliveryId) -> Option<O> {
        let entry = self.unsettled.remove(&id)?;
        self.give_back(entry.slot);
        Some(entry.owner)
    }

    /// Requeues every delivery whose holder `is_lost` names, and says how
    /// many.
    ///
    /// What a lost subscription means: its outstanding deliveries are nobody's
    /// until they are somebody else's. They go back in the order their
    /// messages arrived, which is the nearest thing to the order the
    /// consumer was given them in.
    pub fn requeue_all(&mut self, mut is_lost: impl FnMut(&O) -> bool) -> usize {
        let mut lost: Vec<(u64, DeliveryId)> = self
            .unsettled
            .iter()
            .filter(|(_, entry)| is_lost(&entry.owner))
            .map(|(id, entry)| (entry.slot.stamp, *id))
            .collect();
        lost.sort_unstable();
        for (_, id) in &lost {
            self.requeue(*id);
        }
        lost.len()
    }

    /// Takes back a delivery its consumer refused because of the message, and
    /// says what became of it.
    ///
    /// The holder's connection is remembered on the message, so no scan offers
    /// it that message again; the message goes back to the head of the line
    /// for everybody else. When [`MAX_REFUSALS`] connections have refused it,
    /// nobody is going to take it: it is dropped, its charge is released and
    /// [`QueueStats::dropped`] counts it. `None` when the id is unknown, for
    /// [`Queue::settle`]'s reason.
    pub fn refuse(&mut self, id: DeliveryId) -> Option<Refused<O>> {
        let mut entry = self.unsettled.remove(&id)?;
        self.refused += 1;
        entry.slot.refused.push(entry.owner.connection());
        if entry.slot.refused.len() >= MAX_REFUSALS {
            self.charged -= entry.slot.message.charge();
            self.dropped += 1;
            return Some(Refused::Dropped(entry.owner));
        }
        self.give_back(entry.slot);
        Some(Refused::Returned(entry.owner))
    }

    /// What the queue holds.
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            messages: self.len(),
            unsettled: self.unsettled.len(),
            bytes: self.charged,
            reserved: self.reserved,
            refused: self.refused,
            dropped: self.dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;

    /// A holder is its own connection: two holders are two connections.
    impl Holder for u8 {
        type Connection = u8;

        fn connection(&self) -> u8 {
            *self
        }
    }

    fn message(body: usize) -> QueuedMessage {
        labelled(body, None)
    }

    fn labelled(body: usize, topic: Option<&str>) -> QueuedMessage {
        QueuedMessage {
            body: Arc::new(vec![0u8; body]),
            topic: topic.map(str::to_owned),
            content_type: None,
            attempts: 0,
            trace: None,
        }
    }

    /// Reserves what `message` will charge and admits it, as a reader does.
    fn admit(queue: &mut Queue<u8>, message: QueuedMessage) -> Result<(), Refusal> {
        let cost = message.charge();
        assert!(queue.reserve(cost), "no room even to start");
        queue.push(cost, message)
    }

    /// The topics in the line, in the order they are offered in.
    fn topics(queue: &Queue<u8>) -> Vec<String> {
        (0..queue.len())
            .map(|i| queue.peek(i).expect("in range").topic.to_owned())
            .collect()
    }

    fn visited(queue: &Queue<u8>, seen: Seen) -> Vec<usize> {
        queue.unseen_from(seen).collect()
    }

    #[test]
    fn an_empty_message_still_costs_the_queue_something() {
        // The whole point of the overhead: a producer of empty messages must
        // run out of budget too.
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 3);
        for _ in 0..3 {
            admit(&mut queue, message(0)).expect("within budget");
        }
        assert!(!queue.reserve(labels_cost(None, None)), "the fourth is out");
        assert_eq!(queue.push(0, message(0)), Err(Refusal::Full));
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 3,
                unsettled: 0,
                bytes: PER_MESSAGE_OVERHEAD * 3,
                reserved: 0,
                refused: 0,
                dropped: 0,
            }
        );
    }

    #[test]
    fn a_reservation_counts_against_the_budget_like_a_held_message() {
        // Two readers must not both be told the whole remainder: that is what
        // lets them be concurrent without the queue holding more than it
        // promised.
        let mut queue = Queue::<u8>::new(1000);
        assert_eq!(queue.reserve_up_to(600), 600);
        assert_eq!(
            queue.reserve_up_to(600),
            400,
            "the second reader gets what is left, not what the first was told"
        );
        assert_eq!(queue.reserve_up_to(1), 0, "and then nothing");
        assert!(!queue.reserve(1));

        queue.release(600);
        assert!(queue.reserve(600), "a released promise is room again");
        assert_eq!(queue.stats().reserved, 1000);
    }

    #[test]
    fn pushing_converts_the_reservation_into_a_charge_and_keeps_the_difference_free() {
        // A reader reserved a step of 100 and read 10: the other 90 go back.
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD + 200);
        let promised = PER_MESSAGE_OVERHEAD + 100;
        assert!(queue.reserve(promised));
        let mut body = Vec::with_capacity(100);
        body.extend_from_slice(&[7u8; 10]);
        body.shrink_to_fit();
        let held = QueuedMessage {
            body: Arc::new(body),
            ..message(0)
        };
        queue
            .push(promised, held)
            .expect("a charge below the promise");
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 1,
                unsettled: 0,
                bytes: PER_MESSAGE_OVERHEAD + 10,
                reserved: 0,
                refused: 0,
                dropped: 0,
            }
        );
        assert!(
            queue.reserve(190),
            "what the reader did not use is available again"
        );
    }

    #[test]
    fn a_payload_that_outgrew_its_promise_is_refused_not_admitted() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD + 10);
        assert!(queue.reserve(PER_MESSAGE_OVERHEAD));
        assert_eq!(
            queue.push(PER_MESSAGE_OVERHEAD, message(11)),
            Err(Refusal::Full),
            "the push is the authority, whatever the reader reserved"
        );
        assert_eq!(queue.stats().bytes, 0);
        assert_eq!(
            queue.stats().reserved,
            0,
            "a refused push still consumes it"
        );
    }

    #[test]
    fn a_message_that_fits_exactly_is_admitted_rather_than_refused() {
        // The boundary two authorities could disagree on: a budget of exactly
        // one empty message.
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 2);
        admit(&mut queue, message(0)).expect("within budget");
        admit(&mut queue, message(0)).expect("what the budget admits");
        assert!(!queue.reserve(1));
    }

    #[test]
    fn a_bodys_allocation_is_charged_and_not_its_payload() {
        // The slack is real memory the producer's message size chose.
        // Charging the length would let this queue hold both messages.
        fn slack(len: usize, capacity: usize) -> QueuedMessage {
            let mut body = Vec::with_capacity(capacity);
            body.resize(len, 0u8);
            QueuedMessage {
                body: Arc::new(body),
                ..message(0)
            }
        }

        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 2 + 100);
        admit(&mut queue, slack(10, 100)).expect("the first fits");
        assert!(
            !queue.reserve(slack(10, 100).charge()),
            "two ten-byte payloads fit this budget; two hundred-byte \
             allocations do not"
        );
        assert_eq!(queue.push(0, slack(10, 100)), Err(Refusal::Full));
    }

    #[test]
    fn a_label_is_charged_against_the_same_budget_as_the_payload() {
        // Otherwise a producer sending 512-byte topics and no payload would
        // hold unbounded label bytes under a payload-only budget.
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD + 10);
        let labelled = QueuedMessage {
            topic: Some("x".repeat(11)),
            ..message(0)
        };
        assert!(!queue.reserve(labelled.charge()));
        assert_eq!(queue.push(0, labelled), Err(Refusal::Full));
    }

    #[test]
    fn a_delivery_changes_whose_message_it_is_and_never_what_it_costs() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 4);
        admit(&mut queue, message(5)).expect("fits");
        let charged = queue.stats().bytes;

        let out = queue.dispatch(0, 9).expect("the message is there");
        assert_eq!(out.body.len(), 5);
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 0,
                unsettled: 1,
                bytes: charged,
                reserved: 0,
                refused: 0,
                dropped: 0,
            }
        );

        assert_eq!(queue.written(out.id), Some(&9));
        assert_eq!(queue.requeue(out.id), Some(9), "it can come back");
        assert_eq!(queue.stats().messages, 1);
        assert_eq!(queue.stats().bytes, charged);

        let again = queue.dispatch(0, 3).expect("deliverable again");
        assert_eq!(queue.settle(again.id), Some(3));
        assert_eq!(
            queue.stats().bytes,
            0,
            "settling releases exactly the charge"
        );
        assert_eq!(
            queue.settle(again.id),
            None,
            "a second outcome finds nothing"
        );
        assert_eq!(queue.requeue(again.id), None);
    }

    #[test]
    fn requeueing_a_lost_owners_deliveries_leaves_the_others_alone() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 8);
        for _ in 0..3 {
            admit(&mut queue, message(0)).expect("fits");
        }
        let first = queue.dispatch(0, 1).expect("one");
        let _second = queue.dispatch(0, 2).expect("two");
        let _third = queue.dispatch(0, 1).expect("three");

        assert_eq!(queue.requeue_all(|owner| *owner == 1), 2);
        assert_eq!(queue.stats().messages, 2);
        assert_eq!(queue.stats().unsettled, 1);
        assert_eq!(queue.settle(first.id), None, "it went back with its owner");
    }

    #[test]
    fn a_subscription_is_offered_only_what_it_has_not_seen() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 16);
        for topic in ["a", "b", "c"] {
            admit(&mut queue, labelled(0, Some(topic))).expect("fits");
        }
        assert_eq!(
            visited(&queue, Seen::default()),
            [0, 1, 2],
            "a subscription that has seen nothing is offered everything"
        );

        let mut seen = Seen::default();
        seen.through(Enqueued::Arrived(3), queue.frontier());
        assert_eq!(seen, queue.frontier());
        assert!(visited(&queue, seen).is_empty(), "nothing is new to it");

        // A new arrival is the only message worth a look, however long the
        // line in front of it is.
        admit(&mut queue, labelled(0, Some("d"))).expect("fits");
        assert_eq!(visited(&queue, seen), [3]);
        assert_eq!(queue.peek(3).expect("in range").topic, "d");

        // What is taken out from in front of the unseen part moves its
        // position and not its number.
        queue.dispatch(1, 1).expect("b");
        assert_eq!(visited(&queue, seen), [2]);
        assert_eq!(queue.peek(2).expect("in range").topic, "d");
    }

    #[test]
    fn a_message_that_goes_back_is_new_to_every_subscription_wherever_it_sits() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 16);
        for topic in ["a", "b", "c", "d", "e"] {
            admit(&mut queue, labelled(0, Some(topic))).expect("fits");
        }
        let seen = queue.frontier();
        let taken = queue.dispatch(2, 1).expect("c").id;

        queue.requeue(taken).expect("it is owed");
        assert_eq!(
            topics(&queue),
            ["c", "a", "b", "d", "e"],
            "back at the head"
        );
        assert_eq!(
            visited(&queue, seen),
            [0],
            "a subscription that saw everything is offered the one message \
             that came back, not the line behind it"
        );
        assert_ne!(queue.frontier(), seen);
    }

    #[test]
    fn what_went_back_stays_in_the_order_it_went_back_in_and_ahead_of_arrivals() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 16);
        for topic in ["a", "b", "c", "d"] {
            admit(&mut queue, labelled(0, Some(topic))).expect("fits");
        }
        let a = queue.dispatch(0, 1).expect("a").id;
        let b = queue.dispatch(0, 1).expect("b").id;
        queue.requeue(b).expect("owed");
        // A subscription that has been offered everything there is so far.
        let seen = queue.frontier();
        queue.requeue(a).expect("owed");
        admit(&mut queue, labelled(0, Some("e"))).expect("fits");

        assert_eq!(topics(&queue), ["b", "a", "c", "d", "e"]);
        assert_eq!(
            visited(&queue, seen),
            [1, 4],
            "the return after the one it saw, and the arrival after the ones it saw"
        );
    }

    #[test]
    fn deliveries_taken_back_together_go_back_in_the_order_they_arrived() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 16);
        for topic in ["a", "b", "c", "d"] {
            admit(&mut queue, labelled(0, Some(topic))).expect("fits");
        }
        for _ in 0..4 {
            queue.dispatch(0, 1).expect("taken");
        }
        assert_eq!(queue.requeue_all(|owner| *owner == 1), 4);
        assert_eq!(topics(&queue), ["a", "b", "c", "d"]);
    }

    #[test]
    fn a_refused_message_goes_back_and_is_not_offered_to_its_refuser_again() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 4);
        admit(&mut queue, message(0)).expect("fits");
        let charged = queue.stats().bytes;

        let out = queue.dispatch(0, 7).expect("taken by connection 7");
        assert_eq!(queue.refuse(out.id), Some(Refused::Returned(7)));
        assert_eq!(queue.refuse(out.id), None, "a second outcome finds nothing");
        assert_eq!(queue.stats().messages, 1, "it is the queue's again");
        assert_eq!(queue.stats().bytes, charged, "and no cheaper for it");
        assert_eq!(queue.stats().refused, 1);
        assert_eq!(queue.peek(0).expect("back").refused, [7]);

        let again = queue.dispatch(0, 8).expect("another connection takes it");
        assert_eq!(queue.refuse(again.id), Some(Refused::Returned(8)));
        assert_eq!(queue.peek(0).expect("back").refused, [7, 8]);
    }

    #[test]
    fn a_message_that_max_refusals_connections_refused_is_dropped_and_counted() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 4);
        admit(&mut queue, message(0)).expect("fits");

        for connection in 0..MAX_REFUSALS as u8 - 1 {
            let out = queue.dispatch(0, connection).expect("offered");
            assert_eq!(
                queue.refuse(out.id),
                Some(Refused::Returned(connection)),
                "refusal {connection} leaves it in the queue"
            );
        }
        assert_eq!(queue.stats().messages, 1);
        assert_eq!(queue.stats().dropped, 0);

        let last = queue.dispatch(0, 200).expect("offered once more");
        assert_eq!(queue.refuse(last.id), Some(Refused::Dropped(200)));
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 0,
                unsettled: 0,
                bytes: 0,
                reserved: 0,
                refused: MAX_REFUSALS as u64,
                dropped: 1,
            },
            "gone, counted, and its charge released"
        );
    }

    #[test]
    fn taking_a_delivery_out_of_the_table_cancels_whatever_waits_on_it() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 16);
        for _ in 0..5 {
            admit(&mut queue, message(0)).expect("fits");
        }
        let mut settled = queue.dispatch(0, 1).expect("one");
        let mut requeued = queue.dispatch(0, 1).expect("two");
        let mut swept = queue.dispatch(0, 2).expect("three");
        let mut refused = queue.dispatch(0, 3).expect("four");
        let mut held = queue.dispatch(0, 4).expect("five");
        for out in [
            &mut settled,
            &mut requeued,
            &mut swept,
            &mut refused,
            &mut held,
        ] {
            assert_eq!(
                out.cancelled.try_recv(),
                Err(TryRecvError::Empty),
                "an outstanding delivery is not cancelled"
            );
        }

        queue.settle(settled.id).expect("owed");
        queue.requeue(requeued.id).expect("owed");
        assert_eq!(queue.requeue_all(|owner| *owner == 2), 1);
        queue.refuse(refused.id).expect("owed");
        for out in [&mut settled, &mut requeued, &mut swept, &mut refused] {
            assert_eq!(
                out.cancelled.try_recv(),
                Err(TryRecvError::Closed),
                "the entry went, so did what waited on it"
            );
        }
        assert_eq!(
            held.cancelled.try_recv(),
            Err(TryRecvError::Empty),
            "a delivery nobody touched is still wanted"
        );
    }

    #[test]
    fn an_unlabelled_message_has_the_empty_topic() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 2);
        admit(&mut queue, message(0)).expect("fits");
        assert_eq!(queue.peek(0).expect("held").topic, "");
        assert!(queue.peek(1).is_none());
    }
}
