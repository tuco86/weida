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
//! **Bytes are promised before they are read.** Producers are read
//! concurrently, so "how much room is left" cannot be asked once and acted on
//! later: two readers would each be told the whole remainder. A reader
//! [`Queue::reserve`]s what it is about to buffer, the reservation counts
//! against the budget exactly like a held message does, and
//! [`Queue::push`] turns it into a charge or [`Queue::release`] gives it
//! back. `charged + reserved` never exceeds the budget, whoever is reading.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

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
}

/// A queue: a FIFO of messages, the deliveries owed on them, and the budget
/// both are charged against.
///
/// Generic over who a delivery was handed to, which the queue stores and
/// compares and otherwise never looks at: the broker names a subscription
/// there, and the tests need no connection to name one.
#[derive(Debug)]
pub struct Queue<O> {
    messages: VecDeque<QueuedMessage>,
    /// Handed out, not settled. Keyed by this queue's own delivery id, with
    /// the subscription that holds it recorded so a lost subscription can be
    /// swept and a settlement can say whose bound it frees.
    unsettled: HashMap<DeliveryId, Unsettled<O>>,
    next_delivery: u64,
    /// Sum of [`QueuedMessage::charge`] over `messages` **and** `unsettled`,
    /// maintained on every move rather than recomputed: admission asks for it
    /// per message, and an unsettled delivery is memory this queue still
    /// owes.
    charged: usize,
    /// Bytes promised to payloads still being read. Not yet a message, so not
    /// in `charged`; counted against the budget all the same.
    reserved: usize,
    budget: usize,
    /// How many messages at the **back** of `messages` no delivery scan has
    /// offered to the current set of ready subscriptions yet.
    ///
    /// Everything in front of them was offered and nobody ready wanted it,
    /// and that stays true until the set of ready subscriptions gains a member
    /// or a message goes back to the front ([`Queue::rescan`]). Without this a
    /// queue holding messages nobody ready selects would be walked end to end
    /// on every event, and the length of that walk is the producers' choice.
    unscanned: usize,
}

/// One outstanding delivery: the message to give back, and who holds it.
#[derive(Debug)]
struct Unsettled<O> {
    message: QueuedMessage,
    owner: O,
}

impl<O> Queue<O> {
    /// An empty queue with `budget` bytes.
    pub fn new(budget: usize) -> Queue<O> {
        Queue {
            messages: VecDeque::new(),
            unsettled: HashMap::new(),
            next_delivery: 1,
            charged: 0,
            reserved: 0,
            budget,
            unscanned: 0,
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
        self.messages.push_back(message);
        self.unscanned += 1;
        Ok(())
    }

    /// How many messages are held.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// The topic of the message at `index`, or the empty topic when it has
    /// none.
    ///
    /// The empty string is not a special case: a message with no topic is
    /// matched by the empty filter — which matches every topic — and by no
    /// other, which is exactly what an unlabelled message should be selected
    /// by.
    pub fn topic_at(&self, index: usize) -> Option<&str> {
        let message = self.messages.get(index)?;
        Some(message.topic.as_deref().unwrap_or(""))
    }

    /// Where the next delivery scan starts: the first message no ready
    /// subscription has been offered yet.
    pub fn scan_from(&self) -> usize {
        self.messages.len() - self.unscanned.min(self.messages.len())
    }

    /// Records that a scan offered every message to the ready subscriptions
    /// and none took it.
    pub fn scanned(&mut self) {
        self.unscanned = 0;
    }

    /// Forgets what earlier scans learned: the next one starts at the head.
    ///
    /// For the two events that can make an old message deliverable: a
    /// subscription that became ready, which may select what nobody did, and
    /// a message going back to the front of the line.
    pub fn rescan(&mut self) {
        self.unscanned = self.messages.len();
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
        let message = self.messages.remove(index)?;
        // The scan that found this message offered everything before it and
        // nobody took any of it; what is behind it has not been looked at.
        self.unscanned = self.messages.len() - index;
        let id = DeliveryId(self.next_delivery);
        self.next_delivery += 1;
        let outgoing = Outgoing {
            id,
            body: Arc::clone(&message.body),
            topic: message.topic.clone(),
            content_type: message.content_type.clone(),
            trace: message.trace,
        };
        self.unsettled.insert(id, Unsettled { message, owner });
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
        entry.message.attempts += 1;
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
        self.charged -= entry.message.charge();
        Some(entry.owner)
    }

    /// Puts an unsettled delivery back at the head of the queue, and says
    /// whose it was.
    ///
    /// The charge does not move, because it never left: this is the same
    /// bytes changing state, which is why it cannot be refused. `None` when
    /// the id is unknown, for [`Queue::settle`]'s reason.
    pub fn requeue(&mut self, id: DeliveryId) -> Option<O> {
        let entry = self.unsettled.remove(&id)?;
        self.messages.push_front(entry.message);
        self.rescan();
        Some(entry.owner)
    }

    /// Requeues every delivery whose holder `is_lost` names, and says how
    /// many.
    ///
    /// What a lost subscription means: its outstanding deliveries are nobody's
    /// until they are somebody else's. Order is the queue's own again — the
    /// entries go back to the head in no particular order among themselves,
    /// which is the honest statement, because a queue that promised the
    /// original order across a consumer's death would be promising something
    /// it never had (the deliveries were concurrent).
    pub fn requeue_all(&mut self, mut is_lost: impl FnMut(&O) -> bool) -> usize {
        let lost: Vec<DeliveryId> = self
            .unsettled
            .iter()
            .filter(|(_, entry)| is_lost(&entry.owner))
            .map(|(id, _)| *id)
            .collect();
        for id in &lost {
            self.requeue(*id);
        }
        lost.len()
    }

    /// What the queue holds.
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            messages: self.messages.len(),
            unsettled: self.unsettled.len(),
            bytes: self.charged,
            reserved: self.reserved,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_scan_does_not_revisit_what_it_already_offered() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 16);
        for topic in ["a", "b", "c"] {
            admit(&mut queue, labelled(0, Some(topic))).expect("fits");
        }
        assert_eq!(queue.scan_from(), 0, "nothing was offered yet");

        // A scan that finds nothing: the three are known to be unwanted.
        queue.scanned();
        assert_eq!(queue.scan_from(), 3);

        // A new message is the only one worth a look.
        admit(&mut queue, labelled(0, Some("d"))).expect("fits");
        assert_eq!(queue.scan_from(), 3);
        assert_eq!(queue.topic_at(3), Some("d"));

        // A subscription that became ready may want any of them.
        queue.rescan();
        assert_eq!(queue.scan_from(), 0);

        // Taking the message the scan stopped at leaves what it passed behind.
        queue.dispatch(2, 1).expect("the third");
        assert_eq!(
            queue.scan_from(),
            2,
            "a and b were offered before c was taken; d was not"
        );

        // A message going back to the front invalidates the rest.
        let id = queue.dispatch(2, 1).expect("d").id;
        queue.scanned();
        queue.requeue(id).expect("it is owed");
        assert_eq!(queue.scan_from(), 0);
    }

    #[test]
    fn an_unlabelled_message_has_the_empty_topic() {
        let mut queue = Queue::<u8>::new(PER_MESSAGE_OVERHEAD * 2);
        admit(&mut queue, message(0)).expect("fits");
        assert_eq!(queue.topic_at(0), Some(""));
        assert_eq!(queue.topic_at(1), None);
    }
}
