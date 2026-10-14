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

use std::collections::{HashMap, VecDeque};

use weida::{IncomingMeta, TraceContext};

/// What one queued message costs beyond its payload and its retained labels.
///
/// A budget that counted payload bytes only would bound nothing for a producer
/// sending empty messages: a million zero-byte messages are zero bytes of
/// payload and a million live `QueuedMessage`s. Charging a fixed overhead per
/// message makes `queue_bytes` bound the message *count* as well —
/// `queue_bytes / PER_MESSAGE_OVERHEAD` is the ceiling — which is what
/// `docs/INVARIANTS.md` asks of any allocation a remote peer can drive.
///
/// The number is a deliberate over-estimate of `QueuedMessage`'s own
/// footprint (three `Option`s, a `VecDeque` slot and an allocation header), not
/// a measurement: it is a bound, and a bound that tracks `size_of` would move
/// with every field added here.
pub const PER_MESSAGE_OVERHEAD: usize = 256;

/// One message a queue holds.
///
/// The payload, plus what a delivery needs: the topic a consumer's filter
/// selects on, the content type the producer labelled it with, and the trace
/// context, so that a delivery continues the producer's trace instead of
/// starting a new one.
#[derive(Debug)]
pub struct QueuedMessage {
    /// The payload, as read.
    pub body: Vec<u8>,
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

impl QueuedMessage {
    /// What this message charges against the queue's budget: its
    /// **allocation**, not its payload.
    ///
    /// `body.capacity()` rather than `body.len()`, because a payload arrives
    /// through `IncomingTransfer::collect`, which grows a `Vec` from empty in
    /// 64 KiB reads — so a 2 MiB + 64 KiB message comes to rest in a 4 MiB
    /// allocation. Charging the length would let a queue hold close to twice
    /// `queue_bytes` of resident payload, with the factor chosen by the
    /// producer's message size, and `docs/INVARIANTS.md` states that number as
    /// the bound on what a queue *holds*. The slack is real memory; it is
    /// charged to whoever caused it.
    ///
    /// [`Queue::push`], [`Queue::take`] and [`Queue::push_front`] all
    /// recompute this from the same `Vec`, so an add and its release are the
    /// same number and the running total cannot drift.
    pub fn charge(&self) -> usize {
        PER_MESSAGE_OVERHEAD
            + self.body.capacity()
            + self.topic.as_ref().map_or(0, |t| t.len())
            + self.content_type.as_ref().map_or(0, |t| t.len())
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
}

/// A queue: a FIFO of messages, the deliveries owed on them, and the budget
/// both are charged against.
#[derive(Debug)]
pub struct Queue {
    messages: VecDeque<QueuedMessage>,
    /// Handed out, not settled. Keyed by this queue's own delivery id, with
    /// the subscription that holds it recorded so a bound can be per
    /// subscription and a lost subscription can be swept.
    unsettled: HashMap<DeliveryId, Unsettled>,
    next_delivery: u64,
    /// Sum of [`QueuedMessage::charge`] over `messages` **and** `unsettled`,
    /// maintained on every move rather than recomputed: admission asks for it
    /// per message, and an unsettled delivery is memory this queue still
    /// owes.
    charged: usize,
    budget: usize,
}

/// One outstanding delivery: the message to give back, and who holds it.
#[derive(Debug)]
struct Unsettled {
    message: QueuedMessage,
    consumer: weida::ConsumerId,
    filter: String,
}

impl Queue {
    /// An empty queue with `budget` bytes.
    pub fn new(budget: usize) -> Queue {
        Queue {
            messages: VecDeque::new(),
            unsettled: HashMap::new(),
            next_delivery: 1,
            charged: 0,
            budget,
        }
    }

    /// How many payload bytes would still fit, or `None` when no message fits
    /// at all.
    ///
    /// `Some(n)` is the cap the payload is *read* under, so a producer's
    /// advisory `content_len` never decides how much is buffered — the queue's
    /// own remaining budget does. `Some(0)` is a real answer: a message whose
    /// labels and overhead fill the budget exactly is admitted with an empty
    /// payload, and [`Queue::push`] accepts it. `None` is the refusal, and it
    /// is [`Queue::push`]'s own arithmetic read backwards — `charged + labels`
    /// already past the budget — so the two cannot disagree at the boundary.
    ///
    /// The one thing this cannot promise is that a payload of exactly `n`
    /// bytes is admitted: [`QueuedMessage::charge`] counts the allocation, and
    /// a `Vec` that ends at `n` bytes may have reserved more. That is the
    /// whole remaining gap between this answer and `push`'s, and it is stated
    /// where `push` refuses.
    pub fn room_for(&self, meta: &IncomingMeta) -> Option<usize> {
        let labels = PER_MESSAGE_OVERHEAD
            + meta.topic.as_ref().map_or(0, |t| t.len())
            + meta.content_type.as_ref().map_or(0, |t| t.len());
        self.budget.checked_sub(self.charged + labels)
    }

    /// Admits a message, or refuses it because the queue is full.
    ///
    /// The authority on the bound, and the only one: [`Queue::room_for`] is
    /// this same comparison asked before the payload is read, so a message
    /// that passed that check is refused here only when its `Vec` reserved
    /// more than it was read under ([`QueuedMessage::charge`]). Nothing else
    /// can intervene — a queue's events are one loop, so no second admission
    /// runs between the question and the answer.
    pub fn push(&mut self, message: QueuedMessage) -> Result<(), Refusal> {
        let charge = message.charge();
        if self.charged + charge > self.budget {
            return Err(Refusal::Full);
        }
        self.charged += charge;
        self.messages.push_back(message);
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

    /// Takes the message at `index`, keeping the order of the rest.
    ///
    /// Delivery selects **the first message some consumer may take**, not
    /// strictly the head: with consumer-side filters a queue holds messages
    /// for several filters at once, so a head nobody subscribes for would
    /// otherwise block every message behind it forever. Scanning costs one
    /// pass over a queue whose length is bounded by
    /// `queue_bytes / PER_MESSAGE_OVERHEAD`, and the common case — one
    /// consumer with the empty filter — stops at the first message.
    pub fn take(&mut self, index: usize) -> Option<QueuedMessage> {
        let message = self.messages.remove(index)?;
        self.charged -= message.charge();
        Some(message)
    }

    /// Puts a message back where it was.
    ///
    /// For a delivery whose write never landed: it was charged against this
    /// budget a moment ago, so the charge is re-added rather than re-checked —
    /// refusing here would discard a message the broker has already confirmed,
    /// which is the one thing a queue may never do
    /// (`docs/GUARANTEES.md` §1).
    pub fn push_front(&mut self, message: QueuedMessage) {
        self.charged += message.charge();
        self.messages.push_front(message);
    }

    /// Takes the message at `index` back into this queue as an **unsettled
    /// delivery**, returning the id that will settle it.
    ///
    /// The message arrives here owned, because the broker wrote it with no
    /// lock held — [`Queue::take`] handed it out and this hands it back — so
    /// nothing is copied for a delivery. Its bytes are charged again, and
    /// they are charged **until a consumer reports**: a delivery that may
    /// come back is memory this queue still owes, which is the one accounting
    /// rule here. [`Queue::stats`] reports held and unsettled
    /// separately so an operator can see which it is.
    ///
    /// It cannot be refused, for [`Queue::push_front`]'s reason: this is the
    /// same bytes changing state, and the loop that took them admits nothing
    /// in between.
    pub fn hold(
        &mut self,
        message: QueuedMessage,
        consumer: weida::ConsumerId,
        filter: &str,
    ) -> DeliveryId {
        let id = DeliveryId(self.next_delivery);
        self.next_delivery += 1;
        self.charged += message.charge();
        self.unsettled.insert(
            id,
            Unsettled {
                message,
                consumer,
                filter: filter.to_owned(),
            },
        );
        id
    }

    /// Drops a settled delivery and releases its charge.
    ///
    /// `false` when the id is unknown, which is not a failure: a settlement
    /// and a requeue race on a connection that dies as its consumer reports,
    /// and whichever arrives second finds nothing to do.
    pub fn settle(&mut self, id: DeliveryId) -> bool {
        match self.unsettled.remove(&id) {
            Some(entry) => {
                self.charged -= entry.message.charge();
                true
            }
            None => false,
        }
    }

    /// Puts an unsettled delivery back at the head of the queue.
    ///
    /// The charge does not move, because it never left: this is the same
    /// bytes changing state, which is why it cannot be refused. `false` when
    /// the id is unknown, for [`Queue::settle`]'s reason.
    pub fn requeue(&mut self, id: DeliveryId) -> bool {
        match self.unsettled.remove(&id) {
            Some(entry) => {
                self.messages.push_front(entry.message);
                true
            }
            None => false,
        }
    }

    /// Requeues every delivery one subscription holds, and says how many.
    ///
    /// What a lost subscription means: its outstanding deliveries are nobody's
    /// until they are somebody else's. Order is the queue's own again — the
    /// entries go back to the head in no particular order among themselves,
    /// which is the honest statement, because a queue that promised the
    /// original order across a consumer's death would be promising something
    /// it never had (the deliveries were concurrent).
    pub fn requeue_all(&mut self, consumer: weida::ConsumerId, filter: Option<&str>) -> usize {
        let lost: Vec<DeliveryId> = self
            .unsettled
            .iter()
            .filter(|(_, entry)| {
                entry.consumer == consumer && filter.is_none_or(|f| entry.filter == f)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in &lost {
            self.requeue(*id);
        }
        lost.len()
    }

    /// How many deliveries one subscription holds unsettled.
    ///
    /// The number `max_unsettled` bounds, and it is per subscription because
    /// one slow consumer costs its own slots and nobody else's.
    pub fn unsettled_for(&self, consumer: weida::ConsumerId, filter: &str) -> usize {
        self.unsettled
            .values()
            .filter(|entry| entry.consumer == consumer && entry.filter == filter)
            .count()
    }

    /// What the queue holds.
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            messages: self.messages.len(),
            unsettled: self.unsettled.len(),
            bytes: self.charged,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(body: usize) -> QueuedMessage {
        QueuedMessage {
            body: vec![0u8; body],
            topic: None,
            content_type: None,
            attempts: 0,
            trace: None,
        }
    }

    fn empty_meta() -> IncomingMeta {
        IncomingMeta {
            endpoint: Some("/q".to_owned()),
            content_len: None,
            content_type: None,
            trace: None,
            tracestate: None,
            topic: None,
            peer: None,
            peer_chain: None,
            sequence: None,
            gap: None,
            achieved: None,
            report: Vec::new(),
            report_mode: weida::ReportMode::default(),
            report_id: None,
            segment: None,
            layer: None,
        }
    }

    #[test]
    fn an_empty_message_still_costs_the_queue_something() {
        // The whole point of the overhead: a producer of empty messages must
        // run out of budget too.
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 3);
        for _ in 0..3 {
            queue.push(message(0)).expect("within budget");
        }
        assert_eq!(queue.push(message(0)), Err(Refusal::Full));
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 3,
                unsettled: 0,
                bytes: PER_MESSAGE_OVERHEAD * 3
            }
        );
    }

    #[test]
    fn the_read_cap_shrinks_as_the_queue_fills() {
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 2 + 100);
        assert_eq!(
            queue.room_for(&empty_meta()),
            Some(PER_MESSAGE_OVERHEAD + 100)
        );
        queue.push(message(60)).expect("within budget");
        assert_eq!(queue.room_for(&empty_meta()), Some(40));
        queue.push(message(40)).expect("exactly the remaining room");
        assert_eq!(
            queue.room_for(&empty_meta()),
            None,
            "not even a message's own overhead fits now, and that is the refusal"
        );
    }

    #[test]
    fn a_message_that_fits_exactly_is_admitted_rather_than_refused() {
        // The boundary the two authorities could disagree on: a payload cap
        // of zero says a message fits, not that the queue is full, and
        // `push` agrees.
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 2);
        let meta = empty_meta();
        queue.push(message(0)).expect("within budget");
        let room = queue.room_for(&meta).expect("one empty message fits");
        assert_eq!(room, 0);
        queue.push(message(room)).expect("what the cap admits");
        assert_eq!(queue.room_for(&meta), None);
    }

    #[test]
    fn a_bodys_allocation_is_charged_and_not_its_payload() {
        // A payload read in 64 KiB chunks comes to rest in a geometrically
        // grown `Vec`, so the slack is real memory the producer's message size
        // chose. Charging the length would let this queue hold both messages.
        fn slack(len: usize, capacity: usize) -> QueuedMessage {
            let mut body = Vec::with_capacity(capacity);
            body.resize(len, 0u8);
            QueuedMessage {
                body,
                topic: None,
                content_type: None,
                attempts: 0,
                trace: None,
            }
        }

        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD * 2 + 100);
        queue.push(slack(10, 100)).expect("the first fits");
        assert_eq!(
            queue.push(slack(10, 100)),
            Err(Refusal::Full),
            "two ten-byte payloads fit this budget; two hundred-byte \
             allocations do not"
        );
        // And the release is the same number the add was.
        let taken = queue.take(0).expect("the first is still held");
        assert_eq!(taken.body.len(), 10);
        assert_eq!(
            queue.stats(),
            QueueStats {
                messages: 0,
                unsettled: 0,
                bytes: 0
            }
        );
    }

    #[test]
    fn a_label_is_charged_against_the_same_budget_as_the_payload() {
        // Otherwise a producer sending 512-byte topics and no payload would
        // hold unbounded label bytes under a payload-only budget.
        let mut queue = Queue::new(PER_MESSAGE_OVERHEAD + 10);
        let labelled = QueuedMessage {
            body: Vec::new(),
            topic: Some("x".repeat(11)),
            content_type: None,
            attempts: 0,
            trace: None,
        };
        assert_eq!(queue.push(labelled), Err(Refusal::Full));
    }
}
