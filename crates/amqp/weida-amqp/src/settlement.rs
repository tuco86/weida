//! The unsettled map, and what an outcome proves.
//!
//! A delivery is *unsettled* while either end still holds state for it, and
//! settlement is the act of forgetting: "the state of a delivery at one end
//! becomes irrelevant once that end has settled" (Part 2 §2.6.12). Two
//! properties make the model work and both are asserted here:
//!
//! * **Settlement is one-way.** Once an end has settled it can never unsettle,
//!   and an outcome once published is terminal: "no later frame from the
//!   sender can change it" (Part 2 §2.7.5).
//! * **Settlement is idempotent.** A `disposition` naming a delivery this end
//!   has already forgotten is *not* an error and not an update — it is
//!   nothing. Which is what makes the frame safe to repeat, and is why a lost
//!   disposition needs no recovery protocol.
//!
//! # What each mode proves to an application
//!
//! The settle modes are not about frames, they are about what an application
//! may conclude:
//!
//! | Modes | What the sender knows when `send` returns | Will the peer redeliver? |
//! | --- | --- | --- |
//! | `snd-settle-mode=settled` | that the octets were written, and nothing else | unknowable from here: the sender forgot |
//! | `snd-settle-mode=unsettled`, `rcv-settle-mode=first` | nothing yet; the outcome arrives in a `disposition` and is final when it does | no, once `accepted` arrived |
//! | `snd-settle-mode=unsettled`, `rcv-settle-mode=second` | nothing yet; the receiver's outcome is *provisional* until this end settles and the receiver sees it | no, and the receiver also knows this end knows |
//!
//! The third row is the only one that gives both ends the same knowledge, and
//! it costs a round trip in each direction. It is also the row brokers refuse:
//! Artemis answers "The Broker does not currently support ReceiverSettleMode
//! of SECOND" and RabbitMQ lists exactly-once as unsupported, so a client that
//! asked for it and did not read the answering `attach` would believe it had a
//! guarantee it does not have.
//!
//! # Who bounds the map
//!
//! A **receiver's** map is bounded by the protocol, and the bound is enforced:
//! it can only grow by a delivery arriving, and a delivery that arrives past
//! every limit the receiver ever announced is refused with
//! `amqp:link:transfer-limit-exceeded`, as is an unsettled one that finds the
//! map full (`amqp:resource-limit-exceeded`). A delivery is never handed to
//! the application without being recorded here, which is what lets every
//! acknowledgement be written. An application that never settles runs out of
//! credit and the sender stalls, which is the designed behaviour.
//!
//! A **sender's** map has no such bound — the receiver may keep granting
//! credit while settling nothing — so the bound is ours:
//! [`DEFAULT_MAX_UNSETTLED`]. At the bound `send` waits, exactly as it waits
//! for credit, rather than growing a map nobody bounded.
//!
//! # A sender keeps the answer until it has been read
//!
//! When the receiver settles a delivery its outcome is the answer to the
//! `send`, and it exists nowhere else: the frame that carried it is gone and
//! this end has nothing to remember it by but the record. So a sender's map
//! does not forget a delivery the peer has settled; it marks it finished and
//! keeps it, with the outcome, until the application has taken that outcome
//! ([`Unsettled::take_final`]). A finished delivery counts against the bound
//! like any other, which makes the bound what stops a producer from running
//! ahead of the answers nobody has read: an application that wants to send
//! without ever learning an outcome attaches with `snd-settle-mode=settled`,
//! where there is none to keep. A receiver's map forgets as before: what the
//! sender settles there is the end of a dance whose outcome this end chose
//! itself.

use std::collections::BTreeMap;

use weida_amqp_codec::EncodeError;
use weida_amqp_codec::state::DeliveryState;

use crate::error::Condition;
use crate::owned::OwnedValue;
use crate::window::serial_distance;

/// How many unsettled deliveries one link keeps before `send` waits.
///
/// **A bound of ours**, and one the protocol does not supply: an `attach`'s
/// `unsettled` map may be larger than a frame (that is what
/// `incomplete-unsettled` is for), so nothing on the wire caps it. 2048 is far
/// above any broker's in-flight window — RabbitMQ grants a publisher 170 link
/// credits, Artemis 1000 — and finite.
pub const DEFAULT_MAX_UNSETTLED: usize = 2048;

/// A terminal delivery state: what the peer has committed to.
///
/// The owned counterpart of the four outcomes of
/// [`weida_amqp_codec::state::DeliveryState`]. Owned because an
/// outcome outlives the frame it arrived in by design — it is the thing the
/// application was waiting for.
///
/// `received` is deliberately absent: it is the one non-terminal state, it
/// describes how far a partial body got rather than what happened to the
/// message, and an application that treated it as an answer would be acting on
/// a progress report.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// Processed. Not "on disk": durability is asserted by `header.durable` on
    /// the way in, and a target that cannot honour it MUST refuse the message
    /// rather than accept it.
    Accepted,
    /// Invalid and unprocessable. **Not** redelivered by this node, and it
    /// increments the message's `delivery-count`, which is what a redelivery
    /// limit counts.
    Rejected {
        /// Why, where the peer said. A broker that omits it leaves the
        /// application with nothing to log.
        error: Option<Condition>,
    },
    /// Not acted upon and available again, unchanged. `delivery-count` MUST
    /// NOT be incremented, so a released message is indistinguishable from one
    /// that was never delivered.
    Released,
    /// Available again, with a note.
    Modified {
        /// `true` increments `delivery-count`, so a redelivery limit counts
        /// this attempt.
        delivery_failed: Option<bool>,
        /// `true` means this link endpoint MUST NOT see the message again.
        undeliverable_here: Option<bool>,
        /// Merged into the message's own `message-annotations`, values here
        /// replacing existing keys.
        message_annotations: Option<OwnedValue>,
    },
}

impl Outcome {
    /// The outcome a delivery state carries, or `None` for the one state that
    /// is not an outcome.
    ///
    /// Fallible only because `modified` may carry an annotations map, and
    /// keeping a value past the frame it arrived in means re-encoding it
    /// ([`OwnedValue`]).
    pub fn of(state: &DeliveryState<'_>) -> Result<Option<Self>, EncodeError> {
        Ok(match state {
            DeliveryState::Received { .. } => None,
            DeliveryState::Accepted => Some(Self::Accepted),
            DeliveryState::Rejected { error } => Some(Self::Rejected {
                error: error.as_ref().map(Condition::from_codec),
            }),
            DeliveryState::Released => Some(Self::Released),
            DeliveryState::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations,
            } => Some(Self::Modified {
                delivery_failed: *delivery_failed,
                undeliverable_here: *undeliverable_here,
                message_annotations: match message_annotations {
                    Some(value) => Some(OwnedValue::new(value)?),
                    None => None,
                },
            }),
        })
    }

    /// The wire form.
    #[must_use]
    pub fn to_state(&self) -> DeliveryState<'_> {
        match self {
            Self::Accepted => DeliveryState::Accepted,
            Self::Rejected { error } => DeliveryState::Rejected {
                error: error.as_ref().map(Condition::as_codec),
            },
            Self::Released => DeliveryState::Released,
            Self::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations,
            } => DeliveryState::Modified {
                delivery_failed: *delivery_failed,
                undeliverable_here: *undeliverable_here,
                message_annotations: message_annotations.as_ref().map(OwnedValue::value),
            },
        }
    }

    /// The specification's name for it.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected { .. } => "rejected",
            Self::Released => "released",
            Self::Modified { .. } => "modified",
        }
    }

    /// Whether this outcome increments the message's `header.delivery-count`.
    ///
    /// The difference an application can act on: a `released` message is
    /// indistinguishable from one never delivered, so a redelivery limit does
    /// not count it, while `rejected` and a `modified` with
    /// `delivery-failed=true` do.
    #[must_use]
    pub const fn increments_delivery_count(&self) -> bool {
        match self {
            Self::Accepted | Self::Released => false,
            Self::Rejected { .. } => true,
            Self::Modified {
                delivery_failed, ..
            } => matches!(delivery_failed, Some(true)),
        }
    }

    /// Whether the message may reach a consumer again.
    ///
    /// What the outcome *proves*, which is the reason an application asked for
    /// it: `accepted` and `rejected` are the end of the message's life at this
    /// node, `released` and `modified` put it back.
    #[must_use]
    pub const fn may_be_redelivered(&self) -> bool {
        match self {
            Self::Accepted | Self::Rejected { .. } => false,
            Self::Released | Self::Modified { .. } => true,
        }
    }
}

/// One unsettled delivery, as this end sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct Pending {
    /// The session-scoped id a `disposition` names.
    pub delivery_id: u32,
    /// The sender's tag, which is what survives a link being re-attached: an
    /// `attach.unsettled` map is keyed on the tag and not on the id.
    pub delivery_tag: Vec<u8>,
    /// The outcome this end has published, if any.
    pub ours: Option<Outcome>,
    /// The outcome the peer has published, if any.
    pub theirs: Option<Outcome>,
    /// Whether this end has settled. Once true, never false.
    pub settled_here: bool,
    /// Whether the peer has settled, which makes `theirs` its final answer.
    ///
    /// The delivery is over at both ends; what this end still holds is the
    /// outcome, kept until the application takes it
    /// ([`Link::settled`](crate::Link::settled), or the
    /// [`LinkEvent::Outcome`](crate::link::LinkEvent::Outcome) that reported
    /// it). Only a sender's deliveries are ever kept this way.
    pub settled_there: bool,
}

/// What applying a peer's `disposition` to one delivery amounts to.
#[derive(Clone, Debug, PartialEq)]
pub enum Settlement {
    /// No such delivery here: unknown, or already settled and forgotten.
    ///
    /// Not an error. This is idempotence: a `disposition` repeated after the
    /// delivery was settled is nothing, which is what makes the frame safe to
    /// resend and why a lost one needs no recovery.
    Nothing,
    /// The peer published an outcome and settled with it. Done at both ends,
    /// and the delivery is forgotten here too.
    Settled(Outcome),
    /// The peer published an outcome and did **not** settle: `rcv-settle-mode`
    /// is `second`, and this end owes it a settling `disposition` before the
    /// outcome is final for either.
    Provisional(Outcome),
    /// A non-terminal `received` state: a progress report on a resumed
    /// delivery, recorded and not acted upon.
    Progress,
}

/// One link endpoint's unsettled deliveries.
///
/// Keyed by `delivery-id`, because that is what a `disposition` names and a
/// `disposition` is the only frame that changes anything here. The tag is kept
/// alongside rather than as the key: it is what identifies a delivery across a
/// re-attach, and nothing in this map is looked up by it.
#[derive(Debug)]
pub struct Unsettled {
    by_id: BTreeMap<u32, Record>,
    max: usize,
    /// Whether a delivery the peer has finished stays until its outcome is
    /// taken: a sender's, where the outcome is the answer to what was sent.
    keep_final: bool,
}

/// The longest `delivery-tag` there is: Part 3 §3.2.1 bounds it at 32 octets
/// and the codec refuses a longer one on the way in, so every tag a link
/// holds fits here without an allocation of its own.
pub(crate) const MAX_TAG: usize = 32;

#[derive(Clone, Copy)]
struct Tag {
    len: u8,
    octets: [u8; MAX_TAG],
}

impl Tag {
    fn new(tag: &[u8]) -> Option<Self> {
        let len = u8::try_from(tag.len())
            .ok()
            .filter(|len| usize::from(*len) <= MAX_TAG)?;
        let mut octets = [0u8; MAX_TAG];
        octets[..tag.len()].copy_from_slice(tag);
        Some(Self { len, octets })
    }

    fn as_slice(&self) -> &[u8] {
        &self.octets[..usize::from(self.len)]
    }
}

impl std::fmt::Debug for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), f)
    }
}

#[derive(Debug)]
struct Record {
    tag: Tag,
    ours: Option<Outcome>,
    theirs: Option<Outcome>,
    settled_here: bool,
    /// Whether a provisional answer has been reported for this delivery, so
    /// that a peer repeating its `disposition` is not reported twice.
    reported: bool,
    /// Whether the delivery is over at both ends and what is left is the
    /// outcome, kept for the application to take. Only in a keeping map.
    finished: bool,
}

impl Unsettled {
    /// A map bounded at `max` deliveries, which forgets a delivery as soon as
    /// the peer has settled it. A receiver's.
    #[must_use]
    pub const fn new(max: usize) -> Self {
        Self {
            by_id: BTreeMap::new(),
            max,
            keep_final: false,
        }
    }

    /// A map bounded at `max` deliveries that keeps each one the peer has
    /// settled, finished, until [`Unsettled::take_final`] hands its outcome
    /// to the application. A sender's.
    #[must_use]
    pub const fn for_sender(max: usize) -> Self {
        Self {
            by_id: BTreeMap::new(),
            max,
            keep_final: true,
        }
    }

    /// How many deliveries are unsettled here.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether this end holds state for no delivery at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Whether one more delivery fits.
    #[must_use]
    pub fn has_room(&self) -> bool {
        self.by_id.len() < self.max
    }

    /// How many deliveries the map holds at most.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.max
    }

    /// Records a delivery as unsettled. `false` where the map is full, or
    /// where `tag` is longer than the 32 octets a `delivery-tag` can be.
    pub fn insert(&mut self, delivery_id: u32, tag: &[u8]) -> bool {
        if !self.has_room() {
            return false;
        }
        let Some(tag) = Tag::new(tag) else {
            return false;
        };
        self.by_id.insert(
            delivery_id,
            Record {
                tag,
                ours: None,
                theirs: None,
                settled_here: false,
                reported: false,
                finished: false,
            },
        );
        true
    }

    /// This end's view of one delivery.
    #[must_use]
    pub fn get(&self, delivery_id: u32) -> Option<Pending> {
        self.by_id
            .get(&delivery_id)
            .map(|record| record.pending(delivery_id))
    }

    /// Every unsettled delivery, in delivery-id order.
    #[must_use]
    pub fn pending(&self) -> Vec<Pending> {
        self.by_id
            .iter()
            .map(|(delivery_id, record)| record.pending(*delivery_id))
            .collect()
    }

    /// The delivery-ids this end holds that fall in `first..=last`, in serial
    /// order, **leaving out the finished ones**: a delivery that is over at
    /// both ends has nothing left to settle or to be told about, and what a
    /// range of them would cost is a walk for nothing.
    ///
    /// Serial arithmetic, because a delivery-id is an RFC 1982 serial number:
    /// a range that wraps is an ordinary range and not an empty one, and a
    /// `last` that is not after `first` names `first` alone. The range is a
    /// walk of the ordered map, never of the whole of it: one id costs one
    /// lookup whatever the map holds, which matters because every
    /// acknowledgement the application writes and every `disposition` a peer
    /// sends asks.
    #[must_use]
    pub fn ids_in(&self, first: u32, last: u32) -> Vec<u32> {
        let span = serial_distance(last, first);
        let end = first.wrapping_add(span);
        let mut ids = Vec::new();
        let open = |(id, record): (&u32, &Record)| (!record.finished).then_some(*id);
        if end >= first {
            ids.extend(self.by_id.range(first..=end).filter_map(open));
        } else {
            // Across the wrap: the top of the space, then the bottom, which
            // is also the order the ids were issued in.
            ids.extend(self.by_id.range(first..).filter_map(open));
            ids.extend(self.by_id.range(..=end).filter_map(open));
        }
        ids
    }

    /// Publishes this end's outcome for a delivery, and settles where asked.
    ///
    /// Returns `false` where there is nothing to publish it on — unknown,
    /// already forgotten or finished — which is the idempotent case rather
    /// than an error. An outcome already published is **not** replaced: it is
    /// terminal.
    pub fn record_ours(&mut self, delivery_id: u32, outcome: &Outcome, settle: bool) -> bool {
        let Some(record) = self.by_id.get_mut(&delivery_id) else {
            return false;
        };
        if record.finished {
            return false;
        }
        if record.ours.is_none() {
            record.ours = Some(outcome.clone());
        }
        if settle {
            record.settled_here = true;
            // Settled here *and* there is the end of the delivery's life.
            if record.theirs.is_some() || record.ours.is_some() {
                self.by_id.remove(&delivery_id);
            }
        }
        true
    }

    /// Marks a delivery settled by this end without publishing an outcome.
    ///
    /// The `settled` flag on a `transfer` is exactly this: the sender settles
    /// as it sends and never names a state.
    pub fn settle_here(&mut self, delivery_id: u32) -> bool {
        self.by_id.remove(&delivery_id).is_some()
    }

    /// Ends a delivery at this end after the peer's outcome for it is final,
    /// as [`Unsettled::settle_here`] does, except that a keeping map holds on
    /// to it, finished, until its outcome is taken.
    pub fn finish_here(&mut self, delivery_id: u32) -> bool {
        if !self.keep_final {
            return self.settle_here(delivery_id);
        }
        let Some(record) = self.by_id.get_mut(&delivery_id) else {
            return false;
        };
        if record.theirs.is_none() && record.ours.is_none() {
            // Nothing to keep: a finished record without an outcome would
            // hold a slot for an answer nobody can read.
            self.by_id.remove(&delivery_id);
            return true;
        }
        record.settled_here = true;
        record.finished = true;
        true
    }

    /// Takes the final outcome of a finished delivery and forgets the
    /// delivery, which is what frees its place. `None` where there is no
    /// finished delivery with that id: unknown, already taken, or not yet
    /// answered.
    pub fn take_final(&mut self, delivery_id: u32) -> Option<Outcome> {
        if !self.by_id.get(&delivery_id)?.finished {
            return None;
        }
        let record = self.by_id.remove(&delivery_id)?;
        record.theirs.or(record.ours)
    }

    /// Applies the peer's `disposition` to one delivery.
    ///
    /// `outcome` is `None` where the frame carried no state at all or a
    /// non-terminal `received` — the map takes the outcome already converted,
    /// so that nothing here depends on the codec and nothing here can fail.
    ///
    /// A provisional answer is reported **once**. The outcome the peer first
    /// published is terminal, so a repeat of it carries nothing new, and
    /// reporting each one would let a peer that repeats a `disposition`
    /// fill whatever the application has not read.
    pub fn record_theirs(
        &mut self,
        delivery_id: u32,
        outcome: Option<&Outcome>,
        settled: bool,
    ) -> Settlement {
        let Some(record) = self.by_id.get_mut(&delivery_id) else {
            return Settlement::Nothing;
        };
        // Over, and kept only for its outcome to be read: a repeat of the
        // frame, or a different answer, changes nothing, which is what a
        // delivery already forgotten does as well.
        if record.finished {
            return Settlement::Nothing;
        }
        if let Some(outcome) = outcome
            && record.theirs.is_none()
        {
            // Terminal: the first outcome the peer published is the outcome,
            // and no later frame from it can change one.
            record.theirs = Some(outcome.clone());
        }
        // The outcome that is now final is whichever end published one. A
        // settling `disposition` need carry no state at all — it confirms
        // what was already said — so under `rcv-settle-mode=second` the
        // sender's settling frame may be bare and the answer is the outcome
        // this end published itself.
        let known = record.theirs.clone().or_else(|| record.ours.clone());
        if settled {
            if self.keep_final && known.is_some() {
                record.settled_here = true;
                record.finished = true;
            } else {
                self.by_id.remove(&delivery_id);
            }
            return known.map_or(Settlement::Nothing, Settlement::Settled);
        }
        if record.reported {
            return Settlement::Nothing;
        }
        match known {
            Some(outcome) => {
                record.reported = true;
                Settlement::Provisional(outcome)
            }
            None => Settlement::Progress,
        }
    }
}

impl Record {
    fn pending(&self, delivery_id: u32) -> Pending {
        Pending {
            delivery_id,
            delivery_tag: self.tag.as_slice().to_vec(),
            ours: self.ours.clone(),
            theirs: self.theirs.clone(),
            settled_here: self.settled_here,
            settled_there: self.finished,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map() -> Unsettled {
        let mut map = Unsettled::new(4);
        assert!(map.insert(1, b"t-1"));
        assert!(map.insert(2, b"t-2"));
        map
    }

    #[test]
    fn an_outcome_says_whether_the_message_comes_back() {
        assert!(!Outcome::Accepted.may_be_redelivered());
        assert!(!Outcome::Rejected { error: None }.may_be_redelivered());
        assert!(Outcome::Released.may_be_redelivered());
        assert!(
            Outcome::Modified {
                delivery_failed: None,
                undeliverable_here: None,
                message_annotations: None,
            }
            .may_be_redelivered()
        );
    }

    #[test]
    fn only_some_outcomes_count_against_a_redelivery_limit() {
        assert!(!Outcome::Accepted.increments_delivery_count());
        assert!(
            !Outcome::Released.increments_delivery_count(),
            "a released message is indistinguishable from one never delivered"
        );
        assert!(Outcome::Rejected { error: None }.increments_delivery_count());
        let unset = Outcome::Modified {
            delivery_failed: None,
            undeliverable_here: None,
            message_annotations: None,
        };
        assert!(
            !unset.increments_delivery_count(),
            "unset is not false: it leaves the node's own policy in charge"
        );
        let failed = Outcome::Modified {
            delivery_failed: Some(true),
            undeliverable_here: None,
            message_annotations: None,
        };
        assert!(failed.increments_delivery_count());
    }

    #[test]
    fn received_is_not_an_outcome() {
        assert_eq!(
            Outcome::of(&DeliveryState::Received {
                section_number: 0,
                section_offset: 0,
            }),
            Ok(None)
        );
        assert_eq!(
            Outcome::of(&DeliveryState::Accepted),
            Ok(Some(Outcome::Accepted))
        );
    }

    #[test]
    fn a_settled_disposition_ends_the_delivery_at_both_ends() {
        let mut map = map();
        assert_eq!(
            map.record_theirs(1, Some(&Outcome::Accepted), true),
            Settlement::Settled(Outcome::Accepted)
        );
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(1), None, "forgotten");
    }

    #[test]
    fn a_disposition_for_a_forgotten_delivery_is_nothing_rather_than_an_error() {
        let mut map = map();
        let _ = map.record_theirs(1, Some(&Outcome::Accepted), true);
        // The same frame again, which is what makes it safe to repeat.
        assert_eq!(
            map.record_theirs(1, Some(&Outcome::Accepted), true),
            Settlement::Nothing
        );
        assert_eq!(
            map.record_theirs(99, Some(&Outcome::Released), true),
            Settlement::Nothing
        );
    }

    #[test]
    fn an_unsettled_disposition_is_provisional_and_the_outcome_is_terminal() {
        let mut map = map();
        assert_eq!(
            map.record_theirs(1, Some(&Outcome::Accepted), false),
            Settlement::Provisional(Outcome::Accepted)
        );
        assert_eq!(map.get(1).unwrap().theirs, Some(Outcome::Accepted));
        // A second, different state from the same peer does not replace the
        // first: an outcome once published is terminal.
        let _ = map.record_theirs(1, Some(&Outcome::Released), false);
        assert_eq!(map.get(1).unwrap().theirs, Some(Outcome::Accepted));
    }

    #[test]
    fn a_repeated_provisional_disposition_is_reported_once() {
        let mut map = map();
        assert_eq!(
            map.record_theirs(1, Some(&Outcome::Accepted), false),
            Settlement::Provisional(Outcome::Accepted)
        );
        // Each repeat would otherwise be one more report for the application
        // to read, and a peer can send them as fast as it can write.
        for _ in 0..100 {
            assert_eq!(
                map.record_theirs(1, Some(&Outcome::Accepted), false),
                Settlement::Nothing
            );
            assert_eq!(map.record_theirs(1, None, false), Settlement::Nothing);
        }
        // The final answer is still reported.
        assert_eq!(
            map.record_theirs(1, Some(&Outcome::Accepted), true),
            Settlement::Settled(Outcome::Accepted)
        );
    }

    #[test]
    fn a_received_state_is_progress_and_not_an_answer() {
        let mut map = map();
        // `received` converts to no outcome at all, so what reaches the map
        // is the absence, and the absence is progress rather than an answer.
        assert_eq!(map.record_theirs(1, None, false), Settlement::Progress);
        assert!(map.get(1).unwrap().theirs.is_none(), "nothing terminal yet");
    }

    #[test]
    fn settling_here_is_one_way_and_idempotent() {
        let mut map = map();
        assert!(map.record_ours(1, &Outcome::Accepted, true));
        assert_eq!(map.get(1), None);
        assert!(
            !map.record_ours(1, &Outcome::Accepted, true),
            "settling twice changes nothing"
        );
    }

    #[test]
    fn publishing_an_outcome_without_settling_keeps_the_delivery() {
        let mut map = map();
        assert!(map.record_ours(1, &Outcome::Accepted, false));
        let pending = map.get(1).unwrap();
        assert_eq!(pending.ours, Some(Outcome::Accepted));
        assert!(!pending.settled_here);
        // And it is terminal here too.
        assert!(map.record_ours(1, &Outcome::Released, false));
        assert_eq!(map.get(1).unwrap().ours, Some(Outcome::Accepted));
    }

    fn sender_map() -> Unsettled {
        let mut map = Unsettled::for_sender(4);
        assert!(map.insert(1, b"t-1"));
        assert!(map.insert(2, b"t-2"));
        map
    }

    /// A sender's answer exists nowhere but in its record, so a delivery the
    /// peer has settled is kept, finished, with the outcome, until the
    /// application takes it — and the record keeps its place meanwhile.
    #[test]
    fn a_senders_settled_delivery_is_kept_until_its_outcome_is_taken() {
        let rejected = Outcome::Rejected {
            error: Some(Condition::new("amqp:not-allowed")),
        };
        let mut map = sender_map();
        assert_eq!(
            map.record_theirs(1, Some(&rejected), true),
            Settlement::Settled(rejected.clone())
        );
        let kept = map.get(1).expect("kept, not forgotten");
        assert!(kept.settled_there && kept.settled_here);
        assert_eq!(kept.theirs, Some(rejected.clone()));
        assert_eq!(map.len(), 2, "it still holds its place");

        assert_eq!(map.take_final(1), Some(rejected));
        assert_eq!(map.get(1), None, "taking it is what forgets it");
        assert_eq!(map.len(), 1);
        assert_eq!(map.take_final(1), None, "and it is taken once");
        assert_eq!(map.take_final(2), None, "an unanswered one has no outcome");
    }

    /// What keeps the place is what bounds the producer: with the map full of
    /// answers nobody has taken, one more delivery does not fit, and taking
    /// one makes room.
    #[test]
    fn answers_nobody_has_taken_hold_the_bound() {
        let mut map = Unsettled::for_sender(2);
        assert!(map.insert(1, b"a") && map.insert(2, b"b"));
        let _ = map.record_theirs(1, Some(&Outcome::Accepted), true);
        let _ = map.record_theirs(2, Some(&Outcome::Released), true);
        assert!(!map.has_room(), "two answers, unread, are two deliveries");
        assert!(!map.insert(3, b"c"));
        assert_eq!(map.take_final(1), Some(Outcome::Accepted));
        assert!(map.has_room());
    }

    /// The first answer is the answer: a repeat of the frame, or a different
    /// outcome after it, changes nothing about a delivery that is over.
    #[test]
    fn a_finished_delivery_is_not_answered_twice() {
        let mut map = sender_map();
        let _ = map.record_theirs(1, Some(&Outcome::Accepted), true);
        assert_eq!(
            map.record_theirs(1, Some(&Outcome::Released), true),
            Settlement::Nothing
        );
        assert_eq!(map.record_theirs(1, None, false), Settlement::Nothing);
        assert!(!map.record_ours(1, &Outcome::Released, true));
        assert_eq!(map.take_final(1), Some(Outcome::Accepted));
    }

    /// A finished delivery is over: a range over it settles nothing and a
    /// walk of the map does not find it.
    #[test]
    fn a_finished_delivery_is_not_found_by_a_range() {
        let mut map = sender_map();
        let _ = map.record_theirs(1, Some(&Outcome::Accepted), true);
        assert_eq!(map.ids_in(1, 2), vec![2]);
    }

    /// The second half of `rcv-settle-mode=second`: this end settles because
    /// the receiver's outcome is final, and the outcome is still an answer.
    #[test]
    fn finishing_here_keeps_a_senders_answer_and_forgets_a_receivers() {
        let mut sender = sender_map();
        let _ = sender.record_theirs(1, Some(&Outcome::Accepted), false);
        assert!(sender.finish_here(1));
        assert_eq!(sender.take_final(1), Some(Outcome::Accepted));

        let mut receiver = map();
        let _ = receiver.record_theirs(1, Some(&Outcome::Accepted), false);
        assert!(receiver.finish_here(1));
        assert_eq!(receiver.get(1), None, "a receiver's map forgets as before");
        assert_eq!(receiver.take_final(1), None);
    }

    /// A receiver's map is unchanged: what the sender settles there is the
    /// end of a dance whose outcome this end chose itself.
    #[test]
    fn a_receivers_map_still_forgets_what_the_peer_settles() {
        let mut map = map();
        let _ = map.record_theirs(1, Some(&Outcome::Accepted), true);
        assert_eq!(map.get(1), None);
        assert_eq!(map.take_final(1), None);
    }

    #[test]
    fn a_range_is_serial_and_wraps() {
        let mut map = Unsettled::new(8);
        assert!(map.insert(u32::MAX - 1, b"a"));
        assert!(map.insert(u32::MAX, b"b"));
        assert!(map.insert(0, b"c"));
        assert!(map.insert(1, b"d"));
        assert_eq!(
            map.ids_in(u32::MAX, 0),
            vec![u32::MAX, 0],
            "a range across the wrap is an ordinary range, in the order the ids were issued"
        );
        assert_eq!(
            map.ids_in(u32::MAX - 1, 1),
            vec![u32::MAX - 1, u32::MAX, 0, 1]
        );
        assert_eq!(map.ids_in(7, 9), Vec::<u32>::new());
        assert_eq!(
            map.ids_in(1, 0),
            vec![1],
            "a last that is not after first names first alone"
        );
    }

    #[test]
    fn a_single_id_costs_one_lookup_and_not_a_walk_of_the_map() {
        use std::hint::black_box;
        use std::time::Instant;

        fn filled(n: u32) -> Unsettled {
            let mut map = Unsettled::new(usize::try_from(n).unwrap());
            for id in 0..n {
                assert!(map.insert(id, b"tag"));
            }
            map
        }
        // Two map sizes and the same number of calls: what a walk of the map
        // costs grows with the size, and an ordered lookup does not. The
        // comparison is the ratio, so it holds on a loaded machine, and the
        // margin is wide: a walk is ~64x apart here, a lookup ~1x.
        let small = filled(64);
        let large = filled(4096);
        let calls = 20_000u32;
        let run = |map: &Unsettled| {
            let started = Instant::now();
            for call in 0..calls {
                let id = black_box(call % 64);
                black_box(map.ids_in(id, id));
            }
            started.elapsed()
        };
        let _warm = (run(&small), run(&large));
        let small_time = run(&small);
        let large_time = run(&large);
        assert!(
            large_time < small_time * 8 + std::time::Duration::from_millis(50),
            "64 entries took {small_time:?}, 4096 took {large_time:?}"
        );
    }

    #[test]
    fn a_tag_longer_than_the_protocol_allows_is_not_recorded() {
        let mut map = Unsettled::new(4);
        assert!(map.insert(1, &[7u8; 32]));
        assert_eq!(map.get(1).unwrap().delivery_tag, vec![7u8; 32]);
        assert!(!map.insert(2, &[7u8; 33]));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn the_map_is_bounded_and_says_so() {
        let mut map = Unsettled::new(2);
        assert!(map.insert(1, b"a"));
        assert!(map.insert(2, b"b"));
        assert!(!map.has_room());
        assert!(!map.insert(3, b"c"), "refused, not grown");
        let _ = map.record_theirs(1, Some(&Outcome::Accepted), true);
        assert!(map.has_room(), "settling is what makes room");
    }
}
