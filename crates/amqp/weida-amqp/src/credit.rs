//! Link credit: counted in messages, granted only by the receiver.
//!
//! The other half of AMQP's flow control — [`crate::window`] is the session's,
//! counted in `transfer` **frames**. Both must permit a transfer and the
//! specification ties them together nowhere, which is why they are two types
//! here. One unit of `link-credit` permits `delivery-count` to advance by one,
//! that is, one message (Part 2 §2.6.7); because a message may span any number
//! of frames, link credit says nothing about bytes.
//!
//! Decision [0003](../../../../docs/decisions/0003-credit-unit.md) took this
//! vocabulary for weida's own L2 credit, so the words here are the words
//! there: *credit* is permission the **receiver** grants, and the unit is a
//! message.
//!
//! # Four variables, and who owns each
//!
//! | Variable | Only this end may change it |
//! | --- | --- |
//! | `delivery-count` | the sender |
//! | `link-credit` | the receiver |
//! | `available` | the sender |
//! | `drain` | the receiver |
//!
//! `delivery-count` is a 32-bit RFC 1982 serial number and not a count: it
//! starts wherever the sender's `attach.initial-delivery-count` said and it
//! wraps. Every comparison here goes through [`crate::window::serial_distance`].
//!
//! # An absolute limit, not an increment
//!
//! What a receiver puts on the wire is `delivery-count` and `link-credit`, and
//! what the sender computes from them is
//!
//! ```text
//! link-credit(sender) = delivery-count(receiver) + link-credit(receiver)
//!                       - delivery-count(sender)
//! ```
//!
//! so the pair communicates an **absolute delivery-limit** rather than a
//! delta. That is the whole design: `flow` frames are idempotent, and a
//! duplicated or reordered grant cannot inflate credit. RabbitMQ states the
//! operational consequence as "link credit is set, not accumulated".
//!
//! A receiver that has not yet seen the sender's `attach` does not know a
//! `delivery-count`, MUST NOT set the field, and the sender then substitutes
//! the `initial-delivery-count` it sent itself.
//!
//! # Zero is a stall, not an error
//!
//! "If the link-credit is less than or equal to zero ... a sender MUST NOT
//! send more messages." Nothing is dropped and nothing disconnects: the sender
//! waits. A freshly attached link starts at `link-credit = 0`, so a sender has
//! no permission to send anything at all until a `flow` arrives — which is why
//! [`Credit::may_send`] is asked before every frame rather than once.
//!
//! # Drain turns "wait for a message" into "wait for an answer"
//!
//! `drain=true` tells the sender to send what it has and then advance
//! `delivery-count` until `link-credit` is zero, reporting the new state. The
//! receiver then learns *unambiguously* whether its credit was spent on a
//! message or consumed by the drain, which is how a get-with-timeout is built:
//! `flow(link-credit=1)`, wait, `flow(drain=true)`, wait for zero.

use weida_amqp_codec::Role;

use crate::window::{serial_distance, serial_gt};

/// One link endpoint's credit state (Part 2 §2.6.7).
///
/// The same four variables live at both ends, but the two ends may change
/// different ones, so the methods are named for the end that may call them:
/// [`Credit::apply_grant`] and [`Credit::record_sent`] are a sender's,
/// [`Credit::grant`] and [`Credit::record_received`] a receiver's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Credit {
    role: Role,
    delivery_count: u32,
    link_credit: u32,
    available: u32,
    drain: bool,
    /// What our own `attach.initial-delivery-count` said, kept because it is
    /// the value a sender substitutes for a `flow` whose `delivery-count` is
    /// unset.
    initial_delivery_count: u32,
    /// Whether the partner's `attach` has been seen. A receiver's
    /// `flow.delivery-count` MUST NOT be set before then.
    seen_remote_attach: bool,
    /// A receiver's: the highest delivery-limit it has ever told the sender
    /// it may send up to. The sender may still be acting on a limit this end
    /// has since lowered, so what it sends up to this is in flight rather
    /// than excess; anything past it was never granted, which is what
    /// [`Credit::record_received`] refuses.
    announced_limit: u32,
}

impl Credit {
    /// A sending endpoint starting its sequence at `initial_delivery_count`.
    ///
    /// `link-credit` is zero: a sender may send nothing until the receiver
    /// grants.
    #[must_use]
    pub const fn sender(initial_delivery_count: u32) -> Self {
        Self {
            role: Role::Sender,
            delivery_count: initial_delivery_count,
            link_credit: 0,
            available: 0,
            drain: false,
            initial_delivery_count,
            seen_remote_attach: false,
            announced_limit: 0,
        }
    }

    /// A receiving endpoint. Its `delivery-count` is meaningless until the
    /// sender's `attach` reports where the sequence starts.
    #[must_use]
    pub const fn receiver() -> Self {
        Self {
            role: Role::Receiver,
            delivery_count: 0,
            link_credit: 0,
            available: 0,
            drain: false,
            initial_delivery_count: 0,
            seen_remote_attach: false,
            announced_limit: 0,
        }
    }

    /// Which end this is.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// The `delivery-count`: where the sequence has reached.
    #[must_use]
    pub const fn delivery_count(&self) -> u32 {
        self.delivery_count
    }

    /// How many more messages may be sent.
    #[must_use]
    pub const fn link_credit(&self) -> u32 {
        self.link_credit
    }

    /// The sender's backlog: how much credit it could use.
    #[must_use]
    pub const fn available(&self) -> u32 {
        self.available
    }

    /// Whether the receiver has asked the sender to drain.
    #[must_use]
    pub const fn drain(&self) -> bool {
        self.drain
    }

    /// The absolute delivery-limit the two ends agree on.
    #[must_use]
    pub const fn delivery_limit(&self) -> u32 {
        self.delivery_count.wrapping_add(self.link_credit)
    }

    /// Whether one more message may be sent.
    ///
    /// At zero a sender MUST NOT send: it waits for a `flow` that raises the
    /// limit. The session window has to permit the frame as well, and the two
    /// are asked separately because they are separate schemes.
    #[must_use]
    pub const fn may_send(&self) -> bool {
        self.link_credit > 0
    }

    /// What goes in `flow.delivery-count` from this end.
    ///
    /// A sender always reports its current value. A receiver reports the last
    /// value it knows of the sender's, and MUST NOT set the field at all
    /// before it has seen the sender's `attach` (Part 2 §2.7.4) — a peer
    /// receiving it anyway would recompute its credit from a number that means
    /// nothing.
    #[must_use]
    pub const fn wire_delivery_count(&self) -> Option<u32> {
        match self.role {
            Role::Sender => Some(self.delivery_count),
            Role::Receiver if self.seen_remote_attach => Some(self.delivery_count),
            Role::Receiver => None,
        }
    }

    /// Records the sender's `attach.initial-delivery-count`, learned by a
    /// receiver from the answering `attach`.
    pub const fn seen_sender_attach(&mut self, initial_delivery_count: u32) {
        self.seen_remote_attach = true;
        // Adopting the sender's starting point keeps the limit this end has
        // already granted meaningful: credit is a distance from the count, so
        // moving the count without moving the limit would be a silent grant.
        let credit = self.link_credit;
        self.delivery_count = initial_delivery_count;
        self.link_credit = credit;
        self.announced_limit = initial_delivery_count.wrapping_add(credit);
    }

    /// A **sender**: applies a receiver's `flow`.
    ///
    /// `delivery_count` is the receiver's last known value of ours, `None`
    /// where it had not seen our `attach` — in which case our own
    /// `initial-delivery-count` stands in, which is the branch that decides
    /// whether a link can ever send its first message.
    ///
    /// A `flow` carrying no `link-credit` carries no link credit state and
    /// leaves this end's credit alone; only `drain` is taken from it.
    pub fn apply_grant(
        &mut self,
        delivery_count: Option<u32>,
        link_credit: Option<u32>,
        drain: bool,
    ) {
        debug_assert_eq!(self.role, Role::Sender);
        self.drain = drain;
        let Some(granted) = link_credit else {
            return;
        };
        let base = delivery_count.unwrap_or(self.initial_delivery_count);
        let limit = base.wrapping_add(granted);
        // Serial arithmetic, and floored: a limit already behind our count is
        // no credit rather than four billion.
        self.link_credit = serial_distance(limit, self.delivery_count);
    }

    /// A **sender**: accounts for one message going out.
    ///
    /// Returns `false` and changes nothing where there was no credit, so a
    /// caller that ignored [`Credit::may_send`] still cannot exceed the limit.
    /// "Whenever the sender increases `delivery-count`, it MUST decrease
    /// `link-credit` by the same amount" — one operation, because the limit
    /// they add up to is what must not move.
    pub fn record_sent(&mut self) -> bool {
        debug_assert_eq!(self.role, Role::Sender);
        if !self.may_send() {
            return false;
        }
        self.delivery_count = self.delivery_count.wrapping_add(1);
        self.link_credit -= 1;
        self.available = self.available.saturating_sub(1);
        true
    }

    /// A **sender**: reports its backlog, so the receiver can size its grant.
    pub const fn set_available(&mut self, available: u32) {
        debug_assert!(matches!(self.role, Role::Sender));
        self.available = available;
    }

    /// A **sender**: consumes all remaining credit for a drain.
    ///
    /// "With nothing available the sender MUST advance `delivery-count` until
    /// `link-credit` is zero and send its updated `flow`." Returns how much
    /// credit was consumed, which is zero where there was none — and a sender
    /// that consumed nothing still owes the `flow`, because the receiver is
    /// waiting for the definite answer rather than for a change.
    pub fn consume_for_drain(&mut self) -> u32 {
        debug_assert_eq!(self.role, Role::Sender);
        let consumed = self.link_credit;
        self.delivery_count = self.delivery_count.wrapping_add(consumed);
        self.link_credit = 0;
        consumed
    }

    /// A **receiver**: sets the credit it grants.
    ///
    /// Sets, and does not add: what reaches the wire is the limit
    /// `delivery-count + link-credit`, so granting 10 twice grants 10.
    pub const fn grant(&mut self, credit: u32) {
        debug_assert!(matches!(self.role, Role::Receiver));
        self.link_credit = credit;
        let limit = self.delivery_limit();
        if serial_gt(limit, self.announced_limit) {
            self.announced_limit = limit;
        }
    }

    /// A **receiver**: asks the sender to drain.
    pub const fn set_drain(&mut self, drain: bool) {
        debug_assert!(matches!(self.role, Role::Receiver));
        self.drain = drain;
    }

    /// A **receiver**: accounts for one message that arrived.
    ///
    /// `true` where the message is within what this end has told the sender
    /// it may send, and `false` where it is past every limit this end ever
    /// announced — a sender that ignores credit, or a delivery nobody granted
    /// room for. The count advances either way, because the sequence is the
    /// sender's and it moved.
    ///
    /// A message past the *current* credit but within an earlier, higher
    /// limit is `true`: a receiver that lowers its limit while transfers are
    /// in flight will see them, and they were sent under credit it had
    /// granted. The specification lets it handle those, and this client
    /// does, because dropping a message sent under granted credit would lose
    /// it; what it does not handle is a message that was never granted, which
    /// is `amqp:link:transfer-limit-exceeded`.
    pub fn record_received(&mut self) -> bool {
        debug_assert_eq!(self.role, Role::Receiver);
        let before = self.delivery_count;
        self.delivery_count = before.wrapping_add(1);
        self.link_credit = self.link_credit.saturating_sub(1);
        serial_gt(self.announced_limit, before)
    }

    /// A **receiver**: applies a sender's `flow`.
    ///
    /// The sender's `delivery-count` is authoritative, and adopting it is how
    /// a drain becomes visible: the sender advanced the count to the limit, so
    /// the recomputed credit is zero and the receiver knows its grant was
    /// consumed rather than spent on a message.
    ///
    /// **A count behind this end's own is not adopted.** `flow` and `transfer`
    /// are ordered on the session, so every delivery the sender counted
    /// before a `flow` was received before it, and the count in the `flow` is
    /// never behind: it is level with this end's, or ahead of it by what a
    /// drain consumed. A count that has moved backwards is a peer rewinding
    /// it, and adopting it would hand back credit that was already spent —
    /// `link-credit` would be recomputed from the old count and the next
    /// deliveries would pass the check against the highest limit ever
    /// announced again, for ever. What was counted stays counted.
    pub fn apply_sender_state(&mut self, delivery_count: Option<u32>, available: Option<u32>) {
        debug_assert_eq!(self.role, Role::Receiver);
        if let Some(available) = available {
            self.available = available;
        }
        let Some(count) = delivery_count else {
            return;
        };
        self.seen_remote_attach = true;
        if serial_gt(self.delivery_count, count) {
            return;
        }
        let limit = self.delivery_limit();
        self.delivery_count = count;
        self.link_credit = serial_distance(limit, count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_sender_may_not_send() {
        let credit = Credit::sender(0);
        assert!(!credit.may_send());
        assert_eq!(credit.link_credit(), 0);
    }

    #[test]
    fn a_grant_is_a_limit_and_not_an_increment() {
        let mut credit = Credit::sender(5);
        credit.apply_grant(Some(5), Some(10), false);
        assert_eq!(credit.link_credit(), 10);
        // The same flow twice is the same limit: idempotent, which is what
        // makes a lost or duplicated grant harmless.
        credit.apply_grant(Some(5), Some(10), false);
        assert_eq!(credit.link_credit(), 10);
        assert_eq!(credit.delivery_limit(), 15);
    }

    #[test]
    fn a_grant_from_a_receiver_that_has_not_seen_our_attach_uses_our_initial_count() {
        let mut credit = Credit::sender(7);
        // No `delivery-count` on the wire, because the receiver does not know
        // one yet. Without the substitution the limit would be 3 and a link
        // that starts at 7 could never send.
        credit.apply_grant(None, Some(3), false);
        assert_eq!(credit.link_credit(), 3);
        assert_eq!(credit.delivery_limit(), 10);
    }

    #[test]
    fn sending_moves_the_count_and_leaves_the_limit_where_it_was() {
        let mut credit = Credit::sender(0);
        credit.apply_grant(Some(0), Some(2), false);
        let limit = credit.delivery_limit();
        assert!(credit.record_sent());
        assert_eq!(credit.delivery_count(), 1);
        assert_eq!(credit.link_credit(), 1);
        assert_eq!(credit.delivery_limit(), limit, "the limit is what is fixed");
        assert!(credit.record_sent());
        assert!(!credit.may_send());
        assert!(!credit.record_sent(), "no credit, no send, and no change");
        assert_eq!(credit.delivery_count(), 2);
    }

    #[test]
    fn a_limit_behind_the_count_is_no_credit_rather_than_four_billion() {
        let mut credit = Credit::sender(10);
        credit.apply_grant(Some(4), Some(2), false);
        assert_eq!(credit.link_credit(), 0);
        assert!(!credit.may_send());
    }

    #[test]
    fn the_count_is_a_serial_number_and_wraps() {
        let mut credit = Credit::sender(u32::MAX);
        credit.apply_grant(Some(u32::MAX), Some(2), false);
        assert!(credit.record_sent());
        assert_eq!(credit.delivery_count(), 0, "wrapped, not saturated");
        assert_eq!(credit.link_credit(), 1);
        // And a grant computed across the wrap is still one unit of distance.
        credit.apply_grant(Some(0), Some(1), false);
        assert_eq!(credit.link_credit(), 1);
    }

    #[test]
    fn drain_consumes_every_remaining_credit() {
        let mut credit = Credit::sender(0);
        credit.apply_grant(Some(0), Some(5), true);
        assert!(credit.drain());
        assert_eq!(credit.consume_for_drain(), 5);
        assert_eq!(credit.link_credit(), 0);
        assert_eq!(credit.delivery_count(), 5, "advanced to the limit");
        // Re-applying the same absolute limit grants nothing, so the drain
        // cannot be undone by a repeated flow.
        credit.apply_grant(Some(0), Some(5), true);
        assert_eq!(credit.link_credit(), 0);
    }

    #[test]
    fn a_drain_with_nothing_left_still_owes_a_flow() {
        let mut credit = Credit::sender(3);
        credit.apply_grant(Some(3), Some(0), true);
        assert_eq!(credit.consume_for_drain(), 0);
        assert_eq!(credit.delivery_count(), 3);
    }

    /// A sender that rewinds its count in a `flow` does not get the credit
    /// back: grant ten, receive ten, and a `flow` claiming the count is zero
    /// would — adopted — recompute ten credits out of the same limit and let
    /// the next ten deliveries pass for ever.
    #[test]
    fn a_flow_that_rewinds_the_count_does_not_reopen_credit() {
        let mut credit = Credit::receiver();
        credit.seen_sender_attach(0);
        credit.grant(10);
        for _ in 0..10 {
            assert!(credit.record_received());
        }
        assert_eq!(credit.link_credit(), 0);
        assert_eq!(credit.delivery_count(), 10);

        credit.apply_sender_state(Some(0), Some(0));

        assert_eq!(credit.delivery_count(), 10, "the rewind is not adopted");
        assert_eq!(credit.link_credit(), 0, "and nothing was handed back");
        assert!(
            !credit.record_received(),
            "the eleventh was never granted, whatever the flow said"
        );
    }

    /// Behind in serial arithmetic, which is what a wrap makes of a count
    /// that went the wrong way across it; and level or ahead — a drain — is
    /// still taken.
    #[test]
    fn only_a_count_that_is_level_or_ahead_is_adopted() {
        let mut credit = Credit::receiver();
        credit.seen_sender_attach(u32::MAX - 1);
        credit.grant(4);
        for _ in 0..3 {
            assert!(credit.record_received());
        }
        assert_eq!(credit.delivery_count(), 1, "wrapped");

        credit.apply_sender_state(Some(u32::MAX - 1), None);
        assert_eq!(credit.delivery_count(), 1, "behind across the wrap");

        credit.apply_sender_state(Some(1), None);
        assert_eq!(credit.delivery_count(), 1, "level changes nothing");
        credit.apply_sender_state(Some(2), None);
        assert_eq!(credit.delivery_count(), 2, "ahead: a drain consumed credit");
        assert_eq!(credit.link_credit(), 0, "up to the limit and no further");
    }

    #[test]
    fn a_receiver_grants_by_setting_and_spends_by_receiving() {
        let mut credit = Credit::receiver();
        credit.grant(3);
        assert_eq!(credit.link_credit(), 3);
        credit.grant(3);
        assert_eq!(credit.link_credit(), 3, "set, not accumulated");
        assert!(credit.record_received());
        assert_eq!(credit.link_credit(), 2);
        assert_eq!(credit.delivery_count(), 1);
    }

    #[test]
    fn a_receiver_names_no_delivery_count_before_the_senders_attach() {
        let mut credit = Credit::receiver();
        credit.grant(1);
        assert_eq!(credit.wire_delivery_count(), None);
        credit.seen_sender_attach(42);
        assert_eq!(credit.wire_delivery_count(), Some(42));
        assert_eq!(credit.link_credit(), 1, "the grant survives the count");
        assert_eq!(credit.delivery_limit(), 43);
    }

    #[test]
    fn a_sender_always_names_its_delivery_count() {
        let credit = Credit::sender(9);
        assert_eq!(credit.wire_delivery_count(), Some(9));
    }

    #[test]
    fn a_receiver_sees_a_drain_as_credit_it_did_not_spend() {
        let mut credit = Credit::receiver();
        credit.seen_sender_attach(0);
        credit.grant(5);
        credit.set_drain(true);
        // The sender drained: it advanced the count to the limit without
        // sending anything, which is the definite answer drain exists for.
        credit.apply_sender_state(Some(5), Some(0));
        assert_eq!(credit.link_credit(), 0);
        assert_eq!(credit.delivery_count(), 5);
    }

    #[test]
    fn a_receiver_past_everything_it_granted_reports_it() {
        let mut credit = Credit::receiver();
        credit.seen_sender_attach(0);
        credit.grant(1);
        assert!(credit.record_received());
        assert!(
            !credit.record_received(),
            "past the limit, and said so: the caller refuses the delivery"
        );
        assert_eq!(credit.delivery_count(), 2, "the count still advanced");
    }

    #[test]
    fn a_receiver_that_never_granted_anything_refuses_the_first_delivery() {
        let mut credit = Credit::receiver();
        credit.seen_sender_attach(7);
        assert!(!credit.record_received());
    }

    #[test]
    fn what_was_in_flight_when_credit_was_lowered_is_still_accepted() {
        let mut credit = Credit::receiver();
        credit.seen_sender_attach(0);
        credit.grant(5);
        assert!(credit.record_received());
        assert!(credit.record_received());
        // `stop()`: the limit drops to what has arrived, while the sender may
        // already have sent up to the limit it was told about.
        credit.grant(0);
        assert_eq!(credit.link_credit(), 0);
        for _ in 0..3 {
            assert!(
                credit.record_received(),
                "sent under credit this end had granted"
            );
        }
        assert!(
            !credit.record_received(),
            "the sixth was never granted, however the limit moved in between"
        );
    }

    #[test]
    fn the_announced_limit_follows_serial_arithmetic_across_the_wrap() {
        let mut credit = Credit::receiver();
        credit.seen_sender_attach(u32::MAX - 1);
        credit.grant(3);
        for _ in 0..3 {
            assert!(credit.record_received());
        }
        assert_eq!(credit.delivery_count(), 1, "wrapped");
        assert!(!credit.record_received());
    }

    #[test]
    fn a_flow_carrying_no_link_credit_leaves_credit_alone() {
        let mut credit = Credit::sender(0);
        credit.apply_grant(Some(0), Some(4), false);
        credit.apply_grant(Some(0), None, true);
        assert_eq!(credit.link_credit(), 4);
        assert!(credit.drain(), "but drain is still taken from it");
    }
}
