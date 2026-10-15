//! What a forwarding device asks of its upstream, and each subscriber's share
//! of it.
//!
//! A pub-sub proxy turns the subscriptions of the subscribers behind it into
//! subscriptions of its own, upstream. They all land in **one** table — the
//! XSUB's, and the publisher's table for the proxy's connection — however many
//! subscribers there are, and `max_subscriptions` bounds that table and not
//! anybody's part of it. A device that forwards what it reads as it reads it
//! goes wrong in two ways:
//!
//! - **One subscriber can take the table.** A subscription's prefix is the
//!   subscriber's to choose, and 37/ZMTP bounds neither how many it sends nor
//!   how long they are. A subscriber that holds `max_subscriptions` prefixes
//!   fills the table, and every topic another subscriber asks for afterwards is
//!   refused upstream, in silence: the honest subscriber waits for messages
//!   nobody will send.
//! - **A prefix is forwarded once per subscriber that holds it.** This
//!   library's tables count the repeats (29/PUBSUB: two subscriptions "would
//!   require two CANCEL commands to undo"), and libzmq's publisher keeps a *set*
//!   of connections per prefix, so the first subscriber to leave cancels the
//!   prefix for everybody still holding it.
//!
//! `UpstreamTable` is the device's side of the cure: a count of the
//! subscribers holding each prefix, so that a prefix goes upstream when its
//! first holder arrives and a cancel goes when its last one leaves, and a
//! ceiling on how many distinct prefixes one subscriber may have forwarded
//! ([`upstream_share`]).
//!
//! It only counts. Which subscriber a subscription came from, and what is
//! done to one that asks for more than its share, are the receiving side's
//! ([`crate::split`]), which is the one that knows its subscribers.

use std::collections::HashMap;

/// The divisor that sizes one subscriber's share of an upstream table: it may
/// have `1 / UPSTREAM_SHARE_DIVISOR` of the table's prefixes forwarded.
///
/// Eight, like the share one host gets of a socket's peers: no single
/// subscriber is trusted with more than an eighth of what every subscriber
/// shares, and an honest one holds a handful. A subscriber that needs more
/// than a default proxy gives raises `max_subscriptions` on the proxy's
/// sockets, which raises every share with it.
pub const UPSTREAM_SHARE_DIVISOR: usize = 8;

/// The fewest prefixes one subscriber may have forwarded, whatever the table's
/// size is, so that a small table is not divided into shares too small to
/// use.
pub const UPSTREAM_SHARE_FLOOR: usize = 8;

/// How many distinct prefixes one subscriber may have forwarded upstream when
/// the table holds `capacity` of them: its `1 / UPSTREAM_SHARE_DIVISOR` share,
/// never fewer than [`UPSTREAM_SHARE_FLOOR`] and never more than the table
/// itself. A default proxy, whose tables hold 1024 prefixes, gives each
/// subscriber 128.
pub const fn upstream_share(capacity: usize) -> usize {
    let share = capacity / UPSTREAM_SHARE_DIVISOR;
    let share = if share > UPSTREAM_SHARE_FLOOR {
        share
    } else {
        UPSTREAM_SHARE_FLOOR
    };
    if share < capacity { share } else { capacity }
}

/// What the table decided about one subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The first subscriber holds this prefix: forward the subscribe.
    Forward,
    /// Another subscriber already holds this prefix upstream, and this one now
    /// does too: there is nothing to forward.
    Absorb,
    /// The subscriber already holds its whole share, so this prefix is refused
    /// to it.
    OverShare,
    /// The upstream table is full of other prefixes, so this one is refused.
    TableFull,
}

/// The prefixes a forwarding device has asked its upstream for, and how many
/// subscribers hold each.
///
/// At most `capacity` distinct prefixes, so the device's own memory for it is
/// bounded by the table it stands in front of and not by its subscribers.
#[derive(Debug)]
pub(crate) struct UpstreamTable {
    capacity: usize,
    share: usize,
    /// For each prefix forwarded: how many subscribers hold it. An entry
    /// exists only while it is nonzero.
    holders: HashMap<Vec<u8>, usize>,
}

impl UpstreamTable {
    /// A table of `capacity` prefixes, which is what the upstream's table
    /// holds: the proxy's XSUB is given the same `max_subscriptions` as its
    /// XPUB, and a table that is smaller upstream refuses in silence what this
    /// one allowed.
    pub(crate) fn new(capacity: usize) -> UpstreamTable {
        UpstreamTable {
            capacity,
            share: upstream_share(capacity),
            holders: HashMap::new(),
        }
    }

    /// A subscriber that holds `held` distinct prefixes already, and not this
    /// one, asks for `prefix`.
    ///
    /// A prefix somebody else holds takes no room upstream, so only the
    /// subscriber's share limits it; a new one needs room in the table too.
    /// Nothing is recorded for a refusal.
    pub(crate) fn hold(&mut self, held: usize, prefix: &[u8]) -> Verdict {
        if held >= self.share {
            return Verdict::OverShare;
        }
        if let Some(holders) = self.holders.get_mut(prefix) {
            *holders += 1;
            return Verdict::Absorb;
        }
        if self.holders.len() >= self.capacity {
            return Verdict::TableFull;
        }
        self.holders.insert(prefix.to_vec(), 1);
        Verdict::Forward
    }

    /// A subscriber stops holding `prefix`. `true` when it was the last, so the
    /// cancel goes upstream.
    ///
    /// A prefix nobody holds is nobody's to cancel: the answer is `false`, so a
    /// cancel for something the device never asked for is not passed on.
    pub(crate) fn release(&mut self, prefix: &[u8]) -> bool {
        match self.holders.get_mut(prefix) {
            None => false,
            Some(holders) if *holders > 1 => {
                *holders -= 1;
                false
            }
            Some(_) => {
                self.holders.remove(prefix);
                true
            }
        }
    }

    /// Counts a prefix a subscriber already holds, for a table made after the
    /// subscriber's subscription was handed over.
    pub(crate) fn adopt(&mut self, prefix: &[u8]) {
        *self.holders.entry(prefix.to_vec()).or_insert(0) += 1;
    }

    /// How many subscribers may hold how many prefixes: the share.
    pub(crate) const fn share(&self) -> usize {
        self.share
    }

    /// How many distinct prefixes are forwarded.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.holders.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: **a subscriber's share is an eighth of the table**, at least
    /// [`UPSTREAM_SHARE_FLOOR`] and never more than the table: a default
    /// table of 1024 gives 128, a small one is not divided into nothing, and a
    /// table smaller than the floor gives all of itself.
    #[test]
    fn a_share_is_an_eighth_of_the_table_and_never_less_than_the_floor() {
        assert_eq!(upstream_share(1024), 128);
        assert_eq!(upstream_share(64), UPSTREAM_SHARE_FLOOR);
        assert_eq!(upstream_share(9), UPSTREAM_SHARE_FLOOR);
        assert_eq!(upstream_share(4), 4, "never more than the table");
        assert_eq!(upstream_share(0), 0);
    }

    /// Claim: **a prefix goes upstream for its first holder and its cancel for
    /// its last**, however many subscribers hold it in between: the second
    /// and third add nothing to forward, and the first two to leave take
    /// nothing back.
    #[test]
    fn a_prefix_is_forwarded_for_its_first_holder_and_cancelled_for_its_last() {
        let mut table = UpstreamTable::new(64);
        assert_eq!(table.hold(0, b"news"), Verdict::Forward);
        assert_eq!(table.hold(3, b"news"), Verdict::Absorb);
        assert_eq!(table.hold(0, b"news"), Verdict::Absorb);
        assert_eq!(table.len(), 1);

        assert!(!table.release(b"news"), "two subscribers still hold it");
        assert!(!table.release(b"news"), "one still holds it");
        assert!(table.release(b"news"), "the last one leaving cancels it");
        assert_eq!(table.len(), 0, "and it is gone");
        assert!(
            !table.release(b"news"),
            "a cancel for what nobody holds is not passed on"
        );
    }

    /// Claim: **no subscriber may have more than its share forwarded**, a
    /// prefix somebody else holds included, and a subscriber over its share
    /// takes nothing from the table.
    #[test]
    fn a_subscriber_is_held_to_its_share() {
        let mut table = UpstreamTable::new(1024);
        let share = table.share();
        assert_eq!(share, 128);
        assert_eq!(table.hold(share - 1, b"last"), Verdict::Forward);
        assert_eq!(table.hold(share, b"one too many"), Verdict::OverShare);
        assert_eq!(
            table.hold(share, b"last"),
            Verdict::OverShare,
            "a prefix another subscriber holds counts against the share all the same"
        );
        assert_eq!(table.len(), 1, "a refusal records nothing");
    }

    /// Claim: **the table stops at its capacity**, which bounds the device's own
    /// memory for it, and a prefix already in it is still welcome: it takes no
    /// room.
    #[test]
    fn the_table_stops_at_its_capacity() {
        let mut table = UpstreamTable::new(16);
        for index in 0..16u8 {
            assert_eq!(table.hold(0, &[index]), Verdict::Forward);
        }
        assert_eq!(table.hold(0, b"seventeenth"), Verdict::TableFull);
        assert_eq!(table.hold(0, &[3]), Verdict::Absorb);
        assert_eq!(table.len(), 16);

        assert!(table.release(&[0]));
        assert_eq!(
            table.hold(0, b"seventeenth"),
            Verdict::Forward,
            "room comes back when a prefix is cancelled"
        );
    }

    /// Claim: a prefix a subscriber held before the table existed is counted
    /// like any other, so its cancel is not lost.
    #[test]
    fn an_adopted_prefix_is_held_like_any_other() {
        let mut table = UpstreamTable::new(64);
        table.adopt(b"old");
        table.adopt(b"old");
        assert!(!table.release(b"old"));
        assert!(table.release(b"old"));
    }
}
