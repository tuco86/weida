//! Consumers of one queue, and the credit each of them granted.
//!
//! A queue delivers to **one** consumer per message, which is the whole
//! difference from a publisher's fan-out, so it has to know its consumers
//! individually rather than as a set to copy to. What it keeps per
//! subscription is the absolute limit the consumer stated, the count already
//! delivered, the deliveries it holds unsettled and the handle to write with.
//!
//! **Who may take a message is a maintained set, not a question asked per
//! message.** A subscription is *ready* when it has credit left, holds fewer
//! unsettled deliveries than `max_unsettled`, belongs to a connection holding
//! fewer than `max_unsettled_per_consumer`, and has no write in flight. Every
//! transition that can change that answer goes through one function, so
//! "is anybody ready" is one comparison and the scan for the next delivery
//! walks ready subscriptions only. Counting unsettled deliveries by walking
//! the queue's table instead made every admission cost as much as everything
//! a silent consumer was holding.
//!
//! Generic over the consumer's identity and handle so that the scheduling
//! rules can be tested without a connection; the broker instantiates it with
//! [`weida::ConsumerId`] and [`weida::Consumer`].

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::hash::Hash;
use std::ops::Bound;
use std::sync::Arc;

use crate::queue::DeliveryId;

/// Grants held for a subscription that has not arrived yet.
///
/// A small number on purpose. A CREDIT and the SUBSCRIBE it belongs to ride
/// two separate unidirectional streams and are handled by two independent
/// tasks, so the window in which a grant is unmatched is the time those tasks
/// take to reach one channel — microseconds. An entry that has outlived
/// [`MAX_PENDING_GRANTS`] later grants is not waiting for a SUBSCRIBE still in
/// flight, so the oldest is evicted rather than the newest refused: refusing
/// the newest would let one peer's stale grants deny the mechanism to every
/// other consumer on the queue, which is the starvation this table exists to
/// remove.
const MAX_PENDING_GRANTS: usize = 64;

/// Which subscription: a consuming connection and the filter it subscribed
/// with.
///
/// The filter is shared, not copied: a key is made for every delivery, and a
/// `String` per delivery was one more allocation on the hot path for a value
/// that never changes while the subscription lives.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SubKey<I> {
    pub(crate) id: I,
    pub(crate) filter: Arc<str>,
}

/// One subscription: a consumer, its filter, and its credit.
struct Subscription<H> {
    handle: Arc<H>,
    /// Highest absolute limit this subscription has granted.
    ///
    /// Monotone on purpose: a credit frame rides its own unidirectional
    /// stream, and QUIC orders no stream against another, so a grant that
    /// arrives late would otherwise lower a limit the consumer has already
    /// raised. Keeping the maximum is what makes a duplicated or reordered
    /// grant change nothing
    /// ([0003](https://github.com/tuco86/weida/blob/main/docs/decisions/0003-credit-unit.md)
    /// §4.3).
    limit: u64,
    /// Messages delivered on this subscription since it was created.
    delivered: u64,
    /// Deliveries handed to this subscription and not settled, **including
    /// the one being written**.
    unsettled: usize,
    /// The delivery being written to this consumer right now, if any.
    ///
    /// One at a time: a consumer that stops reading stalls its own write and
    /// that write only, instead of every message the queue could have given
    /// it piling up behind a stream that has no credit. The write names its
    /// delivery so that only *that* delivery's outcome frees the slot; an
    /// outcome that arrives for another one cannot strand the subscription.
    writing: Option<DeliveryId>,
}

impl<H> Subscription<H> {
    /// Raises the standing limit, and says whether it moved.
    ///
    /// The one place a limit is written, so the monotone rule holds for a
    /// grant that arrived early exactly as it does for one that arrived in
    /// order.
    fn raise(&mut self, limit: u64) -> bool {
        if limit <= self.limit {
            return false;
        }
        self.limit = limit;
        true
    }
}

/// Everything one connection subscribed to on this queue.
struct Connection<H> {
    subs: HashMap<Arc<str>, Subscription<H>>,
    /// Unsettled deliveries over all of this connection's subscriptions.
    ///
    /// The bound per subscription alone is a bound times the number of
    /// filters, and the filters are the peer's: one connection holding 256 of
    /// them could pin more than a whole queue.
    unsettled: usize,
}

impl<H> Default for Connection<H> {
    fn default() -> Self {
        Connection {
            subs: HashMap::new(),
            unsettled: 0,
        }
    }
}

/// A grant that arrived before the subscription it names.
struct Pending<I> {
    id: I,
    filter: String,
    limit: u64,
}

/// May this subscription be given a message right now?
fn eligible<H>(
    sub: &Subscription<H>,
    connection_unsettled: usize,
    max_unsettled: usize,
    max_unsettled_per_consumer: usize,
) -> bool {
    sub.delivered < sub.limit
        && sub.writing.is_none()
        && sub.unsettled < max_unsettled
        && connection_unsettled < max_unsettled_per_consumer
}

/// Every subscription on one queue.
///
/// Bounded by what the runtime already bounds: `max_connections` connections,
/// each holding at most `max_subscriptions` filters
/// (`docs/PROTOCOL.md` §10), and a closed connection takes its entries with
/// it.
pub(crate) struct Consumers<I, H> {
    /// Nested so the inner lookup borrows a `&str`: a grant and a stats read
    /// all arrive with a borrowed filter and none of them needs to own it.
    /// The outer key is a connection, so dropping a whole consumer — the
    /// common case, a connection closing — is one removal rather than a pass
    /// over every entry.
    conns: HashMap<I, Connection<H>>,
    /// The subscriptions that may take a message now, in key order.
    ///
    /// Ordered so that round-robin is "the first key after the one served
    /// last", found in one walk, instead of building and sorting a list of
    /// candidates for every message.
    ready: BTreeSet<SubKey<I>>,
    /// Set when a subscription *entered* `ready`: the one event after which a
    /// message nobody wanted a moment ago may have a taker.
    gained: bool,
    /// Grants whose SUBSCRIBE has not arrived yet, newest last.
    ///
    /// `weida::Subscriber::grant` is a one-shot application call and nothing
    /// restates it, so a grant that wins the race against its SUBSCRIBE would
    /// leave its consumer receiving nothing, forever, with no error on either
    /// side. It is held here, and [`Consumers::subscribe`] applies it.
    ///
    /// A deque rather than a map, and capped at [`MAX_PENDING_GRANTS`]: the
    /// peer chooses both halves of the key, and a connection that only ever
    /// grants credit registers no consumer route, so it never produces an
    /// `Unsubscribed` to clean up after — the cap is the only bound there is.
    /// At that length a scan is cheaper than a hash.
    pending: VecDeque<Pending<I>>,
    /// The subscription served last, so two consumers with credit share a
    /// queue's messages instead of one of them taking all of them.
    last: Option<SubKey<I>>,
    max_unsettled: usize,
    max_unsettled_per_consumer: usize,
}

impl<I: Copy + Ord + Hash, H> Consumers<I, H> {
    /// No subscriptions, under the two unsettled bounds.
    pub(crate) fn new(max_unsettled: usize, max_unsettled_per_consumer: usize) -> Self {
        Consumers {
            conns: HashMap::new(),
            ready: BTreeSet::new(),
            gained: false,
            pending: VecDeque::new(),
            last: None,
            max_unsettled,
            max_unsettled_per_consumer,
        }
    }

    /// Records a new subscription with **zero** credit, unless a grant for it
    /// arrived first.
    ///
    /// Zero is the initial value the whole scheme rests on: a consumer that
    /// subscribes and grants nothing receives nothing, which is the only
    /// default that cannot surprise it with a flood (AMQP 1.0's initial link
    /// credit, [0018](https://github.com/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
    /// §4.4). A repeated SUBSCRIBE for the same filter keeps the credit and
    /// the delivered count: it is idempotent, and forgetting the count would
    /// hand a consumer free credit by re-subscribing.
    ///
    /// Returns whether a grant that overtook this SUBSCRIBE was applied, so
    /// the caller can say so; the delivery scan runs after every event
    /// either way.
    pub(crate) fn subscribe(&mut self, id: I, filter: Arc<str>, handle: H) -> bool {
        let held = self.unhold(id, &filter);
        let conn = self.conns.entry(id).or_default();
        let sub = conn
            .subs
            .entry(Arc::clone(&filter))
            .or_insert_with(|| Subscription {
                handle: Arc::new(handle),
                limit: 0,
                delivered: 0,
                unsettled: 0,
                writing: None,
            });
        let raised = held.is_some_and(|limit| sub.raise(limit));
        self.refresh(id, &filter);
        raised
    }

    /// Applies a grant, or holds it until its subscription arrives.
    ///
    /// The limit is absolute, cumulative and monotone: the highest one seen
    /// wins, so a duplicated or reordered grant changes nothing — and
    /// restating the count already delivered changes nothing either, since it
    /// is not above the standing limit. The only pause in v0 is the `0` a
    /// fresh subscription starts at.
    pub(crate) fn grant(&mut self, id: I, filter: &str, limit: u64) {
        match self.lookup(id, filter) {
            Some(sub) => {
                if sub.raise(limit) {
                    self.refresh(id, filter);
                }
            }
            None => self.hold(id, filter, limit),
        }
    }

    /// Removes one subscription, or every subscription of one connection when
    /// `filter` is `None`, and the grants held for them.
    ///
    /// What the subscription's unsettled deliveries become is the queue's
    /// concern and happens in the same breath
    /// (`Queue::requeue_all`): their counters go with the subscription, so a
    /// late outcome for one of them finds nothing here to decrement.
    pub(crate) fn remove(&mut self, id: I, filter: Option<&str>) {
        match filter {
            Some(filter) => {
                self.unhold(id, filter);
                let Some(conn) = self.conns.get_mut(&id) else {
                    return;
                };
                let Some((name, sub)) = conn.subs.remove_entry(filter) else {
                    return;
                };
                let was_capped = conn.unsettled >= self.max_unsettled_per_consumer;
                conn.unsettled = conn.unsettled.saturating_sub(sub.unsettled);
                let capped = conn.unsettled >= self.max_unsettled_per_consumer;
                let emptied = conn.subs.is_empty();
                self.ready.remove(&SubKey { id, filter: name });
                if emptied {
                    self.conns.remove(&id);
                } else if was_capped && !capped {
                    // The connection is under its bound again: its other
                    // subscriptions may be ready.
                    self.refresh_conn(id);
                }
            }
            None => {
                if let Some(conn) = self.conns.remove(&id) {
                    for name in conn.subs.into_keys() {
                        self.ready.remove(&SubKey { id, filter: name });
                    }
                }
                self.pending.retain(|held| held.id != id);
            }
        }
    }

    /// Drops the subscription a failed delivery belongs to.
    ///
    /// A consumer whose write failed or never made progress is gone or
    /// unresponsive, and offering it the next message would park that message
    /// behind the same wait. It is not told: the broker has no way to speak to
    /// a subscriber outside a delivery, and one that is still there resubscribes
    /// like any consumer that was removed.
    pub(crate) fn fail(&mut self, key: &SubKey<I>) {
        self.remove(key.id, Some(&key.filter));
    }

    /// Is any subscription ready to take a message?
    pub(crate) fn any_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Says whether a subscription became ready since the last call, and
    /// forgets it.
    pub(crate) fn take_gained(&mut self) -> bool {
        std::mem::take(&mut self.gained)
    }

    /// The ready subscription that should take a message on `topic`, or
    /// `None` when no ready subscription's filter selects it.
    ///
    /// Round-robin over the ready subscriptions whose filter matches, so two
    /// consumers of one queue share its messages: the first matching key
    /// after the one served last, wrapping around. Nothing is counted yet —
    /// [`Consumers::dispatch`] does that once the queue has agreed to give a
    /// message up.
    pub(crate) fn pick(&self, topic: &str) -> Option<SubKey<I>> {
        let selects = |key: &&SubKey<I>| weida::filter::matches(topic, &key.filter);
        let found = match &self.last {
            Some(last) => self
                .ready
                .range::<SubKey<I>, _>((Bound::Excluded(last), Bound::Unbounded))
                .find(&selects)
                .or_else(|| {
                    self.ready
                        .range::<SubKey<I>, _>((Bound::Unbounded, Bound::Included(last)))
                        .find(&selects)
                }),
            None => self.ready.iter().find(&selects),
        };
        found.cloned()
    }

    /// Counts a delivery against a subscription and returns the handle to
    /// write it with.
    ///
    /// The credit is spent, the delivery is unsettled from now and a write is
    /// in flight, so the subscription leaves the ready set until that write
    /// ends.
    pub(crate) fn dispatch(&mut self, key: &SubKey<I>, delivery: DeliveryId) -> Option<Arc<H>> {
        let conn = self.conns.get_mut(&key.id)?;
        let sub = conn.subs.get_mut(&*key.filter)?;
        sub.delivered += 1;
        sub.unsettled += 1;
        sub.writing = Some(delivery);
        conn.unsettled += 1;
        let handle = Arc::clone(&sub.handle);
        let capped = conn.unsettled >= self.max_unsettled_per_consumer;
        self.last = Some(key.clone());
        if capped {
            self.refresh_conn(key.id);
        } else {
            self.refresh(key.id, &key.filter);
        }
        Some(handle)
    }

    /// A write ended and its bytes are out: the subscription may be given the
    /// next message while this one waits for its report.
    pub(crate) fn written(&mut self, key: &SubKey<I>, delivery: DeliveryId) {
        if let Some(sub) = self.lookup(key.id, &key.filter)
            && sub.writing == Some(delivery)
        {
            sub.writing = None;
            self.refresh(key.id, &key.filter);
        }
    }

    /// A delivery ended — settled, or given back — and its slot is free.
    pub(crate) fn released(&mut self, key: &SubKey<I>, delivery: DeliveryId) {
        let Some(conn) = self.conns.get_mut(&key.id) else {
            return;
        };
        let Some(sub) = conn.subs.get_mut(&*key.filter) else {
            return;
        };
        sub.unsettled = sub.unsettled.saturating_sub(1);
        if sub.writing == Some(delivery) {
            sub.writing = None;
        }
        let was_capped = conn.unsettled >= self.max_unsettled_per_consumer;
        conn.unsettled = conn.unsettled.saturating_sub(1);
        let capped = conn.unsettled >= self.max_unsettled_per_consumer;
        if was_capped && !capped {
            self.refresh_conn(key.id);
        } else {
            self.refresh(key.id, &key.filter);
        }
    }

    /// How many subscriptions this queue serves.
    pub(crate) fn len(&self) -> usize {
        self.conns.values().map(|conn| conn.subs.len()).sum()
    }

    /// Deliveries counted on one subscription, for the broker's own stats.
    pub(crate) fn delivered(&self, id: I, filter: &str) -> Option<u64> {
        self.conns
            .get(&id)?
            .subs
            .get(filter)
            .map(|sub| sub.delivered)
    }

    /// The subscription one key names, if it exists.
    fn lookup(&mut self, id: I, filter: &str) -> Option<&mut Subscription<H>> {
        self.conns.get_mut(&id)?.subs.get_mut(filter)
    }

    /// Re-derives whether one subscription is ready.
    ///
    /// The only way into or out of `ready` besides removal, so the set cannot
    /// disagree with the counters it is derived from.
    fn refresh(&mut self, id: I, filter: &str) {
        let Some(conn) = self.conns.get(&id) else {
            return;
        };
        let Some((name, sub)) = conn.subs.get_key_value(filter) else {
            return;
        };
        let ready = eligible(
            sub,
            conn.unsettled,
            self.max_unsettled,
            self.max_unsettled_per_consumer,
        );
        let key = SubKey {
            id,
            filter: Arc::clone(name),
        };
        if ready {
            self.gained |= self.ready.insert(key);
        } else {
            self.ready.remove(&key);
        }
    }

    /// [`Consumers::refresh`] for every subscription of one connection: what
    /// a change to the connection's own count can move.
    fn refresh_conn(&mut self, id: I) {
        let Some(conn) = self.conns.get(&id) else {
            return;
        };
        for (name, sub) in &conn.subs {
            let ready = eligible(
                sub,
                conn.unsettled,
                self.max_unsettled,
                self.max_unsettled_per_consumer,
            );
            let key = SubKey {
                id,
                filter: Arc::clone(name),
            };
            if ready {
                self.gained |= self.ready.insert(key);
            } else {
                self.ready.remove(&key);
            }
        }
    }

    /// Holds a grant for a subscription that has not arrived, keeping the
    /// highest limit per key and the newest entries at the cap.
    fn hold(&mut self, id: I, filter: &str, limit: u64) {
        if let Some(at) = self.find_held(id, filter) {
            let held = &mut self.pending[at];
            held.limit = held.limit.max(limit);
            return;
        }
        if self.pending.len() >= MAX_PENDING_GRANTS {
            let evicted = self.pending.pop_front().expect("the deque is at its cap");
            tracing::debug!(
                filter = %evicted.filter,
                "evicting the oldest unmatched grant: its subscribe never came"
            );
        }
        tracing::debug!(
            %filter,
            limit,
            "holding a grant whose subscribe has not arrived yet"
        );
        self.pending.push_back(Pending {
            id,
            filter: filter.to_owned(),
            limit,
        });
    }

    /// Where the grant held for one key sits, if one is held.
    fn find_held(&self, id: I, filter: &str) -> Option<usize> {
        let same = |held: &Pending<I>| held.id == id && held.filter == filter;
        self.pending.iter().position(same)
    }

    /// Takes the grant held for one key, if there is one.
    fn unhold(&mut self, id: I, filter: &str) -> Option<u64> {
        let at = self.find_held(id, filter)?;
        self.pending.remove(at).map(|held| held.limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Table = Consumers<u32, ()>;

    fn key(id: u32, filter: &str) -> SubKey<u32> {
        SubKey {
            id,
            filter: Arc::from(filter),
        }
    }

    fn table(max_unsettled: usize, per_consumer: usize) -> Table {
        Consumers::new(max_unsettled, per_consumer)
    }

    fn subscribe(table: &mut Table, id: u32, filter: &str) {
        table.subscribe(id, Arc::from(filter), ());
    }

    /// One whole delivery: picked, dispatched and written.
    fn deliver(table: &mut Table, topic: &str, delivery: u64) -> Option<SubKey<u32>> {
        let key = table.pick(topic)?;
        table.dispatch(&key, DeliveryId(delivery))?;
        table.written(&key, DeliveryId(delivery));
        Some(key)
    }

    #[test]
    fn a_subscription_with_no_credit_is_not_ready() {
        let mut table = table(4, 8);
        subscribe(&mut table, 1, "");
        assert!(!table.any_ready());
        assert_eq!(table.pick("t"), None);

        table.grant(1, "", 1);
        assert!(table.any_ready());
        assert_eq!(table.pick("t"), Some(key(1, "")));
    }

    #[test]
    fn credit_is_absolute_and_a_lower_grant_changes_nothing() {
        let mut table = table(4, 8);
        subscribe(&mut table, 1, "");
        table.grant(1, "", 2);
        table.grant(1, "", 1);
        assert!(deliver(&mut table, "t", 1).is_some());
        assert!(deliver(&mut table, "t", 2).is_some());
        assert!(deliver(&mut table, "t", 3).is_none(), "the limit is two");

        // Restating what was delivered is not above the limit either.
        table.grant(1, "", 2);
        assert!(!table.any_ready());
        table.grant(1, "", 3);
        assert!(table.any_ready());
    }

    #[test]
    fn a_grant_that_overtakes_its_subscribe_is_held_and_applied() {
        let mut table = table(4, 8);
        table.grant(1, "q", 5);
        assert_eq!(table.len(), 0, "a held grant is not a subscription");
        assert!(!table.any_ready());

        assert!(table.subscribe(1, Arc::from("q"), ()), "the grant applied");
        assert!(table.any_ready());
        assert_eq!(table.pick("q"), Some(key(1, "q")));
    }

    #[test]
    fn held_grants_are_capped_and_the_oldest_is_evicted() {
        let mut table = table(4, 8);
        for id in 0..(MAX_PENDING_GRANTS as u32 + 1) {
            table.grant(id, "", 1);
        }
        assert!(
            !table.subscribe(0, Arc::from(""), ()),
            "the oldest grant went to make room"
        );
        assert!(table.subscribe(1, Arc::from(""), ()));
    }

    #[test]
    fn a_dispatch_spends_credit_and_holds_the_subscription_until_the_write_ends() {
        let mut table = table(4, 8);
        subscribe(&mut table, 1, "");
        table.grant(1, "", 10);

        let picked = table.pick("t").expect("ready");
        table.dispatch(&picked, DeliveryId(7)).expect("subscribed");
        assert_eq!(table.delivered(1, ""), Some(1));
        assert!(
            !table.any_ready(),
            "a write is in flight: the next message waits, it is not parked \
             behind the same stream"
        );

        // Another delivery's write does not free the slot.
        table.written(&picked, DeliveryId(8));
        assert!(!table.any_ready());

        table.written(&picked, DeliveryId(7));
        assert!(table.any_ready());
    }

    #[test]
    fn a_settled_delivery_frees_its_slot_even_if_the_write_report_never_came() {
        let mut table = table(4, 8);
        subscribe(&mut table, 1, "");
        table.grant(1, "", 10);
        let picked = table.pick("t").expect("ready");
        table.dispatch(&picked, DeliveryId(1)).expect("subscribed");

        table.released(&picked, DeliveryId(1));
        assert!(
            table.any_ready(),
            "the outcome for the write ends the write"
        );
    }

    #[test]
    fn the_unsettled_bound_is_per_subscription_and_a_settlement_lifts_it() {
        let mut table = table(2, 100);
        subscribe(&mut table, 1, "a");
        subscribe(&mut table, 1, "b");
        table.grant(1, "a", 100);
        table.grant(1, "b", 100);

        assert_eq!(deliver(&mut table, "a", 1), Some(key(1, "a")));
        assert_eq!(deliver(&mut table, "a", 2), Some(key(1, "a")));
        assert_eq!(
            deliver(&mut table, "a", 3),
            None,
            "two outstanding on `a` against a bound of two"
        );
        assert_eq!(
            deliver(&mut table, "b", 4),
            Some(key(1, "b")),
            "one silent subscription does not stop its neighbour"
        );

        table.released(&key(1, "a"), DeliveryId(1));
        assert_eq!(deliver(&mut table, "a", 5), Some(key(1, "a")));
    }

    #[test]
    fn the_per_connection_bound_spans_every_filter_the_connection_holds() {
        let mut table = table(100, 3);
        for filter in ["a", "b", "c", "d"] {
            subscribe(&mut table, 1, filter);
            table.grant(1, filter, 100);
        }
        assert_eq!(deliver(&mut table, "a", 1), Some(key(1, "a")));
        assert_eq!(deliver(&mut table, "a", 2), Some(key(1, "a")));
        assert_eq!(deliver(&mut table, "a", 3), Some(key(1, "a")));
        assert!(
            !table.any_ready(),
            "three outstanding across the connection's filters is its bound"
        );
        assert_eq!(
            table.pick("b"),
            None,
            "a fourth filter of the same connection is not given the message"
        );

        // Another connection is not charged for it.
        subscribe(&mut table, 2, "b");
        table.grant(2, "b", 100);
        assert_eq!(table.pick("b"), Some(key(2, "b")));

        // One settlement anywhere on the connection lifts the bound for all of
        // its filters.
        table.released(&key(1, "a"), DeliveryId(1));
        assert_eq!(table.pick("c"), Some(key(1, "c")));
    }

    #[test]
    fn two_ready_subscriptions_share_the_messages() {
        let mut table = table(100, 100);
        for id in [1, 2, 3] {
            subscribe(&mut table, id, "");
            table.grant(id, "", 100);
        }
        let served: Vec<u32> = (1..=6)
            .map(|delivery| deliver(&mut table, "t", delivery).expect("ready").id)
            .collect();
        assert_eq!(served, [1, 2, 3, 1, 2, 3], "each takes its turn");
    }

    #[test]
    fn a_filter_selects_what_a_subscription_may_take() {
        let mut table = table(100, 100);
        subscribe(&mut table, 1, "px.eur");
        subscribe(&mut table, 2, "px.*");
        table.grant(1, "px.eur", 100);
        table.grant(2, "px.*", 100);

        assert_eq!(table.pick("px.usd"), Some(key(2, "px.*")));
        assert_eq!(table.pick("other"), None);
    }

    #[test]
    fn removing_a_subscription_takes_its_readiness_and_its_held_grants() {
        let mut table = table(100, 100);
        subscribe(&mut table, 1, "a");
        subscribe(&mut table, 1, "b");
        table.grant(1, "a", 1);
        table.grant(1, "b", 1);
        table.grant(1, "never", 1);

        table.remove(1, Some("a"));
        assert_eq!(table.len(), 1);
        assert_eq!(table.pick("a"), None);
        assert!(table.pick("b").is_some());

        table.remove(1, None);
        assert_eq!(table.len(), 0);
        assert!(!table.any_ready());
        assert!(
            !table.subscribe(1, Arc::from("never"), ()),
            "the grant held for the connection went with it"
        );
    }

    #[test]
    fn a_late_outcome_for_a_subscription_that_is_gone_is_ignored() {
        // The queue only reports an outcome whose delivery it still holds, and
        // it takes a removed subscription's deliveries back when it is
        // removed, so this is the table's own guard: a call that names a
        // subscription that is not there finds nothing to change.
        let mut table = table(1, 1);
        subscribe(&mut table, 1, "a");
        table.grant(1, "a", 10);
        let first = table.pick("a").expect("ready");
        table.dispatch(&first, DeliveryId(1)).expect("subscribed");
        table.remove(1, Some("a"));

        table.released(&first, DeliveryId(1));
        table.written(&first, DeliveryId(1));
        assert_eq!(table.len(), 0);
        assert!(!table.any_ready());
        assert_eq!(table.delivered(1, "a"), None);
    }

    #[test]
    fn failing_a_delivery_removes_the_subscription_and_lifts_its_connections_bound() {
        let mut table = table(100, 1);
        subscribe(&mut table, 1, "a");
        subscribe(&mut table, 1, "b");
        table.grant(1, "a", 10);
        table.grant(1, "b", 10);
        let first = table.pick("a").expect("ready");
        table.dispatch(&first, DeliveryId(1)).expect("subscribed");
        assert_eq!(table.pick("b"), None, "the connection is at its bound");

        table.fail(&first);
        assert_eq!(table.len(), 1);
        assert!(
            table.pick("b").is_some(),
            "the bound went with the delivery"
        );
    }

    #[test]
    fn only_a_subscription_entering_the_ready_set_counts_as_a_gain() {
        let mut table = table(100, 100);
        subscribe(&mut table, 1, "");
        assert!(!table.take_gained(), "no credit, nothing became ready");

        table.grant(1, "", 5);
        assert!(table.take_gained());
        assert!(!table.take_gained(), "reading it clears it");

        table.grant(1, "", 6);
        assert!(
            !table.take_gained(),
            "more credit on a subscription that was ready already"
        );

        let picked = table.pick("t").expect("ready");
        table.dispatch(&picked, DeliveryId(1)).expect("subscribed");
        assert!(!table.take_gained(), "leaving the set is not a gain");
        table.written(&picked, DeliveryId(1));
        assert!(table.take_gained(), "coming back is");
    }

    #[test]
    fn a_zero_bound_never_delivers() {
        let mut table = table(0, 100);
        subscribe(&mut table, 1, "");
        table.grant(1, "", 5);
        assert!(!table.any_ready());
    }
}
