//! Consumers of one queue, and the credit each of them granted.
//!
//! A queue delivers to **one** consumer per message, which is the whole
//! difference from a publisher's fan-out, so it has to know its consumers
//! individually rather than as a set to copy to. What it keeps per
//! subscription is two numbers — the absolute limit the consumer stated and
//! the count already delivered — and the handle to write with.

use std::collections::{HashMap, VecDeque};

use weida::{Consumer, ConsumerId};

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

/// One subscription: a consumer, its filter, and its credit.
struct Subscription {
    consumer: Consumer,
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
}

impl Subscription {
    /// Is there room under the limit for one more delivery?
    fn has_credit(&self) -> bool {
        self.delivered < self.limit
    }

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

/// A grant that arrived before the subscription it names.
struct Pending {
    id: ConsumerId,
    filter: String,
    limit: u64,
}

/// Every subscription on one queue.
///
/// Bounded by what the runtime already bounds: `max_connections` connections,
/// each holding at most `max_subscriptions` filters
/// (`docs/PROTOCOL.md` §10), and a closed connection takes its entries with
/// it.
#[derive(Default)]
pub(crate) struct Consumers {
    /// Nested so the inner lookup borrows a `&str`: a grant, an undo and a
    /// stats read all arrive with a borrowed filter and none of them needs to
    /// own it. The outer key is a connection, so dropping a whole consumer —
    /// the common case, a connection closing — is one removal rather than a
    /// pass over every entry.
    subs: HashMap<ConsumerId, HashMap<String, Subscription>>,
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
    pending: VecDeque<Pending>,
    /// Where the next round-robin scan starts, so two consumers with credit
    /// share a queue's messages instead of one of them taking all of them.
    ///
    /// A counter rather than an index into the map: the map's iteration order
    /// is arbitrary but stable between mutations, and all this has to do is
    /// not pick the same entry every time.
    cursor: usize,
}

impl Consumers {
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
    /// Returns whether this subscription now has credit it did not have a
    /// moment ago, which happens exactly when a grant overtook its own
    /// SUBSCRIBE. The caller pumps on `true`: an ordinary fresh subscription
    /// has nothing it may take, but one that arrives with a limit already
    /// standing does, and no later event would notice.
    pub(crate) fn subscribe(&mut self, consumer: Consumer) -> bool {
        let id = consumer.id();
        let filter = consumer.filter().to_owned();
        let held = self.unhold(id, &filter);
        let fresh = Subscription {
            consumer,
            limit: 0,
            delivered: 0,
        };
        let subs = self.subs.entry(id).or_default();
        let sub = subs.entry(filter).or_insert(fresh);
        match held {
            Some(limit) => sub.raise(limit),
            None => false,
        }
    }

    /// Applies a grant, or holds it until its subscription arrives.
    ///
    /// The limit is absolute, cumulative and monotone: the highest one seen
    /// wins, so a duplicated or reordered grant changes nothing — and
    /// restating the count already delivered changes nothing either, since it
    /// is not above the standing limit. The only pause in v0 is the `0` a
    /// fresh subscription starts at.
    ///
    /// Returns whether anything changed, so a caller can skip a delivery scan
    /// that cannot succeed. A held grant returns `false`: there is no
    /// subscription to deliver on yet, and [`Consumers::subscribe`] is what
    /// reports the credit when one appears.
    pub(crate) fn grant(&mut self, id: ConsumerId, filter: &str, limit: u64) -> bool {
        if let Some(sub) = self.lookup(id, filter) {
            return sub.raise(limit);
        }
        self.hold(id, filter, limit);
        false
    }

    /// Removes one subscription, or every subscription of one connection when
    /// `filter` is `None`, and the grants held for them.
    pub(crate) fn remove(&mut self, id: ConsumerId, filter: Option<&str>) {
        match filter {
            Some(filter) => {
                let emptied = match self.subs.get_mut(&id) {
                    Some(subs) => {
                        subs.remove(filter);
                        subs.is_empty()
                    }
                    None => false,
                };
                if emptied {
                    self.subs.remove(&id);
                }
                self.unhold(id, filter);
            }
            None => {
                self.subs.remove(&id);
                self.pending.retain(|held| held.id != id);
            }
        }
    }

    /// Does any subscription have credit at all?
    ///
    /// The cheap half of a delivery scan: one pass over the subscriptions,
    /// which `max_subscriptions` bounds, instead of a pass over the queue for
    /// every message it holds — and the length of the queue is the producer's
    /// choice. A queue nobody has granted credit on is the case a filling
    /// queue is in, and this is what keeps filling it linear.
    pub(crate) fn any_credit(&self) -> bool {
        self.subs
            .values()
            .any(|subs| subs.values().any(Subscription::has_credit))
    }

    /// Picks the next subscription that may take a message on `topic`, and
    /// counts the delivery against its credit.
    ///
    /// Round-robin over the subscriptions with credit whose filter matches, so
    /// two consumers of one queue share its messages. `skip` names the
    /// subscriptions this round has already failed to write to, so one dead
    /// consumer does not swallow the messages the others could have had.
    /// Returns the handle to deliver with; [`Consumers::undo`] gives the
    /// credit back if the write fails.
    ///
    /// `has_room` is asked of every candidate that passes the rest, and it is
    /// how `max_unsettled` reaches this decision without this module knowing
    /// what an unsettled delivery is: the queue owns that table, so the queue
    /// answers the question. A subscription that fails it is not eligible
    /// *this round* and is not otherwise penalised — one settlement makes it
    /// eligible again with no bookkeeping here.
    pub(crate) fn take_turn(
        &mut self,
        topic: &str,
        skip: &[(ConsumerId, String)],
        has_room: &dyn Fn(ConsumerId, &str) -> bool,
    ) -> Option<(ConsumerId, String, Consumer)> {
        let mut eligible: Vec<(ConsumerId, &str)> = self
            .subs
            .iter()
            .flat_map(|(id, subs)| subs.iter().map(move |(filter, sub)| (*id, filter, sub)))
            .filter(|&(id, filter, sub)| {
                sub.has_credit()
                    && !skipped(skip, id, filter)
                    && weida::filter::matches(topic, filter)
                    && has_room(id, filter)
            })
            .map(|(id, filter, _)| (id, filter.as_str()))
            .collect();
        if eligible.is_empty() {
            return None;
        }
        // Sorted so the cursor means the same thing between calls: the map's
        // own order is unspecified and changes as entries are added.
        eligible.sort_unstable();
        let (id, filter) = eligible[self.cursor % eligible.len()];
        let filter = filter.to_owned();
        self.cursor = self.cursor.wrapping_add(1);
        let sub = self.lookup(id, &filter).expect("key came from the map");
        sub.delivered += 1;
        Some((id, filter, sub.consumer.clone()))
    }

    /// Gives one counted delivery back, for a write that never landed.
    pub(crate) fn undo(&mut self, id: ConsumerId, filter: &str) {
        if let Some(sub) = self.lookup(id, filter) {
            sub.delivered = sub.delivered.saturating_sub(1);
        }
    }

    /// How many subscriptions this queue serves.
    pub(crate) fn len(&self) -> usize {
        self.subs.values().map(|subs| subs.len()).sum()
    }

    /// Deliveries counted on one subscription, for the broker's own stats.
    pub(crate) fn delivered(&self, id: ConsumerId, filter: &str) -> Option<u64> {
        self.subs.get(&id)?.get(filter).map(|sub| sub.delivered)
    }

    /// The subscription one key names, if it exists.
    fn lookup(&mut self, id: ConsumerId, filter: &str) -> Option<&mut Subscription> {
        self.subs.get_mut(&id)?.get_mut(filter)
    }

    /// Holds a grant for a subscription that has not arrived, keeping the
    /// highest limit per key and the newest entries at the cap.
    fn hold(&mut self, id: ConsumerId, filter: &str, limit: u64) {
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
    fn find_held(&self, id: ConsumerId, filter: &str) -> Option<usize> {
        let same = |held: &Pending| held.id == id && held.filter == filter;
        self.pending.iter().position(same)
    }

    /// Takes the grant held for one key, if there is one.
    fn unhold(&mut self, id: ConsumerId, filter: &str) -> Option<u64> {
        let at = self.find_held(id, filter)?;
        self.pending.remove(at).map(|held| held.limit)
    }
}

/// Is this subscription excluded from the current delivery round?
fn skipped(skip: &[(ConsumerId, String)], id: ConsumerId, filter: &str) -> bool {
    skip.iter()
        .any(|(held, held_filter)| *held == id && held_filter == filter)
}
