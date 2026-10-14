//! Subscriptions: the publisher-side table, its reference counts, and the two
//! wire forms.
//!
//! **Filtering is the publisher's job.** 37/ZMTP: filtering "SHALL happen at
//! the publisher side (the PUB or XPUB socket)", and "a subscription of 'A'
//! SHALL match all messages starting with 'A'. An empty subscription SHALL
//! match all messages" (`docs/research/zeromq.md` §4.3). So a publisher keeps
//! one of these per subscriber and compares each message's **first frame**
//! against it.
//!
//! **Subscriptions are additive and not idempotent**, which is a counting
//! rule and not a set rule: "subscribing to 'A' and 'A' counts as two
//! subscriptions, and would require two CANCEL commands to undo" (§2). A set
//! would lose that, so this is a multiset — and the count is exactly why the
//! table needs a ceiling, because a peer can open N of them with N commands.
//!
//! **Two wire forms exist and a widely interoperable implementation accepts
//! both.** The 3.x form is the `SUBSCRIBE`/`CANCEL` commands; the ZMTP 2.0
//! form is a one-frame *message* whose first octet is `%x01` for subscribe or
//! `%x00` for unsubscribe. Which one a 3.x peer sends "is not settled by the
//! specification", and `zeromq` 0.6 — the pure-Rust implementation the interop
//! bench uses — announces 3.0 and speaks only the 2.0 form (§4.3). They are
//! distinguishable on the wire, since a command frame carries the COMMAND
//! flag, so this library **accepts both** and lets the sent form be chosen.

use std::collections::HashMap;
use std::sync::Mutex;

/// `%x01`: the first octet of a subscribe in the message form.
pub const SUBSCRIBE_PREFIX: u8 = 1;

/// `%x00`: the first octet of an unsubscribe in the message form.
pub const CANCEL_PREFIX: u8 = 0;

/// Subscriptions one socket may hold for one peer, by default.
///
/// **Not a libzmq option.** 37/ZMTP bounds a subscription neither in length
/// nor in count, and subscriptions are non-idempotent, so "N repeated
/// `SUBSCRIBE` commands cost N entries" is a memory cost a peer chooses
/// (`docs/research/zeromq.md` §11), and `docs/INVARIANTS.md` allows no remote
/// input that causes unbounded allocation. 1024 is far above any real topic
/// set — the zguide's examples subscribe to one or a handful.
///
/// **A count alone is not a bound**: with the length unbounded, 1024 prefixes
/// of a megabyte each are a megabyte times 1024 per peer. Both dimensions
/// are capped, so the product
/// is `DEFAULT_MAX_SUBSCRIPTIONS × DEFAULT_MAX_SUBSCRIPTION_BYTES` per peer
/// — 256 KiB — times `max_peers` peers.
pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 1024;

/// Longest subscription prefix, in bytes, by default.
///
/// **Also not a libzmq option**, and 37/ZMTP's grammar is explicitly
/// unbounded here — "subscription = *OCTET" (`docs/research/zeromq.md` §11)
/// — so the number is ours to choose and to state. 256 B is
/// `docs/PROTOCOL.md` §10's filter bound, and it is far above any real topic:
/// the longest prefix in the zguide's examples is a handful of characters,
/// and weida's own filters live under the same ceiling.
pub const DEFAULT_MAX_SUBSCRIPTION_BYTES: usize = 256;

/// Which wire form a socket **sends** its subscriptions in.
///
/// Receiving accepts both, always: a publisher that read only one form would
/// silently have no subscribers from an implementation that sends the other.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SubscriptionForm {
    /// The 3.x `SUBSCRIBE`/`CANCEL` commands. The default, because this
    /// library speaks ZMTP 3.1.
    #[default]
    Commands,
    /// ZMTP 2.0's one-frame message, `%x01`/`%x00` then the prefix. What a
    /// 3.0 peer such as `zeromq` 0.6 reads.
    LegacyMessage,
}

/// One peer's subscriptions: a bounded multiset of byte prefixes.
///
/// Shared between the session that reads the peer's commands and the socket
/// that matches messages against them, so cloning the `Arc` is how the two
/// meet.
#[derive(Debug)]
pub struct Subscriptions {
    max: usize,
    max_bytes: usize,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Prefix to how many times it was subscribed. The count is the
    /// non-idempotence rule.
    counts: HashMap<Vec<u8>, usize>,
    refused: u64,
}

impl Subscriptions {
    /// An empty table holding at most `max` distinct prefixes, each at most
    /// `max_bytes` long.
    ///
    /// Both bounds are needed: a count without a length lets one peer hold
    /// `max` prefixes of any size at all.
    pub fn new(max: usize, max_bytes: usize) -> Subscriptions {
        Subscriptions {
            max,
            max_bytes,
            state: Mutex::new(State::default()),
        }
    }

    /// The longest prefix this table accepts.
    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Adds one subscription to `prefix`.
    ///
    /// Returns whether this is the **first** subscription to that prefix,
    /// which is what a deduplicating XPUB reports to its application, and
    /// `None` when the subscription was refused — either because the table
    /// is at its count ceiling or because the prefix is longer than
    /// [`Subscriptions::max_bytes`].
    pub fn subscribe(&self, prefix: &[u8]) -> Option<bool> {
        let mut state = self.lock();
        if prefix.len() > self.max_bytes {
            // A prefix's length is as much a peer's choice as its count, and
            // 37/ZMTP bounds neither.
            state.refused += 1;
            return None;
        }
        match state.counts.get_mut(prefix) {
            Some(count) => {
                *count += 1;
                Some(false)
            }
            None => {
                if state.counts.len() >= self.max {
                    state.refused += 1;
                    return None;
                }
                state.counts.insert(prefix.to_vec(), 1);
                Some(true)
            }
        }
    }

    /// Removes one subscription to `prefix`.
    ///
    /// Returns whether that was the **last** one, so the prefix is gone —
    /// two `SUBSCRIBE`s need two `CANCEL`s, and only the second changes what
    /// a publisher sends. `None` means there was no such subscription.
    pub fn cancel(&self, prefix: &[u8]) -> Option<bool> {
        let mut state = self.lock();
        let count = state.counts.get_mut(prefix)?;
        *count -= 1;
        if *count == 0 {
            state.counts.remove(prefix);
            Some(true)
        } else {
            Some(false)
        }
    }

    /// Whether `first_frame` matches any subscription.
    ///
    /// "A binary comparison of the subscription against the start of the
    /// first frame of the message", and an empty subscription matches
    /// everything. A table with nothing in it matches nothing, which is what
    /// makes a fresh SUB silent.
    pub fn matches(&self, first_frame: &[u8]) -> bool {
        self.lock()
            .counts
            .keys()
            .any(|prefix| first_frame.starts_with(prefix))
    }

    /// Distinct prefixes held.
    pub fn len(&self) -> usize {
        self.lock().counts.len()
    }

    /// Whether nothing is subscribed — a fresh SUB, which "shall filter out
    /// all incoming messages".
    pub fn is_empty(&self) -> bool {
        self.lock().counts.is_empty()
    }

    /// How many subscriptions were refused at the ceiling.
    pub fn refused(&self) -> u64 {
        self.lock().refused
    }

    /// The prefixes held, each once, in no particular order.
    ///
    /// What a socket re-sends to a publisher it has just (re)connected to:
    /// the *set*, not the counts, because the counts are the subscriber's own
    /// bookkeeping and the publisher only needs to know what to match.
    pub fn prefixes(&self) -> Vec<Vec<u8>> {
        self.lock().counts.keys().cloned().collect()
    }

    /// Every subscription, with its count, for a socket that has to unwind
    /// them — XSUB "SHOULD send unsubscribe requests for all subscriptions"
    /// when it closes a connection to a publisher.
    pub fn counted(&self) -> Vec<(Vec<u8>, usize)> {
        self.lock()
            .counts
            .iter()
            .map(|(prefix, count)| (prefix.clone(), *count))
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("subscription table poisoned")
    }
}

/// Reads the message form of a subscription: `%x01`/`%x00` and a prefix.
///
/// `None` for anything else. "Messages without a sub/unsub prefix are also
/// received, but have no effect on subscription status", which is why this
/// answers rather than refuses.
pub fn read_message_form(frame: &[u8]) -> Option<(bool, &[u8])> {
    match frame.first() {
        Some(&SUBSCRIBE_PREFIX) => Some((true, &frame[1..])),
        Some(&CANCEL_PREFIX) => Some((false, &frame[1..])),
        _ => None,
    }
}

/// Writes the message form: `%x01`/`%x00` then the prefix.
pub fn write_message_form(subscribe: bool, prefix: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(prefix.len() + 1);
    frame.push(if subscribe {
        SUBSCRIBE_PREFIX
    } else {
        CANCEL_PREFIX
    });
    frame.extend_from_slice(prefix);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the table counts rather than sets — two subscriptions to one
    /// prefix need two cancellations, and only the second changes what a
    /// publisher matches. 37/ZMTP's non-idempotence, which a `HashSet` would
    /// have lost.
    #[test]
    fn subscriptions_are_additive_and_not_idempotent() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        assert_eq!(subs.subscribe(b"topic"), Some(true), "the first is new");
        assert_eq!(subs.subscribe(b"topic"), Some(false), "the second is not");
        assert_eq!(subs.len(), 1, "one prefix, twice");

        assert_eq!(subs.cancel(b"topic"), Some(false), "still subscribed");
        assert!(subs.matches(b"topic/a"));
        assert_eq!(subs.cancel(b"topic"), Some(true), "now it is gone");
        assert!(!subs.matches(b"topic/a"));
        assert_eq!(subs.cancel(b"topic"), None, "and there is nothing left");
    }

    /// Claim: the match is a binary prefix comparison against the first
    /// frame; an empty subscription takes everything and an empty table
    /// takes nothing.
    #[test]
    fn the_match_is_a_binary_prefix() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        assert!(
            !subs.matches(b"anything"),
            "a fresh subscriber filters everything out"
        );

        subs.subscribe(b"weather.uk").expect("room");
        assert!(subs.matches(b"weather.uk.london"));
        assert!(subs.matches(b"weather.uk"));
        assert!(!subs.matches(b"weather.us"));
        // Bytes, not segments: a prefix may end mid-word, which is the loss
        // weida's own segmented filters exist to avoid.
        assert!(subs.matches(b"weather.uk-ish"));

        subs.subscribe(b"").expect("room");
        assert!(subs.matches(b"anything at all"));
        assert!(subs.matches(b""));
    }

    /// Claim: the table has a ceiling, and a peer past it is refused rather
    /// than served — N non-idempotent subscriptions cost N entries, and the
    /// count is the peer's choice.
    #[test]
    fn the_table_has_a_ceiling() {
        let subs = Subscriptions::new(2, DEFAULT_MAX_SUBSCRIPTION_BYTES);
        assert_eq!(subs.subscribe(b"a"), Some(true));
        assert_eq!(subs.subscribe(b"b"), Some(true));
        assert_eq!(subs.subscribe(b"c"), None, "past the ceiling");
        assert_eq!(subs.refused(), 1);
        assert_eq!(subs.len(), 2);

        // A repeat of one already held still counts, because it allocates
        // nothing new.
        assert_eq!(subs.subscribe(b"a"), Some(false));
        assert!(!subs.matches(b"c"));
    }

    /// Claim: **a prefix is bounded in length as well as in count.** One at
    /// the ceiling is accepted and still filters; one octet more is refused,
    /// because 37/ZMTP's `subscription = *OCTET` bounds nothing and a
    /// count-only ceiling would let one peer hold `max` prefixes of any size.
    #[test]
    fn a_prefix_is_bounded_in_length() {
        let subs = Subscriptions::new(DEFAULT_MAX_SUBSCRIPTIONS, 8);
        assert_eq!(subs.max_bytes(), 8);

        let longest = vec![b'x'; 8];
        assert_eq!(subs.subscribe(&longest), Some(true), "at the ceiling");
        let mut matching = longest.clone();
        matching.extend_from_slice(b" and more");
        assert!(
            subs.matches(&matching),
            "a prefix at the ceiling must still filter"
        );

        let over = vec![b'x'; 9];
        assert_eq!(subs.subscribe(&over), None, "one octet too many");
        assert_eq!(subs.refused(), 1);
        assert_eq!(subs.len(), 1, "and nothing was stored for it");
        assert!(!subs.matches(&[b'y'; 9]));
    }

    /// Claim: the message form round-trips, and anything else is not a
    /// subscription at all.
    #[test]
    fn the_message_form_round_trips() {
        let framed = write_message_form(true, b"topic");
        assert_eq!(framed, vec![1, b't', b'o', b'p', b'i', b'c']);
        assert_eq!(
            read_message_form(&framed),
            Some((true, b"topic".as_slice()))
        );

        let framed = write_message_form(false, b"topic");
        assert_eq!(
            read_message_form(&framed),
            Some((false, b"topic".as_slice()))
        );

        // An empty subscription is legal in both directions.
        assert_eq!(read_message_form(&[1]), Some((true, b"".as_slice())));
        // And a payload that is not a subscription says so.
        assert_eq!(read_message_form(b"hello"), None);
        assert_eq!(read_message_form(&[]), None);
    }
}
