//! Bounded deduplication: `Deduplication::Bounded(window)`.
//!
//! A receiver remembers the identity of what it has seen for a while and
//! suppresses a repeat of it. The identity is the tuple
//! `(producer, path, topic, sequence)` — the producer being the connection's
//! proved fingerprint unless DATA key `7` names another one
//! ([decision 0008](../../../docs/decisions/0008-session-identity.md) §4.4) —
//! and "for a while" is the negotiated window
//! ([decision 0001](../../../docs/decisions/0001-sequence-field.md) §7.6).
//!
//! Two bounds, not one. The window bounds *how long* an identity is kept; it
//! says nothing about how many arrive inside it, so the table is additionally
//! capped in count and evicts its oldest entry at the cap — the bound
//! `docs/INVARIANTS.md` names for exactly this structure. And like the
//! sequencer beside it, the whole thing is inert when deduplication is off:
//! no entry is recorded and no table is allocated.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use weida_protocol::header::Deduplication;

use crate::ordering::{Probe, ScopeKey, ScopeRef};

/// What tells two transfers of one scope apart: the producer, and the
/// sequence number.
///
/// `producer` is `None` in the ordinary case, where the producer *is* the
/// connection: the fingerprint is proved by the handshake and identical for
/// every transfer here, so keeping it out of the key costs 32 bytes less per
/// entry and changes nothing.
///
/// The scope is kept beside the key, not inside it. Lookups borrow the
/// caller's path and topic through a [`Probe`], so a transfer whose scope is
/// already tracked allocates nothing, and a miss allocates only when the scope
/// itself is new.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Key {
    producer: Option<[u8; 32]>,
    sequence: u64,
}

/// Remembers recent identities and reports repeats.
pub(crate) struct DedupWindow {
    window: Option<Duration>,
    max_entries: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Insertion order, for expiry and for eviction at the count cap. Each
    /// entry names its scope by the key the table holds, so it costs no name
    /// of its own.
    order: VecDeque<(Instant, ScopeKey, Key)>,
    /// Present identities, per scope. A map rather than a set so that the
    /// entry carries its deadline and expiry needs no second lookup.
    seen: HashMap<ScopeKey, HashMap<Key, Instant>>,
}

impl DedupWindow {
    /// Builds a window from the negotiated level. `Bounded` without a window
    /// is not representable on the wire (`docs/PROTOCOL.md` §6.5), so a
    /// missing window here means the level is off.
    pub(crate) fn new(
        level: Deduplication,
        window_ms: Option<u64>,
        max_entries: usize,
    ) -> DedupWindow {
        let window = match (level, window_ms) {
            (Deduplication::Bounded, Some(ms)) => Some(Duration::from_millis(ms)),
            _ => None,
        };
        DedupWindow {
            window,
            max_entries,
            state: Mutex::new(State::default()),
        }
    }

    /// Is this transfer a repeat of one already seen inside the window?
    ///
    /// The scope is `(path, topic)`: a topic is a scope of its own under its
    /// path, as it is for gap detection. Records the transfer when it is not
    /// a repeat. An identity reused after its window has passed is **not**
    /// suppressed — that is what "bounded" means, and it is the JetStream
    /// shape decision 0001 §7.6 took.
    pub(crate) fn is_duplicate(
        &self,
        producer: Option<[u8; 32]>,
        path: &str,
        topic: Option<&str>,
        sequence: Option<u64>,
        now: Instant,
    ) -> bool {
        let Some(window) = self.window else {
            return false;
        };
        // Nothing names this transfer, so nothing can be deduplicated on it.
        let Some(sequence) = sequence else {
            return false;
        };
        let scope = Probe::new(path, topic);
        let key = Key { producer, sequence };
        let mut state = self.state.lock().expect("dedup window poisoned");
        state.expire(now);
        if state.deadline_of(&scope, key).is_some() {
            return true;
        }
        state.insert(&scope, key, now + window, self.max_entries);
        false
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.state
            .lock()
            .expect("dedup window poisoned")
            .order
            .len()
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        let state = self.state.lock().expect("dedup window poisoned");
        state.seen.capacity() > 0 || state.order.capacity() > 0
    }
}

impl State {
    fn deadline_of(&self, scope: &dyn ScopeRef, key: Key) -> Option<Instant> {
        self.seen.get(scope)?.get(&key).copied()
    }

    fn expire(&mut self, now: Instant) {
        while let Some((deadline, _, _)) = self.order.front() {
            if *deadline > now {
                break;
            }
            let (_, scope, key) = self.order.pop_front().expect("checked above");
            // Only remove when the table still holds *this* entry's deadline:
            // an identity re-inserted after expiry has a later one.
            if self.deadline_of(&scope, key).is_some_and(|d| d <= now) {
                self.remove(&scope, key);
            }
        }
    }

    fn remove(&mut self, scope: &ScopeKey, key: Key) {
        if let Some(lane) = self.seen.get_mut(scope) {
            lane.remove(&key);
            if lane.is_empty() {
                self.seen.remove(scope);
            }
        }
    }

    fn insert(&mut self, scope: &dyn ScopeRef, key: Key, deadline: Instant, max_entries: usize) {
        // Evict oldest first, so the table never exceeds its cap even when
        // every entry is still inside its window. The count bound is what
        // stops a peer from sizing this table by sending fast.
        while self.order.len() >= max_entries {
            let Some((_, oldest, oldest_key)) = self.order.pop_front() else {
                break;
            };
            self.remove(&oldest, oldest_key);
        }
        let owned = match self.seen.get_key_value(scope) {
            Some((existing, _)) => existing.clone(),
            None => ScopeKey::new(scope.path(), scope.topic()),
        };
        self.order.push_back((deadline, owned.clone(), key));
        self.seen.entry(owned).or_default().insert(key, deadline);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: [u8; 32] = [7u8; 32];

    #[test]
    fn a_disabled_window_suppresses_nothing_and_allocates_nothing() {
        for level in [Deduplication::None, Deduplication::Durable] {
            let window = DedupWindow::new(level, Some(60_000), 4096);
            for seq in 0..1000 {
                assert!(!window.is_duplicate(None, "/md", None, Some(seq % 4), Instant::now()));
            }
            assert!(
                !window.allocated(),
                "an off dedup window must not allocate a table"
            );
        }
        // `Bounded` without a window is the same: the wire cannot express it.
        let window = DedupWindow::new(Deduplication::Bounded, None, 4096);
        assert!(!window.is_duplicate(None, "/md", None, Some(1), Instant::now()));
        assert!(!window.allocated());
    }

    #[test]
    fn an_unnumbered_transfer_is_never_a_duplicate() {
        // Nothing on the wire names it, so there is nothing to compare.
        let window = DedupWindow::new(Deduplication::Bounded, Some(60_000), 4096);
        for _ in 0..10 {
            assert!(!window.is_duplicate(None, "/md", None, None, Instant::now()));
        }
        assert_eq!(window.tracked(), 0);
    }

    #[test]
    fn a_repeat_inside_the_window_is_suppressed() {
        let window = DedupWindow::new(Deduplication::Bounded, Some(60_000), 4096);
        assert!(!window.is_duplicate(None, "/md", None, Some(1), Instant::now()));
        assert!(window.is_duplicate(None, "/md", None, Some(1), Instant::now()));
        assert!(window.is_duplicate(None, "/md", None, Some(1), Instant::now()));
        // A different number, scope or producer is a different message.
        assert!(!window.is_duplicate(None, "/md", None, Some(2), Instant::now()));
        assert!(!window.is_duplicate(None, "/other", None, Some(1), Instant::now()));
        assert!(!window.is_duplicate(Some(DIGEST), "/md", None, Some(1), Instant::now()));
        assert!(window.is_duplicate(Some(DIGEST), "/md", None, Some(1), Instant::now()));
    }

    #[test]
    fn a_topic_is_a_scope_of_its_own() {
        let window = DedupWindow::new(Deduplication::Bounded, Some(60_000), 4096);
        assert!(!window.is_duplicate(None, "/a", None, Some(1), Instant::now()));
        // The same number on the same path under a topic is another message,
        // and so is the same topic on another path, and the empty topic.
        assert!(!window.is_duplicate(None, "/a", Some("t"), Some(1), Instant::now()));
        assert!(!window.is_duplicate(None, "/b", Some("t"), Some(1), Instant::now()));
        assert!(!window.is_duplicate(None, "/a", Some(""), Some(1), Instant::now()));
        assert!(window.is_duplicate(None, "/a", Some("t"), Some(1), Instant::now()));
        assert!(window.is_duplicate(None, "/a", None, Some(1), Instant::now()));
        assert_eq!(window.tracked(), 4);
    }

    #[test]
    fn an_identity_reused_after_its_window_is_not_suppressed() {
        // The smallest interval that demonstrates it: a zero-length window
        // has expired by the time the second call takes the lock.
        let window = DedupWindow::new(Deduplication::Bounded, Some(0), 4096);
        assert!(!window.is_duplicate(None, "/md", None, Some(1), Instant::now()));
        assert!(!window.is_duplicate(None, "/md", None, Some(1), Instant::now()));
        assert_eq!(window.tracked(), 1, "the table does not grow with repeats");
    }

    #[test]
    fn the_table_stops_growing_at_its_count_cap() {
        let window = DedupWindow::new(Deduplication::Bounded, Some(60_000), 8);
        for seq in 0..1000 {
            assert!(!window.is_duplicate(None, "/md", None, Some(seq), Instant::now()));
        }
        assert_eq!(
            window.tracked(),
            8,
            "a peer must not be able to size this table by sending fast"
        );
        // The oldest entries are gone, so an old identity is accepted again:
        // the count bound costs suppression, not memory.
        assert!(!window.is_duplicate(None, "/md", None, Some(0), Instant::now()));
        // The newest are still remembered.
        assert!(window.is_duplicate(None, "/md", None, Some(999), Instant::now()));
    }
}
