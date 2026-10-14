//! Bounded deduplication: `Deduplication::Bounded(window)`.
//!
//! A receiver remembers the identity of what it has seen for a while and
//! suppresses a repeat of it. The identity is the triple
//! `(producer, scope, sequence)` — the producer being the connection's proved
//! fingerprint unless DATA key `7` names another one
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

/// The identity of one transfer, for suppression purposes.
///
/// `producer` is `None` in the ordinary case, where the producer *is* the
/// connection: the fingerprint is proved by the handshake and identical for
/// every transfer here, so keeping it out of the key costs 32 bytes less per
/// entry and changes nothing.
///
/// **The key owns its scope, and that is measured rather than argued.**
/// `is_duplicate` builds this key on every call, so `scope.into()`
/// allocates a `Box<str>` even on the two paths that insert nothing. The
/// allocation costs **~10 ns** per call, and the obvious way to avoid it —
/// a two-level `HashMap<Box<str>, HashMap<(producer, sequence), _>>`, whose
/// outer lookup borrows `&str` — is **not faster**: a second hash lookup
/// costs what the allocation cost, so the candidate measured 42.0-42.8 ns
/// against this shape's 41.1-41.3 ns on a miss and the same within noise on a
/// hit (`cargo bench -p weida --bench patterns -- dedup_key`,
/// `docs/IMPLEMENTATION.md` §4.12). Against a 1 KiB push at ~7.9 µs it is
/// 0.13 %, and deduplication is opt-in, so a default connection never pays it
/// at all. The flat key stays.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Identity {
    producer: Option<[u8; 32]>,
    scope: Box<str>,
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
    /// Insertion order, for expiry and for eviction at the count cap.
    order: VecDeque<(Instant, Identity)>,
    /// Present identities. A `HashMap` rather than a set so that the entry
    /// carries its deadline and expiry needs no second lookup.
    seen: HashMap<Identity, Instant>,
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
    /// Records it when it is not. An identity reused after its window has
    /// passed is **not** suppressed — that is what "bounded" means, and it is
    /// the JetStream shape decision 0001 §7.6 took.
    pub(crate) fn is_duplicate(
        &self,
        producer: Option<[u8; 32]>,
        scope: &str,
        sequence: Option<u64>,
    ) -> bool {
        let Some(window) = self.window else {
            return false;
        };
        // Nothing names this transfer, so nothing can be deduplicated on it.
        let Some(sequence) = sequence else {
            return false;
        };
        let identity = Identity {
            producer,
            scope: scope.into(),
            sequence,
        };
        let now = Instant::now();
        let mut state = self.state.lock().expect("dedup window poisoned");
        state.expire(now);
        if state.seen.contains_key(&identity) {
            return true;
        }
        state.insert(identity, now + window, self.max_entries);
        false
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.state.lock().expect("dedup window poisoned").seen.len()
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        let state = self.state.lock().expect("dedup window poisoned");
        state.seen.capacity() > 0 || state.order.capacity() > 0
    }
}

impl State {
    fn expire(&mut self, now: Instant) {
        while let Some((deadline, _)) = self.order.front() {
            if *deadline > now {
                break;
            }
            let (_, identity) = self.order.pop_front().expect("checked above");
            // Only remove when the map still holds *this* entry's deadline:
            // an identity re-inserted after expiry has a later one.
            if self.seen.get(&identity).is_some_and(|d| *d <= now) {
                self.seen.remove(&identity);
            }
        }
    }

    fn insert(&mut self, identity: Identity, deadline: Instant, max_entries: usize) {
        // Evict oldest first, so the table never exceeds its cap even when
        // every entry is still inside its window. The count bound is what
        // stops a peer from sizing this table by sending fast.
        while self.order.len() >= max_entries {
            let Some((_, oldest)) = self.order.pop_front() else {
                break;
            };
            self.seen.remove(&oldest);
        }
        self.order.push_back((deadline, identity.clone()));
        self.seen.insert(identity, deadline);
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
                assert!(!window.is_duplicate(None, "/md", Some(seq % 4)));
            }
            assert!(
                !window.allocated(),
                "an off dedup window must not allocate a table"
            );
        }
        // `Bounded` without a window is the same: the wire cannot express it.
        let window = DedupWindow::new(Deduplication::Bounded, None, 4096);
        assert!(!window.is_duplicate(None, "/md", Some(1)));
        assert!(!window.allocated());
    }

    #[test]
    fn an_unnumbered_transfer_is_never_a_duplicate() {
        // Nothing on the wire names it, so there is nothing to compare.
        let window = DedupWindow::new(Deduplication::Bounded, Some(60_000), 4096);
        for _ in 0..10 {
            assert!(!window.is_duplicate(None, "/md", None));
        }
        assert_eq!(window.tracked(), 0);
    }

    #[test]
    fn a_repeat_inside_the_window_is_suppressed() {
        let window = DedupWindow::new(Deduplication::Bounded, Some(60_000), 4096);
        assert!(!window.is_duplicate(None, "/md", Some(1)));
        assert!(window.is_duplicate(None, "/md", Some(1)));
        assert!(window.is_duplicate(None, "/md", Some(1)));
        // A different number, scope or producer is a different message.
        assert!(!window.is_duplicate(None, "/md", Some(2)));
        assert!(!window.is_duplicate(None, "/other", Some(1)));
        assert!(!window.is_duplicate(Some(DIGEST), "/md", Some(1)));
        assert!(window.is_duplicate(Some(DIGEST), "/md", Some(1)));
    }

    #[test]
    fn an_identity_reused_after_its_window_is_not_suppressed() {
        // The smallest interval that demonstrates it: a zero-length window
        // has expired by the time the second call takes the lock.
        let window = DedupWindow::new(Deduplication::Bounded, Some(0), 4096);
        assert!(!window.is_duplicate(None, "/md", Some(1)));
        assert!(!window.is_duplicate(None, "/md", Some(1)));
        assert_eq!(window.tracked(), 1, "the table does not grow with repeats");
    }

    #[test]
    fn the_table_stops_growing_at_its_count_cap() {
        let window = DedupWindow::new(Deduplication::Bounded, Some(60_000), 8);
        for seq in 0..1000 {
            assert!(!window.is_duplicate(None, "/md", Some(seq)));
        }
        assert_eq!(
            window.tracked(),
            8,
            "a peer must not be able to size this table by sending fast"
        );
        // The oldest entries are gone, so an old identity is accepted again:
        // the count bound costs suppression, not memory.
        assert!(!window.is_duplicate(None, "/md", Some(0)));
        // The newest are still remembered.
        assert!(window.is_duplicate(None, "/md", Some(999)));
    }
}
