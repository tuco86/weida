//! Per-producer sequencing, gap detection and reassembly.
//!
//! One sender-side counter and two receiver-side modes, the two of
//! [decision 0001](../../../docs/decisions/0001-sequence-field.md) §7.5.
//! *Detect*, the default: report the gap and **deliver messages as they
//! arrive**, holding nothing back. *Reassemble*, opt-in: hold an arrival
//! whose predecessors are missing and release the run in sequence order,
//! accepting head-of-line blocking above QUIC in exchange, with the hold
//! bounded by `Limits::max_reorder_hold`.
//!
//! All three parts are inert when the negotiated ordering is `None`, and the
//! reassembler is inert under `detect` as well: no map is ever touched, so an
//! endpoint that negotiated nothing allocates nothing — the hot-path rule of
//! `docs/INVARIANTS.md`, checked by the unit tests at the bottom of this file
//! rather than asserted in prose.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap, btree_map};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use weida_protocol::header::OrderingMode;

/// What a receiver observed about one transfer's place in its producer's
/// sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gap {
    /// The sequence number the receiver expected next.
    pub expected: u64,
    /// The sequence number that actually arrived.
    pub seen: u64,
}

impl Gap {
    /// How many transfers went missing. Always at least one: a `Gap` is only
    /// reported when `seen` is ahead of `expected`.
    pub fn missed(&self) -> u64 {
        self.seen.saturating_sub(self.expected)
    }
}

/// One receiver-side scope: the path a transfer arrived on, and the topic of a
/// published copy when it carries one.
///
/// `(path, None)` and `(path, Some(""))` are different scopes, and so is one
/// topic on two paths. Two publishers that each number their copies per topic
/// are independent counters; keying the receiver by topic alone would merge
/// them, hiding gaps and suppressing transfers that are not repeats.
///
/// Tables hold this owned form and are searched with a [`Probe`] over the
/// caller's borrowed names, so a transfer on a scope that is already tracked
/// allocates nothing: only the first transfer of a scope copies its names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScopeKey {
    path: Arc<str>,
    topic: Option<Arc<str>>,
}

impl ScopeKey {
    pub(crate) fn new(path: &str, topic: Option<&str>) -> ScopeKey {
        ScopeKey {
            path: path.into(),
            topic: topic.map(Into::into),
        }
    }
}

/// The two parts of a scope, however they are held.
pub(crate) trait ScopeRef {
    fn path(&self) -> &str;
    fn topic(&self) -> Option<&str>;
}

/// A scope named by borrowed parts, for lookups that must not allocate.
pub(crate) struct Probe<'a> {
    path: &'a str,
    topic: Option<&'a str>,
}

impl<'a> Probe<'a> {
    pub(crate) fn new(path: &'a str, topic: Option<&'a str>) -> Probe<'a> {
        Probe { path, topic }
    }
}

impl ScopeRef for Probe<'_> {
    fn path(&self) -> &str {
        self.path
    }

    fn topic(&self) -> Option<&str> {
        self.topic
    }
}

impl ScopeRef for ScopeKey {
    fn path(&self) -> &str {
        &self.path
    }

    fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }
}

/// Every hash of a scope goes through here, so an owned key and a borrowed
/// probe of the same scope agree on it.
fn hash_scope<H: Hasher>(path: &str, topic: Option<&str>, state: &mut H) {
    path.hash(state);
    topic.hash(state);
}

impl Hash for ScopeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_scope(self.path(), self.topic(), state);
    }
}

impl Hash for dyn ScopeRef + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_scope(self.path(), self.topic(), state);
    }
}

impl PartialEq for dyn ScopeRef + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.path() == other.path() && self.topic() == other.topic()
    }
}

impl Eq for dyn ScopeRef + '_ {}

impl<'a> Borrow<dyn ScopeRef + 'a> for ScopeKey {
    fn borrow(&self) -> &(dyn ScopeRef + 'a) {
        self
    }
}

/// Assigns sequence numbers per scope: the dialled path, or the topic of a
/// published copy.
///
/// The producer itself is the connection — the peer's proved fingerprint is
/// what names it, so nothing goes on the wire for it
/// ([decision 0008](../../../docs/decisions/0008-session-identity.md) §4.4) —
/// which is why one sequencer lives per connection.
///
/// The table is bounded by `max_sequence_scopes` and never evicts. A scope
/// that was forgotten and then came back would restart at zero, and a
/// receiver that had already seen those numbers would read the restart as
/// repeats and suppress them. So a scope past the cap is sent unnumbered
/// instead: the receiver passes it through with no gap tracking and no
/// deduplication, which is the honest outcome for a scope the sender cannot
/// number.
pub(crate) struct Sequencer {
    enabled: bool,
    /// Bounded by `max_sequence_scopes`, see the type documentation.
    max_scopes: usize,
    counters: Mutex<Positions<Box<str>>>,
    /// Set once the table has refused a scope, so the debug log says so once.
    overflow_logged: AtomicBool,
}

impl Sequencer {
    pub(crate) fn new(ordering: OrderingMode, max_scopes: usize) -> Sequencer {
        Sequencer {
            enabled: ordering != OrderingMode::None,
            // A table with no room could not number anything, so one slot is the floor.
            max_scopes: max_scopes.max(1),
            counters: Mutex::new(Positions::new()),
            overflow_logged: AtomicBool::new(false),
        }
    }

    /// The next number for `scope`, or `None` when ordering is off or the
    /// scope is new and the table is at its cap.
    ///
    /// Numbers start at zero and are dense per scope; a receiver reads a
    /// missing number as a gap. A scope already tracked keeps counting at the
    /// cap, so only a new scope can be left unnumbered.
    pub(crate) fn next(&self, scope: &str) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        let mut counters = self.counters.lock().expect("sequencer poisoned");
        if let Some(position) = counters.touch(scope) {
            let issued = position.next;
            position.next = issued.wrapping_add(1);
            return Some(issued);
        }
        if counters.map.len() >= self.max_scopes {
            if !self.overflow_logged.swap(true, AtomicOrdering::Relaxed) {
                tracing::debug!(
                    max = self.max_scopes,
                    "sequencer scope table full: new scopes are sent unnumbered"
                );
            }
            return None;
        }
        counters.track(scope.into(), 1);
        Some(0)
    }

    #[cfg(test)]
    fn tracked_scopes(&self) -> usize {
        self.counters.lock().expect("sequencer poisoned").map.len()
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        self.counters
            .lock()
            .expect("sequencer poisoned")
            .map
            .capacity()
            > 0
    }
}

/// The position of each scope a table tracks, and the clock that orders the
/// scopes by last use.
struct Positions<K> {
    clock: u64,
    map: HashMap<K, Position>,
}

#[derive(Clone, Copy, Debug)]
struct Position {
    /// The number expected next.
    next: u64,
    /// The clock value at this scope's last use.
    touched: u64,
}

impl<K: Clone + Eq + Hash> Positions<K> {
    fn new() -> Positions<K> {
        Positions {
            clock: 0,
            map: HashMap::new(),
        }
    }

    /// The position of a tracked scope, marked as used now.
    fn touch<Q>(&mut self, scope: &Q) -> Option<&mut Position>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.clock = self.clock.wrapping_add(1);
        let touched = self.clock;
        let position = self.map.get_mut(scope)?;
        position.touched = touched;
        Some(position)
    }

    /// Starts tracking `scope` at `next`. The caller decides whether there is
    /// room; this never evicts.
    fn track(&mut self, scope: K, next: u64) {
        self.clock = self.clock.wrapping_add(1);
        self.map.insert(
            scope,
            Position {
                next,
                touched: self.clock,
            },
        );
    }
}

/// Tracks the next expected number per scope and reports what is missing.
pub(crate) struct GapDetector {
    enabled: bool,
    /// Bounded by `max_sequence_scopes`: the scopes are the peer's paths and
    /// topics, so the table is remote-influenced and needs a named cap
    /// (`docs/INVARIANTS.md`).
    max_scopes: usize,
    expected: Mutex<Positions<ScopeKey>>,
}

impl GapDetector {
    pub(crate) fn new(ordering: OrderingMode, max_scopes: usize) -> GapDetector {
        GapDetector {
            // Detect mode only: under `reassemble` the reassembler owns the
            // position and reports what it had to skip, and two components
            // tracking the same counter would report the same hole twice.
            enabled: ordering == OrderingMode::PerProducerDetect,
            max_scopes,
            expected: Mutex::new(Positions::new()),
        }
    }

    /// Records `sequence` for the scope `(path, topic)` and reports a gap if
    /// one opened.
    ///
    /// A number below the expected one is a repeat or a reordering, not a
    /// gap: detect mode reports what is missing and never reorders, so an
    /// out-of-order arrival lowers nothing and is delivered as it came.
    pub(crate) fn observe(&self, path: &str, topic: Option<&str>, sequence: u64) -> Option<Gap> {
        if !self.enabled {
            return None;
        }
        let mut expected = self.expected.lock().expect("gap detector poisoned");
        let probe: &dyn ScopeRef = &Probe::new(path, topic);
        if let Some(position) = expected.touch(probe) {
            let gap = (sequence > position.next).then_some(Gap {
                expected: position.next,
                seen: sequence,
            });
            // A number below the expected one carries no information about
            // what is still to come: advancing on it would consume a number
            // that has not arrived and silence its gap.
            if sequence >= position.next {
                position.next = sequence.wrapping_add(1);
            }
            return gap;
        }
        // The first number seen for a scope establishes the position; a
        // connection may legitimately start anywhere, because the peer's
        // counter is older than this connection's view of it.
        if !make_room(
            &mut expected.map,
            self.max_scopes,
            "gap detector",
            |position| position.touched,
            |_| true,
        ) {
            return None;
        }
        expected.track(ScopeKey::new(path, topic), sequence.wrapping_add(1));
        None
    }

    #[cfg(test)]
    fn tracked_scopes(&self) -> usize {
        self.expected
            .lock()
            .expect("gap detector poisoned")
            .map
            .len()
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        self.expected
            .lock()
            .expect("gap detector poisoned")
            .map
            .capacity()
            > 0
    }
}

/// Holds out-of-order arrivals back and releases them in sequence order:
/// `PerProducer(reassemble)` of
/// [decision 0001](../../../docs/decisions/0001-sequence-field.md) §7.5.
///
/// Generic in what is held so that the ordering logic can be exercised
/// without a connection; the connection holds `Reassembler<Held>`, where a
/// `Held` is one *unread* transfer — the stream handle, its metadata and its
/// path. **No payload is materialized**, which "core transport does not
/// require payload materialization" ([`docs/INVARIANTS.md`]) forbids. What a
/// held transfer really pins is transport memory: one entry of the peer's
/// `max_concurrent_uni_streams` budget and up to `stream_receive_window`
/// bytes of quinn's receive buffer for that stream, the whole hold being
/// bounded in turn by `connection_receive_window`. That is the accountability
/// [0002](../../../docs/decisions/0002-control-and-bulk-separation.md) §6.6
/// asks for, reached from the other side: 0002 proposed reading eagerly into
/// an application-owned buffer so that transport credit is released early,
/// which v0 cannot do without materializing the payload, so the hold keeps
/// the bytes in the transport where the peer's own window already bounds them
/// and pays for them with head-of-line blocking instead.
pub(crate) struct Reassembler<T> {
    enabled: bool,
    /// Transfers held at once, summed over scopes: the `max_reorder_hold`
    /// bound of `docs/INVARIANTS.md`. `docs/IMPLEMENTATION.md` §4.7 measures
    /// the peak at N − 1 of the transfers in flight, and 84 of 256 with no
    /// adversarial pattern, so this is a configured number and never an
    /// assumption about arrival order.
    max_hold: usize,
    /// Scopes tracked at once, shared bound with the gap detector.
    max_scopes: usize,
    state: Mutex<Reorder<T>>,
}

struct Reorder<T> {
    scopes: HashMap<ScopeKey, Scope<T>>,
    /// Held entries over all scopes, kept here so the cap is one comparison.
    held: usize,
    /// Advanced on every arrival; a scope's `touched` is its last arrival.
    clock: u64,
}

struct Scope<T> {
    /// The number that must arrive before anything after it is released.
    next: u64,
    pending: BTreeMap<u64, T>,
    touched: u64,
}

impl<T> Reassembler<T> {
    pub(crate) fn new(
        ordering: OrderingMode,
        max_hold: usize,
        max_scopes: usize,
    ) -> Reassembler<T> {
        Reassembler {
            enabled: ordering == OrderingMode::PerProducerReassemble,
            max_hold,
            max_scopes,
            state: Mutex::new(Reorder {
                scopes: HashMap::new(),
                held: 0,
                clock: 0,
            }),
        }
    }

    /// Whether anything is reassembled at all. The caller checks this before
    /// building the argument list, so a connection that negotiated `None` or
    /// `detect` never touches this structure and never allocates the `Vec`.
    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Offers one arrival on the scope `(path, topic)` and returns what may
    /// now be delivered, in order.
    ///
    /// An unnumbered arrival, an arrival on an untracked scope and an arrival
    /// below the position are passed straight through: nothing that is still
    /// to come can be ordered against them. So is a **repeat of a number
    /// already held** — the held transfer keeps its slot, and the repeat is
    /// delivered immediately rather than displacing it. A repeat is
    /// delivered, never silently discarded; suppressing one is
    /// deduplication's job and nothing else's
    /// ([decision 0001](../../../docs/decisions/0001-sequence-field.md) §7.1
    /// keeps the two dimensions apart).
    ///
    /// Everything else is held until the numbers before it have arrived — or
    /// until the hold is full, which releases the oldest held transfer out of
    /// order and reports the numbers it skipped as a `Gap`, the behaviour
    /// `docs/GUARANTEES.md` §3 states. Reassembly is never silently weakened:
    /// the application is told.
    pub(crate) fn admit(
        &self,
        path: &str,
        topic: Option<&str>,
        sequence: Option<u64>,
        item: T,
    ) -> Released<T> {
        // Defensive: callers gate on `enabled()` so that an off connection
        // never even builds the argument list, but an off reassembler must
        // hold nothing if it is called anyway.
        if !self.enabled {
            return Released::One(Some((item, None)));
        }
        let Some(sequence) = sequence else {
            return Released::One(Some((item, None)));
        };
        let mut state = self.state.lock().expect("reassembler poisoned");
        let Reorder {
            scopes,
            held,
            clock,
        } = &mut *state;
        *clock = clock.wrapping_add(1);
        let touched = *clock;
        let probe: &dyn ScopeRef = &Probe::new(path, topic);
        if scopes.get(probe).is_none() {
            if !make_room(
                scopes,
                self.max_scopes,
                "reassembler",
                |scope| scope.touched,
                |scope| scope.pending.is_empty(),
            ) {
                // Every tracked scope holds a transfer, and a held transfer is
                // never dropped: the new scope is passed through untracked, so
                // a peer that invents scopes cannot size this table.
                return Released::One(Some((item, None)));
            }
            // The first number seen establishes the position: the peer's
            // counter is older than this connection's view of it.
            scopes.insert(
                ScopeKey::new(path, topic),
                Scope {
                    next: sequence,
                    pending: BTreeMap::new(),
                    touched,
                },
            );
        }

        let entry = scopes.get_mut(probe).expect("present");
        entry.touched = touched;
        if sequence < entry.next {
            // Late: its successors have already gone to the application, so
            // holding it would order it against nothing.
            return Released::One(Some((item, None)));
        }
        if sequence == entry.next && entry.pending.is_empty() {
            // The common case, and the whole hot path of reassemble mode: the
            // next number arrived and nothing is waiting behind it, so it goes
            // straight out without a map node.
            entry.next = sequence.wrapping_add(1);
            return Released::One(Some((item, None)));
        }
        match entry.pending.entry(sequence) {
            btree_map::Entry::Occupied(_) => return Released::One(Some((item, None))),
            btree_map::Entry::Vacant(slot) => slot.insert(item),
        };
        *held += 1;
        let mut out = Vec::new();
        *held -= drain_in_order(entry, &mut out);

        // Enforce the cap by releasing, never by growing. The victim is the
        // scope holding the most, so one stalled producer cannot spend the
        // whole budget and push every other scope into out-of-order release.
        while *held > self.max_hold {
            let Some(victim) = widest_scope(scopes) else {
                break;
            };
            let Some((seq, item)) = victim.pending.pop_first() else {
                break;
            };
            let gap = Gap {
                expected: victim.next,
                seen: seq,
            };
            victim.next = seq.wrapping_add(1);
            out.push((item, Some(gap)));
            *held -= 1;
            *held -= drain_in_order(victim, &mut out);
        }
        Released::Many(out.into_iter())
    }

    #[cfg(test)]
    fn held(&self) -> usize {
        self.state.lock().expect("reassembler poisoned").held
    }

    #[cfg(test)]
    fn tracked_scopes(&self) -> usize {
        self.state
            .lock()
            .expect("reassembler poisoned")
            .scopes
            .len()
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        self.state
            .lock()
            .expect("reassembler poisoned")
            .scopes
            .capacity()
            > 0
    }
}

/// What one arrival releases, in order. A single in-order arrival is the
/// common case and comes back without a heap allocation.
pub(crate) enum Released<T> {
    One(Option<(T, Option<Gap>)>),
    Many(std::vec::IntoIter<(T, Option<Gap>)>),
}

impl<T> Iterator for Released<T> {
    type Item = (T, Option<Gap>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Released::One(slot) => slot.take(),
            Released::Many(items) => items.next(),
        }
    }
}

/// Moves the consecutive run starting at `scope.next` out of the hold and
/// returns how many entries that was.
fn drain_in_order<T>(scope: &mut Scope<T>, out: &mut Vec<(T, Option<Gap>)>) -> usize {
    let mut released = 0;
    while let Some(entry) = scope.pending.first_entry() {
        if *entry.key() != scope.next {
            break;
        }
        out.push((entry.remove(), None));
        scope.next = scope.next.wrapping_add(1);
        released += 1;
    }
    released
}

/// Makes room for one more scope in a table of at most `max_scopes` entries.
///
/// At the cap this drops the least recently used entry that `evictable`
/// accepts, and says so at debug level: whatever that scope held is no longer
/// tracked. Returns `false`, leaving the table as it was, when it is full and
/// every entry is pinned.
fn make_room<K, V>(
    table: &mut HashMap<K, V>,
    max_scopes: usize,
    table_name: &str,
    touched: impl Fn(&V) -> u64,
    evictable: impl Fn(&V) -> bool,
) -> bool
where
    K: Clone + Eq + Hash,
{
    if table.len() < max_scopes {
        return true;
    }
    let victim = table
        .iter()
        .filter(|(_, value)| evictable(*value))
        .min_by_key(|(_, value)| touched(*value))
        .map(|(scope, _)| scope.clone());
    let Some(victim) = victim else {
        return false;
    };
    table.remove(&victim);
    tracing::debug!(
        table = table_name,
        "scope table at its cap: the least recently used scope is no longer tracked"
    );
    true
}

/// The scope holding the most transfers: the one to take a slot from when the
/// hold is full.
fn widest_scope<T>(scopes: &mut HashMap<ScopeKey, Scope<T>>) -> Option<&mut Scope<T>> {
    scopes
        .values_mut()
        .max_by_key(|scope| scope.pending.len())
        .filter(|scope| !scope.pending.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_disabled_sequencer_numbers_nothing_and_allocates_nothing() {
        let sequencer = Sequencer::new(OrderingMode::None, 1024);
        for _ in 0..1000 {
            assert_eq!(sequencer.next("/md"), None);
        }
        assert_eq!(sequencer.tracked_scopes(), 0);
        assert!(
            !sequencer.allocated(),
            "an off sequencer must not allocate a table"
        );
    }

    #[test]
    fn a_disabled_detector_reports_nothing_and_allocates_nothing() {
        let detector = GapDetector::new(OrderingMode::None, 1024);
        for seq in [0, 5, 1, 900] {
            assert_eq!(detector.observe("/md", None, seq), None);
        }
        assert!(
            !detector.allocated(),
            "an off detector must not allocate a table"
        );
    }

    #[test]
    fn numbers_are_dense_and_per_scope() {
        let sequencer = Sequencer::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(sequencer.next("a"), Some(0));
        assert_eq!(sequencer.next("a"), Some(1));
        assert_eq!(sequencer.next("b"), Some(0));
        assert_eq!(sequencer.next("a"), Some(2));
        assert_eq!(sequencer.tracked_scopes(), 2);
    }

    #[test]
    fn the_sender_numbers_the_first_scopes_and_leaves_later_ones_unnumbered() {
        const CAP: usize = 64;
        let sequencer = Sequencer::new(OrderingMode::PerProducerDetect, CAP);
        for i in 0..10_000 {
            let issued = sequencer.next(&format!("topic-{i}"));
            if i < CAP {
                assert_eq!(issued, Some(0), "scope {i} is inside the cap");
            } else {
                assert_eq!(issued, None, "scope {i} is past the cap");
            }
        }
        assert_eq!(sequencer.tracked_scopes(), CAP);
        // A scope numbered before the cap keeps its counter: the sender never
        // forgets a scope, so its numbers stay dense.
        assert_eq!(sequencer.next("topic-0"), Some(1));
        assert_eq!(sequencer.next("topic-0"), Some(2));
    }

    #[test]
    fn a_missing_number_is_a_gap_of_that_size() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", None, 0), None);
        assert_eq!(detector.observe("a", None, 1), None);
        // 2 and 3 never arrive.
        let gap = detector.observe("a", None, 4).expect("a gap");
        assert_eq!(
            gap,
            Gap {
                expected: 2,
                seen: 4
            }
        );
        assert_eq!(gap.missed(), 2);
        // The detector resynchronizes: the next number in order is clean.
        assert_eq!(detector.observe("a", None, 5), None);
    }

    #[test]
    fn scopes_do_not_interfere() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", None, 0), None);
        assert_eq!(detector.observe("b", None, 0), None);
        assert_eq!(detector.observe("a", None, 1), None);
        assert_eq!(detector.observe("b", None, 7).expect("gap").missed(), 6);
    }

    #[test]
    fn a_topic_is_a_scope_of_its_own_under_its_path() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("/a", None, 0), None);
        assert_eq!(detector.observe("/a", Some(""), 0), None);
        assert_eq!(detector.observe("/a", Some("t"), 0), None);
        // Each scope counts its own numbers: a hole in one is not hidden by
        // the others, and the other's position is not moved by it.
        assert_eq!(detector.observe("/a", None, 2).expect("gap").missed(), 1);
        assert_eq!(
            detector.observe("/a", Some("t"), 3).expect("gap").missed(),
            2
        );
        assert_eq!(detector.tracked_scopes(), 3);
    }

    #[test]
    fn an_out_of_order_arrival_is_not_a_gap() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", None, 0), None);
        assert_eq!(detector.observe("a", None, 2).expect("gap").missed(), 1);
        // 1 arrives late: detect mode delivers it and reports no new gap,
        // because nothing further is missing.
        assert_eq!(detector.observe("a", None, 1), None);
        // And it did not consume 3: a late arrival moves the position
        // nowhere, so a later hole is still reported.
        assert_eq!(detector.observe("a", None, 4).expect("gap").missed(), 1);
    }

    #[test]
    fn the_scope_table_stops_growing_at_its_cap() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 4);
        for i in 0..100 {
            detector.observe(&format!("scope-{i}"), None, 0);
        }
        assert_eq!(
            detector.tracked_scopes(),
            4,
            "a peer must not be able to size this table"
        );
    }

    #[test]
    fn an_idle_scope_makes_room_and_the_new_scope_is_tracked() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 4);
        for i in 0..4 {
            detector.observe(&format!("scope-{i}"), None, 0);
        }
        // Using scope-0 again makes it recent, so scope-1 is the one to go.
        assert_eq!(detector.observe("scope-0", None, 1), None);
        // The table is full: a fifth scope takes scope-1's place.
        assert_eq!(detector.observe("scope-4", None, 0), None);
        assert_eq!(detector.tracked_scopes(), 4);
        // The new scope gets gap detection like any tracked scope.
        assert_eq!(detector.observe("scope-4", None, 1), None);
        assert_eq!(
            detector.observe("scope-4", None, 3),
            Some(Gap {
                expected: 2,
                seen: 3
            })
        );
        // The recently used scope kept its position.
        assert_eq!(
            detector.observe("scope-0", None, 3),
            Some(Gap {
                expected: 2,
                seen: 3
            })
        );
    }

    /// Shorthand: the items released by one arrival, without their gaps.
    fn items(released: impl IntoIterator<Item = (u64, Option<Gap>)>) -> Vec<u64> {
        released.into_iter().map(|(item, _)| item).collect()
    }

    #[test]
    fn a_disabled_reassembler_holds_nothing_and_allocates_nothing() {
        for mode in [OrderingMode::None, OrderingMode::PerProducerDetect] {
            let reassembler: Reassembler<u64> = Reassembler::new(mode, 256, 1024);
            assert!(!reassembler.enabled());
            for seq in 0..1000 {
                // Out of order on purpose: an off reassembler still passes
                // everything through untouched.
                assert_eq!(
                    items(reassembler.admit("/md", None, Some(999 - seq), seq)),
                    [seq]
                );
            }
            assert_eq!(reassembler.held(), 0);
            assert!(
                !reassembler.allocated(),
                "an off reassembler must not allocate a table"
            );
        }
    }

    #[test]
    fn out_of_order_arrivals_are_released_in_sequence_order() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 1024);
        assert_eq!(items(r.admit("a", None, Some(0), 0)), [0]);
        // 1 is missing, so 2 and 3 wait for it.
        assert!(items(r.admit("a", None, Some(2), 2)).is_empty());
        assert!(items(r.admit("a", None, Some(3), 3)).is_empty());
        assert_eq!(r.held(), 2);
        // 1 arrives and the whole run goes out at once, in order.
        assert_eq!(items(r.admit("a", None, Some(1), 1)), [1, 2, 3]);
        assert_eq!(r.held(), 0, "the hold must drain");
    }

    #[test]
    fn a_run_released_in_order_carries_no_gap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 1024);
        r.admit("a", None, Some(0), 0);
        r.admit("a", None, Some(2), 2);
        let released: Vec<_> = r.admit("a", None, Some(1), 1).collect();
        assert!(
            released.iter().all(|(_, gap)| gap.is_none()),
            "nothing was missed: the hole was filled"
        );
    }

    #[test]
    fn the_hold_releases_out_of_order_at_its_cap_and_reports_the_gap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 2, 1024);
        assert_eq!(items(r.admit("a", None, Some(0), 0)), [0]);
        // 1 never arrives; 2 and 3 fill the hold.
        assert!(r.admit("a", None, Some(2), 2).next().is_none());
        assert!(r.admit("a", None, Some(3), 3).next().is_none());
        // 4 does not fit: the oldest held transfer is released out of order
        // and says what it skipped, which is exactly what detect mode would
        // have reported. 3 and 4 follow it in order behind the forced
        // release, so the hole at 1 costs one out-of-order delivery and not
        // the whole run.
        let released: Vec<_> = r.admit("a", None, Some(4), 4).collect();
        assert_eq!(items(released.clone()), [2, 3, 4]);
        assert_eq!(
            released[0].1.expect("a gap"),
            Gap {
                expected: 1,
                seen: 2
            }
        );
        assert!(
            released[1..].iter().all(|(_, gap)| gap.is_none()),
            "3 and 4 followed 2 in order"
        );
        assert_eq!(r.held(), 0, "the hold drained behind the forced release");
    }

    #[test]
    fn a_repeated_number_is_delivered_and_does_not_move_the_hold() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 4, 1024);
        let mut released = items(r.admit("a", None, Some(3), 30));
        assert_eq!(released, [30], "the position starts at the first number");

        // 5 arrives twice while 4 is still missing. The first copy takes the
        // slot; the second is delivered rather than displacing it, and the
        // hold counts one transfer, not two.
        assert!(r.admit("a", None, Some(5), 50).next().is_none());
        released.extend(items(r.admit("a", None, Some(5), 51)));
        assert_eq!(r.held(), 1, "a repeat must not inflate the hold");

        // 4 fills the hole and the run goes out; every item offered came back
        // exactly once, so no transfer was dropped on the floor.
        released.extend(items(r.admit("a", None, Some(4), 40)));
        released.sort_unstable();
        assert_eq!(released, [30, 40, 50, 51]);
        assert_eq!(r.held(), 0, "the hold must drain");
    }

    #[test]
    fn the_hold_never_exceeds_its_cap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 8, 1024);
        r.admit("a", None, Some(0), 0);
        // A producer whose 1 never arrives: every later number is held until
        // the cap forces a release, and the cap is never exceeded.
        for seq in 2..500u64 {
            r.admit("a", None, Some(seq), seq);
            assert!(r.held() <= 8, "the hold must be bounded by its cap");
        }
    }

    #[test]
    fn one_stalled_scope_does_not_spend_another_scope_s_budget() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 4, 1024);
        r.admit("stalled", None, Some(0), 0);
        r.admit("busy", None, Some(0), 100);
        // The stalled scope parks two transfers behind a hole.
        r.admit("stalled", None, Some(2), 2);
        r.admit("stalled", None, Some(3), 3);
        // The busy scope parks two more, filling the hold, and then one more.
        r.admit("busy", None, Some(102), 102);
        r.admit("busy", None, Some(103), 103);
        let released: Vec<_> = r.admit("busy", None, Some(104), 104).collect();
        // The widest holder pays: both scopes hold two, so the eviction comes
        // from one of them and the hold stays inside its cap either way.
        assert!(!released.is_empty(), "the cap must release something");
        assert!(r.held() <= 4);
    }

    #[test]
    fn an_unnumbered_or_late_arrival_passes_straight_through() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 1024);
        // Nothing names it, so nothing can be ordered against it.
        assert_eq!(items(r.admit("a", None, None, 7)), [7]);
        assert_eq!(items(r.admit("a", None, Some(5), 5)), [5]);
        // Below the position: its successors already went to the application.
        assert_eq!(items(r.admit("a", None, Some(4), 4)), [4]);
        assert_eq!(r.held(), 0);
    }

    #[test]
    fn a_topic_holds_its_own_position_under_its_path() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 1024);
        assert_eq!(items(r.admit("/a", None, Some(0), 0)), [0]);
        // A hole on the path without a topic holds only that scope.
        assert!(items(r.admit("/a", None, Some(2), 2)).is_empty());
        // The topic's own run is untouched by the hole.
        assert_eq!(items(r.admit("/a", Some("t"), Some(6), 6)), [6]);
        assert_eq!(r.held(), 1);
    }

    #[test]
    fn the_reassembler_scope_table_stops_growing_at_its_cap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 4);
        for i in 0..100u64 {
            // Each scope's second number is a hole, so an untracked scope
            // would be visible as a hold that should not exist.
            r.admit(&format!("scope-{i}"), None, Some(0), i);
            r.admit(&format!("scope-{i}"), None, Some(2), i);
        }
        assert_eq!(
            r.tracked_scopes(),
            4,
            "a peer must not be able to size this table"
        );
        assert_eq!(r.held(), 4, "only the tracked scopes hold anything");
    }

    #[test]
    fn an_idle_scope_makes_room_and_a_scope_holding_transfers_is_kept() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 3);
        // "held" releases 0 and then parks 2 behind the hole at 1.
        assert_eq!(items(r.admit("held", None, Some(0), 0)), [0]);
        assert!(items(r.admit("held", None, Some(2), 2)).is_empty());
        assert_eq!(items(r.admit("idle-a", None, Some(0), 10)), [10]);
        assert_eq!(items(r.admit("idle-b", None, Some(0), 20)), [20]);
        // The table is full. "held" is the oldest scope but holds a transfer,
        // so "idle-a", the least recently used scope with nothing pending,
        // makes room for "fresh".
        assert_eq!(items(r.admit("fresh", None, Some(0), 30)), [30]);
        assert_eq!(r.tracked_scopes(), 3);
        assert_eq!(r.held(), 1, "the holder keeps its transfer");
        // The new scope reorders: 2 waits for 1.
        assert!(items(r.admit("fresh", None, Some(2), 32)).is_empty());
        assert_eq!(items(r.admit("fresh", None, Some(1), 31)), [31, 32]);
        // The holder still releases its transfer once the hole fills.
        assert_eq!(items(r.admit("held", None, Some(1), 1)), [1, 2]);
        assert_eq!(r.held(), 0);
    }

    #[test]
    fn a_full_table_of_holders_still_refuses_a_new_scope() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 2);
        for scope in ["a", "b"] {
            assert_eq!(items(r.admit(scope, None, Some(0), 0)), [0]);
            assert!(items(r.admit(scope, None, Some(2), 2)).is_empty());
        }
        // Both scopes hold a transfer, so neither may be dropped: the new
        // scope is passed through untracked.
        assert_eq!(items(r.admit("c", None, Some(0), 9)), [9]);
        assert_eq!(r.tracked_scopes(), 2);
        assert_eq!(r.held(), 2);
    }
}
