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

use std::collections::{BTreeMap, HashMap, btree_map};
use std::sync::Mutex;

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

/// Assigns sequence numbers per scope: the dialled path, or the topic of a
/// published copy.
///
/// The producer itself is the connection — the peer's proved fingerprint is
/// what names it, so nothing goes on the wire for it
/// ([decision 0008](../../../docs/decisions/0008-session-identity.md) §4.4) —
/// which is why one sequencer lives per connection.
pub(crate) struct Sequencer {
    enabled: bool,
    counters: Mutex<HashMap<Box<str>, u64>>,
}

impl Sequencer {
    pub(crate) fn new(ordering: OrderingMode) -> Sequencer {
        Sequencer {
            enabled: ordering != OrderingMode::None,
            counters: Mutex::new(HashMap::new()),
        }
    }

    /// The next number for `scope`, or `None` when ordering is off.
    ///
    /// Numbers start at zero and are dense per scope; a receiver reads a
    /// missing number as a gap.
    pub(crate) fn next(&self, scope: &str) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        let mut counters = self.counters.lock().expect("sequencer poisoned");
        let next = counters.entry_ref_or_insert(scope);
        Some(next)
    }

    #[cfg(test)]
    fn tracked_scopes(&self) -> usize {
        self.counters.lock().expect("sequencer poisoned").len()
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        self.counters.lock().expect("sequencer poisoned").capacity() > 0
    }
}

/// Small extension so the hot path does one lookup, not two.
trait CounterMap {
    fn entry_ref_or_insert(&mut self, scope: &str) -> u64;
}

impl CounterMap for HashMap<Box<str>, u64> {
    fn entry_ref_or_insert(&mut self, scope: &str) -> u64 {
        match self.get_mut(scope) {
            Some(counter) => {
                let issued = *counter;
                *counter = counter.wrapping_add(1);
                issued
            }
            None => {
                self.insert(scope.into(), 1);
                0
            }
        }
    }
}

/// Tracks the next expected number per scope and reports what is missing.
pub(crate) struct GapDetector {
    enabled: bool,
    /// Bounded by `max_sequence_scopes`: the scopes are the peer's paths and
    /// topics, so the table is remote-influenced and needs a named cap
    /// (`docs/INVARIANTS.md`).
    max_scopes: usize,
    expected: Mutex<HashMap<Box<str>, u64>>,
}

impl GapDetector {
    pub(crate) fn new(ordering: OrderingMode, max_scopes: usize) -> GapDetector {
        GapDetector {
            // Detect mode only: under `reassemble` the reassembler owns the
            // position and reports what it had to skip, and two components
            // tracking the same counter would report the same hole twice.
            enabled: ordering == OrderingMode::PerProducerDetect,
            max_scopes,
            expected: Mutex::new(HashMap::new()),
        }
    }

    /// Records `sequence` for `scope` and reports a gap if one opened.
    ///
    /// A number below the expected one is a repeat or a reordering, not a
    /// gap: detect mode reports what is missing and never reorders, so an
    /// out-of-order arrival lowers nothing and is delivered as it came.
    pub(crate) fn observe(&self, scope: &str, sequence: u64) -> Option<Gap> {
        if !self.enabled {
            return None;
        }
        let mut expected = self.expected.lock().expect("gap detector poisoned");
        let next = expected.get_mut(scope);
        match next {
            Some(next) => {
                let gap = (sequence > *next).then_some(Gap {
                    expected: *next,
                    seen: sequence,
                });
                // A number below the expected one carries no information
                // about what is still to come: advancing on it would consume
                // a number that has not arrived and silence its gap.
                if sequence >= *next {
                    *next = sequence.wrapping_add(1);
                }
                gap
            }
            None => {
                if expected.len() >= self.max_scopes {
                    // At the cap the detector stops tracking new scopes
                    // rather than growing: a peer that invents paths must not
                    // be able to size this table.
                    return None;
                }
                expected.insert(scope.into(), sequence.wrapping_add(1));
                // The first number seen for a scope establishes the position;
                // a connection may legitimately start anywhere, because the
                // peer's counter is older than this connection's view of it.
                None
            }
        }
    }

    #[cfg(test)]
    fn allocated(&self) -> bool {
        self.expected
            .lock()
            .expect("gap detector poisoned")
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
    scopes: HashMap<Box<str>, Scope<T>>,
    /// Held entries over all scopes, kept here so the cap is one comparison.
    held: usize,
}

struct Scope<T> {
    /// The number that must arrive before anything after it is released.
    next: u64,
    pending: BTreeMap<u64, T>,
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
            }),
        }
    }

    /// Whether anything is reassembled at all. The caller checks this before
    /// building the argument list, so a connection that negotiated `None` or
    /// `detect` never touches this structure and never allocates the `Vec`.
    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Offers one arrival and returns what may now be delivered, in order.
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
        scope: &str,
        sequence: Option<u64>,
        item: T,
    ) -> Vec<(T, Option<Gap>)> {
        // Defensive: callers gate on `enabled()` so that an off connection
        // never even builds the argument list, but an off reassembler must
        // hold nothing if it is called anyway.
        if !self.enabled {
            return vec![(item, None)];
        }
        let Some(sequence) = sequence else {
            return vec![(item, None)];
        };
        let mut state = self.state.lock().expect("reassembler poisoned");
        let Reorder { scopes, held } = &mut *state;
        if !scopes.contains_key(scope) {
            if scopes.len() >= self.max_scopes {
                // At the scope cap nothing is tracked and nothing is held: a
                // peer that invents scopes must not be able to size this
                // table.
                return vec![(item, None)];
            }
            // The first number seen establishes the position: the peer's
            // counter is older than this connection's view of it.
            scopes.insert(
                scope.into(),
                Scope {
                    next: sequence,
                    pending: BTreeMap::new(),
                },
            );
        }

        let entry = scopes.get_mut(scope).expect("present");
        if sequence < entry.next {
            // Late: its successors have already gone to the application, so
            // holding it would order it against nothing.
            return vec![(item, None)];
        }
        if sequence == entry.next && entry.pending.is_empty() {
            // The common case, and the whole hot path of reassemble mode: the
            // next number arrived and nothing is waiting behind it, so it goes
            // straight out without a map node.
            entry.next = sequence.wrapping_add(1);
            return vec![(item, None)];
        }
        match entry.pending.entry(sequence) {
            btree_map::Entry::Occupied(_) => return vec![(item, None)],
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
        out
    }

    #[cfg(test)]
    fn held(&self) -> usize {
        self.state.lock().expect("reassembler poisoned").held
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

/// The scope holding the most transfers: the one to take a slot from when the
/// hold is full.
fn widest_scope<T>(scopes: &mut HashMap<Box<str>, Scope<T>>) -> Option<&mut Scope<T>> {
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
        let sequencer = Sequencer::new(OrderingMode::None);
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
            assert_eq!(detector.observe("/md", seq), None);
        }
        assert!(
            !detector.allocated(),
            "an off detector must not allocate a table"
        );
    }

    #[test]
    fn numbers_are_dense_and_per_scope() {
        let sequencer = Sequencer::new(OrderingMode::PerProducerDetect);
        assert_eq!(sequencer.next("a"), Some(0));
        assert_eq!(sequencer.next("a"), Some(1));
        assert_eq!(sequencer.next("b"), Some(0));
        assert_eq!(sequencer.next("a"), Some(2));
        assert_eq!(sequencer.tracked_scopes(), 2);
    }

    #[test]
    fn a_missing_number_is_a_gap_of_that_size() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", 0), None);
        assert_eq!(detector.observe("a", 1), None);
        // 2 and 3 never arrive.
        let gap = detector.observe("a", 4).expect("a gap");
        assert_eq!(
            gap,
            Gap {
                expected: 2,
                seen: 4
            }
        );
        assert_eq!(gap.missed(), 2);
        // The detector resynchronizes: the next number in order is clean.
        assert_eq!(detector.observe("a", 5), None);
    }

    #[test]
    fn scopes_do_not_interfere() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", 0), None);
        assert_eq!(detector.observe("b", 0), None);
        assert_eq!(detector.observe("a", 1), None);
        assert_eq!(detector.observe("b", 7).expect("gap").missed(), 6);
    }

    #[test]
    fn an_out_of_order_arrival_is_not_a_gap() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 1024);
        assert_eq!(detector.observe("a", 0), None);
        assert_eq!(detector.observe("a", 2).expect("gap").missed(), 1);
        // 1 arrives late: detect mode delivers it and reports no new gap,
        // because nothing further is missing.
        assert_eq!(detector.observe("a", 1), None);
        // And it did not consume 3: a late arrival moves the position
        // nowhere, so a later hole is still reported.
        assert_eq!(detector.observe("a", 4).expect("gap").missed(), 1);
    }

    #[test]
    fn the_scope_table_stops_growing_at_its_cap() {
        let detector = GapDetector::new(OrderingMode::PerProducerDetect, 4);
        for i in 0..100 {
            detector.observe(&format!("scope-{i}"), 0);
        }
        assert_eq!(
            detector.expected.lock().expect("poisoned").len(),
            4,
            "a peer must not be able to size this table"
        );
    }

    /// Shorthand: the items released by one arrival, without their gaps.
    fn items(released: Vec<(u64, Option<Gap>)>) -> Vec<u64> {
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
                assert_eq!(items(reassembler.admit("/md", Some(999 - seq), seq)), [seq]);
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
        assert_eq!(items(r.admit("a", Some(0), 0)), [0]);
        // 1 is missing, so 2 and 3 wait for it.
        assert!(items(r.admit("a", Some(2), 2)).is_empty());
        assert!(items(r.admit("a", Some(3), 3)).is_empty());
        assert_eq!(r.held(), 2);
        // 1 arrives and the whole run goes out at once, in order.
        assert_eq!(items(r.admit("a", Some(1), 1)), [1, 2, 3]);
        assert_eq!(r.held(), 0, "the hold must drain");
    }

    #[test]
    fn a_run_released_in_order_carries_no_gap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 1024);
        r.admit("a", Some(0), 0);
        r.admit("a", Some(2), 2);
        let released = r.admit("a", Some(1), 1);
        assert!(
            released.iter().all(|(_, gap)| gap.is_none()),
            "nothing was missed: the hole was filled"
        );
    }

    #[test]
    fn the_hold_releases_out_of_order_at_its_cap_and_reports_the_gap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 2, 1024);
        assert_eq!(items(r.admit("a", Some(0), 0)), [0]);
        // 1 never arrives; 2 and 3 fill the hold.
        assert!(r.admit("a", Some(2), 2).is_empty());
        assert!(r.admit("a", Some(3), 3).is_empty());
        // 4 does not fit: the oldest held transfer is released out of order
        // and says what it skipped, which is exactly what detect mode would
        // have reported. 3 and 4 follow it in order behind the forced
        // release, so the hole at 1 costs one out-of-order delivery and not
        // the whole run.
        let released = r.admit("a", Some(4), 4);
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
        let mut released = items(r.admit("a", Some(3), 30));
        assert_eq!(released, [30], "the position starts at the first number");

        // 5 arrives twice while 4 is still missing. The first copy takes the
        // slot; the second is delivered rather than displacing it, and the
        // hold counts one transfer, not two.
        assert!(r.admit("a", Some(5), 50).is_empty());
        released.extend(items(r.admit("a", Some(5), 51)));
        assert_eq!(r.held(), 1, "a repeat must not inflate the hold");

        // 4 fills the hole and the run goes out; every item offered came back
        // exactly once, so no transfer was dropped on the floor.
        released.extend(items(r.admit("a", Some(4), 40)));
        released.sort_unstable();
        assert_eq!(released, [30, 40, 50, 51]);
        assert_eq!(r.held(), 0, "the hold must drain");
    }

    #[test]
    fn the_hold_never_exceeds_its_cap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 8, 1024);
        r.admit("a", Some(0), 0);
        // A producer whose 1 never arrives: every later number is held until
        // the cap forces a release, and the cap is never exceeded.
        for seq in 2..500u64 {
            r.admit("a", Some(seq), seq);
            assert!(r.held() <= 8, "the hold must be bounded by its cap");
        }
    }

    #[test]
    fn one_stalled_scope_does_not_spend_another_scope_s_budget() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 4, 1024);
        r.admit("stalled", Some(0), 0);
        r.admit("busy", Some(0), 100);
        // The stalled scope parks two transfers behind a hole.
        r.admit("stalled", Some(2), 2);
        r.admit("stalled", Some(3), 3);
        // The busy scope parks two more, filling the hold, and then one more.
        r.admit("busy", Some(102), 102);
        r.admit("busy", Some(103), 103);
        let released = r.admit("busy", Some(104), 104);
        // The widest holder pays: both scopes hold two, so the eviction comes
        // from one of them and the hold stays inside its cap either way.
        assert!(!released.is_empty(), "the cap must release something");
        assert!(r.held() <= 4);
    }

    #[test]
    fn an_unnumbered_or_late_arrival_passes_straight_through() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 1024);
        // Nothing names it, so nothing can be ordered against it.
        assert_eq!(items(r.admit("a", None, 7)), [7]);
        assert_eq!(items(r.admit("a", Some(5), 5)), [5]);
        // Below the position: its successors already went to the application.
        assert_eq!(items(r.admit("a", Some(4), 4)), [4]);
        assert_eq!(r.held(), 0);
    }

    #[test]
    fn the_reassembler_scope_table_stops_growing_at_its_cap() {
        let r: Reassembler<u64> = Reassembler::new(OrderingMode::PerProducerReassemble, 256, 4);
        for i in 0..100u64 {
            // Each scope's second number is a hole, so an untracked scope
            // would be visible as a hold that should not exist.
            r.admit(&format!("scope-{i}"), Some(0), i);
            r.admit(&format!("scope-{i}"), Some(2), i);
        }
        assert_eq!(
            r.state.lock().expect("poisoned").scopes.len(),
            4,
            "a peer must not be able to size this table"
        );
        assert_eq!(r.held(), 4, "only the tracked scopes hold anything");
    }
}
