//! What a fetch orders of which seeder, and when: no I/O, every clock reading
//! passed in ([decisions/0041](../../../../docs/decisions/0041-swarm-sessions.md)
//! §4.3).
//!
//! Each seeder (a *holder*) gets a credit of `per_peer` orders, held from the
//! moment an order is planned until its answer is taken ([`Scheduler::finish`],
//! which the fetch calls when it hands the piece to its caller). How many of
//! those it uses, its *depth*, is libtorrent's
//! `peer_connection::update_desired_queue_size`: [`MIN_DEPTH`] at first, one
//! more per piece in slow start, then [`QUEUE_TIME`] of the seeder's measured
//! rate in pieces.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;

use super::gather::FetchOptions;
use super::{Ask, Bits, Blob};

/// The fewest orders a seeder is given at once: libtorrent's
/// `min_request_queue`.
pub(super) const MIN_DEPTH: usize = 2;

/// How much of a seeder's rate is kept ordered ahead: libtorrent's
/// `request_queue_time`.
pub(super) const QUEUE_TIME: Duration = Duration::from_secs(3);

/// Slow start ends once a second's payload is no more than this many bytes
/// above the second before: libtorrent's slow-start exit.
pub(super) const SLOW_START_GAIN: u64 = 5000;

/// What one seeder looks like to the scheduler.
struct Holder {
    /// The pieces it holds, once it has said.
    have: Option<Bits>,
    have_at: Option<Instant>,
    asking_have: bool,
    /// Orders it holds credit for, from planning to taking.
    busy: usize,
    /// The pieces ordered of it.
    mine: HashSet<u32>,
    /// How many orders it is given at once, at most `per_peer`.
    depth: usize,
    slow_start: bool,
    /// The start of the one-second bucket `this_second` fills, anchored at
    /// the first piece.
    second_start: Option<Instant>,
    this_second: u64,
    /// The payload of the last closed second.
    last_second: u64,
}

impl Holder {
    fn new(per_peer: usize) -> Holder {
        Holder {
            have: None,
            have_at: None,
            asking_have: false,
            busy: 0,
            mine: HashSet::new(),
            depth: MIN_DEPTH.min(per_peer),
            slow_start: true,
            second_start: None,
            this_second: 0,
            last_second: 0,
        }
    }

    /// Counts a piece of `bytes` that arrived at `at`, and sets the depth
    /// as libtorrent does.
    fn arrived(&mut self, bytes: u64, at: Instant, per_peer: usize, piece: u64) {
        let start = *self.second_start.get_or_insert(at);
        let elapsed = at.saturating_duration_since(start).as_secs();
        if elapsed > 0 {
            // The bucket closes, and after a gap of two seconds or more one
            // empty second follows it; older empty seconds change nothing.
            let closed = [self.this_second, 0];
            for bytes in closed.into_iter().take(if elapsed >= 2 { 2 } else { 1 }) {
                let older = self.last_second;
                self.last_second = bytes;
                if self.slow_start
                    && older > 0
                    && self.last_second <= older.saturating_add(SLOW_START_GAIN)
                {
                    self.slow_start = false;
                }
            }
            self.second_start = Some(start + Duration::from_secs(elapsed));
            self.this_second = 0;
        }
        self.this_second = self.this_second.saturating_add(bytes);
        self.depth = if self.slow_start {
            (self.depth + 1).min(per_peer)
        } else {
            let wanted = self
                .last_second
                .saturating_mul(QUEUE_TIME.as_secs())
                .div_ceil(piece.max(1));
            usize::try_from(wanted)
                .unwrap_or(usize::MAX)
                .clamp(MIN_DEPTH.min(per_peer), per_peer)
        };
    }
}

/// The orders of one fetch: which piece of which seeder, under each
/// seeder's credit and the fetch's `max_inflight`.
pub(super) struct Scheduler {
    per_peer: usize,
    max_inflight: usize,
    have_refresh: Duration,
    /// The blob's piece size in bytes.
    piece: u64,
    delivered: Bits,
    list_known: bool,
    /// A LIST is ordered.
    listing: bool,
    /// Keyed by the URL as dialled.
    holders: HashMap<Arc<str>, Holder>,
    /// The orders of each piece.
    inflight: HashMap<u32, Vec<u64>>,
    orders: HashMap<u64, (Arc<str>, Ask)>,
    next_order: u64,
}

impl Scheduler {
    /// A scheduler for `blob`, of which the caller holds `delivered`;
    /// `list_known` when the piece list needs no LIST.
    pub(super) fn new(
        blob: Blob,
        delivered: Bits,
        list_known: bool,
        options: &FetchOptions,
    ) -> Scheduler {
        Scheduler {
            per_peer: options.per_peer.max(1),
            max_inflight: options.max_inflight.max(1),
            have_refresh: options.have_refresh,
            piece: blob.piece().bytes(),
            delivered,
            list_known,
            listing: false,
            holders: HashMap::new(),
            inflight: HashMap::new(),
            orders: HashMap::new(),
            next_order: 0,
        }
    }

    /// Adds a seeder, in slow start at [`MIN_DEPTH`].
    pub(super) fn add(&mut self, url: Arc<str>) {
        let per_peer = self.per_peer;
        self.holders
            .entry(url)
            .or_insert_with(|| Holder::new(per_peer));
    }

    /// Forgets a seeder and its orders; returns the ids of those orders.
    pub(super) fn remove(&mut self, url: &str) -> Vec<u64> {
        let ids = self.release(url);
        self.holders.remove(url);
        ids
    }

    /// Forgets a seeder's orders, which frees their pieces for others, and
    /// keeps the seeder; returns the ids of those orders.
    pub(super) fn release(&mut self, url: &str) -> Vec<u64> {
        let ids: Vec<u64> = self
            .orders
            .iter()
            .filter(|(_, (owner, _))| &**owner == url)
            .map(|(&id, _)| id)
            .collect();
        for &id in &ids {
            self.finish(id, None);
        }
        ids
    }

    /// The seeder and ask of order `id`, while it holds its credit.
    pub(super) fn order(&self, id: u64) -> Option<(Arc<str>, Ask)> {
        self.orders.get(&id).cloned()
    }

    /// Every order there is credit for now: LIST once of any seeder, HAVE
    /// first of each, then claims, then a HAVE again of a seeder that has
    /// been idle for `have_refresh`. Each is recorded as ordered.
    pub(super) fn plan(&mut self, now: Instant, rng: &mut impl Rng) -> Vec<(u64, Arc<str>, Ask)> {
        let mut out = Vec::new();
        let urls: Vec<Arc<str>> = self.holders.keys().cloned().collect();

        if !self.list_known
            && !self.listing
            && let Some(url) = urls.iter().find(|url| self.room(url))
        {
            out.push(self.record(url, Ask::List));
        }

        for url in &urls {
            let holder = &self.holders[url];
            if holder.have.is_none() && !holder.asking_have && self.room(url) {
                out.push(self.record(url, Ask::Have));
            }
        }

        if self.list_known {
            for url in &urls {
                while self.room(url) {
                    let holder = &self.holders[url];
                    let Some(have) = holder.have.as_ref() else {
                        break;
                    };
                    match claim(&self.delivered, have, &self.inflight, &holder.mine, rng) {
                        Some(index) => out.push(self.record(url, Ask::Piece(index))),
                        None => break,
                    }
                }
            }
        }

        for url in &urls {
            let holder = &self.holders[url];
            let due = holder
                .have_at
                .is_some_and(|at| now >= at + self.have_refresh);
            if due && holder.busy == 0 && self.room(url) {
                out.push(self.record(url, Ask::Have));
            }
        }
        out
    }

    /// Whether `url`'s seeder may be given one more order.
    fn room(&self, url: &str) -> bool {
        self.orders.len() < self.max_inflight
            && self
                .holders
                .get(url)
                .is_some_and(|holder| holder.busy < holder.depth)
    }

    /// Records one order of `url`'s seeder.
    fn record(&mut self, url: &Arc<str>, ask: Ask) -> (u64, Arc<str>, Ask) {
        let id = self.next_order;
        self.next_order += 1;
        let holder = self
            .holders
            .get_mut(url)
            .expect("orders go to a known seeder");
        holder.busy += 1;
        match ask {
            Ask::List => self.listing = true,
            Ask::Have => holder.asking_have = true,
            Ask::Piece(index) => {
                holder.mine.insert(index);
                self.inflight.entry(index).or_default().push(id);
            }
        }
        self.orders.insert(id, (Arc::clone(url), ask));
        (id, Arc::clone(url), ask)
    }

    /// Ends order `id` and releases its credit: its answer was taken, or it
    /// failed. `piece` is the payload length of a piece answer and when it
    /// arrived, which sets its seeder's depth. `None` for an order this
    /// scheduler no longer holds.
    pub(super) fn finish(
        &mut self,
        id: u64,
        piece: Option<(u64, Instant)>,
    ) -> Option<(Arc<str>, Ask)> {
        let (url, ask) = self.orders.remove(&id)?;
        match ask {
            Ask::List => self.listing = false,
            Ask::Have => {}
            Ask::Piece(index) => {
                if let Some(ids) = self.inflight.get_mut(&index) {
                    ids.retain(|&other| other != id);
                    if ids.is_empty() {
                        self.inflight.remove(&index);
                    }
                }
            }
        }
        if let Some(holder) = self.holders.get_mut(&url) {
            holder.busy -= 1;
            match ask {
                Ask::Have => holder.asking_have = false,
                Ask::Piece(index) => {
                    holder.mine.remove(&index);
                }
                Ask::List => {}
            }
            if let Some((bytes, at)) = piece {
                holder.arrived(bytes, at, self.per_peer, self.piece);
            }
        }
        Some((url, ask))
    }

    /// Marks piece `index` delivered. `None` if it already was; otherwise the
    /// other orders of that piece, which are no longer needed.
    pub(super) fn deliver(&mut self, index: u32) -> Option<Vec<u64>> {
        if self.delivered.get(index) {
            return None;
        }
        self.delivered.set(index, true);
        Some(self.inflight.get(&index).cloned().unwrap_or_default())
    }

    /// Records what `url`'s seeder holds, as of `now`.
    pub(super) fn set_have(&mut self, url: &str, have: Bits, now: Instant) {
        if let Some(holder) = self.holders.get_mut(url) {
            holder.have = Some(have);
            holder.have_at = Some(now);
        }
    }

    /// The piece list is known: pieces can be claimed.
    pub(super) fn set_list_known(&mut self) {
        self.list_known = true;
    }

    /// The pieces the caller holds.
    pub(super) fn delivered(&self) -> &Bits {
        &self.delivered
    }

    /// The pieces not yet delivered with at least one order.
    pub(super) fn in_flight(&self) -> Bits {
        let mut bits = Bits::new(self.delivered.len());
        for (&index, ids) in &self.inflight {
            if !ids.is_empty() && !self.delivered.get(index) {
                bits.set(index, true);
            }
        }
        bits
    }

    /// Whether no order holds credit.
    pub(super) fn idle(&self) -> bool {
        self.orders.is_empty()
    }

    /// When the next idle seeder is due to be asked HAVE again.
    pub(super) fn next_refresh(&self) -> Option<Instant> {
        self.holders
            .values()
            .filter(|holder| holder.busy == 0)
            .filter_map(|holder| holder.have_at)
            .min()
            .map(|at| at + self.have_refresh)
    }
}

/// The next piece one seeder should fetch, or `None`.
///
/// Candidates are the pieces the caller lacks, the seeder holds and the
/// seeder is not already fetching. A candidate no one is fetching wins,
/// chosen uniformly. Only when **every** missing piece is in flight
/// somewhere (the endgame) does a seeder fetch a second copy, of the
/// candidate with the fewest copies, so a slow seeder cannot hold up the
/// last pieces.
fn claim(
    delivered: &Bits,
    have: &Bits,
    inflight: &HashMap<u32, Vec<u64>>,
    mine: &HashSet<u32>,
    rng: &mut impl Rng,
) -> Option<u32> {
    let mut free = (0u32, None);
    let mut least = (usize::MAX, 0u32, None);
    let mut all_in_flight = true;
    for index in 0..delivered.len() {
        if delivered.get(index) {
            continue;
        }
        let copies = inflight.get(&index).map_or(0, Vec::len);
        if copies == 0 {
            all_in_flight = false;
        }
        if !have.get(index) || mine.contains(&index) {
            continue;
        }
        // Reservoir sampling: each candidate is kept with probability
        // 1 / (candidates seen), which is uniform without a list.
        if copies == 0 {
            free.0 += 1;
            if rng.random_range(0..free.0) == 0 {
                free.1 = Some(index);
            }
        } else if copies < least.0 {
            least = (copies, 1, Some(index));
        } else if copies == least.0 {
            least.1 += 1;
            if rng.random_range(0..least.1) == 0 {
                least.2 = Some(index);
            }
        }
    }
    match free.1 {
        Some(index) => Some(index),
        None if all_in_flight => least.2,
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::super::{BlobId, PieceSize};
    use super::*;

    fn bits(len: u32, set: impl IntoIterator<Item = u32>) -> Bits {
        let mut bits = Bits::new(len);
        for index in set {
            bits.set(index, true);
        }
        bits
    }

    fn flight(entries: &[(u32, usize)]) -> HashMap<u32, Vec<u64>> {
        entries
            .iter()
            .map(|&(index, copies)| (index, (0..copies as u64).collect()))
            .collect()
    }

    /// A scheduler for 64 pieces of `piece`, the list known, with one seeder
    /// `a` that holds every piece.
    fn one_holder(piece: PieceSize, per_peer: usize, now: Instant) -> Scheduler {
        let blob = Blob::new(BlobId::from_bytes([7; 32]), 64 * piece.bytes(), piece)
            .expect("a valid blob");
        let options = FetchOptions {
            per_peer,
            ..FetchOptions::default()
        };
        let mut scheduler = Scheduler::new(blob, Bits::new(64), true, &options);
        scheduler.add(Arc::from("a"));
        scheduler.set_have("a", Bits::full(64), now);
        scheduler
    }

    /// Takes the oldest open order's piece of `bytes`, arrived at `at`: the
    /// fetch's caller took it. Plans first, so `open` holds what was ordered.
    fn take(
        scheduler: &mut Scheduler,
        open: &mut Vec<u64>,
        bytes: u64,
        at: Instant,
        rng: &mut StdRng,
    ) {
        open.extend(scheduler.plan(at, rng).into_iter().map(|(id, _, _)| id));
        let id = open.remove(0);
        let (_, ask) = scheduler
            .finish(id, Some((bytes, at)))
            .expect("an open order");
        let Ask::Piece(index) = ask else {
            panic!("only pieces are ordered here: {ask:?}");
        };
        assert_eq!(scheduler.deliver(index), Some(Vec::new()));
    }

    #[test]
    fn an_order_is_written_only_when_a_piece_is_taken() {
        let mut rng = StdRng::seed_from_u64(6);
        let t = Instant::now();
        let mut scheduler = one_holder(PieceSize::MIN, 4, t);
        let first = scheduler.plan(t, &mut rng);
        assert_eq!(first.len(), MIN_DEPTH, "slow start begins at {MIN_DEPTH}");
        for (id, _, ask) in first {
            scheduler.finish(id, Some((1024, t))).expect("ordered");
            let Ask::Piece(index) = ask else {
                panic!("{ask:?}");
            };
            scheduler.deliver(index).expect("first copy");
        }
        // Two pieces taken in slow start: the depth is at the cap of 4.
        let second = scheduler.plan(t, &mut rng);
        assert_eq!(second.len(), 4);
        // Nothing taken, nothing more ordered: the credit is spent.
        assert!(scheduler.plan(t, &mut rng).is_empty());
        assert_eq!(scheduler.in_flight().ones(), 4);
        let (id, _, ask) = second[0];
        scheduler.finish(id, Some((1024, t))).expect("ordered");
        let Ask::Piece(index) = ask else {
            panic!("{ask:?}");
        };
        scheduler.deliver(index).expect("first copy");
        assert_eq!(
            scheduler.plan(t, &mut rng).len(),
            1,
            "one piece taken, one order"
        );
    }

    #[test]
    fn the_depth_grows_in_slow_start_then_follows_the_rate() {
        const MIB: u64 = 1 << 20;
        let ms = Duration::from_millis;
        let mut rng = StdRng::seed_from_u64(7);
        let t0 = Instant::now();

        // Slow start: one more per piece.
        let mut scheduler = one_holder(PieceSize::DEFAULT, 16, t0);
        let mut open = Vec::new();
        for at in [0, 100, 200] {
            take(&mut scheduler, &mut open, MIB, t0 + ms(at), &mut rng);
        }
        assert_eq!(scheduler.holders["a"].depth, MIN_DEPTH + 3);

        // Two seconds of 4 MiB each: the rate stopped growing, slow start
        // ends at the first piece of the third second, and the depth is
        // three seconds of 4 MiB in 1 MiB pieces.
        let mut scheduler = one_holder(PieceSize::DEFAULT, 16, t0);
        let mut open = Vec::new();
        for at in [0, 100, 200, 300, 1000, 1100, 1200, 1300] {
            take(&mut scheduler, &mut open, MIB, t0 + ms(at), &mut rng);
        }
        assert!(scheduler.holders["a"].slow_start);
        assert_eq!(scheduler.holders["a"].depth, MIN_DEPTH + 8);
        take(&mut scheduler, &mut open, MIB / 2, t0 + ms(2000), &mut rng);
        assert!(!scheduler.holders["a"].slow_start);
        assert_eq!(scheduler.holders["a"].depth, 12);

        // Out of slow start, a closed second of 512 KiB is 1.5 pieces in
        // three seconds: the depth falls to its floor.
        take(&mut scheduler, &mut open, MIB, t0 + ms(3000), &mut rng);
        assert_eq!(scheduler.holders["a"].depth, MIN_DEPTH);
    }

    #[test]
    fn a_claim_is_a_missing_piece_the_seeder_holds() {
        let mut rng = StdRng::seed_from_u64(1);
        let delivered = bits(16, [0, 1, 2, 3]);
        let have = bits(16, [2, 3, 5, 9, 11]);
        for _ in 0..200 {
            let index = claim(
                &delivered,
                &have,
                &HashMap::new(),
                &HashSet::new(),
                &mut rng,
            )
            .expect("the seeder holds missing pieces");
            assert!([5, 9, 11].contains(&index), "claimed {index}");
        }
        // Holding only what the caller has is nothing to claim.
        let have = bits(16, [0, 1]);
        assert_eq!(
            claim(
                &delivered,
                &have,
                &HashMap::new(),
                &HashSet::new(),
                &mut rng
            ),
            None
        );
    }

    #[test]
    fn a_piece_no_one_fetches_wins_over_a_second_copy() {
        let mut rng = StdRng::seed_from_u64(2);
        let delivered = Bits::new(8);
        let have = Bits::full(8);
        // Every piece but 6 is in flight elsewhere.
        let inflight = flight(&[(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (7, 1)]);
        for _ in 0..100 {
            assert_eq!(
                claim(&delivered, &have, &inflight, &HashSet::new(), &mut rng),
                Some(6)
            );
        }
    }

    #[test]
    fn the_endgame_starts_only_when_every_missing_piece_is_in_flight() {
        let mut rng = StdRng::seed_from_u64(3);
        let delivered = bits(4, [0]);
        // This seeder holds 1 and 2, both in flight elsewhere; piece 3 is
        // missing and nobody fetches it, but this seeder cannot.
        let have = bits(4, [1, 2]);
        let inflight = flight(&[(1, 1), (2, 1)]);
        assert_eq!(
            claim(&delivered, &have, &inflight, &HashSet::new(), &mut rng),
            None
        );
        // Once 3 is in flight too, the endgame lets this seeder duplicate.
        let inflight = flight(&[(1, 1), (2, 1), (3, 1)]);
        let index = claim(&delivered, &have, &inflight, &HashSet::new(), &mut rng);
        assert!(matches!(index, Some(1 | 2)), "{index:?}");
    }

    #[test]
    fn the_endgame_duplicates_the_least_copied_piece() {
        let mut rng = StdRng::seed_from_u64(4);
        let delivered = Bits::new(4);
        let have = Bits::full(4);
        let inflight = flight(&[(0, 3), (1, 1), (2, 2), (3, 1)]);
        let mut seen = HashSet::new();
        for _ in 0..200 {
            let index = claim(&delivered, &have, &inflight, &HashSet::new(), &mut rng)
                .expect("the endgame claims");
            assert!(index == 1 || index == 3, "claimed {index}");
            seen.insert(index);
        }
        assert_eq!(seen.len(), 2, "ties are broken at random");
    }

    #[test]
    fn a_seeder_never_fetches_the_same_piece_twice() {
        let mut rng = StdRng::seed_from_u64(5);
        let delivered = Bits::new(4);
        let have = Bits::full(4);
        let mut inflight = HashMap::new();
        let mut mine = HashSet::new();
        for id in 0..4u64 {
            let index =
                claim(&delivered, &have, &inflight, &mine, &mut rng).expect("a piece is left");
            assert!(mine.insert(index), "claimed {index} twice");
            inflight.entry(index).or_insert_with(Vec::new).push(id);
        }
        // Every piece is in flight on this seeder alone: the endgame has no
        // candidate here.
        assert_eq!(claim(&delivered, &have, &inflight, &mine, &mut rng), None);
    }
}
