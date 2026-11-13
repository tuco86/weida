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
use std::ops::Range;
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
    /// The [`Scheduler::free_gen`] at which it held none of the free pieces:
    /// until a piece is freed again or it says HAVE anew, it still holds
    /// none, and nobody looks again.
    dry: Option<u64>,
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
            dry: None,
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
///
/// Every step costs at most the orders in flight, except a claim that the
/// free pieces' random probes miss, which reads at most one bit map per
/// availability level ([`RARE_LEVELS`]) once: a fetch of
/// [`super::MAX_PIECES`] pieces stays linear in its pieces rather than
/// scanning all of them for every order.
pub(super) struct Scheduler {
    per_peer: usize,
    max_inflight: usize,
    have_refresh: Duration,
    /// The blob's piece size in bytes.
    piece: u64,
    delivered: Bits,
    /// Pieces not yet delivered.
    missing: u32,
    /// The pieces not yet delivered that no order fetches: what a claim
    /// picks from before the endgame.
    free: Bits,
    free_count: u32,
    /// Bumped whenever a piece becomes free again.
    free_gen: u64,
    /// The free pieces by how many seeders hold them.
    rarity: Rarity,
    /// The pieces ordered before any other ([`Scheduler::set_priority`]).
    priority: Option<Range<u32>>,
    /// The lowest prioritized piece not yet delivered.
    floor: u32,
    /// The running mean time from a piece's order to its arrival.
    piece_time: Option<Duration>,
    list_known: bool,
    /// A LIST is ordered.
    listing: bool,
    /// Keyed by the URL as dialled.
    holders: HashMap<Arc<str>, Holder>,
    /// The orders of each piece.
    inflight: HashMap<u32, Vec<u64>>,
    orders: HashMap<u64, Order>,
    next_order: u64,
}

/// One order: of which seeder, what, and when it was planned.
struct Order {
    url: Arc<str>,
    ask: Ask,
    at: Instant,
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
        let mut free = Bits::full(delivered.len());
        for index in 0..delivered.len() {
            if delivered.get(index) {
                free.set(index, false);
            }
        }
        let missing = delivered.len() - delivered.ones();
        Scheduler {
            per_peer: options.per_peer.max(1),
            max_inflight: options.max_inflight.max(1),
            have_refresh: options.have_refresh,
            piece: blob.piece().bytes(),
            delivered,
            missing,
            rarity: Rarity::new(&free),
            free,
            free_count: missing,
            free_gen: 0,
            priority: None,
            floor: 0,
            piece_time: None,
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
        if let Some(holder) = self.holders.remove(url)
            && let Some(have) = holder.have
        {
            for index in set_indices(have.as_bytes().iter().copied()) {
                self.rarity.unhold(index);
            }
        }
        ids
    }

    /// Forgets a seeder's orders, which frees their pieces for others, and
    /// keeps the seeder; returns the ids of those orders.
    pub(super) fn release(&mut self, url: &str) -> Vec<u64> {
        let ids: Vec<u64> = self
            .orders
            .iter()
            .filter(|(_, order)| &*order.url == url)
            .map(|(&id, _)| id)
            .collect();
        for &id in &ids {
            self.finish(id, None);
        }
        ids
    }

    /// The seeder and ask of order `id`, while it holds its credit.
    pub(super) fn order(&self, id: u64) -> Option<(Arc<str>, Ask)> {
        self.orders
            .get(&id)
            .map(|order| (Arc::clone(&order.url), order.ask))
    }

    /// Every order there is credit for now: LIST once of any seeder, HAVE
    /// first of each, then a late prioritized piece once more, then claims,
    /// then a HAVE again of a seeder that has been idle for `have_refresh`.
    /// Each is recorded as ordered.
    pub(super) fn plan(&mut self, now: Instant, rng: &mut impl Rng) -> Vec<(u64, Arc<str>, Ask)> {
        let mut out = Vec::new();
        let urls: Vec<Arc<str>> = self.holders.keys().cloned().collect();

        if !self.list_known
            && !self.listing
            && let Some(url) = urls.iter().find(|url| self.room(url))
        {
            out.push(self.record(url, Ask::List, now));
        }

        for url in &urls {
            let holder = &self.holders[url];
            if holder.have.is_none() && !holder.asking_have && self.room(url) {
                out.push(self.record(url, Ask::Have, now));
            }
        }

        if self.list_known {
            for url in &urls {
                while self.room(url) {
                    let holder = &self.holders[url];
                    let Some(have) = holder.have.as_ref() else {
                        break;
                    };
                    if let Some(index) = self.late(url, have, now) {
                        out.push(self.record(url, Ask::Piece(index), now));
                        continue;
                    }
                    let free = self.free_count > 0;
                    if free && holder.dry == Some(self.free_gen) {
                        break;
                    }
                    match self.claim_for(url, rng) {
                        Some(index) => out.push(self.record(url, Ask::Piece(index), now)),
                        None => {
                            if free {
                                let generation = self.free_gen;
                                if let Some(holder) = self.holders.get_mut(url) {
                                    holder.dry = Some(generation);
                                }
                            }
                            break;
                        }
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
                out.push(self.record(url, Ask::Have, now));
            }
        }
        out
    }

    /// The next piece `url`'s seeder should fetch, or `None`: a free
    /// prioritized piece first, then the rarest free piece, and in the
    /// endgame a second copy ([`claim`]).
    fn claim_for(&self, url: &str, rng: &mut impl Rng) -> Option<u32> {
        let holder = self.holders.get(url)?;
        let have = holder.have.as_ref()?;
        self.claim_priority(have).or_else(|| {
            claim(
                (self.free_count > 0).then_some(&self.rarity),
                have,
                &self.delivered,
                &self.inflight,
                &holder.mine,
                rng,
            )
        })
    }

    /// The lowest free prioritized piece `have` holds.
    fn claim_priority(&self, have: &Bits) -> Option<u32> {
        let range = self.priority.as_ref()?;
        (self.floor..range.end).find(|&index| self.free.get(index) && have.get(index))
    }

    /// The lowest prioritized piece that one order of another seeder than
    /// `url` has fetched for at least twice the mean piece time, and that
    /// `have` holds: it is ordered once more.
    fn late(&self, url: &str, have: &Bits, now: Instant) -> Option<u32> {
        let range = self.priority.as_ref()?;
        let mean = self.piece_time?;
        self.inflight
            .iter()
            .filter_map(|(&index, ids)| {
                if !range.contains(&index) || self.delivered.get(index) || !have.get(index) {
                    return None;
                }
                let [id] = ids.as_slice() else {
                    return None;
                };
                let order = self.orders.get(id)?;
                (&*order.url != url && now >= order.at + mean * 2).then_some(index)
            })
            .min()
    }

    /// When the next prioritized piece in flight becomes late, strictly
    /// after `now`: a late piece that no other seeder can take now is
    /// looked at again at the next event, not in a loop.
    pub(super) fn next_late(&self, now: Instant) -> Option<Instant> {
        let range = self.priority.as_ref()?;
        let mean = self.piece_time?;
        self.inflight
            .iter()
            .filter(|(index, ids)| {
                range.contains(index) && !self.delivered.get(**index) && ids.len() == 1
            })
            .filter_map(|(_, ids)| self.orders.get(&ids[0]).map(|order| order.at + mean * 2))
            .filter(|&due| due > now)
            .min()
    }

    /// Orders the missing pieces of `pieces` first, in index order, and
    /// lets a late one be ordered once more ([`Scheduler::plan`]). Indices
    /// past the blob are ignored; an empty range clears the priority.
    pub(super) fn set_priority(&mut self, pieces: Range<u32>) {
        let end = pieces.end.min(self.delivered.len());
        if pieces.start >= end {
            self.priority = None;
            return;
        }
        self.priority = Some(pieces.start..end);
        self.floor = pieces.start;
        self.advance_floor();
    }

    /// Ends the priority.
    pub(super) fn clear_priority(&mut self) {
        self.priority = None;
    }

    /// Moves the floor past the delivered prioritized pieces.
    fn advance_floor(&mut self) {
        if let Some(range) = &self.priority {
            while self.floor < range.end && self.delivered.get(self.floor) {
                self.floor += 1;
            }
        }
    }

    /// Whether `url`'s seeder may be given one more order.
    fn room(&self, url: &str) -> bool {
        self.orders.len() < self.max_inflight
            && self
                .holders
                .get(url)
                .is_some_and(|holder| holder.busy < holder.depth)
    }

    /// Marks piece `index` free or not; whether that changed it.
    fn set_free(&mut self, index: u32, on: bool) -> bool {
        if index >= self.free.len() || self.free.get(index) == on {
            return false;
        }
        self.free.set(index, on);
        if on {
            self.free_count += 1;
            self.rarity.insert(index);
        } else {
            self.free_count -= 1;
            self.rarity.remove(index);
        }
        true
    }

    /// Records one order of `url`'s seeder, planned at `now`.
    fn record(&mut self, url: &Arc<str>, ask: Ask, now: Instant) -> (u64, Arc<str>, Ask) {
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
                self.set_free(index, false);
            }
        }
        self.orders.insert(
            id,
            Order {
                url: Arc::clone(url),
                ask,
                at: now,
            },
        );
        (id, Arc::clone(url), ask)
    }

    /// Ends order `id` and releases its credit: its answer was taken, or it
    /// failed. `piece` is the payload length of a piece answer and when it
    /// arrived, which sets its seeder's depth and the mean piece time.
    /// `None` for an order this scheduler no longer holds.
    pub(super) fn finish(
        &mut self,
        id: u64,
        piece: Option<(u64, Instant)>,
    ) -> Option<(Arc<str>, Ask)> {
        let Order { url, ask, at } = self.orders.remove(&id)?;
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
                // No order fetches it any more, and the caller lacks it: it
                // is free again, which may give a dry seeder something.
                if !self.inflight.contains_key(&index)
                    && !self.delivered.get(index)
                    && self.set_free(index, true)
                {
                    self.free_gen += 1;
                }
                if let Some((_, arrived)) = piece {
                    // RFC 6298's smoothed round-trip time, weight 1/8.
                    let sample = arrived.saturating_duration_since(at);
                    self.piece_time = Some(match self.piece_time {
                        None => sample,
                        Some(mean) => mean * 7 / 8 + sample / 8,
                    });
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
        if index >= self.delivered.len() || self.delivered.get(index) {
            return None;
        }
        self.delivered.set(index, true);
        self.missing -= 1;
        self.set_free(index, false);
        self.advance_floor();
        Some(self.inflight.get(&index).cloned().unwrap_or_default())
    }

    /// Whether the caller holds every piece.
    pub(super) fn complete(&self) -> bool {
        self.missing == 0
    }

    /// Records what `url`'s seeder holds, as of `now`.
    pub(super) fn set_have(&mut self, url: &str, have: Bits, now: Instant) {
        let Some(holder) = self.holders.get_mut(url) else {
            return;
        };
        let old = holder.have.take().unwrap_or_else(|| Bits::new(have.len()));
        debug_assert_eq!(old.len(), have.len(), "a HAVE has one bit per piece");
        let changed = old
            .as_bytes()
            .iter()
            .zip(have.as_bytes())
            .map(|(old, new)| old ^ new);
        for index in set_indices(changed) {
            if have.get(index) {
                self.rarity.hold(index);
            } else {
                self.rarity.unhold(index);
            }
        }
        holder.have = Some(have);
        holder.have_at = Some(now);
        holder.dry = None;
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

    /// The free pieces no seeder holds by its last HAVE.
    pub(super) fn unavailable(&self) -> Bits {
        self.rarity.unheld().clone()
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

/// The indices of the set bits of `bytes`, laid out as [`Bits::as_bytes`].
fn set_indices(bytes: impl Iterator<Item = u8>) -> impl Iterator<Item = u32> {
    (0u32..).zip(bytes).flat_map(|(at, mut byte)| {
        std::iter::from_fn(move || {
            (byte != 0).then(|| {
                let bit = byte.trailing_zeros();
                byte &= byte - 1;
                at * 8 + bit
            })
        })
    })
}

/// Levels above this share one: past this many holders a piece is common.
const RARE_LEVELS: usize = 16;

/// The free pieces by how many seeders hold them, for rarest-first claims.
struct Rarity {
    /// Per piece, how many seeders' last HAVE holds it.
    avail: Vec<u32>,
    /// `levels[l]`: the free pieces whose `avail.min(RARE_LEVELS - 1)` is
    /// `l`; created on first use.
    levels: Vec<Bits>,
    /// Ones in each level.
    counts: Vec<u32>,
}

impl Rarity {
    /// Every piece held by none; `free` all in level 0.
    fn new(free: &Bits) -> Rarity {
        Rarity {
            avail: vec![0; free.len() as usize],
            levels: vec![free.clone()],
            counts: vec![free.ones()],
        }
    }

    /// The level of a piece `avail` seeders hold.
    fn level(avail: u32) -> usize {
        (avail as usize).min(RARE_LEVELS - 1)
    }

    /// One more seeder holds `index`.
    fn hold(&mut self, index: u32) {
        self.shift(index, true);
    }

    /// One seeder fewer holds `index`.
    fn unhold(&mut self, index: u32) {
        self.shift(index, false);
    }

    fn shift(&mut self, index: u32, up: bool) {
        let Some(avail) = self.avail.get_mut(index as usize) else {
            return;
        };
        let from = Rarity::level(*avail);
        *avail = if up {
            avail.saturating_add(1)
        } else {
            avail.saturating_sub(1)
        };
        let to = Rarity::level(*avail);
        if from != to && self.clear(from, index) {
            self.put(to, index);
        }
    }

    /// `index` became free.
    fn insert(&mut self, index: u32) {
        if let Some(&avail) = self.avail.get(index as usize) {
            self.put(Rarity::level(avail), index);
        }
    }

    /// `index` stopped being free.
    fn remove(&mut self, index: u32) {
        if let Some(&avail) = self.avail.get(index as usize) {
            self.clear(Rarity::level(avail), index);
        }
    }

    fn put(&mut self, level: usize, index: u32) {
        let len = self.levels[0].len();
        while self.levels.len() <= level {
            self.levels.push(Bits::new(len));
            self.counts.push(0);
        }
        if !self.levels[level].get(index) {
            self.levels[level].set(index, true);
            self.counts[level] += 1;
        }
    }

    /// Clears `index` in `level`; whether it was set.
    fn clear(&mut self, level: usize, index: u32) -> bool {
        match self.levels.get_mut(level) {
            Some(bits) if bits.get(index) => {
                bits.set(index, false);
                self.counts[level] -= 1;
                true
            }
            _ => false,
        }
    }

    /// The rarest free piece `have` holds, ties uniform. Level 0 is
    /// skipped: a piece a seeder holds is held by at least one.
    fn claim(&self, have: &Bits, rng: &mut impl Rng) -> Option<u32> {
        self.levels
            .iter()
            .zip(&self.counts)
            .skip(1)
            .filter(|&(_, &count)| count > 0)
            .find_map(|(level, _)| claim_free(level, have, rng))
    }

    /// The free pieces no seeder holds.
    fn unheld(&self) -> &Bits {
        &self.levels[0]
    }
}

/// Random probes of the free pieces a claim makes before it reads the bit
/// maps whole.
const PROBES: usize = 64;

/// The next piece one seeder should fetch, or `None`.
///
/// `free` is the missing pieces no one is fetching by how many seeders hold
/// them, `None` when there are none. Candidates are the pieces the caller
/// lacks, the seeder holds and the seeder is not already fetching. A free
/// candidate wins: the rarest free candidate, ties uniform. Only when
/// **no** piece is free (the endgame, every missing piece in flight
/// somewhere) does a seeder fetch a second copy, of the candidate with the
/// fewest copies, so a slow seeder cannot hold up the last pieces.
fn claim(
    free: Option<&Rarity>,
    have: &Bits,
    delivered: &Bits,
    inflight: &HashMap<u32, Vec<u64>>,
    mine: &HashSet<u32>,
    rng: &mut impl Rng,
) -> Option<u32> {
    match free {
        Some(rarity) => rarity.claim(have, rng),
        None => claim_copy(have, delivered, inflight, mine, rng),
    }
}

/// A piece both `free` and `have` hold, uniformly: random probes first,
/// each accepted with the same chance, which is cheap while such pieces
/// are common; then, if they all missed, the k-th of the pieces the two
/// maps share, read byte by byte. Either way every candidate is equally
/// likely. A free piece is never one the seeder fetches already.
fn claim_free(free: &Bits, have: &Bits, rng: &mut impl Rng) -> Option<u32> {
    let len = free.len();
    if len == 0 || have.len() != len {
        return None;
    }
    for _ in 0..PROBES {
        let index = rng.random_range(0..len);
        if free.get(index) && have.get(index) {
            return Some(index);
        }
    }
    let shared = || {
        free.as_bytes()
            .iter()
            .zip(have.as_bytes())
            .map(|(free, have)| free & have)
    };
    let total: u32 = shared().map(u8::count_ones).sum();
    if total == 0 {
        return None;
    }
    let mut k = rng.random_range(0..total);
    for (at, mut byte) in shared().enumerate() {
        let ones = byte.count_ones();
        if k < ones {
            for _ in 0..k {
                byte &= byte - 1;
            }
            return u32::try_from(at * 8)
                .ok()
                .map(|base| base + byte.trailing_zeros());
        }
        k -= ones;
    }
    None
}

/// The endgame's second copy: of the pieces in flight that the caller
/// lacks, the seeder holds and is not fetching, one with the fewest
/// copies, ties broken uniformly.
fn claim_copy(
    have: &Bits,
    delivered: &Bits,
    inflight: &HashMap<u32, Vec<u64>>,
    mine: &HashSet<u32>,
    rng: &mut impl Rng,
) -> Option<u32> {
    let mut least = (usize::MAX, 0u32, None);
    for (&index, ids) in inflight {
        let copies = ids.len();
        if copies == 0 || delivered.get(index) || !have.get(index) || mine.contains(&index) {
            continue;
        }
        // Reservoir sampling: each tie is kept with probability
        // 1 / (ties seen), which is uniform without a list.
        if copies < least.0 {
            least = (copies, 1, Some(index));
        } else if copies == least.0 {
            least.1 += 1;
            if rng.random_range(0..least.1) == 0 {
                least.2 = Some(index);
            }
        }
    }
    least.2
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

    /// [`claim`] with the free set the scheduler would hold for `delivered`
    /// and `inflight`.
    fn pick(
        delivered: &Bits,
        have: &Bits,
        inflight: &HashMap<u32, Vec<u64>>,
        mine: &HashSet<u32>,
        rng: &mut StdRng,
    ) -> Option<u32> {
        let free = bits(
            delivered.len(),
            (0..delivered.len()).filter(|&i| !delivered.get(i) && !inflight.contains_key(&i)),
        );
        let mut rarity = Rarity::new(&free);
        for index in set_indices(have.as_bytes().iter().copied()) {
            rarity.hold(index);
        }
        let free = (free.ones() > 0).then_some(&rarity);
        claim(free, have, delivered, inflight, mine, rng)
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
        let (_, ask) = scheduler.order(id).expect("an open order");
        let Ask::Piece(index) = ask else {
            panic!("only pieces are ordered here: {ask:?}");
        };
        assert_eq!(scheduler.deliver(index), Some(vec![id]));
        scheduler
            .finish(id, Some((bytes, at)))
            .expect("an open order");
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
            let index = pick(
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
            pick(
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
                pick(&delivered, &have, &inflight, &HashSet::new(), &mut rng),
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
            pick(&delivered, &have, &inflight, &HashSet::new(), &mut rng),
            None
        );
        // Once 3 is in flight too, the endgame lets this seeder duplicate.
        let inflight = flight(&[(1, 1), (2, 1), (3, 1)]);
        let index = pick(&delivered, &have, &inflight, &HashSet::new(), &mut rng);
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
            let index = pick(&delivered, &have, &inflight, &HashSet::new(), &mut rng)
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
                pick(&delivered, &have, &inflight, &mine, &mut rng).expect("a piece is left");
            assert!(mine.insert(index), "claimed {index} twice");
            inflight.entry(index).or_insert_with(Vec::new).push(id);
        }
        // Every piece is in flight on this seeder alone: the endgame has no
        // candidate here.
        assert_eq!(pick(&delivered, &have, &inflight, &mine, &mut rng), None);
    }

    /// The scheduler's bookkeeping against its definition: a piece is free
    /// exactly when the caller lacks it and no order fetches it, and each
    /// free piece sits in the availability level of its holder count.
    fn assert_consistent(scheduler: &Scheduler) {
        let len = scheduler.delivered.len();
        let rarity = &scheduler.rarity;
        for index in 0..len {
            let free = scheduler.free.get(index);
            assert_eq!(
                free,
                !scheduler.delivered.get(index) && !scheduler.inflight.contains_key(&index),
                "piece {index}"
            );
            let holders = scheduler
                .holders
                .values()
                .filter(|holder| holder.have.as_ref().is_some_and(|have| have.get(index)))
                .count();
            let avail = rarity.avail[index as usize];
            assert_eq!(avail as usize, holders, "holders of piece {index}");
            let level = Rarity::level(avail);
            if free {
                assert!(
                    rarity.levels.get(level).is_some_and(|bits| bits.get(index)),
                    "free piece {index} in level {level}"
                );
            }
            for (l, bits) in rarity.levels.iter().enumerate() {
                assert_eq!(
                    bits.get(index),
                    free && l == level,
                    "piece {index} in level {l}"
                );
            }
        }
        for (bits, &count) in rarity.levels.iter().zip(&rarity.counts) {
            assert_eq!(bits.ones(), count);
        }
        assert_eq!(rarity.levels.len(), rarity.counts.len());
        assert_eq!(scheduler.free_count, scheduler.free.ones());
        assert_eq!(scheduler.missing, len - scheduler.delivered.ones());
    }

    /// Claim: whatever happens to orders — answered, failed, released with
    /// their seeder, a seeder gone or saying HAVE anew — no missing piece is
    /// lost to the free set, and one full seeder then finishes the blob.
    #[test]
    fn no_missing_piece_is_lost_whatever_the_orders_do() {
        let mut rng = StdRng::seed_from_u64(11);
        let t = Instant::now();
        let count = 64;
        let blob = Blob::new(
            BlobId::from_bytes([7; 32]),
            u64::from(count) * PieceSize::MIN.bytes(),
            PieceSize::MIN,
        )
        .expect("a valid blob");
        let mut scheduler =
            Scheduler::new(blob, bits(count, [3, 9]), true, &FetchOptions::default());
        let urls: [Arc<str>; 3] = [Arc::from("a"), Arc::from("b"), Arc::from("c")];
        let mut open: Vec<u64> = Vec::new();
        for _ in 0..3000 {
            match rng.random_range(0..6) {
                0 => {
                    let url = &urls[rng.random_range(0..urls.len())];
                    scheduler.add(Arc::clone(url));
                    let have = bits(count, (0..count).filter(|_| rng.random_bool(0.3)));
                    scheduler.set_have(url, have, t);
                }
                1 => {
                    let url = &urls[rng.random_range(0..urls.len())];
                    let gone = scheduler.remove(url);
                    open.retain(|id| !gone.contains(id));
                }
                2 => open.extend(scheduler.plan(t, &mut rng).into_iter().map(|(id, _, _)| id)),
                3 if !open.is_empty() => {
                    let id = open.swap_remove(rng.random_range(0..open.len()));
                    scheduler.finish(id, None);
                }
                _ if !open.is_empty() => {
                    let id = open.swap_remove(rng.random_range(0..open.len()));
                    if let Some((_, Ask::Piece(index))) = scheduler.order(id) {
                        scheduler.deliver(index);
                    }
                    scheduler.finish(id, Some((1024, t)));
                }
                _ => {}
            }
            assert_consistent(&scheduler);
        }

        let full: Arc<str> = Arc::from("full");
        scheduler.add(Arc::clone(&full));
        scheduler.set_have(&full, Bits::full(count), t);
        for url in &urls {
            scheduler.remove(url);
        }
        open.clear();
        let mut rounds = 0;
        while !scheduler.complete() {
            rounds += 1;
            assert!(rounds < 10 * count, "the fetch stalled");
            open.extend(scheduler.plan(t, &mut rng).into_iter().map(|(id, _, _)| id));
            let id = open.remove(0);
            if let Some((_, Ask::Piece(index))) = scheduler.order(id) {
                scheduler.deliver(index);
            }
            scheduler.finish(id, Some((1024, t)));
            assert_consistent(&scheduler);
        }
    }

    /// Claim: a seeder found to hold none of the free pieces is not asked
    /// again for nothing, yet claims pieces the moment ones it holds are
    /// freed by another seeder's failed orders.
    #[test]
    fn a_seeder_with_nothing_free_claims_a_piece_freed_later() {
        let mut rng = StdRng::seed_from_u64(12);
        let t = Instant::now();
        let blob = Blob::new(
            BlobId::from_bytes([7; 32]),
            4 * PieceSize::MIN.bytes(),
            PieceSize::MIN,
        )
        .expect("a valid blob");
        let mut scheduler = Scheduler::new(blob, Bits::new(4), true, &FetchOptions::default());
        // `a` holds pieces 0 and 1 and takes both.
        scheduler.add(Arc::from("a"));
        scheduler.set_have("a", bits(4, [0, 1]), t);
        assert_eq!(scheduler.plan(t, &mut rng).len(), 2);
        // `b` holds the same two, both in flight, while 2 and 3 are free:
        // not the endgame, so `b` has nothing and is marked so.
        scheduler.add(Arc::from("b"));
        scheduler.set_have("b", bits(4, [0, 1]), t);
        assert!(scheduler.plan(t, &mut rng).is_empty());
        assert!(scheduler.holders["b"].dry.is_some());
        // `a` goes, its orders failed: both pieces are free again, and `b`
        // takes them.
        scheduler.remove("a");
        let mut again: Vec<Ask> = scheduler
            .plan(t, &mut rng)
            .into_iter()
            .map(|(_, url, ask)| {
                assert_eq!(&*url, "b");
                ask
            })
            .collect();
        again.sort_by_key(|ask| match ask {
            Ask::Piece(index) => *index,
            _ => u32::MAX,
        });
        assert_eq!(again, [Ask::Piece(0), Ask::Piece(1)]);
    }

    /// 64 pieces of [`PieceSize::MIN`], the list known, with `holders`.
    fn with_holders(holders: &[(&str, Bits)], per_peer: usize, now: Instant) -> Scheduler {
        let blob = Blob::new(
            BlobId::from_bytes([7; 32]),
            64 * PieceSize::MIN.bytes(),
            PieceSize::MIN,
        )
        .expect("a valid blob");
        let options = FetchOptions {
            per_peer,
            ..FetchOptions::default()
        };
        let mut scheduler = Scheduler::new(blob, Bits::new(64), true, &options);
        for (url, have) in holders {
            scheduler.add(Arc::from(*url));
            scheduler.set_have(url, have.clone(), now);
        }
        scheduler
    }

    /// `a` holds every piece, `b` and `c` every piece but 5.
    fn five_is_rare(now: Instant) -> Scheduler {
        let all_but_5 = bits(64, (0..64).filter(|&i| i != 5));
        with_holders(
            &[
                ("a", Bits::full(64)),
                ("b", all_but_5.clone()),
                ("c", all_but_5),
            ],
            8,
            now,
        )
    }

    #[test]
    fn the_rarest_piece_is_claimed_first() {
        let mut rng = StdRng::seed_from_u64(13);
        let scheduler = five_is_rare(Instant::now());
        assert_consistent(&scheduler);
        for _ in 0..20 {
            assert_eq!(scheduler.claim_for("a", &mut rng), Some(5));
            let index = scheduler.claim_for("b", &mut rng);
            assert!(index.is_some_and(|i| i != 5), "{index:?}");
        }
    }

    #[test]
    fn prioritized_pieces_are_claimed_first_in_index_order() {
        let mut rng = StdRng::seed_from_u64(14);
        let t = Instant::now();
        let mut scheduler = one_holder(PieceSize::MIN, 8, t);
        scheduler.set_priority(10..14);
        let mut open = Vec::new();
        let mut asked = Vec::new();
        while asked.len() < 4 {
            for (id, _, ask) in scheduler.plan(t, &mut rng) {
                open.push(id);
                asked.push(ask);
            }
            take(&mut scheduler, &mut open, 1024, t, &mut rng);
        }
        assert_eq!(
            asked[..4],
            [
                Ask::Piece(10),
                Ask::Piece(11),
                Ask::Piece(12),
                Ask::Piece(13)
            ]
        );
    }

    #[test]
    fn priority_wins_over_rarity() {
        let mut rng = StdRng::seed_from_u64(15);
        let mut scheduler = five_is_rare(Instant::now());
        scheduler.set_priority(0..2);
        assert_eq!(scheduler.claim_for("a", &mut rng), Some(0));
        scheduler.clear_priority();
        assert_eq!(scheduler.claim_for("a", &mut rng), Some(5));
    }

    #[test]
    fn clearing_the_priority_restores_the_normal_claims() {
        let t = Instant::now();
        let run = |prioritized: bool| {
            let mut rng = StdRng::seed_from_u64(16);
            let mut scheduler = one_holder(PieceSize::MIN, 8, t);
            if prioritized {
                scheduler.set_priority(10..14);
                scheduler.clear_priority();
            }
            let mut asked = Vec::new();
            for _ in 0..20 {
                for (id, _, ask) in scheduler.plan(t, &mut rng) {
                    asked.push(ask);
                    if let Ask::Piece(index) = ask {
                        scheduler.deliver(index);
                    }
                    scheduler.finish(id, Some((1024, t)));
                }
            }
            asked
        };
        let plain = run(false);
        assert!(plain.len() >= 20, "{plain:?}");
        assert_eq!(run(true), plain);
    }

    #[test]
    fn a_late_prioritized_piece_is_ordered_once_more_from_another_seeder() {
        let mut rng = StdRng::seed_from_u64(17);
        let ms = Duration::from_millis;
        let t0 = Instant::now();
        let mut scheduler = one_holder(PieceSize::MIN, 8, t0);
        scheduler.set_priority(0..1);
        let first = scheduler.plan(t0, &mut rng);
        assert_eq!((&*first[0].1, first[0].2), ("a", Ask::Piece(0)));

        scheduler.add(Arc::from("b"));
        scheduler.set_have("b", Bits::full(64), t0);
        let of_b = scheduler.plan(t0, &mut rng);
        assert!(!of_b.is_empty());
        assert!(of_b.iter().all(|&(_, _, ask)| ask != Ask::Piece(0)));
        // `b`'s pieces arrive 10 ms after their order: the mean is 10 ms.
        for &(id, _, ask) in &of_b {
            let Ask::Piece(index) = ask else {
                panic!("{ask:?}");
            };
            scheduler.deliver(index);
            scheduler.finish(id, Some((1024, t0 + ms(10))));
        }
        assert_eq!(scheduler.piece_time, Some(ms(10)));
        assert_eq!(scheduler.next_late(t0 + ms(15)), Some(t0 + ms(20)));
        let early = scheduler.plan(t0 + ms(15), &mut rng);
        assert!(early.iter().all(|&(_, _, ask)| ask != Ask::Piece(0)));

        // One more of `b`'s orders taken, 10 ms after it was ordered: past
        // twice the mean, piece 0 goes to `b` as well.
        let (id, _, ask) = early[0];
        assert_eq!(&*early[0].1, "b");
        let Ask::Piece(index) = ask else {
            panic!("{ask:?}");
        };
        scheduler.deliver(index);
        scheduler.finish(id, Some((1024, t0 + ms(25))));
        let late = scheduler.plan(t0 + ms(25), &mut rng);
        let first_of_b = late
            .iter()
            .find(|(_, url, _)| &**url == "b")
            .expect("b has room again");
        assert_eq!(first_of_b.2, Ask::Piece(0));
        assert_eq!(scheduler.next_late(t0 + ms(25)), None, "two copies now");

        // Two copies in flight: nobody orders a third.
        scheduler.add(Arc::from("c"));
        scheduler.set_have("c", Bits::full(64), t0 + ms(25));
        let of_c = scheduler.plan(t0 + ms(25), &mut rng);
        assert!(!of_c.is_empty());
        assert!(of_c.iter().all(|&(_, _, ask)| ask != Ask::Piece(0)));
        assert_consistent(&scheduler);
    }
}
