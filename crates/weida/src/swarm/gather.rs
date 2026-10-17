//! The dialling side of SWARM: one blob from every connected seeder at once,
//! each piece verified against the root before it is handed out
//! ([decisions/0040](../../../../docs/decisions/0040-swarm-pattern.md) §4.4).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use weida_core::Error;

use super::seed::{MIN_RATE, SEED_STALL, deadline_for};
use super::wire;
use super::{Ask, Bits, Blob, PieceList};
use crate::config::ClientTls;
use crate::conn::ConnHandle;
use crate::reconnect::PeerEvents;
use crate::runtime::{Exec, RuntimeInner};
use crate::stream::{Peer, PeerShared, open_exchange_on};
use crate::transfer::TransferMeta;

/// State of a swarm.
pub struct GatherState {
    peer: Peer,
}

impl GatherState {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, tls: Arc<ClientTls>) -> GatherState {
        GatherState {
            peer: Peer::new(runtime, tls),
        }
    }
}

impl crate::Swarm {
    /// Connects to a seeder at `weida://host:port/path`.
    ///
    /// A swarm takes pieces from **every** seeder it is connected to, so like
    /// a surveyor it accumulates peers on purpose. Connecting during a
    /// [`Fetch`] adds the seeder to it at its next [`Fetch::next`].
    pub async fn connect(&self, url: &str) -> Result<(), Error> {
        self.state().peer.connect(url).await
    }

    /// Forgets a dialled address; see [`Peer::disconnect`].
    pub fn disconnect(&self, url: &str) -> bool {
        self.state().peer.disconnect(url)
    }

    /// Number of connected seeders.
    pub fn peer_count(&self) -> usize {
        self.state().peer.peer_count()
    }

    /// One record per live connection, labelled by the URL as dialled; see
    /// [`Peer::connection_stats`].
    pub fn connection_stats(&self) -> Vec<crate::ConnectionStats> {
        self.state().peer.connection_stats()
    }

    /// The event stream of this swarm's addresses; see [`Peer::events`].
    pub fn events(&self) -> PeerEvents {
        self.state().peer.events()
    }

    /// Starts fetching the pieces of `blob` that `have` lacks.
    ///
    /// `have` is what the caller already holds, so a fetch resumes where an
    /// earlier one stopped; [`FetchOptions::list`] spares it the LIST ask
    /// when the caller kept the list ([`Fetch::list`]). Nothing is asked
    /// until the first [`Fetch::next`].
    ///
    /// # Panics
    ///
    /// When `have` does not have one bit per piece of `blob`, or
    /// [`FetchOptions::list`] is the list of another blob.
    pub fn fetch(&self, blob: Blob, have: Bits, mut options: FetchOptions) -> Fetch {
        assert_eq!(
            have.len(),
            blob.count(),
            "`have` needs one bit per piece of the blob"
        );
        let list = match options.list.take() {
            Some(list) => {
                assert_eq!(list.blob(), blob, "the piece list is of another blob");
                Some(list)
            }
            // A blob of at most one piece has an empty list, checked against
            // the root.
            None if blob.count() <= 1 => PieceList::parse(blob, &[]),
            None => None,
        };
        options.per_peer = options.per_peer.max(1);
        options.max_inflight = options.max_inflight.max(1);
        options.min_rate = options.min_rate.max(1);
        let (tx, rx) = mpsc::channel(options.max_inflight + 1);
        let peer = &self.state().peer;
        Fetch {
            shared: peer.downgrade(),
            exec: peer.exec().clone(),
            events: peer.events(),
            blob,
            options,
            list: list.map(Arc::new),
            delivered: have,
            views: HashMap::new(),
            dropped: HashSet::new(),
            inflight: HashMap::new(),
            tasks: HashMap::new(),
            listing: false,
            next_task: 0,
            tx,
            rx,
        }
    }
}

/// How a [`Fetch`] asks.
#[derive(Clone, Debug)]
pub struct FetchOptions {
    /// The piece list, if the caller already holds it (from an earlier
    /// [`Fetch::list`]); otherwise one seeder is asked for it and the answer
    /// is checked against the root.
    pub list: Option<PieceList>,
    /// The fixed part of every ask's deadline. Default 30 s.
    pub stall: Duration,
    /// The rate, in bytes per second, below which a seeder's answer is too
    /// slow: an ask of `n` bytes expires after `stall + n / min_rate`.
    /// Default 64 KiB/s; zero counts as one.
    pub min_rate: u64,
    /// Asks in flight to one seeder at once. Default 2; zero counts as one.
    pub per_peer: usize,
    /// Asks in flight in total, which bounds the piece bytes this fetch
    /// holds unread at `max_inflight` × piece size. Default 16; zero counts
    /// as one.
    pub max_inflight: usize,
    /// Seeders this fetch takes pieces from; the others stay connected and
    /// unused. Default unbounded.
    pub max_peers: usize,
    /// How long a seeder with nothing this fetch can take waits before it is
    /// asked HAVE again, so a seeder that is still filling serves what it
    /// gained. Default 5 s.
    pub have_refresh: Duration,
}

impl Default for FetchOptions {
    fn default() -> FetchOptions {
        FetchOptions {
            list: None,
            stall: SEED_STALL,
            min_rate: MIN_RATE,
            per_peer: 2,
            max_inflight: 16,
            max_peers: usize::MAX,
            have_refresh: Duration::from_secs(5),
        }
    }
}

/// One verified piece of a blob.
#[derive(Clone, Debug)]
pub struct Piece {
    /// Which piece: its bytes are [`Blob::span`] of it.
    pub index: u32,
    /// The piece's bytes, checked against the blob's root.
    pub bytes: Bytes,
}

/// What one seeder looks like to one fetch.
struct PeerView {
    conn: ConnHandle,
    path: Arc<str>,
    /// The pieces it holds, once it has said.
    have: Option<Bits>,
    have_at: Option<Instant>,
    asking_have: bool,
    /// Asks in flight to it.
    busy: usize,
    /// The pieces in flight to it.
    mine: HashSet<u32>,
}

/// One ask in flight.
struct Task {
    url: Arc<str>,
    ask: Ask,
    handle: JoinHandle<()>,
}

/// A verified answer.
enum Answer {
    List(PieceList),
    Have(Bits),
    Piece(Bytes),
}

/// Why an ask failed, which decides what happens to its seeder.
enum Fault {
    /// The connection went: the seeder keeps its place, and a redial brings
    /// it back.
    Lost,
    /// A refusal, a timeout, a wrong length or bytes that do not verify: the
    /// seeder is dropped for this fetch.
    Bad,
}

struct Outcome {
    task: u64,
    result: Result<Answer, Fault>,
}

/// One blob being gathered from every connected seeder.
///
/// [`Fetch::next`] yields each verified piece once, in any order. Dropping
/// the fetch aborts every ask it has in flight; the pieces it yielded are the
/// caller's.
pub struct Fetch {
    shared: Weak<PeerShared>,
    exec: Exec,
    events: PeerEvents,
    blob: Blob,
    options: FetchOptions,
    list: Option<Arc<PieceList>>,
    delivered: Bits,
    /// Keyed by the URL as dialled.
    views: HashMap<Arc<str>, PeerView>,
    /// Seeders that failed this fetch.
    dropped: HashSet<Arc<str>>,
    /// The tasks fetching each piece.
    inflight: HashMap<u32, Vec<u64>>,
    tasks: HashMap<u64, Task>,
    /// A LIST is in flight.
    listing: bool,
    next_task: u64,
    tx: mpsc::Sender<Outcome>,
    rx: mpsc::Receiver<Outcome>,
}

impl Fetch {
    /// The blob being fetched.
    pub fn blob(&self) -> Blob {
        self.blob
    }

    /// The piece list, once it is known and verified: keep it to resume
    /// without LIST ([`FetchOptions::list`]).
    pub fn list(&self) -> Option<&PieceList> {
        self.list.as_deref()
    }

    /// The pieces the caller holds: those it started with and those this
    /// fetch yielded.
    pub fn delivered(&self) -> &Bits {
        &self.delivered
    }

    /// The next verified piece, or `None` once the caller holds every piece.
    ///
    /// `Some(Err(Error::NotConnected))` when no ask is in flight and no
    /// seeder is left that has not failed this fetch, or when the swarm is
    /// gone; connecting a seeder and calling `next` again resumes.
    ///
    /// Cancel-safe: a call dropped before it returns loses nothing, and the
    /// asks it started keep running.
    pub async fn next(&mut self) -> Option<Result<Piece, Error>> {
        loop {
            if self.delivered.all() {
                return None;
            }
            let refresh = {
                let Some(shared) = self.shared.upgrade() else {
                    return Some(Err(Error::NotConnected));
                };
                self.sync_views(&shared);
                self.schedule();
                if self.tasks.is_empty()
                    && !shared.has_slot_besides(|url| self.dropped.contains(url))
                {
                    return Some(Err(Error::NotConnected));
                }
                self.next_refresh()
            };
            let sleep = refresh.map(|at| {
                self.exec
                    .sleep(at.saturating_duration_since(Instant::now()))
            });
            let sleep = async move {
                match sleep {
                    Some(sleep) => sleep.await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                outcome = self.rx.recv() => {
                    // `self` holds a sender, so the channel never closes.
                    if let Some(outcome) = outcome
                        && let Some(piece) = self.handle(outcome)
                    {
                        return Some(Ok(piece));
                    }
                }
                event = self.events.recv() => {
                    if event.is_none() {
                        return Some(Err(Error::NotConnected));
                    }
                }
                () = sleep => {}
            }
        }
    }

    /// Brings the views in line with the live connections: a seeder whose
    /// address is gone or whose connection was replaced loses its view and
    /// its asks, and a new one gains a view while there is room.
    fn sync_views(&mut self, shared: &PeerShared) {
        let live = shared.live_slots();
        let stale: Vec<Arc<str>> = self
            .views
            .iter()
            .filter(|(url, view)| {
                !live
                    .iter()
                    .any(|(u, conn, _)| u == *url && Arc::ptr_eq(conn, &view.conn))
            })
            .map(|(url, _)| Arc::clone(url))
            .collect();
        for url in stale {
            self.abort_peer(&url);
            self.views.remove(&url);
        }
        for (url, conn, path) in live {
            if self.views.len() >= self.options.max_peers {
                break;
            }
            if self.dropped.contains(&url) || self.views.contains_key(&url) {
                continue;
            }
            self.views.insert(
                url,
                PeerView {
                    conn,
                    path,
                    have: None,
                    have_at: None,
                    asking_have: false,
                    busy: 0,
                    mine: HashSet::new(),
                },
            );
        }
    }

    /// Starts every ask there is room for.
    fn schedule(&mut self) {
        let per_peer = self.options.per_peer;
        let max_inflight = self.options.max_inflight;
        let urls: Vec<Arc<str>> = self.views.keys().cloned().collect();
        let room = |fetch: &Fetch, url: &Arc<str>| {
            fetch.tasks.len() < max_inflight && fetch.views[url].busy < per_peer
        };

        if self.list.is_none()
            && !self.listing
            && let Some(url) = urls.iter().find(|url| room(self, url))
        {
            self.spawn(url, Ask::List);
        }

        for url in &urls {
            let view = &self.views[url];
            if view.have.is_none() && !view.asking_have && room(self, url) {
                self.spawn(url, Ask::Have);
            }
        }

        if self.list.is_some() {
            let mut rng = rand::rng();
            for url in &urls {
                while room(self, url) {
                    let view = &self.views[url];
                    let Some(have) = view.have.as_ref() else {
                        break;
                    };
                    match claim(&self.delivered, have, &self.inflight, &view.mine, &mut rng) {
                        Some(index) => self.spawn(url, Ask::Piece(index)),
                        None => break,
                    }
                }
            }
        }

        let now = Instant::now();
        for url in &urls {
            let view = &self.views[url];
            let due = view
                .have_at
                .is_some_and(|at| now >= at + self.options.have_refresh);
            if due && view.busy == 0 && room(self, url) {
                self.spawn(url, Ask::Have);
            }
        }
    }

    /// When the next idle seeder is due to be asked HAVE again.
    fn next_refresh(&self) -> Option<Instant> {
        self.views
            .values()
            .filter(|view| view.busy == 0)
            .filter_map(|view| view.have_at)
            .min()
            .map(|at| at + self.options.have_refresh)
    }

    /// Starts one ask to `url`'s seeder.
    fn spawn(&mut self, url: &Arc<str>, ask: Ask) {
        let id = self.next_task;
        self.next_task += 1;
        let view = self.views.get_mut(url).expect("asks go to a viewed seeder");
        view.busy += 1;
        match ask {
            Ask::List => self.listing = true,
            Ask::Have => view.asking_have = true,
            Ask::Piece(index) => {
                view.mine.insert(index);
                self.inflight.entry(index).or_default().push(id);
            }
        }
        let conn = ConnHandle::clone(&view.conn);
        let path = Arc::clone(&view.path);
        let blob = self.blob;
        let list = self.list.clone();
        let tx = self.tx.clone();
        let exec = self.exec.clone();
        let limit = deadline_for(
            self.options.stall,
            wire::reply_len(&blob, ask),
            self.options.min_rate,
        );
        let handle = self.exec.spawn(async move {
            let result = exec
                .within(limit, ask_one(&conn, &path, blob, ask, list.as_deref()))
                .await
                .unwrap_or(Err(Fault::Bad));
            let _ = tx.send(Outcome { task: id, result }).await;
        });
        self.tasks.insert(
            id,
            Task {
                url: Arc::clone(url),
                ask,
                handle,
            },
        );
    }

    /// Takes one finished ask in, and yields its piece if it is the first
    /// copy.
    fn handle(&mut self, outcome: Outcome) -> Option<Piece> {
        // An ask aborted after it finished: already forgotten.
        let task = self.tasks.remove(&outcome.task)?;
        self.forget(outcome.task, &task);
        match outcome.result {
            Ok(Answer::List(list)) => {
                self.list.get_or_insert_with(|| Arc::new(list));
                None
            }
            Ok(Answer::Have(have)) => {
                if let Some(view) = self.views.get_mut(&task.url) {
                    view.have = Some(have);
                    view.have_at = Some(Instant::now());
                }
                None
            }
            Ok(Answer::Piece(bytes)) => {
                let Ask::Piece(index) = task.ask else {
                    unreachable!("only a PIECE ask answers with a piece");
                };
                if self.delivered.get(index) {
                    return None;
                }
                self.delivered.set(index, true);
                // The other copies of this piece are not needed any more.
                for id in self.inflight.remove(&index).unwrap_or_default() {
                    if let Some(other) = self.tasks.remove(&id) {
                        other.handle.abort();
                        self.forget(id, &other);
                    }
                }
                Some(Piece { index, bytes })
            }
            Err(Fault::Lost) => None,
            Err(Fault::Bad) => {
                tracing::debug!(url = %task.url, blob = %self.blob.root(), "a seeder failed this fetch and is dropped");
                self.abort_peer(&task.url);
                self.views.remove(&task.url);
                self.dropped.insert(task.url);
                None
            }
        }
    }

    /// Clears what task `id` held: its piece claim, its place in its
    /// seeder's budget, a pending LIST or HAVE.
    fn forget(&mut self, id: u64, task: &Task) {
        match task.ask {
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
        if let Some(view) = self.views.get_mut(&task.url) {
            view.busy -= 1;
            match task.ask {
                Ask::Have => view.asking_have = false,
                Ask::Piece(index) => {
                    view.mine.remove(&index);
                }
                Ask::List => {}
            }
        }
    }

    /// Aborts every ask to `url`'s seeder and gives its pieces back.
    fn abort_peer(&mut self, url: &Arc<str>) {
        let ids: Vec<u64> = self
            .tasks
            .iter()
            .filter(|(_, task)| task.url == *url)
            .map(|(&id, _)| id)
            .collect();
        for id in ids {
            if let Some(task) = self.tasks.remove(&id) {
                task.handle.abort();
                self.forget(id, &task);
            }
        }
    }
}

impl Drop for Fetch {
    fn drop(&mut self) {
        for task in self.tasks.values() {
            task.handle.abort();
        }
    }
}

impl std::fmt::Debug for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetch")
            .field("blob", &self.blob)
            .field("delivered", &self.delivered.ones())
            .field("seeders", &self.views.len())
            .field("in_flight", &self.tasks.len())
            .finish_non_exhaustive()
    }
}

/// Asks one seeder once and checks the answer: its exact length, and its
/// content against the root.
async fn ask_one(
    conn: &ConnHandle,
    path: &str,
    blob: Blob,
    ask: Ask,
    list: Option<&PieceList>,
) -> Result<Answer, Fault> {
    let len = wire::reply_len(&blob, ask);
    let (mut request, reply) = open_exchange_on(conn, path, TransferMeta::default())
        .await
        .map_err(fault)?;
    request
        .write_all(&wire::encode(&blob, ask))
        .await
        .map_err(fault)?;
    request.finish().map_err(fault)?;
    let mut body = reply.recv().await.map_err(fault)?;
    let bytes = body
        .read_capped(usize::try_from(len).unwrap_or(usize::MAX))
        .await
        .map_err(fault)?;
    if bytes.len() as u64 != len {
        return Err(Fault::Bad);
    }
    match ask {
        Ask::List => PieceList::parse(blob, &bytes)
            .map(Answer::List)
            .ok_or(Fault::Bad),
        Ask::Have => Bits::from_bytes(blob.count(), &bytes)
            .map(Answer::Have)
            .ok_or(Fault::Bad),
        Ask::Piece(index) => match list {
            Some(list) if list.check(index, &bytes) => Ok(Answer::Piece(Bytes::from(bytes))),
            _ => Err(Fault::Bad),
        },
    }
}

/// A transport failure keeps the seeder; anything else drops it.
fn fault(e: Error) -> Fault {
    match e {
        Error::ConnectionLost(_) | Error::NotConnected | Error::Indeterminate => Fault::Lost,
        _ => Fault::Bad,
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
