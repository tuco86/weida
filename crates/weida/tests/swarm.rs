//! SWARM: a seeder answers a session's orders from the application's
//! storage, and a swarm gathers one blob from every seeder at once, verifying
//! each piece against the root
//! ([decisions/0040](../../../docs/decisions/0040-swarm-pattern.md),
//! [decisions/0041](../../../docs/decisions/0041-swarm-sessions.md)).

use crate::common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::Server;
use weida::{
    Ask, Bits, Blob, BlobId, Error, ErrorCode, Fetch, FetchOptions, Network, PieceList, PieceSize,
    Runtime, RuntimeConfig, SeedSession, SimNet, TransferMeta, Trust,
};

/// Generous ceiling: every assertion below should settle well within it.
const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("a swarm step", DEADLINE, f).await
}

/// A HAVE ask built from the wire table of 0040 §4.2 by hand, not by the
/// encoder, so the layout is pinned.
fn have_ask(root: &BlobId, size: u64, log2: u8) -> Vec<u8> {
    let mut ask = vec![1, 2, log2];
    ask.extend_from_slice(root.as_bytes());
    ask.extend_from_slice(&size.to_be_bytes());
    assert_eq!(ask.len(), 43);
    ask
}

#[tokio::test]
async fn a_malformed_ask_is_refused_before_accept() {
    let server = Server::start().await;
    let seeder = server.listener.seeder("/swarm").expect("seeder");
    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/swarm")))
        .await
        .expect("connect");

    let refused = within(requester.request(b"hello")).await;
    assert!(
        matches!(refused, Err(Error::Rejected)),
        "five bytes are no ask: {refused:?}"
    );

    let data = vec![7u8; 3000];
    let root = BlobId::of(&data);
    let asking = {
        let ask = have_ask(&root, 3000, 10);
        async move { requester.request(&ask).await?.collect(64).await }
    };
    let answering = async {
        let mut session = seeder.accept().await.expect("the HAVE reaches the seeder");
        assert_eq!(
            session.blob(),
            Blob::new(root, 3000, PieceSize::MIN).expect("blob")
        );
        assert_eq!(session.next().await.expect("an order"), Some(Ask::Have));
        let mut have = Bits::new(3);
        have.set(1, true);
        session.answer_have(&have).await.expect("answered");
        assert_eq!(
            session.next().await.expect("the end"),
            None,
            "a version-1 ask is a session of one order"
        );
    };
    let (answer, ()) = within(async { tokio::join!(asking, answering) }).await;
    assert_eq!(answer.expect("the bit map"), vec![0b010]);
}

#[tokio::test]
async fn an_answer_of_the_wrong_length_is_refused_and_reported() {
    let server = Server::start().await;
    let seeder = server.listener.seeder("/swarm").expect("seeder");
    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/swarm")))
        .await
        .expect("connect");

    let data = vec![9u8; 3000];
    let root = BlobId::of(&data);
    let mut ask = have_ask(&root, 3000, 10);
    ask[1] = 3;
    ask.extend_from_slice(&2u32.to_be_bytes());
    let asking = async move { requester.request(&ask).await?.collect(4096).await };
    let answering = async {
        let mut session = seeder.accept().await.expect("the PIECE reaches the seeder");
        assert_eq!(session.next().await.expect("an order"), Some(Ask::Piece(2)));
        // Piece 2 of 3000 bytes in 1 KiB pieces is 952 bytes long.
        let short = session.answer_piece(&data[2048..2999]).await;
        assert!(
            matches!(short, Err(Error::Protocol(_))),
            "a short piece is the application's error: {short:?}"
        );
    };
    let (answer, ()) = within(async { tokio::join!(asking, answering) }).await;
    assert!(
        answer.is_err(),
        "the asker sees a refusal, not bytes: {answer:?}"
    );
}

/// What a test seeder does with the asks it is given.
#[derive(Clone)]
enum Behavior {
    /// Every piece, every answer right.
    Honest,
    /// Only these pieces; a PIECE ask for another is declined.
    Holds(Bits),
    /// Every piece, each with one bit flipped.
    CorruptPieces,
    /// Every piece in HAVE, and no PIECE ask ever answered.
    SilentPieces,
    /// Honest, after this long per ask.
    Slow(Duration),
    /// Whatever a fetch in progress has gathered so far.
    Relay(Arc<Mutex<Relay>>),
}

/// What a seeder that is still fetching can serve.
struct Relay {
    list: Option<PieceList>,
    have: Bits,
    data: Vec<u8>,
}

/// A seeder on its own server, and every ask it was given.
struct TestSeeder {
    server: Server,
    asks: Arc<Mutex<Vec<Ask>>>,
}

impl TestSeeder {
    fn url(&self) -> String {
        self.server.pinned_url("/swarm")
    }

    fn asks(&self) -> Vec<Ask> {
        self.asks.lock().expect("asks").clone()
    }

    fn pieces_asked(&self) -> Vec<u32> {
        self.asks()
            .into_iter()
            .filter_map(|ask| match ask {
                Ask::Piece(index) => Some(index),
                _ => None,
            })
            .collect()
    }
}

/// Deterministic bytes that differ from piece to piece.
fn blob_bytes(len: usize) -> Arc<Vec<u8>> {
    Arc::new(
        (0..len)
            .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_be_bytes()[0])
            .collect(),
    )
}

fn span(blob: &Blob, index: u32) -> std::ops::Range<usize> {
    let span = blob.span(index).expect("a piece of the blob");
    span.start as usize..span.end as usize
}

async fn seed(net: &SimNet, data: Arc<Vec<u8>>, behavior: Behavior) -> TestSeeder {
    let server = Server::start_on(net, RuntimeConfig::default()).await;
    let seeder = server.listener.seeder("/swarm").expect("seeder");
    let asks = Arc::new(Mutex::new(Vec::new()));
    let list = Arc::new(PieceList::of(&data, PieceSize::MIN).expect("list"));
    let held = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&asks);
    tokio::spawn(async move {
        while let Ok(session) = seeder.accept().await {
            tokio::spawn(serve(
                session,
                Arc::clone(&data),
                Arc::clone(&list),
                behavior.clone(),
                Arc::clone(&held),
                Arc::clone(&recorded),
            ));
        }
    });
    TestSeeder { server, asks }
}

/// Admits a session once, by its blob, then answers its orders one by one
/// as `behavior` says, recording each order as it is read.
async fn serve(
    mut session: SeedSession,
    data: Arc<Vec<u8>>,
    list: Arc<PieceList>,
    behavior: Behavior,
    held: Arc<Mutex<Vec<SeedSession>>>,
    asks: Arc<Mutex<Vec<Ask>>>,
) {
    let blob = list.blob();
    if session.blob() != blob {
        session.decline().await;
        return;
    }
    while let Ok(Some(ask)) = session.next().await {
        asks.lock().expect("asks").push(ask);
        match (&behavior, ask) {
            (Behavior::Slow(delay), _) => {
                tokio::time::sleep(*delay).await;
                honest(&mut session, ask, &data, &list).await;
            }
            (Behavior::Holds(have), Ask::Have) => {
                let _ = session.answer_have(have).await;
            }
            (Behavior::Holds(have), Ask::Piece(index)) if !have.get(index) => {
                session.decline().await;
                return;
            }
            (Behavior::CorruptPieces, Ask::Piece(index)) => {
                let mut bytes = data[span(&blob, index)].to_vec();
                bytes[0] ^= 1;
                let _ = session.answer_piece(&bytes).await;
            }
            (Behavior::SilentPieces, Ask::Piece(_)) => {
                held.lock().expect("held").push(session);
                return;
            }
            (Behavior::Relay(relay), ask) => {
                let (list, have, piece) = {
                    let relay = relay.lock().expect("relay");
                    let piece = match ask {
                        Ask::Piece(index) if relay.have.get(index) => {
                            Some(relay.data[span(&blob, index)].to_vec())
                        }
                        _ => None,
                    };
                    (relay.list.clone(), relay.have.clone(), piece)
                };
                let _ = match (ask, list, piece) {
                    (Ask::List, Some(list), _) => session.answer_list(&list).await,
                    (Ask::Have, _, _) => session.answer_have(&have).await,
                    (Ask::Piece(_), _, Some(piece)) => session.answer_piece(&piece).await,
                    _ => {
                        session.decline().await;
                        return;
                    }
                };
            }
            _ => honest(&mut session, ask, &data, &list).await,
        }
    }
}

async fn honest(session: &mut SeedSession, ask: Ask, data: &[u8], list: &PieceList) {
    let blob = list.blob();
    let _ = match ask {
        Ask::List => session.answer_list(list).await,
        Ask::Have => session.answer_have(&Bits::full(blob.count())).await,
        Ask::Piece(index) => session.answer_piece(&data[span(&blob, index)]).await,
    };
}

/// A seeder of the version-1 wire only, as 0040 shipped it, on a plain
/// replier: an ask is read under the 47-byte cap, so a session open (48
/// bytes) overflows it and is refused with `Rejected`, as a real version-1
/// seeder does. With `lie_about_list` its piece list has the right length
/// and does not hash to the root, which a [`weida::Seeder`] cannot be made
/// to send. Returns the seeder and how many session opens it refused.
async fn version1_seeder(
    net: &SimNet,
    data: Arc<Vec<u8>>,
    lie_about_list: bool,
) -> (TestSeeder, Arc<AtomicUsize>) {
    let server = Server::start_on(net, RuntimeConfig::default()).await;
    let replier = server.listener.replier("/swarm").expect("replier");
    let asks = Arc::new(Mutex::new(Vec::new()));
    let refused = Arc::new(AtomicUsize::new(0));
    let list = PieceList::of(&data, PieceSize::MIN).expect("list");
    let blob = list.blob();
    let recorded = Arc::clone(&asks);
    let opens = Arc::clone(&refused);
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = match request.body().read_capped(47).await {
                Ok(body) => body,
                Err(Error::LimitExceeded) => {
                    opens.fetch_add(1, Ordering::Relaxed);
                    request.refuse(ErrorCode::Rejected).await;
                    continue;
                }
                Err(_) => continue,
            };
            let (ask, answer) = match body[1] {
                1 if lie_about_list => (Ask::List, vec![0x5a; blob.count() as usize * 32]),
                1 => (Ask::List, list.to_bytes()),
                2 => (Ask::Have, Bits::full(blob.count()).as_bytes().to_vec()),
                _ => {
                    let index = u32::from_be_bytes(body[43..47].try_into().expect("index"));
                    (Ask::Piece(index), data[span(&blob, index)].to_vec())
                }
            };
            recorded.lock().expect("asks").push(ask);
            let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                continue;
            };
            let _ = reply.write_all(&answer).await;
            let _ = reply.finish();
        }
    });
    (TestSeeder { server, asks }, refused)
}

fn swarm_runtime(net: &SimNet) -> Runtime {
    Runtime::new(RuntimeConfig {
        network: Network::sim(net),
        ..RuntimeConfig::default()
    })
    .expect("client runtime")
}

/// Pulls pieces until the fetch ends, writing each into `out`; returns the
/// indices in the order they came.
async fn gather(fetch: &mut Fetch, out: &mut [u8]) -> Vec<u32> {
    let blob = fetch.blob();
    let mut indices = Vec::new();
    while let Some(piece) = within(fetch.next()).await {
        let piece = piece.expect("a verified piece");
        out[span(&blob, piece.index)].copy_from_slice(&piece.bytes);
        indices.push(piece.index);
    }
    indices
}

fn alternate(count: u32, even: bool) -> Bits {
    let mut bits = Bits::new(count);
    for index in (0..count).filter(|i| (i % 2 == 0) == even) {
        bits.set(index, true);
    }
    bits
}

/// 17 whole pieces of 1 KiB and a short eighteenth.
const LEN: usize = 17 * 1024 + 100;

#[tokio::test]
async fn a_blob_is_gathered_from_seeders_holding_different_halves() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    assert_eq!(blob.count(), 18);
    let a = seed(
        &net,
        Arc::clone(&data),
        Behavior::Holds(alternate(18, true)),
    )
    .await;
    let b = seed(
        &net,
        Arc::clone(&data),
        Behavior::Holds(alternate(18, false)),
    )
    .await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");
    within(swarm.connect(&b.url())).await.expect("connect b");

    let mut fetch = swarm.fetch(blob, Bits::new(18), FetchOptions::default());
    let mut out = vec![0; LEN];
    let mut indices = gather(&mut fetch, &mut out).await;
    indices.sort_unstable();
    assert_eq!(indices, (0..18).collect::<Vec<_>>(), "each piece once");
    assert_eq!(BlobId::of(&out), blob.root());
    assert!(fetch.delivered().all());
    assert!(!a.pieces_asked().is_empty() && !b.pieces_asked().is_empty());
    assert!(
        a.pieces_asked().iter().all(|i| i % 2 == 0),
        "a holds the even pieces"
    );
    assert!(
        b.pieces_asked().iter().all(|i| i % 2 == 1),
        "b holds the odd pieces"
    );
}

#[tokio::test]
async fn a_seeder_that_sends_a_wrong_piece_is_dropped_and_the_blob_completes() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let a = seed(&net, Arc::clone(&data), Behavior::CorruptPieces).await;
    let b = seed(&net, Arc::clone(&data), Behavior::Honest).await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");
    within(swarm.connect(&b.url())).await.expect("connect b");

    let options = FetchOptions::default();
    let per_peer = options.per_peer;
    let mut fetch = swarm.fetch(blob, Bits::new(18), options);
    let mut out = vec![0; LEN];
    gather(&mut fetch, &mut out).await;
    assert_eq!(BlobId::of(&out), blob.root());
    assert!(
        a.pieces_asked().len() <= per_peer,
        "the corrupting seeder was asked {:?}",
        a.pieces_asked()
    );
}

#[tokio::test]
async fn a_list_that_does_not_hash_to_the_root_drops_the_seeder() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let (a, _) = version1_seeder(&net, Arc::clone(&data), true).await;
    let b = seed(&net, Arc::clone(&data), Behavior::Honest).await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    // The liar alone first, so the list is asked of it.
    within(swarm.connect(&a.url())).await.expect("connect a");
    let mut fetch = swarm.fetch(blob, Bits::new(18), FetchOptions::default());
    let first = within(fetch.next()).await;
    assert!(
        matches!(first, Some(Err(Error::NotConnected))),
        "the only seeder lied: {first:?}"
    );
    assert!(fetch.list().is_none());
    assert!(a.asks().contains(&Ask::List));

    // Connecting an honest seeder resumes the same fetch.
    within(swarm.connect(&b.url())).await.expect("connect b");
    let mut out = vec![0; LEN];
    gather(&mut fetch, &mut out).await;
    assert_eq!(BlobId::of(&out), blob.root());
    assert_eq!(fetch.list().map(PieceList::blob), Some(blob));
    assert!(a.pieces_asked().is_empty(), "a liar is asked no piece");
}

#[tokio::test]
async fn a_seeder_that_speaks_only_version_1_still_serves_a_fetch() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let (a, refused) = version1_seeder(&net, Arc::clone(&data), false).await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");

    let mut fetch = swarm.fetch(blob, Bits::new(18), FetchOptions::default());
    let mut out = vec![0; LEN];
    let mut indices = gather(&mut fetch, &mut out).await;
    indices.sort_unstable();
    assert_eq!(indices, (0..18).collect::<Vec<_>>());
    assert_eq!(BlobId::of(&out), blob.root());
    assert!(
        refused.load(Ordering::Relaxed) >= 1,
        "the fetch tried a session first"
    );
    let mut asked = a.pieces_asked();
    asked.sort_unstable();
    asked.dedup();
    assert_eq!(
        asked,
        (0..18).collect::<Vec<_>>(),
        "every piece came as a version-1 ask"
    );
}

/// Claim: a seeder whose `MAX_PENDING_SESSIONS` places are all taken makes the
/// next session wait instead of dropping it, so every fetch is served over a
/// session of its own: none is read as a version-1 seeder and none fails.
#[tokio::test(start_paused = true)]
async fn a_busy_seeder_makes_a_session_wait_rather_than_drop_it() {
    let net = SimNet::new();
    const FETCHES: usize = weida::MAX_PENDING_SESSIONS + 16;
    let data = blob_bytes(LEN);
    let list = Arc::new(PieceList::of(&data, PieceSize::MIN).expect("list"));
    let blob = list.blob();
    let server = Server::start_on(&net, RuntimeConfig::default()).await;
    let seeder = server.listener.seeder("/swarm").expect("seeder");
    let (open, opened) = tokio::sync::oneshot::channel::<()>();
    let sessions = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&sessions);
    let (served_data, served_list) = (Arc::clone(&data), Arc::clone(&list));
    tokio::spawn(async move {
        let _ = opened.await;
        while let Ok(session) = seeder.accept().await {
            counted.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(serve(
                session,
                Arc::clone(&served_data),
                Arc::clone(&served_list),
                Behavior::Honest,
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
            ));
        }
    });
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&server.pinned_url("/swarm")))
        .await
        .expect("connect");
    let mut fetches = tokio::task::JoinSet::new();
    for _ in 0..FETCHES {
        let mut fetch = swarm.fetch(blob, Bits::new(blob.count()), FetchOptions::default());
        fetches.spawn(async move {
            let mut out = vec![0; LEN];
            gather(&mut fetch, &mut out).await;
            out
        });
    }
    // A detection window, not a wait for correctness: it lets every session
    // reach the seeder before the application accepts any. On a slow machine
    // fewer sessions queue up and the test gets weaker; it never fails wrongly.
    tokio::time::sleep(Duration::from_millis(500)).await;
    open.send(()).expect("the application task waits");
    while let Some(out) = fetches.join_next().await {
        assert_eq!(BlobId::of(&out.expect("a fetch completed")), blob.root());
    }
    assert_eq!(
        sessions.load(Ordering::Relaxed),
        FETCHES,
        "one session per fetch: none was dropped into version-1 asks"
    );
    drop(swarm);
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_stops_taking_pieces_stops_the_seeder() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let list = PieceList::of(&data, PieceSize::MIN).expect("list");
    let blob = list.blob();
    let a = seed(&net, Arc::clone(&data), Behavior::Honest).await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");

    let options = FetchOptions {
        list: Some(list),
        per_peer: 4,
        ..FetchOptions::default()
    };
    let mut fetch = swarm.fetch(blob, Bits::new(18), options);
    for _ in 0..6 {
        within(fetch.next())
            .await
            .expect("a piece")
            .expect("verified");
    }
    // Slow start reached the credit of 4 within the first pieces, so six
    // taken pieces are ten orders: each taken piece is one order more.
    within(async {
        while a.pieces_asked().len() < 10 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(a.pieces_asked().len(), 10, "{:?}", a.pieces_asked());
    assert_eq!(fetch.in_flight().ones(), 4);

    // The caller takes nothing: the seeder gets no more work.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        a.pieces_asked().len(),
        10,
        "an order arrived though no piece was taken"
    );

    within(fetch.next())
        .await
        .expect("a piece")
        .expect("verified");
    within(async {
        while a.pieces_asked().len() < 11 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(a.pieces_asked().len(), 11);
}

#[tokio::test]
async fn a_fetch_asks_only_for_pieces_the_caller_lacks() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let list = PieceList::of(&data, PieceSize::MIN).expect("list");
    let blob = list.blob();
    let a = seed(&net, Arc::clone(&data), Behavior::Honest).await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");

    let mut have = Bits::new(18);
    let mut out = vec![0; LEN];
    for index in 0..9 {
        have.set(index, true);
        out[span(&blob, index)].copy_from_slice(&data[span(&blob, index)]);
    }
    let mut fetch = swarm.fetch(blob, have, FetchOptions::default());
    let mut indices = gather(&mut fetch, &mut out).await;
    indices.sort_unstable();
    assert_eq!(indices, (9..18).collect::<Vec<_>>());
    assert_eq!(BlobId::of(&out), blob.root());
    assert!(
        a.pieces_asked().iter().all(|&i| i >= 9),
        "asked {:?}",
        a.pieces_asked()
    );

    // With the list kept from before, a resumed fetch asks no LIST either.
    let before = a.asks().len();
    let mut have = Bits::full(18);
    have.set(17, false);
    let options = FetchOptions {
        list: Some(list),
        ..FetchOptions::default()
    };
    let mut fetch = swarm.fetch(blob, have, options);
    assert_eq!(gather(&mut fetch, &mut out).await, vec![17]);
    assert!(!a.asks()[before..].contains(&Ask::List));
}

#[tokio::test(start_paused = true)]
async fn a_seeder_that_connects_during_a_fetch_joins_it() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let a = seed(
        &net,
        Arc::clone(&data),
        Behavior::Holds(alternate(18, true)),
    )
    .await;
    let b = seed(
        &net,
        Arc::clone(&data),
        Behavior::Holds(alternate(18, false)),
    )
    .await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");

    let mut fetch = swarm.fetch(blob, Bits::new(18), FetchOptions::default());
    let mut out = vec![0; LEN];
    for _ in 0..9 {
        let piece = within(fetch.next())
            .await
            .expect("a piece")
            .expect("verified");
        assert_eq!(piece.index % 2, 0);
        out[span(&blob, piece.index)].copy_from_slice(&piece.bytes);
    }
    let stuck = tokio::time::timeout(Duration::from_millis(300), fetch.next()).await;
    assert!(
        stuck.is_err(),
        "no seeder holds an odd piece yet: {stuck:?}"
    );

    within(swarm.connect(&b.url())).await.expect("connect b");
    let rest = gather(&mut fetch, &mut out).await;
    assert_eq!(rest.len(), 9);
    assert_eq!(BlobId::of(&out), blob.root());
}

#[tokio::test]
async fn a_fetch_without_a_usable_seeder_is_not_connected() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let client = swarm_runtime(&net);

    let lonely = client.swarm(Trust::by_address());
    let mut fetch = lonely.fetch(blob, Bits::new(18), FetchOptions::default());
    assert!(matches!(
        within(fetch.next()).await,
        Some(Err(Error::NotConnected))
    ));

    let a = seed(&net, Arc::clone(&data), Behavior::CorruptPieces).await;
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");
    let mut fetch = swarm.fetch(blob, Bits::new(18), FetchOptions::default());
    match within(fetch.next()).await {
        Some(Err(Error::NotConnected)) => {}
        other => panic!("a seeder of wrong pieces yields nothing: {other:?}"),
    }
    assert_eq!(fetch.delivered().ones(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_silent_seeder_does_not_hold_up_the_last_piece() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let a = seed(&net, Arc::clone(&data), Behavior::SilentPieces).await;
    let b = seed(&net, Arc::clone(&data), Behavior::Honest).await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");
    within(swarm.connect(&b.url())).await.expect("connect b");

    let options = FetchOptions {
        stall: Duration::from_millis(200),
        ..FetchOptions::default()
    };
    let started = tokio::time::Instant::now();
    let mut fetch = swarm.fetch(blob, Bits::new(18), options);
    let mut out = vec![0; LEN];
    gather(&mut fetch, &mut out).await;
    assert_eq!(BlobId::of(&out), blob.root());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert!(!a.pieces_asked().is_empty(), "the silent seeder was asked");
}

/// Claim: a seeder whose server goes away in the middle of a fetch costs the
/// fetch nothing but time: its orders return to the free pieces the moment
/// its connection is lost, the other seeder serves them, and the caller sees
/// no error. Found as a scenario of a cross-host probe (a seeder process
/// exiting mid-fetch over a real WAN path); here on a paused clock.
#[tokio::test(start_paused = true)]
async fn a_seeder_that_goes_away_mid_fetch_leaves_its_pieces_to_the_others() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let a = seed(
        &net,
        Arc::clone(&data),
        Behavior::Slow(Duration::from_millis(20)),
    )
    .await;
    let b = seed(
        &net,
        Arc::clone(&data),
        Behavior::Slow(Duration::from_millis(20)),
    )
    .await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");
    within(swarm.connect(&b.url())).await.expect("connect b");

    let started = tokio::time::Instant::now();
    let mut fetch = swarm.fetch(blob, Bits::new(18), FetchOptions::default());
    let mut out = vec![0; LEN];
    let mut taken = 0;
    while taken < 4 {
        let piece = within(fetch.next())
            .await
            .expect("a piece")
            .expect("verified");
        out[span(&blob, piece.index)].copy_from_slice(&piece.bytes);
        taken += 1;
    }
    let asked_before = a.pieces_asked().len();
    a.server.binding.close().await;
    gather(&mut fetch, &mut out).await;
    assert_eq!(BlobId::of(&out), blob.root());
    // Far below the 30 s answer deadline: nothing waited for the gone
    // seeder's orders to time out.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert!(asked_before > 0, "the seeder that went away was used");
}

#[tokio::test]
async fn a_blob_of_one_piece_needs_no_list_and_an_empty_blob_no_seeder() {
    let net = SimNet::new();
    let data = blob_bytes(500);
    let blob = Blob::new(BlobId::of(&data), 500, PieceSize::MIN).expect("blob");
    let a = seed(&net, Arc::clone(&data), Behavior::Honest).await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&a.url())).await.expect("connect a");
    let mut fetch = swarm.fetch(blob, Bits::new(1), FetchOptions::default());
    let piece = within(fetch.next())
        .await
        .expect("the piece")
        .expect("verified");
    assert_eq!((piece.index, &piece.bytes[..]), (0, &data[..]));
    assert!(within(fetch.next()).await.is_none());
    assert!(!a.asks().contains(&Ask::List));

    let empty = Blob::new(BlobId::of(b""), 0, PieceSize::MIN).expect("the empty blob");
    let lonely = client.swarm(Trust::by_address());
    let mut fetch = lonely.fetch(empty, Bits::new(0), FetchOptions::default());
    assert!(within(fetch.next()).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn a_dropped_fetch_asks_nothing_more() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    let b = seed(
        &net,
        Arc::clone(&data),
        Behavior::Slow(Duration::from_millis(100)),
    )
    .await;
    let client = swarm_runtime(&net);
    let swarm = client.swarm(Trust::by_address());
    within(swarm.connect(&b.url())).await.expect("connect b");

    let options = FetchOptions::default();
    let per_peer = options.per_peer;
    let mut fetch = swarm.fetch(blob, Bits::new(18), options);
    within(fetch.next())
        .await
        .expect("a piece")
        .expect("verified");
    drop(fetch);
    let at_drop = b.asks().len();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = b.asks().len() - at_drop;
    assert!(
        after <= per_peer,
        "{after} asks arrived after the drop, more than were in flight"
    );
    // A fetch running on its own would have taken all 18 by now.
    assert!(b.pieces_asked().len() < 18, "{:?}", b.pieces_asked());
}

#[tokio::test(start_paused = true)]
async fn a_seeder_that_is_still_fetching_serves_what_it_has() {
    let net = SimNet::new();
    let data = blob_bytes(LEN);
    let blob = PieceList::of(&data, PieceSize::MIN).expect("list").blob();
    // A holds everything and answers slowly, so B fills up over time.
    let a = seed(
        &net,
        Arc::clone(&data),
        Behavior::Slow(Duration::from_millis(50)),
    )
    .await;
    let relay = Arc::new(Mutex::new(Relay {
        list: None,
        have: Bits::new(18),
        data: vec![0; LEN],
    }));
    let b = seed(&net, Arc::clone(&data), Behavior::Relay(Arc::clone(&relay))).await;

    // B fetches from A and feeds its own seeder.
    let b_client = swarm_runtime(&net);
    let b_swarm = b_client.swarm(Trust::by_address());
    within(b_swarm.connect(&a.url()))
        .await
        .expect("b connects a");
    let (first_tx, first_rx) = tokio::sync::oneshot::channel();
    let filling = {
        let relay = Arc::clone(&relay);
        tokio::spawn(async move {
            let mut fetch = b_swarm.fetch(blob, Bits::new(18), FetchOptions::default());
            let mut first = Some(first_tx);
            while let Some(piece) = fetch.next().await {
                let piece = piece.expect("b's piece");
                let mut relay = relay.lock().expect("relay");
                if relay.list.is_none() {
                    relay.list = fetch.list().cloned();
                }
                relay.data[span(&blob, piece.index)].copy_from_slice(&piece.bytes);
                relay.have.set(piece.index, true);
                drop(relay);
                if let Some(tx) = first.take() {
                    let _ = tx.send(());
                }
            }
        })
    };
    within(first_rx).await.expect("b has a piece");
    assert!(
        !relay.lock().expect("relay").have.all(),
        "b is still filling"
    );

    // C knows only B.
    let c_client = swarm_runtime(&net);
    let c_swarm = c_client.swarm(Trust::by_address());
    within(c_swarm.connect(&b.url()))
        .await
        .expect("c connects b");
    let options = FetchOptions {
        have_refresh: Duration::from_millis(100),
        ..FetchOptions::default()
    };
    let mut fetch = c_swarm.fetch(blob, Bits::new(18), options);
    let mut out = vec![0; LEN];
    gather(&mut fetch, &mut out).await;
    assert_eq!(BlobId::of(&out), blob.root());
    let haves = b.asks().iter().filter(|ask| **ask == Ask::Have).count();
    assert!(haves >= 2, "c asked b HAVE {haves} times");
    within(filling).await.expect("b completes");
}
