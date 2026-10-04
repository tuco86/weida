//! RADIO/DISH: lossy fan-out of segments
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
//!
//! Each test defends one row of the note's table: supersession resets the
//! copies a dish could not take in time and no other topic's, a late joiner
//! starts at the next segment, a dish's `max_age` expires its copy without
//! touching another dish's, and a datagram segment is a counted drop where
//! it cannot be a datagram, never a stream. The datagram segments travel on
//! flows the **bound** side opens toward a peer that dialled it, which is
//! B-282's bound-side proof.

mod common;

use std::time::Duration;

use common::{Certs, Server};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use weida::{
    ClientTls, ClientTrust, DEFAULT_DATAGRAM_RECEIVE_BYTES, Dish, Identity, Limits, PeerIdentity,
    Radio, Received, Runtime, RuntimeConfig, Trust,
};

/// Generous ceiling: every assertion below should settle well inside it.
const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// A client runtime whose streams stall after 64 KiB unread: a dish on it
/// that never reads leaves a large segment unacknowledged.
fn stalling(server: &Server) -> Runtime {
    server.client_runtime_with(Limits {
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    })
}

async fn joined(
    server: &Server,
    runtime: &Runtime,
    filter: &str,
    max_age: Option<Duration>,
) -> Dish {
    let dish = runtime.dish(server.trust());
    within(dish.join(filter, max_age)).await.expect("join");
    within(dish.connect(&server.url("/r")))
        .await
        .expect("connect");
    dish
}

/// Waits until the radio holds `n` dishes: a join is a SUBSCRIBE, and the
/// radio learns it when that stream is dispatched.
async fn dishes(radio: &Radio, n: usize) {
    within(async {
        while radio.dish_count() < n {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

fn segment_of(received: Received) -> weida::IncomingTransfer {
    match received {
        Received::Segment(transfer) => transfer,
        other => panic!("expected a stream segment, got {other:?}"),
    }
}

fn send(radio: &Radio, topic: &str, chunks: usize, chunk: usize) {
    let mut segment = radio.segment(topic).expect("segment");
    for _ in 0..chunks {
        segment.write(vec![0x42; chunk]).expect("write");
    }
    segment.finish();
}

#[tokio::test]
async fn a_stalled_dish_loses_old_segments_while_a_fast_one_gets_every_one() {
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let fast_rt = server.client_runtime();
    let fast = joined(&server, &fast_rt, "v", None).await;
    let stalled_rt = stalling(&server);
    let stalled = joined(&server, &stalled_rt, "v", None).await;
    dishes(&radio, 2).await;

    const SEGMENT: usize = 256 * 1024;
    let (read_tx, mut read_rx) = mpsc::channel(1);
    let reader = tokio::spawn(async move {
        for _ in 0..20 {
            let transfer = segment_of(fast.recv().await.expect("recv"));
            let number = transfer.meta().segment.expect("a segment number");
            let body = transfer
                .collect(2 * SEGMENT)
                .await
                .expect("a whole segment");
            assert_eq!(body.len(), SEGMENT);
            read_tx.send(number).await.expect("report");
        }
    });
    // The fast dish reads each segment before the next opens, so nothing of
    // its own is ever in flight when supersession strikes.
    for n in 0..20u64 {
        send(&radio, "v", 16, 16 * 1024);
        assert_eq!(within(read_rx.recv()).await, Some(n));
    }
    within(reader).await.expect("reader");

    let drops = radio.dropped_on("v").expect("drops on v");
    assert!(drops.superseded >= 1, "{drops:?}");

    // The stalled dish holds a queue of segments, and at most the newest of
    // them can still be read whole: every older copy was reset.
    let mut whole = 0;
    while let Ok(Ok(received)) =
        tokio::time::timeout(Duration::from_millis(200), stalled.recv()).await
    {
        if let Ok(body) = within(segment_of(received).collect(2 * SEGMENT)).await {
            assert_eq!(body.len(), SEGMENT);
            whole += 1;
        }
    }
    assert!(whole <= 1, "{whole} whole segments at the stalled dish");
    fast_rt.shutdown().await;
    stalled_rt.shutdown().await;
}

#[tokio::test]
async fn a_healthy_dish_behind_a_slow_path_gets_back_to_back_segments_whole() {
    // Frames of a 30 fps video, ten to a segment, the next segment opening
    // one frame interval after the previous one's last frame: the dish reads
    // everything at once, but its acknowledgement of a finished segment is a
    // round trip away when the successor opens.
    const SEGMENTS: usize = 6;
    const FRAMES: usize = 10;
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let front = common::delay_proxy(server.addr, Duration::from_millis(100)).await;
    let runtime = server.client_runtime();
    let dish = runtime.dish(server.trust());
    within(dish.join("v", None)).await.expect("join");
    within(dish.connect(&format!("weida://127.0.0.1:{}/r", front.port())))
        .await
        .expect("connect");
    dishes(&radio, 1).await;

    let reader = tokio::spawn(async move {
        let mut seen = Vec::new();
        for _ in 0..SEGMENTS {
            let mut transfer = segment_of(within(dish.recv()).await.expect("recv"));
            let number = transfer.meta().segment.expect("a segment number");
            let mut frames = 0;
            let whole = loop {
                let mut len = [0u8; 4];
                match within(transfer.read_exact(&mut len)).await {
                    Ok(_) => {}
                    Err(e) => break e.kind() == std::io::ErrorKind::UnexpectedEof,
                }
                let mut body = vec![0u8; u32::from_le_bytes(len) as usize];
                if within(transfer.read_exact(&mut body)).await.is_err() {
                    break false;
                }
                frames += 1;
            };
            seen.push((number, frames, whole));
        }
        seen
    });

    let mut tick = tokio::time::interval(Duration::from_millis(1000 / 30));
    for _ in 0..SEGMENTS {
        tick.tick().await;
        let mut segment = radio.segment("v").expect("segment");
        for f in 0..FRAMES {
            if f > 0 {
                tick.tick().await;
            }
            let size: usize = if f == 0 { 60_000 } else { 5_000 };
            let mut frame = (size as u32).to_le_bytes().to_vec();
            frame.resize(4 + size, 0x5a);
            segment.write(frame).expect("write");
        }
        segment.finish();
    }
    let seen = within(reader).await.expect("reader");
    for (number, frames, whole) in &seen {
        assert!(
            *whole && *frames == FRAMES,
            "segment {number}: {seen:?} {:?}",
            radio.dropped_on("v")
        );
    }
    let superseded = radio.dropped_on("v").map_or(0, |d| d.superseded);
    assert_eq!(superseded, 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_segment_supersedes_only_its_own_topic() {
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = stalling(&server);
    let dish = joined(&server, &runtime, "", None).await;
    dishes(&radio, 1).await;

    // 256 KiB on `a` cannot be acknowledged through a 64 KiB window the dish
    // does not read, so its copy is in flight while `b` moves on.
    send(&radio, "a", 16, 16 * 1024);
    for _ in 0..5 {
        send(&radio, "b", 1, 1024);
    }
    let mut on_a = None;
    while on_a.is_none() {
        let transfer = segment_of(within(dish.recv()).await.expect("recv"));
        if transfer.meta().topic.as_deref() == Some("a") {
            on_a = Some(transfer);
        }
    }
    let body = within(on_a.expect("a").collect(1 << 20))
        .await
        .expect("the copy on `a` survived five segments on `b`");
    assert_eq!(body.len(), 256 * 1024);
    assert_eq!(radio.dropped_on("a").map_or(0, |d| d.superseded), 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_joiner_receives_the_next_segment_and_nothing_earlier() {
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    for _ in 0..3 {
        send(&radio, "v", 1, 1024);
    }
    let runtime = server.client_runtime();
    let dish = joined(&server, &runtime, "v", None).await;
    dishes(&radio, 1).await;
    send(&radio, "v", 1, 1024);

    let first = segment_of(within(dish.recv()).await.expect("recv"));
    assert_eq!(first.meta().segment, Some(3));
    assert_eq!(first.meta().topic.as_deref(), Some("v"));
    assert_eq!(within(first.collect(4096)).await.expect("body").len(), 1024);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), dish.recv())
            .await
            .is_err(),
        "nothing was retained for the joiner"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_dish_max_age_expires_its_copy() {
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let stalled_rt = stalling(&server);
    let _stalled = joined(&server, &stalled_rt, "v", Some(Duration::from_millis(200))).await;
    let draining_rt = server.client_runtime();
    let draining = joined(&server, &draining_rt, "v", None).await;
    dishes(&radio, 2).await;

    const SEGMENT: usize = 4 << 20;
    let reader = tokio::spawn(async move {
        segment_of(draining.recv().await.expect("recv"))
            .collect(2 * SEGMENT)
            .await
    });
    send(&radio, "v", 16, SEGMENT / 16);

    let body = within(reader)
        .await
        .expect("reader")
        .expect("a whole segment");
    assert_eq!(body.len(), SEGMENT);
    within(async {
        while radio.dropped_on("v").map_or(0, |d| d.expired) < 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let drops = radio.dropped_on("v").expect("drops on v");
    assert_eq!(drops.expired, 1, "{drops:?}");
    assert_eq!(drops.superseded, 0, "{drops:?}");
    stalled_rt.shutdown().await;
    draining_rt.shutdown().await;
}

// --- datagram segments ---------------------------------------------------------

fn with_datagrams() -> Limits {
    Limits {
        datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
        ..Limits::default()
    }
}

/// Collects datagram segments until `count` arrived or the dish stays quiet.
async fn datagrams(dish: &Dish, count: usize) -> Vec<(u64, usize)> {
    let mut seen = Vec::new();
    while seen.len() < count {
        match tokio::time::timeout(Duration::from_millis(500), dish.recv()).await {
            Ok(Ok(Received::Datagram {
                topic,
                segment,
                payload,
            })) => {
                assert_eq!(topic, "voice");
                seen.push((segment, payload.len()));
            }
            Ok(Ok(other)) => panic!("expected a datagram, got {other:?}"),
            Ok(Err(e)) => panic!("recv: {e}"),
            Err(_) => break,
        }
    }
    seen
}

#[tokio::test]
async fn a_datagram_segment_reaches_every_joined_dish() {
    let server = Server::start_with(with_datagrams()).await;
    let radio = server.listener.radio("/r").expect("radio");
    let first_rt = server.client_runtime_with(with_datagrams());
    let first = joined(&server, &first_rt, "voice", None).await;
    let second_rt = server.client_runtime_with(with_datagrams());
    let second = joined(&server, &second_rt, "voice", None).await;
    dishes(&radio, 2).await;

    let readers = tokio::spawn(async move {
        let (a, b) = tokio::join!(datagrams(&first, 50), datagrams(&second, 50));
        (a, b)
    });
    for _ in 0..50 {
        assert_eq!(radio.datagram("voice", vec![0x33; 150]).expect("send"), 2);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (a, b) = within(readers).await.expect("readers");
    for seen in [a, b] {
        assert!(seen.len() >= 45, "received {} of 50", seen.len());
        assert!(seen.iter().all(|(_, len)| *len == 150));
        assert!(seen.windows(2).all(|w| w[0].0 < w[1].0), "{seen:?}");
    }
    first_rt.shutdown().await;
    second_rt.shutdown().await;
}

#[tokio::test]
async fn a_dish_without_datagrams_is_a_named_drop() {
    let server = Server::start_with(with_datagrams()).await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server.client_runtime();
    let dish = joined(&server, &runtime, "voice", None).await;
    dishes(&radio, 1).await;

    assert_eq!(radio.datagram("voice", vec![0x33; 150]).expect("send"), 0);
    let drops = radio.dropped_on("voice").expect("drops on voice");
    assert!(drops.no_datagrams >= 1, "{drops:?}");
    // And no stream segment stands in for it.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), dish.recv())
            .await
            .is_err()
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_datagram_larger_than_the_dish_carries_is_counted_too_large() {
    let server = Server::start_with(with_datagrams()).await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server.client_runtime_with(with_datagrams());
    let dish = joined(&server, &runtime, "voice", None).await;
    dishes(&radio, 1).await;

    // The first one opens the flow; the rest meet the open flow directly.
    for _ in 0..3 {
        radio.datagram("voice", vec![0x33; 4000]).expect("send");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    within(async {
        while radio.dropped_on("voice").map_or(0, |d| d.too_large) < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), dish.recv())
            .await
            .is_err(),
        "nothing too large arrived in any other form"
    );
    runtime.shutdown().await;
}

// --- admission and eviction ------------------------------------------------------

/// A server whose binding requires any proved key, so every dish is a peer
/// with a fingerprint ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)).
struct Keyed {
    listener: weida::Listener,
    url: String,
    _binding: weida::Binding,
    _runtime: Runtime,
    _certs: Certs,
}

impl Keyed {
    async fn start(limits: Limits) -> Keyed {
        let certs = Certs::generate();
        let runtime = Runtime::new(RuntimeConfig {
            limits,
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let listener = runtime.listener();
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback"),
                certs.server_tls().require_client(ClientTrust::AnyKey),
            )
            .await
            .expect("bind");
        let url = format!(
            "weida://{}@127.0.0.1:{}/r",
            certs.fingerprint(),
            binding.local_addr().port()
        );
        Keyed {
            listener,
            url,
            _binding: binding,
            _runtime: runtime,
            _certs: certs,
        }
    }

    /// A dish presenting a fresh key, joined to `filter`; and that key.
    async fn dish(&self, runtime: &Runtime, filter: &str) -> (Dish, PeerIdentity) {
        let id = Identity::generate().expect("identity");
        let peer = PeerIdentity::Key(id.fingerprint().expect("fingerprint"));
        let dish = runtime.dish(ClientTls::new(Trust::by_address()).with_identity(id));
        within(dish.join(filter, None)).await.expect("join");
        within(dish.connect(&self.url)).await.expect("connect");
        (dish, peer)
    }
}

/// Whether `dish` receives a segment within half a second.
async fn hears(dish: &Dish) -> bool {
    tokio::time::timeout(Duration::from_millis(500), dish.recv())
        .await
        .is_ok()
}

#[tokio::test]
async fn admission_refuses_a_join_silently_and_records_nothing() {
    let server = Keyed::start(Limits::default()).await;
    let (asked_tx, mut asked) = mpsc::unbounded_channel();
    let radio = server
        .listener
        .radio("/r")
        .expect("radio")
        .with_admission(move |join| {
            let _ = asked_tx.send((join.peer.cloned(), join.filter.to_owned()));
            join.filter == "room.a"
        });
    let runtime = server_client();

    let (refused, refused_peer) = server.dish(&runtime, "#").await;
    assert_eq!(
        within(asked.recv()).await,
        Some((Some(refused_peer), "#".to_owned())),
        "the admission saw the proved key and the filter as sent"
    );
    assert_eq!(radio.dish_count(), 0, "a refused join is not recorded");

    let (admitted, _) = server.dish(&runtime, "room.a").await;
    dishes(&radio, 1).await;
    send(&radio, "room.a", 1, 1024);
    let segment = segment_of(within(admitted.recv()).await.expect("recv"));
    assert_eq!(segment.meta().topic.as_deref(), Some("room.a"));

    // `#` would have matched: the refused dish hears nothing, and its
    // connection was not closed for asking.
    assert!(!hears(&refused).await, "a refused join receives nothing");
    assert_eq!(refused.peer_count(), 1, "a refusal is silence, not a close");
    runtime.shutdown().await;
}

#[tokio::test]
async fn evict_withdraws_a_join_and_frees_its_subscription_slot() {
    // One subscription per connection: a second join after the eviction
    // fits only if the eviction released the first one's slot, and closes
    // the connection with LIMIT_EXCEEDED otherwise.
    let server = Keyed::start(Limits {
        max_subscriptions: 1,
        ..Limits::default()
    })
    .await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server_client();
    let (dish, peer) = server.dish(&runtime, "room.a").await;
    dishes(&radio, 1).await;

    assert_eq!(radio.evict(&peer, "room.a"), 1);
    assert_eq!(radio.dish_count(), 0);
    send(&radio, "room.a", 1, 1024);
    assert!(!hears(&dish).await, "an evicted join receives nothing");

    within(dish.join("room.b", None))
        .await
        .expect("join room.b");
    dishes(&radio, 1).await;
    send(&radio, "room.b", 1, 1024);
    let segment = segment_of(within(dish.recv()).await.expect("recv"));
    assert_eq!(segment.meta().topic.as_deref(), Some("room.b"));
    assert_eq!(dish.peer_count(), 1, "the freed slot took the new join");

    let stranger = PeerIdentity::Key(
        Identity::generate()
            .expect("identity")
            .fingerprint()
            .expect("fingerprint"),
    );
    assert_eq!(radio.evict(&stranger, "room.b"), 0);
    assert_eq!(radio.dish_count(), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn installing_an_admission_screens_joins_already_recorded() {
    let server = Keyed::start(Limits::default()).await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server_client();
    let (kept, kept_peer) = server.dish(&runtime, "room.a").await;
    let (screened, _) = server.dish(&runtime, "room.a").await;
    dishes(&radio, 2).await;

    let radio = radio.with_admission(move |join| join.peer == Some(&kept_peer));
    assert_eq!(radio.dish_count(), 1, "the refused join was withdrawn");

    send(&radio, "room.a", 1, 1024);
    let segment = segment_of(within(kept.recv()).await.expect("recv"));
    assert_eq!(segment.meta().topic.as_deref(), Some("room.a"));
    assert!(!hears(&screened).await, "the screened dish hears nothing");
    runtime.shutdown().await;
}

fn server_client() -> Runtime {
    Runtime::new(RuntimeConfig::default()).expect("client runtime")
}
