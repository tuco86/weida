//! RADIO/DISH: lossy fan-out of segments
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.6).
//!
//! Each test defends one row of the note's table: supersession resets the
//! copies a dish could not take in time and no other topic's, a late joiner
//! starts at the next segment, a dish's `max_age` expires its copy without
//! touching another dish's, and a datagram segment is a counted drop where
//! it cannot be a datagram, never a stream. The datagram segments travel on
//! flows the **bound** side opens toward a peer that dialled it: the
//! bound-side case.

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Certs, Server};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use weida::{
    ClientTls, ClientTrust, DEFAULT_DATAGRAM_RECEIVE_BYTES, Dish, Error, Identity, Incoming,
    JoinTerms, Limits, PeerIdentity, Radio, Received, Runtime, RuntimeConfig, SegmentTerms, Trust,
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

async fn joined(server: &Server, runtime: &Runtime, filter: &str, terms: JoinTerms) -> Dish {
    let dish = runtime.dish(server.trust());
    within(dish.join(filter, terms)).await.expect("join");
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
    let mut segment = radio
        .segment(topic, SegmentTerms::default())
        .expect("segment");
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
    let fast = joined(&server, &fast_rt, "v", JoinTerms::default()).await;
    let stalled_rt = stalling(&server);
    let stalled = joined(&server, &stalled_rt, "v", JoinTerms::default()).await;
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
    within(dish.join("v", JoinTerms::default()))
        .await
        .expect("join");
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
        let mut segment = radio
            .segment("v", SegmentTerms::default())
            .expect("segment");
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
async fn a_segment_finished_right_before_its_successor_still_arrives_whole() {
    // A GOP ends where the next keyframe begins: the successor opens the
    // moment the segment is finished, while its last chunks and its FIN are
    // still queued for the copy's task.
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server.client_runtime();
    let dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
    dishes(&radio, 1).await;

    const SEGMENTS: u64 = 5;
    for _ in 0..SEGMENTS {
        let mut segment = radio
            .segment("v", SegmentTerms::default())
            .expect("segment");
        for _ in 0..8 {
            segment.write(vec![0x42; 4096]).expect("write");
        }
        segment.finish();
    }
    for _ in 0..SEGMENTS {
        let body = within(segment_of(within(dish.recv()).await.expect("recv")).collect(1 << 20))
            .await
            .expect("a whole segment");
        assert_eq!(body.len(), 8 * 4096);
    }
    assert_eq!(radio.dropped_on("v").map_or(0, |d| d.superseded), 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_segment_whose_finish_found_a_full_queue_is_cut_at_once() {
    // The copy's queue is full when the segment finishes, so its FIN is
    // never queued: the copy loses the segment there and then, before it
    // sends anything, and counts once, under the full queue. The dish is
    // healthy, so only that rule can cut the copy.
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server.client_runtime();
    let dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
    dishes(&radio, 1).await;

    // No await from here on: the copy's task has not taken a chunk yet.
    let mut first = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    for _ in 0..64 {
        first.write(vec![0x42; 1024]).expect("write");
    }
    assert_eq!(first.finish(), 0);
    let mut second = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    second.write(vec![0x42; 1024]).expect("write");
    second.finish();

    let received = segment_of(within(dish.recv()).await.expect("recv"));
    assert_eq!(received.meta().segment, Some(1));
    let drops = radio.dropped_on("v").expect("drops on v");
    assert_eq!(drops.subscriber_queue, 1, "{drops:?}");
    assert_eq!(drops.superseded, 0, "{drops:?}");
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_segment_following_upstream_keeps_its_copy_while_its_successor_opens() {
    // A relay opens segment n+1 the moment its upstream does, while the rest
    // of segment n is still arriving from upstream.
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server.client_runtime();
    let dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
    dishes(&radio, 1).await;

    let relayed = SegmentTerms::default().with_follows_upstream(true);
    let mut a = radio.segment("v", relayed.clone()).expect("segment a");
    a.write(vec![0x41; 4096]).expect("write");
    let mut b = radio.segment("v", relayed).expect("segment b");
    // The copy of a runs and sees its successor while a is unfinished.
    tokio::time::sleep(Duration::from_millis(100)).await;
    for _ in 0..7 {
        a.write(vec![0x41; 4096]).expect("write");
    }
    a.finish();
    b.write(vec![0x42; 4096]).expect("write");
    b.finish();

    let mut bodies = Vec::new();
    for _ in 0..2 {
        let received = segment_of(within(dish.recv()).await.expect("recv"));
        let number = received.meta().segment.expect("a segment number");
        let body = within(received.collect(1 << 20))
            .await
            .expect("a whole segment");
        bodies.push((number, body.len()));
    }
    bodies.sort_unstable();
    assert_eq!(bodies, vec![(0, 8 * 4096), (1, 4096)]);
    assert_eq!(radio.dropped_on("v").map_or(0, |d| d.superseded), 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_segment_following_upstream_still_resets_a_stalled_dish() {
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = stalling(&server);
    let _dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
    dishes(&radio, 1).await;

    // Four windows of a segment the dish never reads, left unfinished.
    let relayed = SegmentTerms::default().with_follows_upstream(true);
    let mut a = radio.segment("v", relayed.clone()).expect("segment a");
    for _ in 0..16 {
        a.write(vec![0x41; 16 * 1024]).expect("write");
    }
    let _b = radio.segment("v", relayed).expect("segment b");
    within(async {
        while radio.dropped_on("v").map_or(0, |d| d.superseded) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    drop(a);
    runtime.shutdown().await;
}

// --- segments from a dialling peer ---------------------------------------------

fn stream_of(incoming: Incoming) -> weida::IncomingTransfer {
    match incoming {
        Incoming::Stream(transfer) => transfer,
        other => panic!("expected a stream, got {other:?}"),
    }
}

#[tokio::test]
async fn a_peer_segment_reaches_an_acceptor_with_its_number() {
    let server = Server::start().await;
    let acceptor = server.listener.acceptor("/up").expect("acceptor");
    let client = server.client_runtime();
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url("/up")))
        .await
        .expect("connect");

    for n in 0..2u64 {
        let mut segment = within(peer.segment("t", SegmentTerms::default()))
            .await
            .expect("segment");
        assert_eq!(segment.number(), n);
        segment.write(vec![n as u8; 1000]).expect("write");
        segment.finish();
        let transfer = stream_of(within(acceptor.accept()).await.expect("accept"));
        assert_eq!(transfer.meta().segment, Some(n));
        assert_eq!(transfer.meta().topic.as_deref(), Some("t"));
        let body = within(transfer.collect(4096))
            .await
            .expect("a whole segment");
        assert_eq!(&body[..], &[n as u8; 1000][..]);
    }
    client.shutdown().await;
}

#[tokio::test]
async fn a_peer_segment_supersedes_the_previous_one_on_its_topic() {
    let server = Server::start_with(Limits {
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    })
    .await;
    let acceptor = server.listener.acceptor("/up").expect("acceptor");
    let client = server.client_runtime();
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url("/up")))
        .await
        .expect("connect");

    // Segment 0 is four windows long and never read: it cannot finish.
    let mut first = within(peer.segment("t", SegmentTerms::default()))
        .await
        .expect("segment 0");
    for _ in 0..16 {
        first.write(vec![0x42; 16 * 1024]).expect("write");
    }
    first.finish();
    let held = stream_of(within(acceptor.accept()).await.expect("accept 0"));
    assert_eq!(held.meta().segment, Some(0));

    let mut second = within(peer.segment("t", SegmentTerms::default()))
        .await
        .expect("segment 1");
    second.write(vec![0x43; 1000]).expect("write");
    second.finish();
    let read = within(held.collect(1 << 20)).await;
    assert!(matches!(read, Err(Error::Canceled)), "{read:?}");
    within(async {
        while peer.segment_drops("t").map_or(0, |d| d.superseded) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(peer.segment_drops("t").map(|d| d.superseded), Some(1));

    let next = stream_of(within(acceptor.accept()).await.expect("accept 1"));
    assert_eq!(next.meta().segment, Some(1));
    let body = within(next.collect(4096)).await.expect("a whole segment");
    assert_eq!(body.len(), 1000);
    client.shutdown().await;
}

#[tokio::test]
async fn two_peers_on_one_connection_number_one_sequence() {
    // Both peers dial the same address with the same trust, so the pool
    // gives them one connection: their segments on a topic share a sequence
    // the acceptor sees only rise.
    let server = Server::start().await;
    let acceptor = server.listener.acceptor("/up").expect("acceptor");
    let client = server.client_runtime();
    let a = client.peer(server.trust());
    let b = client.peer(server.trust());
    within(a.connect(&server.url("/up")))
        .await
        .expect("connect a");
    within(b.connect(&server.url("/up")))
        .await
        .expect("connect b");

    for (n, peer) in [&a, &b].into_iter().enumerate() {
        let mut segment = within(peer.segment("t", SegmentTerms::default()))
            .await
            .expect("segment");
        assert_eq!(segment.number(), n as u64);
        segment.write(vec![0x42; 100]).expect("write");
        segment.finish();
        let transfer = stream_of(within(acceptor.accept()).await.expect("accept"));
        assert_eq!(transfer.meta().segment, Some(n as u64));
        let body = within(transfer.collect(4096))
            .await
            .expect("a whole segment");
        assert_eq!(body.len(), 100);
    }
    client.shutdown().await;
}

// --- layers ---------------------------------------------------------------------

/// A server whose radio gives each dish 256 KiB of unwritten chunks.
async fn small_budget() -> Server {
    Server::start_with(Limits {
        subscriber_buffer_bytes: 256 * 1024,
        ..Limits::default()
    })
    .await
}

/// Every segment the dish receives until it is quiet for 300 ms, each read
/// to its end: `(layer, Ok(body))` for a whole layer, `(layer, Err)` for one
/// that was reset.
async fn layers_heard(dish: &Dish) -> Vec<(u8, Result<Vec<u8>, Error>)> {
    let mut heard = Vec::new();
    while let Ok(Ok(received)) = tokio::time::timeout(Duration::from_millis(300), dish.recv()).await
    {
        let transfer = segment_of(received);
        let layer = transfer.meta().layer.expect("a segment has a layer");
        let body = within(transfer.collect(1 << 20)).await.map(|b| b.to_vec());
        heard.push((layer, body));
    }
    heard
}

#[tokio::test]
async fn a_dish_short_of_budget_keeps_layer_zero_whole_while_upper_layers_are_cut() {
    let server = small_budget().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = stalling(&server);
    let dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
    dishes(&radio, 1).await;

    // No await while writing: nothing is written yet, so the budget holds
    // every queued byte. 12 x 16 KiB of layer 1 fits beside layer 0's 1 KiB;
    // the 160 KiB of layer 0 that follows only fits once layer 1 is cut.
    let mut segment = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    assert_eq!(segment.write_layer(0, vec![0; 1024]).expect("write"), 1);
    for _ in 0..12 {
        assert_eq!(
            segment.write_layer(1, vec![1; 16 * 1024]).expect("write"),
            1
        );
    }
    assert_eq!(
        segment.write_layer(0, vec![0; 160 * 1024]).expect("write"),
        1
    );
    segment.finish();

    let heard = layers_heard(&dish).await;
    let base: Vec<_> = heard.iter().filter(|(layer, _)| *layer == 0).collect();
    assert_eq!(base.len(), 1, "{heard:?}");
    let body = base[0].1.as_ref().expect("layer 0 whole");
    assert_eq!(body.len(), 1024 + 160 * 1024);
    for (layer, body) in &heard {
        if *layer == 1 {
            assert!(matches!(body, Err(Error::Canceled)), "{body:?}");
        }
    }
    let drops = radio.dropped_on("v").expect("drops on v");
    assert_eq!(drops.layers_cut, 1, "{drops:?}");
    assert_eq!(drops.subscriber_budget, 0, "{drops:?}");
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_dish_capped_at_layer_zero_is_never_sent_layer_one() {
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = server.client_runtime();
    let dish = joined(
        &server,
        &runtime,
        "v",
        JoinTerms::default().with_max_layer(0),
    )
    .await;
    dishes(&radio, 1).await;

    let mut segment = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    assert_eq!(segment.write_layer(0, vec![0; 1024]).expect("write"), 1);
    assert_eq!(segment.write_layer(1, vec![1; 1024]).expect("write"), 0);
    assert_eq!(segment.finish(), 1);

    let heard = layers_heard(&dish).await;
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(heard[0].0, 0);
    assert_eq!(heard[0].1.as_ref().expect("whole").len(), 1024);
    assert_eq!(radio.dropped_on("v"), None, "a cap is not a drop");
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_layer_cut_also_cuts_every_higher_layer_of_that_segment() {
    let server = small_budget().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = stalling(&server);
    let dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
    dishes(&radio, 1).await;

    let mut segment = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    for layer in 0..3 {
        assert_eq!(
            segment
                .write_layer(layer, vec![layer; 1024])
                .expect("write"),
            1
        );
    }
    // Larger than the whole budget: layer 2 goes first, then layer 1.
    assert_eq!(
        segment.write_layer(1, vec![1; 300 * 1024]).expect("write"),
        0
    );
    assert_eq!(segment.write_layer(2, vec![2; 1024]).expect("write"), 0);
    assert_eq!(segment.write_layer(0, vec![0; 1024]).expect("write"), 1);
    segment.finish();

    let drops = radio.dropped_on("v").expect("drops on v");
    assert_eq!(drops.layers_cut, 1, "{drops:?}");
    assert_eq!(drops.subscriber_budget, 0, "{drops:?}");
    let heard = layers_heard(&dish).await;
    for (layer, body) in &heard {
        match layer {
            0 => assert_eq!(body.as_ref().expect("layer 0 whole").len(), 2048),
            _ => assert!(matches!(body, Err(Error::Canceled)), "{layer}: {body:?}"),
        }
    }
    assert_eq!(heard.iter().filter(|(layer, _)| *layer == 0).count(), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_receiver_delivers_each_layer_of_a_segment_once() {
    use common::raw;
    use weida::codes;
    use weida_protocol::{DataHeader, FrameKind, encode_frame};

    let server = Server::start().await;
    let acceptor = server.listener.acceptor("/up").expect("acceptor");
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "localhost").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    async fn send(conn: &quinn::Connection, segment: u64, layer: u8) -> quinn::SendStream {
        let header = DataHeader {
            topic: Some("t".into()),
            segment: Some(segment),
            layer: (layer > 0).then_some(layer),
            ..DataHeader::addressed("/up")
        };
        let mut stream = conn.open_uni().await.expect("open uni");
        stream
            .write_all(&encode_frame(FrameKind::Data, &header.encode()))
            .await
            .expect("write header");
        stream.write_all(b"body").await.expect("write body");
        stream
    }
    let canceled = Ok(Some(
        quinn::VarInt::from_u64(codes::CANCELED).expect("varint"),
    ));

    // Each layer of segment 5 once, then layer 1 again and segment 4.
    let mut fresh = Vec::new();
    for layer in [0, 1] {
        let mut stream = send(&conn, 5, layer).await;
        stream.finish().expect("finish");
        let transfer = stream_of(within(acceptor.accept()).await.expect("accept"));
        fresh.push((transfer.meta().segment, transfer.meta().layer));
        fresh.sort_unstable();
        drop(stream);
    }
    assert_eq!(fresh, vec![(Some(5), Some(0)), (Some(5), Some(1))]);
    for (segment, layer) in [(5, 1), (4, 0), (5, 0)] {
        let stale = send(&conn, segment, layer).await;
        assert_eq!(within(stale.stopped()).await, canceled, "{segment}/{layer}");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(200), acceptor.accept())
            .await
            .is_err(),
        "a stale layer reached the acceptor"
    );
}

#[tokio::test]
async fn a_peer_segment_carries_its_layers_to_the_acceptor() {
    let server = Server::start().await;
    let acceptor = server.listener.acceptor("/up").expect("acceptor");
    let client = server.client_runtime();
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url("/up")))
        .await
        .expect("connect");

    let mut segment = within(peer.segment("t", SegmentTerms::default()))
        .await
        .expect("segment");
    assert_eq!(segment.write_layer(0, b"base".to_vec()).expect("write"), 1);
    assert_eq!(segment.write_layer(1, b"more".to_vec()).expect("write"), 1);
    assert_eq!(segment.finish(), 1);

    let mut layers = Vec::new();
    for _ in 0..2 {
        let transfer = stream_of(within(acceptor.accept()).await.expect("accept"));
        assert_eq!(transfer.meta().segment, Some(0));
        let layer = transfer.meta().layer;
        let body = within(transfer.collect(4096)).await.expect("a whole layer");
        layers.push((layer, body.to_vec()));
    }
    layers.sort_unstable();
    assert_eq!(
        layers,
        vec![(Some(0), b"base".to_vec()), (Some(1), b"more".to_vec())]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn an_empty_segment_arrives_as_an_empty_layer_zero() {
    // A radio segment to a dish.
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let acceptor = server.listener.acceptor("/up").expect("acceptor");
    let runtime = server.client_runtime();
    let dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
    dishes(&radio, 1).await;
    let segment = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    assert_eq!(segment.finish(), 1);
    let transfer = segment_of(within(dish.recv()).await.expect("recv"));
    assert_eq!(
        (transfer.meta().segment, transfer.meta().layer),
        (Some(0), Some(0))
    );
    assert!(
        within(transfer.collect(4096))
            .await
            .expect("whole")
            .is_empty()
    );

    // A peer segment to an acceptor.
    let peer = runtime.peer(server.trust());
    within(peer.connect(&server.url("/up")))
        .await
        .expect("connect");
    let segment = within(peer.segment("t", SegmentTerms::default()))
        .await
        .expect("segment");
    assert_eq!(segment.finish(), 1);
    let transfer = stream_of(within(acceptor.accept()).await.expect("accept"));
    assert_eq!(
        (transfer.meta().segment, transfer.meta().layer),
        (Some(0), Some(0))
    );
    assert!(
        within(transfer.collect(4096))
            .await
            .expect("whole")
            .is_empty()
    );
    runtime.shutdown().await;
}

// --- freshness per connection ---------------------------------------------------

#[tokio::test]
async fn a_dish_redialled_to_a_restarted_radio_takes_its_numbers_from_zero() {
    let certs = Certs::generate();
    let first = common::Restartable::start(&certs, "127.0.0.1:0".parse().expect("loopback")).await;
    let addr = first.addr;
    let radio = first.listener.radio("/r").expect("radio");
    let client = Runtime::new(RuntimeConfig {
        reconnect: weida::ReconnectPolicy {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(50),
            jitter: false,
            ..weida::ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let dish = client.dish(certs.client_tls());
    within(dish.join("v", JoinTerms::default()))
        .await
        .expect("join");
    within(dish.connect(&format!("weida://127.0.0.1:{}/r", addr.port())))
        .await
        .expect("connect");
    dishes(&radio, 1).await;
    for n in 0..3u64 {
        send(&radio, "v", 1, 100);
        let received = segment_of(within(dish.recv()).await.expect("recv"));
        assert_eq!(received.meta().segment, Some(n));
        within(received.collect(4096))
            .await
            .expect("a whole segment");
    }

    drop(radio);
    first.stop().await;
    let second = common::Restartable::start(&certs, addr).await;
    let radio = second.listener.radio("/r").expect("radio");
    // The redial sends the join again.
    dishes(&radio, 1).await;
    send(&radio, "v", 1, 100);
    let received = segment_of(within(dish.recv()).await.expect("recv"));
    assert_eq!(received.meta().segment, Some(0));
    assert_eq!(dish.stale(), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn an_acceptor_refuses_a_segment_older_than_one_it_delivered() {
    use common::raw;
    use weida::codes;
    use weida_protocol::{DataHeader, FrameKind, encode_frame};

    let server = Server::start().await;
    let acceptor = server.listener.acceptor("/up").expect("acceptor");
    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "localhost").expect("connect"))
        .await
        .expect("handshake");
    raw::send_hello(&conn).await;

    // The stream stays open: a finished one that the transport acknowledged
    // whole reports no STOP_SENDING.
    async fn send(conn: &quinn::Connection, segment: u64) -> quinn::SendStream {
        let header = DataHeader {
            topic: Some("t".into()),
            segment: Some(segment),
            ..DataHeader::addressed("/up")
        };
        let mut stream = conn.open_uni().await.expect("open uni");
        stream
            .write_all(&encode_frame(FrameKind::Data, &header.encode()))
            .await
            .expect("write header");
        stream.write_all(b"body").await.expect("write body");
        stream
    }

    let mut newest = send(&conn, 5).await;
    newest.finish().expect("finish");
    let transfer = stream_of(within(acceptor.accept()).await.expect("accept"));
    assert_eq!(transfer.meta().segment, Some(5));

    let stale = send(&conn, 3).await;
    assert_eq!(
        within(stale.stopped()).await,
        Ok(Some(
            quinn::VarInt::from_u64(codes::CANCELED).expect("varint")
        ))
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), acceptor.accept())
            .await
            .is_err(),
        "a stale segment reached the acceptor"
    );
}

#[tokio::test]
async fn a_segment_supersedes_only_its_own_topic() {
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtime = stalling(&server);
    let dish = joined(&server, &runtime, "", JoinTerms::default()).await;
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
    let dish = joined(&server, &runtime, "v", JoinTerms::default()).await;
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
    let _stalled = joined(
        &server,
        &stalled_rt,
        "v",
        JoinTerms::default().with_max_age(Duration::from_millis(200)),
    )
    .await;
    let draining_rt = server.client_runtime();
    let draining = joined(&server, &draining_rt, "v", JoinTerms::default()).await;
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
                assert_eq!(&*topic, "voice");
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
    let first = joined(&server, &first_rt, "voice", JoinTerms::default()).await;
    let second_rt = server.client_runtime_with(with_datagrams());
    let second = joined(&server, &second_rt, "voice", JoinTerms::default()).await;
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
    let dish = joined(&server, &runtime, "voice", JoinTerms::default()).await;
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
    let dish = joined(&server, &runtime, "voice", JoinTerms::default()).await;
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

/// A raw dish on `server`, joined to `voice` with the datagram capability. It
/// counts the FLOW streams the radio opens toward it; the first `refuse` of
/// them are stopped with STOP_SENDING and the rest are kept open. The endpoint
/// and the connection are returned, and must stay alive for the flows.
async fn raw_dish(
    server: &Server,
    refuse: usize,
) -> (quinn::Endpoint, quinn::Connection, Arc<AtomicUsize>) {
    use common::raw;
    use weida::codes;
    use weida_protocol::{CAPABILITY_DATAGRAM, FrameKind, Hello, SubscriptionHeader};

    let endpoint = raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "localhost").expect("connect"))
        .await
        .expect("handshake");
    let mut hello = Hello::v0(16 * 1024, 1024);
    hello.capabilities = vec![CAPABILITY_DATAGRAM];
    raw::send_frame(&conn, FrameKind::Hello, &hello.encode()).await;
    let subscribe = SubscriptionHeader::new("/r", "voice").encode();
    raw::send_frame(&conn, FrameKind::Subscribe, &subscribe).await;

    let opened = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&opened);
    let accepting = conn.clone();
    tokio::spawn(async move {
        let mut kept = Vec::new();
        while let Ok(mut stream) = accepting.accept_uni().await {
            let (preamble, _) = raw::read_frame(&mut stream).await;
            if preamble.kind != FrameKind::Flow {
                continue;
            }
            let seen = counted.fetch_add(1, Ordering::SeqCst) + 1;
            if seen <= refuse {
                let code = quinn::VarInt::from_u64(codes::REJECTED).expect("varint");
                let _ = stream.stop(code);
            } else {
                kept.push(stream);
            }
        }
    });
    (endpoint, conn, opened)
}

#[tokio::test]
async fn a_dish_that_refuses_every_flow_costs_a_few_opens_not_one_per_datagram() {
    let server = Server::start_with(with_datagrams()).await;
    let radio = server.listener.radio("/r").expect("radio");
    let (_endpoint, conn, opened) = raw_dish(&server, usize::MAX).await;
    dishes(&radio, 1).await;

    // A datagram a millisecond, so each refusal arrives between two sends:
    // without a backoff every send would open a flow of its own.
    within(async {
        for _ in 0..1000 {
            radio.datagram("voice", vec![0x33; 150]).expect("send");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let flows = opened.load(Ordering::SeqCst);
    assert!(flows <= 8, "{flows} flows opened for 1000 datagrams");
    let drops = radio.dropped_on("voice").expect("drops on voice");
    assert!(drops.subscriber_queue >= 900, "{drops:?}");
    conn.close(0u32.into(), b"done");
}

#[tokio::test]
async fn a_refused_flow_is_opened_again_after_its_backoff_and_takes_datagrams() {
    let server = Server::start_with(with_datagrams()).await;
    let radio = server.listener.radio("/r").expect("radio");
    let (_endpoint, conn, opened) = raw_dish(&server, 1).await;
    dishes(&radio, 1).await;

    // The first flow is refused. Until its backoff is over the datagrams are
    // dropped; then a second flow opens, which the dish keeps, and takes one.
    within(async {
        loop {
            let handed = radio.datagram("voice", vec![0x33; 150]).expect("send");
            if handed == 1 && opened.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    conn.close(0u32.into(), b"done");
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
        within(dish.join(filter, JoinTerms::default()))
            .await
            .expect("join");
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

    within(dish.join("room.b", JoinTerms::default()))
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

// --- per-dish drops ---------------------------------------------------------------

#[tokio::test]
async fn dish_drops_names_the_dish_that_lost_and_forgets_it_when_it_leaves() {
    const CHUNK: usize = 16 * 1024;
    let server = Keyed::start(Limits {
        subscriber_buffer_bytes: 256 * 1024,
        ..Limits::default()
    })
    .await;
    let radio = server.listener.radio("/r").expect("radio");
    let reading_rt = server_client();
    let (reading, reading_peer) = server.dish(&reading_rt, "v").await;
    let stalled_rt = Runtime::new(RuntimeConfig {
        limits: Limits {
            stream_receive_window: 64 * 1024,
            ..Limits::default()
        },
        ..RuntimeConfig::default()
    })
    .expect("stalled runtime");
    let (_stalled, stalled_peer) = server.dish(&stalled_rt, "v").await;
    dishes(&radio, 2).await;

    // The reading dish reports every byte it has read; the stalled one
    // never reads, so its copy holds 64 KiB in flight and the rest queued.
    let (read_tx, mut read_rx) = mpsc::unbounded_channel();
    let reader = tokio::spawn(async move {
        let mut transfer = segment_of(within(reading.recv()).await.expect("recv"));
        let mut buf = vec![0u8; CHUNK];
        let mut total = 0;
        loop {
            let n = within(transfer.read(&mut buf)).await.expect("read");
            if n == 0 {
                // The dish goes back with the count: dropping it would
                // leave the radio.
                return (reading, total);
            }
            total += n;
            let _ = read_tx.send(total);
        }
    });
    let mut segment = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    let mut read = 0;
    for written in 1..=24 {
        assert!(segment.write(vec![0x42; CHUNK]).expect("write") >= 1);
        while read < written * CHUNK {
            read = within(read_rx.recv()).await.expect("the reader reports");
        }
    }
    segment.finish();
    let (_reading, total) = within(reader).await.expect("reader");
    assert_eq!(total, 24 * CHUNK);

    let records = radio.dish_drops();
    assert_eq!(records.len(), 2, "{records:?}");
    let of = |peer: &PeerIdentity| {
        records
            .iter()
            .find(|r| r.peer.as_ref() == Some(peer))
            .expect("a record per dish")
            .clone()
    };
    let stalled = of(&stalled_peer);
    assert_eq!(stalled.subscriber_budget, 1, "{stalled:?}");
    let kept = of(&reading_peer);
    assert_eq!(
        [
            kept.subscriber_budget,
            kept.subscriber_queue,
            kept.superseded,
            kept.expired,
            kept.too_large,
            kept.no_datagrams,
            kept.layers_cut,
        ],
        [0; 7],
        "{kept:?}"
    );

    stalled_rt.shutdown().await;
    within(async {
        while radio.dish_drops().len() != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(radio.dish_drops()[0].peer, Some(reading_peer));
    reading_rt.shutdown().await;
}

/// A dish on its own runtime, joined to each `(filter, terms)` and connected
/// to `/r` of `server`.
async fn dish_joined_to(server: &Server, runtime: &Runtime, joins: &[(&str, JoinTerms)]) -> Dish {
    let dish = runtime.dish(server.trust());
    for (filter, terms) in joins {
        within(dish.join(filter, terms.clone()))
            .await
            .expect("join");
    }
    within(dish.connect(&server.url("/r")))
        .await
        .expect("connect");
    dish
}

#[tokio::test]
async fn a_dish_holding_filters_it_misses_is_reached_once_per_segment() {
    // Each dish holds 40 filters that never match `v`, and `v` itself: its
    // matching filters are one delivery, so each segment arrives once.
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtimes: Vec<Runtime> = (0..4).map(|_| server.client_runtime()).collect();
    let mut owned: Vec<(String, JoinTerms)> = (0..40)
        .map(|i| (format!("miss.{i}"), JoinTerms::default()))
        .collect();
    owned.push(("v".to_owned(), JoinTerms::default()));
    let joins: Vec<(&str, JoinTerms)> = owned
        .iter()
        .map(|(filter, terms)| (filter.as_str(), terms.clone()))
        .collect();
    let mut dish_list = Vec::new();
    for runtime in &runtimes {
        dish_list.push(dish_joined_to(&server, runtime, &joins).await);
    }
    dishes(&radio, 4).await;

    send(&radio, "v", 1, 1024);
    for dish in &dish_list {
        let received = segment_of(within(dish.recv()).await.expect("recv"));
        assert_eq!(received.meta().topic.as_deref(), Some("v"));
        assert_eq!(
            within(received.collect(4096)).await.expect("body").len(),
            1024
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), dish.recv())
                .await
                .is_err(),
            "a second copy reached a dish that joined `v` once"
        );
    }
    for runtime in runtimes {
        runtime.shutdown().await;
    }
}

#[tokio::test]
async fn filters_of_one_dish_combine_their_layer_caps_by_the_largest() {
    // The matching filters of one dish combine: the largest cap applies, and
    // a matching filter without a cap lifts it.
    let server = Server::start().await;
    let radio = server.listener.radio("/r").expect("radio");
    let runtimes: Vec<Runtime> = (0..3).map(|_| server.client_runtime()).collect();
    let base = JoinTerms::default().with_max_layer(0);
    let capped = dish_joined_to(&server, &runtimes[0], &[("v", base.clone())]).await;
    let uncapped = dish_joined_to(
        &server,
        &runtimes[1],
        &[("v", base.clone()), ("#", JoinTerms::default())],
    )
    .await;
    let wider = dish_joined_to(
        &server,
        &runtimes[2],
        &[
            ("v", base.clone()),
            ("*", JoinTerms::default().with_max_layer(1)),
        ],
    )
    .await;
    dishes(&radio, 3).await;

    let mut segment = radio
        .segment("v", SegmentTerms::default())
        .expect("segment");
    assert_eq!(segment.write_layer(0, vec![0; 1024]).expect("write"), 3);
    assert_eq!(segment.write_layer(1, vec![1; 1024]).expect("write"), 2);
    segment.finish();

    let layers_of = |heard: Vec<(u8, Result<Vec<u8>, Error>)>| {
        let mut layers: Vec<u8> = heard.iter().map(|(layer, _)| *layer).collect();
        layers.sort_unstable();
        layers
    };
    assert_eq!(layers_of(layers_heard(&capped).await), vec![0]);
    assert_eq!(layers_of(layers_heard(&uncapped).await), vec![0, 1]);
    assert_eq!(layers_of(layers_heard(&wider).await), vec![0, 1]);
    for runtime in runtimes {
        runtime.shutdown().await;
    }
}
