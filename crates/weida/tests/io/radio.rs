//! The tests of `tests/radio.rs` that need the network, a process or real time.

use crate::common;
use common::Server;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use weida::{JoinTerms, RuntimeConfig, SegmentTerms};

use crate::radio::*;

#[tokio::test]
async fn a_healthy_dish_behind_a_slow_path_gets_back_to_back_segments_whole() {
    // Frames of a 30 fps video, ten to a segment, the next segment opening
    // one frame interval after the previous one's last frame: the dish reads
    // everything at once, but its acknowledgement of a finished segment is a
    // round trip away when the successor opens.
    const SEGMENTS: usize = 6;
    const FRAMES: usize = 10;
    let server = Server::start_os().await;
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
async fn a_receiver_delivers_each_layer_of_a_segment_once() {
    use common::raw;
    use weida::codes;
    use weida_protocol::{DataHeader, FrameKind, encode_frame};

    let server = Server::start_os().await;
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
async fn an_acceptor_refuses_a_segment_older_than_one_it_delivered() {
    use common::raw;
    use weida::codes;
    use weida_protocol::{DataHeader, FrameKind, encode_frame};

    let server = Server::start_os().await;
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
async fn a_dish_that_refuses_every_flow_costs_a_few_opens_not_one_per_datagram() {
    let server = Server::start_os_with_config(RuntimeConfig {
        limits: with_datagrams(),
        ..RuntimeConfig::default()
    })
    .await;
    let radio = server.listener.radio("/r").expect("radio");
    let (_endpoint, conn, opened) = raw_dish(&server, usize::MAX).await;
    dishes(&radio, 1).await;

    // A datagram a millisecond, so each refusal arrives between two sends:
    // without a backoff every send would open a flow of its own. An
    // interval rather than a sleep per send: Windows rounds a 1 ms sleep up
    // to its 15.6 ms timer tick, and the interval catches up in bursts, so
    // the thousand sends take a second there too.
    within(async {
        let mut tick = tokio::time::interval(Duration::from_millis(1));
        for _ in 0..1000 {
            tick.tick().await;
            radio.datagram("voice", vec![0x33; 150]).expect("send");
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
    let server = Server::start_os_with_config(RuntimeConfig {
        limits: with_datagrams(),
        ..RuntimeConfig::default()
    })
    .await;
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
