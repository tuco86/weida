//! Hostile local peers: raw `AF_UNIX` clients that write the local preamble of
//! `docs/PROTOCOL.md` §2.1 and the grouping of `crates/weida/src/grouped.rs` by
//! hand. Nothing here is a library hook; the bytes are written as a peer would
//! write them, so the tests can express what a well-formed client never does:
//! go silent, hold a place without a word, or vanish without closing. The
//! binding is the production unix transport on a `SimNet`
//! (`Transport::SimUnix`), and the raw peers dial it with
//! `SimNet::connect_unix`, on a paused clock.

#![cfg(unix)]

use crate::common;

use std::time::Duration;

use common::{Harness, Transport};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use weida::{Incoming, Limits, RuntimeConfig};
use weida_protocol::{DataHeader, FrameKind, Hello, SubscriptionHeader, encode_frame};

/// Ceiling on any single await in these tests.
const BOUND: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(BOUND, f)
        .await
        .expect("operation timed out")
}

async fn within_for<F: Future>(bound: Duration, f: F) -> F::Output {
    tokio::time::timeout(bound, f)
        .await
        .expect("operation timed out")
}

/// First byte of a control connection (`docs/PROTOCOL.md` §2.1).
const KIND_CONTROL: u8 = 0x01;
/// First byte of a transfer connection, followed by the group token.
const KIND_TRANSFER: u8 = 0x02;
/// Chunk kinds of the socket framing (`crates/weida/src/chunked.rs`).
const CHUNK_DATA: u8 = 0x00;
const CHUNK_FIN: u8 = 0x01;

/// One payload chunk carrying `bytes`: `0x00`, `u32` little-endian length, bytes.
fn chunk(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + bytes.len());
    out.push(CHUNK_DATA);
    out.extend_from_slice(
        &u32::try_from(bytes.len())
            .expect("a chunk fits a u32")
            .to_le_bytes(),
    );
    out.extend_from_slice(bytes);
    out
}

/// The HELLO a peer sends first on its control connection.
fn hello_frame() -> Vec<u8> {
    encode_frame(FrameKind::Hello, &Hello::v0(16 * 1024, 1024).encode())
}

/// A raw connection to the harness's binding, as `UnixStream::connect` is to
/// a socket file.
fn connect(harness: &Harness) -> DuplexStream {
    harness
        .net()
        .expect("a simulated unix harness")
        .connect_unix(harness.socket_path().expect("a unix harness has a path"))
        .expect("connect")
}

/// Opens a control connection and reads the 16-byte group token the server
/// issues. The connection is returned open: whether it stays open is the
/// test's.
async fn dial_control(harness: &Harness) -> (DuplexStream, [u8; 16]) {
    let mut stream = connect(harness);
    within(stream.write_all(&[KIND_CONTROL]))
        .await
        .expect("control byte");
    let mut token = [0u8; 16];
    within(stream.read_exact(&mut token))
        .await
        .expect("group token");
    (stream, token)
}

/// Opens a transfer connection for `token`. Nothing is written yet.
async fn dial_transfer(harness: &Harness, token: &[u8; 16]) -> DuplexStream {
    let mut stream = connect(harness);
    let mut preamble = vec![KIND_TRANSFER];
    preamble.extend_from_slice(token);
    within(stream.write_all(&preamble))
        .await
        .expect("transfer preamble");
    stream
}

#[tokio::test(start_paused = true)]
async fn a_local_peer_that_exits_after_its_hello_is_torn_down() {
    let harness = Harness::start(Transport::SimUnix).await;
    let acceptor = harness.listener.acceptor("/q").expect("acceptor");

    // The peer's HELLO, and the control socket is then left open: it is the
    // peer's end of the link, and the server reads it to learn of the exit.
    let (mut control, token) = dial_control(&harness).await;
    within(control.write_all(&chunk(&hello_frame())))
        .await
        .expect("control HELLO");

    // One SUBSCRIBE on a transfer connection, then the FIN.
    let mut transfer = dial_transfer(&harness, &token).await;
    let subscribe = encode_frame(
        FrameKind::Subscribe,
        &SubscriptionHeader::new("/q", "").encode(),
    );
    within(transfer.write_all(&chunk(&subscribe)))
        .await
        .expect("SUBSCRIBE");
    within(transfer.write_all(&[CHUNK_FIN])).await.expect("FIN");

    let consumer = match within(acceptor.accept()).await.expect("accept") {
        Incoming::Subscribed(consumer) => consumer,
        other => panic!("expected the subscription, got {other:?}"),
    };
    assert_eq!(consumer.path(), "/q");
    assert_eq!(consumer.filter(), "");

    // The peer exits without a word. Its subscription must go with it.
    drop(control);
    let ended = within_for(Duration::from_secs(5), async {
        match acceptor.accept().await.expect("accept") {
            Incoming::Unsubscribed { id, filter: None } => id,
            other => panic!("expected the teardown, got {other:?}"),
        }
    })
    .await;
    assert_eq!(
        ended,
        consumer.id(),
        "the teardown names the peer's consumer"
    );

    drop(transfer);
    within(harness.shutdown()).await;
}

#[tokio::test(start_paused = true)]
async fn an_idle_local_connection_is_closed_after_the_hello_timeout() {
    let harness = Harness::start_with(
        Transport::SimUnix,
        RuntimeConfig {
            limits: Limits {
                hello_timeout_ms: 300,
                ..Limits::default()
            },
            ..RuntimeConfig::default()
        },
    )
    .await;

    // Connected, and silent: the server must give up on the preamble and close.
    let mut idle = connect(&harness);
    let mut byte = [0u8; 1];
    let read = within_for(Duration::from_secs(3), idle.read(&mut byte))
        .await
        .expect("read");
    assert_eq!(read, 0, "an idle connection is closed, not left open");

    within(harness.shutdown()).await;
}

#[tokio::test(start_paused = true)]
async fn connections_over_max_connections_wait_for_a_place() {
    let harness = Harness::start_with(
        Transport::SimUnix,
        RuntimeConfig {
            max_connections: 2,
            limits: Limits {
                hello_timeout_ms: 600,
                ..Limits::default()
            },
            ..RuntimeConfig::default()
        },
    )
    .await;

    // Two sockets that never send a preamble hold both places until their
    // HELLO timeout passes.
    let idle_a = connect(&harness);
    let idle_b = connect(&harness);

    // A third peer with a complete preamble: it must wait for a place rather
    // than be answered while the limit is full.
    let mut third = connect(&harness);
    within(third.write_all(&[KIND_CONTROL]))
        .await
        .expect("control byte");

    // `read` is cancel-safe, so a token that did not come is not half-read.
    let mut early = [0u8; 16];
    let early = tokio::time::timeout(Duration::from_millis(250), third.read(&mut early)).await;
    assert!(
        early.is_err(),
        "a connection over max_connections was answered before a place was free: {early:?}"
    );

    // Once the idle sockets time out and free their places, the third is served.
    let mut token = [0u8; 16];
    within_for(Duration::from_secs(5), third.read_exact(&mut token))
        .await
        .expect("the token once a place is free");

    drop((idle_a, idle_b, third));
    within(harness.shutdown()).await;
}

/// A refused transfer is drained in the background so that its sender
/// finishes and learns of the refusal from the reply. The drain owns the
/// descriptor, so the connection's place must stay taken for as long as the
/// drain lives; otherwise a peer that has its connections refused and keeps
/// them open makes the binding hold as many descriptors as the process may
/// open, with the places never running out.
#[tokio::test(start_paused = true)]
async fn a_refused_transfer_keeps_its_place_until_its_drain_has_ended() {
    // Two places: the control connection and the transfer.
    let harness = Harness::start_with(
        Transport::SimUnix,
        RuntimeConfig {
            max_connections: 2,
            ..RuntimeConfig::default()
        },
    )
    .await;

    let (mut control, token) = dial_control(&harness).await;
    within(control.write_all(&chunk(&hello_frame())))
        .await
        .expect("control HELLO");

    // A transfer to a path nothing serves, left open and never ended.
    let mut transfer = dial_transfer(&harness, &token).await;
    let head = encode_frame(FrameKind::Data, &DataHeader::addressed("/nowhere").encode());
    within(transfer.write_all(&chunk(&head)))
        .await
        .expect("DATA head");
    // More than the socket buffers hold: the writes complete only once the
    // server reads and discards them, which is the refusal having happened.
    let body = chunk(&[0u8; 64 * 1024]);
    for _ in 0..64 {
        within(transfer.write_all(&body))
            .await
            .expect("a refused payload is drained, not left to block its sender");
    }

    // The drain still reads that socket, so its place is still taken: a third
    // connection waits for one instead of being served.
    let mut third = connect(&harness);
    within(third.write_all(&[KIND_CONTROL]))
        .await
        .expect("control byte");
    let mut token_buf = [0u8; 16];
    let early = tokio::time::timeout(Duration::from_millis(300), third.read(&mut token_buf)).await;
    assert!(
        early.is_err(),
        "a place was given back while a drain still held the refused socket: {early:?}"
    );

    // The peer ends the connection, the drain ends with it, and the place is
    // free again.
    drop(transfer);
    within_for(Duration::from_secs(5), third.read_exact(&mut token_buf))
        .await
        .expect("the token once the drain let go of its place");

    drop((control, third));
    within(harness.shutdown()).await;
}

/// A local connection that consumed on a queue nobody is reading owes that
/// queue a notice when it closes, and the notice waits for room. The wait
/// must not keep the connection's place of `max_connections`: a binding
/// whose queue is stuck would otherwise stop serving anyone new.
#[tokio::test(start_paused = true)]
async fn a_closed_local_consumer_waiting_to_be_announced_does_not_hold_a_place() {
    // Three places: the consumer's control connection, and the control and
    // transfer connections of the peer that fills the queue.
    let harness = Harness::start_with(
        Transport::SimUnix,
        RuntimeConfig {
            max_connections: 3,
            endpoint_queue: 1,
            ..RuntimeConfig::default()
        },
    )
    .await;
    let acceptor = harness.listener.acceptor("/q").expect("acceptor");

    // A consumer subscribes. Reading its event once proves it was recorded,
    // and leaves the queue's single slot free.
    let (mut consumer, token) = dial_control(&harness).await;
    within(consumer.write_all(&chunk(&hello_frame())))
        .await
        .expect("consumer HELLO");
    let mut subscribe = dial_transfer(&harness, &token).await;
    let frame = encode_frame(
        FrameKind::Subscribe,
        &SubscriptionHeader::new("/q", "").encode(),
    );
    within(subscribe.write_all(&chunk(&frame)))
        .await
        .expect("SUBSCRIBE");
    within(subscribe.write_all(&[CHUNK_FIN]))
        .await
        .expect("FIN");
    let id = match within(acceptor.accept()).await.expect("accept") {
        Incoming::Subscribed(consumer) => consumer.id(),
        other => panic!("expected the subscription, got {other:?}"),
    };
    drop(subscribe);

    // Another peer fills that slot with a transfer nobody reads.
    let (mut filler, filler_token) = dial_control(&harness).await;
    within(filler.write_all(&chunk(&hello_frame())))
        .await
        .expect("filler HELLO");
    let mut transfer = dial_transfer(&harness, &filler_token).await;
    let mut data = encode_frame(FrameKind::Data, &DataHeader::addressed("/q").encode());
    data.extend_from_slice(b"payload");
    within(transfer.write_all(&chunk(&data)))
        .await
        .expect("DATA");
    within(transfer.write_all(&[CHUNK_FIN])).await.expect("FIN");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The consumer exits. Its notice cannot be delivered, so it waits.
    drop(consumer);

    // The place it held is given out again, though the notice still waits.
    let (_fourth, _) = within_for(Duration::from_secs(5), async {
        dial_control(&harness).await
    })
    .await;

    // The notice itself is not lost: it follows the transfer once the queue
    // is read. A local connection says nothing of a reply, so a path with an
    // acceptor receives its DATA as an exchange.
    assert!(matches!(
        within(acceptor.accept()).await.expect("accept"),
        Incoming::Exchange(_)
    ));
    match within(acceptor.accept()).await.expect("accept") {
        Incoming::Unsubscribed {
            id: gone,
            filter: None,
        } => assert_eq!(gone, id),
        other => panic!("expected the consumer's departure, got {other:?}"),
    }

    drop((filler, transfer));
    within(harness.shutdown()).await;
}
