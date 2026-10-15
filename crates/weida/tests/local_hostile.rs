//! Hostile local peers: raw `AF_UNIX` clients that write the local preamble of
//! `docs/PROTOCOL.md` §2.1 and the grouping of `crates/weida/src/grouped.rs` by
//! hand. Nothing here is a library hook; the bytes are written as a peer would
//! write them, so the tests can express what a well-formed client never does:
//! go silent, hold a place without a word, or vanish without closing.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{Harness, Transport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use weida::{Incoming, Limits, RuntimeConfig};
use weida_protocol::{FrameKind, Hello, SubscriptionHeader, encode_frame};

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

/// Opens a control connection and reads the 16-byte group token the server
/// issues. The socket is returned open: whether it stays open is the test's.
async fn dial_control(path: &Path) -> (UnixStream, [u8; 16]) {
    let mut stream = within(UnixStream::connect(path)).await.expect("connect");
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
async fn dial_transfer(path: &Path, token: &[u8; 16]) -> UnixStream {
    let mut stream = within(UnixStream::connect(path)).await.expect("connect");
    let mut preamble = vec![KIND_TRANSFER];
    preamble.extend_from_slice(token);
    within(stream.write_all(&preamble))
        .await
        .expect("transfer preamble");
    stream
}

fn unix_socket(harness: &Harness) -> PathBuf {
    harness
        .socket_path()
        .expect("a unix harness has a socket")
        .to_path_buf()
}

#[tokio::test]
async fn a_local_peer_that_exits_after_its_hello_is_torn_down() {
    let harness = Harness::start(Transport::Unix).await;
    let acceptor = harness.listener.acceptor("/q").expect("acceptor");
    let path = unix_socket(&harness);

    // The peer's HELLO, and the control socket is then left open: it is the
    // peer's end of the link, and the server reads it to learn of the exit.
    let (mut control, token) = dial_control(&path).await;
    within(control.write_all(&chunk(&hello_frame())))
        .await
        .expect("control HELLO");

    // One SUBSCRIBE on a transfer connection, then the FIN.
    let mut transfer = dial_transfer(&path, &token).await;
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

#[tokio::test]
async fn an_idle_local_connection_is_closed_after_the_hello_timeout() {
    let harness = Harness::start_with(
        Transport::Unix,
        RuntimeConfig {
            limits: Limits {
                hello_timeout_ms: 300,
                ..Limits::default()
            },
            ..RuntimeConfig::default()
        },
    )
    .await;
    let path = unix_socket(&harness);

    // Connected, and silent: the server must give up on the preamble and close.
    let mut idle = within(UnixStream::connect(&path)).await.expect("connect");
    let mut byte = [0u8; 1];
    let read = within_for(Duration::from_secs(3), idle.read(&mut byte))
        .await
        .expect("read");
    assert_eq!(read, 0, "an idle connection is closed, not left open");

    within(harness.shutdown()).await;
}

#[tokio::test]
async fn connections_over_max_connections_wait_for_a_place() {
    let harness = Harness::start_with(
        Transport::Unix,
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
    let path = unix_socket(&harness);

    // Two sockets that never send a preamble hold both places until their
    // HELLO timeout passes.
    let idle_a = within(UnixStream::connect(&path)).await.expect("connect");
    let idle_b = within(UnixStream::connect(&path)).await.expect("connect");

    // A third peer with a complete preamble: it must wait for a place rather
    // than be answered while the limit is full.
    let mut third = within(UnixStream::connect(&path)).await.expect("connect");
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
