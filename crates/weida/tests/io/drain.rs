//! The tests of `tests/drain.rs` that need the network, a process or real time.

use std::time::Duration;
use weida::{Identity, Runtime, RuntimeConfig, TransferMeta, Trust};

use crate::drain::*;

/// Claim: a drain that cannot finish returns at its deadline with a non-zero
/// outstanding count, and that is not an error.
///
/// The peer here is frozen rather than merely slow, and the distinction is
/// the interesting part: QUIC acknowledges bytes into the receive window
/// whether or not the application reads them, so a slow *reader* still
/// settles a finished transfer. What leaves one outstanding is an
/// acknowledgement that never comes. The peer is therefore given its own
/// single-threaded reactor, and that thread is blocked outright: the socket
/// stays open, the connection stays up, and nothing is read or acknowledged.
#[tokio::test]
async fn a_drain_against_a_peer_that_reads_nothing_expires_with_a_count() {
    /// Long enough to cover the drain below with room to spare.
    const FREEZE: Duration = Duration::from_secs(2);

    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (freeze_tx, freeze_rx) = std::sync::mpsc::channel::<()>();

    let peer = std::thread::spawn(move || {
        let reactor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("peer reactor");
        reactor.block_on(async move {
            let runtime = Runtime::new(narrow()).expect("peer runtime");
            let listener = runtime.listener();
            let binding = listener
                .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
                .await
                .expect("bind");
            let _puller = listener.puller("/frozen").expect("puller");
            addr_tx
                .send(binding.local_addr())
                .expect("hand over the port");
            // Waiting off-reactor, so the peer still serves until the signal.
            let _ = tokio::task::spawn_blocking(move || freeze_rx.recv()).await;
            // And then it serves nothing at all.
            std::thread::sleep(FREEZE);
        });
    });

    let addr = addr_rx.recv().expect("peer port");
    let url = format!("weida://127.0.0.1:{}/frozen", addr.port());
    let client = Runtime::new(narrow()).expect("client runtime");
    let pusher = client.pusher(Trust::pin(fingerprint));
    within(pusher.connect(&url)).await.expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"a payload nobody will confirm"))
        .await
        .expect("write");

    freeze_tx.send(()).expect("freeze the peer");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The FIN goes into the silence, and nobody holds its receipt.
    drop(transfer.finish().expect("finish"));
    drop(pusher);

    // The smallest deadline that demonstrates expiry: long enough not to be
    // a scheduling race, short enough to cost a fraction of a second.
    let started = std::time::Instant::now();
    let drained = within(client.drain(Duration::from_millis(200))).await;
    assert_eq!(
        drained.outstanding, 1,
        "the unacknowledged transfer must be counted: {drained:?}"
    );
    assert_eq!(drained.delivered, 0);
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the drain must have waited for its deadline"
    );
    peer.join().expect("peer thread");
}
/// Claim: a draining binding refuses a **new connection** rather than
/// accepting it and then closing it ([0009](../../../docs/decisions/0009-drain.md)
/// §4.5). Admission stops first, and it stops for connections and not only
/// for streams.
///
/// The control is in the same test: the same client dials one path before the
/// drain and another during it. One connection per dialled path means the
/// second dial is a real handshake against the binding rather than a reuse of
/// the first.
#[tokio::test]
async fn a_draining_binding_refuses_a_new_connection() {
    let server = Draining::start().await;
    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let pusher = client.pusher(Trust::by_address());

    within(pusher.connect(&format!("{}/early", server.url)))
        .await
        .expect("a dial before the drain must be accepted");

    let drain = server.begin(Duration::from_secs(2)).await;

    let err = within(pusher.connect(&format!("{}/late", server.url)))
        .await
        .expect_err("a dial during the drain must be refused");
    assert!(
        !drain.is_finished(),
        "the refusal must have been observed while the drain was still running"
    );
    // What the refusal is *not*: a completed handshake that is then closed.
    // quinn refuses the attempt outright, so the dialling side never has a
    // connection at all.
    assert_eq!(
        pusher.peer_count(),
        1,
        "only the early path connected: {err:?}"
    );

    let drained = within(drain).await.expect("drain task");
    assert_eq!(drained.outstanding, 1, "{drained:?}");
    client.shutdown().await;
    server.peer.join().expect("peer thread");
}
/// Claim: while a drain waits, a **new stream on a connection that already
/// exists** is refused with `SHUTDOWN`, and the sender learns it as a refusal
/// rather than as an unexplained stop code
/// ([0009](../../../docs/decisions/0009-drain.md) §4.5).
///
/// The receipt is what carries the answer: `send` does not await one, so the
/// transfer is opened, written and finished by hand and the refusal is read
/// off `Delivery::delivered`.
#[tokio::test]
async fn a_draining_runtime_refuses_a_late_stream_on_an_open_connection() {
    let server = Draining::start().await;
    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let pusher = client.pusher(Trust::by_address());
    within(pusher.connect(&format!("{}/early", server.url)))
        .await
        .expect("connect");

    // A transfer before the drain is served, so the refusal below is the
    // drain's answer and not a broken setup.
    let mut accepted = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(accepted.write_all(b"in time")).await.expect("write");
    within(accepted.finish().expect("finish").delivered())
        .await
        .expect("a transfer before the drain is acknowledged");

    let drain = server.begin(Duration::from_secs(2)).await;

    let mut late = within(pusher.open(TransferMeta::default()))
        .await
        .expect("the stream budget is local, so opening still succeeds");
    // **Past the stream window on purpose.** Eight bytes fit in the peer's
    // window, so their FIN can be acknowledged before the drain's refusal
    // travels back and the receipt then reports `Ok` — which is
    // [0005](../../../docs/decisions/0005-refusal-race.md)'s race, not a
    // defect, and this test lost that coin flip on Windows while passing on
    // Linux. 0005 states the rule that makes it deterministic: a refusal is
    // guaranteed beyond the peer's stream window, and `stream_receive_window`
    // defaults to 1 MiB.
    let past_the_window = vec![0x7au8; 2 * 1024 * 1024];
    let err = match within(late.write_all(&past_the_window)).await {
        Err(e) => e,
        Ok(()) => match late.finish() {
            Ok(receipt) => within(receipt.delivered())
                .await
                .expect_err("a late transfer must be refused"),
            Err(e) => e,
        },
    };
    assert!(
        matches!(err, weida::Error::Rejected),
        "STOP_SENDING(SHUTDOWN) must reach the sender as a refusal, got {err:?}"
    );
    assert!(!drain.is_finished(), "the drain was still running");

    let drained = within(drain).await.expect("drain task");
    assert_eq!(drained.outstanding, 1, "{drained:?}");
    client.shutdown().await;
    server.peer.join().expect("peer thread");
}

/// Claim: a late **exchange** on an open connection is refused the way a late
/// one-way transfer is: the requester learns `Rejected`, whichever of the two
/// `SHUTDOWN` codes it meets first, the stop of its request or the reset of
/// the reply ([0009](../../../docs/decisions/0009-drain.md) §4.5).
#[tokio::test]
async fn a_draining_runtime_refuses_a_late_request_on_an_open_connection() {
    let server = Draining::start().await;
    let client = Runtime::new(RuntimeConfig::default()).expect("client");
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&format!("{}/early", server.url)))
        .await
        .expect("connect");

    let drain = server.begin(Duration::from_secs(2)).await;

    let err = within(requester.request(b"late"))
        .await
        .expect_err("a late exchange must be refused");
    assert!(
        matches!(err, weida::Error::Rejected),
        "SHUTDOWN must reach the requester as a refusal, got {err:?}"
    );
    assert!(!drain.is_finished(), "the drain was still running");

    within(drain).await.expect("drain task");
    client.shutdown().await;
    server.peer.join().expect("peer thread");
}
