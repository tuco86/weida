//! Push/Pull over real QUIC on loopback.
//!
//! Push is the one-way stream primitive: no reply half, no correlation, one
//! unidirectional QUIC stream per message. These tests assert the delivery
//! receipt, the round-robin peer policy, and that mixing patterns on one path
//! is refused rather than reinterpreted.

use crate::common;

use std::time::Duration;

use common::Server;
use weida::{Error, TransferMeta};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("a push/pull step", DEADLINE, f).await
}

#[tokio::test]
async fn push_delivery_receipt() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let payload = vec![0x5au8; 1024];
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(&payload)).await.expect("write");
    let delivery = transfer.finish().expect("finish");

    // The receipt is QUIC's fin-acknowledgement, not an application ack: 1 KiB
    // fits the flow-control window, so the peer's transport holds every byte
    // long before the puller asks for them. Awaiting it *before* `recv` is the
    // whole point — it must not deadlock, and it must not mean "the
    // application read it".
    within(delivery.delivered()).await.expect("delivered");

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(transfer.meta().endpoint.as_deref(), Some("/jobs"));
    assert_eq!(transfer.meta().topic, None);
    let body = within(transfer.collect(64 * 1024)).await.expect("collect");
    assert_eq!(body, payload);

    client.shutdown().await;
}

#[tokio::test]
async fn push_best_effort() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    // Pipeline semantics: `send` discards the receipt and returns at FIN.
    within(pusher.send(b"fire and forget")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    let body = within(transfer.collect(1024)).await.expect("collect");
    assert_eq!(body, b"fire and forget");

    client.shutdown().await;
}

#[tokio::test]
async fn push_round_robins_two_peers() {
    // One pusher, two peers, two distinct destination paths on one server.
    // A peer carries the path that was dialled on it, so which puller receives
    // a message *is* the observable output of the selection policy.
    let server = Server::start().await;
    let left = server.listener.puller("/left").expect("left");
    let right = server.listener.puller("/right").expect("right");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/left")))
        .await
        .expect("dial left");
    within(pusher.connect(&server.url("/right")))
        .await
        .expect("dial right");
    assert_eq!(pusher.peer_count(), 2);

    const TOTAL: usize = 8;
    for i in 0..TOTAL {
        within(pusher.send(format!("m{i}").as_bytes()))
            .await
            .expect("send");
    }

    // Round-robin over two peers splits an even count exactly in half. If
    // selection had collapsed onto one peer, the second `recv` below would
    // block until the deadline.
    let mut seen = Vec::new();
    for _ in 0..TOTAL / 2 {
        let t = within(left.recv()).await.expect("recv left");
        assert_eq!(t.meta().endpoint.as_deref(), Some("/left"));
        seen.push(within(t.collect(64)).await.expect("collect left"));
    }
    for _ in 0..TOTAL / 2 {
        let t = within(right.recv()).await.expect("recv right");
        assert_eq!(t.meta().endpoint.as_deref(), Some("/right"));
        seen.push(within(t.collect(64)).await.expect("collect right"));
    }

    // Every message arrived exactly once, and nowhere else.
    seen.sort();
    let mut expected: Vec<Vec<u8>> = (0..TOTAL).map(|i| format!("m{i}").into_bytes()).collect();
    expected.sort();
    assert_eq!(seen, expected);

    client.shutdown().await;
}

#[tokio::test]
async fn push_cancel_mid_transfer() {
    let server = Server::start().await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"partial")).await.expect("write");

    let mut inbound = within(puller.recv()).await.expect("recv");
    transfer.cancel();

    // The reset surfaces as `Canceled` on the reading side, not as EOF: a
    // canceled push must never look like a complete one.
    let err = within(inbound.read_capped(64 * 1024))
        .await
        .expect_err("a canceled transfer must not read as complete");
    assert!(matches!(err, Error::Canceled), "{err:?}");

    // The connection survives: the next push is served normally.
    within(pusher.send(b"next")).await.expect("send");
    let next = within(puller.recv()).await.expect("recv next");
    assert_eq!(within(next.collect(64)).await.expect("collect"), b"next");

    client.shutdown().await;
}

#[tokio::test]
async fn request_to_pull_path_is_unsupported() {
    let server = Server::start().await;
    let _puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/jobs")))
        .await
        .expect("connect");

    // The path exists but serves one-way transfers. Refusing beats guessing,
    // and the exchange's own reply half carries the reason.
    let err = within(requester.request(b"hello"))
        .await
        .expect_err("a request to a pull path must be refused");
    assert!(matches!(err, Error::Unsupported), "{err:?}");

    client.shutdown().await;
}

/// The 2 MiB payload is what makes this refusal deterministic rather than
/// racy: past the peer's stream receive window the write cannot finish unless
/// its application acts, so the stop code is the only way out
/// (`docs/decisions/0005-refusal-race.md` §4.3). A few bytes would fit in
/// flight and could be acknowledged before the refusal.
#[tokio::test]
async fn push_to_rep_path_is_unsupported() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/rpc").expect("replier");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/rpc")))
        .await
        .expect("connect");

    // A one-way stream has no reply half, so the refusal is the stop code.
    assert!(
        matches!(
            within(push_and_confirm(&pusher, &beyond_the_window())).await,
            Err(Error::Unsupported)
        ),
        "a push to a replier path must be refused with Unsupported"
    );

    client.shutdown().await;
}

/// Deterministic for the same reason as the test above: the 2 MiB payload
/// exceeds the peer's stream receive window, so the refusal cannot be
/// overtaken by the transport receipt
/// (`docs/decisions/0005-refusal-race.md` §4.3).
#[tokio::test]
async fn push_to_an_unknown_path_is_reported() {
    let server = Server::start().await;
    let _puller = server.listener.puller("/jobs").expect("puller");

    let client = server.client_runtime();
    let pusher = client.pusher(server.trust());
    within(pusher.connect(&server.url("/nope")))
        .await
        .expect("connect");

    assert!(
        matches!(
            within(push_and_confirm(&pusher, &beyond_the_window())).await,
            Err(Error::UnknownEndpoint)
        ),
        "an unknown path must be reported"
    );

    client.shutdown().await;
}

/// A payload larger than the default stream receive window.
///
/// A refusal is only guaranteed to be *observed* when the transfer cannot
/// complete without the peer's application acting: a payload that fits in
/// flight can be acknowledged by the peer's transport before its application
/// refuses it, and the receipt then says "delivered" — truthfully, since a
/// transport receipt says nothing about the application. Past the window the
/// write blocks until the peer reads or refuses.
fn beyond_the_window() -> Vec<u8> {
    vec![0u8; 2 * 1024 * 1024]
}

/// Pushes `body` and waits for the transport receipt.
///
/// A refusal races the write: `STOP_SENDING` may arrive mid-write or only after
/// the FIN, so both points are checked and the error value is what matters.
async fn push_and_confirm(pusher: &weida::Pusher, body: &[u8]) -> Result<(), Error> {
    let mut transfer = pusher.open(TransferMeta::default()).await?;
    transfer.write_all(body).await?;
    transfer.finish()?.delivered().await
}
