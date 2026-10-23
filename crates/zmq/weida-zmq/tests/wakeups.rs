//! Waits that must not sleep through what they wait for, on a runtime with
//! threads.
//!
//! A socket waiting for a message or for room parks on two shared bells, and
//! the peer that ends the wait — arriving, connecting, announcing — is
//! handled by another worker thread. These tests put one in each place a
//! lost wake-up could hide: the receive parked with no peer at all, the send
//! blocked with no peer at all, and a ROUTER waiting for a peer it has not
//! learned yet. Each round is a brand-new socket and a brand-new peer, so
//! the timing differs every time; a wait that sleeps through its event
//! never ends, which the wait helper reports as it goes on waiting and the
//! job's own budget finally ends.
//!
//! The deterministic form of the same claims, with the interleaving forced
//! by a hook, is in `src/socket.rs`; this is the one that needs real threads.

use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, DealerSocket, PullSocket, PushSocket, RouterSocket, SocketOptions,
};

/// Rounds per test. Enough to land on the interesting interleavings and few
/// enough that a run is a second or two.
const ROUNDS: usize = 200;

/// How long one wait is expected to take; past it the wait reports and
/// keeps waiting, since a limit on a real clock would fail a slow run.
const BOUND: Duration = Duration::from_secs(10);

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

/// Claim: a receive parked with no peer is woken by a peer that arrives and
/// sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_receive_wakes_for_a_peer_that_arrives_and_sends() {
    let ctx = context();
    for round in 0..ROUNDS {
        let name = format!("inproc://wakeups.receive.{round}");
        let mut sink = PullSocket::new(&ctx).expect("pull");
        sink.bind(&name).await.expect("bind");
        let receiving = tokio::spawn(async move {
            let got = sink.recv().await;
            (sink, got)
        });
        tokio::task::yield_now().await;

        let mut source = PushSocket::new(&ctx).expect("push");
        source.connect(&name).expect("connect");
        source.send("hello").await.expect("send");

        let (_sink, got) = weida_testkit::within("the receiving task", BOUND, receiving)
            .await
            .expect("the task");
        let message = got.unwrap_or_else(|e| panic!("round {round}: the receive slept: {e}"));
        assert_eq!(message.frames()[0].as_slice(), b"hello");
    }
}

/// Claim: a send blocked with no peer is woken by a peer that arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_send_wakes_for_a_peer_that_arrives() {
    let ctx = context();
    for round in 0..ROUNDS {
        let name = format!("inproc://wakeups.send.{round}");
        let mut source = PushSocket::new(&ctx).expect("push");
        source.bind(&name).await.expect("bind");
        let sending = tokio::spawn(async move {
            let sent = source.send("work").await;
            (source, sent)
        });
        tokio::task::yield_now().await;

        let mut sink = PullSocket::new(&ctx).expect("pull");
        sink.connect(&name).expect("connect");
        let message = weida_testkit::within("the work to arrive", BOUND, sink.recv())
            .await
            .unwrap_or_else(|e| panic!("round {round}: the work never arrived: {e}"));
        assert_eq!(message.frames()[0].as_slice(), b"work");

        let (_source, sent) = weida_testkit::within("the sending task", BOUND, sending)
            .await
            .expect("the task");
        sent.unwrap_or_else(|e| panic!("round {round}: the send slept: {e}"));
    }
}

/// Claim: a ROUTER waiting for a peer it has not learned yet hears that
/// peer's first message, whichever worker thread accepts and announces it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_router_waiting_hears_a_peer_that_arrives_and_speaks() {
    let ctx = context();
    for round in 0..ROUNDS {
        let name = format!("inproc://wakeups.router.{round}");
        let mut router = RouterSocket::new(&ctx).expect("router");
        router.bind(&name).await.expect("bind");
        let listening = tokio::spawn(async move {
            let got = router.recv().await;
            (router, got)
        });
        tokio::task::yield_now().await;

        let mut dealer =
            DealerSocket::with_options(&ctx, SocketOptions::default()).expect("dealer");
        dealer.connect(&name).expect("connect");
        dealer.send("hello").await.expect("send");

        let (_router, got) = weida_testkit::within("the listening task", BOUND, listening)
            .await
            .expect("the task");
        let message = got.unwrap_or_else(|e| panic!("round {round}: the ROUTER slept: {e}"));
        assert_eq!(message.len(), 2, "routing id and body");
        assert_eq!(message.frames()[1].as_slice(), b"hello");
    }
}
