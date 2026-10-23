//! weida driven from an executor that is not Tokio.
//!
//! The runtime owns its reactor ([`Runtime::owned`]), so every task, timer and
//! name lookup the library needs runs there, while the test itself drives the
//! futures under `futures::executor::block_on` and touches the payload through
//! the `futures-io` traits. Nothing here is a `#[tokio::test]`, and the
//! assertion at the top of each test is that no ambient reactor exists.

use crate::common;

use std::future::Future;
use std::time::Duration;

use common::Certs;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use weida::{Network, Runtime, RuntimeConfig, SimNet, TransferMeta};

/// The expected time of one test body. Past it a watchdog thread prints a
/// line at every further multiple and the body keeps running: a slow run is
/// not a failure. It is a thread because a timer would need the reactor this
/// test refuses to have.
const DEADLINE: Duration = Duration::from_secs(10);

/// Runs `body` to completion under a foreign executor, reporting when it
/// takes longer than `DEADLINE`.
fn within<F: Future>(body: F) -> F::Output {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let watchdog = std::thread::spawn(move || {
        let mut n = 0u32;
        while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(DEADLINE) {
            n += 1;
            eprintln!("still waiting for a foreign-executor test body, past {n} x {DEADLINE:?}");
        }
    });
    let out = futures::executor::block_on(body);
    drop(tx);
    let _ = watchdog.join();
    out
}

#[test]
fn a_req_rep_round_trip_runs_without_a_tokio_executor() {
    let net = SimNet::new();
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "this test must run with no ambient tokio runtime"
    );

    let certs = Certs::generate();
    let runtime = Runtime::owned(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("owned runtime");
    let listener = runtime.listener();
    let replier = listener.replier("/transform").expect("replier");

    within(async {
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                certs.server_tls(),
            )
            .await
            .expect("bind");
        let url = format!(
            "weida://127.0.0.1:{}/transform",
            binding.local_addr().port()
        );

        // The uppercasing handler, on the same executor as its client: no
        // spawn, because the caller has nothing to spawn onto.
        let server = async {
            let mut request = replier.accept().await.expect("accept");
            let mut body = request.take_body();
            let mut out = request
                .reply(TransferMeta::default())
                .await
                .expect("open reply");
            let mut received = Vec::new();
            // `futures-io`, not `tokio::io`: this is the trait pair the test
            // exists to prove.
            body.read_to_end(&mut received)
                .await
                .expect("read the request body");
            received.make_ascii_uppercase();
            AsyncWriteExt::write_all(&mut out, &received)
                .await
                .expect("write the reply");
            out.finish().expect("finish the reply");
        };

        let client = async {
            let requester = runtime.requester(certs.client_tls());
            requester.connect(&url).await.expect("connect");
            let (mut transfer, reply) = requester
                .open(TransferMeta::default())
                .await
                .expect("open the exchange");
            AsyncWriteExt::write_all(&mut transfer, b"hello weida")
                .await
                .expect("write the request");
            transfer.finish().expect("finish the request");

            let mut answer = reply.recv().await.expect("reply");
            let mut bytes = Vec::new();
            answer
                .read_to_end(&mut bytes)
                .await
                .expect("read the reply body");
            bytes
        };

        let ((), answer) = futures::future::join(server, client).await;
        assert_eq!(answer, b"HELLO WEIDA");

        runtime.shutdown().await;
    });
}

#[test]
fn a_push_pull_transfer_runs_without_a_tokio_executor() {
    let net = SimNet::new();
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "this test must run with no ambient tokio runtime"
    );

    let certs = Certs::generate();
    let runtime = Runtime::owned(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("owned runtime");
    let listener = runtime.listener();
    let puller = listener.puller("/jobs").expect("puller");

    within(async {
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                certs.server_tls(),
            )
            .await
            .expect("bind");
        let url = format!("weida://127.0.0.1:{}/jobs", binding.local_addr().port());

        let pusher = runtime.pusher(certs.client_tls());
        pusher.connect(&url).await.expect("connect");

        // Fire-and-forget on one side, `futures-io` on the other. Both halves
        // run on this executor, which owns no reactor.
        let send = async {
            let mut transfer = pusher
                .open(TransferMeta::default())
                .await
                .expect("open the transfer");
            AsyncWriteExt::write_all(&mut transfer, b"job one")
                .await
                .expect("write the payload");
            // The receipt is dropped: `finish` is synchronous and needs no
            // executor of its own.
            transfer.finish().expect("finish the transfer");
        };

        let receive = async {
            let mut incoming = puller.recv().await.expect("recv");
            let mut body = Vec::new();
            incoming
                .read_to_end(&mut body)
                .await
                .expect("read the payload");
            body
        };

        let ((), body) = futures::future::join(send, receive).await;
        assert_eq!(body, b"job one");

        runtime.shutdown().await;
    });
}

#[test]
fn a_pub_sub_fan_out_runs_without_a_tokio_executor() {
    let net = SimNet::new();
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "this test must run with no ambient tokio runtime"
    );

    let certs = Certs::generate();
    let runtime = Runtime::owned(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("owned runtime");
    let listener = runtime.listener();
    let publisher = listener.publisher("/md").expect("publisher");

    within(async {
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                certs.server_tls(),
            )
            .await
            .expect("bind");
        let url = format!("weida://127.0.0.1:{}/md", binding.local_addr().port());

        let subscriber = runtime.subscriber(certs.client_tls());
        subscriber.connect(&url).await.expect("connect");
        // The empty filter matches every topic under any filter grammar, so
        // this test does not move when the matcher does (grammar:
        // `docs/decisions/0007-topic-namespace.md`). Filtering itself is
        // `tests/pubsub.rs`'s job.
        subscriber.subscribe("").await.expect("subscribe");

        // SUBSCRIBE is a stream of its own, so it may still be in flight when
        // `publish` runs. Retrying until one subscriber is reached needs no
        // timer — which matters, because a timer would need the reactor this
        // test refuses to have.
        let mut reached = 0;
        while reached == 0 {
            reached = publisher.publish("px.eur", &b"4.02"[..]).expect("publish");
            if reached == 0 {
                std::thread::yield_now();
            }
        }
        assert_eq!(reached, 1);

        let mut incoming = subscriber.recv().await.expect("recv");
        assert_eq!(incoming.meta().topic.as_deref(), Some("px.eur"));
        let mut body = Vec::new();
        incoming
            .read_to_end(&mut body)
            .await
            .expect("read the payload");
        assert_eq!(body, b"4.02");

        runtime.shutdown().await;
    });
}
