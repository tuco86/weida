//! The tests of `tests/pubsub.rs` that need the network, a process or real time.

use crate::common;
use common::Server;
use std::time::Duration;
use weida::Limits;

use crate::pubsub::*;

/// A streamed publish may be driven from any executor: its bounded wait is
/// timed on the runtime of the subscribers' connections, and creating a
/// timer on the caller's thread would panic on one that has no reactor.
#[tokio::test]
async fn a_streamed_publish_can_be_driven_without_a_reactor() {
    const BUDGET: usize = 64 * 1024;
    const CHUNK: usize = 16 * 1024;

    let server = Server::start_with(Limits {
        subscriber_buffer_bytes: BUDGET,
        ..Limits::default()
    })
    .await;
    let publisher = server.listener.publisher("/md").expect("publisher");
    // Never reads, so the budget fills and the wait is a real one.
    let client = server.client_runtime_with(Limits {
        connection_receive_window: 128 * 1024,
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    });
    let sub = client.subscriber(server.trust());
    within(sub.connect(&server.url("/md")))
        .await
        .expect("connect");
    within(sub.subscribe("")).await.expect("subscribe");
    await_filters(&publisher, 1).await;

    let mut fan = publisher.open("px.eur");
    let (done, outcome) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        // A plain thread: no Tokio runtime is entered on it.
        let chunk = vec![0x5au8; CHUNK];
        let remaining = futures::executor::block_on(async {
            let mut remaining = fan.subscribers();
            while remaining > 0 {
                remaining = fan
                    .write_within(chunk.clone(), Duration::from_millis(100))
                    .await
                    .expect("write");
            }
            remaining
        });
        let _ = done.send(remaining);
    });
    assert_eq!(
        within(outcome).await.expect("the thread did not panic"),
        0,
        "the subscriber that never reads loses the transfer"
    );

    client.shutdown().await;
}
