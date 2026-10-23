//! A server that accepts, says HELLO and hangs up, over and over: the dialling
//! side must keep its redial schedule across those connections instead of
//! starting it again at `initial` every time one is accepted.

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Certs, raw};
use quinn::VarInt;
use tokio::time::Instant;
use weida::{Network, ReconnectPolicy, Runtime, RuntimeConfig, SimNet};

/// Ceiling on any single await in this test.
const BOUND: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("a redial-storm step", BOUND, f).await
}

#[tokio::test(start_paused = true)]
async fn a_server_that_accepts_says_hello_and_hangs_up_is_redialled_ever_more_slowly() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let (endpoint, addr) = raw::sim_server_endpoint(&net, &certs);
    let accepted = Arc::new(AtomicUsize::new(0));

    // Every connection completes its handshake, says HELLO, and is closed
    // a few milliseconds later, before the peer could settle into it.
    let counter = Arc::clone(&accepted);
    let server = tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                raw::send_hello(&conn).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
                conn.close(VarInt::from_u32(0), b"hangs up");
            });
        }
    });

    let client = Runtime::new(RuntimeConfig {
        reconnect: ReconnectPolicy {
            initial: Duration::from_millis(20),
            max: Duration::from_millis(400),
            jitter: false,
            ..ReconnectPolicy::default()
        },
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let url = format!("weida://127.0.0.1:{}/jobs", addr.port());

    // The first dial is not delayed by the schedule.
    let started = Instant::now();
    within(pusher.connect(&url)).await.expect("first connect");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the first connect waited on the redial schedule"
    );

    // Four seconds of redials. The sleeps stay under two seconds each.
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let seen = accepted.load(Ordering::SeqCst);

    server.abort();
    within(client.shutdown()).await;

    assert!(
        seen >= 3,
        "the pusher did not redial a server that hangs up: {seen} connections"
    );
    assert!(
        seen <= 30,
        "a server that hangs up is redialled at the initial delay: {seen} connections in 4 s"
    );
}
