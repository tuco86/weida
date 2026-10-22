//! The bound side of a dialler that stops answering, on the simulated
//! network: a partition is the dialler vanishing without a FIN or a close.

use crate::common;
use common::Server;
use std::time::Duration;
use weida::{Error, Limits, RuntimeConfig};

use crate::interrupted::*;

#[tokio::test(start_paused = true)]
async fn a_bound_side_observes_a_dead_dialler_within_the_idle_timeout() {
    // A reliable work chain needs bounded detection, and the knobs already
    // exist: `keep_alive` from the dialling side only, `idle_timeout` both
    // ways. Shortened here exactly as `runtime.rs`'s own test does, because
    // the defaults (10 s and 30 s) are too long for a test and the **bound**
    // is what is under test, not the number.
    let limits = Limits {
        keep_alive: Duration::from_millis(50),
        idle_timeout: Duration::from_millis(300),
        ..Limits::default()
    };
    let server = Server::start_with_config(RuntimeConfig {
        limits,
        ..RuntimeConfig::default()
    })
    .await;
    let paired = server.listener.pair("/link").expect("bound pair");
    let url = server.url("/link");

    let client = server.client_runtime_with(limits);
    let dialling = client.pair(server.trust());
    within(dialling.connect(&url)).await.expect("connect");
    within(dialling.send(b"alive")).await.expect("send");
    let inbound = within(paired.recv()).await.expect("recv");
    assert_eq!(
        within(inbound.collect(64)).await.expect("collect"),
        b"alive"
    );

    // The dialler vanishes: from here on nothing it sends arrives and nothing
    // reaches it, with no FIN and no close frame. Anything gentler would test
    // an orderly close, which the idle timeout is not for.
    server
        .net
        .as_ref()
        .expect("a simulated network")
        .partition(server.addr);

    // The bound side notices **within the timeout** rather than eventually,
    // which is the property a work chain depends on: a generous ten times the
    // idle timeout, so the assertion is about the bound and not about the
    // scheduler.
    let err = tokio::time::timeout(limits.idle_timeout * 10, async {
        loop {
            match paired.send(b"still there?").await {
                Ok(()) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(e) => return e,
            }
        }
    })
    .await
    .expect("the bound side observes the death inside ten idle timeouts");
    assert!(
        matches!(err, Error::ConnectionLost(_) | Error::NotConnected),
        "a dead dialler surfaces as a lost connection: {err:?}"
    );
    drop(dialling);
    client.shutdown().await;
}
