//! The tests of `tests/interrupted.rs` that need the network, a process or real time.

use crate::common;
use common::Server;
use std::time::Duration;
use weida::{Error, Limits, RuntimeConfig};

use crate::interrupted::*;

#[tokio::test]
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
    let server = Server::start_os_with_config(RuntimeConfig {
        limits,
        ..RuntimeConfig::default()
    })
    .await;
    let paired = server.listener.pair("/link").expect("bound pair");
    let url = server.url("/link");

    // The dialler gets its **own** reactor, which is the only way a test can
    // make a peer stop answering rather than close politely: dropping the
    // runtime stops the threads that drive the connection, so no FIN and no
    // close frame ever reaches the bound side. Anything gentler than this
    // tests an orderly close, which the idle timeout is not for.
    let client = weida::Runtime::owned(RuntimeConfig {
        limits,
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .expect("owned runtime");
    let dialling = client.pair(server.trust());
    within(dialling.connect(&url)).await.expect("connect");
    within(dialling.send(b"alive")).await.expect("send");
    let inbound = within(paired.recv()).await.expect("recv");
    assert_eq!(
        within(inbound.collect(64)).await.expect("collect"),
        b"alive"
    );

    // The dialler vanishes.
    drop(dialling);
    drop(client);

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
}
