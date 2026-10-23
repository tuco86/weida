//! The tests of `tests/options.rs` that need the network, a process or real time.

use std::time::Duration;
use weida_nng::{Context, ContextConfig, EndpointOptions, PullSocket, PushSocket, SocketOptions};

/// Claim: `NNG_OPT_RECVMAXSZ` is settable **per endpoint before that
/// endpoint starts**, as the manual requires, and the endpoint's number
/// wins over the socket's — which is what makes two trust boundaries on
/// one socket possible (§3, §11).
#[tokio::test]
async fn recvmaxsz_is_settable_per_endpoint() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let generous = SocketOptions {
        recv_max_size: 1024 * 1024,
        // No socket timeout: a slow run is reported by the test's own waits,
        // not failed by an option. The handshake bound is an hour.
        recv_timeout: None,
        handshake_timeout: Duration::from_secs(3600),
        ..SocketOptions::default()
    };

    // The socket would take a mebibyte; this listener takes 64 bytes,
    // because it is the address a stranger reaches.
    let sink = PullSocket::with_options(&ctx, generous.clone()).expect("pull");
    let tight = sink
        .listen_with("tcp://127.0.0.1:0", EndpointOptions::with_recv_max_size(64))
        .await
        .expect("listen");
    let tight_url = tight.url().to_string();

    // And a second listener on the socket's own generous limit, to show
    // the two numbers coexist rather than one replacing the other.
    let roomy = sink
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let over = PushSocket::with_options(&ctx, generous.clone()).expect("push");
    over.dial(&tight_url).await.expect("dial");
    over.send(vec![0u8; 4096]).await.expect("send");
    weida_testkit::until(
        "the pipe being closed for the oversize message",
        Duration::from_secs(2),
        || over.pipe_count() == 0,
    )
    .await;
    assert_eq!(
        over.pipe_count(),
        0,
        "4 KiB past a 64-byte per-listener limit costs the pipe"
    );

    let under = PushSocket::with_options(&ctx, generous).expect("push");
    under.dial(&roomy).await.expect("dial");
    under.send(vec![0u8; 4096]).await.expect("send");
    assert_eq!(
        sink.recv().await.expect("the roomy listener took it").len(),
        4096,
        "the other listener still runs on the socket's own limit"
    );
}
