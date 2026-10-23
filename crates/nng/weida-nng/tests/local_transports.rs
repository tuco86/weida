//! `inproc://` and `ipc://`, end to end, with the protocols that run over
//! them.

use std::time::Duration;

use weida_nng::{
    Context, ContextConfig, Error, PullSocket, PushSocket, RepSocket, ReqSocket, SocketOptions,
};

pub(crate) fn options() -> SocketOptions {
    SocketOptions {
        // No socket timeout: a slow run is reported by the test's own waits,
        // not failed by an option. The handshake bound is an hour, not the
        // library's ten seconds.
        recv_timeout: None,
        send_timeout: None,
        handshake_timeout: Duration::from_secs(3600),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

#[cfg(unix)]
pub(crate) fn temp_socket(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "weida-nng-it-{}-{}-{name}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    path
}

/// Claim: the protocol layer is the same code over `inproc://` as over
/// `tcp://` — a REQ/REP round trip runs unchanged, handshake and framing
/// included.
#[tokio::test]
async fn a_request_and_reply_run_over_inproc() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    rep.listen("inproc://orders").await.expect("listen");
    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial("inproc://orders").await.expect("dial");

    req.send(b"ping".to_vec()).await.expect("send");
    let request = rep.recv().await.expect("recv");
    assert_eq!(request.body(), b"ping");
    rep.send(b"pong".to_vec()).await.expect("reply");
    assert_eq!(req.recv().await.expect("reply").body(), b"pong");
}

/// Claim: the namespace is scoped to the context — a name bound in one is
/// invisible in another — and one owner per name.
#[tokio::test]
async fn the_inproc_namespace_belongs_to_the_context() {
    let first = Context::new(ContextConfig::default()).expect("context");
    let second = Context::new(ContextConfig::default()).expect("context");

    let holder = PullSocket::with_options(&first, options()).expect("pull");
    holder.listen("inproc://shared").await.expect("listen");

    // The same name in the same context is taken.
    let intruder = PullSocket::with_options(&first, options()).expect("pull");
    let err = intruder.listen("inproc://shared").await.unwrap_err();
    assert!(matches!(err, Error::EADDRINUSE(_)), "{err:?}");

    // In another context it is free, and the two never meet.
    let elsewhere = PullSocket::with_options(&second, options()).expect("pull");
    elsewhere
        .listen("inproc://shared")
        .await
        .expect("a different namespace");

    let pusher = PushSocket::with_options(&second, options()).expect("push");
    pusher.dial("inproc://shared").await.expect("dial");
    pusher.send(b"mine".to_vec()).await.expect("send");
    assert_eq!(elsewhere.recv().await.expect("recv").body(), b"mine");
    assert!(
        holder.try_recv().is_err(),
        "a name in another context is another endpoint entirely"
    );
}

/// Claim: `inproc://` accepts `NNG_OPT_RECVMAXSZ` and deliberately ignores
/// it, for NNG's own reason — the peer shares this address space, so the
/// limit would defend against a thread of this process
/// (`docs/research/nanomsg-nng.md` §3, §11).
#[tokio::test]
async fn inproc_accepts_and_ignores_the_size_limit() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let tiny = SocketOptions {
        recv_max_size: 16,
        ..options()
    };
    // The option is accepted rather than refused: a program ported from NNG
    // keeps its configuration.
    let sink = PullSocket::with_options(&ctx, tiny.clone()).expect("pull");
    sink.listen("inproc://big").await.expect("listen");
    let source = PushSocket::with_options(&ctx, tiny).expect("push");
    source.dial("inproc://big").await.expect("dial");

    let payload = vec![0x7Eu8; 64 * 1024];
    source.send(payload.clone()).await.expect("send");
    let arrived = sink.recv().await.expect("64 KiB through a 16-byte limit");
    assert_eq!(arrived.body().len(), payload.len());
    assert_eq!(sink.pipe_count(), 1, "and the pipe is untouched");
}
