//! The tests of `tests/local_transports.rs` that need the network, a process or real time.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use weida_nng::{Admission, PipeEvent, PipeInfo};
use weida_nng::{
    Context, ContextConfig, Error, PullSocket, PushSocket, RepSocket, ReqSocket, SocketOptions,
};

use crate::local_transports::*;

/// Claim: over `tcp://` the same limit is enforced, so the previous test
/// is about the transport rather than about the option being dead.
#[tokio::test]
async fn tcp_enforces_the_limit_inproc_ignores() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let tiny = SocketOptions {
        recv_max_size: 16,
        ..options()
    };
    let sink = PullSocket::with_options(&ctx, tiny).expect("pull");
    let url = sink
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let source = PushSocket::with_options(&ctx, options()).expect("push");
    source.dial(&url).await.expect("dial");
    while sink.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    source.send(vec![0u8; 1024]).await.expect("send");
    for _ in 0..200 {
        if sink.pipe_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        sink.pipe_count(),
        0,
        "a message past RECVMAXSZ costs the pipe over a transport that leaves the process"
    );
}
/// Claim: `ipc://` carries the protocol, and the kernel's UID, GID and PID
/// reach the pipe-add-pre callback — which is where an allow-list runs, as
/// application policy rather than SP authorization (§10).
#[cfg(unix)]
#[tokio::test]
async fn ipc_hands_the_kernels_credentials_to_the_add_pre_hook() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let path = temp_socket("creds");
    let url = format!("ipc://{}", path.display());

    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let seen: Arc<std::sync::Mutex<Option<weida_core::LocalPrincipal>>> =
        Arc::new(std::sync::Mutex::new(None));
    let recorded = Arc::clone(&seen);
    rep.notify(Arc::new(move |event, info: &PipeInfo| {
        if event == PipeEvent::AddPre {
            *recorded.lock().unwrap() = info.credentials;
        }
        Admission::Accept
    }));
    rep.listen(&url).await.expect("listen");

    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial(&url).await.expect("dial");
    req.send(b"who".to_vec()).await.expect("send");
    let request = rep.recv().await.expect("recv");
    assert_eq!(request.body(), b"who");
    rep.send(b"me".to_vec()).await.expect("reply");
    assert_eq!(req.recv().await.expect("reply").body(), b"me");

    let principal = seen.lock().unwrap().expect("the kernel named the peer");
    assert_eq!(principal.pid, Some(std::process::id()));
    assert!(
        rep.pipe_infos()[0].credentials.is_some(),
        "and the credentials stay on the pipe for anybody who asks later"
    );

    // The dialling side has credentials too — the kernel answers both ends.
    assert!(req.pipe_infos()[0].credentials.is_some());
}
/// Claim: an allow-list at the add-pre hook refuses a peer, and the
/// refusal is a close with nothing sent back (§6, §10).
#[cfg(unix)]
#[tokio::test]
async fn an_allow_list_at_the_hook_refuses_an_ipc_peer() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let path = temp_socket("refuse");
    let url = format!("ipc://{}", path.display());

    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let refusals = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&refusals);
    rep.notify(Arc::new(move |event, info: &PipeInfo| {
        if event != PipeEvent::AddPre {
            return Admission::Accept;
        }
        // An allow-list on the one thing the kernel cannot forge.
        match info.credentials {
            Some(principal) if principal.uid == u32::MAX => Admission::Accept,
            _ => {
                counter.fetch_add(1, Ordering::SeqCst);
                Admission::Reject("uid is not on the allow-list".into())
            }
        }
    }));
    rep.listen(&url).await.expect("listen");

    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    let refused = req.dial(&url).await.unwrap_err();
    assert!(
        matches!(
            refused,
            Error::ECONNRESET(_) | Error::ECONNABORTED(_) | Error::ETIMEDOUT(_)
        ),
        "{refused:?}"
    );
    assert_eq!(refusals.load(Ordering::SeqCst), 1);
    assert_eq!(rep.pipe_count(), 0);
}
/// Claim: the socket file is removed when the listener goes, so the next
/// bind of the same path is not a stale-node race.
#[cfg(unix)]
#[tokio::test]
async fn closing_an_ipc_listener_removes_its_node() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let path = temp_socket("cleanup");
    let url = format!("ipc://{}", path.display());

    let sink = PullSocket::with_options(&ctx, options()).expect("pull");
    let listener = sink.listen(&url).await.expect("listen");
    assert!(path.exists());

    listener.close();
    for _ in 0..200 {
        if !path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!path.exists(), "the node went with the listener");

    // And the path is bindable again.
    let again = PullSocket::with_options(&ctx, options()).expect("pull");
    again.listen(&url).await.expect("bind again");
    again.close();
}
