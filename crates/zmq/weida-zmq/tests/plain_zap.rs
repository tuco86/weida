//! PLAIN and the ZAP dialog through the public API.
//!
//! Two sockets, a handler on a REP socket over `inproc://zeromq.zap.01`, and
//! the questions an application can actually ask: does a good password get
//! through, does a bad one get refused before anything flows, and can the
//! application see who was let in.

use std::sync::{Arc, Mutex};

use weida_zmq::{
    Context, ContextConfig, Multipart, RepSocket, SocketOptions, ZAP_ENDPOINT, ZapReply, ZapRequest,
};

pub(crate) fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

pub(crate) fn text(message: &Multipart) -> Vec<u8> {
    message.frames()[0].as_slice().to_vec()
}

/// What the handler was asked, so that a test can check the request as well
/// as the outcome.
pub(crate) type Asked = Arc<Mutex<Vec<ZapRequest>>>;

/// Starts a ZAP handler that answers with `decide`.
///
/// Bound before it is spawned, which is 27/ZAP's "the handler SHALL start
/// before any server starts" — and also what keeps the task's future `Send`,
/// since a socket may be moved into a task but never shared with one.
pub(crate) async fn handler(
    context: &Context,
    decide: impl Fn(ZapRequest) -> ZapReply + Send + 'static,
) -> (Asked, tokio::task::JoinHandle<()>) {
    let asked: Asked = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&asked);
    let mut socket = RepSocket::new(context).expect("rep");
    socket.bind(ZAP_ENDPOINT).await.expect("bind the handler");
    let task = tokio::spawn(async move {
        while let Ok(message) = socket.recv().await {
            let request = ZapRequest::decode(&message).expect("a ZAP request");
            seen.lock().expect("seen").push(request.clone());
            if socket.send(decide(request).encode()).await.is_err() {
                return;
            }
        }
    });
    (asked, task)
}

pub(crate) fn plain_server(domain: &str) -> SocketOptions {
    SocketOptions {
        plain_server: true,
        zap_domain: domain.to_owned(),
        ..SocketOptions::default()
    }
}

pub(crate) fn plain_client(username: &str, password: &str) -> SocketOptions {
    SocketOptions {
        plain_username: Some(username.to_owned()),
        plain_password: Some(password.to_owned()),
        ..SocketOptions::default()
    }
}

/// Claim: the configurations that cannot be delivered are refused at
/// construction, which is this library's rule for every option — a socket
/// that is both PLAIN ends, a username with no password, and
/// `ZMQ_ZAP_ENFORCE_DOMAIN` with nothing to enforce.
#[tokio::test]
async fn unusable_security_configurations_are_refused() {
    let ctx = context();
    for (options, what) in [
        (
            SocketOptions {
                plain_server: true,
                ..plain_client("admin", "secret")
            },
            "both ends of PLAIN",
        ),
        (
            SocketOptions {
                plain_username: Some("admin".to_owned()),
                ..SocketOptions::default()
            },
            "a username with no password",
        ),
        (
            SocketOptions {
                plain_password: Some("secret".to_owned()),
                ..SocketOptions::default()
            },
            "a password with no username",
        ),
        (
            SocketOptions {
                zap_enforce_domain: true,
                ..SocketOptions::default()
            },
            "enforce with no domain",
        ),
        (
            SocketOptions {
                plain_username: Some("x".repeat(256)),
                plain_password: Some(String::new()),
                ..SocketOptions::default()
            },
            "a username past the field's length octet",
        ),
    ] {
        let err = RepSocket::with_options(&ctx, options).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{what}: {err}");
    }
}
