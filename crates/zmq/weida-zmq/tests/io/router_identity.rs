//! A ROUTER's routing ids and the users ZAP authenticated, through the public
//! API.
//!
//! A routing id is whatever a peer announces, and ZAP's user id is a fact the
//! server learned from a handler; nothing in ZeroMQ links them. These tests
//! run the two against each other over PLAIN with a real handler: one peer
//! authenticated as "mallory" announces the routing id of the peer
//! authenticated as "alice", with `ZMQ_ROUTER_HANDOVER` on — which libzmq
//! answers by handing the id, and the traffic addressed to it, to the
//! newcomer.

use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, DealerSocket, Multipart, RepSocket, RouterSocket, RoutingId,
    RoutingKey, SocketOptions, ZAP_ENDPOINT, ZapReply, ZapRequest, ZapStatus,
};

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

/// Bounds one await: a hang becomes a failure that names the step.
async fn bounded<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within 10 s"))
}

async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{what}: never held");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A ZAP handler that admits any PLAIN client whose password is `pw` and
/// names the user by the user name it presented.
async fn handler(context: &Context) -> tokio::task::JoinHandle<()> {
    let mut socket = RepSocket::new(context).expect("rep");
    socket.bind(ZAP_ENDPOINT).await.expect("bind the handler");
    tokio::spawn(async move {
        while let Ok(message) = socket.recv().await {
            let request = ZapRequest::decode(&message).expect("a ZAP request");
            let (username, password) = request.plain_credentials().expect("PLAIN credentials");
            let reply = if password == b"pw" {
                ZapReply::allowed(
                    request.request_id.clone(),
                    String::from_utf8_lossy(username).into_owned(),
                )
            } else {
                ZapReply::refused(
                    request.request_id.clone(),
                    ZapStatus::AuthenticationFailure,
                    "no such user",
                )
            };
            if socket.send(reply.encode()).await.is_err() {
                return;
            }
        }
    })
}

fn router(bind_identity_to_user: bool) -> SocketOptions {
    SocketOptions {
        plain_server: true,
        zap_domain: "test".to_owned(),
        router_handover: true,
        router_bind_identity_to_user: bind_identity_to_user,
        ..SocketOptions::default()
    }
}

/// A DEALER that authenticates as `user` and announces `routing_id`.
fn dealer(context: &Context, user: &str, routing_id: &[u8]) -> DealerSocket {
    DealerSocket::with_options(
        context,
        SocketOptions {
            plain_username: Some(user.to_owned()),
            plain_password: Some("pw".to_owned()),
            routing_id: Some(RoutingId::new(routing_id.to_vec()).expect("a routing id")),
            // One connection, so that a refusal is a refusal and not the
            // start of a reconnect loop.
            reconnect_ivl: None,
            ..SocketOptions::default()
        },
    )
    .expect("dealer")
}

fn framed(id: &[u8], body: &str) -> Multipart {
    let mut message = Multipart::single(id.to_vec());
    message.push(body);
    message
}

/// Claim: **with `router_bind_identity_to_user` a peer authenticated as one
/// user cannot take the routing id of another**, even with
/// `ZMQ_ROUTER_HANDOVER` on: the newcomer is disconnected, the owner of the id
/// stays connected, and a reply addressed to the id still reaches the owner.
#[tokio::test]
async fn a_bound_routing_id_stays_with_the_user_it_was_issued_to() {
    let ctx = context();
    let zap = handler(&ctx).await;
    let mut server = RouterSocket::with_options(&ctx, router(true)).expect("router");
    let endpoint = bounded("bind", server.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind")
        .to_string();

    let mut alice = dealer(&ctx, "alice", b"alice");
    alice.connect(&endpoint).expect("connect");
    bounded("alice sends", alice.send("hello"))
        .await
        .expect("send");
    let heard = bounded("the router hears alice", server.recv())
        .await
        .expect("a message");
    assert_eq!(heard.frames()[0].as_slice(), b"alice");

    let mallory = dealer(&ctx, "mallory", b"alice");
    mallory.connect(&endpoint).expect("connect");
    wait_for("mallory was admitted", || server.connections().len() == 2).await;
    // The ROUTER keys a peer when it is next asked, so ask until it has
    // decided about the newcomer.
    wait_for("the newcomer was refused", || {
        server.peers() == vec![RoutingKey::from_wire(b"alice")] && server.connections().len() == 1
    })
    .await;

    let kept = server.connections();
    assert_eq!(
        kept[0].user_id.as_ref().map(|user| user.as_str()),
        Some("alice"),
        "the connection that stayed is the owner's"
    );
    bounded("the reply", server.send(framed(b"alice", "for you")))
        .await
        .expect("send");
    let reply = bounded("alice hears it", alice.recv())
        .await
        .expect("a reply");
    assert_eq!(reply.frames()[0].as_slice(), b"for you");
    zap.abort();
}

/// Claim: without the option the ROUTER is libzmq's, and the same newcomer
/// takes the id and the incumbent is disconnected — the hijack the option
/// exists to close, kept as the default because libzmq has it.
#[tokio::test]
async fn without_binding_the_newcomer_takes_the_routing_id() {
    let ctx = context();
    let zap = handler(&ctx).await;
    let mut server = RouterSocket::with_options(&ctx, router(false)).expect("router");
    let endpoint = bounded("bind", server.bind("tcp://127.0.0.1:0"))
        .await
        .expect("bind")
        .to_string();

    let mut alice = dealer(&ctx, "alice", b"alice");
    alice.connect(&endpoint).expect("connect");
    bounded("alice sends", alice.send("hello"))
        .await
        .expect("send");
    bounded("the router hears alice", server.recv())
        .await
        .expect("a message");

    let mallory = dealer(&ctx, "mallory", b"alice");
    mallory.connect(&endpoint).expect("connect");
    wait_for("mallory was admitted", || server.connections().len() == 2).await;
    wait_for("alice was handed over", || {
        server.peers() == vec![RoutingKey::from_wire(b"alice")]
            && server.connections().len() == 1
            && server.connections()[0]
                .user_id
                .as_ref()
                .map(|user| user.as_str())
                == Some("mallory")
    })
    .await;
    zap.abort();
}
