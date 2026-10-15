//! L0 proof: the raw stream core, without any pattern on top.
//!
//! One `Acceptor` on one path takes both stream kinds; one `Peer` opens both.
//! Nothing here uses Req/Rep, Push/Pull or Pub/Sub — the point is that those
//! are wrappers, and that the primitives underneath are public and complete.

use crate::common;

use std::time::Duration;

use common::Server;
use weida::{Incoming, TransferMeta};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

#[tokio::test]
async fn acceptor_receives_both_stream_kinds() {
    let server = Server::start().await;
    let acceptor = server.listener.acceptor("/raw").expect("acceptor");

    let served = tokio::spawn(async move {
        let mut kinds = Vec::new();
        for _ in 0..2 {
            match within(acceptor.accept()).await.expect("accept") {
                Incoming::Stream(transfer) => {
                    let body = within(transfer.collect(1024)).await.expect("collect");
                    kinds.push(("stream", body));
                }
                Incoming::Exchange(mut request) => {
                    let body = within(request.body().read_capped(1024))
                        .await
                        .expect("read request");
                    let mut reply = within(request.reply(TransferMeta::default()))
                        .await
                        .expect("open reply");
                    within(reply.write_all(&body.to_ascii_uppercase()))
                        .await
                        .expect("write reply");
                    reply.finish().expect("finish reply");
                    kinds.push(("exchange", body));
                }
                // The three subscription events an L2 queue is served with.
                // This test drives no consumer, so seeing one is a
                // routing bug rather than something to handle.
                other => panic!("unexpected event on a raw acceptor: {other:?}"),
            }
        }
        kinds.sort();
        kinds
    });

    let client = server.client_runtime();
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url("/raw")))
        .await
        .expect("connect");
    assert_eq!(peer.peer_count(), 1);

    // A unidirectional stream: payload only, and a transport receipt.
    let mut uni = within(peer.open(TransferMeta::default()))
        .await
        .expect("open uni");
    within(uni.write_all(b"alpha")).await.expect("write uni");
    within(uni.finish().expect("finish uni").delivered())
        .await
        .expect("delivered");

    // A bidirectional stream: the exchange primitive, correlated by the stream
    // itself.
    let (mut bi, reply) = within(peer.open_bi(TransferMeta::default()))
        .await
        .expect("open bi");
    within(bi.write_all(b"bravo")).await.expect("write bi");
    bi.finish().expect("finish bi");
    let echoed = within(
        within(reply.recv())
            .await
            .expect("recv reply")
            .collect(1024),
    )
    .await
    .expect("collect reply");
    assert_eq!(echoed, b"BRAVO");

    assert_eq!(
        served.await.expect("acceptor task"),
        vec![
            ("exchange", b"bravo".to_vec()),
            ("stream", b"alpha".to_vec()),
        ]
    );

    client.shutdown().await;
}

#[tokio::test]
async fn an_acceptor_path_is_claimed_like_any_other() {
    let server = Server::start().await;
    let _acceptor = server.listener.acceptor("/raw").expect("acceptor");
    assert!(matches!(
        server.listener.replier("/raw"),
        Err(weida::Error::AlreadyRegistered)
    ));
    assert!(matches!(
        server.listener.acceptor("no-slash"),
        Err(weida::Error::InvalidEndpointPath)
    ));
}

/// A weida binding does not advertise `max_datagram_frame_size`, so a peer
/// may send it no datagrams and quinn buffers none (0034 §2.3).
#[tokio::test]
async fn a_weida_binding_advertises_no_datagrams() {
    let server = Server::start().await;
    let endpoint = common::raw::client_endpoint(&server.certs);
    let conn = within(endpoint.connect(server.addr, "127.0.0.1").expect("connect"))
        .await
        .expect("handshake");
    assert_eq!(conn.max_datagram_size(), None);
}
