//! The tests of `tests/stats.rs` that need the network, a process or real time.

use crate::common;
use common::Server;
use std::time::Duration;
use weida::{RuntimeConfig, TransferMeta};

use crate::stats::*;

/// Claim: a requester sees its connection's path and traffic with no flow
/// on it, labelled by the URL exactly as dialled, and the record names no
/// address the application did not write.
///
/// The URL says `localhost`; the connection runs to `127.0.0.1`. Neither
/// that address nor `::1` may appear anywhere in the record.
#[tokio::test]
async fn a_requester_reads_its_connection_without_a_flow() {
    let server = Server::start_os().await;
    let replier = server.listener.replier("/echo").expect("replier");
    let serving = tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = request
                .take_body()
                .collect(1 << 20)
                .await
                .expect("request body");
            let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
            reply.write_all(&body).await.expect("write reply");
            reply.finish().expect("finish reply");
        }
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    assert!(
        requester.connection_stats().is_empty(),
        "nothing dialled, nothing to report"
    );

    let url = format!("weida://localhost:{}/echo", server.addr.port());
    within(requester.connect(&url)).await.expect("connect");
    let first = requester.connection_stats();
    assert_eq!(first.len(), 1, "one dialled address, one record");
    let first = &first[0];
    assert_eq!(&*first.url, url, "the label is the URL as given");
    assert_eq!(first.redials, 0, "the first dial is not a redial");
    let before = first.transport.expect("QUIC has a path");
    assert!(before.path.rtt > Duration::ZERO, "{before:?}");
    assert!(before.path.min_rtt > Duration::ZERO, "{before:?}");
    assert!(before.path.min_rtt <= before.path.rtt, "{before:?}");
    assert!(before.path.current_mtu >= 1200, "{before:?}");

    let body = vec![7u8; 64 * 1024];
    let reply = within(requester.request(&body)).await.expect("request");
    let echoed = within(reply.collect(1 << 20)).await.expect("reply body");
    assert_eq!(echoed.len(), body.len());

    let after = requester.connection_stats();
    let after = after[0].transport.expect("still QUIC");
    assert!(
        after.tx.bytes >= before.tx.bytes + body.len() as u64,
        "the request is in tx: {before:?} -> {after:?}"
    );
    assert!(
        after.rx.bytes >= before.rx.bytes + body.len() as u64,
        "the reply is in rx: {before:?} -> {after:?}"
    );
    assert!(after.tx.datagrams > before.tx.datagrams);
    assert!(after.rx.datagrams > before.rx.datagrams);
    assert!(after.path.sent_packets > before.path.sent_packets);

    let text = format!("{:?}", requester.connection_stats());
    for address in ["127.0.0.1", "::1"] {
        assert!(
            !text.contains(address),
            "the record names {address}, which the application never wrote: {text}"
        );
    }

    assert!(requester.disconnect(&url));
    assert!(
        requester.connection_stats().is_empty(),
        "a forgotten address is not reported"
    );

    client.shutdown().await;
    serving.abort();
}
/// Claim: with `path_report` on both sides, a dialling handle reads the
/// peer's view of the path — the peer's losses are this side's download loss
/// — and a record arrives at once, then every 2 s (0036 §4.5).
#[tokio::test]
async fn both_sides_reporting_give_the_dialling_side_the_peers_view() {
    let server = Server::start_os_with_config(RuntimeConfig {
        limits: reporting(),
        ..RuntimeConfig::default()
    })
    .await;
    let client = server.client_runtime_with(reporting());
    let peer = client.peer(server.trust());
    // Dialled by name, so the only label holds no address either.
    let url = format!("weida://localhost:{}/any", server.addr.port());
    within(peer.connect(&url)).await.expect("connect");

    let remote = within(async {
        loop {
            if let Some(remote) = peer.connection_stats()[0].remote {
                return remote;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(remote.rtt > Duration::ZERO, "{remote:?}");
    assert!(remote.current_mtu >= 1200, "{remote:?}");
    assert!(
        remote.tx.datagrams > 0 && remote.rx.datagrams > 0,
        "{remote:?}"
    );
    assert!(remote.age < Duration::from_secs(3), "{remote:?}");
    assert!(
        !format!("{:?}", peer.connection_stats()).contains("127.0.0.1"),
        "no address in the remote view either"
    );
    client.shutdown().await;
}
