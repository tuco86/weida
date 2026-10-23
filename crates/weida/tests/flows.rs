//! Datagram flows over QUIC
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.2,
//! §4.5): registration, delivery, refusal, the per-flow drop-oldest ring, and
//! the capability that gates all of it.

use crate::common;

use std::time::Duration;

use common::Server;
use weida::{
    DEFAULT_DATAGRAM_RECEIVE_BYTES, Error, FlowMeta, Incoming, IncomingFlow, Limits, Runtime,
};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("a flow step", DEADLINE, f).await
}

/// A profile with flows on.
fn flows() -> Limits {
    Limits {
        datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
        ..Limits::default()
    }
}

/// A payload of `len` bytes whose first eight carry `index`.
fn numbered(index: u64, len: usize) -> Vec<u8> {
    let mut payload = vec![0xA5; len];
    payload[..8].copy_from_slice(&index.to_be_bytes());
    payload
}

fn index_of(datagram: &[u8]) -> u64 {
    u64::from_be_bytes(datagram[..8].try_into().expect("eight bytes"))
}

async fn next_flow(acceptor: &weida::Acceptor) -> IncomingFlow {
    match within(acceptor.accept()).await.expect("accept") {
        Incoming::Flow(flow) => flow,
        other => panic!("expected a flow, got {other:?}"),
    }
}

async fn dialled(server: &Server, client: &Runtime, path: &str) -> weida::Peer {
    let peer = client.peer(server.trust());
    within(peer.connect(&server.url(path)))
        .await
        .expect("connect");
    peer
}

#[tokio::test(start_paused = true)]
async fn a_flow_carries_datagrams_to_an_acceptor() {
    let server = Server::start_with(flows()).await;
    let acceptor = server.listener.acceptor("/v").expect("acceptor");
    let client = server.client_runtime_with(flows());
    let peer = dialled(&server, &client, "/v").await;

    let flow = within(peer.open_flow(FlowMeta::default().with_topic("mic")))
        .await
        .expect("open flow");
    let sender = tokio::spawn(async move {
        for i in 0..100u64 {
            flow.send(numbered(i, 200)).expect("send");
            // Voice paces itself; a burst at registration would outrun the
            // FLOW header and spend the early ring (0034 §4.2).
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        flow.stats()
    });

    let incoming = next_flow(&acceptor).await;
    assert_eq!(incoming.info().endpoint, "/v");
    assert_eq!(incoming.info().topic.as_deref(), Some("mic"));
    let mut seen = Vec::new();
    while let Some(datagram) = within(incoming.recv()).await {
        assert_eq!(datagram.len(), 200);
        assert_eq!(datagram, numbered(index_of(&datagram), 200));
        seen.push(index_of(&datagram));
    }
    let stats = sender.await.expect("sender");
    assert_eq!(stats.sent, 100);
    // Loopback loses none in practice; the bound is loose so the test does
    // not depend on the scheduler.
    assert!(seen.len() >= 90, "received {} of 100", seen.len());
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_flow_to_an_unregistered_path_is_refused() {
    let server = Server::start_with(flows()).await;
    let _acceptor = server.listener.acceptor("/v").expect("acceptor");
    let client = server.client_runtime_with(flows());
    let peer = dialled(&server, &client, "/nobody").await;

    let flow = within(peer.open_flow(FlowMeta::default()))
        .await
        .expect("registration waits for no answer");
    let refused = within(async {
        loop {
            match flow.send(&b"frame"[..]) {
                Ok(()) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(e) => return e,
            }
        }
    })
    .await;
    assert!(matches!(refused, Error::UnknownEndpoint), "{refused:?}");
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_stalled_flow_loses_its_oldest_and_its_sibling_keeps_receiving() {
    let server = Server::start_with(Limits {
        flow_queue_bytes: 2_000,
        ..flows()
    })
    .await;
    let acceptor = server.listener.acceptor("/v").expect("acceptor");
    let client = server.client_runtime_with(flows());
    let peer = dialled(&server, &client, "/v").await;

    let a = within(peer.open_flow(FlowMeta::default().with_topic("a")))
        .await
        .expect("open a");
    let b = within(peer.open_flow(FlowMeta::default().with_topic("b")))
        .await
        .expect("open b");
    let mut stalled = None;
    let mut drained = None;
    for _ in 0..2 {
        let flow = next_flow(&acceptor).await;
        match flow.info().topic.as_deref() {
            Some("a") => stalled = Some(flow),
            Some("b") => drained = Some(flow),
            other => panic!("unexpected topic {other:?}"),
        }
    }
    let (stalled, drained) = (stalled.expect("a"), drained.expect("b"));

    let reader = tokio::spawn(async move {
        let mut seen = Vec::new();
        while seen.len() < 50 {
            let datagram = drained.recv().await.expect("b ended early");
            seen.push(index_of(&datagram));
        }
        seen
    });
    for i in 0..50u64 {
        a.send(numbered(i, 200)).expect("send a");
        b.send(numbered(i, 200)).expect("send b");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let seen = within(reader).await.expect("reader");
    assert_eq!(seen, (0..50).collect::<Vec<_>>());

    // Every datagram of `a` was sent before its twin on `b`, so by now `a`'s
    // ring holds what 2000 bytes hold, the newest, and nothing older.
    let mut held = Vec::new();
    while let Some(datagram) = stalled.try_recv() {
        held.push(index_of(&datagram));
    }
    assert!(!held.is_empty());
    assert!(held[0] > 0, "the oldest survived: {held:?}");
    assert!(held.windows(2).all(|w| w[0] < w[1]), "{held:?}");
    assert!(
        stalled.stats().overflow >= 40,
        "{:?} with {held:?} held",
        stalled.stats()
    );
    client.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_runtime_without_datagrams_cannot_open_a_flow() {
    let server = Server::start_with(flows()).await;
    let acceptor = server.listener.acceptor("/v").expect("acceptor");
    let client = server.client_runtime();
    let peer = dialled(&server, &client, "/v").await;

    let failed = within(peer.open_flow(FlowMeta::default()))
        .await
        .expect_err("no capability, no flow");
    assert!(matches!(failed, Error::DatagramsUnavailable), "{failed:?}");
    // And nothing was sent instead: no stream stands in for the flow.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), acceptor.accept())
            .await
            .is_err()
    );
    client.shutdown().await;
}

#[tokio::test]
async fn a_payload_over_the_datagram_size_is_too_large() {
    let server = Server::start_with(flows()).await;
    let _acceptor = server.listener.acceptor("/v").expect("acceptor");
    let client = server.client_runtime_with(flows());
    let peer = dialled(&server, &client, "/v").await;

    let flow = within(peer.open_flow(FlowMeta::default()))
        .await
        .expect("open flow");
    let max = flow.max_payload().expect("datagrams agreed");
    let refused = flow.send(vec![0u8; max + 1]).expect_err("one byte over");
    assert!(
        matches!(refused, Error::TooLarge { max: m } if m == max),
        "{refused:?}"
    );
    assert_eq!(flow.stats().too_large, 1);
    flow.send(vec![0u8; max]).expect("exactly the maximum fits");
    client.shutdown().await;
}
