//! A dialled address outlives its connection
//! ([0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)).
//!
//! What is asserted is the redial, outbox and re-subscribe contract and nothing about its
//! timing: every server here is restarted on the **same** address, and a
//! test waits on the events the endpoint reports rather than on a clock.
//! The redial policy is set to a few milliseconds so the suite runs in
//! seconds, and `jitter` stays on because a policy nobody runs with jitter
//! would not be the default one.

use crate::common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::Certs;
use tokio::time::timeout;
use weida::{
    Acceptor, Binding, DEFAULT_DATAGRAM_RECEIVE_BYTES, Error, FlowMeta, GiveUp, Incoming,
    IncomingFlow, Limits, Listener, LossCause, Network, OutboxFull, PeerEvent, PeerEvents, Puller,
    ReconnectPolicy, Runtime, RuntimeConfig, SimNet,
};

pub(crate) const DEADLINE: Duration = Duration::from_secs(15);

pub(crate) async fn within<F: Future>(f: F) -> F::Output {
    timeout(DEADLINE, f).await.expect("operation timed out")
}

/// A client configuration whose redial is fast enough to test and still
/// jittered: the default shape at a hundredth of the default numbers.
pub(crate) fn client_config() -> RuntimeConfig {
    RuntimeConfig {
        reconnect: ReconnectPolicy {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(50),
            ..ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    }
}

/// A QUIC server that can be stopped and started again on the same port
/// with the same, or a different, identity.
pub(crate) struct Server {
    pub(crate) _runtime: Runtime,
    pub(crate) listener: Listener,
    pub(crate) binding: Binding,
    pub(crate) addr: SocketAddr,
}

impl Server {
    pub(crate) async fn start(network: Network, certs: &Certs, addr: SocketAddr) -> Server {
        let runtime = Runtime::new(RuntimeConfig {
            network,
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let listener = runtime.listener();
        // A restart re-binds the port the old server just released; the OS
        // may still be handing it back, so the bind is retried briefly.
        let binding = within(async {
            loop {
                match listener.bind_quic(addr, certs.server_tls()).await {
                    Ok(binding) => break binding,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await;
        let addr = binding.local_addr();
        Server {
            _runtime: runtime,
            listener,
            binding,
            addr,
        }
    }

    pub(crate) fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }

    /// Closes the binding and lets the address go, the way a process exit
    /// would.
    pub(crate) async fn stop(self) {
        self.binding.close().await;
    }
}

/// The next event that is not `Retrying`: the transitions a test asserts
/// on are the ones that change the slot's state.
pub(crate) async fn next_transition(events: &mut PeerEvents) -> PeerEvent {
    within(async {
        loop {
            match events.recv().await.expect("event stream open") {
                PeerEvent::Retrying { .. } => continue,
                event => return event,
            }
        }
    })
    .await
}

/// Reads one body off a puller.
pub(crate) async fn pulled(puller: &Puller) -> Vec<u8> {
    let inbound = within(puller.recv()).await.expect("recv");
    within(inbound.collect(64 * 1024)).await.expect("collect")
}

// --- slots, policy and events ---------------------------------------------

/// Claim: a pusher whose server restarts on the same address delivers again
/// with no second `connect`, and the endpoint reports `Lost`, then
/// `Retrying`, then `Connected`.
///
/// The body sent during the outage arrives after it — that is the outbox —
/// and the body that was written before the restart is **not**
/// delivered twice: the puller sees exactly the bodies in order.
#[tokio::test(start_paused = true)]
async fn a_pusher_delivers_again_after_the_server_restarts() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let first = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let addr = first.addr;
    let puller = first.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..client_config()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    within(pusher.connect(&first.url("/jobs")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    within(pusher.send(b"before")).await.expect("send before");
    assert_eq!(pulled(&puller).await, b"before");

    drop(puller);
    first.stop().await;
    let lost = next_transition(&mut events).await;
    assert!(
        matches!(
            lost,
            PeerEvent::Lost {
                cause: LossCause::PeerClosed,
                ..
            }
        ),
        "a server that closed is reported as such, got {lost:?}"
    );
    assert_eq!(
        pusher.peer_count(),
        0,
        "a peer being redialled does not count"
    );

    // Nothing is live, and the send returns at once: the runtime owns it.
    within(pusher.send(b"during"))
        .await
        .expect("send during outage");

    let second = Server::start(Network::sim(&net), &certs, addr).await;
    let puller = second.listener.puller("/jobs").expect("puller");
    let connected = next_transition(&mut events).await;
    assert!(
        matches!(connected, PeerEvent::Connected { .. }),
        "the redial reports the new connection, got {connected:?}"
    );
    assert_eq!(pusher.peer_count(), 1, "the redialled peer counts again");

    assert_eq!(pulled(&puller).await, b"during");
    within(pusher.send(b"after")).await.expect("send after");
    assert_eq!(pulled(&puller).await, b"after");
    assert_eq!(pusher.dropped(), 0, "nothing was discarded");

    client.shutdown().await;
}

/// Claim: `ReconnectPolicy::never()` is the behaviour before 0031 — the loss
/// is reported, nothing redials, and the next send fails with the cause.
#[tokio::test]
async fn the_never_policy_reports_the_loss_and_stops() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let server = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        reconnect: ReconnectPolicy::never(),
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    within(pusher.send(b"before")).await.expect("send");
    assert_eq!(pulled(&puller).await, b"before");

    drop(puller);
    server.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));
    let gave_up = next_transition(&mut events).await;
    assert!(
        matches!(
            gave_up,
            PeerEvent::GaveUp {
                why: GiveUp::Policy { attempts: 0 },
                ..
            }
        ),
        "no attempt is made, got {gave_up:?}"
    );
    let err = within(pusher.send(b"orphan"))
        .await
        .expect_err("a given-up peer fails the send");
    assert!(
        matches!(err, Error::ConnectionLost(LossCause::PeerClosed)),
        "the cause survives, got {err:?}"
    );
    assert_eq!(pusher.peer_count(), 0);

    client.shutdown().await;
}

/// Claim: a server that comes back on the same address with a **different
/// key** is not this peer. The redial pins the key the slot proved first,
/// the handshake refuses the newcomer, and the slot reports
/// `GaveUp { PeerChanged }` rather than going live with a stranger.
#[tokio::test(start_paused = true)]
async fn a_replacement_server_with_a_new_key_is_not_a_reconnect() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let first = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let addr = first.addr;
    let puller = first.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..client_config()
    })
    .expect("client runtime");
    // Both keys are trusted, so nothing but the slot's own pin can tell the
    // newcomer from the peer that answered first.
    let other = Certs::generate();
    let pusher = client.pusher(weida::Trust::pin(certs.fingerprint()).and_pin(other.fingerprint()));
    let mut events = pusher.events();
    within(pusher.connect(&first.url("/jobs")))
        .await
        .expect("connect");
    let PeerEvent::Connected {
        peer: first_peer, ..
    } = next_transition(&mut events).await
    else {
        panic!("expected Connected");
    };
    within(pusher.send(b"before")).await.expect("send");
    assert_eq!(pulled(&puller).await, b"before");

    drop(puller);
    first.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));

    let other = Certs::generate();
    let second = Server::start(Network::sim(&net), &other, addr).await;
    let _puller = second.listener.puller("/jobs").expect("puller");
    let gave_up = next_transition(&mut events).await;
    match gave_up {
        PeerEvent::GaveUp {
            why: GiveUp::PeerChanged { presented },
            ..
        } => {
            assert_eq!(presented, Some(other.fingerprint()));
            assert_ne!(
                first_peer.and_then(|p| p.key()),
                presented,
                "the newcomer's key differs from the first peer's"
            );
        }
        other => panic!("expected PeerChanged, got {other:?}"),
    }
    assert_eq!(pusher.peer_count(), 0, "the stranger never went live");

    client.shutdown().await;
}

// --- open waits through an outage -------------------------------------------

/// Claim: an `open` during the outage does not fail — it waits for the
/// redial and completes on the new connection. `NotConnected` is only what
/// an endpoint that never connected reports.
#[tokio::test(start_paused = true)]
async fn open_waits_for_the_redial_and_a_never_connected_endpoint_does_not() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..client_config()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let err = pusher
        .open(Default::default())
        .await
        .expect_err("never connected");
    assert!(matches!(err, Error::NotConnected), "got {err:?}");

    let first = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let addr = first.addr;
    let puller = first.listener.puller("/jobs").expect("puller");
    let mut events = pusher.events();
    within(pusher.connect(&first.url("/jobs")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    drop(puller);
    first.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));

    let pusher = Arc::new(pusher);
    let opened = {
        let pusher = Arc::clone(&pusher);
        tokio::spawn(async move {
            let mut transfer = pusher.open(Default::default()).await?;
            transfer.write_all(b"waited").await?;
            transfer.finish()?;
            Ok::<(), Error>(())
        })
    };
    tokio::task::yield_now().await;
    assert!(!opened.is_finished(), "the open waits rather than failing");

    let second = Server::start(Network::sim(&net), &certs, addr).await;
    let puller = second.listener.puller("/jobs").expect("puller");
    within(opened).await.expect("join").expect("open completed");
    assert_eq!(pulled(&puller).await, b"waited");

    Arc::try_unwrap(pusher).ok().expect("sole owner");
    client.shutdown().await;
}

/// Claim: `send_timeout` bounds the wait, and the error it produces is the
/// loss that was being waited out, not a new outcome.
#[tokio::test(start_paused = true)]
async fn send_timeout_bounds_the_wait_with_the_loss_cause() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let server = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        send_timeout: Some(Duration::from_millis(50)),
        ..client_config()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    drop(puller);
    server.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));

    let err = within(pusher.open(Default::default()))
        .await
        .expect_err("nobody came back within the timeout");
    assert!(
        matches!(err, Error::ConnectionLost(LossCause::PeerClosed)),
        "the loss being waited out, got {err:?}"
    );

    client.shutdown().await;
}

// --- re-subscribe after redial ----------------------------------------------

/// Claim: a subscriber whose publisher restarts receives the next publish
/// with no application call, and a filter subscribed *during* the outage
/// is present after it.
#[tokio::test(start_paused = true)]
async fn a_subscriber_is_resubscribed_on_the_redialled_connection() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let first = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let addr = first.addr;
    let publisher = first.listener.publisher("/ticks").expect("publisher");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..client_config()
    })
    .expect("client runtime");
    let subscriber = client.subscriber(certs.client_tls());
    let mut events = subscriber.events();
    within(subscriber.subscribe("a"))
        .await
        .expect("subscribe a");
    within(subscriber.connect(&first.url("/ticks")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    // Subscriptions ride their own streams; the first publish may race the
    // SUBSCRIBE, so publish until one lands.
    let got = within(async {
        loop {
            publisher.publish("a", &b"one"[..]).expect("publish");
            if let Ok(Ok(inbound)) = timeout(Duration::from_millis(50), subscriber.recv()).await {
                break inbound.collect(64).await.expect("collect");
            }
        }
    })
    .await;
    assert_eq!(got, b"one");

    drop(publisher);
    first.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));
    within(subscriber.subscribe("b"))
        .await
        .expect("subscribe b during the outage");

    let second = Server::start(Network::sim(&net), &certs, addr).await;
    let publisher = second.listener.publisher("/ticks").expect("publisher");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    // Both filters are on the new connection: the one from before the
    // outage and the one from during it.
    for (topic, body) in [("a", &b"two"[..]), ("b", &b"three"[..])] {
        let got = within(async {
            loop {
                publisher.publish(topic, body).expect("publish");
                if let Ok(Ok(inbound)) = timeout(Duration::from_millis(50), subscriber.recv()).await
                {
                    break inbound.collect(64).await.expect("collect");
                }
            }
        })
        .await;
        assert_eq!(got, body);
    }

    client.shutdown().await;
}

// --- flows across a redial ---------------------------------------------------

pub(crate) fn with_flows(config: RuntimeConfig) -> RuntimeConfig {
    RuntimeConfig {
        limits: Limits {
            datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
            ..Limits::default()
        },
        ..config
    }
}

/// A server with flows on and an acceptor on `/v` registered **before** the
/// port is bound, so a flow re-registered the moment the redial lands finds
/// its path.
pub(crate) async fn flow_server(
    net: &SimNet,
    certs: &Certs,
    addr: SocketAddr,
) -> (Server, Acceptor) {
    let runtime = Runtime::new(with_flows(RuntimeConfig {
        network: Network::sim(net),
        ..RuntimeConfig::default()
    }))
    .expect("runtime");
    let listener = runtime.listener();
    let acceptor = listener.acceptor("/v").expect("acceptor");
    let binding = within(async {
        loop {
            match listener.bind_quic(addr, certs.server_tls()).await {
                Ok(binding) => break binding,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await;
    let addr = binding.local_addr();
    let server = Server {
        _runtime: runtime,
        listener,
        binding,
        addr,
    };
    (server, acceptor)
}

/// Sends on `flow` every 10 ms until `incoming` receives one datagram.
pub(crate) async fn until_one_arrives(flow: &weida::Flow, incoming: &IncomingFlow) {
    within(async {
        loop {
            flow.send(&b"frame"[..]).expect("send");
            if let Ok(Some(_)) =
                tokio::time::timeout(Duration::from_millis(10), incoming.recv()).await
            {
                return;
            }
        }
    })
    .await;
}

pub(crate) async fn accepted_flow(acceptor: &Acceptor) -> IncomingFlow {
    match within(acceptor.accept()).await.expect("accept") {
        Incoming::Flow(flow) => flow,
        other => panic!("expected a flow, got {other:?}"),
    }
}

/// Claim: a flow opened through a dialling peer is registered again on the
/// redialled connection (0034 §4.10). Every send during the outage returns
/// `Ok`, is dropped, and is counted `not_live`; after the redial a new FLOW
/// arrives at the restarted server and carries what is sent next.
#[tokio::test(start_paused = true)]
async fn a_flow_is_reopened_after_the_server_restarts() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let (first, acceptor) =
        flow_server(&net, &certs, "127.0.0.1:0".parse().expect("loopback")).await;
    let addr = first.addr;

    let client = Runtime::new(with_flows(RuntimeConfig {
        network: Network::sim(&net),
        ..client_config()
    }))
    .expect("client runtime");
    let peer = client.peer(certs.client_tls());
    let mut events = peer.events();
    within(peer.connect(&first.url("/v")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    let flow = within(peer.open_flow(FlowMeta::default()))
        .await
        .expect("open flow");
    let incoming = accepted_flow(&acceptor).await;
    until_one_arrives(&flow, &incoming).await;

    drop((incoming, acceptor));
    first.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));
    let before = flow.stats().not_live;
    for _ in 0..20 {
        flow.send(&b"into the gap"[..])
            .expect("a send while nothing is live is a counted drop, not an error");
    }
    assert_eq!(flow.stats().not_live, before + 20);

    let (_second, acceptor) = flow_server(&net, &certs, addr).await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    let incoming = timeout(Duration::from_secs(5), accepted_flow(&acceptor))
        .await
        .expect("the flow was registered again within 5 s");
    until_one_arrives(&flow, &incoming).await;
    client.shutdown().await;
}

// --- the outbox -------------------------------------------------------------

/// Claim: bodies sent during an outage arrive after it, in order, and the
/// runtime discards nothing under the bound.
#[tokio::test(start_paused = true)]
async fn the_outbox_delivers_in_order_after_the_redial() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let first = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let addr = first.addr;
    let puller = first.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..client_config()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    within(pusher.connect(&first.url("/jobs")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    drop(puller);
    first.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));

    for i in 0..100u32 {
        within(pusher.send(format!("m{i}").as_bytes()))
            .await
            .expect("send during outage");
    }

    let second = Server::start(Network::sim(&net), &certs, addr).await;
    let puller = second.listener.puller("/jobs").expect("puller");
    for i in 0..100u32 {
        assert_eq!(pulled(&puller).await, format!("m{i}").as_bytes());
    }
    assert_eq!(pusher.dropped(), 0);

    client.shutdown().await;
}

/// Claim: at the message bound a `Block` sender waits — until the drain
/// begins — and a `Drop` sender discards and counts. A body larger than the
/// byte bound is refused at the call.
#[tokio::test(start_paused = true)]
async fn the_outbox_bound_blocks_or_drops_as_configured() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let first = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let addr = first.addr;
    let puller = first.listener.puller("/jobs").expect("puller");

    let blocking = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        outbox_messages: 3,
        outbox_bytes: 1024,
        ..client_config()
    })
    .expect("client runtime");
    let dropping = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        outbox_messages: 3,
        outbox_bytes: 1024,
        outbox_full: OutboxFull::Drop,
        ..client_config()
    })
    .expect("client runtime");
    let blocked = blocking.pusher(certs.client_tls());
    let dropper = dropping.pusher(certs.client_tls());
    let mut blocked_events = blocked.events();
    let mut dropper_events = dropper.events();
    within(blocked.connect(&first.url("/jobs")))
        .await
        .expect("connect");
    within(dropper.connect(&first.url("/jobs")))
        .await
        .expect("connect");
    for events in [&mut blocked_events, &mut dropper_events] {
        assert!(matches!(
            next_transition(events).await,
            PeerEvent::Connected { .. }
        ));
    }
    drop(puller);
    first.stop().await;
    for events in [&mut blocked_events, &mut dropper_events] {
        assert!(matches!(
            next_transition(events).await,
            PeerEvent::Lost { .. }
        ));
    }

    // Over the byte bound: refused by name, not blocked forever.
    let err = within(blocked.send(&[0u8; 2048]))
        .await
        .expect_err("a body larger than the outbox");
    assert!(matches!(err, Error::LimitExceeded), "got {err:?}");

    for i in 0..3u32 {
        within(blocked.send(format!("b{i}").as_bytes()))
            .await
            .expect("send within the bound");
        within(dropper.send(format!("d{i}").as_bytes()))
            .await
            .expect("send within the bound");
    }
    // The dropper's fourth is discarded and counted; the blocker's waits.
    within(dropper.send(b"d3"))
        .await
        .expect("dropped, not failed");
    assert_eq!(dropper.dropped(), 1);
    let blocked = Arc::new(blocked);
    let fourth = {
        let blocked = Arc::clone(&blocked);
        tokio::spawn(async move { blocked.send(b"b3").await })
    };
    tokio::task::yield_now().await;
    assert!(!fourth.is_finished(), "the fourth send blocks at the bound");

    let second = Server::start(Network::sim(&net), &certs, addr).await;
    let puller = second.listener.puller("/jobs").expect("puller");
    within(fourth)
        .await
        .expect("join")
        .expect("the blocked send completes");
    let mut got: Vec<Vec<u8>> = Vec::new();
    for _ in 0..7 {
        got.push(pulled(&puller).await);
    }
    let blocked_bodies: Vec<&[u8]> = got
        .iter()
        .filter(|b| b.starts_with(b"b"))
        .map(Vec::as_slice)
        .collect();
    let dropped_bodies: Vec<&[u8]> = got
        .iter()
        .filter(|b| b.starts_with(b"d"))
        .map(Vec::as_slice)
        .collect();
    assert_eq!(blocked_bodies, [b"b0", b"b1", b"b2", b"b3"]);
    assert_eq!(dropped_bodies, [b"d0", b"d1", b"d2"], "d3 was discarded");

    Arc::try_unwrap(blocked).ok().expect("sole owner");
    blocking.shutdown().await;
    dropping.shutdown().await;
}

/// Claim: once every address is given up, nothing will drain the outbox, so
/// a `send` fails with the loss as it would on an empty outbox: the sender
/// blocked at the bound is woken with the error rather than left waiting
/// forever, and a later `send` is refused rather than queued for nobody.
#[tokio::test(start_paused = true)]
async fn a_send_fails_once_every_address_is_given_up() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let server = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let puller = server.listener.puller("/jobs").expect("puller");
    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        outbox_messages: 2,
        reconnect: ReconnectPolicy {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(1),
            max_attempts: Some(1),
            ..client_config().reconnect
        },
        ..client_config()
    })
    .expect("client runtime");
    let pusher = Arc::new(client.pusher(certs.client_tls()));
    let mut events = pusher.events();
    within(pusher.connect(&server.url("/jobs")))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    drop(puller);
    server.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));

    for body in [b"q0", b"q1"] {
        within(pusher.send(body))
            .await
            .expect("queued while the redial runs");
    }
    let blocked = {
        let pusher = Arc::clone(&pusher);
        tokio::spawn(async move { pusher.send(b"q2").await })
    };
    tokio::task::yield_now().await;
    assert!(!blocked.is_finished(), "the third send blocks at the bound");

    // The one redial reaches nobody and fails at the handshake timeout, which
    // is longer than `within`'s deadline; the clock is paused, so waiting
    // for it costs nothing.
    let gave_up = timeout(Duration::from_secs(300), async {
        loop {
            match events.recv().await.expect("event stream open") {
                PeerEvent::Retrying { .. } => continue,
                event => return event,
            }
        }
    });
    assert!(matches!(
        gave_up.await.expect("the slot is given up"),
        PeerEvent::GaveUp { .. }
    ));
    let err = within(blocked)
        .await
        .expect("join")
        .expect_err("nothing will ever make room");
    assert!(matches!(err, Error::ConnectionLost(_)), "got {err:?}");
    let err = within(pusher.send(b"q3"))
        .await
        .expect_err("no address is left to drain into");
    assert!(matches!(err, Error::ConnectionLost(_)), "got {err:?}");

    drop(pusher);
    client.shutdown().await;
}

// --- 0031 §4.10: every scheme redials ----------------------------------------

/// Claim: an in-process server that is shut down and bound again under the
/// same bus name is redialled, with the same events as over QUIC, and the
/// wait is on the registry rather than on the clock — the policy here says
/// a minute, and the test does not take one.
#[tokio::test]
async fn an_inproc_bus_that_is_rebound_is_redialled() {
    let bus = format!("weida-reconnect-{}", std::process::id());
    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let binding = listener.bind_inproc(&bus).expect("bind inproc");
    let puller = listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        reconnect: ReconnectPolicy {
            initial: Duration::from_secs(60),
            max: Duration::from_secs(60),
            ..ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(weida::Trust::by_address());
    let mut events = pusher.events();
    let url = format!("weida+inproc://{bus}/jobs");
    within(pusher.connect(&url)).await.expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    within(pusher.send(b"before")).await.expect("send");
    assert_eq!(pulled(&puller).await, b"before");

    // The server process goes away: its runtime closes every connection
    // and the bus name is released with the binding.
    drop(puller);
    drop(binding);
    drop(listener);
    server.shutdown().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));
    within(pusher.send(b"during"))
        .await
        .expect("send during outage");

    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let binding = listener.bind_inproc(&bus).expect("rebind inproc");
    let puller = listener.puller("/jobs").expect("puller");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    assert_eq!(pulled(&puller).await, b"during");
    drop(binding);

    client.shutdown().await;
}

/// Waits until `count()` reads `want`, polling: a bus member reports no
/// events, only its peer count.
async fn peer_count_reaches(count: impl Fn() -> usize, want: usize) {
    within(async {
        while count() != want {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

/// Claim: a bus member whose joined member restarts on the same address
/// reaches it again once the redial is back, as every other dialling
/// pattern does, rather than counting it as a peer it never sends to.
#[tokio::test(start_paused = true)]
async fn a_bus_member_reaches_a_member_that_restarted() {
    let net = SimNet::new();
    let certs = Certs::generate();
    let first = Server::start(
        Network::sim(&net),
        &certs,
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await;
    let addr = first.addr;
    let joined = first
        .listener
        .bus("/bus", certs.client_tls())
        .expect("bus member");

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..client_config()
    })
    .expect("client runtime");
    let member = client
        .listener()
        .bus("/bus", certs.client_tls())
        .expect("bus member");
    within(member.connect(&first.url("/bus")))
        .await
        .expect("join");
    assert_eq!(within(member.send(b"before")).await.expect("send"), 1);
    let inbound = within(joined.recv()).await.expect("recv");
    assert_eq!(
        within(inbound.collect(64)).await.expect("collect"),
        b"before"
    );

    drop(joined);
    first.stop().await;
    peer_count_reaches(|| member.peer_count(), 0).await;

    let second = Server::start(Network::sim(&net), &certs, addr).await;
    let joined = second
        .listener
        .bus("/bus", certs.client_tls())
        .expect("bus member");
    peer_count_reaches(|| member.peer_count(), 1).await;

    assert_eq!(
        within(member.send(b"after")).await.expect("send"),
        1,
        "the redialled member is reached again"
    );
    let inbound = within(joined.recv()).await.expect("recv");
    assert_eq!(
        within(inbound.collect(64)).await.expect("collect"),
        b"after"
    );

    client.shutdown().await;
}

// --- restart handoff --------------------------------------------------------
