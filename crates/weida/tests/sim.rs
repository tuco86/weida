//! Real QUIC and TLS over a simulated network
//! ([0042](../../../docs/decisions/0042-simulated-network.md)).
//!
//! Every runtime here is on one `SimNet`, so nothing opens a socket: identities, refusals,
//! redials and their events are the production code's, and the faults are the network's. The
//! tests marked `start_paused` run on a paused Tokio clock; each ends by asserting that the
//! minutes of QUIC and redial time it spent took seconds of wall time at most.

use std::net::SocketAddr;
use std::time::Duration;

use weida::{
    Binding, ClientTls, EndpointAddr, Error, Fingerprint, GiveUp, Identity, Limits, Listener,
    LossCause, Network, PeerEvent, PeerEvents, PeerIdentity, Puller, ReconnectPolicy, Runtime,
    RuntimeConfig, ServerTls, SimNet, TransferMeta, Trust,
};

/// A wait on the Tokio clock: generous, because under a paused clock it costs no wall time.
const WITHIN: Duration = Duration::from_secs(600);
/// The most wall time a paused test may take.
const REAL: Duration = Duration::from_secs(10);

async fn within_paused<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(WITHIN, f)
        .await
        .expect("operation timed out")
}

/// A runtime on `net` whose redial starts at 5 ms and backs off to 600 s.
fn on(net: &SimNet) -> RuntimeConfig {
    RuntimeConfig {
        network: Network::sim(net),
        reconnect: ReconnectPolicy {
            initial: Duration::from_millis(5),
            max: Duration::from_secs(600),
            ..ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    }
}

fn identity() -> (Identity, Fingerprint) {
    let identity = Identity::generate_for(["127.0.0.1"]).expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    (identity, fingerprint)
}

/// A QUIC server on the simulated network, stopped and started again on the same address.
struct SimServer {
    runtime: Runtime,
    listener: Listener,
    binding: Binding,
    addr: SocketAddr,
}

impl SimServer {
    async fn start(net: &SimNet, addr: SocketAddr, tls: impl Into<ServerTls>) -> SimServer {
        let runtime = Runtime::new(on(net)).expect("runtime");
        let listener = runtime.listener();
        let binding = listener.bind_quic(addr, tls).await.expect("bind");
        let addr = binding.local_addr();
        SimServer {
            runtime,
            listener,
            binding,
            addr,
        }
    }

    fn url(&self, path: &str, peer: Option<Fingerprint>) -> String {
        EndpointAddr {
            host: "127.0.0.1".into(),
            port: Some(self.addr.port()),
            path: path.into(),
            peer,
        }
        .to_string()
    }

    async fn stop(self) {
        self.binding.close().await;
    }
}

/// The next event that is not `Retrying`.
async fn next_transition(events: &mut PeerEvents) -> PeerEvent {
    within_paused(async {
        loop {
            match events.recv().await.expect("event stream open") {
                PeerEvent::Retrying { .. } => continue,
                event => return event,
            }
        }
    })
    .await
}

async fn pulled(puller: &Puller) -> Vec<u8> {
    let inbound = within_paused(puller.recv()).await.expect("recv");
    within_paused(inbound.collect(64 * 1024))
        .await
        .expect("collect")
}

/// Answers every request on `path` with its own body.
fn echo(server: &SimServer, path: &str) {
    let replier = server.listener.replier(path).expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = request.take_body().collect(1 << 20).await.expect("body");
            let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
            reply.write_all(&body).await.expect("write");
            reply.finish().expect("finish");
        }
    });
}

async fn request(requester: &weida::Requester, body: &[u8]) -> Vec<u8> {
    let reply = within_paused(requester.request(body))
        .await
        .expect("request");
    within_paused(reply.collect(1 << 20)).await.expect("reply")
}

/// Claim: a client's key is proved over the simulated network exactly as over UDP: the server
/// sees `PeerIdentity::Key` of the client, and the client reached the key its URL pins.
#[tokio::test]
async fn a_pinned_client_proves_its_key_over_the_simulated_network() {
    let net = SimNet::new();
    let (server_id, server_fp) = identity();
    let (client_id, client_fp) = identity();
    let server = SimServer::start(
        &net,
        "127.0.0.1:0".parse().expect("addr"),
        ServerTls::new(server_id).require_client(Trust::pin(client_fp)),
    )
    .await;
    let replier = server.listener.replier("/who").expect("replier");
    tokio::spawn(async move {
        while let Ok(request) = replier.accept().await {
            let answer: &[u8] = if request.meta().peer == Some(PeerIdentity::Key(client_fp)) {
                b"ok"
            } else {
                b"anonymous"
            };
            let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
            reply.write_all(answer).await.expect("write");
            reply.finish().expect("finish");
        }
    });

    let client = Runtime::new(on(&net)).expect("client");
    let requester = client.requester(ClientTls::new(Trust::by_address()).with_identity(client_id));
    within_paused(requester.connect(&server.url("/who", Some(server_fp))))
        .await
        .expect("connect");
    assert_eq!(request(&requester, b"who").await, b"ok");
    assert!(
        net.stats().delivered > 0,
        "the datagrams crossed the simulation"
    );

    client.shutdown().await;
    server.runtime.shutdown().await;
}

/// Claim: mTLS refuses a client without a key, and a pin refuses a server with another key,
/// naming the key that answered; the correctly pinned client still connects afterwards.
#[tokio::test]
async fn mtls_refuses_an_anonymous_client_and_a_pin_refuses_another_key() {
    let net = SimNet::new();
    let (server_id, server_fp) = identity();
    let (client_id, client_fp) = identity();
    let (_, other_fp) = identity();
    let server = SimServer::start(
        &net,
        "127.0.0.1:0".parse().expect("addr"),
        ServerTls::new(server_id).require_client(Trust::pin(client_fp)),
    )
    .await;
    echo(&server, "/echo");
    let client = Runtime::new(on(&net)).expect("client");

    let anonymous = client.requester(ClientTls::new(Trust::by_address()));
    let refused = within_paused(anonymous.connect(&server.url("/echo", Some(server_fp)))).await;
    assert!(
        refused.is_err(),
        "an anonymous client is refused: {refused:?}"
    );

    let mispinned =
        client.requester(ClientTls::new(Trust::by_address()).with_identity(client_id.clone()));
    match within_paused(mispinned.connect(&server.url("/echo", Some(other_fp)))).await {
        Err(Error::Untrusted(presented)) => assert_eq!(presented, server_fp),
        other => panic!("expected Untrusted, got {other:?}"),
    }

    let pinned = client.requester(ClientTls::new(Trust::by_address()).with_identity(client_id));
    within_paused(pinned.connect(&server.url("/echo", Some(server_fp))))
        .await
        .expect("the right pin connects");
    assert_eq!(request(&pinned, b"hi").await, b"hi");

    client.shutdown().await;
    server.runtime.shutdown().await;
}

/// Claim: a server closed and bound again on the same address drives the subscriber's events
/// as a restart does over UDP: `Lost { PeerClosed }`, `Connected` for the same key, and
/// `GaveUp { PeerChanged }` for a server with another key.
#[tokio::test(start_paused = true)]
async fn a_server_closed_and_bound_again_drives_the_subscriber_events() {
    let real = std::time::Instant::now();
    let net = SimNet::new();
    let (a_id, a_fp) = identity();
    let (b_id, b_fp) = identity();
    let first = SimServer::start(&net, "127.0.0.1:0".parse().expect("addr"), a_id.clone()).await;
    let addr = first.addr;
    let _publisher = first.listener.publisher("/events").expect("publisher");

    let client = Runtime::new(on(&net)).expect("client");
    let subscriber = client.subscriber(Trust::pin(a_fp).and_pin(b_fp));
    let mut events = subscriber.events();
    within_paused(subscriber.connect(&first.url("/events", None)))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));

    first.stop().await;
    match next_transition(&mut events).await {
        PeerEvent::Lost { cause, .. } => assert_eq!(cause, LossCause::PeerClosed),
        other => panic!("expected Lost, got {other:?}"),
    }

    let again = SimServer::start(&net, addr, a_id).await;
    let _publisher = again.listener.publisher("/events").expect("publisher");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));

    again.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));
    let replaced = SimServer::start(&net, addr, b_id).await;
    let _publisher = replaced.listener.publisher("/events").expect("publisher");
    match next_transition(&mut events).await {
        PeerEvent::GaveUp {
            why: GiveUp::PeerChanged { presented },
            ..
        } => assert_eq!(presented, Some(b_fp)),
        other => panic!("expected GaveUp PeerChanged, got {other:?}"),
    }

    client.shutdown().await;
    replaced.runtime.shutdown().await;
    assert!(
        real.elapsed() < REAL,
        "took {:?} of wall time",
        real.elapsed()
    );
}

/// Claim: a partition is an unannounced disconnect: the dialler learns of it at the idle
/// timeout, as `LossCause::IdleTimeout`, and once the path heals it redials the same binding
/// and delivers again.
#[tokio::test(start_paused = true)]
async fn a_partition_is_lost_at_the_idle_timeout_and_heal_redials() {
    let real = std::time::Instant::now();
    let net = SimNet::new();
    let (server_id, server_fp) = identity();
    let server = SimServer::start(&net, "127.0.0.1:0".parse().expect("addr"), server_id).await;
    let puller = server.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(on(&net)).expect("client");
    let pusher = client.pusher(Trust::pin(server_fp));
    let mut events = pusher.events();
    within_paused(pusher.connect(&server.url("/jobs", None)))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));

    let cut = tokio::time::Instant::now();
    net.partition(server.addr);
    match next_transition(&mut events).await {
        PeerEvent::Lost { cause, .. } => assert_eq!(cause, LossCause::IdleTimeout),
        other => panic!("expected Lost, got {other:?}"),
    }
    assert!(
        cut.elapsed() >= Limits::default().idle_timeout,
        "{:?}",
        cut.elapsed()
    );

    net.heal(server.addr);
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    within_paused(pusher.send(b"after")).await.expect("send");
    assert_eq!(pulled(&puller).await, b"after");

    client.shutdown().await;
    server.runtime.shutdown().await;
    assert!(
        real.elapsed() < REAL,
        "took {:?} of wall time",
        real.elapsed()
    );
}

/// Claim: a one-way delay costs at least a round trip per request and loses nothing.
#[tokio::test(start_paused = true)]
async fn a_delayed_path_costs_a_round_trip_and_loses_nothing() {
    let real = std::time::Instant::now();
    let net = SimNet::new();
    let (server_id, server_fp) = identity();
    let server = SimServer::start(&net, "127.0.0.1:0".parse().expect("addr"), server_id).await;
    echo(&server, "/echo");
    let client = Runtime::new(on(&net)).expect("client");
    let requester = client.requester(Trust::pin(server_fp));
    within_paused(requester.connect(&server.url("/echo", None)))
        .await
        .expect("connect");

    net.set_delay(server.addr, Duration::from_millis(50));
    let body: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let start = tokio::time::Instant::now();
    assert_eq!(request(&requester, &body).await, body);
    assert!(
        start.elapsed() >= Duration::from_millis(100),
        "{:?}",
        start.elapsed()
    );

    client.shutdown().await;
    server.runtime.shutdown().await;
    assert!(
        real.elapsed() < REAL,
        "took {:?} of wall time",
        real.elapsed()
    );
}

/// Claim: datagrams the network drops are QUIC's to retransmit: the exchange completes and the
/// drops are counted.
#[tokio::test(start_paused = true)]
async fn dropped_datagrams_are_retransmitted() {
    let real = std::time::Instant::now();
    let net = SimNet::new();
    let (server_id, server_fp) = identity();
    let server = SimServer::start(&net, "127.0.0.1:0".parse().expect("addr"), server_id).await;
    echo(&server, "/echo");
    let client = Runtime::new(on(&net)).expect("client");
    let requester = client.requester(Trust::pin(server_fp));
    within_paused(requester.connect(&server.url("/echo", None)))
        .await
        .expect("connect");

    let before = net.stats().dropped;
    net.drop_next(server.addr, 3);
    assert_eq!(request(&requester, b"through").await, b"through");
    assert!(net.stats().dropped >= before + 3, "{:?}", net.stats());

    client.shutdown().await;
    server.runtime.shutdown().await;
    assert!(
        real.elapsed() < REAL,
        "took {:?} of wall time",
        real.elapsed()
    );
}

/// Claim: a connection that stayed up for the policy's `max` restarts the redial schedule, on
/// the runtime's clock: after 1200 s of paused time the next loss is `Retrying { attempt: 1 }`,
/// not the attempt after the last outage's.
#[tokio::test(start_paused = true)]
async fn the_redial_schedule_restarts_after_a_connection_that_stayed_up() {
    let real = std::time::Instant::now();
    let net = SimNet::new();
    let (server_id, server_fp) = identity();
    let first = SimServer::start(
        &net,
        "127.0.0.1:0".parse().expect("addr"),
        server_id.clone(),
    )
    .await;
    let addr = first.addr;
    let _puller = first.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(on(&net)).expect("client");
    let pusher = client.pusher(Trust::pin(server_fp));
    let mut events = pusher.events();
    within_paused(pusher.connect(&first.url("/jobs", None)))
        .await
        .expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));

    first.stop().await;
    within_paused(async {
        loop {
            if let Some(PeerEvent::Retrying { attempt: 3, .. }) = events.recv().await {
                break;
            }
        }
    })
    .await;

    let second = SimServer::start(&net, addr, server_id).await;
    let _puller = second.listener.puller("/jobs").expect("puller");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));

    tokio::time::sleep(Duration::from_secs(1200)).await;
    second.stop().await;
    let attempt = within_paused(async {
        loop {
            match events.recv().await.expect("event stream open") {
                PeerEvent::Retrying { attempt, .. } => break attempt,
                PeerEvent::Lost { .. } => continue,
                other => panic!("expected Lost then Retrying, got {other:?}"),
            }
        }
    })
    .await;
    assert_eq!(
        attempt, 1,
        "a connection up for 1200 s earned a fresh schedule"
    );

    client.shutdown().await;
    assert!(
        real.elapsed() < REAL,
        "took {:?} of wall time",
        real.elapsed()
    );
}

/// Claim: a simulated address is taken while its binding serves and free again once the
/// binding closes, or once its runtime shuts down.
#[tokio::test]
async fn a_bound_address_is_free_again_once_its_binding_closes() {
    let net = SimNet::new();
    let (id, _) = identity();
    let first = SimServer::start(&net, "127.0.0.1:0".parse().expect("addr"), id.clone()).await;
    let addr = first.addr;

    let second = Runtime::new(on(&net)).expect("second");
    let second_listener = second.listener();
    match second_listener.bind_quic(addr, id.clone()).await {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::AddrInUse),
        other => panic!("expected AddrInUse, got {other:?}"),
    }
    first.stop().await;
    let _held = second_listener
        .bind_quic(addr, id.clone())
        .await
        .expect("free once the binding closed");

    let third = Runtime::new(on(&net)).expect("third");
    let third_listener = third.listener();
    assert!(
        third_listener.bind_quic(addr, id.clone()).await.is_err(),
        "taken by the second runtime"
    );
    second.shutdown().await;
    let _bound = third_listener
        .bind_quic(addr, id)
        .await
        .expect("free once the runtime shut down");
    third.shutdown().await;
}
