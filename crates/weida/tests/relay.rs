//! A dial-only peer reached through a relay (`docs/PROTOCOL.md` §9.6), with real QUIC and
//! TLS on one `SimNet` and a paused clock.
//!
//! R is the relay, H the hidden peer that only dials out (it has no `bind_quic`), D the
//! dialler. H serves its endpoints through its registration at R, D reaches them as
//! `weida+relay://<H>@<R>@host:port/path`, and TLS runs end to end between D and H.

use std::net::SocketAddr;
use std::time::Duration;

use weida::{
    Binding, ClientTls, ClientTrust, DEFAULT_DATAGRAM_RECEIVE_BYTES, Error, Fingerprint, Identity,
    Limits, Listener, LossCause, Network, PeerEvent, PeerEvents, PeerIdentity, RELAY_PATH,
    ReconnectPolicy, Relay, Requester, Runtime, RuntimeConfig, ServerTls, SimNet, TransferMeta,
    Trust,
};

/// The expected time of a wait on the Tokio clock: generous, because under a paused clock it
/// costs no wall time, and a paused clock reaching it means nothing can run any more.
const WITHIN: Duration = Duration::from_secs(600);

async fn within_paused<F: Future>(f: F) -> F::Output {
    weida_testkit::within("a relay step", WITHIN, f).await
}

/// A runtime on `net` whose redial starts at 5 ms and backs off to 600 s; with `datagrams`,
/// its connections carry QUIC datagrams.
fn on(net: &SimNet, datagrams: bool) -> RuntimeConfig {
    let limits = if datagrams {
        Limits {
            datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
            ..Limits::default()
        }
    } else {
        Limits::default()
    };
    RuntimeConfig {
        network: Network::sim(net),
        limits,
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

/// R: a relay on a QUIC binding whose application admits, or refuses, everything.
struct RelayServer {
    runtime: Runtime,
    addr: SocketAddr,
    key: Fingerprint,
}

impl RelayServer {
    async fn start(
        net: &SimNet,
        addr: SocketAddr,
        id: Identity,
        datagrams: bool,
        admit: bool,
    ) -> RelayServer {
        let key = id.fingerprint().expect("fingerprint");
        let runtime = Runtime::new(on(net, datagrams)).expect("runtime");
        let listener = runtime.listener();
        // A registration is addressed by its key, so the relay asks every peer for one.
        let binding = listener
            .bind_quic(addr, ServerTls::new(id).require_client(ClientTrust::AnyKey))
            .await
            .expect("bind");
        let relay = listener.relay().expect("relay");
        tokio::spawn(answer(relay, admit));
        RelayServer {
            runtime,
            addr: binding.local_addr(),
            key,
        }
    }

    /// Where H registers.
    fn url(&self) -> String {
        format!(
            "weida://{}@127.0.0.1:{}{RELAY_PATH}",
            self.key,
            self.addr.port()
        )
    }

    /// Where D reaches `path` on `target`.
    fn relayed(&self, target: Fingerprint, path: &str) -> String {
        format!(
            "weida+relay://{target}@{}@127.0.0.1:{}{path}",
            self.key,
            self.addr.port()
        )
    }
}

async fn answer(relay: Relay, admit: bool) {
    while let Ok(request) = relay.accept().await {
        if admit {
            let _ = request.admit().await;
        } else {
            request.refuse().await;
        }
    }
}

/// H: a runtime with no binding of its own but the one through the relay, which only `d`
/// may use. `/who` answers with the key of whoever asked.
struct Hidden {
    runtime: Runtime,
    listener: Listener,
    binding: Binding,
}

impl Hidden {
    async fn start(
        net: &SimNet,
        relay: &RelayServer,
        id: Identity,
        d: Fingerprint,
        datagrams: bool,
    ) -> Hidden {
        let runtime = Runtime::new(on(net, datagrams)).expect("runtime");
        let listener = runtime.listener();
        let binding = within_paused(listener.bind_relay(
            &relay.url(),
            ClientTls::new(Trust::by_address()).with_identity(id.clone()),
            ServerTls::new(id).require_client(Trust::pin(d)),
        ))
        .await
        .expect("bind through the relay");
        let replier = listener.replier("/who").expect("replier");
        tokio::spawn(async move {
            while let Ok(request) = replier.accept().await {
                let who = format!("{:?}", request.meta().peer);
                let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                    continue;
                };
                if reply.write_all(who.as_bytes()).await.is_ok() {
                    let _ = reply.finish();
                }
            }
        });
        Hidden {
            runtime,
            listener,
            binding,
        }
    }
}

/// D's identity on the client side.
fn dialler_tls(id: Identity) -> ClientTls {
    ClientTls::new(Trust::by_address()).with_identity(id)
}

async fn who(requester: &Requester) -> String {
    let reply = within_paused(requester.request(b"who"))
        .await
        .expect("request");
    let body = within_paused(reply.collect(1024)).await.expect("reply");
    String::from_utf8(body).expect("utf-8")
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

fn any_addr() -> SocketAddr {
    "127.0.0.1:0".parse().expect("addr")
}

/// Claim: a peer with no binding of its own answers an exchange and publishes through the
/// relay, and what it sees is the dialler's own key, not the relay's: TLS is end to end.
#[tokio::test(start_paused = true)]
async fn a_dial_only_peer_answers_and_publishes_through_the_relay_and_sees_the_dialler() {
    let net = SimNet::new();
    let (r_id, _) = identity();
    let (h_id, h_fp) = identity();
    let (d_id, d_fp) = identity();
    let relay = RelayServer::start(&net, any_addr(), r_id, true, true).await;
    let hidden = Hidden::start(&net, &relay, h_id, d_fp, true).await;
    let publisher = hidden.listener.publisher("/events").expect("publisher");

    let dialler = Runtime::new(on(&net, true)).expect("dialler");
    let requester = dialler.requester(dialler_tls(d_id.clone()));
    within_paused(requester.connect(&relay.relayed(h_fp, "/who")))
        .await
        .expect("connect through the relay");
    assert_eq!(
        who(&requester).await,
        format!("{:?}", Some(PeerIdentity::Key(d_fp)))
    );

    let subscriber = dialler.subscriber(dialler_tls(d_id));
    within_paused(subscriber.connect(&relay.relayed(h_fp, "/events")))
        .await
        .expect("connect the subscriber");
    within_paused(subscriber.subscribe("status"))
        .await
        .expect("subscribe");
    weida_testkit::until("the subscription reaches the hidden peer", WITHIN, || {
        publisher.subscriber_count() == 1
    })
    .await;
    publisher.publish("status", &b"up"[..]).expect("publish");
    let transfer = within_paused(subscriber.recv()).await.expect("recv");
    assert_eq!(
        within_paused(transfer.collect(1024))
            .await
            .expect("collect"),
        b"up"
    );

    dialler.shutdown().await;
    hidden.runtime.shutdown().await;
    relay.runtime.shutdown().await;
}

/// Claim: without the datagram capability anywhere, every packet travels as a record on the
/// exchanges, and the relay still carries the connection.
#[tokio::test(start_paused = true)]
async fn the_relay_carries_a_peer_whose_runtimes_have_no_datagrams() {
    let net = SimNet::new();
    let (r_id, _) = identity();
    let (h_id, h_fp) = identity();
    let (d_id, d_fp) = identity();
    let relay = RelayServer::start(&net, any_addr(), r_id, false, true).await;
    let hidden = Hidden::start(&net, &relay, h_id, d_fp, false).await;

    let dialler = Runtime::new(on(&net, false)).expect("dialler");
    let requester = dialler.requester(dialler_tls(d_id));
    within_paused(requester.connect(&relay.relayed(h_fp, "/who")))
        .await
        .expect("connect through the relay");
    assert_eq!(
        who(&requester).await,
        format!("{:?}", Some(PeerIdentity::Key(d_fp)))
    );

    dialler.shutdown().await;
    hidden.runtime.shutdown().await;
    relay.runtime.shutdown().await;
}

/// Claim: a relay that goes away is a loss on both ends, the hidden peer's registration and
/// the dialler's connection through it, and both come back once a relay serves the address
/// again.
#[tokio::test(start_paused = true)]
async fn a_relay_that_goes_away_is_lost_on_both_ends_and_both_redial_through_it() {
    let net = SimNet::new();
    let (r_id, _) = identity();
    let (h_id, h_fp) = identity();
    let (d_id, d_fp) = identity();
    let relay = RelayServer::start(&net, any_addr(), r_id.clone(), true, true).await;
    let hidden = Hidden::start(&net, &relay, h_id, d_fp, true).await;
    let mut registration = hidden.binding.relay_events().expect("a relayed binding");

    let dialler = Runtime::new(on(&net, true)).expect("dialler");
    let requester = dialler.requester(dialler_tls(d_id));
    let mut dialled = requester.events();
    within_paused(requester.connect(&relay.relayed(h_fp, "/who")))
        .await
        .expect("connect through the relay");
    assert!(matches!(
        next_transition(&mut dialled).await,
        PeerEvent::Connected { .. }
    ));
    let me = format!("{:?}", Some(PeerIdentity::Key(d_fp)));
    assert_eq!(who(&requester).await, me);

    let (addr, url) = (relay.addr, relay.url());
    relay.runtime.shutdown().await;
    match next_transition(&mut registration).await {
        PeerEvent::Lost { cause, .. } => assert_eq!(cause, LossCause::PeerClosed),
        other => panic!("expected the registration lost, got {other:?}"),
    }
    match next_transition(&mut dialled).await {
        PeerEvent::Lost { cause, .. } => assert_eq!(cause, LossCause::IdleTimeout),
        other => panic!("expected the dialled connection lost, got {other:?}"),
    }

    let again = RelayServer::start(&net, addr, r_id, true, true).await;
    assert_eq!(again.url(), url, "the same relay, at the same address");
    assert!(matches!(
        next_transition(&mut registration).await,
        PeerEvent::Connected { .. }
    ));
    assert!(matches!(
        next_transition(&mut dialled).await,
        PeerEvent::Connected { .. }
    ));
    assert_eq!(who(&requester).await, me);

    dialler.shutdown().await;
    hidden.runtime.shutdown().await;
    again.runtime.shutdown().await;
}

/// Claim: a registration the relay's application refuses fails `bind_relay`, and nothing is
/// served.
#[tokio::test(start_paused = true)]
async fn a_refused_registration_fails_bind_relay() {
    let net = SimNet::new();
    let (r_id, _) = identity();
    let (h_id, _) = identity();
    let relay = RelayServer::start(&net, any_addr(), r_id, true, false).await;

    let runtime = Runtime::new(on(&net, true)).expect("runtime");
    let bound = within_paused(runtime.listener().bind_relay(
        &relay.url(),
        ClientTls::new(Trust::by_address()).with_identity(h_id.clone()),
        ServerTls::new(h_id),
    ))
    .await;
    assert!(matches!(bound, Err(Error::Rejected)), "{bound:?}");

    runtime.shutdown().await;
    relay.runtime.shutdown().await;
}

/// Claim: a key nobody registered at the relay is an unknown endpoint to the dialler.
#[tokio::test(start_paused = true)]
async fn a_peer_the_relay_does_not_know_is_unknown_to_the_dialler() {
    let net = SimNet::new();
    let (r_id, _) = identity();
    let (_, h_fp) = identity();
    let (d_id, _) = identity();
    let relay = RelayServer::start(&net, any_addr(), r_id, true, true).await;

    let dialler = Runtime::new(on(&net, true)).expect("dialler");
    let requester = dialler.requester(dialler_tls(d_id));
    let dialled = within_paused(requester.connect(&relay.relayed(h_fp, "/who"))).await;
    assert!(
        matches!(dialled, Err(Error::UnknownEndpoint)),
        "{dialled:?}"
    );

    dialler.shutdown().await;
    relay.runtime.shutdown().await;
}

/// Claim: once the dialler's endpoint is gone and the hidden peer's binding closed, every
/// connection involved closes after the pool's linger, and the network falls silent: no
/// relay state keeps a connection alive on its own.
#[tokio::test(start_paused = true)]
async fn a_finished_relay_conversation_leaves_the_network_silent_after_the_linger() {
    let net = SimNet::new();
    let (r_id, _) = identity();
    let (h_id, h_fp) = identity();
    let (d_id, d_fp) = identity();
    let relay = RelayServer::start(&net, any_addr(), r_id, true, true).await;
    let hidden = Hidden::start(&net, &relay, h_id, d_fp, true).await;

    let dialler = Runtime::new(on(&net, true)).expect("dialler");
    let requester = dialler.requester(dialler_tls(d_id));
    within_paused(requester.connect(&relay.relayed(h_fp, "/who")))
        .await
        .expect("connect through the relay");
    who(&requester).await;
    drop(requester);
    hidden.binding.close().await;

    // The binding's close ends the dialler's inner connection and with it the tunnel; the
    // dialler's hop to the relay and the hidden peer's registration each close one linger
    // after their last use.
    tokio::time::sleep(Limits::default().idle_timeout * 4).await;
    let quiet_from = net.stats().delivered;
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(
        net.stats().delivered,
        quiet_from,
        "no datagram crosses the network once every connection closed"
    );

    dialler.shutdown().await;
    hidden.runtime.shutdown().await;
    relay.runtime.shutdown().await;
}
