//! Identity and trust: who a peer is, and on what terms it is accepted.
//!
//! Every test dials a real loopback server. The claims defended here are the
//! ones an operator relies on when a fingerprint is the whole of a peer's
//! identity: a pin accepts exactly one key, an address that names a key
//! overrides everything else, a binding can demand a client identity and
//! learn which one it got, and a connection authenticated on one set of terms
//! is never handed to an endpoint dialling on another.

use crate::common;

use std::future::Future;
use std::time::Duration;

use common::{Certs, Server};
use weida::{
    ClientTls, ClientTrust, Error, Fingerprint, Identity, Network, PeerIdentity, Runtime,
    RuntimeConfig, ServerTls, SimNet, Trust,
};

pub(crate) const DEADLINE: Duration = Duration::from_secs(10);

pub(crate) async fn within<F: Future>(f: F) -> F::Output {
    weida_testkit::within("an identity step", DEADLINE, f).await
}

/// Spawns an echo replier and returns nothing; the handler runs until the
/// listener goes away.
pub(crate) fn spawn_echo(replier: weida::Replier) {
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = request.body().read_capped(1024).await.expect("body");
            let mut reply = request
                .reply(weida::TransferMeta::default())
                .await
                .expect("reply");
            reply.write_all(&body).await.expect("write");
            let _ = reply.finish();
        }
    });
}

pub(crate) async fn roundtrip(requester: &weida::Requester) -> Vec<u8> {
    let reply = within(requester.request(b"ping")).await.expect("request");
    within(reply.collect(64)).await.expect("collect")
}

#[tokio::test]
async fn an_address_that_names_the_peer_needs_no_other_trust() {
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));

    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/echo")))
        .await
        .expect("connect by address");
    assert_eq!(roundtrip(&requester).await, b"ping");

    client.shutdown().await;
}

#[tokio::test]
async fn a_wrong_pin_in_the_address_is_refused_with_what_answered() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/echo").expect("replier");

    let stranger = Certs::generate();
    let url = format!(
        "weida://{}@127.0.0.1:{}/echo",
        stranger.fingerprint(),
        server.addr.port()
    );
    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    let err = within(requester.connect(&url)).await.unwrap_err();
    match err {
        Error::Untrusted(presented) => assert_eq!(presented, server.certs.fingerprint()),
        other => panic!("expected Untrusted, got {other:?}"),
    }
    assert!(err_is_definite(&Error::Untrusted(
        server.certs.fingerprint()
    )));
    assert_eq!(requester.peer_count(), 0, "a refused dial adds no peer");

    client.shutdown().await;
}

pub(crate) fn err_is_definite(e: &Error) -> bool {
    e.is_definite_failure()
}

#[tokio::test]
async fn an_address_pin_overrides_an_anchor_that_would_have_accepted() {
    // The server's own certificate is a valid anchor for it. Naming a
    // different key in the address must still refuse it: the address is the
    // most specific statement of intent and wins.
    let server = Server::start().await;
    let _replier = server.listener.replier("/echo").expect("replier");

    let stranger = Certs::generate();
    let url = format!(
        "weida://{}@127.0.0.1:{}/echo",
        stranger.fingerprint(),
        server.addr.port()
    );
    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    assert!(matches!(
        within(requester.connect(&url)).await.unwrap_err(),
        Error::Untrusted(_)
    ));

    client.shutdown().await;
}

#[tokio::test]
async fn a_pinned_trust_accepts_the_key_and_only_the_key() {
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));
    let stranger = Certs::generate();

    let client = server.client_runtime();

    let pinned = client.requester(Trust::pin(server.certs.fingerprint()));
    within(pinned.connect(&server.url("/echo")))
        .await
        .expect("pinned connect");
    assert_eq!(roundtrip(&pinned).await, b"ping");

    let wrong = client.requester(Trust::pin(stranger.fingerprint()));
    assert!(matches!(
        within(wrong.connect(&server.url("/echo")))
            .await
            .unwrap_err(),
        Error::Untrusted(_)
    ));

    client.shutdown().await;
}

#[tokio::test]
async fn pins_and_anchors_compose() {
    let net = SimNet::new();
    // One endpoint, two servers: one reachable through its anchor, the other
    // through its pin. Neither term alone would cover both.
    let anchored = Server::start_on(&net, RuntimeConfig::default()).await;
    let pinned = Server::start_on(&net, RuntimeConfig::default()).await;
    spawn_echo(anchored.listener.replier("/echo").expect("replier"));
    spawn_echo(pinned.listener.replier("/echo").expect("replier"));

    let client = anchored.client_runtime();
    let requester = client
        .requester(Trust::anchor(anchored.certs.cert.clone()).and_pin(pinned.certs.fingerprint()));
    within(requester.connect(&anchored.url("/echo")))
        .await
        .expect("anchored");
    within(requester.connect(&pinned.url("/echo")))
        .await
        .expect("pinned");
    assert_eq!(requester.peer_count(), 2);
    for _ in 0..2 {
        assert_eq!(roundtrip(&requester).await, b"ping");
    }

    client.shutdown().await;
}

#[tokio::test]
async fn an_anchor_checks_the_name_and_a_pin_does_not() {
    let net = SimNet::new();
    // A certificate trusted as an anchor is verified the way a CA-issued one
    // is: the host dialled must be among its names. This identity names only
    // `localhost`, and the dial goes to `127.0.0.1`, so the anchor path
    // refuses it — while a pin on the same key accepts it, because a pin
    // says nothing about names.
    let identity = Identity::generate_for(["localhost"]).expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let cert_pem = identity.certificate_pem().expect("cert");

    let runtime = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic("127.0.0.1:0".parse().expect("loopback"), identity)
        .await
        .expect("bind");
    spawn_echo(listener.replier("/echo").expect("replier"));
    let url = format!("weida://127.0.0.1:{}/echo", binding.local_addr().port());

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client");
    let anchored = client.requester(Trust::anchor(cert_pem.clone()));
    match within(anchored.connect(&url)).await.unwrap_err() {
        Error::Untrusted(presented) => assert_eq!(presented, fingerprint),
        other => panic!("expected Untrusted, got {other:?}"),
    }

    let pinned = client.requester(Trust::pin(fingerprint));
    within(pinned.connect(&url)).await.expect("pinned connect");
    assert_eq!(roundtrip(&pinned).await, b"ping");

    // Both terms together: the pin carries it, the anchor's name check is
    // never reached.
    let both = client.requester(Trust::anchor(cert_pem).and_pin(fingerprint));
    within(both.connect(&url)).await.expect("connect");

    client.shutdown().await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_binding_that_requires_clients_learns_who_they_are() {
    let net = SimNet::new();
    let identity = Identity::generate().expect("identity");
    let client_identity = Identity::generate().expect("client identity");
    let client_fp = client_identity.fingerprint().expect("fingerprint");

    let runtime = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity.clone()).require_client(Trust::pin(client_fp)),
        )
        .await
        .expect("bind");
    let url = format!(
        "weida://{}@127.0.0.1:{}/who",
        identity.fingerprint().expect("fingerprint"),
        binding.local_addr().port()
    );

    let replier = listener.replier("/who").expect("replier");
    let seen = tokio::spawn(async move {
        let request = replier.accept().await.expect("accept");
        let peer = request.meta().peer.clone();
        let mut reply = request
            .reply(weida::TransferMeta::default())
            .await
            .expect("reply");
        reply.write_all(b"ok").await.expect("write");
        let _ = reply.finish();
        peer
    });

    // Anonymous: the handshake fails, and the failure is a TLS outcome.
    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client");
    let anonymous = client.requester(Trust::by_address());
    let err = within(anonymous.connect(&url)).await.unwrap_err();
    assert!(matches!(err, Error::Tls(_)), "{err:?}");

    // An identity the binding does not trust: refused just the same.
    let stranger = client.requester(
        ClientTls::new(Trust::by_address()).with_identity(Identity::generate().expect("id")),
    );
    let err = within(stranger.connect(&url)).await.unwrap_err();
    assert!(matches!(err, Error::Tls(_)), "{err:?}");

    // The pinned identity: accepted, and the handler sees exactly its key.
    let trusted =
        client.requester(ClientTls::new(Trust::by_address()).with_identity(client_identity));
    within(trusted.connect(&url)).await.expect("connect");
    let reply = within(trusted.request(b"?")).await.expect("request");
    assert_eq!(within(reply.collect(16)).await.expect("collect"), b"ok");
    assert_eq!(
        within(seen).await.expect("handler"),
        Some(PeerIdentity::Key(client_fp))
    );

    // The reply carries the server's identity the same way.
    let reply = within(trusted.open(weida::TransferMeta::default()))
        .await
        .expect("open");
    drop(reply);

    client.shutdown().await;
    runtime.shutdown().await;
}

/// A griasdi-shaped device identity: a device key certified by a user key,
/// presented as the two-certificate chain `[device, user]`. Returns the
/// identity, the device key's fingerprint and the chain's DER, leaf first.
pub(crate) fn device_chain() -> (Identity, Fingerprint, Vec<Vec<u8>>) {
    let user_key = rcgen::KeyPair::generate().expect("user key");
    let user_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("user params");
    let user_cert = user_params.self_signed(&user_key).expect("user cert");
    let device_key = rcgen::KeyPair::generate().expect("device key");
    let device_cert = rcgen::CertificateParams::new(vec!["device".to_owned()])
        .expect("device params")
        .signed_by(&device_key, &rcgen::Issuer::new(user_params, &user_key))
        .expect("device cert");
    let identity = Identity::from_pem(
        device_cert.pem() + &user_cert.pem(),
        device_key.serialize_pem(),
    );
    let fingerprint = identity.fingerprint().expect("device fingerprint");
    let ders = vec![device_cert.der().to_vec(), user_cert.der().to_vec()];
    (identity, fingerprint, ders)
}

/// Claim: a binding that requires any key proves the key and judges nothing
/// about it ([0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
/// §4.1). An anonymous client is refused; two strangers are admitted as two
/// peers; and a leaf signed by a user key, not a CA, parses and is admitted
/// as its own fingerprint (0035 §4.5, the requirement's open question 1).
#[tokio::test]
async fn a_binding_that_requires_any_key_proves_it_and_judges_nothing() {
    let net = SimNet::new();
    let identity = Identity::generate().expect("identity");
    let runtime = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity.clone()).require_client(ClientTrust::AnyKey),
        )
        .await
        .expect("bind");
    let url = format!(
        "weida://{}@127.0.0.1:{}/who",
        identity.fingerprint().expect("fingerprint"),
        binding.local_addr().port()
    );

    let replier = listener.replier("/who").expect("replier");
    let (seen_tx, mut seen) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok(request) = replier.accept().await {
            let meta = request.meta();
            let chain = meta
                .peer_chain
                .as_ref()
                .map(|c| c.certs().map(<[u8]>::to_vec).collect::<Vec<_>>());
            let _ = seen_tx.send((meta.peer.clone(), chain));
            let mut reply = request
                .reply(weida::TransferMeta::default())
                .await
                .expect("reply");
            reply.write_all(b"ok").await.expect("write");
            let _ = reply.finish();
        }
    });

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client");

    // Anonymous: refused in the handshake, as under a trust.
    let anonymous = client.requester(Trust::by_address());
    let err = within(anonymous.connect(&url)).await.unwrap_err();
    assert!(matches!(err, Error::Tls(_)), "{err:?}");

    // Two strangers nobody pinned: both admitted, as two peers.
    let mut strangers = Vec::new();
    for _ in 0..2 {
        let id = Identity::generate().expect("id");
        let fp = id.fingerprint().expect("fingerprint");
        let requester = client.requester(ClientTls::new(Trust::by_address()).with_identity(id));
        within(requester.connect(&url)).await.expect("connect");
        assert_eq!(roundtrip(&requester).await, b"ok");
        let (peer, chain) = within(seen.recv()).await.expect("seen");
        assert_eq!(peer, Some(PeerIdentity::Key(fp)));
        assert_eq!(
            chain.map(|c| c.len()),
            Some(1),
            "a generated identity is one certificate"
        );
        strangers.push(fp);
    }
    assert_ne!(strangers[0], strangers[1]);

    // A device leaf signed by a user key: parsed, proved, and the peer is
    // the device key; the chain behind it arrives as presented, leaf first.
    let (device, device_fp, device_ders) = device_chain();
    let requester = client.requester(ClientTls::new(Trust::by_address()).with_identity(device));
    within(requester.connect(&url))
        .await
        .expect("connect device");
    assert_eq!(roundtrip(&requester).await, b"ok");
    let (peer, chain) = within(seen.recv()).await.expect("seen");
    assert_eq!(peer, Some(PeerIdentity::Key(device_fp)));
    assert_eq!(chain, Some(device_ders));

    client.shutdown().await;
    runtime.shutdown().await;
}

/// The next `Lost` on `events`, skipping the `Connected` that came first.
pub(crate) async fn lost(events: &mut weida::PeerEvents) -> weida::LossCause {
    within(async {
        loop {
            match events.recv().await.expect("event stream open") {
                weida::PeerEvent::Lost { cause, .. } => return cause,
                weida::PeerEvent::Connected { .. } => {}
                other => panic!("unexpected event {other:?}"),
            }
        }
    })
    .await
}

/// Claim: a binding can close every connection of one proved key, and of no
/// other, which is the requirement's open question 2
/// ([0035](../../../docs/decisions/0035-keys-proved-not-judged.md) §4.4).
/// The dialler sees the peer close; the application did not have to track a
/// connection.
#[tokio::test]
async fn disconnect_closes_every_connection_of_one_peer_and_no_other() {
    let net = SimNet::new();
    let identity = Identity::generate().expect("identity");
    let runtime = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity.clone()).require_client(ClientTrust::AnyKey),
        )
        .await
        .expect("bind");
    let url = |path: &str| {
        format!(
            "weida://{}@127.0.0.1:{}{path}",
            identity.fingerprint().expect("fingerprint"),
            binding.local_addr().port()
        )
    };
    spawn_echo(listener.replier("/a").expect("replier a"));
    spawn_echo(listener.replier("/b").expect("replier b"));

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client");
    let a_id = Identity::generate().expect("a");
    let a_fp = a_id.fingerprint().expect("fingerprint a");
    let a_tls = ClientTls::new(Trust::by_address()).with_identity(a_id);
    // One peer, two paths: two connections.
    let a_on_a = client.requester(a_tls.clone());
    let a_on_b = client.requester(a_tls);
    let b_on_a = client.requester(
        ClientTls::new(Trust::by_address()).with_identity(Identity::generate().expect("b")),
    );
    let mut a_events = [a_on_a.events(), a_on_b.events()];
    within(a_on_a.connect(&url("/a"))).await.expect("a on /a");
    within(a_on_b.connect(&url("/b"))).await.expect("a on /b");
    within(b_on_a.connect(&url("/a"))).await.expect("b on /a");
    for requester in [&a_on_a, &a_on_b, &b_on_a] {
        assert_eq!(roundtrip(requester).await, b"ping");
    }

    assert_eq!(binding.disconnect(a_fp), 2);
    for events in &mut a_events {
        assert_eq!(lost(events).await, weida::LossCause::PeerClosed);
    }
    assert_eq!(
        roundtrip(&b_on_a).await,
        b"ping",
        "another key is untouched"
    );

    let unknown = Identity::generate()
        .expect("unknown")
        .fingerprint()
        .expect("fingerprint");
    assert_eq!(binding.disconnect(unknown), 0);

    client.shutdown().await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn an_anonymous_client_is_seen_as_nobody() {
    let server = Server::start().await;
    let replier = server.listener.replier("/who").expect("replier");
    let seen = tokio::spawn(async move {
        let request = replier.accept().await.expect("accept");
        let meta = request.meta();
        (meta.peer.clone(), meta.peer_chain.clone())
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    within(requester.connect(&server.url("/who")))
        .await
        .expect("connect");
    let (mut transfer, reply) = within(requester.open(weida::TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"?")).await.expect("write");
    let _ = transfer.finish();
    assert_eq!(within(seen).await.expect("handler"), (None, None));
    drop(reply);

    client.shutdown().await;
}

#[tokio::test]
async fn the_reply_names_the_server_the_requester_dialled() {
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));

    let client = server.client_runtime();
    let requester = client.requester(Trust::by_address());
    within(requester.connect(&server.pinned_url("/echo")))
        .await
        .expect("connect");
    let reply = within(requester.request(b"x")).await.expect("request");
    assert_eq!(
        reply.meta().peer,
        Some(PeerIdentity::Key(server.certs.fingerprint()))
    );
    // The dialling side keeps the chain the server presented, too.
    assert_eq!(
        reply.meta().peer_chain.as_ref().map(|c| c.certs().len()),
        Some(1)
    );

    client.shutdown().await;
}

#[tokio::test]
async fn connections_are_not_shared_across_different_terms() {
    // Same runtime, same authority. The first endpoint pins the right key
    // and connects. The second names a different key in its address; if the
    // pool handed it the first connection it would succeed without ever
    // checking. It must not.
    let server = Server::start().await;
    spawn_echo(server.listener.replier("/echo").expect("replier"));
    let stranger = Certs::generate();

    let client = server.client_runtime();
    let right = client.requester(Trust::by_address());
    within(right.connect(&server.pinned_url("/echo")))
        .await
        .expect("connect");
    assert_eq!(roundtrip(&right).await, b"ping");

    let wrong = client.requester(Trust::by_address());
    let url = format!(
        "weida://{}@127.0.0.1:{}/echo",
        stranger.fingerprint(),
        server.addr.port()
    );
    assert!(matches!(
        within(wrong.connect(&url)).await.unwrap_err(),
        Error::Untrusted(_)
    ));
    // And the first endpoint is unaffected by its neighbour's refusal.
    assert_eq!(roundtrip(&right).await, b"ping");

    client.shutdown().await;
}

/// Claim: `max_connections_per_peer` bounds what one peer can hold, counted by
/// the identity it proved, and one connection per dialled path is exactly why
/// it is needed — the dialling side chooses the path count
/// ([0002](../../../docs/decisions/0002-control-and-bulk-separation.md) §7).
///
/// The binding requires a client identity, because a bound it cannot attribute
/// is a bound it cannot enforce: two anonymous connections may not be treated
/// as one peer.
#[tokio::test]
async fn a_peer_cannot_hold_more_connections_than_its_ceiling() {
    let net = SimNet::new();
    const CEILING: usize = 3;

    let identity = Identity::generate().expect("identity");
    let fingerprint = identity.fingerprint().expect("fingerprint");
    let client_identity = Identity::generate().expect("client identity");
    let client_fp = client_identity.fingerprint().expect("client fingerprint");

    let runtime = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        max_connections_per_peer: CEILING,
        ..RuntimeConfig::default()
    })
    .expect("runtime");
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0".parse().expect("loopback"),
            ServerTls::new(identity).require_client(Trust::pin(client_fp)),
        )
        .await
        .expect("bind");
    let base = format!(
        "weida://{}@127.0.0.1:{}",
        fingerprint,
        binding.local_addr().port()
    );
    // Held for the duration: dropping a puller unregisters its path.
    let _pullers: Vec<_> = (0..=CEILING)
        .map(|i| listener.puller(&format!("/p{i}")).expect("puller"))
        .collect();

    let client = Runtime::new(RuntimeConfig {
        network: Network::sim(&net),
        ..RuntimeConfig::default()
    })
    .expect("client");
    let pusher =
        client.pusher(ClientTls::new(Trust::pin(fingerprint)).with_identity(client_identity));

    // Up to the ceiling: one connection per path, all accepted.
    for i in 0..CEILING {
        within(pusher.connect(&format!("{base}/p{i}")))
            .await
            .unwrap_or_else(|e| panic!("path {i} must connect: {e:?}"));
    }
    assert_eq!(pusher.peer_count(), CEILING);

    // One more path is one more connection, and the binding refuses it by
    // saying which limit was hit.
    let err = within(pusher.connect(&format!("{base}/p{CEILING}")))
        .await
        .expect_err("the ceiling must bite");
    assert!(matches!(err, Error::LimitExceeded), "{err:?}");

    client.shutdown().await;
    runtime.shutdown().await;
}
