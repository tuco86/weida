//! The tests of `tests/tls.rs` that need the network, a process or real time.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use weida_nng::{
    Admission, AuthMode, Context, ContextConfig, Error, PipeEvent, PipeInfo, RepSocket, ReqSocket,
    SocketOptions, TlsConfig,
};

use crate::tls::*;

/// Claim: a REQ/REP round trip runs over `tls+tcp://`, the peer's
/// certificate is validated against the configured CA, and the
/// verification result, the common name and the alternative names all
/// reach the pipe-add-pre hook (§10).
#[tokio::test]
async fn a_round_trip_over_tls_reports_the_peer_to_the_hook() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    // The listener requires a client certificate, which is what puts a
    // peer on the client side into the hook's hands.
    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(client.ca_pem.clone()),
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");

    let seen: Arc<std::sync::Mutex<Option<weida_nng::TlsPeer>>> =
        Arc::new(std::sync::Mutex::new(None));
    let recorded = Arc::clone(&seen);
    rep.notify(Arc::new(move |event, info: &PipeInfo| {
        if event == PipeEvent::AddPre {
            *recorded.lock().unwrap() = info.tls.clone();
        }
        Admission::Accept
    }));
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    // The listening URL keeps its scheme, so `NNG_OPT_URL` is dialable.
    assert!(url.starts_with("tls+tcp://"), "{url}");

    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(server.ca_pem.clone()),
            cert_pem: Some(client.cert_pem.clone()),
            key_pem: Some(client.key_pem.clone()),
            // The URL names an address, which carries no name a
            // certificate can be checked against, so the expected name is
            // said explicitly — which is what NNG_OPT_TLS_SERVER_NAME is
            // for.
            server_name: Some("server.test".to_owned()),
        }),
    )
    .expect("req");
    req.dial(&url).await.expect("dial");

    req.send(b"ping".to_vec()).await.expect("send");
    let request = rep.recv().await.expect("recv");
    assert_eq!(request.body(), b"ping");
    rep.send(b"pong".to_vec()).await.expect("reply");
    assert_eq!(req.recv().await.expect("reply").body(), b"pong");

    let peer = seen.lock().unwrap().clone().expect("the hook saw the peer");
    assert!(peer.verified, "the client certificate validated");
    assert_eq!(peer.common_name.as_deref(), Some("client.test"));
    assert_eq!(peer.subject_alt_names, vec!["client.test".to_owned()]);

    // The dialling side knows the same kind of thing about its server.
    let server_peer = req.pipe_infos()[0]
        .tls
        .clone()
        .expect("the dialler has a TLS peer too");
    assert!(server_peer.verified);
    assert_eq!(
        server_peer.subject_alt_names,
        vec!["server.test".to_owned()]
    );
}
/// Claim: an allow-list at the hook refuses a peer on what TLS
/// established, and the refusal is a close with nothing sent back — which
/// is all SP has (§6, §10).
#[tokio::test]
async fn an_allow_list_on_the_alt_names_refuses_a_peer() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(client.ca_pem.clone()),
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");
    let refusals = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&refusals);
    rep.notify(Arc::new(move |event, info: &PipeInfo| {
        if event != PipeEvent::AddPre {
            return Admission::Accept;
        }
        let allowed = info.tls.as_ref().is_some_and(|peer| {
            peer.verified
                && peer
                    .subject_alt_names
                    .iter()
                    .any(|name| name == "allowed.test")
        });
        if allowed {
            Admission::Accept
        } else {
            counter.fetch_add(1, Ordering::SeqCst);
            Admission::Reject("not on the allow-list".into())
        }
    }));
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(server.ca_pem.clone()),
            cert_pem: Some(client.cert_pem.clone()),
            key_pem: Some(client.key_pem.clone()),
            server_name: Some("server.test".to_owned()),
        }),
    )
    .expect("req");
    // The dial may or may not return before the refusal reaches it: the
    // refusal is a close, so what the dialler observes is a pipe that
    // existed and then did not. Either way the listener admitted nothing.
    // The close is deliberately abrupt — the refusing side sends no TLS
    // `close_notify` — and rustls reports that differently per platform:
    // as a reset or abort on Linux, as an unexpected EOF (`ESYSERR`) on
    // Windows. All of them are the same refusal seen from the other end.
    let dialled = req.dial(&url).await;
    weida_testkit::until(
        "the refusal being counted and the dialler's pipe gone",
        Duration::from_secs(2),
        || refusals.load(Ordering::SeqCst) >= 1 && req.pipe_count() == 0,
    )
    .await;
    if let Err(error) = dialled {
        assert!(
            matches!(
                error,
                Error::ECONNRESET(_)
                    | Error::ECONNABORTED(_)
                    | Error::ETIMEDOUT(_)
                    | Error::ESYSERR(_)
            ),
            "{error:?}"
        );
    }
    assert_eq!(refusals.load(Ordering::SeqCst), 1);
    assert_eq!(rep.pipe_count(), 0, "the listener admitted nothing");
    assert_eq!(req.pipe_count(), 0, "and the dialler's pipe is gone");
}
/// Claim: a peer whose certificate does not validate against the
/// configured CA is refused as `NNG_EPEERAUTH`, which is the code NNG's
/// own dial reports for it (§8).
#[tokio::test]
async fn an_untrusted_certificate_is_peer_auth() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let stranger = pki("stranger.test");

    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::None,
            ca_pem: None,
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    // The client trusts a CA that did not sign the server's certificate.
    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(stranger.ca_pem.clone()),
            cert_pem: None,
            key_pem: None,
            server_name: Some("server.test".to_owned()),
        }),
    )
    .expect("req");
    let err = req.dial(&url).await.unwrap_err();
    assert!(matches!(err, Error::EPEERAUTH(_)), "{err:?}");
}
/// Claim: `NNG_TLS_AUTH_MODE_NONE` encrypts and establishes nothing, and
/// says so — `verified` is false, which is the field an allow-list has to
/// read.
#[tokio::test]
async fn auth_mode_none_encrypts_and_proves_nothing() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");

    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::None,
            ca_pem: None,
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::None,
            ca_pem: None,
            cert_pem: None,
            key_pem: None,
            // Any name will do: nothing is checked against it.
            server_name: Some("whatever.invalid".to_owned()),
        }),
    )
    .expect("req");
    req.dial(&url).await.expect("an unverified dial succeeds");

    req.send(b"ping".to_vec()).await.expect("send");
    assert_eq!(rep.recv().await.expect("recv").body(), b"ping");

    let peer = req.pipe_infos()[0].tls.clone().expect("a TLS peer");
    assert!(
        !peer.verified,
        "nothing was validated, and the field says so rather than implying otherwise"
    );
    // The server side of an unauthenticated client has no certificate at
    // all, so there is nothing to report and nothing to allow-list on.
    let client_side = rep.pipe_infos()[0].tls.clone().expect("a TLS peer");
    assert!(!client_side.verified);
    assert_eq!(client_side.common_name, None);
}
/// Claim: a connection that opens over TCP and then sends nothing does not
/// stop the listener from accepting the next one. A silent connection's
/// handshake is bounded by `handshake_timeout` and runs beside the accept
/// loop; if it ran in front of the accept loop, one idle socket would hold
/// back every TLS client behind it.
#[tokio::test]
async fn a_silent_tcp_connection_does_not_stop_the_listener_from_accepting() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    let rep = RepSocket::with_options(
        &ctx,
        SocketOptions {
            handshake_timeout: Duration::from_millis(500),
            ..options(listener_tls(&server, &client))
        },
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    // Connected and silent: the listener is waiting for a ClientHello that
    // never comes. The stream stays alive for the whole test.
    let _silent = TcpStream::connect(tcp_address(&url))
        .await
        .expect("connect");

    let req = ReqSocket::with_options(&ctx, options(dialer_tls(&server, &client))).expect("req");
    weida_testkit::within(
        "the TLS client being admitted",
        Duration::from_secs(3),
        req.dial(&url),
    )
    .await
    .expect("dial");
    exchange_once(&req, &rep).await;
}
/// Claim: a connection that never completes its handshake is closed when
/// `handshake_timeout` runs out, and that close frees its pipe slot. With
/// `max_pipes = 1` the silent connection holds the only slot, so a listener
/// that never reclaimed it would refuse every later client. The close must
/// also come late enough to show the slot is held while the peer is silent,
/// rather than the connection being refused on arrival.
#[tokio::test]
async fn a_silent_connection_is_closed_at_the_handshake_deadline_and_frees_its_slot() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    let rep = RepSocket::with_options(
        &ctx,
        SocketOptions {
            max_pipes: 1,
            handshake_timeout: Duration::from_millis(500),
            ..options(listener_tls(&server, &client))
        },
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let mut silent = TcpStream::connect(tcp_address(&url))
        .await
        .expect("connect");
    let waited = closed_by_listener(&mut silent, Duration::from_secs(2)).await;
    assert!(
        waited >= Duration::from_millis(300),
        "the silent connection was closed before its handshake deadline: {waited:?}"
    );

    let req = ReqSocket::with_options(&ctx, options(dialer_tls(&server, &client))).expect("req");
    dial_until_admitted(&req, &url, Duration::from_secs(3)).await;
    exchange_once(&req, &rep).await;
}
/// Claim: a dial to a peer that accepts the TCP connection and never answers
/// the TLS ClientHello gives up at `handshake_timeout` with `ETIMEDOUT`.
/// Without that deadline a dialler would wait on such a peer indefinitely.
#[tokio::test]
async fn a_dial_to_a_peer_that_never_answers_the_tls_hello_times_out() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    let mute = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = mute.local_addr().expect("address");
    // Accepts the connection, holds it open, and never writes to it.
    let holder = tokio::spawn(async move {
        let (_held, _) = mute.accept().await.expect("accept");
        std::future::pending::<()>().await;
    });

    let req = ReqSocket::with_options(
        &ctx,
        SocketOptions {
            handshake_timeout: Duration::from_millis(500),
            ..options(dialer_tls(&server, &client))
        },
    )
    .expect("req");
    let outcome = weida_testkit::within(
        "the dial giving up",
        Duration::from_secs(3),
        req.dial(&format!("tls+tcp://{address}")),
    )
    .await;
    holder.abort();
    match outcome {
        Err(error) => assert!(matches!(error, Error::ETIMEDOUT(_)), "{error:?}"),
        Ok(_) => panic!("a dial to a peer that never answered was admitted"),
    }
}
/// Claim: a listener runs at most `max_tls_handshakes` handshakes at once. A
/// connection over that cap is closed without any handshake work, so a crowd
/// of silent connections cannot hold every handshake slot for the whole
/// handshake deadline, and the slot returns when the connection holding it
/// goes away.
#[tokio::test]
async fn tls_handshakes_in_flight_are_capped() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    let rep = RepSocket::with_options(
        &ctx,
        SocketOptions {
            max_tls_handshakes: 1,
            handshake_timeout: Duration::from_secs(3600),
            ..options(listener_tls(&server, &client))
        },
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let address = tcp_address(&url).to_owned();

    // The listener accepts in arrival order, so this connection takes the
    // only handshake slot and the next one arrives behind it.
    let mut holder = TcpStream::connect(&address).await.expect("connect");
    let mut over_cap = TcpStream::connect(&address).await.expect("connect");
    closed_by_listener(&mut over_cap, Duration::from_secs(1)).await;

    // The holder is still open: a short read on it times out rather than
    // reporting end of stream.
    let mut byte = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(300), holder.read(&mut byte))
            .await
            .is_err(),
        "the connection holding the handshake slot was closed"
    );

    // Once the holder goes, its slot is released and the next TLS client is
    // admitted.
    drop(holder);
    let req = ReqSocket::with_options(&ctx, options(dialer_tls(&server, &client))).expect("req");
    dial_until_admitted(&req, &url, Duration::from_secs(3)).await;
    exchange_once(&req, &rep).await;
}
/// Claim: a connection that arrives when `max_pipes` pipes are already
/// admitted is closed at once, with nothing sent to it and no wait for a
/// handshake or its deadline, and the admitted peer keeps its pipe and its
/// traffic. Without this ordering every refused peer would cost a TLS
/// handshake, and a flood of them could hold the handshake slots.
#[tokio::test]
async fn a_connection_over_the_pipe_ceiling_is_refused_before_any_handshake() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    let rep = RepSocket::with_options(
        &ctx,
        SocketOptions {
            max_pipes: 1,
            handshake_timeout: Duration::from_secs(3600),
            ..options(listener_tls(&server, &client))
        },
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let req = ReqSocket::with_options(&ctx, options(dialer_tls(&server, &client))).expect("req");
    weida_testkit::within("the TLS client being admitted", BOUND, req.dial(&url))
        .await
        .expect("dial");
    exchange_once(&req, &rep).await;
    assert_eq!(rep.pipe_count(), 1);

    let mut extra = TcpStream::connect(tcp_address(&url))
        .await
        .expect("connect");
    let mut byte = [0u8; 1];
    let read = weida_testkit::within(
        "the listener closing the extra connection",
        Duration::from_secs(1),
        extra.read(&mut byte),
    )
    .await;
    assert_eq!(
        read.expect("a clean close"),
        0,
        "the listener sent nothing before it closed the connection"
    );

    exchange_once(&req, &rep).await;
    assert_eq!(rep.pipe_count(), 1, "the admitted peer keeps its pipe");
}
