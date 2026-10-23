//! Req/Rep over real QUIC on loopback: one bidirectional stream per exchange.
//!
//! Every test here runs a real `quinn` endpoint pair with a freshly generated
//! certificate. Nothing is mocked and the library exposes no test hooks.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::{Certs, Server, Xorshift};
use tokio::io::AsyncReadExt;
use weida::{Error, RuntimeConfig, SimNet, TraceContext, TransferMeta};

/// Serves one request with an uppercasing echo handler.
///
/// The request half is detached before the reply half is opened, so both
/// directions of the one bidirectional stream are live at the same time — the
/// overlap the architecture requires (§83 requirements 6-8).
async fn spawn_uppercase_handler(replier: weida::Replier) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut body = request.take_body();
        let mut out = request
            .reply(TransferMeta::default())
            .await
            .expect("open reply");
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let n = body.read(&mut chunk).await.expect("read body");
            if n == 0 {
                break;
            }
            chunk[..n].make_ascii_uppercase();
            out.write_all(&chunk[..n]).await.expect("write reply");
        }
        out.finish().expect("finish reply");
    })
}

#[tokio::test]
async fn echo_roundtrip() {
    let server = Server::start().await;
    let replier = server.listener.replier("/transform").expect("replier");
    let handler = spawn_uppercase_handler(replier).await;

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/transform"))
        .await
        .expect("connect");

    let body = "hello weida ".repeat(64);
    assert_eq!(body.len(), 768);

    let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
    transfer.write_all(body.as_bytes()).await.expect("write");
    let delivery = transfer.finish().expect("finish");

    // The transport receipt says the peer's QUIC stack holds every byte. It is
    // not an application acknowledgement, and the reply below is the stronger
    // statement.
    delivery.delivered().await.expect("delivered");

    let received = reply
        .recv()
        .await
        .expect("recv reply")
        .collect(1 << 20)
        .await
        .expect("collect");
    assert_eq!(received, body.to_uppercase().into_bytes());

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn request_convenience_returns_the_reply_stream() {
    let server = Server::start().await;
    let replier = server.listener.replier("/transform").expect("replier");
    let handler = spawn_uppercase_handler(replier).await;

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/transform"))
        .await
        .expect("connect");

    let reply = requester.request(b"abc").await.expect("request");
    assert_eq!(reply.collect(16).await.expect("collect"), b"ABC");

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn streaming_overlap() {
    // 4 MiB per half: far beyond both the 1 MiB stream window and the 16 MiB
    // connection window, so the transfer cannot complete unless it really
    // streams in both directions at once.
    const HALF: usize = 4 * 1024 * 1024;

    let server = Server::start().await;
    let replier = server.listener.replier("/transform").expect("replier");
    let handler = spawn_uppercase_handler(replier).await;

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/transform"))
        .await
        .expect("connect");

    let mut rng = Xorshift::new(0x51ee_d0d0_5eed_0001);
    let mut payload = vec![0u8; HALF * 2];
    rng.fill(&mut payload);
    // Restrict to lowercase ASCII so the transform is a bijection we can check.
    for b in payload.iter_mut() {
        *b = b'a' + (*b % 26);
    }

    let (mut transfer, reply) = requester
        .open(TransferMeta::default().with_content_len((HALF * 2) as u64))
        .await
        .expect("open");

    // The reply must be drained concurrently with writing the request. This is
    // not a test artefact: with a 1:1 responder, flow control in both
    // directions is live at the same time, so a client that writes its whole
    // request before reading anything would stall the responder — and
    // therefore itself. The two halves of one bidirectional stream are
    // independent, which is what makes the overlap possible (master doc §10).
    let (first_byte_tx, first_byte_rx) = tokio::sync::oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut body = reply.recv().await.expect("early reply header");
        let mut received = Vec::with_capacity(HALF * 2);
        let mut chunk = vec![0u8; 64 * 1024];
        let mut announce = Some(first_byte_tx);
        loop {
            let n = body.read(&mut chunk).await.expect("read reply");
            if n == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..n]);
            if let Some(tx) = announce.take() {
                let _ = tx.send(received[0]);
            }
        }
        received
    });

    transfer.write_all(&payload[..HALF]).await.expect("write 1");

    // Proof of overlap: a reply byte exists before the request reaches FIN.
    let first = weida_testkit::within(
        "a reply byte before the request is finished",
        Duration::from_secs(30),
        first_byte_rx,
    )
    .await
    .expect("reader task alive");
    assert_eq!(first, payload[0].to_ascii_uppercase());

    transfer.write_all(&payload[HALF..]).await.expect("write 2");
    transfer.finish().expect("finish");

    let received = reader.await.expect("reader task");
    assert_eq!(received.len(), payload.len());
    let expected: Vec<u8> = payload.iter().map(|b| b.to_ascii_uppercase()).collect();
    assert_eq!(received, expected);

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn cancel_mid_transfer() {
    let server = Server::start().await;
    let replier = server.listener.replier("/sink").expect("replier");

    let observed = Arc::new(tokio::sync::Mutex::new(None::<Error>));
    let server_side = Arc::clone(&observed);
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            match request.body().read(&mut chunk).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) => {
                    // The reset surfaces as an I/O error carrying the code.
                    *server_side.lock().await = Some(Error::Io(e));
                    break;
                }
            }
        }
        // The requester abandoned both halves, so there is nobody to answer.
        // Dropping the request is the honest end of the exchange; the ERROR it
        // would emit lands on a stream the peer already stopped.
        drop(request);
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/sink"))
        .await
        .expect("connect");

    let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
    let chunk = vec![0x61u8; 256 * 1024];
    for _ in 0..4 {
        transfer.write_all(&chunk).await.expect("write");
    }
    transfer.cancel();
    drop(reply);

    handler.await.expect("handler");
    assert!(
        observed.lock().await.is_some(),
        "the server must observe the reset instead of a clean EOF"
    );

    // The connection is still usable: cancellation is per stream.
    let replier2 = server.listener.replier("/again").expect("replier");
    let handler2 = spawn_uppercase_handler(replier2).await;
    let requester2 = client.requester(server.trust());
    requester2
        .connect(&server.url("/again"))
        .await
        .expect("reconnect");
    let reply = requester2.request(b"ok").await.expect("second request");
    assert_eq!(reply.collect(16).await.expect("collect"), b"OK");

    handler2.await.expect("handler2");
    client.shutdown().await;
}

#[tokio::test]
async fn reply_abort_when_reply_stream_dropped() {
    let server = Server::start().await;
    let replier = server.listener.replier("/firehose").expect("replier");

    let saw_cancel = Arc::new(tokio::sync::Mutex::new(false));
    let server_side = Arc::clone(&saw_cancel);
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        // Taken before `reply` consumes the request; the future is independent
        // of the stream handle, so it can sit beside the writes in a `select!`.
        let canceled = request.canceled();
        tokio::pin!(canceled);
        // Opening the reply half is itself a write, so the stop may already
        // have arrived. That is the same observation as a failed write later:
        // nobody is listening.
        let mut reply = match request.reply(TransferMeta::default()).await {
            Ok(reply) => reply,
            Err(e) => {
                assert!(matches!(e, Error::Canceled), "{e:?}");
                *server_side.lock().await = true;
                return;
            }
        };
        let chunk = vec![0x7au8; 64 * 1024];
        loop {
            tokio::select! {
                _ = &mut canceled => {
                    *server_side.lock().await = true;
                    break;
                }
                written = reply.write_all(&chunk) => {
                    if let Err(e) = written {
                        // The requester's stop reached the stream first.
                        assert!(matches!(e, Error::Canceled), "{e:?}");
                        *server_side.lock().await = true;
                        break;
                    }
                }
            }
        }
        // A canceled reply is abandoned, not finished.
        drop(reply);
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/firehose"))
        .await
        .expect("connect");

    let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
    transfer.write_all(b"start").await.expect("write");
    transfer.finish().expect("finish");

    // Walking away from the reply half is the whole cancellation mechanism:
    // dropping it stops the stream, which is what the responder observes. No
    // CANCEL frame and no correlation id are involved.
    drop(reply);

    weida_testkit::within(
        "the handler to notice the cancellation",
        Duration::from_secs(10),
        handler,
    )
    .await
    .expect("handler");
    assert!(*saw_cancel.lock().await);

    // The connection survives.
    let replier2 = server.listener.replier("/after").expect("replier");
    let handler2 = spawn_uppercase_handler(replier2).await;
    let requester2 = client.requester(server.trust());
    requester2
        .connect(&server.url("/after"))
        .await
        .expect("reconnect");
    let reply = requester2.request(b"live").await.expect("request");
    assert_eq!(reply.collect(16).await.expect("collect"), b"LIVE");

    handler2.await.expect("handler2");
    client.shutdown().await;
}

#[tokio::test]
async fn canceled_resolves_when_the_requester_walks_away() {
    // The handler below can only finish by awaiting `canceled()`: once the
    // liveness marker is written it issues no further reads or writes, so no
    // other error path can wake it. That makes this the one test that proves
    // the future itself, rather than a `Canceled` write error standing in
    // for it.
    let server = Server::start().await;
    let replier = server.listener.replier("/watch").expect("replier");

    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        let canceled = request.canceled();
        let mut reply = request
            .reply(TransferMeta::default())
            .await
            .expect("open reply");
        // Tell the requester the reply half is live, then go quiet.
        reply.write_all(b"live").await.expect("write reply");
        canceled.await;
        // A canceled reply is abandoned, not finished.
        drop(reply);
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/watch"))
        .await
        .expect("connect");

    let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
    transfer.write_all(b"watch me").await.expect("write");
    transfer.finish().expect("finish");

    let mut body = reply.recv().await.expect("recv reply");
    let mut marker = [0u8; 4];
    body.read_exact(&mut marker)
        .await
        .expect("read the liveness marker");
    assert_eq!(&marker, b"live");

    // Walking away mid-reply stops the half; that stop is what `canceled()`
    // observes.
    drop(body);

    weida_testkit::within(
        "canceled() to resolve when the requester walks away",
        Duration::from_secs(10),
        handler,
    )
    .await
    .expect("handler");

    client.shutdown().await;
}

/// [0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md):
/// a receiver sees no trace context unless the sender propagated one, on both
/// halves of an exchange.
///
/// The pair with `trace_propagation` below is the claim: the same two peers,
/// the same calls, and the only difference is whether the caller supplied a
/// context. A runtime that minted one passes that test and fails this one.
#[tokio::test]
async fn a_transfer_carries_no_trace_context_unless_the_sender_supplied_one() {
    let server = Server::start().await;
    let replier = server.listener.replier("/untraced").expect("replier");

    let seen = Arc::new(tokio::sync::Mutex::new(Some(())));
    let server_side = Arc::clone(&seen);
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        // What the request's metadata says about a trace nobody started.
        *server_side.lock().await = request.meta().trace.map(|_| ());
        let _ = request.body().read_capped(64).await.expect("read");
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        reply.write_all(b"y").await.expect("write");
        reply.finish().expect("finish");
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/untraced"))
        .await
        .expect("connect");

    let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
    assert_eq!(
        transfer.trace(),
        None,
        "nothing mints a context on the caller's behalf"
    );
    transfer.write_all(b"x").await.expect("write");
    transfer.finish().expect("finish");

    let body = reply.recv().await.expect("recv reply");
    assert!(
        body.meta().trace.is_none(),
        "and the reply half carries none either"
    );

    handler.await.expect("handler");
    assert!(
        seen.lock().await.is_none(),
        "the responder saw no trace context on a request that started no trace"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn trace_propagation() {
    let server = Server::start().await;
    let replier = server.listener.replier("/traced").expect("replier");

    let seen = Arc::new(tokio::sync::Mutex::new(None::<TraceContext>));
    let server_side = Arc::clone(&seen);
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        *server_side.lock().await = request.meta().trace;
        let _ = request.body().read_capped(1024).await;
        // No explicit trace: the reply must inherit the request's context.
        let reply = request
            .reply(TransferMeta::default())
            .await
            .expect("open reply");
        reply.finish().expect("finish reply");
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/traced"))
        .await
        .expect("connect");

    let trace =
        TraceContext::parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .expect("valid traceparent");

    let (mut transfer, reply) = requester
        .open(TransferMeta::default().with_trace(trace))
        .await
        .expect("open");
    assert_eq!(transfer.trace(), Some(trace));
    transfer.write_all(b"x").await.expect("write");
    transfer.finish().expect("finish");

    let body = reply.recv().await.expect("recv reply");
    let reply_trace = body.meta().trace.expect("reply carries a trace context");
    assert_eq!(reply_trace.trace_id, trace.trace_id);
    assert_eq!(reply_trace.span_id, trace.span_id);

    handler.await.expect("handler");
    assert_eq!(seen.lock().await.expect("server saw a trace"), trace);
    client.shutdown().await;
}

#[tokio::test]
async fn unknown_endpoint_is_reported() {
    let server = Server::start().await;
    let _replier = server.listener.replier("/known").expect("replier");

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/nowhere"))
        .await
        .expect("connect");

    let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
    // The refusal may surface on the write (STOP_SENDING on the request half)
    // or on the reply half's ERROR frame, depending on how fast the peer
    // answers; both must name the cause.
    let err = match transfer.write_all(&vec![0u8; 4096]).await {
        Err(e) => e,
        Ok(()) => {
            let _ = transfer.finish();
            reply.recv().await.expect_err("reply must fail")
        }
    };
    assert!(
        matches!(err, Error::UnknownEndpoint),
        "expected UnknownEndpoint, got {err:?}"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn dropping_a_request_without_replying_reports_no_reply() {
    let server = Server::start().await;
    let replier = server.listener.replier("/silent").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        drop(request);
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    requester
        .connect(&server.url("/silent"))
        .await
        .expect("connect");

    let (mut transfer, reply) = requester.open(TransferMeta::default()).await.expect("open");
    transfer.write_all(b"anyone there?").await.expect("write");
    transfer.finish().expect("finish");

    // The ERROR arrives on this exchange's own reply half: no transfer id, no
    // control stream, nothing to correlate.
    let err = reply.recv().await.expect_err("reply must fail");
    assert!(
        matches!(err, Error::NoReply),
        "expected NoReply, got {err:?}"
    );

    handler.await.expect("handler");
    client.shutdown().await;
}

/// A reply that fails before its head is written is a reply that never
/// comes: the requester learns `NoReply`, as it does for a dropped request,
/// rather than a reply stream that ends with nothing in it.
#[tokio::test]
async fn a_reply_refused_before_its_head_reports_no_reply() {
    let server = Server::start().await;
    let replier = server.listener.replier("/strict").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        let unreadable = TransferMeta::default().with_content_type("c".repeat(4096));
        request.reply(unreadable).await.err()
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    weida_testkit::within(
        "the connect",
        Duration::from_secs(10),
        requester.connect(&server.url("/strict")),
    )
    .await
    .expect("connect");

    let err = weida_testkit::within(
        "the request",
        Duration::from_secs(10),
        requester.request(b"please"),
    )
    .await
    .expect_err("the reply never came");
    assert!(
        matches!(err, Error::NoReply),
        "expected NoReply, got {err:?}"
    );
    assert!(
        matches!(handler.await.expect("handler"), Some(Error::LimitExceeded)),
        "the replier learned why its reply failed"
    );

    client.shutdown().await;
}

#[tokio::test]
async fn concurrent_exchanges_do_not_interfere() {
    // Each exchange is its own bidirectional stream, so there is nothing to
    // mix up: no per-connection table, no identifiers, no ordering between
    // them. Running several at once is the check that this really holds.
    const COUNT: usize = 16;

    let server = Server::start().await;
    let replier = server.listener.replier("/transform").expect("replier");
    tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            tokio::spawn(async move {
                let mut body = request.take_body();
                let payload = body.read_capped(1024).await.expect("read body");
                let mut out = request
                    .reply(TransferMeta::default())
                    .await
                    .expect("open reply");
                out.write_all(&payload.to_ascii_uppercase())
                    .await
                    .expect("write reply");
                out.finish().expect("finish reply");
            });
        }
    });

    let client = server.client_runtime();
    let requester = Arc::new(client.requester(server.trust()));
    requester
        .connect(&server.url("/transform"))
        .await
        .expect("connect");

    let mut tasks = Vec::with_capacity(COUNT);
    for i in 0..COUNT {
        let requester = Arc::clone(&requester);
        tasks.push(tokio::spawn(async move {
            let body = format!("exchange-{i}");
            let reply = requester
                .request(body.as_bytes())
                .await
                .expect("request")
                .collect(1024)
                .await
                .expect("collect");
            assert_eq!(reply, body.to_uppercase().into_bytes());
        }));
    }
    for task in tasks {
        task.await.expect("exchange");
    }

    client.shutdown().await;
}

#[tokio::test]
async fn multi_peer_requests_round_robin() {
    let net = SimNet::new();
    let a = Server::start_on(&net, RuntimeConfig::default()).await;
    let b = Server::start_on(&net, RuntimeConfig::default()).await;
    let handler_a = spawn_uppercase_handler(a.listener.replier("/rr").expect("replier")).await;
    let handler_b = spawn_uppercase_handler(b.listener.replier("/rr").expect("replier")).await;

    // One requester trusting both servers: trust is a property of the dialling
    // endpoint, so two peers under different CAs need one endpoint, not two
    // runtimes.
    let client = weida::Runtime::new(weida::RuntimeConfig {
        network: weida::Network::sim(&net),
        ..weida::RuntimeConfig::default()
    })
    .expect("client runtime");
    let requester = client
        .requester(weida::Trust::anchor(a.certs.cert.clone()).and_anchor(b.certs.cert.clone()));
    requester.connect(&a.url("/rr")).await.expect("connect a");
    requester.connect(&b.url("/rr")).await.expect("connect b");
    assert_eq!(requester.peer_count(), 2);

    for _ in 0..2 {
        let reply = requester.request(b"peer").await.expect("request");
        assert_eq!(reply.collect(16).await.expect("collect"), b"PEER");
    }

    handler_a.await.expect("handler a");
    handler_b.await.expect("handler b");
    client.shutdown().await;
}

#[tokio::test]
async fn endpoints_with_different_trust_do_not_share_a_connection() {
    // The pool is keyed by authority *and* trust terms. Sharing on authority
    // alone would hand one endpoint a peer that was authenticated on a
    // different endpoint's terms.
    let server = Server::start().await;
    let handler = spawn_uppercase_handler(server.listener.replier("/t").expect("replier")).await;

    let client = server.client_runtime();

    // Trusts this server: dialling succeeds.
    let trusting = client.requester(server.trust());
    trusting.connect(&server.url("/t")).await.expect("connect");
    let reply = trusting.request(b"hi").await.expect("request");
    assert_eq!(reply.collect(64).await.expect("collect"), b"HI");

    // Same runtime, same authority, unrelated certificate authority. If the
    // pool reused the first connection this would wrongly succeed.
    let stranger = Certs::generate();
    let distrusting = client.requester(stranger.client_tls());
    let err = distrusting
        .connect(&server.url("/t"))
        .await
        .expect_err("a peer outside our trust must not be accepted");
    assert_eq!(
        match err {
            Error::Untrusted(presented) => presented,
            other => panic!("expected Untrusted, got {other:?}"),
        },
        server.certs.fingerprint(),
        "the refusal names the identity that answered"
    );

    handler.await.expect("handler");
    client.shutdown().await;
}

#[tokio::test]
async fn duplicate_endpoint_registration_is_rejected() {
    let server = Server::start().await;
    let _first = server.listener.replier("/dup").expect("first");
    assert!(matches!(
        server.listener.replier("/dup"),
        Err(Error::AlreadyRegistered)
    ));
    assert!(matches!(
        server.listener.replier("no-slash"),
        Err(Error::InvalidEndpointPath)
    ));
}
