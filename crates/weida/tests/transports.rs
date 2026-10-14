//! The same pattern tests over every transport.
//!
//! Each body below is written once and run over all of them: native QUIC,
//! the in-process transport of
//! [decision 0010](../../docs/decisions/0010-local-transport.md), `AF_UNIX`
//! and, on Windows, a named pipe. That is
//! what the transport boundary is for — above it, the
//! frames, the HELLO exchange, the negotiation and the patterns are the same
//! (`docs/PROTOCOL.md` §2.1) — and running the *same* body is the only way to
//! check that claim rather than restate it.
//!
//! All six patterns are here, and PAIR is the one whose presence changes
//! something: its **bound** side opens a stream toward a peer that dialled
//! it, which on a socket transport is only possible over the reverse pool
//! [0012 §4.4]. Req/Rep never exercises that — a reply rides the request's
//! own bidirectional stream — so before PAIR nothing here proved the pool
//! carries an ordinary send. BUS, by contrast, needs no pool at all: every
//! member writes on a connection it dialled itself, and `bus_over_unix` is
//! what turns that from an argument into a fact.

mod common;

use std::time::Duration;

use common::{Harness, Transport};
use weida::{
    DEFAULT_DATAGRAM_RECEIVE_BYTES, Deduplication, Error, FlowMeta, GuaranteeSet, Incoming, Limits,
    OrderingMode, RuntimeConfig, TransferMeta,
};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Req/Rep: one exchange, uppercased and returned.
async fn req_rep_echo(h: &Harness) {
    let replier = h.listener.replier("/transform").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut body = request.take_body();
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        let payload = body.read_capped(64 * 1024).await.expect("read");
        reply
            .write_all(&payload.to_ascii_uppercase())
            .await
            .expect("write");
        reply.finish().expect("finish");
    });

    let client = h.client();
    let requester = client.requester(h.trust());
    within(requester.connect(&h.url("/transform")))
        .await
        .expect("connect");
    let (mut request, reply) = within(requester.open(TransferMeta::default()))
        .await
        .expect("open");
    within(request.write_all(b"hello weida"))
        .await
        .expect("write");
    request.finish().expect("finish");
    let body = within(
        within(reply.recv())
            .await
            .expect("reply")
            .collect(64 * 1024),
    )
    .await
    .expect("collect");
    assert_eq!(body, b"HELLO WEIDA".to_vec());
    within(handler).await.expect("handler");
    client.shutdown().await;
}

/// Push/Pull: a one-way transfer and its receipt.
async fn push_pull_delivery(h: &Harness) {
    let puller = h.listener.puller("/jobs").expect("puller");

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"work item"))
        .await
        .expect("write");
    let delivery = transfer.finish().expect("finish");

    let received = within(puller.recv()).await.expect("recv");
    // What a peer presents is the transport's business and nothing else's:
    // an anonymous QUIC client and an in-process peer prove nothing, while
    // the kernel proves a principal on a socket [0010 §4.4].
    match h.transport {
        #[cfg(unix)]
        Transport::Unix => {
            let peer = received.meta().peer.clone().expect("the kernel proved one");
            assert!(peer.local().is_some() && peer.key().is_none());
        }
        #[cfg(windows)]
        Transport::Pipe => {
            let peer = received.meta().peer.clone().expect("the kernel proved one");
            assert!(peer.windows().is_some() && peer.key().is_none());
        }
        _ => assert_eq!(received.meta().peer, None),
    }
    let body = within(received.collect(64 * 1024)).await.expect("collect");
    assert_eq!(body, b"work item".to_vec());
    within(delivery.delivered()).await.expect("delivered");
    client.shutdown().await;
}

/// Pub/Sub: a filtered fan-out to two subscribers.
async fn pub_sub_fan_out(h: &Harness) {
    let publisher = h.listener.publisher("/md").expect("publisher");

    // Two client runtimes, because two subscribers on one runtime share the
    // pooled connection and collide on the path — which is what
    // `a_second_subscriber_on_one_connection_collides` pins.
    let first_client = h.client();
    let second_client = h.client();
    let first = first_client.subscriber(h.trust());
    let second = second_client.subscriber(h.trust());
    within(first.connect(&h.url("/md"))).await.expect("connect");
    within(second.connect(&h.url("/md")))
        .await
        .expect("connect");
    within(first.subscribe("px.#")).await.expect("subscribe");
    within(second.subscribe("fx.#")).await.expect("subscribe");

    // The publisher learns of a subscription asynchronously; publish until
    // both filters are registered rather than sleeping for them.
    within(async {
        while publisher.filter_count() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;

    publisher.publish("px.eur", &b"price"[..]).expect("publish");
    publisher.publish("fx.usd", &b"rate"[..]).expect("publish");

    let to_first = within(first.recv()).await.expect("recv");
    assert_eq!(to_first.meta().topic.as_deref(), Some("px.eur"));
    assert_eq!(
        within(to_first.collect(1024)).await.expect("collect"),
        b"price".to_vec()
    );
    let to_second = within(second.recv()).await.expect("recv");
    assert_eq!(to_second.meta().topic.as_deref(), Some("fx.usd"));
    assert_eq!(
        within(to_second.collect(1024)).await.expect("collect"),
        b"rate".to_vec()
    );
    first_client.shutdown().await;
    second_client.shutdown().await;
}

/// PAIR: one-way transfers in both directions on one connection.
///
/// The direction that matters here is the **bound** side's: it writes on the
/// connection its peer dialled, which on a socket transport is only possible
/// over the reverse pool the dialling side parked
/// (`docs/decisions/0012-local-connection-grouping.md` §4.4). Req/Rep never
/// exercises that — a reply rides the request's own bidirectional stream — so
/// PAIR is the first pattern whose *bound* side opens a stream, and this body
/// is what proves the pool carries it.
async fn pair_both_directions(h: &Harness) {
    let bound = h.listener.pair("/link").expect("bound pair");

    let client = h.client();
    let dialling = client.pair(h.trust());
    within(dialling.connect(&h.url("/link")))
        .await
        .expect("connect");

    within(dialling.send(b"from the dialler"))
        .await
        .expect("send");
    let inbound = within(bound.recv()).await.expect("recv");
    assert_eq!(
        within(inbound.collect(1024)).await.expect("collect"),
        b"from the dialler".to_vec()
    );

    within(bound.send(b"from the bound side"))
        .await
        .expect("the bound side reaches its peer on every transport");
    let answer = within(dialling.recv()).await.expect("recv");
    assert_eq!(
        within(answer.collect(1024)).await.expect("collect"),
        b"from the bound side".to_vec()
    );
    client.shutdown().await;
}

/// SURVEY: one exchange per respondent, answered inside the deadline.
async fn survey_one_respondent(h: &Harness) {
    let respondent = h.listener.respondent("/poll").expect("respondent");
    let answering = tokio::spawn(async move {
        let mut request = respondent.accept().await.expect("accept");
        let body = request.body().read_capped(1024).await.expect("body");
        assert_eq!(body, b"who is there");
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        reply.write_all(b"here").await.expect("write");
        reply.finish().expect("finish");
    });

    let client = h.client();
    let surveyor = client.surveyor(h.trust());
    within(surveyor.connect(&h.url("/poll")))
        .await
        .expect("connect");
    let mut run = within(surveyor.survey(b"who is there", Duration::from_secs(5)))
        .await
        .expect("survey");
    assert_eq!(run.respondents(), 1);
    assert_eq!(
        within(run.next(1024))
            .await
            .expect("an answer")
            .expect("a reply"),
        b"here".to_vec()
    );
    assert!(within(run.next(1024)).await.is_none());
    assert_eq!(run.late(), 0);
    within(answering).await.expect("respondent");
    client.shutdown().await;
}

/// BUS: two members, each bound on its own binding and dialling the other.
///
/// Two harnesses rather than one, because a bus member *is* a binding: this
/// is the only pattern that needs two of them on the same transport. Every
/// send goes out over a connection the sender dialled, so unlike PAIR a bus
/// needs no reverse pool — which is exactly the kind of claim that is worth a
/// test rather than an argument.
async fn bus_two_members(a: &Harness, b: &Harness) {
    let first = a.listener.bus("/bus", b.trust()).expect("first member");
    let second = b.listener.bus("/bus", a.trust()).expect("second member");
    within(first.connect(&b.url("/bus"))).await.expect("join b");
    within(second.connect(&a.url("/bus")))
        .await
        .expect("join a");

    assert_eq!(
        within(first.send(b"from the first")).await.expect("send"),
        1
    );
    let seen = within(second.recv()).await.expect("recv");
    assert_eq!(
        within(seen.collect(1024)).await.expect("collect"),
        b"from the first".to_vec()
    );

    assert_eq!(
        within(second.send(b"from the second")).await.expect("send"),
        1
    );
    let seen = within(first.recv()).await.expect("recv");
    assert_eq!(
        within(seen.collect(1024)).await.expect("collect"),
        b"from the second".to_vec()
    );
    assert_eq!(first.dropped(), 0);
    assert_eq!(second.dropped(), 0);
}

#[tokio::test]
async fn pair_over_quic() {
    pair_both_directions(&Harness::start(Transport::Quic).await).await;
}

#[tokio::test]
async fn pair_over_inproc() {
    pair_both_directions(&Harness::start(Transport::Inproc).await).await;
}

#[cfg(unix)]
#[tokio::test]
async fn pair_over_unix() {
    pair_both_directions(&Harness::start(Transport::Unix).await).await;
}

#[cfg(windows)]
#[tokio::test]
async fn pair_over_pipe() {
    pair_both_directions(&Harness::start(Transport::Pipe).await).await;
}

/// A runtime configuration with datagram flows on.
fn with_flows() -> RuntimeConfig {
    RuntimeConfig {
        limits: Limits {
            datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
            ..Limits::default()
        },
        ..RuntimeConfig::default()
    }
}

/// A datagram flow: registered once, then 100 paced datagrams, in order and
/// byte for byte. On QUIC they ride DATAGRAM frames; on every local
/// transport the FLOW stream carries them as length-prefixed records
/// (`docs/PROTOCOL.md` §2.1), which is what this body proves is the same
/// contract.
async fn flow_carries_datagrams(h: &Harness) {
    let acceptor = h.listener.acceptor("/v").expect("acceptor");
    let client = h.client_with(with_flows());
    let peer = client.peer(h.trust());
    within(peer.connect(&h.url("/v"))).await.expect("connect");

    let flow = within(peer.open_flow(FlowMeta::default().with_topic("mic")))
        .await
        .expect("open flow");
    let sender = tokio::spawn(async move {
        for i in 0..100u64 {
            let mut payload = vec![0x5A; 200];
            payload[..8].copy_from_slice(&i.to_be_bytes());
            flow.send(payload).expect("send");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });
    let incoming = match within(acceptor.accept()).await.expect("accept") {
        Incoming::Flow(flow) => flow,
        other => panic!("expected a flow, got {other:?}"),
    };
    assert_eq!(incoming.info().topic.as_deref(), Some("mic"));
    let mut seen = Vec::new();
    while let Some(datagram) = within(incoming.recv()).await {
        assert_eq!(datagram.len(), 200);
        assert!(datagram[8..].iter().all(|b| *b == 0x5A));
        seen.push(u64::from_be_bytes(datagram[..8].try_into().expect("index")));
    }
    sender.await.expect("sender");
    assert!(seen.len() >= 90, "received {} of 100", seen.len());
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
    client.shutdown().await;
}

#[tokio::test]
async fn flow_over_quic() {
    flow_carries_datagrams(&Harness::start_with(Transport::Quic, with_flows()).await).await;
}

#[tokio::test]
async fn flow_over_inproc() {
    flow_carries_datagrams(&Harness::start_with(Transport::Inproc, with_flows()).await).await;
}

#[cfg(unix)]
#[tokio::test]
async fn flow_over_unix() {
    flow_carries_datagrams(&Harness::start_with(Transport::Unix, with_flows()).await).await;
}

#[cfg(windows)]
#[tokio::test]
async fn flow_over_pipe() {
    flow_carries_datagrams(&Harness::start_with(Transport::Pipe, with_flows()).await).await;
}

#[tokio::test]
async fn survey_over_quic() {
    survey_one_respondent(&Harness::start(Transport::Quic).await).await;
}

#[tokio::test]
async fn survey_over_inproc() {
    survey_one_respondent(&Harness::start(Transport::Inproc).await).await;
}

#[cfg(unix)]
#[tokio::test]
async fn survey_over_unix() {
    survey_one_respondent(&Harness::start(Transport::Unix).await).await;
}

#[cfg(windows)]
#[tokio::test]
async fn survey_over_pipe() {
    survey_one_respondent(&Harness::start(Transport::Pipe).await).await;
}

#[tokio::test]
async fn bus_over_quic() {
    let a = Harness::start(Transport::Quic).await;
    let b = Harness::start(Transport::Quic).await;
    bus_two_members(&a, &b).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn bus_over_inproc() {
    let a = Harness::start(Transport::Inproc).await;
    let b = Harness::start(Transport::Inproc).await;
    bus_two_members(&a, &b).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn bus_over_unix() {
    let a = Harness::start(Transport::Unix).await;
    let b = Harness::start(Transport::Unix).await;
    bus_two_members(&a, &b).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[cfg(windows)]
#[tokio::test]
async fn bus_over_pipe() {
    let a = Harness::start(Transport::Pipe).await;
    let b = Harness::start(Transport::Pipe).await;
    bus_two_members(&a, &b).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn req_rep_over_quic() {
    req_rep_echo(&Harness::start(Transport::Quic).await).await;
}

#[tokio::test]
async fn req_rep_over_inproc() {
    req_rep_echo(&Harness::start(Transport::Inproc).await).await;
}

/// `AF_UNIX` runs the same Req/Rep body. Ignored, with the reason, where the
/// suite cannot create a socket at all.
#[cfg(unix)]
#[tokio::test]
async fn req_rep_over_unix() {
    req_rep_echo(&Harness::start(Transport::Unix).await).await;
}

#[cfg(unix)]
#[tokio::test]
async fn push_pull_over_unix() {
    push_pull_delivery(&Harness::start(Transport::Unix).await).await;
}

/// Claim: the peer a local connection presents is the one the **kernel**
/// attributed to it, and it is a principal rather than a key
/// ([decisions/0010](../../docs/decisions/0010-local-transport.md) §4.4).
///
/// The uid is this process's own, because both ends are this test. What the
/// assertion is really for is the *shape*: `IncomingMeta::peer` carries a
/// local principal on a socket transport, a key on QUIC and nothing in
/// process, and a PID is reported where the platform has one and never
/// authorized on.
#[cfg(unix)]
#[tokio::test]
async fn a_unix_peer_presents_the_principal_the_kernel_proved() {
    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"work item")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    let peer = transfer
        .meta()
        .peer
        .clone()
        .expect("a local peer is proved");
    let principal = peer.local().expect("a principal, not a key");
    assert!(peer.key().is_none(), "a local peer presents no key");
    assert_eq!(
        principal.uid,
        owner_uid(h.socket_path().expect("a unix harness")),
        "the kernel names the process on the other end"
    );
    if cfg!(target_os = "linux") {
        assert!(
            principal.pid.is_some(),
            "Linux reports a PID; it is an observation, never authorized on"
        );
    }
    within(transfer.collect(64)).await.expect("collect");
    client.shutdown().await;
    h.shutdown().await;
}

/// The uid that owns a path: this process bound the socket, so it is also
/// the uid the kernel must be reporting for the peer.
#[cfg(unix)]
fn owner_uid(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).expect("the bound socket").uid()
}

/// Claim: a socket file left behind by a crash does not stop the next bind.
///
/// Unlink-then-bind is the only answer `AF_UNIX` has — closing a socket does
/// not remove its node — and the race it opens is closed by the directory's
/// ownership and permissions, which is why the doc comment on `bind_unix`
/// makes that the caller's obligation
/// (`docs/research/ipc.md` §1.2, [0010 §4.5]).
#[cfg(unix)]
#[tokio::test]
async fn a_stale_socket_file_does_not_stop_the_next_bind() {
    let h = Harness::start(Transport::Unix).await;
    let path = h.socket_path().expect("a unix harness").to_path_buf();
    assert!(path.exists(), "the socket file is there while bound");

    // A crash leaves the node behind: drop the *binding* without removing it,
    // by leaking the harness's runtime and recreating the file.
    let runtime = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let listener = runtime.listener();
    h.shutdown().await;
    std::os::unix::net::UnixListener::bind(&path).expect("leave a stale node");
    assert!(path.exists(), "a stale socket file is what a crash leaves");

    let second = listener.bind_unix(&path).expect("bind over the stale node");
    assert_eq!(second.path(), path);

    // And the mode is the one set explicitly, not whatever umask allowed.
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&path)
        .expect("bound socket")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "the socket mode is set, not inherited");
    runtime.shutdown().await;
}

/// Claim: a transfer connection that does not name a live peer is dispatched
/// nowhere.
///
/// The group token is what binds a peer's connections together
/// ([decisions/0012](../../docs/decisions/0012-local-connection-grouping.md)
/// §4.2); a connection carrying an unknown one is refused before any frame
/// on it is read, which is what stops a second process on the same machine
/// from injecting transfers into somebody else's peer.
#[cfg(unix)]
#[tokio::test]
async fn a_transfer_connection_with_an_unknown_token_is_refused() {
    use tokio::io::AsyncWriteExt;

    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let socket = h.socket_path().expect("a unix harness").to_path_buf();

    // A transfer connection whose token names no peer: kind byte plus 16
    // zero bytes, then a DATA frame for a path that exists.
    let mut raw = tokio::net::UnixStream::connect(&socket)
        .await
        .expect("connect");
    let mut preamble = vec![0x02u8];
    preamble.extend_from_slice(&[0u8; 16]);
    raw.write_all(&preamble).await.expect("write preamble");
    let header = weida_protocol::DataHeader::addressed("/jobs").encode();
    raw.write_all(&weida_protocol::encode_frame(
        weida_protocol::FrameKind::Data,
        &header,
    ))
    .await
    .expect("write frame");
    raw.write_all(b"injected").await.expect("write payload");
    drop(raw);

    // Nothing reaches the endpoint. A real transfer does, which is what
    // makes the negative observable rather than a race with the test's own
    // impatience.
    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"legitimate")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(
        within(transfer.collect(64)).await.expect("collect"),
        b"legitimate".to_vec(),
        "the refused connection must not have been dispatched"
    );
    client.shutdown().await;
    h.shutdown().await;
}

/// Claim: a subscriber that parks nothing learns so at subscribe time.
///
/// The reverse pool is what makes fan-out possible over a socket transport
/// [0012 §4.4]; a runtime configured with none of it cannot receive a
/// published copy, and saying so at `connect` is the difference between a
/// refusal and a silence.
#[cfg(unix)]
#[tokio::test]
async fn a_subscriber_that_parks_nothing_is_refused_at_connect() {
    let h = Harness::start(Transport::Unix).await;
    let _publisher = h.listener.publisher("/md").expect("publisher");
    let mut config = weida::RuntimeConfig::default();
    config.limits.max_parked_reverse = 0;
    let client = h.client_with(config);
    let subscriber = client.subscriber(h.trust());

    let err = within(subscriber.connect(&h.url("/md")))
        .await
        .expect_err("no pool, no fan-out");
    assert!(matches!(err, weida::Error::Unsupported), "{err:?}");

    client.shutdown().await;
    h.shutdown().await;
}

/// Claim: when the pool runs out, the copy is dropped and counted — the
/// publisher is not stalled and the subscription is not torn down.
///
/// A pool of one, and a subscriber that never reads: the first copy takes
/// the parked connection and sits there unread, so the replacement the
/// subscriber parks is the only one available for the next copy. Publishing
/// faster than the pool can refill is exactly the overload [0012 §4.4]
/// answers with a drop, the same answer an exhausted subscriber budget
/// already gets.
#[cfg(unix)]
#[tokio::test]
async fn an_exhausted_reverse_pool_drops_the_copy_and_counts_it() {
    let h = Harness::start(Transport::Unix).await;
    let publisher = h.listener.publisher("/md").expect("publisher");
    let mut config = weida::RuntimeConfig::default();
    config.limits.max_parked_reverse = 1;
    let client = h.client_with(config);
    let subscriber = client.subscriber(h.trust());
    within(subscriber.connect(&h.url("/md")))
        .await
        .expect("connect");
    within(subscriber.subscribe("px.#"))
        .await
        .expect("subscribe");
    within(async {
        while publisher.filter_count() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;

    // Publish without ever reading, and count the attempts: the other two
    // ways `dropped` can rise need 1024 queued copies (the writer queue) or
    // 8 MiB of them (the subscriber budget), so a drop within a handful of
    // five-byte publishes can only be the empty pool.
    let mut published = 0usize;
    within(async {
        while publisher.dropped() == 0 && published < 64 {
            publisher.publish("px.eur", &b"price"[..]).expect("publish");
            published += 1;
            tokio::task::yield_now().await;
        }
    })
    .await;

    assert!(
        publisher.dropped() >= 1,
        "a pool of one, outrun by {published} publishes, must have dropped a copy"
    );
    // And the count says which failure it was: not a subscriber that could
    // not keep up, but a subscriber with nothing parked.
    let starved = publisher
        .dropped_on("px.eur")
        .expect("the dropped topic is counted");
    assert!(starved.no_parked_connection >= 1, "{starved:?}");
    assert_eq!(starved.subscriber_budget, 0, "{starved:?}");
    // The subscription survives its drops: a copy published after the pool
    // has recovered still arrives.
    let arrived = within(async {
        loop {
            publisher.publish("px.eur", &b"later"[..]).expect("publish");
            if let Ok(copy) = subscriber.recv().await {
                break copy;
            }
        }
    })
    .await;
    assert_eq!(arrived.meta().topic.as_deref(), Some("px.eur"));

    client.shutdown().await;
    h.shutdown().await;
}

/// The named pipe runs the same three bodies.
#[cfg(windows)]
#[tokio::test]
async fn req_rep_over_pipe() {
    req_rep_echo(&Harness::start(Transport::Pipe).await).await;
}

#[cfg(windows)]
#[tokio::test]
async fn push_pull_over_pipe() {
    push_pull_delivery(&Harness::start(Transport::Pipe).await).await;
}

#[cfg(windows)]
#[tokio::test]
async fn pub_sub_over_pipe() {
    pub_sub_fan_out(&Harness::start(Transport::Pipe).await).await;
}

/// Claim: the peer a pipe connection presents is the account the **kernel**
/// attributed to it — the client's token SID — and it is a principal rather
/// than a key ([decisions/0010](../../docs/decisions/0010-local-transport.md)
/// §4.4).
///
/// The SID is this process's own, because both ends are this test; what the
/// assertion is for is the shape, and that the pid is reported and never
/// authorized on.
#[cfg(windows)]
#[tokio::test]
async fn a_pipe_peer_presents_the_principal_the_kernel_proved() {
    let h = Harness::start(Transport::Pipe).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"work item")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    let peer = transfer
        .meta()
        .peer
        .clone()
        .expect("a local peer is proved");
    let principal = peer.windows().expect("an account, not a key");
    assert!(peer.key().is_none() && peer.local().is_none());
    assert!(
        principal.sid.starts_with("S-1-"),
        "a SID in its string form: {}",
        principal.sid
    );
    assert_eq!(
        principal.pid,
        Some(std::process::id()),
        "the pipe reports the client's pid; an observation, never authorized on"
    );
    within(transfer.collect(64)).await.expect("collect");
    client.shutdown().await;
    h.shutdown().await;
}

/// Claim: a transfer connection that does not name a live peer is dispatched
/// nowhere — the same admission rule as on `AF_UNIX`, over a pipe
/// ([decisions/0012](../../docs/decisions/0012-local-connection-grouping.md)
/// §4.2).
#[cfg(windows)]
#[tokio::test]
async fn a_pipe_transfer_connection_with_an_unknown_token_is_refused() {
    use tokio::io::AsyncWriteExt;

    let h = Harness::start(Transport::Pipe).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let path = format!(r"\\.\pipe\{}", h.pipe_name().expect("a pipe harness"));

    // A transfer connection whose token names no peer: kind byte plus 16
    // zero bytes, then a DATA frame for a path that exists, as one payload
    // chunk.
    let mut raw = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(&path)
        .expect("open");
    let mut preamble = vec![0x02u8];
    preamble.extend_from_slice(&[0u8; 16]);
    raw.write_all(&preamble).await.expect("write preamble");
    let header = weida_protocol::DataHeader::addressed("/jobs").encode();
    let mut body = weida_protocol::encode_frame(weida_protocol::FrameKind::Data, &header);
    body.extend_from_slice(b"injected");
    let mut chunk = vec![0x00u8];
    chunk.extend_from_slice(&(body.len() as u32).to_le_bytes());
    chunk.extend_from_slice(&body);
    chunk.push(0x01);
    raw.write_all(&chunk).await.expect("write chunk");
    drop(raw);

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    within(pusher.send(b"legitimate")).await.expect("send");

    let transfer = within(puller.recv()).await.expect("recv");
    assert_eq!(
        within(transfer.collect(64)).await.expect("collect"),
        b"legitimate".to_vec(),
        "the refused connection must not have been dispatched"
    );
    client.shutdown().await;
    h.shutdown().await;
}

/// Claim: a request abandoned mid-payload is reported to the replier as a
/// cancellation, by name.
///
/// A pipe has no half-close and no reset, so the end of a stream and its
/// abandonment are both chunks of the pipe transport's own framing; the
/// abandonment carries the code, which is more than the socket can say. A
/// framing that lost the reset marker would leave the replier reading until
/// the connection died.
#[cfg(windows)]
#[tokio::test]
async fn an_abandoned_request_over_a_pipe_is_a_named_cancellation() {
    let h = Harness::start(Transport::Pipe).await;
    let replier = h.listener.replier("/slow").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut body = request.take_body();
        body.read_capped(64 * 1024).await
    });

    let client = h.client();
    let requester = client.requester(h.trust());
    within(requester.connect(&h.url("/slow")))
        .await
        .expect("connect");
    let (mut request, _reply) = within(requester.open(TransferMeta::default()))
        .await
        .expect("open");
    within(request.write_all(b"partial")).await.expect("write");
    request.cancel();

    let seen = within(handler).await.expect("handler");
    assert!(
        matches!(seen, Err(Error::Canceled)),
        "the replier learns the request was abandoned: {seen:?}"
    );
    client.shutdown().await;
    h.shutdown().await;
}
/// Claim: over `AF_UNIX` a cancelled transfer is reported as a cancellation,
/// and **never** as a payload that looks complete.
///
/// This is the guarantee a plain socket cannot give. A unix socket has a
/// half-close and no abort — `SO_LINGER` with a zero timeout plus `close` is
/// byte-for-byte a plain close at the reader, which was measured rather than
/// assumed — so without the chunk framing `finish` and `reset` would be the
/// same call and a reader would take half a message for a whole one.
/// [PATTERNS.md](../../../docs/PATTERNS.md) §1.5,
/// [GUARANTEES.md](../../../docs/GUARANTEES.md) §6 and
/// [FAILURE_MODEL.md](../../../docs/FAILURE_MODEL.md) §4 state it for every
/// transport; this test holds the unix socket to it.
#[cfg(unix)]
#[tokio::test]
async fn an_abandoned_transfer_over_a_socket_is_a_named_cancellation() {
    let h = Harness::start(Transport::Unix).await;
    let replier = h.listener.replier("/slow").expect("replier");
    let handler = tokio::spawn(async move {
        let mut request = replier.accept().await.expect("accept");
        let mut body = request.take_body();
        body.read_capped(64 * 1024).await
    });

    let client = h.client();
    let requester = client.requester(h.trust());
    within(requester.connect(&h.url("/slow")))
        .await
        .expect("connect");
    let (mut request, _reply) = within(requester.open(TransferMeta::default()))
        .await
        .expect("open");
    within(request.write_all(b"partial")).await.expect("write");
    request.cancel();

    let seen = within(handler).await.expect("handler");
    assert!(
        matches!(seen, Err(Error::Canceled)),
        "a cancelled transfer must not read as a complete one: {seen:?}"
    );
    client.shutdown().await;
    h.shutdown().await;
}

/// Claim: over `AF_UNIX` a transfer **dropped** without `finish` is a
/// cancellation too, which is the case an application hits by accident rather
/// than on purpose.
///
/// The drop path is the one that matters in production: a task that panics or
/// a future that is cancelled mid-write leaves exactly this. `Drop` resets the
/// stream, and without the chunk framing that reset would be a clean close.
#[cfg(unix)]
#[tokio::test]
async fn a_dropped_transfer_over_a_socket_is_a_cancellation_and_not_a_short_payload() {
    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let drained = tokio::spawn(async move {
        let transfer = puller.recv().await.expect("recv");
        transfer.collect(64 * 1024).await
    });

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    within(transfer.write_all(b"half a message"))
        .await
        .expect("write");
    // No `finish()`: the transfer simply goes out of scope.
    drop(transfer);

    let seen = within(drained).await.expect("drain task");
    assert!(
        seen.is_err(),
        "a dropped transfer must not arrive as {seen:?}"
    );
    assert!(
        matches!(seen, Err(Error::Canceled)),
        "and it is a cancellation by name: {seen:?}"
    );
    client.shutdown().await;
    h.shutdown().await;
}

/// Claim: a finished transfer over `AF_UNIX` still arrives whole, byte for
/// byte, across more writes than one chunk.
///
/// The other half of the framing change, and the regression it could cause:
/// a reader that mis-parses a chunk boundary would truncate or concatenate.
/// Three writes of different sizes, one of them larger than the read buffer.
#[cfg(unix)]
#[tokio::test]
async fn a_socket_payload_written_in_several_chunks_arrives_whole() {
    let h = Harness::start(Transport::Unix).await;
    let puller = h.listener.puller("/jobs").expect("puller");
    let reading = tokio::spawn(async move {
        let transfer = puller.recv().await.expect("recv");
        transfer.collect(1024 * 1024).await
    });

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    let mut transfer = within(pusher.open(TransferMeta::default()))
        .await
        .expect("open");
    let big = vec![0x7au8; 96 * 1024];
    within(transfer.write_all(b"one")).await.expect("write one");
    within(transfer.write_all(&big)).await.expect("write big");
    within(transfer.write_all(b"three"))
        .await
        .expect("write three");
    transfer.finish().expect("finish");

    let body = within(reading)
        .await
        .expect("read task")
        .expect("a whole payload");
    let mut expected = b"one".to_vec();
    expected.extend_from_slice(&big);
    expected.extend_from_slice(b"three");
    assert_eq!(body.len(), expected.len());
    assert_eq!(
        body, expected,
        "the chunk boundaries are not in the payload"
    );
    client.shutdown().await;
    h.shutdown().await;
}

/// Claim: an unknown-name pipe is a closed peer, and the address rules of
/// [0010 §4.8] hold for the pipe scheme too.
#[cfg(windows)]
#[tokio::test]
async fn a_pipe_address_is_checked_before_it_is_dialled() {
    let client = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let pusher = client.pusher(weida::ClientTls::new(weida::Trust::by_address()));

    let err = within(pusher.connect("weida+pipe://weida-nobody-serves-this/jobs"))
        .await
        .expect_err("no such pipe");
    assert!(matches!(err, Error::ConnectionLost(_)), "{err:?}");

    let err = within(pusher.connect(r"weida+pipe://..\admin$\x/jobs"))
        .await
        .expect_err("a backslash would leave the pipe namespace");
    assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    client.shutdown().await;
}

#[tokio::test]
async fn push_pull_over_quic() {
    push_pull_delivery(&Harness::start(Transport::Quic).await).await;
}

#[tokio::test]
async fn push_pull_over_inproc() {
    push_pull_delivery(&Harness::start(Transport::Inproc).await).await;
}

#[tokio::test]
async fn pub_sub_over_quic() {
    pub_sub_fan_out(&Harness::start(Transport::Quic).await).await;
}

#[cfg(unix)]
#[tokio::test]
async fn pub_sub_over_unix() {
    pub_sub_fan_out(&Harness::start(Transport::Unix).await).await;
}

#[tokio::test]
async fn pub_sub_over_inproc() {
    pub_sub_fan_out(&Harness::start(Transport::Inproc).await).await;
}

/// Claim: a long run of local exchanges reclaims its stream slots, so the
/// only thing `max_local_streams` bounds is what is really live.
///
/// What this pins (`docs/IMPLEMENTATION.md` §4.13):
/// locally a stream **is** an OS object counted against `max_local_streams`
/// (255), and a receipt parked for the drain holds its send half — so a
/// parked set sized by QUIC's stream budgets would hold every descriptor and a
/// sequential run would fail with `LimitExceeded` part way through, with
/// nothing in flight and nothing wrong. A thousand exchanges is four times the
/// ceiling, so any per-transfer leak fails this well before the end.
async fn sequential_exchanges_reclaim_their_slots(h: &Harness) {
    const EXCHANGES: u32 = 1000;

    let replier = h.listener.replier("/echo").expect("replier");
    let handler = tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let Ok(body) = request.take_body().collect(4096).await else {
                continue;
            };
            let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                continue;
            };
            if reply.write_all(&body).await.is_ok() {
                let _ = reply.finish();
            }
        }
    });

    let client = h.client();
    let requester = client.requester(h.trust());
    within(requester.connect(&h.url("/echo")))
        .await
        .expect("connect");
    for i in 0..EXCHANGES {
        let reply = within(requester.request(b"ping"))
            .await
            .unwrap_or_else(|e| panic!("exchange {i} of {EXCHANGES} failed: {e:?}"));
        let body = within(reply.collect(64)).await.expect("reply body");
        assert_eq!(body, b"ping");
    }

    client.shutdown().await;
    handler.abort();
}

#[tokio::test]
async fn sequential_exchanges_reclaim_their_slots_over_inproc() {
    let h = Harness::start(Transport::Inproc).await;
    sequential_exchanges_reclaim_their_slots(&h).await;
    h.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn sequential_exchanges_reclaim_their_slots_over_unix() {
    let h = Harness::start(Transport::Unix).await;
    sequential_exchanges_reclaim_their_slots(&h).await;
    h.shutdown().await;
}

#[cfg(windows)]
#[tokio::test]
async fn sequential_exchanges_reclaim_their_slots_over_pipe() {
    let h = Harness::start(Transport::Pipe).await;
    sequential_exchanges_reclaim_their_slots(&h).await;
    h.shutdown().await;
}

/// Claim: a local `open` waits for a free stream slot, so a fire-and-forget
/// sender that outruns its puller is slowed down and every message still
/// arrives.
///
/// What this pins (`docs/IMPLEMENTATION.md` §4.13): locally a
/// stream **is** an OS object counted against `max_local_streams` (255) —
/// one socket on `AF_UNIX`, one channel pair in process — and a transfer
/// holds its slot until both ends are done with it. A sender that outruns
/// its puller therefore reaches the ceiling with nothing wrong, and an `open`
/// that refused there with `LimitExceeded` would be `Reject` where
/// `docs/GUARANTEES.md` §6 promises `Block`. The local open parks on a slot
/// exactly as QUIC's parks on the peer's stream budget, so the run completes
/// at the puller's pace.
///
/// The sends are concurrent because that is what a fire-and-forget sender
/// does and what reaches the ceiling on both transports: `AF_UNIX` frees a
/// slot as soon as the sending side is done with the socket, so only
/// outstanding transfers can pile up there.
async fn push_pull_waits_for_a_slot(h: &Harness) {
    /// More than twice `max_local_streams`, so the ceiling is reached with
    /// hundreds of sends still to place.
    const MESSAGES: usize = 600;
    /// Per message: far slower than a local send, which is what makes the
    /// transfers pile up against the ceiling.
    const PULL_DELAY: Duration = Duration::from_micros(500);

    let puller = h.listener.puller("/jobs").expect("puller");
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
    let pulling = tokio::spawn(async move {
        for _ in 0..MESSAGES {
            tokio::time::sleep(PULL_DELAY).await;
            let Ok(transfer) = puller.recv().await else {
                break;
            };
            let Ok(body) = transfer.collect(64).await else {
                break;
            };
            if seen_tx.send(body).is_err() {
                break;
            }
        }
    });

    let client = h.client();
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");
    let sends = (0..MESSAGES).map(|i| {
        let pusher = &pusher;
        async move {
            pusher
                .send(i.to_string().as_bytes())
                .await
                .unwrap_or_else(|e| panic!("send {i} of {MESSAGES} failed: {e:?}"));
        }
    });
    within(futures::future::join_all(sends)).await;

    // Every message, not every message in order: Push makes no ordering
    // promise across transfers, and one connection per transfer is exactly
    // the reason ([0010 §4.2]).
    let mut arrived = Vec::with_capacity(MESSAGES);
    for i in 0..MESSAGES {
        let body = within(seen_rx.recv())
            .await
            .unwrap_or_else(|| panic!("only {i} of {MESSAGES} messages arrived"));
        let text = String::from_utf8(body.to_vec()).expect("payload is its index");
        arrived.push(text.parse::<usize>().expect("payload is its index"));
    }
    arrived.sort_unstable();
    assert_eq!(
        arrived,
        (0..MESSAGES).collect::<Vec<_>>(),
        "every pushed message must arrive exactly once"
    );
    within(pulling).await.expect("puller");
    client.shutdown().await;
}

#[tokio::test]
async fn push_pull_waits_for_a_slot_over_inproc() {
    let h = Harness::start(Transport::Inproc).await;
    push_pull_waits_for_a_slot(&h).await;
    h.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn push_pull_waits_for_a_slot_over_unix() {
    let h = Harness::start(Transport::Unix).await;
    push_pull_waits_for_a_slot(&h).await;
    h.shutdown().await;
}

#[cfg(windows)]
#[tokio::test]
async fn push_pull_waits_for_a_slot_over_pipe() {
    let h = Harness::start(Transport::Pipe).await;
    push_pull_waits_for_a_slot(&h).await;
    h.shutdown().await;
}

/// Claim: a bus nobody bound is unreachable, and the address rules of
/// [0010 §4.8] are enforced before anything is dialled.
#[tokio::test]
async fn a_local_address_is_checked_before_it_is_dialled() {
    let client = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let pusher = client.pusher(weida::ClientTls::new(weida::Trust::by_address()));

    // Nothing bound: the same outcome as dialling a closed port.
    let err = within(pusher.connect("weida+inproc://nobody-bound/jobs"))
        .await
        .expect_err("no such bus");
    assert!(matches!(err, Error::ConnectionLost(_)), "{err:?}");

    // A fingerprint would claim an authentication a local address cannot do.
    let err = within(pusher.connect(
        "weida+inproc://sha256:0000000000000000000000000000000000000000000000000000000000000000@bus/jobs",
    ))
    .await
    .expect_err("a local address carries no fingerprint");
    assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");

    // 257 bytes of bus name: one past the budget libzmq uses for the same
    // thing.
    let long = "b".repeat(257);
    let err = within(pusher.connect(&format!("weida+inproc://{long}/jobs")))
        .await
        .expect_err("bus name too long");
    assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    client.shutdown().await;
}

/// Claim: a local peer that goes away is reported as a loss, not as a hang.
///
/// A local connection has no socket to close, so the connection itself is
/// the only handle there is: shutting the server runtime down closes the
/// links it registered, and the dialling side sees the same
/// `ConnectionLost` a QUIC peer's close produces
/// ([decisions/0010](../../docs/decisions/0010-local-transport.md) §4.2,
/// `LossCause`). Under `ReconnectPolicy::never()`, so that the loss
/// reaches the caller; with the default policy the runtime would wait for
/// the bus to be bound again instead (`tests/reconnect.rs`).
#[tokio::test]
async fn a_local_peer_that_goes_away_is_reported_as_connection_loss() {
    let h = Harness::start(Transport::Inproc).await;
    let _puller = h.listener.puller("/jobs").expect("puller");
    let client = h.client_with(RuntimeConfig {
        reconnect: weida::ReconnectPolicy::never(),
        ..RuntimeConfig::default()
    });
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");

    // The server goes away: its bindings close and its connections with them.
    h.shutdown().await;

    let err = within(async {
        loop {
            match pusher.open(TransferMeta::default()).await {
                Ok(mut transfer) => {
                    // The connection may not have noticed yet; a write to a
                    // gone peer is what settles it.
                    if let Err(e) = transfer.write_all(b"x").await {
                        break e;
                    }
                }
                Err(e) => break e,
            }
        }
    })
    .await;
    assert!(
        matches!(err, Error::ConnectionLost(_) | Error::NotConnected),
        "{err:?}"
    );
    client.shutdown().await;
}

/// Claim: the guarantees and the drain are above the transport, so they work
/// over the in-process one unchanged.
///
/// Ordering, deduplication and `Runtime::drain` are all defined on headers
/// and receipts rather than on sockets, and the transport boundary is what
/// keeps that true. Nothing about them is excluded locally
/// (`docs/decisions/0010-local-transport.md` §4.2).
#[tokio::test]
async fn guarantees_and_the_drain_work_over_inproc() {
    let guarantees = GuaranteeSet {
        ordering: OrderingMode::PerProducerDetect,
        deduplication: Deduplication::Bounded,
        dedup_window_ms: Some(500),
        ..GuaranteeSet::CORE
    };
    let config = RuntimeConfig {
        guarantees,
        ..RuntimeConfig::default()
    };
    let h = Harness::start_with(Transport::Inproc, config.clone()).await;
    let puller = h.listener.puller("/jobs").expect("puller");

    let client = h.client_with(config);
    let pusher = client.pusher(h.trust());
    within(pusher.connect(&h.url("/jobs")))
        .await
        .expect("connect");

    for body in [&b"first"[..], &b"second"[..]] {
        let mut transfer = within(pusher.open(TransferMeta::default()))
            .await
            .expect("open");
        within(transfer.write_all(body)).await.expect("write");
        // Fire and forget: the receipt is dropped, so only a drain can wait
        // for it.
        drop(transfer.finish().expect("finish"));
    }

    for expected in [0u64, 1] {
        let transfer = within(puller.recv()).await.expect("recv");
        assert_eq!(
            transfer.meta().sequence,
            Some(expected),
            "the negotiated ordering numbers local transfers too"
        );
        assert_eq!(transfer.meta().gap, None);
        within(transfer.collect(1024)).await.expect("collect");
    }

    let drained = within(client.drain(Duration::from_secs(2))).await;
    assert_eq!(
        drained.outstanding, 0,
        "a local drain waits on the same receipts: {drained:?}"
    );
    assert!(drained.delivered <= 2, "{drained:?}");
    h.shutdown().await;
}
