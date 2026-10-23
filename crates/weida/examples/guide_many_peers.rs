//! The guide's chapter 2: **many peers, and the drop.**
//!
//! ```text
//! cargo run -p weida --example guide_many_peers
//! ```
//!
//! Five programs, one per claim `docs/GUIDE.md` §2 makes, in the shape
//! chapter 1's program established: each returns its outcome as a **value**
//! rather than printing it, so `crates/weida/tests/guide.rs` asserts the code
//! a reader runs rather than a second copy written to be testable.
//!
//! Chapter 1 had one peer on each side, which is the only configuration in
//! which a send has no policy. From two peers on, every pattern has to answer
//! the same three questions — which peer, what if it cannot keep up, and who
//! else is affected — and the answers are different per pattern on purpose.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use weida::{Error, Identity, Limits, Runtime, RuntimeConfig, Subscriber, Trust};

/// One `weida://` address and the peer it names.
pub struct Bound {
    /// `weida://<fingerprint>@127.0.0.1:<port>/<path>`.
    pub url: String,
    /// Kept alive: dropping a runtime closes its connections.
    pub runtime: Runtime,
    /// Kept alive: dropping the listener releases the binding.
    pub listener: weida::Listener,
    _binding: weida::Binding,
}

/// Binds `path` on an ephemeral loopback port under a fresh identity, with
/// `config` for the serving side.
///
/// # Errors
///
/// Whatever generating an identity or binding a socket reports.
pub async fn bind_with(path: &str, config: RuntimeConfig) -> Result<Bound, Error> {
    let identity = Identity::generate()?;
    let fingerprint = identity.fingerprint()?;
    let runtime = Runtime::new(config)?;
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a loopback literal"),
            identity,
        )
        .await?;
    let url = format!(
        "weida://{fingerprint}@127.0.0.1:{}{path}",
        binding.local_addr().port()
    );
    Ok(Bound {
        url,
        runtime,
        listener,
        _binding: binding,
    })
}

/// [`bind_with`] at the defaults.
///
/// # Errors
///
/// Whatever generating an identity or binding a socket reports.
pub async fn bind(path: &str) -> Result<Bound, Error> {
    bind_with(path, RuntimeConfig::default()).await
}

/// Connects `count` subscribers to `url`, each on its own runtime built from
/// `config`, and waits until the publisher can see all of them.
///
/// One runtime per subscriber is not ceremony: a subscriber claims its path in
/// its connection's namespace, so two subscribers on one runtime would share
/// the pooled connection and the second would be refused. A real fan-out has
/// one connection per subscriber anyway — this is what a deployment looks
/// like, folded into one process.
///
/// **The subscriber's own config matters as much as the publisher's**, which
/// is why it is a parameter. A subscriber that stops reading does not stall
/// the publisher until its *own* receive queue is full: the publisher's writer
/// keeps handing copies to QUIC and QUIC keeps delivering them into the
/// subscriber's runtime. With the default 256-deep queue on both sides, 64
/// unread messages back nothing up anywhere — which is what the first version
/// of §2.2 measured, and reported as zero drops.
///
/// # Errors
///
/// Whatever building a runtime, dialling or subscribing reports.
pub async fn subscribers_with(
    url: &str,
    count: usize,
    config: RuntimeConfig,
) -> Result<Vec<(Runtime, Arc<Subscriber>)>, Error> {
    let mut set = Vec::with_capacity(count);
    for _ in 0..count {
        let client = Runtime::new(config.clone())?;
        let sub = client.subscriber(Trust::by_address());
        sub.connect(url).await?;
        sub.subscribe("").await?;
        set.push((client, Arc::new(sub)));
    }
    Ok(set)
}

/// [`subscribers_with`] at the defaults.
///
/// # Errors
///
/// Whatever building a runtime, dialling or subscribing reports.
pub async fn subscribers(
    url: &str,
    count: usize,
) -> Result<Vec<(Runtime, Arc<Subscriber>)>, Error> {
    subscribers_with(url, count, RuntimeConfig::default()).await
}

// --- §2.1 Selection is the pattern's ---------------------------------------

/// How three peers divided one sender's messages, per pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selection {
    /// Messages each of three pullers received from six pushes.
    pub push_per_peer: Vec<usize>,
    /// Messages each of three subscribers received from two publishes.
    pub fanout_per_subscriber: Vec<usize>,
    /// Messages each of three BUS members received when every member sent one.
    pub bus_per_member: Vec<usize>,
}

/// Claim §2.1: **the pattern chooses the peer set; the call does not change.**
///
/// The same one-way send reaches one peer, every peer, or every peer but the
/// sender, and nothing at the call site says which. Push rotates
/// (`crates/weida/src/stream.rs`, one `fetch_add` per send), Pub/Sub copies to
/// every matching subscriber, BUS copies to every member except the one
/// sending.
///
/// # Errors
///
/// Whatever binding, dialling or sending reports.
pub async fn selection() -> Result<Selection, Error> {
    const PUSHES: usize = 6;

    // Push: three pullers, three servers, one path, one sender.
    let mut servers = Vec::new();
    for _ in 0..3 {
        servers.push(bind("/work").await?);
    }
    let mut pullers = Vec::new();
    for server in &servers {
        pullers.push(server.listener.puller("/work")?);
    }
    let client = Runtime::new(RuntimeConfig::default())?;
    let pusher = client.pusher(Trust::by_address());
    for server in &servers {
        pusher.connect(&server.url).await?;
    }
    for message in 0..PUSHES {
        pusher.send(format!("job {message}").as_bytes()).await?;
    }
    let mut push_per_peer = Vec::with_capacity(pullers.len());
    for puller in &pullers {
        let mut mine = 0;
        // Each puller is owed its share, which is awaited however long it
        // takes; the short window after it is what proves it is no more: a
        // fourth message would have to come from somewhere.
        for _ in 0..PUSHES / pullers.len() {
            puller.recv().await?.collect(1024).await?;
            mine += 1;
        }
        while let Ok(Ok(transfer)) =
            tokio::time::timeout(Duration::from_millis(250), puller.recv()).await
        {
            transfer.collect(1024).await?;
            mine += 1;
        }
        push_per_peer.push(mine);
    }
    client.shutdown().await;
    for server in servers {
        server.runtime.shutdown().await;
    }

    // Pub/Sub: one publisher, three subscribers, every message to each.
    let bound = bind("/prices").await?;
    let publisher = bound.listener.publisher("/prices")?;
    let subs = subscribers(&bound.url, 3).await?;
    while publisher.filter_count() != subs.len() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    for message in 0..2 {
        publisher.publish("px.eur", format!("tick {message}"))?;
    }
    let mut fanout_per_subscriber = Vec::with_capacity(subs.len());
    for (_, sub) in &subs {
        let mut mine = 0;
        // Both ticks are awaited, however long they take; only then does a
        // short window check that no third copy follows.
        for _ in 0..2 {
            sub.recv().await?.collect(1024).await?;
            mine += 1;
        }
        while let Ok(Ok(transfer)) =
            tokio::time::timeout(Duration::from_millis(250), sub.recv()).await
        {
            transfer.collect(1024).await?;
            mine += 1;
        }
        fanout_per_subscriber.push(mine);
    }
    drop(subs);
    bound.runtime.shutdown().await;

    // BUS: three members, each sending once, nobody hearing itself.
    let mut buses = Vec::new();
    for index in 0..3 {
        buses.push(bind(&format!("/bus{index}")).await?);
    }
    let mut members = Vec::new();
    for (index, bus) in buses.iter().enumerate() {
        // The one factory that takes a path *and* dialling terms: a bus
        // member is bound and dialling at once.
        members.push(
            bus.listener
                .bus(&format!("/bus{index}"), Trust::by_address())?,
        );
    }
    for (index, member) in members.iter().enumerate() {
        for (other, bus) in buses.iter().enumerate() {
            if other != index {
                member.connect(&bus.url).await?;
            }
        }
    }
    for (index, member) in members.iter().enumerate() {
        member
            .send(format!("hello from {index}").as_bytes())
            .await?;
    }
    let mut bus_per_member = Vec::with_capacity(members.len());
    for member in &members {
        let mut mine = 0;
        // Every other member's message is awaited, however long it takes;
        // only then does a short window check that no third one follows.
        for _ in 0..members.len() - 1 {
            member.recv().await?.collect(1024).await?;
            mine += 1;
        }
        while let Ok(Ok(transfer)) =
            tokio::time::timeout(Duration::from_millis(250), member.recv()).await
        {
            transfer.collect(1024).await?;
            mine += 1;
        }
        bus_per_member.push(mine);
    }
    drop(members);
    for bus in buses {
        bus.runtime.shutdown().await;
    }

    Ok(Selection {
        push_per_peer,
        fanout_per_subscriber,
        bus_per_member,
    })
}

// --- §2.2 One slow reader ---------------------------------------------------

/// What a subscriber that stops reading did to itself and to its neighbour.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlowReader {
    /// Messages published in the paced phase.
    pub published: usize,
    /// Messages the reading subscriber received in that phase.
    pub healthy_received: usize,
    /// Publishes the publisher reported enqueueing for **both** subscribers.
    ///
    /// The distinction that keeps this claim honest: a subscriber that lost a
    /// copy is still a subscriber, and `publish` says so. A count below
    /// `published` would mean the silent subscriber had *left*, which would be
    /// a different experiment.
    pub reached_both: usize,
    /// Copies dropped while the publisher was paced, which is zero.
    pub dropped_while_paced: u64,
    /// Messages the silent subscriber absorbed in the unpaced phase before the
    /// first copy was refused.
    pub absorbed: usize,
    /// Copies the publisher dropped, summed over causes.
    pub dropped: u64,
    /// The cause the publisher counted, named.
    pub cause: &'static str,
}

/// Claim §2.2: **a subscriber that stops reading costs its neighbour nothing,
/// and costs itself nothing either until the publisher outruns its
/// transport.**
///
/// Two phases, because the mechanism has two halves and the first version of
/// this program conflated them.
///
/// **Phase one, paced.** The publisher waits for the reading subscriber
/// between messages. The reader gets **every** message while its neighbour
/// reads nothing at all — that is the isolation the per-subscriber stream buys
/// ([PATTERNS.md](../../../docs/PATTERNS.md) §1.3), and the drop counter stays
/// at **zero**: QUIC absorbs what the silent subscriber's application never
/// takes, and the publisher's budget permit is released when the bytes reach
/// the wire rather than when somebody reads them.
///
/// **Phase two, flat out.** The same publisher stops waiting. Now it outruns
/// the silent subscriber's transport, that subscriber's queue and budget fill,
/// and copies are dropped and counted. The number this phase reports — how
/// many more messages the silent subscriber absorbed before the first loss —
/// is the only honest answer to "how far behind may a subscriber fall", and it
/// is a property of the transport rather than of a configured limit.
///
/// The lesson is in the pairing: **overload is a rate, not a backlog.** A
/// subscriber that has read nothing for a megabyte is fine; a publisher that
/// publishes faster than a subscriber's transport drains is not.
///
/// # Errors
///
/// Whatever binding, dialling or publishing reports.
pub async fn slow_reader() -> Result<SlowReader, Error> {
    const PACED: usize = 512;
    const PAYLOAD: usize = 4096;

    let bound = bind("/feed").await?;
    let publisher = bound.listener.publisher("/feed")?;
    let subs = subscribers(&bound.url, 2).await?;
    while publisher.filter_count() != subs.len() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let reading = Arc::clone(&subs[0].1);
    let payload = vec![0x7au8; PAYLOAD];

    // Phase one: one message in flight, and the reader sets the pace.
    let mut healthy_received = 0;
    let mut reached_both = 0;
    for _ in 0..PACED {
        // The return value is the publisher's own account of the fan-out: a
        // subscriber that loses a copy is still a subscriber, and a count
        // below two would mean the silent one had *left* — a different
        // experiment.
        if publisher.publish("px.eur", payload.clone())? == 2 {
            reached_both += 1;
        }
        match reading.recv().await {
            Ok(transfer) => {
                transfer.collect(64 * 1024).await?;
                healthy_received += 1;
            }
            _ => break,
        }
    }
    let dropped_while_paced = publisher.dropped();

    // Phase two: nobody sets the pace. The silent subscriber's transport is
    // now the bottleneck, and the bound is whatever it reaches first.
    let mut absorbed = 0usize;
    while publisher.dropped() == 0 && absorbed < 64 * 1024 {
        publisher.publish("px.eur", payload.clone())?;
        absorbed += 1;
        if absorbed.is_multiple_of(256) {
            tokio::task::yield_now().await;
        }
    }

    let drops = publisher.dropped_on("px.eur");
    let cause = match drops {
        Some(ref drops) if drops.subscriber_budget > 0 => "the byte budget",
        Some(ref drops) if drops.subscriber_queue > 0 => "the queue",
        Some(_) => "no parked connection",
        None => "nothing was dropped",
    };
    let dropped = publisher.dropped();

    drop(subs);
    bound.runtime.shutdown().await;
    Ok(SlowReader {
        published: PACED,
        healthy_received,
        reached_both,
        dropped_while_paced,
        absorbed,
        dropped,
        cause,
    })
}

// --- §2.3 Which ceiling binds ----------------------------------------------

/// What a stalled subscriber reached first, at one per-subscriber budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ceiling {
    /// `Limits::subscriber_buffer_bytes` for the run.
    pub budget: usize,
    /// Messages accepted before the first drop.
    pub accepted: usize,
    /// Bytes those messages were, which is the interesting comparison.
    pub bytes: usize,
    /// `"the queue"` or `"the byte budget"`.
    pub bound: &'static str,
}

/// Claim §2.3: **a stalled subscriber is bounded twice, the counter says which
/// bound it hit, and which one that is depends on how the budget compares with
/// the connection's flow-control window.**
///
/// The two ceilings are `endpoint_queue` **messages** and
/// `subscriber_buffer_bytes` of **payload**, and the mechanism that picks
/// between them is not the payload size — it is QUIC. A copy's budget permit
/// is released when the bytes reach the wire, and the wire swallows a
/// connection's receive window's worth of them before it stops. So:
///
/// * a budget **smaller** than that window is exhausted while QUIC is still
///   accepting, and the budget refuses first;
/// * a budget **larger** than it never fills, because the window stops the
///   writer first, and then the queue fills and refuses.
///
/// Both rows below are that sentence: same payload, same queue, one variable.
/// The 64 KiB budget refuses on the budget; the 8 MiB default refuses on the
/// queue — which is exactly what `benches/fanout.rs` measured at the defaults
/// (`docs/IMPLEMENTATION.md` §4.11), and the reason this program exists rather
/// than a paragraph. The two values are an order of magnitude from the
/// absorption band measured there (0.6-1.5 MiB), because that band is a
/// property of the connection and not a constant worth pinning.
///
/// The operational point is the counter, not the rule: `dropped_on(topic)`
/// separates `subscriber_budget` from `subscriber_queue`, so a starving
/// subscriber is diagnosable without a packet capture.
///
/// # Errors
///
/// Whatever binding, dialling or publishing reports.
pub async fn ceilings() -> Result<Vec<Ceiling>, Error> {
    const PAYLOAD: usize = 1024;

    let mut found = Vec::new();
    for budget in [64 * 1024usize, 8 * 1024 * 1024] {
        let mut config = RuntimeConfig::default();
        config.limits = Limits {
            subscriber_buffer_bytes: budget,
            ..config.limits
        };
        let bound = bind_with("/bulk", config.clone()).await?;
        let publisher = bound.listener.publisher("/bulk")?;
        let subs = subscribers_with(&bound.url, 1, config).await?;
        while publisher.filter_count() != subs.len() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let payload = vec![0x7au8; PAYLOAD];
        let mut accepted = 0usize;
        // Nobody reads. The ceiling is whichever of the two this loop reaches
        // first, and the loop ends when a copy is refused.
        while publisher.dropped() == 0 && accepted < 64 * 1024 {
            publisher.publish("px.eur", payload.clone())?;
            accepted += 1;
            // Every 256 messages, so the writer tasks get the executor and the
            // queue is not filled by this loop's own greed.
            if accepted.is_multiple_of(256) {
                tokio::task::yield_now().await;
            }
        }
        let drops = publisher.dropped_on("px.eur");
        let bound_name = match drops {
            Some(ref drops) if drops.subscriber_budget > 0 => "the byte budget",
            Some(ref drops) if drops.subscriber_queue > 0 => "the queue",
            Some(_) => "no parked connection",
            None => "nothing was dropped",
        };
        found.push(Ceiling {
            budget,
            accepted,
            bytes: accepted * PAYLOAD,
            bound: bound_name,
        });

        drop(subs);
        bound.runtime.shutdown().await;
    }
    Ok(found)
}

// --- §2.4 What width costs the publisher -----------------------------------

/// What one publish cost at one width.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Width {
    /// Subscribers connected.
    pub subscribers: usize,
    /// Time one `publish` call took, averaged over the run.
    pub per_publish: Duration,
    /// Copies that arrived, which must be `subscribers * messages`.
    pub copies: usize,
    /// Copies the publisher dropped.
    pub dropped: u64,
}

/// Claim §2.4: **the publisher's cost per message grows with the width, and
/// its cost per subscriber falls.**
///
/// `publish` is synchronous and never waits on a subscriber, so its cost is
/// width matcher walks, width queue pushes and width budget reservations. The
/// interesting half is that the *marginal* cost per subscriber goes down: the
/// fixed cost of a publish is amortized and the registry walk gets its cache.
/// `docs/IMPLEMENTATION.md` §4.11 measured 287 ns at width 1 and 22.94 µs at
/// width 256 — 88.8 ns per subscriber — which is the number the guide's §0.1
/// width table is built on.
///
/// # Errors
///
/// Whatever binding, dialling or publishing reports.
pub async fn width(widths: &[usize], messages: usize) -> Result<Vec<Width>, Error> {
    const PAYLOAD: usize = 1024;

    let mut found = Vec::with_capacity(widths.len());
    for &count in widths {
        let bound = bind(&format!("/wide{count}")).await?;
        let publisher = bound.listener.publisher(&format!("/wide{count}"))?;
        let subs = subscribers(&bound.url, count).await?;
        while publisher.filter_count() != count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let mut drains = Vec::with_capacity(count);
        for (_, sub) in &subs {
            let sub = Arc::clone(sub);
            drains.push(tokio::spawn(async move {
                let mut mine = 0;
                for _ in 0..messages {
                    let Ok(transfer) = sub.recv().await else {
                        break;
                    };
                    if transfer.collect(64 * 1024).await.is_err() {
                        break;
                    }
                    mine += 1;
                }
                mine
            }));
        }

        let payload = vec![0x7au8; PAYLOAD];
        let started = Instant::now();
        for _ in 0..messages {
            let reached = publisher.publish("px.eur", payload.clone())?;
            debug_assert_eq!(reached, count);
        }
        let per_publish = started.elapsed() / messages as u32;

        let mut copies = 0;
        for drain in drains {
            copies += drain.await.unwrap_or(0);
        }
        found.push(Width {
            subscribers: count,
            per_publish,
            copies,
            dropped: publisher.dropped(),
        });

        drop(subs);
        bound.runtime.shutdown().await;
    }
    Ok(found)
}

// --- §2.5 A silent peer -----------------------------------------------------

/// What one survey heard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Surveyed {
    /// Respondents the survey was sent to.
    pub asked: usize,
    /// Answers collected before the deadline.
    pub answered: usize,
    /// Answers the deadline cut off.
    pub missing: usize,
}

/// Claim §2.5: **a survey is a fan-out with a deadline, so a silent peer is a
/// number rather than a hang.**
///
/// The other patterns answer "what if a peer cannot keep up" by dropping or by
/// rotating past it. SURVEY is the one that has to answer "what if a peer
/// never answers at all", and its answer is the deadline the caller passes:
/// the run returns what arrived, and what did not arrive is the difference.
/// `deadline` is the survey's window: the silent peer is cut off when it ends.
///
/// # Errors
///
/// Whatever binding, dialling or surveying reports.
pub async fn survey_with_a_silent_peer(deadline: Duration) -> Result<Surveyed, Error> {
    const ASKED: usize = 3;

    let mut servers = Vec::new();
    for index in 0..ASKED {
        servers.push(bind(&format!("/poll{index}")).await?);
    }
    let mut answering = Vec::new();
    for (index, server) in servers.iter().enumerate() {
        let respondent = server.listener.respondent(&format!("/poll{index}"))?;
        // The last respondent takes the question and never answers it: an
        // accepted request that is held, which is the silence a deadline
        // exists for. Note what it is *not* — not unreachable, not refusing,
        // not slow to connect. It answered the handshake and then stopped.
        let silent = index == ASKED - 1;
        answering.push(tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok(mut question) = respondent.accept().await {
                if silent {
                    held.push(question);
                    continue;
                }
                let Ok(body) = question.body().read_capped(1024).await else {
                    break;
                };
                let Ok(mut reply) = question.reply(weida::TransferMeta::default()).await else {
                    break;
                };
                if reply.write_all(&body).await.is_err() {
                    break;
                }
                if reply.finish().is_err() {
                    break;
                }
            }
        }));
    }

    let client = Runtime::new(RuntimeConfig::default())?;
    let surveyor = client.surveyor(Trust::by_address());
    for server in &servers {
        surveyor.connect(&server.url).await?;
    }
    let mut run = surveyor.survey(b"who is there", deadline).await?;
    let mut answered = 0;
    while let Some(answer) = run.next(1024).await {
        if answer.is_ok() {
            answered += 1;
        }
    }

    for task in answering {
        task.abort();
    }
    client.shutdown().await;
    for server in servers {
        server.runtime.shutdown().await;
    }
    Ok(Surveyed {
        asked: ASKED,
        answered,
        missing: ASKED - answered,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("§2.1 selection is the pattern's");
    let selected = selection().await?;
    println!(
        "     6 pushes over 3 pullers:      {:?}",
        selected.push_per_peer
    );
    println!(
        "     2 publishes to 3 subscribers: {:?}",
        selected.fanout_per_subscriber
    );
    println!(
        "     3 BUS members, one send each:  {:?}  <- never itself",
        selected.bus_per_member
    );

    println!("§2.2 one slow reader");
    let slow = slow_reader().await?;
    println!(
        "     paced:    {} published, all {} enqueued for both, the reader got {}, \
         {} dropped",
        slow.published, slow.reached_both, slow.healthy_received, slow.dropped_while_paced
    );
    println!(
        "     flat out: {} more absorbed by a subscriber that has never read, then \
         {} dropped, cause: {}",
        slow.absorbed, slow.dropped, slow.cause
    );

    println!("§2.3 which ceiling binds");
    for ceiling in ceilings().await? {
        println!(
            "     budget {:>4} KiB: {:>5} messages ({:>4} KiB) accepted, then {}",
            ceiling.budget >> 10,
            ceiling.accepted,
            ceiling.bytes >> 10,
            ceiling.bound
        );
    }

    println!("§2.4 what width costs the publisher");
    let widths = width(&[1, 16, 64], 32).await?;
    for measured in &widths {
        println!(
            "     {:>3} subscribers: {:>10?} per publish, {:>5} copies, {} dropped",
            measured.subscribers, measured.per_publish, measured.copies, measured.dropped
        );
    }
    if let (Some(one), Some(many)) = (widths.first(), widths.last()) {
        let marginal = many.per_publish.saturating_sub(one.per_publish)
            / (many.subscribers.saturating_sub(one.subscribers)).max(1) as u32;
        println!("     marginal cost per subscriber: {marginal:?}");
    }
    if cfg!(debug_assertions) {
        println!(
            "     ^ debug build: run with --release to compare with the measured \
             figures (IMPLEMENTATION.md §4.11)"
        );
    }

    println!("§2.5 a silent peer");
    let surveyed = survey_with_a_silent_peer(Duration::from_millis(500)).await?;
    println!(
        "     asked {}, answered {}, silent {} within the deadline",
        surveyed.asked, surveyed.answered, surveyed.missing
    );
    Ok(())
}
