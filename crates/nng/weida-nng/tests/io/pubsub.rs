//! PUB and SUB against each other, over real connections.

use std::time::Duration;

use weida_nng::{Context, ContextConfig, Error, PubSocket, SocketOptions, SubSocket};

fn options() -> SocketOptions {
    SocketOptions {
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

async fn publisher(ctx: &Context) -> (PubSocket, String) {
    let publisher = PubSocket::with_options(ctx, options()).expect("pub");
    let url = publisher
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    (publisher, url)
}

async fn subscriber(ctx: &Context, url: &str, options: SocketOptions) -> SubSocket {
    let subscriber = SubSocket::with_options(ctx, options).expect("sub");
    subscriber.dial(url).await.expect("dial");
    subscriber
}

/// Claim: the filter is at the receiver, and the sheet's consequence is
/// observable — a subscriber that asked for one prefix is **still sent**
/// the others and discards them (§4).
///
/// The plausible bug this fails on is a publisher-side optimisation:
/// anybody who later teaches PUB to skip a subscriber makes `discarded()`
/// stay at zero, and this test goes red rather than the parity table going
/// quietly wrong.
#[tokio::test]
async fn a_subscriber_is_sent_what_it_did_not_ask_for_and_throws_it_away() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (publisher, url) = publisher(&ctx).await;
    let picky = subscriber(&ctx, &url, options()).await;
    picky.subscribe(b"weather.".to_vec());
    while publisher.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for topic in ["sports.result", "weather.rain", "sports.fixture"] {
        let broadcast = publisher.send(topic.as_bytes().to_vec()).expect("send");
        assert_eq!(
            broadcast.queued, 1,
            "the publisher offered the copy without testing any subscription"
        );
    }

    let admitted = picky.recv().await.expect("the subscribed one");
    assert_eq!(admitted.body(), b"weather.rain");

    for _ in 0..200 {
        if picky.discarded() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        picky.discarded(),
        2,
        "both unsubscribed publications crossed the link and were thrown away here"
    );
}

/// Claim: an empty subscription admits everything, several subscriptions
/// may be held at once, and unsubscribing a prefix nobody holds is
/// `NNG_ENOENT` (§4).
#[tokio::test]
async fn subscriptions_are_prefixes_and_an_empty_one_admits_everything() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (publisher, url) = publisher(&ctx).await;

    let everything = subscriber(&ctx, &url, options()).await;
    everything.subscribe(Vec::new());
    let two = subscriber(&ctx, &url, options()).await;
    two.subscribe(b"a".to_vec());
    two.subscribe(b"c".to_vec());
    // A repeated subscription is not a second one: SP has nothing on the
    // wire to count, unlike ZMTP.
    two.subscribe(b"a".to_vec());
    assert_eq!(two.subscriptions().len(), 2);

    while publisher.pipe_count() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for body in ["alpha", "bravo", "charlie"] {
        publisher.send(body.as_bytes().to_vec()).expect("send");
    }

    for expected in ["alpha", "bravo", "charlie"] {
        assert_eq!(
            everything.recv().await.expect("all").body(),
            expected.as_bytes()
        );
    }
    assert_eq!(two.recv().await.expect("a").body(), b"alpha");
    assert_eq!(two.recv().await.expect("c").body(), b"charlie");
    assert_eq!(everything.discarded(), 0);

    two.unsubscribe(b"a").expect("held");
    let err = two.unsubscribe(b"zzz").unwrap_err();
    assert!(matches!(err, Error::ENOENT(_)), "{err:?}");

    publisher.send(b"alpha again".to_vec()).expect("send");
    publisher.send(b"charlie again".to_vec()).expect("send");
    assert_eq!(
        two.recv().await.expect("still subscribed").body(),
        b"charlie again",
        "the unsubscribed prefix no longer admits anything"
    );
}

/// Claim: `SUB_PREFNEW` chooses which end of a full queue is lost, and the
/// two choices are visible in what a reader gets out (§4).
#[tokio::test]
async fn prefnew_chooses_which_end_of_a_full_queue_is_lost() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (publisher, url) = publisher(&ctx).await;

    let newest = subscriber(
        &ctx,
        &url,
        SocketOptions {
            recv_depth: Some(2),
            sub_prefer_new: true,
            ..options()
        },
    )
    .await;
    let oldest = subscriber(
        &ctx,
        &url,
        SocketOptions {
            recv_depth: Some(2),
            sub_prefer_new: false,
            ..options()
        },
    )
    .await;
    newest.subscribe(Vec::new());
    oldest.subscribe(Vec::new());
    while publisher.pipe_count() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for n in 1..=6u8 {
        publisher.send(vec![n]).expect("send");
    }
    // Let the filters drain the pipes into the two socket queues.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut kept_newest = Vec::new();
    while let Ok(message) = newest.try_recv() {
        kept_newest.push(message.body()[0]);
    }
    let mut kept_oldest = Vec::new();
    while let Ok(message) = oldest.try_recv() {
        kept_oldest.push(message.body()[0]);
    }

    assert_eq!(kept_newest.len(), 2, "the queue holds its depth");
    assert_eq!(kept_oldest.len(), 2);
    assert!(
        kept_newest[0] > kept_oldest[0],
        "PREFNEW=true kept the newer publications ({kept_newest:?}) and PREFNEW=false the \
         older ones ({kept_oldest:?})"
    );
    assert_eq!(kept_oldest, [1, 2], "the first two arrivals were preserved");
}

/// Claim: a publisher with no subscriber succeeds and reaches nobody — PUB
/// is best-effort with no receipt of any kind (§4, §6).
#[tokio::test]
async fn a_publication_with_no_subscriber_reaches_nobody_and_succeeds() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let (publisher, _url) = publisher(&ctx).await;
    let broadcast = publisher.send(b"into the void".to_vec()).expect("send");
    assert_eq!(broadcast.queued, 0);
    assert_eq!(broadcast.dropped, 0);

    publisher.close();
    let err = publisher.send(b"after the close".to_vec()).unwrap_err();
    assert!(matches!(err, Error::ECLOSED(_)), "{err:?}");
}
