//! XPUB and XSUB: the raw pub/sub types a proxy is built from.
//!
//! 29/PUBSUB, through `docs/research/zeromq.md` §4.3:
//!
//! - **XPUB** is "as PUB plus a double queue per subscriber, inbound
//!   messages fair-queued to the application, subscription commands
//!   delivered to the application, optional normalization so that multiple
//!   identical subscriptions result in a single command only", and it
//!   "SHALL, if the subscriber peer disconnects prematurely, generate a
//!   suitable unsubscribe request for the calling application".
//! - **XSUB** is "as SUB plus sending messages and subscriptions upstream";
//!   it "SHALL send all messages to all connected publishers", silently
//!   drops on a full outgoing queue and never blocks.
//!
//! **Subscriptions are messages here.** `zmq_socket(3)`: "byte 1 (for
//! subscriptions) or byte 0 (for unsubscriptions) followed by the
//! subscription body. Messages without a sub/unsub prefix are also received,
//! but have no effect on subscription status." That is what makes the pub/sub
//! proxy possible: an XPUB's application reads a subscription and writes it
//! into an XSUB, which puts it on the wire in whichever form that socket is
//! configured for.
//!
//! **The synthesized unsubscribe is the application's record, not the
//! peer's table.** A subscriber's table dies with its connection, so an XPUB
//! remembers what its application was *handed* for each peer and, when the
//! peer goes, hands the application one `%x00` per prefix still held —
//! otherwise a proxy would keep forwarding a subscription nobody holds any
//! more. It is the application's record and not the table because the two can
//! differ: a subscription notification is dropped when the application is not
//! reading, and an unsubscribe for a subscription the application never saw
//! would cancel, upstream, one that another subscriber's holds. What that
//! remembers is bounded by the same product as everything else: `max_peers`
//! peers times `max_subscriptions` prefixes, and the departed are turned into
//! unsubscribes one at a time ([`crate::split`]).

use std::sync::Arc;
use std::time::Duration;

use weida_zmtp::SocketType;

use crate::context::Context;
use crate::error::Result;
use crate::message::Multipart;
use crate::options::SocketOptions;
use crate::pipe::MuteAction;
use crate::pubsub::Published;
use crate::session::ZmtpSession;
use crate::socket::{SocketCore, socket_endpoints};
use crate::split::{self, XPubEvents, XPubPublish, XPubRecv, XSubRecv, XSubSend};
use crate::subscriptions::Subscriptions;

/// An XPUB socket: a PUB whose application sees the subscriptions.
#[derive(Debug)]
pub struct XPubSocket {
    core: SocketCore,
    /// The receive-side memory: what the application was told each peer
    /// holds, the unsubscribes still owed for peers that left, and who spoke
    /// last. Lives with the receiving half once split ([`crate::split`]).
    events: XPubEvents,
}

impl XPubSocket {
    /// An XPUB socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<XPubSocket> {
        XPubSocket::with_options(context, SocketOptions::default())
    }

    /// An XPUB socket with `options`.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<XPubSocket> {
        options.pipe.outgoing.mute = MuteAction::Drop;
        Ok(XPubSocket {
            core: SocketCore::new(context, SocketType::XPub, options)?,
            events: XPubEvents::default(),
        })
    }

    /// Publishes to every subscriber whose subscriptions match the first
    /// frame — PUB's rule, and PUB's drop at the high-water mark.
    pub fn publish(&mut self, message: impl Into<Multipart>) -> Published {
        split::fan_out(&self.core, message.into(), true)
    }

    /// Receives the next subscription or message from a subscriber.
    ///
    /// A subscription arrives in the `%x01`/`%x00` form; a message a
    /// subscriber sent arrives as it was sent, since "messages without a
    /// sub/unsub prefix are also received"; and an unsubscribe synthesized
    /// for a peer that vanished arrives the same way a real one would, which
    /// is the point of synthesizing it.
    ///
    /// Bounded by `ZMQ_RCVTIMEO`.
    pub async fn recv(&mut self) -> Result<Multipart> {
        self.events.recv(&self.core).await
    }

    /// The `ZMQ_DONTWAIT` form.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.events.try_recv(&self.core)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        self.events.recv_within(&self.core, limit).await
    }

    /// Applies a subscription on this socket's own authority, for
    /// `ZMQ_XPUB_MANUAL`.
    ///
    /// The subscription goes to the peer whose subscription was delivered
    /// most recently — libzmq applies it to the last pipe, and this is that
    /// rule stated. Fails with `EINVAL` when no subscription has been
    /// delivered yet, because there is no peer to apply it to.
    pub fn subscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.events.subscribe(&self.core, prefix.as_ref())
    }

    /// Removes a subscription on this socket's own authority.
    pub fn unsubscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.events.unsubscribe(&self.core, prefix.as_ref())
    }

    /// Sends one ZMTP `ERROR` to the subscriber whose subscription arrived
    /// last, naming `reason`.
    ///
    /// The counterpart of [`XPubSocket::subscribe`] and
    /// [`XPubSocket::unsubscribe`], which also act on that subscriber: those
    /// decide what this socket will match, and this says *why* something was
    /// not matched. **libzmq has no equivalent** — its XPUB application can
    /// decline to apply a subscription under `ZMQ_XPUB_MANUAL` and cannot
    /// tell the subscriber anything — so `docs/libraries/zmq.md` §9 carries
    /// it as a row. See [`crate::pipe::Pipe::refuse`] for why 37/ZMTP's
    /// `ERROR` is the only channel there is.
    ///
    /// The connection stays: what a peer does with an `ERROR` is the peer's
    /// own rule.
    ///
    /// # Errors
    ///
    /// `EINVAL` when no subscription has been delivered yet, so there is no
    /// subscriber to answer.
    pub fn refuse(&self, reason: &str) -> Result<()> {
        self.events.refuse(&self.core, reason)
    }

    /// Subscribers this socket has.
    pub fn subscriber_count(&self) -> usize {
        split::subscriber_count(&self.core)
    }

    /// Whether any subscriber's table would take `topic`.
    pub fn anybody_wants(&self, topic: &[u8]) -> bool {
        split::anybody_wants(&self.core, topic)
    }

    /// Splits the socket into a publishing half and a receiving half,
    /// usable at the same time from two tasks.
    ///
    /// Publishing reads only the subscribers' tables; receiving owns the
    /// socket's memory of them — and so `subscribe`, `unsubscribe` and
    /// `refuse`, which act on the subscriber that spoke last, go with the
    /// receiving half ([`crate::split`]).
    pub fn split(self) -> (XPubPublish, XPubRecv) {
        (
            XPubPublish {
                core: self.core.clone(),
            },
            XPubRecv {
                core: self.core,
                events: self.events,
            },
        )
    }
}

socket_endpoints!(XPubSocket);

/// An XSUB socket: a SUB that can also speak upstream.
#[derive(Debug)]
pub struct XSubSocket {
    core: SocketCore,
    mine: Arc<Subscriptions>,
}

impl XSubSocket {
    /// An XSUB socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<XSubSocket> {
        XSubSocket::with_options(context, SocketOptions::default())
    }

    /// An XSUB socket with `options`.
    ///
    /// Its outgoing queue drops rather than blocks, because XSUB "never
    /// blocks" on sending upstream, and its incoming queue drops, which is
    /// 29/PUBSUB's rule for a receiving subscriber.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<XSubSocket> {
        options.pipe.outgoing.mute = MuteAction::Drop;
        options.pipe.incoming.mute = MuteAction::Drop;
        let mine = Arc::new(Subscriptions::new(
            options.max_subscriptions,
            options.max_subscription_bytes,
        ));
        let session = ZmtpSession::subscribing(SocketType::XSub, Arc::clone(&mine));
        Ok(XSubSocket {
            core: SocketCore::with_session(context, SocketType::XSub, options, Arc::new(session))?,
            mine,
        })
    }

    /// Subscribes to `prefix`, forwarding it to every publisher — and to
    /// every publisher this socket connects or reconnects to later, because
    /// the set travels with the session.
    pub fn subscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        split::xsub_subscribe(&self.core, &self.mine, prefix.as_ref())
    }

    /// Removes one subscription to `prefix`, forwarding the cancellation.
    pub fn unsubscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        split::xsub_unsubscribe(&self.core, &self.mine, prefix.as_ref())
    }

    /// The prefixes this socket holds, each once.
    pub fn subscriptions(&self) -> Vec<Vec<u8>> {
        self.mine.prefixes()
    }

    /// Sends a message upstream to **every** connected publisher.
    ///
    /// "SHALL send all messages to all connected publishers", dropping for a
    /// publisher whose queue is full and never blocking. Returns how many
    /// took it and how many dropped it, for the same reason `PubSocket`
    /// does: a fan-out that returned nothing would hide the loss.
    ///
    /// A message whose first frame is a `%x01`/`%x00` subscription travels as
    /// a subscription, since that is what the form means on this socket type
    /// — which is exactly how a proxy forwards what its XPUB read.
    pub fn send(&mut self, message: impl Into<Multipart>) -> Published {
        split::fan_out(&self.core, message.into(), false)
    }

    /// Receives the next published message, fair-queued across publishers.
    pub async fn recv(&mut self) -> Result<Multipart> {
        split::recv_fair(&self.core).await
    }

    /// The `ZMQ_DONTWAIT` form.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        split::recv_fair_within(&self.core, limit).await
    }

    /// Splits the socket into a sending half — subscriptions and upstream
    /// messages — and a receiving half, usable at the same time from two
    /// tasks ([`crate::split`]).
    pub fn split(self) -> (XSubSend, XSubRecv) {
        (
            XSubSend {
                core: self.core.clone(),
                mine: self.mine,
            },
            XSubRecv { core: self.core },
        )
    }
}

socket_endpoints!(XSubSocket);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use crate::pubsub::{PubSocket, SubSocket};

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn bytes(message: &Multipart) -> Vec<u8> {
        message.frames()[0].as_slice().to_vec()
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn next(socket: &mut XPubSocket) -> Vec<u8> {
        let message = tokio::time::timeout(Duration::from_secs(10), socket.recv())
            .await
            .expect("an event arrived")
            .expect("event");
        bytes(&message)
    }

    /// Claim: an XPUB hands its application each subscription in the
    /// `1`/`0` form, deduplicated by default — 29/PUBSUB's normalization, so
    /// "multiple identical subscriptions result in a single command only".
    #[tokio::test]
    async fn xpub_delivers_subscriptions_in_the_one_zero_form() {
        let ctx = context();
        let mut broker = XPubSocket::new(&ctx).expect("xpub");
        let endpoint = broker.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| broker.subscriber_count() == 1).await;

        subscriber.subscribe("news").expect("subscribe");
        subscriber.subscribe("news").expect("again");
        assert_eq!(next(&mut broker).await, b"\x01news".to_vec());

        // The second subscribe changed nothing, so nothing more is
        // delivered; the unsubscribe that empties the prefix is.
        subscriber.unsubscribe("news").expect("one");
        subscriber.unsubscribe("news").expect("two");
        assert_eq!(next(&mut broker).await, b"\x00news".to_vec());
    }

    /// Claim: an XPUB can tell one subscriber **why** a subscription was not
    /// honoured, with 37/ZMTP's `ERROR` — the channel libzmq's API does not
    /// expose and an adapter cannot do without, since a silently ignored
    /// subscription is a subscriber waiting forever.
    ///
    /// The receiving side's own rule is what ends the connection: "the peer
    /// SHALL treat an incoming ERROR command as fatal", so the SUB socket
    /// drops the connection and the publisher sees its subscriber leave.
    #[tokio::test]
    async fn a_refused_subscription_reaches_the_subscriber_as_an_error() {
        let ctx = context();
        let mut broker = XPubSocket::new(&ctx).expect("xpub");
        let endpoint = broker.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| broker.subscriber_count() == 1).await;
        subscriber.subscribe("news").expect("subscribe");
        assert_eq!(next(&mut broker).await, b"\x01news".to_vec());

        // Answered to the subscriber whose subscription arrived last, which
        // is the same rule `subscribe` and `unsubscribe` keep.
        broker
            .refuse("that prefix names no weida filter")
            .expect("a subscriber to answer");
        wait_for(|| broker.subscriber_count() == 0).await;
        assert!(
            subscriber
                .recv_timeout(Duration::from_millis(200))
                .await
                .is_err(),
            "the subscriber's connection ended on the ERROR"
        );
    }

    /// Claim: `ZMQ_XPUB_VERBOSE` delivers every subscription and
    /// `ZMQ_XPUB_VERBOSER` every unsubscription too — the counts a proxy
    /// needs to forward faithfully, which the default deduplication loses.
    #[tokio::test]
    async fn verbose_and_verboser_deliver_the_repeats() {
        let ctx = context();
        let mut verbose = XPubSocket::with_options(
            &ctx,
            SocketOptions {
                xpub_verbose: true,
                ..SocketOptions::default()
            },
        )
        .expect("xpub");
        let endpoint = verbose.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| verbose.subscriber_count() == 1).await;
        subscriber.subscribe("a").expect("one");
        subscriber.subscribe("a").expect("two");
        assert_eq!(next(&mut verbose).await, b"\x01a".to_vec());
        assert_eq!(
            next(&mut verbose).await,
            b"\x01a".to_vec(),
            "verbose delivers the repeat"
        );

        let mut verboser = XPubSocket::with_options(
            &ctx,
            SocketOptions {
                xpub_verboser: true,
                ..SocketOptions::default()
            },
        )
        .expect("xpub");
        let endpoint = verboser.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| verboser.subscriber_count() == 1).await;
        // Two subscribes and one cancel: the cancel leaves the publisher
        // still holding the prefix, so it changed nothing — and that is the
        // event the default would swallow.
        subscriber.subscribe("b").expect("one");
        subscriber.subscribe("b").expect("two");
        subscriber.unsubscribe("b").expect("one off");
        assert_eq!(next(&mut verboser).await, b"\x01b".to_vec());
        assert_eq!(next(&mut verboser).await, b"\x01b".to_vec());
        assert_eq!(
            next(&mut verboser).await,
            b"\x00b".to_vec(),
            "verboser delivers an unsubscribe that changed nothing"
        );
        assert!(
            verboser.anybody_wants(b"b.still"),
            "and the prefix is still held, which is what made it a no-change"
        );
    }

    /// Claim: `ZMQ_XPUB_MANUAL` reports subscriptions without applying them,
    /// so the application decides what this socket matches — which is how a
    /// broker authorizes a subscription instead of honouring it.
    #[tokio::test]
    async fn manual_reports_without_applying() {
        let ctx = context();
        let mut broker = XPubSocket::with_options(
            &ctx,
            SocketOptions {
                xpub_manual: true,
                ..SocketOptions::default()
            },
        )
        .expect("xpub");
        let endpoint = broker.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| broker.subscriber_count() == 1).await;

        subscriber.subscribe("secret").expect("ask");
        assert_eq!(next(&mut broker).await, b"\x01secret".to_vec());
        assert!(
            !broker.anybody_wants(b"secret.plans"),
            "a manual XPUB must not have applied it"
        );
        assert_eq!(broker.publish("secret.plans").unmatched, 1);

        // The application decides to allow it.
        broker.subscribe("secret").expect("authorize");
        assert!(broker.anybody_wants(b"secret.plans"));
        assert_eq!(broker.publish("secret.plans").delivered, 1);
        assert_eq!(
            bytes(&subscriber.recv().await.expect("recv")),
            b"secret.plans".to_vec()
        );
    }

    /// Claim: `ZMQ_XPUB_WELCOME_MSG` reaches a subscriber as soon as it
    /// connects, with no subscription and no publish — "sent on connect and
    /// reconnect".
    #[tokio::test]
    async fn a_welcome_message_greets_every_subscriber() {
        let ctx = context();
        let broker = XPubSocket::with_options(
            &ctx,
            SocketOptions {
                xpub_welcome_msg: Some(b"welcome".to_vec()),
                ..SocketOptions::default()
            },
        )
        .expect("xpub");
        let endpoint = broker.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        let greeting = tokio::time::timeout(Duration::from_secs(10), subscriber.recv())
            .await
            .expect("the welcome arrived")
            .expect("welcome");
        assert_eq!(bytes(&greeting), b"welcome".to_vec());
    }

    /// Claim: when a subscriber disconnects, its subscriptions come back to
    /// the application as unsubscribes — 29/PUBSUB's "generate a suitable
    /// unsubscribe request", without which a proxy would forward a
    /// subscription nobody holds.
    #[tokio::test]
    async fn a_departure_becomes_an_unsubscribe() {
        let ctx = context();
        let mut broker = XPubSocket::new(&ctx).expect("xpub");
        let endpoint = broker.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| broker.subscriber_count() == 1).await;
        subscriber.subscribe("gone.soon").expect("subscribe");
        assert_eq!(next(&mut broker).await, b"\x01gone.soon".to_vec());

        subscriber.close();
        drop(subscriber);
        assert_eq!(
            next(&mut broker).await,
            b"\x00gone.soon".to_vec(),
            "the departure must be reported as an unsubscribe"
        );
        assert_eq!(broker.subscriber_count(), 0);
    }

    /// Claim: **an XPUB owes its application an unsubscribe for exactly the
    /// subscriptions the application was handed**, and for no others. A
    /// subscriber's own table can hold prefixes whose notification was
    /// dropped because the application was not reading, and an unsubscribe
    /// for one of those, forwarded upstream by a proxy, cancels a
    /// subscription the application never made — one that another
    /// subscriber's may be holding. Here the notification queue holds eight
    /// and the subscriber subscribes fifty prefixes in a burst the
    /// application does not read: the table holds all fifty, the application
    /// is handed eight, and when the subscriber goes it is owed those eight
    /// back and nothing else.
    #[tokio::test]
    async fn a_departure_is_unsubscribed_only_for_what_the_application_was_handed() {
        use crate::pipe::{PipeConfig, QueueConfig};
        use std::collections::BTreeSet;

        let ctx = context();
        let mut broker = XPubSocket::with_options(
            &ctx,
            SocketOptions {
                pipe: PipeConfig {
                    incoming: QueueConfig {
                        hwm: 8,
                        ..PipeConfig::default().incoming
                    },
                    ..PipeConfig::default()
                },
                ..SocketOptions::default()
            },
        )
        .expect("xpub");
        let endpoint = broker.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| broker.subscriber_count() == 1).await;

        for n in 0..50 {
            subscriber
                .subscribe(format!("topic.{n:02}"))
                .expect("subscribe");
        }
        wait_for(|| broker.anybody_wants(b"topic.49")).await;

        let mut handed = BTreeSet::new();
        while let Ok(event) = broker.try_recv() {
            let frame = event.frames()[0].as_slice();
            assert_eq!(frame[0], 1, "only subscribes so far");
            handed.insert(frame[1..].to_vec());
        }
        assert_eq!(
            handed.len(),
            8,
            "the queue held eight; the other forty-two notifications were dropped"
        );

        subscriber.close();
        drop(subscriber);
        wait_for(|| broker.subscriber_count() == 0).await;
        let mut released = BTreeSet::new();
        // Margin: 300 ms of silence ends the drain. The unsubscribes are
        // generated in memory, so the interval only has to exceed the time
        // that takes, which is microseconds.
        while let Ok(event) = broker.recv_timeout(Duration::from_millis(300)).await {
            let frame = event.frames()[0].as_slice();
            assert_eq!(frame[0], 0, "only unsubscribes now");
            released.insert(frame[1..].to_vec());
        }
        assert_eq!(
            released, handed,
            "an unsubscribe for every subscribe the application saw, and for no other"
        );
    }

    /// Claim: an XSUB forwards its subscriptions upstream — to a plain PUB,
    /// which knows nothing of XSUB — and re-sends them when the publisher
    /// comes back, because the set travels with the session.
    #[tokio::test]
    async fn xsub_forwards_upstream_and_resends_on_reconnect() {
        let ctx = context();
        let publisher = PubSocket::new(&ctx).expect("pub");
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = XSubSocket::with_options(
            &ctx,
            SocketOptions {
                reconnect_ivl: Some(Duration::from_millis(20)),
                ..SocketOptions::default()
            },
        )
        .expect("xsub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| publisher.subscriber_count() == 1).await;

        subscriber.subscribe("feed").expect("subscribe");
        wait_for(|| publisher.anybody_wants(b"feed.1")).await;
        let mut publisher = publisher;
        assert_eq!(publisher.publish("feed.1 hello").delivered, 1);
        assert_eq!(
            bytes(&subscriber.recv().await.expect("recv")),
            b"feed.1 hello".to_vec()
        );

        // The publisher goes away and another takes the endpoint.
        publisher.close();
        drop(publisher);
        let mut second = PubSocket::new(&ctx).expect("second pub");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match second.bind(&endpoint.to_string()).await {
                Ok(_) => break,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the endpoint never came free: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        wait_for(|| second.anybody_wants(b"feed.2")).await;
        assert_eq!(second.publish("feed.2 again").delivered, 1);
    }

    /// Claim: an XSUB's messages go upstream to every publisher and reach an
    /// XPUB's application — "SHALL send all messages to all connected
    /// publishers" meeting "inbound messages fair-queued to the
    /// application", which together are what a pub/sub proxy is made of.
    #[tokio::test]
    async fn xsub_sends_messages_upstream_to_an_xpub() {
        let ctx = context();
        let mut broker = XPubSocket::new(&ctx).expect("xpub");
        let endpoint = broker.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut upstream = XSubSocket::new(&ctx).expect("xsub");
        upstream.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| broker.subscriber_count() == 1).await;

        let report = upstream.send("not a subscription");
        assert_eq!(report.delivered, 1);
        assert_eq!(next(&mut broker).await, b"not a subscription".to_vec());

        // And a subscription travels as a subscription, which is how the
        // proxy forwards what its XPUB read.
        upstream.subscribe("relayed").expect("subscribe");
        wait_for(|| broker.anybody_wants(b"relayed.thing")).await;
    }

    /// Claim: the `ZMQ_XPUB_*` options are XPUB's, and a socket type that
    /// cannot honour them is told at construction.
    #[tokio::test]
    async fn the_xpub_options_belong_to_xpub() {
        let ctx = context();
        let err = XSubSocket::with_options(
            &ctx,
            SocketOptions {
                xpub_verbose: true,
                ..SocketOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        let err = PubSocket::with_options(
            &ctx,
            SocketOptions {
                xpub_welcome_msg: Some(b"hello".to_vec()),
                ..SocketOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }
}
