//! PUB and SUB: publish-subscribe, 29/PUBSUB.
//!
//! - **PUB** keeps one outgoing queue per subscriber, "SHALL perform a binary
//!   comparison of the subscription against the start of the first frame of
//!   the message", "SHALL silently drop the message if the queue for a
//!   subscriber is full", "SHALL NOT block on sending", and "SHALL silently
//!   discard any messages that subscribers send it"
//!   (`docs/research/zeromq.md` §4.3).
//! - **SUB** receives only, fair-queues its publishers, and "SHALL silently
//!   discard messages if the queue for a publisher is full".
//!
//! **The filtering is the publisher's**, which is why a SUB socket's
//! `subscribe` is a message on the wire rather than a local predicate: the
//! table lives beside the subscriber's queue on the publisher, and a
//! publisher with an empty table for a peer sends that peer nothing. A fresh
//! SUB therefore receives nothing at all until it asks, and an empty
//! subscription asks for everything.
//!
//! **Dropping is the point, not a compromise.** "PUB is used mainly for
//! transient event distribution where stability of the network (e.g.
//! consistently low memory usage) is more important than reliability of
//! traffic" — so a slow subscriber loses messages instead of slowing the
//! publisher down, and [`PubSocket::publish`] reports how many copies were
//! dropped rather than hiding it.
//!
//! **Both subscription wire forms are accepted and the sent one is
//! configurable** — see [`crate::SubscriptionForm`]. A publisher that read only the
//! 3.x commands would have no subscribers at all from `zeromq` 0.6, which
//! announces 3.0 and sends ZMTP 2.0's message form.

use std::sync::Arc;
use std::time::Duration;

use weida_zmtp::SocketType;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::{Message, Multipart};
use crate::options::SocketOptions;
use crate::pipe::MuteAction;
use crate::session::ZmtpSession;
use crate::socket::{SocketCore, socket_endpoints};
use crate::split;
use crate::subscriptions::{self, Subscriptions};

/// What one [`PubSocket::publish`] achieved.
///
/// A publisher that returned nothing would be hiding the pattern's whole
/// trade: "if a publisher has no connected subscribers, it drops all
/// messages", and a subscriber whose queue is full loses its copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Published {
    /// Subscribers whose queue took a copy.
    pub delivered: usize,
    /// Subscribers that matched but whose queue was full, so their copy was
    /// dropped — 29/PUBSUB's "SHALL silently drop", made a number.
    pub dropped: usize,
    /// Peers whose subscriptions did not match. Not a loss: they asked for
    /// something else.
    pub unmatched: usize,
}

/// A PUB socket: send only, fan-out, publisher-side filtering.
#[derive(Debug)]
pub struct PubSocket {
    core: SocketCore,
}

impl PubSocket {
    /// A PUB socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<PubSocket> {
        PubSocket::with_options(context, SocketOptions::default())
    }

    /// A PUB socket with `options`.
    ///
    /// The mute action is `zmq_socket(3)`'s row for PUB — drop — and is not a
    /// setting: a publisher that blocked would let one slow subscriber stall
    /// every other one, which is the failure ZeroMQ 2.x's infinite high-water
    /// mark was known for.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<PubSocket> {
        options.pipe.outgoing.mute = MuteAction::Drop;
        Ok(PubSocket {
            core: SocketCore::new(context, SocketType::Pub, options)?,
        })
    }

    /// Publishes `message` to every subscriber whose subscriptions match its
    /// first frame.
    ///
    /// Never blocks and never fails: a subscriber with a full queue loses its
    /// copy and a message nobody subscribed to goes nowhere, both counted in
    /// [`Published`]. That is the pattern, not a degradation.
    pub fn publish(&mut self, message: impl Into<Multipart>) -> Published {
        split::fan_out(&self.core, message.into(), true)
    }

    /// How many subscribers this socket has right now, and what each holds.
    ///
    /// A publisher's view of its own subscriptions, which is what an
    /// application uses to know whether anybody is listening yet — the
    /// zguide's "slow joiner" needs exactly this or a second channel.
    pub fn subscriber_count(&self) -> usize {
        split::subscriber_count(&self.core)
    }

    /// Whether any subscriber's table would take `topic`.
    pub fn anybody_wants(&self, topic: &[u8]) -> bool {
        split::anybody_wants(&self.core, topic)
    }
}

socket_endpoints!(PubSocket);

/// A SUB socket: receive only, fair-queued, filtered by its publishers.
#[derive(Debug)]
pub struct SubSocket {
    core: SocketCore,
    /// What this socket asks for. Shared with its sessions, so a reconnect
    /// re-sends the set without the socket noticing.
    mine: Arc<Subscriptions>,
}

impl SubSocket {
    /// A SUB socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<SubSocket> {
        SubSocket::with_options(context, SocketOptions::default())
    }

    /// A SUB socket with `options`.
    ///
    /// The incoming mute action is 29/PUBSUB's rule for a receiving
    /// subscriber — "SHALL silently discard messages if the queue for a
    /// publisher is full" — which is the one receive-side drop the RFCs
    /// state, and the reason a slow subscriber cannot slow a publisher down
    /// through this socket either.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<SubSocket> {
        options.pipe.incoming.mute = MuteAction::Drop;
        let mine = Arc::new(Subscriptions::new(
            options.max_subscriptions,
            options.max_subscription_bytes,
        ));
        let session = ZmtpSession::subscribing(SocketType::Sub, Arc::clone(&mine));
        Ok(SubSocket {
            core: SocketCore::with_session(context, SocketType::Sub, options, Arc::new(session))?,
            mine,
        })
    }

    /// Subscribes to `prefix`, telling every publisher.
    ///
    /// Additive and not idempotent: two calls need two
    /// [`SubSocket::unsubscribe`]s, because that is what 37/ZMTP says a
    /// publisher counts. An empty prefix subscribes to everything.
    ///
    /// A publisher this socket connects to *later* is told at its handshake,
    /// and so is one it reconnects to — the set travels with the session.
    pub fn subscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        let prefix = prefix.as_ref();
        if self.mine.subscribe(prefix).is_none() {
            return Err(Error::EINVAL(
                format!(
                    "this socket already holds its ceiling of {} distinct subscriptions",
                    self.mine.len()
                )
                .into(),
            ));
        }
        self.announce(true, prefix);
        Ok(())
    }

    /// Removes one subscription to `prefix`, telling every publisher when it
    /// was the last one.
    ///
    /// Fails with `EINVAL` when nothing was subscribed to that prefix: a
    /// cancellation that matched nothing would be a cancellation the
    /// publisher never counted.
    pub fn unsubscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        let prefix = prefix.as_ref();
        match self.mine.cancel(prefix) {
            None => Err(Error::EINVAL(
                "this socket is not subscribed to that prefix".into(),
            )),
            Some(_) => {
                // Told every time, not only on the last one: the publisher
                // keeps the count, so a cancellation it does not hear about
                // would leave its table one ahead of ours forever.
                self.announce(false, prefix);
                Ok(())
            }
        }
    }

    /// The prefixes this socket is subscribed to, each once.
    pub fn subscriptions(&self) -> Vec<Vec<u8>> {
        self.mine.prefixes()
    }

    /// Receives the next published message, fair-queued across publishers,
    /// bounded by `ZMQ_RCVTIMEO`.
    pub async fn recv(&mut self) -> Result<Multipart> {
        let limit = self.core.options().recv_timeout;
        let exec = self.core.exec().clone();
        match limit {
            None => self.core.recv_fair().await.map(|(_, message)| message),
            Some(limit) => match exec.within(limit, self.core.recv_fair()).await {
                Some(result) => result.map(|(_, message)| message),
                None => Err(Error::EAGAIN(
                    format!("nothing was published within {limit:?} (ZMQ_RCVTIMEO)").into(),
                )),
            },
        }
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when nothing is queued.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        let exec = self.core.exec().clone();
        match exec.within(limit, self.core.recv_fair()).await {
            Some(result) => result.map(|(_, message)| message),
            None => Err(Error::EAGAIN(
                format!("nothing was published within {limit:?}").into(),
            )),
        }
    }

    /// Queues the subscription change for every current publisher.
    ///
    /// The queue carries it in the `%x01`/`%x00` API form and the session
    /// puts it on the wire in this socket's [`crate::SubscriptionForm`] — so the
    /// choice of form lives in one place and a socket never writes a command
    /// itself.
    fn announce(&self, subscribe: bool, prefix: &[u8]) {
        let frame = Message::from(subscriptions::write_message_form(subscribe, prefix));
        // A publisher that is not reachable right now will be told at the
        // handshake instead, from the set.
        split::tell_every_peer(&self.core, Multipart::single(frame));
    }
}

socket_endpoints!(SubSocket);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use crate::pipe::{PipeConfig, QueueConfig};
    use crate::subscriptions::SubscriptionForm;

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn text(message: &Multipart) -> String {
        String::from_utf8_lossy(message.frames()[0].as_slice()).into_owned()
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Claim: a subscription made in the gap between `connect` and the
    /// handshake reaches the publisher **once**, so one `unsubscribe`
    /// undoes it.
    ///
    /// Subscriptions count rather than set — "subscribing to 'A' and 'A'
    /// counts as two subscriptions, and would require two CANCEL commands to
    /// undo" — so a socket that both queued the subscription and replayed it
    /// from its table at the handshake would leave the publisher needing two
    /// cancellations for something the application asked for once, and the
    /// application's `unsubscribe` would change nothing at all, so the
    /// unsubscriptions would never appear in the Espresso trace.
    #[tokio::test]
    async fn a_subscription_made_before_the_handshake_needs_one_cancel() {
        let ctx = context();
        let mut publisher = PubSocket::new(&ctx).expect("pub");
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        // In the gap: the pipe exists, the handshake has not finished.
        subscriber.subscribe("A").expect("subscribe");
        wait_for(|| publisher.anybody_wants(b"A")).await;

        subscriber.unsubscribe("A").expect("unsubscribe");
        wait_for(|| !publisher.anybody_wants(b"A")).await;
        assert_eq!(
            publisher.publish("A one").delivered,
            0,
            "one unsubscribe stopped the publisher"
        );
    }

    /// Claim: the filter is a binary prefix on the first frame, applied by
    /// the publisher — and a fresh subscriber gets nothing until it asks.
    #[tokio::test]
    async fn a_fresh_subscriber_gets_nothing_and_a_prefix_gets_its_topics() {
        let ctx = context();
        let mut publisher = PubSocket::new(&ctx).expect("pub");
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| publisher.subscriber_count() == 1).await;

        // Subscribed to nothing: the publisher has nothing to send it.
        let report = publisher.publish("weather.uk.london 12C");
        assert_eq!(report.delivered, 0);
        assert_eq!(report.unmatched, 1, "a fresh SUB filters everything out");

        subscriber.subscribe("weather.uk").expect("subscribe");
        wait_for(|| publisher.anybody_wants(b"weather.uk.london")).await;

        assert_eq!(publisher.publish("weather.uk.london 12C").delivered, 1);
        assert_eq!(
            publisher.publish("weather.us.boston 20C").unmatched,
            1,
            "a prefix that does not match is not sent"
        );
        assert_eq!(
            text(&subscriber.recv().await.expect("recv")),
            "weather.uk.london 12C"
        );

        // The empty subscription takes everything, including what no other
        // prefix matched.
        subscriber.subscribe("").expect("subscribe to all");
        wait_for(|| publisher.anybody_wants(b"anything")).await;
        assert_eq!(publisher.publish("weather.us.boston 20C").delivered, 1);
        assert_eq!(
            text(&subscriber.recv().await.expect("recv")),
            "weather.us.boston 20C"
        );
    }

    /// Claim: subscriptions are additive and non-idempotent all the way to
    /// the publisher — two subscribes need two cancellations before the
    /// publisher stops sending.
    #[tokio::test]
    async fn two_subscribes_need_two_cancels() {
        let ctx = context();
        let mut publisher = PubSocket::new(&ctx).expect("pub");
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| publisher.subscriber_count() == 1).await;

        subscriber.subscribe("topic").expect("first");
        subscriber.subscribe("topic").expect("second");
        wait_for(|| publisher.anybody_wants(b"topic")).await;

        subscriber.unsubscribe("topic").expect("first cancel");
        // One cancellation is not enough: the publisher counted two.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            publisher.anybody_wants(b"topic"),
            "one CANCEL must not undo two SUBSCRIBEs"
        );
        assert_eq!(publisher.publish("topic still wanted").delivered, 1);

        subscriber.unsubscribe("topic").expect("second cancel");
        wait_for(|| !publisher.anybody_wants(b"topic")).await;
        assert_eq!(publisher.publish("topic no longer").unmatched, 1);

        let err = subscriber.unsubscribe("topic").unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }

    /// Claim: a subscriber whose queue is full loses copies rather than
    /// slowing the publisher down, and the loss is reported rather than
    /// silent. `ZMQ_SNDHWM` of 1 makes the bound reachable in a test; the
    /// behaviour at the bound is what is asserted.
    #[tokio::test]
    async fn a_full_subscriber_loses_copies_and_the_publisher_says_so() {
        let ctx = context();
        let mut publisher = PubSocket::with_options(
            &ctx,
            SocketOptions {
                pipe: PipeConfig {
                    outgoing: QueueConfig {
                        hwm: 1,
                        mute: MuteAction::Drop,
                        ..PipeConfig::default().outgoing
                    },
                    ..PipeConfig::default()
                },
                ..SocketOptions::default()
            },
        )
        .expect("pub");
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| publisher.subscriber_count() == 1).await;
        subscriber.subscribe("").expect("subscribe to all");
        wait_for(|| publisher.anybody_wants(b"x")).await;

        // Publish far more than the queue and the socket buffers can hold.
        // Nothing blocks and nothing fails; some copies are lost, and the
        // publisher reports how many.
        let mut dropped = 0;
        for n in 0..5_000 {
            let report = publisher.publish(format!("event {n}"));
            dropped += report.dropped;
        }
        assert!(
            dropped > 0,
            "a subscriber that never reads must lose copies rather than block the publisher"
        );

        // And the subscriber still works: what it does get is well-formed.
        let first = subscriber.recv().await.expect("recv");
        assert!(text(&first).starts_with("event "));
    }

    /// Claim: with `ZMQ_SNDHWM` set to libzmq's "no limit", a subscriber
    /// that never reads is **still** bounded, because the queue is bounded
    /// in bytes as well (`pipe::DEFAULT_QUEUE_BYTES`). Nothing here
    /// can refuse on a message count: `hwm: 0` means there is none.
    #[tokio::test]
    async fn a_byte_ceiling_bounds_a_subscriber_whose_high_water_mark_is_unlimited() {
        let ctx = context();
        let mut publisher = PubSocket::with_options(
            &ctx,
            SocketOptions {
                pipe: PipeConfig {
                    outgoing: QueueConfig {
                        hwm: 0,
                        max_bytes: 4096,
                        mute: MuteAction::Drop,
                    },
                    ..PipeConfig::default()
                },
                ..SocketOptions::default()
            },
        )
        .expect("pub");
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| publisher.subscriber_count() == 1).await;
        subscriber.subscribe("").expect("subscribe to all");
        wait_for(|| publisher.anybody_wants(b"x")).await;

        // A kibibyte per message and five mebibytes in total: far more than
        // the ceiling, the socket buffers and anything the subscriber's
        // unread queue could absorb.
        let body = "x".repeat(1024);
        let mut dropped = 0;
        for _ in 0..5_000 {
            dropped += publisher.publish(body.clone()).dropped;
        }
        assert!(
            dropped > 0,
            "a queue with no message bound must still refuse at its byte ceiling"
        );
        for peer in publisher.core.peers().iter() {
            assert!(
                peer.pipe.outgoing().queued_bytes() <= 4096 + 1024,
                "the ceiling plus the one message an empty queue always takes: {}",
                peer.pipe.outgoing().queued_bytes()
            );
        }
    }

    /// Claim: both subscription wire forms work. The default sends 3.x
    /// commands; `SubscriptionForm::LegacyMessage` sends ZMTP 2.0's
    /// one-frame message — what a 3.0 peer such as `zeromq` 0.6 reads — and a
    /// publisher accepts either, because one that read only one form would
    /// silently have no subscribers from an implementation that sends the
    /// other.
    #[tokio::test]
    async fn both_subscription_wire_forms_reach_the_publisher() {
        let ctx = context();
        for form in [SubscriptionForm::Commands, SubscriptionForm::LegacyMessage] {
            let mut publisher = PubSocket::new(&ctx).expect("pub");
            let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut subscriber = SubSocket::with_options(
                &ctx,
                SocketOptions {
                    subscription_form: form,
                    ..SocketOptions::default()
                },
            )
            .expect("sub");
            subscriber.connect(&endpoint.to_string()).expect("connect");
            wait_for(|| publisher.subscriber_count() == 1).await;

            subscriber.subscribe("news").expect("subscribe");
            wait_for(|| publisher.anybody_wants(b"news.today")).await;
            assert_eq!(
                publisher.publish("news.today headline").delivered,
                1,
                "{form:?} must reach the publisher"
            );
            assert_eq!(
                text(&subscriber.recv().await.expect("recv")),
                "news.today headline"
            );

            // And the unsubscribe in the same form is heard too.
            subscriber.unsubscribe("news").expect("unsubscribe");
            wait_for(|| !publisher.anybody_wants(b"news.today")).await;
        }
    }

    /// Claim: a subscription survives a reconnect without the application
    /// asking — the set travels with the session, so a publisher that comes
    /// back is told what this socket wants.
    #[tokio::test]
    async fn a_subscription_is_re_sent_on_reconnect() {
        let ctx = context();
        let first = PubSocket::new(&ctx).expect("pub");
        let endpoint = first.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut subscriber = SubSocket::with_options(
            &ctx,
            SocketOptions {
                reconnect_ivl: Some(Duration::from_millis(20)),
                ..SocketOptions::default()
            },
        )
        .expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| first.subscriber_count() == 1).await;
        subscriber.subscribe("live").expect("subscribe");
        wait_for(|| first.anybody_wants(b"live.feed")).await;

        // The publisher goes away and a new one takes the same endpoint. The
        // retry is the OS releasing the listening socket, not a race in the
        // library: closing a socket aborts its accept task, and the kernel
        // frees the port when that task is actually dropped.
        first.close();
        drop(first);
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

        // Nobody called subscribe again, and the new publisher knows.
        wait_for(|| second.anybody_wants(b"live.feed")).await;
        assert_eq!(second.publish("live.feed again").delivered, 1);
        assert_eq!(
            text(&subscriber.recv().await.expect("recv")),
            "live.feed again"
        );
    }

    /// Claim: a publisher discards what a subscriber sends it — "PUB SHALL
    /// silently discard any messages that subscribers send it" — while still
    /// reading the one thing such a message may be, a subscription in the
    /// 2.0 form. The subscription arrives; the payload does not.
    #[tokio::test]
    async fn a_publisher_discards_what_a_subscriber_sends() {
        let ctx = context();
        let publisher = PubSocket::new(&ctx).expect("pub");
        let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
        let subscriber = SubSocket::new(&ctx).expect("sub");
        subscriber.connect(&endpoint.to_string()).expect("connect");
        wait_for(|| publisher.subscriber_count() == 1).await;

        // Push a payload straight onto the wire, bypassing the socket's own
        // API: this is what a misbehaving subscriber does.
        let peers = subscriber.core.peers();
        peers[0]
            .pipe
            .outgoing()
            .try_send(Multipart::single("i am not a subscription"))
            .expect("queued");
        // And a legitimate 2.0-form subscription behind it.
        peers[0]
            .pipe
            .outgoing()
            .try_send(Multipart::single(subscriptions::write_message_form(
                true, b"ok",
            )))
            .expect("queued");

        // The subscription took effect, which proves the publisher read past
        // the payload rather than choking on it — and the payload reached no
        // application, because a PUB has nowhere to put one.
        wait_for(|| publisher.anybody_wants(b"ok.then")).await;
        for peer in publisher.core.peers().iter() {
            assert!(
                peer.pipe.incoming().is_empty(),
                "a publisher must hand its application nothing a subscriber sent"
            );
        }
    }
}
