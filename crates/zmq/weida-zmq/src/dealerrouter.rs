//! DEALER and ROUTER: request-reply with the state machine taken out.
//!
//! 28/REQREP, through `docs/research/zeromq.md` §4.2:
//!
//! - **DEALER** "works as an asynchronous replacement for REQ". Unrestricted
//!   in both directions: round-robin over the peers "available only when
//!   [they have] an outgoing queue that is not full", block or error when
//!   none is, never discard, fair-queue inbound. It adds **no envelope at
//!   all** — talking to a REP means the *application* writes the empty
//!   delimiter, which is what "must emulate REQ's envelope exactly" means.
//! - **ROUTER** "works as an asynchronous replacement for REP". It
//!   identifies each peer's double queue by a routing id and, inbound,
//!   prefixes the message with it; outbound it removes the first frame as
//!   that id and routes by it, and "SHALL either silently drop the message,
//!   or return an error, depending on configuration, if the queue does not
//!   exist, or is full". It "SHALL NOT block on sending".
//!
//! **The routing id is the whole of ROUTER.** "An identity (also called an
//! address) is a binary string whose only meaning is 'this is a unique handle
//! to the connection'". A peer may choose its own through the `Identity`
//! property of its `READY` — `ZMQ_ROUTING_ID` on its side — and a peer that
//! chooses none is given one, distinguishable by its leading zero
//! ([`RoutingKey`]). ROUTER learns a peer only when it has one, which is why
//! `ZMQ_PROBE_ROUTER` exists: it makes a new connection announce itself with
//! an empty message so that "an application can really reply" *and* speak
//! first.
//!
//! **Three ROUTER options change what happens at the edges**, and each is
//! libzmq's own: `ZMQ_ROUTER_MANDATORY` turns the silent drop into
//! `EHOSTUNREACH` (or `EAGAIN` at the high-water mark),
//! `ZMQ_ROUTER_HANDOVER` lets a newcomer take an identity from an incumbent
//! rather than being rejected, and `ZMQ_PROBE_ROUTER` is set on the *peer*
//! rather than on the ROUTER.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use weida_zmtp::SocketType;

use crate::context::Context;
use crate::error::Result;
use crate::identity::RoutingKey;
use crate::message::Multipart;
use crate::options::SocketOptions;
use crate::pipe::{MuteAction, Sent};
use crate::socket::{SocketCore, socket_endpoints};
use crate::split::{self, DealerRecv, DealerSend, RouterRecv, RouterSend, Routing};

/// A DEALER socket: REQ without the lockstep.
#[derive(Debug)]
pub struct DealerSocket {
    core: SocketCore,
}

impl DealerSocket {
    /// A DEALER socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<DealerSocket> {
        DealerSocket::with_options(context, SocketOptions::default())
    }

    /// A DEALER socket with `options`.
    ///
    /// The mute action is the table's row for DEALER — block — and not a
    /// setting: "SHALL not accept further messages when it has no available
    /// peers" and never discard.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<DealerSocket> {
        options.pipe.outgoing.mute = MuteAction::Block;
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(DealerSocket {
            core: SocketCore::new(context, SocketType::Dealer, options)?,
        })
    }

    /// Sends a message, round-robin over the peers with room.
    ///
    /// Unrestricted: no alternation, no envelope, no state. Blocks when every
    /// peer's queue is full and when there is no peer at all, bounded by
    /// `ZMQ_SNDTIMEO`.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        split::send_never_dropping(&self.core, message.into()).await
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` rather than a wait.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// Sends under an explicit wall-clock bound.
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        split::send_within(&self.core, message.into(), limit).await
    }

    /// Receives the next message from any peer, fair-queued, bounded by
    /// `ZMQ_RCVTIMEO`.
    pub async fn recv(&mut self) -> Result<Multipart> {
        split::recv_fair(&self.core).await
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when nothing is queued.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        split::recv_fair_within(&self.core, limit).await
    }

    /// Splits the socket into a sending half and a receiving half, usable
    /// at the same time from two tasks.
    ///
    /// The two directions of a DEALER are independent — round-robin out,
    /// fair-queue in, no alternation between them — so a task parked in
    /// `recv` need not hold up a `send`. The connections stay open while
    /// either half lives and close when the last one is dropped, as the
    /// whole socket would ([`crate::split`]).
    pub fn split(self) -> (DealerSend, DealerRecv) {
        (
            DealerSend {
                core: self.core.clone(),
            },
            DealerRecv { core: self.core },
        )
    }
}

socket_endpoints!(DealerSocket);

/// A ROUTER socket: REP without the lockstep, addressing peers by routing id.
///
/// **The routing table is bounded, and by a number that is already named.**
/// It holds exactly one entry per live peer — [`RouterSocket::recv`],
/// [`RouterSocket::send`] and [`RouterSocket::peers`] all reconcile it
/// against the engine's peer set first, inserting for a peer that has
/// arrived and dropping the entry of one that has gone — so its ceiling is
/// `max_peers` entries, each at most
/// [`MAX_ROUTING_ID_BYTES`](crate::MAX_ROUTING_ID_BYTES) of peer-chosen
/// bytes: 1024 × 255 B at the defaults, twice over for the two directions of
/// the map. A stranger chooses the *contents* of a key but not how many
/// there are, and a departed peer's key does not outlive it. That is stated
/// here rather than assumed from `max_peers`, and a test asserts the
/// shrinking half.
#[derive(Debug)]
pub struct RouterSocket {
    core: SocketCore,
    /// The routing table, shared with the halves once split; see
    /// [`crate::split`].
    routing: Arc<Mutex<Routing>>,
}

impl RouterSocket {
    /// A ROUTER socket on `context`, with libzmq's defaults.
    pub fn new(context: &Context) -> Result<RouterSocket> {
        RouterSocket::with_options(context, SocketOptions::default())
    }

    /// A ROUTER socket with `options`.
    ///
    /// The mute action is the table's row for ROUTER — drop — and not a
    /// setting: a ROUTER that blocked on one slow peer would stall every
    /// other, which is why its default is "brutal". `ZMQ_ROUTER_MANDATORY`
    /// changes what the *caller* is told, not what the queue does.
    pub fn with_options(context: &Context, mut options: SocketOptions) -> Result<RouterSocket> {
        options.pipe.outgoing.mute = MuteAction::Drop;
        options.pipe.incoming.mute = MuteAction::Block;
        Ok(RouterSocket {
            core: SocketCore::new(context, SocketType::Router, options)?,
            routing: Arc::new(Mutex::new(Routing::new())),
        })
    }

    /// Receives the next message, prefixed with the sender's routing id.
    ///
    /// `[routing-id][…the peer's frames]`, fair-queued across peers and
    /// bounded by `ZMQ_RCVTIMEO`. The id is the peer's announced `Identity`
    /// where it announced one and a generated key otherwise.
    pub async fn recv(&mut self) -> Result<Multipart> {
        split::router_recv(&self.core, &self.routing).await
    }

    /// The `ZMQ_DONTWAIT` form: `EAGAIN` when nothing is queued.
    pub fn try_recv(&mut self) -> Result<Multipart> {
        split::router_try_recv(&self.core, &self.routing)
    }

    /// Receives under an explicit wall-clock bound.
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        split::router_recv_within(&self.core, &self.routing, limit).await
    }

    /// Sends `message`, whose **first frame is the routing id** of the peer
    /// it is for.
    ///
    /// Never blocks unless `ZMQ_ROUTER_MANDATORY` is set. Without it, an
    /// unroutable message and a full queue are both [`Sent::Dropped`] — the
    /// silent drop, made visible in the return value. With it, an unroutable
    /// message is `EHOSTUNREACH` and a full queue is waited on, bounded by
    /// `ZMQ_SNDTIMEO`, which is libzmq's rule exactly.
    ///
    /// Fails with `EINVAL` when the message carries nothing but its routing
    /// id: a ROUTER strips that frame, and what is left would be no message.
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        split::router_send(&self.core, &self.routing, message.into()).await
    }

    /// The `ZMQ_DONTWAIT` form.
    ///
    /// Without `ZMQ_ROUTER_MANDATORY` this is the same silent drop; with it,
    /// a full queue is `EAGAIN` and an unknown id `EHOSTUNREACH`, which is
    /// the pair libzmq documents for the option under `ZMQ_DONTWAIT`.
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        split::router_try_send(&self.core, &self.routing, message.into())
    }

    /// The routing ids this ROUTER can address right now.
    ///
    /// A ROUTER knows a peer only once it has a routing id for it, which is
    /// after the peer's handshake — and, without `ZMQ_PROBE_ROUTER` on the
    /// peer, that is all it knows until the peer speaks.
    pub fn peers(&mut self) -> Vec<RoutingKey> {
        split::lock(&self.routing).keys(&self.core)
    }

    /// Splits the socket into a sending half and a receiving half, usable
    /// at the same time from two tasks.
    ///
    /// The two directions of a ROUTER are independent; what they share is
    /// the routing table, which both halves hold behind a lock that is
    /// never held across an await ([`crate::split`]).
    pub fn split(self) -> (RouterSend, RouterRecv) {
        (
            RouterSend {
                core: self.core.clone(),
                routing: Arc::clone(&self.routing),
            },
            RouterRecv {
                core: self.core,
                routing: self.routing,
            },
        )
    }

    /// How many routing ids the table holds right now, for the test that
    /// pins its shrinking half.
    #[cfg(test)]
    fn routing_len(&self) -> usize {
        split::lock(&self.routing).len()
    }
}

socket_endpoints!(RouterSocket);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextConfig;
    use crate::endpoint::Endpoint;
    use crate::engine::PeerId;
    use crate::identity::RoutingId;
    use crate::message::Message;
    use crate::pipe::{PipeConfig, Queue, QueueConfig};
    use crate::zap::ZapUserId;

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    fn body(message: &Multipart) -> Vec<Vec<u8>> {
        message
            .frames()
            .iter()
            .map(|frame| frame.as_slice().to_vec())
            .collect()
    }

    async fn wait_for(done: impl FnMut() -> bool) {
        weida_testkit::until("the condition", weida_testkit::EXPECTED, done).await;
    }

    fn dealer_options(routing_id: &[u8]) -> SocketOptions {
        SocketOptions {
            routing_id: Some(RoutingId::new(routing_id).expect("id")),
            reconnect_ivl: Some(Duration::from_millis(20)),
            ..SocketOptions::default()
        }
    }

    /// Claim: an option a socket type cannot honour is refused where it is
    /// set — `ZMQ_PROBE_ROUTER` on a socket that is not REQ, DEALER or
    /// ROUTER, and the ROUTER options on anything else.
    #[tokio::test]
    async fn an_option_the_socket_type_cannot_honour_is_refused() {
        let ctx = context();
        let err = DealerSocket::with_options(
            &ctx,
            SocketOptions {
                router_mandatory: true,
                ..SocketOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        let err = RouterSocket::with_options(
            &ctx,
            SocketOptions {
                req_correlate: true,
                ..SocketOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        // And the options each type *does* own are accepted.
        RouterSocket::with_options(
            &ctx,
            SocketOptions {
                router_mandatory: true,
                router_handover: true,
                probe_router: true,
                ..SocketOptions::default()
            },
        )
        .expect("a ROUTER may have all three");
    }

    /// Claim: a ROUTER message that is only a routing id is refused rather
    /// than sent as an empty message — the frame is stripped, so what is
    /// left has to be a message.
    #[tokio::test]
    async fn a_router_message_needs_a_body() {
        let ctx = context();
        let mut router = RouterSocket::new(&ctx).expect("router");
        let err = router
            .send(Multipart::single("just-an-id"))
            .await
            .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }

    /// Dials an endpoint nothing listens on, which leaves a peer entry whose
    /// outgoing queue nobody drains, and records on it what a handshake would.
    fn stranded(router: &RouterSocket, port: u16, user: Option<&str>, id: Option<&[u8]>) -> PeerId {
        let engine = router.core.engine();
        let endpoint = Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
        let peer = engine.connect(&endpoint).expect("connect");
        engine.assume(
            peer,
            user.map(ZapUserId::new),
            id.map(|id| RoutingId::new(id.to_vec()).expect("a routing id")),
        );
        peer
    }

    /// Claim: **a message the peer's queue cannot take is a drop, not an
    /// error** — "0 discards silently when it cannot be routed or the peer's
    /// SNDHWM is reached", and a ceiling in bytes is the same refusal as a
    /// ceiling in messages. A check made for one byte of room before a send of
    /// the real size says yes and then fails with `EAGAIN`, which a ROUTER
    /// without `ZMQ_ROUTER_MANDATORY` never reports. With the option, the
    /// refusal under `ZMQ_DONTWAIT` is `EAGAIN`, as before.
    #[tokio::test]
    async fn a_message_the_peers_queue_cannot_take_is_a_drop() {
        let ctx = context();
        let body = vec![b'x'; 100_000];
        let each = Queue::charge(&Multipart::single(body.clone()));
        let options = |mandatory: bool| SocketOptions {
            router_mandatory: mandatory,
            pipe: PipeConfig {
                outgoing: QueueConfig {
                    hwm: 0,
                    // Two of these messages fit, and a third does not.
                    max_bytes: 2 * each + 1000,
                    mute: MuteAction::Drop,
                },
                ..PipeConfig::default()
            },
            ..SocketOptions::default()
        };
        let framed = || {
            Multipart::new(vec![Message::from("worker"), Message::from(body.clone())])
                .expect("frames")
        };

        let mut lenient = RouterSocket::with_options(&ctx, options(false)).expect("router");
        stranded(&lenient, 9, None, Some(b"worker"));
        assert_eq!(lenient.peers(), vec![RoutingKey::from_wire(b"worker")]);
        assert_eq!(lenient.send(framed()).await.expect("first"), Sent::Queued);
        assert_eq!(lenient.send(framed()).await.expect("second"), Sent::Queued);
        assert_eq!(
            lenient.send(framed()).await.expect("a drop, not an error"),
            Sent::Dropped
        );
        assert_eq!(
            lenient.try_send(framed()).expect("a drop, not an error"),
            Sent::Dropped
        );

        let mut strict = RouterSocket::with_options(&ctx, options(true)).expect("router");
        stranded(&strict, 10, None, Some(b"worker"));
        assert_eq!(strict.peers().len(), 1);
        strict.try_send(framed()).expect("first");
        strict.try_send(framed()).expect("second");
        let err = strict.try_send(framed()).unwrap_err();
        assert_eq!(err.errno(), "EAGAIN", "{err}");
    }

    /// Claim: **with `router_bind_identity_to_user` a routing id is the
    /// authenticated user's and nobody else's.** An announced id is accepted
    /// only if it is byte for byte the user id the ZAP handler named for the
    /// connection: a peer authorized as "bravo" cannot take the id of the peer
    /// authorized as "alpha" — not by claiming it first, and not by claiming
    /// it with `ZMQ_ROUTER_HANDOVER` on — and a peer no handler named a user
    /// for cannot announce one at all. A peer that announces nothing still
    /// gets a generated id.
    #[tokio::test]
    async fn a_bound_routing_id_is_the_authenticated_users() {
        let ctx = context();
        let mut router = RouterSocket::with_options(
            &ctx,
            SocketOptions {
                router_handover: true,
                router_bind_identity_to_user: true,
                zap_domain: "test".to_string(),
                reconnect_ivl: None,
                ..SocketOptions::default()
            },
        )
        .expect("router");

        let alpha = stranded(&router, 9, Some("alpha"), Some(b"alpha"));
        let hijacker = stranded(&router, 10, Some("bravo"), Some(b"alpha"));
        let nameless = stranded(&router, 11, None, Some(b"charlie"));
        let quiet = stranded(&router, 12, Some("delta"), None);

        let keys = router.peers();
        assert_eq!(keys.len(), 2, "{keys:?}");
        assert!(keys.contains(&RoutingKey::from_wire(b"alpha")));
        assert!(keys.iter().any(RoutingKey::is_generated), "{keys:?}");

        let engine = router.core.engine();
        assert!(engine.peer(alpha).is_some(), "the owner of the id stays");
        assert!(
            engine.peer(hijacker).is_none(),
            "another user's claim is refused even with handover on"
        );
        assert!(
            engine.peer(nameless).is_none(),
            "an id no user vouches for is refused"
        );
        assert!(
            engine.peer(quiet).is_some(),
            "a peer that claims nothing is untouched"
        );
    }

    /// Claim: without the option the ROUTER is libzmq's: a routing id is
    /// whatever the peer announces, and with `ZMQ_ROUTER_HANDOVER` a newcomer
    /// takes it from the incumbent whoever either of them was authorized as.
    /// That is the hijack the option exists to close, and the default stays
    /// as libzmq has it.
    #[tokio::test]
    async fn without_binding_a_routing_id_is_whatever_the_peer_announces() {
        let ctx = context();
        let mut router = RouterSocket::with_options(
            &ctx,
            SocketOptions {
                router_handover: true,
                reconnect_ivl: None,
                ..SocketOptions::default()
            },
        )
        .expect("router");
        let alpha = stranded(&router, 9, Some("alpha"), Some(b"alpha"));
        let newcomer = stranded(&router, 10, Some("bravo"), Some(b"alpha"));

        assert_eq!(router.peers(), vec![RoutingKey::from_wire(b"alpha")]);
        let engine = router.core.engine();
        assert!(
            engine.peer(alpha).is_none(),
            "the incumbent was handed over"
        );
        assert!(engine.peer(newcomer).is_some(), "and the newcomer holds it");
    }

    /// Claim: the binding is refused where it cannot work — without ZAP, where
    /// no peer would ever have a user to compare with, and on any socket type
    /// but ROUTER.
    #[tokio::test]
    async fn identity_binding_needs_zap_and_a_router() {
        let ctx = context();
        let binding = SocketOptions {
            router_bind_identity_to_user: true,
            ..SocketOptions::default()
        };
        let err = RouterSocket::with_options(&ctx, binding.clone()).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
        assert!(err.cause().contains("ZAP"), "{err}");

        let with_zap = SocketOptions {
            zap_domain: "test".to_string(),
            ..binding
        };
        RouterSocket::with_options(&ctx, with_zap.clone()).expect("a ROUTER with ZAP may bind");
        let err = DealerSocket::with_options(&ctx, with_zap).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: both directions are unrestricted — a DEALER sends two messages
        /// without waiting for anything, and the ROUTER answers them in the
        /// order it likes, addressing each by the routing id it was handed.
        #[tokio::test]
        async fn dealer_and_router_are_unrestricted_in_both_directions() {
            let ctx = context();
            let mut router = RouterSocket::new(&ctx).expect("router");
            let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut dealer = DealerSocket::new(&ctx).expect("dealer");
            dealer.connect(&endpoint.to_string()).expect("connect");

            // Two requests, no reply awaited in between: this is what REQ
            // refuses with EFSM and DEALER exists to allow.
            dealer.send("one").await.expect("first");
            dealer.send("two").await.expect("second");

            let first = router.recv().await.expect("first request");
            let second = router.recv().await.expect("second request");
            assert_eq!(first.len(), 2, "routing id plus body");
            assert_eq!(body(&first)[1], b"one".to_vec());
            assert_eq!(body(&second)[1], b"two".to_vec());
            assert_eq!(
                body(&first)[0],
                body(&second)[0],
                "one peer, one routing id"
            );

            // Replies, newest first, because nothing makes a ROUTER answer in
            // order either.
            let key = body(&first)[0].clone();
            for reply in ["answer two", "answer one"] {
                let framed = Multipart::new(vec![Message::from(key.clone()), Message::from(reply)])
                    .expect("frames");
                assert_eq!(router.send(framed).await.expect("reply"), Sent::Queued);
            }
            assert_eq!(
                body(&dealer.recv().await.expect("reply")),
                vec![b"answer two".to_vec()]
            );
            assert_eq!(
                body(&dealer.recv().await.expect("reply")),
                vec![b"answer one".to_vec()]
            );
        }
        /// Claim: a peer that names itself is addressed by that name, and one
        /// that does not is addressed by a generated id — distinguishable by its
        /// leading zero, which is libzmq's own convention.
        #[tokio::test]
        async fn a_peer_chosen_identity_is_honoured() {
            let ctx = context();
            let mut router = RouterSocket::new(&ctx).expect("router");
            let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut named = DealerSocket::with_options(&ctx, dealer_options(b"worker-1"))
                .expect("named dealer");
            named.connect(&endpoint.to_string()).expect("connect");
            named.send("from the named one").await.expect("send");

            let request = router.recv().await.expect("request");
            assert_eq!(body(&request)[0], b"worker-1".to_vec());

            let mut anonymous = DealerSocket::new(&ctx).expect("anonymous dealer");
            anonymous.connect(&endpoint.to_string()).expect("connect");
            anonymous
                .send("from the anonymous one")
                .await
                .expect("send");

            let request = router.recv().await.expect("request");
            let key = body(&request)[0].clone();
            assert_eq!(key[0], 0, "a generated routing id starts with a zero octet");
            assert_eq!(
                key.len(),
                5,
                "zero plus a 32-bit number, as libzmq shapes it"
            );

            // And the ROUTER can address either of them by what it was handed.
            let framed =
                Multipart::new(vec![Message::from(key), Message::from("hello")]).expect("frames");
            assert_eq!(router.send(framed).await.expect("reply"), Sent::Queued);
            assert_eq!(
                body(&anonymous.recv().await.expect("reply")),
                vec![b"hello".to_vec()]
            );
        }
        /// Claim: `ZMQ_ROUTER_MANDATORY` is the difference between a message
        /// disappearing and a caller being told. Unroutable is `EHOSTUNREACH`
        /// with it and a reported drop without it; a full queue is `EAGAIN` with
        /// it under `ZMQ_DONTWAIT`.
        #[tokio::test]
        async fn router_mandatory_replaces_the_silent_drop() {
            let ctx = context();
            let mut lenient = RouterSocket::new(&ctx).expect("router");
            let stranger = Multipart::new(vec![
                Message::from("nobody-here"),
                Message::from("into the void"),
            ])
            .expect("frames");
            assert_eq!(
                lenient.send(stranger.clone()).await.expect("dropped"),
                Sent::Dropped,
                "the default is the brutal one"
            );

            let mut strict = RouterSocket::with_options(
                &ctx,
                SocketOptions {
                    router_mandatory: true,
                    ..SocketOptions::default()
                },
            )
            .expect("router");
            let err = strict.send(stranger).await.unwrap_err();
            assert_eq!(err.errno(), "EHOSTUNREACH", "{err}");

            // At the high-water mark, under ZMQ_DONTWAIT, the same option says
            // EAGAIN instead.
            let mut full = RouterSocket::with_options(
                &ctx,
                SocketOptions {
                    router_mandatory: true,
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
            .expect("router");
            let endpoint = full.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut dealer =
                DealerSocket::with_options(&ctx, dealer_options(b"slow")).expect("dealer");
            dealer.connect(&endpoint.to_string()).expect("connect");
            dealer.send("hello").await.expect("send");
            full.recv()
                .await
                .expect("the request, so the peer is known");

            // The queue holds one; the second try_send has nowhere to put its
            // message and says so rather than dropping it.
            let mut refused = 0;
            for _ in 0..8 {
                let framed = Multipart::new(vec![Message::from("slow"), Message::from("x")])
                    .expect("frames");
                if let Err(e) = full.try_send(framed) {
                    assert_eq!(e.errno(), "EAGAIN", "{e}");
                    refused += 1;
                }
            }
            assert!(
                refused > 0,
                "a full queue under mandatory must report EAGAIN"
            );
        }
        /// Claim: two peers claiming one identity are resolved by
        /// `ZMQ_ROUTER_HANDOVER` — rejected by default, and with the option the
        /// newcomer takes the name and the incumbent is disconnected.
        #[tokio::test]
        async fn router_handover_decides_a_duplicate_identity() {
            let ctx = context();

            // Default: the newcomer is rejected, so the incumbent keeps the name.
            let mut strict = RouterSocket::new(&ctx).expect("router");
            let endpoint = strict.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut first = DealerSocket::with_options(
                &ctx,
                SocketOptions {
                    reconnect_ivl: None,
                    ..dealer_options(b"twin")
                },
            )
            .expect("first");
            first.connect(&endpoint.to_string()).expect("connect");
            first.send("i am the incumbent").await.expect("send");
            let request = strict.recv().await.expect("request");
            assert_eq!(body(&request)[0], b"twin".to_vec());

            let second = DealerSocket::with_options(
                &ctx,
                SocketOptions {
                    reconnect_ivl: None,
                    ..dealer_options(b"twin")
                },
            )
            .expect("second");
            second.connect(&endpoint.to_string()).expect("connect");
            wait_for(|| strict.peers().len() == 1).await;
            // One name, one peer: the newcomer was evicted rather than admitted.
            assert_eq!(strict.peers(), vec![RoutingKey::from_wire(b"twin")]);
            let framed = Multipart::new(vec![Message::from("twin"), Message::from("still you")])
                .expect("frames");
            assert_eq!(strict.send(framed).await.expect("reply"), Sent::Queued);
            assert_eq!(
                body(&first.recv().await.expect("reply")),
                vec![b"still you".to_vec()],
                "the incumbent kept the name"
            );

            // With handover the answer is the other one.
            let mut handing_over = RouterSocket::with_options(
                &ctx,
                SocketOptions {
                    router_handover: true,
                    ..SocketOptions::default()
                },
            )
            .expect("router");
            let endpoint = handing_over.bind("tcp://127.0.0.1:0").await.expect("bind");
            let mut incumbent = DealerSocket::with_options(
                &ctx,
                SocketOptions {
                    reconnect_ivl: None,
                    ..dealer_options(b"twin")
                },
            )
            .expect("incumbent");
            incumbent.connect(&endpoint.to_string()).expect("connect");
            incumbent.send("first here").await.expect("send");
            handing_over.recv().await.expect("request");

            let mut newcomer = DealerSocket::with_options(
                &ctx,
                SocketOptions {
                    reconnect_ivl: None,
                    ..dealer_options(b"twin")
                },
            )
            .expect("newcomer");
            newcomer.connect(&endpoint.to_string()).expect("connect");
            newcomer.send("mine now").await.expect("send");
            let request = handing_over.recv().await.expect("the newcomer's request");
            assert_eq!(body(&request)[1], b"mine now".to_vec());

            let framed = Multipart::new(vec![Message::from("twin"), Message::from("for you")])
                .expect("frames");
            assert_eq!(
                handing_over.send(framed).await.expect("reply"),
                Sent::Queued
            );
            assert_eq!(
                body(&newcomer.recv().await.expect("reply")),
                vec![b"for you".to_vec()],
                "the newcomer took the name"
            );
        }
        /// Claim: `ZMQ_PROBE_ROUTER` makes a new connection announce itself, so
        /// the ROUTER can address a peer that has said nothing of its own — the
        /// gap the option exists to close.
        #[tokio::test]
        async fn probe_router_announces_a_peer_before_it_speaks() {
            let ctx = context();
            let mut router = RouterSocket::new(&ctx).expect("router");
            let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

            let mut dealer = DealerSocket::with_options(
                &ctx,
                SocketOptions {
                    probe_router: true,
                    ..dealer_options(b"prober")
                },
            )
            .expect("dealer");
            dealer.connect(&endpoint.to_string()).expect("connect");

            // The application sent nothing. The probe is what arrives, and the
            // application is expected to filter it — an empty body.
            let probe = router.recv().await.expect("the probe");
            assert_eq!(body(&probe)[0], b"prober".to_vec());
            assert_eq!(probe.len(), 2);
            assert!(probe.frames()[1].is_empty(), "the probe carries nothing");

            // And the ROUTER can now speak first, which is the point.
            let framed = Multipart::new(vec![
                Message::from("prober"),
                Message::from("you are known"),
            ])
            .expect("frames");
            assert_eq!(router.send(framed).await.expect("send"), Sent::Queued);
            assert_eq!(
                body(&dealer.recv().await.expect("message")),
                vec![b"you are known".to_vec()]
            );
        }
        /// Claim: the routing table follows the peers rather than growing with
        /// whatever has ever connected — one entry per live peer, and a
        /// departure takes its entry with it. That is what bounds the table at
        /// `max_peers`, and it is the half a stranger could otherwise exploit by
        /// reconnecting under a new identity each time.
        #[tokio::test]
        async fn the_routing_table_follows_the_live_peers() {
            let ctx = context();
            let mut router = RouterSocket::new(&ctx).expect("router");
            let endpoint = router.bind("tcp://127.0.0.1:0").await.expect("bind");

            for name in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
                let mut dealer = DealerSocket::with_options(
                    &ctx,
                    SocketOptions {
                        reconnect_ivl: None,
                        ..dealer_options(name)
                    },
                )
                .expect("dealer");
                dealer.connect(&endpoint.to_string()).expect("connect");
                dealer.send("hello").await.expect("send");
                router.recv().await.expect("request");
                // Each peer is dropped immediately: three identities have been
                // seen, and none of them may still be in the table.
                drop(dealer);
            }

            wait_for(|| router.peers().is_empty()).await;
            assert_eq!(
                router.routing_len(),
                0,
                "both directions of the map must shrink with the peers"
            );
        }
        /// Claim: **a message from a peer the table refuses is not delivered**,
        /// even when it is taken while the ROUTER is already waiting. The first
        /// look the table takes at such a peer is then with its message in hand,
        /// and that look is where a routing id that is not the user's gets the
        /// peer disconnected: what a peer policy turned away sent must not reach
        /// the application under an id nobody gave it, and the receive goes on to
        /// the next message.
        #[tokio::test]
        async fn a_message_from_a_peer_the_table_refuses_is_not_delivered() {
            let ctx = context();
            let router = RouterSocket::with_options(
                &ctx,
                SocketOptions {
                    router_bind_identity_to_user: true,
                    zap_domain: "test".to_string(),
                    reconnect_ivl: None,
                    ..SocketOptions::default()
                },
            )
            .expect("router");
            let engine = router.core.engine().clone();
            let (_send, mut recv) = router.split();
            let waiting = tokio::spawn(async move { recv.recv().await });
            // The receive reaches its wait before either peer exists.
            tokio::task::yield_now().await;

            let nowhere =
                |port: u16| Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
            let alpha = || Some(RoutingId::new(b"alpha".to_vec()).expect("a routing id"));
            // Authorized as "bravo" and announcing the id of "alpha": refused.
            let hijacker = engine.connect(&nowhere(9)).expect("connect");
            engine.assume(hijacker, Some(ZapUserId::new("bravo")), alpha());
            // Authorized as "alpha" and announcing it: the owner.
            let owner = engine.connect(&nowhere(10)).expect("connect");
            engine.assume(owner, Some(ZapUserId::new("alpha")), alpha());
            for (peer, text) in [(hijacker, "forged"), (owner, "hello")] {
                engine
                    .pipe_of(peer)
                    .expect("the peer's pipe")
                    .incoming()
                    .send(Multipart::single(text))
                    .await
                    .expect("queued");
            }

            let heard =
                weida_testkit::within("the owner's message", weida_testkit::EXPECTED, waiting)
                    .await
                    .expect("the receive task")
                    .expect("the owner's message");
            assert_eq!(
                body(&heard),
                vec![b"alpha".to_vec(), b"hello".to_vec()],
                "the first message heard was the refused peer's"
            );
            assert!(
                engine.peer(hijacker).is_none(),
                "the refused peer was disconnected"
            );
        }
    }
}
