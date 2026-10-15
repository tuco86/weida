//! The two halves of a socket whose directions are independent.
//!
//! Five socket types have a send side and a receive side that the
//! specification does not couple — DEALER, ROUTER, PAIR, XPUB and XSUB — so
//! a task parked in `recv` should not make a concurrent `send` on the same
//! socket wait behind it. Each of those types has a `split(self)` that hands
//! back a sending half and a receiving half, usable **at the same time from
//! two tasks**. REQ and REP are deliberately not here: 28/REQREP makes them
//! alternate, and a split would promise an independence the protocol
//! forbids.
//!
//! The halves are the same `SocketCore` twice
//! ([`SocketCore`] is a handle), so nothing is
//! implemented a second time: the socket types and their halves call the
//! same functions in this module, over the `Sync` inside of the core. The
//! engine — the connections, the queues, the peers — stays alive while
//! either half does and closes when the last one goes, exactly as a whole
//! socket closes when it is dropped.
//!
//! What a half is *not*: a way to share a socket between threads. Each half
//! is `Send + !Sync` like the socket it came from, so the thread rule holds
//! per half, and the two halves may live on two tasks and two threads
//! because the state they touch together is the engine's, which is `Sync`,
//! plus the routing table of a ROUTER, which is behind a lock that is never
//! held across an await.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::engine::{Peer, PeerId};
use crate::error::{Error, Result};
use crate::identity::RoutingKey;
use crate::message::{Message, Multipart};
use crate::pipe::{MuteAction, Sent, Weighed};
use crate::pubsub::Published;
use crate::socket::{Shared, SocketCore};
use crate::subscriptions::{self, Subscriptions};
use crate::upstream::{UpstreamTable, Verdict};

// --- the bodies the socket types and their halves share ---------------------

/// A round-robin send that never discards: DEALER's and PAIR's.
pub(crate) async fn send_never_dropping(core: &Shared, message: Multipart) -> Result<()> {
    let limit = core.options().send_timeout;
    let exec = core.exec().clone();
    let delivered = match limit {
        None => core.send_round_robin(message).await?,
        Some(limit) => match exec.within(limit, core.send_round_robin(message)).await {
            Some(result) => result?,
            None => {
                return Err(Error::EAGAIN(
                    format!("no peer took the message within {limit:?} (ZMQ_SNDTIMEO)").into(),
                ));
            }
        },
    };
    if delivered.peer.is_none() {
        return Err(Error::EAGAIN(
            "no peer took the message; this socket type never discards one".into(),
        ));
    }
    Ok(())
}

/// A round-robin send under an explicit bound.
pub(crate) async fn send_within(core: &Shared, message: Multipart, limit: Duration) -> Result<()> {
    let exec = core.exec().clone();
    match exec.within(limit, core.send_round_robin(message)).await {
        Some(result) => result.map(|_| ()),
        None => Err(Error::EAGAIN(
            format!("no peer took the message within {limit:?}").into(),
        )),
    }
}

/// A fair-queued receive bounded by `ZMQ_RCVTIMEO`.
pub(crate) async fn recv_fair(core: &Shared) -> Result<Multipart> {
    let limit = core.options().recv_timeout;
    let exec = core.exec().clone();
    match limit {
        None => core.recv_fair().await.map(|(_, message)| message),
        Some(limit) => match exec.within(limit, core.recv_fair()).await {
            Some(result) => result.map(|(_, message)| message),
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
            )),
        },
    }
}

/// A fair-queued receive under an explicit bound.
pub(crate) async fn recv_fair_within(core: &Shared, limit: Duration) -> Result<Multipart> {
    let exec = core.exec().clone();
    match exec.within(limit, core.recv_fair()).await {
        Some(result) => result.map(|(_, message)| message),
        None => Err(Error::EAGAIN(
            format!("nothing arrived within {limit:?}").into(),
        )),
    }
}

/// A fan-out to every peer with room, dropping for the rest: PUB's and
/// XPUB's publish and XSUB's upstream send, with an optional match on the
/// first frame.
///
/// The message is weighed once and its topic borrowed for the whole loop, so
/// what each peer costs is the copy it is offered and nothing else.
pub(crate) fn fan_out(core: &Shared, message: Multipart, matching: bool) -> Published {
    let weighed = Weighed::new(message);
    let topic = weighed.message().frames()[0].as_slice();
    let mut report = Published {
        delivered: 0,
        dropped: 0,
        unmatched: 0,
    };
    for peer in core.peers().iter() {
        if matching && !peer.subscriptions.matches(topic) {
            report.unmatched += 1;
            continue;
        }
        if peer.pipe.outgoing_ref().offer(weighed.clone()).is_ok() {
            report.delivered += 1;
        } else {
            report.dropped += 1;
            tracing::trace!(
                peer = %peer.id,
                "dropped a copy: the subscriber's queue is full"
            );
        }
    }
    report
}

/// How many subscribers a publisher has: every peer a sender may use right
/// now.
pub(crate) fn subscriber_count(core: &Shared) -> usize {
    core.peers().len()
}

/// Whether any peer's subscriptions would take `topic`.
pub(crate) fn anybody_wants(core: &Shared, topic: &[u8]) -> bool {
    core.peers()
        .iter()
        .any(|peer| peer.subscriptions.matches(topic))
}

/// Puts `message` in the queue of every peer that has room for it, and
/// nothing more: what a SUB or an XSUB says to its publishers about a change
/// to its subscriptions. A publisher that cannot take it now is told at its
/// handshake instead, from the table that travels with the session.
pub(crate) fn tell_every_peer(core: &Shared, message: Multipart) {
    let weighed = Weighed::new(message);
    for peer in core.peers().iter() {
        let _ = peer.pipe.outgoing_ref().offer(weighed.clone());
    }
}

// --- DEALER -------------------------------------------------------------------

/// The sending half of a [`DealerSocket`][crate::DealerSocket].
#[derive(Debug)]
pub struct DealerSend {
    pub(crate) core: SocketCore,
}

/// The receiving half of a [`DealerSocket`][crate::DealerSocket].
#[derive(Debug)]
pub struct DealerRecv {
    pub(crate) core: SocketCore,
}

impl DealerSend {
    /// As [`DealerSocket::send`][crate::DealerSocket::send].
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        send_never_dropping(&self.core, message.into()).await
    }

    /// As [`DealerSocket::try_send`][crate::DealerSocket::try_send].
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// As [`DealerSocket::send_timeout`][crate::DealerSocket::send_timeout].
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        send_within(&self.core, message.into(), limit).await
    }
}

impl DealerRecv {
    /// As [`DealerSocket::recv`][crate::DealerSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        recv_fair(&self.core).await
    }

    /// As [`DealerSocket::try_recv`][crate::DealerSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// As [`DealerSocket::recv_timeout`][crate::DealerSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        recv_fair_within(&self.core, limit).await
    }
}

// --- PAIR ---------------------------------------------------------------------

/// The sending half of a [`PairSocket`][crate::PairSocket].
#[derive(Debug)]
pub struct PairSend {
    pub(crate) core: SocketCore,
}

/// The receiving half of a [`PairSocket`][crate::PairSocket].
#[derive(Debug)]
pub struct PairRecv {
    pub(crate) core: SocketCore,
}

impl PairSend {
    /// As [`PairSocket::send`][crate::PairSocket::send].
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        send_never_dropping(&self.core, message.into()).await
    }

    /// As [`PairSocket::try_send`][crate::PairSocket::try_send].
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<()> {
        self.core.try_send_round_robin(message.into()).map(|_| ())
    }

    /// As [`PairSocket::send_timeout`][crate::PairSocket::send_timeout].
    pub async fn send_timeout(
        &mut self,
        message: impl Into<Multipart>,
        limit: Duration,
    ) -> Result<()> {
        send_within(&self.core, message.into(), limit).await
    }
}

impl PairRecv {
    /// As [`PairSocket::recv`][crate::PairSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        recv_fair(&self.core).await
    }

    /// As [`PairSocket::try_recv`][crate::PairSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// As [`PairSocket::recv_timeout`][crate::PairSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        recv_fair_within(&self.core, limit).await
    }
}

// --- XSUB ---------------------------------------------------------------------

/// The sending half of an [`XSubSocket`][crate::XSubSocket]: subscriptions
/// and upstream messages.
#[derive(Debug)]
pub struct XSubSend {
    pub(crate) core: SocketCore,
    pub(crate) mine: Arc<Subscriptions>,
}

/// The receiving half of an [`XSubSocket`][crate::XSubSocket].
#[derive(Debug)]
pub struct XSubRecv {
    pub(crate) core: SocketCore,
}

impl XSubSend {
    /// As [`XSubSocket::subscribe`][crate::XSubSocket::subscribe].
    pub fn subscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        xsub_subscribe(&self.core, &self.mine, prefix.as_ref())
    }

    /// As [`XSubSocket::unsubscribe`][crate::XSubSocket::unsubscribe].
    pub fn unsubscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        xsub_unsubscribe(&self.core, &self.mine, prefix.as_ref())
    }

    /// As [`XSubSocket::subscriptions`][crate::XSubSocket::subscriptions].
    pub fn subscriptions(&self) -> Vec<Vec<u8>> {
        self.mine.prefixes()
    }

    /// As [`XSubSocket::send`][crate::XSubSocket::send].
    pub fn send(&mut self, message: impl Into<Multipart>) -> Published {
        fan_out(&self.core, message.into(), false)
    }
}

impl XSubRecv {
    /// As [`XSubSocket::recv`][crate::XSubSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        recv_fair(&self.core).await
    }

    /// As [`XSubSocket::try_recv`][crate::XSubSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.core.try_recv_fair().map(|(_, message)| message)
    }

    /// As [`XSubSocket::recv_timeout`][crate::XSubSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        recv_fair_within(&self.core, limit).await
    }
}

pub(crate) fn xsub_subscribe(core: &Shared, mine: &Subscriptions, prefix: &[u8]) -> Result<()> {
    if mine.subscribe(prefix).is_none() {
        return Err(Error::EINVAL(
            format!(
                "this socket already holds its ceiling of {} distinct subscriptions",
                mine.len()
            )
            .into(),
        ));
    }
    xsub_forward(core, subscriptions::write_message_form(true, prefix));
    Ok(())
}

pub(crate) fn xsub_unsubscribe(core: &Shared, mine: &Subscriptions, prefix: &[u8]) -> Result<()> {
    match mine.cancel(prefix) {
        None => Err(Error::EINVAL(
            "this socket is not subscribed to that prefix".into(),
        )),
        Some(_) => {
            xsub_forward(core, subscriptions::write_message_form(false, prefix));
            Ok(())
        }
    }
}

fn xsub_forward(core: &Shared, frame: Vec<u8>) {
    tell_every_peer(core, Multipart::single(Message::from(frame)));
}

// --- XPUB ---------------------------------------------------------------------

/// The publishing half of an [`XPubSocket`][crate::XPubSocket].
#[derive(Debug)]
pub struct XPubPublish {
    pub(crate) core: SocketCore,
}

/// The receiving half of an [`XPubSocket`][crate::XPubSocket]: the
/// subscriptions and messages subscribers send, and the three operations on
/// the subscriber that spoke last — `subscribe`, `unsubscribe`, `refuse` —
/// which belong here because "last" is what this half knows.
#[derive(Debug)]
pub struct XPubRecv {
    pub(crate) core: SocketCore,
    pub(crate) events: XPubEvents,
}

impl XPubPublish {
    /// As [`XPubSocket::publish`][crate::XPubSocket::publish].
    pub fn publish(&mut self, message: impl Into<Multipart>) -> Published {
        fan_out(&self.core, message.into(), true)
    }

    /// As [`XPubSocket::subscriber_count`][crate::XPubSocket::subscriber_count].
    pub fn subscriber_count(&self) -> usize {
        subscriber_count(&self.core)
    }

    /// As [`XPubSocket::anybody_wants`][crate::XPubSocket::anybody_wants].
    pub fn anybody_wants(&self, topic: &[u8]) -> bool {
        anybody_wants(&self.core, topic)
    }
}

impl XPubRecv {
    /// As [`XPubSocket::recv`][crate::XPubSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        self.events.recv(&self.core).await
    }

    /// As [`XPubSocket::try_recv`][crate::XPubSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        self.events.try_recv(&self.core)
    }

    /// As [`XPubSocket::recv_timeout`][crate::XPubSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        self.events.recv_within(&self.core, limit).await
    }

    /// As [`XPubSocket::recv_forwarded`][crate::XPubSocket::recv_forwarded].
    pub async fn recv_forwarded(&mut self) -> Result<Multipart> {
        self.events.recv_forwarded(&self.core).await
    }

    /// As [`XPubSocket::subscribe`][crate::XPubSocket::subscribe].
    pub fn subscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.events.subscribe(&self.core, prefix.as_ref())
    }

    /// As [`XPubSocket::unsubscribe`][crate::XPubSocket::unsubscribe].
    pub fn unsubscribe(&mut self, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.events.unsubscribe(&self.core, prefix.as_ref())
    }

    /// As [`XPubSocket::refuse`][crate::XPubSocket::refuse].
    pub fn refuse(&self, reason: &str) -> Result<()> {
        self.events.refuse(&self.core, reason)
    }
}

/// The receive-side memory of an XPUB: what its application has been told
/// each peer holds, the peers that left and the unsubscribes still owed for
/// them, and who spoke last.
///
/// **The unit of accounting is what the application was handed.** An XPUB's
/// application — a proxy, typically — turns the subscriptions it reads into
/// subscriptions of its own, upstream, so what it is owed when a subscriber
/// vanishes is one unsubscribe for every subscribe it *saw* and has seen no
/// unsubscribe for. A peer's own table is a different record: it can hold
/// prefixes whose notification was dropped because the application was not
/// reading, and an unsubscribe synthesized from the table would cancel,
/// upstream, a subscription the application never made — one that another
/// subscriber's (counted) subscription may be holding. So this keeps a record
/// of its own, updated by the events it hands over, and never reads a table.
///
/// **That is a count per prefix and not a set.** Under `ZMQ_XPUB_VERBOSE` and
/// `ZMQ_XPUB_VERBOSER` the application is handed every subscribe, repeats
/// included, and an upstream XSUB counts each one it forwards: a record that
/// kept the prefix once would owe it one unsubscribe, and the upstream
/// subscription would outlive the subscriber that held it. Each unsubscribe
/// handed over takes one off the count, so a cancel that `ZMQ_XPUB_VERBOSER`
/// hands over while the peer's own table still holds the prefix leaves this
/// record holding it too.
///
/// **Everything here is bounded by the tables' own product.** A peer's record
/// holds at most `max_subscriptions` prefixes of at most
/// `max_subscription_bytes` each, with a count apiece, and the peers that left
/// are turned into unsubscribes one at a time, ahead of any new event: no
/// subscription is handed over while a departed peer is still owed, so the
/// records of the departed and of the live together never outnumber the peers
/// that were live.
///
/// **Read as a forwarding device's end** ([`XPubEvents::recv_forwarded`]), the
/// same record answers a second question: how many distinct prefixes each
/// subscriber has had forwarded. That is its share of the table upstream
/// ([`crate::upstream`]), and a subscribe that would take a subscriber over it
/// is not handed over: the subscriber is told so with an `ERROR`, once. A
/// prefix is handed over for the first subscriber that holds it, and its
/// unsubscribe for the last.
#[derive(Debug, Default)]
pub(crate) struct XPubEvents {
    /// What the application believes each peer holds: for each prefix, the
    /// subscribes it was handed less the unsubscribes it was handed since — or,
    /// under `ZMQ_XPUB_MANUAL`, the ones it applied itself. A peer that holds
    /// nothing has no entry.
    held: HashMap<PeerId, HashMap<Vec<u8>, usize>>,
    /// The peers that left holding something, oldest first, each waiting to be
    /// turned into unsubscribes: one per receive, each prefix as many times as
    /// it was held, "SHALL, if the subscriber peer disconnects prematurely,
    /// generate a suitable unsubscribe request for the calling application."
    departed: VecDeque<Vec<(Vec<u8>, usize)>>,
    /// The peer-set generation the departures were last looked for at.
    /// Looking again is worth a pass over `held` only when the engine has
    /// moved on, and a number keeps none of the peers it was taken from
    /// alive while it waits.
    seen: Option<u64>,
    /// The peer whose subscription was delivered most recently, which is what
    /// `ZMQ_XPUB_MANUAL`'s `subscribe` applies to — libzmq applies it to the
    /// last pipe too.
    last_subscriber: Option<PeerId>,
    /// The prefixes a forwarding device's subscribers hold upstream, once the
    /// socket is read as that device's end. `None` for a socket that is not.
    upstream: Option<UpstreamTable>,
    /// The subscribers refused something upstream and told so, once. A
    /// subscriber that ignores the `ERROR` and goes on asking is not told
    /// again, because every telling is a message queued on its connection for as
    /// long as it does not read. Only live peers are kept.
    told: HashSet<PeerId>,
}

impl XPubEvents {
    pub(crate) async fn recv(&mut self, core: &Shared) -> Result<Multipart> {
        let limit = core.options().recv_timeout;
        let exec = core.exec().clone();
        match limit {
            None => self.next_event(core).await,
            Some(limit) => match exec.within(limit, self.next_event(core)).await {
                Some(result) => result,
                None => Err(Error::EAGAIN(
                    format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
                )),
            },
        }
    }

    pub(crate) fn try_recv(&mut self, core: &Shared) -> Result<Multipart> {
        self.poll_event(core)
            .ok_or_else(|| Error::EAGAIN("nothing is queued from any peer".into()))
    }

    pub(crate) async fn recv_within(
        &mut self,
        core: &Shared,
        limit: Duration,
    ) -> Result<Multipart> {
        let exec = core.exec().clone();
        match exec.within(limit, self.next_event(core)).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?}").into(),
            )),
        }
    }

    /// As [`XPubEvents::recv`], for a socket that is a forwarding device's end:
    /// each prefix is reported once however many subscribers hold it, and no
    /// subscriber has more of the table upstream than its share.
    pub(crate) async fn recv_forwarded(&mut self, core: &Shared) -> Result<Multipart> {
        self.share_upstream(core);
        self.recv(core).await
    }

    /// Makes this socket a forwarding device's end, once.
    ///
    /// A `ZMQ_XPUB_MANUAL` socket stays what it was: what it reports is a
    /// request the application grants with [`XPubEvents::subscribe`], not
    /// something a subscriber holds, so there is nothing to count. Whatever
    /// the record held before is counted, so that its departures are not
    /// lost.
    fn share_upstream(&mut self, core: &Shared) {
        if self.upstream.is_some() || core.options().xpub_manual {
            return;
        }
        let mut table = UpstreamTable::new(core.options().max_subscriptions);
        for prefixes in self.held.values() {
            for prefix in prefixes.keys() {
                table.adopt(prefix);
            }
        }
        self.upstream = Some(table);
    }

    pub(crate) fn subscribe(&mut self, core: &Shared, prefix: &[u8]) -> Result<()> {
        let peer = self.subscriber(core)?;
        if peer.subscriptions.subscribe(prefix).is_none() {
            return Err(Error::EINVAL(
                "that subscriber is at its subscription ceiling".into(),
            ));
        }
        // Applied by the application, so owed an unsubscribe when the
        // subscriber goes: under `ZMQ_XPUB_MANUAL` this is the only thing
        // that makes a peer hold anything.
        *self
            .held
            .entry(peer.id)
            .or_default()
            .entry(prefix.to_vec())
            .or_insert(0) += 1;
        Ok(())
    }

    pub(crate) fn unsubscribe(&mut self, core: &Shared, prefix: &[u8]) -> Result<()> {
        let peer = self.subscriber(core)?;
        match peer.subscriptions.cancel(prefix) {
            Some(_) => {
                self.release(peer.id, prefix);
                Ok(())
            }
            None => Err(Error::EINVAL(
                "that subscriber does not hold that subscription".into(),
            )),
        }
    }

    pub(crate) fn refuse(&self, core: &Shared, reason: &str) -> Result<()> {
        self.subscriber(core)?.pipe.refuse(reason);
        Ok(())
    }

    async fn next_event(&mut self, core: &Shared) -> Result<Multipart> {
        // An event that is already there never creates a bell.
        if let Some(event) = self.poll_event(core) {
            return Ok(event);
        }
        loop {
            // An XPUB must wake on a peer **going away** as well as on a
            // message, because a departure is an event it owes its
            // application — so it parks on the bell that rings for both, and
            // creates it before the look, so that neither can happen between
            // the look and the sleep unnoticed.
            let mut woken = std::pin::pin!(core.readable());
            woken.as_mut().enable();
            if let Some(event) = self.poll_event(core) {
                return Ok(event);
            }
            woken.await;
        }
    }

    /// The next event the application is owed, if there is one: the
    /// unsubscribe for a peer that left, ahead of everything else, and then
    /// what the peers sent.
    fn poll_event(&mut self, core: &Shared) -> Option<Multipart> {
        self.reconcile(core);
        if let Some(unsubscribe) = self.next_departure() {
            return Some(unsubscribe);
        }
        while let Ok((peer, message)) = core.try_recv_fair() {
            if self.admit(core, peer, &message) {
                return Some(message);
            }
        }
        None
    }

    fn subscriber(&self, core: &Shared) -> Result<Peer> {
        self.last_subscriber
            .and_then(|id| core.engine().peer(id))
            .ok_or_else(|| {
                Error::EINVAL(
                    "no subscription has been delivered yet, so there is no subscriber to apply \
                     this to"
                        .into(),
                )
            })
    }

    /// Notes what handing `message` to the application tells it, and says
    /// whether to hand it over at all.
    ///
    /// Only a one-frame message in the subscription form is an event; any
    /// other is something a subscriber sent, and passes. Under
    /// `ZMQ_XPUB_MANUAL` a subscription is a request and not a holding, so
    /// nothing is recorded until the application applies it.
    ///
    /// A subscriber that would hold more than `max_subscriptions` distinct
    /// prefixes in the application's view is disconnected, and its subscribe
    /// is not handed over. The table refuses such a peer itself, so this is
    /// reachable only by cancelling and subscribing again while the
    /// notifications of the cancellations are being dropped, and the record
    /// must not grow with a peer that does that. A repeat of a prefix that is
    /// already held is counted and takes no room under that ceiling.
    fn admit(&mut self, core: &Shared, peer: PeerId, message: &Multipart) -> bool {
        let Some((subscribe, prefix)) = subscription_of(message) else {
            return true;
        };
        self.last_subscriber = Some(peer);
        if core.options().xpub_manual {
            return true;
        }
        if self.upstream.is_some() {
            return if subscribe {
                self.hold_upstream(core, peer, prefix)
            } else {
                self.release_upstream(core, peer, prefix)
            };
        }
        if !subscribe {
            self.release(peer, prefix);
            return true;
        }
        let held = self.held.entry(peer).or_default();
        if let Some(count) = held.get_mut(prefix) {
            *count += 1;
        } else {
            if held.len() >= core.options().max_subscriptions {
                core.engine().evict(peer);
                return false;
            }
            held.insert(prefix.to_vec(), 1);
        }
        true
    }

    /// A subscribe from `peer` for a socket that is a forwarding device's end:
    /// whether it is handed over, which is when `peer` is the first to hold the
    /// prefix. A prefix the peer holds already is a repeat — `ZMQ_XPUB_VERBOSE`
    /// hands those over — and is no second holder; one that would take the peer
    /// over its share, or fill the table, is refused to it.
    fn hold_upstream(&mut self, core: &Shared, peer: PeerId, prefix: &[u8]) -> bool {
        let Some(table) = self.upstream.as_mut() else {
            return true;
        };
        let held = self.held.entry(peer).or_default();
        if let Some(count) = held.get_mut(prefix) {
            *count += 1;
            return false;
        }
        let verdict = table.hold(held.len(), prefix);
        match verdict {
            Verdict::Forward | Verdict::Absorb => {
                held.insert(prefix.to_vec(), 1);
                verdict == Verdict::Forward
            }
            Verdict::OverShare | Verdict::TableFull => {
                if held.is_empty() {
                    self.held.remove(&peer);
                }
                self.refuse_upstream(core, peer, verdict);
                false
            }
        }
    }

    /// An unsubscribe from `peer` for a socket that is a forwarding device's
    /// end: whether it is handed over, which is when `peer` was the last to
    /// hold the prefix.
    ///
    /// The cancel that is handed over is the one that emptied the peer's own
    /// table, so it ends the peer's holding whatever count the record has —
    /// except under `ZMQ_XPUB_VERBOSER`, which hands over every cancel and so
    /// counts them off. A cancel for something the peer was never forwarded
    /// for, a refused subscription included, is nobody's to pass on.
    fn release_upstream(&mut self, core: &Shared, peer: PeerId, prefix: &[u8]) -> bool {
        let verboser = core.options().xpub_verboser;
        let Some(held) = self.held.get_mut(&peer) else {
            return false;
        };
        let gone = match held.get_mut(prefix) {
            None => return false,
            Some(count) if verboser && *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => {
                held.remove(prefix);
                true
            }
        };
        if held.is_empty() {
            self.held.remove(&peer);
        }
        gone && self
            .upstream
            .as_mut()
            .is_some_and(|table| table.release(prefix))
    }

    /// Tells `peer`, once, why a subscription of its was not forwarded: the
    /// `ERROR` [`XPubEvents::refuse`] sends. 37/ZMTP has the peer treat it as
    /// fatal, so a subscriber that heeds it ends its own connection, and its
    /// holdings go upstream as cancels when it does.
    fn refuse_upstream(&mut self, core: &Shared, peer: PeerId, verdict: Verdict) {
        tracing::debug!(%peer, ?verdict, "did not forward a subscription");
        if !self.told.insert(peer) {
            return;
        }
        let reason = match (verdict, self.upstream.as_ref()) {
            (Verdict::OverShare, Some(table)) => format!(
                "this subscriber already holds its share of the proxy's upstream \
                 subscriptions, {} prefixes",
                table.share()
            ),
            _ => "the proxy's upstream subscriptions are full".to_owned(),
        };
        if let Some(pipe) = core.pipe_of(peer) {
            pipe.refuse(&reason);
        }
    }

    /// Takes one off what the application holds of `prefix` for `peer`: the
    /// prefix is forgotten when none is left, and the peer when it holds
    /// nothing.
    fn release(&mut self, peer: PeerId, prefix: &[u8]) {
        let Some(held) = self.held.get_mut(&peer) else {
            return;
        };
        if let Some(count) = held.get_mut(prefix) {
            // An entry exists only while it is nonzero.
            *count -= 1;
            if *count == 0 {
                held.remove(prefix);
            }
        }
        if held.is_empty() {
            self.held.remove(&peer);
        }
    }

    /// Moves every peer that has left from `held` to `departed`, when the
    /// peer set has changed since the last look — and does nothing otherwise,
    /// so a receive costs one atomic load and not a pass over every peer's
    /// record.
    fn reconcile(&mut self, core: &Shared) {
        let engine = core.engine();
        // Read before the snapshot: a change between the two costs one more
        // pass, and never a missed one.
        let generation = engine.generation();
        if self.seen == Some(generation) {
            return;
        }
        let live = engine.peers();
        // The snapshot is sorted by id, so "is it still here" is a binary
        // search and the pass is N log N, not N².
        let is_live = |id: &PeerId| live.binary_search_by_key(id, |peer| peer.id).is_ok();
        let gone: Vec<PeerId> = self
            .held
            .keys()
            .copied()
            .filter(|id| !is_live(id))
            .collect();
        for id in gone {
            if let Some(prefixes) = self.held.remove(&id) {
                self.departed.push_back(prefixes.into_iter().collect());
            }
        }
        if self.last_subscriber.is_some_and(|id| !is_live(&id)) {
            self.last_subscriber = None;
        }
        // A peer refused everything holds nothing, so the pass over `held`
        // does not find it.
        self.told.retain(is_live);
        self.seen = Some(generation);
    }

    /// The next unsubscribe owed for a peer that left, one at a time:
    /// generated when it is handed over, not queued whole, so that a peer
    /// which held a thousand prefixes does not become a thousand messages in
    /// memory at once.
    fn next_departure(&mut self) -> Option<Multipart> {
        if self.upstream.is_some() {
            return self.next_cancel_upstream();
        }
        let owed = self.departed.front_mut()?;
        let (prefix, count) = owed.last_mut()?;
        let notice = Multipart::single(Message::from(subscriptions::write_message_form(
            false, prefix,
        )));
        // An entry exists only while it is nonzero.
        *count -= 1;
        if *count == 0 {
            owed.pop();
        }
        if owed.is_empty() {
            self.departed.pop_front();
        }
        Some(notice)
    }

    /// The next cancel a forwarding device's end owes upstream for a peer that
    /// left. The peer held each prefix once as far as the table is concerned,
    /// however often it subscribed, and a prefix another subscriber holds is
    /// not cancelled: the cancels are the ones the table says were the last.
    fn next_cancel_upstream(&mut self) -> Option<Multipart> {
        let table = self.upstream.as_mut()?;
        while let Some(owed) = self.departed.front_mut() {
            let Some((prefix, _)) = owed.pop() else {
                self.departed.pop_front();
                continue;
            };
            if owed.is_empty() {
                self.departed.pop_front();
            }
            if table.release(&prefix) {
                return Some(Multipart::single(Message::from(
                    subscriptions::write_message_form(false, &prefix),
                )));
            }
        }
        None
    }
}

/// A one-frame message in the subscription form, read: `true` for a
/// subscribe, and the prefix.
pub(crate) fn subscription_of(message: &Multipart) -> Option<(bool, &[u8])> {
    if message.len() != 1 {
        return None;
    }
    subscriptions::read_message_form(message.frames()[0].as_slice())
}

// --- ROUTER -------------------------------------------------------------------

/// The sending half of a [`RouterSocket`][crate::RouterSocket].
#[derive(Debug)]
pub struct RouterSend {
    pub(crate) core: SocketCore,
    pub(crate) routing: Arc<Mutex<Routing>>,
}

/// The receiving half of a [`RouterSocket`][crate::RouterSocket].
#[derive(Debug)]
pub struct RouterRecv {
    pub(crate) core: SocketCore,
    pub(crate) routing: Arc<Mutex<Routing>>,
}

impl RouterSend {
    /// As [`RouterSocket::send`][crate::RouterSocket::send].
    pub async fn send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        router_send(&self.core, &self.routing, message.into()).await
    }

    /// As [`RouterSocket::try_send`][crate::RouterSocket::try_send].
    pub fn try_send(&mut self, message: impl Into<Multipart>) -> Result<Sent> {
        router_try_send(&self.core, &self.routing, message.into())
    }

    /// As [`RouterSocket::peers`][crate::RouterSocket::peers].
    pub fn peers(&mut self) -> Vec<RoutingKey> {
        lock(&self.routing).keys(&self.core)
    }
}

impl RouterRecv {
    /// As [`RouterSocket::recv`][crate::RouterSocket::recv].
    pub async fn recv(&mut self) -> Result<Multipart> {
        router_recv(&self.core, &self.routing).await
    }

    /// As [`RouterSocket::try_recv`][crate::RouterSocket::try_recv].
    pub fn try_recv(&mut self) -> Result<Multipart> {
        router_try_recv(&self.core, &self.routing)
    }

    /// As [`RouterSocket::recv_timeout`][crate::RouterSocket::recv_timeout].
    pub async fn recv_timeout(&mut self, limit: Duration) -> Result<Multipart> {
        router_recv_within(&self.core, &self.routing, limit).await
    }

    /// As [`RouterSocket::peers`][crate::RouterSocket::peers].
    pub fn peers(&mut self) -> Vec<RoutingKey> {
        lock(&self.routing).keys(&self.core)
    }
}

/// A ROUTER's routing table: routing id to peer, and back.
///
/// Behind a `Mutex` only so that the two halves of a split ROUTER may hold
/// it together; the lock is taken for a reconciliation or a lookup and
/// never across an await. Bounded as the socket's doc says: one entry per
/// live peer, reconciled against the engine's peer snapshot — and **only
/// when that snapshot has changed**. A table that re-read every peer on every
/// message would cost the whole peer count per message, and a stranger
/// chooses the count.
#[derive(Debug)]
pub(crate) struct Routing {
    /// Two maps because both directions are hot: inbound needs the id of a
    /// peer, outbound the peer of an id.
    by_key: HashMap<RoutingKey, PeerId>,
    /// The id as the frame an inbound message carries in front of its own,
    /// built once when the peer is learned: a receive hands out a copy of it
    /// instead of rebuilding the frame from a key every time.
    by_peer: HashMap<PeerId, Message>,
    /// The counter behind a generated routing id; see [`RoutingKey`].
    next_generated: u32,
    /// The peer-set generation this table was last reconciled at. The same
    /// number again means no peer arrived, left or announced since.
    reconciled: Option<u64>,
}

/// Lets the table be searched by the bytes a message carries, without
/// building a key — an allocation per outbound message — for the lookup. A
/// `RoutingKey` hashes and compares as its bytes do, which is what `Borrow`
/// requires.
impl std::borrow::Borrow<[u8]> for RoutingKey {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Routing {
    pub(crate) fn new() -> Routing {
        Routing {
            by_key: HashMap::new(),
            by_peer: HashMap::new(),
            next_generated: 1,
            reconciled: None,
        }
    }

    /// Entries in the table, both maps kept in step, for the test that pins
    /// its shrinking half.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        debug_assert_eq!(self.by_key.len(), self.by_peer.len());
        self.by_key.len()
    }

    pub(crate) fn keys(&mut self, core: &Shared) -> Vec<RoutingKey> {
        self.learn_peers(core);
        let mut keys: Vec<RoutingKey> = self.by_key.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub(crate) fn peer_of(&self, key: &[u8]) -> Option<PeerId> {
        self.by_key.get(key).copied()
    }

    /// Gives every current peer a routing id, and forgets the ones that left.
    ///
    /// This is also where a duplicate identity is resolved:
    /// `ZMQ_ROUTER_HANDOVER` decides whether the newcomer takes the name and
    /// the incumbent is disconnected, or the newcomer is rejected — libzmq's
    /// default being to reject it. With `router_bind_identity_to_user` an
    /// identity is also refused outright unless it is the user id the ZAP
    /// handler named for the connection, so that nobody authorized as one
    /// user can squat or take over the name of another.
    ///
    /// Does nothing when the peer set has not changed since the table was last
    /// reconciled.
    pub(crate) fn learn_peers(&mut self, core: &Shared) {
        let engine = core.engine();
        // Read before the snapshot: a change between the two costs one more
        // pass, and never a missed one.
        let generation = engine.generation();
        if self.reconciled == Some(generation) {
            return;
        }
        let live = engine.peers();
        // The snapshot is sorted by id, so "is it still here" is a binary
        // search and the pass is N log N, not N².
        let gone: Vec<PeerId> = self
            .by_peer
            .keys()
            .copied()
            .filter(|id| live.binary_search_by_key(id, |peer| peer.id).is_err())
            .collect();
        for id in gone {
            if let Some(frame) = self.by_peer.remove(&id) {
                self.by_key.remove(frame.as_slice());
            }
        }
        for peer in live.iter() {
            if self.by_peer.contains_key(&peer.id) {
                continue;
            }
            if !peer.announced {
                // Its READY has not been read yet, so its own identity is
                // not known: keying it now would key it by a generated id
                // for the rest of its life.
                continue;
            }
            if core.options().router_bind_identity_to_user
                && let Some(identity) = &peer.identity
                && peer.user_id.as_ref().map(|user| user.as_str().as_bytes())
                    != Some(identity.as_bytes())
            {
                tracing::debug!(
                    peer = %peer.id,
                    "refused a routing id that is not the authenticated user's \
                     (router_bind_identity_to_user)"
                );
                engine.evict(peer.id);
                continue;
            }
            let key = match &peer.identity {
                Some(identity) => RoutingKey::announced(identity),
                None => self.next_key(),
            };
            if let Some(incumbent) = self.by_key.get(&key).copied() {
                if core.options().router_handover {
                    tracing::debug!(
                        key = ?key,
                        %incumbent,
                        "handing a routing id over to a newcomer (ZMQ_ROUTER_HANDOVER)"
                    );
                    engine.evict(incumbent);
                    self.by_peer.remove(&incumbent);
                } else {
                    // Peer-chosen and repeatable at the peer's reconnect
                    // rate, so never louder than a debug line.
                    tracing::debug!(
                        key = ?key,
                        %incumbent,
                        "rejected a peer claiming a routing id already in use"
                    );
                    engine.evict(peer.id);
                    continue;
                }
            }
            let frame = Message::from(key.as_bytes());
            self.by_key.insert(key, peer.id);
            self.by_peer.insert(peer.id, frame);
        }
        self.reconciled = Some(generation);
    }

    /// This peer's routing id as a frame, learning it if the handshake has
    /// completed since the last reconciliation — which it usually has,
    /// because a message from a peer is proof that its `READY` was read.
    ///
    /// `None` for a peer the engine no longer has once the table has looked
    /// at it: the look is where a routing id that is not its user's, or one
    /// already taken, gets the peer disconnected, so a peer with no entry
    /// then is one policy turned away, or one that left. Nothing it sent is
    /// to be delivered under an id nobody gave it. A live peer the table has
    /// not keyed — one whose handshake has not been seen to finish — is given
    /// a generated id.
    pub(crate) fn frame_for(&mut self, core: &Shared, peer: PeerId) -> Option<Message> {
        if let Some(frame) = self.by_peer.get(&peer) {
            return Some(frame.clone());
        }
        self.learn_peers(core);
        if let Some(frame) = self.by_peer.get(&peer) {
            return Some(frame.clone());
        }
        core.pipe_of(peer)?;
        let key = self.next_key();
        let frame = Message::from(key.as_bytes());
        self.by_key.insert(key, peer);
        self.by_peer.insert(peer, frame.clone());
        Some(frame)
    }

    fn next_key(&mut self) -> RoutingKey {
        let key = RoutingKey::generated(self.next_generated);
        self.next_generated = self.next_generated.wrapping_add(1).max(1);
        key
    }
}

pub(crate) fn lock(routing: &Mutex<Routing>) -> std::sync::MutexGuard<'_, Routing> {
    routing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) async fn router_recv(core: &Shared, routing: &Mutex<Routing>) -> Result<Multipart> {
    let limit = core.options().recv_timeout;
    let exec = core.exec().clone();
    match limit {
        None => router_await(core, routing).await,
        Some(limit) => match exec.within(limit, router_await(core, routing)).await {
            Some(result) => result,
            None => Err(Error::EAGAIN(
                format!("nothing arrived within {limit:?} (ZMQ_RCVTIMEO)").into(),
            )),
        },
    }
}

pub(crate) async fn router_recv_within(
    core: &Shared,
    routing: &Mutex<Routing>,
    limit: Duration,
) -> Result<Multipart> {
    let exec = core.exec().clone();
    match exec.within(limit, router_await(core, routing)).await {
        Some(result) => result,
        None => Err(Error::EAGAIN(
            format!("nothing arrived within {limit:?}").into(),
        )),
    }
}

pub(crate) fn router_try_recv(core: &Shared, routing: &Mutex<Routing>) -> Result<Multipart> {
    lock(routing).learn_peers(core);
    loop {
        let (peer, message) = core.try_recv_fair()?;
        if let Some(message) = with_routing_id(core, routing, peer, message) {
            return Ok(message);
        }
    }
}

async fn router_await(core: &Shared, routing: &Mutex<Routing>) -> Result<Multipart> {
    lock(routing).learn_peers(core);
    loop {
        let (peer, message) = core.recv_fair().await?;
        if let Some(message) = with_routing_id(core, routing, peer, message) {
            return Ok(message);
        }
    }
}

/// `message` with its sender's routing id in front, or `None` when the table
/// has no id for the sender ([`Routing::frame_for`]) and the message is
/// dropped.
fn with_routing_id(
    core: &Shared,
    routing: &Mutex<Routing>,
    peer: PeerId,
    message: Multipart,
) -> Option<Multipart> {
    let Some(id) = lock(routing).frame_for(core, peer) else {
        tracing::debug!(%peer, "dropped a message from a peer the routing table does not hold");
        return None;
    };
    let mut frames = message.into_frames();
    frames.insert(0, id);
    Some(Multipart::new(frames).expect("a routing id plus at least one frame"))
}

pub(crate) async fn router_send(
    core: &Shared,
    routing: &Mutex<Routing>,
    message: Multipart,
) -> Result<Sent> {
    let (id, body) = split_routing_id(message)?;
    let mandatory = core.options().router_mandatory;
    let peer = {
        let mut table = lock(routing);
        table.learn_peers(core);
        table.peer_of(id.as_slice())
    };
    let Some(peer) = peer else {
        return unroutable(id.as_slice(), mandatory);
    };
    let Some(pipe) = core.pipe_of(peer) else {
        return unroutable(id.as_slice(), mandatory);
    };
    let queue = pipe.outgoing_ref();
    // The queue decides once, against this message's real size: a message too
    // large for the room left is refused here exactly as a full queue is,
    // not as an error the caller was never promised.
    let body = match queue.offer(Weighed::new(body)) {
        Ok(()) => return Ok(Sent::Queued),
        Err(refused) => refused.into_message(),
    };
    if !mandatory {
        // "0 discards silently when it cannot be routed **or the peer's
        // SNDHWM is reached**."
        tracing::debug!(
            key = ?RoutingKey::from_wire(id.as_slice()),
            "dropped a message: the peer's queue is full"
        );
        return Ok(Sent::Dropped);
    }
    // ZMQ_ROUTER_MANDATORY without ZMQ_DONTWAIT: wait for room for this
    // message, bounded by ZMQ_SNDTIMEO, and report the peer going away.
    let limit = core.options().send_timeout;
    crate::socket::within(core.exec(), limit, queue.send_with(body, MuteAction::Block)).await
}

pub(crate) fn router_try_send(
    core: &Shared,
    routing: &Mutex<Routing>,
    message: Multipart,
) -> Result<Sent> {
    let (id, body) = split_routing_id(message)?;
    let mandatory = core.options().router_mandatory;
    let peer = {
        let mut table = lock(routing);
        table.learn_peers(core);
        table.peer_of(id.as_slice())
    };
    let Some(peer) = peer else {
        return unroutable(id.as_slice(), mandatory);
    };
    let Some(pipe) = core.pipe_of(peer) else {
        return unroutable(id.as_slice(), mandatory);
    };
    if pipe.outgoing_ref().offer(Weighed::new(body)).is_ok() {
        return Ok(Sent::Queued);
    }
    if mandatory {
        Err(Error::EAGAIN(
            "the peer's queue is full (ZMQ_ROUTER_MANDATORY)".into(),
        ))
    } else {
        Ok(Sent::Dropped)
    }
}

fn unroutable(id: &[u8], mandatory: bool) -> Result<Sent> {
    let key = RoutingKey::from_wire(id);
    if mandatory {
        Err(Error::EHOSTUNREACH(
            format!("no peer holds the routing id {key:?} (ZMQ_ROUTER_MANDATORY)").into(),
        ))
    } else {
        // "ROUTER sockets do have a somewhat brutal way of dealing with
        // messages they can't send anywhere: they drop them silently."
        tracing::debug!(key = ?key, "dropped a message for a routing id nobody holds");
        Ok(Sent::Dropped)
    }
}

/// Takes the routing id off the front of an outbound message.
///
/// `EINVAL` when nothing would be left: a ROUTER strips that frame, and a
/// message that is only its address is no message.
pub(crate) fn split_routing_id(message: Multipart) -> Result<(Message, Multipart)> {
    let mut frames = message.into_frames();
    if frames.len() < 2 {
        return Err(Error::EINVAL(
            "a ROUTER message is a routing id followed by the message; this one has only the \
             routing id"
                .into(),
        ));
    }
    let id = frames.remove(0);
    let body = Multipart::new(frames).expect("at least one frame remains");
    Ok((id, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{Context, ContextConfig};
    use crate::options::SocketOptions;
    use weida_zmtp::SocketType;

    fn xpub(options: SocketOptions) -> SocketCore {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        SocketCore::new(&ctx, SocketType::XPub, options).expect("core")
    }

    fn subscribe(prefix: &[u8]) -> Multipart {
        Multipart::single(subscriptions::write_message_form(true, prefix))
    }

    fn cancel(prefix: &[u8]) -> Multipart {
        Multipart::single(subscriptions::write_message_form(false, prefix))
    }

    /// Claim: **what the application is owed for one peer is bounded by the
    /// table's own ceiling.** The record follows the events the application is
    /// handed, so a subscriber that cancels and subscribes anew while the
    /// notifications of its cancellations are being dropped could otherwise
    /// grow it without end; past `max_subscriptions` distinct prefixes the
    /// peer is disconnected and the subscribe that would have crossed the
    /// line is not handed over, so the application never holds one the
    /// record does not.
    #[tokio::test]
    async fn the_record_of_one_peer_stops_at_the_table_ceiling() {
        let core = xpub(SocketOptions {
            max_subscriptions: 2,
            ..SocketOptions::default()
        });
        let peer = PeerId::detached();
        let mut events = XPubEvents::default();

        assert!(events.admit(&core, peer, &subscribe(b"a")));
        assert!(events.admit(&core, peer, &subscribe(b"b")));
        assert!(
            events.admit(&core, peer, &subscribe(b"a")),
            "a repeat takes no room"
        );
        assert!(
            !events.admit(&core, peer, &subscribe(b"c")),
            "a third distinct prefix is not handed over"
        );
        assert!(events.admit(&core, peer, &cancel(b"a")));
        assert!(
            !events.admit(&core, peer, &subscribe(b"c")),
            "the repeat was handed over too, so one cancel leaves the prefix held"
        );
        assert!(events.admit(&core, peer, &cancel(b"a")));
        assert!(
            events.admit(&core, peer, &subscribe(b"c")),
            "there is room again once the last of it was cancelled"
        );
        assert!(
            events.admit(&core, peer, &Multipart::single("hello")),
            "a message that is no subscription passes untouched"
        );
        assert_eq!(events.held[&peer].len(), 2);
    }

    /// Claim: **a peer that left is turned into unsubscribes one prefix at a
    /// time**, from a single record, and never as a queue of messages built
    /// all at once: a subscriber that held a thousand prefixes and went must
    /// not become a thousand messages in memory because its application is
    /// slow to read. Each is `%x00` and the prefix, and the record is gone
    /// when the last one is handed over.
    #[tokio::test]
    async fn a_departed_peer_is_one_record_until_it_is_handed_over() {
        let core = xpub(SocketOptions::default());
        let mut events = XPubEvents::default();
        // The engine has no such peer, which is what having left looks like.
        let gone = PeerId::detached();
        for prefix in [b"a".as_slice(), b"b", b"c"] {
            assert!(events.admit(&core, gone, &subscribe(prefix)));
        }

        events.reconcile(&core);
        assert!(events.held.is_empty());
        assert_eq!(
            events.departed.len(),
            1,
            "one record for the peer, not a message per prefix"
        );

        let mut owed = Vec::new();
        while let Some(message) = events.next_departure() {
            owed.push(message.frames()[0].as_slice().to_vec());
        }
        owed.sort();
        assert_eq!(
            owed,
            [b"\x00a".to_vec(), b"\x00b".to_vec(), b"\x00c".to_vec()]
        );
        assert!(events.departed.is_empty());
    }

    /// Claim: **a departed peer is owed one unsubscribe for every subscribe the
    /// application was handed and has seen no unsubscribe for**, repeats
    /// included. Under `ZMQ_XPUB_VERBOSE` every subscribe is handed over, and a
    /// proxy that forwards each one to an upstream XSUB — which counts them —
    /// holds that many subscriptions for the peer: owed a single unsubscribe,
    /// it would keep the upstream subscription for a topic nobody holds.
    #[tokio::test]
    async fn a_departed_peer_is_owed_one_unsubscribe_per_subscribe_handed_over() {
        let core = xpub(SocketOptions {
            xpub_verbose: true,
            ..SocketOptions::default()
        });
        let mut events = XPubEvents::default();
        let gone = PeerId::detached();
        for prefix in [b"a".as_slice(), b"a", b"a", b"b"] {
            assert!(events.admit(&core, gone, &subscribe(prefix)));
        }

        events.reconcile(&core);
        let mut owed = Vec::new();
        while let Some(message) = events.next_departure() {
            owed.push(message.frames()[0].as_slice().to_vec());
        }
        owed.sort();
        assert_eq!(
            owed,
            [
                b"\x00a".to_vec(),
                b"\x00a".to_vec(),
                b"\x00a".to_vec(),
                b"\x00b".to_vec()
            ]
        );
        assert!(events.departed.is_empty());
    }

    /// Claim: **an unsubscribe the application was handed takes one off what
    /// the peer holds, and no more.** `ZMQ_XPUB_VERBOSER` hands over a cancel
    /// that left the peer's own table holding the prefix; a record that forgot
    /// the prefix on it would owe nothing at the peer's departure, and the
    /// subscribe the application has seen no unsubscribe for would stay
    /// forwarded upstream for good.
    #[tokio::test]
    async fn an_unsubscribe_handed_over_takes_one_off_what_a_departed_peer_is_owed() {
        let core = xpub(SocketOptions {
            xpub_verboser: true,
            ..SocketOptions::default()
        });
        let mut events = XPubEvents::default();
        let gone = PeerId::detached();
        for message in [subscribe(b"a"), subscribe(b"a"), cancel(b"a")] {
            assert!(events.admit(&core, gone, &message));
        }

        events.reconcile(&core);
        let owed = events
            .next_departure()
            .expect("the subscribe no unsubscribe has answered");
        assert_eq!(owed.frames()[0].as_slice(), b"\x00a");
        assert!(events.next_departure().is_none());
    }

    /// A dialled peer that never connects, which has an id the engine knows
    /// until it is disconnected, and the endpoint that disconnects it.
    fn a_peer(core: &SocketCore, port: u16) -> (PeerId, crate::endpoint::Endpoint) {
        let endpoint =
            crate::endpoint::Endpoint::parse(&format!("tcp://127.0.0.1:{port}")).expect("endpoint");
        let id = core.engine().connect(&endpoint).expect("connect");
        (id, endpoint)
    }

    /// An XPUB read as a forwarding device's end, without ever connecting
    /// anybody: the peers are the engine's own dialled ones.
    fn a_device_end(options: SocketOptions) -> (SocketCore, XPubEvents) {
        let core = xpub(SocketOptions {
            reconnect_ivl: None,
            ..options
        });
        let mut events = XPubEvents::default();
        events.share_upstream(&core);
        assert!(events.upstream.is_some());
        (core, events)
    }

    /// Everything a departed peer is owed, as the bytes the application reads.
    fn departures(events: &mut XPubEvents) -> Vec<Vec<u8>> {
        let mut owed = Vec::new();
        while let Some(message) = events.next_departure() {
            owed.push(message.frames()[0].as_slice().to_vec());
        }
        owed.sort();
        owed
    }

    /// Claim: **a prefix is handed to a forwarding device for its first
    /// holder and its cancel for its last**, however many subscribers hold it
    /// in between. What the device forwards is then one subscription for the
    /// prefix, which is what an upstream that keeps a set of connections per
    /// prefix can hold, and the first subscriber to leave does not cancel it
    /// for the rest.
    #[tokio::test]
    async fn a_prefix_is_handed_over_for_its_first_holder_and_cancelled_with_its_last() {
        let (core, mut events) = a_device_end(SocketOptions::default());
        let (first, _) = a_peer(&core, 9);
        let (second, _) = a_peer(&core, 19);

        assert!(events.admit(&core, first, &subscribe(b"news")));
        assert!(
            !events.admit(&core, second, &subscribe(b"news")),
            "the second holder of a prefix adds nothing to forward"
        );
        assert!(
            !events.admit(&core, first, &cancel(b"news")),
            "one of two holders letting go cancels nothing"
        );
        assert!(
            events.admit(&core, second, &cancel(b"news")),
            "the last holder letting go cancels it"
        );
        assert!(
            !events.admit(&core, second, &cancel(b"news")),
            "a cancel for a prefix nobody holds is not passed on"
        );
        assert!(
            events.admit(&core, second, &Multipart::single("hello")),
            "a message that is no subscription passes untouched"
        );
        assert!(events.held.is_empty(), "nobody holds anything any more");
    }

    /// Claim: **a subscriber that leaves cancels only what nobody else holds.**
    /// A prefix it shared stays forwarded for the one that is left, and is
    /// cancelled when that one goes too.
    #[tokio::test]
    async fn a_departed_subscriber_cancels_only_what_nobody_else_holds() {
        let (core, mut events) = a_device_end(SocketOptions::default());
        let (first, first_at) = a_peer(&core, 9);
        let (second, second_at) = a_peer(&core, 19);
        assert!(events.admit(&core, first, &subscribe(b"a")));
        assert!(events.admit(&core, first, &subscribe(b"shared")));
        assert!(!events.admit(&core, second, &subscribe(b"shared")));
        assert!(events.admit(&core, second, &subscribe(b"b")));

        core.engine().disconnect(&first_at).expect("disconnect");
        events.reconcile(&core);
        assert_eq!(
            departures(&mut events),
            [b"\x00a".to_vec()],
            "the prefix only the first held"
        );

        core.engine().disconnect(&second_at).expect("disconnect");
        events.reconcile(&core);
        assert_eq!(
            departures(&mut events),
            [b"\x00b".to_vec(), b"\x00shared".to_vec()],
            "and the second one's, shared prefix included now that it is the last"
        );
        assert!(events.departed.is_empty() && events.held.is_empty());
    }

    /// Claim: **no subscriber has more than its share of the table
    /// forwarded**: the prefixes past it are not handed over, the subscriber
    /// is told why with an `ERROR`, once however much more it asks for, and a
    /// cancel for what was refused is not passed on. Another subscriber is
    /// not held to what the first one asked for.
    #[tokio::test]
    async fn a_subscriber_over_its_share_is_told_once_and_not_forwarded() {
        let (core, mut events) = a_device_end(SocketOptions {
            max_subscriptions: 64,
            ..SocketOptions::default()
        });
        let share = crate::upstream::upstream_share(64);
        let (greedy, greedy_at) = a_peer(&core, 9);
        let (honest, _) = a_peer(&core, 19);

        for index in 0..share {
            let prefix = format!("junk-{index}");
            assert!(
                events.admit(&core, greedy, &subscribe(prefix.as_bytes())),
                "{prefix} is within the share"
            );
        }
        for extra in ["one-too-many", "and-another", "and-a-third"] {
            assert!(
                !events.admit(&core, greedy, &subscribe(extra.as_bytes())),
                "{extra} is past the share"
            );
        }
        assert!(
            !events.admit(&core, greedy, &cancel(b"one-too-many")),
            "nothing was forwarded for it, so there is nothing to cancel"
        );
        assert_eq!(
            events.held[&greedy].len(),
            share,
            "the refused are not kept"
        );

        let pipe = core.pipe_of(greedy).expect("the subscriber's pipe");
        let reason = tokio::time::timeout(Duration::from_secs(10), pipe.refusal())
            .await
            .expect("the subscriber was told");
        assert!(reason.contains("share"), "{reason}");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), pipe.refusal())
                .await
                .is_err(),
            "and told once, not for every subscription it went on to send"
        );

        assert!(
            events.admit(&core, honest, &subscribe(b"news")),
            "another subscriber has its own share"
        );

        core.engine().disconnect(&greedy_at).expect("disconnect");
        events.reconcile(&core);
        assert!(
            events.told.is_empty(),
            "a subscriber that left is forgotten"
        );
        assert_eq!(departures(&mut events).len(), share);
    }

    /// Claim: **the table upstream is not overfilled by many subscribers each
    /// within its share.** A prefix that would be new there is refused to
    /// whoever asks once it is full, and one that is in it already is still
    /// welcome, because it takes no room.
    #[tokio::test]
    async fn a_full_upstream_table_refuses_a_new_prefix_and_not_a_held_one() {
        // A table of 16 gives each subscriber 8.
        let (core, mut events) = a_device_end(SocketOptions {
            max_subscriptions: 16,
            ..SocketOptions::default()
        });
        let [(first, _), (second, _), (third, _)] =
            [a_peer(&core, 9), a_peer(&core, 19), a_peer(&core, 29)];
        for (peer, name) in [(first, "a"), (second, "b")] {
            for index in 0..8 {
                let prefix = format!("{name}-{index}");
                assert!(events.admit(&core, peer, &subscribe(prefix.as_bytes())));
            }
        }

        assert!(
            !events.admit(&core, third, &subscribe(b"new")),
            "the table is full of other prefixes"
        );
        assert!(
            !events.admit(&core, third, &subscribe(b"a-0")),
            "a held prefix is nothing to forward, but it is the third holder's now"
        );
        assert_eq!(events.held[&third].len(), 1);
        assert!(
            events.admit(&core, first, &cancel(b"a-1")),
            "a prefix cancelled makes room again"
        );
        assert!(events.admit(&core, third, &subscribe(b"new")));
    }

    /// Claim: **a repeat is no second holder, and the cancel that follows ends
    /// the holding**, under `ZMQ_XPUB_VERBOSE`, which hands over every
    /// subscribe and only the cancel that emptied the peer's own table. A
    /// record that counted the repeats would owe the upstream a cancel for a
    /// prefix the peer has already let go of.
    #[tokio::test]
    async fn under_verbose_a_repeat_is_no_second_holder() {
        let (core, mut events) = a_device_end(SocketOptions {
            xpub_verbose: true,
            ..SocketOptions::default()
        });
        let (peer, _) = a_peer(&core, 9);
        assert!(events.admit(&core, peer, &subscribe(b"a")));
        assert!(!events.admit(&core, peer, &subscribe(b"a")), "a repeat");
        assert!(
            events.admit(&core, peer, &cancel(b"a")),
            "the cancel that emptied the peer's table ends its holding"
        );
        assert!(events.held.is_empty());
    }

    /// Claim: **under `ZMQ_XPUB_VERBOSER`, which hands over every cancel, each
    /// one takes one off the count**, and a cancel for a prefix the peer never
    /// held is not passed on, so it cannot cancel what another subscriber
    /// holds.
    #[tokio::test]
    async fn under_verboser_every_cancel_takes_one_off() {
        let (core, mut events) = a_device_end(SocketOptions {
            xpub_verboser: true,
            ..SocketOptions::default()
        });
        let (peer, _) = a_peer(&core, 9);
        let (other, _) = a_peer(&core, 19);
        assert!(events.admit(&core, peer, &subscribe(b"a")));
        assert!(!events.admit(&core, peer, &subscribe(b"a")));
        assert!(!events.admit(&core, other, &cancel(b"a")), "never held it");
        assert!(!events.admit(&core, peer, &cancel(b"a")), "one is left");
        assert!(events.admit(&core, peer, &cancel(b"a")), "and now none");
    }

    /// Claim: a `ZMQ_XPUB_MANUAL` socket is not made a forwarding device's end:
    /// what it reports are requests for the application to grant, and a
    /// request refused for a share would be one the application never saw.
    #[tokio::test]
    async fn a_manual_socket_is_left_as_it_was() {
        let core = xpub(SocketOptions {
            xpub_manual: true,
            ..SocketOptions::default()
        });
        let mut events = XPubEvents::default();
        events.share_upstream(&core);
        assert!(events.upstream.is_none());
    }

    /// A change to the peer set: a dialled peer that never connects arrives.
    fn a_peer_arrives(core: &SocketCore) {
        let nowhere = crate::endpoint::Endpoint::parse("tcp://127.0.0.1:9").expect("endpoint");
        core.engine().connect(&nowhere).expect("connect");
    }

    /// Claim: **an XPUB looks for departed peers only when the peer set has
    /// changed.** A receive that finds the set as it was costs one atomic
    /// load; a pass over every peer's record on every receive is a cost per
    /// message that a stranger multiplies by connecting. Shown with a record
    /// for a peer the engine does not have: it is left alone until the set
    /// changes, and then noticed.
    #[tokio::test]
    async fn a_receive_looks_for_departures_only_when_the_peer_set_changed() {
        let core = xpub(SocketOptions {
            reconnect_ivl: None,
            ..SocketOptions::default()
        });
        let mut events = XPubEvents::default();
        events.reconcile(&core);
        events
            .held
            .entry(PeerId::detached())
            .or_default()
            .insert(b"a".to_vec(), 1);

        events.reconcile(&core);
        assert!(
            events.departed.is_empty(),
            "the peer set has not changed, so nobody was looked for"
        );

        a_peer_arrives(&core);
        events.reconcile(&core);
        assert_eq!(
            events.departed.len(),
            1,
            "the set changed, and the peer is gone"
        );
    }

    /// Claim: **a ROUTER reconciles its routing table only when the peer set
    /// has changed**, for the same reason: re-reading every peer on every
    /// message is a cost a stranger multiplies by connecting. Shown with an
    /// entry for a peer the engine does not have.
    #[tokio::test]
    async fn a_router_reconciles_only_when_the_peer_set_changed() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let core = SocketCore::new(
            &ctx,
            SocketType::Router,
            SocketOptions {
                reconnect_ivl: None,
                ..SocketOptions::default()
            },
        )
        .expect("core");
        let mut routing = Routing::new();
        routing.learn_peers(&core);

        let ghost = PeerId::detached();
        routing.by_peer.insert(ghost, Message::from("ghost"));
        routing
            .by_key
            .insert(RoutingKey::from_wire(b"ghost"), ghost);
        routing.learn_peers(&core);
        assert_eq!(
            routing.len(),
            1,
            "nothing changed, so nothing was reconciled"
        );

        a_peer_arrives(&core);
        routing.learn_peers(&core);
        assert_eq!(
            routing.len(),
            0,
            "the set changed, and the ghost is dropped"
        );
    }
}
