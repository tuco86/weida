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
/// **Everything here is bounded by the tables' own product.** A peer's record
/// holds at most `max_subscriptions` prefixes of at most
/// `max_subscription_bytes` each, and the peers that left are turned into
/// unsubscribes one at a time, ahead of any new event: no subscription is
/// handed over while a departed peer is still owed, so the records of the
/// departed and of the live together never outnumber the peers that were
/// live.
#[derive(Debug, Default)]
pub(crate) struct XPubEvents {
    /// What the application believes each peer holds: the prefixes it was
    /// handed a subscribe for and no unsubscribe since — or, under
    /// `ZMQ_XPUB_MANUAL`, the ones it applied itself. A peer that holds
    /// nothing has no entry.
    held: HashMap<PeerId, HashSet<Vec<u8>>>,
    /// The peers that left holding something, oldest first, each waiting to be
    /// turned into unsubscribes: one prefix per receive, "SHALL, if the
    /// subscriber peer disconnects prematurely, generate a suitable
    /// unsubscribe request for the calling application."
    departed: VecDeque<Vec<Vec<u8>>>,
    /// The peer-set generation the departures were last looked for at.
    /// Looking again is worth a pass over `held` only when the engine has
    /// moved on, and a number keeps none of the peers it was taken from
    /// alive while it waits.
    seen: Option<u64>,
    /// The peer whose subscription was delivered most recently, which is what
    /// `ZMQ_XPUB_MANUAL`'s `subscribe` applies to — libzmq applies it to the
    /// last pipe too.
    last_subscriber: Option<PeerId>,
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
        self.held
            .entry(peer.id)
            .or_default()
            .insert(prefix.to_vec());
        Ok(())
    }

    pub(crate) fn unsubscribe(&mut self, core: &Shared, prefix: &[u8]) -> Result<()> {
        let peer = self.subscriber(core)?;
        match peer.subscriptions.cancel(prefix) {
            Some(last) => {
                if last {
                    self.release(peer.id, prefix);
                }
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
    /// must not grow with a peer that does that.
    fn admit(&mut self, core: &Shared, peer: PeerId, message: &Multipart) -> bool {
        let Some((subscribe, prefix)) = subscription_of(message) else {
            return true;
        };
        self.last_subscriber = Some(peer);
        if core.options().xpub_manual {
            return true;
        }
        if !subscribe {
            self.release(peer, prefix);
            return true;
        }
        let held = self.held.entry(peer).or_default();
        if !held.contains(prefix) {
            if held.len() >= core.options().max_subscriptions {
                core.engine().evict(peer);
                return false;
            }
            held.insert(prefix.to_vec());
        }
        true
    }

    /// Forgets that the application holds `prefix` for `peer`.
    fn release(&mut self, peer: PeerId, prefix: &[u8]) {
        if let Some(held) = self.held.get_mut(&peer) {
            held.remove(prefix);
            if held.is_empty() {
                self.held.remove(&peer);
            }
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
        self.seen = Some(generation);
    }

    /// The next unsubscribe owed for a peer that left, one prefix at a time:
    /// generated when it is handed over, not queued whole, so that a peer
    /// which held a thousand prefixes does not become a thousand messages in
    /// memory at once.
    fn next_departure(&mut self) -> Option<Multipart> {
        let owed = self.departed.front_mut()?;
        let prefix = owed.pop()?;
        if owed.is_empty() {
            self.departed.pop_front();
        }
        Some(Multipart::single(Message::from(
            subscriptions::write_message_form(false, &prefix),
        )))
    }
}

/// A one-frame message in the subscription form, read: `true` for a
/// subscribe, and the prefix.
fn subscription_of(message: &Multipart) -> Option<(bool, &[u8])> {
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
    pub(crate) fn frame_for(&mut self, core: &Shared, peer: PeerId) -> Message {
        if let Some(frame) = self.by_peer.get(&peer) {
            return frame.clone();
        }
        self.learn_peers(core);
        if let Some(frame) = self.by_peer.get(&peer) {
            return frame.clone();
        }
        let key = self.next_key();
        let frame = Message::from(key.as_bytes());
        self.by_key.insert(key, peer);
        self.by_peer.insert(peer, frame.clone());
        frame
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
    let (peer, message) = core.try_recv_fair()?;
    Ok(with_routing_id(core, routing, peer, message))
}

async fn router_await(core: &Shared, routing: &Mutex<Routing>) -> Result<Multipart> {
    lock(routing).learn_peers(core);
    let (peer, message) = core.recv_fair().await?;
    Ok(with_routing_id(core, routing, peer, message))
}

fn with_routing_id(
    core: &Shared,
    routing: &Mutex<Routing>,
    peer: PeerId,
    message: Multipart,
) -> Multipart {
    let id = lock(routing).frame_for(core, peer);
    let mut frames = message.into_frames();
    frames.insert(0, id);
    Multipart::new(frames).expect("a routing id plus at least one frame")
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
            "a repeat holds nothing new"
        );
        assert!(
            !events.admit(&core, peer, &subscribe(b"c")),
            "a third distinct prefix is not handed over"
        );
        assert!(events.admit(&core, peer, &cancel(b"a")));
        assert!(
            events.admit(&core, peer, &subscribe(b"c")),
            "there is room again once one was cancelled"
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
            .insert(b"a".to_vec());

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
