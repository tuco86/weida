//! The link: `attach`, `detach`, the two handle spaces, and the credit a
//! delivery is sent under.
//!
//! A link is "a unidirectional named route between two nodes, attached at
//! each end to a terminus" (Part 2 §2.6). Five of its rules are easy to get
//! subtly wrong, and each one has a test here.
//!
//! # Two schemes must permit one transfer
//!
//! A message goes out only when **both** the link's credit and the session's
//! window allow it. Link credit is counted in messages and lives in
//! [`crate::credit`]; the window is counted in `transfer` frames and lives in
//! [`crate::window`]. So a message larger than one frame takes one unit of
//! credit and as many window units as it takes frames, and either scheme
//! alone can stall a sender that the other is happy with. The specification
//! ties them together nowhere, and neither does this module.
//!
//! # The two ends choose their handles independently
//!
//! "The ends need not agree: the local choice is the *output handle*, the
//! remote the *input handle*" (Part 2 §2.6.2). So an `attach` carries the
//! handle *its own sender* chose, and the answering `attach` carries a
//! different number. Which means the answer cannot be correlated by handle —
//! it is correlated by **name**, and this module keeps three maps because of
//! it: by our output handle, by the peer's input handle, and by name.
//!
//! ```text
//! client                                                    broker
//!   attach(name="orders", handle=0, role=sender)   -->
//!                                                  <--  attach(name="orders", handle=7, role=receiver)
//!   transfer(handle=0)                             -->
//!                                                  <--  disposition(role=receiver)
//! ```
//!
//! # The name is the identity, and a second attach steals
//!
//! A name "MUST uniquely identify the link among all links of the same
//! direction between the two containers", so a link is active on one
//! connection at a time and a second attach elsewhere *steals* it: the first
//! MUST be closed with `amqp:link:stolen` (Part 2 §2.6.1). The rule exists so
//! that re-establishment works when only one party has noticed a failure —
//! without it, a client reconnecting after a network partition the broker has
//! not seen yet could not get its link back.
//!
//! This client implements the half it owns: attaching a link whose name and
//! direction are already in use on this connection detaches the incumbent
//! with `amqp:link:stolen` and proceeds. Silently refusing the second attach
//! would be the *opposite* of the specified behaviour, and quietly running
//! two links with one name would break the peer's own bookkeeping.
//!
//! # An errored handle is never silently reused
//!
//! "An errored link endpoint MUST be detached with `detach(error=...)` and
//! destroyed; any later input on that handle or its delivery-ids MUST end the
//! session with `amqp:session:errant-link`" (Part 2 §2.6.5). Handles *may* be
//! reused after a clean close — but reusing one that was detached with an
//! error means a late frame for the dead link lands on a live one, and the
//! peer is entitled to still be sending them. So handle allocation skips
//! a handle that was errored, for the life of the session.
//!
//! # Deliveries on one link do not interleave, and are bounded
//!
//! "The deliveries on a given link MUST NOT interleave" (Part 2 §2.6.14), so
//! a second delivery-tag arriving while the first is incomplete is refused
//! here rather than concatenated — the octets of two messages are not a
//! message. And nothing in the protocol bounds the *number* of frames a
//! delivery may take, which is what makes `max-message-size` load-bearing
//! rather than advisory: without it a peer hands a receiver an unbounded
//! message one bounded frame at a time. The bound is checked before the
//! buffer grows, and exceeding it is `amqp:link:message-size-exceeded`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, mpsc, oneshot};
use weida_amqp_codec::message::{Message, Progress, Reassembly};
use weida_amqp_codec::performative::{Attach, Detach, Disposition, Flow, Performative, Transfer};
use weida_amqp_codec::state::DeliveryState;
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{DecodeError, ReceiverSettleMode, Role, SenderSettleMode};
use weida_runtime::Exec;

use crate::connection::{Lease, Outbound};
use crate::credit::Credit;
use crate::delivery::{Delivery, Sent};
use crate::error::{Condition, Error, Result};
use crate::options::multiple;
use crate::queue::{self, Queued};
use crate::settlement::{MAX_TAG, Outcome, Pending, Settlement, Unsettled};
use crate::terminus::{Source, Target};
use crate::window::Windows;

/// How many reports — outcomes and credit updates — a link keeps for a reader
/// that has not come.
///
/// **A bound of ours.** The connection driver never waits for an application
/// to read what a link tells it, so nothing is queued against a capacity; what
/// bounds the queue is where its contents come from. A delivery is queued only
/// against credit the application granted itself. An outcome or a credit
/// update is also held in the link's own state, so once this many are waiting
/// the oldest is let go and the current state is still one call away
/// ([`Link::unsettled`], [`Link::credit`]): a sender keeps the delivery the
/// peer settled, with its outcome, until the application has taken the
/// outcome, and that is also what bounds how many a sender can have waiting.
/// An application that reads its events never loses one.
pub const MAX_QUEUED_REPORTS: usize = 1024;

/// The queue a link's events travel in.
pub(crate) fn events() -> (queue::Sender<LinkEvent>, queue::Receiver<LinkEvent>) {
    queue::channel(MAX_QUEUED_REPORTS)
}

/// How this end settles and when, plus what the peer actually created.
///
/// Filled in from the answering `attach`, which is **data and not an
/// acknowledgement**: "an endpoint that cannot create exactly the requested
/// terminus MAY adjust properties but MUST then report what it actually
/// created" (Part 2 §2.6.3). A client that ignored the answer would be
/// running against a terminus it did not ask for — a queue instead of an
/// exchange, a filter silently dropped, a settle mode it cannot honour.
#[derive(Clone, Debug, PartialEq)]
pub struct Negotiated {
    /// How the sender settles. Authoritative from whichever end has
    /// `role=sender`, because the field describes that end's own behaviour.
    pub snd_settle_mode: SenderSettleMode,
    /// When the receiver settles. Authoritative from whichever end has
    /// `role=receiver`.
    ///
    /// The field worth checking: `Second` is what exactly-once is built from
    /// and Artemis refuses it outright ("The Broker does not currently
    /// support ReceiverSettleMode of SECOND"), while RabbitMQ lists
    /// exactly-once as unsupported. A client that asked for `Second` and did
    /// not read the answer would believe it had a guarantee it does not have.
    pub rcv_settle_mode: ReceiverSettleMode,
    /// The source the peer says is in place. `None` means the peer refused to
    /// provide a terminus and MUST then immediately detach.
    pub remote_source: Option<Source>,
    /// The target the peer says is in place.
    pub remote_target: Option<Target>,
    /// The peer's `max-message-size`: zero or unset means no limit.
    pub remote_max_message_size: Option<u64>,
    /// What the peer offered.
    pub remote_offered_capabilities: Vec<String>,
}

/// Where a link is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkState {
    /// `attach` sent, the answering `attach` not yet seen.
    Attaching,
    /// Both `attach` frames exchanged.
    Attached,
    /// `detach` sent, the partner's not yet seen.
    Detaching,
    /// Done. `Some` where either end gave a condition.
    Detached(Option<Condition>),
}

impl LinkState {
    /// Whether deliveries may flow.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Attached)
    }
}

/// How a link is attached.
#[derive(Clone, Debug)]
pub struct LinkOptions {
    /// `attach.name`, mandatory and the link's real identity.
    pub name: String,
    /// `attach.role`: `false` is the sender, `true` the receiver.
    pub role: Role,
    /// The source terminus. A receiver describes the node it reads from; a
    /// sender still sends one, because "the link is always considered to be
    /// between the source as described by the sender, and the target as
    /// described by the receiver".
    pub source: Option<Source>,
    /// The target terminus.
    pub target: Option<Target>,
    /// How this end will settle if it is the sender.
    pub snd_settle_mode: SenderSettleMode,
    /// When this end will settle if it is the receiver.
    pub rcv_settle_mode: ReceiverSettleMode,
    /// Zero or unset means no limit. Set it: the alternative is agreeing to
    /// a message of unbounded size, and `amqp:link:message-size-exceeded`
    /// exists precisely so a receiver can say no.
    pub max_message_size: Option<u64>,
    /// Where a sender starts its `delivery-count` sequence. MUST NOT be null
    /// for a sender and is ignored for a receiver.
    pub initial_delivery_count: u32,
    /// `attach.offered-capabilities`.
    pub offered_capabilities: Vec<String>,
    /// `attach.desired-capabilities`.
    pub desired_capabilities: Vec<String>,
}

impl LinkOptions {
    /// A sending link to `target`.
    #[must_use]
    pub fn sender(name: impl Into<String>, target: Target) -> Self {
        Self {
            name: name.into(),
            role: Role::Sender,
            source: Some(Source::default()),
            target: Some(target),
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            initial_delivery_count: 0,
            offered_capabilities: Vec::new(),
            desired_capabilities: Vec::new(),
        }
    }

    /// A receiving link from `source`.
    #[must_use]
    pub fn receiver(name: impl Into<String>, source: Source) -> Self {
        Self {
            name: name.into(),
            role: Role::Receiver,
            source: Some(source),
            target: Some(Target::default()),
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            initial_delivery_count: 0,
            offered_capabilities: Vec::new(),
            desired_capabilities: Vec::new(),
        }
    }

    /// Refuses an unusable configuration where it is configured.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Configuration(
                "attach.name is mandatory and is the link's identity: a second \
                 attach of the same name steals the first, so an empty one \
                 would steal every link on the connection"
                    .into(),
            ));
        }
        Ok(())
    }

    fn to_attach(&self, handle: u32) -> Attach<'_> {
        let mut attach = Attach::new(&self.name, handle, self.role);
        attach.snd_settle_mode = self.snd_settle_mode;
        attach.rcv_settle_mode = self.rcv_settle_mode;
        attach.source = self.source.as_ref().map(Source::to_value);
        attach.target = self.target.as_ref().map(Target::to_value);
        // MUST NOT be null for a sender; harmless and ignored for a receiver,
        // and sending it either way keeps one code path.
        attach.initial_delivery_count = Some(self.initial_delivery_count);
        attach.max_message_size = self.max_message_size;
        attach.offered_capabilities = multiple(&self.offered_capabilities);
        attach.desired_capabilities = multiple(&self.desired_capabilities);
        attach
    }
}

/// This client's `max-message-size`, deliberately not the specification's
/// "no limit".
///
/// **A default that differs on purpose** (0013 §4.4 item 5). Unset or zero
/// means unlimited, which is agreeing to allocate whatever a peer sends one
/// bounded frame at a time. 16 MiB is far above what any broker in the sheet
/// accepts — Service Bus caps a Premium message at 100 MB and a Standard one
/// at 256 KB — and finite. Settable back to zero, but never silently.
pub const DEFAULT_MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024;

/// What a link hands to the layer above it.
#[derive(Clone, Debug)]
pub enum LinkEvent {
    /// A whole message, reassembled from however many `transfer` frames it
    /// took.
    Delivery(Delivery),
    /// A delivery's state changed, because a `disposition` said so.
    ///
    /// `settled` is the difference between a provisional answer and a final
    /// one: `false` means the peer published the outcome but has not settled,
    /// which under `rcv-settle-mode=second` is the middle of the dance.
    ///
    /// A report: one that has not been read when [`MAX_QUEUED_REPORTS`] more
    /// have arrived is let go, and the state it described is still in
    /// [`Link::unsettled`]. That holds for the answer to a delivery this end
    /// *sent*, final ones included: the delivery stays there, finished and
    /// with its outcome, until the application takes the outcome by reading
    /// this event or by [`Link::settled`], so the only copy of a rejection is
    /// never the event. What a *receiver* is told when the sender settles —
    /// the end of the `second` dance — confirms an outcome this end chose
    /// itself, and is gone from `unsettled` by then.
    Outcome {
        /// Which delivery.
        delivery_id: u32,
        /// What the peer committed to.
        outcome: Outcome,
        /// Whether the delivery is now settled at both ends.
        settled: bool,
    },
    /// The peer's `flow` changed this link's credit state, and the new state
    /// is the one [`Link::credit`] reports.
    ///
    /// Worth an event rather than only a field, because a receiver draining a
    /// link is *waiting* for this: credit reaching zero with no delivery in
    /// between is the definite answer that there was nothing to get. Events
    /// are queued in the order the link saw things in, so a delivery that
    /// arrived before the `flow` is read before it.
    ///
    /// A report, and one that supersedes the one directly before it: several
    /// `flow` frames with nothing between them are one event carrying the
    /// latest state.
    Flow(Credit),
    /// The link detached.
    Detached(Option<Condition>),
}

impl Queued for LinkEvent {
    fn is_report(&self) -> bool {
        matches!(self, Self::Outcome { .. } | Self::Flow(_))
    }

    fn absorb(&mut self, next: Self) -> Option<Self> {
        match (&mut *self, next) {
            (Self::Flow(have), Self::Flow(newer)) => {
                *have = newer;
                None
            }
            (_, next) => Some(next),
        }
    }
}

/// The state a link's handle and the connection driver share.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) name: String,
    pub(crate) role: Role,
    /// Our choice, and the number that goes in every frame we send.
    pub(crate) output_handle: u32,
    /// The session's outgoing channel.
    pub(crate) channel: u16,
    pub(crate) state: Mutex<LinkState>,
    pub(crate) negotiated: Mutex<Option<Negotiated>>,
    /// The peer's choice, learned from the answering `attach`.
    pub(crate) input_handle: Mutex<Option<u32>>,
    /// This link's credit, counted in messages.
    pub(crate) credit: Mutex<Credit>,
    /// The session this link rides on, held for the **other** credit scheme:
    /// a `transfer` frame needs link credit *and* a session window unit, the
    /// two are independent, and only the session has the second.
    pub(crate) session: Arc<crate::session::Shared>,
    /// Woken whenever either scheme may have made room — a `flow` for this
    /// link, or one for the session it is on. A sender waiting for credit
    /// waits here rather than polling.
    pub(crate) flow: Notify,
    /// The largest frame the peer will accept, which is what decides where a
    pub(crate) max_frame_size: u32,
    /// How many payload octets fit in one frame of a delivery on this link.
    ///
    /// Measured once, when the link is attached: the first frame's
    /// performative is the largest one a delivery carries and it depends only
    /// on the handle and the tag's length, so encoding it per message told
    /// nothing new.
    pub(crate) frame_budget: usize,
    /// The next delivery-tag this end will choose.
    pub(crate) next_tag: Mutex<u64>,
    /// The deliveries this end still holds state for.
    ///
    /// Shared with the driver because both write it: a sender records a
    /// delivery here as it sends, and the driver retires it when the
    /// receiver's `disposition` arrives.
    pub(crate) unsettled: Mutex<Unsettled>,
    /// Held while a delivery is on its way out, so that two deliveries on one
    /// link never interleave (Part 2 §2.6.14) however many tasks send on it.
    /// An async lock, taken by the sender before it looks at credit or the
    /// window, so a waiting sender holds nothing but its place in the line.
    pub(crate) send_lock: Arc<AsyncMutex<()>>,
}

/// An AMQP 1.0 link.
#[derive(Debug)]
pub struct Link {
    shared: Arc<Shared>,
    outbound: mpsc::Sender<Outbound>,
    events: queue::Receiver<LinkEvent>,
    /// The reactor, for the one task a dropped `send` can leave behind: the
    /// frame that closes a delivery whose first frames are already out.
    exec: Exec,
    /// Keeps the connection alive as long as this handle is.
    _lease: Lease,
}

impl Link {
    pub(crate) fn new(
        shared: Arc<Shared>,
        outbound: mpsc::Sender<Outbound>,
        events: queue::Receiver<LinkEvent>,
        exec: Exec,
        lease: Lease,
    ) -> Self {
        Self {
            shared,
            outbound,
            events,
            exec,
            _lease: lease,
        }
    }

    /// The link's name, which is its identity between the two containers.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// Which end of the link this is.
    #[must_use]
    pub fn role(&self) -> Role {
        self.shared.role
    }

    /// Our handle: the number this end writes into every frame it sends.
    #[must_use]
    pub fn output_handle(&self) -> u32 {
        self.shared.output_handle
    }

    /// The peer's handle: the number it writes into every frame it sends.
    ///
    /// A different number from [`Link::output_handle`] in general, which is
    /// why the answering `attach` is correlated by name and not by handle.
    #[must_use]
    pub fn input_handle(&self) -> Option<u32> {
        *self.shared.input_handle.lock().expect("not poisoned")
    }

    /// Where the link is.
    #[must_use]
    pub fn state(&self) -> LinkState {
        self.shared.state.lock().expect("not poisoned").clone()
    }

    /// What the answering `attach` said: the settle modes actually in force
    /// and the termini actually created.
    #[must_use]
    pub fn negotiated(&self) -> Option<Negotiated> {
        self.shared.negotiated.lock().expect("not poisoned").clone()
    }

    /// The source the peer says is in place, or `None` if it refused to
    /// provide one.
    #[must_use]
    pub fn remote_source(&self) -> Option<Source> {
        self.negotiated().and_then(|n| n.remote_source)
    }

    /// The target the peer says is in place.
    #[must_use]
    pub fn remote_target(&self) -> Option<Target> {
        self.negotiated().and_then(|n| n.remote_target)
    }

    /// The next event, or `None` once the link has detached and everything
    /// queued has been taken.
    ///
    /// Reading the final outcome of a delivery this end sent is what takes
    /// it: the delivery is forgotten with the event, which makes its place in
    /// the unsettled map free for the next one.
    pub async fn next_event(&mut self) -> Option<LinkEvent> {
        let event = self.events.recv().await?;
        self.taken(&event);
        Some(event)
    }

    /// The application has this event, so what it stood for is no longer
    /// owed: a sender's finished delivery, kept for its outcome, is let go.
    fn taken(&self, event: &LinkEvent) {
        if self.shared.role != Role::Sender {
            return;
        }
        if let LinkEvent::Outcome {
            delivery_id,
            settled: true,
            ..
        } = event
            && self.take_outcome(*delivery_id).is_some()
        {
            self.shared.flow.notify_waiters();
        }
    }

    /// The final outcome of a delivery this end sent, and the delivery with
    /// it, if the peer has settled it.
    fn take_outcome(&self, delivery_id: u32) -> Option<Outcome> {
        self.shared
            .unsettled
            .lock()
            .expect("not poisoned")
            .take_final(delivery_id)
    }

    /// The next whole message, skipping the events that are not one.
    ///
    /// `None` where the link detached before another delivery arrived — which
    /// includes the drain case, and is why a caller that needs to tell
    /// "nothing to get" from "link gone" reads [`Link::next_event`] instead.
    pub async fn next_delivery(&mut self) -> Option<Delivery> {
        loop {
            match self.events.recv().await? {
                LinkEvent::Delivery(delivery) => return Some(delivery),
                LinkEvent::Detached(_) => return None,
                LinkEvent::Flow(_) | LinkEvent::Outcome { .. } => {}
            }
        }
    }

    /// This link's credit state: `delivery-count`, `link-credit`,
    /// `available` and `drain`.
    #[must_use]
    pub fn credit(&self) -> Credit {
        *self.shared.credit.lock().expect("not poisoned")
    }

    /// Whether a message could go out right now.
    ///
    /// Three conditions, and they are three because the protocol has three:
    /// the link must be attached, the **link** must have credit — counted in
    /// messages, granted by the receiver — and the **session** window must
    /// have room for a frame. A generous grant with a full window sends
    /// nothing, and so does an empty window with generous credit.
    #[must_use]
    pub fn may_send(&self) -> bool {
        self.state().is_usable()
            && self.credit().may_send()
            && self
                .shared
                .session
                .windows
                .lock()
                .expect("not poisoned")
                .may_send()
    }

    /// A **receiver**: grants credit for `credit` more messages.
    ///
    /// Sets rather than adds. What goes on the wire is the absolute limit
    /// `delivery-count + link-credit`, so granting 10 twice grants 10 and a
    /// `flow` that is duplicated or overtaken cannot inflate anything. Clears
    /// `drain`, because a grant and a drain are opposite requests.
    ///
    /// Refused where the sender could then deliver more unsettled messages
    /// than this end can keep state for: the receiver's unsettled map is
    /// bounded ([`DEFAULT_MAX_UNSETTLED`](crate::settlement::DEFAULT_MAX_UNSETTLED)),
    /// a delivery that arrives unsettled when it is full is refused with
    /// `amqp:resource-limit-exceeded`, and the way for an application never to
    /// meet that is to grant no more than it holds room for. A link whose
    /// sender settles everything as it sends records nothing, so its credit
    /// is not limited by the map.
    pub async fn grant_credit(&self, credit: u32) -> Result<()> {
        self.role_must_be(Role::Receiver, "grant credit")?;
        self.check_room_for(credit)?;
        self.change_and_flow(false, |state, _| {
            state.grant(credit);
            state.set_drain(false);
        })
        .await
    }

    /// Refuses a grant of `credit` where the unsettled map could not hold what
    /// it lets arrive.
    fn check_room_for(&self, credit: u32) -> Result<()> {
        let settles_on_send = matches!(
            self.negotiated().map(|n| n.snd_settle_mode),
            Some(SenderSettleMode::Settled)
        );
        if settles_on_send {
            return Ok(());
        }
        let (held, capacity) = {
            let map = self.shared.unsettled.lock().expect("not poisoned");
            (map.len(), map.capacity())
        };
        let room = capacity.saturating_sub(held);
        if usize::try_from(credit).is_ok_and(|credit| credit <= room) {
            return Ok(());
        }
        Err(Error::Configuration(format!(
            "granting {credit} credit on link {} could let the sender deliver more unsettled \
             messages than this end can keep state for: it holds {held} of at most {capacity}, \
             so at most {room} may be granted until some are settled",
            self.shared.name
        )))
    }

    /// A **receiver**: asks the sender to drain.
    ///
    /// The sender sends whatever it has and then advances `delivery-count`
    /// until `link-credit` is zero, reporting the new state. That is what
    /// turns "wait for a message" into "wait for a definite answer": credit
    /// reaching zero with no delivery in between means there was nothing to
    /// get, and a get-with-timeout is built out of exactly that.
    pub async fn drain(&self) -> Result<()> {
        self.role_must_be(Role::Receiver, "drain")?;
        self.change_and_flow(false, |state, _| state.set_drain(true))
            .await
    }

    /// A **receiver**: stops the link with `flow(link-credit=0, echo=true)`.
    ///
    /// The echo is the point. Transfers already in flight may still arrive
    /// until the sender has processed the new limit, and the echoed `flow` is
    /// the marker after which no further transfer will come — without it,
    /// "stopped" is a hope rather than an observation (Part 2 §2.6.10).
    pub async fn stop(&self) -> Result<()> {
        self.role_must_be(Role::Receiver, "stop")?;
        self.change_and_flow(true, |state, _| {
            state.grant(0);
            state.set_drain(false);
        })
        .await
    }

    /// A **receiver**: restores the session's incoming window and tells the
    /// peer.
    ///
    /// The session window shrinks as `transfer` frames arrive and only grows
    /// again when the receiver says so; this is the saying so. A receiver
    /// that granted link credit but never replenished the window would stall
    /// its sender with credit in hand.
    pub async fn replenish_session_window(&self) -> Result<()> {
        let window = self.shared.session.options.incoming_window;
        self.change_and_flow(false, |_, windows| windows.replenish_incoming(window))
            .await
    }

    /// The deliveries this end still holds state for, in delivery-id order.
    ///
    /// Empty is the answer an application usually wants: it means nothing is
    /// outstanding in either direction. A sender's list also holds the
    /// deliveries the peer has settled whose outcome nobody has taken yet,
    /// marked [`Pending::settled_there`]: the answer is kept with them.
    #[must_use]
    pub fn unsettled(&self) -> Vec<Pending> {
        self.shared
            .unsettled
            .lock()
            .expect("not poisoned")
            .pending()
    }

    /// A **receiver**: accepts a delivery.
    ///
    /// What it commits to: the message was processed and the sender may
    /// retire it at the source. It does **not** say "on disk" — durability is
    /// asserted by `header.durable` on the way in, and a target that cannot
    /// honour it MUST refuse the message rather than accept it.
    pub async fn accept(&self, delivery_id: u32) -> Result<()> {
        self.settle(delivery_id, delivery_id, &Outcome::Accepted)
            .await
    }

    /// A **receiver**: rejects a delivery as invalid and unprocessable.
    ///
    /// What it commits to: this node will not redeliver it, and the message's
    /// `delivery-count` goes up — so a redelivery limit counts this attempt.
    pub async fn reject(&self, delivery_id: u32, error: Option<Condition>) -> Result<()> {
        self.settle(delivery_id, delivery_id, &Outcome::Rejected { error })
            .await
    }

    /// A **receiver**: releases a delivery, unchanged and available again.
    ///
    /// What it commits to: nothing was acted upon and `delivery-count` is
    /// **not** incremented, which makes a released message indistinguishable
    /// from one that was never delivered.
    pub async fn release(&self, delivery_id: u32) -> Result<()> {
        self.settle(delivery_id, delivery_id, &Outcome::Released)
            .await
    }

    /// A **receiver**: makes a delivery available again, with a note.
    ///
    /// `delivery_failed` increments `delivery-count`;
    /// `undeliverable_here` says this link endpoint MUST NOT see it again.
    /// Both are `Option` because the specification gives them no defaults,
    /// and unset leaves the node's own policy in charge.
    pub async fn modify(
        &self,
        delivery_id: u32,
        delivery_failed: Option<bool>,
        undeliverable_here: Option<bool>,
    ) -> Result<()> {
        self.settle(
            delivery_id,
            delivery_id,
            &Outcome::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations: None,
            },
        )
        .await
    }

    /// Settles every delivery this end holds in `first..=last` with one
    /// `disposition`.
    ///
    /// One frame for a range is the point of the range: "a disposition MAY
    /// cover deliveries from many links of the session provided they all have
    /// this role", and a receiver acknowledging a batch writes one frame
    /// rather than one per message. Deliveries in the range that this end no
    /// longer holds are simply not there — that is idempotence, not an error:
    /// one it already settled, one the sender settled as it sent it, and one
    /// that was never delivered all need no frame, and `Ok` is the true
    /// answer for each. Every delivery this end *did* hand to the
    /// application unsettled is held, because a delivery that could not be
    /// recorded is refused instead of delivered.
    ///
    /// An error where the acknowledgement could not reach the peer: the
    /// connection is gone, or the link has detached and the peer destroyed
    /// what it held for these deliveries, so they will be offered again.
    ///
    /// Cancel-safe: dropping the future before it finishes leaves every
    /// delivery held and nothing written, so the call can simply be made
    /// again. Waiting is for room in the driver's queue, and no delivery is
    /// forgotten until the place for its `disposition` is already reserved.
    pub async fn settle(&self, first: u32, last: u32, outcome: &Outcome) -> Result<()> {
        // A sender may also publish a state — that is the `second` dance's
        // second half — but naming an outcome is the receiver's move.
        let second = matches!(
            self.negotiated().map(|n| n.rcv_settle_mode),
            Some(ReceiverSettleMode::Second)
        );
        // Nothing here to settle is `Ok` and no wait: writing a frame anyway
        // would be harmless for the peer and a lie to a reader of the log.
        if self.to_settle(first, last)?.is_empty() {
            return Ok(());
        }
        // Built before anything is changed: a frame that cannot be encoded
        // is an error with every delivery still held.
        let frame = self.disposition_frame(first, last, Some(outcome), !second)?;
        let permit = self.permit().await?;

        // No wait from here to the end: the deliveries are forgotten and the
        // `disposition` is queued as one step.
        let covered = self.to_settle(first, last)?;
        if covered.is_empty() {
            // The peer settled them while this waited for room.
            return Ok(());
        }
        {
            let mut map = self.shared.unsettled.lock().expect("not poisoned");
            for id in &covered {
                // Under `second` this end does *not* settle yet: it publishes
                // the outcome and waits for the sender to settle first, which
                // is the whole difference between the two modes.
                map.record_ours(*id, outcome, !second);
            }
        }
        // The map made room, so a sender parked on it can go.
        self.shared.flow.notify_waiters();
        permit.send(Outbound::Frame(frame));
        Ok(())
    }

    /// The deliveries `settle` would settle in `first..=last`. An error where
    /// there are some and the link has gone: the peer destroyed what it held
    /// for them.
    fn to_settle(&self, first: u32, last: u32) -> Result<Vec<u32>> {
        let covered = self
            .shared
            .unsettled
            .lock()
            .expect("not poisoned")
            .ids_in(first, last);
        if !covered.is_empty()
            && let LinkState::Detached(condition) = self.state()
        {
            return Err(Error::Closed(condition));
        }
        Ok(covered)
    }

    /// The outcome of one delivery, waited for.
    ///
    /// Returns when the delivery is settled at both ends, which is the only
    /// point at which the answer cannot change. Under
    /// `rcv-settle-mode=first` that is the receiver's single `disposition`;
    /// under `second` it is the end of the three-frame dance.
    ///
    /// A sender's answer is kept with the delivery until it is taken, here or
    /// by reading the [`LinkEvent::Outcome`] that reported it, so this finds
    /// it whether it arrived before the call or while the call was waiting,
    /// and whether or not the event was let go of in between. Taking it is
    /// what frees the delivery's place in the unsettled map.
    ///
    /// Cancel-safe: the answer is taken in one step with returning it, so a
    /// dropped future has not lost it.
    pub async fn settled(&mut self, delivery_id: u32) -> Result<Outcome> {
        if let Some(outcome) = self.answered(delivery_id) {
            return Ok(outcome);
        }
        if self
            .shared
            .unsettled
            .lock()
            .expect("not poisoned")
            .get(delivery_id)
            .is_none()
        {
            return Err(Error::Configuration(format!(
                "delivery {delivery_id} is not unsettled on link {}: it was \
                 settled on send, already settled, or never sent from here",
                self.shared.name
            )));
        }
        loop {
            match self.events.recv().await {
                Some(LinkEvent::Outcome {
                    delivery_id: id,
                    outcome,
                    settled: true,
                }) if id == delivery_id => {
                    // The record holds the same answer; taking it is what
                    // makes room.
                    self.take_outcome(delivery_id);
                    self.shared.flow.notify_waiters();
                    return Ok(outcome);
                }
                Some(LinkEvent::Detached(condition)) => {
                    // An answer that arrived before the link went is still
                    // an answer.
                    return self.answered(delivery_id).ok_or(Error::Closed(condition));
                }
                // Another delivery's event, read through. This one's own may
                // have been let go while others were being read: its answer
                // is in the record.
                Some(_) => {
                    if let Some(outcome) = self.answered(delivery_id) {
                        return Ok(outcome);
                    }
                }
                None => return Err(Error::ConnectionGone),
            }
        }
    }

    /// Takes the answer a sender kept for `delivery_id`, if there is one, with
    /// the event that reported it, so that reading the queue afterwards does
    /// not answer the same question twice.
    fn answered(&mut self, delivery_id: u32) -> Option<Outcome> {
        let outcome = self.take_outcome(delivery_id)?;
        self.events.discard(|event| {
            matches!(
                event,
                LinkEvent::Outcome { delivery_id: id, settled: true, .. } if *id == delivery_id
            )
        });
        self.shared.flow.notify_waiters();
        Some(outcome)
    }

    /// One `disposition` covering `first..=last`, encoded.
    fn disposition_frame(
        &self,
        first: u32,
        last: u32,
        outcome: Option<&Outcome>,
        settled: bool,
    ) -> Result<Vec<u8>> {
        let state = outcome.map(Outcome::to_state);
        let mut disposition = Disposition::new(self.shared.role, first);
        // `last` unset means `first` alone, so a single delivery is the
        // shorter frame rather than a range of one.
        disposition.last = (last != first).then_some(last);
        disposition.settled = settled;
        disposition.state = state.as_ref().map(DeliveryState::to_value);
        self.frame(Performative::Disposition(disposition), &[])
    }

    /// A **sender**: sends one message, stalling until both schemes permit
    /// it.
    ///
    /// Link credit is taken once for the whole message, because it is counted
    /// in messages; a session window unit is taken for **each frame**, which
    /// is what makes a large message dribble out under a tight window instead
    /// of violating it.
    ///
    /// Cancel-safe, as [`Link::send_payload`] says.
    pub async fn send(&self, message: &Message<'_>) -> Result<Sent> {
        let mut payload = Vec::with_capacity(SEND_PAYLOAD_ROOM);
        message.encode(&mut payload)?;
        self.send_payload(&payload).await
    }

    /// A **sender**: sends already-encoded message sections.
    ///
    /// The sections are not re-parsed: a forwarder that received octets and
    /// is passing them on has nothing to gain from a decode and re-encode,
    /// and the specification's message identity is the octets.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future is safe wherever it is, which matters because the
    /// call waits — for credit, for window, for room in the driver's queue —
    /// and a `timeout` around it is the obvious thing to write.
    ///
    /// Until the first frame is queued nothing has been taken: not the
    /// credit, not the place in the unsettled map, not the window. A message
    /// that fits one frame is queued with everything it takes in one step.
    /// A message of several frames is never left half-sent: once its first
    /// frame is queued the delivery is open, and a future dropped before the
    /// last frame closes it with `transfer(aborted=true)` — queued by a task
    /// as soon as the session window has room for the frame — and forgets it
    /// here, so the receiver discards what it holds and the next send on the
    /// link starts a clean delivery. The credit it took stays spent: the
    /// receiver counted it when the first frame arrived.
    ///
    /// Sends on one link take turns, in the order they asked, because the
    /// deliveries of a link do not interleave (Part 2 §2.6.14).
    pub async fn send_payload(&self, payload: &[u8]) -> Result<Sent> {
        self.role_must_be(Role::Sender, "send")?;
        // A task waiting for its turn holds nothing but its place in line.
        let turn = Arc::clone(&self.shared.send_lock).lock_owned().await;
        let settled = matches!(self.settle_mode(), SenderSettleMode::Settled);
        let tag = self.peek_tag();
        let budget = self.shared.frame_budget;
        let egress = self.egress();

        let mut end = budget.min(payload.len());
        let mut more = end < payload.len();
        // One unit of link credit, for the message, taken before the first
        // frame and not per frame: "one unit of link-credit permits the
        // delivery-count to advance by one", and a multi-frame message
        // advances it once.
        let delivery_id = egress.first(settled, &tag, &payload[..end], more).await?;
        self.advance_tag();

        let mut frames = 1;
        if more {
            // From here to the last frame no future may be dropped without
            // the receiver being told: this closes the delivery if one is.
            let mut open = Open {
                egress: egress.clone(),
                exec: self.exec.clone(),
                delivery_id,
                tag,
                settled,
                turn: Some(turn),
                closed: false,
            };
            let mut offset = end;
            while more {
                end = (offset + budget).min(payload.len());
                more = end < payload.len();
                egress.next(&payload[offset..end], more).await?;
                frames += 1;
                offset = end;
            }
            open.closed = true;
        }

        Ok(Sent {
            delivery_id,
            delivery_tag: tag.to_vec(),
            frames,
            settled,
        })
    }

    /// How this end settles, as the answering `attach` left it.
    ///
    /// `Mixed` before the answer arrives, which is the specification's own
    /// default and means "decided per delivery" — and this client's decision
    /// for a `Mixed` link is unsettled, because a settled transfer is one the
    /// receiver cannot report anything about.
    fn settle_mode(&self) -> SenderSettleMode {
        self.negotiated()
            .map_or(SenderSettleMode::Mixed, |n| n.snd_settle_mode)
    }

    /// Refuses an operation the other end of the link owns.
    fn role_must_be(&self, role: Role, what: &str) -> Result<()> {
        if self.shared.role == role {
            return Ok(());
        }
        Err(Error::Configuration(format!(
            "only the {} may {what}, and link {} is the {}",
            role_name(role),
            self.shared.name,
            role_name(self.shared.role)
        )))
    }

    /// The delivery-tag the next delivery will carry: a per-link counter,
    /// eight octets.
    ///
    /// The requirement is that a tag be "unique among the deliveries either
    /// end could consider unsettled on this link", which a counter satisfies
    /// without a random source — and the field allows 32 octets, so eight is
    /// a quarter of the budget for a number that cannot repeat before the
    /// connection has outlived everything.
    ///
    /// Read here and moved by [`Link::advance_tag`] once the delivery's first
    /// frame is queued, so a send that gives up before that leaves no gap in
    /// the sequence. Sends take turns, which makes the two one step.
    fn peek_tag(&self) -> [u8; TAG_LEN] {
        self.shared
            .next_tag
            .lock()
            .expect("not poisoned")
            .to_be_bytes()
    }

    fn advance_tag(&self) {
        let mut next = self.shared.next_tag.lock().expect("not poisoned");
        *next = next.wrapping_add(1);
    }

    /// What puts this link's frames on the wire, detached from the handle.
    fn egress(&self) -> Egress {
        Egress {
            shared: Arc::clone(&self.shared),
            outbound: self.outbound.clone(),
        }
    }

    /// Room for one frame in the driver's queue, reserved: what is done with
    /// it afterwards cannot wait, so a state change made with a permit in hand
    /// and the frame that reports it are one step.
    ///
    /// Cancel-safe: a future dropped while waiting has changed nothing.
    async fn permit(&self) -> Result<mpsc::Permit<'_, Outbound>> {
        self.outbound
            .reserve()
            .await
            .map_err(|_| Error::ConnectionGone)
    }

    /// Changes the state a `flow` reports and queues the `flow`, with nothing
    /// that can be dropped between the two.
    ///
    /// Every `flow` carries the session's three mandatory fields whether or
    /// not it names a handle, so a link-level `flow` refreshes the session
    /// too — which is also why an `echo` asking for session state is
    /// satisfied by one. A frame that cannot be encoded puts the state back.
    async fn change_and_flow(
        &self,
        echo: bool,
        change: impl FnOnce(&mut Credit, &mut Windows),
    ) -> Result<()> {
        let permit = self.permit().await?;
        // One order wherever these are held together: credit, the map, the
        // window.
        let mut credit = self.shared.credit.lock().expect("not poisoned");
        let mut windows = self.shared.session.windows.lock().expect("not poisoned");
        let (was_credit, was_windows) = (*credit, *windows);
        change(&mut credit, &mut windows);
        let flow = flow_frame(&windows, &credit, self.shared.output_handle, echo);
        match self.frame(Performative::Flow(flow), &[]) {
            Ok(bytes) => {
                permit.send(Outbound::Frame(bytes));
                Ok(())
            }
            Err(error) => {
                *credit = was_credit;
                *windows = was_windows;
                Err(error)
            }
        }
    }

    /// Encodes one frame: a performative and, for a `transfer`, the payload
    /// that follows it in the same body.
    fn frame(&self, performative: Performative<'_>, payload: &[u8]) -> Result<Vec<u8>> {
        encode(&self.shared, performative, payload)
    }

    /// Detaches the link, destroying the endpoint at both ends.
    ///
    /// Idempotent. Cancel-safe: the link is `Detaching` only once the
    /// `detach` is queued, so a dropped call leaves it attached.
    pub async fn detach(&self) -> Result<()> {
        self.detach_with(true, None).await
    }

    /// Detaches the link with an error.
    ///
    /// An errored endpoint is destroyed and **its handle is never reused for
    /// the life of the session**: the peer is entitled to still be sending
    /// frames for it, and a reused handle would deliver them to a live link.
    ///
    /// Cancel-safe, as [`Link::detach`] says.
    pub async fn detach_with(&self, closed: bool, error: Option<Condition>) -> Result<()> {
        let ended =
            |state: &LinkState| matches!(state, LinkState::Detaching | LinkState::Detached(_));
        if ended(&self.state()) {
            return Ok(());
        }
        let codec = error.as_ref().map(Condition::as_codec);
        let bytes = self.frame(
            Performative::Detach(Detach {
                handle: self.shared.output_handle,
                closed,
                error: codec,
            }),
            &[],
        )?;
        let permit = self.permit().await?;
        let mut state = self.shared.state.lock().expect("not poisoned");
        if ended(&state) {
            return Ok(());
        }
        *state = LinkState::Detaching;
        permit.send(Outbound::Frame(bytes));
        Ok(())
    }
}

/// Refuses to keep waiting on a link that has gone.
fn usable(shared: &Shared) -> Result<()> {
    let state = shared.state.lock().expect("not poisoned");
    if state.is_usable() {
        return Ok(());
    }
    Err(match &*state {
        LinkState::Detached(Some(condition)) => Error::Closed(Some(condition.clone())),
        _ => Error::ConnectionGone,
    })
}

/// Encodes one frame of a link: a performative and, for a `transfer`, the
/// payload that follows it in the same body.
fn encode(shared: &Shared, performative: Performative<'_>, payload: &[u8]) -> Result<Vec<u8>> {
    // Sized for the whole frame up front, so that building it never
    // reallocates and the payload is copied exactly once.
    let mut bytes = Vec::with_capacity(FRAME_HEADER + FRAME_PERFORMATIVE_ROOM + payload.len());
    weida_amqp_codec::frame::write(
        &mut bytes,
        weida_amqp_codec::frame::FrameKind::Amqp,
        shared.channel,
        shared.max_frame_size,
        |body| {
            performative.encode(body)?;
            body.extend_from_slice(payload);
            Ok(())
        },
    )?;
    Ok(bytes)
}

/// What puts one link's frames on the wire, apart from the handle that asked
/// for them.
///
/// A delivery's way out can outlive the future that began it: if that future
/// is dropped with a message half sent, the frame that closes the delivery
/// has to be written by something that is not the caller. So everything a
/// frame needs is held here by value, and a clone can be handed to a task.
#[derive(Clone)]
struct Egress {
    shared: Arc<Shared>,
    outbound: mpsc::Sender<Outbound>,
}

impl Egress {
    fn frame(&self, performative: Performative<'_>, payload: &[u8]) -> Result<Vec<u8>> {
        encode(&self.shared, performative, payload)
    }

    /// Waits until `commit` can put a frame on the wire, and puts it there.
    ///
    /// `commit` is offered a place in the driver's queue whenever the link
    /// may be ready. It answers `Ok(Some(..))` where it took what it needed
    /// and used the place, `Ok(None)` where the credit, the window or the room
    /// it wants is not there yet, and `Err` where the link cannot send at
    /// all. It never waits, so what it takes and the frame it queues are one
    /// step: a dropped future loses a wait and never half of a commit.
    ///
    /// Registered with the waker *before* the state is read, so a `flow` or a
    /// `disposition` landing between the two is not a lost wake-up.
    async fn queue<T>(
        &self,
        mut commit: impl FnMut(mpsc::Permit<'_, Outbound>) -> Result<Option<T>>,
    ) -> Result<T> {
        loop {
            let notified = self.shared.flow.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            usable(&self.shared)?;
            let permit = self
                .outbound
                .reserve()
                .await
                .map_err(|_| Error::ConnectionGone)?;
            if let Some(done) = commit(permit)? {
                return Ok(done);
            }
            notified.await;
        }
    }

    /// The first frame of a delivery. Takes, in one step with queueing it,
    /// one unit of link credit for the message, a place in the unsettled map
    /// where the delivery is not settled on send, and the first unit of
    /// session window. Reports the delivery-id, which is the transfer-id of
    /// this frame.
    ///
    /// Two things can be exhausted besides the window: the credit the
    /// receiver granted, and the unsettled map this end keeps. The map only
    /// matters for an unsettled delivery — a settled one is recorded nowhere
    /// — and is a bound of ours, because a receiver that keeps granting
    /// credit while settling nothing, or a sender that never reads the
    /// answers, would otherwise grow it without limit
    /// ([`crate::settlement::DEFAULT_MAX_UNSETTLED`]).
    async fn first(
        &self,
        settled: bool,
        tag: &[u8; TAG_LEN],
        fragment: &[u8],
        more: bool,
    ) -> Result<u32> {
        self.queue(|permit| {
            let mut credit = self.shared.credit.lock().expect("not poisoned");
            let mut unsettled = self.shared.unsettled.lock().expect("not poisoned");
            let mut windows = self.shared.session.windows.lock().expect("not poisoned");
            if !credit.may_send() || !(settled || unsettled.has_room()) || !windows.may_send() {
                return Ok(None);
            }
            // A delivery's id is the transfer-id of its first frame. The
            // specification never states the relation between the two
            // sequences; every implementation derives both from the one
            // counter, and a peer that expected otherwise would have no way
            // to say so.
            let delivery_id = windows.next_outgoing_id;
            let mut transfer = Transfer::new(self.shared.output_handle);
            transfer.delivery_id = Some(delivery_id);
            transfer.delivery_tag = Some(tag);
            transfer.message_format = Some(0);
            transfer.settled = Some(settled);
            transfer.more = more;
            // Built before anything is taken: a frame that cannot be encoded
            // leaves every count where it was.
            let bytes = self.frame(Performative::Transfer(transfer), fragment)?;
            let reserved = windows.reserve();
            debug_assert_eq!(reserved, Some(delivery_id));
            credit.record_sent();
            if !settled {
                // Unsettled: this end keeps state for the delivery until the
                // receiver's `disposition` retires it and the application has
                // taken the answer. A settled transfer is recorded nowhere,
                // because the sender has already forgotten it — which is what
                // "settled on send" means and why it proves nothing about the
                // message.
                unsettled.insert(delivery_id, tag);
            }
            permit.send(Outbound::Frame(bytes));
            Ok(Some(delivery_id))
        })
        .await
    }

    /// A later frame of an open delivery: the next unit of session window,
    /// taken in one step with queueing the frame.
    async fn next(&self, fragment: &[u8], more: bool) -> Result<()> {
        self.queue(|permit| {
            let mut windows = self.shared.session.windows.lock().expect("not poisoned");
            if !windows.may_send() {
                return Ok(None);
            }
            let mut transfer = Transfer::new(self.shared.output_handle);
            transfer.more = more;
            let bytes = self.frame(Performative::Transfer(transfer), fragment)?;
            windows.reserve();
            permit.send(Outbound::Frame(bytes));
            Ok(Some(()))
        })
        .await
    }

    /// Tells the receiver to discard a delivery this end could not finish. A
    /// `transfer` like any other, so it waits for window like any other.
    async fn abort(&self, delivery_id: u32, tag: &[u8; TAG_LEN]) -> Result<()> {
        self.queue(|permit| {
            let mut windows = self.shared.session.windows.lock().expect("not poisoned");
            if !windows.may_send() {
                return Ok(None);
            }
            let mut transfer = Transfer::new(self.shared.output_handle);
            transfer.delivery_id = Some(delivery_id);
            transfer.delivery_tag = Some(tag);
            transfer.aborted = true;
            let bytes = self.frame(Performative::Transfer(transfer), &[])?;
            windows.reserve();
            permit.send(Outbound::Frame(bytes));
            Ok(Some(()))
        })
        .await
    }
}

/// A delivery of several frames between its first frame and its last.
///
/// Half a delivery on the wire is worse than none, and a future can be
/// dropped at any wait in between. Dropping this before `closed` is set is
/// what closes the delivery: this end forgets it — "an aborted delivery is
/// implicitly settled" (Part 2 §2.7.5) — and a task writes `transfer(aborted)`
/// so the receiver discards what it holds, as soon as the window has room for
/// the frame. The task holds the link's turn until it has, so the next
/// delivery cannot start in the middle of it.
struct Open {
    egress: Egress,
    exec: Exec,
    delivery_id: u32,
    tag: [u8; TAG_LEN],
    settled: bool,
    /// The link's send lock, held until the delivery is closed on the wire.
    turn: Option<OwnedMutexGuard<()>>,
    closed: bool,
}

impl Drop for Open {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let shared = &self.egress.shared;
        if !self.settled
            && let Ok(mut map) = shared.unsettled.lock()
        {
            map.settle_here(self.delivery_id);
        }
        // The place is free, and a sender parked on it can go.
        shared.flow.notify_waiters();
        // A link that is gone has nobody to tell: the peer destroyed what it
        // held with the endpoint.
        if usable(shared).is_err() {
            return;
        }
        let egress = self.egress.clone();
        let (delivery_id, tag) = (self.delivery_id, self.tag);
        let turn = self.turn.take();
        self.exec.spawn(async move {
            let _turn = turn;
            if let Err(error) = egress.abort(delivery_id, &tag).await {
                tracing::debug!(delivery_id, %error, "a dropped send could not close its delivery");
            }
        });
    }
}

/// One session's links, in the driver.
#[derive(Debug, Default)]
pub(crate) struct Links {
    by_output: HashMap<u32, Entry>,
    /// The peer's handle to ours. Two spaces, because the two ends choose
    /// independently.
    input_to_output: HashMap<u32, u32>,
    /// Handles that were detached with an error, never reused for the life
    /// of the session.
    errored: HashSet<u32>,
    /// The **peer's** handles for links cleanly detached on this session. A
    /// frame naming one is a race with the `detach` and is discarded.
    detached: HashSet<u32>,
    /// The **peer's** handles for links detached with an error. Part 2 §2.6.5
    /// requires later input on one to end the session with
    /// `amqp:session:errant-link`, which is the opposite treatment.
    errant: HashSet<u32>,
    /// The subset of `errant` this end detached and the peer has not
    /// answered yet. Until its answering `detach` arrives the peer does not
    /// know, and its `flow` or `transfer` for the link may already be on the
    /// wire: that is a crossing, not later input, and is discarded. Once the
    /// answer lands, the strict rule applies.
    unanswered: HashSet<u32>,
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) shared: Arc<Shared>,
    pub(crate) events: queue::Sender<LinkEvent>,
    pub(crate) pending: Option<oneshot::Sender<Result<Link>>>,
    pub(crate) outbound: mpsc::Sender<crate::connection::Outbound>,
    pub(crate) rx: Option<queue::Receiver<LinkEvent>>,
    /// What we asked for, kept so that the answering `attach` can be read
    /// against it: the settle modes we own are ours and the ones we do not
    /// are the peer's, and only this tells them apart.
    pub(crate) options: Box<LinkOptions>,
    /// The delivery being reassembled, bounded by the `max-message-size`
    /// **this** end advertised: the bound is ours to enforce, because it is
    /// our buffer the frames land in.
    pub(crate) reassembly: Reassembly,
    /// Whether any frame of the delivery being received said it was settled.
    ///
    /// "For subsequent transfers in a multi-transfer delivery if the settled
    /// flag is left unset then it MUST be interpreted as true if and only if
    /// the value of the settled flag on any of the preceding transfers for
    /// the delivery is true" (Part 2 §2.7.5), so the last frame's flag alone
    /// does not say whether the delivery is settled.
    pub(crate) delivery_settled: bool,
}

/// What one incoming frame for a link amounts to.
#[derive(Debug)]
pub(crate) enum Incoming {
    /// Accounted for, with nothing for the application yet.
    Nothing,
    /// A whole message.
    Delivery(Delivery),
    /// The link must be detached with this condition, and its handle is
    /// poisoned for the life of the session.
    Refused(Condition),
}

impl Entry {
    /// One `transfer` frame for this link.
    ///
    /// Four things happen here and the order matters. The delivery's identity
    /// is checked. The receiver's credit is spent, once per delivery rather
    /// than once per frame, and a delivery that no credit this end ever
    /// granted covers is refused: the bound on what a sender can make this
    /// end hold is the credit it granted itself. The unsettled map is
    /// checked for room, because a delivery handed to the application that
    /// could not be recorded could never be acknowledged. And the payload is
    /// taken **under the `max-message-size` bound**.
    ///
    /// A delivery that fits one frame, which is nearly all of them, is handed
    /// on as a slice of the frame it arrived in: nothing is reassembled and
    /// nothing is copied. Only one that takes several frames is gathered in
    /// a buffer of its own.
    pub(crate) fn accept_transfer(&mut self, transfer: &Transfer<'_>, payload: Bytes) -> Incoming {
        if self.shared.role == Role::Sender {
            // A sending link has no incoming deliveries; the peer has the
            // roles the wrong way round and there is nothing to reassemble.
            return Incoming::Refused(Condition::described(
                condition::NOT_ALLOWED,
                format!(
                    "a transfer arrived on link {}, which this end attached as the sender",
                    self.shared.name
                ),
            ));
        }

        // A tag that differs from the one being reassembled is a *second*
        // delivery arriving before the first finished, which Part 2 §2.6.14
        // forbids outright: "deliveries on one link MUST NOT interleave".
        // Reported here rather than left to the payload check, because the
        // octets of two messages concatenated are not a message.
        let in_progress = self.reassembly.in_progress();
        let interleaving = in_progress
            && transfer
                .delivery_tag
                .is_some_and(|tag| self.reassembly.delivery_tag() != Some(tag));
        if interleaving {
            return Incoming::Refused(Condition::described(
                condition::NOT_ALLOWED,
                format!(
                    "a second delivery began on link {} before the first was complete",
                    self.shared.name
                ),
            ));
        }

        if !in_progress {
            // The first frame of a delivery is where its credit and its room
            // are spent.
            if !self
                .shared
                .credit
                .lock()
                .expect("not poisoned")
                .record_received()
            {
                // "The sender MUST NOT exceed the delivery-limit", and the
                // receiver may handle the excess or detach with
                // `amqp:link:transfer-limit-exceeded`. Past every limit this
                // end ever announced there is nothing to handle it against:
                // taking it would make the number of deliveries a link holds
                // the sender's choice and not the receiver's.
                return Incoming::Refused(Condition::described(
                    condition::LINK_TRANSFER_LIMIT_EXCEEDED,
                    format!(
                        "a delivery arrived on link {} past every limit this end had granted",
                        self.shared.name
                    ),
                ));
            }
            self.delivery_settled = false;
            if transfer.settled != Some(true)
                && !self
                    .shared
                    .unsettled
                    .lock()
                    .expect("not poisoned")
                    .has_room()
            {
                return Incoming::Refused(Condition::described(
                    condition::RESOURCE_LIMIT_EXCEEDED,
                    format!(
                        "link {} holds as many unsettled deliveries as it can keep state for, \
                         and another arrived",
                        self.shared.name
                    ),
                ));
            }
        }
        if transfer.settled == Some(true) {
            self.delivery_settled = true;
        }

        // One frame is the whole delivery: no reassembly, and the payload is
        // the frame's own octets.
        if !in_progress && !transfer.more && !transfer.aborted {
            let max = self.reassembly.max_message_size();
            let size = payload.len() as u64;
            if max != 0 && size > max {
                return Incoming::Refused(Condition::described(
                    condition::LINK_MESSAGE_SIZE_EXCEEDED,
                    DecodeError::MessageTooLarge { size, max }.to_string(),
                ));
            }
            return self.complete(
                transfer.delivery_id.unwrap_or_default(),
                transfer.delivery_tag.unwrap_or_default(),
                transfer.message_format.unwrap_or_default(),
                payload,
            );
        }

        match self.reassembly.accept(transfer, &payload) {
            Ok(Progress::Incomplete { .. }) => Incoming::Nothing,
            Ok(Progress::Aborted) => {
                // "Discard everything transferred for this delivery"; it is
                // implicitly settled, so there is nothing to answer either.
                tracing::debug!(link = self.shared.name, "the sender aborted a delivery");
                self.delivery_settled = false;
                Incoming::Nothing
            }
            Ok(Progress::Complete) => {
                let delivery_id = self.reassembly.delivery_id().unwrap_or_default();
                let format = self.reassembly.message_format().unwrap_or_default();
                let mut tag = [0u8; MAX_TAG];
                let held = self.reassembly.delivery_tag().unwrap_or_default();
                let len = held.len().min(MAX_TAG);
                tag[..len].copy_from_slice(&held[..len]);
                let payload = Bytes::from(self.reassembly.take());
                self.complete(delivery_id, &tag[..len], format, payload)
            }
            Err(error) => {
                // `MessageTooLarge` is the one that matters: the codec
                // refused it before the buffer grew, and
                // `amqp:link:message-size-exceeded` is the condition that
                // exists so a receiver can say no.
                let condition = if matches!(error, DecodeError::MessageTooLarge { .. }) {
                    condition::LINK_MESSAGE_SIZE_EXCEEDED
                } else {
                    condition::NOT_ALLOWED
                };
                self.reassembly.reset();
                self.delivery_settled = false;
                Incoming::Refused(Condition::described(condition, error.to_string()))
            }
        }
    }

    /// A delivery all of whose octets are in: recorded where it has to be, and
    /// handed to the application.
    fn complete(
        &mut self,
        delivery_id: u32,
        tag: &[u8],
        message_format: u32,
        payload: Bytes,
    ) -> Incoming {
        let settled = std::mem::take(&mut self.delivery_settled);
        // The sender has not settled, so this end keeps state for the
        // delivery until the application says what happened. One that
        // arrived settled is recorded nowhere: there is nothing to answer
        // and nothing the answer could change.
        if !settled
            && !self
                .shared
                .unsettled
                .lock()
                .expect("not poisoned")
                .insert(delivery_id, tag)
        {
            return Incoming::Refused(Condition::described(
                condition::RESOURCE_LIMIT_EXCEEDED,
                format!(
                    "link {} could not record delivery {delivery_id}: its unsettled map is full",
                    self.shared.name
                ),
            ));
        }
        Incoming::Delivery(Delivery::new(
            delivery_id,
            tag.to_vec(),
            message_format,
            settled,
            self.shared.output_handle,
            payload,
        ))
    }

    /// The peer's `flow` for this link.
    ///
    /// Returns the `flow` this end owes in answer, which is owed in two
    /// cases: an `echo` asked for our state, or a `drain` did — a drained
    /// sender MUST report, and a sender with nothing available MUST advance
    /// `delivery-count` until `link-credit` is zero first.
    pub(crate) fn accept_flow(
        &mut self,
        flow: &Flow<'_>,
        windows: &Windows,
    ) -> Option<Flow<'static>> {
        let mut credit = self.shared.credit.lock().expect("not poisoned");
        let mut owed = flow.echo;
        match self.shared.role {
            Role::Sender => {
                credit.apply_grant(flow.delivery_count, flow.link_credit, flow.drain);
                if flow.drain {
                    credit.consume_for_drain();
                    owed = true;
                }
            }
            Role::Receiver => credit.apply_sender_state(flow.delivery_count, flow.available),
        }
        let snapshot = *credit;
        drop(credit);
        // The waker before the event: a sender parked on credit should be
        // running again whether or not anything is reading the events.
        self.shared.flow.notify_waiters();
        // Never with `echo` set: answering an echo with an echo loops
        // forever, and the specification says so.
        owed.then(|| flow_frame(windows, &snapshot, self.shared.output_handle, false))
    }

    /// The credit state, for the event the application sees.
    pub(crate) fn credit(&self) -> Credit {
        *self.shared.credit.lock().expect("not poisoned")
    }

    /// Wakes anything parked on this link because the session window moved.
    pub(crate) fn wake(&self) {
        self.shared.flow.notify_waiters();
    }

    /// Applies a `disposition` covering `first..=last` to this link.
    ///
    /// Returns what each delivery amounted to, paired with its id, so that
    /// the driver can answer and report without holding the map. Nothing is
    /// allocated where this link holds none of the ids, which is most of the
    /// links a session-wide `disposition` is offered to.
    pub(crate) fn accept_disposition(
        &mut self,
        first: u32,
        last: u32,
        outcome: Option<&Outcome>,
        settled: bool,
    ) -> Vec<(u32, Settlement)> {
        let mut map = self.shared.unsettled.lock().expect("not poisoned");
        let covered = map.ids_in(first, last);
        if covered.is_empty() {
            return Vec::new();
        }
        let applied = covered
            .into_iter()
            .map(|id| {
                let settlement = map.record_theirs(id, outcome, settled);
                (id, settlement)
            })
            .collect::<Vec<_>>();
        drop(map);
        // The map may have made room, and a sender parked on it is parked on
        // the same waker as one parked on credit.
        self.shared.flow.notify_waiters();
        applied
    }

    /// Whether this link is the end a `disposition` with this role is about.
    ///
    /// "One `disposition` MAY cover deliveries from many links of the session
    /// provided they all have this role" — the role names the *speaker*, so a
    /// receiver's `disposition` is about deliveries this end sent.
    pub(crate) fn addressed_by(&self, role: Role) -> bool {
        match role {
            Role::Receiver => self.shared.role == Role::Sender,
            Role::Sender => self.shared.role == Role::Receiver,
        }
    }

    /// Whether this end must settle before the peer's outcome is final.
    pub(crate) fn second_mode(&self) -> bool {
        matches!(
            self.shared
                .negotiated
                .lock()
                .expect("not poisoned")
                .as_ref()
                .map(|n| n.rcv_settle_mode),
            Some(ReceiverSettleMode::Second)
        )
    }

    /// Ends a delivery at this end after the peer's outcome for it is final.
    /// A sender keeps the delivery, finished, until the answer is taken.
    pub(crate) fn finish_here(&self, delivery_id: u32) {
        self.shared
            .unsettled
            .lock()
            .expect("not poisoned")
            .finish_here(delivery_id);
    }
}

impl Links {
    /// The lowest free output handle, skipping any that was errored.
    ///
    /// `handle_max` is the *peer's*, because it states the highest handle its
    /// sender will accept. Lowest-free is what Part 2 §2.6.2 recommends and
    /// is what keeps the table dense.
    pub(crate) fn lowest_free(&self, handle_max: u32) -> Option<u32> {
        (0..=handle_max)
            .find(|handle| !self.by_output.contains_key(handle) && !self.errored.contains(handle))
    }

    /// The link of this name and direction that a second `attach` would
    /// steal.
    ///
    /// The uniqueness scope of Part 2 §2.6.1: among links of the *same
    /// direction* between the two containers, so a sender and a receiver may
    /// share a name.
    ///
    /// A link this end is already **detaching** is not a candidate. It is on
    /// its way out, its name is being given up, and stealing it would send
    /// `amqp:link:stolen` for a link the application closed itself and poison
    /// the handle for the rest of the session. The case is not hypothetical:
    /// a peer that does not answer a `detach` until its own application lets
    /// go — `fe2o3-amqp`'s acceptor is one — leaves the entry in this table
    /// for exactly as long as it takes a client to detach and re-attach the
    /// same name.
    pub(crate) fn by_name(&self, name: &str, role: Role) -> Option<u32> {
        self.by_output
            .iter()
            .find(|(_, entry)| {
                entry.shared.name == name
                    && entry.shared.role == role
                    && !matches!(
                        *entry.shared.state.lock().expect("not poisoned"),
                        LinkState::Detaching | LinkState::Detached(_)
                    )
            })
            .map(|(handle, _)| *handle)
    }

    pub(crate) fn insert(&mut self, handle: u32, entry: Entry) {
        self.by_output.insert(handle, entry);
    }

    pub(crate) fn get_mut(&mut self, output: u32) -> Option<&mut Entry> {
        self.by_output.get_mut(&output)
    }

    /// The link a frame carrying the peer's handle belongs to.
    pub(crate) fn by_input(&mut self, input: u32) -> Option<&mut Entry> {
        let output = *self.input_to_output.get(&input)?;
        self.by_output.get_mut(&output)
    }

    /// Whether the peer's handle is already mapped, which is what
    /// `amqp:session:handle-in-use` is about.
    pub(crate) fn input_in_use(&self, input: u32) -> bool {
        self.input_to_output.contains_key(&input)
    }

    pub(crate) fn map_input(&mut self, input: u32, output: u32) {
        // A handle the peer attaches anew is no longer the errored link's:
        // the peer may reuse it once both ends detached (Part 2 §2.6.4).
        self.errant.remove(&input);
        self.input_to_output.insert(input, output);
    }

    /// Forgets a link. `errored` poisons its handle for the life of the
    /// session.
    ///
    /// The **peer's** handle is remembered either way, because a frame for a
    /// link that has just gone is a race and not a fault: see
    /// [`Links::was_detached`].
    pub(crate) fn remove(&mut self, output: u32, errored: bool) -> Option<Entry> {
        let inputs: Vec<u32> = self
            .input_to_output
            .iter()
            .filter(|(_, mapped)| **mapped == output)
            .map(|(input, _)| *input)
            .collect();
        for input in inputs {
            self.input_to_output.remove(&input);
            if errored {
                self.errant.insert(input);
            } else {
                self.detached.insert(input);
            }
        }
        if errored {
            self.errored.insert(output);
        }
        self.by_output.remove(&output)
    }

    /// Whether the peer's handle names a link that was attached on this
    /// session and has since been **cleanly** detached.
    ///
    /// A frame naming such a handle is a race, not a fault: the peer's
    /// `flow`, `transfer` or `disposition` may already have been in flight
    /// when the `detach` crossed it. Ending the session over one would make
    /// every orderly link close a coin toss, and `fe2o3-amqp`'s acceptor
    /// really does send a `flow` for a handle it has just detached.
    pub(crate) fn was_detached(&self, input: u32) -> bool {
        self.detached.contains(&input) || self.unanswered.contains(&input)
    }

    /// Whether the peer's handle names a link that was detached **with an
    /// error**.
    ///
    /// Part 2 §2.6.5 is explicit about this one and it is the opposite case:
    /// "any later input on that handle or its delivery-ids MUST end the
    /// session with `amqp:session:errant-link`".
    pub(crate) fn was_errant(&self, input: u32) -> bool {
        self.errant.contains(&input)
    }

    /// Marks the peer's handles of a link this end is detaching with an
    /// error as not yet answered: see [`Links::unanswered`]. Called with
    /// [`Links::inputs_of`] taken before [`Links::remove`] forgets them.
    pub(crate) fn await_answer(&mut self, inputs: &[u32]) {
        self.unanswered.extend(inputs.iter().copied());
    }

    /// The peer answered the `detach` of its handle `input`: from now on
    /// input on it is errant, or it is the peer's again to attach anew.
    /// `true` when the handle was waiting for that answer.
    pub(crate) fn answered(&mut self, input: u32) -> bool {
        self.unanswered.remove(&input)
    }

    /// The peer's handles mapped to the link `output`.
    pub(crate) fn inputs_of(&self, output: u32) -> Vec<u32> {
        self.input_to_output
            .iter()
            .filter(|(_, mapped)| **mapped == output)
            .map(|(input, _)| *input)
            .collect()
    }

    /// Wakes everything parked on any link of this session, which is what a
    /// session-level `flow` amounts to: the window may have room now.
    pub(crate) fn wake_all(&self) {
        for entry in self.by_output.values() {
            entry.wake();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.by_output.len()
    }

    pub(crate) fn drain(&mut self) -> Vec<Entry> {
        self.input_to_output.clear();
        self.by_output.drain().map(|(_, entry)| entry).collect()
    }

    /// Every link a `disposition` spoken by `role` is about.
    ///
    /// The role names the speaker, so a receiver's `disposition` addresses
    /// this end's *senders*. One frame covering many links of a session is
    /// legal and is the point of the range, which is why this is an iterator
    /// and not a lookup.
    pub(crate) fn addressed(&mut self, role: Role) -> impl Iterator<Item = &mut Entry> {
        self.by_output
            .values_mut()
            .filter(move |entry| entry.addressed_by(role))
    }
}

/// Reads the answering `attach` into what was actually negotiated.
///
/// The settle modes are not a handshake where both sides must agree on one
/// value: each field describes the behaviour of *one* end, so the
/// authoritative value comes from whichever `attach` was sent by the end the
/// field is about (Part 2 §2.7.3). This client keeps both and reports them,
/// because the peer may have narrowed what we asked for and only the
/// application can decide whether that is acceptable.
pub(crate) fn negotiate(ours: &LinkOptions, theirs: &Attach<'_>) -> Result<Negotiated> {
    let (snd_settle_mode, rcv_settle_mode) = match ours.role {
        // We are the sender, so snd-settle-mode is ours and rcv-settle-mode
        // is the peer's.
        Role::Sender => (ours.snd_settle_mode, theirs.rcv_settle_mode),
        // We are the receiver, so it is the other way round.
        Role::Receiver => (theirs.snd_settle_mode, ours.rcv_settle_mode),
    };
    Ok(Negotiated {
        snd_settle_mode,
        rcv_settle_mode,
        remote_source: match theirs.source.clone() {
            Some(value) => Some(Source::from_value(value)?),
            None => None,
        },
        remote_target: match theirs.target.clone() {
            Some(value) => Some(Target::from_value(value)?),
            None => None,
        },
        remote_max_message_size: theirs.max_message_size.filter(|size| *size != 0),
        remote_offered_capabilities: theirs
            .offered_capabilities
            .iter()
            .map(str::to_owned)
            .collect(),
    })
}

/// Builds this client's `attach` for a link on `handle`.
pub(crate) fn attach_frame(channel: u16, handle: u32, options: &LinkOptions) -> Result<Vec<u8>> {
    let attach = options.to_attach(handle);
    let mut bytes = Vec::new();
    weida_amqp_codec::frame::write(
        &mut bytes,
        weida_amqp_codec::frame::FrameKind::Amqp,
        channel,
        u32::MAX,
        |body| Performative::Attach(attach).encode(body),
    )?;
    Ok(bytes)
}

/// Builds a `flow` carrying this link's credit and its session's windows.
///
/// Every `flow` carries `incoming-window`, `next-outgoing-id` and
/// `outgoing-window` whether or not it names a handle — they are mandatory —
/// so a link-level `flow` refreshes the session state too, and an `echo`
/// asking for session state is answered by one.
pub(crate) fn flow_frame(
    windows: &Windows,
    credit: &Credit,
    handle: u32,
    echo: bool,
) -> Flow<'static> {
    Flow {
        next_incoming_id: windows.wire_next_incoming_id(),
        incoming_window: windows.incoming_window,
        next_outgoing_id: windows.next_outgoing_id,
        outgoing_window: windows.outgoing_window,
        handle: Some(handle),
        delivery_count: credit.wire_delivery_count(),
        // A receiver states the credit it grants; a sender states what it has
        // left, which is the same number read from the other side.
        link_credit: Some(credit.link_credit()),
        available: Some(credit.available()),
        drain: credit.drain(),
        echo,
        properties: None,
    }
}

/// Eight octets: `SIZE`, `DOFF`, `TYPE` and two type-specific bytes.
const FRAME_HEADER: usize = 8;

/// Room reserved in a frame buffer for the performative ahead of the payload.
///
/// A `transfer` carrying its identity is the largest performative a link
/// writes with a payload and is about thirty octets; the rest are shorter.
/// A guess that is a little high costs a few spare octets, and one that is
/// low costs one reallocation, so it is not measured per frame.
const FRAME_PERFORMATIVE_ROOM: usize = 64;

/// What a typical message encodes to, reserved up front so that encoding one
/// rarely reallocates. A larger message grows the buffer as it must.
const SEND_PAYLOAD_ROOM: usize = 256;

/// The delivery-tags this end chooses: a counter, eight octets.
const TAG_LEN: usize = 8;

/// How many payload octets fit in one frame of a delivery on `handle`.
///
/// Measured rather than guessed: the first frame's performative is the
/// largest one the delivery will carry — it is the frame with the id, the
/// tag and the format — so encoding it once and subtracting gives a budget
/// every frame of the delivery fits inside. It depends only on the handle,
/// the tag's length and the peer's frame ceiling, none of which change while
/// the link lives, so it is worked out when the link is attached.
pub(crate) fn frame_budget(handle: u32, max_frame_size: u32) -> Result<usize> {
    let tag = [0u8; TAG_LEN];
    let mut probe = Transfer::new(handle);
    probe.delivery_id = Some(u32::MAX);
    probe.delivery_tag = Some(&tag);
    probe.message_format = Some(0);
    probe.settled = Some(true);
    probe.more = true;
    let mut scratch = Vec::with_capacity(FRAME_PERFORMATIVE_ROOM);
    Performative::Transfer(probe).encode(&mut scratch)?;
    let ceiling = max_frame_size.max(weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE) as usize;
    Ok(ceiling.saturating_sub(FRAME_HEADER + scratch.len()).max(1))
}

/// The word for a role, for an error message a person has to read.
const fn role_name(role: Role) -> &'static str {
    match role {
        Role::Sender => "sender",
        Role::Receiver => "receiver",
    }
}

/// The condition a stolen link is closed with (Part 2 §2.8.18).
pub const STOLEN: &str = condition::LINK_STOLEN;

#[cfg(test)]
mod tests {
    use super::*;

    fn session_shared() -> Arc<crate::session::Shared> {
        Arc::new(crate::session::Shared {
            outgoing_channel: 0,
            incoming_channel: Mutex::new(Some(0)),
            windows: Mutex::new(Windows::new(0, 400, 400)),
            state: Mutex::new(crate::session::SessionState::Begun),
            options: crate::session::SessionOptions::default(),
        })
    }

    fn entry(name: &str, role: Role, handle: u32) -> Entry {
        let (events, rx) = events();
        let (outbound, _) = mpsc::channel(4);
        Entry {
            shared: Arc::new(Shared {
                name: name.to_owned(),
                role,
                output_handle: handle,
                channel: 0,
                state: Mutex::new(LinkState::Attaching),
                negotiated: Mutex::new(None),
                input_handle: Mutex::new(None),
                credit: Mutex::new(match role {
                    Role::Sender => Credit::sender(0),
                    Role::Receiver => Credit::receiver(),
                }),
                session: session_shared(),
                flow: Notify::new(),
                max_frame_size: weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE,
                frame_budget: 400,
                next_tag: Mutex::new(0),
                unsettled: Mutex::new(match role {
                    Role::Sender => Unsettled::for_sender(crate::settlement::DEFAULT_MAX_UNSETTLED),
                    Role::Receiver => Unsettled::new(crate::settlement::DEFAULT_MAX_UNSETTLED),
                }),
                send_lock: Arc::new(AsyncMutex::new(())),
            }),
            events,
            pending: None,
            outbound,
            rx: Some(rx),
            options: Box::new(LinkOptions::sender(name, Target::at("q"))),
            reassembly: Reassembly::new(DEFAULT_MAX_MESSAGE_SIZE),
            delivery_settled: false,
        }
    }

    #[test]
    fn handles_are_allocated_lowest_free() {
        let mut links = Links::default();
        assert_eq!(links.lowest_free(255), Some(0));
        links.insert(0, entry("a", Role::Sender, 0));
        links.insert(1, entry("b", Role::Sender, 1));
        assert_eq!(links.lowest_free(255), Some(2));
        // A cleanly closed link's handle is free again.
        links.remove(0, false);
        assert_eq!(links.lowest_free(255), Some(0));
        assert_eq!(links.len(), 1);
    }

    #[test]
    fn an_errored_handle_is_never_reused() {
        // The peer is entitled to still be sending frames for the dead link,
        // and a reused handle would deliver them to a live one.
        let mut links = Links::default();
        links.insert(0, entry("a", Role::Sender, 0));
        links.remove(0, true);
        assert_eq!(
            links.lowest_free(255),
            Some(1),
            "handle 0 is poisoned for the life of the session"
        );
        links.insert(1, entry("b", Role::Sender, 1));
        links.remove(1, false);
        assert_eq!(links.lowest_free(255), Some(1), "a clean close frees it");
    }

    #[test]
    fn frames_crossing_our_errored_detach_are_discarded_until_the_peer_answers() {
        // Found against fe2o3-amqp 0.18: our steal detached a link with
        // `amqp:link:stolen` while the peer's `flow` for it was on the wire,
        // and treating that `flow` as errant input ended the session.
        let mut links = Links::default();
        links.insert(0, entry("a", Role::Sender, 0));
        links.map_input(5, 0);
        let inputs = links.inputs_of(0);
        links.remove(0, true);
        links.await_answer(&inputs);
        assert!(links.was_detached(5), "a crossing frame is discarded");

        assert!(links.answered(5));
        assert!(!links.was_detached(5));
        assert!(
            links.was_errant(5),
            "after the peer's answer, input on the handle ends the session"
        );

        links.insert(1, entry("a", Role::Sender, 1));
        links.map_input(5, 1);
        assert!(
            !links.was_errant(5),
            "the peer may attach its handle anew once both ends detached"
        );
    }

    #[test]
    fn the_bound_on_the_table_is_the_peers_handle_max() {
        let mut links = Links::default();
        links.insert(0, entry("a", Role::Sender, 0));
        assert_eq!(links.lowest_free(1), Some(1));
        links.insert(1, entry("b", Role::Sender, 1));
        assert_eq!(
            links.lowest_free(1),
            None,
            "the peer's handle-max is what bounds what we may use"
        );
    }

    #[test]
    fn the_two_handle_spaces_are_separate() {
        let mut links = Links::default();
        links.insert(0, entry("orders", Role::Sender, 0));
        // The peer answered with its own handle 7.
        assert!(!links.input_in_use(7));
        links.map_input(7, 0);
        assert!(links.input_in_use(7));
        assert_eq!(links.by_input(7).map(|e| e.shared.output_handle), Some(0));
        assert!(
            links.by_input(0).is_none(),
            "our own output handle is not an input handle"
        );
        links.remove(0, false);
        assert!(!links.input_in_use(7));
    }

    #[test]
    fn a_name_is_unique_per_direction_and_not_per_connection() {
        // Part 2 §2.6.1 scopes uniqueness to links of the same direction, so
        // a sender and a receiver may share a name.
        let mut links = Links::default();
        links.insert(0, entry("orders", Role::Sender, 0));
        links.insert(1, entry("orders", Role::Receiver, 1));
        assert_eq!(links.by_name("orders", Role::Sender), Some(0));
        assert_eq!(links.by_name("orders", Role::Receiver), Some(1));
        assert_eq!(links.by_name("invoices", Role::Sender), None);
    }

    #[test]
    fn an_empty_link_name_is_refused_where_it_is_configured() {
        let mut options = LinkOptions::sender("", Target::at("q"));
        let error = options.validate().expect_err("refused");
        assert!(error.to_string().contains("steals"), "{error}");
        options.name = "orders".into();
        options.validate().expect("a named link is fine");
    }

    #[test]
    fn the_settle_modes_come_from_the_end_each_one_describes() {
        // Not a handshake on one value: snd-settle-mode describes the
        // sender's behaviour and rcv-settle-mode the receiver's, so each
        // comes from the end it is about.
        let mut ours = LinkOptions::sender("orders", Target::at("q"));
        ours.snd_settle_mode = SenderSettleMode::Unsettled;
        ours.rcv_settle_mode = ReceiverSettleMode::Second;

        let mut theirs = Attach::new("orders", 7, Role::Receiver);
        theirs.snd_settle_mode = SenderSettleMode::Mixed;
        // The broker refuses `second` and says `first`, which is what
        // Artemis does.
        theirs.rcv_settle_mode = ReceiverSettleMode::First;
        let their_target = Target::at("q");
        theirs.target = Some(their_target.to_value());

        let negotiated = negotiate(&ours, &theirs).expect("reads");
        assert_eq!(
            negotiated.snd_settle_mode,
            SenderSettleMode::Unsettled,
            "we are the sender, so snd-settle-mode is ours"
        );
        assert_eq!(
            negotiated.rcv_settle_mode,
            ReceiverSettleMode::First,
            "we are not the receiver, so rcv-settle-mode is the peer's - and \
             asking for Second does not make it so"
        );

        // And the other way round when we receive.
        let mut ours = LinkOptions::receiver("orders", Source::at("q"));
        ours.rcv_settle_mode = ReceiverSettleMode::Second;
        let mut theirs = Attach::new("orders", 7, Role::Sender);
        theirs.snd_settle_mode = SenderSettleMode::Settled;
        let their_source = Source::at("q");
        theirs.source = Some(their_source.to_value());
        let negotiated = negotiate(&ours, &theirs).expect("reads");
        assert_eq!(negotiated.snd_settle_mode, SenderSettleMode::Settled);
        assert_eq!(negotiated.rcv_settle_mode, ReceiverSettleMode::Second);
    }

    #[test]
    fn a_null_terminus_in_the_answer_is_a_refusal_and_not_an_error() {
        // "A partner that will not provide a terminus answers with that field
        // null, which establishes a link to a nonexistent terminus, and MUST
        // then immediately detach."
        let ours = LinkOptions::receiver("orders", Source::at("nope"));
        let theirs = Attach::new("orders", 7, Role::Sender);
        let negotiated = negotiate(&ours, &theirs).expect("reads");
        assert_eq!(negotiated.remote_source, None);
        assert_eq!(negotiated.remote_target, None);
    }

    #[test]
    fn a_zero_max_message_size_is_folded_into_no_limit() {
        // Zero and unset both mean unlimited, so no caller has to remember
        // the rule.
        let ours = LinkOptions::sender("orders", Target::at("q"));
        let mut theirs = Attach::new("orders", 7, Role::Receiver);
        theirs.max_message_size = Some(0);
        assert_eq!(
            negotiate(&ours, &theirs).unwrap().remote_max_message_size,
            None
        );
        theirs.max_message_size = Some(1024);
        assert_eq!(
            negotiate(&ours, &theirs).unwrap().remote_max_message_size,
            Some(1024)
        );
    }

    #[test]
    fn our_attach_sets_a_bounded_max_message_size() {
        let options = LinkOptions::sender("orders", Target::at("q"));
        assert_eq!(options.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
        let bytes = attach_frame(0, 3, &options).expect("encodes");
        let frame = weida_amqp_codec::frame::decode(&bytes, u32::MAX).expect("a frame");
        let (performative, _) =
            Performative::decode(frame.body, weida_amqp_codec::Limits::DEFAULT).unwrap();
        match performative {
            Performative::Attach(attach) => {
                assert_eq!(attach.name, "orders");
                assert_eq!(attach.handle, 3);
                assert_eq!(attach.role, Role::Sender);
                assert_eq!(attach.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
                assert_eq!(
                    attach.initial_delivery_count,
                    Some(0),
                    "MUST NOT be null for a sender"
                );
                assert!(attach.target.is_some());
            }
            other => panic!("expected attach, got {}", other.name()),
        }
    }
}
