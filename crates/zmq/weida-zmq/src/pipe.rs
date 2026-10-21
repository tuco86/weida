//! The per-peer queue — ZeroMQ's "pipe" — and what happens when it is full.
//!
//! **Every pattern RFC specifies the same object.** One queue per connected
//! peer, or a double queue, one per direction; created when an outgoing
//! connection is *initiated* and maintained "whether or not the connection is
//! established"; created when a peer connects; destroyed on that peer's
//! disconnect, "discarding any messages it contains"; with sizes constrained
//! "to a runtime-configurable limit" (`docs/research/zeromq.md` §2). That is
//! [`Pipe`], and the limit is `ZMQ_SNDHWM`/`ZMQ_RCVHWM`.
//!
//! **Credit is local and nobody grants it.** ZMTP 3.1 has no flow-control
//! signalling at all — credits are listed under "Topics for Discussion" — so
//! a high-water mark is purely a local bound on a local queue, applied per
//! peer, counted in *messages* rather than bytes (§5). It is also inexact in
//! libzmq: kernel socket buffers sit underneath it, "the actual limit may be
//! as much as 90% lower depending on the flow of messages", and for `inproc`
//! "the real HWM is the sum of both sides' configured HWMs" (§5, §11). What
//! is exact is the *behaviour at the bound*, which is what this module
//! implements and what its tests pin.
//!
//! **What happens at the bound is a property of the socket type**, from
//! `zmq_socket(3)`'s "Action in mute state" column: block, drop, or return
//! `EAGAIN` (§4.1). [`MuteAction::sending`] is that column, written once, so
//! that the socket types carry it rather than each re-deciding it at every
//! send site.
//!
//! # A queue is bounded twice: in messages, as libzmq is, and in bytes
//!
//! libzmq's high-water mark counts messages, and a message may be as large as
//! `ZMQ_MAXMSGSIZE` allows, so the memory one peer can occupy is the
//! **product** of the two — 1000 × 1 MiB per direction per peer at the
//! defaults. Stating the product is honest but it is not a bound
//! ([`crate::DEFAULT_MAX_MESSAGE_SIZE`]), so every queue carries
//! [`QueueConfig::max_bytes`] as well: [`DEFAULT_QUEUE_BYTES`], 8 MiB, per
//! direction per peer.
//!
//! The two bounds are **not** the same thing said twice. `max_message_size` is
//! a ceiling and not a size: dividing the byte budget by it would cut
//! libzmq's documented default from 1000 messages to 8 for every socket,
//! including the ones whose messages are forty bytes. Enforcing bytes where
//! the bytes actually are keeps `ZMQ_SNDHWM` behaving exactly as
//! `zmq_setsockopt(3)` says it does and refuses only the peer that occupies
//! the memory.
//!
//! **The bytes are accounted, not only counted.** A queued message is charged
//! its payload *plus* [`QUEUE_FRAME_OVERHEAD`] for every frame and
//! [`QUEUE_MESSAGE_OVERHEAD`] for the message ([`Queue::charge`]), because
//! what a peer makes the receiver hold is not what it sent: an empty frame is
//! two octets on the wire and a frame handle plus an allocation in memory. A
//! payload-only count sees a message of a thousand empty frames as zero
//! bytes, so only `ZMQ_SNDHWM` would bound the queue — at the product
//! `hwm × max_message_frames × frame size`, a number nobody states. With the
//! charge the same message is about 64 KiB and the ceiling admits 127 of them
//! instead of a thousand.
//!
//! **A queue always accepts one message**, however large, which is why the
//! byte ceiling applies only to a queue that is not already empty. Without
//! that rule a message above the ceiling could never be queued at all, and a
//! blocking socket type would wait for room that cannot appear — a deadlock
//! in place of a bound. The exposure per direction per peer is therefore
//! `max_bytes - 1` plus the charge of one message, which `max_message_size`
//! and `max_message_frames` bound: **about 9.1 MiB** at the defaults
//! (`8 MiB + 1 MiB + 1024 × 64 B + 64 B`).
//!
//! # Who is told when a queue changes
//!
//! A socket that receives from many peers must not park on one wait per peer:
//! that is a vector of futures built on every park and every one of them
//! polled on every wake, at a count a stranger chooses. Instead the engine
//! gives all of a socket's pipes the same two bells (`Pipe::attach`): one
//! rung after a message arrives from any peer, one rung after a message
//! leaves for any peer, and the socket parks on one of them. A bell carries no
//! message — it says "look again" — so the wait that uses it is allocation-free,
//! and it registers *before* it looks, so a message that lands between the
//! look and the sleep ends the wait instead of being slept through.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;
use weida_zmtp::SocketType;

use crate::error::{Error, Result};
use crate::message::Multipart;

/// Default `ZMQ_SNDHWM`: outstanding messages queued for one peer.
///
/// libzmq's default, and libzmq's unit — messages, not bytes
/// (`docs/research/zeromq.md` §5, §11). Zero means no limit, which is what
/// ZeroMQ 2.x had by default and "was easy but also typically fatal for
/// high-volume publishers" (§11); it is available and it is not the default.
pub const DEFAULT_SNDHWM: usize = 1000;

/// Default `ZMQ_RCVHWM`: the same bound on the receiving direction.
pub const DEFAULT_RCVHWM: usize = 1000;

/// Default byte ceiling on one direction of one peer's queue.
///
/// **libzmq has no such option**, and that is the reason this exists: its
/// high-water marks count messages (§5), so at the defaults a single peer
/// could hold 1000 × `ZMQ_MAXMSGSIZE` = 1 GiB per direction, and
/// "no remote input can cause unbounded memory allocation"
/// (`docs/INVARIANTS.md`) would be a statement about a product nobody
/// bounded.
///
/// 8 MiB matches the byte budget used by weida's own subscriber queues
/// (`Limits::subscriber_buffer_bytes`), while remaining a ZeroMQ-local
/// implementation bound. `0` means no ceiling, which is libzmq's behaviour
/// and is available for a caller who wants exactly it.
pub const DEFAULT_QUEUE_BYTES: u64 = 8 * 1024 * 1024;

/// What one frame costs a queue besides its bytes, in bytes.
///
/// A queue charges every message for the memory it **makes the receiver
/// hold**, and a frame is more than its payload: a handle in the message's
/// frame list (32 B at most) and, for a non-empty payload, an allocation with
/// the allocator's own header and rounding. 64 B covers both with room to
/// spare, so a million one-byte frames cost the queue 64 MB and not the 2 MB
/// they cost their sender on the wire — and a frame of no bytes costs
/// something at all, which is the case a payload-only count cannot see. See
/// [`Queue::charge`].
pub const QUEUE_FRAME_OVERHEAD: u64 = 64;

/// What one queued message costs besides its frames, in bytes: the message's
/// frame list, its slot in the queue and the bookkeeping that goes with them.
/// Charged once per message, so a flood of one-frame empty messages is bounded
/// by `max_bytes / (QUEUE_MESSAGE_OVERHEAD + QUEUE_FRAME_OVERHEAD)` even when
/// the high-water mark is off.
pub const QUEUE_MESSAGE_OVERHEAD: u64 = 64;

/// What a socket does when a peer's queue is full, or when it has no peer to
/// send to at all: `zmq_socket(3)`'s "Action in mute state".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MuteAction {
    /// Block until there is room. REQ, DEALER, PUSH, PULL, PAIR, CLIENT,
    /// CHANNEL and SCATTER, and the reason PUSH "SHALL NOT discard messages
    /// that it cannot send".
    Block,
    /// Discard the message silently and count it. PUB, XPUB, XSUB, ROUTER and
    /// RADIO: a publisher that blocked would let one slow subscriber stall
    /// every other one.
    Drop,
    /// Report `EAGAIN` and let the caller decide. SERVER, PEER and STREAM —
    /// the thread-safe family, which has no blocking send.
    Fail,
}

impl MuteAction {
    /// What `zmq_socket(3)`'s table says for `socket`, or `None` where the
    /// manual omits the row.
    ///
    /// The manual gives no mute-state action for REP, SUB, DISH or GATHER
    /// while giving one for PULL (`docs/research/zeromq.md` §4.1). That gap
    /// is reported rather than filled in: a socket type whose row is missing
    /// must state its own choice where it is implemented, and see
    /// [`MuteAction::receiving`] for the one receive-side rule the RFCs do
    /// state.
    pub const fn sending(socket: SocketType) -> Option<MuteAction> {
        match socket {
            SocketType::Req
            | SocketType::Dealer
            | SocketType::Push
            | SocketType::Pull
            | SocketType::Pair
            | SocketType::Client
            | SocketType::Channel
            | SocketType::Scatter => Some(MuteAction::Block),
            SocketType::Pub
            | SocketType::XPub
            | SocketType::XSub
            | SocketType::Router
            | SocketType::Radio => Some(MuteAction::Drop),
            SocketType::Server | SocketType::Peer => Some(MuteAction::Fail),
            SocketType::Rep | SocketType::Sub | SocketType::Dish | SocketType::Gather => None,
        }
    }

    /// What a *receiving* socket does when one publisher's queue is full.
    ///
    /// Only the pub/sub RFCs state a receive-side rule: a SUB or XSUB "SHALL
    /// silently discard messages if the queue for a publisher is full"
    /// (29/PUBSUB, `docs/research/zeromq.md` §5). Every other socket type
    /// gets `None`, which means the RFCs are silent and the receiving side is
    /// backpressure — the queue simply stops being drained, which is how a
    /// PULL slows a PUSH down.
    pub const fn receiving(socket: SocketType) -> Option<MuteAction> {
        match socket {
            SocketType::Sub | SocketType::XSub => Some(MuteAction::Drop),
            _ => None,
        }
    }
}

/// How one direction of a peer's pipe is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    /// `ZMQ_SNDHWM`/`ZMQ_RCVHWM` for this direction, in messages. `0` means
    /// no limit, as in libzmq.
    pub hwm: usize,
    /// The same direction's ceiling in bytes, which libzmq has no option for,
    /// counted as [`Queue::charge`] counts: payload plus
    /// [`QUEUE_FRAME_OVERHEAD`] per frame and [`QUEUE_MESSAGE_OVERHEAD`] per
    /// message. `0` means no ceiling. A queue that is empty accepts one
    /// message of any size regardless of this number, so the memory one peer
    /// occupies is bounded by `max_bytes - 1` plus one message's charge and
    /// never by this alone.
    pub max_bytes: u64,
    /// What to do at either bound.
    pub mute: MuteAction,
}

impl QueueConfig {
    /// The outgoing direction of a socket type, at libzmq's default HWM.
    ///
    /// Fails with `EINVAL` for a socket type whose mute-state row the manual
    /// omits: that choice belongs to the socket type, not to this
    /// constructor.
    pub const fn sending(socket: SocketType) -> Result<QueueConfig> {
        match MuteAction::sending(socket) {
            Some(mute) => Ok(QueueConfig {
                hwm: DEFAULT_SNDHWM,
                max_bytes: DEFAULT_QUEUE_BYTES,
                mute,
            }),
            None => Err(Error::EINVAL(std::borrow::Cow::Borrowed(
                "zmq_socket(3) states no mute-state action for this socket type; \
                 the socket must choose one explicitly",
            ))),
        }
    }
}

/// What a send did.
///
/// A drop is a return value rather than a silent `Ok`: `MuteAction::Drop` is
/// a documented outcome of `zmq_send` on a PUB socket, and a caller that
/// wants to count or log it must be able to see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Sent {
    /// The message is in the queue.
    Queued,
    /// The queue was full and this socket type drops rather than blocks. The
    /// message is gone; nothing will retry it.
    Dropped,
}

/// One direction of one peer's pipe: a bounded queue of whole messages.
///
/// Whole messages, because a message is delivered "all frames or none" (§3),
/// so a queue slot always holds a complete one. Bounded in messages because
/// that is libzmq's unit of credit (§5), **and** in bytes because a message
/// count is not a memory bound (module documentation).
pub struct Queue {
    config: QueueConfig,
    state: Mutex<QueueState>,
    /// Woken when a message is taken out, for a blocked sender.
    room: Notify,
    /// Woken when a message goes in, for a waiting receiver.
    ready: Notify,
    /// Rung after a message goes in, when the socket receives from this
    /// queue: the socket-wide bell its fair-queued receive parks on instead
    /// of on one wait per peer. Set once, by [`Pipe::attach`].
    on_push: OnceLock<Arc<Notify>>,
    /// Rung after a message is taken out, when the socket sends into this
    /// queue: the bell its blocked senders park on.
    on_pop: OnceLock<Arc<Notify>>,
}

struct QueueState {
    messages: VecDeque<Weighed>,
    /// What the queued messages are charged in total — see [`Queue::charge`]
    /// — maintained on every push and pop rather than summed on demand: a
    /// send at the bound is on the hot path and walking the queue there would
    /// make the ceiling cost more than the memory it saves.
    bytes: u64,
    dropped: u64,
    closed: bool,
}

/// A message together with what a queue charges for holding it.
///
/// The charge is computed once, when the message is first offered, and then
/// travels with it: a send that tries several peers does not walk the frames
/// again for each, and a pop subtracts exactly what the push added.
#[derive(Clone, Debug)]
pub(crate) struct Weighed {
    message: Multipart,
    charge: u64,
}

impl Weighed {
    /// Weighs `message` once.
    pub(crate) fn new(message: Multipart) -> Weighed {
        let charge = Queue::charge(&message);
        Weighed { message, charge }
    }

    /// The message, for a sender that was refused and keeps what it has.
    pub(crate) fn into_message(self) -> Multipart {
        self.message
    }

    /// The message, to look at without giving it up.
    pub(crate) fn message(&self) -> &Multipart {
        &self.message
    }
}

/// Which bound a queue reached, so the refusal can name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Full {
    /// `ZMQ_SNDHWM`/`ZMQ_RCVHWM`: the message count.
    Messages,
    /// [`QueueConfig::max_bytes`]: the byte ceiling.
    Bytes,
}

/// The body a window event queues, so an assertion can name it.
#[cfg(test)]
const WINDOW_BODY: &str = "queued in the window";

/// What a test makes happen in the instant between a wait's check and its
/// await — the window `enable()` closes.
///
/// That instant is **inside a single poll**: no other task on this thread can
/// run there, so the hazard is not reachable through the public surface, and
/// only a second thread landing at exactly that point ever hits it. These
/// hooks are how the tests below reach it deterministically instead of racing
/// for it; they compile under `cfg(test)` only.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum Window {
    /// A message is queued, notifying `ready`.
    Arrives,
    /// A message is taken, notifying `room`.
    Frees,
}

#[cfg(test)]
thread_local! {
    /// Armed by [`Queue::arm_window`], fired once by the next wait on this
    /// thread that reaches its window.
    static WINDOW: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Queue {
    /// An empty queue under `config`.
    pub fn new(config: QueueConfig) -> Queue {
        Queue {
            config,
            state: Mutex::new(QueueState {
                messages: VecDeque::new(),
                bytes: 0,
                dropped: 0,
                closed: false,
            }),
            room: Notify::new(),
            ready: Notify::new(),
            on_push: OnceLock::new(),
            on_pop: OnceLock::new(),
        }
    }

    /// What a queue charges for holding `message`: its payload, plus
    /// [`QUEUE_FRAME_OVERHEAD`] for every frame and [`QUEUE_MESSAGE_OVERHEAD`]
    /// for the message.
    ///
    /// The number [`QueueConfig::max_bytes`] is measured in, and the reason it
    /// is not [`Multipart::total_bytes`]: that counts what the sender paid on
    /// the wire, while the receiver holds the frames themselves — one handle
    /// each and, for a payload, one allocation. A message of a thousand empty
    /// frames has no payload at all and still costs a thousand handles.
    pub fn charge(message: &Multipart) -> u64 {
        message
            .frames()
            .iter()
            .fold(QUEUE_MESSAGE_OVERHEAD, |total, frame| {
                total
                    .saturating_add(QUEUE_FRAME_OVERHEAD)
                    .saturating_add(frame.len() as u64)
            })
    }

    /// This queue's bound and its action at the bound.
    pub const fn config(&self) -> QueueConfig {
        self.config
    }

    /// Messages queued right now.
    pub fn len(&self) -> usize {
        self.lock().messages.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.lock().messages.is_empty()
    }

    /// What the queued messages are charged, in bytes — the number
    /// [`QueueConfig::max_bytes`] bounds, as [`Queue::charge`] counts it.
    pub fn queued_bytes(&self) -> u64 {
        self.lock().bytes
    }

    /// Messages this queue has discarded at either bound, for a socket type
    /// that drops.
    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    /// Whether the peer's pipe has been destroyed.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Queues `message`, applying this queue's mute action at the bound.
    ///
    /// - [`MuteAction::Block`] waits for room — the backpressure a PUSH or a
    ///   DEALER applies, and the reason neither discards.
    /// - [`MuteAction::Drop`] discards the message being sent, keeping what
    ///   is already queued, and counts it in [`Queue::dropped`].
    /// - [`MuteAction::Fail`] reports `EAGAIN`.
    ///
    /// Fails with `EHOSTUNREACH` once the pipe is destroyed: the peer is
    /// gone, and the RFCs destroy its queue with it.
    pub async fn send(&self, message: Multipart) -> Result<Sent> {
        self.send_with(message, self.config.mute).await
    }

    /// [`Queue::send`] under a mute action the caller chooses: a ROUTER's
    /// queues drop, and `ZMQ_ROUTER_MANDATORY` asks one of them to wait.
    pub(crate) async fn send_with(&self, message: Multipart, mute: MuteAction) -> Result<Sent> {
        let weighed = Weighed::new(message);
        loop {
            // Register before looking, so that room freed between the two
            // wakes this wait instead of being missed by it.
            let mut room = std::pin::pin!(self.room.notified());
            room.as_mut().enable();
            {
                let mut state = self.lock();
                if state.closed {
                    return Err(gone());
                }
                let Some(bound) = self.full(&state, weighed.charge) else {
                    self.push(&mut state, weighed);
                    drop(state);
                    self.pushed();
                    return Ok(Sent::Queued);
                };
                match mute {
                    MuteAction::Drop => {
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    MuteAction::Fail => {
                        return Err(Error::EAGAIN(self.at(bound, state.bytes).into()));
                    }
                    MuteAction::Block => {}
                }
            }
            #[cfg(test)]
            self.in_window(Window::Frees);
            room.await;
        }
    }

    /// Queues `message` without ever waiting — `ZMQ_DONTWAIT`.
    ///
    /// Reports `EAGAIN` at the bound **whatever** the mute action is: a
    /// caller that asked not to block is asking for the refusal, not for a
    /// silent drop. A socket type that drops still drops on the blocking
    /// path, where the drop is its documented behaviour rather than the
    /// caller's choice. The message is consumed either way.
    pub fn try_send(&self, message: Multipart) -> Result<()> {
        let weighed = Weighed::new(message);
        let mut state = self.lock();
        if state.closed {
            return Err(gone());
        }
        if let Some(bound) = self.full(&state, weighed.charge) {
            return Err(Error::EAGAIN(self.at(bound, state.bytes).into()));
        }
        self.push(&mut state, weighed);
        drop(state);
        self.pushed();
        Ok(())
    }

    /// Queues the message if it fits right now, and hands it back if it does
    /// not — because the queue is full at either bound, or destroyed.
    ///
    /// The decision is made once, under the queue's lock, against the
    /// message's real charge. A check made first ("is there room?") and a
    /// send made after it can disagree when the message is larger than the
    /// room the check imagined, and the send then loses a message its caller
    /// believed it could place.
    pub(crate) fn offer(&self, weighed: Weighed) -> Result<(), Weighed> {
        let mut state = self.lock();
        if state.closed || self.full(&state, weighed.charge).is_some() {
            return Err(weighed);
        }
        self.push(&mut state, weighed);
        drop(state);
        self.pushed();
        Ok(())
    }

    /// Takes the next message, waiting for one.
    ///
    /// Fails with `EHOSTUNREACH` when the pipe is destroyed and drained: the
    /// peer is gone and nothing further will arrive on it.
    pub async fn recv(&self) -> Result<Multipart> {
        loop {
            let mut ready = std::pin::pin!(self.ready.notified());
            ready.as_mut().enable();
            {
                let mut state = self.lock();
                if let Some(message) = Self::pop(&mut state) {
                    drop(state);
                    self.popped();
                    return Ok(message);
                }
                if state.closed {
                    return Err(gone());
                }
            }
            #[cfg(test)]
            self.in_window(Window::Arrives);
            ready.await;
        }
    }

    /// Takes the next message if one is queued — `ZMQ_DONTWAIT`.
    pub fn try_recv(&self) -> Result<Multipart> {
        let mut state = self.lock();
        match Self::pop(&mut state) {
            Some(message) => {
                drop(state);
                self.popped();
                Ok(message)
            }
            None if state.closed => Err(gone()),
            None => Err(Error::EAGAIN("nothing is queued from this peer".into())),
        }
    }

    /// Destroys the queue: discards what it holds and wakes everyone waiting
    /// on it.
    ///
    /// This is the pattern RFCs' rule for a disconnect — the double queue is
    /// destroyed, "discarding any messages it contains" (§2) — and it is why
    /// a ZeroMQ send that returned is not a delivery: the queue it went into
    /// may be thrown away with its peer.
    ///
    /// Returns how many messages were discarded.
    pub fn close(&self) -> usize {
        let discarded = {
            let mut state = self.lock();
            state.closed = true;
            let discarded = state.messages.len();
            state.messages.clear();
            state.bytes = 0;
            discarded
        };
        self.room.notify_waiters();
        self.ready.notify_waiters();
        // Whoever parked on a socket-wide bell must look again: this queue is
        // not one they can wait for any more.
        if let Some(bell) = self.on_push.get() {
            bell.notify_waiters();
        }
        if let Some(bell) = self.on_pop.get() {
            bell.notify_waiters();
        }
        discarded
    }

    /// Which bound a message of `charge` bytes would cross, if any.
    ///
    /// The byte ceiling is skipped for an empty queue, so a message larger
    /// than the ceiling is queued rather than waited for forever (module
    /// documentation).
    fn full(&self, state: &QueueState, charge: u64) -> Option<Full> {
        if self.config.hwm != 0 && state.messages.len() >= self.config.hwm {
            return Some(Full::Messages);
        }
        if self.config.max_bytes != 0
            && !state.messages.is_empty()
            && state.bytes.saturating_add(charge) > self.config.max_bytes
        {
            return Some(Full::Bytes);
        }
        None
    }

    /// What a refusal at `bound` says, naming the number that was reached.
    ///
    /// `queued` is passed in rather than read here: every caller already
    /// holds the state lock, and a `std::sync::Mutex` is not reentrant.
    fn at(&self, bound: Full, queued: u64) -> String {
        match bound {
            Full::Messages => format!(
                "this peer's queue holds its high-water mark of {} messages",
                self.config.hwm
            ),
            Full::Bytes => format!(
                "this peer's queue holds {queued} bytes of its {} byte ceiling",
                self.config.max_bytes
            ),
        }
    }

    fn push(&self, state: &mut QueueState, message: Weighed) {
        state.bytes = state.bytes.saturating_add(message.charge);
        state.messages.push_back(message);
    }

    fn pop(state: &mut QueueState) -> Option<Multipart> {
        let weighed = state.messages.pop_front()?;
        state.bytes = state.bytes.saturating_sub(weighed.charge);
        Some(weighed.message)
    }

    /// Tells whoever waits for a message here — and the socket's bell, if
    /// this is a queue the socket receives from — that one arrived. Called
    /// after the push and after the lock is released, so a waiter that
    /// registered first and then looked cannot miss it.
    fn pushed(&self) {
        self.ready.notify_waiters();
        if let Some(bell) = self.on_push.get() {
            bell.notify_waiters();
        }
    }

    /// The counterpart of [`Queue::pushed`] for room: a message left.
    fn popped(&self) {
        self.room.notify_waiters();
        if let Some(bell) = self.on_pop.get() {
            bell.notify_waiters();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.state.lock().expect("queue poisoned")
    }

    /// Arms the next window on this thread: the event fires once, between
    /// the check and the await of the first wait that reaches it.
    #[cfg(test)]
    fn arm_window() {
        WINDOW.with(|armed| armed.set(true));
    }

    /// Fires an armed window event on this queue — see [`Window`].
    #[cfg(test)]
    fn in_window(&self, event: Window) {
        if !WINDOW.with(std::cell::Cell::take) {
            return;
        }
        match event {
            Window::Arrives => {
                self.try_send(Multipart::single(WINDOW_BODY))
                    .expect("the window's message needs room");
            }
            Window::Frees => {
                self.try_recv()
                    .expect("the window's receive needs a message");
            }
        }
    }
}

fn gone() -> Error {
    Error::EHOSTUNREACH(
        "this peer's pipe was destroyed, discarding what it held; the peer is gone".into(),
    )
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Queue")
            .field("hwm", &self.config.hwm)
            .field("max_bytes", &self.config.max_bytes)
            .field("mute", &self.config.mute)
            .field("queued", &state.messages.len())
            .field("queued_bytes", &state.bytes)
            .field("dropped", &state.dropped)
            .field("closed", &state.closed)
            .finish()
    }
}

/// How both directions of one peer's pipe are bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipeConfig {
    /// The outgoing direction: `ZMQ_SNDHWM` and the socket's mute action.
    pub outgoing: QueueConfig,
    /// The incoming direction: `ZMQ_RCVHWM` and what a full receive queue
    /// does.
    pub incoming: QueueConfig,
}

impl Default for PipeConfig {
    /// libzmq's defaults with the blocking action, which is what every
    /// bidirectional pattern in `zmq_socket(3)`'s table except ROUTER does.
    fn default() -> PipeConfig {
        PipeConfig {
            outgoing: QueueConfig {
                hwm: DEFAULT_SNDHWM,
                max_bytes: DEFAULT_QUEUE_BYTES,
                mute: MuteAction::Block,
            },
            incoming: QueueConfig {
                hwm: DEFAULT_RCVHWM,
                max_bytes: DEFAULT_QUEUE_BYTES,
                mute: MuteAction::Block,
            },
        }
    }
}

/// One peer's double queue: outgoing and incoming, each bounded on its own.
///
/// Created when a connection is *initiated* rather than when it completes,
/// and maintained "whether or not the connection is established" (§2) — which
/// is why `ZMQ_IMMEDIATE` exists as an option to suppress exactly that, and
/// why a message can be queued for a peer that never answers. Cloning shares
/// the queues: the socket holds one clone and the connection engine the
/// other, which is the only way a queue can be a rendezvous between them.
///
/// **A pipe is oriented from the socket's side, and that is why two sessions
/// cannot be paired through this type.** [`Pipe::outgoing`] is the queue of
/// messages headed *for* the peer and [`Pipe::incoming`] what arrived *from*
/// it, and a session pumps against that orientation: it pops `outgoing` onto
/// the wire and pushes what it reads into `incoming`. Two sessions driven
/// against each other therefore need two pipes whose halves are **crossed** —
/// one session's `outgoing` being the other's `incoming` — and
/// [`Pipe::new`] is the only constructor there is: a `Pipe` cannot be
/// assembled from two given queues, so the crossing is not expressible
/// through the public API. Sharing one pipe between them, the obvious move,
/// makes both sessions pop the same `outgoing` queue and push the same
/// `incoming` one, so each message is raced by both readers and half of them
/// arrive where nobody is looking. Nothing is broken by this: the engine
/// pairs a session with a *socket*, one pipe per peer with the socket holding
/// the other end, which is why every socket-level test over TCP delivers.
/// `Pipe::crossed` is that pairing, for a test inside this crate that wants
/// it; it exists under `cfg(test)` because nothing but a harness has any use
/// for a pipe whose other end is not a socket.
#[derive(Clone, Debug)]
pub struct Pipe {
    outgoing: Arc<Queue>,
    incoming: Arc<Queue>,
    refusals: Arc<Refusals>,
}

impl Pipe {
    /// A fresh double queue under `config`.
    pub fn new(config: PipeConfig) -> Pipe {
        Pipe {
            outgoing: Arc::new(Queue::new(config.outgoing)),
            incoming: Arc::new(Queue::new(config.incoming)),
            refusals: Arc::new(Refusals::default()),
        }
    }

    /// Two pipes with their halves crossed: what one side sends, the other
    /// receives.
    ///
    /// The pairing the public API cannot express — see the type's own
    /// documentation for why — and the whole of what a harness driving two
    /// sessions against each other needs, since both halves are just
    /// `Arc<Queue>` and the crossing is which `Arc` each end holds.
    /// `config.outgoing` bounds the first pipe's outgoing direction and hence
    /// the second's incoming one, and `config.incoming` the other way round.
    /// The `ERROR` channels stay separate: a refusal is one connection's, and
    /// there are two here.
    #[cfg(test)]
    pub(crate) fn crossed(config: PipeConfig) -> (Pipe, Pipe) {
        let there = Arc::new(Queue::new(config.outgoing));
        let back = Arc::new(Queue::new(config.incoming));
        (
            Pipe {
                outgoing: Arc::clone(&there),
                incoming: Arc::clone(&back),
                refusals: Arc::new(Refusals::default()),
            },
            Pipe {
                outgoing: back,
                incoming: there,
                refusals: Arc::new(Refusals::default()),
            },
        )
    }

    /// The queue of messages headed for this peer.
    pub fn outgoing(&self) -> Arc<Queue> {
        Arc::clone(&self.outgoing)
    }

    /// The queue of messages received from this peer.
    pub fn incoming(&self) -> Arc<Queue> {
        Arc::clone(&self.incoming)
    }

    /// The queue of messages headed for this peer, borrowed.
    ///
    /// For the paths that look at every peer's queue once per message and must
    /// not pay a reference-count round trip on each: [`Pipe::outgoing`] is the
    /// owned form, for whoever keeps the queue.
    pub(crate) fn outgoing_ref(&self) -> &Queue {
        &self.outgoing
    }

    /// The queue of messages received from this peer, borrowed; see
    /// [`Pipe::outgoing_ref`].
    pub(crate) fn incoming_ref(&self) -> &Queue {
        &self.incoming
    }

    /// Makes this pipe ring a socket's two bells: `readable` after a message
    /// arrives from the peer and `writable` after one leaves for it.
    ///
    /// Idempotent, and the first attachment stays. The engine calls it for
    /// every pipe it publishes to a socket, which is what lets the socket's
    /// waits park on two shared bells instead of on one future per peer
    /// (module documentation).
    pub(crate) fn attach(&self, readable: &Arc<Notify>, writable: &Arc<Notify>) {
        self.incoming.on_push.get_or_init(|| Arc::clone(readable));
        self.outgoing.on_pop.get_or_init(|| Arc::clone(writable));
    }

    /// Asks this connection to carry one ZMTP `ERROR` naming `reason`.
    ///
    /// **No libzmq counterpart**, and the parity table says so
    /// (`docs/libraries/zmq.md` §9). 37/ZMTP's only per-connection error
    /// channel is the `ERROR` command — "the peer SHALL treat an incoming
    /// ERROR command as fatal" — and libzmq's API has no way to send one: an
    /// XPUB application can decline to *apply* a subscription
    /// (`ZMQ_XPUB_MANUAL`) and cannot tell the subscriber why. An adapter
    /// that must refuse what a peer asked for has nothing else to say it
    /// with, and a silently ignored subscription is a subscriber waiting
    /// forever for messages nobody will send.
    ///
    /// The reason is sanitized to what the command may carry: printable
    /// ASCII, at most 255 octets. What the peer does about it is the peer's
    /// choice; this side keeps the connection.
    pub fn refuse(&self, reason: &str) {
        self.refusals.push(reason);
    }

    /// The next `ERROR` this connection has been asked to carry.
    ///
    /// Cancel-safe: a dropped wait leaves the reason queued, which is what
    /// lets the session hold this in a `select!`.
    pub async fn refusal(&self) -> String {
        self.refusals.next().await
    }

    /// Destroys both queues, discarding what they hold: the peer
    /// disconnected. Returns the counts discarded, outgoing first.
    pub fn close(&self) -> (usize, usize) {
        (self.outgoing.close(), self.incoming.close())
    }
}

/// `ERROR` commands an application asked one connection to carry.
#[derive(Debug, Default)]
struct Refusals {
    reasons: Mutex<VecDeque<String>>,
    ready: Notify,
}

impl Refusals {
    fn push(&self, reason: &str) {
        self.reasons
            .lock()
            .expect("refusals poisoned")
            .push_back(sanitize(reason));
        self.ready.notify_waiters();
    }

    async fn next(&self) -> String {
        loop {
            let mut ready = std::pin::pin!(self.ready.notified());
            // Registered before the check, so a reason queued between the two
            // wakes this rather than being missed.
            ready.as_mut().enable();
            if let Some(reason) = self.reasons.lock().expect("refusals poisoned").pop_front() {
                return reason;
            }
            ready.await;
        }
    }
}

/// What an `ERROR` reason may be: `error-reason = short-size 0*255VCHAR`,
/// plus the space libzmq's own reasons contain.
///
/// Anything else becomes `?`, because a reason is for a log and a reason
/// carrying control octets is a log injection rather than a diagnosis.
fn sanitize(reason: &str) -> String {
    reason
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .take(255)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    fn queue(hwm: usize, mute: MuteAction) -> Arc<Queue> {
        Arc::new(Queue::new(QueueConfig {
            hwm,
            max_bytes: DEFAULT_QUEUE_BYTES,
            mute,
        }))
    }

    /// A queue bounded in bytes rather than in messages: the high-water mark
    /// is out of the way, so what refuses is the ceiling.
    fn byte_queue(max_bytes: u64, mute: MuteAction) -> Arc<Queue> {
        Arc::new(Queue::new(QueueConfig {
            hwm: 0,
            max_bytes,
            mute,
        }))
    }

    fn message(body: &str) -> Multipart {
        Multipart::single(body)
    }

    /// Polls `future` by hand, with a waker that does nothing, exactly as
    /// often as one wake is worth: once to let it register and check — the
    /// armed window fires inside that poll — and once more for the wake it
    /// is owed. `None` is the lost wakeup: the event happened and the waiter
    /// is still asleep, which is what no amount of further polling would
    /// change.
    fn poll_through_the_window<F: Future>(future: F) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..2 {
            if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
                return Some(out);
            }
        }
        None
    }

    /// Claim: the mute-state column of `zmq_socket(3)`'s table, row for row,
    /// including the four rows the manual omits.
    #[test]
    fn the_mute_action_is_the_manuals_table() {
        for socket in [
            SocketType::Req,
            SocketType::Dealer,
            SocketType::Push,
            SocketType::Pull,
            SocketType::Pair,
            SocketType::Client,
            SocketType::Channel,
            SocketType::Scatter,
        ] {
            assert_eq!(
                MuteAction::sending(socket),
                Some(MuteAction::Block),
                "{socket:?}"
            );
        }
        for socket in [
            SocketType::Pub,
            SocketType::XPub,
            SocketType::XSub,
            SocketType::Router,
            SocketType::Radio,
        ] {
            assert_eq!(
                MuteAction::sending(socket),
                Some(MuteAction::Drop),
                "{socket:?}"
            );
        }
        for socket in [SocketType::Server, SocketType::Peer] {
            assert_eq!(
                MuteAction::sending(socket),
                Some(MuteAction::Fail),
                "{socket:?}"
            );
        }
        // The manual prints no action for these four, and the gap is reported
        // rather than guessed.
        for socket in [
            SocketType::Rep,
            SocketType::Sub,
            SocketType::Dish,
            SocketType::Gather,
        ] {
            assert_eq!(MuteAction::sending(socket), None, "{socket:?}");
            let err = QueueConfig::sending(socket).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{socket:?}: {err}");
        }

        // The one receive-side rule the RFCs do state.
        assert_eq!(
            MuteAction::receiving(SocketType::Sub),
            Some(MuteAction::Drop)
        );
        assert_eq!(
            MuteAction::receiving(SocketType::XSub),
            Some(MuteAction::Drop)
        );
        assert_eq!(MuteAction::receiving(SocketType::Pull), None);
    }

    /// Claim: at the bound, a blocking queue blocks — and the send completes
    /// as soon as a receive makes room. This is the backpressure PUSH,
    /// DEALER and PAIR apply, and the reason they never discard.
    #[tokio::test(start_paused = true)]
    async fn at_the_bound_a_blocking_queue_waits_for_room() {
        let q = queue(1, MuteAction::Block);
        assert_eq!(q.send(message("first")).await.expect("first"), Sent::Queued);

        let sender = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.send(message("second")).await })
        };
        // Margin: 50 ms for a send that must *not* complete. The
        // phenomenon is the absence of an event, so the interval only has to
        // exceed the time the send would take if the bound did not hold,
        // which is a queue push on the same thread — microseconds.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!sender.is_finished(), "the bound must hold the sender");
        assert_eq!(q.dropped(), 0, "a blocking queue discards nothing");

        // Draining one message is what lets it through.
        assert_eq!(q.recv().await.expect("first out"), message("first"));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), sender)
                .await
                .expect("the sender completed")
                .expect("the task")
                .expect("the send"),
            Sent::Queued
        );
        assert_eq!(q.recv().await.expect("second out"), message("second"));
    }

    /// Claim: at the bound, a dropping queue discards the message being sent,
    /// keeps what it already holds and counts the loss. PUB must not let one
    /// slow subscriber stall the others.
    #[tokio::test]
    async fn at_the_bound_a_dropping_queue_discards_the_new_message() {
        let q = queue(1, MuteAction::Drop);
        assert_eq!(q.send(message("kept")).await.expect("kept"), Sent::Queued);
        assert_eq!(
            q.send(message("lost")).await.expect("lost"),
            Sent::Dropped,
            "a drop is reported, not a silent Ok"
        );
        assert_eq!(q.dropped(), 1);
        assert_eq!(q.len(), 1);
        assert_eq!(
            q.recv().await.expect("out"),
            message("kept"),
            "the queued message must survive the drop"
        );
    }

    /// Claim: at the bound, a failing queue reports `EAGAIN` — the
    /// thread-safe family's answer, which has no blocking send.
    #[tokio::test]
    async fn at_the_bound_a_failing_queue_reports_eagain() {
        let q = queue(1, MuteAction::Fail);
        q.send(message("first")).await.expect("first");
        let err = q.send(message("second")).await.unwrap_err();
        assert_eq!(err.errno(), "EAGAIN", "{err}");
        assert_eq!(q.len(), 1);
        assert_eq!(q.dropped(), 0, "a refusal is not a drop");
    }

    /// Claim: `ZMQ_DONTWAIT` reports `EAGAIN` at the bound whatever the mute
    /// action is — including on a queue that would otherwise drop, because a
    /// caller who asked not to block asked for the refusal.
    #[tokio::test]
    async fn dontwait_reports_eagain_whatever_the_mute_action() {
        for mute in [MuteAction::Block, MuteAction::Drop, MuteAction::Fail] {
            let q = queue(1, mute);
            q.try_send(message("first")).expect("first");
            let err = q.try_send(message("second")).unwrap_err();
            assert_eq!(err.errno(), "EAGAIN", "{mute:?}: {err}");
            assert_eq!(q.dropped(), 0, "{mute:?}: try_send never drops");
        }
    }

    /// Claim: a receive that finds nothing is `EAGAIN` without waiting, and
    /// the blocking one returns as soon as a message arrives.
    #[tokio::test(start_paused = true)]
    async fn a_receive_waits_and_dontwait_does_not() {
        let q = queue(DEFAULT_RCVHWM, MuteAction::Block);
        let err = q.try_recv().unwrap_err();
        assert_eq!(err.errno(), "EAGAIN", "{err}");

        let receiver = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.recv().await })
        };
        // Margin: 20 ms for a receive that must not complete yet; a
        // receive that wrongly returned would have done so in microseconds.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!receiver.is_finished());
        q.send(message("late")).await.expect("send");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), receiver)
                .await
                .expect("the receiver completed")
                .expect("the task")
                .expect("the recv"),
            message("late")
        );
    }

    /// Claim: a high-water mark of zero is libzmq's "no limit" — the 2.x
    /// default, available and deliberately not ours.
    #[tokio::test]
    async fn a_zero_high_water_mark_is_no_limit() {
        let q = queue(0, MuteAction::Fail);
        for n in 0..2_000 {
            assert_eq!(
                q.send(message(&format!("{n}"))).await.expect("no limit"),
                Sent::Queued
            );
        }
        assert_eq!(q.len(), 2_000);
        assert_eq!(q.dropped(), 0);
    }

    /// Claim: the byte ceiling bounds a peer whose messages are large, where
    /// the high-water mark alone would let 1000 of them in. The ceiling is
    /// reached by *bytes*, so the refusal names bytes and not the message
    /// count — and with `hwm: 0` there is no message count to blame.
    #[tokio::test]
    async fn at_the_byte_ceiling_a_failing_queue_names_the_bytes() {
        let chunk = message(&"x".repeat(400));
        let small = message(&"y".repeat(100));
        let (big, little) = (Queue::charge(&chunk), Queue::charge(&small));
        // Room for two chunks and one small message, to the byte.
        let q = byte_queue(2 * big + little, MuteAction::Fail);
        assert_eq!(q.send(chunk.clone()).await.expect("first"), Sent::Queued);
        assert_eq!(q.send(chunk.clone()).await.expect("second"), Sent::Queued);
        assert_eq!(q.queued_bytes(), 2 * big);

        // A third chunk is over the ceiling, and nothing about the message
        // count is at its bound: two messages of an unlimited number.
        let err = q.send(chunk.clone()).await.unwrap_err();
        assert!(
            matches!(err, Error::EAGAIN(_)),
            "the byte ceiling refuses like the message bound does: {err:?}"
        );
        assert!(
            err.cause().contains("byte ceiling"),
            "the refusal says which bound was reached: {}",
            err.cause()
        );
        assert_eq!(q.len(), 2);

        // A smaller message still fits under the ceiling: what is bounded is
        // the memory, not the number of sends.
        assert_eq!(q.send(small).await.expect("the rest"), Sent::Queued);
        assert_eq!(q.queued_bytes(), 2 * big + little);
    }

    /// Claim: at the byte ceiling every mute action means what it means at
    /// the message bound — a dropping socket type drops and counts, and a
    /// blocking one waits until a receive frees the bytes.
    #[tokio::test(start_paused = true)]
    async fn at_the_byte_ceiling_the_mute_action_still_decides() {
        let dropping = byte_queue(64, MuteAction::Drop);
        dropping.send(message(&"a".repeat(60))).await.expect("kept");
        assert_eq!(
            dropping
                .send(message(&"b".repeat(60)))
                .await
                .expect("dropped"),
            Sent::Dropped
        );
        assert_eq!(dropping.dropped(), 1);
        assert_eq!(dropping.len(), 1);

        let blocking = Arc::new(Queue::new(QueueConfig {
            hwm: 0,
            max_bytes: 64,
            mute: MuteAction::Block,
        }));
        blocking
            .send(message(&"a".repeat(60)))
            .await
            .expect("first");
        let sender = Arc::clone(&blocking);
        let waiting = tokio::spawn(async move { sender.send(message(&"b".repeat(60))).await });
        // The same 50 ms margin the message-bound test uses, and for the
        // same reason: the phenomenon is the absence of an event.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "the byte ceiling must hold the sender"
        );
        blocking.recv().await.expect("drain the ceiling");
        assert_eq!(
            waiting.await.expect("join").expect("room at last"),
            Sent::Queued
        );
        assert_eq!(
            blocking.queued_bytes(),
            Queue::charge(&message(&"b".repeat(60)))
        );
    }

    /// Claim: a queue always accepts one message, however large, so a
    /// message above the ceiling is queued instead of waiting for room that
    /// can never appear. This is what keeps `max_message_size` the bound on
    /// the single-message case, and the byte ceiling a bound on the queue.
    #[tokio::test]
    async fn one_message_above_the_ceiling_is_queued_rather_than_deadlocked() {
        let q = byte_queue(1024, MuteAction::Block);
        let huge = message(&"x".repeat(4096));
        let charged = Queue::charge(&huge);
        assert_eq!(q.send(huge).await.expect("the one message"), Sent::Queued);
        assert_eq!(q.queued_bytes(), charged);

        // And the next one waits, because the queue is no longer empty: the
        // exemption is for an empty queue, not for every large message.
        let refused = q
            .offer(Weighed::new(message("after")))
            .expect_err("the queue is not empty any more");
        assert_eq!(refused.into_message(), message("after"));
        let err = q.try_send(message("after")).unwrap_err();
        assert!(matches!(err, Error::EAGAIN(_)), "{err:?}");
    }

    /// Claim: `DEFAULT_QUEUE_BYTES` is what a real peer meets, so the
    /// product the message limit's documentation states — `hwm ×
    /// max_message_size` — is not the exposure. A peer sending 1 MiB
    /// messages gets seven of them queued, because the eighth and its
    /// overhead would pass 8 MiB, and not a thousand.
    #[tokio::test]
    async fn the_default_queue_bounds_a_peer_in_bytes_not_in_messages() {
        let q = queue(DEFAULT_SNDHWM, MuteAction::Drop);
        let one_mib = message(&"x".repeat(1024 * 1024));
        let each = Queue::charge(&one_mib);
        for _ in 0..64 {
            q.send(one_mib.clone()).await.expect("send");
        }
        let held = DEFAULT_QUEUE_BYTES / each;
        assert_eq!(held, 7, "8 MiB of 1 MiB messages and their overhead");
        assert_eq!(q.len() as u64, held, "not 1000");
        assert_eq!(q.queued_bytes(), held * each);
        assert!(q.queued_bytes() <= DEFAULT_QUEUE_BYTES);
        assert_eq!(q.dropped(), 64 - held);
        assert!(
            q.len() < DEFAULT_SNDHWM,
            "the message bound was never the thing that refused"
        );
    }

    /// Claim: **a message of empty frames is charged for its frames.** A
    /// payload-only count sees nothing in it, so a queue with no message
    /// bound took such messages without end while each cost the receiver a
    /// thousand frame handles. With the overhead charged the byte ceiling
    /// refuses, and what it admits is the arithmetic the module states.
    #[test]
    fn a_message_of_empty_frames_is_charged_for_its_frames() {
        // `hwm: 0`: only the byte ceiling can refuse.
        let q = byte_queue(DEFAULT_QUEUE_BYTES, MuteAction::Fail);
        let hollow = Multipart::new(vec![crate::message::Message::empty(); 1024]).expect("frames");
        assert_eq!(hollow.total_bytes(), 0, "there is nothing in it to count");
        let each = Queue::charge(&hollow);
        assert_eq!(each, 1024 * QUEUE_FRAME_OVERHEAD + QUEUE_MESSAGE_OVERHEAD);

        let mut accepted = 0u64;
        while q.try_send(hollow.clone()).is_ok() {
            accepted += 1;
            assert!(
                accepted < 10_000,
                "a queue with no message bound must still refuse hollow messages"
            );
        }
        assert_eq!(accepted, DEFAULT_QUEUE_BYTES / each);
        assert_eq!(q.queued_bytes(), accepted * each);
        assert!(q.queued_bytes() <= DEFAULT_QUEUE_BYTES);
    }

    /// Claim: the message itself costs something even with one empty frame,
    /// so a flood of the smallest message there is cannot fill a queue
    /// without a message bound past the byte ceiling either.
    #[test]
    fn the_smallest_message_is_charged_too() {
        let q = byte_queue(1024 * 1024, MuteAction::Fail);
        let smallest = Multipart::single(crate::message::Message::empty());
        let each = Queue::charge(&smallest);
        assert_eq!(each, QUEUE_FRAME_OVERHEAD + QUEUE_MESSAGE_OVERHEAD);

        let mut accepted = 0u64;
        while q.try_send(smallest.clone()).is_ok() {
            accepted += 1;
            assert!(accepted < 100_000, "the ceiling must refuse");
        }
        assert_eq!(accepted, 1024 * 1024 / each);
    }

    /// Claim: whether a message fits is decided once, against its real size,
    /// and a refusal hands the message back. A check that asks "is there a
    /// byte of room?" and a send that asks "is there room for these two
    /// mebibytes?" disagree, and the send then loses a message its caller
    /// believed it could place — which a socket that promises never to
    /// discard cannot afford.
    #[test]
    fn an_offer_decides_against_the_real_size_and_keeps_what_it_refuses() {
        let small = message(&"s".repeat(100));
        let large = message(&"l".repeat(2000));
        let little = Queue::charge(&small);
        // Room for three small messages and nothing like a large one.
        let q = byte_queue(3 * little, MuteAction::Block);
        assert!(q.offer(Weighed::new(small.clone())).is_ok());
        assert!(q.offer(Weighed::new(small.clone())).is_ok());

        let back = q
            .offer(Weighed::new(large.clone()))
            .expect_err("two kilobytes do not fit");
        assert_eq!(back.into_message(), large, "the refused message comes back");
        assert_eq!(q.len(), 2, "and nothing was queued for it");
        assert!(
            q.offer(Weighed::new(small)).is_ok(),
            "the room that was there is still there"
        );
        assert_eq!(q.queued_bytes(), 3 * little);

        // A destroyed queue refuses too, and the message still comes back.
        q.close();
        let late = q
            .offer(Weighed::new(message("late")))
            .expect_err("the pipe is gone");
        assert_eq!(late.into_message(), message("late"));
    }

    /// Claim: an attached pipe rings the socket's bells, each for its own
    /// direction: `readable` after a message arrives from the peer and
    /// `writable` after one leaves for it — and queuing for the peer is not
    /// room, so it rings neither.
    #[test]
    fn an_attached_pipe_rings_the_socket_bells() {
        let readable = Arc::new(Notify::new());
        let writable = Arc::new(Notify::new());
        let pipe = Pipe::new(PipeConfig::default());
        pipe.attach(&readable, &writable);

        let mut arrived = std::pin::pin!(readable.notified());
        let mut freed = std::pin::pin!(writable.notified());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(arrived.as_mut().poll(&mut cx).is_pending());
        assert!(freed.as_mut().poll(&mut cx).is_pending());

        pipe.incoming().try_send(message("in")).expect("queued");
        assert!(arrived.as_mut().poll(&mut cx).is_ready());
        assert!(
            freed.as_mut().poll(&mut cx).is_pending(),
            "a message arriving is not room"
        );

        pipe.outgoing().try_send(message("out")).expect("queued");
        assert!(
            freed.as_mut().poll(&mut cx).is_pending(),
            "a message queued for the peer is not room"
        );
        pipe.outgoing().try_recv().expect("the session takes it");
        assert!(freed.as_mut().poll(&mut cx).is_ready());
    }

    /// Claim: destroying a pipe rings both bells, so a socket parked on them
    /// looks again and finds the peer gone rather than sleeping on it.
    #[test]
    fn closing_an_attached_pipe_rings_both_bells() {
        let readable = Arc::new(Notify::new());
        let writable = Arc::new(Notify::new());
        let pipe = Pipe::new(PipeConfig::default());
        pipe.attach(&readable, &writable);

        let mut incoming = std::pin::pin!(readable.notified());
        let mut outgoing = std::pin::pin!(writable.notified());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(incoming.as_mut().poll(&mut cx).is_pending());
        assert!(outgoing.as_mut().poll(&mut cx).is_pending());

        pipe.close();
        assert!(incoming.as_mut().poll(&mut cx).is_ready());
        assert!(outgoing.as_mut().poll(&mut cx).is_ready());
    }

    /// Claim: destroying a peer's pipe discards what it held and unblocks
    /// everyone on it — the pattern RFCs' disconnect rule, and the reason a
    /// send that returned is not a delivery.
    #[tokio::test(start_paused = true)]
    async fn a_destroyed_pipe_discards_and_unblocks() {
        let q = queue(2, MuteAction::Block);
        q.send(message("one")).await.expect("one");
        q.send(message("two")).await.expect("two");

        let blocked = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.send(message("three")).await })
        };
        // Margin: as above — 20 ms against a would-be completion measured
        // in microseconds.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());

        assert_eq!(q.close(), 2, "both queued messages are discarded");
        let err = tokio::time::timeout(Duration::from_secs(5), blocked)
            .await
            .expect("the blocked sender was woken")
            .expect("the task")
            .unwrap_err();
        assert_eq!(err.errno(), "EHOSTUNREACH", "{err}");

        assert!(q.is_closed());
        assert_eq!(q.len(), 0);
        assert_eq!(q.try_recv().unwrap_err().errno(), "EHOSTUNREACH");
        assert_eq!(q.recv().await.unwrap_err().errno(), "EHOSTUNREACH");
        assert_eq!(
            q.send(message("after")).await.unwrap_err().errno(),
            "EHOSTUNREACH"
        );
    }

    /// Claim: a pipe is two independent queues, so a full outgoing direction
    /// does not stop messages arriving from that peer — which is what
    /// "double queue, one per direction" buys.
    #[tokio::test]
    async fn the_two_directions_of_a_pipe_are_independent() {
        let pipe = Pipe::new(PipeConfig {
            outgoing: QueueConfig {
                hwm: 1,
                max_bytes: DEFAULT_QUEUE_BYTES,
                mute: MuteAction::Fail,
            },
            incoming: QueueConfig {
                hwm: 4,
                max_bytes: DEFAULT_QUEUE_BYTES,
                mute: MuteAction::Block,
            },
        });
        let out = pipe.outgoing();
        let inn = pipe.incoming();

        out.send(message("to the peer")).await.expect("outgoing");
        assert_eq!(
            out.send(message("over the bound"))
                .await
                .unwrap_err()
                .errno(),
            "EAGAIN"
        );

        inn.send(message("from the peer")).await.expect("incoming");
        assert_eq!(
            inn.recv().await.expect("delivered"),
            message("from the peer")
        );

        // Cloning a pipe shares the queues rather than copying them: that is
        // how the socket and the connection engine meet.
        let same = pipe.clone();
        assert_eq!(same.outgoing().len(), 1);

        assert_eq!(pipe.close(), (1, 0));
        assert!(same.incoming().is_closed());
    }

    /// Claim: crossed pipes carry in both directions — what one side puts
    /// into `outgoing` is what the other takes out of `incoming`, which is
    /// the pairing a harness for two sessions needs and the one `Pipe::new`
    /// cannot produce. Sharing a single pipe instead makes both ends pop the
    /// same queue, so half the traffic arrives where nobody is looking.
    #[tokio::test]
    async fn crossed_pipes_carry_in_both_directions() {
        let (here, there) = Pipe::crossed(PipeConfig::default());

        here.outgoing()
            .send(message("to the other side"))
            .await
            .expect("queued");
        assert_eq!(
            there.incoming().recv().await.expect("arrived"),
            message("to the other side")
        );

        there
            .outgoing()
            .send(message("and back"))
            .await
            .expect("queued");
        assert_eq!(
            here.incoming().recv().await.expect("arrived"),
            message("and back")
        );

        // Each end's `ERROR` channel is its own: a refusal belongs to one
        // connection, and a crossed pair stands in for two.
        here.refuse("no");
        assert!(there.refusals.reasons.lock().expect("refusals").is_empty());
    }

    /// Claim: a queue holds whole messages, so multipart survives the trip
    /// frame for frame and cannot be interleaved with another message.
    #[tokio::test]
    async fn a_queue_slot_holds_a_whole_message() {
        let q = queue(2, MuteAction::Block);
        let envelope = Multipart::new(vec![
            crate::message::Message::empty(),
            crate::message::Message::from("body"),
        ])
        .expect("frames");
        q.send(envelope.clone()).await.expect("send");
        q.send(Multipart::single("other")).await.expect("send");

        let first = q.recv().await.expect("first");
        assert_eq!(first, envelope);
        assert_eq!(first.len(), 2);
        assert_eq!(q.recv().await.expect("second"), Multipart::single("other"));
    }

    // The two waits a queue keeps, against the window between a wait's check
    // and its await, and what exactly covers that window. The socket-wide
    // waits that replaced the other two have the same window and the same
    // tests, beside them in `socket.rs`.
    //
    // The hazard is inside a *single poll*: nothing else on this thread runs
    // between a wait's check and its await, so it is not reachable through
    // the public surface at all, and the `Window` hook exists to occupy that
    // instant deterministically instead of racing a second thread for it.
    //
    // What fails without the fix is the shape that creates the `Notified`
    // **below** the check — `self.ready.notified().await` after an empty
    // read, the obvious simplification of these loops: both tests then
    // report the message or the room that never woke anybody. What does
    // **not** fail is dropping `enable()` alone while the `notified()` stays
    // above the check, and that is worth writing down rather than assuming:
    // `Notified` snapshots the count of `notify_waiters()` calls when it is
    // *constructed* and its first poll completes if the count moved (tokio
    // 1.53.1, `sync/notify.rs:569` and `:1121`), so construction — not
    // `enable()` — is what covers the window for a `notify_waiters()`.
    // `enable()` is still what makes that registration explicit rather than
    // a detail of tokio's, and it is what a `notify_one()` would require, so
    // it stays and these tests hold the creation point above the check.

    /// Claim: a message queued in the instant between a receiver's check and
    /// its await still wakes that receiver — the message must not sit in the
    /// queue while its reader sleeps.
    #[test]
    fn a_message_arriving_in_the_window_wakes_a_receiver() {
        let q = queue(4, MuteAction::Block);
        Queue::arm_window();
        let received = poll_through_the_window(q.recv())
            .expect("a message queued in the window must wake the receiver")
            .expect("the receive");
        assert_eq!(received, message(WINDOW_BODY));
        assert!(q.is_empty(), "the woken receiver took the message");
    }

    /// Claim: room freed in the instant between a blocked sender's check and
    /// its await still wakes that sender — the backpressure half of the same
    /// hazard, and the reason a reply could sit in a queue while its session
    /// slept.
    #[test]
    fn room_freed_in_the_window_wakes_a_blocked_sender() {
        let q = queue(1, MuteAction::Block);
        q.try_send(message("filling the bound")).expect("filling");
        Queue::arm_window();
        let sent = poll_through_the_window(q.send(message("blocked")))
            .expect("room freed in the window must wake the sender")
            .expect("the send");
        assert_eq!(sent, Sent::Queued);
        assert_eq!(
            q.len(),
            1,
            "the window took one out and the send put one in"
        );
    }
}
