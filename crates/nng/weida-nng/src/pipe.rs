//! The per-pipe queues — `NNG_OPT_SENDBUF` and `NNG_OPT_RECVBUF` — and what
//! each protocol does when one is full.
//!
//! "NNG socket `SENDBUF` and `RECVBUF` are depths in **messages**, each
//! configurable from 0 through 8192, not byte credit"
//! (`docs/research/nanomsg-nng.md` §5). So a queue here holds whole
//! [`Message`]s and counts them, exactly as NNG does — and counts their
//! octets too, for the reason below.
//!
//! # A queue is bounded twice: in messages, as NNG is, and in bytes
//!
//! A depth counts messages and `NNG_OPT_RECVMAXSZ` bounds one message
//! ([`crate::message`]), so what a peer can make a queue hold is the
//! **product** of the two: [`DEFAULT_RECV_DEPTH`] messages of
//! [`DEFAULT_RECV_MAX_SIZE`](crate::DEFAULT_RECV_MAX_SIZE) is 128 MiB per
//! pipe, and a socket admits [`DEFAULT_MAX_PIPES`](crate::DEFAULT_MAX_PIPES)
//! pipes — 128 GiB nobody bounded, which is not what "no remote input can
//! cause unbounded memory allocation" (`docs/INVARIANTS.md`) says. So every
//! queue carries [`QueueConfig::max_bytes`] as well: [`DEFAULT_QUEUE_BYTES`],
//! 8 MiB per direction per pipe, which is the number the sibling ZMTP
//! library bounds the same quantity with.
//!
//! The byte ceiling is enforced here rather than divided into a smaller
//! depth, because `NNG_OPT_RECVMAXSZ` is a **ceiling and not a size**:
//! dividing by it would cut every queue to eight messages, including the
//! ones whose messages are forty octets, and `NNG_OPT_RECVBUF` would stop
//! meaning what NNG says it means. Counting the octets where they are
//! refuses only the pipe that occupies the memory.
//!
//! **A queue always accepts one message**, however large, so the ceiling
//! applies only to a queue that is not already empty: without that rule a
//! message above the ceiling could never be queued at all and a blocking
//! protocol would wait for room that cannot appear — a deadlock in place of
//! a bound. What one direction of one pipe can hold is therefore
//! `max_bytes` plus one `NNG_OPT_RECVMAXSZ`, 9 MiB at the defaults, and a
//! socket at its pipe ceiling holds at most 9 GiB of incoming queues rather
//! than the 128 GiB the depth alone allowed. Which of the two bounds is
//! full changes nothing about what happens next: both are a full queue, and
//! the answer to a full queue is [`FullAction`]'s.
//!
//! **Zero is not "unlimited" here; it is zero.** libzmq's high-water mark of
//! `0` means no limit, and NNG's buffer depth of `0` means no buffer: "A
//! full or zero-depth send path blocks until it can queue/hand off" (§5),
//! and PUSH ships with `SENDBUF=0` precisely so that a send waits for a
//! puller that can take it rather than piling up behind one that cannot
//! (§5, §4). A [`Queue`] of depth zero is therefore a rendezvous: a message
//! may be deposited only when a taker is already waiting for it. Getting
//! this backwards would turn PUSH's documented default into "no bound at
//! all", which is the opposite of what it says and a violation of
//! `docs/INVARIANTS.md` besides.
//!
//! **What happens at the bound is a property of the protocol**, not of the
//! call site: "PUSH/PAIR block, BUS drops, and SUB drops old or rejects new
//! locally" (§12/P4). [`FullAction`] is that sentence written once, and
//! [`FullAction::sending`]/[`FullAction::receiving`] are the per-protocol
//! table, so no socket type re-decides it at each send.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use tokio::sync::futures::Notified;
use weida_sp::EndpointType;

use crate::error::{Error, Result};
use crate::message::Message;

/// Deepest queue NNG admits, in messages: "each configurable from 0 through
/// 8192" (§5).
pub const MAX_QUEUE_DEPTH: usize = 8192;

/// Default `NNG_OPT_SENDBUF`, in messages.
///
/// Zero, which is the value NNG documents for PUSH (§5) and the value that
/// makes a send mean what SP says it means: the message is handed to a peer
/// that can take it now, or the sender waits. A positive depth is an
/// explicit decision to let messages pile up for a peer that is not keeping
/// up, and it is available up to [`MAX_QUEUE_DEPTH`].
pub const DEFAULT_SEND_DEPTH: usize = 0;

/// Default `NNG_OPT_RECVBUF`, in messages.
///
/// **NNG publishes no default for this one**, so the number is ours and is
/// named here rather than hidden. Zero would mean a message only moves off
/// the connection while the application is already parked in a receive,
/// which turns every gap between two `recv` calls into transport
/// backpressure and, for the protocols that drop rather than block —
/// PUB/SUB, BUS, SURVEY — into loss the sheet does not describe. 128 is deep
/// enough that an application doing work between receives does not stall its
/// peer, and shallow enough that a slow reader is bounded within one socket
/// rather than within one machine (`docs/INVARIANTS.md`).
pub const DEFAULT_RECV_DEPTH: usize = 128;

/// Default `NNG_OPT_SENDBUF` for the protocols that **drop** rather than
/// wait — PUB, BUS and SURVEYOR.
///
/// [`DEFAULT_SEND_DEPTH`] is zero because that is PUSH's documented value,
/// and for PUSH it means what it says: the send waits for a puller that
/// can take the message. For a protocol whose answer to a full queue is to
/// discard, a depth of zero would mean discarding every message that
/// arrives while the connection's writer is busy with the previous one,
/// which is not "best effort" but "almost never" — and it is not what a
/// real NNG node does, because its pipes carry a queue of their own. So
/// the dropping protocols get a depth, and it is named here rather than
/// hidden in each of them.
pub const DEFAULT_BROADCAST_SEND_DEPTH: usize = 128;

/// Default byte ceiling on one direction of one pipe's queue, in octets.
///
/// **NNG has no such option**, and that is the reason this exists: its
/// buffer depths count messages (§5), so at the defaults one pipe could hold
/// [`DEFAULT_RECV_DEPTH`] × `NNG_OPT_RECVMAXSZ` = 128 MiB per direction and
/// a socket 128 GiB of it, and "no remote input can cause unbounded memory
/// allocation" (`docs/INVARIANTS.md`) would be a claim about a product
/// nobody bounded.
///
/// 8 MiB is the number the sibling ZMTP library gives the same quantity and
/// the number weida's own runtime gives a subscriber, so a socket, a bridge
/// and a weida endpoint expose a peer to the same order of memory rather
/// than to three numbers chosen separately. `0` means no ceiling, which is
/// NNG's own behaviour and is available to a caller who wants exactly it.
pub const DEFAULT_QUEUE_BYTES: u64 = 8 * 1024 * 1024;

/// What a protocol does when the queue it is putting a message into is full.
///
/// The four values are the four behaviours the sheet names, and no socket
/// type invents a fifth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FullAction {
    /// Wait for room, and report `NNG_ETIMEDOUT` if a send timeout expires
    /// first. PUSH and PAIR: "PUSH/PAIR block" (§12/P4), "with no eligible
    /// peer, the send waits or times out" (§4).
    Block,
    /// Discard the message being sent, keep what is queued, and let the send
    /// succeed. BUS: "send is best-effort, nonblocking, and discards when a
    /// peer cannot receive" (§4); PUB and SURVEYOR broadcast the same way
    /// (§4, §5).
    Drop,
    /// Discard the **oldest** queued message to make room for the new one.
    /// SUB's default, `SUB_PREFNEW=true`: "the default policy removes its
    /// oldest queued message to make room" (§4).
    DropOldest,
    /// Refuse the **new** message, keeping what is queued.
    /// `SUB_PREFNEW=false`: "preserves old queued messages by rejecting the
    /// new message" (§4).
    RejectNewest,
}

impl FullAction {
    /// What `protocol` does at a full outgoing queue.
    ///
    /// Blocking is the rule and dropping is the named exception, which is
    /// the shape the sheet states it in: the general contract is "a full or
    /// zero-depth send path blocks until it can queue/hand off, unless the
    /// socket send timeout yields `NNG_ETIMEDOUT`" (§5), and BUS, PUB and
    /// SURVEYOR are the three that broadcast best-effort instead (§4).
    ///
    /// **REP and RESPONDENT drop as well, for a different reason.** Their
    /// send is addressed to one pipe — the requester's — and a send that
    /// waited for that pipe would let one requester that stopped reading
    /// hold every other client of the socket hostage: the context that
    /// answers it parks behind that requester's full TCP window, and so
    /// does every worker the socket has once they have all been given
    /// requests by it. A reply the pipe cannot take is therefore
    /// discarded and the send succeeds, which is what the requester's own
    /// timer — a resend for REQ, a deadline for SURVEYOR — already recovers
    /// from (§4, §6). The depth that makes this usable is
    /// [`DEFAULT_BROADCAST_SEND_DEPTH`], for the argument given there.
    pub const fn sending(protocol: EndpointType) -> FullAction {
        match protocol {
            EndpointType::Bus
            | EndpointType::Pub
            | EndpointType::Surveyor
            | EndpointType::Rep
            | EndpointType::Respondent => FullAction::Drop,
            _ => FullAction::Block,
        }
    }

    /// What `protocol` does at a full **incoming** queue: it stops reading
    /// the connection.
    ///
    /// [`FullAction::Block`] for every protocol that receives, and not
    /// because nothing else is specified — SUB has two documented policies
    /// (§4) — but because its policies belong one layer up. A SUB filters
    /// by prefix, and a queue that dropped a subscribed message to make room
    /// for an unsubscribed one would be applying `SUB_PREFNEW` to traffic
    /// the application never asked for. So the pipe's queue is plain
    /// backpressure, which is the only signal SP has — it grants no credit
    /// on the wire (§12/P12), so not reading is the whole of it — and
    /// `SUB_PREFNEW` applies to the queue of *admitted* publications on
    /// the socket, which is where NNG's receive buffer is too.
    ///
    /// **PUB and PUSH have no receive operation**, so there is nobody to
    /// push back for and nothing to queue: what a peer writes to them is
    /// read and discarded as it arrives, as NNG's `pub0` and `push0` do with
    /// whatever reaches them. A queue that held it would hold up to its
    /// whole ceiling of octets nobody will ever take, for as long as the
    /// connection lives.
    pub const fn receiving(protocol: EndpointType) -> FullAction {
        match protocol {
            EndpointType::Pub | EndpointType::Push => FullAction::Drop,
            _ => FullAction::Block,
        }
    }
}

/// One pipe's identity within one socket, stable for that pipe's lifetime.
///
/// NNG's `nng_pipe` is "an opaque, value-passed handle for one connection,
/// associated with exactly one creating dialer or listener and therefore one
/// socket" (§2). The number is also what a raw BUS puts in its header and
/// what a device pushes onto a tag stack, so it is 31 bits wide: the tag
/// stack's top bit is its terminator and belongs to the encoding
/// [rfc-reqrep §5].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipeId(u32);

impl PipeId {
    /// The largest pipe ID, which is what the tag stack's 31 bits allow.
    pub const MAX: u32 = 0x7fff_ffff;

    /// A pipe ID from a counter, masked into the 31 bits a tag stack has
    /// room for.
    pub const fn new(id: u32) -> PipeId {
        PipeId(id & PipeId::MAX)
    }

    /// The number, for a header, a log line or a routing table.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for PipeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipe {}", self.0)
    }
}

/// How one direction of one pipe is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    /// `NNG_OPT_SENDBUF` or `NNG_OPT_RECVBUF` for this direction, in
    /// messages, `0..=`[`MAX_QUEUE_DEPTH`]. Zero is a rendezvous, not
    /// "unlimited".
    pub depth: usize,
    /// The same direction's ceiling in octets, which NNG has no option for.
    /// `0` means no ceiling. A queue that is empty accepts one message of
    /// any size regardless of this number, so what one direction of one pipe
    /// holds is bounded by `max_bytes` plus one `NNG_OPT_RECVMAXSZ` and
    /// never by this alone (module documentation).
    pub max_bytes: u64,
    /// What to do at either bound.
    pub full: FullAction,
}

impl QueueConfig {
    /// Refuses a depth NNG would refuse, where it is configured.
    pub fn validate(&self, option: &str) -> Result<()> {
        if self.depth > MAX_QUEUE_DEPTH {
            return Err(Error::EINVAL(
                format!(
                    "{option} is 0..={MAX_QUEUE_DEPTH} messages; {} is out of range",
                    self.depth
                )
                .into(),
            ));
        }
        Ok(())
    }
}

/// What a send did.
///
/// A drop is a return value rather than a silent `Ok`, because for BUS, PUB
/// and SURVEYOR it is the documented outcome and a caller that wants to
/// count or log it must be able to see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Sent {
    /// The message is queued for the peer.
    Queued,
    /// The queue was full and this protocol discards rather than waits. The
    /// message is gone and nothing will retry it.
    Dropped,
}

/// What a socket's pipes tell the socket as a whole.
///
/// A socket waits on its pipes as a set: a receive wants whichever pipe has
/// a message first, a send whichever has room first. One wake-up per socket
/// answers both, where waiting on every pipe separately would cost a future
/// per pipe on every wait. Each queue of a pipe made with [`Pipe::linked`]
/// feeds the one that concerns it — an incoming queue the *arrived* signal
/// when a message goes in, an outgoing queue the *room* signal when a
/// message comes out or a taker parks — and the engine feeds both when the
/// set of pipes itself changes, because a waiter's view of the set is then
/// out of date.
///
/// **Create the wait before looking**, in either case. A [`Notified`] sees
/// every wake-up since it was *created*, not since it was first polled, so
/// one made before the look cannot miss a message or a slot that appears
/// between the look and the await.
#[derive(Debug, Default)]
pub struct Signals {
    arrived: Notify,
    room: Notify,
}

impl Signals {
    /// A pair of signals nobody has fed yet.
    pub fn new() -> Signals {
        Signals::default()
    }

    /// Ends when a message may be waiting on some pipe's incoming queue, or
    /// the set of pipes changed.
    pub fn arrived(&self) -> Notified<'_> {
        self.arrived.notified()
    }

    /// Ends when some pipe's outgoing queue may have room, or the set of
    /// pipes changed.
    pub fn room(&self) -> Notified<'_> {
        self.room.notified()
    }

    /// Wakes every waiter of both signals: the set of pipes changed, or the
    /// socket closed.
    pub fn pipes_changed(&self) {
        self.arrived.notify_waiters();
        self.room.notify_waiters();
    }
}

/// Which of a socket's signals a queue feeds, if any.
#[derive(Debug, Default)]
enum Link {
    #[default]
    None,
    /// An incoming queue: a message going in is an arrival.
    Incoming(Arc<Signals>),
    /// An outgoing queue: a message coming out is room.
    Outgoing(Arc<Signals>),
}

/// One direction of one pipe: a bounded queue of whole messages.
pub struct Queue {
    config: QueueConfig,
    state: Mutex<QueueState>,
    /// Woken when a message is taken out, or when a taker parks — both are
    /// "there may be room now" for a depth-zero rendezvous.
    room: Notify,
    /// Woken when a message goes in.
    ready: Notify,
    /// Messages queued, mirrored outside the lock so that a scan over many
    /// pipes passes an empty one with a load and not a lock.
    queued: AtomicUsize,
    link: Link,
}

struct QueueState {
    messages: VecDeque<Message>,
    /// The octets the queued messages occupy — what
    /// [`QueueConfig::max_bytes`] bounds. Maintained rather than summed on
    /// every send: a full queue is asked about once per message and the
    /// counter costs less than the walk.
    bytes: u64,
    /// Takers currently parked in [`Queue::recv`]. Only a depth-zero queue
    /// consults it, and for that queue it *is* the room: a rendezvous
    /// accepts a message exactly when somebody is already waiting for one.
    waiting: usize,
    dropped: u64,
    closed: bool,
}

impl Queue {
    /// An empty queue under `config`, feeding no socket-wide signal.
    pub fn new(config: QueueConfig) -> Queue {
        Queue::with_link(config, Link::None)
    }

    fn with_link(config: QueueConfig, link: Link) -> Queue {
        Queue {
            config,
            state: Mutex::new(QueueState {
                messages: VecDeque::new(),
                bytes: 0,
                waiting: 0,
                dropped: 0,
                closed: false,
            }),
            room: Notify::new(),
            ready: Notify::new(),
            queued: AtomicUsize::new(0),
            link,
        }
    }

    /// This queue's two bounds and its action at them.
    pub const fn config(&self) -> QueueConfig {
        self.config
    }

    /// Messages queued right now.
    pub fn len(&self) -> usize {
        self.lock().messages.len()
    }

    /// Whether the queue holds nothing.
    pub fn is_empty(&self) -> bool {
        self.lock().messages.is_empty()
    }

    /// Whether a message was queued when this was last updated, without
    /// taking the queue's lock.
    ///
    /// A hint for a scan over many queues: a `false` may lag a push that
    /// is happening at this moment, which the push's own wake-up covers, and
    /// a `true` may lag a take, which the take itself then finds out.
    pub fn has_messages(&self) -> bool {
        self.queued.load(Ordering::Acquire) != 0
    }

    /// Octets the queued messages occupy — what
    /// [`QueueConfig::max_bytes`] bounds.
    pub fn queued_bytes(&self) -> u64 {
        self.lock().bytes
    }

    /// Messages this queue discarded at its bound.
    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    /// Whether the pipe has been destroyed.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Whether one more message fits **right now**.
    ///
    /// This is the eligibility test PUSH round-robins over: "PUSH selects
    /// only a puller capable of accepting a message, so its rotation is over
    /// the ready subset rather than a static worker list" (§4). For a
    /// depth-zero queue it is true only while a taker is parked, which is
    /// exactly what "capable of accepting" means with no buffer.
    pub fn has_room(&self) -> bool {
        let state = self.lock();
        // One octet, not one message: without the message in hand that is
        // the strongest thing that can be said about the byte ceiling, and
        // the send itself re-checks against the real size.
        !state.closed && self.full(&state, 1).is_none()
    }

    /// Waits until nothing is queued, or the queue is destroyed.
    ///
    /// What a socket whose last handle was dropped waits on before it closes
    /// a pipe: a message queued for the peer is the application's, and the
    /// connection's writer may not have reached it yet. A message the writer
    /// has taken is no longer queued, and is finished however the pipe is
    /// closed behind it.
    pub async fn drained(&self) {
        loop {
            // Created before the look, so a take between the look and the
            // await ends the wait.
            let freed = self.room.notified();
            {
                let state = self.lock();
                if state.messages.is_empty() || state.closed {
                    return;
                }
            }
            freed.await;
        }
    }

    /// Queues `message`, applying this queue's [`FullAction`] at the bound.
    ///
    /// Fails with `NNG_ECLOSED` once the pipe is destroyed: the peer is gone
    /// and its queue went with it.
    pub async fn send(&self, message: Message) -> Result<Sent> {
        let size = message.wire_len() as u64;
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
                if self.full(&state, size).is_none() {
                    self.push(&mut state, message, size);
                    drop(state);
                    self.pushed();
                    return Ok(Sent::Queued);
                }
                match self.config.full {
                    FullAction::Drop => {
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    FullAction::DropOldest => {
                        // Discard the oldest to make room for the newest,
                        // which is what `SUB_PREFNEW` asks for — and as
                        // many as it takes, because one forty-octet message
                        // is not room for a megabyte and pushing anyway
                        // would let the octet ceiling creep upwards one
                        // message at a time.
                        let mut discarded = 0;
                        while self.full(&state, size).is_some() {
                            if self.pop(&mut state).is_none() {
                                break;
                            }
                            discarded += 1;
                        }
                        state.dropped += discarded;
                        if discarded > 0 {
                            // Something was discarded, so the newest takes
                            // its place.
                            self.push(&mut state, message, size);
                            drop(state);
                            self.pushed();
                            self.freed();
                            return Ok(Sent::Queued);
                        }
                        // Nothing to discard — a depth-zero queue with no
                        // taker — so the newest is what goes, which is the
                        // only way the policy means anything there.
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    FullAction::RejectNewest => {
                        state.dropped += 1;
                        return Ok(Sent::Dropped);
                    }
                    // Blocking is the one action that keeps the message:
                    // the loop waits for room and offers it again.
                    FullAction::Block => {}
                }
            }
            room.await;
        }
    }

    /// Queues `message` without ever waiting.
    ///
    /// Reports `NNG_EAGAIN` at the bound **whatever** the action is: a
    /// caller that asked not to block is asking for the refusal, not for a
    /// silent drop. A protocol that drops still drops on the waiting path,
    /// where the drop is its documented behaviour rather than the caller's
    /// choice.
    pub fn try_send(&self, message: Message) -> Result<()> {
        let closed = self.is_closed();
        let size = message.wire_len() as u64;
        match self.offer(message) {
            Ok(()) => Ok(()),
            Err(_) if closed => Err(gone()),
            Err(_) => Err(Error::EAGAIN(self.full_cause(size))),
        }
    }

    /// Queues `message` if there is room **right now**, and hands it back
    /// if there is not.
    ///
    /// The primitive a round-robin needs: a sender that has just seen
    /// [`Queue::has_room`] may still lose the last slot to another thread —
    /// an NNG socket is usable from several at once (§2) — and a send that
    /// swallowed the message on that race would lose it. A closed queue
    /// hands it back too, so a caller rotating over pipes skips a dead one
    /// and keeps its message.
    ///
    /// Builds no error and copies nothing: a refusal is the message itself
    /// coming back, which is what lets a broadcast try every pipe at the
    /// price of a lock each.
    pub fn offer(&self, message: Message) -> std::result::Result<(), Message> {
        let mut state = self.lock();
        let size = message.wire_len() as u64;
        if state.closed || self.full(&state, size).is_some() {
            return Err(message);
        }
        self.push(&mut state, message, size);
        drop(state);
        self.pushed();
        Ok(())
    }

    /// Takes the next message, waiting for one.
    ///
    /// Parking here is also what makes a depth-zero queue accept anything at
    /// all, so the waiter count is published to senders before the wait.
    pub async fn recv(&self) -> Result<Message> {
        loop {
            let mut ready = std::pin::pin!(self.ready.notified());
            ready.as_mut().enable();
            {
                let mut state = self.lock();
                if let Some(message) = self.pop(&mut state) {
                    drop(state);
                    self.freed();
                    return Ok(message);
                }
                if state.closed {
                    return Err(gone());
                }
                state.waiting += 1;
            }
            self.freed();
            let parked = Parked(self);
            ready.await;
            drop(parked);
        }
    }

    /// Takes the next message if one is queued.
    pub fn try_recv(&self) -> Result<Message> {
        let mut state = self.lock();
        match self.pop(&mut state) {
            Some(message) => {
                drop(state);
                self.freed();
                Ok(message)
            }
            None if state.closed => Err(gone()),
            None => Err(Error::EAGAIN("nothing is queued from this peer".into())),
        }
    }

    /// Destroys the queue: discards what it holds and wakes everyone waiting
    /// on it.
    ///
    /// "A pipe is removed when its peer, owning dialer/listener, or
    /// `nng_pipe_close()` closes it" and "communication over that pipe is
    /// then impossible" (§1). Returns how many messages were discarded,
    /// because SP reports nothing to anybody and a count in a log is the
    /// only trace there will be.
    pub fn close(&self) -> usize {
        let discarded = {
            let mut state = self.lock();
            state.closed = true;
            let discarded = state.messages.len();
            state.messages.clear();
            state.bytes = 0;
            self.queued.store(0, Ordering::Release);
            discarded
        };
        self.freed();
        self.pushed();
        discarded
    }

    /// Which bound `size` further octets would cross, if any.
    ///
    /// The byte ceiling is skipped for an empty queue, so a message larger
    /// than the ceiling is queued rather than waited for forever (module
    /// documentation).
    fn full(&self, state: &QueueState, size: u64) -> Option<Full> {
        crosses(
            self.config,
            state.waiting,
            state.messages.len(),
            state.bytes,
            size,
        )
    }

    /// What a refusal of a message of `size` says, naming the bound it met.
    ///
    /// Takes the state lock itself, so a caller must not already hold it: a
    /// `std::sync::Mutex` is not reentrant. [`Queue::try_send`] calls this
    /// only after [`Queue::offer`] has handed the message back.
    fn full_cause(&self, size: u64) -> crate::error::Cause {
        let state = self.lock();
        match self.full(&state, size) {
            Some(Full::Bytes) => format!(
                "this pipe's queue holds {} octets of its {} octet ceiling",
                state.bytes, self.config.max_bytes
            )
            .into(),
            _ if self.config.depth == 0 => {
                "no peer is ready to take a message and this queue has no buffer (depth 0)".into()
            }
            _ => format!(
                "this pipe's queue holds its depth of {} messages",
                self.config.depth
            )
            .into(),
        }
    }

    fn push(&self, state: &mut QueueState, message: Message, size: u64) {
        state.bytes = state.bytes.saturating_add(size);
        state.messages.push_back(message);
        self.queued.store(state.messages.len(), Ordering::Release);
    }

    fn pop(&self, state: &mut QueueState) -> Option<Message> {
        let message = state.messages.pop_front()?;
        state.bytes = state.bytes.saturating_sub(message.wire_len() as u64);
        self.queued.store(state.messages.len(), Ordering::Release);
        Some(message)
    }

    /// A message went in: wake whoever waits on this queue, and the socket's
    /// receivers if this is a pipe's incoming queue.
    fn pushed(&self) {
        self.ready.notify_waiters();
        if let Link::Incoming(signals) = &self.link {
            signals.arrived.notify_waiters();
        }
    }

    /// Room appeared: wake whoever waits on this queue, and the socket's
    /// senders if this is a pipe's outgoing queue.
    fn freed(&self) {
        self.room.notify_waiters();
        if let Link::Outgoing(signals) = &self.link {
            signals.room.notify_waiters();
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().expect("queue poisoned")
    }
}

/// Which of a queue's two bounds a message met.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Full {
    /// [`QueueConfig::depth`]: the message count, or a rendezvous with
    /// nobody waiting.
    Depth,
    /// [`QueueConfig::max_bytes`]: the octet ceiling.
    Bytes,
}

/// Which bound a message of `size` octets would cross in a queue under
/// `config` that holds `len` messages and `bytes` octets with `waiting`
/// takers parked on it.
///
/// One rule for every place that keeps messages under the two bounds: the
/// pipe's queues, and the socket's buffer of what retired pipes had already
/// received. A depth of zero is a rendezvous — room exists only while
/// somebody is waiting for a message, and only for as many messages as
/// there are waiters — and the octet ceiling is skipped for an empty queue
/// so that a message above it is queued rather than waited for for ever.
pub(crate) fn crosses(
    config: QueueConfig,
    waiting: usize,
    len: usize,
    bytes: u64,
    size: u64,
) -> Option<Full> {
    if config.depth == 0 {
        if waiting <= len {
            return Some(Full::Depth);
        }
    } else if len >= config.depth {
        return Some(Full::Depth);
    }
    if config.max_bytes != 0 && len != 0 && bytes.saturating_add(size) > config.max_bytes {
        return Some(Full::Bytes);
    }
    None
}

/// How both directions of one pipe are bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipeConfig {
    /// `NNG_OPT_SENDBUF` and the protocol's action at its bound.
    pub outgoing: QueueConfig,
    /// `NNG_OPT_RECVBUF` and the protocol's action at its bound.
    pub incoming: QueueConfig,
}

impl PipeConfig {
    /// The defaults of `protocol`: NNG's depths and NNG's behaviour at the
    /// bound, so that a socket type that says nothing gets the protocol's
    /// own answer rather than a house one.
    pub const fn of(protocol: EndpointType) -> PipeConfig {
        let sending = FullAction::sending(protocol);
        PipeConfig {
            outgoing: QueueConfig {
                depth: PipeConfig::default_send_depth(protocol),
                max_bytes: DEFAULT_QUEUE_BYTES,
                full: sending,
            },
            incoming: QueueConfig {
                depth: if PipeConfig::receives(protocol) {
                    DEFAULT_RECV_DEPTH
                } else {
                    0
                },
                max_bytes: DEFAULT_QUEUE_BYTES,
                full: FullAction::receiving(protocol),
            },
        }
    }

    /// Whether `protocol` has a receive operation, and so a use for an
    /// incoming queue. PUB and PUSH do not: their queue holds nothing and
    /// discards what a peer writes to them ([`FullAction::receiving`]).
    pub const fn receives(protocol: EndpointType) -> bool {
        !matches!(protocol, EndpointType::Pub | EndpointType::Push)
    }

    /// `NNG_OPT_SENDBUF`'s default for `protocol`: zero where a full queue
    /// makes the sender wait, and [`DEFAULT_BROADCAST_SEND_DEPTH`] where it
    /// makes the sender discard.
    pub const fn default_send_depth(protocol: EndpointType) -> usize {
        match FullAction::sending(protocol) {
            FullAction::Block => DEFAULT_SEND_DEPTH,
            _ => DEFAULT_BROADCAST_SEND_DEPTH,
        }
    }
}

/// One connection's double queue, outgoing and incoming, each bounded on
/// its own.
///
/// A pipe exists **only while its connection does**: "endpoints create
/// pipes, which are message-oriented connected streams and commonly map 1:1
/// to TCP or IPC connections" (§1), and a pipe "is removed when its peer,
/// owning dialer/listener, or `nng_pipe_close()` closes it" (§1). That is
/// the opposite of ZMTP's rule, where a queue is created when a connection
/// is *initiated* and survives reconnects, and it is why nothing here can
/// be queued for a peer that has not arrived.
///
/// Cloning shares the queues: the socket holds one clone and the session
/// driving the connection holds the other, which is the only way a queue
/// can be a rendezvous between them.
#[derive(Clone, Debug)]
pub struct Pipe {
    id: PipeId,
    outgoing: Arc<Queue>,
    incoming: Arc<Queue>,
    /// `NNG_OPT_RECVMAXSZ` of the endpoint that made this pipe, which is
    /// what a protocol that takes a header off a message judges the rest
    /// against. Zero is "no limit".
    recv_limit: u64,
}

impl Pipe {
    /// A pipe with both queues empty, feeding no socket-wide signal and
    /// with no payload limit.
    pub fn new(id: PipeId, config: PipeConfig) -> Pipe {
        Pipe {
            id,
            outgoing: Arc::new(Queue::new(config.outgoing)),
            incoming: Arc::new(Queue::new(config.incoming)),
            recv_limit: 0,
        }
    }

    /// A pipe whose queues feed `signals`: arrivals on the incoming side,
    /// room on the outgoing one.
    pub fn linked(id: PipeId, config: PipeConfig, signals: &Arc<Signals>) -> Pipe {
        Pipe {
            id,
            outgoing: Arc::new(Queue::with_link(
                config.outgoing,
                Link::Outgoing(Arc::clone(signals)),
            )),
            incoming: Arc::new(Queue::with_link(
                config.incoming,
                Link::Incoming(Arc::clone(signals)),
            )),
            recv_limit: 0,
        }
    }

    /// This pipe with `limit` as the payload limit its endpoint set, in
    /// octets, where zero is no limit.
    pub fn with_recv_limit(mut self, limit: u64) -> Pipe {
        self.recv_limit = limit;
        self
    }

    /// This pipe's id within its socket.
    pub const fn id(&self) -> PipeId {
        self.id
    }

    /// Whether a payload of `len` octets is within what this pipe's
    /// endpoint admits.
    ///
    /// The transport judges a message by its declared length, header and
    /// body together, before it knows where one ends; a protocol that takes
    /// a header off the message — a tag stack — judges the payload that is
    /// left with this, which is `NNG_OPT_RECVMAXSZ` exactly.
    pub fn admits_payload(&self, len: usize) -> bool {
        self.recv_limit == 0 || len as u64 <= self.recv_limit
    }

    /// Messages on their way to the peer.
    pub fn outgoing(&self) -> &Queue {
        &self.outgoing
    }

    /// Messages that arrived from the peer.
    pub fn incoming(&self) -> &Queue {
        &self.incoming
    }

    /// Destroys both queues and reports what they held, which is the only
    /// trace a discarded message leaves: SP tells nobody anything (§6).
    pub fn close(&self) -> Discarded {
        Discarded {
            outgoing: self.outgoing.close(),
            incoming: self.incoming.close(),
        }
    }

    /// Whether this pipe has been destroyed.
    pub fn is_closed(&self) -> bool {
        self.outgoing.is_closed()
    }
}

/// What destroying a pipe discarded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Discarded {
    /// Messages queued for the peer that will never be sent.
    pub outgoing: usize,
    /// Messages received from the peer that will never be delivered.
    pub incoming: usize,
}

/// Decrements the parked-taker count however [`Queue::recv`]'s await ends,
/// cancellation included: a cancelled receive that left its count behind
/// would make a depth-zero queue accept a message nobody is waiting for.
struct Parked<'a>(&'a Queue);

impl Drop for Parked<'_> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.waiting = state.waiting.saturating_sub(1);
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Queue")
            .field("depth", &self.config.depth)
            .field("max_bytes", &self.config.max_bytes)
            .field("full", &self.config.full)
            .field("queued", &state.messages.len())
            .field("queued_bytes", &state.bytes)
            .field("dropped", &state.dropped)
            .field("closed", &state.closed)
            .finish()
    }
}

fn gone() -> Error {
    Error::ECLOSED("this pipe is closed; its queue went with it".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(depth: usize, full: FullAction) -> QueueConfig {
        QueueConfig {
            depth,
            max_bytes: DEFAULT_QUEUE_BYTES,
            full,
        }
    }

    /// A queue bounded only by its octet ceiling: the depth is out of the
    /// way, so what refuses is the ceiling.
    fn byte_queue(max_bytes: u64, full: FullAction) -> Queue {
        Queue::new(QueueConfig {
            depth: MAX_QUEUE_DEPTH,
            max_bytes,
            full,
        })
    }

    fn msg(n: u8) -> Message {
        Message::from_body(vec![n])
    }

    /// Claim: the per-protocol table is the sheet's sentence — PUSH and PAIR
    /// block, BUS and PUB and SURVEYOR drop — plus the two repliers, whose
    /// reply is addressed to one pipe and must not wait for it; and an
    /// incoming queue blocks, because `SUB_PREFNEW` applies to the admitted
    /// publications on the socket rather than to everything that arrived —
    /// except on the two protocols with no receive operation, which discard.
    #[test]
    fn the_full_queue_action_is_the_protocols_own() {
        assert_eq!(FullAction::sending(EndpointType::Push), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::PairV0), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::PairV1), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::Req), FullAction::Block);
        assert_eq!(FullAction::sending(EndpointType::Bus), FullAction::Drop);
        assert_eq!(FullAction::sending(EndpointType::Pub), FullAction::Drop);
        assert_eq!(
            FullAction::sending(EndpointType::Surveyor),
            FullAction::Drop
        );
        assert_eq!(FullAction::sending(EndpointType::Rep), FullAction::Drop);
        assert_eq!(
            FullAction::sending(EndpointType::Respondent),
            FullAction::Drop
        );
        assert_eq!(FullAction::receiving(EndpointType::Sub), FullAction::Block);
        assert_eq!(FullAction::receiving(EndpointType::Pull), FullAction::Block);
        assert_eq!(FullAction::receiving(EndpointType::Rep), FullAction::Block);
        assert_eq!(FullAction::receiving(EndpointType::Pub), FullAction::Drop);
        assert_eq!(FullAction::receiving(EndpointType::Push), FullAction::Drop);
    }

    /// Claim: a reply queue drops when it is full instead of making its
    /// sender wait. A queue under REP's configuration that is full accepts
    /// the next message as a discard and the sender goes on, which is what
    /// keeps one requester that stopped reading from parking a replier.
    #[test]
    fn a_reply_queue_that_is_full_drops_instead_of_waiting() {
        let config = PipeConfig::of(EndpointType::Rep);
        assert_eq!(config.outgoing.full, FullAction::Drop);
        assert!(
            config.outgoing.depth > 0,
            "a depth of zero would drop every reply while the writer is busy"
        );
        let queue = Queue::new(QueueConfig {
            depth: 1,
            ..config.outgoing
        });
        futures::executor::block_on(async {
            assert_eq!(queue.send(msg(1)).await.expect("room"), Sent::Queued);
            let second = futures::poll!(std::pin::pin!(queue.send(msg(2))));
            assert_eq!(
                second,
                std::task::Poll::Ready(Ok(Sent::Dropped)),
                "a full reply queue must not make the sender wait"
            );
        });
    }

    /// Claim: a PUB or PUSH pipe holds nothing a peer writes to it. The
    /// incoming queue of a protocol with no receive operation discards on
    /// arrival, so a peer that streams data at it fills no memory.
    #[test]
    fn a_protocol_with_no_receive_operation_discards_what_arrives() {
        for protocol in [EndpointType::Pub, EndpointType::Push] {
            let config = PipeConfig::of(protocol);
            assert!(!PipeConfig::receives(protocol));
            let pipe = Pipe::new(PipeId::new(1), config);
            futures::executor::block_on(async {
                for _ in 0..20 {
                    let sent = pipe
                        .incoming()
                        .send(Message::from_body(vec![0u8; 1024 * 1024]))
                        .await
                        .expect("accepted as a discard");
                    assert_eq!(sent, Sent::Dropped, "{protocol:?}");
                }
            });
            assert_eq!(pipe.incoming().queued_bytes(), 0, "{protocol:?}");
            assert!(pipe.incoming().is_empty(), "{protocol:?}");
            assert_eq!(pipe.incoming().dropped(), 20, "{protocol:?}");
        }
        assert!(PipeConfig::receives(EndpointType::Rep));
        assert!(PipeConfig::of(EndpointType::Pull).incoming.depth > 0);
    }

    /// Claim: a wait created before a message goes in — on any pipe of a
    /// socket — ends when it does, and so does one for room, with the wait
    /// made *before* the look; the lost wake-up this rules out is a receive
    /// that parks on a snapshot taken a moment before a pipe delivered.
    #[test]
    fn a_pipe_feeds_its_sockets_signals() {
        let signals = Arc::new(Signals::new());
        let config = PipeConfig::of(EndpointType::Pull);
        let quiet = Pipe::linked(PipeId::new(1), config, &signals);
        let busy = Pipe::linked(PipeId::new(2), config, &signals);
        futures::executor::block_on(async {
            let mut arrived = std::pin::pin!(signals.arrived());
            assert!(
                futures::poll!(arrived.as_mut()).is_pending(),
                "nothing has arrived"
            );
            busy.incoming().send(msg(1)).await.expect("queued");
            assert!(
                futures::poll!(arrived.as_mut()).is_ready(),
                "a message on any pipe ends the wait for one"
            );
            assert!(!quiet.incoming().has_messages());
            assert!(busy.incoming().has_messages());

            let mut room = std::pin::pin!(signals.room());
            assert!(futures::poll!(room.as_mut()).is_pending());
            busy.outgoing().close();
            assert!(
                futures::poll!(room.as_mut()).is_ready(),
                "a pipe going away is a reason to look again"
            );
        });

        // A wait created before the message is ended by it even if it is
        // first polled afterwards, which is what `notified()` promises and
        // what a snapshot-then-wait caller relies on.
        futures::executor::block_on(async {
            let early = signals.arrived();
            quiet.incoming().send(msg(2)).await.expect("queued");
            let mut early = std::pin::pin!(early);
            assert!(futures::poll!(early.as_mut()).is_ready());
        });
    }

    /// Claim: a depth-zero queue is a rendezvous and not an unbounded one.
    /// This is PUSH's documented default, and getting it backwards would
    /// turn "no buffer" into "no bound".
    #[test]
    fn depth_zero_is_a_rendezvous() {
        let queue = Queue::new(config(0, FullAction::Block));
        assert!(!queue.has_room(), "nobody is waiting, so nothing fits");
        assert!(matches!(queue.try_send(msg(1)), Err(Error::EAGAIN(_))));
        assert_eq!(queue.len(), 0);

        futures::executor::block_on(async {
            let taking = queue.recv();
            let mut taking = std::pin::pin!(taking);
            // Poll once so the taker is parked and its room is published.
            assert!(
                futures::poll!(taking.as_mut()).is_pending(),
                "nothing is queued yet"
            );
            assert!(queue.has_room(), "a parked taker is the room");
            queue.try_send(msg(7)).expect("the rendezvous accepts one");
            assert!(
                queue.try_send(msg(8)).is_err(),
                "one waiter is room for one message"
            );
            assert_eq!(taking.await.expect("delivered").body(), [7]);
        });
    }

    /// Claim: the octet ceiling binds before the depth, which is the bound
    /// a message count cannot express. [`MAX_QUEUE_DEPTH`] is the deepest
    /// queue NNG admits, and 8192 messages of `NNG_OPT_RECVMAXSZ` is 8 GiB
    /// for one direction of one pipe — so at that depth what stops a peer
    /// sending many small messages is [`DEFAULT_QUEUE_BYTES`] and not the
    /// count, and the refusal names the octets because the count is nowhere
    /// near its bound.
    #[test]
    fn the_octet_ceiling_binds_before_the_depth() {
        let queue = byte_queue(DEFAULT_QUEUE_BYTES, FullAction::Block);
        futures::executor::block_on(async {
            let chunk = Message::from_body(vec![0u8; 4096]);
            let fits = (DEFAULT_QUEUE_BYTES / 4096) as usize;
            for n in 0..fits {
                assert_eq!(
                    queue.send(chunk.clone()).await.expect("under the ceiling"),
                    Sent::Queued,
                    "message {n}"
                );
            }
            assert_eq!(queue.queued_bytes(), DEFAULT_QUEUE_BYTES);
            assert_eq!(queue.len(), fits, "8 MiB of 4 KiB messages");
            assert!(fits < MAX_QUEUE_DEPTH, "with the depth nowhere near full");

            let err = queue.try_send(chunk.clone()).unwrap_err();
            assert!(matches!(err, Error::EAGAIN(_)), "{err:?}");
            assert!(
                err.cause().contains(&DEFAULT_QUEUE_BYTES.to_string()),
                "{}",
                err.cause()
            );
            assert!(err.cause().contains("octet"), "{}", err.cause());

            // Octets, not slots, are what a taker frees here.
            assert_eq!(queue.recv().await.expect("one out").len(), 4096);
            assert_eq!(queue.queued_bytes(), DEFAULT_QUEUE_BYTES - 4096);
            assert_eq!(queue.send(chunk).await.expect("room again"), Sent::Queued);
        });
    }

    /// Claim: an empty queue accepts one message however large, so the
    /// octet ceiling never becomes a deadlock — a blocking protocol holding
    /// a message above the ceiling would otherwise wait for room that
    /// cannot appear. The exemption is for an empty queue and not for every
    /// large message.
    #[test]
    fn an_empty_queue_accepts_a_message_above_its_ceiling() {
        let queue = byte_queue(64, FullAction::Block);
        futures::executor::block_on(async {
            let huge = Message::from_body(vec![7u8; 4096]);
            assert_eq!(
                queue.send(huge).await.expect("the one message"),
                Sent::Queued
            );
            assert_eq!(queue.queued_bytes(), 4096);

            let second = queue.send(Message::from_body(vec![1u8; 4096]));
            let mut second = std::pin::pin!(second);
            assert!(
                futures::poll!(second.as_mut()).is_pending(),
                "the queue is no longer empty, so the ceiling applies"
            );
            assert_eq!(queue.recv().await.expect("the first out").len(), 4096);
            assert_eq!(
                second.await.expect("queued once the queue emptied"),
                Sent::Queued
            );
            assert_eq!(queue.dropped(), 0, "a blocking queue discards nothing");
        });
    }

    /// Claim: a blocking queue makes the sender wait rather than discard,
    /// and the wait ends when a taker frees a slot. This is PUSH and PAIR.
    #[test]
    fn a_blocking_queue_waits_for_room() {
        let queue = Queue::new(config(1, FullAction::Block));
        futures::executor::block_on(async {
            assert_eq!(queue.send(msg(1)).await.expect("first"), Sent::Queued);

            let second = queue.send(msg(2));
            let mut second = std::pin::pin!(second);
            assert!(
                futures::poll!(second.as_mut()).is_pending(),
                "the queue is full"
            );
            assert_eq!(queue.dropped(), 0, "a blocking queue discards nothing");

            assert_eq!(queue.recv().await.expect("first out").body(), [1]);
            assert_eq!(second.await.expect("second in"), Sent::Queued);
            assert_eq!(queue.recv().await.expect("second out").body(), [2]);
        });
    }

    /// Claim: a dropping queue keeps what it has, discards the new message,
    /// counts it, and still reports success — BUS "discards when a peer
    /// cannot receive" while "the originating send succeeds without
    /// blocking" (§4).
    #[test]
    fn a_dropping_queue_discards_the_new_message_and_succeeds() {
        let queue = Queue::new(config(1, FullAction::Drop));
        futures::executor::block_on(async {
            assert_eq!(queue.send(msg(1)).await.expect("first"), Sent::Queued);
            assert_eq!(queue.send(msg(2)).await.expect("second"), Sent::Dropped);
            assert_eq!(queue.dropped(), 1);
            assert_eq!(queue.len(), 1);
            assert_eq!(
                queue.recv().await.expect("what survived").body(),
                [1],
                "the queued message is the one that was kept"
            );
        });
    }

    /// Claim: `SUB_PREFNEW=true` drops the oldest to make room, which is the
    /// opposite choice from the same full queue, and it is observable in
    /// what comes out.
    #[test]
    fn dropping_the_oldest_keeps_the_newest() {
        let queue = Queue::new(config(2, FullAction::DropOldest));
        futures::executor::block_on(async {
            for n in 1..=4 {
                assert_eq!(queue.send(msg(n)).await.expect("sent"), Sent::Queued);
            }
            assert_eq!(queue.dropped(), 2);
            assert_eq!(queue.recv().await.expect("oldest survivor").body(), [3]);
            assert_eq!(queue.recv().await.expect("newest").body(), [4]);
        });
    }

    /// Claim: `SUB_PREFNEW=false` rejects the new message instead, so the
    /// two policies differ in what a reader sees and not merely in a
    /// counter.
    #[test]
    fn rejecting_the_newest_keeps_the_oldest() {
        let queue = Queue::new(config(2, FullAction::RejectNewest));
        futures::executor::block_on(async {
            for n in 1..=4 {
                let sent = queue.send(msg(n)).await.expect("sent");
                assert_eq!(sent, if n <= 2 { Sent::Queued } else { Sent::Dropped });
            }
            assert_eq!(queue.dropped(), 2);
            assert_eq!(queue.recv().await.expect("first").body(), [1]);
            assert_eq!(queue.recv().await.expect("second").body(), [2]);
        });
    }

    /// Claim: a non-blocking send refuses at the bound whatever the
    /// protocol's action is — the caller asked for the refusal — and a
    /// non-blocking receive refuses an empty queue.
    #[test]
    fn the_non_blocking_forms_refuse_rather_than_drop() {
        for action in [
            FullAction::Block,
            FullAction::Drop,
            FullAction::DropOldest,
            FullAction::RejectNewest,
        ] {
            let queue = Queue::new(config(1, action));
            queue.try_send(msg(1)).expect("room for one");
            let err = queue.try_send(msg(2)).unwrap_err();
            assert!(matches!(err, Error::EAGAIN(_)), "{action:?}: {err:?}");
            assert_eq!(queue.dropped(), 0, "{action:?} discarded on the try path");
            assert_eq!(queue.try_recv().expect("the queued one").body(), [1]);
            assert!(matches!(queue.try_recv(), Err(Error::EAGAIN(_))));
        }
    }

    /// Claim: closing a pipe discards what its queue held, reports how much,
    /// and makes every further operation `NNG_ECLOSED` — including one that
    /// was already waiting, which is how a parked receive is unblocked when
    /// a peer goes away.
    #[test]
    fn closing_a_queue_discards_and_unblocks() {
        let queue = Queue::new(config(4, FullAction::Block));
        futures::executor::block_on(async {
            queue.send(msg(1)).await.expect("first");
            queue.send(msg(2)).await.expect("second");

            let waiting = Queue::new(config(1, FullAction::Block));
            let parked = waiting.recv();
            let mut parked = std::pin::pin!(parked);
            assert!(futures::poll!(parked.as_mut()).is_pending());
            assert_eq!(waiting.close(), 0);
            assert!(matches!(parked.await, Err(Error::ECLOSED(_))));

            assert_eq!(queue.close(), 2);
            assert!(queue.is_closed());
            assert!(matches!(queue.try_send(msg(3)), Err(Error::ECLOSED(_))));
            assert!(matches!(queue.send(msg(3)).await, Err(Error::ECLOSED(_))));
            assert!(matches!(queue.try_recv(), Err(Error::ECLOSED(_))));
        });
    }

    /// Claim: a depth NNG refuses is refused here, at configuration time,
    /// with the option named — never silently clamped.
    #[test]
    fn a_depth_out_of_nngs_range_is_refused_where_it_is_configured() {
        assert!(
            config(MAX_QUEUE_DEPTH, FullAction::Block)
                .validate("NNG_OPT_SENDBUF")
                .is_ok()
        );
        assert!(
            config(0, FullAction::Block)
                .validate("NNG_OPT_SENDBUF")
                .is_ok()
        );
        let err = config(MAX_QUEUE_DEPTH + 1, FullAction::Block)
            .validate("NNG_OPT_RECVBUF")
            .unwrap_err();
        assert!(matches!(err, Error::EINVAL(_)), "{err:?}");
        assert!(err.cause().contains("NNG_OPT_RECVBUF"));
        assert!(err.cause().contains("8192"));
    }

    /// Claim: a pipe ID fits the 31 bits a tag stack leaves for it, because
    /// the top bit is the stack's terminator and a device pushes pipe IDs
    /// onto that stack.
    #[test]
    fn a_pipe_id_fits_the_tag_stack() {
        assert_eq!(PipeId::new(7).get(), 7);
        assert_eq!(PipeId::new(u32::MAX).get(), PipeId::MAX);
        assert_eq!(PipeId::MAX, weida_sp::backtrace::MAX_ID);
        assert_eq!(PipeId::new(9).to_string(), "pipe 9");
    }
}
