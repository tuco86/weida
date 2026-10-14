//! The session: `begin`, `end`, and the two independently numbered
//! directions.
//!
//! A session "correlates two unidirectional channels into a bidirectional
//! sequential conversation" (Part 2 §2.5), and the word *unidirectional* is
//! the part that catches implementers out. Channels are numbered per
//! direction and the two numbers are unrelated:
//!
//! ```text
//! client                                              broker
//!   begin(remote-channel=null)        -- channel 0 -->
//!                                     <-- channel 4 -- begin(remote-channel=0)
//!   transfer                          -- channel 0 -->
//!                                     <-- channel 4 -- disposition
//! ```
//!
//! So one session here holds *two* channel numbers: the one it writes into
//! every frame it sends, and the one the peer writes into every frame it
//! sends. The answering `begin` is what ties them together — it arrives on
//! the peer's outgoing channel and carries our outgoing channel in
//! `remote-channel` — and until it arrives there is nothing to route incoming
//! frames by.
//!
//! # Channels are allocated lowest-free
//!
//! Part 2 §2.5.1: "implementations are RECOMMENDED always to use the lowest
//! free channel number". Not cosmetic — it keeps the session table dense, and
//! the table is bounded by the *peer's* `channel-max` rather than ours,
//! because that is the number that says what channels the peer will accept.
//!
//! # `end`, and the `DISCARDING` state
//!
//! An orderly `end` is answered with an `end`. An `end` carrying an error is
//! different: the sender "MUST send `end(error=...)` and then silently
//! discard all incoming frames until the partner's `end`"
//! (Part 2 §2.5.4-2.5.5). Discarding is not ignoring — the frames are counted
//! and logged — but nothing in them is acted upon, because the session's
//! state is by definition no longer trustworthy. [`SessionState::Discarding`]
//! is that state, and it exists as a value rather than as a comment so that a
//! test can assert a frame arriving in it changed nothing.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use weida_amqp_codec::frame::{self, FrameKind};
use weida_amqp_codec::performative::{Begin, End, Flow, Performative};
use weida_amqp_codec::types::condition;

use crate::error::{Condition, Error, Result};
use crate::link::{Link, LinkOptions, Links};
use crate::options::multiple;
use crate::window::{DEFAULT_WINDOW, RemoteFlow, Windows};
use weida_runtime::Exec;

/// How many frames may be queued for one session before the driver waits.
///
/// **A bound of ours.** The session's own `incoming-window` bounds how many
/// `transfer` frames may be in flight, but `attach`, `flow`, `disposition`
/// and `detach` are outside that window entirely — the specification puts no
/// limit on them at all. This is the limit, and reaching it applies
/// backpressure to the connection driver rather than growing a queue nobody
/// bounded.
pub const SESSION_QUEUE: usize = 64;

/// `begin.handle-max` this client advertises, bounding the link table.
///
/// **The protocol's own bound, set to a number rather than left at the
/// default.** `handle-max` defaults to `4294967295`, which is 2^32 links per
/// session; 255 is the number this client will actually accept, and it is
/// what makes the link table bounded rather than merely large.
pub const DEFAULT_HANDLE_MAX: u32 = 255;

/// How a session is begun.
#[derive(Clone, Debug)]
pub struct SessionOptions {
    /// `begin.incoming-window`, in `transfer` frames. Mandatory on the wire,
    /// which is what makes it one of the three bounds in AMQP that are safe
    /// by construction.
    pub incoming_window: u32,
    /// `begin.outgoing-window`, in `transfer` frames.
    pub outgoing_window: u32,
    /// `begin.handle-max`: the highest link handle this end will accept.
    pub handle_max: u32,
    /// `begin.offered-capabilities`.
    pub offered_capabilities: Vec<String>,
    /// `begin.desired-capabilities`.
    pub desired_capabilities: Vec<String>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            incoming_window: DEFAULT_WINDOW,
            outgoing_window: DEFAULT_WINDOW,
            handle_max: DEFAULT_HANDLE_MAX,
            offered_capabilities: Vec::new(),
            desired_capabilities: Vec::new(),
        }
    }
}

/// Where a session is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionState {
    /// `begin` sent, the answering `begin` not yet seen.
    Beginning,
    /// Both `begin` frames exchanged.
    Begun,
    /// `end` sent without an error; waiting for the partner's.
    Ending,
    /// `end(error=...)` sent or received. Every incoming frame is discarded
    /// until the partner's `end` arrives.
    Discarding(Condition),
    /// Done.
    Ended(Option<Condition>),
}

impl SessionState {
    /// Whether links may be attached and transfers sent.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Begun)
    }

    /// Whether incoming frames are being discarded rather than acted upon.
    #[must_use]
    pub const fn is_discarding(&self) -> bool {
        matches!(self, Self::Discarding(_))
    }
}

/// What a session hands to the layer above it.
#[derive(Clone, Debug)]
pub enum SessionEvent {
    /// A frame for this session, whole and undecoded.
    ///
    /// Undecoded on purpose: the connection driver decodes each frame far
    /// enough to do the session's own bookkeeping — complete a `begin`, apply
    /// a `flow` to the windows, count a `transfer` against the incoming
    /// window — and the link layer decodes it again for the fields only a
    /// link can interpret. One extra decode of a small performative against
    /// one owned mirror of all nine of them is a trade this crate makes
    /// deliberately.
    Frame(Vec<u8>),
    /// The session ended.
    Ended(Option<Condition>),
}

/// The state a session's handle and the connection driver share.
#[derive(Debug)]
pub(crate) struct Shared {
    /// The channel this end writes into every frame it sends.
    pub(crate) outgoing_channel: u16,
    /// The channel the peer writes into every frame it sends. `None` until
    /// the answering `begin` arrives.
    pub(crate) incoming_channel: Mutex<Option<u16>>,
    pub(crate) windows: Mutex<Windows>,
    pub(crate) state: Mutex<SessionState>,
    pub(crate) options: SessionOptions,
}

/// An AMQP 1.0 session.
#[derive(Debug)]
pub struct Session {
    shared: Arc<Shared>,
    /// Frames on their way out, routed through the connection driver so that
    /// `close` stays the last thing ever written.
    outbound: mpsc::Sender<crate::connection::Outbound>,
    events: mpsc::Receiver<SessionEvent>,
    /// The reactor, for bounding the wait on an answering `attach`. The
    /// protocol gives no deadline for it, so the deadline is ours.
    exec: Exec,
    attach_timeout: Duration,
}

impl Session {
    pub(crate) fn new(
        shared: Arc<Shared>,
        outbound: mpsc::Sender<crate::connection::Outbound>,
        events: mpsc::Receiver<SessionEvent>,
        exec: Exec,
        attach_timeout: Duration,
    ) -> Self {
        Self {
            shared,
            outbound,
            events,
            exec,
            attach_timeout,
        }
    }

    /// Attaches a link on the lowest free handle.
    ///
    /// Returns once the answering `attach` has arrived, because until then
    /// nothing is known about what the peer actually created — which
    /// terminus, which settle modes, which handle to expect its frames on.
    ///
    /// A link whose name and direction are already attached on this
    /// connection **steals** the incumbent: the first is detached with
    /// `amqp:link:stolen` and this one proceeds. That is Part 2 §2.6.1's
    /// rule and not a convenience — it is what makes re-establishment work
    /// when only one party has noticed a failure.
    pub async fn attach(&self, options: LinkOptions) -> Result<Link> {
        options.validate()?;
        if !self.state().is_usable() {
            return Err(Error::ConnectionGone);
        }
        let (reply, wait) = oneshot::channel();
        self.outbound
            .send(crate::connection::Outbound::Attach {
                channel: self.shared.outgoing_channel,
                options: Box::new(options),
                reply,
            })
            .await
            .map_err(|_| Error::ConnectionGone)?;
        match self.exec.within(self.attach_timeout, wait).await {
            Some(Ok(result)) => result,
            Some(Err(_)) => Err(Error::ConnectionGone),
            None => Err(Error::HandshakeTimeout {
                step: "the answering attach",
            }),
        }
    }

    /// The channel this session writes into the frames it sends.
    #[must_use]
    pub fn outgoing_channel(&self) -> u16 {
        self.shared.outgoing_channel
    }

    /// The channel the peer writes into the frames it sends.
    ///
    /// A different number from [`Session::outgoing_channel`] in general, and
    /// the reason this type carries two.
    #[must_use]
    pub fn incoming_channel(&self) -> Option<u16> {
        *self.shared.incoming_channel.lock().expect("not poisoned")
    }

    /// Where the session is.
    #[must_use]
    pub fn state(&self) -> SessionState {
        self.shared.state.lock().expect("not poisoned").clone()
    }

    /// A snapshot of the six flow-control variables.
    #[must_use]
    pub fn windows(&self) -> Windows {
        *self.shared.windows.lock().expect("not poisoned")
    }

    /// Whether one more `transfer` frame may be sent without exceeding the
    /// peer's incoming window.
    #[must_use]
    pub fn may_send(&self) -> bool {
        self.state().is_usable() && self.windows().may_send()
    }

    /// The next event, or `None` once the session has ended and everything
    /// queued has been taken.
    pub async fn next_event(&mut self) -> Option<SessionEvent> {
        self.events.recv().await
    }

    /// Sends a `flow` carrying this session's state, replenishing the
    /// incoming window.
    ///
    /// The receiver's half of the scheme: the window shrinks as frames arrive
    /// and only grows again when the receiver says so, which is exactly this
    /// frame. `echo` asks the peer for its own state at the earliest
    /// convenient opportunity — and answering an echo with an echo loops
    /// forever, so this client never sets it in response to one.
    pub async fn flow(&self, echo: bool) -> Result<()> {
        let (next_incoming_id, next_outgoing_id, outgoing_window, incoming_window) = {
            let mut windows = self.shared.windows.lock().expect("not poisoned");
            windows.replenish_incoming(self.shared.options.incoming_window);
            (
                windows.wire_next_incoming_id(),
                windows.next_outgoing_id,
                windows.outgoing_window,
                windows.incoming_window,
            )
        };
        let mut flow = Flow::session(incoming_window, next_outgoing_id, outgoing_window);
        flow.next_incoming_id = next_incoming_id;
        flow.echo = echo;
        self.send(Performative::Flow(flow)).await
    }

    /// Sends one performative on this session's outgoing channel.
    pub(crate) async fn send(&self, performative: Performative<'_>) -> Result<()> {
        let mut bytes = Vec::new();
        frame::write(
            &mut bytes,
            FrameKind::Amqp,
            self.shared.outgoing_channel,
            u32::MAX,
            |body| performative.encode(body),
        )?;
        self.outbound
            .send(crate::connection::Outbound::Frame(bytes))
            .await
            .map_err(|_| Error::ConnectionGone)
    }

    /// Ends the session.
    ///
    /// Idempotent: ending an ended session is `Ok(())`.
    pub async fn end(&self) -> Result<()> {
        self.end_with(None).await
    }

    /// Ends the session with an error.
    ///
    /// The sender then enters [`SessionState::Discarding`] and silently
    /// discards all incoming frames until the partner's `end` arrives, which
    /// is what Part 2 §2.5.4 requires: the session's state is no longer
    /// trustworthy, so acting on anything in it would be acting on a
    /// misunderstanding.
    pub async fn end_with(&self, error: Option<Condition>) -> Result<()> {
        {
            let mut state = self.shared.state.lock().expect("not poisoned");
            if matches!(*state, SessionState::Ended(_)) {
                return Ok(());
            }
            *state = match &error {
                Some(condition) => SessionState::Discarding(condition.clone()),
                None => SessionState::Ending,
            };
        }
        let codec = error.as_ref().map(Condition::as_codec);
        self.send(Performative::End(End { error: codec })).await
    }
}

/// The connection driver's side of every session on one connection.
///
/// Two maps rather than one, because the two channel numberings are
/// independent: sessions are keyed by *our* outgoing channel, and a second
/// map translates the peer's outgoing channel into it. A single map would
/// have to assume the two numbers agree, which they usually do not.
#[derive(Debug, Default)]
pub(crate) struct Table {
    by_outgoing: HashMap<u16, Entry>,
    incoming_to_outgoing: HashMap<u16, u16>,
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) shared: Arc<Shared>,
    /// This session's links, owned by the driver.
    pub(crate) links: Links,
    /// The `handle-max` the peer advertised in its answering `begin`. The
    /// bound on this session's link table, and the peer's number rather
    /// than ours.
    pub(crate) remote_handle_max: u32,
    pub(crate) events: mpsc::Sender<SessionEvent>,
    /// The caller waiting for the answering `begin`.
    pub(crate) pending: Option<oneshot::Sender<Result<Session>>>,
    /// The outbound channel a handed-out `Session` writes through, kept so
    /// that the handle can be built once the answer arrives.
    pub(crate) outbound: mpsc::Sender<crate::connection::Outbound>,
    /// The receiver half, held until the handle is built.
    pub(crate) rx: Option<mpsc::Receiver<SessionEvent>>,
}

impl Table {
    /// The lowest free outgoing channel, or `None` where the peer's
    /// `channel-max` leaves none.
    ///
    /// The *peer's* maximum, not ours: `channel-max` states the highest
    /// channel number its sender will accept, so the bound on what we may
    /// use is the one the peer advertised.
    pub(crate) fn lowest_free(&self, peer_channel_max: u16) -> Option<u16> {
        (0..=peer_channel_max).find(|channel| !self.by_outgoing.contains_key(channel))
    }

    pub(crate) fn insert(&mut self, channel: u16, entry: Entry) {
        self.by_outgoing.insert(channel, entry);
    }

    pub(crate) fn get_mut(&mut self, outgoing: u16) -> Option<&mut Entry> {
        self.by_outgoing.get_mut(&outgoing)
    }

    /// The session a frame arriving on `incoming` belongs to.
    pub(crate) fn by_incoming(&mut self, incoming: u16) -> Option<&mut Entry> {
        let outgoing = *self.incoming_to_outgoing.get(&incoming)?;
        self.by_outgoing.get_mut(&outgoing)
    }

    /// Ties the peer's outgoing channel to ours, which is what the answering
    /// `begin` is for.
    pub(crate) fn map_incoming(&mut self, incoming: u16, outgoing: u16) {
        self.incoming_to_outgoing.insert(incoming, outgoing);
    }

    pub(crate) fn remove(&mut self, outgoing: u16) -> Option<Entry> {
        self.incoming_to_outgoing
            .retain(|_, mapped| *mapped != outgoing);
        self.by_outgoing.remove(&outgoing)
    }

    pub(crate) fn is_mapped(&self, incoming: u16) -> bool {
        self.incoming_to_outgoing.contains_key(&incoming)
    }

    pub(crate) fn len(&self) -> usize {
        self.by_outgoing.len()
    }

    /// Every session, for tearing them all down when the connection goes.
    pub(crate) fn drain(&mut self) -> Vec<Entry> {
        self.incoming_to_outgoing.clear();
        self.by_outgoing.drain().map(|(_, entry)| entry).collect()
    }
}

/// Builds this client's `begin` for a session on `channel`.
pub(crate) fn begin_frame(
    channel: u16,
    options: &SessionOptions,
    windows: &Windows,
) -> Result<Vec<u8>> {
    let mut begin = Begin::new(
        windows.next_outgoing_id,
        options.incoming_window,
        options.outgoing_window,
    );
    begin.handle_max = options.handle_max;
    begin.offered_capabilities = multiple(&options.offered_capabilities);
    begin.desired_capabilities = multiple(&options.desired_capabilities);
    let mut bytes = Vec::new();
    frame::write(&mut bytes, FrameKind::Amqp, channel, u32::MAX, |body| {
        Performative::Begin(begin).encode(body)
    })?;
    Ok(bytes)
}

/// The flow state a `begin` or a `flow` carries.
pub(crate) fn remote_flow_of_begin(begin: &Begin<'_>) -> RemoteFlow {
    RemoteFlow {
        // A `begin` has no `next-incoming-id` field at all, which is the
        // "the peer has not seen our begin" case of the window formula.
        next_incoming_id: None,
        incoming_window: begin.incoming_window,
        next_outgoing_id: begin.next_outgoing_id,
        outgoing_window: begin.outgoing_window,
    }
}

/// The flow state a `flow` carries.
pub(crate) fn remote_flow_of_flow(flow: &Flow<'_>) -> RemoteFlow {
    RemoteFlow {
        next_incoming_id: flow.next_incoming_id,
        incoming_window: flow.incoming_window,
        next_outgoing_id: flow.next_outgoing_id,
        outgoing_window: flow.outgoing_window,
    }
}

/// The condition this client answers a frame on an in-range but unmapped
/// channel with.
///
/// **Ours, because the specification names none.** Part 2 §2.7.1 covers a
/// channel *above* `channel-max`: that "MUST close the connection with
/// `amqp:connection:framing-error`". A channel inside the range that no
/// session was ever begun on is a different thing and the text does not name
/// a condition for it, so this client uses `amqp:not-allowed` — "the peer
/// tried to use a capability or operation that is not allowed in the current
/// state" — and says in the `close` which channel it was.
pub const UNMAPPED_CHANNEL_CONDITION: &str = condition::NOT_ALLOWED;

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(channel: u16) -> Entry {
        let (events, rx) = mpsc::channel(4);
        let (outbound, _) = mpsc::channel(4);
        Entry {
            shared: Arc::new(Shared {
                outgoing_channel: channel,
                incoming_channel: Mutex::new(None),
                windows: Mutex::new(Windows::new(0, 8, 8)),
                state: Mutex::new(SessionState::Beginning),
                options: SessionOptions::default(),
            }),
            events,
            links: Links::default(),
            remote_handle_max: 255,
            pending: None,
            outbound,
            rx: Some(rx),
        }
    }

    #[test]
    fn channels_are_allocated_lowest_free() {
        let mut table = Table::default();
        assert_eq!(table.lowest_free(15), Some(0));
        table.insert(0, entry(0));
        assert_eq!(table.lowest_free(15), Some(1));
        table.insert(1, entry(1));
        table.insert(2, entry(2));
        assert_eq!(table.lowest_free(15), Some(3));
        // A gap is filled before the next number, which is what keeps the
        // table dense.
        table.remove(1);
        assert_eq!(table.lowest_free(15), Some(1));
    }

    #[test]
    fn the_bound_on_the_table_is_the_peers_channel_max() {
        let mut table = Table::default();
        // A peer advertising channel-max 1 accepts channels 0 and 1 and
        // nothing else, whatever we advertised.
        table.insert(0, entry(0));
        assert_eq!(table.lowest_free(1), Some(1));
        table.insert(1, entry(1));
        assert_eq!(
            table.lowest_free(1),
            None,
            "the peer's channel-max is what bounds what we may use"
        );
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn the_two_directions_are_mapped_independently() {
        let mut table = Table::default();
        table.insert(0, entry(0));
        // Our outgoing channel is 0; the peer answers on its own channel 4.
        assert!(!table.is_mapped(4));
        table.map_incoming(4, 0);
        assert!(table.is_mapped(4));
        assert_eq!(
            table.by_incoming(4).map(|e| e.shared.outgoing_channel),
            Some(0)
        );
        assert!(
            table.by_incoming(0).is_none(),
            "our own outgoing number is not an incoming one"
        );
        // Removing the session forgets both directions.
        table.remove(0);
        assert!(!table.is_mapped(4));
    }

    #[test]
    fn the_discarding_state_is_a_value_rather_than_a_comment() {
        let discarding = SessionState::Discarding(Condition::new(condition::DECODE_ERROR));
        assert!(discarding.is_discarding());
        assert!(!discarding.is_usable());
        assert!(!SessionState::Ending.is_discarding());
        assert!(SessionState::Begun.is_usable());
        assert!(!SessionState::Beginning.is_usable());
    }

    #[test]
    fn a_begin_carries_no_next_incoming_id_which_is_the_formulas_other_branch() {
        let begin = Begin::new(7, 400, 400);
        let remote = remote_flow_of_begin(&begin);
        assert_eq!(remote.next_incoming_id, None);
        assert_eq!(remote.next_outgoing_id, 7);
        assert_eq!(remote.incoming_window, 400);
    }

    #[test]
    fn our_begin_advertises_a_bounded_handle_max() {
        let options = SessionOptions::default();
        assert_eq!(options.handle_max, DEFAULT_HANDLE_MAX);
        assert_ne!(
            options.handle_max,
            weida_amqp_codec::performative::DEFAULT_HANDLE_MAX,
            "the specification's default is 2^32 links per session"
        );
        let windows = Windows::new(0, 400, 400);
        let bytes = begin_frame(3, &options, &windows).expect("encodes");
        let read = frame::decode(&bytes, u32::MAX).expect("a frame");
        assert_eq!(read.header.channel, 3);
        let (performative, _) =
            Performative::decode(read.body, weida_amqp_codec::Limits::DEFAULT).unwrap();
        match performative {
            Performative::Begin(begin) => {
                assert_eq!(begin.remote_channel, None, "the opening begin has none");
                assert_eq!(begin.handle_max, DEFAULT_HANDLE_MAX);
                assert_eq!(begin.incoming_window, DEFAULT_WINDOW);
            }
            other => panic!("expected begin, got {}", other.name()),
        }
    }
}
