//! REQ and REP: request-reply with contexts, request IDs and the resend
//! timer.
//!
//! "A cooked REQ sends one outstanding request per socket context and
//! normally spreads requests among peer REP sockets; the selected REP
//! receives then replies. REQ automatically resends until reply or timeout"
//! (`docs/research/nanomsg-nng.md` §4). Both halves are here: the spreading
//! is the round-robin over the pipes that can accept now, and the resending
//! is the socket's own clock.
//!
//! **A context is the unit, not the socket.** "`nng_ctx` shares its socket,
//! endpoints, and pipes but owns stateful protocol state such as a request
//! ID and retry timer, enabling independent concurrent transactions" (§2).
//! So [`ReqCtx`] holds the request ID, the outstanding body and the
//! deadline, and a socket is the one context it opened for itself — which
//! is exactly how `nng_send()` on a REQ socket relates to `nng_ctx_send()`.
//!
//! **Retry is not free, and a test says so.** "REQ retransmission creates
//! duplicate requests, including when a reply is lost; the protocol has no
//! deduplication key exposed to the REP beyond its routing header" (§7).
//!
//! **All three triggers.** "An outstanding request is resent after its
//! resend timer elapses, when the original peer disconnects, or when a peer
//! becomes available while it is waiting" (§4). One task per socket watches
//! all three, so a context holding an unanswered request is retried whether
//! or not anybody is inside `recv` at that moment.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use weida_sp::{EndpointType, backtrace};

use crate::context::Context;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::PipeId;
use crate::replier::{CtxId, Replier, ReplierCtx, direct_stack};
use crate::socket::{SocketCore, socket_endpoints, within};

/// How often the socket's resend clock looks at its contexts.
///
/// NNG's own granularity: "the resend clock's default check granularity is
/// one second and is shared by the socket's contexts" (§4). The clock also
/// wakes when a deadline is armed or the pipe set changes, so this is a
/// ceiling on lateness rather than the mechanism.
pub const RESEND_GRANULARITY: Duration = Duration::from_secs(1);

/// Request IDs are 31 bits, "seeded at random and incremented per requester
/// context" [rfc-reqrep §5].
///
/// One stream for the whole process rather than one per context: it gives
/// the same uniqueness with no chance of two contexts of one socket
/// colliding, and the seed — taken once from the clock — is what keeps a
/// restarted process from reusing IDs its previous incarnation left in
/// flight, which is why the RFC asks for randomness at all.
static REQUEST_IDS: LazyLock<AtomicU32> = LazyLock::new(|| {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos() ^ (since.as_secs() as u32))
        .unwrap_or(1);
    AtomicU32::new(seed & backtrace::MAX_ID)
});

fn next_request_id() -> u32 {
    REQUEST_IDS.fetch_add(1, Ordering::Relaxed) & backtrace::MAX_ID
}

// --------------------------------------------------------------------------
// REQ
// --------------------------------------------------------------------------

/// One request a context is waiting for a reply to.
#[derive(Clone, Debug)]
struct Outstanding {
    id: u32,
    payload: Bytes,
    /// The pipe it went to last, so that the pipe's disappearance is a
    /// resend trigger (§4).
    pipe: Option<PipeId>,
    /// When the resend clock should send it again.
    deadline: Instant,
}

#[derive(Debug, Default)]
struct ReqCtxState {
    outstanding: Option<Outstanding>,
    /// A reply another context's receive pumped in on our behalf.
    reply: Option<Message>,
    /// "Only one pending receive per context" (§4).
    receiving: bool,
}

struct ReqShared {
    core: Arc<SocketCore>,
    resend_time: Duration,
    contexts: Mutex<HashMap<CtxId, ReqCtxState>>,
    next_ctx: AtomicU64,
    /// Woken when a reply lands in somebody's slot.
    delivered: Notify,
    /// Woken when a deadline is armed, so the clock re-reads them.
    rearmed: Arc<Notify>,
    resend: Mutex<Option<JoinHandle<()>>>,
}

impl ReqShared {
    fn lock(&self) -> MutexGuard<'_, HashMap<CtxId, ReqCtxState>> {
        self.contexts.lock().expect("req contexts poisoned")
    }
}

impl std::fmt::Debug for ReqShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqShared")
            .field("resend_time", &self.resend_time)
            .field("contexts", &self.lock().len())
            .finish_non_exhaustive()
    }
}

impl Drop for ReqShared {
    fn drop(&mut self) {
        if let Some(task) = self.resend.lock().expect("resend poisoned").take() {
            task.abort();
        }
    }
}

/// A REQ socket: one outstanding request per context, resent until
/// answered.
///
/// Cloning shares everything, because `nng_socket` is a value an
/// application copies (§2).
#[derive(Clone, Debug)]
pub struct ReqSocket {
    core: Arc<SocketCore>,
    shared: Arc<ReqShared>,
    /// The context a bare `send`/`recv` uses, which is what NNG gives every
    /// socket implicitly.
    implicit: ReqCtx,
}

impl ReqSocket {
    /// A REQ socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<ReqSocket> {
        ReqSocket::with_options(context, SocketOptions::default())
    }

    /// A REQ socket with `options`.
    ///
    /// `NNG_OPT_SENDBUF` and `NNG_OPT_RECVBUF` are refused here by name:
    /// REQ "permits only one outstanding transaction per context", so there
    /// is nothing for a depth to bound (§5).
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<ReqSocket> {
        let resend_time = options.resend_time;
        let core = Arc::new(SocketCore::new(context, EndpointType::Req, options)?);
        let shared = Arc::new(ReqShared {
            core: Arc::clone(&core),
            resend_time,
            contexts: Mutex::new(HashMap::new()),
            next_ctx: AtomicU64::new(1),
            delivered: Notify::new(),
            rearmed: Arc::new(Notify::new()),
            resend: Mutex::new(None),
        });
        // The clock holds the socket's state weakly: a clock that owned it
        // would keep a socket nobody can reach alive for as long as it ran.
        let task = core.exec().spawn(resend_loop(
            Arc::downgrade(&shared),
            core.engine().clone(),
            Arc::clone(&shared.rearmed),
        ));
        *shared.resend.lock().expect("resend poisoned") = Some(task);
        let implicit = ReqCtx::open(&shared);
        Ok(ReqSocket {
            core,
            shared,
            implicit,
        })
    }

    /// `nng_ctx_open()`: a context with its own request ID, its own resend
    /// deadline and its own outstanding request, over this socket's pipes.
    pub fn context(&self) -> ReqCtx {
        ReqCtx::open(&self.shared)
    }

    /// The context a bare [`ReqSocket::send`] and [`ReqSocket::recv`] use.
    pub fn implicit_context(&self) -> ReqCtx {
        self.implicit.clone()
    }

    /// Sends a request on this socket's own context.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.implicit.send(body).await
    }

    /// Receives the reply to this socket's own outstanding request.
    pub async fn recv(&self) -> Result<Message> {
        self.implicit.recv().await
    }

    /// How long a context waits for its reply before sending the request
    /// again: `NNG_OPT_REQ_RESENDTIME`.
    pub fn resend_time(&self) -> Duration {
        self.shared.resend_time
    }
}

socket_endpoints!(ReqSocket);

/// One REQ transaction: an `nng_ctx` on a REQ socket.
///
/// Cloning is what `nng_ctx` being a value means; the context is retired
/// when the last clone goes, and an outstanding request is forgotten with
/// it — its reply, if it ever arrives, is discarded, the same thing a newer
/// request does to an older one (§4).
#[derive(Clone, Debug)]
pub struct ReqCtx {
    handle: Arc<ReqHandle>,
}

#[derive(Debug)]
struct ReqHandle {
    id: CtxId,
    shared: Arc<ReqShared>,
}

impl Drop for ReqHandle {
    fn drop(&mut self) {
        self.shared.lock().remove(&self.id);
    }
}

impl ReqCtx {
    fn open(shared: &Arc<ReqShared>) -> ReqCtx {
        let id = CtxId::new(shared.next_ctx.fetch_add(1, Ordering::Relaxed));
        shared.lock().insert(id, ReqCtxState::default());
        ReqCtx {
            handle: Arc::new(ReqHandle {
                id,
                shared: Arc::clone(shared),
            }),
        }
    }

    /// This context's id, as `nng_ctx_id()` reports it.
    pub fn id(&self) -> CtxId {
        self.handle.id
    }

    fn shared(&self) -> &Arc<ReqShared> {
        &self.handle.shared
    }

    /// Sends `body` as a request, to one of the available repliers.
    ///
    /// The 32-bit request ID goes out with its terminal bit set, which is
    /// what tells a replier where the tag stack ends [rfc-reqrep §5] and
    /// what the reply is matched by.
    ///
    /// A second send cancels interest in the first reply: "sending a newer
    /// request cancels the requester's interest in its earlier reply and
    /// causes a late old reply to be discarded. It does not withdraw the
    /// earlier request from a replier" (§4).
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        let payload = Bytes::from(body.into());
        let id = next_request_id();
        let pipe = self
            .shared()
            .core
            .send_round_robin(request(id, payload.clone()))
            .await?;
        {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            state.outstanding = Some(Outstanding {
                id,
                payload,
                pipe: Some(pipe),
                deadline: Instant::now() + self.shared().resend_time,
            });
            state.reply = None;
        }
        self.shared().rearmed.notify_waiters();
        Ok(())
    }

    /// Receives the reply to this context's outstanding request.
    ///
    /// `NNG_ESTATE` with no outstanding request, and `NNG_ESTATE` for a
    /// second concurrent receive on one context: "REQ receive without an
    /// active request, or a second concurrent receive, returns
    /// `NNG_ESTATE`" (§4).
    pub async fn recv(&self) -> Result<Message> {
        {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            if state.outstanding.is_none() {
                return Err(Error::ESTATE(
                    "this context has no outstanding request to receive a reply to".into(),
                ));
            }
            if state.receiving {
                return Err(Error::ESTATE(
                    "this context already has a receive in progress; one per context".into(),
                ));
            }
            state.receiving = true;
        }
        let _guard = Receiving {
            shared: self.shared(),
            id: self.id(),
        };
        let limit = self.shared().core.options().recv_timeout;
        within(self.shared().core.exec(), limit, self.wait_for_reply()).await
    }

    async fn wait_for_reply(&self) -> Result<Message> {
        let shared = self.shared();
        loop {
            {
                let mut contexts = shared.lock();
                let state = contexts.entry(self.id()).or_default();
                if let Some(reply) = state.reply.take() {
                    state.outstanding = None;
                    return Ok(reply);
                }
            }
            // Nobody pumps on our behalf unless we pump ourselves: whichever
            // context is inside a receive drains the pipes and files each
            // reply under the request ID it carries.
            //
            // Both waits are created before the look. A `Notified` sees
            // every wake-up since it was created, so a sibling filing our
            // reply, or a reply landing on a pipe, between the drain below
            // and the park ends the wait instead of being missed by it —
            // with the reply already in our slot this wait would otherwise
            // run to the resend or the receive timeout.
            let delivered = shared.delivered.notified();
            let arrived = shared.core.message_signal();
            // And the socket is checked on every turn: a close wakes both
            // waits, and what a receive must find then is `NNG_ECLOSED`.
            shared.core.ensure_open()?;
            if let Some((pipe, message)) = shared.core.try_take_any() {
                route(shared, pipe, message);
                continue;
            }
            tokio::select! {
                () = delivered => {}
                () = arrived => {}
            }
        }
    }
}

/// Releases the one-receive-per-context flag however the receive ends,
/// including a cancelled future and an expired `NNG_OPT_RECVTIMEO`.
struct Receiving<'a> {
    shared: &'a ReqShared,
    id: CtxId,
}

impl Drop for Receiving<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.shared.lock().get_mut(&self.id) {
            state.receiving = false;
        }
    }
}

/// Files an arrived reply under the context whose request ID it carries,
/// and discards it when no context is waiting for that ID — which is what a
/// cancelled request's late reply is (§4).
fn route(shared: &Arc<ReqShared>, pipe: PipeId, mut message: Message) {
    let max_hops = shared.core.options().max_ttl;
    let Ok(shape) = backtrace::measure(message.body(), max_hops) else {
        // "If the reply is shorter than 32 bits, it is malformed and the
        // endpoint MUST ignore it" [rfc-reqrep §5].
        return;
    };
    // A reply whose payload is over the limit of the endpoint it arrived
    // on is dropped, as an oversize request is at a replier.
    if !shared.core.payload_fits(pipe, message.len() - shape.len) {
        return;
    }
    let mut contexts = shared.lock();
    for state in contexts.values_mut() {
        if state
            .outstanding
            .as_ref()
            .is_some_and(|outstanding| outstanding.id == shape.id)
        {
            // The stack is split off only now that somebody is waiting for
            // the reply: one nobody is waiting for is dropped untouched.
            if message.split_header_off_body(shape.len).is_err() {
                return;
            }
            state.reply = Some(message);
            drop(contexts);
            shared.delivered.notify_waiters();
            return;
        }
    }
}

/// The socket's resend clock: the three triggers, in one place.
///
/// It sleeps until the nearest deadline, or [`RESEND_GRANULARITY`] —
/// NNG's own one second — whichever is sooner, and is woken early by a
/// context arming a deadline or by the pipe set changing. A resend time
/// shorter than the granularity is therefore honoured rather than rounded
/// up to it.
async fn resend_loop(shared: Weak<ReqShared>, engine: Engine, rearmed: Arc<Notify>) {
    loop {
        // Created before the clock reads its deadlines, so a deadline armed
        // or a pipe that arrives while it reads ends the wait.
        let rearm = rearmed.notified();
        let changed = engine.changed();
        let wake = match shared.upgrade() {
            Some(shared) => next_wake(&shared),
            None => return,
        };
        let tick = engine.exec().sleep(wake);
        tokio::select! {
            () = rearm => {}
            () = changed => {}
            () = tick => {}
        }
        if engine.is_closed() {
            return;
        }
        let Some(shared) = shared.upgrade() else {
            return;
        };
        resend_due(&shared);
    }
}

/// How long the clock may sleep: until the nearest armed deadline, capped
/// at NNG's granularity so that a request with no deadline in sight still
/// has its peer-gone trigger looked at once a second.
fn next_wake(shared: &Arc<ReqShared>) -> Duration {
    let now = Instant::now();
    shared
        .lock()
        .values()
        .filter_map(|state| state.outstanding.as_ref())
        .map(|outstanding| outstanding.deadline.saturating_duration_since(now))
        .min()
        .unwrap_or(RESEND_GRANULARITY)
        .min(RESEND_GRANULARITY)
}

/// A request on the wire: its id with the terminal bit set, then the
/// payload, which is shared with the context that may have to send it again.
fn request(id: u32, payload: Bytes) -> Message {
    Message::from_parts(direct_stack(id), payload)
}

fn resend_due(shared: &Arc<ReqShared>) {
    let now = Instant::now();
    let pipes = shared.core.pipes();
    let due: Vec<(CtxId, u32, Bytes)> = {
        let contexts = shared.lock();
        contexts
            .iter()
            .filter_map(|(id, state)| {
                let outstanding = state.outstanding.as_ref()?;
                // One: the timer elapsed. Two: the peer it went to is gone.
                // Three: it has no peer at all and one is available now
                // (§4).
                let timer = outstanding.deadline <= now;
                let peer_gone = outstanding
                    .pipe
                    .is_none_or(|pipe| pipes.get(pipe).is_none());
                (timer || (peer_gone && !pipes.is_empty()))
                    .then(|| (*id, outstanding.id, outstanding.payload.clone()))
            })
            .collect()
    };
    for (ctx, id, payload) in due {
        // Round-robin again, so a resend after a disconnect reaches a
        // different replier — which is what makes the third trigger useful.
        let sent = shared.core.offer_round_robin(request(id, payload));
        let mut contexts = shared.lock();
        let Some(state) = contexts.get_mut(&ctx) else {
            continue;
        };
        let Some(outstanding) = state.outstanding.as_mut() else {
            continue;
        };
        match sent {
            Ok(pipe) => {
                outstanding.pipe = Some(pipe);
                outstanding.deadline = Instant::now() + shared.resend_time;
            }
            // Nowhere to send it right now. The request stays outstanding
            // with no pipe, which is trigger three the next time one
            // arrives; the deadline moves on so the clock waits rather
            // than spinning on a deadline it cannot satisfy.
            Err(_) => {
                outstanding.pipe = None;
                outstanding.deadline = Instant::now() + shared.resend_time;
            }
        }
    }
}

// --------------------------------------------------------------------------
// REP
// --------------------------------------------------------------------------

/// A REP socket: receive then reply, in that order, once per context.
///
/// The machine underneath is [`crate::replier`]'s, which RESPONDENT uses
/// too: the two protocols differ in the id in their header and in what the
/// terminal tag is called, and in nothing a replier does.
#[derive(Clone, Debug)]
pub struct RepSocket {
    core: Arc<SocketCore>,
    shared: Arc<Replier>,
    implicit: RepCtx,
}

/// One REP transaction: an `nng_ctx` on a REP socket.
pub type RepCtx = ReplierCtx;

impl RepSocket {
    /// A REP socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<RepSocket> {
        RepSocket::with_options(context, SocketOptions::default())
    }

    /// A REP socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<RepSocket> {
        let shared = Replier::new(
            context,
            EndpointType::Rep,
            options,
            "a REP context may send only the reply to a request it received",
        )?;
        let core = Arc::clone(shared.core());
        let implicit = ReplierCtx::open(&shared);
        Ok(RepSocket {
            core,
            shared,
            implicit,
        })
    }

    /// `nng_ctx_open()`: a context that processes one request
    /// independently of the others, which is how "several requests to be
    /// processed in parallel over one socket" works (§4).
    pub fn context(&self) -> RepCtx {
        ReplierCtx::open(&self.shared)
    }

    /// The context a bare [`RepSocket::recv`] and [`RepSocket::send`] use.
    pub fn implicit_context(&self) -> RepCtx {
        self.implicit.clone()
    }

    /// Requests dropped with their pipe kept: a tag stack deeper than this
    /// socket's `NNG_OPT_MAXTTL`, or a payload over `NNG_OPT_RECVMAXSZ`
    /// once the stack is taken off.
    ///
    /// Both are messages to drop and not connections to end — devices
    /// legitimately forward deep stacks, and the pipe a device shares among
    /// all its clients is not one client's to close — and the sender is told
    /// nothing, so this count is the only trace.
    pub fn dropped(&self) -> u64 {
        self.shared.dropped()
    }

    /// Receives a request on this socket's own context.
    pub async fn recv(&self) -> Result<Message> {
        self.implicit.recv().await
    }

    /// Replies to the request this socket's own context received.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.implicit.send(body).await
    }
}

socket_endpoints!(RepSocket);

#[cfg(test)]
mod tests {
    use super::*;

    use crate::context::ContextConfig;
    use weida_sp::Backtrace;

    /// A reply as it comes off a pipe: the tag stack is in the body, where
    /// [`route`] looks for it, and not yet split into a header.
    fn arrived(id: u32, payload: &[u8]) -> Message {
        let mut body = Backtrace::direct(id).encode();
        body.extend_from_slice(payload);
        Message::from_body(body)
    }

    /// Arms an outstanding request the way [`ReqCtx::send`] does once its
    /// round robin has picked a pipe.
    ///
    /// A REQ send needs a peer to pick, and a real peer would answer over a
    /// pipe — whose arrival wakes every parked context by itself and so
    /// hides the wake this test is about.
    fn arm(ctx: &ReqCtx, id: u32) {
        let mut contexts = ctx.shared().lock();
        let state = contexts.entry(ctx.id()).or_default();
        state.outstanding = Some(Outstanding {
            id,
            payload: Bytes::new(),
            pipe: None,
            deadline: Instant::now() + Duration::from_secs(60),
        });
    }

    /// Claim: a reply filed by a **sibling** context wakes the context it
    /// belongs to. Whichever context is inside a receive drains the pipes
    /// for all of them and [`route`] files each reply under the request ID
    /// it carries, so the context that owns a reply is usually not the one
    /// that took it off the pipe; without the wake its receive runs to
    /// `NNG_OPT_RECVTIMEO` with the reply already in its slot. Calling
    /// [`route`] directly is exactly what a sibling's drain does, with no
    /// pipe traffic that could wake the parked receive by accident.
    #[tokio::test]
    async fn a_reply_filed_by_a_sibling_wakes_the_context_it_belongs_to() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let options = SocketOptions {
            recv_timeout: None,
            resend_time: Duration::from_secs(60),
            ..SocketOptions::default()
        };
        let socket = ReqSocket::with_options(&ctx, options).expect("req socket");
        let parked = socket.context();
        let sibling = socket.context();
        arm(&parked, 7);
        arm(&sibling, 9);

        let waiting = parked.recv();
        let mut waiting = std::pin::pin!(waiting);
        assert!(
            futures::poll!(waiting.as_mut()).is_pending(),
            "nobody has replied yet"
        );

        route(&socket.shared, PipeId::new(1), arrived(7, b"mine"));

        let reply = weida_testkit::within(
            "the reply the sibling filed",
            Duration::from_secs(10),
            waiting,
        )
        .await
        .expect("the reply the sibling filed");
        assert_eq!(reply.body(), b"mine");
    }
}

#[cfg(test)]
mod drop_tests {
    use super::*;

    use crate::context::ContextConfig;

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: a reply sent just before its socket goes out of scope still
        /// reaches the requester. `send` returns once the reply is queued, and a
        /// close that discarded the queue would lose it in the moment before
        /// the connection's writer takes it; closing on the last drop must let
        /// what is queued be written first.
        #[tokio::test]
        async fn a_reply_sent_just_before_the_replier_is_dropped_is_delivered() {
            let ctx = Context::new(ContextConfig::default()).expect("context");
            let options = SocketOptions {
                recv_timeout: None,
                send_timeout: None,
                ..SocketOptions::default()
            };
            for round in 0..30 {
                let rep = RepSocket::with_options(&ctx, options.clone()).expect("rep");
                let url = rep
                    .listen("tcp://127.0.0.1:0")
                    .await
                    .expect("listen")
                    .url()
                    .to_string();
                let serving = tokio::spawn(async move {
                    let request = rep.recv().await.expect("a request");
                    rep.send(request.body().to_vec()).await.expect("the reply");
                    // `rep` goes out of scope here, with the reply queued.
                });
                let req = ReqSocket::with_options(&ctx, options.clone()).expect("req");
                req.dial(&url).await.expect("dial");
                let body = format!("round {round}").into_bytes();
                req.send(body.clone()).await.expect("send");
                let reply = req
                    .recv()
                    .await
                    .unwrap_or_else(|error| panic!("round {round}: the reply was lost: {error}"));
                assert_eq!(reply.body(), &body[..]);
                serving.await.expect("task");
            }
        }
        /// Claim: the same holds when the replier is the side that **dialled**.
        /// A dialled connection's session runs inside the task of its dialer, so
        /// a close that stopped the dialers first would drop the writer before it
        /// had taken the reply; a socket dropped with a message queued stops
        /// dialling and still writes what it holds.
        #[tokio::test]
        async fn a_reply_sent_just_before_a_dialling_replier_is_dropped_is_delivered() {
            let ctx = Context::new(ContextConfig::default()).expect("context");
            let options = SocketOptions {
                recv_timeout: None,
                send_timeout: None,
                ..SocketOptions::default()
            };
            for round in 0..30 {
                let req = ReqSocket::with_options(&ctx, options.clone()).expect("req");
                let url = req
                    .listen("tcp://127.0.0.1:0")
                    .await
                    .expect("listen")
                    .url()
                    .to_string();
                let rep = RepSocket::with_options(&ctx, options.clone()).expect("rep");
                rep.dial(&url).await.expect("dial");
                let serving = tokio::spawn(async move {
                    let request = rep.recv().await.expect("a request");
                    rep.send(request.body().to_vec()).await.expect("the reply");
                    // `rep` goes out of scope here, with the reply queued behind a
                    // connection it dialled.
                });
                let body = format!("round {round}").into_bytes();
                req.send(body.clone()).await.expect("send");
                let reply = req
                    .recv()
                    .await
                    .unwrap_or_else(|error| panic!("round {round}: the reply was lost: {error}"));
                assert_eq!(reply.body(), &body[..]);
                serving.await.expect("task");
            }
        }
    }
}
