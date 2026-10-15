//! The replying half of a tag-stack protocol, written once.
//!
//! REP and RESPONDENT are the same machine. Both receive a message whose
//! body begins with the 32-bit tag stack, both remember the stack and the
//! pipe it arrived on, both may send only the answer to what they
//! received, and both return the reply toward the originator of the most
//! recently received message (`docs/research/nanomsg-nng.md` §4). The only
//! differences are the protocol id in the header and what the terminal tag
//! is called — a request ID for REP, a survey ID for RESPONDENT — and
//! neither of those is behaviour.
//!
//! So the machine lives here and [`crate::reqrep::RepSocket`] and
//! [`crate::survey::RespondentSocket`] are two sockets over it. Writing it
//! twice would mean two places for the `NNG_ESTATE` rule to drift apart.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use weida_sp::{EndpointType, backtrace};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::PipeId;
use crate::socket::{SocketCore, within};

/// A context's identity within its socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CtxId(u64);

impl CtxId {
    /// A context id from a counter.
    pub(crate) const fn new(id: u64) -> CtxId {
        CtxId(id)
    }

    /// The number, as `nng_ctx_id()` reports it.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for CtxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "context {}", self.0)
    }
}

/// The tag stack a REQ or SURVEYOR sends directly to its peer: one tag, the
/// request or survey id with its terminal bit set [rfc-reqrep §5].
pub(crate) fn direct_stack(id: u32) -> [u8; backtrace::TAG_LEN] {
    ((id & backtrace::MAX_ID) | backtrace::TERMINAL).to_be_bytes()
}

/// What a context needs to answer the message it received.
#[derive(Clone, Debug)]
struct Answering {
    /// The tag stack of the message being answered, kept as the header of a
    /// message with no body: a reply is that header around the reply's
    /// body, and the stack is never decoded again.
    stack: Message,
    pipe: PipeId,
}

#[derive(Debug, Default)]
struct ReplierCtxState {
    /// The stack and the pipe of the message this context is answering.
    answering: Option<Answering>,
    receiving: bool,
}

/// What every context of one replying socket shares.
pub struct Replier {
    core: Arc<SocketCore>,
    contexts: Mutex<HashMap<CtxId, ReplierCtxState>>,
    next_ctx: AtomicU64,
    /// What a send without a receive is called, in this protocol's words.
    refusal: &'static str,
    /// Messages dropped with their pipe kept: a tag stack deeper than this
    /// socket's `NNG_OPT_MAXTTL`, or a payload over `NNG_OPT_RECVMAXSZ`
    /// once the stack is taken off.
    dropped: AtomicU64,
}

impl Replier {
    /// A replying socket speaking `protocol`.
    pub(crate) fn new(
        context: &Context,
        protocol: EndpointType,
        options: SocketOptions,
        refusal: &'static str,
    ) -> Result<Arc<Replier>> {
        let core = Arc::new(SocketCore::new(context, protocol, options)?);
        Ok(Arc::new(Replier {
            core,
            contexts: Mutex::new(HashMap::new()),
            next_ctx: AtomicU64::new(1),
            refusal,
            dropped: AtomicU64::new(0),
        }))
    }

    /// The socket underneath, for the endpoint surface.
    pub(crate) fn core(&self) -> &Arc<SocketCore> {
        &self.core
    }

    /// Messages dropped with their pipe kept: see [`Replier::dropped`].
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<CtxId, ReplierCtxState>> {
        self.contexts.lock().expect("replier contexts poisoned")
    }
}

impl std::fmt::Debug for Replier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replier")
            .field("contexts", &self.lock().len())
            .finish_non_exhaustive()
    }
}

/// One replying transaction: an `nng_ctx` on a REP or RESPONDENT socket.
///
/// Cloning is what `nng_ctx` being a value means; the context is retired
/// when the last clone goes.
#[derive(Clone, Debug)]
pub struct ReplierCtx {
    handle: Arc<ReplierHandle>,
}

#[derive(Debug)]
struct ReplierHandle {
    id: CtxId,
    shared: Arc<Replier>,
}

impl Drop for ReplierHandle {
    fn drop(&mut self) {
        self.shared.lock().remove(&self.id);
    }
}

impl ReplierCtx {
    pub(crate) fn open(shared: &Arc<Replier>) -> ReplierCtx {
        let id = CtxId(shared.next_ctx.fetch_add(1, Ordering::Relaxed));
        shared.lock().insert(id, ReplierCtxState::default());
        ReplierCtx {
            handle: Arc::new(ReplierHandle {
                id,
                shared: Arc::clone(shared),
            }),
        }
    }

    /// This context's id.
    pub fn id(&self) -> CtxId {
        self.handle.id
    }

    fn shared(&self) -> &Arc<Replier> {
        &self.handle.shared
    }

    /// Receives one message, remembering how to answer it.
    ///
    /// A second concurrent receive on one context is `NNG_ESTATE`: "a
    /// second simultaneous receive is likewise rejected" (§4).
    pub async fn recv(&self) -> Result<Message> {
        {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
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
        within(self.shared().core.exec(), limit, self.take()).await
    }

    async fn take(&self) -> Result<Message> {
        let core = &self.shared().core;
        let max_hops = core.options().max_ttl;
        loop {
            // Created before the look, so that a message arriving between
            // the look and the await ends the wait instead of being missed
            // by it. And the socket is checked on every turn: a close wakes
            // this wait, and what it must find then is `NNG_ECLOSED`.
            let arrived = core.message_signal();
            core.ensure_open()?;
            let Some((pipe, mut message)) = core.try_take_any() else {
                arrived.await;
                continue;
            };
            let shape = match backtrace::measure(message.body(), max_hops) {
                Ok(shape) => shape,
                Err(error) if !error.is_violation() => {
                    // A stack deeper than the local `MAXTTL` is a message
                    // to drop and not a connection to end: "we can
                    // legitimately receive messages with too many hops
                    // from devices", and the pipe is very likely one a
                    // device shares among all its clients. The peer is
                    // told nothing, and the count is the only trace.
                    self.shared().dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(_) => {
                    // A stack that ends inside a tag is malformed rather
                    // than merely deep: "if the reply is shorter than 32
                    // bits, it is malformed and the endpoint MUST ignore
                    // it" [rfc-reqrep §5], and NNG closes the pipe on a
                    // truncated tag rather than reading the next message
                    // out of a stream it can no longer trust (§8). So the
                    // pipe goes and the loop looks at the others.
                    core.engine().close_pipe(pipe);
                    continue;
                }
            };
            if message.split_header_off_body(shape.len).is_err() {
                core.engine().close_pipe(pipe);
                continue;
            }
            // `NNG_OPT_RECVMAXSZ` bounds what the application receives: the
            // transport admitted this message by its declared length with
            // the stack in it, and the payload that is left is judged here.
            // Dropped rather than ended, for the reason a deep stack is: a
            // device forwarding it hands the pipe it shares among its
            // clients to the backend, and one client's oversize message
            // must not be able to end that.
            if !core.payload_fits(pipe, message.len()) {
                self.shared().dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let stack = Message::from_parts(message.header(), Vec::new());
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            state.answering = Some(Answering { stack, pipe });
            return Ok(message);
        }
    }

    /// Answers what this context received.
    ///
    /// `NNG_ESTATE` when nothing was received: "a cooked REP may send only
    /// after receiving its corresponding request … violations return
    /// `NNG_ESTATE`" (§4), and a respondent needs a received survey for the
    /// same reason.
    ///
    /// An answer whose originator has gone is discarded rather than
    /// reported: SP has no way to tell anybody, and the originator's own
    /// timer — a resend for REQ, a survey deadline for SURVEYOR — is what
    /// recovers (§4, §6). So is one the originator's connection cannot take:
    /// the send never waits for the requester, because a requester that
    /// stopped reading must not be able to hold every other client of the
    /// socket behind it.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        let Some(answering) = self
            .shared()
            .lock()
            .get_mut(&self.id())
            .and_then(|state| state.answering.take())
        else {
            return Err(Error::ESTATE(self.shared().refusal.into()));
        };
        let body: Vec<u8> = body.into();
        let reply = Message::from_parts(answering.stack.header(), body);
        match self.shared().core.send_to(answering.pipe, reply).await {
            Ok(_) => Ok(()),
            Err(Error::ECLOSED(_)) => Ok(()),
            Err(other) => Err(other),
        }
    }
}

struct Receiving<'a> {
    shared: &'a Replier,
    id: CtxId,
}

impl Drop for Receiving<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.shared.lock().get_mut(&self.id) {
            state.receiving = false;
        }
    }
}
