//! SURVEYOR and RESPONDENT: a broadcast with a deadline.
//!
//! "A surveyor broadcasts a survey to every respondent, then accepts at
//! most one response per respondent; a respondent may decline by not
//! replying" (`docs/research/nanomsg-nng.md` §4). The header is the same
//! 32-bit tag stack REQ/REP uses, with a survey ID in the terminal
//! position (§3), so the answering half is literally
//! [`crate::replier`]'s — RESPONDENT and REP differ in their protocol id
//! and in nothing a replier does.
//!
//! **The deadline starts at send, not at arrival.** "Survey time starts
//! when the survey is sent, not when a particular respondent receives it"
//! (§4). So a slow network spends the same budget a slow respondent does,
//! and there is no way to tell the two apart.
//!
//! **Silence is not failure, and it is not distinguishable from
//! slowness.** "A response after expiry is discarded, which makes
//! nonresponse indistinguishable from a slow, unreachable, or deliberately
//! silent respondent" (§4). A test asserts that equality rather than
//! treating an absent answer as an error: what a surveyor learns at its
//! deadline is how many answers it got, and nothing whatever about who did
//! not answer or why.
//!
//! **At most one response per respondent.** "A surveyor normally expects
//! at most one response from each respondent, but the manual warns that
//! some topologies can duplicate responses. The pattern therefore supports
//! collection within a deadline, not a quorum-certified membership result"
//! (§4). This library collects at most one per pipe and discards a second,
//! which is the "normally" case; a duplicate that arrived through a
//! different pipe is a different pipe and is collected, because from here
//! it is indistinguishable from a second respondent.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use weida_sp::{EndpointType, backtrace};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::options::SocketOptions;
use crate::pipe::{
    DEFAULT_QUEUE_BYTES, DEFAULT_RECV_DEPTH, FullAction, PipeId, QueueConfig, crosses,
};
use crate::replier::{CtxId, Replier, ReplierCtx, direct_stack};
use crate::socket::{SocketCore, socket_endpoints, within};

/// Survey IDs, like request IDs, are 31 bits with the terminal bit set on
/// the wire (§3). One clock-seeded stream for the process, for the reason
/// [`crate::reqrep`] gives: a restarted process must not reuse the IDs its
/// previous incarnation left in flight.
static SURVEY_IDS: LazyLock<AtomicU32> = LazyLock::new(|| {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos().rotate_left(7) ^ (since.as_secs() as u32))
        .unwrap_or(1);
    AtomicU32::new(seed & backtrace::MAX_ID)
});

fn next_survey_id() -> u32 {
    SURVEY_IDS.fetch_add(1, Ordering::Relaxed) & backtrace::MAX_ID
}

/// One survey a context is collecting answers to.
#[derive(Debug)]
struct Survey {
    id: u32,
    /// When the collection stops. Set when the survey was **sent** (§4).
    deadline: Instant,
    /// Answers collected so far, in arrival order, and the octets they
    /// occupy: both are bounded by [`SurveyorShared::answer_bound`], so a
    /// respondent that reconnects under a fresh pipe id to answer again
    /// cannot grow what a survey holds without limit.
    answers: VecDeque<Message>,
    answers_bytes: u64,
    /// Pipes that have already answered: "at most one response per
    /// respondent" (§4). At most as many entries as there are answers.
    answered: HashSet<PipeId>,
}

#[derive(Debug, Default)]
struct SurveyorCtxState {
    survey: Option<Survey>,
    receiving: bool,
}

struct SurveyorShared {
    core: Arc<SocketCore>,
    survey_time: Duration,
    /// What one survey may hold of answers nobody has taken yet: the
    /// receive depth NNG gives every context, and the octet ceiling of a
    /// pipe's queue, with the same rule that an empty buffer accepts one
    /// answer of any size. An answer past either is discarded and counted
    /// with the late ones, which is where SP puts everything a surveyor
    /// declines.
    answer_bound: QueueConfig,
    contexts: Mutex<HashMap<CtxId, SurveyorCtxState>>,
    next_ctx: AtomicU64,
    /// Woken when an answer lands in some context's slot.
    ///
    /// An answer is filed by whichever context happens to be inside a
    /// receive, and [`route`] walks every context to find the one whose
    /// survey it answers — so the slot's owner is usually not the drainer.
    /// Without this signal that owner stays parked on its pipes until
    /// unrelated traffic arrives or its deadline passes, and `recv` reports
    /// `NNG_ETIMEDOUT` for an answer already in memory. [`crate::reqrep`]
    /// carries the same signal for the same reason.
    delivered: Notify,
    late: AtomicU64,
}

impl SurveyorShared {
    fn lock(&self) -> MutexGuard<'_, HashMap<CtxId, SurveyorCtxState>> {
        self.contexts.lock().expect("survey contexts poisoned")
    }
}

impl std::fmt::Debug for SurveyorShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurveyorShared")
            .field("survey_time", &self.survey_time)
            .field("contexts", &self.lock().len())
            .finish_non_exhaustive()
    }
}

/// A SURVEYOR socket: broadcast, then collect until the deadline.
#[derive(Clone, Debug)]
pub struct SurveyorSocket {
    core: Arc<SocketCore>,
    shared: Arc<SurveyorShared>,
    implicit: SurveyorCtx,
}

impl SurveyorSocket {
    /// A SURVEYOR socket on `context`, with NNG's defaults — including
    /// `NNG_OPT_SURVEYOR_SURVEYTIME`.
    pub fn new(context: &Context) -> Result<SurveyorSocket> {
        SurveyorSocket::with_options(context, SocketOptions::default())
    }

    /// A SURVEYOR socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<SurveyorSocket> {
        let survey_time = options.survey_time;
        let answer_bound = QueueConfig {
            depth: options.recv_depth.unwrap_or(DEFAULT_RECV_DEPTH).max(1),
            max_bytes: DEFAULT_QUEUE_BYTES,
            full: FullAction::Drop,
        };
        let core = Arc::new(SocketCore::new(context, EndpointType::Surveyor, options)?);
        let shared = Arc::new(SurveyorShared {
            core: Arc::clone(&core),
            survey_time,
            answer_bound,
            contexts: Mutex::new(HashMap::new()),
            next_ctx: AtomicU64::new(1),
            delivered: Notify::new(),
            late: AtomicU64::new(0),
        });
        let implicit = SurveyorCtx::open(&shared);
        Ok(SurveyorSocket {
            core,
            shared,
            implicit,
        })
    }

    /// `nng_ctx_open()`: a context with its own survey and its own
    /// deadline. "SURVEYOR contexts can overlap surveys with individual
    /// deadlines" (§4).
    pub fn context(&self) -> SurveyorCtx {
        SurveyorCtx::open(&self.shared)
    }

    /// The context a bare [`SurveyorSocket::send`] and
    /// [`SurveyorSocket::recv`] use.
    pub fn implicit_context(&self) -> SurveyorCtx {
        self.implicit.clone()
    }

    /// Broadcasts a survey on this socket's own context.
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.implicit.send(body).await
    }

    /// Collects the next response to this socket's own survey.
    pub async fn recv(&self) -> Result<Message> {
        self.implicit.recv().await
    }

    /// `NNG_OPT_SURVEYOR_SURVEYTIME`.
    pub fn survey_time(&self) -> Duration {
        self.shared.survey_time
    }

    /// Responses thrown away because their survey had expired or was
    /// replaced, because the respondent had already answered, or because
    /// the survey already held as many answers as it may — the receive
    /// depth, and the octet ceiling of a pipe's queue — that nobody had
    /// taken yet.
    ///
    /// The only trace a discarded late response leaves: nothing is sent
    /// back and the respondent never learns it was too slow (§4, §6).
    ///
    /// A response is routed — and so judged late — when a receive drains
    /// the pipes, and nothing drains them between surveys. So this reader
    /// drains them itself before answering: a late answer that arrived
    /// after the last receive gave up is counted here, not left in a pipe
    /// for the next survey's receive to stumble over.
    pub fn discarded_late(&self) -> u64 {
        while let Some((pipe, message)) = self.shared.core.try_take_any() {
            route(&self.shared, pipe, message);
        }
        self.shared.late.load(Ordering::Relaxed)
    }
}

socket_endpoints!(SurveyorSocket);

/// One survey: an `nng_ctx` on a SURVEYOR socket.
#[derive(Clone, Debug)]
pub struct SurveyorCtx {
    handle: Arc<SurveyorHandle>,
}

#[derive(Debug)]
struct SurveyorHandle {
    id: CtxId,
    shared: Arc<SurveyorShared>,
}

impl Drop for SurveyorHandle {
    fn drop(&mut self) {
        self.shared.lock().remove(&self.id);
    }
}

impl SurveyorCtx {
    fn open(shared: &Arc<SurveyorShared>) -> SurveyorCtx {
        let id = CtxId::new(shared.next_ctx.fetch_add(1, Ordering::Relaxed));
        shared.lock().insert(id, SurveyorCtxState::default());
        SurveyorCtx {
            handle: Arc::new(SurveyorHandle {
                id,
                shared: Arc::clone(shared),
            }),
        }
    }

    /// This context's id.
    pub fn id(&self) -> CtxId {
        self.handle.id
    }

    fn shared(&self) -> &Arc<SurveyorShared> {
        &self.handle.shared
    }

    /// Broadcasts `body` as a survey to every connected respondent and
    /// starts the deadline.
    ///
    /// Best effort, like every SP broadcast: a respondent whose queue
    /// cannot take the survey loses it and is then indistinguishable from
    /// one that chose not to answer (§4, §5). Starting another survey
    /// cancels this context's previous one, whose late answers are then
    /// discarded (§4).
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        let id = next_survey_id();
        let body: Vec<u8> = body.into();
        let survey = Message::from_parts(direct_stack(id), body);
        // The deadline starts at the send, before the copies go out, which
        // is what "survey time starts when the survey is sent" means (§4).
        let deadline = Instant::now() + self.shared().survey_time;
        {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            state.survey = Some(Survey {
                id,
                deadline,
                answers: VecDeque::new(),
                answers_bytes: 0,
                answered: HashSet::new(),
            });
        }
        self.shared().core.send_to_all_except(&survey, None);
        Ok(())
    }

    /// Collects the next response to this context's survey.
    ///
    /// `NNG_ESTATE` with no active survey, `NNG_ETIMEDOUT` once the
    /// deadline passes with nothing more collected — and after that
    /// `NNG_ESTATE` again, because the survey is over: "a blocked receive
    /// expires as `NNG_ETIMEDOUT`, then a receive without an outstanding
    /// survey is `NNG_ESTATE`" (§8).
    pub async fn recv(&self) -> Result<Message> {
        let deadline = {
            let mut contexts = self.shared().lock();
            let state = contexts.entry(self.id()).or_default();
            let Some(survey) = state.survey.as_ref() else {
                return Err(Error::ESTATE(
                    "this context has no active survey to collect responses to".into(),
                ));
            };
            if state.receiving {
                return Err(Error::ESTATE(
                    "this context already has a receive in progress; one per context".into(),
                ));
            }
            state.receiving = true;
            survey.deadline
        };
        let _guard = Collecting {
            shared: self.shared(),
            id: self.id(),
        };
        let budget = deadline.saturating_duration_since(Instant::now());
        let outcome = within(self.shared().core.exec(), Some(budget), self.collect()).await;
        if matches!(outcome, Err(Error::ETIMEDOUT(_))) {
            // The survey is over. The next receive says so in the
            // protocol's own words rather than timing out again (§8).
            if let Some(state) = self.shared().lock().get_mut(&self.id()) {
                state.survey = None;
            }
            return Err(Error::ETIMEDOUT(
                "the survey deadline passed; a response after it is discarded".into(),
            ));
        }
        outcome
    }

    async fn collect(&self) -> Result<Message> {
        let shared = self.shared();
        loop {
            {
                let mut contexts = shared.lock();
                let state = contexts.entry(self.id()).or_default();
                let Some(survey) = state.survey.as_mut() else {
                    return Err(Error::ESTATE(
                        "this context's survey was replaced or closed".into(),
                    ));
                };
                if let Some(answer) = survey.answers.pop_front() {
                    survey.answers_bytes = survey
                        .answers_bytes
                        .saturating_sub(answer.wire_len() as u64);
                    return Ok(answer);
                }
            }
            // Whichever context is inside a receive drains the pipes for
            // all of them and files each response under the survey ID it
            // carries, exactly as a REQ context does with replies. So this
            // wait has two wake-ups to race: a message on a pipe, and a
            // sibling filing an answer into *this* context's slot.
            //
            // Both are created before the look: a filing, or an arrival,
            // between the drain below and the select ends the wait instead
            // of being missed by it, which would run this to the deadline.
            let delivered = shared.delivered.notified();
            let arrived = shared.core.message_signal();
            // A close wakes both, and what a receive must find then is
            // `NNG_ECLOSED` and not the deadline.
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

struct Collecting<'a> {
    shared: &'a SurveyorShared,
    id: CtxId,
}

impl Drop for Collecting<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.shared.lock().get_mut(&self.id) {
            state.receiving = false;
        }
    }
}

/// Files a response under the context whose survey it answers, discarding
/// it when the survey is gone, expired, or already answered by that pipe.
fn route(shared: &Arc<SurveyorShared>, pipe: PipeId, mut message: Message) {
    let max_hops = shared.core.options().max_ttl;
    let Ok(shape) = backtrace::measure(message.body(), max_hops) else {
        return;
    };
    // An answer whose payload is over the limit of the endpoint it arrived
    // on is discarded, as an oversize survey is at a respondent.
    if !shared.core.payload_fits(pipe, message.len() - shape.len) {
        shared.late.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let now = Instant::now();
    let mut contexts = shared.lock();
    for state in contexts.values_mut() {
        let Some(survey) = state.survey.as_mut() else {
            continue;
        };
        if survey.id != shape.id {
            continue;
        }
        let size = message.wire_len() as u64;
        let full = crosses(
            shared.answer_bound,
            0,
            survey.answers.len(),
            survey.answers_bytes,
            size,
        )
        .is_some();
        if survey.deadline <= now || survey.answered.contains(&pipe) || full {
            // Late, or a second answer from a respondent that already
            // answered, or one more than the survey may hold. Discarded
            // with nothing sent back (§4).
            shared.late.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // The stack is split off only for an answer that is kept.
        if message.split_header_off_body(shape.len).is_err() {
            return;
        }
        survey.answered.insert(pipe);
        survey.answers_bytes = survey.answers_bytes.saturating_add(size);
        survey.answers.push_back(message);
        drop(contexts);
        // The context this answer belongs to is usually not the one that
        // drained the pipe it arrived on, and nothing else will wake it:
        // overlapping surveys are what contexts are for (§4).
        shared.delivered.notify_waiters();
        return;
    }
    // No context is collecting this survey any more: its answer is late by
    // definition.
    shared.late.fetch_add(1, Ordering::Relaxed);
}

// --------------------------------------------------------------------------
// RESPONDENT
// --------------------------------------------------------------------------

/// A RESPONDENT socket: receive a survey, then answer it — or do not.
///
/// The machine is [`crate::replier`]'s, the same one REP uses.
#[derive(Clone, Debug)]
pub struct RespondentSocket {
    core: Arc<SocketCore>,
    shared: Arc<Replier>,
    implicit: RespondentCtx,
}

/// One survey a respondent is answering: an `nng_ctx` on a RESPONDENT
/// socket.
pub type RespondentCtx = ReplierCtx;

impl RespondentSocket {
    /// A RESPONDENT socket on `context`, with NNG's defaults.
    pub fn new(context: &Context) -> Result<RespondentSocket> {
        RespondentSocket::with_options(context, SocketOptions::default())
    }

    /// A RESPONDENT socket with `options`.
    pub fn with_options(context: &Context, options: SocketOptions) -> Result<RespondentSocket> {
        let shared = Replier::new(
            context,
            EndpointType::Respondent,
            options,
            "a RESPONDENT context may send only the response to a survey it received",
        )?;
        let core = Arc::clone(shared.core());
        let implicit = ReplierCtx::open(&shared);
        Ok(RespondentSocket {
            core,
            shared,
            implicit,
        })
    }

    /// `nng_ctx_open()`: "respondent contexts route each incoming survey to
    /// one context and return its reply toward the latest survey received
    /// there" (§4).
    pub fn context(&self) -> RespondentCtx {
        ReplierCtx::open(&self.shared)
    }

    /// The context a bare [`RespondentSocket::recv`] and
    /// [`RespondentSocket::send`] use.
    pub fn implicit_context(&self) -> RespondentCtx {
        self.implicit.clone()
    }

    /// Surveys dropped with their pipe kept: a tag stack deeper than this
    /// socket's `NNG_OPT_MAXTTL`, or a payload over `NNG_OPT_RECVMAXSZ`
    /// once the stack is taken off.
    ///
    /// Both are messages to drop and not connections to end, and the sender
    /// is told nothing, so this count is the only trace.
    pub fn dropped(&self) -> u64 {
        self.shared.dropped()
    }

    /// Receives a survey on this socket's own context.
    pub async fn recv(&self) -> Result<Message> {
        self.implicit.recv().await
    }

    /// Answers the survey this socket's own context received.
    ///
    /// A respondent that never calls this has **declined**, which is a
    /// legitimate outcome the protocol provides no way to signal (§4).
    pub async fn send(&self, body: impl Into<Vec<u8>>) -> Result<()> {
        self.implicit.send(body).await
    }
}

socket_endpoints!(RespondentSocket);

#[cfg(test)]
mod tests {
    use super::*;

    use crate::context::ContextConfig;
    use weida_sp::Backtrace;

    fn options() -> SocketOptions {
        SocketOptions {
            survey_time: Duration::from_secs(2),
            ..SocketOptions::default()
        }
    }

    /// An answer as it comes off a pipe: the tag stack is in the body,
    /// where [`route`] looks for it, and not yet split into a header.
    fn arrived(id: u32, payload: &[u8]) -> Message {
        let mut body = Backtrace::direct(id).encode();
        body.extend_from_slice(payload);
        Message::from_body(body)
    }

    fn survey_id(ctx: &SurveyorCtx) -> u32 {
        ctx.shared()
            .lock()
            .get(&ctx.id())
            .and_then(|state| state.survey.as_ref().map(|survey| survey.id))
            .expect("an armed survey")
    }

    /// Claim: an answer filed by a **sibling** context wakes the context it
    /// belongs to. Whichever context is inside a receive drains the pipes
    /// for all of them, so the context that owns an answer is usually not
    /// the one that took it off the pipe; if the filing does not wake the
    /// owner, its receive waits out the deadline and reports
    /// `NNG_ETIMEDOUT` for an answer that is already in memory — and the
    /// answer is not counted late either, so it is neither delivered nor
    /// discarded. Calling [`route`] directly is exactly what a sibling's
    /// drain does, with no pipe traffic that could wake the parked receive
    /// by accident.
    #[tokio::test]
    async fn an_answer_filed_by_a_sibling_wakes_the_context_it_belongs_to() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let surveyor = SurveyorSocket::with_options(&ctx, options()).expect("surveyor");
        let parked = surveyor.context();
        let sibling = surveyor.context();
        parked.send(b"who is there".to_vec()).await.expect("one");
        sibling.send(b"and who else".to_vec()).await.expect("two");

        let id = survey_id(&parked);
        let collecting = parked.recv();
        let mut collecting = std::pin::pin!(collecting);
        assert!(
            futures::poll!(collecting.as_mut()).is_pending(),
            "nobody has answered yet"
        );

        route(&surveyor.shared, PipeId::new(1), arrived(id, b"here"));

        let answer = collecting.await.expect("the answer the sibling filed");
        assert_eq!(answer.body(), b"here");
        assert_eq!(surveyor.discarded_late(), 0, "it was delivered, not late");
    }
}
