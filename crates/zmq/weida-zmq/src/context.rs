//! The context: the reactor, the `inproc` namespace and the socket ceiling.
//!
//! libzmq's context "holds the I/O thread pool and the `inproc` namespace"
//! and bounds how many sockets may exist at once
//! (`docs/research/zeromq.md` §2). This one holds the same three things, with
//! the thread pool replaced by `weida-runtime`'s [`Exec`]: a reactor the
//! context may own, borrow from the ambient runtime, or take a handle to —
//! the three constructors of
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4, which
//! mirror weida's `Runtime` because a ZeroMQ library's audience is
//! overwhelmingly synchronous and must not have to stand in a reactor to call
//! it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use weida_runtime::{CloseBudget, Exec, OwnedReactor};

use crate::endpoint::MAX_INPROC_NAME_BYTES;
use crate::error::{Error, Result};
use crate::inproc::Inproc;

/// Sockets one context may hold at once, by default.
///
/// libzmq's `ZMQ_MAX_SOCKETS` default, and `zmq_socket()` reports `EMFILE`
/// once it is reached (`docs/research/zeromq.md` §11).
pub const DEFAULT_MAX_SOCKETS: usize = 1023;

/// How long [`Context::shutdown`] waits for open sockets, by default.
///
/// **This is one of the two defaults that deliberately differ from libzmq**
/// (0013 §4.4 item 5). `ZMQ_LINGER` defaults to `-1`, infinite, so
/// `zmq_ctx_term()` can block for as long as a peer likes — a hang with a
/// rationale, and a well-known bug source among libzmq's own users
/// (`docs/research/zeromq.md` §11, §12/P17). A finite default is the same
/// discipline weida applies to its own shutdown
/// ([0009](../../../docs/decisions/0009-drain.md) §4.3, §4.4), and one second
/// is the number weida picked there. It is settable — including to something
/// very large — but it is never silent, and it is never infinite by
/// accident.
pub const DEFAULT_CLOSE_BUDGET: Duration = Duration::from_secs(1);

/// How a [`Context`] is configured.
///
/// Every field is an option libzmq has, under the name 0013 §4.4 chose for
/// it. `ZMQ_IO_THREADS` is deliberately absent: it is replaced by
/// `worker_threads`, because the thread pool is a Tokio runtime here and
/// sizing it is the reactor's business.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextConfig {
    /// `ZMQ_MAX_SOCKETS`: sockets this context may hold at once. Must be at
    /// least 1.
    pub max_sockets: usize,
    /// Worker threads of the reactor [`Context::owned`] creates, replacing
    /// `ZMQ_IO_THREADS` (0013 §4.4 item 4). Ignored by [`Context::new`] and
    /// [`Context::with_handle`], which run on a reactor somebody else sized.
    pub worker_threads: usize,
    /// `ZMQ_LINGER` as a bounded close budget: how long
    /// [`Context::shutdown`] waits for the sockets of this context to be
    /// dropped. See [`DEFAULT_CLOSE_BUDGET`] for why it is finite.
    pub close_budget: Duration,
}

impl Default for ContextConfig {
    fn default() -> ContextConfig {
        ContextConfig {
            max_sockets: DEFAULT_MAX_SOCKETS,
            worker_threads: 1,
            close_budget: DEFAULT_CLOSE_BUDGET,
        }
    }
}

impl ContextConfig {
    /// Refuses an unusable configuration at configuration time, which is the
    /// rule every option in 0013 §4.4 item 4 follows: honoured or refused,
    /// never silently corrected.
    fn validate(&self, sizing_a_reactor: bool) -> Result<()> {
        if self.max_sockets == 0 {
            return Err(Error::EINVAL(
                "ContextConfig::max_sockets must be at least 1 (ZMQ_MAX_SOCKETS)".into(),
            ));
        }
        if sizing_a_reactor && self.worker_threads == 0 {
            return Err(Error::EINVAL(
                "ContextConfig::worker_threads must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// A socket's identity inside one context.
///
/// libzmq's `zmq_socket()` hands back an opaque pointer; this is the same
/// idea with a number, allocated by the context so that a socket can be
/// named in a log line, counted against the ceiling and recognised by the
/// peer it dialled over `inproc://`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SocketId(u64);

impl SocketId {
    /// The number, for a caller that wants to print or key on it.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SocketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "socket {}", self.0)
    }
}

/// What every socket of one context shares.
struct ContextInner {
    config: ContextConfig,
    exec: Exec,
    inproc: Arc<Inproc>,
    state: Mutex<State>,
    /// Signalled whenever a socket slot is released, so that `shutdown` can
    /// wait for the last one without polling.
    released: Notify,
    /// Present only for a context created by [`Context::owned`]; dropped with
    /// the last clone, which shuts the reactor down in the background.
    _reactor: Option<OwnedReactor>,
}

/// The two numbers and the one flag the ceiling decision needs, under one
/// lock so that the decision is atomic: a socket is never admitted to a
/// context that has begun terminating.
struct State {
    open: usize,
    next_id: u64,
    terminated: bool,
}

/// A process-level execution and resource container for ZeroMQ sockets.
///
/// Cloning shares everything: the reactor, the `inproc` namespace, the socket
/// ceiling and the termination flag. That is libzmq's context too — "two
/// contexts are two separate ZeroMQ instances" (`docs/research/zeromq.md`
/// §2), and two clones are one instance.
///
/// Three ways to get one, differing only in where the reactor comes from:
/// [`Context::new`] borrows the ambient one, [`Context::with_handle`] takes a
/// handle to somebody else's, and [`Context::owned`] creates and owns one.
#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

impl Context {
    /// Creates a context on the current Tokio reactor.
    ///
    /// Fails with `EMTHREAD` when there is none: the sockets need one, and
    /// failing here beats failing later at an unrelated call site.
    pub fn new(config: ContextConfig) -> Result<Context> {
        config.validate(false)?;
        Ok(Context::from_parts(config, Exec::current()?, None))
    }

    /// Creates a context on the Tokio runtime `handle` names.
    ///
    /// For a process that already runs a reactor somewhere other than the
    /// calling thread: nothing has to be ambient, and the caller's own
    /// executor is never consulted.
    ///
    /// Returns a `Result` where weida's `Runtime::with_handle` — which this
    /// otherwise mirrors — is infallible, because a `ContextConfig` carries
    /// ZeroMQ options and an option is refused where it is configured:
    /// `max_sockets: 0` is `EINVAL` here rather than a ceiling nothing can
    /// pass later (0013 §4.4 item 4).
    pub fn with_handle(handle: tokio::runtime::Handle, config: ContextConfig) -> Result<Context> {
        config.validate(false)?;
        Ok(Context::from_parts(config, Exec::from_handle(handle), None))
    }

    /// Creates a context that **owns** a reactor with
    /// [`ContextConfig::worker_threads`] workers.
    ///
    /// This is the constructor libzmq's audience needs: no reactor, now or
    /// later, and the futures this library hands back may be driven by any
    /// executor.
    ///
    /// ```
    /// use weida_zmq::{Context, ContextConfig};
    ///
    /// // No ambient Tokio runtime anywhere, and not even a Tokio executor
    /// // driving the future: the context owns the reactor its sockets need.
    /// let ctx = Context::owned(ContextConfig::default())?;
    /// let socket = ctx.open_socket()?;
    /// assert_eq!(ctx.socket_count(), 1);
    /// drop(socket);
    ///
    /// let report = futures::executor::block_on(ctx.shutdown());
    /// assert_eq!(report.outstanding(), 0);
    /// # Ok::<(), weida_zmq::Error>(())
    /// ```
    ///
    /// Fails with `EINVAL` when `worker_threads` is `0`, and with `EMTHREAD`
    /// when the OS refuses the threads.
    pub fn owned(config: ContextConfig) -> Result<Context> {
        config.validate(true)?;
        let (exec, reactor) = Exec::owned(config.worker_threads, "weida-zmq")?;
        Ok(Context::from_parts(config, exec, Some(reactor)))
    }

    fn from_parts(config: ContextConfig, exec: Exec, reactor: Option<OwnedReactor>) -> Context {
        Context {
            inner: Arc::new(ContextInner {
                exec,
                inproc: Arc::new(Inproc::new(MAX_INPROC_NAME_BYTES)),
                state: Mutex::new(State {
                    open: 0,
                    next_id: 1,
                    terminated: false,
                }),
                released: Notify::new(),
                config,
                _reactor: reactor,
            }),
        }
    }

    /// The configuration this context was created with.
    pub fn config(&self) -> &ContextConfig {
        &self.inner.config
    }

    /// The reactor every socket of this context runs its tasks, timers and
    /// name lookups on.
    pub fn exec(&self) -> &Exec {
        &self.inner.exec
    }

    /// This context's `inproc://` namespace.
    ///
    /// Context-scoped, which is what libzmq's is: a name bound in one context
    /// is invisible in another, "two contexts are two separate ZeroMQ
    /// instances" (`docs/research/zeromq.md` §2). Names are bounded at
    /// [`MAX_INPROC_NAME_BYTES`].
    /// Handed out as the shared handle it is: a binding outlives the call
    /// that made it and unbinds its name when it is dropped, so the
    /// namespace has to be reference-counted rather than borrowed.
    pub fn inproc(&self) -> &Arc<Inproc> {
        &self.inner.inproc
    }

    /// Sockets currently open on this context.
    pub fn socket_count(&self) -> usize {
        self.lock().open
    }

    /// Whether [`Context::shutdown`] has begun.
    pub fn is_terminated(&self) -> bool {
        self.lock().terminated
    }

    /// Reserves a socket slot against the ceiling and allocates its id.
    ///
    /// This is `zmq_socket()`'s accounting without a socket type: the ceiling
    /// is checked here, so every socket type gets it for free by holding one
    /// of these. The slot is released when it is dropped, which is
    /// `zmq_close()`.
    ///
    /// Fails with `EMFILE` at the ceiling — exactly where libzmq fails
    /// (`docs/research/zeromq.md` §11) — and with `ETERM` once the context is
    /// terminating, because a terminated context admits nothing new.
    pub fn open_socket(&self) -> Result<SocketSlot> {
        let mut state = self.lock();
        if state.terminated {
            return Err(Error::ETERM(
                "the context is terminated; it admits no new socket".into(),
            ));
        }
        let ceiling = self.inner.config.max_sockets;
        if state.open >= ceiling {
            return Err(Error::EMFILE(
                format!(
                    "this context already holds {} sockets, its ZMQ_MAX_SOCKETS ceiling",
                    ceiling
                )
                .into(),
            ));
        }
        state.open += 1;
        let id = SocketId(state.next_id);
        state.next_id += 1;
        drop(state);
        Ok(SocketSlot {
            id,
            context: Arc::clone(&self.inner),
        })
    }

    /// Terminates the context: admits no further socket, then waits — for at
    /// most [`ContextConfig::close_budget`] — for the sockets it still holds
    /// to be dropped.
    ///
    /// This is `zmq_ctx_term()` with a bound. libzmq's blocks until every
    /// socket is closed and, with the default infinite `ZMQ_LINGER`, that is
    /// forever (`docs/research/zeromq.md` §11, §12/P17). Here the wait is a
    /// [`CloseBudget`], and what the budget did not achieve comes back as a
    /// number rather than as an error: [`Terminated::outstanding`] is
    /// something to log, retry against or ignore, exactly as weida's drain
    /// receipts are ([0009](../../../docs/decisions/0009-drain.md) §4.6).
    ///
    /// Sockets that outlive the budget are not broken: they keep working
    /// until they are dropped, and every operation on them reports `ETERM`,
    /// which is how libzmq unblocks a thread parked in `zmq_recv`.
    pub async fn shutdown(self) -> Terminated {
        let budget = CloseBudget::start(self.inner.config.close_budget);
        self.lock().terminated = true;

        loop {
            // Subscribe before reading the count: a slot released between the
            // two must wake this wait rather than be missed by it.
            let released = self.inner.released.notified();
            let open = self.lock().open;
            if open == 0 {
                return Terminated { outstanding: 0 };
            }
            if budget.is_spent() {
                return Terminated { outstanding: open };
            }
            let deadline = self.inner.exec.sleep(budget.remaining());
            tokio::select! {
                () = released => {}
                () = deadline => return Terminated { outstanding: self.lock().open },
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().expect("context state poisoned")
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Context")
            .field("max_sockets", &self.inner.config.max_sockets)
            .field("open_sockets", &state.open)
            .field("terminated", &state.terminated)
            .finish_non_exhaustive()
    }
}

/// One socket's slot under its context's ceiling, plus its id.
///
/// Every socket type holds one, which is what makes
/// `ZMQ_MAX_SOCKETS` a fact about the context rather than a number each
/// socket type has to remember. Dropping it is `zmq_close()`: the slot is
/// returned and a `shutdown` waiting for it is woken.
pub struct SocketSlot {
    id: SocketId,
    context: Arc<ContextInner>,
}

impl std::fmt::Debug for SocketSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketSlot")
            .field("id", &self.id)
            .field("terminated", &self.is_terminated())
            .finish()
    }
}

impl SocketSlot {
    /// This socket's id within its context.
    pub const fn id(&self) -> SocketId {
        self.id
    }

    /// Whether the context has begun terminating, which every operation on
    /// the socket holding this slot must report as `ETERM`.
    pub fn is_terminated(&self) -> bool {
        self.context
            .state
            .lock()
            .expect("context state poisoned")
            .terminated
    }
}

impl Drop for SocketSlot {
    fn drop(&mut self) {
        {
            let mut state = self.context.state.lock().expect("context state poisoned");
            state.open -= 1;
        }
        // Only the last release ends a waiting `shutdown`, but waking on
        // every one costs nothing and keeps the rule simple: the waiter
        // re-reads the count.
        self.context.released.notify_waiters();
    }
}

/// What a [`Context::shutdown`] achieved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Terminated {
    outstanding: usize,
}

impl Terminated {
    /// Sockets still open when the close budget ran out. Zero means the
    /// context is fully closed; anything else is a number to log — not an
    /// error, because the alternative to reporting it is waiting forever.
    pub const fn outstanding(&self) -> usize {
        self.outstanding
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn small(max_sockets: usize) -> ContextConfig {
        ContextConfig {
            max_sockets,
            ..ContextConfig::default()
        }
    }

    /// Claim: `new` needs an ambient reactor and says so in libzmq's
    /// vocabulary rather than weida's.
    #[test]
    fn a_context_without_a_reactor_fails_with_emthread() {
        let err = Context::new(ContextConfig::default()).unwrap_err();
        assert_eq!(err.errno(), "EMTHREAD", "{err}");
    }

    /// Claim: `with_handle` runs on somebody else's reactor and the calling
    /// thread still has none.
    #[test]
    fn with_handle_uses_the_handed_reactor() {
        let reactor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a reactor");
        let ctx = Context::with_handle(reactor.handle().clone(), ContextConfig::default())
            .expect("context");
        assert!(tokio::runtime::Handle::try_current().is_err());
        assert_eq!(ctx.config().max_sockets, DEFAULT_MAX_SOCKETS);
        assert_eq!(ctx.socket_count(), 0);
    }

    /// Claim: an owned context needs no ambient reactor, on this thread or
    /// any other.
    #[test]
    fn an_owned_context_needs_no_ambient_reactor() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let ctx = Context::owned(ContextConfig::default()).expect("context");
        let slot = ctx.open_socket().expect("a socket slot");
        assert_eq!(slot.id().get(), 1);
        drop(slot);
        let report = futures::executor::block_on(ctx.shutdown());
        assert_eq!(report.outstanding(), 0);
    }

    /// Claim: an unusable option value is refused when it is configured, with
    /// `EINVAL`, rather than corrected or ignored (0013 §4.4 item 4).
    #[test]
    fn unusable_configuration_is_refused_with_einval() {
        let err = Context::owned(ContextConfig {
            worker_threads: 0,
            ..ContextConfig::default()
        })
        .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        let err = Context::owned(small(0)).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        // `new` and `with_handle` size no reactor, so `worker_threads` is
        // theirs to ignore — as `Runtime`'s are.
        let reactor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a reactor");
        assert!(
            Context::with_handle(
                reactor.handle().clone(),
                ContextConfig {
                    worker_threads: 0,
                    ..ContextConfig::default()
                }
            )
            .is_ok()
        );
    }

    /// Claim: a socket past the ceiling fails with `EMFILE`, where libzmq's
    /// `zmq_socket()` fails, and a closed socket frees its slot.
    #[tokio::test]
    async fn the_socket_ceiling_is_emfile() {
        let ctx = Context::new(small(2)).expect("context");
        let first = ctx.open_socket().expect("first");
        let second = ctx.open_socket().expect("second");
        assert_eq!(ctx.socket_count(), 2);

        let err = ctx.open_socket().unwrap_err();
        assert_eq!(err.errno(), "EMFILE", "{err}");
        assert!(err.cause().contains("ZMQ_MAX_SOCKETS"), "{err}");

        // zmq_close() frees the slot, and the next socket gets a fresh id
        // rather than the closed one's.
        drop(second);
        assert_eq!(ctx.socket_count(), 1);
        let third = ctx.open_socket().expect("after a close");
        assert_ne!(third.id(), first.id());
        assert_eq!(third.id().get(), 3);
    }

    /// Claim: a terminated context admits no new socket, and the sockets it
    /// still holds know they are on one.
    #[tokio::test(start_paused = true)]
    async fn a_terminated_context_admits_nothing_and_says_so() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let slot = ctx.open_socket().expect("a slot");
        assert!(!slot.is_terminated());
        assert!(!ctx.is_terminated());

        let terminating = ctx.clone();
        let report = terminating.shutdown().await;
        assert_eq!(report.outstanding(), 1, "the socket was still open");
        assert!(ctx.is_terminated());
        assert!(slot.is_terminated());

        let err = ctx.open_socket().unwrap_err();
        assert_eq!(err.errno(), "ETERM", "{err}");
    }

    /// Claim: shutdown waits for an open socket and returns as soon as the
    /// last one is dropped — the bounded version of `zmq_ctx_term()`.
    #[tokio::test(start_paused = true)]
    async fn shutdown_waits_for_the_last_socket_within_the_budget() {
        let ctx = Context::new(ContextConfig {
            close_budget: Duration::from_secs(30),
            ..ContextConfig::default()
        })
        .expect("context");
        let slot = ctx.open_socket().expect("a slot");
        // Margin: the socket closes at 50 ms and the budget is 30 s, so the
        // two outcomes are three orders of magnitude apart and the assertion
        // below sits between them. The defect this catches — a shutdown that
        // waits out its budget instead of noticing the last close — takes
        // 30 s and fails at 5.
        let closing = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(slot);
        });

        let started = tokio::time::Instant::now();
        let report = ctx.shutdown().await;
        closing.await.expect("the closing task");
        assert_eq!(report.outstanding(), 0);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the wait must end with the last socket, not with the budget"
        );
    }

    /// Claim: the close budget is finite and it bites — the deliberate
    /// deviation from `ZMQ_LINGER = -1`, which would hang here forever.
    ///
    /// Margin: a 20 ms budget against a 5 s assertion, so the machine has
    /// 250 times the budget to get there; the defect — an infinite linger —
    /// never returns at all, which no margin can rescue.
    #[tokio::test(start_paused = true)]
    async fn the_close_budget_is_finite() {
        let ctx = Context::new(ContextConfig {
            close_budget: Duration::from_millis(20),
            ..ContextConfig::default()
        })
        .expect("context");
        let _held = ctx.open_socket().expect("a slot");

        let started = tokio::time::Instant::now();
        let report = ctx.shutdown().await;
        assert_eq!(report.outstanding(), 1);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "an infinite linger would never have returned"
        );
    }

    /// Claim: the `inproc` namespace belongs to the context — the same name
    /// is free in a second context, a dial in one never reaches the other,
    /// and the budget is libzmq's 256 bytes.
    #[tokio::test]
    async fn the_inproc_namespace_is_the_contexts_own() {
        let ctx = Context::new(ContextConfig::default()).expect("context");
        let other = Context::new(ContextConfig::default()).expect("second context");

        let mut here = ctx.inproc().bind("orders").expect("bind");
        // A second context is a second ZeroMQ instance: the same name is free
        // there, and a dial in one never reaches the other.
        let mut elsewhere = other
            .inproc()
            .bind("orders")
            .expect("free in a fresh context");

        let dialler = ctx.open_socket().expect("a slot");
        let _ours = ctx
            .inproc()
            .dial("orders", dialler.id())
            .expect("our own binder");
        assert_eq!(
            here.accept().await.expect("delivered here").from,
            dialler.id()
        );
        assert!(
            elsewhere.accept().now_or_never().is_none(),
            "the other context's binder must have seen nothing"
        );

        assert_eq!(ctx.inproc().max_name_bytes(), MAX_INPROC_NAME_BYTES);
        assert!(
            ctx.inproc()
                .bind(&"x".repeat(MAX_INPROC_NAME_BYTES + 1))
                .is_err()
        );
    }
}
