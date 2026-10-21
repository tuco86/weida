//! The context: the reactor, the `inproc://` namespace and the socket
//! ceiling.
//!
//! **NNG has no object like this, and that is the point.** `nng_socket` is a
//! value in a process-global table, the I/O threads are NNG's own, and
//! `nng_fini()` tears the library down for everybody. A Rust library cannot
//! own the process, so the three things NNG keeps global — the reactor, the
//! `inproc` namespace and the resource ceiling — are held by an explicit
//! value instead, created three ways
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4): on
//! the ambient reactor ([`Context::new`]), on somebody else's
//! ([`Context::with_handle`]), or on one it owns ([`Context::owned`]), which
//! is what a caller with no reactor at all needs.
//!
//! **This is not `nng_ctx`.** NNG's context is per-transaction protocol
//! state — one request ID and its resend timer, one survey and its deadline
//! (`docs/research/nanomsg-nng.md` §2) — and it belongs to a socket, not to
//! a process. Those are the per-protocol context types of REQ/REP and
//! SURVEYOR/RESPONDENT; this type is the container they all run in.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use weida_runtime::{CloseBudget, Exec, OwnedReactor};

/// Sockets one context may hold at once, by default.
///
/// **NNG publishes no such ceiling**, because its sockets live in a global
/// table sized by the process. One is required here for the reason every
/// bound in this crate exists: a socket is created on behalf of something,
/// and in a server that something is eventually a peer
/// (`docs/INVARIANTS.md`). The number matches the ceiling `weida-zmq` takes
/// from `ZMQ_MAX_SOCKETS`, so that two libraries in one process do not
/// surprise an operator with two different numbers, and it is settable.
pub const DEFAULT_MAX_SOCKETS: usize = 1023;

/// How long [`Context::shutdown`] waits for open sockets, by default.
///
/// NNG has no linger and no drain: "Closing a pipe removes it; no generic
/// in-flight drain, linger, or outcome guarantee is specified" (§12/P17). A
/// close that waited on a peer's behaviour forever would be a hang with a
/// rationale, so the wait is finite and the part it did not achieve comes
/// back as a number ([0009](../../../docs/decisions/0009-drain.md) §4.3).
/// One second is the number weida picked there.
pub const DEFAULT_CLOSE_BUDGET: Duration = Duration::from_secs(1);

/// How a [`Context`] is configured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextConfig {
    /// Sockets this context may hold at once. Must be at least 1. See
    /// [`DEFAULT_MAX_SOCKETS`] for why the number exists at all.
    pub max_sockets: usize,
    /// Worker threads of the reactor [`Context::owned`] creates. Ignored by
    /// [`Context::new`] and [`Context::with_handle`], which run on a reactor
    /// somebody else sized.
    pub worker_threads: usize,
    /// How long [`Context::shutdown`] waits for the sockets of this context
    /// to be dropped. See [`DEFAULT_CLOSE_BUDGET`].
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
    /// Refuses an unusable configuration where it is configured, which is
    /// the rule every option in this crate follows: honoured or refused,
    /// never silently corrected (0013 §4.4 item 4).
    fn validate(&self, sizing_a_reactor: bool) -> Result<()> {
        if self.max_sockets == 0 {
            return Err(Error::EINVAL(
                "ContextConfig::max_sockets must be at least 1".into(),
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

use crate::endpoint::MAX_INPROC_NAME_BYTES;
use crate::error::{Error, Result};

/// A socket's identity inside one context.
///
/// NNG's `nng_socket` carries an `id` an application can read
/// (`nng_socket_id()`); this is that number, allocated by the context so
/// that a socket can be named in a log line and counted against the ceiling.
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
    inproc: Arc<crate::inproc::Inproc>,
    state: Mutex<State>,
    /// Signalled whenever a socket slot is released, so that `shutdown` can
    /// wait for the last one without polling.
    released: Notify,
    /// Present only for a context created by [`Context::owned`]; dropped
    /// with the last clone, which shuts the reactor down in the background.
    _reactor: Option<OwnedReactor>,
}

/// The two numbers and the one flag the ceiling decision needs, under one
/// lock so that the decision is atomic: a socket is never admitted to a
/// context that has begun closing.
struct State {
    open: usize,
    next_id: u64,
    closed: bool,
}

/// The reactor, the `inproc://` namespace and the socket ceiling a set of
/// NNG sockets shares.
///
/// Cloning shares all three: two clones are one container, and two contexts
/// are two — an `inproc://` name bound in one is invisible in the other,
/// which is the only scoping rule an in-process namespace can have when the
/// process is not the unit.
#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

impl Context {
    /// Creates a context on the current Tokio reactor.
    ///
    /// Fails with `NNG_ENOFILES` when there is none: the sockets need one,
    /// and failing here beats failing later at an unrelated call site.
    pub fn new(config: ContextConfig) -> Result<Context> {
        config.validate(false)?;
        Ok(Context::from_parts(config, Exec::current()?, None))
    }

    /// Creates a context on the Tokio runtime `handle` names.
    ///
    /// For a process that already runs a reactor somewhere other than the
    /// calling thread: nothing has to be ambient, and the caller's own
    /// executor is never consulted.
    pub fn with_handle(handle: tokio::runtime::Handle, config: ContextConfig) -> Result<Context> {
        config.validate(false)?;
        Ok(Context::from_parts(config, Exec::from_handle(handle), None))
    }

    /// Creates a context that **owns** a reactor with
    /// [`ContextConfig::worker_threads`] workers.
    ///
    /// This is the constructor NNG's audience needs: NNG starts its own I/O
    /// threads when the library initializes and its users write plain
    /// blocking calls, so a Rust port that demanded an ambient Tokio runtime
    /// would demand something none of them has.
    ///
    /// ```
    /// use weida_nng::{Context, ContextConfig};
    ///
    /// // No ambient Tokio runtime anywhere: the context owns the reactor its
    /// // sockets need, and the futures it hands back run on any executor.
    /// let ctx = Context::owned(ContextConfig::default())?;
    /// let slot = ctx.open_socket()?;
    /// assert_eq!(ctx.socket_count(), 1);
    /// drop(slot);
    /// # Ok::<(), weida_nng::Error>(())
    /// ```
    ///
    /// Fails with `NNG_EINVAL` when `worker_threads` is `0`, and with
    /// `NNG_ENOFILES` when the OS refuses the threads.
    pub fn owned(config: ContextConfig) -> Result<Context> {
        config.validate(true)?;
        let (exec, reactor) = Exec::owned(config.worker_threads, "weida-nng")?;
        Ok(Context::from_parts(config, exec, Some(reactor)))
    }

    fn from_parts(config: ContextConfig, exec: Exec, reactor: Option<OwnedReactor>) -> Context {
        Context {
            inner: Arc::new(ContextInner {
                exec,
                inproc: Arc::new(crate::inproc::Inproc::new(MAX_INPROC_NAME_BYTES)),
                state: Mutex::new(State {
                    open: 0,
                    next_id: 1,
                    closed: false,
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
    /// Context-scoped: a name bound in one context is invisible in
    /// another, which is the only scoping rule an in-process namespace can
    /// have when the process is not the unit. Names are bounded at
    /// [`MAX_INPROC_NAME_BYTES`]. Handed out as the shared handle it is,
    /// because a binding outlives the call that made it and frees its name
    /// when it is dropped.
    pub fn inproc(&self) -> &Arc<crate::inproc::Inproc> {
        &self.inner.inproc
    }

    /// The byte budget an `inproc://` name in this context lives under.
    pub const fn max_inproc_name_bytes(&self) -> usize {
        MAX_INPROC_NAME_BYTES
    }

    /// Sockets currently open on this context.
    pub fn socket_count(&self) -> usize {
        self.lock().open
    }

    /// Whether [`Context::shutdown`] has begun.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Reserves a socket slot against the ceiling and allocates its id.
    ///
    /// This is `nng_*_open()`'s accounting without a protocol: the ceiling is
    /// checked here, so every socket type gets it by holding one of these.
    /// The slot is released when it is dropped, which is `nng_close()`.
    ///
    /// Fails with `NNG_ENOFILES` at the ceiling and with `NNG_ECLOSED` once
    /// the context is closing, because a closed context admits nothing new.
    pub fn open_socket(&self) -> Result<SocketSlot> {
        let mut state = self.lock();
        if state.closed {
            return Err(Error::ECLOSED(
                "the context is closed; it admits no new socket".into(),
            ));
        }
        let ceiling = self.inner.config.max_sockets;
        if state.open >= ceiling {
            return Err(Error::ENOFILES(
                format!("this context already holds its ceiling of {ceiling} sockets").into(),
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

    /// Closes the context: admits no further socket, then waits — for at
    /// most [`ContextConfig::close_budget`] — for the sockets it holds to be
    /// dropped.
    ///
    /// What the budget did not achieve comes back as a number rather than as
    /// an error: [`Closed::outstanding`] is something to log, retry against
    /// or ignore. Sockets that outlive the budget are not broken; they keep
    /// working until they are dropped, and every attempt to open a new one
    /// reports `NNG_ECLOSED`.
    pub async fn shutdown(self) -> Closed {
        let budget = CloseBudget::start(self.inner.config.close_budget);
        self.lock().closed = true;

        loop {
            // Subscribe before reading the count: a slot released between
            // the two must wake this wait rather than be missed by it.
            let released = self.inner.released.notified();
            let open = self.lock().open;
            if open == 0 {
                return Closed { outstanding: 0 };
            }
            if budget.is_spent() {
                return Closed { outstanding: open };
            }
            let deadline = self.inner.exec.sleep(budget.remaining());
            tokio::select! {
                () = released => {}
                () = deadline => return Closed { outstanding: self.lock().open },
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
            .field("closed", &state.closed)
            .finish_non_exhaustive()
    }
}

/// One socket's slot under its context's ceiling, plus its id.
///
/// Every socket type holds one, which is what makes the ceiling a fact about
/// the context rather than a number each socket type has to remember.
/// Dropping it is `nng_close()`: the slot is returned and a `shutdown`
/// waiting for it is woken.
pub struct SocketSlot {
    id: SocketId,
    context: Arc<ContextInner>,
}

impl std::fmt::Debug for SocketSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketSlot")
            .field("id", &self.id)
            .field("context_closed", &self.context_is_closed())
            .finish()
    }
}

impl SocketSlot {
    /// This socket's id within its context.
    pub const fn id(&self) -> SocketId {
        self.id
    }

    /// Whether the context has begun closing, which every operation on the
    /// socket holding this slot reports as `NNG_ECLOSED`.
    pub fn context_is_closed(&self) -> bool {
        self.context
            .state
            .lock()
            .expect("context state poisoned")
            .closed
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
pub struct Closed {
    outstanding: usize,
}

impl Closed {
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

    fn small(max_sockets: usize) -> ContextConfig {
        ContextConfig {
            max_sockets,
            ..ContextConfig::default()
        }
    }

    /// Claim: the three constructors differ only in where the reactor comes
    /// from, and the one that needs an ambient reactor says so in NNG's
    /// vocabulary rather than weida's.
    #[test]
    fn the_three_constructors_differ_only_in_the_reactor() {
        let err = Context::new(ContextConfig::default()).unwrap_err();
        assert_eq!(err.name(), "NNG_ENOFILES", "{err}");

        let reactor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a reactor");
        let handed = Context::with_handle(reactor.handle().clone(), ContextConfig::default())
            .expect("context on a handed reactor");
        assert!(tokio::runtime::Handle::try_current().is_err());
        assert_eq!(handed.socket_count(), 0);

        let owned = Context::owned(ContextConfig::default()).expect("owned reactor");
        assert_eq!(owned.config().max_sockets, DEFAULT_MAX_SOCKETS);
    }

    /// Claim: an unusable configuration is refused where it is configured,
    /// with `NNG_EINVAL`, rather than becoming a ceiling nothing can pass.
    #[test]
    fn a_zero_ceiling_is_refused_at_configuration_time() {
        let err = Context::owned(small(0)).unwrap_err();
        assert!(matches!(err, Error::EINVAL(_)), "{err:?}");
        let err = Context::owned(ContextConfig {
            worker_threads: 0,
            ..ContextConfig::default()
        })
        .unwrap_err();
        assert!(matches!(err, Error::EINVAL(_)), "{err:?}");
    }

    /// Claim: the ceiling is real and is reported as `NNG_ENOFILES`, and a
    /// closed socket returns its slot so the next open succeeds.
    #[test]
    fn the_socket_ceiling_admits_exactly_its_number() {
        let ctx = Context::owned(small(2)).expect("context");
        let first = ctx.open_socket().expect("first");
        let second = ctx.open_socket().expect("second");
        assert_eq!(ctx.socket_count(), 2);
        assert_ne!(first.id(), second.id());

        let err = ctx.open_socket().unwrap_err();
        assert!(matches!(err, Error::ENOFILES(_)), "{err:?}");
        assert!(err.cause().contains('2'));

        drop(second);
        assert_eq!(ctx.socket_count(), 1);
        assert!(ctx.open_socket().is_ok());
    }

    /// Claim: a clone is the same container — the same ceiling, the same
    /// count — because two clones are one context and two contexts are two.
    #[test]
    fn a_clone_shares_the_ceiling() {
        let ctx = Context::owned(small(1)).expect("context");
        let clone = ctx.clone();
        let _slot = ctx.open_socket().expect("first");
        assert_eq!(clone.socket_count(), 1);
        assert!(matches!(clone.open_socket(), Err(Error::ENOFILES(_))));

        let separate = Context::owned(small(1)).expect("a second context");
        assert_eq!(separate.socket_count(), 0);
        assert!(separate.open_socket().is_ok());
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: shutdown is bounded. A socket held past the budget does not
        /// hang the close; it comes back as a count, and the context admits
        /// nothing new afterwards.
        #[test]
        fn shutdown_is_bounded_and_reports_what_it_could_not_close() {
            let ctx = Context::owned(ContextConfig {
                close_budget: Duration::from_millis(20),
                ..ContextConfig::default()
            })
            .expect("context");
            let held = ctx.open_socket().expect("socket");

            let report = futures::executor::block_on(ctx.clone().shutdown());
            assert_eq!(report.outstanding(), 1);
            assert!(ctx.is_closed());
            assert!(matches!(ctx.open_socket(), Err(Error::ECLOSED(_))));
            assert!(held.context_is_closed());

            drop(held);
            let report = futures::executor::block_on(ctx.shutdown());
            assert_eq!(report.outstanding(), 0);
        }
    }
}
