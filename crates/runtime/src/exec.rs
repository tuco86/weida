//! The one surface onto the async runtime: tasks, timers and DNS.

use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use weida_core::Error;

/// A library's whole surface onto the async runtime: tasks, timers and DNS.
///
/// An `Exec` is a Tokio handle and nothing more. It never owns the runtime,
/// so a task holding one can neither keep the runtime alive nor drop it from
/// inside itself. Cloning is a handle clone.
///
/// **Why a type rather than plain `tokio::spawn`.** A library that calls
/// `tokio::spawn`, `tokio::time` or `tokio::net::lookup_host` directly
/// demands that *its caller* be inside a Tokio reactor, on the very thread
/// that called it. Holding an `Exec` moves that requirement into the library:
/// the reactor is wherever the `Exec` points, the caller may drive the
/// returned futures on any executor — `futures::executor::block_on`
/// included — and a grep for those three names over the library's own `src`
/// is the proof that no path escaped
/// (`docs/ARCHITECTURE.md` §5).
#[derive(Clone)]
pub struct Exec {
    handle: Handle,
}

impl Exec {
    /// Wraps the runtime `handle` names.
    ///
    /// For a process that already runs a reactor somewhere other than the
    /// calling thread: nothing has to be ambient, and the caller's own
    /// executor is never consulted.
    pub fn from_handle(handle: Handle) -> Exec {
        Exec { handle }
    }

    /// The ambient handle.
    ///
    /// Fails when the calling thread is not inside a Tokio runtime. Failing
    /// here — at construction — beats failing later at an unrelated call
    /// site.
    pub fn current() -> Result<Exec, Error> {
        Handle::try_current()
            .map(Exec::from_handle)
            .map_err(|_| Error::Runtime("no ambient tokio runtime to run on".into()))
    }

    /// Creates a multi-thread Tokio runtime with `worker_threads` workers,
    /// named `thread_name`, and returns a handle onto it beside the
    /// [`OwnedReactor`] that keeps it alive.
    ///
    /// This is the constructor for a library whose users have no reactor at
    /// all, which is most of a synchronous protocol library's audience: the
    /// library owns the reactor its sockets and timers need, and the caller
    /// keeps its own executor — or none. The reactor dies with the returned
    /// [`OwnedReactor`], so a library holds it beside every handle it hands
    /// out and drops it last.
    ///
    /// Fails when `worker_threads` is `0`, or when the OS refuses the
    /// threads.
    pub fn owned(worker_threads: usize, thread_name: &str) -> Result<(Exec, OwnedReactor), Error> {
        if worker_threads == 0 {
            return Err(Error::Runtime("worker_threads must be at least 1".into()));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(worker_threads)
            .thread_name(thread_name)
            .on_thread_start(block_signals)
            .build()
            .map_err(Error::Io)?;
        let exec = Exec::from_handle(runtime.handle().clone());
        Ok((exec, OwnedReactor(Some(runtime))))
    }

    /// Enters the runtime context, for the constructors that register a
    /// socket with the reactor as they are built — `quinn`'s endpoints,
    /// `tokio::net`'s listeners. Held around the constructor only, never
    /// across an await.
    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.handle.enter()
    }

    /// Spawns a task on the runtime. Works from any thread, with or without
    /// an ambient reactor — which is why a library holding an `Exec` never
    /// calls `tokio::spawn`.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// A timer on this runtime's wheel. The `Sleep` is created inside the
    /// runtime context, so the returned future may be awaited anywhere.
    ///
    /// Every clock a protocol has is one of these: a reconnect interval, a
    /// handshake deadline, a heartbeat, a connect timeout, a send or receive
    /// timeout, a linger budget.
    pub fn sleep(&self, duration: Duration) -> tokio::time::Sleep {
        let _guard = self.handle.enter();
        tokio::time::sleep(duration)
    }

    /// The runtime's clock: what `sleep` and `within` count against.
    ///
    /// Under a paused Tokio clock (`tokio::time::pause`, `start_paused`) this is the paused time,
    /// so a deadline or backoff measured with it ends when the timers say it does. Elsewhere it is
    /// `std::time::Instant::now()`.
    pub fn now(&self) -> std::time::Instant {
        let _guard = self.handle.enter();
        tokio::time::Instant::now().into_std()
    }

    /// Awaits `future`, giving up after `limit`.
    ///
    /// `None` means the future had not finished; it is dropped, so whatever
    /// it held is released. This is the only place an await is bounded on
    /// wall-clock time, for the same reason [`Exec::sleep`] lives here: the
    /// timer belongs to the runtime, not to the caller.
    ///
    /// Dropping the future ends the *wait* and nothing else: work it handed
    /// to a blocking thread — a name lookup ([`Exec::resolve`]) — runs on
    /// until the OS returns, so a connect timeout or handshake budget built
    /// on this bounds the caller's time, not that thread's.
    pub async fn within<F: Future>(&self, limit: Duration, future: F) -> Option<F::Output> {
        let deadline = self.sleep(limit);
        tokio::select! {
            output = future => Some(output),
            () = deadline => None,
        }
    }

    /// Resolves `host:port` through the **system** resolver.
    ///
    /// The convenience the competitor libraries use: a foreign-protocol client
    /// dials what its own configuration names, and none of them has a reason
    /// to let an application replace name resolution. weida's own dial path
    /// goes through [`crate::Resolver`] instead, because a `weida://`
    /// authority may name a set and what a set means is the deployment's
    /// decision (`docs/decisions/0020-cluster-and-discovery.md` §4.2).
    ///
    /// One implementation, two entry points: this delegates to
    /// [`crate::SystemResolver`].
    ///
    /// **A deadline around this call ends the wait, not the lookup.**
    /// `getaddrinfo` blocks a thread and cannot be cancelled, so a connect
    /// timeout or handshake budget that expires while this is pending drops
    /// the future and returns to the caller, while the lookup thread runs on
    /// until the OS answers. At most [`crate::MAX_OUTSTANDING_LOOKUPS`] such
    /// lookups exist at once; a call beyond that fails with
    /// [`Error::LimitExceeded`] instead of waiting for one.
    pub async fn resolve(
        &self,
        host: &str,
        port: u16,
        max_addresses: usize,
    ) -> Result<Vec<std::net::SocketAddr>, Error> {
        use crate::resolve::Resolver;
        crate::resolve::SystemResolver
            .resolve(self, host, Some(port), max_addresses)
            .await
    }
}

impl std::fmt::Debug for Exec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exec").finish_non_exhaustive()
    }
}

/// Keeps a Tokio runtime created by [`Exec::owned`] alive for as long as
/// whatever created it.
///
/// Hold it beside every handle onto that runtime and drop it last. Dropping
/// it shuts the runtime down **in the background** rather than blocking,
/// because the last owner may go out of scope on one of that runtime's own
/// worker threads, where dropping a Tokio runtime panics.
pub struct OwnedReactor(Option<tokio::runtime::Runtime>);

impl Drop for OwnedReactor {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            // The last owner may go out of scope on one of this runtime's own
            // worker threads. Dropping a Tokio runtime there panics; shutting
            // it down in the background does not.
            runtime.shutdown_background();
        }
    }
}

impl std::fmt::Debug for OwnedReactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedReactor").finish_non_exhaustive()
    }
}

/// Blocks every asynchronous signal on the calling thread, which is an owned
/// reactor's worker or blocking-pool thread.
///
/// A process-directed signal goes to any thread that does not block it. If a
/// reactor thread took it, a caller blocked in `poll` on a readiness
/// descriptor would never see `EINTR` and Ctrl-C would not end its wait; with
/// the reactor's threads masked, as libzmq masks its I/O threads, the signal
/// lands on a caller thread ([decisions/0045], R12). The synchronous faults
/// (SIGSEGV, SIGBUS, SIGILL, SIGFPE) stay deliverable, because they are
/// raised on the thread that faulted and blocking them is undefined.
///
/// A failure leaves the default mask, which only costs `EINTR` delivery.
///
/// [decisions/0045]: ../../../docs/decisions/0045-the-c-abi-is-the-boundary.md
#[cfg(unix)]
fn block_signals() {
    use nix::sys::signal::{SigSet, SigmaskHow, Signal, pthread_sigmask};
    let mut set = SigSet::all();
    for fault in [
        Signal::SIGSEGV,
        Signal::SIGBUS,
        Signal::SIGILL,
        Signal::SIGFPE,
    ] {
        set.remove(fault);
    }
    let _ = pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&set), None);
}

#[cfg(not(unix))]
fn block_signals() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exec_without_an_ambient_reactor_fails() {
        let err = Exec::current().unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    #[test]
    fn zero_worker_threads_is_rejected() {
        let err = Exec::owned(0, "test").unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    /// The whole point of `owned`: no ambient reactor, on this thread or any
    /// other, and the library's tasks still run — driven here by an executor
    /// that is not Tokio at all.
    #[test]
    fn an_owned_reactor_needs_no_ambient_one() {
        assert!(Handle::try_current().is_err());
        let (exec, reactor) = Exec::owned(1, "test").expect("owned reactor");
        let joined = exec.spawn(async { 7u8 });
        assert_eq!(futures::executor::block_on(joined).expect("task"), 7);
        drop(reactor);
    }

    /// R12 of the C ABI: a process signal must reach a caller thread, so no
    /// thread of an owned reactor accepts one, while a fault stays deliverable.
    #[cfg(unix)]
    #[test]
    fn an_owned_reactors_threads_block_signals() {
        use nix::sys::signal::{SigSet, SigmaskHow, Signal, pthread_sigmask};
        let (exec, reactor) = Exec::owned(1, "test").expect("owned reactor");
        let mask = exec.spawn(async {
            let mut old = SigSet::empty();
            pthread_sigmask(SigmaskHow::SIG_BLOCK, None, Some(&mut old)).expect("read the mask");
            old
        });
        let mask = futures::executor::block_on(mask).expect("task");
        assert!(mask.contains(Signal::SIGINT));
        assert!(mask.contains(Signal::SIGTERM));
        assert!(!mask.contains(Signal::SIGSEGV));
        drop(reactor);
    }

    #[tokio::test(start_paused = true)]
    async fn within_gives_up_on_a_future_that_never_finishes() {
        let exec = Exec::current().expect("ambient runtime");
        assert!(
            exec.within(Duration::from_millis(1), std::future::pending::<()>())
                .await
                .is_none()
        );
        assert_eq!(
            exec.within(Duration::from_secs(30), async { 3 }).await,
            Some(3)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn now_reads_the_runtime_clock() {
        let exec = Exec::current().expect("ambient runtime");
        let before = exec.now();
        tokio::time::advance(Duration::from_secs(3600)).await;
        let advanced = exec.now() - before;
        assert!(advanced >= Duration::from_secs(3600), "{advanced:?}");
        assert!(advanced < Duration::from_secs(3601), "{advanced:?}");
        // That an hour passed on `exec.now()` while the test ran in a blink is
        // the paused clock's own proof; a bound on the wall clock would add
        // nothing and would turn red on a loaded machine.
    }
}
