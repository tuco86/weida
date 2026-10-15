//! `weida_zmq.sync`: ZeroMQ for a Python program with no event loop.
//!
//! ```python
//! from weida_zmq import sync
//!
//! context = sync.Context()
//! server = sync.RepSocket(context)
//! server.bind("tcp://127.0.0.1:5555")
//! while True:
//!     request = server.recv()
//!     server.send(b"World")
//! ```
//!
//! # It is a facade over a facade, and implements nothing
//!
//! `weida-zmq`'s own `blocking` module is the synchronous ZeroMQ: one wrapper
//! per socket type over a context that owns its reactor, with
//! `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO`/`ZMQ_DONTWAIT` where libzmq puts them and
//! **no second implementation of any protocol behaviour**. This module is that
//! module with Python argument conversion around it. REQ's alternation, the
//! mute actions, the routing table and the timeouts are decided once, in the
//! asynchronous sockets, which is why the synchronous surface cannot disagree
//! with the asynchronous one
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4.4
//! item 3, [0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! # The GIL is released while blocked
//!
//! Every call here parks the calling thread until the operation finishes, and
//! it does so inside [`Python::detach`], so other Python threads run while one
//! is waiting in `recv`. A blocking binding that held the GIL would make a
//! second thread pointless, and the usual shape of a synchronous ZeroMQ
//! program — one thread per socket, which is libzmq's own thread rule — would
//! be a program that runs one socket at a time.
//!
//! The reactor belongs to the context, so no asyncio loop exists in such a
//! process and none is needed: `weida-runtime`'s promise that "the caller may
//! drive the returned futures on any executor" is what the facade underneath
//! is built on.
//!
//! # What ends a wait that has no deadline
//!
//! A `recv()` or a blocking `send()` with no timeout waits for as long as the
//! peer does. Two things end it, and neither needs the peer:
//!
//! - **Ctrl-C.** On the main thread the wait asks Python every 100 ms whether
//!   a signal handler has something to raise ([`interruptible`]), so
//!   `KeyboardInterrupt` arrives, and the message that had not come stays
//!   where it was. Any other thread cannot receive a signal and parks until
//!   the socket wakes it.
//! - **`close()` from another thread.** The thread that is parked gets
//!   `ENOTSOCK`, which is what a call on a closed socket gets anywhere, and
//!   `close()` returns once the socket is closed rather than after the next
//!   message.
//!
//! Every other call on a socket takes the same lock, so a `send()` made from a
//! second thread while the first is parked in `recv()` queues behind it, as it
//! does for two coroutines on the asyncio surface. A program that reads and
//! writes one socket from two threads at once wants two sockets, or the
//! asyncio surface's `split()`; `close()` is the only call that does not wait
//! its turn.

use std::pin::pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyModule;
use tokio::sync::Notify;
use weida_py_core::{duration, interruptible};
use weida_zmq::Context;
use weida_zmq::blocking::BlockingContext;

use crate::errors::raise;
use crate::values::{PyMultipart, PyPublished, PySent, message_from};

/// A context that owns its reactor, for a caller with no loop.
#[pyclass(frozen, name = "Context", module = "weida_zmq.sync")]
pub struct SyncContext {
    inner: BlockingContext,
}

impl SyncContext {
    /// The facade's context, as the socket constructors take it.
    pub fn inner(&self) -> &BlockingContext {
        &self.inner
    }
}

#[pymethods]
impl SyncContext {
    /// `Context.owned` under the hood: the reactor is the library's and the
    /// caller never sees it.
    #[new]
    #[pyo3(signature = (*, options=None, max_sockets=None, worker_threads=None, close_budget=None))]
    fn new(
        py: Python<'_>,
        options: Option<&crate::options::PyContextOptions>,
        max_sockets: Option<usize>,
        worker_threads: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<SyncContext> {
        let config =
            crate::context::configuration(py, options, max_sockets, worker_threads, close_budget)?;
        let inner = raise(py, BlockingContext::with_config(config))?;
        Ok(SyncContext { inner })
    }

    /// `ZMQ_MAX_SOCKETS`.
    #[getter]
    fn max_sockets(&self) -> usize {
        self.inner.context().config().max_sockets
    }

    /// `ZMQ_LINGER` as a finite budget, in seconds.
    #[getter]
    fn close_budget(&self) -> f64 {
        self.inner.context().config().close_budget.as_secs_f64()
    }

    /// Sockets currently open on this context.
    #[getter]
    fn socket_count(&self) -> usize {
        self.inner.context().socket_count()
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.sync.Context sockets={} max_sockets={}>",
            self.socket_count(),
            self.max_sockets()
        )
    }
}

/// A timeout in seconds, or `None` for `ZMQ_SNDTIMEO`/`ZMQ_RCVTIMEO`.
fn limit(py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Duration>> {
    match timeout {
        None => Ok(None),
        Some(seconds) => duration("a timeout", seconds).map(Some).map_err(|bad| {
            crate::errors::to_py(py, &weida_py_core::Errno::new("EINVAL", format!("{bad}")))
        }),
    }
}

/// How `close()` reaches a call that is parked in the socket.
///
/// The parked call holds the socket's lock for as long as it waits, and
/// `close()` needs that lock, so a flag that every waiting call also listens
/// to is the only way for `close()` to end the wait rather than queue behind
/// it.
struct Closing {
    /// Set by [`begin`](Closing::begin) and never cleared: a closed socket
    /// stays closed.
    closed: AtomicBool,
    /// Wakes the calls that are parked when `close()` begins.
    parked: Notify,
}

impl Closing {
    fn new() -> Closing {
        Closing {
            closed: AtomicBool::new(false),
            parked: Notify::new(),
        }
    }

    /// `close()` has been called: ends the waits that are parked and refuses
    /// the ones that start.
    fn begin(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.parked.notify_waiters();
    }

    /// Waits for `operation`, or ends with `ENOTSOCK` as soon as `close()`
    /// begins, dropping the operation where it stands.
    ///
    /// Dropping is what cancelling a coroutine does to the same futures on the
    /// asyncio surface, so every future that surface is cancelled in may be
    /// dropped here: a receive leaves its message queued.
    async fn until_closed<T>(
        &self,
        operation: impl Future<Output = weida_zmq::Result<T>>,
    ) -> weida_zmq::Result<T> {
        let mut woken = pin!(self.parked.notified());
        // Registered before the flag is read, so that a `begin` between the
        // two wakes this wait instead of slipping past it.
        woken.as_mut().enable();
        if self.closed.load(Ordering::SeqCst) {
            return Err(closed());
        }
        tokio::select! {
            biased;
            () = woken => Err(closed()),
            outcome = operation => outcome,
        }
    }
}

fn closed() -> weida_zmq::Error {
    weida_zmq::Error::ENOTSOCK("the socket was closed while this call was waiting".into())
}

/// One synchronous socket class: the endpoint surface, plus its pattern's.
macro_rules! sync_socket {
    ($class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal, [$($flag:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {}, [$($flag)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [recv $($rest:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Receives the next message, blocking until one arrives.
            ///
            /// Bounded by `ZMQ_RCVTIMEO`, or by `timeout` seconds when one is
            /// given. The GIL is released while waiting. Ctrl-C ends the wait
            /// on the main thread, and `close()` from another thread ends it
            /// with `ENOTSOCK`.
            #[pyo3(signature = (*, timeout=None))]
            fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<PyMultipart> {
                let limit = limit(py, timeout)?;
                let received = self.wait(py, async move |socket| match limit {
                    Some(limit) => socket.recv_timeout(limit).await,
                    None => socket.recv().await,
                })?;
                raise(py, received).map(PyMultipart::of)
            }

            /// `ZMQ_DONTWAIT`: the message if one is queued, `EAGAIN` if not.
            fn recv_nowait(&self, py: Python<'_>) -> PyResult<PyMultipart> {
                let received = py.detach(|| self.lock().try_recv());
                raise(py, received).map(PyMultipart::of)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [send $($rest:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Sends a message, blocking until it is queued.
            ///
            /// Bounded by `ZMQ_SNDTIMEO`, or by `timeout` seconds when one is
            /// given. The GIL is released while waiting. Ctrl-C ends the wait
            /// on the main thread, and `close()` from another thread ends it
            /// with `ENOTSOCK`.
            #[pyo3(signature = (message, *, timeout=None))]
            fn send(
                &self,
                py: Python<'_>,
                message: &Bound<'_, PyAny>,
                timeout: Option<f64>,
            ) -> PyResult<()> {
                let message = message_from(message)?;
                let limit = limit(py, timeout)?;
                let sent = self.wait(py, async move |socket| match limit {
                    Some(limit) => socket.send_timeout(message, limit).await,
                    None => socket.send(message).await,
                })?;
                raise(py, sent)
            }

            /// `ZMQ_DONTWAIT`: queues the message or reports `EAGAIN`.
            fn send_nowait(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<()> {
                let message = message_from(message)?;
                let sent = py.detach(|| self.lock().try_send(message));
                raise(py, sent)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [report $($rest:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Sends a message and reports what became of it: this socket
            /// type drops at the high-water mark rather than blocking, and
            /// waits only where `ZMQ_ROUTER_MANDATORY` says to — a wait that
            /// ends as `recv`'s does.
            fn send(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<PySent> {
                let message = message_from(message)?;
                let sent = self.wait(py, async move |socket| socket.send(message).await)?;
                raise(py, sent).map(PySent::of)
            }

        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [report_now $($rest:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// `ZMQ_DONTWAIT`, with the same report. A REP socket has no such
            /// form, here or in the asynchronous surface: its reply is
            /// delivered to the peer that asked or discarded because that
            /// peer is gone, so it never waits for room.
            fn send_nowait(
                &self,
                py: Python<'_>,
                message: &Bound<'_, PyAny>,
            ) -> PyResult<PySent> {
                let message = message_from(message)?;
                let sent = py.detach(|| self.lock().try_send(message));
                raise(py, sent).map(PySent::of)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [publish $($rest:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Publishes a message to every matching subscriber. A publisher
            /// never blocks, so there is nothing to bound.
            fn send(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<PyPublished> {
                let message = message_from(message)?;
                Ok(PyPublished::of(py.detach(|| self.lock().publish(message))))
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [upstream $($rest:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Sends upstream: a message, or a subscription in libzmq's
            /// `1`/`0` form, which is what an XSUB's send is. Never blocks.
            fn send(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<PyPublished> {
                let message = message_from(message)?;
                Ok(PyPublished::of(py.detach(|| self.lock().send(message))))
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [subscribe $($rest:ident)*]) => {
        sync_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Subscribes to a prefix. An empty prefix matches everything.
            ///
            /// Synchronous in the asynchronous surface too, so this is the
            /// asynchronous socket's own call reached through the facade.
            fn subscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
                let prefix = weida_py_core::payload_of(prefix)?;
                let done = py.detach(|| self.lock().socket().subscribe(&prefix));
                raise(py, done)
            }

            /// Cancels one subscription to a prefix.
            fn unsubscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
                let prefix = weida_py_core::payload_of(prefix)?;
                let done = py.detach(|| self.lock().socket().unsubscribe(&prefix));
                raise(py, done)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($pattern:tt)*}, []) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_zmq.sync")]
        pub struct $class {
            /// A `Mutex` and not a lease: a blocking socket is used by one
            /// thread at a time, which is libzmq's own rule, and the GIL is
            /// released before the lock is taken so a second Python thread
            /// runs while this one waits.
            socket: Mutex<weida_zmq::blocking::$rust>,
            /// The reactor's clock and its runtime context, which a wait
            /// that arms a timer needs.
            #[allow(dead_code, reason = "a PUB never waits, so only the classes that \
                                          receive or block read it")]
            context: Context,
            /// How `close()` reaches a wait that holds the lock.
            closing: Closing,
        }

        impl $class {
            fn lock(&self) -> std::sync::MutexGuard<'_, weida_zmq::blocking::$rust> {
                self.socket
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
            }

            /// Runs a call that may wait on a peer, with the GIL released.
            ///
            /// The wait ends with Python's signal handlers on the main thread
            /// and with `close()` on any thread; see the module
            /// documentation. Both drop the operation where it stands, which
            /// is the cancellation the asyncio surface gives the same
            /// futures.
            // The guard is held across the wait on purpose: the operation
            // borrows the socket for as long as it runs, the future is polled
            // on this thread only, and a second call must queue behind it.
            #[allow(clippy::await_holding_lock)]
            #[allow(dead_code, reason = "a PUB never waits, so only the classes that \
                                          receive or block call it")]
            fn wait<T: Send>(
                &self,
                py: Python<'_>,
                operation: impl AsyncFnOnce(&mut weida_zmq::$rust) -> weida_zmq::Result<T> + Send,
            ) -> PyResult<weida_zmq::Result<T>> {
                interruptible(py, self.context.exec(), || async {
                    let mut socket = self.lock();
                    self.closing.until_closed(operation(socket.socket())).await
                })
            }
        }

        #[pymethods]
        impl $class {
            /// Opens the socket on `context`, with `options` if any are given.
            #[new]
            #[pyo3(signature = (context, options=None))]
            fn new(
                py: Python<'_>,
                context: &SyncContext,
                options: Option<&crate::options::PySocketOptions>,
            ) -> PyResult<$class> {
                let configured = options.map(|o| o.library()).unwrap_or_default();
                let socket = raise(
                    py,
                    weida_zmq::blocking::$rust::with_options(context.inner(), configured),
                )?;
                let socket = $class {
                    socket: Mutex::new(socket),
                    context: context.inner().context().clone(),
                    closing: Closing::new(),
                };
                if let Some(options) = options {
                    let applied = py.detach(|| {
                        crate::ops::apply_subscriptions(
                            socket.lock().socket(),
                            &options.subscriptions,
                        )
                    });
                    raise(py, applied)?;
                }
                Ok(socket)
            }

            /// The socket type this socket announces in its ZMTP `READY`.
            #[getter]
            fn socket_type(&self) -> &'static str {
                $kind
            }

            /// Binds an endpoint and returns the one actually bound.
            fn bind(&self, py: Python<'_>, endpoint: &str) -> PyResult<String> {
                let bound = py.detach(|| self.lock().bind(endpoint));
                raise(py, bound).map(|endpoint| endpoint.to_string())
            }

            /// Connects an endpoint, dialling behind it.
            fn connect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
                let done = py.detach(|| self.lock().connect(endpoint));
                raise(py, done)
            }

            /// Stops accepting on an endpoint.
            fn unbind(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
                let done = py.detach(|| self.lock().unbind(endpoint));
                raise(py, done)
            }

            /// Disconnects an endpoint and reports what its queues held.
            fn disconnect(
                &self,
                py: Python<'_>,
                endpoint: &str,
            ) -> PyResult<crate::sockets::Discarded> {
                let discarded = py.detach(|| self.lock().disconnect(endpoint));
                raise(py, discarded).map(|discarded| crate::sockets::Discarded {
                    outgoing: discarded.outgoing,
                    incoming: discarded.incoming,
                })
            }

            /// `ZMQ_LAST_ENDPOINT`.
            fn last_endpoint(&self, py: Python<'_>) -> Option<String> {
                py.detach(|| {
                    self.lock()
                        .last_endpoint()
                        .map(|endpoint| endpoint.to_string())
                })
            }

            /// `zmq_close`.
            ///
            /// Ends a `recv()` or `send()` that another thread is parked in,
            /// which raises `ENOTSOCK` there, and returns once the socket is
            /// closed.
            fn close(&self, py: Python<'_>) {
                self.closing.begin();
                py.detach(|| self.lock().close());
            }

            fn __repr__(&self) -> String {
                concat!("<weida_zmq.sync.", $python, " ", $kind, ">").to_owned()
            }

            $($pattern)*
        }
    };
}

sync_socket!(
    SyncReqSocket,
    ReqSocket,
    "ReqSocket",
    "REQ",
    "A REQ socket: one request out, one reply in, in that order.",
    [send recv]
);
sync_socket!(
    SyncRepSocket,
    RepSocket,
    "RepSocket",
    "REP",
    "A REP socket: one request in, one reply out.",
    [report recv]
);
sync_socket!(
    SyncDealerSocket,
    DealerSocket,
    "DealerSocket",
    "DEALER",
    "A DEALER socket: request-reply without the lockstep.",
    [send recv]
);
sync_socket!(
    SyncRouterSocket,
    RouterSocket,
    "RouterSocket",
    "ROUTER",
    "A ROUTER socket: the routing id is the first frame.",
    [report report_now recv]
);
sync_socket!(
    SyncPubSocket,
    PubSocket,
    "PubSocket",
    "PUB",
    "A PUB socket: publisher-side prefix matching, dropping at the high-water mark.",
    [publish]
);
sync_socket!(
    SyncSubSocket,
    SubSocket,
    "SubSocket",
    "SUB",
    "A SUB socket: nothing arrives until something is subscribed to.",
    [recv subscribe]
);
sync_socket!(
    SyncXPubSocket,
    XPubSocket,
    "XPubSocket",
    "XPUB",
    "An XPUB socket: PUB, with its subscribers' subscriptions delivered to the application.",
    [publish recv]
);
sync_socket!(
    SyncXSubSocket,
    XSubSocket,
    "XSubSocket",
    "XSUB",
    "An XSUB socket: SUB, with subscriptions sent upstream.",
    [upstream recv subscribe]
);
sync_socket!(
    SyncPushSocket,
    PushSocket,
    "PushSocket",
    "PUSH",
    "A PUSH socket: round-robin over the workers with room.",
    [send]
);
sync_socket!(
    SyncPullSocket,
    PullSocket,
    "PullSocket",
    "PULL",
    "A PULL socket: the fair-queued sink of a pipeline.",
    [recv]
);
sync_socket!(
    SyncPairSocket,
    PairSocket,
    "PairSocket",
    "PAIR",
    "A PAIR socket: exactly one peer.",
    [send recv]
);

/// Builds `weida_zmq.sync` and registers it so that `import weida_zmq.sync`
/// works as well as `from weida_zmq import sync`.
pub fn install(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent.py();
    let sync = PyModule::new(py, "sync")?;
    sync.add_class::<SyncContext>()?;
    sync.add_class::<SyncReqSocket>()?;
    sync.add_class::<SyncRepSocket>()?;
    sync.add_class::<SyncDealerSocket>()?;
    sync.add_class::<SyncRouterSocket>()?;
    sync.add_class::<SyncPubSocket>()?;
    sync.add_class::<SyncSubSocket>()?;
    sync.add_class::<SyncXPubSocket>()?;
    sync.add_class::<SyncXSubSocket>()?;
    sync.add_class::<SyncPushSocket>()?;
    sync.add_class::<SyncPullSocket>()?;
    sync.add_class::<SyncPairSocket>()?;
    parent.add("sync", &sync)?;
    // A submodule built in Rust is an attribute of its parent but not an
    // entry in `sys.modules`, and `import weida_zmq.sync` reads the latter.
    py.import("sys")?
        .getattr("modules")?
        .set_item("weida_zmq.sync", &sync)?;
    Ok(())
}
