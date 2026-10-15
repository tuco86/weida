//! `weida_nng.sync`: SP for a Python program with no event loop.
//!
//! ```python
//! from weida_nng import sync
//!
//! context = sync.Context()
//! server = sync.RepSocket(context)
//! server.listen("tcp://127.0.0.1:5555")
//! while True:
//!     request = server.recv()
//!     server.send(b"World")
//! ```
//!
//! # It is a facade over a facade, and implements nothing
//!
//! `weida-nng`'s own `blocking` module is the synchronous SP: one wrapper
//! per protocol over a context that owns its reactor, with
//! `NNG_OPT_SENDTIMEO`/`NNG_OPT_RECVTIMEO` where NNG puts them and **no
//! second implementation of any protocol behaviour**. This module is that
//! module with Python argument conversion around it. REQ's alternation and
//! its resend timer, the tag stacks, the survey deadline and the full-queue
//! actions are decided once, in the asynchronous sockets, which is why the
//! synchronous surface cannot disagree with the asynchronous one
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4.4
//! item 3, [0014](../../../../docs/decisions/0014-parallel-libraries.md)
//! §2).
//!
//! # The GIL is released while blocked
//!
//! Every call here parks the calling thread until the operation finishes,
//! and it does so inside [`Python::detach`], so other Python threads run
//! while one is waiting in `recv`. A blocking binding that held the GIL
//! would make a second thread pointless, and the usual shape of a
//! synchronous NNG program — one thread per socket, or one per context —
//! would be a program that runs one socket at a time.
//!
//! The reactor belongs to the context, so no asyncio loop exists in such a
//! process and none is needed.
//!
//! # A timeout is what makes a stall an exception
//!
//! `recv_timeout` and `send_timeout` are the socket's options and are
//! honoured by the asynchronous socket underneath. Without one, NNG's
//! default is to wait forever — which is a hang in a synchronous program,
//! so every example here sets them and the module documentation says why.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use weida_nng::blocking::BlockingContext;

use crate::errors::raise;
use crate::values::PyBroadcast;

/// A context that owns its reactor, for a caller with no loop.
#[pyclass(frozen, name = "Context", module = "weida_nng.sync")]
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
    /// `Context::owned` under the hood: the reactor is the library's and
    /// the caller never sees it.
    #[new]
    #[pyo3(signature = (*, max_sockets=None, worker_threads=None, close_budget=None))]
    fn new(
        py: Python<'_>,
        max_sockets: Option<usize>,
        worker_threads: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<SyncContext> {
        let mut config = weida_nng::ContextConfig::default();
        if let Some(max_sockets) = max_sockets {
            config.max_sockets = max_sockets;
        }
        if let Some(worker_threads) = worker_threads {
            config.worker_threads = worker_threads;
        }
        if let Some(seconds) = close_budget {
            config.close_budget = raise(
                py,
                crate::context::seconds_to_duration("close_budget", seconds),
            )?;
        }
        let inner = raise(py, BlockingContext::with_config(config))?;
        Ok(SyncContext { inner })
    }

    /// How many sockets this context may hold at once.
    #[getter]
    fn max_sockets(&self) -> usize {
        self.inner.context().config().max_sockets
    }

    /// Sockets currently open on this context.
    #[getter]
    fn socket_count(&self) -> usize {
        self.inner.context().socket_count()
    }

    /// Admits no further socket, then waits at most `close_budget` for the
    /// open ones. Returns how many were still open when it ran out.
    ///
    /// The GIL is released while waiting, as everywhere else here.
    fn shutdown(&self, py: Python<'_>) -> PyResult<usize> {
        let closed = weida_py_core::interruptible(py, self.inner.context().exec(), || {
            self.inner.context().clone().shutdown()
        })?;
        Ok(closed.outstanding())
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nng.sync.Context sockets={} max_sockets={}>",
            self.inner.context().socket_count(),
            self.inner.context().config().max_sockets
        )
    }
}

/// One synchronous socket class: the endpoint surface, plus its protocol's.
macro_rules! sync_socket {
    ($class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal, [$($flag:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {}, [$($flag)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [send $($rest:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Sends one message, blocking until it is queued.
            ///
            /// Bounded by the socket's `send_timeout`; the expiry is
            /// `ETIMEDOUT` rather than a thread that never returns. The GIL
            /// is released while waiting.
            fn send(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<()> {
                let body = weida_py_core::payload_of(body)?;
                let sent = weida_py_core::interruptible(py, self.blocking.context().exec(), || {
                    self.socket.socket().send(body)
                })?;
                raise(py, sent)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [try_send $($rest:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// `NNG_FLAG_NONBLOCK`: queues the message if a peer can take
            /// it now, and reports `ETIMEDOUT` if none can.
            fn send_nowait(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<()> {
                let body = weida_py_core::payload_of(body)?;
                raise(py, self.socket.try_send(body))
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [broadcast $($rest:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Sends one copy to every connected peer and reports what
            /// became of them. Never blocks.
            fn send(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<PyBroadcast> {
                let body = weida_py_core::payload_of(body)?;
                raise(py, self.socket.send(body)).map(PyBroadcast::of)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [recv $($rest:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Receives the next message body, blocking until one arrives.
            ///
            /// Bounded by the socket's `recv_timeout`; the expiry is
            /// `ETIMEDOUT`. The GIL is released while waiting.
            fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
                let received = weida_py_core::interruptible(py, self.blocking.context().exec(), || {
                    self.socket.socket().recv()
                })?;
                let message = raise(py, received)?;
                Ok(weida_py_core::py_bytes(py, message.body()))
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [try_recv $($rest:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// `NNG_FLAG_NONBLOCK`: the message if one is queued, `EAGAIN`
            /// if none is.
            fn recv_nowait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
                let message = raise(py, self.socket.try_recv())?;
                Ok(weida_py_core::py_bytes(py, message.body()))
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [subscribe $($rest:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Subscribes to a byte prefix. The empty prefix admits
            /// everything, and the match happens here, at the receiver.
            fn subscribe(&self, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
                self.socket.subscribe(weida_py_core::payload_of(prefix)?);
                Ok(())
            }

            /// Cancels one subscription. `ENOENT` if it was never made.
            fn unsubscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
                let prefix = weida_py_core::payload_of(prefix)?;
                raise(py, self.socket.socket().unsubscribe(prefix))
            }

            /// Publications this socket threw away.
            #[getter]
            fn discarded(&self) -> u64 {
                self.socket.socket().discarded()
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [context($context:ident) $($rest:tt)*]) => {
        sync_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// `nng_ctx_open()`: a transaction of its own on this socket,
            /// which is how a synchronous program runs two of them — one
            /// thread each.
            fn context(&self) -> $context {
                $context {
                context: self.socket.context_of(),
                blocking: self.blocking.clone(),
                }
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($pattern:tt)*}, []) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_nng.sync")]
        pub struct $class {
            socket: weida_nng::blocking::$rust,
            blocking: BlockingContext,
        }

        #[pymethods]
        impl $class {
            /// Opens the socket on `context`, with `options` if any are
            /// given. Options are refused here, as they are on the
            /// asynchronous socket.
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
                    weida_nng::blocking::$rust::with_options(context.inner(), configured),
                )?;
                if let Some(options) = options {
                    raise(
                        py,
                        crate::subscriptions::Subscribes::apply_prefixes(
                            socket.socket(),
                            &options.subscriptions,
                        ),
                    )?;
                }
                Ok($class {
                    socket,
                    blocking: context.inner().clone(),
                })
            }

            /// The SP protocol this socket speaks, as NNG names it.
            #[getter]
            fn protocol(&self) -> &'static str {
                $protocol
            }

            /// `nng_dial()`: blocks until the peer's protocol header has
            /// arrived, bounded by the handshake timeout.
            fn dial(&self, py: Python<'_>, url: &str) -> PyResult<String> {
                let dialled = weida_py_core::interruptible(py, self.blocking.context().exec(), || {
                    self.socket.socket().dial(url)
                })?;
                raise(py, dialled).map(|dialer| dialer.url().to_string())
            }

            /// `NNG_FLAG_NONBLOCK`: creates the dialer and returns at once.
            fn dial_nowait(&self, py: Python<'_>, url: &str) -> PyResult<String> {
                raise(py, self.socket.dial_nonblocking(url))
                    .map(|dialer| dialer.url().to_string())
            }

            /// `nng_listen()`: returns the endpoint actually bound.
            fn listen(&self, py: Python<'_>, url: &str) -> PyResult<String> {
                let listening = weida_py_core::interruptible(py, self.blocking.context().exec(), || {
                    self.socket.socket().listen(url)
                })?;
                raise(py, listening).map(|listener| listener.url().to_string())
            }

            /// Pipes this socket can talk to right now.
            #[getter]
            fn pipe_count(&self) -> usize {
                self.socket.pipe_count()
            }

            /// `nng_close()`. Idempotent.
            fn close(&self) {
                self.socket.close();
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!("<weida_nng.sync.", $python, " ", $protocol, " pipes={}>"),
                    self.socket.pipe_count()
                )
            }

            $($pattern)*
        }
    };
}

/// One synchronous context class.
macro_rules! sync_context {
    ($class:ident, $rust:ident, $python:literal, $what:literal) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_nng.sync")]
        pub struct $class {
            context: weida_nng::blocking::$rust,
            blocking: BlockingContext,
        }

        #[pymethods]
        impl $class {
            /// Sends this context's message, blocking under the socket's
            /// `send_timeout`.
            fn send(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<()> {
                let body = weida_py_core::payload_of(body)?;
                let sent =
                    weida_py_core::interruptible(py, self.blocking.context().exec(), || {
                        self.context.context().send(body)
                    })?;
                raise(py, sent)
            }

            /// Receives this context's message, blocking under the socket's
            /// `recv_timeout` and, for a surveyor, its own deadline.
            fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
                let received =
                    weida_py_core::interruptible(py, self.blocking.context().exec(), || {
                        self.context.context().recv()
                    })?;
                let message = raise(py, received)?;
                Ok(weida_py_core::py_bytes(py, message.body()))
            }

            fn __repr__(&self) -> String {
                concat!("<weida_nng.sync.", $python, ">").to_owned()
            }
        }
    };
}

sync_context!(
    SyncReqContext,
    ReqCtx,
    "ReqContext",
    "One REQ transaction, blocking."
);
sync_context!(
    SyncReplyContext,
    ReplierCtx,
    "ReplyContext",
    "One REP or RESPONDENT transaction, blocking."
);
sync_context!(
    SyncSurveyContext,
    SurveyorCtx,
    "SurveyContext",
    "One survey, blocking, with its own deadline."
);

sync_socket!(
    SyncReqSocket,
    ReqSocket,
    "ReqSocket",
    "req",
    "A REQ socket: send a request, then receive its reply.",
    [send recv context(SyncReqContext)]
);
sync_socket!(
    SyncRepSocket,
    RepSocket,
    "RepSocket",
    "rep",
    "A REP socket: receive a request, then answer it.",
    [recv send context(SyncReplyContext)]
);
sync_socket!(
    SyncPushSocket,
    PushSocket,
    "PushSocket",
    "push",
    "A PUSH socket: round-robin over the peers that can take a message.",
    [send try_send]
);
sync_socket!(
    SyncPullSocket,
    PullSocket,
    "PullSocket",
    "pull",
    "A PULL socket: the fair-queued sink of a pipeline.",
    [recv try_recv]
);
sync_socket!(
    SyncPubSocket,
    PubSocket,
    "PubSocket",
    "pub",
    "A PUB socket: one copy to every subscriber, best effort.",
    [broadcast]
);
sync_socket!(
    SyncSubSocket,
    SubSocket,
    "SubSocket",
    "sub",
    "A SUB socket: receive only what matches a subscription.",
    [recv try_recv subscribe]
);
sync_socket!(
    SyncPair0Socket,
    Pair0Socket,
    "Pair0Socket",
    "pair0",
    "A PAIR v0 socket: one peer at a time, no protocol header.",
    [send try_send recv try_recv]
);
sync_socket!(
    SyncPair1Socket,
    Pair1Socket,
    "Pair1Socket",
    "pair1",
    "A PAIR v1 socket: one peer at a time, with a hop count.",
    [send try_send recv]
);
sync_socket!(
    SyncSurveyorSocket,
    SurveyorSocket,
    "SurveyorSocket",
    "surveyor",
    "A SURVEYOR socket: broadcast, then collect until the deadline.",
    [send recv context(SyncSurveyContext)]
);
sync_socket!(
    SyncRespondentSocket,
    RespondentSocket,
    "RespondentSocket",
    "respondent",
    "A RESPONDENT socket: receive a survey, then answer it.",
    [recv send context(SyncReplyContext)]
);
sync_socket!(
    SyncBusSocket,
    BusSocket,
    "BusSocket",
    "bus",
    "A BUS socket: one hop, best effort, to every connected peer.",
    [broadcast recv try_recv]
);

/// Builds `weida_nng.sync` and registers it so that `import weida_nng.sync`
/// works as well as `from weida_nng import sync`.
pub fn install(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent.py();
    let sync = PyModule::new(py, "sync")?;
    sync.add_class::<SyncContext>()?;
    sync.add_class::<SyncReqContext>()?;
    sync.add_class::<SyncReplyContext>()?;
    sync.add_class::<SyncSurveyContext>()?;
    sync.add_class::<SyncReqSocket>()?;
    sync.add_class::<SyncRepSocket>()?;
    sync.add_class::<SyncPushSocket>()?;
    sync.add_class::<SyncPullSocket>()?;
    sync.add_class::<SyncPubSocket>()?;
    sync.add_class::<SyncSubSocket>()?;
    sync.add_class::<SyncPair0Socket>()?;
    sync.add_class::<SyncPair1Socket>()?;
    sync.add_class::<SyncSurveyorSocket>()?;
    sync.add_class::<SyncRespondentSocket>()?;
    sync.add_class::<SyncBusSocket>()?;
    parent.add("sync", &sync)?;
    // A submodule built in Rust is an attribute of its parent but not an
    // entry in `sys.modules`, and `import weida_nng.sync` reads the latter.
    py.import("sys")?
        .getattr("modules")?
        .set_item("weida_nng.sync", &sync)?;
    Ok(())
}
