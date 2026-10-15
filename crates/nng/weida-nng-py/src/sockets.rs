//! The eleven SP protocols, one Python class each.
//!
//! NNG has one `nng_socket` handle and a constructor per protocol; this
//! module has eleven classes, because the patterns are not interchangeable
//! and a type error at construction beats `NNG_ENOTSUP` at the first send.
//! It is the shape `weida-nng` already has in Rust, carried across
//! unchanged: a PUB has no `recv` here because a PUB cannot receive, rather
//! than having one that always fails.
//!
//! # Concurrency
//!
//! A `weida-nng` socket's `send` and `recv` take `&self` and the socket is
//! `Sync`, so two coroutines may use one socket at the same time and a
//! receive parked on an empty socket does not block a send. That is the
//! difference from this workspace's ZeroMQ binding, where libzmq's thread
//! rule is a `&mut self` and every call leases the socket; SP has no such
//! rule, and the protocols that need one transaction at a time — REQ, REP,
//! SURVEYOR, RESPONDENT — enforce it per **context** with `NNG_ESTATE`,
//! which is the protocol's own answer and is what `Context` objects are
//! for.
//!
//! # What is not here
//!
//! Raw sockets and `nng_device`. Raw mode hands the protocol header to the
//! application, and a Python surface for it would be a byte-slicing API
//! over a header this binding's classes deliberately keep — the library has
//! both in Rust for the programs that build forwarders. What is missing is
//! named here rather than half-offered.

use std::sync::Arc;

use pyo3::prelude::*;
use weida_py_core::Bridge;

use crate::context::Context;
use crate::errors::errno_of;
use crate::values::PyBroadcast;

/// Builds one socket class: the endpoint surface every socket has, plus
/// whichever operations its protocol permits.
///
/// The operations are named as flags and accumulated into **one**
/// `#[pymethods]` block, because a second one needs PyO3's
/// `multiple-pymethods` feature and its inventory registration, which an
/// `abi3` wheel should not pay for.
macro_rules! python_socket {
    ($class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal, [$($flag:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {}, [$($flag)*]);
    };

    // An awaiting send: the protocols that wait for room rather than
    // discard.
    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [send $($rest:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Sends one message, whole.
            ///
            /// Bounded by `NNG_OPT_SENDTIMEO`; the expiry is `ETIMEDOUT`
            /// and the message is still the caller's. A cancelled task
            /// leaves the socket usable.
            fn send<'py>(
                &self,
                py: Python<'py>,
                body: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let body = weida_py_core::payload_of(body)?;
                let socket = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    socket.send(body).await.map_err(errno_of)
                })
            }
        }, [$($rest)*]);
    };

    // The non-blocking form of that send.
    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [try_send $($rest:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Queues the message if a peer can take it now, and reports
            /// `ETIMEDOUT` if none can — which is what NNG's own
            /// non-blocking send says when there is no eligible peer.
            fn send_nowait(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<()> {
                let body = weida_py_core::payload_of(body)?;
                crate::errors::raise(py, self.socket.try_send(body))
            }
        }, [$($rest)*]);
    };

    // A broadcast: never waits, and reports what became of the copies.
    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [broadcast $($rest:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Sends one copy to every connected peer and reports what
            /// became of them.
            ///
            /// This protocol is best effort: a peer whose queue is full
            /// has its copy discarded and the send still succeeds, so the
            /// answer is a `Broadcast` counting both — the only place that
            /// number exists.
            fn send<'py>(
                &self,
                py: Python<'py>,
                body: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let body = weida_py_core::payload_of(body)?;
                let socket = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    socket
                        .send(body)
                        .map(PyBroadcast::of)
                        .map_err(errno_of)
                })
            }

            /// The same thing without the coroutine, since a broadcast
            /// never waits.
            fn send_nowait(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<PyBroadcast> {
                let body = weida_py_core::payload_of(body)?;
                crate::errors::raise(py, self.socket.send(body)).map(PyBroadcast::of)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [recv $($rest:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Receives the next message body, whole.
            ///
            /// Bounded by `NNG_OPT_RECVTIMEO`; the expiry is `ETIMEDOUT`.
            /// A cancelled task leaves the socket usable and the message
            /// unread.
            fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let socket = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    socket
                        .recv()
                        .await
                        .map(crate::values::Body)
                        .map_err(errno_of)
                })
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [try_recv $($rest:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// The message if one is queued, `EAGAIN` if none is.
            fn recv_nowait(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyBytes>> {
                let message = crate::errors::raise(py, self.socket.try_recv())?;
                Ok(weida_py_core::py_bytes(py, message.body()).unbind())
            }
        }, [$($rest)*]);
    };

    // The subscription surface, which only SUB has.
    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [subscribe $($rest:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Subscribes to a byte prefix. The empty prefix admits
            /// everything.
            ///
            /// The match happens **here**, at the receiver, which is where
            /// SP puts it: a publisher sends every publication to every
            /// subscriber and this socket drops what it did not ask for.
            fn subscribe(&self, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
                self.socket.subscribe(weida_py_core::payload_of(prefix)?);
                Ok(())
            }

            /// Cancels one subscription. `ENOENT` if it was never made.
            fn unsubscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
                let prefix = weida_py_core::payload_of(prefix)?;
                crate::errors::raise(py, self.socket.unsubscribe(prefix))
            }

            /// Every prefix this socket currently holds.
            #[getter]
            fn subscriptions(&self, py: Python<'_>) -> Vec<Py<pyo3::types::PyBytes>> {
                self.socket
                    .subscriptions()
                    .iter()
                    .map(|prefix| weida_py_core::py_bytes(py, prefix).unbind())
                    .collect()
            }

            /// Publications this socket threw away: filtered out, or
            /// dropped because its queue was full.
            #[getter]
            fn discarded(&self) -> u64 {
                self.socket.discarded()
            }
        }, [$($rest)*]);
    };

    // `nng_ctx_open`: a transaction of this socket's own, for the four
    // protocols that have one.
    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($acc:tt)*}, [context($context:ident) $($rest:tt)*]) => {
        python_socket!(@build $class, $rust, $python, $protocol, $what, {
            $($acc)*

            /// Opens a transaction of its own on this socket.
            ///
            /// The socket's bare `send`/`recv` are one implicit context,
            /// as NNG's are; this is how a caller gets a second, a tenth,
            /// and the parallelism they are for. Dropping it is
            /// `nng_ctx_close`.
            fn context(&self) -> crate::contexts::$context {
                crate::contexts::$context::wrap(self.socket.context(), self.bridge.clone())
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $protocol:literal, $what:literal,
     {$($pattern:tt)*}, []) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_nng")]
        pub struct $class {
            socket: Arc<weida_nng::$rust>,
            bridge: Bridge,
        }

        impl $class {
            /// The socket itself, for the classes built on one — a context
            /// is opened from its socket.
            #[allow(dead_code)]
            pub(crate) fn socket(&self) -> &Arc<weida_nng::$rust> {
                &self.socket
            }

            /// The reactor and errno mapping this socket was opened with.
            #[allow(dead_code)]
            pub(crate) fn bridge(&self) -> &Bridge {
                &self.bridge
            }
        }

        #[pymethods]
        impl $class {
            /// Opens the socket on `context`, against its ceiling, with
            /// `options` if any are given.
            ///
            /// Every option is honoured or refused **here**, where it is
            /// configured: a `send_depth` on a protocol that has no send
            /// queue fails at this line rather than silently doing
            /// nothing.
            #[new]
            #[pyo3(signature = (context, options=None))]
            fn new(
                py: Python<'_>,
                context: &Context,
                options: Option<&crate::options::PySocketOptions>,
            ) -> PyResult<$class> {
                let configured = options
                    .map(|o| o.library())
                    .unwrap_or_default();
                let socket = crate::errors::raise(
                    py,
                    weida_nng::$rust::with_options(context.inner(), configured),
                )?;
                let socket = Arc::new(socket);
                if let Some(options) = options {
                    crate::errors::raise(
                        py,
                        crate::subscriptions::Subscribes::apply_prefixes(
                            socket.as_ref(),
                            &options.subscriptions,
                        ),
                    )?;
                }
                Ok($class {
                    socket,
                    bridge: context.bridge().clone(),
                })
            }

            /// The SP protocol this socket speaks, as NNG names it.
            #[getter]
            fn protocol(&self) -> &'static str {
                $protocol
            }

            /// Dials `url` and returns once the peer's protocol header has
            /// arrived, which is what NNG's synchronous `nng_dial` does.
            ///
            /// Unlike NNG's, the wait costs no thread and is bounded by
            /// the socket's handshake timeout.
            fn dial<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
                let socket = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    socket
                        .dial(&url)
                        .await
                        .map(|dialer| dialer.url().to_string())
                        .map_err(errno_of)
                })
            }

            /// `NNG_FLAG_NONBLOCK`: creates the dialer, returns at once,
            /// and retries in the background with the reconnect backoff.
            fn dial_nowait(&self, py: Python<'_>, url: &str) -> PyResult<String> {
                let dialer = crate::errors::raise(py, self.socket.dial_nonblocking(url))?;
                Ok(dialer.url().to_string())
            }

            /// Listens on `url` and returns the endpoint actually bound,
            /// which is the only way to learn a wildcard port.
            fn listen<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
                let socket = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    socket
                        .listen(&url)
                        .await
                        .map(|listener| listener.url().to_string())
                        .map_err(errno_of)
                })
            }

            /// Pipes this socket can talk to right now.
            #[getter]
            fn pipe_count(&self) -> usize {
                self.socket.pipe_count()
            }

            /// `nng_close`: stops dialling and accepting, closes every
            /// pipe and destroys their queues. Idempotent.
            fn close(&self) {
                self.socket.close();
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!("<weida_nng.", $python, " ", $protocol, " pipes={}>"),
                    self.socket.pipe_count()
                )
            }

            $($pattern)*
        }
    };
}

python_socket!(
    PyReqSocket,
    ReqSocket,
    "ReqSocket",
    "req",
    "A REQ socket: send a request, then receive its reply.\n\nThe socket's own \
     `send`/`recv` are one implicit context, as NNG's are. `context()` opens \
     more, and several contexts on one socket run their transactions in \
     parallel.",
    [send recv context(PyReqContext)]
);

python_socket!(
    PyRepSocket,
    RepSocket,
    "RepSocket",
    "rep",
    "A REP socket: receive a request, then answer it.\n\nThe tag stack of the \
     request is remembered and written back in front of the reply, which is \
     how an answer finds its way home through any number of devices.",
    [recv send context(PyReplyContext)]
);

python_socket!(
    PyPushSocket,
    PushSocket,
    "PushSocket",
    "push",
    "A PUSH socket: send only, round-robin over the peers that can take a \
     message now.\n\nA PUSH never discards: with no eligible peer the send \
     waits and then reports `ETIMEDOUT`.",
    [send try_send]
);

python_socket!(
    PyPullSocket,
    PullSocket,
    "PullSocket",
    "pull",
    "A PULL socket: receive only, fair-queued over its pipes.",
    [recv try_recv]
);

python_socket!(
    PyPubSocket,
    PubSocket,
    "PubSocket",
    "pub",
    "A PUB socket: send only, one copy to every subscriber.\n\nThere is no \
     filtering here — SP filters at the receiver — so every subscriber gets \
     every publication and drops what it did not ask for.",
    [broadcast]
);

python_socket!(
    PySubSocket,
    SubSocket,
    "SubSocket",
    "sub",
    "A SUB socket: receive only, and only what matches a subscription.",
    [recv try_recv subscribe]
);

python_socket!(
    PyPair0Socket,
    Pair0Socket,
    "Pair0Socket",
    "pair0",
    "A PAIR v0 socket: one peer at a time, in both directions.\n\nNo protocol \
     header at all, which is what makes v0 the legacy-compatible choice; a \
     second peer's connection is refused while one is live.",
    [send try_send recv try_recv]
);

python_socket!(
    PyPair1Socket,
    Pair1Socket,
    "Pair1Socket",
    "pair1",
    "A PAIR v1 socket: one peer at a time, with a hop count.\n\nThe 32-bit \
     hop-count word in front of the body is what makes a forwarding topology \
     safe: a message past `max_ttl` hops is dropped and the pipe is kept.",
    [send try_send recv]
);

python_socket!(
    PySurveyorSocket,
    SurveyorSocket,
    "SurveyorSocket",
    "surveyor",
    "A SURVEYOR socket: broadcast a survey, then collect answers until the \
     deadline.\n\nThe deadline starts at the send and runs for \
     `survey_time`; when it expires the collect reports `ETIMEDOUT`, which \
     is indistinguishable from silence because that is all SP gives a \
     surveyor.",
    [send recv context(PySurveyContext)]
);

python_socket!(
    PyRespondentSocket,
    RespondentSocket,
    "RespondentSocket",
    "respondent",
    "A RESPONDENT socket: receive a survey, then answer it — or do not, \
     which SP treats as absence rather than failure.",
    [recv send context(PyReplyContext)]
);

python_socket!(
    PyBusSocket,
    BusSocket,
    "BusSocket",
    "bus",
    "A BUS socket: one hop, best effort, to every directly connected \
     peer.\n\nBUS broadcasts one hop and no further, so a mesh must be fully \
     connected for every node to see a message.",
    [broadcast recv try_recv]
);
