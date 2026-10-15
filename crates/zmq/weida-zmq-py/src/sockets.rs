//! The eleven stable socket types, one Python class each.
//!
//! libzmq has one `Socket` type and a constant to say what it is; this module
//! has eleven classes, because the patterns are not interchangeable and a type
//! error at construction beats `ENOTSUP` at the first send. It is the shape
//! `weida-zmq` already has in Rust, carried across unchanged: a PUB has no
//! `recv` here because a PUB cannot receive, rather than having one that
//! always fails.
//!
//! Every class gets the endpoint surface every socket has, and then whichever
//! of [`ops`](crate::ops)'s four shapes its pattern is — which is why the
//! class list below reads as a table of capabilities and why no socket type
//! has an implementation of its own here.
//!
//! # One operation at a time, per socket object
//!
//! A `weida-zmq` socket handle is `Send` and **not** `Sync`: libzmq's
//! "applications MUST NOT use a socket from multiple threads" expressed as a
//! type. So each socket here lives in a
//! [`Retirable`](weida_py_core::Retirable) slot and every call leases it for
//! as long as the operation runs. Two coroutines
//! that use one socket therefore *queue*; they do not interleave and they do
//! not race.
//!
//! The consequence is worth stating plainly rather than discovering: a task
//! parked in `await sock.recv()` holds the socket, so a concurrent
//! `await sock.send(...)` on that same object waits for the receive to finish
//! or to be cancelled. `recv_nowait` and `send_nowait` report `EAGAIN`
//! instead of queueing, because they were told not to wait.
//!
//! Where the two directions of a socket type are independent, that is not the
//! only option: `await sock.split()` hands out a sending and a receiving half
//! onto the same connections, usable from two coroutines at once
//! ([`crate::halves`]). DEALER, ROUTER, PAIR, XPUB and XSUB have it; REQ and
//! REP do not, because 28/REQREP's alternation is one sequence. Otherwise:
//! two sockets, which is what the patterns are for, or one coroutine owning
//! the socket.
//!
//! # Why every awaiting call is a coroutine
//!
//! Even the calls `weida-zmq` answers synchronously (`connect`, `unbind`,
//! `last_endpoint`, `close`) are `await`ed here. They need the socket, the
//! socket may be held by a parked receive, and a *synchronous* Python method
//! that waited for it would block the event-loop thread itself — the one thing
//! an asyncio library must never do. Awaiting costs a coroutine; blocking the
//! loop costs the program. The two `_nowait` methods are the exception, and
//! they are exempt precisely because they never wait for anything.

use pyo3::prelude::*;
use weida_py_core::{Errno, Retirable};

use crate::context::Context;
use crate::errors::{errno_of, retired};
use crate::ops::lease;
use crate::values::{PyMultipart, PyPublished, PySent};

/// What a `disconnect` threw away, in messages.
///
/// libzmq's `zmq_disconnect` reports nothing at all, so a caller cannot tell a
/// clean disconnect from one that dropped a hundred queued messages. This is
/// that number, in both directions.
#[pyclass(frozen, get_all, name = "Discarded", module = "weida_zmq")]
pub struct Discarded {
    /// Messages queued for the peer and never sent.
    pub outgoing: usize,
    /// Messages received from the peer and never read.
    pub incoming: usize,
}

#[pymethods]
impl Discarded {
    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.Discarded outgoing={} incoming={}>",
            self.outgoing, self.incoming
        )
    }
}

/// The bridge a socket of this module was opened with.
///
/// [`crate::devices`]'s functions take sockets as `PyAny` — a device end is
/// any of the eleven classes — and still need a reactor to run on. Every
/// socket of one context carries the same bridge, so the first argument's is
/// the proxy's.
pub fn bridge_of(object: &Bound<'_, PyAny>) -> PyResult<weida_py_core::Bridge> {
    macro_rules! any_of {
        ($($class:ident),+ $(,)?) => {
            $(if let Ok(socket) = object.extract::<PyRef<'_, $class>>() {
                return Ok(socket.bridge().clone());
            })+
        };
    }
    any_of!(
        PyReqSocket,
        PyRepSocket,
        PyDealerSocket,
        PyRouterSocket,
        PyPubSocket,
        PySubSocket,
        PyXPubSocket,
        PyXSubSocket,
        PyPushSocket,
        PyPullSocket,
        PyPairSocket,
    );
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected a socket of this module",
    ))
}

/// Builds one socket class: the endpoint surface every socket type has, plus
/// the methods its pattern adds.
///
/// The pattern methods are named as flags — `recv`, `send`, `report`,
/// `publish`, `subscribe` — and accumulated into **one** `#[pymethods]` block,
/// because a second one needs PyO3's `multiple-pymethods` feature and its
/// inventory registration, which an `abi3` wheel should not pay for. Each flag
/// expands to a delegation into [`crate::ops`], so a pattern's behaviour is
/// written once for every socket type that has it.
macro_rules! python_socket {
    ($class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal, [$($flag:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {}, [$($flag)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [recv $($rest:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Receives the next message, whole.
            ///
            /// Bounded by `ZMQ_RCVTIMEO`, or by `timeout` seconds when one is
            /// given; either way the expiry is `EAGAIN`. A cancelled task
            /// leaves the socket usable and the message unread.
            #[pyo3(signature = (*, timeout=None))]
            fn recv<'py>(
                &self,
                py: Python<'py>,
                timeout: Option<f64>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::recv(py, &self.bridge, &self.socket, timeout)
            }

            /// `ZMQ_DONTWAIT`: the message if one is queued, `EAGAIN` if not.
            fn recv_nowait(&self, py: Python<'_>) -> PyResult<PyMultipart> {
                crate::ops::recv_nowait(py, &self.socket)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [send $($rest:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Sends a message: a `Multipart`, one `bytes` frame, or an
            /// iterable of frames. All frames or none.
            ///
            /// This socket type blocks rather than discarding, bounded by
            /// `ZMQ_SNDTIMEO` or by `timeout` seconds when one is given; the
            /// expiry is `EAGAIN` and the message is still the caller's.
            #[pyo3(signature = (message, *, timeout=None))]
            fn send<'py>(
                &self,
                py: Python<'py>,
                message: &Bound<'py, PyAny>,
                timeout: Option<f64>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::send(py, &self.bridge, &self.socket, message, timeout)
            }

            /// `ZMQ_DONTWAIT`: queues the message or reports `EAGAIN`.
            fn send_nowait(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<()> {
                crate::ops::send_nowait(py, &self.socket, message)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [report $($rest:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Sends a message and reports what became of it.
            ///
            /// This socket type's mute action is *drop*, so a full queue is a
            /// `Sent` saying `dropped` rather than an exception or a wait —
            /// libzmq returns success for both and this does not.
            fn send<'py>(
                &self,
                py: Python<'py>,
                message: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::send_reporting(py, &self.bridge, &self.socket, message)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [report_now $($rest:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// `ZMQ_DONTWAIT`, with the same report.
            fn send_nowait(
                &self,
                py: Python<'_>,
                message: &Bound<'_, PyAny>,
            ) -> PyResult<PySent> {
                crate::ops::send_reporting_nowait(py, &self.socket, message)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [publish $($rest:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Publishes a message to every subscriber whose subscription
            /// matches its first frame.
            ///
            /// A publisher never blocks: the answer is a `Published` counting
            /// the subscribers that took a copy and the ones whose queue was
            /// full, which is the only place that number exists.
            fn send<'py>(
                &self,
                py: Python<'py>,
                message: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::publish(py, &self.bridge, &self.socket, message)
            }

            /// The same thing without the coroutine, since a publish never
            /// waits. `EAGAIN` only if another coroutine holds the socket.
            fn send_nowait(
                &self,
                py: Python<'_>,
                message: &Bound<'_, PyAny>,
            ) -> PyResult<PyPublished> {
                crate::ops::publish_nowait(py, &self.socket, message)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [subscribe $($rest:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Subscribes to a prefix. An empty prefix matches everything.
            ///
            /// Subscriptions are additive and **not** idempotent, which is
            /// 29/PUBSUB's rule: two subscriptions to one prefix need two
            /// cancellations.
            fn subscribe<'py>(
                &self,
                py: Python<'py>,
                prefix: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let prefix = weida_py_core::payload_of(prefix)?;
                crate::ops::subscribe(py, &self.bridge, &self.socket, prefix)
            }

            /// Cancels one subscription to a prefix.
            fn unsubscribe<'py>(
                &self,
                py: Python<'py>,
                prefix: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let prefix = weida_py_core::payload_of(prefix)?;
                crate::ops::unsubscribe(py, &self.bridge, &self.socket, prefix)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($acc:tt)*}, [split $send:ident $recv:ident $($rest:ident)*]) => {
        python_socket!(@build $class, $rust, $python, $kind, $what, {
            $($acc)*

            /// Splits the socket into a sending half and a receiving half,
            /// usable **at the same time** from two coroutines.
            ///
            /// This socket type's two directions are independent, so a task
            /// parked in the receiving half's `recv` does not hold up the
            /// sending half's `send` — the limitation
            /// [`crate::sockets`] describes for a whole socket. The
            /// connections stay while either half lives.
            ///
            /// **This object is retired by the call**: every later method on
            /// it raises `ENOTSOCK` naming the split, because the socket now
            /// lives in its halves.
            fn split<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                let bridge = self.bridge.clone();
                self.bridge.awaitable(py, async move {
                    let socket = slot.retire().await.map_err(retired)?;
                    let (send, recv) = socket.split();
                    Ok::<_, Errno>((
                        crate::halves::$send::new(send, bridge.clone()),
                        crate::halves::$recv::new(recv, bridge),
                    ))
                })
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
     {$($pattern:tt)*}, []) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_zmq")]
        pub struct $class {
            socket: Retirable<weida_zmq::$rust>,
            bridge: weida_py_core::Bridge,
        }

        impl $class {
            /// This socket's slot, for the calls that need the socket itself
            /// rather than an operation on it — `serve_pair` takes a PAIR,
            /// which is why only one socket type's copy of this is used.
            #[allow(dead_code)]
            pub(crate) fn slot(&self) -> &Retirable<weida_zmq::$rust> {
                &self.socket
            }

            /// The reactor and errno mapping this socket was opened with.
            pub(crate) fn bridge(&self) -> &weida_py_core::Bridge {
                &self.bridge
            }

        }


        #[pymethods]
        impl $class {
            /// Opens the socket on `context`, against its `ZMQ_MAX_SOCKETS`
            /// ceiling, with `options` if any are given.
            ///
            /// Every option is honoured or refused **here**, where it is
            /// configured: a `ZMQ_SUBSCRIBE` set on a socket type that has no
            /// subscriptions fails at this line rather than silently doing
            /// nothing.
            #[new]
            #[pyo3(signature = (context, options=None))]
            fn new(
                py: Python<'_>,
                context: &Context,
                options: Option<&crate::options::PySocketOptions>,
            ) -> PyResult<$class> {
                let configured = options.map(|o| o.library()).unwrap_or_default();
                let mut socket = crate::errors::raise(
                    py,
                    weida_zmq::$rust::with_options(context.inner(), configured),
                )?;
                if let Some(options) = options {
                    crate::errors::raise(
                        py,
                        crate::ops::apply_subscriptions(&mut socket, &options.subscriptions),
                    )?;
                }
                Ok($class {
                    socket: Retirable::new(socket),
                    bridge: context.bridge().clone(),
                })
            }

            /// The socket type this socket announces in its ZMTP `READY`.
            #[getter]
            fn socket_type(&self) -> &'static str {
                $kind
            }

            /// Binds an endpoint and returns the one actually bound.
            ///
            /// `tcp://127.0.0.1:0` comes back with the port the kernel chose,
            /// which is the only way to learn a wildcard port — libzmq's
            /// `ZMQ_LAST_ENDPOINT`.
            fn bind<'py>(&self, py: Python<'py>, endpoint: String) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    lease(&slot)
                        .await?
                        .bind(&endpoint)
                        .await
                        .map(|bound| bound.to_string())
                        .map_err(errno_of)
                })
            }

            /// Connects an endpoint. Returns as soon as the peer exists, like
            /// `zmq_connect`: the queue is there and the dialling happens
            /// behind it.
            fn connect<'py>(
                &self,
                py: Python<'py>,
                endpoint: String,
            ) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    lease(&slot).await?.connect(&endpoint).map_err(errno_of)
                })
            }

            /// Stops accepting on an endpoint. Peers already accepted there
            /// keep their connections.
            fn unbind<'py>(
                &self,
                py: Python<'py>,
                endpoint: String,
            ) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    lease(&slot).await?.unbind(&endpoint).map_err(errno_of)
                })
            }

            /// Disconnects an endpoint and reports what its queues held.
            fn disconnect<'py>(
                &self,
                py: Python<'py>,
                endpoint: String,
            ) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    lease(&slot)
                        .await?
                        .disconnect(&endpoint)
                        .map(|discarded| Discarded {
                            outgoing: discarded.outgoing,
                            incoming: discarded.incoming,
                        })
                        .map_err(errno_of)
                })
            }

            /// `ZMQ_LAST_ENDPOINT`: the last endpoint bound or connected, or
            /// `None` when there has been neither.
            fn last_endpoint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    Ok::<Option<String>, Errno>(
                        lease(&slot)
                            .await?
                            .last_endpoint()
                            .map(|endpoint| endpoint.to_string()),
                    )
                })
            }

            /// Peers this socket may send to right now.
            fn peer_count<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    Ok::<usize, Errno>(lease(&slot).await?.peer_count())
                })
            }

            /// Every connection this socket has, connected or not, as a
            /// snapshot: what the peer announced, what a ZAP handler said
            /// about it, and what the kernel says about a local one.
            fn peers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    Ok::<Vec<crate::identity::PyPeer>, Errno>(
                        lease(&slot)
                            .await?
                            .connections()
                            .iter()
                            .map(crate::identity::PyPeer::of)
                            .collect(),
                    )
                })
            }

            /// `zmq_socket_monitor`: this socket's connection lifecycle as an
            /// async iterator of typed events, replacing any monitor
            /// installed before.
            ///
            /// `events` is libzmq's bit mask of `ZMQ_EVENT_*`; every event by
            /// default.
            #[pyo3(signature = (events=None))]
            fn monitor<'py>(
                &self,
                py: Python<'py>,
                events: Option<u16>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                let bridge = self.bridge.clone();
                let events = crate::monitor::PyMonitor::events(events);
                self.bridge.awaitable(py, async move {
                    let installed = lease(&slot).await?.monitor(events);
                    Ok::<crate::monitor::PyMonitor, Errno>(crate::monitor::PyMonitor::new(
                        installed, bridge,
                    ))
                })
            }

            /// `zmq_close`: stops accepting and dialling, and destroys every
            /// queue. The socket's slot under `ZMQ_MAX_SOCKETS` is returned
            /// when Python collects the object.
            fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = self.socket.clone();
                self.bridge.awaitable(py, async move {
                    lease(&slot).await?.close();
                    Ok::<(), Errno>(())
                })
            }

            fn __repr__(&self) -> String {
                concat!("<weida_zmq.", $python, " ", $kind, ">").to_owned()
            }

            $($pattern)*
        }
    };
}

/// The socket types a device end may be, which is `weida_zmq::Device`'s own
/// list: REQ and REP are absent because a proxy between them would drive two
/// state machines that 28/REQREP requires the *application* to drive, and
/// libzmq's own devices do not accept them either.
macro_rules! device_ends {
    ($($class:ident),+ $(,)?) => {
        $(impl $class {
            /// A future that leases this socket as a device end, for
            /// [`crate::devices`].
            pub(crate) fn leasing(&self) -> crate::devices::Leasing {
                crate::devices::leasing(&self.socket)
            }
        })+
    };
}

python_socket!(
    PyReqSocket,
    ReqSocket,
    "ReqSocket",
    "REQ",
    "A REQ socket: one request out, one reply in, in that order (`EFSM` otherwise).",
    [send recv]
);
python_socket!(
    PyRepSocket,
    RepSocket,
    "RepSocket",
    "REP",
    "A REP socket: one request in, one reply out, with the envelope kept between them.",
    [report recv]
);
python_socket!(
    PyDealerSocket,
    DealerSocket,
    "DealerSocket",
    "DEALER",
    "A DEALER socket: request-reply without the lockstep, round-robin out and fair-queued in.",
    [send recv split PyDealerSend PyDealerRecv]
);
python_socket!(
    PyRouterSocket,
    RouterSocket,
    "RouterSocket",
    "ROUTER",
    "A ROUTER socket: every message carries the peer's routing id as its first frame.",
    [report report_now recv split PyRouterSend PyRouterRecv]
);
python_socket!(
    PyPubSocket,
    PubSocket,
    "PubSocket",
    "PUB",
    "A PUB socket: publisher-side prefix matching, and a drop rather than a block at the high-water mark.",
    [publish]
);
python_socket!(
    PySubSocket,
    SubSocket,
    "SubSocket",
    "SUB",
    "A SUB socket: nothing arrives until something is subscribed to.",
    [recv subscribe]
);
python_socket!(
    PyXPubSocket,
    XPubSocket,
    "XPubSocket",
    "XPUB",
    "An XPUB socket: PUB, with its subscribers' subscriptions delivered to the application.",
    [publish recv subscribe split PyXPubPublish PyXPubRecv]
);
python_socket!(
    PyXSubSocket,
    XSubSocket,
    "XSubSocket",
    "XSUB",
    "An XSUB socket: SUB, with subscriptions sent upstream as messages.",
    [publish recv subscribe split PyXSubSend PyXSubRecv]
);
python_socket!(
    PyPushSocket,
    PushSocket,
    "PushSocket",
    "PUSH",
    "A PUSH socket: round-robin over the workers with room, blocking rather than discarding.",
    [send]
);
python_socket!(
    PyPullSocket,
    PullSocket,
    "PullSocket",
    "PULL",
    "A PULL socket: the fair-queued sink of a pipeline.",
    [recv]
);
python_socket!(
    PyPairSocket,
    PairSocket,
    "PairSocket",
    "PAIR",
    "A PAIR socket: exactly one peer, no reconnection, no routing.",
    [send recv split PyPairSend PyPairRecv]
);

device_ends!(
    PyDealerSocket,
    PyRouterSocket,
    PyPubSocket,
    PySubSocket,
    PyXPubSocket,
    PyXSubSocket,
    PyPushSocket,
    PyPullSocket,
    PyPairSocket,
);
