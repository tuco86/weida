//! The two halves of a split socket, as Python classes.
//!
//! `sock.split()` on a DEALER, ROUTER, PAIR, XPUB or XSUB returns
//! `(send_half, recv_half)`, and the two are usable **at the same time from
//! two coroutines**: a task parked in `await recv_half.recv()` does not
//! make `await send_half.send(...)` wait behind it, which is what pyzmq
//! permits and the one-lease-per-socket rule of [`crate::sockets`] otherwise
//! enforces on a whole socket.
//! The halves are `weida-zmq`'s own (`weida_zmq::split`), so no pattern
//! behaviour is decided here: each half is one of the library's half types
//! in its own slot, and every method is the same [`crate::ops`] delegation
//! the whole socket used.
//!
//! The socket object `split()` was called on is **retired**: its slot will
//! never hold the socket again, and any later call on it raises `ENOTSOCK`
//! naming the split. REQ and REP have no `split`, because 28/REQREP makes
//! their two directions alternate and a split would promise an
//! independence the protocol forbids.
//!
//! What a half is not: a way to share one socket between two coroutines in
//! *the same* direction. Each half is still one slot, leased to one
//! operation at a time, for the same reason the whole socket was.

use std::sync::Arc;

use pyo3::prelude::*;
use weida_zmq::split::{
    DealerRecv, DealerSend, PairRecv, PairSend, RouterRecv, RouterSend, XPubPublish, XPubRecv,
    XSubRecv, XSubSend,
};

use crate::lease::Slot;
use crate::values::{PyMultipart, PyPublished, PySent};

/// Builds one half class with the operation methods its direction has.
macro_rules! python_half {
    ($class:ident, $rust:ident, $python:literal, $what:literal, [$($flag:ident)*]) => {
        python_half!(@build $class, $rust, $python, $what, {}, [$($flag)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $what:literal,
     {$($acc:tt)*}, [recv $($rest:ident)*]) => {
        python_half!(@build $class, $rust, $python, $what, {
            $($acc)*

            /// Receives the next message, whole; bounded by `ZMQ_RCVTIMEO`
            /// or by `timeout` seconds. See the socket's `recv`.
            #[pyo3(signature = (*, timeout=None))]
            fn recv<'py>(
                &self,
                py: Python<'py>,
                timeout: Option<f64>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::recv(py, &self.bridge, &self.half, timeout)
            }

            /// `ZMQ_DONTWAIT`: the message if one is queued, `EAGAIN` if not.
            fn recv_nowait(&self, py: Python<'_>) -> PyResult<PyMultipart> {
                crate::ops::recv_nowait(py, &self.half)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $what:literal,
     {$($acc:tt)*}, [send $($rest:ident)*]) => {
        python_half!(@build $class, $rust, $python, $what, {
            $($acc)*

            /// Sends a message, blocking rather than discarding; bounded by
            /// `ZMQ_SNDTIMEO` or by `timeout` seconds. See the socket's
            /// `send`.
            #[pyo3(signature = (message, *, timeout=None))]
            fn send<'py>(
                &self,
                py: Python<'py>,
                message: &Bound<'py, PyAny>,
                timeout: Option<f64>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::send(py, &self.bridge, &self.half, message, timeout)
            }

            /// `ZMQ_DONTWAIT`: queues the message or reports `EAGAIN`.
            fn send_nowait(&self, py: Python<'_>, message: &Bound<'_, PyAny>) -> PyResult<()> {
                crate::ops::send_nowait(py, &self.half, message)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $what:literal,
     {$($acc:tt)*}, [report $($rest:ident)*]) => {
        python_half!(@build $class, $rust, $python, $what, {
            $($acc)*

            /// Sends a message and reports what became of it. See the
            /// socket's `send`.
            fn send<'py>(
                &self,
                py: Python<'py>,
                message: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::send_reporting(py, &self.bridge, &self.half, message)
            }

            /// `ZMQ_DONTWAIT`, with the same report.
            fn send_nowait(
                &self,
                py: Python<'_>,
                message: &Bound<'_, PyAny>,
            ) -> PyResult<PySent> {
                crate::ops::send_reporting_nowait(py, &self.half, message)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $what:literal,
     {$($acc:tt)*}, [publish $($rest:ident)*]) => {
        python_half!(@build $class, $rust, $python, $what, {
            $($acc)*

            /// Publishes to every subscriber whose subscription matches,
            /// and reports the count. See the socket's `send`.
            fn send<'py>(
                &self,
                py: Python<'py>,
                message: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::ops::publish(py, &self.bridge, &self.half, message)
            }

            /// The same thing without the coroutine, since a publish never
            /// waits.
            fn send_nowait(
                &self,
                py: Python<'_>,
                message: &Bound<'_, PyAny>,
            ) -> PyResult<PyPublished> {
                crate::ops::publish_nowait(py, &self.half, message)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $what:literal,
     {$($acc:tt)*}, [subscribe $($rest:ident)*]) => {
        python_half!(@build $class, $rust, $python, $what, {
            $($acc)*

            /// Subscribes to a prefix. See the socket's `subscribe`.
            fn subscribe<'py>(
                &self,
                py: Python<'py>,
                prefix: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let prefix = weida_py_core::payload_of(prefix)?;
                crate::ops::subscribe(py, &self.bridge, &self.half, prefix)
            }

            /// Cancels one subscription to a prefix.
            fn unsubscribe<'py>(
                &self,
                py: Python<'py>,
                prefix: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let prefix = weida_py_core::payload_of(prefix)?;
                crate::ops::unsubscribe(py, &self.bridge, &self.half, prefix)
            }
        }, [$($rest)*]);
    };

    (@build $class:ident, $rust:ident, $python:literal, $what:literal,
     {$($methods:tt)*}, []) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_zmq")]
        pub struct $class {
            half: Arc<Slot<$rust>>,
            bridge: weida_py_core::Bridge,
        }

        impl $class {
            pub(crate) fn new(half: $rust, bridge: weida_py_core::Bridge) -> $class {
                $class {
                    half: Slot::new(half),
                    bridge,
                }
            }
        }

        #[pymethods]
        impl $class {
            fn __repr__(&self) -> String {
                concat!("<weida_zmq.", $python, ">").to_owned()
            }

            $($methods)*
        }
    };
}

python_half!(
    PyDealerSend,
    DealerSend,
    "DealerSend",
    "The sending half of a split DealerSocket.",
    [send]
);
python_half!(
    PyDealerRecv,
    DealerRecv,
    "DealerRecv",
    "The receiving half of a split DealerSocket.",
    [recv]
);
python_half!(
    PyRouterSend,
    RouterSend,
    "RouterSend",
    "The sending half of a split RouterSocket: the first frame is the routing id.",
    [report]
);
python_half!(
    PyRouterRecv,
    RouterRecv,
    "RouterRecv",
    "The receiving half of a split RouterSocket: every message carries the peer's routing id.",
    [recv]
);
python_half!(
    PyPairSend,
    PairSend,
    "PairSend",
    "The sending half of a split PairSocket.",
    [send]
);
python_half!(
    PyPairRecv,
    PairRecv,
    "PairRecv",
    "The receiving half of a split PairSocket.",
    [recv]
);
python_half!(
    PyXPubPublish,
    XPubPublish,
    "XPubPublish",
    "The publishing half of a split XPubSocket.",
    [publish]
);
python_half!(
    PyXPubRecv,
    XPubRecv,
    "XPubRecv",
    "The receiving half of a split XPubSocket: subscriptions arrive here, and `subscribe`/`unsubscribe` act on the subscriber that spoke last.",
    [recv subscribe]
);
python_half!(
    PyXSubSend,
    XSubSend,
    "XSubSend",
    "The sending half of a split XSubSocket: subscriptions and upstream messages.",
    [publish subscribe]
);
python_half!(
    PyXSubRecv,
    XSubRecv,
    "XSubRecv",
    "The receiving half of a split XSubSocket.",
    [recv]
);

/// Adds the ten half classes to the module.
pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDealerSend>()?;
    module.add_class::<PyDealerRecv>()?;
    module.add_class::<PyRouterSend>()?;
    module.add_class::<PyRouterRecv>()?;
    module.add_class::<PyPairSend>()?;
    module.add_class::<PyPairRecv>()?;
    module.add_class::<PyXPubPublish>()?;
    module.add_class::<PyXPubRecv>()?;
    module.add_class::<PyXSubSend>()?;
    module.add_class::<PyXSubRecv>()?;
    Ok(())
}
