//! ZeroMQ for Python, on the Rust implementation rather than on libzmq.
//!
//! `import weida_zmq` gets a context, eleven socket types and libzmq's errno
//! vocabulary as exception classes. Underneath is `weida-zmq` — a ZeroMQ
//! implementation with no C library, no `weida` and no `weida-protocol` in it
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4) — and
//! `weida-py-core`, the PyO3 foundation every binding in this workspace shares
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! ```python
//! import asyncio
//! import weida_zmq
//!
//! async def main():
//!     context = weida_zmq.Context()
//!     server = weida_zmq.RepSocket(context)
//!     client = weida_zmq.ReqSocket(context)
//!     endpoint = await server.bind("tcp://127.0.0.1:0")   # the chosen port
//!     await client.connect(endpoint)
//!
//!     await client.send(b"Hello")
//!     assert await server.recv() == [b"Hello"]
//!     await server.send(b"World")
//!     assert await client.recv() == [b"World"]
//!
//!     await context.shutdown()
//!
//! asyncio.run(main())
//! ```
//!
//! # Asyncio first
//!
//! Every call is a coroutine, driven on the reactor the context owns; nothing
//! blocks the event loop and nothing holds the GIL while waiting. A cancelled
//! task cancels the Rust future underneath rather than leaving it running. The
//! synchronous surface — the same eleven types over `weida-zmq`'s `blocking`
//! facade, for a process with no loop at all — is `weida_zmq.sync`, which
//! implements nothing of its own: the facade exists in Rust and building it
//! here would build it twice (0014 §2).
//!
//! # Errors
//!
//! Every failure is a class of this module, named for libzmq's errno and
//! derived from `weida_zmq.ZmqError`, carrying `errno` and `cause`:
//!
//! ```python
//! try:
//!     await socket.recv()
//! except weida_zmq.EFSM as wrong_order:
//!     assert wrong_order.errno == "EFSM"
//! ```
//!
//! # Building it
//!
//! `crates/py/develop.sh` builds this binding into a `uv`-managed virtualenv
//! and runs its Python tests (`crates/zmq/weida-zmq-py/develop.sh` is its
//! wrapper). The wheel is `abi3` from CPython 3.9, so one build serves every
//! later interpreter, and `package.sh` builds it and proves it runs in a fresh
//! virtualenv with no Rust toolchain on `PATH`.
//!
//! # What differs from `pyzmq`
//!
//! Row by row in `docs/libraries/zmq-py.md`, this binding's parity document.

use pyo3::prelude::*;

mod context;
mod devices;
mod errors;
mod halves;
mod identity;
mod monitor;
mod ops;
mod options;
mod sockets;
mod sync;
mod values;

/// `weida_zmq`, as Python sees it.
#[pymodule]
fn weida_zmq(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<context::Context>()?;
    module.add_class::<options::PyZmqOption>()?;
    module.add_class::<options::PySocketOptions>()?;
    module.add_class::<options::PyContextOptions>()?;
    module.add("OPTIONS", options::table(module.py())?)?;
    module.add_function(pyo3::wrap_pyfunction!(option, module)?)?;
    // `::` because `#[pymodule]` puts a module of this function's name in
    // scope, and that name is the crate's.
    module.add(
        "DEFAULT_CLOSE_BUDGET",
        ::weida_zmq::DEFAULT_CLOSE_BUDGET.as_secs_f64(),
    )?;
    module.add(
        "DEFAULT_MAX_MESSAGE_SIZE",
        ::weida_zmq::DEFAULT_MAX_MESSAGE_SIZE,
    )?;
    module.add_class::<identity::PyCurveKey>()?;
    module.add_class::<identity::PyRoutingId>()?;
    module.add_class::<identity::PyZapUserId>()?;
    module.add_class::<identity::PyPeer>()?;
    module.add_function(pyo3::wrap_pyfunction!(identity::curve_keypair, module)?)?;
    module.add("ZAP_ENDPOINT", ::weida_zmq::ZAP_ENDPOINT)?;
    module.add_class::<monitor::PyMonitor>()?;
    module.add_class::<monitor::PyMonitorEvent>()?;
    module.add_class::<devices::PyProxyStatistics>()?;
    module.add_function(pyo3::wrap_pyfunction!(devices::proxy, module)?)?;
    module.add_function(pyo3::wrap_pyfunction!(devices::proxy_steerable, module)?)?;
    devices::controls(module)?;
    module.add("DEFAULT_MAX_SOCKETS", ::weida_zmq::DEFAULT_MAX_SOCKETS)?;
    module.add_class::<sockets::Discarded>()?;
    module.add_class::<values::PyMultipart>()?;
    module.add_class::<values::PySent>()?;
    module.add_class::<values::PyPublished>()?;
    module.add_class::<sockets::PyReqSocket>()?;
    module.add_class::<sockets::PyRepSocket>()?;
    module.add_class::<sockets::PyDealerSocket>()?;
    module.add_class::<sockets::PyRouterSocket>()?;
    module.add_class::<sockets::PyPubSocket>()?;
    module.add_class::<sockets::PySubSocket>()?;
    module.add_class::<sockets::PyXPubSocket>()?;
    module.add_class::<sockets::PyXSubSocket>()?;
    module.add_class::<sockets::PyPushSocket>()?;
    module.add_class::<sockets::PyPullSocket>()?;
    module.add_class::<sockets::PyPairSocket>()?;
    halves::install(module)?;
    sync::install(module)?;
    module.add("__all__", every_name())?;
    Ok(())
}

/// `weida_zmq.option("ZMQ_SNDHWM")`: one row of libzmq's option table, or
/// `None` for a name it does not have.
#[pyfunction]
fn option(py: Python<'_>, name: &str) -> PyResult<Option<Py<options::PyZmqOption>>> {
    options::row(py, name)
}

/// What `from weida_zmq import *` gets: the classes above and the exception
/// family, which `errors::install` adds by name.
fn every_name() -> Vec<&'static str> {
    let mut names = vec![
        "Context",
        "Discarded",
        "Multipart",
        "Sent",
        "Published",
        "ReqSocket",
        "RepSocket",
        "DealerSocket",
        "RouterSocket",
        "PubSocket",
        "SubSocket",
        "XPubSocket",
        "XSubSocket",
        "PushSocket",
        "PullSocket",
        "PairSocket",
        "DealerSend",
        "DealerRecv",
        "RouterSend",
        "RouterRecv",
        "PairSend",
        "PairRecv",
        "XPubPublish",
        "XPubRecv",
        "XSubSend",
        "XSubRecv",
        "ZmqError",
        "CurveKey",
        "RoutingId",
        "ZapUserId",
        "Peer",
        "curve_keypair",
        "ZAP_ENDPOINT",
        "Monitor",
        "MonitorEvent",
        "ProxyStatistics",
        "proxy",
        "proxy_steerable",
        "CONTROL_PAUSE",
        "CONTROL_RESUME",
        "CONTROL_TERMINATE",
        "CONTROL_STATISTICS",
        "sync",
        "ZmqOption",
        "SocketOptions",
        "ContextOptions",
        "OPTIONS",
        "option",
        "DEFAULT_CLOSE_BUDGET",
        "DEFAULT_MAX_MESSAGE_SIZE",
        "DEFAULT_MAX_SOCKETS",
    ];
    names.extend_from_slice(errors::NAMES);
    names
}
