//! AMQP 1.0 for Python, on the Rust client rather than on Proton.
//!
//! `import weida_amqp` gets a connection, sessions, links and the
//! specification's own error conditions as exception classes. Underneath is
//! `weida-amqp` — an AMQP 1.0 client with no C library, no `weida` and no
//! `weida-protocol` in it
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4) — and
//! `weida-py-core`, the PyO3 foundation every binding in this workspace shares
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! ```python
//! import asyncio
//! import weida_amqp
//!
//! async def main():
//!     connection = await weida_amqp.connect("127.0.0.1", 5672, container_id="app")
//!     session = await connection.begin()
//!
//!     sender = await session.attach("orders", "sender", "/queues/orders")
//!     outcome = await sender.send(b"an order")
//!     assert outcome.name == "accepted"          # what the receiver committed to
//!     assert not outcome.may_be_redelivered
//!
//!     receiver = await session.attach("invoices", "receiver", "/queues/invoices")
//!     await receiver.grant_credit(10)            # nothing arrives before this
//!     async for delivery in receiver:
//!         print(delivery.body(), delivery.application_properties())
//!         await receiver.accept(delivery.delivery_id)
//!         break
//!
//!     await connection.close()
//!
//! asyncio.run(main())
//! ```
//!
//! # Three things this surface insists on
//!
//! * **`await send` returns the delivery's terminal state**, not a boolean.
//!   On an unsettled link the coroutine completes when the peer's
//!   `disposition` has settled the delivery, so the `await` means "the
//!   receiver told me what happened". On a `settled`-mode link it returns
//!   `None`, because nothing is coming and nothing can be concluded — which is
//!   the mode's whole cost, reported rather than hidden.
//! * **Credit is granted explicitly.** `grant_credit(n)` and nothing before
//!   it. AMQP's link credit is the receiver's instrument, and a binding that
//!   granted some behind the caller's back would be choosing its prefetch.
//! * **The answering `attach` is data.** `negotiated()` reports the settle
//!   modes actually in force and the addresses the peer actually created,
//!   because a broker may narrow what was asked for and a caller that did not
//!   look would believe it had a guarantee it does not have.
//!
//! # Errors
//!
//! Two vocabularies in one family, both under `weida_amqp.AmqpError`: one
//! class per client-side failure (`HandshakeTimeout`, `ConnectionGone`,
//! `Configuration`, ...) and one per error condition the specification names
//! (`LinkStolen`, `SessionWindowViolation`, `ResourceLimitExceeded`, ...). A
//! condition wins where the error carries one, because the condition is the
//! part a program can branch on.
//!
//! ```python
//! try:
//!     await session.attach("orders", "sender", "/queues/nope")
//! except weida_amqp.NotFound as missing:
//!     assert missing.errno == "NotFound"
//! ```
//!
//! `weida_amqp.CONDITIONS` maps each class name to the `amqp:...` symbol it
//! stands for, because a Python name cannot contain a colon and a broker's
//! documentation is written in symbols.
//!
//! # Synchronous
//!
//! `weida_amqp.sync` is the same client with no event loop in the process; see
//! [`sync`]. It implements no protocol behaviour of its own.
//!
//! # What differs from `python-qpid-proton`
//!
//! Row by row in `docs/libraries/amqp-py.md`, this binding's parity document.
//!
//! # Building it
//!
//! `crates/py/develop.sh` builds this binding into a `uv`-managed virtualenv
//! and runs its Python tests (`crates/amqp/weida-amqp-py/develop.sh` is its
//! wrapper). The wheel is `abi3` from CPython 3.9, and `package.sh` proves it
//! runs in a fresh virtualenv with no Rust toolchain on `PATH`.

use pyo3::prelude::*;

mod connection;
mod errors;
mod link;
mod session;
pub mod sync;
mod values;

/// Opens a connection: the header exchange, the SASL dialog if one was asked
/// for, and `open` on channel 0.
///
/// `worker_threads` sizes the reactor this connection owns. One is enough for
/// a client: the driver is a single task and the work is I/O.
#[pyfunction]
#[pyo3(signature = (
    host,
    port,
    *,
    container_id="weida-amqp-py",
    hostname=None,
    max_frame_size=None,
    channel_max=None,
    idle_time_out=None,
    sasl_user=None,
    sasl_password=None,
    sasl_anonymous=false,
    handshake_timeout=None,
    worker_threads=1,
))]
#[allow(clippy::too_many_arguments, reason = "one keyword argument per option")]
fn connect<'py>(
    py: Python<'py>,
    host: &str,
    port: u16,
    container_id: &str,
    hostname: Option<String>,
    max_frame_size: Option<u32>,
    channel_max: Option<u16>,
    idle_time_out: Option<f64>,
    sasl_user: Option<String>,
    sasl_password: Option<String>,
    sasl_anonymous: bool,
    handshake_timeout: Option<f64>,
    worker_threads: usize,
) -> PyResult<Bound<'py, PyAny>> {
    let options = connection::options_from(
        py,
        container_id,
        hostname,
        max_frame_size,
        channel_max,
        idle_time_out,
        sasl_user,
        sasl_password,
        sasl_anonymous,
        handshake_timeout,
    )?;
    // The reactor is built synchronously, because nothing can be awaited
    // until it exists and building one does not block. The handshake itself
    // **is** awaited: a blocking connect here would park the caller's event
    // loop, which in a test means parking the peer it is connecting to.
    let (exec, reactor) = errors::raise(py, connection::reactor(worker_threads))?;
    let bridge = weida_py_core::Bridge::new(exec.clone(), errors::to_py);
    let host = host.to_owned();
    bridge.awaitable(py, async move {
        // `::` because `#[pymodule] fn weida_amqp` puts a module of that name
        // in this crate's root, which would otherwise shadow the library.
        let inner = ::weida_amqp::Connection::connect(&exec, &host, port, options)
            .await
            .map_err(errors::errno_of)?;
        Ok(connection::PyConnection::of(inner, reactor))
    })
}

/// `weida_amqp`, as Python sees it.
#[pymodule]
fn weida_amqp(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<connection::PyConnection>()?;
    module.add_class::<session::PySession>()?;
    module.add_class::<session::Windows>()?;
    module.add_class::<link::PyLink>()?;
    module.add_class::<link::Negotiated>()?;
    module.add_class::<link::Credit>()?;
    module.add_class::<values::PyDelivery>()?;
    module.add_class::<values::PyOutcome>()?;
    module.add_function(pyo3::wrap_pyfunction!(connect, module)?)?;
    sync::install(module)?;
    Ok(())
}
