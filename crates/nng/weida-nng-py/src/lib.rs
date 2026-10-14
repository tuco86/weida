//! nanomsg/NNG's Scalability Protocols for Python, on the Rust
//! implementation rather than on the C library.
//!
//! `import weida_nng` gets a context, eleven socket types, `nng_ctx` as
//! Python objects and NNG's errno vocabulary as exception classes.
//! Underneath is `weida-nng` — an SP implementation with no C library, no
//! `weida` and no `weida-protocol` in it
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4) —
//! and `weida-py-core`, the PyO3 foundation every binding in this workspace
//! shares ([0014](../../../../docs/decisions/0014-parallel-libraries.md)
//! §2).
//!
//! ```python
//! import asyncio
//! import weida_nng
//!
//! async def main():
//!     context = weida_nng.Context()
//!     server = weida_nng.RepSocket(context)
//!     client = weida_nng.ReqSocket(context)
//!     url = await server.listen("tcp://127.0.0.1:0")   # the chosen port
//!     await client.dial(url)
//!
//!     await client.send(b"Hello")
//!     assert await server.recv() == b"Hello"
//!     await server.send(b"World")
//!     assert await client.recv() == b"World"
//!
//!     await context.shutdown()
//!
//! asyncio.run(main())
//! ```
//!
//! # Asyncio first
//!
//! Every call that can wait is a coroutine, driven on the reactor the
//! context owns; nothing blocks the event loop and nothing holds the GIL
//! while waiting. A cancelled task cancels the Rust future underneath
//! rather than leaving it running.
//!
//! Two coroutines may use one socket at the same time: an SP socket's
//! `send` and `recv` take `&self`, so a parked receive does not hold the
//! socket against a send. Where a protocol *does* need one transaction at a
//! time it says so per context, with `ESTATE` — which is why
//! `socket.context()` exists and why concurrent requests are concurrent
//! rather than queued.
//!
//! # And synchronous beside it
//!
//! `weida_nng.sync` is the same eleven protocols and the same contexts for
//! a process with no loop at all, over `weida-nng`'s own `blocking` facade.
//! It implements nothing: both surfaces drive one set of sockets, which is
//! why they cannot disagree and why a `sync` socket and an asyncio socket
//! exchange messages in the suite.
//!
//! # Errors
//!
//! Every failure is a class of this module, named for NNG's errno and
//! derived from `weida_nng.NngError`, carrying `errno` and `cause`:
//!
//! ```python
//! try:
//!     await responder.send(b"unasked")
//! except weida_nng.ESTATE as wrong_order:
//!     assert wrong_order.errno == "ESTATE"
//! ```
//!
//! # Building it
//!
//! `crates/py/develop.sh` builds this binding into a `uv`-managed virtualenv
//! and runs its Python tests (`crates/nng/weida-nng-py/develop.sh` is its
//! wrapper). The wheel is `abi3` from CPython 3.9, so one build serves every
//! later interpreter.
//!
//! # What differs from NNG
//!
//! Row by row in `docs/libraries/nng.md`, the library's parity document.
//! This binding adds two absences of its own, both named rather than
//! half-offered: raw sockets with `nng_device`, and the TLS transport's
//! configuration — `tls+tcp://` is refused here until the certificate
//! surface is designed for Python, rather than being offered with a
//! configuration a caller cannot supply.

use pyo3::prelude::*;

mod context;
mod contexts;
mod errors;
mod options;
mod sockets;
mod subscriptions;
mod sync;
mod values;

/// `weida_nng`, as Python sees it.
#[pymodule]
fn weida_nng(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<context::Context>()?;
    module.add_class::<options::PySocketOptions>()?;
    module.add_class::<options::PyNngOption>()?;
    module.add("OPTIONS", options::table(module.py())?)?;
    module.add_function(pyo3::wrap_pyfunction!(option, module)?)?;
    module.add_class::<values::PyBroadcast>()?;
    module.add_class::<contexts::PyReqContext>()?;
    module.add_class::<contexts::PyReplyContext>()?;
    module.add_class::<contexts::PySurveyContext>()?;
    module.add_class::<sockets::PyReqSocket>()?;
    module.add_class::<sockets::PyRepSocket>()?;
    module.add_class::<sockets::PyPushSocket>()?;
    module.add_class::<sockets::PyPullSocket>()?;
    module.add_class::<sockets::PyPubSocket>()?;
    module.add_class::<sockets::PySubSocket>()?;
    module.add_class::<sockets::PyPair0Socket>()?;
    module.add_class::<sockets::PyPair1Socket>()?;
    module.add_class::<sockets::PySurveyorSocket>()?;
    module.add_class::<sockets::PyRespondentSocket>()?;
    module.add_class::<sockets::PyBusSocket>()?;
    sync::install(module)?;
    // The numbers a caller compares a configuration against, from the
    // library rather than retyped here.
    // `::` because `#[pymodule]` puts a module of this function's name in
    // scope, and that name is the crate's.
    module.add("DEFAULT_MAX_SOCKETS", ::weida_nng::DEFAULT_MAX_SOCKETS)?;
    module.add("DEFAULT_MAX_PIPES", ::weida_nng::DEFAULT_MAX_PIPES)?;
    module.add("DEFAULT_RECV_MAX_SIZE", ::weida_nng::DEFAULT_RECV_MAX_SIZE)?;
    module.add("MAX_QUEUE_DEPTH", ::weida_nng::MAX_QUEUE_DEPTH)?;
    module.add("NNG_MAX_TTL", ::weida_nng::NNG_MAX_TTL)?;
    module.add("SPEC_MAX_TTL", ::weida_nng::SPEC_MAX_TTL)?;
    module.add("NNG_MAXADDRLEN", ::weida_nng::NNG_MAXADDRLEN)?;
    module.add(
        "DEFAULT_CLOSE_BUDGET",
        ::weida_nng::DEFAULT_CLOSE_BUDGET.as_secs_f64(),
    )?;
    module.add(
        "DEFAULT_SURVEY_TIME",
        ::weida_nng::DEFAULT_SURVEY_TIME.as_secs_f64(),
    )?;
    module.add(
        "DEFAULT_RESEND_TIME",
        ::weida_nng::DEFAULT_RESEND_TIME.as_secs_f64(),
    )?;
    module.add("__all__", every_name())?;
    Ok(())
}

/// `weida_nng.option("NNG_OPT_RECVMAXSZ")`: one row of `nng_options(5)`, or
/// `None` for a name NNG does not have.
#[pyfunction]
fn option(py: Python<'_>, name: &str) -> PyResult<Option<Py<options::PyNngOption>>> {
    options::row(py, name)
}

/// What `from weida_nng import *` gets: the classes above and the exception
/// family, which `errors::install` adds by name.
fn every_name() -> Vec<&'static str> {
    let mut names = vec![
        "Context",
        "SocketOptions",
        "NngOption",
        "OPTIONS",
        "option",
        "Broadcast",
        "ReqContext",
        "ReplyContext",
        "SurveyContext",
        "ReqSocket",
        "RepSocket",
        "PushSocket",
        "PullSocket",
        "PubSocket",
        "SubSocket",
        "Pair0Socket",
        "Pair1Socket",
        "SurveyorSocket",
        "RespondentSocket",
        "BusSocket",
        "NngError",
        "DEFAULT_MAX_SOCKETS",
        "DEFAULT_MAX_PIPES",
        "DEFAULT_RECV_MAX_SIZE",
        "MAX_QUEUE_DEPTH",
        "NNG_MAX_TTL",
        "SPEC_MAX_TTL",
        "NNG_MAXADDRLEN",
        "DEFAULT_CLOSE_BUDGET",
        "DEFAULT_SURVEY_TIME",
        "DEFAULT_RESEND_TIME",
    ];
    names.extend_from_slice(errors::NAMES);
    names
}
