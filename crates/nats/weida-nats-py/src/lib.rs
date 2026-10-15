//! Core NATS for Python, on the Rust implementation.
//!
//! `import weida_nats` gets a connection, subscriptions with queue groups,
//! request-reply with a mandatory window, and the library's failure
//! vocabulary as exception classes. Underneath is `weida-nats` — a NATS
//! client with no `weida` and no `weida-protocol` in it
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4) — and
//! `weida-py-core`, the PyO3 foundation every binding in this workspace
//! shares ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! ```python
//! import asyncio
//! import weida_nats
//!
//! async def main():
//!     nats = await weida_nats.connect("127.0.0.1", 4222)
//!
//!     orders = await nats.subscribe("orders.>")
//!     await nats.publish("orders.created", b"{}")
//!     message = await orders.next()
//!     assert message.subject_str == "orders.created"
//!
//!     # The window is mandatory: a request API that can hang is a bug.
//!     reply = await nats.request("service.echo", b"ping", 2.0)
//!     assert reply.payload == b"ping"
//!
//!     await nats.close()
//!
//! asyncio.run(main())
//! ```
//!
//! # Two surfaces, one client
//!
//! Every call of `weida_nats` is a coroutine, driven on the reactor the
//! connection owns; nothing blocks the event loop and nothing holds the GIL
//! while waiting. `weida_nats.sync` is the same objects with no coroutines,
//! for a process with no loop at all — the same futures, driven on the
//! calling thread with the GIL released. Neither surface implements any
//! protocol behaviour: subject validation, `INFO.max_payload`, the inbox,
//! the mandatory request window, the 503-versus-timeout distinction, queue
//! groups and every bound are `weida-nats`'s, called and never re-derived.
//!
//! # Errors
//!
//! Every failure is a class of this module, named for the library's own enum
//! variant and derived from `weida_nats.NatsError`, carrying `errno` and
//! `cause`:
//!
//! ```python
//! try:
//!     await nats.request("nobody.listening", b"?", 1.0)
//! except weida_nats.NoResponders:
//!     ...   # 503: nobody was subscribed, now
//! except weida_nats.RequestTimeout:
//!     ...   # the window closed: somebody may well have been
//! ```
//!
//! Those two are distinct classes because the library keeps them distinct on
//! purpose, and a caller that retries needs to tell them apart.
//!
//! # Building it
//!
//! `crates/py/develop.sh` builds this binding into a `uv`-managed virtualenv
//! and runs its Python tests (`crates/nats/weida-nats-py/develop.sh` is its
//! wrapper). The wheel is `abi3` from CPython 3.9, so one build serves every
//! later interpreter.
//!
//! # What is not here
//!
//! TLS. `Connection::connect_tls` takes the caller's
//! `rustls::ClientConfig`, and which certificates an application trusts is
//! the application's decision — there is no Python object to hand it, so
//! this module does not offer a TLS connect rather than choosing a trust
//! store for its callers. A server whose `INFO` demands TLS arrives as
//! `weida_nats.TlsRequired`.

use pyo3::prelude::*;

mod connection;
mod errors;
mod options;
mod subscription;
mod sync;
mod values;

/// `weida_nats`, as Python sees it.
#[pymodule]
fn weida_nats(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<connection::PyConnection>()?;
    module.add_class::<subscription::PySubscription>()?;
    module.add_class::<values::PyMessage>()?;
    module.add_class::<values::PyRemoteInfo>()?;
    module.add_class::<values::PyState>()?;
    module.add_function(pyo3::wrap_pyfunction!(connection::connect, module)?)?;
    // `::` because `#[pymodule]` puts a module of this function's name in
    // scope, and that name is the crate's.
    module.add("NO_RESPONDERS", ::weida_nats::NO_RESPONDERS)?;
    module.add("INBOX_PREFIX", ::weida_nats::options::INBOX_PREFIX)?;
    module.add("DEFAULT_PORT", ::weida_nats::options::PORT)?;
    sync::install(module)?;
    module.add("__all__", every_name())?;
    Ok(())
}

/// What `from weida_nats import *` gets: the classes above and the exception
/// family, which `errors::install` adds by name.
fn every_name() -> Vec<&'static str> {
    let mut names = vec![
        "Connection",
        "Subscription",
        "Message",
        "RemoteInfo",
        "State",
        "connect",
        "sync",
        "NatsError",
        "NO_RESPONDERS",
        "INBOX_PREFIX",
        "DEFAULT_PORT",
    ];
    names.extend_from_slice(errors::NAMES);
    names
}
