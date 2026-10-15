//! weida for Python, on the Rust implementation.
//!
//! `import weida` gets a runtime, a QUIC binding, all six patterns, the
//! cursor back channel and the library's failure vocabulary as exception
//! classes. Underneath is `weida` itself, so this is the binding of the
//! **product** rather than of a competitor implementation: the other five bind
//! ZeroMQ, SP, MQTT, AMQP and NATS
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! ```python
//! import asyncio
//! import weida
//!
//! async def main():
//!     server = weida.Runtime()
//!     identity = weida.Identity.generate()
//!     binding = await server.bind("127.0.0.1:0", identity)
//!     replier = binding.replier("/echo")
//!
//!     async def serve():
//!         request = await replier.accept(1 << 20)
//!         await request.reply(request.payload)
//!
//!     answering = asyncio.create_task(serve())
//!
//!     client = weida.Runtime()
//!     requester = client.requester(weida.Trust.by_address())
//!     # The address names the key that must answer, so nothing else is
//!     # configured: no CA, no certificate file.
//!     await requester.connect(binding.url("/echo"))
//!     assert await requester.request(b"ping", 1 << 20) == b"ping"
//!     await answering
//!
//! asyncio.run(main())
//! ```
//!
//! # Every receive takes a ceiling, and that is not an inconvenience
//!
//! `accept`, `request`, `recv` each take a maximum payload size in bytes.
//! weida's payloads are streams — [`INVARIANTS.md`](../../../../docs/INVARIANTS.md)
//! forbids the core from materializing them — and a Python object *is*
//! materialized, so the caller who wants the bytes in memory is the one who
//! has to say how many of them there may be. A binding that chose a default
//! would be choosing how much memory a stranger may make a Python process
//! allocate. A payload larger than the ceiling is `weida.LimitExceeded`.
//!
//! # What is here, and what is not
//!
//! **All six of weida's patterns**, on both surfaces. Asyncio:
//! `Requester`/`Replier`,
//! `Pusher`/`Puller`, `Publisher`/`Subscriber` and
//! `Paired`, `Surveyor`/`Respondent`, `BusMember`, each with
//! the whole-payload calls a caller reaches for first — plus the streamed
//! forms for a payload that does not fit memory where the pattern has one:
//! `pusher.open()`, `requester.open()`, `publisher.open(topic)`,
//! `puller.recv_stream()` and `subscriber.recv_stream()`. Synchronous:
//! `weida.sync`, which is
//! `weida::blocking` with argument conversion around it, for a process with
//! no event loop; it shares every value class with the asyncio surface and
//! deliberately has no streamed form, because the facade takes whole
//! payloads and a `sync` module that invented streaming would be inventing a
//! second facade.
//!
//! A survey is a **value** on both surfaces — `weida.Survey`, with the
//! answers and the counts that make silence readable — and not an
//! asynchronous iterator: the only channel out of a bridged future here is
//! the errno family, so `async for` would need a second error channel
//! invented for `StopAsyncIteration` alone. The Rust `SurveyRun` is where
//! answers arrive one at a time. `weida.Cursors` reads the same way, and for
//! the same reason: `while (set := await cursors.changed()) is not None:`.
//!
//! **The cursor surface** is here too: `pusher.send(payload,
//! report=[weida.PROCESSED])` orders a report and hands back the
//! `weida.Cursors` to read it on, `puller.recv_reporting(max_bytes)` hands a
//! receiver the `weida.Reporter` to answer with, and `IncomingMeta` carries
//! the three report fields the wire does. A level is an **integer** —
//! `weida.ACCEPTED`, `weida.PROCESSED`, or an application's own number at or
//! above `weida.APPLICATION_FLOOR` — because the level space is open and a
//! class would close it.
//!
//! What is absent, with the reason: the raw L0 surface — `Peer` and
//! `Acceptor`, weida's own stream-level API — because every pattern above is
//! built on it and a Python caller that wants a bare stream wants the Rust
//! API; and type stubs, which are not shipped
//! (`docs/libraries/weida-py.md` §10).
//!
//! # No protocol behaviour lives here
//!
//! HELLO, negotiation, the dispatch rules of
//! [PROTOCOL.md](../../../../docs/PROTOCOL.md) §9.4, the drain, every limit
//! and every guarantee are `weida`'s. This crate converts arguments, awaits,
//! and maps failures onto classes.

use pyo3::prelude::*;

mod cursors;
mod endpoints;
mod errors;
mod lock;
mod patterns;
mod pubsub;
mod runtime;
mod streams;
mod sync;
mod values;

/// `weida`, as Python sees it.
#[pymodule]
fn weida(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<runtime::PyRuntime>()?;
    module.add_class::<runtime::PyBinding>()?;
    module.add_class::<values::PyTrust>()?;
    module.add_class::<values::PyIdentity>()?;
    module.add_class::<values::PyIncomingMeta>()?;
    module.add_class::<values::PySurvey>()?;
    module.add_class::<values::PyDishDrops>()?;
    module.add_class::<values::PyConnectionStats>()?;
    module.add_class::<values::PyTransportStats>()?;
    module.add_class::<values::PyPathStats>()?;
    module.add_class::<values::PyUdpCounts>()?;
    module.add_class::<values::PyRemoteStats>()?;
    module.add_class::<endpoints::PyRequester>()?;
    module.add_class::<endpoints::PyReplier>()?;
    module.add_class::<endpoints::PyRequest>()?;
    module.add_class::<endpoints::PyPusher>()?;
    module.add_class::<endpoints::PyPuller>()?;
    module.add_class::<pubsub::PyPublisher>()?;
    module.add_class::<pubsub::PySubscriber>()?;
    module.add_class::<pubsub::PyFanOut>()?;
    module.add_class::<patterns::PyPaired>()?;
    module.add_class::<patterns::PySurveyor>()?;
    module.add_class::<patterns::PyRespondent>()?;
    module.add_class::<patterns::PyBusMember>()?;
    module.add_class::<patterns::PyRadio>()?;
    module.add_class::<patterns::PySegment>()?;
    module.add_class::<patterns::PyDish>()?;
    module.add_class::<streams::PyOutgoingStream>()?;
    module.add_class::<streams::PyIncomingStream>()?;
    module.add_class::<streams::PyReply>()?;
    module.add_class::<cursors::PyCursors>()?;
    module.add_class::<cursors::PyReporter>()?;
    // The cursor vocabulary: the named rungs of weida's ladder, the floor an
    // application's own stages start at, and the two report modes. Integers
    // rather than an enum class, because the level space is **open** — an
    // application names its own numbers above the floor — and a class would
    // close what the protocol leaves open.
    for (name, value) in cursors::NAMED_LEVELS {
        module.add(name, value)?;
    }
    module.add("APPLICATION_FLOOR", ::weida::CursorLevel::APPLICATION_FLOOR)?;
    for (name, value) in cursors::MODES {
        module.add(name, value)?;
    }
    sync::install(module)?;
    module.add("VERSION", ::weida::VERSION)?;
    module.add("ALPN", ::weida::ALPN)?;
    module.add("__all__", every_name())?;
    Ok(())
}

/// What `from weida import *` gets: the classes above and the exception
/// family, which `errors::install` adds by name.
fn every_name() -> Vec<&'static str> {
    let mut names = vec![
        "Runtime",
        "Binding",
        "Trust",
        "Identity",
        "IncomingMeta",
        "Requester",
        "Replier",
        "Request",
        "Pusher",
        "Puller",
        "Publisher",
        "Subscriber",
        "FanOut",
        "Paired",
        "Surveyor",
        "Survey",
        "Respondent",
        "BusMember",
        "Radio",
        "Segment",
        "Dish",
        "DishDrops",
        "ConnectionStats",
        "TransportStats",
        "PathStats",
        "UdpCounts",
        "RemoteStats",
        "OutgoingStream",
        "IncomingStream",
        "Reply",
        "Cursors",
        "Reporter",
        "APPLICATION_FLOOR",
        "PROGRESS",
        "FINAL_ONLY",
        "sync",
        "VERSION",
        "ALPN",
        "WeidaError",
    ];
    names.extend(cursors::NAMED_LEVELS.iter().map(|(name, _)| *name));
    names.extend_from_slice(errors::NAMES);
    names
}

/// The limits both runtime constructors build: the defaults, with datagram
/// flows switched on at their documented size when `datagrams` is set, and
/// path reports listed when `path_report` is.
pub(crate) fn limits_with(datagrams: bool, path_report: bool) -> ::weida::Limits {
    let defaults = ::weida::Limits::default();
    ::weida::Limits {
        datagram_receive_bytes: if datagrams {
            ::weida::DEFAULT_DATAGRAM_RECEIVE_BYTES
        } else {
            defaults.datagram_receive_bytes
        },
        path_report,
        ..defaults
    }
}
