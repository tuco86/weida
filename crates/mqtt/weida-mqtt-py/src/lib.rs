//! MQTT 5.0 for Python, on the Rust implementation.
//!
//! `import weida_mqtt` gets a context, a client, a delivery stream and MQTT's
//! reason codes as exception classes. Underneath is `weida-mqtt` — an MQTT 5.0
//! client with no C library, no `weida` and no `weida-protocol` in it
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4) — and
//! `weida-py-core`, the PyO3 foundation every binding in this workspace shares
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! ```python
//! import asyncio
//! import weida_mqtt
//!
//! async def main():
//!     context = weida_mqtt.Context()
//!     options = weida_mqtt.ConnectOptions("sensor-1")
//!     client, events = await context.connect("127.0.0.1:1883", options)
//!
//!     granted = await client.subscribe([weida_mqtt.Subscription("room/+", 2)])
//!     assert granted == [2]                       # one code per filter
//!
//!     done = await client.publish(
//!         weida_mqtt.Message("room/12", b"21.5", qos=2)
//!     )
//!     assert done.kind == "complete"              # the PUBCOMP arrived
//!
//!     async for delivery in events:
//!         print(delivery.topic, delivery.payload, delivery.qos)
//!         break
//!
//!     await client.disconnect()
//!
//! asyncio.run(main())
//! ```
//!
//! # This is a client, and MQTT's topology is asymmetric
//!
//! There is no `weida_mqtt.Broker`, and there will not be one here: retained
//! storage, the session store, subscription routing and the Will's timer all
//! live in the server, and a server is a broker, outside the client libraries
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2). What
//! this module has is everything a client says and reads;
//! `docs/libraries/mqtt.md` §0 draws the line row by row.
//!
//! # Asyncio first
//!
//! Every call is a coroutine, driven on the reactor the context owns; nothing
//! blocks the event loop and nothing holds the GIL while waiting. A cancelled
//! task cancels the Rust future underneath — and for a publish that means the
//! *caller* stops waiting while the exchange, which is session state, survives
//! to be retransmitted on the next connection. That asymmetry is the
//! protocol's and not this binding's.
//!
//! **`weida_mqtt.sync` is the same client without a loop** — see [`sync`],
//! which is a facade over the library's own `blocking` module and implements
//! no protocol behaviour of its own. Its one difference is where it has to be:
//! a receive deadline, because a blocking caller has no task to cancel.
//!
//! # Errors
//!
//! Every failure is a class of this module, named for the MQTT reason code it
//! carries and derived from `weida_mqtt.MqttError`:
//!
//! ```python
//! try:
//!     await client.publish(weida_mqtt.Message("a", b"x", retain=True))
//! except weida_mqtt.RetainNotSupported as refused:
//!     assert refused.reason_code == 0x9A          # the byte, for a log
//!     assert "retain" in refused.cause.lower()    # the words, for a human
//! ```
//!
//! The refusal above never reaches the wire: the server declared `Retain
//! Available` 0 in CONNACK and the client answers with the code the server
//! *would* have sent. `errors` explains why one class per code rather than one
//! per packet.
//!
//! # Building it
//!
//! `crates/py/develop.sh` builds this binding into a `uv`-managed virtualenv
//! and runs its Python tests (`crates/mqtt/weida-mqtt-py/develop.sh` is its
//! wrapper).
//!
//! # What is not here
//!
//! **TLS.** The library has it behind a default-on `tls` feature and this
//! binding turns that feature *off*, because the trust anchors are the
//! caller's `rustls::ClientConfig` and this surface has no way to hand one in
//! from Python. A binding that linked a TLS stack it could not configure would
//! be linking it for nothing. `Client.is_encrypted` is therefore always
//! `False`, and says so rather than being absent.

#![warn(missing_docs)]

use pyo3::prelude::*;

mod client;
mod errors;
mod options;
// Public where the other four are not, and the reason is the documentation
// rather than the code: `sync`'s module doc is the argument for having a
// second surface at all — why it implements nothing, where the deadline goes
// and why `publish` has no timeout — and a reader of this crate's docs should
// be able to reach it. The other modules are plumbing whose public face is
// the Python classes they register.
pub mod sync;
mod values;

/// `weida_mqtt`, as Python sees it.
#[pymodule]
fn weida_mqtt(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<client::PyContext>()?;
    module.add_class::<client::PyClient>()?;
    module.add_class::<client::PyEvents>()?;
    module.add_class::<client::PySession>()?;
    module.add_class::<options::PyConnectOptions>()?;
    module.add_class::<options::PyWill>()?;
    module.add_class::<values::PyMessage>()?;
    module.add_class::<values::PyDelivery>()?;
    module.add_class::<values::PySubscription>()?;
    module.add_class::<values::PyCompletion>()?;
    sync::install(module)?;

    // The two DISCONNECT codes a client sends, named rather than left as
    // magic numbers at every call site: 0x00 discards the Will
    // ([MQTT-3.14.4-3]) and 0x04 asks for it anyway, and a caller writing
    // `client.disconnect(4)` is a caller who will have to look it up.
    module.add("NORMAL_DISCONNECTION", 0x00u8)?;
    module.add("DISCONNECT_WITH_WILL_MESSAGE", 0x04u8)?;

    // `::` because `#[pymodule]` puts a module of this function's name in
    // scope, and that name is the crate's.
    module.add(
        "DEFAULT_RECEIVE_MAXIMUM",
        ::weida_mqtt::DEFAULT_RECEIVE_MAXIMUM,
    )?;
    module.add(
        "DEFAULT_TOPIC_ALIAS_MAXIMUM",
        ::weida_mqtt::DEFAULT_TOPIC_ALIAS_MAXIMUM,
    )?;
    module.add("MAX_TOPIC_BYTES", ::weida_mqtt::MAX_TOPIC_BYTES)?;
    module.add("SHARE_PREFIX", ::weida_mqtt::SHARE_PREFIX)?;

    module.add_function(pyo3::wrap_pyfunction!(matches, module)?)?;
    module.add_function(pyo3::wrap_pyfunction!(check_topic_filter, module)?)?;
    module.add_function(pyo3::wrap_pyfunction!(check_topic_name, module)?)?;
    Ok(())
}

/// Whether `filter` selects `topic`, by MQTT's own rules.
///
/// `+` matches exactly one level and `#` matches the parent level and every
/// level below it, so `sport/#` matches bare `sport`; a filter beginning with a
/// wildcard never matches a `$`-prefixed topic ([MQTT-4.7.2-1]); and neither
/// `$share` nor a ShareName is considered.
///
/// Matching is the *server's* job, so this exists for the one case a client
/// needs it: telling which of its own filters caused a delivery where the
/// server declared `Subscription Identifiers Available` 0.
#[pyfunction]
fn matches(filter: &str, topic: &str) -> bool {
    ::weida_mqtt::matches(filter, topic)
}

/// Raises `ValueError` where `filter` is not a legal Topic Filter, naming the
/// rule it broke.
///
/// The whole grammar of 4.7.1: at least one character, at most 65,535 bytes,
/// `+` occupying a whole level, `#` last and alone in its level, and a
/// `$share/` prefix split off first so the rules apply to the filter and not
/// to the ShareName.
#[pyfunction]
fn check_topic_filter(filter: &str) -> PyResult<()> {
    ::weida_mqtt::check_topic_filter(filter)
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
}

/// Raises `ValueError` where `topic` is not a legal Topic Name.
///
/// A name is not a pattern: `+` and `#` are refused ([MQTT-3.3.2-2]), and a
/// zero-length name is legal only with an established Topic Alias (3.3.2.1),
/// which is not something a caller of this function has.
#[pyfunction]
fn check_topic_name(topic: &str) -> PyResult<()> {
    ::weida_mqtt::check_topic_name(topic, false)
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
}
