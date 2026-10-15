//! Two vocabularies, one exception family.
//!
//! AMQP has more to say about a failure than an errno does, and a Python
//! caller needs both halves of it:
//!
//! * **what this client did** — a handshake step that timed out, a
//!   configuration it refused, a peer that spoke a version it does not know.
//!   One class per [`weida_amqp::Error`] variant.
//! * **what the peer said** — an `amqp:...` *error condition*, carried in
//!   `close`, `end`, `detach` or a `rejected` outcome. One class per condition
//!   the specification names, so that `except weida_amqp.LinkStolen` is a
//!   branch a program can write.
//!
//! Both derive from `weida_amqp.AmqpError` and both carry `errno` — the class
//! name — and `cause`. A condition class additionally answers `condition()`
//! with the symbol as it appeared on the wire, because the symbol is what a
//! broker's documentation is written in and a Python name cannot contain a
//! colon.
//!
//! **Neither table can drift.** [`name_of`] is a `match` over
//! [`weida_amqp::Error`] written by the same macro that writes the variant
//! name list, so a variant added to the library and not added here is a
//! compile error in this file. The condition table is generated from the
//! codec's own `condition` constants in the same way, and a test asserts that
//! every constant the codec exports has a class here.

use pyo3::prelude::*;
use weida_amqp::Error;
use weida_amqp_codec::types::condition;
use weida_py_core::{Errno, ErrorClasses};

/// The module's exception classes, built once at import.
static ERRORS: ErrorClasses = ErrorClasses::new();

/// The base class every failure of this module derives from.
const BASE: &str = "AmqpError";

/// Writes the variant name list and the exhaustive match from one source.
macro_rules! variants {
    ($($name:ident),+ $(,)?) => {
        /// Every client-side failure name this module has a class for.
        const VARIANTS: &[&str] = &[$(stringify!($name)),+];

        /// The class name of one error.
        ///
        /// Exhaustive, with no wildcard: [`weida_amqp::Error`] is not
        /// `#[non_exhaustive]` for exactly this reason, so a variant added
        /// upstream and not added here is a **compile error** in this crate
        /// rather than a failure that quietly arrives in Python as the base
        /// class.
        fn variant_of(error: &Error) -> &'static str {
            match error {
                $(Error::$name { .. } => stringify!($name),)+
            }
        }
    };
}

variants!(
    Io,
    Runtime,
    Decode,
    Encode,
    BadProtocolHeader,
    SecurityLayerRequired,
    VersionMismatch,
    Sasl,
    NoSharedSaslMechanism,
    Tls,
    Closed,
    Local,
    IdleTimeout,
    WriteTimeout,
    HandshakeTimeout,
    ConnectionGone,
    Configuration,
);

/// Writes the condition table: the Python class name, and the symbol it stands
/// for, from one source.
macro_rules! conditions {
    ($($name:ident => $symbol:expr),+ $(,)?) => {
        /// Every condition class name, in the order Part 2 §2.8.15-§2.8.18
        /// and Part 4 §4.5.8 give them.
        const CONDITIONS: &[(&str, &str)] = &[$((stringify!($name), $symbol)),+];
    };
}

// The names are the symbol's own words in Python's spelling: a class cannot be
// called `amqp:link:stolen`, and `LinkStolen` is the name every broker's
// documentation would recognise. The connection, session and link prefixes are
// kept, because `amqp:connection:forced` and `amqp:link:detach-forced` are
// different conditions with the same English word in them.
conditions!(
    InternalError => condition::INTERNAL_ERROR,
    NotFound => condition::NOT_FOUND,
    UnauthorizedAccess => condition::UNAUTHORIZED_ACCESS,
    DecodeError => condition::DECODE_ERROR,
    ResourceLimitExceeded => condition::RESOURCE_LIMIT_EXCEEDED,
    NotAllowed => condition::NOT_ALLOWED,
    InvalidField => condition::INVALID_FIELD,
    NotImplemented => condition::NOT_IMPLEMENTED,
    ResourceLocked => condition::RESOURCE_LOCKED,
    PreconditionFailed => condition::PRECONDITION_FAILED,
    ResourceDeleted => condition::RESOURCE_DELETED,
    IllegalState => condition::ILLEGAL_STATE,
    FrameSizeTooSmall => condition::FRAME_SIZE_TOO_SMALL,
    ConnectionForced => condition::CONNECTION_FORCED,
    ConnectionFramingError => condition::CONNECTION_FRAMING_ERROR,
    ConnectionRedirect => condition::CONNECTION_REDIRECT,
    SessionWindowViolation => condition::SESSION_WINDOW_VIOLATION,
    SessionErrantLink => condition::SESSION_ERRANT_LINK,
    SessionHandleInUse => condition::SESSION_HANDLE_IN_USE,
    SessionUnattachedHandle => condition::SESSION_UNATTACHED_HANDLE,
    LinkDetachForced => condition::LINK_DETACH_FORCED,
    LinkTransferLimitExceeded => condition::LINK_TRANSFER_LIMIT_EXCEEDED,
    LinkMessageSizeExceeded => condition::LINK_MESSAGE_SIZE_EXCEEDED,
    LinkRedirect => condition::LINK_REDIRECT,
    LinkStolen => condition::LINK_STOLEN,
    TransactionUnknownId => condition::TRANSACTION_UNKNOWN_ID,
    TransactionRollback => condition::TRANSACTION_ROLLBACK,
    TransactionTimeout => condition::TRANSACTION_TIMEOUT,
);

/// Every class name this module installs: the client's own failures and the
/// specification's conditions.
fn names() -> Vec<&'static str> {
    VARIANTS
        .iter()
        .copied()
        .chain(CONDITIONS.iter().map(|(name, _)| *name))
        .collect()
}

/// The Python class name for a condition symbol, or `None` for a symbol
/// outside the specification's table — a broker's own `com.microsoft:...`, for
/// instance, which reaches Python as the base class with the symbol in
/// `cause`.
fn class_of_condition(symbol: &str) -> Option<&'static str> {
    CONDITIONS
        .iter()
        .find(|(_, known)| *known == symbol)
        .map(|(name, _)| *name)
}

/// Creates the classes and adds them to the module. Called once, at import.
pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    ERRORS.install(module, BASE, &names())?;
    // The symbol table, so a caller can map a condition it caught back to the
    // name a broker's documentation uses without hard-coding the spelling.
    let table = pyo3::types::PyDict::new(module.py());
    for (name, symbol) in CONDITIONS {
        table.set_item(name, symbol)?;
    }
    module.add("CONDITIONS", table)?;
    Ok(())
}

/// The protocol-neutral form the bridge carries: the class name and why.
///
/// Called on a reactor thread, with no GIL held — which is why it does not
/// build the exception itself.
///
/// The **condition wins** where the error carries one. `Error::Closed` and
/// `Error::Local` are how a condition reaches a caller, and reporting them as
/// `Closed` would throw away the only part a program can branch on; a caller
/// who wants the coarse distinction catches `AmqpError`.
pub fn errno_of(error: Error) -> Errno {
    let cause = error.to_string();
    let name = match &error {
        Error::Closed(Some(condition)) | Error::Local(condition) => {
            class_of_condition(&condition.condition).unwrap_or_else(|| variant_of(&error))
        }
        other => variant_of(other),
    };
    Errno::new(name, cause)
}

/// Turns an [`Errno`] into this module's exception. The
/// [`ErrnoMapper`](weida_py_core::ErrnoMapper) every
/// [`Bridge`](weida_py_core::Bridge) here holds.
///
/// One name is not a failure at all: [`STOP_ASYNC_ITERATION`], which the end
/// of a delivery stream uses. An `async for` that ends is not an error, and
/// Python spells the end of an async iteration with an exception, so this is
/// where the two meet.
pub fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
    if errno.name() == STOP_ASYNC_ITERATION {
        return pyo3::exceptions::PyStopAsyncIteration::new_err(errno.cause().to_owned());
    }
    ERRORS.error(py, errno)
}

/// The marker name that ends an `async for` rather than raising.
pub const STOP_ASYNC_ITERATION: &str = "StopAsyncIteration";

/// The end of a stream, as the bridge carries it.
pub fn end_of_stream(what: &str) -> Errno {
    Errno::new(STOP_ASYNC_ITERATION, what.to_owned())
}

/// Raises a library error from a synchronous call.
pub fn raise<T>(py: Python<'_>, result: Result<T, Error>) -> PyResult<T> {
    result.map_err(|error| to_py(py, &errno_of(error)))
}

/// A refusal this binding makes itself, in the library's own vocabulary.
///
/// Used where a Python argument cannot become a library call at all — a
/// settle-mode name that is not one of the three. `Configuration` is the
/// library's own variant for "refused where it was configured", so the class a
/// caller catches is the same one the library would have raised.
pub fn configuration(py: Python<'_>, cause: impl Into<String>) -> PyErr {
    to_py(py, &Errno::new("Configuration", cause.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every condition the codec exports has a class here. The codec's
    /// `condition` module is the specification's table; a constant added there
    /// and not here would reach Python as the base class, which is the
    /// silent-drift failure this test exists to prevent.
    #[test]
    fn every_condition_the_codec_names_has_a_class() {
        // The codec has no iterable list of its condition constants — they are
        // `pub const`s — so the assertion is the other direction: every symbol
        // this table names is one the codec exports, and the count matches the
        // number of constants in `types::condition`. Both halves are needed:
        // the first catches a typo, the second catches an omission.
        for (name, symbol) in CONDITIONS {
            assert!(
                symbol.starts_with("amqp:"),
                "{name} maps to {symbol}, which is not an AMQP condition"
            );
        }
        assert_eq!(
            CONDITIONS.len(),
            28,
            "the codec's `condition` module exports 28 constants: 13 general, \
             3 connection, 4 session, 5 link and 3 transaction"
        );
    }

    #[test]
    fn a_condition_beats_the_variant_that_carried_it() {
        let errno = errno_of(Error::Local(weida_amqp::Condition::described(
            condition::LINK_STOLEN,
            "attached again",
        )));
        assert_eq!(errno.name(), "LinkStolen");
        assert!(errno.cause().contains("attached again"));
    }

    #[test]
    fn a_vendor_condition_falls_back_to_the_variant() {
        let errno = errno_of(Error::Closed(Some(weida_amqp::Condition::described(
            "com.microsoft:timeout",
            "the operator says so",
        ))));
        assert_eq!(
            errno.name(),
            "Closed",
            "a symbol outside the specification's table has no class of its own"
        );
        assert!(errno.cause().contains("com.microsoft:timeout"));
    }

    #[test]
    fn a_client_side_failure_keeps_its_variant_name() {
        let errno = errno_of(Error::HandshakeTimeout { step: "open" });
        assert_eq!(errno.name(), "HandshakeTimeout");
        let errno = errno_of(Error::ConnectionGone);
        assert_eq!(errno.name(), "ConnectionGone");
    }

    #[test]
    fn the_two_tables_do_not_collide() {
        for (name, _) in CONDITIONS {
            assert!(
                !VARIANTS.contains(name),
                "{name} is both a variant name and a condition name, so one \
                 class would shadow the other"
            );
        }
    }
}
