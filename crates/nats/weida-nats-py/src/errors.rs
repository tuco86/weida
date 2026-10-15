//! `weida-nats`'s failure vocabulary, as this module's exception classes.
//!
//! `weida_nats.NoResponders`, `weida_nats.RequestTimeout`,
//! `weida_nats.PayloadTooLarge` and every other variant of the library's
//! error enum, each its own class under
//! `weida_nats.NatsError`, each instance carrying `errno` and `cause`. A
//! caller writes the branch request-reply is made of — `except NoResponders`
//! for "nobody was listening, now", `except RequestTimeout` for "somebody may
//! well have been" — instead of matching on the text of a `RuntimeError`.
//!
//! **The table cannot drift.** [`name_of`] is a `match` over
//! [`weida_nats::Error`] written by the same macro that writes the name list,
//! so a variant added to the library that is not added here is a compile
//! error in this file rather than a failure that silently arrives as the base
//! class. That check is why `weida_nats::Error` is not `#[non_exhaustive]`:
//! a wildcard arm would make the drift invisible.

use pyo3::prelude::*;
use weida_nats::Error;
use weida_py_core::{Errno, ErrorClasses};

/// The module's exception classes, built once at import.
static ERRORS: ErrorClasses = ErrorClasses::new();

/// The base class every failure of this module derives from.
const BASE: &str = "NatsError";

/// Writes the name list and the exhaustive match from one source.
macro_rules! failures {
    ($($name:ident),+ $(,)?) => {
        /// Every failure name this module has a class for.
        pub(crate) const NAMES: &[&str] = &[$(stringify!($name)),+];

        /// The name of one error, checked against the library's enum by the
        /// compiler.
        ///
        /// `{ .. }` matches a unit, tuple and struct variant alike, so one
        /// arm shape serves all three and the macro needs to know nothing
        /// about a variant beyond its name.
        fn name_of(error: &Error) -> &'static str {
            match error {
                $(Error::$name { .. } => stringify!($name),)+
            }
        }
    };
}

failures!(
    Io,
    Runtime,
    Decode,
    Encode,
    Protocol,
    Server,
    AuthenticationRequired,
    NonceMissing,
    Signature,
    TlsRequired,
    TlsUnsupported,
    Tls,
    PayloadTooLarge,
    ControlLineTooLong,
    HeadersUnsupported,
    NoResponders,
    RequestTimeout,
    StaleConnection,
    HandshakeTimeout,
    WriteTimeout,
    ConnectionGone,
    TooManySubscriptions,
    TooManyPendingRequests,
    InvalidSubject,
    Configuration,
);

/// Creates the classes and adds them to the module. Called once, at import.
pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    ERRORS.install(module, BASE, NAMES)
}

/// The protocol-neutral form the bridge carries: the failure's name and why.
///
/// Called on a reactor thread, with no GIL held — which is why it does not
/// build the exception itself.
pub fn errno_of(error: Error) -> Errno {
    Errno::new(name_of(&error), error.to_string())
}

/// Turns an [`Errno`] into this module's exception. The
/// [`ErrnoMapper`](weida_py_core::ErrnoMapper) every [`Bridge`](weida_py_core::Bridge)
/// here holds.
///
/// Two names are not failures of the library and have no class in the family,
/// because Python already has the right exception for each:
///
/// * [`STOP_ASYNC_ITERATION`] — a subscription that has ended. An `async for`
///   that ends is not a failure, and Python spells the end of an async
///   iteration with an exception, so this is where the two meet.
/// * [`WOULD_BLOCK`] — a call that was told not to wait found the
///   subscription in use by another coroutine. `BlockingIOError` *is*
///   `EAGAIN` in Python, which is the same answer libzmq gives for the same
///   question.
///
/// Every other name is looked up in the family.
pub fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
    match errno.name() {
        STOP_ASYNC_ITERATION => {
            pyo3::exceptions::PyStopAsyncIteration::new_err(errno.cause().to_owned())
        }
        WOULD_BLOCK => pyo3::exceptions::PyBlockingIOError::new_err(errno.cause().to_owned()),
        _ => ERRORS.error(py, errno),
    }
}

/// The name this crate's mapper turns into `StopAsyncIteration`.
///
/// Not a variant of [`weida_nats::Error`]: it is the marker for "this
/// subscription has ended", and it exists because an iteration's end is not a
/// failure.
pub const STOP_ASYNC_ITERATION: &str = "StopAsyncIteration";

/// The name this crate's mapper turns into `BlockingIOError`.
///
/// Also not a variant: it is the marker for "this would have had to wait and
/// was told not to".
pub const WOULD_BLOCK: &str = "BlockingIOError";

/// Raises a library error from a synchronous call.
pub fn raise<T>(py: Python<'_>, result: Result<T, Error>) -> PyResult<T> {
    result.map_err(|error| to_py(py, &errno_of(error)))
}
