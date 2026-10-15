//! libzmq's errno vocabulary, as this module's exception classes.
//!
//! `weida_zmq.EAGAIN`, `weida_zmq.EFSM`, `weida_zmq.ETERM` and sixteen more,
//! each its own class under `weida_zmq.ZmqError`, each instance carrying
//! `errno` and `cause`. A caller writes the branch the zguide's recipes are
//! made of — `except EAGAIN` for a timeout, `except EFSM` for a REQ socket out
//! of step — instead of matching on the text of a `RuntimeError`.
//!
//! **The table cannot drift.** [`name_of`] is a `match` over
//! [`weida_zmq::Error`] written by the same macro that writes the name list,
//! so a variant added to the library that is not added here is a compile
//! error in this file rather than an errno that silently arrives as the base
//! class.

use pyo3::prelude::*;
use weida_py_core::{Errno, ErrorClasses, Retired};
use weida_zmq::Error;

/// The module's exception classes, built once at import.
static ERRORS: ErrorClasses = ErrorClasses::new();

/// The base class every errno of this module derives from.
const BASE: &str = "ZmqError";

/// Writes the errno list and the exhaustive match from one source.
macro_rules! errnos {
    ($($name:ident),+ $(,)?) => {
        /// Every errno name this module has a class for.
        pub(crate) const NAMES: &[&str] = &[$(stringify!($name)),+];

        /// The errno name of one error, checked against the library's enum by
        /// the compiler.
        fn name_of(error: &Error) -> &'static str {
            match error {
                $(Error::$name(_) => stringify!($name),)+
            }
        }
    };
}

errnos!(
    EAGAIN,
    EFSM,
    ETERM,
    EHOSTUNREACH,
    EMFILE,
    EINVAL,
    EPROTONOSUPPORT,
    ENOCOMPATPROTO,
    EACCES,
    EADDRINUSE,
    EADDRNOTAVAIL,
    ENOTSOCK,
    EINTR,
    EMTHREAD,
    EMSGSIZE,
    ENOTSUP,
    ETIMEDOUT,
    ENOENT,
    EIO,
);

/// Creates the classes and adds them to the module. Called once, at import.
pub fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    ERRORS.install(module, BASE, NAMES)
}

/// The protocol-neutral form the bridge carries: the errno name and why.
///
/// Called on a reactor thread, with no GIL held — which is why it does not
/// build the exception itself.
pub fn errno_of(error: Error) -> Errno {
    Errno::new(name_of(&error), error.cause())
}

/// What a call on a socket that `split()` consumed gets: `ENOTSOCK`, naming
/// the split, because the socket lives on in its halves and this object has
/// nothing left to lease.
pub fn retired(_: Retired) -> Errno {
    Errno::new(
        "ENOTSOCK",
        "this socket was split; use the halves split() returned",
    )
}

/// Turns an [`Errno`] into this module's exception. The
/// [`ErrnoMapper`](weida_py_core::ErrnoMapper) every [`Bridge`] here holds.
///
/// One name is not an errno and not an error: the marker
/// [`STOP_ASYNC_ITERATION`](crate::monitor::STOP_ASYNC_ITERATION), which a
/// monitor's end uses. An `async for` that ends is not a failure, and Python
/// spells the end of an async iteration with an exception, so this is where
/// the two meet. Every other name is looked up in the family.
pub fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
    if errno.name() == crate::monitor::STOP_ASYNC_ITERATION {
        return pyo3::exceptions::PyStopAsyncIteration::new_err(errno.cause().to_owned());
    }
    ERRORS.error(py, errno)
}

/// Raises a library error from a synchronous call.
pub fn raise<T>(py: Python<'_>, result: Result<T, Error>) -> PyResult<T> {
    result.map_err(|error| to_py(py, &errno_of(error)))
}
