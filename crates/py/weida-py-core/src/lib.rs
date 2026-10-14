//! What every Python binding in this workspace shares, and no protocol in it.
//!
//! Each of the five libraries of
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) gets a Python
//! binding, and `weida` has its own; the six bindings differ only in which
//! protocol they speak.
//! Everything else — how a Rust errno becomes a Python exception, how a Rust
//! future becomes something a coroutine can `await`, what a cancelled
//! `asyncio.Task` does to the Rust future underneath, and what a payload costs
//! on the way across — is the same problem six times, so it is solved here
//! once ([0014](../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! `[dependencies]` is `pyo3` and `weida-runtime`. **`weida`,
//! `weida-protocol` and every protocol library are absent and must stay
//! absent**: this crate is the foundation of a ZeroMQ binding *and* of an MQTT
//! one, and a foundation that knew either could not be both. It is the same
//! dependency-direction rule 0013 §4.2 gave `weida-runtime`, one layer up.
//!
//! # The four things
//!
//! - [`Errno`] and [`ErrorFamily`] — a library's errno-named enum becomes one
//!   distinct Python exception class per variant under a single base class, so
//!   Python code writes `except EAGAIN` where C code reads `errno`, and
//!   `except ZmqError` catches the lot. Nothing is flattened into
//!   `RuntimeError`, which is the whole point: an errno vocabulary that
//!   arrives as one class is an errno vocabulary a caller cannot branch on.
//! - [`Bridge`] — the `Runtime`/`Context` bridge. It holds the library's
//!   [`Exec`](weida_runtime::Exec) and turns a Rust future into an awaitable
//!   resolved on the caller's *running* asyncio loop. The reactor stays the
//!   library's, which is why this crate does not use `pyo3-async-runtimes`:
//!   that crate owns the runtime and asks a library to register with it, and
//!   every library here already owns one through
//!   [`weida_runtime::Exec`]'s three constructors. The equivalent is
//!   [`Bridge::awaitable`], about a hundred lines, and it is named here rather
//!   than depended on.
//! - Cancellation, propagated rather than swallowed. A cancelled
//!   `asyncio.Future` aborts the task driving the Rust future, so the Rust
//!   future is *dropped* — which in Rust is what cancelling it means, and is
//!   how the socket it borrowed becomes usable again.
//! - [`bytes`] — the payload boundary, with the one copy each direction
//!   forces stated where it happens and measured in
//!   `benches/bytes_boundary.rs` rather than assumed.
//!
//! # What a binding writes
//!
//! ```no_run
//! use pyo3::prelude::*;
//! use weida_py_core::{Bridge, Errno, ErrorClasses};
//!
//! // One per module, initialised at import.
//! static ERRORS: ErrorClasses = ErrorClasses::new();
//!
//! // The mapper is a plain function pointer, so the bridge can hold it and no
//! // generic parameter or lifetime travels with it.
//! fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
//!     ERRORS.error(py, errno)
//! }
//!
//! # fn install(module: &Bound<'_, PyModule>, exec: weida_runtime::Exec) -> PyResult<Bridge> {
//! ERRORS.install(module, "ZmqError", &["EAGAIN", "EFSM", "ETERM"])?;
//! let bridge = Bridge::new(exec, to_py);
//! # Ok(bridge)
//! # }
//! ```
//!
//! and then, per method — shown as text rather than as a doctest, because the
//! socket in it belongs to a binding this crate must not depend on, and a
//! doctest the gate never compiles is a claim nobody checks:
//!
//! ```text
//! fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
//!     let socket = self.socket.clone();
//!     self.bridge.awaitable(py, async move {
//!         socket.lock().await.recv().await.map_err(errno_of)
//!     })
//! }
//! ```
//!
//! # The GIL
//!
//! Nothing here holds the GIL across an await. [`Bridge::awaitable`] returns
//! as soon as the future is spawned; the future runs on the library's reactor
//! with no Python state in it at all; the GIL is taken once, briefly, when the
//! result is converted and handed to the loop. A binding's synchronous surface
//! is the mirror image: it releases the GIL with
//! [`Python::detach`](pyo3::Python::detach) for as long as it blocks, so a
//! blocking call in one thread never stops Python in another.

#![warn(missing_docs)]

pub mod bridge;
pub mod bytes;
pub mod errors;

pub use bridge::{Bridge, PyValue};
pub use bytes::{payload, payload_of, py_bytes};
pub use errors::{Errno, ErrnoMapper, ErrorClasses, ErrorFamily};
