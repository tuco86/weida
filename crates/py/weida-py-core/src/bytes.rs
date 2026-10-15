//! The payload boundary: what crosses as `bytes`, and what it costs.
//!
//! A messaging library's payload is opaque bytes in both languages, so the
//! boundary is pure plumbing — and the plumbing is where a binding quietly
//! pays for the same bytes three times. The rule
//! [0014](../../../docs/decisions/0014-parallel-libraries.md) §2 sets is: no
//! copy where PyO3 allows none, and the copy *measured* where it does not.
//! Both halves are here.
//!
//! # Where no copy happens
//!
//! Python to Rust, PyO3 hands out a borrow of the interpreter's own buffer:
//! [`PyBytes::as_bytes`](pyo3::types::PyBytesMethods::as_bytes) is a pointer
//! into the `bytes` object, and [`payload`] hands back a
//! [`PyBackedBytes`], which is that borrow plus
//! a reference count — no bytes are touched. A binding that only *reads* a
//! payload (a subscription prefix to match, a key to check, a length to bound)
//! copies nothing at all.
//!
//! # Where exactly one copy happens, and why it is not avoidable
//!
//! - **Python to Rust, when the library takes ownership.** `weida-zmq`'s
//!   `Message` — and every other frame type in this workspace — owns a
//!   `Vec<u8>`, because a frame outlives the call that queued it and is sent by
//!   a reactor thread long after. CPython's allocation cannot become a Rust
//!   one, so [`payload_of`] copies once, straight out of the interpreter's
//!   buffer into the `Vec` the library keeps. Not twice: no intermediate
//!   `PyBackedBytes`, no `to_vec` of a `to_vec`.
//! - **Rust to Python.** A CPython `bytes` object owns its storage inside its
//!   own allocation and there is no API — limited or otherwise — that adopts a
//!   foreign buffer. [`py_bytes`] therefore uses
//!   [`PyBytes::new`](pyo3::types::PyBytes::new), which allocates the object
//!   and copies the frame into it in one step. A `memoryview` over
//!   Rust-owned bytes would avoid the copy, and is deliberately **not** used
//!   here: the buffer protocol is not part of the limited API before CPython
//!   3.11, and the wheel is `abi3` from 3.9 — a zero-copy path that only
//!   exists on some interpreters is a performance cliff, not a feature.
//!
//! `benches/bytes_boundary.rs` measures both directions at 0 B, 64 B, 1 KiB
//! and 1 MiB, and the numbers go into
//! [IMPLEMENTATION.md](../../../docs/IMPLEMENTATION.md) rather than into a
//! sentence claiming the copy is cheap.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyByteArray, PyBytes};

/// Borrows a Python payload without copying it.
///
/// `bytes` is borrowed through its reference count; `bytearray` — which is
/// mutable, so a borrow would be a dangling promise — is copied once by PyO3
/// into a `Box<[u8]>`. Anything else is a `TypeError` naming what is accepted:
/// a `str` is refused rather than encoded, because guessing an encoding for
/// somebody else's wire format is how mojibake gets sent.
pub fn payload(object: &Bound<'_, PyAny>) -> PyResult<PyBackedBytes> {
    object
        .extract::<PyBackedBytes>()
        .map_err(|_| wrong_type(object))
}

/// Takes a Python payload as owned bytes: the one copy an owning frame type
/// forces, and no second one.
pub fn payload_of(object: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = object.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    if let Ok(array) = object.cast::<PyByteArray>() {
        return Ok(array.to_vec());
    }
    Err(wrong_type(object))
}

/// Hands `payload` to Python as a `bytes` object.
///
/// One copy, into CPython's own allocation. See the module documentation for
/// why zero is not on the menu.
pub fn py_bytes<'py>(py: Python<'py>, payload: &[u8]) -> Bound<'py, PyBytes> {
    PyBytes::new(py, payload)
}

/// A `bytes` object made from Rust-owned octets the first time Python asks for
/// it, and the same object every time after.
///
/// A getter that calls [`py_bytes`] copies the payload on **every access**, and
/// `if m.payload: ... len(m.payload) ... use(m.payload)` is three copies of a
/// body that already costs one. This is the cell a `#[pyclass]` keeps beside
/// the field it converts instead: the first [`get`](LazyBytes::get) makes the
/// object, and the rest are a reference-count increment. Identity holds too —
/// `m.payload is m.payload` — which is also what a caller expects of an
/// immutable value.
///
/// The Rust octets stay where they are. A message that arrived as `bytes` and
/// is only ever forwarded never pays for the conversion, which is why the cell
/// is lazy rather than filled at construction.
pub struct LazyBytes {
    cell: PyOnceLock<Py<PyBytes>>,
}

impl Default for LazyBytes {
    fn default() -> LazyBytes {
        LazyBytes::new()
    }
}

impl LazyBytes {
    /// An empty cell.
    pub const fn new() -> LazyBytes {
        LazyBytes {
            cell: PyOnceLock::new(),
        }
    }

    /// A cell already holding `object`, for a payload that arrived from Python
    /// as `bytes`: handing the same object back is no copy at all.
    pub fn seeded(object: Bound<'_, PyBytes>) -> LazyBytes {
        let cell = PyOnceLock::new();
        // A fresh cell cannot already be set.
        let _ = cell.set(object.py(), object.unbind());
        LazyBytes { cell }
    }

    /// The `bytes` object for `octets`, copied into it on the first call.
    ///
    /// `octets` must be the same on every call: only the first is read.
    pub fn get<'py>(&self, py: Python<'py>, octets: &[u8]) -> Bound<'py, PyBytes> {
        self.cell
            .get_or_init(py, || py_bytes(py, octets).unbind())
            .bind(py)
            .clone()
    }
}

/// The one error message this module has, so that both entry points refuse the
/// same things in the same words.
fn wrong_type(object: &Bound<'_, PyAny>) -> PyErr {
    let name = object
        .get_type()
        .name()
        .and_then(|name| name.extract::<String>())
        .unwrap_or_else(|_| "an unnameable object".to_owned());
    PyTypeError::new_err(format!(
        "a payload must be bytes or bytearray, not {name}; encode a str yourself, \
         because a wire format's encoding is not this library's to guess"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_and_bytearray_cross_and_a_str_does_not() {
        Python::initialize();
        Python::attach(|py| {
            let bytes = PyBytes::new(py, b"frame").into_any();
            assert_eq!(payload(&bytes).unwrap().as_ref(), b"frame");
            assert_eq!(payload_of(&bytes).unwrap(), b"frame");

            let array = PyByteArray::new(py, b"frame").into_any();
            assert_eq!(payload(&array).unwrap().as_ref(), b"frame");
            assert_eq!(payload_of(&array).unwrap(), b"frame");

            let text = pyo3::types::PyString::new(py, "frame").into_any();
            let refused = payload_of(&text).expect_err("a str is not a payload");
            assert!(refused.is_instance_of::<PyTypeError>(py));
            assert!(payload(&text).is_err());
        });
    }

    #[test]
    fn an_empty_payload_is_a_payload() {
        Python::initialize();
        Python::attach(|py| {
            let empty = PyBytes::new(py, b"").into_any();
            assert!(payload_of(&empty).unwrap().is_empty());
            assert_eq!(py_bytes(py, b"").len().unwrap(), 0);
        });
    }
}
