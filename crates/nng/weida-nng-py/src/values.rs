//! The values a call answers with: a message body, and what a broadcast
//! became.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use weida_nng::{Broadcast, Message};

/// A message body on its way to Python.
///
/// A bare `Vec<u8>` would arrive as a list of integers, which is what PyO3
/// does with a vector of anything. A payload is `bytes` in both languages,
/// so the conversion is named here once and every `recv` returns it.
///
/// It holds the message rather than a copy of its body: the body is a
/// shared allocation on the Rust side, and the one copy this conversion
/// makes is into the `bytes` object Python owns.
pub struct Body(pub Message);

impl Body {
    /// The library's message, for a receive that has to give it back.
    pub fn into_inner(self) -> Message {
        self.0
    }
}

impl<'py> IntoPyObject<'py> for Body {
    type Target = PyBytes;
    type Output = Bound<'py, PyBytes>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyBytes::new(py, self.0.body()))
    }
}

/// What a broadcast became: how many peers took a copy, and how many could
/// not.
///
/// PUB, BUS and SURVEYOR are best effort — "delivery may reach some, all,
/// or none" — and NNG's own send reports success either way. This is the
/// only place the second number exists.
#[pyclass(frozen, get_all, name = "Broadcast", module = "weida_nng")]
pub struct PyBroadcast {
    /// Copies queued for a peer.
    pub queued: usize,
    /// Copies discarded because a peer could not take one.
    pub dropped: usize,
}

impl PyBroadcast {
    /// The library's own report.
    pub fn of(broadcast: Broadcast) -> PyBroadcast {
        PyBroadcast {
            queued: broadcast.queued,
            dropped: broadcast.dropped,
        }
    }
}

#[pymethods]
impl PyBroadcast {
    fn __repr__(&self) -> String {
        format!(
            "<weida_nng.Broadcast queued={} dropped={}>",
            self.queued, self.dropped
        )
    }
}
