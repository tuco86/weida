//! The context: the reactor, the `inproc://` namespace and the socket
//! ceiling.
//!
//! NNG has no object like this — `nng_socket` is a bare handle and the
//! reactor, the `inproc` namespace and the resource ceiling are global to the
//! process. The library makes all three explicit, and all three constructors
//! mean something to a Python process:
//!
//! | Python | Rust | What it is for |
//! | --- | --- | --- |
//! | `Context()` / `Context.owned()` | `Context::owned` | The default, and the only one a plain Python program can use: the context owns a reactor sized by `worker_threads`, and no asyncio loop drives it. |
//! | `Context.current()` | `Context::new` | The *ambient* Tokio reactor. A plain Python process has none, so this raises `ESTATE` — the honest answer rather than a hidden second reactor. It is reachable when this module is imported into a process a Rust host already runs a reactor in. |
//! | `Context.sharing(other)` | `Context::with_handle` | A second context on the first one's reactor: two `inproc://` namespaces and two ceilings over one thread pool. |

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyType;
use weida_nng::{ContextConfig, Error};
use weida_py_core::Bridge;

use crate::errors;

/// `weida_nng.Context`.
#[pyclass(frozen, name = "Context", module = "weida_nng")]
pub struct Context {
    inner: weida_nng::Context,
    bridge: Bridge,
}

impl Context {
    /// The library context every socket of this module is opened on.
    pub fn inner(&self) -> &weida_nng::Context {
        &self.inner
    }

    /// The reactor and errno mapping every socket of this context inherits.
    pub fn bridge(&self) -> &Bridge {
        &self.bridge
    }

    fn wrap(inner: weida_nng::Context) -> Context {
        let bridge = Bridge::new(inner.exec().clone(), errors::to_py);
        Context { inner, bridge }
    }
}

/// Builds the library's configuration, refusing a value where it is
/// configured rather than rounding it into something usable.
fn config(
    max_sockets: Option<usize>,
    worker_threads: Option<usize>,
    close_budget: Option<f64>,
) -> Result<ContextConfig, Error> {
    let mut config = ContextConfig::default();
    if let Some(max_sockets) = max_sockets {
        config.max_sockets = max_sockets;
    }
    if let Some(worker_threads) = worker_threads {
        config.worker_threads = worker_threads;
    }
    if let Some(seconds) = close_budget {
        config.close_budget = seconds_to_duration("close_budget", seconds)?;
    }
    Ok(config)
}

/// A number of seconds as a duration, or `EINVAL` naming the value. The
/// check is the shared one: `NaN`, an infinity, a negative number and a
/// value past `MAX_SECONDS` are refused rather than panicking.
pub(crate) fn seconds_to_duration(what: &str, seconds: f64) -> Result<Duration, Error> {
    weida_py_core::duration(what, seconds).map_err(|e| Error::EINVAL(e.to_string().into()))
}

#[pymethods]
impl Context {
    /// A context that owns its reactor: `Context::owned`.
    ///
    /// `close_budget` is how long `shutdown` waits for this context's
    /// sockets, in seconds, and it is finite by default (one second) where
    /// `nng_close` waits for its own internal drain.
    #[new]
    #[pyo3(signature = (*, max_sockets=None, worker_threads=None, close_budget=None))]
    fn new(
        py: Python<'_>,
        max_sockets: Option<usize>,
        worker_threads: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        let config = errors::raise(py, config(max_sockets, worker_threads, close_budget))?;
        errors::raise(py, weida_nng::Context::owned(config)).map(Context::wrap)
    }

    /// The same as `Context(...)`, spelled for a reader who wants to see
    /// which of the three constructors is in use.
    #[classmethod]
    #[pyo3(signature = (*, max_sockets=None, worker_threads=None, close_budget=None))]
    fn owned(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        max_sockets: Option<usize>,
        worker_threads: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        Context::new(py, max_sockets, worker_threads, close_budget)
    }

    /// A context on the **ambient** Tokio reactor: `Context::new`.
    ///
    /// A plain Python process has none and this raises there.
    /// `worker_threads` is not taken, because the reactor was sized by
    /// whoever created it.
    #[classmethod]
    #[pyo3(signature = (*, max_sockets=None, close_budget=None))]
    fn current(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        max_sockets: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        let config = errors::raise(py, config(max_sockets, None, close_budget))?;
        errors::raise(py, weida_nng::Context::new(config)).map(Context::wrap)
    }

    /// A second context on `other`'s reactor: `Context::with_handle`.
    ///
    /// Two `inproc://` namespaces and two ceilings over one thread pool,
    /// which is what a process wants when it isolates two subsystems from
    /// each other's names without paying for a second thread pool.
    #[classmethod]
    #[pyo3(signature = (other, *, max_sockets=None, close_budget=None))]
    fn sharing(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        other: &Context,
        max_sockets: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        let config = errors::raise(py, config(max_sockets, None, close_budget))?;
        // `Exec` hands out no handle, so the handle is taken the way Tokio
        // hands one out: from inside the runtime's context, which
        // `Exec::enter` is for. The guard is held across nothing but this
        // call.
        let handle = {
            let _entered = other.inner.exec().enter();
            tokio::runtime::Handle::current()
        };
        errors::raise(py, weida_nng::Context::with_handle(handle, config)).map(Context::wrap)
    }

    /// How many sockets this context may hold at once.
    #[getter]
    fn max_sockets(&self) -> usize {
        self.inner.config().max_sockets
    }

    /// The reactor's worker threads.
    #[getter]
    fn worker_threads(&self) -> usize {
        self.inner.config().worker_threads
    }

    /// How long `shutdown` waits, in seconds.
    #[getter]
    fn close_budget(&self) -> f64 {
        self.inner.config().close_budget.as_secs_f64()
    }

    /// Sockets currently open on this context.
    #[getter]
    fn socket_count(&self) -> usize {
        self.inner.socket_count()
    }

    /// Whether [`shutdown`](Context::shutdown) has begun.
    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Admits no further socket, then waits at most `close_budget` for the
    /// open ones to be closed.
    ///
    /// Returns how many sockets were still open when the budget ran out — a
    /// number to log or retry against, not an exception, because a peer that
    /// will not go away is not this call's failure.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        self.bridge.awaitable(
            py,
            async move { Ok(context.shutdown().await.outstanding()) },
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nng.Context sockets={} max_sockets={}{}>",
            self.inner.socket_count(),
            self.inner.config().max_sockets,
            if self.inner.is_closed() {
                " closed"
            } else {
                ""
            }
        )
    }
}
