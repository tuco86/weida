//! The bridge: a Rust future the caller's asyncio loop can await.
//!
//! # The shape of the problem
//!
//! A library here owns its reactor ([`weida_runtime::Exec`]), and a Python
//! caller owns an event loop. Neither may drive the other: the asyncio loop
//! cannot poll a Tokio future, and a Tokio worker must not block on Python.
//! So the two are joined by the one object asyncio already has for "a result
//! that arrives later from somewhere else" — a `loop.create_future()` —
//! completed through `loop.call_soon_threadsafe`, which is asyncio's only
//! thread-safe entry point.
//!
//! ```text
//! Python thread                    reactor thread
//! ─────────────                    ──────────────
//! sock.recv()        → a coroutine; nothing has happened yet
//! await it
//!   the coroutine starts ─ spawn ─▶ the Rust future runs, no GIL
//!   and awaits the delivery                │
//!   the loop runs other tasks              │ done
//!   ◀── call_soon_threadsafe ───────────────┘ (GIL taken once, briefly)
//!   set_result / set_exception
//! ```
//!
//! The coroutine is the outer layer for two reasons: `asyncio.create_task`
//! and `TaskGroup.create_task` accept a coroutine and refuse a bare future,
//! and a coroutine is what defers the work to the first `await` — see
//! [`Bridge::awaitable`].
//!
//! # Why not `pyo3-async-runtimes`
//!
//! That crate solves the same problem, and solves it the other way round: it
//! owns a Tokio runtime (or is handed the one global runtime through
//! `tokio::runtime::Builder`) and asks the library to use *its* reactor. Every
//! library in this workspace already owns a reactor — that is what
//! [`weida_runtime::Exec`]'s three constructors are for, and the point of
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.2 — and two
//! runtimes in one process, one per binding, is the cost of not saying so. This
//! module is the replacement: it drives futures on the
//! *library's* `Exec` and adds no runtime of its own.
//!
//! # Cancellation
//!
//! A Python `Task` that is cancelled while awaiting cancels the future it
//! waits on, and a cancelled `asyncio.Future` runs its done-callbacks. This
//! module registers one: it aborts the task driving the Rust future, so the
//! Rust future is **dropped** at its next suspension point. In Rust that *is*
//! cancellation — the `recv` releases the socket it borrowed, the queue keeps
//! the message it had not yet taken, and the socket is usable for the next
//! call. Nothing is swallowed: a binding that ignored the callback would leave
//! a Rust task running for a result no one will read.
//!
//! # Failure modes that are not hangs
//!
//! - **The future panics.** It is polled inside `catch_unwind`, so the panic
//!   becomes a `RuntimeError` on the Python future rather than a task that
//!   dies with nobody awaiting it. A future that panicked and told no one is
//!   the hang [LOOP.md](../../../docs/LOOP.md) §2 forbids.
//! - **The loop is closed** before the result arrives. `call_soon_threadsafe`
//!   raises, and there is nothing left to tell: a closed loop has no task
//!   awaiting anything. The error is dropped deliberately and the reason is
//!   written down here rather than in a comment nobody reads.
//! - **No running loop.** The coroutine's first step asks for one, so a
//!   coroutine created outside a loop and awaited inside one is fine, and one
//!   awaited nowhere never asks. Only `asyncio.run`'s own rules apply.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context as TaskContext, Poll, Waker};

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use weida_runtime::Exec;

use crate::errors::{Errno, ErrnoMapper};

/// What a bridged future may hand back to Python.
///
/// Implemented for everything PyO3 can convert — `()`, `bool`, `Vec<u8>`,
/// `String`, a `#[pyclass]`, a tuple of those — with the two bounds the bridge
/// needs on top: the value crosses a thread (`Send`) and outlives the call
/// that made it (`'static`), because it is produced on a reactor thread after
/// the Python call has already returned.
pub trait PyValue: Send + 'static {
    /// Converts under the GIL, on the loop's own thread.
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl<T> PyValue for T
where
    T: for<'py> IntoPyObjectExt<'py> + Send + 'static,
{
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.into_py_any(py)
    }
}

/// The library's reactor and its error vocabulary, in the one object a binding
/// hands to every socket it creates.
///
/// Cloning is two pointer copies: an [`Exec`] is a runtime handle and an
/// [`ErrnoMapper`] is a function pointer.
#[derive(Clone)]
pub struct Bridge {
    exec: Exec,
    errno: ErrnoMapper,
}

impl Bridge {
    /// A bridge onto `exec`, reporting failures through `errno`.
    pub fn new(exec: Exec, errno: ErrnoMapper) -> Bridge {
        Bridge { exec, errno }
    }

    /// The reactor every future handed to this bridge runs on.
    pub fn exec(&self) -> &Exec {
        &self.exec
    }

    /// How this bridge turns a library errno into a Python exception.
    pub fn errno_mapper(&self) -> ErrnoMapper {
        self.errno
    }

    /// Hands Python a coroutine that drives `future` on the library's
    /// reactor.
    ///
    /// **A coroutine rather than a bare `asyncio.Future`**, because asyncio's
    /// own entry points are not interchangeable: `await` and `asyncio.gather`
    /// take either, while `asyncio.create_task` and `TaskGroup.create_task`
    /// take a coroutine and nothing else. A binding whose calls cannot be
    /// handed to a `TaskGroup` would be excluded from the way Python 3.11
    /// onward writes concurrency, for the sake of one object allocation.
    ///
    /// **The Rust future starts at the first `await`, not at this call.** A
    /// coroutine that is never awaited therefore does nothing at all, which
    /// is what Python's own `async def` promises and what makes
    /// `task = create_task(sock.recv()); task.cancel()` safe: with an eager
    /// start, that pair cancels a `Task` that never ran while the receive it
    /// already began takes the next message off the socket and throws it
    /// away. The delivery future is created by the loop that runs the
    /// coroutine, so it is also the loop that awaits it, and this call needs
    /// no running loop of its own.
    ///
    /// The GIL is not held while the future runs, and the future holds no
    /// Python object, so a binding cannot accidentally touch the interpreter
    /// from a reactor thread.
    pub fn awaitable<'py, F, T>(&self, py: Python<'py>, future: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = Result<T, Errno>> + Send + 'static,
        T: PyValue,
    {
        let exec = self.exec.clone();
        let errno = self.errno;
        let start = Start {
            begin: Mutex::new(Some(Box::new(move |py| start(py, &exec, errno, future)))),
        };
        wrapper(py)?.call1((Py::new(py, start)?,))
    }
}

/// Creates the delivery future on the running loop and starts the work.
///
/// Runs inside the coroutine, so "the running loop" is the loop that will
/// await the result.
///
/// **One poll inline first.** A receive with a message already queued or a
/// send with room in the queue is ready on its first poll, and for those the
/// delivery future is completed here, on the loop's thread: `await` of an
/// already-done future returns without yielding, so the common case of a
/// busy program costs no reactor hop and no `call_soon_threadsafe` wakeup
/// (measured at ~78 µs in IMPLEMENTATION.md §4.15). Only a future that is genuinely
/// waiting is spawned. The poll runs inside the reactor's context, so a
/// future that creates a timer on its first step finds one, and with a
/// no-op waker: a future left pending is polled again the moment its task
/// starts, which re-registers the task's real waker.
fn start<F, T>(py: Python<'_>, exec: &Exec, errno: ErrnoMapper, future: F) -> PyResult<Py<PyAny>>
where
    F: Future<Output = Result<T, Errno>> + Send + 'static,
    T: PyValue,
{
    let event_loop = py
        .import(intern!(py, "asyncio"))?
        .call_method0(intern!(py, "get_running_loop"))?;
    let delivery = event_loop.call_method0(intern!(py, "create_future"))?;

    let mut guarded = Guarded::new(future);
    let first = {
        let _reactor = exec.enter();
        let mut cx = TaskContext::from_waker(Waker::noop());
        Pin::new(&mut guarded).poll(&mut cx)
    };
    if let Poll::Ready(outcome) = first {
        match resolve(py, outcome, errno) {
            Ok(value) => delivery.call_method1(intern!(py, "set_result"), (value,))?,
            Err(err) => {
                delivery.call_method1(intern!(py, "set_exception"), (err.into_value(py),))?
            }
        };
        return Ok(delivery.unbind());
    }

    let target = delivery.clone().unbind();
    let notify = event_loop.unbind();
    let task = exec.spawn(async move {
        let outcome = guarded.await;
        deliver(notify, target, outcome, errno);
    });

    // The done-callback fires for *any* completion; only a cancellation has
    // anything to abort, and aborting a task that already finished is a
    // no-op.
    let cancellation = Py::new(
        py,
        Cancellation {
            abort: Box::new(move || task.abort()),
        },
    )?;
    delivery.call_method1(intern!(py, "add_done_callback"), (cancellation,))?;
    Ok(delivery.unbind())
}

/// The coroutine's first step: begin the operation, once.
#[pyclass(frozen)]
struct Start {
    /// Taken by the first call. A second one cannot happen — a coroutine
    /// cannot be awaited twice — but a `Mutex<Option<_>>` says so in the type
    /// rather than in a comment.
    #[allow(clippy::type_complexity)]
    begin: Mutex<Option<Box<dyn for<'py> FnOnce(Python<'py>) -> PyResult<Py<PyAny>> + Send>>>,
}

#[pymethods]
impl Start {
    fn __call__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let begin = self
            .begin
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match begin {
            Some(begin) => begin(py),
            None => Err(PyRuntimeError::new_err(
                "this operation was already started; a coroutine is awaited once",
            )),
        }
    }
}

impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bridge").field("exec", &self.exec).finish()
    }
}

/// The two-line `async def` that turns a [`Start`] into a coroutine.
///
/// Compiled once per process, on the first `await` of any binding, and kept
/// here rather than per [`Bridge`]: it closes over nothing, so one copy serves
/// every module. Two lines of Python beat forty lines of Rust emulating
/// `Future.__await__`'s `_asyncio_future_blocking` protocol, which is
/// CPython-internal and would have to be re-checked against every release.
///
/// It is compiled into a private dictionary rather than a module, so nothing
/// appears in `sys.modules` that a caller did not import.
fn wrapper(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static WRAPPER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let wrapper = WRAPPER.get_or_try_init(py, || {
        let namespace = PyDict::new(py);
        py.run(
            c"async def awaited(start):\n    return await start()\n",
            Some(&namespace),
            None,
        )?;
        namespace
            .get_item(intern!(py, "awaited"))?
            .ok_or_else(|| {
                PyRuntimeError::new_err("the bridge's coroutine wrapper failed to compile")
            })
            .map(Bound::unbind)
    })?;
    Ok(wrapper.bind(py))
}

/// The outcome as Python sees it: the value converted, the errno mapped, a
/// panic named.
fn resolve<T: PyValue>(
    py: Python<'_>,
    outcome: Result<Result<T, Errno>, Panicked>,
    errno: ErrnoMapper,
) -> PyResult<Py<PyAny>> {
    match outcome {
        Ok(Ok(value)) => value.into_python(py),
        Ok(Err(failure)) => Err(errno(py, &failure)),
        Err(panicked) => Err(PyRuntimeError::new_err(format!(
            "the Rust future panicked: {}",
            panicked.0
        ))),
    }
}

/// Hands the outcome to the loop that created the future.
///
/// Runs on a reactor thread with no GIL held: it takes the GIL once, for the
/// conversion and the `call_soon_threadsafe`, and gives it back.
fn deliver<T: PyValue>(
    event_loop: Py<PyAny>,
    awaitable: Py<PyAny>,
    outcome: Result<Result<T, Errno>, Panicked>,
    errno: ErrnoMapper,
) {
    Python::attach(|py| {
        let resolved = resolve(py, outcome, errno);
        let completion = match Py::new(
            py,
            Completion {
                awaitable,
                outcome: Mutex::new(Some(resolved)),
            },
        ) {
            Ok(completion) => completion,
            // Allocating one object failed, which means the interpreter is out
            // of memory; it has bigger problems than this future.
            Err(err) => return err.write_unraisable(py, None),
        };
        if let Err(err) = event_loop
            .bind(py)
            .call_method1(intern!(py, "call_soon_threadsafe"), (completion,))
        {
            // A closed or stopped loop raises `RuntimeError` here. Nothing is
            // awaiting the future in that case — the loop that would run the
            // await is gone — so dropping the error is the whole of the
            // handling, and it cannot hang anybody.
            if !err.is_instance_of::<PyRuntimeError>(py) {
                err.write_unraisable(py, None);
            }
        }
    });
}

/// Sets the result on the loop's own thread, which is the only place asyncio
/// permits it.
#[pyclass(frozen)]
struct Completion {
    awaitable: Py<PyAny>,
    /// Taken on the first call; a `Mutex` because a `#[pyclass]` is shared and
    /// `__call__` takes `&self`. Never held across a call into Python.
    outcome: Mutex<Option<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl Completion {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let awaitable = self.awaitable.bind(py);
        // Cancelled between the reactor's `call_soon_threadsafe` and this
        // callback: the future is already done and setting a result on it is an
        // `InvalidStateError`.
        if awaitable.call_method0(intern!(py, "done"))?.is_truthy()? {
            return Ok(());
        }
        let taken = self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match taken {
            Some(Ok(value)) => {
                awaitable.call_method1(intern!(py, "set_result"), (value,))?;
            }
            Some(Err(err)) => {
                awaitable.call_method1(intern!(py, "set_exception"), (err.into_value(py),))?;
            }
            None => {}
        }
        Ok(())
    }
}

/// The done-callback that propagates a Python cancellation onto the Rust
/// future.
#[pyclass(frozen)]
struct Cancellation {
    /// `JoinHandle::abort`, in a closure rather than a field, which is what
    /// keeps `tokio` out of this crate's manifest.
    abort: Box<dyn Fn() + Send + Sync>,
}

#[pymethods]
impl Cancellation {
    fn __call__(&self, awaitable: &Bound<'_, PyAny>) -> PyResult<()> {
        if awaitable
            .call_method0(intern!(awaitable.py(), "cancelled"))?
            .is_truthy()?
        {
            (self.abort)();
        }
        Ok(())
    }
}

/// What a panic left behind, as words.
struct Panicked(String);

/// Polls a future inside `catch_unwind`, so that a panic is a result rather
/// than a task nobody hears from.
struct Guarded<F> {
    inner: Pin<Box<F>>,
}

impl<F: Future> Guarded<F> {
    fn new(future: F) -> Guarded<F> {
        Guarded {
            inner: Box::pin(future),
        }
    }
}

impl<F: Future> Future for Guarded<F> {
    type Output = Result<F::Output, Panicked>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // `Pin<Box<F>>` is `Unpin` whatever `F` is, so this needs no `unsafe`
        // and no projection crate.
        let guarded = self.as_mut().get_mut();
        let inner = guarded.inner.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(Panicked(panic_message(panic)))),
        }
    }
}

/// The message a panic carried, for the two payload types `panic!` produces.
fn panic_message(panic: Box<dyn Any + Send>) -> String {
    match panic.downcast::<String>() {
        Ok(message) => *message,
        Err(panic) => match panic.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "a panic with no message".to_owned(),
        },
    }
}
