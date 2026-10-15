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
//! Python thread                reactor thread           delivery thread
//! ─────────────                ──────────────           ───────────────
//! sock.recv()  → a coroutine; nothing has happened yet
//! await it
//!   the coroutine starts ─ spawn ─▶ the Rust future runs, no GIL
//!   and awaits the delivery            │
//!   the loop runs other tasks          │ done ─ queue ─▶ the GIL, once per batch
//!   ◀──────────────── call_soon_threadsafe ───────────────┘
//!   the coroutine takes the value and converts it, here
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
//! # The GIL
//!
//! No reactor thread ever asks for it. A worker that finishes a future queues
//! it, and one delivery thread acquires the GIL once per batch; see
//! the `handoff` module's documentation for why, and for what it does
//! at interpreter exit.
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
//! # A value that finishes after its task was cancelled
//!
//! The abort above only reaches a future that has not finished. A receive can
//! finish on the reactor, and have the message it took on its way to the loop,
//! in the same moment the task is cancelled; the cancellation then lands on a
//! future that has already consumed something. There are two such moments and
//! both are covered:
//!
//! - the completion reaches the loop and finds the `asyncio.Future` already
//!   cancelled;
//! - the completion is delivered, and the task is cancelled before the
//!   coroutine has run again — which asyncio answers by raising
//!   `CancelledError` at the `await` instead of returning what the future
//!   holds.
//!
//! Neither can be told apart from the Rust side except by ownership, so the
//! value is not converted until the coroutine takes it: the future is
//! completed with a small box that holds it, `take` converts it, and a box
//! that is dropped without having been taken — however it came to be — hands
//! the value to the closure of [`Bridge::awaitable_restoring`]. A binding
//! whose operation consumed something it cannot read twice (a message, a
//! delivery, a stream chunk) passes a closure that puts it back; for one that
//! consumed nothing worth keeping, [`Bridge::awaitable`] drops it, and says so.
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
//!   written down here rather than in a comment nobody reads. A value the
//!   future produced goes back through its restore closure, if it has one.
//! - **No running loop.** The coroutine's first step asks for one, so a
//!   coroutine created outside a loop and awaited inside one is fine, and one
//!   awaited nowhere never asks. Only `asyncio.run`'s own rules apply.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::task::{Context as TaskContext, Poll, Waker};

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use weida_runtime::Exec;

use crate::errors::{Errno, ErrnoMapper};
use crate::handoff::{self, Finished, Settle};

/// What a bridged future may hand back to Python.
///
/// Implemented for everything PyO3 can convert — `()`, `bool`, `Vec<u8>`,
/// `String`, a `#[pyclass]`, a tuple of those — with the two bounds the bridge
/// needs on top: the value crosses a thread (`Send`) and outlives the call
/// that made it (`'static`), because it is produced on a reactor thread after
/// the Python call has already returned.
pub trait PyValue: Send + 'static {
    /// Converts under the GIL, on the loop's own thread, when the awaiting
    /// coroutine takes the value.
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

/// What a closure given to [`Bridge::awaitable_restoring`] is.
type Restore<T> = Box<dyn FnOnce(T) + Send>;

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
    ///
    /// **A value the task never took is dropped.** That is right for an
    /// operation that consumed nothing it cannot recreate — a send, a
    /// connect, a read of state. An operation that took a message off a queue
    /// and returns it uses [`awaitable_restoring`](Bridge::awaitable_restoring).
    pub fn awaitable<'py, F, T>(&self, py: Python<'py>, future: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = Result<T, Errno>> + Send + 'static,
        T: PyValue,
    {
        self.begin(py, future, None)
    }

    /// [`awaitable`](Bridge::awaitable) for an operation that consumed
    /// something, with the way to give it back.
    ///
    /// `restore` is called with the value the future produced if the awaiting
    /// task is cancelled before it has taken it — see the module
    /// documentation for the two moments that covers. It is called at most
    /// once, never for a value the task did take, and **on any thread, with
    /// or without the GIL**: it must not block, and must not touch a Python
    /// object. Pushing the value onto a queue under a `std` mutex is what it
    /// is for; [`Slot::restore`](crate::Slot::restore) is exactly that.
    ///
    /// A value is also given back if the loop was closed before it could be
    /// delivered, and if the interpreter is exiting.
    pub fn awaitable_restoring<'py, F, T>(
        &self,
        py: Python<'py>,
        future: F,
        restore: impl FnOnce(T) + Send + 'static,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = Result<T, Errno>> + Send + 'static,
        T: PyValue,
    {
        self.begin(py, future, Some(Box::new(restore)))
    }

    fn begin<'py, F, T>(
        &self,
        py: Python<'py>,
        future: F,
        restore: Option<Restore<T>>,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = Result<T, Errno>> + Send + 'static,
        T: PyValue,
    {
        let exec = self.exec.clone();
        let errno = self.errno;
        let start = Start {
            begin: Mutex::new(Some(Box::new(move |py| {
                start(py, &exec, errno, future, restore)
            }))),
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
///
/// A value produced inline needs no restore closure: the coroutine awaits an
/// already-done future, which does not yield, so nothing can cancel it
/// between here and the value being taken.
fn start<F, T>(
    py: Python<'_>,
    exec: &Exec,
    errno: ErrnoMapper,
    future: F,
    restore: Option<Restore<T>>,
) -> PyResult<Py<PyAny>>
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

    handoff::prepare(py)?;
    let target = delivery.clone().unbind();
    let notify = event_loop.unbind();
    let task = exec.spawn(async move {
        let outcome = guarded.await;
        handoff::submit(Finished {
            event_loop: notify,
            awaitable: target,
            settle: Box::new(Pending {
                outcome,
                errno,
                restore,
            }),
        });
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
            .unwrap_or_else(PoisonError::into_inner)
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

/// The three-line `async def` that turns a [`Start`] into a coroutine.
///
/// Compiled once per process, on the first `await` of any binding, and kept
/// here rather than per [`Bridge`]: it closes over nothing, so one copy serves
/// every module. A few lines of Python beat forty lines of Rust emulating
/// `Future.__await__`'s `_asyncio_future_blocking` protocol, which is
/// CPython-internal and would have to be re-checked against every release.
///
/// What it awaits is either the converted value (a future that finished on
/// its first poll) or a [`Delivered`], which it asks for the value in the same
/// step in which it resumes — there is no `await` between the two, so there is
/// no place for a cancellation to land and leave the box unread.
///
/// It is compiled into a private dictionary rather than a module, so nothing
/// appears in `sys.modules` that a caller did not import.
fn wrapper(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static WRAPPER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let wrapper = WRAPPER.get_or_try_init(py, || {
        let namespace = PyDict::new(py);
        namespace.set_item(intern!(py, "Delivered"), py.get_type::<Delivered>())?;
        py.run(
            c"async def awaited(start):\n    \
                  result = await start()\n    \
                  if result.__class__ is Delivered:\n        \
                      return result.take()\n    \
                  return result\n",
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
        Err(panicked) => Err(panic_error(&panicked)),
    }
}

/// What a panic in a bridged future is to Python.
fn panic_error(panicked: &Panicked) -> PyErr {
    PyRuntimeError::new_err(format!("the Rust future panicked: {}", panicked.0))
}

/// Whether `awaitable` is done, which for a future the loop completes means it
/// was cancelled.
fn is_done(py: Python<'_>, awaitable: &Bound<'_, PyAny>) -> PyResult<bool> {
    awaitable.call_method0(intern!(py, "done"))?.is_truthy()
}

/// A finished future, waiting on the loop's thread to be settled.
struct Pending<T: PyValue> {
    outcome: Result<Result<T, Errno>, Panicked>,
    errno: ErrnoMapper,
    restore: Option<Restore<T>>,
}

impl<T: PyValue> Settle for Pending<T> {
    fn settle(self: Box<Self>, py: Python<'_>, awaitable: &Bound<'_, PyAny>) -> PyResult<()> {
        let Pending {
            outcome,
            errno,
            restore,
        } = *self;
        match outcome {
            Ok(Ok(value)) => {
                // From here the value is owned by `taken`, so every way out of
                // this function — the future already cancelled, a Python call
                // failing — ends in its `Drop`, which hands the value back.
                let taken = Taken {
                    value: Some(value),
                    restore,
                };
                if is_done(py, awaitable)? {
                    return Ok(());
                }
                let delivered = Py::new(
                    py,
                    Delivered {
                        claim: Mutex::new(Some(Box::new(taken))),
                    },
                )?;
                awaitable.call_method1(intern!(py, "set_result"), (delivered,))?;
                Ok(())
            }
            // A failure is not something a binding restores, and a task that
            // was cancelled has nobody to tell.
            Ok(Err(failure)) => {
                if is_done(py, awaitable)? {
                    return Ok(());
                }
                let error = errno(py, &failure);
                awaitable.call_method1(intern!(py, "set_exception"), (error.into_value(py),))?;
                Ok(())
            }
            Err(panicked) => {
                if is_done(py, awaitable)? {
                    return Ok(());
                }
                let error = panic_error(&panicked);
                awaitable.call_method1(intern!(py, "set_exception"), (error.into_value(py),))?;
                Ok(())
            }
        }
    }
}

/// What the Rust future produced, held until Python takes it or gives it back.
struct Taken<T: PyValue> {
    value: Option<T>,
    restore: Option<Restore<T>>,
}

impl<T: PyValue> Drop for Taken<T> {
    fn drop(&mut self) {
        if let (Some(value), Some(restore)) = (self.value.take(), self.restore.take()) {
            restore(value);
        }
    }
}

/// [`Taken`] without its type parameter, so that one Python class can hold any.
trait Claim: Send {
    fn claim(self: Box<Self>, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl<T: PyValue> Claim for Taken<T> {
    fn claim(mut self: Box<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Taken out, so the `Drop` above finds nothing to give back: the value
        // is Python's from this line on, converted or failing to convert.
        let value = self.value.take().expect("a value is taken once");
        value.into_python(py)
    }
}

/// What the delivery future holds: the value, not yet converted.
///
/// A future completed with this rather than with the converted value is what
/// lets a value whose task was cancelled be recognised by ownership: if the
/// coroutine never calls [`take`](Delivered::take), this is dropped, and
/// dropping it hands the value to the restore closure.
#[pyclass(frozen)]
struct Delivered {
    claim: Mutex<Option<Box<dyn Claim>>>,
}

#[pymethods]
impl Delivered {
    /// Converts the value, on the loop's thread under the GIL.
    fn take(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let claim = self
            .claim
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match claim {
            Some(claim) => claim.claim(py),
            None => Err(PyRuntimeError::new_err("this value was already taken")),
        }
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
