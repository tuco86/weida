//! How a finished Rust future reaches the loop that awaits it, without a
//! reactor worker ever waiting for the GIL.
//!
//! # The problem
//!
//! `loop.call_soon_threadsafe` is a Python call, so something must hold the GIL
//! to make it. If that something is the reactor worker that just finished the
//! future, the worker parks until the interpreter hands the GIL over — up to
//! the 5 ms switch interval per completion while another Python thread is busy
//! — and a worker that is parked is not driving the connections the reactor
//! exists for. With one worker (the default of the `weida` binding) that is
//! all of its I/O: acknowledgements, keep-alives and every other connection
//! stall behind each completion.
//!
//! # The shape
//!
//! A worker that finishes a future **queues** it and goes back to its work. One
//! dedicated thread, started the first time a future has to wait, takes the
//! queue, acquires the GIL **once for everything that is queued** and hands
//! each finished future to its loop. Completions for one loop travel in one
//! `call_soon_threadsafe`, so a burst costs one loop wakeup rather than one per
//! future.
//!
//! The conversion to a Python object does not happen here. It happens when the
//! awaiting coroutine takes the value, on the loop's own thread, where the GIL
//! is held anyway — and where a task that was cancelled in the meantime can
//! still be told to give the value back (see [`crate::Bridge::awaitable_restoring`]).
//!
//! # Interpreter exit
//!
//! A thread that asks for the GIL after the interpreter has begun to finalize
//! is terminated by CPython (or hangs, by version), and a reactor thread that
//! completes a future at exit is exactly such a thread. So the hand-over is
//! behind a [`Gate`]: the first time anything is queued, an `atexit` function
//! is registered, and when it runs it closes the gate and waits — with the GIL
//! released, so a hand-over in progress can finish — until nothing is inside.
//! From then on a finished future is dropped rather than delivered, which is
//! the only thing that can be done for a loop that is about to be torn down.
//! The delivery thread also attaches with [`Python::try_attach`], which
//! declines once Python reports that it is shutting down.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyRuntimeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyCFunction;

/// How many finished futures one acquisition of the GIL hands over at most.
///
/// Bounds how long the delivery thread holds the interpreter against the
/// loop's own thread when completions arrive faster than they are handed
/// over: past this it lets go and takes the rest in the next round.
const MAX_BATCH: usize = 256;

/// How long the exit function waits for a hand-over in progress.
///
/// A hand-over is a few Python calls per finished future, so this is generous;
/// it exists so that an interpreter exit cannot hang on a delivery thread that
/// is stuck behind something it cannot see.
const QUIESCE_LIMIT: Duration = Duration::from_secs(2);

/// What the loop does with one finished future, once it is on the loop's thread.
pub(crate) trait Settle: Send {
    /// Completes `awaitable`, or — where it was cancelled first — disposes of
    /// what the future produced. Runs with the GIL held, on the loop's thread.
    fn settle(self: Box<Self>, py: Python<'_>, awaitable: &Bound<'_, PyAny>) -> PyResult<()>;
}

/// One finished future, on its way to the loop that awaits it.
pub(crate) struct Finished {
    /// The loop that created `awaitable`, and the only one that may complete it.
    pub event_loop: Py<PyAny>,
    /// The `asyncio.Future` the coroutine awaits.
    pub awaitable: Py<PyAny>,
    /// What to do with it there.
    pub settle: Box<dyn Settle>,
}

/// Starts the delivery thread and registers the exit function, once.
///
/// Called with the GIL held, by the first future that has to wait, so a
/// refusal by the operating system to start a thread reaches the Python caller
/// as an exception instead of a completion that never comes.
pub(crate) fn prepare(py: Python<'_>) -> PyResult<()> {
    match queue() {
        Ok(_) => {}
        Err(why) => {
            return Err(PyRuntimeError::new_err(format!(
                "the bridge could not start its delivery thread: {why}"
            )));
        }
    }
    static EXIT_HOOK: PyOnceLock<()> = PyOnceLock::new();
    EXIT_HOOK.get_or_try_init(py, || {
        let quiesce = PyCFunction::new_closure(
            py,
            Some(c"weida_py_core_quiesce"),
            Some(c"Stops handing finished futures to event loops; registered with atexit."),
            |args, _kwargs| -> PyResult<()> {
                GATE.close();
                args.py().detach(|| GATE.wait_until_idle(QUIESCE_LIMIT));
                Ok(())
            },
        )?;
        py.import(intern!(py, "atexit"))?
            .call_method1(intern!(py, "register"), (quiesce,))?;
        Ok::<(), PyErr>(())
    })?;
    Ok(())
}

/// Queues a finished future for the delivery thread. Callable from any thread,
/// with or without the GIL, and never blocks.
///
/// If the thread is gone — the interpreter is exiting — the future is dropped,
/// which is the right thing to do with it: see the module documentation.
pub(crate) fn submit(finished: Finished) {
    if let Ok(queue) = queue() {
        // A send fails only when the receiver has gone, and the value comes
        // back inside the error to be dropped here.
        let _ = queue.send(finished);
    }
}

/// The queue's sending half, creating the thread behind it on first use.
fn queue() -> Result<&'static Sender<Finished>, &'static str> {
    static QUEUE: LazyLock<Result<Sender<Finished>, String>> = LazyLock::new(|| {
        let (sender, receiver) = mpsc::channel();
        std::thread::Builder::new()
            .name("weida-py-handoff".to_owned())
            .spawn(move || run(&receiver))
            .map(|_| sender)
            .map_err(|why| why.to_string())
    });
    QUEUE.as_ref().map_err(String::as_str)
}

/// The delivery thread: take everything queued, hand it over, wait.
fn run(queue: &Receiver<Finished>) {
    while let Ok(first) = queue.recv() {
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match queue.try_recv() {
                Ok(next) => batch.push(next),
                Err(_) => break,
            }
        }
        hand_over(batch);
    }
}

/// Hands a batch to the loops, or drops it where Python is going away.
fn hand_over(batch: Vec<Finished>) {
    let Some(_inside) = GATE.enter() else {
        return;
    };
    // `try_attach`, not `attach`: where Python reports that it is shutting
    // down the batch is dropped instead of asking a dying interpreter for the
    // GIL.
    let _ = Python::try_attach(|py| {
        let mut batch = batch.into_iter().peekable();
        while let Some(first) = batch.next() {
            let event_loop = first.event_loop;
            let mut entries = vec![Entry {
                awaitable: first.awaitable,
                settle: first.settle,
            }];
            // Consecutive completions for one loop share one wakeup.
            while let Some(next) =
                batch.next_if(|next| next.event_loop.as_ptr() == event_loop.as_ptr())
            {
                entries.push(Entry {
                    awaitable: next.awaitable,
                    settle: next.settle,
                });
            }
            schedule(py, &event_loop, entries);
        }
    });
}

/// Asks `event_loop` to run the completions on its own thread.
fn schedule(py: Python<'_>, event_loop: &Py<PyAny>, entries: Vec<Entry>) {
    let completion = match Py::new(
        py,
        Completion {
            entries: Mutex::new(entries),
        },
    ) {
        Ok(completion) => completion,
        // Allocating one object failed, which means the interpreter is out of
        // memory; it has bigger problems than this future. The entries went
        // with the failed value and were disposed of by their own `Drop`.
        Err(err) => return err.write_unraisable(py, None),
    };
    if let Err(err) = event_loop
        .bind(py)
        .call_method1(intern!(py, "call_soon_threadsafe"), (completion,))
    {
        // A closed or stopped loop raises `RuntimeError` here. Nothing awaits
        // the futures in that case — the loop that would run the await is gone
        // — so dropping the error is the whole of the handling, and it cannot
        // hang anybody. The completion dropped with the failed call, and with
        // it the values, which went back through their restore hooks.
        if !err.is_instance_of::<PyRuntimeError>(py) {
            err.write_unraisable(py, None);
        }
    }
}

/// One finished future inside a [`Completion`].
struct Entry {
    awaitable: Py<PyAny>,
    settle: Box<dyn Settle>,
}

/// Runs on the loop's thread, the only place asyncio permits a future to be
/// completed, and settles every future of one hand-over.
#[pyclass(frozen)]
struct Completion {
    /// Taken on the call; a `Mutex` because a `#[pyclass]` is shared and
    /// `__call__` takes `&self`. Never held across a call into Python.
    entries: Mutex<Vec<Entry>>,
}

#[pymethods]
impl Completion {
    fn __call__(&self, py: Python<'_>) {
        let entries =
            std::mem::take(&mut *self.entries.lock().unwrap_or_else(PoisonError::into_inner));
        for entry in entries {
            let awaitable = entry.awaitable.bind(py);
            // One future failing to settle must not strand the rest of the
            // batch: report it where Python reports an exception nobody can
            // catch, and go on.
            if let Err(err) = entry.settle.settle(py, awaitable) {
                err.write_unraisable(py, Some(awaitable));
            }
        }
    }
}

/// The one gate every hand-over passes through, and the exit function closes.
static GATE: Gate = Gate::new();

/// Admits hand-overs until it is closed, and counts the ones inside.
///
/// The counter goes up **before** the flag is read and the exit function sets
/// the flag **before** it reads the counter, with sequentially consistent
/// ordering on all four: either the exit function sees the hand-over and waits
/// for it, or the hand-over sees the flag and does not start. There is no
/// interleaving in which both miss each other.
struct Gate {
    closed: AtomicBool,
    inside: AtomicUsize,
}

impl Gate {
    const fn new() -> Gate {
        Gate {
            closed: AtomicBool::new(false),
            inside: AtomicUsize::new(0),
        }
    }

    /// Admission, or `None` once the gate is closed.
    fn enter(&self) -> Option<Inside<'_>> {
        self.inside.fetch_add(1, Ordering::SeqCst);
        let inside = Inside { gate: self };
        if self.closed.load(Ordering::SeqCst) {
            // Dropping `inside` takes the count back out.
            return None;
        }
        Some(inside)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Waits until nothing is inside, or `limit` passes. Returns whether it
    /// is idle. Meant to be called with the GIL released, because a
    /// hand-over in progress may be waiting for it.
    fn wait_until_idle(&self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while self.inside.load(Ordering::SeqCst) != 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
    }
}

/// One hand-over inside the gate.
struct Inside<'a> {
    gate: &'a Gate,
}

impl Drop for Inside<'_> {
    fn drop(&mut self) {
        self.gate.inside.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_closed_gate_admits_nobody_and_leaves_no_count_behind() {
        let gate = Gate::new();
        gate.close();
        assert!(gate.enter().is_none());
        assert!(gate.wait_until_idle(Duration::from_millis(5)), "no leak");
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        #[test]
        fn an_open_gate_admits_and_counts() {
            let gate = Gate::new();
            let first = gate.enter().expect("open");
            let second = gate.enter().expect("open");
            assert!(!gate.wait_until_idle(Duration::from_millis(5)));
            drop(first);
            drop(second);
            assert!(gate.wait_until_idle(Duration::from_millis(5)));
        }
        #[test]
        fn closing_waits_for_the_hand_over_already_inside() {
            let gate = Gate::new();
            let inside = gate.enter().expect("open");
            gate.close();
            std::thread::scope(|scope| {
                let waiter = scope.spawn(|| gate.wait_until_idle(Duration::from_secs(5)));
                std::thread::sleep(Duration::from_millis(20));
                drop(inside);
                assert!(waiter.join().expect("the waiter finished"));
            });
            assert!(gate.enter().is_none(), "and nobody enters afterwards");
        }
    }
}
