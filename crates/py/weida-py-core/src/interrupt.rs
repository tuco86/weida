//! A blocking wait that Ctrl-C can end.
//!
//! # The problem
//!
//! Python runs signal handlers on the **main thread, between bytecodes**. A
//! main thread parked inside Rust never returns to the interpreter, so
//! `SIGINT` (`KeyboardInterrupt`), `signal.alarm`, a `SIGTERM` handler and
//! `faulthandler`'s timeouts all wait for the call to finish — and a
//! synchronous `recv()` on a peer that went quiet does not finish. The process
//! is then unkillable short of `SIGKILL`.
//!
//! # The shape
//!
//! [`interruptible`] drives a future on the calling thread with the GIL
//! released, as a binding's blocking surface already does, and on the **main
//! thread only** wakes every [`SLICE`] to ask Python whether a signal is
//! pending. If a handler raised, the wait ends with that exception, unchanged,
//! and the future is dropped; if it returned, the wait goes on.
//!
//! The future is polled **across** the slices, never recreated. That matters
//! for two reasons. A library receive that carries its own deadline
//! (`NNG_OPT_RECVTIMEO`, a request window) would start that deadline again at
//! every slice if each slice were a fresh call, and a request or a publish
//! would be sent again. And it keeps the question of cancel safety down to the
//! one place it is asked: **when a signal ends the wait**. That drops the
//! future mid-operation, which is what cancelling an `asyncio.Task` does to the
//! same future on the asynchronous surface, so every future that surface can
//! be cancelled in can be waited for here.
//!
//! A thread that is not the main thread cannot receive a Python signal, so it
//! parks until the future wakes it and pays nothing for the slices.

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::{Duration, Instant};

use pyo3::intern;
use pyo3::prelude::*;
use weida_runtime::Exec;

/// How long the main thread waits between asking Python for pending signals.
///
/// The latency of Ctrl-C while blocked, and the cost of it: ten wakeups a
/// second on one thread, each a brief re-attach to the interpreter.
pub const SLICE: Duration = Duration::from_millis(100);

/// Waits for the future `work` builds, with the GIL released, and lets Python's
/// signal handlers run while it waits.
///
/// `work` runs **inside** the released section, on the waiting thread, which is
/// what lets it take a lock or create a timer there without holding the GIL
/// across either. `exec`'s runtime context is entered for the duration, the
/// way a library future that arms a timer or opens a socket needs.
///
/// # Errors
///
/// Whatever Python's signal handlers raise — `KeyboardInterrupt` for the
/// default `SIGINT` — and nothing else: the future's own output, success or
/// failure, is the `Ok` value.
///
/// # Cancel safety
///
/// A signal that raises ends the wait by dropping the future where it stands.
/// Pass only futures that can be dropped there; see the module documentation.
pub fn interruptible<F, T>(
    py: Python<'_>,
    exec: &Exec,
    work: impl FnOnce() -> F + Send,
) -> PyResult<T>
where
    F: Future<Output = T>,
    T: Send,
{
    let main = on_main_thread(py);
    py.detach(move || {
        let _entered = exec.enter();
        let mut future = pin!(work());
        let parker = Arc::new(Parker {
            thread: std::thread::current(),
            woken: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&parker));
        let mut cx = Context::from_waker(&waker);
        let mut next_check = Instant::now() + SLICE;
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return Ok(output);
            }
            // Parked until the future's waker fires or, on the main thread,
            // until a slice is up. `park` returns spuriously, so the flag is
            // what says a wake happened.
            while !parker.woken.swap(false, Ordering::Acquire) {
                if !main {
                    std::thread::park();
                    continue;
                }
                let now = Instant::now();
                if now >= next_check {
                    break;
                }
                std::thread::park_timeout(next_check - now);
            }
            if main && Instant::now() >= next_check {
                // Brief, and only here: the thread is otherwise detached, so
                // a program full of threads blocked in `recv()` does not queue
                // for the GIL ten times a second apiece.
                Python::attach(|py| py.check_signals())?;
                next_check = Instant::now() + SLICE;
            }
        }
    })
}

/// Whether Python would run a signal handler on this thread.
///
/// `PyErr_CheckSignals` is a no-op anywhere but the main thread of the main
/// interpreter, so a thread that is not it has nothing to check for. If the
/// question cannot be answered the answer is "yes": checking needlessly costs a
/// re-attach, and not checking costs the feature.
fn on_main_thread(py: Python<'_>) -> bool {
    let answer = || -> PyResult<bool> {
        let threading = py.import(intern!(py, "threading"))?;
        let current = threading.call_method0(intern!(py, "current_thread"))?;
        let main = threading.call_method0(intern!(py, "main_thread"))?;
        Ok(current.is(&main))
    };
    answer().unwrap_or(true)
}

/// Wakes the thread that is waiting on a future.
struct Parker {
    thread: Thread,
    woken: AtomicBool,
}

impl Wake for Parker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// A future that is ready on its `n`th poll and wakes itself until then.
    struct After {
        polls_left: usize,
        seen: Arc<AtomicUsize>,
    }

    impl Future for After {
        type Output = &'static str;

        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.seen.fetch_add(1, Ordering::SeqCst);
            if self.polls_left == 0 {
                return Poll::Ready("done");
            }
            self.polls_left -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    fn exec() -> (Exec, weida_runtime::OwnedReactor) {
        Exec::owned(1, "interrupt-test").expect("a reactor")
    }

    #[test]
    fn a_ready_future_returns_its_output() {
        Python::initialize();
        let (exec, _reactor) = exec();
        Python::attach(|py| {
            let out = interruptible(py, &exec, || async { 41 + 1 }).expect("no signal pending");
            assert_eq!(out, 42);
        });
    }

    #[test]
    fn a_future_that_wakes_itself_is_polled_until_it_is_ready() {
        Python::initialize();
        let (exec, _reactor) = exec();
        let seen = Arc::new(AtomicUsize::new(0));
        Python::attach(|py| {
            let counter = Arc::clone(&seen);
            let out = interruptible(py, &exec, move || After {
                polls_left: 3,
                seen: counter,
            })
            .expect("no signal pending");
            assert_eq!(out, "done");
        });
        assert_eq!(seen.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn a_thread_that_is_not_main_does_not_ask_for_signals() {
        Python::initialize();
        // A thread of its own is never the main thread, which is the point;
        // the test's own thread can be (nextest runs one test per process,
        // on its main thread).
        std::thread::spawn(|| Python::attach(|py| assert!(!on_main_thread(py))))
            .join()
            .expect("the assertion holds");
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        #[test]
        fn a_future_woken_from_another_thread_ends_the_wait() {
            Python::initialize();
            let (exec, _reactor) = exec();
            // A reactor task finishes after the waiting thread has parked.
            let sleeper = exec.clone();
            let joined = exec.spawn(async move {
                sleeper.sleep(Duration::from_millis(30)).await;
                9u8
            });
            Python::attach(|py| {
                let out = interruptible(py, &exec, move || joined).expect("no signal pending");
                assert_eq!(out.expect("the task finished"), 9);
            });
        }
    }
}
