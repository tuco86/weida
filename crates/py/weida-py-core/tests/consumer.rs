//! The bridge-contract test: a binding that binds nothing.
//!
//! This is a `#[pyclass]` with four methods and no protocol behind it —
//! deliberately, because what is under test is the foundation and not a
//! library. It stands in for a real binding such as `weida-zmq-py` and proves
//! the three things the bridge promises, from Python, through a real asyncio loop:
//!
//! 1. an awaitable completing with a value;
//! 2. an exception arriving as its own class, carrying its errno name;
//! 3. a cancelled `asyncio.Task` cancelling the Rust future — asserted by the
//!    future's own `Drop`, which is the only honest evidence that it was
//!    dropped rather than left running for a result nobody will read.
//!
//! Two more failure modes are asserted here because a bridge that gets them
//! wrong hangs, and [LOOP.md](../../../../docs/LOOP.md) §2 forbids hanging: a
//! panicking Rust future becomes an exception, and awaiting with no running
//! loop fails at the call rather than never completing.

use std::ffi::CString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyModule;
use weida_py_core::{Bridge, Errno, ErrorClasses};
use weida_runtime::Exec;

/// The module's exception classes, exactly as a real binding declares them.
static ERRORS: ErrorClasses = ErrorClasses::new();

/// How many parked futures have been dropped. A cancellation that reached the
/// Rust future increments this; one that was swallowed does not.
static DROPPED: AtomicUsize = AtomicUsize::new(0);

/// How many futures have begun running. A coroutine that was built and never
/// awaited must leave this alone, which is what "the work starts at the first
/// await" means.
static STARTED: AtomicUsize = AtomicUsize::new(0);

/// The one function a binding writes to join its error enum to its classes.
fn to_py(py: Python<'_>, errno: &Errno) -> PyErr {
    ERRORS.error(py, errno)
}

/// Counts its own destruction, so that "the Rust future was dropped" is a
/// number Python can read rather than a claim.
struct DropFlag;

impl Drop for DropFlag {
    fn drop(&mut self) {
        DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

#[pyclass]
struct Demo {
    bridge: Bridge,
}

#[pymethods]
impl Demo {
    /// Completes with the payload it was given, after really suspending: the
    /// value arrives from a reactor thread while the loop runs other tasks.
    fn echo<'py>(&self, py: Python<'py>, payload: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let exec = self.bridge.exec().clone();
        self.bridge.awaitable(py, async move {
            STARTED.fetch_add(1, Ordering::SeqCst);
            exec.sleep(Duration::from_millis(10)).await;
            Ok(payload)
        })
    }

    /// How many futures have begun.
    fn started(&self) -> usize {
        STARTED.load(Ordering::SeqCst)
    }

    /// Fails with an errno the module has a class for.
    fn fail<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let exec = self.bridge.exec().clone();
        self.bridge.awaitable(py, async move {
            exec.sleep(Duration::from_millis(1)).await;
            Err::<(), Errno>(Errno::new("EAGAIN", "nothing to receive (ZMQ_DONTWAIT)"))
        })
    }

    /// Never completes, and says so when it is dropped.
    fn park<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.bridge.awaitable(py, async move {
            let _flag = DropFlag;
            std::future::pending::<()>().await;
            Ok(())
        })
    }

    /// Panics on a reactor thread.
    fn boom<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let exec = self.bridge.exec().clone();
        self.bridge.awaitable(py, async move {
            exec.sleep(Duration::from_millis(1)).await;
            panic!("a future that panicked");
            #[allow(unreachable_code)]
            Ok(())
        })
    }

    /// How many parked futures have been dropped so far.
    fn dropped(&self) -> usize {
        DROPPED.load(Ordering::SeqCst)
    }
}

/// Every assertion, in the language the binding is for.
const SCRIPT: &str = r#"
import asyncio

# Nothing happens before the first await: the call outside a loop builds a
# coroutine and starts no work, so the operation is never half-begun behind a
# task that was cancelled before it ran.
pending = demo.echo(b"nowhere")
assert demo.started() == 0, "the work must not begin until the coroutine runs"
pending.close()
assert demo.started() == 0

async def main():
    # 1. An awaitable completing, with the payload it was given.
    assert await demo.echo(b"ping") == b"ping"
    assert await demo.echo(b"") == b""

    # 2. The exception class, its base and the errno name it carries.
    try:
        await demo.fail()
        raise AssertionError("fail() must raise")
    except EAGAIN as expected:
        assert isinstance(expected, DemoError), "every errno derives from the base"
        assert expected.errno == "EAGAIN", expected.errno
        assert expected.cause == "nothing to receive (ZMQ_DONTWAIT)", expected.cause
        assert str(expected) == "nothing to receive (ZMQ_DONTWAIT)", str(expected)
        assert type(expected).__name__ == "EAGAIN"
    # And it is not the same class as another errno, which is the whole point.
    assert EAGAIN is not ETERM
    assert issubclass(ETERM, DemoError)

    # 3. A cancelled task cancels the Rust future. `park()` never completes, so
    # the only way `dropped` can move is the cancellation reaching it.
    before = demo.dropped()
    task = asyncio.create_task(demo.park())
    await asyncio.sleep(0.05)
    assert not task.done()
    task.cancel()
    try:
        await task
        raise AssertionError("a cancelled task must raise CancelledError")
    except asyncio.CancelledError:
        pass
    # The abort lands on a reactor thread, so wait for it with a bound rather
    # than forever (LOOP.md 2).
    for _ in range(200):
        if demo.dropped() == before + 1:
            break
        await asyncio.sleep(0.01)
    assert demo.dropped() == before + 1, "the Rust future was not dropped"

    # A panic is an exception, not a task that dies in silence.
    try:
        await demo.boom()
        raise AssertionError("boom() must raise")
    except RuntimeError as panicked:
        assert "a future that panicked" in str(panicked), str(panicked)

    # The loop is still healthy afterwards.
    assert await demo.echo(b"after") == b"after"

asyncio.run(main())
"#;

#[test]
fn a_binding_of_nothing_proves_the_foundation() {
    Python::initialize();
    let (exec, reactor) = Exec::owned(2, "py-core-test").expect("a reactor for the test");

    let outcome = Python::attach(|py| -> PyResult<()> {
        let module = PyModule::new(py, "demo")?;
        module.add_class::<Demo>()?;
        ERRORS.install(&module, "DemoError", &["EAGAIN", "ETERM"])?;

        let demo = Py::new(
            py,
            Demo {
                bridge: Bridge::new(exec.clone(), to_py),
            },
        )?;
        let globals = module.dict();
        globals.set_item("demo", demo)?;

        let script = CString::new(SCRIPT).expect("the script has no NUL");
        py.run(script.as_c_str(), Some(&globals), None)
    });

    // Drop the reactor before reporting, so a failure does not also leak a
    // runtime into the next test.
    drop(reactor);
    if let Err(err) = outcome {
        Python::attach(|py| err.print(py));
        panic!("the Python consumer failed; its traceback is above");
    }
}
