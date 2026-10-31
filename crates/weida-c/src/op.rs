//! The one implementation behind every pending operation: work spawned on
//! the runtime's reactor, whose result is taken once (docs/c/README.md R3b,
//! R11).

use std::ffi::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::task::AbortHandle;
use weida_rs::Error;
use weida_runtime::Exec;

use crate::error::{closed, fail};
use crate::ready::{Signal, wait_readable};
use crate::try_handle::gave_up;

/// One spawned operation and the result it left.
pub(crate) struct Op<T> {
    result: Mutex<Option<Result<T, Error>>>,
    taken: AtomicBool,
    dropped: AtomicBool,
    signal: Arc<Signal>,
    task: Mutex<Option<AbortHandle>>,
}

/// Stores a result when the task ends without one: a runtime that shut down
/// drops its tasks, and a waiter must not wait for that forever.
struct Completion<T> {
    op: Arc<Op<T>>,
    done: bool,
}

impl<T> Completion<T> {
    fn complete(&mut self, result: Result<T, Error>) {
        self.done = true;
        *self.op.lock() = Some(result);
        self.op.signal.raise();
    }
}

impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        if !self.done {
            self.complete(Err(Error::Runtime(
                "the operation ended without a result: its runtime shut down".into(),
            )));
        }
    }
}

impl<T: Send + 'static> Op<T> {
    /// Spawns `work` on `exec`'s reactor; its result is stored and the
    /// op's descriptor raised when it ends.
    pub(crate) fn start(
        exec: &Exec,
        work: impl Future<Output = Result<T, Error>> + Send + 'static,
    ) -> Arc<Op<T>> {
        let op = Arc::new(Op {
            result: Mutex::new(None),
            taken: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            signal: Signal::new(),
            task: Mutex::new(None),
        });
        let mut completion = Completion {
            op: Arc::clone(&op),
            done: false,
        };
        let task = exec.spawn(async move {
            let result = work.await;
            completion.complete(result);
        });
        *op.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task.abort_handle());
        op
    }
}

impl<T> Op<T> {
    fn lock(&self) -> MutexGuard<'_, Option<Result<T, Error>>> {
        self.result.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The readiness descriptor: readable exactly while a result waits.
    pub(crate) fn fd(&self) -> Result<i64, c_int> {
        self.signal.fd()
    }

    /// Takes the result, waiting up to `timeout_ms` (R4) for it.
    ///
    /// A failed operation records its error on the calling thread. Every
    /// call after the one that took the result returns `-WEIDA_ECLOSED`.
    pub(crate) fn wait(&self, timeout_ms: i64) -> Result<T, c_int> {
        let deadline = u64::try_from(timeout_ms)
            .ok()
            .filter(|ms| *ms > 0)
            .and_then(|ms| Instant::now().checked_add(Duration::from_millis(ms)));
        loop {
            if self.dropped.load(Ordering::Acquire) || self.taken.load(Ordering::Acquire) {
                return Err(closed());
            }
            if let Some(result) = self.lock().take() {
                self.taken.store(true, Ordering::Release);
                self.signal.clear();
                return result.map_err(fail);
            }
            let wait_ms = match (timeout_ms, deadline) {
                (0, _) => return Err(gave_up()),
                (ms, _) if ms < 0 => -1,
                (_, Some(deadline)) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(gave_up());
                    }
                    i64::try_from(left.as_millis().max(1)).unwrap_or(i64::MAX)
                }
                (_, None) => -1,
            };
            let fd = self.signal.fd()?;
            wait_readable(fd, wait_ms)?;
            // A wake that found no result is the op's own clear racing a
            // late raise; drain it before waiting again.
            if self.lock().is_none() {
                self.signal.clear();
            }
        }
    }

    /// Ends the op: an unfinished task is aborted, and every wait on it
    /// returns `-WEIDA_ECLOSED`.
    pub(crate) fn close(&self) {
        self.dropped.store(true, Ordering::Release);
        if let Some(task) = self.task.lock().unwrap_or_else(|e| e.into_inner()).take() {
            task.abort();
        }
        self.signal.raise();
    }

    #[cfg(test)]
    fn is_ready(&self) -> bool {
        self.signal.is_raised()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{WEIDA_EAGAIN, WEIDA_ECLOSED};
    use tokio::sync::oneshot;

    const WAIT: Duration = Duration::from_secs(5);

    #[tokio::test(start_paused = true)]
    async fn an_op_hands_its_result_over_once() {
        let exec = Exec::current().expect("ambient runtime");
        let (tx, rx) = oneshot::channel::<u32>();
        let op = Op::start(&exec, async move { rx.await.map_err(|_| Error::Canceled) });

        assert_eq!(op.wait(0).err(), Some(-WEIDA_EAGAIN));
        tx.send(9).expect("the op is waiting");
        weida_testkit::until("the op completes", WAIT, || op.is_ready()).await;
        assert_eq!(op.wait(0).ok(), Some(9));
        assert_eq!(op.wait(0).err(), Some(-WEIDA_ECLOSED));
    }

    #[tokio::test(start_paused = true)]
    async fn closing_an_unfinished_op_aborts_its_task() {
        let exec = Exec::current().expect("ambient runtime");
        let (tx, rx) = oneshot::channel::<()>();
        let op = Op::start(&exec, async move {
            let _held = tx;
            std::future::pending::<Result<(), Error>>().await
        });
        tokio::task::yield_now().await;
        op.close();
        weida_testkit::within("the task is dropped", WAIT, rx)
            .await
            .expect_err("the aborted task drops its sender");
        assert_eq!(op.wait(0).err(), Some(-WEIDA_ECLOSED));
    }
}
