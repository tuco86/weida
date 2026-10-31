//! The one implementation behind every handle whose calls are tries on the
//! caller's thread (docs/c/README.md R3a, R4, R11).

use std::ffi::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::error::{WEIDA_EAGAIN, closed, fail_abi};
use crate::ready::{Signal, wait_readable};

/// A handle's state, polled under its own mutex with a waker that raises
/// its readiness signal.
pub(crate) struct Tried<S> {
    state: Mutex<Option<S>>,
    signal: Arc<Signal>,
    dropped: AtomicBool,
}

impl<S> Tried<S> {
    pub(crate) fn new(state: S) -> Tried<S> {
        Tried {
            state: Mutex::new(Some(state)),
            signal: Signal::new(),
            dropped: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<S>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The readiness descriptor.
    pub(crate) fn fd(&self) -> Result<i64, c_int> {
        self.signal.fd()
    }

    /// Runs `f` on the state without polling: a synchronous accessor.
    /// `-WEIDA_ECLOSED` once the handle was dropped or consumed.
    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> Result<R, c_int> {
        match self.lock().as_mut() {
            Some(state) => Ok(f(state)),
            None => Err(closed()),
        }
    }

    /// Ends the handle: every wait on it returns `-WEIDA_ECLOSED`, and the
    /// state is handed back for the caller to drop or consume.
    pub(crate) fn close(&self) -> Option<S> {
        self.dropped.store(true, Ordering::Release);
        let state = self.lock().take();
        self.signal.raise();
        state
    }

    /// Polls `f` until it is ready or `timeout_ms` (R4) gives up.
    ///
    /// Each round clears the signal, polls under the mutex, and raises the
    /// signal again on `Ready`, since there may be more. On `Pending` it
    /// waits for the descriptor with no lock held: a wake raises it, and so
    /// does a drop from another thread.
    pub(crate) fn attempt<R>(
        &self,
        timeout_ms: i64,
        mut f: impl FnMut(&mut S, &mut Context<'_>) -> Poll<R>,
    ) -> Result<R, c_int> {
        let deadline = u64::try_from(timeout_ms)
            .ok()
            .filter(|ms| *ms > 0)
            .and_then(|ms| Instant::now().checked_add(Duration::from_millis(ms)));
        let waker = Waker::from(Arc::clone(&self.signal));
        loop {
            if self.dropped.load(Ordering::Acquire) {
                return Err(closed());
            }
            self.signal.clear();
            let polled = {
                let mut state = self.lock();
                let Some(state) = state.as_mut() else {
                    return Err(closed());
                };
                f(state, &mut Context::from_waker(&waker))
            };
            if let Poll::Ready(out) = polled {
                self.signal.raise();
                return Ok(out);
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
                // A bound so large that `Instant` cannot hold it.
                (_, None) => -1,
            };
            let fd = self.signal.fd()?;
            wait_readable(fd, wait_ms)?;
        }
    }
}

pub(crate) fn gave_up() -> c_int {
    fail_abi(WEIDA_EAGAIN, c"nothing was ready within the timeout")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A source driven by hand: `Ready` when a value is loaded, otherwise
    /// `Pending` with the waker kept.
    #[derive(Default)]
    struct Source {
        value: Option<u32>,
        waker: Option<Waker>,
    }

    fn take(source: &mut Source, cx: &mut Context<'_>) -> Poll<u32> {
        match source.value.take() {
            Some(v) => Poll::Ready(v),
            None => {
                source.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    #[test]
    fn a_try_follows_the_wakes_of_its_source() {
        let tried = Tried::new(Source::default());

        assert_eq!(tried.attempt(0, take), Err(-WEIDA_EAGAIN));
        assert!(!tried.signal.is_raised());

        let waker = tried
            .with(|s| s.waker.take())
            .expect("open")
            .expect("the pending poll kept a waker");
        tried.with(|s| s.value = Some(5)).expect("open");
        waker.wake();
        assert!(tried.signal.is_raised(), "a wake raises the signal");

        assert_eq!(tried.attempt(0, take), Ok(5));
        assert!(tried.signal.is_raised(), "a ready try leaves it raised");

        assert_eq!(tried.attempt(0, take), Err(-WEIDA_EAGAIN));
        assert!(!tried.signal.is_raised(), "a pending try clears it");
    }

    #[test]
    fn a_closed_handle_answers_closed() {
        let tried = Tried::new(Source::default());
        assert!(tried.close().is_some());
        assert_eq!(tried.attempt(0, take), Err(-crate::error::WEIDA_ECLOSED));
    }
}
