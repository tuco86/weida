//! Waits for tests that report instead of killing.
//!
//! A test in this workspace has no hard time limit on a real clock: a slow
//! run is still a run, and a limit that fires on a loaded machine turns a
//! healthy test red (weida nightly #81). Every wait here takes an *expected*
//! time instead. Past it, and at every further multiple of it, the wait
//! prints what it is still waiting for and goes on waiting. The hang guard is
//! the CI job's own budget, and whoever watches the run decides what a wait
//! far past its expected time means.
//!
//! **A paused Tokio clock is the exception, and it keeps its limit.** There
//! time only moves when every task is idle, so reaching the expected time
//! means nothing can make progress any more: a hang, reported at once rather
//! than after the job budget. Load cannot cause it. The waits tell the two
//! clocks apart by comparing the Tokio clock with the wall clock: when the
//! Tokio clock ran far ahead, it was paused.
//!
//! Use [`within`] around one future, [`until`] for a condition polled from
//! async code, [`poll_until`] for one polled from a plain thread, and
//! [`recv`] for a `std::sync::mpsc` receiver.

use std::future::Future;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

/// The expected time of one wait when a test has no better number: what a
/// step takes on an idle machine is milliseconds, and ten seconds is where a
/// slow one starts to be worth a line.
pub const EXPECTED: Duration = Duration::from_secs(10);

/// How often [`until`] and [`poll_until`] look at their condition.
const POLL: Duration = Duration::from_millis(5);

/// Whether the Tokio clock moved at least twice as far as the wall clock
/// since the wait began: only a paused clock does that.
fn clock_was_paused(tokio_elapsed: Duration, wall_elapsed: Duration) -> bool {
    wall_elapsed * 2 < tokio_elapsed
}

/// One line on stderr for a wait past `n` times its expected time.
fn report(what: &str, n: u32, expected: Duration, waited: Duration) {
    eprintln!("still waiting for {what}: {waited:.1?}, past {n} x its expected {expected:?}");
}

/// Waits for `f`, reporting at every multiple of `expected`.
///
/// On a paused Tokio clock it panics at `expected` instead, naming `what`:
/// a paused clock reaches it only when nothing can run any more.
pub async fn within<F: Future>(what: &str, expected: Duration, f: F) -> F::Output {
    let wall = Instant::now();
    let start = tokio::time::Instant::now();
    let mut f = std::pin::pin!(f);
    let mut n = 1u32;
    loop {
        tokio::select! {
            out = &mut f => return out,
            () = tokio::time::sleep_until(start + expected * n) => {
                if clock_was_paused(start.elapsed(), wall.elapsed()) {
                    panic!(
                        "{what} did not finish within {expected:?} on a paused clock, \
                         where that is a hang rather than a slow machine"
                    );
                }
                report(what, n, expected, wall.elapsed());
                n = n.saturating_add(1);
            }
        }
    }
}

/// Waits until `done` holds, polling it from async code, with the
/// reporting and the paused-clock rule of [`within`].
pub async fn until(what: &str, expected: Duration, mut done: impl FnMut() -> bool) {
    within(what, expected, async {
        while !done() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
}

/// Waits until `done` holds, polling it from a plain thread and reporting
/// at every multiple of `expected`. There is no Tokio clock here, so there
/// is no paused-clock limit either.
pub fn poll_until(what: &str, expected: Duration, mut done: impl FnMut() -> bool) {
    let wall = Instant::now();
    let mut n = 1u32;
    while !done() {
        std::thread::sleep(POLL);
        if wall.elapsed() >= expected * n {
            report(what, n, expected, wall.elapsed());
            n = n.saturating_add(1);
        }
    }
}

/// Receives from `rx`, reporting at every multiple of `expected` while
/// nothing arrives. `Err` only when every sender is gone.
pub fn recv<T>(rx: &Receiver<T>, what: &str, expected: Duration) -> Result<T, RecvTimeoutError> {
    let wall = Instant::now();
    let mut n = 1u32;
    loop {
        match rx.recv_timeout(expected) {
            Ok(value) => return Ok(value),
            Err(RecvTimeoutError::Timeout) => {
                report(what, n, expected, wall.elapsed());
                n = n.saturating_add(1);
            }
            Err(e @ RecvTimeoutError::Disconnected) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: on a paused clock a future that can never finish is a hang,
    /// reported at its expected time rather than waited on for ever.
    #[tokio::test(start_paused = true)]
    #[should_panic(expected = "on a paused clock")]
    async fn a_paused_clock_turns_the_expected_time_into_a_limit() {
        within(
            "nothing",
            Duration::from_secs(1),
            std::future::pending::<()>(),
        )
        .await;
    }

    /// Claim: on a paused clock a future that finishes, even late in virtual
    /// time, is returned.
    #[tokio::test(start_paused = true)]
    async fn a_paused_clock_returns_what_finishes_in_time() {
        let out = within("a sleep", Duration::from_secs(10), async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            7
        })
        .await;
        assert_eq!(out, 7);
    }

    /// Claim: on a real clock a wait past its expected time is not cut short:
    /// the future finishes and its value comes back.
    #[tokio::test]
    async fn a_real_clock_waits_past_the_expected_time() {
        let out = within("a slow step", Duration::from_millis(5), async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            "done"
        })
        .await;
        assert_eq!(out, "done");
    }

    /// Claim: the clock test tells a paused clock from a busy machine. A wall
    /// clock that kept pace, even a slow one, is real; one left far behind is
    /// a paused Tokio clock.
    #[test]
    fn a_tokio_clock_far_ahead_of_the_wall_clock_was_paused() {
        let s = Duration::from_secs;
        assert!(clock_was_paused(s(10), Duration::from_millis(3)));
        assert!(!clock_was_paused(s(10), s(10)));
        assert!(!clock_was_paused(s(10), s(30)));
    }

    /// Claim: `until` returns once its condition holds, on a paused clock too.
    #[tokio::test(start_paused = true)]
    async fn until_returns_when_the_condition_holds() {
        let start = tokio::time::Instant::now();
        until("the clock to move", Duration::from_secs(10), || {
            start.elapsed() >= Duration::from_secs(1)
        })
        .await;
    }

    /// Claim: `recv` waits past its expected time for a value still coming,
    /// and reports a closed channel as `Disconnected`.
    #[test]
    fn recv_waits_for_a_late_value_and_reports_a_closed_channel() {
        let (tx, rx) = std::sync::mpsc::channel();
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            tx.send(5).expect("send");
        });
        assert_eq!(recv(&rx, "a value", Duration::from_millis(5)), Ok(5));
        sender.join().expect("sender");
        assert_eq!(
            recv(&rx, "nothing", Duration::from_millis(5)),
            Err(RecvTimeoutError::Disconnected)
        );
    }

    /// Claim: `poll_until` returns once its condition holds.
    #[test]
    fn poll_until_returns_when_the_condition_holds() {
        let start = Instant::now();
        poll_until("the wall clock to move", Duration::from_millis(5), || {
            start.elapsed() >= Duration::from_millis(20)
        });
    }
}
