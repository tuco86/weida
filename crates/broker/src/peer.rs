//! Waiting on a peer, and for how long.
//!
//! Every read of a producer's payload and every write to a consumer is a wait
//! on somebody else's flow-control credit, and a peer that never raises it is
//! indistinguishable from a slow one until a clock says otherwise. This module
//! is that clock: one deadline shape, used by admission and by delivery, so
//! that "stalled" means the same thing on both sides of the queue.

use std::time::Duration;

use tokio::time::{Instant, timeout_at};

/// The slowest rate, in bytes per second, at which a peer may move a payload
/// without being treated as stalled.
///
/// An idle timeout alone bounds nothing: a peer that sends one byte just
/// inside every window never goes idle, and an 8 MiB payload at that rate
/// holds its reader for years. Past the first window the deadline therefore
/// moves out only by what this rate allows for the bytes that actually moved,
/// so a peer slower than it is cut after about one window however steadily it
/// trickles. The same shape as `MinRate` in Apache's `mod_reqtimeout`.
pub const MIN_PEER_RATE: u64 = 16 * 1024;

/// Nothing moved before the deadline.
#[derive(Debug)]
pub(crate) struct Stalled;

/// A deadline for one transfer that moves with its progress.
///
/// Two clocks, and a step must beat the earlier of them:
///
/// - **idle**: no more than `idle` after the last byte moved, which catches a
///   peer that stops;
/// - **rate**: no later than `idle` after the start plus what
///   [`MIN_PEER_RATE`] allows for the bytes moved so far, which catches a peer
///   that never quite stops.
#[derive(Debug)]
pub(crate) struct Pace {
    idle: Duration,
    started: Instant,
    last: Instant,
    moved: u64,
}

impl Pace {
    /// A transfer that starts now.
    pub(crate) fn new(idle: Duration) -> Pace {
        let now = Instant::now();
        Pace {
            idle,
            started: now,
            last: now,
            moved: 0,
        }
    }

    /// The instant by which the next step must have completed.
    pub(crate) fn deadline(&self) -> Instant {
        let by_idle = self.last + self.idle;
        let by_rate = self.started + self.idle + Duration::from_secs(self.moved / MIN_PEER_RATE);
        by_idle.min(by_rate)
    }

    /// Runs one step of peer I/O under the current deadline.
    ///
    /// The returned future does not borrow the pace, so the caller can record
    /// what the step moved the moment it completes.
    pub(crate) fn step<F: Future>(
        &self,
        wait: F,
    ) -> impl Future<Output = Result<F::Output, Stalled>> + use<F> {
        let deadline = self.deadline();
        async move { timeout_at(deadline, wait).await.map_err(|_| Stalled) }
    }

    /// Records that `bytes` moved just now.
    pub(crate) fn progressed(&mut self, bytes: usize) {
        self.moved = self.moved.saturating_add(bytes as u64);
        self.last = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDLE: Duration = Duration::from_millis(200);

    #[tokio::test(start_paused = true)]
    async fn a_step_that_never_completes_is_cut_at_the_idle_window() {
        let pace = Pace::new(IDLE);
        let started = tokio::time::Instant::now();
        let outcome = pace.step(std::future::pending::<()>()).await;
        assert!(outcome.is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn progress_moves_the_idle_deadline_out() {
        let mut pace = Pace::new(IDLE);
        for _ in 0..3 {
            tokio::time::sleep(IDLE / 2).await;
            pace.progressed(1024 * 1024);
        }
        // 600 ms in, well past the first window, and a fast peer is still
        // inside its deadline.
        assert!(pace.deadline() > tokio::time::Instant::now());
        assert!(
            pace.step(async { 7 }).await.is_ok(),
            "a step that completes is not cut"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_trickle_is_cut_although_it_never_goes_idle() {
        let mut pace = Pace::new(IDLE);
        let started = tokio::time::Instant::now();
        loop {
            // One byte inside every window: never idle, never fast enough.
            let step = pace.step(tokio::time::sleep(IDLE / 4));
            match step.await {
                Ok(()) => pace.progressed(1),
                Err(Stalled) => break,
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "the rate clock never cut a peer that moved one byte per window"
            );
        }
        assert!(started.elapsed() >= IDLE, "cut no earlier than the window");
    }
}
