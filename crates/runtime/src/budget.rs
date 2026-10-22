//! The bounded close budget.

use std::time::{Duration, Instant};

/// A finite budget for a shutdown, started once and spent by every phase of
/// it.
///
/// **Why the type exists.** A close has phases — stop admitting work, let
/// what is already finished reach the peer, then close the sockets and wait
/// for them to go idle — and each phase's wait is decided by somebody else's
/// network. Without one budget spanning all of them, the length of a close is
/// whatever the slowest peer makes it: QUIC's closing and draining periods
/// last about three times the path's probe timeout, and ZeroMQ's `ZMQ_LINGER`
/// defaults to infinite, which is why `zmq_ctx_term()` is known as a place
/// processes hang (`docs/research/zeromq.md` §12/P17). A process that must
/// exit within a budget of its own cannot use an API like that
/// ([decisions/0009](../../../docs/decisions/0009-drain.md) §4.3, §4.4).
///
/// So: one budget, taken at the start, and each phase asks how much is left.
/// A phase that overruns leaves the next one [`Duration::ZERO`] rather than a
/// negative number, which is a bound that still holds rather than a wait that
/// starts over.
///
/// ```
/// # use std::time::Duration;
/// # use weida_runtime::CloseBudget;
/// let budget = CloseBudget::start(Duration::from_secs(1));
/// // First phase waits on `budget.remaining()`, and so does the next.
/// assert!(budget.remaining() <= Duration::from_secs(1));
/// assert_eq!(budget.limit(), Duration::from_secs(1));
/// ```
#[derive(Clone, Copy, Debug)]
pub struct CloseBudget {
    limit: Duration,
    started: Instant,
}

impl CloseBudget {
    /// Starts a budget of `limit`, from now.
    pub fn start(limit: Duration) -> CloseBudget {
        CloseBudget {
            limit,
            started: Instant::now(),
        }
    }

    /// What the whole budget was, regardless of what is left of it.
    pub const fn limit(&self) -> Duration {
        self.limit
    }

    /// What is left, saturating at [`Duration::ZERO`]: the bound the next
    /// phase of the close gets.
    pub fn remaining(&self) -> Duration {
        self.limit.saturating_sub(self.started.elapsed())
    }

    /// Whether the budget is used up. A spent budget still bounds: the next
    /// phase gets zero and does not wait.
    pub fn is_spent(&self) -> bool {
        self.remaining().is_zero()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        /// Claim: the phases of one close share one budget — what the second
        /// phase gets is what the first left, and never more than the limit.
        #[test]
        fn a_budget_is_spent_by_the_phases_that_share_it() {
            // Far longer than the sleep: atik's scheduler has let a 10 ms sleep
            // run past 40 ms, which spent a 50 ms budget.
            let budget = CloseBudget::start(Duration::from_secs(10));
            assert_eq!(budget.limit(), Duration::from_secs(10));
            let before = budget.remaining();
            assert!(before <= Duration::from_secs(10));
            std::thread::sleep(Duration::from_millis(10));
            let after = budget.remaining();
            assert!(after < before, "{after:?} must be less than {before:?}");
            assert!(!budget.is_spent());
        }
        /// Claim: an overrun leaves zero rather than wrapping, so the next phase
        /// is still bounded.
        #[test]
        fn an_overrun_budget_is_zero_and_not_negative() {
            let budget = CloseBudget::start(Duration::from_millis(1));
            std::thread::sleep(Duration::from_millis(5));
            assert_eq!(budget.remaining(), Duration::ZERO);
            assert!(budget.is_spent());
        }
    }
}
