//! How often a flood of one kind of event is said out loud.
//!
//! The server decides how fast messages arrive and how fast it refuses what
//! this client sent, so the rate at which a client drops copies or hears
//! refusals is the peer's. One log line per event would put the cost of an
//! overload — a lossy subject conversion, a log record — on the client at
//! exactly the moment it has the least to spare.

use std::time::{Duration, Instant};

/// How often one source of events is logged at most.
pub(crate) const REPORT_EVERY: Duration = Duration::from_secs(1);

/// Counts events and says when one of them is due a log line.
#[derive(Debug, Default)]
pub(crate) struct ReportLog {
    last: Option<Instant>,
    unreported: u64,
}

impl ReportLog {
    /// Notes one event at `now`: `Some(n)` where `n` events, this one
    /// included, are due a log line, and `None` where one was written lately
    /// enough.
    pub(crate) fn record(&mut self, now: Instant) -> Option<u64> {
        self.unreported += 1;
        let due = self
            .last
            .is_none_or(|last| now.saturating_duration_since(last) >= REPORT_EVERY);
        if !due {
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.unreported))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A flood of events is one log line a second, carrying the count of what
    /// it stands for.
    #[test]
    fn events_are_reported_at_most_once_a_second() {
        let start = Instant::now();
        let mut log = ReportLog::default();
        assert_eq!(log.record(start), Some(1), "the first is reported at once");
        for n in 1..=1000u32 {
            assert_eq!(
                log.record(start + Duration::from_millis(u64::from(n) % 900)),
                None,
                "inside the second after the last report"
            );
        }
        assert_eq!(
            log.record(start + Duration::from_millis(1500)),
            Some(1001),
            "the next report stands for every event since the last"
        );
        assert_eq!(log.record(start + Duration::from_millis(1600)), None);
    }
}
