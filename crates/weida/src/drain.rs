//! The bounded drain of [decision 0009](../../../docs/decisions/0009-drain.md).
//!
//! `Runtime::shutdown` is abortive: it closes, and a FIN that was queued but
//! not yet acknowledged never lands [0009 §4.1]. A drain is the counterpart —
//! stop admitting, give the finished transfers their chance, then perform the
//! same close — and what it can wait for is exactly one thing: the peer's
//! **transport** receipt, the condition `Delivery::delivered()` reports
//! [0009 §4.2]. Nothing here waits for a peer application to read anything,
//! and nothing here goes on the wire [0009 §4.8].
//!
//! The state below exists because a fire-and-forget sender drops its
//! `Delivery` and with it the only handle that can observe the
//! acknowledgement. Such a receipt is parked **on its connection** instead of
//! being dropped, so that a later drain has something to wait on. A receipt
//! the application keeps is never parked: whoever holds it is doing the
//! waiting.
//!
//! Parking is on the send path, so it costs one uncontended lock on the
//! connection's own deque and a push — no runtime-wide structure, no polling
//! and no task. The set is walked only when it is full and once more at drain
//! time.
//!
//! It holds nothing new in the sense of
//! [`docs/INVARIANTS.md`](../../../docs/INVARIANTS.md) — a receipt is a
//! future over a stream that already exists — but it is a set, so it is
//! bounded: per connection, by the stream budgets already in `Limits`.
//! Both are local quantities; a peer cannot grow this by sending.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use weida_core::Limits;

use crate::conn::ConnCtx;

/// One finished transfer's acknowledgement, as the transport reports it:
/// `Ok(None)` delivered, `Ok(Some(code))` refused, `Err` connection gone.
pub(crate) type Receipt =
    Pin<Box<dyn Future<Output = Result<Option<u64>, weida_core::Error>> + Send + Sync>>;

/// What a drain achieved, counted locally.
///
/// Both numbers are about *this* drain: the transfers that were still
/// unacknowledged when it started. Neither is a statement about the peer's
/// application — L0 has no such signal, which is why the drain reports counts
/// rather than a guarantee [0009 §4.6].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Drained {
    /// Transfers whose payload and FIN the peer's transport acknowledged
    /// before the deadline.
    pub delivered: u64,
    /// Transfers still unacknowledged when the deadline expired, plus any the
    /// peer refused with `STOP_SENDING` and any whose connection was lost.
    /// A non-zero count is not an error.
    pub outstanding: u64,
}

/// The runtime-wide half: the admission flag, the connections a drain must
/// visit, and the count of receipts that had to be dropped unobserved.
///
/// Deliberately *not* the receipts themselves. Parking is on the hot path of
/// every fire-and-forget send, so it must not touch a structure shared by
/// every connection and every worker thread; the receipts live on the
/// connection that produced them ([`ConnDrain`]) and a drain collects them
/// from there.
pub(crate) struct DrainState {
    /// Set for good when a drain starts: bindings stop accepting connections
    /// and a new inbound stream is refused with `SHUTDOWN` [0009 §4.5].
    draining: AtomicBool,
    /// Every connection this runtime has spawned, weakly: a drain walks them
    /// to collect their parked receipts. Touched once per connection, never
    /// per message.
    connections: Mutex<Vec<Weak<ConnCtx>>>,
    /// Receipts evicted at a connection's cap before they settled. They can
    /// no longer be observed, so the next drain counts them as outstanding
    /// rather than assuming they landed.
    evicted: AtomicU64,
}

impl DrainState {
    pub(crate) fn new() -> DrainState {
        DrainState {
            draining: AtomicBool::new(false),
            connections: Mutex::new(Vec::new()),
            evicted: AtomicU64::new(0),
        }
    }

    /// Whether admission has stopped.
    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    /// Stops admission. Idempotent.
    pub(crate) fn begin(&self) {
        self.draining.store(true, Ordering::Relaxed);
    }

    /// Records a connection so a later drain can find its parked receipts.
    ///
    /// Dead entries are swept when the vector would grow, which bounds the
    /// list at roughly twice the live connection count without a sweep on
    /// every registration.
    pub(crate) fn register(&self, conn: &Arc<ConnCtx>) {
        let mut connections = self.connections.lock().expect("drain state poisoned");
        if connections.len() == connections.capacity() {
            connections.retain(|conn| conn.strong_count() > 0);
        }
        connections.push(Arc::downgrade(conn));
    }

    /// Closes every live connection this runtime owns.
    ///
    /// A QUIC connection is closed by closing its endpoint; a local one has
    /// no endpoint to close, so the connection itself is the only handle
    /// there is.
    pub(crate) fn close_all(&self, code: u64, reason: &str) {
        let connections = self.connections.lock().expect("drain state poisoned");
        for conn in connections.iter().filter_map(|conn| conn.upgrade()) {
            conn.conn.close(code, reason);
        }
    }

    /// Counts one receipt that was dropped before it could settle.
    pub(crate) fn evict(&self) {
        self.evicted.fetch_add(1, Ordering::Relaxed);
    }

    /// Takes everything this drain has to wait for, from every live
    /// connection.
    ///
    /// The registry is **swept, not emptied**: `Runtime::drain` calls this
    /// and then `close_all`, and on a local transport that registry is the
    /// only handle a close has (`docs/decisions/0010-local-transport.md`
    /// §4.2). Draining the vector here left `close_all` nothing to close, so
    /// a drained in-process or socket peer never saw the shutdown at all.
    pub(crate) fn take(&self) -> (Vec<Receipt>, u64) {
        let live: Vec<Arc<ConnCtx>> = {
            let mut connections = self.connections.lock().expect("drain state poisoned");
            connections.retain(|conn| conn.strong_count() > 0);
            connections
                .iter()
                .filter_map(|conn| conn.upgrade())
                .collect()
        };
        let mut receipts = Vec::new();
        for conn in live {
            receipts.append(&mut conn.parked.take());
        }
        (receipts, self.evicted.swap(0, Ordering::Relaxed))
    }
}

/// The parked receipts of **one** connection.
///
/// One short uncontended lock on the send path, no reaping and no polling:
/// parking a receipt is a push. The set only has to be walked when it is
/// full, and then settled receipts are reaped to make room before anything
/// is thrown away.
pub(crate) struct ConnDrain {
    parked: Mutex<Parked>,
    /// Cap on this connection's parked set, taken from the stream budgets:
    /// what can plausibly be unacknowledged at once is what can be in flight
    /// at once, so this adds no new knob (0009 §5 asks for none).
    ///
    /// **The budget it is taken from is this side's, and what bounds a parked
    /// set is the peer's** — `max_concurrent_uni_streams` and
    /// `max_concurrent_bidi_streams` are what this runtime grants *inbound*,
    /// while how many receipts can be pending at once is what the peer granted
    /// this side to open. The two are the same number between two default
    /// runtimes and need not be in general, so this cap is a **local
    /// heuristic** rather than a derived bound, and it is written down as one.
    /// Using the peer's number instead would mean reading its
    /// transport parameters, which `quinn` does not expose.
    ///
    /// **What that heuristic costs is measured, and it is nothing on a healthy
    /// connection**
    /// (`a_fire_and_forget_producer_never_fills_the_parked_receipt_set`). The
    /// sweep in [`ConnDrain::park`] is O(parked) under this mutex, with each
    /// poll taking `quinn`'s connection-state lock — but it only runs at the
    /// cap, and the cap is not reachable while the peer is acknowledging: a
    /// receipt settles when the FIN is acknowledged and the stream's
    /// concurrency slot frees at the same moment, so the parked set and the
    /// in-flight count are bounded by the same quantity and `open` waits
    /// before the set can outgrow it. Measured: 20 000 fire-and-forget sends
    /// at 43.5 Kmsg/s leave **2 656** receipts parked against a cap of 3 072
    /// and evict **zero**, twice in a row. Reaching the sweep needs a peer
    /// that stops acknowledging while this side keeps opening streams, which
    /// the stream budget makes a bounded window.
    ///
    /// **On a local transport the budget is a different quantity**: a parked
    /// receipt holds its send half, which there is an OS connection counted
    /// against `max_local_streams` (`docs/IMPLEMENTATION.md` §4.13), so a
    /// parked set sized 1024+1024 would consume all 255 descriptors and the
    /// next `open` would fail with `LimitExceeded` — nothing in flight,
    /// nothing wrong, no way for the caller to know.
    /// Locally the cap is therefore **half** the local ceiling: parked
    /// receipts may hold at most half the descriptors, which leaves the other
    /// half to open with, and a drain still sees every receipt whose outcome
    /// is open. Because that `open` waits for a slot instead of failing,
    /// this half is also what keeps the wait finite.
    max_parked: usize,
    /// Fewest parks between two sweeps of a full set. A sweep polls every
    /// parked receipt, so one sweep per this many parks costs at most eight
    /// polls per park whatever the set holds. A sweep that frees `f` receipts
    /// waits for `max(f, sweep_interval)` parks before the next.
    sweep_interval: usize,
}

impl ConnDrain {
    pub(crate) fn new(limits: &Limits, streams_are_local: bool) -> ConnDrain {
        let by_stream_budget = limits.max_concurrent_uni_streams as usize
            + limits.max_concurrent_bidi_streams as usize;
        let max_parked = if streams_are_local {
            by_stream_budget.min(limits.max_local_streams / 2).max(1)
        } else {
            by_stream_budget
        };
        Self::with_capacity(max_parked)
    }

    fn with_capacity(max_parked: usize) -> ConnDrain {
        ConnDrain {
            parked: Mutex::new(Parked::default()),
            max_parked,
            sweep_interval: (max_parked / 8).max(1),
        }
    }

    /// Parks the receipt of a finished transfer nobody is waiting on.
    ///
    /// Returns `true` when an unsettled receipt had to be dropped to make
    /// room, which the caller counts as a loss the next drain reports.
    pub(crate) fn park(&self, receipt: Receipt) -> bool {
        // Never `expect` here. `park` runs inside `Drop for Delivery`, on the
        // fire-and-forget path of every pattern, and a panic in a destructor
        // during an unwind aborts the process. A deque of receipts has no
        // invariant a panic could have broken, so a poisoned lock is taken
        // rather than propagated.
        let mut guard = self.parked.lock().unwrap_or_else(|e| e.into_inner());
        let parked = &mut *guard;
        // Every park counts toward the next sweep, so a sweep of a full set
        // is due no sooner than `sweep_interval` parks after the last one.
        parked.parks_until_sweep = parked.parks_until_sweep.saturating_sub(1);
        let mut lost = false;
        if parked.receipts.len() >= self.max_parked {
            if parked.parks_until_sweep == 0 {
                // Only now is it worth looking: everything settled goes.
                let before = parked.receipts.len();
                parked
                    .receipts
                    .retain_mut(|receipt| settled(receipt).is_none());
                // The room this sweep made is the wait before the next one,
                // so a set that stays full of unsettled receipts is swept once
                // per `sweep_interval` parks, not on every park.
                let freed = before - parked.receipts.len();
                parked.parks_until_sweep = freed.max(self.sweep_interval);
            }
            if parked.receipts.len() >= self.max_parked {
                // The oldest goes. It counts as lost only if it had not
                // settled: a settled receipt loses nothing, and a sweep would
                // have dropped it silently too.
                if let Some(mut oldest) = parked.receipts.pop_front() {
                    lost = settled(&mut oldest).is_none();
                }
            }
        }
        parked.receipts.push_back(receipt);
        lost
    }

    /// Drops every parked receipt that has already settled, and reports how
    /// many went.
    ///
    /// Called when opening a stream has just failed with `LimitExceeded`, for
    /// the reason `ConnCtx::reap_parked` documents: on a local transport a
    /// parked receipt holds a file descriptor, and the parked set's own cap is
    /// too coarse to notice. Nothing is thrown away that has not settled, so a
    /// drain still sees every receipt whose outcome is still open.
    pub(crate) fn reap(&self) -> usize {
        let mut guard = self.parked.lock().unwrap_or_else(|e| e.into_inner());
        let parked = &mut *guard;
        let before = parked.receipts.len();
        parked
            .receipts
            .retain_mut(|receipt| settled(receipt).is_none());
        // A full sweep leaves nothing waiting on the cadence of parks.
        parked.parks_until_sweep = 0;
        before - parked.receipts.len()
    }

    /// Takes this connection's receipts, for a drain.
    pub(crate) fn take(&self) -> Vec<Receipt> {
        let mut guard = self.parked.lock().unwrap_or_else(|e| e.into_inner());
        let parked = &mut *guard;
        parked.parks_until_sweep = 0;
        std::mem::take(&mut parked.receipts).into()
    }
}

/// The parked receipts of one connection, and how many parks must pass before
/// a full set is swept again.
#[derive(Default)]
struct Parked {
    receipts: VecDeque<Receipt>,
    parks_until_sweep: usize,
}

/// Polls one parked receipt without a real waker: `Some` when it has settled.
///
fn settled(receipt: &mut Receipt) -> Option<Result<Option<u64>, weida_core::Error>> {
    let mut cx = Context::from_waker(Waker::noop());
    match receipt.as_mut().poll(&mut cx) {
        Poll::Ready(outcome) => Some(outcome),
        Poll::Pending => None,
    }
}

/// Awaits `receipts` until they settle or `deadline` elapses, counting both.
///
/// Polling the whole set on every wake is linear, which is the right trade
/// here: the set is bounded, and this runs once per process rather than per
/// message.
pub(crate) async fn wait_for(
    receipts: Vec<Receipt>,
    evicted: u64,
    deadline: impl Future<Output = ()>,
) -> Drained {
    let mut delivered = 0u64;
    let mut refused = 0u64;
    let mut pending: Vec<Option<Receipt>> = receipts.into_iter().map(Some).collect();

    {
        let settle = std::future::poll_fn(|cx: &mut Context<'_>| {
            let mut left = 0usize;
            for slot in pending.iter_mut() {
                let Some(receipt) = slot else { continue };
                match receipt.as_mut().poll(cx) {
                    Poll::Ready(Ok(None)) => {
                        delivered += 1;
                        *slot = None;
                    }
                    // The peer stopped the stream, or the connection went
                    // away: settled, but not delivered.
                    Poll::Ready(_) => {
                        refused += 1;
                        *slot = None;
                    }
                    Poll::Pending => left += 1,
                }
            }
            if left == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });

        tokio::select! {
            () = settle => {}
            () = deadline => {}
        }
    }

    let unfinished = pending.iter().filter(|slot| slot.is_some()).count() as u64;
    Drained {
        delivered,
        outstanding: unfinished + refused + evicted,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// A receipt that never settles and counts how often it is polled.
    struct Stuck {
        polls: Arc<AtomicUsize>,
    }

    impl Future for Stuck {
        type Output = Result<Option<u64>, weida_core::Error>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Pending
        }
    }

    /// Parking past the cap drops exactly one unsettled receipt per park, as
    /// it always has, without polling the whole set on each of them.
    #[test]
    fn parking_past_the_cap_costs_a_bounded_number_of_polls() {
        const CAP: usize = 256;
        const PARKS: usize = 2_000;
        let drain = ConnDrain::with_capacity(CAP);
        let polls = Arc::new(AtomicUsize::new(0));
        let mut lost = 0;
        for _ in 0..CAP + PARKS {
            let receipt: Receipt = Box::pin(Stuck {
                polls: Arc::clone(&polls),
            });
            if drain.park(receipt) {
                lost += 1;
            }
        }
        assert_eq!(lost, PARKS, "each park past the cap loses one receipt");
        let polls = polls.load(Ordering::Relaxed);
        assert!(
            polls <= 16 * (CAP + PARKS),
            "{polls} polls for {} parks",
            CAP + PARKS
        );
    }

    /// Settled receipts make room silently: only an unsettled receipt counts
    /// as lost.
    #[test]
    fn settled_receipts_make_room_without_counting_as_lost() {
        const CAP: usize = 8;
        let drain = ConnDrain::with_capacity(CAP);
        for _ in 0..CAP {
            let receipt: Receipt = Box::pin(std::future::ready(Ok(None)));
            assert!(!drain.park(receipt));
        }
        for _ in 0..4 * CAP {
            let receipt: Receipt = Box::pin(std::future::ready(Ok(None)));
            assert!(!drain.park(receipt), "a settled receipt is not a loss");
        }
    }
}
