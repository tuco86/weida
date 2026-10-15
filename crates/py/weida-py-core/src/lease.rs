//! One handle, leased to one coroutine at a time.
//!
//! # Why not a mutex
//!
//! A library handle that is read with `&mut self` — a subscription's receiving
//! half, a link's event queue, a ZeroMQ socket under libzmq's thread rule — is
//! shared between coroutines by a lock, and the obvious lock is wrong twice
//! over. A `tokio::sync::Mutex` guard hands out `&T`, so a future holding one
//! across an `await` is `Send` only for a `Sync` handle, and most of these are
//! not; and a guard *borrows*, while
//! [`Bridge::awaitable`](crate::Bridge::awaitable) wants a `'static` future.
//!
//! What is allowed is exactly what `Send` means: **move** the handle to the
//! task that will use it, one task at a time. That is a [`Lease`] — it *owns*
//! the handle while it holds it — and it is why a future holding one is
//! `'static`. It also turns "two coroutines read one handle" from undefined
//! ordering into a queue: the second waits for the first, and a call that was
//! told not to wait ([`Slot::try_acquire`]) says so instead.
//!
//! The wait queue is built here from `std` alone, so that this crate keeps
//! the two dependencies its manifest names.
//!
//! # Cancellation returns the handle
//!
//! A cancelled `asyncio.Task` drops the Rust future, which drops the lease,
//! and the lease's `Drop` puts the handle back in its [`Slot`] and wakes whoever
//! is waiting. That is what keeps "a cancelled receive leaves the handle
//! usable" true without the binding doing anything at the call site, and it is
//! why the lease owns an `Option`: the `Drop` has to be able to take it.
//!
//! # A value that was taken and never delivered goes back
//!
//! A receive can finish on the reactor in the window before the Python task
//! that asked for it has taken the answer: the task is cancelled in that
//! window, and the message the handle gave up has no reader.
//! [`Bridge::awaitable_restoring`](crate::Bridge::awaitable_restoring) hands
//! such a value to a closure, and [`Slot::restore`] is what that closure calls:
//! the value goes to the *front* of the slot's own queue, and the next receive
//! reads [`Lease::restored`] before it reads the handle. Nothing is reordered
//! relative to what the handle still holds, because the restored value was the
//! oldest.
//!
//! **A receive that is already parked on the handle when a value is restored
//! is not woken by it**: it keeps waiting for the handle's next message, and
//! the restored value is returned by the receive after it. Two coroutines
//! reading one handle already have no defined order between them; the
//! guarantee is that nothing is lost, not that the two interleave in arrival
//! order.
//!
//! # A handle that leaves for good
//!
//! A ZeroMQ socket that is split into a sending and a receiving half is
//! consumed by it: the object the caller still holds has nothing left to
//! lease, and a coroutine that is already queued behind a lease must be told
//! so rather than wait for a handle that will never come back.
//! [`Retirable`] is a slot for that: [`Retirable::retire`] takes the handle out
//! for good, and every acquire after it — parked or later — resolves to
//! [`Retired`]. It is an ordinary [`Slot`] over an `Option` underneath, so the
//! lease's wait queue and its cancellation behaviour are the same, and the
//! plain [`Slot`], which nothing retires, keeps an acquire that cannot fail.
//!
//! The mechanism is shared by every binding whose receive consumes something
//! it cannot read twice, or whose handle is used by one coroutine at a time.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

/// Where a handle lives when nobody is using it.
///
/// `I` is the type of what a cancelled receive hands back, for a slot that has
/// anything to hand back; the default `()` is a slot with nothing to restore.
pub struct Slot<S, I = ()> {
    /// A `std::sync::Mutex`, never held across an `await`: it guards a move
    /// and a queue, not an operation.
    state: Mutex<State<S, I>>,
}

struct State<S, I> {
    /// `None` while a [`Lease`] holds the handle.
    held: Option<S>,
    /// What cancelled receives gave back, oldest first.
    restored: VecDeque<I>,
    /// The acquires parked until the handle comes back.
    waiting: Vec<Waiter>,
    /// The next number [`Waiter::ticket`] hands out.
    next_ticket: u64,
}

/// One parked [`Acquire`].
struct Waiter {
    ticket: u64,
    waker: Waker,
}

impl<S, I> State<S, I> {
    /// The wakers of every parked acquire, leaving the queue empty: each
    /// parks again, under the same lock, if the handle is gone by the time it
    /// is polled.
    fn drain_waiters(&mut self) -> Vec<Waker> {
        self.waiting.drain(..).map(|waiter| waiter.waker).collect()
    }
}

impl<S, I> Slot<S, I> {
    /// A slot holding `value`.
    pub fn new(value: S) -> Arc<Slot<S, I>> {
        Arc::new(Slot {
            state: Mutex::new(State {
                held: Some(value),
                restored: VecDeque::new(),
                waiting: Vec::new(),
                next_ticket: 0,
            }),
        })
    }

    /// Takes the handle, waiting for whoever has it to give it back.
    ///
    /// The wait is unbounded on purpose: it ends when the operation in front
    /// finishes or is cancelled, and putting a timeout on *queueing* would turn
    /// a slow peer into a failure no protocol named. A caller that wants a
    /// bound puts it on its own `await` (`asyncio.wait_for`), which cancels
    /// this one.
    pub fn acquire(self: &Arc<Slot<S, I>>) -> Acquire<S, I> {
        Acquire {
            slot: Arc::clone(self),
            ticket: None,
        }
    }

    /// Takes the handle if it is free, and does not wait if it is not.
    pub fn try_acquire(self: &Arc<Slot<S, I>>) -> Option<Lease<S, I>> {
        let value = self.lock().held.take()?;
        Some(Lease {
            slot: Arc::clone(self),
            value: Some(value),
        })
    }

    /// Gives back something a receive took and nobody read, so that the next
    /// receive returns it before it reads the handle.
    ///
    /// Callable from any thread and at any time, including while a lease is
    /// out. It does not wake a receive that is already waiting on the handle;
    /// see the module documentation for why that is the whole of the promise.
    pub fn restore(&self, item: I) {
        self.lock().restored.push_front(item);
    }

    fn lock(&self) -> MutexGuard<'_, State<S, I>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The future [`Slot::acquire`] returns.
///
/// Dropping it before it resolves takes it out of the queue, so a cancelled
/// wait leaves nothing behind that a later release would have to step over.
pub struct Acquire<S, I = ()> {
    slot: Arc<Slot<S, I>>,
    /// This wait's place in [`State::waiting`], once it has parked.
    ticket: Option<u64>,
}

impl<S, I> Future for Acquire<S, I> {
    type Output = Lease<S, I>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Lease<S, I>> {
        // `Acquire` holds an `Arc` and a number, so it is `Unpin` and this
        // needs no projection.
        let this = &mut *self;
        let mut state = this.slot.lock();
        if let Some(value) = state.held.take() {
            if let Some(ticket) = this.ticket.take() {
                state.waiting.retain(|waiter| waiter.ticket != ticket);
            }
            drop(state);
            return Poll::Ready(Lease {
                slot: Arc::clone(&this.slot),
                value: Some(value),
            });
        }
        // Parked, or parked again after a release that somebody else won:
        // registered under the lock that `held` is read under, so a release
        // between the look above and the registration cannot be missed.
        let ticket = match this.ticket {
            Some(ticket) => ticket,
            None => {
                let ticket = state.next_ticket;
                state.next_ticket += 1;
                this.ticket = Some(ticket);
                ticket
            }
        };
        match state
            .waiting
            .iter_mut()
            .find(|waiter| waiter.ticket == ticket)
        {
            Some(waiter) => {
                if !waiter.waker.will_wake(cx.waker()) {
                    waiter.waker.clone_from(cx.waker());
                }
            }
            None => state.waiting.push(Waiter {
                ticket,
                waker: cx.waker().clone(),
            }),
        }
        Poll::Pending
    }
}

impl<S, I> Drop for Acquire<S, I> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket {
            self.slot
                .lock()
                .waiting
                .retain(|waiter| waiter.ticket != ticket);
        }
    }
}

/// Exclusive ownership of the handle, given back when this is dropped.
pub struct Lease<S, I = ()> {
    slot: Arc<Slot<S, I>>,
    /// Always `Some` until `Drop`.
    value: Option<S>,
}

impl<S, I> Lease<S, I> {
    /// The oldest value a cancelled receive gave back, if there is one.
    ///
    /// A receive asks this before it asks the handle.
    pub fn restored(&mut self) -> Option<I> {
        self.slot.lock().restored.pop_front()
    }

    /// The oldest restored value `wanted` accepts, leaving the others where
    /// they are. For a slot whose receives take different kinds of thing from
    /// one queue and must not hand one of them the other's.
    pub fn restored_where(&mut self, wanted: impl FnMut(&I) -> bool) -> Option<I> {
        let mut state = self.slot.lock();
        let at = state.restored.iter().position(wanted)?;
        state.restored.remove(at)
    }
}

impl<S, I> Deref for Lease<S, I> {
    type Target = S;

    fn deref(&self) -> &S {
        self.value.as_ref().expect("a lease holds its handle")
    }
}

impl<S, I> DerefMut for Lease<S, I> {
    fn deref_mut(&mut self) -> &mut S {
        self.value.as_mut().expect("a lease holds its handle")
    }
}

impl<S, I> Drop for Lease<S, I> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            let wakers = {
                let mut state = self.slot.lock();
                state.held = Some(value);
                state.drain_waiters()
            };
            // Woken after the lock is released: a waker is arbitrary code.
            for waker in wakers {
                waker.wake();
            }
        }
    }
}

/// What a [`Retirable`] answers once its handle has left for good.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retired;

/// A [`Slot`] whose handle can be taken out for good.
///
/// Cloning shares the slot, as cloning the `Arc` a plain slot lives in does.
pub struct Retirable<S> {
    /// `None` for good once retired: the handle is an `Option` so that the
    /// slot's own lease and wait queue carry the retirement, and so that a
    /// coroutine parked in [`acquire`](Retirable::acquire) finds it when the
    /// lease in front of it ends rather than needing a wake-up of its own.
    slot: Arc<Slot<Option<S>>>,
}

impl<S> Clone for Retirable<S> {
    fn clone(&self) -> Retirable<S> {
        Retirable {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl<S> Retirable<S> {
    /// A slot holding `value`.
    pub fn new(value: S) -> Retirable<S> {
        Retirable {
            slot: Slot::new(Some(value)),
        }
    }

    /// Takes the handle, waiting for whoever has it to give it back, as
    /// [`Slot::acquire`] does.
    ///
    /// # Errors
    ///
    /// [`Retired`] once the handle was taken out for good, including while
    /// this call was waiting for it.
    pub async fn acquire(&self) -> Result<Live<S>, Retired> {
        Live::of(self.slot.acquire().await)
    }

    /// Takes the handle if it is free, and does not wait if it is not:
    /// `Ok(None)` is "somebody else has it".
    ///
    /// # Errors
    ///
    /// [`Retired`] once the handle was taken out for good.
    pub fn try_acquire(&self) -> Result<Option<Live<S>>, Retired> {
        match self.slot.try_acquire() {
            None => Ok(None),
            Some(lease) => Live::of(lease).map(Some),
        }
    }

    /// Takes the handle out for good, waiting for a lease in front of this
    /// call to end first.
    ///
    /// # Errors
    ///
    /// [`Retired`] if another call retired it first.
    pub async fn retire(&self) -> Result<S, Retired> {
        let mut lease = self.slot.acquire().await;
        // The lease goes back to the slot empty when it drops here, which is
        // what retires it and wakes the acquires queued behind this one.
        lease.take().ok_or(Retired)
    }
}

/// Exclusive ownership of a [`Retirable`]'s handle, given back when this is
/// dropped.
pub struct Live<S> {
    /// Always `Some`: [`Live::of`] refuses an empty one, and only
    /// [`Retirable::retire`] empties the slot, which it does under a lease
    /// of its own.
    lease: Lease<Option<S>>,
}

impl<S> Live<S> {
    fn of(lease: Lease<Option<S>>) -> Result<Live<S>, Retired> {
        // An empty lease is dropped here and goes back to the slot as it was,
        // so the next acquire queued behind it finds the same thing.
        if lease.is_some() {
            Ok(Live { lease })
        } else {
            Err(Retired)
        }
    }
}

impl<S> Deref for Live<S> {
    type Target = S;

    fn deref(&self) -> &S {
        let handle: &Option<S> = &self.lease;
        handle.as_ref().expect("a live lease holds its handle")
    }
}

impl<S> DerefMut for Live<S> {
    fn deref_mut(&mut self) -> &mut S {
        let handle: &mut Option<S> = &mut self.lease;
        handle.as_mut().expect("a live lease holds its handle")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;
    use std::time::Duration;

    use super::*;

    /// A waker that records that it was woken, for polling by hand.
    struct Flag(AtomicBool);

    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn poll_once<F: Future + Unpin>(future: &mut F, flag: &Arc<Flag>) -> Poll<F::Output> {
        let waker = Waker::from(Arc::clone(flag));
        Pin::new(future).poll(&mut Context::from_waker(&waker))
    }

    #[test]
    fn a_cancelled_acquire_leaves_the_handle_usable() {
        let slot: Arc<Slot<&str>> = Slot::new("handle");
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let lease = slot.try_acquire().expect("the handle is free");

        // A second coroutine queues behind the lease and is then cancelled.
        let mut waiting = slot.acquire();
        assert!(poll_once(&mut waiting, &flag).is_pending());
        drop(waiting);
        assert!(slot.try_acquire().is_none(), "the lease still holds it");

        drop(lease);
        assert!(
            !flag.0.load(Ordering::SeqCst),
            "a cancelled wait must not be woken: it is not in the queue"
        );
        assert_eq!(*slot.try_acquire().expect("handed back"), "handle");
    }

    #[test]
    fn a_waiting_acquire_is_woken_when_the_lease_ends_and_gets_the_handle() {
        let slot: Arc<Slot<u32>> = Slot::new(7);
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let lease = slot.try_acquire().expect("the handle is free");

        let mut waiting = slot.acquire();
        assert!(poll_once(&mut waiting, &flag).is_pending());
        drop(lease);
        assert!(flag.0.load(Ordering::SeqCst), "the release woke the waiter");
        match poll_once(&mut waiting, &flag) {
            Poll::Ready(lease) => assert_eq!(*lease, 7),
            Poll::Pending => panic!("the handle was free"),
        }
    }

    #[test]
    fn a_waiter_that_lost_the_race_parks_again_and_is_woken_by_the_next_release() {
        let slot: Arc<Slot<u32>> = Slot::new(1);
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let lease = slot.try_acquire().expect("free");

        let mut waiting = slot.acquire();
        assert!(poll_once(&mut waiting, &flag).is_pending());
        drop(lease);
        // Somebody who did not queue takes it first.
        let barger = slot.try_acquire().expect("free again");
        flag.0.store(false, Ordering::SeqCst);
        assert!(poll_once(&mut waiting, &flag).is_pending());

        drop(barger);
        assert!(flag.0.load(Ordering::SeqCst), "it queued again");
        assert!(poll_once(&mut waiting, &flag).is_ready());
    }

    #[test]
    fn restored_values_come_back_oldest_first_and_before_the_handle() {
        let slot: Arc<Slot<&str, u32>> = Slot::new("handle");
        slot.restore(2);
        slot.restore(1);
        let mut lease = slot.try_acquire().expect("free");
        assert_eq!(lease.restored(), Some(1));
        assert_eq!(lease.restored(), Some(2));
        assert_eq!(lease.restored(), None);
    }

    #[test]
    fn a_restore_while_a_lease_is_out_reaches_the_next_lease() {
        let slot: Arc<Slot<&str, u32>> = Slot::new("handle");
        let first = slot.try_acquire().expect("free");
        slot.restore(9);
        drop(first);
        let mut second = slot.try_acquire().expect("free");
        assert_eq!(second.restored(), Some(9));
    }

    #[test]
    fn restored_where_skips_what_is_not_wanted() {
        let slot: Arc<Slot<(), &str>> = Slot::new(());
        slot.restore("outcome");
        slot.restore("delivery");
        let mut lease = slot.try_acquire().expect("free");
        assert_eq!(
            lease.restored_where(|item| *item == "outcome"),
            Some("outcome")
        );
        assert_eq!(lease.restored(), Some("delivery"));
    }

    #[test]
    fn a_lease_dropped_on_another_thread_wakes_a_parked_acquire() {
        let slot: Arc<Slot<u32>> = Slot::new(3);
        let lease = slot.try_acquire().expect("free");
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(lease);
        });
        let lease = futures::executor::block_on(slot.acquire());
        assert_eq!(*lease, 3);
        releaser.join().expect("the releasing thread finished");
    }

    #[test]
    fn a_retired_slot_hands_out_its_handle_once_and_then_refuses() {
        let slot = Retirable::new(5u32);
        assert_eq!(futures::executor::block_on(slot.retire()), Ok(5));
        assert!(futures::executor::block_on(slot.acquire()).is_err());
        assert!(matches!(slot.try_acquire(), Err(Retired)));
        assert_eq!(futures::executor::block_on(slot.retire()), Err(Retired));
    }

    #[test]
    fn a_live_lease_is_given_back_and_try_acquire_does_not_wait() {
        let slot = Retirable::new("handle");
        let held = slot
            .try_acquire()
            .expect("not retired")
            .expect("nobody holds it");
        assert!(
            matches!(slot.try_acquire(), Ok(None)),
            "a busy slot is not a retired one"
        );
        assert_eq!(*held, "handle");
        drop(held);
        assert!(matches!(slot.try_acquire(), Ok(Some(_))));
    }

    #[test]
    fn retiring_waits_for_the_lease_in_front_and_tells_every_acquire_behind_it() {
        let slot = Retirable::new(8u32);
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let held = futures::executor::block_on(slot.acquire()).expect("not retired");

        let mut retiring = Box::pin(slot.retire());
        assert!(poll_once(&mut retiring, &flag).is_pending());
        let mut behind = Box::pin(slot.acquire());
        assert!(poll_once(&mut behind, &flag).is_pending());
        // A call that was told not to wait still sees a held slot as held.
        assert!(matches!(slot.try_acquire(), Ok(None)));

        drop(held);
        assert!(flag.0.load(Ordering::SeqCst), "the release woke a waiter");
        // The retirement was first in line, so it takes the handle; the
        // acquire behind it then finds the slot empty rather than parked.
        match poll_once(&mut retiring, &flag) {
            Poll::Ready(handle) => assert_eq!(handle, Ok(8)),
            Poll::Pending => panic!("the handle was free"),
        }
        match poll_once(&mut behind, &flag) {
            Poll::Ready(outcome) => assert!(outcome.is_err(), "the handle is gone"),
            Poll::Pending => panic!("the retirement released the slot"),
        }
    }

    #[test]
    fn a_retirement_races_parked_acquires_on_other_threads_and_leaves_none_waiting() {
        let slot = Retirable::new(3u32);
        let held = futures::executor::block_on(slot.acquire()).expect("not retired");
        let (done, finished) = std::sync::mpsc::channel();
        for retires in [true, false, false] {
            let slot = slot.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let handed = if retires {
                    futures::executor::block_on(slot.retire()).is_ok()
                } else {
                    futures::executor::block_on(slot.acquire()).is_ok()
                };
                let _ = done.send((retires, handed));
            });
        }
        // All three are parked behind `held` well within this.
        std::thread::sleep(Duration::from_millis(100));
        drop(held);
        let mut retirements = 0;
        for _ in 0..3 {
            let (retires, handed) = finished
                .recv_timeout(Duration::from_secs(10))
                .expect("a parked call was never answered");
            if retires {
                assert!(handed, "the one retirement got the handle");
                retirements += 1;
            }
        }
        assert_eq!(retirements, 1);
        assert!(futures::executor::block_on(slot.acquire()).is_err());
    }
}
