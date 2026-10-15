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
//! The mechanism is shared by every binding whose receive consumes something
//! it cannot read twice. `weida-zmq-py` keeps its own copy of this file for the
//! moment: it additionally *retires* a slot when a socket is split into two
//! halves, which nothing else here does, and it moves here with that state.

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
}
