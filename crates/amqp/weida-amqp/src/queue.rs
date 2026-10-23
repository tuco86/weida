//! The queue between the connection driver and a handle.
//!
//! The driver is one task for every session and link of a connection, so it
//! must never wait for an application: an application is under no obligation
//! to read what a link tells it, and a driver that waited for one would stop
//! reading its peer, stop answering it and stop timing it out, for every link
//! at once. A bounded channel that the driver `send`s into is exactly that
//! wait, so this is not one.
//!
//! [`Sender::push`] never waits and never fails for want of room. What keeps
//! the queue from growing without bound is not a capacity but where the
//! growth comes from:
//!
//! * **data** — a delivery — is queued only against credit the application
//!   itself granted, so the depth is whatever it asked for;
//! * a **report** — an outcome, a credit update — is a fact the link also
//!   keeps in its own state, so it is merged into the report behind it where
//!   it says the same thing and, past the bound the channel was made with,
//!   the oldest one is let go. That includes the final outcome of a delivery
//!   a sender sent: the link keeps the delivery, with the outcome, until the
//!   application has taken it. An application that wants every report reads
//!   them; one that does not loses the oldest and keeps the current state,
//!   which it can always ask for.
//!
//! The order is the order the driver saw things in, because a reader draining
//! a link relies on it: "credit reached zero" only means "nothing came" if no
//! delivery is queued ahead of the report that says so.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// What the queue needs to know about an item.
pub(crate) trait Queued: Sized {
    /// Whether this is a report: a fact that is also kept in the owner's own
    /// state, and that a reader who never comes may lose the oldest of.
    fn is_report(&self) -> bool;

    /// Folds `next`, which arrived directly behind `self`, into `self` when
    /// the two say one thing. `None` means it was absorbed; `Some` hands it
    /// back to be queued on its own.
    fn absorb(&mut self, next: Self) -> Option<Self>;
}

struct State<T> {
    items: VecDeque<T>,
    /// How many of `items` are reports.
    reports: usize,
    /// The sender is gone or has said it is done: once the queue is empty
    /// there will be nothing more.
    closed: bool,
    /// The receiver is gone: nothing queued will ever be read.
    abandoned: bool,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    ready: Notify,
    /// How many reports are kept for a reader that has not come.
    max_reports: usize,
}

/// A queue with no capacity and a bound on how many reports can pile up in
/// it.
pub(crate) fn channel<T: Queued>(max_reports: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            items: VecDeque::new(),
            reports: 0,
            closed: false,
            abandoned: false,
        }),
        ready: Notify::new(),
        max_reports,
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

/// The driver's end. Never waits.
pub(crate) struct Sender<T: Queued> {
    shared: Arc<Shared<T>>,
}

impl<T: Queued> Sender<T> {
    /// Queues `item` behind everything already there.
    ///
    /// Dropped where the receiver has gone, which is the application saying
    /// it will never read it.
    pub(crate) fn push(&self, item: T) {
        let mut state = self.shared.state.lock().expect("not poisoned");
        if state.abandoned || state.closed {
            return;
        }
        let item = match state.items.back_mut() {
            Some(tail) => match tail.absorb(item) {
                None => {
                    drop(state);
                    self.shared.ready.notify_one();
                    return;
                }
                Some(item) => item,
            },
            None => item,
        };
        if item.is_report() {
            if state.reports >= self.shared.max_reports
                && let Some(oldest) = state.items.iter().position(T::is_report)
            {
                state.items.remove(oldest);
                state.reports -= 1;
            }
            state.reports += 1;
        }
        state.items.push_back(item);
        drop(state);
        self.shared.ready.notify_one();
    }

    /// Says there will be nothing more: the receiver gets what is queued and
    /// then `None`.
    pub(crate) fn close(&self) {
        self.shared.state.lock().expect("not poisoned").closed = true;
        self.shared.ready.notify_one();
    }
}

impl<T: Queued> Drop for Sender<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T: Queued> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.state.lock().expect("not poisoned");
        f.debug_struct("Sender")
            .field("queued", &state.items.len())
            .field("closed", &state.closed)
            .finish()
    }
}

/// The handle's end.
pub(crate) struct Receiver<T: Queued> {
    shared: Arc<Shared<T>>,
}

impl<T: Queued> Receiver<T> {
    /// The next item, or `None` once the sender is done and the queue is
    /// empty. Cancel-safe: nothing is held between the awaits.
    pub(crate) async fn recv(&mut self) -> Option<T> {
        loop {
            {
                let mut state = self.shared.state.lock().expect("not poisoned");
                if let Some(item) = state.items.pop_front() {
                    if item.is_report() {
                        state.reports -= 1;
                    }
                    return Some(item);
                }
                if state.closed {
                    return None;
                }
            }
            // A permit is stored when the push came first, so an item that
            // landed between the check and this await is not missed.
            self.shared.ready.notified().await;
        }
    }

    /// Drops every queued item `unwanted` picks out.
    ///
    /// For an item that has been answered another way, so that reading the
    /// queue afterwards does not answer the same question twice.
    pub(crate) fn discard(&mut self, unwanted: impl Fn(&T) -> bool) {
        let mut state = self.shared.state.lock().expect("not poisoned");
        let mut gone = 0;
        state.items.retain(|item| {
            if unwanted(item) {
                gone += usize::from(item.is_report());
                false
            } else {
                true
            }
        });
        state.reports -= gone;
    }
}

impl<T: Queued> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().expect("not poisoned");
        state.abandoned = true;
        state.items.clear();
        state.reports = 0;
    }
}

impl<T: Queued> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.state.lock().expect("not poisoned");
        f.debug_struct("Receiver")
            .field("queued", &state.items.len())
            .field("closed", &state.closed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Small, so that the tests can fill it.
    const BOUND: usize = 16;

    #[derive(Debug, PartialEq)]
    enum Item {
        Data(u32),
        /// A report that supersedes the report directly before it.
        Level(u32),
        /// A report that stands on its own.
        Note(u32),
    }

    impl Queued for Item {
        fn is_report(&self) -> bool {
            !matches!(self, Self::Data(_))
        }

        fn absorb(&mut self, next: Self) -> Option<Self> {
            match (&mut *self, next) {
                (Self::Level(have), Self::Level(newer)) => {
                    *have = newer;
                    None
                }
                (_, next) => Some(next),
            }
        }
    }

    async fn next(receiver: &mut Receiver<Item>) -> Option<Item> {
        weida_testkit::within(
            "the queue to answer",
            Duration::from_secs(5),
            receiver.recv(),
        )
        .await
    }

    #[tokio::test]
    async fn the_order_is_the_order_the_driver_saw_things_in() {
        let (sender, mut receiver) = channel::<Item>(BOUND);
        sender.push(Item::Level(1));
        sender.push(Item::Data(1));
        sender.push(Item::Level(2));
        assert_eq!(next(&mut receiver).await, Some(Item::Level(1)));
        assert_eq!(next(&mut receiver).await, Some(Item::Data(1)));
        assert_eq!(next(&mut receiver).await, Some(Item::Level(2)));
    }

    #[tokio::test]
    async fn a_report_is_merged_into_the_one_directly_behind_which_it_lands() {
        let (sender, mut receiver) = channel::<Item>(BOUND);
        for level in 0..10_000 {
            sender.push(Item::Level(level));
        }
        assert_eq!(next(&mut receiver).await, Some(Item::Level(9_999)));
        sender.close();
        assert_eq!(
            next(&mut receiver).await,
            None,
            "one item, not ten thousand"
        );
    }

    #[tokio::test]
    async fn a_reader_who_never_comes_loses_old_reports_and_never_data() {
        let (sender, mut receiver) = channel::<Item>(BOUND);
        sender.push(Item::Data(0));
        for note in 0..(BOUND as u32 * 3) {
            sender.push(Item::Note(note));
        }
        sender.push(Item::Data(1));
        sender.close();

        let mut data = Vec::new();
        let mut notes = Vec::new();
        while let Some(item) = next(&mut receiver).await {
            match item {
                Item::Data(n) => data.push(n),
                Item::Note(n) => notes.push(n),
                Item::Level(_) => unreachable!("none was pushed"),
            }
        }
        assert_eq!(data, vec![0, 1], "data is never dropped");
        assert_eq!(notes.len(), BOUND, "reports are bounded");
        assert_eq!(
            notes.last().copied(),
            Some(BOUND as u32 * 3 - 1),
            "and what is kept is the newest"
        );
    }

    #[tokio::test]
    async fn the_sender_never_waits_however_much_is_queued() {
        let (sender, _receiver) = channel::<Item>(BOUND);
        // A bounded channel would stop taking items at its capacity; the
        // loop finishing is the assertion.
        for n in 0..100_000 {
            sender.push(Item::Data(n));
        }
    }

    #[tokio::test]
    async fn closing_ends_the_stream_after_what_is_queued() {
        let (sender, mut receiver) = channel::<Item>(BOUND);
        sender.push(Item::Data(7));
        drop(sender);
        assert_eq!(next(&mut receiver).await, Some(Item::Data(7)));
        assert_eq!(next(&mut receiver).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_reader_is_woken_by_a_push() {
        let (sender, mut receiver) = channel::<Item>(BOUND);
        let reader = tokio::spawn(async move { next(&mut receiver).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender.push(Item::Data(3));
        assert_eq!(reader.await.unwrap(), Some(Item::Data(3)));
    }

    #[tokio::test]
    async fn nothing_is_kept_for_a_receiver_that_has_gone() {
        let (sender, receiver) = channel::<Item>(BOUND);
        sender.push(Item::Data(1));
        drop(receiver);
        sender.push(Item::Data(2));
        let state = sender.shared.state.lock().expect("not poisoned");
        assert!(state.items.is_empty());
    }
}
