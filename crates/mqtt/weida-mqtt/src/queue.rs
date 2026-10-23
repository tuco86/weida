//! The queue between the connection task and the application's event stream:
//! bounded in entries **and** in bytes, with the producer never blocked.
//!
//! A message count is not a memory bound (`docs/INVARIANTS.md` says so for
//! every foreign-protocol library): at `incoming_queue` entries of
//! `maximum_packet_size` each, a count alone lets one peer pin a gigabyte. So an
//! entry is charged what it holds, and the queue admits one only while both
//! numbers stay under their ceiling.
//!
//! **A queue that is empty still accepts one entry of any size**, because a
//! bound that refuses the only entry in an otherwise empty queue is a deadlock
//! and not a bound: the exposure is `max_bytes - 1 + one entry`, and one
//! entry's size is the packet size cap's to bound.
//!
//! **The producer never awaits the consumer.** [`Producer::offer`] hands the
//! entry back where the queue is full, and the connection task then stops
//! *reading* until [`Producer::space`] says an offer would be admitted, while it
//! keeps answering commands and timers. That is the whole difference from a
//! bounded channel's `send().await`, which parks the one task that owns the
//! socket for as long as the application is slow.
//!
//! The two small kinds of entry that are not peer-driven bulk — the end of the
//! connection and the completion of an exchange the session already held — go
//! in with [`Producer::push`], which is not subject to either ceiling and is
//! bounded by what produces it.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

/// A queue of at most `max_entries` entries holding at most `max_bytes` bytes,
/// as its producer and consumer ends.
pub(crate) fn channel<T>(max_entries: usize, max_bytes: usize) -> (Producer<T>, Consumer<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            entries: VecDeque::new(),
            bytes: 0,
            finished: false,
            abandoned: false,
        }),
        space: Notify::new(),
        ready: Notify::new(),
        max_entries,
        max_bytes,
    });
    (
        Producer {
            shared: Arc::clone(&shared),
        },
        Consumer { shared },
    )
}

struct Shared<T> {
    state: Mutex<State<T>>,
    /// Woken when the consumer takes an entry, which is the producer's wait
    /// for space. `Notify` keeps one permit when nobody is waiting, so a wake
    /// that lands between the producer's check and its wait is not lost.
    space: Notify,
    /// Woken when the producer adds an entry or ends, which is the consumer's
    /// wait for something to take.
    ready: Notify,
    max_entries: usize,
    max_bytes: usize,
}

struct State<T> {
    /// Each entry with what it was charged.
    entries: VecDeque<(T, usize)>,
    bytes: usize,
    /// The producer is gone: what is queued is the last of it.
    finished: bool,
    /// The consumer is gone: nothing queued will ever be read.
    abandoned: bool,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        // Nothing in this module panics while holding the lock, and a poisoned
        // queue is still a consistent one.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether an entry charged `bytes` would be admitted now.
    fn admits(&self, state: &State<T>, bytes: usize) -> bool {
        state.abandoned
            || state.entries.is_empty()
            || (state.entries.len() < self.max_entries
                && state.bytes.saturating_add(bytes) <= self.max_bytes)
    }
}

/// The end the connection task holds.
pub(crate) struct Producer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Producer<T> {
    /// Adds `entry`, charged `bytes`, or hands it back where the queue is full.
    ///
    /// Where the consumer has gone the entry is dropped and the answer is
    /// success: an application that dropped its event stream has said it wants
    /// no deliveries, and the connection is still its to use.
    pub(crate) fn offer(&self, entry: T, bytes: usize) -> Result<(), T> {
        let mut state = self.shared.lock();
        if state.abandoned {
            return Ok(());
        }
        if !self.shared.admits(&state, bytes) {
            return Err(entry);
        }
        state.bytes = state.bytes.saturating_add(bytes);
        state.entries.push_back((entry, bytes));
        drop(state);
        self.shared.ready.notify_one();
        Ok(())
    }

    /// Adds `entry` whatever the ceilings say.
    ///
    /// For the entries the application must see and that nothing a peer sends
    /// can multiply: the last one, and the completion or loss of an exchange
    /// the session was already holding, each bounded by the session's own
    /// in-flight ceiling.
    pub(crate) fn push(&self, entry: T) {
        let mut state = self.shared.lock();
        if state.abandoned {
            return;
        }
        state.entries.push_back((entry, 0));
        drop(state);
        self.shared.ready.notify_one();
    }

    /// Completes when [`Producer::offer`] of an entry charged `bytes` would be
    /// admitted, which is when the consumer has taken enough.
    ///
    /// Cancel-safe: it holds nothing, and re-checks the queue each time it is
    /// woken.
    pub(crate) async fn space(&self, bytes: usize) {
        loop {
            {
                let state = self.shared.lock();
                if self.shared.admits(&state, bytes) {
                    return;
                }
            }
            self.shared.space.notified().await;
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.shared.lock().finished = true;
        self.shared.ready.notify_one();
    }
}

/// The end the application holds.
pub(crate) struct Consumer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Consumer<T> {
    /// The next entry, or `None` once the producer has ended and everything it
    /// queued has been taken.
    ///
    /// Cancel-safe: an entry leaves the queue in the same step that returns it.
    pub(crate) async fn next(&mut self) -> Option<T> {
        loop {
            {
                let mut state = self.shared.lock();
                if let Some((entry, bytes)) = state.entries.pop_front() {
                    state.bytes -= bytes;
                    drop(state);
                    self.shared.space.notify_one();
                    return Some(entry);
                }
                if state.finished {
                    return None;
                }
            }
            self.shared.ready.notified().await;
        }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // What is queued will never be read, so it is released now rather than
        // held for as long as the connection lives; and a producer waiting for
        // space is told there is some, because nothing will ever be admitted
        // again and nothing will ever be waited on.
        let released = {
            let mut state = self.shared.lock();
            state.abandoned = true;
            state.bytes = 0;
            std::mem::take(&mut state.entries)
        };
        drop(released);
        self.shared.space.notify_one();
    }
}

impl<T> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.lock();
        f.debug_struct("Consumer")
            .field("queued", &state.entries.len())
            .field("bytes", &state.bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Every await in these tests is reported when it runs long, so a wake
    /// that never comes is seen; on a paused clock it fails at once.
    async fn bounded<F: Future>(future: F) -> F::Output {
        weida_testkit::within("a step of the test", Duration::from_secs(5), future).await
    }

    #[tokio::test]
    async fn entries_come_out_in_the_order_they_went_in() {
        let (producer, mut consumer) = channel(8, 1024);
        for entry in 0..3u8 {
            producer.offer(entry, 1).expect("room");
        }
        for entry in 0..3u8 {
            assert_eq!(bounded(consumer.next()).await, Some(entry));
        }
    }

    #[tokio::test]
    async fn the_count_is_a_ceiling() {
        let (producer, mut consumer) = channel(2, 1 << 20);
        producer.offer(1u8, 1).expect("room");
        producer.offer(2, 1).expect("room");
        assert_eq!(producer.offer(3, 1), Err(3), "a third is handed back");

        assert_eq!(bounded(consumer.next()).await, Some(1));
        producer.offer(3, 1).expect("one slot came free");
    }

    /// The point of the module: a count alone would admit these, and the bytes
    /// do not.
    #[tokio::test]
    async fn the_bytes_are_a_ceiling_beside_the_count() {
        let (producer, mut consumer) = channel(1000, 100);
        producer.offer("a", 60).expect("room");
        assert_eq!(producer.offer("b", 60), Err("b"), "120 bytes against 100");
        producer.offer("c", 40).expect("exactly the budget");

        assert_eq!(bounded(consumer.next()).await, Some("a"));
        producer.offer("b", 60).expect("the 60 came back");
    }

    /// A bound that refused the only entry of an empty queue would be a
    /// deadlock, so the first entry is admitted at any size and the second
    /// waits for it.
    #[tokio::test]
    async fn an_empty_queue_admits_one_entry_of_any_size() {
        let (producer, mut consumer) = channel(8, 100);
        producer.offer("huge", 1 << 30).expect("the only entry");
        assert_eq!(producer.offer("next", 1), Err("next"));

        assert_eq!(bounded(consumer.next()).await, Some("huge"));
        producer.offer("next", 1).expect("the queue is empty again");
    }

    #[tokio::test(start_paused = true)]
    async fn space_completes_when_the_consumer_takes_enough() {
        let (producer, mut consumer) = channel(8, 100);
        producer.offer(1u8, 90).expect("room");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), producer.space(50))
                .await
                .is_err(),
            "90 + 50 is over 100"
        );

        let waiting = bounded(async {
            let (waited, taken) = tokio::join!(producer.space(50), consumer.next());
            assert_eq!(taken, Some(1));
            waited
        });
        waiting.await;
        producer.offer(2, 50).expect("room now");
    }

    /// A consumer that took an entry before the producer began to wait leaves
    /// the wake behind, so the wait ends at once rather than never.
    #[tokio::test]
    async fn a_wake_before_the_wait_is_not_lost() {
        let (producer, mut consumer) = channel(1, 100);
        producer.offer(1u8, 1).expect("room");
        assert_eq!(producer.offer(2, 1), Err(2));
        assert_eq!(bounded(consumer.next()).await, Some(1));
        bounded(producer.space(1)).await;
    }

    #[tokio::test]
    async fn the_end_is_seen_after_everything_queued() {
        let (producer, mut consumer) = channel(8, 100);
        producer.offer(1u8, 1).expect("room");
        drop(producer);
        assert_eq!(bounded(consumer.next()).await, Some(1));
        assert_eq!(bounded(consumer.next()).await, None);
    }

    /// An application that dropped its stream does not stop the connection:
    /// what the producer offers is discarded and counted as taken.
    #[tokio::test]
    async fn a_dropped_consumer_discards_instead_of_refusing() {
        let (producer, consumer) = channel(1, 1);
        producer.offer(1u8, 1).expect("room");
        drop(consumer);
        for entry in 2..100u8 {
            producer.offer(entry, 1 << 20).expect("discarded");
        }
        bounded(producer.space(1 << 20)).await;
    }

    /// The forced push is for the entries no peer can multiply, and it is the
    /// only way past a full queue.
    #[tokio::test]
    async fn a_forced_push_ignores_the_ceilings() {
        let (producer, mut consumer) = channel(1, 10);
        producer.offer(1u8, 10).expect("room");
        producer.push(2);
        assert_eq!(bounded(consumer.next()).await, Some(1));
        assert_eq!(bounded(consumer.next()).await, Some(2));
    }

    /// `next` is dropped mid-wait routinely (a receive with a deadline), and
    /// must not lose what arrives afterwards.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_next_loses_nothing() {
        let (producer, mut consumer) = channel(8, 100);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), consumer.next())
                .await
                .is_err()
        );
        producer.offer(7u8, 1).expect("room");
        assert_eq!(bounded(consumer.next()).await, Some(7));
    }
}
