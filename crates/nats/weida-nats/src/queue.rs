//! The byte budget behind every queue of delivered messages.
//!
//! A message **count** is not a memory bound: `subscription_queue` messages of
//! `max_inbound_payload` octets each is `subscription_queue` times that, per
//! subscription, and the subscription table holds thousands of them. So every
//! message the driver hands to an application reader — a subscription's, or the
//! one a pending request waits on — is first charged against one budget for the
//! whole connection, and a message that does not fit is dropped and counted
//! rather than queued. Core NATS is at-most-once, so a dropped copy is a loss
//! the delivery contract already allows; an unbounded queue is not.
//!
//! The charge travels with the message. [`Queued`] is what the channels carry,
//! and dropping it — because the application took the message, because the
//! subscription was dropped with messages still in its channel, because the
//! channel was closed — gives the bytes back. There is no release call to
//! forget.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::message::Message;

/// The least a queued message is charged, in octets.
///
/// A message's subject, reply subject and payload are slices of the read
/// buffer chunk the server's octets arrived in (`transport::READ_CHUNK`), and a
/// slice keeps its whole chunk alive for as long as it is queued: a ten-octet
/// message held up by a slow reader pins up to a chunk's worth of memory that
/// the neighbours it arrived with have already released. Charging only what the
/// message *is* would let a peer that interleaves small messages for a stalled
/// reader with other traffic hold memory the budget never counted. A quarter of
/// a chunk keeps what is retained within a small constant factor of what is
/// charged — about four, and at most twice that where the buffer's growth
/// over-allocated — whatever the interleaving, and costs a reader of small
/// messages nothing it
/// would notice: the per-subscription count bound is reached long before the
/// budget at this size.
pub(crate) const PER_MESSAGE_OVERHEAD: usize = 4 * 1024;

/// The octets of delivered messages that may wait for readers, over one
/// connection.
#[derive(Debug)]
pub(crate) struct Budget {
    limit: usize,
    used: AtomicUsize,
}

impl Budget {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    /// Reserves `bytes`, or `None` where that would pass the limit.
    ///
    /// Lock-free: the driver is the only task that charges, and readers only
    /// ever give bytes back, so a concurrent release can make the answer
    /// pessimistic by a moment and never optimistic.
    pub(crate) fn try_charge(self: &Arc<Self>, bytes: usize) -> Option<Charge> {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let next = used.checked_add(bytes).filter(|next| *next <= self.limit)?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Some(Charge {
                        budget: Arc::clone(self),
                        bytes,
                    });
                }
                Err(now) => used = now,
            }
        }
    }

    /// What is charged now.
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// Bytes reserved in a [`Budget`], given back when this is dropped.
#[derive(Debug)]
pub(crate) struct Charge {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// A message waiting for its reader, with the budget it holds.
#[derive(Debug)]
pub(crate) struct Queued {
    message: Message,
    /// Held, never read: releasing it is dropping it.
    _charge: Charge,
}

impl Queued {
    pub(crate) fn new(message: Message, charge: Charge) -> Self {
        Self {
            message,
            _charge: charge,
        }
    }

    /// The message, with its charge released: the reader has it now, and what
    /// a reader holds is the application's memory rather than the queue's.
    pub(crate) fn into_message(self) -> Message {
        self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The budget is a bound: a charge that does not fit is refused and
    /// leaves nothing behind.
    #[test]
    fn a_charge_past_the_limit_is_refused() {
        let budget = Budget::new(100);
        let first = budget.try_charge(60).expect("fits");
        assert!(budget.try_charge(41).is_none(), "100 - 60 = 40 left");
        assert_eq!(budget.used(), 60, "a refusal reserves nothing");
        let second = budget.try_charge(40).expect("exactly what is left");
        assert_eq!(budget.used(), 100);
        drop((first, second));
    }

    /// Giving the bytes back is dropping the charge, wherever that happens.
    #[test]
    fn dropping_a_charge_releases_it() {
        let budget = Budget::new(100);
        let charge = budget.try_charge(100).expect("fits");
        assert!(budget.try_charge(1).is_none());
        drop(charge);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_charge(100).is_some());
    }

    /// An amount that would overflow the counter is a refusal, not a wrap.
    #[test]
    fn a_charge_that_overflows_is_refused() {
        let budget = Budget::new(usize::MAX);
        let held = budget.try_charge(usize::MAX - 1).expect("fits");
        assert!(budget.try_charge(usize::MAX).is_none());
        drop(held);
    }
}
