//! Delivery: one message written to one consumer, and the report that settles
//! it.
//!
//! A write to a consumer waits on that consumer's stream credit, so it runs in
//! a task of its own and never in the queue's event loop. The loop hands out
//! at most one write per subscription at a time ([`crate::consumers`]); a
//! consumer that stops reading therefore holds one message and one task until
//! the deadline of [`crate::peer::Pace`] ends the write, and every other
//! consumer is served meanwhile. A write that ends — in bytes out or in
//! failure — is an event for the loop, which is the only place queue state
//! moves in response to a consumer.
//!
//! **A failure is about the connection or about the message, and the two are
//! answered differently.** A write that fails or stalls says the consumer's
//! connection is not taking deliveries: dropping the subscription alone would
//! leave a client that still believes it is subscribed, so the connection is
//! closed ([`weida::codes::LIMIT_EXCEEDED`]) and the client's redial
//! subscribes again. A delivery that never started because its header is one
//! the consumer's connection will not read says nothing about the connection
//! — it carries on, and the next message may be fine — so it is only
//! refused: the message goes back for other consumers and is not offered to
//! that connection again ([`Undelivered`]). Closing the connection for it
//! would turn one such message into a loop of redials, each offered the same
//! message first.
//!
//! **A delivery's tasks live as long as the queue's entry for it.** Both the
//! write and the watcher stop when the entry goes ([`Outgoing::cancelled`]),
//! so a delivery the queue took back — its subscription ended, its consumer
//! was dropped — leaves no task behind, and the cursor state its consumer's
//! connection kept for the report goes with it.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use weida::{Acknowledgement, Consumer, CursorLevel, Cursors, Error, TransferMeta, codes};

use crate::broker::Event;
use crate::broker::Served;
use crate::peer::{Pace, Stalled};
use crate::queue::{DeliveryId, Outgoing};

/// The level a delivery orders and a settlement is.
///
/// One constant rather than a parameter: what a queue needs to know is that
/// its consumer processed the message, and a queue that let an operator
/// choose a different level would be letting them choose what "settled"
/// means.
const SETTLEMENT: CursorLevel = CursorLevel::Known(Acknowledgement::Processed);

/// How much of a payload one write hands the transport at a time.
///
/// A write is one wait, and a wait has one deadline, so the slice is the unit
/// of progress: small enough that a consumer receiving at
/// [`crate::peer::MIN_PEER_RATE`] finishes a slice well inside the window, and
/// large enough that a message is a handful of calls.
const WRITE_SLICE: usize = 64 * 1024;

/// A message handed to a consumer, and the handle to write it with.
pub(crate) struct Dispatch {
    pub(crate) consumer: Arc<Consumer>,
    pub(crate) outgoing: Outgoing,
}

/// Writes one delivery in a task of its own and tells the queue's loop how it
/// ended.
///
/// **A delivery is not a deletion.** Each one orders `Processed` on its own
/// DATA header and keeps the cursors that come back. The queue still holds the
/// message — charged — until the loop is told the consumer reported it; this
/// task holds only a share of the payload to write it, and lets go the moment
/// the write is over.
pub(crate) fn spawn(served: &Served, dispatch: Dispatch) {
    let events = served.events.clone();
    let idle = served.io_timeout;
    tokio::spawn(async move {
        let Dispatch { consumer, outgoing } = dispatch;
        let Outgoing {
            id,
            body,
            topic,
            content_type,
            trace,
            mut cancelled,
        } = outgoing;
        let meta = TransferMeta {
            content_type,
            content_len: Some(body.len() as u64),
            trace,
            topic,
            achieved: None,
            ..TransferMeta::default()
        }
        // The order that makes settlement possible, and the only level this
        // queue asks its consumer for: `Processed` is the consumer's own
        // statement about its own hop (`GUARANTEES.md` §1).
        .with_report([SETTLEMENT]);
        let sent = tokio::select! {
            sent = send(&consumer, meta, &body, idle) => sent,
            // The queue took the delivery back while it was being written:
            // the stream is reset with the future, and nobody is waiting for
            // an outcome.
            _ = &mut cancelled => return,
        };
        drop(body);
        match sent {
            Ok(Some(cursors)) => {
                drop(consumer);
                // Sent before the watcher exists, so the queue's loop sees the
                // write end before it can see the delivery settle.
                if events.send(Event::Written(id)).await.is_ok() {
                    tokio::spawn(watch(id, cursors, cancelled, events));
                }
            }
            // No cursors means the header carried no report, which cannot
            // happen for a delivery this module built — but a settlement that
            // will never come would strand the message and its slot, so the
            // honest answer is to give it back now rather than to hold it
            // forever.
            Ok(None) => {
                drop(consumer);
                tracing::error!("a delivery ordered a report and got no cursors; requeuing");
                let _ = events.send(Event::Written(id)).await;
                let _ = events.send(Event::Lost(id)).await;
            }
            // Nothing was written and the connection is as it was: the
            // message goes back for somebody else and this consumer goes on.
            Err(Undelivered::Refused) => {
                drop(consumer);
                tracing::debug!(
                    "a consumer's connection will not read a delivery's header; \
                     the message goes back for other consumers"
                );
                let _ = events.send(Event::Refused(id)).await;
            }
            Err(e @ (Undelivered::Stalled | Undelivered::Failed(_))) => {
                tracing::debug!(error = %e, "a delivery failed; closing the consumer's connection and requeuing the message");
                // The connection is closed before the queue hears of it. The
                // subscription is dropped either way, and a consumer left
                // holding an open connection would never learn that it is
                // not being served: its redial, which subscribes again, is
                // triggered by the close and by nothing else.
                consumer.close(codes::LIMIT_EXCEEDED, e.close_reason());
                drop(consumer);
                let _ = events.send(Event::Failed(id)).await;
            }
        }
    });
}

/// Why a delivery did not reach its consumer.
///
/// Sorted by **what failed**, because the answers differ: the connection
/// is closed for the first two and kept for the third.
#[derive(Debug)]
enum Undelivered {
    /// The consumer made no progress inside the deadline.
    Stalled,
    /// The transport refused: the connection is gone, or the consumer stopped
    /// the stream.
    Failed(Error),
    /// A limit that is about this message and not about the connection: the
    /// header the delivery needs is longer than the consumer's connection said
    /// it will read, or exceeds a cap no peer would read. Nothing was written.
    Refused,
}

impl std::fmt::Display for Undelivered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Undelivered::Stalled => f.write_str("the consumer stopped reading"),
            Undelivered::Failed(e) => e.fmt(f),
            Undelivered::Refused => f.write_str("the consumer will not read this delivery"),
        }
    }
}

impl Undelivered {
    /// Sorts what opening a delivery failed with.
    ///
    /// [`Error::LimitExceeded`] from opening is the message's own: the header
    /// is checked against what the consumer's connection will read before a
    /// stream is opened, so nothing was written and the connection is
    /// untouched. Anything else — a stream that could not be opened, a head
    /// that could not be written, a connection that is gone — is the
    /// connection's.
    ///
    /// A connection its peer closed with `LIMIT_EXCEEDED` is reported as the
    /// same variant. That connection is going away on its own and takes its
    /// subscriptions with it through the runtime's unsubscribe, so counting
    /// the delivery as refused costs the message one refusal against a
    /// connection that no longer exists.
    fn opening(error: Error) -> Undelivered {
        match error {
            Error::LimitExceeded => Undelivered::Refused,
            other => Undelivered::Failed(other),
        }
    }

    /// What the consumer's connection is closed with, for its own logs.
    ///
    /// Only for the failures that close it.
    fn close_reason(&self) -> &'static str {
        match self {
            Undelivered::Stalled => "consumer stopped reading deliveries",
            Undelivered::Failed(_) | Undelivered::Refused => "a delivery to this consumer failed",
        }
    }
}

/// Writes one delivery and keeps the report it ordered.
///
/// [`weida::Consumer::deliver`] is the whole-payload convenience and it drops
/// the cursors; this is the same three calls with the handle kept and a
/// deadline on each wait. A write that does not finish is abandoned by
/// dropping the transfer, which resets the stream so the consumer discards
/// the partial payload instead of waiting for a FIN.
async fn send(
    consumer: &Consumer,
    meta: TransferMeta,
    body: &[u8],
    idle: Duration,
) -> Result<Option<Cursors>, Undelivered> {
    let mut pace = Pace::new(idle);
    let mut transfer = pace
        .step(consumer.open(meta))
        .await
        .map_err(|Stalled| Undelivered::Stalled)?
        .map_err(Undelivered::opening)?;
    let cursors = transfer.cursors();
    for slice in body.chunks(WRITE_SLICE) {
        pace.step(transfer.write_all(slice))
            .await
            .map_err(|Stalled| Undelivered::Stalled)?
            .map_err(Undelivered::Failed)?;
        pace.progressed(slice.len());
    }
    transfer.finish().map_err(Undelivered::Failed)?;
    Ok(cursors)
}

/// Watches one delivery's report and tells the queue's loop how it ended.
///
/// One task per outstanding delivery, bounded by the unsettled limits of the
/// subscription and of the connection — the same shape a fan-out's writer and
/// a survey's collector have. It holds no lock and touches no queue state: it
/// reads cursors and sends one [`Event`], which is what keeps the queue's own
/// state in one loop.
///
/// The wait is unbounded on purpose and bounded in fact: `changed` resolves
/// with `None` when the reporter finishes, when the stream is reset **and**
/// when the connection goes away, so a consumer that vanishes ends this task
/// without a timer ([0029](https://github.com/tuco86/weida/blob/main/docs/decisions/0029-a-report-is-relayed-a-certificate-is-not.md)
/// §4.6). A consumer that stays and never reports costs its own unsettled
/// slots and the queue's budget for the message, and nothing else — and only
/// for as long as the queue holds the delivery: when `cancelled` resolves the
/// entry is gone, nothing is owed an outcome, and the task ends and releases
/// the report entry it holds.
async fn watch(
    delivery: DeliveryId,
    mut cursors: Cursors,
    mut cancelled: oneshot::Receiver<()>,
    events: mpsc::Sender<Event>,
) {
    let outcome = loop {
        tokio::select! {
            _ = &mut cancelled => return,
            changed = cursors.changed() => match changed {
                Some(set) if set.offset(SETTLEMENT).is_some() => break Event::Settled(delivery),
                // A level the delivery did not order, or an offset that moved
                // on a level that is not this one: not a settlement, keep
                // reading.
                Some(_) => continue,
                None => break Event::Lost(delivery),
            },
        }
    };
    // A closed channel means the queue stopped serving, which is the one case
    // where nothing is owed to anybody.
    if events.send(outcome).await.is_err() {
        tracing::debug!("an outcome arrived after its queue stopped");
    }
}
