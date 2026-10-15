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

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use weida::{Acknowledgement, Consumer, CursorLevel, Cursors, Error, TransferMeta};

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
        let sent = send(&consumer, meta, &body, idle).await;
        drop(body);
        drop(consumer);
        match sent {
            Ok(Some(cursors)) => {
                // Sent before the watcher exists, so the queue's loop sees the
                // write end before it can see the delivery settle.
                if events.send(Event::Written(id)).await.is_ok() {
                    tokio::spawn(watch(id, cursors, events));
                }
            }
            // No cursors means the header carried no report, which cannot
            // happen for a delivery this module built — but a settlement that
            // will never come would strand the message and its slot, so the
            // honest answer is to give it back now rather than to hold it
            // forever.
            Ok(None) => {
                tracing::error!("a delivery ordered a report and got no cursors; requeuing");
                let _ = events.send(Event::Written(id)).await;
                let _ = events.send(Event::Lost(id)).await;
            }
            Err(e) => {
                tracing::debug!(error = %e, "a delivery failed; the consumer is dropped and the message goes back");
                let _ = events.send(Event::Failed(id)).await;
            }
        }
    });
}

/// Why a delivery did not reach its consumer.
#[derive(Debug)]
enum Undelivered {
    /// The consumer made no progress inside the deadline.
    Stalled,
    /// The transport refused: the connection is gone, or the consumer stopped
    /// the stream.
    Failed(Error),
}

impl std::fmt::Display for Undelivered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Undelivered::Stalled => f.write_str("the consumer stopped reading"),
            Undelivered::Failed(e) => e.fmt(f),
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
        .map_err(Undelivered::Failed)?;
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
/// slots and the queue's budget for the message, and nothing else.
async fn watch(delivery: DeliveryId, mut cursors: Cursors, events: mpsc::Sender<Event>) {
    let outcome = loop {
        match cursors.changed().await {
            Some(set) if set.offset(SETTLEMENT).is_some() => break Event::Settled(delivery),
            // A level the delivery did not order, or an offset that moved on a
            // level that is not this one: not a settlement, keep reading.
            Some(_) => continue,
            None => break Event::Lost(delivery),
        }
    };
    // A closed channel means the queue stopped serving, which is the one case
    // where nothing is owed to anybody.
    if events.send(outcome).await.is_err() {
        tracing::debug!("an outcome arrived after its queue stopped");
    }
}
