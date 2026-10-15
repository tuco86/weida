//! Admission: a producer's message, read concurrently into a queue.
//!
//! Everything in this file waits on a producer — for the rest of its payload,
//! for room in the window its reply travels in, for the stream a cursor needs —
//! so none of it runs in the queue's event loop. The loop only *starts* an
//! admission: it hands over a permit, and a task of its own reads the payload
//! under the deadline of [`crate::peer::Pace`], puts the finished message into
//! the queue and answers. A producer that stops half way costs one permit, one
//! reservation and one parked task until the deadline, and costs every other
//! producer and every consumer nothing.
//!
//! Three things bound what that concurrency can hold:
//!
//! - **permits**: at most `max_inflight_admissions` admissions are in flight
//!   per queue, and the loop stops reading streams while they all are;
//! - **reservations**: a payload is charged to the queue's budget as it is
//!   buffered, in steps, so a thousand readers cannot each be told the whole
//!   remainder ([`crate::queue::Queue::reserve`]);
//! - **deadlines**: no wait on a producer outlives `io_timeout`.
//!
//! The order messages enter a queue is the order they finish arriving. QUIC
//! orders bytes within a stream and nothing between streams, and holding a
//! finished message back until an unfinished one in front of it is done would
//! hand a stalled producer exactly the power this design takes from it.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::OwnedSemaphorePermit;
use weida::{
    CursorLevel, ErrorCode, IncomingMeta, IncomingRequest, IncomingTransfer, Reporter, TransferMeta,
};

use crate::broker::{Achieved, Served};
use crate::peer::{Pace, Stalled};
use crate::queue::{QueuedMessage, Refusal, labels_cost};

/// The first step of a payload's reservation, in bytes.
///
/// What a producer that has sent nothing yet holds against the queue's budget,
/// beyond the message's fixed cost: small enough that a thousand stalled
/// producers cannot take a queue's room from the ones that are sending, large
/// enough that most messages need one allocation. A producer's `content_len`
/// can make the first step *smaller*, never larger — it is advisory, and a
/// producer that declares a gigabyte and sends nothing must not reserve one.
const FIRST_READ: usize = 1024;

/// Starts admitting an exchange, holding `permit` until it is over.
pub(crate) fn start_exchange(
    served: &Arc<Served>,
    request: IncomingRequest,
    permit: OwnedSemaphorePermit,
) {
    tokio::spawn(admit_exchange(Arc::clone(served), request, permit));
}

/// Starts admitting a one-way transfer, holding `permit` until it is over.
pub(crate) fn start_stream(
    served: &Arc<Served>,
    transfer: IncomingTransfer,
    permit: OwnedSemaphorePermit,
) {
    tokio::spawn(admit_stream(Arc::clone(served), transfer, permit));
}

/// Admits a producer's exchange and confirms it, or refuses it on the reply
/// half.
///
/// The confirm is the reply: DATA carrying the achieved level and a FIN, no
/// payload. An exchange already has a reply half, so a publisher confirm needs
/// no frame kind of its own — and the stream is the correlation, so the confirm
/// names nothing (0018 §4.6).
async fn admit_exchange(
    served: Arc<Served>,
    mut request: IncomingRequest,
    _permit: OwnedSemaphorePermit,
) {
    let io_timeout = served.io_timeout;
    let Some(reservation) = Reservation::open(&served, request.meta()) else {
        // Refused before a byte is read, which is what `Reject` backpressure
        // means; `refuse` stops the request half too, so a producer still
        // writing stops instead of filling a window nobody will read.
        refuse(request, io_timeout).await;
        return;
    };
    let body = request.take_body();
    // Taken before the payload is consumed: `reporter()` reads the order the
    // producer put in its DATA header, and the read consumes the transfer.
    let reporter = body.reporter();
    let read = match read_message(body, reservation, io_timeout).await {
        Ok(read) => read,
        Err(ReadFailed::TooLarge) => {
            refuse(request, io_timeout).await;
            return;
        }
        Err(ReadFailed::Broken) => return,
    };
    match enqueue(&served, read) {
        Ok(admitted_bytes) => {
            // Both answers, and they say different things. The reply half is
            // the application's channel and carries the confirm it always
            // carried (DATA key `8`); the cursor is for a producer that
            // ordered one, and a producer that ordered none pays nothing.
            report_accepted(&served, reporter, admitted_bytes);
            confirm(request, io_timeout).await;
        }
        // A refusal reports **nothing**: the producer learns from the
        // refusal, not from a missing cursor.
        Err(Refusal::Full) => refuse(request, io_timeout).await,
    }
}

/// Admits a one-way transfer, and reports a cursor if the producer ordered
/// one.
///
/// This is the shape [0024](https://github.com/tuco86/weida/blob/main/docs/decisions/0024-three-families-one-back-channel.md)
/// §4.4a exists for: a producer that orders `Accepted` gets a reliable verdict
/// **on one unidirectional payload stream**, with no exchange and no reply
/// half. A producer that orders nothing gets the transport receipt and nothing
/// more — the honest spelling of `acks=0` (0018 §4.6). A refusal has
/// nowhere to go on a unidirectional stream, so a queue with no room is a
/// `STOP_SENDING(REJECTED)` — dropping the unread transfer — and no cursor.
async fn admit_stream(
    served: Arc<Served>,
    transfer: IncomingTransfer,
    _permit: OwnedSemaphorePermit,
) {
    // Dropping an unread payload is the refusal: the handle stops the stream
    // with `REJECTED`, which is the only answer a unidirectional stream can
    // carry.
    let Some(reservation) = Reservation::open(&served, transfer.meta()) else {
        return;
    };
    let reporter = transfer.reporter();
    let Ok(read) = read_message(transfer, reservation, served.io_timeout).await else {
        return;
    };
    match enqueue(&served, read) {
        Ok(admitted_bytes) => report_accepted(&served, reporter, admitted_bytes),
        // Not a race: the reservation covered what was buffered. What is left
        // is a payload whose allocation ended up larger than what it
        // reserved, which cannot happen to a payload this module read. The
        // payload is already read, so there is no stream left to stop and
        // **nothing is sent**: the producer that ordered a cursor sees it
        // never arrive, which is the only answer a one-way stream has left
        // once its FIN is in.
        Err(Refusal::Full) => tracing::debug!(
            path = served.acceptor.path(),
            "dropping a one-way message that overran its reservation"
        ),
    }
}

/// Puts a message the queue has promised room for into the queue, and wakes
/// the loop that delivers.
///
/// Returns the payload bytes admitted, which is what `Accepted` is reported at.
fn enqueue(served: &Served, read: Read) -> Result<u64, Refusal> {
    let admitted_bytes = read.message.body.len() as u64;
    // The producer's trace, logged where it is still available: after the
    // exchange ends nothing can reconstruct which trace a queued message
    // belongs to, and a delivery continues it.
    let trace = read.message.trace;
    read.reservation.commit(read.message)?;
    if let Some(trace) = trace {
        tracing::debug!(
            path = served.acceptor.path(),
            bytes = admitted_bytes,
            trace_id = %trace.trace_id_hex(),
            "admitted a message into a queue"
        );
    }
    served.wake.notify_one();
    Ok(admitted_bytes)
}

/// Room a reader has been promised in a queue, returned unused if it is
/// dropped.
///
/// The accounting half of reading concurrently: what is held here counts
/// against the queue's budget exactly as a queued message does, so a payload
/// that stalls, fails or is refused gives its bytes back by going out of
/// scope, and no path out of an admission can forget to.
struct Reservation {
    served: Arc<Served>,
    bytes: usize,
}

impl Reservation {
    /// Reserves what a message costs before its payload: the fixed overhead
    /// and its labels. `None` is a queue with no room for one more message at
    /// all.
    fn open(served: &Arc<Served>, meta: &IncomingMeta) -> Option<Reservation> {
        let cost = labels_cost(meta.topic.as_deref(), meta.content_type.as_deref());
        let reserved = served
            .queue
            .lock()
            .expect("queue mutex poisoned")
            .reserve(cost);
        reserved.then(|| Reservation {
            served: Arc::clone(served),
            bytes: cost,
        })
    }

    /// Reserves up to `want` more payload bytes, and says how many it got.
    fn grow(&mut self, want: usize) -> usize {
        let granted = self
            .served
            .queue
            .lock()
            .expect("queue mutex poisoned")
            .reserve_up_to(want);
        self.bytes += granted;
        granted
    }

    /// Hands the reservation to the queue with the message it was for.
    fn commit(mut self, message: QueuedMessage) -> Result<(), Refusal> {
        let bytes = std::mem::take(&mut self.bytes);
        self.served
            .queue
            .lock()
            .expect("queue mutex poisoned")
            .push(bytes, message)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        // A poisoned queue is already a dead queue, and a destructor that
        // panics on top of it would abort the process for a counter.
        if let Ok(mut queue) = self.served.queue.lock() {
            queue.release(self.bytes);
        }
    }
}

/// A payload read into memory the queue has promised to hold.
struct Read {
    message: QueuedMessage,
    reservation: Reservation,
}

/// Why a payload did not become a queued message.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReadFailed {
    /// It exceeded the room the queue had; nothing was buffered past it.
    TooLarge,
    /// The producer stalled, or it or the connection failed mid-payload.
    Broken,
}

/// Reads a payload under the queue's remaining budget and the producer's
/// deadline.
///
/// The labels come from the transfer's own metadata, taken before the read
/// consumes it, so a one-way message costs no copy of the rest of its header.
async fn read_message(
    mut body: IncomingTransfer,
    mut reservation: Reservation,
    idle: Duration,
) -> Result<Read, ReadFailed> {
    let (content_len, topic, content_type, trace) = {
        let meta = body.meta();
        (
            meta.content_len,
            meta.topic.clone(),
            meta.content_type.clone(),
            meta.trace,
        )
    };
    let mut pace = Pace::new(idle);
    let payload = read_payload(
        &mut body,
        content_len,
        |want| reservation.grow(want),
        &mut pace,
    )
    .await?;
    Ok(Read {
        message: QueuedMessage {
            body: Arc::new(payload),
            topic,
            content_type,
            attempts: 0,
            trace,
        },
        reservation,
    })
}

/// Reads `source` to its end into a buffer that is only ever as large as what
/// the queue has agreed to hold.
///
/// `grow` is asked for the next step of room and answers with what it got,
/// possibly less, possibly none; none while the producer is still sending is
/// [`ReadFailed::TooLarge`], and nothing was buffered past what was granted.
/// That is the bound, and it is the queue's own remaining budget rather than
/// the producer's advisory `hint`, which only ever makes the first step
/// smaller.
///
/// Every wait is a step of `pace`, so a producer that stops, or that trickles
/// slower than [`crate::peer::MIN_PEER_RATE`], ends the read with
/// [`ReadFailed::Broken`].
///
/// The buffer grows by doubling and is trimmed to the payload at the end, so a
/// message is charged for its length and not for the slack its growth left.
/// When it is full the read asks for one more byte before it asks for more
/// room: a payload that ends exactly at its allocation costs no reservation it
/// does not use, and a payload that fits the budget to the byte is admitted.
pub(crate) async fn read_payload<R>(
    source: &mut R,
    hint: Option<u64>,
    mut grow: impl FnMut(usize) -> usize,
    pace: &mut Pace,
) -> Result<Vec<u8>, ReadFailed>
where
    R: AsyncRead + Unpin,
{
    let first = hint.map_or(FIRST_READ, |len| {
        usize::try_from(len).unwrap_or(usize::MAX).min(FIRST_READ)
    });
    let mut body = Vec::with_capacity(grow(first));
    loop {
        if body.len() < body.capacity() {
            if step(pace, source.read_buf(&mut body)).await? == 0 {
                break;
            }
            continue;
        }
        let mut probe = [0u8; 1];
        if step(pace, source.read(&mut probe)).await? == 0 {
            break;
        }
        let more = grow(body.capacity().max(FIRST_READ));
        if more == 0 {
            return Err(ReadFailed::TooLarge);
        }
        body.reserve_exact(more);
        body.push(probe[0]);
    }
    body.shrink_to_fit();
    Ok(body)
}

/// One read under the pace, recording what it moved.
async fn step(
    pace: &mut Pace,
    read: impl Future<Output = io::Result<usize>>,
) -> Result<usize, ReadFailed> {
    match pace.step(read).await {
        Ok(Ok(moved)) => {
            pace.progressed(moved);
            Ok(moved)
        }
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "a producer's payload failed mid-read");
            Err(ReadFailed::Broken)
        }
        Err(Stalled) => {
            tracing::debug!("a producer's payload stopped arriving; giving up on it");
            Err(ReadFailed::Broken)
        }
    }
}

/// Refuses an exchange on its reply half, for as long as the producer lets it.
async fn refuse(request: IncomingRequest, io_timeout: Duration) {
    let refused = tokio::time::timeout(io_timeout, request.refuse(ErrorCode::Rejected)).await;
    if refused.is_err() {
        tracing::debug!("a refusal was never read; giving up on it");
    }
}

/// Writes the confirm: the achieved level in the reply header, then FIN.
async fn confirm(request: IncomingRequest, io_timeout: Duration) {
    let meta = TransferMeta::default().with_achieved(Achieved::Accepted.level());
    let sent = tokio::time::timeout(io_timeout, async {
        request.reply(meta).await?.finish().map(drop)
    })
    .await;
    match sent {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::debug!(error = %e, "failed to send a confirm"),
        Err(_) => tracing::debug!("a confirm was never read; giving up on it"),
    }
}

/// Reports `Accepted` at the admitted body length, once, and FINs — in a task
/// of its own.
///
/// `Accepted` is a **verdict rather than a prefix** (0023 §4.2): it is true of
/// the whole message or of none of it, so there is exactly one record even in
/// `Progress` mode and there is no granularity to configure. A level this
/// broker cannot reach — `Stored`, with no store — is simply not reported, and
/// the producer sees it missing rather than failed.
///
/// A cursor is best effort by construction and nothing waits on it, which is
/// why it is the one answer allowed to be dropped: `Reporter::report` opens
/// the cursor stream lazily, and opening a stream blocks until the peer raises
/// `max_concurrent_uni_streams`. A producer that orders `Accepted` on every
/// message and never raises it would otherwise hold one parked task per
/// message it sends. So reports run in their own tasks, under the deadline,
/// and at most [`MAX_REPORTS_IN_FLIGHT`] at a time; past that the report is
/// skipped. They do not share the admission permits, which a stalled report
/// would otherwise pin.
fn report_accepted(served: &Served, reporter: Option<Reporter>, bytes: u64) {
    let Some(mut reporter) = reporter else {
        return;
    };
    let Ok(permit) = Arc::clone(&served.reports).try_acquire_owned() else {
        tracing::debug!(
            path = served.acceptor.path(),
            "too many cursor reports in flight; skipping one"
        );
        return;
    };
    let io_timeout = served.io_timeout;
    tokio::spawn(async move {
        let _permit = permit;
        let report = async move {
            let level = CursorLevel::Known(Achieved::Accepted.level());
            if let Err(e) = reporter.report(level, bytes).await {
                tracing::debug!(error = %e, "failed to report an accepted cursor");
            }
            if let Err(e) = reporter.finish().await {
                tracing::debug!(error = %e, "failed to finish a cursor report");
            }
        };
        if tokio::time::timeout(io_timeout, report).await.is_err() {
            tracing::debug!("gave up on a cursor: the peer opened no stream for it");
        }
    });
}

/// Cursor reports a queue runs at once.
///
/// A report is a best-effort extra, so the number only has to be far above
/// what an honest producer needs — a report takes a stream open and a few
/// bytes, so a few dozen at once is a busy queue — and far below what a
/// hostile one can make: without it, tasks parked in a stream open are bounded
/// only by admission rate times `io_timeout`.
pub(crate) const MAX_REPORTS_IN_FLIGHT: usize = 256;

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use tokio::io::{AsyncWriteExt, DuplexStream, duplex};

    use super::*;

    const IDLE: Duration = Duration::from_millis(250);

    /// A budget for [`read_payload`]: grants up to `left` bytes in total, and
    /// records every step it was asked for.
    struct Budget {
        left: usize,
        asked: Vec<usize>,
        granted: usize,
    }

    impl Budget {
        fn new(left: usize) -> Budget {
            Budget {
                left,
                asked: Vec::new(),
                granted: 0,
            }
        }

        fn grow(&mut self, want: usize) -> usize {
            self.asked.push(want);
            let granted = want.min(self.left);
            self.left -= granted;
            self.granted += granted;
            granted
        }
    }

    async fn read(
        source: &mut DuplexStream,
        hint: Option<u64>,
        budget: &mut Budget,
    ) -> Result<Vec<u8>, ReadFailed> {
        let mut pace = Pace::new(IDLE);
        read_payload(source, hint, |want| budget.grow(want), &mut pace).await
    }

    /// A producer that sends `bytes` and finishes.
    fn producer(bytes: Vec<u8>) -> DuplexStream {
        let (mut writer, reader) = duplex(64 * 1024);
        tokio::spawn(async move {
            let _ = writer.write_all(&bytes).await;
            // Dropping the writer is the FIN.
        });
        reader
    }

    #[tokio::test]
    async fn a_payload_that_fills_the_budget_exactly_is_admitted() {
        let mut budget = Budget::new(64);
        let body = read(&mut producer(vec![7; 64]), Some(64), &mut budget)
            .await
            .expect("exactly what the budget allows");
        assert_eq!(body, vec![7; 64]);
        assert_eq!(
            body.capacity(),
            64,
            "charged for the payload, not for slack"
        );
        assert_eq!(budget.granted, 64);
    }

    #[tokio::test]
    async fn a_payload_over_the_budget_is_refused_and_nothing_past_it_was_granted() {
        let mut budget = Budget::new(16);
        let outcome = read(&mut producer(vec![1; 4096]), None, &mut budget).await;
        assert_eq!(outcome, Err(ReadFailed::TooLarge));
        assert_eq!(
            budget.granted, 16,
            "the budget is the cap, whatever is sent"
        );
    }

    #[tokio::test]
    async fn a_payload_one_byte_over_the_budget_is_refused() {
        let mut budget = Budget::new(64);
        let outcome = read(&mut producer(vec![1; 65]), Some(64), &mut budget).await;
        assert_eq!(outcome, Err(ReadFailed::TooLarge));
    }

    #[tokio::test]
    async fn an_empty_payload_is_a_message_even_with_no_room_left() {
        let mut budget = Budget::new(0);
        let body = read(&mut producer(Vec::new()), Some(0), &mut budget)
            .await
            .expect("an empty message needs no payload room");
        assert!(body.is_empty());
        assert_eq!(body.capacity(), 0);
    }

    #[tokio::test]
    async fn a_short_payload_is_trimmed_to_its_length() {
        let mut budget = Budget::new(1 << 20);
        let body = read(&mut producer(b"work item".to_vec()), None, &mut budget)
            .await
            .expect("read");
        assert_eq!(body, b"work item");
        assert_eq!(
            body.capacity(),
            body.len(),
            "the first step was a guess; the charge is the length"
        );
    }

    #[tokio::test]
    async fn a_payload_larger_than_the_first_step_arrives_whole() {
        let payload: Vec<u8> = (0..300_000u32).map(|n| (n % 251) as u8).collect();
        let mut budget = Budget::new(1 << 20);
        let body = read(&mut producer(payload.clone()), None, &mut budget)
            .await
            .expect("read");
        assert_eq!(body, payload);
        assert_eq!(body.capacity(), body.len());
        assert!(
            budget.asked.windows(2).all(|pair| pair[1] >= pair[0]),
            "steps never shrink while the payload keeps coming: {:?}",
            budget.asked
        );
    }

    #[tokio::test]
    async fn a_producers_content_len_never_makes_the_first_step_bigger() {
        let mut budget = Budget::new(1 << 20);
        read(&mut producer(Vec::new()), Some(u64::MAX), &mut budget)
            .await
            .expect("an empty payload");
        assert_eq!(
            budget.asked.first(),
            Some(&FIRST_READ),
            "a declared gigabyte reserves one step, not a gigabyte"
        );

        let mut budget = Budget::new(1 << 20);
        read(&mut producer(vec![1; 9]), Some(9), &mut budget)
            .await
            .expect("nine bytes");
        assert_eq!(budget.asked.first(), Some(&9), "but it can make it smaller");
    }

    #[tokio::test]
    async fn a_producer_that_stops_is_given_up_on_at_the_deadline() {
        // The writer sends a prefix and keeps its end open: no FIN, no bytes.
        let (mut writer, mut reader) = duplex(1024);
        writer.write_all(b"half a mess").await.expect("prefix");
        let mut budget = Budget::new(1 << 20);
        let started = Instant::now();
        let outcome = read(&mut reader, Some(1 << 20), &mut budget).await;
        assert_eq!(outcome, Err(ReadFailed::Broken));
        assert!(started.elapsed() < Duration::from_secs(5));
        drop(writer);
    }

    #[tokio::test]
    async fn a_producer_that_trickles_is_given_up_on_although_it_never_goes_idle() {
        let (mut writer, mut reader) = duplex(1024);
        let trickle = tokio::spawn(async move {
            // One byte inside every idle window: the idle clock alone would
            // wait for this until the budget ran out.
            for _ in 0..100 {
                if writer.write_all(&[1]).await.is_err() {
                    return;
                }
                tokio::time::sleep(IDLE / 5).await;
            }
        });
        let mut budget = Budget::new(1 << 20);
        let started = Instant::now();
        let outcome = read(&mut reader, None, &mut budget).await;
        assert_eq!(outcome, Err(ReadFailed::Broken));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the trickle was never cut"
        );
        trickle.abort();
    }
}
