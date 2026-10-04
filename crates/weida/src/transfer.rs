//! Transfer handles.
//!
//! These own their `quinn` streams outright: payload bytes go straight to the
//! socket without passing through the connection actor, so writing and reading
//! a transfer costs no task hop and takes no lock (master doc §49). The actor
//! is involved only for the two control frames a destructor may still need to
//! emit — and `finish()` costs one more thing, paid once at the end of a
//! transfer rather than per write: an unawaited `Delivery` hands its receipt
//! to the connection's parked set on drop, which is one uncontended lock and
//! a push, so that a drain can wait for it
//! (`docs/decisions/0009-drain.md` §4.2).
//!
//! Nothing here materializes a payload. `AsyncRead`/`AsyncWrite` are the
//! primitive API; `collect(max_bytes)` is an opt-in convenience with an
//! explicit cap (master doc §8, §81 rule 3).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::transport::{RecvHalf, SendHalf};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use weida_core::{Error, ErrorCode, PeerIdentity, TraceContext};
use weida_protocol::header::limits::MAX_REPORT_LEVELS;
use weida_protocol::header::{Acknowledgement, CursorLevel, ReportMode};
use weida_protocol::{DataHeader, ErrorHeader, FrameKind, codes};

use crate::conn::{ConnHandle, Ctl, read_frame, write_error_frame};
use crate::cursor::{Cursors, Reporter, order_report};
use crate::drain::Receipt;
use crate::identity::PeerChain;
use crate::ordering::Gap;

/// Per-transfer metadata supplied by the application.
#[derive(Clone, Debug, Default)]
pub struct TransferMeta {
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Trace context to propagate. `None` generates a fresh root context.
    pub trace: Option<TraceContext>,
    /// Topic this transfer is labelled with (DATA key `5`).
    ///
    /// Opaque bytes, selected by the filter grammar of `docs/PROTOCOL.md`
    /// §6.4. A publisher's fan-out sets it per copy and does not read this
    /// field; what it is here for is the **other** sender that needs a label:
    /// a producer sending to an L2 queue, whose consumers each filter on it
    /// (0018 §4.5). A topic is never a pattern — `*` in one is an ordinary
    /// byte.
    pub topic: Option<String>,
    /// The completion level this side **achieved** for the message it is
    /// answering (DATA key `8`).
    ///
    /// Only an L2 hop sets it, and only on a reply: it is the publisher
    /// confirm of
    /// [decisions/0018](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
    /// §4.6. A v0 application leaves it `None`, which claims nothing beyond
    /// the transport receipt.
    pub achieved: Option<Acknowledgement>,
    /// Levels to ask the receiver to report on, as cursors (DATA keys `9`
    /// and `10`).
    ///
    /// An **order, not a guarantee**: a receiver that cannot reach a level
    /// simply does not report it, and the transfer does not fail for it. A
    /// level a peer must reach is the negotiated `acknowledgement` dimension
    /// of HELLO instead
    /// ([0006](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0006-guarantee-sets.md)
    /// §4.4).
    ///
    /// Ordering levels changes **nothing** about the transfer's topology: the
    /// report rides a unidirectional stream of its own, so a Push transfer
    /// that orders cursors is still one unidirectional stream
    /// ([0024](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0024-three-families-one-back-channel.md)
    /// §4.4a). The cursors arrive on the handle
    /// [`OutgoingTransfer::cursors`] hands out.
    pub report: Vec<CursorLevel>,
    /// How often the receiver should report (DATA key `11`).
    ///
    /// [`ReportMode::Progress`] is the default and costs no header byte. The
    /// *granularity* of progress is the reporter's own number and is never
    /// negotiated.
    pub report_mode: ReportMode,
}

impl TransferMeta {
    /// Sets the content type label.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> TransferMeta {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets the advisory content length.
    pub fn with_content_len(mut self, len: u64) -> TransferMeta {
        self.content_len = Some(len);
        self
    }

    /// Propagates an existing trace context.
    pub fn with_trace(mut self, trace: TraceContext) -> TransferMeta {
        self.trace = Some(trace);
        self
    }

    /// Labels the transfer with a topic.
    pub fn with_topic(mut self, topic: impl Into<String>) -> TransferMeta {
        self.topic = Some(topic.into());
        self
    }

    /// States the completion level this side achieved.
    ///
    /// A claim about this hop only, and a claim a peer will act on: `weida`
    /// itself never sets it, so anything here comes from an L2 layer that
    /// took responsibility for the message.
    pub fn with_achieved(mut self, achieved: Acknowledgement) -> TransferMeta {
        self.achieved = Some(achieved);
        self
    }

    /// Orders a report for `levels`.
    ///
    /// The levels are sorted and deduplicated, so the wire form is canonical
    /// whatever order a caller names them in. More than
    /// [`weida_protocol::header::limits::MAX_REPORT_LEVELS`] distinct levels
    /// fails the send with [`Error::LimitExceeded`] and puts nothing on the
    /// wire.
    pub fn with_report(mut self, levels: impl IntoIterator<Item = CursorLevel>) -> TransferMeta {
        let mut levels: Vec<CursorLevel> = levels.into_iter().collect();
        levels.sort_unstable();
        levels.dedup();
        self.report = levels;
        self
    }

    /// Asks for one record per level at the end rather than progress.
    pub fn with_report_mode(mut self, mode: ReportMode) -> TransferMeta {
        self.report_mode = mode;
        self
    }
}

/// Metadata of an inbound transfer.
#[derive(Clone, Debug)]
pub struct IncomingMeta {
    /// Endpoint path. Present on an initiating stream, absent on a reply.
    pub endpoint: Option<String>,
    /// Advisory payload length.
    pub content_len: Option<u64>,
    /// Opaque content type label.
    pub content_type: Option<String>,
    /// Parsed trace context.
    ///
    /// A malformed `traceparent` yields `None`: W3C Trace Context requires an
    /// unparsable value to be ignored, and it is within its wire cap, so it is
    /// not a protocol violation.
    pub trace: Option<TraceContext>,
    /// Opaque `tracestate`, forwarded unmodified.
    pub tracestate: Option<String>,
    /// Pub/Sub topic, when the transfer came from a publisher fan-out.
    pub topic: Option<String>,
    /// The public-key fingerprint the sending peer proved in the TLS
    /// handshake; `None` when it dialled anonymously.
    ///
    /// This is the identity to authorize on. It comes from the handshake, not
    /// from anything the peer wrote into a header, so it cannot be claimed —
    /// only proved (master doc §47).
    pub peer: Option<PeerIdentity>,
    /// The certificate chain the peer presented behind `peer`, leaf first
    /// ([decisions/0035](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.2). `None` when `peer` is not a key, and when the chain exceeded
    /// the bound weida keeps.
    pub peer_chain: Option<PeerChain>,
    /// The sender's per-producer sequence number, when it numbered this
    /// transfer (DATA key `6`).
    pub sequence: Option<u64>,
    /// What went missing before this transfer, when the connection
    /// negotiated `PerProducer(detect)` and a number was skipped.
    ///
    /// Detect mode reports the gap and delivers the message that arrived;
    /// nothing is held back and nothing is refetched
    /// ([decision 0001](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0001-sequence-field.md)
    /// §7.5). `None` means either that ordering is off or that nothing is
    /// missing.
    pub gap: Option<Gap>,
    /// The completion level the sender achieved for the message this transfer
    /// answers (DATA key `8`), when it claimed one.
    ///
    /// On the reply half of an exchange with an L2 broker this is the
    /// publisher confirm: `Some(Acknowledgement::Accepted)` means the broker
    /// has taken responsibility for the message in memory. `None` is the v0
    /// case and claims nothing beyond the transport receipt.
    pub achieved: Option<Acknowledgement>,
    /// Levels the sender **ordered** a report for (DATA key `10`).
    ///
    /// Empty is the ordinary case. Where it is not, [`IncomingTransfer::reporter`]
    /// hands out the [`Reporter`] bound to this transfer; a level this side
    /// cannot honour is simply not reported, and the transfer does not fail
    /// for it.
    pub report: Vec<CursorLevel>,
    /// How often the sender asked to be told (DATA key `11`).
    pub report_mode: ReportMode,
    /// The id the sender allocated for the report (DATA key `9`).
    ///
    /// Present exactly when [`IncomingMeta::report`] is non-empty; it names
    /// the CURSOR stream a reporter opens, and it is the sender's number, not
    /// ours.
    pub report_id: Option<u64>,
    /// The RADIO segment number (DATA key `13`), when a radio sent this
    /// transfer as a stream segment
    /// ([decisions/0034](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0034-late-is-lost.md)
    /// §4.6).
    pub segment: Option<u64>,
}

impl IncomingMeta {
    pub(crate) fn from_header(
        header: &DataHeader,
        peer: Option<PeerIdentity>,
        peer_chain: Option<PeerChain>,
    ) -> IncomingMeta {
        IncomingMeta {
            endpoint: header.endpoint.clone(),
            content_len: header.content_len,
            content_type: header.content_type.clone(),
            trace: header
                .traceparent
                .as_deref()
                .and_then(|v| TraceContext::parse_traceparent(v).ok()),
            tracestate: header.tracestate.clone(),
            topic: header.topic.clone(),
            peer,
            peer_chain,
            sequence: header.sequence,
            gap: None,
            achieved: header.achieved,
            report: header.report.clone(),
            report_mode: header.report_mode,
            report_id: header.report_id,
            segment: header.segment,
        }
    }

    /// Attaches what the connection's gap detector observed.
    pub(crate) fn with_gap(mut self, gap: Option<Gap>) -> IncomingMeta {
        self.gap = gap;
        self
    }
}

/// Generates a fresh root trace context.
///
/// **A caller's call, never the library's.** Nothing in weida mints a context
/// on an application's behalf: a `traceparent` is propagated exactly where one
/// was supplied ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md)),
/// because a minted root is not a safe default but a fabricated fact — a hop
/// that received a context and forgot to pass it on would emit a *new* trace
/// rather than nothing, and a collector would see two unrelated traces instead
/// of one broken chain.
///
/// So this is how a caller that wants to *start* a trace says so:
///
/// ```no_run
/// # use weida::{TransferMeta, new_trace};
/// let meta = TransferMeta::default().with_trace(new_trace());
/// ```
pub fn new_trace() -> TraceContext {
    use rand::Rng;
    let mut rng = rand::rng();
    loop {
        let trace_id: [u8; 16] = rng.random();
        let span_id: [u8; 8] = rng.random();
        // All-zero identifiers are forbidden; retrying is free and keeps the
        // invariant in one place.
        if let Some(ctx) = TraceContext::new(trace_id, span_id, 0x01) {
            return ctx;
        }
    }
}

/// Builds the DATA header for an outgoing transfer.
///
/// `endpoint` is `Some` on an initiating stream and `None` on the reply half of
/// an exchange, which is the only distinction the header still makes: the
/// stream itself carries the role and the correlation.
///
/// `report_id` is the id [`outgoing_header`] allocated, and it must be `Some`
/// exactly when `meta.report` is non-empty — the decoder refuses either half
/// without the other (`docs/PROTOCOL.md` §6.2). The level cap is enforced
/// here rather than at each pattern, so every send path inherits it.
pub(crate) fn data_header(
    endpoint: Option<&str>,
    meta: &TransferMeta,
    tracestate: Option<String>,
    report_id: Option<u64>,
) -> Result<(DataHeader, Option<TraceContext>), Error> {
    if meta.report.len() > MAX_REPORT_LEVELS {
        return Err(Error::LimitExceeded);
    }
    // Key `3` is written exactly when the caller supplied a context. Minting
    // one here cost 58 bytes on every frame of every pattern — the largest
    // single item of a 135-byte frame for a 64-byte push — and fabricated a
    // root nobody asked for
    // ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md)).
    let trace = meta.trace;
    let header = DataHeader {
        endpoint: endpoint.map(str::to_owned),
        content_len: meta.content_len,
        content_type: meta.content_type.clone(),
        traceparent: trace.map(|t| t.to_traceparent()),
        tracestate,
        topic: meta.topic.clone(),
        // Keys 6 and 7 are specified ahead of code: the codec carries them,
        // the v0 runtime writes neither (`docs/PROTOCOL.md` §6.2).
        sequence: None,
        producer: None,
        achieved: meta.achieved,
        report_id,
        report: meta.report.clone(),
        report_mode: meta.report_mode,
        // Key 13 is written by a radio, or by a dialling peer's
        // `Peer::segment` toward a bound path; each sets it on the header it
        // builds itself.
        segment: None,
    };
    Ok((header, trace))
}

/// Builds an outgoing transfer's header and, where it orders a report,
/// allocates the id and the channel that report's cursors will arrive on.
///
/// The allocation happens **before** the header is written, so the table entry
/// exists by the time a peer can answer: a reporter that is faster than the
/// sender's next line of code still finds an id to report on.
pub(crate) fn outgoing_header(
    conn: &ConnHandle,
    endpoint: Option<&str>,
    meta: &TransferMeta,
    tracestate: Option<String>,
) -> Result<(DataHeader, Option<TraceContext>, Option<Cursors>), Error> {
    if meta.report.is_empty() {
        let (header, trace) = data_header(endpoint, meta, tracestate, None)?;
        return Ok((header, trace, None));
    }
    // The cap is checked before an id is spent, so a refused send leaves the
    // table exactly as it was.
    if meta.report.len() > MAX_REPORT_LEVELS {
        return Err(Error::LimitExceeded);
    }
    let (report_id, cursors) = order_report(conn);
    let (header, trace) = data_header(endpoint, meta, tracestate, Some(report_id))?;
    Ok((header, trace, Some(cursors)))
}

/// An outgoing transfer: one QUIC send stream, owned outright.
///
/// Also implements [`AsyncWrite`], so a transfer can be a `tokio::io::copy`
/// destination. [`OutgoingTransfer::finish`] marks the FIN and hands back the
/// [`Delivery`] receipt.
pub struct OutgoingTransfer {
    /// `None` only after `finish` moved it into the expiry task.
    stream: Option<SendHalf>,
    trace: Option<TraceContext>,
    settled: bool,
    /// The deadline of [`OutgoingTransfer::expire_at`], if one was set.
    expiry: Option<Pin<Box<tokio::time::Sleep>>>,
    /// The connection this stream belongs to: where an unawaited receipt is
    /// parked so a drain can wait on it
    /// (`docs/decisions/0009-drain.md` §4.2).
    conn: ConnHandle,
    /// The report this transfer ordered, until the caller takes it.
    ///
    /// Kept here so that a caller which never asks releases the report's
    /// table entry by dropping the transfer, and a caller which does asks
    /// gets a handle that **outlives** the transfer: the terminal cursor
    /// arrives after the payload's FIN.
    cursors: Option<Cursors>,
}

impl OutgoingTransfer {
    pub(crate) fn new(
        stream: SendHalf,
        trace: Option<TraceContext>,
        conn: ConnHandle,
        cursors: Option<Cursors>,
    ) -> OutgoingTransfer {
        OutgoingTransfer {
            stream: Some(stream),
            trace,
            settled: false,
            expiry: None,
            conn,
            cursors,
        }
    }

    /// The trace context propagated with this transfer, if the caller supplied
    /// one.
    ///
    /// `None` is the normal case: weida propagates a context and never mints
    /// one ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md)).
    /// A caller that wants a trace passes it — `TransferMeta::with_trace`,
    /// from [`crate::new_trace`] or from an inbound
    /// [`IncomingMeta::trace`] — and reads it back here.
    pub fn trace(&self) -> Option<TraceContext> {
        self.trace
    }

    /// The cursors this transfer ordered, once.
    ///
    /// `None` when nothing was ordered, and `None` on every call after the
    /// first: there is one report, so there is one reader. The handle is
    /// independent of the transfer and stays usable after
    /// [`OutgoingTransfer::finish`], which is the point — a verdict such as
    /// `Accepted` arrives *after* the FIN.
    pub fn cursors(&mut self) -> Option<Cursors> {
        self.cursors.take()
    }

    fn stream(&mut self) -> &mut SendHalf {
        self.stream
            .as_mut()
            .expect("the stream is taken only by finish, which consumes the transfer")
    }

    /// Orders this transfer against the other streams of its connection, on
    /// `quinn`'s scale: higher is sent first, and the default is `0`.
    ///
    /// A connection is one dialled path, so a priority orders transfers on
    /// one path and never across paths. On a local transport, where each
    /// stream is its own OS connection, it is a no-op
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.3).
    pub fn set_priority(&mut self, priority: i32) {
        self.stream().set_priority(priority);
    }

    /// Resets this transfer with `CANCELED` if its bytes are not all
    /// acknowledged by `deadline`
    /// ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.3).
    ///
    /// A write still running at the deadline fails with [`Error::Expired`].
    /// On QUIC the deadline also holds after [`OutgoingTransfer::finish`]:
    /// the receipt then resolves to [`Error::Expired`] if the tail was still
    /// unacknowledged, because a reset is accepted until every byte is. On a
    /// local transport it acts until `finish`. The reader sees `Canceled`,
    /// never EOF. `Expired` is not a definite failure: the peer may have
    /// read every byte before the reset landed.
    pub fn expire_at(&mut self, deadline: std::time::Instant) {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        self.expiry = Some(Box::pin(self.conn.exec.sleep(left)));
    }

    /// Resets the stream for an expiry and counts it, once.
    fn expire(&mut self) {
        if !self.settled {
            self.settled = true;
            self.stream().reset(codes::CANCELED);
            self.conn
                .shared
                .expired
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn expired(&self) -> bool {
        self.expiry.as_ref().is_some_and(|e| e.is_elapsed())
    }

    /// Writes the whole buffer.
    ///
    /// A peer that refuses the transfer mid-write surfaces here as
    /// [`Error::Rejected`], [`Error::UnknownEndpoint`], [`Error::Unsupported`]
    /// or [`Error::Canceled`]; a deadline set with
    /// [`OutgoingTransfer::expire_at`] that passes first, as
    /// [`Error::Expired`].
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        if self.expired() {
            self.expire();
            return Err(Error::Expired);
        }
        let stream = self
            .stream
            .as_mut()
            .expect("the stream is taken only by finish, which consumes the transfer");
        let Some(expiry) = self.expiry.as_mut() else {
            return stream.write_all(buf).await;
        };
        let written = tokio::select! {
            biased;
            () = expiry.as_mut() => None,
            written = stream.write_all(buf) => Some(written),
        };
        match written {
            Some(written) => written,
            None => {
                self.expire();
                Err(Error::Expired)
            }
        }
    }

    /// Marks the end of the payload and returns the delivery receipt.
    ///
    /// Returns immediately: the FIN is queued and the peer's transport
    /// acknowledgement is awaited through [`Delivery::delivered`], or ignored
    /// entirely by dropping the receipt.
    ///
    /// Fails only if the stream is already closed — the peer reset it, or the
    /// connection went away — in which case no FIN was ever sent, or if the
    /// transfer's deadline has already passed ([`Error::Expired`]).
    pub fn finish(mut self) -> Result<Delivery, Error> {
        if self.expired() {
            self.expire();
            return Err(Error::Expired);
        }
        self.settled = true;
        let mut stream = self
            .stream
            .take()
            .expect("the stream is taken only here, and finish consumes the transfer");
        stream.finish()?;
        // `stopped()` yields a `'static` future, so the receipt outlives the
        // handle it came from.
        let receipt: Receipt = match self.expiry.take() {
            // On QUIC a reset is accepted until every byte is acknowledged,
            // so the deadline still holds: the stream moves into a task that
            // races the receipt against it.
            Some(expiry) if stream.is_quic() => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let shared = Arc::clone(&self.conn.shared);
                self.conn.exec.spawn(async move {
                    let stopped = stream.stopped();
                    let outcome = tokio::select! {
                        outcome = stopped => outcome,
                        () = expiry => {
                            stream.reset(codes::CANCELED);
                            shared.expired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Err(Error::Expired)
                        }
                    };
                    let _ = tx.send(outcome);
                });
                Box::pin(async move { rx.await.unwrap_or(Err(Error::Indeterminate)) })
            }
            _ => stream.stopped(),
        };
        Ok(Delivery {
            stopped: Some(receipt),
            conn: Arc::clone(&self.conn),
        })
    }

    /// Abandons the transfer, resetting the stream with `CANCELED`.
    pub fn cancel(mut self) {
        self.settled = true;
        self.stream().reset(codes::CANCELED);
    }
}

impl Drop for OutgoingTransfer {
    fn drop(&mut self) {
        if !self.settled
            && let Some(stream) = self.stream.as_mut()
        {
            // Dropping without `finish` is an abandoned transfer: reset the
            // stream so the peer discards the partial payload instead of
            // waiting for a FIN that will never come.
            stream.reset(codes::CANCELED);
        }
    }
}

impl AsyncWrite for OutgoingTransfer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // The deadline is polled first, so a writer blocked on flow control
        // is woken by it and fails rather than waiting past it.
        if let Some(expiry) = self.expiry.as_mut()
            && expiry.as_mut().poll(cx).is_ready()
        {
            self.expire();
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                Error::Expired,
            )));
        }
        AsyncWrite::poll_write(Pin::new(self.stream()), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(self.stream()), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(self.stream()), cx)
    }
}

/// Beside the Tokio traits, because a caller's executor need not be Tokio:
/// the two families differ only in shape, not in behaviour.
impl futures_io::AsyncWrite for OutgoingTransfer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        <Self as AsyncWrite>::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        <Self as AsyncWrite>::poll_flush(self, cx)
    }

    /// `futures-io`'s close is Tokio's shutdown: both end the write half.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        <Self as AsyncWrite>::poll_shutdown(self, cx)
    }
}

impl std::fmt::Debug for OutgoingTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingTransfer")
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

/// The transport receipt for a finished transfer.
///
/// This is QUIC's own fin-acknowledgement, not an application acknowledgement:
/// [`Delivery::delivered`] resolving means *the peer's transport holds every
/// byte*, and says nothing about the peer's application having read, stored or
/// processed them. Guarantees of that shape belong to a broker hop and are
/// deliberately absent from the v0 core (`docs/GUARANTEES.md`).
///
/// Dropping a `Delivery` observes no outcome and waits for nothing, which is
/// the fire-and-forget path. It is not quite free: the receipt is handed to
/// the connection's parked set — one uncontended lock and a push — so that
/// [`crate::Runtime::drain`] has something to wait on
/// (`docs/decisions/0009-drain.md` §4.2). A receipt the caller *does* await
/// is never parked; whoever holds it is doing the waiting.
pub struct Delivery {
    stopped: Option<Receipt>,
    conn: ConnHandle,
}

impl Delivery {
    /// Waits for the peer's transport to acknowledge the whole payload.
    ///
    /// `Ok(())` means every byte and the FIN were acknowledged. A refusal
    /// (`STOP_SENDING`) surfaces as the matching error; a connection lost after
    /// the FIN yields [`Error::Indeterminate`], because the payload may or may
    /// not have arrived (`docs/FAILURE_MODEL.md`).
    pub async fn delivered(mut self) -> Result<(), Error> {
        let stopped = self.stopped.take().expect("receipt taken only here");
        match stopped.await {
            Ok(None) => Ok(()),
            Ok(Some(code)) => Err(codes::stop_reason(code).into()),
            Err(e) => Err(e),
        }
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if let Some(receipt) = self.stopped.take()
            && self.conn.parked.park(receipt)
        {
            // The connection's parked set was full of receipts that had not
            // settled, so the oldest was dropped unobserved: the next drain
            // reports it as outstanding rather than assuming it landed.
            self.conn.shared.drain.evict();
        }
    }
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery").finish_non_exhaustive()
    }
}

/// An inbound transfer: one receive stream plus the metadata that described
/// it.
///
/// Implements [`AsyncRead`]. Reaching EOF is just EOF: the v0 core emits no
/// acknowledgement, because a brokerless one would only restate what QUIC's
/// own transport receipt already says.
pub struct IncomingTransfer {
    stream: RecvHalf,
    meta: Arc<IncomingMeta>,
    /// The connection this transfer arrived on: where a report goes back.
    conn: ConnHandle,
    /// Set once the payload ended, was reset, or was refused: `Drop` then has
    /// nothing left to stop.
    done: bool,
}

impl IncomingTransfer {
    pub(crate) fn new(
        stream: RecvHalf,
        meta: Arc<IncomingMeta>,
        conn: ConnHandle,
    ) -> IncomingTransfer {
        IncomingTransfer {
            stream,
            meta,
            conn,
            done: false,
        }
    }

    /// Metadata from the DATA header.
    pub fn meta(&self) -> &IncomingMeta {
        &self.meta
    }

    /// A reporter for the levels the sender ordered, if it ordered any.
    ///
    /// The report rides a unidirectional stream of its own, so this adds
    /// nothing to the transfer's topology and the transfer never waits on it.
    /// A level this side cannot honour is simply not reported; the sender
    /// observes the absence rather than a failure.
    ///
    /// On an exchange the request's reporter comes from
    /// [`IncomingRequest::body`], and the **reply** direction is reported by
    /// the responder ordering levels in its own reply header — so there is one
    /// accessor rather than two.
    pub fn reporter(&self) -> Option<Reporter> {
        let report_id = self.meta.report_id?;
        if self.meta.report.is_empty() {
            return None;
        }
        Some(Reporter::new(
            ConnHandle::clone(&self.conn),
            report_id,
            self.meta.report.clone(),
            self.meta.report_mode,
        ))
    }

    /// Refuses the payload with `STOP_SENDING(code)` and forgets the transfer.
    pub(crate) fn refuse(mut self, code: u64) {
        self.done = true;
        self.stream.stop(code);
    }

    /// Reads the whole remaining payload, refusing to exceed `max_bytes`.
    ///
    /// Over the cap the rest of the payload is refused with
    /// `STOP_SENDING(REJECTED)` and [`Error::LimitExceeded`] is returned: the
    /// bytes are never buffered first.
    ///
    /// This is a convenience for small payloads; streaming users should use
    /// [`AsyncRead`] instead. Use it on a borrowed body
    /// ([`IncomingRequest::body`]); [`IncomingTransfer::collect`] is the
    /// by-value form.
    pub async fn read_capped(&mut self, max_bytes: usize) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            match self.stream.read(&mut chunk).await {
                Ok(Some(0)) => continue,
                Ok(Some(n)) => {
                    if out.len() + n > max_bytes {
                        self.done = true;
                        self.stream.stop(codes::REJECTED);
                        return Err(Error::LimitExceeded);
                    }
                    out.extend_from_slice(&chunk[..n]);
                }
                Ok(None) => {
                    self.done = true;
                    return Ok(out);
                }
                Err(e) => {
                    self.done = true;
                    return Err(e);
                }
            }
        }
    }

    /// Consuming form of [`IncomingTransfer::read_capped`].
    pub async fn collect(mut self, max_bytes: usize) -> Result<Vec<u8>, Error> {
        self.read_capped(max_bytes).await
    }
}

impl Drop for IncomingTransfer {
    fn drop(&mut self) {
        if !self.done {
            // The application walked away mid-payload: refuse the rest rather
            // than draining bytes nobody wants.
            self.stream.stop(codes::REJECTED);
        }
    }
}

impl std::fmt::Debug for IncomingTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingTransfer")
            .field("meta", &self.meta)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for IncomingTransfer {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match AsyncRead::poll_read(Pin::new(&mut self.stream), cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() == before {
                    self.done = true;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                self.done = true;
                Poll::Ready(Err(e))
            }
            pending => pending,
        }
    }
}

/// The `futures-io` counterpart. It goes through the Tokio implementation
/// above rather than the stream directly, so the end-of-payload bookkeeping
/// happens exactly once and in one place.
impl futures_io::AsyncRead for IncomingTransfer {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);
        match <Self as AsyncRead>::poll_read(self, cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The reply half of an exchange, before the application claims it.
///
/// Dropping it without a reply is what tells the requester that none is
/// coming: the ERROR frame goes through the connection actor with a
/// non-blocking `notify`, because a destructor may run on a thread with no
/// reactor.
struct ReplyHalf {
    send: Option<SendHalf>,
    conn: ConnHandle,
}

impl Drop for ReplyHalf {
    fn drop(&mut self) {
        if let Some(send) = self.send.take() {
            self.conn.notify(Ctl::ReplyError {
                send,
                code: ErrorCode::NoReply,
            });
        }
    }
}

/// A request accepted by a [`crate::Replier`] or a raw [`crate::Acceptor`].
///
/// One bidirectional QUIC stream: the request payload arrives on the receive
/// half, the reply leaves on the send half. Dropping it without calling
/// [`IncomingRequest::reply`] tells the requester that no reply is coming,
/// instead of leaving it waiting.
pub struct IncomingRequest {
    body: Option<IncomingTransfer>,
    meta: Arc<IncomingMeta>,
    reply: ReplyHalf,
}

impl IncomingRequest {
    pub(crate) fn new(body: IncomingTransfer, send: SendHalf, conn: ConnHandle) -> IncomingRequest {
        IncomingRequest {
            meta: Arc::clone(&body.meta),
            body: Some(body),
            reply: ReplyHalf {
                send: Some(send),
                conn,
            },
        }
    }

    /// Metadata from the request's DATA header.
    pub fn meta(&self) -> &IncomingMeta {
        &self.meta
    }

    /// The request payload, borrowed.
    ///
    /// Panics after [`IncomingRequest::take_body`] has detached it.
    pub fn body(&mut self) -> &mut IncomingTransfer {
        self.body
            .as_mut()
            .expect("the request body was detached by take_body")
    }

    /// Detaches the request payload so it outlives the request handle.
    ///
    /// Needed exactly when the handler streams in both directions at once:
    /// [`IncomingRequest::reply`] consumes the request, and a body left inside
    /// it is refused with `REJECTED` at that point. Detaching first keeps both
    /// halves of the exchange live simultaneously, which is what makes
    /// simultaneous request and reply streaming possible (master doc §10).
    ///
    /// Panics if called twice.
    pub fn take_body(&mut self) -> IncomingTransfer {
        self.body
            .take()
            .expect("the request body is detached at most once")
    }

    /// Resolves when the requester stops wanting a reply.
    ///
    /// This is the reply half's `STOP_SENDING`, which a requester emits by
    /// dropping its [`ReplyStream`]. The future is independent of the stream
    /// handle, so it can sit in a `select!` beside the reply writes — but it
    /// must be taken *before* [`IncomingRequest::reply`] consumes the request.
    pub fn canceled(&self) -> impl Future<Output = ()> + Send + use<> {
        let stopped = self.reply.send.as_ref().map(|s| s.stopped());
        async move {
            match stopped {
                Some(fut) => {
                    let _ = fut.await;
                }
                None => std::future::pending().await,
            }
        }
    }

    /// Opens the reply half.
    ///
    /// Consuming: an exchange has exactly one reply, so a second one is not
    /// representable. A request body still attached here is dropped, which
    /// refuses whatever is *left* of it with `REJECTED`; a body already read
    /// to EOF costs nothing. Call [`IncomingRequest::take_body`] first if the
    /// handler still needs to read while it replies.
    ///
    /// The reply inherits the request's trace context unless `meta` overrides
    /// it.
    pub async fn reply(mut self, meta: TransferMeta) -> Result<OutgoingTransfer, Error> {
        // `IncomingTransfer::drop` stops the half only when the payload has
        // not already ended, so a completed request is left alone.
        drop(self.body.take());
        let mut send = self
            .reply
            .send
            .take()
            .expect("the send half is taken exactly once, by this method");

        let meta = match (meta.trace, self.meta.trace) {
            (None, Some(inherited)) => meta.with_trace(inherited),
            _ => meta,
        };
        // No endpoint: the stream is the correlation, so the reply half
        // addresses nothing. A reply may order its own report, and that is
        // how the **reply** direction is reported: the responder allocates
        // from its own id space and the requester reports on it.
        let (header, trace, cursors) =
            outgoing_header(&self.reply.conn, None, &meta, self.meta.tracestate.clone())?;
        write_data_preamble(&mut send, &header).await?;
        Ok(OutgoingTransfer::new(
            send,
            trace,
            Arc::clone(&self.reply.conn),
            cursors,
        ))
    }

    /// Refuses the exchange: a typed ERROR instead of a reply.
    ///
    /// This is the one place where an **application** decides an outcome the
    /// requester will see, and it exists because Req/Rep is the only pattern
    /// where a refusal has somewhere to go: the reply half
    /// (`docs/decisions/0005-refusal-race.md` §4.3). The requester's
    /// `request()` fails with the matching [`Error`] — `Rejected` for
    /// [`ErrorCode::Rejected`], `NoReply` for [`ErrorCode::NoReply`] — rather
    /// than waiting for a reply that is not coming.
    ///
    /// The request half is stopped at the same time, so a peer still writing a
    /// payload stops rather than filling a window nobody will read.
    ///
    /// Use [`ErrorCode::NoReply`] where the request was taken and no reply
    /// will exist — an adapter whose far side dropped it silently, for
    /// instance — and [`ErrorCode::Rejected`] where this side declined it.
    pub async fn refuse(self, code: ErrorCode) {
        self.refuse_coded(code, codes::REJECTED).await;
    }

    /// Refuses with an explicit `STOP_SENDING` code, for the runtime's own
    /// routing refusals, which have their own codes (`docs/PROTOCOL.md` §9.3).
    pub(crate) async fn refuse_coded(mut self, code: ErrorCode, stop: u64) {
        if let Some(body) = self.body.take() {
            body.refuse(stop);
        }
        let Some(mut send) = self.reply.send.take() else {
            return;
        };
        if let Err(e) = write_error_frame(&mut send, code).await {
            tracing::debug!(error = %e, "failed to refuse an exchange");
        }
    }
}

impl std::fmt::Debug for IncomingRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingRequest")
            .field("meta", self.meta())
            .finish_non_exhaustive()
    }
}

/// Builds the preamble and DATA header of an outgoing transfer in **one**
/// buffer, and reports where the frame starts in it.
///
/// The frame's length field precedes the header it measures, so the header has
/// to be encoded first. The previous shape encoded it into a fresh `Vec` —
/// paying the 8/16/32/64/128 growth chain for a ~70-byte header — then
/// allocated a second exact-capacity `Vec` and copied the whole thing into it
/// (B-250).
///
/// Instead the header is encoded **after** a reserved `MAX_PREAMBLE_LEN`, and
/// the preamble is right-aligned against it: one allocation whose capacity is
/// right the first time, no copy of the header, and one contiguous slice to
/// write. The bytes are identical to the two-buffer form, which
/// `a_data_frame_is_built_in_one_buffer_and_is_byte_identical` asserts —
/// getting the alignment wrong is the one regression this shape can cause.
fn data_frame(header: &DataHeader) -> (Vec<u8>, usize) {
    /// Room for a typical DATA header after the reserved preamble: an
    /// endpoint path, a `content_len`, and a caller's trace context if there
    /// is one. A header past this grows the `Vec` once, which is the case
    /// this constant exists to make rare rather than impossible.
    const HEADER_HINT: usize = 160;

    let pre = weida_protocol::MAX_PREAMBLE_LEN;
    let mut buf = Vec::with_capacity(pre + HEADER_HINT);
    buf.resize(pre, 0);
    header.encode_into(&mut buf);
    let header_len = buf.len() - pre;

    let (bytes, len) = weida_protocol::preamble_bytes(FrameKind::Data, header_len as u64);
    let start = pre - len;
    buf[start..pre].copy_from_slice(&bytes[..len]);
    (buf, start)
}

/// Writes the preamble and DATA header of an outgoing transfer.
pub(crate) async fn write_data_preamble(
    stream: &mut SendHalf,
    header: &DataHeader,
) -> Result<(), Error> {
    let (buf, start) = data_frame(header);
    stream.write_all(&buf[start..]).await
}

/// The reply half of an exchange the requester is waiting on.
///
/// Dropping it before [`ReplyStream::recv`] stops the half with `CANCELED`, so
/// a responder streaming a long reply learns that nobody is listening. That
/// stop replaces the CANCEL frame of earlier drafts: the stream carries the
/// correlation, so cancellation needs no identifier and no control frame.
pub struct ReplyStream {
    recv: Option<RecvHalf>,
    conn: ConnHandle,
}

impl ReplyStream {
    pub(crate) fn new(recv: RecvHalf, conn: ConnHandle) -> ReplyStream {
        ReplyStream {
            recv: Some(recv),
            conn,
        }
    }

    /// Waits for the reply header.
    ///
    /// A DATA header yields the reply payload; an ERROR header yields the
    /// corresponding [`Error`], which is how `UNKNOWN_ENDPOINT`, `UNSUPPORTED`
    /// and `NO_REPLY` reach the requester.
    ///
    /// A connection lost while waiting is [`Error::Indeterminate`], never
    /// `ConnectionLost`: the replier may already have acted on the request and
    /// produced an answer we never saw, so claiming a definite failure here
    /// would be a lie (master doc §22).
    pub async fn recv(mut self) -> Result<IncomingTransfer, Error> {
        let mut recv = self.recv.take().expect("the receiver is taken once");
        let (preamble, header) = read_frame(&mut recv, self.conn.limits.max_header_bytes)
            .await
            .map_err(indeterminate_on_loss)?;
        match preamble.kind {
            FrameKind::Data => {
                let header = DataHeader::decode(&header)?;
                let meta = Arc::new(IncomingMeta::from_header(
                    &header,
                    self.conn.peer.clone(),
                    self.conn.peer_chain.clone(),
                ));
                Ok(IncomingTransfer::new(
                    recv,
                    meta,
                    ConnHandle::clone(&self.conn),
                ))
            }
            FrameKind::Error => {
                let header = ErrorHeader::decode(&header)?;
                Err(header.error_code().map_or_else(
                    || Error::Transport(format!("peer reported error code {}", header.code)),
                    Error::from,
                ))
            }
            other => Err(Error::Protocol(format!(
                "{other} is not legal on the reply half of an exchange"
            ))),
        }
    }
}

/// Re-labels a lost connection while awaiting a reply.
fn indeterminate_on_loss(e: Error) -> Error {
    match e {
        Error::ConnectionLost(_) => Error::Indeterminate,
        other => other,
    }
}

impl Drop for ReplyStream {
    fn drop(&mut self) {
        if let Some(mut recv) = self.recv.take() {
            recv.stop(codes::CANCELED);
        }
    }
}

impl std::fmt::Debug for ReplyStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyStream")
            .field("awaiting", &self.recv.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_meta_builders_compose() {
        let trace = new_trace();
        let meta = TransferMeta::default()
            .with_content_type("text/plain")
            .with_content_len(7)
            .with_trace(trace);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(meta.content_len, Some(7));
        assert_eq!(meta.trace, Some(trace));
    }

    #[test]
    fn generated_trace_contexts_are_valid_and_distinct() {
        let a = new_trace();
        let b = new_trace();
        assert_ne!(a.trace_id, b.trace_id);
        assert!(a.is_sampled());
        assert_eq!(
            TraceContext::parse_traceparent(&a.to_traceparent()).unwrap(),
            a
        );
    }

    /// [0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md):
    /// key `3` is written exactly when the caller supplied a context, and the
    /// 58 bytes it costs are the reason.
    #[test]
    fn a_header_carries_no_trace_context_unless_one_was_supplied() {
        let meta = TransferMeta::default().with_content_len(3);
        let (header, trace) = data_header(Some("/transform"), &meta, None, None).unwrap();
        assert_eq!(header.endpoint.as_deref(), Some("/transform"));
        assert_eq!(header.content_len, Some(3));
        assert_eq!(trace, None, "nothing mints a context");
        assert_eq!(header.traceparent, None, "and nothing writes one");
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);

        let supplied = new_trace();
        let (header, trace) = data_header(
            Some("/transform"),
            &meta.clone().with_trace(supplied),
            None,
            None,
        )
        .unwrap();
        assert_eq!(trace, Some(supplied));
        assert_eq!(
            header.traceparent.as_deref(),
            Some(&*supplied.to_traceparent()),
            "a supplied context is propagated verbatim"
        );
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);
    }

    /// The number the decision turns on, asserted rather than quoted: the
    /// trace context was the largest single item of a 64-byte push's frame.
    #[test]
    fn a_trace_context_costs_fifty_eight_bytes_of_frame() {
        let meta = TransferMeta::default().with_content_len(64);
        let (bare, _) = data_header(Some("/t"), &meta, None, None).unwrap();
        let (traced, _) = data_header(
            Some("/t"),
            &meta.clone().with_trace(new_trace()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            traced.encode().len() - bare.encode().len(),
            58,
            "1 byte of key, 2 of the tstr prefix, 55 of the value"
        );
    }

    #[test]
    fn reply_headers_carry_no_endpoint_but_keep_tracestate() {
        let (header, _) = data_header(
            None,
            &TransferMeta::default(),
            Some("vendor=x".into()),
            None,
        )
        .unwrap();
        assert_eq!(header.endpoint, None);
        assert_eq!(header.tracestate.as_deref(), Some("vendor=x"));
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);
    }

    /// The one regression the one-buffer construction can cause: a preamble
    /// aligned by one byte too many or too few (B-250).
    ///
    /// Asserted against the shape it replaced — encode the header, then build
    /// `preamble ++ header` — over headers whose length crosses the varint's
    /// own boundary, because that is where the alignment changes width.
    #[test]
    fn a_data_frame_is_built_in_one_buffer_and_is_byte_identical() {
        use weida_protocol::{encode_frame, header::limits::MAX_ENDPOINT_BYTES};

        for path_len in [1usize, 60, 61, 62, 200, MAX_ENDPOINT_BYTES] {
            let path = format!("/{}", "a".repeat(path_len - 1));
            let meta = TransferMeta::default()
                .with_content_len(1 << 20)
                .with_trace(new_trace());
            let (header, _) = data_header(Some(&path), &meta, None, None).unwrap();

            let (buf, start) = data_frame(&header);
            let want = encode_frame(FrameKind::Data, &header.encode());
            assert_eq!(
                &buf[start..],
                want.as_slice(),
                "a {path_len}-byte path frames differently in one buffer"
            );
            // And the peer can read it back: the preamble's length field has
            // to agree with what follows it, which a mis-aligned write would
            // break without changing the byte count.
            let (preamble, used) =
                weida_protocol::parse_preamble(&buf[start..], 64 * 1024).expect("a preamble");
            assert_eq!(preamble.kind, FrameKind::Data);
            assert_eq!(
                DataHeader::decode(&buf[start + used..]).unwrap(),
                header,
                "the header the length field points at"
            );
        }
    }

    #[test]
    fn a_report_order_is_sorted_deduplicated_and_capped() {
        let accepted = CursorLevel::Known(Acknowledgement::Accepted);
        let stored = CursorLevel::Known(Acknowledgement::Stored);
        let app = CursorLevel::Application(17);
        // A caller's order is arbitrary; the wire form is canonical, so two
        // peers ordering the same levels send the same bytes.
        let meta = TransferMeta::default().with_report([app, stored, accepted, stored]);
        assert_eq!(meta.report, vec![accepted, stored, app]);
        let (header, _) = data_header(Some("/t"), &meta, None, Some(1)).unwrap();
        assert_eq!(header.report_id, Some(1));
        assert_eq!(DataHeader::decode(&header.encode()).unwrap(), header);

        // One past the cap fails the send and puts nothing on the wire. The
        // levels are distinct, so `with_report` keeps all of them.
        let over = TransferMeta::default().with_report(
            (0..=MAX_REPORT_LEVELS as u64)
                .map(|i| CursorLevel::Application(CursorLevel::APPLICATION_FLOOR + i)),
        );
        assert!(matches!(
            data_header(Some("/t"), &over, None, Some(1)),
            Err(Error::LimitExceeded)
        ));
    }

    #[test]
    fn incoming_meta_ignores_a_malformed_traceparent() {
        let mut header = DataHeader::addressed("/x");
        header.traceparent = Some("not-a-traceparent".into());
        let meta = IncomingMeta::from_header(&header, None, None);
        assert!(meta.trace.is_none());

        let good = new_trace();
        header.traceparent = Some(good.to_traceparent());
        assert_eq!(
            IncomingMeta::from_header(&header, None, None).trace,
            Some(good)
        );
    }
}
