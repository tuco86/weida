//! Per-connection stream dispatch and the small control actor.
//!
//! There are no correlation tables left. Req/Rep rides one bidirectional QUIC
//! stream, so the stream *is* the correlation and there is nothing per
//! connection to index. What remains of the actor is a serialization point for
//! the two frames a destructor may need to emit without a reactor of its own.
//!
//! **Payload bytes never traverse the actor.** API handles own their `quinn`
//! streams directly, so writing and reading a transfer costs no task hop and
//! takes no lock (master doc §49).

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc, watch};
use weida_core::{Error, ErrorCode, Limits, LossCause, PeerIdentity};
use weida_protocol::header::{GuaranteeSet, MAX_CURSOR_RECORD_LEN};
use weida_protocol::{
    Agreed, CAPABILITY_DATAGRAM, CAPABILITY_PATH_REPORT, CreditHeader, CursorHeader, DataHeader,
    ErrorHeader, FlowHeader, FrameKind, HeaderError, Hello, MAX_PREAMBLE_LEN, Preamble, ReportHead,
    SubscriptionHeader, codes, decode_cursor_record, encode_frame, negotiate, parse_preamble,
};

use crate::dedup::DedupWindow;
use crate::identity::PeerChain;
use crate::listener::{Namespace, Route};
use crate::ordering::{GapDetector, Reassembler, Sequencer};
use crate::pubsub::SubRegistry;
use crate::runtime::{Exec, Shared};
use crate::stream::{Consumer, ConsumerId, CreditGrant, Incoming};
use crate::transfer::{IncomingMeta, IncomingRequest, IncomingTransfer};
use crate::transport::{Link, RecvHalf, SendHalf};

/// Depth of the control channel between API handles and the actor.
const CTL_QUEUE: usize = 1024;

/// Messages the connection actor accepts.
///
/// Both exist for the same reason: a `Drop` may run on a thread with no Tokio
/// reactor, and a destructor must never be the thing that needs one.
pub(crate) enum Ctl {
    /// Emit an UNSUBSCRIBE frame.
    SendUnsubscribe { path: Arc<str>, filter: String },
    /// Report a failure on the reply half of an exchange and close it.
    ReplyError { send: SendHalf, code: ErrorCode },
}

/// Shared, cheaply clonable handle to one connection.
pub(crate) struct ConnCtx {
    pub conn: Link,
    pub ctl: mpsc::Sender<Ctl>,
    pub limits: Limits,
    /// Endpoint routing table. Client connections get a fresh empty one rather
    /// than sharing the listener's: a `Sub` registers its path on the
    /// connection it dialled, so both directions need a namespace.
    pub namespace: Arc<Namespace>,
    /// Publisher-side subscriptions. `None` on a client connection: a peer
    /// that subscribes to us when we serve no publishers is useless, not
    /// hostile, so the frame is ignored rather than treated as a violation.
    pub subs: Option<Arc<SubRegistry>>,
    /// The identity the peer proved in the handshake, `None` for an anonymous
    /// client. Fixed for the life of the connection.
    pub peer: Option<PeerIdentity>,
    /// The chain behind `peer`, when it fits the bound weida keeps; fixed for
    /// the life of the connection.
    pub peer_chain: Option<PeerChain>,
    /// The runtime this connection's tasks and timers run on.
    pub exec: Exec,
    /// The guarantee set this side offers and requires. Negotiation refuses a
    /// peer that cannot match it, so it is also the effective set.
    pub guarantees: GuaranteeSet,
    /// Numbers outgoing transfers per scope; inert unless ordering is on.
    pub sequencer: Sequencer,
    /// Reports gaps in inbound sequences; inert unless detect mode is on.
    pub gaps: GapDetector,
    /// Holds out-of-order arrivals back; inert unless reassemble mode is on.
    pub reorder: Reassembler<Held>,
    /// Suppresses repeated identities; inert unless deduplication is on.
    pub dedup: DedupWindow,
    /// Receipts of finished transfers on this connection that nobody is
    /// waiting on, for [`crate::Runtime::drain`].
    pub parked: crate::drain::ConnDrain,
    /// Reports **this** side ordered: the ids handed out with a DATA header's
    /// key `9`, and the channel each one's cursors arrive on
    /// (`docs/PROTOCOL.md` §6.7). Bounded by the [`crate::Cursors`] handles
    /// the application holds, so it needs no remote-facing cap: a peer can
    /// only report on ids we allocated.
    pub reports: crate::cursor::ReportTable,
    /// Counters and flags shared with every other connection of this
    /// runtime: the duplicate count and the drain's admission flag.
    pub shared: Arc<Shared>,
    /// Datagram flows on this connection: the inbound table and the early
    /// ring, bounded by `max_flows`, `flow_queue_bytes` and
    /// `flow_early_bytes` ([`crate::flow`]).
    pub flows: crate::flow::FlowTable,
    /// When this connection was established: after the QUIC handshake, before
    /// HELLO. What [`crate::ConnectionStats::age`] counts from.
    pub established: std::time::Instant,
    /// Numbers [`crate::Peer::segment`] per `(path, topic)` on this dialling
    /// connection, shared by every `Peer` the pool gave it; at most
    /// `max_sequence_scopes` entries
    /// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
    /// §4.11).
    pub segments_out: crate::segment::SegmentNumbers,
    /// The newest segment delivered per `(path, topic)` on this connection,
    /// to dishes and every other receiver alike: what a stale segment is
    /// checked against; at most `max_sequence_scopes` entries (0037 §4.11).
    pub segments_in: crate::segment::Newest,
    /// How long finished segment copies on this connection take from FIN to
    /// acknowledgement: what a superseded copy's grace learns from
    /// ([`crate::segment::SettleTime`]).
    pub settle: crate::segment::SettleTime,
    /// The peer's latest path record and whether its REPORT stream was seen:
    /// one fixed-size slot ([decisions/0036](../../../docs/decisions/0036-connection-statistics.md)
    /// §4.5).
    pub remote: crate::report::RemoteSlot,
    /// One slot per inbound stream that may be reading its frame head at a
    /// time: [`Limits::header_slots`]. A stream takes one for the read of its
    /// preamble and one for the read and decode of its header, and holds none
    /// while it waits for negotiation. The raw header bytes are gone before
    /// the slot is given back, so the buffers a connection holds for frame
    /// heads are bounded by the slots: not by the peer's stream budget, and
    /// not by what its streams go on to do.
    pub header_slots: Semaphore,
    agreed: watch::Receiver<Option<Agreed>>,
}

pub(crate) type ConnHandle = Arc<ConnCtx>;

impl ConnCtx {
    /// Spawns the actor, both accept loops and the HELLO exchange for `conn`.
    pub(crate) fn spawn(
        conn: Link,
        limits: Limits,
        namespace: Arc<Namespace>,
        subs: Option<Arc<SubRegistry>>,
        exec: Exec,
        guarantees: GuaranteeSet,
        shared: Arc<Shared>,
    ) -> ConnHandle {
        let (ctl_tx, ctl_rx) = mpsc::channel(CTL_QUEUE);
        let (agreed_tx, agreed_rx) = watch::channel(None);
        let agreed_tx = Arc::new(agreed_tx);

        let streams_are_local = conn.streams_are_local();
        let ctx = Arc::new(ConnCtx {
            peer: conn.peer(),
            peer_chain: conn.peer_chain(),
            conn,
            ctl: ctl_tx,
            limits,
            namespace,
            subs,
            exec: exec.clone(),
            guarantees,
            sequencer: Sequencer::new(guarantees.ordering, limits.max_sequence_scopes),
            gaps: GapDetector::new(guarantees.ordering, limits.max_sequence_scopes),
            reorder: Reassembler::new(
                guarantees.ordering,
                limits.max_reorder_hold,
                limits.max_sequence_scopes,
            ),
            dedup: DedupWindow::new(
                guarantees.deduplication,
                guarantees.dedup_window_ms,
                limits.max_dedup_entries,
            ),
            parked: crate::drain::ConnDrain::new(&limits, streams_are_local),
            reports: crate::cursor::ReportTable::new(),
            shared,
            flows: crate::flow::FlowTable::new(&limits),
            agreed: agreed_rx,
            established: exec.now(),
            segments_out: crate::segment::SegmentNumbers::new(limits.max_sequence_scopes),
            segments_in: crate::segment::Newest::new(limits.max_sequence_scopes),
            settle: crate::segment::SettleTime::default(),
            remote: crate::report::RemoteSlot::default(),
            // tokio panics above `Semaphore::MAX_PERMITS`.
            header_slots: Semaphore::new(limits.header_slots().min(Semaphore::MAX_PERMITS)),
        });

        // Once per connection, never per message: a drain collects the
        // parked receipts from here.
        ctx.shared.drain.register(&ctx);

        exec.spawn(driver(Arc::downgrade(&ctx), ctl_rx, exec.clone()));
        exec.spawn(hello_deadline(Arc::clone(&ctx), Arc::clone(&agreed_tx)));
        exec.spawn(accept_uni_loop(Arc::clone(&ctx), agreed_tx));
        exec.spawn(accept_bi_loop(Arc::clone(&ctx)));
        exec.spawn(send_hello(Arc::clone(&ctx), limits, guarantees));
        // One reader per connection, and only where the profile enabled
        // flows: a connection that enables nothing runs no reader at all.
        if limits.datagram_receive_bytes > 0 && ctx.conn.is_quic() {
            exec.spawn(crate::flow::datagram_reader(Arc::clone(&ctx)));
        }
        ctx
    }

    /// Waits until the peer HELLO has been negotiated.
    ///
    /// QUIC streams are unordered relative to each other, so a DATA stream can
    /// be accepted before the peer's HELLO. Parking here is correct behaviour,
    /// not a protocol violation.
    ///
    /// A connection that dies while we wait fails this immediately with the
    /// reason it died. That matters most for a client whose handshake looked
    /// complete but whose identity the server then refused: the refusal is a
    /// connection close that arrives *after* `connect` resolved, and the
    /// HELLO that would end this wait is never coming.
    pub(crate) async fn negotiated(&self) -> Result<Agreed, Error> {
        let mut rx = self.agreed.clone();
        loop {
            if let Some(agreed) = *rx.borrow_and_update() {
                return Ok(agreed);
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        // The sender is gone because the connection is: the
                        // accept loops and the deadline hold it and end with
                        // it. Report why, not merely that.
                        return Err(self
                            .conn
                            .close_reason()
                            .unwrap_or(Error::ConnectionLost(LossCause::LocallyClosed)));
                    }
                }
                reason = self.conn.closed() => return Err(reason),
            }
        }
    }

    /// The negotiated parameters if negotiation has completed, without
    /// waiting.
    pub(crate) fn agreed_now(&self) -> Option<Agreed> {
        *self.agreed.borrow()
    }

    /// Sends a control message without waiting.
    ///
    /// Used from `Drop`, where blocking is not an option. A full queue means
    /// 1024 outstanding control messages on one connection; dropping the
    /// notification there is preferable to blocking a destructor.
    pub(crate) fn notify(&self, ctl: Ctl) {
        if self.ctl.try_send(ctl).is_err() {
            tracing::debug!("connection control queue full or closed; notification dropped");
        }
    }

    /// Opens a unidirectional stream.
    ///
    /// Waits where the transport has no stream slot free: on QUIC inside
    /// `quinn`, until the peer raises `max_concurrent_uni_streams`; on a local
    /// transport inside `max_local_streams`. Either way this is `Block`
    /// (`docs/GUARANTEES.md` §6) and the deadline is the caller's.
    pub(crate) async fn open_uni(&self) -> Result<SendHalf, Error> {
        self.free_local_slots();
        self.conn.open_uni().await
    }

    /// Opens a bidirectional stream.
    ///
    /// The peer learns of the stream only once the first bytes are written, and
    /// the DATA header is always written first, so an exchange never announces
    /// itself before it says what it is.
    pub(crate) async fn open_bi(&self) -> Result<(SendHalf, RecvHalf), Error> {
        self.free_local_slots();
        self.conn.open_bi().await
    }

    /// Frees the parked receipts that have already settled, when a local
    /// transport has no slot left for the open that follows.
    ///
    /// This exists because of what a stream slot **is** on a local transport:
    /// one OS connection or one channel pair, counted against
    /// `max_local_streams`
    /// ([0010](../../../docs/decisions/0010-local-transport.md) §4.2) — and a
    /// receipt parked for the drain holds its send half, so it holds a slot
    /// ([0009](../../../docs/decisions/0009-drain.md) §4.2). The parked set is
    /// bounded by the QUIC stream budgets, which have nothing to do with that
    /// local ceiling, so without reaping a long run of local transfers would
    /// exhaust the descriptors while the parked set sat below its own cap:
    /// `LimitExceeded` with nothing in flight and nothing wrong. Because the
    /// local open waits instead of failing, not reaping here would turn that
    /// into a wait nobody can end.
    ///
    /// Reaping only on pressure keeps the cost where it belongs: the check is
    /// one atomic load, and the walk happens only with every slot taken.
    /// The QUIC path never reaches it, because a QUIC stream slot is not a
    /// descriptor — a full budget there is the *peer's*, which reaping this
    /// side cannot fix.
    fn free_local_slots(&self) {
        if !self.conn.local_slots_exhausted() {
            return;
        }
        let freed = self.parked.reap();
        if freed > 0 {
            tracing::debug!(
                freed,
                "reaped settled receipts to make room for another stream"
            );
        }
    }
}

/// Refuses a header of `len` bytes that the peer's HELLO said it would not
/// read.
///
/// Its preamble parser closes the connection over one, taking every other
/// transfer on it along, so a sender checks the length it is about to write
/// ([`Agreed::check_header_len`]). Before negotiation nothing is known and
/// nothing is refused; every sender of a connection waits for it, or answers
/// something that did.
pub(crate) fn check_header_len(ctx: &ConnCtx, len: usize) -> Result<(), Error> {
    match ctx.agreed_now() {
        Some(agreed) => agreed
            .check_header_len(len)
            .map_err(|_| Error::LimitExceeded),
        None => Ok(()),
    }
}

/// Maps a `quinn` connection error onto the outcome vocabulary.
pub(crate) fn conn_error(e: quinn::ConnectionError) -> Error {
    match e {
        quinn::ConnectionError::ApplicationClosed(frame) => match frame.error_code.into_inner() {
            codes::NEGOTIATION_FAILED => {
                Error::Negotiation("peer closed the connection: negotiation failed".into())
            }
            codes::PROTOCOL_VIOLATION => {
                Error::Protocol("peer closed the connection: protocol violation".into())
            }
            codes::LIMIT_EXCEEDED => Error::LimitExceeded,
            codes::RESTARTING => Error::ConnectionLost(LossCause::PeerRestarting),
            // Any other application close code is the peer saying goodbye in
            // its own words: a shutdown, or a code this version does not map.
            _ => Error::ConnectionLost(LossCause::PeerClosed),
        },
        quinn::ConnectionError::LocallyClosed => Error::ConnectionLost(LossCause::LocallyClosed),
        // An idle timeout or a stateless reset is a lost connection: whatever
        // was in flight will never complete, which is the definite outcome
        // `ConnectionLost` promises (`docs/FAILURE_MODEL.md`). The cause is
        // kept because the next action differs — an idle timeout invites a
        // redial, a reset says the peer forgot us, and a deliberate close may
        // mean it does not want one yet.
        quinn::ConnectionError::TimedOut => Error::ConnectionLost(LossCause::IdleTimeout),
        quinn::ConnectionError::Reset => Error::ConnectionLost(LossCause::Reset),
        // Error codes 0x100..0x200 carry a TLS alert: the handshake itself
        // failed, whether we detected it or the peer told us so. That is a
        // TLS outcome, not a transport one.
        quinn::ConnectionError::TransportError(t) if is_tls_alert(t.code) => {
            Error::Tls(t.to_string())
        }
        quinn::ConnectionError::ConnectionClosed(c) if is_tls_alert(c.error_code) => {
            Error::Tls(format!("peer aborted the handshake: {c}"))
        }
        other => Error::Transport(other.to_string()),
    }
}

fn is_tls_alert(code: quinn::TransportErrorCode) -> bool {
    (0x100..0x200).contains(&u64::from(code))
}

/// Maps a stream write failure onto the outcome vocabulary.
pub(crate) fn write_error(e: quinn::WriteError) -> Error {
    match e {
        quinn::WriteError::Stopped(code) => codes::stop_reason(code.into_inner()).into(),
        quinn::WriteError::ConnectionLost(e) => conn_error(e),
        quinn::WriteError::ClosedStream => Error::Transport("stream already closed".into()),
        quinn::WriteError::ZeroRttRejected => {
            Error::Transport("0-RTT data rejected by the peer".into())
        }
    }
}

/// Maps a stream read failure onto the outcome vocabulary.
pub(crate) fn read_error(e: quinn::ReadError) -> Error {
    match e {
        // A reset carries the same codes a stop does and means the same
        // outcome, so it maps through the one table every local transport
        // uses too: a draining peer's `SHUTDOWN` is a refusal whichever half
        // of an exchange it arrives on.
        quinn::ReadError::Reset(code) => match codes::stop_reason(code.into_inner()) {
            weida_core::StopReason::Other(other) => {
                Error::Transport(format!("peer reset the stream with code {other}"))
            }
            reason => reason.into(),
        },
        quinn::ReadError::ConnectionLost(e) => conn_error(e),
        quinn::ReadError::ClosedStream => Error::Transport("stream already closed".into()),
        quinn::ReadError::IllegalOrderedRead => {
            Error::Transport("ordered read after unordered read".into())
        }
        quinn::ReadError::ZeroRttRejected => {
            Error::Transport("0-RTT data rejected by the peer".into())
        }
    }
}

/// The connection actor: serializes the frames destructors ask for.
///
/// Holds the connection weakly. A transport handle is not clonable — a local
/// connection owns its channels — so the actor borrows the one the context
/// owns, and ends when the last handle to that context goes away and the
/// control queue closes with it.
async fn driver(ctx: Weak<ConnCtx>, mut rx: mpsc::Receiver<Ctl>, exec: Exec) {
    while let Some(ctl) = rx.recv().await {
        let Some(ctx) = ctx.upgrade() else { break };
        handle_ctl(ctx, ctl, &exec);
    }
}

fn handle_ctl(ctx: ConnHandle, ctl: Ctl, exec: &Exec) {
    match ctl {
        Ctl::SendUnsubscribe { path, filter } => {
            let header = SubscriptionHeader::new(&*path, filter).encode();
            exec.spawn(async move {
                if let Err(e) = write_control(&ctx.conn, FrameKind::Unsubscribe, &header).await {
                    tracing::debug!(error = %e, "failed to send an UNSUBSCRIBE frame");
                }
            });
        }
        Ctl::ReplyError { mut send, code } => {
            exec.spawn(async move {
                if let Err(e) = write_error_frame(&mut send, code).await {
                    tracing::debug!(error = %e, "failed to report a reply failure");
                }
            });
        }
    }
}

/// Writes an ERROR frame and the FIN on the reply half of an exchange.
///
/// ERROR is legal nowhere else: it is the alternative to a reply, so it needs
/// neither a transfer id nor a stream of its own.
pub(crate) async fn write_error_frame(send: &mut SendHalf, code: ErrorCode) -> Result<(), Error> {
    let frame = encode_frame(FrameKind::Error, &ErrorHeader::new(code).encode());
    send.write_all(&frame).await?;
    send.finish()
}

/// Writes one header-only control frame on its own unidirectional stream.
pub(crate) async fn write_control(
    conn: &Link,
    kind: FrameKind,
    header: &[u8],
) -> Result<(), Error> {
    let mut stream = if kind == FrameKind::Hello {
        conn.open_control().await?
    } else {
        conn.open_uni().await?
    };
    stream.write_all(&encode_frame(kind, header)).await?;
    stream.finish()?;
    Ok(())
}

/// The HELLO this side sends: v0 plus the configured guarantee declarations.
///
/// Offered and required are the same set: a peer that offers less fails the
/// handshake, which is what lets the rest of the code treat the local set as
/// the effective one (`docs/PROTOCOL.md` §2.3 step 6).
fn hello_for(limits: Limits, guarantees: GuaranteeSet, quic: bool) -> Hello {
    let declaration = (!guarantees.is_core()).then_some(guarantees);
    // Capability `1` exactly when the profile enables flows, and `2` when it
    // asks for path reports on a QUIC connection (`docs/PROTOCOL.md` §6.1).
    let mut capabilities = Vec::new();
    if limits.datagram_receive_bytes > 0 {
        capabilities.push(CAPABILITY_DATAGRAM);
    }
    if limits.path_report && quic {
        capabilities.push(CAPABILITY_PATH_REPORT);
    }
    Hello {
        guarantees_offered: declaration,
        guarantees_required: declaration,
        capabilities,
        ..Hello::v0(
            limits.max_header_bytes,
            u64::from(limits.max_concurrent_uni_streams),
        )
    }
}

/// Sends our HELLO, declaring the configured guarantee set.
async fn send_hello(ctx: ConnHandle, limits: Limits, guarantees: GuaranteeSet) {
    let hello = hello_for(limits, guarantees, ctx.conn.is_quic());
    if let Err(e) = write_control(&ctx.conn, FrameKind::Hello, &hello.encode()).await {
        tracing::debug!(error = %e, "failed to send HELLO");
    }
}

/// Closes the connection if the peer's HELLO never arrives.
async fn hello_deadline(ctx: ConnHandle, agreed_tx: Arc<watch::Sender<Option<Agreed>>>) {
    tokio::select! {
        () = ctx.exec.sleep(Duration::from_millis(ctx.limits.hello_timeout_ms)) => {}
        // A connection that is already gone needs no deadline, and holding
        // one would keep its state alive for the whole timeout.
        _ = ctx.conn.closed() => return,
    }
    if agreed_tx.borrow().is_none() {
        tracing::debug!("peer HELLO did not arrive in time");
        ctx.conn.close(codes::NEGOTIATION_FAILED, "hello timeout");
    }
}

/// Accepts inbound unidirectional streams and spawns one task per stream.
///
/// The tasks are bounded by the QUIC `max_concurrent_uni_streams` limit: the
/// peer cannot create more parse tasks than the transport lets it open
/// streams. What those tasks hold for their frame heads is bounded
/// separately, by [`ConnCtx::header_slots`].
async fn accept_uni_loop(ctx: ConnHandle, agreed_tx: Arc<watch::Sender<Option<Agreed>>>) {
    loop {
        match ctx.conn.accept_uni().await {
            Ok(stream) => {
                // Admission stopped: the drain refuses new work rather than
                // taking on more of it (`docs/decisions/0009-drain.md` §4.5).
                if ctx.shared.drain.is_draining() {
                    refuse_uni(stream);
                    continue;
                }
                let ctx = Arc::clone(&ctx);
                let agreed_tx = Arc::clone(&agreed_tx);
                ctx.exec.clone().spawn(async move {
                    if let Err(e) = handle_stream(&ctx, &agreed_tx, stream).await {
                        tracing::debug!(error = %e, "inbound stream failed");
                    }
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "connection closed; uni accept loop ending");
                break;
            }
        }
    }
}

/// Accepts inbound bidirectional streams: one exchange each.
///
/// Bounded the same way, by `max_concurrent_bidi_streams`.
async fn accept_bi_loop(ctx: ConnHandle) {
    loop {
        match ctx.conn.accept_bi().await {
            Ok((mut send, recv)) => {
                if ctx.shared.drain.is_draining() {
                    refuse_uni(recv);
                    send.reset(codes::SHUTDOWN);
                    continue;
                }
                let ctx = Arc::clone(&ctx);
                let by_path = ctx.conn.dispatch_by_path();
                ctx.exec.clone().spawn(async move {
                    let outcome = if by_path {
                        handle_local(&ctx, send, recv).await
                    } else {
                        handle_bi(&ctx, send, recv).await
                    };
                    if let Err(e) = outcome {
                        tracing::debug!(error = %e, "inbound exchange failed");
                    }
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "connection closed; bidi accept loop ending");
                break;
            }
        }
    }
}

/// Dispatches one accepted **local** connection, by the path it addresses.
///
/// A local connection carries no stream kind — that vocabulary is QUIC's —
/// so what decides whether a reply is expected is the pattern registered at
/// the path (`docs/PROTOCOL.md` §2.1,
/// [decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
/// §4.3). A replier answers on the same connection; everything else is a
/// one-way transfer and the write half is dropped.
async fn handle_local(ctx: &ConnHandle, send: SendHalf, mut recv: RecvHalf) -> Result<(), Error> {
    let preamble = match read_preamble_within(ctx, &mut recv).await {
        Ok(preamble) => preamble,
        Err(Error::Protocol(reason)) => return violation(ctx, &reason),
        Err(e) => return refuse_head(recv, send, e),
    };

    // Nothing but HELLO may be interpreted before negotiation completes,
    // which is the rule `handle_stream` states for QUIC — and it matters
    // *more* here: every frame arrives on an OS connection of its own, so a
    // frame overtaking the control connection's HELLO is likelier on these
    // transports than on QUIC. Without the gate a peer whose HELLO will be
    // refused could still spend `max_subscriptions` and register
    // subscriptions that outlive its refusal.
    if preamble.kind != FrameKind::Hello {
        ctx.negotiated().await?;
    }
    let head = match read_head_within(ctx, &mut recv, &preamble).await {
        Ok(Ok(head)) => head,
        Ok(Err(e)) => return violation(ctx, &e.to_string()),
        Err(e) => return refuse_head(recv, send, e),
    };

    match head {
        // HELLO belongs to the control connection and nowhere else: a peer
        // that sends one here has not understood the grouping [0012 §4.1].
        Head::Hello(_) => violation(ctx, "HELLO is legal only on the control connection"),
        Head::Error => violation(ctx, "ERROR is legal only in answer to a request"),
        Head::Subscribe(header) => handle_subscription(ctx, header, true).await,
        Head::Unsubscribe(header) => handle_subscription(ctx, header, false).await,
        Head::Credit(header) => handle_credit(ctx, header).await,
        // A report about a transfer we sent on this connection. Both
        // dispatchers route it to the same place, so QUIC, inproc and the two
        // grouped local transports share one implementation.
        Head::Cursor(header) => {
            drop(send);
            handle_cursor(ctx, recv, header).await
        }
        Head::Flow(header) => {
            drop(send);
            crate::flow::handle_flow(ctx, recv, header).await
        }
        // A path report rides QUIC only (`docs/PROTOCOL.md` §6.10): a local
        // transport has no path, and its HELLO never lists code 2.
        Head::Report => violation(ctx, "REPORT is legal only on QUIC"),
        Head::Data(decoded) => {
            let Some(path) = decoded.endpoint.as_deref() else {
                return violation(ctx, "DATA on a local connection must name an endpoint");
            };
            match ctx.namespace.lookup(path) {
                // A reply is expected: the exchange rides this connection in
                // both directions.
                Some(Route::Request(queue)) => {
                    let request = request_for(ctx, decoded, recv, send);
                    if let Err(gone) = queue.send(request).await {
                        tracing::debug!(
                            path = ?gone.0.meta().endpoint,
                            "endpoint went away while dispatching"
                        );
                    }
                    Ok(())
                }
                Some(Route::Raw(queue)) => {
                    let request = request_for(ctx, decoded, recv, send);
                    if let Err(gone) = queue.send(Incoming::Exchange(request)).await
                        && let Incoming::Exchange(request) = gone.0
                    {
                        tracing::debug!(
                            path = ?request.meta().endpoint,
                            "acceptor went away while dispatching"
                        );
                    }
                    Ok(())
                }
                // Nothing writes back on a puller or publisher path, so the
                // write half goes away with the chance to reply; the refusal
                // of an unknown or mismatched path is `dispatch`'s.
                _ => {
                    drop(send);
                    dispatch_data(ctx, recv, decoded).await
                }
            }
        }
    }
}

/// Refuses an inbound stream without reading it: the drain's answer to work
/// that arrived too late.
fn refuse_uni(mut stream: RecvHalf) {
    stream.stop(codes::SHUTDOWN);
}

/// Refuses a CURSOR stream naming a report this side never ordered.
///
/// A cursor is never load-bearing, so an unknown `report_id` costs a stream
/// reset and nothing else: no state was allocated for an id we never handed
/// out, which is exactly the hostile case (`docs/PROTOCOL.md` §6.7 rule 3).
/// The connection survives.
fn refuse_cursor(mut stream: RecvHalf) -> Result<(), Error> {
    stream.stop(codes::CANCELED);
    Ok(())
}

/// Applies one inbound CURSOR stream to the report it names.
///
/// Four rules of `docs/PROTOCOL.md` §6.7, in this order:
///
/// * a malformed head frame is a connection violation, exactly as every other
///   malformed header is;
/// * an unknown `report_id` resets the stream and the connection survives —
///   no state exists for an id we never handed out, which is the hostile
///   case;
/// * a record that repeats an offset or moves one backwards changes nothing,
///   and neither does a level the sender never ordered: [`crate::cursor::CursorSet::advance`]
///   keeps the maximum, so the peer is told nothing and no notification is
///   sent;
/// * a record truncated at FIN **is** a violation. A record is at most
///   `MAX_CURSOR_RECORD_LEN` bytes and a sender writes whole records, so half
///   of one at FIN is a codec bug rather than a race.
async fn handle_cursor(
    ctx: &ConnHandle,
    mut stream: RecvHalf,
    head: CursorHeader,
) -> Result<(), Error> {
    let Some(tx) = ctx.reports.claim(head.report_id) else {
        return refuse_cursor(stream);
    };

    // One record is at most 16 bytes; the buffer holds a few so a burst costs
    // one read rather than one per record, and `pending` never grows past a
    // record plus one read.
    let mut buf = [0u8; 64];
    let mut pending: Vec<u8> = Vec::with_capacity(64 + MAX_CURSOR_RECORD_LEN);
    loop {
        match stream.read(&mut buf).await {
            Ok(Some(0)) => continue,
            Ok(Some(n)) => {
                pending.extend_from_slice(&buf[..n]);
                let mut at = 0;
                while let Some((level, offset, used)) = match decode_cursor_record(&pending[at..]) {
                    Ok(record) => record,
                    Err(e) => return violation(ctx, &e.to_string()),
                } {
                    at += used;
                    tx.send_if_modified(|shared| shared.advance(level, offset));
                }
                pending.drain(..at);
            }
            Ok(None) => {
                if !pending.is_empty() {
                    return violation(ctx, "a cursor stream ended inside a record");
                }
                break;
            }
            // The peer reset the cursor stream, or the connection went away.
            // Neither is a failure of anything: a cursor is never
            // load-bearing, and the reader learns it by the channel closing.
            Err(e) => {
                tracing::debug!(error = %e, report_id = head.report_id, "cursor stream ended");
                break;
            }
        }
    }
    // Dropping the last sender is what turns `Cursors::changed()` into
    // `None`; the table entry has to go first, or ours would not be the last.
    ctx.reports.release(head.report_id);
    drop(tx);
    Ok(())
}

/// The header bytes a frame head keeps without the heap: a typical DATA
/// header is about 70 bytes, and a trace context adds 58.
const INLINE_HEADER: usize = 192;

/// How much of a larger header's buffer is committed before the bytes for it
/// have arrived.
const HEADER_STEP: usize = 2048;

/// The header bytes of one frame.
///
/// Held inline up to [`INLINE_HEADER`] and on the heap above it, where the
/// buffer grows as bytes arrive: the length a preamble announces is the
/// peer's number, and memory follows the bytes the peer has actually sent,
/// which `quinn`'s windows already bound.
pub(crate) enum HeaderBytes {
    Inline {
        buf: [u8; INLINE_HEADER],
        len: usize,
    },
    Heap(Vec<u8>),
}

impl std::ops::Deref for HeaderBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            HeaderBytes::Inline { buf, len } => &buf[..*len],
            HeaderBytes::Heap(bytes) => bytes,
        }
    }
}

/// A frame head decoded as its kind says, which is what every dispatcher
/// works from.
///
/// It is made under the header slot, with the raw bytes still in hand
/// ([`read_header_within`]), and the bytes are gone when it exists: a FLOW or
/// a REPORT stream lives as long as its peer lets it, a SUBSCRIBE waits on a
/// full queue, and none of them can be holding up to `max_header_bytes` of a
/// peer's padding while they do.
enum Head {
    Hello(Hello),
    Data(DataHeader),
    /// Never decoded: it is a violation wherever it arrives on a stream the
    /// dispatchers read, and its content would change nothing about that.
    Error,
    Subscribe(SubscriptionHeader),
    Unsubscribe(SubscriptionHeader),
    Credit(CreditHeader),
    Cursor(CursorHeader),
    Flow(FlowHeader),
    /// The REPORT head frame carries nothing; its records follow it.
    Report,
}

/// Decodes the header bytes of a frame of `kind`.
fn decode_head(kind: FrameKind, bytes: &[u8]) -> Result<Head, HeaderError> {
    Ok(match kind {
        FrameKind::Hello => Head::Hello(Hello::decode(bytes)?),
        FrameKind::Data => Head::Data(DataHeader::decode(bytes)?),
        FrameKind::Error => Head::Error,
        FrameKind::Subscribe => Head::Subscribe(SubscriptionHeader::decode(bytes)?),
        FrameKind::Unsubscribe => Head::Unsubscribe(SubscriptionHeader::decode(bytes)?),
        FrameKind::Credit => Head::Credit(CreditHeader::decode(bytes)?),
        FrameKind::Cursor => Head::Cursor(CursorHeader::decode(bytes)?),
        FrameKind::Flow => Head::Flow(FlowHeader::decode(bytes)?),
        FrameKind::Report => {
            ReportHead::decode(bytes)?;
            Head::Report
        }
    })
}

/// Fills `buf` from `stream`. The stream ending first is the peer ending it
/// inside something it began.
async fn fill(stream: &mut RecvHalf, buf: &mut [u8], part: &str) -> Result<(), Error> {
    let mut filled = 0;
    while filled < buf.len() {
        match stream.read(&mut buf[filled..]).await? {
            Some(0) => continue,
            Some(n) => filled += n,
            None => return Err(Error::Protocol(format!("stream ended inside the {part}"))),
        }
    }
    Ok(())
}

/// Reads one preamble, enforcing the header cap before anything is
/// allocated.
async fn read_preamble(stream: &mut RecvHalf, max_header_bytes: u64) -> Result<Preamble, Error> {
    let mut head = [0u8; MAX_PREAMBLE_LEN];
    // Magic, kind and the first byte of the length. Its two high bits say how
    // many bytes the length takes in all, so the second read is exactly the
    // remainder and the preamble never takes a byte of the header.
    fill(stream, &mut head[..3], "preamble").await?;
    let end = 2 + (1usize << (head[2] >> 6));
    fill(stream, &mut head[3..end], "preamble").await?;
    parse_preamble(&head[..end], max_header_bytes)
        .map(|(preamble, _)| preamble)
        .map_err(|e| Error::Protocol(e.to_string()))
}

/// Reads the header a preamble announced, whose length the preamble has
/// already held to `max_header_bytes`.
async fn read_header(stream: &mut RecvHalf, preamble: &Preamble) -> Result<HeaderBytes, Error> {
    let len = usize::try_from(preamble.header_len)
        .map_err(|_| Error::Protocol("header length does not fit this platform".into()))?;
    if len <= INLINE_HEADER {
        let mut buf = [0u8; INLINE_HEADER];
        fill(stream, &mut buf[..len], "header").await?;
        return Ok(HeaderBytes::Inline { buf, len });
    }
    let mut bytes: Vec<u8> = Vec::new();
    while bytes.len() < len {
        let start = bytes.len();
        bytes.resize(start + (len - start).min(HEADER_STEP), 0);
        match stream.read(&mut bytes[start..]).await? {
            Some(n) => bytes.truncate(start + n),
            None => return Err(Error::Protocol("stream ended inside the header".into())),
        }
    }
    Ok(HeaderBytes::Heap(bytes))
}

/// Reads a whole frame head: preamble plus the header bytes it announces.
///
/// Without a deadline of its own: whoever waits for a reply (the requester)
/// is waiting on a peer that may be computing, and the bound on that wait is
/// the caller's.
pub(crate) async fn read_frame(
    stream: &mut RecvHalf,
    max_header_bytes: u64,
) -> Result<(Preamble, HeaderBytes), Error> {
    let preamble = read_preamble(stream, max_header_bytes).await?;
    let header = read_header(stream, &preamble).await?;
    Ok((preamble, header))
}

/// Runs one read of an inbound stream's frame head under the header phase's
/// two bounds: a free header slot, and the header deadline counted from the
/// moment the slot was taken.
///
/// Both exist for the stream that opens and then says nothing: it holds a
/// unit of its peer's stream credit and a task for as long as it likes, and
/// the QUIC idle timer does not end it while the peer keeps pinging.
async fn in_header_phase<T>(
    ctx: &ConnCtx,
    read: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let Ok(_slot) = ctx.header_slots.acquire().await else {
        return Err(Error::ConnectionLost(LossCause::LocallyClosed));
    };
    let deadline = Duration::from_millis(ctx.limits.header_timeout_ms);
    ctx.exec.within(deadline, read).await.unwrap_or_else(|| {
        Err(Error::Transport(
            "the frame head did not arrive in time".into(),
        ))
    })
}

async fn read_preamble_within(ctx: &ConnCtx, stream: &mut RecvHalf) -> Result<Preamble, Error> {
    in_header_phase(ctx, read_preamble(stream, ctx.limits.max_header_bytes)).await
}

/// Reads the header a preamble announced and decodes it, under one header
/// slot.
///
/// The raw bytes are dropped inside the slot, as soon as `decode` has run: a
/// peer pads a header with keys the decoder skips, up to `max_header_bytes`,
/// and what a stream keeps while it is dispatched, parked on a full queue or
/// reading its payload for hours, is the decoded header, bounded by the
/// protocol's text caps. The memory a connection holds for frame heads is
/// therefore the slots times `max_header_bytes`
/// ([`Limits::worst_case_header_memory`]) whatever its streams go on to do.
///
/// `Err` is a head that did not arrive; `Ok(Err(_))` is one that did and does
/// not decode, which is a violation.
async fn read_header_within<T>(
    ctx: &ConnCtx,
    stream: &mut RecvHalf,
    preamble: &Preamble,
    decode: impl FnOnce(&[u8]) -> Result<T, HeaderError>,
) -> Result<Result<T, HeaderError>, Error> {
    in_header_phase(ctx, async {
        let header = read_header(stream, preamble).await?;
        Ok(decode(&header))
    })
    .await
}

/// [`read_header_within`] for a stream that may open with any frame kind.
async fn read_head_within(
    ctx: &ConnCtx,
    stream: &mut RecvHalf,
    preamble: &Preamble,
) -> Result<Result<Head, HeaderError>, Error> {
    let kind = preamble.kind;
    read_header_within(ctx, stream, preamble, |bytes| decode_head(kind, bytes)).await
}

/// Ends an inbound stream whose frame head did not arrive: the peer is told
/// with `STOP_SENDING`, and the connection stays, because a stalled stream
/// is not a malformed one.
fn refuse_stream(mut stream: RecvHalf, error: Error) -> Result<(), Error> {
    stream.stop(codes::CANCELED);
    Err(error)
}

/// Reads the preamble and header of one inbound unidirectional stream, then
/// dispatches it.
async fn handle_stream(
    ctx: &ConnHandle,
    agreed_tx: &watch::Sender<Option<Agreed>>,
    mut stream: RecvHalf,
) -> Result<(), Error> {
    // A parked reverse connection waits here for the peer's first byte, with
    // no header slot and no deadline: until the peer writes, it is a stream
    // the peer has not opened yet, not one whose frame head is late.
    let opened = tokio::select! {
        opened = stream.opened() => opened?,
        reason = ctx.conn.closed() => return Err(reason),
    };
    if !opened {
        return Ok(());
    }
    let preamble = match read_preamble_within(ctx, &mut stream).await {
        Ok(preamble) => preamble,
        Err(Error::Protocol(reason)) => return violation(ctx, &reason),
        Err(e) => return refuse_stream(stream, e),
    };

    // Nothing but HELLO may be interpreted before negotiation completes. A
    // stream parked here holds no header slot: the HELLO that ends the wait
    // may be behind it.
    if preamble.kind != FrameKind::Hello {
        ctx.negotiated().await?;
    }

    let head = match read_head_within(ctx, &mut stream, &preamble).await {
        Ok(Ok(head)) => head,
        Ok(Err(e)) => return violation(ctx, &e.to_string()),
        Err(e) => return refuse_stream(stream, e),
    };

    match head {
        Head::Hello(theirs) => handle_hello(ctx, agreed_tx, &theirs),
        Head::Data(header) => dispatch_data(ctx, stream, header).await,
        // ERROR answers a request, and a request always arrives on a
        // bidirectional stream, so its reply half is the only place an ERROR
        // can legitimately appear.
        Head::Error => violation(
            ctx,
            "ERROR is legal only on the reply half of a bidirectional stream",
        ),
        Head::Subscribe(header) => handle_subscription(ctx, header, true).await,
        Head::Unsubscribe(header) => handle_subscription(ctx, header, false).await,
        Head::Credit(header) => handle_credit(ctx, header).await,
        Head::Cursor(header) => handle_cursor(ctx, stream, header).await,
        Head::Flow(header) => crate::flow::handle_flow(ctx, stream, header).await,
        Head::Report => crate::report::receive_reports(ctx, stream).await,
    }
}

pub(crate) fn violation(ctx: &ConnHandle, reason: &str) -> Result<(), Error> {
    tracing::debug!(reason, "closing connection: protocol violation");
    ctx.conn.close(codes::PROTOCOL_VIOLATION, reason);
    Err(Error::Protocol(reason.to_owned()))
}

/// Applies a SUBSCRIBE or UNSUBSCRIBE frame.
///
/// Both carry the same header, and both are already parked behind negotiation
/// by `handle_stream`, so a subscription that overtakes the peer's HELLO is
/// delayed rather than refused.
///
/// **Where it lands depends on what serves the path.** A publisher's path
/// keeps its subscription in the [`crate::pubsub::SubRegistry`], which is
/// what fans a published message out. A raw acceptor's path — an L2 queue —
/// gets the subscription as an event instead, because a queue delivers to
/// *one* consumer and therefore has to know them individually
/// (`docs/decisions/0018-minimal-broker.md` §4.5). The per-connection
/// `max_subscriptions` bound is the same number either way.
async fn handle_subscription(
    ctx: &ConnHandle,
    header: SubscriptionHeader,
    subscribe: bool,
) -> Result<(), Error> {
    let Some(subs) = ctx.subs.as_ref() else {
        // A client connection serves no publishers. Nothing to do, and nothing
        // wrong with the peer's frame.
        tracing::debug!(
            endpoint = ?header.endpoint,
            "ignoring a subscription frame: this side publishes nothing"
        );
        return Ok(());
    };
    let conn_id = ctx.conn.stable_id();

    // A radio's joins: kept in its hub, and counted against the same
    // per-connection `max_subscriptions` as every other subscription.
    if let Some(Route::Radio(hub)) = ctx.namespace.lookup(&header.endpoint) {
        if subscribe {
            let joined = hub.join(
                ctx,
                header.filter,
                header.max_age_ms,
                header.max_layer,
                || subs.reserve(ctx),
            );
            if joined.is_err() {
                return too_many_subscriptions(ctx);
            }
        } else {
            hub.leave(conn_id, &header.filter);
        }
        return Ok(());
    }

    if let Some(Route::Raw(queue)) = ctx.namespace.lookup(&header.endpoint) {
        if subscribe {
            // One reservation per filter this connection holds on the path.
            // A repeated SUBSCRIBE changes nothing: it is not counted twice
            // and the queue is not told twice.
            if ctx
                .namespace
                .note_consumer(conn_id, &header.endpoint, &header.filter)
            {
                if subs.reserve(ctx).is_err() {
                    return too_many_subscriptions(ctx);
                }
                // The record is made, and the connection may have closed
                // before it was: `drop_consumers` takes a connection's records
                // only once the close is visible, so either this check sees
                // the close or that call sees the record. A record made after
                // it ran is one nothing removes, and the queue would be told
                // of a consumer whose end it has already been told of.
                if ctx.conn.close_reason().is_some() {
                    ctx.namespace
                        .forget_consumer(conn_id, &header.endpoint, &header.filter);
                    return Ok(());
                }
                let consumer = Consumer::new(
                    Arc::clone(ctx),
                    Arc::from(header.endpoint.as_str()),
                    header.filter,
                );
                if queue.send(Incoming::Subscribed(consumer)).await.is_err() {
                    tracing::debug!(endpoint = ?header.endpoint, "acceptor went away; subscription dropped");
                }
            }
        } else if ctx
            .namespace
            .forget_consumer(conn_id, &header.endpoint, &header.filter)
        {
            // Only a filter this connection really holds gives a reservation
            // back: releasing for an UNSUBSCRIBE that matches nothing would
            // let a peer walk the count down and subscribe without limit.
            subs.release(conn_id);
            let gone = Incoming::Unsubscribed {
                id: ConsumerId::from_conn(conn_id),
                filter: Some(header.filter),
            };
            if queue.send(gone).await.is_err() {
                tracing::debug!(endpoint = ?header.endpoint, "acceptor went away; unsubscribe dropped");
            }
        }
        return Ok(());
    }

    if subscribe {
        if subs
            .subscribe(&header.endpoint, ctx, header.filter)
            .is_err()
        {
            return too_many_subscriptions(ctx);
        }
    } else {
        subs.unsubscribe(&header.endpoint, conn_id, &header.filter);
    }
    Ok(())
}

/// SUBSCRIBE arrives on a unidirectional stream, so there is no reply half to
/// answer with an ERROR frame; the connection is the only granularity
/// available.
fn too_many_subscriptions(ctx: &ConnHandle) -> Result<(), Error> {
    tracing::debug!(
        max = ctx.limits.max_subscriptions,
        "subscription limit reached; closing the connection"
    );
    ctx.conn.close(
        codes::LIMIT_EXCEEDED,
        "too many subscriptions on one connection",
    );
    Err(Error::LimitExceeded)
}

/// Applies a CREDIT frame: an L2 consumer's absolute delivery limit.
///
/// Only a queue can honour one, so only a raw acceptor's path receives it. A
/// path with no queue **ignores** it rather than closing the connection or
/// resetting the stream: credit is idempotent state, not a transfer, and the
/// frame has no reply half to refuse on — the same position SUBSCRIBE takes
/// for a path no publisher has claimed yet.
async fn handle_credit(ctx: &ConnHandle, header: CreditHeader) -> Result<(), Error> {
    let Some(Route::Raw(queue)) = ctx.namespace.lookup(&header.endpoint) else {
        tracing::debug!(
            endpoint = ?header.endpoint,
            "ignoring a credit frame: no queue serves this path"
        );
        return Ok(());
    };
    let grant = Incoming::Credit(CreditGrant {
        id: ConsumerId::from_conn(ctx.conn.stable_id()),
        filter: header.filter,
        limit: header.limit,
    });
    if queue.send(grant).await.is_err() {
        tracing::debug!(endpoint = ?header.endpoint, "acceptor went away; credit dropped");
    }
    Ok(())
}

/// What a closed connection owes the queues it consumed on: a notice, per
/// queue, that its consumer is gone.
///
/// A queue needs this and a publisher does not: a publisher forgets a
/// subscriber, while a queue owes something for what it handed that consumer
/// and has to decide what to do with it (0018 §4.7).
///
/// It holds the queues and the consumer's id and **nothing of the
/// connection**. A queue that is full and unread makes [`Departure::announce`]
/// wait for as long as its application likes, and that wait must not keep a
/// dead connection, or anything that counts it, alive with it.
pub(crate) struct Departure {
    id: ConsumerId,
    queues: Vec<(Arc<str>, mpsc::Sender<Incoming>)>,
}

/// Takes what `ctx`'s connection owes the queues it consumed on.
///
/// Called once, when the connection closes. The table is emptied by the same
/// call, so a second close reports nothing.
pub(crate) fn take_departure(ctx: &ConnCtx) -> Departure {
    let conn_id = ctx.conn.stable_id();
    let queues = ctx
        .namespace
        .take_consumer_routes(conn_id)
        .into_iter()
        .filter_map(|(path, route)| match route {
            Route::Raw(queue) => Some((path, queue)),
            _ => None,
        })
        .collect();
    Departure {
        id: ConsumerId::from_conn(conn_id),
        queues,
    }
}

impl Departure {
    /// Whether there is nobody to tell.
    pub(crate) fn is_empty(&self) -> bool {
        self.queues.is_empty()
    }

    /// Tells every queue, waiting for room in each.
    pub(crate) async fn announce(self) {
        for (path, queue) in self.queues {
            let gone = Incoming::Unsubscribed {
                id: self.id,
                filter: None,
            };
            if queue.send(gone).await.is_err() {
                tracing::debug!(path = ?path, "acceptor went away before its consumer did");
            }
        }
    }
}

/// Applies the peer's HELLO.
///
/// A side sends exactly one (`docs/PROTOCOL.md` §2.2). A second would
/// rewrite the parameters that live traffic already runs under and, where
/// path reports were agreed, start another report task each time: a rate
/// the peer chooses, where §6.10 says it cannot. So it is a violation, and
/// the agreement is stored by whichever HELLO gets there first.
fn handle_hello(
    ctx: &ConnHandle,
    agreed_tx: &watch::Sender<Option<Agreed>>,
    theirs: &Hello,
) -> Result<(), Error> {
    if agreed_tx.borrow().is_some() {
        return violation(ctx, "a second HELLO on one connection");
    }
    let ours = hello_for(ctx.limits, ctx.guarantees, ctx.conn.is_quic());
    match negotiate(&ours, theirs) {
        Ok(agreed) => {
            // Two HELLOs can both reach this point; only the first to store
            // its result is the HELLO.
            let first = agreed_tx.send_if_modified(|slot| {
                if slot.is_some() {
                    return false;
                }
                *slot = Some(agreed);
                true
            });
            if !first {
                return violation(ctx, "a second HELLO on one connection");
            }
            tracing::debug!(
                version = agreed.version,
                send_max_header_bytes = agreed.send_max_header_bytes,
                "negotiated"
            );
            if agreed.path_report {
                ctx.exec
                    .spawn(crate::report::send_reports(ConnHandle::clone(ctx)));
            }
            Ok(())
        }
        Err(e) => {
            tracing::debug!(error = %e, "negotiation failed");
            ctx.conn.close(codes::NEGOTIATION_FAILED, &e.to_string());
            Err(e.into())
        }
    }
}

/// Routes one decoded one-way DATA header and the stream behind it.
///
/// Shared by the QUIC dispatcher and the local one. A misroute is answered
/// with `STOP_SENDING`, not with a frame: there is no reply half here to
/// carry one, and the stop code says everything an ERROR header would have.
async fn dispatch_data(
    ctx: &ConnHandle,
    mut stream: RecvHalf,
    header: DataHeader,
) -> Result<(), Error> {
    // The numbers the receiver routes by are copied out first: the metadata
    // then takes the header's strings over, so nothing is cloned and what
    // is checked below is read straight from the metadata.
    let (sequence, producer) = (header.sequence, header.producer);
    let mut meta = Arc::new(IncomingMeta::from_header(
        header,
        ctx.peer.clone(),
        ctx.peer_chain.clone(),
    ));
    let Some(path) = meta.endpoint.as_deref() else {
        return violation(ctx, "DATA on a unidirectional stream must name an endpoint");
    };

    // The scope is the pair (path, topic) for a transfer that carries a topic
    // and the path alone otherwise: the `(producer, endpoint or topic)`
    // scope of decision 0001 §7.1, with the endpoint kept because two
    // publishers that both publish one topic number their copies
    // independently. Both the dedup window and the gap detector key on it.
    let topic = meta.topic.as_deref();

    // Deduplication sits between the wire and everything else: a repeat is
    // read to EOF and thrown away, so the sender sees an ordinary receipt and
    // neither the application nor the gap detector ever sees the message
    // twice. A dedup window saves the application, not the bandwidth — the
    // bytes crossed the wire before anything could know they were a repeat.
    if ctx
        .dedup
        .is_duplicate(producer, path, topic, sequence, ctx.exec.now())
    {
        ctx.shared.duplicates.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(path, sequence = ?sequence, "duplicate suppressed");
        // Read to EOF rather than dropped: dropping the stream would reset
        // it, and the sender would read that as a refusal
        // ([FAILURE_MODEL.md](../../../docs/FAILURE_MODEL.md) §4) — which a
        // successfully deduplicated message is not.
        return stream.discard().await;
    }

    // Reassemble mode: hold this arrival if the numbers before it have not
    // come yet, and dispatch whatever run that completes. Held transfers are
    // unread streams — nothing is materialized — so what waits is quinn's
    // receive buffer for them, which is what makes `max_reorder_hold` a
    // memory bound.
    if ctx.reorder.enabled() {
        let held = Held {
            stream,
            meta: Arc::clone(&meta),
        };
        let released = ctx.reorder.admit(path, topic, sequence, held);
        drop(meta);
        for (held, gap) in released {
            let mut meta = held.meta;
            if gap.is_some() {
                Arc::make_mut(&mut meta).gap = gap;
            }
            dispatch(
                ctx,
                IncomingTransfer::new(held.stream, meta, Arc::clone(ctx)),
            )
            .await;
        }
        return Ok(());
    }

    // Detect mode: report what is missing and deliver what arrived.
    let gap = sequence.and_then(|seq| ctx.gaps.observe(path, topic, seq));
    if gap.is_some() {
        Arc::make_mut(&mut meta).gap = gap;
    }
    dispatch(ctx, IncomingTransfer::new(stream, meta, Arc::clone(ctx))).await;
    Ok(())
}

/// One transfer, held with everything needed to deliver it later.
///
/// The payload is **not** here: a held transfer is an unread `RecvStream`,
/// which is what keeps reassembly inside the "core transport does not require
/// payload materialization" invariant (`docs/INVARIANTS.md`). The path it
/// was addressed to is `meta.endpoint`.
pub(crate) struct Held {
    stream: RecvHalf,
    meta: Arc<IncomingMeta>,
}

/// The refusal a stream earns when the path it names cannot serve it —
/// [PROTOCOL.md](../../../docs/PROTOCOL.md) §9.3's table, as one table.
///
/// Both dispatchers take their refusal from this one table instead of each
/// writing out its own pairing of `ErrorCode` and stop code at the call site,
/// so §9.3 is enforced in one place: a seventh [`Route`] variant added to one
/// match and not the other would be a silent divergence between what a Push
/// sender and a Req sender learn about the same misroute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Refusal {
    /// What an exchange's ERROR frame carries.
    pub(crate) code: ErrorCode,
    /// What a one-way stream's `STOP_SENDING` carries.
    pub(crate) stop: u64,
}

impl Refusal {
    /// Nothing is registered at this path — and the same answer for an
    /// endpoint that went away between the lookup and the hand-over, because
    /// a sender cannot tell the two apart and does not need to.
    pub(crate) const UNKNOWN: Refusal = Refusal {
        code: ErrorCode::UnknownEndpoint,
        stop: codes::UNKNOWN_ENDPOINT,
    };
    /// The path is served, by something that does not take this stream kind:
    /// a one-way transfer aimed at a replier, an exchange aimed at a puller,
    /// anything aimed at a publisher. Refusing is the honest answer; the
    /// alternative is to reinterpret the sender.
    pub(crate) const WRONG_SHAPE: Refusal = Refusal {
        code: ErrorCode::Unsupported,
        stop: codes::UNSUPPORTED,
    };
    /// A paired endpoint already has its one peer. A capacity decision rather
    /// than a routing mistake, which is why it is a third row and not a
    /// variant of the second
    /// ([0005](../../../docs/decisions/0005-refusal-race.md)).
    pub(crate) const PAIR_TAKEN: Refusal = Refusal {
        code: ErrorCode::Rejected,
        stop: codes::LIMIT_EXCEEDED,
    };
}

/// Which stream kind is asking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wanted {
    /// A unidirectional transfer: Push, Pub/Sub, PAIR, BUS.
    OneWay,
    /// The initiating half of an exchange: Req/Rep, SURVEY.
    Exchange,
    /// A datagram flow's FLOW stream.
    Flow,
}

/// `None` when the route serves `wanted`, otherwise the refusal it earns.
pub(crate) fn refusal_for(route: Option<&Route>, wanted: Wanted) -> Option<Refusal> {
    match (route, wanted) {
        (None, _) => Some(Refusal::UNKNOWN),
        (Some(Route::Raw(_)), _) => None,
        (Some(Route::Transfer(_) | Route::Pair { .. } | Route::Dish(_)), Wanted::OneWay) => None,
        (Some(Route::Request(_)), Wanted::Exchange) => None,
        // A dish takes a radio's datagram segments on flows (0034 §4.6).
        (Some(Route::Dish(_)), Wanted::Flow) => None,
        // Everything else is the path being served by the wrong shape: a
        // publisher or a radio takes nothing inbound, a replier takes no
        // one-way transfer, and a pair carries one-way transfers in both
        // directions so an exchange aimed at one is the same category error
        // as an exchange aimed at a puller.
        (
            Some(
                Route::Request(_)
                | Route::Transfer(_)
                | Route::Pub
                | Route::Pair { .. }
                | Route::Radio(_)
                | Route::Dish(_),
            ),
            _,
        ) => Some(Refusal::WRONG_SHAPE),
    }
}

/// Hands one arrived transfer to whatever is bound at `path`.
///
/// Every refusal here comes from [`refusal_for`] or a [`Refusal`] constant;
/// no stop code is written out at a call site.
async fn dispatch(ctx: &ConnHandle, transfer: IncomingTransfer) {
    // The path is read through a handle of its own, so it stays borrowable
    // while the transfer moves on to whatever serves it.
    let meta = transfer.meta_arc();
    let Some(path) = meta.endpoint.as_deref() else {
        transfer.refuse(Refusal::UNKNOWN.stop);
        return;
    };
    let route = ctx.namespace.lookup(path);
    if let Some(refusal) = refusal_for(route.as_ref(), Wanted::OneWay) {
        tracing::debug!(path, ?refusal, "the path does not serve a one-way transfer");
        transfer.refuse(refusal.stop);
        return;
    }
    // A segment not newer than one delivered here on its path and topic is
    // stale, whoever receives it; a dish counts its own (0037 §4.11).
    if !matches!(route, Some(Route::Dish(_)))
        && let (Some(topic), Some(segment)) =
            (transfer.meta().topic.as_deref(), transfer.meta().segment)
        && !ctx
            .segments_in
            .fresh(path, topic, segment, transfer.meta().layer.unwrap_or(0))
    {
        tracing::debug!(
            path,
            topic,
            segment,
            "a segment not newer than one delivered here is refused"
        );
        transfer.refuse(codes::CANCELED);
        return;
    }
    match route {
        // Awaiting a queue slot is the backpressure path: it stalls this
        // stream's task, which stalls the peer through QUIC flow control.
        Some(Route::Transfer(queue)) => {
            if let Err(e) = queue.send(transfer).await {
                tracing::debug!(path, "endpoint went away while dispatching");
                e.0.refuse(Refusal::UNKNOWN.stop);
            }
        }
        Some(Route::Raw(queue)) => {
            if let Err(e) = queue.send(Incoming::Stream(transfer)).await {
                tracing::debug!(path, "acceptor went away while dispatching");
                if let Incoming::Stream(t) = e.0 {
                    t.refuse(Refusal::UNKNOWN.stop);
                }
            }
        }
        // Exactly one peer, and the **first** one is kept: a stream from any
        // other connection is refused while the first keeps working. ZeroMQ's
        // PAIR drops the newcomer silently; refusing and saying so is this
        // repository's rule for a capacity decision
        // (`docs/decisions/0005-refusal-race.md`).
        Some(Route::Pair { queue, owner }) => {
            if !owner.claim(ctx) {
                tracing::debug!(path, "a paired endpoint already has its peer");
                transfer.refuse(Refusal::PAIR_TAKEN.stop);
                return;
            }
            if let Err(e) = queue.send(transfer).await {
                tracing::debug!(path, "endpoint went away while dispatching");
                e.0.refuse(Refusal::UNKNOWN.stop);
            }
        }
        // A segment: the dish discards one not newer than the newest
        // delivered on this connection, path and topic, and never blocks the
        // connection on a full queue (0034 §4.6, 0037 §4.11).
        Some(Route::Dish(route)) => crate::radio::deliver_segment(ctx, path, &route, transfer),
        // `refusal_for` above already refused these.
        Some(Route::Request(_) | Route::Pub | Route::Radio(_)) | None => {
            unreachable!("refusal_for refuses every route that cannot serve a one-way transfer")
        }
    }
}

/// Wraps an arrived exchange: the request body and the reply half, with the
/// metadata the header gave, moved out of it rather than copied.
fn request_for(
    ctx: &ConnHandle,
    header: DataHeader,
    recv: RecvHalf,
    send: SendHalf,
) -> IncomingRequest {
    IncomingRequest::new(
        IncomingTransfer::new(
            recv,
            Arc::new(IncomingMeta::from_header(
                header,
                ctx.peer.clone(),
                ctx.peer_chain.clone(),
            )),
            Arc::clone(ctx),
        ),
        send,
        Arc::clone(ctx),
    )
}

/// Ends a stream whose frame head did not arrive: both halves are refused,
/// so a peer that opened an exchange and went quiet learns that it was
/// turned away instead of waiting for a reply that is not coming.
fn refuse_head(mut recv: RecvHalf, mut send: SendHalf, error: Error) -> Result<(), Error> {
    recv.stop(codes::CANCELED);
    send.reset(codes::CANCELED);
    Err(error)
}

/// Dispatches one exchange arriving on a bidirectional stream.
///
/// Only DATA may open one. A misroute is answered on the reply half with a
/// real ERROR frame, which is the whole reason the reply half exists.
async fn handle_bi(ctx: &ConnHandle, mut send: SendHalf, mut recv: RecvHalf) -> Result<(), Error> {
    let preamble = match read_preamble_within(ctx, &mut recv).await {
        Ok(preamble) => preamble,
        Err(Error::Protocol(reason)) => return violation(ctx, &reason),
        Err(e) => return refuse_head(recv, send, e),
    };
    if preamble.kind != FrameKind::Data {
        return violation(
            ctx,
            &format!("{} may not open a bidirectional stream", preamble.kind),
        );
    }
    ctx.negotiated().await?;

    let header = match read_header_within(ctx, &mut recv, &preamble, DataHeader::decode).await {
        Ok(Ok(header)) => header,
        Ok(Err(e)) => return violation(ctx, &e.to_string()),
        Err(e) => return refuse_head(recv, send, e),
    };
    let Some(path) = header.endpoint.as_deref() else {
        return violation(
            ctx,
            "the initiating half of an exchange must name an endpoint",
        );
    };

    let route = ctx.namespace.lookup(path);
    if let Some(refusal) = refusal_for(route.as_ref(), Wanted::Exchange) {
        tracing::debug!(path = ?path, ?refusal, "the path does not serve an exchange");
        // Said on the reply half as a real ERROR frame, and the request half
        // is stopped so a payload still being written is not accepted. A
        // refusal builds no metadata and no request: nothing here is kept.
        recv.stop(refusal.stop);
        if let Err(e) = write_error_frame(&mut send, refusal.code).await {
            tracing::debug!(error = %e, "failed to refuse an exchange");
        }
        return Ok(());
    }
    let request = request_for(ctx, header, recv, send);
    match route {
        // Backpressure again: a full accept queue stalls this task, and the
        // peer feels it through flow control. If the endpoint went away, the
        // request's own destructor reports NO_REPLY.
        Some(Route::Request(queue)) => {
            if let Err(gone) = queue.send(request).await {
                tracing::debug!(
                    path = ?gone.0.meta().endpoint,
                    "endpoint went away while dispatching"
                );
            }
        }
        Some(Route::Raw(queue)) => {
            if let Err(gone) = queue.send(Incoming::Exchange(request)).await
                && let Incoming::Exchange(request) = gone.0
            {
                tracing::debug!(
                    path = ?request.meta().endpoint,
                    "acceptor went away while dispatching"
                );
            }
        }
        // `refusal_for` above already refused these.
        Some(
            Route::Transfer(_) | Route::Pub | Route::Pair { .. } | Route::Radio(_) | Route::Dish(_),
        )
        | None => {
            unreachable!("refusal_for refuses every route that cannot serve an exchange")
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ConnCtx, ConnHandle, Refusal, Wanted, handle_subscription, read_header_within,
        read_preamble_within, refusal_for, take_departure,
    };
    use crate::listener::{Namespace, PairOwner, Route};
    use crate::pubsub::SubRegistry;
    use crate::runtime::{Exec, Shared};
    use crate::transport::{Link, RecvHalf};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::mpsc;
    use weida_core::Limits;
    use weida_protocol::header::{GuaranteeSet, OrderingMode};
    use weida_protocol::{DataHeader, FrameKind, SubscriptionHeader, encode_frame};

    fn transfer() -> Route {
        Route::Transfer(mpsc::channel(1).0)
    }

    fn request() -> Route {
        Route::Request(mpsc::channel(1).0)
    }

    fn raw() -> Route {
        Route::Raw(mpsc::channel(1).0)
    }

    fn pair() -> Route {
        Route::Pair {
            queue: mpsc::channel(1).0,
            owner: Arc::new(PairOwner::new()),
        }
    }

    /// One row per pairing of [`PROTOCOL.md`] §9.3's table, both stream kinds,
    /// which is the point of having one table: a seventh `Route` variant added
    /// to one dispatcher and not the other would be a silent divergence
    /// between what a Push sender and a Req sender learn about the same
    /// misroute.
    ///
    /// [`PROTOCOL.md`]: https://github.com/tuco86/weida/blob/main/docs/PROTOCOL.md
    #[test]
    fn every_route_mismatch_earns_the_refusal_the_protocol_names() {
        // Nothing registered: the same answer for both kinds.
        assert_eq!(refusal_for(None, Wanted::OneWay), Some(Refusal::UNKNOWN));
        assert_eq!(refusal_for(None, Wanted::Exchange), Some(Refusal::UNKNOWN));

        // A raw acceptor takes both kinds, tagged.
        assert_eq!(refusal_for(Some(&raw()), Wanted::OneWay), None);
        assert_eq!(refusal_for(Some(&raw()), Wanted::Exchange), None);

        // A puller and a pair take one-way transfers and refuse exchanges.
        for route in [transfer(), pair()] {
            assert_eq!(refusal_for(Some(&route), Wanted::OneWay), None);
            assert_eq!(
                refusal_for(Some(&route), Wanted::Exchange),
                Some(Refusal::WRONG_SHAPE)
            );
        }

        // A replier is the mirror image.
        assert_eq!(refusal_for(Some(&request()), Wanted::Exchange), None);
        assert_eq!(
            refusal_for(Some(&request()), Wanted::OneWay),
            Some(Refusal::WRONG_SHAPE)
        );

        // A publisher takes nothing inbound at all.
        assert_eq!(
            refusal_for(Some(&Route::Pub), Wanted::OneWay),
            Some(Refusal::WRONG_SHAPE)
        );
        assert_eq!(
            refusal_for(Some(&Route::Pub), Wanted::Exchange),
            Some(Refusal::WRONG_SHAPE)
        );

        // A flow is served by a raw acceptor only; everything else is the
        // wrong shape, and no path at all is unknown.
        assert_eq!(refusal_for(None, Wanted::Flow), Some(Refusal::UNKNOWN));
        assert_eq!(refusal_for(Some(&raw()), Wanted::Flow), None);
        for route in [transfer(), pair(), request(), Route::Pub] {
            assert_eq!(
                refusal_for(Some(&route), Wanted::Flow),
                Some(Refusal::WRONG_SHAPE)
            );
        }

        // A radio, like a publisher, takes nothing inbound.
        let radio = Route::Radio(Arc::new(crate::radio::RadioHub::new(
            "/r",
            weida_core::Limits::default(),
        )));
        for wanted in [Wanted::OneWay, Wanted::Exchange, Wanted::Flow] {
            assert_eq!(
                refusal_for(Some(&radio), wanted),
                Some(Refusal::WRONG_SHAPE)
            );
        }
    }

    /// The three rows carry the codes the wire documents name, and the
    /// capacity row is **not** the routing row: a pair that already has its
    /// peer is `LIMIT_EXCEEDED`, which a sender can tell apart from a path
    /// that serves the wrong shape.
    #[test]
    fn the_three_refusals_are_distinct_on_the_wire() {
        use weida_protocol::codes;
        assert_eq!(Refusal::UNKNOWN.stop, codes::UNKNOWN_ENDPOINT);
        assert_eq!(Refusal::WRONG_SHAPE.stop, codes::UNSUPPORTED);
        assert_eq!(Refusal::PAIR_TAKEN.stop, codes::LIMIT_EXCEEDED);
        assert_ne!(Refusal::UNKNOWN, Refusal::WRONG_SHAPE);
        assert_ne!(Refusal::WRONG_SHAPE, Refusal::PAIR_TAKEN);
    }

    /// A connection a namespace can hold consumer records for: the accepted
    /// end of an in-process link. The dialled end comes back too, because
    /// dropping it ends the link.
    async fn connection(
        namespace: &Arc<Namespace>,
        subs: &Arc<SubRegistry>,
        bus: &str,
    ) -> (ConnHandle, crate::inproc::LocalConn) {
        let mut dials = crate::inproc::bind(bus).expect("bus");
        let dialled = crate::inproc::dial(bus, 255, 1024).expect("dial");
        let accepted = dials.recv().await.expect("accepted end");
        let shared = Arc::new(Shared {
            duplicates: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            drain: crate::drain::DrainState::new(),
        });
        let ctx = ConnCtx::spawn(
            Link::Local(accepted),
            Limits::default(),
            Arc::clone(namespace),
            Some(Arc::clone(subs)),
            Exec::current().expect("a tokio runtime"),
            GuaranteeSet::CORE,
            shared,
        );
        (ctx, dialled)
    }

    /// A SUBSCRIBE whose connection closed before it was handled: the accept
    /// task has taken the connection's records already, so a record made now
    /// is one nothing removes, and the queue must not be told of a consumer
    /// whose end it has been told of.
    #[tokio::test]
    async fn a_queue_subscription_handled_after_its_connection_closed_leaves_no_record() {
        let namespace = Arc::new(Namespace::new());
        let subs = Arc::new(SubRegistry::new(Limits::default(), OrderingMode::None));
        let (queue, mut announced) = mpsc::channel(8);
        namespace
            .register("/q", Route::Raw(queue))
            .expect("register");
        let (ctx, _dialled) = connection(&namespace, &subs, "conn-unit-closed-subscribe").await;
        let conn_id = ctx.conn.stable_id();
        ctx.conn.close(
            weida_protocol::codes::SHUTDOWN,
            "closed before its SUBSCRIBE was handled",
        );
        // What the accept task does once the close is visible.
        take_departure(&ctx).announce().await;

        handle_subscription(&ctx, SubscriptionHeader::new("/q", "a.#"), true)
            .await
            .expect("nothing is wrong with the frame");
        assert!(
            namespace.take_consumer_routes(conn_id).is_empty(),
            "a record made now would never be removed"
        );
        assert!(
            announced.try_recv().is_err(),
            "the queue is told of nothing"
        );
    }

    /// The raw header bytes are decoded, and dropped, while the stream still
    /// holds its header slot: what a connection holds for frame heads is
    /// bounded by its slots, whatever its streams go on to do afterwards.
    #[tokio::test]
    async fn a_header_is_decoded_while_its_slot_is_held() {
        let namespace = Arc::new(Namespace::new());
        let subs = Arc::new(SubRegistry::new(Limits::default(), OrderingMode::None));
        let (ctx, dialled) = connection(&namespace, &subs, "conn-unit-header-slot").await;
        // A stream toward the dialled end, whose receiving half is ours to read.
        let mut send = ctx.open_uni().await.expect("open");
        let frame = encode_frame(FrameKind::Data, &DataHeader::addressed("/q").encode());
        send.write_all(&frame).await.expect("write");
        let mut recv = RecvHalf::Local(dialled.accept_uni().await.expect("accept"));
        let preamble = read_preamble_within(&ctx, &mut recv)
            .await
            .expect("preamble");

        let free = ctx.header_slots.available_permits();
        let mut during = None;
        let header = read_header_within(&ctx, &mut recv, &preamble, |bytes| {
            during = Some(ctx.header_slots.available_permits());
            DataHeader::decode(bytes)
        })
        .await
        .expect("the head arrived")
        .expect("it decodes");
        assert_eq!(header.endpoint.as_deref(), Some("/q"));
        assert_eq!(during, Some(free - 1), "decoded while the slot was held");
        assert_eq!(
            ctx.header_slots.available_permits(),
            free,
            "and the slot is given back after"
        );
    }
}
