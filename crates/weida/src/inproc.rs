//! The in-process transport: `weida+inproc://<bus>/<path>`.
//!
//! The first of the three local transports of
//! [decision 0010](../../../docs/decisions/0010-local-transport.md) §4.1, and
//! the cheapest place to prove that the transport boundary in the runtime is
//! real: there is no socket, no TLS, no credential and nothing to configure —
//! two halves of a `tokio` duplex channel and a name.
//!
//! **A transfer is one channel pair.** That is §4.2's "the OS connection is
//! the stream" with the only OS object an in-process transport has: opening a
//! stream mints a fresh pair, and the stream's lifetime is that pair's. There
//! is no multiplexing over a shared buffer, so the head-of-line coupling
//! [0002](../../../docs/decisions/0002-control-and-bulk-separation.md) exists
//! to remove cannot arise here either.
//!
//! **Nobody is proved, because there is nobody else** [0010 §4.4]: both ends
//! are this process, so `IncomingMeta::peer` is `None`, exactly as it is for
//! an anonymous TLS client.
//!
//! Everything above the transport is unchanged: the same preamble and frames
//! (`docs/PROTOCOL.md` §3, §4), the same HELLO exchange and the same
//! negotiation, with the version fenced by `versions` instead of ALPN
//! (§2.1, [0010 §4.3]).

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use weida_core::{Error, LossCause, MAX_BUS_BYTES};
use weida_protocol::codes;
use weida_runtime::NameRegistry;

/// No code: the sentinel for "nothing signalled here yet".
const NO_CODE: u64 = u64::MAX;

/// Every bus bound in this process. A bus name is unique to the process and
/// two processes using the same name never meet [0010 §4.8].
///
/// The registry is `weida-runtime`'s, with the byte budget passed in: an
/// in-process namespace of bound names is not weida's idea and libzmq's
/// `inproc://` is the same object under another name
/// (`docs/research/ipc.md` §8.1).
static BUSES: LazyLock<NameRegistry<LocalConn>> =
    LazyLock::new(|| NameRegistry::new(MAX_BUS_BYTES));

/// Validates a bus name.
///
/// The length and the control bytes are the registry's rule; the separator is
/// weida's, because a bus name is one field of `weida+inproc://<bus>/<path>`
/// and a `/` in it would name a different endpoint.
pub fn validate_bus(bus: &str) -> Result<(), Error> {
    BUSES.validate(bus)?;
    if bus.as_bytes().contains(&b'/') {
        return Err(Error::InvalidAddress(format!(
            "invalid byte in inproc bus name: {bus:?}"
        )));
    }
    Ok(())
}

/// Registers `bus` and returns the queue of connections dialled to it.
pub(crate) fn bind(bus: &str) -> Result<mpsc::UnboundedReceiver<LocalConn>, Error> {
    validate_bus(bus)?;
    BUSES.bind(bus)
}

/// Removes `bus` from the registry; the binding's own drop calls this.
pub(crate) fn unbind(bus: &str) {
    BUSES.unbind(bus);
}

/// Resolves once `bus` is bound, at once if it already is: what a redial
/// waits on in process, where a bus is back exactly when its name is
/// registered again (0031 §4.10).
pub(crate) async fn wait_bound(bus: &str) {
    BUSES.wait_bound(bus).await;
}

/// Dials `bus`, handing the far half to whoever bound it.
pub(crate) fn dial(bus: &str, max_streams: usize, buffer: usize) -> Result<LocalConn, Error> {
    validate_bus(bus)?;
    // Nothing is bound here: the same outcome a dial to a closed port has,
    // reported before anything is allocated.
    let Some(tx) = BUSES.lookup(bus) else {
        return Err(Error::ConnectionLost(LossCause::PeerClosed));
    };
    let (dialled, accepted) = LocalConn::pair(max_streams, buffer);
    tx.send(accepted)
        .map_err(|_| Error::ConnectionLost(LossCause::PeerClosed))?;
    Ok(dialled)
}

/// One side of an in-process connection.
///
/// Holds the queues its peer opens streams into, and shares one closed-state
/// cell with that peer so either side's close is seen by both.
pub(crate) struct LocalConn {
    id: usize,
    /// Unidirectional streams this side opens, handed to the peer.
    uni_to_peer: mpsc::UnboundedSender<LocalRecv>,
    /// Exchanges this side opens.
    bi_to_peer: mpsc::UnboundedSender<(LocalSend, LocalRecv)>,
    /// One queue per stream kind, because the two accept loops are separate
    /// tasks: a single queue would let the loop expecting one kind consume
    /// the other's stream.
    uni_from_peer: tokio::sync::Mutex<mpsc::UnboundedReceiver<LocalRecv>>,
    bi_from_peer: tokio::sync::Mutex<mpsc::UnboundedReceiver<(LocalSend, LocalRecv)>>,
    state: Arc<LinkState>,
    /// The slots live streams over this connection hold, one permit each:
    /// `max_local_streams` of them, and an `open` with none free waits for
    /// one rather than failing (`docs/GUARANTEES.md` §6).
    slots: Arc<Semaphore>,
    buffer: usize,
}

/// Shared by both ends of one connection: whether it is closed, why, and by
/// which end.
struct LinkState {
    code: AtomicU64,
    reason: StdMutex<String>,
    /// The `id` of the end that closed: on QUIC a local close and a peer's
    /// close are two different errors, and the redial policy tells them
    /// apart, so the one shared cell has to remember who wrote it.
    closed_by: AtomicUsize,
    closed: Notify,
}

impl LinkState {
    fn close(&self, code: u64, reason: &str, by: usize) {
        if self
            .code
            .compare_exchange(NO_CODE, code, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.closed_by.store(by, Ordering::Release);
            *self.reason.lock().expect("close reason poisoned") = reason.to_owned();
            self.closed.notify_waiters();
        }
    }

    fn close_reason(&self, me: usize) -> Option<Error> {
        let code = self.code.load(Ordering::Acquire);
        (code != NO_CODE).then(|| {
            let local = self.closed_by.load(Ordering::Acquire) == me;
            closed_error(code, local)
        })
    }
}

/// Maps a local close code onto the outcome vocabulary, the same way
/// `conn_error` maps an application close on QUIC — including its
/// distinction between this side's close and the peer's.
fn closed_error(code: u64, local: bool) -> Error {
    match code {
        codes::NEGOTIATION_FAILED => {
            Error::Negotiation("peer closed the connection: negotiation failed".into())
        }
        codes::LIMIT_EXCEEDED => Error::LimitExceeded,
        codes::PROTOCOL_VIOLATION => Error::Protocol("peer reported a protocol violation".into()),
        _ if local => Error::ConnectionLost(LossCause::LocallyClosed),
        _ => Error::ConnectionLost(LossCause::PeerClosed),
    }
}

impl LocalConn {
    /// Builds the two ends of one connection.
    fn pair(max_streams: usize, buffer: usize) -> (LocalConn, LocalConn) {
        let (a_uni_tx, a_uni_rx) = mpsc::unbounded_channel();
        let (b_uni_tx, b_uni_rx) = mpsc::unbounded_channel();
        let (a_bi_tx, a_bi_rx) = mpsc::unbounded_channel();
        let (b_bi_tx, b_bi_rx) = mpsc::unbounded_channel();
        let state = Arc::new(LinkState {
            code: AtomicU64::new(NO_CODE),
            reason: StdMutex::new(String::new()),
            closed_by: AtomicUsize::new(usize::MAX),
            closed: Notify::new(),
        });
        // One budget per connection, shared by both directions: what the cap
        // bounds is the live transfers on this connection. tokio panics
        // above `Semaphore::MAX_PERMITS`.
        let slots = Arc::new(Semaphore::new(max_streams.min(Semaphore::MAX_PERMITS)));
        let (id, peer_id) = (
            crate::transport::next_serial(),
            crate::transport::next_serial(),
        );
        (
            LocalConn {
                id,
                uni_to_peer: b_uni_tx,
                bi_to_peer: b_bi_tx,
                uni_from_peer: tokio::sync::Mutex::new(a_uni_rx),
                bi_from_peer: tokio::sync::Mutex::new(a_bi_rx),
                state: Arc::clone(&state),
                slots: Arc::clone(&slots),
                buffer,
            },
            LocalConn {
                id: peer_id,
                uni_to_peer: a_uni_tx,
                bi_to_peer: a_bi_tx,
                uni_from_peer: tokio::sync::Mutex::new(b_uni_rx),
                bi_from_peer: tokio::sync::Mutex::new(b_bi_rx),
                state,
                slots,
                buffer,
            },
        )
    }

    pub(crate) fn stable_id(&self) -> usize {
        self.id
    }

    pub(crate) fn close_reason(&self) -> Option<Error> {
        self.state.close_reason(self.id)
    }

    pub(crate) fn close(&self, code: u64, reason: &str) {
        self.state.close(code, reason, self.id);
    }

    /// Resolves when either side closes, with the reason.
    pub(crate) async fn closed(&self) -> Error {
        loop {
            if let Some(reason) = self.state.close_reason(self.id) {
                return reason;
            }
            self.state.closed.notified().await;
        }
    }

    /// Whether every slot is taken right now.
    ///
    /// One atomic load, for the caller that has something to free before it
    /// parks on a slot (`ConnCtx::open_uni`).
    pub(crate) fn slots_exhausted(&self) -> bool {
        self.slots.available_permits() == 0
    }

    /// A slot for one more live transfer, waiting for one where the cap is
    /// reached.
    ///
    /// This is the local shape of what `quinn` does with the peer's
    /// `max_concurrent_uni_streams`: an `open` with no budget left parks
    /// until a stream ends, and the deadline is the caller's, not the
    /// transport's. Dropping the future is the cancellation, and it takes
    /// nothing with it, because a waiter holds no slot.
    async fn slot(&self) -> Result<Arc<StreamSlot>, Error> {
        let waiting = Arc::clone(&self.slots).acquire_owned();
        tokio::select! {
            permit = waiting => match permit {
                Ok(permit) => Ok(Arc::new(StreamSlot { _permit: permit })),
                Err(_) => Err(Error::ConnectionLost(LossCause::PeerClosed)),
            },
            reason = self.closed() => Err(reason),
        }
    }

    pub(crate) async fn open_uni(&self) -> Result<LocalSend, Error> {
        if let Some(closed) = self.close_reason() {
            return Err(closed);
        }
        let slot = self.slot().await?;
        let (send, recv) = stream_pair(self.buffer, slot);
        self.uni_to_peer
            .send(recv)
            .map_err(|_| Error::ConnectionLost(LossCause::PeerClosed))?;
        Ok(send)
    }

    pub(crate) async fn open_bi(&self) -> Result<(LocalSend, LocalRecv), Error> {
        if let Some(closed) = self.close_reason() {
            return Err(closed);
        }
        // One transfer, one slot, as on the socket transports where an
        // exchange is one connection [0012 §4.3]: the two channel pairs an
        // in-process exchange needs share it and release it together.
        let slot = self.slot().await?;
        let (send, peer_recv) = stream_pair(self.buffer, Arc::clone(&slot));
        let (peer_send, recv) = stream_pair(self.buffer, slot);
        self.bi_to_peer
            .send((peer_send, peer_recv))
            .map_err(|_| Error::ConnectionLost(LossCause::PeerClosed))?;
        Ok((send, recv))
    }

    pub(crate) async fn accept_uni(&self) -> Result<LocalRecv, Error> {
        let mut queue = self.uni_from_peer.lock().await;
        tokio::select! {
            opened = queue.recv() => opened.ok_or(Error::ConnectionLost(LossCause::PeerClosed)),
            reason = self.closed() => Err(reason),
        }
    }

    pub(crate) async fn accept_bi(&self) -> Result<(LocalSend, LocalRecv), Error> {
        let mut queue = self.bi_from_peer.lock().await;
        tokio::select! {
            opened = queue.recv() => opened.ok_or(Error::ConnectionLost(LossCause::PeerClosed)),
            reason = self.closed() => Err(reason),
        }
    }
}

/// Keeps one live stream counted against `max_local_streams`: the permit is
/// returned when both halves of the stream are gone.
struct StreamSlot {
    _permit: OwnedSemaphorePermit,
}

/// Per-stream signalling: the two codes that cross it and the FIN.
struct Signal {
    /// Set by the reader: the peer refuses the rest (`STOP_SENDING`).
    stop: AtomicU64,
    /// Set by the writer: the payload is abandoned (`RESET_STREAM`).
    reset: AtomicU64,
    /// Set by the writer: the payload ended cleanly (FIN).
    finished: AtomicBool,
    changed: Notify,
    _slot: Arc<StreamSlot>,
}

fn stream_pair(buffer: usize, slot: Arc<StreamSlot>) -> (LocalSend, LocalRecv) {
    let (writer, reader) = tokio::io::duplex(buffer);
    let signal = Arc::new(Signal {
        stop: AtomicU64::new(NO_CODE),
        reset: AtomicU64::new(NO_CODE),
        finished: AtomicBool::new(false),
        changed: Notify::new(),
        _slot: slot,
    });
    (
        LocalSend {
            io: Some(writer),
            signal: Arc::clone(&signal),
        },
        LocalRecv {
            io: Some(reader),
            signal,
        },
    )
}

/// The writing half of one in-process stream.
pub(crate) struct LocalSend {
    io: Option<DuplexStream>,
    signal: Arc<Signal>,
}

impl LocalSend {
    fn stop_code(&self) -> Option<u64> {
        let code = self.signal.stop.load(Ordering::Acquire);
        (code != NO_CODE).then_some(code)
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        if let Some(code) = self.stop_code() {
            return Err(codes::stop_reason(code).into());
        }
        let Some(io) = self.io.as_mut() else {
            return Err(Error::Transport("stream already closed".into()));
        };
        match io.write_all(buf).await {
            Ok(()) => Ok(()),
            Err(e) => Err(match self.stop_code() {
                Some(code) => codes::stop_reason(code).into(),
                None => Error::Transport(format!("local stream write failed: {e}")),
            }),
        }
    }

    /// Ends the payload: the FIN.
    ///
    /// Synchronous, like the QUIC one, because there is nothing to flush —
    /// a write only returned once its bytes were in the peer's buffer, and
    /// dropping this half is what the reader sees as the end.
    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        if self.io.take().is_none() {
            return Err(Error::Transport("stream already closed".into()));
        }
        self.signal.finished.store(true, Ordering::Release);
        self.signal.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn reset(&mut self, code: u64) {
        self.signal.reset.store(code, Ordering::Release);
        self.signal.changed.notify_waiters();
        self.io = None;
    }

    /// The receipt: resolves once the peer holds the payload, or once the
    /// peer refuses it.
    ///
    /// In process, "the peer's transport holds every byte" is literally true
    /// as soon as the bytes are in the channel and the FIN is set, because
    /// the channel *is* the peer's transport — and the channel is bounded,
    /// so a write only completed if there was room for it.
    pub(crate) fn stopped(
        &self,
    ) -> impl Future<Output = Result<Option<u64>, Error>> + Send + Sync + use<> {
        let signal = Arc::clone(&self.signal);
        async move {
            loop {
                let waiting = signal.changed.notified();
                let stop = signal.stop.load(Ordering::Acquire);
                if stop != NO_CODE {
                    return Ok(Some(stop));
                }
                if signal.finished.load(Ordering::Acquire) {
                    return Ok(None);
                }
                waiting.await;
            }
        }
    }
}

/// The reading half of one in-process stream.
pub(crate) struct LocalRecv {
    io: Option<DuplexStream>,
    signal: Arc<Signal>,
}

impl LocalRecv {
    /// Reads what is available; `None` at the end of the payload.
    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, Error> {
        let Some(io) = self.io.as_mut() else {
            return Ok(None);
        };
        let read = io
            .read(buf)
            .await
            .map_err(|e| Error::Transport(format!("local stream read failed: {e}")))?;
        if read == 0 {
            // End of the channel: a FIN unless the writer abandoned it.
            let reset = self.signal.reset.load(Ordering::Acquire);
            if reset != NO_CODE {
                return Err(codes::stop_reason(reset).into());
            }
            return Ok(None);
        }
        Ok(Some(read))
    }

    /// Refuses the rest of the payload, the local `STOP_SENDING`.
    pub(crate) fn stop(&mut self, code: u64) {
        self.signal.stop.store(code, Ordering::Release);
        self.signal.changed.notify_waiters();
        self.io = None;
    }

    pub(crate) fn io_mut(&mut self) -> Option<&mut DuplexStream> {
        self.io.as_mut()
    }

    pub(crate) fn reset_code(&self) -> Option<u64> {
        let code = self.signal.reset.load(Ordering::Acquire);
        (code != NO_CODE).then_some(code)
    }
}

impl LocalSend {
    pub(crate) fn io_mut(&mut self) -> Option<&mut DuplexStream> {
        self.io.as_mut()
    }
}

impl Drop for LocalRecv {
    fn drop(&mut self) {
        // A reader that walks away from a payload still being written is a
        // cancelled read, and the writer's receipt must not wait for a peer
        // that is gone. A reader that drops *after* the FIN cancels nothing:
        // the transfer already landed, and saying otherwise would turn every
        // completed fire-and-forget transfer into a refusal.
        if self.io.is_some()
            && !self.signal.finished.load(Ordering::Acquire)
            && self.signal.stop.load(Ordering::Acquire) == NO_CODE
        {
            self.signal.stop.store(codes::CANCELED, Ordering::Release);
            self.signal.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_bus_name_is_bounded_and_has_no_separator() {
        assert!(validate_bus("orders").is_ok());
        assert!(validate_bus(&"x".repeat(MAX_BUS_BYTES)).is_ok());
        assert!(validate_bus(&"x".repeat(MAX_BUS_BYTES + 1)).is_err());
        assert!(validate_bus("").is_err());
        assert!(validate_bus("has/slash").is_err());
        assert!(validate_bus("has\ncontrol").is_err());
    }

    #[tokio::test]
    async fn a_stream_carries_bytes_and_its_fin() {
        let (a, _b) = LocalConn::pair(8, 64 * 1024);
        let mut send = a.open_uni().await.expect("open");
        send.write_all(b"payload").await.expect("write");
        send.finish().expect("finish");
        let mut recv = _b.accept_uni().await.expect("accept");
        let mut buf = [0u8; 16];
        let n = recv.read(&mut buf).await.expect("read").expect("bytes");
        assert_eq!(&buf[..n], b"payload");
        assert_eq!(recv.read(&mut buf).await.expect("read"), None, "the FIN");
    }

    #[tokio::test(start_paused = true)]
    async fn the_stream_budget_makes_an_open_wait_for_a_slot() {
        let (a, b) = LocalConn::pair(2, 1024);
        let _one = a.open_uni().await.expect("first");
        let _two = a.open_uni().await.expect("second");
        let third = tokio::spawn(async move {
            a.open_uni().await.expect("the third waits for a slot");
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !third.is_finished(),
            "max_local_streams must bound live transfers"
        );
        drop(_one);
        // The slot is returned when both halves of that stream are gone.
        let accepted = b.accept_uni().await.expect("accept");
        drop(accepted);
        weida_testkit::within("a slot came free", Duration::from_secs(5), third)
            .await
            .expect("the waiting open completed");
    }

    #[tokio::test]
    async fn a_refused_stream_reports_its_code_to_the_writer() {
        let (a, b) = LocalConn::pair(8, 1024);
        let send = a.open_uni().await.expect("open");
        let mut recv = b.accept_uni().await.expect("accept");
        recv.stop(codes::REJECTED);
        let stopped = send.stopped().await.expect("stopped");
        assert_eq!(stopped, Some(codes::REJECTED));
    }

    #[tokio::test]
    async fn closing_either_side_is_seen_by_both() {
        let (a, b) = LocalConn::pair(8, 1024);
        assert!(a.close_reason().is_none());
        b.close(codes::SHUTDOWN, "going away");
        assert!(matches!(
            a.close_reason(),
            Some(Error::ConnectionLost(LossCause::PeerClosed))
        ));
        assert!(
            a.open_uni().await.is_err(),
            "a closed connection opens nothing"
        );
    }
}
