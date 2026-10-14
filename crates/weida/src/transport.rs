//! The transport boundary: what the runtime needs from a transport, and
//! nothing more.
//!
//! The transports are the variants of three closed enums over QUIC,
//! in-process links, `AF_UNIX` and named pipes
//! ([decision 0010](../../../docs/decisions/0010-local-transport.md)). They
//! carry the same frames, the same HELLO and the same negotiation
//! (`docs/PROTOCOL.md` §2.1), so what differs is exactly the three types
//! below: a connection that opens and accepts streams, and the two halves of
//! a stream.
//!
//! An enum rather than a trait object, deliberately. The set of transports is
//! closed and small, it is decided in this crate, and the payload path must
//! stay a direct call: dispatching a `write_all` through a vtable would put
//! an indirection on exactly the path `docs/INVARIANTS.md` keeps free of task
//! hops and locks. Another transport means one variant per type and no new
//! concept.
//!
//! Everything transport-specific lives behind these types: error mapping,
//! the stream-kind vocabulary, and the two codes that can cross a stream
//! (`STOP_SENDING` and `RESET_STREAM`, or their local equivalents).

use std::pin::Pin;
use std::task::{Context, Poll};

use quinn::VarInt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use weida_core::{Error, PeerIdentity};

use crate::conn::{conn_error, read_error, write_error};
use crate::identity::PeerChain;
use crate::inproc::{LocalConn, LocalRecv, LocalSend};
use weida_protocol::LOCAL_MAX_DATAGRAM;

/// One connection: a QUIC connection, one in-process link, or one peer's
/// group of local connections — `AF_UNIX` or named pipe
/// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)).
pub(crate) enum Link {
    Quic(quinn::Connection),
    Local(LocalConn),
    /// Boxed: a grouped link carries its pools and channels inline and would
    /// otherwise dwarf the other variants.
    #[cfg(unix)]
    Unix(Box<crate::grouped::Grouped<crate::unix::UnixLocal>>),
    #[cfg(windows)]
    Pipe(Box<crate::grouped::Grouped<crate::pipe::PipeStream>>),
}

/// The writing half of one stream.
pub(crate) enum SendHalf {
    Quic(quinn::SendStream),
    Local(LocalSend),
    #[cfg(unix)]
    Unix(crate::grouped::LocalSend<crate::unix::UnixLocal>),
    #[cfg(windows)]
    Pipe(crate::grouped::LocalSend<crate::pipe::PipeStream>),
}

/// The reading half of one stream.
pub(crate) enum RecvHalf {
    Quic(quinn::RecvStream),
    Local(LocalRecv),
    #[cfg(unix)]
    Unix(crate::grouped::LocalRecv<crate::unix::UnixLocal>),
    #[cfg(windows)]
    Pipe(crate::grouped::LocalRecv<crate::pipe::PipeStream>),
}

impl Link {
    /// Who the peer is, once it has been proved, or `None`.
    ///
    /// Two kinds of proof and never a claim: a key from the TLS handshake, or
    /// a principal the kernel attributed to the process on the other end
    /// [0010 §4.4]. In process there is nobody else, so there is nothing to
    /// prove and nothing to report.
    pub(crate) fn peer(&self) -> Option<PeerIdentity> {
        match self {
            Link::Quic(conn) => crate::tls::peer_fingerprint(conn).map(PeerIdentity::Key),
            Link::Local(_) => None,
            #[cfg(unix)]
            Link::Unix(conn) => conn.peer(),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.peer(),
        }
    }

    /// The chain behind [`Link::peer`], when the peer proved a key and the
    /// chain fits the bound weida keeps
    /// ([decisions/0035](../../../docs/decisions/0035-keys-proved-not-judged.md)
    /// §4.2). No local transport has one.
    pub(crate) fn peer_chain(&self) -> Option<PeerChain> {
        match self {
            Link::Quic(conn) => crate::tls::peer_chain(conn),
            Link::Local(_) => None,
            #[cfg(unix)]
            Link::Unix(_) => None,
            #[cfg(windows)]
            Link::Pipe(_) => None,
        }
    }

    /// Whether an accepted stream is dispatched by the **path** it addresses
    /// rather than by its stream kind.
    ///
    /// True on the socket transports, where a connection carries no kind and
    /// the pattern registered at the path says whether a reply is expected
    /// (`docs/PROTOCOL.md` §2.1,
    /// [decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.3).
    pub(crate) fn dispatch_by_path(&self) -> bool {
        match self {
            Link::Quic(_) | Link::Local(_) => false,
            #[cfg(unix)]
            Link::Unix(_) => true,
            #[cfg(windows)]
            Link::Pipe(_) => true,
        }
    }

    /// True where a stream is an operating-system object counted against
    /// `max_local_streams` rather than a QUIC stream inside one connection.
    ///
    /// What depends on it: how many receipts the drain may park. A parked
    /// receipt holds its send half, and on these transports that half is a
    /// descriptor ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.2), so a parked set sized for QUIC's stream budgets would hold the
    /// whole local ceiling and leave nothing to open with.
    pub(crate) fn streams_are_local(&self) -> bool {
        match self {
            Link::Quic(_) => false,
            Link::Local(_) => true,
            #[cfg(unix)]
            Link::Unix(_) => true,
            #[cfg(windows)]
            Link::Pipe(_) => true,
        }
    }

    /// True where every local stream slot is in use, so the next `open` will
    /// wait for one.
    ///
    /// The caller that has parked receipts holding slots frees them first
    /// (`ConnCtx::open_uni`). One atomic load; on QUIC there is no such
    /// budget to read, because the peer's stream limit is `quinn`'s to
    /// account for.
    pub(crate) fn local_slots_exhausted(&self) -> bool {
        match self {
            Link::Quic(_) => false,
            Link::Local(conn) => conn.slots_exhausted(),
            #[cfg(unix)]
            Link::Unix(conn) => conn.slots_exhausted(),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.slots_exhausted(),
        }
    }

    /// An identifier stable for the life of this connection, for the
    /// subscription registry.
    pub(crate) fn stable_id(&self) -> usize {
        match self {
            Link::Quic(conn) => conn.stable_id(),
            Link::Local(conn) => conn.stable_id(),
            #[cfg(unix)]
            Link::Unix(conn) => conn.stable_id(),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.stable_id(),
        }
    }

    /// Why this connection is closed, or `None` while it is live.
    pub(crate) fn close_reason(&self) -> Option<Error> {
        match self {
            Link::Quic(conn) => conn.close_reason().map(conn_error),
            Link::Local(conn) => conn.close_reason(),
            #[cfg(unix)]
            Link::Unix(conn) => conn.close_reason(),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.close_reason(),
        }
    }

    pub(crate) fn close(&self, code: u64, reason: &str) {
        match self {
            Link::Quic(conn) => conn.close(
                VarInt::from_u64(code).expect("application codes are small"),
                reason.as_bytes(),
            ),
            Link::Local(conn) => conn.close(code, reason),
            #[cfg(unix)]
            Link::Unix(conn) => conn.close(code, reason),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.close(code, reason),
        }
    }

    /// Resolves when the connection closes, with the reason.
    pub(crate) async fn closed(&self) -> Error {
        match self {
            Link::Quic(conn) => conn_error(conn.closed().await),
            Link::Local(conn) => conn.closed().await,
            #[cfg(unix)]
            Link::Unix(conn) => conn.closed().await,
            #[cfg(windows)]
            Link::Pipe(conn) => conn.closed().await,
        }
    }

    pub(crate) async fn open_uni(&self) -> Result<SendHalf, Error> {
        match self {
            Link::Quic(conn) => conn
                .open_uni()
                .await
                .map(SendHalf::Quic)
                .map_err(conn_error),
            Link::Local(conn) => conn.open_uni().await.map(SendHalf::Local),
            #[cfg(unix)]
            Link::Unix(conn) => conn.open_uni().await.map(SendHalf::Unix),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.open_uni().await.map(SendHalf::Pipe),
        }
    }

    /// Opens the stream a HELLO is written on.
    ///
    /// Only a grouped socket transport distinguishes it: there the two ends
    /// of the control connection are the one pair of stream halves that exist
    /// without dialling, and HELLO is what they are for
    /// ([decisions/0012](../../../docs/decisions/0012-local-connection-grouping.md) §4.1).
    pub(crate) async fn open_control(&self) -> Result<SendHalf, Error> {
        match self {
            #[cfg(unix)]
            Link::Unix(conn) => conn.open_control().map(SendHalf::Unix),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.open_control().map(SendHalf::Pipe),
            _ => self.open_uni().await,
        }
    }

    /// Whether a peer that dialled this side can only be written to over
    /// connections it parked.
    ///
    /// QUIC and the in-process pair let either end open a stream at any
    /// time. A socket transport does not: an accepted socket is not
    /// dialable, so fan-out rides the reverse pool of
    /// [0012](../../../docs/decisions/0012-local-connection-grouping.md)
    /// §4.4, and a subscriber over such a transport must park before it can
    /// receive anything.
    pub(crate) fn needs_reverse_pool(&self) -> bool {
        match self {
            #[cfg(unix)]
            Link::Unix(_) => true,
            #[cfg(windows)]
            Link::Pipe(_) => true,
            _ => false,
        }
    }

    /// Fills this connection's reverse pool, returning how many are parked.
    /// Transports that need no pool park nothing and say so.
    pub(crate) async fn park_reverse(&self) -> Result<usize, Error> {
        match self {
            #[cfg(unix)]
            Link::Unix(conn) => conn.park_reverse().await,
            #[cfg(windows)]
            Link::Pipe(conn) => conn.park_reverse().await,
            _ => Ok(0),
        }
    }

    /// Replaces parked connections as the peer spends them, until this
    /// connection closes.
    pub(crate) async fn maintain_reverse(&self) {
        match self {
            #[cfg(unix)]
            Link::Unix(conn) => conn.maintain_reverse().await,
            #[cfg(windows)]
            Link::Pipe(conn) => conn.maintain_reverse().await,
            _ => {}
        }
    }

    pub(crate) async fn open_bi(&self) -> Result<(SendHalf, RecvHalf), Error> {
        match self {
            Link::Quic(conn) => conn
                .open_bi()
                .await
                .map(|(s, r)| (SendHalf::Quic(s), RecvHalf::Quic(r)))
                .map_err(conn_error),
            Link::Local(conn) => conn
                .open_bi()
                .await
                .map(|(s, r)| (SendHalf::Local(s), RecvHalf::Local(r))),
            #[cfg(unix)]
            Link::Unix(conn) => conn
                .open_bi()
                .await
                .map(|(s, r)| (SendHalf::Unix(s), RecvHalf::Unix(r))),
            #[cfg(windows)]
            Link::Pipe(conn) => conn
                .open_bi()
                .await
                .map(|(s, r)| (SendHalf::Pipe(s), RecvHalf::Pipe(r))),
        }
    }

    pub(crate) async fn accept_uni(&self) -> Result<RecvHalf, Error> {
        match self {
            Link::Quic(conn) => conn
                .accept_uni()
                .await
                .map(RecvHalf::Quic)
                .map_err(conn_error),
            Link::Local(conn) => conn.accept_uni().await.map(RecvHalf::Local),
            #[cfg(unix)]
            Link::Unix(conn) => conn.accept_uni().await.map(RecvHalf::Unix),
            #[cfg(windows)]
            Link::Pipe(conn) => conn.accept_uni().await.map(RecvHalf::Pipe),
        }
    }

    pub(crate) async fn accept_bi(&self) -> Result<(SendHalf, RecvHalf), Error> {
        match self {
            Link::Quic(conn) => conn
                .accept_bi()
                .await
                .map(|(s, r)| (SendHalf::Quic(s), RecvHalf::Quic(r)))
                .map_err(conn_error),
            Link::Local(conn) => conn
                .accept_bi()
                .await
                .map(|(s, r)| (SendHalf::Local(s), RecvHalf::Local(r))),
            #[cfg(unix)]
            Link::Unix(conn) => conn
                .accept_bi()
                .await
                .map(|(s, r)| (SendHalf::Unix(s), RecvHalf::Unix(r))),
            #[cfg(windows)]
            Link::Pipe(conn) => conn
                .accept_bi()
                .await
                .map(|(s, r)| (SendHalf::Pipe(s), RecvHalf::Pipe(r))),
        }
    }

    /// True on QUIC, where a flow's datagrams travel as DATAGRAM frames; the
    /// local transports carry them on the FLOW stream (`docs/PROTOCOL.md`
    /// §2.1).
    pub(crate) fn is_quic(&self) -> bool {
        matches!(self, Link::Quic(_))
    }

    /// The largest datagram this connection carries now, prefix included:
    /// what the peer advertised and the path allows on QUIC, `None` when the
    /// peer advertised nothing; [`LOCAL_MAX_DATAGRAM`] on a local transport.
    pub(crate) fn max_datagram_size(&self) -> Option<usize> {
        match self {
            Link::Quic(conn) => conn.max_datagram_size(),
            _ => Some(LOCAL_MAX_DATAGRAM),
        }
    }

    /// Hands one datagram to `quinn` without waiting. QUIC only: a local
    /// flow writes its datagrams on its FLOW stream instead.
    pub(crate) fn send_datagram(&self, data: bytes::Bytes) -> Result<(), Error> {
        match self {
            Link::Quic(conn) => conn.send_datagram(data).map_err(|e| match e {
                quinn::SendDatagramError::UnsupportedByPeer
                | quinn::SendDatagramError::Disabled => Error::DatagramsUnavailable,
                quinn::SendDatagramError::TooLarge => Error::TooLarge {
                    max: conn.max_datagram_size().unwrap_or(0),
                },
                quinn::SendDatagramError::ConnectionLost(e) => conn_error(e),
            }),
            _ => Err(Error::Unsupported),
        }
    }

    /// The next datagram the peer sent. QUIC only; a local transport has
    /// none, and its flows never ask.
    pub(crate) async fn read_datagram(&self) -> Result<bytes::Bytes, Error> {
        match self {
            Link::Quic(conn) => conn.read_datagram().await.map_err(conn_error),
            _ => Err(Error::Unsupported),
        }
    }

    /// The connection's transport statistics on QUIC; `None` on a local
    /// transport, which has no path to measure.
    pub(crate) fn transport_stats(&self) -> Option<crate::TransportStats> {
        match self {
            Link::Quic(conn) => Some(crate::TransportStats::of(conn)),
            _ => None,
        }
    }
}

impl SendHalf {
    /// True for a QUIC stream, whose reset is still accepted after `finish`
    /// until every byte is acknowledged.
    pub(crate) fn is_quic(&self) -> bool {
        matches!(self, SendHalf::Quic(_))
    }

    /// Orders this stream against the other streams of its connection, on
    /// `quinn`'s scale: higher is sent first.
    ///
    /// A no-op on every local transport, where each stream is its own OS
    /// connection and there is nothing on the same connection to order it
    /// against ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md)
    /// §4.3).
    pub(crate) fn set_priority(&self, priority: i32) {
        if let SendHalf::Quic(s) = self {
            let _ = s.set_priority(priority);
        }
    }
}

impl SendHalf {
    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        match self {
            SendHalf::Quic(s) => s.write_all(buf).await.map_err(write_error),
            SendHalf::Local(s) => s.write_all(buf).await,
            #[cfg(unix)]
            SendHalf::Unix(s) => s.write_all(buf).await,
            #[cfg(windows)]
            SendHalf::Pipe(s) => s.write_all(buf).await,
        }
    }

    /// Queues the FIN. Synchronous on every transport: nothing is flushed
    /// here, which is what keeps a fire-and-forget send free of an await.
    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        match self {
            SendHalf::Quic(s) => s
                .finish()
                .map_err(|_| Error::Transport("stream already closed".into())),
            SendHalf::Local(s) => s.finish(),
            #[cfg(unix)]
            SendHalf::Unix(s) => s.finish(),
            #[cfg(windows)]
            SendHalf::Pipe(s) => s.finish(),
        }
    }

    /// Abandons the payload: `RESET_STREAM`, or its local equivalent.
    pub(crate) fn reset(&mut self, code: u64) {
        match self {
            SendHalf::Quic(s) => {
                let _ = s.reset(VarInt::from_u64(code).expect("application codes are small"));
            }
            SendHalf::Local(s) => s.reset(code),
            #[cfg(unix)]
            SendHalf::Unix(s) => s.reset(code),
            #[cfg(windows)]
            SendHalf::Pipe(s) => s.reset(code),
        }
    }

    /// The receipt: `Ok(None)` once the peer's transport holds the payload,
    /// `Ok(Some(code))` if the peer refused it, `Err` if the connection went
    /// away first.
    pub(crate) fn stopped(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, Error>> + Send + Sync>> {
        match self {
            SendHalf::Quic(s) => {
                let stopped = s.stopped();
                Box::pin(async move {
                    match stopped.await {
                        Ok(code) => Ok(code.map(VarInt::into_inner)),
                        // The payload may or may not have arrived, which is
                        // exactly what `Indeterminate` is for.
                        Err(quinn::StoppedError::ConnectionLost(_)) => Err(Error::Indeterminate),
                        Err(quinn::StoppedError::ZeroRttRejected) => {
                            Err(Error::Transport("0-RTT data rejected by the peer".into()))
                        }
                    }
                })
            }
            SendHalf::Local(s) => Box::pin(s.stopped()),
            #[cfg(unix)]
            SendHalf::Unix(s) => Box::pin(s.stopped()),
            #[cfg(windows)]
            SendHalf::Pipe(s) => Box::pin(s.stopped()),
        }
    }
}

impl RecvHalf {
    /// Reads what is available; `None` at the end of the payload.
    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, Error> {
        match self {
            RecvHalf::Quic(r) => r.read(buf).await.map_err(read_error),
            RecvHalf::Local(r) => r.read(buf).await,
            #[cfg(unix)]
            RecvHalf::Unix(r) => r.read(buf).await,
            #[cfg(windows)]
            RecvHalf::Pipe(r) => r.read(buf).await,
        }
    }

    pub(crate) async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Error> {
        match self {
            RecvHalf::Quic(r) => r
                .read_exact(buf)
                .await
                .map_err(|e| Error::Protocol(format!("truncated header: {e}"))),
            RecvHalf::Local(r) => r.read_exact(buf).await,
            #[cfg(unix)]
            RecvHalf::Unix(r) => r.read_exact(buf).await,
            #[cfg(windows)]
            RecvHalf::Pipe(r) => r.read_exact(buf).await,
        }
    }

    /// Refuses the rest of the payload: `STOP_SENDING`, or its local
    /// equivalent.
    pub(crate) fn stop(&mut self, code: u64) {
        match self {
            RecvHalf::Quic(r) => {
                let _ = r.stop(VarInt::from_u64(code).expect("application codes are small"));
            }
            RecvHalf::Local(r) => r.stop(code),
            #[cfg(unix)]
            RecvHalf::Unix(r) => r.stop(code),
            #[cfg(windows)]
            RecvHalf::Pipe(r) => r.stop(code),
        }
    }
}

impl AsyncWrite for SendHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            SendHalf::Quic(s) => AsyncWrite::poll_write(Pin::new(s), cx, buf),
            SendHalf::Local(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_write(cx, buf),
                None => Poll::Ready(Err(closed_io())),
            },
            #[cfg(unix)]
            SendHalf::Unix(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_write(cx, buf),
                None => Poll::Ready(Err(closed_io())),
            },
            #[cfg(windows)]
            SendHalf::Pipe(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_write(cx, buf),
                None => Poll::Ready(Err(closed_io())),
            },
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SendHalf::Quic(s) => AsyncWrite::poll_flush(Pin::new(s), cx),
            SendHalf::Local(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_flush(cx),
                None => Poll::Ready(Ok(())),
            },
            #[cfg(unix)]
            SendHalf::Unix(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_flush(cx),
                None => Poll::Ready(Ok(())),
            },
            #[cfg(windows)]
            SendHalf::Pipe(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_flush(cx),
                None => Poll::Ready(Ok(())),
            },
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SendHalf::Quic(s) => AsyncWrite::poll_shutdown(Pin::new(s), cx),
            SendHalf::Local(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_shutdown(cx),
                None => Poll::Ready(Ok(())),
            },
            #[cfg(unix)]
            SendHalf::Unix(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_shutdown(cx),
                None => Poll::Ready(Ok(())),
            },
            #[cfg(windows)]
            SendHalf::Pipe(s) => match s.io_mut() {
                Some(io) => Pin::new(io).poll_shutdown(cx),
                None => Poll::Ready(Ok(())),
            },
        }
    }
}

impl AsyncRead for RecvHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            RecvHalf::Quic(r) => AsyncRead::poll_read(Pin::new(r), cx, buf),
            RecvHalf::Local(r) => {
                let reset = r.reset_code();
                match r.io_mut() {
                    Some(io) => match Pin::new(io).poll_read(cx, buf) {
                        Poll::Ready(Ok(())) if buf.filled().is_empty() && reset.is_some() => {
                            Poll::Ready(Err(std::io::Error::other("stream reset by the peer")))
                        }
                        other => other,
                    },
                    None => Poll::Ready(Ok(())),
                }
            }
            #[cfg(unix)]
            RecvHalf::Unix(r) => match r.io_mut() {
                Some(io) => AsyncRead::poll_read(Pin::new(io), cx, buf),
                None => Poll::Ready(Ok(())),
            },
            #[cfg(windows)]
            RecvHalf::Pipe(r) => match r.io_mut() {
                Some(io) => AsyncRead::poll_read(Pin::new(io), cx, buf),
                None => Poll::Ready(Ok(())),
            },
        }
    }
}

fn closed_io() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "stream already closed")
}
