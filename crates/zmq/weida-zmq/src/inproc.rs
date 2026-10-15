//! The `inproc` transport: a context-scoped namespace and a pair of buffers.
//!
//! `zmq_inproc(7)`: it "passes messages via memory directly between threads
//! sharing a single 0MQ context", involves no I/O threads, allows names up to
//! 256 characters, and **since libzmq 4.0 no longer requires bind before
//! connect** (`docs/research/zeromq.md` §12, §13). All four facts are here:
//!
//! - **The namespace belongs to the context.** A name bound in one context is
//!   invisible in another, because "two contexts are two separate ZeroMQ
//!   instances" (§2). It is `weida-runtime`'s [`NameRegistry`][weida_runtime::NameRegistry],
//!   which exists
//!   for exactly this shape — a set of names, one owner each, a queue from
//!   the dialler to the owner, and a byte budget on the name
//!   ([0010](../../../docs/decisions/0010-local-transport.md) §4.8). The
//!   budget is libzmq's 256, which is the same number weida's buses use.
//! - **No I/O threads, no kernel.** A connection is one
//!   [`tokio::io::duplex`] pair, so the session layer above it is the same
//!   code that runs over TCP: the greeting, the NULL handshake and the
//!   framing are the protocol's, not the transport's.
//! - **Connect before bind parks.** A dial to a name nobody holds waits for
//!   the bind instead of failing, which is the 4.0 behaviour. It is a wait on
//!   a [`Notify`], not a poll, so a connect that is never answered costs
//!   nothing.
//! - **The byte buffer is not the high-water mark.** The HWM is a message
//!   count on the [`crate::Pipe`] either side holds; the duplex buffer below
//!   is a byte window on the wire between them, which is also why libzmq
//!   warns that for `inproc` "sender and receiver share buffers; real HWM is
//!   the sum of both sides' configured HWMs" (§8).

use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use crate::context::SocketId;
use crate::error::{Error, Result};
use crate::transport::Stream;

/// Bytes in flight on one direction of one `inproc` connection.
///
/// A byte window on the wire, deliberately not a configurable option:
/// libzmq's `inproc` has no `ZMQ_SNDBUF`/`ZMQ_RCVBUF` (those are kernel
/// buffers and there is no kernel here), and what an application bounds is
/// the message count of `ZMQ_SNDHWM`/`ZMQ_RCVHWM`. 64 KiB is large enough
/// that a ZMTP handshake and a typical message never stall on it and small
/// enough that a stalled reader cannot hold a megabyte per connection.
pub const INPROC_BUFFER_BYTES: usize = 64 * 1024;

/// Who dialled an `inproc://` name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialler {
    /// A socket, by its id in the context.
    Socket(SocketId),
    /// The library's own ZAP dialog, which is a connection and not a socket:
    /// it takes no socket slot under `ZMQ_MAX_SOCKETS`, because a stranger
    /// decides how many of them are in flight.
    ZapDialog,
}

impl std::fmt::Display for Dialler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dialler::Socket(id) => write!(f, "{id}"),
            Dialler::ZapDialog => f.write_str("the ZAP dialog"),
        }
    }
}

impl PartialEq<SocketId> for Dialler {
    /// Whether this is that socket dialling.
    fn eq(&self, other: &SocketId) -> bool {
        *self == Dialler::Socket(*other)
    }
}

/// A dial to an `inproc://` name, handed to whoever bound it.
///
/// The binder's half of the connection plus who dialled, which is what a log
/// line needs to say who arrived.
#[derive(Debug)]
pub struct InprocDial {
    /// Who dialled.
    pub from: Dialler,
    /// The accepting side's half of the connection.
    pub stream: Stream,
}

/// One context's `inproc://` namespace.
///
/// Cloning the context shares it; two contexts never share one, which is the
/// isolation libzmq's context gives.
#[derive(Debug)]
pub struct Inproc {
    names: weida_runtime::NameRegistry<InprocDial>,
    /// Signalled whenever a name is bound, so that a dial that arrived first
    /// can wait rather than spin.
    bound: Notify,
}

impl Inproc {
    /// An empty namespace whose names may be `max_name_bytes` long.
    pub fn new(max_name_bytes: usize) -> Inproc {
        Inproc {
            names: weida_runtime::NameRegistry::new(max_name_bytes),
            bound: Notify::new(),
        }
    }

    /// The name budget of this namespace — libzmq's 256 characters.
    pub const fn max_name_bytes(&self) -> usize {
        self.names.max_name_bytes()
    }

    /// Binds `name` and returns the binding that accepts dials to it.
    ///
    /// Fails with `EADDRINUSE` for a name somebody already holds — one owner
    /// per name, and unlike `ipc://` nobody steals an `inproc://` endpoint
    /// (§8: the `ipc` transport "is not consistent with the `tcp` or
    /// `inproc` transports" precisely here) — and with `EINVAL` for a name
    /// past the budget.
    pub fn bind(self: &Arc<Self>, name: &str) -> Result<InprocBinding> {
        let incoming = self.names.bind(name).map_err(|e| match e {
            weida_core::Error::AlreadyRegistered => Error::EADDRINUSE(
                format!("inproc://{name} is already bound in this context").into(),
            ),
            other => Error::EINVAL(other.to_string().into()),
        })?;
        // A dial that arrived before this bind is parked on exactly this.
        self.bound.notify_waiters();
        Ok(InprocBinding {
            inproc: Arc::clone(self),
            name: name.to_owned(),
            incoming,
        })
    }

    /// Whether anybody holds `name` right now.
    pub fn is_bound(&self, name: &str) -> bool {
        self.names.lookup(name).is_some()
    }

    /// Dials `name`, returning the dialling side's half of the connection.
    ///
    /// `None` when nobody holds the name: the caller decides whether that is
    /// a failure or something to wait for, and for `inproc` it is something
    /// to wait for ([`Inproc::wait_until_bound`]).
    pub fn dial(&self, name: &str, from: SocketId) -> Option<Stream> {
        self.connect(name, Dialler::Socket(from))
    }

    /// [`Inproc::dial`] for the library's own ZAP dialog, which is a
    /// connection and not a socket: no socket slot is taken.
    pub(crate) fn dial_zap(&self, name: &str) -> Option<Stream> {
        self.connect(name, Dialler::ZapDialog)
    }

    fn connect(&self, name: &str, from: Dialler) -> Option<Stream> {
        let owner = self.names.lookup(name)?;
        let (theirs, ours) = tokio::io::duplex(INPROC_BUFFER_BYTES);
        owner
            .send(InprocDial {
                from,
                stream: Stream::inproc(theirs),
            })
            .ok()?;
        Some(Stream::inproc(ours))
    }

    /// Waits until somebody binds `name`.
    ///
    /// This is libzmq 4.0's "no longer requires bind before connect": the
    /// dial is not an error, it is early. Returns at once when the name is
    /// already bound.
    pub async fn wait_until_bound(&self, name: &str) {
        loop {
            let waiting: Notified<'_> = self.bound.notified();
            let mut waiting = std::pin::pin!(waiting);
            // Registered before the check, so a bind between the two cannot
            // be missed — the one race this wait could have.
            waiting.as_mut().enable();
            if self.is_bound(name) {
                return;
            }
            waiting.await;
        }
    }
}

/// One bound `inproc://` name: the queue of dials, and the name itself until
/// this is dropped.
///
/// **Drop is the unbind.** A binding lives in the task that accepts on it, so
/// aborting that task — which is what `zmq_unbind` and closing a socket do —
/// releases the name with no separate bookkeeping to forget.
#[derive(Debug)]
pub struct InprocBinding {
    inproc: Arc<Inproc>,
    name: String,
    incoming: tokio::sync::mpsc::UnboundedReceiver<InprocDial>,
}

impl InprocBinding {
    /// The name this binding holds.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The next socket to dial this name, or `None` once the namespace has
    /// let go of the queue.
    pub async fn accept(&mut self) -> Option<InprocDial> {
        self.incoming.recv().await
    }
}

impl Drop for InprocBinding {
    fn drop(&mut self) {
        self.inproc.names.unbind(&self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::MAX_INPROC_NAME_BYTES;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn namespace() -> Arc<Inproc> {
        Arc::new(Inproc::new(MAX_INPROC_NAME_BYTES))
    }

    fn dialler() -> SocketId {
        // A `SocketId` only labels the dialler in a log line; the context
        // that issues real ones is not needed to test the namespace.
        crate::context::Context::new(crate::context::ContextConfig::default())
            .expect("context")
            .open_socket()
            .expect("slot")
            .id()
    }

    /// Claim: a dial and its bind are the two halves of one duplex stream,
    /// and the bytes cross without a kernel in the way.
    #[tokio::test]
    async fn a_dial_and_its_bind_are_one_connection() {
        let inproc = namespace();
        let mut binding = inproc.bind("orders").expect("bind");
        let from = dialler();
        let mut ours = inproc.dial("orders", from).expect("dialled");
        let mut dial = binding.accept().await.expect("accepted");
        assert_eq!(dial.from, Dialler::Socket(from));

        ours.write_all(b"ping").await.expect("write");
        let mut got = [0u8; 4];
        dial.stream.read_exact(&mut got).await.expect("read");
        assert_eq!(&got, b"ping");

        dial.stream.write_all(b"pong").await.expect("write back");
        let mut back = [0u8; 4];
        ours.read_exact(&mut back).await.expect("read back");
        assert_eq!(&back, b"pong");
    }

    /// Claim: a name has one owner, the second bind is `EADDRINUSE` rather
    /// than a silent steal, and dropping the binding frees the name.
    #[tokio::test]
    async fn one_owner_per_name_and_drop_frees_it() {
        let inproc = namespace();
        let binding = inproc.bind("orders").expect("bind");
        let err = inproc.bind("orders").unwrap_err();
        assert_eq!(err.errno(), "EADDRINUSE", "{err}");

        drop(binding);
        assert!(!inproc.is_bound("orders"));
        let _again = inproc.bind("orders").expect("free after the drop");
    }

    /// Claim: a dial to a name nobody holds waits for the bind — libzmq
    /// 4.0's fix — and completes when it arrives, without polling.
    #[tokio::test]
    async fn a_dial_before_the_bind_waits_for_it() {
        let inproc = namespace();
        assert!(inproc.dial("later", dialler()).is_none());

        let waiting = {
            let inproc = Arc::clone(&inproc);
            tokio::spawn(async move {
                inproc.wait_until_bound("later").await;
                inproc.dial("later", dialler()).is_some()
            })
        };
        // The bind happens after the wait has begun, which is the ordering
        // the enable-then-check exists for.
        tokio::task::yield_now().await;
        let mut binding = inproc.bind("later").expect("bind");
        assert!(waiting.await.expect("the waiter"), "the dial completed");
        assert!(binding.accept().await.is_some());
    }

    /// Claim: an over-long name is refused by the namespace as well as by the
    /// endpoint parser, because the budget is the registry's.
    #[tokio::test]
    async fn the_name_budget_is_the_registrys() {
        let inproc = namespace();
        assert_eq!(inproc.max_name_bytes(), MAX_INPROC_NAME_BYTES);
        let err = inproc
            .bind(&"x".repeat(MAX_INPROC_NAME_BYTES + 1))
            .unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
    }
}
