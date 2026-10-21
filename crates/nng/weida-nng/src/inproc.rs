//! The `inproc` transport: a context-scoped namespace and a pair of
//! buffers.
//!
//! `nng_inproc(7)`: messages pass between sockets in one process without
//! touching the kernel, and the transport "accepts but deliberately ignores
//! `RECVMAXSZ`, because peers share an address space"
//! (`docs/research/nanomsg-nng.md` §3, §11). Both halves are here, and the
//! second one is the interesting one: a size limit defends against a peer
//! that chose the number, and an `inproc` peer is a thread of this process
//! that could have allocated the memory itself.
//!
//! - **The namespace belongs to the [`Context`](crate::Context).** NNG's is
//!   process-global because NNG owns the process; a Rust library cannot, so
//!   a name bound in one context is invisible in another. It is
//!   `weida-runtime`'s [`NameRegistry`](weida_runtime::NameRegistry), which
//!   exists for exactly this shape — a set of names, one owner each, a
//!   queue from the dialler to the owner, and a byte budget on the name
//!   ([0010](../../../docs/decisions/0010-local-transport.md) §4.8). The
//!   budget is [`MAX_INPROC_NAME_BYTES`](crate::MAX_INPROC_NAME_BYTES),
//!   derived from `NNG_MAXADDRLEN` so that there is one number rather than
//!   two.
//! - **No kernel and no I/O thread.** A connection is one
//!   [`tokio::io::duplex`] pair, so the SP session above it is the same code
//!   that runs over TCP: the 8-octet header, the pairing check and the
//!   64-bit framing are the protocol's, not the transport's.
//! - **A dial before the bind waits.** NNG's `inproc` dialer retries rather
//!   than failing outright, so a dial to a name nobody holds parks on a
//!   [`Notify`] until it appears — no poll, and a connect nobody ever
//!   answers costs nothing but the waiter.

use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use crate::context::SocketId;
use crate::error::{Error, Result};
use crate::transport::Stream;

/// Bytes in flight on one direction of one `inproc` connection.
///
/// A byte window on the wire between the two halves, deliberately not an
/// option: NNG's `inproc` has no kernel buffer to size, and what an
/// application bounds is the message count of
/// `NNG_OPT_SENDBUF`/`NNG_OPT_RECVBUF` on the pipe. 64 KiB is large enough
/// that a handshake and a typical message never stall on it and small
/// enough that a stalled reader cannot hold a megabyte per connection
/// (`docs/INVARIANTS.md`).
pub const INPROC_BUFFER_BYTES: usize = 64 * 1024;

/// A dial to an `inproc://` name, handed to whoever bound it.
#[derive(Debug)]
pub struct InprocDial {
    /// The socket that dialled, which is what a log line needs to say who
    /// arrived.
    pub from: SocketId,
    /// The accepting side's half of the connection.
    pub stream: Stream,
}

/// One context's `inproc://` namespace.
#[derive(Debug)]
pub struct Inproc {
    names: weida_runtime::NameRegistry<InprocDial>,
    /// Signalled whenever a name is bound, so a dial that arrived first can
    /// wait rather than spin.
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

    /// The name budget of this namespace.
    pub const fn max_name_bytes(&self) -> usize {
        self.names.max_name_bytes()
    }

    /// Binds `name` and returns the binding that holds it.
    ///
    /// `NNG_EADDRINUSE` when somebody already holds the name — one owner
    /// per name, and the second caller is told rather than silently
    /// displacing the first — and `NNG_EADDRINVAL` for a name past the
    /// budget or carrying a control byte.
    pub fn bind(self: &Arc<Self>, name: &str) -> Result<InprocBinding> {
        let incoming = self.names.bind(name).map_err(|e| match e {
            weida_core::Error::AlreadyRegistered => Error::EADDRINUSE(
                format!("inproc://{name} is already bound in this context").into(),
            ),
            other => Error::from(other),
        })?;
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

    /// Dials `name`, handing the other half of the connection to its owner.
    ///
    /// `None` when nobody holds the name, which is a dial to wait on
    /// ([`Inproc::wait_until_bound`]) rather than an error: NNG's dialer
    /// retries.
    pub fn dial(&self, name: &str, from: SocketId) -> Option<Stream> {
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

    /// Waits until somebody binds `name`. Returns at once if one already
    /// holds it.
    pub async fn wait_until_bound(&self, name: &str) {
        loop {
            let waiting: Notified<'_> = self.bound.notified();
            let mut waiting = std::pin::pin!(waiting);
            // `notified()` registers when polled, so a bind between the
            // check and the await must be able to wake this.
            waiting.as_mut().enable();
            if self.is_bound(name) {
                return;
            }
            waiting.await;
        }
    }
}

/// One held `inproc://` name. Dropping it frees the name.
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

    /// The next dial to this name, or `None` once every dialler has let go
    /// of the queue.
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn namespace() -> Arc<Inproc> {
        Arc::new(Inproc::new(crate::MAX_INPROC_NAME_BYTES))
    }

    /// A socket id, which only a context can hand out.
    fn an_id() -> SocketId {
        let ctx = crate::Context::owned(crate::ContextConfig::default()).expect("context");
        ctx.open_socket().expect("slot").id()
    }

    /// Claim: one owner per name, the second bind refused by name, and the
    /// name freed when the binding is dropped.
    #[test]
    fn one_owner_per_name() {
        let inproc = namespace();
        let held = inproc.bind("orders").expect("bind");
        assert!(inproc.is_bound("orders"));
        let err = inproc.bind("orders").unwrap_err();
        assert!(matches!(err, Error::EADDRINUSE(_)), "{err:?}");
        drop(held);
        assert!(!inproc.is_bound("orders"));
        assert!(inproc.bind("orders").is_ok());
    }

    /// Claim: a name past the budget or carrying a control byte is an
    /// address error, checked in the namespace as well as in the URL
    /// parser, so it cannot be reached around.
    #[test]
    fn a_name_is_bounded_and_printable() {
        let inproc = namespace();
        let long = "a".repeat(crate::MAX_INPROC_NAME_BYTES + 1);
        assert!(matches!(inproc.bind(&long), Err(Error::EADDRINVAL(_))));
        assert!(matches!(
            inproc.bind("has\nnewline"),
            Err(Error::EADDRINVAL(_))
        ));
    }

    /// Claim: a dial reaches the owner and the two halves carry bytes, and
    /// a dial to a name nobody holds reaches nobody.
    #[tokio::test]
    async fn a_dial_hands_the_owner_the_other_half() {
        let inproc = namespace();
        assert!(inproc.dial("orders", an_id()).is_none());

        let mut binding = inproc.bind("orders").expect("bind");
        let mut ours = inproc.dial("orders", an_id()).expect("the owner is there");
        let dialled = binding.accept().await.expect("a dial");
        let mut theirs = dialled.stream;

        ours.write_all(b"ping").await.expect("write");
        let mut seen = [0u8; 4];
        theirs.read_exact(&mut seen).await.expect("read");
        assert_eq!(&seen, b"ping");
    }

    /// Claim: a dial that arrives before the bind waits for it rather than
    /// failing, which is what NNG's retrying dialer amounts to.
    #[tokio::test(start_paused = true)]
    async fn a_dial_before_the_bind_waits() {
        let inproc = namespace();
        let waiting = {
            let inproc = Arc::clone(&inproc);
            tokio::spawn(async move {
                inproc.wait_until_bound("late").await;
                inproc.is_bound("late")
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let _binding = inproc.bind("late").expect("bind");
        assert!(waiting.await.expect("task"));
    }
}
