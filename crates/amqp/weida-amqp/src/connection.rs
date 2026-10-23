//! The connection: version negotiation, the security layers, `open`, the
//! idle timeout and `close`.
//!
//! ```text
//! TCP connect
//!   |
//!   +-- TlsMode::Direct ------> TLS handshake (no header at all)
//!   |
//!   +-- TlsMode::Layered -----> AMQP %d2 1.0.0  both ways, TLS handshake
//!   |
//!   +-- Sasl != None ---------> AMQP %d3 1.0.0  both ways
//!   |                           sasl-mechanisms / sasl-init / [challenge,
//!   |                           response]* / sasl-outcome
//!   v
//! AMQP %d0 1.0.0  both ways
//! open  ---------------------->
//!       <---------------------- open
//! (empty frame every idle-time-out/2 while there is nothing else to send)
//! close ---------------------->                 the last thing ever written
//!       <---------------------- close
//! ```
//!
//! # The four ways negotiation fails, kept apart
//!
//! Part 2 §2.2 puts them all in the same eight octets, and a client that
//! folded them would retry the wrong thing:
//!
//! 1. **A different protocol-id.** "Protocol-id is not part of the version,
//!    so 'highest supported version' does not apply to it": a server
//!    requiring SASL answers a `%d0` request with `%d3` and closes. That is a
//!    *demand*, and this client reports it as
//!    [`Error::SecurityLayerRequired`] rather than retrying blind, because a
//!    client with no credentials configured cannot satisfy it and looping
//!    would only hide that.
//! 2. **A different version.** `AMQP` + `0 0 9 1` is an AMQP 0-9-1 server on
//!    the shared port 5672 — [`Error::VersionMismatch`].
//! 3. **Octets that are not a header at all** —
//!    [`Error::BadProtocolHeader`].
//! 4. **Nothing at all.** The protocol gives no deadline for the answering
//!    header, so this client applies its own:
//!    [`Error::HandshakeTimeout`].
//!
//! # The idle timeout is two independent clocks
//!
//! `open.idle-time-out` states "the maximum period the *sender of that open*
//! wants between frames arriving from its partner", and each peer has its own
//! (Part 2 §2.4.5). So there are two:
//!
//! * **Ours to keep**: the peer's advertised value. This client sends an
//!   empty frame every half of it when it has nothing else to send, which is
//!   the same halving the specification recommends for the advertisement and
//!   for the same reason — a keep-alive that arrives exactly at the deadline
//!   arrives too late. The period has a floor ([`MIN_KEEPALIVE`]): the peer
//!   chooses the number, and a peer that asks for a frame every millisecond
//!   is not owed a thousand writes a second.
//! * **Ours to enforce**: [`ConnectionOptions::idle_time_out`]. On expiry
//!   this client closes with `amqp:connection:forced` and an explanation,
//!   which is what Part 2 §2.4.5 says a peer SHOULD do, and only then drops
//!   the transport.
//!
//! # One task, and it never waits for a peer or for an application
//!
//! Every session and link of a connection is served by one driver task, so
//! anything that task waits for, they all wait for. It therefore waits for
//! three things only: the socket's readiness, its timers and its inputs.
//!
//! * **Writing never blocks it.** Every frame is queued in an outbox
//!   and goes out as the socket takes it,
//!   inside the same `select!` that reads, so a peer that has stopped reading
//!   cannot stop the connection from reading, answering, timing out or
//!   closing. A stall clock restarts whenever the socket takes anything; no
//!   progress for [`ConnectionOptions::write_timeout`] fails the connection
//!   with [`Error::WriteTimeout`]. Past
//!   [`OUTBOX_HIGH_WATER`](crate::transport::OUTBOX_HIGH_WATER) queued octets
//!   the driver stops taking frames from local senders, and it stops reading
//!   the peer once the *answers* to the peer's own frames are what is past it:
//!   the application's frames do not count against reading, or a peer
//!   streaming in both directions would stop both ends reading at once. The
//!   two together bound what a peer that never reads can make it hold.
//! * **No application is waited for.** What a link or a session tells its
//!   handle goes into a queue the driver never waits on
//!   (`crate::queue`), and what bounds those queues is where their
//!   contents come from: a delivery is queued only against credit the
//!   application granted itself, and everything else is state the handle can
//!   also ask for.
//! * **Reading is inline.** The reader is polled by the same `select!`, so it
//!   is dropped with the driver: a peer that goes silent cannot leave a
//!   half-open socket and a task behind.
//!
//! Ending the connection is bounded the same way: what is still queued —
//! `close` above all — gets [`ConnectionOptions::close_budget`] to leave, and
//! so does closing the transport.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::time::Instant;
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::message::Reassembly;
use weida_amqp_codec::performative::{
    Attach, Begin, Close, Detach, Disposition, End, Flow, Open, Performative, Transfer,
};
use weida_amqp_codec::protocol_header::{ProtocolHeader, ProtocolId};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, Role};
use weida_runtime::Exec;

use crate::credit::Credit;
use crate::error::{Condition, Error, Result};
use crate::link::{self, Incoming, Link, LinkEvent, LinkOptions, LinkState, Links};
use crate::options::{ConnectionOptions, Sasl, TlsMode, multiple};
use crate::sasl;
use crate::session::{
    self, Entry, Session, SessionEvent, SessionOptions, SessionState, Shared, Table,
};
use crate::settlement::{DEFAULT_MAX_UNSETTLED, Outcome, Settlement, Unsettled};
use crate::transport::{self, FrameReader, FrameWriter, Outbox, Step, Wire};
use crate::window::Windows;

/// How many outgoing frames may be queued for the driver before a sender
/// waits.
///
/// A local bound: the protocol has no queue here at all, and an unbounded
/// channel would let an application outrun the transport without ever being
/// told. Backpressure on the sender is the honest answer, and it is what the
/// session window and link credit do one layer up.
pub const OUTGOING_QUEUE: usize = 64;

/// The least period between the empty frames this client sends to keep a peer's
/// idle threshold from expiring, whatever the peer asked for.
///
/// **A bound of ours.** The period is half of the peer's `open.idle-time-out`,
/// and the peer chooses that number: without a floor a value of 1 millisecond
/// makes this client write and flush every half millisecond for as long as the
/// connection lives. 50 ms is twenty frames a second, which a peer that wants
/// them can have, and below the 100 ms period a 200 ms threshold asks for.
pub const MIN_KEEPALIVE: Duration = Duration::from_millis(50);

/// What the peer said in its `open`, owned.
///
/// A borrowed `Open` does not outlive the frame it arrived in, and every one
/// of these fields is consulted for the life of the connection: the frame
/// size bounds every write, the channel maximum bounds the session table, the
/// idle timeout drives a timer, and the capabilities decide what may be used
/// at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteOpen {
    /// The peer's container name.
    pub container_id: String,
    /// The host the peer thinks it is.
    pub hostname: Option<String>,
    /// The largest frame the peer will accept.
    pub max_frame_size: u32,
    /// The highest channel number the peer will accept.
    pub channel_max: u16,
    /// Milliseconds the peer wants between frames from us; `None` means it
    /// asked for no timeout.
    pub idle_time_out: Option<u32>,
    /// Capabilities the peer offers.
    pub offered_capabilities: Vec<String>,
    /// Capabilities the peer wants. A peer MUST NOT use one it did not list.
    pub desired_capabilities: Vec<String>,
}

impl RemoteOpen {
    fn from_codec(open: &Open<'_>) -> Self {
        Self {
            container_id: open.container_id.to_owned(),
            hostname: open.hostname.map(str::to_owned),
            max_frame_size: open.max_frame_size,
            channel_max: open.channel_max,
            // Zero equals unset (Part 2 §2.4.5), so it is folded into `None`
            // here rather than left to every caller to remember.
            idle_time_out: open.idle_time_out.filter(|ms| *ms != 0),
            offered_capabilities: open
                .offered_capabilities
                .iter()
                .map(str::to_owned)
                .collect(),
            desired_capabilities: open
                .desired_capabilities
                .iter()
                .map(str::to_owned)
                .collect(),
        }
    }

    /// Whether the peer offered `capability`.
    #[must_use]
    pub fn offers(&self, capability: &str) -> bool {
        self.offered_capabilities.iter().any(|c| c == capability)
    }

    /// The negotiated frame size: the smaller of what we will accept and what
    /// the peer will accept, never below `MIN-MAX-FRAME-SIZE`.
    ///
    /// Both directions matter and they are different numbers. This is the one
    /// that bounds what *we* may write, because "a peer MUST NOT send frames
    /// larger than its partner can handle" (Part 2 §2.7.1).
    #[must_use]
    pub fn outgoing_frame_size(&self) -> u32 {
        self.max_frame_size.max(MIN_MAX_FRAME_SIZE)
    }
}

/// Where a connection is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// Both `open` frames have been exchanged.
    Open,
    /// We sent `close` and are waiting for the peer's.
    Closing,
    /// Done. `Some` where either side gave a condition.
    Closed(Option<Condition>),
    /// The connection failed; the string is the error as it was reported.
    Failed(String),
}

impl State {
    /// Whether anything further can be sent.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Open)
    }

    /// Why nothing further can be sent, as the error a caller gets.
    ///
    /// `None` while the connection is usable. The peer's own condition is
    /// kept rather than replaced by "the connection is gone": the condition
    /// is the only explanation the protocol carries, and a caller that asked
    /// for a session on a connection the peer refused wants to read
    /// `amqp:resource-limit-exceeded`, not a synonym for silence.
    #[must_use]
    pub fn refusal(&self) -> Option<Error> {
        match self {
            Self::Open => None,
            Self::Closing | Self::Closed(None) => Some(Error::ConnectionGone),
            Self::Closed(Some(condition)) => Some(Error::Closed(Some(condition.clone()))),
            // The reason a connection failed went to whoever was waiting on
            // the operation that failed; it stays readable through
            // [`Connection::state`]. A caller arriving afterwards is told the
            // connection is gone, which is all its own call can act on.
            Self::Failed(_) => Some(Error::ConnectionGone),
        }
    }
}

/// What the driver accepts from a handle.
enum Command {
    /// Begin a session on the lowest free outgoing channel, and report the
    /// handle once the answering `begin` has tied the two channel numberings
    /// together.
    Begin {
        options: SessionOptions,
        reply: oneshot::Sender<Result<Session>>,
    },
    /// `close`, with an optional condition, and a channel to report on.
    Close {
        error: Option<Condition>,
        done: oneshot::Sender<Result<()>>,
    },
}

/// An AMQP 1.0 connection.
///
/// Cloning shares the connection: every clone speaks to the same driver task
/// and sees the same state. The connection lives as long as any handle does —
/// a clone, a [`Session`] begun on it, a [`Link`] attached on that — and ends
/// when the last one is dropped: what was queued is written, `close` follows,
/// and then the transport is closed. A session or a link therefore goes on
/// working after the `Connection` it came from has left scope.
#[derive(Clone, Debug)]
pub struct Connection {
    inner: Arc<Inner>,
}

/// What a session asks the connection driver to do.
///
/// Sessions do not own the transport: everything they send goes through the
/// driver, which is what keeps `close` the last thing ever written and what
/// lets the driver refuse a frame once it has sent one. Attaching is a
/// request rather than a frame because the driver owns the handle space and
/// the link table.
#[derive(Debug)]
pub(crate) enum Outbound {
    /// A whole frame, channel already in its header.
    Frame(Vec<u8>),
    /// Attach a link on the lowest free handle of this session.
    Attach {
        /// The session's outgoing channel.
        channel: u16,
        /// Boxed because `LinkOptions` carries two termini and this enum's
        /// other variant is one `Vec`.
        options: Box<LinkOptions>,
        /// Answered once the answering `attach` has arrived.
        reply: oneshot::Sender<Result<Link>>,
    },
}

#[derive(Debug)]
struct Inner {
    exec: Exec,
    options: Arc<ConnectionOptions>,
    remote: RemoteOpen,
    commands: mpsc::Sender<Command>,
    state: watch::Receiver<State>,
}

/// What keeps the connection alive for a handle that is not a
/// [`Connection`]: a sender into the driver's command queue, whose last copy
/// being dropped is what ends the connection. A [`Session`] and a [`Link`]
/// each hold one, so that `Connection::connect(..).await?.begin(..).await?`
/// does not close the connection under the session it returns.
///
/// Held by the driver only weakly, because a strong copy there would keep the
/// queue open for ever and no connection whose handles had all gone would
/// end. `None` where there is no copy left to make, which is a handle nobody
/// can be waiting on.
#[derive(Debug)]
pub(crate) struct Lease {
    /// Held for what dropping it does, which is nothing until it is the last.
    _commands: Option<mpsc::Sender<Command>>,
}

impl Lease {
    fn of(commands: &mpsc::WeakSender<Command>) -> Self {
        Self {
            _commands: commands.upgrade(),
        }
    }
}

impl Connection {
    /// Opens a connection to `host:port` over plain TCP.
    ///
    /// Refuses [`TlsMode::Layered`] and [`TlsMode::Direct`], because those
    /// need the caller's `rustls::ClientConfig` and silently dropping TLS
    /// from a configuration that asked for it is exactly the thing 0013 §4.4
    /// item 4 forbids.
    pub async fn connect(
        exec: &Exec,
        host: &str,
        port: u16,
        options: ConnectionOptions,
    ) -> Result<Self> {
        options.validate()?;
        if !matches!(options.tls, TlsMode::None) {
            return Err(Error::Configuration(
                "this configuration asks for TLS; use Connection::connect_tls, \
                 which takes the rustls::ClientConfig the trust decision needs"
                    .into(),
            ));
        }
        let stream = transport::connect(exec, host, port, &options).await?;
        Self::establish(exec, Wire::new(stream), options, None).await
    }

    /// Opens a connection to `host:port` with TLS, reached the way
    /// [`ConnectionOptions::tls`] says.
    ///
    /// `config` is the caller's: which certificates are valid is the
    /// application's decision. The SNI name is
    /// [`ConnectionOptions::tls_server_name`] or, failing that, `host`.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        exec: &Exec,
        host: &str,
        port: u16,
        options: ConnectionOptions,
        config: Arc<tokio_rustls::rustls::ClientConfig>,
    ) -> Result<Self> {
        options.validate()?;
        let server_name = options
            .tls_server_name
            .clone()
            .unwrap_or_else(|| host.to_owned());
        let stream = transport::connect(exec, host, port, &options).await?;
        let mut wire = Wire::new(stream);

        match options.tls {
            TlsMode::None => {
                return Err(Error::Configuration(
                    "connect_tls with TlsMode::None: set TlsMode::Layered or \
                     TlsMode::Direct, or use Connection::connect"
                        .into(),
                ));
            }
            TlsMode::Direct => {}
            TlsMode::Layered => {
                // `AMQP %d2 1.0.0` both ways, in the clear, before the
                // handshake. A server that answers with anything else has
                // refused the layering, and that is the same demand a `%d3`
                // answer is.
                step(
                    exec,
                    &options,
                    "sending the TLS protocol header",
                    wire.write_protocol_header(ProtocolHeader::TLS),
                )
                .await?;
                let answer = step(
                    exec,
                    &options,
                    "the answering TLS protocol header",
                    wire.read_protocol_header(),
                )
                .await?;
                check_header(answer, ProtocolId::Tls)?;
            }
        }

        // The handshake is the first thing after TCP connect in the direct
        // form and the first thing after the header in the layered one, and
        // a server that accepts the connection and then says nothing, or
        // drips a `ServerHello`, would otherwise hold the caller for as long
        // as the OS allows.
        let stream = step(
            exec,
            &options,
            "the TLS handshake",
            transport::upgrade_tls(wire.into_stream(), config, &server_name),
        )
        .await?;
        Self::establish(exec, Wire::new(stream), options, None).await
    }

    /// Runs the handshake on an already-established stream and starts the
    /// driver.
    ///
    /// Separate from the constructors so that a test can hand in a socket
    /// pair, and so that the TLS and non-TLS paths share one sequence rather
    /// than two that can drift.
    pub async fn establish(
        exec: &Exec,
        mut wire: Wire,
        options: ConnectionOptions,
        _reserved: Option<()>,
    ) -> Result<Self> {
        options.validate()?;
        let options = Arc::new(options);

        if options.sasl != Sasl::None {
            step(
                exec,
                &options,
                "sending the SASL protocol header",
                wire.write_protocol_header(ProtocolHeader::SASL),
            )
            .await?;
            let answer = step(
                exec,
                &options,
                "the answering SASL protocol header",
                wire.read_protocol_header(),
            )
            .await?;
            check_header(answer, ProtocolId::Sasl)?;
            sasl::dialog(exec, &mut wire, &options).await?;
        }

        // `AMQP %d0 1.0.0`, either directly or after the layer above.
        step(
            exec,
            &options,
            "sending the AMQP protocol header",
            wire.write_protocol_header(ProtocolHeader::AMQP),
        )
        .await?;
        let answer = step(
            exec,
            &options,
            "the answering AMQP protocol header",
            wire.read_protocol_header(),
        )
        .await?;
        check_header(answer, ProtocolId::Amqp)?;

        // `open` on channel 0, as the first frame. Pipelined in the sense
        // Part 2 §2.4.2 permits: sent without waiting for the peer's, and
        // staying inside what every implementation must support until the
        // peer's `open` says otherwise.
        let mut out = Vec::new();
        let local_open = local_open(&options);
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(local_open).encode(body)
        })?;
        wire.queue().extend_from_slice(&out);
        step(exec, &options, "sending open", wire.flush()).await?;

        let remote = {
            let frame = step(exec, &options, "the peer's open", wire.read_frame()).await?;
            if frame.header.channel != 0 {
                return Err(Error::Local(Condition::described(
                    condition::CONNECTION_FRAMING_ERROR,
                    format!(
                        "open arrived on channel {}, and the first frame MUST be \
                         on channel 0",
                        frame.header.channel
                    ),
                )));
            }
            let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT)?;
            match performative {
                Performative::Open(open) => RemoteOpen::from_codec(&open),
                Performative::Close(Close { error }) => {
                    return Err(Error::Closed(error.as_ref().map(Condition::from_codec)));
                }
                other => {
                    return Err(Error::Local(Condition::described(
                        condition::ILLEGAL_STATE,
                        format!("expected open, the peer sent {}", other.name()),
                    )));
                }
            }
        };

        wire.negotiated(options.max_frame_size);
        let (mut reader, mut writer) = wire.split();
        reader.negotiated(options.max_frame_size);
        writer.negotiated(remote.outgoing_frame_size());

        let (commands, rx) = mpsc::channel(OUTGOING_QUEUE);
        let (outbound, outbound_rx) = mpsc::channel(OUTGOING_QUEUE);
        let (state_tx, state) = watch::channel(State::Open);
        let driver = Driver {
            exec: exec.clone(),
            options: Arc::clone(&options),
            remote: remote.clone(),
            rx,
            commands: commands.downgrade(),
            outbound_rx,
            outbound: outbound.clone(),
            state: state_tx,
            sessions: Table::default(),
        };
        exec.spawn(driver.run(reader, writer));

        Ok(Self {
            inner: Arc::new(Inner {
                exec: exec.clone(),
                options,
                remote,
                commands,
                state,
            }),
        })
    }

    /// Begins a session on the lowest free outgoing channel.
    ///
    /// Returns once the answering `begin` has arrived, because until then
    /// there is no incoming channel to route the peer's frames by and
    /// therefore nothing a caller could do with the handle. The channel is
    /// the lowest free one, as Part 2 §2.5.1 recommends, and the bound on how
    /// many sessions there can be is the *peer's* `channel-max` — the number
    /// that says which channels it will accept.
    pub async fn begin(&self, options: SessionOptions) -> Result<Session> {
        if let Some(refusal) = self.state().refusal() {
            return Err(refusal);
        }
        let (reply, wait) = oneshot::channel();
        if self
            .inner
            .commands
            .send(Command::Begin { options, reply })
            .await
            .is_err()
        {
            return Err(self.gone().await);
        }
        match self
            .inner
            .exec
            .within(self.inner.options.handshake_timeout, wait)
            .await
        {
            Some(Ok(result)) => result,
            Some(Err(_)) => Err(self.gone().await),
            None => Err(Error::HandshakeTimeout {
                step: "the answering begin",
            }),
        }
    }

    /// The error for an operation that found the driver gone.
    ///
    /// The peer's own condition where it gave one: a command that raced the
    /// peer's `close` is refused for the same reason one that came a moment
    /// later is, and a caller that asked for a session on a connection the
    /// peer refused wants to read `amqp:resource-limit-exceeded`, not a
    /// synonym for silence. The driver publishes the final state a moment
    /// after it stops taking commands, so this waits for it, within the
    /// close budget.
    async fn gone(&self) -> Error {
        self.inner
            .exec
            .within(self.inner.options.close_budget, self.closed())
            .await
            .and_then(|state| state.refusal())
            .unwrap_or(Error::ConnectionGone)
    }

    /// What the peer said in its `open`.
    #[must_use]
    pub fn remote(&self) -> &RemoteOpen {
        &self.inner.remote
    }

    /// What this client said in its `open`.
    #[must_use]
    pub fn options(&self) -> &ConnectionOptions {
        &self.inner.options
    }

    /// The reactor this connection runs on.
    #[must_use]
    pub fn exec(&self) -> &Exec {
        &self.inner.exec
    }

    /// Where the connection is now.
    #[must_use]
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }

    /// Waits for the connection to leave [`State::Open`].
    pub async fn closed(&self) -> State {
        let mut state = self.inner.state.clone();
        loop {
            if !state.borrow().is_usable() {
                return state.borrow().clone();
            }
            if state.changed().await.is_err() {
                return State::Failed("the connection driver stopped".into());
            }
        }
    }

    /// Sends `close`, waits for the peer's, and drops the transport.
    ///
    /// Part 2 §2.4.3: `close` MUST be the last thing ever written, the sender
    /// SHOULD keep reading until the partner's `close` arrives, and SHOULD
    /// apply a timeout before dropping the transport. The timeout is
    /// [`ConnectionOptions::close_budget`], and it is finite for the same
    /// reason every close budget in this repository is: when it runs out the
    /// driver drops the transport whether or not the peer answered, and
    /// whether or not the peer was reading.
    ///
    /// Idempotent: closing a closed connection is `Ok(())`, because a caller
    /// that races its own shutdown should not have to hold a lock to be
    /// correct.
    pub async fn close(&self) -> Result<()> {
        self.close_with(None).await
    }

    /// Sends `close(error=...)`.
    pub async fn close_with(&self, error: Option<Condition>) -> Result<()> {
        if !self.state().is_usable() {
            return Ok(());
        }
        let (done, wait) = oneshot::channel();
        if self
            .inner
            .commands
            .send(Command::Close { error, done })
            .await
            .is_err()
        {
            return Ok(());
        }
        // The driver enforces the same budget itself, so this is the caller's
        // bound on the wait and not the only one.
        match self
            .inner
            .exec
            .within(self.inner.options.close_budget, wait)
            .await
        {
            Some(Ok(result)) => result,
            // The driver dropped the channel, which means it finished.
            Some(Err(_)) => Ok(()),
            None => Ok(()),
        }
    }
}

/// Builds this client's `open`.
fn local_open(options: &ConnectionOptions) -> Open<'_> {
    let mut open = Open::new(&options.container_id);
    open.hostname = options.hostname.as_deref();
    open.max_frame_size = options.max_frame_size;
    open.channel_max = options.channel_max;
    open.idle_time_out = options.advertised_idle_time_out();
    open.offered_capabilities = multiple(&options.offered_capabilities);
    open.desired_capabilities = multiple(&options.desired_capabilities);
    open
}

/// Bounds one handshake step on wall-clock time.
///
/// Every step needs it and none of them has a deadline in the protocol: a
/// server that accepts the TCP connection and then says nothing would
/// otherwise hold the client for as long as the OS allows.
async fn step<T, F>(
    exec: &Exec,
    options: &ConnectionOptions,
    name: &'static str,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match exec.within(options.handshake_timeout, future).await {
        Some(result) => result,
        None => Err(Error::HandshakeTimeout { step: name }),
    }
}

/// Reads the peer's answering header as the demand it is.
fn check_header(answer: ProtocolHeader, requested: ProtocolId) -> Result<()> {
    if answer.id != requested {
        return Err(Error::SecurityLayerRequired {
            requested,
            offered: answer.id,
        });
    }
    if !answer.version.is_1_0_0() {
        return Err(Error::VersionMismatch {
            offered: answer.version,
        });
    }
    Ok(())
}

/// The period between keep-alive frames for a peer that asked for
/// `idle_time_out` milliseconds: half of it, so that a frame is never exactly
/// at the deadline, and never below [`MIN_KEEPALIVE`]. `None` where the peer
/// asked for no timeout.
fn keepalive_period(idle_time_out: Option<u32>) -> Option<Duration> {
    idle_time_out
        .map(|ms| Duration::from_millis(u64::from(ms)) / 2)
        .filter(|period| !period.is_zero())
        .map(|period| period.max(MIN_KEEPALIVE))
}

/// The task that owns the wire once the handshake is done.
struct Driver {
    exec: Exec,
    options: Arc<ConnectionOptions>,
    remote: RemoteOpen,
    rx: mpsc::Receiver<Command>,
    /// Held weakly, for the [`Lease`] a session or a link is handed.
    commands: mpsc::WeakSender<Command>,
    outbound_rx: mpsc::Receiver<Outbound>,
    /// Kept so that a `Session` handed to a caller can be given a sender
    /// without the driver having to clone one out of thin air.
    outbound: mpsc::Sender<Outbound>,
    state: watch::Sender<State>,
    sessions: Table,
}

impl Driver {
    async fn run(mut self, mut reader: FrameReader, mut writer: FrameWriter) {
        let write_timeout = self.options.write_timeout;
        let close_budget = self.options.close_budget;
        // Ours to send: an empty frame at half the interval the peer asked
        // for. Ours to enforce: our own threshold, measured from the last
        // frame that arrived.
        let keepalive = keepalive_period(self.remote.idle_time_out);
        let deadline = self.options.idle_time_out;

        let mut out = Outbox::new(writer.max_frame_size());
        let mut closing = false;
        let mut close_done: Option<oneshot::Sender<Result<()>>> = None;

        // Four clocks, each one a `Sleep` that outlives a turn of the loop and
        // is moved only by the event it measures. A `Sleep` built inside the
        // loop would restart whenever any *other* branch completed, so under
        // traffic a keep-alive would never be sent, an idle threshold would
        // never be reached and a stalled write would never be noticed.
        //
        // * `tick` — the last time this side wrote anything — moves when the
        //   socket takes octets;
        // * `expiry` — the last frame that arrived — moves when one does, and
        //   nowhere else, so a connection whose peer has silently gone is
        //   timed out even while this side keeps sending;
        // * `stall` runs only while the socket owes something, and moves when
        //   it takes something, so it measures *no progress* and not a slow
        //   reader;
        // * `close_by` runs from the moment `close` is queued.
        //
        // The guards on the arms disable them when they do not apply, so an
        // unarmed `Sleep` is never awaited.
        let started = Instant::now();
        let mut tick = std::pin::pin!(self.exec.sleep(Duration::ZERO));
        let mut expiry = std::pin::pin!(self.exec.sleep(Duration::ZERO));
        let mut stall = std::pin::pin!(self.exec.sleep(write_timeout));
        let mut close_by = std::pin::pin!(self.exec.sleep(close_budget));
        if let Some(period) = keepalive {
            tick.as_mut().reset(started + period);
        }
        if let Some(period) = deadline {
            expiry.as_mut().reset(started + period);
        }
        let mut owed_before = false;

        // The state the connection ended in, and whether what is queued
        // should still be given the chance to leave: true where a `close` is
        // in the queue behind it.
        let (outcome, flush): (State, bool) = loop {
            // Output appearing after a quiet spell starts the stall clock.
            let owes = out.owes();
            if owes && !owed_before {
                stall.as_mut().reset(Instant::now() + write_timeout);
            }
            owed_before = owes;

            // Not `biased`: with any one arm first, a source that is always
            // ready would starve the rest — commands the read, the read the
            // timers — and what the driver must never be is starved by one
            // peer or one application.
            tokio::select! {
                // The socket takes some of what is queued, or, once it has
                // all of it, a TLS session pushes out the last record. One
                // step either way, so that this arm is also the progress the
                // stall clock measures.
                progress = writer.step(out.pending()), if out.owes() => match progress {
                    Ok(Step::Wrote(taken)) => {
                        out.took(taken);
                        let now = Instant::now();
                        stall.as_mut().reset(now + write_timeout);
                        if let Some(period) = keepalive {
                            tick.as_mut().reset(now + period);
                        }
                    }
                    Ok(Step::Flushed) => {
                        out.flushed();
                        stall.as_mut().reset(Instant::now() + write_timeout);
                    }
                    Err(error) => break (State::Failed(error.to_string()), false),
                },

                // Output has been waiting and the socket has taken nothing.
                () = stall.as_mut(), if out.owes() => {
                    break (
                        State::Failed(Error::WriteTimeout { after: write_timeout }.to_string()),
                        false,
                    );
                }

                // `close` was queued and the peer has not answered inside the
                // budget. The caller's own wait is bounded by the same number;
                // this is the driver's, so the transport is dropped whether
                // or not anybody is still waiting.
                () = close_by.as_mut(), if closing => {
                    if let Some(done) = close_done.take() {
                        let _ = done.send(Ok(()));
                    }
                    break (State::Closed(None), false);
                }

                () = expiry.as_mut(), if deadline.is_some() => {
                    let after_ms = u32::try_from(
                        deadline.unwrap_or_default().as_millis()
                    ).unwrap_or(u32::MAX);
                    // SHOULD close with an error explaining why, and MAY then
                    // drop the socket.
                    if !closing {
                        let _ = queue_close(
                            &mut out,
                            Some(&Condition::described(
                                condition::CONNECTION_FORCED,
                                format!("no frame within the {after_ms} ms idle threshold"),
                            )),
                        );
                    }
                    break (State::Failed(Error::IdleTimeout { after_ms }.to_string()), true);
                }

                // Read inline rather than through a forwarding task:
                // `next_frame` is cancel-safe, so a `select!` that drops it
                // keeps every octet it had buffered, and the reader goes when
                // the driver does. Not read while what is queued as an answer
                // to the peer's own frames is past the high-water mark, and
                // the application's frames do not count: a peer streaming in
                // both directions would otherwise stop reading for as long as
                // the socket was slower than the application.
                incoming = reader.next_frame(), if !out.answers_congested() => match incoming {
                    Ok(frame) => {
                        // The one event the read threshold measures from.
                        if let Some(period) = deadline {
                            expiry.as_mut().reset(Instant::now() + period);
                        }
                        // Whatever this frame makes the driver queue is an
                        // answer to the peer, counted apart from what the
                        // application queued.
                        let answers_from = out.end();
                        let handled = self.handle(frame, &mut out, closing);
                        out.answered(answers_from);
                        match handled {
                            Ok(None) => {}
                            Ok(Some(state)) => {
                                if let Some(done) = close_done.take() {
                                    let _ = done.send(Ok(()));
                                }
                                break (state, true);
                            }
                            Err(error) => {
                                if let Some(done) = close_done.take() {
                                    let _ = done.send(Err(Error::ConnectionGone));
                                }
                                // Part 2 §2.4.3: tell the peer why, where the
                                // fault was in what it sent.
                                let told = !closing && tell_why(&mut out, &error);
                                break (State::Failed(error.to_string()), told);
                            }
                        }
                    }
                    Err(error) => {
                        if let Some(done) = close_done.take() {
                            // The peer went away without answering, which is
                            // the case the close budget exists for.
                            let _ = done.send(Ok(()));
                            break (State::Closed(None), false);
                        }
                        let told = !closing && tell_why(&mut out, &error);
                        break (State::Failed(error.to_string()), told);
                    }
                },

                // An empty frame: a header, no body, no meaning beyond
                // liveness. Channel 0, which the specification says it SHOULD
                // be and MUST be before `open` has been received. Only while
                // the socket owes nothing: queued output is the heartbeat.
                () = tick.as_mut(), if keepalive.is_some() && !closing && !out.owes() => {
                    out.push(&frame::empty(0));
                }

                command = self.rx.recv() => match command {
                    Some(Command::Begin { options, reply }) => {
                        if closing {
                            let _ = reply.send(Err(Error::ConnectionGone));
                            continue;
                        }
                        self.begin(options, reply, &mut out);
                    }

                    Some(Command::Close { error, done }) => {
                        if closing {
                            let _ = done.send(Ok(()));
                            continue;
                        }
                        // Everything already handed to the driver is written
                        // first. "`close` MUST be the last frame ever
                        // written" makes it *last*; it is not a licence to
                        // drop a transfer a sender had already completed
                        // (Part 2 §2.4.3).
                        self.flush_outbound(&mut out);
                        if let Err(error) = queue_close(&mut out, error.as_ref()) {
                            let _ = done.send(Err(error));
                            break (State::Closed(None), true);
                        }
                        closing = true;
                        self.state.send_replace(State::Closing);
                        close_by.as_mut().reset(Instant::now() + close_budget);
                        close_done = Some(done);
                    }

                    // Every handle is gone and nobody asked for a close.
                    // Part 2 §2.4.3 wants one written anyway: a connection
                    // dropped without it leaves the peer to discover the
                    // loss by timeout.
                    None => {
                        if !closing {
                            self.flush_outbound(&mut out);
                            let _ = queue_close(&mut out, None);
                        }
                        break (State::Closed(None), true);
                    }
                },

                queued = self.outbound_rx.recv(), if !out.congested() => match queued {
                    Some(first) => {
                        if let Err(error) = self.take_outbound(first, &mut out, closing) {
                            break (State::Failed(error.to_string()), false);
                        }
                    }
                    // Only the driver's own clone is left, which cannot
                    // happen while the driver runs.
                    None => continue,
                },
            }
        };

        // "Sessions also end automatically when the connection closes or is
        // interrupted" (Part 2 §2.5.2). Telling each one is what lets a
        // caller holding a `Session` find out, rather than waiting on a
        // channel nobody will ever send on.
        let ended = match &outcome {
            State::Closed(condition) => condition.clone(),
            State::Failed(why) => Some(Condition::described(
                condition::CONNECTION_FORCED,
                why.clone(),
            )),
            _ => None,
        };
        for entry in self.sessions.drain() {
            *entry.shared.state.lock().expect("not poisoned") = SessionState::Ended(ended.clone());
            end_session(entry, ended.clone());
        }

        // What is still queued — `close` above all — gets the budget to leave
        // and no more: a peer that has stopped reading is the case this
        // bounds, and there is nothing to wait for if the write itself
        // failed.
        if flush {
            let _ = self
                .exec
                .within(close_budget, transport::drain(&mut writer, &mut out))
                .await;
        }
        // A failure is known now and does not wait on the transport closing.
        let failed = matches!(outcome, State::Failed(_));
        if failed {
            self.state.send_replace(outcome.clone());
        }
        // Closing the transport can wait on a peer as well — a TLS session
        // writes its closing alert first — so it is given what a close is
        // given.
        let _ = self.exec.within(close_budget, writer.shutdown()).await;
        if !failed {
            self.state.send_replace(outcome);
        }
    }

    /// Moves what is already waiting in the senders' queue into the outbox,
    /// without waiting for more.
    ///
    /// Called before `close` so that a frame a caller had already handed over
    /// is not overtaken by it. Only what is queued *now* is taken: a frame
    /// offered after the close was asked for is still dropped, because by then
    /// `close` is the last frame and that is the whole rule.
    fn flush_outbound(&mut self, out: &mut Outbox) {
        while let Ok(item) = self.outbound_rx.try_recv() {
            match item {
                Outbound::Frame(bytes) => out.push(&bytes),
                Outbound::Attach { reply, .. } => {
                    let _ = reply.send(Err(Error::ConnectionGone));
                }
            }
        }
    }

    /// One thing a session or link handed over, and whatever else is already
    /// waiting behind it, all into the outbox: frames queued in one turn leave
    /// in one write.
    fn take_outbound(&mut self, first: Outbound, out: &mut Outbox, closing: bool) -> Result<()> {
        let mut next = Some(first);
        while let Some(item) = next.take() {
            match item {
                // `close` MUST be the last thing ever written, so a session
                // frame that raced the close is dropped rather than written
                // after it.
                Outbound::Frame(bytes) => {
                    if !closing {
                        out.push(&bytes);
                    }
                }
                Outbound::Attach {
                    channel,
                    options,
                    reply,
                } => {
                    if closing {
                        let _ = reply.send(Err(Error::ConnectionGone));
                    } else {
                        self.attach(channel, *options, reply, out)?;
                    }
                }
            }
            if !out.congested() {
                next = self.outbound_rx.try_recv().ok();
            }
        }
        Ok(())
    }

    /// One incoming frame. `Ok(Some(state))` ends the connection.
    ///
    /// Nothing here waits: what the frame calls for is queued on the outbox
    /// and the application is told through queues the driver never waits on.
    fn handle(&mut self, frame: Bytes, out: &mut Outbox, closing: bool) -> Result<Option<State>> {
        let decoded = frame::decode(
            &frame,
            out.max_frame_size().max(self.options.max_frame_size),
        )?;
        let channel = decoded.header.channel;
        // SASL ended before the first AMQP frame, so a frame of the other
        // type is not one this layer reads; and an empty one is a keep-alive
        // only at the AMQP layer, where Part 5 §5.3.1 makes it fatal at the
        // other.
        if decoded.header.kind != FrameKind::Amqp {
            return Ok(refuse(
                out,
                Condition::described(
                    condition::CONNECTION_FRAMING_ERROR,
                    format!(
                        "a frame of type 0x{:02x} arrived after the SASL layer had ended; \
                         only AMQP frames (type 0x00) are allowed",
                        decoded.header.kind.octet()
                    ),
                ),
            ));
        }
        if decoded.is_empty() {
            // The keep-alive. Nothing to do but have received it, which the
            // idle deadline already noticed by being reset.
            return Ok(None);
        }
        let (performative, used) = Performative::decode(decoded.body, Limits::DEFAULT)?;
        // Everything after the performative in the same body is the
        // `transfer` payload, byte for byte — Part 2 §2.7.5 defines it as
        // exactly that and nothing parses it here. A slice of the frame, not
        // a copy of it.
        let payload = frame.slice(decoded.header.body_offset() + used..);
        match performative {
            Performative::Close(Close { error }) => {
                // Connection control is on channel 0.
                if channel != 0 && !closing {
                    return Ok(refuse(
                        out,
                        Condition::described(
                            condition::CONNECTION_FRAMING_ERROR,
                            format!(
                                "close arrived on channel {channel}, and connection control \
                                 is on channel 0"
                            ),
                        ),
                    ));
                }
                let condition = error.as_ref().map(Condition::from_codec);
                if !closing {
                    // Simultaneous close is legal at both levels; the only
                    // observable difference is which error each side reports
                    // (Part 2 §2.4.4). Answering is what makes it orderly.
                    let _ = queue_close(out, None);
                }
                Ok(Some(State::Closed(condition)))
            }
            other if closing => {
                // We have sent `close` and are only reading until the
                // partner's arrives. Discarding is what the specification
                // asks for, and logging it is how a reader of the log finds
                // out the peer was still talking.
                tracing::debug!(
                    performative = other.name(),
                    "discarding a frame while closing"
                );
                Ok(None)
            }
            Performative::Open(_) => Ok(refuse(
                out,
                Condition::described(
                    condition::ILLEGAL_STATE,
                    "a second open arrived on a connection that is already open".to_owned(),
                ),
            )),
            other => self.route(channel, other, payload, out),
        }
    }

    /// Begins a session: lowest free outgoing channel, `begin` on the wire,
    /// and the caller parked until the answer ties the two numberings
    /// together.
    fn begin(
        &mut self,
        options: SessionOptions,
        reply: oneshot::Sender<Result<Session>>,
        out: &mut Outbox,
    ) {
        // The bound is the peer's `channel-max`: it is the number that says
        // which channels the peer will accept, and ours says nothing about
        // what we may use.
        let Some(channel) = self.sessions.lowest_free(self.remote.channel_max) else {
            let _ = reply.send(Err(Error::Local(Condition::described(
                condition::RESOURCE_LIMIT_EXCEEDED,
                format!(
                    "every channel up to the peer's channel-max of {} is in use",
                    self.remote.channel_max
                ),
            ))));
            return;
        };

        let windows = Windows::new(0, options.incoming_window, options.outgoing_window);
        let bytes = match session::begin_frame(channel, &options, &windows) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };

        let (events, rx) = session::events();
        self.sessions.insert(
            channel,
            Entry {
                shared: Arc::new(Shared {
                    outgoing_channel: channel,
                    incoming_channel: Mutex::new(None),
                    windows: Mutex::new(windows),
                    state: Mutex::new(SessionState::Beginning),
                    options,
                }),
                events,
                remote_handle_max: 0,
                links: Links::default(),
                pending: Some(reply),
                outbound: self.outbound.clone(),
                rx: Some(rx),
            },
        );
        out.push(&bytes);
    }

    /// Attaches a link: lowest free handle, `attach` on the wire, and the
    /// caller parked until the answer says what the peer actually created.
    ///
    /// A link whose name and direction are already attached steals the
    /// incumbent, which is detached with `amqp:link:stolen` first
    /// (Part 2 §2.6.1).
    fn attach(
        &mut self,
        channel: u16,
        options: LinkOptions,
        reply: oneshot::Sender<Result<Link>>,
        out: &mut Outbox,
    ) -> Result<()> {
        // Read before the table is borrowed: the bound is the peer's
        // `handle-max` from its answering `begin`, and the frame ceiling is
        // the peer's `max-frame-size` from its `open` — a message is split on
        // what the *peer* will accept, never on what we would.
        let handle_max = self.remote_handle_max(channel);
        let outgoing_frame_size = self.remote.outgoing_frame_size();
        let Some(entry) = self.sessions.get_mut(channel) else {
            let _ = reply.send(Err(Error::ConnectionGone));
            return Ok(());
        };

        // The steal. Doing it before allocating a handle means the
        // incumbent's handle is not a candidate for the newcomer, which
        // matters because a stolen link is an errored one.
        let stolen = entry.links.by_name(&options.name, options.role);
        if let Some(incumbent) = stolen {
            let condition = Condition::described(
                link::STOLEN,
                format!(
                    "link {} was attached again in the same direction",
                    options.name
                ),
            );
            detach_with_error(entry, out, channel, incumbent, condition);
        }

        let Some(handle) = entry.links.lowest_free(handle_max) else {
            let _ = reply.send(Err(Error::Local(Condition::described(
                condition::RESOURCE_LIMIT_EXCEEDED,
                format!("every handle up to the peer's handle-max of {handle_max} is in use"),
            ))));
            return Ok(());
        };

        let bytes = match link::attach_frame(channel, handle, &options) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
        let frame_budget = match link::frame_budget(handle, outgoing_frame_size) {
            Ok(budget) => budget,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };

        let (events, rx) = link::events();
        let credit = match options.role {
            // A sender starts where its own `attach.initial-delivery-count`
            // said and with no credit at all: nothing may go out until the
            // receiver grants.
            Role::Sender => Credit::sender(options.initial_delivery_count),
            Role::Receiver => Credit::receiver(),
        };
        // Our own `max-message-size` bounds what may land in our buffer; the
        // peer's bounds what we may send, and that one arrives in the
        // answering `attach`.
        let ours = options.max_message_size.unwrap_or(0);
        let session = Arc::clone(&entry.shared);
        entry.links.insert(
            handle,
            link::Entry {
                shared: Arc::new(link::Shared {
                    name: options.name.clone(),
                    role: options.role,
                    output_handle: handle,
                    channel,
                    state: Mutex::new(LinkState::Attaching),
                    negotiated: Mutex::new(None),
                    input_handle: Mutex::new(None),
                    credit: Mutex::new(credit),
                    session,
                    flow: Notify::new(),
                    max_frame_size: outgoing_frame_size,
                    frame_budget,
                    next_tag: Mutex::new(0),
                    unsettled: Mutex::new(match options.role {
                        // A sender's answers are kept until they are read.
                        Role::Sender => Unsettled::for_sender(DEFAULT_MAX_UNSETTLED),
                        Role::Receiver => Unsettled::new(DEFAULT_MAX_UNSETTLED),
                    }),
                    send_lock: Arc::new(tokio::sync::Mutex::new(())),
                }),
                events,
                pending: Some(reply),
                outbound: self.outbound.clone(),
                rx: Some(rx),
                options: Box::new(options),
                reassembly: Reassembly::new(ours),
                delivery_settled: false,
            },
        );
        out.push(&bytes);
        Ok(())
    }

    /// The `handle-max` the peer advertised for this session.
    ///
    /// Kept per session because `begin.handle-max` is per session, and
    /// falling back to our own would be using the wrong side's number: the
    /// peer's is what says which handles it will accept.
    fn remote_handle_max(&mut self, channel: u16) -> u32 {
        self.sessions
            .get_mut(channel)
            .map_or(0, |entry| entry.remote_handle_max)
    }

    /// One frame for a session, or for a channel that has none.
    fn route(
        &mut self,
        channel: u16,
        performative: Performative<'_>,
        payload: Bytes,
        out: &mut Outbox,
    ) -> Result<Option<State>> {
        // A channel above what we advertised is a framing error and the
        // specification says so: "out-of-range channel MUST close the
        // connection with amqp:connection:framing-error" (Part 2 §2.7.1).
        if channel > self.options.channel_max {
            return Ok(refuse(
                out,
                Condition::described(
                    condition::CONNECTION_FRAMING_ERROR,
                    format!(
                        "channel {channel} is above the channel-max of {} this connection advertised",
                        self.options.channel_max
                    ),
                ),
            ));
        }

        // The answering `begin` is the one frame that arrives on a channel
        // with no mapping yet, and its `remote-channel` is what creates the
        // mapping.
        if let Performative::Begin(begin) = &performative
            && let Some(ours) = begin.remote_channel
            && !self.sessions.is_mapped(channel)
        {
            return Ok(self.complete_begin(channel, ours, begin, out));
        }

        let Some(entry) = self.sessions.by_incoming(channel) else {
            // In range, but no session was ever begun on it. The
            // specification names no condition for this case - it covers
            // only the out-of-range one above - so the condition is ours and
            // the description says which channel it was.
            return Ok(refuse(
                out,
                Condition::described(
                    session::UNMAPPED_CHANNEL_CONDITION,
                    format!(
                        "{} arrived on channel {channel}, which is not mapped to a session",
                        performative.name()
                    ),
                ),
            ));
        };

        // A session that sent or received `end(error=...)` discards
        // everything until the partner's `end`. Counted and logged, never
        // acted upon.
        let discarding = entry
            .shared
            .state
            .lock()
            .expect("not poisoned")
            .is_discarding();
        if discarding && !matches!(performative, Performative::End(_)) {
            tracing::debug!(
                channel,
                performative = performative.name(),
                "discarding a frame for a session in DISCARDING"
            );
            return Ok(None);
        }

        // `attach`, `detach` and `disposition` belong to the link layer: they
        // change tables the session's own bookkeeping does not touch. A
        // `disposition` names no handle at all — it names a range of
        // delivery-ids and a role — so it is the one frame that has to be
        // offered to every link of the session.
        match performative {
            Performative::Attach(attach) => self.route_attach(channel, &attach, out),
            Performative::Detach(detach) => self.route_detach(channel, &detach, out),
            Performative::Disposition(disposition) => {
                self.route_disposition(channel, disposition, out)
            }
            Performative::Flow(flow) => self.route_flow(channel, &flow, out),
            Performative::Transfer(transfer) => {
                self.route_transfer(channel, &transfer, payload, out)
            }
            Performative::End(end) => self.route_end(channel, &end, out),
            // A second `begin` or `open` for a session that has begun, or a
            // `close` that was not caught above: not frames this state
            // permits.
            other => Ok(refuse(
                out,
                Condition::described(
                    condition::ILLEGAL_STATE,
                    format!(
                        "{} arrived on channel {channel}, whose session has already begun",
                        other.name()
                    ),
                ),
            )),
        }
    }

    /// A `flow` from the peer: the session's windows, and, where it names a
    /// handle, the link's credit.
    fn route_flow(
        &mut self,
        channel: u16,
        flow: &Flow<'_>,
        out: &mut Outbox,
    ) -> Result<Option<State>> {
        let Some(entry) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        entry
            .shared
            .windows
            .lock()
            .expect("not poisoned")
            .apply_flow(session::remote_flow_of_flow(flow));
        // The session window may have grown, and a sender parked on it is
        // parked on its link. Every `flow` refreshes session state whether or
        // not it names a handle, so every `flow` wakes them.
        entry.links.wake_all();

        // A `flow` without a handle is a session-level update, and the
        // windows above are all there is to do with it.
        let Some(handle) = flow.handle else {
            return Ok(None);
        };
        let outgoing = entry.shared.outgoing_channel;
        // A snapshot, because an answering `flow` carries the session's three
        // mandatory fields and the link must not hold the window lock while
        // it builds one.
        let windows = *entry.shared.windows.lock().expect("not poisoned");
        let Some(link) = entry.links.by_input(handle) else {
            return Ok(unattached(entry, handle, "flow", out));
        };
        let answer = link.accept_flow(flow, &windows);
        let credit = link.credit();
        link.events.push(LinkEvent::Flow(credit));
        if let Some(answer) = answer {
            write_flow(out, outgoing, answer)?;
        }
        Ok(None)
    }

    /// A `transfer` from the peer: counted against the session's incoming
    /// window, then handed to the link it names.
    fn route_transfer(
        &mut self,
        channel: u16,
        transfer: &Transfer<'_>,
        payload: Bytes,
        out: &mut Outbox,
    ) -> Result<Option<State>> {
        let Some(entry) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        let outgoing = entry.shared.outgoing_channel;
        let violation = {
            let mut windows = entry.shared.windows.lock().expect("not poisoned");
            let id = windows.next_incoming_id;
            windows.record_received(id)
        };
        if let Err(violation) = violation {
            // Part 2 §2.8.17: `end` the session with
            // `amqp:session:window-violation`.
            let condition =
                Condition::described(condition::SESSION_WINDOW_VIOLATION, violation.to_string());
            *entry.shared.state.lock().expect("not poisoned") =
                SessionState::Discarding(condition.clone());
            let _ = write_end(out, outgoing, Some(&condition));
            return Ok(None);
        }

        let handle = transfer.handle;
        let Some(link) = entry.links.by_input(handle) else {
            return Ok(unattached(entry, handle, "transfer", out));
        };
        let output = link.shared.output_handle;
        match link.accept_transfer(transfer, payload) {
            Incoming::Nothing => {}
            Incoming::Delivery(delivery) => link.events.push(LinkEvent::Delivery(delivery)),
            Incoming::Refused(condition) => {
                // "An errored link endpoint MUST be detached with
                // detach(error=...) and destroyed", and its handle is
                // poisoned for the life of the session so that a late frame
                // for the dead link cannot land on a live one.
                detach_with_error(entry, out, outgoing, output, condition);
            }
        }
        Ok(None)
    }

    /// An `end` from the peer.
    fn route_end(
        &mut self,
        channel: u16,
        end: &End<'_>,
        out: &mut Outbox,
    ) -> Result<Option<State>> {
        let Some(entry) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        let condition = end.error.as_ref().map(Condition::from_codec);
        let outgoing = entry.shared.outgoing_channel;
        let answered = matches!(
            *entry.shared.state.lock().expect("not poisoned"),
            SessionState::Ending | SessionState::Discarding(_)
        );
        *entry.shared.state.lock().expect("not poisoned") = SessionState::Ended(condition.clone());
        if !answered {
            // Answering is what makes it orderly; simultaneous `end` is legal
            // and the only difference is which condition each side reports.
            let _ = write_end(out, outgoing, None);
        }
        // "Sessions also end automatically when the connection closes"; a
        // session ending takes its links with it for the same reason.
        if let Some(session) = self.sessions.remove(outgoing) {
            end_session(session, condition);
        }
        Ok(None)
    }

    /// A `disposition` from the peer.
    ///
    /// It names no handle. `role` names the *speaker*, so a receiver's
    /// `disposition` is about deliveries this end sent and a sender's is
    /// about deliveries this end received; the frame is offered to every link
    /// of the session in the addressed direction, and a link that holds none
    /// of the ids simply does nothing. That is also where idempotence comes
    /// from: a repeated `disposition` finds nothing to change.
    fn route_disposition(
        &mut self,
        channel: u16,
        disposition: Disposition<'_>,
        out: &mut Outbox,
    ) -> Result<Option<State>> {
        let first = disposition.first;
        // "The highest delivery-id covered; unset means `first` alone."
        let last = disposition.last.unwrap_or(first);
        // Moved, not cloned: the performative is this frame's own.
        let state = match disposition.state {
            Some(value) => Some(weida_amqp_codec::state::DeliveryState::from_value(value)?),
            None => None,
        };
        let outcome = match &state {
            Some(state) => Outcome::of(state)?,
            None => None,
        };

        let Some(session) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        let outgoing = session.shared.outgoing_channel;
        let mut owed: Vec<(u32, Outcome)> = Vec::new();
        for link in session.links.addressed(disposition.role) {
            let second = link.second_mode();
            let sender = link.shared.role == Role::Sender;
            for (delivery_id, settlement) in
                link.accept_disposition(first, last, outcome.as_ref(), disposition.settled)
            {
                match settlement {
                    Settlement::Nothing | Settlement::Progress => {}
                    Settlement::Settled(outcome) => {
                        link.events.push(LinkEvent::Outcome {
                            delivery_id,
                            outcome,
                            settled: true,
                        });
                    }
                    Settlement::Provisional(outcome) => {
                        // `rcv-settle-mode=second`: the receiver published an
                        // outcome and is waiting for this end to settle
                        // before it settles itself. Owed by the driver,
                        // because the application has nothing left to decide
                        // — and once written, the delivery is over here, so
                        // the report is final.
                        let final_here = second && sender;
                        if final_here {
                            owed.push((delivery_id, outcome.clone()));
                            // Settled here as well now, so the delivery is
                            // over at this end.
                            link.finish_here(delivery_id);
                        }
                        link.events.push(LinkEvent::Outcome {
                            delivery_id,
                            outcome,
                            settled: final_here,
                        });
                    }
                }
            }
        }
        write_owed(out, outgoing, &owed)?;
        Ok(None)
    }

    /// An `attach` from the peer: either the answer to ours, or an
    /// unsolicited one.
    fn route_attach(
        &mut self,
        channel: u16,
        attach: &Attach<'_>,
        out: &mut Outbox,
    ) -> Result<Option<State>> {
        let Some(entry) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        let outgoing = entry.shared.outgoing_channel;

        // A handle above the `handle-max` this end advertised is what that
        // number exists to refuse: "out-of-range handle MUST close the
        // connection with amqp:connection:framing-error" (Part 2 §2.7.2). It
        // is also what keeps the table the peer's handles are mapped in as
        // small as the bound says.
        let handle_max = entry.shared.options.handle_max;
        if attach.handle > handle_max {
            return Ok(refuse(
                out,
                Condition::described(
                    condition::CONNECTION_FRAMING_ERROR,
                    format!(
                        "attach on handle {}, above the handle-max of {handle_max} this session advertised",
                        attach.handle
                    ),
                ),
            ));
        }

        // "Attaching on a handle already in use MUST be answered with an
        // immediate close carrying amqp:session:handle-in-use"
        // (Part 2 §2.6.2). The handle space in question is the *input* one:
        // the peer's own numbering.
        if entry.links.input_in_use(attach.handle) {
            return Ok(refuse(
                out,
                Condition::described(
                    condition::SESSION_HANDLE_IN_USE,
                    format!("handle {} is already attached", attach.handle),
                ),
            ));
        }

        // The answer is correlated by *name*, because the two ends choose
        // their handles independently and the numbers do not agree.
        let ours = entry.links.by_name(attach.name, attach.role.opposite());
        let Some(ours) = ours else {
            // An unsolicited `attach`. This client holds no nodes — AMQP 1.0
            // "defines no operation to create, configure, enumerate or delete
            // a node" and a client is not a broker — so the specified refusal
            // is an answering `attach` with the terminus null followed by an
            // immediate `detach` (Part 2 §2.6.3).
            refuse_attach(out, outgoing, attach)?;
            let _ = write_detach(out, outgoing, attach.handle, true, None);
            tracing::debug!(
                link = attach.name,
                handle = attach.handle,
                "refused an unsolicited attach: this client offers no nodes"
            );
            return Ok(None);
        };

        let Some(link) = entry.links.get_mut(ours) else {
            return Ok(None);
        };

        // A link that has already been answered is attached: a second
        // `attach` for it from the peer is the peer taking the link over, and
        // the endpoint it takes it from is closed with `amqp:link:stolen`
        // (Part 2 §2.6.1). Mapping the new handle onto it as well would let
        // one link accumulate a handle per frame, and would reset its credit
        // and its negotiated modes under an application that is using them.
        if link
            .shared
            .input_handle
            .lock()
            .expect("not poisoned")
            .is_some()
        {
            let condition = Condition::described(
                link::STOLEN,
                format!("link {} was attached again by the peer", attach.name),
            );
            detach_with_error(entry, out, outgoing, ours, condition);
            // The new `attach` names a link this end no longer has.
            refuse_attach(out, outgoing, attach)?;
            let _ = write_detach(out, outgoing, attach.handle, true, None);
            return Ok(None);
        }

        let negotiated = match link::negotiate(&link.options, attach) {
            Ok(negotiated) => negotiated,
            Err(error) => {
                if let Some(reply) = link.pending.take() {
                    let _ = reply.send(Err(error));
                }
                return Ok(None);
            }
        };
        *link.shared.input_handle.lock().expect("not poisoned") = Some(attach.handle);
        *link.shared.negotiated.lock().expect("not poisoned") = Some(negotiated);
        if link.shared.role == Role::Receiver {
            // Where the sender's sequence starts. A receiver's credit is a
            // distance from a `delivery-count` it does not choose, so until
            // this arrives it cannot name one in a `flow` — and MUST NOT
            // (Part 2 §2.7.4).
            link.shared
                .credit
                .lock()
                .expect("not poisoned")
                .seen_sender_attach(attach.initial_delivery_count.unwrap_or_default());
        }
        *link.shared.state.lock().expect("not poisoned") = LinkState::Attached;
        let handed = match (link.pending.take(), link.rx.take()) {
            (Some(reply), Some(rx)) => {
                let handle = Link::new(
                    Arc::clone(&link.shared),
                    link.outbound.clone(),
                    rx,
                    self.exec.clone(),
                    Lease::of(&self.commands),
                );
                let _ = reply.send(Ok(handle));
                true
            }
            _ => false,
        };
        entry.links.map_input(attach.handle, ours);
        tracing::debug!(
            link = attach.name,
            output = ours,
            input = attach.handle,
            links = entry.links.len(),
            handed_over = handed,
            "link attached"
        );
        Ok(None)
    }

    /// A `detach` from the peer.
    ///
    /// Answered, unless this end sent one first. A link endpoint is destroyed
    /// only when **both** ends have detached (Part 2 §2.6.4), so a peer that
    /// initiated the detach is waiting for this frame — and a peer whose
    /// application holds the link until it arrives waits forever without it.
    /// `fe2o3-amqp`'s acceptor is such a peer: its `close()` does not return
    /// until the answering `detach` lands.
    fn route_detach(
        &mut self,
        channel: u16,
        detach: &Detach<'_>,
        out: &mut Outbox,
    ) -> Result<Option<State>> {
        let Some(entry) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        let outgoing = entry.shared.outgoing_channel;
        let condition = detach.error.as_ref().map(Condition::from_codec);
        let Some(link) = entry.links.by_input(detach.handle) else {
            if entry.links.answered(detach.handle) {
                tracing::debug!(
                    handle = detach.handle,
                    "the peer answered the detach of a link this end ended with an error"
                );
            } else {
                tracing::debug!(
                    handle = detach.handle,
                    "a detach for a handle that is not attached"
                );
            }
            return Ok(None);
        };
        let output = link.shared.output_handle;
        // Whether this end had already sent its own `detach`; answering twice
        // would be a frame for a link that no longer exists at the peer.
        let answered = matches!(
            *link.shared.state.lock().expect("not poisoned"),
            LinkState::Detaching | LinkState::Detached(_)
        );
        // An errored endpoint's handle is never reused, because the peer is
        // entitled to still be sending frames for it.
        let errored = condition.is_some();
        if let Some(link) = entry.links.remove(output, errored) {
            end_link(link, condition);
        }
        if !answered {
            // The same `closed` flag the peer used: answering `closed=true`
            // with `closed=false` would claim the deliveries are still live.
            let _ = write_detach(out, outgoing, output, detach.closed, None);
        }
        Ok(None)
    }

    /// The answering `begin`: tie the peer's outgoing channel to ours and
    /// hand the caller its handle.
    fn complete_begin(
        &mut self,
        incoming: u16,
        ours: u16,
        begin: &Begin<'_>,
        out: &mut Outbox,
    ) -> Option<State> {
        let exec = self.exec.clone();
        let attach_timeout = self.options.handshake_timeout;
        let Some(entry) = self.sessions.get_mut(ours) else {
            // A `remote-channel` naming a session we never begun. Nothing to
            // attach it to, and inventing one would be inventing state.
            tracing::warn!(
                incoming,
                remote_channel = ours,
                "an answering begin named a channel with no session"
            );
            return None;
        };
        // The answer to a `begin` is one frame. A second one for a session
        // that has already been answered is not a repeat to ignore: it would
        // re-run the window arithmetic under a session in use, and would turn
        // one that is ending back into one that is begun.
        if entry.pending.is_none() {
            return refuse(
                out,
                Condition::described(
                    condition::ILLEGAL_STATE,
                    format!(
                        "a begin on channel {incoming} answered channel {ours}, \
                         whose session has already begun"
                    ),
                ),
            );
        }
        *entry.shared.incoming_channel.lock().expect("not poisoned") = Some(incoming);
        entry
            .shared
            .windows
            .lock()
            .expect("not poisoned")
            .apply_begin(session::remote_flow_of_begin(begin));
        // The bound on this session's link table, and the peer's number: it
        // states which handles the peer will accept.
        entry.remote_handle_max = begin.handle_max;
        *entry.shared.state.lock().expect("not poisoned") = SessionState::Begun;
        let handle = match (entry.pending.take(), entry.rx.take()) {
            (Some(reply), Some(rx)) => {
                let session = Session::new(
                    Arc::clone(&entry.shared),
                    entry.outbound.clone(),
                    rx,
                    exec,
                    attach_timeout,
                    Lease::of(&self.commands),
                );
                let _ = reply.send(Ok(session));
                true
            }
            _ => false,
        };
        self.sessions.map_incoming(incoming, ours);
        tracing::debug!(
            outgoing = ours,
            incoming,
            handed_over = handle,
            sessions = self.sessions.len(),
            "session begun"
        );
        None
    }
}

/// Closes the connection because of `condition`: `close` is the last thing
/// this end writes, and the connection ends as closed with that condition.
///
/// A failure to queue it is ignored, because the connection is ending either
/// way and there is nothing better to do with the failure.
fn refuse(out: &mut Outbox, condition: Condition) -> Option<State> {
    let _ = queue_close(out, Some(&condition));
    Some(State::Closed(Some(condition)))
}

/// Queues `close(condition)` for a peer whose octets could not be read, where
/// the error names a condition; `false` where there is nothing to say or the
/// transport is what failed.
fn tell_why(out: &mut Outbox, error: &Error) -> bool {
    let Some(code) = error.wire_condition() else {
        return false;
    };
    if !matches!(error, Error::Decode(_)) {
        return false;
    }
    queue_close(out, Some(&Condition::described(code, error.to_string()))).is_ok()
}

/// A frame naming a handle that is not an attached link.
///
/// Two different conditions, and the specification distinguishes them. A
/// handle detached **with an error**: "any later input on that handle or its
/// delivery-ids MUST end the session with `amqp:session:errant-link`"
/// (Part 2 §2.6.5). A handle never attached at all:
/// `amqp:session:unattached-handle` (§2.8.17). A handle cleanly detached is
/// neither: the peer's frame may already have been in flight when the two
/// crossed, and `fe2o3-amqp`'s acceptor really does send a `flow` for a
/// handle it has just detached, so ending the session over it would make
/// every orderly link close a coin toss.
fn unattached(entry: &mut Entry, handle: u32, what: &str, out: &mut Outbox) -> Option<State> {
    if entry.links.was_detached(handle) {
        tracing::debug!(
            handle,
            performative = what,
            "discarding a frame for a link that was already detached"
        );
        return None;
    }
    let condition = if entry.links.was_errant(handle) {
        Condition::described(
            condition::SESSION_ERRANT_LINK,
            format!("{what} arrived on handle {handle}, whose link was detached with an error"),
        )
    } else {
        Condition::described(
            condition::SESSION_UNATTACHED_HANDLE,
            format!("{what} arrived on handle {handle}, which is not an attached link"),
        )
    };
    let outgoing = entry.shared.outgoing_channel;
    *entry.shared.state.lock().expect("not poisoned") = SessionState::Discarding(condition.clone());
    let _ = write_end(out, outgoing, Some(&condition));
    None
}

/// Tells a link that it is over, and wakes whatever is parked on it.
///
/// The state first, so that what wakes finds the link gone; then the event,
/// which closes the queue behind it, so that a handle reading events finds out
/// rather than waiting on a queue nobody will push to again.
fn end_link(link: link::Entry, condition: Option<Condition>) {
    *link.shared.state.lock().expect("not poisoned") = LinkState::Detached(condition.clone());
    link.events.push(LinkEvent::Detached(condition));
    link.shared.flow.notify_waiters();
}

/// Detaches the link `output` with `condition` on this end's initiative,
/// destroys it and poisons the peer's handles for it (Part 2 §2.6.5). Until
/// the peer's answering `detach` arrives, its frames for the link are still
/// crossing ours and are discarded rather than ending the session:
/// [`link::Links::await_answer`].
fn detach_with_error(
    entry: &mut Entry,
    out: &mut Outbox,
    channel: u16,
    output: u32,
    condition: Condition,
) {
    let _ = write_detach(out, channel, output, true, Some(&condition));
    let inputs = entry.links.inputs_of(output);
    if let Some(link) = entry.links.remove(output, true) {
        end_link(link, Some(condition));
    }
    entry.links.await_answer(&inputs);
}

/// Tells every link of an ending session, and then the session, that it is
/// over.
///
/// A session ending destroys its link endpoints, so a caller holding a
/// [`Link`] finds out here rather than waiting on a queue nobody will push to
/// again.
fn end_session(mut session: Entry, condition: Option<Condition>) {
    for link in session.links.drain() {
        end_link(link, condition.clone());
    }
    session.events.push(SessionEvent::Ended(condition));
}

/// The specified refusal of an `attach` this client cannot honour: an
/// answering `attach` with both termini null.
///
/// Part 2 §2.6.3: "a partner that will not provide a terminus answers with
/// that field null, which establishes a link to a nonexistent terminus, and
/// MUST then immediately detach." An error code would be inventing one; this
/// is the absence the specification asks for.
fn refuse_attach(out: &mut Outbox, channel: u16, theirs: &Attach<'_>) -> Result<()> {
    let mut answer = Attach::new(theirs.name, theirs.handle, theirs.role.opposite());
    // Both termini null: nothing was created.
    answer.source = None;
    answer.target = None;
    if answer.role == Role::Sender {
        answer.initial_delivery_count = Some(0);
    }
    out.frame(channel, |body| Performative::Attach(answer).encode(body))?;
    Ok(())
}

/// Queues `detach` on one session's outgoing channel.
fn write_detach(
    out: &mut Outbox,
    channel: u16,
    handle: u32,
    closed: bool,
    error: Option<&Condition>,
) -> Result<()> {
    let codec = error.map(Condition::as_codec);
    out.frame(channel, |body| {
        Performative::Detach(Detach {
            handle,
            closed,
            error: codec,
        })
        .encode(body)
    })?;
    Ok(())
}

/// Queues `flow` on one session's outgoing channel.
///
/// The driver writes this one rather than the link handle, because the two
/// cases that produce it are answers the *driver* owes: an `echo` asking for
/// our state, and the report a drained sender MUST send. Neither waits for an
/// application to notice.
fn write_flow(out: &mut Outbox, channel: u16, flow: Flow<'_>) -> Result<()> {
    out.frame(channel, |body| Performative::Flow(flow).encode(body))?;
    Ok(())
}

/// Queues the settling `disposition`s the driver owes, one frame for every run
/// of consecutive delivery-ids that were answered the same way.
///
/// The driver writes these because the case that produces them is not the
/// application's: under `rcv-settle-mode=second` the sender MUST settle once
/// the receiver has published its outcome, and nothing is left to decide. A
/// range is one frame, which is what keeps one incoming range `disposition`
/// from becoming a frame per delivery.
fn write_owed(out: &mut Outbox, channel: u16, owed: &[(u32, Outcome)]) -> Result<()> {
    let mut at = 0;
    while at < owed.len() {
        let (first, outcome) = &owed[at];
        let mut last = *first;
        let mut end = at + 1;
        while end < owed.len() && owed[end].0 == last.wrapping_add(1) && owed[end].1 == *outcome {
            last = owed[end].0;
            end += 1;
        }
        let state = outcome.to_state();
        let mut disposition = Disposition::new(Role::Sender, *first);
        disposition.last = (last != *first).then_some(last);
        disposition.settled = true;
        disposition.state = Some(weida_amqp_codec::state::DeliveryState::to_value(&state));
        out.frame(channel, |body| {
            Performative::Disposition(disposition).encode(body)
        })?;
        at = end;
    }
    Ok(())
}

/// Queues `end` on one session's outgoing channel.
fn write_end(out: &mut Outbox, channel: u16, error: Option<&Condition>) -> Result<()> {
    let codec = error.map(Condition::as_codec);
    out.frame(channel, |body| {
        Performative::End(End { error: codec }).encode(body)
    })?;
    Ok(())
}

/// Queues `close`, which is the last thing ever written on a connection.
fn queue_close(out: &mut Outbox, error: Option<&Condition>) -> Result<()> {
    let codec = error.map(Condition::as_codec);
    out.frame(0, |body| {
        Performative::Close(Close { error: codec }).encode(body)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keepalive_is_half_the_threshold() {
        assert_eq!(
            keepalive_period(Some(200)),
            Some(Duration::from_millis(100)),
            "a keep-alive exactly at the deadline arrives too late"
        );
        assert_eq!(keepalive_period(None), None, "no threshold, no keep-alive");
        assert_eq!(keepalive_period(Some(0)), None);
    }

    #[test]
    fn a_peer_cannot_make_the_keepalive_a_busy_loop() {
        // The peer picks this number, and one millisecond would make half of
        // it a write every half millisecond for the life of the connection.
        for asked in [1, 2, 10, 99] {
            assert_eq!(
                keepalive_period(Some(asked)),
                Some(MIN_KEEPALIVE),
                "idle-time-out {asked} ms"
            );
        }
        assert!(keepalive_period(Some(u32::MAX)).is_some_and(|period| period > MIN_KEEPALIVE));
    }
}
