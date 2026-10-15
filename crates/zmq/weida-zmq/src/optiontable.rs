//! Every `zmq_setsockopt` and `zmq_ctx_set` option, honoured or refused by
//! name.
//!
//! 0013 §4.4 item 4: an option is **honoured or refused, never silently
//! ignored**. The typed fields of [`SocketOptions`](crate::SocketOptions) and
//! [`ContextConfig`](crate::ContextConfig) are the honoured half — a value
//! this library cannot deliver already fails where it is set. This module is
//! the other half: the options libzmq has and this library does not, each
//! with the reason, so that a reader porting a `zmq_setsockopt` call finds an
//! answer rather than a silence.
//!
//! # Where the list comes from
//!
//! `docs/research/zeromq.md`: §2's context options, §4's per-pattern
//! options, §5's flow-control options, §10's security options, §11's limit
//! table and §13's 4.3.5 additions, plus the option names libzmq's
//! `zmq_setsockopt(3)` defines for the transports and mechanisms that sheet
//! names as absent. Nothing here is invented: every `ZMQ_*` below is an
//! option libzmq 4.3.5 accepts.
//!
//! **Read-only options are not in the table.** `ZMQ_EVENTS`, `ZMQ_FD`,
//! `ZMQ_LAST_ENDPOINT`, `ZMQ_SOCKET_LIMIT`, `ZMQ_MSG_T_SIZE` and
//! `ZMQ_MECHANISM` are `zmq_getsockopt`/`zmq_ctx_get` only, so there is no
//! configuration-time decision to record for them. `ZMQ_LAST_ENDPOINT` is
//! answered by [`crate::Engine`]'s own accessor.
//!
//! # The five reasons
//!
//! [`Refusal`] is closed on purpose: a refusal that could not be put in one
//! of these five would be an option nobody had thought about.
//!
//! # The two deliberate default changes
//!
//! 0013 §4.4 item 5. They are in the table as honoured options, and their
//! rows say where the changed default lives:
//!
//! * `ZMQ_LINGER` is infinite in libzmq and **finite** here
//!   ([`DEFAULT_CLOSE_BUDGET`](crate::DEFAULT_CLOSE_BUDGET)), because
//!   "termination waits forever" is how a ZeroMQ process hangs at shutdown.
//! * `ZMQ_MAXMSGSIZE` is "no limit" in libzmq and **bounded** here
//!   ([`DEFAULT_MAX_MESSAGE_SIZE`](crate::DEFAULT_MAX_MESSAGE_SIZE)), because
//!   a frame may declare 2^63-1 octets and that option is the only defence.
//!
//! Both are settable back to anything the type allows; neither is silent.

use std::fmt;

use crate::error::{Error, Result};

/// Which libzmq call takes the option.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// `zmq_setsockopt`, per socket.
    Socket,
    /// `zmq_ctx_set`, per context and before the first socket.
    Context,
}

/// Why an option is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The transport it configures is not implemented here. The named
    /// transports are absent as transports, not as options.
    NoTransport(&'static str),
    /// A libzmq DRAFT option: "in DRAFT state, not yet available in stable
    /// releases", and "WebSockets support is disabled by default if DRAFT
    /// APIs are disabled" — so a peer cannot rely on it either.
    DraftOnly,
    /// Deprecated by libzmq itself in favour of ZAP, whose dialog this
    /// library implements: "This option is deprecated, please use
    /// authentication via the ZAP API."
    ZapInstead,
    /// Replaced by a `weida-runtime` construct, named here.
    RuntimeInstead(&'static str),
    /// Absent, with what is missing named. Never a silence and never a
    /// "partial".
    Absent(&'static str),
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refusal::NoTransport(what) => {
                write!(f, "no transport: this library does not implement {what}")
            }
            Refusal::DraftOnly => f.write_str(
                "draft only: it is a libzmq DRAFT option, absent from stable builds, so no \
                 peer can rely on it",
            ),
            Refusal::ZapInstead => f.write_str(
                "deprecated in favour of ZAP: libzmq's own manual says so, and the ZAP dialog \
                 is implemented here",
            ),
            Refusal::RuntimeInstead(what) => {
                write!(f, "replaced by a weida-runtime construct: {what}")
            }
            Refusal::Absent(what) => write!(f, "absent: {what}"),
        }
    }
}

/// What this library does with one option.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Honoured, under the name this library gives it.
    Honoured(&'static str),
    /// Refused at configuration time, with the reason.
    Refused(Refusal),
}

/// One row of the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZmqOption {
    /// libzmq's own name.
    pub name: &'static str,
    /// The call that takes it.
    pub scope: Scope,
    /// Honoured or refused.
    pub verdict: Verdict,
}

impl ZmqOption {
    /// The refusal this option produces, or `None` when it is honoured.
    pub fn refusal(&self) -> Option<Error> {
        match self.verdict {
            Verdict::Honoured(_) => None,
            Verdict::Refused(reason) => Some(Error::EINVAL(
                format!("{} is refused - {reason}", self.name).into(),
            )),
        }
    }
}

/// Looks one option up by libzmq's name.
pub fn option(name: &str) -> Option<&'static ZmqOption> {
    OPTIONS.iter().find(|option| option.name == name)
}

/// What this library binds an option to, or the refusal.
///
/// The configuration-time answer to a `zmq_setsockopt` call, by name: a
/// caller porting C can ask before it ports.
///
/// # Errors
///
/// `EINVAL` naming the reason for a refused option, and `EINVAL` for a name
/// that is not in the table at all — including the read-only ones, which are
/// not settable in libzmq either.
pub fn honoured(name: &str) -> Result<&'static str> {
    let Some(option) = option(name) else {
        return Err(Error::EINVAL(
            format!(
                "{name} is not an option this library records; the table is \
                 weida_zmq::optiontable::OPTIONS"
            )
            .into(),
        ));
    };
    match option.verdict {
        Verdict::Honoured(binding) => Ok(binding),
        Verdict::Refused(_) => Err(option.refusal().expect("a refused option refuses")),
    }
}

use Refusal::{Absent, DraftOnly, NoTransport, RuntimeInstead, ZapInstead};
use Scope::{Context as Ctx, Socket as Sock};
use Verdict::{Honoured, Refused};

/// The whole table, in libzmq's own names.
pub const OPTIONS: &[ZmqOption] = &[
    // ---- context ------------------------------------------------------
    ZmqOption {
        name: "ZMQ_IO_THREADS",
        scope: Ctx,
        verdict: Refused(RuntimeInstead(
            "ContextConfig::worker_threads sizes the reactor Context::owned creates, and \
             Context::new joins a reactor somebody else sized; the I/O thread pool is a Tokio \
             runtime here and there is no second pool to count",
        )),
    },
    ZmqOption {
        name: "ZMQ_MAX_SOCKETS",
        scope: Ctx,
        verdict: Honoured("ContextConfig::max_sockets, default 1023 as in libzmq"),
    },
    ZmqOption {
        name: "ZMQ_MAX_MSGSZ",
        scope: Ctx,
        verdict: Refused(Absent(
            "a context-wide message ceiling on top of the per-socket one; ZMQ_MAXMSGSIZE is \
             where a message is bounded here, and two ceilings for one question is how a \
             message gets refused for a reason nobody can find",
        )),
    },
    ZmqOption {
        name: "ZMQ_BLOCKY",
        scope: Ctx,
        verdict: Refused(RuntimeInstead(
            "ContextConfig::close_budget is finite by default, so there is no block-forever to \
             switch off; ZMQ_BLOCKY exists because libzmq's default is the other way",
        )),
    },
    ZmqOption {
        name: "ZMQ_ZERO_COPY_RECV",
        scope: Ctx,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_BUSY_POLL",
        scope: Ctx,
        verdict: Refused(RuntimeInstead(
            "how the reactor waits is the reactor's business; a Tokio runtime is not \
             busy-pollable from here",
        )),
    },
    ZmqOption {
        name: "ZMQ_THREAD_SCHED_POLICY",
        scope: Ctx,
        verdict: Refused(RuntimeInstead(
            "the reactor owns its threads: Context::owned creates and names them, Context::new \
             and Context::with_handle borrow somebody else's",
        )),
    },
    ZmqOption {
        name: "ZMQ_THREAD_PRIORITY",
        scope: Ctx,
        verdict: Refused(RuntimeInstead("the reactor owns its threads")),
    },
    ZmqOption {
        name: "ZMQ_THREAD_AFFINITY_CPU_ADD",
        scope: Ctx,
        verdict: Refused(RuntimeInstead("the reactor owns its threads")),
    },
    ZmqOption {
        name: "ZMQ_THREAD_AFFINITY_CPU_REMOVE",
        scope: Ctx,
        verdict: Refused(RuntimeInstead("the reactor owns its threads")),
    },
    ZmqOption {
        name: "ZMQ_THREAD_NAME_PREFIX",
        scope: Ctx,
        verdict: Refused(RuntimeInstead(
            "Context::owned names its reactor's threads \"weida-zmq\"",
        )),
    },
    // ---- queues, sizes and timeouts -----------------------------------
    ZmqOption {
        name: "ZMQ_SNDHWM",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::pipe.outgoing.hwm, in messages, default 1000, beside \
             SocketOptions::pipe.outgoing.max_bytes (8 MiB), which libzmq has no option for",
        ),
    },
    ZmqOption {
        name: "ZMQ_RCVHWM",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::pipe.incoming.hwm, in messages, default 1000, beside \
             SocketOptions::pipe.incoming.max_bytes (8 MiB), which libzmq has no option for",
        ),
    },
    ZmqOption {
        name: "ZMQ_MAXMSGSIZE",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::max_message_size - bounded by default where libzmq has no limit, \
             which is 0013 §4.4 item 5's second deliberate deviation (DEFAULT_MAX_MESSAGE_SIZE)",
        ),
    },
    ZmqOption {
        name: "ZMQ_LINGER",
        scope: Sock,
        verdict: Honoured(
            "ContextConfig::close_budget - finite by default where libzmq's is infinite, which \
             is 0013 §4.4 item 5's first deliberate deviation (DEFAULT_CLOSE_BUDGET)",
        ),
    },
    ZmqOption {
        name: "ZMQ_SNDTIMEO",
        scope: Sock,
        verdict: Honoured("SocketOptions::send_timeout"),
    },
    ZmqOption {
        name: "ZMQ_RCVTIMEO",
        scope: Sock,
        verdict: Honoured("SocketOptions::recv_timeout"),
    },
    ZmqOption {
        name: "ZMQ_SNDBUF",
        scope: Sock,
        verdict: Refused(Absent(
            "kernel socket buffers are left at the OS default; what this library bounds is the \
             message queue, in messages, under ZMQ_SNDHWM",
        )),
    },
    ZmqOption {
        name: "ZMQ_RCVBUF",
        scope: Sock,
        verdict: Refused(Absent(
            "kernel socket buffers are left at the OS default; ZMQ_RCVHWM is the queue this \
             library bounds",
        )),
    },
    ZmqOption {
        name: "ZMQ_CONFLATE",
        scope: Sock,
        verdict: Refused(Absent(
            "keep-only-the-last is a queue this library does not have, and §11 names it as one \
             of the four unbounded-resource shapes: \"the queue and memory will grow with each \
             message received\" on an inbound socket nobody reads",
        )),
    },
    ZmqOption {
        name: "ZMQ_AFFINITY",
        scope: Sock,
        verdict: Refused(RuntimeInstead(
            "which I/O thread serves a connection: there is one reactor and it schedules its \
             own tasks",
        )),
    },
    // ---- connection lifecycle -----------------------------------------
    ZmqOption {
        name: "ZMQ_RECONNECT_IVL",
        scope: Sock,
        verdict: Honoured("SocketOptions::reconnect_ivl, None for libzmq's -1"),
    },
    ZmqOption {
        name: "ZMQ_RECONNECT_IVL_MAX",
        scope: Sock,
        verdict: Honoured("SocketOptions::reconnect_ivl_max, None for libzmq's 0"),
    },
    ZmqOption {
        name: "ZMQ_RECONNECT_STOP",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_HANDSHAKE_IVL",
        scope: Sock,
        verdict: Honoured("SocketOptions::handshake_ivl, finite by default"),
    },
    ZmqOption {
        name: "ZMQ_CONNECT_TIMEOUT",
        scope: Sock,
        verdict: Honoured("SocketOptions::connect_timeout"),
    },
    ZmqOption {
        name: "ZMQ_IMMEDIATE",
        scope: Sock,
        verdict: Honoured("SocketOptions::immediate"),
    },
    ZmqOption {
        name: "ZMQ_BACKLOG",
        scope: Sock,
        verdict: Honoured("SocketOptions::backlog, default 100"),
    },
    ZmqOption {
        name: "ZMQ_HEARTBEAT_IVL",
        scope: Sock,
        verdict: Honoured("SocketOptions::heartbeat_ivl"),
    },
    ZmqOption {
        name: "ZMQ_HEARTBEAT_TIMEOUT",
        scope: Sock,
        verdict: Honoured("SocketOptions::heartbeat_timeout"),
    },
    ZmqOption {
        name: "ZMQ_HEARTBEAT_TTL",
        scope: Sock,
        verdict: Honoured("SocketOptions::heartbeat_ttl, deciseconds on the wire"),
    },
    ZmqOption {
        name: "ZMQ_TCP_KEEPALIVE",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::tcp_keepalive, None for libzmq's -1: SO_KEEPALIVE is left as the \
             operating system has it, which is the default",
        ),
    },
    ZmqOption {
        name: "ZMQ_TCP_KEEPALIVE_IDLE",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::tcp_keepalive_idle, whole seconds, None for libzmq's -1; refused \
             unless ZMQ_TCP_KEEPALIVE is 1, where libzmq ignores it",
        ),
    },
    ZmqOption {
        name: "ZMQ_TCP_KEEPALIVE_CNT",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::tcp_keepalive_cnt, None for libzmq's -1; refused unless \
             ZMQ_TCP_KEEPALIVE is 1, where libzmq ignores it",
        ),
    },
    ZmqOption {
        name: "ZMQ_TCP_KEEPALIVE_INTVL",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::tcp_keepalive_intvl, whole seconds, None for libzmq's -1; refused \
             unless ZMQ_TCP_KEEPALIVE is 1, where libzmq ignores it",
        ),
    },
    ZmqOption {
        name: "ZMQ_TCP_MAXRT",
        scope: Sock,
        verdict: Refused(Absent(
            "TCP_USER_TIMEOUT is not set; ZMQ_HANDSHAKE_IVL bounds a handshake and \
             ZMQ_HEARTBEAT_IVL bounds a silence",
        )),
    },
    ZmqOption {
        name: "ZMQ_TOS",
        scope: Sock,
        verdict: Refused(Absent(
            "no IP type-of-service is set; the OS default applies",
        )),
    },
    ZmqOption {
        name: "ZMQ_BINDTODEVICE",
        scope: Sock,
        verdict: Refused(Absent(
            "SO_BINDTODEVICE is not set; an endpoint's interface is chosen by the address it \
             names",
        )),
    },
    ZmqOption {
        name: "ZMQ_IPV6",
        scope: Sock,
        verdict: Refused(Absent(
            "IPv6 is never disabled: every address a name resolves to is dialled whatever its \
             family, so there is no ZMQ_IPV6=0 to honour",
        )),
    },
    ZmqOption {
        name: "ZMQ_USE_FD",
        scope: Sock,
        verdict: Refused(Absent(
            "binding a file descriptor this library did not open; weida-runtime's bind hygiene \
             is what the ipc transport relies on and an adopted descriptor would bypass it",
        )),
    },
    // ---- patterns ------------------------------------------------------
    ZmqOption {
        name: "ZMQ_ROUTING_ID",
        scope: Sock,
        verdict: Honoured("SocketOptions::routing_id, 1-255 octets with a nonzero first one"),
    },
    ZmqOption {
        name: "ZMQ_IDENTITY",
        scope: Sock,
        verdict: Honoured(
            "SocketOptions::routing_id - libzmq's own deprecated name for ZMQ_ROUTING_ID, and \
             the wire property is still called Identity",
        ),
    },
    ZmqOption {
        name: "ZMQ_ROUTER_MANDATORY",
        scope: Sock,
        verdict: Honoured("SocketOptions::router_mandatory"),
    },
    ZmqOption {
        name: "ZMQ_ROUTER_HANDOVER",
        scope: Sock,
        verdict: Honoured("SocketOptions::router_handover"),
    },
    ZmqOption {
        name: "ZMQ_PROBE_ROUTER",
        scope: Sock,
        verdict: Honoured("SocketOptions::probe_router, refused on other socket types"),
    },
    ZmqOption {
        name: "ZMQ_ROUTER_NOTIFY",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_CONNECT_ROUTING_ID",
        scope: Sock,
        verdict: Refused(Absent(
            "assigning the next peer's routing id from the connecting side; a ROUTER here \
             addresses a peer by the identity it announced or by one this library gave it",
        )),
    },
    ZmqOption {
        name: "ZMQ_REQ_CORRELATE",
        scope: Sock,
        verdict: Honoured("SocketOptions::req_correlate"),
    },
    ZmqOption {
        name: "ZMQ_REQ_RELAXED",
        scope: Sock,
        verdict: Honoured("SocketOptions::req_relaxed, which requires req_correlate"),
    },
    ZmqOption {
        name: "ZMQ_SUBSCRIBE",
        scope: Sock,
        verdict: Honoured("SubSocket::subscribe and XSubSocket::subscribe"),
    },
    ZmqOption {
        name: "ZMQ_UNSUBSCRIBE",
        scope: Sock,
        verdict: Honoured("SubSocket::unsubscribe and XSubSocket::unsubscribe"),
    },
    ZmqOption {
        name: "ZMQ_XPUB_VERBOSE",
        scope: Sock,
        verdict: Honoured("SocketOptions::xpub_verbose"),
    },
    ZmqOption {
        name: "ZMQ_XPUB_VERBOSER",
        scope: Sock,
        verdict: Honoured("SocketOptions::xpub_verboser"),
    },
    ZmqOption {
        name: "ZMQ_XPUB_MANUAL",
        scope: Sock,
        verdict: Honoured("SocketOptions::xpub_manual, with XPubSocket::subscribe applying one"),
    },
    ZmqOption {
        name: "ZMQ_XPUB_WELCOME_MSG",
        scope: Sock,
        verdict: Honoured("SocketOptions::xpub_welcome_msg, sent on connect and reconnect"),
    },
    ZmqOption {
        name: "ZMQ_XPUB_MANUAL_LAST_VALUE",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_XPUB_NODROP",
        scope: Sock,
        verdict: Refused(Absent(
            "a publisher drops at the high-water mark, which 29/PUBSUB requires - \"SHALL \
             silently drop the message if the queue for a subscriber is full\"; reporting \
             EAGAIN to the publisher instead is not implemented",
        )),
    },
    ZmqOption {
        name: "ZMQ_INVERT_MATCHING",
        scope: Sock,
        verdict: Refused(Absent(
            "sending to everything except the matching subscribers, which must be set on both \
             ends to work; the matcher here matches prefixes and does not invert",
        )),
    },
    ZmqOption {
        name: "ZMQ_STREAM_NOTIFY",
        scope: Sock,
        verdict: Refused(Absent(
            "ZMQ_STREAM is not a socket type here, so there are no connect and disconnect \
             messages to switch on",
        )),
    },
    ZmqOption {
        name: "ZMQ_HELLO_MSG",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_DISCONNECT_MSG",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_HICCUP_MSG",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_METADATA",
        scope: Sock,
        verdict: Refused(Absent(
            "application metadata properties in the handshake; what this library sends is \
             Socket-Type and Identity, and a peer's other properties are readable rather than \
             settable",
        )),
    },
    ZmqOption {
        name: "ZMQ_IN_BATCH_SIZE",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    ZmqOption {
        name: "ZMQ_OUT_BATCH_SIZE",
        scope: Sock,
        verdict: Refused(DraftOnly),
    },
    // ---- security ------------------------------------------------------
    ZmqOption {
        name: "ZMQ_PLAIN_SERVER",
        scope: Sock,
        verdict: Honoured("SocketOptions::plain_server"),
    },
    ZmqOption {
        name: "ZMQ_PLAIN_USERNAME",
        scope: Sock,
        verdict: Honoured("SocketOptions::plain_username, which selects PLAIN as the client"),
    },
    ZmqOption {
        name: "ZMQ_PLAIN_PASSWORD",
        scope: Sock,
        verdict: Honoured("SocketOptions::plain_password"),
    },
    ZmqOption {
        name: "ZMQ_CURVE_SERVER",
        scope: Sock,
        verdict: Honoured("SocketOptions::curve_server"),
    },
    ZmqOption {
        name: "ZMQ_CURVE_PUBLICKEY",
        scope: Sock,
        verdict: Honoured("SocketOptions::curve_publickey, 32 octets or 40 characters of Z85"),
    },
    ZmqOption {
        name: "ZMQ_CURVE_SECRETKEY",
        scope: Sock,
        verdict: Honoured("SocketOptions::curve_secretkey, 32 octets or 40 characters of Z85"),
    },
    ZmqOption {
        name: "ZMQ_CURVE_SERVERKEY",
        scope: Sock,
        verdict: Honoured("SocketOptions::curve_serverkey, which selects CURVE as the client"),
    },
    ZmqOption {
        name: "ZMQ_ZAP_DOMAIN",
        scope: Sock,
        verdict: Honoured("SocketOptions::zap_domain, the switch that turns authorization on"),
    },
    ZmqOption {
        name: "ZMQ_ZAP_ENFORCE_DOMAIN",
        scope: Sock,
        verdict: Honoured("SocketOptions::zap_enforce_domain, checked at configuration time"),
    },
    ZmqOption {
        name: "ZMQ_GSSAPI_SERVER",
        scope: Sock,
        verdict: Refused(Absent(
            "the GSSAPI mechanism; this library announces NULL, PLAIN or CURVE in the greeting \
             and a mechanism it cannot speak must not be announced",
        )),
    },
    ZmqOption {
        name: "ZMQ_GSSAPI_PLAINTEXT",
        scope: Sock,
        verdict: Refused(Absent("the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER")),
    },
    ZmqOption {
        name: "ZMQ_GSSAPI_PRINCIPAL",
        scope: Sock,
        verdict: Refused(Absent("the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER")),
    },
    ZmqOption {
        name: "ZMQ_GSSAPI_SERVICE_PRINCIPAL",
        scope: Sock,
        verdict: Refused(Absent("the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER")),
    },
    ZmqOption {
        name: "ZMQ_GSSAPI_PRINCIPAL_NAMETYPE",
        scope: Sock,
        verdict: Refused(Absent("the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER")),
    },
    ZmqOption {
        name: "ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE",
        scope: Sock,
        verdict: Refused(Absent("the GSSAPI mechanism; see ZMQ_GSSAPI_SERVER")),
    },
    ZmqOption {
        name: "ZMQ_TCP_ACCEPT_FILTER",
        scope: Sock,
        verdict: Refused(ZapInstead),
    },
    ZmqOption {
        name: "ZMQ_IPC_FILTER_UID",
        scope: Sock,
        verdict: Refused(ZapInstead),
    },
    ZmqOption {
        name: "ZMQ_IPC_FILTER_GID",
        scope: Sock,
        verdict: Refused(ZapInstead),
    },
    ZmqOption {
        name: "ZMQ_IPC_FILTER_PID",
        scope: Sock,
        verdict: Refused(ZapInstead),
    },
    // ---- transports this library does not have -------------------------
    ZmqOption {
        name: "ZMQ_RATE",
        scope: Sock,
        verdict: Refused(NoTransport("pgm, epgm or norm multicast")),
    },
    ZmqOption {
        name: "ZMQ_RECOVERY_IVL",
        scope: Sock,
        verdict: Refused(NoTransport("pgm, epgm or norm multicast")),
    },
    ZmqOption {
        name: "ZMQ_MULTICAST_HOPS",
        scope: Sock,
        verdict: Refused(NoTransport("pgm, epgm or udp multicast")),
    },
    ZmqOption {
        name: "ZMQ_MULTICAST_MAXTPDU",
        scope: Sock,
        verdict: Refused(NoTransport("pgm or epgm multicast")),
    },
    ZmqOption {
        name: "ZMQ_SOCKS_PROXY",
        scope: Sock,
        verdict: Refused(NoTransport("a SOCKS5 client for outbound tcp")),
    },
    ZmqOption {
        name: "ZMQ_WSS_KEY_PEM",
        scope: Sock,
        verdict: Refused(NoTransport(
            "ws or wss, which is DRAFT in libzmq and needs GnuTLS",
        )),
    },
    ZmqOption {
        name: "ZMQ_WSS_CERT_PEM",
        scope: Sock,
        verdict: Refused(NoTransport("ws or wss")),
    },
    ZmqOption {
        name: "ZMQ_WSS_TRUST_PEM",
        scope: Sock,
        verdict: Refused(NoTransport("ws or wss")),
    },
    ZmqOption {
        name: "ZMQ_WSS_HOSTNAME",
        scope: Sock,
        verdict: Refused(NoTransport("ws or wss")),
    },
    ZmqOption {
        name: "ZMQ_WSS_TRUST_SYSTEM",
        scope: Sock,
        verdict: Refused(NoTransport("ws or wss")),
    },
    ZmqOption {
        name: "ZMQ_VMCI_BUFFER_SIZE",
        scope: Sock,
        verdict: Refused(NoTransport("vmci")),
    },
    ZmqOption {
        name: "ZMQ_VMCI_BUFFER_MIN_SIZE",
        scope: Sock,
        verdict: Refused(NoTransport("vmci")),
    },
    ZmqOption {
        name: "ZMQ_VMCI_BUFFER_MAX_SIZE",
        scope: Sock,
        verdict: Refused(NoTransport("vmci")),
    },
    ZmqOption {
        name: "ZMQ_VMCI_CONNECT_TIMEOUT",
        scope: Sock,
        verdict: Refused(NoTransport("vmci")),
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ContextConfig, DEFAULT_CLOSE_BUDGET};
    use crate::message::DEFAULT_MAX_MESSAGE_SIZE;
    use crate::options::SocketOptions;

    /// Claim: the table is walkable and every row decides something — a
    /// honoured row names where the option lives, a refused row produces
    /// `EINVAL` naming the reason, and no row is silent or duplicated.
    #[test]
    fn every_option_is_honoured_or_refused() {
        assert!(OPTIONS.len() > 80, "the table is {}", OPTIONS.len());
        let mut honoured = 0;
        let mut refused = 0;
        for option in OPTIONS {
            assert!(
                option.name.starts_with("ZMQ_"),
                "{} is not a libzmq name",
                option.name
            );
            assert_eq!(
                OPTIONS
                    .iter()
                    .filter(|other| other.name == option.name)
                    .count(),
                1,
                "{} appears twice",
                option.name
            );
            match option.verdict {
                Verdict::Honoured(binding) => {
                    honoured += 1;
                    assert!(!binding.is_empty(), "{} names nothing", option.name);
                    assert_eq!(
                        super::honoured(option.name).expect("honoured"),
                        binding,
                        "{}",
                        option.name
                    );
                    assert!(option.refusal().is_none(), "{}", option.name);
                }
                Verdict::Refused(reason) => {
                    refused += 1;
                    let err = option.refusal().expect("a reason");
                    assert_eq!(err.errno(), "EINVAL", "{}: {err}", option.name);
                    assert!(
                        err.cause().contains(option.name),
                        "{} does not name itself: {err}",
                        option.name
                    );
                    // The reason is in the message, not only in the type.
                    let head = match reason {
                        Refusal::NoTransport(_) => "no transport",
                        Refusal::DraftOnly => "draft only",
                        Refusal::ZapInstead => "deprecated in favour of ZAP",
                        Refusal::RuntimeInstead(_) => "replaced by a weida-runtime construct",
                        Refusal::Absent(_) => "absent",
                    };
                    assert!(
                        err.cause().contains(head),
                        "{} does not name its reason: {err}",
                        option.name
                    );
                    assert!(
                        super::honoured(option.name).is_err(),
                        "{} is refused and honoured at once",
                        option.name
                    );
                }
            }
        }
        // Both halves are populated: a table that refused everything, or
        // honoured everything, would pass the per-row assertions above.
        assert!(honoured >= 35, "only {honoured} honoured");
        assert!(refused >= 40, "only {refused} refused");

        // Every reason is used by at least one row; an unused variant would
        // be a category nobody needed.
        for head in [
            "no transport",
            "draft only",
            "deprecated in favour of ZAP",
            "replaced by a weida-runtime construct",
            "absent",
        ] {
            assert!(
                OPTIONS.iter().any(|option| option
                    .refusal()
                    .is_some_and(|err| err.cause().contains(head))),
                "no row is refused for: {head}"
            );
        }
    }

    /// Claim: a name that is not an option — including libzmq's read-only
    /// ones — is refused rather than accepted by accident.
    #[test]
    fn a_name_outside_the_table_is_refused() {
        for name in ["ZMQ_EVENTS", "ZMQ_FD", "ZMQ_SOCKET_LIMIT", "ZMQ_NONSENSE"] {
            let err = super::honoured(name).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");
            assert!(err.cause().contains(name), "{err}");
            assert!(option(name).is_none(), "{name}");
        }
    }

    /// Claim: the two deliberate default changes are what the table says they
    /// are, and are deviations rather than restatements of libzmq.
    #[test]
    fn the_two_deliberate_defaults_differ_from_libzmq() {
        // ZMQ_LINGER: libzmq's default is -1, infinite. Here it is a finite
        // budget, and the table's row for ZMQ_LINGER says so.
        assert_eq!(
            ContextConfig::default().close_budget,
            DEFAULT_CLOSE_BUDGET,
            "the default close budget is the documented one"
        );
        assert!(
            DEFAULT_CLOSE_BUDGET > std::time::Duration::ZERO,
            "a zero budget would discard rather than linger"
        );
        let linger = option("ZMQ_LINGER").expect("in the table");
        let Verdict::Honoured(binding) = linger.verdict else {
            panic!("ZMQ_LINGER is honoured");
        };
        assert!(binding.contains("close_budget"), "{binding}");
        assert!(binding.contains("finite"), "{binding}");

        // ZMQ_MAXMSGSIZE: libzmq's default is -1, no limit. Here it is a
        // number, and it is the one the socket options actually start with.
        assert_eq!(
            SocketOptions::default().max_message_size,
            DEFAULT_MAX_MESSAGE_SIZE
        );
        assert!(DEFAULT_MAX_MESSAGE_SIZE < i64::MAX as u64);
        let max = option("ZMQ_MAXMSGSIZE").expect("in the table");
        let Verdict::Honoured(binding) = max.verdict else {
            panic!("ZMQ_MAXMSGSIZE is honoured");
        };
        assert!(binding.contains("max_message_size"), "{binding}");
        assert!(binding.contains("bounded by default"), "{binding}");
    }

    /// Claim: every option this library's own types carry appears in the
    /// table as honoured. The table is the index into the options, so an
    /// option that exists and is not listed is a silence of another kind.
    #[test]
    fn the_options_this_library_has_are_all_listed() {
        for name in [
            "ZMQ_SNDHWM",
            "ZMQ_RCVHWM",
            "ZMQ_MAXMSGSIZE",
            "ZMQ_LINGER",
            "ZMQ_SNDTIMEO",
            "ZMQ_RCVTIMEO",
            "ZMQ_RECONNECT_IVL",
            "ZMQ_RECONNECT_IVL_MAX",
            "ZMQ_HANDSHAKE_IVL",
            "ZMQ_CONNECT_TIMEOUT",
            "ZMQ_IMMEDIATE",
            "ZMQ_BACKLOG",
            "ZMQ_HEARTBEAT_IVL",
            "ZMQ_HEARTBEAT_TIMEOUT",
            "ZMQ_HEARTBEAT_TTL",
            "ZMQ_TCP_KEEPALIVE",
            "ZMQ_TCP_KEEPALIVE_IDLE",
            "ZMQ_TCP_KEEPALIVE_CNT",
            "ZMQ_TCP_KEEPALIVE_INTVL",
            "ZMQ_ROUTING_ID",
            "ZMQ_IDENTITY",
            "ZMQ_ROUTER_MANDATORY",
            "ZMQ_ROUTER_HANDOVER",
            "ZMQ_PROBE_ROUTER",
            "ZMQ_REQ_CORRELATE",
            "ZMQ_REQ_RELAXED",
            "ZMQ_SUBSCRIBE",
            "ZMQ_UNSUBSCRIBE",
            "ZMQ_XPUB_VERBOSE",
            "ZMQ_XPUB_VERBOSER",
            "ZMQ_XPUB_MANUAL",
            "ZMQ_XPUB_WELCOME_MSG",
            "ZMQ_PLAIN_SERVER",
            "ZMQ_PLAIN_USERNAME",
            "ZMQ_PLAIN_PASSWORD",
            "ZMQ_CURVE_SERVER",
            "ZMQ_CURVE_PUBLICKEY",
            "ZMQ_CURVE_SECRETKEY",
            "ZMQ_CURVE_SERVERKEY",
            "ZMQ_ZAP_DOMAIN",
            "ZMQ_ZAP_ENFORCE_DOMAIN",
            "ZMQ_MAX_SOCKETS",
        ] {
            super::honoured(name).unwrap_or_else(|e| panic!("{name} should be honoured: {e}"));
        }
    }
}
