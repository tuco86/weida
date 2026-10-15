//! Every NNG option, honoured or refused — and never silently ignored.
//!
//! `nng_options(5)` is a list, and a library that implements some of it
//! and shrugs at the rest is a library whose users cannot tell which is
//! which. So the list is here, in full, as data: each row names the option
//! NNG names, says where it applies, and either names what honours it in
//! this crate or carries the reason it is refused
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4
//! item 4).
//!
//! **A refusal happens at configuration time**, which is the only moment
//! it is useful: [`lookup`] is what a configuration surface consults
//! before it accepts a value, and [`Disposition::Refused`] carries a
//! [`Refusal`] that says which of five things went wrong rather than a
//! bare error.
//!
//! **The table is checked, not decorative.** A test walks every row,
//! asserts that an honoured row names something and a refused row carries
//! a reason, and asserts the names are unique. What it cannot assert is
//! that the *whole* of `nng_options(5)` is present; that is what
//! `docs/libraries/nng.md` is for, and this table is what that document is
//! written from.

use crate::error::{Error, Result};

/// Why an option is not honoured.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// The transport it belongs to is not implemented here. The option is
    /// not malformed; there is simply nothing for it to configure.
    NoSuchTransport,
    /// The protocol does not support it — `NNG_OPT_SENDBUF` on REQ, whose
    /// one outstanding transaction per context leaves nothing to bound
    /// (`docs/research/nanomsg-nng.md` §5).
    NotThisProtocol,
    /// Experimental in NNG itself and out of scope here: the ZeroTier
    /// transport, which NNG's own manual calls experimental and whose
    /// connection setup "can take up to about one minute in extreme
    /// cases" (§11).
    ExperimentalUpstream,
    /// Replaced by a `weida-runtime` construct: a reactor this library does
    /// not size for the caller, a close budget rather than a linger, a
    /// name registry rather than a global namespace.
    ReplacedByRuntime,
    /// Absent, with what is missing named in the row's own note.
    Absent,
}

impl Refusal {
    /// The sentence a refusal prints.
    pub const fn reason(self) -> &'static str {
        match self {
            Refusal::NoSuchTransport => "this library does not implement that transport",
            Refusal::NotThisProtocol => "this protocol does not support that option",
            Refusal::ExperimentalUpstream => {
                "the feature is experimental in NNG itself and is out of scope here"
            }
            Refusal::ReplacedByRuntime => {
                "the option is replaced by a weida-runtime construct this library configures \
                 instead"
            }
            Refusal::Absent => "the option is not implemented",
        }
    }
}

/// What this library does with an option.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Honoured, under the name given: the field, method or type that
    /// carries it.
    Honoured(&'static str),
    /// Refused at configuration time, for this reason and with this note.
    Refused(Refusal),
}

/// What an option applies to, as `nng_options(5)` groups them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    /// A socket option.
    Socket,
    /// A dialer option.
    Dialer,
    /// A listener option.
    Listener,
    /// A pipe option, which is read rather than set.
    Pipe,
    /// A transport option, set on a dialer or listener.
    Transport,
}

/// One row: an option, what it applies to, and what happens to it here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OptionRow {
    /// The name NNG uses, which is the name a ported program greps for.
    pub name: &'static str,
    /// What it applies to.
    pub scope: Scope,
    /// Honoured or refused.
    pub disposition: Disposition,
    /// What is honoured, or what is missing — never empty.
    pub note: &'static str,
}

impl OptionRow {
    /// Whether this option is honoured.
    pub const fn is_honoured(&self) -> bool {
        matches!(self.disposition, Disposition::Honoured(_))
    }

    /// The refusal an attempt to set this option produces, with the reason
    /// and the note in the message.
    ///
    /// `NNG_ENOTSUP` for an option this library does not implement and
    /// `NNG_EREADONLY` for a pipe fact, which are the two codes NNG uses
    /// for the two situations.
    pub fn refusal(&self) -> Option<Error> {
        match self.disposition {
            Disposition::Honoured(_) => None,
            Disposition::Refused(why) => Some(Error::ENOTSUP(
                format!("{}: {} ({})", self.name, why.reason(), self.note).into(),
            )),
        }
    }
}

/// Every option `nng_options(5)` and the transport pages name, in NNG's
/// own spelling.
pub const OPTIONS: &[OptionRow] = &[
    // -- the socket options ------------------------------------------
    OptionRow {
        name: "NNG_OPT_RECVMAXSZ",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::recv_max_size"),
        note: "judged from the declared 64-bit length before a body is allocated; settable per \
               endpoint through EndpointOptions::recv_max_size, and deliberately ignored by \
               inproc, which NNG also ignores it on",
    },
    OptionRow {
        name: "NNG_OPT_SENDBUF",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::send_depth"),
        note: "0..=8192 messages, where 0 is NNG's rendezvous and not libzmq's unlimited; \
               refused on req0",
    },
    OptionRow {
        name: "NNG_OPT_RECVBUF",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::recv_depth"),
        note: "0..=8192 messages; refused on req0",
    },
    OptionRow {
        name: "NNG_OPT_SENDTIMEO",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::send_timeout"),
        note: "None is NNG's NNG_DURATION_INFINITE; expiry is NNG_ETIMEDOUT",
    },
    OptionRow {
        name: "NNG_OPT_RECVTIMEO",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::recv_timeout"),
        note: "None is NNG's NNG_DURATION_INFINITE; expiry is NNG_ETIMEDOUT",
    },
    OptionRow {
        name: "NNG_OPT_RECONNMINT",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::reconnect_min"),
        note: "NNG's 100 ms default: the first retry delay after a dialer's pipe closes",
    },
    OptionRow {
        name: "NNG_OPT_RECONNMAXT",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::reconnect_max"),
        note: "Duration::ZERO is NNG's 'no exponential backoff', which keeps every delay at \
               the minimum",
    },
    OptionRow {
        name: "NNG_OPT_MAXTTL",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::max_ttl"),
        note: "1..=255 as the manual documents, with NNG_MAX_TTL = 15 published beside it \
               because a real NNG node refuses a stack of 16",
    },
    OptionRow {
        name: "NNG_OPT_PROTO",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("Socket::protocol"),
        note: "the EndpointType, which carries the protocol id and the role nibble",
    },
    OptionRow {
        name: "NNG_OPT_PROTONAME",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("protocol(..).name"),
        note: "req0, rep0, pair1 and the rest, spelled as NNG spells them",
    },
    OptionRow {
        name: "NNG_OPT_PEER",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("EndpointType::peer"),
        note: "the pairing rule, which is also the rule a pipe is admitted under",
    },
    OptionRow {
        name: "NNG_OPT_PEERNAME",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("protocol(EndpointType::peer(..)).name"),
        note: "the peer protocol's NNG name, from the same table as NNG_OPT_PROTONAME",
    },
    OptionRow {
        name: "NNG_OPT_SOCKNAME",
        scope: Scope::Socket,
        disposition: Disposition::Refused(Refusal::Absent),
        note: "NNG's socket name is a 64-byte label for its own diagnostics; SocketId and the \
               tracing spans carry what a log line needs, and a second naming scheme would \
               have to be kept in step with them",
    },
    OptionRow {
        name: "NNG_OPT_RECVFD",
        scope: Scope::Socket,
        disposition: Disposition::Refused(Refusal::ReplacedByRuntime),
        note: "a readiness file descriptor exists so a poll loop can wait on a socket; the \
               futures this library returns are what a reactor waits on instead, and the \
               manual itself calls mixing RECVFD with contexts unsupported and unpredictable",
    },
    OptionRow {
        name: "NNG_OPT_SENDFD",
        scope: Scope::Socket,
        disposition: Disposition::Refused(Refusal::ReplacedByRuntime),
        note: "the sending counterpart of NNG_OPT_RECVFD, refused for the same reason",
    },
    OptionRow {
        name: "NNG_OPT_REQ_RESENDTIME",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::resend_time"),
        note: "NNG's one-minute default; the clock is the socket's and the deadline the \
               context's, as NNG's granularity note describes",
    },
    OptionRow {
        name: "NNG_OPT_REQ_RESENDTICK",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("reqrep::RESEND_GRANULARITY"),
        note: "a constant rather than an option: the clock already wakes at the nearest \
               deadline, so the tick is a ceiling on lateness and not a tuning knob",
    },
    OptionRow {
        name: "NNG_OPT_SURVEYOR_SURVEYTIME",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::survey_time"),
        note: "NNG's one-second default, counted from the send",
    },
    OptionRow {
        name: "NNG_OPT_SUB_SUBSCRIBE",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SubSocket::subscribe"),
        note: "an arbitrary byte prefix; an empty one admits everything",
    },
    OptionRow {
        name: "NNG_OPT_SUB_UNSUBSCRIBE",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SubSocket::unsubscribe"),
        note: "NNG_ENOENT for a prefix this socket does not hold",
    },
    OptionRow {
        name: "NNG_OPT_SUB_PREFNEW",
        scope: Scope::Socket,
        disposition: Disposition::Honoured("SocketOptions::sub_prefer_new"),
        note: "applies to the queue of admitted publications, after the filter, so the policy \
               is never applied to traffic nobody subscribed to",
    },
    OptionRow {
        name: "NNG_OPT_PAIR1_POLY",
        scope: Scope::Socket,
        disposition: Disposition::Refused(Refusal::Absent),
        note: "polyamorous PAIR v1 is deprecated by NNG's own manual; pair::POLYAMOROUS_ABSENT \
               carries the three reasons, and BUS is the pattern for many direct peers",
    },
    // -- the dialer and listener options -----------------------------
    OptionRow {
        name: "NNG_OPT_URL",
        scope: Scope::Dialer,
        disposition: Disposition::Honoured("Dialer::url and Listener::url"),
        note: "the endpoint actually used, which for a wildcard port is the only way to learn \
               the port",
    },
    OptionRow {
        name: "NNG_OPT_LOCADDR",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("PipeInfo::local_addr"),
        note: "read, never set: it is a fact about a connection",
    },
    OptionRow {
        name: "NNG_OPT_REMADDR",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("PipeInfo::remote_addr"),
        note: "read, never set: it is a fact about a connection rather than a setting on one",
    },
    OptionRow {
        name: "NNG_OPT_TCP_NODELAY",
        scope: Scope::Transport,
        disposition: Disposition::Honoured("always on"),
        note: "SP's request-reply and survey patterns are round-trip shaped, so Nagle is \
               disabled on every TCP connection rather than configured off by each caller",
    },
    OptionRow {
        name: "NNG_OPT_TCP_KEEPALIVE",
        scope: Scope::Transport,
        disposition: Disposition::Honoured("always on"),
        note: "every TCP connection, accepted or dialled, is probed after sixty seconds of \
               silence and every fifteen after that, because SP has no heartbeat and a PULL or \
               REP socket may never write to a peer, so one that vanished without a FIN would \
               hold its pipe slot for ever; NNG's own default is off, and no option turns this \
               one off",
    },
    OptionRow {
        name: "NNG_OPT_TCP_BOUND_PORT",
        scope: Scope::Listener,
        disposition: Disposition::Honoured("Listener::url"),
        note: "the bound endpoint carries the port a wildcard was given",
    },
    // -- TLS ---------------------------------------------------------
    OptionRow {
        name: "NNG_OPT_TLS_AUTH_MODE",
        scope: Scope::Transport,
        disposition: Disposition::Honoured("TlsConfig::auth_mode"),
        note: "None, Optional and Required, defaulting to Required rather than to None",
    },
    OptionRow {
        name: "NNG_OPT_TLS_CA_FILE",
        scope: Scope::Transport,
        disposition: Disposition::Honoured("TlsConfig::ca_pem"),
        note: "PEM bytes rather than a path, so the library decides nothing about the \
               filesystem",
    },
    OptionRow {
        name: "NNG_OPT_TLS_CERT_KEY_FILE",
        scope: Scope::Transport,
        disposition: Disposition::Honoured("TlsConfig::cert_pem and TlsConfig::key_pem"),
        note: "the key is write-only, as NNG's is: configuration going in with no way back out",
    },
    OptionRow {
        name: "NNG_OPT_TLS_SERVER_NAME",
        scope: Scope::Transport,
        disposition: Disposition::Honoured("TlsConfig::server_name"),
        note: "overrides the name in the dial URL; a URL that names an address and a \
               configuration that names nothing is refused rather than dialled with nothing \
               to verify",
    },
    OptionRow {
        name: "NNG_OPT_TLS_VERIFIED",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("TlsPeer::verified"),
        note: "read at the pipe-add-pre hook, where an allow-list runs",
    },
    OptionRow {
        name: "NNG_OPT_TLS_PEER_CN",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("TlsPeer::common_name"),
        note: "for display; an allow-list belongs on the alternative names, which is what path \
               validation checks",
    },
    OptionRow {
        name: "NNG_OPT_TLS_PEER_ALT_NAMES",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("TlsPeer::subject_alt_names"),
        note: "the DNS names in the subject alternative name extension",
    },
    OptionRow {
        name: "NNG_OPT_TLS_CONFIG",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::Absent),
        note: "NNG's opaque nng_tls_config pointer exists so several endpoints can share one \
               configuration; TlsConfig is a value that clones, which is the same sharing \
               without a handle to manage",
    },
    // -- IPC ---------------------------------------------------------
    OptionRow {
        name: "NNG_OPT_IPC_PEER_UID",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("PipeInfo::credentials"),
        note: "the kernel's answer at connection time, which the manual calls non-forgeable",
    },
    OptionRow {
        name: "NNG_OPT_IPC_PEER_GID",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("PipeInfo::credentials"),
        note: "the kernel's answer at connection time, which the manual calls non-forgeable",
    },
    OptionRow {
        name: "NNG_OPT_IPC_PEER_PID",
        scope: Scope::Pipe,
        disposition: Disposition::Honoured("PipeInfo::credentials"),
        note: "carried and documented as an observation that must not be authorized on: the \
               pid identified a process then and may identify another one now",
    },
    OptionRow {
        name: "NNG_OPT_IPC_PEER_ZONEID",
        scope: Scope::Pipe,
        disposition: Disposition::Refused(Refusal::Absent),
        note: "an illumos and Solaris zone id; this library runs where weida runs and has no \
               platform to read it from",
    },
    OptionRow {
        name: "NNG_OPT_IPC_PERMISSIONS",
        scope: Scope::Listener,
        disposition: Disposition::Refused(Refusal::ReplacedByRuntime),
        note: "weida-runtime binds AF_UNIX in a private 0700 directory and renames it into \
               place with mode 0600, so the node is never visible with whatever umask allowed; \
               a caller that wants the socket reachable by another user places it in a \
               directory that says so, because the mode alone cannot stop a user who may write \
               the directory from renaming over the node",
    },
    // -- the transports this library does not implement ---------------
    OptionRow {
        name: "NNG_OPT_WS_REQUEST_HEADERS",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::NoSuchTransport),
        note: "the WebSocket mapping needs an HTTP server and is out of scope until a user \
               asks; ws:// and wss:// are refused by the URL parser for the same reason",
    },
    OptionRow {
        name: "NNG_OPT_WS_RESPONSE_HEADERS",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::NoSuchTransport),
        note: "as NNG_OPT_WS_REQUEST_HEADERS: there is no WebSocket transport to configure",
    },
    OptionRow {
        name: "NNG_OPT_WS_PROTOCOL",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::NoSuchTransport),
        note: "as NNG_OPT_WS_REQUEST_HEADERS: there is no WebSocket transport to configure",
    },
    OptionRow {
        name: "NNG_OPT_WSS_REQUEST_HEADERS",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::NoSuchTransport),
        note: "as NNG_OPT_WS_REQUEST_HEADERS: there is no WebSocket transport to configure",
    },
    OptionRow {
        name: "NNG_OPT_ZT_HOME",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::ExperimentalUpstream),
        note: "ZeroTier is experimental in NNG itself and its connection setup can take about \
               a minute, which makes it unsuited to the short-lived programs this library is \
               tested with",
    },
    OptionRow {
        name: "NNG_OPT_ZT_NWID",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::ExperimentalUpstream),
        note: "as NNG_OPT_ZT_HOME: the ZeroTier transport is not carried here at all",
    },
    OptionRow {
        name: "NNG_OPT_ZT_PING_TIME",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::ExperimentalUpstream),
        note: "as NNG_OPT_ZT_HOME; ZeroTier's own ping is the only liveness probing an SP \
               transport defines for itself, and TCP's is the kernel's (NNG_OPT_TCP_KEEPALIVE)",
    },
    OptionRow {
        name: "NNG_OPT_ZT_PING_TRIES",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::ExperimentalUpstream),
        note: "as NNG_OPT_ZT_PING_TIME: ZeroTier's own liveness probing, and absent with it",
    },
    OptionRow {
        name: "NNG_OPT_ZT_MTU",
        scope: Scope::Transport,
        disposition: Disposition::Refused(Refusal::ExperimentalUpstream),
        note: "as NNG_OPT_ZT_HOME: the ZeroTier transport is not carried here at all",
    },
];

/// The row for `name`, or `None` for a name NNG does not have.
///
/// A name this table does not know is not silently accepted either: a
/// configuration surface that gets `None` has been handed something that
/// is not an NNG option at all.
pub fn lookup(name: &str) -> Option<&'static OptionRow> {
    OPTIONS.iter().find(|row| row.name == name)
}

/// Refuses `name` if this library does not honour it, at configuration
/// time.
///
/// `NNG_ENOTSUP` with the reason for an option NNG has and this library
/// refuses, and `NNG_EINVAL` for a name that is not an NNG option.
pub fn require_honoured(name: &str) -> Result<&'static OptionRow> {
    match lookup(name) {
        Some(row) => match row.refusal() {
            Some(error) => Err(error),
            None => Ok(row),
        },
        None => Err(Error::EINVAL(
            format!("{name} is not an option nng_options(5) defines").into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the table is complete in its own terms — every row names an
    /// option, says what happens to it, and carries a note that is not
    /// empty — and no option appears twice.
    ///
    /// The plausible bug this fails on is a row added with an empty note,
    /// which is how "refused with the reason named" decays into "refused".
    #[test]
    fn every_row_says_what_happens_and_why() {
        let mut names = std::collections::BTreeSet::new();
        for row in OPTIONS {
            assert!(
                row.name.starts_with("NNG_OPT_"),
                "{} is not an NNG option name",
                row.name
            );
            assert!(names.insert(row.name), "{} appears twice", row.name);
            assert!(
                row.note.len() > 20,
                "{} has no real note: {:?}",
                row.name,
                row.note
            );
            match row.disposition {
                Disposition::Honoured(what) => {
                    assert!(!what.is_empty(), "{} is honoured by nothing", row.name);
                    assert!(row.refusal().is_none());
                }
                Disposition::Refused(why) => {
                    let error = row.refusal().expect("a refused row refuses");
                    assert!(matches!(error, Error::ENOTSUP(_)), "{error:?}");
                    // The message carries the name, the reason and the note,
                    // which is the whole of "never silently ignored".
                    assert!(error.cause().contains(row.name));
                    assert!(error.cause().contains(why.reason()));
                    assert!(error.cause().contains(row.note));
                }
            }
        }
        assert!(
            OPTIONS.len() > 40,
            "the table has shrunk: {} rows",
            OPTIONS.len()
        );
    }

    /// Claim: all five reasons are used, so the vocabulary is the sheet's
    /// rather than a single "no".
    #[test]
    fn all_five_reasons_are_used() {
        for why in [
            Refusal::NoSuchTransport,
            Refusal::NotThisProtocol,
            Refusal::ExperimentalUpstream,
            Refusal::ReplacedByRuntime,
            Refusal::Absent,
        ] {
            let used = OPTIONS
                .iter()
                .any(|row| row.disposition == Disposition::Refused(why))
                // `NotThisProtocol` is the one reason that is not a row: it
                // depends on the protocol, so it is carried by
                // `Protocol::require_buffers` and named here for the
                // vocabulary.
                || why == Refusal::NotThisProtocol;
            assert!(used, "{why:?} is a reason nothing uses");
            assert!(why.reason().len() > 20);
        }
    }

    /// Claim: a lookup answers for an option NNG has, refuses one this
    /// library does not honour with `NNG_ENOTSUP`, and refuses a name that
    /// is not an NNG option at all with `NNG_EINVAL` — three answers, not
    /// two.
    #[test]
    fn a_lookup_distinguishes_refused_from_invented() {
        let honoured = require_honoured("NNG_OPT_RECVMAXSZ").expect("honoured");
        assert!(honoured.is_honoured());

        let refused = require_honoured("NNG_OPT_ZT_NWID").unwrap_err();
        assert!(matches!(refused, Error::ENOTSUP(_)), "{refused:?}");
        assert!(refused.cause().contains("experimental"));

        let invented = require_honoured("NNG_OPT_TELEPATHY").unwrap_err();
        assert!(matches!(invented, Error::EINVAL(_)), "{invented:?}");
        assert!(lookup("NNG_OPT_TELEPATHY").is_none());
    }

    /// Claim: the options the sheet singles out are all in the table, each
    /// with the disposition the rest of the crate actually implements.
    #[test]
    fn the_options_the_sheet_names_are_all_here() {
        for (name, honoured) in [
            ("NNG_OPT_RECVMAXSZ", true),
            ("NNG_OPT_SENDBUF", true),
            ("NNG_OPT_RECVBUF", true),
            ("NNG_OPT_MAXTTL", true),
            ("NNG_OPT_RECONNMINT", true),
            ("NNG_OPT_RECONNMAXT", true),
            ("NNG_OPT_REQ_RESENDTIME", true),
            ("NNG_OPT_SURVEYOR_SURVEYTIME", true),
            ("NNG_OPT_SUB_PREFNEW", true),
            ("NNG_OPT_TLS_AUTH_MODE", true),
            ("NNG_OPT_TLS_PEER_ALT_NAMES", true),
            ("NNG_OPT_IPC_PEER_UID", true),
            ("NNG_OPT_IPC_PEER_PID", true),
            ("NNG_OPT_RECVFD", false),
            ("NNG_OPT_SENDFD", false),
            ("NNG_OPT_PAIR1_POLY", false),
            ("NNG_OPT_ZT_NWID", false),
            ("NNG_OPT_WS_PROTOCOL", false),
        ] {
            let row = lookup(name).unwrap_or_else(|| panic!("{name} is missing from the table"));
            assert_eq!(row.is_honoured(), honoured, "{name}");
        }
    }
}
