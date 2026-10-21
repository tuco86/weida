//! Endpoints: `transport://address`, parsed and bounded.
//!
//! Three transports, which are ZeroMQ's working set: `tcp`, `ipc` and
//! `inproc` (`docs/research/zeromq.md` §13,
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.7 item 2).
//! Every other ZeroMQ transport is named absent rather than treated as a typo
//! — a caller who wrote `epgm://` made a supportable request that this
//! library refuses, and [`Error::EPROTONOSUPPORT`] is exactly libzmq's answer
//! to it.
//!
//! Parsing happens **before** anything is bound or dialled, and all three
//! length rules are libzmq's own numbers rather than ours, so that a program
//! ported from libzmq meets the same refusals at the same points
//! (`docs/research/zeromq.md` §11).

use std::fmt;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::error::{Error, Result};

/// Longest `inproc://` **name**, in bytes.
///
/// libzmq: an `inproc` name "may be up to 256 characters"
/// (`zmq_inproc(7)`, `docs/research/zeromq.md` §11). weida bounds its own bus
/// names at the same number for the same reason
/// ([0010](../../../docs/decisions/0010-local-transport.md) §4.8), which is
/// why [`weida_core::MAX_BUS_BYTES`] is this number too.
pub const MAX_INPROC_NAME_BYTES: usize = 256;

/// Longest `ipc://` **endpoint string**, in bytes, including the scheme.
///
/// libzmq: "On Linux, the maximum is 113 characters including the `ipc://`
/// prefix" (`zmq_ipc(7)`, `docs/research/zeromq.md` §11). The number is the
/// kernel's and not ZeroMQ's: `sun_path` holds 108 octets on Linux including
/// the terminator, so 107 path bytes, and 107 + `"ipc://"`.len() == 113 —
/// which is exactly [`weida_core::MAX_SOCKET_PATH_BYTES`] on this platform,
/// derived independently in `docs/research/ipc.md` §1.1. The budget is
/// enforced on the whole endpoint string, as libzmq states it, so a program
/// ported from libzmq is refused at the same character.
pub const MAX_IPC_ENDPOINT_BYTES: usize = 113;

/// The `ipc://` scheme, whose length counts against [`MAX_IPC_ENDPOINT_BYTES`].
const IPC_SCHEME: &str = "ipc://";

/// ZeroMQ transports this library does not implement, each with the reason
/// its absence is a decision rather than an oversight.
const ABSENT_TRANSPORTS: &[(&str, &str)] = &[
    (
        "pgm",
        "reliable multicast is out of scope until a user asks",
    ),
    (
        "epgm",
        "reliable multicast is out of scope until a user asks",
    ),
    ("udp", "the multicast/datagram family is out of scope"),
    (
        "ws",
        "WebSockets are a draft-gated libzmq feature and out of scope",
    ),
    (
        "wss",
        "WebSockets are a draft-gated libzmq feature and out of scope",
    ),
    ("vmci", "hypervisor transport, out of scope"),
    ("tipc", "cluster transport, out of scope"),
    ("vsock", "hypervisor transport, out of scope"),
];

/// What a `tcp://` endpoint names as its host.
///
/// libzmq accepts three shapes and this keeps them apart, because they mean
/// different things at bind time: a wildcard binds every interface, a literal
/// needs no resolver, and a name may be a hostname *or* an interface name
/// (`tcp://eth0:6000`) and is resolved where it is used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpHost {
    /// `*`: every interface. `INADDR_ANY`, and only meaningful for a bind.
    Any,
    /// An IPv4 or IPv6 literal. An IPv6 literal is written in brackets:
    /// `tcp://[::1]:5555`.
    Ip(IpAddr),
    /// A hostname or an interface name, resolved where it is used.
    Name(String),
}

impl fmt::Display for TcpHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TcpHost::Any => f.write_str("*"),
            TcpHost::Ip(ip) if ip.is_ipv6() => write!(f, "[{ip}]"),
            TcpHost::Ip(ip) => write!(f, "{ip}"),
            TcpHost::Name(name) => f.write_str(name),
        }
    }
}

/// A parsed ZeroMQ endpoint.
///
/// `Display` renders the canonical string again, which is what
/// `ZMQ_LAST_ENDPOINT` will report once binding exists — and the reason a
/// wildcard port has to be read back at all: a caller that binds
/// `tcp://*:*` cannot know the port it got without asking
/// (`docs/research/zeromq.md` §2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    /// `tcp://host:port`. Port `0` is the wildcard the OS fills in.
    Tcp {
        /// The interface, literal or name this endpoint names.
        host: TcpHost,
        /// The port, or `0` for "whatever the OS gives me".
        port: u16,
    },
    /// `ipc://path`: an `AF_UNIX` socket on the filesystem.
    Ipc(PathBuf),
    /// `inproc://name`: a name in the context's own namespace. Two contexts
    /// using the same name never meet (`docs/research/zeromq.md` §2).
    Inproc(String),
}

impl Endpoint {
    /// Parses `endpoint`, refusing everything libzmq refuses and naming why.
    pub fn parse(endpoint: &str) -> Result<Endpoint> {
        let Some((transport, address)) = endpoint.split_once("://") else {
            return Err(Error::EINVAL(
                format!("an endpoint must be transport://address: {endpoint:?}").into(),
            ));
        };
        match transport {
            "tcp" => parse_tcp(address),
            "ipc" => parse_ipc(endpoint, address),
            "inproc" => parse_inproc(address),
            other => {
                if let Some((_, why)) = ABSENT_TRANSPORTS.iter().find(|(name, _)| *name == other) {
                    Err(Error::EPROTONOSUPPORT(
                        format!("the {other} transport is not implemented: {why}").into(),
                    ))
                } else {
                    Err(Error::EPROTONOSUPPORT(
                        format!(
                            "unknown transport {other:?}; this library speaks tcp, ipc and inproc"
                        )
                        .into(),
                    ))
                }
            }
        }
    }

    /// The transport name this endpoint would be written with.
    pub const fn transport(&self) -> &'static str {
        match self {
            Endpoint::Tcp { .. } => "tcp",
            Endpoint::Ipc(_) => "ipc",
            Endpoint::Inproc(_) => "inproc",
        }
    }

    /// Whether the port is the wildcard, so that the bound endpoint has to be
    /// read back to be useful.
    pub const fn has_wildcard_port(&self) -> bool {
        matches!(self, Endpoint::Tcp { port: 0, .. })
    }
}

fn parse_tcp(address: &str) -> Result<Endpoint> {
    // An IPv6 literal is bracketed, because otherwise its own colons are
    // indistinguishable from the port separator.
    let (host_text, port_text) = if let Some(rest) = address.strip_prefix('[') {
        let Some((literal, tail)) = rest.split_once(']') else {
            return Err(Error::EINVAL(
                format!("unterminated IPv6 literal in tcp endpoint: {address:?}").into(),
            ));
        };
        let Some(port) = tail.strip_prefix(':') else {
            return Err(Error::EINVAL(
                format!("a tcp endpoint needs host:port: {address:?}").into(),
            ));
        };
        let ip: IpAddr = literal
            .parse()
            .map_err(|_| Error::EINVAL(format!("not an IP literal: {literal:?}").into()))?;
        return Ok(Endpoint::Tcp {
            host: TcpHost::Ip(ip),
            port: parse_port(port, address)?,
        });
    } else {
        match address.rsplit_once(':') {
            Some(parts) => parts,
            None => {
                return Err(Error::EINVAL(
                    format!("a tcp endpoint needs host:port: {address:?}").into(),
                ));
            }
        }
    };
    if host_text.is_empty() {
        return Err(Error::EINVAL(
            format!("a tcp endpoint needs a host: {address:?}").into(),
        ));
    }
    // A colon left in the host half means the address was an unbracketed
    // IPv6 literal: `tcp://::1:5555` splits into a host that parses as `::1`
    // and a port, *and* is a valid IPv6 address on its own. That ambiguity is
    // exactly why the brackets exist, so the lenient reading is refused with
    // the fix in the message rather than guessed at.
    if host_text.contains(':') {
        return Err(Error::EINVAL(
            format!("write an IPv6 literal in brackets: tcp://[{host_text}]:port").into(),
        ));
    }
    let host = match host_text {
        "*" => TcpHost::Any,
        text => match text.parse::<IpAddr>() {
            Ok(ip) => TcpHost::Ip(ip),
            Err(_) => TcpHost::Name(text.to_owned()),
        },
    };
    Ok(Endpoint::Tcp {
        host,
        port: parse_port(port_text, address)?,
    })
}

/// `*` and `0` are the same request — "any free port" — and libzmq accepts
/// both; everything else must be a port number.
fn parse_port(text: &str, address: &str) -> Result<u16> {
    if text == "*" {
        return Ok(0);
    }
    text.parse::<u16>()
        .map_err(|_| Error::EINVAL(format!("not a port number: {text:?} in {address:?}").into()))
}

fn parse_ipc(endpoint: &str, path: &str) -> Result<Endpoint> {
    if path.is_empty() {
        return Err(Error::EINVAL(
            "an ipc endpoint needs a path: \"ipc://\"".into(),
        ));
    }
    if path.contains('\0') {
        return Err(Error::EINVAL(
            format!("a NUL would truncate the socket path where the kernel reads it: {path:?}")
                .into(),
        ));
    }
    if endpoint.len() > MAX_IPC_ENDPOINT_BYTES {
        return Err(Error::EINVAL(
            format!(
                "an ipc endpoint is at most {MAX_IPC_ENDPOINT_BYTES} bytes including the \
                 {IPC_SCHEME:?} prefix (libzmq's own limit on Linux, and the kernel's \
                 sun_path budget underneath it); this one is {} bytes",
                endpoint.len()
            )
            .into(),
        ));
    }
    Ok(Endpoint::Ipc(PathBuf::from(path)))
}

fn parse_inproc(name: &str) -> Result<Endpoint> {
    if name.is_empty() || name.len() > MAX_INPROC_NAME_BYTES {
        return Err(Error::EINVAL(
            format!("an inproc name must be 1..={MAX_INPROC_NAME_BYTES} bytes: {name:?}").into(),
        ));
    }
    // A name ends up in a log line, an error message and a comparison; a
    // control byte in it is a mistake in every one of those places.
    if name.bytes().any(|byte| byte < 0x20) {
        return Err(Error::EINVAL(
            format!("invalid byte in inproc name: {name:?}").into(),
        ));
    }
    Ok(Endpoint::Inproc(name.to_owned()))
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Tcp { host, port } => write!(f, "tcp://{host}:{port}"),
            Endpoint::Ipc(path) => write!(f, "{IPC_SCHEME}{}", path.display()),
            Endpoint::Inproc(name) => write!(f, "inproc://{name}"),
        }
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(endpoint: &str) -> Result<Endpoint> {
        Endpoint::parse(endpoint)
    }
}

impl Endpoint {
    /// The `ipc://` path, for a caller that has to bind it.
    pub fn ipc_path(&self) -> Option<&Path> {
        match self {
            Endpoint::Ipc(path) => Some(path),
            _ => None,
        }
    }

    /// The `inproc://` name, for a caller that has to register it.
    pub fn inproc_name(&self) -> Option<&str> {
        match self {
            Endpoint::Inproc(name) => Some(name),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(endpoint: &str) -> Endpoint {
        Endpoint::parse(endpoint).unwrap_or_else(|e| panic!("{endpoint:?}: {e}"))
    }

    /// Claim: the three shapes libzmq's `tcp://` accepts all parse, and each
    /// keeps the distinction that matters at bind time.
    #[test]
    fn tcp_accepts_a_wildcard_a_literal_and_a_name() {
        assert_eq!(
            parse("tcp://*:5555"),
            Endpoint::Tcp {
                host: TcpHost::Any,
                port: 5555
            }
        );
        assert_eq!(
            parse("tcp://127.0.0.1:5555"),
            Endpoint::Tcp {
                host: TcpHost::Ip("127.0.0.1".parse().unwrap()),
                port: 5555
            }
        );
        assert_eq!(
            parse("tcp://eth0:6000"),
            Endpoint::Tcp {
                host: TcpHost::Name("eth0".to_owned()),
                port: 6000
            }
        );
    }

    /// Claim: an IPv6 literal parses in brackets, and the bare form is
    /// refused with the fix rather than silently read as `host:port`.
    #[test]
    fn ipv6_literals_are_bracketed() {
        assert_eq!(
            parse("tcp://[::1]:5555"),
            Endpoint::Tcp {
                host: TcpHost::Ip("::1".parse().unwrap()),
                port: 5555
            }
        );
        assert_eq!(parse("tcp://[::1]:5555").to_string(), "tcp://[::1]:5555");

        let err = Endpoint::parse("tcp://::1:5555").unwrap_err();
        assert_eq!(err.errno(), "EINVAL");
        assert!(err.cause().contains("brackets"), "{err}");

        let err = Endpoint::parse("tcp://[::1:5555").unwrap_err();
        assert_eq!(err.errno(), "EINVAL");
        assert!(err.cause().contains("unterminated"), "{err}");
    }

    /// Claim: `*` and `0` are the same request, and a wildcard port announces
    /// itself so a caller knows it must read the bound endpoint back.
    #[test]
    fn a_wildcard_port_is_zero() {
        let star = parse("tcp://*:*");
        assert_eq!(star, parse("tcp://*:0"));
        assert!(star.has_wildcard_port());
        assert!(!parse("tcp://*:5555").has_wildcard_port());
        assert_eq!(star.to_string(), "tcp://*:0");
    }

    /// Claim: every malformed `tcp://` is refused with `EINVAL` and a message
    /// that names what was wrong.
    #[test]
    fn malformed_tcp_endpoints_are_refused() {
        for bad in [
            "tcp://127.0.0.1",
            "tcp://:5555",
            "tcp://host:70000",
            "tcp://host:http",
            "tcp://host:",
        ] {
            let err = Endpoint::parse(bad).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{bad}: {err}");
            assert!(!err.cause().is_empty(), "{bad}");
        }
    }

    /// Claim: an endpoint without a transport is `EINVAL`, and an endpoint
    /// with a ZeroMQ transport we do not implement is `EPROTONOSUPPORT` with
    /// the transport named — not a parse failure, because the caller wrote
    /// something ZeroMQ understands.
    #[test]
    fn an_unsupported_transport_is_named_absent() {
        let err = Endpoint::parse("tcp:/127.0.0.1:5555").unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");

        for absent in ["pgm", "epgm", "udp", "ws", "wss", "vmci", "tipc", "vsock"] {
            let err = Endpoint::parse(&format!("{absent}://whatever:1")).unwrap_err();
            assert_eq!(err.errno(), "EPROTONOSUPPORT", "{absent}: {err}");
            assert!(err.cause().contains(absent), "{err}");
        }

        let err = Endpoint::parse("carrier-pigeon://nest").unwrap_err();
        assert_eq!(err.errno(), "EPROTONOSUPPORT");
        assert!(err.cause().contains("tcp, ipc and inproc"), "{err}");
    }

    /// Claim: the `ipc://` budget is libzmq's 113 bytes over the whole
    /// endpoint string, and it bites at exactly that length.
    #[test]
    fn the_ipc_budget_counts_the_scheme() {
        let room = MAX_IPC_ENDPOINT_BYTES - IPC_SCHEME.len();
        let longest = format!("{IPC_SCHEME}{}", "a".repeat(room));
        assert_eq!(longest.len(), MAX_IPC_ENDPOINT_BYTES);
        assert_eq!(
            parse(&longest).ipc_path(),
            Some(Path::new(&"a".repeat(room)))
        );

        let over = format!("{IPC_SCHEME}{}", "a".repeat(room + 1));
        let err = Endpoint::parse(&over).unwrap_err();
        assert_eq!(err.errno(), "EINVAL");
        assert!(err.cause().contains("113"), "{err}");

        // The libzmq number and the kernel's own budget agree where sun_path
        // has 108 bytes: 113 is that minus the terminator plus "ipc://".
        // macOS has 104, so there the kernel's budget is the smaller one.
        #[cfg(not(target_os = "macos"))]
        assert_eq!(
            MAX_IPC_ENDPOINT_BYTES - IPC_SCHEME.len(),
            weida_core::MAX_SOCKET_PATH_BYTES,
            "the two derivations of the same limit disagree"
        );
    }

    /// Claim: an `ipc://` endpoint with no path, or with a NUL that the
    /// kernel would truncate at, is refused.
    #[test]
    fn malformed_ipc_endpoints_are_refused() {
        for bad in ["ipc://", "ipc://a\0b"] {
            let err = Endpoint::parse(bad).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{bad}: {err}");
        }
    }

    /// Claim: the `inproc://` name budget is libzmq's 256 bytes, empty names
    /// and control bytes are refused, and the round trip is stable.
    #[test]
    fn the_inproc_budget_is_256_bytes() {
        let longest = "x".repeat(MAX_INPROC_NAME_BYTES);
        assert_eq!(
            parse(&format!("inproc://{longest}")).inproc_name(),
            Some(longest.as_str())
        );
        assert_eq!(MAX_INPROC_NAME_BYTES, weida_core::MAX_BUS_BYTES);

        for bad in [
            format!("inproc://{}", "x".repeat(MAX_INPROC_NAME_BYTES + 1)),
            "inproc://".to_owned(),
            "inproc://has\ncontrol".to_owned(),
        ] {
            let err = Endpoint::parse(&bad).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{bad}: {err}");
        }

        assert_eq!(parse("inproc://orders").to_string(), "inproc://orders");
        assert_eq!(parse("inproc://orders").transport(), "inproc");
    }

    /// Claim: what `Display` writes, `parse` reads back — the property
    /// `ZMQ_LAST_ENDPOINT` needs to be usable.
    #[test]
    fn display_round_trips_through_parse() {
        for text in [
            "tcp://*:0",
            "tcp://127.0.0.1:5555",
            "tcp://[fe80::1]:5555",
            "tcp://example.invalid:80",
            "ipc:///tmp/weida-zmq.sock",
            "inproc://orders",
        ] {
            let parsed: Endpoint = text.parse().expect(text);
            assert_eq!(parsed.to_string(), text);
            assert_eq!(Endpoint::parse(&parsed.to_string()).expect(text), parsed);
        }
    }
}
