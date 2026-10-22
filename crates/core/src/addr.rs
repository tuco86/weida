//! Endpoint addressing: `weida://[fingerprint@]host:port/path`.
//!
//! The path is an **opaque identifier** (master doc §4). Nothing in the core
//! interprets its structure: no hierarchy, no wildcards, no topic semantics.
//!
//! The optional fingerprint in the userinfo position names the peer expected to
//! answer: `weida://sha256:9f86…@10.0.0.8:7443/samples` is a complete
//! description of *where* to dial and *whom* to accept, in one string that a
//! discovery record, a config line or a pasted terminal line can carry.

use std::fmt;

use crate::error::Error;
use crate::identity::Fingerprint;

/// URL scheme of the native transport.
pub const SCHEME: &str = "weida";

/// Maximum endpoint path length in bytes (also the wire cap for DATA key 0).
pub const MAX_PATH_BYTES: usize = 512;

/// Validates an endpoint path.
///
/// A path MUST start with `/`, be 1..=[`MAX_PATH_BYTES`] bytes long and contain
/// no byte below `0x20`. Every other byte sequence is accepted: the path is an
/// opaque identifier, so `*`, `..` and `%20` carry no meaning here.
pub fn validate_endpoint_path(path: &str) -> Result<(), Error> {
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err(Error::InvalidEndpointPath);
    }
    if !path.starts_with('/') {
        return Err(Error::InvalidEndpointPath);
    }
    if path.bytes().any(|b| b < 0x20) {
        return Err(Error::InvalidEndpointPath);
    }
    Ok(())
}

/// The port a `weida://` URL means when it writes none.
///
/// A portless URL does not mean "guess": it means the authority names a **set**
/// of nodes rather than one, which is what a Kubernetes headless service
/// answers with, and every node of such a set listens on the same port
/// ([decisions/0020](../../../docs/decisions/0020-cluster-and-discovery.md)
/// §4.2).
pub const DEFAULT_PORT: u16 = 7443;

/// A parsed `weida://[fingerprint@]host[:port]/path` address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointAddr {
    /// Host as written: a DNS name or an IP literal without brackets.
    pub host: String,
    /// Port as written, or `None` when the URL names none.
    ///
    /// `None` is not a missing value: it is the statement that the authority
    /// names a **set** of equivalent nodes, resolved through DNS and dialled
    /// on [`DEFAULT_PORT`] ([decisions/0020](../../../docs/decisions/0020-cluster-and-discovery.md)
    /// §4.2). A written port means exactly one endpoint and no discovery.
    pub port: Option<u16>,
    /// Opaque endpoint identifier, starting with `/`.
    pub path: String,
    /// The peer's public-key fingerprint, when the address names one.
    ///
    /// When present it is the only identity the dialling side accepts for this
    /// address, whatever else it trusts.
    pub peer: Option<Fingerprint>,
}

impl EndpointAddr {
    /// Parses a `weida://[fingerprint@]host[:port]/path` URL.
    ///
    /// The port is **optional**, and its absence is meaningful rather than
    /// lenient: it selects discovery, where the host names a set of nodes and
    /// [`DEFAULT_PORT`] is the port (0020 §4.2). IPv6 literals are bracketed,
    /// the fingerprint is in [`Fingerprint`]'s text form, and the path is
    /// validated by [`validate_endpoint_path`].
    pub fn parse(input: &str) -> Result<EndpointAddr, Error> {
        let invalid = |m: &str| Error::InvalidAddress(format!("{m}: {input:?}"));

        let rest = input
            .strip_prefix(SCHEME)
            .and_then(|r| r.strip_prefix("://"))
            .ok_or_else(|| invalid("expected scheme weida://"))?;

        let (authority, path) = match rest.find('/') {
            Some(i) => rest.split_at(i),
            None => return Err(invalid("missing endpoint path")),
        };

        let (peer, authority) = match authority.rsplit_once('@') {
            Some((fp, rest)) => {
                let peer = fp
                    .parse::<Fingerprint>()
                    .map_err(|_| invalid("expected sha256:<64 hex digits> before '@'"))?;
                (Some(peer), rest)
            }
            None => (None, authority),
        };

        let (host, port_str) = if let Some(after) = authority.strip_prefix('[') {
            let close = after
                .find(']')
                .ok_or_else(|| invalid("unterminated IPv6 literal"))?;
            let host = &after[..close];
            let rest = &after[close + 1..];
            match rest.strip_prefix(':') {
                Some(port) => (host, Some(port)),
                None if rest.is_empty() => (host, None),
                None => return Err(invalid("expected ':port' after an IPv6 literal")),
            }
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h, Some(p)),
                // No colon at all: a bare name, which is the discovery form.
                None => (authority, None),
            }
        };

        if host.is_empty() {
            return Err(invalid("empty host"));
        }
        if host.bytes().any(|b| b < 0x20 || b == b'/' || b == b'@') {
            return Err(invalid("invalid byte in host"));
        }
        let port = match port_str {
            Some(text) => {
                let port: u16 = text.parse().map_err(|_| invalid("port is not a u16"))?;
                if port == 0 {
                    return Err(invalid("port 0 is not connectable"));
                }
                Some(port)
            }
            None => None,
        };

        validate_endpoint_path(path).map_err(|_| invalid("invalid endpoint path"))?;

        Ok(EndpointAddr {
            host: host.to_owned(),
            port,
            path: path.to_owned(),
            peer,
        })
    }

    /// True if the host is an IPv6 literal and must be bracketed when printed.
    fn host_needs_brackets(&self) -> bool {
        self.host.contains(':')
    }
}

impl fmt::Display for EndpointAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(SCHEME)?;
        f.write_str("://")?;
        if let Some(peer) = &self.peer {
            write!(f, "{peer}@")?;
        }
        match (self.host_needs_brackets(), self.port) {
            (true, Some(port)) => write!(f, "[{}]:{}{}", self.host, port, self.path),
            (true, None) => write!(f, "[{}]{}", self.host, self.path),
            (false, Some(port)) => write!(f, "{}:{}{}", self.host, port, self.path),
            // Round-trips: a printed address without a port parses back to one
            // without a port, and means the same set.
            (false, None) => write!(f, "{}{}", self.host, self.path),
        }
    }
}

/// URL scheme of the in-process transport.
pub const SCHEME_INPROC: &str = "weida+inproc";

/// Longest in-process bus name, in bytes: libzmq's budget for the same thing
/// ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.8).
pub const MAX_BUS_BYTES: usize = 256;

/// A parsed `weida+inproc://<bus>/<path>` address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InprocAddr {
    /// Bus name, unique within the process.
    pub bus: String,
    /// Opaque endpoint identifier, starting with `/`.
    pub path: String,
}

impl fmt::Display for InprocAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SCHEME_INPROC}://{}{}", self.bus, self.path)
    }
}

/// An address of any transport.
///
/// The transport is part of the address and nothing falls back from one to
/// another on its own: that would change who may connect and what proves
/// them without saying so [0010 §4.6].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    /// `weida://[fingerprint@]host:port/path` — the network transport.
    Quic(EndpointAddr),
    /// `weida+inproc://bus/path` — in process, no socket and no identity.
    Inproc(InprocAddr),
    /// `weida+unix://<percent-encoded>/path` — `AF_UNIX`, the kernel proves
    /// the peer.
    Unix(UnixAddr),
    /// `weida+pipe://<name>/path` — a Windows named pipe, the kernel proves
    /// the peer.
    Pipe(PipeAddr),
}

impl Address {
    /// Parses an address of any transport, by scheme.
    pub fn parse(input: &str) -> Result<Address, Error> {
        if let Some(rest) = input
            .strip_prefix(SCHEME_INPROC)
            .and_then(|r| r.strip_prefix("://"))
        {
            return parse_inproc(input, rest).map(Address::Inproc);
        }
        if input.starts_with(SCHEME_UNIX) {
            return UnixAddr::parse(input).map(Address::Unix);
        }
        if input.starts_with(SCHEME_PIPE) {
            return PipeAddr::parse(input).map(Address::Pipe);
        }
        EndpointAddr::parse(input).map(Address::Quic)
    }

    /// The endpoint path, whatever the transport.
    pub fn path(&self) -> &str {
        match self {
            Address::Quic(a) => &a.path,
            Address::Inproc(a) => &a.path,
            Address::Unix(a) => &a.path,
            Address::Pipe(a) => &a.path,
        }
    }
}

fn parse_inproc(input: &str, rest: &str) -> Result<InprocAddr, Error> {
    let invalid = |m: &str| Error::InvalidAddress(format!("{m}: {input:?}"));

    let (bus, path) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => return Err(invalid("missing endpoint path")),
    };
    // There is no key to pin on a local transport, and an address that looks
    // like it authenticates but does not is worse than one that plainly does
    // not [0010 §4.8].
    if bus.contains('@') {
        return Err(invalid("a local address carries no fingerprint"));
    }
    if bus.is_empty() || bus.len() > MAX_BUS_BYTES {
        return Err(invalid("bus name must be 1..=256 bytes"));
    }
    if bus.bytes().any(|b| b < 0x20) {
        return Err(invalid("invalid byte in bus name"));
    }
    validate_endpoint_path(path).map_err(|_| invalid("invalid endpoint path"))?;

    Ok(InprocAddr {
        bus: bus.to_owned(),
        path: path.to_owned(),
    })
}

/// URL scheme of the `AF_UNIX` transport.
pub const SCHEME_UNIX: &str = "weida+unix";

/// Longest socket path this platform accepts, in bytes, after decoding.
///
/// `sun_path` is `char[108]` on Linux and `char[104]` on macOS, and Rust's
/// standard library, which every bind and connect here goes through, keeps
/// room for the terminating NUL on both: 107 usable on Linux, 103 on macOS.
/// On macOS an App Group container path plus a team-ID-prefixed group name
/// consumes most of it, so the budget is checked against the *expanded* path
/// ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.5,
/// `docs/research/ipc.md` §1.1, §2.1, §6.1).
pub const MAX_SOCKET_PATH_BYTES: usize = if cfg!(target_os = "macos") { 103 } else { 107 };

/// A parsed `weida+unix://<percent-encoded-socket-path>/<path>` address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnixAddr {
    /// Filesystem path of the socket, **decoded**.
    pub socket: String,
    /// Opaque endpoint identifier, starting with `/`.
    pub path: String,
}

impl UnixAddr {
    /// Parses `weida+unix://<percent-encoded-socket-path>/<path>`.
    ///
    /// The socket path is percent-encoded because it contains the same
    /// separator the endpoint path uses, and it is validated against this
    /// platform's `sun_path` budget **after** decoding [0010 §4.8]. The
    /// `sha256:…@` userinfo form is refused: there is no key to pin on a
    /// local transport, and an address that looks like it authenticates but
    /// does not is worse than one that plainly does not.
    pub fn parse(input: &str) -> Result<UnixAddr, Error> {
        let invalid = |m: &str| Error::InvalidAddress(format!("{m}: {input:?}"));
        let rest = input
            .strip_prefix(SCHEME_UNIX)
            .and_then(|r| r.strip_prefix("://"))
            .ok_or_else(|| invalid("expected scheme weida+unix://"))?;

        let (authority, path) = match rest.find('/') {
            Some(i) => rest.split_at(i),
            None => return Err(invalid("missing endpoint path")),
        };
        if authority.contains('@') {
            return Err(invalid("a local address carries no fingerprint"));
        }
        let socket = percent_decode(authority).ok_or_else(|| invalid("invalid percent escape"))?;
        if socket.is_empty() {
            return Err(invalid("empty socket path"));
        }
        if socket.len() > MAX_SOCKET_PATH_BYTES {
            return Err(invalid(&format!(
                "socket path exceeds this platform's {MAX_SOCKET_PATH_BYTES}-byte sun_path budget after decoding"
            )));
        }
        // A NUL would truncate `sun_path` where the kernel reads it, and the
        // abstract namespace — a leading NUL — is deliberately not this
        // transport's [0010 §4.5].
        if socket.bytes().any(|b| b == 0) {
            return Err(invalid("a socket path contains no NUL"));
        }
        validate_endpoint_path(path).map_err(|_| invalid("invalid endpoint path"))?;

        Ok(UnixAddr {
            socket,
            path: path.to_owned(),
        })
    }
}

impl fmt::Display for UnixAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SCHEME_UNIX}://")?;
        for byte in self.socket.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    f.write_str(std::str::from_utf8(&[byte]).expect("ascii"))?;
                }
                other => write!(f, "%{other:02X}")?,
            }
        }
        f.write_str(&self.path)
    }
}

/// URL scheme of the Windows named-pipe transport.
pub const SCHEME_PIPE: &str = "weida+pipe";

/// Longest pipe name accepted, in bytes.
///
/// The "entire pipe name string can be up to 256 characters long" — the
/// name after `\\.\pipe\`, which is the part an address carries
/// (`docs/research/ipc.md` §3.1).
pub const MAX_PIPE_NAME_BYTES: usize = 256;

/// The local pipe namespace every [`PipeAddr`] is mapped into.
///
/// Always `\\.\pipe\` and never a UNC path with a computer name: an address
/// names a pipe on this machine only, which is the address-level half of
/// `PIPE_REJECT_REMOTE_CLIENTS` [0010 §4.8].
pub const PIPE_NAMESPACE: &str = r"\\.\pipe\";

/// A parsed `weida+pipe://<pipe-name>/<path>` address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipeAddr {
    /// The pipe's name **without** the `\\.\pipe\` prefix.
    pub name: String,
    /// Opaque endpoint identifier, starting with `/`.
    pub path: String,
}

impl PipeAddr {
    /// Parses `weida+pipe://<pipe-name>/<path>`.
    ///
    /// The name is everything up to the first `/`, so it cannot contain the
    /// endpoint separator; a backslash is refused too, because in the pipe
    /// namespace it is the path separator and would let an address name
    /// something outside `\\.\pipe\`. The `sha256:…@` userinfo form is
    /// refused for the same reason as on every local transport: there is no
    /// key to pin [0010 §4.8].
    pub fn parse(input: &str) -> Result<PipeAddr, Error> {
        let invalid = |m: &str| Error::InvalidAddress(format!("{m}: {input:?}"));
        let rest = input
            .strip_prefix(SCHEME_PIPE)
            .and_then(|r| r.strip_prefix("://"))
            .ok_or_else(|| invalid("expected scheme weida+pipe://"))?;

        let (name, path) = match rest.find('/') {
            Some(i) => rest.split_at(i),
            None => return Err(invalid("missing endpoint path")),
        };
        if name.contains('@') {
            return Err(invalid("a local address carries no fingerprint"));
        }
        if name.is_empty() || name.len() > MAX_PIPE_NAME_BYTES {
            return Err(invalid(&format!(
                "pipe name must be 1..={MAX_PIPE_NAME_BYTES} bytes"
            )));
        }
        if name.bytes().any(|b| b < 0x20 || b == b'\\') {
            return Err(invalid("invalid byte in pipe name"));
        }
        validate_endpoint_path(path).map_err(|_| invalid("invalid endpoint path"))?;

        Ok(PipeAddr {
            name: name.to_owned(),
            path: path.to_owned(),
        })
    }

    /// The OS path of the pipe: `\\.\pipe\<name>`.
    pub fn os_path(&self) -> String {
        format!("{PIPE_NAMESPACE}{}", self.name)
    }
}

impl fmt::Display for PipeAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SCHEME_PIPE}://{}{}", self.name, self.path)
    }
}

/// Decodes percent escapes, returning `None` on a malformed one.
///
/// Deliberately small: what has to round-trip here is a filesystem path, and
/// the encoding exists only so that the socket path's separators cannot be
/// mistaken for the endpoint path's [0010 §4.8].
fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes.get(i + 1..i + 3)?;
                let hex = std::str::from_utf8(hex).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const FP: &str = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    #[test]
    fn parses_ipv4_authority() {
        let a = EndpointAddr::parse("weida://127.0.0.1:7443/transform").unwrap();
        assert_eq!(a.host, "127.0.0.1");
        assert_eq!(a.port, Some(7443));
        assert_eq!(a.path, "/transform");
        assert_eq!(a.peer, None);
        assert_eq!(a.to_string(), "weida://127.0.0.1:7443/transform");
    }

    #[test]
    fn parses_a_peer_fingerprint_before_the_authority() {
        let s = format!("weida://{FP}@[::1]:7443/x");
        let a = EndpointAddr::parse(&s).unwrap();
        assert_eq!(a.host, "::1");
        assert_eq!(a.peer, Some(FP.parse().unwrap()));
        assert_eq!(a.to_string(), s);
    }

    #[test]
    fn parses_bracketed_ipv6_authority() {
        let a = EndpointAddr::parse("weida://[::1]:7443/x").unwrap();
        assert_eq!(a.host, "::1");
        assert_eq!(a.port, Some(7443));
        assert_eq!(a.path, "/x");
        assert_eq!(a.to_string(), "weida://[::1]:7443/x");
    }

    /// Claim: a URL without a port is legal and means a **set** — the
    /// discovery form of
    /// [0020](../../../docs/decisions/0020-cluster-and-discovery.md) §4.2 —
    /// and it round-trips through `Display` unchanged, because a printed
    /// address that gained a port would mean something else.
    #[test]
    fn a_portless_url_is_the_discovery_form() {
        let a = EndpointAddr::parse("weida://jobs.prod.svc.cluster.local/queue").expect("parse");
        assert_eq!(a.host, "jobs.prod.svc.cluster.local");
        assert_eq!(a.port, None);
        assert_eq!(a.path, "/queue");
        assert_eq!(a.peer, None);
        assert_eq!(
            a.to_string(),
            "weida://jobs.prod.svc.cluster.local/queue",
            "a portless address prints without a port"
        );
        assert_eq!(EndpointAddr::parse(&a.to_string()).expect("reparse"), a);
    }

    /// A bracketed IPv6 literal without a port is the same form, and a
    /// bracket followed by anything but `:port` is still an error rather than
    /// a host.
    #[test]
    fn a_portless_ipv6_literal_round_trips_and_a_malformed_one_does_not() {
        let a = EndpointAddr::parse("weida://[::1]/x").expect("parse");
        assert_eq!(a.host, "::1");
        assert_eq!(a.port, None);
        assert_eq!(a.to_string(), "weida://[::1]/x");
        assert_eq!(EndpointAddr::parse(&a.to_string()).expect("reparse"), a);

        assert!(EndpointAddr::parse("weida://[::1]x/y").is_err());
    }

    /// The pinned form works without a port too: the fingerprint identifies
    /// the peer, the name identifies the set (0020 §4.4).
    #[test]
    fn a_portless_url_may_still_pin_a_fingerprint() {
        let a = EndpointAddr::parse(&format!("weida://{FP}@jobs.example/queue")).expect("parse");
        assert_eq!(a.port, None);
        assert_eq!(a.peer, Some(FP.parse().expect("fingerprint")));
    }

    #[test]
    fn parses_dns_name_and_deep_path() {
        let a = EndpointAddr::parse("weida://broker.example.com:443/a/b/c").unwrap();
        assert_eq!(a.host, "broker.example.com");
        assert_eq!(a.path, "/a/b/c");
    }

    #[test]
    fn path_stays_opaque() {
        let a = EndpointAddr::parse("weida://h:1/important*/../%20").unwrap();
        assert_eq!(a.path, "/important*/../%20");
    }

    #[test]
    fn rejects_bad_addresses() {
        let cases = [
            "mq://127.0.0.1:7443/x",
            // `weida://127.0.0.1/x` is not here: it is the discovery form of
            // 0020 §4.2. An authority without a port names a set, dialled on
            // `DEFAULT_PORT`. For a literal there is nothing to resolve, so it
            // is one node on the default port.
            "weida://127.0.0.1:7443",
            "weida://:7443/x",
            "weida://127.0.0.1:0/x",
            "weida://127.0.0.1:99999/x",
            "weida://127.0.0.1:abc/x",
            "weida://[::1:7443/x",
            "weida://[::1]7443/x",
            "127.0.0.1:7443/x",
            "weida://127.0.0.1:7443/a\u{0}b",
            "weida://127.0.0.1:7443/a\u{1f}b",
            "weida://user@host:1/x",
            "weida://sha256:abc@host:1/x",
            "weida://@host:1/x",
            "weida://sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@@host:1/x",
        ];
        for c in cases {
            assert!(
                EndpointAddr::parse(c).is_err(),
                "expected rejection of {c:?}"
            );
        }
    }

    #[test]
    fn path_length_bounds() {
        let ok = format!("weida://h:1/{}", "a".repeat(MAX_PATH_BYTES - 1));
        assert!(EndpointAddr::parse(&ok).is_ok());
        let too_long = format!("weida://h:1/{}", "a".repeat(MAX_PATH_BYTES));
        assert!(EndpointAddr::parse(&too_long).is_err());
    }

    #[test]
    fn path_validation_rules() {
        assert!(validate_endpoint_path("/").is_ok());
        assert!(validate_endpoint_path("").is_err());
        assert!(validate_endpoint_path("no-leading-slash").is_err());
        assert!(validate_endpoint_path("/\t").is_err());
        assert!(validate_endpoint_path(&"/".repeat(MAX_PATH_BYTES)).is_ok());
        assert!(validate_endpoint_path(&"/".repeat(MAX_PATH_BYTES + 1)).is_err());
    }

    #[test]
    fn display_parse_roundtrip() {
        let pinned = format!("weida://{FP}@host:1/a");
        for s in [
            "weida://127.0.0.1:7443/transform",
            "weida://[::1]:7443/x",
            "weida://host:1/a",
            pinned.as_str(),
        ] {
            let a = EndpointAddr::parse(s).unwrap();
            assert_eq!(EndpointAddr::parse(&a.to_string()).unwrap(), a);
        }
    }

    #[test]
    fn a_unix_socket_path_round_trips_through_percent_encoding() {
        let a = UnixAddr::parse("weida+unix://%2Frun%2Fweida.sock/jobs").expect("parse");
        assert_eq!(a.socket, "/run/weida.sock");
        assert_eq!(a.path, "/jobs");
        // The encoding exists so that the socket path's separators cannot be
        // read as the endpoint path's, so it must survive a round trip.
        assert_eq!(UnixAddr::parse(&a.to_string()).expect("reparse"), a);
    }

    #[test]
    fn a_unix_address_is_validated_after_decoding() {
        // One byte past this platform's `sun_path` budget, written encoded:
        // the check has to happen on the decoded form or it would pass.
        let long: String = std::iter::repeat_n("%61", MAX_SOCKET_PATH_BYTES + 1).collect();
        let err = UnixAddr::parse(&format!("weida+unix://{long}/jobs")).unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");

        let ok: String = std::iter::repeat_n("%61", MAX_SOCKET_PATH_BYTES).collect();
        assert!(UnixAddr::parse(&format!("weida+unix://{ok}/jobs")).is_ok());
    }

    #[test]
    fn a_unix_address_refuses_what_would_look_authenticated() {
        for case in [
            // No key can be pinned on a local transport [0010 §4.8].
            "weida+unix://sha256:0000000000000000000000000000000000000000000000000000000000000000@%2Ftmp%2Fs/jobs",
            // A NUL would truncate `sun_path`, and the abstract namespace is
            // deliberately not this transport.
            "weida+unix://%00abstract/jobs",
            // Malformed escape, empty socket path, missing endpoint path.
            "weida+unix://%2/jobs",
            "weida+unix:///jobs",
            "weida+unix://%2Ftmp%2Fs",
        ] {
            assert!(
                UnixAddr::parse(case).is_err(),
                "expected rejection of {case:?}"
            );
        }
    }

    #[test]
    fn a_pipe_address_names_a_local_pipe_only() {
        let a = PipeAddr::parse("weida+pipe://weida-jobs.v1/jobs").expect("parse");
        assert_eq!(a.name, "weida-jobs.v1");
        assert_eq!(a.path, "/jobs");
        // Always the local namespace, never a UNC path with a computer name.
        assert_eq!(a.os_path(), r"\\.\pipe\weida-jobs.v1");
        assert_eq!(PipeAddr::parse(&a.to_string()).expect("reparse"), a);
        assert!(matches!(
            Address::parse("weida+pipe://x/y").expect("by scheme"),
            Address::Pipe(_)
        ));
    }

    #[test]
    fn a_pipe_address_refuses_what_would_escape_or_authenticate() {
        for case in [
            // No key can be pinned on a local transport [0010 §4.8].
            "weida+pipe://sha256:0000000000000000000000000000000000000000000000000000000000000000@x/jobs",
            // A backslash is the pipe namespace's separator: it would name
            // something outside `\\.\pipe\`.
            r"weida+pipe://..\..\c$\boot.ini/jobs",
            // Empty name, control byte, over the 256-byte cap, missing
            // endpoint path.
            "weida+pipe:///jobs",
            "weida+pipe://a\u{1}b/jobs",
            "weida+pipe://x",
        ] {
            assert!(
                PipeAddr::parse(case).is_err(),
                "expected rejection of {case:?}"
            );
        }
        let long = "a".repeat(MAX_PIPE_NAME_BYTES + 1);
        assert!(PipeAddr::parse(&format!("weida+pipe://{long}/jobs")).is_err());
        let ok = "a".repeat(MAX_PIPE_NAME_BYTES);
        assert!(PipeAddr::parse(&format!("weida+pipe://{ok}/jobs")).is_ok());
    }
}
