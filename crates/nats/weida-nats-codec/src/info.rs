//! The fields of `INFO` that a client acts on.
//!
//! `INFO` is "sent to client after initial TCP/IP connection" and reports
//! "server identity, version, host/port, `max_payload`, protocol level, and
//! feature/security fields" (`docs/research/nats.md` §1). It is a JSON object
//! on the control line, and this crate has no JSON dependency, so
//! [`Op::Info`](crate::Op::Info) carries the **raw octets** and this module is
//! the reader for the fields the handshake needs. Nothing is decoded until
//! the caller asks: an `INFO` whose unknown fields a future server changed is
//! still an `INFO` whose `max_payload` this reads.
//!
//! Fifteen fields, and no others, because these are the ones a client
//! *acts* on:
//!
//! * identity for logging and for the reconnect list: `server_id`,
//!   `server_name`, `version`, `host`, `port`;
//! * capability, which decides what the `CONNECT` may claim: `proto`,
//!   `headers`, `max_payload`;
//! * security, which decides what the `CONNECT` must carry and whether TLS
//!   comes first: `auth_required`, `tls_required`, `tls_available`,
//!   `tls_verify`, `nonce`;
//! * topology and lifecycle: `connect_urls`, `ldm`.
//!
//! Everything else the reference lists — `go`, `git_commit`, `client_id`,
//! `client_ip`, `cluster`, `domain`, `jetstream`, `ws_connect_urls`,
//! `ip` — is stepped over. A field a client does not act on
//! is a field it should not fail on either, so an unknown or unread field
//! never makes an `INFO` unreadable; only a *known* field of the wrong shape
//! does.

use std::borrow::Cow;

use crate::error::DecodeError;
use crate::json::{Scanner, set_once};
use crate::limits::Limits;

/// The fields of one `INFO` object.
///
/// Every field is optional here, including the ones the reference marks
/// "always": a client that refused an `INFO` for a missing `server_name`
/// would refuse to talk to an older server over a field it does not need.
/// The fields the handshake truly requires are checked by the layer that
/// requires them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerInfo<'a> {
    /// "The unique identifier of the NATS server."
    pub server_id: Option<Cow<'a, str>>,
    /// "The name of the NATS server."
    pub server_name: Option<Cow<'a, str>>,
    /// "The version of NATS."
    pub version: Option<Cow<'a, str>>,
    /// "An integer indicating the protocol version of the server. The server
    /// version 1.2.0 sets this to `1` to indicate that it supports the 'Echo'
    /// feature." A client may only send `CONNECT.echo` and expect
    /// asynchronous `INFO` where this is at least 1.
    pub proto: Option<u64>,
    /// "The IP address used to start the NATS server."
    pub host: Option<Cow<'a, str>>,
    /// "The port number the NATS server is configured to listen on."
    pub port: Option<u64>,
    /// "Maximum payload size, in bytes, that the server will accept from the
    /// client."
    ///
    /// This is the number a client puts into
    /// [`Limits::with_max_payload`](crate::Limits::with_max_payload): the cap
    /// on every declared byte count is remote configuration, learned here,
    /// which is why it is an argument to decoding and not a constant.
    pub max_payload: Option<u64>,
    /// "Whether the server supports headers." A client must not send `HPUB`
    /// where this is absent or false.
    pub headers: Option<bool>,
    /// "If this is true, then the client should try to authenticate upon
    /// connect."
    pub auth_required: Option<bool>,
    /// "If this is true, then the client must perform the TLS/1.2
    /// handshake" — before ordinary protocol exchange, not after it.
    pub tls_required: Option<bool>,
    /// If this is true, the server accepts TLS but does not require it. This
    /// is the mixed-mode server, which takes a plaintext client as well as a
    /// TLS one, so a client chooses whether to upgrade.
    pub tls_available: Option<bool>,
    /// "If this is true, the client must provide a valid certificate during
    /// the TLS handshake."
    pub tls_verify: Option<bool>,
    /// "The nonce for use in CONNECT." A server that sends one expects the
    /// NKey client to sign exactly these bytes in `CONNECT.sig`.
    pub nonce: Option<Cow<'a, str>>,
    /// "List of server urls that a client can connect to", resent whenever
    /// the cluster's topology changes.
    ///
    /// Remote input that grows with the cluster, so its length is bounded by
    /// [`Limits::max_connect_urls`](crate::Limits::max_connect_urls).
    pub connect_urls: Option<Vec<Cow<'a, str>>>,
    /// "If the server supports *Lame Duck Mode* notifications, and the
    /// current server has transitioned to lame duck, `ldm` will be set to
    /// `true`" — the drain notice of `docs/research/nats.md` §1.
    pub ldm: Option<bool>,
}

impl<'a> ServerInfo<'a> {
    /// Read the fields of one `INFO` JSON object.
    ///
    /// `json` is the argument of the `INFO` line, `{` through `}`, exactly as
    /// [`Op::Info`](crate::Op::Info) carries it.
    pub fn parse(json: &'a [u8], limits: Limits) -> Result<Self, DecodeError> {
        let mut scanner = Scanner::object(json)?;
        let mut info = Self::default();
        while let Some(key) = scanner.next_key()? {
            // An explicit `null` means the same as an absent field: `INFO`
            // marks most of these optional and a server writing one out as
            // null is not saying anything else.
            if scanner.value_is_null() {
                continue;
            }
            match key {
                b"server_id" => set_once(
                    &mut info.server_id,
                    "server_id",
                    scanner.string_value("server_id")?,
                )?,
                b"server_name" => set_once(
                    &mut info.server_name,
                    "server_name",
                    scanner.string_value("server_name")?,
                )?,
                b"version" => set_once(
                    &mut info.version,
                    "version",
                    scanner.string_value("version")?,
                )?,
                b"proto" => set_once(&mut info.proto, "proto", scanner.u64_value("proto")?)?,
                b"host" => set_once(&mut info.host, "host", scanner.string_value("host")?)?,
                b"port" => set_once(&mut info.port, "port", scanner.u64_value("port")?)?,
                b"max_payload" => set_once(
                    &mut info.max_payload,
                    "max_payload",
                    scanner.u64_value("max_payload")?,
                )?,
                b"headers" => {
                    set_once(&mut info.headers, "headers", scanner.bool_value("headers")?)?
                }
                b"auth_required" => set_once(
                    &mut info.auth_required,
                    "auth_required",
                    scanner.bool_value("auth_required")?,
                )?,
                b"tls_required" => set_once(
                    &mut info.tls_required,
                    "tls_required",
                    scanner.bool_value("tls_required")?,
                )?,
                b"tls_available" => set_once(
                    &mut info.tls_available,
                    "tls_available",
                    scanner.bool_value("tls_available")?,
                )?,
                b"tls_verify" => set_once(
                    &mut info.tls_verify,
                    "tls_verify",
                    scanner.bool_value("tls_verify")?,
                )?,
                b"nonce" => set_once(&mut info.nonce, "nonce", scanner.string_value("nonce")?)?,
                b"connect_urls" => set_once(
                    &mut info.connect_urls,
                    "connect_urls",
                    scanner.string_array("connect_urls", limits.max_connect_urls)?,
                )?,
                b"ldm" => set_once(&mut info.ldm, "ldm", scanner.bool_value("ldm")?)?,
                _ => scanner.skip_value()?,
            }
        }
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference's own telnet transcript from `demo.nats.io`.
    const DEMO: &[u8] = br#"{"server_id":"Zk0GQ3JBSrg3oyxCRRlE09","version":"1.2.0","proto":1,"go":"go1.10.3","host":"0.0.0.0","port":4222,"max_payload":1048576,"client_id":2392}"#;

    #[test]
    fn the_published_transcript_reads() {
        let info = ServerInfo::parse(DEMO, Limits::DEFAULT).expect("decodes");
        assert_eq!(info.server_id.as_deref(), Some("Zk0GQ3JBSrg3oyxCRRlE09"));
        assert_eq!(info.version.as_deref(), Some("1.2.0"));
        assert_eq!(info.proto, Some(1));
        assert_eq!(info.host.as_deref(), Some("0.0.0.0"));
        assert_eq!(info.port, Some(4222));
        assert_eq!(info.max_payload, Some(1_048_576));
        // `go` and `client_id` are fields this reader steps over rather than
        // fails on.
        assert_eq!(info.server_name, None);
    }

    #[test]
    fn connect_urls_is_bounded_as_it_is_read() {
        let json =
            br#"{"connect_urls":["10.0.0.184:4333","192.168.129.1:4333","192.168.192.1:4333"]}"#;
        let info = ServerInfo::parse(json, Limits::DEFAULT).expect("decodes");
        assert_eq!(
            info.connect_urls.expect("present"),
            [
                "10.0.0.184:4333",
                "192.168.129.1:4333",
                "192.168.192.1:4333"
            ]
        );

        let tight = Limits {
            max_connect_urls: 2,
            ..Limits::DEFAULT
        };
        assert_eq!(
            ServerInfo::parse(json, tight),
            Err(DecodeError::ArrayTooLong {
                field: "connect_urls",
                cap: 2
            })
        );
    }

    #[test]
    fn a_known_field_of_the_wrong_shape_is_named() {
        assert_eq!(
            ServerInfo::parse(br#"{"max_payload":"1048576"}"#, Limits::DEFAULT),
            Err(DecodeError::JsonWrongType {
                field: "max_payload",
                expected: "an integer"
            })
        );
        assert_eq!(
            ServerInfo::parse(br#"{"ldm":1}"#, Limits::DEFAULT),
            Err(DecodeError::JsonWrongType {
                field: "ldm",
                expected: "a boolean"
            })
        );
        assert_eq!(
            ServerInfo::parse(br#"{"proto":1,"proto":2}"#, Limits::DEFAULT),
            Err(DecodeError::JsonDuplicateField { field: "proto" })
        );
    }

    #[test]
    fn an_unknown_object_field_does_not_make_the_info_unreadable() {
        // A shape no current server sends, and exactly the shape a future one
        // might: the fields this client acts on must still arrive.
        let json = br#"{"jetstream":{"limits":{"max_memory":-1}},"ldm":true,"nonce":null}"#;
        let info = ServerInfo::parse(json, Limits::DEFAULT).expect("decodes");
        assert_eq!(info.ldm, Some(true));
        assert_eq!(info.nonce, None, "an explicit null is an absent field");
    }

    #[test]
    fn tls_available_is_read_beside_tls_required() {
        let info = ServerInfo::parse(
            br#"{"tls_required":false,"tls_available":true}"#,
            Limits::DEFAULT,
        )
        .expect("decodes");
        assert_eq!(info.tls_required, Some(false));
        assert_eq!(info.tls_available, Some(true));
    }
}
