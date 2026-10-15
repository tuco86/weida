//! The `CONNECT` object: "the client version of the `INFO` message".
//!
//! This is the one JSON object this crate *writes*, so it is a struct of
//! typed fields rather than raw octets: the client chooses every value, and a
//! client that had to assemble JSON itself would be a client that could put a
//! `CRLF` in the middle of a control line.
//!
//! The field order is the reference's own table order, which is also the
//! order the Go client's default string uses — so the canonical encoding of a
//! `CONNECT` is byte-identical to the one in the published example.
//!
//! Everything is escaped on the way out: a password with a
//! quote in it, an NKey signature with a backslash, a client name with a
//! newline. None of the three can end the string, the object, or the line.

use std::borrow::Cow;
use std::fmt;

use crate::error::DecodeError;
use crate::json::{ObjectWriter, Scanner, set_once};
use crate::limits::Limits;

/// The fields of one `CONNECT` object.
///
/// The three the reference marks required unconditionally — `verbose`,
/// `pedantic`, `tls_required` — are plain `bool` and are always written, with
/// `false` as the answer a client that says nothing gives. Everything else is
/// optional and is written only when present, because "absent" and "false"
/// are different answers to `echo` and to `headers`: the server reads an
/// absent `echo` as the old protocol rather than as `echo: false`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Connect<'a> {
    /// "Turns on `+OK` protocol acknowledgements." Most clients send `false`;
    /// the server's own default is `true`, which is why it is always written.
    pub verbose: bool,
    /// "Turns on additional strict format checking, e.g. for properly formed
    /// subjects."
    pub pedantic: bool,
    /// "Indicates whether the client requires an SSL connection."
    pub tls_required: bool,
    /// "Client authorization token", required "if `auth_required` is true".
    pub auth_token: Option<Cow<'a, str>>,
    /// "Connection username."
    pub user: Option<Cow<'a, str>>,
    /// "Connection password."
    pub pass: Option<Cow<'a, str>>,
    /// "Client name."
    pub name: Option<Cow<'a, str>>,
    /// "The implementation language of the client."
    pub lang: Option<Cow<'a, str>>,
    /// "The version of the client."
    pub version: Option<Cow<'a, str>>,
    /// "Sending `0` (or absent) indicates client supports original protocol.
    /// Sending `1` indicates that the client supports dynamic reconfiguration
    /// of cluster topology changes by asynchronously receiving `INFO`
    /// messages with known servers it can reconnect to."
    pub protocol: Option<u64>,
    /// "If set to `false`, the server (version 1.2.0+) will not send
    /// originating messages from this connection to its own subscriptions."
    /// Only to be sent where `INFO.proto` is at least 1.
    pub echo: Option<bool>,
    /// "In case the server has responded with a `nonce` on `INFO`, then a
    /// NATS client must use this field to reply with the signed `nonce`."
    pub sig: Option<Cow<'a, str>>,
    /// "The JWT that identifies a user permissions and account."
    pub jwt: Option<Cow<'a, str>>,
    /// "Enable quick replies for cases where a request is sent to a topic
    /// with no responders" — the `503` status of
    /// [`Headers`](crate::Headers), which needs `headers` as well.
    pub no_responders: Option<bool>,
    /// "Whether the client supports headers." Without it the server will not
    /// deliver `HMSG`, and no-responder replies cannot arrive.
    pub headers: Option<bool>,
    /// "The public NKey to authenticate the client. This will be used to
    /// verify the signature (`sig`) against the `nonce` provided in the
    /// `INFO` message."
    pub nkey: Option<Cow<'a, str>>,
}

/// Prints every field, but only whether each secret is set.
///
/// The derived form would print the password, token, JWT and signature in any
/// `{:?}` (a `tracing` field, an `unwrap` message, a failing assertion). The
/// nkey is a public key and is printed like the other identifiers.
impl fmt::Debug for Connect<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connect")
            .field("verbose", &self.verbose)
            .field("pedantic", &self.pedantic)
            .field("tls_required", &self.tls_required)
            .field("auth_token", &Redacted(self.auth_token.as_deref()))
            .field("user", &self.user)
            .field("pass", &Redacted(self.pass.as_deref()))
            .field("name", &self.name)
            .field("lang", &self.lang)
            .field("version", &self.version)
            .field("protocol", &self.protocol)
            .field("echo", &self.echo)
            .field("sig", &Redacted(self.sig.as_deref()))
            .field("jwt", &Redacted(self.jwt.as_deref()))
            .field("no_responders", &self.no_responders)
            .field("headers", &self.headers)
            .field("nkey", &self.nkey)
            .finish()
    }
}

/// A secret, shown only as whether it is present. The value is never
/// formatted, so it cannot leak through a width, precision or alternate flag.
struct Redacted<'a>(Option<&'a str>);

impl fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(_) => f.write_str("Some(<redacted>)"),
            None => f.write_str("None"),
        }
    }
}

impl<'a> Connect<'a> {
    /// Read the fields of one `CONNECT` JSON object.
    ///
    /// A client never receives `CONNECT`; this exists so that what the
    /// encoder writes can be checked by reading it back, which is the only
    /// way a codec with no counterpart on the other side of the socket can be
    /// tested at all.
    pub fn parse(json: &'a [u8], _limits: Limits) -> Result<Self, DecodeError> {
        let mut scanner = Scanner::object(json)?;
        let mut connect = Self::default();
        // The three unconditional booleans go through an `Option` on the way
        // in so that a repeat is still caught: `false` is a value, not an
        // absence.
        let mut verbose = None;
        let mut pedantic = None;
        let mut tls_required = None;
        while let Some(key) = scanner.next_key()? {
            if scanner.value_is_null() {
                continue;
            }
            match key {
                b"verbose" => set_once(&mut verbose, "verbose", scanner.bool_value("verbose")?)?,
                b"pedantic" => {
                    set_once(&mut pedantic, "pedantic", scanner.bool_value("pedantic")?)?;
                }
                b"tls_required" => set_once(
                    &mut tls_required,
                    "tls_required",
                    scanner.bool_value("tls_required")?,
                )?,
                b"auth_token" => set_once(
                    &mut connect.auth_token,
                    "auth_token",
                    scanner.string_value("auth_token")?,
                )?,
                b"user" => set_once(&mut connect.user, "user", scanner.string_value("user")?)?,
                b"pass" => set_once(&mut connect.pass, "pass", scanner.string_value("pass")?)?,
                b"name" => set_once(&mut connect.name, "name", scanner.string_value("name")?)?,
                b"lang" => set_once(&mut connect.lang, "lang", scanner.string_value("lang")?)?,
                b"version" => set_once(
                    &mut connect.version,
                    "version",
                    scanner.string_value("version")?,
                )?,
                b"protocol" => set_once(
                    &mut connect.protocol,
                    "protocol",
                    scanner.u64_value("protocol")?,
                )?,
                b"echo" => set_once(&mut connect.echo, "echo", scanner.bool_value("echo")?)?,
                b"sig" => set_once(&mut connect.sig, "sig", scanner.string_value("sig")?)?,
                b"jwt" => set_once(&mut connect.jwt, "jwt", scanner.string_value("jwt")?)?,
                b"no_responders" => set_once(
                    &mut connect.no_responders,
                    "no_responders",
                    scanner.bool_value("no_responders")?,
                )?,
                b"headers" => set_once(
                    &mut connect.headers,
                    "headers",
                    scanner.bool_value("headers")?,
                )?,
                b"nkey" => set_once(&mut connect.nkey, "nkey", scanner.string_value("nkey")?)?,
                _ => scanner.skip_value()?,
            }
        }
        connect.verbose = verbose.unwrap_or(false);
        connect.pedantic = pedantic.unwrap_or(false);
        connect.tls_required = tls_required.unwrap_or(false);
        Ok(connect)
    }

    /// Append the JSON object, `{` through `}`.
    ///
    /// Cannot fail: every value is escaped, so no field can produce an octet
    /// the control line or the object cannot hold.
    pub fn write_json(&self, out: &mut Vec<u8>) {
        let mut object = ObjectWriter::new(out);
        object.bool("verbose", self.verbose);
        object.bool("pedantic", self.pedantic);
        object.bool("tls_required", self.tls_required);
        for (key, value) in [
            ("auth_token", &self.auth_token),
            ("user", &self.user),
            ("pass", &self.pass),
            ("name", &self.name),
            ("lang", &self.lang),
            ("version", &self.version),
        ] {
            if let Some(value) = value {
                object.string(key, value);
            }
        }
        if let Some(protocol) = self.protocol {
            object.u64("protocol", protocol);
        }
        if let Some(echo) = self.echo {
            object.bool("echo", echo);
        }
        for (key, value) in [("sig", &self.sig), ("jwt", &self.jwt)] {
            if let Some(value) = value {
                object.string(key, value);
            }
        }
        if let Some(no_responders) = self.no_responders {
            object.bool("no_responders", no_responders);
        }
        if let Some(headers) = self.headers {
            object.bool("headers", headers);
        }
        if let Some(nkey) = &self.nkey {
            object.string("nkey", nkey);
        }
        object.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_go_clients_default_string_is_the_canonical_encoding() {
        // "Here is an example from the default string of the Go client."
        const GO: &[u8] =
            br#"{"verbose":false,"pedantic":false,"tls_required":false,"name":"","lang":"go","version":"1.2.2","protocol":1}"#;
        let connect = Connect::parse(GO, Limits::DEFAULT).expect("decodes");
        assert_eq!(connect.name.as_deref(), Some(""));
        assert_eq!(connect.lang.as_deref(), Some("go"));
        assert_eq!(connect.protocol, Some(1));
        let mut written = Vec::new();
        connect.write_json(&mut written);
        assert_eq!(written, GO, "field order is the reference's table order");
    }

    #[test]
    fn a_credential_cannot_escape_its_string() {
        let connect = Connect {
            user: Some(Cow::Borrowed("ali\"ce")),
            pass: Some(Cow::Borrowed("p\\a\r\nss")),
            ..Connect::default()
        };
        let mut written = Vec::new();
        connect.write_json(&mut written);
        assert!(
            !written.contains(&b'\r') && !written.contains(&b'\n'),
            "a CONNECT that broke its own control line: {written:?}"
        );
        assert_eq!(
            Connect::parse(&written, Limits::DEFAULT).expect("decodes"),
            connect
        );
    }

    #[test]
    fn absent_and_false_are_different_answers() {
        let quiet = Connect::parse(br#"{}"#, Limits::DEFAULT).expect("decodes");
        assert_eq!(quiet.echo, None);
        assert!(!quiet.verbose);
        let mut written = Vec::new();
        quiet.write_json(&mut written);
        assert_eq!(
            written,
            br#"{"verbose":false,"pedantic":false,"tls_required":false}"#
        );

        let echoing = Connect::parse(br#"{"echo":false}"#, Limits::DEFAULT).expect("decodes");
        assert_eq!(echoing.echo, Some(false));
    }

    #[test]
    fn debug_shows_whether_each_secret_is_set_and_nothing_of_it() {
        let connect = Connect {
            pass: Some(Cow::Borrowed("hunter2")),
            auth_token: Some(Cow::Borrowed("t0k3n")),
            jwt: Some(Cow::Borrowed("ey.J.W.T")),
            sig: Some(Cow::Borrowed("SIGNATURE")),
            user: Some(Cow::Borrowed("alice")),
            ..Connect::default()
        };
        let shown = format!("{connect:?}");
        for secret in ["hunter2", "t0k3n", "ey.J.W.T", "SIGNATURE"] {
            assert!(!shown.contains(secret), "{secret} leaked: {shown}");
        }
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(shown.contains("alice"), "{shown}");
        assert!(shown.contains("auth_token: Some(<redacted>)"), "{shown}");

        let empty = format!("{:?}", Connect::default());
        assert!(empty.contains("pass: None"), "{empty}");
    }
}
