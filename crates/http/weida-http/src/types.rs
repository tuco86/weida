//! Methods, status codes, fields, versions, heads and URLs.

use std::fmt;

use bytes::Bytes;
use weida_http_codec::EncodeError;
use weida_http_codec::fields::{is_field_value, is_sensitive, is_token};
use weida_http_codec::uri::{Scheme, Target};

use crate::error::{Error, Result};

#[derive(Clone, PartialEq, Eq, Hash)]
enum MethodRepr {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Options,
    Patch,
    Trace,
    Connect,
    Other(Box<str>),
}

/// A request method (RFC 9110 §9). Case-sensitive, as the RFC says.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Method(MethodRepr);

impl Method {
    /// `GET`.
    pub const GET: Method = Method(MethodRepr::Get);
    /// `HEAD`.
    pub const HEAD: Method = Method(MethodRepr::Head);
    /// `POST`.
    pub const POST: Method = Method(MethodRepr::Post);
    /// `PUT`.
    pub const PUT: Method = Method(MethodRepr::Put);
    /// `DELETE`.
    pub const DELETE: Method = Method(MethodRepr::Delete);
    /// `OPTIONS`.
    pub const OPTIONS: Method = Method(MethodRepr::Options);
    /// `PATCH` (RFC 5789).
    pub const PATCH: Method = Method(MethodRepr::Patch);
    /// `TRACE`.
    pub const TRACE: Method = Method(MethodRepr::Trace);
    /// `CONNECT`, which this library neither sends nor serves (0044 §6).
    pub const CONNECT: Method = Method(MethodRepr::Connect);

    /// The method named by `b`, which must be a token.
    ///
    /// # Errors
    ///
    /// [`Error::Encode`] for anything that is not a token.
    pub fn from_bytes(b: &[u8]) -> Result<Method> {
        Ok(Method(match b {
            b"GET" => MethodRepr::Get,
            b"HEAD" => MethodRepr::Head,
            b"POST" => MethodRepr::Post,
            b"PUT" => MethodRepr::Put,
            b"DELETE" => MethodRepr::Delete,
            b"OPTIONS" => MethodRepr::Options,
            b"PATCH" => MethodRepr::Patch,
            b"TRACE" => MethodRepr::Trace,
            b"CONNECT" => MethodRepr::Connect,
            other if is_token(other) => {
                // A token is ASCII, so this cannot fail.
                MethodRepr::Other(String::from_utf8_lossy(other).into())
            }
            _ => return Err(Error::Encode(EncodeError::InvalidName { what: "method" })),
        }))
    }

    /// The method as written on the wire.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match &self.0 {
            MethodRepr::Get => "GET",
            MethodRepr::Head => "HEAD",
            MethodRepr::Post => "POST",
            MethodRepr::Put => "PUT",
            MethodRepr::Delete => "DELETE",
            MethodRepr::Options => "OPTIONS",
            MethodRepr::Patch => "PATCH",
            MethodRepr::Trace => "TRACE",
            MethodRepr::Connect => "CONNECT",
            MethodRepr::Other(m) => m,
        }
    }
}

impl fmt::Debug for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A status code, `100..=999` (RFC 9110 §15).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Status(u16);

impl Status {
    /// 200 OK.
    pub const OK: Status = Status(200);
    /// 204 No Content.
    pub const NO_CONTENT: Status = Status(204);
    /// 400 Bad Request.
    pub const BAD_REQUEST: Status = Status(400);
    /// 404 Not Found.
    pub const NOT_FOUND: Status = Status(404);
    /// 500 Internal Server Error.
    pub const INTERNAL_SERVER_ERROR: Status = Status(500);
    /// 501 Not Implemented.
    pub const NOT_IMPLEMENTED: Status = Status(501);

    /// The status `code`.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] outside `100..=999`.
    pub fn new(code: u16) -> Result<Status> {
        if (100..=999).contains(&code) {
            Ok(Status(code))
        } else {
            Err(Error::Protocol(format!(
                "status {code} is not three digits"
            )))
        }
    }

    /// The code.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// 1xx.
    #[must_use]
    pub const fn is_informational(self) -> bool {
        self.0 >= 100 && self.0 < 200
    }

    /// 2xx.
    #[must_use]
    pub const fn is_success(self) -> bool {
        self.0 >= 200 && self.0 < 300
    }

    /// 3xx.
    #[must_use]
    pub const fn is_redirection(self) -> bool {
        self.0 >= 300 && self.0 < 400
    }
}

impl fmt::Debug for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An ordered list of fields. Names are lowercased on entry; a value is kept
/// as the caller gave it.
///
/// A field marked sensitive, or one whose name is a credential
/// (`authorization`, `proxy-authorization`, `cookie`, `set-cookie`), is
/// never put into an HTTP/2 or HTTP/3 compression table and prints as
/// `<redacted>` in `Debug`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Fields {
    entries: Vec<(Box<str>, Bytes, bool)>,
}

impl Fields {
    /// No fields.
    #[must_use]
    pub fn new() -> Fields {
        Fields::default()
    }

    fn check(name: &str, value: &[u8]) -> Result<Box<str>> {
        if !is_token(name.as_bytes()) {
            return Err(Error::Encode(EncodeError::InvalidName {
                what: "field name",
            }));
        }
        if !is_field_value(value) {
            return Err(Error::Encode(EncodeError::InvalidValue {
                what: "field value",
            }));
        }
        Ok(name.to_ascii_lowercase().into_boxed_str())
    }

    /// Replaces every field named `name` with one holding `value`.
    ///
    /// # Errors
    ///
    /// [`Error::Encode`] for a name that is not a token or a value that is
    /// not a field value (RFC 9110 §5.5: no CR, LF or NUL, no whitespace at
    /// either end).
    pub fn insert(&mut self, name: &str, value: impl Into<Bytes>) -> Result<()> {
        let value = value.into();
        let name = Self::check(name, &value)?;
        self.entries.retain(|(n, _, _)| *n != name);
        self.entries.push((name, value, false));
        Ok(())
    }

    /// Adds a field after the others.
    ///
    /// # Errors
    ///
    /// As [`Fields::insert`].
    pub fn append(&mut self, name: &str, value: impl Into<Bytes>) -> Result<()> {
        let value = value.into();
        let name = Self::check(name, &value)?;
        self.entries.push((name, value, false));
        Ok(())
    }

    /// Adds a field that is never indexed by HTTP/2 or HTTP/3 compression
    /// and never printed.
    ///
    /// # Errors
    ///
    /// As [`Fields::insert`].
    pub fn append_sensitive(&mut self, name: &str, value: impl Into<Bytes>) -> Result<()> {
        let value = value.into();
        let name = Self::check(name, &value)?;
        self.entries.push((name, value, true));
        Ok(())
    }

    /// The first value of `name`, ignoring ASCII case.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Bytes> {
        self.entries
            .iter()
            .find(|(n, _, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v, _)| v)
    }

    /// Every value of `name`, in order.
    pub fn get_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Bytes> + 'a {
        self.entries
            .iter()
            .filter(move |(n, _, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v, _)| v)
    }

    /// Removes every field named `name`; returns whether there was one.
    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|(n, _, _)| !n.eq_ignore_ascii_case(name));
        self.entries.len() != before
    }

    /// Every field in order, as `(name, value)`.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Bytes)> {
        self.entries.iter().map(|(n, v, _)| (&**n, v))
    }

    /// The number of fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adds a field that the codec already validated, as decoded from the
    /// wire.
    pub(crate) fn push_decoded(&mut self, name: &[u8], value: Bytes) {
        let name = String::from_utf8_lossy(name).to_ascii_lowercase();
        self.entries.push((name.into_boxed_str(), value, false));
    }
}

impl fmt::Debug for Fields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (name, value, sensitive) in &self.entries {
            if *sensitive || is_sensitive(name.as_bytes()) {
                map.entry(name, &"<redacted>");
            } else {
                map.entry(name, &String::from_utf8_lossy(value));
            }
        }
        map.finish()
    }
}

/// The HTTP version an exchange used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Version {
    /// HTTP/1.1 (or HTTP/1.0 from a peer).
    Http11,
    /// HTTP/2.
    Http2,
    /// HTTP/3.
    Http3,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Http11 => "HTTP/1.1",
            Self::Http2 => "HTTP/2",
            Self::Http3 => "HTTP/3",
        })
    }
}

/// A request head in HTTP/2's model, used for every version: on HTTP/1.1 a
/// server fills `authority` from `Host` and `scheme` from the connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestHead {
    /// The method.
    pub method: Method,
    /// The scheme.
    pub scheme: Scheme,
    /// The authority: host and, when not the default, port.
    pub authority: String,
    /// The path and query, starting with `/`.
    pub path: String,
    /// The regular fields; no pseudo-fields, no `host`.
    pub fields: Fields,
}

/// A response head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseHead {
    /// The status.
    pub status: Status,
    /// The fields.
    pub fields: Fields,
}

impl ResponseHead {
    /// A head with `status` and no fields.
    #[must_use]
    pub fn new(status: Status) -> ResponseHead {
        ResponseHead {
            status,
            fields: Fields::new(),
        }
    }
}

/// An `http` or `https` URL, as a client requests it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Url {
    /// The scheme.
    pub scheme: Scheme,
    /// The host: a name, or an IP address (IPv6 without brackets).
    pub host: String,
    /// The port.
    pub port: u16,
    /// The path and query, starting with `/`.
    pub path_and_query: String,
}

impl Url {
    /// Parses an absolute `http` or `https` URL. Userinfo is refused, a
    /// fragment dropped, and every octet must be visible ASCII.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidUrl`] naming the input and the rule it broke.
    pub fn parse(input: &str) -> Result<Url> {
        let t = Target::parse(input).map_err(|e| match e {
            weida_http_codec::DecodeError::Malformed { what } => {
                Error::InvalidUrl(format!("{input:?}: {what}"))
            }
            other => Error::InvalidUrl(format!("{input:?}: {other}")),
        })?;
        Ok(Url {
            scheme: t.scheme,
            host: t.host.to_owned(),
            port: t.port,
            path_and_query: t.path_and_query.into_owned(),
        })
    }

    /// The authority as sent in `host` or `:authority`.
    #[must_use]
    pub fn authority(&self) -> String {
        Target {
            scheme: self.scheme,
            host: &self.host,
            port: self.port,
            path_and_query: "".into(),
        }
        .authority()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_never_print() {
        let mut f = Fields::new();
        f.append("Authorization", "Bearer secret").unwrap();
        f.append_sensitive("x-api-key", "k-123").unwrap();
        f.append("accept", "*/*").unwrap();
        let shown = format!("{f:?}");
        assert!(!shown.contains("secret"), "{shown}");
        assert!(!shown.contains("k-123"), "{shown}");
        assert!(shown.contains("*/*"), "{shown}");
    }

    #[test]
    fn names_are_lowercased_and_insert_replaces() {
        let mut f = Fields::new();
        f.append("X-A", "1").unwrap();
        f.append("x-a", "2").unwrap();
        assert_eq!(f.get_all("X-a").count(), 2);
        f.insert("x-A", "3").unwrap();
        assert_eq!(f.iter().collect::<Vec<_>>(), [("x-a", &Bytes::from("3"))]);
        assert!(f.remove("X-A"));
        assert!(f.is_empty());
    }

    #[test]
    fn a_value_with_a_line_break_is_refused() {
        let mut f = Fields::new();
        assert!(matches!(f.append("a", "b\r\nc: d"), Err(Error::Encode(_))));
        assert!(matches!(f.append("a b", "c"), Err(Error::Encode(_))));
        assert!(f.is_empty());
    }

    #[test]
    fn methods_are_tokens() {
        assert_eq!(Method::from_bytes(b"GET").unwrap(), Method::GET);
        assert_eq!(Method::from_bytes(b"PURGE").unwrap().as_str(), "PURGE");
        assert!(Method::from_bytes(b"GE T").is_err());
        assert!(Status::new(99).is_err());
        assert!(Status::new(302).unwrap().is_redirection());
    }
}
