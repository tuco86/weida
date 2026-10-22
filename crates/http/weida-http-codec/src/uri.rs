//! `http` and `https` URLs on the client side, origin-form targets on the
//! server side (RFC 9110 §4.2, RFC 9112 §3.2, RFC 3986 §3).
//!
//! Deliberately narrow: a client of this library names an origin and a path,
//! and nothing else a URL can carry has a meaning for it. Userinfo is refused
//! (RFC 9110 §4.2.4 deprecates it, and a credential in a URL ends up in logs);
//! a fragment is dropped (it is never sent, RFC 9110 §7.1); every octet must
//! be visible ASCII, so a caller with a space or a non-ASCII character in a
//! path percent-encodes it first. No percent-decoding and no normalization
//! happen here: the path goes onto the wire exactly as given.

use crate::error::DecodeError;
use std::borrow::Cow;

/// The two schemes HTTP defines (RFC 9110 §4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scheme {
    /// `http`, port 80.
    Http,
    /// `https`, port 443.
    Https,
}

impl Scheme {
    /// 80 or 443.
    #[must_use]
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }

    /// `"http"` or `"https"`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

/// A parsed absolute URL: the origin and what is sent as the request target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target<'a> {
    /// The scheme.
    pub scheme: Scheme,
    /// A registered name or an IP address; an IPv6 literal without its
    /// brackets. Case as written.
    pub host: &'a str,
    /// The port, the scheme's default when the URL names none.
    pub port: u16,
    /// The path and query, never empty: an empty path is `/`, and a query
    /// without a path is `/?query` (RFC 9112 §3.2.1).
    pub path_and_query: Cow<'a, str>,
}

const fn malformed(what: &'static str) -> DecodeError {
    DecodeError::Malformed { what }
}

fn is_reg_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=%".contains(&b)
}

impl<'a> Target<'a> {
    /// The authority as sent in `Host` or `:authority`: brackets around an
    /// IPv6 literal, `:port` only when it is not the scheme's default.
    #[must_use]
    pub fn authority(&self) -> String {
        let mut out = String::with_capacity(self.host.len() + 8);
        if self.host.contains(':') {
            out.push('[');
            out.push_str(self.host);
            out.push(']');
        } else {
            out.push_str(self.host);
        }
        if self.port != self.scheme.default_port() {
            out.push(':');
            out.push_str(&self.port.to_string());
        }
        out
    }

    /// Parses an absolute `http` or `https` URL.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Malformed`] naming the rule: `"scheme"`, `"userinfo"`,
    /// `"host"`, `"IPv6 literal"`, `"port"`, or
    /// `"a byte outside visible ASCII: percent-encode it"`.
    pub fn parse(input: &'a str) -> Result<Target<'a>, DecodeError> {
        if !input.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return Err(malformed("a byte outside visible ASCII: percent-encode it"));
        }
        let (scheme, rest) = if input.len() >= 7 && input[..7].eq_ignore_ascii_case("http://") {
            (Scheme::Http, &input[7..])
        } else if input.len() >= 8 && input[..8].eq_ignore_ascii_case("https://") {
            (Scheme::Https, &input[8..])
        } else {
            return Err(malformed("scheme"));
        };
        let rest = rest.split_once('#').map_or(rest, |(before, _)| before);
        let split = rest.find(['/', '?']).unwrap_or(rest.len());
        let (authority, path) = rest.split_at(split);
        if authority.contains('@') {
            return Err(malformed("userinfo"));
        }

        let (host, port) = if let Some(after) = authority.strip_prefix('[') {
            let (literal, tail) = after.split_once(']').ok_or(malformed("IPv6 literal"))?;
            if literal.parse::<core::net::Ipv6Addr>().is_err() {
                return Err(malformed("IPv6 literal"));
            }
            let port = match tail {
                "" => None,
                _ => Some(tail.strip_prefix(':').ok_or(malformed("port"))?),
            };
            (literal, port)
        } else {
            match authority.split_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            }
        };
        if !authority.starts_with('[') && (host.is_empty() || !host.bytes().all(is_reg_name_byte)) {
            return Err(malformed("host"));
        }
        let port = match port {
            None | Some("") => scheme.default_port(),
            Some(digits) => {
                if digits.len() > 5 || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(malformed("port"));
                }
                match digits.parse::<u32>() {
                    Ok(p @ 1..=65_535) => p as u16,
                    _ => return Err(malformed("port")),
                }
            }
        };
        let path_and_query = if path.is_empty() {
            Cow::Borrowed("/")
        } else if path.starts_with('?') {
            Cow::Owned(format!("/{path}"))
        } else {
            Cow::Borrowed(path)
        };
        Ok(Target {
            scheme,
            host,
            port,
            path_and_query,
        })
    }
}

/// Checks a request target in origin form (RFC 9112 §3.2.1): it starts with
/// `/` and holds visible ASCII only.
///
/// # Errors
///
/// [`DecodeError::Malformed`] `"request target"`.
pub fn parse_origin_form(input: &[u8]) -> Result<&str, DecodeError> {
    if input.first() != Some(&b'/') || !input.iter().all(|b| (0x21..=0x7e).contains(b)) {
        return Err(malformed("request target"));
    }
    core::str::from_utf8(input).map_err(|_| malformed("request target"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn what(input: &str) -> &'static str {
        match Target::parse(input) {
            Err(DecodeError::Malformed { what }) => what,
            other => panic!("{input}: {other:?}"),
        }
    }

    #[test]
    fn an_ipv6_authority_keeps_its_brackets_and_a_non_default_port() {
        let t = Target::parse("https://[::1]:8443/a?b").unwrap();
        assert_eq!(t.host, "::1");
        assert_eq!(t.port, 8443);
        assert_eq!(t.path_and_query, "/a?b");
        assert_eq!(t.authority(), "[::1]:8443");
    }

    #[test]
    fn the_default_port_is_elided_from_the_authority() {
        let t = Target::parse("HTTP://Example.com:80").unwrap();
        assert_eq!(t.scheme, Scheme::Http);
        assert_eq!(t.host, "Example.com");
        assert_eq!(t.port, 80);
        assert_eq!(t.path_and_query, "/");
        assert_eq!(t.authority(), "Example.com");
        assert_eq!(Target::parse("https://h:444").unwrap().authority(), "h:444");
    }

    #[test]
    fn a_query_without_a_path_gets_the_root_path() {
        assert_eq!(
            Target::parse("http://h?x=1").unwrap().path_and_query,
            "/?x=1"
        );
    }

    #[test]
    fn userinfo_is_refused() {
        assert_eq!(what("https://user:pw@host/"), "userinfo");
    }

    #[test]
    fn a_fragment_is_dropped() {
        let t = Target::parse("http://h/p?q#frag").unwrap();
        assert_eq!(t.path_and_query, "/p?q");
    }

    #[test]
    fn every_rule_has_its_own_name() {
        assert_eq!(what("ftp://h/"), "scheme");
        assert_eq!(what("http:///p"), "host");
        assert_eq!(what("http://h:0/"), "port");
        assert_eq!(what("http://h:65536/"), "port");
        assert_eq!(what("http://h:x/"), "port");
        assert_eq!(what("http://[::g]/"), "IPv6 literal");
        assert_eq!(
            what("http://h/a b"),
            "a byte outside visible ASCII: percent-encode it"
        );
        assert_eq!(
            what("http://h/\u{e4}"),
            "a byte outside visible ASCII: percent-encode it"
        );
        assert_eq!(what("http://h<x>/"), "host");
    }

    #[test]
    fn origin_form_starts_with_a_slash() {
        assert_eq!(parse_origin_form(b"/a?b").unwrap(), "/a?b");
        assert!(parse_origin_form(b"a").is_err());
        assert!(parse_origin_form(b"/a b").is_err());
        assert!(parse_origin_form(b"").is_err());
    }
}
