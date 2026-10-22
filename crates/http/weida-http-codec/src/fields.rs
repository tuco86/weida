//! Field names and values (RFC 9110 §5).
//!
//! The grammar every version shares: a name is a token, a value is visible
//! octets, spaces and tabs with none at either end. HTTP/2 and HTTP/3 add
//! that names are lowercase and that connection-specific fields are malformed
//! ([`CONNECTION_SPECIFIC`]); those rules live in their modules.

/// The fields RFC 9113 §8.2.2 and RFC 9114 §4.2 call connection-specific: an
/// HTTP/2 or HTTP/3 message that carries one is malformed, and an HTTP/1.1
/// library manages them itself rather than taking them from an application.
pub const CONNECTION_SPECIFIC: [&str; 5] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "upgrade",
];

/// One field line as it arrived: the name as written (case preserved) and
/// the value with surrounding whitespace removed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field<'a> {
    /// The field name, a token.
    pub name: &'a [u8],
    /// The field value, trimmed.
    pub value: &'a [u8],
}

/// `tchar` (RFC 9110 §5.6.2).
#[must_use]
pub const fn is_tchar(b: u8) -> bool {
    matches!(b,
        b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.'
        | b'^' | b'_' | b'`' | b'|' | b'~'
        | b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z')
}

/// A non-empty run of `tchar`: a field name or a method (RFC 9110 §5.6.2).
#[must_use]
pub fn is_token(b: &[u8]) -> bool {
    !b.is_empty() && b.iter().all(|&c| is_tchar(c))
}

/// A field value (RFC 9110 §5.5): `VCHAR`, `obs-text`, SP and HTAB, no CR, LF
/// or NUL, and no SP or HTAB at either end. The empty value is valid.
#[must_use]
pub fn is_field_value(b: &[u8]) -> bool {
    let inner = b
        .iter()
        .all(|&c| c == b' ' || c == b'\t' || (0x21..=0x7e).contains(&c) || c >= 0x80);
    let edges = !matches!(b.first(), Some(b' ' | b'\t')) && !matches!(b.last(), Some(b' ' | b'\t'));
    inner && edges
}

/// Whether a field carries a credential an encoder must never put into a
/// compression table, and a log must never print: `authorization`,
/// `proxy-authorization`, `cookie` and `set-cookie`, compared ignoring ASCII
/// case (RFC 7541 §7.1.3, RFC 9204 §7.1.3).
#[must_use]
pub fn is_sensitive(name: &[u8]) -> bool {
    [
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
    ]
    .iter()
    .any(|s| name.eq_ignore_ascii_case(s.as_bytes()))
}

/// Whether `name` is one of [`CONNECTION_SPECIFIC`], ignoring ASCII case.
#[must_use]
pub fn is_connection_specific(name: &[u8]) -> bool {
    CONNECTION_SPECIFIC
        .iter()
        .any(|s| name.eq_ignore_ascii_case(s.as_bytes()))
}

/// Strips SP and HTAB from both ends (RFC 9110 §5.5 OWS).
#[must_use]
pub fn trim_ows(mut b: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = b {
        b = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = b {
        b = rest;
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_refuse_separators_and_the_empty_string() {
        assert!(is_token(b"Content-Length"));
        assert!(is_token(b"x!#$%&'*+-.^_`|~9"));
        assert!(!is_token(b""));
        assert!(!is_token(b"a b"));
        assert!(!is_token(b"a:b"));
        assert!(!is_token(b"(a)"));
    }

    #[test]
    fn values_refuse_line_breaks_nul_and_edge_whitespace() {
        assert!(is_field_value(b""));
        assert!(is_field_value(b"a b\tc"));
        assert!(is_field_value(b"\x80\xff"));
        assert!(!is_field_value(b"a\r\nb"));
        assert!(!is_field_value(b"a\nb"));
        assert!(!is_field_value(b"a\0b"));
        assert!(!is_field_value(b" a"));
        assert!(!is_field_value(b"a\t"));
    }

    #[test]
    fn credentials_are_sensitive_in_any_case() {
        assert!(is_sensitive(b"Authorization"));
        assert!(is_sensitive(b"SET-COOKIE"));
        assert!(!is_sensitive(b"content-type"));
    }
}
