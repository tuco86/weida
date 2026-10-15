//! The caller's bounds, passed to every decode, and the numbers the server
//! documentation defaults them to.
//!
//! NATS declares its sizes in ASCII on the control line: `PUB x 18446744073709551615`
//! is twenty-six octets that announce sixteen exabytes, and `MSG` can do the
//! same in the other direction. A decoder that believed a declared count
//! before checking it would be killed by a control line that fits in a single
//! read. So the payload bound is an **argument**, not a constant, and it is
//! checked from the control line alone — before the payload is looked at and
//! before anything is reserved.
//!
//! The bound is an argument for a second reason as well: the real value is
//! remote configuration. The server announces its own ceiling in
//! `INFO.max_payload` ("Maximum payload size, in bytes, that the server will
//! accept from the client"), so a client learns it at connect time and cannot
//! know it before. [`DEFAULT_MAX_PAYLOAD`] is the server documentation's
//! default for that setting, offered as a starting value — never used as the
//! cap by this crate, which has no way to reach for it.
//!
//! Five numbers, because a peer has five ways to make a decoder commit
//! memory: a declared payload count, a control line that never ends, an
//! `INFO` line that never ends, a header block with unboundedly many entries,
//! and a `connect_urls` array naming unboundedly many servers. Only the first
//! of the five is length-prefixed; the other four are bounded while they are
//! walked or searched for.

/// The server documentation's default for `max_payload`, one mebibyte
/// (`docs/research/nats.md` §11).
///
/// A **named constant, not a cap**: the cap is [`Limits::max_payload`], which
/// the caller sets from the server's `INFO`. This constant exists so that a
/// caller with no `INFO` yet — during the handshake, or in a test — has the
/// documented number to hand instead of a literal.
pub const DEFAULT_MAX_PAYLOAD: u64 = 1024 * 1024;

/// A starting value for `max_control_line`, in octets before the `CRLF`.
///
/// A **named constant, not a cap**: the cap is [`Limits::max_control_line`].
///
/// The two published numbers disagree. The protocol reference's error table
/// says the `max_control_line` server option "default is 1024 bytes"; the
/// server has compiled a 4096-octet default for several major versions. This
/// crate names the larger, because the bound applies here to lines a *server*
/// sends us: choosing the larger risks accepting a line no server would have
/// sent, while choosing the smaller would refuse a legal `HMSG` carrying a
/// long subject, a long `_INBOX` reply and a `sid`.
pub const DEFAULT_MAX_CONTROL_LINE: usize = 4096;

/// A starting value for `max_info_line`, in octets before the `CRLF`.
///
/// A **named constant, not a cap**: the cap is [`Limits::max_info_line`].
///
/// `INFO` is the one line whose length grows with the cluster: `connect_urls`
/// names up to [`DEFAULT_MAX_CONNECT_URLS`] servers, and the line also carries
/// a nonce and account fields. Sharing the 4096-octet bound of `MSG`, `HMSG`
/// and `PING` would end the connection on a legitimate `INFO` from a large
/// cluster and leave the URL cap unreachable. 64 KiB holds 256 URLs of typical
/// length with room to spare. The bound still applies while the `CRLF` is
/// searched for, so a peer cannot make the reader buffer an unending `INFO`.
pub const DEFAULT_MAX_INFO_LINE: usize = 64 * 1024;

/// A starting value for the number of entries in one `NATS/1.0` header block.
///
/// A **named constant, not a cap**: the cap is
/// [`Limits::max_header_entries`].
///
/// Neither the protocol reference nor ADR-4 bounds the entry count, and the
/// block's own byte count bounds it only weakly: `a:\r\n` is four octets, so
/// a 1 MiB header block can declare a quarter of a million entries, each one
/// costing a vector slot rather than four octets. 128 is two orders of
/// magnitude above the JetStream headers a server actually sends.
pub const DEFAULT_MAX_HEADER_ENTRIES: u32 = 128;

/// A starting value for the number of URLs in `INFO.connect_urls`.
///
/// A **named constant, not a cap**: the cap is
/// [`Limits::max_connect_urls`].
///
/// `connect_urls` is remote input that grows with the cluster — "when a NATS
/// server cluster expands, an `INFO` message is sent to the client with an
/// updated `connect_urls` list" — and a JSON array announces no length before
/// its elements, so the only place to stop is while reading them. 256 is
/// larger than any cluster a single client usefully fails over across.
pub const DEFAULT_MAX_CONNECT_URLS: u32 = 256;

/// How deeply a value this crate does *not* read may nest before it is
/// refused.
///
/// A **fixed constant, not a caller argument**, because it is not a policy: no
/// field of `INFO` or `CONNECT` nests at all. It exists only so that an
/// unknown field carrying an object or an array — a field a future server
/// version might add — can be stepped over rather than making the whole
/// `INFO` unreadable, and stepping over it is bounded so the step cannot
/// recurse a peer into the stack.
pub const MAX_JSON_DEPTH: u32 = 8;

/// Bounds a decoder applies to remote input.
///
/// Every decode entry point in this crate takes one, by value. There is no
/// way to switch a bound off: `max_payload = u64::MAX` still cannot make a
/// decoder read past the input slice, but it does let a peer's declared count
/// through, which is exactly the thing the type exists to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The largest payload a control line may declare, in octets.
    ///
    /// For `PUB` and `MSG` this is the `#bytes` argument. For `HPUB` and
    /// `HMSG` it is the *total*, not the payload alone, because "headers
    /// count within the `HPUB` total size and therefore within the server's
    /// accepted message size" (`docs/research/nats.md` §3).
    ///
    /// Set it from the server's `INFO.max_payload`;
    /// [`DEFAULT_MAX_PAYLOAD`] is the documented default of that setting.
    pub max_payload: u64,
    /// The largest control line other than `INFO`, in octets before its `CRLF`.
    ///
    /// This bounds the search for the `CRLF` itself, so a peer that sends a
    /// gigabyte without one is stopped at the bound rather than at the end of
    /// the buffer.
    pub max_control_line: usize,
    /// The largest `INFO` line, in octets before its `CRLF`.
    ///
    /// `INFO` follows the size of the cluster, so it cannot share the bound of
    /// the other lines: a legitimate `INFO` carrying a long `connect_urls` list
    /// must not be refused under `max_control_line`. The bound is applied to
    /// the `CRLF` search exactly as `max_control_line` is, so an `INFO` that
    /// never ends is refused here rather than after the reader's buffer fills.
    pub max_info_line: usize,
    /// The largest number of `name: value` entries in one header block.
    pub max_header_entries: u32,
    /// The largest number of elements in `INFO.connect_urls`.
    pub max_connect_urls: u32,
}

impl Limits {
    /// The documented defaults, for a caller that has not yet read `INFO`.
    ///
    /// `max_payload` is the server's documented default rather than a
    /// negotiated value; a client that has read `INFO` must replace it, and
    /// one that has not must not publish anyway.
    pub const DEFAULT: Self = Self {
        max_payload: DEFAULT_MAX_PAYLOAD,
        max_control_line: DEFAULT_MAX_CONTROL_LINE,
        max_info_line: DEFAULT_MAX_INFO_LINE,
        max_header_entries: DEFAULT_MAX_HEADER_ENTRIES,
        max_connect_urls: DEFAULT_MAX_CONNECT_URLS,
    };

    /// The defaults with `max_payload` replaced by what the server announced.
    ///
    /// The one call a client makes after reading `INFO`, which is the moment
    /// the real bound becomes knowable.
    #[must_use]
    pub const fn with_max_payload(self, max_payload: u64) -> Self {
        Self {
            max_payload,
            ..self
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_documented_default_payload_is_one_mebibyte() {
        // `docs/research/nats.md` §11: "max_payload limits the maximum client
        // payload accepted by a server and defaults to 1 MiB".
        assert_eq!(DEFAULT_MAX_PAYLOAD, 1_048_576);
        assert_eq!(Limits::DEFAULT.max_payload, DEFAULT_MAX_PAYLOAD);
    }

    #[test]
    fn info_replaces_only_the_payload_bound() {
        let learned = Limits::DEFAULT.with_max_payload(8 * 1024 * 1024);
        assert_eq!(learned.max_payload, 8 * 1024 * 1024);
        assert_eq!(learned.max_control_line, DEFAULT_MAX_CONTROL_LINE);
        assert_eq!(learned.max_info_line, DEFAULT_MAX_INFO_LINE);
        assert_eq!(learned.max_header_entries, DEFAULT_MAX_HEADER_ENTRIES);
        assert_eq!(learned.max_connect_urls, DEFAULT_MAX_CONNECT_URLS);
    }
}
