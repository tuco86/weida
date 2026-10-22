//! The caller's bounds, passed to every decode, and the numbers they default
//! to.
//!
//! HTTP sets no limit of its own on a head, a field count or a chunk line
//! (RFC 9110 §5.4 lets a recipient refuse with 431 and says nothing about
//! where): every bound is the recipient's choice. So each is an **argument**,
//! carried by value in [`Limits`], and each is checked from what the peer
//! declared or from how far the decoder has searched, before anything is
//! reserved for it.
//!
//! Four numbers, because a peer has four ways to make a decoder commit memory
//! or time: a head (or a compressed field section) that never ends or expands
//! without bound, a field section of unboundedly many tiny fields, a chunk-size
//! line that never ends, and a frame whose length prefix announces megabytes.

/// A starting value for [`Limits::max_head_bytes`]: 64 KiB.
///
/// A **named constant, not a cap**: the cap is the field, set by the caller.
/// 64 KiB is four times the HTTP/2 default frame payload and well above what
/// browsers send (cookies included); servers commonly refuse at 8 to 64 KiB.
pub const DEFAULT_MAX_HEAD_BYTES: usize = 64 * 1024;

/// A starting value for [`Limits::max_fields`]: 128.
///
/// A **named constant, not a cap**. `a:\r\n` is four octets, so a 64 KiB head
/// can carry sixteen thousand fields, each costing a vector slot; 128 is an
/// order of magnitude above what a browser sends.
pub const DEFAULT_MAX_FIELDS: usize = 128;

/// A starting value for [`Limits::max_chunk_line`]: 1 KiB.
///
/// A **named constant, not a cap**. A chunk-size line is sixteen hex digits at
/// most; the rest is chunk extensions, which nobody uses and which this codec
/// steps over, so the bound only has to be generous to them.
pub const DEFAULT_MAX_CHUNK_LINE: usize = 1024;

/// A starting value for [`Limits::max_frame_size`]: 16 384, the HTTP/2
/// default and minimum of `SETTINGS_MAX_FRAME_SIZE` (RFC 9113 §6.5.2).
///
/// A **named constant, not a cap**.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;

/// Bounds a decoder applies to remote input.
///
/// Every decode entry point in this crate takes one, by value. There is no way
/// to switch a bound off; `usize::MAX` still cannot make a decoder read past
/// its input, but it lets a peer's declaration through, which is exactly what
/// the type exists to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The largest head, in octets.
    ///
    /// An HTTP/1.1 request or status line with its field lines and the empty
    /// line that ends them; a chunked body's trailer section; a decoded HTTP/2
    /// or HTTP/3 field section counted as RFC 9113 §6.5.2 counts it (name plus
    /// value plus 32 per field); and an HTTP/3 HEADERS frame payload.
    pub max_head_bytes: usize,
    /// The largest number of fields in one field section.
    pub max_fields: usize,
    /// The largest chunk-size line, chunk extensions and line ending included.
    pub max_chunk_line: usize,
    /// The largest HTTP/2 frame payload, and the largest HTTP/3 frame payload
    /// other than DATA and HEADERS.
    pub max_frame_size: u32,
}

impl Limits {
    /// The defaults: [`DEFAULT_MAX_HEAD_BYTES`], [`DEFAULT_MAX_FIELDS`],
    /// [`DEFAULT_MAX_CHUNK_LINE`] and [`DEFAULT_MAX_FRAME_SIZE`].
    pub const DEFAULT: Self = Self {
        max_head_bytes: DEFAULT_MAX_HEAD_BYTES,
        max_fields: DEFAULT_MAX_FIELDS,
        max_chunk_line: DEFAULT_MAX_CHUNK_LINE,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
    };
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
    fn the_defaults_are_the_documented_numbers() {
        assert_eq!(Limits::default(), Limits::DEFAULT);
        assert_eq!(Limits::DEFAULT.max_head_bytes, 65_536);
        assert_eq!(Limits::DEFAULT.max_fields, 128);
        assert_eq!(Limits::DEFAULT.max_chunk_line, 1024);
        // RFC 9113 §6.5.2: the initial value of SETTINGS_MAX_FRAME_SIZE.
        assert_eq!(Limits::DEFAULT.max_frame_size, 16_384);
    }
}
