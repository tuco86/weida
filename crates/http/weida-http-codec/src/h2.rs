//! HTTP/2 frames (RFC 9113 §4, §6).
//!
//! [`Frame::decode`] reads one frame from the front of a buffer and checks
//! what the frame alone can tell: its length against the caller's
//! `SETTINGS_MAX_FRAME_SIZE` (from the 9-octet header, before the payload is
//! looked at), the fixed lengths, padding, which frames may and may not name
//! stream 0, and the SETTINGS values RFC 9113 §6.5.2 constrains. Everything
//! that needs connection state (stream states, flow-control windows,
//! CONTINUATION sequencing, a zero WINDOW_UPDATE increment) is the
//! connection's to decide.

use crate::error::DecodeError;

/// The client connection preface (RFC 9113 §3.4).
pub const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// The frame header's length (RFC 9113 §4.1).
pub const HEADER_LEN: usize = 9;

/// The largest value `SETTINGS_MAX_FRAME_SIZE` may take: 2^24 - 1.
pub const MAX_FRAME_SIZE_LIMIT: u32 = (1 << 24) - 1;

/// The smallest value `SETTINGS_MAX_FRAME_SIZE` may take, and its default.
pub const MIN_MAX_FRAME_SIZE: u32 = 16_384;

/// The largest flow-control window: 2^31 - 1 (RFC 9113 §6.9.1).
pub const MAX_WINDOW: u32 = (1 << 31) - 1;

/// The initial stream and connection window: 65 535 (RFC 9113 §6.9.2).
pub const DEFAULT_WINDOW: u32 = 65_535;

/// Error codes (RFC 9113 §7).
pub mod code {
    /// Graceful shutdown.
    pub const NO_ERROR: u32 = 0x0;
    /// Unspecific protocol error.
    pub const PROTOCOL_ERROR: u32 = 0x1;
    /// Unexpected internal error.
    pub const INTERNAL_ERROR: u32 = 0x2;
    /// Flow-control limits exceeded.
    pub const FLOW_CONTROL_ERROR: u32 = 0x3;
    /// SETTINGS not acknowledged in time.
    pub const SETTINGS_TIMEOUT: u32 = 0x4;
    /// Frame received for a closed stream.
    pub const STREAM_CLOSED: u32 = 0x5;
    /// Frame of the wrong size.
    pub const FRAME_SIZE_ERROR: u32 = 0x6;
    /// Stream not processed.
    pub const REFUSED_STREAM: u32 = 0x7;
    /// Stream cancelled.
    pub const CANCEL: u32 = 0x8;
    /// Compression state not updated.
    pub const COMPRESSION_ERROR: u32 = 0x9;
    /// TCP connection error for CONNECT.
    pub const CONNECT_ERROR: u32 = 0xa;
    /// Processing capacity exceeded.
    pub const ENHANCE_YOUR_CALM: u32 = 0xb;
    /// Negotiated TLS parameters not acceptable.
    pub const INADEQUATE_SECURITY: u32 = 0xc;
    /// Use HTTP/1.1 for the request.
    pub const HTTP_1_1_REQUIRED: u32 = 0xd;
}

/// Setting identifiers (RFC 9113 §6.5.2).
pub mod setting {
    /// `SETTINGS_HEADER_TABLE_SIZE`.
    pub const HEADER_TABLE_SIZE: u16 = 0x1;
    /// `SETTINGS_ENABLE_PUSH`.
    pub const ENABLE_PUSH: u16 = 0x2;
    /// `SETTINGS_MAX_CONCURRENT_STREAMS`.
    pub const MAX_CONCURRENT_STREAMS: u16 = 0x3;
    /// `SETTINGS_INITIAL_WINDOW_SIZE`.
    pub const INITIAL_WINDOW_SIZE: u16 = 0x4;
    /// `SETTINGS_MAX_FRAME_SIZE`.
    pub const MAX_FRAME_SIZE: u16 = 0x5;
    /// `SETTINGS_MAX_HEADER_LIST_SIZE`.
    pub const MAX_HEADER_LIST_SIZE: u16 = 0x6;
}

const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const PRIORITY: u8 = 0x2;
const RST_STREAM: u8 = 0x3;
const SETTINGS: u8 = 0x4;
const PUSH_PROMISE: u8 = 0x5;
const PING: u8 = 0x6;
const GOAWAY: u8 = 0x7;
const WINDOW_UPDATE: u8 = 0x8;
const CONTINUATION: u8 = 0x9;

const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const PRIORITY_FLAG: u8 = 0x20;

/// One HTTP/2 frame, borrowing its payload from the input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    /// DATA (§6.1); padding removed.
    Data {
        /// The stream, never 0.
        stream: u32,
        /// `END_STREAM`.
        end_stream: bool,
        /// The data.
        data: &'a [u8],
    },
    /// HEADERS (§6.2); padding removed, priority fields read and dropped.
    Headers {
        /// The stream, never 0.
        stream: u32,
        /// `END_STREAM`.
        end_stream: bool,
        /// `END_HEADERS`.
        end_headers: bool,
        /// The field block fragment.
        block: &'a [u8],
    },
    /// PRIORITY (§6.3), deprecated; read and ignored by the connection.
    Priority {
        /// The stream, never 0.
        stream: u32,
    },
    /// RST_STREAM (§6.4).
    RstStream {
        /// The stream, never 0.
        stream: u32,
        /// The error code.
        code: u32,
    },
    /// SETTINGS (§6.5), on stream 0.
    Settings {
        /// `ACK`; an acknowledgement carries no parameters.
        ack: bool,
        /// The parameters in order.
        params: Vec<(u16, u32)>,
    },
    /// PUSH_PROMISE (§6.6); padding removed.
    PushPromise {
        /// The stream, never 0.
        stream: u32,
        /// The promised stream.
        promised: u32,
        /// `END_HEADERS`.
        end_headers: bool,
        /// The field block fragment.
        block: &'a [u8],
    },
    /// PING (§6.7), on stream 0.
    Ping {
        /// `ACK`.
        ack: bool,
        /// The opaque data.
        data: [u8; 8],
    },
    /// GOAWAY (§6.8), on stream 0.
    GoAway {
        /// The last stream the sender processed or might process.
        last_stream: u32,
        /// The error code.
        code: u32,
        /// Additional debug data.
        debug: &'a [u8],
    },
    /// WINDOW_UPDATE (§6.9); a zero increment is returned, not refused.
    WindowUpdate {
        /// The stream, or 0 for the connection.
        stream: u32,
        /// The increment, 31 bits.
        increment: u32,
    },
    /// CONTINUATION (§6.10).
    Continuation {
        /// The stream, never 0.
        stream: u32,
        /// `END_HEADERS`.
        end_headers: bool,
        /// The field block fragment.
        block: &'a [u8],
    },
    /// A frame of a type this crate does not know, which a recipient ignores
    /// (§4.1, §5.5).
    Unknown {
        /// The frame type.
        kind: u8,
        /// The stream.
        stream: u32,
    },
}

const fn malformed(what: &'static str) -> DecodeError {
    DecodeError::Malformed { what }
}

fn u32_at(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// Removes padding (§6.1): the first octet is the pad length, which must
/// leave `reserved` octets and the pad itself inside the payload.
fn unpad(payload: &[u8], flags: u8, reserved: usize) -> Result<&[u8], DecodeError> {
    if flags & PADDED == 0 {
        if payload.len() < reserved {
            return Err(malformed("frame length"));
        }
        return Ok(payload);
    }
    let (&pad, rest) = payload.split_first().ok_or(malformed("frame length"))?;
    let pad = usize::from(pad);
    if pad + reserved > rest.len() {
        return Err(malformed("padding"));
    }
    Ok(&rest[..rest.len() - pad])
}

impl<'a> Frame<'a> {
    /// Decodes one frame from the front of `input`. Returns it and the octets
    /// it used.
    ///
    /// # Errors
    ///
    /// `Incomplete` with the exact number of missing octets; `TooLarge`
    /// `"frame"` for a payload length above `max_frame_size`, decided from
    /// the header alone; `Malformed` `"frame length"`, `"padding"`,
    /// `"stream identifier"`, `"SETTINGS_ENABLE_PUSH"`,
    /// `"SETTINGS_INITIAL_WINDOW_SIZE"`, `"SETTINGS_MAX_FRAME_SIZE"`.
    pub fn decode(input: &'a [u8], max_frame_size: u32) -> Result<(Frame<'a>, usize), DecodeError> {
        if input.len() < HEADER_LEN {
            return Err(DecodeError::Incomplete {
                needed: HEADER_LEN - input.len(),
            });
        }
        let len = u32::from_be_bytes([0, input[0], input[1], input[2]]);
        if len > max_frame_size {
            return Err(DecodeError::TooLarge {
                what: "frame",
                declared: u64::from(len),
                cap: u64::from(max_frame_size),
            });
        }
        let end = HEADER_LEN + len as usize;
        if input.len() < end {
            return Err(DecodeError::Incomplete {
                needed: end - input.len(),
            });
        }
        let kind = input[3];
        let flags = input[4];
        let stream = u32_at(&input[5..9]) & MAX_WINDOW;
        let payload = &input[HEADER_LEN..end];

        let needs_stream = matches!(
            kind,
            DATA | HEADERS | PRIORITY | RST_STREAM | PUSH_PROMISE | CONTINUATION
        );
        let needs_zero = matches!(kind, SETTINGS | PING | GOAWAY);
        if (needs_stream && stream == 0) || (needs_zero && stream != 0) {
            return Err(malformed("stream identifier"));
        }

        let frame = match kind {
            DATA => Frame::Data {
                stream,
                end_stream: flags & END_STREAM != 0,
                data: unpad(payload, flags, 0)?,
            },
            HEADERS => {
                let reserved = if flags & PRIORITY_FLAG != 0 { 5 } else { 0 };
                let body = unpad(payload, flags, reserved)?;
                Frame::Headers {
                    stream,
                    end_stream: flags & END_STREAM != 0,
                    end_headers: flags & END_HEADERS != 0,
                    block: &body[reserved..],
                }
            }
            PRIORITY => {
                if payload.len() != 5 {
                    return Err(malformed("frame length"));
                }
                Frame::Priority { stream }
            }
            RST_STREAM => {
                if payload.len() != 4 {
                    return Err(malformed("frame length"));
                }
                Frame::RstStream {
                    stream,
                    code: u32_at(payload),
                }
            }
            SETTINGS => {
                let ack = flags & ACK != 0;
                if !payload.len().is_multiple_of(6) || (ack && !payload.is_empty()) {
                    return Err(malformed("frame length"));
                }
                let mut params = Vec::with_capacity(payload.len() / 6);
                for p in payload.as_chunks::<6>().0 {
                    let id = u16::from_be_bytes([p[0], p[1]]);
                    let value = u32_at(&p[2..]);
                    match id {
                        setting::ENABLE_PUSH if value > 1 => {
                            return Err(malformed("SETTINGS_ENABLE_PUSH"));
                        }
                        setting::INITIAL_WINDOW_SIZE if value > MAX_WINDOW => {
                            return Err(malformed("SETTINGS_INITIAL_WINDOW_SIZE"));
                        }
                        setting::MAX_FRAME_SIZE
                            if !(MIN_MAX_FRAME_SIZE..=MAX_FRAME_SIZE_LIMIT).contains(&value) =>
                        {
                            return Err(malformed("SETTINGS_MAX_FRAME_SIZE"));
                        }
                        _ => {}
                    }
                    params.push((id, value));
                }
                Frame::Settings { ack, params }
            }
            PUSH_PROMISE => {
                let body = unpad(payload, flags, 4)?;
                Frame::PushPromise {
                    stream,
                    promised: u32_at(body) & MAX_WINDOW,
                    end_headers: flags & END_HEADERS != 0,
                    block: &body[4..],
                }
            }
            PING => {
                let data: [u8; 8] = payload.try_into().map_err(|_| malformed("frame length"))?;
                Frame::Ping {
                    ack: flags & ACK != 0,
                    data,
                }
            }
            GOAWAY => {
                if payload.len() < 8 {
                    return Err(malformed("frame length"));
                }
                Frame::GoAway {
                    last_stream: u32_at(payload) & MAX_WINDOW,
                    code: u32_at(&payload[4..]),
                    debug: &payload[8..],
                }
            }
            WINDOW_UPDATE => {
                if payload.len() != 4 {
                    return Err(malformed("frame length"));
                }
                Frame::WindowUpdate {
                    stream,
                    increment: u32_at(payload) & MAX_WINDOW,
                }
            }
            CONTINUATION => Frame::Continuation {
                stream,
                end_headers: flags & END_HEADERS != 0,
                block: payload,
            },
            kind => Frame::Unknown { kind, stream },
        };
        Ok((frame, end))
    }

    /// Appends the frame. No padding and no priority fields are written.
    /// [`Frame::Unknown`] writes nothing: it carries no payload to send.
    ///
    /// The caller keeps every payload within the peer's
    /// `SETTINGS_MAX_FRAME_SIZE`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let head = |len: usize, kind: u8, flags: u8, stream: u32, out: &mut Vec<u8>| {
            debug_assert!(len <= MAX_FRAME_SIZE_LIMIT as usize);
            out.extend_from_slice(&(len as u32).to_be_bytes()[1..]);
            out.push(kind);
            out.push(flags);
            out.extend_from_slice(&(stream & MAX_WINDOW).to_be_bytes());
        };
        let flag = |on: bool, f: u8| if on { f } else { 0 };
        match self {
            Frame::Data {
                stream,
                end_stream,
                data,
            } => {
                head(
                    data.len(),
                    DATA,
                    flag(*end_stream, END_STREAM),
                    *stream,
                    out,
                );
                out.extend_from_slice(data);
            }
            Frame::Headers {
                stream,
                end_stream,
                end_headers,
                block,
            } => {
                let flags = flag(*end_stream, END_STREAM) | flag(*end_headers, END_HEADERS);
                head(block.len(), HEADERS, flags, *stream, out);
                out.extend_from_slice(block);
            }
            Frame::Priority { stream } => {
                head(5, PRIORITY, 0, *stream, out);
                // Exclusive 0, dependency 0, weight 16 (the default, 15 + 1).
                out.extend_from_slice(&[0, 0, 0, 0, 15]);
            }
            Frame::RstStream { stream, code } => {
                head(4, RST_STREAM, 0, *stream, out);
                out.extend_from_slice(&code.to_be_bytes());
            }
            Frame::Settings { ack, params } => {
                head(params.len() * 6, SETTINGS, flag(*ack, ACK), 0, out);
                for (id, value) in params {
                    out.extend_from_slice(&id.to_be_bytes());
                    out.extend_from_slice(&value.to_be_bytes());
                }
            }
            Frame::PushPromise {
                stream,
                promised,
                end_headers,
                block,
            } => {
                let flags = flag(*end_headers, END_HEADERS);
                head(4 + block.len(), PUSH_PROMISE, flags, *stream, out);
                out.extend_from_slice(&(promised & MAX_WINDOW).to_be_bytes());
                out.extend_from_slice(block);
            }
            Frame::Ping { ack, data } => {
                head(8, PING, flag(*ack, ACK), 0, out);
                out.extend_from_slice(data);
            }
            Frame::GoAway {
                last_stream,
                code,
                debug,
            } => {
                head(8 + debug.len(), GOAWAY, 0, 0, out);
                out.extend_from_slice(&(last_stream & MAX_WINDOW).to_be_bytes());
                out.extend_from_slice(&code.to_be_bytes());
                out.extend_from_slice(debug);
            }
            Frame::WindowUpdate { stream, increment } => {
                head(4, WINDOW_UPDATE, 0, *stream, out);
                out.extend_from_slice(&(increment & MAX_WINDOW).to_be_bytes());
            }
            Frame::Continuation {
                stream,
                end_headers,
                block,
            } => {
                head(
                    block.len(),
                    CONTINUATION,
                    flag(*end_headers, END_HEADERS),
                    *stream,
                    out,
                );
                out.extend_from_slice(block);
            }
            Frame::Unknown { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_length_is_refused_from_the_header_alone() {
        // A header announcing 2^24 - 1 octets and no payload at all.
        let head = [0xff, 0xff, 0xff, DATA, 0, 0, 0, 0, 1];
        assert_eq!(
            Frame::decode(&head, 16_384),
            Err(DecodeError::TooLarge {
                what: "frame",
                declared: 16_777_215,
                cap: 16_384
            })
        );
        assert_eq!(
            Frame::decode(&head[..5], 16_384),
            Err(DecodeError::Incomplete { needed: 4 })
        );
    }

    #[test]
    fn the_reserved_bit_is_ignored() {
        let mut out = Vec::new();
        Frame::WindowUpdate {
            stream: 3,
            increment: 7,
        }
        .encode(&mut out);
        out[5] |= 0x80;
        out[9] |= 0x80;
        assert_eq!(
            Frame::decode(&out, 16_384).unwrap().0,
            Frame::WindowUpdate {
                stream: 3,
                increment: 7
            }
        );
    }
}
