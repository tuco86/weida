//! The video rows' frame record: `[u32 BE total length][u64 BE send µs]`
//! followed by zero padding up to the total length. The relay rechunks the
//! stream at 64 KiB, so [`Parser`] takes slices at any boundary.

/// Bytes of the record header; the smallest frame.
pub const HEADER: usize = 12;

/// Appends one frame of `total` bytes (at least [`HEADER`]) sent at
/// `send_us`.
pub fn encode(send_us: u64, total: usize, out: &mut Vec<u8>) {
    debug_assert!(total >= HEADER);
    let len = u32::try_from(total).expect("a frame fits in u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&send_us.to_be_bytes());
    out.resize(out.len() + (total - HEADER), 0);
}

/// Finds frame boundaries in a byte stream fed in arbitrary slices.
#[derive(Default)]
pub struct Parser {
    header: [u8; HEADER],
    have: usize,
    /// Padding bytes still to skip in the current frame.
    skip: usize,
}

impl Parser {
    /// Feeds `bytes` and calls `frame(send_us, total)` for every frame whose
    /// last byte is among them.
    pub fn feed(&mut self, mut bytes: &[u8], mut frame: impl FnMut(u64, usize)) {
        while !bytes.is_empty() {
            if self.have < HEADER {
                let n = (HEADER - self.have).min(bytes.len());
                self.header[self.have..self.have + n].copy_from_slice(&bytes[..n]);
                self.have += n;
                bytes = &bytes[n..];
                if self.have < HEADER {
                    return;
                }
                let total = u32::from_be_bytes(self.header[..4].try_into().expect("4 bytes"));
                self.skip = (total as usize).saturating_sub(HEADER);
                if self.skip == 0 {
                    self.complete(&mut frame);
                }
                continue;
            }
            let n = self.skip.min(bytes.len());
            self.skip -= n;
            bytes = &bytes[n..];
            if self.skip == 0 {
                self.complete(&mut frame);
            }
        }
    }

    fn complete(&mut self, frame: &mut impl FnMut(u64, usize)) {
        let total = u32::from_be_bytes(self.header[..4].try_into().expect("4 bytes"));
        let send_us = u64::from_be_bytes(self.header[4..].try_into().expect("8 bytes"));
        frame(send_us, (total as usize).max(HEADER));
        self.have = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn stream() -> (Vec<u8>, Vec<(u64, usize)>) {
        let mut state = 0x9e37_79b9_7f4a_7c15;
        let mut out = Vec::new();
        let mut frames = Vec::new();
        for i in 0..200u64 {
            let total = HEADER + (xorshift(&mut state) % (70_000 - HEADER as u64)) as usize;
            let send_us = i * 16_667 + (xorshift(&mut state) % 1000);
            encode(send_us, total, &mut out);
            frames.push((send_us, total));
        }
        (out, frames)
    }

    fn parse_in(bytes: &[u8], mut chunk: impl FnMut() -> usize) -> Vec<(u64, usize)> {
        let mut parser = Parser::default();
        let mut got = Vec::new();
        let mut rest = bytes;
        while !rest.is_empty() {
            let n = chunk().clamp(1, rest.len());
            parser.feed(&rest[..n], |s, t| got.push((s, t)));
            rest = &rest[n..];
        }
        got
    }

    #[test]
    fn frames_parse_across_any_rechunking() {
        let (bytes, frames) = stream();
        for size in [1, 7, 64 * 1024] {
            assert_eq!(parse_in(&bytes, || size), frames, "chunk size {size}");
        }
        let mut state = 42;
        let random = parse_in(&bytes, || (xorshift(&mut state) % 100_000) as usize);
        assert_eq!(random, frames, "random chunk sizes");
    }

    #[test]
    fn a_partial_frame_is_not_reported() {
        let mut bytes = Vec::new();
        encode(5, 100, &mut bytes);
        encode(9, 40, &mut bytes);
        let mut parser = Parser::default();
        let mut got = Vec::new();
        parser.feed(&bytes[..bytes.len() - 1], |s, t| got.push((s, t)));
        assert_eq!(got, vec![(5, 100)]);
        parser.feed(&bytes[bytes.len() - 1..], |s, t| got.push((s, t)));
        assert_eq!(got, vec![(5, 100), (9, 40)]);
    }
}
