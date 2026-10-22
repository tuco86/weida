//! HTTP/1.1 over one byte stream: the client side here, the server side in
//! `server`.

pub(crate) mod client;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weida_http_codec::fields::Field;

use crate::error::Result;
use crate::transport::Stream;
use crate::types::Fields;

/// The most one read asks the socket for, and so the most one body chunk
/// read off a connection holds beyond what was already buffered.
pub(crate) const READ_CHUNK: usize = 16 * 1024;

/// A connection with its read buffer: octets read past the end of one
/// message belong to the next.
#[derive(Debug)]
pub(crate) struct Conn {
    pub(crate) io: Stream,
    pub(crate) buf: BytesMut,
}

impl Conn {
    pub(crate) fn new(io: Stream) -> Conn {
        Conn {
            io,
            buf: BytesMut::new(),
        }
    }

    /// Reads at most [`READ_CHUNK`] octets onto the buffer; `0` is the end of
    /// the stream.
    pub(crate) async fn read_more(&mut self) -> Result<usize> {
        self.buf.reserve(READ_CHUNK);
        let mut limited = (&mut self.io).take(READ_CHUNK as u64);
        Ok(limited.read_buf(&mut self.buf).await?)
    }

    /// Writes `data` whole and flushes.
    pub(crate) async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.io.write_all(data).await?;
        self.io.flush().await?;
        Ok(())
    }
}

/// Decoded fields into [`Fields`], copying each value once out of the read
/// buffer.
pub(crate) fn to_fields(decoded: &[Field<'_>]) -> Fields {
    let mut fields = Fields::new();
    for f in decoded {
        fields.push_decoded(f.name, Bytes::copy_from_slice(f.value));
    }
    fields
}

/// `(name, value)` pairs for the codec's encoders.
pub(crate) fn pairs(fields: &Fields) -> Vec<(&[u8], &[u8])> {
    fields
        .iter()
        .map(|(n, v)| (n.as_bytes(), v.as_ref()))
        .collect()
}
