//! Message bodies: [`BodyReader`] for what arrives.
//!
//! A body is streamed in chunks as the connection delivers them; nothing here
//! holds more of a body than one chunk unless the caller asks for all of it
//! with [`BodyReader::collect`], which takes the caller's cap.

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::h1::client::H1Body;
use crate::types::Fields;

#[derive(Debug)]
enum ReaderInner {
    Empty,
    H1(H1Body),
}

/// A body that arrives.
#[derive(Debug)]
pub struct BodyReader {
    inner: ReaderInner,
}

impl BodyReader {
    pub(crate) fn h1(body: H1Body) -> BodyReader {
        BodyReader {
            inner: ReaderInner::H1(body),
        }
    }

    /// A body with nothing in it.
    #[must_use]
    pub fn empty() -> BodyReader {
        BodyReader {
            inner: ReaderInner::Empty,
        }
    }

    /// The next chunk, or `None` at the end.
    ///
    /// # Errors
    ///
    /// A transport or protocol error; after one, the body is unusable.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>> {
        match &mut self.inner {
            ReaderInner::Empty => Ok(None),
            ReaderInner::H1(body) => body.chunk().await,
        }
    }

    /// The whole body, refused as soon as it is known to exceed `max`
    /// octets: from a declared length before anything is read, otherwise
    /// once what arrived would pass it.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`], or what [`BodyReader::chunk`] returns.
    pub async fn collect(&mut self, max: usize) -> Result<Bytes> {
        if self.content_length().is_some_and(|n| n > max as u64) {
            return Err(Error::TooLarge { max });
        }
        let mut out = BytesMut::new();
        while let Some(chunk) = self.chunk().await? {
            if out.len() + chunk.len() > max {
                return Err(Error::TooLarge { max });
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }

    /// The length the sender declared, if it declared one.
    #[must_use]
    pub fn content_length(&self) -> Option<u64> {
        match &self.inner {
            ReaderInner::Empty => Some(0),
            ReaderInner::H1(body) => body.content_length(),
        }
    }

    /// The trailer fields, once the body ended with some.
    #[must_use]
    pub fn trailers(&self) -> Option<&Fields> {
        match &self.inner {
            ReaderInner::Empty => None,
            ReaderInner::H1(body) => body.trailers(),
        }
    }
}
