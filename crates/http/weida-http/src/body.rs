//! Message bodies: [`BodyReader`] for what arrives, [`BodyWriter`] for what a
//! server sends.
//!
//! A body is streamed in chunks as the connection delivers them; nothing here
//! holds more of a body than one chunk unless the caller asks for all of it
//! with [`BodyReader::collect`], which takes the caller's cap. Where a
//! connection task delivers a body, it hands chunks over a channel of
//! [`BODY_QUEUE`] places and fills it only while there is room, which is the
//! backpressure: a reader that stops reading stops the connection reading.

use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::h1::client::H1Body;
use crate::types::Fields;

/// Chunks a connection task holds for a reader that has not taken them yet.
pub(crate) const BODY_QUEUE: usize = 4;

/// What a connection task hands a [`BodyReader`].
#[derive(Debug)]
pub(crate) enum Event {
    /// Body octets.
    Data(Bytes),
    /// The end, with the trailers if any came.
    End(Option<Fields>),
    /// The body failed.
    Error(Error),
}

/// A body a connection task delivers over a channel.
#[derive(Debug)]
pub(crate) struct ChannelBody {
    rx: mpsc::Receiver<Event>,
    content_length: Option<u64>,
    trailers: Option<Fields>,
    done: bool,
    /// Fired at the first poll: an HTTP/1.1 server sends `100 Continue` then.
    on_first_poll: Option<oneshot::Sender<()>>,
}

impl ChannelBody {
    pub(crate) fn new(
        rx: mpsc::Receiver<Event>,
        content_length: Option<u64>,
        on_first_poll: Option<oneshot::Sender<()>>,
    ) -> ChannelBody {
        ChannelBody {
            rx,
            content_length,
            trailers: None,
            done: false,
            on_first_poll,
        }
    }

    async fn chunk(&mut self) -> Result<Option<Bytes>> {
        if self.done {
            return Ok(None);
        }
        if let Some(first) = self.on_first_poll.take() {
            let _ = first.send(());
        }
        match self.rx.recv().await {
            Some(Event::Data(data)) => Ok(Some(data)),
            Some(Event::End(trailers)) => {
                self.done = true;
                self.trailers = trailers;
                Ok(None)
            }
            Some(Event::Error(e)) => {
                self.done = true;
                Err(e)
            }
            None => {
                self.done = true;
                Err(Error::Closed)
            }
        }
    }
}

#[derive(Debug)]
enum ReaderInner {
    Empty,
    H1(H1Body),
    Channel(ChannelBody),
    H2(crate::h2::StreamReader),
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

    pub(crate) fn channel(body: ChannelBody) -> BodyReader {
        BodyReader {
            inner: ReaderInner::Channel(body),
        }
    }

    pub(crate) fn h2(body: crate::h2::StreamReader) -> BodyReader {
        BodyReader {
            inner: ReaderInner::H2(body),
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
            ReaderInner::Channel(body) => body.chunk().await,
            ReaderInner::H2(body) => body.chunk().await,
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
            ReaderInner::Channel(body) => body.content_length,
            ReaderInner::H2(body) => body.content_length(),
        }
    }

    /// The trailer fields, once the body ended with some.
    #[must_use]
    pub fn trailers(&self) -> Option<&Fields> {
        match &self.inner {
            ReaderInner::Empty => None,
            ReaderInner::H1(body) => body.trailers(),
            ReaderInner::Channel(body) => body.trailers.as_ref(),
            ReaderInner::H2(body) => body.trailers(),
        }
    }
}

/// How a body a server sends is delimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutBody {
    /// No body may be sent: HEAD, 204, 304.
    None,
    /// Exactly this many octets.
    Length(u64),
    /// Chunked (HTTP/1.1) or framed (HTTP/2, HTTP/3): trailers allowed.
    Open,
    /// Delimited by closing the connection (an HTTP/1.0 client).
    UntilClose,
}

/// What a [`BodyWriter`] hands a connection task.
#[derive(Debug)]
pub(crate) enum WriteEvent {
    /// Body octets.
    Data(Bytes),
    /// The end, with trailers; `done` reports whether it reached the wire.
    Finish {
        trailers: Option<Fields>,
        done: oneshot::Sender<Result<()>>,
    },
}

/// Where a [`BodyWriter`]'s octets go.
#[derive(Debug)]
enum Sink {
    /// An HTTP/1.1 connection task.
    Channel(mpsc::Sender<WriteEvent>),
    /// An HTTP/2 stream.
    H2(crate::h2::StreamWriter),
    /// Finished or aborted.
    Gone,
}

/// A body that is sent: a server's response body, or a client's request
/// body inside an [`crate::OpenRequest`].
///
/// Writing more than a declared `content-length`, finishing short of it,
/// or dropping the writer unfinished aborts the message: on HTTP/1.1 the
/// connection is closed, on HTTP/2 the stream is reset with `CANCEL`, so the
/// peer sees an incomplete message rather than a complete wrong one.
#[derive(Debug)]
pub struct BodyWriter {
    sink: Sink,
    framing: OutBody,
    written: u64,
}

impl BodyWriter {
    pub(crate) fn channel(tx: mpsc::Sender<WriteEvent>, framing: OutBody) -> BodyWriter {
        BodyWriter {
            sink: Sink::Channel(tx),
            framing,
            written: 0,
        }
    }

    pub(crate) fn h2(writer: crate::h2::StreamWriter, framing: OutBody) -> BodyWriter {
        BodyWriter {
            sink: Sink::H2(writer),
            framing,
            written: 0,
        }
    }

    /// A writer for a message that carries no body and is already complete.
    pub(crate) fn done() -> BodyWriter {
        BodyWriter {
            sink: Sink::Gone,
            framing: OutBody::None,
            written: 0,
        }
    }

    fn abort(&mut self, e: Error) -> Error {
        self.sink = Sink::Gone;
        e
    }

    /// Sends `data` as the next part of the body; waits while the
    /// connection has no room (or, on HTTP/2, no flow-control credit).
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] for a body on a message that has none (a response
    /// to HEAD, 204, 304) or past the declared `content-length`, which aborts
    /// the message; [`Error::Closed`] when the connection is gone.
    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        match self.framing {
            OutBody::None => {
                return Err(self.abort(Error::Protocol("a body on a message that has none".into())));
            }
            OutBody::Length(n) if self.written + data.len() as u64 > n => {
                return Err(self.abort(Error::Protocol(format!(
                    "more body than the declared content-length of {n}"
                ))));
            }
            _ => {}
        }
        self.written += data.len() as u64;
        let result = match &mut self.sink {
            Sink::Channel(tx) => tx
                .send(WriteEvent::Data(data))
                .await
                .map_err(|_| Error::Closed),
            Sink::H2(writer) => writer.write(data).await,
            Sink::Gone => Err(Error::Closed),
        };
        result.map_err(|e| self.abort(e))
    }

    /// Ends the body; returns once it reached the connection.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] short of a declared `content-length`, which
    /// aborts the message; [`Error::Closed`]; a transport error.
    pub async fn finish(self) -> Result<()> {
        self.end(None).await
    }

    /// Ends the body with trailer fields.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] `"trailers need chunked framing"` when the
    /// body has a declared length or none; as [`BodyWriter::finish`].
    pub async fn finish_with_trailers(self, trailers: Fields) -> Result<()> {
        self.end(Some(trailers)).await
    }

    pub(crate) async fn end(mut self, trailers: Option<Fields>) -> Result<()> {
        let trailers = trailers.filter(|t| !t.is_empty());
        if trailers.is_some() && self.framing != OutBody::Open {
            return Err(self.abort(Error::Unsupported("trailers need chunked framing")));
        }
        if let OutBody::Length(n) = self.framing
            && self.written != n
        {
            let written = self.written;
            return Err(self.abort(Error::Protocol(format!(
                "the body ended at {written} of the declared content-length of {n}"
            ))));
        }
        match std::mem::replace(&mut self.sink, Sink::Gone) {
            Sink::Channel(tx) => {
                let (done, wrote) = oneshot::channel();
                tx.send(WriteEvent::Finish { trailers, done })
                    .await
                    .map_err(|_| Error::Closed)?;
                wrote.await.map_err(|_| Error::Closed)?
            }
            Sink::H2(writer) => writer.finish(trailers).await,
            Sink::Gone if self.framing == OutBody::None => Ok(()),
            Sink::Gone => Err(Error::Closed),
        }
    }
}
