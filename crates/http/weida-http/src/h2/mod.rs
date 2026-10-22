//! HTTP/2 over TLS (RFC 9113): one connection driver for both roles.
//!
//! The driver task (`conn`) owns the connection and every stream's state;
//! everything else talks to it through commands. A stream's received data
//! lands in a buffer shared with its [`StreamReader`] and never exceeds the
//! window this side advertised, which is the flow control's whole point; a
//! [`StreamWriter`]'s data waits in the driver until both windows allow it.

pub(crate) mod conn;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::sync::{Notify, mpsc, oneshot};
use weida_http_codec::h2::code;

use crate::client::Response;
use crate::error::{Error, Result};
use crate::types::{Fields, RequestHead, ResponseHead};

/// Why a stream ended badly, as its reader and writer see it.
#[derive(Clone, Debug)]
pub(crate) enum StreamError {
    /// RST_STREAM with this code.
    Reset(u32),
    /// Not processed: GOAWAY or REFUSED_STREAM.
    GoingAway,
    /// The connection ended.
    Closed,
}

impl StreamError {
    pub(crate) fn to_error(&self) -> Error {
        match self {
            Self::Reset(code) => Error::Reset {
                code: u64::from(*code),
            },
            Self::GoingAway => Error::GoingAway,
            Self::Closed => Error::Closed,
        }
    }
}

/// A stream's receive side, shared between the driver and the reader.
#[derive(Debug, Default)]
pub(crate) struct RecvShared {
    inner: Mutex<RecvInner>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct RecvInner {
    buf: BytesMut,
    end: bool,
    trailers: Option<Fields>,
    error: Option<StreamError>,
}

impl RecvShared {
    fn lock(&self) -> std::sync::MutexGuard<'_, RecvInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn push(&self, data: &[u8]) {
        self.lock().buf.extend_from_slice(data);
        self.notify.notify_one();
    }

    pub(crate) fn end(&self, trailers: Option<Fields>) {
        let mut inner = self.lock();
        inner.end = true;
        inner.trailers = trailers;
        drop(inner);
        self.notify.notify_one();
    }

    pub(crate) fn fail(&self, e: StreamError) {
        let mut inner = self.lock();
        if !inner.end && inner.error.is_none() {
            inner.error = Some(e);
        }
        drop(inner);
        self.notify.notify_one();
    }

    /// Takes what is buffered and nobody will read, for the window.
    pub(crate) fn discard(&self) -> usize {
        let mut inner = self.lock();
        let n = inner.buf.len();
        inner.buf.clear();
        n
    }
}

/// What the driver is asked to do.
#[derive(Debug)]
pub(crate) enum Cmd {
    /// Client: open a stream with this request head.
    Open {
        head: RequestHead,
        length: Option<u64>,
        end_stream: bool,
        reply: oneshot::Sender<Result<Opened>>,
    },
    /// Send body data; `done` once it is queued for the wire.
    Data {
        id: u32,
        data: Bytes,
        end: bool,
        done: oneshot::Sender<Result<()>>,
    },
    /// End the body with trailers.
    Trailers {
        id: u32,
        trailers: Fields,
        done: oneshot::Sender<Result<()>>,
    },
    /// Server: send the response head.
    Respond {
        id: u32,
        head: ResponseHead,
        end_stream: bool,
        done: oneshot::Sender<Result<()>>,
    },
    /// Reset the stream with this code.
    Reset { id: u32, code: u32 },
    /// The reader is gone: what arrives is credited and dropped.
    StopReceiving { id: u32 },
    /// The reader took `n` octets: credit them.
    Consumed { id: u32, n: usize },
}

/// A stream a client opened, and where its response arrives.
#[derive(Debug)]
pub(crate) struct Opened {
    pub(crate) writer: StreamWriter,
    pub(crate) response: oneshot::Receiver<Result<Response>>,
}

/// A clonable handle onto one HTTP/2 connection's driver.
#[derive(Clone, Debug)]
pub(crate) struct H2Handle {
    cmd: mpsc::UnboundedSender<Cmd>,
    alive: Arc<AtomicBool>,
}

impl H2Handle {
    /// Whether new streams can still be opened: the driver runs and no
    /// GOAWAY came.
    pub(crate) fn is_usable(&self) -> bool {
        self.alive.load(Ordering::Acquire) && !self.cmd.is_closed()
    }

    /// Opens a stream with `head`.
    pub(crate) async fn open(
        &self,
        head: RequestHead,
        length: Option<u64>,
        end_stream: bool,
    ) -> Result<Opened> {
        let (reply, opened) = oneshot::channel();
        self.cmd
            .send(Cmd::Open {
                head,
                length,
                end_stream,
                reply,
            })
            .map_err(|_| Error::GoingAway)?;
        opened.await.map_err(|_| Error::GoingAway)?
    }
}

/// The receive side of one stream, as a [`crate::BodyReader`].
#[derive(Debug)]
pub(crate) struct StreamReader {
    shared: Arc<RecvShared>,
    cmd: mpsc::UnboundedSender<Cmd>,
    id: u32,
    content_length: Option<u64>,
    trailers: Option<Fields>,
    done: bool,
}

impl StreamReader {
    pub(crate) fn new(
        shared: Arc<RecvShared>,
        cmd: mpsc::UnboundedSender<Cmd>,
        id: u32,
        content_length: Option<u64>,
    ) -> StreamReader {
        StreamReader {
            shared,
            cmd,
            id,
            content_length,
            trailers: None,
            done: false,
        }
    }

    pub(crate) fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    pub(crate) fn trailers(&self) -> Option<&Fields> {
        self.trailers.as_ref()
    }

    pub(crate) async fn chunk(&mut self) -> Result<Option<Bytes>> {
        if self.done {
            return Ok(None);
        }
        loop {
            {
                let mut inner = self.shared.lock();
                if !inner.buf.is_empty() {
                    let data = inner.buf.split().freeze();
                    drop(inner);
                    let _ = self.cmd.send(Cmd::Consumed {
                        id: self.id,
                        n: data.len(),
                    });
                    return Ok(Some(data));
                }
                if let Some(e) = &inner.error {
                    self.done = true;
                    return Err(e.to_error());
                }
                if inner.end {
                    self.done = true;
                    self.trailers = inner.trailers.take();
                    return Ok(None);
                }
            }
            // `notify_one` keeps a permit when nobody waits, so a push between
            // the check above and this await is not lost.
            self.shared.notify.notified().await;
        }
    }
}

impl Drop for StreamReader {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.cmd.send(Cmd::StopReceiving { id: self.id });
        }
    }
}

/// The send side of one stream.
#[derive(Debug)]
pub(crate) struct StreamWriter {
    cmd: mpsc::UnboundedSender<Cmd>,
    id: u32,
    finished: bool,
}

impl StreamWriter {
    /// Drops a writer whose stream needs no body: its HEADERS ended it.
    pub(crate) fn disarm(mut self) {
        self.finished = true;
    }

    pub(crate) fn new(cmd: mpsc::UnboundedSender<Cmd>, id: u32) -> StreamWriter {
        StreamWriter {
            cmd,
            id,
            finished: false,
        }
    }

    pub(crate) async fn write(&mut self, data: Bytes) -> Result<()> {
        let (done, queued) = oneshot::channel();
        self.cmd
            .send(Cmd::Data {
                id: self.id,
                data,
                end: false,
                done,
            })
            .map_err(|_| Error::Closed)?;
        queued.await.map_err(|_| Error::Closed)?
    }

    pub(crate) async fn finish(mut self, trailers: Option<Fields>) -> Result<()> {
        self.finished = true;
        let (done, queued) = oneshot::channel();
        let cmd = match trailers {
            Some(trailers) => Cmd::Trailers {
                id: self.id,
                trailers,
                done,
            },
            None => Cmd::Data {
                id: self.id,
                data: Bytes::new(),
                end: true,
                done,
            },
        };
        self.cmd.send(cmd).map_err(|_| Error::Closed)?;
        queued.await.map_err(|_| Error::Closed)?
    }
}

impl Drop for StreamWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.cmd.send(Cmd::Reset {
                id: self.id,
                code: code::CANCEL,
            });
        }
    }
}

/// A server's answer to one HTTP/2 stream.
#[derive(Debug)]
pub(crate) struct H2Responder {
    pub(crate) cmd: mpsc::UnboundedSender<Cmd>,
    pub(crate) id: u32,
    pub(crate) head_request: bool,
    pub(crate) responded: bool,
}

impl H2Responder {
    /// Sends the head; `end_stream` when no body follows.
    pub(crate) async fn send_head(&mut self, head: ResponseHead, end_stream: bool) -> Result<()> {
        self.responded = true;
        let (done, sent) = oneshot::channel();
        self.cmd
            .send(Cmd::Respond {
                id: self.id,
                head,
                end_stream,
                done,
            })
            .map_err(|_| Error::Closed)?;
        sent.await.map_err(|_| Error::Closed)?
    }

    pub(crate) fn writer(&self) -> StreamWriter {
        StreamWriter::new(self.cmd.clone(), self.id)
    }
}

impl Drop for H2Responder {
    fn drop(&mut self) {
        if !self.responded {
            let _ = self.cmd.send(Cmd::Reset {
                id: self.id,
                code: code::INTERNAL_ERROR,
            });
        }
    }
}
