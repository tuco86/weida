//! The client side of HTTP/1.1: one request on a connection it owns, the
//! response head, and the body read straight off the connection.
//!
//! A request owns its connection from the first octet of the head to the last
//! octet of the response body. The body reader hands the connection back to
//! the pool when the response ended cleanly and both sides meant to keep it
//! (no `connection: close`, not an HTTP/1.0 answer without `keep-alive`, not
//! delimited by close, nothing read past its end); otherwise the connection is
//! dropped, which closes it.

use bytes::{Buf, Bytes};
use weida_http_codec::h1::{self, Chunk, ChunkedDecoder, Framing};
use weida_http_codec::{DecodeError, Limits};

use crate::body::BodyReader;
use crate::client::Response;
use crate::error::{Error, Result};
use crate::h1::{Conn, READ_CHUNK, pairs, to_fields};
use crate::pool::PoolReturn;
use crate::types::{Fields, Method, RequestHead, ResponseHead, Status, Version};

/// How a request body is delimited on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutFraming {
    /// No body and no `content-length`.
    None,
    /// `content-length`.
    Length(u64),
    /// `transfer-encoding: chunked`.
    Chunked,
}

/// A request whose head is written (or pending) and whose body is being sent.
#[derive(Debug)]
pub(crate) struct Upload {
    conn: Conn,
    framing: OutFraming,
    written: u64,
    /// Octets not yet on the wire: the head, until the first write.
    out: Vec<u8>,
    method: Method,
    limits: Limits,
    reused: bool,
    keep: Option<PoolReturn>,
    broken: bool,
}

/// A transport error on a connection taken from the pool, before any octet of
/// an answer, is a connection the server closed while it was idle: the request
/// was not processed.
fn idle_close(reused: bool, e: Error) -> Error {
    match e {
        Error::Io(_) if reused => Error::GoingAway,
        e => e,
    }
}

impl Upload {
    /// Encodes the head; nothing is written until the first [`Upload::write`]
    /// or [`Upload::finish`].
    pub(crate) fn start(
        conn: Conn,
        head: &RequestHead,
        framing: OutFraming,
        limits: Limits,
        reused: bool,
        keep: Option<PoolReturn>,
    ) -> Result<Upload> {
        let mut fields: Vec<(&[u8], &[u8])> = Vec::with_capacity(head.fields.len() + 2);
        fields.push((b"host", head.authority.as_bytes()));
        fields.extend(pairs(&head.fields));
        let length;
        match framing {
            OutFraming::None => {}
            OutFraming::Length(n) => {
                length = n.to_string();
                fields.push((b"content-length", length.as_bytes()));
            }
            OutFraming::Chunked => fields.push((b"transfer-encoding", b"chunked")),
        }
        let mut out = Vec::with_capacity(256);
        h1::encode_request_head(
            head.method.as_str().as_bytes(),
            head.path.as_bytes(),
            &fields,
            &mut out,
        )?;
        Ok(Upload {
            conn,
            framing,
            written: 0,
            out,
            method: head.method.clone(),
            limits,
            reused,
            keep,
            broken: false,
        })
    }

    async fn flush_out(&mut self, data: &[u8]) -> Result<()> {
        let result = if data.is_empty() {
            self.conn.send(&self.out).await
        } else if self.out.is_empty() {
            self.conn.send(data).await
        } else if data.len() <= READ_CHUNK {
            self.out.extend_from_slice(data);
            self.conn.send(&self.out).await
        } else {
            match self.conn.send(&self.out).await {
                Ok(()) => self.conn.send(data).await,
                Err(e) => Err(e),
            }
        };
        self.out.clear();
        result.map_err(|e| {
            self.broken = true;
            idle_close(self.reused, e)
        })
    }

    /// Sends `data` as part of the body.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] past a declared `content-length` or for a body on
    /// a request that declared none; the connection is then unusable.
    pub(crate) async fn write(&mut self, data: Bytes) -> Result<()> {
        if self.broken {
            return Err(Error::Closed);
        }
        if data.is_empty() {
            return Ok(());
        }
        match self.framing {
            OutFraming::None => {
                self.broken = true;
                Err(Error::Protocol(
                    "a body on a request that declared none".into(),
                ))
            }
            OutFraming::Length(n) => {
                if self.written + data.len() as u64 > n {
                    self.broken = true;
                    return Err(Error::Protocol(format!(
                        "more body than the declared content-length of {n}"
                    )));
                }
                self.written += data.len() as u64;
                self.flush_out(&data).await
            }
            OutFraming::Chunked => {
                self.written += data.len() as u64;
                let mut chunk = Vec::with_capacity(data.len() + 12);
                h1::encode_chunk(&data, &mut chunk);
                self.flush_out(&chunk).await
            }
        }
    }

    /// Ends the body, with trailers on a chunked one, and reads the response
    /// head.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] short of a declared `content-length`, or for
    /// trailers on a body that is not chunked; transport and decode errors.
    pub(crate) async fn finish(mut self, trailers: Option<&Fields>) -> Result<Response> {
        if self.broken {
            return Err(Error::Closed);
        }
        match self.framing {
            OutFraming::Length(n) if self.written != n => {
                return Err(Error::Protocol(format!(
                    "the body ended at {} of the declared content-length of {n}",
                    self.written
                )));
            }
            OutFraming::Chunked => {
                let empty = Fields::new();
                h1::encode_last_chunk(&pairs(trailers.unwrap_or(&empty)), &mut self.out)?;
            }
            _ if trailers.is_some_and(|t| !t.is_empty()) => {
                return Err(Error::Unsupported("trailers need chunked framing"));
            }
            _ => {}
        }
        self.flush_out(&[]).await?;
        read_response(self.conn, &self.method, self.limits, self.reused, self.keep).await
    }
}

/// Reads the response head off `conn`, skipping interim responses.
pub(crate) async fn read_response(
    mut conn: Conn,
    method: &Method,
    limits: Limits,
    reused: bool,
    keep: Option<PoolReturn>,
) -> Result<Response> {
    let mut answered = false;
    loop {
        if !conn.buf.is_empty() {
            answered = true;
            match h1::decode_response(&conn.buf, limits) {
                Ok((head, used)) => {
                    if (100..200).contains(&head.status) {
                        if head.status == 101 {
                            return Err(Error::Protocol(
                                "101 Switching Protocols is not supported".into(),
                            ));
                        }
                        conn.buf.advance(used);
                        continue;
                    }
                    let framing = h1::response_framing(
                        method.as_str().as_bytes(),
                        head.status,
                        &head.fields,
                    )?;
                    let keep_alive = !h1::connection_has(&head.fields, "close")
                        && (head.minor == 1 || h1::connection_has(&head.fields, "keep-alive"))
                        && framing != Framing::UntilClose;
                    let status = Status::new(head.status)?;
                    let fields = to_fields(&head.fields);
                    conn.buf.advance(used);
                    let body = H1Body::new(conn, framing, limits, keep.filter(|_| keep_alive));
                    return Ok(Response {
                        head: ResponseHead { status, fields },
                        version: Version::Http11,
                        body: BodyReader::h1(body),
                    });
                }
                Err(DecodeError::Incomplete { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        let n = conn
            .read_more()
            .await
            .map_err(|e| idle_close(reused && !answered, e))?;
        if n == 0 {
            if reused && !answered {
                return Err(Error::GoingAway);
            }
            return Err(Error::Protocol(
                "the server closed the connection before the response head".into(),
            ));
        }
    }
}

#[derive(Debug)]
enum BodyState {
    Length(u64),
    Chunked(ChunkedDecoder),
    UntilClose,
    Done,
    Failed,
}

/// A response body read straight off its connection.
#[derive(Debug)]
pub(crate) struct H1Body {
    conn: Option<Conn>,
    state: BodyState,
    limits: Limits,
    keep: Option<PoolReturn>,
    content_length: Option<u64>,
    trailers: Option<Fields>,
}

impl H1Body {
    pub(crate) fn new(
        conn: Conn,
        framing: Framing,
        limits: Limits,
        keep: Option<PoolReturn>,
    ) -> H1Body {
        let (state, content_length) = match framing {
            Framing::Empty => (BodyState::Length(0), Some(0)),
            Framing::Length(n) => (BodyState::Length(n), Some(n)),
            Framing::Chunked => (BodyState::Chunked(ChunkedDecoder::new()), None),
            Framing::UntilClose => (BodyState::UntilClose, None),
        };
        let mut body = H1Body {
            conn: Some(conn),
            state,
            limits,
            keep,
            content_length,
            trailers: None,
        };
        if content_length == Some(0) {
            body.end();
        }
        body
    }

    pub(crate) fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    pub(crate) fn trailers(&self) -> Option<&Fields> {
        self.trailers.as_ref()
    }

    /// The body ended: the connection goes back to the pool when it may.
    fn end(&mut self) {
        self.state = BodyState::Done;
        if let (Some(conn), Some(keep)) = (self.conn.take(), self.keep.take())
            && conn.buf.is_empty()
        {
            keep.put(conn);
        }
    }

    fn fail(&mut self, e: Error) -> Error {
        self.state = BodyState::Failed;
        self.conn = None;
        e
    }

    /// What the buffer alone yields, without reading.
    fn step(&mut self) -> Step {
        let Some(conn) = self.conn.as_mut() else {
            return match self.state {
                BodyState::Failed => Step::Failed,
                _ => Step::Done,
            };
        };
        match &mut self.state {
            BodyState::Done => Step::Done,
            BodyState::Failed => Step::Failed,
            BodyState::Length(remaining) => {
                if conn.buf.is_empty() {
                    return Step::NeedMore;
                }
                let n = (*remaining).min(conn.buf.len() as u64) as usize;
                let data = conn.buf.split_to(n).freeze();
                *remaining -= n as u64;
                if *remaining == 0 {
                    Step::Last(data)
                } else {
                    Step::Data(data)
                }
            }
            BodyState::Chunked(decoder) => match decoder.step(&conn.buf, self.limits) {
                Ok((Chunk::Data(data), used)) => {
                    let len = data.len();
                    conn.buf.advance(used - len);
                    Step::Data(conn.buf.split_to(len).freeze())
                }
                Ok((Chunk::End(trailers), used)) => {
                    self.trailers = Some(to_fields(&trailers));
                    conn.buf.advance(used);
                    Step::End
                }
                Err(DecodeError::Incomplete { .. }) => Step::NeedMore,
                Err(e) => Step::Err(e.into()),
            },
            BodyState::UntilClose => {
                if conn.buf.is_empty() {
                    Step::NeedMore
                } else {
                    Step::Data(conn.buf.split().freeze())
                }
            }
        }
    }

    pub(crate) async fn chunk(&mut self) -> Result<Option<Bytes>> {
        loop {
            match self.step() {
                Step::Data(data) => return Ok(Some(data)),
                Step::Last(data) => {
                    self.end();
                    return Ok(Some(data));
                }
                Step::End | Step::Done => {
                    self.end();
                    return Ok(None);
                }
                Step::Failed => return Err(Error::Closed),
                Step::Err(e) => return Err(self.fail(e)),
                Step::NeedMore => {}
            }
            let conn = self
                .conn
                .as_mut()
                .expect("a body in progress has its connection");
            match conn.read_more().await {
                Ok(0) if matches!(self.state, BodyState::UntilClose) => {
                    self.end();
                    return Ok(None);
                }
                Ok(0) => {
                    let e = Error::Protocol("the connection closed before the body ended".into());
                    return Err(self.fail(e));
                }
                Ok(_) => {}
                Err(e) => return Err(self.fail(e)),
            }
        }
    }
}

/// What one look at the buffer found.
enum Step {
    /// Body octets, more to come.
    Data(Bytes),
    /// The last body octets.
    Last(Bytes),
    /// The end of the body.
    End,
    /// The body ended before.
    Done,
    /// The body failed before.
    Failed,
    /// The buffer holds nothing usable yet.
    NeedMore,
    /// The octets are refused.
    Err(Error),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use weida_http_codec::uri::Scheme;
    use weida_runtime::Exec;

    use super::*;
    use crate::pool::Pool;
    use crate::transport::Stream;

    /// One step of a test, expected within ten seconds: past that the wait
    /// reports and goes on instead of failing (`weida_testkit::within`).
    #[track_caller]
    fn within<F: Future>(f: F) -> impl Future<Output = F::Output> {
        let at = std::panic::Location::caller();
        async move {
            let what = format!("the step at {}:{}", at.file(), at.line());
            weida_testkit::within(&what, Duration::from_secs(10), f).await
        }
    }

    fn pair() -> (Conn, DuplexStream) {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Conn::new(Stream::Test(a)), b)
    }

    fn head(method: Method) -> RequestHead {
        RequestHead {
            method,
            scheme: Scheme::Http,
            authority: "example.com".into(),
            path: "/p".into(),
            fields: Fields::new(),
        }
    }

    /// Reads one request head off the server end.
    async fn read_head(server: &mut DuplexStream) -> String {
        let mut got = Vec::new();
        while !got.ends_with(b"\r\n\r\n") {
            let mut b = [0u8; 1];
            server.read_exact(&mut b).await.unwrap();
            got.push(b[0]);
        }
        String::from_utf8(got).unwrap()
    }

    async fn exchange(
        method: Method,
        answer: &'static [u8],
        keep: Option<PoolReturn>,
    ) -> (Response, DuplexStream) {
        let (conn, mut server) = pair();
        let upload = Upload::start(
            conn,
            &head(method),
            OutFraming::None,
            Limits::DEFAULT,
            false,
            keep,
        )
        .unwrap();
        let server_side = async {
            let head = read_head(&mut server).await;
            assert!(
                head.starts_with("GET /p HTTP/1.1\r\nhost: example.com\r\n")
                    || head.starts_with("HEAD")
            );
            server.write_all(answer).await.unwrap();
            server
        };
        let (response, server) =
            within(async { tokio::join!(upload.finish(None), server_side) }).await;
        (response.unwrap(), server)
    }

    async fn body(response: &mut Response) -> Vec<u8> {
        within(response.body.collect(1 << 20))
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn a_content_length_body() {
        let (mut r, _s) = exchange(
            Method::GET,
            b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello",
            None,
        )
        .await;
        assert_eq!(r.head.status, Status::OK);
        assert_eq!(r.body.content_length(), Some(5));
        assert_eq!(body(&mut r).await, b"hello");
    }

    #[tokio::test]
    async fn a_chunked_body_with_trailers() {
        let (mut r, _s) = exchange(
            Method::GET,
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\nx-sum: 5\r\n\r\n",
            None,
        )
        .await;
        assert_eq!(body(&mut r).await, b"abcde");
        assert_eq!(
            r.body.trailers().and_then(|t| t.get("x-sum")),
            Some(&Bytes::from("5"))
        );
    }

    #[tokio::test]
    async fn a_body_until_close() {
        let (mut r, mut s) = exchange(Method::GET, b"HTTP/1.1 200 OK\r\n\r\nall of it", None).await;
        s.shutdown().await.unwrap();
        drop(s);
        assert_eq!(body(&mut r).await, b"all of it");
    }

    #[tokio::test]
    async fn head_and_204_have_no_body() {
        let (mut r, _s) = exchange(
            Method::HEAD,
            b"HTTP/1.1 200 OK\r\ncontent-length: 50\r\n\r\n",
            None,
        )
        .await;
        assert_eq!(body(&mut r).await, b"");
        let (mut r, _s) = exchange(
            Method::GET,
            b"HTTP/1.1 204 No Content\r\ncontent-length: 50\r\n\r\n",
            None,
        )
        .await;
        assert_eq!(body(&mut r).await, b"");
    }

    #[tokio::test]
    async fn interim_responses_are_skipped_and_101_refused() {
        let (mut r, _s) = exchange(
            Method::GET,
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nlink: </a>\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
            None,
        )
        .await;
        assert_eq!(r.head.status, Status::OK);
        assert_eq!(body(&mut r).await, b"ok");

        let (conn, mut server) = pair();
        let upload = Upload::start(
            conn,
            &head(Method::GET),
            OutFraming::None,
            Limits::DEFAULT,
            false,
            None,
        )
        .unwrap();
        server
            .write_all(b"HTTP/1.1 101 Switching Protocols\r\nupgrade: x\r\n\r\n")
            .await
            .unwrap();
        let e = within(upload.finish(None)).await.unwrap_err();
        assert!(matches!(e, Error::Protocol(m) if m.contains("101")));
    }

    fn pool() -> (Arc<Pool>, PoolReturn) {
        let pool = Arc::new(Pool::new(
            Exec::current().unwrap(),
            4,
            Duration::from_secs(90),
        ));
        let keep = PoolReturn::new(&pool, (Scheme::Http, "example.com".into(), 80));
        (pool, keep)
    }

    #[tokio::test]
    async fn keep_alive_returns_the_connection_and_close_does_not() {
        let (pool, keep) = pool();
        let key = keep.key.clone();
        let (mut r, _s) = exchange(
            Method::GET,
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
            Some(keep.clone()),
        )
        .await;
        assert_eq!(pool.idle_count(&key), 0, "not before the body ended");
        body(&mut r).await;
        assert_eq!(pool.idle_count(&key), 1);

        let (mut r, _s) = exchange(
            Method::GET,
            b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 2\r\n\r\nok",
            Some(keep.clone()),
        )
        .await;
        body(&mut r).await;
        let (mut r, _s) = exchange(
            Method::GET,
            b"HTTP/1.0 200 OK\r\ncontent-length: 2\r\n\r\nok",
            Some(keep),
        )
        .await;
        body(&mut r).await;
        assert_eq!(pool.idle_count(&key), 1);
    }

    #[tokio::test]
    async fn collect_refuses_a_declared_length_past_its_cap_before_reading() {
        let (conn, mut server) = pair();
        let upload = Upload::start(
            conn,
            &head(Method::GET),
            OutFraming::None,
            Limits::DEFAULT,
            false,
            None,
        )
        .unwrap();
        // The head declares a gigabyte and nothing of it is ever sent.
        server
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1073741824\r\n\r\n")
            .await
            .unwrap();
        let mut r = within(upload.finish(None)).await.unwrap();
        let e = within(r.body.collect(1024)).await.unwrap_err();
        assert!(matches!(e, Error::TooLarge { max: 1024 }));
    }

    #[tokio::test]
    async fn a_declared_length_is_enforced_on_upload() {
        let (conn, _server) = pair();
        let mut upload = Upload::start(
            conn,
            &head(Method::POST),
            OutFraming::Length(3),
            Limits::DEFAULT,
            false,
            None,
        )
        .unwrap();
        within(upload.write(Bytes::from("ab"))).await.unwrap();
        let e = within(upload.write(Bytes::from("cd"))).await.unwrap_err();
        assert!(matches!(e, Error::Protocol(_)));
    }

    #[tokio::test]
    async fn a_chunked_upload_is_framed() {
        let (conn, mut server) = pair();
        let mut upload = Upload::start(
            conn,
            &head(Method::POST),
            OutFraming::Chunked,
            Limits::DEFAULT,
            false,
            None,
        )
        .unwrap();
        let client = async {
            upload.write(Bytes::from("hello")).await.unwrap();
            upload.finish(None).await
        };
        let server_side = async {
            let head = read_head(&mut server).await;
            assert!(head.contains("transfer-encoding: chunked\r\n"), "{head}");
            let mut rest = vec![0u8; b"5\r\nhello\r\n0\r\n\r\n".len()];
            server.read_exact(&mut rest).await.unwrap();
            assert_eq!(rest, b"5\r\nhello\r\n0\r\n\r\n");
            server
                .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                .await
                .unwrap();
            server
        };
        let (r, _s) = within(async { tokio::join!(client, server_side) }).await;
        assert_eq!(r.unwrap().head.status, Status::NO_CONTENT);
    }
}
