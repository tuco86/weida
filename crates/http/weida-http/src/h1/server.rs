//! The server side of HTTP/1.1: one task per connection, one exchange at a
//! time.
//!
//! The task reads a head, hands the application an [`Exchange`], feeds the
//! request body into the exchange's reader while there is room, writes the
//! response as the application produces it, and only then reads the next
//! head, so pipelined requests are answered in the order they came.

use bytes::Buf;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use weida_http_codec::h1::{self, Chunk, ChunkedDecoder, Framing};
use weida_http_codec::uri::{Target, parse_origin_form};
use weida_http_codec::{DecodeError, Limits};

use crate::body::{BODY_QUEUE, BodyReader, ChannelBody, Event, OutBody, WriteEvent};
use crate::error::Error;
use crate::h1::{Conn, pairs, to_fields};
use crate::server::{ConnCtx, Exchange, ResponseBody, ResponseStart, add_date};
use crate::types::{Fields, Method, RequestHead, Status, Version};

/// How much of a request body the application left unread is read and
/// thrown away to keep the connection; past it the connection closes.
const DRAIN_MAX: usize = 64 * 1024;

/// A request head that passed every check.
struct Parsed {
    head: RequestHead,
    minor: u8,
    framing: Framing,
    keep_alive: bool,
    expect_continue: bool,
}

/// Why no request came: silently, or with a refusal status.
enum NoRequest {
    Silent,
    Refuse(u16),
}

/// Serves `conn` until either side ends it.
pub(crate) async fn serve(mut conn: Conn, ctx: ConnCtx) {
    let mut first = true;
    loop {
        let parsed = tokio::select! {
            parsed = read_request(&mut conn, &ctx, first) => parsed,
            () = ctx.closing() => return,
        };
        let parsed = match parsed {
            Ok(parsed) => parsed,
            Err(NoRequest::Silent) => return,
            Err(NoRequest::Refuse(status)) => {
                refuse(&mut conn, status).await;
                return;
            }
        };
        first = false;
        let keep = tokio::select! {
            keep = exchange(&mut conn, &ctx, parsed) => keep,
            () = ctx.closed() => false,
        };
        if !keep || !ctx.is_open() {
            let _ = conn.io.shutdown().await;
            return;
        }
    }
}

/// Writes a refusal with `connection: close`.
async fn refuse(conn: &mut Conn, status: u16) {
    let status = Status::new(status).expect("a refusal status is three digits");
    let mut fields = Fields::new();
    add_date(&mut fields);
    let mut out = Vec::with_capacity(128);
    let mut list = pairs(&fields);
    list.push((b"content-length", b"0"));
    list.push((b"connection", b"close"));
    h1::encode_response_head(
        1,
        status.as_u16(),
        status.reason().as_bytes(),
        &list,
        &mut out,
    )
    .expect("a refusal head encodes");
    let _ = conn.send(&out).await;
    let _ = conn.io.shutdown().await;
}

fn refusal_for(e: DecodeError) -> NoRequest {
    NoRequest::Refuse(match e {
        DecodeError::TooLarge { .. } => 431,
        DecodeError::Malformed { what: "version" } => 505,
        DecodeError::Malformed {
            what: "transfer coding",
        } => 501,
        _ => 400,
    })
}

/// Reads and checks one request head: the first octet within
/// `header_timeout` (the first request) or `idle_timeout` (later ones), the
/// whole head within `header_timeout` of it.
async fn read_request(
    conn: &mut Conn,
    ctx: &ConnCtx,
    first: bool,
) -> std::result::Result<Parsed, NoRequest> {
    let options = &ctx.options;
    if conn.buf.is_empty() {
        let wait = if first {
            options.header_timeout
        } else {
            options.idle_timeout
        };
        match ctx.exec.within(wait, conn.read_more()).await {
            Some(Ok(n)) if n > 0 => {}
            _ => return Err(NoRequest::Silent),
        }
    }
    let limits = options.limits;
    let parsed = ctx
        .exec
        .within(options.header_timeout, async {
            loop {
                match parse(&conn.buf, limits, ctx) {
                    Ok((parsed, used)) => {
                        conn.buf.advance(used);
                        return Ok(parsed);
                    }
                    Err(Some(refusal)) => return Err(refusal),
                    Err(None) => {}
                }
                match conn.read_more().await {
                    Ok(n) if n > 0 => {}
                    _ => return Err(NoRequest::Silent),
                }
            }
        })
        .await;
    parsed.unwrap_or(Err(NoRequest::Refuse(408)))
}

/// One head off the front of `buf`: `Err(None)` for "not yet".
fn parse(
    buf: &[u8],
    limits: Limits,
    ctx: &ConnCtx,
) -> std::result::Result<(Parsed, usize), Option<NoRequest>> {
    let (head, used) = match h1::decode_request(buf, limits) {
        Ok(decoded) => decoded,
        Err(DecodeError::Incomplete { .. }) => return Err(None),
        Err(e) => return Err(Some(refusal_for(e))),
    };
    let framing = h1::request_framing(&head.fields).map_err(|e| Some(refusal_for(e)))?;
    let bad = || Some(NoRequest::Refuse(400));
    let method = Method::from_bytes(head.method).map_err(|_| bad())?;
    if method == Method::CONNECT {
        return Err(Some(NoRequest::Refuse(501)));
    }

    let hosts: Vec<&[u8]> = head
        .fields
        .iter()
        .filter(|f| f.name.eq_ignore_ascii_case(b"host"))
        .map(|f| f.value)
        .collect();
    if hosts.len() > 1 || (head.minor == 1 && hosts.is_empty()) {
        return Err(bad());
    }
    let host = hosts
        .first()
        .map(|h| std::str::from_utf8(h).map_err(|_| bad()))
        .transpose()?
        .unwrap_or("");

    // Origin form, absolute form (which names the authority itself), or the
    // asterisk of a server-wide OPTIONS (RFC 9112 §3.2).
    let (authority, path) = if head.target.first() == Some(&b'/') {
        let path = parse_origin_form(head.target).map_err(|_| bad())?;
        (host.to_owned(), path.to_owned())
    } else if head.target == b"*" && method == Method::OPTIONS {
        (host.to_owned(), "*".to_owned())
    } else {
        let text = std::str::from_utf8(head.target).map_err(|_| bad())?;
        let target = Target::parse(text).map_err(|_| bad())?;
        (target.authority(), target.path_and_query.into_owned())
    };

    let expect_continue = head.minor == 1
        && head.fields.iter().any(|f| {
            f.name.eq_ignore_ascii_case(b"expect") && f.value.eq_ignore_ascii_case(b"100-continue")
        });
    let keep_alive = !h1::connection_has(&head.fields, "close")
        && (head.minor == 1 || h1::connection_has(&head.fields, "keep-alive"));

    let regular: Vec<_> = head
        .fields
        .iter()
        .filter(|f| {
            !f.name.eq_ignore_ascii_case(b"host")
                && !weida_http_codec::fields::is_connection_specific(f.name)
        })
        .copied()
        .collect();
    let parsed = Parsed {
        head: RequestHead {
            method,
            scheme: ctx.scheme,
            authority,
            path,
            fields: to_fields(&regular),
        },
        minor: head.minor,
        framing,
        keep_alive,
        expect_continue,
    };
    Ok((parsed, used))
}

/// A request body read off the connection, chunk by chunk.
struct Feed {
    state: FeedState,
    limits: Limits,
}

enum FeedState {
    Length(u64),
    Chunked(ChunkedDecoder),
    Done,
}

impl Feed {
    fn new(framing: Framing, limits: Limits) -> Feed {
        let state = match framing {
            Framing::Length(n) if n > 0 => FeedState::Length(n),
            Framing::Chunked => FeedState::Chunked(ChunkedDecoder::new()),
            _ => FeedState::Done,
        };
        Feed { state, limits }
    }

    fn is_done(&self) -> bool {
        matches!(self.state, FeedState::Done)
    }

    /// What the buffer alone yields.
    fn take(&mut self, conn: &mut Conn) -> Option<Event> {
        match &mut self.state {
            FeedState::Done => Some(Event::End(None)),
            FeedState::Length(remaining) => {
                if conn.buf.is_empty() {
                    return None;
                }
                let n = (*remaining).min(conn.buf.len() as u64) as usize;
                let data = conn.buf.split_to(n).freeze();
                *remaining -= n as u64;
                if *remaining == 0 {
                    self.state = FeedState::Done;
                }
                Some(Event::Data(data))
            }
            FeedState::Chunked(decoder) => match decoder.step(&conn.buf, self.limits) {
                Ok((Chunk::Data(data), used)) => {
                    let len = data.len();
                    conn.buf.advance(used - len);
                    Some(Event::Data(conn.buf.split_to(len).freeze()))
                }
                Ok((Chunk::End(trailers), used)) => {
                    let trailers = to_fields(&trailers);
                    conn.buf.advance(used);
                    self.state = FeedState::Done;
                    Some(Event::End((!trailers.is_empty()).then_some(trailers)))
                }
                Err(DecodeError::Incomplete { .. }) => None,
                Err(e) => {
                    self.state = FeedState::Done;
                    Some(Event::Error(e.into()))
                }
            },
        }
    }

    /// The next event; cancel-safe, since a read only appends to the
    /// connection's buffer.
    async fn next(&mut self, conn: &mut Conn) -> Event {
        loop {
            if let Some(event) = self.take(conn) {
                return event;
            }
            match conn.read_more().await {
                Ok(n) if n > 0 => {}
                Ok(_) => {
                    self.state = FeedState::Done;
                    return Event::Error(Error::Protocol(
                        "the client closed before the body ended".into(),
                    ));
                }
                Err(e) => {
                    self.state = FeedState::Done;
                    return Event::Error(e);
                }
            }
        }
    }
}

/// The response as it is being written.
struct Writing {
    rx: mpsc::Receiver<WriteEvent>,
    framing: OutBody,
}

/// The fields of a response head on the wire.
fn head_octets(start: &ResponseStart, close: bool) -> Vec<u8> {
    let mut list = pairs(&start.head.fields);
    if start.framing == OutBody::Open {
        list.push((b"transfer-encoding", b"chunked"));
    }
    if close && !wants_close(&start.head.fields) {
        list.push((b"connection", b"close"));
    }
    let status = start.head.status;
    let mut out = Vec::with_capacity(256);
    h1::encode_response_head(
        1,
        status.as_u16(),
        status.reason().as_bytes(),
        &list,
        &mut out,
    )
    .expect("fields were validated on entry");
    out
}

/// Whether the application asked to close with its response.
fn wants_close(fields: &Fields) -> bool {
    fields.get_all("connection").any(|v| {
        v.split(|&b| b == b',')
            .any(|t| weida_http_codec::fields::trim_ows(t).eq_ignore_ascii_case(b"close"))
    })
}

/// One exchange: hand it over, feed its body, write its response. Returns
/// whether the connection may carry another.
async fn exchange(conn: &mut Conn, ctx: &ConnCtx, parsed: Parsed) -> bool {
    let Parsed {
        head,
        minor,
        framing,
        keep_alive,
        expect_continue,
    } = parsed;
    let head_request = head.method == Method::HEAD;
    let mut feed = Feed::new(framing, ctx.options.limits);
    let (resp_tx, mut resp_rx) = oneshot::channel::<ResponseStart>();

    let (body, mut body_tx, mut continue_rx) = if feed.is_done() {
        (BodyReader::empty(), None, None)
    } else {
        let (tx, rx) = mpsc::channel(BODY_QUEUE);
        let (first_tx, first_rx) = if expect_continue {
            let (t, r) = oneshot::channel();
            (Some(t), Some(r))
        } else {
            (None, None)
        };
        let length = match framing {
            Framing::Length(n) => Some(n),
            _ => None,
        };
        (
            BodyReader::channel(ChannelBody::new(rx, length, first_tx)),
            Some(tx),
            first_rx,
        )
    };
    let exchange = Exchange {
        head,
        version: Version::Http11,
        peer: ctx.peer,
        body,
        responder: crate::server::Responder::h1(resp_tx, head_request, minor),
    };
    if ctx.exchanges.send(exchange).await.is_err() {
        return false;
    }

    let mut permit: Option<mpsc::OwnedPermit<Event>> = None;
    let mut writing: Option<Writing> = None;
    let mut close = !keep_alive;
    let mut continue_sent = false;
    // Whether the reader has had its last event (the end or an error).
    let mut handed_end = body_tx.is_none();
    loop {
        let feeding = !handed_end && body_tx.is_some() && continue_rx.is_none();
        tokio::select! {
            biased;
            start = &mut resp_rx, if writing.is_none() => {
                let Ok(start) = start else {
                    // The responder was dropped unanswered.
                    refuse(conn, 500).await;
                    return false;
                };
                close |= wants_close(&start.head.fields)
                    || start.framing == OutBody::UntilClose
                    || !ctx.is_open()
                    || (expect_continue && !continue_sent && !feed.is_done());
                let mut out = head_octets(&start, close);
                match start.body {
                    ResponseBody::Full(body, done) => {
                        if start.framing != OutBody::None {
                            out.extend_from_slice(&body);
                        }
                        let wrote = conn.send(&out).await;
                        let failed = wrote.is_err();
                        let _ = done.send(wrote);
                        if failed {
                            return false;
                        }
                        break;
                    }
                    ResponseBody::Stream(rx) => {
                        if conn.send(&out).await.is_err() {
                            return false;
                        }
                        writing = Some(Writing { rx, framing: start.framing });
                    }
                }
            }
            event = async { writing.as_mut().expect("guarded").rx.recv().await }, if writing.is_some() => {
                let framing = writing.as_ref().expect("guarded").framing;
                match event {
                    Some(WriteEvent::Data(data)) => {
                        let wrote = if framing == OutBody::Open {
                            let mut chunk = Vec::with_capacity(data.len() + 12);
                            h1::encode_chunk(&data, &mut chunk);
                            conn.send(&chunk).await
                        } else {
                            conn.send(&data).await
                        };
                        if wrote.is_err() {
                            return false;
                        }
                    }
                    Some(WriteEvent::Finish { trailers, done }) => {
                        let wrote = if framing == OutBody::Open {
                            let empty = Fields::new();
                            let mut last = Vec::new();
                            match h1::encode_last_chunk(&pairs(trailers.as_ref().unwrap_or(&empty)), &mut last) {
                                Ok(()) => conn.send(&last).await,
                                Err(e) => Err(e.into()),
                            }
                        } else {
                            Ok(())
                        };
                        let failed = wrote.is_err();
                        let _ = done.send(wrote);
                        if failed {
                            return false;
                        }
                        break;
                    }
                    // The writer was dropped unfinished: the response is cut
                    // off, and only closing says so.
                    None => return false,
                }
            }
            first = async { continue_rx.as_mut().expect("guarded").await }, if continue_rx.is_some() => {
                continue_rx = None;
                if first.is_ok() && writing.is_none() {
                    if conn.send(b"HTTP/1.1 100 Continue\r\n\r\n").await.is_err() {
                        return false;
                    }
                    continue_sent = true;
                }
            }
            reserved = async { body_tx.clone().expect("guarded").reserve_owned().await }, if feeding && permit.is_none() => {
                match reserved {
                    Ok(p) => permit = Some(p),
                    // The application dropped the body: stop handing it over.
                    Err(_) => body_tx = None,
                }
            }
            event = feed.next(conn), if feeding && permit.is_some() => {
                handed_end = matches!(event, Event::End(_) | Event::Error(_));
                permit.take().expect("guarded").send(event);
            }
        }
    }

    // The response is out. Whatever of the body the application did not take
    // is read and dropped, up to a bound; past it the connection closes.
    drop(body_tx);
    if close || !ctx.is_open() {
        return false;
    }
    if expect_continue && !continue_sent && !feed.is_done() {
        return false;
    }
    let mut drained = 0usize;
    while !feed.is_done() {
        let read = ctx
            .exec
            .within(ctx.options.header_timeout, feed.next(conn))
            .await;
        match read {
            Some(Event::Data(data)) => {
                drained += data.len();
                if drained > DRAIN_MAX {
                    return false;
                }
            }
            Some(Event::End(_)) => {}
            Some(Event::Error(_)) | None => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::watch;
    use weida_http_codec::uri::Scheme;
    use weida_runtime::Exec;

    use super::*;
    use crate::options::ServerOptions;
    use crate::server::State;
    use crate::transport::Stream;
    use crate::types::ResponseHead;

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

    struct Harness {
        client: DuplexStream,
        exchanges: mpsc::Receiver<Exchange>,
        _state: watch::Sender<State>,
    }

    fn harness(options: ServerOptions) -> Harness {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (tx, rx) = mpsc::channel(8);
        let (state, state_rx) = watch::channel(State::Open);
        let ctx = ConnCtx {
            exec: Exec::current().unwrap(),
            options: Arc::new(options),
            exchanges: tx,
            state: state_rx,
            peer: SocketAddr::from(([127, 0, 0, 1], 9)),
            scheme: Scheme::Http,
        };
        tokio::spawn(serve(Conn::new(Stream::Test(a)), ctx));
        Harness {
            client: b,
            exchanges: rx,
            _state: state,
        }
    }

    /// Reads one response off the client end: head, then a body of
    /// `content-length` octets.
    async fn response(client: &mut DuplexStream) -> (String, Vec<u8>) {
        let mut got = Vec::new();
        while !got.ends_with(b"\r\n\r\n") {
            let mut b = [0u8; 1];
            client.read_exact(&mut b).await.unwrap();
            got.push(b[0]);
        }
        let head = String::from_utf8(got).unwrap();
        let length = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length: "))
            .map_or(0, |n| n.trim().parse().unwrap());
        let mut body = vec![0; length];
        client.read_exact(&mut body).await.unwrap();
        (head, body)
    }

    async fn answer(exchange: Exchange, body: &'static str) {
        exchange
            .responder
            .respond_bytes(ResponseHead::new(Status::OK), Bytes::from(body))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pipelined_requests_are_answered_in_order() {
        let mut h = harness(ServerOptions::default());
        h.client
            .write_all(b"GET /1 HTTP/1.1\r\nhost: a\r\n\r\nGET /2 HTTP/1.1\r\nhost: a\r\n\r\n")
            .await
            .unwrap();
        let first = within(h.exchanges.recv()).await.unwrap();
        assert_eq!(first.head.path, "/1");
        assert_eq!(first.head.authority, "a");
        // The second is not read before the first is answered.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), h.exchanges.recv())
                .await
                .is_err()
        );
        answer(first, "one").await;
        let second = within(h.exchanges.recv()).await.unwrap();
        assert_eq!(second.head.path, "/2");
        answer(second, "two").await;
        let (head1, body1) = within(response(&mut h.client)).await;
        let (_, body2) = within(response(&mut h.client)).await;
        assert!(head1.starts_with("HTTP/1.1 200 OK\r\n"), "{head1}");
        assert!(head1.contains("\r\ndate: "), "{head1}");
        assert_eq!(
            (body1.as_slice(), body2.as_slice()),
            (&b"one"[..], &b"two"[..])
        );
    }

    #[tokio::test]
    async fn continue_is_sent_only_when_the_body_is_first_read() {
        let mut h = harness(ServerOptions::default());
        h.client
            .write_all(
                b"POST / HTTP/1.1\r\nhost: a\r\nexpect: 100-continue\r\ncontent-length: 3\r\n\r\n",
            )
            .await
            .unwrap();
        let mut ex = within(h.exchanges.recv()).await.unwrap();
        let mut b = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), h.client.read_exact(&mut b))
                .await
                .is_err(),
            "nothing before the body is read"
        );
        let reading = tokio::spawn(async move {
            let body = ex.body.collect(16).await.unwrap();
            (ex, body)
        });
        let mut cont = [0u8; 25];
        within(h.client.read_exact(&mut cont)).await.unwrap();
        assert_eq!(&cont, b"HTTP/1.1 100 Continue\r\n\r\n");
        h.client.write_all(b"abc").await.unwrap();
        let (ex, body) = within(reading).await.unwrap();
        assert_eq!(body, "abc");
        answer(ex, "").await;
        let (head, _) = within(response(&mut h.client)).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    }

    async fn refused(request: &[u8], options: ServerOptions) -> String {
        let mut h = harness(options);
        h.client.write_all(request).await.unwrap();
        let mut all = Vec::new();
        within(h.client.read_to_end(&mut all)).await.unwrap();
        String::from_utf8(all).unwrap()
    }

    #[tokio::test]
    async fn bad_heads_are_refused_with_their_status() {
        let d = ServerOptions::default;
        for (request, status) in [
            (&b"GET / HTTP/1.1\r\nhost : a\r\n\r\n"[..], "400"),
            (b"GET / HTTP/1.1\r\n\r\n", "400"),
            (b"GET / HTTP/1.1\r\nhost: a\r\nhost: b\r\n\r\n", "400"),
            (
                b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: gzip\r\n\r\n",
                "501",
            ),
            (b"CONNECT a:443 HTTP/1.1\r\nhost: a:443\r\n\r\n", "501"),
            (b"GET / HTTP/2.0\r\nhost: a\r\n\r\n", "505"),
        ] {
            let answer = refused(request, d()).await;
            assert!(
                answer.starts_with(&format!("HTTP/1.1 {status} ")),
                "{answer}"
            );
            assert!(answer.contains("connection: close\r\n"), "{answer}");
        }
        let small = ServerOptions {
            limits: Limits {
                max_head_bytes: 64,
                ..Limits::DEFAULT
            },
            ..d()
        };
        let mut big = b"GET / HTTP/1.1\r\nhost: a\r\nx: ".to_vec();
        big.extend_from_slice(&[b'a'; 100]);
        big.extend_from_slice(b"\r\n\r\n");
        assert!(refused(&big, small).await.starts_with("HTTP/1.1 431 "));
    }

    #[tokio::test(start_paused = true)]
    async fn a_head_that_stalls_gets_408_and_silence_gets_nothing() {
        let options = || ServerOptions {
            header_timeout: Duration::from_secs(2),
            ..ServerOptions::default()
        };
        let answer = refused(b"GET / HTTP/1.1\r\nhost:", options()).await;
        assert!(answer.starts_with("HTTP/1.1 408 "), "{answer}");
        assert_eq!(refused(b"", options()).await, "");
    }

    #[tokio::test]
    async fn a_dropped_responder_answers_500_and_closes() {
        let mut h = harness(ServerOptions::default());
        h.client
            .write_all(b"GET / HTTP/1.1\r\nhost: a\r\n\r\n")
            .await
            .unwrap();
        drop(within(h.exchanges.recv()).await.unwrap());
        let mut all = Vec::new();
        within(h.client.read_to_end(&mut all)).await.unwrap();
        let all = String::from_utf8(all).unwrap();
        assert!(all.starts_with("HTTP/1.1 500 "), "{all}");
        assert!(all.contains("connection: close"), "{all}");
    }

    #[tokio::test]
    async fn head_gets_no_body() {
        let mut h = harness(ServerOptions::default());
        h.client
            .write_all(b"HEAD / HTTP/1.1\r\nhost: a\r\n\r\nGET / HTTP/1.1\r\nhost: a\r\n\r\n")
            .await
            .unwrap();
        let ex = within(h.exchanges.recv()).await.unwrap();
        answer(ex, "hello").await;
        let ex = within(h.exchanges.recv()).await.unwrap();
        answer(ex, "world").await;
        // The HEAD answer declares 5 octets and carries none: the next
        // octets on the wire are the second response.
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut b = [0u8; 1];
            within(h.client.read_exact(&mut b)).await.unwrap();
            head.push(b[0]);
        }
        assert!(String::from_utf8_lossy(&head).contains("content-length: 5"));
        let (second, body) = within(response(&mut h.client)).await;
        assert!(second.starts_with("HTTP/1.1 200"), "{second}");
        assert_eq!(body, b"world");
    }

    #[tokio::test]
    async fn a_streamed_response_is_chunked_with_trailers() {
        let mut h = harness(ServerOptions::default());
        h.client
            .write_all(b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nx-in: 1\r\n\r\n")
            .await
            .unwrap();
        let mut ex = within(h.exchanges.recv()).await.unwrap();
        assert_eq!(within(ex.body.collect(16)).await.unwrap(), "abc");
        assert_eq!(
            ex.body
                .trailers()
                .and_then(|t| t.get("x-in"))
                .map(|v| &v[..]),
            Some(&b"1"[..])
        );
        let mut w = ex
            .responder
            .respond(ResponseHead::new(Status::OK))
            .await
            .unwrap();
        w.write(Bytes::from("de")).await.unwrap();
        let mut trailers = Fields::new();
        trailers.append("x-out", "2").unwrap();
        within(w.finish_with_trailers(trailers)).await.unwrap();
        let mut got = vec![0u8; 0];
        while !got.ends_with(b"x-out: 2\r\n\r\n") {
            let mut b = [0u8; 1];
            within(h.client.read_exact(&mut b)).await.unwrap();
            got.push(b[0]);
        }
        let got = String::from_utf8(got).unwrap();
        assert!(
            got.contains("transfer-encoding: chunked\r\n\r\n2\r\nde\r\n0\r\n"),
            "{got}"
        );
    }
}
