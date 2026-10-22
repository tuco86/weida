//! HTTP/3 over QUIC (RFC 9114), on quinn, behind the feature `h3`.
//!
//! QUIC does what an HTTP/2 connection driver does by hand: streams, their
//! flow control and their resets. What is left is HTTP/3's own framing on
//! each stream, the control stream with SETTINGS and GOAWAY, and QPACK, which
//! this library uses with the static table only (0044 §4): both sides
//! advertise `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0`, so no encoder or decoder
//! stream is opened (RFC 9204 §4.2), the peer's are drained unread, and a
//! field section that refers to a dynamic table closes the connection with
//! `QPACK_DECOMPRESSION_FAILED`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::{Buf, Bytes, BytesMut};
use quinn::VarInt;
use tokio::sync::mpsc;
use weida_http_codec::h3::{self as wire, FrameHeader, code, frame, setting, stream};
use weida_http_codec::{DecodeError, Limits, qpack, varint};

use crate::body::{BodyReader, BodyWriter, OutBody};
use crate::client::Response;
use crate::error::{Error, Result};
use crate::h2::conn::request_head;
use crate::server::{ConnCtx, Exchange, Responder};
use crate::types::{Fields, Method, RequestHead, ResponseHead, Version};

/// The most one read takes off a stream.
const READ_CHUNK: usize = 16 * 1024;

fn varint(v: u64) -> VarInt {
    VarInt::from_u64(v).unwrap_or(VarInt::MAX)
}

fn close(conn: &quinn::Connection, code: u64, why: &str) {
    conn.close(varint(code), why.as_bytes());
}

/// What a stream read failure means to the caller.
fn read_error(e: quinn::ReadError) -> Error {
    match e {
        quinn::ReadError::Reset(code) if code.into_inner() == code::H3_REQUEST_REJECTED => {
            Error::GoingAway
        }
        quinn::ReadError::Reset(code) => Error::Reset {
            code: code.into_inner(),
        },
        quinn::ReadError::ConnectionLost(e) => connection_error(e),
        other => Error::Protocol(other.to_string()),
    }
}

fn write_error(e: quinn::WriteError) -> Error {
    match e {
        quinn::WriteError::Stopped(code) if code.into_inner() == code::H3_REQUEST_REJECTED => {
            Error::GoingAway
        }
        quinn::WriteError::Stopped(code) => Error::Reset {
            code: code.into_inner(),
        },
        quinn::WriteError::ConnectionLost(e) => connection_error(e),
        other => Error::Protocol(other.to_string()),
    }
}

fn connection_error(e: quinn::ConnectionError) -> Error {
    match e {
        quinn::ConnectionError::ApplicationClosed(close)
            if close.error_code.into_inner() == code::H3_NO_ERROR =>
        {
            Error::GoingAway
        }
        quinn::ConnectionError::LocallyClosed => Error::Closed,
        other => Error::Protocol(other.to_string()),
    }
}

/// A stream's frames, read off it into one buffer.
struct Frames {
    recv: quinn::RecvStream,
    buf: BytesMut,
    ended: bool,
}

impl Frames {
    fn new(recv: quinn::RecvStream) -> Frames {
        Frames {
            recv,
            buf: BytesMut::new(),
            ended: false,
        }
    }

    /// Reads more onto the buffer; `false` at the end of the stream.
    async fn fill(&mut self) -> Result<bool> {
        if self.ended {
            return Ok(false);
        }
        match self.recv.read_chunk(READ_CHUNK, true).await {
            Ok(Some(chunk)) => {
                self.buf.extend_from_slice(&chunk.bytes);
                Ok(true)
            }
            Ok(None) => {
                self.ended = true;
                Ok(false)
            }
            Err(e) => Err(read_error(e)),
        }
    }

    /// A stream type or other lone integer.
    async fn varint(&mut self) -> Result<Option<u64>> {
        loop {
            match varint::decode(&self.buf) {
                Ok((v, used)) => {
                    self.buf.advance(used);
                    return Ok(Some(v));
                }
                Err(_) => {
                    if !self.fill().await? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// The next frame header, or `None` at a clean end of the stream.
    async fn header(&mut self) -> Result<Option<FrameHeader>> {
        loop {
            match wire::decode_frame_header(&self.buf) {
                Ok((h, used)) => {
                    self.buf.advance(used);
                    return Ok(Some(h));
                }
                Err(_) => {
                    if !self.fill().await? {
                        return if self.buf.is_empty() {
                            Ok(None)
                        } else {
                            Err(Error::Protocol(
                                "a frame cut off by the end of its stream".into(),
                            ))
                        };
                    }
                }
            }
        }
    }

    /// A whole frame payload of `len` octets, refused above `cap` before it
    /// is read.
    async fn payload(&mut self, len: u64, cap: usize) -> Result<Bytes> {
        if len > cap as u64 {
            return Err(Error::TooLarge { max: cap });
        }
        let len = len as usize;
        while self.buf.len() < len {
            if !self.fill().await? {
                return Err(Error::Protocol(
                    "a frame cut off by the end of its stream".into(),
                ));
            }
        }
        Ok(self.buf.split_to(len).freeze())
    }

    /// Up to `remaining` octets of a DATA frame's payload.
    async fn data(&mut self, remaining: &mut u64) -> Result<Bytes> {
        if self.buf.is_empty() && !self.fill().await? {
            return Err(Error::Protocol(
                "a DATA frame cut off by the end of its stream".into(),
            ));
        }
        let n = (*remaining).min(self.buf.len() as u64) as usize;
        *remaining -= n as u64;
        Ok(self.buf.split_to(n).freeze())
    }

    /// Steps over `len` octets of an unknown frame.
    async fn skip(&mut self, mut len: u64) -> Result<()> {
        while len > 0 {
            if self.buf.is_empty() && !self.fill().await? {
                return Err(Error::Protocol(
                    "a frame cut off by the end of its stream".into(),
                ));
            }
            let n = len.min(self.buf.len() as u64) as usize;
            self.buf.advance(n);
            len -= n as u64;
        }
        Ok(())
    }
}

/// Decoded `(name, value)` pairs.
type FieldList = Vec<(Vec<u8>, Vec<u8>)>;

fn decode_fields(block: &[u8], limits: Limits) -> std::result::Result<FieldList, DecodeError> {
    let mut out = Vec::new();
    qpack::decode_field_section(block, limits, &mut out)?;
    Ok(out)
}

fn to_fields(fields: &[(Vec<u8>, Vec<u8>)]) -> Fields {
    let mut out = Fields::new();
    for (n, v) in fields {
        out.push_decoded(n, Bytes::copy_from_slice(v));
    }
    out
}

/// A field section that cannot be decoded: a dynamic-table reference is the
/// connection's error, anything else the message's.
fn field_error(conn: &quinn::Connection, e: DecodeError) -> Error {
    if matches!(
        e,
        DecodeError::Malformed {
            what: "dynamic table reference"
        }
    ) {
        close(
            conn,
            code::QPACK_DECOMPRESSION_FAILED,
            "a dynamic table reference",
        );
    }
    Error::Protocol(format!("a field section: {e}"))
}

/// Frames that never belong on a request stream.
fn unexpected_on_request(kind: u64) -> bool {
    wire::is_http2_reserved(kind)
        || matches!(
            kind,
            frame::SETTINGS
                | frame::GOAWAY
                | frame::MAX_PUSH_ID
                | frame::CANCEL_PUSH
                | frame::PUSH_PROMISE
        )
}

/// The body of an HTTP/3 message, read frame by frame off its stream.
pub(crate) struct H3Reader {
    frames: Frames,
    conn: quinn::Connection,
    limits: Limits,
    remaining: u64,
    content_length: Option<u64>,
    received: u64,
    trailers: Option<Fields>,
    done: bool,
    stop_code: u64,
}

impl std::fmt::Debug for H3Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3Reader")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl H3Reader {
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
        let result = self.next().await;
        if result.is_err() {
            self.done = true;
        }
        result
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        loop {
            if self.remaining > 0 {
                let data = self.frames.data(&mut self.remaining).await?;
                self.received += data.len() as u64;
                if self.content_length.is_some_and(|n| self.received > n) {
                    return Err(self.message_error("more body than its content-length"));
                }
                return Ok(Some(data));
            }
            let Some(h) = self.frames.header().await? else {
                if self.content_length.is_some_and(|n| self.received != n) {
                    return Err(self.message_error("a body short of its content-length"));
                }
                self.done = true;
                return Ok(None);
            };
            match h.kind {
                frame::DATA => self.remaining = h.len,
                frame::HEADERS => {
                    let block = self
                        .frames
                        .payload(h.len, self.limits.max_head_bytes)
                        .await?;
                    let fields = decode_fields(&block, self.limits)
                        .map_err(|e| field_error(&self.conn, e))?;
                    if fields.iter().any(|(n, _)| n.starts_with(b":")) {
                        return Err(self.message_error("a pseudo-header in trailers"));
                    }
                    // Only unknown (reserved, grease) frames may follow
                    // trailers (RFC 9114 §4.1, §9).
                    while let Some(next) = self.frames.header().await? {
                        if next.kind == frame::DATA
                            || next.kind == frame::HEADERS
                            || unexpected_on_request(next.kind)
                        {
                            close(
                                &self.conn,
                                code::H3_FRAME_UNEXPECTED,
                                "a frame after trailers",
                            );
                            return Err(Error::Protocol("a frame after trailers".into()));
                        }
                        self.frames.skip(next.len).await?;
                    }
                    if self.content_length.is_some_and(|n| self.received != n) {
                        return Err(self.message_error("a body short of its content-length"));
                    }
                    self.trailers = Some(to_fields(&fields));
                    self.done = true;
                    return Ok(None);
                }
                kind if unexpected_on_request(kind) => {
                    close(
                        &self.conn,
                        code::H3_FRAME_UNEXPECTED,
                        "a frame not allowed on a request stream",
                    );
                    return Err(Error::Protocol(
                        "a frame not allowed on a request stream".into(),
                    ));
                }
                _ => self.frames.skip(h.len).await?,
            }
        }
    }

    fn message_error(&mut self, why: &str) -> Error {
        let _ = self.frames.recv.stop(varint(code::H3_MESSAGE_ERROR));
        Error::Protocol(why.into())
    }
}

impl Drop for H3Reader {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.frames.recv.stop(varint(self.stop_code));
        }
    }
}

/// Keeps a server connection open while one response is unfinished: the
/// connection closes on `Server::close` only once every guard is gone.
struct InFlight {
    count: Arc<AtomicUsize>,
    done: Arc<tokio::sync::Notify>,
    exec: weida_runtime::Exec,
}

impl InFlight {
    fn new(
        count: &Arc<AtomicUsize>,
        done: &Arc<tokio::sync::Notify>,
        exec: &weida_runtime::Exec,
    ) -> InFlight {
        count.fetch_add(1, Ordering::AcqRel);
        InFlight {
            count: count.clone(),
            done: done.clone(),
            exec: exec.clone(),
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if self.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.done.notify_one();
        }
    }
}

/// How long a finished response's stream is watched for the peer's
/// acknowledgement before its guard is let go.
const LINGER: std::time::Duration = std::time::Duration::from_secs(10);

/// Lets `guard` go once the peer acknowledged everything on `send`, so that
/// closing the connection does not cut off what was already written.
fn linger(send: quinn::SendStream, guard: Option<InFlight>) {
    let Some(guard) = guard else {
        return;
    };
    let exec = guard.exec.clone();
    exec.clone().spawn(async move {
        let _ = exec.within(LINGER, send.stopped()).await;
        drop(guard);
    });
}

/// The send side of an HTTP/3 message.
pub(crate) struct H3Writer {
    send: Option<quinn::SendStream>,
    finished: bool,
    guard: Option<InFlight>,
}

impl std::fmt::Debug for H3Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3Writer")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl H3Writer {
    fn new(send: quinn::SendStream, guard: Option<InFlight>) -> H3Writer {
        H3Writer {
            send: Some(send),
            finished: false,
            guard,
        }
    }

    pub(crate) async fn write(&mut self, data: Bytes) -> Result<()> {
        let send = self.send.as_mut().ok_or(Error::Closed)?;
        let mut head = Vec::with_capacity(16);
        wire::encode_data_header(data.len() as u64, &mut head)?;
        if data.len() <= READ_CHUNK {
            head.extend_from_slice(&data);
            return send.write_all(&head).await.map_err(write_error);
        }
        send.write_all(&head).await.map_err(write_error)?;
        send.write_all(&data).await.map_err(write_error)
    }

    pub(crate) async fn finish(mut self, trailers: Option<Fields>) -> Result<()> {
        self.finished = true;
        let mut send = self.send.take().ok_or(Error::Closed)?;
        if let Some(trailers) = trailers {
            let fields: Vec<(&[u8], &[u8], bool)> = trailers
                .iter_full()
                .map(|(n, v, s)| (n.as_bytes(), &v[..], s))
                .collect();
            let mut block = Vec::new();
            qpack::encode_field_section(&fields, &mut block);
            let mut out = Vec::with_capacity(block.len() + 8);
            wire::encode_headers(&block, &mut out);
            send.write_all(&out).await.map_err(write_error)?;
        }
        send.finish().map_err(|_| Error::Closed)?;
        linger(send, self.guard.take());
        Ok(())
    }
}

impl Drop for H3Writer {
    fn drop(&mut self) {
        if !self.finished
            && let Some(send) = self.send.as_mut()
        {
            let _ = send.reset(varint(code::H3_REQUEST_CANCELLED));
        }
    }
}

/// Writes a HEADERS frame for `fields`.
async fn send_headers(send: &mut quinn::SendStream, fields: &[(&[u8], &[u8], bool)]) -> Result<()> {
    let mut block = Vec::new();
    qpack::encode_field_section(fields, &mut block);
    let mut out = Vec::with_capacity(block.len() + 8);
    wire::encode_headers(&block, &mut out);
    send.write_all(&out).await.map_err(write_error)
}

/// What both roles share about one connection.
#[derive(Debug, Default)]
struct Shared {
    /// A GOAWAY arrived: no new requests.
    going_away: AtomicBool,
    /// Whether the peer's control stream was seen.
    peer_control: AtomicBool,
}

/// The control stream and the peer's unidirectional streams.
///
/// Opens ours with SETTINGS and keeps it for the connection's life (closing
/// it is a connection error, RFC 9114 §6.2.1); `goaway` carries a GOAWAY to
/// write on it. Accepts the peer's and handles them by type.
fn start_control(
    exec: &weida_runtime::Exec,
    conn: &quinn::Connection,
    limits: Limits,
    server: bool,
    shared: Arc<Shared>,
) -> mpsc::UnboundedSender<u64> {
    let (goaway_tx, mut goaway_rx) = mpsc::unbounded_channel::<u64>();
    let control = conn.clone();
    exec.spawn(async move {
        let Ok(mut send) = control.open_uni().await else {
            return;
        };
        let mut out = Vec::new();
        varint::encode(stream::CONTROL, &mut out).expect("a small integer");
        wire::encode_settings(
            &[
                (setting::MAX_FIELD_SECTION_SIZE, limits.max_head_bytes as u64),
                (setting::QPACK_MAX_TABLE_CAPACITY, 0),
                (setting::QPACK_BLOCKED_STREAMS, 0),
            ],
            &mut out,
        )
        .expect("small integers");
        if send.write_all(&out).await.is_err() {
            return;
        }
        loop {
            tokio::select! {
                id = goaway_rx.recv() => {
                    let Some(id) = id else { break };
                    let mut out = Vec::new();
                    if wire::encode_goaway(id, &mut out).is_ok() && send.write_all(&out).await.is_err() {
                        break;
                    }
                }
                _ = control.closed() => break,
            }
        }
        // Kept open until the connection ends.
        control.closed().await;
        drop(send);
    });
    let accept = conn.clone();
    let accept_exec = exec.clone();
    exec.spawn(async move {
        while let Ok(recv) = accept.accept_uni().await {
            let conn = accept.clone();
            let shared = shared.clone();
            accept_exec.spawn(async move {
                let _ = peer_uni(conn, recv, limits, server, shared).await;
            });
        }
    });
    goaway_tx
}

/// One of the peer's unidirectional streams.
async fn peer_uni(
    conn: quinn::Connection,
    recv: quinn::RecvStream,
    limits: Limits,
    server: bool,
    shared: Arc<Shared>,
) -> Result<()> {
    let mut frames = Frames::new(recv);
    let Some(kind) = frames.varint().await? else {
        return Ok(());
    };
    match kind {
        stream::CONTROL => {
            if shared.peer_control.swap(true, Ordering::AcqRel) {
                close(
                    &conn,
                    code::H3_STREAM_CREATION_ERROR,
                    "a second control stream",
                );
                return Ok(());
            }
            control_stream(&conn, &mut frames, limits, &shared).await
        }
        stream::QPACK_ENCODER | stream::QPACK_DECODER => {
            // With a table capacity of 0 there is nothing to read here; what
            // arrives anyway is drained.
            while frames.fill().await? {
                frames.buf.clear();
            }
            Ok(())
        }
        stream::PUSH => {
            let code = if server {
                code::H3_STREAM_CREATION_ERROR
            } else {
                code::H3_ID_ERROR
            };
            close(&conn, code, "a push stream");
            Ok(())
        }
        _ => {
            let _ = frames.recv.stop(varint(code::H3_STREAM_CREATION_ERROR));
            Ok(())
        }
    }
}

async fn control_stream(
    conn: &quinn::Connection,
    frames: &mut Frames,
    limits: Limits,
    shared: &Shared,
) -> Result<()> {
    let first = frames.header().await?;
    match first {
        Some(h) if h.kind == frame::SETTINGS => {
            let payload = frames
                .payload(h.len, limits.max_frame_size as usize)
                .await?;
            if wire::decode_settings(&payload, limits).is_err() {
                close(conn, code::H3_SETTINGS_ERROR, "SETTINGS");
                return Ok(());
            }
        }
        _ => {
            close(
                conn,
                code::H3_MISSING_SETTINGS,
                "the control stream did not start with SETTINGS",
            );
            return Ok(());
        }
    }
    loop {
        let Some(h) = frames.header().await? else {
            close(
                conn,
                code::H3_CLOSED_CRITICAL_STREAM,
                "the control stream ended",
            );
            return Ok(());
        };
        match h.kind {
            frame::GOAWAY => {
                let payload = frames.payload(h.len, 16).await?;
                if wire::decode_goaway(&payload).is_err() {
                    close(conn, code::H3_FRAME_ERROR, "GOAWAY");
                    return Ok(());
                }
                shared.going_away.store(true, Ordering::Release);
            }
            frame::SETTINGS | frame::DATA | frame::HEADERS | frame::PUSH_PROMISE => {
                close(
                    conn,
                    code::H3_FRAME_UNEXPECTED,
                    "a frame not allowed on the control stream",
                );
                return Ok(());
            }
            kind if wire::is_http2_reserved(kind) => {
                close(conn, code::H3_FRAME_UNEXPECTED, "an HTTP/2 frame type");
                return Ok(());
            }
            _ => frames.skip(h.len).await?,
        }
    }
}

/// A client's HTTP/3 connection to one origin.
#[derive(Clone, Debug)]
pub(crate) struct H3Handle {
    conn: quinn::Connection,
    shared: Arc<Shared>,
    limits: Limits,
}

impl H3Handle {
    pub(crate) fn is_usable(&self) -> bool {
        self.conn.close_reason().is_none() && !self.shared.going_away.load(Ordering::Acquire)
    }

    /// Opens a request stream and sends `head`.
    pub(crate) async fn open(
        &self,
        head: &RequestHead,
        length: Option<u64>,
        end_stream: bool,
    ) -> Result<(Option<H3Writer>, H3Response)> {
        if !self.is_usable() {
            return Err(Error::GoingAway);
        }
        let (mut send, recv) = self.conn.open_bi().await.map_err(connection_error)?;
        let length_text = length.map(|n| n.to_string());
        let mut fields: Vec<(&[u8], &[u8], bool)> = vec![
            (b":method", head.method.as_str().as_bytes(), false),
            (b":scheme", head.scheme.as_str().as_bytes(), false),
            (b":authority", head.authority.as_bytes(), false),
            (b":path", head.path.as_bytes(), false),
        ];
        fields.extend(
            head.fields
                .iter_full()
                .map(|(n, v, s)| (n.as_bytes(), &v[..], s)),
        );
        if let Some(n) = &length_text {
            fields.push((b"content-length", n.as_bytes(), false));
        }
        send_headers(&mut send, &fields).await?;
        let writer = if end_stream {
            send.finish().map_err(|_| Error::Closed)?;
            None
        } else {
            Some(H3Writer::new(send, None))
        };
        Ok((
            writer,
            H3Response {
                frames: Frames::new(recv),
                conn: self.conn.clone(),
                limits: self.limits,
                head_request: head.method == Method::HEAD,
            },
        ))
    }
}

/// Where a client's response arrives.
pub(crate) struct H3Response {
    frames: Frames,
    conn: quinn::Connection,
    limits: Limits,
    head_request: bool,
}

impl std::fmt::Debug for H3Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3Response").finish_non_exhaustive()
    }
}

impl H3Response {
    /// Reads the response head, skipping interim answers.
    pub(crate) async fn head(mut self) -> Result<Response> {
        loop {
            let Some(h) = self.frames.header().await? else {
                return Err(Error::Protocol(
                    "the stream ended before the response head".into(),
                ));
            };
            match h.kind {
                frame::HEADERS => {}
                kind if unexpected_on_request(kind) || kind == frame::DATA => {
                    close(
                        &self.conn,
                        code::H3_FRAME_UNEXPECTED,
                        "a frame before the response head",
                    );
                    return Err(Error::Protocol("a frame before the response head".into()));
                }
                _ => {
                    self.frames.skip(h.len).await?;
                    continue;
                }
            }
            let block = self
                .frames
                .payload(h.len, self.limits.max_head_bytes)
                .await?;
            let fields =
                decode_fields(&block, self.limits).map_err(|e| field_error(&self.conn, e))?;
            let Some((status, head_fields, length)) = crate::h2::conn::response_head(&fields)
            else {
                let _ = self.frames.recv.stop(varint(code::H3_MESSAGE_ERROR));
                return Err(Error::Protocol("a malformed response head".into()));
            };
            if status.is_informational() {
                continue;
            }
            let no_body = self.head_request || status.as_u16() == 204 || status.as_u16() == 304;
            let body = BodyReader::h3(H3Reader {
                frames: self.frames,
                conn: self.conn,
                limits: self.limits,
                remaining: 0,
                content_length: if no_body { None } else { length },
                received: 0,
                trailers: None,
                done: false,
                stop_code: code::H3_REQUEST_CANCELLED,
            });
            return Ok(Response {
                head: ResponseHead {
                    status,
                    fields: head_fields,
                },
                version: Version::Http3,
                body,
            });
        }
    }
}

/// QUIC's transport parameters for HTTP/3: our windows, three unidirectional
/// streams (control, and the QPACK pair a peer may open), and the
/// bidirectional limit given (0 for a client, which serves no requests).
fn transport(
    stream_window: u32,
    connection_window: u32,
    bidi: u32,
    idle: Option<std::time::Duration>,
) -> quinn::TransportConfig {
    let mut t = quinn::TransportConfig::default();
    t.stream_receive_window(VarInt::from_u32(stream_window));
    t.receive_window(VarInt::from_u32(connection_window));
    t.max_concurrent_uni_streams(VarInt::from_u32(3));
    t.max_concurrent_bidi_streams(VarInt::from_u32(bidi));
    if let Some(idle) = idle.and_then(|d| quinn::IdleTimeout::try_from(d).ok()) {
        t.max_idle_timeout(Some(idle));
    }
    t
}

/// The client side: QUIC endpoints made on first use, one per address family.
#[derive(Debug, Default)]
pub(crate) struct ClientEndpoints {
    v4: std::sync::Mutex<Option<quinn::Endpoint>>,
    v6: std::sync::Mutex<Option<quinn::Endpoint>>,
}

impl ClientEndpoints {
    fn get(&self, exec: &weida_runtime::Exec, v6: bool) -> Result<quinn::Endpoint> {
        let slot = if v6 { &self.v6 } else { &self.v4 };
        let mut slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(endpoint) = slot.as_ref() {
            return Ok(endpoint.clone());
        }
        let bind: SocketAddr = if v6 {
            (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
        } else {
            (std::net::Ipv4Addr::UNSPECIFIED, 0).into()
        };
        let _guard = exec.enter();
        let endpoint = quinn::Endpoint::client(bind)?;
        *slot = Some(endpoint.clone());
        Ok(endpoint)
    }
}

/// What a client dial needs.
pub(crate) struct Dial<'a> {
    pub(crate) exec: &'a weida_runtime::Exec,
    pub(crate) endpoints: &'a ClientEndpoints,
    pub(crate) tls: &'a Arc<rustls::ClientConfig>,
    pub(crate) host: &'a str,
    pub(crate) addrs: &'a [SocketAddr],
    pub(crate) budget: &'a crate::dial::Budget,
    pub(crate) stream_window: u32,
    pub(crate) connection_window: u32,
    pub(crate) limits: Limits,
}

/// How long a handshake to one address runs alone before the next address
/// is tried beside it (RFC 8305 §5's "Connection Attempt Delay").
const ATTEMPT_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

/// Opens an HTTP/3 connection to the first of `addrs` that answers.
///
/// UDP gives no refusal to wait for: a handshake to an address nothing
/// listens on simply goes unanswered. So the addresses are raced rather than
/// tried in turn, each starting [`ATTEMPT_DELAY`] after the one before, and
/// the first handshake to finish wins.
pub(crate) async fn connect(d: Dial<'_>) -> Result<H3Handle> {
    let mut tls = (**d.tls).clone();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| Error::Tls(format!("building the QUIC crypto: {e}")))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport(
        d.stream_window,
        d.connection_window,
        0,
        None,
    )));
    let mut last = Error::Timeout {
        step: "the QUIC handshake",
    };
    let (tx, mut rx) = mpsc::channel(d.addrs.len().max(1));
    let mut attempts = Vec::new();
    for (index, addr) in d.addrs.iter().enumerate() {
        let endpoint = match d.endpoints.get(d.exec, addr.is_ipv6()) {
            Ok(endpoint) => endpoint,
            Err(e) => {
                last = e;
                continue;
            }
        };
        let connecting = match endpoint.connect_with(config.clone(), *addr, d.host) {
            Ok(c) => c,
            Err(e) => {
                last = Error::Tls(e.to_string());
                continue;
            }
        };
        let tx = tx.clone();
        let exec = d.exec.clone();
        let delay = ATTEMPT_DELAY * index as u32;
        attempts.push(d.exec.spawn(async move {
            exec.sleep(delay).await;
            let _ = tx.send(connecting.await).await;
        }));
    }
    drop(tx);
    let first = d
        .exec
        .within(d.budget.left(d.exec), async move {
            while let Some(result) = rx.recv().await {
                match result {
                    Ok(conn) => return Ok(conn),
                    Err(e) => last = connection_error(e),
                }
            }
            Err(last)
        })
        .await;
    for attempt in attempts {
        attempt.abort();
    }
    let conn = match first {
        Some(result) => result?,
        None => {
            return Err(Error::Timeout {
                step: "the QUIC handshake",
            });
        }
    };
    let shared = Arc::new(Shared::default());
    // A client sends no GOAWAY of its own; the sender is dropped.
    let _ = start_control(d.exec, &conn, d.limits, false, shared.clone());
    Ok(H3Handle {
        conn,
        shared,
        limits: d.limits,
    })
}

/// A server's answer to one HTTP/3 request.
pub(crate) struct H3Responder {
    send: Option<quinn::SendStream>,
    head_request: bool,
    guard: Option<InFlight>,
}

impl std::fmt::Debug for H3Responder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H3Responder").finish_non_exhaustive()
    }
}

impl H3Responder {
    pub(crate) fn head_request(&self) -> bool {
        self.head_request
    }

    /// Sends the head; finishes the stream when no body follows.
    pub(crate) async fn send_head(
        &mut self,
        head: ResponseHead,
        end_stream: bool,
    ) -> Result<Option<H3Writer>> {
        let mut send = self.send.take().ok_or(Error::Closed)?;
        let mut head = head;
        head.fields.remove("connection");
        let status = head.status.as_u16().to_string();
        let mut fields: Vec<(&[u8], &[u8], bool)> = vec![(b":status", status.as_bytes(), false)];
        fields.extend(
            head.fields
                .iter_full()
                .map(|(n, v, s)| (n.as_bytes(), &v[..], s)),
        );
        send_headers(&mut send, &fields).await?;
        if end_stream {
            send.finish().map_err(|_| Error::Closed)?;
            linger(send, self.guard.take());
            return Ok(None);
        }
        Ok(Some(H3Writer::new(send, self.guard.take())))
    }
}

impl Drop for H3Responder {
    fn drop(&mut self) {
        if let Some(send) = self.send.as_mut() {
            let _ = send.reset(varint(code::H3_INTERNAL_ERROR));
        }
    }
}

/// Builds the server side's QUIC configuration from the caller's
/// certificate configuration.
pub(crate) fn server_config(
    config: &rustls::ServerConfig,
    options: &crate::options::ServerOptions,
) -> Result<quinn::ServerConfig> {
    let mut tls = config.clone();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|e| Error::Tls(format!("building the QUIC crypto: {e}")))?;
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    server.transport_config(Arc::new(transport(
        options.stream_window,
        options.connection_window,
        options.max_concurrent_streams,
        Some(options.idle_timeout),
    )));
    Ok(server)
}

/// Serves one QUIC connection until it ends.
pub(crate) async fn serve(incoming: quinn::Incoming, ctx: ConnCtx) {
    let options = ctx.options.clone();
    let Ok(connecting) = incoming.accept() else {
        return;
    };
    let conn = match ctx.exec.within(options.header_timeout, connecting).await {
        Some(Ok(conn)) => conn,
        _ => return,
    };
    let shared = Arc::new(Shared::default());
    let goaway = start_control(&ctx.exec, &conn, options.limits, true, shared.clone());
    // The next request stream id not yet taken, for GOAWAY.
    let next_id = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let mut closing = false;
    loop {
        tokio::select! {
            accepted = conn.accept_bi(), if !closing => {
                let Ok((send, recv)) = accepted else { break };
                next_id.fetch_max(send.id().index() * 4 + 4, Ordering::AcqRel);
                // Held from the stream's first octet until its answer was
                // acknowledged.
                let guard = InFlight::new(&in_flight, &done, &ctx.exec);
                let ctx = ctx.clone();
                let conn = conn.clone();
                ctx.exec.clone().spawn(request(conn, send, recv, ctx, guard));
            }
            () = ctx.closing(), if !closing => {
                closing = true;
                let _ = goaway.send(next_id.load(Ordering::Acquire));
                if in_flight.load(Ordering::Acquire) == 0 {
                    break;
                }
            }
            () = done.notified(), if closing => {
                if in_flight.load(Ordering::Acquire) == 0 {
                    break;
                }
            }
            () = ctx.closed() => break,
            _ = conn.closed() => return,
        }
    }
    close(&conn, code::H3_NO_ERROR, "");
}

/// One request stream: its head, then an exchange for the application.
async fn request(
    conn: quinn::Connection,
    mut send: quinn::SendStream,
    recv: quinn::RecvStream,
    ctx: ConnCtx,
    guard: InFlight,
) {
    let limits = ctx.options.limits;
    let mut frames = Frames::new(recv);
    let reject = |send: &mut quinn::SendStream, frames: &mut Frames, code: u64| {
        let _ = send.reset(varint(code));
        let _ = frames.recv.stop(varint(code));
    };
    let first = async {
        loop {
            match frames.header().await? {
                None => return Ok(None),
                Some(h) if h.kind == frame::HEADERS => {
                    return frames.payload(h.len, limits.max_head_bytes).await.map(Some);
                }
                Some(h) if h.kind == frame::DATA || unexpected_on_request(h.kind) => {
                    return Err(Error::Protocol("a frame before the request head".into()));
                }
                Some(h) => frames.skip(h.len).await?,
            }
        }
    };
    let block = match ctx.exec.within(ctx.options.header_timeout, first).await {
        Some(Ok(Some(block))) => block,
        Some(Ok(None)) | None => {
            reject(&mut send, &mut frames, code::H3_REQUEST_INCOMPLETE);
            return;
        }
        Some(Err(Error::TooLarge { .. })) => {
            reject(&mut send, &mut frames, code::H3_EXCESSIVE_LOAD);
            return;
        }
        Some(Err(Error::Protocol(_))) => {
            close(
                &conn,
                code::H3_FRAME_UNEXPECTED,
                "a frame before the request head",
            );
            return;
        }
        Some(Err(_)) => return,
    };
    let fields = match decode_fields(&block, limits) {
        Ok(fields) => fields,
        Err(e) => {
            let _ = field_error(&conn, e);
            reject(&mut send, &mut frames, code::H3_MESSAGE_ERROR);
            return;
        }
    };
    let Some((head, length)) = request_head(&fields) else {
        reject(&mut send, &mut frames, code::H3_MESSAGE_ERROR);
        return;
    };
    let head_request = head.method == Method::HEAD;
    let body = BodyReader::h3(H3Reader {
        frames,
        conn: conn.clone(),
        limits,
        remaining: 0,
        content_length: length,
        received: 0,
        trailers: None,
        done: false,
        stop_code: code::H3_NO_ERROR,
    });
    let exchange = Exchange {
        head,
        version: Version::Http3,
        peer: conn.remote_address(),
        body,
        responder: Responder::h3(H3Responder {
            send: Some(send),
            head_request,
            guard: Some(guard),
        }),
    };
    let _ = ctx.exchanges.send(exchange).await;
}

/// The writer of a response whose head [`H3Responder::send_head`] sent.
pub(crate) fn writer(writer: Option<H3Writer>, framing: OutBody) -> BodyWriter {
    match writer {
        Some(w) => BodyWriter::h3(w, framing),
        None => BodyWriter::done(),
    }
}
