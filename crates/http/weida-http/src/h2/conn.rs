//! The HTTP/2 connection driver: one task per connection, both roles.
//!
//! The task owns both halves of the stream and loops over three things: a
//! read of the peer's octets, a write of what is queued, and the commands of
//! readers, writers, responders and the client. Every frame is handled to the
//! end before the next is read, so the stream table, both HPACK contexts and
//! every flow-control window have one owner and no lock.
//!
//! What it holds is bounded: the queued output by [`OUT_HIGH`] (past it, the
//! peer is not read until the peer reads), a stream's received data by the
//! window this side advertised, the streams by `SETTINGS_MAX_CONCURRENT_STREAMS`
//! (ours as a server; the peer's as a client, beyond which opens wait), and a
//! header block by `max_head_bytes`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use weida_http_codec::fields::is_connection_specific;
use weida_http_codec::h2::{self, Frame, code, setting};
use weida_http_codec::uri::Scheme;
use weida_http_codec::{DecodeError, Limits, hpack};
use weida_runtime::Exec;

use super::{
    Cmd, H2Handle, H2Responder, Opened, RecvShared, StreamError, StreamReader, StreamWriter,
};
use crate::body::BodyReader;
use crate::client::Response;
use crate::error::{Error, Result};
use crate::server::{ConnCtx, Exchange, Responder};
use crate::transport::Stream;
use crate::types::{Fields, Method, RequestHead, ResponseHead, Status, Version};

/// Queued output above which the peer is not read.
const OUT_HIGH: usize = 256 * 1024;

/// How long a closing connection may take to flush its last frames.
const FLUSH_ON_EXIT: Duration = Duration::from_secs(1);

/// The windows and bounds one side advertises.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Local {
    pub(crate) stream_window: u32,
    pub(crate) connection_window: u32,
    pub(crate) max_concurrent_streams: Option<u32>,
    pub(crate) limits: Limits,
}

/// One stream's state.
struct StreamState {
    // Send side.
    send_window: i64,
    pending: VecDeque<Pending>,
    local_closed: bool,
    // Receive side.
    recv: Arc<RecvShared>,
    recv_window: i64,
    unacked: u32,
    remote_closed: bool,
    discard: bool,
    expect_len: Option<u64>,
    received: u64,
    // Client: where the response head goes, and whether it went.
    response: Option<oneshot::Sender<Result<Response>>>,
    head_request: bool,
}

enum Pending {
    Data(Bytes, bool, Option<oneshot::Sender<Result<()>>>),
    Trailers(Fields, oneshot::Sender<Result<()>>),
}

/// A connection error: GOAWAY with the code, then close.
struct ConnError {
    code: u32,
    why: String,
}

fn conn_err(code: u32, why: impl Into<String>) -> ConnError {
    ConnError {
        code,
        why: why.into(),
    }
}

/// An open that waits for the peer's stream limit.
type WaitingOpen = (
    RequestHead,
    Option<u64>,
    bool,
    oneshot::Sender<Result<Opened>>,
);

struct Driver {
    exec: Exec,
    server: Option<ConnCtx>,
    local: Local,
    reader: ReadHalf<Stream>,
    rbuf: BytesMut,
    writer: WriteHalf<Stream>,
    out: Vec<u8>,
    cmd_tx: mpsc::WeakUnboundedSender<Cmd>,
    /// A server driver keeps its own sender, so the channel stays open for
    /// the streams it creates; a client's lives in its handles.
    keep_tx: Option<mpsc::UnboundedSender<Cmd>>,
    cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    handles_gone: bool,
    alive: Arc<AtomicBool>,
    encoder: hpack::Encoder,
    decoder: hpack::Decoder,
    streams: HashMap<u32, StreamState>,
    next_local_id: u32,
    last_remote_id: u32,
    peer_max_frame: u32,
    peer_initial_window: i64,
    peer_max_streams: u32,
    conn_send_window: i64,
    conn_recv_window: i64,
    conn_unacked: u32,
    settings_acked: bool,
    headers_in_progress: Option<(u32, bool, Vec<u8>)>,
    goaway_received: bool,
    goaway_sent: bool,
    waiting_opens: VecDeque<WaitingOpen>,
    pending_exchanges: VecDeque<Exchange>,
}

/// Starts the client side of an HTTP/2 connection on `stream` (TLS with ALPN
/// `h2` already done) and returns its handle.
pub(crate) fn spawn_client(exec: &Exec, stream: Stream, local: Local) -> H2Handle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let alive = Arc::new(AtomicBool::new(true));
    let handle = H2Handle {
        cmd: cmd_tx.clone(),
        alive: alive.clone(),
    };
    let mut driver = Driver::new(exec.clone(), None, local, stream, &cmd_tx, cmd_rx, alive);
    driver.next_local_id = 1;
    driver.out.extend_from_slice(h2::PREFACE);
    driver.send_preamble();
    drop(cmd_tx);
    exec.spawn(async move { driver.run().await });
    handle
}

/// Serves the server side of an HTTP/2 connection on `stream` until it
/// ends.
pub(crate) async fn serve(stream: Stream, ctx: ConnCtx) {
    let options = ctx.options.clone();
    let local = Local {
        stream_window: options.stream_window,
        connection_window: options.connection_window,
        max_concurrent_streams: Some(options.max_concurrent_streams),
        limits: options.limits,
    };
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let alive = Arc::new(AtomicBool::new(true));
    let exec = ctx.exec.clone();
    let mut driver = Driver::new(
        exec.clone(),
        Some(ctx),
        local,
        stream,
        &cmd_tx,
        cmd_rx,
        alive,
    );
    // A server driver holds a sender of its own: it ends by GOAWAY, idleness
    // or the peer, never because its streams' senders are gone.
    driver.keep_tx = Some(cmd_tx);
    // The client speaks first: the 24-octet preface, within header_timeout.
    let preface = async {
        while driver.rbuf.len() < h2::PREFACE.len() {
            match driver.reader.read_buf(&mut driver.rbuf).await {
                Ok(n) if n > 0 => {}
                _ => return false,
            }
        }
        driver.rbuf.starts_with(h2::PREFACE)
    };
    match exec.within(options.header_timeout, preface).await {
        Some(true) => {}
        _ => return,
    }
    driver.rbuf.advance(h2::PREFACE.len());
    driver.send_preamble();
    driver.run().await;
}

fn status_field(status: Status) -> String {
    status.as_u16().to_string()
}

impl Driver {
    fn new(
        exec: Exec,
        server: Option<ConnCtx>,
        local: Local,
        stream: Stream,
        cmd_tx: &mpsc::UnboundedSender<Cmd>,
        cmd_rx: mpsc::UnboundedReceiver<Cmd>,
        alive: Arc<AtomicBool>,
    ) -> Driver {
        let (reader, writer) = tokio::io::split(stream);
        Driver {
            exec,
            server,
            local,
            reader,
            rbuf: BytesMut::with_capacity(16 * 1024),
            writer,
            out: Vec::with_capacity(16 * 1024),
            cmd_tx: cmd_tx.downgrade(),
            keep_tx: None,
            cmd_rx,
            handles_gone: false,
            alive,
            encoder: hpack::Encoder::new(),
            decoder: hpack::Decoder::new(hpack::DEFAULT_TABLE_SIZE),
            streams: HashMap::new(),
            next_local_id: 2,
            last_remote_id: 0,
            peer_max_frame: h2::MIN_MAX_FRAME_SIZE,
            peer_initial_window: i64::from(h2::DEFAULT_WINDOW),
            peer_max_streams: u32::MAX,
            conn_send_window: i64::from(h2::DEFAULT_WINDOW),
            conn_recv_window: i64::from(h2::DEFAULT_WINDOW),
            conn_unacked: 0,
            settings_acked: false,
            headers_in_progress: None,
            goaway_received: false,
            goaway_sent: false,
            waiting_opens: VecDeque::new(),
            pending_exchanges: VecDeque::new(),
        }
    }

    fn is_server(&self) -> bool {
        self.server.is_some()
    }

    fn frame(&mut self, frame: &Frame<'_>) {
        frame.encode(&mut self.out);
    }

    /// Our SETTINGS, and the connection window raised to ours.
    fn send_preamble(&mut self) {
        let mut params = vec![
            (setting::INITIAL_WINDOW_SIZE, self.local.stream_window),
            (setting::MAX_FRAME_SIZE, self.local.limits.max_frame_size),
            (
                setting::MAX_HEADER_LIST_SIZE,
                u32::try_from(self.local.limits.max_head_bytes).unwrap_or(u32::MAX),
            ),
        ];
        if self.is_server() {
            if let Some(n) = self.local.max_concurrent_streams {
                params.push((setting::MAX_CONCURRENT_STREAMS, n));
            }
        } else {
            params.insert(0, (setting::ENABLE_PUSH, 0));
        }
        self.frame(&Frame::Settings { ack: false, params });
        let raise = i64::from(self.local.connection_window) - i64::from(h2::DEFAULT_WINDOW);
        if raise > 0 {
            self.frame(&Frame::WindowUpdate {
                stream: 0,
                increment: raise as u32,
            });
            self.conn_recv_window += raise;
        }
    }

    /// The receive window a new stream starts with: ours once the peer
    /// acknowledged it, until then at least the protocol's default.
    fn initial_recv_window(&self) -> i64 {
        let ours = i64::from(self.local.stream_window);
        if self.settings_acked {
            ours
        } else {
            ours.max(i64::from(h2::DEFAULT_WINDOW))
        }
    }

    fn new_stream(&self, head_request: bool) -> StreamState {
        StreamState {
            send_window: self.peer_initial_window,
            pending: VecDeque::new(),
            local_closed: false,
            recv: Arc::new(RecvShared::default()),
            recv_window: self.initial_recv_window(),
            unacked: 0,
            remote_closed: false,
            discard: false,
            expect_len: None,
            received: 0,
            response: None,
            head_request,
        }
    }

    fn cmd_sender(&self) -> Option<mpsc::UnboundedSender<Cmd>> {
        match &self.keep_tx {
            Some(tx) => Some(tx.clone()),
            None => self.cmd_tx.upgrade(),
        }
    }

    /// Writes a header block as HEADERS and CONTINUATION frames, split at
    /// the peer's frame size.
    fn send_headers(&mut self, id: u32, fields: &[(&[u8], &[u8], bool)], end_stream: bool) {
        let mut block = Vec::new();
        self.encoder.encode(fields, &mut block);
        let max = self.peer_max_frame as usize;
        let mut chunks = block.chunks(max.max(1)).peekable();
        let first = chunks.next().unwrap_or(&[]);
        self.frame(&Frame::Headers {
            stream: id,
            end_stream,
            end_headers: chunks.peek().is_none(),
            block: first,
        });
        while let Some(chunk) = chunks.next() {
            let last = chunks.peek().is_none();
            Frame::Continuation {
                stream: id,
                end_headers: last,
                block: chunk,
            }
            .encode(&mut self.out);
        }
    }

    fn reset(&mut self, id: u32, code: u32) {
        self.frame(&Frame::RstStream { stream: id, code });
        if let Some(stream) = self.streams.remove(&id) {
            self.fail_stream(stream, StreamError::Reset(code));
        }
    }

    fn fail_stream(&mut self, mut stream: StreamState, e: StreamError) {
        stream.recv.fail(e.clone());
        // Data nobody will read gives its credit back to the connection.
        self.conn_unacked = self
            .conn_unacked
            .saturating_add(stream.recv.discard() as u32);
        if let Some(response) = stream.response.take() {
            let _ = response.send(Err(e.to_error()));
        }
        for pending in stream.pending.drain(..) {
            match pending {
                Pending::Data(_, _, Some(done)) | Pending::Trailers(_, done) => {
                    let _ = done.send(Err(e.to_error()));
                }
                Pending::Data(_, _, None) => {}
            }
        }
    }

    fn maybe_remove(&mut self, id: u32) {
        let done = self.streams.get(&id).is_some_and(|s| {
            s.local_closed && s.remote_closed && s.response.is_none() && s.pending.is_empty()
        });
        if done {
            self.streams.remove(&id);
            self.start_waiting_opens();
        }
    }

    fn local_stream_count(&self) -> u32 {
        let local_odd = !self.is_server();
        self.streams
            .keys()
            .filter(|id| (*id % 2 == 1) == local_odd)
            .count() as u32
    }

    fn start_waiting_opens(&mut self) {
        while !self.waiting_opens.is_empty() && self.local_stream_count() < self.peer_max_streams {
            let (head, length, end, reply) = self.waiting_opens.pop_front().expect("checked");
            self.open(head, length, end, reply);
        }
    }

    fn open(
        &mut self,
        head: RequestHead,
        length: Option<u64>,
        end_stream: bool,
        reply: oneshot::Sender<Result<Opened>>,
    ) {
        if self.goaway_received || self.goaway_sent {
            let _ = reply.send(Err(Error::GoingAway));
            return;
        }
        if self.local_stream_count() >= self.peer_max_streams {
            self.waiting_opens
                .push_back((head, length, end_stream, reply));
            return;
        }
        let Some(cmd) = self.cmd_sender() else {
            let _ = reply.send(Err(Error::Closed));
            return;
        };
        if self.next_local_id > h2::MAX_WINDOW {
            let _ = reply.send(Err(Error::GoingAway));
            return;
        }
        let id = self.next_local_id;
        self.next_local_id += 2;
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
        self.send_headers(id, &fields, end_stream);
        let (response_tx, response_rx) = oneshot::channel();
        let mut stream = self.new_stream(head.method == Method::HEAD);
        stream.local_closed = end_stream;
        stream.response = Some(response_tx);
        self.streams.insert(id, stream);
        let _ = reply.send(Ok(Opened {
            writer: StreamWriter::new(cmd, id),
            response: response_rx,
        }));
    }

    fn on_cmd(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Open {
                head,
                length,
                end_stream,
                reply,
            } => self.open(head, length, end_stream, reply),
            Cmd::Data {
                id,
                data,
                end,
                done,
            } => match self.streams.get_mut(&id) {
                Some(s) if !s.local_closed => {
                    s.pending.push_back(Pending::Data(data, end, Some(done)))
                }
                _ => {
                    let _ = done.send(Err(Error::Closed));
                }
            },
            Cmd::Trailers { id, trailers, done } => match self.streams.get_mut(&id) {
                Some(s) if !s.local_closed => {
                    s.pending.push_back(Pending::Trailers(trailers, done))
                }
                _ => {
                    let _ = done.send(Err(Error::Closed));
                }
            },
            Cmd::Respond {
                id,
                mut head,
                end_stream,
                done,
            } => {
                if !self.streams.contains_key(&id) {
                    let _ = done.send(Err(Error::Closed));
                    return;
                }
                head.fields.remove("connection");
                let status = status_field(head.status);
                let mut fields: Vec<(&[u8], &[u8], bool)> =
                    vec![(b":status", status.as_bytes(), false)];
                fields.extend(
                    head.fields
                        .iter_full()
                        .map(|(n, v, s)| (n.as_bytes(), &v[..], s)),
                );
                self.send_headers(id, &fields, end_stream);
                if end_stream && let Some(s) = self.streams.get_mut(&id) {
                    s.local_closed = true;
                }
                let _ = done.send(Ok(()));
                self.after_local_close(id);
            }
            Cmd::Reset { id, code } => {
                if self.streams.contains_key(&id) {
                    self.reset(id, code);
                }
            }
            Cmd::StopReceiving { id } => {
                let server = self.is_server();
                let Some(s) = self.streams.get_mut(&id) else {
                    return;
                };
                s.discard = true;
                let dropped = s.recv.discard();
                let (remote_closed, local_closed) = (s.remote_closed, s.local_closed);
                self.conn_unacked = self.conn_unacked.saturating_add(dropped as u32);
                if !remote_closed {
                    if !server {
                        self.reset(id, code::CANCEL);
                    } else if local_closed {
                        self.reset(id, code::NO_ERROR);
                    }
                }
                self.credit_connection();
            }
            Cmd::Consumed { id, n } => {
                self.conn_unacked = self.conn_unacked.saturating_add(n as u32);
                let half = self.local.stream_window / 2;
                let mut update = None;
                if let Some(s) = self.streams.get_mut(&id)
                    && !s.remote_closed
                {
                    s.unacked = s.unacked.saturating_add(n as u32);
                    if s.unacked >= half.max(1) {
                        update = Some(s.unacked);
                        s.recv_window += i64::from(s.unacked);
                        s.unacked = 0;
                    }
                }
                if let Some(increment) = update {
                    self.frame(&Frame::WindowUpdate {
                        stream: id,
                        increment,
                    });
                }
                self.credit_connection();
            }
        }
    }

    /// Returns consumed connection credit once half the window is used.
    fn credit_connection(&mut self) {
        if self.conn_unacked >= (self.local.connection_window / 2).max(1) {
            let increment = self.conn_unacked;
            self.conn_unacked = 0;
            self.conn_recv_window += i64::from(increment);
            self.frame(&Frame::WindowUpdate {
                stream: 0,
                increment,
            });
        }
    }

    fn after_local_close(&mut self, id: u32) {
        self.maybe_remove(id);
    }

    /// Moves pending data into the output as both windows allow.
    fn flush_streams(&mut self) {
        let mut ids: Vec<u32> = self
            .streams
            .iter()
            .filter(|(_, s)| !s.pending.is_empty())
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        for id in ids {
            loop {
                if self.out.len() >= OUT_HIGH {
                    return;
                }
                let max_frame = self.peer_max_frame as usize;
                let conn_window = self.conn_send_window;
                let Some(s) = self.streams.get_mut(&id) else {
                    break;
                };
                let Some(front) = s.pending.front_mut() else {
                    break;
                };
                match front {
                    Pending::Data(data, end, done) => {
                        let end = *end;
                        if data.is_empty() {
                            if end {
                                Frame::Data {
                                    stream: id,
                                    end_stream: true,
                                    data: &[],
                                }
                                .encode(&mut self.out);
                                s.local_closed = true;
                            }
                            if let Some(done) = done.take() {
                                let _ = done.send(Ok(()));
                            }
                            s.pending.pop_front();
                            continue;
                        }
                        let n = (data.len() as i64)
                            .min(s.send_window)
                            .min(conn_window)
                            .min(max_frame as i64);
                        if n <= 0 {
                            break;
                        }
                        let piece = data.split_to(n as usize);
                        let last = data.is_empty();
                        s.send_window -= n;
                        self.conn_send_window -= n;
                        Frame::Data {
                            stream: id,
                            end_stream: end && last,
                            data: &piece,
                        }
                        .encode(&mut self.out);
                        if last {
                            if end {
                                s.local_closed = true;
                            }
                            if let Some(done) = done.take() {
                                let _ = done.send(Ok(()));
                            }
                            s.pending.pop_front();
                        }
                    }
                    Pending::Trailers(..) => {
                        let Some(Pending::Trailers(trailers, done)) = s.pending.pop_front() else {
                            unreachable!("matched above")
                        };
                        s.local_closed = true;
                        let fields: Vec<(&[u8], &[u8], bool)> = trailers
                            .iter_full()
                            .map(|(n, v, sens)| (n.as_bytes(), &v[..], sens))
                            .collect();
                        self.send_headers(id, &fields, true);
                        let _ = done.send(Ok(()));
                    }
                }
            }
            self.maybe_remove(id);
        }
    }

    /// Handles every whole frame in the read buffer.
    fn on_input(&mut self) -> std::result::Result<(), ConnError> {
        let mut buf = std::mem::take(&mut self.rbuf);
        let result = loop {
            match Frame::decode(&buf, self.local.limits.max_frame_size) {
                Ok((frame, used)) => {
                    let flow = (used - h2::HEADER_LEN) as i64;
                    if let Err(e) = self.on_frame(frame, flow) {
                        break Err(e);
                    }
                    buf.advance(used);
                }
                Err(DecodeError::Incomplete { .. }) => break Ok(()),
                Err(DecodeError::TooLarge { .. }) => {
                    break Err(conn_err(
                        code::FRAME_SIZE_ERROR,
                        "a frame above our SETTINGS_MAX_FRAME_SIZE",
                    ));
                }
                Err(DecodeError::Malformed { what }) => {
                    let code = match what {
                        "frame length" => code::FRAME_SIZE_ERROR,
                        "SETTINGS_INITIAL_WINDOW_SIZE" => code::FLOW_CONTROL_ERROR,
                        _ => code::PROTOCOL_ERROR,
                    };
                    break Err(conn_err(code, what));
                }
                Err(e) => break Err(conn_err(code::PROTOCOL_ERROR, e.to_string())),
            }
        };
        self.rbuf = buf;
        result
    }

    fn on_frame(&mut self, frame: Frame<'_>, flow: i64) -> std::result::Result<(), ConnError> {
        if let Some((pending_id, _, _)) = &self.headers_in_progress
            && !matches!(frame, Frame::Continuation { stream, .. } if stream == *pending_id)
        {
            return Err(conn_err(
                code::PROTOCOL_ERROR,
                "a frame inside a header block",
            ));
        }
        match frame {
            Frame::Data {
                stream,
                end_stream,
                data,
            } => self.on_data(stream, end_stream, data, flow),
            Frame::Headers {
                stream,
                end_stream,
                end_headers,
                block,
            } => {
                if block.len() > self.local.limits.max_head_bytes {
                    return Err(conn_err(
                        code::ENHANCE_YOUR_CALM,
                        "a header block above max_head_bytes",
                    ));
                }
                if end_headers {
                    self.on_header_block(stream, end_stream, block)
                } else {
                    self.headers_in_progress = Some((stream, end_stream, block.to_vec()));
                    Ok(())
                }
            }
            Frame::Continuation {
                stream,
                end_headers,
                block,
            } => {
                let Some((id, end_stream, mut buf)) = self.headers_in_progress.take() else {
                    return Err(conn_err(
                        code::PROTOCOL_ERROR,
                        "CONTINUATION without HEADERS",
                    ));
                };
                debug_assert_eq!(id, stream);
                if buf.len() + block.len() > self.local.limits.max_head_bytes {
                    return Err(conn_err(
                        code::ENHANCE_YOUR_CALM,
                        "a header block above max_head_bytes",
                    ));
                }
                buf.extend_from_slice(block);
                if end_headers {
                    self.on_header_block(id, end_stream, &buf)
                } else {
                    self.headers_in_progress = Some((id, end_stream, buf));
                    Ok(())
                }
            }
            Frame::Priority { .. } | Frame::Unknown { .. } => Ok(()),
            Frame::RstStream { stream, code } => {
                if let Some(s) = self.streams.remove(&stream) {
                    let e = if code == code::REFUSED_STREAM {
                        StreamError::GoingAway
                    } else {
                        StreamError::Reset(code)
                    };
                    self.fail_stream(s, e);
                    self.start_waiting_opens();
                } else if self.is_idle(stream) {
                    return Err(conn_err(
                        code::PROTOCOL_ERROR,
                        "RST_STREAM on an idle stream",
                    ));
                }
                Ok(())
            }
            Frame::Settings { ack: true, .. } => {
                self.settings_acked = true;
                Ok(())
            }
            Frame::Settings { ack: false, params } => {
                for (id, value) in params {
                    self.apply_setting(id, value)?;
                }
                self.frame(&Frame::Settings {
                    ack: true,
                    params: Vec::new(),
                });
                self.start_waiting_opens();
                Ok(())
            }
            Frame::PushPromise { .. } => Err(conn_err(
                code::PROTOCOL_ERROR,
                "PUSH_PROMISE with push disabled",
            )),
            Frame::Ping { ack, data } => {
                if !ack {
                    self.frame(&Frame::Ping { ack: true, data });
                }
                Ok(())
            }
            Frame::GoAway {
                last_stream,
                code,
                debug: debug_data,
            } => {
                if code != code::NO_ERROR {
                    let why = String::from_utf8_lossy(debug_data);
                    tracing::debug!(code, %why, "peer sent GOAWAY");
                }
                self.goaway_received = true;
                self.alive.store(false, Ordering::Release);
                let local_odd = !self.is_server();
                let refused: Vec<u32> = self
                    .streams
                    .keys()
                    .filter(|id| (*id % 2 == 1) == local_odd && **id > last_stream)
                    .copied()
                    .collect();
                for id in refused {
                    if let Some(s) = self.streams.remove(&id) {
                        self.fail_stream(s, StreamError::GoingAway);
                    }
                }
                for (_, _, _, reply) in self.waiting_opens.drain(..) {
                    let _ = reply.send(Err(Error::GoingAway));
                }
                Ok(())
            }
            Frame::WindowUpdate { stream, increment } => {
                if stream == 0 {
                    if increment == 0 {
                        return Err(conn_err(code::PROTOCOL_ERROR, "a zero WINDOW_UPDATE"));
                    }
                    self.conn_send_window += i64::from(increment);
                    if self.conn_send_window > i64::from(h2::MAX_WINDOW) {
                        return Err(conn_err(
                            code::FLOW_CONTROL_ERROR,
                            "the connection window overflowed",
                        ));
                    }
                    return Ok(());
                }
                let overflow = match self.streams.get_mut(&stream) {
                    Some(_) if increment == 0 => Some(code::PROTOCOL_ERROR),
                    Some(s) => {
                        s.send_window += i64::from(increment);
                        (s.send_window > i64::from(h2::MAX_WINDOW))
                            .then_some(code::FLOW_CONTROL_ERROR)
                    }
                    None => None,
                };
                if let Some(code) = overflow {
                    self.reset(stream, code);
                }
                Ok(())
            }
        }
    }

    /// A stream id the peer could not have used yet.
    fn is_idle(&self, id: u32) -> bool {
        let local = (id % 2 == 1) != self.is_server();
        if local {
            id >= self.next_local_id
        } else {
            id > self.last_remote_id
        }
    }

    fn apply_setting(&mut self, id: u16, value: u32) -> std::result::Result<(), ConnError> {
        match id {
            setting::HEADER_TABLE_SIZE => self.encoder.set_peer_max_table_size(value),
            setting::MAX_CONCURRENT_STREAMS => self.peer_max_streams = value,
            setting::INITIAL_WINDOW_SIZE => {
                let delta = i64::from(value) - self.peer_initial_window;
                self.peer_initial_window = i64::from(value);
                for s in self.streams.values_mut() {
                    s.send_window += delta;
                    if s.send_window > i64::from(h2::MAX_WINDOW) {
                        return Err(conn_err(
                            code::FLOW_CONTROL_ERROR,
                            "a stream window overflowed",
                        ));
                    }
                }
            }
            setting::MAX_FRAME_SIZE => self.peer_max_frame = value,
            _ => {}
        }
        Ok(())
    }

    fn on_data(
        &mut self,
        id: u32,
        end_stream: bool,
        data: &[u8],
        flow: i64,
    ) -> std::result::Result<(), ConnError> {
        if flow > self.conn_recv_window {
            return Err(conn_err(
                code::FLOW_CONTROL_ERROR,
                "DATA beyond the connection window",
            ));
        }
        self.conn_recv_window -= flow;
        let Some(s) = self.streams.get_mut(&id) else {
            if self.is_idle(id) {
                return Err(conn_err(code::PROTOCOL_ERROR, "DATA on an idle stream"));
            }
            // A stream that ended: its credit goes straight back.
            self.conn_unacked = self.conn_unacked.saturating_add(flow as u32);
            self.credit_connection();
            self.frame(&Frame::RstStream {
                stream: id,
                code: code::STREAM_CLOSED,
            });
            return Ok(());
        };
        if s.remote_closed || s.response.is_some() {
            // After END_STREAM, or before a response head.
            self.conn_unacked = self.conn_unacked.saturating_add(flow as u32);
            self.reset(id, code::STREAM_CLOSED);
            self.credit_connection();
            return Ok(());
        }
        if flow > s.recv_window {
            self.conn_unacked = self.conn_unacked.saturating_add(flow as u32);
            self.reset(id, code::FLOW_CONTROL_ERROR);
            self.credit_connection();
            return Ok(());
        }
        s.recv_window -= flow;
        s.received += data.len() as u64;
        // Padding is credited at once; data when the reader takes it.
        let padding = flow - data.len() as i64;
        let over = s.expect_len.is_some_and(|n| s.received > n);
        let short = end_stream && s.expect_len.is_some_and(|n| s.received != n);
        if over || short {
            self.conn_unacked = self.conn_unacked.saturating_add(flow as u32);
            self.reset(id, code::PROTOCOL_ERROR);
            self.credit_connection();
            return Ok(());
        }
        if s.discard {
            self.conn_unacked = self.conn_unacked.saturating_add(flow as u32);
        } else {
            self.conn_unacked = self.conn_unacked.saturating_add(padding as u32);
            if !data.is_empty() {
                s.recv.push(data);
            }
        }
        if end_stream {
            s.remote_closed = true;
            s.recv.end(None);
        }
        self.credit_connection();
        if end_stream {
            self.maybe_remove(id);
        }
        Ok(())
    }

    fn on_header_block(
        &mut self,
        id: u32,
        end_stream: bool,
        block: &[u8],
    ) -> std::result::Result<(), ConnError> {
        let mut fields = Vec::new();
        let decoded = self.decoder.decode(block, self.local.limits, &mut fields);
        let malformed_stream = match decoded {
            Ok(()) => false,
            Err(DecodeError::Malformed {
                what: "uppercase field name",
            }) => true,
            Err(DecodeError::TooLarge { .. }) => {
                return Err(conn_err(
                    code::ENHANCE_YOUR_CALM,
                    "a field section above max_head_bytes",
                ));
            }
            Err(e) => return Err(conn_err(code::COMPRESSION_ERROR, e.to_string())),
        };
        if self.is_server() {
            self.on_request_block(id, end_stream, fields, malformed_stream)
        } else {
            self.on_response_block(id, end_stream, fields, malformed_stream)
        }
    }

    fn on_request_block(
        &mut self,
        id: u32,
        end_stream: bool,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        malformed: bool,
    ) -> std::result::Result<(), ConnError> {
        if let Some(s) = self.streams.get_mut(&id) {
            // Trailers.
            if !end_stream || s.remote_closed {
                self.reset(id, code::PROTOCOL_ERROR);
                return Ok(());
            }
            if malformed || fields.iter().any(|(n, _)| n.starts_with(b":")) {
                self.reset(id, code::PROTOCOL_ERROR);
                return Ok(());
            }
            let short = s.expect_len.is_some_and(|n| s.received != n);
            if short {
                self.reset(id, code::PROTOCOL_ERROR);
                return Ok(());
            }
            s.remote_closed = true;
            s.recv.end(Some(to_fields(&fields)));
            self.maybe_remove(id);
            return Ok(());
        }
        if id.is_multiple_of(2) || id <= self.last_remote_id {
            return Err(conn_err(
                code::PROTOCOL_ERROR,
                "a stream id that is not new",
            ));
        }
        self.last_remote_id = id;
        if self.goaway_sent {
            self.frame(&Frame::RstStream {
                stream: id,
                code: code::REFUSED_STREAM,
            });
            return Ok(());
        }
        let limit = self.local.max_concurrent_streams.unwrap_or(u32::MAX);
        // A stream the peer reset leaves `streams` while its exchange still
        // waits for the accept queue, so both are counted against the limit.
        if self.streams.len() as u32 >= limit || self.pending_exchanges.len() as u32 >= limit {
            self.frame(&Frame::RstStream {
                stream: id,
                code: code::REFUSED_STREAM,
            });
            return Ok(());
        }
        let ctx = self
            .server
            .clone()
            .expect("a server driver has its context");
        let head = if malformed {
            None
        } else {
            request_head(&fields)
        };
        let Some((head, expect_len)) = head else {
            self.frame(&Frame::RstStream {
                stream: id,
                code: code::PROTOCOL_ERROR,
            });
            return Ok(());
        };
        let Some(cmd) = self.cmd_sender() else {
            return Ok(());
        };
        let mut stream = self.new_stream(false);
        stream.expect_len = expect_len;
        let body = if end_stream {
            stream.remote_closed = true;
            if expect_len.is_some_and(|n| n != 0) {
                self.frame(&Frame::RstStream {
                    stream: id,
                    code: code::PROTOCOL_ERROR,
                });
                return Ok(());
            }
            BodyReader::empty()
        } else {
            BodyReader::h2(StreamReader::new(
                stream.recv.clone(),
                cmd.clone(),
                id,
                expect_len,
            ))
        };
        let head_request = head.method == Method::HEAD;
        self.streams.insert(id, stream);
        let exchange = Exchange {
            head,
            version: Version::Http2,
            peer: ctx.peer,
            body,
            responder: Responder::h2(H2Responder {
                cmd,
                id,
                head_request,
                responded: false,
            }),
        };
        self.pending_exchanges.push_back(exchange);
        Ok(())
    }

    fn on_response_block(
        &mut self,
        id: u32,
        end_stream: bool,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        malformed: bool,
    ) -> std::result::Result<(), ConnError> {
        let cmd = self.cmd_sender();
        let Some(s) = self.streams.get_mut(&id) else {
            if self.is_idle(id) {
                return Err(conn_err(code::PROTOCOL_ERROR, "HEADERS on an idle stream"));
            }
            return Ok(());
        };
        if malformed {
            self.reset(id, code::PROTOCOL_ERROR);
            return Ok(());
        }
        if s.response.is_none() {
            // Trailers.
            if !end_stream || fields.iter().any(|(n, _)| n.starts_with(b":")) {
                self.reset(id, code::PROTOCOL_ERROR);
                return Ok(());
            }
            let short = s.expect_len.is_some_and(|n| s.received != n);
            if short {
                self.reset(id, code::PROTOCOL_ERROR);
                return Ok(());
            }
            s.remote_closed = true;
            s.recv.end(Some(to_fields(&fields)));
            self.maybe_remove(id);
            return Ok(());
        }
        let Some((status, head_fields, expect_len)) = response_head(&fields) else {
            self.reset(id, code::PROTOCOL_ERROR);
            return Ok(());
        };
        if status.is_informational() {
            if end_stream {
                self.reset(id, code::PROTOCOL_ERROR);
            }
            return Ok(());
        }
        let Some(cmd) = cmd else {
            return Ok(());
        };
        let no_body = s.head_request || status.as_u16() == 204 || status.as_u16() == 304;
        s.expect_len = if no_body { None } else { expect_len };
        let body = if end_stream {
            s.remote_closed = true;
            BodyReader::empty()
        } else {
            BodyReader::h2(StreamReader::new(s.recv.clone(), cmd, id, s.expect_len))
        };
        let response = Response {
            head: ResponseHead {
                status,
                fields: head_fields,
            },
            version: Version::Http2,
            body,
        };
        if let Some(tx) = s.response.take() {
            let _ = tx.send(Ok(response));
        }
        self.maybe_remove(id);
        Ok(())
    }

    /// Whether the connection has nothing left to do.
    fn finished(&self) -> bool {
        let quiet = self.streams.is_empty() && self.pending_exchanges.is_empty();
        let ending =
            self.goaway_sent || self.goaway_received || (self.handles_gone && !self.is_server());
        quiet && ending && self.waiting_opens.is_empty()
    }

    fn send_goaway(&mut self, code: u32, why: &str) {
        if self.goaway_sent {
            return;
        }
        self.goaway_sent = true;
        self.alive.store(false, Ordering::Release);
        self.frame(&Frame::GoAway {
            last_stream: self.last_remote_id,
            code,
            debug: why.as_bytes(),
        });
    }

    async fn run(mut self) {
        let mut closing = false;
        let mut idle_since = self.exec.now();
        // Frames that arrived with the server's preface read are already in
        // the buffer.
        if let Err(e) = self.on_input() {
            self.send_goaway(e.code, &e.why);
            self.shutdown().await;
            return;
        }
        loop {
            self.flush_streams();
            if self.streams.is_empty() {
                // nothing
            } else {
                idle_since = self.exec.now();
            }
            if self.finished() {
                break;
            }
            if self.handles_gone && !self.is_server() && self.streams.is_empty() {
                self.send_goaway(code::NO_ERROR, "");
                continue;
            }
            let can_read = self.out.len() < OUT_HIGH;
            let idle_limit = self.server.as_ref().map(|ctx| ctx.options.idle_timeout);
            let idle_left = idle_limit.map(|limit| {
                limit.saturating_sub(self.exec.now().saturating_duration_since(idle_since))
            });
            let server = self.server.clone();
            tokio::select! {
                read = self.reader.read_buf(&mut self.rbuf), if can_read => {
                    match read {
                        Ok(n) if n > 0 => {
                            if let Err(e) = self.on_input() {
                                tracing::debug!(code = e.code, why = %e.why, "HTTP/2 connection error");
                                self.send_goaway(e.code, &e.why);
                                break;
                            }
                        }
                        _ => break,
                    }
                }
                wrote = self.writer.write(&self.out), if !self.out.is_empty() => {
                    match wrote {
                        Ok(n) if n > 0 => {
                            self.out.drain(..n);
                        }
                        _ => break,
                    }
                    if self.out.is_empty() {
                        let _ = self.writer.flush().await;
                    }
                }
                cmd = self.cmd_rx.recv(), if !self.handles_gone => {
                    match cmd {
                        Some(cmd) => self.on_cmd(cmd),
                        None => self.handles_gone = true,
                    }
                }
                permit = async { server.as_ref().expect("guarded").exchanges.reserve().await },
                    if server.is_some() && !self.pending_exchanges.is_empty() =>
                {
                    match permit {
                        Ok(permit) => permit.send(self.pending_exchanges.pop_front().expect("guarded")),
                        Err(_) => break,
                    }
                }
                () = async { server.as_ref().expect("guarded").closing().await }, if server.is_some() && !closing => {
                    closing = true;
                    self.send_goaway(code::NO_ERROR, "");
                }
                () = async { server.as_ref().expect("guarded").closed().await }, if server.is_some() && closing => break,
                () = self.exec.sleep(idle_left.unwrap_or(Duration::MAX)), if idle_left.is_some() && self.streams.is_empty() && !self.goaway_sent => {
                    self.send_goaway(code::NO_ERROR, "idle");
                }
            }
        }
        self.shutdown().await;
    }

    async fn shutdown(&mut self) {
        self.alive.store(false, Ordering::Release);
        let streams: Vec<StreamState> = self.streams.drain().map(|(_, s)| s).collect();
        for s in streams {
            self.fail_stream(s, StreamError::Closed);
        }
        for (_, _, _, reply) in self.waiting_opens.drain(..) {
            let _ = reply.send(Err(Error::GoingAway));
        }
        let out = std::mem::take(&mut self.out);
        let writer = &mut self.writer;
        let _ = self
            .exec
            .within(FLUSH_ON_EXIT, async {
                writer.write_all(&out).await?;
                writer.flush().await?;
                writer.shutdown().await
            })
            .await;
    }
}

fn to_fields(fields: &[(Vec<u8>, Vec<u8>)]) -> Fields {
    let mut out = Fields::new();
    for (n, v) in fields {
        out.push_decoded(n, Bytes::copy_from_slice(v));
    }
    out
}

/// The request head in `fields` (RFC 9113 §8.3.1), and its declared length;
/// `None` for a malformed request.
pub(crate) fn request_head(fields: &[(Vec<u8>, Vec<u8>)]) -> Option<(RequestHead, Option<u64>)> {
    let mut method = None;
    let mut scheme = None;
    let mut path = None;
    let mut authority = None;
    let mut host = None;
    let mut regular = Fields::new();
    let mut length = None;
    let mut seen_regular = false;
    for (name, value) in fields {
        if let Some(pseudo) = name.strip_prefix(b":") {
            if seen_regular {
                return None;
            }
            let slot = match pseudo {
                b"method" => &mut method,
                b"scheme" => &mut scheme,
                b"path" => &mut path,
                b"authority" => &mut authority,
                _ => return None,
            };
            if slot.replace(value.clone()).is_some() {
                return None;
            }
            continue;
        }
        seen_regular = true;
        if is_connection_specific(name) {
            return None;
        }
        if name == b"te" && value != b"trailers" {
            return None;
        }
        if name == b"host" {
            host = Some(value.clone());
            continue;
        }
        if name == b"content-length" {
            let n = std::str::from_utf8(value).ok()?.parse::<u64>().ok()?;
            if length.replace(n).is_some_and(|m| m != n) {
                return None;
            }
        }
        regular.push_decoded(name, Bytes::copy_from_slice(value));
    }
    let method = Method::from_bytes(&method?).ok()?;
    if method == Method::CONNECT {
        return None;
    }
    let scheme = match scheme?.as_slice() {
        b"http" => Scheme::Http,
        b"https" => Scheme::Https,
        _ => return None,
    };
    let path = String::from_utf8(path?).ok()?;
    if path.is_empty() {
        return None;
    }
    let authority = String::from_utf8(authority.or(host)?).ok()?;
    Some((
        RequestHead {
            method,
            scheme,
            authority,
            path,
            fields: regular,
        },
        length,
    ))
}

/// The status, fields and declared length of a response head; `None` for a
/// malformed one.
pub(crate) fn response_head(
    fields: &[(Vec<u8>, Vec<u8>)],
) -> Option<(Status, Fields, Option<u64>)> {
    let mut status = None;
    let mut regular = Fields::new();
    let mut length = None;
    let mut seen_regular = false;
    for (name, value) in fields {
        if name.starts_with(b":") {
            if seen_regular || name != b":status" || status.is_some() {
                return None;
            }
            let digits = std::str::from_utf8(value).ok()?;
            if digits.len() != 3 {
                return None;
            }
            status = Some(Status::new(digits.parse().ok()?).ok()?);
            continue;
        }
        seen_regular = true;
        if is_connection_specific(name) {
            return None;
        }
        if name == b"content-length" {
            let n = std::str::from_utf8(value).ok()?.parse::<u64>().ok()?;
            if length.replace(n).is_some_and(|m| m != n) {
                return None;
            }
        }
        regular.push_decoded(name, Bytes::copy_from_slice(value));
    }
    Some((status?, regular, length))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;

    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::watch;

    use super::*;
    use crate::client::open_h2;
    use crate::h1::client::OutFraming;
    use crate::options::ServerOptions;
    use crate::server::State;

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

    fn local(stream_window: u32) -> Local {
        Local {
            stream_window,
            connection_window: 4 << 20,
            max_concurrent_streams: None,
            limits: Limits::DEFAULT,
        }
    }

    fn server_ctx(
        options: ServerOptions,
    ) -> (ConnCtx, mpsc::Receiver<Exchange>, watch::Sender<State>) {
        let (tx, rx) = mpsc::channel(256);
        let (state, state_rx) = watch::channel(State::Open);
        let ctx = ConnCtx {
            exec: Exec::current().unwrap(),
            options: Arc::new(options),
            exchanges: tx,
            state: state_rx,
            peer: SocketAddr::from(([127, 0, 0, 1], 9)),
            scheme: Scheme::Https,
        };
        (ctx, rx, state)
    }

    /// Our client driver against our server driver over a pipe.
    fn pair(
        client: Local,
        options: ServerOptions,
    ) -> (H2Handle, mpsc::Receiver<Exchange>, watch::Sender<State>) {
        let (a, b) = tokio::io::duplex(1 << 20);
        let handle = spawn_client(&Exec::current().unwrap(), Stream::Test(a), client);
        let (ctx, rx, state) = server_ctx(options);
        tokio::spawn(serve(Stream::Test(b), ctx));
        (handle, rx, state)
    }

    fn get(path: &str) -> RequestHead {
        RequestHead {
            method: Method::GET,
            scheme: Scheme::Https,
            authority: "example.com".into(),
            path: path.into(),
            fields: Fields::new(),
        }
    }

    #[tokio::test]
    async fn two_hundred_requests_through_ten_streams() {
        let options = ServerOptions {
            max_concurrent_streams: 10,
            ..ServerOptions::default()
        };
        let (handle, mut exchanges, _state) = pair(local(1 << 20), options);
        let open = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let (o, m) = (open.clone(), most.clone());
        tokio::spawn(async move {
            while let Some(ex) = exchanges.recv().await {
                let (o, m) = (o.clone(), m.clone());
                tokio::spawn(async move {
                    let now = o.fetch_add(1, Ordering::SeqCst) + 1;
                    m.fetch_max(now, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    let path = Bytes::from(ex.head.path.clone());
                    o.fetch_sub(1, Ordering::SeqCst);
                    ex.responder
                        .respond_bytes(ResponseHead::new(Status::OK), path)
                        .await
                        .unwrap();
                });
            }
        });
        let mut tasks = Vec::new();
        for i in 0..200 {
            let handle = handle.clone();
            tasks.push(tokio::spawn(async move {
                let path = format!("/{i}");
                let open = open_h2(&handle, &get(&path), OutFraming::None)
                    .await
                    .unwrap();
                let mut r = open.finish().await.unwrap();
                assert_eq!(r.version, Version::Http2);
                assert_eq!(r.body.collect(64).await.unwrap(), path);
            }));
        }
        for task in tasks {
            within(task).await.unwrap();
        }
        assert!(most.load(Ordering::SeqCst) <= 10, "{most:?}");
    }

    async fn stream_answer(mut exchanges: mpsc::Receiver<Exchange>, written: Arc<AtomicUsize>) {
        let ex = exchanges.recv().await.unwrap();
        let mut w = ex
            .responder
            .respond(ResponseHead::new(Status::OK))
            .await
            .unwrap();
        let piece = Bytes::from(vec![7u8; 16 * 1024]);
        for _ in 0..256 {
            w.write(piece.clone()).await.unwrap();
            written.fetch_add(piece.len(), Ordering::SeqCst);
        }
        w.finish().await.unwrap();
    }

    #[tokio::test]
    async fn four_mebibytes_through_a_64_kib_window() {
        let (handle, exchanges, _state) = pair(local(64 * 1024), ServerOptions::default());
        let written = Arc::new(AtomicUsize::new(0));
        tokio::spawn(stream_answer(exchanges, written));
        let open = open_h2(&handle, &get("/"), OutFraming::None).await.unwrap();
        let mut r = within(open.finish()).await.unwrap();
        let body = within(r.body.collect(8 << 20)).await.unwrap();
        assert_eq!(body.len(), 4 << 20);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reader_that_stops_stalls_the_sender_at_its_window() {
        let (handle, exchanges, _state) = pair(local(64 * 1024), ServerOptions::default());
        let written = Arc::new(AtomicUsize::new(0));
        let sender = tokio::spawn(stream_answer(exchanges, written.clone()));
        let open = open_h2(&handle, &get("/"), OutFraming::None).await.unwrap();
        let r = open.finish().await.unwrap();
        // Nobody reads the body: the server's writes stop at the window.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!sender.is_finished());
        assert_eq!(written.load(Ordering::SeqCst), 64 * 1024);
        drop(r);
    }

    #[tokio::test]
    async fn trailers_both_ways() {
        let (handle, mut exchanges, _state) = pair(local(1 << 20), ServerOptions::default());
        tokio::spawn(async move {
            let mut ex = exchanges.recv().await.unwrap();
            let body = ex.body.collect(1024).await.unwrap();
            assert_eq!(body, "up");
            let got = ex
                .body
                .trailers()
                .and_then(|t| t.get("x-in").cloned())
                .unwrap();
            let mut w = ex
                .responder
                .respond(ResponseHead::new(Status::OK))
                .await
                .unwrap();
            w.write(Bytes::from_static(b"down")).await.unwrap();
            let mut trailers = Fields::new();
            trailers.append("x-out", got).unwrap();
            w.finish_with_trailers(trailers).await.unwrap();
        });
        let mut head = get("/");
        head.method = Method::POST;
        let mut open = open_h2(&handle, &head, OutFraming::Chunked).await.unwrap();
        open.write(Bytes::from_static(b"up")).await.unwrap();
        let mut trailers = Fields::new();
        trailers.append("x-in", "7").unwrap();
        let mut r = within(open.finish_with_trailers(trailers)).await.unwrap();
        assert_eq!(within(r.body.collect(64)).await.unwrap(), "down");
        assert_eq!(
            r.body
                .trailers()
                .and_then(|t| t.get("x-out"))
                .map(|v| &v[..]),
            Some(&b"7"[..])
        );
    }

    /// Reads frames off a raw peer's end until `want` says stop.
    async fn frames(
        io: &mut DuplexStream,
        buf: &mut BytesMut,
        mut want: impl FnMut(&Frame<'_>) -> bool,
    ) {
        loop {
            while let Ok((frame, used)) = Frame::decode(buf, 1 << 20) {
                let stop = want(&frame);
                buf.advance(used);
                if stop {
                    return;
                }
            }
            let n = io.read_buf(buf).await.unwrap();
            assert!(n > 0, "the peer closed");
        }
    }

    #[tokio::test]
    async fn goaway_fails_the_streams_above_its_last_id() {
        let (a, mut peer) = tokio::io::duplex(1 << 20);
        let handle = spawn_client(&Exec::current().unwrap(), Stream::Test(a), local(1 << 20));
        let h = handle.clone();
        let first = tokio::spawn(async move {
            open_h2(&h, &get("/1"), OutFraming::None)
                .await?
                .finish()
                .await
        });
        let h = handle.clone();
        let second = tokio::spawn(async move {
            // After the first, so it is stream 3.
            tokio::task::yield_now().await;
            open_h2(&h, &get("/3"), OutFraming::None)
                .await?
                .finish()
                .await
        });

        let mut preface = [0u8; 24];
        within(peer.read_exact(&mut preface)).await.unwrap();
        let mut out = Vec::new();
        Frame::Settings {
            ack: false,
            params: vec![],
        }
        .encode(&mut out);
        peer.write_all(&out).await.unwrap();
        let mut buf = BytesMut::new();
        let mut streams = Vec::new();
        within(frames(&mut peer, &mut buf, |f| {
            if let Frame::Headers { stream, .. } = f {
                streams.push(*stream);
            }
            streams.len() == 2
        }))
        .await;
        assert_eq!(streams, [1, 3]);

        let mut out = Vec::new();
        Frame::GoAway {
            last_stream: 1,
            code: code::NO_ERROR,
            debug: b"",
        }
        .encode(&mut out);
        let mut block = Vec::new();
        hpack::Encoder::new().encode(&[(b":status", b"200", false)], &mut block);
        Frame::Headers {
            stream: 1,
            end_stream: true,
            end_headers: true,
            block: &block,
        }
        .encode(&mut out);
        peer.write_all(&out).await.unwrap();

        let r = within(first).await.unwrap().unwrap();
        assert_eq!(r.head.status, Status::OK);
        assert!(matches!(
            within(second).await.unwrap(),
            Err(Error::GoingAway)
        ));
        assert!(!handle.is_usable());
    }

    #[tokio::test]
    async fn an_uppercase_name_or_a_missing_path_resets_the_stream() {
        let (mut peer, b) = tokio::io::duplex(1 << 20);
        let (ctx, _exchanges, _state) = server_ctx(ServerOptions::default());
        tokio::spawn(serve(Stream::Test(b), ctx));
        let mut out = h2::PREFACE.to_vec();
        Frame::Settings {
            ack: false,
            params: vec![],
        }
        .encode(&mut out);
        let mut enc = hpack::Encoder::new();
        let base: [(&[u8], &[u8], bool); 3] = [
            (b":method", b"GET", false),
            (b":scheme", b"https", false),
            (b":authority", b"a", false),
        ];
        let mut upper = base.to_vec();
        upper.push((b":path", b"/", false));
        upper.push((b"X-Upper", b"1", false));
        let mut block = Vec::new();
        enc.encode(&upper, &mut block);
        Frame::Headers {
            stream: 1,
            end_stream: true,
            end_headers: true,
            block: &block,
        }
        .encode(&mut out);
        let mut block = Vec::new();
        enc.encode(&base, &mut block);
        Frame::Headers {
            stream: 3,
            end_stream: true,
            end_headers: true,
            block: &block,
        }
        .encode(&mut out);
        peer.write_all(&out).await.unwrap();

        let mut buf = BytesMut::new();
        let mut resets = Vec::new();
        within(frames(&mut peer, &mut buf, |f| {
            if let Frame::RstStream { stream, code } = f {
                resets.push((*stream, *code));
            }
            resets.len() == 2
        }))
        .await;
        assert_eq!(
            resets,
            [(1, code::PROTOCOL_ERROR), (3, code::PROTOCOL_ERROR)]
        );
    }

    #[tokio::test]
    async fn streams_reset_while_nobody_accepts_are_refused_past_the_limit() {
        const LIMIT: u32 = 4;
        const OPENED: u32 = 3 * LIMIT;
        let (mut peer, b) = tokio::io::duplex(1 << 20);
        let (mut ctx, _unused, _state) = server_ctx(ServerOptions {
            max_concurrent_streams: LIMIT,
            ..ServerOptions::default()
        });
        // An accept queue of one that nobody reads.
        let (tx, _exchanges) = mpsc::channel(1);
        ctx.exchanges = tx;
        tokio::spawn(serve(Stream::Test(b), ctx));

        let mut out = h2::PREFACE.to_vec();
        Frame::Settings {
            ack: false,
            params: vec![],
        }
        .encode(&mut out);
        let mut enc = hpack::Encoder::new();
        let request: [(&[u8], &[u8], bool); 4] = [
            (b":method", b"GET", false),
            (b":scheme", b"https", false),
            (b":authority", b"a", false),
            (b":path", b"/", false),
        ];
        for i in 0..OPENED {
            let stream = 2 * i + 1;
            let mut block = Vec::new();
            enc.encode(&request, &mut block);
            Frame::Headers {
                stream,
                end_stream: true,
                end_headers: true,
                block: &block,
            }
            .encode(&mut out);
            Frame::RstStream {
                stream,
                code: code::CANCEL,
            }
            .encode(&mut out);
        }
        Frame::Ping {
            ack: false,
            data: *b"after-it",
        }
        .encode(&mut out);
        peer.write_all(&out).await.unwrap();

        // Output is in order, so every refusal precedes the PING's answer.
        let mut buf = BytesMut::new();
        let mut refused = Vec::new();
        within(frames(&mut peer, &mut buf, |f| match f {
            Frame::RstStream { stream, code } => {
                assert_eq!(*code, code::REFUSED_STREAM);
                refused.push(*stream);
                false
            }
            Frame::Ping { ack, .. } => *ack,
            _ => false,
        }))
        .await;
        // At most the limit wait in the driver and one in the accept queue;
        // every later stream is refused.
        let taken = OPENED - refused.len() as u32;
        assert!(taken <= LIMIT + 1, "{taken} exchanges kept");
        let first_refused = 2 * taken + 1;
        assert_eq!(
            refused,
            (first_refused..2 * OPENED).step_by(2).collect::<Vec<_>>()
        );
    }
}
