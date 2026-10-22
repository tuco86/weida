//! The server: listeners, connections, and exchanges pulled by
//! [`Server::accept`].
//!
//! Pull-style, as weida's patterns are: no handler closures and no service
//! stack. Each listener runs an accept loop; each connection a task that
//! parses requests and queues one [`Exchange`] per request, at most
//! `accept_queue` of them, and a full queue makes the connections wait. The
//! application takes an exchange, reads its body if it wants to, and answers
//! through its [`Responder`].

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use weida_http_codec::fields::is_connection_specific;
use weida_http_codec::uri::Scheme;
use weida_runtime::{CloseBudget, Exec};

use crate::body::{BODY_QUEUE, BodyReader, BodyWriter, OutBody, WriteEvent};
use crate::error::{Error, Result};
use crate::h1;
use crate::h1::Conn;
use crate::options::ServerOptions;
use crate::transport::Stream;
use crate::types::{Fields, RequestHead, ResponseHead, Version};

/// Where a server is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum State {
    /// Accepting and serving.
    Open,
    /// No new connections or requests; exchanges in flight finish.
    Closing,
    /// Everything left is dropped.
    Closed,
}

/// What every connection task of one server shares.
#[derive(Clone)]
pub(crate) struct ConnCtx {
    pub(crate) exec: Exec,
    pub(crate) options: Arc<ServerOptions>,
    pub(crate) exchanges: mpsc::Sender<Exchange>,
    pub(crate) state: watch::Receiver<State>,
    pub(crate) peer: SocketAddr,
    pub(crate) scheme: Scheme,
}

impl ConnCtx {
    /// Resolves once the server is closed hard (or gone).
    pub(crate) async fn closed(&self) {
        let mut state = self.state.clone();
        let _ = state.wait_for(|s| *s == State::Closed).await;
    }

    /// Resolves once the server stops taking requests (or is gone).
    pub(crate) async fn closing(&self) {
        let mut state = self.state.clone();
        let _ = state.wait_for(|s| *s != State::Open).await;
    }

    pub(crate) fn is_open(&self) -> bool {
        *self.state.borrow() == State::Open
    }
}

/// One request and the means to answer it.
#[derive(Debug)]
pub struct Exchange {
    /// The request head; on HTTP/1.1 `authority` is the `Host` field (or the
    /// absolute-form target's).
    pub head: RequestHead,
    /// The version the request came over.
    pub version: Version,
    /// The client's address.
    pub peer: SocketAddr,
    /// The request body.
    pub body: BodyReader,
    /// Where the response goes.
    pub responder: Responder,
}

/// A response that the connection task writes.
#[derive(Debug)]
pub(crate) struct ResponseStart {
    pub(crate) head: ResponseHead,
    pub(crate) framing: OutBody,
    pub(crate) body: ResponseBody,
}

/// The body of a [`ResponseStart`].
#[derive(Debug)]
pub(crate) enum ResponseBody {
    /// Whole, with a report of whether it reached the wire.
    Full(Bytes, oneshot::Sender<Result<()>>),
    /// Streamed by a [`BodyWriter`].
    Stream(mpsc::Receiver<WriteEvent>),
}

/// The answer to one [`Exchange`].
///
/// Dropped without answering, it answers `500 Internal Server Error` and,
/// on HTTP/1.1, closes the connection.
#[derive(Debug)]
pub struct Responder {
    inner: ResponderInner,
}

#[derive(Debug)]
enum ResponderInner {
    H1 {
        tx: oneshot::Sender<ResponseStart>,
        head_request: bool,
        minor: u8,
    },
    #[cfg(feature = "tls")]
    H2(crate::h2::H2Responder),
    #[cfg(feature = "h3")]
    H3(crate::h3::H3Responder),
}

/// Adds `date` (RFC 9110 §6.6.1: an origin server with a clock MUST) when
/// the application set none. Wall time, not the runtime's clock: a date is
/// read by people and caches, not by timers.
pub(crate) fn add_date(fields: &mut Fields) {
    if fields.get("date").is_some() {
        return;
    }
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut date = Vec::with_capacity(29);
    weida_http_codec::date::imf_fixdate(secs, &mut date);
    fields
        .insert("date", date)
        .expect("an IMF-fixdate is a field value");
}

/// Refuses the fields a connection owns. `connection: close` is the one an
/// application may set on HTTP/1.1, to end the connection after the answer.
fn check_response_fields(fields: &Fields) -> Result<()> {
    for (name, _) in fields.iter() {
        if name != "connection" && is_connection_specific(name.as_bytes()) {
            return Err(Error::Configuration(format!(
                "{name} is the connection's field, not the response's"
            )));
        }
    }
    Ok(())
}

impl Responder {
    pub(crate) fn h1(
        tx: oneshot::Sender<ResponseStart>,
        head_request: bool,
        minor: u8,
    ) -> Responder {
        Responder {
            inner: ResponderInner::H1 {
                tx,
                head_request,
                minor,
            },
        }
    }

    #[cfg(feature = "tls")]
    pub(crate) fn h2(responder: crate::h2::H2Responder) -> Responder {
        Responder {
            inner: ResponderInner::H2(responder),
        }
    }

    #[cfg(feature = "h3")]
    pub(crate) fn h3(responder: crate::h3::H3Responder) -> Responder {
        Responder {
            inner: ResponderInner::H3(responder),
        }
    }

    /// Sends the response head; the body follows through the returned
    /// writer. With a `content-length` field the body must be exactly that
    /// long; without one it is chunked (or, to an HTTP/1.0 client, delimited
    /// by closing the connection). Responses to HEAD and 204 and 304
    /// responses carry no body.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for a connection-specific field or a
    /// `content-length` that is not a number; [`Error::Protocol`] for an
    /// interim (1xx) status; [`Error::Closed`] when the connection is gone.
    pub async fn respond(self, mut head: ResponseHead) -> Result<BodyWriter> {
        check_response_fields(&head.fields)?;
        if head.status.is_informational() {
            return Err(Error::Protocol(
                "an interim status is not a response".into(),
            ));
        }
        add_date(&mut head.fields);
        match self.inner {
            ResponderInner::H1 {
                tx,
                head_request,
                minor,
            } => {
                let framing = h1_framing(&head, head_request, minor)?;
                let (wtx, wrx) = mpsc::channel(BODY_QUEUE);
                tx.send(ResponseStart {
                    head,
                    framing,
                    body: ResponseBody::Stream(wrx),
                })
                .map_err(|_| Error::Closed)?;
                Ok(BodyWriter::channel(wtx, framing))
            }
            #[cfg(feature = "tls")]
            ResponderInner::H2(mut responder) => {
                // HTTP/2 frames every body, so "until close" never arises.
                let framing = h1_framing(&head, responder.head_request, 1)?;
                let end_stream = framing == OutBody::None;
                responder.send_head(head, end_stream).await?;
                Ok(if end_stream {
                    BodyWriter::done()
                } else {
                    BodyWriter::h2(responder.writer(), framing)
                })
            }
            #[cfg(feature = "h3")]
            ResponderInner::H3(mut responder) => {
                let framing = h1_framing(&head, responder.head_request(), 1)?;
                let end_stream = framing == OutBody::None;
                let writer = responder.send_head(head, end_stream).await?;
                Ok(crate::h3::writer(writer, framing))
            }
        }
    }

    /// Sends a whole response; `content-length` is set from `body`. Returns
    /// once it reached the connection.
    ///
    /// # Errors
    ///
    /// As [`Responder::respond`], and [`Error::Protocol`] for a body on a 204
    /// or 304.
    pub async fn respond_bytes(self, mut head: ResponseHead, body: Bytes) -> Result<()> {
        check_response_fields(&head.fields)?;
        if head.status.is_informational() {
            return Err(Error::Protocol(
                "an interim status is not a response".into(),
            ));
        }
        let status = head.status.as_u16();
        let no_body = status == 204 || status == 304;
        if no_body && !body.is_empty() {
            return Err(Error::Protocol(format!("a body on a {status} response")));
        }
        add_date(&mut head.fields);
        match self.inner {
            ResponderInner::H1 {
                tx, head_request, ..
            } => {
                if !no_body {
                    head.fields
                        .insert("content-length", body.len().to_string())
                        .expect("digits are a field value");
                }
                let framing = if head_request || no_body {
                    OutBody::None
                } else {
                    OutBody::Length(body.len() as u64)
                };
                let (done, wrote) = oneshot::channel();
                tx.send(ResponseStart {
                    head,
                    framing,
                    body: ResponseBody::Full(body, done),
                })
                .map_err(|_| Error::Closed)?;
                wrote.await.map_err(|_| Error::Closed)?
            }
            #[cfg(feature = "tls")]
            ResponderInner::H2(mut responder) => {
                if !no_body {
                    head.fields
                        .insert("content-length", body.len().to_string())
                        .expect("digits are a field value");
                }
                let end_stream = no_body || responder.head_request || body.is_empty();
                responder.send_head(head, end_stream).await?;
                if end_stream {
                    return Ok(());
                }
                let mut writer =
                    BodyWriter::h2(responder.writer(), OutBody::Length(body.len() as u64));
                writer.write(body).await?;
                writer.finish().await
            }
            #[cfg(feature = "h3")]
            ResponderInner::H3(mut responder) => {
                if !no_body {
                    head.fields
                        .insert("content-length", body.len().to_string())
                        .expect("digits are a field value");
                }
                let end_stream = no_body || responder.head_request() || body.is_empty();
                let writer = responder.send_head(head, end_stream).await?;
                let mut writer = crate::h3::writer(writer, OutBody::Length(body.len() as u64));
                if !end_stream {
                    writer.write(body).await?;
                }
                writer.finish().await
            }
        }
    }
}

/// How an HTTP/1.1 response body is delimited.
fn h1_framing(head: &ResponseHead, head_request: bool, minor: u8) -> Result<OutBody> {
    let status = head.status.as_u16();
    if head_request || status == 204 || status == 304 {
        return Ok(OutBody::None);
    }
    if let Some(v) = head.fields.get("content-length") {
        let n = std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Error::Configuration("content-length is not a number".into()))?;
        return Ok(OutBody::Length(n));
    }
    Ok(if minor >= 1 {
        OutBody::Open
    } else {
        OutBody::UntilClose
    })
}

/// How a listener's connections start.
#[derive(Clone)]
enum Acceptor {
    Plain,
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::TlsAcceptor),
}

impl Acceptor {
    fn scheme(&self) -> Scheme {
        match self {
            Acceptor::Plain => Scheme::Http,
            #[cfg(feature = "tls")]
            Acceptor::Tls(_) => Scheme::Https,
        }
    }
}

/// An HTTP server. Bind it to one or more addresses, then take exchanges
/// with [`Server::accept`].
pub struct Server(Arc<Inner>);

struct Inner {
    exec: Exec,
    options: Arc<ServerOptions>,
    exchanges_tx: mpsc::Sender<Exchange>,
    exchanges_rx: tokio::sync::Mutex<mpsc::Receiver<Exchange>>,
    permits: Arc<Semaphore>,
    state: watch::Sender<State>,
    listeners: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.state.send(State::Closed);
        for listener in self
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            listener.abort();
        }
    }
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("state", &*self.0.state.borrow())
            .finish_non_exhaustive()
    }
}

impl Server {
    /// A server on `exec`'s runtime, bound to nothing yet.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for options [`ServerOptions::validate`]
    /// refuses.
    pub fn new(exec: &Exec, options: ServerOptions) -> Result<Server> {
        options.validate()?;
        let (exchanges_tx, exchanges_rx) = mpsc::channel(options.accept_queue);
        let permits = Arc::new(Semaphore::new(
            options.max_connections.min(Semaphore::MAX_PERMITS),
        ));
        let (state, _) = watch::channel(State::Open);
        Ok(Server(Arc::new(Inner {
            exec: exec.clone(),
            options: Arc::new(options),
            exchanges_tx,
            exchanges_rx: tokio::sync::Mutex::new(exchanges_rx),
            permits,
            state,
            listeners: Mutex::new(Vec::new()),
        })))
    }

    fn listen(&self, addr: SocketAddr) -> Result<TcpListener> {
        let std = std::net::TcpListener::bind(addr)?;
        std.set_nonblocking(true)?;
        let _guard = self.0.exec.enter();
        Ok(TcpListener::from_std(std)?)
    }

    fn spawn_listener(&self, listener: TcpListener, acceptor: Acceptor) {
        let ctx = Listening {
            exec: self.0.exec.clone(),
            options: self.0.options.clone(),
            exchanges: self.0.exchanges_tx.clone(),
            permits: self.0.permits.clone(),
            state: self.0.state.subscribe(),
        };
        let handle = self.0.exec.spawn(accept_loop(ctx, listener, acceptor));
        self.0
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(handle);
    }

    /// Serves plain HTTP/1.1 on `addr`; returns the address bound (port 0
    /// picks one).
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the address cannot be bound; [`Error::Closed`]
    /// after [`Server::close`].
    pub async fn bind_tcp(&self, addr: SocketAddr) -> Result<SocketAddr> {
        if *self.0.state.borrow() != State::Open {
            return Err(Error::Closed);
        }
        let listener = self.listen(addr)?;
        let local = listener.local_addr()?;
        self.spawn_listener(listener, Acceptor::Plain);
        Ok(local)
    }

    /// Serves HTTPS on `addr` with the caller's certificate configuration,
    /// cloned with ALPN `h2` and `http/1.1`; returns the address bound.
    ///
    /// # Errors
    ///
    /// As [`Server::bind_tcp`].
    #[cfg(feature = "tls")]
    pub async fn bind_tls(
        &self,
        addr: SocketAddr,
        config: Arc<rustls::ServerConfig>,
    ) -> Result<SocketAddr> {
        if *self.0.state.borrow() != State::Open {
            return Err(Error::Closed);
        }
        let mut config = (*config).clone();
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let listener = self.listen(addr)?;
        let local = listener.local_addr()?;
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        self.spawn_listener(listener, Acceptor::Tls(acceptor));
        Ok(local)
    }

    /// Serves HTTP/3 on UDP `addr` with the caller's certificate
    /// configuration, cloned with ALPN `h3`; returns the address bound.
    ///
    /// # Errors
    ///
    /// [`Error::Tls`] when the configuration cannot drive QUIC (QUIC needs
    /// TLS 1.3); [`Error::Io`] when the address cannot be bound;
    /// [`Error::Closed`] after [`Server::close`].
    #[cfg(feature = "h3")]
    pub async fn bind_quic(
        &self,
        addr: SocketAddr,
        config: Arc<rustls::ServerConfig>,
    ) -> Result<SocketAddr> {
        if *self.0.state.borrow() != State::Open {
            return Err(Error::Closed);
        }
        let server_config = crate::h3::server_config(&config, &self.0.options)?;
        let endpoint = {
            let _guard = self.0.exec.enter();
            quinn::Endpoint::server(server_config, addr)?
        };
        let local = endpoint.local_addr()?;
        let ctx = Listening {
            exec: self.0.exec.clone(),
            options: self.0.options.clone(),
            exchanges: self.0.exchanges_tx.clone(),
            permits: self.0.permits.clone(),
            state: self.0.state.subscribe(),
        };
        let handle = self.0.exec.spawn(quic_accept_loop(ctx, endpoint));
        self.0
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(handle);
        Ok(local)
    }

    /// The next exchange.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once [`Server::close`] began.
    pub async fn accept(&self) -> Result<Exchange> {
        let mut state = self.0.state.subscribe();
        if *state.borrow() != State::Open {
            return Err(Error::Closed);
        }
        let mut rx = self.0.exchanges_rx.lock().await;
        tokio::select! {
            exchange = rx.recv() => exchange.ok_or(Error::Closed),
            _ = state.wait_for(|s| *s != State::Open) => Err(Error::Closed),
        }
    }

    /// Stops accepting, lets the exchanges in flight finish within
    /// `close_timeout`, then drops the rest.
    pub async fn close(&self) {
        let _ = self.0.state.send(State::Closing);
        for listener in self
            .0
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            listener.abort();
        }
        let budget = CloseBudget::start(self.0.options.close_timeout);
        let all = u32::try_from(self.0.options.max_connections.min(Semaphore::MAX_PERMITS))
            .unwrap_or(u32::MAX);
        let _ = self
            .0
            .exec
            .within(budget.remaining(), self.0.permits.acquire_many(all))
            .await;
        let _ = self.0.state.send(State::Closed);
    }
}

/// What an accept loop needs; deliberately not the server itself, so that
/// dropping the [`Server`] ends its loops.
struct Listening {
    exec: Exec,
    options: Arc<ServerOptions>,
    exchanges: mpsc::Sender<Exchange>,
    permits: Arc<Semaphore>,
    state: watch::Receiver<State>,
}

#[cfg(feature = "h3")]
async fn quic_accept_loop(ctx: Listening, endpoint: quinn::Endpoint) {
    let mut state = ctx.state.clone();
    loop {
        let permit = tokio::select! {
            permit = ctx.permits.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => return,
            },
            _ = state.wait_for(|s| *s != State::Open) => return,
        };
        let incoming = tokio::select! {
            incoming = endpoint.accept() => match incoming {
                Some(incoming) => incoming,
                None => return,
            },
            _ = state.wait_for(|s| *s != State::Open) => return,
        };
        let conn = ConnCtx {
            exec: ctx.exec.clone(),
            options: ctx.options.clone(),
            exchanges: ctx.exchanges.clone(),
            state: ctx.state.clone(),
            peer: incoming.remote_address(),
            scheme: Scheme::Https,
        };
        ctx.exec.spawn(async move {
            crate::h3::serve(incoming, conn).await;
            drop(permit);
        });
    }
}

async fn accept_loop(ctx: Listening, listener: TcpListener, acceptor: Acceptor) {
    let mut state = ctx.state.clone();
    loop {
        // A connection takes its place before it is accepted, so past
        // `max_connections` the next one waits in the listen backlog.
        let permit = tokio::select! {
            permit = ctx.permits.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => return,
            },
            _ = state.wait_for(|s| *s != State::Open) => return,
        };
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = state.wait_for(|s| *s != State::Open) => return,
        };
        let (tcp, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                // Out of descriptors, or a connection reset before it was
                // taken: say so and let the next one try.
                tracing::warn!(%error, "accepting a connection failed");
                ctx.exec.sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        let conn = ConnCtx {
            exec: ctx.exec.clone(),
            options: ctx.options.clone(),
            exchanges: ctx.exchanges.clone(),
            state: ctx.state.clone(),
            peer,
            scheme: acceptor.scheme(),
        };
        ctx.exec.spawn(serve(conn, tcp, acceptor.clone(), permit));
    }
}

/// One connection, from TLS handshake to close.
async fn serve(
    ctx: ConnCtx,
    tcp: tokio::net::TcpStream,
    acceptor: Acceptor,
    _permit: OwnedSemaphorePermit,
) {
    let stream = match acceptor {
        Acceptor::Plain => Stream::Plain(tcp),
        #[cfg(feature = "tls")]
        Acceptor::Tls(acceptor) => {
            match ctx
                .exec
                .within(ctx.options.header_timeout, acceptor.accept(tcp))
                .await
            {
                Some(Ok(tls)) => Stream::TlsServer(Box::new(tls)),
                Some(Err(error)) => {
                    tracing::debug!(peer = %ctx.peer, %error, "TLS handshake failed");
                    return;
                }
                None => return,
            }
        }
    };
    #[cfg(feature = "tls")]
    if stream.alpn() == Some(b"h2") {
        crate::h2::conn::serve(stream, ctx).await;
        return;
    }
    h1::server::serve(Conn::new(stream), ctx).await;
}
