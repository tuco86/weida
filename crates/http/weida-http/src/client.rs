//! The client: requests to any origin over pooled connections.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;
use weida_http_codec::fields::is_connection_specific;
use weida_http_codec::uri::Scheme;
use weida_runtime::Exec;

use crate::body::{BodyReader, BodyWriter, OutBody};
use crate::dial::{self, Budget};
use crate::error::{Error, Result};
use crate::h1::Conn;
use crate::h1::client::{OutFraming, Upload};
use crate::h2::H2Handle;
#[cfg(feature = "tls")]
use crate::h2::conn::Local;
use crate::options::{ClientOptions, Protocol};
use crate::pool::{OriginKey, Pool, PoolReturn};
use crate::types::{Fields, Method, RequestHead, ResponseHead, Url, Version};

/// A response: its head, the version it came over, and its body.
#[derive(Debug)]
pub struct Response {
    /// Status and fields.
    pub head: ResponseHead,
    /// The version the exchange used.
    pub version: Version,
    /// The body, streamed.
    pub body: BodyReader,
}

/// An HTTP client. Cheap to clone; clones share one pool of connections.
///
/// It follows no redirects and stores no cookies: a 3xx is returned to the
/// caller, who decides whether the request (and its credentials) may go to
/// the place named (0044 §4).
#[derive(Clone)]
pub struct Client(Arc<Inner>);

struct Inner {
    exec: Exec,
    options: ClientOptions,
    pool: Arc<Pool>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("protocol", &self.0.options.protocol)
            .finish_non_exhaustive()
    }
}

/// A request whose body is still being written.
#[derive(Debug)]
pub struct OpenRequest {
    inner: OpenInner,
}

#[derive(Debug)]
enum OpenInner {
    H1(Upload),
    H2 {
        body: BodyWriter,
        response: oneshot::Receiver<Result<Response>>,
    },
}

/// The stream's own error when there is one: a write that failed because the
/// stream was refused or reset says less than the refusal itself.
fn stream_error(response: &mut oneshot::Receiver<Result<Response>>, e: Error) -> Error {
    match response.try_recv() {
        Ok(Err(stream)) => stream,
        _ => e,
    }
}

impl OpenRequest {
    /// Sends `data` as the next part of the body.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] past a declared `content-length`; transport
    /// errors. After an error the request is unusable.
    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        match &mut self.inner {
            OpenInner::H1(upload) => upload.write(data).await,
            OpenInner::H2 { body, response } => match body.write(data).await {
                Ok(()) => Ok(()),
                Err(e) => Err(stream_error(response, e)),
            },
        }
    }

    /// Ends the body and waits for the response head.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] short of a declared `content-length`; transport,
    /// decode and protocol errors.
    pub async fn finish(self) -> Result<Response> {
        self.end(None).await
    }

    /// Ends the body with trailer fields and waits for the response head.
    /// On HTTP/1.1 only a body without a declared length (chunked) can carry
    /// trailers.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] `"trailers need chunked framing"`; as
    /// [`OpenRequest::finish`].
    pub async fn finish_with_trailers(self, trailers: Fields) -> Result<Response> {
        self.end(Some(trailers)).await
    }

    async fn end(self, trailers: Option<Fields>) -> Result<Response> {
        match self.inner {
            OpenInner::H1(upload) => upload.finish(trailers.as_ref()).await,
            OpenInner::H2 { body, mut response } => {
                if let Err(e) = body.end(trailers).await {
                    return Err(stream_error(&mut response, e));
                }
                response.await.map_err(|_| Error::Closed)?
            }
        }
    }
}

/// A method that carries a body by definition, so an empty one is announced
/// as `content-length: 0` (RFC 9110 §8.6).
fn expects_body(method: &Method) -> bool {
    *method == Method::POST || *method == Method::PUT || *method == Method::PATCH
}

impl Client {
    /// A client on `exec`'s runtime.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for options [`ClientOptions::validate`]
    /// refuses.
    pub fn new(exec: &Exec, options: ClientOptions) -> Result<Client> {
        options.validate()?;
        let pool = Arc::new(Pool::new(
            exec.clone(),
            options.max_idle_per_origin,
            options.idle_timeout,
        ));
        Ok(Client(Arc::new(Inner {
            exec: exec.clone(),
            options,
            pool,
        })))
    }

    /// Sends a request with a complete body and waits for the response head.
    ///
    /// The body goes with a `content-length`: always when it is not empty,
    /// and for `POST`, `PUT` and `PATCH` when it is. A request the server
    /// refused unprocessed (an idle connection it had closed, HTTP/2 GOAWAY)
    /// is sent once more on a fresh connection.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidUrl`]; [`Error::Unsupported`] for `CONNECT`;
    /// [`Error::Configuration`] for a connection-specific field, `host`, a
    /// `content-length` that differs from the body, or `https` without
    /// [`ClientOptions`]' TLS configuration; transport, TLS, decode and
    /// protocol errors.
    pub async fn request(
        &self,
        method: Method,
        url: &str,
        mut fields: Fields,
        body: Bytes,
    ) -> Result<Response> {
        let (url, mut head) = self.prepare(method, url, fields.clone())?;
        if let Some(declared) = fields.get("content-length") {
            if declared.as_ref() != body.len().to_string().as_bytes() {
                return Err(Error::Configuration(
                    "content-length differs from the body's length".into(),
                ));
            }
            fields.remove("content-length");
            head.fields = fields;
        }
        let framing = if !body.is_empty() || expects_body(&head.method) {
            OutFraming::Length(body.len() as u64)
        } else {
            OutFraming::None
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = async {
                let mut open = self.start(&url, &head, framing).await?;
                open.write(body.clone()).await?;
                open.finish().await
            }
            .await;
            match result {
                Err(Error::GoingAway) if attempt == 1 => {
                    tracing::debug!(origin = %url.authority(), "request not processed, sending it again");
                }
                result => return result,
            }
        }
    }

    /// Opens a request whose body the caller streams with
    /// [`OpenRequest::write`]; [`OpenRequest::finish`] waits for the response
    /// head. Without a `content-length` field the body is chunked on
    /// HTTP/1.1. Never retried.
    ///
    /// # Errors
    ///
    /// As [`Client::request`], and [`Error::Configuration`] for a
    /// `content-length` that is not a number.
    pub async fn open(&self, method: Method, url: &str, fields: Fields) -> Result<OpenRequest> {
        let (url, mut head) = self.prepare(method, url, fields)?;
        let framing = match head.fields.get("content-length") {
            Some(v) => {
                let n = std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| Error::Configuration("content-length is not a number".into()))?;
                head.fields.remove("content-length");
                OutFraming::Length(n)
            }
            None => OutFraming::Chunked,
        };
        self.start(&url, &head, framing).await
    }

    fn prepare(&self, method: Method, url: &str, fields: Fields) -> Result<(Url, RequestHead)> {
        let url = Url::parse(url)?;
        if method == Method::CONNECT {
            return Err(Error::Unsupported("CONNECT"));
        }
        for (name, _) in fields.iter() {
            if is_connection_specific(name.as_bytes()) || name == "host" || name == "te" {
                return Err(Error::Configuration(format!(
                    "{name} is the connection's field, not the request's"
                )));
            }
        }
        if url.scheme == Scheme::Https && !self.has_tls() {
            return Err(Error::Configuration(
                "https needs ClientOptions::tls".into(),
            ));
        }
        let head = RequestHead {
            method,
            scheme: url.scheme,
            authority: url.authority(),
            path: url.path_and_query.clone(),
            fields,
        };
        Ok((url, head))
    }

    fn has_tls(&self) -> bool {
        #[cfg(feature = "tls")]
        {
            self.0.options.tls.is_some()
        }
        #[cfg(not(feature = "tls"))]
        {
            false
        }
    }

    /// A connection to `url`'s origin, and the request started on it: the
    /// origin's shared HTTP/2 connection, else its newest idle HTTP/1.1 one,
    /// else a new one.
    async fn start(
        &self,
        url: &Url,
        head: &RequestHead,
        framing: OutFraming,
    ) -> Result<OpenRequest> {
        let options = &self.0.options;
        match (options.protocol, url.scheme) {
            (Protocol::Http2, Scheme::Http) => {
                return Err(Error::Unsupported("h2 without TLS (h2c)"));
            }
            (Protocol::Http3, Scheme::Http) => return Err(Error::Unsupported("h3 without TLS")),
            (Protocol::Http3, Scheme::Https) => {
                return Err(Error::Unsupported("built without the h3 feature"));
            }
            _ => {}
        }
        let key: OriginKey = (url.scheme, url.host.to_ascii_lowercase(), url.port);
        if let Some(handle) = self.0.pool.shared(&key) {
            return open_h2(&handle, head, framing).await;
        }
        let (conn, reused) = match self.0.pool.take_h1(&key) {
            Some(conn) => (conn, true),
            None => match self.dial(url).await? {
                Dialed::H1(conn) => (conn, false),
                #[cfg(feature = "tls")]
                Dialed::H2(handle) => {
                    // The first h2 connection to an origin is its shared one;
                    // a later one serves this request and closes after it.
                    self.0.pool.set_shared(key, &handle);
                    return open_h2(&handle, head, framing).await;
                }
            },
        };
        let keep = PoolReturn::new(&self.0.pool, key);
        let upload = Upload::start(conn, head, framing, options.limits, reused, Some(keep))?;
        Ok(OpenRequest {
            inner: OpenInner::H1(upload),
        })
    }

    async fn dial(&self, url: &Url) -> Result<Dialed> {
        let exec = &self.0.exec;
        let options = &self.0.options;
        let budget = Budget::start(exec, options.connect_timeout);
        let stream = dial::connect(
            exec,
            &url.host,
            url.port,
            options.max_resolved_addresses,
            &budget,
        )
        .await?;
        match url.scheme {
            Scheme::Http => Ok(Dialed::H1(Conn::new(stream))),
            #[cfg(feature = "tls")]
            Scheme::Https => {
                let config = options
                    .tls
                    .as_ref()
                    .ok_or_else(|| Error::Configuration("https needs ClientOptions::tls".into()))?;
                let alpn: &[&[u8]] = match options.protocol {
                    Protocol::Http1 => &[b"http/1.1"],
                    Protocol::Http2 => &[b"h2"],
                    _ => &[b"h2", b"http/1.1"],
                };
                let stream =
                    dial::upgrade_tls(exec, stream, config, &url.host, alpn, &budget).await?;
                if stream.alpn() == Some(b"h2") {
                    let local = Local {
                        stream_window: options.stream_window,
                        connection_window: options.connection_window,
                        max_concurrent_streams: None,
                        limits: options.limits,
                    };
                    return Ok(Dialed::H2(crate::h2::conn::spawn_client(
                        exec, stream, local,
                    )));
                }
                if options.protocol == Protocol::Http2 {
                    return Err(Error::Protocol("the server did not agree to h2".into()));
                }
                Ok(Dialed::H1(Conn::new(stream)))
            }
            #[cfg(not(feature = "tls"))]
            Scheme::Https => Err(Error::Configuration(
                "https needs ClientOptions::tls".into(),
            )),
        }
    }
}

/// A new connection: HTTP/1.1, or an HTTP/2 driver.
enum Dialed {
    H1(Conn),
    #[cfg(feature = "tls")]
    H2(H2Handle),
}

/// Opens a stream for `head` on an HTTP/2 connection.
pub(crate) async fn open_h2(
    handle: &H2Handle,
    head: &RequestHead,
    framing: OutFraming,
) -> Result<OpenRequest> {
    let (length, body_framing, end_stream) = match framing {
        OutFraming::None => (None, OutBody::None, true),
        OutFraming::Length(n) => (Some(n), OutBody::Length(n), n == 0),
        OutFraming::Chunked => (None, OutBody::Open, false),
    };
    let opened = handle.open(head.clone(), length, end_stream).await?;
    let body = if end_stream {
        // The writer has nothing to send: HEADERS carried END_STREAM.
        opened.writer.disarm();
        BodyWriter::done()
    } else {
        BodyWriter::h2(opened.writer, body_framing)
    };
    Ok(OpenRequest {
        inner: OpenInner::H2 {
            body,
            response: opened.response,
        },
    })
}
