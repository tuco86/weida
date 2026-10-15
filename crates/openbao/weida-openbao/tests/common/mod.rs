//! A scripted OpenBao: an HTTP/1.1 server on loopback that answers each
//! request from a script, so a test states exactly what the API said.
//!
//! No TLS — the client is configured with `http://`, as against a dev
//! server — and no HTTP beyond what the client sends: one request per
//! connection is not assumed, `Content-Length` bodies are, and every answer
//! closes the connection so the parser needs no keep-alive bookkeeping.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One request as the script sees it.
#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Value,
}

impl Request {
    pub fn token(&self) -> Option<&str> {
        self.headers.get("x-vault-token").map(String::as_str)
    }
}

/// What a script answers with, byte for byte: for the tests that need a
/// header the JSON form cannot carry, or a body that is not what OpenBao sends.
pub struct Raw {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Raw {
    /// `body` as OpenBao sends one.
    pub fn json(status: u16, body: &Value) -> Raw {
        Raw {
            status,
            headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
            body: body.to_string().into_bytes(),
        }
    }
}

type Handler = Box<dyn Fn(&Request) -> Raw + Send + Sync>;
type Handlers = HashMap<(String, String), Arc<Handler>>;

/// A script: path → handler. A path not in the script answers 404 with an
/// OpenBao-shaped error, and every request is recorded.
#[derive(Clone)]
pub struct Script {
    handlers: Arc<Mutex<Handlers>>,
    pub seen: Arc<Mutex<Vec<Request>>>,
}

impl Script {
    pub fn new() -> Script {
        Script {
            handlers: Arc::new(Mutex::new(HashMap::new())),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Answers `method path` (path without `/v1/`) with `status` and `body`.
    pub fn on(
        &self,
        method: &str,
        path: &str,
        handler: impl Fn(&Request) -> (u16, Value) + Send + Sync + 'static,
    ) -> &Script {
        self.on_raw(method, path, move |request| {
            let (status, body) = handler(request);
            Raw::json(status, &body)
        })
    }

    /// Like [`Script::on`], for an answer given as bytes.
    pub fn on_raw(
        &self,
        method: &str,
        path: &str,
        handler: impl Fn(&Request) -> Raw + Send + Sync + 'static,
    ) -> &Script {
        self.handlers.lock().expect("script poisoned").insert(
            (
                method.to_owned(),
                format!("/v1/{}", path.trim_start_matches('/')),
            ),
            Arc::new(Box::new(handler)),
        );
        self
    }

    /// A fixed answer.
    pub fn answer(&self, method: &str, path: &str, status: u16, body: Value) -> &Script {
        self.on(method, path, move |_| (status, body.clone()))
    }

    /// Requests seen for `path`, in order.
    pub fn seen_on(&self, path: &str) -> Vec<Request> {
        let path = format!("/v1/{}", path.trim_start_matches('/'));
        self.seen
            .lock()
            .expect("seen poisoned")
            .iter()
            .filter(|r| r.path == path)
            .cloned()
            .collect()
    }

    fn dispatch(&self, request: &Request) -> Raw {
        self.seen
            .lock()
            .expect("seen poisoned")
            .push(request.clone());
        let handler = self
            .handlers
            .lock()
            .expect("script poisoned")
            .get(&(request.method.clone(), request.path.clone()))
            .cloned();
        match handler {
            Some(handler) => handler(request),
            None => Raw::json(
                404,
                &serde_json::json!({ "errors": [format!("unscripted: {} {}", request.method, request.path)] }),
            ),
        }
    }
}

impl Default for Script {
    fn default() -> Self {
        Script::new()
    }
}

/// The server: bound on an ephemeral loopback port, serving `script` until
/// dropped.
pub struct Bao {
    pub address: String,
    pub script: Script,
    _serving: tokio::task::JoinHandle<()>,
}

impl Bao {
    pub async fn start(script: Script) -> Bao {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = format!("http://{}", listener.local_addr().expect("addr"));
        let serving = tokio::spawn({
            let script = script.clone();
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let script = script.clone();
                    tokio::spawn(async move { serve(stream, script).await });
                }
            }
        });
        Bao {
            address,
            script,
            _serving: serving,
        }
    }
}

impl Drop for Bao {
    fn drop(&mut self) {
        self._serving.abort();
    }
}

async fn serve(mut stream: tokio::net::TcpStream, script: Script) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    // Read until the head is complete and the body is as long as announced.
    let (head_len, body_len) = loop {
        let n = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(end) = find(&buffer, b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..end]).into_owned();
            let body_len = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    (k.trim().eq_ignore_ascii_case("content-length"))
                        .then(|| v.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if buffer.len() >= end + 4 + body_len {
                break (end + 4, body_len);
            }
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_len]).into_owned();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or_default().to_owned();
    let headers: HashMap<String, String> = lines
        .filter_map(|l| {
            let (k, v) = l.split_once(':')?;
            Some((k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        })
        .collect();
    let body = &buffer[head_len..head_len + body_len];
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body).unwrap_or(Value::Null)
    };
    let request = Request {
        method,
        path,
        headers,
        body,
    };
    let answer = script.dispatch(&request);
    // Chunked when the script says so: an answer that declares no length is
    // the one the client has to cut without being told how long it is.
    let chunked = answer.headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("transfer-encoding") && value.eq_ignore_ascii_case("chunked")
    });
    let mut head = format!("HTTP/1.1 {} {}\r\n", answer.status, reason(answer.status));
    for (name, value) in &answer.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if !chunked {
        head.push_str(&format!("Content-Length: {}\r\n", answer.body.len()));
    }
    head.push_str("Connection: close\r\n\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    if chunked {
        for chunk in answer.body.chunks(64 * 1024) {
            let _ = stream
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await;
            let _ = stream.write_all(chunk).await;
            let _ = stream.write_all(b"\r\n").await;
        }
        let _ = stream.write_all(b"0\r\n\r\n").await;
    } else {
        let _ = stream.write_all(&answer.body).await;
    }
    let _ = stream.shutdown().await;
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        307 => "Temporary Redirect",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Status",
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
