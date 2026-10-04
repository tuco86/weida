//! Shared harness for the integration tests: a real QUIC server and client on
//! loopback, with a freshly generated self-signed certificate.
//!
//! No test hooks exist in the library. Everything below drives the public API
//! or, for the hostile-peer suite, speaks the wire protocol directly.

// Each test binary uses a different subset of this harness.
#![allow(dead_code)]

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::Instant;
use weida::{
    Binding, ClientTls, Fingerprint, Identity, Limits, Listener, Runtime, RuntimeConfig, ServerTls,
    Trust,
};

/// A generated certificate on disk, removed when dropped.
pub struct Certs {
    pub dir: PathBuf,
    pub cert_pem: PathBuf,
    pub key_pem: PathBuf,
}

impl Certs {
    /// Generates a certificate valid for `localhost`, `127.0.0.1` and `::1`.
    pub fn generate() -> Certs {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "weida-test-{}-{}-{id}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        // A killed test process cannot run `Drop`, so the directory may outlive
        // the run. Owner-only permissions keep the leftover key material
        // unreadable rather than merely short-lived.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .expect("restrict temp dir");
        }

        let generated = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
            "::1".to_owned(),
        ])
        .expect("generate certificate");

        let cert_pem = dir.join("cert.pem");
        let key_pem = dir.join("key.pem");
        std::fs::write(&cert_pem, generated.cert.pem()).expect("write cert");
        std::fs::write(&key_pem, generated.signing_key.serialize_pem()).expect("write key");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_pem, std::fs::Permissions::from_mode(0o600))
                .expect("restrict key");
        }

        Certs {
            dir,
            cert_pem,
            key_pem,
        }
    }

    pub fn identity(&self) -> Identity {
        Identity::from_pem_files(&self.cert_pem, &self.key_pem)
    }

    pub fn server_tls(&self) -> ServerTls {
        ServerTls::new(self.identity())
    }

    /// Trusts this certificate as an anchor: the name-checked CA path.
    pub fn client_tls(&self) -> ClientTls {
        ClientTls::new(Trust::anchor_file(&self.cert_pem))
    }

    pub fn fingerprint(&self) -> Fingerprint {
        self.identity().fingerprint().expect("fingerprint")
    }
}

impl Drop for Certs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A running server: runtime, listener and one loopback binding.
pub struct Server {
    pub runtime: Runtime,
    pub listener: Listener,
    pub binding: Binding,
    pub addr: SocketAddr,
    pub certs: Certs,
}

impl Server {
    /// Starts a server on an ephemeral loopback port with default limits.
    pub async fn start() -> Server {
        Server::start_with(Limits::default()).await
    }

    /// Starts a server with explicit limits.
    pub async fn start_with(limits: Limits) -> Server {
        Server::start_with_config(RuntimeConfig {
            limits,
            ..RuntimeConfig::default()
        })
        .await
    }

    /// Starts a server with a whole runtime configuration, for the tests that
    /// need more than limits — a guarantee set, for instance.
    pub async fn start_with_config(config: RuntimeConfig) -> Server {
        let certs = Certs::generate();
        let runtime = Runtime::new(config).expect("runtime");
        let listener = runtime.listener();
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                certs.server_tls(),
            )
            .await
            .expect("bind");
        let addr = binding.local_addr();
        Server {
            runtime,
            listener,
            binding,
            addr,
            certs,
        }
    }

    /// A `weida://` URL for `path` on this server.
    pub fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.addr.port(), path)
    }

    /// The same URL with the server's fingerprint in it: pinned by address.
    pub fn pinned_url(&self, path: &str) -> String {
        format!(
            "weida://{}@127.0.0.1:{}{}",
            self.certs.fingerprint(),
            self.addr.port(),
            path
        )
    }

    /// A client runtime. Trust is supplied per endpoint; see [`Server::trust`].
    pub fn client_runtime(&self) -> Runtime {
        self.client_runtime_with(Limits::default())
    }

    /// A client runtime with explicit limits.
    pub fn client_runtime_with(&self, limits: Limits) -> Runtime {
        self.client_runtime_with_config(RuntimeConfig {
            limits,
            ..RuntimeConfig::default()
        })
    }

    /// A client runtime with a whole configuration.
    pub fn client_runtime_with_config(&self, config: RuntimeConfig) -> Runtime {
        Runtime::new(config).expect("client runtime")
    }

    /// Trust anchors for this server, to hand to a dialling endpoint.
    pub fn trust(&self) -> ClientTls {
        self.certs.client_tls()
    }
}

/// A UDP forwarder in front of `server` that releases every packet
/// `one_way` late, in both directions and in the order it arrived: a slow
/// path on loopback that neither loses nor reorders. One front socket faces
/// the clients; each client address gets its own back socket toward the
/// server.
pub async fn delay_proxy(server: SocketAddr, one_way: Duration) -> SocketAddr {
    let front = Arc::new(
        UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind the proxy's front"),
    );
    let addr = front.local_addr().expect("the proxy's address");
    tokio::spawn(async move {
        let mut backs: HashMap<SocketAddr, mpsc::UnboundedSender<(Instant, Vec<u8>)>> =
            HashMap::new();
        let mut buf = vec![0u8; 65536];
        loop {
            let (n, from) = front
                .recv_from(&mut buf)
                .await
                .expect("receive at the proxy's front");
            let release = Instant::now() + one_way;
            let packet = buf[..n].to_vec();
            let up = match backs.entry(from) {
                Entry::Occupied(known) => known.into_mut(),
                Entry::Vacant(new) => {
                    let back = Arc::new(
                        UdpSocket::bind("127.0.0.1:0")
                            .await
                            .expect("bind a proxy back socket"),
                    );
                    back.connect(server)
                        .await
                        .expect("point the back socket at the server");
                    // Toward the server.
                    let (up, mut up_rx) = mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
                    let sender = Arc::clone(&back);
                    tokio::spawn(async move {
                        while let Some((at, packet)) = up_rx.recv().await {
                            tokio::time::sleep_until(at).await;
                            let _ = sender.send(&packet).await;
                        }
                    });
                    // Back toward the client.
                    let (down, mut down_rx) = mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
                    let to_client = Arc::clone(&front);
                    tokio::spawn(async move {
                        while let Some((at, packet)) = down_rx.recv().await {
                            tokio::time::sleep_until(at).await;
                            let _ = to_client.send_to(&packet, from).await;
                        }
                    });
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 65536];
                        while let Ok(n) = back.recv(&mut buf).await {
                            if down
                                .send((Instant::now() + one_way, buf[..n].to_vec()))
                                .is_err()
                            {
                                return;
                            }
                        }
                    });
                    new.insert(up)
                }
            };
            let _ = up.send((release, packet));
        }
    });
    addr
}

/// Which transport a parametrized test runs over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Native QUIC on loopback, with TLS and a generated identity.
    Quic,
    /// In process: `weida+inproc://<bus>/<path>`, no socket, no TLS
    /// ([decisions/0010](../../../docs/decisions/0010-local-transport.md)).
    Inproc,
    /// `AF_UNIX`: `weida+unix://<percent-encoded>/<path>`, no TLS, the peer
    /// proved by the kernel [0010 §4.4, §4.5].
    #[cfg(unix)]
    Unix,
    /// A Windows named pipe: `weida+pipe://<name>/<path>`, no TLS, the peer
    /// proved by the kernel [0010 §4.4, §4.5].
    #[cfg(windows)]
    Pipe,
}

/// A server reachable over either transport, so that one test body can be
/// run over both.
///
/// This is the point of the boundary B-037 introduced: nothing in the bodies
/// below knows which transport it is on, because above the transport the
/// frames, the HELLO exchange and the patterns are the same
/// (`docs/PROTOCOL.md` §2.1).
pub struct Harness {
    pub transport: Transport,
    pub runtime: Runtime,
    pub listener: Listener,
    quic: Option<(Binding, Certs)>,
    local: Option<weida::LocalBinding>,
    #[cfg(unix)]
    unix: Option<(weida::UnixBinding, std::path::PathBuf)>,
    #[cfg(windows)]
    pipe: Option<weida::PipeBinding>,
}

impl Harness {
    pub async fn start(transport: Transport) -> Harness {
        Harness::start_with(transport, RuntimeConfig::default()).await
    }

    pub async fn start_with(transport: Transport, config: RuntimeConfig) -> Harness {
        let runtime = Runtime::new(config).expect("runtime");
        let listener = runtime.listener();
        match transport {
            Transport::Quic => {
                let certs = Certs::generate();
                let binding = listener
                    .bind_quic(
                        "127.0.0.1:0".parse().expect("loopback address"),
                        certs.server_tls(),
                    )
                    .await
                    .expect("bind");
                Harness {
                    transport,
                    runtime,
                    listener,
                    quic: Some((binding, certs)),
                    local: None,
                    #[cfg(unix)]
                    unix: None,
                    #[cfg(windows)]
                    pipe: None,
                }
            }
            Transport::Inproc => {
                static BUS: AtomicU32 = AtomicU32::new(0);
                let bus = format!(
                    "weida-test-{}-{}",
                    std::process::id(),
                    BUS.fetch_add(1, Ordering::Relaxed)
                );
                let binding = listener.bind_inproc(&bus).expect("bind inproc");
                Harness {
                    transport,
                    runtime,
                    listener,
                    quic: None,
                    local: Some(binding),
                    #[cfg(unix)]
                    unix: None,
                    #[cfg(windows)]
                    pipe: None,
                }
            }
            #[cfg(unix)]
            Transport::Unix => {
                static SOCKET: AtomicU32 = AtomicU32::new(0);
                // A short path in a private directory: `sun_path` is 107
                // bytes on Linux and 104 on macOS, and the directory is what
                // closes the unlink-then-bind race [0010 §4.5].
                let dir = std::env::temp_dir().join(format!("weida-{}", std::process::id()));
                std::fs::create_dir_all(&dir).expect("socket directory");
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                        .expect("private socket directory");
                }
                let path = dir.join(format!("s{}", SOCKET.fetch_add(1, Ordering::Relaxed)));
                let binding = listener.bind_unix(&path).expect("bind unix");
                Harness {
                    transport,
                    runtime,
                    listener,
                    quic: None,
                    local: None,
                    unix: Some((binding, path)),
                }
            }
            #[cfg(windows)]
            Transport::Pipe => {
                static PIPE: AtomicU32 = AtomicU32::new(0);
                let name = format!(
                    "weida-test-{}-{}",
                    std::process::id(),
                    PIPE.fetch_add(1, Ordering::Relaxed)
                );
                let binding = listener.bind_pipe(&name).expect("bind pipe");
                Harness {
                    transport,
                    runtime,
                    listener,
                    quic: None,
                    local: None,
                    pipe: Some(binding),
                }
            }
        }
    }

    /// An address for `path` on this server, in the scheme of its transport.
    pub fn url(&self, path: &str) -> String {
        #[cfg(unix)]
        if let Some((_, socket)) = &self.unix {
            let encoded: String = socket
                .to_str()
                .expect("utf-8 socket path")
                .bytes()
                .map(|b| match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                        (b as char).to_string()
                    }
                    other => format!("%{other:02X}"),
                })
                .collect();
            return format!("weida+unix://{encoded}{path}");
        }
        #[cfg(windows)]
        if let Some(binding) = &self.pipe {
            return format!("weida+pipe://{}{path}", binding.name());
        }
        match (&self.quic, &self.local) {
            (Some((binding, _)), _) => {
                format!("weida://127.0.0.1:{}{}", binding.local_addr().port(), path)
            }
            (_, Some(binding)) => format!("weida+inproc://{}{}", binding.bus(), path),
            _ => unreachable!("a harness always has one binding"),
        }
    }

    /// The socket path, on the `AF_UNIX` harness.
    #[cfg(unix)]
    pub fn socket_path(&self) -> Option<&std::path::Path> {
        self.unix.as_ref().map(|(_, path)| path.as_path())
    }

    /// The pipe name, on the named-pipe harness.
    #[cfg(windows)]
    pub fn pipe_name(&self) -> Option<&str> {
        self.pipe.as_ref().map(|binding| binding.name())
    }

    /// Trust for a dialling endpoint. On a local address there is no key to
    /// check, so the terms are simply unused [0010 §4.4].
    pub fn trust(&self) -> ClientTls {
        match &self.quic {
            Some((_, certs)) => certs.client_tls(),
            None => ClientTls::new(Trust::by_address()),
        }
    }

    /// A client runtime for this harness.
    pub fn client(&self) -> Runtime {
        self.client_with(RuntimeConfig::default())
    }

    /// A client runtime with a whole configuration.
    pub fn client_with(&self, config: RuntimeConfig) -> Runtime {
        Runtime::new(config).expect("client runtime")
    }

    /// Shuts the server down, closing its connections on either transport.
    pub async fn shutdown(self) {
        let Harness {
            runtime,
            listener,
            quic,
            local,
            #[cfg(windows)]
            pipe,
            ..
        } = self;
        drop(listener);
        drop(quic);
        drop(local);
        #[cfg(windows)]
        drop(pipe);
        runtime.shutdown().await;
    }
}

/// Raw wire-protocol peers.
///
/// These speak `weida-protocol` over bare `quinn` so the hostile-peer suite can
/// send byte sequences the library would never produce. The library itself has
/// no test hooks.
pub mod raw {
    use std::sync::Arc;

    use quinn::rustls::pki_types::pem::PemObject;
    use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use weida_protocol::{FrameKind, Hello, MAX_PREAMBLE_LEN, Preamble, encode_frame};

    use super::Certs;

    fn provider() -> Arc<quinn::rustls::crypto::CryptoProvider> {
        Arc::new(quinn::rustls::crypto::ring::default_provider())
    }

    /// A client endpoint trusting `certs` and offering the weida ALPN.
    pub fn client_endpoint(certs: &Certs) -> quinn::Endpoint {
        let mut roots = quinn::rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(&certs.cert_pem).expect("read cert") {
            roots.add(cert.expect("parse cert")).expect("add root");
        }
        let mut crypto = quinn::rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&quinn::rustls::version::TLS13])
            .expect("tls 1.3")
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth();
        crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];
        let config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto).expect("quic crypto"),
        ));

        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().expect("loopback"))
            .expect("client endpoint");
        endpoint.set_default_client_config(config);
        endpoint
    }

    /// A server configuration for one identity.
    ///
    /// Separate from [`server_endpoint`] so a test can hand the *same* socket
    /// a different identity between handshakes — the only way to reproduce one
    /// authority answering with two different peers, since two `quinn`
    /// endpoints cannot share a port.
    pub fn server_config(certs: &Certs) -> quinn::ServerConfig {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&certs.cert_pem)
            .expect("read cert")
            .collect::<Result<_, _>>()
            .expect("parse cert");
        let key = PrivateKeyDer::from_pem_file(&certs.key_pem).expect("read key");
        let mut crypto = quinn::rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&quinn::rustls::version::TLS13])
            .expect("tls 1.3")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server cert");
        crypto.alpn_protocols = vec![weida_protocol::ALPN.to_vec()];
        quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(crypto).expect("quic crypto"),
        ))
    }

    /// A server endpoint on an ephemeral loopback port.
    pub fn server_endpoint(certs: &Certs) -> (quinn::Endpoint, std::net::SocketAddr) {
        let endpoint = quinn::Endpoint::server(
            server_config(certs),
            "127.0.0.1:0".parse().expect("loopback"),
        )
        .expect("server endpoint");
        let addr = endpoint.local_addr().expect("local addr");
        (endpoint, addr)
    }

    /// Sends a well-formed HELLO so the peer's negotiation completes.
    pub async fn send_hello(conn: &quinn::Connection) {
        send_frame(conn, FrameKind::Hello, &Hello::v0(16 * 1024, 1024).encode()).await;
    }

    /// A HELLO header whose `guarantees_offered` map is built byte by byte.
    ///
    /// The library's own encoder cannot produce an illegal set — that is the
    /// point of `GuaranteeSet::validate` — so the hostile suite writes the
    /// CBOR itself: the five required keys, then key `5` holding exactly the
    /// `(key, value)` pairs given.
    pub fn hello_with_guarantee_map(pairs: &[(u64, u64)]) -> Vec<u8> {
        fn uint(out: &mut Vec<u8>, value: u64) {
            match value {
                0..=23 => out.push(value as u8),
                24..=255 => out.extend_from_slice(&[0x18, value as u8]),
                256..=65535 => {
                    out.push(0x19);
                    out.extend_from_slice(&(value as u16).to_be_bytes());
                }
                _ => {
                    out.push(0x1B);
                    out.extend_from_slice(&value.to_be_bytes());
                }
            }
        }

        let mut out = vec![0xA6];
        uint(&mut out, 0);
        out.extend_from_slice(&[0x81, 0x00]); // versions = [0]
        uint(&mut out, 1);
        uint(&mut out, 16 * 1024);
        uint(&mut out, 2);
        uint(&mut out, 1024);
        uint(&mut out, 3);
        out.push(0x80); // capabilities = []
        uint(&mut out, 4);
        out.push(0x80); // required_capabilities = []
        uint(&mut out, 5);
        assert!(pairs.len() < 24, "the map header below is one byte");
        out.push(0xA0 | pairs.len() as u8);
        for (key, value) in pairs {
            uint(&mut out, *key);
            uint(&mut out, *value);
        }
        out
    }

    /// Sends one header-only frame on its own stream.
    pub async fn send_frame(conn: &quinn::Connection, kind: FrameKind, header: &[u8]) {
        let mut stream = conn.open_uni().await.expect("open uni");
        stream
            .write_all(&encode_frame(kind, header))
            .await
            .expect("write frame");
        stream.finish().expect("finish");
    }

    /// Sends arbitrary bytes on a fresh unidirectional stream.
    pub async fn send_raw(conn: &quinn::Connection, bytes: &[u8]) {
        let mut stream = conn.open_uni().await.expect("open uni");
        stream.write_all(bytes).await.expect("write raw");
        stream.finish().expect("finish");
    }

    /// Reads one frame's preamble and header from `stream`.
    pub async fn read_frame(stream: &mut quinn::RecvStream) -> (Preamble, Vec<u8>) {
        let mut scratch = [0u8; MAX_PREAMBLE_LEN];
        let mut have = 0usize;
        let preamble = loop {
            match weida_protocol::parse_preamble(&scratch[..have], 16 * 1024) {
                Ok((preamble, _)) => break preamble,
                Err(weida_protocol::PreambleError::Incomplete) => {
                    let n = stream
                        .read(&mut scratch[have..have + 1])
                        .await
                        .expect("read preamble")
                        .expect("stream ended inside the preamble");
                    have += n;
                }
                Err(e) => panic!("bad preamble: {e}"),
            }
        };
        let mut header = vec![0u8; preamble.header_len as usize];
        stream.read_exact(&mut header).await.expect("read header");
        (preamble, header)
    }

    /// Accepts inbound streams until one carries a frame of `kind`, returning
    /// the stream and its header bytes.
    ///
    /// Necessary because the peer's own HELLO arrives on an unrelated stream:
    /// unidirectional streams are unordered, so "the next stream" is never a
    /// safe assumption.
    pub async fn accept_frame(
        conn: &quinn::Connection,
        kind: FrameKind,
    ) -> (quinn::RecvStream, Vec<u8>) {
        loop {
            let mut stream = conn.accept_uni().await.expect("accept uni");
            let (preamble, header) = read_frame(&mut stream).await;
            if preamble.kind == kind {
                return (stream, header);
            }
        }
    }

    /// Accepts unidirectional streams until a DATA frame arrives, returning
    /// its header.
    pub async fn accept_data(
        conn: &quinn::Connection,
    ) -> (quinn::RecvStream, weida_protocol::DataHeader) {
        let (stream, header) = accept_frame(conn, FrameKind::Data).await;
        (
            stream,
            weida_protocol::DataHeader::decode(&header).expect("decode DATA header"),
        )
    }

    /// Accepts one bidirectional stream and reads its DATA header.
    ///
    /// The initiating half of an exchange always starts with DATA, so anything
    /// else here is the library misbehaving.
    pub async fn accept_exchange(
        conn: &quinn::Connection,
    ) -> (
        quinn::SendStream,
        quinn::RecvStream,
        weida_protocol::DataHeader,
    ) {
        let (send, mut recv) = conn.accept_bi().await.expect("accept bi");
        let (preamble, header) = read_frame(&mut recv).await;
        assert_eq!(preamble.kind, FrameKind::Data, "exchanges open with DATA");
        (
            send,
            recv,
            weida_protocol::DataHeader::decode(&header).expect("decode DATA header"),
        )
    }

    /// Opens a bidirectional stream and writes one DATA header on it.
    pub async fn open_exchange(
        conn: &quinn::Connection,
        header: &weida_protocol::DataHeader,
    ) -> (quinn::SendStream, quinn::RecvStream) {
        let (mut send, recv) = conn.open_bi().await.expect("open bi");
        let encoded = header.encode();
        let mut bytes = Vec::new();
        weida_protocol::encode_preamble(FrameKind::Data, encoded.len() as u64, &mut bytes);
        bytes.extend_from_slice(&encoded);
        send.write_all(&bytes).await.expect("write DATA header");
        (send, recv)
    }

    /// Waits for the peer to close the connection and returns the application
    /// error code it used.
    pub async fn closed_code(conn: &quinn::Connection) -> u64 {
        match conn.closed().await {
            quinn::ConnectionError::ApplicationClosed(frame) => frame.error_code.into_inner(),
            other => panic!("expected an application close, got {other}"),
        }
    }
}

/// Deterministic payload generator: no dependency, reproducible failures.
pub struct Xorshift(u64);

impl Xorshift {
    pub fn new(seed: u64) -> Xorshift {
        Xorshift(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Fills `buf` with pseudorandom bytes.
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&bytes[..n]);
        }
    }
}

/// FNV-1a, for cheap end-to-end payload verification.
pub struct Fnv(u64);

impl Default for Fnv {
    fn default() -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv {
    pub fn update(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x1000_0000_01b3);
        }
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}
