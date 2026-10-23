//! The baselines under weida: raw quinn with weida's transport defaults and
//! plain TCP, both speaking one 9-byte request: direction (`0` client to
//! server, `1` server to client) and length (u64 BE). Upward, the server
//! answers one byte once it has read the whole length, so the client's time
//! ends at delivery as `push-1`'s does.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::VarInt;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::pem::PemObject;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use weida::Limits;

use crate::{MIB, identity_pem, pattern};

type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

const ALPN: &[u8] = b"weida-perf";

/// What one baseline transfer measured beside its time.
pub struct Measured {
    pub rtt: Option<Duration>,
    pub lost: Option<u64>,
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn certificates() -> Res<Vec<CertificateDer<'static>>> {
    Ok(CertificateDer::pem_slice_iter(identity_pem()).collect::<Result<_, _>>()?)
}

/// quinn's transport settings as weida's `transport_config` sets them for
/// the default `Limits`: windows and the congestion controller.
fn transport(dialling: bool) -> Res<quinn::TransportConfig> {
    let limits = Limits::default();
    let mut tc = quinn::TransportConfig::default();
    tc.stream_receive_window(VarInt::from_u64(limits.stream_receive_window)?);
    tc.receive_window(VarInt::from_u64(limits.connection_receive_window)?);
    tc.keep_alive_interval(dialling.then_some(limits.keep_alive));
    tc.congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default()));
    Ok(tc)
}

/// Serves raw quinn on `addr` until the process ends.
pub async fn serve_quinn(addr: SocketAddr) -> Res<()> {
    let key = PrivateKeyDer::from_pem_slice(identity_pem())?;
    let mut tls = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(certificates()?, key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(Arc::new(transport(false)?));
    let endpoint = quinn::Endpoint::server(config, addr)?;
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            tokio::spawn(async move {
                if let Err(e) = quinn_session(incoming).await {
                    eprintln!("quinn session: {e}");
                }
            });
        }
    });
    Ok(())
}

async fn quinn_session(incoming: quinn::Incoming) -> Res<()> {
    let conn = incoming.await?;
    let (mut send, mut recv) = conn.accept_bi().await?;
    let mut head = [0u8; 9];
    recv.read_exact(&mut head).await?;
    let len = u64::from_be_bytes(head[1..].try_into()?);
    if head[0] == 0 {
        let mut buf = vec![0u8; MIB];
        let mut got = 0u64;
        while let Some(n) = recv.read(&mut buf).await? {
            got += n as u64;
        }
        if got != len {
            return Err(format!("quinn sink: {got} of {len} bytes").into());
        }
        send.write_all(&[1]).await?;
        send.finish()?;
    } else {
        let chunk = pattern();
        let mut left = len;
        while left > 0 {
            let n = left.min(chunk.len() as u64) as usize;
            send.write_chunk(chunk.slice(..n)).await?;
            left -= n as u64;
        }
        send.finish()?;
    }
    conn.closed().await;
    Ok(())
}

/// Accepts only the perf certificate, and checks the handshake signature.
#[derive(Debug)]
struct PinPerf {
    certificate: CertificateDer<'static>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinPerf {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.certificate.as_ref() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("not the perf certificate".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// One raw quinn transfer of `len` bytes; `up` sends to the server.
pub async fn quinn_transfer(server: SocketAddr, up: bool, len: u64) -> Res<Measured> {
    let provider = provider();
    let verifier = PinPerf {
        certificate: certificates()?.remove(0),
        provider: Arc::clone(&provider),
    };
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport(true)?));
    let local: SocketAddr = if server.is_ipv4() {
        "0.0.0.0:0".parse()?
    } else {
        "[::]:0".parse()?
    };
    let mut endpoint = quinn::Endpoint::client(local)?;
    endpoint.set_default_client_config(config);
    let conn = endpoint.connect(server, "weida-perf")?.await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    let mut head = [0u8; 9];
    head[0] = u8::from(!up);
    head[1..].copy_from_slice(&len.to_be_bytes());
    send.write_all(&head).await?;
    if up {
        let chunk = pattern();
        let mut left = len;
        while left > 0 {
            let n = left.min(chunk.len() as u64) as usize;
            send.write_chunk(chunk.slice(..n)).await?;
            left -= n as u64;
        }
        send.finish()?;
        let mut ack = [0u8; 1];
        recv.read_exact(&mut ack).await?;
    } else {
        send.finish()?;
        let mut buf = vec![0u8; MIB];
        let mut got = 0u64;
        while let Some(n) = recv.read(&mut buf).await? {
            got += n as u64;
        }
        if got != len {
            return Err(format!("quinn source: {got} of {len} bytes").into());
        }
    }
    let stats = conn.stats();
    let measured = Measured {
        rtt: Some(conn.rtt()),
        lost: Some(stats.path.lost_packets),
    };
    conn.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    Ok(measured)
}

/// Serves plain TCP on `addr` until the process ends.
pub async fn serve_tcp(addr: SocketAddr) -> Res<()> {
    let listener = TcpListener::bind(addr).await?;
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                if let Err(e) = tcp_session(stream).await {
                    eprintln!("tcp session: {e}");
                }
            });
        }
    });
    Ok(())
}

async fn tcp_session(mut stream: TcpStream) -> Res<()> {
    stream.set_nodelay(true)?;
    let mut head = [0u8; 9];
    stream.read_exact(&mut head).await?;
    let len = u64::from_be_bytes(head[1..].try_into()?);
    if head[0] == 0 {
        let got = sink(&mut stream).await?;
        if got != len {
            return Err(format!("tcp sink: {got} of {len} bytes").into());
        }
        stream.write_all(&[1]).await?;
    } else {
        source(&mut stream, len).await?;
    }
    stream.shutdown().await?;
    Ok(())
}

async fn sink(stream: &mut TcpStream) -> Res<u64> {
    let mut buf = vec![0u8; MIB];
    let mut got = 0u64;
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(got);
        }
        got += n as u64;
    }
}

async fn source(stream: &mut TcpStream, len: u64) -> Res<()> {
    let chunk = pattern();
    let mut left = len;
    while left > 0 {
        let n = left.min(chunk.len() as u64) as usize;
        stream.write_all(&chunk[..n]).await?;
        left -= n as u64;
    }
    Ok(())
}

/// One TCP transfer of `len` bytes; `up` sends to the server.
pub async fn tcp_transfer(server: SocketAddr, up: bool, len: u64) -> Res<()> {
    let mut stream = TcpStream::connect(server).await?;
    stream.set_nodelay(true)?;
    let mut head = [0u8; 9];
    head[0] = u8::from(!up);
    head[1..].copy_from_slice(&len.to_be_bytes());
    stream.write_all(&head).await?;
    if up {
        source(&mut stream, len).await?;
        stream.shutdown().await?;
        let mut ack = [0u8; 1];
        stream.read_exact(&mut ack).await?;
    } else {
        let got = sink(&mut stream).await?;
        if got != len {
            return Err(format!("tcp source: {got} of {len} bytes").into());
        }
    }
    Ok(())
}
