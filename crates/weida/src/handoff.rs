//! Handing a QUIC binding over to a successor process
//! ([decisions/0039](../../../docs/decisions/0039-handover-says-when.md)).
//!
//! Every QUIC binding sits on a [`HandoverSocket`], a thin decorator over the
//! runtime's UDP socket. While the binding serves it delegates everything.
//! [`Binding::hand_over`](crate::Binding::hand_over) makes it *hold*: from
//! then on nothing is read from the socket, and every datagram the endpoint
//! sends — the `RESTARTING` close of each connection — is kept rather than
//! sent. The held datagrams travel to the successor in a [`Handoff`], and
//! [`Binding::take_over`](crate::Binding::take_over) sends them once the
//! successor serves, so a client learns of the restart only when its redial
//! can be accepted.

use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use weida_core::Error;

/// The decorator delegates everything.
const LIVE: u8 = 0;
/// Sends are kept, nothing is read.
const HOLDING: u8 = 1;
/// Sends are dropped, nothing is read.
const DISCARDING: u8 = 2;

/// The first bytes of an encoded [`Handoff`].
const MAGIC: &[u8; 4] = b"WDHO";
/// The one version of the byte format.
const VERSION: u8 = 1;

const TAG_NONE: u8 = 0;
const TAG_V4: u8 = 4;
const TAG_V6: u8 = 6;

/// One datagram the predecessor's endpoint sent while its socket was held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeldDatagram {
    pub(crate) destination: SocketAddr,
    pub(crate) src_ip: Option<IpAddr>,
    pub(crate) contents: Vec<u8>,
}

/// The socket under every QUIC binding: the runtime's UDP socket, plus the
/// switch that lets [`Binding::hand_over`](crate::Binding::hand_over) stop
/// the endpoint reading and capture what it sends.
pub(crate) struct HandoverSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    mode: AtomicU8,
    /// Written under [`HOLDING`] only; the mode changes under this lock, so a
    /// send never lands after [`HandoverSocket::release`] took the vector.
    held: Mutex<Vec<HeldDatagram>>,
}

impl HandoverSocket {
    pub(crate) fn new(inner: Arc<dyn AsyncUdpSocket>) -> Self {
        HandoverSocket {
            inner,
            mode: AtomicU8::new(LIVE),
            held: Mutex::new(Vec::new()),
        }
    }

    /// Stops reading and starts keeping every datagram sent.
    pub(crate) fn hold(&self) {
        let _held = self.lock();
        self.mode.store(HOLDING, Ordering::Release);
    }

    /// Takes the datagrams kept since [`HandoverSocket::hold`]; anything sent
    /// later is dropped.
    pub(crate) fn release(&self) -> Vec<HeldDatagram> {
        let mut held = self.lock();
        self.mode.store(DISCARDING, Ordering::Release);
        std::mem::take(&mut *held)
    }

    /// Sends one held datagram on the underlying socket, whatever the mode,
    /// waiting for the socket to become writable when it is full.
    pub(crate) async fn send_raw(&self, datagram: &HeldDatagram) -> io::Result<()> {
        let transmit = Transmit {
            destination: datagram.destination,
            ecn: None,
            contents: &datagram.contents,
            segment_size: None,
            src_ip: datagram.src_ip,
        };
        let mut poller: Option<Pin<Box<dyn UdpPoller>>> = None;
        loop {
            match self.inner.try_send(&transmit) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let poller =
                        poller.get_or_insert_with(|| Arc::clone(&self.inner).create_io_poller());
                    std::future::poll_fn(|cx| poller.as_mut().poll_writable(cx)).await?;
                }
                result => return result,
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<HeldDatagram>> {
        self.held.lock().expect("held datagrams poisoned")
    }
}

impl AsyncUdpSocket for HandoverSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Arc::clone(&self.inner).create_io_poller()
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        if self.mode.load(Ordering::Acquire) == LIVE {
            return self.inner.try_send(transmit);
        }
        let mut held = self.lock();
        match self.mode.load(Ordering::Acquire) {
            LIVE => return self.inner.try_send(transmit),
            HOLDING => {
                // A GSO batch is several datagrams of `segment_size` bytes,
                // the last one possibly shorter; each is kept on its own.
                let size = transmit.segment_size.unwrap_or(transmit.contents.len());
                for contents in transmit.contents.chunks(size.max(1)) {
                    held.push(HeldDatagram {
                        destination: transmit.destination,
                        src_ip: transmit.src_ip,
                        contents: contents.to_vec(),
                    });
                }
            }
            _ => {}
        }
        // Neither keeping nor dropping ever blocks: a `WouldBlock` would only
        // have the connection wait on the real socket's writability.
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        // A held socket is never read again: what arrives stays in the
        // kernel's buffer for the successor.
        if self.mode.load(Ordering::Acquire) == LIVE {
            self.inner.poll_recv(cx, bufs, meta)
        } else {
            Poll::Pending
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    // The trait's defaults would turn GSO and GRO off and change the MTU
    // behaviour, so the inner socket's answers are passed on.
    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

impl fmt::Debug for HandoverSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandoverSocket")
            .field("mode", &self.mode.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// What [`Binding::hand_over`](crate::Binding::hand_over) leaves for the
/// successor: the bound socket and the close packets held for its clients
/// ([decisions/0039](../../../docs/decisions/0039-handover-says-when.md)).
///
/// [`Handoff::into_parts`] splits it in two. The socket goes to the successor
/// as a descriptor, over `SCM_RIGHTS` or inherited across `exec`, and is
/// served there with [`Listener::bind_quic_socket`](crate::Listener::bind_quic_socket).
/// The bytes go alongside it, in any channel, to
/// [`Binding::take_over`](crate::Binding::take_over). They are QUIC packets,
/// readable in the clear only as far as their headers go, and need no
/// secrecy.
pub struct Handoff {
    pub(crate) socket: std::net::UdpSocket,
    pub(crate) local_addr: SocketAddr,
    pub(crate) held: Vec<HeldDatagram>,
}

impl Handoff {
    /// How many datagrams are held for the successor to send.
    pub fn held(&self) -> usize {
        self.held.len()
    }

    /// The address the socket is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The socket, and the held datagrams as bytes for
    /// [`Binding::take_over`](crate::Binding::take_over).
    pub fn into_parts(self) -> (std::net::UdpSocket, Vec<u8>) {
        let bytes = encode(self.local_addr, &self.held);
        (self.socket, bytes)
    }
}

impl fmt::Debug for Handoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handoff")
            .field("local_addr", &self.local_addr)
            .field("held", &self.held.len())
            .finish_non_exhaustive()
    }
}

/// Encodes a handoff: `b"WDHO"`, version `1`, the local address, a `u32`
/// count and per datagram its destination, its source address and its
/// `u16`-length-prefixed bytes. Integers are big-endian.
pub(crate) fn encode(local_addr: SocketAddr, held: &[HeldDatagram]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        MAGIC.len()
            + 1
            + 19
            + 4
            + held
                .iter()
                .map(|d| 19 + 17 + 2 + d.contents.len())
                .sum::<usize>(),
    );
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    put_addr(&mut out, local_addr);
    out.extend_from_slice(&(held.len() as u32).to_be_bytes());
    for datagram in held {
        put_addr(&mut out, datagram.destination);
        match datagram.src_ip {
            None => out.push(TAG_NONE),
            Some(ip) => put_ip(&mut out, ip),
        }
        // A UDP datagram is at most 65535 bytes, so its length fits.
        out.extend_from_slice(&(datagram.contents.len() as u16).to_be_bytes());
        out.extend_from_slice(&datagram.contents);
    }
    out
}

/// Decodes what [`encode`] wrote, refusing anything else with
/// `Error::Io` of kind `InvalidData`.
pub(crate) fn decode(bytes: &[u8]) -> Result<(SocketAddr, Vec<HeldDatagram>), Error> {
    let Some(rest) = bytes.strip_prefix(MAGIC.as_slice()) else {
        return Err(invalid("handoff: not a weida handoff".into()));
    };
    let mut reader = Reader(rest);
    let version = reader.u8()?;
    if version != VERSION {
        return Err(invalid(format!("handoff: unknown version {version}")));
    }
    let local_addr = reader.addr()?;
    let count = reader.u32()?;
    // The count is not trusted for an allocation: entries are pushed as they
    // parse, so a lying count fails on truncation instead.
    let mut held = Vec::new();
    for _ in 0..count {
        let destination = reader.addr()?;
        let src_ip = match reader.u8()? {
            TAG_NONE => None,
            tag => Some(reader.ip(tag)?),
        };
        let len = usize::from(reader.u16()?);
        if len == 0 {
            return Err(malformed());
        }
        let contents = reader.take(len)?.to_vec();
        held.push(HeldDatagram {
            destination,
            src_ip,
            contents,
        });
    }
    if !reader.0.is_empty() {
        return Err(malformed());
    }
    Ok((local_addr, held))
}

fn put_addr(out: &mut Vec<u8>, addr: SocketAddr) {
    put_ip(out, addr.ip());
    out.extend_from_slice(&addr.port().to_be_bytes());
}

fn put_ip(out: &mut Vec<u8>, ip: IpAddr) {
    match ip {
        IpAddr::V4(v4) => {
            out.push(TAG_V4);
            out.extend_from_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            out.push(TAG_V6);
            out.extend_from_slice(&v6.octets());
        }
    }
}

fn invalid(reason: String) -> Error {
    Error::Io(io::Error::new(io::ErrorKind::InvalidData, reason))
}

fn malformed() -> Error {
    invalid("handoff: malformed".into())
}

/// A cursor over the encoded bytes; every short read is `malformed`.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.0.len() < n {
            return Err(malformed());
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn ip(&mut self, tag: u8) -> Result<IpAddr, Error> {
        match tag {
            TAG_V4 => Ok(IpAddr::V4(Ipv4Addr::from(self.array::<4>()?))),
            TAG_V6 => Ok(IpAddr::V6(Ipv6Addr::from(self.array::<16>()?))),
            _ => Err(malformed()),
        }
    }

    fn addr(&mut self) -> Result<SocketAddr, Error> {
        let tag = self.u8()?;
        let ip = self.ip(tag)?;
        let port = self.u16()?;
        Ok(SocketAddr::new(ip, port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> (SocketAddr, Vec<HeldDatagram>) {
        let local: SocketAddr = "127.0.0.1:4433".parse().unwrap();
        let held = vec![
            HeldDatagram {
                destination: "10.0.0.7:50000".parse().unwrap(),
                src_ip: None,
                contents: vec![0x40, 1, 2, 3],
            },
            HeldDatagram {
                destination: "[2001:db8::7]:50001".parse().unwrap(),
                src_ip: Some("2001:db8::1".parse().unwrap()),
                contents: vec![0x41; 1200],
            },
        ];
        (local, held)
    }

    fn assert_invalid_data(bytes: &[u8]) {
        match decode(bytes) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{bytes:?}"),
            other => panic!("{bytes:?} decoded as {other:?}"),
        }
    }

    #[test]
    fn a_handoff_survives_its_encoding() {
        let (local, held) = sample();
        assert_eq!(decode(&encode(local, &held)).unwrap(), (local, held));

        let v6: SocketAddr = "[::]:9".parse().unwrap();
        assert_eq!(decode(&encode(v6, &[])).unwrap(), (v6, Vec::new()));
    }

    #[test]
    fn decode_refuses_what_encode_never_writes() {
        let (local, held) = sample();
        let valid = encode(local, &held);

        let mut magic = valid.clone();
        magic[0] = b'X';
        assert_invalid_data(&magic);

        let mut version = valid.clone();
        version[MAGIC.len()] = 2;
        match decode(&version) {
            Err(Error::Io(e)) => assert!(e.to_string().contains("unknown version 2"), "{e}"),
            other => panic!("version 2 decoded as {other:?}"),
        }

        for end in 0..valid.len() {
            assert_invalid_data(&valid[..end]);
        }

        let mut trailing = valid.clone();
        trailing.push(0);
        assert_invalid_data(&trailing);

        let empty = HeldDatagram {
            destination: "10.0.0.7:50000".parse().unwrap(),
            src_ip: None,
            contents: Vec::new(),
        };
        assert_invalid_data(&encode(local, &[empty]));
    }
}
