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

/// The most datagrams a handoff holds and a successor accepts. A handover
/// holds a close packet per connection, and `Limits::max_connections`
/// defaults to 1024 per binding, so this leaves room for the packets of
/// handshakes still in flight; a handoff past it is not one this process
/// made.
pub(crate) const MAX_HELD_DATAGRAMS: usize = 4096;
/// The largest datagram a handoff holds and a successor accepts: `quinn`'s
/// largest UDP payload under its default MTU discovery (1452 bytes), with
/// headroom.
pub(crate) const MAX_HELD_DATAGRAM_BYTES: usize = 1500;

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
                    // Only what `decode` would accept: a datagram it refuses
                    // would make the successor refuse the whole handoff, and
                    // every client with it. The count is bounded for the same
                    // reason and so that a handover cannot pin memory.
                    if held.len() < MAX_HELD_DATAGRAMS
                        && plausible(transmit.destination, transmit.src_ip, contents)
                    {
                        held.push(HeldDatagram {
                            destination: transmit.destination,
                            src_ip: transmit.src_ip,
                            contents: contents.to_vec(),
                        });
                    }
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
/// The bytes go alongside it to
/// [`Binding::take_over`](crate::Binding::take_over). They are QUIC packets,
/// readable in the clear only as far as their headers go, and need no
/// secrecy.
///
/// **They do need the integrity of their channel.** Whoever can write them
/// decides what the successor sends from the binding's UDP port, and to whom:
/// `take_over` sends each datagram it is given, once. That is bounded and not
/// harmless: it refuses more than 4096 datagrams, one larger than 1500
/// bytes, and a destination that is multicast, broadcast, unspecified or
/// port 0, none of which a client has — but within those bounds it sends
/// what it is told. The successor cannot check the destinations against
/// connections, because it has none: they were the predecessor's. So carry
/// the bytes over the channel that carries the socket descriptor, which only
/// the two processes' own user can reach, and not through a file or socket
/// other users can write.
///
/// A binding that holds more than 4096 datagrams keeps the first ones: the
/// clients whose close did not fit notice through `Limits::idle_timeout` and
/// redial on their normal schedule.
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
///
/// "Anything else" includes what no handover can have produced: more than
/// [`MAX_HELD_DATAGRAMS`] datagrams, and a datagram [`plausible`] refuses.
/// The bytes are trusted by the caller that passes them, and still bounded
/// here, because the bound is cheap and what it protects is not.
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
    if !usize::try_from(count).is_ok_and(|count| count <= MAX_HELD_DATAGRAMS) {
        return Err(invalid(format!(
            "handoff: {count} datagrams, a handover holds at most {MAX_HELD_DATAGRAMS}"
        )));
    }
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
        // Before the copy: a length no datagram has is refused unread.
        if len > MAX_HELD_DATAGRAM_BYTES {
            return Err(malformed());
        }
        let contents = reader.take(len)?.to_vec();
        if !plausible(destination, src_ip, &contents) {
            return Err(malformed());
        }
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

/// Whether a handover could have held this datagram.
///
/// A QUIC client has a unicast address with a port, so a destination that is
/// multicast, broadcast, unspecified or port 0 is no client's; the source is
/// a local unicast address when there is one; and the contents are one
/// packet of at most [`MAX_HELD_DATAGRAM_BYTES`]. The first byte is not
/// looked at: a connection whose peer offers the `grease_quic_bit` transport
/// parameter (RFC 9287, which `quinn` sends by default) carries packets whose
/// fixed bit is zero at random, so no first byte is a safe test for a QUIC
/// packet, and one that refused such a close would lose a client's
/// restart notice at random. This is a bound on what a successor may be told
/// to send, not a validation of the packet.
fn plausible(destination: SocketAddr, src_ip: Option<IpAddr>, contents: &[u8]) -> bool {
    (1..=MAX_HELD_DATAGRAM_BYTES).contains(&contents.len())
        && unicast(destination.ip())
        && destination.port() != 0
        && src_ip.is_none_or(unicast)
}

/// Whether `ip` could be a single client's address. An IPv4-mapped IPv6
/// address is judged as the IPv4 address it names, because a dual-stack
/// socket, which is what a QUIC binding on `[::]` is, sends it as that.
fn unicast(ip: IpAddr) -> bool {
    let ip = ip.to_canonical();
    !(ip.is_multicast() || ip.is_unspecified() || ip == IpAddr::V4(Ipv4Addr::BROADCAST))
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

    fn held_datagram(destination: &str, contents: Vec<u8>) -> HeldDatagram {
        HeldDatagram {
            destination: destination.parse().unwrap(),
            src_ip: None,
            contents,
        }
    }

    #[test]
    fn decode_refuses_more_datagrams_than_a_handover_holds() {
        let (local, _) = sample();
        let one = held_datagram("10.0.0.7:50000", vec![0x40]);

        let at_the_cap = vec![one.clone(); MAX_HELD_DATAGRAMS];
        let (_, decoded) = decode(&encode(local, &at_the_cap)).unwrap();
        assert_eq!(decoded.len(), MAX_HELD_DATAGRAMS);

        let past_the_cap = vec![one; MAX_HELD_DATAGRAMS + 1];
        assert_invalid_data(&encode(local, &past_the_cap));
    }

    #[test]
    fn decode_refuses_a_destination_no_client_has() {
        let (local, _) = sample();
        for destination in [
            "224.0.0.1:5000",
            "239.255.255.250:1900",
            "255.255.255.255:5000",
            "0.0.0.0:5000",
            "10.0.0.7:0",
            "[ff02::1]:5000",
            "[::]:5000",
            "[::ffff:224.0.0.1]:5000",
            "[::ffff:255.255.255.255]:5000",
            "[::ffff:0.0.0.0]:5000",
        ] {
            let held = held_datagram(destination, vec![0x40, 1, 2, 3]);
            assert_invalid_data(&encode(local, &[held]));
        }
        for source in [
            "224.0.0.1",
            "0.0.0.0",
            "ff02::1",
            "::ffff:224.0.0.1",
            "::ffff:255.255.255.255",
            "::ffff:0.0.0.0",
        ] {
            let mut held = held_datagram("10.0.0.7:5000", vec![0x40, 1, 2, 3]);
            held.src_ip = Some(source.parse().unwrap());
            assert_invalid_data(&encode(local, &[held]));
        }
        // A dual-stack client's own address is an ordinary unicast one.
        let mapped = held_datagram("[::ffff:10.0.0.7]:5000", vec![0x40, 1, 2, 3]);
        assert!(decode(&encode(local, &[mapped])).is_ok());
    }

    #[test]
    fn decode_refuses_a_datagram_larger_than_one_packet() {
        let (local, _) = sample();
        let accepted = |contents: Vec<u8>| {
            let held = held_datagram("10.0.0.7:5000", contents);
            decode(&encode(local, &[held])).is_ok()
        };
        // The first byte is not a test: a connection that negotiated the
        // grease bit sends packets whose fixed bit is zero at random.
        assert!(accepted(vec![0x00, 1]));
        assert!(accepted(vec![0x1f, 1]));
        assert!(accepted(vec![0xc0, 1]));
        // One packet is at most one MTU.
        let mut at_the_cap = vec![0x40; MAX_HELD_DATAGRAM_BYTES];
        assert!(accepted(at_the_cap.clone()));
        at_the_cap.push(0x40);
        assert!(!accepted(at_the_cap));
    }

    /// A socket that sends nothing and receives nothing, for the decorator
    /// to sit on.
    #[derive(Debug)]
    struct Nowhere;

    impl UdpPoller for Nowhere {
        fn poll_writable(self: Pin<&mut Self>, _: &mut Context) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncUdpSocket for Nowhere {
        fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
            Box::pin(Nowhere)
        }

        fn try_send(&self, _: &Transmit) -> io::Result<()> {
            Ok(())
        }

        fn poll_recv(
            &self,
            _: &mut Context,
            _: &mut [IoSliceMut<'_>],
            _: &mut [RecvMeta],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:4433".parse().unwrap())
        }
    }

    #[test]
    fn a_held_socket_keeps_only_what_a_successor_would_accept() {
        let socket = HandoverSocket::new(Arc::new(Nowhere));
        socket.hold();
        let send = |destination: &str, contents: &[u8], segment_size: Option<usize>| {
            socket
                .try_send(&Transmit {
                    destination: destination.parse().unwrap(),
                    ecn: None,
                    contents,
                    segment_size,
                    src_ip: None,
                })
                .unwrap();
        };
        // Neither of these could be a client's close.
        send("224.0.0.1:9", &[0x40], None);
        send("10.0.0.7:9", &[0x40; MAX_HELD_DATAGRAM_BYTES + 1], None);
        // A packet whose fixed bit is zero is a client's close all the same:
        // `quinn` greases the bit by default (RFC 9287).
        send("10.0.0.7:9", &[0x1f, 9], None);
        // A GSO batch is kept as the datagrams it is made of.
        send("10.0.0.7:9", &[0x40, 1, 2, 3, 0x40, 5], Some(4));
        for _ in 0..MAX_HELD_DATAGRAMS + 10 {
            send("10.0.0.7:9", &[0x40], None);
        }

        let held = socket.release();
        assert_eq!(held.len(), MAX_HELD_DATAGRAMS, "bounded, first ones kept");
        assert_eq!(held[0].contents, [0x1f, 9]);
        assert_eq!(held[1].contents, [0x40, 1, 2, 3]);
        assert_eq!(held[2].contents, [0x40, 5]);
        // What was kept is what a successor accepts.
        let (local, _) = sample();
        assert!(decode(&encode(local, &held)).is_ok());
    }
}
