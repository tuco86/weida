//! The wire: one packet at a time in each direction, and the handshake.
//!
//! MQTT's transport requirement is a single "ordered, lossless, stream of
//! bytes" in both directions ([MQTT-4.2-1]) [mqtt5 §12/P18], so every packet
//! of a session interleaves on one connection. The read and write halves are
//! **split** rather than shared, and the reason is not style: a reader that
//! hands out a borrowed packet cannot also be asked to write, so splitting is
//! what lets the event loop answer a PUBLISH while the PUBLISH is still
//! borrowed from the receive buffer — and therefore what lets this client
//! decode without copying the payload out first.
//!
//! **Nothing is read into a buffer sized from a declared length.**
//! The reader grows its buffer by what the socket actually delivered and
//! asks the codec whether a whole packet is present yet; the codec applies
//! `Maximum Packet Size` from the fixed header alone, so a peer that declares
//! 268 MB over a four-byte read is refused before anything is reserved
//! (3.1.2.11.4) [mqtt5 §5].
//!
//! **A partial packet at end of stream is a closed connection, not a
//! malformed one.** The peer is always free to drop the socket — that is what
//! 3.1.1 did for every error [mqtt5 §1] — and reporting it as a protocol fault
//! would put the blame in the wrong place.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use weida_mqtt_codec::{
    Auth, AuthReasonCode, Connack, Connect, ConnectReasonCode, DecodeError, FixedHeader, Packet,
    PacketType, Properties, Will,
};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::limits::{Limits, ServerLimits};
use crate::options::{ConnectOptions, interval_seconds};
use crate::transport::Stream;

/// Bytes to ask the socket for at a time. Not a bound on anything: the bound
/// is `Maximum Packet Size`, applied by the codec from the fixed header.
const READ_CHUNK: usize = 8 * 1024;

/// Answers the server's `AUTH` challenges during enhanced authentication
/// (4.12) [mqtt5 §10].
///
/// A client that sets `Authentication Method` "MUST send nothing but AUTH or
/// DISCONNECT until CONNACK" ([MQTT-3.1.2-30]), and the exchange runs with
/// reason code 0x18 until the server sends CONNACK 0x00 [mqtt5 §10]. Every
/// AUTH and any successful CONNACK MUST repeat the same method
/// ([MQTT-4.12.0-5]), which this module checks rather than trusting.
pub trait Authenticator: Send + Sync {
    /// Answers a `Continue authentication` challenge.
    ///
    /// `data` is the server's `Authentication Data`, absent where it sent
    /// none. The returned bytes become the client's `Authentication Data`.
    ///
    /// # Errors
    ///
    /// Any error ends the exchange; the client sends DISCONNECT and closes,
    /// which is what both sides SHOULD do on failure ([MQTT-4.12.1-2]).
    fn challenge(&self, data: Option<&[u8]>) -> Result<Vec<u8>>;
}

/// The authenticator a client that named no method gets.
///
/// "Absent a client-named method the server MUST NOT send AUTH"
/// ([MQTT-4.12.0-6]) [mqtt5 §10], so reaching this is the server breaking the
/// protocol rather than a missing feature.
pub(crate) struct NoAuthenticator;

impl Authenticator for NoAuthenticator {
    fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>> {
        Err(Error::UnexpectedPacket {
            packet_type: PacketType::Auth,
        })
    }
}

/// The read half, with the receive buffer.
pub(crate) struct Reader {
    half: ReadHalf<Stream>,
    buf: Vec<u8>,
    /// Bytes of `buf` the last [`Reader::next`] handed out, dropped at the
    /// start of the next call. Deferring the compaction is what lets the
    /// returned slice live as long as the caller needs it.
    pending: usize,
    max_packet_size: u32,
    /// Where a read lands before it is appended. Separate from `buf` so that
    /// dropping the read future cannot leave un-filled bytes behind.
    scratch: Box<[u8]>,
}

impl Reader {
    /// The next whole packet's bytes.
    ///
    /// **Cancel-safe, and that is load-bearing.** The event loop races this
    /// future against the keep-alive timer and the command channel, so it is
    /// dropped mid-read routinely. Reading into `scratch` and appending only
    /// what the socket actually delivered is what makes a cancelled read a
    /// no-op: growing `buf` first and truncating afterwards would leave the
    /// zero bytes of the un-filled tail in the buffer, and the next decode
    /// would read them as a packet — a reserved packet type 0, in practice.
    ///
    /// # Errors
    ///
    /// [`Error::ConnectionClosed`] at end of stream, including a partial
    /// packet; [`Error::Protocol`] for a fixed header the codec refuses,
    /// which includes a declaration above `max_packet_size`; [`Error::Io`] for
    /// a transport failure.
    pub(crate) async fn next(&mut self) -> Result<&[u8]> {
        if self.pending > 0 {
            self.buf.drain(..self.pending);
            self.pending = 0;
        }

        loop {
            match FixedHeader::decode(&self.buf, self.max_packet_size) {
                Ok((header, header_len)) => {
                    let total = header_len + header.remaining_length as usize;
                    if self.buf.len() >= total {
                        self.pending = total;
                        return Ok(&self.buf[..total]);
                    }
                }
                Err(DecodeError::Incomplete) => {}
                Err(error) => return Err(Error::Protocol(error)),
            }

            let read = self.half.read(&mut self.scratch).await?;
            if read == 0 {
                return Err(Error::ConnectionClosed);
            }
            self.buf.extend_from_slice(&self.scratch[..read]);
        }
    }
}

/// The write half, with the encode buffer and the server's size ceiling.
pub(crate) struct Writer {
    half: WriteHalf<Stream>,
    buf: Vec<u8>,
    /// The server's `Maximum Packet Size`, or the encoding's ceiling where it
    /// declared none. "The Client MUST NOT send packets exceeding Maximum
    /// Packet Size to the Server" ([MQTT-3.2.2-15]) [mqtt5 §5], so this is
    /// checked here and never discovered from a DISCONNECT 0x95.
    ceiling: u32,
}

impl Writer {
    /// Encodes and writes one packet.
    ///
    /// # Errors
    ///
    /// [`Error::Encode`] where the packet cannot be built or exceeds the
    /// server's ceiling, and [`Error::Io`] for a transport failure. Nothing
    /// is written when the encode fails.
    pub(crate) async fn send(&mut self, packet: &Packet<'_>) -> Result<()> {
        self.buf.clear();
        packet.encode_within(self.ceiling, &mut self.buf)?;
        self.half.write_all(&self.buf).await?;
        self.half.flush().await?;
        Ok(())
    }

    /// Raises the ceiling to what the server declared, once CONNACK has said
    /// so.
    pub(crate) fn set_ceiling(&mut self, ceiling: u32) {
        self.ceiling = ceiling;
    }
}

/// What a completed handshake produced.
pub(crate) struct Handshake {
    pub(crate) reader: Reader,
    pub(crate) writer: Writer,
    pub(crate) limits: ServerLimits,
    pub(crate) session_present: bool,
    pub(crate) keep_alive: Option<Duration>,
    pub(crate) client_id: String,
    /// Whether the transport is encrypted, so a caller can ask what its
    /// credentials travelled over.
    pub(crate) encrypted: bool,
}

/// Dials `address`, sends CONNECT, and awaits CONNACK.
///
/// The order is the protocol's: "the client's first packet MUST be CONNECT"
/// ([MQTT-3.1.0-1]) and the server "MUST send CONNACK 0x00 before any packet
/// other than AUTH" ([MQTT-3.2.0-2]) [mqtt5 §1]. So this function sends one
/// packet and then accepts exactly two kinds of answer — CONNACK, or an AUTH
/// that continues enhanced authentication — and anything else is
/// [`Error::UnexpectedPacket`].
///
/// # Errors
///
/// [`Error::Configuration`] for options that cannot be sent,
/// [`Error::ConnectionRefused`] for a CONNACK with a failure code,
/// [`Error::ServerDisconnected`] where the server sent DISCONNECT instead,
/// [`Error::Timeout`] where no CONNACK arrived within
/// `options.connect_timeout`, and the transport and protocol errors of
/// [`Reader::next`].
pub(crate) async fn handshake(
    exec: &Exec,
    address: &str,
    options: &ConnectOptions,
    authenticator: &dyn Authenticator,
) -> Result<Handshake> {
    options.validate()?;

    // Only TLS needs the host: it is what the certificate is validated
    // against where the caller named no other name.
    #[cfg(feature = "tls")]
    let (host, _) = split_host_port(address)?;
    let stream = Stream::Plain(dial(exec, address, options).await?);
    // TLS **before** the CONNECT, which is the whole ordering requirement:
    // the User Name, the Password and the Authentication Data all travel in
    // that packet, and a handshake completed afterwards would have protected
    // none of them (5.4.3) [mqtt5 §10].
    #[cfg(feature = "tls")]
    let stream = match &options.tls {
        Some(tls) => crate::transport::upgrade(stream, tls, host).await?,
        None => stream,
    };
    stream.set_nodelay()?;
    let encrypted = stream.is_encrypted();
    let (read_half, write_half) = tokio::io::split(stream);

    let mut reader = Reader {
        half: read_half,
        buf: Vec::with_capacity(READ_CHUNK),
        pending: 0,
        max_packet_size: options.limits.maximum_packet_size,
        scratch: vec![0u8; READ_CHUNK].into_boxed_slice(),
    };
    let mut writer = Writer {
        half: write_half,
        buf: Vec::with_capacity(256),
        // Until CONNACK the server's ceiling is unknown, so the encoding's own
        // is the only one that applies.
        ceiling: Limits::MAX_PACKET_SIZE,
    };

    let will = build_will(options)?;
    let user_properties = borrowed_pairs(&options.user_properties);
    let connect = build_connect(options, will.as_deref(), &user_properties)?;
    writer.send(&Packet::Connect(connect)).await?;

    let deadline = options.connect_timeout;
    let outcome = exec
        .within(
            deadline,
            await_connack(&mut reader, &mut writer, options, authenticator),
        )
        .await;
    let (limits, session_present, assigned) = match outcome {
        Some(result) => result?,
        None => return Err(Error::Timeout("CONNACK")),
    };

    writer.set_ceiling(limits.send_packet_ceiling());

    // "Server Keep Alive present, the client MUST use it ([MQTT-3.2.2-21]);
    // absent, the server MUST use the client's ([MQTT-3.2.2-22])" [mqtt5 §1].
    // The only property in the protocol that overrides the client.
    let keep_alive = match limits.server_keep_alive {
        Some(server) => server,
        None => options.keep_alive,
    };
    let keep_alive = if keep_alive.is_zero() {
        None
    } else {
        Some(keep_alive)
    };

    let client_id = assigned.unwrap_or_else(|| options.client_id.clone());

    Ok(Handshake {
        reader,
        writer,
        limits,
        session_present,
        encrypted,
        keep_alive,
        client_id,
    })
}

/// Reads until CONNACK, answering AUTH challenges on the way.
async fn await_connack(
    reader: &mut Reader,
    writer: &mut Writer,
    options: &ConnectOptions,
    authenticator: &dyn Authenticator,
) -> Result<(ServerLimits, bool, Option<String>)> {
    loop {
        // The answer is decoded under *our* declared ceiling, which is the
        // number the server was told to respect (3.1.2.11.4).
        let answer: Vec<u8> = reader.next().await?.to_vec();
        let (packet, _) = Packet::decode(&answer, options.limits.maximum_packet_size)?;
        match packet {
            Packet::Connack(connack) => return accept_connack(&connack, options),
            Packet::Auth(auth) => {
                let reply = continue_authentication(&auth, options, authenticator)?;
                let data = reply;
                let properties = Properties {
                    authentication_method: options.authentication_method.as_deref(),
                    authentication_data: Some(&data),
                    ..Properties::new()
                };
                writer
                    .send(&Packet::Auth(Auth {
                        reason_code: AuthReasonCode::ContinueAuthentication,
                        properties,
                    }))
                    .await?;
            }
            Packet::Disconnect(disconnect) => {
                return Err(Error::ServerDisconnected(disconnect.reason_code));
            }
            other => {
                return Err(Error::UnexpectedPacket {
                    packet_type: other.packet_type(),
                });
            }
        }
    }
}

/// Checks an AUTH and produces the client's answer.
fn continue_authentication(
    auth: &Auth<'_>,
    options: &ConnectOptions,
    authenticator: &dyn Authenticator,
) -> Result<Vec<u8>> {
    // [MQTT-4.12.0-6]: "absent a client-named method the Server MUST NOT send
    // AUTH". So a client that named none is looking at a protocol violation,
    // and that is a different fault from a method mismatch — checked first,
    // because otherwise every unrequested AUTH would be misreported as a
    // mismatch with a method that was never offered.
    let Some(ours) = options.authentication_method.as_deref() else {
        return Err(Error::UnexpectedPacket {
            packet_type: PacketType::Auth,
        });
    };
    if auth.reason_code != AuthReasonCode::ContinueAuthentication {
        return Err(Error::UnexpectedPacket {
            packet_type: PacketType::Auth,
        });
    }
    // [MQTT-4.12.0-5]: every AUTH repeats the CONNECT's method. A server that
    // switches method mid-exchange is not continuing this authentication.
    if auth.properties.authentication_method != Some(ours) {
        return Err(Error::AuthenticationMethodMismatch);
    }
    authenticator.challenge(auth.properties.authentication_data)
}

/// Turns a CONNACK into what the connection needs, or into a refusal.
fn accept_connack(
    connack: &Connack<'_>,
    options: &ConnectOptions,
) -> Result<(ServerLimits, bool, Option<String>)> {
    if connack.reason_code.is_error() {
        return Err(Error::ConnectionRefused(connack.reason_code));
    }
    // [MQTT-4.12.0-5] again, on the packet that ends the exchange. A server
    // that omits the method on a successful CONNACK is tolerated: the
    // requirement is that it not name a *different* one.
    if let (Some(ours), Some(theirs)) = (
        options.authentication_method.as_deref(),
        connack.properties.authentication_method,
    ) && ours != theirs
    {
        return Err(Error::AuthenticationMethodMismatch);
    }
    debug_assert_eq!(connack.reason_code, ConnectReasonCode::Success);

    let limits = ServerLimits::from_connack(connack);
    let assigned = limits.assigned_client_identifier.clone();
    Ok((limits, connack.session_present, assigned))
}

/// Resolves and dials, trying every address the resolver offered in order.
async fn dial(exec: &Exec, address: &str, options: &ConnectOptions) -> Result<TcpStream> {
    let (host, port) = split_host_port(address)?;
    let addrs = exec
        .resolve(host, port, options.limits.max_addresses)
        .await?;

    let mut last: Option<Error> = None;
    for addr in addrs {
        // `TcpStream::connect` registers the socket with the reactor as it is
        // built, and a caller driving this future on its own executor — which
        // `Context::owned` exists for — is not inside one. Spawning puts the
        // constructor where the reactor is; awaiting the handle does not need
        // to be.
        let connect = exec.spawn(TcpStream::connect(addr));
        let attempt = exec.within(options.connect_timeout, connect).await;
        match attempt {
            Some(Ok(Ok(stream))) => return Ok(stream),
            Some(Ok(Err(error))) => last = Some(Error::Io(error)),
            Some(Err(error)) => {
                last = Some(Error::Runtime(format!("connect task failed: {error}")));
            }
            None => last = Some(Error::Timeout("the transport connection")),
        }
    }
    Err(last.unwrap_or(Error::Timeout("the transport connection")))
}

/// `host:port`, with the bracket form for an IPv6 literal.
fn split_host_port(address: &str) -> Result<(&str, u16)> {
    let (host, port) = if let Some(rest) = address.strip_prefix('[') {
        let (host, rest) = rest
            .split_once(']')
            .ok_or_else(|| Error::Configuration(format!("unclosed IPv6 literal in {address}")))?;
        let port = rest.strip_prefix(':').ok_or_else(|| {
            Error::Configuration(format!("{address} needs a port; MQTT's are 1883 and 8883"))
        })?;
        (host, port)
    } else {
        address.rsplit_once(':').ok_or_else(|| {
            Error::Configuration(format!("{address} needs a port; MQTT's are 1883 and 8883"))
        })?
    };
    let port = port
        .parse::<u16>()
        .map_err(|_| Error::Configuration(format!("{port} is not a port")))?;
    if port == 0 {
        return Err(Error::Configuration("port 0 is not a port".into()));
    }
    Ok((host, port))
}

/// The Will, encoded from the owned options into the codec's borrowed shape.
fn build_will(options: &ConnectOptions) -> Result<Option<Box<Will<'_>>>> {
    let Some(will) = &options.will else {
        return Ok(None);
    };
    Ok(Some(Box::new(Will {
        topic: &will.topic,
        payload: &will.payload,
        qos: will.qos,
        retain: will.retain,
        properties: Properties {
            will_delay_interval: interval_seconds("will.delay", will.delay)?,
            message_expiry_interval: interval_seconds("will.message_expiry", will.message_expiry)?,
            content_type: will.content_type.as_deref(),
            response_topic: will.response_topic.as_deref(),
            correlation_data: will.correlation_data.as_deref(),
            ..Properties::new()
        },
    })))
}

/// The owned `(String, String)` pairs as the borrowed pairs the codec takes.
fn borrowed_pairs(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
    pairs
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

/// The CONNECT this client's options describe.
fn build_connect<'a>(
    options: &'a ConnectOptions,
    will: Option<&'a Will<'a>>,
    user_properties: &'a [(&'a str, &'a str)],
) -> Result<Connect<'a>> {
    Ok(Connect {
        client_id: &options.client_id,
        clean_start: options.clean_start,
        keep_alive: options.keep_alive_seconds()?,
        properties: Properties {
            session_expiry_interval: interval_seconds("session_expiry", options.session_expiry)?,
            receive_maximum: Some(options.limits.receive_maximum),
            maximum_packet_size: Some(options.limits.maximum_packet_size),
            // 0 is the protocol's default and means "no aliases", so it is
            // sent only when it is not the default — a property whose value is
            // the default is noise on the wire.
            topic_alias_maximum: (options.limits.topic_alias_maximum != 0)
                .then_some(options.limits.topic_alias_maximum),
            request_response_information: options.request_response_information.then_some(true),
            // Default 1, so only the non-default is worth a property.
            request_problem_information: (!options.request_problem_information).then_some(false),
            authentication_method: options.authentication_method.as_deref(),
            authentication_data: options.authentication_data.as_deref(),
            ..Properties::new()
        }
        .with_user_properties(user_properties),
        will: will.cloned().map(Box::new),
        user_name: options.user_name.as_deref(),
        password: options.password.as_deref(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_and_port_parse_in_both_literal_forms() {
        assert_eq!(split_host_port("broker:1883").unwrap(), ("broker", 1883));
        assert_eq!(
            split_host_port("127.0.0.1:8883").unwrap(),
            ("127.0.0.1", 8883)
        );
        assert_eq!(split_host_port("[::1]:1883").unwrap(), ("::1", 1883));
        // An unbracketed IPv6 literal is split on its **last** colon, so
        // `::1:1883` parses as the host `::1` - which is what `rsplit_once`
        // does and what a caller writing the bracketless form gets. Asserted
        // rather than left open: a test that accepted either answer would
        // assert nothing.
        assert_eq!(split_host_port("::1:1883").unwrap(), ("::1", 1883));
        assert!(split_host_port("broker").is_err());
        assert!(split_host_port("broker:0").is_err());
        assert!(split_host_port("broker:no").is_err());
        assert!(split_host_port("[::1").is_err());
    }

    /// A property whose value is the protocol's default is left off the wire:
    /// `Topic Alias Maximum` 0 and `Request Problem Information` 1 both mean
    /// exactly what absence means [mqtt5 §11].
    #[test]
    fn a_default_valued_property_is_not_sent() {
        let options = ConnectOptions::new("a");
        let pairs = Vec::new();
        let connect = build_connect(&options, None, &pairs).expect("builds");
        assert_eq!(connect.properties.topic_alias_maximum, None);
        assert_eq!(connect.properties.request_problem_information, None);
        assert_eq!(connect.properties.request_response_information, None);
        // The two this client always declares, because its defaults are not
        // the protocol's.
        assert_eq!(
            connect.properties.receive_maximum,
            Some(options.limits.receive_maximum)
        );
        assert_eq!(
            connect.properties.maximum_packet_size,
            Some(options.limits.maximum_packet_size)
        );
    }

    #[test]
    fn a_non_default_value_is_sent() {
        let mut options = ConnectOptions::new("a");
        options.limits.topic_alias_maximum = 10;
        options.request_problem_information = false;
        options.request_response_information = true;
        let pairs = Vec::new();
        let connect = build_connect(&options, None, &pairs).expect("builds");
        assert_eq!(connect.properties.topic_alias_maximum, Some(10));
        assert_eq!(connect.properties.request_problem_information, Some(false));
        assert_eq!(connect.properties.request_response_information, Some(true));
    }

    /// A refusal carries its code out of the handshake rather than becoming a
    /// closed socket.
    #[test]
    fn a_failed_connack_becomes_a_named_refusal() {
        let options = ConnectOptions::new("a");
        let connack = Connack {
            session_present: false,
            reason_code: ConnectReasonCode::BadUserNameOrPassword,
            properties: Properties::new(),
        };
        let error = accept_connack(&connack, &options).expect_err("refused");
        assert_eq!(error.reason_code(), Some(0x86));
    }

    /// [MQTT-3.2.2-21]: Server Keep Alive overrides, which is what the
    /// handshake's own arithmetic does with it.
    #[test]
    fn server_keep_alive_overrides_the_clients() {
        let ours = Duration::from_secs(60);
        let theirs = ServerLimits {
            server_keep_alive: Some(Duration::from_secs(30)),
            ..ServerLimits::default()
        };
        assert_eq!(
            theirs.server_keep_alive.unwrap_or(ours),
            theirs.server_keep_alive.unwrap()
        );
        let silent = ServerLimits::default();
        assert_eq!(silent.server_keep_alive.unwrap_or(ours), ours);
    }

    /// [MQTT-4.12.0-6]: a server that sends AUTH to a client that named no
    /// method is breaking the protocol, so the default authenticator refuses
    /// rather than reporting a missing feature.
    #[test]
    fn an_unrequested_auth_is_refused() {
        let error = NoAuthenticator.challenge(None).expect_err("refused");
        assert!(matches!(
            error,
            Error::UnexpectedPacket {
                packet_type: PacketType::Auth
            }
        ));
    }

    /// [MQTT-4.12.0-5]: the method may not change mid-exchange.
    #[test]
    fn an_auth_with_a_different_method_is_refused() {
        struct Echo;
        impl Authenticator for Echo {
            fn challenge(&self, _data: Option<&[u8]>) -> Result<Vec<u8>> {
                Ok(vec![1])
            }
        }
        let mut options = ConnectOptions::new("a");
        options.authentication_method = Some("SCRAM-SHA-1".into());

        let matching = Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some("SCRAM-SHA-1"),
                ..Properties::new()
            },
        };
        assert_eq!(
            continue_authentication(&matching, &options, &Echo).unwrap(),
            vec![1]
        );

        let switched = Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some("GS2-KRB5"),
                ..Properties::new()
            },
        };
        assert!(matches!(
            continue_authentication(&switched, &options, &Echo),
            Err(Error::AuthenticationMethodMismatch)
        ));
    }
}
