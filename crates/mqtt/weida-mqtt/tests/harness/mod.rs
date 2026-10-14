//! A scriptable MQTT 5.0 server, built on the codec, for testing the client.
//!
//! It is faithful on the wire and is deliberately **not** an independent
//! implementation: it shares this workspace's codec, so it proves the client's
//! behaviour and not interoperability. Interoperability is what
//! `interop_rumqttd.rs` and `interop_mqtt5.rs` measure, against brokers that
//! share no code with ours.
//!
//! What it is for is the half of the client's behaviour a real broker cannot
//! be made to exhibit on demand: a CONNACK with one particular availability
//! flag clear, a `Server Keep Alive` that overrides, a PINGREQ deliberately
//! left unanswered, a DISCONNECT with a chosen reason code, a connection
//! dropped mid-exchange so a session has something to resume.
//!
//! It records the **bytes** of everything the client sent, not just the packet
//! types, because some of what the client must get right is inside the packet:
//! a retransmission carries DUP 1 and its original Packet Identifier
//! ([MQTT-3.3.1-1], [MQTT-4.6.0-1]), and asserting that needs the packet.

// The harness is compiled separately into every integration-test binary, and
// no single one of them uses all of it: `connection.rs` never looks inside a
// packet, `session.rs` never needs an idle step. Without this each binary
// reports the other's half as dead.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use weida_mqtt::ConnectOptions;
use weida_mqtt_codec::{
    Auth, AuthReasonCode, Connack, ConnectReasonCode, FixedHeader, Packet, PacketType, PayloadList,
    Properties, Puback, PubackReasonCode, Pubcomp, PubcompReasonCode, Pubrec, QoS, Suback,
    SubackReasonCode, Unsuback, UnsubackReasonCode, varint,
};

/// The ceiling every harness decode is done under: the largest packet the
/// encoding can express (2.1.4). The harness is not the thing under test, so
/// it declines nothing.
const CAP: u32 = varint::MAX + 5;

/// What the scripted server does after the CONNECT arrives.
///
/// The acknowledging acts exist so a test can express a **lost packet**: the
/// four packets of a QoS 2 handshake, each in turn read and then not answered,
/// are the four cases [mqtt5 §6]'s figure 4.3 can be interrupted at. `Expect`
/// is "received and not answered", which is what a lost acknowledgement looks
/// like from the client's side.
#[derive(Clone, Debug)]
pub enum Act {
    /// Send this packet's bytes.
    Send(Vec<u8>),
    /// Read one packet and record it, answering nothing.
    Expect,
    /// Read one PUBLISH and answer it: PUBACK at QoS 1, PUBREC at QoS 2.
    ///
    /// Counts an onward delivery, and applies the receiver's own duplicate
    /// suppression: a QoS 2 Packet Identifier already awaiting its PUBREL is
    /// answered with another PUBREC and **not** counted again, which is what
    /// [MQTT-4.3.3-10] requires of a receiver.
    AckPublish,
    /// Read one PUBLISH, **accept** it — counting the delivery and holding
    /// the QoS 2 identifier — and answer nothing.
    ///
    /// This is a lost PUBREC or PUBACK: the server has the message and the
    /// client does not know it. Indistinguishable from a lost PUBLISH on the
    /// wire, and distinguishable in the outcome, because the resent PUBLISH
    /// is then a repeat and MUST NOT be delivered again ([MQTT-4.3.3-10]).
    AcceptPublish,
    /// Read one PUBREL, **release** the identifier, and answer nothing.
    ///
    /// This is a lost PUBCOMP: the server is finished and the client is not.
    /// The resent PUBREL then draws 0x92, because the identifier is gone.
    AcceptPubrel,
    /// Read one PUBLISH and answer PUBREC — or PUBACK at QoS 1 — with this
    /// failure code, which ends the exchange: the message "MUST NOT be
    /// retransmitted" ([MQTT-4.4.0-2]) [mqtt5 §6].
    RefusePublish(u8),
    /// Read one PUBREL and answer PUBCOMP 0x00.
    CompletePubrel,
    /// Read one PUBREL and answer PUBCOMP 0x92 (Packet Identifier not found),
    /// which is what a server whose PUBCOMP was lost answers the second time:
    /// it has already released the identifier. "Not an error during recovery"
    /// (3.6.2.1) [mqtt5 §6].
    ForgetPubrel,
    /// Read one SUBSCRIBE and answer SUBACK with these codes, in this order.
    ///
    /// The codes are bytes rather than a typed list so a script can name a
    /// count that disagrees with the filters it was sent, which is the
    /// [MQTT-3.9.3-1] fault a client has to report rather than guess at.
    Suback(Vec<u8>),
    /// Read one UNSUBSCRIBE and answer UNSUBACK with these codes.
    Unsuback(Vec<u8>),
    /// Read one AUTH and answer AUTH with this reason code, echoing the
    /// client's `Authentication Data` back so a test can assert the exchange
    /// carried it.
    ///
    /// The method is copied from the client's packet, which is what a
    /// conforming server does ([MQTT-4.12.0-5]); a test that needs the
    /// **wrong** method uses `AnswerAuthAs`.
    AnswerAuth(u8),
    /// The same, answering with this method instead of the client's - the
    /// [MQTT-4.12.0-5] violation a client has to catch.
    AnswerAuthAs(u8, &'static str),
    /// Wait, so a client timer can expire.
    Idle(Duration),
    /// Drop the connection with no DISCONNECT, which is always available to a
    /// server and is what 3.1.1 did for every error [mqtt5 §1.9]. Also how a
    /// test leaves a session with something to resume.
    Close,
}

/// A server that runs one script per connection, in order.
///
/// A reconnect test hands it two scripts: the first connection gets the first,
/// the second the second.
pub struct Server {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
    connects: Arc<AtomicUsize>,
    deliveries: Arc<AtomicUsize>,
    done: mpsc::Receiver<()>,
}

impl Server {
    /// Binds an ephemeral port and runs `script` against the first client.
    pub async fn start(script: Vec<Act>) -> Server {
        Server::start_all(vec![script]).await
    }

    /// The same for several consecutive connections, one script each.
    ///
    /// The CONNECT is read before each script starts, so a script's first
    /// `Send` is the answer to it.
    pub async fn start_all(scripts: Vec<Vec<Act>>) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let connects = Arc::new(AtomicUsize::new(0));
        let deliveries = Arc::new(AtomicUsize::new(0));
        // Shared across connections on purpose: a QoS 2 identifier held when
        // a connection drops is still held by the *session* when the client
        // reconnects, which is what makes the duplicate-suppression half of
        // the lost-packet replays testable.
        let unreleased = Arc::new(Mutex::new(BTreeSet::new()));
        let (finished, done) = mpsc::channel(scripts.len().max(1));

        let shared = Shared {
            seen: Arc::clone(&seen),
            deliveries: Arc::clone(&deliveries),
            unreleased: Arc::clone(&unreleased),
        };
        let task_connects = Arc::clone(&connects);
        tokio::spawn(async move {
            for script in scripts {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                task_connects.fetch_add(1, Ordering::SeqCst);
                run(
                    stream,
                    script,
                    Shared {
                        seen: Arc::clone(&shared.seen),
                        deliveries: Arc::clone(&shared.deliveries),
                        unreleased: Arc::clone(&shared.unreleased),
                    },
                )
                .await;
                if finished.send(()).await.is_err() {
                    return;
                }
            }
        });

        Server {
            address,
            seen,
            connects,
            deliveries,
            done,
        }
    }

    /// The address a client dials.
    pub fn address(&self) -> String {
        self.address.to_string()
    }

    /// The packet types received, in order, across every connection. Each
    /// connection's CONNECT is included.
    pub async fn seen(&self) -> Vec<PacketType> {
        self.seen
            .lock()
            .await
            .iter()
            .map(|bytes| {
                Packet::decode(bytes, CAP)
                    .map(|(packet, _)| packet.packet_type())
                    .unwrap_or_else(|error| panic!("the harness received {error}"))
            })
            .collect()
    }

    /// The bytes of every packet received, in order, for a test that must look
    /// inside one.
    pub async fn seen_bytes(&self) -> Vec<Vec<u8>> {
        self.seen.lock().await.clone()
    }

    /// How many clients have connected.
    pub fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }

    /// Onward deliveries this server accepted, with a repeated QoS 2 Packet
    /// Identifier counted once. The number a lost-packet replay must leave at
    /// exactly one.
    pub fn deliveries(&self) -> usize {
        self.deliveries.load(Ordering::SeqCst)
    }

    /// Waits for one script to finish.
    pub async fn finished(&mut self) {
        let _ = self.done.recv().await;
    }
}

/// What one connection's script shares with the server handle.
struct Shared {
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
    /// Onward deliveries this server accepted, counting a repeated QoS 2
    /// Packet Identifier once — the receiver's own duplicate suppression
    /// ([MQTT-4.3.3-10]).
    deliveries: Arc<AtomicUsize>,
    /// QoS 2 identifiers received and not yet released by a PUBREL, which is
    /// the state that suppression needs.
    unreleased: Arc<Mutex<BTreeSet<u16>>>,
}

async fn run(mut stream: TcpStream, script: Vec<Act>, shared: Shared) {
    // The client's first packet MUST be CONNECT ([MQTT-3.1.0-1]), so reading
    // it unconditionally is not an assumption but the protocol.
    let mut buf = Vec::new();
    match read_packet(&mut stream, &mut buf).await {
        Some(packet) => shared.seen.lock().await.push(packet),
        None => return,
    }

    for act in script {
        match act {
            Act::Send(bytes) => {
                if stream.write_all(&bytes).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
            Act::Expect => match read_packet(&mut stream, &mut buf).await {
                Some(packet) => shared.seen.lock().await.push(packet),
                None => return,
            },
            Act::AckPublish | Act::RefusePublish(_) | Act::AcceptPublish => {
                let Some(raw) = read_packet(&mut stream, &mut buf).await else {
                    return;
                };
                shared.seen.lock().await.push(raw.clone());
                let answer = answer_publish(&raw, &act, &shared).await;
                if let Some(answer) = answer {
                    if stream.write_all(&answer).await.is_err() {
                        return;
                    }
                    let _ = stream.flush().await;
                }
            }
            Act::CompletePubrel | Act::ForgetPubrel | Act::AcceptPubrel => {
                let Some(raw) = read_packet(&mut stream, &mut buf).await else {
                    return;
                };
                shared.seen.lock().await.push(raw.clone());
                let Packet::Pubrel(pubrel) = decode(&raw) else {
                    panic!(
                        "the script expected a PUBREL, got {}",
                        decode(&raw).packet_type()
                    );
                };
                let reason_code = match act {
                    Act::CompletePubrel => {
                        shared.unreleased.lock().await.remove(&pubrel.packet_id);
                        Some(PubcompReasonCode::Success)
                    }
                    // The server already released it, which is the state a
                    // server whose PUBCOMP was lost is in.
                    Act::ForgetPubrel => Some(PubcompReasonCode::PacketIdentifierNotFound),
                    // Released, and the PUBCOMP is lost on the way back.
                    _ => {
                        shared.unreleased.lock().await.remove(&pubrel.packet_id);
                        None
                    }
                };
                let Some(reason_code) = reason_code else {
                    continue;
                };
                let answer = bytes(&Packet::Pubcomp(Pubcomp {
                    packet_id: pubrel.packet_id,
                    reason_code,
                    properties: Properties::new(),
                }));
                if stream.write_all(&answer).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
            Act::Suback(codes) => {
                let Some(raw) = read_packet(&mut stream, &mut buf).await else {
                    return;
                };
                shared.seen.lock().await.push(raw.clone());
                let Packet::Subscribe(subscribe) = decode(&raw) else {
                    panic!(
                        "the script expected a SUBSCRIBE, got {}",
                        decode(&raw).packet_type()
                    );
                };
                let codes: Vec<SubackReasonCode> = codes
                    .iter()
                    .map(|code| {
                        SubackReasonCode::from_byte(*code).expect("a SUBACK code the script names")
                    })
                    .collect();
                let answer = bytes(&Packet::Suback(Suback {
                    packet_id: subscribe.packet_id,
                    properties: Properties::new(),
                    reason_codes: PayloadList::new(&codes),
                }));
                if stream.write_all(&answer).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
            Act::Unsuback(codes) => {
                let Some(raw) = read_packet(&mut stream, &mut buf).await else {
                    return;
                };
                shared.seen.lock().await.push(raw.clone());
                let Packet::Unsubscribe(unsubscribe) = decode(&raw) else {
                    panic!(
                        "the script expected an UNSUBSCRIBE, got {}",
                        decode(&raw).packet_type()
                    );
                };
                let codes: Vec<UnsubackReasonCode> = codes
                    .iter()
                    .map(|code| {
                        UnsubackReasonCode::from_byte(*code)
                            .expect("an UNSUBACK code the script names")
                    })
                    .collect();
                let answer = bytes(&Packet::Unsuback(Unsuback {
                    packet_id: unsubscribe.packet_id,
                    properties: Properties::new(),
                    reason_codes: PayloadList::new(&codes),
                }));
                if stream.write_all(&answer).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
            Act::AnswerAuth(code) | Act::AnswerAuthAs(code, _) => {
                let Some(raw) = read_packet(&mut stream, &mut buf).await else {
                    return;
                };
                shared.seen.lock().await.push(raw.clone());
                let Packet::Auth(request) = decode(&raw) else {
                    panic!(
                        "the script expected an AUTH, got {}",
                        decode(&raw).packet_type()
                    );
                };
                let method = match act {
                    Act::AnswerAuthAs(_, method) => Some(method),
                    _ => request.properties.authentication_method,
                };
                let data = request.properties.authentication_data.map(<[u8]>::to_vec);
                let answer = bytes(&Packet::Auth(Auth {
                    reason_code: AuthReasonCode::from_byte(code)
                        .expect("an AUTH code the script names"),
                    properties: Properties {
                        authentication_method: method,
                        authentication_data: data.as_deref(),
                        ..Properties::new()
                    },
                }));
                if stream.write_all(&answer).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
            Act::Idle(duration) => tokio::time::sleep(duration).await,
            Act::Close => return,
        }
    }
    // Hold the connection open until the client goes away, so a test that
    // asserts the client's own close sees it.
    let mut sink = [0u8; 64];
    while let Ok(read) = stream.read(&mut sink).await {
        if read == 0 {
            return;
        }
    }
}

/// The answer to a PUBLISH, applying the receiver's duplicate suppression.
///
/// `None` means the server accepted the message and answered nothing, which
/// is a lost acknowledgement.
async fn answer_publish(raw: &[u8], act: &Act, shared: &Shared) -> Option<Vec<u8>> {
    let Packet::Publish(publish) = decode(raw) else {
        panic!(
            "the script expected a PUBLISH, got {}",
            decode(raw).packet_type()
        );
    };
    let failure = match act {
        Act::RefusePublish(code) => Some(*code),
        _ => None,
    };
    let silent = matches!(act, Act::AcceptPublish);

    match publish.qos {
        QoS::AtMostOnce => {
            // No response at all, and no retry: QoS 0 "arrives either once or
            // not at all" (4.3.1).
            shared.deliveries.fetch_add(1, Ordering::SeqCst);
            None
        }
        QoS::AtLeastOnce => {
            let packet_id = publish.packet_id.expect("QoS 1 carries an identifier");
            let reason_code = match failure {
                Some(code) => PubackReasonCode::from_byte(code).expect("a PUBACK code"),
                None => {
                    shared.deliveries.fetch_add(1, Ordering::SeqCst);
                    PubackReasonCode::Success
                }
            };
            if silent {
                return None;
            }
            Some(bytes(&Packet::Puback(Puback {
                packet_id,
                reason_code,
                properties: Properties::new(),
            })))
        }
        QoS::ExactlyOnce => {
            let packet_id = publish.packet_id.expect("QoS 2 carries an identifier");
            let reason_code = match failure {
                Some(code) => PubackReasonCode::from_byte(code).expect("a PUBREC code"),
                None => {
                    // [MQTT-4.3.3-10]: a repeat is answered again and MUST NOT
                    // cause a duplicate onward delivery.
                    let first_sight = shared.unreleased.lock().await.insert(packet_id);
                    if first_sight {
                        shared.deliveries.fetch_add(1, Ordering::SeqCst);
                    }
                    PubackReasonCode::Success
                }
            };
            if silent {
                return None;
            }
            Some(bytes(&Packet::Pubrec(Pubrec {
                packet_id,
                reason_code,
                properties: Properties::new(),
            })))
        }
    }
}

/// Reads exactly one packet, returning its bytes.
async fn read_packet(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        if let Ok((header, header_len)) = FixedHeader::decode(buf, CAP) {
            let total = header_len + header.remaining_length as usize;
            if buf.len() >= total {
                let packet = buf[..total].to_vec();
                buf.drain(..total);
                return Some(packet);
            }
        }
        let before = buf.len();
        buf.resize(before + 1024, 0);
        let read = stream.read(&mut buf[before..]).await.ok()?;
        buf.truncate(before + read);
        if read == 0 {
            return None;
        }
    }
}

/// One packet's bytes, for a script.
pub fn bytes(packet: &Packet<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    packet.encode(&mut out).expect("a script packet encodes");
    out
}

/// Decodes one of [`Server::seen_bytes`]'s entries.
pub fn decode(bytes: &[u8]) -> Packet<'_> {
    Packet::decode(bytes, CAP)
        .map(|(packet, _)| packet)
        .expect("a recorded packet decodes")
}

/// A CONNACK with Success and `properties`, which is how a script declares
/// what the server does and does not offer.
pub fn connack(properties: Properties<'_>) -> Vec<u8> {
    connack_session(false, properties)
}

/// The same with `Session Present` chosen, for the tests that resume a
/// session.
pub fn connack_session(session_present: bool, properties: Properties<'_>) -> Vec<u8> {
    bytes(&Packet::Connack(Connack {
        session_present,
        reason_code: ConnectReasonCode::Success,
        properties,
    }))
}

/// Options for a client under test against the scripted server.
///
/// Keep Alive is off: a scripted server that has run out of script answers no
/// PINGREQ, so an enabled keep-alive would make every test wait out the
/// PINGRESP deadline before its own assertions ran. The keep-alive behaviour
/// has its own tests in `connection.rs`, which switch it back on.
pub fn options(client_id: &str) -> ConnectOptions {
    let mut options = ConnectOptions::new(client_id);
    options.keep_alive = Duration::ZERO;
    options.connect_timeout = Duration::from_secs(5);
    options
}

/// Every await in an interop test is bounded by this, so a broker that stops
/// answering ends a test instead of hanging it.
pub const DEADLINE: Duration = Duration::from_secs(10);

/// A Client Identifier no previous run has used.
///
/// Sessions outlive a test run by design - that is what `Session Expiry
/// Interval` is for - so a fixed identifier makes the *second* run of a
/// session test start against state the first one left. Which is not a flaw
/// in the tests: it is [MQTT-3.2.2-4] biting. A fresh identifier per run is
/// what keeps each test measuring the thing it is about.
pub fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    format!("{prefix}-{nanos:x}")
}
