//! The synchronous facade, driven from a thread with no reactor at all.
//!
//! That is the whole claim of `weida_mqtt::blocking`, and it is not something
//! a `#[tokio::test]` can make: those run *inside* a runtime, so a facade that
//! secretly needed one would pass. Every test here is a plain `#[test]`, which
//! is a thread with no ambient reactor, and the context's own is the only one
//! in the process.
//!
//! What is **not** tested here is any protocol behaviour, because the facade
//! decides none: the QoS machines, the session, the refusals and the timers
//! are the asynchronous surface's and are tested against it. What is tested is
//! exactly what the facade adds — that a blocking caller reaches them, that
//! the deadline `recv_timeout` adds works, and that the connection survives
//! its expiry.

#![cfg(feature = "blocking")]

use crate::harness;

use std::time::Duration;

use harness::{Act, Server, bytes, connack};
use weida_mqtt::blocking::BlockingContext;
use weida_mqtt::{ConnectOptions, DisconnectReasonCode, Error, Message, QoS, Subscription};
use weida_mqtt_codec::{Connack, ConnectReasonCode, Packet, Properties, Publish};

fn delivery(topic: &str, payload: &[u8]) -> Vec<u8> {
    bytes(&Packet::Publish(Publish {
        topic,
        payload,
        qos: QoS::AtMostOnce,
        ..Publish::default()
    }))
}

fn options() -> ConnectOptions {
    harness::options("blocking-client")
}

/// The harness is async, so it needs a runtime of its own — started on a
/// **separate** thread, so the thread running the facade has none. That
/// separation is the point: a facade that needed an ambient reactor would fail
/// here and pass under `#[tokio::test]`.
struct ScriptedServer {
    address: String,
    _runtime: std::thread::JoinHandle<()>,
    done: std::sync::mpsc::Receiver<()>,
}

impl ScriptedServer {
    fn start(scripts: Vec<Vec<Act>>) -> ScriptedServer {
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let (done_tx, done) = std::sync::mpsc::channel();
        let runtime = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for the server");
            runtime.block_on(async move {
                let mut server = Server::start_all(scripts).await;
                address_tx.send(server.address().to_string()).expect("sent");
                server.finished().await;
                let _ = done_tx.send(());
                // Hold the server alive until the test is done with it.
                std::future::pending::<()>().await;
            });
        });
        ScriptedServer {
            address: address_rx.recv().expect("the server's address"),
            _runtime: runtime,
            done,
        }
    }

    fn finished(&self) {
        let _ = self.done.recv_timeout(Duration::from_secs(10));
    }
}

/// A round trip with no event loop in the process: connect, subscribe,
/// publish at QoS 2, receive.
#[test]
fn the_facade_completes_a_round_trip_with_no_ambient_reactor() {
    // The claim, asserted before anything else: this thread has no reactor.
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "this test's whole point is a thread with no ambient runtime"
    );

    let server = ScriptedServer::start(vec![vec![
        Act::Send(connack(Properties::new())),
        Act::Suback(vec![0x02]),
        Act::AckPublish,     // the QoS 2 PUBLISH, answered PUBREC
        Act::CompletePubrel, // and PUBCOMP
        Act::Send(delivery("room/12", b"21.5")),
        Act::Expect, // the DISCONNECT
    ]]);

    let context = BlockingContext::owned(1).expect("a reactor the context owns");
    let (client, mut deliveries) = context
        .connect(&server.address, options())
        .expect("connects");

    let granted = client
        .subscribe(vec![Subscription::new("room/+", QoS::ExactlyOnce)])
        .expect("subscribed");
    assert_eq!(granted.len(), 1);
    assert_eq!(granted[0].as_byte(), 0x02);

    let completion = client
        .publish(Message::new("room/12", "21.5").at(QoS::ExactlyOnce))
        .expect("publishes");
    assert!(
        matches!(completion, weida_mqtt::Completion::Complete(_)),
        "the PUBCOMP, blocking: {completion:?}"
    );

    let delivered = deliveries.recv().expect("a delivery");
    assert_eq!(delivered.topic, "room/12");
    assert_eq!(delivered.payload, &b"21.5"[..]);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .expect("disconnects");
    server.finished();
}

/// **Where the asynchronous surface has cancellation, this has a deadline.**
///
/// The expiry is not a failure of the connection: nothing was consumed, so the
/// next call waits again and gets the delivery that arrives later. That is the
/// half a `recv_timeout` that closed the connection would get wrong, and it is
/// why this test does both in order.
#[test]
fn a_deadline_expires_without_disturbing_the_connection() {
    let server = ScriptedServer::start(vec![vec![
        Act::Send(connack(Properties::new())),
        // Nothing for a while, so the first deadline expires.
        Act::Idle(Duration::from_millis(600)),
        Act::Send(delivery("room/12", b"late")),
        Act::Expect,
    ]]);

    let context = BlockingContext::owned(1).expect("a reactor");
    let (client, mut deliveries) = context
        .connect(&server.address, options())
        .expect("connects");

    let error = deliveries
        .recv_timeout(Duration::from_millis(200))
        .expect_err("nothing has arrived yet");
    assert!(matches!(error, Error::Timeout("a delivery")), "{error}");

    // The connection is untouched: the delivery that arrives later is still
    // there to be read.
    let delivered = deliveries
        .recv_timeout(Duration::from_secs(5))
        .expect("the delivery that arrived after the deadline");
    assert_eq!(delivered.payload, &b"late"[..]);

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .expect("disconnects");
    server.finished();
}

/// A refusal the facade inherits rather than re-derives: the server declared
/// `Retain Available` 0 and a retained publish never reaches the wire.
///
/// The point is not the refusal — that is tested against the asynchronous
/// surface — but that the **synchronous** surface reports the identical error
/// with the identical code, which is what "no protocol behaviour is decided
/// twice" means in practice.
#[test]
fn a_refusal_is_the_asynchronous_surfaces_refusal() {
    let declines = bytes(&Packet::Connack(Connack {
        session_present: false,
        reason_code: ConnectReasonCode::Success,
        properties: Properties {
            retain_available: Some(false),
            maximum_qos: Some(QoS::AtLeastOnce),
            ..Properties::new()
        },
    }));
    let server = ScriptedServer::start(vec![vec![
        Act::Send(declines),
        Act::Idle(Duration::from_secs(2)),
    ]]);

    let context = BlockingContext::owned(1).expect("a reactor");
    let (client, _deliveries) = context
        .connect(&server.address, options())
        .expect("connects");

    assert_eq!(
        client
            .publish(Message::new("a/b", "x").retained())
            .expect_err("refused")
            .reason_code(),
        Some(0x9A),
        "Retain not supported, refused before the wire"
    );
    assert_eq!(
        client
            .publish(Message::new("a/b", "x").at(QoS::ExactlyOnce))
            .expect_err("refused")
            .reason_code(),
        Some(0x9B),
        "QoS not supported, the same"
    );
    // And what the server did allow goes through.
    client
        .publish(Message::new("a/b", "x").at(QoS::AtMostOnce))
        .expect("QoS 0 is below the declared maximum");

    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .ok();
}

/// Two threads, each blocking on its own connection.
///
/// This is what "the future is polled on the thread that asked" buys: the
/// reactor's threads are driving the connections, so neither caller is waiting
/// for a worker that is waiting for it. A facade that used the reactor's own
/// `block_on` would deadlock here with `worker_threads` of 1.
#[test]
fn two_threads_each_block_on_their_own_connection() {
    let server = ScriptedServer::start(vec![
        vec![
            Act::Send(connack(Properties::new())),
            Act::AckPublish,
            Act::Expect,
        ],
        vec![
            Act::Send(connack(Properties::new())),
            Act::AckPublish,
            Act::Expect,
        ],
    ]);
    let address = server.address.clone();

    // One reactor thread for two blocking callers: if either parked a worker,
    // this would not finish.
    let context = BlockingContext::owned(1).expect("a reactor");

    let threads: Vec<_> = (0..2)
        .map(|index| {
            let context = context.clone();
            let address = address.clone();
            std::thread::spawn(move || {
                let mut options = options();
                options.client_id = format!("blocking-{index}");
                let (client, _deliveries) = context.connect(&address, options).expect("connects");
                let completion = client
                    .publish(Message::new("a/b", "x").at(QoS::AtLeastOnce))
                    .expect("publishes");
                client
                    .disconnect(DisconnectReasonCode::NormalDisconnection)
                    .ok();
                completion
            })
        })
        .collect();

    for thread in threads {
        let completion = thread.join().expect("the thread finishes");
        assert!(
            matches!(completion, weida_mqtt::Completion::Acknowledged(_)),
            "{completion:?}"
        );
    }
}

/// The documented one-thread pattern — publish, then read deliveries on the
/// same thread — must not deadlock when a flood has filled the delivery
/// queue: the only thread that could drain it is the one `publish` blocks, so
/// `publish` must not wait for the queue.
#[test]
fn a_publish_returns_while_the_delivery_queue_is_full_on_one_thread() {
    let mut script = vec![Act::Send(connack(Properties::new()))];
    script.extend((0..1100).map(|_| Act::Send(delivery("t", b"x"))));
    script.push(Act::Expect); // the publish
    script.push(Act::Expect); // the DISCONNECT
    let server = ScriptedServer::start(vec![script]);

    let context = BlockingContext::owned(1).expect("a reactor the context owns");
    let mut options = options();
    options.limits.incoming_queue = 16;
    let (client, mut deliveries) = context.connect(&server.address, options).expect("connects");
    // Long enough for the flood to arrive and fill the queue.
    std::thread::sleep(Duration::from_millis(500));

    // On a thread of its own, so that a hang is a failed assertion and not a
    // hung test run.
    let (tx, rx) = std::sync::mpsc::channel();
    let publisher = std::thread::spawn(move || {
        let published = client.publish(Message::new("t", "x"));
        let _ = tx.send(published.is_ok());
        client
    });
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10)),
        Ok(true),
        "publish returned with 1,100 deliveries waiting"
    );
    let client = publisher.join().expect("the publishing thread");

    // Nothing was lost to get there: the deliveries are still to be read.
    let first = deliveries
        .recv_timeout(Duration::from_secs(5))
        .expect("a delivery");
    assert_eq!(first.payload, b"x");
    client
        .disconnect(DisconnectReasonCode::NormalDisconnection)
        .expect("disconnects");
    server.finished();
}
