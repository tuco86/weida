//! Retained messages and the Will on the wire: RETAIN, the zero-byte delete,
//! telling a retained delivery from a live one, and the Will.
//!
//! # What a scripted server can and cannot prove about the Will
//!
//! The Will is **published by the server**, so "the Will fires" is not an
//! observable of this client at all - no client ever sees its own Will. What
//! *is* observable, and what every rule in 3.1.2.5 and 3.14.4 turns on, is
//! **how the connection ended**:
//!
//! * a DISCONNECT with reason code 0x00 - the server "MUST discard the Will
//!   without publishing it" ([MQTT-3.14.4-3]);
//! * a DISCONNECT with 0x04 `DisconnectWithWillMessage` - the server publishes
//!   it anyway, which is the only way a client asks for its own Will;
//! * the transport closing with no DISCONNECT at all - the "abnormal
//!   disconnection" that is the mechanism's entire purpose (3.1.2.5).
//!
//! So these tests assert which of those three the server saw, which is exactly
//! the client's half of the contract. The broker half - that the message
//! actually appears on the Will Topic, and that it does not appear when a new
//! connection to the session arrives inside the Will Delay Interval - is
//! measured against a real broker in the interop tests (`interop_rumqttd.rs`,
//! `interop_mqtt5.rs`), because asserting it here would only be asserting our
//! own scripted server.

use crate::harness;

use std::time::Duration;

use harness::bytes;
use weida_mqtt::{Client, ConnectOptions, Context, QoS, WillMessage};
use weida_mqtt_codec::{Packet, Publish};

pub(crate) fn options() -> ConnectOptions {
    harness::options("retain-client")
}

/// A delivery on `topic` with the RETAIN flag as given.
pub(crate) fn delivery(topic: &str, payload: &[u8], retain: bool) -> Vec<u8> {
    bytes(&Packet::Publish(Publish {
        topic,
        payload,
        qos: QoS::AtMostOnce,
        retain,
        ..Publish::default()
    }))
}

/// The Will Topic is a Topic **Name**: no wildcards, never zero-length
/// ([MQTT-3.1.3-10]), refused at configuration time because nothing about it
/// depends on the server.
#[tokio::test]
async fn a_will_topic_is_a_topic_name_and_is_refused_otherwise() {
    let context = Context::new().expect("ambient");
    // No server is started: none of these reaches a socket, which is the
    // point.
    for topic in ["status/+", "status/#", ""] {
        let mut options = options();
        options.will = Some(WillMessage {
            topic: topic.into(),
            payload: b"gone".to_vec(),
            ..WillMessage::default()
        });
        assert!(options.validate().is_err(), "{topic:?} is not a Topic Name");
        assert!(
            Client::connect(&context, "127.0.0.1:1", options)
                .await
                .is_err(),
            "{topic:?} never reaches a dial"
        );
    }

    // A Will Delay Interval above the Four Byte Integer is refused the same
    // way.
    let mut options = options();
    options.will = Some(WillMessage {
        topic: "status/client".into(),
        delay: Some(Duration::from_secs(u64::from(u32::MAX) + 1)),
        ..WillMessage::default()
    });
    assert!(options.validate().is_err());
}
