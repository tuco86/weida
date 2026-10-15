//! ZMTP 3.1 codec: greeting, framing, commands and connection metadata.
//!
//! This crate is the codec under `weida-zmq`, the ZeroMQ library. It is
//! **sans-I/O and weida-free**: no sockets, no executor, no dependency - not
//! even on `weida-core`. What it exports is a byte-for-byte encoder and
//! decoder for [37/ZMTP], the protocol libzmq speaks, so that it can be
//! checked against a foreign specification rather than against weida's
//! opinion of it. `weida-zmq` is built on top and depends on this crate and
//! on `weida-runtime`; this one deliberately depends on neither.
//!
//! [37/ZMTP]: https://rfc.zeromq.org/spec/37/
//!
//! # Shape
//!
//! * [`greeting`] - the fixed 64-octet greeting, the 11-octet version sniff,
//!   the mechanism field, and the accept rules for version and mechanism.
//! * [`frame`] - the flags octet and one- or eight-octet size field, with the
//!   cap that a long frame's declared length is checked against **before** the
//!   body is touched.
//! * [`command`] - `READY`, `ERROR`, `SUBSCRIBE`, `CANCEL`, `PING`, `PONG`,
//!   and PLAIN's `HELLO`, `WELCOME` and `INITIATE` ([24/ZMTP-PLAIN]). The
//!   mechanism's own warning - it is "not robust against even the simplest
//!   traffic snooping or spoofing attacks" - is a property of the mechanism
//!   rather than of the encoding, and this crate still depends on nothing.
//! * [`curve`] - CURVE's `HELLO`, `WELCOME`, `INITIATE`, `READY` and
//!   `MESSAGE` ([26/CURVEZMQ]) as byte layouts, with every cryptographic box
//!   an **opaque range**. No key is generated, sealed or opened here; that is
//!   `weida-zmq`'s single cryptographic dependency, and the split is what
//!   keeps the layouts checkable against a hex dump.
//! * [`z85`] - the printable key encoding of [32/Z85], 40 characters per
//!   32-octet key, both ways.
//! * [`metadata`] - the `READY` property dictionary and the `Socket-Type`
//!   table, including which peer types are legal opposite which.
//!
//! [24/ZMTP-PLAIN]: https://rfc.zeromq.org/spec/24/
//! [26/CURVEZMQ]: https://rfc.zeromq.org/spec/26/
//! [32/Z85]: https://rfc.zeromq.org/spec/32/
//!
//! Decoders borrow: a decoded frame body is a slice of the caller's buffer and
//! a decoded [`Command`] points into that body, so the only allocation on the
//! way in is the property list of a `READY` or `INITIATE`: one allocation of at
//! most [`MAX_PROPERTIES`] entries.
//!
//! # Reading a stream
//!
//! ```
//! use weida_zmtp::{Command, FrameKind, Greeting, Mechanism, Metadata, SocketType, frame};
//!
//! // 1. Exchange greetings. Ours is 64 octets; the peer's must agree about
//! //    the mechanism, and speak 3.1 or higher.
//! let ours = Greeting::null();
//! let peer = Greeting::decode(&ours.encode()).expect("a greeting");
//! peer.accept(Mechanism::NULL).expect("agreement");
//!
//! // 2. The NULL handshake: one READY each way.
//! let ready = Command::Ready(Metadata::new().with_socket_type(SocketType::Push))
//!     .encode()
//!     .expect("a READY");
//!
//! // 3. Then frames, commands and messages intermixed.
//! let (header, body, used) = frame::decode(&ready, 8 * 1024 * 1024).expect("a frame");
//! assert_eq!(used, ready.len());
//! assert_eq!(header.kind, FrameKind::Command);
//! match Command::decode(body).expect("a command") {
//!     Command::Ready(md) => assert_eq!(md.socket_type(), Some(SocketType::Push)),
//!     other => panic!("expected READY, got {}", other.name()),
//! }
//! ```
//!
//! # Bounds
//!
//! ZMTP has no credit on the wire and no size limit in its grammar beyond
//! 2^63-1 octets per frame; libzmq's `ZMQ_MAXMSGSIZE` defaults to unlimited.
//! Every decode entry point therefore takes the cap as an argument, and
//! [`FrameError::BodyTooLarge`] is reported from the declared length alone.
//! Nothing in this crate reserves memory on behalf of a peer except the
//! property list of a metadata dictionary, which is capped at
//! [`MAX_PROPERTIES`] entries.
//!
//! # Where this codec departs from the text
//!
//! * **Command names are length-prefixed.** The prose says "a printable
//!   command name, a null octet separator, and data"; the ABNF says
//!   `command-name = short-size 1*255command-name-char`, and libzmq sends
//!   `\x05READY`. The grammar wins - see [`command`].
//! * **`ERROR` reasons may contain spaces.** `error-reason` is specified as
//!   `VCHAR`, which excludes the space octet, while libzmq's own reasons read
//!   "Unknown mechanism". Printable ASCII including space is accepted;
//!   anything else is refused in both directions.
//! * **The filler is not validated.** The greeting's last 31 octets are
//!   specified as zero, but no rule asks a reader to check them and a
//!   mismatch has no meaning. The eight padding octets are a stronger case:
//!   validating them is explicitly forbidden.
//! * **No downgrade.** ZMTP 1.0 and 2.0 detection is optional, and this codec
//!   declines: a peer below 3.1 is refused with
//!   [`GreetingError::UnsupportedVersion`].
//! * **`JOIN`/`LEAVE` are not implemented.** They belong to RADIO/DISH, which
//!   `weida-zmq` does not carry (`docs/libraries/zmq.md` §2); they decode to
//!   [`CommandError::UnknownName`].
//! * **CURVE's HELLO padding is 72 octets, not 70.** 26/CURVEZMQ's ABNF says
//!   `hello-padding = 72%x00` and its prose says 70; only 72 makes the
//!   specification's own 200-octet HELLO add up. The grammar wins again, in
//!   both directions - see [`curve`] and `docs/research/zeromq.md` §10.

#![warn(missing_docs)]

pub mod command;
pub mod curve;
pub mod error;
pub mod frame;
pub mod greeting;
pub mod metadata;
pub mod z85;

pub use command::{Command, MAX_PING_CONTEXT, MAX_PLAIN_FIELD};
pub use curve::{CurveCommand, InitiatePlaintext};
pub use error::{CommandError, CurveError, FrameError, GreetingError, Z85Error};
pub use frame::{FrameHeader, FrameKind};
pub use greeting::{GREETING_LEN, Greeting, Mechanism, VERSION, Version};
pub use metadata::{MAX_PROPERTIES, Metadata, SocketType};
