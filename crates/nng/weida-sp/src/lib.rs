//! nanomsg/NNG Scalability Protocols (SP v1) codec: the TCP mapping's
//! protocol header, its message framing, and the per-protocol headers.
//!
//! This crate is the codec under `weida-nng`, the nanomsg/NNG library. It is
//! **sans-I/O and weida-free**: no sockets, no executor, no dependency - not
//! even on `weida-core`. What it exports is a byte-for-byte encoder and
//! decoder for what an NNG or mangos peer puts on a TCP connection, so that
//! it can be checked against a foreign specification rather than against
//! weida's opinion of it (`docs/ARCHITECTURE.md` §4). `weida-nng` is built on
//! top and depends on this crate and on `weida-runtime`; this one depends on
//! neither.
//!
//! # Shape
//!
//! * [`header`] - the 8-octet protocol header (`0x00 'S' 'P'`, version,
//!   endpoint type, reserved), the endpoint-type registry and the pairing
//!   rule.
//! * [`message`] - the 64-bit size field, with the cap that a declared length
//!   is checked against **before** the body is touched.
//! * [`backtrace`] - the 32-bit tag stack REQ/REP and SURVEYOR/RESPONDENT put
//!   in front of a body, terminated by the ID with its high bit set.
//! * [`pair`] - PAIR v1's 32-bit hop count.
//!
//! Decoders borrow: a decoded body is a slice of the caller's buffer, and
//! splitting a per-protocol header off it borrows again. The only allocation
//! on the way in is the peer-ID list of a forwarded tag stack, bounded by the
//! `max_hops` the caller passes.
//!
//! # Reading a connection
//!
//! ```
//! use weida_sp::{Backtrace, EndpointType, ProtocolHeader, backtrace, message};
//!
//! // 1. Both sides send the protocol header immediately and wait for the
//! //    peer's. There is no other handshake.
//! let ours = ProtocolHeader::new(EndpointType::Rep);
//! let peer = ProtocolHeader::decode(&ProtocolHeader::new(EndpointType::Req).encode())
//!     .expect("a protocol header");
//! assert!(peer.accepts(EndpointType::Rep));
//! let _ = ours.encode();
//!
//! // 2. Then messages: a 64-bit size and that many octets.
//! let wire = message::encode(&Backtrace::direct(1).encode_message(b"ping"));
//! let (body, used) = message::decode(&wire, 8 * 1024 * 1024).expect("a message");
//! assert_eq!(used, wire.len());
//!
//! // 3. A REQ/REP body begins with the tag stack; the payload is what is left.
//! let (stack, payload) = backtrace::decode(body, backtrace::DEFAULT_MAX_HOPS).expect("tags");
//! assert_eq!(stack.id, 1);
//! assert_eq!(payload, b"ping");
//! ```
//!
//! # Bounds
//!
//! SP grants no credit on the wire [nanomsg-nng §12/P12], a message may
//! declare 2^64-1 octets [rfc-tcp §3], and `NNG_OPT_RECVMAXSZ` - the only
//! inbound defence - is unlimited by default [nanomsg-nng §5]. Every decode
//! entry point therefore takes its bound as an argument:
//! [`message::decode`] the byte cap, [`backtrace::decode`] and [`pair::decode`]
//! the hop cap. Nothing in this crate reserves memory on behalf of a peer.
//!
//! # Where this codec departs from the text
//!
//! Each of these is cited below to the research sheet
//! (`docs/research/nanomsg-nng.md`), and where a real peer settles one,
//! `docs/libraries/nng.md` §8 records what it measured.
//!
//! * **Endpoint type numbers come from the implementation.** The SP RFCs
//!   assign the 12-bit protocol IDs and delegate the 4-bit endpoint roles to
//!   the per-protocol RFCs, which never published them [rfc-ids §1]. The role
//!   halves here are NNG's registry [nng-src `core/protocol.h`].
//! * **PAIR v1's initial hop count is the one that was measured.** The
//!   sheet says the counter starts at one [nanomsg-nng §4]; a reading of
//!   NNG's source suggested zero [nng-src `pair1/pair.c`]; a running NNG
//!   1.4.0-rc.0 sends **one** [nanomsg-nng §31]. This codec encodes one
//!   and decodes both - see [`pair`].
//! * **The hop ceiling has two values.** 1-255 on the specification side
//!   [nanomsg-nng §11], 15 in NNG's source [nng-src `core/defs.h`]. Both are
//!   published as constants and the caller chooses - see [`backtrace`].
//! * **An oversized message is fatal here, and recoverable in NNG.** NNG
//!   discards the message and keeps the pipe [nanomsg-nng §8]; a sans-I/O
//!   decoder cannot drain the declared octets, so it reports the condition and
//!   leaves that choice to a caller that owns the socket - see
//!   [`error::MessageError::is_violation`].
//! * **PAIR v0 and PUB/SUB have no per-protocol header**, so there is no
//!   module for them: a PAIR v0 body is the payload, and a SUB topic is the
//!   leading bytes of the body with no field of its own [nanomsg-nng §3]. Any
//!   topic split is the caller's configuration, not framing.

#![warn(missing_docs)]

pub mod backtrace;
pub mod error;
pub mod header;
pub mod message;
pub mod pair;

pub use backtrace::Backtrace;
pub use error::{HeaderError, MessageError, TagError};
pub use header::{EndpointType, HEADER_LEN, MAGIC, ProtocolHeader, VERSION};
