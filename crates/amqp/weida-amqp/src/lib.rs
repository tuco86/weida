//! An AMQP 1.0 client in Rust, usable with no weida in the picture.
//!
//! This crate is a **product, not an adapter**: it implements the client half
//! of the OASIS Standard of 29 October 2012 for applications that speak AMQP,
//! rather than bridging AMQP into weida. Nothing of weida's protocol is in it
//! and nothing of it is in weida
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4, §4.5).
//!
//! What it shares with weida is the machine room: `weida-runtime`'s reactor,
//! resolver and close budget, and `weida-core`'s error vocabulary at the
//! boundary. `[dependencies]` names `weida-runtime`, `weida-core` and
//! `weida-amqp-codec`, and **never** `weida` or `weida-protocol`: an AMQP
//! user must not link quinn and weida's pattern layer to open a TCP
//! connection [0013 §4.2].
//!
//! # Client, not broker
//!
//! A client attaches links to a peer's nodes. AMQP 1.0 "defines no operation
//! to create, configure, enumerate or delete a node", so there is no server
//! to build from the core standard alone
//! ([0014](../../../docs/decisions/0014-parallel-libraries.md) §2). This
//! crate holds the client half of every concept the protocol defines for a
//! client — including the ones a broker also has, such as a terminus's
//! expiry policy — and no node model at all.
//!
//! # What exists so far
//!
//! * [`Connection`] — the whole negotiation: the eight-octet header sent
//!   immediately on connect and the partner's answer read as the *demand* it
//!   is, the SASL dialog, TLS reached either way, `open` on channel 0, the
//!   idle timeout in both directions, and `close` as the last thing ever
//!   written.
//! * [`ConnectionOptions`] — every field of `open` under the name Part 2
//!   §2.7.1 gives it, plus the bounds the protocol does not supply. Three
//!   defaults deliberately differ from the specification's and the table in
//!   [`options`] says which and why.
//! * [`Sasl`] — `ANONYMOUS`, `PLAIN` and `EXTERNAL`, chosen rather than
//!   negotiated, with all five outcome codes distinguished.
//! * [`TlsMode`] — the two ways Part 5 §5.2 reaches TLS, which are not
//!   interchangeable.
//! * [`transport::Wire`] — protocol headers and frames over one buffer, with
//!   the 512-octet pre-negotiation ceiling in force until `open` has been
//!   read.
//! * [`Session`] — `begin` and `end`, the two channel numberings, and the six
//!   flow-control variables of [`Windows`], counted in `transfer` **frames**.
//! * [`Link`] — `attach` and `detach`, the two independent handle spaces, the
//!   termini of [`Source`] and [`Target`], the steal, and the credit of
//!   [`Credit`], counted in **messages**.
//! * [`Delivery`] and [`Sent`] — a message, however many `transfer` frames it
//!   took, reassembled under the link's `max-message-size`.
//!
//! # Rules this crate keeps
//!
//! * **Options are honoured or refused, never silently ignored** [0013 §4.4
//!   item 4]. A configuration this client cannot deliver fails in
//!   [`ConnectionOptions::validate`] or at the constructor, with a message
//!   naming the value — a `TlsMode` set on [`Connection::connect`], a
//!   `max-frame-size` below the 512 both peers MUST accept.
//! * **Three defaults deliberately differ from the specification's** [0013
//!   §4.4 item 5]: `max-frame-size`, `idle-time-out` and `channel-max`. All
//!   three are settable back; none is silent. See [`options`].
//! * **Every remote-influenced table has a named bound.** Where the protocol
//!   supplies one this crate uses it — the session table is bounded by
//!   `channel-max` ([`ConnectionOptions::max_sessions`]), the link table by
//!   the peer's `handle-max` and by the `handle-max` this end advertised, a
//!   link's messages by the credit its receiver granted and a session's
//!   frames by its window — and where it does not, the bound is ours and says
//!   so: [`connection::OUTGOING_QUEUE`] for frames queued for the driver,
//!   [`transport::OUTBOX_HIGH_WATER`] for octets queued for a peer that is
//!   not reading, [`link::MAX_QUEUED_REPORTS`] for what a link tells a
//!   handle that never reads, [`link::DEFAULT_MAX_MESSAGE_SIZE`] for a
//!   message a peer would otherwise send one bounded frame at a time,
//!   [`settlement::DEFAULT_MAX_UNSETTLED`] for the deliveries a link keeps
//!   state for, [`sasl::MAX_ROUNDS`] for the challenge/response loop Part 5
//!   leaves unbounded, and [`ConnectionOptions::max_resolved_addresses`] for
//!   the resolver's answer.
//! * **The driver waits for no peer and no application.** It is one task for
//!   every session and link of a connection, so it reads, writes, times out
//!   and closes inside one `select!`, with a clock
//!   ([`ConnectionOptions::write_timeout`]) that fails a connection whose
//!   peer has stopped reading; and what it tells an application goes into a
//!   queue it never waits on. See [`connection`].
//! * **Identity types stay apart** [0013 §4.4 item 6]. A SASL identity and
//!   weida's proved `Fingerprint` are different claims about different
//!   things, and no conversion between them exists in this workspace.
//!
//! # Sources
//!
//! Every number and every quoted rule comes from
//! `docs/research/amqp10.md`, which cites the five OASIS parts. Part
//! references in this crate's documentation are to those: Part 1 Types,
//! Part 2 Transport, Part 3 Messaging, Part 4 Transactions, Part 5 Security.

#![warn(missing_docs)]

pub mod connection;
pub mod credit;
pub mod delivery;
pub mod error;
pub mod link;
pub mod options;
pub mod owned;
mod queue;
pub mod sasl;
pub mod session;
pub mod settlement;
pub mod terminus;
pub mod transport;
pub mod window;

pub use connection::{Connection, RemoteOpen, State};
pub use credit::Credit;
pub use delivery::{Delivery, Sent};
pub use error::{Condition, Error, Result};
pub use link::{Link, LinkEvent, LinkOptions, LinkState, Negotiated};
pub use options::{ConnectionOptions, Sasl, TlsMode};
pub use owned::OwnedValue;
pub use session::{Session, SessionOptions};
pub use settlement::{Outcome, Pending, Settlement};
pub use terminus::{DistributionMode, Source, Target, TerminusDurability, TerminusExpiryPolicy};
pub use transport::{Stream, Wire};
pub use window::Windows;
