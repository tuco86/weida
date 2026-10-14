//! CURVE command layouts: `HELLO`, `WELCOME`, `INITIATE`, `READY` and
//! `MESSAGE` of [26/CURVEZMQ] as octets, with every cryptographic box an
//! opaque range.
//!
//! [26/CURVEZMQ]: https://rfc.zeromq.org/spec/26/
//!
//! # Why there is no crypto here
//!
//! This crate depends on nothing - see the manifest, whose `[dependencies]`
//! section is empty on purpose. A box is therefore a byte range with a
//! documented length and a documented content, and opening or sealing one is
//! the caller's business: `weida-zmq` takes the one cryptographic dependency
//! and does that work. The split is not tidiness. The layouts are the part
//! that has to match libzmq octet for octet, they are checkable against the
//! specification with a hex dump, and they must stay checkable without
//! anybody having to trust a curve implementation first.
//!
//! What this module does own is every length and every fixed prefix, so that
//! nothing downstream has to rediscover them:
//!
//! ```text
//! hello    = %d5 "HELLO"    version padding client-key short-nonce signature-box
//!            2 + 72 + 32 + 8 + 80, six octets of name          = 200 octets
//! welcome  = %d7 "WELCOME"  long-nonce welcome-box
//!            16 + 144, eight octets of name                    = 168 octets
//! initiate = %d8 "INITIATE" cookie short-nonce initiate-box
//!            96 + 8 + 144*, nine octets of name                = 257+ octets
//! ready    = %d5 "READY"    short-nonce ready-box
//!            8 + 16*, six octets of name                       =  30+ octets
//! message  = %d7 "MESSAGE"  short-nonce message-box
//!            8 + 17*, eight octets of name                     =  33+ octets
//! ```
//!
//! The four handshake commands travel as **command** frames. `MESSAGE` does
//! not: its body is a command body — the name, the nonce and the box — but
//! libzmq 4.3.5 puts a **message** frame header in front of it and closes
//! the connection on the command-framed form. That was measured, not read:
//! `crates/zmq/weida-zmq/tests/interop_libzmq.rs` completes a CURVE
//! handshake with libzmq and the first command-framed `MESSAGE` ends it.
//! This module encodes bodies; which header goes in front is the caller's,
//! and [`CurveCommand::encode`] is the command-framed form the four
//! handshake commands use.
//!
//! # The 72-versus-70 contradiction in the HELLO padding
//!
//! 26/CURVEZMQ disagrees with itself. The ABNF says `hello-padding =
//! 72%x00`; the prose says "An anti-amplification padding field. This SHALL
//! be 70 octets, all zero." Only 72 adds up: the specification's own HELLO
//! total is 200 octets, and 6 + 2 + 72 + 32 + 8 + 80 = 200 while 70 gives
//! 198. **This module implements 72**, in both directions, and refuses a
//! 198-octet `HELLO`. The reasoning and the arithmetic are recorded in
//! `docs/research/zeromq.md` §10, and the vectors are asserted in
//! `tests/golden_vectors.rs`.
//!
//! The practical consequence is worth naming: a peer written from the prose
//! sends 198 octets, every field after the padding is displaced by two, and
//! the `HELLO` is refused as [`CurveError::BadLength`] rather than mistaken
//! for a valid one - which is the good failure, because the alternative is a
//! signature box read two octets out of phase.
//!
//! # Mechanism-dependent names
//!
//! `HELLO`, `WELCOME`, `INITIATE` and `READY` are also PLAIN's or NULL's
//! command names, with completely different bodies. The mechanism is settled
//! by the greeting before any command is read, so the caller knows which
//! decoder to use: [`Command::decode`](crate::Command::decode) for NULL and
//! PLAIN, [`CurveCommand::decode`] for CURVE. Nothing here guesses.
//! `ERROR` is shared verbatim and stays with [`Command`](crate::Command).

use crate::error::CurveError;
use crate::frame::{self, FrameKind};
use crate::metadata::Metadata;

/// A long-term or transient Curve25519 key, public or secret. "These sizes
/// are not configurable; they are enforced by the underlying cryptography
/// library and act as universal constants for CurveZMQ implementations."
pub const KEY_LEN: usize = 32;

/// What sealing a box costs: a box is "16 octets larger than their
/// plaintext".
pub const BOX_OVERHEAD: usize = 16;

/// A complete crypto_box nonce: eight or sixteen octets on the wire, behind a
/// fixed prefix of the complementary length.
pub const NONCE_LEN: usize = 24;

/// The octets of a short nonce that travel: a counter, not a random value.
pub const SHORT_NONCE_LEN: usize = 8;

/// The prefix of a short nonce, which does not travel.
pub const SHORT_NONCE_PREFIX_LEN: usize = NONCE_LEN - SHORT_NONCE_LEN;

/// The octets of a long nonce that travel: unique random data.
pub const LONG_NONCE_LEN: usize = 16;

/// The prefix of a long nonce, which does not travel.
pub const LONG_NONCE_PREFIX_LEN: usize = NONCE_LEN - LONG_NONCE_LEN;

/// `hello-version = %x1 %x0`. The only version 26/CURVEZMQ defines, pinned in
/// both directions the way the greeting's version is: a `HELLO` announcing
/// anything else is refused rather than guessed at, which is also what libzmq
/// does.
pub const VERSION: [u8; 2] = [1, 0];

/// `hello-padding = 72%x00` - the ABNF's count, not the prose's 70. See the
/// module documentation.
pub const HELLO_PADDING_LEN: usize = 72;

/// `hello-box = 80OCTET`, `Box [64 * %x0](C'->S)`: 64 zero octets sealed
/// under the client's transient secret and the server's long-term public key,
/// which is what makes a `HELLO` worth 200 octets to forge.
pub const SIGNATURE_BOX_LEN: usize = 64 + BOX_OVERHEAD;

/// The whole `HELLO` command body, name included.
pub const HELLO_LEN: usize =
    6 + 2 + HELLO_PADDING_LEN + KEY_LEN + SHORT_NONCE_LEN + SIGNATURE_BOX_LEN;

/// `welcome-box = 144OCTET`, `Box [S' + cookie](S->C')`: the server's
/// transient public key and the 96-octet cookie.
pub const WELCOME_BOX_LEN: usize = KEY_LEN + COOKIE_LEN + BOX_OVERHEAD;

/// The whole `WELCOME` command body, name included. Smaller than
/// [`HELLO_LEN`] on purpose: that is the anti-amplification property, and the
/// padding is what buys it.
pub const WELCOME_LEN: usize = 8 + LONG_NONCE_LEN + WELCOME_BOX_LEN;

/// `cookie = cookie-nonce cookie-box`, 16 + 80: the server's entire memory of
/// an unauthenticated client, handed to the client to hold.
pub const COOKIE_LEN: usize = LONG_NONCE_LEN + NONCE_BOX_LEN;

/// `vouch = vouch-nonce vouch-box`, 16 + 80, the same shape as a cookie.
pub const VOUCH_LEN: usize = COOKIE_LEN;

/// An 80-octet box over two keys: the cookie's `Box [C' + s'](K)` and the
/// vouch's `Box [C',S](C->S')` are both 64 octets of plaintext.
pub const NONCE_BOX_LEN: usize = 2 * KEY_LEN + BOX_OVERHEAD;

/// The smallest `initiate-box`: `Box [C + vouch + metadata](C'->S')` with no
/// metadata at all.
pub const INITIATE_BOX_MIN_LEN: usize = KEY_LEN + VOUCH_LEN + BOX_OVERHEAD;

/// The smallest whole `INITIATE` command body - the specification's "257+".
pub const INITIATE_MIN_LEN: usize = 9 + COOKIE_LEN + SHORT_NONCE_LEN + INITIATE_BOX_MIN_LEN;

/// The smallest `ready-box`: `Box [metadata](S'->C')` over no metadata.
pub const READY_BOX_MIN_LEN: usize = BOX_OVERHEAD;

/// The smallest whole `READY` command body - the specification's "30+".
pub const READY_MIN_LEN: usize = 6 + SHORT_NONCE_LEN + READY_BOX_MIN_LEN;

/// The smallest `message-box`: the flags octet is always there, so the
/// plaintext is never empty.
pub const MESSAGE_BOX_MIN_LEN: usize = 1 + BOX_OVERHEAD;

/// The smallest whole `MESSAGE` command body.
pub const MESSAGE_MIN_LEN: usize = 8 + SHORT_NONCE_LEN + MESSAGE_BOX_MIN_LEN;

/// `message-flags`, bit 0: more frames follow, the MORE flag of an
/// unencrypted message frame moved inside the box.
pub const MESSAGE_FLAG_MORE: u8 = 0x01;

/// `message-flags`, bit 1: the box holds a **command** rather than a message
/// frame.
///
/// 26/CURVEZMQ's grammar reserves bits 7-1 and says nothing about this one,
/// but a CURVE connection has no plaintext after its `READY` and 37/ZMTP's
/// commands do not stop being needed: a `SUBSCRIBE`, a `PING` or an `ERROR`
/// after the handshake has to travel inside a `MESSAGE` box like everything
/// else. libzmq puts it in bit 1 - `flags |= 0x02` for a command frame - so
/// that is what interoperates, and the reserved-bits sentence is what the
/// grammar has instead of a rule.
pub const MESSAGE_FLAG_COMMAND: u8 = 0x02;

/// Nonce prefix of a `HELLO` signature box.
pub const PREFIX_HELLO: &[u8; SHORT_NONCE_PREFIX_LEN] = b"CurveZMQHELLO---";
/// Nonce prefix of an `INITIATE` box.
pub const PREFIX_INITIATE: &[u8; SHORT_NONCE_PREFIX_LEN] = b"CurveZMQINITIATE";
/// Nonce prefix of a `READY` box.
pub const PREFIX_READY: &[u8; SHORT_NONCE_PREFIX_LEN] = b"CurveZMQREADY---";
/// Nonce prefix of a `MESSAGE` box sent by the client.
pub const PREFIX_MESSAGE_CLIENT: &[u8; SHORT_NONCE_PREFIX_LEN] = b"CurveZMQMESSAGEC";
/// Nonce prefix of a `MESSAGE` box sent by the server.
pub const PREFIX_MESSAGE_SERVER: &[u8; SHORT_NONCE_PREFIX_LEN] = b"CurveZMQMESSAGES";
/// Nonce prefix of a `WELCOME` box.
pub const PREFIX_WELCOME: &[u8; LONG_NONCE_PREFIX_LEN] = b"WELCOME-";
/// Nonce prefix of a cookie box.
pub const PREFIX_COOKIE: &[u8; LONG_NONCE_PREFIX_LEN] = b"COOKIE--";
/// Nonce prefix of a vouch box.
pub const PREFIX_VOUCH: &[u8; LONG_NONCE_PREFIX_LEN] = b"VOUCH---";

/// Builds the 24-octet nonce of a short-nonce box: a 16-octet fixed prefix
/// and the eight-octet counter that travels, in network order.
///
/// The counter is why this is a function and not a comment. "The client and
/// server SHALL NOT send more than 2^64-1 commands in one connection", and a
/// repeated nonce under the same key is the end of the box's guarantees, so
/// the only safe source is a counter that never goes backwards. The prefixes
/// keep the four uses apart even at the same counter value:
/// [`PREFIX_HELLO`], [`PREFIX_INITIATE`], [`PREFIX_READY`],
/// [`PREFIX_MESSAGE_CLIENT`] and [`PREFIX_MESSAGE_SERVER`].
pub const fn short_nonce(prefix: &[u8; SHORT_NONCE_PREFIX_LEN], counter: u64) -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    let mut at = 0;
    while at < SHORT_NONCE_PREFIX_LEN {
        nonce[at] = prefix[at];
        at += 1;
    }
    let counter = counter.to_be_bytes();
    let mut at = 0;
    while at < SHORT_NONCE_LEN {
        nonce[SHORT_NONCE_PREFIX_LEN + at] = counter[at];
        at += 1;
    }
    nonce
}

/// Builds the 24-octet nonce of a long-nonce box: an eight-octet fixed prefix
/// ([`PREFIX_WELCOME`], [`PREFIX_COOKIE`], [`PREFIX_VOUCH`]) and 16 octets
/// that must be unique, which for these three uses means random.
pub const fn long_nonce(
    prefix: &[u8; LONG_NONCE_PREFIX_LEN],
    unique: &[u8; LONG_NONCE_LEN],
) -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    let mut at = 0;
    while at < LONG_NONCE_PREFIX_LEN {
        nonce[at] = prefix[at];
        at += 1;
    }
    let mut at = 0;
    while at < LONG_NONCE_LEN {
        nonce[LONG_NONCE_PREFIX_LEN + at] = unique[at];
        at += 1;
    }
    nonce
}

/// A decoded CURVE command. Every box is a borrowed range: this type says
/// where the ciphertext is and how long it is, never what is in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CurveCommand<'a> {
    /// The client's first command, 200 octets, of which 72 are zero padding
    /// so that a `HELLO` costs more to send than the `WELCOME` it provokes.
    Hello {
        /// `hello-client = 32OCTET`: the client's **transient** public key
        /// C'. The long-term key C stays inside the `INITIATE` box, which is
        /// the client-identity protection CURVE claims.
        client_key: &'a [u8; KEY_LEN],
        /// The counter half of the nonce, prefixed [`PREFIX_HELLO`].
        nonce: u64,
        /// `Box [64 * %x0](C'->S)`, 80 octets: 64 zero octets, which proves
        /// the client knows S and possesses C'.
        signature_box: &'a [u8; SIGNATURE_BOX_LEN],
    },
    /// The server's answer, 168 octets, after which the server keeps no state
    /// at all: "It's generated a keypair, sent that back to the client in a
    /// way only the client can read, and thrown it away."
    Welcome {
        /// The travelling half of a long nonce, prefixed [`PREFIX_WELCOME`];
        /// it must be unique and is therefore random.
        nonce: &'a [u8; LONG_NONCE_LEN],
        /// `Box [S' + cookie](S->C')`, 144 octets.
        welcome_box: &'a [u8; WELCOME_BOX_LEN],
    },
    /// The client's second command: the cookie handed back, and the client's
    /// long-term key, vouch and metadata inside a box.
    Initiate {
        /// The cookie exactly as it came out of the `WELCOME` box. The client
        /// cannot read it; the server can, and needs nothing else.
        cookie: &'a [u8; COOKIE_LEN],
        /// The counter half of the nonce, prefixed [`PREFIX_INITIATE`].
        nonce: u64,
        /// `Box [C + vouch + metadata](C'->S')`, at least
        /// [`INITIATE_BOX_MIN_LEN`] octets. Its plaintext layout is
        /// [`InitiatePlaintext`].
        initiate_box: &'a [u8],
    },
    /// The server's metadata, the last command of the handshake.
    Ready {
        /// The counter half of the nonce, prefixed [`PREFIX_READY`].
        nonce: u64,
        /// `Box [metadata](S'->C')`, at least [`READY_BOX_MIN_LEN`] octets.
        ready_box: &'a [u8],
    },
    /// Application data, either way, once the handshake is done.
    Message {
        /// The counter half of the nonce, prefixed
        /// [`PREFIX_MESSAGE_CLIENT`] or [`PREFIX_MESSAGE_SERVER`] by
        /// direction. The two prefixes are why both ends can count from zero.
        nonce: u64,
        /// `Box [flags + payload]`, at least [`MESSAGE_BOX_MIN_LEN`] octets;
        /// the flags octet carries [`MESSAGE_FLAG_MORE`].
        message_box: &'a [u8],
    },
}

impl<'a> CurveCommand<'a> {
    /// The wire name.
    pub const fn name(&self) -> &'static str {
        match self {
            CurveCommand::Hello { .. } => "HELLO",
            CurveCommand::Welcome { .. } => "WELCOME",
            CurveCommand::Initiate { .. } => "INITIATE",
            CurveCommand::Ready { .. } => "READY",
            CurveCommand::Message { .. } => "MESSAGE",
        }
    }

    /// Decodes a CURVE command body: the contents of a frame whose COMMAND
    /// flag was set, without the flags and size octets.
    ///
    /// # Errors
    ///
    /// [`CurveError::BadLength`] for a fixed-size command of the wrong size
    /// and [`CurveError::TooShort`] for a variable one below its minimum -
    /// the two cases are named apart because they mean different things about
    /// the peer. [`CurveError::UnsupportedVersion`] for a `HELLO` that is not
    /// 1.0, and [`CurveError::UnknownName`] for anything else, `ERROR`
    /// included: that one is [`Command`](crate::Command)'s.
    pub fn decode(body: &'a [u8]) -> Result<Self, CurveError> {
        let name_len = usize::from(*body.first().ok_or(CurveError::BadName)?);
        if name_len == 0 {
            return Err(CurveError::BadName);
        }
        let name = body.get(1..1 + name_len).ok_or(CurveError::BadName)?;
        if !name.iter().all(u8::is_ascii_alphabetic) {
            return Err(CurveError::BadName);
        }
        let data = &body[1 + name_len..];

        match name {
            b"HELLO" => {
                exact(body.len(), HELLO_LEN, "HELLO")?;
                let version = *array::<2>(data);
                if version != VERSION {
                    return Err(CurveError::UnsupportedVersion(version[0], version[1]));
                }
                // The padding is written as zeros and not checked on the way
                // in: no rule asks a reader to check it, a mismatch has no
                // remedy, and the length check above is what actually
                // defends the fields behind it. The same reading as the
                // greeting's filler.
                let at = 2 + HELLO_PADDING_LEN;
                Ok(CurveCommand::Hello {
                    client_key: array(&data[at..at + KEY_LEN]),
                    nonce: counter(&data[at + KEY_LEN..]),
                    signature_box: array(&data[at + KEY_LEN + SHORT_NONCE_LEN..]),
                })
            }
            b"WELCOME" => {
                exact(body.len(), WELCOME_LEN, "WELCOME")?;
                Ok(CurveCommand::Welcome {
                    nonce: array(&data[..LONG_NONCE_LEN]),
                    welcome_box: array(&data[LONG_NONCE_LEN..]),
                })
            }
            b"INITIATE" => {
                least(body.len(), INITIATE_MIN_LEN, "INITIATE")?;
                let at = COOKIE_LEN + SHORT_NONCE_LEN;
                Ok(CurveCommand::Initiate {
                    cookie: array(&data[..COOKIE_LEN]),
                    nonce: counter(&data[COOKIE_LEN..]),
                    initiate_box: &data[at..],
                })
            }
            b"READY" => {
                least(body.len(), READY_MIN_LEN, "READY")?;
                Ok(CurveCommand::Ready {
                    nonce: counter(data),
                    ready_box: &data[SHORT_NONCE_LEN..],
                })
            }
            b"MESSAGE" => {
                least(body.len(), MESSAGE_MIN_LEN, "MESSAGE")?;
                Ok(CurveCommand::Message {
                    nonce: counter(data),
                    message_box: &data[SHORT_NONCE_LEN..],
                })
            }
            _ => Err(CurveError::UnknownName),
        }
    }

    /// Appends the command body - name and data, without frame header.
    ///
    /// # Errors
    ///
    /// [`CurveError::TooShort`] if a box is below its minimum length; a box
    /// shorter than [`BOX_OVERHEAD`] cannot exist, and one that is merely
    /// wrong would be refused by the decoder at the other end.
    pub fn encode_body(&self, out: &mut Vec<u8>) -> Result<(), CurveError> {
        let name = self.name().as_bytes();
        out.push(name.len() as u8);
        out.extend_from_slice(name);
        match self {
            CurveCommand::Hello {
                client_key,
                nonce,
                signature_box,
            } => {
                out.extend_from_slice(&VERSION);
                out.extend_from_slice(&[0u8; HELLO_PADDING_LEN]);
                out.extend_from_slice(*client_key);
                out.extend_from_slice(&nonce.to_be_bytes());
                out.extend_from_slice(*signature_box);
            }
            CurveCommand::Welcome { nonce, welcome_box } => {
                out.extend_from_slice(*nonce);
                out.extend_from_slice(*welcome_box);
            }
            CurveCommand::Initiate {
                cookie,
                nonce,
                initiate_box,
            } => {
                least(
                    initiate_box.len() + INITIATE_MIN_LEN - INITIATE_BOX_MIN_LEN,
                    INITIATE_MIN_LEN,
                    "INITIATE",
                )?;
                out.extend_from_slice(*cookie);
                out.extend_from_slice(&nonce.to_be_bytes());
                out.extend_from_slice(initiate_box);
            }
            CurveCommand::Ready { nonce, ready_box } => {
                least(
                    ready_box.len() + READY_MIN_LEN - READY_BOX_MIN_LEN,
                    READY_MIN_LEN,
                    "READY",
                )?;
                out.extend_from_slice(&nonce.to_be_bytes());
                out.extend_from_slice(ready_box);
            }
            CurveCommand::Message { nonce, message_box } => {
                least(
                    message_box.len() + MESSAGE_MIN_LEN - MESSAGE_BOX_MIN_LEN,
                    MESSAGE_MIN_LEN,
                    "MESSAGE",
                )?;
                out.extend_from_slice(&nonce.to_be_bytes());
                out.extend_from_slice(message_box);
            }
        }
        Ok(())
    }

    /// Builds the complete frame, flags and size included. An `INITIATE` is
    /// always a long frame: 257 octets is past the short size field's 255.
    ///
    /// **A `MESSAGE` is framed as a message, the other four as commands** —
    /// this function frames what a peer accepts rather than what the grammar
    /// names. 26/CURVEZMQ calls `MESSAGE` a command and gives it a command
    /// body — `%d7 "MESSAGE"`, the short nonce, the box, which is what
    /// [`encode_body`](Self::encode_body) writes — but that body travels
    /// behind a **message** frame header, with the COMMAND bit clear, and a
    /// command-framed `MESSAGE` closes the connection. Measured against
    /// libzmq 4.3.5 in `weida-zmq`'s `tests/interop_libzmq.rs` and asserted as
    /// `00 21` in `tests/golden_vectors.rs`. The handshake commands stay
    /// command frames as specified, so the two forms are mixed inside one
    /// connection.
    ///
    /// The outer MORE flag is zero because the real one is the flags octet
    /// inside the box, which only the sealing side can write; a caller that
    /// wants the header in its own buffer uses
    /// [`encode_body`](Self::encode_body) and frames it itself, the way
    /// `weida-zmq`'s `CurveTransport::seal_frame` does.
    ///
    /// # Errors
    ///
    /// As [`encode_body`](Self::encode_body).
    pub fn encode(&self) -> Result<Vec<u8>, CurveError> {
        let mut body = Vec::new();
        self.encode_body(&mut body)?;
        let kind = match self {
            CurveCommand::Message { .. } => FrameKind::Message { more: false },
            _ => FrameKind::Command,
        };
        Ok(frame::encode(kind, &body))
    }
}

/// The plaintext of an `INITIATE` box, which is octets like everything else
/// here - the box around it is the caller's to open.
///
/// ```text
/// initiate-plaintext = client-key vouch metadata
/// client-key = 32OCTET     ; the client's long-term public key C
/// vouch      = 96OCTET     ; vouch-nonce vouch-box
/// metadata   = *property
/// ```
///
/// The vouch is `Box [C',S](C->S')`, a deliberate deviation from CurveCP:
/// signing the server's long-term key alongside the transient one is what
/// "reduce\[s\] the risk of client impersonation", because a vouch captured by
/// one server cannot be replayed at another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitiatePlaintext<'a> {
    /// The client's long-term public key C, which reaches a ZAP handler as
    /// the CURVE credential and is the only thing an authorization decision
    /// can be made on.
    pub client_key: &'a [u8; KEY_LEN],
    /// `vouch = vouch-nonce vouch-box`, splittable with [`split_nonce_box`].
    pub vouch: &'a [u8; VOUCH_LEN],
    /// The client's connection metadata, where NULL puts it in `READY`.
    pub metadata: Metadata<'a>,
}

/// The smallest `INITIATE` plaintext: both keys and no metadata.
pub const INITIATE_PLAINTEXT_MIN_LEN: usize = KEY_LEN + VOUCH_LEN;

impl<'a> InitiatePlaintext<'a> {
    /// Reads an opened `INITIATE` box.
    ///
    /// # Errors
    ///
    /// [`CurveError::TooShort`] below [`INITIATE_PLAINTEXT_MIN_LEN`], and
    /// [`CurveError::Metadata`] for a malformed property dictionary.
    pub fn decode(plaintext: &'a [u8]) -> Result<Self, CurveError> {
        least(
            plaintext.len(),
            INITIATE_PLAINTEXT_MIN_LEN,
            "INITIATE plaintext",
        )?;
        Ok(Self {
            client_key: array(&plaintext[..KEY_LEN]),
            vouch: array(&plaintext[KEY_LEN..KEY_LEN + VOUCH_LEN]),
            metadata: Metadata::decode(&plaintext[KEY_LEN + VOUCH_LEN..])
                .map_err(CurveError::Metadata)?,
        })
    }

    /// Writes the plaintext to be sealed.
    ///
    /// # Errors
    ///
    /// [`CurveError::Metadata`] if the metadata cannot be encoded.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CurveError> {
        out.extend_from_slice(self.client_key);
        out.extend_from_slice(self.vouch);
        self.metadata.encode(out).map_err(CurveError::Metadata)
    }
}

/// Splits a cookie or a vouch into its long nonce and its 80-octet box. The
/// two fields have the same shape, which is not a coincidence: both are a box
/// whose nonce nobody else will ever choose.
pub fn split_nonce_box(field: &[u8; COOKIE_LEN]) -> (&[u8; LONG_NONCE_LEN], &[u8; NONCE_BOX_LEN]) {
    let (nonce, sealed) = field.split_at(LONG_NONCE_LEN);
    (array(nonce), array(sealed))
}

/// The other direction of [`split_nonce_box`].
pub fn join_nonce_box(
    nonce: &[u8; LONG_NONCE_LEN],
    sealed: &[u8; NONCE_BOX_LEN],
) -> [u8; COOKIE_LEN] {
    let mut field = [0u8; COOKIE_LEN];
    field[..LONG_NONCE_LEN].copy_from_slice(nonce);
    field[LONG_NONCE_LEN..].copy_from_slice(sealed);
    field
}

/// Splits an opened `WELCOME` box into the server's transient public key S'
/// and the cookie: `welcome-plaintext = server-key cookie`.
pub fn split_welcome_plaintext(
    plaintext: &[u8; KEY_LEN + COOKIE_LEN],
) -> (&[u8; KEY_LEN], &[u8; COOKIE_LEN]) {
    let (key, cookie) = plaintext.split_at(KEY_LEN);
    (array(key), array(cookie))
}

/// Joins the two halves of a `WELCOME` plaintext.
pub fn join_welcome_plaintext(
    server_key: &[u8; KEY_LEN],
    cookie: &[u8; COOKIE_LEN],
) -> [u8; KEY_LEN + COOKIE_LEN] {
    let mut plaintext = [0u8; KEY_LEN + COOKIE_LEN];
    plaintext[..KEY_LEN].copy_from_slice(server_key);
    plaintext[KEY_LEN..].copy_from_slice(cookie);
    plaintext
}

/// Splits a 64-octet two-key plaintext - a cookie's `C' + s'` or a vouch's
/// `C' + S` - into its halves.
pub fn split_key_pair(plaintext: &[u8; 2 * KEY_LEN]) -> (&[u8; KEY_LEN], &[u8; KEY_LEN]) {
    let (first, second) = plaintext.split_at(KEY_LEN);
    (array(first), array(second))
}

/// Joins two keys into the 64-octet plaintext of a cookie or vouch box.
pub fn join_key_pair(first: &[u8; KEY_LEN], second: &[u8; KEY_LEN]) -> [u8; 2 * KEY_LEN] {
    let mut plaintext = [0u8; 2 * KEY_LEN];
    plaintext[..KEY_LEN].copy_from_slice(first);
    plaintext[KEY_LEN..].copy_from_slice(second);
    plaintext
}

/// A slice of statically known length as a reference to an array, for fields
/// the length checks have already proven present.
fn array<const N: usize>(slice: &[u8]) -> &[u8; N] {
    slice[..N].try_into().expect("the length was checked")
}

/// The eight-octet counter half of a short nonce.
fn counter(data: &[u8]) -> u64 {
    u64::from_be_bytes(*array::<SHORT_NONCE_LEN>(data))
}

/// A fixed-size command's length rule.
fn exact(actual: usize, expected: usize, name: &'static str) -> Result<(), CurveError> {
    if actual == expected {
        Ok(())
    } else {
        Err(CurveError::BadLength {
            name,
            expected,
            actual,
        })
    }
}

/// A variable-size command's length rule.
fn least(actual: usize, minimum: usize, name: &'static str) -> Result<(), CurveError> {
    if actual >= minimum {
        Ok(())
    } else {
        Err(CurveError::TooShort {
            name,
            minimum,
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::SocketType;

    const CLIENT_KEY: [u8; KEY_LEN] = [0xC1; KEY_LEN];
    const SIGNATURE: [u8; SIGNATURE_BOX_LEN] = [0x5A; SIGNATURE_BOX_LEN];

    #[test]
    fn the_specifications_own_totals_come_out() {
        // The four counts 26/CURVEZMQ states in prose, derived here from the
        // field lengths instead of copied: if a field length is wrong, one of
        // these fails rather than the whole module quietly disagreeing with
        // libzmq.
        assert_eq!(HELLO_LEN, 200);
        assert_eq!(WELCOME_LEN, 168);
        assert_eq!(INITIATE_MIN_LEN, 257);
        assert_eq!(READY_MIN_LEN, 30);
        assert_eq!(MESSAGE_MIN_LEN, 33);
        // The anti-amplification property that the padding exists for.
        const { assert!(HELLO_LEN > WELCOME_LEN) };
    }

    #[test]
    fn hello_round_trips_with_seventy_two_octets_of_padding() {
        let hello = CurveCommand::Hello {
            client_key: &CLIENT_KEY,
            nonce: 1,
            signature_box: &SIGNATURE,
        };
        let mut body = Vec::new();
        hello.encode_body(&mut body).expect("encode");
        assert_eq!(body.len(), HELLO_LEN);
        assert_eq!(&body[..8], b"\x05HELLO\x01\x00");
        assert!(body[8..8 + HELLO_PADDING_LEN].iter().all(|&b| b == 0));
        assert_eq!(CurveCommand::decode(&body).expect("decode"), hello);
    }

    #[test]
    fn a_hello_written_from_the_prose_is_refused() {
        // 70 octets of padding: 198, every field behind the padding shifted
        // by two. Refusing it is the point - a 200-octet reader that trusted
        // the name would read the signature box out of phase.
        let mut body = Vec::new();
        CurveCommand::Hello {
            client_key: &CLIENT_KEY,
            nonce: 1,
            signature_box: &SIGNATURE,
        }
        .encode_body(&mut body)
        .expect("encode");
        body.drain(8..10);
        assert_eq!(body.len(), 198);
        assert_eq!(
            CurveCommand::decode(&body),
            Err(CurveError::BadLength {
                name: "HELLO",
                expected: 200,
                actual: 198
            })
        );
    }

    #[test]
    fn a_hello_of_another_version_is_refused() {
        let mut body = Vec::new();
        CurveCommand::Hello {
            client_key: &CLIENT_KEY,
            nonce: 1,
            signature_box: &SIGNATURE,
        }
        .encode_body(&mut body)
        .expect("encode");
        body[7] = 1;
        assert_eq!(
            CurveCommand::decode(&body),
            Err(CurveError::UnsupportedVersion(1, 1))
        );
    }

    #[test]
    fn welcome_and_initiate_and_ready_and_message_round_trip() {
        let nonce = [0x11; LONG_NONCE_LEN];
        let welcome_box = [0xB0; WELCOME_BOX_LEN];
        let cookie = [0xCA; COOKIE_LEN];
        let initiate_box = [0x1B; INITIATE_BOX_MIN_LEN];
        let ready_box = [0xBD; READY_BOX_MIN_LEN];
        let message_box = [0xE7; MESSAGE_BOX_MIN_LEN];

        for command in [
            CurveCommand::Welcome {
                nonce: &nonce,
                welcome_box: &welcome_box,
            },
            CurveCommand::Initiate {
                cookie: &cookie,
                nonce: 2,
                initiate_box: &initiate_box,
            },
            CurveCommand::Ready {
                nonce: 3,
                ready_box: &ready_box,
            },
            CurveCommand::Message {
                nonce: u64::MAX,
                message_box: &message_box,
            },
        ] {
            let mut body = Vec::new();
            command.encode_body(&mut body).expect("encode");
            assert_eq!(
                CurveCommand::decode(&body).expect("decode"),
                command,
                "{}",
                command.name()
            );
        }
    }

    #[test]
    fn a_box_below_its_minimum_is_refused_in_both_directions() {
        let cookie = [0xCA; COOKIE_LEN];
        let short = [0u8; INITIATE_BOX_MIN_LEN - 1];
        let initiate = CurveCommand::Initiate {
            cookie: &cookie,
            nonce: 0,
            initiate_box: &short,
        };
        assert_eq!(
            initiate.encode(),
            Err(CurveError::TooShort {
                name: "INITIATE",
                minimum: INITIATE_MIN_LEN,
                actual: INITIATE_MIN_LEN - 1
            })
        );
        let empty: &[u8] = &[];
        assert!(
            CurveCommand::Ready {
                nonce: 0,
                ready_box: empty
            }
            .encode()
            .is_err()
        );
        assert!(
            CurveCommand::Message {
                nonce: 0,
                message_box: empty
            }
            .encode()
            .is_err()
        );
    }

    #[test]
    fn an_initiate_is_always_a_long_frame() {
        let cookie = [0xCA; COOKIE_LEN];
        let initiate_box = [0x1B; INITIATE_BOX_MIN_LEN];
        let frame_bytes = CurveCommand::Initiate {
            cookie: &cookie,
            nonce: 2,
            initiate_box: &initiate_box,
        }
        .encode()
        .expect("encode");
        // COMMAND | LONG, then eight octets of size: 257 does not fit in one.
        assert_eq!(frame_bytes[0], frame::COMMAND | frame::LONG);
        assert_eq!(&frame_bytes[1..9], &257u64.to_be_bytes());
        assert_eq!(frame_bytes.len(), 9 + INITIATE_MIN_LEN);
    }

    #[test]
    fn a_plain_or_null_command_name_is_not_a_curve_one() {
        // Same names, different bodies: `ERROR` belongs to `Command`, and a
        // NULL `READY` full of metadata is not a CURVE `READY`.
        assert_eq!(
            CurveCommand::decode(b"\x05ERROR\x03bad"),
            Err(CurveError::UnknownName)
        );
        assert_eq!(
            CurveCommand::decode(b"\x05READY"),
            Err(CurveError::TooShort {
                name: "READY",
                minimum: 30,
                actual: 6
            })
        );
        assert_eq!(CurveCommand::decode(b""), Err(CurveError::BadName));
        assert_eq!(CurveCommand::decode(b"\x00"), Err(CurveError::BadName));
        assert_eq!(CurveCommand::decode(b"\x05HEL"), Err(CurveError::BadName));
    }

    #[test]
    fn nonces_are_a_prefix_and_a_counter() {
        assert_eq!(
            short_nonce(PREFIX_HELLO, 1),
            *b"CurveZMQHELLO---\x00\x00\x00\x00\x00\x00\x00\x01"
        );
        assert_eq!(
            short_nonce(PREFIX_MESSAGE_SERVER, u64::MAX),
            *b"CurveZMQMESSAGES\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
        );
        assert_eq!(
            long_nonce(PREFIX_COOKIE, &[0x11; LONG_NONCE_LEN]),
            *b"COOKIE--\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11"
        );
        // The two message prefixes differ in exactly one octet, which is what
        // lets both ends count from zero without ever sharing a nonce.
        assert_ne!(PREFIX_MESSAGE_CLIENT, PREFIX_MESSAGE_SERVER);
        for prefix in [
            PREFIX_HELLO,
            PREFIX_INITIATE,
            PREFIX_READY,
            PREFIX_MESSAGE_CLIENT,
            PREFIX_MESSAGE_SERVER,
        ] {
            assert_eq!(prefix.len(), SHORT_NONCE_PREFIX_LEN);
        }
    }

    #[test]
    fn the_initiate_plaintext_round_trips() {
        let vouch = join_nonce_box(&[0x22; LONG_NONCE_LEN], &[0xCB; NONCE_BOX_LEN]);
        let plaintext = InitiatePlaintext {
            client_key: &CLIENT_KEY,
            vouch: &vouch,
            metadata: Metadata::new().with_socket_type(SocketType::Dealer),
        };
        let mut out = Vec::new();
        plaintext.encode(&mut out).expect("encode");
        assert_eq!(&out[..KEY_LEN], &CLIENT_KEY);
        assert_eq!(InitiatePlaintext::decode(&out).expect("decode"), plaintext);

        // No metadata at all is the 128-octet minimum.
        let bare = InitiatePlaintext {
            client_key: &CLIENT_KEY,
            vouch: &vouch,
            metadata: Metadata::new(),
        };
        let mut out = Vec::new();
        bare.encode(&mut out).expect("encode");
        assert_eq!(out.len(), INITIATE_PLAINTEXT_MIN_LEN);
        assert_eq!(InitiatePlaintext::decode(&out).expect("decode"), bare);
        assert!(InitiatePlaintext::decode(&out[..127]).is_err());
    }

    #[test]
    fn the_composite_fields_split_the_way_they_were_joined() {
        let nonce = [0x22; LONG_NONCE_LEN];
        let sealed = [0xCB; NONCE_BOX_LEN];
        let cookie = join_nonce_box(&nonce, &sealed);
        assert_eq!(split_nonce_box(&cookie), (&nonce, &sealed));
        assert_eq!(COOKIE_LEN, VOUCH_LEN);

        let server_key = [0x53; KEY_LEN];
        let welcome = join_welcome_plaintext(&server_key, &cookie);
        assert_eq!(welcome.len(), KEY_LEN + COOKIE_LEN);
        assert_eq!(split_welcome_plaintext(&welcome), (&server_key, &cookie));

        let pair = join_key_pair(&CLIENT_KEY, &server_key);
        assert_eq!(pair.len(), NONCE_BOX_LEN - BOX_OVERHEAD);
        assert_eq!(split_key_pair(&pair), (&CLIENT_KEY, &server_key));
    }
}
