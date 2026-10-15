//! CURVE: the keys, the handshake and the message boxes
//! ([26/CURVEZMQ](https://rfc.zeromq.org/spec/26/),
//! [25/ZMTP-CURVE](https://rfc.zeromq.org/spec/25/)).
//!
//! The octets are [`weida_zmtp::curve`]'s and are not repeated here; this
//! module is the cryptography, the sequencing and the policy. What it adds to
//! the layouts is the four keys, the cookie and its lifetime, the nonce
//! counters, the ZAP credential and the transport that boxes every message
//! after the handshake.
//!
//! # The four keys
//!
//! Per connection, 26/CURVEZMQ names four: the client's long-term pair
//! `C`/`c` and the server's `S`/`s`, plus a **transient** pair on each side,
//! `C'`/`c'` and `S'`/`s'`, generated for this connection and thrown away
//! with it. The long-term keys authenticate; the transient keys encrypt.
//! That separation is what "perfect forward secrecy" means here: "Session
//! keys are held in memory and destroyed when the connection is closed", so
//! a long-term key stolen tomorrow does not open a conversation recorded
//! today ([`CurveTransport::destroy`]).
//!
//! `C` never travels in clear text — it is inside the `INITIATE` box, which
//! is the client-identity protection CURVE claims — and it is what reaches a
//! ZAP handler as the CURVE credential, "a 32-byte long-term public key of
//! the peer being authenticated" (27/ZAP).
//!
//! # The cookie
//!
//! A server answers a `HELLO` with a `WELCOME` and then **keeps nothing**:
//! "It's generated a keypair, sent that back to the client in a way only the
//! client can read, and thrown it away." This implementation does exactly
//! that. The cookie is sealed with `crypto_secretbox` under 32 random octets
//! that only this server holds, for one interval. After
//! [`CurveServer::welcome`] the only state left is that cookie key; `C'` and
//! `s'` come back out of the cookie the client echoes in its `INITIATE`, and
//! the cookie key is dropped the moment that `INITIATE` opens — or when the
//! interval runs out, which 26/CURVEZMQ puts at "a short interval, for
//! example 60 seconds" and this module at [`COOKIE_LIFETIME`]. An expired
//! cookie is refused rather than tolerated: the cookie *is* the anti-DoS
//! design, and a cookie key that never expired would be a long-term key
//! nobody chose.
//!
//! # The three security models
//!
//! 26/CURVEZMQ names three, and all three are reachable from this crate's
//! options without any new knob:
//!
//! 1. **"Where the server does not check client keys at all."**
//!    `ZMQ_CURVE_SERVER` with an empty `ZMQ_ZAP_DOMAIN` **and no ZAP handler
//!    bound**: a handler that is bound is asked about every client whatever
//!    the domain, as libzmq asks it ([`crate::zap::asks_handler`]). Knowing
//!    `S` is the whole of the authorization, which is what a public service
//!    with confidentiality but no authentication wants.
//! 2. **"Where all clients share the same public key, that the server
//!    checks."** A `ZMQ_ZAP_DOMAIN` and a ZAP handler whose table has one
//!    entry. The handler answers 200 or 400 on the key it is given.
//! 3. **"Where each client has its own key… the server can grant access to
//!    clients according to their authenticated identity."** The same domain
//!    and a handler whose table has one entry per client, whose 200 also
//!    names a user id — which this library keeps per connection as
//!    [`crate::ZapUserId`], readable on the peer.
//!
//! Models 2 and 3 are one option surface and one wire: the difference is the
//! handler's table and whether its 200 names a user, which is where the
//! difference belongs, since "the significance of domains are an application
//! issue and not relevant to ZAP". [`SecurityModel`] is the part this library
//! can see, and it says which of the two groups a configuration selects.
//!
//! # Nonces
//!
//! Every box takes a 24-octet nonce, and 26/CURVEZMQ fixes the first 16 (or
//! 8) octets per box kind so that two boxes under the same key cannot collide
//! even at the same counter value — the prefixes are
//! [`weida_zmtp::curve`]'s. What travels is a **counter** for the short form
//! and 16 **random** octets for the long form. The counter here starts at 1
//! and is shared between one side's handshake boxes and its message boxes, as
//! libzmq's is, so a nonce never repeats within a connection; 2^64-1 is the
//! specification's own ceiling and reaching it is an error rather than a
//! wrap. On the way in, a `MESSAGE` whose counter does not exceed the highest
//! seen is refused: replay is one of the attacks 26/CURVEZMQ claims to
//! defend against, and the counter is the only thing that can defend it.
//!
//! # The primitives
//!
//! The Diffie-Hellman step is X25519 from [`curve25519_dalek`], with RFC 7748
//! clamping and nothing else. RustCrypto's `crypto_box`, which the tests use as
//! the reference, reduces the clamped scalar modulo the group order before it
//! multiplies; libsodium, which libzmq uses, does not. For a point of prime
//! order the reduction changes nothing, and every key a legitimate peer sends
//! is such a point, so the two agree on those keys. On a point of small order
//! the reduction is what leaks bits of the secret, so this module does not do
//! it.
//!
//! A box is then NaCl's layout: HSalsa20 over the shared secret with a zero
//! nonce gives the key, XSalsa20-Poly1305 seals under it, and the 16-octet tag
//! comes before the ciphertext, so a box is 16 octets longer than its
//! plaintext. That is [`crypto_secretbox`]'s `XSalsa20Poly1305`, used through
//! its HSalsa20 key derivation and its detached in-place API, which lets a
//! frame be sealed and opened in the buffer it travels in. The tests check the
//! bytes against `crypto_box`, RustCrypto's own NaCl implementation, which is
//! a dev-dependency and not part of the library.
//!
//! # Box reuse
//!
//! A pair of keys makes one box, derived by the first stage that needs it and
//! kept until the next stage replaces it. The `HELLO` signature box and the
//! `WELCOME` box are the same pair, `(s, C')`. The `INITIATE` box, the `READY`
//! box and the transport are the pair `(s', C')` on the server and `(c', S')`
//! on the client. The vouch is the one box of a different pair, `(s', C)` on
//! the server and `(c, S')` on the client, derived once `INITIATE` has named
//! `C`. An unauthenticated `HELLO` therefore costs the server one shared
//! secret, for opening the signature box, and one base-point multiplication,
//! for the transient key pair; the cookie costs no key agreement at all.
//!
//! # Small-order keys
//!
//! A peer key of small order, the zero key among them, makes clamped X25519
//! return an all-zero secret whatever the other key is, so the box it keys
//! has a key anyone can compute. `session_box` refuses that output with
//! `EACCES`, as libsodium's `crypto_scalarmult` does. Every key a peer sends
//! goes through it: `C'` in the `HELLO`, `S'` in the `WELCOME` and `C` in the
//! vouch.
//!
//! # Secrets in memory
//!
//! Every buffer that holds key material is a [`Zeroizing`] array, or is the
//! key of a box, which is wiped when the box is dropped. The long-term secret
//! exists once per [`CurveSecretKey`], shared by its clones; the transient
//! secrets and the cookie key live only as long as the stage that needs them.
//! Safe Rust cannot reach the stack: a secret passed by value, as the X25519
//! scalar is, can leave a copy in a frame that nothing wipes. What is
//! guaranteed is that no heap buffer or long-lived field holding a secret is
//! freed without being wiped first.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crypto_secretbox::aead::OsRng;
use crypto_secretbox::aead::rand_core::RngCore;
use crypto_secretbox::{AeadInPlace, Kdf, Key, KeyInit, Nonce, Tag, XSalsa20Poly1305};
use curve25519_dalek::montgomery::MontgomeryPoint;
use weida_zmtp::curve as layout;
use weida_zmtp::{CurveCommand, FrameKind, InitiatePlaintext, Metadata, frame, z85};
use zeroize::{Zeroize, Zeroizing};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::options::{Security, SocketOptions};

/// A CURVE key, public or secret: 32 octets, "not configurable; they are
/// enforced by the underlying cryptography library".
pub const KEY_LEN: usize = layout::KEY_LEN;

/// A CURVE key as Z85 text: 40 characters, which is what
/// `zmq_curve_keypair` prints and `zmq_setsockopt` accepts beside the 32
/// binary octets.
pub const KEY_TEXT_LEN: usize = z85::KEY_TEXT_LEN;

/// How long a server holds the cookie key for one connection.
///
/// 26/CURVEZMQ: the server "MUST discard \[the cookie key\] after a short
/// interval, for example 60 seconds, or as soon as the client sends a valid
/// INITIATE". 60 seconds is the specification's own example and is also
/// twice `ZMQ_HANDSHAKE_IVL`'s 30 s default, so a handshake that is inside
/// its own budget is never refused by this one.
pub const COOKIE_LIFETIME: Duration = Duration::from_secs(60);

/// The `HELLO` signature box's plaintext: "Box \[64 * %x0\](C'->S)".
const SIGNATURE_PLAINTEXT: [u8; 64] = [0u8; 64];

/// The name block of a `MESSAGE` command: one length octet, then the name.
/// The codec decodes it back, and [`CurveCommand::decode`] is what refuses a
/// command that is not one.
const MESSAGE_NAME_BLOCK: &[u8] = b"\x07MESSAGE";

/// What precedes the box in a `MESSAGE` command: the name block and the short
/// nonce that travels.
const MESSAGE_HEAD_LEN: usize = MESSAGE_NAME_BLOCK.len() + layout::SHORT_NONCE_LEN;

/// Which of 26/CURVEZMQ's security models a configuration selects, as far as
/// this library can see it.
///
/// See the module documentation: the RFC names three, and the second and
/// third are the same options and the same wire, told apart by the handler's
/// table and by whether its 200 names a user id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecurityModel {
    /// Model 1: "the server does not check client keys at all". A CURVE
    /// server with no `ZMQ_ZAP_DOMAIN` and no ZAP handler bound in its
    /// context; every client that knows `S` is admitted, and its key is still
    /// confidential and still authenticated as *some* key.
    AnyClient,
    /// Models 2 and 3: the client's long-term key goes to a ZAP handler,
    /// which either holds one shared key or one key per client and may name
    /// a user id for the connection.
    CheckedByHandler,
}

/// What security model a socket's options select in `context`, or `None`
/// where the socket is not a CURVE server and therefore checks nobody.
///
/// Needs the context because a handler bound there is what turns a server
/// without a domain from model 1 into a checked one.
pub fn security_model(options: &SocketOptions, context: &Context) -> Option<SecurityModel> {
    match options.security() {
        Security::CurveServer if crate::zap::asks_handler(options, context) => {
            Some(SecurityModel::CheckedByHandler)
        }
        Security::CurveServer => Some(SecurityModel::AnyClient),
        _ => None,
    }
}

/// A CURVE public key: a long-term `C`/`S` or a transient `C'`/`S'`.
///
/// Printable, because a public key is meant to be published — `Debug` and
/// `Display` both write the 40-character Z85 form, which is the form
/// `zmq_curve_keypair` prints and configuration files carry.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CurvePublicKey([u8; KEY_LEN]);

impl CurvePublicKey {
    /// A key from its 32 octets.
    pub const fn from_bytes(bytes: [u8; KEY_LEN]) -> CurvePublicKey {
        CurvePublicKey(bytes)
    }

    /// A key as `zmq_setsockopt` takes one: "32 binary bytes or 40-character
    /// Z85".
    ///
    /// # Errors
    ///
    /// `EINVAL` for any other length, or for 40 characters that are not Z85,
    /// naming both accepted forms.
    pub fn parse(option: &[u8]) -> Result<CurvePublicKey> {
        parse_key(option).map(CurvePublicKey)
    }

    /// The 32 octets.
    pub const fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// The 40-character Z85 form.
    pub fn to_z85(&self) -> String {
        z85::encode_key(&self.0)
    }
}

impl fmt::Debug for CurvePublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CurvePublicKey({})", self.to_z85())
    }
}

impl fmt::Display for CurvePublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_z85())
    }
}

/// A CURVE secret key: a long-term `c`/`s`.
///
/// Deliberately not printable and deliberately not `Copy`: `Debug` writes a
/// placeholder, because a secret key in a log is a secret key on a disk, and
/// comparison is constant-time, because `SocketOptions` derives `PartialEq`
/// and an early-exiting comparison there would be a timing oracle nobody
/// asked for.
///
/// `Clone` shares the key instead of copying it: the octets exist once, in a
/// buffer that is wiped when the last clone is dropped, and the options that
/// every connection takes a copy of hold a reference. The public key is
/// derived when the key is built, so asking for it costs nothing.
#[derive(Clone)]
pub struct CurveSecretKey(Arc<Inner>);

/// What the clones of a [`CurveSecretKey`] share.
struct Inner {
    secret: Zeroizing<[u8; KEY_LEN]>,
    public: CurvePublicKey,
}

impl CurveSecretKey {
    /// A key from its 32 octets. The public key is derived here, once.
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> CurveSecretKey {
        CurveSecretKey::from_secret(Zeroizing::new(bytes))
    }

    /// A key as `zmq_setsockopt` takes one: 32 binary octets or 40
    /// characters of Z85.
    ///
    /// # Errors
    ///
    /// `EINVAL`, as [`CurvePublicKey::parse`].
    pub fn parse(option: &[u8]) -> Result<CurveSecretKey> {
        parse_key(option).map(CurveSecretKey::from_bytes)
    }

    fn from_secret(secret: Zeroizing<[u8; KEY_LEN]>) -> CurveSecretKey {
        let public = CurvePublicKey(public_key_of(&secret));
        CurveSecretKey(Arc::new(Inner { secret, public }))
    }

    /// The public key this secret key belongs to.
    ///
    /// Which is why `ZMQ_CURVE_PUBLICKEY` is optional on a server: libzmq's
    /// manual says a server "does not need to know its own public key", and
    /// that is because X25519 derives it.
    pub fn public_key(&self) -> CurvePublicKey {
        self.0.public
    }

    /// The 40-character Z85 form. Named so that writing a secret key down is
    /// a deliberate call and never a `Display`. The text is a copy this crate
    /// does not wipe: it belongs to the caller from the moment it returns.
    pub fn to_z85(&self) -> String {
        z85::encode_key(&self.0.secret)
    }

    fn secret_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0.secret
    }

    /// Whether two keys are one allocation, which is what a clone is.
    #[cfg(test)]
    fn shares_allocation_with(&self, other: &CurveSecretKey) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for CurveSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CurveSecretKey(<secret>)")
    }
}

impl PartialEq for CurveSecretKey {
    fn eq(&self, other: &CurveSecretKey) -> bool {
        let mut difference = 0u8;
        for (ours, theirs) in self.0.secret.iter().zip(other.0.secret.iter()) {
            difference |= ours ^ theirs;
        }
        difference == 0
    }
}

impl Eq for CurveSecretKey {}

/// A fresh CURVE key pair, as `zmq_curve_keypair` returns one.
pub fn keypair() -> (CurvePublicKey, CurveSecretKey) {
    let secret = CurveSecretKey::from_secret(random_key());
    (secret.public_key(), secret)
}

/// 32 binary octets or 40 characters of Z85, and nothing else.
fn parse_key(option: &[u8]) -> Result<[u8; KEY_LEN]> {
    match option.len() {
        KEY_LEN => Ok(option.try_into().expect("32 octets")),
        KEY_TEXT_LEN => {
            let text = std::str::from_utf8(option).map_err(|_| {
                Error::EINVAL(
                    "a 40-octet CURVE key is Z85 text, and this one is not UTF-8 at all".into(),
                )
            })?;
            z85::decode_key(text).map_err(|e| {
                Error::EINVAL(format!("a 40-character CURVE key must be Z85: {e}").into())
            })
        }
        other => Err(Error::EINVAL(
            format!(
                "a CURVE key is {KEY_LEN} binary octets or {KEY_TEXT_LEN} characters of Z85; \
                 this one is {other} octets"
            )
            .into(),
        )),
    }
}

/// The short-nonce counter of one direction.
///
/// Starts at 1, never repeats, and reaching 2^64-1 is the specification's own
/// ceiling: "SHALL NOT send more than 2^64-1 commands in one connection".
#[derive(Debug)]
struct Counter(u64);

impl Counter {
    const fn new() -> Counter {
        Counter(1)
    }

    fn take(&mut self) -> Result<u64> {
        let value = self.0;
        self.0 = self.0.checked_add(1).ok_or_else(|| {
            Error::ENOCOMPATPROTO(
                "this connection has sent 2^64-1 CURVE commands, which is the ceiling \
                 26/CURVEZMQ sets; a further box would repeat a nonce"
                    .into(),
            )
        })?;
        Ok(value)
    }
}

/// The box of one pair of keys: `crypto_box`'s construction after its key
/// agreement, an XSalsa20-Poly1305 keyed by HSalsa20 of the shared secret. It
/// wipes its key when it is dropped.
type SessionBox = XSalsa20Poly1305;

/// 32 octets from the operating system, in a buffer that is wiped on drop.
fn random_key() -> Zeroizing<[u8; KEY_LEN]> {
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    OsRng.fill_bytes(&mut *key);
    key
}

/// The X25519 shared secret of `secret` and `peer`, with RFC 7748 clamping.
///
/// The result is not checked here: [`session_box`] is the caller that keys a
/// box from it, and it checks first.
fn x25519(secret: &[u8; KEY_LEN], peer: &[u8; KEY_LEN]) -> Zeroizing<[u8; KEY_LEN]> {
    Zeroizing::new(MontgomeryPoint(*peer).mul_clamped(*secret).to_bytes())
}

/// The public key of `secret`: X25519 of the base point.
fn public_key_of(secret: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
    MontgomeryPoint::mul_base_clamped(*secret).to_bytes()
}

/// The box of `secret` and `peer`: NaCl's `crypto_box` construction, with the
/// shared secret and the key derived from it wiped on every path.
///
/// # Errors
///
/// `EACCES` if the shared secret is all zero, which clamped X25519 produces
/// for a small-order peer key whatever the secret is: the box would then be
/// keyed by a constant anyone can compute. libsodium's `crypto_scalarmult`
/// refuses the same output.
fn session_box(secret: &[u8; KEY_LEN], peer: &[u8; KEY_LEN]) -> Result<SessionBox> {
    let shared = x25519(secret, peer);
    // Every octet is folded in before the test, so the time taken does not
    // depend on where the first non-zero octet is.
    let nonzero = shared.iter().fold(0u8, |acc, octet| acc | octet);
    if nonzero == 0 {
        return Err(Error::EACCES(
            "the peer's public key is a small-order public key: its X25519 shared secret is \
             all zero, which every such key produces"
                .into(),
        ));
    }
    // HSalsa20 of the shared secret under a zero nonce: the key `crypto_box`
    // derives from the shared secret, which is not uniform by itself.
    let mut key = <SessionBox as Kdf>::kdf(Key::from_slice(&shared[..]), &Default::default());
    let boxed = SessionBox::new(&key);
    key.as_mut_slice().zeroize();
    Ok(boxed)
}

/// The cookie key a server holds between its `WELCOME` and one `INITIATE`.
///
/// One random key per connection, which is what 26/CURVEZMQ asks for: the
/// cookie is opaque to every peer, so the server needs nothing but a secret
/// of its own to seal it with.
struct CookieKey {
    key: XSalsa20Poly1305,
    /// When this key stops being accepted. The other half of "or as soon as
    /// the client sends a valid INITIATE".
    expires: Instant,
}

impl CookieKey {
    fn new(lifetime: Duration) -> CookieKey {
        let key = random_key();
        CookieKey {
            key: XSalsa20Poly1305::new(Key::from_slice(&key[..])),
            expires: Instant::now() + lifetime,
        }
    }
}

/// The client half of a CURVE handshake.
///
/// One per connection, because the transient pair is one per connection.
pub struct CurveClient {
    /// `C` and `c`: the identity a ZAP handler will be told about.
    long_public: CurvePublicKey,
    long_secret: CurveSecretKey,
    /// `S`: the server this client will talk to, and nobody else.
    server_key: [u8; KEY_LEN],
    /// `C'` and `c'`.
    transient_public: [u8; KEY_LEN],
    transient_secret: Zeroizing<[u8; KEY_LEN]>,
    /// `(c', S)`, derived by the `HELLO` and kept until the `WELCOME` opens.
    hello_box: Option<SessionBox>,
    /// `S'`, learned from the `WELCOME` box.
    server_transient: Option<[u8; KEY_LEN]>,
    /// `(c', S')`: the box the `INITIATE`, the `READY` and the transport run
    /// on, derived when the `WELCOME` is read.
    session: Option<SessionBox>,
    /// The cookie, held only between `WELCOME` and `INITIATE`.
    cookie: Option<[u8; layout::COOKIE_LEN]>,
    counter: Counter,
}

impl CurveClient {
    /// A client with its long-term pair and the server's public key.
    pub fn new(
        long_public: CurvePublicKey,
        long_secret: &CurveSecretKey,
        server_key: CurvePublicKey,
    ) -> CurveClient {
        let transient_secret = random_key();
        let transient_public = public_key_of(&transient_secret);
        CurveClient {
            long_public,
            long_secret: long_secret.clone(),
            server_key: *server_key.as_bytes(),
            transient_public,
            transient_secret,
            hello_box: None,
            server_transient: None,
            session: None,
            cookie: None,
            counter: Counter::new(),
        }
    }

    /// The `HELLO` frame: `C'`, a counter nonce and 64 zero octets sealed to
    /// the server's long-term key, which is the proof that this client knows
    /// `S`.
    ///
    /// # Errors
    ///
    /// `EACCES` if the server key is of small order; `ENOCOMPATPROTO` if the
    /// nonce counter is exhausted; `EIO` if the box cannot be sealed, which
    /// needs a broken cryptography library.
    pub fn hello(&mut self) -> Result<Vec<u8>> {
        let nonce = self.counter.take()?;
        let hello_box = session_box(&self.transient_secret, &self.server_key)?;
        let signature_box = seal(
            &hello_box,
            &layout::short_nonce(layout::PREFIX_HELLO, nonce),
            &SIGNATURE_PLAINTEXT,
        )?;
        let signature_box =
            sized::<{ layout::SIGNATURE_BOX_LEN }>(&signature_box, "HELLO signature")?;
        // The WELCOME opens under this box, so it is kept until then.
        self.hello_box = Some(hello_box);
        CurveCommand::Hello {
            client_key: &self.transient_public,
            nonce,
            signature_box: &signature_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// Reads a `WELCOME`: opens the box with `c'` and `S`, and keeps `S'`, the
    /// cookie and the box of `(c', S')`.
    ///
    /// # Errors
    ///
    /// `EFSM` if no `HELLO` has been sent. `EACCES` if the box does not open —
    /// which means the peer does not hold `s`, so it is not the server this
    /// client was configured for — or if `S'` is of small order.
    /// `ENOCOMPATPROTO` for a malformed command.
    pub fn read_welcome(&mut self, body: &[u8]) -> Result<()> {
        let CurveCommand::Welcome { nonce, welcome_box } =
            CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("WELCOME", body));
        };
        let Some(hello_box) = &self.hello_box else {
            return Err(Error::EFSM(
                "a WELCOME is opened with the box a HELLO derives, and no HELLO has been sent"
                    .into(),
            ));
        };
        let plaintext = sized::<{ layout::KEY_LEN + layout::COOKIE_LEN }>(
            &open(
                hello_box,
                &layout::long_nonce(layout::PREFIX_WELCOME, nonce),
                welcome_box,
                "WELCOME",
            )?,
            "WELCOME",
        )?;
        let (server_transient, cookie) = layout::split_welcome_plaintext(&plaintext);
        let session = session_box(&self.transient_secret, server_transient)?;
        self.server_transient = Some(*server_transient);
        self.cookie = Some(*cookie);
        self.session = Some(session);
        self.hello_box = None;
        Ok(())
    }

    /// The `INITIATE` frame: the cookie echoed, and `C`, the vouch and this
    /// socket's metadata sealed to `S'`.
    ///
    /// # Errors
    ///
    /// `EFSM` if no `WELCOME` has been read; `EACCES` if the vouch key is of
    /// small order; otherwise as [`Self::hello`].
    pub fn initiate(&mut self, metadata: &Metadata<'_>) -> Result<Vec<u8>> {
        let (Some(server_transient), Some(cookie), Some(session)) =
            (self.server_transient, self.cookie, self.session.as_ref())
        else {
            return Err(Error::EFSM(
                "an INITIATE needs the S' and the cookie that a WELCOME carries".into(),
            ));
        };
        // `vouch = vouch-nonce vouch-box`, `Box [C',S](C->S')`: the client's
        // long-term key signs the pair, so a vouch collected by one server
        // cannot be replayed at another.
        let vouch_key = session_box(self.long_secret.secret_bytes(), &server_transient)?;
        let mut vouch_nonce = [0u8; layout::LONG_NONCE_LEN];
        OsRng.fill_bytes(&mut vouch_nonce);
        let vouch_box = seal(
            &vouch_key,
            &layout::long_nonce(layout::PREFIX_VOUCH, &vouch_nonce),
            &layout::join_key_pair(&self.transient_public, &self.server_key),
        )?;
        let vouch = layout::join_nonce_box(
            &vouch_nonce,
            &sized::<{ layout::NONCE_BOX_LEN }>(&vouch_box, "vouch")?,
        );

        let mut plaintext = Vec::new();
        InitiatePlaintext {
            client_key: self.long_public.as_bytes(),
            vouch: &vouch,
            metadata: metadata.clone(),
        }
        .encode(&mut plaintext)
        .map_err(curve_error)?;

        let nonce = self.counter.take()?;
        let initiate_box = seal(
            session,
            &layout::short_nonce(layout::PREFIX_INITIATE, nonce),
            &plaintext,
        )?;
        // The cookie is the server's memory, not ours: it goes back and this
        // side forgets it.
        self.cookie = None;
        CurveCommand::Initiate {
            cookie: &cookie,
            nonce,
            initiate_box: &initiate_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// Reads a `READY` and returns the server's metadata dictionary.
    ///
    /// # Errors
    ///
    /// `EACCES` if the box does not open, `ENOCOMPATPROTO` for a malformed
    /// command or a `READY` before the `WELCOME`.
    pub fn read_ready(&mut self, body: &[u8]) -> Result<Vec<u8>> {
        let Some(session) = &self.session else {
            return Err(unexpected("WELCOME", body));
        };
        let CurveCommand::Ready { nonce, ready_box } =
            CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("READY", body));
        };
        let mut metadata = open(
            session,
            &layout::short_nonce(layout::PREFIX_READY, nonce),
            ready_box,
            "READY",
        )?;
        // The metadata dictionary is not key material, so its buffer leaves
        // the wipe policy here, as the caller's own bytes.
        Ok(std::mem::take(&mut *metadata))
    }

    /// The message transport this handshake agreed: `C'` and `S'`, with the
    /// client's own nonce prefix and the counter it has reached.
    ///
    /// # Errors
    ///
    /// `EFSM` if the handshake did not get as far as a `WELCOME`.
    pub fn into_transport(self) -> Result<CurveTransport> {
        let Some(session) = self.session else {
            return Err(Error::EFSM(
                "a CURVE transport needs the S' a WELCOME carries".into(),
            ));
        };
        Ok(CurveTransport {
            boxed: Some(session),
            send_prefix: layout::PREFIX_MESSAGE_CLIENT,
            recv_prefix: layout::PREFIX_MESSAGE_SERVER,
            send: Counter(self.counter.0),
            highest_seen: 0,
        })
    }
}

/// What a server learned from one `INITIATE`.
pub struct OpenedInitiate {
    /// The client's long-term public key `C` — the ZAP credential, and the
    /// only thing an authorization decision can be made on.
    pub client_key: CurvePublicKey,
    /// The opened box, whose metadata section is [`Self::metadata`]. It was
    /// opened under session keys, so it is wiped when it is dropped.
    plaintext: Zeroizing<Vec<u8>>,
}

impl OpenedInitiate {
    /// The client's connection metadata, where NULL puts it in `READY`.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` for a malformed property dictionary.
    pub fn metadata(&self) -> Result<Metadata<'_>> {
        Ok(InitiatePlaintext::decode(&self.plaintext)
            .map_err(curve_error)?
            .metadata)
    }

    /// The metadata section of the opened box, as the property dictionary
    /// octets. The length was checked when the box was opened, so the
    /// section is present even when it is empty.
    pub fn metadata_bytes(&self) -> &[u8] {
        &self.plaintext[layout::KEY_LEN + layout::VOUCH_LEN..]
    }
}

impl fmt::Debug for OpenedInitiate {
    /// The client key and nothing else: the opened box also holds the vouch,
    /// and a vouch in a log is a box somebody kept.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenedInitiate")
            .field("client_key", &self.client_key)
            .finish_non_exhaustive()
    }
}

/// What `read_hello` keeps for `welcome`: the client's transient key and the
/// box of `(s, C')`, which opened the `HELLO` and seals the `WELCOME`.
struct HelloState {
    client_transient: [u8; KEY_LEN],
    boxed: SessionBox,
}

/// The server half of a CURVE handshake.
pub struct CurveServer {
    /// `s`. The public key `S` is derived from it once, when the key was
    /// built: "a server does not need to know its own public key".
    long_secret: CurveSecretKey,
    /// How long a cookie key lives once it is made. A parameter rather than
    /// a constant so that the expiry can be tested without waiting a minute.
    cookie_lifetime: Duration,
    /// `C'` and the box of `(s, C')`, held only between the `HELLO` and the
    /// `WELCOME`.
    hello: Option<HelloState>,
    /// The cookie key, held only between the `WELCOME` and a valid
    /// `INITIATE`.
    cookie: Option<CookieKey>,
    /// The box of `(s', C')`, recovered from the cookie by a valid
    /// `INITIATE`: what the `READY` and the transport run on.
    session: Option<SessionBox>,
    counter: Counter,
}

impl CurveServer {
    /// A server holding its long-term secret key.
    pub fn new(long_secret: &CurveSecretKey, cookie_lifetime: Duration) -> CurveServer {
        CurveServer {
            long_secret: long_secret.clone(),
            cookie_lifetime,
            hello: None,
            cookie: None,
            session: None,
            counter: Counter::new(),
        }
    }

    /// Reads a `HELLO`: opens the signature box with `s`, which proves the
    /// client knows `S`, and keeps `C'` and the box that opened it for the
    /// `WELCOME`.
    ///
    /// # Errors
    ///
    /// `EACCES` if the client key is of small order, if the signature box does
    /// not open, or if it does not hold the 64 zero octets: the peer does not
    /// know this server's public key. `ENOCOMPATPROTO` for a malformed or
    /// mis-sized command — including the 198-octet `HELLO` that 26/CURVEZMQ's
    /// prose describes and its own grammar contradicts.
    pub fn read_hello(&mut self, body: &[u8]) -> Result<()> {
        let CurveCommand::Hello {
            client_key,
            nonce,
            signature_box,
        } = CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("HELLO", body));
        };
        let boxed = session_box(self.long_secret.secret_bytes(), client_key)?;
        let signature = open(
            &boxed,
            &layout::short_nonce(layout::PREFIX_HELLO, nonce),
            signature_box,
            "HELLO",
        )?;
        if signature.as_slice() != &SIGNATURE_PLAINTEXT[..] {
            return Err(Error::EACCES(
                "the HELLO signature box does not hold the 64 zero octets 26/CURVEZMQ \
                 specifies"
                    .into(),
            ));
        }
        self.hello = Some(HelloState {
            client_transient: *client_key,
            boxed,
        });
        Ok(())
    }

    /// The `WELCOME` frame, and the moment this server forgets the
    /// connection: `S'` and the cookie go to the client, and all that is left
    /// here is the cookie key.
    ///
    /// # Errors
    ///
    /// `EFSM` if no `HELLO` has been read.
    pub fn welcome(&mut self) -> Result<Vec<u8>> {
        let Some(hello) = self.hello.take() else {
            return Err(Error::EFSM(
                "a WELCOME needs the C' that a HELLO carries".into(),
            ));
        };
        let transient_secret = random_key();
        let transient_public = public_key_of(&transient_secret);

        // `cookie = cookie-nonce cookie-box`, holding C' and s' so that the
        // server can keep neither.
        let cookie_key = CookieKey::new(self.cookie_lifetime);
        let mut cookie_nonce = [0u8; layout::LONG_NONCE_LEN];
        OsRng.fill_bytes(&mut cookie_nonce);
        let cookie_plaintext = Zeroizing::new(layout::join_key_pair(
            &hello.client_transient,
            &transient_secret,
        ));
        let cookie_box = seal(
            &cookie_key.key,
            &layout::long_nonce(layout::PREFIX_COOKIE, &cookie_nonce),
            &cookie_plaintext[..],
        )?;
        let cookie = layout::join_nonce_box(
            &cookie_nonce,
            &sized::<{ layout::NONCE_BOX_LEN }>(&cookie_box, "cookie")?,
        );
        self.cookie = Some(cookie_key);

        // `Box [S' + cookie](S->C')`, under the box the HELLO derived.
        let mut welcome_nonce = [0u8; layout::LONG_NONCE_LEN];
        OsRng.fill_bytes(&mut welcome_nonce);
        let welcome_box = seal(
            &hello.boxed,
            &layout::long_nonce(layout::PREFIX_WELCOME, &welcome_nonce),
            &layout::join_welcome_plaintext(&transient_public, &cookie),
        )?;
        let welcome_box = sized::<{ layout::WELCOME_BOX_LEN }>(&welcome_box, "WELCOME")?;
        CurveCommand::Welcome {
            nonce: &welcome_nonce,
            welcome_box: &welcome_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// Reads an `INITIATE`: opens the cookie to get `C'` and `s'` back, opens
    /// the box, checks the vouch, and **discards the cookie key**.
    ///
    /// # Errors
    ///
    /// `EACCES` for a cookie that has expired, a cookie this server did not
    /// seal, a box that does not open, a client key of small order, or a vouch
    /// that does not name this connection's `C'` and this server's `S` — the
    /// last one being the replay a vouch exists to prevent. `ENOCOMPATPROTO`
    /// for a malformed command.
    pub fn read_initiate(&mut self, body: &[u8]) -> Result<OpenedInitiate> {
        let CurveCommand::Initiate {
            cookie,
            nonce,
            initiate_box,
        } = CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("INITIATE", body));
        };
        let cookie_key = self.cookie.as_ref().ok_or_else(|| {
            Error::EACCES(
                "this INITIATE has no cookie key to open it: either no WELCOME was sent or \
                 the cookie was already spent"
                    .into(),
            )
        })?;
        if Instant::now() > cookie_key.expires {
            // Dropped here as well as on success: an expired key must not
            // survive the connection that could not use it in time.
            self.cookie = None;
            return Err(Error::EACCES(
                format!(
                    "the cookie key expired: 26/CURVEZMQ requires it to be discarded after a \
                     short interval, and this one lived {COOKIE_LIFETIME:?}"
                )
                .into(),
            ));
        }
        let (cookie_nonce, cookie_box) = layout::split_nonce_box(cookie);
        let cookie_plaintext = Zeroizing::new(sized::<{ 2 * layout::KEY_LEN }>(
            &open(
                &cookie_key.key,
                &layout::long_nonce(layout::PREFIX_COOKIE, cookie_nonce),
                cookie_box,
                "cookie",
            )?,
            "cookie",
        )?);
        let (client_transient, transient_secret) = layout::split_key_pair(&cookie_plaintext);
        let session = session_box(transient_secret, client_transient)?;

        let plaintext = open(
            &session,
            &layout::short_nonce(layout::PREFIX_INITIATE, nonce),
            initiate_box,
            "INITIATE",
        )?;
        let initiate = InitiatePlaintext::decode(&plaintext).map_err(curve_error)?;
        let client_key = *initiate.client_key;

        // `Box [C',S](C->S')`: opened with `s'` and `C`, and its plaintext
        // must name this connection's C' and this server's S.
        let vouch_session = session_box(transient_secret, &client_key)?;
        let (vouch_nonce, vouch_box) = layout::split_nonce_box(initiate.vouch);
        let vouch = sized::<{ 2 * layout::KEY_LEN }>(
            &open(
                &vouch_session,
                &layout::long_nonce(layout::PREFIX_VOUCH, vouch_nonce),
                vouch_box,
                "vouch",
            )?,
            "vouch",
        )?;
        let (vouched_transient, vouched_server) = layout::split_key_pair(&vouch);
        if vouched_transient != client_transient
            || vouched_server != self.long_secret.public_key().as_bytes()
        {
            return Err(Error::EACCES(
                "the vouch box names another connection's transient key or another server, \
                 which is the replay the vouch exists to refuse"
                    .into(),
            ));
        }

        // "or as soon as the client sends a valid INITIATE": the cookie key
        // has done its work and must not open a second one.
        self.cookie = None;
        self.session = Some(session);
        Ok(OpenedInitiate {
            client_key: CurvePublicKey(client_key),
            plaintext,
        })
    }

    /// The `READY` frame: this socket's metadata, sealed to `C'`.
    ///
    /// # Errors
    ///
    /// `EFSM` if no valid `INITIATE` has been read.
    pub fn ready(&mut self, metadata: &Metadata<'_>) -> Result<Vec<u8>> {
        let Some(session) = &self.session else {
            return Err(Error::EFSM(
                "a READY needs the session keys a valid INITIATE recovers".into(),
            ));
        };
        let mut plaintext = Vec::new();
        metadata.encode(&mut plaintext).map_err(metadata_error)?;
        let nonce = self.counter.take()?;
        let ready_box = seal(
            session,
            &layout::short_nonce(layout::PREFIX_READY, nonce),
            &plaintext,
        )?;
        CurveCommand::Ready {
            nonce,
            ready_box: &ready_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// The message transport this handshake agreed.
    ///
    /// # Errors
    ///
    /// `EFSM` if no valid `INITIATE` has been read.
    pub fn into_transport(self) -> Result<CurveTransport> {
        let Some(session) = self.session else {
            return Err(Error::EFSM(
                "a CURVE transport needs the session keys a valid INITIATE recovers".into(),
            ));
        };
        Ok(CurveTransport {
            boxed: Some(session),
            send_prefix: layout::PREFIX_MESSAGE_SERVER,
            recv_prefix: layout::PREFIX_MESSAGE_CLIENT,
            send: Counter(self.counter.0),
            highest_seen: 0,
        })
    }

    /// Whether the cookie key is still held. The observable half of "the
    /// server keeps no state for an unauthenticated client".
    pub const fn holds_cookie(&self) -> bool {
        self.cookie.is_some()
    }
}

/// The session keys of one established CURVE connection, and the nonce
/// counters that go with them.
///
/// Every frame after the handshake travels as a `MESSAGE` command whose box
/// holds a flags octet and the frame body — application messages and the
/// commands between them alike, because a CURVE connection has no plaintext
/// after its `READY`.
pub struct CurveTransport {
    /// The box of this connection's session keys. `None` once destroyed,
    /// which drops the box and wipes its key.
    boxed: Option<SessionBox>,
    send_prefix: &'static [u8; layout::SHORT_NONCE_PREFIX_LEN],
    recv_prefix: &'static [u8; layout::SHORT_NONCE_PREFIX_LEN],
    send: Counter,
    /// The highest counter seen from the peer. A `MESSAGE` that does not
    /// exceed it is a replay.
    highest_seen: u64,
}

impl fmt::Debug for CurveTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurveTransport")
            .field("alive", &self.boxed.is_some())
            .field("sent", &self.send.0)
            .field("highest_seen", &self.highest_seen)
            .finish_non_exhaustive()
    }
}

impl CurveTransport {
    /// Seals one frame: the flags octet and the body, as one `MESSAGE` ready
    /// for the wire, in a buffer of its own.
    ///
    /// # Errors
    ///
    /// As [`Self::seal_frame_into`].
    pub fn seal_frame(&mut self, flags: u8, body: &[u8]) -> Result<Vec<u8>> {
        let mut frame = Vec::new();
        self.seal_frame_into(flags, body, &mut frame)?;
        Ok(frame)
    }

    /// Appends one complete wire frame to `out`: a message-frame header, then
    /// the `MESSAGE` command — `\x07MESSAGE`, the nonce counter, the 16-octet
    /// tag and the ciphertext of `flags || body`.
    ///
    /// The frame takes one reservation of exactly its size, the body is copied
    /// once, and the box is encrypted where it lies, with the tag written into
    /// its slot.
    ///
    /// **In a message frame, not a command frame.** 26/CURVEZMQ calls
    /// `MESSAGE` a command and the body is a command body — the name, the
    /// nonce and the box — but libzmq 4.3.5 puts it behind a **message**
    /// frame header, flags without the COMMAND bit, and refuses the
    /// command-framed form: measured in `tests/interop_libzmq.rs`, where a
    /// CURVE handshake with libzmq succeeds and the first command-framed
    /// `MESSAGE` closes the connection. The outer MORE flag stays zero
    /// because the real one is the flags octet inside the box. Reading
    /// accepts either kind ([`Self::open_message_in_place`] takes a body).
    ///
    /// The nonce counter is taken before anything is written. A failure after
    /// that point leaves a gap in the counter, which the peer accepts because
    /// it requires only that counters increase, and never a reused nonce. On
    /// error `out` is truncated back to the length it had.
    ///
    /// # Errors
    ///
    /// `ENOTSOCK` after [`Self::destroy`], `ENOCOMPATPROTO` at the counter
    /// ceiling, `EIO` if the box cannot be sealed.
    pub fn seal_frame_into(&mut self, flags: u8, body: &[u8], out: &mut Vec<u8>) -> Result<()> {
        let boxed = live(&self.boxed)?;
        let nonce = self.send.take()?;
        let command_len = MESSAGE_HEAD_LEN + layout::BOX_OVERHEAD + 1 + body.len();
        let header_len = if command_len as u64 > frame::MAX_SHORT_BODY {
            9
        } else {
            2
        };
        let start = out.len();
        out.reserve_exact(header_len + command_len);
        frame::encode_header(FrameKind::Message { more: false }, command_len as u64, out);
        out.extend_from_slice(MESSAGE_NAME_BLOCK);
        out.extend_from_slice(&nonce.to_be_bytes());
        let tag_at = out.len();
        out.resize(tag_at + layout::BOX_OVERHEAD, 0);
        out.push(flags);
        out.extend_from_slice(body);

        let nonce_bytes = layout::short_nonce(self.send_prefix, nonce);
        let ciphertext_at = tag_at + layout::BOX_OVERHEAD;
        match boxed.encrypt_in_place_detached(
            Nonce::from_slice(&nonce_bytes),
            &[],
            &mut out[ciphertext_at..],
        ) {
            Ok(tag) => {
                out[tag_at..ciphertext_at].copy_from_slice(tag.as_slice());
                Ok(())
            }
            Err(_) => {
                out.truncate(start);
                Err(seal_error())
            }
        }
    }

    /// Opens one `MESSAGE` command body, returning the flags octet and the
    /// frame body it held.
    ///
    /// This is [`Self::open_message_in_place`] on a copy of `body`, with the
    /// header of the box dropped from the copy.
    ///
    /// # Errors
    ///
    /// As [`Self::open_message_in_place`].
    pub fn open_message(&mut self, body: &[u8]) -> Result<(u8, Vec<u8>)> {
        let mut command_body = body.to_vec();
        let (flags, start) = self.open_message_in_place(&mut command_body)?;
        command_body.drain(..start);
        Ok((flags, command_body))
    }

    /// Opens a `MESSAGE` command in place. `command_body` is the command body
    /// as it came off the wire, and it is decrypted where it lies. Returns the
    /// flags octet and the offset at which the frame body starts; the frame
    /// body runs to the end of `command_body`.
    ///
    /// The counter is checked before any decryption, and `highest_seen`
    /// advances only once the box has opened. The tag is checked before the
    /// keystream is applied, so a box that does not open is left as it came
    /// in, and the counter is not moved by it.
    ///
    /// # Errors
    ///
    /// `ENOTSOCK` after [`Self::destroy`]. `ENOCOMPATPROTO` for a command that
    /// is not a `MESSAGE` or whose counter does not exceed the highest seen: a
    /// repeated nonce is a replay, and 26/CURVEZMQ names replay among the
    /// attacks it defends against. `EACCES` if the box does not open.
    pub fn open_message_in_place(&mut self, command_body: &mut [u8]) -> Result<(u8, usize)> {
        let boxed = live(&self.boxed)?;
        let nonce = match CurveCommand::decode(command_body).map_err(curve_error)? {
            CurveCommand::Message { nonce, .. } => nonce,
            _ => {
                return Err(Error::ENOCOMPATPROTO(
                    "a CURVE connection carries MESSAGE commands after its READY, and this is \
                     something else"
                        .into(),
                ));
            }
        };
        if nonce <= self.highest_seen {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "the peer's MESSAGE nonce {nonce} does not exceed the {} already seen, \
                     so it is a replay or a restarted counter",
                    self.highest_seen
                )
                .into(),
            ));
        }
        let (_, sealed) = command_body
            .split_at_mut_checked(MESSAGE_HEAD_LEN)
            .ok_or_else(short_message)?;
        let (tag, ciphertext) = sealed
            .split_at_mut_checked(layout::BOX_OVERHEAD)
            .ok_or_else(short_message)?;
        let nonce_bytes = layout::short_nonce(self.recv_prefix, nonce);
        boxed
            .decrypt_in_place_detached(
                Nonce::from_slice(&nonce_bytes),
                &[],
                ciphertext,
                Tag::from_slice(tag),
            )
            .map_err(|_| open_error("MESSAGE"))?;
        self.highest_seen = nonce;
        let flags = *ciphertext.first().ok_or_else(short_message)?;
        Ok((flags, MESSAGE_HEAD_LEN + layout::BOX_OVERHEAD + 1))
    }

    /// Destroys the session keys: "Session keys are held in memory and
    /// destroyed when the connection is closed."
    ///
    /// Called when the connection ends. The box is dropped, which wipes its
    /// key, and afterwards nothing can be sealed or opened on this transport,
    /// which is what makes the destruction observable rather than a claim
    /// about memory.
    pub fn destroy(&mut self) {
        self.boxed = None;
        self.send = Counter(0);
        self.highest_seen = 0;
    }

    /// How many boxes this side has sealed, counting the handshake's.
    pub const fn sent(&self) -> u64 {
        self.send.0
    }
}

/// The box of a transport that has not been destroyed.
fn live(boxed: &Option<SessionBox>) -> Result<&SessionBox> {
    boxed.as_ref().ok_or_else(|| {
        Error::ENOTSOCK("this connection's CURVE session keys have been destroyed".into())
    })
}

/// A box, sealed: the 16-octet tag, then the ciphertext, which is NaCl's
/// layout.
fn seal(boxed: &SessionBox, nonce: &[u8; layout::NONCE_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
    let mut sealed = Vec::with_capacity(layout::BOX_OVERHEAD + plaintext.len());
    sealed.resize(layout::BOX_OVERHEAD, 0);
    sealed.extend_from_slice(plaintext);
    let tag = boxed
        .encrypt_in_place_detached(
            Nonce::from_slice(nonce),
            &[],
            &mut sealed[layout::BOX_OVERHEAD..],
        )
        .map_err(|_| seal_error())?;
    sealed[..layout::BOX_OVERHEAD].copy_from_slice(tag.as_slice());
    Ok(sealed)
}

/// A box, opened — or `EACCES`, because a box that does not open is a peer
/// that does not hold the key it claimed. The plaintext is returned in a
/// buffer that is wiped when it is dropped.
fn open(
    boxed: &SessionBox,
    nonce: &[u8; layout::NONCE_LEN],
    sealed: &[u8],
    what: &str,
) -> Result<Zeroizing<Vec<u8>>> {
    let (tag, ciphertext) = sealed
        .split_at_checked(layout::BOX_OVERHEAD)
        .ok_or_else(|| open_error(what))?;
    let mut plaintext = Zeroizing::new(ciphertext.to_vec());
    boxed
        .decrypt_in_place_detached(
            Nonce::from_slice(nonce),
            &[],
            &mut plaintext,
            Tag::from_slice(tag),
        )
        .map_err(|_| open_error(what))?;
    Ok(plaintext)
}

fn seal_error() -> Error {
    Error::EIO("the cryptography library refused to seal a CURVE box".into())
}

fn open_error(what: &str) -> Error {
    Error::EACCES(
        format!(
            "the {what} box does not open: the peer does not hold the key it would need, or \
             the octets were altered"
        )
        .into(),
    )
}

fn short_message() -> Error {
    Error::ENOCOMPATPROTO("a MESSAGE command is shorter than its box".into())
}

/// A box or plaintext of exactly the length its layout states.
fn sized<const N: usize>(bytes: &[u8], what: &str) -> Result<[u8; N]> {
    bytes.try_into().map_err(|_| {
        Error::ENOCOMPATPROTO(
            format!(
                "a CURVE {what} is {N} octets and this one is {}",
                bytes.len()
            )
            .into(),
        )
    })
}

fn unexpected(want: &str, body: &[u8]) -> Error {
    let got = CurveCommand::decode(body)
        .map(|command| command.name().to_owned())
        .unwrap_or_else(|_| "something unreadable".to_owned());
    Error::ENOCOMPATPROTO(format!("expected a CURVE {want}, got {got}").into())
}

fn curve_error(error: weida_zmtp::CurveError) -> Error {
    Error::ENOCOMPATPROTO(format!("malformed CURVE command: {error}").into())
}

/// A metadata dictionary that will not encode, which is a property this
/// socket set rather than anything a peer sent.
fn metadata_error(error: weida_zmtp::CommandError) -> Error {
    Error::ENOCOMPATPROTO(format!("malformed CURVE metadata: {error}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_zmtp::SocketType;

    /// A handshake driven to completion, as the session does it.
    fn handshake() -> (CurveTransport, CurveTransport) {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        client.read_welcome(&welcome).expect("read WELCOME");
        let initiate = body(
            &client
                .initiate(&Metadata::new().with_socket_type(SocketType::Req))
                .expect("INITIATE"),
        );
        let opened = server.read_initiate(&initiate).expect("read INITIATE");
        assert_eq!(opened.client_key, client_public);
        let ready = body(
            &server
                .ready(&Metadata::new().with_socket_type(SocketType::Rep))
                .expect("READY"),
        );
        client.read_ready(&ready).expect("read READY");

        (
            client.into_transport().expect("client transport"),
            server.into_transport().expect("server transport"),
        )
    }

    /// The command body of a frame this module produced.
    fn body(frame_bytes: &[u8]) -> Vec<u8> {
        let (_, body, used) = weida_zmtp::frame::decode(frame_bytes, 1 << 20).expect("a frame");
        assert_eq!(used, frame_bytes.len());
        body.to_vec()
    }

    /// The octets of 64 hex digits.
    fn hex32(text: &str) -> [u8; KEY_LEN] {
        assert_eq!(text.len(), 2 * KEY_LEN, "64 hex digits");
        let mut octets = [0u8; KEY_LEN];
        for (i, octet) in octets.iter_mut().enumerate() {
            *octet = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).expect("hex digits");
        }
        octets
    }

    /// An encoding of a field element: `first` and then 0xff, with the top
    /// bit clear, which is how the non-canonical forms of p-1, p and p+1 are
    /// written.
    fn field_encoding(first: u8) -> [u8; KEY_LEN] {
        let mut octets = [0xffu8; KEY_LEN];
        octets[0] = first;
        octets[KEY_LEN - 1] = 0x7f;
        octets
    }

    /// The small-order encodings of Curve25519 a peer could send: the zero
    /// key, u = 1, the two points of order eight, and the non-canonical
    /// encodings of p-1, p and p+1 (the field elements -1, 0 and 1).
    fn small_order_keys() -> [[u8; KEY_LEN]; 7] {
        let mut one = [0u8; KEY_LEN];
        one[0] = 1;
        [
            [0u8; KEY_LEN],
            one,
            hex32("e0eb7a7c3b41b8ae1656e3faf19fc46ada098deb9c32b1fd866205165f49b800"),
            hex32("5f9c95bca3508c24b1d0b1559c83ef5b04445cc4581c8e86d8224eddd09f1157"),
            field_encoding(0xec),
            field_encoding(0xed),
            field_encoding(0xee),
        ]
    }

    /// The box a naive implementation keys from an all-zero shared secret:
    /// HSalsa20 of zero, built with `crypto_secretbox` directly.
    fn box_keyed_by_zero_secret() -> SessionBox {
        let zero = [0u8; KEY_LEN];
        let key = <SessionBox as Kdf>::kdf(Key::from_slice(&zero), &Default::default());
        SessionBox::new(&key)
    }

    /// Claim: a whole handshake runs, both sides derive the same session
    /// keys, and a message sealed by one opens on the other.
    #[test]
    fn the_handshake_agrees_on_session_keys() {
        let (mut client, mut server) = handshake();

        let sealed = client.seal_frame(0, b"hello").expect("seal");
        let (flags, payload) = server.open_message(&body(&sealed)).expect("open");
        assert_eq!(flags, 0);
        assert_eq!(payload, b"hello");

        let sealed = server.seal_frame(1, b"more").expect("seal");
        let (flags, payload) = client.open_message(&body(&sealed)).expect("open");
        assert_eq!(flags, 1, "the MORE flag travels inside the box");
        assert_eq!(payload, b"more");
    }

    /// Claim: a nonce never repeats within a session — not across the
    /// handshake and the messages, and not between the two directions.
    #[test]
    fn a_nonce_never_repeats_within_a_session() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        // Every nonce this connection puts on the wire, **prefix included**,
        // because the prefix is what keeps a HELLO and a MESSAGE apart at the
        // same counter value and the two directions apart at the same one.
        let mut seen: Vec<[u8; layout::NONCE_LEN]> = Vec::new();
        let mut record = |frame: &[u8], from_client: bool| {
            let body = body(frame);
            let nonce = match CurveCommand::decode(&body).expect("a CURVE command") {
                CurveCommand::Hello { nonce, .. } => {
                    layout::short_nonce(layout::PREFIX_HELLO, nonce)
                }
                CurveCommand::Welcome { nonce, .. } => {
                    layout::long_nonce(layout::PREFIX_WELCOME, nonce)
                }
                CurveCommand::Initiate { nonce, .. } => {
                    layout::short_nonce(layout::PREFIX_INITIATE, nonce)
                }
                CurveCommand::Ready { nonce, .. } => {
                    layout::short_nonce(layout::PREFIX_READY, nonce)
                }
                CurveCommand::Message { nonce, .. } => layout::short_nonce(
                    if from_client {
                        layout::PREFIX_MESSAGE_CLIENT
                    } else {
                        layout::PREFIX_MESSAGE_SERVER
                    },
                    nonce,
                ),
            };
            seen.push(nonce);
        };

        let hello = client.hello().expect("HELLO");
        record(&hello, true);
        server.read_hello(&body(&hello)).expect("read HELLO");
        let welcome = server.welcome().expect("WELCOME");
        record(&welcome, false);
        client.read_welcome(&body(&welcome)).expect("read WELCOME");
        let initiate = client.initiate(&Metadata::new()).expect("INITIATE");
        record(&initiate, true);
        server.read_initiate(&body(&initiate)).expect("read");
        let ready = server.ready(&Metadata::new()).expect("READY");
        record(&ready, false);
        client.read_ready(&body(&ready)).expect("read READY");

        let mut client_transport = client.into_transport().expect("client transport");
        let mut server_transport = server.into_transport().expect("server transport");
        for i in 0..64u8 {
            let from_client = client_transport.seal_frame(0, &[i]).expect("seal");
            record(&from_client, true);
            server_transport
                .open_message(&body(&from_client))
                .expect("open");
            let from_server = server_transport.seal_frame(0, &[i]).expect("seal");
            record(&from_server, false);
            client_transport
                .open_message(&body(&from_server))
                .expect("open");
        }

        let total = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), total, "a nonce repeated within the session");
        assert_eq!(total, 4 + 2 * 64);
    }

    /// Claim: the counter is what refuses a replay, in both directions.
    #[test]
    fn a_replayed_message_is_refused() {
        let (mut client, mut server) = handshake();
        let sealed = body(&client.seal_frame(0, b"once").expect("seal"));
        server.open_message(&sealed).expect("the first arrival");
        let err = server.open_message(&sealed).unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
        assert!(err.cause().contains("replay"), "{err}");
    }

    /// Claim: the cookie key is discarded by a valid `INITIATE`, and a second
    /// `INITIATE` cannot be opened.
    #[test]
    fn the_cookie_is_spent_by_a_valid_initiate() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        assert!(!server.holds_cookie(), "no cookie before a WELCOME");
        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        assert!(server.holds_cookie(), "the WELCOME made one");
        client.read_welcome(&welcome).expect("read WELCOME");
        let initiate = body(&client.initiate(&Metadata::new()).expect("INITIATE"));
        server.read_initiate(&initiate).expect("read INITIATE");
        assert!(
            !server.holds_cookie(),
            "a valid INITIATE spends the cookie key"
        );

        let err = server.read_initiate(&initiate).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
        assert!(err.cause().contains("already spent"), "{err}");
    }

    /// Claim: a cookie the interval has run out on is refused, which is the
    /// other half of 26/CURVEZMQ's rule.
    #[test]
    fn an_expired_cookie_is_refused() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        // Zero lifetime: the cookie is stale by the time the INITIATE
        // arrives, which is the same code path as sixty seconds later.
        let mut server = CurveServer::new(&server_secret, Duration::ZERO);

        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        client.read_welcome(&welcome).expect("read WELCOME");
        let initiate = body(&client.initiate(&Metadata::new()).expect("INITIATE"));
        let err = server.read_initiate(&initiate).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
        assert!(err.cause().contains("expired"), "{err}");
        assert!(!server.holds_cookie(), "and the stale key is gone");
    }

    /// Claim: a cookie sealed for one connection does not open under the key
    /// of another, so the cookie is bound to the server that made it.
    #[test]
    fn a_cookie_does_not_open_under_another_connections_key() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let hello = body(&client.hello().expect("HELLO"));

        let mut first = CurveServer::new(&server_secret, COOKIE_LIFETIME);
        let mut second = CurveServer::new(&server_secret, COOKIE_LIFETIME);
        first.read_hello(&hello).expect("read HELLO");
        second.read_hello(&hello).expect("read HELLO");
        let welcome = body(&first.welcome().expect("WELCOME"));
        second.welcome().expect("the second WELCOME");
        client.read_welcome(&welcome).expect("read WELCOME");
        let initiate = body(&client.initiate(&Metadata::new()).expect("INITIATE"));

        // The cookie in this INITIATE is the first server's, so the second
        // server's key must not open it.
        let err = second.read_initiate(&initiate).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
        first
            .read_initiate(&initiate)
            .expect("the server that sealed the cookie opens it");
    }

    /// Claim: a client that does not know `S` cannot get past `HELLO`, which
    /// is what the signature box is for.
    #[test]
    fn a_client_with_the_wrong_server_key_is_refused() {
        let (_, server_secret) = keypair();
        let (other_public, _) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, other_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        let hello = body(&client.hello().expect("HELLO"));
        let err = server.read_hello(&hello).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
    }

    /// Claim: a vouch that names another server is refused. That is the
    /// deviation from CurveCP — `Box [C',S](C->S')` rather than
    /// `Box [C'](C->S)` — doing its job.
    #[test]
    fn a_vouch_for_another_server_is_refused() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        // The client knows the real S for its HELLO, then vouches for a
        // different one: a man in the middle relaying a vouch it collected
        // elsewhere.
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        client.read_welcome(&welcome).expect("read WELCOME");
        let (elsewhere, _) = keypair();
        client.server_key = *elsewhere.as_bytes();
        let initiate = body(&client.initiate(&Metadata::new()).expect("INITIATE"));

        let err = server.read_initiate(&initiate).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
        assert!(err.cause().contains("vouch"), "{err}");
    }

    /// Claim: a `HELLO` whose transient key is of small order is refused, even
    /// with a signature box that a naive implementation would open, because
    /// that box is keyed by the all-zero shared secret every such key yields.
    #[test]
    fn a_hello_with_a_small_order_key_is_refused() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        for key in small_order_keys() {
            let zero_box = box_keyed_by_zero_secret();
            let nonce = layout::short_nonce(layout::PREFIX_HELLO, 1);
            let signature = sized::<{ layout::SIGNATURE_BOX_LEN }>(
                &seal(&zero_box, &nonce, &SIGNATURE_PLAINTEXT).expect("seal"),
                "HELLO signature",
            )
            .expect("80 octets");
            // The premise of the refusal: the box does open under the zero key.
            open(&zero_box, &nonce, &signature, "HELLO")
                .expect("the zero-keyed box opens, so only the small-order rule refuses it");

            let mut hello = Vec::new();
            CurveCommand::Hello {
                client_key: &key,
                nonce: 1,
                signature_box: &signature,
            }
            .encode_body(&mut hello)
            .expect("a HELLO");
            let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);
            let err = server.read_hello(&hello).unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
        }

        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let hello = body(&client.hello().expect("HELLO"));
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);
        server
            .read_hello(&hello)
            .expect("a HELLO from a real key is accepted");
    }

    /// Claim: a `WELCOME` whose transient server key `S'` is of small order is
    /// refused by the client, so no box is keyed from a constant.
    #[test]
    fn a_welcome_with_a_small_order_server_key_is_refused() {
        for key in small_order_keys() {
            let (server_public, server_secret) = keypair();
            let (client_public, client_secret) = keypair();
            let mut client = CurveClient::new(client_public, &client_secret, server_public);
            client.hello().expect("HELLO");

            // The WELCOME a server holding `s` sends to this client, with the
            // small-order key inside. Its box is `(s, C')`, which the client
            // derives too, from its own side of the same pair.
            let boxed = session_box(server_secret.secret_bytes(), &client.transient_public)
                .expect("the box of (s, C')");
            let nonce = [0x5a; layout::LONG_NONCE_LEN];
            let welcome_box = sized::<{ layout::WELCOME_BOX_LEN }>(
                &seal(
                    &boxed,
                    &layout::long_nonce(layout::PREFIX_WELCOME, &nonce),
                    &layout::join_welcome_plaintext(&key, &[0u8; layout::COOKIE_LEN]),
                )
                .expect("seal"),
                "WELCOME",
            )
            .expect("144 octets");
            let mut welcome = Vec::new();
            CurveCommand::Welcome {
                nonce: &nonce,
                welcome_box: &welcome_box,
            }
            .encode_body(&mut welcome)
            .expect("a WELCOME");

            let err = client.read_welcome(&welcome).unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
        }
    }

    /// Claim: an `INITIATE` whose client key `C` is of small order is refused
    /// by the server at the vouch, so the server never trusts a constant key.
    #[test]
    fn a_vouch_with_a_small_order_client_key_is_refused() {
        for key in small_order_keys() {
            let (server_public, server_secret) = keypair();
            let (client_public, client_secret) = keypair();
            let mut client = CurveClient::new(client_public, &client_secret, server_public);
            let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);
            let hello = body(&client.hello().expect("HELLO"));
            server.read_hello(&hello).expect("read HELLO");
            let welcome = body(&server.welcome().expect("WELCOME"));
            client.read_welcome(&welcome).expect("read WELCOME");

            // An INITIATE a naive implementation would accept in full: the vouch
            // is sealed under the box keyed by the all-zero secret and names this
            // connection's C' and the server's S. Only the small-order `C` is
            // wrong, so the refusal has to come from the key check.
            let server_transient = client.server_transient.expect("S'");
            let session = session_box(&client.transient_secret, &server_transient)
                .expect("the box of (c', S')");
            let vouch_nonce = [0x33u8; layout::LONG_NONCE_LEN];
            let vouch_box = seal(
                &box_keyed_by_zero_secret(),
                &layout::long_nonce(layout::PREFIX_VOUCH, &vouch_nonce),
                &layout::join_key_pair(&client.transient_public, server_public.as_bytes()),
            )
            .expect("seal");
            open(
                &box_keyed_by_zero_secret(),
                &layout::long_nonce(layout::PREFIX_VOUCH, &vouch_nonce),
                &vouch_box,
                "vouch",
            )
            .expect("the premise: a naive implementation would open this vouch");
            let vouch = layout::join_nonce_box(
                &vouch_nonce,
                &sized::<{ layout::NONCE_BOX_LEN }>(&vouch_box, "vouch").expect("80 octets"),
            );
            let mut plaintext = Vec::new();
            InitiatePlaintext {
                client_key: &key,
                vouch: &vouch,
                metadata: Metadata::new(),
            }
            .encode(&mut plaintext)
            .expect("an INITIATE plaintext");
            let initiate_box = seal(
                &session,
                &layout::short_nonce(layout::PREFIX_INITIATE, 2),
                &plaintext,
            )
            .expect("seal");
            let cookie = client.cookie.expect("the cookie");
            let mut initiate = Vec::new();
            CurveCommand::Initiate {
                cookie: &cookie,
                nonce: 2,
                initiate_box: &initiate_box,
            }
            .encode_body(&mut initiate)
            .expect("an INITIATE");

            let err = server.read_initiate(&initiate).unwrap_err();
            assert_eq!(err.errno(), "EACCES", "{err}");
        }
    }

    /// Claim: destroyed session keys seal and open nothing.
    #[test]
    fn destroyed_session_keys_carry_no_more_messages() {
        let (mut client, mut server) = handshake();
        let sealed = body(&client.seal_frame(0, b"before").expect("seal"));
        server.open_message(&sealed).expect("open");

        client.destroy();
        let err = client.seal_frame(0, b"after").unwrap_err();
        assert_eq!(err.errno(), "ENOTSOCK", "{err}");
        server.destroy();
        let err = server.open_message(&sealed).unwrap_err();
        assert_eq!(err.errno(), "ENOTSOCK", "{err}");
    }

    /// Claim: a transport seals into a buffer and opens in place. The body
    /// sits where the returned offset says, a forged box neither opens nor
    /// moves the replay counter, and a replayed box is refused.
    #[test]
    fn a_transport_seals_and_opens_in_place() {
        let (mut client, mut server) = handshake();

        // Two frames appended to one buffer. The second is long enough for the
        // long size field, so both header forms are exercised.
        let mut wire = Vec::new();
        client
            .seal_frame_into(layout::MESSAGE_FLAG_MORE, b"in place", &mut wire)
            .expect("seal");
        let first_end = wire.len();
        let long = vec![0xa5u8; 1000];
        client.seal_frame_into(0, &long, &mut wire).expect("seal");

        let (_, command, used) =
            weida_zmtp::frame::decode(&wire[..first_end], 1 << 20).expect("the first frame");
        assert_eq!(used, first_end);
        let mut first = command.to_vec();
        let (flags, at) = server
            .open_message_in_place(&mut first)
            .expect("open the first frame");
        assert_eq!(
            flags,
            layout::MESSAGE_FLAG_MORE,
            "the MORE flag travels inside the box"
        );
        assert_eq!(&first[at..], b"in place");

        let (_, command, used) =
            weida_zmtp::frame::decode(&wire[first_end..], 1 << 20).expect("the second frame");
        assert_eq!(used, wire.len() - first_end);
        let mut second = command.to_vec();
        let (flags, at) = server
            .open_message_in_place(&mut second)
            .expect("open the second frame");
        assert_eq!(flags, 0);
        assert_eq!(&second[at..], &long[..]);

        // A frame whose ciphertext has one flipped bit: refused, and the
        // genuine box still opens afterwards because the counter did not move.
        let mut wire = Vec::new();
        client
            .seal_frame_into(0, b"after a forgery", &mut wire)
            .expect("seal");
        let (_, command, _) = weida_zmtp::frame::decode(&wire, 1 << 20).expect("a frame");
        let genuine = command.to_vec();
        let mut forged = genuine.clone();
        let last = forged.len() - 1;
        forged[last] ^= 0x01;
        let err = server.open_message_in_place(&mut forged).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");

        let mut opened = genuine.clone();
        let (_, at) = server
            .open_message_in_place(&mut opened)
            .expect("the genuine box opens after a forgery");
        assert_eq!(&opened[at..], b"after a forgery");

        // The same octets again are a replay, refused before any decryption.
        let err = server
            .open_message_in_place(&mut genuine.clone())
            .unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
    }

    /// Claim: a `CurveSecretKey` clone shares the key's allocation instead of
    /// copying the secret.
    #[test]
    fn a_secret_key_clone_shares_its_allocation() {
        let (_, secret) = keypair();
        let copy = secret.clone();
        assert!(
            copy.shares_allocation_with(&secret),
            "a clone shares the key"
        );
        let (_, other) = keypair();
        assert!(!other.shares_allocation_with(&secret));
    }

    /// Claim: X25519 is RFC 7748 — the two vectors of section 5.2, and the
    /// public keys and shared secret of section 6.1 — so the Diffie-Hellman
    /// step agrees with every other implementation of the function.
    #[test]
    fn x25519_matches_rfc_7748() {
        assert_eq!(
            *x25519(
                &hex32("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4"),
                &hex32("e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c"),
            ),
            hex32("c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552"),
        );
        assert_eq!(
            *x25519(
                &hex32("4b66e9d4d1b4673c5ad22691957d6af5c11b6421e0ea01d42ca4169e7918ba0d"),
                &hex32("e5210f12786811d3f4b7959d0538ae2c31dbe7106fc03c3efc4cd549c715a493"),
            ),
            hex32("95cbde9476e8907d7aade45cb4b873f88b595a68799fa152e6f8f7647aac7957"),
        );

        let alice = hex32("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        let bob = hex32("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
        assert_eq!(
            public_key_of(&alice),
            hex32("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"),
        );
        assert_eq!(
            public_key_of(&bob),
            hex32("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f"),
        );
        let shared = hex32("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742");
        assert_eq!(*x25519(&alice, &public_key_of(&bob)), shared);
        assert_eq!(*x25519(&bob, &public_key_of(&alice)), shared);
    }

    /// Claim: the local box is `crypto_box`'s box byte for byte, so a box
    /// sealed here opens in the reference implementation and the reverse.
    #[test]
    fn the_local_box_is_nacls_crypto_box() {
        use crypto_box::aead::Aead;

        for _ in 0..8 {
            let (public_a, secret_a) = keypair();
            let (public_b, secret_b) = keypair();
            let nonce = [0x42u8; layout::NONCE_LEN];
            let message = b"a message both implementations seal and open";

            let reference_ab = crypto_box::SalsaBox::new(
                &crypto_box::PublicKey::from(*public_b.as_bytes()),
                &crypto_box::SecretKey::from(*secret_a.secret_bytes()),
            );
            let local_ab =
                session_box(secret_a.secret_bytes(), public_b.as_bytes()).expect("a box");
            let sealed = seal(&local_ab, &nonce, message).expect("seal");
            assert_eq!(
                sealed,
                reference_ab
                    .encrypt(crypto_box::Nonce::from_slice(&nonce), &message[..])
                    .expect("crypto_box seals"),
                "the same octets, tag first"
            );

            let reference_ba = crypto_box::SalsaBox::new(
                &crypto_box::PublicKey::from(*public_a.as_bytes()),
                &crypto_box::SecretKey::from(*secret_b.secret_bytes()),
            );
            let sealed_by_reference = reference_ba
                .encrypt(crypto_box::Nonce::from_slice(&nonce), &message[..])
                .expect("crypto_box seals");
            let local_ba =
                session_box(secret_b.secret_bytes(), public_a.as_bytes()).expect("a box");
            let opened = open(&local_ba, &nonce, &sealed_by_reference, "crypto_box")
                .expect("the local box opens the reference box");
            assert_eq!(opened.as_slice(), &message[..]);
        }
    }

    /// Claim: a key is 32 binary octets or 40 characters of Z85, and
    /// anything else is refused with both forms named.
    #[test]
    fn a_key_is_binary_or_z85() {
        let (public, secret) = keypair();
        let text = public.to_z85();
        assert_eq!(text.len(), KEY_TEXT_LEN);
        assert_eq!(
            CurvePublicKey::parse(text.as_bytes()).expect("Z85"),
            public,
            "the Z85 form parses to the same key"
        );
        assert_eq!(
            CurvePublicKey::parse(public.as_bytes()).expect("binary"),
            public
        );
        assert_eq!(
            CurveSecretKey::parse(secret.to_z85().as_bytes()).expect("Z85"),
            secret
        );

        for wrong in [&b""[..], &[0u8; 31][..], &[0u8; 33][..], &[0u8; 39][..]] {
            let err = CurvePublicKey::parse(wrong).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");
            assert!(err.cause().contains("40"), "{err}");
        }
        // Forty octets that are not Z85: the length is right and the
        // alphabet is not.
        let err = CurvePublicKey::parse(&[b'"'; 40]).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
        assert!(err.cause().contains("Z85"), "{err}");
    }

    /// Claim: a secret key does not print itself, and a public key does.
    #[test]
    fn a_secret_key_is_not_printable() {
        let (public, secret) = keypair();
        assert!(!format!("{secret:?}").contains(&secret.to_z85()));
        assert_eq!(format!("{public}"), public.to_z85());
        assert!(format!("{public:?}").contains(&public.to_z85()));
    }

    /// Claim: a server derives its own public key, since libzmq's manual
    /// says it "does not need to know" it.
    #[test]
    fn a_server_derives_its_own_public_key() {
        let (public, secret) = keypair();
        assert_eq!(secret.public_key(), public);
    }
}
