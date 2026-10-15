//! How a connection is configured, and every bound it carries.
//!
//! Each field is a field of `open` under the name Part 2 §2.7.1 gives it, or
//! a local bound the protocol does not supply. The difference matters and is
//! stated per field, because AMQP's own defaults are the least safe part of
//! it: a peer accepting them has agreed to a single frame of four gigabytes,
//! 65536 sessions, 2^32 links per session and a connection that never times
//! out (Part 2 §2.4.5, §2.7.1-2.7.3). Only three limits in the protocol are
//! safe by construction — `incoming-window` and `outgoing-window`, which are
//! mandatory, and `link-credit`, which starts at zero.
//!
//! So this type changes three defaults away from the specification's, and
//! says so:
//!
//! | Field | Specification | Here | Why |
//! | --- | --- | --- | --- |
//! | `max_frame_size` | `4294967295` (no limit) | [`DEFAULT_MAX_FRAME_SIZE`] | A four-gigabyte frame is a four-gigabyte allocation on a peer's word |
//! | `idle_time_out` | unset (never) | [`DEFAULT_IDLE_TIME_OUT`] | A connection that never times out holds its resources until the OS notices |
//! | `channel_max` | `65535` | [`DEFAULT_CHANNEL_MAX`] | Each session is a live table entry, and 65536 of them is a number nobody asked for |
//!
//! Two bounds are ours where the protocol has none and are not changed
//! defaults, because the specification has nothing to change them from:
//! [`ConnectionOptions::handshake_timeout`] for every step before `open` has
//! been read, and [`ConnectionOptions::write_timeout`] for a peer that stops
//! reading once the connection is up.
//!
//! All three changed defaults are settable back to the specification's
//! value, and none of them is silent.

use std::fmt;
use std::time::Duration;

use weida_amqp_codec::frame::MIN_MAX_FRAME_SIZE;

use crate::error::{Error, Result};

/// Turns a list of owned strings into the wire form a `multiple` field takes.
///
/// Shared by `open`, `begin`, `attach` and both termini, because all six of
/// them carry capability lists and Part 1 §1.4's rule — one value is a bare
/// symbol, several are an array, none is null — is the same for all of them.
pub(crate) fn multiple(items: &[String]) -> weida_amqp_codec::Multiple<'_> {
    use weida_amqp_codec::Multiple;
    match items {
        [] => Multiple::None,
        [one] => Multiple::One(one),
        many => Multiple::Many(many.iter().map(String::as_str).collect()),
    }
}

/// This client's `max-frame-size`, deliberately not the specification's
/// default of `4294967295`.
///
/// 128 KiB, which is what Artemis advertises
/// (`MAX_FRAME_SIZE_DEFAULT = 128 * 1024`) and comfortably above Dispatch's
/// 16384 and Service Bus's 262144 for Standard. Large enough that an ordinary
/// message is one frame, small enough that a frame is a bounded read.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 128 * 1024;

/// This client's `idle-time-out`, deliberately not "unset".
///
/// 60 seconds, the value Artemis's `connectionTTL` uses. What this client
/// *advertises* is half of it, because Part 2 §2.4.5 says the advertised
/// value SHOULD be half the local threshold to avoid spurious expiry — see
/// [`ConnectionOptions::advertised_idle_time_out`].
pub const DEFAULT_IDLE_TIME_OUT: Duration = Duration::from_secs(60);

/// This client's `channel-max`, deliberately not the specification's 65535.
///
/// 255, so 256 simultaneous sessions. **This is the protocol's own bound on
/// the session table**: the table can never hold more entries than
/// `channel-max + 1`, so naming a small number here is what makes the table
/// bounded rather than merely large.
pub const DEFAULT_CHANNEL_MAX: u16 = 255;

/// How long each step of the handshake may take.
///
/// The protocol gives no deadline for any of them — not for the answering
/// protocol header, not for `sasl-mechanisms`, not for the peer's `open` —
/// so a server that accepts a TCP connection and then says nothing would
/// hold a client forever. This is ours, and it is finite by construction.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// How long `close` waits for the peer's answering `close`.
///
/// Part 2 §2.4.3: the sender of `close` SHOULD keep reading until the
/// partner's `close` arrives and SHOULD apply a timeout before dropping the
/// transport. The specification names no number; this is it.
pub const DEFAULT_CLOSE_BUDGET: Duration = Duration::from_secs(5);

/// How long the socket may take no octets at all while output waits for it.
///
/// **Ours, because the protocol gives a writer no deadline.** A peer that
/// keeps the connection open and stops reading fills the kernel's send
/// buffer, after which a write waits for as long as the peer chooses. The
/// clock restarts whenever the socket takes anything, so a slow reader is not
/// a stalled one; only no progress at all for this long fails the connection
/// with [`Error::WriteTimeout`]. Shorter than [`DEFAULT_IDLE_TIME_OUT`] so
/// that a peer that has stopped reading is found before one that has merely
/// gone quiet.
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Addresses a hostname may expand to.
///
/// A resolver answer is remote input, so the count is capped
/// (`docs/INVARIANTS.md`). `localhost` routinely yields two.
pub const DEFAULT_MAX_RESOLVED_ADDRESSES: usize = 8;

/// The IANA port for AMQP: `PORT` (Part 2 §2.8.19).
pub const PORT: u16 = 5672;

/// The IANA port for TLS-first AMQP: `SECURE-PORT`, the bare `amqps`
/// listener (Part 2 §2.8.19, Part 5 §5.2.1).
pub const SECURE_PORT: u16 = 5671;

/// Which SASL mechanism this client will use, and with what.
///
/// The client "MUST authenticate using the highest-level security profile it
/// can handle from the list provided by the partner" (Part 5 §5.3), so the
/// choice is between *this* setting and what the server offered: a client
/// configured for `PLAIN` against a server offering only `ANONYMOUS` is a
/// configuration error rather than a silent downgrade.
///
/// `Debug` is written by hand and prints `PLAIN`'s password as
/// `"<redacted>"`: this type is held by [`ConnectionOptions`], which a
/// [`Connection`](crate::Connection) keeps for its whole life, so a derived
/// `Debug` would put the password into every `{:?}`, `dbg!` and `tracing`
/// field that touches either.
#[derive(Clone, PartialEq, Eq, Default)]
pub enum Sasl {
    /// No SASL layer at all: the header is `AMQP %d0 1.0.0` and `open`
    /// follows immediately.
    ///
    /// A server that requires SASL answers with `%d3`, which this client
    /// reports as [`Error::SecurityLayerRequired`]
    /// rather than retrying.
    #[default]
    None,
    /// `ANONYMOUS`: what a server not requiring authentication SHOULD
    /// advertise, and what Service Bus needs in order to defer authorization
    /// to its `$cbs` node.
    Anonymous,
    /// `PLAIN`, whose whole exchange is one NUL-separated initial response.
    ///
    /// The mechanism carries the password in the clear, so it belongs inside
    /// TLS. This client does not refuse it outside TLS — RabbitMQ on a
    /// loopback socket is a legitimate deployment — but it does not hide the
    /// fact either.
    Plain {
        /// The authentication identity.
        username: String,
        /// The password.
        password: String,
    },
    /// `EXTERNAL`: the identity comes from the layer below, in practice a TLS
    /// client certificate. RabbitMQ reaches x.509 authentication through it.
    External {
        /// The optional authorization identity. Empty means "whatever the
        /// lower layer says".
        authzid: String,
    },
}

impl Sasl {
    /// The mechanism name on the wire, or `None` for no SASL layer.
    #[must_use]
    pub const fn mechanism(&self) -> Option<&'static str> {
        Some(match self {
            Self::None => return None,
            Self::Anonymous => weida_amqp_codec::sasl::ANONYMOUS,
            Self::Plain { .. } => weida_amqp_codec::sasl::PLAIN,
            Self::External { .. } => weida_amqp_codec::sasl::EXTERNAL,
        })
    }
}

impl fmt::Debug for Sasl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Anonymous => f.write_str("Anonymous"),
            Self::Plain { username, .. } => f
                .debug_struct("Plain")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::External { authzid } => f
                .debug_struct("External")
                .field("authzid", authzid)
                .finish(),
        }
    }
}

/// How TLS is reached, where it is reached at all.
///
/// Part 5 §5.2 gives two ways and they are not interchangeable: one announces
/// itself in the clear and one does not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// No TLS.
    #[default]
    None,
    /// In-band: exchange `AMQP %d2 1.0.0`, negotiate TLS, then exchange
    /// `AMQP %d0 1.0.0` *inside* it. Two header exchanges, the second
    /// encrypted.
    Layered,
    /// Out-of-band: a pure TLS listener, conventionally on 5671, where the
    /// handshake happens first and no header precedes it. The documented
    /// alternative to `%d2`, and what `amqps` means.
    Direct,
}

/// What this client tells the peer about itself, and every bound it applies.
#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    /// `open.container-id`, mandatory.
    ///
    /// MUST be stable across reconnects, because link recovery is keyed on
    /// the `(source-container, target-container, link-name)` tuple
    /// (Part 2 §2.6.1) and the Addressing draft strengthens it to
    /// "each container MUST use an AMQP network-unique container-id".
    pub container_id: String,
    /// `open.hostname`. RabbitMQ selects a non-default virtual host through
    /// this field — `vhost:tenant-1` — and *not* through the address.
    pub hostname: Option<String>,
    /// `open.max-frame-size`: the largest frame this client will accept. See
    /// [`DEFAULT_MAX_FRAME_SIZE`].
    pub max_frame_size: u32,
    /// `open.channel-max`: the highest channel number this client will
    /// accept, so one less than the number of sessions. See
    /// [`DEFAULT_CHANNEL_MAX`].
    pub channel_max: u16,
    /// The local idle threshold. What goes on the wire is half of it.
    /// `None` disables both the advertisement and the local expiry.
    pub idle_time_out: Option<Duration>,
    /// `open.offered-capabilities`.
    pub offered_capabilities: Vec<String>,
    /// `open.desired-capabilities`. A peer MUST NOT use a capability it did
    /// not list here.
    pub desired_capabilities: Vec<String>,
    /// Which SASL mechanism, and with what.
    pub sasl: Sasl,
    /// How TLS is reached.
    pub tls: TlsMode,
    /// The name to validate the server certificate against. Defaults to the
    /// host that was dialled; set it where the dialled address is an IP
    /// literal fronting a named service.
    pub tls_server_name: Option<String>,
    /// Per-step handshake deadline. See [`DEFAULT_HANDSHAKE_TIMEOUT`].
    pub handshake_timeout: Duration,
    /// How long `close` waits for the peer's answer, and how long what is
    /// still queued gets to leave once the connection is ending. See
    /// [`DEFAULT_CLOSE_BUDGET`].
    pub close_budget: Duration,
    /// How long the socket may take no octets at all while output waits for
    /// it before the connection fails with [`Error::WriteTimeout`]. See
    /// [`DEFAULT_WRITE_TIMEOUT`].
    pub write_timeout: Duration,
    /// How many addresses a hostname may expand to.
    pub max_resolved_addresses: usize,
}

impl ConnectionOptions {
    /// Options for a container named `container_id`, with this client's
    /// defaults everywhere else.
    #[must_use]
    pub fn new(container_id: impl Into<String>) -> Self {
        Self {
            container_id: container_id.into(),
            hostname: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            channel_max: DEFAULT_CHANNEL_MAX,
            idle_time_out: Some(DEFAULT_IDLE_TIME_OUT),
            offered_capabilities: Vec::new(),
            desired_capabilities: Vec::new(),
            sasl: Sasl::None,
            tls: TlsMode::None,
            tls_server_name: None,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            close_budget: DEFAULT_CLOSE_BUDGET,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            max_resolved_addresses: DEFAULT_MAX_RESOLVED_ADDRESSES,
        }
    }

    /// What goes in `open.idle-time-out`: half the local threshold.
    ///
    /// Part 2 §2.4.5: "the advertised value SHOULD be half the local
    /// threshold to avoid spurious expiry". The halving is here rather than
    /// at the call site so that a caller who sets one number gets both
    /// behaviours consistent — the value the peer is asked to beat, and the
    /// value this client measures against.
    #[must_use]
    pub fn advertised_idle_time_out(&self) -> Option<u32> {
        let local = self.idle_time_out?;
        let half = u32::try_from(local.as_millis() / 2).unwrap_or(u32::MAX);
        // Zero equals unset, and a threshold so short that half of it rounds
        // to zero would advertise "no timeout" while still expiring locally.
        Some(half.max(1))
    }

    /// How many sessions this connection may hold, which is the protocol's
    /// own bound: `channel-max + 1` (Part 2 §2.7.1).
    #[must_use]
    pub const fn max_sessions(&self) -> u32 {
        self.channel_max as u32 + 1
    }

    /// Refuses an unusable configuration where it was configured.
    pub fn validate(&self) -> Result<()> {
        if self.container_id.is_empty() {
            return Err(Error::Configuration(
                "container-id is mandatory on open and must not be empty".into(),
            ));
        }
        if self.max_frame_size < MIN_MAX_FRAME_SIZE {
            return Err(Error::Configuration(format!(
                "max-frame-size {} is below MIN-MAX-FRAME-SIZE {MIN_MAX_FRAME_SIZE}, \
                 which both peers MUST accept whatever they advertise",
                self.max_frame_size
            )));
        }
        if self.max_resolved_addresses == 0 {
            return Err(Error::Configuration(
                "max_resolved_addresses must be at least 1".into(),
            ));
        }
        if self.handshake_timeout.is_zero() {
            return Err(Error::Configuration(
                "handshake_timeout must be non-zero: the protocol gives no deadline \
                 of its own, so zero would mean no handshake can ever finish"
                    .into(),
            ));
        }
        if self.write_timeout.is_zero() {
            return Err(Error::Configuration(
                "write_timeout must be non-zero: zero would call every write a \
                 stall before the socket had a chance to take it"
                    .into(),
            ));
        }
        if matches!(self.tls, TlsMode::None) && self.tls_server_name.is_some() {
            return Err(Error::Configuration(
                "tls_server_name is set but tls is TlsMode::None".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_advertised_idle_timeout_is_half_the_local_one() {
        let mut options = ConnectionOptions::new("c1");
        options.idle_time_out = Some(Duration::from_secs(60));
        assert_eq!(options.advertised_idle_time_out(), Some(30_000));
        options.idle_time_out = None;
        assert_eq!(options.advertised_idle_time_out(), None);
    }

    #[test]
    fn a_threshold_too_short_to_halve_still_advertises_something() {
        // Zero equals unset on the wire, so halving a 1 ms threshold to 0
        // would advertise "no timeout" while this client went on expiring
        // locally - the peer would be closed for missing a deadline it was
        // never told about.
        let mut options = ConnectionOptions::new("c1");
        options.idle_time_out = Some(Duration::from_millis(1));
        assert_eq!(options.advertised_idle_time_out(), Some(1));
    }

    #[test]
    fn the_session_table_is_bounded_by_channel_max() {
        let mut options = ConnectionOptions::new("c1");
        assert_eq!(options.channel_max, DEFAULT_CHANNEL_MAX);
        assert_eq!(options.max_sessions(), 256);
        options.channel_max = 0;
        assert_eq!(
            options.max_sessions(),
            1,
            "channel-max 0 still admits the one session on channel 0"
        );
        options.channel_max = u16::MAX;
        assert_eq!(
            options.max_sessions(),
            65_536,
            "the specification's default"
        );
    }

    #[test]
    fn the_three_changed_defaults_are_the_ones_documented() {
        let options = ConnectionOptions::new("c1");
        assert_ne!(
            options.max_frame_size,
            weida_amqp_codec::frame::DEFAULT_MAX_FRAME_SIZE,
            "the specification's default is no limit at all"
        );
        assert!(
            options.idle_time_out.is_some(),
            "the specification's is unset"
        );
        assert_ne!(options.channel_max, u16::MAX);
    }

    #[test]
    fn a_frame_size_below_the_floor_is_refused_where_it_is_configured() {
        let mut options = ConnectionOptions::new("c1");
        options.max_frame_size = 511;
        let error = options.validate().expect_err("refused");
        assert!(error.to_string().contains("MIN-MAX-FRAME-SIZE"), "{error}");
        options.max_frame_size = MIN_MAX_FRAME_SIZE;
        options.validate().expect("512 is exactly the floor");
    }

    #[test]
    fn an_empty_container_id_is_refused() {
        let options = ConnectionOptions::new("");
        assert!(options.validate().is_err());
    }

    #[test]
    fn each_sasl_setting_names_its_mechanism() {
        assert_eq!(Sasl::None.mechanism(), None);
        assert_eq!(Sasl::Anonymous.mechanism(), Some("ANONYMOUS"));
        assert_eq!(
            Sasl::Plain {
                username: "guest".into(),
                password: "guest".into()
            }
            .mechanism(),
            Some("PLAIN")
        );
        assert_eq!(
            Sasl::External {
                authzid: String::new()
            }
            .mechanism(),
            Some("EXTERNAL")
        );
    }

    #[test]
    fn a_password_never_reaches_a_debug_print() {
        let sasl = Sasl::Plain {
            username: "svc".into(),
            password: "hunter2".into(),
        };
        let printed = format!("{sasl:?} {sasl:#?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(
            printed.contains("svc"),
            "the identity is not a secret: {printed}"
        );

        // The type that holds it, and everything that holds that, prints it
        // the same way: a derived `Debug` anywhere above would undo the
        // redaction.
        let mut options = ConnectionOptions::new("c1");
        options.sasl = sasl;
        let printed = format!("{options:?} {options:#?}");
        assert!(!printed.contains("hunter2"), "{printed}");
    }

    #[test]
    fn a_zero_write_timeout_is_refused_where_it_is_configured() {
        let mut options = ConnectionOptions::new("c1");
        assert_eq!(options.write_timeout, DEFAULT_WRITE_TIMEOUT);
        options.write_timeout = Duration::ZERO;
        let error = options.validate().expect_err("refused");
        assert!(error.to_string().contains("write_timeout"), "{error}");
    }
}
