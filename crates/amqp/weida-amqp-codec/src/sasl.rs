//! The five SASL bodies, and the 512-octet frame they live in.
//!
//! SASL is a *layer*, not a performative family: the dialog runs in frames of
//! type `0x01` between two protocol headers, `AMQP %d3 1.0.0` before and
//! `AMQP %d0 1.0.0` after (Part 5 §5.3). Inside it there are exactly five
//! bodies, and Part 5 numbers them `0x40` to `0x44` — a range the nine
//! transport performatives do not touch, so a frame's type octet and its
//! descriptor agree about which layer it belongs to.
//!
//! ```text
//! sasl-mechanisms 0x40   sasl-init 0x41   sasl-challenge 0x42
//! sasl-response   0x43   sasl-outcome 0x44
//! ```
//!
//! # The frame size does not negotiate
//!
//! A SASL frame is capped at `MIN-MAX-FRAME-SIZE` = 512 octets "with no way
//! to negotiate more" (Part 5 §5.3.1), which is why
//! [`SASL_MAX_FRAME_SIZE`](crate::frame::SASL_MAX_FRAME_SIZE) is a constant
//! rather than a field: a mechanism whose challenge does not fit cannot be
//! used over AMQP at all, and the CBS extension had to raise the number to
//! 8192 in its own document to carry a JWT.
//!
//! An empty SASL frame — a header and no body — is "an irrecoverable error"
//! (Part 5 §5.3.1), which is the one place AMQP treats a keep-alive shape as
//! fatal. [`SaslFrame::decode`] reports it as
//! [`DecodeError::MissingMandatoryField`] naming the body, because what is
//! missing is the body itself.

use crate::decode;
use crate::encode;
use crate::error::{DecodeError, EncodeError};
use crate::fields::Fields;
use crate::limits::Limits;
use crate::types::Multiple;
use crate::value::{Descriptor, Value};

/// The five SASL bodies, numeric descriptor and symbolic descriptor together.
pub const DESCRIPTORS: [(u64, &str); 5] = [
    (0x0000_0000_0000_0040, "amqp:sasl-mechanisms:list"),
    (0x0000_0000_0000_0041, "amqp:sasl-init:list"),
    (0x0000_0000_0000_0042, "amqp:sasl-challenge:list"),
    (0x0000_0000_0000_0043, "amqp:sasl-response:list"),
    (0x0000_0000_0000_0044, "amqp:sasl-outcome:list"),
];

/// `ANONYMOUS` (RFC 4505): what a server not requiring authentication SHOULD
/// advertise, and the mechanism Service Bus needs in order to defer
/// authorization to its `$cbs` node.
pub const ANONYMOUS: &str = "ANONYMOUS";

/// `PLAIN` (RFC 4616): a username and a password separated by NUL octets,
/// normatively referenced by Part 0 and required by nothing.
pub const PLAIN: &str = "PLAIN";

/// `EXTERNAL` (RFC 4422): the identity comes from the layer below, in
/// practice a TLS client certificate. RabbitMQ reaches x.509 authentication
/// through it.
pub const EXTERNAL: &str = "EXTERNAL";

/// `sasl-code`: how the dialog ended (Part 5 §5.3.3.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SaslCode {
    /// `0`: authentication succeeded.
    Ok,
    /// `1`: the supplied credentials were not valid.
    Auth,
    /// `2`: a system error occurred; the outcome of authentication is
    /// undefined.
    Sys,
    /// `3`: a permanent system error. Retrying will not help.
    SysPerm,
    /// `4`: a transient system error. The specification says it is
    /// transient and nothing about how long to wait, which is the one thing a
    /// client actually needs to know.
    SysTemp,
}

impl SaslCode {
    /// The `ubyte` on the wire.
    #[must_use]
    pub const fn octet(self) -> u8 {
        match self {
            Self::Ok => 0,
            Self::Auth => 1,
            Self::Sys => 2,
            Self::SysPerm => 3,
            Self::SysTemp => 4,
        }
    }

    /// The code an octet names.
    pub const fn from_octet(octet: u8) -> Result<Self, DecodeError> {
        match octet {
            0 => Ok(Self::Ok),
            1 => Ok(Self::Auth),
            2 => Ok(Self::Sys),
            3 => Ok(Self::SysPerm),
            4 => Ok(Self::SysTemp),
            other => Err(DecodeError::RestrictionViolated {
                restriction: "sasl-code",
                value: other as u64,
                limit: 4,
            }),
        }
    }

    /// Whether the dialog succeeded.
    #[must_use]
    pub const fn is_ok(self) -> bool {
        matches!(self, Self::Ok)
    }
}

/// `sasl-mechanisms`, `0x40`: what the server will accept
/// (Part 5 §5.3.3.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SaslMechanisms<'a> {
    /// Mandatory and `multiple`, in decreasing order of preference. An empty
    /// or null list is invalid, and a server not requiring authentication
    /// SHOULD advertise exactly `ANONYMOUS`.
    pub server_mechanisms: Multiple<'a>,
}

/// `sasl-init`, `0x41`: the client's choice (Part 5 §5.3.3.2).
#[derive(Clone, PartialEq, Eq)]
pub struct SaslInit<'a> {
    /// Mandatory. One of the mechanisms the server offered; the client MUST
    /// authenticate "using the highest-level security profile it can handle
    /// from the list provided by the partner".
    pub mechanism: &'a str,
    /// The mechanism's initial response, where it has one. `PLAIN` puts its
    /// whole exchange here.
    pub initial_response: Option<&'a [u8]>,
    /// The host the client is authenticating to, which the server MAY use to
    /// select both the back end and the credentials to validate against.
    pub hostname: Option<&'a str>,
}

/// `sasl-outcome`, `0x44`: the result (Part 5 §5.3.3.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SaslOutcome<'a> {
    /// Mandatory.
    pub code: SaslCode,
    /// The mechanism's final challenge, where it has one.
    pub additional_data: Option<&'a [u8]>,
}

/// One SASL body.
#[derive(Clone, PartialEq, Eq)]
pub enum SaslFrame<'a> {
    /// `sasl-mechanisms`, server to client, first.
    Mechanisms(SaslMechanisms<'a>),
    /// `sasl-init`, client to server, once.
    Init(SaslInit<'a>),
    /// `sasl-challenge`, server to client, zero or more times.
    Challenge(&'a [u8]),
    /// `sasl-response`, client to server, once per challenge.
    Response(&'a [u8]),
    /// `sasl-outcome`, server to client, last.
    Outcome(SaslOutcome<'a>),
}

/// What `Debug` prints in place of a secret's octets: how many there are.
struct Redacted(usize);

impl core::fmt::Debug for Redacted {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<redacted: {} octets>", self.0)
    }
}

/// `initial_response` is where PLAIN puts its password, so `Debug` shows the
/// length of the response and nothing of its octets.
impl core::fmt::Debug for SaslInit<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SaslInit")
            .field("mechanism", &self.mechanism)
            .field(
                "initial_response",
                &self
                    .initial_response
                    .map(|response| Redacted(response.len())),
            )
            .field("hostname", &self.hostname)
            .finish()
    }
}

/// A `Response` is the client's answer to a challenge, which can be a
/// credential, so `Debug` shows its length and nothing of its octets.
impl core::fmt::Debug for SaslFrame<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Mechanisms(body) => f.debug_tuple("Mechanisms").field(body).finish(),
            Self::Init(body) => f.debug_tuple("Init").field(body).finish(),
            Self::Challenge(challenge) => f.debug_tuple("Challenge").field(challenge).finish(),
            Self::Response(response) => f
                .debug_tuple("Response")
                .field(&Redacted(response.len()))
                .finish(),
            Self::Outcome(body) => f.debug_tuple("Outcome").field(body).finish(),
        }
    }
}

impl<'a> SaslFrame<'a> {
    /// Decodes one SASL body from a type-`0x01` frame body.
    ///
    /// ```
    /// use weida_amqp_codec::{Limits, sasl::{self, SaslFrame, SaslInit}};
    ///
    /// // PLAIN carries `\0user\0password` as its initial response.
    /// let init = SaslFrame::Init(SaslInit {
    ///     mechanism: sasl::PLAIN,
    ///     initial_response: Some(b"\0guest\0guest"),
    ///     hostname: None,
    /// });
    /// let mut body = Vec::new();
    /// init.encode(&mut body).expect("encodes");
    /// let (back, used) = SaslFrame::decode(&body, Limits::DEFAULT).expect("decodes");
    /// assert_eq!(used, body.len());
    /// assert_eq!(back, init);
    /// ```
    pub fn decode(input: &'a [u8], limits: Limits) -> Result<(Self, usize), DecodeError> {
        if input.is_empty() {
            // "An empty SASL frame is an irrecoverable error": there is no
            // keep-alive at this layer, so a body-less frame is not a frame
            // whose body is optional.
            return Err(DecodeError::MissingMandatoryField {
                composite: "sasl frame",
                field: "body",
            });
        }
        let composite = decode::composite(input, limits)?;
        let used = composite.used;
        let code = resolve(&composite.descriptor).ok_or(DecodeError::UnknownComposite {
            kind: "SASL body",
            descriptor: composite.descriptor.code(),
        })?;
        let frame = match code {
            0x40 => {
                let mut fields = Fields::new(composite.fields, "sasl-mechanisms");
                Self::Mechanisms(SaslMechanisms {
                    server_mechanisms: fields.required_multiple("sasl-server-mechanisms")?,
                })
            }
            0x41 => {
                let mut fields = Fields::new(composite.fields, "sasl-init");
                Self::Init(SaslInit {
                    mechanism: fields.required_symbol("mechanism")?,
                    initial_response: fields.binary("initial-response")?,
                    hostname: fields.string("hostname")?,
                })
            }
            0x42 => {
                let mut fields = Fields::new(composite.fields, "sasl-challenge");
                Self::Challenge(fields.required_binary("challenge")?)
            }
            0x43 => {
                let mut fields = Fields::new(composite.fields, "sasl-response");
                Self::Response(fields.required_binary("response")?)
            }
            0x44 => {
                let mut fields = Fields::new(composite.fields, "sasl-outcome");
                let octet = fields.required_ubyte("code")?;
                Self::Outcome(SaslOutcome {
                    code: SaslCode::from_octet(octet)?,
                    additional_data: fields.binary("additional-data")?,
                })
            }
            other => {
                return Err(DecodeError::UnknownComposite {
                    kind: "SASL body",
                    descriptor: Some(other),
                });
            }
        };
        Ok((frame, used))
    }

    /// Appends this body's canonical encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Self::Mechanisms(body) => {
                if body.server_mechanisms.is_empty() {
                    return Err(EncodeError::MissingMandatoryField {
                        composite: "sasl-mechanisms",
                        field: "sasl-server-mechanisms",
                    });
                }
                encode::composite(
                    &Descriptor::Code(0x40),
                    &[body.server_mechanisms.to_value()],
                    out,
                )
            }
            Self::Init(body) => encode::composite(
                &Descriptor::Code(0x41),
                &[
                    Value::Symbol(body.mechanism),
                    body.initial_response.map_or(Value::Null, Value::Binary),
                    body.hostname.map_or(Value::Null, Value::String),
                ],
                out,
            ),
            Self::Challenge(challenge) => {
                encode::composite(&Descriptor::Code(0x42), &[Value::Binary(challenge)], out)
            }
            Self::Response(response) => {
                encode::composite(&Descriptor::Code(0x43), &[Value::Binary(response)], out)
            }
            Self::Outcome(body) => encode::composite(
                &Descriptor::Code(0x44),
                &[
                    Value::Ubyte(body.code.octet()),
                    body.additional_data.map_or(Value::Null, Value::Binary),
                ],
                out,
            ),
        }
    }

    /// The numeric descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> u64 {
        match self {
            Self::Mechanisms(_) => 0x40,
            Self::Init(_) => 0x41,
            Self::Challenge(_) => 0x42,
            Self::Response(_) => 0x43,
            Self::Outcome(_) => 0x44,
        }
    }

    /// The body's name, for a log line.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Mechanisms(_) => "sasl-mechanisms",
            Self::Init(_) => "sasl-init",
            Self::Challenge(_) => "sasl-challenge",
            Self::Response(_) => "sasl-response",
            Self::Outcome(_) => "sasl-outcome",
        }
    }
}

/// Builds `PLAIN`'s initial response: `authzid NUL authcid NUL passwd`, with
/// an empty authorization identity (RFC 4616).
///
/// A function rather than a note in the documentation, because getting the
/// NUL octets wrong is the single most common way a `PLAIN` dialog fails and
/// the failure looks like a wrong password.
#[must_use]
pub fn plain_response(username: &str, password: &str) -> Vec<u8> {
    let mut response = Vec::with_capacity(username.len() + password.len() + 2);
    response.push(0);
    response.extend_from_slice(username.as_bytes());
    response.push(0);
    response.extend_from_slice(password.as_bytes());
    response
}

/// The numeric descriptor a SASL descriptor names, in either form.
fn resolve(descriptor: &Descriptor<'_>) -> Option<u64> {
    match descriptor {
        Descriptor::Code(code) => Some(*code),
        Descriptor::Symbol(name) => DESCRIPTORS
            .iter()
            .find(|(_, symbolic)| symbolic == name)
            .map(|(code, _)| *code),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{self, FrameKind, SASL_MAX_FRAME_SIZE};

    fn round_trip(body: &SaslFrame<'_>) -> Vec<u8> {
        let mut bytes = Vec::new();
        body.encode(&mut bytes).expect("encodes");
        let (back, used) = SaslFrame::decode(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(used, bytes.len(), "{}", body.name());
        assert_eq!(&back, body, "{}", body.name());
        bytes
    }

    #[test]
    fn every_sasl_body_round_trips() {
        round_trip(&SaslFrame::Mechanisms(SaslMechanisms {
            server_mechanisms: Multiple::from_slice(&[PLAIN, ANONYMOUS]),
        }));
        round_trip(&SaslFrame::Init(SaslInit {
            mechanism: PLAIN,
            initial_response: Some(b"\0guest\0guest"),
            hostname: Some("localhost"),
        }));
        round_trip(&SaslFrame::Init(SaslInit {
            mechanism: ANONYMOUS,
            initial_response: None,
            hostname: None,
        }));
        round_trip(&SaslFrame::Challenge(b"challenge"));
        round_trip(&SaslFrame::Response(b"response"));
        for code in [
            SaslCode::Ok,
            SaslCode::Auth,
            SaslCode::Sys,
            SaslCode::SysPerm,
            SaslCode::SysTemp,
        ] {
            round_trip(&SaslFrame::Outcome(SaslOutcome {
                code,
                additional_data: None,
            }));
        }
        round_trip(&SaslFrame::Outcome(SaslOutcome {
            code: SaslCode::Ok,
            additional_data: Some(b"final"),
        }));
    }

    #[test]
    fn all_five_outcome_codes_are_distinguished() {
        for (octet, code) in [
            (0, SaslCode::Ok),
            (1, SaslCode::Auth),
            (2, SaslCode::Sys),
            (3, SaslCode::SysPerm),
            (4, SaslCode::SysTemp),
        ] {
            assert_eq!(SaslCode::from_octet(octet).unwrap(), code);
            assert_eq!(code.octet(), octet);
        }
        assert!(SaslCode::Ok.is_ok());
        assert!(!SaslCode::SysTemp.is_ok());
        assert_eq!(
            SaslCode::from_octet(5),
            Err(DecodeError::RestrictionViolated {
                restriction: "sasl-code",
                value: 5,
                limit: 4
            })
        );
    }

    #[test]
    fn an_empty_mechanism_list_is_invalid_in_both_directions() {
        let mut out = Vec::new();
        assert_eq!(
            SaslFrame::Mechanisms(SaslMechanisms {
                server_mechanisms: Multiple::None,
            })
            .encode(&mut out),
            Err(EncodeError::MissingMandatoryField {
                composite: "sasl-mechanisms",
                field: "sasl-server-mechanisms"
            })
        );
        assert!(out.is_empty());

        let mut bytes = Vec::new();
        encode::composite(&Descriptor::Code(0x40), &[Value::Null], &mut bytes).expect("encodes");
        assert_eq!(
            SaslFrame::decode(&bytes, Limits::DEFAULT),
            Err(DecodeError::MissingMandatoryField {
                composite: "sasl-mechanisms",
                field: "sasl-server-mechanisms"
            })
        );
    }

    #[test]
    fn an_empty_sasl_frame_is_irrecoverable() {
        // At the AMQP layer a body-less frame is the idle keep-alive; here it
        // is fatal, and the difference is worth a test because the octets are
        // identical.
        let bytes = frame::empty(0);
        let amqp = frame::decode(&bytes, SASL_MAX_FRAME_SIZE).expect("a frame");
        assert!(amqp.is_empty(), "the same eight octets are a valid frame");
        assert_eq!(
            SaslFrame::decode(amqp.body, Limits::DEFAULT),
            Err(DecodeError::MissingMandatoryField {
                composite: "sasl frame",
                field: "body"
            })
        );
    }

    #[test]
    fn a_sasl_dialog_fits_the_five_hundred_and_twelve_octet_frame() {
        // The cap does not negotiate, so the encoder has to be told it and
        // the check has to happen before the octets leave.
        let mut out = Vec::new();
        frame::write(&mut out, FrameKind::Sasl, 0, SASL_MAX_FRAME_SIZE, |body| {
            SaslFrame::Init(SaslInit {
                mechanism: PLAIN,
                initial_response: Some(&plain_response("guest", "guest")),
                hostname: None,
            })
            .encode(body)
        })
        .expect("fits");
        let read = frame::decode(&out, SASL_MAX_FRAME_SIZE).expect("a frame");
        assert_eq!(read.header.kind, FrameKind::Sasl);
        let (body, _) = SaslFrame::decode(read.body, Limits::DEFAULT).expect("a body");
        assert_eq!(
            body,
            SaslFrame::Init(SaslInit {
                mechanism: PLAIN,
                initial_response: Some(b"\0guest\0guest"),
                hostname: None
            })
        );

        // A token too big for the frame is refused here rather than on the
        // wire: this is the limit the CBS extension had to raise to 8192 in a
        // document of its own.
        let token = vec![b'j'; 600];
        let mut out = Vec::new();
        let error = frame::write(&mut out, FrameKind::Sasl, 0, SASL_MAX_FRAME_SIZE, |body| {
            SaslFrame::Response(&token).encode(body)
        })
        .expect_err("refused");
        assert!(matches!(error, EncodeError::FrameTooLarge { max: 512, .. }));
        assert!(out.is_empty());
    }

    #[test]
    fn plain_puts_nul_octets_where_rfc_4616_does() {
        assert_eq!(plain_response("guest", "guest"), b"\0guest\0guest");
        assert_eq!(plain_response("", ""), b"\0\0");
    }

    #[test]
    fn a_symbolic_descriptor_names_the_same_body() {
        let mut bytes = Vec::new();
        encode::composite(
            &Descriptor::Symbol("amqp:sasl-outcome:list"),
            &[Value::Ubyte(0)],
            &mut bytes,
        )
        .expect("encodes");
        let (body, _) = SaslFrame::decode(&bytes, Limits::DEFAULT).expect("decodes");
        assert_eq!(
            body,
            SaslFrame::Outcome(SaslOutcome {
                code: SaslCode::Ok,
                additional_data: None
            })
        );
    }

    #[test]
    fn a_sixth_sasl_body_is_refused() {
        let mut bytes = Vec::new();
        encode::composite(&Descriptor::Code(0x45), &[], &mut bytes).expect("encodes");
        assert_eq!(
            SaslFrame::decode(&bytes, Limits::DEFAULT),
            Err(DecodeError::UnknownComposite {
                kind: "SASL body",
                descriptor: Some(0x45)
            })
        );
    }

    #[test]
    fn the_sasl_descriptors_do_not_collide_with_the_performatives() {
        // 0x40..0x44 against 0x10..0x18: a frame's type octet and its
        // descriptor cannot disagree about which layer it belongs to.
        for (sasl, _) in DESCRIPTORS {
            assert!(
                !crate::performative::DESCRIPTORS
                    .iter()
                    .any(|(code, _)| *code == sasl)
            );
        }
    }

    #[test]
    fn debug_output_never_shows_a_credential() {
        let response = plain_response("user", "hunter2");
        let init = SaslFrame::Init(SaslInit {
            mechanism: PLAIN,
            initial_response: Some(&response),
            hostname: Some("broker"),
        });
        let shown = format!("{init:?}");
        assert!(
            shown.contains("PLAIN") && shown.contains("broker"),
            "the other fields print normally: {shown}"
        );
        assert!(
            !shown.contains("hunter2"),
            "the password is printed: {shown}"
        );
        assert!(
            !shown.contains(&format!("{response:?}")),
            "the octets are printed: {shown}"
        );

        let secret: &[u8] = b"hunter2";
        let shown = format!("{:?}", SaslFrame::Response(secret));
        assert!(
            !shown.contains("hunter2"),
            "the response is printed: {shown}"
        );
        assert!(
            !shown.contains(&format!("{secret:?}")),
            "the response octets are printed: {shown}"
        );
        assert!(shown.contains("7 octets"), "the length is kept: {shown}");
    }
}
