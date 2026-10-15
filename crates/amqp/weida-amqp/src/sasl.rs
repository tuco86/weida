//! The client half of the SASL dialog.
//!
//! ```text
//! server -> sasl-mechanisms(sasl-server-mechanisms)   in decreasing preference
//! client -> sasl-init(mechanism, initial-response?, hostname?)
//! server -> sasl-challenge(challenge)      zero or more times
//! client -> sasl-response(response)
//! server -> sasl-outcome(code, additional-data?)
//! ```
//!
//! Three things about this layer are worth writing down because they differ
//! from every other part of AMQP.
//!
//! **The frame size does not negotiate.** A SASL frame is capped at
//! `MIN-MAX-FRAME-SIZE` = 512 octets "with no way to negotiate more"
//! (Part 5 §5.3.1). A mechanism whose challenge does not fit cannot be used
//! over AMQP at all — which is why the claims-based-security extension had to
//! raise the number to 8192 in a document of its own to carry a JWT. This
//! client applies the 512 in both directions and reports the refusal rather
//! than truncating.
//!
//! **An empty SASL frame is fatal.** At the AMQP layer a body-less frame is
//! the idle keep-alive; here the same eight octets are "an irrecoverable
//! error" (Part 5 §5.3.1). The codec reports it and this dialog does not
//! treat it as a heartbeat.
//!
//! **The mechanism is chosen, not negotiated.** The client "MUST authenticate
//! using the highest-level security profile it can handle from the list
//! provided by the partner" (Part 5 §5.3). This client is configured with one
//! mechanism, so the rule reduces to a membership test: if what the caller
//! configured is not on the server's list, that is
//! [`Error::NoSharedSaslMechanism`] at the point of failure rather than a
//! silent downgrade to something weaker.

use std::sync::Arc;

use weida_amqp_codec::frame::{self, FrameKind, SASL_MAX_FRAME_SIZE};
use weida_amqp_codec::sasl::{SaslCode, SaslFrame, SaslInit, plain_response};
use weida_amqp_codec::{Limits, Multiple};
use weida_runtime::Exec;

use crate::error::{Error, Result};
use crate::options::{ConnectionOptions, Sasl};
use crate::transport::Wire;

/// How many challenge/response rounds this client will run.
///
/// **A bound of ours: the protocol has none.** Part 5 §5.3.2 says "zero or
/// more" `sasl-challenge`/`sasl-response` rounds follow, which is an
/// unbounded loop against a hostile or broken server. Sixteen is far above
/// what any mechanism the specification normatively references needs — PLAIN
/// and ANONYMOUS need zero, SCRAM needs two — and finite.
pub const MAX_ROUNDS: usize = 16;

/// Runs the dialog to its outcome.
///
/// `wire` is positioned just after the `AMQP %d3 1.0.0` exchange, and is left
/// positioned for the `AMQP %d0 1.0.0` exchange that follows a successful
/// outcome.
pub(crate) async fn dialog(
    exec: &Exec,
    wire: &mut Wire,
    options: &Arc<ConnectionOptions>,
) -> Result<()> {
    let mechanism = options
        .sasl
        .mechanism()
        .ok_or_else(|| Error::Configuration("no SASL mechanism configured".into()))?;

    let offered = read_mechanisms(exec, wire, options).await?;
    if !offered.iter().any(|found| found == mechanism) {
        return Err(Error::NoSharedSaslMechanism { offered });
    }

    let initial = match &options.sasl {
        Sasl::None => None,
        // ANONYMOUS's trace field is optional and this client sends none: it
        // is advisory text about who is connecting anonymously, and inventing
        // one would be inventing an identity.
        Sasl::Anonymous => None,
        Sasl::Plain { username, password } => Some(plain_response(username, password)),
        // EXTERNAL's response is the authorization identity, empty meaning
        // "whatever the layer below says" (RFC 4422 §4.4.1.2). An empty
        // response and an absent one are the same thing to a server, and the
        // empty one is what says "use the certificate".
        Sasl::External { authzid } => Some(authzid.as_bytes().to_vec()),
    };

    send_within(
        exec,
        wire,
        options,
        "sending sasl-init",
        &SaslFrame::Init(SaslInit {
            mechanism,
            initial_response: initial.as_deref(),
            hostname: options.hostname.as_deref(),
        }),
    )
    .await?;

    for _ in 0..MAX_ROUNDS {
        match read_body(exec, wire, options, "a SASL challenge or outcome").await? {
            SaslBody::Challenge(challenge) => {
                // None of the mechanisms this client speaks has a second
                // round, so a challenge here means the server is running a
                // mechanism we agreed to by name but cannot continue. An
                // empty response is the only honest answer, and the server's
                // outcome will say what it made of it.
                tracing::debug!(
                    mechanism,
                    challenge = challenge.len(),
                    "a SASL challenge this mechanism has no answer for"
                );
                send_within(
                    exec,
                    wire,
                    options,
                    "sending sasl-response",
                    &SaslFrame::Response(&[]),
                )
                .await?;
            }
            SaslBody::Outcome(code) => {
                return if code.is_ok() {
                    Ok(())
                } else {
                    Err(Error::Sasl {
                        code,
                        mechanism: mechanism.to_owned(),
                    })
                };
            }
        }
    }
    Err(Error::Sasl {
        code: SaslCode::Sys,
        mechanism: format!("{mechanism}: more than {MAX_ROUNDS} challenge rounds"),
    })
}

/// The two bodies a server may send after `sasl-init`.
enum SaslBody {
    Challenge(Vec<u8>),
    Outcome(SaslCode),
}

async fn read_mechanisms(
    exec: &Exec,
    wire: &mut Wire,
    options: &Arc<ConnectionOptions>,
) -> Result<Vec<String>> {
    let frame = match exec
        .within(options.handshake_timeout, wire.read_frame())
        .await
    {
        Some(result) => result?,
        None => {
            return Err(Error::HandshakeTimeout {
                step: "the server's sasl-mechanisms",
            });
        }
    };
    expect_sasl_frame(&frame)?;
    let (body, _) = SaslFrame::decode(frame.body, Limits::DEFAULT)?;
    match body {
        SaslFrame::Mechanisms(mechanisms) => Ok(collect(&mechanisms.server_mechanisms)),
        other => Err(Error::Decode(weida_amqp_codec::DecodeError::WrongType {
            field: "sasl-mechanisms",
            // The descriptor is the honest thing to report here, and the
            // codec's `name` is what a log line wants.
            code: other.descriptor() as u8,
        })),
    }
}

async fn read_body(
    exec: &Exec,
    wire: &mut Wire,
    options: &Arc<ConnectionOptions>,
    step: &'static str,
) -> Result<SaslBody> {
    let frame = match exec
        .within(options.handshake_timeout, wire.read_frame())
        .await
    {
        Some(result) => result?,
        None => return Err(Error::HandshakeTimeout { step }),
    };
    expect_sasl_frame(&frame)?;
    let (body, _) = SaslFrame::decode(frame.body, Limits::DEFAULT)?;
    match body {
        SaslFrame::Challenge(challenge) => Ok(SaslBody::Challenge(challenge.to_vec())),
        SaslFrame::Outcome(outcome) => Ok(SaslBody::Outcome(outcome.code)),
        other => Err(Error::Decode(weida_amqp_codec::DecodeError::WrongType {
            field: "sasl-challenge or sasl-outcome",
            code: other.descriptor() as u8,
        })),
    }
}

/// A frame in the SASL layer must be type `0x01`.
///
/// Not pedantry: the two layers assign their descriptors from disjoint ranges
/// (`0x10..0x18` against `0x40..0x44`), so a type-`0x00` frame here is a
/// server that has left the SASL layer early and the client must not decode
/// its body as a SASL body.
fn expect_sasl_frame(frame: &weida_amqp_codec::Frame<'_>) -> Result<()> {
    if frame.header.kind != FrameKind::Sasl {
        return Err(Error::Decode(
            weida_amqp_codec::DecodeError::UnknownFrameType(frame.header.kind.octet()),
        ));
    }
    Ok(())
}

/// Writes one SASL frame inside the handshake deadline: a server that accepts
/// the connection and never reads is the case, and nothing in the protocol
/// would end the wait.
async fn send_within(
    exec: &Exec,
    wire: &mut Wire,
    options: &Arc<ConnectionOptions>,
    step: &'static str,
    body: &SaslFrame<'_>,
) -> Result<()> {
    match exec
        .within(options.handshake_timeout, send(wire, body))
        .await
    {
        Some(result) => result,
        None => Err(Error::HandshakeTimeout { step }),
    }
}

async fn send(wire: &mut Wire, body: &SaslFrame<'_>) -> Result<()> {
    let mut out = Vec::new();
    // The 512 is not negotiable, so it is the bound here and the refusal is
    // ours before the octets leave.
    frame::write(&mut out, FrameKind::Sasl, 0, SASL_MAX_FRAME_SIZE, |buf| {
        body.encode(buf)
    })?;
    wire.queue().extend_from_slice(&out);
    wire.flush().await
}

fn collect(mechanisms: &Multiple<'_>) -> Vec<String> {
    mechanisms.iter().map(str::to_owned).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_configured_mechanism_is_the_one_that_must_be_offered() {
        // The rule reduces to a membership test because this client holds
        // one mechanism. The alternative - picking whatever the server
        // offered - is the silent downgrade the specification's "highest
        // level it can handle" exists to prevent.
        let plain = Sasl::Plain {
            username: "guest".into(),
            password: "guest".into(),
        };
        assert_eq!(plain.mechanism(), Some("PLAIN"));
        let offered = ["ANONYMOUS".to_owned()];
        assert!(!offered.iter().any(|m| m == plain.mechanism().unwrap()));
    }

    #[test]
    fn plain_builds_the_response_rfc_4616_specifies() {
        assert_eq!(plain_response("guest", "guest"), b"\0guest\0guest");
    }

    #[test]
    fn the_round_bound_is_ours_and_finite() {
        // Part 5 §5.3.2 says "zero or more" rounds, which is an unbounded
        // loop; this is the number that makes it finite.
        assert_eq!(MAX_ROUNDS, 16);
    }
}
