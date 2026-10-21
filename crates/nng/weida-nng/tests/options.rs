//! The option table, walked, and the one option the manual insists is
//! per-endpoint.

use weida_nng::{Disposition, OPTIONS, optiontable};

/// Claim: a walk over the whole table finds every option either honoured
/// under a named construct or refused with a reason — never silently
/// ignored (§4.4 item 4 of decision 0013).
///
/// The plausible bug this fails on is a row whose refusal prints nothing
/// a caller can act on, which is how "refused with the reason named"
/// becomes "refused".
#[test]
fn every_option_is_honoured_or_refused_with_a_reason() {
    let mut honoured = 0usize;
    let mut refused = 0usize;
    for row in OPTIONS {
        match row.disposition {
            Disposition::Honoured(what) => {
                honoured += 1;
                assert!(
                    optiontable::require_honoured(row.name).is_ok(),
                    "{} is honoured by {what} but the lookup refuses it",
                    row.name
                );
            }
            Disposition::Refused(why) => {
                refused += 1;
                let error = optiontable::require_honoured(row.name)
                    .expect_err("a refused option is refused");
                assert!(error.cause().contains(row.name));
                assert!(error.cause().contains(why.reason()));
            }
        }
    }
    assert!(honoured > 25, "only {honoured} options are honoured");
    assert!(refused > 8, "only {refused} options are refused");
}
