//! A number of seconds from Python, as a [`Duration`] that cannot panic.
//!
//! Every binding takes timeouts as a Python `float`, and
//! `Duration::from_secs_f64` **panics** on a negative number, on `NaN` and on
//! anything past the range of a `u64` of seconds. A panic out of a binding is
//! a `pyo3_runtime.PanicException`, a `BaseException` that `except Exception`
//! does not catch, so `sock.recv(timeout=1e30)` would end a program that
//! catches everything it expects. This is the one place the check is made, so
//! six bindings cannot disagree about it.
//!
//! The ceiling is [`MAX_SECONDS`], and it is lower than what a `Duration` can
//! hold on purpose: a deadline is added to an `Instant`, and an `Instant`
//! panics on overflow long before a `Duration` does.

use std::time::Duration;

use pyo3::PyErr;
use pyo3::exceptions::PyValueError;

/// The longest timeout a binding accepts, in seconds: `u32::MAX`, a little
/// over 136 years.
///
/// Past this a deadline stops being a deadline — it is "never" spelled as a
/// number — and `Instant + Duration` is no longer safe to compute. The wire
/// formats these bindings speak count their intervals in 32-bit seconds, so
/// nothing a protocol can express is refused.
pub const MAX_SECONDS: f64 = u32::MAX as f64;

/// What [`duration`] refuses, in words a caller can act on.
#[derive(Clone, Debug, PartialEq)]
pub struct BadSeconds {
    what: String,
    value: f64,
}

impl BadSeconds {
    /// The argument's name, as the caller wrote it.
    pub fn what(&self) -> &str {
        &self.what
    }
}

impl std::fmt::Display for BadSeconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} must be a finite number of seconds from 0 to {MAX_SECONDS}, not {}",
            self.what, self.value
        )
    }
}

impl std::error::Error for BadSeconds {}

/// A binding that does not map the refusal onto a class of its own gets
/// `ValueError`, which is what a bad argument is.
impl From<BadSeconds> for PyErr {
    fn from(refused: BadSeconds) -> PyErr {
        PyValueError::new_err(refused.to_string())
    }
}

/// `seconds` as a [`Duration`], or a [`BadSeconds`] naming `what`.
///
/// Refused: `NaN`, an infinity, a negative number (negative zero is zero) and
/// anything above [`MAX_SECONDS`].
///
/// # Errors
///
/// [`BadSeconds`], which converts to `ValueError` with `?` and to whichever
/// class a binding prefers through its message.
pub fn duration(what: &str, seconds: f64) -> Result<Duration, BadSeconds> {
    if seconds.is_nan() || !(0.0..=MAX_SECONDS).contains(&seconds) {
        return Err(BadSeconds {
            what: what.to_owned(),
            value: seconds,
        });
    }
    // Cannot fail inside the range above; the `Err` arm keeps this function
    // total rather than leaving a panic where the whole point is none.
    Duration::try_from_secs_f64(seconds).map_err(|_| BadSeconds {
        what: what.to_owned(),
        value: seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_timeouts_convert() {
        assert_eq!(duration("t", 0.0), Ok(Duration::ZERO));
        assert_eq!(duration("t", -0.0), Ok(Duration::ZERO));
        assert_eq!(duration("t", 1.5), Ok(Duration::from_millis(1500)));
        assert!(duration("t", MAX_SECONDS).is_ok());
    }

    #[test]
    fn nothing_that_would_panic_is_accepted() {
        for bad in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -1.0,
            1e30,
            f64::MAX,
            MAX_SECONDS * 2.0,
        ] {
            let refused = duration("timeout", bad).expect_err("refused, not a panic");
            assert_eq!(refused.what(), "timeout");
            assert!(refused.to_string().contains("timeout"), "{refused}");
        }
    }
}
