//! A credential held as text.

use zeroize::Zeroizing;

/// An OpenBao token, an AppRole secret id or a wrapping token.
///
/// It is **wiped from memory when it is dropped**, and nothing it prints says
/// what it holds: a value that reaches a `tracing` field, a panic message or a
/// `dbg!` must not be a way to read a credential, which is the rule
/// [`weida::Pem`] follows for the same reason. It has no `PartialEq`,
/// `Display` or `Deref` either; reading it is the deliberate call
/// [`Secret::expose`].
///
/// The copies the HTTP stack makes on the way out — the header a token is sent
/// in, the buffer an answer is read into — are not this crate's to wipe.
#[derive(Clone)]
pub struct Secret(Zeroizing<String>);

impl Secret {
    /// Takes ownership of `value`, which is wiped when the secret is dropped.
    pub fn new(value: impl Into<String>) -> Secret {
        Secret(Zeroizing::new(value.into()))
    }

    /// The credential itself. Keep the borrow short, and do not copy it into a
    /// `String` that is not wiped.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Secret {
        Secret::new(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Secret {
        Secret::new(value)
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_prints_nothing_of_what_it_holds() {
        let secret = Secret::new("s3cret-token");
        assert!(!format!("{secret:?}").contains("s3cret"));
        assert!(!format!("{secret:#?}").contains("s3cret"));
        assert_eq!(secret.expose(), "s3cret-token");
    }
}
