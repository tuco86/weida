//! The host a remote peer counts as.
//!
//! A listener that bounds what one peer may hold has to decide what "one peer"
//! is, and the socket address is the wrong answer: an IPv6 host is routed a
//! whole `/64` and picks the rest of the address itself, and a dual-stack
//! listener sees an IPv4 client under two spellings. Every transport crate that
//! keeps such a ceiling asks this module, so that they bound the same threat the
//! same way and the rule has one place to be wrong in.

use std::net::{IpAddr, Ipv6Addr};

/// How a per-host ceiling counts the peer at one address.
///
/// Built by [`host_key`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostKey {
    /// What the ceiling counts the peer under: its IPv4 address, or its IPv6
    /// address with the low 64 bits cleared.
    ///
    /// Count under this and not under the whole `HostKey`: [`HostKey::loopback`]
    /// is a fact about the address and not part of the host, and two addresses
    /// of one `/64` can disagree on it.
    pub key: IpAddr,
    /// Whether the peer is this machine's own processes, in any form.
    ///
    /// A ceiling that exists to stop a stranger from holding the table has
    /// nothing to say about loopback, which a test or a sidecar legitimately
    /// opens by the thousand; whether to count it anyway is the caller's
    /// decision, and this is what it needs to make it.
    pub loopback: bool,
}

/// The key a per-host ceiling counts the peer at `ip` under, and whether that
/// peer is local. A listener takes `ip` from the accepted connection's
/// [`SocketAddr::ip`](std::net::SocketAddr::ip).
///
/// An IPv4 peer is counted by its address and an IPv6 peer by its `/64`. An
/// IPv6 host is routed a whole `/64`, so a count per address would be a count
/// per suffix the host chooses: one host could hold every slot of a table while
/// staying under the ceiling of each address it connects from. An IPv4-mapped
/// IPv6 address — what an IPv4 client looks like to a dual-stack listener,
/// `tcp://[::]:port` — is counted as the IPv4 address it carries, so one host
/// is one key however it reaches the listener.
///
/// Loopback is judged on the canonical address and **before** the `/64` is
/// taken. `::ffff:127.0.0.1` is loopback like `127.0.0.1`, and `::2` is not,
/// though it shares `::1`'s `/64` and therefore its key.
pub fn host_key(ip: IpAddr) -> HostKey {
    let ip = ip.to_canonical();
    let key = match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            let [a, b, c, d, ..] = v6.segments();
            IpAddr::V6(Ipv6Addr::new(a, b, c, d, 0, 0, 0, 0))
        }
    };
    HostKey {
        key,
        loopback: ip.is_loopback(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(text: &str) -> HostKey {
        host_key(text.parse().expect("an address"))
    }

    fn address(text: &str) -> IpAddr {
        text.parse().expect("an address")
    }

    /// Claim: an IPv4 peer is counted by its own address, and two addresses are
    /// two hosts.
    #[test]
    fn an_ipv4_peer_is_its_own_key() {
        assert_eq!(of("192.0.2.1").key, address("192.0.2.1"));
        assert_ne!(of("192.0.2.1").key, of("192.0.2.2").key);
    }

    /// Claim: an IPv6 host is its `/64`: every address of one is one key, and
    /// the key is the prefix with the host part cleared.
    #[test]
    fn an_ipv6_peer_is_its_slash_64() {
        let host = of("2001:db8:0:1::1").key;
        assert_eq!(host, address("2001:db8:0:1::"));
        assert_eq!(host, of("2001:db8:0:1:ffff:ffff:ffff:ffff").key);
        assert_eq!(host, of("2001:db8:0:1:dead:beef:0:7").key);
        assert_ne!(
            host,
            of("2001:db8:0:2::1").key,
            "the next /64 is another host"
        );
        assert_ne!(
            host,
            of("2001:db8:1:1::1").key,
            "a difference in the prefix is another host"
        );
    }

    /// Claim: an IPv4 client of a dual-stack listener is the IPv4 host it
    /// carries, so the mapped and the plain form are one key.
    #[test]
    fn an_ipv4_mapped_address_is_the_ipv4_host_it_carries() {
        assert_eq!(of("::ffff:192.0.2.1").key, of("192.0.2.1").key);
        assert_eq!(of("::ffff:192.0.2.1").key, address("192.0.2.1"));
        assert_ne!(of("::ffff:192.0.2.1").key, of("::ffff:192.0.2.2").key);
    }

    /// Claim: loopback is recognised in every form, judged on the canonical
    /// address and before the `/64` is taken — so `::2`, which shares `::1`'s
    /// key, is not loopback, and the flag and not the key is what tells them
    /// apart.
    #[test]
    fn loopback_is_judged_on_the_address_and_not_on_the_key() {
        for local in ["127.0.0.1", "127.255.255.254", "::1", "::ffff:127.0.0.1"] {
            assert!(of(local).loopback, "{local} is loopback");
        }
        for remote in ["192.0.2.1", "2001:db8::1", "::", "::2", "::ffff:192.0.2.1"] {
            assert!(!of(remote).loopback, "{remote} is not loopback");
        }
        assert_eq!(of("::1").key, of("::2").key);
        assert_ne!(of("::1").loopback, of("::2").loopback);
    }
}
