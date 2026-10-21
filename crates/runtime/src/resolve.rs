//! Name resolution, and the seam that lets an application replace it.
//!
//! A name becomes a set of addresses, and **how** is not this library's
//! decision to fix. The default is the system resolver, which answers A and
//! AAAA and therefore one port for the whole set; that is exactly right for a
//! Kubernetes headless Service, where every Pod listens on the same container
//! port, and exactly wrong for a cloud load balancer, where one IP forwards
//! several ports to several backends. The second case is what DNS SRV
//! expresses — one host, one record per port — and what no A/AAAA answer can.
//!
//! So the shape is the one this workspace already uses for the decisions that
//! belong to the application: `Trust` decides what a dial believes,
//! `weida-amqp` takes the caller's `rustls::ClientConfig`, and a
//! [`Resolver`] decides what a name means. Shipping only the system resolver
//! and calling SRV unsupported would put a DNS stack in everybody's dependency
//! graph to serve one deployment shape; refusing SRV outright would make that
//! deployment shape unreachable. A seam does neither.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use weida_core::{DEFAULT_PORT, Error};

use crate::exec::Exec;

/// The future a [`Resolver`] returns.
///
/// Boxed, because a resolver is held as a trait object in configuration and
/// `async fn` in a trait is not dyn-compatible. One allocation per dial, on a
/// path that is already doing a DNS round trip.
pub type Resolved<'a> = Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, Error>> + Send + 'a>>;

/// What a name means.
///
/// Implement this to answer a `weida://` authority with whatever the
/// deployment actually uses — DNS SRV, a service registry, the cloud
/// provider's API, or a table in a configuration file. The default is
/// [`SystemResolver`].
///
/// # The contract
///
/// - **Order matters.** The addresses are dialled in the order returned, so a
///   resolver that knows a preference expresses it by sorting. The caller
///   tries the next one when a dial fails.
/// - **`port` is the port the URL wrote, or `None` when it wrote none.**
///   `None` means the authority names a *set*
///   ([decisions/0020](../../../docs/decisions/0020-cluster-and-discovery.md)
///   §4.2), and it is the resolver that decides which ports that set listens
///   on: the system resolver uses [`DEFAULT_PORT`], an SRV resolver uses the
///   ports the records carry.
/// - **`max_addresses` is a cap, not a hint.** A resolver answer is remote
///   input, so returning more than asked for is a bug in the resolver
///   (`docs/INVARIANTS.md`); the caller does not re-truncate.
/// - **An empty answer is an error**, not an empty set: a caller with no
///   address has nothing to dial and deserves the reason.
///
/// # The `Exec`
///
/// Resolution runs on the runtime the library was given, not on an ambient
/// one: `tokio::net::lookup_host` needs a Tokio context, and a library that
/// demanded its caller be inside one would be the mistake `Exec` exists to
/// prevent. A resolver that spawns work does so through the handle it is
/// passed.
///
/// # Deadlines
///
/// A caller's deadline around [`Resolver::resolve`] ends the *wait*, not the
/// work: the future is dropped, but a lookup that blocks a thread (the system
/// resolver's `getaddrinfo`) runs on until the OS answers. A resolver that
/// does that should bound how many such lookups it leaves outstanding, as
/// [`SystemResolver`] does with [`MAX_OUTSTANDING_LOOKUPS`].
pub trait Resolver: fmt::Debug + Send + Sync + 'static {
    /// Resolves `name` into the addresses to dial, in order.
    fn resolve<'a>(
        &'a self,
        exec: &'a Exec,
        name: &'a str,
        port: Option<u16>,
        max_addresses: usize,
    ) -> Resolved<'a>;
}

/// The system resolver: an IP literal in place, everything else through
/// `getaddrinfo`.
///
/// What it answers is A and AAAA records, so the whole set shares one port —
/// the one the URL wrote, or [`DEFAULT_PORT`] when it wrote none. That covers
/// a literal, a plain name and a Kubernetes headless Service, whose A/AAAA
/// answer *is* the set of Pod addresses. It cannot express a port-forwarding
/// load balancer; [`Resolver`] is how that deployment brings its own answer.
///
/// # A deadline does not end a lookup
///
/// `getaddrinfo` blocks an OS thread and cannot be cancelled. A caller that
/// puts a deadline around [`Resolver::resolve`] (a connect timeout, a
/// handshake budget, [`Exec::within`]) stops *waiting* when it expires, but the
/// thread running the lookup keeps going until the system resolver answers,
/// which can be as long as its own retry schedule (tens of seconds). To keep
/// a name server that never answers from accumulating blocked threads, at
/// most [`MAX_OUTSTANDING_LOOKUPS`] lookups run at once in the process; a
/// lookup asked for beyond that is refused with [`Error::LimitExceeded`]
/// instead of queueing. An IP literal never counts, because it is parsed in
/// place.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemResolver;

/// The most system name lookups ([`SystemResolver`]) in flight in one process.
///
/// Each one holds a blocking-pool thread inside `getaddrinfo` for as long as
/// the system resolver takes, and a caller's deadline does not shorten that.
/// The cap keeps a name server that stops answering from turning every
/// timed-out dial into one more stuck thread. A slot frees when the lookup
/// itself returns, not when the caller that asked for it gave up.
pub const MAX_OUTSTANDING_LOOKUPS: usize = 64;

/// The lookups of this process that have not returned yet.
static LOOKUPS: Lookups = Lookups::new(MAX_OUTSTANDING_LOOKUPS);

/// A count of lookups in flight, with a ceiling.
struct Lookups {
    cap: usize,
    outstanding: AtomicUsize,
}

/// One lookup's place under the ceiling; freed when the lookup returns.
struct Slot(&'static Lookups);

impl Lookups {
    const fn new(cap: usize) -> Lookups {
        Lookups {
            cap,
            outstanding: AtomicUsize::new(0),
        }
    }

    /// Takes a place, or `None` when `cap` lookups are already in flight.
    fn claim(&'static self) -> Option<Slot> {
        let mut taken = self.outstanding.load(Ordering::Acquire);
        loop {
            if taken >= self.cap {
                return None;
            }
            match self.outstanding.compare_exchange_weak(
                taken,
                taken + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Slot(self)),
                Err(seen) => taken = seen,
            }
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.outstanding.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Runs `work` on the runtime under one of `lookups`' places.
///
/// The place travels with the spawned task, not with this future: dropping
/// this future (a caller's deadline) detaches the task, which keeps its place
/// until `work` returns.
async fn bounded<F>(
    exec: &Exec,
    lookups: &'static Lookups,
    work: F,
) -> Result<std::io::Result<Vec<SocketAddr>>, Error>
where
    F: Future<Output = std::io::Result<Vec<SocketAddr>>> + Send + 'static,
{
    let slot = lookups.claim().ok_or(Error::LimitExceeded)?;
    exec.spawn(async move {
        let answer = work.await;
        drop(slot);
        answer
    })
    .await
    .map_err(|e| Error::Runtime(format!("name resolution task failed: {e}")))
}

impl Resolver for SystemResolver {
    fn resolve<'a>(
        &'a self,
        exec: &'a Exec,
        name: &'a str,
        port: Option<u16>,
        max_addresses: usize,
    ) -> Resolved<'a> {
        Box::pin(async move {
            let port = port.unwrap_or(DEFAULT_PORT);
            if let Ok(ip) = name.parse::<IpAddr>() {
                return Ok(vec![SocketAddr::new(ip, port)]);
            }
            let query = (name.to_owned(), port);
            let looked_up = bounded(exec, &LOOKUPS, async move {
                tokio::net::lookup_host(query)
                    .await
                    .map(|addrs| addrs.collect::<Vec<SocketAddr>>())
            })
            .await?;
            let addrs: Vec<SocketAddr> = looked_up
                .map_err(|e| Error::InvalidAddress(format!("cannot resolve {name}:{port}: {e}")))?
                .into_iter()
                .take(max_addresses)
                .collect();
            if addrs.is_empty() {
                return Err(Error::InvalidAddress(format!(
                    "{name}:{port} resolved to no addresses"
                )));
            }
            Ok(addrs)
        })
    }
}

/// The resolver a configuration holds.
///
/// An `Arc` because a runtime's configuration is cloned per endpoint and a
/// resolver may hold a cache, a connection to a registry or nothing at all.
#[derive(Clone, Debug)]
pub struct SharedResolver(Arc<dyn Resolver>);

impl SharedResolver {
    /// Wraps a resolver.
    pub fn new(resolver: impl Resolver) -> SharedResolver {
        SharedResolver(Arc::new(resolver))
    }

    /// Resolves through the wrapped resolver.
    pub fn resolve<'a>(
        &'a self,
        exec: &'a Exec,
        name: &'a str,
        port: Option<u16>,
        max_addresses: usize,
    ) -> Resolved<'a> {
        self.0.resolve(exec, name, port, max_addresses)
    }
}

impl Default for SharedResolver {
    fn default() -> SharedResolver {
        SharedResolver::new(SystemResolver)
    }
}

impl PartialEq for SharedResolver {
    /// Two shared resolvers are equal when they are the same resolver.
    ///
    /// Pointer identity, because a resolver is an implementation and not a
    /// value: the connection pool keys on configuration, and two different
    /// resolvers must not be treated as one even if they happen to answer the
    /// same way today.
    fn eq(&self, other: &SharedResolver) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for SharedResolver {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A resolver that answers from a table, which is what a test needs and
    /// also what a static deployment would write.
    #[derive(Debug)]
    struct Table(Vec<SocketAddr>);

    impl Resolver for Table {
        fn resolve<'a>(
            &'a self,
            _exec: &'a Exec,
            _name: &'a str,
            _port: Option<u16>,
            max_addresses: usize,
        ) -> Resolved<'a> {
            Box::pin(async move { Ok(self.0.iter().copied().take(max_addresses).collect()) })
        }
    }

    #[tokio::test]
    async fn a_literal_needs_no_resolver_and_takes_the_written_port() {
        let exec = Exec::current().expect("ambient runtime");
        let addrs = SystemResolver
            .resolve(&exec, "127.0.0.1", Some(9000), 8)
            .await
            .expect("literal");
        assert_eq!(addrs, vec!["127.0.0.1:9000".parse().expect("addr")]);
    }

    #[tokio::test]
    async fn a_literal_without_a_written_port_takes_the_default() {
        let exec = Exec::current().expect("ambient runtime");
        let addrs = SystemResolver
            .resolve(&exec, "127.0.0.1", None, 8)
            .await
            .expect("literal");
        assert_eq!(addrs[0].port(), DEFAULT_PORT);
    }

    /// The load-balancer shape: one address, several ports, which no A/AAAA
    /// answer can express and a replaced resolver can.
    #[tokio::test]
    async fn a_replaced_resolver_may_answer_several_ports_on_one_address() {
        let exec = Exec::current().expect("ambient runtime");
        let table = SharedResolver::new(Table(vec![
            "203.0.113.7:7443".parse().expect("addr"),
            "203.0.113.7:7444".parse().expect("addr"),
            "203.0.113.7:7445".parse().expect("addr"),
        ]));
        let addrs = table
            .resolve(&exec, "lb.example", None, 8)
            .await
            .expect("table");
        assert_eq!(addrs.len(), 3);
        assert!(addrs.iter().all(|a| a.ip().to_string() == "203.0.113.7"));
        assert_eq!(
            addrs.iter().map(|a| a.port()).collect::<Vec<_>>(),
            vec![7443, 7444, 7445],
            "the order is the resolver's and is preserved"
        );
    }

    #[tokio::test]
    async fn the_cap_is_the_callers_and_the_resolver_honours_it() {
        let exec = Exec::current().expect("ambient runtime");
        let table = SharedResolver::new(Table(vec![
            "203.0.113.7:7443".parse().expect("addr"),
            "203.0.113.7:7444".parse().expect("addr"),
            "203.0.113.7:7445".parse().expect("addr"),
        ]));
        let addrs = table
            .resolve(&exec, "lb.example", None, 2)
            .await
            .expect("table");
        assert_eq!(addrs.len(), 2, "a resolver answer is remote input");
    }

    #[test]
    fn two_resolvers_are_equal_only_when_they_are_the_same_one() {
        let one = SharedResolver::default();
        let same = one.clone();
        let other = SharedResolver::default();
        assert_eq!(one, same);
        assert_ne!(
            one, other,
            "identical behaviour is not identity: the pool keys on this"
        );
    }

    #[tokio::test]
    async fn resolves_ip_literals_without_dns() {
        let exec = Exec::current().expect("ambient runtime");
        assert_eq!(
            SystemResolver
                .resolve(&exec, "127.0.0.1", Some(7443), 8)
                .await
                .expect("v4"),
            vec![SocketAddr::from(([127, 0, 0, 1], 7443))]
        );
        let v6 = SystemResolver
            .resolve(&exec, "::1", Some(7443), 8)
            .await
            .expect("v6");
        assert_eq!(v6.len(), 1);
        assert_eq!(v6[0].port(), 7443);
        assert!(v6[0].is_ipv6());
    }

    /// Claim: a deadline that ends the wait does not free the lookup's place,
    /// because the thread behind it is still in `getaddrinfo`; a lookup asked
    /// for beyond the cap is refused rather than queued, and a place frees
    /// when the lookup returns.
    #[tokio::test(start_paused = true)]
    async fn a_lookup_the_caller_gave_up_on_keeps_its_place_until_it_returns() {
        static CAPPED: Lookups = Lookups::new(2);
        let exec = Exec::current().expect("ambient runtime");
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let stuck = |gate: Arc<tokio::sync::Semaphore>| async move {
            let _ = gate.acquire().await;
            Ok(Vec::new())
        };
        for _ in 0..2 {
            let waited = exec
                .within(
                    Duration::from_millis(20),
                    bounded(&exec, &CAPPED, stuck(Arc::clone(&gate))),
                )
                .await;
            assert!(waited.is_none(), "the lookup is still blocked");
        }

        let refused = bounded(&exec, &CAPPED, stuck(Arc::clone(&gate))).await;
        assert!(
            matches!(refused, Err(Error::LimitExceeded)),
            "both places are held by lookups nobody waits for any more"
        );

        gate.add_permits(2);
        exec.within(Duration::from_secs(5), async {
            while CAPPED.outstanding.load(Ordering::Acquire) != 0 {
                exec.sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the places free once the lookups return");
        bounded(&exec, &CAPPED, async { Ok(Vec::new()) })
            .await
            .expect("admitted again")
            .expect("answered");
    }

    /// Tests that touch the filesystem, network, processes or real time: the
    /// integration tier (`.config/nextest.toml`).
    mod io {
        use super::*;

        #[tokio::test]
        async fn an_unresolvable_name_is_an_error_rather_than_an_empty_set() {
            let exec = Exec::current().expect("ambient runtime");
            let err = SystemResolver
                .resolve(&exec, "no-such-host.invalid", Some(1), 8)
                .await
                .expect_err("`.invalid` never resolves");
            assert!(
                err.to_string().contains("no-such-host.invalid"),
                "the error names what could not be resolved: {err}"
            );
        }
        /// Claim: a hostname yields every address the resolver offers, in its
        /// order and no more than the cap. `localhost` is the case that matters —
        /// it commonly resolves to both `::1` and `127.0.0.1`, and dialling only
        /// the first reaches a server bound to the other never.
        #[tokio::test]
        async fn a_hostname_resolves_to_every_address_up_to_the_cap() {
            let exec = Exec::current().expect("ambient runtime");
            let all = SystemResolver
                .resolve(&exec, "localhost", Some(7443), 8)
                .await
                .expect("localhost");
            assert!(!all.is_empty());
            assert!(all.iter().all(|a| a.port() == 7443));

            let capped = SystemResolver
                .resolve(&exec, "localhost", Some(7443), 1)
                .await
                .expect("localhost");
            assert_eq!(capped.len(), 1, "the cap must bound the answer");
            assert_eq!(capped[0], all[0], "and it must keep the resolver's order");
        }
    }
}
