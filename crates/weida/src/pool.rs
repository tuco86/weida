//! Client connection pool.
//!
//! One QUIC endpoint per runtime and **one connection per dialled endpoint
//! path** ([decisions/0002](../../../docs/decisions/0002-control-and-bulk-separation.md)
//! §6.2). The path is part of the key, so a slow reader on one path cannot
//! stall a writer on another: they share no connection and therefore no
//! receive window, which is the only isolation QUIC actually gives
//! (`docs/PATTERNS.md` §1.3).
//!
//! The rest of the key is the authority as written, the trust configuration
//! and the fingerprint the address named, rather than the resolved address:
//! the TLS server name comes from the same string, so two spellings of one
//! address are genuinely different peers as far as authentication is
//! concerned.
//!
//! **The connections of one peer are bound together by the proved
//! fingerprint, and by nothing else**
//! ([decisions/0008](../../../docs/decisions/0008-session-identity.md) §4.2).
//! Dialling a second path is dialling the same peer, so the pool checks that
//! the peer that answers is the one that answered before and refuses the
//! connection otherwise — the load-balancer case, where two dials to one
//! authority reach two different servers.

use std::collections::HashMap;
use std::sync::Arc;

use quinn::VarInt;
use tokio::sync::Mutex;
use weida_core::{EndpointAddr, Error, Fingerprint, Limits};
use weida_protocol::codes;

use crate::config::{ClientTls, Discovery, RuntimeConfig};
use crate::conn::{ConnCtx, ConnHandle, conn_error};
use crate::listener::Namespace;
use crate::runtime::{Exec, Shared};
use crate::tls;
use crate::transport::{Link, QuicLink};

/// The port a dial uses, and the refusal a `Single` runtime owes a portless
/// URL.
///
/// Separated out so the rule is testable without a socket: it is the whole of
/// [0020](../../../docs/decisions/0020-cluster-and-discovery.md) §4.2's client
/// half.
fn dial_port(written: Option<u16>, discovery: Discovery, host: &str) -> Result<u16, Error> {
    match (written, discovery) {
        (Some(port), _) => Ok(port),
        (None, Discovery::Aware) => Ok(weida_core::DEFAULT_PORT),
        (None, Discovery::Single) => Err(Error::InvalidAddress(format!(
            "{host} names no port, which means a set of nodes, and this runtime is \
             configured `Discovery::Single`: write the port, or allow discovery"
        ))),
    }
}

pub(crate) struct ClientPool {
    state: Mutex<PoolState>,
    /// Handed to every connection this pool dials; owned by the runtime.
    shared: Arc<Shared>,
}

/// Which **peer** a connection belongs to: authority, trust configuration and
/// the fingerprint the address named. Two endpoints dialling the same
/// `host:port` on different terms are not the same peer — one was
/// authenticated on one set of terms, not both.
type PeerKey = (String, u16, Arc<ClientTls>, Option<Fingerprint>);

/// Which **connection**: a peer plus the endpoint path it was dialled for.
type ConnKey = (PeerKey, String);

#[derive(Default)]
struct PoolState {
    endpoint: Option<quinn::Endpoint>,
    connections: HashMap<ConnKey, ConnHandle>,
}

impl ClientPool {
    pub(crate) fn new(shared: Arc<Shared>) -> ClientPool {
        ClientPool {
            state: Mutex::new(PoolState::default()),
            shared,
        }
    }

    /// Removes and returns the client endpoint, for shutdown.
    pub(crate) async fn take_endpoint(&self) -> Option<quinn::Endpoint> {
        let mut state = self.state.lock().await;
        state.connections.clear();
        state.endpoint.take()
    }

    /// Returns a live connection to `path` on `host:port`, authenticated on
    /// `tls`'s terms — and, when the address named one, as `expected` —
    /// dialling if necessary.
    ///
    /// One connection per path: two paths on one peer never share a
    /// connection, so neither can stall the other. The returned handle has
    /// completed the HELLO exchange, so callers never have to think about
    /// negotiation, and the peer it proved has been checked against the
    /// identity this peer proved on its other connections.
    ///
    /// A new connection is dialled with `limits`: the runtime's, or a
    /// swarm's with larger receive windows. They are not part of the key,
    /// because only a swarm dials a seeder's path, so one key is always
    /// dialled with the same limits.
    pub(crate) async fn connect(
        &self,
        config: &RuntimeConfig,
        limits: &Limits,
        exec: &Exec,
        addr: &EndpointAddr,
        tls: &Arc<ClientTls>,
    ) -> Result<ConnHandle, Error> {
        let EndpointAddr {
            host,
            port,
            path,
            peer: expected,
        } = addr;
        let (host, expected) = (host.as_str(), *expected);
        // A portless authority is a set (0020 §4.2); which port that set
        // listens on is not a guess but a constant, and whether a set is
        // allowed at all is the runtime's configuration.
        let written_port = *port;
        let port = dial_port(written_port, config.discovery, host)?;
        let peer: PeerKey = (host.to_owned(), port, Arc::clone(tls), expected);
        let key: ConnKey = (peer.clone(), path.clone());
        // What the pool has to say is read under the lock and the lock is
        // then **released for the dial**. Holding it across name resolution,
        // the handshake attempts and the HELLO exchange — seconds of a
        // peer's choosing — serialized every dial in the process against
        // every other, and made `Runtime::close`, which takes this same
        // lock to retire the client endpoint, wait on a peer: 0009 §4.4
        // forbids exactly that.
        //
        // The cost of releasing it is that two dials for one key can both
        // build a connection. That race is settled below, where the loser
        // closes its own and returns the winner's, and it costs one extra
        // handshake rather than a lock that spans the network.
        let (endpoint, known) = {
            let mut state = self.state.lock().await;
            if let Some(existing) = state.connections.get(&key) {
                if existing.conn.close_reason().is_none() {
                    return Ok(ConnHandle::clone(existing));
                }
                state.connections.remove(&key);
            }
            // A connection whose key is never dialled again would otherwise
            // be retained forever, and with it its namespace, its dedup
            // window, its reorder hold and its driver task.
            state
                .connections
                .retain(|_, handle| handle.conn.close_reason().is_none());
            let endpoint = match &state.endpoint {
                Some(endpoint) => endpoint.clone(),
                None => {
                    let endpoint = config.network.bind_client(exec)?;
                    state.endpoint = Some(endpoint.clone());
                    endpoint
                }
            };
            (endpoint, state.peer_identity(&peer))
        };

        let (client_config, refused) = tls::client_config(tls, expected, limits)?;

        let addrs = config
            .resolver
            .resolve(exec, host, written_port, config.max_resolved_addresses)
            .await?;
        // Every address, in the resolver's order, until one answers. The first
        // is not necessarily reachable: `localhost` commonly resolves to `::1`
        // and `127.0.0.1`, and a server bound to one of them refuses the
        // other. The last error is what the caller sees when none answer,
        // because it is the one that describes the last thing tried.
        let mut last: Option<Error> = None;
        let mut established = None;
        for (index, addr) in addrs.iter().enumerate() {
            let is_last = index + 1 == addrs.len();
            tracing::debug!(%addr, host, is_last, "dialling");
            let connecting = {
                // Inside the runtime context: `quinn` reads its own runtime
                // out of the ambient reactor while it builds the attempt.
                let _guard = exec.enter();
                match endpoint.connect_with(client_config.clone(), *addr, host) {
                    Ok(connecting) => connecting,
                    Err(e) => {
                        last = Some(Error::Transport(format!("connect to {addr} failed: {e}")));
                        continue;
                    }
                }
            };
            // The handshake is driven *on* the runtime rather than by whoever
            // awaits `connect`: completing it spawns the connection driver,
            // and the caller's executor need not be tokio. Awaiting the join
            // handle needs nothing.
            let attempt = exec.spawn(connecting);
            // Every address but the last is bounded: an address that answers
            // nothing gives QUIC no refusal to observe, so an unbounded
            // attempt on an unreachable `::1` would reach `127.0.0.1` only
            // after a full handshake timeout. The last one keeps the whole
            // budget, which is also what an IP literal and a single-address
            // name get
            // (`RuntimeConfig::connect_attempt_timeout`, RFC 8305's
            // Connection Attempt Delay).
            let joined = if is_last {
                Some(attempt.await)
            } else {
                exec.within(config.connect_attempt_timeout, attempt).await
            };
            let handshake = match joined {
                Some(joined) => {
                    joined.map_err(|e| Error::Runtime(format!("dial task failed: {e}")))?
                }
                None => {
                    last = Some(Error::Transport(format!(
                        "connect to {addr} did not answer within {:?}",
                        config.connect_attempt_timeout
                    )));
                    continue;
                }
            };
            match handshake {
                Ok(conn) => {
                    established = Some(conn);
                    break;
                }
                Err(e) => {
                    // The verifier saw the peer before the handshake died:
                    // report who answered, so the operator can decide whether
                    // to pin it. A refusal is about *this* peer's key and is
                    // not retried against another address.
                    let refused = refused.lock().expect("refusal record poisoned").take();
                    if let Some(presented) = refused {
                        return Err(Error::Untrusted(presented));
                    }
                    last = Some(conn_error(e));
                }
            }
        }
        let conn = match established {
            Some(conn) => conn,
            None => {
                return Err(last.unwrap_or_else(|| {
                    Error::InvalidAddress(format!("{host}:{port} resolved to no addresses"))
                }));
            }
        };

        // The connections of one peer are bound together by the proved
        // fingerprint and by nothing else (0008 §4.2). Dialling a second path
        // is dialling the same peer, so a connection that proved a different
        // identity than this peer's *live* connections is not this peer: it
        // is a second server behind one authority, and serving a transfer on
        // it would mean talking to somebody the caller never asked for.
        //
        // The comparison is against live connections rather than a remembered
        // fingerprint, because a peer is this peer only while a connection to
        // it lives: once the last one is gone, a replacement server with a new
        // key is a new peer and nothing should still be objecting to it.
        let presented = crate::tls::peer_fingerprint(&conn);
        if let Some(known) = known
            && known != presented
        {
            conn.close(
                VarInt::from_u32(codes::SHUTDOWN as u32),
                b"peer identity differs from this peer's other connections",
            );
            // `Untrusted` carries who answered, which is the fingerprint an
            // operator needs to decide whether to pin it. An anonymous answer
            // cannot be reported that way, and cannot be bound to anything
            // either.
            return Err(match presented {
                Some(fp) => Error::Untrusted(fp),
                None => Error::Tls(
                    "the peer that answered proved no identity, so it cannot be the peer this \
                     runtime is already connected to"
                        .into(),
                ),
            });
        }

        // A fresh namespace per client connection: a subscriber registers its
        // path here so fanned-out copies have somewhere to go. It is not the
        // listener's namespace — a client serves nothing on its own account.
        let handle = ConnCtx::spawn(
            Link::Quic(QuicLink::new(conn)),
            *limits,
            Arc::new(Namespace::new()),
            None,
            exec.clone(),
            config.guarantees,
            Arc::clone(&self.shared),
        );
        // Negotiation must complete before the caller can send anything: a
        // DATA frame ahead of our own HELLO would be parked by the peer, and a
        // version mismatch must fail `connect`, not the first request.
        handle.negotiated().await?;

        // The race the released lock allows, settled: whoever inserted first
        // owns the key, and this connection is closed rather than left open
        // for nobody. Both sides then hand their caller the same connection,
        // which is what a pool is for.
        let mut state = self.state.lock().await;
        let winner = state
            .connections
            .get(&key)
            .filter(|held| held.conn.close_reason().is_none())
            .map(ConnHandle::clone);
        if let Some(winner) = winner {
            handle
                .conn
                .close(codes::SHUTDOWN, "a concurrent dial won this pool key");
            return Ok(winner);
        }
        state.connections.insert(key, ConnHandle::clone(&handle));
        Ok(handle)
    }
}

impl PoolState {
    /// The identity this peer's live connections proved, if it has any.
    ///
    /// `Some(None)` is a peer whose live connections proved nothing at all
    /// and is a real answer, distinct from `None` — no live connection, so
    /// nothing to be consistent with.
    fn peer_identity(&self, peer: &PeerKey) -> Option<Option<Fingerprint>> {
        self.connections
            .iter()
            .find(|((p, _), handle)| p == peer && handle.conn.close_reason().is_none())
            // The pool is a QUIC structure, and what it compares is the key
            // the peer proved; a local principal never reaches it.
            .map(|(_, handle)| handle.peer.as_ref().and_then(|id| id.key()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: a written port is used as written, a portless authority is the
    /// discovery form and dials `DEFAULT_PORT`, and a `Single` runtime
    /// **refuses** the portless form rather than resolving it — 0020 §4.2's
    /// client half.
    #[test]
    fn the_dial_port_follows_the_url_then_the_discovery_mode() {
        assert_eq!(
            dial_port(Some(9000), Discovery::Aware, "h").expect("written"),
            9000
        );
        assert_eq!(
            dial_port(Some(9000), Discovery::Single, "h").expect("written"),
            9000,
            "a written port is never overridden by the mode"
        );
        assert_eq!(
            dial_port(None, Discovery::Aware, "jobs.example").expect("discovered"),
            weida_core::DEFAULT_PORT
        );

        let refused = dial_port(None, Discovery::Single, "jobs.example")
            .expect_err("a set is not one endpoint");
        let message = refused.to_string();
        assert!(
            message.contains("jobs.example") && message.contains("Discovery::Single"),
            "the refusal names the host and the reason: {message}"
        );
    }
}
