//! Connections per origin.
//!
//! An HTTP/1.1 connection carries one exchange at a time, so a client keeps
//! the ones that finished cleanly and hands the newest back out for the next
//! request to the same origin: at most `max_idle_per_origin` of them, none
//! older than `idle_timeout`. An HTTP/2 connection carries every exchange to
//! its origin at once, so an origin has at most one, shared.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use weida_http_codec::uri::Scheme;
use weida_runtime::Exec;

use crate::h1::Conn;
use crate::h2::H2Handle;

/// An origin: scheme, lowercase host, port (RFC 9110 §4.3.1).
pub(crate) type OriginKey = (Scheme, String, u16);

#[derive(Default)]
struct Origin {
    /// Oldest first.
    idle_h1: Vec<(Instant, Conn)>,
    /// The one HTTP/2 connection every request to the origin shares.
    shared: Option<H2Handle>,
}

/// The idle connections of one client.
pub(crate) struct Pool {
    exec: Exec,
    origins: Mutex<HashMap<OriginKey, Origin>>,
    max_idle: usize,
    idle_timeout: Duration,
}

impl Pool {
    pub(crate) fn new(exec: Exec, max_idle: usize, idle_timeout: Duration) -> Pool {
        Pool {
            exec,
            origins: Mutex::new(HashMap::new()),
            max_idle,
            idle_timeout,
        }
    }

    fn origins(&self) -> std::sync::MutexGuard<'_, HashMap<OriginKey, Origin>> {
        self.origins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The newest idle HTTP/1.1 connection to `key` that is younger than the
    /// idle timeout; older ones are dropped on the way.
    pub(crate) fn take_h1(&self, key: &OriginKey) -> Option<Conn> {
        let now = self.exec.now();
        let mut origins = self.origins();
        let origin = origins.get_mut(key)?;
        origin
            .idle_h1
            .retain(|(since, _)| now.saturating_duration_since(*since) < self.idle_timeout);
        let conn = origin.idle_h1.pop().map(|(_, conn)| conn);
        if origin.idle_h1.is_empty() && origin.shared.is_none() {
            origins.remove(key);
        }
        conn
    }

    /// Keeps `conn` for reuse, dropping the oldest beyond the cap.
    pub(crate) fn put_h1(&self, key: OriginKey, conn: Conn) {
        if self.max_idle == 0 {
            return;
        }
        let now = self.exec.now();
        let mut origins = self.origins();
        let origin = origins.entry(key).or_default();
        origin.idle_h1.push((now, conn));
        while origin.idle_h1.len() > self.max_idle {
            origin.idle_h1.remove(0);
        }
    }

    /// The origin's shared HTTP/2 connection, if it can still open streams;
    /// one that cannot is forgotten.
    pub(crate) fn shared(&self, key: &OriginKey) -> Option<H2Handle> {
        let mut origins = self.origins();
        let origin = origins.get_mut(key)?;
        match &origin.shared {
            Some(handle) if handle.is_usable() => Some(handle.clone()),
            Some(_) => {
                origin.shared = None;
                None
            }
            None => None,
        }
    }

    /// Makes `handle` the origin's shared connection unless a usable one is
    /// already there; returns whether it was taken.
    #[cfg(feature = "tls")]
    pub(crate) fn set_shared(&self, key: OriginKey, handle: &H2Handle) -> bool {
        let mut origins = self.origins();
        let origin = origins.entry(key).or_default();
        if origin.shared.as_ref().is_some_and(H2Handle::is_usable) {
            return false;
        }
        origin.shared = Some(handle.clone());
        true
    }

    /// How many idle HTTP/1.1 connections `key` has.
    #[cfg(test)]
    pub(crate) fn idle_count(&self, key: &OriginKey) -> usize {
        self.origins().get(key).map_or(0, |o| o.idle_h1.len())
    }
}

/// Where a connection goes back to when its exchange ended cleanly.
#[derive(Clone)]
pub(crate) struct PoolReturn {
    pub(crate) pool: Weak<Pool>,
    pub(crate) key: OriginKey,
}

impl PoolReturn {
    pub(crate) fn new(pool: &Arc<Pool>, key: OriginKey) -> PoolReturn {
        PoolReturn {
            pool: Arc::downgrade(pool),
            key,
        }
    }

    /// Hands `conn` back, if the client still exists.
    pub(crate) fn put(self, conn: Conn) {
        if let Some(pool) = self.pool.upgrade() {
            pool.put_h1(self.key, conn);
        }
    }
}

impl std::fmt::Debug for PoolReturn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PoolReturn").field(&self.key).finish()
    }
}
