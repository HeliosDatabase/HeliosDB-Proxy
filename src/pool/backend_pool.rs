//! Data-path backend connection pool for Transaction / Statement pooling modes.
//!
//! This is the *raw-stream* pool that actually multiplexes clients onto a
//! bounded set of backend connections — the piece that makes the
//! `pool-modes` feature do real work on the wire. It is deliberately distinct
//! from [`crate::pool::manager::ConnectionPoolManager`], which models pooling
//! over the higher-level `BackendClient` message API; the proxy data path
//! forwards raw PostgreSQL-wire bytes, so it needs a pool of authenticated
//! `TcpStream`s.
//!
//! ## Identity keying (why this is safe)
//!
//! HeliosProxy authenticates backend connections by **passing the client's own
//! credentials through** to PostgreSQL (the client SCRAM handshake is relayed).
//! A parked connection is therefore authenticated as a specific
//! `(user, database)` principal. The pool keys idle connections by
//! `node\0user\0database`, so a connection is only ever handed to a client that
//! connected with the *same* identity — and that client independently
//! authenticated before it could reach the pool. This is exactly PgBouncer's
//! per-(user,db) pooling model; it does not multiplex distinct users onto one
//! backend identity (that would need proxy-terminated auth with a shared
//! backend credential, which is a separate, larger change).
//!
//! ## Cleanliness
//!
//! A connection is `DISCARD ALL`-reset by the caller before it is parked
//! (see the release path in `server.rs`), so the next borrower — possibly a
//! *different* client of the same identity — never inherits GUCs, temp tables,
//! prepared statements, or advisory locks. On checkout the connection is
//! liveness-probed so a peer that closed the socket while idle is dropped
//! rather than handed out.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;

/// Build the pool key for a `(node, user, database)` triple. NUL-delimited so
/// the three components can never collide across boundaries.
pub fn pool_key(node: &str, user: &str, database: &str) -> String {
    format!("{}\0{}\0{}", node, user, database)
}

/// A bounded set of idle, authenticated backend connections, partitioned by
/// connection identity. Cheap to clone-share behind an `Arc`.
pub struct BackendIdlePool {
    /// identity-key -> stack of idle authenticated streams.
    idle: DashMap<String, Vec<TcpStream>>,
    /// Hard cap on idle connections parked per identity key.
    max_idle_per_key: usize,
    /// Checkout hits — a parked connection was reused.
    reuses: AtomicU64,
    /// Connections parked (checked in) successfully.
    parked: AtomicU64,
    /// Check-ins refused because the per-key idle cap was reached (the
    /// connection is closed by the caller dropping it).
    over_capacity: AtomicU64,
    /// Parked connections dropped at checkout because the peer had closed
    /// them (or left unexpected bytes) while idle.
    stale_evicted: AtomicU64,
}

impl BackendIdlePool {
    /// Create a pool that parks at most `max_idle_per_key` connections per
    /// `(node,user,db)` identity. A floor of 1 is enforced so the pool always
    /// retains at least one reusable connection.
    pub fn new(max_idle_per_key: usize) -> Self {
        Self {
            idle: DashMap::new(),
            max_idle_per_key: max_idle_per_key.max(1),
            reuses: AtomicU64::new(0),
            parked: AtomicU64::new(0),
            over_capacity: AtomicU64::new(0),
            stale_evicted: AtomicU64::new(0),
        }
    }

    /// Take a live idle connection for `key`, or `None` if the pool has no
    /// usable one (caller then dials a fresh connection). Dead/stale parked
    /// connections are evicted in passing.
    pub fn checkout(&self, key: &str) -> Option<TcpStream> {
        let mut guard = self.idle.get_mut(key)?;
        while let Some(stream) = guard.pop() {
            if Self::probe_alive(&stream) {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                return Some(stream);
            }
            // Peer closed (or desynced) while idle — drop it and try the next.
            self.stale_evicted.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Park a (freshly reset) connection for reuse under `key`. Returns `false`
    /// when the per-key cap is already reached — in that case the connection is
    /// dropped (closed) by being moved in and discarded, shedding excess
    /// capacity.
    pub fn checkin(&self, key: &str, stream: TcpStream) -> bool {
        let mut entry = self.idle.entry(key.to_string()).or_default();
        if entry.len() >= self.max_idle_per_key {
            self.over_capacity.fetch_add(1, Ordering::Relaxed);
            return false; // `stream` dropped here → socket closed.
        }
        entry.push(stream);
        self.parked.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Liveness probe for an idle parked connection: a clean idle backend has
    /// no pending bytes, so a non-blocking read should report `WouldBlock`.
    /// `Ok(0)` means the peer closed; `Ok(n>0)` means unexpected data (protocol
    /// desync) — both are treated as dead.
    fn probe_alive(stream: &TcpStream) -> bool {
        let mut probe = [0u8; 1];
        matches!(
            stream.try_read(&mut probe),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
        )
    }

    /// Total idle connections currently parked across all identities.
    pub fn idle_count(&self) -> usize {
        self.idle.iter().map(|e| e.value().len()).sum()
    }

    /// Number of checkout hits (connections reused rather than dialed fresh).
    pub fn reuses(&self) -> u64 {
        self.reuses.load(Ordering::Relaxed)
    }

    /// Number of successful check-ins.
    pub fn parked(&self) -> u64 {
        self.parked.load(Ordering::Relaxed)
    }

    /// Number of check-ins refused for exceeding the per-key idle cap.
    pub fn over_capacity(&self) -> u64 {
        self.over_capacity.load(Ordering::Relaxed)
    }

    /// Number of stale connections evicted at checkout.
    pub fn stale_evicted(&self) -> u64 {
        self.stale_evicted.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Open a connected TcpStream pair against a throwaway loopback listener so
    /// tests can exercise the pool's bookkeeping with real (live) sockets.
    async fn live_stream(listener: &TcpListener) -> TcpStream {
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, _server) = tokio::join!(connect, accept);
        // Keep the server side alive by leaking it into a long-lived holder via
        // the caller; here we just return the client side. The accepted half is
        // dropped, which is fine for liveness tests that re-accept per stream.
        client.unwrap()
    }

    #[test]
    fn pool_key_is_nul_delimited_and_distinct() {
        assert_eq!(pool_key("n", "u", "d"), "n\0u\0d");
        assert_ne!(pool_key("n", "ud", ""), pool_key("n", "u", "d"));
    }

    #[tokio::test]
    async fn checkin_then_checkout_reuses_same_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4);
        let key = pool_key("127.0.0.1:5432", "bench", "benchdb");

        // Park a live connection, then check it back out.
        let s = live_stream(&listener).await;
        let parked_addr = s.local_addr().unwrap();
        assert!(pool.checkin(&key, s));
        assert_eq!(pool.idle_count(), 1);

        let got = pool.checkout(&key).expect("a parked connection is reusable");
        assert_eq!(got.local_addr().unwrap(), parked_addr, "same socket reused");
        assert_eq!(pool.reuses(), 1);
        assert_eq!(pool.idle_count(), 0);

        // Empty pool → miss.
        assert!(pool.checkout(&key).is_none());
    }

    #[tokio::test]
    async fn distinct_identities_do_not_share() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4);
        let alice = pool_key("n", "alice", "db");
        let bob = pool_key("n", "bob", "db");

        pool.checkin(&alice, live_stream(&listener).await);
        // Bob must NOT see alice's connection.
        assert!(pool.checkout(&bob).is_none());
        assert!(pool.checkout(&alice).is_some());
    }

    #[tokio::test]
    async fn per_key_cap_sheds_excess() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(2);
        let key = pool_key("n", "u", "d");

        assert!(pool.checkin(&key, live_stream(&listener).await));
        assert!(pool.checkin(&key, live_stream(&listener).await));
        // Third exceeds the cap of 2 → refused (and dropped/closed).
        assert!(!pool.checkin(&key, live_stream(&listener).await));
        assert_eq!(pool.over_capacity(), 1);
        assert_eq!(pool.idle_count(), 2);
    }

    #[tokio::test]
    async fn checkout_evicts_a_closed_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4);
        let key = pool_key("n", "u", "d");

        // Park a connection, then close the server side so the parked socket is
        // dead.
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        pool.checkin(&key, client);
        drop(server); // peer closes
        // Give the close a moment to propagate.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Checkout must not hand out the dead connection.
        assert!(pool.checkout(&key).is_none());
        assert_eq!(pool.stale_evicted(), 1);
    }
}
