//! Shared pooled backend connections for the non-PG-wire gateways (H-06).
//!
//! The HTTP SQL, MCP and GraphQL gateways used to dial and authenticate a fresh
//! `BackendClient` for every request (`BackendClient::connect`), paying TCP +
//! TLS + auth per call and creating a reconnect burst exactly when a backend is
//! already stressed. This pool keeps a small number of authenticated idle
//! clients per backend identity (host/port/user/db) and hands them back out,
//! validating liveness on checkout and dropping dead ones.
//!
//! Contract (see `docs/internal/H-06-interface-contract.md`):
//! - A checked-out client is owned by exactly one request for its duration.
//! - `release` returns it to the idle set after a successful, session-neutral
//!   statement (autocommit SQL leaves no state behind).
//! - `discard` drops a client whose request failed, whose SQL opened an
//!   explicit transaction, or that changed session state — a gateway request
//!   must never inherit another request's session.
//! - The pool is intentionally dumb: no eviction task. Bounded by
//!   `[limits] gateway_pool_max_idle` per identity; extras are dropped. It
//!   does carry reuse/discard/miss counters (H-06 slice 1, below) — not yet
//!   exported through `/metrics`, since that needs the per-gateway pool
//!   instances threaded into `src/admin.rs`'s state, which is wired in
//!   `src/server.rs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::backend::{BackendClient, BackendConfig, BackendResult};

/// A tiny identity-keyed pool of authenticated backend clients.
///
/// Carries its own reuse/discard/miss counters (H-06 slice 1) so each
/// gateway's pool — the HTTP SQL, MCP and GraphQL gateways each own a
/// separate `BackendClientPool` instance — can report how effectively it is
/// avoiding a fresh dial per request. `idle_count` (below) doubles as the
/// idle gauge.
#[derive(Default)]
pub struct BackendClientPool {
    idle: Mutex<HashMap<String, Vec<BackendClient>>>,
    max_idle_per_key: usize,
    /// Checkouts that reused a pooled, live idle client.
    reuse_count: AtomicU64,
    /// Clients dropped rather than returned to (or taken from) the idle set:
    /// an explicit `discard()`, a dead idle candidate found during
    /// `acquire`, a `release()` above the per-identity idle ceiling, or a
    /// `release()` while pooling is disabled (`max_idle_per_key == 0`).
    discard_count: AtomicU64,
    /// Checkouts that found no reusable idle client and dialed a fresh one
    /// (including every checkout while pooling is disabled).
    miss_count: AtomicU64,
}

impl BackendClientPool {
    /// `max_idle_per_key` caps how many idle clients are retained per backend
    /// identity (0 disables pooling: every acquire dials).
    pub fn new(max_idle_per_key: usize) -> Self {
        Self {
            idle: Mutex::new(HashMap::new()),
            max_idle_per_key,
            reuse_count: AtomicU64::new(0),
            discard_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    fn key(cfg: &BackendConfig) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}",
            cfg.host,
            cfg.port,
            cfg.user,
            cfg.database.as_deref().unwrap_or(""),
            cfg.application_name.as_deref().unwrap_or(""),
            matches!(cfg.tls_mode, crate::backend::TlsMode::Disable) as u8,
        )
    }

    /// Check out a live client for `cfg`, reusing an idle one when possible.
    /// Dead idle clients are dropped and the next candidate (or a fresh dial)
    /// is tried.
    pub async fn acquire(&self, cfg: &BackendConfig) -> BackendResult<BackendClient> {
        if self.max_idle_per_key == 0 {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return BackendClient::connect(cfg).await;
        }
        let key = Self::key(cfg);
        loop {
            let candidate = {
                let mut idle = self.idle.lock();
                idle.get_mut(&key).and_then(|v| v.pop())
            };
            match candidate {
                Some(client) if client.is_probably_alive() => {
                    self.reuse_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(client);
                }
                Some(_dead) => {
                    self.discard_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                None => {
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return BackendClient::connect(cfg).await;
                }
            }
        }
    }

    /// Return a session-neutral, healthy client to the idle set (or drop it if
    /// the per-identity ceiling is reached, or pooling is disabled).
    pub fn release(&self, cfg: &BackendConfig, client: BackendClient) {
        if self.max_idle_per_key == 0 {
            self.discard_count.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let key = Self::key(cfg);
        let mut idle = self.idle.lock();
        let list = idle.entry(key).or_default();
        if list.len() < self.max_idle_per_key {
            list.push(client);
        } else {
            self.discard_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drop a client that must not be reused (failed request, explicit
    /// transaction, or session-state change).
    pub fn discard(&self, client: BackendClient) {
        self.discard_count.fetch_add(1, Ordering::Relaxed);
        drop(client);
    }

    /// Number of idle clients retained (all identities) — the idle gauge.
    pub fn idle_count(&self) -> usize {
        self.idle.lock().values().map(Vec::len).sum()
    }

    /// Checkouts that reused a pooled, live idle client.
    pub fn reuse_count(&self) -> u64 {
        self.reuse_count.load(Ordering::Relaxed)
    }

    /// Clients dropped rather than returned to (or taken from) the idle set.
    /// See the field doc comment on `BackendClientPool::discard_count` for
    /// exactly which paths increment this.
    pub fn discard_count(&self) -> u64 {
        self.discard_count.load(Ordering::Relaxed)
    }

    /// Checkouts that found no reusable idle client and dialed a fresh one.
    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }
}

/// Convenience alias used by the gateway constructors.
pub type SharedBackendPool = Arc<BackendClientPool>;

impl std::fmt::Debug for BackendClientPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendClientPool")
            .field("max_idle_per_key", &self.max_idle_per_key)
            .field("idle_count", &self.idle_count())
            .field("reuse_count", &self.reuse_count())
            .field("discard_count", &self.discard_count())
            .field("miss_count", &self.miss_count())
            .finish()
    }
}

/// May a gateway connection be returned to the pool after executing `sql`?
///
/// Only session-neutral statements qualify: no explicit transaction control
/// (`BEGIN`/`START`/`COMMIT`/`ROLLBACK`), no session state (`SET`/`RESET`/
/// `DISCARD`/`DECLARE`/`LISTEN`/`PREPARE`/`CREATE TEMP`) and no interior `;`
/// (a multi-statement string can hide any of the above behind a plain lead).
/// Everything else is executed on a connection that is then discarded.
pub fn statement_is_session_neutral(sql: &str) -> bool {
    use crate::protocol::starts_with_ci;
    let trimmed = sql.trim();
    let core = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    if core.contains(';') {
        return false;
    }
    const SESSION_STATEFUL: [&str; 10] = [
        "BEGIN", "START", "COMMIT", "ROLLBACK", "SET", "RESET", "DISCARD", "DECLARE", "LISTEN",
        "PREPARE",
    ];
    if SESSION_STATEFUL.iter().any(|kw| starts_with_ci(core, kw)) {
        return false;
    }
    // CREATE TEMP/TEMPORARY TABLE leaves a session-scoped object behind.
    if starts_with_ci(core, "CREATE") {
        let rest = core["CREATE".len()..].trim_start();
        if starts_with_ci(rest, "TEMP") || starts_with_ci(rest, "TEMPORARY") {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::TlsMode;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};

    fn test_cfg() -> BackendConfig {
        BackendConfig {
            host: "127.0.0.1".into(),
            port: 1,
            user: "test".into(),
            password: None,
            database: Some("d".into()),
            application_name: Some("gateway-pool-test".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_millis(50),
            query_timeout: Duration::from_millis(50),
            tls_config: crate::backend::tls::default_client_config(),
        }
    }

    async fn live_client() -> BackendClient {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client_side = TcpStream::connect(addr).await.unwrap();
        let (server_side, _) = listener.accept().await.unwrap();
        // Keep the peer socket open for the lifetime of the test process so the
        // pooled client stays "probably alive".
        std::mem::forget(server_side);
        BackendClient::from_tcp_for_test(client_side)
    }

    #[tokio::test]
    async fn released_client_is_reused() {
        let pool = BackendClientPool::new(4);
        let cfg = test_cfg();

        pool.release(&cfg, live_client().await);
        assert_eq!(pool.idle_count(), 1);
        assert_eq!(pool.reuse_count(), 0);

        let first = pool.acquire(&cfg).await.unwrap();
        assert_eq!(pool.idle_count(), 0, "reuse must consume the idle entry");
        assert_eq!(
            pool.reuse_count(),
            1,
            "the acquire above must count as a reuse"
        );
        assert_eq!(pool.miss_count(), 0);
        assert_eq!(pool.discard_count(), 0);
        drop(first);
    }

    #[tokio::test]
    async fn pool_capacity_bounds_retention() {
        let pool = BackendClientPool::new(1);
        let cfg = test_cfg();
        pool.release(&cfg, live_client().await);
        pool.release(&cfg, live_client().await);
        assert_eq!(pool.idle_count(), 1, "second idle client must be dropped");
        assert_eq!(
            pool.discard_count(),
            1,
            "the release above the ceiling must count as a discard"
        );
    }

    #[tokio::test]
    async fn zero_capacity_disables_pooling() {
        let pool = BackendClientPool::new(0);
        let cfg = test_cfg();
        pool.release(&cfg, live_client().await);
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(
            pool.discard_count(),
            1,
            "release with pooling disabled must count as a discard"
        );

        // acquire() on a disabled pool always dials fresh — a miss — whether
        // or not the dial itself succeeds.
        assert!(pool.acquire(&cfg).await.is_err());
        assert_eq!(pool.miss_count(), 1);
    }

    #[tokio::test]
    async fn acquire_on_empty_pool_counts_as_miss() {
        let pool = BackendClientPool::new(4);
        let cfg = test_cfg(); // port 1 — nothing listens there.
        assert!(
            pool.acquire(&cfg).await.is_err(),
            "no idle candidate and nothing listening on port 1"
        );
        assert_eq!(pool.miss_count(), 1);
        assert_eq!(pool.reuse_count(), 0);
        assert_eq!(pool.discard_count(), 0);
    }

    #[tokio::test]
    async fn acquire_drops_dead_idle_candidate_and_counts_it_as_discard() {
        let pool = BackendClientPool::new(4);
        let cfg = test_cfg();
        // A client whose peer has been dropped is no longer "probably alive"
        // — but the FIN is not guaranteed to be visible to `poll_peek`
        // instantly, even on loopback (see the identical wait loop in
        // `backend::stream::tests::test_is_probably_alive_detects_peer_close`),
        // so poll until it actually reads dead before handing it to the pool.
        let dead = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let client_side = TcpStream::connect(addr).await.unwrap();
            let (server_side, _) = listener.accept().await.unwrap();
            drop(server_side); // peer gone: the socket is now dead.
            let client = BackendClient::from_tcp_for_test(client_side);
            for _ in 0..50 {
                if !client.is_probably_alive() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(!client.is_probably_alive(), "peer close did not propagate");
            client
        };
        pool.release(&cfg, dead);
        assert_eq!(pool.idle_count(), 1);

        // acquire() must skip the dead candidate (counting it as a discard)
        // and fall through to a fresh dial (counting a miss), since nothing
        // listens on port 1.
        assert!(pool.acquire(&cfg).await.is_err());
        assert_eq!(pool.discard_count(), 1);
        assert_eq!(pool.miss_count(), 1);
        assert_eq!(pool.reuse_count(), 0);
    }

    #[tokio::test]
    async fn explicit_discard_counts() {
        let pool = BackendClientPool::new(4);
        pool.discard(live_client().await);
        assert_eq!(pool.discard_count(), 1);
        assert_eq!(pool.idle_count(), 0, "a discarded client is never pooled");
    }

    #[test]
    fn session_neutral_classifier() {
        // Plain autocommit SQL is reusable.
        assert!(statement_is_session_neutral("SELECT 1"));
        assert!(statement_is_session_neutral("  insert into t values (1); "));
        // Transaction control and session state are not.
        assert!(!statement_is_session_neutral("BEGIN"));
        assert!(!statement_is_session_neutral("START TRANSACTION"));
        assert!(!statement_is_session_neutral("COMMIT"));
        assert!(!statement_is_session_neutral("ROLLBACK;"));
        assert!(!statement_is_session_neutral("SET search_path = x"));
        assert!(!statement_is_session_neutral("DISCARD ALL"));
        assert!(!statement_is_session_neutral(
            "DECLARE c CURSOR FOR SELECT 1"
        ));
        assert!(!statement_is_session_neutral("CREATE TEMP TABLE t (x int)"));
        // Multi-statement strings can hide anything after the lead.
        assert!(!statement_is_session_neutral("SELECT 1; SET x = 1"));
    }
}
