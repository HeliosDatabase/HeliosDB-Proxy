//! Connection Pool - HeliosProxy
//!
//! Manages connection pooling with configurable limits, idle timeout,
//! and health-aware connection management.
//!
//! Every connection the pool hands out carries a live backend socket. A pool
//! needs a backend template ([`ConnectionPool::with_backend_template`]) and a
//! per-node endpoint ([`ConnectionPool::add_node_with_endpoint`]) to open one;
//! without either, [`ConnectionPool::get_connection`] fails with
//! [`ProxyError::Pool`] instead of returning a connection with no backend.

use crate::backend::{BackendClient, BackendConfig};
use crate::{NodeId, ProxyError, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use uuid::Uuid;

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum connections per node
    pub min_connections: usize,
    /// Maximum connections per node
    pub max_connections: usize,
    /// Connection idle timeout
    pub idle_timeout: Duration,
    /// Connection lifetime (max age before recycling)
    pub max_lifetime: Duration,
    /// Acquire timeout
    pub acquire_timeout: Duration,
    /// Validate connection before use
    pub test_on_acquire: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(1800),
            acquire_timeout: Duration::from_secs(30),
            test_on_acquire: true,
        }
    }
}

/// A pooled connection
pub struct PooledConnection {
    /// Connection ID
    pub id: Uuid,
    /// Node this connection belongs to
    pub node_id: NodeId,
    /// When the connection was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last used timestamp
    pub last_used: chrono::DateTime<chrono::Utc>,
    /// Connection state
    pub state: ConnectionState,
    /// Use count
    pub use_count: u64,
    /// Semaphore permit held for the lifetime of this connection.
    /// Dropped when the connection is dropped or closed, releasing one
    /// slot back to the per-node semaphore. `None` in unit-test helpers
    /// that construct `PooledConnection` without going through the pool.
    pub(crate) permit: Option<OwnedSemaphorePermit>,
    /// Live backend connection. Always `Some` for a connection handed out
    /// by [`ConnectionPool::get_connection`]; `None` only for values built
    /// directly by crate-internal unit tests. Every operation that needs the
    /// backend (reset query, validation, checkout liveness) treats `None` as
    /// a failure, never as success.
    pub(crate) client: Option<BackendClient>,
}

impl PooledConnection {
    /// Whether this connection carries a live backend client.
    pub fn has_live_client(&self) -> bool {
        self.client.is_some()
    }
}

// Custom Debug skips the `BackendClient` which doesn't implement Debug.
impl std::fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("id", &self.id)
            .field("node_id", &self.node_id)
            .field("created_at", &self.created_at)
            .field("last_used", &self.last_used)
            .field("state", &self.state)
            .field("use_count", &self.use_count)
            .field("has_permit", &self.permit.is_some())
            .field("has_live_client", &self.client.is_some())
            .finish()
    }
}

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Available for use
    Idle,
    /// Currently in use
    InUse,
    /// Being validated
    Validating,
    /// Closed/invalid
    Closed,
}

/// Subtract `n` from a gauge, saturating at zero instead of wrapping.
///
/// The compare-exchange loop `AtomicU64::fetch_update` runs, written out:
/// that method is deprecated from Rust 1.99, and its replacement
/// (`try_update`) does not exist on the 1.86 MSRV.
fn saturating_sub_seqcst(gauge: &AtomicU64, n: u64) {
    let mut current = gauge.load(Ordering::SeqCst);
    while let Err(actual) = gauge.compare_exchange_weak(
        current,
        current.saturating_sub(n),
        Ordering::SeqCst,
        Ordering::SeqCst,
    ) {
        current = actual;
    }
}

/// Per-node connection pool
struct NodePool {
    /// Available connections
    connections: Vec<PooledConnection>,
    /// Semaphore for limiting connections
    semaphore: Arc<Semaphore>,
    /// Total created connections
    total_created: u64,
    /// Total closed connections
    total_closed: u64,
    /// Endpoint host:port. `None` for a node registered with
    /// [`ConnectionPool::add_node`]: acquisitions on it fail, because the
    /// pool has nowhere to connect.
    endpoint: Option<(String, u16)>,
}

impl NodePool {
    fn new(max_connections: usize) -> Self {
        Self {
            connections: Vec::new(),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            total_created: 0,
            total_closed: 0,
            endpoint: None,
        }
    }
}

/// Connection Pool Manager
pub struct ConnectionPool {
    /// Configuration
    config: PoolConfig,
    /// Per-node pools
    pools: Arc<RwLock<HashMap<NodeId, NodePool>>>,
    /// Total connections across all nodes
    total_connections: AtomicU64,
    /// Active (in-use) connections
    active_connections: AtomicU64,
    /// Metrics counters (atomics; snapshotted into PoolMetrics on demand)
    metrics: PoolMetricsCounters,
    /// Backend-connection template. Host/port are overridden per-node
    /// from `NodePool.endpoint`. When `None`, every acquisition fails with
    /// [`ProxyError::Pool`]: the pool cannot open a backend connection.
    backend_template: Option<BackendConfig>,
}

/// Lock-free counters backing `PoolMetrics`. Every increment is a single
/// atomic `fetch_add` with `Relaxed` ordering — no RwLock or `.await`.
#[derive(Debug, Default)]
struct PoolMetricsCounters {
    acquires: AtomicU64,
    acquire_failures: AtomicU64,
    connections_created: AtomicU64,
    connections_closed: AtomicU64,
    connections_recycled: AtomicU64,
    validation_failures: AtomicU64,
    acquire_timeouts: AtomicU64,
}

impl PoolMetricsCounters {
    fn snapshot(&self) -> PoolMetrics {
        PoolMetrics {
            acquires: self.acquires.load(Ordering::Relaxed),
            acquire_failures: self.acquire_failures.load(Ordering::Relaxed),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            connections_recycled: self.connections_recycled.load(Ordering::Relaxed),
            validation_failures: self.validation_failures.load(Ordering::Relaxed),
            acquire_timeouts: self.acquire_timeouts.load(Ordering::Relaxed),
        }
    }
}

/// Pool metrics (plain-data snapshot of `PoolMetricsCounters`)
#[derive(Debug, Clone, Default)]
pub struct PoolMetrics {
    /// Total connection acquires
    pub acquires: u64,
    /// Acquire failures
    pub acquire_failures: u64,
    /// Connections created
    pub connections_created: u64,
    /// Connections closed
    pub connections_closed: u64,
    /// Connections recycled (exceeded lifetime)
    pub connections_recycled: u64,
    /// Validation failures
    pub validation_failures: u64,
    /// Timeout waiting for connection
    pub acquire_timeouts: u64,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            pools: Arc::new(RwLock::new(HashMap::new())),
            total_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            metrics: PoolMetricsCounters::default(),
            backend_template: None,
        }
    }

    /// Attach a backend-connection template. With a template *and* a
    /// per-node endpoint (set via `add_node_with_endpoint`), the pool
    /// opens real PG connections via `crate::backend::BackendClient`.
    /// Without a template, [`Self::get_connection`] fails with
    /// [`ProxyError::Pool`].
    pub fn with_backend_template(mut self, template: BackendConfig) -> Self {
        self.backend_template = Some(template);
        self
    }

    /// Register a node without an endpoint.
    ///
    /// The node gets its per-node slot accounting (stats, eviction,
    /// `max_connections` semaphore), but the pool has no address to connect
    /// to, so [`Self::get_connection`] for it fails with [`ProxyError::Pool`].
    /// Use [`Self::add_node_with_endpoint`] for a node that serves
    /// connections.
    pub async fn add_node(&self, node_id: NodeId) {
        let mut pools = self.pools.write().await;
        if let std::collections::hash_map::Entry::Vacant(e) = pools.entry(node_id) {
            e.insert(NodePool::new(self.config.max_connections));
            tracing::debug!("Added node {:?} to connection pool", node_id);
        }
    }

    /// Add a node with endpoint info. When combined with
    /// `with_backend_template`, `create_connection` opens a live
    /// `BackendClient` against the given `host:port`.
    pub async fn add_node_with_endpoint(
        &self,
        node_id: NodeId,
        host: impl Into<String>,
        port: u16,
    ) {
        let mut pools = self.pools.write().await;
        if let std::collections::hash_map::Entry::Vacant(e) = pools.entry(node_id) {
            let mut np = NodePool::new(self.config.max_connections);
            np.endpoint = Some((host.into(), port));
            e.insert(np);
            tracing::debug!(
                "Added node {:?} to connection pool (with endpoint)",
                node_id
            );
        }
    }

    /// Decrement the `total_connections` gauge by `n`, saturating at zero
    /// rather than wrapping.
    ///
    /// `close_all` stores 0 outright, so a connection returned or a batch
    /// of connections evicted afterwards must not underflow the gauge to
    /// near-`u64::MAX`.
    fn dec_total_connections_by(&self, n: u64) {
        saturating_sub_seqcst(&self.total_connections, n);
    }

    /// Decrement the `total_connections` gauge by one without wrapping
    /// past zero. See `dec_total_connections_by`.
    fn dec_total_connections(&self) {
        self.dec_total_connections_by(1);
    }

    /// Decrement the `active_connections` gauge without wrapping past
    /// zero. Mirrors `dec_total_connections` — `close_all` stores 0
    /// outright, so a connection returned or closed afterwards must not
    /// underflow this gauge either.
    fn dec_active_connections(&self) {
        saturating_sub_seqcst(&self.active_connections, 1);
    }

    /// Liveness gate applied to an idle connection at checkout time,
    /// entirely controlled by `PoolConfig::test_on_acquire` — with it
    /// `false` an operator has asked to skip acquire-time validation, so
    /// no check (peek included) is performed and every idle connection is
    /// accepted as-is.
    ///
    /// With it `true`, [`Self::validate_connection`] runs the free,
    /// non-blocking socket peek: a connection the backend closed while it
    /// sat idle, or one without a backend client, is never handed out.
    async fn checkout_liveness_ok(&self, conn: &PooledConnection) -> bool {
        if !self.config.test_on_acquire {
            return true;
        }
        // `validate_connection` counts its own failures.
        matches!(self.validate_connection(conn).await, Ok(true))
    }

    /// Why `node_id` cannot serve connections, if it cannot: no backend
    /// template on the pool, or no endpoint for the node.
    fn unconfigured_reason(&self, node_id: &NodeId, pool: &NodePool) -> Option<String> {
        if self.backend_template.is_none() {
            return Some(format!(
                "connection pool has no backend template, so it cannot open a backend \
                 connection for node {:?}; attach one with \
                 ConnectionPool::with_backend_template",
                node_id
            ));
        }
        if pool.endpoint.is_none() {
            return Some(format!(
                "node {:?} was registered without an endpoint, so the pool cannot open a \
                 backend connection for it; register it with \
                 ConnectionPool::add_node_with_endpoint",
                node_id
            ));
        }
        None
    }

    /// Remove a node from the pool
    pub async fn remove_node(&self, node_id: &NodeId) {
        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.remove(node_id) {
            let count = pool.connections.len() as u64;
            self.dec_total_connections_by(count);
            tracing::debug!("Removed node {:?} from connection pool", node_id);
        }
    }

    /// Get a connection from the pool.
    ///
    /// Fails with [`ProxyError::Pool`] when the pool has no backend template
    /// or the node no endpoint (see [`Self::add_node`]): the pool never hands
    /// out a connection without a live backend client.
    pub async fn get_connection(&self, node_id: &NodeId) -> Result<PooledConnection> {
        self.metrics.acquires.fetch_add(1, Ordering::Relaxed);

        // Step 1: Briefly take the write lock to (a) pop an idle connection if
        // available, and (b) clone the per-node Arc<Semaphore>. The lock is
        // dropped before we await anything.
        let (mut maybe_idle, semaphore) = {
            let mut pools = self.pools.write().await;
            let pool = pools.get_mut(node_id).ok_or_else(|| {
                ProxyError::Connection(format!("Node {:?} not found in pool", node_id))
            })?;
            if let Some(reason) = self.unconfigured_reason(node_id, pool) {
                self.metrics
                    .acquire_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::Pool(reason));
            }

            let semaphore = pool.semaphore.clone();
            let idle = pool
                .connections
                .iter()
                .position(|c| c.state == ConnectionState::Idle)
                .map(|idx| pool.connections.swap_remove(idx));
            (idle, semaphore)
        };

        // Step 2: If we got an idle connection, check its age. If still fresh,
        // return it directly — its permit is already attached, no new permit
        // needs to be acquired.
        if let Some(mut conn) = maybe_idle.take() {
            let age = chrono::Utc::now()
                .signed_duration_since(conn.created_at)
                .to_std()
                .unwrap_or(Duration::ZERO);

            if age <= self.config.max_lifetime && self.checkout_liveness_ok(&conn).await {
                conn.state = ConnectionState::InUse;
                conn.last_used = chrono::Utc::now();
                conn.use_count += 1;
                self.active_connections.fetch_add(1, Ordering::SeqCst);
                return Ok(conn);
            }

            // Too old, or the backend closed / desynced the socket while it
            // sat idle — drop it (which releases its permit) and fall through
            // to the create-new path.
            self.metrics
                .connections_recycled
                .fetch_add(1, Ordering::Relaxed);
            self.dec_total_connections();
            drop(conn);
        }

        // Step 3: No reusable idle connection — acquire a permit (bounded by
        // max_connections) and create a new connection that owns the permit.
        let permit = match tokio::time::timeout(
            self.config.acquire_timeout,
            semaphore.acquire_owned(),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => {
                self.metrics
                    .acquire_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::PoolExhausted(format!(
                    "Failed to acquire semaphore for node {:?}",
                    node_id
                )));
            }
            Err(_) => {
                self.metrics
                    .acquire_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::Timeout(format!(
                    "Timeout acquiring connection for node {:?}",
                    node_id
                )));
            }
        };

        let conn = self.create_connection(*node_id, Some(permit)).await?;
        self.active_connections.fetch_add(1, Ordering::SeqCst);
        self.total_connections.fetch_add(1, Ordering::SeqCst);

        {
            let mut pools = self.pools.write().await;
            if let Some(pool) = pools.get_mut(node_id) {
                pool.total_created += 1;
            }
        }

        Ok(conn)
    }

    /// Return a connection to the pool
    pub async fn return_connection(&self, mut conn: PooledConnection) {
        self.dec_active_connections();

        let mut pools = self.pools.write().await;
        // `dropped` carries the connection out of the lock's scope so the fd
        // close and the semaphore-permit release happen after the write
        // guard is released, not while it's held.
        let dropped = if let Some(pool) = pools.get_mut(&conn.node_id) {
            conn.state = ConnectionState::Idle;
            conn.last_used = chrono::Utc::now();
            pool.connections.push(conn);
            None
        } else {
            // The node was removed (`remove_node`) while this connection was
            // checked out, so it can only be dropped here. `remove_node` can
            // only account for the connections that were sitting in the pool
            // at that moment, so without this decrement the in-use ones leak
            // into `total_connections` permanently.
            self.dec_total_connections();
            self.metrics
                .connections_closed
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                "Dropped connection {:?}: node {:?} no longer in pool",
                conn.id,
                conn.node_id
            );
            Some(conn)
        };
        drop(pools);
        drop(dropped);
    }

    /// Close a connection (don't return to pool)
    pub async fn close_connection(&self, conn: PooledConnection) {
        self.dec_active_connections();
        self.dec_total_connections();
        self.metrics
            .connections_closed
            .fetch_add(1, Ordering::Relaxed);

        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.get_mut(&conn.node_id) {
            pool.total_closed += 1;
        }

        tracing::debug!("Closed connection {:?}", conn.id);
    }

    /// Create a new connection, attaching the given semaphore permit.
    ///
    /// Opens a live `BackendClient` via TCP (+ optional TLS) from the
    /// pool's `backend_template` and the node's endpoint, and bundles it
    /// into the returned `PooledConnection`. Fails with
    /// [`ProxyError::Pool`] when either is missing.
    async fn create_connection(
        &self,
        node_id: NodeId,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<PooledConnection> {
        let cfg = {
            let pools = self.pools.read().await;
            let pool = pools.get(&node_id).ok_or_else(|| {
                ProxyError::Connection(format!("Node {:?} not found in pool", node_id))
            })?;
            match (&self.backend_template, &pool.endpoint) {
                (Some(template), Some((host, port))) => {
                    let mut cfg = template.clone();
                    cfg.host = host.clone();
                    cfg.port = *port;
                    cfg
                }
                _ => {
                    let reason = self
                        .unconfigured_reason(&node_id, pool)
                        .unwrap_or_else(|| format!("node {:?} is not configured", node_id));
                    return Err(ProxyError::Pool(reason));
                }
            }
        };
        let client = match BackendClient::connect(&cfg).await {
            Ok(c) => Some(c),
            Err(e) => {
                return Err(ProxyError::Connection(format!(
                    "backend connect for node {:?} failed: {}",
                    node_id, e
                )));
            }
        };

        let now = chrono::Utc::now();
        let conn = PooledConnection {
            id: Uuid::new_v4(),
            node_id,
            created_at: now,
            last_used: now,
            state: ConnectionState::InUse,
            use_count: 1,
            permit,
            client,
        };

        self.metrics
            .connections_created
            .fetch_add(1, Ordering::Relaxed);

        tracing::debug!("Created connection {:?} for node {:?}", conn.id, node_id);

        Ok(conn)
    }

    /// Validate a connection with a socket-level liveness check.
    ///
    /// Returns `Ok(false)` (and counts a validation failure) for a closed
    /// connection, for one without a live backend client, and for one
    /// whose socket the non-blocking peek finds closed, errored or holding
    /// unexpected unread bytes. This is a peek, not a query round-trip: a
    /// backend that accepts bytes but no longer answers queries is found
    /// by the next query, not here.
    pub async fn validate_connection(&self, conn: &PooledConnection) -> Result<bool> {
        let alive = conn.state != ConnectionState::Closed
            && conn
                .client
                .as_ref()
                .is_some_and(|client| client.is_probably_alive());
        if !alive {
            self.metrics
                .validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(alive)
    }

    /// Run a reset query on a connection before returning it to the pool.
    /// Used by pool-modes `release` for Transaction and Statement modes.
    ///
    /// Fails with [`ProxyError::Pool`] when the connection has no live
    /// backend client (the reset cannot run), and with
    /// [`ProxyError::Connection`] when the backend rejects or does not
    /// answer the query.
    pub async fn run_reset_query(&self, conn: &mut PooledConnection, query: &str) -> Result<()> {
        let client = conn.client.as_mut().ok_or_else(|| {
            ProxyError::Pool(format!(
                "reset query not run on connection {:?}: it has no live backend client",
                conn.id
            ))
        })?;
        client
            .execute(query)
            .await
            .map_err(|e| ProxyError::Connection(format!("reset query failed: {}", e)))?;
        Ok(())
    }

    /// Close all connections
    pub async fn close_all(&self) -> Result<()> {
        let mut pools = self.pools.write().await;
        for pool in pools.values_mut() {
            pool.connections.clear();
        }
        self.total_connections.store(0, Ordering::SeqCst);
        self.active_connections.store(0, Ordering::SeqCst);
        tracing::info!("Closed all connections");
        Ok(())
    }

    /// Evict idle connections that have exceeded idle timeout
    pub async fn evict_idle(&self) {
        let mut pools = self.pools.write().await;
        let mut evicted = 0;

        for pool in pools.values_mut() {
            let before = pool.connections.len();
            pool.connections.retain(|conn| {
                let idle_time = chrono::Utc::now()
                    .signed_duration_since(conn.last_used)
                    .to_std()
                    .unwrap_or(Duration::ZERO);

                idle_time < self.config.idle_timeout
            });
            evicted += before - pool.connections.len();
        }

        if evicted > 0 {
            self.dec_total_connections_by(evicted as u64);
            tracing::debug!("Evicted {} idle connections", evicted);
        }
    }

    /// Get total connections
    pub async fn total_connections(&self) -> usize {
        self.total_connections.load(Ordering::SeqCst) as usize
    }

    /// Get active connections
    pub async fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst) as usize
    }

    /// Get pool metrics
    pub async fn metrics(&self) -> PoolMetrics {
        self.metrics.snapshot()
    }

    /// Get per-node statistics
    pub async fn node_stats(&self, node_id: &NodeId) -> Option<NodePoolStats> {
        let pools = self.pools.read().await;
        pools.get(node_id).map(|pool| NodePoolStats {
            idle_connections: pool
                .connections
                .iter()
                .filter(|c| c.state == ConnectionState::Idle)
                .count(),
            total_created: pool.total_created,
            total_closed: pool.total_closed,
        })
    }
}

/// Per-node pool statistics
#[derive(Debug, Clone)]
pub struct NodePoolStats {
    /// Number of idle connections
    pub idle_connections: usize,
    /// Total connections created for this node
    pub total_created: u64,
    /// Total connections closed for this node
    pub total_closed: u64,
}

/// Minimal in-process PostgreSQL wire backend for unit tests: answers the
/// startup handshake (trust auth) and simple queries, records every query
/// text, and can drop every open connection on demand. Shared with the
/// pool-modes manager tests.
#[cfg(test)]
pub(crate) mod test_backend {
    use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;

    pub(crate) struct FakePg {
        pub(crate) addr: SocketAddr,
        queries: Arc<Mutex<Vec<String>>>,
        accepted: Arc<AtomicUsize>,
        fail_queries: Arc<AtomicBool>,
        kill: Arc<watch::Sender<u64>>,
    }

    impl FakePg {
        pub(crate) async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local_addr");
            let queries = Arc::new(Mutex::new(Vec::new()));
            let accepted = Arc::new(AtomicUsize::new(0));
            let fail_queries = Arc::new(AtomicBool::new(false));
            let (kill, _) = watch::channel(0u64);
            let kill = Arc::new(kill);
            {
                let queries = queries.clone();
                let accepted = accepted.clone();
                let fail_queries = fail_queries.clone();
                let kill = kill.clone();
                tokio::spawn(async move {
                    while let Ok((sock, _)) = listener.accept().await {
                        accepted.fetch_add(1, Ordering::SeqCst);
                        let queries = queries.clone();
                        let fail_queries = fail_queries.clone();
                        let kill_rx = kill.subscribe();
                        tokio::spawn(async move {
                            let _ = serve_conn(sock, queries, fail_queries, kill_rx).await;
                        });
                    }
                });
            }
            Self {
                addr,
                queries,
                accepted,
                fail_queries,
                kill,
            }
        }

        /// A backend template whose host/port the pool overrides per node.
        pub(crate) fn template(&self) -> BackendConfig {
            BackendConfig {
                host: self.addr.ip().to_string(),
                port: self.addr.port(),
                user: "postgres".into(),
                password: None,
                database: None,
                application_name: Some("helios-pool-test".into()),
                tls_mode: TlsMode::Disable,
                connect_timeout: Duration::from_secs(2),
                query_timeout: Duration::from_secs(2),
                tls_config: default_client_config(),
            }
        }

        /// Every query text received so far, in order.
        pub(crate) fn queries(&self) -> Vec<String> {
            self.queries.lock().expect("queries lock").clone()
        }

        /// Number of backend connections accepted so far.
        pub(crate) fn accepted(&self) -> usize {
            self.accepted.load(Ordering::SeqCst)
        }

        /// Answer every following query with an ErrorResponse.
        pub(crate) fn fail_queries(&self, fail: bool) {
            self.fail_queries.store(fail, Ordering::SeqCst);
        }

        /// Close every currently open backend connection (new ones are
        /// still accepted).
        pub(crate) fn drop_connections(&self) {
            self.kill.send_modify(|generation| *generation += 1);
        }
    }

    async fn serve_conn(
        mut sock: TcpStream,
        queries: Arc<Mutex<Vec<String>>>,
        fail_queries: Arc<AtomicBool>,
        mut kill_rx: watch::Receiver<u64>,
    ) -> std::io::Result<()> {
        // StartupMessage: [len: u32 incl. itself][protocol + parameters].
        let len = sock.read_u32().await? as usize;
        let mut startup = vec![0u8; len.saturating_sub(4)];
        sock.read_exact(&mut startup).await?;
        // AuthenticationOk, then ReadyForQuery (idle).
        sock.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;
        sock.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;

        loop {
            let tag = tokio::select! {
                tag = sock.read_u8() => tag?,
                _ = kill_rx.changed() => return Ok(()),
            };
            let len = sock.read_u32().await? as usize;
            let mut body = vec![0u8; len.saturating_sub(4)];
            sock.read_exact(&mut body).await?;
            match tag {
                b'Q' => {
                    let text = String::from_utf8_lossy(body.strip_suffix(&[0]).unwrap_or(&body))
                        .into_owned();
                    queries.lock().expect("queries lock").push(text);
                    if fail_queries.load(Ordering::SeqCst) {
                        let mut fields = Vec::new();
                        fields.extend_from_slice(b"SERROR\0CXX000\0Mfake backend failure\0\0");
                        let mut msg = vec![b'E'];
                        msg.extend_from_slice(&((fields.len() + 4) as u32).to_be_bytes());
                        msg.extend_from_slice(&fields);
                        sock.write_all(&msg).await?;
                    } else {
                        let tag = b"OK\0";
                        let mut msg = vec![b'C'];
                        msg.extend_from_slice(&((tag.len() + 4) as u32).to_be_bytes());
                        msg.extend_from_slice(tag);
                        sock.write_all(&msg).await?;
                    }
                    sock.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;
                }
                b'X' => return Ok(()),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_backend::FakePg;
    use super::*;

    /// A pool with a backend template and one node whose endpoint is a
    /// live fake backend.
    async fn live_pool(config: PoolConfig) -> (ConnectionPool, NodeId, FakePg) {
        let backend = FakePg::start().await;
        let pool = ConnectionPool::new(config).with_backend_template(backend.template());
        let node_id = NodeId::new();
        pool.add_node_with_endpoint(node_id, backend.addr.ip().to_string(), backend.addr.port())
            .await;
        (pool, node_id, backend)
    }

    /// Wait until the pool's single parked connection for `node_id` sees its
    /// socket closed by the backend.
    async fn wait_until_parked_dead(pool: &ConnectionPool, node_id: &NodeId) {
        for _ in 0..200 {
            {
                let pools = pool.pools.read().await;
                let parked = &pools.get(node_id).expect("node pool").connections[0];
                if !parked
                    .client
                    .as_ref()
                    .expect("live client")
                    .is_probably_alive()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("socket never observed dead (FIN never arrived) — test is inconclusive");
    }

    /// A connection value with no backend client, as only crate-internal
    /// tests can build one.
    fn detached_connection(node_id: NodeId) -> PooledConnection {
        let now = chrono::Utc::now();
        PooledConnection {
            id: Uuid::new_v4(),
            node_id,
            created_at: now,
            last_used: now,
            state: ConnectionState::InUse,
            use_count: 1,
            permit: None,
            client: None,
        }
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.min_connections, 2);
        assert_eq!(config.max_connections, 10);
        assert!(config.test_on_acquire);
    }

    #[tokio::test]
    async fn test_add_remove_node() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let node_id = NodeId::new();

        pool.add_node(node_id).await;
        assert!(pool.node_stats(&node_id).await.is_some());

        pool.remove_node(&node_id).await;
        assert!(pool.node_stats(&node_id).await.is_none());
    }

    #[tokio::test]
    async fn test_get_return_connection() {
        let (pool, node_id, backend) = live_pool(PoolConfig::default()).await;

        // Get connection
        let conn = pool.get_connection(&node_id).await.expect("get failed");
        assert_eq!(conn.node_id, node_id);
        assert_eq!(conn.state, ConnectionState::InUse);
        assert!(conn.has_live_client());
        assert_eq!(pool.active_connections().await, 1);
        assert_eq!(backend.accepted(), 1);

        // Return connection
        pool.return_connection(conn).await;
        assert_eq!(pool.active_connections().await, 0);
    }

    #[tokio::test]
    async fn test_metrics() {
        let (pool, node_id, _backend) = live_pool(PoolConfig::default()).await;

        let conn = pool.get_connection(&node_id).await.expect("get failed");
        pool.return_connection(conn).await;

        let metrics = pool.metrics().await;
        assert_eq!(metrics.acquires, 1);
        assert_eq!(metrics.connections_created, 1);
    }

    /// Regression: `max_connections` must be enforced while connections are
    /// in use. Before the permit fix, the semaphore permit was released
    /// immediately at the end of `get_connection`, so the pool could hand out
    /// an unlimited number of simultaneously-active connections.
    #[tokio::test]
    async fn test_max_connections_enforced_while_in_use() {
        let (pool, node_id, _backend) = live_pool(PoolConfig {
            min_connections: 0,
            max_connections: 2,
            acquire_timeout: Duration::from_millis(50),
            ..Default::default()
        })
        .await;

        let c1 = pool.get_connection(&node_id).await.expect("first acquire");
        let c2 = pool.get_connection(&node_id).await.expect("second acquire");

        // Third acquire must fail with a Timeout — the two permits are held
        // by c1 and c2 and have not been released.
        let err = pool
            .get_connection(&node_id)
            .await
            .expect_err("third acquire should time out while c1/c2 held");
        assert!(
            matches!(err, ProxyError::Timeout(_)),
            "expected Timeout, got {err:?}"
        );

        // Dropping c1 releases its permit; the next acquire must succeed.
        drop(c1);
        let _c3 = pool
            .get_connection(&node_id)
            .await
            .expect("acquire should succeed after c1 dropped");

        // Keep c2 alive through the end of the test.
        drop(c2);
    }

    /// `with_backend_template` + `add_node_with_endpoint` must actually
    /// attempt a real connect at `create_connection` time — proven by
    /// pointing at a deliberately-unreachable address and verifying the
    /// acquire surfaces a Connection error containing the node id.
    #[tokio::test]
    async fn test_backend_template_with_unreachable_endpoint_errors() {
        use crate::backend::{tls::default_client_config, TlsMode};

        let template = BackendConfig {
            host: "unused".into(),
            port: 0,
            user: "postgres".into(),
            password: None,
            database: None,
            application_name: Some("helios-pool".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_millis(200),
            query_timeout: Duration::from_millis(200),
            tls_config: default_client_config(),
        };

        let pool = ConnectionPool::new(PoolConfig {
            max_connections: 2,
            acquire_timeout: Duration::from_millis(300),
            ..Default::default()
        })
        .with_backend_template(template);

        let node_id = NodeId::new();
        // 127.0.0.1:1 — no daemon, so TCP connect refuses.
        pool.add_node_with_endpoint(node_id, "127.0.0.1", 1).await;

        let err = pool
            .get_connection(&node_id)
            .await
            .expect_err("acquire must fail when backend is unreachable");
        match err {
            ProxyError::Connection(msg) => {
                assert!(
                    msg.contains("backend connect"),
                    "expected backend-connect error, got {}",
                    msg
                );
            }
            other => panic!("expected Connection error, got {:?}", other),
        }
    }

    /// Fail closed: without a backend template the pool cannot open a
    /// backend connection, so an acquire is an explicit `Pool` error — it
    /// never hands out a connection without a live client.
    #[tokio::test]
    async fn test_acquire_without_template_fails_closed() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let node_id = NodeId::new();
        pool.add_node_with_endpoint(node_id, "127.0.0.1", 5432)
            .await;

        let err = pool
            .get_connection(&node_id)
            .await
            .expect_err("acquire without a backend template must fail");
        match err {
            ProxyError::Pool(msg) => assert!(
                msg.contains("no backend template"),
                "unexpected message: {msg}"
            ),
            other => panic!("expected Pool error, got {other:?}"),
        }
        let metrics = pool.metrics().await;
        assert_eq!(metrics.acquire_failures, 1);
        assert_eq!(metrics.connections_created, 0);
        assert_eq!(pool.total_connections().await, 0);
        assert_eq!(pool.active_connections().await, 0);
    }

    /// Fail closed: a node registered with `add_node` has no endpoint, so
    /// even with a template the acquire is an explicit `Pool` error.
    #[tokio::test]
    async fn test_acquire_without_endpoint_fails_closed() {
        let backend = FakePg::start().await;
        let pool =
            ConnectionPool::new(PoolConfig::default()).with_backend_template(backend.template());
        let node_id = NodeId::new();
        pool.add_node(node_id).await;

        let err = pool
            .get_connection(&node_id)
            .await
            .expect_err("acquire for a node without an endpoint must fail");
        match err {
            ProxyError::Pool(msg) => assert!(
                msg.contains("without an endpoint"),
                "unexpected message: {msg}"
            ),
            other => panic!("expected Pool error, got {other:?}"),
        }
        assert_eq!(backend.accepted(), 0, "no backend connection may be opened");
        assert_eq!(pool.metrics().await.acquire_failures, 1);
    }

    /// `validate_connection` reports a connection without a backend client
    /// as not valid (it used to report it as "looks alive").
    #[tokio::test]
    async fn test_validate_connection_without_client_is_invalid() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let conn = detached_connection(NodeId::new());

        assert!(!pool.validate_connection(&conn).await.expect("validate"));
        assert_eq!(pool.metrics().await.validation_failures, 1);
    }

    /// `validate_connection` accepts a live socket and rejects one the
    /// backend closed.
    #[tokio::test]
    async fn test_validate_connection_checks_the_socket() {
        let (pool, node_id, backend) = live_pool(PoolConfig::default()).await;
        let conn = pool.get_connection(&node_id).await.expect("acquire");
        assert!(pool.validate_connection(&conn).await.expect("validate"));

        backend.drop_connections();
        let mut invalid = false;
        for _ in 0..200 {
            if !pool.validate_connection(&conn).await.expect("validate") {
                invalid = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(invalid, "closed backend socket was still reported valid");
    }

    /// `run_reset_query` fails when there is no backend to run it on,
    /// instead of reporting a reset that never ran.
    #[tokio::test]
    async fn test_run_reset_query_without_client_errors() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let mut conn = detached_connection(NodeId::new());

        let err = pool
            .run_reset_query(&mut conn, "DISCARD ALL")
            .await
            .expect_err("reset without a backend client must fail");
        match err {
            ProxyError::Pool(msg) => assert!(
                msg.contains("no live backend client"),
                "unexpected message: {msg}"
            ),
            other => panic!("expected Pool error, got {other:?}"),
        }
    }

    /// `run_reset_query` sends the query to the backend and surfaces a
    /// backend error.
    #[tokio::test]
    async fn test_run_reset_query_runs_on_backend() {
        let (pool, node_id, backend) = live_pool(PoolConfig::default()).await;
        let mut conn = pool.get_connection(&node_id).await.expect("acquire");

        pool.run_reset_query(&mut conn, "DISCARD ALL")
            .await
            .expect("reset");
        assert_eq!(backend.queries(), vec!["DISCARD ALL".to_string()]);

        backend.fail_queries(true);
        let err = pool
            .run_reset_query(&mut conn, "DISCARD ALL")
            .await
            .expect_err("backend error must surface");
        assert!(matches!(err, ProxyError::Connection(_)), "got {err:?}");
    }

    /// An idle connection without a backend client is never handed out at
    /// checkout: it is recycled and replaced with a live one.
    #[tokio::test]
    async fn test_idle_connection_without_client_is_not_handed_out() {
        let (pool, node_id, _backend) = live_pool(PoolConfig {
            test_on_acquire: true,
            ..Default::default()
        })
        .await;
        let detached = detached_connection(node_id);
        let detached_id = detached.id;
        pool.active_connections.fetch_add(1, Ordering::SeqCst);
        pool.total_connections.fetch_add(1, Ordering::SeqCst);
        pool.return_connection(detached).await;

        let conn = pool.get_connection(&node_id).await.expect("acquire");
        assert_ne!(conn.id, detached_id);
        assert!(conn.has_live_client());
        let metrics = pool.metrics().await;
        assert_eq!(metrics.validation_failures, 1);
        assert_eq!(metrics.connections_recycled, 1);
    }

    /// Regression (gauge leak): a connection that is checked out when its
    /// node is removed can only be dropped on return. `remove_node` accounts
    /// for the connections sitting in the pool at that moment, not the
    /// in-use ones, so `return_connection` must decrement the gauge itself —
    /// otherwise `total_connections` climbs forever.
    #[tokio::test]
    async fn test_return_after_remove_node_decrements_total_connections() {
        let (pool, node_id, _backend) = live_pool(PoolConfig::default()).await;

        let conn = pool.get_connection(&node_id).await.expect("acquire");
        assert_eq!(pool.total_connections().await, 1);

        // Node goes away while the connection is checked out; the in-use
        // connection is not in `pool.connections`, so nothing is decremented.
        pool.remove_node(&node_id).await;
        assert_eq!(pool.total_connections().await, 1);

        pool.return_connection(conn).await;
        assert_eq!(
            pool.total_connections().await,
            0,
            "returning to a removed node must not leak the gauge"
        );
        assert_eq!(pool.active_connections().await, 0);
        assert_eq!(pool.metrics().await.connections_closed, 1);
    }

    /// Returning to a removed node must never underflow either gauge, even
    /// when `close_all` has already zeroed both.
    #[tokio::test]
    async fn test_return_after_remove_node_does_not_underflow_total() {
        let (pool, node_id, _backend) = live_pool(PoolConfig::default()).await;

        let conn = pool.get_connection(&node_id).await.expect("acquire");
        pool.remove_node(&node_id).await;
        pool.close_all().await.expect("close_all");
        assert_eq!(pool.total_connections().await, 0);
        assert_eq!(pool.active_connections().await, 0);

        pool.return_connection(conn).await;
        assert_eq!(
            pool.total_connections().await,
            0,
            "total_connections must saturate at zero, not wrap to u64::MAX"
        );
        assert_eq!(
            pool.active_connections().await,
            0,
            "active_connections must saturate at zero, not wrap to u64::MAX"
        );
    }

    /// Regression (stale socket handed out): an idle connection whose
    /// backend closed the socket must be recycled at checkout instead of
    /// being handed to the next caller. `test_on_acquire` is set, as it is
    /// by default.
    #[tokio::test]
    async fn test_closed_socket_is_not_handed_out_on_checkout() {
        let (pool, node_id, backend) = live_pool(PoolConfig {
            min_connections: 0,
            max_connections: 4,
            test_on_acquire: true,
            acquire_timeout: Duration::from_millis(500),
            ..Default::default()
        })
        .await;

        // Park a live connection.
        let conn = pool.get_connection(&node_id).await.expect("acquire");
        let dead_id = conn.id;
        pool.return_connection(conn).await;
        assert_eq!(pool.total_connections().await, 1);

        // Backend closes it while the connection sits idle.
        backend.drop_connections();
        wait_until_parked_dead(&pool, &node_id).await;

        let fresh = pool.get_connection(&node_id).await.expect("re-acquire");
        assert_ne!(
            fresh.id, dead_id,
            "pool handed out a connection whose socket the backend had closed"
        );

        let metrics = pool.metrics().await;
        assert_eq!(
            metrics.validation_failures, 1,
            "the dead socket must be counted as a validation failure"
        );
        assert_eq!(
            metrics.connections_recycled, 1,
            "the dead connection must be recycled, not reused"
        );
        assert_eq!(
            metrics.connections_created, 2,
            "a replacement connection must be created"
        );
        // One recycled (-1) plus one created (+1): the gauge stays at 1.
        assert_eq!(pool.total_connections().await, 1);
    }

    /// Regression (spec deviation): `test_on_acquire = false` must skip
    /// ALL acquire-time validation, including the live-socket peek. This
    /// is the operator's one on/off switch for the checkout liveness gate.
    #[tokio::test]
    async fn test_on_acquire_false_skips_dead_socket_peek() {
        let (pool, node_id, backend) = live_pool(PoolConfig {
            min_connections: 0,
            max_connections: 4,
            test_on_acquire: false,
            acquire_timeout: Duration::from_millis(500),
            ..Default::default()
        })
        .await;

        // Park a live connection.
        let conn = pool.get_connection(&node_id).await.expect("acquire");
        let parked_id = conn.id;
        pool.return_connection(conn).await;

        // Backend closes it while the connection sits idle.
        backend.drop_connections();
        wait_until_parked_dead(&pool, &node_id).await;

        // With test_on_acquire = false, the dead connection must still be
        // handed back out — no peek, no validate_connection.
        let reused = pool.get_connection(&node_id).await.expect("re-acquire");
        assert_eq!(
            reused.id, parked_id,
            "test_on_acquire = false must skip the liveness peek entirely"
        );

        let metrics = pool.metrics().await;
        assert_eq!(
            metrics.validation_failures, 0,
            "no validation must run at all when test_on_acquire is false"
        );
        assert_eq!(
            metrics.connections_recycled, 0,
            "the dead connection must not be recycled when validation is disabled"
        );
    }

    /// A live socket must still be reused — the liveness probe must not
    /// throw away healthy idle connections.
    #[tokio::test]
    async fn test_live_socket_connection_is_reused_on_checkout() {
        let (pool, node_id, backend) = live_pool(PoolConfig {
            min_connections: 0,
            max_connections: 4,
            test_on_acquire: true,
            ..Default::default()
        })
        .await;

        let conn = pool.get_connection(&node_id).await.expect("acquire");
        let live_id = conn.id;
        pool.return_connection(conn).await;

        let reused = pool.get_connection(&node_id).await.expect("re-acquire");
        assert_eq!(reused.id, live_id, "healthy idle connection must be reused");
        let metrics = pool.metrics().await;
        assert_eq!(metrics.connections_created, 1);
        assert_eq!(metrics.validation_failures, 0);
        assert_eq!(backend.accepted(), 1);
    }

    /// Returning a connection to the pool keeps the permit attached, so
    /// reusing it should not consume a new permit.
    #[tokio::test]
    async fn test_return_then_reacquire_reuses_permit() {
        let (pool, node_id, _backend) = live_pool(PoolConfig {
            min_connections: 0,
            max_connections: 1,
            acquire_timeout: Duration::from_millis(50),
            ..Default::default()
        })
        .await;

        let c1 = pool.get_connection(&node_id).await.expect("first acquire");
        pool.return_connection(c1).await;

        // Pool now has one idle connection. Re-acquire must succeed without
        // creating a new connection (and without timing out).
        let c2 = pool.get_connection(&node_id).await.expect("reacquire");
        assert!(
            c2.permit.is_some(),
            "reused connection must carry its permit"
        );

        let metrics = pool.metrics().await;
        assert_eq!(
            metrics.connections_created, 1,
            "reuse must not create a second connection"
        );
    }
}
