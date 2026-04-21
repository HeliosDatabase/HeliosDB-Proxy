//! Connection Pool - HeliosProxy
//!
//! Manages connection pooling with configurable limits, idle timeout,
//! and health-aware connection management.

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
#[derive(Debug)]
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

/// Per-node connection pool
struct NodePool {
    /// Node ID
    node_id: NodeId,
    /// Available connections
    connections: Vec<PooledConnection>,
    /// Semaphore for limiting connections
    semaphore: Arc<Semaphore>,
    /// Total created connections
    total_created: u64,
    /// Total closed connections
    total_closed: u64,
}

impl NodePool {
    fn new(node_id: NodeId, max_connections: usize) -> Self {
        Self {
            node_id,
            connections: Vec::new(),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            total_created: 0,
            total_closed: 0,
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
        }
    }

    /// Add a node to the pool
    pub async fn add_node(&self, node_id: NodeId) {
        let mut pools = self.pools.write().await;
        if !pools.contains_key(&node_id) {
            pools.insert(
                node_id,
                NodePool::new(node_id, self.config.max_connections),
            );
            tracing::debug!("Added node {:?} to connection pool", node_id);
        }
    }

    /// Remove a node from the pool
    pub async fn remove_node(&self, node_id: &NodeId) {
        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.remove(node_id) {
            let count = pool.connections.len() as u64;
            self.total_connections.fetch_sub(count, Ordering::SeqCst);
            tracing::debug!("Removed node {:?} from connection pool", node_id);
        }
    }

    /// Get a connection from the pool
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

            if age <= self.config.max_lifetime {
                conn.state = ConnectionState::InUse;
                conn.last_used = chrono::Utc::now();
                conn.use_count += 1;
                self.active_connections.fetch_add(1, Ordering::SeqCst);
                return Ok(conn);
            }

            // Too old — drop it (which releases its permit) and fall through
            // to the create-new path.
            self.metrics
                .connections_recycled
                .fetch_add(1, Ordering::Relaxed);
            self.total_connections.fetch_sub(1, Ordering::SeqCst);
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
        self.active_connections.fetch_sub(1, Ordering::SeqCst);

        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.get_mut(&conn.node_id) {
            conn.state = ConnectionState::Idle;
            conn.last_used = chrono::Utc::now();
            pool.connections.push(conn);
        }
    }

    /// Close a connection (don't return to pool)
    pub async fn close_connection(&self, conn: PooledConnection) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
        self.total_connections.fetch_sub(1, Ordering::SeqCst);
        self.metrics
            .connections_closed
            .fetch_add(1, Ordering::Relaxed);

        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.get_mut(&conn.node_id) {
            pool.total_closed += 1;
        }

        tracing::debug!("Closed connection {:?}", conn.id);
    }

    /// Create a new connection, attaching the given semaphore permit (if any)
    /// so that it's released when the connection is dropped or closed.
    async fn create_connection(
        &self,
        node_id: NodeId,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<PooledConnection> {
        // TODO: Implement actual connection creation
        // For skeleton, we create a mock connection

        let now = chrono::Utc::now();
        let conn = PooledConnection {
            id: Uuid::new_v4(),
            node_id,
            created_at: now,
            last_used: now,
            state: ConnectionState::InUse,
            use_count: 1,
            permit,
        };

        self.metrics
            .connections_created
            .fetch_add(1, Ordering::Relaxed);

        tracing::debug!("Created connection {:?} for node {:?}", conn.id, node_id);

        Ok(conn)
    }

    /// Validate a connection
    pub async fn validate_connection(&self, conn: &PooledConnection) -> Result<bool> {
        // TODO: Implement actual validation (e.g., ping)
        // For skeleton, always return true
        if conn.state == ConnectionState::Closed {
            self.metrics
                .validation_failures
                .fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }
        Ok(true)
    }

    /// Close all connections
    pub async fn close_all(&self) -> Result<()> {
        let mut pools = self.pools.write().await;
        for (_, pool) in pools.iter_mut() {
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

        for (_, pool) in pools.iter_mut() {
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
            self.total_connections
                .fetch_sub(evicted as u64, Ordering::SeqCst);
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let pool = ConnectionPool::new(PoolConfig::default());
        let node_id = NodeId::new();

        pool.add_node(node_id).await;

        // Get connection
        let conn = pool.get_connection(&node_id).await.expect("get failed");
        assert_eq!(conn.node_id, node_id);
        assert_eq!(conn.state, ConnectionState::InUse);
        assert_eq!(pool.active_connections().await, 1);

        // Return connection
        pool.return_connection(conn).await;
        assert_eq!(pool.active_connections().await, 0);
    }

    #[tokio::test]
    async fn test_metrics() {
        let pool = ConnectionPool::new(PoolConfig::default());
        let node_id = NodeId::new();

        pool.add_node(node_id).await;

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
        let pool = ConnectionPool::new(PoolConfig {
            min_connections: 0,
            max_connections: 2,
            acquire_timeout: Duration::from_millis(50),
            ..Default::default()
        });
        let node_id = NodeId::new();
        pool.add_node(node_id).await;

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

    /// Returning a connection to the pool keeps the permit attached, so
    /// reusing it should not consume a new permit.
    #[tokio::test]
    async fn test_return_then_reacquire_reuses_permit() {
        let pool = ConnectionPool::new(PoolConfig {
            min_connections: 0,
            max_connections: 1,
            acquire_timeout: Duration::from_millis(50),
            ..Default::default()
        });
        let node_id = NodeId::new();
        pool.add_node(node_id).await;

        let c1 = pool.get_connection(&node_id).await.expect("first acquire");
        pool.return_connection(c1).await;

        // Pool now has one idle connection. Re-acquire must succeed without
        // creating a new connection (and without timing out).
        let c2 = pool.get_connection(&node_id).await.expect("reacquire");
        assert!(c2.permit.is_some(), "reused connection must carry its permit");

        let metrics = pool.metrics().await;
        assert_eq!(
            metrics.connections_created, 1,
            "reuse must not create a second connection"
        );
    }
}
