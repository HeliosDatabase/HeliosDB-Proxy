//! Connection Pool Manager
//!
//! Central coordinator for mode-aware connection pooling.

use super::config::PoolModeConfig;
use super::lease::{ClientId, ConnectionLease, LeaseAction};
use super::metrics::PoolModeMetrics;
use super::mode::PoolingMode;
use crate::backend::BackendConfig;
use crate::connection_pool::{ConnectionPool, PoolConfig};
use crate::{NodeEndpoint, NodeId, ProxyError, Result};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Connection pool manager with mode awareness
///
/// Manages connection leases across multiple nodes with support for
/// different pooling modes (session, transaction, statement).
///
/// Leases carry live backend connections, which the manager can only open
/// with a backend template ([`Self::with_backend_template`]). Without one,
/// [`Self::acquire`] fails with [`ProxyError::Pool`]; node registration,
/// idle eviction, statistics and metrics still work.
pub struct ConnectionPoolManager {
    /// Pool mode configuration
    config: PoolModeConfig,
    /// Backend-connection template handed to every per-node pool; host and
    /// port come from each node's endpoint. `None`: acquisitions fail.
    backend_template: Option<BackendConfig>,
    /// Releases whose reset query could not run because the connection had
    /// no live backend client (the connection was closed, not pooled).
    resets_skipped: AtomicU64,
    /// Underlying connection pools per node
    pools: DashMap<NodeId, ConnectionPool>,
    /// Active leases by client ID
    active_leases: DashMap<ClientId, LeaseInfo>,
    /// Metrics
    metrics: Arc<PoolModeMetrics>,
}

/// Information about an active lease
struct LeaseInfo {
    /// Node the lease is connected to
    node_id: NodeId,
    /// Statements executed
    statements: u64,
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total connections across all pools
    pub total_connections: usize,
    /// Active (leased) connections
    pub active_connections: usize,
    /// Idle connections
    pub idle_connections: usize,
    /// Number of nodes
    pub node_count: usize,
    /// Per-node statistics
    pub node_stats: Vec<NodePoolStats>,
}

/// Per-node pool statistics
#[derive(Debug, Clone)]
pub struct NodePoolStats {
    /// Node identifier
    pub node_id: NodeId,
    /// Total connections for this node
    pub total: usize,
    /// Active connections
    pub active: usize,
    /// Idle connections
    pub idle: usize,
}

impl ConnectionPoolManager {
    /// Create a new connection pool manager
    pub fn new(config: PoolModeConfig) -> Self {
        Self {
            config,
            backend_template: None,
            resets_skipped: AtomicU64::new(0),
            pools: DashMap::new(),
            active_leases: DashMap::new(),
            metrics: Arc::new(PoolModeMetrics::new()),
        }
    }

    /// Attach a backend-connection template. Nodes added afterwards get a
    /// pool that opens live connections to the node's `host:port` with the
    /// template's credentials, TLS mode and timeouts.
    pub fn with_backend_template(mut self, template: BackendConfig) -> Self {
        self.backend_template = Some(template);
        self
    }

    /// Add a node to the pool manager.
    ///
    /// With a backend template the node's pool connects to `node.host` /
    /// `node.port`; without one the node is registered for accounting only
    /// and [`Self::acquire`] for it fails with [`ProxyError::Pool`].
    pub async fn add_node(&self, node: &NodeEndpoint) {
        let pool_config = PoolConfig {
            min_connections: self.config.min_idle as usize,
            max_connections: self.config.max_pool_size as usize,
            idle_timeout: self.config.idle_timeout(),
            max_lifetime: self.config.max_lifetime(),
            acquire_timeout: self.config.acquire_timeout(),
            test_on_acquire: self.config.test_on_acquire,
        };

        let pool = match &self.backend_template {
            Some(template) => {
                let pool = ConnectionPool::new(pool_config).with_backend_template(template.clone());
                pool.add_node_with_endpoint(node.id, node.host.clone(), node.port)
                    .await;
                pool
            }
            None => {
                let pool = ConnectionPool::new(pool_config);
                pool.add_node(node.id).await;
                pool
            }
        };
        self.pools.insert(node.id, pool);

        tracing::debug!("Added node {:?} to pool manager", node.id);
    }

    /// Remove a node from the pool manager
    pub async fn remove_node(&self, node_id: &NodeId) {
        if let Some((_, pool)) = self.pools.remove(node_id) {
            let _ = pool.close_all().await;
        }
        tracing::debug!("Removed node {:?} from pool manager", node_id);
    }

    /// Acquire a connection lease
    ///
    /// # Arguments
    /// * `client_id` - Client identifier
    /// * `node_id` - Target node
    ///
    /// # Returns
    /// A connection lease for the specified node
    pub async fn acquire(&self, client_id: ClientId, node_id: &NodeId) -> Result<ConnectionLease> {
        self.acquire_with_mode(client_id, node_id, self.config.default_mode)
            .await
    }

    /// Acquire a connection lease with specific mode
    pub async fn acquire_with_mode(
        &self,
        client_id: ClientId,
        node_id: &NodeId,
        mode: PoolingMode,
    ) -> Result<ConnectionLease> {
        // Check if client already has a lease
        if let Some(existing) = self.active_leases.get(&client_id) {
            if existing.node_id == *node_id {
                // Already has lease for this node - this shouldn't happen in normal usage
                // but we handle it gracefully
                tracing::warn!(
                    "Client {:?} already has active lease for node {:?}",
                    client_id,
                    node_id
                );
            }
        }

        // Get pool for node
        let pool = self
            .pools
            .get(node_id)
            .ok_or_else(|| ProxyError::PoolExhausted(format!("Node {:?} not in pool", node_id)))?;

        // Try to acquire connection
        let acquire_start = Instant::now();
        let connection =
            match tokio::time::timeout(self.config.acquire_timeout(), pool.get_connection(node_id))
                .await
            {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    self.metrics.record_acquire_failure();
                    return Err(e);
                }
                Err(_) => {
                    self.metrics.record_acquire_timeout();
                    return Err(ProxyError::Timeout(format!(
                        "Timeout acquiring connection for node {:?}",
                        node_id
                    )));
                }
            };

        let _acquire_duration = acquire_start.elapsed();

        // Create lease
        let lease = ConnectionLease::new(connection, mode, client_id);

        // Track active lease
        self.active_leases.insert(
            client_id,
            LeaseInfo {
                node_id: *node_id,
                statements: 0,
            },
        );

        // Record metrics
        self.metrics.record_acquire(mode);

        tracing::trace!(
            "Acquired {:?} lease for client {:?} on node {:?}",
            mode,
            client_id,
            node_id
        );

        Ok(lease)
    }

    /// Release a connection lease
    ///
    /// The connection will be reset and returned to the pool.
    pub async fn release(&self, lease: ConnectionLease) {
        let client_id = lease.client_id();
        let mode = lease.mode();
        let statements = lease.statements_executed();
        let duration_ms = lease.lease_duration().as_millis() as u64;

        // Remove from active leases
        if let Some((_, info)) = self.active_leases.remove(&client_id) {
            // Get pool
            if let Some(pool) = self.pools.get(&info.node_id) {
                let mut connection = lease.into_connection();

                // Reset connection for Transaction / Statement modes with
                // the configured reset query (default `DISCARD ALL`). A
                // connection without a live backend client cannot be reset:
                // it is closed instead of pooled, and counted as a skipped
                // reset (`resets_skipped`), never as a reset that ran.
                if mode != PoolingMode::Session && !connection.has_live_client() {
                    self.resets_skipped.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        connection = ?connection.id,
                        "reset query skipped: connection has no live backend client; \
                         closing it instead of returning it to the pool"
                    );
                    pool.close_connection(connection).await;
                    self.metrics.record_connection_closed();
                    return;
                }
                if mode != PoolingMode::Session {
                    let reset_query = self.config.reset_query.as_str();
                    match pool.run_reset_query(&mut connection, reset_query).await {
                        Ok(()) => {
                            tracing::trace!(query = reset_query, "reset query executed on release");
                            self.metrics.record_reset(true);
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "reset query failed; connection will not be returned to pool"
                            );
                            self.metrics.record_reset(false);
                            pool.close_connection(connection).await;
                            return;
                        }
                    }
                }

                // Return to pool
                pool.return_connection(connection).await;
            }
        }

        // Record metrics
        self.metrics.record_release(mode, duration_ms, statements);

        tracing::trace!(
            "Released {:?} lease for client {:?} after {} statements",
            mode,
            client_id,
            statements
        );
    }

    /// Release a connection lease and close it (don't return to pool)
    pub async fn release_and_close(&self, lease: ConnectionLease) {
        let client_id = lease.client_id();
        let mode = lease.mode();
        let statements = lease.statements_executed();
        let duration_ms = lease.lease_duration().as_millis() as u64;

        // Remove from active leases
        if let Some((_, info)) = self.active_leases.remove(&client_id) {
            // Get pool
            if let Some(pool) = self.pools.get(&info.node_id) {
                let connection = lease.into_connection();
                pool.close_connection(connection).await;
                self.metrics.record_connection_closed();
            }
        }

        // Record metrics
        self.metrics.record_release(mode, duration_ms, statements);
    }

    /// Process a completed statement and determine if connection should be released
    ///
    /// # Arguments
    /// * `lease` - The connection lease (mutable)
    /// * `sql` - The SQL that was executed
    ///
    /// # Returns
    /// The action to take with the connection
    pub fn on_statement_complete(&self, lease: &mut ConnectionLease, sql: &str) -> LeaseAction {
        let action = lease.on_statement_complete(sql);

        // Update statement count in lease info
        if let Some(mut info) = self.active_leases.get_mut(&lease.client_id()) {
            info.statements += 1;
        }

        // Track transaction completion
        if action == LeaseAction::Reset {
            self.metrics.record_transaction_complete();
        }

        action
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> PoolStats {
        let mut total = 0;
        let mut active = 0;
        let mut node_stats = Vec::new();

        for entry in self.pools.iter() {
            let node_id = *entry.key();
            let pool = entry.value();

            let pool_total = pool.total_connections().await;
            let pool_active = pool.active_connections().await;
            let pool_idle = pool_total.saturating_sub(pool_active);

            total += pool_total;
            active += pool_active;

            node_stats.push(NodePoolStats {
                node_id,
                total: pool_total,
                active: pool_active,
                idle: pool_idle,
            });
        }

        PoolStats {
            total_connections: total,
            active_connections: active,
            idle_connections: total.saturating_sub(active),
            node_count: self.pools.len(),
            node_stats,
        }
    }

    /// Get metrics
    pub fn metrics(&self) -> &PoolModeMetrics {
        &self.metrics
    }

    /// Releases whose reset query was skipped because the connection had no
    /// live backend client. Such a connection is closed, not pooled, and is
    /// not counted in `connection_resets`.
    pub fn resets_skipped(&self) -> u64 {
        self.resets_skipped.load(Ordering::Relaxed)
    }

    /// Get configuration
    pub fn config(&self) -> &PoolModeConfig {
        &self.config
    }

    /// Get default pooling mode
    pub fn default_mode(&self) -> PoolingMode {
        self.config.default_mode
    }

    /// Check if a client has an active lease
    pub fn has_active_lease(&self, client_id: &ClientId) -> bool {
        self.active_leases.contains_key(client_id)
    }

    /// Get the number of active leases
    pub fn active_lease_count(&self) -> usize {
        self.active_leases.len()
    }

    /// Close all connections in all pools
    pub async fn close_all(&self) {
        for entry in self.pools.iter() {
            let _ = entry.value().close_all().await;
        }
        self.active_leases.clear();
        tracing::info!("Closed all connections in pool manager");
    }

    /// Evict idle connections from all pools
    pub async fn evict_idle(&self) {
        for entry in self.pools.iter() {
            entry.value().evict_idle().await;
        }
    }
}

impl std::fmt::Debug for ConnectionPoolManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPoolManager")
            .field("default_mode", &self.config.default_mode)
            .field("max_pool_size", &self.config.max_pool_size)
            .field("active_leases", &self.active_leases.len())
            .field("nodes", &self.pools.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_pool::test_backend::FakePg;
    use crate::connection_pool::{ConnectionState, PooledConnection};

    /// A transaction-mode manager whose node points at a live fake backend.
    async fn live_manager() -> (ConnectionPoolManager, NodeEndpoint, FakePg) {
        let backend = FakePg::start().await;
        let manager = ConnectionPoolManager::new(PoolModeConfig::transaction_mode())
            .with_backend_template(backend.template());
        let node = NodeEndpoint::new(backend.addr.ip().to_string(), backend.addr.port());
        manager.add_node(&node).await;
        (manager, node, backend)
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let config = PoolModeConfig::default();
        let manager = ConnectionPoolManager::new(config);

        assert_eq!(manager.default_mode(), PoolingMode::Session);
        assert_eq!(manager.active_lease_count(), 0);
    }

    #[tokio::test]
    async fn test_add_remove_node() {
        let config = PoolModeConfig::default();
        let manager = ConnectionPoolManager::new(config);

        let node = NodeEndpoint::new("localhost", 5432);
        manager.add_node(&node).await;

        let stats = manager.get_stats().await;
        assert_eq!(stats.node_count, 1);

        manager.remove_node(&node.id).await;

        let stats = manager.get_stats().await;
        assert_eq!(stats.node_count, 0);
    }

    #[tokio::test]
    async fn test_acquire_release() {
        let (manager, node, backend) = live_manager().await;

        let client_id = ClientId::new();
        let lease = manager.acquire(client_id, &node.id).await.unwrap();

        assert!(manager.has_active_lease(&client_id));
        assert_eq!(manager.active_lease_count(), 1);

        manager.release(lease).await;

        assert!(!manager.has_active_lease(&client_id));
        assert_eq!(manager.active_lease_count(), 0);
        // Transaction mode: the reset query ran on the backend.
        assert_eq!(backend.queries(), vec!["DISCARD ALL".to_string()]);
        let snapshot = manager.metrics().snapshot();
        assert_eq!(snapshot.connection_resets, 1);
        assert_eq!(snapshot.reset_failures, 0);
        assert_eq!(manager.resets_skipped(), 0);
        // The reset connection went back to the pool.
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.idle_connections, 1);
    }

    #[tokio::test]
    async fn test_metrics_recording() {
        let (manager, node, _backend) = live_manager().await;

        let client_id = ClientId::new();
        let lease = manager.acquire(client_id, &node.id).await.unwrap();

        let snapshot = manager.metrics().snapshot();
        assert_eq!(snapshot.acquires, 1);

        manager.release(lease).await;

        let snapshot = manager.metrics().snapshot();
        assert_eq!(snapshot.releases, 1);
    }

    /// Fail closed: without a backend template the manager cannot open a
    /// backend connection, so acquire is an explicit error, counted as an
    /// acquire failure, and no lease is recorded.
    #[tokio::test]
    async fn test_acquire_without_backend_template_fails_closed() {
        let manager = ConnectionPoolManager::new(PoolModeConfig::transaction_mode());
        let node = NodeEndpoint::new("localhost", 5432);
        manager.add_node(&node).await;

        let client_id = ClientId::new();
        let err = manager
            .acquire(client_id, &node.id)
            .await
            .expect_err("acquire without a backend template must fail");
        assert!(matches!(err, ProxyError::Pool(_)), "got {err:?}");
        assert!(!manager.has_active_lease(&client_id));
        let snapshot = manager.metrics().snapshot();
        assert_eq!(snapshot.acquires, 0);
        assert_eq!(snapshot.acquire_failures, 1);
        assert_eq!(manager.get_stats().await.total_connections, 0);
    }

    /// A release whose connection has no live backend client must not be
    /// recorded as a reset that ran: it counts as skipped, the connection is
    /// closed instead of pooled, and `connection_resets` stays at zero.
    #[tokio::test]
    async fn test_release_without_live_client_skips_reset_and_closes() {
        let manager = ConnectionPoolManager::new(PoolModeConfig::transaction_mode());
        let node = NodeEndpoint::new("localhost", 5432);
        manager.add_node(&node).await;

        let now = chrono::Utc::now();
        let detached = PooledConnection {
            id: uuid::Uuid::new_v4(),
            node_id: node.id,
            created_at: now,
            last_used: now,
            state: ConnectionState::InUse,
            use_count: 1,
            permit: None,
            client: None,
        };
        let client_id = ClientId::new();
        manager.active_leases.insert(
            client_id,
            LeaseInfo {
                node_id: node.id,
                statements: 0,
            },
        );
        let lease = ConnectionLease::new(detached, PoolingMode::Transaction, client_id);

        manager.release(lease).await;

        assert_eq!(manager.resets_skipped(), 1);
        let snapshot = manager.metrics().snapshot();
        assert_eq!(snapshot.connection_resets, 0, "no reset ran");
        assert_eq!(snapshot.reset_failures, 0);
        assert_eq!(snapshot.connections_closed, 1);
        assert!(!manager.has_active_lease(&client_id));
        let stats = manager.get_stats().await;
        assert_eq!(stats.idle_connections, 0, "connection must not be pooled");
    }

    /// A reset the backend rejects is recorded as a failed reset and the
    /// connection is closed, not pooled.
    #[tokio::test]
    async fn test_release_with_failing_reset_closes_connection() {
        let (manager, node, backend) = live_manager().await;
        let client_id = ClientId::new();
        let lease = manager.acquire(client_id, &node.id).await.unwrap();

        backend.fail_queries(true);
        manager.release(lease).await;

        let snapshot = manager.metrics().snapshot();
        assert_eq!(snapshot.connection_resets, 1);
        assert_eq!(snapshot.reset_failures, 1);
        assert_eq!(manager.resets_skipped(), 0);
        let stats = manager.get_stats().await;
        assert_eq!(
            stats.total_connections, 0,
            "failed-reset connection is closed"
        );
    }
}
