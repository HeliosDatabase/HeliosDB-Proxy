//! Tenant-Aware Connection Pool
//!
//! This module provides connection pool management with per-tenant isolation
//! and resource allocation.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use super::config::{TenantConfig, TenantId, TenantPoolConfig};

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is idle and available
    Idle,
    /// Connection is in use
    Active,
    /// Connection is being validated
    Validating,
    /// Connection is closed
    Closed,
}

/// A pooled connection handle
#[derive(Debug)]
pub struct PooledConnection {
    /// Connection identifier
    id: u64,

    /// Tenant that owns this connection
    tenant_id: TenantId,

    /// When the connection was created
    created_at: Instant,

    /// When the connection was last used
    last_used: Instant,

    /// Current state
    state: ConnectionState,

    /// Total queries executed
    queries_executed: u64,

    /// Backend connection info (e.g., socket address)
    backend_info: String,
}

impl PooledConnection {
    /// Create a new pooled connection
    pub fn new(id: u64, tenant_id: TenantId, backend_info: impl Into<String>) -> Self {
        let now = Instant::now();
        Self {
            id,
            tenant_id,
            created_at: now,
            last_used: now,
            state: ConnectionState::Idle,
            queries_executed: 0,
            backend_info: backend_info.into(),
        }
    }

    /// Get connection ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get tenant ID
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Get connection age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get idle time
    pub fn idle_time(&self) -> Duration {
        self.last_used.elapsed()
    }

    /// Get connection state
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Check if connection is available
    pub fn is_available(&self) -> bool {
        self.state == ConnectionState::Idle
    }

    /// Mark connection as active
    pub fn mark_active(&mut self) {
        self.state = ConnectionState::Active;
        self.last_used = Instant::now();
    }

    /// Mark connection as idle
    pub fn mark_idle(&mut self) {
        self.state = ConnectionState::Idle;
        self.last_used = Instant::now();
    }

    /// Increment query counter
    pub fn record_query(&mut self) {
        self.queries_executed += 1;
        self.last_used = Instant::now();
    }

    /// Get backend info
    pub fn backend_info(&self) -> &str {
        &self.backend_info
    }

    /// Get queries executed count
    pub fn queries_executed(&self) -> u64 {
        self.queries_executed
    }
}

/// Per-tenant connection pool
#[derive(Debug)]
pub struct TenantPool {
    /// Tenant ID
    tenant_id: TenantId,

    /// Pool configuration
    config: TenantPoolConfig,

    /// Active connections count
    active_count: AtomicU32,

    /// Idle connections count
    idle_count: AtomicU32,

    /// Total connections created
    total_created: AtomicU64,

    /// Total connections closed
    total_closed: AtomicU64,

    /// Waiting requests count
    waiting_count: AtomicU32,

    /// Pool creation time
    created_at: Instant,
}

impl TenantPool {
    /// Create a new tenant pool
    pub fn new(tenant_id: TenantId, config: TenantPoolConfig) -> Self {
        Self {
            tenant_id,
            config,
            active_count: AtomicU32::new(0),
            idle_count: AtomicU32::new(0),
            total_created: AtomicU64::new(0),
            total_closed: AtomicU64::new(0),
            waiting_count: AtomicU32::new(0),
            created_at: Instant::now(),
        }
    }

    /// Get tenant ID
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Get pool configuration
    pub fn config(&self) -> &TenantPoolConfig {
        &self.config
    }

    /// Get active connection count
    pub fn active_count(&self) -> u32 {
        self.active_count.load(Ordering::Relaxed)
    }

    /// Get idle connection count
    pub fn idle_count(&self) -> u32 {
        self.idle_count.load(Ordering::Relaxed)
    }

    /// Get total connection count
    pub fn total_count(&self) -> u32 {
        self.active_count() + self.idle_count()
    }

    /// Get waiting requests count
    pub fn waiting_count(&self) -> u32 {
        self.waiting_count.load(Ordering::Relaxed)
    }

    /// Check if pool is at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.total_count() >= self.config.max_connections
    }

    /// Check if pool can accept new connection
    pub fn can_create_connection(&self) -> bool {
        self.total_count() < self.config.max_connections
    }

    /// Check if pool has available connections
    pub fn has_available(&self) -> bool {
        self.idle_count() > 0
    }

    /// Record connection acquired
    pub fn record_acquire(&self) {
        self.idle_count.fetch_sub(1, Ordering::Relaxed);
        self.active_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection released
    pub fn record_release(&self) {
        self.active_count.fetch_sub(1, Ordering::Relaxed);
        self.idle_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection created
    pub fn record_created(&self) {
        self.idle_count.fetch_add(1, Ordering::Relaxed);
        self.total_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection closed
    pub fn record_closed(&self, was_active: bool) {
        if was_active {
            self.active_count.fetch_sub(1, Ordering::Relaxed);
        } else {
            self.idle_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.total_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record waiting request
    pub fn record_waiting(&self) {
        self.waiting_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record request no longer waiting
    pub fn record_not_waiting(&self) {
        self.waiting_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get pool utilization (0.0 to 1.0)
    pub fn utilization(&self) -> f32 {
        let total = self.total_count();
        if total == 0 {
            return 0.0;
        }
        self.active_count() as f32 / self.config.max_connections as f32
    }

    /// Get pool age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> TenantPoolStats {
        TenantPoolStats {
            tenant_id: self.tenant_id.clone(),
            active: self.active_count(),
            idle: self.idle_count(),
            max: self.config.max_connections,
            waiting: self.waiting_count(),
            total_created: self.total_created.load(Ordering::Relaxed),
            total_closed: self.total_closed.load(Ordering::Relaxed),
            utilization: self.utilization(),
            age: self.age(),
        }
    }
}

/// Tenant pool statistics
#[derive(Debug, Clone)]
pub struct TenantPoolStats {
    /// Tenant ID
    pub tenant_id: TenantId,

    /// Active connections
    pub active: u32,

    /// Idle connections
    pub idle: u32,

    /// Maximum connections
    pub max: u32,

    /// Waiting requests
    pub waiting: u32,

    /// Total connections created
    pub total_created: u64,

    /// Total connections closed
    pub total_closed: u64,

    /// Pool utilization (0.0 to 1.0)
    pub utilization: f32,

    /// Pool age
    pub age: Duration,
}

/// Tenant connection pool manager
///
/// Manages connection pools across all tenants.
pub struct TenantConnectionPool {
    /// Per-tenant pools
    pools: DashMap<TenantId, Arc<TenantPool>>,

    /// Shared pool for small/unknown tenants
    shared_pool: Arc<TenantPool>,

    /// Default pool configuration
    #[allow(dead_code)]
    default_config: TenantPoolConfig,

    /// Connection ID counter
    connection_counter: AtomicU64,

    /// Total acquire count
    total_acquires: AtomicU64,

    /// Total acquire timeouts
    total_timeouts: AtomicU64,

    /// Threshold for dedicated pool (connections)
    dedicated_pool_threshold: u32,
}

impl TenantConnectionPool {
    /// Create a new tenant connection pool manager
    pub fn new(default_config: TenantPoolConfig) -> Self {
        let shared_pool = Arc::new(TenantPool::new(
            TenantId::new("__shared__"),
            TenantPoolConfig {
                max_connections: 50,
                ..default_config.clone()
            },
        ));

        Self {
            pools: DashMap::new(),
            shared_pool,
            default_config,
            connection_counter: AtomicU64::new(0),
            total_acquires: AtomicU64::new(0),
            total_timeouts: AtomicU64::new(0),
            dedicated_pool_threshold: 5,
        }
    }

    /// Set dedicated pool threshold
    pub fn with_dedicated_threshold(mut self, threshold: u32) -> Self {
        self.dedicated_pool_threshold = threshold;
        self
    }

    /// Get or create pool for tenant
    pub fn get_pool(&self, tenant: &TenantId, config: &TenantConfig) -> Arc<TenantPool> {
        // Check if tenant should use dedicated pool
        if config.pool.dedicated_pool
            || config.pool.max_connections >= self.dedicated_pool_threshold
        {
            self.pools
                .entry(tenant.clone())
                .or_insert_with(|| Arc::new(TenantPool::new(tenant.clone(), config.pool.clone())))
                .clone()
        } else {
            self.shared_pool.clone()
        }
    }

    /// Get existing pool for tenant (if any)
    pub fn get_existing_pool(&self, tenant: &TenantId) -> Option<Arc<TenantPool>> {
        self.pools.get(tenant).map(|p| p.clone())
    }

    /// Create a tenant-specific pool
    pub fn create_tenant_pool(&self, tenant: &TenantId, config: TenantPoolConfig) {
        self.pools
            .insert(tenant.clone(), Arc::new(TenantPool::new(tenant.clone(), config)));
    }

    /// Remove a tenant pool
    pub fn remove_tenant_pool(&self, tenant: &TenantId) -> Option<Arc<TenantPool>> {
        self.pools.remove(tenant).map(|(_, pool)| pool)
    }

    /// Generate new connection ID
    pub fn next_connection_id(&self) -> u64 {
        self.connection_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Record an acquire attempt
    pub fn record_acquire(&self) {
        self.total_acquires.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timeout
    pub fn record_timeout(&self) {
        self.total_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all tenant pool stats
    pub fn all_stats(&self) -> Vec<TenantPoolStats> {
        let mut stats: Vec<TenantPoolStats> = self
            .pools
            .iter()
            .map(|entry| entry.value().stats())
            .collect();

        // Add shared pool stats
        stats.push(self.shared_pool.stats());

        stats
    }

    /// Get tenant pool stats
    pub fn tenant_stats(&self, tenant: &TenantId) -> Option<TenantPoolStats> {
        self.pools.get(tenant).map(|p| p.stats())
    }

    /// Get shared pool stats
    pub fn shared_pool_stats(&self) -> TenantPoolStats {
        self.shared_pool.stats()
    }

    /// Get total number of tenant pools
    pub fn tenant_pool_count(&self) -> usize {
        self.pools.len()
    }

    /// Get aggregate statistics
    pub fn aggregate_stats(&self) -> AggregatePoolStats {
        let mut total_active = 0u32;
        let mut total_idle = 0u32;
        let mut total_max = 0u32;
        let mut total_waiting = 0u32;

        for pool in self.pools.iter() {
            total_active += pool.active_count();
            total_idle += pool.idle_count();
            total_max += pool.config().max_connections;
            total_waiting += pool.waiting_count();
        }

        // Add shared pool
        total_active += self.shared_pool.active_count();
        total_idle += self.shared_pool.idle_count();
        total_max += self.shared_pool.config().max_connections;
        total_waiting += self.shared_pool.waiting_count();

        AggregatePoolStats {
            tenant_pools: self.pools.len(),
            total_active,
            total_idle,
            total_max,
            total_waiting,
            total_acquires: self.total_acquires.load(Ordering::Relaxed),
            total_timeouts: self.total_timeouts.load(Ordering::Relaxed),
            average_utilization: if total_max > 0 {
                total_active as f32 / total_max as f32
            } else {
                0.0
            },
        }
    }
}

/// Aggregate pool statistics
#[derive(Debug, Clone)]
pub struct AggregatePoolStats {
    /// Number of tenant-specific pools
    pub tenant_pools: usize,

    /// Total active connections across all pools
    pub total_active: u32,

    /// Total idle connections across all pools
    pub total_idle: u32,

    /// Total maximum connections across all pools
    pub total_max: u32,

    /// Total waiting requests
    pub total_waiting: u32,

    /// Total acquire attempts
    pub total_acquires: u64,

    /// Total timeout occurrences
    pub total_timeouts: u64,

    /// Average utilization across all pools
    pub average_utilization: f32,
}

/// Acquire result
#[derive(Debug)]
pub enum AcquireResult {
    /// Connection acquired successfully
    Success(PooledConnection),

    /// Waiting for connection
    Waiting,

    /// Pool is at capacity
    PoolExhausted,

    /// Tenant not found
    TenantNotFound,

    /// Acquire timed out
    Timeout,
}

/// Connection lease for tenant
pub struct TenantConnectionLease {
    /// The pooled connection
    connection: PooledConnection,

    /// Pool reference for returning connection
    pool: Arc<TenantPool>,

    /// Lease start time
    leased_at: Instant,

    /// Whether connection was used
    used: bool,
}

impl TenantConnectionLease {
    /// Create a new lease
    pub fn new(connection: PooledConnection, pool: Arc<TenantPool>) -> Self {
        Self {
            connection,
            pool,
            leased_at: Instant::now(),
            used: false,
        }
    }

    /// Get the connection
    pub fn connection(&self) -> &PooledConnection {
        &self.connection
    }

    /// Get mutable connection
    pub fn connection_mut(&mut self) -> &mut PooledConnection {
        self.used = true;
        &mut self.connection
    }

    /// Get tenant ID
    pub fn tenant_id(&self) -> &TenantId {
        self.connection.tenant_id()
    }

    /// Get lease duration
    pub fn lease_duration(&self) -> Duration {
        self.leased_at.elapsed()
    }

    /// Mark as used
    pub fn mark_used(&mut self) {
        self.used = true;
    }

    /// Check if lease was used
    pub fn was_used(&self) -> bool {
        self.used
    }

    /// Release the lease (return connection to pool)
    pub fn release(mut self) {
        self.connection.mark_idle();
        self.pool.record_release();
    }
}

impl Drop for TenantConnectionLease {
    fn drop(&mut self) {
        // Auto-release if not explicitly released
        if self.connection.state() == ConnectionState::Active {
            self.pool.record_release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pooled_connection() {
        let tenant = TenantId::new("test");
        let mut conn = PooledConnection::new(1, tenant.clone(), "127.0.0.1:5432");

        assert_eq!(conn.id(), 1);
        assert_eq!(conn.tenant_id().as_str(), "test");
        assert!(conn.is_available());
        assert_eq!(conn.state(), ConnectionState::Idle);

        conn.mark_active();
        assert!(!conn.is_available());
        assert_eq!(conn.state(), ConnectionState::Active);

        conn.record_query();
        assert_eq!(conn.queries_executed(), 1);

        conn.mark_idle();
        assert!(conn.is_available());
    }

    #[test]
    fn test_tenant_pool() {
        let tenant = TenantId::new("test");
        let config = TenantPoolConfig {
            max_connections: 10,
            ..Default::default()
        };
        let pool = TenantPool::new(tenant.clone(), config);

        assert_eq!(pool.tenant_id().as_str(), "test");
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.idle_count(), 0);
        assert!(!pool.is_at_capacity());
        assert!(pool.can_create_connection());

        pool.record_created();
        assert_eq!(pool.idle_count(), 1);

        pool.record_acquire();
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.idle_count(), 0);

        pool.record_release();
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.idle_count(), 1);
    }

    #[test]
    fn test_tenant_pool_utilization() {
        let tenant = TenantId::new("test");
        let config = TenantPoolConfig {
            max_connections: 10,
            ..Default::default()
        };
        let pool = TenantPool::new(tenant, config);

        assert_eq!(pool.utilization(), 0.0);

        // Create 5 connections, use 3
        for _ in 0..5 {
            pool.record_created();
        }
        for _ in 0..3 {
            pool.record_acquire();
        }

        assert_eq!(pool.active_count(), 3);
        assert_eq!(pool.idle_count(), 2);
        assert!((pool.utilization() - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_tenant_connection_pool() {
        let config = TenantPoolConfig::default();
        let manager = TenantConnectionPool::new(config.clone());

        let tenant = TenantId::new("tenant_a");
        let tenant_config = TenantConfig::builder()
            .id("tenant_a")
            .name("Tenant A")
            .database_isolation("tenant_a_db")
            .max_connections(20)
            .pool(TenantPoolConfig {
                max_connections: 20,
                dedicated_pool: true,
                ..Default::default()
            })
            .build();

        let pool = manager.get_pool(&tenant, &tenant_config);
        assert_eq!(pool.config().max_connections, 20);

        let stats = manager.aggregate_stats();
        assert_eq!(stats.tenant_pools, 1);
    }

    #[test]
    fn test_shared_pool_usage() {
        let config = TenantPoolConfig::default();
        let manager = TenantConnectionPool::new(config.clone());

        let tenant = TenantId::new("small_tenant");
        let tenant_config = TenantConfig::builder()
            .id("small_tenant")
            .name("Small Tenant")
            .schema_isolation("shared", "small_tenant")
            .max_connections(2) // Below threshold
            .build();

        let pool = manager.get_pool(&tenant, &tenant_config);

        // Should use shared pool
        assert_eq!(pool.tenant_id().as_str(), "__shared__");
    }

    #[test]
    fn test_tenant_connection_lease() {
        let tenant = TenantId::new("test");
        let config = TenantPoolConfig::default();
        let pool = Arc::new(TenantPool::new(tenant.clone(), config));

        pool.record_created();
        pool.record_acquire();

        let conn = PooledConnection::new(1, tenant.clone(), "127.0.0.1:5432");
        let mut lease = TenantConnectionLease::new(conn, pool.clone());

        assert_eq!(lease.tenant_id().as_str(), "test");
        assert!(!lease.was_used());

        lease.mark_used();
        assert!(lease.was_used());

        // Release explicitly
        lease.release();
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.idle_count(), 1);
    }

    #[test]
    fn test_pool_stats() {
        let tenant = TenantId::new("test");
        let config = TenantPoolConfig {
            max_connections: 10,
            ..Default::default()
        };
        let pool = TenantPool::new(tenant, config);

        pool.record_created();
        pool.record_created();
        pool.record_acquire();

        let stats = pool.stats();
        assert_eq!(stats.active, 1);
        assert_eq!(stats.idle, 1);
        assert_eq!(stats.max, 10);
        assert_eq!(stats.total_created, 2);
    }
}
