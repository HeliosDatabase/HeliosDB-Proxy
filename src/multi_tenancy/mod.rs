//! Multi-Tenancy Support for HeliosProxy
//!
//! This module provides comprehensive multi-tenant isolation for database proxying.
//!
//! # Features
//!
//! - **Multiple Isolation Strategies**: Database, Schema, Row-level, or Branch isolation
//! - **Flexible Tenant Identification**: Header, username prefix, JWT, or database name
//! - **Per-Tenant Connection Pools**: Dedicated or shared pools with configurable limits
//! - **Query Transformation**: Automatic tenant filtering for row-level isolation
//! - **Comprehensive Metrics**: Per-tenant query stats, latencies, and costs
//!
//! # Example
//!
//! ```rust
//! use heliosdb::proxy::multi_tenancy::{
//!     TenantManager, TenantConfig, IsolationStrategy,
//!     IdentificationMethod, RequestContext,
//! };
//!
//! // Create tenant manager
//! let manager = TenantManager::new();
//!
//! // Register a tenant with schema isolation
//! let config = TenantConfig::builder()
//!     .id("acme")
//!     .name("Acme Corp")
//!     .schema_isolation("shared_db", "acme")
//!     .max_connections(50)
//!     .qps_limit(1000)
//!     .build();
//!
//! manager.register_tenant(config);
//!
//! // Identify tenant from request
//! let ctx = RequestContext::new()
//!     .with_header("X-Tenant-Id", "acme");
//!
//! let tenant = manager.identify_tenant(&ctx);
//! ```
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │              MULTI-TENANT PROXY                  │
//! │                                                  │
//! │  ┌──────────────────────────────────────────┐   │
//! │  │ Tenant Identification                    │   │
//! │  │ - Header (X-Tenant-Id)                   │   │
//! │  │ - Username prefix (tenant.user)          │   │
//! │  │ - JWT claim                              │   │
//! │  └──────────────────────────────────────────┘   │
//! │                    │                             │
//! │                    ▼                             │
//! │  ┌──────────────────────────────────────────┐   │
//! │  │ Isolation Strategy                       │   │
//! │  │ Database | Schema | Row | Branch         │   │
//! │  └──────────────────────────────────────────┘   │
//! │                    │                             │
//! │                    ▼                             │
//! │  ┌──────────────────────────────────────────┐   │
//! │  │ Per-Tenant Resources                     │   │
//! │  │ - Connection pools                       │   │
//! │  │ - Rate limits                            │   │
//! │  │ - Metrics                                │   │
//! │  └──────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod identifier;
pub mod isolation;
pub mod metrics;
pub mod pool;
pub mod transformer;

use std::sync::Arc;

use dashmap::DashMap;

pub use config::{
    IdentificationMethod, IsolationStrategy, MultiTenancyConfig, TenantAiConfig, TenantConfig,
    TenantConfigBuilder, TenantId, TenantPermissions, TenantPoolConfig, TenantRateLimits,
};
pub use identifier::{
    create_identifier, CompositeIdentifier, DatabaseNameIdentifier, HeaderTenantIdentifier,
    JwtClaimIdentifier, RequestContext, SqlContextIdentifier, TenantIdentifier,
    UsernamePrefixIdentifier,
};
pub use isolation::{
    create_handler, BranchIsolationHandler, DatabaseIsolationHandler, IsolationHandler,
    IsolationRouter, RoutingDecision, RowIsolationHandler, SchemaIsolationHandler,
    TenantProvisioner,
};
pub use metrics::{
    AggregateMetricsSnapshot, TenantCostEntry, TenantCostReport, TenantCostTracker, TenantMetrics,
    TenantMetricsSnapshot, TenantStats,
};
pub use pool::{
    AcquireResult, AggregatePoolStats, ConnectionState, PooledConnection, TenantConnectionLease,
    TenantConnectionPool, TenantPool, TenantPoolStats,
};
pub use transformer::{validate_query, QueryValidation, TenantQueryTransformer, TransformResult};

/// Central tenant manager
///
/// Coordinates all multi-tenancy functionality including identification,
/// isolation, connection pooling, and metrics.
pub struct TenantManager {
    /// Global configuration
    config: MultiTenancyConfig,

    /// Tenant configurations
    tenants: DashMap<TenantId, TenantConfig>,

    /// Tenant identifier
    identifier: Arc<dyn TenantIdentifier>,

    /// Isolation router
    isolation_router: IsolationRouter,

    /// Connection pool manager
    pool_manager: TenantConnectionPool,

    /// Query transformer
    query_transformer: TenantQueryTransformer,

    /// Metrics collector
    metrics: TenantMetrics,

    /// Cost tracker
    cost_tracker: TenantCostTracker,

    /// Tenant provisioner
    provisioner: TenantProvisioner,
}

impl TenantManager {
    /// Create a new tenant manager with default configuration
    pub fn new() -> Self {
        Self::with_config(MultiTenancyConfig::default())
    }

    /// Create a tenant manager with custom configuration
    pub fn with_config(config: MultiTenancyConfig) -> Self {
        let identifier = create_identifier(&config.identification);

        Self {
            config: config.clone(),
            tenants: DashMap::new(),
            identifier: Arc::from(identifier),
            isolation_router: IsolationRouter::new(),
            pool_manager: TenantConnectionPool::new(TenantPoolConfig::default()),
            query_transformer: TenantQueryTransformer::new(),
            metrics: TenantMetrics::new(),
            cost_tracker: TenantCostTracker::new(),
            provisioner: TenantProvisioner::new(),
        }
    }

    /// Check if multi-tenancy is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Register a tenant
    pub fn register_tenant(&self, config: TenantConfig) {
        let tenant_id = config.id.clone();

        // Register isolation handler
        self.isolation_router.register_from_config(&config);

        // Create connection pool
        self.pool_manager
            .create_tenant_pool(&tenant_id, config.pool.clone());

        // Store config
        self.tenants.insert(tenant_id, config);
    }

    /// Unregister a tenant
    pub fn unregister_tenant(&self, tenant: &TenantId) -> Option<TenantConfig> {
        self.pool_manager.remove_tenant_pool(tenant);
        self.tenants.remove(tenant).map(|(_, c)| c)
    }

    /// Get tenant configuration
    pub fn get_tenant(&self, tenant: &TenantId) -> Option<TenantConfig> {
        self.tenants.get(tenant).map(|e| e.clone())
    }

    /// Check if tenant exists
    pub fn has_tenant(&self, tenant: &TenantId) -> bool {
        self.tenants.contains_key(tenant)
    }

    /// Get all tenant IDs
    pub fn tenant_ids(&self) -> Vec<TenantId> {
        self.tenants.iter().map(|e| e.key().clone()).collect()
    }

    /// Get tenant count
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// Identify tenant from request context
    pub fn identify_tenant(&self, request: &RequestContext) -> Option<TenantId> {
        let tenant_id = self.identifier.identify(request)?;

        // Check if tenant exists or if unknown tenants are allowed
        if self.has_tenant(&tenant_id) {
            Some(tenant_id)
        } else if self.config.allow_unknown_tenants {
            // Auto-create if enabled
            if self.config.auto_create_tenants {
                let config = self.create_default_tenant_config(&tenant_id);
                self.register_tenant(config);
            }
            Some(tenant_id)
        } else {
            None
        }
    }

    /// Create default configuration for a new tenant
    fn create_default_tenant_config(&self, tenant: &TenantId) -> TenantConfig {
        let isolation = self.provisioner.generate_isolation(
            tenant,
            self.config.default_config.isolation.strategy_name(),
            self.config.default_config.isolation.database_name(),
        );

        TenantConfig::builder()
            .id(tenant.clone())
            .name(tenant.0.clone())
            .isolation(isolation)
            .rate_limits(self.config.default_config.rate_limits.clone())
            .pool(self.config.default_config.pool.clone())
            .build()
    }

    /// Get routing decision for a tenant
    pub fn get_routing(&self, tenant: &TenantId) -> Option<RoutingDecision> {
        let config = self.get_tenant(tenant)?;
        Some(self.isolation_router.get_routing(tenant, &config))
    }

    /// Transform a query for tenant isolation
    pub fn transform_query(&self, query: &str, tenant: &TenantId) -> TransformResult {
        if let Some(config) = self.get_tenant(tenant) {
            self.query_transformer.transform(query, tenant, &config)
        } else {
            TransformResult::passthrough(query)
        }
    }

    /// Validate a query for a tenant
    pub fn validate_query(&self, query: &str, tenant: &TenantId) -> QueryValidation {
        if let Some(config) = self.get_tenant(tenant) {
            validate_query(query, tenant, &config)
        } else {
            QueryValidation {
                valid: false,
                violations: vec!["Unknown tenant".to_string()],
            }
        }
    }

    /// Get connection pool for a tenant
    pub fn get_pool(&self, tenant: &TenantId) -> Option<Arc<TenantPool>> {
        let config = self.get_tenant(tenant)?;
        Some(self.pool_manager.get_pool(tenant, &config))
    }

    /// Record a query execution
    pub fn record_query(
        &self,
        tenant: &TenantId,
        duration: std::time::Duration,
        rows: u64,
        bytes_read: u64,
        bytes_written: u64,
        success: bool,
    ) {
        self.metrics.record_query(tenant, duration, rows, success);
        self.metrics.record_bytes(tenant, bytes_read, bytes_written);
        self.cost_tracker
            .record_query_cost(tenant, rows, bytes_read, bytes_written);
    }

    /// Get metrics for a tenant
    pub fn tenant_metrics(&self, tenant: &TenantId) -> Option<TenantMetricsSnapshot> {
        self.metrics.snapshot(tenant)
    }

    /// Get aggregate metrics
    pub fn aggregate_metrics(&self) -> AggregateMetricsSnapshot {
        self.metrics.aggregate_snapshot()
    }

    /// Get top tenants by queries
    pub fn top_tenants_by_queries(&self, limit: usize) -> Vec<TenantMetricsSnapshot> {
        self.metrics.top_by_queries(limit)
    }

    /// Get cost for a tenant
    pub fn tenant_cost(&self, tenant: &TenantId) -> Option<f64> {
        self.cost_tracker.get_cost(tenant)
    }

    /// Get cost report
    pub fn cost_report(&self) -> TenantCostReport {
        self.cost_tracker.cost_report()
    }

    /// Get pool statistics for all tenants
    pub fn pool_stats(&self) -> Vec<TenantPoolStats> {
        self.pool_manager.all_stats()
    }

    /// Get aggregate pool statistics
    pub fn aggregate_pool_stats(&self) -> AggregatePoolStats {
        self.pool_manager.aggregate_stats()
    }

    /// Get the tenant provisioner
    pub fn provisioner(&self) -> &TenantProvisioner {
        &self.provisioner
    }

    /// Get the query transformer
    pub fn query_transformer(&self) -> &TenantQueryTransformer {
        &self.query_transformer
    }

    /// Get the metrics collector
    pub fn metrics(&self) -> &TenantMetrics {
        &self.metrics
    }

    /// Check if request is from admin user
    pub fn is_admin_request(&self, request: &RequestContext) -> bool {
        if let Some(pattern) = &self.config.admin_user_pattern {
            if let Some(username) = &request.username {
                // Simple pattern matching (in production, use regex)
                return username.starts_with(pattern) || username == pattern;
            }
        }
        false
    }

    /// Update tenant configuration
    pub fn update_tenant(&self, tenant: &TenantId, config: TenantConfig) -> bool {
        if self.tenants.contains_key(tenant) {
            self.isolation_router.register_from_config(&config);
            self.pool_manager
                .create_tenant_pool(tenant, config.pool.clone());
            self.tenants.insert(tenant.clone(), config);
            true
        } else {
            false
        }
    }

    /// Enable a tenant
    pub fn enable_tenant(&self, tenant: &TenantId) -> bool {
        if let Some(mut entry) = self.tenants.get_mut(tenant) {
            entry.enabled = true;
            true
        } else {
            false
        }
    }

    /// Disable a tenant
    pub fn disable_tenant(&self, tenant: &TenantId) -> bool {
        if let Some(mut entry) = self.tenants.get_mut(tenant) {
            entry.enabled = false;
            true
        } else {
            false
        }
    }

    /// Check if tenant is enabled
    pub fn is_tenant_enabled(&self, tenant: &TenantId) -> bool {
        self.tenants.get(tenant).map(|c| c.enabled).unwrap_or(false)
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for TenantManager
pub struct TenantManagerBuilder {
    config: MultiTenancyConfig,
    identifier: Option<Arc<dyn TenantIdentifier>>,
    query_transformer: Option<TenantQueryTransformer>,
    provisioner: Option<TenantProvisioner>,
}

impl TenantManagerBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: MultiTenancyConfig::enabled(),
            identifier: None,
            query_transformer: None,
            provisioner: None,
        }
    }

    /// Set configuration
    pub fn config(mut self, config: MultiTenancyConfig) -> Self {
        self.config = config;
        self
    }

    /// Set custom identifier
    pub fn identifier(mut self, identifier: Arc<dyn TenantIdentifier>) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// Use header identification
    pub fn header_identification(mut self, header: impl Into<String>) -> Self {
        self.config.identification = IdentificationMethod::header(header);
        self
    }

    /// Use username prefix identification
    pub fn username_prefix_identification(mut self, separator: char) -> Self {
        self.config.identification = IdentificationMethod::username_prefix(separator);
        self
    }

    /// Set custom query transformer
    pub fn query_transformer(mut self, transformer: TenantQueryTransformer) -> Self {
        self.query_transformer = Some(transformer);
        self
    }

    /// Set custom provisioner
    pub fn provisioner(mut self, provisioner: TenantProvisioner) -> Self {
        self.provisioner = Some(provisioner);
        self
    }

    /// Allow unknown tenants
    pub fn allow_unknown_tenants(mut self) -> Self {
        self.config.allow_unknown_tenants = true;
        self
    }

    /// Auto-create tenants
    pub fn auto_create_tenants(mut self) -> Self {
        self.config.auto_create_tenants = true;
        self
    }

    /// Set default tenant config
    pub fn default_tenant_config(mut self, config: TenantConfig) -> Self {
        self.config.default_config = config;
        self
    }

    /// Build the TenantManager
    pub fn build(self) -> TenantManager {
        let mut manager = TenantManager::with_config(self.config);

        if let Some(identifier) = self.identifier {
            manager.identifier = identifier;
        }

        if let Some(transformer) = self.query_transformer {
            manager.query_transformer = transformer;
        }

        if let Some(provisioner) = self.provisioner {
            manager.provisioner = provisioner;
        }

        manager
    }
}

impl Default for TenantManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_tenant_manager_creation() {
        let manager = TenantManager::new();
        assert_eq!(manager.tenant_count(), 0);
    }

    #[test]
    fn test_register_and_get_tenant() {
        let manager = TenantManager::new();

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme Corp")
            .schema_isolation("shared", "acme")
            .build();

        manager.register_tenant(config.clone());

        assert!(manager.has_tenant(&TenantId::new("acme")));
        assert_eq!(manager.tenant_count(), 1);

        let retrieved = manager.get_tenant(&TenantId::new("acme")).unwrap();
        assert_eq!(retrieved.name, "Acme Corp");
    }

    #[test]
    fn test_identify_tenant() {
        let manager = TenantManagerBuilder::new()
            .header_identification("X-Tenant-Id")
            .build();

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .database_isolation("acme_db")
            .build();

        manager.register_tenant(config);

        let ctx = RequestContext::new().with_header("X-Tenant-Id", "acme");
        let tenant = manager.identify_tenant(&ctx);

        assert!(tenant.is_some());
        assert_eq!(tenant.unwrap().as_str(), "acme");
    }

    #[test]
    fn test_unknown_tenant_rejected() {
        let manager = TenantManager::new();

        let ctx = RequestContext::new().with_header("X-Tenant-Id", "unknown");
        let tenant = manager.identify_tenant(&ctx);

        assert!(tenant.is_none());
    }

    #[test]
    fn test_auto_create_tenants() {
        let manager = TenantManagerBuilder::new()
            .header_identification("X-Tenant-Id")
            .allow_unknown_tenants()
            .auto_create_tenants()
            .build();

        let ctx = RequestContext::new().with_header("X-Tenant-Id", "new_tenant");
        let tenant = manager.identify_tenant(&ctx);

        assert!(tenant.is_some());
        assert!(manager.has_tenant(&TenantId::new("new_tenant")));
    }

    #[test]
    fn test_routing_decision() {
        let manager = TenantManager::new();

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .schema_isolation("shared_db", "acme_schema")
            .build();

        manager.register_tenant(config);

        let routing = manager.get_routing(&TenantId::new("acme")).unwrap();
        assert_eq!(routing.database, Some("shared_db".to_string()));
        assert_eq!(routing.search_path, Some("acme_schema".to_string()));
    }

    #[test]
    fn test_query_transformation() {
        let transformer = TenantQueryTransformer::new().register_table("users", "tenant_id");

        let mut manager = TenantManager::new();
        manager.query_transformer = transformer;

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .row_isolation("shared_db", "tenant_id")
            .build();

        manager.register_tenant(config);

        let result = manager.transform_query("SELECT * FROM users", &TenantId::new("acme"));

        assert!(result.transformed);
        assert!(result.query.contains("tenant_id = 'acme'"));
    }

    #[test]
    fn test_metrics_recording() {
        let manager = TenantManager::new();

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .database_isolation("acme_db")
            .build();

        manager.register_tenant(config);

        let tenant = TenantId::new("acme");
        manager.record_query(&tenant, Duration::from_millis(10), 100, 1024, 512, true);
        manager.record_query(&tenant, Duration::from_millis(20), 200, 2048, 1024, false);

        let snapshot = manager.tenant_metrics(&tenant).unwrap();
        assert_eq!(snapshot.queries, 2);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.rows_processed, 300);
    }

    #[test]
    fn test_enable_disable_tenant() {
        let manager = TenantManager::new();

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .database_isolation("acme_db")
            .build();

        manager.register_tenant(config);

        assert!(manager.is_tenant_enabled(&TenantId::new("acme")));

        manager.disable_tenant(&TenantId::new("acme"));
        assert!(!manager.is_tenant_enabled(&TenantId::new("acme")));

        manager.enable_tenant(&TenantId::new("acme"));
        assert!(manager.is_tenant_enabled(&TenantId::new("acme")));
    }

    #[test]
    fn test_tenant_manager_builder() {
        let default_config = TenantConfig::builder()
            .id("default")
            .name("Default")
            .schema_isolation("shared", "default")
            .max_connections(10)
            .build();

        let manager = TenantManagerBuilder::new()
            .header_identification("X-Org-Id")
            .allow_unknown_tenants()
            .auto_create_tenants()
            .default_tenant_config(default_config)
            .build();

        assert!(manager.is_enabled());
    }

    #[test]
    fn test_unregister_tenant() {
        let manager = TenantManager::new();

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .database_isolation("acme_db")
            .build();

        manager.register_tenant(config);
        assert!(manager.has_tenant(&TenantId::new("acme")));

        let removed = manager.unregister_tenant(&TenantId::new("acme"));
        assert!(removed.is_some());
        assert!(!manager.has_tenant(&TenantId::new("acme")));
    }
}
