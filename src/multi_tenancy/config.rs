//! Multi-Tenancy Configuration Types
//!
//! This module provides configuration structures for multi-tenant proxy operation.
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb::proxy::multi_tenancy::{
//!     TenantConfig, IsolationStrategy, TenantPoolConfig, TenantRateLimits,
//! };
//!
//! let config = TenantConfig::builder()
//!     .id("tenant_a")
//!     .name("Acme Corp")
//!     .isolation(IsolationStrategy::Schema {
//!         database_name: "shared_db".to_string(),
//!         schema_name: "tenant_a".to_string(),
//!     })
//!     .max_connections(50)
//!     .qps_limit(1000)
//!     .build();
//! ```

use std::collections::HashMap;
use std::time::Duration;

/// Unique tenant identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(pub String);

impl TenantId {
    /// Create a new tenant ID
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the tenant ID as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for TenantId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for TenantId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Tenant isolation strategy
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationStrategy {
    /// Separate database per tenant
    Database {
        /// Database name for this tenant
        database_name: String,
    },

    /// Separate schema per tenant (same database)
    Schema {
        /// Database name containing the schema
        database_name: String,
        /// Schema name for this tenant
        schema_name: String,
    },

    /// Row-level security (same tables)
    Row {
        /// Database name
        database_name: String,
        /// Column name containing tenant ID
        tenant_column: String,
    },

    /// Branch per tenant (HeliosDB-Lite specific)
    Branch {
        /// Branch name for this tenant
        branch_name: String,
    },
}

impl IsolationStrategy {
    /// Create database isolation strategy
    pub fn database(name: impl Into<String>) -> Self {
        Self::Database {
            database_name: name.into(),
        }
    }

    /// Create schema isolation strategy
    pub fn schema(database: impl Into<String>, schema: impl Into<String>) -> Self {
        Self::Schema {
            database_name: database.into(),
            schema_name: schema.into(),
        }
    }

    /// Create row-level isolation strategy
    pub fn row(database: impl Into<String>, column: impl Into<String>) -> Self {
        Self::Row {
            database_name: database.into(),
            tenant_column: column.into(),
        }
    }

    /// Create branch isolation strategy
    pub fn branch(name: impl Into<String>) -> Self {
        Self::Branch {
            branch_name: name.into(),
        }
    }

    /// Get the database name for this strategy
    pub fn database_name(&self) -> Option<&str> {
        match self {
            Self::Database { database_name } => Some(database_name),
            Self::Schema { database_name, .. } => Some(database_name),
            Self::Row { database_name, .. } => Some(database_name),
            Self::Branch { .. } => None,
        }
    }

    /// Get the schema name if using schema isolation
    pub fn schema_name(&self) -> Option<&str> {
        match self {
            Self::Schema { schema_name, .. } => Some(schema_name),
            _ => None,
        }
    }

    /// Get the tenant column if using row-level isolation
    pub fn tenant_column(&self) -> Option<&str> {
        match self {
            Self::Row { tenant_column, .. } => Some(tenant_column),
            _ => None,
        }
    }

    /// Get the branch name if using branch isolation
    pub fn branch_name(&self) -> Option<&str> {
        match self {
            Self::Branch { branch_name } => Some(branch_name),
            _ => None,
        }
    }

    /// Check if this strategy requires query transformation
    pub fn requires_query_transform(&self) -> bool {
        matches!(self, Self::Row { .. })
    }

    /// Check if this strategy requires connection routing
    pub fn requires_connection_routing(&self) -> bool {
        matches!(self, Self::Database { .. } | Self::Branch { .. })
    }

    /// Get display name for this strategy
    pub fn strategy_name(&self) -> &'static str {
        match self {
            Self::Database { .. } => "database",
            Self::Schema { .. } => "schema",
            Self::Row { .. } => "row",
            Self::Branch { .. } => "branch",
        }
    }
}

/// Tenant-specific rate limits
#[derive(Debug, Clone)]
pub struct TenantRateLimits {
    /// Maximum queries per second
    pub qps_limit: u32,

    /// Maximum concurrent connections
    pub max_connections: u32,

    /// Maximum query duration before kill
    pub max_query_duration: Duration,

    /// Maximum result size (bytes)
    pub max_result_size: u64,

    /// Maximum rows per query
    pub max_rows_per_query: u64,

    /// Burst allowance (multiplier over qps_limit for short bursts)
    pub burst_multiplier: f32,
}

impl Default for TenantRateLimits {
    fn default() -> Self {
        Self {
            qps_limit: 100,
            max_connections: 10,
            max_query_duration: Duration::from_secs(60),
            max_result_size: 100 * 1024 * 1024, // 100MB
            max_rows_per_query: 100_000,
            burst_multiplier: 2.0,
        }
    }
}

impl TenantRateLimits {
    /// Create new rate limits with QPS limit
    pub fn with_qps(qps: u32) -> Self {
        Self {
            qps_limit: qps,
            ..Default::default()
        }
    }

    /// Set the QPS limit
    pub fn qps_limit(mut self, limit: u32) -> Self {
        self.qps_limit = limit;
        self
    }

    /// Set the max connections
    pub fn max_connections(mut self, limit: u32) -> Self {
        self.max_connections = limit;
        self
    }

    /// Set the max query duration
    pub fn max_query_duration(mut self, duration: Duration) -> Self {
        self.max_query_duration = duration;
        self
    }

    /// Set the burst multiplier
    pub fn burst_multiplier(mut self, multiplier: f32) -> Self {
        self.burst_multiplier = multiplier;
        self
    }
}

/// Tenant-specific connection pool configuration
#[derive(Debug, Clone)]
pub struct TenantPoolConfig {
    /// Maximum connections in pool
    pub max_connections: u32,

    /// Minimum idle connections
    pub min_idle: u32,

    /// Idle connection timeout
    pub idle_timeout: Duration,

    /// Maximum connection lifetime
    pub max_lifetime: Duration,

    /// Connection acquire timeout
    pub acquire_timeout: Duration,

    /// Whether this tenant uses dedicated pool
    pub dedicated_pool: bool,
}

impl Default for TenantPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_idle: 1,
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(3600),
            acquire_timeout: Duration::from_secs(5),
            dedicated_pool: false,
        }
    }
}

impl TenantPoolConfig {
    /// Create pool config with max connections
    pub fn with_max_connections(max: u32) -> Self {
        Self {
            max_connections: max,
            ..Default::default()
        }
    }

    /// Set dedicated pool flag
    pub fn dedicated(mut self) -> Self {
        self.dedicated_pool = true;
        self
    }

    /// Set min idle connections
    pub fn min_idle(mut self, min: u32) -> Self {
        self.min_idle = min;
        self
    }

    /// Set idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }
}

/// Tenant permissions and restrictions
#[derive(Debug, Clone)]
pub struct TenantPermissions {
    /// Allowed SQL operations (SELECT, INSERT, UPDATE, DELETE, DDL)
    pub allowed_operations: Vec<String>,

    /// Blocked tables (cannot access)
    pub blocked_tables: Vec<String>,

    /// Read-only mode
    pub read_only: bool,

    /// Can execute DDL statements
    pub allow_ddl: bool,

    /// Can execute EXPLAIN/ANALYZE
    pub allow_explain: bool,

    /// Can access system tables
    pub allow_system_access: bool,

    /// Maximum tables per query (for complexity limiting)
    pub max_tables_per_query: u32,
}

impl Default for TenantPermissions {
    fn default() -> Self {
        Self {
            allowed_operations: vec![
                "SELECT".to_string(),
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
            ],
            blocked_tables: vec![],
            read_only: false,
            allow_ddl: false,
            allow_explain: true,
            allow_system_access: false,
            max_tables_per_query: 10,
        }
    }
}

impl TenantPermissions {
    /// Create read-only permissions
    pub fn read_only() -> Self {
        Self {
            allowed_operations: vec!["SELECT".to_string()],
            read_only: true,
            ..Default::default()
        }
    }

    /// Create full access permissions
    pub fn full_access() -> Self {
        Self {
            allowed_operations: vec![
                "SELECT".to_string(),
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
                "CREATE".to_string(),
                "ALTER".to_string(),
                "DROP".to_string(),
            ],
            allow_ddl: true,
            allow_system_access: true,
            ..Default::default()
        }
    }

    /// Check if operation is allowed
    pub fn is_operation_allowed(&self, operation: &str) -> bool {
        self.allowed_operations
            .iter()
            .any(|op| op.eq_ignore_ascii_case(operation))
    }

    /// Check if table access is allowed
    pub fn is_table_allowed(&self, table: &str) -> bool {
        !self
            .blocked_tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(table))
    }
}

/// AI workload configuration per tenant
#[derive(Debug, Clone)]
pub struct TenantAiConfig {
    /// Knowledge base identifier
    pub knowledge_base: Option<String>,

    /// Embedding model to use
    pub embedding_model: String,

    /// Maximum retrieval results
    pub retrieval_limit: u32,

    /// Token budget per day
    pub daily_token_budget: Option<u64>,

    /// Enable agent workspace
    pub agent_workspace_enabled: bool,

    /// Maximum concurrent agents
    pub max_concurrent_agents: u32,
}

impl Default for TenantAiConfig {
    fn default() -> Self {
        Self {
            knowledge_base: None,
            embedding_model: "default".to_string(),
            retrieval_limit: 10,
            daily_token_budget: None,
            agent_workspace_enabled: true,
            max_concurrent_agents: 5,
        }
    }
}

/// Full tenant configuration
#[derive(Debug, Clone)]
pub struct TenantConfig {
    /// Tenant identifier
    pub id: TenantId,

    /// Display name
    pub name: String,

    /// Isolation strategy
    pub isolation: IsolationStrategy,

    /// Rate limits
    pub rate_limits: TenantRateLimits,

    /// Connection pool settings
    pub pool: TenantPoolConfig,

    /// Permissions and restrictions
    pub permissions: TenantPermissions,

    /// AI workload configuration
    pub ai_config: TenantAiConfig,

    /// Custom metadata
    pub metadata: HashMap<String, String>,

    /// Whether tenant is enabled
    pub enabled: bool,

    /// Tenant creation timestamp
    pub created_at: std::time::SystemTime,
}

impl TenantConfig {
    /// Create a new tenant config builder
    pub fn builder() -> TenantConfigBuilder {
        TenantConfigBuilder::new()
    }

    /// Create with minimal configuration
    pub fn new(id: impl Into<TenantId>, isolation: IsolationStrategy) -> Self {
        Self {
            id: id.into(),
            name: String::new(),
            isolation,
            rate_limits: TenantRateLimits::default(),
            pool: TenantPoolConfig::default(),
            permissions: TenantPermissions::default(),
            ai_config: TenantAiConfig::default(),
            metadata: HashMap::new(),
            enabled: true,
            created_at: std::time::SystemTime::now(),
        }
    }

    /// Check if tenant is in a healthy state
    pub fn is_healthy(&self) -> bool {
        self.enabled
    }

    /// Get effective max connections considering pool config
    pub fn effective_max_connections(&self) -> u32 {
        self.pool
            .max_connections
            .min(self.rate_limits.max_connections)
    }
}

/// Builder for TenantConfig
#[derive(Debug, Default)]
pub struct TenantConfigBuilder {
    id: Option<TenantId>,
    name: Option<String>,
    isolation: Option<IsolationStrategy>,
    rate_limits: Option<TenantRateLimits>,
    pool: Option<TenantPoolConfig>,
    permissions: Option<TenantPermissions>,
    ai_config: Option<TenantAiConfig>,
    metadata: HashMap<String, String>,
    enabled: bool,
}

impl TenantConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Set tenant ID
    pub fn id(mut self, id: impl Into<TenantId>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set tenant name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set isolation strategy
    pub fn isolation(mut self, strategy: IsolationStrategy) -> Self {
        self.isolation = Some(strategy);
        self
    }

    /// Set database isolation
    pub fn database_isolation(self, database: impl Into<String>) -> Self {
        self.isolation(IsolationStrategy::database(database))
    }

    /// Set schema isolation
    pub fn schema_isolation(self, database: impl Into<String>, schema: impl Into<String>) -> Self {
        self.isolation(IsolationStrategy::schema(database, schema))
    }

    /// Set row-level isolation
    pub fn row_isolation(self, database: impl Into<String>, column: impl Into<String>) -> Self {
        self.isolation(IsolationStrategy::row(database, column))
    }

    /// Set branch isolation
    pub fn branch_isolation(self, branch: impl Into<String>) -> Self {
        self.isolation(IsolationStrategy::branch(branch))
    }

    /// Set rate limits
    pub fn rate_limits(mut self, limits: TenantRateLimits) -> Self {
        self.rate_limits = Some(limits);
        self
    }

    /// Set QPS limit
    pub fn qps_limit(mut self, limit: u32) -> Self {
        let mut limits = self.rate_limits.take().unwrap_or_default();
        limits.qps_limit = limit;
        self.rate_limits = Some(limits);
        self
    }

    /// Set max connections
    pub fn max_connections(mut self, max: u32) -> Self {
        let mut pool = self.pool.take().unwrap_or_default();
        pool.max_connections = max;
        self.pool = Some(pool);

        let mut limits = self.rate_limits.take().unwrap_or_default();
        limits.max_connections = max;
        self.rate_limits = Some(limits);
        self
    }

    /// Set pool configuration
    pub fn pool(mut self, config: TenantPoolConfig) -> Self {
        self.pool = Some(config);
        self
    }

    /// Set permissions
    pub fn permissions(mut self, perms: TenantPermissions) -> Self {
        self.permissions = Some(perms);
        self
    }

    /// Set read-only mode
    pub fn read_only(self) -> Self {
        self.permissions(TenantPermissions::read_only())
    }

    /// Set AI configuration
    pub fn ai_config(mut self, config: TenantAiConfig) -> Self {
        self.ai_config = Some(config);
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set enabled state
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Build the TenantConfig
    pub fn build(self) -> TenantConfig {
        TenantConfig {
            id: self.id.expect("tenant id is required"),
            name: self.name.unwrap_or_default(),
            isolation: self.isolation.expect("isolation strategy is required"),
            rate_limits: self.rate_limits.unwrap_or_default(),
            pool: self.pool.unwrap_or_default(),
            permissions: self.permissions.unwrap_or_default(),
            ai_config: self.ai_config.unwrap_or_default(),
            metadata: self.metadata,
            enabled: self.enabled,
            created_at: std::time::SystemTime::now(),
        }
    }
}

/// How tenants are identified from requests
#[derive(Debug, Clone)]
pub enum IdentificationMethod {
    /// Extract from HTTP header
    Header {
        /// Header name (e.g., "X-Tenant-Id")
        header_name: String,
    },

    /// Extract from username prefix
    UsernamePrefix {
        /// Separator character (e.g., '.')
        separator: char,
    },

    /// Extract from JWT claim
    JwtClaim {
        /// Claim name (e.g., "tenant_id")
        claim_name: String,
        /// JWT issuer for validation
        issuer: Option<String>,
    },

    /// Extract from database name
    DatabaseName,

    /// SQL context variable
    SqlContext {
        /// Variable name (e.g., "helios.tenant_id")
        variable_name: String,
    },
}

impl Default for IdentificationMethod {
    fn default() -> Self {
        Self::Header {
            header_name: "X-Tenant-Id".to_string(),
        }
    }
}

impl IdentificationMethod {
    /// Create header identification
    pub fn header(name: impl Into<String>) -> Self {
        Self::Header {
            header_name: name.into(),
        }
    }

    /// Create username prefix identification
    pub fn username_prefix(separator: char) -> Self {
        Self::UsernamePrefix { separator }
    }

    /// Create JWT claim identification
    pub fn jwt_claim(claim: impl Into<String>) -> Self {
        Self::JwtClaim {
            claim_name: claim.into(),
            issuer: None,
        }
    }

    /// Create database name identification
    pub fn database_name() -> Self {
        Self::DatabaseName
    }

    /// Create SQL context identification
    pub fn sql_context(variable: impl Into<String>) -> Self {
        Self::SqlContext {
            variable_name: variable.into(),
        }
    }
}

/// Global multi-tenancy configuration
#[derive(Debug, Clone)]
pub struct MultiTenancyConfig {
    /// Whether multi-tenancy is enabled
    pub enabled: bool,

    /// How to identify tenants
    pub identification: IdentificationMethod,

    /// Default tenant configuration
    pub default_config: TenantConfig,

    /// Whether to allow unknown tenants
    pub allow_unknown_tenants: bool,

    /// Whether to create tenants on-demand
    pub auto_create_tenants: bool,

    /// Maximum tenants allowed
    pub max_tenants: u32,

    /// Enable cross-tenant analytics for admins
    pub cross_tenant_analytics: bool,

    /// Admin user pattern (for cross-tenant access)
    pub admin_user_pattern: Option<String>,
}

impl Default for MultiTenancyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            identification: IdentificationMethod::default(),
            default_config: TenantConfig::new(
                TenantId::new("default"),
                IsolationStrategy::schema("public", "public"),
            ),
            allow_unknown_tenants: false,
            auto_create_tenants: false,
            max_tenants: 1000,
            cross_tenant_analytics: false,
            admin_user_pattern: None,
        }
    }
}

impl MultiTenancyConfig {
    /// Create enabled multi-tenancy config
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Set identification method
    pub fn with_identification(mut self, method: IdentificationMethod) -> Self {
        self.identification = method;
        self
    }

    /// Set default tenant config
    pub fn with_default_config(mut self, config: TenantConfig) -> Self {
        self.default_config = config;
        self
    }

    /// Allow unknown tenants
    pub fn allow_unknown(mut self) -> Self {
        self.allow_unknown_tenants = true;
        self
    }

    /// Enable auto-creation of tenants
    pub fn auto_create(mut self) -> Self {
        self.auto_create_tenants = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id() {
        let id = TenantId::new("test_tenant");
        assert_eq!(id.as_str(), "test_tenant");
        assert_eq!(id.to_string(), "test_tenant");

        let id2: TenantId = "another".into();
        assert_eq!(id2.as_str(), "another");
    }

    #[test]
    fn test_isolation_strategy() {
        let db = IsolationStrategy::database("mydb");
        assert_eq!(db.database_name(), Some("mydb"));
        assert_eq!(db.strategy_name(), "database");
        assert!(db.requires_connection_routing());
        assert!(!db.requires_query_transform());

        let schema = IsolationStrategy::schema("mydb", "myschema");
        assert_eq!(schema.database_name(), Some("mydb"));
        assert_eq!(schema.schema_name(), Some("myschema"));
        assert_eq!(schema.strategy_name(), "schema");

        let row = IsolationStrategy::row("mydb", "tenant_id");
        assert_eq!(row.tenant_column(), Some("tenant_id"));
        assert!(row.requires_query_transform());

        let branch = IsolationStrategy::branch("tenant_branch");
        assert_eq!(branch.branch_name(), Some("tenant_branch"));
        assert!(branch.requires_connection_routing());
    }

    #[test]
    fn test_tenant_config_builder() {
        let config = TenantConfig::builder()
            .id("tenant_a")
            .name("Acme Corp")
            .schema_isolation("shared_db", "tenant_a")
            .max_connections(50)
            .qps_limit(1000)
            .metadata("tier", "enterprise")
            .build();

        assert_eq!(config.id.as_str(), "tenant_a");
        assert_eq!(config.name, "Acme Corp");
        assert_eq!(config.pool.max_connections, 50);
        assert_eq!(config.rate_limits.qps_limit, 1000);
        assert_eq!(config.metadata.get("tier"), Some(&"enterprise".to_string()));
    }

    #[test]
    fn test_tenant_permissions() {
        let default = TenantPermissions::default();
        assert!(default.is_operation_allowed("SELECT"));
        assert!(default.is_operation_allowed("select"));
        assert!(!default.is_operation_allowed("CREATE"));
        assert!(!default.allow_ddl);

        let read_only = TenantPermissions::read_only();
        assert!(read_only.is_operation_allowed("SELECT"));
        assert!(!read_only.is_operation_allowed("INSERT"));
        assert!(read_only.read_only);

        let full = TenantPermissions::full_access();
        assert!(full.is_operation_allowed("CREATE"));
        assert!(full.allow_ddl);
    }

    #[test]
    fn test_identification_methods() {
        let header = IdentificationMethod::header("X-Tenant-Id");
        assert!(
            matches!(header, IdentificationMethod::Header { header_name } if header_name == "X-Tenant-Id")
        );

        let prefix = IdentificationMethod::username_prefix('.');
        assert!(matches!(
            prefix,
            IdentificationMethod::UsernamePrefix { separator: '.' }
        ));

        let jwt = IdentificationMethod::jwt_claim("tenant_id");
        assert!(
            matches!(jwt, IdentificationMethod::JwtClaim { claim_name, .. } if claim_name == "tenant_id")
        );
    }

    #[test]
    fn test_multi_tenancy_config() {
        let config = MultiTenancyConfig::enabled()
            .with_identification(IdentificationMethod::header("X-Org-Id"))
            .allow_unknown()
            .auto_create();

        assert!(config.enabled);
        assert!(config.allow_unknown_tenants);
        assert!(config.auto_create_tenants);
    }
}
