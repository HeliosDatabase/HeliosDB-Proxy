//! Tenant Isolation Strategy Implementations
//!
//! This module implements the actual isolation behavior for different strategies.
//!
//! # Strategies
//!
//! - **Database**: Each tenant gets a separate database
//! - **Schema**: Each tenant gets a separate schema within a shared database
//! - **Row**: Each tenant's data is filtered by a tenant_id column
//! - **Branch**: Each tenant gets a HeliosDB-Lite branch

use std::collections::HashMap;
use std::sync::Arc;

use super::config::{IsolationStrategy, TenantConfig, TenantId};

/// Result of isolation routing decision
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// Target database name
    pub database: Option<String>,

    /// Schema search path to set
    pub search_path: Option<String>,

    /// Branch to use (HeliosDB-Lite specific)
    pub branch: Option<String>,

    /// SQL commands to execute before query
    pub pre_query_commands: Vec<String>,

    /// Whether query transformation is needed
    pub requires_transform: bool,
}

impl RoutingDecision {
    /// Create a default routing decision (no special routing)
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self {
            database: None,
            search_path: None,
            branch: None,
            pre_query_commands: Vec::new(),
            requires_transform: false,
        }
    }

    /// Create a database routing decision
    pub fn database(name: impl Into<String>) -> Self {
        Self {
            database: Some(name.into()),
            ..Self::default()
        }
    }

    /// Create a schema routing decision
    pub fn schema(database: impl Into<String>, schema: impl Into<String>) -> Self {
        let schema_name = schema.into();
        Self {
            database: Some(database.into()),
            search_path: Some(schema_name.clone()),
            pre_query_commands: vec![format!("SET search_path TO {}", schema_name)],
            ..Self::default()
        }
    }

    /// Create a branch routing decision
    pub fn branch(name: impl Into<String>) -> Self {
        Self {
            branch: Some(name.into()),
            ..Self::default()
        }
    }

    /// Create a row-level routing decision
    pub fn row_level(database: impl Into<String>) -> Self {
        Self {
            database: Some(database.into()),
            requires_transform: true,
            ..Self::default()
        }
    }
}

/// Trait for isolation strategy handlers
pub trait IsolationHandler: Send + Sync {
    /// Get routing decision for tenant
    fn get_routing(&self, tenant: &TenantId, config: &TenantConfig) -> RoutingDecision;

    /// Check if tenant can access a table
    fn can_access_table(&self, tenant: &TenantId, table: &str, config: &TenantConfig) -> bool;

    /// Get isolation strategy name
    fn strategy_name(&self) -> &'static str;
}

/// Database isolation handler
///
/// Routes each tenant to their dedicated database.
#[derive(Debug, Clone, Default)]
pub struct DatabaseIsolationHandler;

impl DatabaseIsolationHandler {
    /// Create a new database isolation handler
    pub fn new() -> Self {
        Self
    }
}

impl IsolationHandler for DatabaseIsolationHandler {
    fn get_routing(&self, _tenant: &TenantId, config: &TenantConfig) -> RoutingDecision {
        if let IsolationStrategy::Database { database_name } = &config.isolation {
            RoutingDecision::database(database_name)
        } else {
            RoutingDecision::default()
        }
    }

    fn can_access_table(&self, _tenant: &TenantId, _table: &str, config: &TenantConfig) -> bool {
        // In database isolation, tenant owns all tables in their database
        config.permissions.is_table_allowed(_table)
    }

    fn strategy_name(&self) -> &'static str {
        "database"
    }
}

/// Schema isolation handler
///
/// Routes each tenant to their schema within a shared database.
#[derive(Debug, Clone, Default)]
pub struct SchemaIsolationHandler;

impl SchemaIsolationHandler {
    /// Create a new schema isolation handler
    pub fn new() -> Self {
        Self
    }
}

impl IsolationHandler for SchemaIsolationHandler {
    fn get_routing(&self, _tenant: &TenantId, config: &TenantConfig) -> RoutingDecision {
        if let IsolationStrategy::Schema {
            database_name,
            schema_name,
        } = &config.isolation
        {
            RoutingDecision::schema(database_name, schema_name)
        } else {
            RoutingDecision::default()
        }
    }

    fn can_access_table(&self, _tenant: &TenantId, table: &str, config: &TenantConfig) -> bool {
        // Check if table reference includes schema
        if let IsolationStrategy::Schema { schema_name, .. } = &config.isolation {
            // If table is qualified (schema.table), check it matches tenant's schema
            if let Some((schema, _)) = table.split_once('.') {
                return schema.eq_ignore_ascii_case(schema_name)
                    && config.permissions.is_table_allowed(table);
            }
        }
        // Unqualified tables are allowed (will use search_path)
        config.permissions.is_table_allowed(table)
    }

    fn strategy_name(&self) -> &'static str {
        "schema"
    }
}

/// Row-level isolation handler
///
/// Transforms queries to filter by tenant column.
#[derive(Debug, Clone, Default)]
pub struct RowIsolationHandler {
    /// Tables that require tenant filtering
    tenant_tables: HashMap<String, String>,
}

impl RowIsolationHandler {
    /// Create a new row isolation handler
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table that requires tenant filtering
    pub fn register_table(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.tenant_tables.insert(table.into(), column.into());
        self
    }
}

impl IsolationHandler for RowIsolationHandler {
    fn get_routing(&self, _tenant: &TenantId, config: &TenantConfig) -> RoutingDecision {
        if let IsolationStrategy::Row { database_name, .. } = &config.isolation {
            RoutingDecision::row_level(database_name)
        } else {
            RoutingDecision::default()
        }
    }

    fn can_access_table(&self, _tenant: &TenantId, table: &str, config: &TenantConfig) -> bool {
        config.permissions.is_table_allowed(table)
    }

    fn strategy_name(&self) -> &'static str {
        "row"
    }
}

/// Branch isolation handler (HeliosDB-Lite specific)
///
/// Routes each tenant to their dedicated branch.
#[derive(Debug, Clone, Default)]
pub struct BranchIsolationHandler;

impl BranchIsolationHandler {
    /// Create a new branch isolation handler
    pub fn new() -> Self {
        Self
    }
}

impl IsolationHandler for BranchIsolationHandler {
    fn get_routing(&self, _tenant: &TenantId, config: &TenantConfig) -> RoutingDecision {
        if let IsolationStrategy::Branch { branch_name } = &config.isolation {
            RoutingDecision::branch(branch_name)
        } else {
            RoutingDecision::default()
        }
    }

    fn can_access_table(&self, _tenant: &TenantId, _table: &str, config: &TenantConfig) -> bool {
        // In branch isolation, tenant owns all tables in their branch
        config.permissions.is_table_allowed(_table)
    }

    fn strategy_name(&self) -> &'static str {
        "branch"
    }
}

/// Create an isolation handler for a strategy
pub fn create_handler(strategy: &IsolationStrategy) -> Arc<dyn IsolationHandler> {
    match strategy {
        IsolationStrategy::Database { .. } => Arc::new(DatabaseIsolationHandler::new()),
        IsolationStrategy::Schema { .. } => Arc::new(SchemaIsolationHandler::new()),
        IsolationStrategy::Row { .. } => Arc::new(RowIsolationHandler::new()),
        IsolationStrategy::Branch { .. } => Arc::new(BranchIsolationHandler::new()),
    }
}

/// Isolation router that manages routing for all tenants
pub struct IsolationRouter {
    /// Default handler for unregistered tenants
    default_handler: Arc<dyn IsolationHandler>,

    /// Per-tenant handlers (interior mutability for shared access)
    handlers: parking_lot::RwLock<HashMap<TenantId, Arc<dyn IsolationHandler>>>,
}

impl IsolationRouter {
    /// Create a new isolation router
    pub fn new() -> Self {
        Self {
            default_handler: Arc::new(SchemaIsolationHandler::new()),
            handlers: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Set the default handler
    pub fn with_default_handler(mut self, handler: Arc<dyn IsolationHandler>) -> Self {
        self.default_handler = handler;
        self
    }

    /// Register a handler for a tenant
    pub fn register_tenant(&self, tenant: TenantId, handler: Arc<dyn IsolationHandler>) {
        self.handlers.write().insert(tenant, handler);
    }

    /// Register handler based on tenant config
    pub fn register_from_config(&self, config: &TenantConfig) {
        let handler = create_handler(&config.isolation);
        self.handlers.write().insert(config.id.clone(), handler);
    }

    /// Get routing decision for tenant
    pub fn get_routing(&self, tenant: &TenantId, config: &TenantConfig) -> RoutingDecision {
        let handlers = self.handlers.read();
        handlers
            .get(tenant)
            .unwrap_or(&self.default_handler)
            .get_routing(tenant, config)
    }

    /// Check if tenant can access a table
    pub fn can_access_table(&self, tenant: &TenantId, table: &str, config: &TenantConfig) -> bool {
        let handlers = self.handlers.read();
        handlers
            .get(tenant)
            .unwrap_or(&self.default_handler)
            .can_access_table(tenant, table, config)
    }
}

impl Default for IsolationRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Tenant provisioner for setting up new tenants
pub struct TenantProvisioner {
    /// Template for database names
    database_template: String,

    /// Template for schema names
    schema_template: String,

    /// Template for branch names
    branch_template: String,
}

impl Default for TenantProvisioner {
    fn default() -> Self {
        Self {
            database_template: "tenant_{id}_db".to_string(),
            schema_template: "tenant_{id}".to_string(),
            branch_template: "tenant_{id}".to_string(),
        }
    }
}

impl TenantProvisioner {
    /// Create a new provisioner
    pub fn new() -> Self {
        Self::default()
    }

    /// Set database name template
    pub fn database_template(mut self, template: impl Into<String>) -> Self {
        self.database_template = template.into();
        self
    }

    /// Set schema name template
    pub fn schema_template(mut self, template: impl Into<String>) -> Self {
        self.schema_template = template.into();
        self
    }

    /// Set branch name template
    pub fn branch_template(mut self, template: impl Into<String>) -> Self {
        self.branch_template = template.into();
        self
    }

    /// Generate database name for tenant
    pub fn generate_database_name(&self, tenant: &TenantId) -> String {
        self.database_template.replace("{id}", &tenant.0)
    }

    /// Generate schema name for tenant
    pub fn generate_schema_name(&self, tenant: &TenantId) -> String {
        self.schema_template.replace("{id}", &tenant.0)
    }

    /// Generate branch name for tenant
    pub fn generate_branch_name(&self, tenant: &TenantId) -> String {
        self.branch_template.replace("{id}", &tenant.0)
    }

    /// Generate isolation strategy for tenant
    pub fn generate_isolation(
        &self,
        tenant: &TenantId,
        strategy_type: &str,
        shared_database: Option<&str>,
    ) -> IsolationStrategy {
        match strategy_type {
            "database" => IsolationStrategy::database(self.generate_database_name(tenant)),
            "schema" => IsolationStrategy::schema(
                shared_database.unwrap_or("shared"),
                self.generate_schema_name(tenant),
            ),
            "row" => IsolationStrategy::row(shared_database.unwrap_or("shared"), "tenant_id"),
            "branch" => IsolationStrategy::branch(self.generate_branch_name(tenant)),
            _ => IsolationStrategy::schema("public", self.generate_schema_name(tenant)),
        }
    }

    /// Generate SQL to create database isolation
    pub fn sql_create_database(&self, tenant: &TenantId) -> Vec<String> {
        let db_name = self.generate_database_name(tenant);
        vec![
            format!("CREATE DATABASE {} WITH OWNER = postgres", db_name),
            format!("GRANT ALL PRIVILEGES ON DATABASE {} TO postgres", db_name),
        ]
    }

    /// Generate SQL to create schema isolation
    pub fn sql_create_schema(&self, tenant: &TenantId, database: &str) -> Vec<String> {
        let schema_name = self.generate_schema_name(tenant);
        vec![
            format!("-- Connect to database: {}", database),
            format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name),
            format!("GRANT ALL ON SCHEMA {} TO postgres", schema_name),
        ]
    }

    /// Generate SQL to create row-level security policy
    pub fn sql_create_rls_policy(
        &self,
        tenant: &TenantId,
        table: &str,
        tenant_column: &str,
    ) -> Vec<String> {
        let policy_name = format!("tenant_{}_policy", tenant.0);
        vec![
            format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY", table),
            format!(
                "CREATE POLICY {} ON {} FOR ALL USING ({} = '{}')",
                policy_name, table, tenant_column, tenant.0
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_tenancy::config::{TenantConfig, TenantPermissions};

    fn create_test_config(id: &str, isolation: IsolationStrategy) -> TenantConfig {
        TenantConfig::builder()
            .id(id)
            .name(format!("Test {}", id))
            .isolation(isolation)
            .build()
    }

    #[test]
    fn test_routing_decision() {
        let db = RoutingDecision::database("mydb");
        assert_eq!(db.database, Some("mydb".to_string()));
        assert!(!db.requires_transform);

        let schema = RoutingDecision::schema("mydb", "myschema");
        assert_eq!(schema.database, Some("mydb".to_string()));
        assert_eq!(schema.search_path, Some("myschema".to_string()));
        assert!(!schema.pre_query_commands.is_empty());

        let branch = RoutingDecision::branch("mybranch");
        assert_eq!(branch.branch, Some("mybranch".to_string()));

        let row = RoutingDecision::row_level("mydb");
        assert!(row.requires_transform);
    }

    #[test]
    fn test_database_isolation_handler() {
        let handler = DatabaseIsolationHandler::new();
        let config = create_test_config("tenant_a", IsolationStrategy::database("tenant_a_db"));

        let routing = handler.get_routing(&TenantId::new("tenant_a"), &config);
        assert_eq!(routing.database, Some("tenant_a_db".to_string()));
        assert_eq!(handler.strategy_name(), "database");
    }

    #[test]
    fn test_schema_isolation_handler() {
        let handler = SchemaIsolationHandler::new();
        let config = create_test_config(
            "tenant_a",
            IsolationStrategy::schema("shared_db", "tenant_a"),
        );

        let routing = handler.get_routing(&TenantId::new("tenant_a"), &config);
        assert_eq!(routing.database, Some("shared_db".to_string()));
        assert_eq!(routing.search_path, Some("tenant_a".to_string()));
        assert_eq!(handler.strategy_name(), "schema");

        // Test table access
        let tenant = TenantId::new("tenant_a");
        assert!(handler.can_access_table(&tenant, "users", &config));
        assert!(handler.can_access_table(&tenant, "tenant_a.users", &config));
        assert!(!handler.can_access_table(&tenant, "tenant_b.users", &config));
    }

    #[test]
    fn test_row_isolation_handler() {
        let handler = RowIsolationHandler::new()
            .register_table("users", "tenant_id")
            .register_table("orders", "tenant_id");

        let config =
            create_test_config("tenant_a", IsolationStrategy::row("shared_db", "tenant_id"));

        let routing = handler.get_routing(&TenantId::new("tenant_a"), &config);
        assert_eq!(routing.database, Some("shared_db".to_string()));
        assert!(routing.requires_transform);
        assert_eq!(handler.strategy_name(), "row");
    }

    #[test]
    fn test_branch_isolation_handler() {
        let handler = BranchIsolationHandler::new();
        let config = create_test_config("tenant_a", IsolationStrategy::branch("tenant_a_branch"));

        let routing = handler.get_routing(&TenantId::new("tenant_a"), &config);
        assert_eq!(routing.branch, Some("tenant_a_branch".to_string()));
        assert_eq!(handler.strategy_name(), "branch");
    }

    #[test]
    fn test_isolation_router() {
        let router = IsolationRouter::new();

        let config_a = create_test_config("tenant_a", IsolationStrategy::database("tenant_a_db"));
        let config_b =
            create_test_config("tenant_b", IsolationStrategy::schema("shared", "tenant_b"));

        router.register_from_config(&config_a);
        router.register_from_config(&config_b);

        let routing_a = router.get_routing(&TenantId::new("tenant_a"), &config_a);
        assert_eq!(routing_a.database, Some("tenant_a_db".to_string()));

        let routing_b = router.get_routing(&TenantId::new("tenant_b"), &config_b);
        assert_eq!(routing_b.database, Some("shared".to_string()));
        assert_eq!(routing_b.search_path, Some("tenant_b".to_string()));
    }

    #[test]
    fn test_tenant_provisioner() {
        let provisioner = TenantProvisioner::new();
        let tenant = TenantId::new("acme");

        assert_eq!(
            provisioner.generate_database_name(&tenant),
            "tenant_acme_db"
        );
        assert_eq!(provisioner.generate_schema_name(&tenant), "tenant_acme");
        assert_eq!(provisioner.generate_branch_name(&tenant), "tenant_acme");

        let isolation = provisioner.generate_isolation(&tenant, "schema", Some("shared_db"));
        assert!(matches!(
            isolation,
            IsolationStrategy::Schema { database_name, schema_name }
            if database_name == "shared_db" && schema_name == "tenant_acme"
        ));
    }

    #[test]
    fn test_provisioner_sql_generation() {
        let provisioner = TenantProvisioner::new();
        let tenant = TenantId::new("acme");

        let db_sql = provisioner.sql_create_database(&tenant);
        assert!(!db_sql.is_empty());
        assert!(db_sql[0].contains("CREATE DATABASE"));

        let schema_sql = provisioner.sql_create_schema(&tenant, "shared");
        assert!(schema_sql.iter().any(|s| s.contains("CREATE SCHEMA")));

        let rls_sql = provisioner.sql_create_rls_policy(&tenant, "users", "tenant_id");
        assert!(rls_sql.iter().any(|s| s.contains("ROW LEVEL SECURITY")));
    }
}
