//! GraphQL Gateway Configuration
//!
//! Configuration types for the GraphQL-to-SQL gateway.

use std::collections::HashMap;
use std::time::Duration;

/// GraphQL gateway configuration
#[derive(Debug, Clone)]
pub struct GraphQLConfig {
    /// Enable GraphQL gateway
    pub enabled: bool,
    /// Endpoint path (default: "/graphql")
    pub endpoint: String,
    /// Enable GraphQL Playground
    pub playground: bool,
    /// Enable introspection queries
    pub introspection: bool,
    /// Schema configuration
    pub schema: SchemaConfig,
    /// Complexity limits
    pub limits: LimitsConfig,
    /// Batching configuration
    pub batching: BatchingConfig,
    /// Caching configuration
    pub caching: CachingConfig,
    /// Table-specific configurations
    pub tables: Vec<TableConfig>,
    /// Custom relationship configurations
    pub relationships: Vec<RelationshipConfig>,
}

impl Default for GraphQLConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/graphql".to_string(),
            playground: true,
            introspection: true,
            schema: SchemaConfig::default(),
            limits: LimitsConfig::default(),
            batching: BatchingConfig::default(),
            caching: CachingConfig::default(),
            tables: Vec::new(),
            relationships: Vec::new(),
        }
    }
}

impl GraphQLConfig {
    /// Create a new configuration builder
    pub fn builder() -> GraphQLConfigBuilder {
        GraphQLConfigBuilder::new()
    }

    /// Get table configuration by table name
    pub fn get_table_config(&self, table_name: &str) -> Option<&TableConfig> {
        self.tables.iter().find(|t| t.name == table_name)
    }

    /// Check if a column should be excluded
    pub fn is_column_excluded(&self, table_name: &str, column_name: &str) -> bool {
        self.get_table_config(table_name)
            .map(|tc| tc.exclude_columns.contains(&column_name.to_string()))
            .unwrap_or(false)
    }

    /// Get the GraphQL name for a table
    pub fn get_graphql_name(&self, table_name: &str) -> String {
        self.get_table_config(table_name)
            .and_then(|tc| tc.graphql_name.clone())
            .unwrap_or_else(|| crate::graphql::to_pascal_case(table_name))
    }
}

/// Configuration builder for GraphQL gateway
#[derive(Debug, Default)]
pub struct GraphQLConfigBuilder {
    config: GraphQLConfig,
}

impl GraphQLConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: GraphQLConfig::default(),
        }
    }

    /// Set enabled status
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set endpoint path
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.endpoint = endpoint.into();
        self
    }

    /// Enable/disable playground
    pub fn playground(mut self, enabled: bool) -> Self {
        self.config.playground = enabled;
        self
    }

    /// Enable/disable introspection
    pub fn introspection(mut self, enabled: bool) -> Self {
        self.config.introspection = enabled;
        self
    }

    /// Set auto-generation of schema
    pub fn auto_generate(mut self, enabled: bool) -> Self {
        self.config.schema.auto_generate = enabled;
        self
    }

    /// Set schema refresh interval
    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        self.config.schema.refresh_interval = interval;
        self
    }

    /// Set maximum query depth
    pub fn max_depth(mut self, depth: u32) -> Self {
        self.config.limits.max_depth = depth;
        self
    }

    /// Set maximum query complexity
    pub fn max_complexity(mut self, complexity: u32) -> Self {
        self.config.limits.max_complexity = complexity;
        self
    }

    /// Set maximum aliases per query
    pub fn max_aliases(mut self, aliases: u32) -> Self {
        self.config.limits.max_aliases = aliases;
        self
    }

    /// Enable/disable batching
    pub fn batching(mut self, enabled: bool) -> Self {
        self.config.batching.enabled = enabled;
        self
    }

    /// Set batch window
    pub fn batch_window(mut self, window: Duration) -> Self {
        self.config.batching.window = window;
        self
    }

    /// Set maximum batch size
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.config.batching.max_batch_size = size;
        self
    }

    /// Enable/disable caching
    pub fn caching(mut self, enabled: bool) -> Self {
        self.config.caching.enabled = enabled;
        self
    }

    /// Set default cache TTL
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.config.caching.default_ttl = ttl;
        self
    }

    /// Add a table configuration
    pub fn table(mut self, table: TableConfig) -> Self {
        self.config.tables.push(table);
        self
    }

    /// Add a relationship configuration
    pub fn relationship(mut self, relationship: RelationshipConfig) -> Self {
        self.config.relationships.push(relationship);
        self
    }

    /// Build the configuration
    pub fn build(self) -> GraphQLConfig {
        self.config
    }
}

/// Schema generation configuration
#[derive(Debug, Clone)]
pub struct SchemaConfig {
    /// Automatically generate schema from database
    pub auto_generate: bool,
    /// Schema refresh interval
    pub refresh_interval: Duration,
    /// Include system tables
    pub include_system_tables: bool,
    /// Schema prefix filter (include only tables with this prefix)
    pub schema_prefix: Option<String>,
    /// Excluded schemas
    pub excluded_schemas: Vec<String>,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            auto_generate: true,
            refresh_interval: Duration::from_secs(300), // 5 minutes
            include_system_tables: false,
            schema_prefix: None,
            excluded_schemas: vec!["pg_catalog".to_string(), "information_schema".to_string()],
        }
    }
}

/// Query complexity limits configuration
#[derive(Debug, Clone)]
pub struct LimitsConfig {
    /// Maximum query depth
    pub max_depth: u32,
    /// Maximum query complexity
    pub max_complexity: u32,
    /// Maximum number of aliases
    pub max_aliases: u32,
    /// Maximum number of root fields
    pub max_root_fields: u32,
    /// Maximum batch size for DataLoader
    pub max_batch_size: u32,
    /// Query timeout
    pub query_timeout: Duration,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_depth: 10,
            max_complexity: 1000,
            max_aliases: 10,
            max_root_fields: 20,
            max_batch_size: 1000,
            query_timeout: Duration::from_secs(30),
        }
    }
}

/// DataLoader batching configuration
#[derive(Debug, Clone)]
pub struct BatchingConfig {
    /// Enable batching
    pub enabled: bool,
    /// Batch window duration
    pub window: Duration,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Enable request deduplication
    pub dedupe: bool,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window: Duration::from_millis(10),
            max_batch_size: 100,
            dedupe: true,
        }
    }
}

/// Response caching configuration
#[derive(Debug, Clone)]
pub struct CachingConfig {
    /// Enable caching
    pub enabled: bool,
    /// Default TTL for cached responses
    pub default_ttl: Duration,
    /// Cache parsed queries
    pub cache_parsed_queries: bool,
    /// Maximum number of cached queries
    pub max_cached_queries: usize,
    /// Per-type cache TTL overrides
    pub type_ttls: HashMap<String, Duration>,
}

impl Default for CachingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_ttl: Duration::from_secs(60),
            cache_parsed_queries: true,
            max_cached_queries: 10000,
            type_ttls: HashMap::new(),
        }
    }
}

/// Table-specific configuration
#[derive(Debug, Clone)]
pub struct TableConfig {
    /// Database table name
    pub name: String,
    /// GraphQL type name override
    pub graphql_name: Option<String>,
    /// Columns to exclude from schema
    pub exclude_columns: Vec<String>,
    /// Maximum query depth for this table
    pub max_depth: Option<u32>,
    /// Enable mutations for this table
    pub enable_mutations: bool,
    /// Primary key column(s)
    pub primary_key: Option<Vec<String>>,
    /// Custom description
    pub description: Option<String>,
    /// Authorization rules
    pub authorization: Option<AuthorizationConfig>,
}

impl TableConfig {
    /// Create a new table configuration
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            graphql_name: None,
            exclude_columns: Vec::new(),
            max_depth: None,
            enable_mutations: true,
            primary_key: None,
            description: None,
            authorization: None,
        }
    }

    /// Set GraphQL type name
    pub fn with_graphql_name(mut self, name: impl Into<String>) -> Self {
        self.graphql_name = Some(name.into());
        self
    }

    /// Exclude columns
    pub fn exclude(mut self, columns: Vec<String>) -> Self {
        self.exclude_columns = columns;
        self
    }

    /// Set maximum depth
    pub fn with_max_depth(mut self, depth: u32) -> Self {
        self.max_depth = Some(depth);
        self
    }

    /// Enable/disable mutations
    pub fn mutations(mut self, enabled: bool) -> Self {
        self.enable_mutations = enabled;
        self
    }

    /// Set primary key
    pub fn with_primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = Some(columns);
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// Table authorization configuration
#[derive(Debug, Clone)]
pub struct AuthorizationConfig {
    /// Roles that can read
    pub read_roles: Vec<String>,
    /// Roles that can create
    pub create_roles: Vec<String>,
    /// Roles that can update
    pub update_roles: Vec<String>,
    /// Roles that can delete
    pub delete_roles: Vec<String>,
    /// Row-level security filter expression
    pub row_filter: Option<String>,
}

impl Default for AuthorizationConfig {
    fn default() -> Self {
        Self {
            read_roles: Vec::new(),
            create_roles: Vec::new(),
            update_roles: Vec::new(),
            delete_roles: Vec::new(),
            row_filter: None,
        }
    }
}

/// Relationship configuration
#[derive(Debug, Clone)]
pub struct RelationshipConfig {
    /// Relationship name (used in GraphQL field)
    pub name: String,
    /// Source table
    pub from_table: String,
    /// Target table
    pub to_table: String,
    /// Source column (foreign key)
    pub from_column: String,
    /// Target column (primary key)
    pub to_column: String,
    /// Relationship type
    pub relation_type: String,
    /// Description
    pub description: Option<String>,
}

impl RelationshipConfig {
    /// Create a new relationship configuration
    pub fn new(
        name: impl Into<String>,
        from_table: impl Into<String>,
        to_table: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            from_table: from_table.into(),
            to_table: to_table.into(),
            from_column: "id".to_string(),
            to_column: "id".to_string(),
            relation_type: "many_to_one".to_string(),
            description: None,
        }
    }

    /// Set from column
    pub fn from_column(mut self, column: impl Into<String>) -> Self {
        self.from_column = column.into();
        self
    }

    /// Set to column
    pub fn to_column(mut self, column: impl Into<String>) -> Self {
        self.to_column = column.into();
        self
    }

    /// Set relation type
    pub fn relation_type(mut self, rel_type: impl Into<String>) -> Self {
        self.relation_type = rel_type.into();
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = GraphQLConfig::builder()
            .endpoint("/api/graphql")
            .playground(true)
            .introspection(true)
            .max_depth(5)
            .max_complexity(500)
            .batching(true)
            .batch_window(Duration::from_millis(20))
            .build();

        assert_eq!(config.endpoint, "/api/graphql");
        assert!(config.playground);
        assert!(config.introspection);
        assert_eq!(config.limits.max_depth, 5);
        assert_eq!(config.limits.max_complexity, 500);
        assert!(config.batching.enabled);
        assert_eq!(config.batching.window, Duration::from_millis(20));
    }

    #[test]
    fn test_table_config() {
        let table = TableConfig::new("users")
            .with_graphql_name("User")
            .exclude(vec!["password_hash".to_string()])
            .with_max_depth(3)
            .mutations(true);

        assert_eq!(table.name, "users");
        assert_eq!(table.graphql_name, Some("User".to_string()));
        assert!(table.exclude_columns.contains(&"password_hash".to_string()));
        assert_eq!(table.max_depth, Some(3));
        assert!(table.enable_mutations);
    }

    #[test]
    fn test_relationship_config() {
        let rel = RelationshipConfig::new("author", "posts", "users")
            .from_column("user_id")
            .to_column("id")
            .relation_type("many_to_one");

        assert_eq!(rel.name, "author");
        assert_eq!(rel.from_table, "posts");
        assert_eq!(rel.to_table, "users");
        assert_eq!(rel.from_column, "user_id");
        assert_eq!(rel.to_column, "id");
        assert_eq!(rel.relation_type, "many_to_one");
    }

    #[test]
    fn test_get_graphql_name() {
        let config = GraphQLConfig::builder()
            .table(TableConfig::new("users").with_graphql_name("User"))
            .build();

        assert_eq!(config.get_graphql_name("users"), "User");
        assert_eq!(config.get_graphql_name("blog_posts"), "BlogPosts");
    }

    #[test]
    fn test_is_column_excluded() {
        let config = GraphQLConfig::builder()
            .table(TableConfig::new("users").exclude(vec!["password".to_string()]))
            .build();

        assert!(config.is_column_excluded("users", "password"));
        assert!(!config.is_column_excluded("users", "email"));
        assert!(!config.is_column_excluded("posts", "title"));
    }

    #[test]
    fn test_defaults() {
        let config = GraphQLConfig::default();

        assert!(config.enabled);
        assert_eq!(config.endpoint, "/graphql");
        assert!(config.playground);
        assert!(config.introspection);
        assert!(config.schema.auto_generate);
        assert_eq!(config.limits.max_depth, 10);
        assert_eq!(config.limits.max_complexity, 1000);
        assert!(config.batching.enabled);
        assert!(config.caching.enabled);
    }
}
