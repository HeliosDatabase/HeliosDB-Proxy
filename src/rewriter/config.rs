//! Query Rewriter Configuration
//!
//! Configuration types for the query rewriting system.

use super::rules::RewriteRule;
use std::time::Duration;

/// Query rewriter configuration
#[derive(Debug, Clone)]
pub struct RewriterConfig {
    /// Enable query rewriting
    pub enabled: bool,

    /// Log rewrite operations
    pub log_rewrites: bool,

    /// Log rewrite errors
    pub log_errors: bool,

    /// Rewrite rules
    pub rules: Vec<RewriteRule>,

    /// Automatically expand SELECT * to column list
    pub expand_select_star: bool,

    /// Add default LIMIT to queries without one
    pub add_default_limit: bool,

    /// Default LIMIT value
    pub default_limit: u32,

    /// Maximum query length to process
    pub max_query_length: usize,

    /// Cache rewritten queries by fingerprint
    pub cache_enabled: bool,

    /// Cache TTL
    pub cache_ttl: Duration,

    /// Maximum cache entries
    pub max_cache_entries: usize,

    /// Agent query safety rules
    pub agent_safety: AgentSafetyConfig,
}

impl Default for RewriterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_rewrites: false,
            log_errors: true,
            rules: Vec::new(),
            expand_select_star: false,
            add_default_limit: false,
            default_limit: 1000,
            max_query_length: 1_000_000,
            cache_enabled: true,
            cache_ttl: Duration::from_secs(300),
            max_cache_entries: 10000,
            agent_safety: AgentSafetyConfig::default(),
        }
    }
}

impl RewriterConfig {
    /// Create a new enabled config
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a builder
    pub fn builder() -> RewriterConfigBuilder {
        RewriterConfigBuilder::new()
    }
}

/// Builder for RewriterConfig
#[derive(Default)]
pub struct RewriterConfigBuilder {
    config: RewriterConfig,
}

impl RewriterConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: RewriterConfig {
                enabled: true,
                ..Default::default()
            },
        }
    }

    /// Enable/disable rewriting
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Log rewrites
    pub fn log_rewrites(mut self, log: bool) -> Self {
        self.config.log_rewrites = log;
        self
    }

    /// Log errors
    pub fn log_errors(mut self, log: bool) -> Self {
        self.config.log_errors = log;
        self
    }

    /// Add a rule
    pub fn rule(mut self, rule: RewriteRule) -> Self {
        self.config.rules.push(rule);
        self
    }

    /// Add multiple rules
    pub fn rules(mut self, rules: Vec<RewriteRule>) -> Self {
        self.config.rules.extend(rules);
        self
    }

    /// Enable SELECT * expansion
    pub fn expand_select_star(mut self, enabled: bool) -> Self {
        self.config.expand_select_star = enabled;
        self
    }

    /// Enable default LIMIT
    pub fn add_default_limit(mut self, enabled: bool) -> Self {
        self.config.add_default_limit = enabled;
        self
    }

    /// Set default LIMIT value
    pub fn default_limit(mut self, limit: u32) -> Self {
        self.config.default_limit = limit;
        self
    }

    /// Set max query length
    pub fn max_query_length(mut self, length: usize) -> Self {
        self.config.max_query_length = length;
        self
    }

    /// Enable caching
    pub fn cache_enabled(mut self, enabled: bool) -> Self {
        self.config.cache_enabled = enabled;
        self
    }

    /// Set cache TTL
    pub fn cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.cache_ttl = ttl;
        self
    }

    /// Set agent safety config
    pub fn agent_safety(mut self, config: AgentSafetyConfig) -> Self {
        self.config.agent_safety = config;
        self
    }

    /// Build the config
    pub fn build(self) -> RewriterConfig {
        self.config
    }
}

/// Agent query safety configuration
#[derive(Debug, Clone)]
pub struct AgentSafetyConfig {
    /// Enable agent safety rules
    pub enabled: bool,

    /// Maximum rows for agent queries
    pub max_rows: u32,

    /// Maximum query timeout for agents
    pub max_timeout: Duration,

    /// Forbidden tables for agents
    pub forbidden_tables: Vec<String>,

    /// Required WHERE clause tables
    pub require_where_tables: Vec<String>,

    /// Block DDL for agents
    pub block_ddl: bool,

    /// Block admin commands for agents
    pub block_admin: bool,
}

impl Default for AgentSafetyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_rows: 10000,
            max_timeout: Duration::from_secs(30),
            forbidden_tables: vec![
                "pg_catalog.*".to_string(),
                "information_schema.*".to_string(),
                "system.*".to_string(),
                "secrets".to_string(),
                "credentials".to_string(),
            ],
            require_where_tables: Vec::new(),
            block_ddl: true,
            block_admin: true,
        }
    }
}

impl AgentSafetyConfig {
    /// Create a permissive config (for trusted agents)
    pub fn permissive() -> Self {
        Self {
            enabled: true,
            max_rows: 100000,
            max_timeout: Duration::from_secs(300),
            forbidden_tables: Vec::new(),
            require_where_tables: Vec::new(),
            block_ddl: false,
            block_admin: false,
        }
    }

    /// Create a restrictive config (for untrusted agents)
    pub fn restrictive() -> Self {
        Self {
            enabled: true,
            max_rows: 1000,
            max_timeout: Duration::from_secs(10),
            forbidden_tables: vec![
                "pg_catalog.*".to_string(),
                "information_schema.*".to_string(),
                "system.*".to_string(),
                "secrets".to_string(),
                "credentials".to_string(),
                "users".to_string(),
                "accounts".to_string(),
            ],
            require_where_tables: vec!["*".to_string()],
            block_ddl: true,
            block_admin: true,
        }
    }

    /// Check if a table is forbidden
    pub fn is_forbidden(&self, table: &str) -> bool {
        for pattern in &self.forbidden_tables {
            if pattern.ends_with("*") {
                let prefix = &pattern[..pattern.len() - 1];
                if table.starts_with(prefix) {
                    return true;
                }
            } else if pattern == table {
                return true;
            }
        }
        false
    }
}

/// Built-in rule templates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinRule {
    /// Add index hints
    AddIndexHints,

    /// Expand SELECT *
    ExpandSelectStar,

    /// Add default LIMIT
    AddDefaultLimit,

    /// Add tenant filter
    AddTenantFilter,

    /// Route to specific branch
    RouteToBranch,

    /// Agent safety limits
    AgentSafety,
}

impl BuiltinRule {
    /// Get rule ID
    pub fn id(&self) -> &'static str {
        match self {
            Self::AddIndexHints => "builtin:add_index_hints",
            Self::ExpandSelectStar => "builtin:expand_select_star",
            Self::AddDefaultLimit => "builtin:add_default_limit",
            Self::AddTenantFilter => "builtin:add_tenant_filter",
            Self::RouteToBranch => "builtin:route_to_branch",
            Self::AgentSafety => "builtin:agent_safety",
        }
    }

    /// Get rule description
    pub fn description(&self) -> &'static str {
        match self {
            Self::AddIndexHints => "Add index hints based on query patterns",
            Self::ExpandSelectStar => "Expand SELECT * to column list",
            Self::AddDefaultLimit => "Add LIMIT to queries without one",
            Self::AddTenantFilter => "Add tenant ID filter for multi-tenancy",
            Self::RouteToBranch => "Add branch routing hints",
            Self::AgentSafety => "Apply safety limits for AI agent queries",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RewriterConfig::default();
        assert!(!config.enabled);
        assert!(config.rules.is_empty());
    }

    #[test]
    fn test_config_builder() {
        let config = RewriterConfig::builder()
            .enabled(true)
            .log_rewrites(true)
            .add_default_limit(true)
            .default_limit(500)
            .build();

        assert!(config.enabled);
        assert!(config.log_rewrites);
        assert!(config.add_default_limit);
        assert_eq!(config.default_limit, 500);
    }

    #[test]
    fn test_agent_safety_forbidden_tables() {
        let config = AgentSafetyConfig::default();

        assert!(config.is_forbidden("pg_catalog.pg_tables"));
        assert!(config.is_forbidden("secrets"));
        assert!(!config.is_forbidden("users"));
    }

    #[test]
    fn test_restrictive_agent_config() {
        let config = AgentSafetyConfig::restrictive();

        assert!(config.is_forbidden("users"));
        assert!(config.block_ddl);
        assert_eq!(config.max_rows, 1000);
    }
}
