//! Routing Configuration
//!
//! Configuration for query routing hints and policies.

use super::{ConsistencyLevel, RouteTarget};
use std::collections::HashMap;
use std::time::Duration;

/// Routing configuration
#[derive(Debug, Clone)]
pub struct RoutingConfig {
    /// Default routing policy
    pub default: DefaultPolicy,
    /// Hint configuration
    pub hints: HintConfig,
    /// Consistency level definitions
    pub consistency: HashMap<ConsistencyLevel, ConsistencyConfig>,
    /// Route aliases (e.g., "vector" -> ["standby-vector-1", "standby-vector-2"])
    pub aliases: HashMap<String, Vec<String>>,
    /// Per-route configuration
    pub routes: HashMap<RouteTarget, RouteConfig>,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        let mut consistency = HashMap::new();
        consistency.insert(
            ConsistencyLevel::Strong,
            ConsistencyConfig {
                allowed_nodes: vec!["primary".to_string(), "standby-sync".to_string()],
                max_lag_ms: 0,
            },
        );
        consistency.insert(
            ConsistencyLevel::Bounded,
            ConsistencyConfig {
                allowed_nodes: vec![
                    "primary".to_string(),
                    "standby-sync".to_string(),
                    "standby-semisync".to_string(),
                ],
                max_lag_ms: 1000,
            },
        );
        consistency.insert(
            ConsistencyLevel::Eventual,
            ConsistencyConfig {
                allowed_nodes: vec!["*".to_string()],
                max_lag_ms: u64::MAX,
            },
        );

        Self {
            default: DefaultPolicy::default(),
            hints: HintConfig::default(),
            consistency,
            aliases: HashMap::new(),
            routes: HashMap::new(),
        }
    }
}

impl RoutingConfig {
    /// Create a new routing configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a route alias
    pub fn add_alias(&mut self, alias: &str, nodes: Vec<String>) {
        self.aliases.insert(alias.to_string(), nodes);
    }

    /// Get nodes for an alias
    pub fn resolve_alias(&self, alias: &str) -> Option<&Vec<String>> {
        self.aliases.get(alias)
    }

    /// Get consistency config for level
    pub fn get_consistency_config(&self, level: ConsistencyLevel) -> Option<&ConsistencyConfig> {
        self.consistency.get(&level)
    }

    /// Check if a hint is allowed
    pub fn is_hint_allowed(&self, hint_name: &str) -> bool {
        if !self.hints.enabled {
            return false;
        }

        match hint_name {
            "node" => self.hints.allow_node_hints,
            "route" => true,
            "primary" => self.hints.allow_primary_reads,
            _ => true,
        }
    }
}

/// Default routing policy
#[derive(Debug, Clone)]
pub struct DefaultPolicy {
    /// Default target for read queries
    pub read_target: RouteTarget,
    /// Default target for write queries
    pub write_target: RouteTarget,
    /// Default consistency level
    pub consistency: ConsistencyLevel,
    /// Default query timeout
    pub timeout: Duration,
    /// Enable read/write auto-detection
    pub auto_detect_writes: bool,
}

impl Default for DefaultPolicy {
    fn default() -> Self {
        Self {
            read_target: RouteTarget::Standby,
            write_target: RouteTarget::Primary,
            consistency: ConsistencyLevel::Eventual,
            timeout: Duration::from_secs(30),
            auto_detect_writes: true,
        }
    }
}

/// Hint configuration
#[derive(Debug, Clone)]
pub struct HintConfig {
    /// Enable routing hints
    pub enabled: bool,
    /// Allow routing to specific nodes by name
    pub allow_node_hints: bool,
    /// Allow routing reads to primary
    pub allow_primary_reads: bool,
    /// Require authentication for hints
    pub require_auth: bool,
    /// Strip hints before sending to backend
    pub strip_hints: bool,
    /// Log routing decisions
    pub log_decisions: bool,
    /// Maximum lag override allowed via hint (None = no limit)
    pub max_lag_override: Option<Duration>,
}

impl Default for HintConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_node_hints: true,
            allow_primary_reads: true,
            require_auth: false,
            strip_hints: true,
            log_decisions: false,
            max_lag_override: Some(Duration::from_secs(60)),
        }
    }
}

/// Consistency level configuration
#[derive(Debug, Clone)]
pub struct ConsistencyConfig {
    /// Allowed node patterns
    pub allowed_nodes: Vec<String>,
    /// Maximum lag in milliseconds (0 = no lag allowed)
    pub max_lag_ms: u64,
}

impl ConsistencyConfig {
    /// Check if a node name matches this consistency config
    pub fn allows_node(&self, node_name: &str) -> bool {
        self.allowed_nodes.iter().any(|pattern| {
            if pattern == "*" {
                true
            } else if pattern.ends_with('*') {
                node_name.starts_with(&pattern[..pattern.len() - 1])
            } else {
                node_name == pattern
            }
        })
    }
}

/// Per-route configuration
#[derive(Debug, Clone)]
pub struct RouteConfig {
    /// Nodes that can serve this route
    pub node_patterns: Vec<String>,
    /// Maximum lag for this route
    pub max_lag_ms: u64,
    /// Load balancing strategy override
    pub load_balance: Option<LoadBalanceStrategy>,
}

/// Load balancing strategies for routes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadBalanceStrategy {
    /// Round-robin selection
    RoundRobin,
    /// Weighted round-robin
    Weighted,
    /// Least connections
    LeastConnections,
    /// Latency-based selection
    LatencyBased,
    /// Random selection
    Random,
}

/// Route alias configuration
#[derive(Debug, Clone)]
pub struct AliasConfig {
    /// Alias name
    pub name: String,
    /// Target nodes for this alias
    pub nodes: Vec<String>,
    /// Whether this is an auto-detected alias
    pub auto: bool,
}

impl AliasConfig {
    /// Create a new alias configuration
    pub fn new(name: &str, nodes: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            nodes,
            auto: false,
        }
    }

    /// Create an auto-detected alias
    pub fn auto(name: &str) -> Self {
        Self {
            name: name.to_string(),
            nodes: Vec::new(),
            auto: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RoutingConfig::default();

        assert!(config.hints.enabled);
        assert!(config.hints.allow_node_hints);
        assert!(config.hints.allow_primary_reads);
        assert_eq!(config.default.read_target, RouteTarget::Standby);
        assert_eq!(config.default.write_target, RouteTarget::Primary);
    }

    #[test]
    fn test_consistency_config() {
        let config = RoutingConfig::default();

        let strong = config
            .get_consistency_config(ConsistencyLevel::Strong)
            .unwrap();
        assert_eq!(strong.max_lag_ms, 0);
        assert!(strong.allowed_nodes.contains(&"primary".to_string()));

        let eventual = config
            .get_consistency_config(ConsistencyLevel::Eventual)
            .unwrap();
        assert!(eventual.allows_node("any-node"));
    }

    #[test]
    fn test_alias_resolution() {
        let mut config = RoutingConfig::default();
        config.add_alias(
            "vector",
            vec![
                "standby-vector-1".to_string(),
                "standby-vector-2".to_string(),
            ],
        );

        let nodes = config.resolve_alias("vector").unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_node_pattern_matching() {
        let config = ConsistencyConfig {
            allowed_nodes: vec!["standby-*".to_string()],
            max_lag_ms: 1000,
        };

        assert!(config.allows_node("standby-sync-1"));
        assert!(config.allows_node("standby-async-2"));
        assert!(!config.allows_node("primary"));
    }

    #[test]
    fn test_hint_allowed() {
        let mut config = RoutingConfig::default();
        assert!(config.is_hint_allowed("route"));
        assert!(config.is_hint_allowed("node"));

        config.hints.allow_node_hints = false;
        assert!(!config.is_hint_allowed("node"));

        config.hints.enabled = false;
        assert!(!config.is_hint_allowed("route"));
    }
}
