//! Node Filter
//!
//! Filters nodes based on routing hints and consistency requirements.

use super::{ConsistencyLevel, ParsedHints, Result, RouteTarget, RoutingConfig, RoutingError};
use std::time::Duration;

/// Node information for filtering
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Node name/identifier
    pub name: String,
    /// Node role (primary/standby)
    pub role: NodeRole,
    /// Sync mode
    pub sync_mode: SyncMode,
    /// Current replication lag
    pub lag_ms: u64,
    /// Is node healthy
    pub healthy: bool,
    /// Is node enabled for routing
    pub enabled: bool,
    /// Node weight for load balancing
    pub weight: u32,
    /// Tags for custom routing (e.g., "vector", "analytics")
    pub tags: Vec<String>,
    /// Zone/region for locality routing
    pub zone: Option<String>,
}

impl NodeInfo {
    /// Create a new primary node
    pub fn primary(name: &str) -> Self {
        Self {
            name: name.to_string(),
            role: NodeRole::Primary,
            sync_mode: SyncMode::Primary,
            lag_ms: 0,
            healthy: true,
            enabled: true,
            weight: 100,
            tags: Vec::new(),
            zone: None,
        }
    }

    /// Create a new standby node
    pub fn standby(name: &str, sync_mode: SyncMode) -> Self {
        Self {
            name: name.to_string(),
            role: NodeRole::Standby,
            sync_mode,
            lag_ms: 0,
            healthy: true,
            enabled: true,
            weight: 100,
            tags: Vec::new(),
            zone: None,
        }
    }

    /// Set lag
    pub fn with_lag(mut self, lag_ms: u64) -> Self {
        self.lag_ms = lag_ms;
        self
    }

    /// Set tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Set zone
    pub fn with_zone(mut self, zone: &str) -> Self {
        self.zone = Some(zone.to_string());
        self
    }

    /// Check if node has a specific tag
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }
}

/// Node role
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Standby,
    ReadReplica,
}

/// Sync mode for standby nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Primary node
    Primary,
    /// Fully synchronous replication
    Sync,
    /// Semi-synchronous replication
    SemiSync,
    /// Asynchronous replication
    Async,
}

impl SyncMode {
    /// Check if this mode matches a route target
    pub fn matches_target(&self, target: RouteTarget) -> bool {
        match target {
            RouteTarget::Primary => *self == SyncMode::Primary,
            RouteTarget::Sync => *self == SyncMode::Sync,
            RouteTarget::SemiSync => *self == SyncMode::SemiSync,
            RouteTarget::Async => *self == SyncMode::Async,
            RouteTarget::Standby => {
                matches!(self, SyncMode::Sync | SyncMode::SemiSync | SyncMode::Async)
            }
            RouteTarget::Any => true,
            RouteTarget::Local => true,  // Handled separately
            RouteTarget::Vector => true, // Handled via tags
        }
    }
}

/// Node filter for routing decisions
#[derive(Debug)]
pub struct NodeFilter {
    /// Routing configuration
    config: RoutingConfig,
    /// Local zone (for local routing)
    local_zone: Option<String>,
}

impl NodeFilter {
    /// Create a new node filter
    pub fn new(config: RoutingConfig) -> Self {
        Self {
            config,
            local_zone: None,
        }
    }

    /// Set local zone
    pub fn with_local_zone(mut self, zone: &str) -> Self {
        self.local_zone = Some(zone.to_string());
        self
    }

    /// Filter nodes based on criteria
    pub fn filter<'a>(&self, nodes: &'a [NodeInfo], criteria: &NodeCriteria) -> FilterResult<'a> {
        let mut eligible: Vec<&NodeInfo> =
            nodes.iter().filter(|n| n.healthy && n.enabled).collect();

        let mut reasons = Vec::new();

        // Filter by specific node name
        if let Some(ref name) = criteria.node_name {
            let count_before = eligible.len();
            eligible.retain(|n| n.name == *name);
            if eligible.len() < count_before {
                reasons.push(format!("Filtered to node: {}", name));
            }
        }

        // Filter by route target
        if let Some(target) = criteria.route {
            let count_before = eligible.len();
            eligible.retain(|n| self.matches_route_target(n, target));
            if eligible.len() < count_before {
                reasons.push(format!("Filtered by route target: {:?}", target));
            }
        }

        // Filter by consistency level
        if let Some(consistency) = criteria.consistency {
            let count_before = eligible.len();
            eligible.retain(|n| self.meets_consistency(n, consistency, criteria.max_lag));
            if eligible.len() < count_before {
                reasons.push(format!("Filtered by consistency: {:?}", consistency));
            }
        }

        // Filter by max lag
        if let Some(max_lag) = criteria.max_lag {
            let count_before = eligible.len();
            let max_lag_ms = max_lag.as_millis() as u64;
            eligible.retain(|n| n.lag_ms <= max_lag_ms);
            if eligible.len() < count_before {
                reasons.push(format!("Filtered by max lag: {}ms", max_lag_ms));
            }
        }

        // Filter by tags
        if !criteria.required_tags.is_empty() {
            let count_before = eligible.len();
            eligible.retain(|n| criteria.required_tags.iter().all(|tag| n.has_tag(tag)));
            if eligible.len() < count_before {
                reasons.push(format!("Filtered by tags: {:?}", criteria.required_tags));
            }
        }

        // Handle local routing
        if criteria.route == Some(RouteTarget::Local) {
            if let Some(ref local_zone) = self.local_zone {
                let local_nodes: Vec<_> = eligible
                    .iter()
                    .filter(|n| n.zone.as_ref() == Some(local_zone))
                    .copied()
                    .collect();

                if !local_nodes.is_empty() {
                    eligible = local_nodes;
                    reasons.push(format!("Preferred local zone: {}", local_zone));
                }
            }
        }

        // Handle vector routing
        if criteria.route == Some(RouteTarget::Vector) {
            let vector_nodes: Vec<_> = eligible
                .iter()
                .filter(|n| n.has_tag("vector"))
                .copied()
                .collect();

            if !vector_nodes.is_empty() {
                eligible = vector_nodes;
                reasons.push("Filtered to vector-capable nodes".to_string());
            }
        }

        // Resolve aliases
        if let Some(ref alias) = criteria.alias {
            if let Some(alias_nodes) = self.config.resolve_alias(alias) {
                let count_before = eligible.len();
                eligible.retain(|n| alias_nodes.contains(&n.name));
                if eligible.len() < count_before {
                    reasons.push(format!("Resolved alias: {}", alias));
                }
            }
        }

        FilterResult {
            eligible,
            reasons,
            fallback_used: false,
        }
    }

    /// Check if node matches route target
    fn matches_route_target(&self, node: &NodeInfo, target: RouteTarget) -> bool {
        match target {
            RouteTarget::Primary => node.role == NodeRole::Primary,
            RouteTarget::Standby => node.role == NodeRole::Standby,
            RouteTarget::Sync => node.sync_mode == SyncMode::Sync,
            RouteTarget::SemiSync => node.sync_mode == SyncMode::SemiSync,
            RouteTarget::Async => node.sync_mode == SyncMode::Async,
            RouteTarget::Any => true,
            RouteTarget::Local => true, // Handled in filter()
            RouteTarget::Vector => node.has_tag("vector"),
        }
    }

    /// Check if node meets consistency requirements
    fn meets_consistency(
        &self,
        node: &NodeInfo,
        level: ConsistencyLevel,
        max_lag: Option<Duration>,
    ) -> bool {
        let config = match self.config.get_consistency_config(level) {
            Some(c) => c,
            None => return true, // No config = allow all
        };

        // Check if node name matches allowed patterns
        if !config.allows_node(&node.name)
            && !config.allows_node(&format!("{:?}", node.role).to_lowercase())
        {
            return false;
        }

        // Check lag constraint
        let max_lag_ms = max_lag
            .map(|d| d.as_millis() as u64)
            .unwrap_or(config.max_lag_ms);

        if max_lag_ms < u64::MAX && node.lag_ms > max_lag_ms {
            return false;
        }

        true
    }

    /// Get default criteria for a query type
    pub fn default_criteria_for_read(&self) -> NodeCriteria {
        NodeCriteria {
            route: Some(self.config.default.read_target),
            consistency: Some(self.config.default.consistency),
            ..Default::default()
        }
    }

    /// Get default criteria for a write
    pub fn default_criteria_for_write(&self) -> NodeCriteria {
        NodeCriteria {
            route: Some(self.config.default.write_target),
            consistency: Some(ConsistencyLevel::Strong),
            ..Default::default()
        }
    }
}

/// Criteria for node filtering
#[derive(Debug, Clone, Default)]
pub struct NodeCriteria {
    /// Specific node name
    pub node_name: Option<String>,
    /// Route target
    pub route: Option<RouteTarget>,
    /// Consistency level
    pub consistency: Option<ConsistencyLevel>,
    /// Maximum acceptable lag
    pub max_lag: Option<Duration>,
    /// Required tags
    pub required_tags: Vec<String>,
    /// Alias to resolve
    pub alias: Option<String>,
    /// Branch name (for branch-aware routing)
    pub branch: Option<String>,
}

impl NodeCriteria {
    /// Create criteria from parsed hints
    pub fn from_hints(hints: &ParsedHints) -> Self {
        Self {
            node_name: hints.node.clone(),
            route: hints.route,
            consistency: hints.consistency,
            max_lag: hints.max_lag,
            required_tags: Vec::new(),
            alias: None,
            branch: hints.branch.clone(),
        }
    }

    /// Add a required tag
    pub fn with_tag(mut self, tag: &str) -> Self {
        self.required_tags.push(tag.to_string());
        self
    }

    /// Set alias
    pub fn with_alias(mut self, alias: &str) -> Self {
        self.alias = Some(alias.to_string());
        self
    }
}

/// Result of node filtering
#[derive(Debug)]
pub struct FilterResult<'a> {
    /// Eligible nodes after filtering
    pub eligible: Vec<&'a NodeInfo>,
    /// Reasons for filtering decisions
    pub reasons: Vec<String>,
    /// Whether fallback was used
    pub fallback_used: bool,
}

impl<'a> FilterResult<'a> {
    /// Check if any nodes match
    pub fn has_matches(&self) -> bool {
        !self.eligible.is_empty()
    }

    /// Get number of matches
    pub fn count(&self) -> usize {
        self.eligible.len()
    }

    /// Get first match
    pub fn first(&self) -> Option<&'a NodeInfo> {
        self.eligible.first().copied()
    }

    /// Convert to error if no matches
    pub fn require_match(&self, context: &str) -> Result<&'a NodeInfo> {
        self.first().ok_or_else(|| {
            RoutingError::NoMatchingNodes(format!(
                "{}: reasons: {}",
                context,
                self.reasons.join(", ")
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_nodes() -> Vec<NodeInfo> {
        vec![
            NodeInfo::primary("primary"),
            NodeInfo::standby("standby-sync-1", SyncMode::Sync),
            NodeInfo::standby("standby-async-1", SyncMode::Async).with_lag(500),
            NodeInfo::standby("standby-async-2", SyncMode::Async).with_lag(5000),
            NodeInfo::standby("standby-vector-1", SyncMode::Async)
                .with_tags(vec!["vector".to_string()]),
        ]
    }

    #[test]
    fn test_filter_by_route_target() {
        let filter = NodeFilter::new(RoutingConfig::default());
        let nodes = test_nodes();

        // Filter for primary
        let criteria = NodeCriteria {
            route: Some(RouteTarget::Primary),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert_eq!(result.count(), 1);
        assert_eq!(result.first().unwrap().name, "primary");

        // Filter for any standby
        let criteria = NodeCriteria {
            route: Some(RouteTarget::Standby),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert_eq!(result.count(), 4);
    }

    #[test]
    fn test_filter_by_sync_mode() {
        let filter = NodeFilter::new(RoutingConfig::default());
        let nodes = test_nodes();

        let criteria = NodeCriteria {
            route: Some(RouteTarget::Sync),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert_eq!(result.count(), 1);
        assert_eq!(result.first().unwrap().name, "standby-sync-1");
    }

    #[test]
    fn test_filter_by_max_lag() {
        let filter = NodeFilter::new(RoutingConfig::default());
        let nodes = test_nodes();

        let criteria = NodeCriteria {
            max_lag: Some(Duration::from_millis(1000)),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);

        // Should exclude standby-async-2 (5000ms lag)
        assert!(result.eligible.iter().all(|n| n.lag_ms <= 1000));
    }

    #[test]
    fn test_filter_by_node_name() {
        let filter = NodeFilter::new(RoutingConfig::default());
        let nodes = test_nodes();

        let criteria = NodeCriteria {
            node_name: Some("standby-sync-1".to_string()),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert_eq!(result.count(), 1);
        assert_eq!(result.first().unwrap().name, "standby-sync-1");
    }

    #[test]
    fn test_filter_by_tag() {
        let filter = NodeFilter::new(RoutingConfig::default());
        let nodes = test_nodes();

        let criteria = NodeCriteria {
            route: Some(RouteTarget::Vector),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert_eq!(result.count(), 1);
        assert_eq!(result.first().unwrap().name, "standby-vector-1");
    }

    #[test]
    fn test_filter_with_alias() {
        let mut config = RoutingConfig::default();
        config.add_alias(
            "analytics",
            vec!["standby-async-1".to_string(), "standby-async-2".to_string()],
        );

        let filter = NodeFilter::new(config);
        let nodes = test_nodes();

        let criteria = NodeCriteria {
            alias: Some("analytics".to_string()),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert_eq!(result.count(), 2);
    }

    #[test]
    fn test_local_zone_preference() {
        let filter = NodeFilter::new(RoutingConfig::default()).with_local_zone("us-west-1");

        let nodes = vec![
            NodeInfo::standby("standby-1", SyncMode::Async).with_zone("us-east-1"),
            NodeInfo::standby("standby-2", SyncMode::Async).with_zone("us-west-1"),
        ];

        let criteria = NodeCriteria {
            route: Some(RouteTarget::Local),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert_eq!(result.count(), 1);
        assert_eq!(result.first().unwrap().name, "standby-2");
    }

    #[test]
    fn test_no_match_error() {
        let filter = NodeFilter::new(RoutingConfig::default());
        let nodes = test_nodes();

        let criteria = NodeCriteria {
            node_name: Some("nonexistent".to_string()),
            ..Default::default()
        };
        let result = filter.filter(&nodes, &criteria);
        assert!(!result.has_matches());

        let err = result.require_match("test context");
        assert!(err.is_err());
    }

    #[test]
    fn test_from_hints() {
        let parser = super::super::HintParser::new();
        let hints = parser.parse("/*helios:route=sync,lag=100ms*/ SELECT 1");

        let criteria = NodeCriteria::from_hints(&hints);
        assert_eq!(criteria.route, Some(RouteTarget::Sync));
        assert_eq!(criteria.max_lag, Some(Duration::from_millis(100)));
    }
}
