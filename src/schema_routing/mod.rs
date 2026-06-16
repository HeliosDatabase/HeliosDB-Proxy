//! Schema-Aware Routing
//!
//! Feature 13 of the HeliosProxy roadmap.
//!
//! This module provides intelligent query routing based on schema semantics:
//!
//! - **Table Classification**: HOT/WARM/COLD temperature, OLTP/OLAP/Vector workload
//! - **Query Analysis**: Detect access patterns, shard keys, complexity
//! - **Smart Routing**: Route to optimal nodes based on schema + query characteristics
//! - **AI Workload Detection**: Recognize RAG, embedding, and agent workloads
//! - **Learning Classifier**: Automatically learn and update classifications
//!
//! # Architecture
//!
//! ```text
//! Query → Analyzer → Schema Registry → Router → Node Selection
//!                          ↑
//!                   Learning Classifier
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb::proxy::schema_routing::{
//!     SchemaAwareRouter, SchemaRegistry, SchemaRoutingConfig,
//! };
//!
//! let config = SchemaRoutingConfig::builder()
//!     .auto_discover(true)
//!     .refresh_interval(Duration::from_secs(300))
//!     .build();
//!
//! let registry = SchemaRegistry::new();
//! let router = SchemaAwareRouter::new(config, registry);
//!
//! let decision = router.route("SELECT * FROM users WHERE id = 1").await;
//! ```

pub mod admin;
pub mod analyzer;
pub mod classifier;
pub mod discovery;
pub mod metrics;
pub mod registry;
pub mod router;

pub use admin::{AdminError, SchemaRoutingAdmin};
pub use analyzer::{QueryAnalysis, QueryAnalyzer, ShardKeyValue, TableRef};
pub use classifier::{
    ClassificationModel, LearningClassifier, QueryHistory, QueryType, TableClassification,
};
pub use discovery::{DiscoveryConfig, DiscoveryError, SchemaDiscovery};
pub use metrics::{
    AIWorkloadStats, MetricsReport, RAGStats, RoutingStats, SchemaRoutingMetrics, TableStats,
    WorkloadStats,
};
pub use registry::{
    AccessPattern, ColumnSchema, DataTemperature, IndexSchema, NodeCapabilities, PartitionKey,
    Relationship, SchemaRegistry, ShardingConfig, TableSchema, WorkloadType,
};
pub use router::{
    AIWorkloadType, RAGStage, RouteTarget, RoutingDecision, RoutingPreference, RoutingReason,
    SchemaAwareRouter,
};

use std::collections::HashMap;
use std::time::Duration;

/// Schema-aware routing configuration
#[derive(Debug, Clone)]
pub struct SchemaRoutingConfig {
    /// Enable schema-aware routing
    pub enabled: bool,
    /// Auto-discover schema from database
    pub auto_discover: bool,
    /// Schema refresh interval
    pub refresh_interval: Duration,
    /// Enable learning classifier
    pub learning_enabled: bool,
    /// Classification update threshold (queries before reclassification)
    pub classification_threshold: u64,
    /// Default temperature for new tables
    pub default_temperature: DataTemperature,
    /// Default workload for new tables
    pub default_workload: WorkloadType,
    /// Table configurations
    pub tables: Vec<TableConfig>,
    /// Node capability configurations
    pub node_capabilities: HashMap<String, NodeCapabilities>,
}

impl Default for SchemaRoutingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_discover: true,
            refresh_interval: Duration::from_secs(300),
            learning_enabled: true,
            classification_threshold: 1000,
            default_temperature: DataTemperature::Warm,
            default_workload: WorkloadType::Mixed,
            tables: Vec::new(),
            node_capabilities: HashMap::new(),
        }
    }
}

/// Builder for SchemaRoutingConfig
#[derive(Debug, Default)]
pub struct SchemaRoutingConfigBuilder {
    config: SchemaRoutingConfig,
}

impl SchemaRoutingConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable schema-aware routing
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Enable auto-discovery of schema
    pub fn auto_discover(mut self, auto_discover: bool) -> Self {
        self.config.auto_discover = auto_discover;
        self
    }

    /// Set schema refresh interval
    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        self.config.refresh_interval = interval;
        self
    }

    /// Enable learning classifier
    pub fn learning_enabled(mut self, enabled: bool) -> Self {
        self.config.learning_enabled = enabled;
        self
    }

    /// Set classification threshold
    pub fn classification_threshold(mut self, threshold: u64) -> Self {
        self.config.classification_threshold = threshold;
        self
    }

    /// Set default temperature for new tables
    pub fn default_temperature(mut self, temp: DataTemperature) -> Self {
        self.config.default_temperature = temp;
        self
    }

    /// Set default workload for new tables
    pub fn default_workload(mut self, workload: WorkloadType) -> Self {
        self.config.default_workload = workload;
        self
    }

    /// Add table configuration
    pub fn add_table(mut self, table: TableConfig) -> Self {
        self.config.tables.push(table);
        self
    }

    /// Add node capability configuration
    pub fn add_node_capability(
        mut self,
        node_name: impl Into<String>,
        caps: NodeCapabilities,
    ) -> Self {
        self.config.node_capabilities.insert(node_name.into(), caps);
        self
    }

    /// Build the configuration
    pub fn build(self) -> SchemaRoutingConfig {
        self.config
    }
}

impl SchemaRoutingConfig {
    /// Create a builder
    pub fn builder() -> SchemaRoutingConfigBuilder {
        SchemaRoutingConfigBuilder::new()
    }
}

/// Table configuration for schema routing
#[derive(Debug, Clone)]
pub struct TableConfig {
    /// Table name
    pub name: String,
    /// Data temperature
    pub temperature: DataTemperature,
    /// Workload type
    pub workload: WorkloadType,
    /// Access pattern
    pub access_pattern: AccessPattern,
    /// Shard key (if sharded)
    pub shard_key: Option<String>,
    /// Shard count
    pub shard_count: Option<u32>,
    /// Preferred nodes
    pub preferred_nodes: Vec<String>,
}

impl TableConfig {
    /// Create a new table configuration
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            temperature: DataTemperature::Warm,
            workload: WorkloadType::Mixed,
            access_pattern: AccessPattern::Mixed,
            shard_key: None,
            shard_count: None,
            preferred_nodes: Vec::new(),
        }
    }

    /// Set temperature
    pub fn with_temperature(mut self, temp: DataTemperature) -> Self {
        self.temperature = temp;
        self
    }

    /// Set workload type
    pub fn with_workload(mut self, workload: WorkloadType) -> Self {
        self.workload = workload;
        self
    }

    /// Set access pattern
    pub fn with_access_pattern(mut self, pattern: AccessPattern) -> Self {
        self.access_pattern = pattern;
        self
    }

    /// Set shard key
    pub fn with_shard_key(mut self, key: impl Into<String>, count: u32) -> Self {
        self.shard_key = Some(key.into());
        self.shard_count = Some(count);
        self
    }

    /// Add preferred node
    pub fn with_preferred_node(mut self, node: impl Into<String>) -> Self {
        self.preferred_nodes.push(node.into());
        self
    }
}

/// Sync mode for replication
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncMode {
    /// Synchronous replication
    Sync,
    /// Asynchronous replication
    Async,
    /// Primary node
    Primary,
}

/// Node information for routing
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Node identifier
    pub id: String,
    /// Node name
    pub name: String,
    /// Is this the primary node
    pub is_primary: bool,
    /// Sync mode
    pub sync_mode: SyncMode,
    /// Node capabilities
    pub capabilities: NodeCapabilities,
    /// Current load (0.0 - 1.0)
    pub current_load: f64,
    /// Current latency in milliseconds
    pub current_latency_ms: u64,
    /// Indexes loaded in memory
    pub indexes_in_memory: Vec<String>,
}

impl NodeInfo {
    /// Create a new node
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            is_primary: false,
            sync_mode: SyncMode::Async,
            capabilities: NodeCapabilities::default(),
            current_load: 0.0,
            current_latency_ms: 0,
            indexes_in_memory: Vec::new(),
        }
    }

    /// Set as primary
    pub fn as_primary(mut self) -> Self {
        self.is_primary = true;
        self.sync_mode = SyncMode::Primary;
        self
    }

    /// Set sync mode
    pub fn with_sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Set capabilities
    pub fn with_capabilities(mut self, caps: NodeCapabilities) -> Self {
        self.capabilities = caps;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = SchemaRoutingConfig::builder()
            .enabled(true)
            .auto_discover(true)
            .refresh_interval(Duration::from_secs(60))
            .learning_enabled(true)
            .default_temperature(DataTemperature::Hot)
            .build();

        assert!(config.enabled);
        assert!(config.auto_discover);
        assert_eq!(config.refresh_interval, Duration::from_secs(60));
        assert_eq!(config.default_temperature, DataTemperature::Hot);
    }

    #[test]
    fn test_table_config_builder() {
        let config = TableConfig::new("users")
            .with_temperature(DataTemperature::Hot)
            .with_workload(WorkloadType::OLTP)
            .with_access_pattern(AccessPattern::PointLookup)
            .with_preferred_node("primary")
            .with_preferred_node("standby-sync");

        assert_eq!(config.name, "users");
        assert_eq!(config.temperature, DataTemperature::Hot);
        assert_eq!(config.workload, WorkloadType::OLTP);
        assert_eq!(config.preferred_nodes.len(), 2);
    }

    #[test]
    fn test_node_info() {
        let node = NodeInfo::new("node1", "primary")
            .as_primary()
            .with_capabilities(NodeCapabilities {
                vector_search: true,
                gpu_acceleration: true,
                ..Default::default()
            });

        assert!(node.is_primary);
        assert_eq!(node.sync_mode, SyncMode::Primary);
        assert!(node.capabilities.vector_search);
    }
}
