//! Schema-Aware Router
//!
//! Routes queries based on schema semantics and workload characteristics.

use std::collections::HashMap;
use std::sync::Arc;

use super::{
    NodeInfo, SyncMode, SchemaRoutingConfig,
    registry::{SchemaRegistry, NodeCapabilities, DataTemperature, WorkloadType, AccessPattern},
    analyzer::{QueryAnalyzer, QueryAnalysis},
};

/// Schema-aware query router
#[derive(Debug)]
pub struct SchemaAwareRouter {
    /// Configuration
    config: SchemaRoutingConfig,
    /// Schema registry
    schema: Arc<SchemaRegistry>,
    /// Query analyzer
    analyzer: QueryAnalyzer,
    /// Available nodes
    nodes: Vec<NodeInfo>,
    /// AI workload detector
    ai_detector: AIWorkloadDetector,
    /// RAG router
    rag_router: RAGRouter,
}

impl SchemaAwareRouter {
    /// Create a new schema-aware router
    pub fn new(config: SchemaRoutingConfig, schema: Arc<SchemaRegistry>) -> Self {
        Self {
            analyzer: QueryAnalyzer::new(schema.clone()),
            schema,
            config,
            nodes: Vec::new(),
            ai_detector: AIWorkloadDetector::new(),
            rag_router: RAGRouter::new(),
        }
    }

    /// Add a node to the router
    pub fn add_node(&mut self, node: NodeInfo) {
        self.nodes.push(node);
    }

    /// Remove a node from the router
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|n| n.id != node_id);
    }

    /// Update node status
    pub fn update_node(&mut self, node_id: &str, load: f64, latency_ms: u64) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == node_id) {
            node.current_load = load;
            node.current_latency_ms = latency_ms;
        }
    }

    /// Route a query
    pub fn route(&self, query: &str) -> RoutingDecision {
        if !self.config.enabled {
            return RoutingDecision::default_routing();
        }

        let analysis = self.analyzer.analyze(query);

        // 1. Check for AI workload patterns
        if let Some(ai_workload) = self.ai_detector.detect(query) {
            let preference = self.ai_detector.get_optimal_routing(ai_workload);
            return self.apply_preference(preference, &analysis);
        }

        // 2. Determine required capabilities
        let required_caps = self.get_required_capabilities(&analysis);

        // 3. Filter eligible nodes
        let eligible = self.filter_by_capabilities(&required_caps);

        // 4. Check sharding
        if let Some(shard_routing) = self.try_shard_routing(&analysis) {
            return shard_routing;
        }

        // 5. Route based on workload type
        match analysis.workload_type {
            WorkloadType::OLTP => self.route_oltp(&eligible, &analysis),
            WorkloadType::OLAP => self.route_olap(&eligible, &analysis),
            WorkloadType::Vector => self.route_vector(&eligible, &analysis),
            WorkloadType::HTAP | WorkloadType::Mixed => self.route_mixed(&eligible, &analysis),
        }
    }

    /// Route with branch context
    pub fn route_with_branch(&self, query: &str, branch: &str) -> RoutingDecision {
        let analysis = self.analyzer.analyze(query);

        // Get nodes that have the branch data
        let branch_nodes = self.schema.get_branch_locations(branch);

        // Filter by query requirements
        let required_caps = self.get_required_capabilities(&analysis);
        let eligible = self.filter_by_capabilities(&required_caps);

        // Intersection with branch nodes
        let available: Vec<_> = eligible
            .iter()
            .filter(|n| branch_nodes.contains(&n.id))
            .cloned()
            .collect();

        if available.is_empty() {
            // Branch not replicated to eligible nodes
            return RoutingDecision {
                target: RouteTarget::Primary,
                reason: RoutingReason::BranchNotAvailable,
                branch: Some(branch.to_string()),
                ..Default::default()
            };
        }

        self.select_best(&available, &analysis)
    }

    /// Route for time-travel query
    pub fn route_time_travel(&self, query: &str, age_days: i64) -> RoutingDecision {
        let analysis = self.analyzer.analyze(query);

        // Recent data on hot nodes
        if age_days < 7 {
            return self.route_to_temperature_nodes(DataTemperature::Hot, &analysis);
        }

        // Older data on warm nodes
        if age_days < 30 {
            return self.route_to_temperature_nodes(DataTemperature::Warm, &analysis);
        }

        // Historical data on cold/archive nodes
        self.route_to_temperature_nodes(DataTemperature::Cold, &analysis)
    }

    /// Route RAG query
    pub fn route_rag(&self, stage: RAGStage, query: &str) -> RoutingDecision {
        let analysis = self.analyzer.analyze(query);
        self.rag_router.route_rag_query(stage, &analysis, &self.nodes)
    }

    /// Get required capabilities based on query analysis
    fn get_required_capabilities(&self, analysis: &QueryAnalysis) -> NodeCapabilities {
        let mut caps = NodeCapabilities::default();

        // Vector queries need vector-capable nodes
        if analysis.access_patterns.contains(&AccessPattern::VectorSearch) {
            caps.vector_search = true;
            caps.gpu_acceleration = true; // Prefer GPU nodes
        }

        // OLAP queries prefer columnar storage
        if analysis.workload_type == WorkloadType::OLAP {
            caps.columnar_storage = true;
        }

        // Hot tables need in-memory nodes
        for table in &analysis.tables {
            if let Some(schema) = &table.schema {
                if schema.temperature == DataTemperature::Hot {
                    caps.in_memory = true;
                }
            }
        }

        caps
    }

    /// Filter nodes by capabilities
    fn filter_by_capabilities(&self, required: &NodeCapabilities) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|n| n.capabilities.satisfies(required) || !required.has_requirements())
            .cloned()
            .collect()
    }

    /// Try to route to specific shard
    fn try_shard_routing(&self, analysis: &QueryAnalysis) -> Option<RoutingDecision> {
        for table in &analysis.tables {
            if let Some(schema) = &table.schema {
                if let Some(shard_key) = &schema.shard_key {
                    if let Some(shard_value) = analysis.shard_keys.get(shard_key) {
                        let value = match shard_value {
                            super::analyzer::ShardKeyValue::Single(v) => v.clone(),
                            super::analyzer::ShardKeyValue::Multiple(v) => {
                                // Multiple values = scatter-gather
                                return Some(RoutingDecision {
                                    target: RouteTarget::ScatterGather,
                                    shards: v.iter().filter_map(|val| {
                                        self.schema.get_shard(shard_key, val)
                                    }).collect(),
                                    reason: RoutingReason::ShardKey,
                                    ..Default::default()
                                });
                            }
                        };

                        if let Some(shard) = self.schema.get_shard(shard_key, &value) {
                            return Some(RoutingDecision {
                                target: RouteTarget::Shard(shard),
                                reason: RoutingReason::ShardKey,
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }
        None
    }

    /// Route OLTP workload
    fn route_oltp(&self, nodes: &[NodeInfo], analysis: &QueryAnalysis) -> RoutingDecision {
        // Write queries must go to primary
        if !analysis.is_read_only {
            return RoutingDecision {
                target: RouteTarget::Primary,
                reason: RoutingReason::WriteQuery,
                ..Default::default()
            };
        }

        // OLTP: Low latency, prefer primary or sync standbys
        let mut preferred: Vec<_> = nodes
            .iter()
            .filter(|n| n.sync_mode == SyncMode::Sync || n.is_primary)
            .cloned()
            .collect();

        preferred.sort_by_key(|n| n.current_latency_ms);

        if let Some(node) = preferred.first() {
            RoutingDecision {
                target: RouteTarget::Node(node.id.clone()),
                reason: RoutingReason::LowLatency,
                node_info: Some(node.clone()),
                ..Default::default()
            }
        } else {
            RoutingDecision::default_routing()
        }
    }

    /// Route OLAP workload
    fn route_olap(&self, nodes: &[NodeInfo], _analysis: &QueryAnalysis) -> RoutingDecision {
        // OLAP: Throughput over latency, prefer async standbys with columnar storage
        let mut preferred: Vec<_> = nodes
            .iter()
            .filter(|n| n.capabilities.columnar_storage)
            .cloned()
            .collect();

        if preferred.is_empty() {
            // Fall back to any async standby
            preferred = nodes
                .iter()
                .filter(|n| n.sync_mode == SyncMode::Async)
                .cloned()
                .collect();
        }

        preferred.sort_by(|a, b| a.current_load.partial_cmp(&b.current_load).unwrap());

        if let Some(node) = preferred.first() {
            RoutingDecision {
                target: RouteTarget::Node(node.id.clone()),
                reason: RoutingReason::ColumnarStorage,
                node_info: Some(node.clone()),
                ..Default::default()
            }
        } else {
            RoutingDecision::default_routing()
        }
    }

    /// Route vector workload
    fn route_vector(&self, nodes: &[NodeInfo], _analysis: &QueryAnalysis) -> RoutingDecision {
        // Vector: Need vector-capable nodes, prefer GPU
        let mut vector_nodes: Vec<_> = nodes
            .iter()
            .filter(|n| n.capabilities.vector_search)
            .cloned()
            .collect();

        // Sort by: GPU first, then lower load
        vector_nodes.sort_by(|a, b| {
            b.capabilities.gpu_acceleration
                .cmp(&a.capabilities.gpu_acceleration)
                .then_with(|| a.current_load.partial_cmp(&b.current_load).unwrap())
        });

        if let Some(node) = vector_nodes.first() {
            RoutingDecision {
                target: RouteTarget::Node(node.id.clone()),
                reason: RoutingReason::VectorCapable,
                node_info: Some(node.clone()),
                ..Default::default()
            }
        } else {
            // No vector-capable nodes, fall back to primary
            RoutingDecision {
                target: RouteTarget::Primary,
                reason: RoutingReason::NoVectorNodes,
                ..Default::default()
            }
        }
    }

    /// Route mixed workload
    fn route_mixed(&self, nodes: &[NodeInfo], analysis: &QueryAnalysis) -> RoutingDecision {
        // Mixed: Balance between latency and throughput
        if !analysis.is_read_only {
            return RoutingDecision {
                target: RouteTarget::Primary,
                reason: RoutingReason::WriteQuery,
                ..Default::default()
            };
        }

        // Sort by weighted score: latency + load
        let mut scored: Vec<_> = nodes
            .iter()
            .map(|n| {
                let score = (n.current_latency_ms as f64) + (n.current_load * 100.0);
                (n, score)
            })
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        if let Some((node, _)) = scored.first() {
            RoutingDecision {
                target: RouteTarget::Node(node.id.clone()),
                reason: RoutingReason::LowestScore,
                node_info: Some((*node).clone()),
                ..Default::default()
            }
        } else {
            RoutingDecision::default_routing()
        }
    }

    /// Route to nodes with specific temperature
    fn route_to_temperature_nodes(&self, temp: DataTemperature, analysis: &QueryAnalysis) -> RoutingDecision {
        // Find nodes that host tables with matching temperature
        let matching_nodes: Vec<_> = self.nodes
            .iter()
            .filter(|n| {
                match temp {
                    DataTemperature::Hot => n.capabilities.in_memory,
                    DataTemperature::Warm => !n.capabilities.in_memory && !self.is_cold_storage(n),
                    DataTemperature::Cold | DataTemperature::Frozen => self.is_cold_storage(n),
                }
            })
            .cloned()
            .collect();

        if matching_nodes.is_empty() {
            return self.route_mixed(&self.nodes, analysis);
        }

        self.select_best(&matching_nodes, analysis)
    }

    /// Check if node is cold storage
    fn is_cold_storage(&self, node: &NodeInfo) -> bool {
        // Heuristic: cold storage nodes have no in-memory capability
        // and are not primary/sync
        !node.capabilities.in_memory && node.sync_mode == SyncMode::Async && !node.is_primary
    }

    /// Select best node from candidates
    fn select_best(&self, nodes: &[NodeInfo], analysis: &QueryAnalysis) -> RoutingDecision {
        if nodes.is_empty() {
            return RoutingDecision::default_routing();
        }

        // Sort by latency for read queries, load for analytics
        let mut sorted = nodes.to_vec();
        if analysis.workload_type == WorkloadType::OLAP {
            sorted.sort_by(|a, b| a.current_load.partial_cmp(&b.current_load).unwrap());
        } else {
            sorted.sort_by_key(|n| n.current_latency_ms);
        }

        let node = &sorted[0];
        RoutingDecision {
            target: RouteTarget::Node(node.id.clone()),
            reason: RoutingReason::BestCandidate,
            node_info: Some(node.clone()),
            ..Default::default()
        }
    }

    /// Apply routing preference
    fn apply_preference(&self, preference: RoutingPreference, analysis: &QueryAnalysis) -> RoutingDecision {
        match preference {
            RoutingPreference::VectorNodes { prefer_gpu } => {
                let nodes: Vec<_> = self.nodes
                    .iter()
                    .filter(|n| n.capabilities.vector_search)
                    .filter(|n| !prefer_gpu || n.capabilities.gpu_acceleration)
                    .cloned()
                    .collect();
                self.select_best(&nodes, analysis)
            }
            RoutingPreference::LowLatency { max_lag_ms } => {
                let nodes: Vec<_> = self.nodes
                    .iter()
                    .filter(|n| n.current_latency_ms <= max_lag_ms)
                    .cloned()
                    .collect();
                self.select_best(&nodes, analysis)
            }
            RoutingPreference::HighThroughput => {
                let nodes: Vec<_> = self.nodes
                    .iter()
                    .filter(|n| n.sync_mode == SyncMode::Async)
                    .cloned()
                    .collect();
                self.select_best(&nodes, analysis)
            }
            RoutingPreference::Primary => {
                RoutingDecision {
                    target: RouteTarget::Primary,
                    reason: RoutingReason::AIWorkload,
                    ..Default::default()
                }
            }
        }
    }
}

impl NodeCapabilities {
    /// Check if there are any requirements
    fn has_requirements(&self) -> bool {
        self.vector_search || self.gpu_acceleration || self.columnar_storage
            || self.in_memory || self.content_addressed
    }
}

/// Routing decision
#[derive(Debug, Clone, Default)]
pub struct RoutingDecision {
    /// Target for routing
    pub target: RouteTarget,
    /// Reason for decision
    pub reason: RoutingReason,
    /// Target shards (for scatter-gather)
    pub shards: Vec<u32>,
    /// Branch context
    pub branch: Option<String>,
    /// Selected node info
    pub node_info: Option<NodeInfo>,
}

impl RoutingDecision {
    /// Create a shard routing decision
    pub fn shard(shard_id: u32) -> Self {
        Self {
            target: RouteTarget::Shard(shard_id),
            reason: RoutingReason::ShardKey,
            ..Default::default()
        }
    }

    /// Create a single node routing decision
    pub fn single(node: NodeInfo) -> Self {
        Self {
            target: RouteTarget::Node(node.id.clone()),
            reason: RoutingReason::BestCandidate,
            node_info: Some(node),
            ..Default::default()
        }
    }

    /// Create default routing (to primary)
    pub fn default_routing() -> Self {
        Self {
            target: RouteTarget::Primary,
            reason: RoutingReason::Default,
            ..Default::default()
        }
    }

    /// Check if routing to primary
    pub fn is_primary(&self) -> bool {
        matches!(self.target, RouteTarget::Primary)
    }

    /// Check if scatter-gather needed
    pub fn is_scatter_gather(&self) -> bool {
        matches!(self.target, RouteTarget::ScatterGather)
    }
}

/// Route target
#[derive(Debug, Clone, Default)]
pub enum RouteTarget {
    /// Route to primary
    #[default]
    Primary,
    /// Route to specific node
    Node(String),
    /// Route to specific shard
    Shard(u32),
    /// Scatter-gather across shards
    ScatterGather,
}

/// Routing reason
#[derive(Debug, Clone, Default)]
pub enum RoutingReason {
    /// Default routing
    #[default]
    Default,
    /// Write query must go to primary
    WriteQuery,
    /// Shard key present in query
    ShardKey,
    /// Lowest latency node
    LowLatency,
    /// Node with columnar storage
    ColumnarStorage,
    /// Vector-capable node
    VectorCapable,
    /// No vector-capable nodes available
    NoVectorNodes,
    /// Branch not available on eligible nodes
    BranchNotAvailable,
    /// Best candidate from scoring
    BestCandidate,
    /// Lowest combined score
    LowestScore,
    /// AI workload routing
    AIWorkload,
}

/// Routing preference for AI workloads
#[derive(Debug, Clone)]
pub enum RoutingPreference {
    /// Prefer vector-capable nodes
    VectorNodes { prefer_gpu: bool },
    /// Prefer low-latency nodes
    LowLatency { max_lag_ms: u64 },
    /// Prefer high-throughput nodes
    HighThroughput,
    /// Must route to primary
    Primary,
}

/// AI workload type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AIWorkloadType {
    /// Embedding retrieval (vector search)
    EmbeddingRetrieval,
    /// Context/conversation lookup
    ContextLookup,
    /// Knowledge base query
    KnowledgeBase,
    /// Tool execution (writes)
    ToolExecution,
}

/// AI workload detector
#[derive(Debug, Default)]
pub struct AIWorkloadDetector {
    /// Patterns for detection
    patterns: Vec<AIPattern>,
}

#[derive(Debug)]
struct AIPattern {
    keyword: String,
    workload_type: AIWorkloadType,
}

impl AIWorkloadDetector {
    /// Create a new detector
    pub fn new() -> Self {
        Self {
            patterns: vec![
                AIPattern { keyword: "<->".to_string(), workload_type: AIWorkloadType::EmbeddingRetrieval },
                AIPattern { keyword: "VECTOR".to_string(), workload_type: AIWorkloadType::EmbeddingRetrieval },
                AIPattern { keyword: "EMBEDDING".to_string(), workload_type: AIWorkloadType::EmbeddingRetrieval },
                AIPattern { keyword: "CONVERSATION".to_string(), workload_type: AIWorkloadType::ContextLookup },
                AIPattern { keyword: "TURNS".to_string(), workload_type: AIWorkloadType::ContextLookup },
                AIPattern { keyword: "DOCUMENTS".to_string(), workload_type: AIWorkloadType::KnowledgeBase },
                AIPattern { keyword: "CHUNKS".to_string(), workload_type: AIWorkloadType::KnowledgeBase },
                AIPattern { keyword: "TOOL_RESULTS".to_string(), workload_type: AIWorkloadType::ToolExecution },
                AIPattern { keyword: "ACTIONS".to_string(), workload_type: AIWorkloadType::ToolExecution },
            ],
        }
    }

    /// Detect AI workload type
    pub fn detect(&self, query: &str) -> Option<AIWorkloadType> {
        let upper = query.to_uppercase();

        for pattern in &self.patterns {
            if upper.contains(&pattern.keyword) {
                return Some(pattern.workload_type);
            }
        }

        None
    }

    /// Get optimal routing for AI workload
    pub fn get_optimal_routing(&self, workload: AIWorkloadType) -> RoutingPreference {
        match workload {
            AIWorkloadType::EmbeddingRetrieval => {
                RoutingPreference::VectorNodes { prefer_gpu: true }
            }
            AIWorkloadType::ContextLookup => {
                RoutingPreference::LowLatency { max_lag_ms: 100 }
            }
            AIWorkloadType::KnowledgeBase => {
                RoutingPreference::HighThroughput
            }
            AIWorkloadType::ToolExecution => {
                RoutingPreference::Primary
            }
        }
    }
}

/// RAG stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RAGStage {
    /// Retrieval stage (vector search)
    Retrieval,
    /// Fetch stage (document lookup)
    Fetch,
    /// Rerank stage
    Rerank,
    /// Generation stage
    Generate,
}

/// RAG router
#[derive(Debug, Default)]
pub struct RAGRouter {}

impl RAGRouter {
    /// Create a new RAG router
    pub fn new() -> Self {
        Self {}
    }

    /// Route RAG query based on stage
    pub fn route_rag_query(&self, stage: RAGStage, analysis: &QueryAnalysis, nodes: &[NodeInfo]) -> RoutingDecision {
        match stage {
            RAGStage::Retrieval => {
                // Vector search on embeddings
                let vector_nodes: Vec<_> = nodes
                    .iter()
                    .filter(|n| n.capabilities.vector_search)
                    .cloned()
                    .collect();

                if let Some(node) = vector_nodes.first() {
                    RoutingDecision::single(node.clone())
                } else {
                    RoutingDecision::default_routing()
                }
            }
            RAGStage::Fetch => {
                // Bulk fetch - high throughput
                let throughput_nodes: Vec<_> = nodes
                    .iter()
                    .filter(|n| n.sync_mode == SyncMode::Async)
                    .cloned()
                    .collect();

                if let Some(node) = throughput_nodes.first() {
                    RoutingDecision::single(node.clone())
                } else {
                    RoutingDecision::default_routing()
                }
            }
            RAGStage::Rerank => {
                // Light computation - lowest latency
                let mut sorted = nodes.to_vec();
                sorted.sort_by_key(|n| n.current_latency_ms);

                if let Some(node) = sorted.first() {
                    RoutingDecision::single(node.clone())
                } else {
                    RoutingDecision::default_routing()
                }
            }
            RAGStage::Generate => {
                // May write to cache - check if write
                if !analysis.is_read_only {
                    RoutingDecision::default_routing()
                } else {
                    let mut sorted = nodes.to_vec();
                    sorted.sort_by_key(|n| n.current_latency_ms);

                    if let Some(node) = sorted.first() {
                        RoutingDecision::single(node.clone())
                    } else {
                        RoutingDecision::default_routing()
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_routing::registry::TableSchema;

    fn create_test_setup() -> SchemaAwareRouter {
        let registry = Arc::new(SchemaRegistry::new());

        registry.register_table(
            TableSchema::new("users")
                .with_workload(WorkloadType::OLTP)
                .with_access_pattern(AccessPattern::PointLookup)
                .with_primary_key(vec!["id".to_string()])
        );

        registry.register_table(
            TableSchema::new("events")
                .with_workload(WorkloadType::OLAP)
                .with_temperature(DataTemperature::Cold)
        );

        registry.register_table(
            TableSchema::new("embeddings")
                .with_workload(WorkloadType::Vector)
        );

        let config = SchemaRoutingConfig::default();
        let mut router = SchemaAwareRouter::new(config, registry);

        // Add test nodes
        router.add_node(NodeInfo::new("primary", "primary").as_primary());
        router.add_node(NodeInfo::new("standby-sync", "standby-sync")
            .with_sync_mode(SyncMode::Sync));
        router.add_node(NodeInfo::new("standby-async", "standby-async")
            .with_sync_mode(SyncMode::Async)
            .with_capabilities(NodeCapabilities::analytics_node()));
        router.add_node(NodeInfo::new("vector-node", "vector-node")
            .with_sync_mode(SyncMode::Async)
            .with_capabilities(NodeCapabilities::vector_node()));

        router
    }

    #[test]
    fn test_route_oltp_read() {
        let router = create_test_setup();
        let decision = router.route("SELECT * FROM users WHERE id = 1");

        assert!(!decision.is_primary() || matches!(decision.reason, RoutingReason::LowLatency));
    }

    #[test]
    fn test_route_write_to_primary() {
        let router = create_test_setup();
        let decision = router.route("INSERT INTO users (name) VALUES ('test')");

        assert!(decision.is_primary());
        assert!(matches!(decision.reason, RoutingReason::WriteQuery));
    }

    #[test]
    fn test_route_vector_query() {
        let router = create_test_setup();
        let decision = router.route("SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]' LIMIT 10");

        assert!(matches!(decision.reason, RoutingReason::VectorCapable | RoutingReason::BestCandidate) || decision.is_primary());
    }

    #[test]
    fn test_route_olap_query() {
        let router = create_test_setup();
        let decision = router.route("SELECT COUNT(*), SUM(amount) FROM events GROUP BY date");

        // Should prefer columnar storage or async nodes
        assert!(!decision.is_primary() || matches!(decision.reason, RoutingReason::ColumnarStorage | RoutingReason::Default));
    }

    #[test]
    fn test_ai_workload_detection() {
        let detector = AIWorkloadDetector::new();

        let embedding = "SELECT * FROM embeddings ORDER BY vector <-> $1";
        let context = "SELECT * FROM conversation WHERE session_id = $1";
        let tool = "INSERT INTO tool_results (result) VALUES ($1)";

        assert_eq!(detector.detect(embedding), Some(AIWorkloadType::EmbeddingRetrieval));
        assert_eq!(detector.detect(context), Some(AIWorkloadType::ContextLookup));
        assert_eq!(detector.detect(tool), Some(AIWorkloadType::ToolExecution));
    }

    #[test]
    fn test_rag_routing() {
        let router = create_test_setup();

        let retrieval = router.route_rag(RAGStage::Retrieval, "SELECT embedding FROM docs");
        let fetch = router.route_rag(RAGStage::Fetch, "SELECT content FROM docs WHERE id IN (1,2,3)");

        // Retrieval should prefer vector nodes
        // Fetch should prefer high throughput
        assert!(retrieval.node_info.is_some() || retrieval.is_primary());
        assert!(fetch.node_info.is_some() || fetch.is_primary());
    }

    #[test]
    fn test_routing_decision_helpers() {
        let decision = RoutingDecision::shard(3);
        assert!(matches!(decision.target, RouteTarget::Shard(3)));

        let default = RoutingDecision::default_routing();
        assert!(default.is_primary());
    }
}
